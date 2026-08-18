use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use alloy_network::Ethereum;
use evm_fork_cache::reactive::{
    BlockRef as RuntimeBlockRef, EventSubscriber, HandlerId, InterestOwnerSubscriber,
    SubscriberDeliveryToken, SubscriberResumePosition,
};
use evm_fork_cache_event_protocol::{
    PROTOCOL_VERSION,
    v1::{
        Acknowledge, AcknowledgementCommitted, ApplyDesiredState, Barrier, ClientMessage, Cursor,
        Delivery, DesiredStateApplied, Heartbeat, HelloAccepted, ServerMessage, ServiceLimits,
        client_message, delivery,
        event_stream_server::{EventStream, EventStreamServer},
        server_message,
    },
};
use evm_fork_cache_remote::{
    GrpcEventTransport, GrpcTransportConfig, RemoteEventTransport, RemoteSubscriber,
    RemoteTransportError,
};
use futures::{Stream, StreamExt};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Request, Response, Status};

#[derive(Default)]
struct TestService {
    acknowledgements: Arc<Mutex<Vec<Acknowledge>>>,
}

#[derive(Default)]
struct ReconnectService {
    connections: Arc<AtomicUsize>,
}

#[derive(Default)]
struct CancellationService {
    applies: Arc<Mutex<Vec<ApplyDesiredState>>>,
    acknowledgements: Arc<Mutex<Vec<Acknowledge>>>,
}

struct HeartbeatStallingService;

#[tonic::async_trait]
impl EventStream for HeartbeatStallingService {
    type SessionStream =
        Pin<Box<dyn Stream<Item = Result<ServerMessage, Status>> + Send + 'static>>;

    async fn session(
        &self,
        request: Request<tonic::Streaming<ClientMessage>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let mut inbound = request.into_inner();
        let (sender, receiver) = mpsc::channel(8);
        tokio::spawn(async move {
            while let Some(Ok(message)) = inbound.next().await {
                match message.message {
                    Some(client_message::Message::Hello(ref hello))
                        if sender
                            .send(Ok(ServerMessage {
                                message: Some(server_message::Message::HelloAccepted(
                                    HelloAccepted {
                                        protocol_version: PROTOCOL_VERSION,
                                        session_id: hello.session_id.clone(),
                                        chain_id: hello.chain_id,
                                        committed_revision: 0,
                                        acknowledged_cursor: None,
                                        desired_state: None,
                                        capabilities: None,
                                        service_limits: None,
                                        runtime_checkpoint_position: None,
                                    },
                                )),
                            }))
                            .await
                            .is_err() =>
                    {
                        break;
                    }
                    Some(client_message::Message::Hello(_)) => {}
                    Some(client_message::Message::ApplyDesiredState(_)) => loop {
                        if sender
                            .send(Ok(ServerMessage {
                                message: Some(server_message::Message::Heartbeat(Heartbeat {
                                    unix_millis: 0,
                                })),
                            }))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    },
                    _ => {}
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

struct TamperedAckCursorService;

#[derive(Default)]
struct ResumeHandshakeStallingService {
    connections: Arc<AtomicUsize>,
}

#[tonic::async_trait]
impl EventStream for ResumeHandshakeStallingService {
    type SessionStream =
        Pin<Box<dyn Stream<Item = Result<ServerMessage, Status>> + Send + 'static>>;

    async fn session(
        &self,
        request: Request<tonic::Streaming<ClientMessage>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let connection = self.connections.fetch_add(1, Ordering::SeqCst);
        let mut inbound = request.into_inner();
        let (sender, receiver) = mpsc::channel(8);
        tokio::spawn(async move {
            let Some(Ok(ClientMessage {
                message: Some(client_message::Message::Hello(hello)),
            })) = inbound.next().await
            else {
                return;
            };
            if connection == 0 {
                sender
                    .send(Ok(ServerMessage {
                        message: Some(server_message::Message::HelloAccepted(HelloAccepted {
                            protocol_version: PROTOCOL_VERSION,
                            session_id: hello.session_id,
                            chain_id: hello.chain_id,
                            committed_revision: 0,
                            acknowledged_cursor: None,
                            desired_state: None,
                            capabilities: None,
                            service_limits: None,
                            runtime_checkpoint_position: None,
                        })),
                    }))
                    .await
                    .expect("initial response stream");
            } else {
                std::future::pending::<()>().await;
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

#[tonic::async_trait]
impl EventStream for TamperedAckCursorService {
    type SessionStream =
        Pin<Box<dyn Stream<Item = Result<ServerMessage, Status>> + Send + 'static>>;

    async fn session(
        &self,
        request: Request<tonic::Streaming<ClientMessage>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let mut inbound = request.into_inner();
        let (sender, receiver) = mpsc::channel(8);
        tokio::spawn(async move {
            while let Some(Ok(message)) = inbound.next().await {
                match message.message {
                    Some(client_message::Message::Hello(hello)) => {
                        sender
                            .send(Ok(ServerMessage {
                                message: Some(server_message::Message::HelloAccepted(
                                    HelloAccepted {
                                        protocol_version: PROTOCOL_VERSION,
                                        session_id: hello.session_id,
                                        chain_id: hello.chain_id,
                                        committed_revision: 0,
                                        acknowledged_cursor: None,
                                        desired_state: None,
                                        capabilities: None,
                                        service_limits: None,
                                        runtime_checkpoint_position: None,
                                    },
                                )),
                            }))
                            .await
                            .expect("client response stream");
                    }
                    Some(client_message::Message::ApplyDesiredState(request)) => {
                        sender
                            .send(Ok(ServerMessage {
                                message: Some(server_message::Message::DesiredStateApplied(
                                    DesiredStateApplied {
                                        session_id: request.session_id,
                                        revision: request.new_revision,
                                        activation_sequence: 1,
                                    },
                                )),
                            }))
                            .await
                            .expect("client response stream");
                    }
                    Some(client_message::Message::DeliveryDemand(demand)) => {
                        sender
                            .send(Ok(ServerMessage {
                                message: Some(server_message::Message::Delivery(Delivery {
                                    session_id: demand.session_id,
                                    sequence: 1,
                                    query_revision: 1,
                                    delivery_token: 1_u64.to_be_bytes().to_vec(),
                                    cursor: Some(Cursor {
                                        chain_id: 1,
                                        query_revision: 1,
                                        next_block: 11,
                                        canonical_head: None,
                                        batch_sequence: 1,
                                        provider_checkpoint: b"original".to_vec(),
                                        owner_backfill_activation_block: None,
                                    }),
                                    payload: Some(delivery::Payload::Barrier(Barrier {
                                        id: b"tampered-ack-test".to_vec(),
                                        block: None,
                                    })),
                                    checkpoint_neutral: false,
                                })),
                            }))
                            .await
                            .expect("client response stream");
                    }
                    Some(client_message::Message::Acknowledge(acknowledgement)) => {
                        sender
                            .send(Ok(ServerMessage {
                                message: Some(server_message::Message::AcknowledgementCommitted(
                                    AcknowledgementCommitted {
                                        session_id: acknowledgement.session_id,
                                        sequence: acknowledgement.sequence,
                                        cursor: Some(Cursor {
                                            chain_id: 1,
                                            query_revision: 1,
                                            next_block: 11,
                                            canonical_head: None,
                                            batch_sequence: acknowledgement.sequence,
                                            provider_checkpoint: b"tampered".to_vec(),
                                            owner_backfill_activation_block: None,
                                        }),
                                    },
                                )),
                            }))
                            .await
                            .expect("client response stream");
                    }
                    _ => {}
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

#[tokio::test]
async fn grpc_transport_rejects_zero_keepalive_and_reconnect_durations_before_connecting() {
    let mut zero_interval = GrpcTransportConfig::default();
    zero_interval.http2_keep_alive_interval = std::time::Duration::ZERO;
    let mut zero_timeout = GrpcTransportConfig::default();
    zero_timeout.http2_keep_alive_timeout = std::time::Duration::ZERO;
    let mut zero_initial_reconnect = GrpcTransportConfig::default();
    zero_initial_reconnect.reconnect_initial_delay = std::time::Duration::ZERO;
    let mut zero_max_reconnect = GrpcTransportConfig::default();
    zero_max_reconnect.reconnect_max_delay = std::time::Duration::ZERO;

    for config in [
        zero_interval,
        zero_timeout,
        zero_initial_reconnect,
        zero_max_reconnect,
    ] {
        let result = GrpcEventTransport::connect_with_config(
            "http://127.0.0.1:1",
            "runtime-a",
            1,
            0,
            config,
        )
        .await;
        let Err(error) = result else {
            panic!("zero keepalive duration must be rejected before dialing");
        };
        assert!(matches!(error, RemoteTransportError::Protocol(_)));
    }
}

#[tokio::test]
async fn heartbeats_cannot_extend_the_absolute_control_operation_deadline() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("listener address");
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(EventStreamServer::new(HeartbeatStallingService))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_receiver.await;
            })
            .await
            .expect("test gRPC server");
    });
    let mut config = GrpcTransportConfig::default();
    config.control_response_timeout = std::time::Duration::from_millis(30);
    let mut transport = GrpcEventTransport::connect_with_config(
        format!("http://{address}"),
        "runtime-a",
        1,
        0,
        config,
    )
    .await
    .expect("initial session");

    let result = tokio::time::timeout(
        std::time::Duration::from_millis(250),
        transport.apply_desired_state(ApplyDesiredState {
            protocol_version: PROTOCOL_VERSION,
            session_id: "runtime-a".into(),
            chain_id: 1,
            expected_revision: 0,
            new_revision: 1,
            owners: Vec::new(),
        }),
    )
    .await
    .expect("absolute operation deadline");
    assert!(matches!(result, Err(RemoteTransportError::Unavailable(_))));

    drop(transport);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn resume_reconnect_and_hello_share_the_control_operation_deadline() {
    let service = ResumeHandshakeStallingService::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("listener address");
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(EventStreamServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_receiver.await;
            })
            .await
            .expect("test gRPC server");
    });
    let mut config = GrpcTransportConfig::default();
    config.control_response_timeout = std::time::Duration::from_millis(30);
    config.handshake_timeout = std::time::Duration::from_secs(5);
    let mut transport = GrpcEventTransport::connect_with_config(
        format!("http://{address}"),
        "runtime-a",
        1,
        0,
        config,
    )
    .await
    .expect("initial session");
    transport
        .restore_position(&SubscriberResumePosition::new(
            1,
            RuntimeBlockRef {
                number: 0,
                hash: Default::default(),
                parent_hash: Some(Default::default()),
                timestamp: Some(0),
            },
            Vec::new(),
            Some(SubscriberDeliveryToken::new(1_u64.to_be_bytes().to_vec())),
            None,
        ))
        .expect("pending-delivery resume proof");

    let result = tokio::time::timeout(
        std::time::Duration::from_millis(250),
        transport.apply_desired_state(ApplyDesiredState {
            protocol_version: PROTOCOL_VERSION,
            session_id: "runtime-a".into(),
            chain_id: 1,
            expected_revision: 0,
            new_revision: 1,
            owners: Vec::new(),
        }),
    )
    .await
    .expect("whole control deadline must bound resume handshake");
    assert!(matches!(result, Err(RemoteTransportError::Unavailable(_))));

    drop(transport);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn acknowledgement_confirmation_must_echo_the_exact_delivery_cursor() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("listener address");
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(EventStreamServer::new(TamperedAckCursorService))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_receiver.await;
            })
            .await
            .expect("test gRPC server");
    });
    let mut transport = GrpcEventTransport::connect(format!("http://{address}"), "runtime-a", 1, 0)
        .await
        .expect("initial session");
    transport
        .apply_desired_state(ApplyDesiredState {
            protocol_version: PROTOCOL_VERSION,
            session_id: "runtime-a".into(),
            chain_id: 1,
            expected_revision: 0,
            new_revision: 1,
            owners: Vec::new(),
        })
        .await
        .expect("desired state");
    let delivery = transport
        .next_delivery()
        .await
        .expect("delivery stream")
        .expect("delivery");
    let result = transport
        .acknowledge(Acknowledge {
            session_id: delivery.session_id,
            sequence: delivery.sequence,
            delivery_token: delivery.delivery_token,
        })
        .await;
    assert!(matches!(result, Err(RemoteTransportError::Protocol(_))));

    drop(transport);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tonic::async_trait]
impl EventStream for CancellationService {
    type SessionStream =
        Pin<Box<dyn Stream<Item = Result<ServerMessage, Status>> + Send + 'static>>;

    async fn session(
        &self,
        request: Request<tonic::Streaming<ClientMessage>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let applies = Arc::clone(&self.applies);
        let acknowledgements = Arc::clone(&self.acknowledgements);
        let mut inbound = request.into_inner();
        let (sender, receiver) = mpsc::channel(8);
        tokio::spawn(async move {
            while let Some(Ok(message)) = inbound.next().await {
                match message.message {
                    Some(client_message::Message::Hello(hello)) => {
                        sender
                            .send(Ok(ServerMessage {
                                message: Some(server_message::Message::HelloAccepted(
                                    HelloAccepted {
                                        protocol_version: PROTOCOL_VERSION,
                                        session_id: hello.session_id,
                                        chain_id: hello.chain_id,
                                        committed_revision: 0,
                                        acknowledged_cursor: None,
                                        desired_state: None,
                                        capabilities: None,
                                        service_limits: None,
                                        runtime_checkpoint_position: None,
                                    },
                                )),
                            }))
                            .await
                            .expect("client response stream");
                    }
                    Some(client_message::Message::ApplyDesiredState(request)) => {
                        applies.lock().await.push(request.clone());
                        sender
                            .send(Ok(ServerMessage {
                                message: Some(server_message::Message::Delivery(Delivery {
                                    session_id: request.session_id.clone(),
                                    sequence: 1,
                                    query_revision: request.new_revision,
                                    delivery_token: 1_u64.to_be_bytes().to_vec(),
                                    cursor: Some(Cursor {
                                        chain_id: request.chain_id,
                                        query_revision: request.new_revision,
                                        next_block: 11,
                                        canonical_head: None,
                                        batch_sequence: 1,
                                        provider_checkpoint: Vec::new(),
                                        owner_backfill_activation_block: None,
                                    }),
                                    payload: Some(delivery::Payload::Barrier(Barrier {
                                        id: format!("desired-state:{}", request.new_revision)
                                            .into_bytes(),
                                        block: None,
                                    })),
                                    checkpoint_neutral: true,
                                })),
                            }))
                            .await
                            .expect("client response stream");
                        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
                        sender
                            .send(Ok(ServerMessage {
                                message: Some(server_message::Message::DesiredStateApplied(
                                    DesiredStateApplied {
                                        session_id: request.session_id,
                                        revision: request.new_revision,
                                        activation_sequence: 1,
                                    },
                                )),
                            }))
                            .await
                            .expect("client response stream");
                    }
                    Some(client_message::Message::Acknowledge(acknowledgement)) => {
                        acknowledgements.lock().await.push(acknowledgement.clone());
                        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
                        sender
                            .send(Ok(ServerMessage {
                                message: Some(server_message::Message::AcknowledgementCommitted(
                                    AcknowledgementCommitted {
                                        session_id: acknowledgement.session_id,
                                        sequence: acknowledgement.sequence,
                                        cursor: Some(Cursor {
                                            chain_id: 1,
                                            query_revision: 1,
                                            next_block: 11,
                                            canonical_head: None,
                                            batch_sequence: acknowledgement.sequence,
                                            provider_checkpoint: Vec::new(),
                                            owner_backfill_activation_block: None,
                                        }),
                                    },
                                )),
                            }))
                            .await
                            .expect("client response stream");
                    }
                    Some(client_message::Message::DeliveryDemand(demand)) => {
                        sender
                            .send(Ok(ServerMessage {
                                message: Some(server_message::Message::Delivery(Delivery {
                                    session_id: demand.session_id,
                                    sequence: 2,
                                    query_revision: 1,
                                    delivery_token: 2_u64.to_be_bytes().to_vec(),
                                    cursor: Some(Cursor {
                                        chain_id: 1,
                                        query_revision: 1,
                                        next_block: 11,
                                        canonical_head: None,
                                        batch_sequence: 2,
                                        provider_checkpoint: b"runtime-visible".to_vec(),
                                        owner_backfill_activation_block: None,
                                    }),
                                    payload: Some(delivery::Payload::Barrier(Barrier {
                                        id: b"runtime-visible".to_vec(),
                                        block: None,
                                    })),
                                    checkpoint_neutral: false,
                                })),
                            }))
                            .await
                            .expect("client response stream");
                    }
                    _ => {}
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

#[tonic::async_trait]
impl EventStream for ReconnectService {
    type SessionStream =
        Pin<Box<dyn Stream<Item = Result<ServerMessage, Status>> + Send + 'static>>;

    async fn session(
        &self,
        request: Request<tonic::Streaming<ClientMessage>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let connection = self.connections.fetch_add(1, Ordering::SeqCst);
        let mut inbound = request.into_inner();
        let (sender, receiver) = mpsc::channel(8);
        tokio::spawn(async move {
            if let Some(Ok(ClientMessage {
                message: Some(client_message::Message::Hello(hello)),
            })) = inbound.next().await
            {
                if connection == 1 {
                    sender
                        .send(Ok(ServerMessage {
                            message: Some(server_message::Message::Error(
                                evm_fork_cache_event_protocol::v1::ProtocolError {
                                    code:
                                        evm_fork_cache_event_protocol::v1::ErrorCode::SessionInUse
                                            .into(),
                                    message: "previous stream still owns the lease".into(),
                                    committed_revision: 0,
                                    retryable: true,
                                },
                            )),
                        }))
                        .await
                        .expect("client response stream");
                    return;
                }
                sender
                    .send(Ok(ServerMessage {
                        message: Some(server_message::Message::HelloAccepted(HelloAccepted {
                            protocol_version: PROTOCOL_VERSION,
                            session_id: hello.session_id.clone(),
                            chain_id: hello.chain_id,
                            committed_revision: 0,
                            acknowledged_cursor: None,
                            desired_state: None,
                            capabilities: None,
                            service_limits: Some(ServiceLimits {
                                max_owners: if connection == 0 { 10 } else { 20 },
                                ..Default::default()
                            }),
                            runtime_checkpoint_position: None,
                        })),
                    }))
                    .await
                    .expect("client response stream");
                if connection > 1 {
                    let Some(Ok(ClientMessage {
                        message: Some(client_message::Message::DeliveryDemand(_)),
                    })) = inbound.next().await
                    else {
                        return;
                    };
                    sender
                        .send(Ok(ServerMessage {
                            message: Some(server_message::Message::Delivery(Delivery {
                                session_id: hello.session_id,
                                sequence: 1,
                                query_revision: 0,
                                delivery_token: 1_u64.to_be_bytes().to_vec(),
                                cursor: Some(Cursor {
                                    chain_id: hello.chain_id,
                                    query_revision: 0,
                                    next_block: 11,
                                    canonical_head: None,
                                    batch_sequence: 1,
                                    provider_checkpoint: Vec::new(),
                                    owner_backfill_activation_block: None,
                                }),
                                payload: Some(delivery::Payload::Barrier(Barrier {
                                    id: b"source-progress:0:11".to_vec(),
                                    block: None,
                                })),
                                checkpoint_neutral: true,
                            })),
                        }))
                        .await
                        .expect("client response stream");
                    std::future::pending::<()>().await;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

#[tonic::async_trait]
impl EventStream for TestService {
    type SessionStream =
        Pin<Box<dyn Stream<Item = Result<ServerMessage, Status>> + Send + 'static>>;

    async fn session(
        &self,
        request: Request<tonic::Streaming<ClientMessage>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let acknowledgements = Arc::clone(&self.acknowledgements);
        let mut inbound = request.into_inner();
        let (sender, receiver) = mpsc::channel(8);
        tokio::spawn(async move {
            while let Some(Ok(message)) = inbound.next().await {
                match message.message {
                    Some(client_message::Message::Hello(hello)) => {
                        sender
                            .send(Ok(ServerMessage {
                                message: Some(server_message::Message::HelloAccepted(
                                    HelloAccepted {
                                        protocol_version: PROTOCOL_VERSION,
                                        session_id: hello.session_id,
                                        chain_id: hello.chain_id,
                                        committed_revision: 0,
                                        acknowledged_cursor: None,
                                        desired_state: None,
                                        capabilities: None,
                                        service_limits: None,
                                        runtime_checkpoint_position: None,
                                    },
                                )),
                            }))
                            .await
                            .expect("client response stream");
                    }
                    Some(client_message::Message::ApplyDesiredState(request)) => {
                        sender
                            .send(Ok(ServerMessage {
                                message: Some(server_message::Message::Delivery(Delivery {
                                    session_id: request.session_id.clone(),
                                    sequence: 1,
                                    query_revision: request.new_revision,
                                    delivery_token: 1_u64.to_be_bytes().to_vec(),
                                    cursor: Some(Cursor {
                                        chain_id: request.chain_id,
                                        query_revision: request.new_revision,
                                        next_block: 11,
                                        canonical_head: None,
                                        batch_sequence: 1,
                                        provider_checkpoint: Vec::new(),
                                        owner_backfill_activation_block: None,
                                    }),
                                    payload: Some(delivery::Payload::Barrier(Barrier {
                                        id: b"desired-state:1".to_vec(),
                                        block: None,
                                    })),
                                    checkpoint_neutral: true,
                                })),
                            }))
                            .await
                            .expect("client response stream");
                        sender
                            .send(Ok(ServerMessage {
                                message: Some(server_message::Message::DesiredStateApplied(
                                    DesiredStateApplied {
                                        session_id: request.session_id,
                                        revision: request.new_revision,
                                        activation_sequence: 1,
                                    },
                                )),
                            }))
                            .await
                            .expect("client response stream");
                    }
                    Some(client_message::Message::Acknowledge(acknowledgement)) => {
                        acknowledgements.lock().await.push(acknowledgement.clone());
                        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
                        sender
                            .send(Ok(ServerMessage {
                                message: Some(server_message::Message::AcknowledgementCommitted(
                                    AcknowledgementCommitted {
                                        session_id: acknowledgement.session_id,
                                        sequence: acknowledgement.sequence,
                                        cursor: Some(Cursor {
                                            chain_id: 1,
                                            query_revision: 1,
                                            next_block: 11,
                                            canonical_head: None,
                                            batch_sequence: acknowledgement.sequence,
                                            provider_checkpoint: Vec::new(),
                                            owner_backfill_activation_block: None,
                                        }),
                                    },
                                )),
                            }))
                            .await
                            .expect("client response stream");
                    }
                    _ => {}
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

#[tokio::test]
async fn grpc_transport_negotiates_session_and_multiplexes_control_and_data() {
    let acknowledgements = Arc::new(Mutex::new(Vec::new()));
    let service = TestService {
        acknowledgements: Arc::clone(&acknowledgements),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("listener address");
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(EventStreamServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_receiver.await;
            })
            .await
            .expect("test gRPC server");
    });

    let mut transport = GrpcEventTransport::connect(format!("http://{address}"), "runtime-a", 1, 0)
        .await
        .expect("connect and negotiate");
    let applied = transport
        .apply_desired_state(ApplyDesiredState {
            protocol_version: PROTOCOL_VERSION,
            session_id: "runtime-a".into(),
            chain_id: 1,
            expected_revision: 0,
            new_revision: 1,
            owners: Vec::new(),
        })
        .await
        .expect("desired state applied");
    assert_eq!(applied.revision, 1);

    let batch = transport
        .next_delivery()
        .await
        .expect("data stream")
        .expect("data batch");
    let replay = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        transport.next_delivery(),
    )
    .await
    .expect("unacknowledged delivery must be replayable on the same stream")
    .expect("data stream")
    .expect("replayed data batch");
    assert_eq!(replay, batch);
    let started = std::time::Instant::now();
    transport
        .acknowledge(Acknowledge {
            session_id: batch.session_id.clone(),
            sequence: batch.sequence,
            delivery_token: batch.delivery_token.clone(),
        })
        .await
        .expect("durably commit acknowledgement");
    assert!(
        started.elapsed() >= std::time::Duration::from_millis(30),
        "transport must wait for the server's durable ACK confirmation"
    );

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if !acknowledgements.lock().await.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("server acknowledgement");
    assert_eq!(acknowledgements.lock().await[0].sequence, 1);

    drop(transport);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn grpc_transport_reconnects_a_terminated_stream_and_resumes_delivery() {
    let connections = Arc::new(AtomicUsize::new(0));
    let service = ReconnectService {
        connections: Arc::clone(&connections),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("listener address");
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(EventStreamServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_receiver.await;
            })
            .await
            .expect("test gRPC server");
    });

    let mut config = GrpcTransportConfig::default();
    config.reconnect_initial_delay = std::time::Duration::from_secs(1);
    config.reconnect_max_delay = std::time::Duration::from_millis(10);
    let mut transport = GrpcEventTransport::connect_with_config(
        format!("http://{address}"),
        "runtime-a",
        1,
        0,
        config,
    )
    .await
    .expect("initial session");
    let batch = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        transport.next_delivery(),
    )
    .await
    .expect("reconnect delay must be clamped to its configured maximum")
    .expect("resumed stream")
    .expect("replayed batch");

    assert_eq!(batch.sequence, 1);
    assert!(connections.load(Ordering::SeqCst) >= 3);
    assert!(transport.reconnect_count() >= 1);
    assert_eq!(
        transport
            .accepted()
            .service_limits
            .as_ref()
            .expect("refreshed reconnect policy")
            .max_owners,
        20
    );
    drop(transport);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn cancelled_apply_and_ack_are_resumed_without_duplicate_wire_requests() {
    let applies = Arc::new(Mutex::new(Vec::new()));
    let acknowledgements = Arc::new(Mutex::new(Vec::new()));
    let service = CancellationService {
        applies: Arc::clone(&applies),
        acknowledgements: Arc::clone(&acknowledgements),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("listener address");
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(EventStreamServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_receiver.await;
            })
            .await
            .expect("test gRPC server");
    });

    let mut transport = GrpcEventTransport::connect(format!("http://{address}"), "runtime-a", 1, 0)
        .await
        .expect("initial session");
    let request = ApplyDesiredState {
        protocol_version: PROTOCOL_VERSION,
        session_id: "runtime-a".into(),
        chain_id: 1,
        expected_revision: 0,
        new_revision: 1,
        owners: Vec::new(),
    };
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(10),
            transport.apply_desired_state(request.clone()),
        )
        .await
        .is_err()
    );
    assert!(transport.next_delivery().await.is_err());
    transport
        .apply_desired_state(request)
        .await
        .expect("resume pending apply");
    assert_eq!(applies.lock().await.len(), 1);

    let delivery = transport.next_delivery().await.unwrap().unwrap();
    let acknowledgement = Acknowledge {
        session_id: delivery.session_id,
        sequence: delivery.sequence,
        delivery_token: delivery.delivery_token,
    };
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(10),
            transport.acknowledge(acknowledgement.clone()),
        )
        .await
        .is_err()
    );
    assert!(transport.next_delivery().await.is_err());
    transport
        .acknowledge(acknowledgement)
        .await
        .expect("resume pending acknowledgement");
    assert_eq!(acknowledgements.lock().await.len(), 1);

    drop(transport);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn cancelled_bulk_owner_upsert_reconciles_one_exact_grpc_revision() {
    let applies = Arc::new(Mutex::new(Vec::new()));
    let acknowledgements = Arc::new(Mutex::new(Vec::new()));
    let service = CancellationService {
        applies: Arc::clone(&applies),
        acknowledgements: Arc::clone(&acknowledgements),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("listener address");
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(EventStreamServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_receiver.await;
            })
            .await
            .expect("test gRPC server");
    });

    let transport = GrpcEventTransport::connect(format!("http://{address}"), "bulk", 1, 0)
        .await
        .expect("transport");
    let mut subscriber = RemoteSubscriber::<_, Ethereum>::new("bulk", 1, transport);
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(10),
            subscriber.upsert_interest_owners(vec![
                (HandlerId::new("owner-a"), Vec::new()),
                (HandlerId::new("owner-b"), Vec::new()),
            ]),
        )
        .await
        .is_err()
    );

    let runtime_delivery =
        tokio::time::timeout(std::time::Duration::from_secs(1), subscriber.next_batch())
            .await
            .expect("cancelled bulk reconciliation must remain bounded")
            .expect("reconcile cancelled bulk upsert")
            .expect("runtime-visible delivery after the internal activation");
    assert_eq!(subscriber.committed_revision(), 1);
    assert_eq!(runtime_delivery.chain_controls().len(), 1);
    let applied = applies.lock().await;
    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].owners.len(), 2);
    assert_eq!(acknowledgements.lock().await.len(), 1);

    drop(applied);
    drop(subscriber);
    let _ = shutdown_sender.send(());
    tokio::time::timeout(std::time::Duration::from_secs(1), server)
        .await
        .expect("test server shutdown must remain bounded")
        .expect("join server");
}
