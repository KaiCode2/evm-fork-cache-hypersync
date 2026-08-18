use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use evm_fork_cache::reactive::{
    BlockRef as RuntimeBlockRef, EventSubscriber, SubscriberCapability, SubscriberCheckpoint,
    SubscriberDeliveryToken, SubscriberResumePosition,
};
use evm_fork_cache_event_protocol::{
    PROTOCOL_VERSION,
    v1::{
        Acknowledge, ApplyDesiredState, Barrier, BlockInterest, BlockMode, BlockProgressEvent,
        BlockRef, Capability, ChainEvent, ClientMessage, Cursor, DataPayload, Delivery,
        DeliveryDemand, DeliveryScope, ErrorCode, EventRecord, Heartbeat, Hello, OwnerInterests,
        PortableInterest, Reorg, SourceCapabilities, chain_event, client_message, delivery,
        event_stream_client::EventStreamClient, portable_interest, server_message,
    },
};
use evm_fork_cache_event_service::{
    DeliveryRequest, EventService, EventServiceConfigError, EventServiceLimits, EventSource,
    EventSourceError, PreparationId, SessionAuthorizer, SessionStore,
};
use evm_fork_cache_remote::{
    GrpcEventTransport, GrpcTransportConfig, RemoteEventTransport, RemoteSubscriber,
    RemoteTransportError,
};
use tokio::sync::{Mutex, Notify, mpsc, oneshot};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::metadata::MetadataMap;

struct RequireBearer;

impl SessionAuthorizer for RequireBearer {
    fn authorize(&self, metadata: &MetadataMap) -> Result<(), String> {
        match metadata
            .get("authorization")
            .and_then(|value| value.to_str().ok())
        {
            Some("Bearer test-secret") => Ok(()),
            _ => Err("missing bearer".into()),
        }
    }
}

struct SessionScopedBearer;

impl SessionAuthorizer for SessionScopedBearer {
    fn authorize(&self, metadata: &MetadataMap) -> Result<(), String> {
        RequireBearer.authorize(metadata)
    }

    fn authorize_session(
        &self,
        metadata: &MetadataMap,
        session_id: &str,
        chain_id: u64,
    ) -> Result<(), String> {
        self.authorize(metadata)?;
        if session_id == "allowed" && chain_id == 1 {
            Ok(())
        } else {
            Err("principal is not scoped to this session".into())
        }
    }
}

#[derive(Default)]
struct ScriptedProvider {
    delivered: Mutex<bool>,
    acknowledgements: Mutex<Vec<Acknowledge>>,
    reject_prepare: AtomicBool,
    fail_activate: AtomicBool,
    fail_abort: AtomicBool,
    fail_release: AtomicBool,
    preparations: AtomicUsize,
    aborts: AtomicUsize,
    releases: AtomicUsize,
    prepared_checkpoint_bytes: AtomicUsize,
    delivery_checkpoint_bytes: AtomicUsize,
}

#[derive(Default)]
struct WakeProvider {
    ready: AtomicBool,
    attempts: AtomicUsize,
    wake: Notify,
}

#[derive(Default)]
struct BlockingDataAckProvider {
    delivered: Mutex<bool>,
    data_acknowledgements: AtomicUsize,
    data_ack_entered: Notify,
    data_ack_release: Notify,
}

struct HangingPrepareProvider;

struct HangingReleaseProvider;

struct FailingReleaseProvider;

struct SecretErrorProvider;

#[derive(Clone, Copy)]
enum ConstraintBehavior {
    ReturnNone,
    IgnoreConstraint,
    RejectConstraint,
}

#[derive(Clone, Debug, PartialEq)]
struct RequestObservation {
    session_id: String,
    acknowledged_cursor: Option<Cursor>,
    required_reorg_anchor: Option<BlockRef>,
}

struct ConstraintProvider {
    behavior: ConstraintBehavior,
    next_requests: Mutex<Vec<RequestObservation>>,
    wait_requests: Mutex<Vec<RequestObservation>>,
    next_seen: Notify,
    wait_seen: Notify,
    wake: Notify,
}

impl ConstraintProvider {
    fn new(behavior: ConstraintBehavior) -> Self {
        Self {
            behavior,
            next_requests: Mutex::new(Vec::new()),
            wait_requests: Mutex::new(Vec::new()),
            next_seen: Notify::new(),
            wait_seen: Notify::new(),
            wake: Notify::new(),
        }
    }
}

#[async_trait]
impl EventSource for HangingPrepareProvider {
    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities::default()
    }

    async fn prepare_desired_state(
        &self,
        _preparation_id: PreparationId,
        _desired_state: &ApplyDesiredState,
        _acknowledged_cursor: Option<&Cursor>,
    ) -> Result<Option<Cursor>, EventSourceError> {
        std::future::pending().await
    }

    async fn next_delivery(
        &self,
        _request: DeliveryRequest<'_>,
    ) -> Result<Option<Delivery>, EventSourceError> {
        Ok(None)
    }

    async fn acknowledge(
        &self,
        _chain_id: u64,
        _acknowledgement: &Acknowledge,
        _committed_cursor: &Cursor,
    ) -> Result<(), EventSourceError> {
        Ok(())
    }
}

#[async_trait]
impl EventSource for HangingReleaseProvider {
    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities::default()
    }

    async fn release_session(
        &self,
        _session_id: &str,
        _chain_id: u64,
    ) -> Result<(), EventSourceError> {
        std::future::pending().await
    }

    async fn next_delivery(
        &self,
        _request: DeliveryRequest<'_>,
    ) -> Result<Option<Delivery>, EventSourceError> {
        Ok(None)
    }

    async fn acknowledge(
        &self,
        _chain_id: u64,
        _acknowledgement: &Acknowledge,
        _committed_cursor: &Cursor,
    ) -> Result<(), EventSourceError> {
        Ok(())
    }
}

#[async_trait]
impl EventSource for FailingReleaseProvider {
    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities::default()
    }

    async fn release_session(
        &self,
        _session_id: &str,
        _chain_id: u64,
    ) -> Result<(), EventSourceError> {
        Err(EventSourceError::internal(
            "provider cleanup failed with secret-token",
        ))
    }

    async fn next_delivery(
        &self,
        _request: DeliveryRequest<'_>,
    ) -> Result<Option<Delivery>, EventSourceError> {
        Ok(None)
    }

    async fn acknowledge(
        &self,
        _chain_id: u64,
        _acknowledgement: &Acknowledge,
        _committed_cursor: &Cursor,
    ) -> Result<(), EventSourceError> {
        Ok(())
    }
}

#[async_trait]
impl EventSource for SecretErrorProvider {
    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities::default()
    }

    async fn next_delivery(
        &self,
        _request: DeliveryRequest<'_>,
    ) -> Result<Option<Delivery>, EventSourceError> {
        Err(EventSourceError::unavailable(
            "https://user:super-secret@indexer.invalid/private",
        ))
    }

    async fn acknowledge(
        &self,
        _chain_id: u64,
        _acknowledgement: &Acknowledge,
        _committed_cursor: &Cursor,
    ) -> Result<(), EventSourceError> {
        Ok(())
    }
}

fn observe_request(request: DeliveryRequest<'_>) -> RequestObservation {
    RequestObservation {
        session_id: request.desired_state().session_id.clone(),
        acknowledged_cursor: request.acknowledged_cursor().cloned(),
        required_reorg_anchor: request.required_reorg_anchor().cloned(),
    }
}

#[async_trait]
impl EventSource for ConstraintProvider {
    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities::default()
    }

    async fn next_delivery(
        &self,
        request: DeliveryRequest<'_>,
    ) -> Result<Option<Delivery>, EventSourceError> {
        self.next_requests
            .lock()
            .await
            .push(observe_request(request));
        self.next_seen.notify_one();
        match self.behavior {
            ConstraintBehavior::ReturnNone => Ok(None),
            ConstraintBehavior::RejectConstraint => {
                if request.required_reorg_anchor().is_some() {
                    Err(EventSourceError::unsupported(
                        "test source cannot honor durable reorg anchors",
                    ))
                } else {
                    Ok(None)
                }
            }
            ConstraintBehavior::IgnoreConstraint => {
                let desired = request.desired_state();
                let acknowledged = request.acknowledged_cursor().ok_or_else(|| {
                    EventSourceError::internal("test fixture is missing its cursor")
                })?;
                let anchor = request.required_reorg_anchor().ok_or_else(|| {
                    EventSourceError::internal("test fixture is missing its reorg anchor")
                })?;
                let number = anchor
                    .number
                    .checked_add(1)
                    .ok_or_else(|| EventSourceError::internal("test fixture anchor overflowed"))?;
                let next_block = number.checked_add(1).ok_or_else(|| {
                    EventSourceError::internal("test fixture successor overflowed")
                })?;
                let sequence = acknowledged.batch_sequence.checked_add(1).ok_or_else(|| {
                    EventSourceError::internal("test fixture sequence overflowed")
                })?;
                let later = BlockRef {
                    number,
                    hash: vec![0x31; 32],
                    parent_hash: anchor.hash.clone(),
                    timestamp: anchor.timestamp.saturating_add(1),
                };
                Ok(Some(Delivery {
                    session_id: desired.session_id.clone(),
                    sequence,
                    query_revision: desired.new_revision,
                    delivery_token: sequence.to_be_bytes().to_vec(),
                    cursor: Some(Cursor {
                        chain_id: desired.chain_id,
                        query_revision: desired.new_revision,
                        next_block,
                        canonical_head: Some(later.clone()),
                        batch_sequence: sequence,
                        provider_checkpoint: b"ignored-constraint".to_vec(),
                        owner_backfill_activation_block: acknowledged
                            .owner_backfill_activation_block,
                    }),
                    payload: Some(delivery::Payload::Barrier(Barrier {
                        id: b"ignored-reorg-constraint".to_vec(),
                        block: Some(later),
                    })),
                    checkpoint_neutral: false,
                }))
            }
        }
    }

    async fn wait_for_update(&self, request: DeliveryRequest<'_>) -> Result<(), EventSourceError> {
        self.wait_requests
            .lock()
            .await
            .push(observe_request(request));
        self.wait_seen.notify_one();
        self.wake.notified().await;
        Ok(())
    }

    async fn acknowledge(
        &self,
        _chain_id: u64,
        _acknowledgement: &Acknowledge,
        _committed_cursor: &Cursor,
    ) -> Result<(), EventSourceError> {
        Ok(())
    }
}

struct ChainAwareProvider;

#[async_trait]
impl EventSource for ChainAwareProvider {
    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities::default()
    }

    fn capabilities_for_chain(&self, chain_id: u64) -> SourceCapabilities {
        SourceCapabilities {
            capabilities: vec![if chain_id == 1 {
                Capability::Live.into()
            } else {
                Capability::Historical.into()
            }],
            sources: Vec::new(),
        }
    }

    async fn next_delivery(
        &self,
        _request: DeliveryRequest<'_>,
    ) -> Result<Option<Delivery>, EventSourceError> {
        Ok(None)
    }

    async fn acknowledge(
        &self,
        _chain_id: u64,
        _acknowledgement: &Acknowledge,
        _committed_cursor: &Cursor,
    ) -> Result<(), EventSourceError> {
        Ok(())
    }
}

fn data_delivery(desired_state: &ApplyDesiredState, cursor: Option<&Cursor>) -> Delivery {
    let sequence = cursor.map_or(1, |cursor| cursor.batch_sequence + 1);
    let head = BlockRef {
        number: 10,
        hash: vec![0x10; 32],
        parent_hash: vec![0x0f; 32],
        timestamp: 10,
    };
    Delivery {
        session_id: desired_state.session_id.clone(),
        sequence,
        query_revision: desired_state.new_revision,
        delivery_token: sequence.to_be_bytes().to_vec(),
        cursor: Some(Cursor {
            chain_id: desired_state.chain_id,
            query_revision: desired_state.new_revision,
            next_block: 11,
            canonical_head: Some(head.clone()),
            batch_sequence: sequence,
            provider_checkpoint: b"scripted-checkpoint".to_vec(),
            owner_backfill_activation_block: None,
        }),
        payload: Some(delivery::Payload::Data(DataPayload {
            records: vec![EventRecord {
                event: Some(ChainEvent {
                    event: Some(chain_event::Event::BlockProgress(BlockProgressEvent {
                        block: Some(head),
                    })),
                }),
                canonical_audience: true,
                owner_ids: Vec::new(),
                scope: DeliveryScope::CanonicalProgress.into(),
            }],
        })),
        checkpoint_neutral: false,
    }
}

#[async_trait]
impl EventSource for WakeProvider {
    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities::default()
    }

    async fn next_delivery(
        &self,
        request: DeliveryRequest<'_>,
    ) -> Result<Option<Delivery>, EventSourceError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        if !self.ready.load(Ordering::SeqCst) {
            return Ok(None);
        }
        Ok(Some(data_delivery(
            request.desired_state(),
            request.acknowledged_cursor(),
        )))
    }

    async fn wait_for_update(&self, _request: DeliveryRequest<'_>) -> Result<(), EventSourceError> {
        self.wake.notified().await;
        Ok(())
    }

    async fn acknowledge(
        &self,
        _chain_id: u64,
        _acknowledgement: &Acknowledge,
        _committed_cursor: &Cursor,
    ) -> Result<(), EventSourceError> {
        Ok(())
    }
}

#[async_trait]
impl EventSource for BlockingDataAckProvider {
    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities::default()
    }

    async fn next_delivery(
        &self,
        request: DeliveryRequest<'_>,
    ) -> Result<Option<Delivery>, EventSourceError> {
        let mut delivered = self.delivered.lock().await;
        if *delivered {
            return Ok(None);
        }
        *delivered = true;
        Ok(Some(data_delivery(
            request.desired_state(),
            request.acknowledged_cursor(),
        )))
    }

    async fn acknowledge(
        &self,
        _chain_id: u64,
        acknowledgement: &Acknowledge,
        _committed_cursor: &Cursor,
    ) -> Result<(), EventSourceError> {
        if acknowledgement.sequence > 1 {
            self.data_acknowledgements.fetch_add(1, Ordering::SeqCst);
            self.data_ack_entered.notify_one();
            self.data_ack_release.notified().await;
        }
        Ok(())
    }
}

#[async_trait]
impl EventSource for ScriptedProvider {
    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities {
            capabilities: vec![
                Capability::Historical.into(),
                Capability::Logs.into(),
                Capability::DynamicFilters.into(),
                Capability::ExplicitReorgs.into(),
                Capability::OwnerScopedDelivery.into(),
                Capability::DurableReplay.into(),
            ],
            sources: Vec::new(),
        }
    }

    async fn prepare_desired_state(
        &self,
        _preparation_id: PreparationId,
        _desired_state: &ApplyDesiredState,
        _acknowledged_cursor: Option<&Cursor>,
    ) -> Result<Option<Cursor>, EventSourceError> {
        self.preparations.fetch_add(1, Ordering::SeqCst);
        if self.reject_prepare.load(Ordering::SeqCst) {
            return Err(EventSourceError::unsupported("forced prepare rejection"));
        }
        let checkpoint_bytes = self.prepared_checkpoint_bytes.load(Ordering::SeqCst);
        Ok((checkpoint_bytes > 0).then(|| Cursor {
            chain_id: _desired_state.chain_id,
            query_revision: _desired_state.new_revision,
            next_block: 11,
            canonical_head: None,
            batch_sequence: 0,
            provider_checkpoint: vec![0x54; checkpoint_bytes],
            owner_backfill_activation_block: None,
        }))
    }

    async fn activate_desired_state(
        &self,
        _preparation_id: PreparationId,
        _desired_state: &ApplyDesiredState,
        _acknowledged_cursor: Option<&Cursor>,
    ) -> Result<(), EventSourceError> {
        if self.fail_activate.load(Ordering::SeqCst) {
            return Err(EventSourceError::unavailable("forced activation failure"));
        }
        Ok(())
    }

    async fn abort_desired_state(
        &self,
        _preparation_id: PreparationId,
        _desired_state: &ApplyDesiredState,
    ) -> Result<(), EventSourceError> {
        self.aborts.fetch_add(1, Ordering::SeqCst);
        if self.fail_abort.load(Ordering::SeqCst) {
            return Err(EventSourceError::internal("forced abort failure"));
        }
        Ok(())
    }

    async fn release_session(
        &self,
        _session_id: &str,
        _chain_id: u64,
    ) -> Result<(), EventSourceError> {
        self.releases.fetch_add(1, Ordering::SeqCst);
        if self.fail_release.load(Ordering::SeqCst) {
            return Err(EventSourceError::internal("forced release failure"));
        }
        Ok(())
    }

    async fn next_delivery(
        &self,
        request: DeliveryRequest<'_>,
    ) -> Result<Option<Delivery>, EventSourceError> {
        let mut delivered = self.delivered.lock().await;
        if *delivered {
            return Ok(None);
        }
        *delivered = true;
        let mut delivery = data_delivery(request.desired_state(), request.acknowledged_cursor());
        let checkpoint_bytes = self.delivery_checkpoint_bytes.load(Ordering::SeqCst);
        if checkpoint_bytes > 0 {
            delivery
                .cursor
                .as_mut()
                .expect("cursor")
                .provider_checkpoint = vec![0x55; checkpoint_bytes];
        }
        Ok(Some(delivery))
    }

    async fn acknowledge(
        &self,
        _chain_id: u64,
        acknowledgement: &Acknowledge,
        _committed_cursor: &Cursor,
    ) -> Result<(), EventSourceError> {
        self.acknowledgements
            .lock()
            .await
            .push(acknowledgement.clone());
        Ok(())
    }
}

async fn launch<P: EventSource>(
    store: Arc<Mutex<SessionStore>>,
    provider: Arc<P>,
    poll_interval: Duration,
) -> (
    std::net::SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let service = EventService::new(store, provider, poll_interval).expect("valid poll interval");
    launch_service(service).await
}

async fn launch_service<P: EventSource>(
    service: EventService<P>,
) -> (
    std::net::SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("listener address");
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let service_shutdown = service.shutdown_handle();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service.into_server())
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_receiver.await;
                service_shutdown.shutdown();
            })
            .await
            .expect("event server");
    });
    (address, shutdown_sender, server)
}

fn desired_state(session_id: &str) -> ApplyDesiredState {
    ApplyDesiredState {
        protocol_version: PROTOCOL_VERSION,
        session_id: session_id.into(),
        chain_id: 1,
        expected_revision: 0,
        new_revision: 1,
        owners: Vec::new(),
    }
}

async fn acknowledge_delivery(transport: &mut GrpcEventTransport, delivery: &Delivery) {
    transport
        .acknowledge(Acknowledge {
            session_id: delivery.session_id.clone(),
            sequence: delivery.sequence,
            delivery_token: delivery.delivery_token.clone(),
        })
        .await
        .expect("acknowledge delivery");
}

async fn persist_data_outbox(
    session_id: &str,
) -> (Arc<Mutex<SessionStore>>, ApplyDesiredState, Delivery) {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let desired = desired_state(session_id);
    store
        .lock()
        .await
        .apply_desired_state(desired.clone())
        .expect("desired state");
    let activation = store
        .lock()
        .await
        .load(session_id, 1)
        .unwrap()
        .pending_delivery
        .unwrap();
    store
        .lock()
        .await
        .acknowledge(
            1,
            &Acknowledge {
                session_id: session_id.into(),
                sequence: activation.sequence,
                delivery_token: activation.delivery_token,
            },
        )
        .expect("ack activation");
    let acknowledged = store
        .lock()
        .await
        .load(session_id, 1)
        .unwrap()
        .acknowledged_cursor;
    let pending = data_delivery(&desired, acknowledged.as_ref());
    store
        .lock()
        .await
        .save_pending(&desired, &pending)
        .expect("persist data outbox");
    (store, desired, pending)
}

async fn persist_reorg_promise(
    session_id: &str,
) -> (
    Arc<Mutex<SessionStore>>,
    ApplyDesiredState,
    Cursor,
    BlockRef,
) {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let desired = desired_state(session_id);
    store
        .lock()
        .await
        .apply_desired_state(desired.clone())
        .expect("desired state");
    let activation = store
        .lock()
        .await
        .load(session_id, 1)
        .expect("session")
        .pending_delivery
        .expect("activation");
    store
        .lock()
        .await
        .acknowledge(
            1,
            &Acknowledge {
                session_id: session_id.into(),
                sequence: activation.sequence,
                delivery_token: activation.delivery_token,
            },
        )
        .expect("ack activation");
    let activation_cursor = store
        .lock()
        .await
        .load(session_id, 1)
        .expect("session")
        .acknowledged_cursor;
    let forward = data_delivery(&desired, activation_cursor.as_ref());
    store
        .lock()
        .await
        .save_pending(&desired, &forward)
        .expect("forward data");
    store
        .lock()
        .await
        .acknowledge(
            1,
            &Acknowledge {
                session_id: session_id.into(),
                sequence: forward.sequence,
                delivery_token: forward.delivery_token.clone(),
            },
        )
        .expect("ack forward data");

    let old_cursor = forward.cursor.as_ref().expect("forward cursor");
    let old_tip = old_cursor
        .canonical_head
        .as_ref()
        .expect("forward canonical head")
        .clone();
    let ancestor = BlockRef {
        number: 9,
        hash: old_tip.parent_hash.clone(),
        parent_hash: vec![0x0e; 32],
        timestamp: 9,
    };
    let new_tip = BlockRef {
        number: 10,
        hash: vec![0x20; 32],
        parent_hash: ancestor.hash.clone(),
        timestamp: 10,
    };
    let sequence = old_cursor.batch_sequence + 1;
    let reorg = Delivery {
        session_id: session_id.into(),
        sequence,
        query_revision: desired.new_revision,
        delivery_token: sequence.to_be_bytes().to_vec(),
        cursor: Some(Cursor {
            chain_id: desired.chain_id,
            query_revision: desired.new_revision,
            next_block: 10,
            canonical_head: Some(ancestor.clone()),
            batch_sequence: sequence,
            provider_checkpoint: b"reorg-control".to_vec(),
            owner_backfill_activation_block: old_cursor.owner_backfill_activation_block,
        }),
        payload: Some(delivery::Payload::Reorg(Reorg {
            common_ancestor: Some(ancestor),
            old_tip: Some(old_tip),
            new_tip: Some(new_tip.clone()),
        })),
        checkpoint_neutral: false,
    };
    store
        .lock()
        .await
        .save_pending(&desired, &reorg)
        .expect("reorg control");
    store
        .lock()
        .await
        .acknowledge(
            1,
            &Acknowledge {
                session_id: session_id.into(),
                sequence,
                delivery_token: reorg.delivery_token,
            },
        )
        .expect("ack reorg control");
    let persisted = store
        .lock()
        .await
        .load(session_id, 1)
        .expect("reorg promise");
    assert_eq!(persisted.expected_reorg_tip, Some(new_tip.clone()));
    assert!(persisted.pending_delivery.is_none());
    (
        store,
        desired,
        persisted.acknowledged_cursor.expect("reorg cursor"),
        new_tip,
    )
}

fn resume_position_for(delivery: &Delivery) -> SubscriberResumePosition {
    let head = delivery
        .cursor
        .as_ref()
        .and_then(|cursor| cursor.canonical_head.as_ref());
    SubscriberResumePosition::new(
        delivery.cursor.as_ref().expect("delivery cursor").chain_id,
        RuntimeBlockRef {
            number: head.map_or(0, |head| head.number),
            hash: head
                .and_then(|head| head.hash.as_slice().try_into().ok())
                .unwrap_or_default(),
            parent_hash: head.and_then(|head| head.parent_hash.as_slice().try_into().ok()),
            timestamp: head.map(|head| head.timestamp),
        },
        Vec::new(),
        Some(SubscriberDeliveryToken::new(
            delivery.delivery_token.clone(),
        )),
        (!delivery
            .cursor
            .as_ref()
            .expect("delivery cursor")
            .provider_checkpoint
            .is_empty())
        .then(|| {
            SubscriberCheckpoint::new(
                delivery
                    .cursor
                    .as_ref()
                    .expect("delivery cursor")
                    .provider_checkpoint
                    .clone(),
            )
        }),
    )
}

#[tokio::test]
async fn oversized_session_identity_is_rejected_before_leasing_or_loading_state() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let provider = Arc::new(ScriptedProvider::default());
    let service = EventService::new(store, provider, Duration::from_millis(1))
        .expect("valid poll interval")
        .with_limits({
            let mut limits = EventServiceLimits::default();
            limits.max_identifier_bytes = 4;
            limits
        });
    let metrics = service.metrics();
    let (address, shutdown_sender, server) = launch_service(service).await;

    let error =
        match GrpcEventTransport::connect(format!("http://{address}"), "too-long", 1, 0).await {
            Ok(_) => panic!("oversized session identity was accepted"),
            Err(error) => error,
        };
    assert!(matches!(
        error,
        RemoteTransportError::Remote {
            code: evm_fork_cache_event_protocol::v1::ErrorCode::ResourceExhausted,
            retryable: false,
            ..
        }
    ));
    assert_eq!(metrics.snapshot().active_sessions, 0);
    assert_eq!(metrics.snapshot().sessions_accepted, 0);

    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn grpc_sessions_preserve_the_full_unsigned_chain_id_domain() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let provider = Arc::new(ScriptedProvider::default());
    let service = EventService::new(Arc::clone(&store), provider, Duration::from_millis(1))
        .expect("valid poll interval");
    let (address, shutdown_sender, server) = launch_service(service).await;

    let mut transport =
        GrpcEventTransport::connect(format!("http://{address}"), "full-width-chain", u64::MAX, 0)
            .await
            .expect("negotiate a full-width chain id");
    let mut desired = desired_state("full-width-chain");
    desired.chain_id = u64::MAX;
    transport
        .apply_desired_state(desired.clone())
        .await
        .expect("apply state for a full-width chain id");

    assert_eq!(
        store
            .lock()
            .await
            .load("full-width-chain", u64::MAX)
            .expect("load full-width session")
            .desired_state,
        Some(desired)
    );

    drop(transport);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn authenticated_principal_cannot_take_over_a_different_session_identity() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let provider = Arc::new(ScriptedProvider::default());
    let service = EventService::new(
        Arc::clone(&store),
        Arc::clone(&provider),
        Duration::from_secs(60),
    )
    .expect("service")
    .with_authorizer(Arc::new(SessionScopedBearer));
    let (address, shutdown_sender, server) = launch_service(service).await;

    let error = match GrpcEventTransport::connect_with_authorization(
        format!("http://{address}"),
        "victim-session",
        1,
        0,
        Some("Bearer test-secret".into()),
    )
    .await
    {
        Ok(_) => panic!("cross-session takeover was accepted"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        RemoteTransportError::Remote {
            code: evm_fork_cache_event_protocol::v1::ErrorCode::Authentication,
            retryable: false,
            ..
        }
    ));
    assert!(
        store
            .lock()
            .await
            .load("victim-session", 1)
            .unwrap()
            .desired_state
            .is_none()
    );
    assert_eq!(provider.preparations.load(Ordering::SeqCst), 0);

    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[test]
fn zero_poll_interval_is_rejected_during_service_construction() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let provider = Arc::new(ScriptedProvider::default());
    assert!(matches!(
        EventService::new(store, provider, Duration::ZERO),
        Err(EventServiceConfigError::ZeroPollInterval)
    ));

    let service = EventService::new(
        Arc::new(Mutex::new(
            SessionStore::open_in_memory().expect("session store"),
        )),
        Arc::new(ScriptedProvider::default()),
        Duration::from_secs(1),
    )
    .expect("service");
    assert!(matches!(
        service
            .clone()
            .with_source_operation_timeout(Duration::ZERO),
        Err(EventServiceConfigError::ZeroSourceOperationTimeout)
    ));
    assert!(matches!(
        service.with_client_send_timeout(Duration::ZERO),
        Err(EventServiceConfigError::ZeroClientSendTimeout)
    ));
}

#[test]
fn zero_client_hello_timeout_is_rejected_during_service_configuration() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let provider = Arc::new(ScriptedProvider::default());
    let service = EventService::new(store, provider, Duration::from_secs(1)).expect("service");

    assert!(matches!(
        service.with_client_hello_timeout(Duration::ZERO),
        Err(EventServiceConfigError::ZeroClientHelloTimeout)
    ));
}

#[tokio::test]
async fn stream_that_never_sends_hello_is_closed_by_the_prelease_deadline() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let provider = Arc::new(ScriptedProvider::default());
    let service = EventService::new(store, provider, Duration::from_secs(60))
        .expect("service")
        .with_client_hello_timeout(Duration::from_millis(20))
        .expect("Hello timeout");
    let (address, shutdown_sender, server) = launch_service(service).await;
    let mut client = EventStreamClient::connect(format!("http://{address}"))
        .await
        .expect("client");
    let (_sender, receiver) = mpsc::channel(1);
    let mut inbound = client
        .session(ReceiverStream::new(receiver))
        .await
        .expect("open unnegotiated stream")
        .into_inner();

    let status = tokio::time::timeout(Duration::from_secs(1), inbound.message())
        .await
        .expect("pre-Hello deadline")
        .expect_err("unnegotiated stream must close with a status");
    assert_eq!(status.code(), tonic::Code::DeadlineExceeded);

    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn heartbeat_before_hello_is_rejected_without_negotiating_a_session() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let provider = Arc::new(ScriptedProvider::default());
    let (address, shutdown_sender, server) = launch(store, provider, Duration::from_secs(60)).await;
    let mut client = EventStreamClient::connect(format!("http://{address}"))
        .await
        .expect("client");
    let (sender, receiver) = mpsc::channel(2);
    let mut inbound = client
        .session(ReceiverStream::new(receiver))
        .await
        .expect("open unnegotiated stream")
        .into_inner();

    sender
        .send(ClientMessage {
            message: Some(client_message::Message::Heartbeat(Heartbeat {
                unix_millis: 1,
            })),
        })
        .await
        .expect("send heartbeat");
    let response = inbound
        .message()
        .await
        .expect("protocol response")
        .expect("error message");
    let error = match response.message {
        Some(server_message::Message::Error(error)) => error,
        other => panic!("expected pre-Hello protocol error, got {other:?}"),
    };
    assert_eq!(error.code, i32::from(ErrorCode::ProtocolVersion));
    assert_eq!(error.message, "Hello must be sent first");

    drop(sender);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn hung_source_prepare_is_bounded_and_does_not_commit_authority() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let service = EventService::new(
        Arc::clone(&store),
        Arc::new(HangingPrepareProvider),
        Duration::from_secs(60),
    )
    .expect("service")
    .with_source_operation_timeout(Duration::from_millis(20))
    .expect("source timeout");
    let (address, shutdown_sender, server) = launch_service(service).await;
    let mut transport = GrpcEventTransport::connect(format!("http://{address}"), "timeout", 1, 0)
        .await
        .expect("transport");
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        transport.apply_desired_state(desired_state("timeout")),
    )
    .await
    .expect("service source deadline")
    .expect_err("hung source must fail");
    assert!(matches!(
        error,
        RemoteTransportError::Remote {
            code: evm_fork_cache_event_protocol::v1::ErrorCode::SourceUnavailable,
            retryable: true,
            ..
        }
    ));
    assert!(
        store
            .lock()
            .await
            .load("timeout", 1)
            .unwrap()
            .desired_state
            .is_none()
    );

    drop(transport);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn malformed_and_aggregate_oversized_desired_states_never_reach_the_source() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let provider = Arc::new(ScriptedProvider::default());
    let service = EventService::new(store, Arc::clone(&provider), Duration::from_secs(60))
        .expect("service")
        .with_limits({
            let mut limits = EventServiceLimits::default();
            limits.max_total_interests = 1;
            limits
        });
    let (address, shutdown_sender, server) = launch_service(service).await;
    let mut transport = GrpcEventTransport::connect(format!("http://{address}"), "validated", 1, 0)
        .await
        .expect("transport");

    let mut malformed = desired_state("validated");
    malformed.protocol_version = 0;
    assert!(matches!(
        transport.apply_desired_state(malformed).await,
        Err(RemoteTransportError::Remote {
            code: evm_fork_cache_event_protocol::v1::ErrorCode::ProtocolVersion,
            ..
        })
    ));

    let interest = PortableInterest {
        kind: Some(portable_interest::Kind::Block(BlockInterest {
            mode: BlockMode::Header.into(),
        })),
    };
    let mut oversized = desired_state("validated");
    oversized.owners.push(OwnerInterests {
        owner_id: "owner".into(),
        interests: vec![interest.clone(), interest],
        backfill: None,
        canonical: false,
    });
    assert!(matches!(
        transport.apply_desired_state(oversized).await,
        Err(RemoteTransportError::Remote {
            code: evm_fork_cache_event_protocol::v1::ErrorCode::ResourceExhausted,
            ..
        })
    ));

    let mut revision_outside_storage = desired_state("validated");
    revision_outside_storage.expected_revision = i64::MAX as u64;
    revision_outside_storage.new_revision = i64::MAX as u64 + 1;
    assert!(matches!(
        transport
            .apply_desired_state(revision_outside_storage)
            .await,
        Err(RemoteTransportError::Remote {
            code: evm_fork_cache_event_protocol::v1::ErrorCode::InvalidInterest,
            retryable: false,
            ..
        })
    ));
    assert_eq!(provider.preparations.load(Ordering::SeqCst), 0);

    drop(transport);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn desired_state_quotas_are_enforced_before_deep_structural_validation() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let provider = Arc::new(ScriptedProvider::default());
    let service = EventService::new(store, Arc::clone(&provider), Duration::from_secs(60))
        .expect("service")
        .with_limits({
            let mut limits = EventServiceLimits::default();
            limits.max_owners = 1;
            limits
        });
    let (address, shutdown_sender, server) = launch_service(service).await;
    let mut transport =
        GrpcEventTransport::connect(format!("http://{address}"), "quota-first", 1, 0)
            .await
            .expect("transport");
    let mut request = desired_state("quota-first");
    request.owners = vec![
        OwnerInterests {
            owner_id: String::new(),
            interests: Vec::new(),
            backfill: None,
            canonical: false,
        },
        OwnerInterests {
            owner_id: "second".into(),
            interests: Vec::new(),
            backfill: None,
            canonical: false,
        },
    ];

    assert!(matches!(
        transport.apply_desired_state(request).await,
        Err(RemoteTransportError::Remote {
            code: evm_fork_cache_event_protocol::v1::ErrorCode::ResourceExhausted,
            ..
        })
    ));
    assert_eq!(provider.preparations.load(Ordering::SeqCst), 0);

    drop(transport);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn remote_subscriber_exposes_negotiated_runtime_capabilities() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let provider = Arc::new(ScriptedProvider::default());
    let (address, shutdown_sender, server) = launch(store, provider, Duration::from_secs(60)).await;

    let subscriber = RemoteSubscriber::connect(format!("http://{address}"), "capabilities", 1)
        .await
        .expect("connect remote subscriber");
    let capabilities = subscriber.capabilities();
    for capability in [
        SubscriberCapability::HistoricalBackfill,
        SubscriberCapability::Logs,
        SubscriberCapability::DynamicInterests,
        SubscriberCapability::ExplicitReorgs,
        SubscriberCapability::OwnerScopedDelivery,
        SubscriberCapability::DurableReplay,
        SubscriberCapability::Barriers,
    ] {
        assert!(capabilities.supports(capability), "missing {capability:?}");
    }
    assert!(!capabilities.supports(SubscriberCapability::Live));

    drop(subscriber);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn hello_advertises_capabilities_for_the_negotiated_chain() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let (address, shutdown_sender, server) =
        launch(store, Arc::new(ChainAwareProvider), Duration::from_secs(60)).await;
    let live = RemoteSubscriber::connect(format!("http://{address}"), "chain-one", 1)
        .await
        .expect("chain one");
    let historical = RemoteSubscriber::connect(format!("http://{address}"), "chain-two", 2)
        .await
        .expect("chain two");
    assert!(live.capabilities().supports(SubscriberCapability::Live));
    assert!(
        !live
            .capabilities()
            .supports(SubscriberCapability::HistoricalBackfill)
    );
    assert!(
        historical
            .capabilities()
            .supports(SubscriberCapability::HistoricalBackfill)
    );
    assert!(
        !historical
            .capabilities()
            .supports(SubscriberCapability::Live)
    );

    drop((live, historical));
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn service_prepares_then_persists_and_replays_the_activation_barrier() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let provider = Arc::new(ScriptedProvider::default());
    let (address, shutdown_sender, server) = launch(
        Arc::clone(&store),
        Arc::clone(&provider),
        Duration::from_millis(1),
    )
    .await;

    let mut transport = GrpcEventTransport::connect(format!("http://{address}"), "runtime-a", 1, 0)
        .await
        .expect("connect transport");
    let applied = transport
        .apply_desired_state(desired_state("runtime-a"))
        .await
        .expect("apply desired state");
    assert_eq!(applied.activation_sequence, 1);
    let activation = transport.next_delivery().await.unwrap().unwrap();
    assert!(matches!(
        activation.payload,
        Some(delivery::Payload::Barrier(_))
    ));
    drop(transport);

    let mut transport = GrpcEventTransport::connect(format!("http://{address}"), "runtime-a", 1, 0)
        .await
        .expect("reconnect transport");
    assert_eq!(
        transport
            .accepted()
            .desired_state
            .as_ref()
            .expect("authoritative desired state")
            .new_revision,
        1
    );
    let replay = transport.next_delivery().await.unwrap().unwrap();
    assert_eq!(replay.delivery_token, activation.delivery_token);
    acknowledge_delivery(&mut transport, &replay).await;

    let data = tokio::time::timeout(Duration::from_secs(1), transport.next_delivery())
        .await
        .expect("data timeout")
        .expect("data stream")
        .expect("data delivery");
    assert!(matches!(data.payload, Some(delivery::Payload::Data(_))));
    acknowledge_delivery(&mut transport, &data).await;

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let persisted = store.lock().await.load("runtime-a", 1).expect("load");
            if persisted
                .acknowledged_cursor
                .as_ref()
                .is_some_and(|cursor| cursor.batch_sequence == 2)
                && persisted.pending_delivery.is_none()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("durable data acknowledgement");
    let persisted = store.lock().await.load("runtime-a", 1).expect("load");
    assert_eq!(
        persisted
            .acknowledged_cursor
            .expect("acknowledged cursor")
            .batch_sequence,
        2
    );
    assert!(persisted.pending_delivery.is_none());
    assert_eq!(provider.acknowledgements.lock().await.len(), 2);

    drop(transport);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn repeated_same_stream_demand_replays_the_exact_unacknowledged_outbox_item() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let provider = Arc::new(ScriptedProvider::default());
    let service =
        EventService::new(store, provider, Duration::from_secs(60)).expect("valid poll interval");
    let metrics = service.metrics();
    let (address, shutdown_sender, server) = launch_service(service).await;
    let mut client = EventStreamClient::connect(format!("http://{address}"))
        .await
        .expect("client");
    let (sender, receiver) = mpsc::channel(8);
    let mut inbound = client
        .session(ReceiverStream::new(receiver))
        .await
        .expect("session")
        .into_inner();
    sender
        .send(ClientMessage {
            message: Some(client_message::Message::Hello(Hello {
                protocol_version: PROTOCOL_VERSION,
                session_id: "same-stream".into(),
                chain_id: 1,
                acknowledged_sequence: 0,
                pending_delivery_resume: None,
            })),
        })
        .await
        .unwrap();
    assert!(matches!(
        inbound.message().await.unwrap().unwrap().message,
        Some(server_message::Message::HelloAccepted(_))
    ));
    sender
        .send(ClientMessage {
            message: Some(client_message::Message::ApplyDesiredState(desired_state(
                "same-stream",
            ))),
        })
        .await
        .unwrap();
    assert!(matches!(
        inbound.message().await.unwrap().unwrap().message,
        Some(server_message::Message::DesiredStateApplied(_))
    ));
    sender
        .send(ClientMessage {
            message: Some(client_message::Message::DeliveryDemand(DeliveryDemand {
                session_id: "same-stream".into(),
            })),
        })
        .await
        .unwrap();
    let first = match inbound.message().await.unwrap().unwrap().message {
        Some(server_message::Message::Delivery(delivery)) => delivery,
        other => panic!("expected activation delivery, got {other:?}"),
    };
    sender
        .send(ClientMessage {
            message: Some(client_message::Message::DeliveryDemand(DeliveryDemand {
                session_id: "same-stream".into(),
            })),
        })
        .await
        .unwrap();
    let replay = tokio::time::timeout(Duration::from_secs(1), inbound.message())
        .await
        .expect("same-stream replay timeout")
        .unwrap()
        .unwrap();
    assert_eq!(
        replay.message,
        Some(server_message::Message::Delivery(first))
    );
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.desired_states_committed, 1);
    assert_eq!(snapshot.deliveries_persisted, 1);
    assert_eq!(snapshot.deliveries_replayed, 1);

    drop(sender);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn neutral_activation_reconnect_replays_only_after_each_explicit_demand() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    store
        .lock()
        .await
        .apply_desired_state(desired_state("neutral-reconnect"))
        .expect("persist neutral activation");
    let service = EventService::new(
        Arc::clone(&store),
        Arc::new(ScriptedProvider::default()),
        Duration::from_secs(60),
    )
    .expect("valid service");
    let metrics = service.metrics();
    let (address, shutdown_sender, server) = launch_service(service).await;

    let mut first =
        GrpcEventTransport::connect(format!("http://{address}"), "neutral-reconnect", 1, 0)
            .await
            .expect("first handshake");
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(metrics.snapshot().deliveries_replayed, 0);
    let activation = first
        .next_delivery()
        .await
        .expect("first demand")
        .expect("activation replay");
    assert!(activation.checkpoint_neutral);
    assert_eq!(metrics.snapshot().deliveries_replayed, 1);
    drop(first);

    let mut second = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match GrpcEventTransport::connect(
                format!("http://{address}"),
                "neutral-reconnect",
                1,
                0,
            )
            .await
            {
                Ok(transport) => break transport,
                Err(RemoteTransportError::Remote {
                    code: evm_fork_cache_event_protocol::v1::ErrorCode::SessionInUse,
                    ..
                }) => tokio::task::yield_now().await,
                Err(error) => panic!("unexpected reconnect error: {error}"),
            }
        }
    })
    .await
    .expect("session lease release");
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(metrics.snapshot().deliveries_replayed, 1);
    let replay = second
        .next_delivery()
        .await
        .expect("reconnect demand")
        .expect("activation replay");
    assert_eq!(replay, activation);
    assert_eq!(metrics.snapshot().deliveries_replayed, 2);

    drop(second);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn cancelled_ack_after_durable_commit_resumes_without_a_duplicate_wire_ack() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let provider = Arc::new(BlockingDataAckProvider::default());
    let service = EventService::new(
        Arc::clone(&store),
        Arc::clone(&provider),
        Duration::from_millis(1),
    )
    .expect("valid service");
    let metrics = service.metrics();
    let (address, shutdown_sender, server) = launch_service(service).await;
    let mut transport = GrpcEventTransport::connect(format!("http://{address}"), "lost-ack", 1, 0)
        .await
        .expect("connect transport");
    transport
        .apply_desired_state(desired_state("lost-ack"))
        .await
        .expect("apply desired state");
    let activation = transport.next_delivery().await.unwrap().unwrap();
    acknowledge_delivery(&mut transport, &activation).await;
    let delivery = transport.next_delivery().await.unwrap().unwrap();
    let acknowledgement = Acknowledge {
        session_id: delivery.session_id,
        sequence: delivery.sequence,
        delivery_token: delivery.delivery_token,
    };

    {
        let entered = provider.data_ack_entered.notified();
        tokio::pin!(entered);
        let pending_ack = transport.acknowledge(acknowledgement.clone());
        tokio::pin!(pending_ack);
        tokio::time::timeout(Duration::from_secs(1), async {
            tokio::select! {
                result = &mut pending_ack => panic!("ACK confirmed before fault injection: {result:?}"),
                () = &mut entered => {}
            }
        })
        .await
        .expect("provider acknowledgement entry");

        let persisted = store.lock().await.load("lost-ack", 1).expect("load");
        assert!(persisted.pending_delivery.is_none());
        assert_eq!(
            persisted
                .acknowledged_cursor
                .expect("durably committed cursor")
                .batch_sequence,
            acknowledgement.sequence
        );
    }

    provider.data_ack_release.notify_one();
    transport
        .acknowledge(acknowledgement)
        .await
        .expect("resume confirmation after cancellation");
    assert_eq!(provider.data_acknowledgements.load(Ordering::SeqCst), 1);
    assert_eq!(metrics.snapshot().acknowledgements_committed, 2);

    drop(transport);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn rejected_source_preparation_never_becomes_authoritative() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let provider = Arc::new(ScriptedProvider::default());
    provider.reject_prepare.store(true, Ordering::SeqCst);
    let (address, shutdown_sender, server) =
        launch(Arc::clone(&store), provider, Duration::from_secs(60)).await;
    let mut transport = GrpcEventTransport::connect(format!("http://{address}"), "rejected", 1, 0)
        .await
        .expect("connect transport");

    transport
        .apply_desired_state(desired_state("rejected"))
        .await
        .expect_err("source preparation must fail before persistence");
    let persisted = store.lock().await.load("rejected", 1).expect("load");
    assert!(persisted.desired_state.is_none());
    assert!(persisted.pending_delivery.is_none());

    drop(transport);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn failed_abort_after_prepare_rejection_closes_and_releases_the_session() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let provider = Arc::new(ScriptedProvider::default());
    provider.reject_prepare.store(true, Ordering::SeqCst);
    provider.fail_abort.store(true, Ordering::SeqCst);
    let service =
        EventService::new(store, Arc::clone(&provider), Duration::from_secs(60)).expect("service");
    let (address, shutdown_sender, server) = launch_service(service).await;
    let mut client = EventStreamClient::connect(format!("http://{address}"))
        .await
        .expect("client");
    let (sender, receiver) = mpsc::channel(8);
    let mut inbound = client
        .session(ReceiverStream::new(receiver))
        .await
        .expect("session")
        .into_inner();
    sender
        .send(ClientMessage {
            message: Some(client_message::Message::Hello(Hello {
                protocol_version: PROTOCOL_VERSION,
                session_id: "failed-prepare-abort".into(),
                chain_id: 1,
                acknowledged_sequence: 0,
                pending_delivery_resume: None,
            })),
        })
        .await
        .expect("Hello");
    assert!(matches!(
        inbound.message().await.unwrap().unwrap().message,
        Some(server_message::Message::HelloAccepted(_))
    ));
    sender
        .send(ClientMessage {
            message: Some(client_message::Message::ApplyDesiredState(desired_state(
                "failed-prepare-abort",
            ))),
        })
        .await
        .expect("apply");
    let status = inbound
        .message()
        .await
        .expect_err("failed compensation must fence the stream");
    assert_eq!(status.code(), tonic::Code::Unavailable);
    tokio::time::timeout(Duration::from_secs(1), async {
        while provider.releases.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("release after failed compensation");

    provider.reject_prepare.store(false, Ordering::SeqCst);
    provider.fail_abort.store(false, Ordering::SeqCst);
    let replacement = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match GrpcEventTransport::connect(
                format!("http://{address}"),
                "failed-prepare-abort",
                1,
                0,
            )
            .await
            {
                Ok(transport) => break transport,
                Err(RemoteTransportError::Remote {
                    code: ErrorCode::SessionInUse,
                    ..
                }) => tokio::task::yield_now().await,
                Err(error) => panic!("unexpected replacement connection failure: {error}"),
            }
        }
    })
    .await
    .expect("successful release permits one clean replacement generation");
    drop((replacement, sender));
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn failed_abort_and_release_after_store_rejection_retain_the_session_lease() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let provider = Arc::new(ScriptedProvider::default());
    provider.fail_abort.store(true, Ordering::SeqCst);
    provider.fail_release.store(true, Ordering::SeqCst);
    let service =
        EventService::new(store, Arc::clone(&provider), Duration::from_secs(60)).expect("service");
    let metrics = service.metrics();
    let (address, shutdown_sender, server) = launch_service(service).await;
    let mut client = EventStreamClient::connect(format!("http://{address}"))
        .await
        .expect("client");
    let (sender, receiver) = mpsc::channel(8);
    let mut inbound = client
        .session(ReceiverStream::new(receiver))
        .await
        .expect("session")
        .into_inner();
    sender
        .send(ClientMessage {
            message: Some(client_message::Message::Hello(Hello {
                protocol_version: PROTOCOL_VERSION,
                session_id: "failed-store-abort".into(),
                chain_id: 1,
                acknowledged_sequence: 0,
                pending_delivery_resume: None,
            })),
        })
        .await
        .expect("Hello");
    assert!(matches!(
        inbound.message().await.unwrap().unwrap().message,
        Some(server_message::Message::HelloAccepted(_))
    ));
    let mut stale = desired_state("failed-store-abort");
    stale.expected_revision = 1;
    stale.new_revision = 2;
    sender
        .send(ClientMessage {
            message: Some(client_message::Message::ApplyDesiredState(stale)),
        })
        .await
        .expect("stale apply");
    let status = inbound
        .message()
        .await
        .expect_err("failed store compensation must fence the stream");
    assert_eq!(status.code(), tonic::Code::Unavailable);
    tokio::time::timeout(Duration::from_secs(1), async {
        while metrics.snapshot().source_errors < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("abort and release failures are observed");

    let replacement =
        GrpcEventTransport::connect(format!("http://{address}"), "failed-store-abort", 1, 0).await;
    assert!(matches!(
        replacement,
        Err(RemoteTransportError::Remote {
            code: ErrorCode::SessionInUse,
            ..
        })
    ));
    assert_eq!(metrics.snapshot().active_sessions, 1);

    drop(sender);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn post_commit_activation_failure_is_uncertain_and_reconciles_persisted_authority() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let provider = Arc::new(ScriptedProvider::default());
    provider.fail_activate.store(true, Ordering::SeqCst);
    let service = EventService::new(
        Arc::clone(&store),
        Arc::clone(&provider),
        Duration::from_secs(60),
    )
    .expect("valid service");
    let metrics = service.metrics();
    let (address, shutdown_sender, server) = launch_service(service).await;
    let mut transport = GrpcEventTransport::connect_with_config(
        format!("http://{address}"),
        "activation-uncertain",
        1,
        0,
        {
            let mut config = GrpcTransportConfig::default();
            config.reconnect_attempts = 1;
            config.control_response_timeout = Duration::from_secs(1);
            config
        },
    )
    .await
    .expect("transport");
    let request = desired_state("activation-uncertain");
    assert!(matches!(
        transport.apply_desired_state(request.clone()).await,
        Err(RemoteTransportError::Unavailable(_))
    ));
    assert_eq!(
        store
            .lock()
            .await
            .load("activation-uncertain", 1)
            .expect("persisted authority")
            .desired_state
            .expect("desired state")
            .new_revision,
        1,
        "durable commit must not be presented as a definitive rejection"
    );

    provider.fail_activate.store(false, Ordering::SeqCst);
    let applied = transport
        .apply_desired_state(request)
        .await
        .expect("exact retry reconciles and activates persisted revision");
    assert_eq!(applied.revision, 1);
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.desired_states_committed, 1);
    assert_eq!(snapshot.deliveries_persisted, 1);

    drop(transport);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn idle_session_can_replace_desired_state_without_a_speculative_outbox_item() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let provider = Arc::new(ScriptedProvider::default());
    let (address, shutdown_sender, server) =
        launch(Arc::clone(&store), provider, Duration::from_millis(1)).await;
    let mut transport = GrpcEventTransport::connect(format!("http://{address}"), "dynamic", 1, 0)
        .await
        .expect("connect transport");
    transport
        .apply_desired_state(desired_state("dynamic"))
        .await
        .expect("initial desired state");
    let activation = transport.next_delivery().await.unwrap().unwrap();
    acknowledge_delivery(&mut transport, &activation).await;

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        store
            .lock()
            .await
            .load("dynamic", 1)
            .expect("idle state")
            .pending_delivery
            .is_none()
    );
    let mut replacement = desired_state("dynamic");
    replacement.expected_revision = 1;
    replacement.new_revision = 2;
    let applied = transport
        .apply_desired_state(replacement)
        .await
        .expect("replace idle desired state");
    assert_eq!(applied.revision, 2);

    drop(transport);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn service_wakes_on_provider_update_before_the_poll_fallback() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let provider = Arc::new(WakeProvider::default());
    let (address, shutdown_sender, server) =
        launch(store, Arc::clone(&provider), Duration::from_secs(60)).await;
    let mut transport =
        GrpcEventTransport::connect(format!("http://{address}"), "runtime-wake", 1, 0)
            .await
            .expect("connect transport");
    transport
        .apply_desired_state(desired_state("runtime-wake"))
        .await
        .expect("apply desired state");
    let activation = transport.next_delivery().await.unwrap().unwrap();
    acknowledge_delivery(&mut transport, &activation).await;

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        provider.attempts.load(Ordering::SeqCst),
        0,
        "an idle client must not fill its durable outbox speculatively"
    );

    {
        let delivery = transport.next_delivery();
        tokio::pin!(delivery);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut delivery)
                .await
                .is_err(),
            "the demand should remain pending while the provider is caught up"
        );
        assert!(provider.attempts.load(Ordering::SeqCst) > 0);
        provider.ready.store(true, Ordering::SeqCst);
        provider.wake.notify_one();
        tokio::time::timeout(Duration::from_secs(1), &mut delivery)
            .await
            .expect("provider wake delivery timeout")
            .expect("provider wake stream")
            .expect("provider wake delivery");
    }
    drop(transport);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn timer_poll_passes_the_committed_reorg_anchor_to_the_source() {
    let (store, desired, acknowledged, anchor) =
        persist_reorg_promise("timer-reorg-constraint").await;
    let provider = Arc::new(ConstraintProvider::new(ConstraintBehavior::ReturnNone));
    let (address, shutdown_sender, server) = launch(
        Arc::clone(&store),
        Arc::clone(&provider),
        Duration::from_millis(1),
    )
    .await;
    let mut transport = GrpcEventTransport::connect(
        format!("http://{address}"),
        &desired.session_id,
        desired.chain_id,
        acknowledged.batch_sequence,
    )
    .await
    .expect("restore reorg session");

    {
        let delivery = transport.next_delivery();
        tokio::pin!(delivery);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut delivery)
                .await
                .is_err(),
            "a caught-up constrained source should leave demand pending"
        );
        tokio::time::timeout(Duration::from_secs(1), provider.next_seen.notified())
            .await
            .expect("timer source poll");
    }
    let observations = provider.next_requests.lock().await.clone();
    assert!(!observations.is_empty());
    assert!(observations.iter().all(|observation| {
        observation.session_id == desired.session_id
            && observation.acknowledged_cursor.as_ref() == Some(&acknowledged)
            && observation.required_reorg_anchor.as_ref() == Some(&anchor)
    }));

    drop(transport);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn provider_update_poll_preserves_the_committed_reorg_anchor() {
    let (store, desired, acknowledged, anchor) =
        persist_reorg_promise("wake-reorg-constraint").await;
    let provider = Arc::new(ConstraintProvider::new(ConstraintBehavior::ReturnNone));
    let (address, shutdown_sender, server) = launch(
        Arc::clone(&store),
        Arc::clone(&provider),
        Duration::from_secs(60),
    )
    .await;
    let mut transport = GrpcEventTransport::connect(
        format!("http://{address}"),
        &desired.session_id,
        desired.chain_id,
        acknowledged.batch_sequence,
    )
    .await
    .expect("restore reorg session");

    {
        let delivery = transport.next_delivery();
        tokio::pin!(delivery);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut delivery)
                .await
                .is_err(),
            "the initial caught-up poll should leave demand pending"
        );
        tokio::time::timeout(Duration::from_secs(1), provider.next_seen.notified())
            .await
            .expect("initial source poll");
        tokio::time::timeout(Duration::from_secs(1), provider.wait_seen.notified())
            .await
            .expect("provider update wait");
        let initial_next_count = provider.next_requests.lock().await.len();
        provider.wake.notify_one();
        loop {
            if provider.next_requests.lock().await.len() > initial_next_count {
                break;
            }
            tokio::time::timeout(Duration::from_secs(1), provider.next_seen.notified())
                .await
                .expect("provider update source poll");
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut delivery)
                .await
                .is_err(),
            "a caught-up provider update should leave demand pending"
        );
    }
    let expected = RequestObservation {
        session_id: desired.session_id.clone(),
        acknowledged_cursor: Some(acknowledged),
        required_reorg_anchor: Some(anchor),
    };
    let next_requests = provider.next_requests.lock().await.clone();
    let wait_requests = provider.wait_requests.lock().await.clone();
    assert!(next_requests.len() >= 2);
    assert!(!wait_requests.is_empty());
    assert!(next_requests.iter().all(|request| request == &expected));
    assert!(wait_requests.iter().all(|request| request == &expected));

    drop(transport);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn source_that_ignores_the_reorg_anchor_cannot_poison_the_outbox() {
    let (store, desired, acknowledged, anchor) =
        persist_reorg_promise("ignored-reorg-constraint").await;
    let provider = Arc::new(ConstraintProvider::new(
        ConstraintBehavior::IgnoreConstraint,
    ));
    let (address, shutdown_sender, server) =
        launch(Arc::clone(&store), provider, Duration::from_millis(1)).await;
    let mut client = EventStreamClient::connect(format!("http://{address}"))
        .await
        .expect("client");
    let (sender, receiver) = mpsc::channel(4);
    let mut inbound = client
        .session(ReceiverStream::new(receiver))
        .await
        .expect("session")
        .into_inner();
    sender
        .send(ClientMessage {
            message: Some(client_message::Message::Hello(Hello {
                protocol_version: PROTOCOL_VERSION,
                session_id: desired.session_id.clone(),
                chain_id: desired.chain_id,
                acknowledged_sequence: acknowledged.batch_sequence,
                pending_delivery_resume: None,
            })),
        })
        .await
        .expect("Hello");
    assert!(matches!(
        inbound
            .message()
            .await
            .expect("Hello response")
            .expect("Hello"),
        evm_fork_cache_event_protocol::v1::ServerMessage {
            message: Some(server_message::Message::HelloAccepted(_))
        }
    ));
    sender
        .send(ClientMessage {
            message: Some(client_message::Message::DeliveryDemand(DeliveryDemand {
                session_id: desired.session_id.clone(),
            })),
        })
        .await
        .expect("delivery demand");
    let status = tokio::time::timeout(Duration::from_secs(1), inbound.message())
        .await
        .expect("invalid source response deadline")
        .expect_err("the stream must fail closed");
    assert_eq!(status.code(), tonic::Code::Unavailable);
    let persisted = store
        .lock()
        .await
        .load(&desired.session_id, desired.chain_id)
        .expect("session after rejected source output");
    assert_eq!(persisted.expected_reorg_tip, Some(anchor));
    assert!(persisted.pending_delivery.is_none());
    assert_eq!(persisted.acknowledged_cursor, Some(acknowledged));

    drop(sender);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn source_may_fail_closed_when_it_cannot_honor_a_reorg_anchor() {
    let (store, desired, acknowledged, anchor) =
        persist_reorg_promise("unsupported-reorg-constraint").await;
    let provider = Arc::new(ConstraintProvider::new(
        ConstraintBehavior::RejectConstraint,
    ));
    let (address, shutdown_sender, server) =
        launch(Arc::clone(&store), provider, Duration::from_millis(1)).await;
    let mut transport = GrpcEventTransport::connect(
        format!("http://{address}"),
        &desired.session_id,
        desired.chain_id,
        acknowledged.batch_sequence,
    )
    .await
    .expect("restore reorg session");

    assert!(matches!(
        transport.next_delivery().await,
        Err(RemoteTransportError::Remote {
            code: ErrorCode::UnsupportedInterest,
            retryable: false,
            ..
        })
    ));
    let persisted = store
        .lock()
        .await
        .load(&desired.session_id, desired.chain_id)
        .expect("session after unsupported constraint");
    assert_eq!(persisted.expected_reorg_tip, Some(anchor));
    assert!(persisted.pending_delivery.is_none());
    assert_eq!(persisted.acknowledged_cursor, Some(acknowledged));

    drop(transport);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn oversized_delivery_is_rejected_before_outbox_persistence() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let provider = Arc::new(ScriptedProvider::default());
    provider
        .delivery_checkpoint_bytes
        .store(4_096, Ordering::SeqCst);
    let service = EventService::new(
        Arc::clone(&store),
        Arc::clone(&provider),
        Duration::from_millis(1),
    )
    .expect("service")
    .with_limits({
        let mut limits = EventServiceLimits::default();
        limits.max_delivery_bytes = 512;
        limits
    });
    let (address, shutdown_sender, server) = launch_service(service).await;
    let mut transport = GrpcEventTransport::connect(format!("http://{address}"), "bounded", 1, 0)
        .await
        .expect("transport");
    transport
        .apply_desired_state(desired_state("bounded"))
        .await
        .expect("desired state");
    let activation = transport.next_delivery().await.unwrap().unwrap();
    acknowledge_delivery(&mut transport, &activation).await;

    assert!(matches!(
        transport.next_delivery().await,
        Err(RemoteTransportError::Remote {
            code: evm_fork_cache_event_protocol::v1::ErrorCode::ResourceExhausted,
            retryable: false,
            ..
        })
    ));
    let persisted = store.lock().await.load("bounded", 1).expect("session");
    assert!(persisted.pending_delivery.is_none());
    assert_eq!(
        persisted
            .acknowledged_cursor
            .expect("activation cursor")
            .batch_sequence,
        1
    );

    drop(transport);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn oversized_activation_cursor_is_rejected_before_authority_is_committed() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let provider = Arc::new(ScriptedProvider::default());
    provider
        .prepared_checkpoint_bytes
        .store(4_096, Ordering::SeqCst);
    let service = EventService::new(
        Arc::clone(&store),
        Arc::clone(&provider),
        Duration::from_secs(60),
    )
    .expect("service")
    .with_limits({
        let mut limits = EventServiceLimits::default();
        limits.max_delivery_bytes = 512;
        limits
    });
    let (address, shutdown_sender, server) = launch_service(service).await;
    let mut transport =
        GrpcEventTransport::connect(format!("http://{address}"), "bounded-activation", 1, 0)
            .await
            .expect("transport");

    assert!(matches!(
        transport
            .apply_desired_state(desired_state("bounded-activation"))
            .await,
        Err(RemoteTransportError::Remote {
            code: evm_fork_cache_event_protocol::v1::ErrorCode::ResourceExhausted,
            retryable: false,
            ..
        })
    ));
    let persisted = store
        .lock()
        .await
        .load("bounded-activation", 1)
        .expect("session");
    assert!(persisted.desired_state.is_none());
    assert!(persisted.pending_delivery.is_none());
    assert_eq!(provider.preparations.load(Ordering::SeqCst), 1);
    assert_eq!(
        provider.aborts.load(Ordering::SeqCst),
        1,
        "a prepared source candidate must be compensated"
    );

    drop(transport);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn legacy_outbox_item_is_replayed_even_after_the_configured_limit_is_lowered() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let desired = desired_state("legacy-oversized");
    store
        .lock()
        .await
        .apply_desired_state(desired.clone())
        .expect("desired state");
    let activation = store
        .lock()
        .await
        .load("legacy-oversized", 1)
        .unwrap()
        .pending_delivery
        .unwrap();
    store
        .lock()
        .await
        .acknowledge(
            1,
            &Acknowledge {
                session_id: "legacy-oversized".into(),
                sequence: activation.sequence,
                delivery_token: activation.delivery_token,
            },
        )
        .expect("ack activation");
    let mut oversized = data_delivery(
        &desired,
        store
            .lock()
            .await
            .load("legacy-oversized", 1)
            .unwrap()
            .acknowledged_cursor
            .as_ref(),
    );
    oversized.cursor.as_mut().unwrap().provider_checkpoint = vec![0x66; 4_096];
    store
        .lock()
        .await
        .save_pending(&desired, &oversized)
        .expect("legacy oversized pending");

    let provider = Arc::new(ScriptedProvider::default());
    let service = EventService::new(Arc::clone(&store), provider, Duration::from_millis(1))
        .expect("service")
        .with_limits({
            let mut limits = EventServiceLimits::default();
            limits.max_delivery_bytes = 512;
            limits
        });
    let (address, shutdown_sender, server) = launch_service(service).await;
    let mut transport =
        GrpcEventTransport::connect(format!("http://{address}"), "legacy-oversized", 1, 0)
            .await
            .expect("recovered session");
    let replay = transport.next_delivery().await.unwrap().unwrap();
    assert_eq!(replay, oversized);
    assert_eq!(
        store
            .lock()
            .await
            .load("legacy-oversized", 1)
            .unwrap()
            .pending_delivery,
        Some(oversized)
    );
    acknowledge_delivery(&mut transport, &replay).await;

    drop(transport);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn high_level_restore_sends_durable_sequence_and_rejects_service_regression() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    store
        .lock()
        .await
        .apply_desired_state(desired_state("restore"))
        .expect("desired state");
    let activation = store
        .lock()
        .await
        .load("restore", 1)
        .unwrap()
        .pending_delivery
        .unwrap();
    store
        .lock()
        .await
        .acknowledge(
            1,
            &Acknowledge {
                session_id: "restore".into(),
                sequence: activation.sequence,
                delivery_token: activation.delivery_token,
            },
        )
        .expect("ack activation");
    let provider = Arc::new(ScriptedProvider::default());
    let (address, shutdown_sender, server) = launch(store, provider, Duration::from_secs(60)).await;
    let position = SubscriberResumePosition::new(
        1,
        RuntimeBlockRef {
            number: 0,
            hash: Default::default(),
            parent_hash: Some(Default::default()),
            timestamp: Some(0),
        },
        Vec::new(),
        Some(SubscriberDeliveryToken::new(2_u64.to_be_bytes().to_vec())),
        None,
    );
    let error = match RemoteSubscriber::connect_from_position(
        format!("http://{address}"),
        "restore",
        1,
        &position,
    )
    .await
    {
        Ok(_) => panic!("service behind restored sequence was accepted"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        RemoteTransportError::Remote {
            code: evm_fork_cache_event_protocol::v1::ErrorCode::RevisionConflict,
            retryable: false,
            ..
        }
    ));

    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn restored_pending_delivery_replays_and_commits_without_advancing_past_it() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let desired = desired_state("crash-window");
    store
        .lock()
        .await
        .apply_desired_state(desired.clone())
        .expect("desired state");
    let activation = store
        .lock()
        .await
        .load("crash-window", 1)
        .unwrap()
        .pending_delivery
        .unwrap();
    store
        .lock()
        .await
        .acknowledge(
            1,
            &Acknowledge {
                session_id: "crash-window".into(),
                sequence: activation.sequence,
                delivery_token: activation.delivery_token,
            },
        )
        .expect("ack activation");
    let acknowledged = store
        .lock()
        .await
        .load("crash-window", 1)
        .unwrap()
        .acknowledged_cursor;
    let pending = data_delivery(&desired, acknowledged.as_ref());
    store
        .lock()
        .await
        .save_pending(&desired, &pending)
        .expect("persist crash-window delivery");

    let provider = Arc::new(ScriptedProvider::default());
    let (address, shutdown_sender, server) = launch(
        Arc::clone(&store),
        Arc::clone(&provider),
        Duration::from_secs(60),
    )
    .await;
    let position = resume_position_for(&pending);
    let mut subscriber = RemoteSubscriber::connect_from_position(
        format!("http://{address}"),
        "crash-window",
        1,
        &position,
    )
    .await
    .expect("exact pending resume proof");

    let replay = subscriber
        .next_batch()
        .await
        .expect("delivery stream")
        .expect("pending replay");
    let replay_token = replay.delivery_token().expect("delivery token").clone();
    assert_eq!(replay_token.as_bytes(), pending.delivery_token);
    assert_eq!(
        replay
            .subscriber_checkpoint()
            .expect("provider checkpoint")
            .as_bytes(),
        pending
            .cursor
            .as_ref()
            .unwrap()
            .provider_checkpoint
            .as_slice()
    );
    subscriber
        .acknowledge_delivery(replay_token)
        .await
        .expect("commit replay acknowledgement");

    let persisted = store.lock().await.load("crash-window", 1).unwrap();
    assert!(persisted.pending_delivery.is_none());
    assert_eq!(
        persisted
            .acknowledged_cursor
            .expect("committed replay cursor")
            .batch_sequence,
        pending.sequence
    );
    assert_eq!(provider.acknowledgements.lock().await.len(), 1);

    drop(subscriber);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn synchronous_core_restore_hook_renegotiates_and_replays_pending_delivery() {
    let (store, _desired, pending) = persist_data_outbox("hook-crash-window").await;
    let provider = Arc::new(ScriptedProvider::default());
    let (address, shutdown_sender, server) =
        launch(Arc::clone(&store), provider, Duration::from_secs(60)).await;
    let mut subscriber =
        RemoteSubscriber::connect(format!("http://{address}"), "hook-crash-window", 1)
            .await
            .expect("initial convenience connection");
    subscriber
        .restore_position(&resume_position_for(&pending))
        .expect("synchronous restore hand-off");

    let replay = tokio::time::timeout(Duration::from_secs(2), subscriber.next_batch())
        .await
        .expect("resume handshake timeout")
        .expect("delivery stream")
        .expect("pending replay");
    let token = replay.delivery_token().expect("delivery token").clone();
    assert_eq!(token.as_bytes(), pending.delivery_token);
    assert!(subscriber.transport().reconnect_count() >= 1);
    subscriber
        .acknowledge_delivery(token)
        .await
        .expect("commit replay acknowledgement");
    let persisted = store.lock().await.load("hook-crash-window", 1).unwrap();
    assert!(persisted.pending_delivery.is_none());
    assert_eq!(
        persisted
            .acknowledged_cursor
            .expect("committed replay cursor")
            .batch_sequence,
        pending.sequence
    );

    drop(subscriber);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn sequence_zero_discovers_an_acknowledged_runtime_then_requires_proof_reconnect() {
    let (store, _desired, delivered) = persist_data_outbox("acked-discovery").await;
    store
        .lock()
        .await
        .acknowledge(
            1,
            &Acknowledge {
                session_id: "acked-discovery".into(),
                sequence: delivered.sequence,
                delivery_token: delivered.delivery_token.clone(),
            },
        )
        .expect("install an acknowledged runtime checkpoint");
    let provider = Arc::new(ScriptedProvider::default());
    let (address, shutdown_sender, server) =
        launch(Arc::clone(&store), provider, Duration::from_millis(1)).await;

    let mut subscriber =
        RemoteSubscriber::connect(format!("http://{address}"), "acked-discovery", 1)
            .await
            .expect("sequence-zero authority discovery");
    let error = subscriber
        .next_batch()
        .await
        .expect_err("discovery cannot advance before runtime proof");
    assert!(error.to_string().contains("restore_position"));

    subscriber
        .restore_position(&resume_position_for(&delivered))
        .expect("prove the discovered runtime checkpoint");
    let next = tokio::time::timeout(Duration::from_secs(2), subscriber.next_batch())
        .await
        .expect("proof-bearing reconnect timeout")
        .expect("proof-bearing delivery stream")
        .expect("next delivery after proof reconnect");
    assert_eq!(next.chain_id(), Some(1));
    assert!(subscriber.transport().reconnect_count() >= 1);

    drop(subscriber);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn discovery_only_stream_is_server_gated_from_every_authority_mutation() {
    let (store, desired, delivered) = persist_data_outbox("raw-discovery-gate").await;
    store
        .lock()
        .await
        .acknowledge(
            1,
            &Acknowledge {
                session_id: "raw-discovery-gate".into(),
                sequence: delivered.sequence,
                delivery_token: delivered.delivery_token.clone(),
            },
        )
        .expect("install acknowledged runtime authority");
    let service = EventService::new(
        Arc::clone(&store),
        Arc::new(ScriptedProvider::default()),
        Duration::from_secs(60),
    )
    .expect("service");
    let (address, shutdown_sender, server) = launch_service(service).await;
    let mut client = EventStreamClient::connect(format!("http://{address}"))
        .await
        .expect("client");
    let (sender, receiver) = mpsc::channel(8);
    let mut inbound = client
        .session(ReceiverStream::new(receiver))
        .await
        .expect("session")
        .into_inner();
    sender
        .send(ClientMessage {
            message: Some(client_message::Message::Hello(Hello {
                protocol_version: PROTOCOL_VERSION,
                session_id: "raw-discovery-gate".into(),
                chain_id: 1,
                acknowledged_sequence: 0,
                pending_delivery_resume: None,
            })),
        })
        .await
        .expect("discovery Hello");
    assert!(matches!(
        inbound.message().await.unwrap().unwrap().message,
        Some(server_message::Message::HelloAccepted(_))
    ));

    let mut replacement = desired;
    replacement.expected_revision = 1;
    replacement.new_revision = 2;
    let forbidden = [
        client_message::Message::ApplyDesiredState(replacement),
        client_message::Message::DeliveryDemand(DeliveryDemand {
            session_id: "raw-discovery-gate".into(),
        }),
        client_message::Message::Acknowledge(Acknowledge {
            session_id: "raw-discovery-gate".into(),
            sequence: delivered.sequence,
            delivery_token: delivered.delivery_token,
        }),
    ];
    for message in forbidden {
        sender
            .send(ClientMessage {
                message: Some(message),
            })
            .await
            .expect("forbidden discovery operation");
        let response = inbound
            .message()
            .await
            .expect("response stream")
            .expect("protocol error response");
        assert!(matches!(
            response.message,
            Some(server_message::Message::Error(ref error))
                if ErrorCode::try_from(error.code) == Ok(ErrorCode::RevisionConflict)
                    && !error.retryable
        ));
    }
    assert_eq!(
        store
            .lock()
            .await
            .load("raw-discovery-gate", 1)
            .expect("unchanged discovery authority")
            .desired_state
            .expect("desired state")
            .new_revision,
        1
    );

    drop(sender);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn pending_resume_rejects_a_checkpoint_that_does_not_match_the_outbox() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let desired = desired_state("forged-resume");
    store
        .lock()
        .await
        .apply_desired_state(desired.clone())
        .expect("desired state");
    let activation = store
        .lock()
        .await
        .load("forged-resume", 1)
        .unwrap()
        .pending_delivery
        .unwrap();
    store
        .lock()
        .await
        .acknowledge(
            1,
            &Acknowledge {
                session_id: "forged-resume".into(),
                sequence: activation.sequence,
                delivery_token: activation.delivery_token,
            },
        )
        .expect("ack activation");
    let acknowledged = store
        .lock()
        .await
        .load("forged-resume", 1)
        .unwrap()
        .acknowledged_cursor;
    let pending = data_delivery(&desired, acknowledged.as_ref());
    store
        .lock()
        .await
        .save_pending(&desired, &pending)
        .expect("persist crash-window delivery");

    let provider = Arc::new(ScriptedProvider::default());
    let (address, shutdown_sender, server) = launch(store, provider, Duration::from_secs(60)).await;
    let mut forged = resume_position_for(&pending);
    forged.subscriber_checkpoint = Some(SubscriberCheckpoint::new(b"wrong-checkpoint".to_vec()));
    let error = match RemoteSubscriber::connect_from_position(
        format!("http://{address}"),
        "forged-resume",
        1,
        &forged,
    )
    .await
    {
        Ok(_) => panic!("mismatched pending-delivery proof was accepted"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        RemoteTransportError::Remote {
            code: evm_fork_cache_event_protocol::v1::ErrorCode::RevisionConflict,
            retryable: false,
            ..
        }
    ));

    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn checkpoint_neutral_authority_does_not_require_a_runtime_restore() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    store
        .lock()
        .await
        .apply_desired_state(desired_state("hook-restore"))
        .expect("desired state");
    let activation = store
        .lock()
        .await
        .load("hook-restore", 1)
        .unwrap()
        .pending_delivery
        .unwrap();
    store
        .lock()
        .await
        .acknowledge(
            1,
            &Acknowledge {
                session_id: "hook-restore".into(),
                sequence: activation.sequence,
                delivery_token: activation.delivery_token,
            },
        )
        .expect("ack activation");
    let provider = Arc::new(ScriptedProvider::default());
    let (address, shutdown_sender, server) =
        launch(store, provider, Duration::from_millis(1)).await;
    let mut subscriber = RemoteSubscriber::connect(format!("http://{address}"), "hook-restore", 1)
        .await
        .expect("initial authority handshake");
    let delivery = tokio::time::timeout(Duration::from_secs(2), subscriber.next_batch())
        .await
        .expect("delivery timeout")
        .expect("delivery stream")
        .expect("data delivery");
    assert_eq!(
        u64::from_be_bytes(
            delivery
                .delivery_token()
                .unwrap()
                .as_bytes()
                .try_into()
                .unwrap()
        ),
        2
    );
    assert_eq!(subscriber.transport().reconnect_count(), 0);

    drop(subscriber);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn only_one_live_connection_can_own_a_durable_session() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let provider = Arc::new(ScriptedProvider::default());
    let service =
        EventService::new(store, provider, Duration::from_secs(60)).expect("valid poll interval");
    let metrics = service.metrics();
    let (address, shutdown_sender, server) = launch_service(service).await;
    let transport_a = GrpcEventTransport::connect(format!("http://{address}"), "leased", 1, 0)
        .await
        .expect("connect lease owner");
    let error = match GrpcEventTransport::connect(format!("http://{address}"), "leased", 1, 0).await
    {
        Ok(_) => panic!("second connection must not duplicate-consume the session"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("another connection owns this session")
    );
    assert_eq!(metrics.snapshot().active_sessions, 1);
    assert_eq!(metrics.snapshot().lease_rejections, 1);
    drop(transport_a);

    let transport_b = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match GrpcEventTransport::connect(format!("http://{address}"), "leased", 1, 0).await {
                Ok(transport) => break transport,
                Err(_) => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .expect("lease is released when the owning stream closes");
    assert!(metrics.snapshot().sessions_accepted >= 2);
    drop(transport_b);

    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn persisted_session_quota_bounds_identity_churn_but_allows_existing_retries() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let mut limits = EventServiceLimits::default();
    limits.max_persisted_sessions = 1;
    let service = EventService::new(
        Arc::clone(&store),
        Arc::new(ScriptedProvider::default()),
        Duration::from_secs(60),
    )
    .expect("valid service")
    .with_limits(limits);
    let (address, shutdown_sender, server) = launch_service(service).await;

    let mut existing =
        GrpcEventTransport::connect(format!("http://{address}"), "stable-session", 1, 0)
            .await
            .expect("existing session");
    assert_eq!(
        existing
            .accepted()
            .service_limits
            .as_ref()
            .expect("advertised limits")
            .max_persisted_sessions,
        1
    );
    let stable = desired_state("stable-session");
    existing
        .apply_desired_state(stable.clone())
        .await
        .expect("create first durable identity");
    existing
        .apply_desired_state(stable)
        .await
        .expect("exact retry remains valid at capacity");

    let mut churned =
        GrpcEventTransport::connect(format!("http://{address}"), "churned-session", 1, 0)
            .await
            .expect("second transport may negotiate before persistence");
    assert!(matches!(
        churned
            .apply_desired_state(desired_state("churned-session"))
            .await,
        Err(RemoteTransportError::Remote {
            code: evm_fork_cache_event_protocol::v1::ErrorCode::ResourceExhausted,
            retryable: false,
            ..
        })
    ));
    assert!(
        store
            .lock()
            .await
            .load("churned-session", 1)
            .expect("load")
            .desired_state
            .is_none()
    );

    drop((existing, churned));
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn lease_is_acquired_before_source_prepare_and_release_hook_runs_on_disconnect() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    store
        .lock()
        .await
        .apply_desired_state(desired_state("lease-order"))
        .expect("persist desired state");
    let provider = Arc::new(ScriptedProvider::default());
    let (address, shutdown_sender, server) = launch(
        Arc::clone(&store),
        Arc::clone(&provider),
        Duration::from_secs(60),
    )
    .await;
    let first = GrpcEventTransport::connect(format!("http://{address}"), "lease-order", 1, 0)
        .await
        .expect("first lease");
    assert_eq!(provider.preparations.load(Ordering::SeqCst), 1);

    let second =
        GrpcEventTransport::connect(format!("http://{address}"), "lease-order", 1, 0).await;
    assert!(matches!(
        second,
        Err(RemoteTransportError::Remote {
            code: evm_fork_cache_event_protocol::v1::ErrorCode::SessionInUse,
            ..
        })
    ));
    assert_eq!(
        provider.preparations.load(Ordering::SeqCst),
        1,
        "rejected lease must not allocate provider state"
    );

    drop(first);
    tokio::time::timeout(Duration::from_secs(1), async {
        while provider.releases.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("release hook");
    assert_eq!(provider.releases.load(Ordering::SeqCst), 1);

    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn timed_out_release_fails_closed_instead_of_racing_a_new_session_generation() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let service = EventService::new(
        store,
        Arc::new(HangingReleaseProvider),
        Duration::from_secs(60),
    )
    .expect("service")
    .with_source_operation_timeout(Duration::from_millis(20))
    .expect("source timeout");
    let metrics = service.metrics();
    let (address, shutdown_sender, server) = launch_service(service).await;
    let first = GrpcEventTransport::connect(format!("http://{address}"), "stuck-release", 1, 0)
        .await
        .expect("first lease");
    drop(first);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let second =
        GrpcEventTransport::connect(format!("http://{address}"), "stuck-release", 1, 0).await;
    assert!(matches!(
        second,
        Err(RemoteTransportError::Remote {
            code: evm_fork_cache_event_protocol::v1::ErrorCode::SessionInUse,
            ..
        })
    ));
    assert_eq!(metrics.snapshot().active_sessions, 1);

    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn failed_release_fails_closed_instead_of_racing_a_new_session_generation() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let service = EventService::new(
        store,
        Arc::new(FailingReleaseProvider),
        Duration::from_secs(60),
    )
    .expect("service");
    let metrics = service.metrics();
    let (address, shutdown_sender, server) = launch_service(service).await;
    let first = GrpcEventTransport::connect(format!("http://{address}"), "failed-release", 1, 0)
        .await
        .expect("first lease");
    drop(first);
    tokio::time::timeout(Duration::from_secs(1), async {
        while metrics.snapshot().source_errors == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("release failure must be observed");

    let second =
        GrpcEventTransport::connect(format!("http://{address}"), "failed-release", 1, 0).await;
    assert!(matches!(
        second,
        Err(RemoteTransportError::Remote {
            code: evm_fork_cache_event_protocol::v1::ErrorCode::SessionInUse,
            ..
        })
    ));
    assert_eq!(metrics.snapshot().active_sessions, 1);

    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn graceful_shutdown_closes_active_streams_and_runs_bounded_source_cleanup() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let provider = Arc::new(ScriptedProvider::default());
    let service = EventService::new(store, Arc::clone(&provider), Duration::from_secs(60))
        .expect("service")
        .with_source_operation_timeout(Duration::from_millis(200))
        .expect("source timeout");
    let metrics = service.metrics();
    let (address, shutdown_sender, server) = launch_service(service).await;
    let transport = GrpcEventTransport::connect(format!("http://{address}"), "shutdown", 1, 0)
        .await
        .expect("active stream");
    assert_eq!(metrics.snapshot().active_sessions, 1);

    shutdown_sender.send(()).expect("signal shutdown");
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("graceful shutdown must not wait forever on an event stream")
        .expect("join server");
    assert_eq!(provider.releases.load(Ordering::SeqCst), 1);
    assert_eq!(metrics.snapshot().active_sessions, 0);
    drop(transport);
}

#[tokio::test]
async fn raw_source_errors_never_cross_the_grpc_protocol_boundary() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let service = EventService::new(
        store,
        Arc::new(SecretErrorProvider),
        Duration::from_millis(1),
    )
    .expect("service");
    let (address, shutdown_sender, server) = launch_service(service).await;
    let mut transport = GrpcEventTransport::connect(format!("http://{address}"), "sanitized", 1, 0)
        .await
        .expect("transport");
    transport
        .apply_desired_state(desired_state("sanitized"))
        .await
        .expect("desired state");
    let activation = transport
        .next_delivery()
        .await
        .expect("activation stream")
        .expect("activation");
    transport
        .acknowledge(Acknowledge {
            session_id: "sanitized".into(),
            sequence: activation.sequence,
            delivery_token: activation.delivery_token,
        })
        .await
        .expect("activation acknowledgement");

    let error = transport
        .next_delivery()
        .await
        .expect_err("source failure must reach the client as a sanitized class");
    let rendered = error.to_string();
    assert!(rendered.contains("temporarily unavailable"));
    assert!(!rendered.contains("super-secret"));
    assert!(!rendered.contains("indexer.invalid"));

    drop(transport);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn oversized_desired_state_is_rejected_before_it_becomes_durable() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let provider = Arc::new(ScriptedProvider::default());
    let service = EventService::new(Arc::clone(&store), provider, Duration::from_secs(60))
        .expect("valid poll interval")
        .with_limits({
            let mut limits = EventServiceLimits::default();
            limits.max_owners = 1;
            limits
        });
    let (address, shutdown_sender, server) = launch_service(service).await;
    let mut transport = GrpcEventTransport::connect(format!("http://{address}"), "limited", 1, 0)
        .await
        .expect("connect limited session");
    assert_eq!(
        transport
            .accepted()
            .service_limits
            .as_ref()
            .expect("advertised limits")
            .max_owners,
        1
    );
    let mut desired = desired_state("limited");
    desired.owners = ["a", "b"]
        .into_iter()
        .map(|owner_id| OwnerInterests {
            owner_id: owner_id.into(),
            interests: Vec::new(),
            backfill: None,
            canonical: false,
        })
        .collect();
    let error = transport
        .apply_desired_state(desired)
        .await
        .expect_err("owner limit must be enforced");
    assert!(error.to_string().contains("owner count exceeds"));
    assert!(
        store
            .lock()
            .await
            .load("limited", 1)
            .expect("load rejected session")
            .desired_state
            .is_none()
    );

    drop(transport);
    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn authorization_hook_rejects_unauthenticated_streams_and_accepts_bearer_metadata() {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let provider = Arc::new(ScriptedProvider::default());
    let service = EventService::new(store, provider, Duration::from_secs(60))
        .expect("valid poll interval")
        .with_authorizer(Arc::new(RequireBearer));
    let (address, shutdown_sender, server) = launch_service(service).await;
    assert!(matches!(
        GrpcEventTransport::connect(format!("http://{address}"), "auth", 1, 0).await,
        Err(RemoteTransportError::Remote {
            code: evm_fork_cache_event_protocol::v1::ErrorCode::Authentication,
            retryable: false,
            ..
        })
    ));
    let authenticated = GrpcEventTransport::connect_with_authorization(
        format!("http://{address}"),
        "auth",
        1,
        0,
        Some("Bearer test-secret".into()),
    )
    .await
    .expect("authorized connection");
    drop(authenticated);

    let _ = shutdown_sender.send(());
    server.await.expect("join server");
}
