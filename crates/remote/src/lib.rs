#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

mod hybrid;

pub use hybrid::{
    HYBRID_MAX_CANONICAL_HISTORY, HYBRID_MAX_HANDLER_ID_BYTES, HYBRID_MAX_RECENT_INPUTS,
    HYBRID_MAX_RECENT_OWNER_ENTRIES, HYBRID_MAX_SOURCE_CHECKPOINT_BYTES,
    HYBRID_MAX_SOURCE_DELIVERY_TOKEN_BYTES, HybridConfig, HybridPhase, HybridSource,
    HybridSubscriber,
};

use std::{
    collections::{HashMap, HashSet, VecDeque},
    marker::PhantomData,
    time::Duration,
};

use alloy_consensus::Header as ConsensusHeader;
use alloy_network::{Ethereum, Network};
use alloy_primitives::{Address, B256, Bytes, Log as PrimitiveLog, U256, keccak256};
use alloy_rlp::Decodable;
use alloy_rpc_types_eth::{Filter, FilterBlockOption, Header as RpcHeader, Log as RpcLog};
use async_trait::async_trait;
use evm_fork_cache::reactive::{
    BlockInterest as RuntimeBlockInterest, BlockInterestMode, BlockRef as RuntimeBlockRef,
    ChainControl, ChainStatus, DeliveryAudience, DeliveryScope as RuntimeDeliveryScope,
    EventSubscriber, HandlerId, InputSource, InterestOwnerSubscriber,
    LogInterest as RuntimeLogInterest, ReactiveContext, ReactiveInput, ReactiveInputBatch,
    ReactiveInputDelivery, ReactiveInputRecord, ReactiveInterest, SubscriberBackfill,
    SubscriberCapabilities, SubscriberCapability, SubscriberCheckpoint, SubscriberDeliveryToken,
    SubscriberError, SubscriberNextBatch, SubscriberOperation, SubscriberPayloadCommitment,
    SubscriberResumePosition,
};
use evm_fork_cache_event_protocol::v1::{
    Acknowledge, ApplyDesiredState, Backfill, BlockInterest, BlockMode, BlockRef as WireBlockRef,
    Capability as WireCapability, ClientMessage, Cursor, Delivery, DeliveryDemand,
    DeliveryScope as WireDeliveryScope, DesiredStateApplied, ErrorCode, FinalityKind, Hello,
    HelloAccepted, LogEvent as WireLog, LogInterest as WireLogInterest, OwnerInterests,
    PendingDeliveryResume, PortableInterest, ProtocolError, ServerMessage, SourceCapabilities,
    TopicValues, chain_event, client_message, delivery, event_stream_client::EventStreamClient,
    portable_interest, server_message,
};
use evm_fork_cache_event_protocol::{MAX_MESSAGE_SIZE_BYTES, PROTOCOL_VERSION};
use prost::Message;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

/// Compile provider-portable interests while leaving local matchers and route
/// extraction on the runtime machine.
///
/// # Errors
///
/// Returns [`RemoteError::UnsupportedFilterBlockOption`] for log filters with
/// a block range or block hash, and [`RemoteError::UnsupportedInterest`] for
/// full-block or pending-transaction interests that protocol v1 cannot carry.
pub fn compile_portable_interests<N: Network>(
    interests: &[ReactiveInterest<N>],
) -> Result<Vec<PortableInterest>, RemoteError> {
    interests
        .iter()
        .map(|interest| match interest {
            ReactiveInterest::Logs(log) => {
                if log.provider_filter.block_option != FilterBlockOption::default() {
                    return Err(RemoteError::UnsupportedFilterBlockOption);
                }
                let mut addresses: Vec<_> = log
                    .provider_filter
                    .address
                    .iter()
                    .map(|address| address.as_slice().to_vec())
                    .collect();
                addresses.sort();

                let last_topic = log
                    .provider_filter
                    .topics
                    .iter()
                    .rposition(|topic| !topic.is_empty());
                let topics = last_topic.map_or_else(Vec::new, |last| {
                    log.provider_filter.topics[..=last]
                        .iter()
                        .map(|topic| {
                            let mut values: Vec<_> = topic
                                .iter()
                                .map(|value| value.as_slice().to_vec())
                                .collect();
                            values.sort();
                            TopicValues { values }
                        })
                        .collect()
                });
                Ok(PortableInterest {
                    kind: Some(portable_interest::Kind::Log(WireLogInterest {
                        addresses,
                        topics,
                    })),
                })
            }
            ReactiveInterest::Blocks(block) => {
                let mode = match block.mode {
                    BlockInterestMode::Header => BlockMode::Header,
                    BlockInterestMode::FullBlock => {
                        return Err(RemoteError::UnsupportedInterest("full block"));
                    }
                };
                Ok(PortableInterest {
                    kind: Some(portable_interest::Kind::Block(BlockInterest {
                        mode: mode.into(),
                    })),
                })
            }
            ReactiveInterest::PendingTransactions(_) => {
                Err(RemoteError::UnsupportedInterest("pending transaction"))
            }
        })
        .collect()
}

/// Remote interest compilation or transport failure.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum RemoteError {
    /// The current service does not support this reactive interest.
    #[error("remote subscriber does not support {0} interests")]
    UnsupportedInterest(&'static str),
    /// Block ranges belong to owner backfill/cursor state, not provider filters.
    #[error("remote log interests cannot carry an Alloy filter block range or block hash")]
    UnsupportedFilterBlockOption,
    /// The service returned desired state that cannot reconstruct a subscriber.
    #[error("invalid authoritative desired state: {0}")]
    InvalidAuthoritativeState(&'static str),
}

/// Narrow transport boundary implemented by the tonic client and test doubles.
#[async_trait]
pub trait RemoteEventTransport: Send {
    /// Runtime capabilities negotiated or otherwise guaranteed by this
    /// transport's remote authority.
    ///
    /// The subscriber snapshots this value when it is constructed. Custom
    /// transports should return only capabilities they enforce end to end;
    /// the default is deliberately empty and therefore fails closed in
    /// coordinators that require historical or durable behavior.
    fn capabilities(&self) -> SubscriberCapabilities {
        SubscriberCapabilities::default()
    }

    /// Validate and seed a runtime-restored durable position before polling.
    ///
    /// # Errors
    ///
    /// Returns an error when the position is malformed, belongs to another
    /// chain/session authority, or conflicts with the transport's durable
    /// cursor or pending replay proof. Failure must leave the prior transport
    /// authority usable.
    fn restore_position(
        &mut self,
        _position: &SubscriberResumePosition,
    ) -> Result<(), RemoteTransportError> {
        Ok(())
    }

    /// Sequence durably acknowledged by the service after a restore hand-off.
    /// A transport returns `None` when it has no separate remote authority.
    fn durable_acknowledged_sequence(&self) -> Option<u64> {
        None
    }

    /// Exact durable cursor currently accepted by the remote authority.
    fn durable_acknowledged_cursor(&self) -> Option<Cursor> {
        None
    }

    /// Atomically replace the authoritative desired state.
    ///
    /// # Errors
    ///
    /// Returns an error for transport failure, protocol-invalid confirmation,
    /// or a structured service rejection. An uncertain failure must remain
    /// safely retryable with the same request.
    async fn apply_desired_state(
        &mut self,
        request: ApplyDesiredState,
    ) -> Result<DesiredStateApplied, RemoteTransportError>;

    /// Receive the next ordered data or chain-control delivery.
    ///
    /// # Errors
    ///
    /// Returns an error when the transport is unavailable or the service
    /// violates the negotiated delivery protocol. Failure must not silently
    /// consume an unacknowledged delivery.
    async fn next_delivery(&mut self) -> Result<Option<Delivery>, RemoteTransportError>;

    /// Commit one successfully ingested data batch.
    ///
    /// # Errors
    ///
    /// Returns an error for transport failure, a mismatched acknowledgement,
    /// or a service rejection. An uncertain result must be reconciled before a
    /// later delivery is exposed.
    async fn acknowledge(
        &mut self,
        acknowledgement: Acknowledge,
    ) -> Result<(), RemoteTransportError>;
}

/// Remote service or network failure.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum RemoteTransportError {
    /// The remote endpoint is unavailable or rejected the request.
    #[error("remote event service unavailable: {0}")]
    Unavailable(String),
    /// The service returned an invalid lifecycle acknowledgement.
    #[error("remote event service protocol error: {0}")]
    Protocol(String),
    /// A structured application-level rejection from the remote service.
    #[error("remote event service error {code:?} at revision {committed_revision}: {message}")]
    Remote {
        /// Stable protocol error classification.
        code: ErrorCode,
        /// Sanitized server-provided explanation.
        message: String,
        /// Service-authoritative desired-state revision at rejection time.
        committed_revision: u64,
        /// Whether reconnecting and retrying the same operation may succeed.
        retryable: bool,
    },
}

impl RemoteTransportError {
    fn operation_uncertain(&self) -> bool {
        matches!(
            self,
            Self::Unavailable(_)
                | Self::Remote {
                    retryable: true,
                    ..
                }
        )
    }
}

/// Network and control-operation deadlines for the tonic transport. Delivery
/// waits are intentionally unbounded because an idle chain/source is healthy;
/// cancellation remains immediate through the borrowed future.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct GrpcTransportConfig {
    /// Maximum time to establish one HTTP/2 channel.
    pub connect_timeout: Duration,
    /// Maximum time to establish the stream and receive `HelloAccepted`.
    pub handshake_timeout: Duration,
    /// Maximum time to wait for apply/ack confirmation. Delivery waits are not
    /// governed by this value. This is one absolute deadline for the whole
    /// control operation; heartbeats and reconnects do not extend it.
    pub control_response_timeout: Duration,
    /// Interval between HTTP/2 PING frames used to detect a half-open channel.
    pub http2_keep_alive_interval: Duration,
    /// Maximum time to wait for an HTTP/2 keepalive PING acknowledgement.
    pub http2_keep_alive_timeout: Duration,
    /// Send HTTP/2 keepalive PING frames even while no RPC messages are moving.
    /// This should normally remain enabled for a long-lived event stream.
    pub http2_keep_alive_while_idle: bool,
    /// Number of handshake attempts made after a stream disconnect.
    pub reconnect_attempts: usize,
    /// Delay before the second reconnect attempt.
    pub reconnect_initial_delay: Duration,
    /// Upper bound for exponential reconnect delay.
    pub reconnect_max_delay: Duration,
}

impl Default for GrpcTransportConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            // The shipped service may spend up to 45 seconds preparing or
            // proving source state before Hello/apply confirmation.
            handshake_timeout: Duration::from_secs(60),
            control_response_timeout: Duration::from_secs(60),
            http2_keep_alive_interval: Duration::from_secs(30),
            http2_keep_alive_timeout: Duration::from_secs(10),
            http2_keep_alive_while_idle: true,
            reconnect_attempts: 5,
            reconnect_initial_delay: Duration::from_millis(200),
            reconnect_max_delay: Duration::from_secs(3),
        }
    }
}

/// Tonic transport for one long-lived, bidirectional event session.
pub struct GrpcEventTransport {
    endpoint: String,
    session_id: String,
    chain_id: u64,
    acknowledged_sequence: u64,
    hello_acknowledged_sequence: u64,
    authorization: Option<String>,
    reconnects: u64,
    sender: mpsc::Sender<ClientMessage>,
    inbound: tonic::Streaming<ServerMessage>,
    buffered: VecDeque<ServerMessage>,
    accepted: HelloAccepted,
    pending_apply: Option<PendingOperation<ApplyDesiredState>>,
    pending_acknowledgement: Option<PendingOperation<Acknowledge>>,
    delivery_demand_sent: bool,
    in_flight_delivery: Option<Delivery>,
    config: GrpcTransportConfig,
    position_confirmation_required: bool,
    resume_handshake_required: bool,
    pending_delivery_resume: Option<PendingDeliveryResume>,
}

#[derive(Clone)]
struct PendingOperation<T> {
    value: T,
    sent: bool,
}

struct OpenedSession {
    sender: mpsc::Sender<ClientMessage>,
    inbound: tonic::Streaming<ServerMessage>,
    accepted: HelloAccepted,
}

enum StreamReceive {
    Message(Box<ServerMessage>),
    Reconnected,
}

impl GrpcEventTransport {
    /// Connect to an event service and negotiate a versioned session.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteTransportError`] when the endpoint or transport
    /// configuration is invalid, the HTTP/2 connection/handshake fails, or the
    /// service rejects or violates protocol negotiation.
    pub async fn connect(
        endpoint: impl Into<String>,
        session_id: impl Into<String>,
        chain_id: u64,
        acknowledged_sequence: u64,
    ) -> Result<Self, RemoteTransportError> {
        Self::connect_with_authorization_and_config(
            endpoint,
            session_id,
            chain_id,
            acknowledged_sequence,
            None,
            GrpcTransportConfig::default(),
        )
        .await
    }

    /// Connect with an HTTP authorization metadata value, typically
    /// `Bearer <token>`. The value is retained only for reconnects.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteTransportError`] for invalid authorization metadata,
    /// endpoint/configuration errors, connection or handshake failure, and
    /// service rejection or protocol-invalid negotiation.
    pub async fn connect_with_authorization(
        endpoint: impl Into<String>,
        session_id: impl Into<String>,
        chain_id: u64,
        acknowledged_sequence: u64,
        authorization: Option<String>,
    ) -> Result<Self, RemoteTransportError> {
        Self::connect_with_authorization_and_config(
            endpoint,
            session_id,
            chain_id,
            acknowledged_sequence,
            authorization,
            GrpcTransportConfig::default(),
        )
        .await
    }

    /// Connect using explicit network/control deadlines.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteTransportError`] when a configured deadline/retry bound
    /// is invalid, the connection or handshake fails, or negotiation is
    /// rejected or protocol-invalid.
    pub async fn connect_with_config(
        endpoint: impl Into<String>,
        session_id: impl Into<String>,
        chain_id: u64,
        acknowledged_sequence: u64,
        config: GrpcTransportConfig,
    ) -> Result<Self, RemoteTransportError> {
        Self::connect_with_authorization_and_config(
            endpoint,
            session_id,
            chain_id,
            acknowledged_sequence,
            None,
            config,
        )
        .await
    }

    /// Connect with authorization and explicit network/control deadlines.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteTransportError`] for invalid metadata or configuration,
    /// connection/handshake failure, and service rejection or
    /// protocol-invalid negotiation.
    pub async fn connect_with_authorization_and_config(
        endpoint: impl Into<String>,
        session_id: impl Into<String>,
        chain_id: u64,
        acknowledged_sequence: u64,
        authorization: Option<String>,
        config: GrpcTransportConfig,
    ) -> Result<Self, RemoteTransportError> {
        Self::connect_with_resume(
            endpoint.into(),
            session_id.into(),
            chain_id,
            acknowledged_sequence,
            authorization,
            config,
            None,
        )
        .await
    }

    async fn connect_with_resume(
        endpoint: String,
        session_id: String,
        chain_id: u64,
        acknowledged_sequence: u64,
        authorization: Option<String>,
        config: GrpcTransportConfig,
        pending_delivery_resume: Option<PendingDeliveryResume>,
    ) -> Result<Self, RemoteTransportError> {
        validate_transport_config(&config)?;
        validate_resume_proof_sequence(acknowledged_sequence, pending_delivery_resume.as_ref())?;
        let opened = open_session(
            &endpoint,
            &session_id,
            chain_id,
            acknowledged_sequence,
            pending_delivery_resume.as_ref(),
            authorization.as_deref(),
            &config,
        )
        .await?;
        let durable_sequence = accepted_sequence(&opened.accepted)?;
        let runtime_sequence = accepted_runtime_sequence(&opened.accepted);
        let accepted_runtime_position = acknowledged_sequence == runtime_sequence;
        let accepted_pending_replay = pending_delivery_resume.is_some()
            && durable_sequence.checked_add(1) == Some(acknowledged_sequence);
        Ok(Self {
            endpoint,
            session_id,
            chain_id,
            acknowledged_sequence: durable_sequence,
            hello_acknowledged_sequence: acknowledged_sequence,
            authorization,
            reconnects: 0,
            sender: opened.sender,
            inbound: opened.inbound,
            buffered: VecDeque::new(),
            accepted: opened.accepted,
            pending_apply: None,
            pending_acknowledgement: None,
            delivery_demand_sent: false,
            in_flight_delivery: None,
            config,
            position_confirmation_required: !accepted_runtime_position && !accepted_pending_replay,
            resume_handshake_required: false,
            pending_delivery_resume,
        })
    }

    /// Session metadata returned by the authoritative service.
    pub fn accepted(&self) -> &HelloAccepted {
        &self.accepted
    }

    /// Number of transport sessions successfully replaced after disconnect.
    pub const fn reconnect_count(&self) -> u64 {
        self.reconnects
    }

    async fn reconnect(&mut self) -> Result<(), RemoteTransportError> {
        let mut delay = self
            .config
            .reconnect_initial_delay
            .min(self.config.reconnect_max_delay);
        let mut last_error = None;
        let hello_sequence = self
            .pending_delivery_resume
            .as_ref()
            .map_or(Ok(self.acknowledged_sequence), resume_proof_sequence)?;
        for attempt in 0..self.config.reconnect_attempts {
            if attempt > 0 {
                tokio::time::sleep(delay).await;
                delay = delay.saturating_mul(2).min(self.config.reconnect_max_delay);
            }
            match open_session(
                &self.endpoint,
                &self.session_id,
                self.chain_id,
                hello_sequence,
                self.pending_delivery_resume.as_ref(),
                self.authorization.as_deref(),
                &self.config,
            )
            .await
            {
                Ok(opened) => {
                    self.validate_reconnected_authority(&opened.accepted)?;
                    let durable_sequence = accepted_sequence(&opened.accepted)?;
                    self.sender = opened.sender;
                    self.inbound = opened.inbound;
                    self.accepted = opened.accepted;
                    self.acknowledged_sequence = durable_sequence;
                    self.hello_acknowledged_sequence = hello_sequence;
                    self.buffered.clear();
                    if let Some(pending) = self.pending_apply.as_mut() {
                        pending.sent = false;
                    }
                    if let Some(pending) = self.pending_acknowledgement.as_mut() {
                        pending.sent = false;
                    }
                    self.delivery_demand_sent = false;
                    self.reconnects = self.reconnects.saturating_add(1);
                    return Ok(());
                }
                Err(error @ RemoteTransportError::Protocol(_)) => return Err(error),
                Err(
                    error @ RemoteTransportError::Remote {
                        retryable: false, ..
                    },
                ) => {
                    return Err(error);
                }
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            RemoteTransportError::Unavailable("session reconnect attempts exhausted".into())
        }))
    }

    async fn send(&mut self, message: client_message::Message) -> Result<(), RemoteTransportError> {
        let message = ClientMessage {
            message: Some(message),
        };
        match tokio::time::timeout(
            self.config.control_response_timeout,
            self.sender.send(message.clone()),
        )
        .await
        {
            Ok(Ok(())) => return Ok(()),
            Err(_) => {
                return Err(RemoteTransportError::Unavailable(
                    "timed out queueing event-session request".into(),
                ));
            }
            Ok(Err(_)) => {}
        }
        self.reconnect().await?;
        tokio::time::timeout(
            self.config.control_response_timeout,
            self.sender.send(message),
        )
        .await
        .map_err(|_| {
            RemoteTransportError::Unavailable(
                "timed out queueing event-session request after reconnect".into(),
            )
        })?
        .map_err(|_| RemoteTransportError::Unavailable("event session closed".into()))
    }

    async fn send_until(
        &mut self,
        message: client_message::Message,
        deadline: tokio::time::Instant,
    ) -> Result<(), RemoteTransportError> {
        let message = ClientMessage {
            message: Some(message),
        };
        match tokio::time::timeout_at(deadline, self.sender.send(message.clone())).await {
            Ok(Ok(())) => return Ok(()),
            Err(_) => return Err(control_deadline_elapsed()),
            Ok(Err(_)) => {}
        }
        tokio::time::timeout_at(deadline, self.reconnect())
            .await
            .map_err(|_| control_deadline_elapsed())??;
        tokio::time::timeout_at(deadline, self.sender.send(message))
            .await
            .map_err(|_| control_deadline_elapsed())?
            .map_err(|_| RemoteTransportError::Unavailable("event session closed".into()))
    }

    async fn ensure_resume_handshake(&mut self) -> Result<(), RemoteTransportError> {
        if self.position_confirmation_required {
            return Err(RemoteTransportError::Protocol(
                "restore_position must confirm the runtime checkpoint before an existing session can advance"
                    .into(),
            ));
        }
        if !self.resume_handshake_required {
            return Ok(());
        }
        // `restore_position` is synchronous, so close the original request
        // stream here and negotiate a fresh Hello carrying the restored
        // sequence before any control or data operation can proceed.
        let (placeholder, receiver) = mpsc::channel(1);
        drop(receiver);
        drop(std::mem::replace(&mut self.sender, placeholder));
        self.reconnect().await?;
        self.resume_handshake_required = false;
        Ok(())
    }

    fn validate_reconnected_authority(
        &self,
        accepted: &HelloAccepted,
    ) -> Result<(), RemoteTransportError> {
        let desired_matches_current = accepted.committed_revision
            == self.accepted.committed_revision
            && accepted.desired_state == self.accepted.desired_state;
        let desired_matches_pending = self.pending_apply.as_ref().is_some_and(|pending| {
            accepted.committed_revision == pending.value.new_revision
                && accepted.desired_state.as_ref() == Some(&pending.value)
        });
        if !desired_matches_current && !desired_matches_pending {
            return Err(RemoteTransportError::Protocol(
                "service authority changed unexpectedly during reconnect".into(),
            ));
        }
        if accepted.capabilities != self.accepted.capabilities {
            return Err(RemoteTransportError::Protocol(
                "service capabilities changed during reconnect".into(),
            ));
        }
        let durable_sequence = accepted_sequence(accepted)?;
        let acknowledgement_matches_pending = self
            .pending_acknowledgement
            .as_ref()
            .is_some_and(|pending| durable_sequence == pending.value.sequence);
        if durable_sequence != self.acknowledged_sequence && !acknowledgement_matches_pending {
            return Err(RemoteTransportError::Protocol(
                "service acknowledgement cursor changed unexpectedly during reconnect".into(),
            ));
        }
        Ok(())
    }

    async fn receive_stream(&mut self) -> Result<StreamReceive, RemoteTransportError> {
        match self.inbound.message().await {
            Ok(Some(message)) => Ok(StreamReceive::Message(Box::new(message))),
            Ok(None) | Err(_) => {
                self.reconnect().await?;
                Ok(StreamReceive::Reconnected)
            }
        }
    }

    async fn receive_control(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<StreamReceive, RemoteTransportError> {
        tokio::time::timeout_at(deadline, self.receive_stream())
            .await
            .map_err(|_| {
                RemoteTransportError::Unavailable(
                    "timed out waiting for event-service control confirmation".into(),
                )
            })?
    }
}

async fn open_session(
    endpoint: &str,
    session_id: &str,
    chain_id: u64,
    acknowledged_sequence: u64,
    pending_delivery_resume: Option<&PendingDeliveryResume>,
    authorization: Option<&str>,
    config: &GrpcTransportConfig,
) -> Result<OpenedSession, RemoteTransportError> {
    let endpoint = tonic::transport::Endpoint::from_shared(endpoint.to_owned())
        .map_err(|error| RemoteTransportError::Protocol(error.to_string()))?
        .connect_timeout(config.connect_timeout)
        .http2_keep_alive_interval(config.http2_keep_alive_interval)
        .keep_alive_timeout(config.http2_keep_alive_timeout)
        .keep_alive_while_idle(config.http2_keep_alive_while_idle);
    let channel = tokio::time::timeout(config.connect_timeout, endpoint.connect())
        .await
        .map_err(|_| {
            RemoteTransportError::Unavailable("event-service connection timed out".into())
        })?
        .map_err(unavailable)?;
    let mut client = EventStreamClient::<Channel>::new(channel)
        .max_decoding_message_size(MAX_MESSAGE_SIZE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_SIZE_BYTES);
    let (sender, receiver) = mpsc::channel(32);
    let mut request = tonic::Request::new(ReceiverStream::new(receiver));
    if let Some(authorization) = authorization {
        let value = authorization.parse().map_err(|_| {
            RemoteTransportError::Protocol("invalid authorization metadata value".into())
        })?;
        request.metadata_mut().insert("authorization", value);
    }
    let response = tokio::time::timeout(config.handshake_timeout, client.session(request))
        .await
        .map_err(|_| RemoteTransportError::Unavailable("event-session handshake timed out".into()))?
        .map_err(status_error)?;
    let mut inbound = response.into_inner();
    sender
        .send(ClientMessage {
            message: Some(client_message::Message::Hello(Hello {
                protocol_version: PROTOCOL_VERSION,
                session_id: session_id.to_owned(),
                chain_id,
                acknowledged_sequence,
                pending_delivery_resume: pending_delivery_resume.cloned(),
            })),
        })
        .await
        .map_err(|_| RemoteTransportError::Unavailable("session request closed".into()))?;
    let accepted = match tokio::time::timeout(config.handshake_timeout, inbound.message())
        .await
        .map_err(|_| RemoteTransportError::Unavailable("HelloAccepted timed out".into()))?
        .map_err(status_error)?
    {
        Some(ServerMessage {
            message: Some(server_message::Message::HelloAccepted(accepted)),
        }) => accepted,
        Some(ServerMessage {
            message: Some(server_message::Message::Error(error)),
        }) => return Err(protocol_error(error)),
        Some(_) => {
            return Err(RemoteTransportError::Protocol(
                "service did not begin with HelloAccepted".into(),
            ));
        }
        None => {
            return Err(RemoteTransportError::Unavailable(
                "service closed during session negotiation".into(),
            ));
        }
    };
    if accepted.protocol_version != PROTOCOL_VERSION
        || accepted.session_id != session_id
        || accepted.chain_id != chain_id
    {
        return Err(RemoteTransportError::Protocol(format!(
            "invalid HelloAccepted: version={}, session={}, chain={}",
            accepted.protocol_version, accepted.session_id, accepted.chain_id
        )));
    }
    validate_hello_authority(
        &accepted,
        session_id,
        chain_id,
        acknowledged_sequence,
        pending_delivery_resume,
    )?;
    Ok(OpenedSession {
        sender,
        inbound,
        accepted,
    })
}

fn accepted_sequence(accepted: &HelloAccepted) -> Result<u64, RemoteTransportError> {
    Ok(accepted
        .acknowledged_cursor
        .as_ref()
        .map_or(0, |cursor| cursor.batch_sequence))
}

fn accepted_runtime_cursor(accepted: &HelloAccepted) -> Option<&Cursor> {
    accepted
        .runtime_checkpoint_position
        .as_ref()
        .map_or(accepted.acknowledged_cursor.as_ref(), |position| {
            position.cursor.as_ref()
        })
}

fn accepted_runtime_sequence(accepted: &HelloAccepted) -> u64 {
    accepted_runtime_cursor(accepted).map_or(0, |cursor| cursor.batch_sequence)
}

fn validate_hello_authority(
    accepted: &HelloAccepted,
    session_id: &str,
    chain_id: u64,
    requested_acknowledged_sequence: u64,
    pending_delivery_resume: Option<&PendingDeliveryResume>,
) -> Result<(), RemoteTransportError> {
    let durable_sequence = accepted_sequence(accepted)?;
    let runtime_sequence = accepted_runtime_sequence(accepted);
    let exact_pending_resume = pending_delivery_resume.is_some()
        && durable_sequence.checked_add(1) == Some(requested_acknowledged_sequence);
    let sequence_zero_discovery =
        requested_acknowledged_sequence == 0 && pending_delivery_resume.is_none();
    let legacy_authority = accepted.runtime_checkpoint_position.is_none();
    let requested_matches_authority = if legacy_authority {
        requested_acknowledged_sequence <= durable_sequence
    } else {
        requested_acknowledged_sequence == runtime_sequence
    };
    if !requested_matches_authority && !exact_pending_resume && !sequence_zero_discovery {
        return Err(RemoteTransportError::Protocol(
            "service runtime checkpoint authority does not match the restored client position"
                .into(),
        ));
    }
    if let Some(cursor) = accepted.acknowledged_cursor.as_ref()
        && (cursor.chain_id != chain_id
            || cursor.query_revision > accepted.committed_revision
            || cursor.batch_sequence == 0)
    {
        return Err(RemoteTransportError::Protocol(
            "HelloAccepted contains an invalid acknowledged cursor".into(),
        ));
    }
    if let Some(head) = accepted
        .acknowledged_cursor
        .as_ref()
        .and_then(|cursor| cursor.canonical_head.as_ref())
    {
        runtime_block_ref(head).map_err(|error| {
            RemoteTransportError::Protocol(format!(
                "HelloAccepted canonical head is invalid: {error}"
            ))
        })?;
    }
    if let Some(runtime_cursor) = accepted_runtime_cursor(accepted) {
        if runtime_cursor.chain_id != chain_id
            || runtime_cursor.query_revision > accepted.committed_revision
            || runtime_cursor.batch_sequence == 0
            || runtime_cursor.batch_sequence > durable_sequence
            || (runtime_cursor.batch_sequence == durable_sequence
                && accepted.acknowledged_cursor.as_ref() != Some(runtime_cursor))
        {
            return Err(RemoteTransportError::Protocol(
                "HelloAccepted contains an invalid runtime checkpoint cursor".into(),
            ));
        }
        if let Some(head) = runtime_cursor.canonical_head.as_ref() {
            runtime_block_ref(head).map_err(|error| {
                RemoteTransportError::Protocol(format!(
                    "HelloAccepted runtime checkpoint head is invalid: {error}"
                ))
            })?;
        }
    }
    match accepted.desired_state.as_ref() {
        Some(desired)
            if desired.protocol_version == PROTOCOL_VERSION
                && desired.session_id == session_id
                && desired.chain_id == chain_id
                && desired.new_revision == accepted.committed_revision
                && desired.expected_revision.checked_add(1)
                    == Some(accepted.committed_revision) => {}
        None if accepted.committed_revision == 0 => {}
        _ => {
            return Err(RemoteTransportError::Protocol(
                "HelloAccepted desired state does not match its authority".into(),
            ));
        }
    }
    Ok(())
}

fn resume_sequence(position: &SubscriberResumePosition) -> Result<u64, RemoteTransportError> {
    let Some(token) = position.delivery_token.as_ref() else {
        if position.subscriber_checkpoint.is_some() {
            return Err(RemoteTransportError::Protocol(
                "restored subscriber checkpoint is missing its delivery token".into(),
            ));
        }
        return Ok(0);
    };
    let bytes = token.as_bytes().try_into().map_err(|_| {
        RemoteTransportError::Protocol(
            "remote delivery tokens must be exactly eight-byte big-endian sequences".into(),
        )
    })?;
    Ok(u64::from_be_bytes(bytes))
}

fn resume_proof_sequence(resume: &PendingDeliveryResume) -> Result<u64, RemoteTransportError> {
    let bytes = resume.delivery_token.as_slice().try_into().map_err(|_| {
        RemoteTransportError::Protocol(
            "pending-delivery resume tokens must be exactly eight-byte big-endian sequences".into(),
        )
    })?;
    Ok(u64::from_be_bytes(bytes))
}

fn validate_resume_proof_sequence(
    acknowledged_sequence: u64,
    resume: Option<&PendingDeliveryResume>,
) -> Result<(), RemoteTransportError> {
    if let Some(resume) = resume
        && resume_proof_sequence(resume)? != acknowledged_sequence
    {
        return Err(RemoteTransportError::Protocol(
            "pending-delivery resume proof does not match its Hello sequence".into(),
        ));
    }
    Ok(())
}

fn pending_delivery_resume(
    position: &SubscriberResumePosition,
) -> Result<Option<PendingDeliveryResume>, RemoteTransportError> {
    let sequence = resume_sequence(position)?;
    let Some(token) = position.delivery_token.as_ref() else {
        return Ok(None);
    };
    let resume = PendingDeliveryResume {
        delivery_token: token.as_bytes().to_vec(),
        provider_checkpoint: position
            .subscriber_checkpoint
            .as_ref()
            .and_then(|checkpoint| {
                (!checkpoint.as_bytes().is_empty()).then(|| checkpoint.as_bytes().to_vec())
            }),
        coverage_head: Some(wire_block_ref(&position.coverage_head)?),
    };
    validate_resume_proof_sequence(sequence, Some(&resume))?;
    Ok(Some(resume))
}

fn resume_proof_matches_cursor(resume: &PendingDeliveryResume, cursor: &Cursor) -> bool {
    cursor.provider_checkpoint == resume.provider_checkpoint.as_deref().unwrap_or_default()
        && cursor.canonical_head.as_ref() == resume.coverage_head.as_ref()
}

fn wire_block_ref(block: &RuntimeBlockRef) -> Result<WireBlockRef, RemoteTransportError> {
    let parent_hash = block.parent_hash.ok_or_else(|| {
        RemoteTransportError::Protocol(
            "pending-delivery resume coverage is missing its parent hash".into(),
        )
    })?;
    let timestamp = block.timestamp.ok_or_else(|| {
        RemoteTransportError::Protocol(
            "pending-delivery resume coverage is missing its timestamp".into(),
        )
    })?;
    Ok(WireBlockRef {
        number: block.number,
        hash: block.hash.as_slice().to_vec(),
        parent_hash: parent_hash.as_slice().to_vec(),
        timestamp,
    })
}

fn validate_resume_position(
    accepted: &HelloAccepted,
    position: &SubscriberResumePosition,
) -> Result<(), RemoteTransportError> {
    let sequence = resume_sequence(position)?;
    let durable_sequence = accepted_sequence(accepted)?;
    let runtime_sequence = accepted_runtime_sequence(accepted);
    let resumes_pending = durable_sequence.checked_add(1) == Some(sequence);
    if runtime_sequence != sequence && !resumes_pending {
        return Err(RemoteTransportError::Protocol(format!(
            "service runtime checkpoint sequence {runtime_sequence} does not match restored runtime sequence {sequence}"
        )));
    }
    if resumes_pending {
        return Ok(());
    }
    let cursor = accepted_runtime_cursor(accepted);
    let restored_checkpoint = position.subscriber_checkpoint.as_ref();
    let checkpoint_matches = match cursor {
        None => restored_checkpoint.is_none(),
        Some(cursor) if cursor.provider_checkpoint.is_empty() => restored_checkpoint.is_none(),
        Some(cursor) => restored_checkpoint
            .is_some_and(|checkpoint| checkpoint.as_bytes() == cursor.provider_checkpoint),
    };
    if !checkpoint_matches {
        return Err(RemoteTransportError::Protocol(
            "service provider checkpoint does not match the restored runtime checkpoint".into(),
        ));
    }
    let service_head = cursor.and_then(|cursor| cursor.canonical_head.as_ref());
    let coverage_matches = service_head
        .map(runtime_block_ref)
        .transpose()
        .map_err(|error| {
            RemoteTransportError::Protocol(format!(
                "service runtime checkpoint head is invalid: {error}"
            ))
        })?
        .as_ref()
        == Some(&position.coverage_head);
    if !coverage_matches {
        return Err(RemoteTransportError::Protocol(
            "service canonical head does not exactly match restored runtime coverage".into(),
        ));
    }
    Ok(())
}

#[async_trait]
impl RemoteEventTransport for GrpcEventTransport {
    fn capabilities(&self) -> SubscriberCapabilities {
        runtime_capabilities(self.accepted.capabilities.as_ref())
    }

    fn restore_position(
        &mut self,
        position: &SubscriberResumePosition,
    ) -> Result<(), RemoteTransportError> {
        validate_resume_position(&self.accepted, position)?;
        let sequence = resume_sequence(position)?;
        let resume = pending_delivery_resume(position)?;
        let proof_changed = self.pending_delivery_resume != resume;
        self.acknowledged_sequence = accepted_sequence(&self.accepted)?;
        self.in_flight_delivery = None;
        self.resume_handshake_required =
            self.hello_acknowledged_sequence != sequence || proof_changed;
        self.pending_delivery_resume = resume;
        self.position_confirmation_required = false;
        Ok(())
    }

    fn durable_acknowledged_sequence(&self) -> Option<u64> {
        Some(self.acknowledged_sequence)
    }

    fn durable_acknowledged_cursor(&self) -> Option<Cursor> {
        self.accepted.acknowledged_cursor.clone()
    }

    async fn apply_desired_state(
        &mut self,
        request: ApplyDesiredState,
    ) -> Result<DesiredStateApplied, RemoteTransportError> {
        let deadline = tokio::time::Instant::now()
            .checked_add(self.config.control_response_timeout)
            .ok_or_else(|| {
                RemoteTransportError::Protocol("control response deadline is out of range".into())
            })?;
        tokio::time::timeout_at(deadline, self.ensure_resume_handshake())
            .await
            .map_err(|_| control_deadline_elapsed())??;
        if self.pending_acknowledgement.is_some() {
            return Err(RemoteTransportError::Protocol(
                "an acknowledgement must be reconciled before desired state can change".into(),
            ));
        }
        match self.pending_apply.as_ref() {
            Some(pending) if pending.value != request => {
                return Err(RemoteTransportError::Protocol(
                    "a different desired-state operation is still pending".into(),
                ));
            }
            None => {
                self.pending_apply = Some(PendingOperation {
                    value: request.clone(),
                    sent: false,
                });
            }
            Some(_) => {}
        }
        loop {
            if !self
                .pending_apply
                .as_ref()
                .is_some_and(|pending| pending.sent)
            {
                self.send_until(
                    client_message::Message::ApplyDesiredState(request.clone()),
                    deadline,
                )
                .await?;
                self.pending_apply
                    .as_mut()
                    .expect("pending apply exists")
                    .sent = true;
            }
            let message = match self.receive_control(deadline).await? {
                StreamReceive::Message(message) => *message,
                StreamReceive::Reconnected => continue,
            };
            match message.message {
                Some(server_message::Message::DesiredStateApplied(applied)) => {
                    if applied.session_id != request.session_id
                        || applied.revision != request.new_revision
                        || applied.activation_sequence
                            != self.acknowledged_sequence.checked_add(1).ok_or_else(|| {
                                RemoteTransportError::Protocol(
                                    "delivery sequence overflow during desired-state apply".into(),
                                )
                            })?
                    {
                        return Err(RemoteTransportError::Protocol(
                            "desired-state confirmation does not match the request".into(),
                        ));
                    }
                    self.pending_apply = None;
                    self.accepted.committed_revision = request.new_revision;
                    self.accepted.desired_state = Some(request.clone());
                    return Ok(applied);
                }
                Some(server_message::Message::Delivery(_)) => self.buffered.push_back(message),
                Some(server_message::Message::AcknowledgementCommitted(_)) => {
                    return Err(RemoteTransportError::Protocol(
                        "unexpected acknowledgement while applying desired state".into(),
                    ));
                }
                Some(server_message::Message::Error(error)) => {
                    let error = protocol_error(error);
                    if !error.operation_uncertain() {
                        self.pending_apply = None;
                    }
                    return Err(error);
                }
                Some(server_message::Message::Heartbeat(_)) => {}
                Some(server_message::Message::HelloAccepted(_)) | None => {
                    return Err(RemoteTransportError::Protocol(
                        "unexpected message while applying desired state".into(),
                    ));
                }
            }
        }
    }

    async fn next_delivery(&mut self) -> Result<Option<Delivery>, RemoteTransportError> {
        self.ensure_resume_handshake().await?;
        if self.pending_apply.is_some() || self.pending_acknowledgement.is_some() {
            return Err(RemoteTransportError::Protocol(
                "a cancelled control operation must be retried before receiving delivery".into(),
            ));
        }
        if let Some(delivery) = self.in_flight_delivery.as_ref() {
            return Ok(Some(delivery.clone()));
        }
        loop {
            let message = if let Some(message) = self.buffered.pop_front() {
                message
            } else {
                if !self.delivery_demand_sent {
                    self.send(client_message::Message::DeliveryDemand(DeliveryDemand {
                        session_id: self.session_id.clone(),
                    }))
                    .await?;
                    self.delivery_demand_sent = true;
                }
                match self.receive_stream().await? {
                    StreamReceive::Message(message) => *message,
                    StreamReceive::Reconnected => continue,
                }
            };
            match message.message {
                Some(server_message::Message::Delivery(delivery)) => {
                    self.delivery_demand_sent = false;
                    let expected_sequence =
                        self.acknowledged_sequence.checked_add(1).ok_or_else(|| {
                            RemoteTransportError::Protocol("delivery sequence overflow".into())
                        })?;
                    validate_transport_delivery(
                        DeliveryAuthority {
                            session_id: &self.session_id,
                            chain_id: self.chain_id,
                            revision: self.accepted.committed_revision,
                            expected_sequence,
                            acknowledged_cursor: self.accepted.acknowledged_cursor.as_ref(),
                            activation_baseline: desired_state_global_baseline(
                                self.accepted.desired_state.as_ref(),
                            ),
                            requires_open_backfill_boundary: desired_state_has_open_backfill(
                                self.accepted.desired_state.as_ref(),
                            ),
                        },
                        &delivery,
                    )?;
                    self.in_flight_delivery = Some(delivery.clone());
                    return Ok(Some(delivery));
                }
                Some(server_message::Message::Heartbeat(_)) => {}
                Some(server_message::Message::Error(error)) => {
                    self.delivery_demand_sent = false;
                    return Err(protocol_error(error));
                }
                Some(server_message::Message::DesiredStateApplied(_))
                | Some(server_message::Message::AcknowledgementCommitted(_))
                | Some(server_message::Message::HelloAccepted(_))
                | None => {
                    return Err(RemoteTransportError::Protocol(
                        "unexpected control message on data stream".into(),
                    ));
                }
            }
        }
    }

    async fn acknowledge(
        &mut self,
        acknowledgement: Acknowledge,
    ) -> Result<(), RemoteTransportError> {
        let deadline = tokio::time::Instant::now()
            .checked_add(self.config.control_response_timeout)
            .ok_or_else(|| {
                RemoteTransportError::Protocol("control response deadline is out of range".into())
            })?;
        tokio::time::timeout_at(deadline, self.ensure_resume_handshake())
            .await
            .map_err(|_| control_deadline_elapsed())??;
        if self.pending_apply.is_some() {
            return Err(RemoteTransportError::Protocol(
                "a desired-state operation must be reconciled before acknowledgement".into(),
            ));
        }
        let (expected_cursor, restored_resume) =
            if let Some(delivery) = self.in_flight_delivery.as_ref() {
                if acknowledgement.session_id != delivery.session_id
                    || acknowledgement.sequence != delivery.sequence
                    || acknowledgement.delivery_token != delivery.delivery_token
                {
                    return Err(RemoteTransportError::Protocol(
                        "acknowledgement does not match the in-flight delivery".into(),
                    ));
                }
                let cursor = delivery.cursor.clone().ok_or_else(|| {
                    RemoteTransportError::Protocol("in-flight delivery cursor is missing".into())
                })?;
                (Some(cursor), None)
            } else {
                let resume = self.pending_delivery_resume.clone().ok_or_else(|| {
                    RemoteTransportError::Protocol(
                        "acknowledgement requires an in-flight or exactly restored delivery".into(),
                    )
                })?;
                if acknowledgement.session_id != self.session_id
                    || acknowledgement.sequence != resume_proof_sequence(&resume)?
                    || acknowledgement.delivery_token != resume.delivery_token
                {
                    return Err(RemoteTransportError::Protocol(
                        "acknowledgement does not match the restored pending delivery".into(),
                    ));
                }
                (None, Some(resume))
            };
        match self.pending_acknowledgement.as_ref() {
            Some(pending) if pending.value != acknowledgement => {
                return Err(RemoteTransportError::Protocol(
                    "a different acknowledgement is still pending".into(),
                ));
            }
            None => {
                self.pending_acknowledgement = Some(PendingOperation {
                    value: acknowledgement.clone(),
                    sent: false,
                });
            }
            Some(_) => {}
        }
        loop {
            if !self
                .pending_acknowledgement
                .as_ref()
                .is_some_and(|pending| pending.sent)
            {
                self.send_until(
                    client_message::Message::Acknowledge(acknowledgement.clone()),
                    deadline,
                )
                .await?;
                self.pending_acknowledgement
                    .as_mut()
                    .expect("pending acknowledgement exists")
                    .sent = true;
            }
            let message = match self.receive_control(deadline).await? {
                StreamReceive::Message(message) => *message,
                StreamReceive::Reconnected => continue,
            };
            match message.message {
                Some(server_message::Message::AcknowledgementCommitted(committed)) => {
                    let cursor = committed.cursor.as_ref().ok_or_else(|| {
                        RemoteTransportError::Protocol(
                            "acknowledgement confirmation is missing its cursor".into(),
                        )
                    })?;
                    if committed.session_id != acknowledgement.session_id
                        || committed.sequence != acknowledgement.sequence
                        || cursor.chain_id != self.chain_id
                        || cursor.query_revision != self.accepted.committed_revision
                        || cursor.batch_sequence != acknowledgement.sequence
                        || expected_cursor
                            .as_ref()
                            .is_some_and(|expected| expected != cursor)
                        || restored_resume
                            .as_ref()
                            .is_some_and(|resume| !resume_proof_matches_cursor(resume, cursor))
                    {
                        return Err(RemoteTransportError::Protocol(
                            "acknowledgement confirmation does not exactly match the request and delivered cursor"
                                .into(),
                        ));
                    }
                    self.pending_acknowledgement = None;
                    self.acknowledged_sequence = committed.sequence;
                    self.accepted.acknowledged_cursor = committed.cursor;
                    self.in_flight_delivery = None;
                    if self
                        .pending_delivery_resume
                        .as_ref()
                        .and_then(|resume| resume_proof_sequence(resume).ok())
                        .is_some_and(|sequence| sequence <= committed.sequence)
                    {
                        self.pending_delivery_resume = None;
                    }
                    return Ok(());
                }
                Some(server_message::Message::Delivery(delivery))
                    if delivery.sequence == acknowledgement.sequence
                        && delivery.delivery_token == acknowledgement.delivery_token =>
                {
                    // A reconnect replays the still-pending item before it
                    // can receive this idempotent acknowledgement retry.
                }
                Some(server_message::Message::Error(error)) => {
                    let error = protocol_error(error);
                    if !error.operation_uncertain() {
                        self.pending_acknowledgement = None;
                    }
                    return Err(error);
                }
                Some(server_message::Message::Heartbeat(_)) => {}
                Some(server_message::Message::Delivery(_))
                | Some(server_message::Message::DesiredStateApplied(_))
                | Some(server_message::Message::HelloAccepted(_))
                | None => {
                    return Err(RemoteTransportError::Protocol(
                        "unexpected message while acknowledging delivery".into(),
                    ));
                }
            }
        }
    }
}

fn unavailable(error: impl std::fmt::Display) -> RemoteTransportError {
    RemoteTransportError::Unavailable(error.to_string())
}

fn control_deadline_elapsed() -> RemoteTransportError {
    RemoteTransportError::Unavailable(
        "timed out waiting for event-service control operation".into(),
    )
}

fn status_error(status: tonic::Status) -> RemoteTransportError {
    match status.code() {
        tonic::Code::Unauthenticated | tonic::Code::PermissionDenied => {
            RemoteTransportError::Remote {
                code: ErrorCode::Authentication,
                message: "event session authentication failed".into(),
                committed_revision: 0,
                retryable: false,
            }
        }
        tonic::Code::ResourceExhausted => RemoteTransportError::Remote {
            code: ErrorCode::ResourceExhausted,
            message: "event service resource limit exceeded".into(),
            committed_revision: 0,
            retryable: true,
        },
        _ => RemoteTransportError::Unavailable(status.to_string()),
    }
}

fn validate_transport_config(config: &GrpcTransportConfig) -> Result<(), RemoteTransportError> {
    if config.connect_timeout.is_zero()
        || config.handshake_timeout.is_zero()
        || config.control_response_timeout.is_zero()
        || config.http2_keep_alive_interval.is_zero()
        || config.http2_keep_alive_timeout.is_zero()
        || config.reconnect_initial_delay.is_zero()
        || config.reconnect_max_delay.is_zero()
        || config.reconnect_attempts == 0
    {
        return Err(RemoteTransportError::Protocol(
            "transport timeouts, keepalive/reconnect durations, and reconnect attempts must be greater than zero"
                .into(),
        ));
    }
    Ok(())
}

fn protocol_error(error: ProtocolError) -> RemoteTransportError {
    match ErrorCode::try_from(error.code) {
        Ok(ErrorCode::Unspecified) | Err(_) => RemoteTransportError::Protocol(format!(
            "service returned unknown error code {}",
            error.code
        )),
        Ok(code) => RemoteTransportError::Remote {
            code,
            message: error.message,
            committed_revision: error.committed_revision,
            retryable: error.retryable,
        },
    }
}

#[derive(Clone, Copy)]
struct DeliveryAuthority<'a> {
    session_id: &'a str,
    chain_id: u64,
    revision: u64,
    expected_sequence: u64,
    acknowledged_cursor: Option<&'a Cursor>,
    activation_baseline: Option<&'a WireBlockRef>,
    requires_open_backfill_boundary: bool,
}

fn validate_transport_delivery(
    authority: DeliveryAuthority<'_>,
    delivery: &Delivery,
) -> Result<(), RemoteTransportError> {
    let cursor = delivery.cursor.as_ref().ok_or_else(|| {
        RemoteTransportError::Protocol("delivery is missing its authoritative cursor".into())
    })?;
    if delivery.session_id != authority.session_id
        || delivery.sequence != authority.expected_sequence
        || delivery.query_revision != authority.revision
        || delivery.delivery_token != delivery.sequence.to_be_bytes()
        || delivery.payload.is_none()
        || cursor.chain_id != authority.chain_id
        || cursor.query_revision != authority.revision
        || cursor.batch_sequence != delivery.sequence
    {
        return Err(RemoteTransportError::Protocol(
            "delivery identity, revision, sequence, token, payload, or cursor is invalid".into(),
        ));
    }
    validate_wire_delivery_progress(
        authority.acknowledged_cursor,
        authority.activation_baseline,
        delivery,
        authority.requires_open_backfill_boundary,
    )
    .map_err(|message| RemoteTransportError::Protocol(message.into()))?;
    Ok(())
}

fn desired_state_has_open_backfill(desired_state: Option<&ApplyDesiredState>) -> bool {
    desired_state.is_some_and(|desired_state| {
        desired_state.owners.iter().any(|owner| {
            owner
                .backfill
                .as_ref()
                .is_some_and(|backfill| backfill.to_block_excl.is_none())
        })
    })
}

fn desired_state_global_baseline(
    desired_state: Option<&ApplyDesiredState>,
) -> Option<&WireBlockRef> {
    desired_state?
        .owners
        .iter()
        .find(|owner| owner.canonical)
        .and_then(|owner| owner.backfill.as_ref())
        .and_then(|backfill| backfill.retained_baseline.as_ref())
}

fn validate_wire_delivery_progress(
    acknowledged_cursor: Option<&Cursor>,
    activation_baseline: Option<&WireBlockRef>,
    delivery: &Delivery,
    requires_open_backfill_boundary: bool,
) -> Result<(), &'static str> {
    let cursor = delivery
        .cursor
        .as_ref()
        .ok_or("delivery cursor is missing")?;
    let payload = delivery
        .payload
        .as_ref()
        .ok_or("delivery payload is missing")?;
    if matches!(payload, delivery::Payload::Barrier(barrier) if barrier.id.is_empty()) {
        return Err("barrier identifier is empty");
    }
    let is_reorg = matches!(payload, delivery::Payload::Reorg(_));
    let is_activation = is_wire_activation(acknowledged_cursor, activation_baseline, delivery);
    let is_scan_progress = is_wire_cursor_progress(delivery);
    if delivery.checkpoint_neutral != (is_activation || is_scan_progress) {
        return Err(
            "checkpoint-neutral marker must identify exactly an activation or scan-only progress barrier",
        );
    }
    if is_activation {
        if requires_open_backfill_boundary && cursor.owner_backfill_activation_block.is_none() {
            return Err("open owner backfill activation is missing its portable boundary");
        }
        if cursor
            .owner_backfill_activation_block
            .is_some_and(|activation| activation < cursor.next_block)
        {
            return Err("owner backfill activation boundary precedes its scan cursor");
        }
    } else if acknowledged_cursor.is_some_and(|acknowledged| {
        acknowledged.query_revision == cursor.query_revision
            && acknowledged.owner_backfill_activation_block
                != cursor.owner_backfill_activation_block
    }) {
        return Err("owner backfill activation boundary changed within one revision");
    }
    if !is_reorg
        && acknowledged_cursor.is_some_and(|acknowledged| {
            (cursor.next_block < acknowledged.next_block && !is_activation)
                || wire_head_regresses_or_changes(acknowledged, cursor)
        })
    {
        return Err("non-reorg delivery cursor regresses canonical progress");
    }
    if let delivery::Payload::Reorg(reorg) = payload {
        let ancestor = reorg
            .common_ancestor
            .as_ref()
            .ok_or("reorg is missing its common ancestor")?;
        let old_tip = reorg
            .old_tip
            .as_ref()
            .ok_or("reorg is missing its old tip")?;
        let new_tip = reorg
            .new_tip
            .as_ref()
            .ok_or("reorg is missing its new tip")?;
        validate_wire_reorg_shape(ancestor, old_tip, new_tip)?;
        if acknowledged_cursor.and_then(|cursor| cursor.canonical_head.as_ref()) != Some(old_tip)
            || old_tip.number <= ancestor.number
            || new_tip.number <= ancestor.number
            || ancestor.number.checked_add(1) != Some(cursor.next_block)
        {
            return Err("reorg ancestry, prior tip, or cursor successor is invalid");
        }
    }

    let expected_head = match payload {
        delivery::Payload::Data(data) => {
            if data.records.is_empty() {
                return Err("data delivery contains no records");
            }
            let mut head = None;
            let mut last_number = None;
            let mut only_owner_catchup = true;
            let mut highest_canonical_record = None;
            let mut highest_canonical_coverage = None;
            for record in &data.records {
                let scope = WireDeliveryScope::try_from(record.scope)
                    .map_err(|_| "data record has an unknown delivery scope")?;
                if scope == WireDeliveryScope::Unspecified {
                    return Err("data record has an unspecified delivery scope");
                }
                if scope == WireDeliveryScope::OwnerCatchup {
                    continue;
                }
                only_owner_catchup = false;
                let Some(event) = record.event.as_ref().and_then(|event| event.event.as_ref())
                else {
                    return Err("data record is missing its chain event");
                };
                let (record_number, block) = match event {
                    chain_event::Event::BlockHeader(header) => (
                        header.block.as_ref().map(|block| block.number),
                        header.block.as_ref(),
                    ),
                    chain_event::Event::BlockProgress(progress) => (
                        progress.block.as_ref().map(|block| block.number),
                        progress.block.as_ref(),
                    ),
                    chain_event::Event::Log(log) => (Some(log.block_number), None),
                };
                let record_number =
                    record_number.ok_or("canonical block record is missing its block reference")?;
                highest_canonical_record = Some(
                    highest_canonical_record
                        .map_or(record_number, |known: u64| known.max(record_number)),
                );
                if let Some(block) = block {
                    highest_canonical_coverage = Some(
                        highest_canonical_coverage
                            .map_or(block.number, |known: u64| known.max(block.number)),
                    );
                    if last_number.is_some_and(|previous| block.number < previous) {
                        return Err("canonical block records are not ordered by height");
                    }
                    last_number = Some(block.number);
                    head = Some(block);
                }
            }
            if !only_owner_catchup
                && highest_canonical_record
                    .zip(highest_canonical_coverage)
                    .is_none_or(|(record, coverage)| coverage < record)
            {
                return Err(
                    "canonical data is not certified by a final block identity at or above every canonical record",
                );
            }
            if only_owner_catchup {
                acknowledged_cursor.and_then(|acknowledged| acknowledged.canonical_head.as_ref())
            } else {
                head
            }
        }
        delivery::Payload::Reorg(reorg) => reorg.common_ancestor.as_ref(),
        delivery::Payload::Finality(_) => {
            acknowledged_cursor.and_then(|acknowledged| acknowledged.canonical_head.as_ref())
        }
        delivery::Payload::Barrier(barrier) => barrier
            .block
            .as_ref()
            .or_else(|| {
                acknowledged_cursor.and_then(|acknowledged| acknowledged.canonical_head.as_ref())
            })
            .or(if is_activation {
                activation_baseline
            } else {
                None
            }),
    };
    if cursor.canonical_head.as_ref() != expected_head {
        return Err("delivery cursor canonical head disagrees with its payload");
    }
    let cursor_is_behind_coverage = cursor
        .canonical_head
        .as_ref()
        .is_some_and(|head| cursor.next_block <= head.number);
    let is_owner_catchup_data = matches!(payload, delivery::Payload::Data(data)
    if data.records.iter().all(|record| {
        record.scope == i32::from(WireDeliveryScope::OwnerCatchup)
    }));
    let preserves_acknowledged_head = acknowledged_cursor
        .is_none_or(|acknowledged| cursor.canonical_head == acknowledged.canonical_head);
    if cursor_is_behind_coverage
        && !(preserves_acknowledged_head
            && (is_activation || is_owner_catchup_data || is_scan_progress))
    {
        return Err("delivery cursor next block does not follow its canonical head");
    }
    Ok(())
}

fn validate_wire_reorg_shape(
    ancestor: &WireBlockRef,
    old_tip: &WireBlockRef,
    new_tip: &WireBlockRef,
) -> Result<(), &'static str> {
    let direct_old_parent_mismatch =
        old_tip.number == ancestor.number.saturating_add(1) && old_tip.parent_hash != ancestor.hash;
    let direct_new_parent_mismatch =
        new_tip.number == ancestor.number.saturating_add(1) && new_tip.parent_hash != ancestor.hash;
    if old_tip.number <= ancestor.number
        || new_tip.number <= ancestor.number
        || old_tip.timestamp <= ancestor.timestamp
        || new_tip.timestamp <= ancestor.timestamp
        || old_tip.hash == new_tip.hash
        || old_tip.hash == ancestor.hash
        || new_tip.hash == ancestor.hash
        || direct_old_parent_mismatch
        || direct_new_parent_mismatch
    {
        return Err("reorg tips do not describe two distinct descendant branches");
    }
    Ok(())
}

fn is_wire_activation(
    acknowledged_cursor: Option<&Cursor>,
    activation_baseline: Option<&WireBlockRef>,
    delivery: &Delivery,
) -> bool {
    let Some(cursor) = delivery.cursor.as_ref() else {
        return false;
    };
    let expected_revision =
        acknowledged_cursor.map_or(Some(1), |cursor| cursor.query_revision.checked_add(1));
    let expected_head = acknowledged_cursor
        .and_then(|cursor| cursor.canonical_head.as_ref())
        .or(activation_baseline);
    expected_revision == Some(delivery.query_revision)
        && cursor.canonical_head.as_ref() == expected_head
        && matches!(
            delivery.payload.as_ref(),
            Some(delivery::Payload::Barrier(barrier))
                if barrier.block.is_none()
                    && barrier.id
                        == format!("desired-state:{}", delivery.query_revision).as_bytes()
        )
}

fn is_wire_cursor_progress(delivery: &Delivery) -> bool {
    let Some(cursor) = delivery.cursor.as_ref() else {
        return false;
    };
    matches!(
        delivery.payload.as_ref(),
        Some(delivery::Payload::Barrier(barrier))
            if barrier.block.is_none()
                && barrier.id
                    == format!(
                        "source-progress:{}:{}",
                        delivery.query_revision, cursor.next_block
                    )
                    .as_bytes()
    )
}

fn wire_head_regresses_or_changes(previous: &Cursor, current: &Cursor) -> bool {
    match (
        previous.canonical_head.as_ref(),
        current.canonical_head.as_ref(),
    ) {
        (Some(previous), Some(current)) => {
            current.number < previous.number
                || (current.number == previous.number && current != previous)
        }
        (Some(_), None) => true,
        _ => false,
    }
}

#[derive(Clone)]
struct OwnedInterests<N: Network> {
    interests: Vec<ReactiveInterest<N>>,
    backfill: Option<SubscriberBackfill>,
}

#[derive(Clone)]
struct PendingCandidate<N: Network> {
    request: ApplyDesiredState,
    base_interests: Vec<ReactiveInterest<N>>,
    global_backfill: Option<SubscriberBackfill>,
    owners: HashMap<HandlerId, OwnedInterests<N>>,
}

#[derive(Clone)]
struct PendingNeutralAcknowledgement {
    sequence: u64,
    token: Vec<u8>,
    cursor: Cursor,
}

type RestoredDesiredState = (
    Vec<ReactiveInterest<Ethereum>>,
    Option<SubscriberBackfill>,
    HashMap<HandlerId, OwnedInterests<Ethereum>>,
);

/// Remote, owner-aware event subscriber backed by an authoritative service.
pub struct RemoteSubscriber<T, N: Network = Ethereum> {
    session_id: String,
    chain_id: u64,
    committed_revision: u64,
    base_interests: Vec<ReactiveInterest<N>>,
    global_backfill: Option<SubscriberBackfill>,
    owners: HashMap<HandlerId, OwnedInterests<N>>,
    pending_candidate: Option<PendingCandidate<N>>,
    pending_neutral_acknowledgement: Option<PendingNeutralAcknowledgement>,
    acknowledged_sequence: u64,
    acknowledged_cursor: Option<Cursor>,
    acknowledged_token: Option<Vec<u8>>,
    delivered_sequence: Option<u64>,
    delivered_cursor: Option<Cursor>,
    capabilities: SubscriberCapabilities,
    transport: T,
    network: PhantomData<fn() -> N>,
}

impl<T, N: Network> RemoteSubscriber<T, N> {
    /// Create a remote subscriber for one durable session and chain.
    pub fn new(session_id: impl Into<String>, chain_id: u64, transport: T) -> Self
    where
        T: RemoteEventTransport,
    {
        Self::new_at_revision(session_id, chain_id, 0, transport)
    }

    /// Restore a remote subscriber at a service-authoritative revision.
    pub fn new_at_revision(
        session_id: impl Into<String>,
        chain_id: u64,
        committed_revision: u64,
        transport: T,
    ) -> Self
    where
        T: RemoteEventTransport,
    {
        let capabilities = transport.capabilities();
        Self {
            session_id: session_id.into(),
            chain_id,
            committed_revision,
            base_interests: Vec::new(),
            global_backfill: None,
            owners: HashMap::new(),
            pending_candidate: None,
            pending_neutral_acknowledgement: None,
            acknowledged_sequence: 0,
            acknowledged_cursor: None,
            acknowledged_token: None,
            delivered_sequence: None,
            delivered_cursor: None,
            capabilities,
            transport,
            network: PhantomData,
        }
    }

    /// Last authoritative desired-state revision acknowledged by the service.
    pub fn committed_revision(&self) -> u64 {
        self.committed_revision
    }

    /// Borrow the transport.
    pub fn transport(&self) -> &T {
        &self.transport
    }

    /// Mutably borrow the transport.
    pub fn transport_mut(&mut self) -> &mut T {
        &mut self.transport
    }
}

impl<T> RemoteSubscriber<T, Ethereum>
where
    T: RemoteEventTransport,
{
    /// Restore local registration mirrors from the service-authoritative wire
    /// state. Provider-portable filters are exact; local-only matchers and
    /// route-key extractors cannot cross the protocol boundary and are reset.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteError::InvalidAuthoritativeState`] when the supplied
    /// session, chain, revision, owner topology, filter, or backfill state does
    /// not form one self-consistent authoritative snapshot.
    pub fn new_from_authoritative(
        session_id: impl Into<String>,
        chain_id: u64,
        transport: T,
        desired_state: Option<ApplyDesiredState>,
        committed_revision: u64,
    ) -> Result<Self, RemoteError> {
        let session_id = session_id.into();
        let capabilities = transport.capabilities();
        let (base_interests, global_backfill, owners) = match desired_state {
            Some(desired_state) => decode_authoritative_desired_state(
                &session_id,
                chain_id,
                committed_revision,
                desired_state,
            )?,
            None if committed_revision == 0 => (Vec::new(), None, HashMap::new()),
            None => {
                return Err(RemoteError::InvalidAuthoritativeState(
                    "non-zero revision is missing desired state",
                ));
            }
        };
        let mut subscriber = Self {
            session_id,
            chain_id,
            committed_revision,
            base_interests,
            global_backfill,
            owners,
            pending_candidate: None,
            pending_neutral_acknowledgement: None,
            acknowledged_sequence: 0,
            acknowledged_cursor: None,
            acknowledged_token: None,
            delivered_sequence: None,
            delivered_cursor: None,
            capabilities,
            transport,
            network: PhantomData,
        };
        subscriber.restore_transport_authority();
        Ok(subscriber)
    }
}

impl RemoteSubscriber<GrpcEventTransport, Ethereum> {
    /// Connect and restore the authoritative desired-state revision.
    ///
    /// If this sequence-zero handshake finds an existing nonzero durable
    /// cursor, operations remain gated until [`EventSubscriber::restore_position`]
    /// confirms the corresponding runtime checkpoint. Persisted callers that
    /// already have that position should prefer [`Self::connect_from_position`].
    ///
    /// # Errors
    ///
    /// Returns [`RemoteTransportError`] when transport negotiation fails or the
    /// service's authoritative desired state/cursor is internally inconsistent.
    pub async fn connect(
        endpoint: impl Into<String>,
        session_id: impl Into<String>,
        chain_id: u64,
    ) -> Result<Self, RemoteTransportError> {
        let session_id = session_id.into();
        let transport = GrpcEventTransport::connect(endpoint, &session_id, chain_id, 0).await?;
        let acknowledged_sequence = accepted_sequence(transport.accepted())?;
        let revision = transport.accepted().committed_revision;
        let desired_state = transport.accepted().desired_state.clone();
        let mut subscriber =
            Self::new_from_authoritative(session_id, chain_id, transport, desired_state, revision)
                .map_err(|error| RemoteTransportError::Protocol(error.to_string()))?;
        subscriber.acknowledged_sequence = acknowledged_sequence;
        Ok(subscriber)
    }

    /// Connect using the delivery token/checkpoint restored with runtime state.
    /// Unlike the convenience `connect` constructor, this sends the restored
    /// sequence and cursor proof in `Hello`. Exact service/runtime agreement is
    /// required except for the proven crash window where that exact delivery
    /// remains one sequence ahead in the service's durable outbox.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteTransportError`] when the restored position is malformed
    /// or disagrees with remote authority, transport negotiation fails, or the
    /// accepted authoritative state is protocol-invalid.
    pub async fn connect_from_position(
        endpoint: impl Into<String>,
        session_id: impl Into<String>,
        chain_id: u64,
        position: &SubscriberResumePosition,
    ) -> Result<Self, RemoteTransportError> {
        let session_id = session_id.into();
        let sequence = resume_sequence(position)?;
        let resume = pending_delivery_resume(position)?;
        let mut transport = GrpcEventTransport::connect_with_resume(
            endpoint.into(),
            session_id.clone(),
            chain_id,
            sequence,
            None,
            GrpcTransportConfig::default(),
            resume,
        )
        .await?;
        transport.restore_position(position)?;
        let acknowledged_sequence = accepted_sequence(transport.accepted())?;
        let revision = transport.accepted().committed_revision;
        let desired_state = transport.accepted().desired_state.clone();
        let mut subscriber =
            Self::new_from_authoritative(session_id, chain_id, transport, desired_state, revision)
                .map_err(|error| RemoteTransportError::Protocol(error.to_string()))?;
        subscriber.acknowledged_sequence = acknowledged_sequence;
        Ok(subscriber)
    }

    /// Connect with bearer authorization retained across transport reconnects.
    /// Existing nonzero sessions have the same restore gate as [`Self::connect`].
    ///
    /// # Security
    ///
    /// Use an HTTPS endpoint, or plaintext only inside an authenticated and
    /// trusted local service mesh. The API retains the bearer token in memory
    /// so it can authenticate reconnect attempts.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteTransportError`] for invalid authorization metadata,
    /// failed/rejected negotiation, or an inconsistent authoritative state.
    pub async fn connect_with_bearer_token(
        endpoint: impl Into<String>,
        session_id: impl Into<String>,
        chain_id: u64,
        bearer_token: impl AsRef<str>,
    ) -> Result<Self, RemoteTransportError> {
        let session_id = session_id.into();
        let transport = GrpcEventTransport::connect_with_authorization(
            endpoint,
            &session_id,
            chain_id,
            0,
            Some(format!("Bearer {}", bearer_token.as_ref())),
        )
        .await?;
        let acknowledged_sequence = accepted_sequence(transport.accepted())?;
        let revision = transport.accepted().committed_revision;
        let desired_state = transport.accepted().desired_state.clone();
        let mut subscriber =
            Self::new_from_authoritative(session_id, chain_id, transport, desired_state, revision)
                .map_err(|error| RemoteTransportError::Protocol(error.to_string()))?;
        subscriber.acknowledged_sequence = acknowledged_sequence;
        Ok(subscriber)
    }

    /// Bearer-authenticated variant of `connect_from_position`.
    ///
    /// # Security
    ///
    /// Use an HTTPS endpoint, or plaintext only inside an authenticated and
    /// trusted local service mesh. The API retains the bearer token in memory
    /// so it can authenticate reconnect attempts.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteTransportError`] when authorization metadata or the
    /// restored position is invalid, negotiation fails, or remote authority
    /// disagrees with the supplied durable proof.
    pub async fn connect_with_bearer_token_from_position(
        endpoint: impl Into<String>,
        session_id: impl Into<String>,
        chain_id: u64,
        bearer_token: impl AsRef<str>,
        position: &SubscriberResumePosition,
    ) -> Result<Self, RemoteTransportError> {
        let session_id = session_id.into();
        let sequence = resume_sequence(position)?;
        let resume = pending_delivery_resume(position)?;
        let mut transport = GrpcEventTransport::connect_with_resume(
            endpoint.into(),
            session_id.clone(),
            chain_id,
            sequence,
            Some(format!("Bearer {}", bearer_token.as_ref())),
            GrpcTransportConfig::default(),
            resume,
        )
        .await?;
        transport.restore_position(position)?;
        let acknowledged_sequence = accepted_sequence(transport.accepted())?;
        let revision = transport.accepted().committed_revision;
        let desired_state = transport.accepted().desired_state.clone();
        let mut subscriber =
            Self::new_from_authoritative(session_id, chain_id, transport, desired_state, revision)
                .map_err(|error| RemoteTransportError::Protocol(error.to_string()))?;
        subscriber.acknowledged_sequence = acknowledged_sequence;
        Ok(subscriber)
    }
}

impl<T, N> RemoteSubscriber<T, N>
where
    T: RemoteEventTransport,
    N: Network,
{
    fn commit_candidate(
        &mut self,
        candidate: PendingCandidate<N>,
        applied: &DesiredStateApplied,
    ) -> Result<(), SubscriberError> {
        if applied.session_id != self.session_id
            || applied.revision != candidate.request.new_revision
            || applied.activation_sequence
                != self.acknowledged_sequence.checked_add(1).ok_or_else(|| {
                    SubscriberError::Provider("remote delivery sequence overflow".into())
                })?
        {
            return Err(SubscriberError::Provider(format!(
                "invalid desired-state acknowledgement: session={}, revision={}",
                applied.session_id, applied.revision
            )));
        }
        self.base_interests = candidate.base_interests;
        self.global_backfill = candidate.global_backfill;
        self.owners = candidate.owners;
        self.committed_revision = candidate.request.new_revision;
        self.pending_candidate = None;
        self.maybe_complete_backfill();
        Ok(())
    }

    fn restore_transport_authority(&mut self) {
        if let Some(sequence) = self.transport.durable_acknowledged_sequence() {
            self.acknowledged_sequence = sequence;
            self.acknowledged_token = (sequence != 0).then(|| sequence.to_be_bytes().to_vec());
        }
        self.acknowledged_cursor = self.transport.durable_acknowledged_cursor();
        self.maybe_complete_backfill();
    }

    fn has_incomplete_backfill(&self) -> bool {
        self.global_backfill.is_some() || self.owners.values().any(|owner| owner.backfill.is_some())
    }

    fn has_open_backfill(&self) -> bool {
        self.global_backfill
            .as_ref()
            .is_some_and(|backfill| backfill.end_block().is_none())
            || self.owners.values().any(|owner| {
                owner
                    .backfill
                    .as_ref()
                    .is_some_and(|backfill| backfill.end_block().is_none())
            })
    }

    fn maybe_complete_backfill(&mut self) {
        if !self.has_incomplete_backfill() {
            return;
        }
        let Some(cursor) = self.acknowledged_cursor.as_ref() else {
            return;
        };
        if cursor.query_revision != self.committed_revision {
            return;
        }
        if let Some(backfill) = self.global_backfill.as_ref() {
            let target = backfill
                .end_block()
                .and_then(|end| end.checked_add(1))
                .or(cursor.owner_backfill_activation_block);
            if target.is_some_and(|target| cursor.next_block >= target) {
                self.global_backfill = None;
            }
        }
        for owner in self.owners.values_mut() {
            let Some(backfill) = owner.backfill.as_ref() else {
                continue;
            };
            let target = backfill
                .end_block()
                .and_then(|end| end.checked_add(1))
                .or(cursor.owner_backfill_activation_block);
            if target.is_some_and(|target| cursor.next_block >= target) {
                owner.backfill = None;
            }
        }
    }

    async fn reconcile_neutral_acknowledgement(&mut self) -> Result<(), SubscriberError> {
        let Some(pending) = self.pending_neutral_acknowledgement.clone() else {
            return Ok(());
        };
        self.transport
            .acknowledge(Acknowledge {
                session_id: self.session_id.clone(),
                sequence: pending.sequence,
                delivery_token: pending.token.clone(),
            })
            .await
            .map_err(transport_error)?;
        self.acknowledged_sequence = pending.sequence;
        self.acknowledged_cursor = Some(pending.cursor);
        self.acknowledged_token = Some(pending.token);
        self.pending_neutral_acknowledgement = None;
        self.maybe_complete_backfill();
        Ok(())
    }

    async fn reconcile_pending_candidate(&mut self) -> Result<(), SubscriberError> {
        let Some(candidate) = self.pending_candidate.clone() else {
            return Ok(());
        };
        let applied = match self
            .transport
            .apply_desired_state(candidate.request.clone())
            .await
        {
            Ok(applied) => applied,
            Err(error) => {
                if !error.operation_uncertain() {
                    self.pending_candidate = None;
                }
                return Err(transport_error(error));
            }
        };
        self.commit_candidate(candidate, &applied)
    }

    async fn apply_candidate(
        &mut self,
        base_interests: Vec<ReactiveInterest<N>>,
        global_backfill: Option<SubscriberBackfill>,
        owners: HashMap<HandlerId, OwnedInterests<N>>,
        supersedes_incomplete_backfill: bool,
    ) -> Result<(), SubscriberError> {
        self.reconcile_neutral_acknowledgement().await?;
        if self.pending_candidate.is_some() {
            return Err(SubscriberError::Provider(
                "remote desired-state reconciliation is still pending".into(),
            ));
        }
        if self.has_incomplete_backfill() && !supersedes_incomplete_backfill {
            return Err(SubscriberError::Provider(
                "remote owner backfill is still in progress; lifecycle mutation is fenced until an acknowledged cursor reaches global coverage"
                    .into(),
            ));
        }
        let new_revision =
            self.committed_revision
                .checked_add(1)
                .ok_or(SubscriberError::InvalidConfig(
                    "remote desired-state revision overflow",
                ))?;
        let request = build_desired_state(
            &self.session_id,
            self.chain_id,
            self.committed_revision,
            new_revision,
            &base_interests,
            global_backfill.as_ref(),
            &owners,
        )
        .map_err(remote_error)?;
        let candidate = PendingCandidate {
            request: request.clone(),
            base_interests,
            global_backfill,
            owners,
        };
        self.pending_candidate = Some(candidate.clone());
        let applied = match self.transport.apply_desired_state(request).await {
            Ok(applied) => applied,
            Err(error) => {
                if !error.operation_uncertain() {
                    self.pending_candidate = None;
                }
                return Err(transport_error(error));
            }
        };
        self.commit_candidate(candidate, &applied)
    }
}

impl<T> EventSubscriber<Ethereum> for RemoteSubscriber<T, Ethereum>
where
    T: RemoteEventTransport,
{
    fn chain_id(&self) -> Option<u64> {
        Some(self.chain_id)
    }

    fn capabilities(&self) -> SubscriberCapabilities {
        self.capabilities.clone()
    }

    fn restore_position(
        &mut self,
        position: &SubscriberResumePosition,
    ) -> Result<(), SubscriberError> {
        if self.pending_candidate.is_some()
            || self.pending_neutral_acknowledgement.is_some()
            || self.delivered_sequence.is_some()
        {
            return Err(SubscriberError::Provider(
                "cannot restore a remote position while an operation or delivery is pending".into(),
            ));
        }
        self.transport
            .restore_position(position)
            .map_err(transport_error)?;
        let restored_sequence = resume_sequence(position).map_err(transport_error)?;
        let durable_sequence = self
            .transport
            .durable_acknowledged_sequence()
            .unwrap_or(restored_sequence);
        self.acknowledged_sequence = durable_sequence;
        self.acknowledged_cursor = self.transport.durable_acknowledged_cursor();
        self.acknowledged_token = (self.acknowledged_sequence != 0)
            .then(|| self.acknowledged_sequence.to_be_bytes().to_vec());
        self.delivered_sequence = (position.delivery_token.is_some()
            && durable_sequence.checked_add(1) == Some(restored_sequence))
        .then_some(restored_sequence);
        self.delivered_cursor = None;
        self.maybe_complete_backfill();
        Ok(())
    }

    fn register_interests(
        &mut self,
        interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        let base_interests = interests.to_vec();
        Box::pin(async move {
            self.reconcile_pending_candidate().await?;
            self.apply_candidate(base_interests, None, HashMap::new(), false)
                .await
        })
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, Ethereum> {
        Box::pin(async move {
            self.reconcile_pending_candidate().await?;
            self.reconcile_neutral_acknowledgement().await?;
            loop {
                match self
                    .transport
                    .next_delivery()
                    .await
                    .map_err(transport_error)?
                {
                    None => return Ok(None),
                    Some(delivery) => {
                        let expected_sequence = self
                            .delivered_sequence
                            .map_or_else(|| self.acknowledged_sequence.checked_add(1), Some)
                            .ok_or_else(|| {
                                SubscriberError::Provider(
                                    "remote delivery sequence overflow".into(),
                                )
                            })?;
                        let sequence = delivery.sequence;
                        let delivered_cursor = delivery.cursor.clone();
                        let checkpoint_neutral = delivery.checkpoint_neutral;
                        let delivery_token = delivery.delivery_token.clone();
                        let requires_open_backfill_boundary = self.has_open_backfill();
                        let activation_baseline = self
                            .global_backfill
                            .as_ref()
                            .and_then(SubscriberBackfill::retained_anchor)
                            .map(wire_retained_baseline)
                            .transpose()
                            .map_err(remote_error)?;
                        let authoritative_owners: HashSet<_> =
                            self.owners.keys().map(HandlerId::as_str).collect();
                        let batch = decode_delivery(
                            DeliveryAuthority {
                                session_id: &self.session_id,
                                chain_id: self.chain_id,
                                revision: self.committed_revision,
                                expected_sequence,
                                acknowledged_cursor: self.acknowledged_cursor.as_ref(),
                                activation_baseline: activation_baseline.as_ref(),
                                requires_open_backfill_boundary,
                            },
                            delivery,
                            &authoritative_owners,
                        )
                        .map_err(|error| SubscriberError::Provider(error.to_string()))?;
                        if checkpoint_neutral {
                            self.pending_neutral_acknowledgement =
                                Some(PendingNeutralAcknowledgement {
                                    sequence,
                                    token: delivery_token,
                                    cursor: delivered_cursor.ok_or_else(|| {
                                        SubscriberError::Provider(
                                            "checkpoint-neutral delivery is missing its cursor"
                                                .into(),
                                        )
                                    })?,
                                });
                            self.reconcile_neutral_acknowledgement().await?;
                            continue;
                        }
                        self.delivered_sequence = Some(sequence);
                        self.delivered_cursor = delivered_cursor;
                        return Ok(Some(batch));
                    }
                }
            }
        })
    }

    fn acknowledge_delivery(
        &mut self,
        token: SubscriberDeliveryToken,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.reconcile_pending_candidate().await?;
            self.reconcile_neutral_acknowledgement().await?;
            let bytes = token.as_bytes();
            let sequence = u64::from_be_bytes(bytes.try_into().map_err(|_| {
                SubscriberError::Provider("invalid remote delivery token width".into())
            })?);
            if self.delivered_sequence.is_none()
                && self.acknowledged_sequence == sequence
                && self.acknowledged_token.as_deref() == Some(bytes)
            {
                return Ok(());
            }
            if self.delivered_sequence != Some(sequence) {
                return Err(SubscriberError::Provider(
                    "remote acknowledgement does not match the delivered sequence".into(),
                ));
            }
            let acknowledged_token = token.as_bytes().to_vec();
            self.transport
                .acknowledge(Acknowledge {
                    session_id: self.session_id.clone(),
                    sequence,
                    delivery_token: acknowledged_token.clone(),
                })
                .await
                .map_err(transport_error)?;
            self.acknowledged_sequence = sequence;
            self.acknowledged_cursor = self
                .delivered_cursor
                .take()
                .or_else(|| self.transport.durable_acknowledged_cursor());
            self.acknowledged_token = Some(acknowledged_token);
            self.delivered_sequence = None;
            self.maybe_complete_backfill();
            Ok(())
        })
    }
}

fn runtime_capabilities(wire: Option<&SourceCapabilities>) -> SubscriberCapabilities {
    let Some(wire) = wire else {
        return SubscriberCapabilities::default();
    };
    let mut capabilities = Vec::new();
    let mut has_safe_head = false;
    let mut has_finalized_head = false;
    for capability in &wire.capabilities {
        let Ok(capability) = WireCapability::try_from(*capability) else {
            continue;
        };
        let mapped = match capability {
            WireCapability::Historical => Some(SubscriberCapability::HistoricalBackfill),
            WireCapability::Live => Some(SubscriberCapability::Live),
            WireCapability::Logs => Some(SubscriberCapability::Logs),
            WireCapability::Headers => Some(SubscriberCapability::BlockHeaders),
            WireCapability::DynamicFilters => Some(SubscriberCapability::DynamicInterests),
            WireCapability::ExplicitReorgs => Some(SubscriberCapability::ExplicitReorgs),
            WireCapability::SafeHead => {
                has_safe_head = true;
                None
            }
            WireCapability::FinalizedHead => {
                has_finalized_head = true;
                None
            }
            WireCapability::OwnerScopedDelivery => Some(SubscriberCapability::OwnerScopedDelivery),
            WireCapability::DurableReplay => Some(SubscriberCapability::DurableReplay),
            WireCapability::Unspecified
            | WireCapability::Transactions
            | WireCapability::FullBlocks
            | WireCapability::Pending
            | WireCapability::ServerFiltering
            | WireCapability::NativeCheckpoint => None,
        };
        if let Some(mapped) = mapped {
            capabilities.push(mapped);
        }
    }
    if has_safe_head && has_finalized_head {
        capabilities.push(SubscriberCapability::FinalityUpdates);
    }
    capabilities.push(SubscriberCapability::Barriers);
    SubscriberCapabilities::new(capabilities)
}

#[cfg(test)]
mod capability_tests {
    use super::*;

    #[test]
    fn wire_capabilities_never_overclaim_unimplemented_runtime_shapes() {
        let wire = SourceCapabilities {
            capabilities: vec![
                WireCapability::FullBlocks.into(),
                WireCapability::Pending.into(),
                WireCapability::SafeHead.into(),
            ],
            sources: Vec::new(),
        };
        let capabilities = runtime_capabilities(Some(&wire));
        assert!(!capabilities.supports(SubscriberCapability::FullBlocks));
        assert!(!capabilities.supports(SubscriberCapability::PendingTransactions));
        assert!(!capabilities.supports(SubscriberCapability::FinalityUpdates));
    }

    #[test]
    fn finality_requires_both_safe_and_finalized_wire_heads() {
        let wire = SourceCapabilities {
            capabilities: vec![
                WireCapability::SafeHead.into(),
                WireCapability::FinalizedHead.into(),
            ],
            sources: Vec::new(),
        };
        assert!(runtime_capabilities(Some(&wire)).supports(SubscriberCapability::FinalityUpdates));
    }
}

#[cfg(test)]
mod resume_tests {
    use super::*;

    fn pending_position(coverage_head: RuntimeBlockRef) -> SubscriberResumePosition {
        SubscriberResumePosition::new(
            1,
            coverage_head,
            Vec::new(),
            Some(SubscriberDeliveryToken::new(1_u64.to_be_bytes().to_vec())),
            None,
        )
    }

    #[test]
    fn pending_resume_rejects_incomplete_coverage_metadata() {
        let missing_parent = pending_position(RuntimeBlockRef {
            number: 12,
            hash: B256::repeat_byte(0x12),
            parent_hash: None,
            timestamp: Some(1_700_000_012),
        });
        assert!(
            pending_delivery_resume(&missing_parent)
                .expect_err("missing parent hash cannot become a durable wire proof")
                .to_string()
                .contains("missing its parent hash")
        );

        let missing_timestamp = pending_position(RuntimeBlockRef {
            number: 12,
            hash: B256::repeat_byte(0x12),
            parent_hash: Some(B256::repeat_byte(0x11)),
            timestamp: None,
        });
        assert!(
            pending_delivery_resume(&missing_timestamp)
                .expect_err("missing timestamp cannot become a durable wire proof")
                .to_string()
                .contains("missing its timestamp")
        );
    }

    #[test]
    fn pending_resume_preserves_complete_coverage_metadata_exactly() {
        let position = pending_position(RuntimeBlockRef {
            number: 12,
            hash: B256::repeat_byte(0x12),
            parent_hash: Some(B256::repeat_byte(0x11)),
            timestamp: Some(1_700_000_012),
        });
        let resume = pending_delivery_resume(&position)
            .expect("complete proof")
            .expect("pending resume");
        let coverage = resume.coverage_head.expect("coverage head");
        assert_eq!(coverage.number, 12);
        assert_eq!(coverage.hash, vec![0x12; 32]);
        assert_eq!(coverage.parent_hash, vec![0x11; 32]);
        assert_eq!(coverage.timestamp, 1_700_000_012);
    }

    #[test]
    fn runtime_resume_requires_exact_checkpoint_presence_and_coverage() {
        let head = WireBlockRef {
            number: 12,
            hash: vec![0x12; 32],
            parent_hash: vec![0x11; 32],
            timestamp: 1_700_000_012,
        };
        let cursor = Cursor {
            chain_id: 1,
            query_revision: 1,
            next_block: 13,
            canonical_head: Some(head),
            batch_sequence: 1,
            provider_checkpoint: b"checkpoint".to_vec(),
            owner_backfill_activation_block: None,
        };
        let accepted = HelloAccepted {
            acknowledged_cursor: Some(cursor.clone()),
            runtime_checkpoint_position: Some(
                evm_fork_cache_event_protocol::v1::RuntimeCheckpointPosition {
                    cursor: Some(cursor),
                },
            ),
            ..Default::default()
        };
        let coverage = RuntimeBlockRef {
            number: 12,
            hash: B256::repeat_byte(0x12),
            parent_hash: Some(B256::repeat_byte(0x11)),
            timestamp: Some(1_700_000_012),
        };
        let exact = SubscriberResumePosition::new(
            1,
            coverage,
            Vec::new(),
            Some(SubscriberDeliveryToken::new(1_u64.to_be_bytes().to_vec())),
            Some(SubscriberCheckpoint::new(b"checkpoint".to_vec())),
        );
        validate_resume_position(&accepted, &exact).expect("exact runtime position");

        let mut missing_checkpoint = exact.clone();
        missing_checkpoint.subscriber_checkpoint = None;
        assert!(
            validate_resume_position(&accepted, &missing_checkpoint)
                .expect_err("missing checkpoint presence must not equal nonempty authority")
                .to_string()
                .contains("provider checkpoint")
        );

        let mut wrong_coverage = exact;
        wrong_coverage.coverage_head.hash = B256::repeat_byte(0xee);
        assert!(
            validate_resume_position(&accepted, &wrong_coverage)
                .expect_err("coverage identity must match exactly")
                .to_string()
                .contains("canonical head")
        );
    }
}

impl<T> InterestOwnerSubscriber<Ethereum> for RemoteSubscriber<T, Ethereum>
where
    T: RemoteEventTransport,
{
    fn upsert_interest_owners(
        &mut self,
        owner_updates: Vec<(HandlerId, Vec<ReactiveInterest<Ethereum>>)>,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.reconcile_pending_candidate().await?;
            let mut seen = HashSet::with_capacity(owner_updates.len());
            let mut owners = self.owners.clone();
            for (owner, interests) in owner_updates {
                if !seen.insert(owner.clone()) {
                    return Err(SubscriberError::InvalidConfig(
                        "bulk remote owner upsert contains a duplicate owner",
                    ));
                }
                owners.insert(
                    owner,
                    OwnedInterests {
                        interests,
                        backfill: None,
                    },
                );
            }
            self.apply_candidate(self.base_interests.clone(), None, owners, false)
                .await
        })
    }

    fn replace_interest_owners(
        &mut self,
        replacement: Vec<(HandlerId, Vec<ReactiveInterest<Ethereum>>)>,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.reconcile_pending_candidate().await?;
            let mut owners = HashMap::with_capacity(replacement.len());
            for (owner, interests) in replacement {
                if owners
                    .insert(
                        owner,
                        OwnedInterests {
                            interests,
                            backfill: None,
                        },
                    )
                    .is_some()
                {
                    return Err(SubscriberError::InvalidConfig(
                        "exact remote owner replacement contains a duplicate owner",
                    ));
                }
            }
            self.apply_candidate(Vec::new(), None, owners, true).await
        })
    }

    fn replace_interest_owners_with_global_backfill(
        &mut self,
        replacement: Vec<(HandlerId, Vec<ReactiveInterest<Ethereum>>)>,
        backfill: SubscriberBackfill,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.reconcile_pending_candidate().await?;
            let mut owners = HashMap::with_capacity(replacement.len());
            for (owner, interests) in replacement {
                if owners
                    .insert(
                        owner,
                        OwnedInterests {
                            interests,
                            backfill: None,
                        },
                    )
                    .is_some()
                {
                    return Err(SubscriberError::InvalidConfig(
                        "global remote owner replacement contains a duplicate owner",
                    ));
                }
            }
            self.apply_candidate(Vec::new(), Some(backfill), owners, true)
                .await
        })
    }

    fn add_interest_owner(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<Ethereum>],
    ) -> SubscriberOperation<'_, ()> {
        let interests = interests.to_vec();
        Box::pin(async move {
            self.reconcile_pending_candidate().await?;
            let mut owners = self.owners.clone();
            owners.insert(
                owner,
                OwnedInterests {
                    interests,
                    backfill: None,
                },
            );
            self.apply_candidate(self.base_interests.clone(), None, owners, false)
                .await
        })
    }

    fn add_interest_owner_with_backfill(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<Ethereum>],
        backfill: SubscriberBackfill,
    ) -> SubscriberOperation<'_, ()> {
        let interests = interests.to_vec();
        Box::pin(async move {
            self.reconcile_pending_candidate().await?;
            let (owner_backfill, global_backfill) = split_mid_lifecycle_backfill(backfill)?;
            let mut owners = self.owners.clone();
            owners.insert(
                owner,
                OwnedInterests {
                    interests,
                    backfill: Some(owner_backfill),
                },
            );
            self.apply_candidate(self.base_interests.clone(), global_backfill, owners, false)
                .await
        })
    }

    fn add_interest_owner_with_canonical_catchup(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<Ethereum>],
        retained: RuntimeBlockRef,
    ) -> SubscriberOperation<'_, ()> {
        let interests = interests.to_vec();
        Box::pin(async move {
            self.reconcile_pending_candidate().await?;
            let mut owners = self.owners.clone();
            owners.insert(
                owner,
                OwnedInterests {
                    interests,
                    backfill: Some(SubscriberBackfill::from_block(retained.number)),
                },
            );
            let global_backfill = SubscriberBackfill::after_canonical_block(retained)?;
            self.apply_candidate(
                self.base_interests.clone(),
                Some(global_backfill),
                owners,
                false,
            )
            .await
        })
    }

    fn remove_interest_owner(
        &mut self,
        owner: &HandlerId,
    ) -> SubscriberOperation<'_, Option<Vec<ReactiveInterest<Ethereum>>>> {
        let owner = owner.clone();
        Box::pin(async move {
            self.reconcile_pending_candidate().await?;
            let mut owners = self.owners.clone();
            let removed = owners.remove(&owner).map(|owned| owned.interests);
            self.apply_candidate(self.base_interests.clone(), None, owners, false)
                .await?;
            Ok(removed)
        })
    }

    fn owner_interests(&self, owner: &HandlerId) -> Option<&[ReactiveInterest<Ethereum>]> {
        self.owners
            .get(owner)
            .map(|owned| owned.interests.as_slice())
    }
}

fn split_mid_lifecycle_backfill(
    backfill: SubscriberBackfill,
) -> Result<(SubscriberBackfill, Option<SubscriberBackfill>), SubscriberError> {
    let Some(baseline) = backfill.retained_anchor().copied() else {
        return Ok((backfill, None));
    };
    if baseline.number.checked_add(1) == Some(backfill.start_block()) {
        return Ok((backfill, None));
    }
    if baseline.number != backfill.start_block() {
        return Err(SubscriberError::InvalidConfig(
            "retained owner backfill anchor is not its start or exact predecessor",
        ));
    }
    let owner_backfill = backfill.end_block().map_or_else(
        || SubscriberBackfill::from_block(backfill.start_block()),
        |end| SubscriberBackfill::range(backfill.start_block(), end),
    );
    Ok((owner_backfill, None))
}

fn build_desired_state<N: Network>(
    session_id: &str,
    chain_id: u64,
    expected_revision: u64,
    new_revision: u64,
    base_interests: &[ReactiveInterest<N>],
    global_backfill: Option<&SubscriberBackfill>,
    owners: &HashMap<HandlerId, OwnedInterests<N>>,
) -> Result<ApplyDesiredState, RemoteError> {
    let mut wire_owners = Vec::with_capacity(
        owners.len() + usize::from(!base_interests.is_empty() || global_backfill.is_some()),
    );
    if !base_interests.is_empty() || global_backfill.is_some() {
        wire_owners.push(OwnerInterests {
            owner_id: String::new(),
            interests: compile_portable_interests(base_interests)?,
            backfill: global_backfill.map(encode_backfill).transpose()?,
            canonical: true,
        });
    }
    for (owner, owned) in owners {
        wire_owners.push(OwnerInterests {
            owner_id: owner.as_str().to_owned(),
            interests: compile_portable_interests(&owned.interests)?,
            backfill: owned.backfill.as_ref().map(encode_backfill).transpose()?,
            canonical: false,
        });
    }
    wire_owners.sort_by(|left, right| left.owner_id.cmp(&right.owner_id));
    Ok(ApplyDesiredState {
        protocol_version: PROTOCOL_VERSION,
        session_id: session_id.to_owned(),
        chain_id,
        expected_revision,
        new_revision,
        owners: wire_owners,
    })
}

fn encode_backfill(backfill: &SubscriberBackfill) -> Result<Backfill, RemoteError> {
    let to_block_excl = backfill
        .end_block()
        .map(|end| {
            end.checked_add(1)
                .ok_or(RemoteError::InvalidAuthoritativeState(
                    "backfill exclusive upper bound overflows u64",
                ))
        })
        .transpose()?;
    if let Some(baseline) = backfill.retained_anchor()
        && baseline.number.checked_add(1) != Some(backfill.start_block())
    {
        return Err(RemoteError::InvalidAuthoritativeState(
            "retained backfill baseline must immediately precede its start",
        ));
    }
    Ok(Backfill {
        from_block: backfill.start_block(),
        to_block_excl,
        retained_baseline: backfill
            .retained_anchor()
            .map(wire_retained_baseline)
            .transpose()?,
    })
}

fn wire_retained_baseline(block: &RuntimeBlockRef) -> Result<WireBlockRef, RemoteError> {
    let parent_hash = block
        .parent_hash
        .ok_or(RemoteError::InvalidAuthoritativeState(
            "retained backfill baseline is missing its parent hash",
        ))?;
    let timestamp = block
        .timestamp
        .ok_or(RemoteError::InvalidAuthoritativeState(
            "retained backfill baseline is missing its timestamp",
        ))?;
    Ok(WireBlockRef {
        number: block.number,
        hash: block.hash.as_slice().to_vec(),
        parent_hash: parent_hash.as_slice().to_vec(),
        timestamp,
    })
}

fn decode_authoritative_backfill(backfill: Backfill) -> Result<SubscriberBackfill, RemoteError> {
    if backfill
        .to_block_excl
        .is_some_and(|end| end < backfill.from_block)
        || (backfill.retained_baseline.is_none()
            && backfill
                .to_block_excl
                .is_some_and(|end| end == backfill.from_block))
    {
        return Err(RemoteError::InvalidAuthoritativeState(
            "owner backfill range is invalid",
        ));
    }
    if let Some(baseline) = backfill.retained_baseline {
        if baseline.number.checked_add(1) != Some(backfill.from_block) {
            return Err(RemoteError::InvalidAuthoritativeState(
                "retained backfill baseline must immediately precede its start",
            ));
        }
        let baseline = runtime_block_ref(&baseline).map_err(|_| {
            RemoteError::InvalidAuthoritativeState(
                "retained backfill baseline has an invalid block identity",
            )
        })?;
        return backfill
            .to_block_excl
            .map_or_else(
                || SubscriberBackfill::after_canonical_block(baseline),
                |end| SubscriberBackfill::after_canonical_block_through(baseline, end - 1),
            )
            .map_err(|_| {
                RemoteError::InvalidAuthoritativeState(
                    "retained backfill baseline or upper bound is invalid",
                )
            });
    }
    Ok(backfill.to_block_excl.map_or_else(
        || SubscriberBackfill::from_block(backfill.from_block),
        |end| SubscriberBackfill::range(backfill.from_block, end - 1),
    ))
}

fn decode_authoritative_desired_state(
    session_id: &str,
    chain_id: u64,
    committed_revision: u64,
    desired_state: ApplyDesiredState,
) -> Result<RestoredDesiredState, RemoteError> {
    if desired_state.protocol_version != PROTOCOL_VERSION
        || desired_state.session_id != session_id
        || desired_state.chain_id != chain_id
        || desired_state.new_revision != committed_revision
        || desired_state.expected_revision.checked_add(1) != Some(committed_revision)
    {
        return Err(RemoteError::InvalidAuthoritativeState(
            "identity, version, or revision does not match HelloAccepted",
        ));
    }
    let mut base_interests = Vec::new();
    let mut global_backfill = None;
    let mut has_base_interests = false;
    let mut owners = HashMap::new();
    for owner in desired_state.owners {
        let interests = decode_portable_interests(owner.interests)?;
        if owner.canonical {
            if !owner.owner_id.is_empty() {
                return Err(RemoteError::InvalidAuthoritativeState(
                    "canonical interests must have an empty owner id",
                ));
            }
            if has_base_interests {
                return Err(RemoteError::InvalidAuthoritativeState(
                    "base owner appears more than once",
                ));
            }
            has_base_interests = true;
            base_interests = interests;
            global_backfill = owner
                .backfill
                .map(decode_authoritative_backfill)
                .transpose()?;
        } else if owner.owner_id.is_empty() {
            return Err(RemoteError::InvalidAuthoritativeState(
                "non-canonical owner id is empty",
            ));
        } else {
            let backfill = owner
                .backfill
                .map(decode_authoritative_backfill)
                .transpose()?;
            let owner_id = HandlerId::try_new(owner.owner_id).map_err(|_| {
                RemoteError::InvalidAuthoritativeState("non-canonical owner id is empty")
            })?;
            if owners
                .insert(
                    owner_id,
                    OwnedInterests {
                        interests,
                        backfill,
                    },
                )
                .is_some()
            {
                return Err(RemoteError::InvalidAuthoritativeState(
                    "owner appears more than once",
                ));
            }
        }
    }
    Ok((base_interests, global_backfill, owners))
}

fn decode_portable_interests(
    interests: Vec<PortableInterest>,
) -> Result<Vec<ReactiveInterest<Ethereum>>, RemoteError> {
    interests
        .into_iter()
        .map(|interest| match interest.kind {
            Some(portable_interest::Kind::Log(log)) => {
                let addresses = log
                    .addresses
                    .into_iter()
                    .map(|address| {
                        Address::try_from(address.as_slice()).map_err(|_| {
                            RemoteError::InvalidAuthoritativeState("log address is not 20 bytes")
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if log.topics.len() > 4 {
                    return Err(RemoteError::InvalidAuthoritativeState(
                        "log filter has more than four topic positions",
                    ));
                }
                let mut filter = Filter::new().address(addresses);
                for (index, accepted) in log.topics.into_iter().enumerate() {
                    let topics = accepted
                        .values
                        .into_iter()
                        .map(|topic| {
                            B256::try_from(topic.as_slice()).map_err(|_| {
                                RemoteError::InvalidAuthoritativeState("log topic is not 32 bytes")
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    filter = match index {
                        0 => filter.event_signature(topics),
                        1 => filter.topic1(topics),
                        2 => filter.topic2(topics),
                        3 => filter.topic3(topics),
                        _ => unreachable!("topic count was bounded above"),
                    };
                }
                Ok(ReactiveInterest::Logs(RuntimeLogInterest {
                    provider_filter: filter,
                    local_matcher: None,
                    route_key: None,
                }))
            }
            Some(portable_interest::Kind::Block(block)) => match BlockMode::try_from(block.mode) {
                Ok(BlockMode::Header) => Ok(ReactiveInterest::Blocks(RuntimeBlockInterest {
                    mode: BlockInterestMode::Header,
                })),
                Ok(BlockMode::FullBlock) => Err(RemoteError::UnsupportedInterest("full block")),
                _ => Err(RemoteError::InvalidAuthoritativeState(
                    "block mode is unspecified or unknown",
                )),
            },
            None => Err(RemoteError::InvalidAuthoritativeState(
                "portable interest kind is missing",
            )),
        })
        .collect()
}

fn remote_error(error: RemoteError) -> SubscriberError {
    match error {
        RemoteError::UnsupportedInterest(kind) => SubscriberError::Unsupported(kind),
        RemoteError::UnsupportedFilterBlockOption => SubscriberError::Unsupported(
            "remote log filters with block constraints are unsupported",
        ),
        RemoteError::InvalidAuthoritativeState(message) => {
            SubscriberError::Provider(format!("invalid authoritative desired state: {message}"))
        }
    }
}

fn transport_error(error: RemoteTransportError) -> SubscriberError {
    SubscriberError::Provider(error.to_string())
}

fn decode_delivery(
    authority: DeliveryAuthority<'_>,
    delivery: Delivery,
    authoritative_owners: &HashSet<&str>,
) -> Result<ReactiveInputBatch<Ethereum>, RemoteDecodeError> {
    let payload_commitment = SubscriberPayloadCommitment::new(keccak256(delivery.encode_to_vec()));
    if delivery.session_id != authority.session_id {
        return Err(RemoteDecodeError::Session {
            expected: authority.session_id.to_owned(),
            received: delivery.session_id,
        });
    }
    let cursor = delivery
        .cursor
        .as_ref()
        .ok_or(RemoteDecodeError::MissingField("delivery.cursor"))?;
    if cursor.chain_id != authority.chain_id {
        return Err(RemoteDecodeError::Chain {
            expected: authority.chain_id,
            received: cursor.chain_id,
        });
    }
    if let Some(head) = cursor.canonical_head.as_ref() {
        runtime_block_ref(head)?;
    }
    if delivery.query_revision != authority.revision
        || delivery.sequence != authority.expected_sequence
        || cursor.query_revision != delivery.query_revision
        || cursor.batch_sequence != delivery.sequence
    {
        return Err(RemoteDecodeError::Cursor);
    }
    if delivery.delivery_token.as_slice() != delivery.sequence.to_be_bytes() {
        return Err(RemoteDecodeError::DeliveryToken);
    }
    validate_wire_delivery_progress(
        authority.acknowledged_cursor,
        authority.activation_baseline,
        &delivery,
        authority.requires_open_backfill_boundary,
    )
    .map_err(RemoteDecodeError::CursorAuthority)?;
    let token = delivery.delivery_token;
    let checkpoint = cursor.provider_checkpoint.clone();
    let payload = delivery
        .payload
        .ok_or(RemoteDecodeError::MissingField("delivery.payload"))?;
    let mut batch = match payload {
        delivery::Payload::Data(data) => {
            decode_data_payload(authority.chain_id, data.records, authoritative_owners)?
        }
        delivery::Payload::Reorg(reorg) => {
            ReactiveInputBatch::new(Vec::new()).with_chain_controls([ChainControl::Reorg {
                common_ancestor: runtime_block_ref(
                    &reorg
                        .common_ancestor
                        .ok_or(RemoteDecodeError::MissingField("reorg.common_ancestor"))?,
                )?,
                old_tip: runtime_block_ref(
                    &reorg
                        .old_tip
                        .ok_or(RemoteDecodeError::MissingField("reorg.old_tip"))?,
                )?,
                new_tip: runtime_block_ref(
                    &reorg
                        .new_tip
                        .ok_or(RemoteDecodeError::MissingField("reorg.new_tip"))?,
                )?,
            }])
        }
        delivery::Payload::Finality(finality) => {
            let block = runtime_block_ref(
                &finality
                    .block
                    .ok_or(RemoteDecodeError::MissingField("finality.block"))?,
            )?;
            let control = match FinalityKind::try_from(finality.kind)
                .map_err(|_| RemoteDecodeError::Finality(finality.kind))?
            {
                FinalityKind::Safe => ChainControl::Safe(block),
                FinalityKind::Finalized => ChainControl::Finalized(block),
                FinalityKind::Unspecified => {
                    return Err(RemoteDecodeError::Finality(finality.kind));
                }
            };
            ReactiveInputBatch::new(Vec::new()).with_chain_controls([control])
        }
        delivery::Payload::Barrier(barrier) => {
            if barrier.id.is_empty() {
                return Err(RemoteDecodeError::EmptyBarrierId);
            }
            ReactiveInputBatch::new(Vec::new()).with_chain_controls([ChainControl::Barrier {
                id: barrier.id,
                block: barrier.block.as_ref().map(runtime_block_ref).transpose()?,
            }])
        }
    };
    batch = batch
        .with_chain_id(authority.chain_id)
        .with_payload_commitment(payload_commitment)
        .with_delivery_token(SubscriberDeliveryToken::new(token));
    if !checkpoint.is_empty() {
        batch = batch.with_subscriber_checkpoint(SubscriberCheckpoint::new(checkpoint));
    }
    Ok(batch)
}

fn decode_data_payload(
    expected_chain_id: u64,
    wire_records: Vec<evm_fork_cache_event_protocol::v1::EventRecord>,
    authoritative_owners: &HashSet<&str>,
) -> Result<ReactiveInputBatch<Ethereum>, RemoteDecodeError> {
    if wire_records.is_empty() {
        return Err(RemoteDecodeError::EmptyData);
    }
    let mut blocks = HashMap::new();
    // Canonical ordering places logs before the final compact progress record
    // at the same height. Pre-index every explicit block identity so those logs
    // receive the complete parent/timestamp proof rather than a synthetic
    // partial context that only the later control could enrich.
    for wire_record in &wire_records {
        let Some(event) = wire_record
            .event
            .as_ref()
            .and_then(|event| event.event.as_ref())
        else {
            continue;
        };
        let wire = match event {
            chain_event::Event::BlockHeader(header) => header.block.as_ref(),
            chain_event::Event::BlockProgress(progress) => progress.block.as_ref(),
            chain_event::Event::Log(_) => None,
        };
        if let Some(wire) = wire {
            let block = runtime_block_ref(wire)?;
            if let Some(known) = blocks.insert(block.number, block)
                && known != block
            {
                return Err(RemoteDecodeError::BlockHashMismatch(block.number));
            }
        }
    }
    let mut explicit_headers = HashSet::new();
    let mut explicit_progress = HashSet::new();
    let mut log_identities = HashSet::new();
    let mut transaction_indexes = HashMap::<(B256, B256), u64>::new();
    let mut transaction_hashes = HashMap::<(B256, u64), B256>::new();
    let mut last_event_order = None;
    let mut records = Vec::with_capacity(wire_records.len());
    let mut canonical_progress = None;
    for wire_record in wire_records {
        let audience = decode_audience(
            wire_record.canonical_audience,
            wire_record.owner_ids,
            authoritative_owners,
        )?;
        let scope = decode_delivery_scope(wire_record.scope)?;
        if scope == RuntimeDeliveryScope::OwnerCatchup && matches!(audience, DeliveryAudience::All)
        {
            return Err(RemoteDecodeError::OwnerCatchupAudience);
        }
        let event = wire_record
            .event
            .ok_or(RemoteDecodeError::MissingField("event_record.event"))?;
        let event = event
            .event
            .ok_or(RemoteDecodeError::MissingField("chain_event.event"))?;
        let event_order = match &event {
            chain_event::Event::BlockHeader(header) => (
                header.block.as_ref().map_or(0, |block| block.number),
                0_u8,
                0,
                0,
            ),
            chain_event::Event::Log(log) => {
                (log.block_number, 1_u8, log.transaction_index, log.log_index)
            }
            chain_event::Event::BlockProgress(progress) => (
                progress.block.as_ref().map_or(0, |block| block.number),
                2_u8,
                0,
                0,
            ),
        };
        if last_event_order.is_some_and(|previous| event_order < previous) {
            return Err(RemoteDecodeError::EventOrder);
        }
        last_event_order = Some(event_order);
        match event {
            chain_event::Event::BlockHeader(header) => {
                let wire = header
                    .block
                    .ok_or(RemoteDecodeError::MissingField("block_header.block"))?;
                let block = runtime_block_ref(&wire)?;
                if !explicit_headers.insert(block.number) {
                    return Err(RemoteDecodeError::DuplicateBlock(block.number));
                }
                let hash = block.hash;
                if header.consensus_header_rlp.is_empty() {
                    return Err(RemoteDecodeError::MissingHeaderRlp);
                }
                let mut encoded = header.consensus_header_rlp.as_slice();
                let inner = ConsensusHeader::decode(&mut encoded)
                    .map_err(|error| RemoteDecodeError::HeaderRlp(error.to_string()))?;
                if !encoded.is_empty() {
                    return Err(RemoteDecodeError::HeaderRlp(
                        "trailing bytes after consensus header".into(),
                    ));
                }
                if inner.hash_slow() != hash {
                    return Err(RemoteDecodeError::HeaderHash);
                }
                if inner.number != block.number
                    || Some(inner.parent_hash) != block.parent_hash
                    || Some(inner.timestamp) != block.timestamp
                {
                    return Err(RemoteDecodeError::HeaderMetadata);
                }
                let rpc_header = RpcHeader {
                    hash,
                    inner,
                    total_difficulty: decode_u256(
                        &header.total_difficulty,
                        "block_header.total_difficulty",
                    )?,
                    size: decode_u256(&header.size, "block_header.size")?,
                };
                if let Some(known) = blocks.insert(block.number, block)
                    && known != block
                {
                    return Err(RemoteDecodeError::BlockHashMismatch(block.number));
                }
                records.push(ReactiveInputDelivery::new(
                    ReactiveInputRecord::new(
                        ReactiveInput::BlockHeader(rpc_header),
                        included_context(expected_chain_id, block, None, None, input_source(scope)),
                    ),
                    audience,
                    scope,
                ));
            }
            chain_event::Event::Log(log) => {
                let block_hash = decode_b256(&log.block_hash, "log.block_hash")?;
                let transaction_hash = decode_b256(&log.transaction_hash, "log.transaction_hash")?;
                if !log_identities.insert((block_hash, log.log_index)) {
                    return Err(RemoteDecodeError::DuplicateLog(log.log_index));
                }
                if transaction_indexes
                    .insert((block_hash, transaction_hash), log.transaction_index)
                    .is_some_and(|known| known != log.transaction_index)
                    || transaction_hashes
                        .insert((block_hash, log.transaction_index), transaction_hash)
                        .is_some_and(|known| known != transaction_hash)
                {
                    return Err(RemoteDecodeError::TransactionIdentity);
                }
                let block = blocks
                    .get(&log.block_number)
                    .copied()
                    .unwrap_or(RuntimeBlockRef {
                        number: log.block_number,
                        hash: block_hash,
                        parent_hash: None,
                        timestamp: Some(log.block_timestamp),
                    });
                if block.hash != block_hash || block.timestamp != Some(log.block_timestamp) {
                    return Err(RemoteDecodeError::BlockHashMismatch(log.block_number));
                }
                blocks.entry(log.block_number).or_insert(block);
                let rpc_log = decode_log(&log)?;
                let context = if log.removed {
                    ReactiveContext {
                        chain_id: Some(expected_chain_id),
                        source: input_source(scope),
                        chain_status: ChainStatus::Reorged {
                            dropped_from: block,
                        },
                        block: Some(block),
                        transaction_index: Some(log.transaction_index),
                        log_index: Some(log.log_index),
                    }
                } else {
                    included_context(
                        expected_chain_id,
                        block,
                        Some(log.transaction_index),
                        Some(log.log_index),
                        input_source(scope),
                    )
                };
                records.push(ReactiveInputDelivery::new(
                    ReactiveInputRecord::new(ReactiveInput::Log(rpc_log), context),
                    audience,
                    scope,
                ));
            }
            chain_event::Event::BlockProgress(progress) => {
                if scope == RuntimeDeliveryScope::OwnerCatchup {
                    return Err(RemoteDecodeError::OwnerProgress);
                }
                let wire = progress
                    .block
                    .ok_or(RemoteDecodeError::MissingField("block_progress.block"))?;
                let block = runtime_block_ref(&wire)?;
                if !explicit_progress.insert(block.number) {
                    return Err(RemoteDecodeError::DuplicateBlock(block.number));
                }
                if let Some(known) = blocks.insert(block.number, block)
                    && known != block
                {
                    return Err(RemoteDecodeError::BlockHashMismatch(block.number));
                }
                // Runtime chain controls are applied after every record in the
                // envelope. Retaining an earlier page-level progress proof
                // would therefore regress behind a later canonical record.
                // Every proof is still decoded and identity-checked above;
                // only the highest authenticated coverage boundary is needed
                // to certify the complete ordered page.
                canonical_progress = Some(block);
            }
        }
    }
    Ok(
        ReactiveInputBatch::from_deliveries(records).with_chain_controls(
            canonical_progress
                .into_iter()
                .map(ChainControl::CanonicalProgress),
        ),
    )
}

fn decode_audience(
    canonical: bool,
    owner_ids: Vec<String>,
    authoritative_owners: &HashSet<&str>,
) -> Result<DeliveryAudience, RemoteDecodeError> {
    if canonical {
        if !owner_ids.is_empty() {
            return Err(RemoteDecodeError::Audience);
        }
        return Ok(DeliveryAudience::All);
    }
    if owner_ids.is_empty() {
        return Err(RemoteDecodeError::Audience);
    }
    let mut seen = HashSet::with_capacity(owner_ids.len());
    let mut owners = Vec::with_capacity(owner_ids.len());
    for owner in owner_ids {
        if owner.is_empty()
            || !authoritative_owners.contains(owner.as_str())
            || !seen.insert(owner.clone())
        {
            return Err(RemoteDecodeError::Audience);
        }
        owners.push(HandlerId::try_new(owner).map_err(|_| RemoteDecodeError::Audience)?);
    }
    Ok(DeliveryAudience::Owners(owners))
}

fn decode_delivery_scope(scope: i32) -> Result<RuntimeDeliveryScope, RemoteDecodeError> {
    match WireDeliveryScope::try_from(scope) {
        Ok(WireDeliveryScope::Canonical) => Ok(RuntimeDeliveryScope::Canonical),
        Ok(WireDeliveryScope::CanonicalProgress) => Ok(RuntimeDeliveryScope::CanonicalProgress),
        Ok(WireDeliveryScope::OwnerCatchup) => Ok(RuntimeDeliveryScope::OwnerCatchup),
        Ok(WireDeliveryScope::Unspecified) | Err(_) => Err(RemoteDecodeError::DeliveryScope(scope)),
    }
}

const fn input_source(scope: RuntimeDeliveryScope) -> InputSource {
    match scope {
        RuntimeDeliveryScope::Canonical => InputSource::Subscription,
        RuntimeDeliveryScope::CanonicalProgress | RuntimeDeliveryScope::OwnerCatchup => {
            InputSource::Backfill
        }
        _ => InputSource::Subscription,
    }
}

fn decode_log(log: &WireLog) -> Result<RpcLog, RemoteDecodeError> {
    if log.topics.len() > 4 {
        return Err(RemoteDecodeError::TopicCount(log.topics.len()));
    }
    let address = decode_address(&log.address, "log.address")?;
    let topics = log
        .topics
        .iter()
        .map(|topic| decode_b256(topic, "log.topic"))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RpcLog {
        inner: PrimitiveLog::new_unchecked(address, topics, Bytes::from(log.data.clone())),
        block_hash: Some(decode_b256(&log.block_hash, "log.block_hash")?),
        block_number: Some(log.block_number),
        block_timestamp: Some(log.block_timestamp),
        transaction_hash: Some(decode_b256(&log.transaction_hash, "log.transaction_hash")?),
        transaction_index: Some(log.transaction_index),
        log_index: Some(log.log_index),
        removed: log.removed,
    })
}

fn runtime_block_ref(
    block: &evm_fork_cache_event_protocol::v1::BlockRef,
) -> Result<RuntimeBlockRef, RemoteDecodeError> {
    Ok(RuntimeBlockRef {
        number: block.number,
        hash: decode_b256(&block.hash, "block.hash")?,
        parent_hash: Some(decode_b256(&block.parent_hash, "block.parent_hash")?),
        timestamp: Some(block.timestamp),
    })
}

fn included_context(
    chain_id: u64,
    block: RuntimeBlockRef,
    transaction_index: Option<u64>,
    log_index: Option<u64>,
    source: InputSource,
) -> ReactiveContext {
    ReactiveContext {
        chain_id: Some(chain_id),
        source,
        chain_status: ChainStatus::Included {
            block,
            confirmations: 0,
        },
        block: Some(block),
        transaction_index,
        log_index,
    }
}

fn decode_address(bytes: &[u8], field: &'static str) -> Result<Address, RemoteDecodeError> {
    if bytes.len() != 20 {
        return Err(RemoteDecodeError::Width {
            field,
            expected: 20,
            received: bytes.len(),
        });
    }
    Ok(Address::from_slice(bytes))
}

fn decode_b256(bytes: &[u8], field: &'static str) -> Result<B256, RemoteDecodeError> {
    if bytes.len() != 32 {
        return Err(RemoteDecodeError::Width {
            field,
            expected: 32,
            received: bytes.len(),
        });
    }
    Ok(B256::from_slice(bytes))
}

fn decode_u256(bytes: &[u8], field: &'static str) -> Result<Option<U256>, RemoteDecodeError> {
    if bytes.len() > 32 {
        return Err(RemoteDecodeError::QuantityWidth {
            field,
            received: bytes.len(),
        });
    }
    Ok((!bytes.is_empty()).then(|| U256::from_be_slice(bytes)))
}

#[derive(Debug, thiserror::Error)]
enum RemoteDecodeError {
    #[error("remote data delivery contains no records")]
    EmptyData,
    #[error("remote barrier identifier is empty")]
    EmptyBarrierId,
    #[error("remote delivery is missing `{0}`")]
    MissingField(&'static str),
    #[error("remote delivery session mismatch: expected `{expected}`, received `{received}`")]
    Session { expected: String, received: String },
    #[error("remote delivery chain mismatch: expected {expected}, received {received}")]
    Chain { expected: u64, received: u64 },
    #[error("remote delivery token does not encode its sequence")]
    DeliveryToken,
    #[error("remote delivery cursor does not match its query revision and sequence")]
    Cursor,
    #[error("remote delivery cursor authority is invalid: {0}")]
    CursorAuthority(&'static str),
    #[error("remote finality kind {0} is invalid")]
    Finality(i32),
    #[error("`{field}` must be {expected} bytes, got {received}")]
    Width {
        field: &'static str,
        expected: usize,
        received: usize,
    },
    #[error("`{field}` must be at most 32 bytes, got {received}")]
    QuantityWidth {
        field: &'static str,
        received: usize,
    },
    #[error("invalid consensus header RLP: {0}")]
    HeaderRlp(String),
    #[error("full block-header delivery is missing consensus header RLP")]
    MissingHeaderRlp,
    #[error("consensus header RLP hash does not match its block reference")]
    HeaderHash,
    #[error("consensus header number, parent, or timestamp does not match its block reference")]
    HeaderMetadata,
    #[error("log block hash does not match block event at height {0}")]
    BlockHashMismatch(u64),
    #[error("block {0} appears more than once as a header/progress record")]
    DuplicateBlock(u64),
    #[error("wire delivery audience is empty, duplicated, or contradictory")]
    Audience,
    #[error("wire delivery scope {0} is unspecified or unknown")]
    DeliveryScope(i32),
    #[error("owner catch-up cannot emit canonical block progress")]
    OwnerProgress,
    #[error("owner catch-up must target named owners rather than canonical broadcast")]
    OwnerCatchupAudience,
    #[error("EVM logs may carry at most four topics, got {0}")]
    TopicCount(usize),
    #[error("log identity with index {0} appears more than once in one delivery")]
    DuplicateLog(u64),
    #[error("remote data records are not in canonical event order")]
    EventOrder,
    #[error("remote logs disagree on transaction hash/index identity within a block")]
    TransactionIdentity,
}
