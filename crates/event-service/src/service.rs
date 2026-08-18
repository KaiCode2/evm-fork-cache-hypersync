use std::{
    collections::HashSet,
    pin::Pin,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use evm_fork_cache_event_protocol::{
    MAX_MESSAGE_SIZE_BYTES, PROTOCOL_VERSION,
    v1::{
        Acknowledge, AcknowledgementCommitted, ApplyDesiredState, BlockRef, ClientMessage, Cursor,
        Delivery, ErrorCode, Hello, HelloAccepted, PendingDeliveryResume, ProtocolError,
        RuntimeCheckpointPosition, ServerMessage, ServiceLimits, SourceCapabilities,
        client_message,
        event_stream_server::{EventStream, EventStreamServer},
        portable_interest, server_message,
    },
};
use futures::{Stream, StreamExt};
use tokio::sync::{Mutex, mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, metadata::MetadataMap};

use prost::Message;

use crate::{
    DesiredStateError, PersistedSession, SessionStore, SessionStoreError, validate_desired_state,
};

/// Process-local identity for one prepared desired-state candidate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PreparationId(u64);

impl PreparationId {
    const fn new(value: u64) -> Self {
        Self(value)
    }
}

/// Durable, provider-neutral inputs for producing one source delivery.
///
/// The service constructs this request only from committed session state. Its
/// private fields and accessors leave room for later source constraints without
/// forcing providers to destructure a version-sensitive public shape.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct DeliveryRequest<'a> {
    desired_state: &'a ApplyDesiredState,
    acknowledged_cursor: Option<&'a Cursor>,
    required_reorg_anchor: Option<&'a BlockRef>,
}

impl<'a> DeliveryRequest<'a> {
    /// Construct an unconstrained poll after the committed cursor.
    pub const fn new(
        desired_state: &'a ApplyDesiredState,
        acknowledged_cursor: Option<&'a Cursor>,
    ) -> Self {
        Self {
            desired_state,
            acknowledged_cursor,
            required_reorg_anchor: None,
        }
    }

    /// Require the first canonical replacement delivery to certify this anchor.
    #[must_use]
    pub const fn with_required_reorg_anchor(mut self, anchor: Option<&'a BlockRef>) -> Self {
        self.required_reorg_anchor = anchor;
        self
    }

    /// Complete authoritative desired state for the active revision.
    pub const fn desired_state(self) -> &'a ApplyDesiredState {
        self.desired_state
    }

    /// Last transport cursor committed by the durable service.
    pub const fn acknowledged_cursor(self) -> Option<&'a Cursor> {
        self.acknowledged_cursor
    }

    /// Exact replacement anchor promised by an acknowledged reorg, if any.
    ///
    /// A source must return `None` until it can honor this constraint, or return
    /// a delivery that explicitly certifies the anchor. A blockful barrier must
    /// stop at the anchor. Data may end later only when it includes a complete
    /// continuous descendant suffix through its terminal cursor. Ignoring this
    /// value is a source contract violation and the service rejects the output.
    pub const fn required_reorg_anchor(self) -> Option<&'a BlockRef> {
        self.required_reorg_anchor
    }
}

/// Provider-neutral source boundary used by the durable event service.
///
/// # Cancellation and failure atomicity
///
/// The service applies deadlines by dropping these async futures. Every method
/// must therefore be cancellation-safe as well as failure-atomic: if its future
/// is dropped or returns `Err`, a retry with the same durable inputs must remain
/// valid and must not observe a partially published transition. Implementations
/// may stage idempotent work behind a preparation or delivery token, but must
/// bound and eventually release such work. Durable service state, not an
/// in-memory source task, is the authority after restart.
#[async_trait]
pub trait EventSource: Send + Sync + 'static {
    /// Advertise the guarantees and topology implemented by this source.
    fn capabilities(&self) -> SourceCapabilities;

    /// Advertise guarantees for a particular chain. Multi-chain sources should
    /// override this; single-capability sources retain the legacy behavior.
    fn capabilities_for_chain(&self, _chain_id: u64) -> SourceCapabilities {
        self.capabilities()
    }

    /// Validate and fully prepare a desired state before it becomes durable.
    ///
    /// A source that establishes a provider-native activation boundary may
    /// return the corresponding portable cursor. The service persists that
    /// cursor in the activation barrier, so acknowledging the barrier cannot
    /// rewind a newly prepared source (for example, from archive head to
    /// genesis). Sources without a prepared boundary return `None`.
    ///
    /// Cancellation or `Err` must leave the active revision unchanged. Any
    /// staged candidate must remain safely retryable by `preparation_id` or
    /// removable by [`EventSource::abort_desired_state`].
    ///
    /// # Errors
    ///
    /// Returns [`EventSourceError`] when the desired state is invalid or
    /// unsupported, resources are exhausted, the provider is unavailable, or
    /// preparation cannot complete atomically.
    async fn prepare_desired_state(
        &self,
        _preparation_id: PreparationId,
        _desired_state: &ApplyDesiredState,
        _acknowledged_cursor: Option<&Cursor>,
    ) -> Result<Option<Cursor>, EventSourceError> {
        Ok(None)
    }

    /// Activate a previously prepared desired state after its durable commit.
    ///
    /// Cancellation or `Err` must not expose a partially activated revision;
    /// the previous active source must remain usable and the candidate must
    /// remain retryable or abortable.
    ///
    /// # Errors
    ///
    /// Returns [`EventSourceError`] when the prepared candidate is missing or
    /// inconsistent, resources are unavailable, or activation cannot complete
    /// atomically.
    async fn activate_desired_state(
        &self,
        _preparation_id: PreparationId,
        _desired_state: &ApplyDesiredState,
        _acknowledged_cursor: Option<&Cursor>,
    ) -> Result<(), EventSourceError> {
        Ok(())
    }

    /// Discard a prepared desired state when its durable commit fails.
    ///
    /// This operation must be idempotent. Cancellation or `Err` must leave a
    /// later abort retry able to finish cleanup without losing the active source.
    ///
    /// # Errors
    ///
    /// Returns [`EventSourceError`] when cleanup cannot currently complete.
    /// Implementations must leave the same abort safely retryable.
    async fn abort_desired_state(
        &self,
        _preparation_id: PreparationId,
        _desired_state: &ApplyDesiredState,
    ) -> Result<(), EventSourceError> {
        Ok(())
    }

    /// Release all process-local state for a stream that owned this durable
    /// session. The service invokes this at most once for each successfully
    /// negotiated lease, after it stops using every other source method.
    ///
    /// Implementations must be idempotent and cancellation-safe. An error or
    /// cancellation at [`EventService::with_source_operation_timeout`] may mean
    /// cleanup is partial. In either case the service retains the lease (fail
    /// closed), so another generation cannot overlap it; a service restart is
    /// needed to recover that session identity.
    ///
    /// # Errors
    ///
    /// Returns [`EventSourceError`] when process-local session cleanup cannot
    /// complete. The service then retains the lease to prevent overlap.
    async fn release_session(
        &self,
        _session_id: &str,
        _chain_id: u64,
    ) -> Result<(), EventSourceError> {
        Ok(())
    }

    /// Produce the next delivery after the durable cursor, or `None` when caught up.
    ///
    /// Implementations must inspect [`DeliveryRequest::required_reorg_anchor`].
    /// Returning `None` or a typed unsupported error is valid when the source
    /// cannot yet certify it; returning output that skips it is a contract
    /// violation and will be rejected before outbox persistence.
    ///
    /// Cancellation or `Err` must not advance the acknowledged source cursor.
    /// Repeating the call with the same inputs must return the same pending
    /// token and payload, or reconstruct an equivalent delivery without skipping
    /// data. Provider reads performed before cancellation are observations only.
    ///
    /// # Errors
    ///
    /// Returns [`EventSourceError`] for invalid/unsupported requests, resource
    /// exhaustion, source unavailability, or an internal continuity failure.
    async fn next_delivery(
        &self,
        request: DeliveryRequest<'_>,
    ) -> Result<Option<Delivery>, EventSourceError>;

    /// Wait until the source reports that a newer archive height may be
    /// available. Providers without a push signal can retain the default;
    /// [`EventService`] always keeps its polling interval as a fallback.
    /// The request is identical to the one used for delivery production, so a
    /// push source can wait for a required reorg anchor instead of waking on a
    /// newer but insufficient source hint.
    /// This future is observational: cancellation or `Err` must not consume a
    /// unique wakeup needed for correctness, because polling remains authoritative.
    ///
    /// # Errors
    ///
    /// Returns [`EventSourceError`] when the wakeup request is invalid or the
    /// source cannot observe updates. Failure must not consume correctness
    /// state.
    async fn wait_for_update(&self, _request: DeliveryRequest<'_>) -> Result<(), EventSourceError> {
        std::future::pending().await
    }

    /// Reconcile provider-local state after the service has already committed
    /// runtime ingestion and its durable cursor.
    ///
    /// This is a post-commit hook, not a transaction participant. It must be
    /// idempotent by delivery token/sequence and treat `committed_cursor` as the
    /// authority. Cancellation or `Err` cannot roll back the service commit; a
    /// retry (including after restart with no pending in-memory delivery) must
    /// converge provider-local state to that committed cursor without replaying
    /// side effects or rejecting an already-applied acknowledgement.
    ///
    /// # Errors
    ///
    /// Returns [`EventSourceError`] when provider-local reconciliation cannot
    /// currently converge to the already committed cursor. The error cannot
    /// roll back service authority.
    async fn acknowledge(
        &self,
        chain_id: u64,
        acknowledgement: &Acknowledge,
        committed_cursor: &Cursor,
    ) -> Result<(), EventSourceError>;
}

/// Stable classification for source failures. The public protocol exposes the
/// class and retry disposition, never provider credentials or raw internals.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum EventSourceErrorKind {
    /// The requested source operation or query is malformed.
    InvalidRequest,
    /// The source cannot provide the requested capability.
    Unsupported,
    /// A configured or provider resource limit was exceeded.
    ResourceExhausted,
    /// The source is temporarily unavailable and the operation may be retried.
    Unavailable,
    /// The source encountered an unexpected non-client failure.
    Internal,
}

/// Typed source worker failure surfaced through the versioned protocol.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("event source failed ({kind:?}): {message}")]
#[non_exhaustive]
pub struct EventSourceError {
    /// Stable protocol-facing failure classification.
    pub kind: EventSourceErrorKind,
    /// Sanitized diagnostic message safe to return to the client.
    pub message: String,
}

impl EventSourceError {
    /// Construct an invalid-request source failure.
    pub fn invalid(message: impl Into<String>) -> Self {
        Self {
            kind: EventSourceErrorKind::InvalidRequest,
            message: message.into(),
        }
    }

    /// Construct an unsupported-operation source failure.
    pub fn unsupported(message: impl Into<String>) -> Self {
        Self {
            kind: EventSourceErrorKind::Unsupported,
            message: message.into(),
        }
    }

    /// Construct a resource-exhaustion source failure.
    pub fn resource_exhausted(message: impl Into<String>) -> Self {
        Self {
            kind: EventSourceErrorKind::ResourceExhausted,
            message: message.into(),
        }
    }

    /// Construct a retryable source-unavailable failure.
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self {
            kind: EventSourceErrorKind::Unavailable,
            message: message.into(),
        }
    }

    /// Construct an unexpected internal source failure.
    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            kind: EventSourceErrorKind::Internal,
            message: message.into(),
        }
    }
}

/// Durable tonic event service.
pub struct EventService<P> {
    store: Arc<Mutex<SessionStore>>,
    provider: Arc<P>,
    poll_interval: Duration,
    client_hello_timeout: Duration,
    source_operation_timeout: Duration,
    client_send_timeout: Duration,
    next_preparation_id: Arc<AtomicU64>,
    active_sessions: Arc<StdMutex<HashSet<(String, u64)>>>,
    limits: EventServiceLimits,
    authorizer: Arc<dyn SessionAuthorizer>,
    metrics: EventServiceMetrics,
    shutdown: watch::Sender<bool>,
}

/// Cloneable signal used to close active event sessions during server shutdown.
#[derive(Clone)]
pub struct EventServiceShutdown {
    sender: watch::Sender<bool>,
}

impl EventServiceShutdown {
    /// Signal every active session loop to release its source state and lease.
    pub fn shutdown(&self) {
        self.sender.send_replace(true);
    }
}

/// Invalid event-service construction parameters.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum EventServiceConfigError {
    /// Tokio intervals cannot represent a zero-duration polling fallback.
    #[error("event-service poll interval must be greater than zero")]
    ZeroPollInterval,
    #[error("event-service source operation timeout must be greater than zero")]
    /// A zero source-operation timeout was configured.
    ZeroSourceOperationTimeout,
    #[error("event-service client Hello timeout must be greater than zero")]
    /// A zero client-Hello timeout was configured.
    ZeroClientHelloTimeout,
    #[error("event-service client send timeout must be greater than zero")]
    /// A zero outbound client-send timeout was configured.
    ZeroClientSendTimeout,
}

/// Lock-free operational counters shared by all service clones.
#[derive(Clone, Default)]
pub struct EventServiceMetrics {
    inner: Arc<EventServiceMetricCounters>,
}

#[derive(Default)]
struct EventServiceMetricCounters {
    active_sessions: AtomicU64,
    sessions_accepted: AtomicU64,
    authentication_rejections: AtomicU64,
    lease_rejections: AtomicU64,
    desired_states_committed: AtomicU64,
    deliveries_persisted: AtomicU64,
    deliveries_replayed: AtomicU64,
    acknowledgements_committed: AtomicU64,
    source_errors: AtomicU64,
}

/// Point-in-time service health counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct EventServiceMetricsSnapshot {
    /// Streams currently holding a session lease.
    pub active_sessions: u64,
    /// Session streams accepted since service start.
    pub sessions_accepted: u64,
    /// Connections rejected by the configured authorizer.
    pub authentication_rejections: u64,
    /// Streams rejected because the session lease or capacity was unavailable.
    pub lease_rejections: u64,
    /// Desired-state revisions durably committed.
    pub desired_states_committed: u64,
    /// Deliveries durably placed in a session outbox.
    pub deliveries_persisted: u64,
    /// Pending deliveries replayed from a durable outbox.
    pub deliveries_replayed: u64,
    /// New delivery acknowledgements durably committed.
    pub acknowledgements_committed: u64,
    /// Source operations that returned an error.
    pub source_errors: u64,
}

impl EventServiceMetrics {
    /// Snapshot monotonic counters and the current active-session gauge.
    pub fn snapshot(&self) -> EventServiceMetricsSnapshot {
        EventServiceMetricsSnapshot {
            active_sessions: self.inner.active_sessions.load(Ordering::Relaxed),
            sessions_accepted: self.inner.sessions_accepted.load(Ordering::Relaxed),
            authentication_rejections: self.inner.authentication_rejections.load(Ordering::Relaxed),
            lease_rejections: self.inner.lease_rejections.load(Ordering::Relaxed),
            desired_states_committed: self.inner.desired_states_committed.load(Ordering::Relaxed),
            deliveries_persisted: self.inner.deliveries_persisted.load(Ordering::Relaxed),
            deliveries_replayed: self.inner.deliveries_replayed.load(Ordering::Relaxed),
            acknowledgements_committed: self
                .inner
                .acknowledgements_committed
                .load(Ordering::Relaxed),
            source_errors: self.inner.source_errors.load(Ordering::Relaxed),
        }
    }
}

/// Connection-level authorization hook for bearer, mTLS, or gateway identity.
pub trait SessionAuthorizer: Send + Sync + 'static {
    /// Authorize a new transport connection from its request metadata.
    ///
    /// # Errors
    ///
    /// Returns a client-safe rejection reason when the connection is not
    /// authorized. The message must not contain credentials or private
    /// authentication material.
    fn authorize(&self, metadata: &MetadataMap) -> Result<(), String>;

    /// Authorize the principal for the durable identity requested by `Hello`.
    /// The default preserves connection-only policies while allowing mTLS or
    /// bearer implementations to prevent cross-session takeover.
    ///
    /// # Errors
    ///
    /// Returns a client-safe rejection reason when the principal may not claim
    /// the requested session and chain. The message must not expose secrets.
    fn authorize_session(
        &self,
        metadata: &MetadataMap,
        _session_id: &str,
        _chain_id: u64,
    ) -> Result<(), String> {
        self.authorize(metadata)
    }
}

struct AllowAll;

impl SessionAuthorizer for AllowAll {
    fn authorize(&self, _metadata: &MetadataMap) -> Result<(), String> {
        Ok(())
    }
}

/// Resource policy enforced before desired state reaches a provider.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct EventServiceLimits {
    /// Maximum owners in one desired state.
    pub max_owners: usize,
    /// Maximum portable interests attached to one owner.
    pub max_interests_per_owner: usize,
    /// Maximum address alternatives in one log interest.
    pub max_addresses_per_log_interest: usize,
    /// Maximum alternatives in one indexed log topic.
    pub max_values_per_topic: usize,
    /// Maximum number of blocks in one explicitly bounded backfill.
    pub max_bounded_backfill_blocks: u64,
    /// Maximum UTF-8 bytes in a session or owner identifier.
    pub max_identifier_bytes: usize,
    /// Maximum interests across the entire desired state.
    pub max_total_interests: usize,
    /// Maximum address and topic alternatives across the desired state.
    pub max_total_filter_values: usize,
    /// Maximum encoded desired-state size accepted from a client.
    pub max_desired_state_bytes: usize,
    /// Maximum encoded delivery size persisted or sent to a client.
    pub max_delivery_bytes: usize,
    /// Maximum simultaneously leased session streams.
    pub max_active_sessions: usize,
    /// Maximum durable session identities retained in the SQLite authority.
    pub max_persisted_sessions: usize,
}

impl Default for EventServiceLimits {
    fn default() -> Self {
        Self {
            max_owners: 4_096,
            max_interests_per_owner: 256,
            max_addresses_per_log_interest: 4_096,
            max_values_per_topic: 1_024,
            max_bounded_backfill_blocks: 10_000_000,
            max_identifier_bytes: 256,
            max_total_interests: 16_384,
            max_total_filter_values: 1_000_000,
            max_desired_state_bytes: 16 * 1024 * 1024,
            max_delivery_bytes: MAX_MESSAGE_SIZE_BYTES,
            max_active_sessions: 4_096,
            max_persisted_sessions: 65_536,
        }
    }
}

impl EventServiceLimits {
    fn wire(self) -> ServiceLimits {
        ServiceLimits {
            max_owners: self.max_owners.min(u32::MAX as usize) as u32,
            max_interests_per_owner: self.max_interests_per_owner.min(u32::MAX as usize) as u32,
            max_addresses_per_log_interest: self
                .max_addresses_per_log_interest
                .min(u32::MAX as usize) as u32,
            max_values_per_topic: self.max_values_per_topic.min(u32::MAX as usize) as u32,
            max_bounded_backfill_blocks: self.max_bounded_backfill_blocks,
            max_identifier_bytes: self.max_identifier_bytes.min(u32::MAX as usize) as u32,
            max_total_interests: self.max_total_interests.min(u32::MAX as usize) as u32,
            max_total_filter_values: self.max_total_filter_values.min(u64::MAX as usize) as u64,
            max_desired_state_bytes: self.max_desired_state_bytes.min(u64::MAX as usize) as u64,
            max_delivery_bytes: delivery_limit(self).min(u64::MAX as usize) as u64,
            max_active_sessions: self.max_active_sessions.min(u32::MAX as usize) as u32,
            max_persisted_sessions: self.max_persisted_sessions.min(u64::MAX as usize) as u64,
        }
    }
}

impl<P> EventService<P>
where
    P: EventSource,
{
    /// Construct a service sharing one durable session database and provider.
    ///
    /// # Errors
    ///
    /// Returns [`EventServiceConfigError::ZeroPollInterval`] when the polling
    /// fallback is zero.
    pub fn new(
        store: Arc<Mutex<SessionStore>>,
        provider: Arc<P>,
        poll_interval: Duration,
    ) -> Result<Self, EventServiceConfigError> {
        if poll_interval.is_zero() {
            return Err(EventServiceConfigError::ZeroPollInterval);
        }
        let (shutdown, _) = watch::channel(false);
        Ok(Self {
            store,
            provider,
            poll_interval,
            client_hello_timeout: Duration::from_secs(10),
            source_operation_timeout: Duration::from_secs(30),
            client_send_timeout: Duration::from_secs(30),
            next_preparation_id: Arc::new(AtomicU64::new(1)),
            active_sessions: Arc::new(StdMutex::new(HashSet::new())),
            limits: EventServiceLimits::default(),
            authorizer: Arc::new(AllowAll),
            metrics: EventServiceMetrics::default(),
            shutdown,
        })
    }

    /// Replace the default resource policy advertised and enforced by the service.
    pub fn with_limits(mut self, limits: EventServiceLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Bound source operations that would otherwise stall session control
    /// traffic indefinitely. Provider wakeups remain independently cancelable.
    ///
    /// # Errors
    ///
    /// Returns [`EventServiceConfigError::ZeroSourceOperationTimeout`] when
    /// `timeout` is zero.
    pub fn with_source_operation_timeout(
        mut self,
        timeout: Duration,
    ) -> Result<Self, EventServiceConfigError> {
        if timeout.is_zero() {
            return Err(EventServiceConfigError::ZeroSourceOperationTimeout);
        }
        self.source_operation_timeout = timeout;
        Ok(self)
    }

    /// Bound how long a newly opened stream may remain unnegotiated. This
    /// protects the pre-lease path, which is intentionally outside the active
    /// durable-session quota.
    ///
    /// # Errors
    ///
    /// Returns [`EventServiceConfigError::ZeroClientHelloTimeout`] when
    /// `timeout` is zero.
    pub fn with_client_hello_timeout(
        mut self,
        timeout: Duration,
    ) -> Result<Self, EventServiceConfigError> {
        if timeout.is_zero() {
            return Err(EventServiceConfigError::ZeroClientHelloTimeout);
        }
        self.client_hello_timeout = timeout;
        Ok(self)
    }

    /// Bound response backpressure from a client that stops reading its stream.
    ///
    /// # Errors
    ///
    /// Returns [`EventServiceConfigError::ZeroClientSendTimeout`] when
    /// `timeout` is zero.
    pub fn with_client_send_timeout(
        mut self,
        timeout: Duration,
    ) -> Result<Self, EventServiceConfigError> {
        if timeout.is_zero() {
            return Err(EventServiceConfigError::ZeroClientSendTimeout);
        }
        self.client_send_timeout = timeout;
        Ok(self)
    }

    /// Install a connection-level authorization policy.
    pub fn with_authorizer(mut self, authorizer: Arc<dyn SessionAuthorizer>) -> Self {
        self.authorizer = authorizer;
        self
    }

    /// Shared operational counters for health endpoints or metrics exporters.
    pub fn metrics(&self) -> EventServiceMetrics {
        self.metrics.clone()
    }

    /// Current number of leased sessions.
    pub fn active_session_count(&self) -> u64 {
        self.metrics.snapshot().active_sessions
    }

    /// Obtain a signal that should be triggered together with the tonic server's
    /// graceful-shutdown future so long-lived bidirectional streams can close.
    pub fn shutdown_handle(&self) -> EventServiceShutdown {
        EventServiceShutdown {
            sender: self.shutdown.clone(),
        }
    }

    /// Wrap this implementation in the generated tonic server.
    pub fn into_server(self) -> EventStreamServer<Self> {
        EventStreamServer::new(self)
            .max_decoding_message_size(MAX_MESSAGE_SIZE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_SIZE_BYTES)
    }
}

impl<P> Clone for EventService<P> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            provider: Arc::clone(&self.provider),
            poll_interval: self.poll_interval,
            client_hello_timeout: self.client_hello_timeout,
            source_operation_timeout: self.source_operation_timeout,
            client_send_timeout: self.client_send_timeout,
            next_preparation_id: Arc::clone(&self.next_preparation_id),
            active_sessions: Arc::clone(&self.active_sessions),
            limits: self.limits,
            authorizer: Arc::clone(&self.authorizer),
            metrics: self.metrics.clone(),
            shutdown: self.shutdown.clone(),
        }
    }
}

type ResponseStream = Pin<Box<dyn Stream<Item = Result<ServerMessage, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl<P> EventStream for EventService<P>
where
    P: EventSource,
{
    type SessionStream = ResponseStream;

    async fn session(
        &self,
        request: Request<tonic::Streaming<ClientMessage>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        if self.authorizer.authorize(request.metadata()).is_err() {
            self.metrics
                .inner
                .authentication_rejections
                .fetch_add(1, Ordering::Relaxed);
            return Err(Status::unauthenticated(
                "event session authentication failed",
            ));
        }
        if *self.shutdown.borrow() {
            return Err(Status::unavailable("event service is shutting down"));
        }
        let request_metadata = request.metadata().clone();
        let mut inbound = request.into_inner();
        let (outbound, receiver) = mpsc::channel(32);
        let store = Arc::clone(&self.store);
        let provider = Arc::clone(&self.provider);
        let next_preparation_id = Arc::clone(&self.next_preparation_id);
        let active_sessions = Arc::clone(&self.active_sessions);
        let poll_interval = self.poll_interval;
        let client_hello_timeout = self.client_hello_timeout;
        let source_operation_timeout = self.source_operation_timeout;
        let client_send_timeout = self.client_send_timeout;
        let limits = self.limits;
        let authorizer = Arc::clone(&self.authorizer);
        let metrics = self.metrics.clone();
        let shutdown = wait_for_service_shutdown(self.shutdown.subscribe());
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(poll_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let hello_deadline = tokio::time::sleep(client_hello_timeout);
            tokio::pin!(hello_deadline);
            tokio::pin!(shutdown);
            let mut identity: Option<(String, u64)> = None;
            let mut desired_state: Option<ApplyDesiredState> = None;
            let mut cursor: Option<Cursor> = None;
            let mut required_reorg_anchor: Option<BlockRef> = None;
            let mut has_pending = false;
            // A pending item loaded at Hello or already emitted on this stream
            // is a replay on its next send. A newly committed activation has
            // not been sent yet, so its first demand is the original delivery.
            let mut pending_was_sent = false;
            let mut delivery_requested = false;
            let mut session_lease: Option<SessionLease> = None;
            let mut position_confirmed = false;

            loop {
                let update_desired_state = desired_state.clone();
                let update_cursor = cursor.clone();
                let update_reorg_anchor = required_reorg_anchor.clone();
                tokio::select! {
                    () = &mut shutdown => {
                        break;
                    }
                    _ = &mut hello_deadline, if identity.is_none() => {
                        let _ = outbound.try_send(Err(Status::deadline_exceeded(
                            "event session Hello timed out",
                        )));
                        break;
                    }
                    inbound_message = inbound.next() => {
                        let Some(inbound_message) = inbound_message else { break };
                        let message = match inbound_message {
                            Ok(message) => message,
                            Err(_) => break,
                        };
                        match message.message {
                            Some(client_message::Message::Hello(hello)) => {
                                if identity.is_some() {
                                    if !send_error(&outbound, ErrorCode::InvalidInterest, "session already negotiated", 0, false).await { break; }
                                    continue;
                                }
                                if hello.protocol_version != PROTOCOL_VERSION {
                                    if !send_error(&outbound, ErrorCode::ProtocolVersion, "unsupported protocol version", 0, false).await { break; }
                                    continue;
                                }
                                if hello.session_id.len() > limits.max_identifier_bytes {
                                    if !send_error(&outbound, ErrorCode::ResourceExhausted, "session identifier exceeds the configured byte limit", 0, false).await { break; }
                                    continue;
                                }
                                if hello.session_id.is_empty() {
                                    if !send_error(&outbound, ErrorCode::InvalidInterest, "session identifier is empty", 0, false).await { break; }
                                    continue;
                                }
                                if authorizer.authorize_session(
                                    &request_metadata,
                                    &hello.session_id,
                                    hello.chain_id,
                                ).is_err() {
                                    metrics.inner.authentication_rejections.fetch_add(1, Ordering::Relaxed);
                                    if !send_error(&outbound, ErrorCode::Authentication, "event session is not authorized for this identity", 0, false).await { break; }
                                    continue;
                                }
                                let lease = match SessionLease::acquire(
                                    Arc::clone(&active_sessions),
                                    (hello.session_id.clone(), hello.chain_id),
                                    limits.max_active_sessions,
                                    metrics.clone(),
                                ) {
                                    Ok(lease) => lease,
                                    Err(LeaseError::InUse) => {
                                        metrics.inner.lease_rejections.fetch_add(1, Ordering::Relaxed);
                                        if !send_error(&outbound, ErrorCode::SessionInUse, "another connection owns this session", 0, true).await { break; }
                                        continue;
                                    }
                                    Err(LeaseError::Capacity) => {
                                        metrics.inner.lease_rejections.fetch_add(1, Ordering::Relaxed);
                                        if !send_error(&outbound, ErrorCode::ResourceExhausted, "active session capacity is exhausted", 0, true).await { break; }
                                        continue;
                                    }
                                    Err(LeaseError::Poisoned) => {
                                        let _ = send_internal(&outbound).await;
                                        break;
                                    }
                                };
                                let persisted = match store.lock().await.load(&hello.session_id, hello.chain_id) {
                                    Ok(persisted) => persisted,
                                    Err(_) => {
                                        let _ = send_internal(&outbound).await;
                                        break;
                                    }
                                };
                                let committed_revision = persisted.desired_state.as_ref().map_or(0, |state| state.new_revision);
                                let hello_position = match validate_hello_position(&hello, &persisted) {
                                    Ok(position) => position,
                                    Err(_) => {
                                        if !send_error(&outbound, ErrorCode::RevisionConflict, "client resume position does not match durable service state", committed_revision, false).await { break; }
                                        continue;
                                    }
                                };
                                if persisted.pending_delivery.as_ref().is_some_and(|delivery| {
                                    encoded_delivery_len(delivery) > MAX_MESSAGE_SIZE_BYTES
                                }) {
                                    if !send_error(&outbound, ErrorCode::ResourceExhausted, "durable pending delivery exceeds the hard transport limit", committed_revision, false).await { break; }
                                    continue;
                                }
                                session_lease = Some(lease);
                                let prepared = if let Some(committed) = persisted.desired_state.as_ref() {
                                    let Some(preparation_id) = allocate_preparation_id(
                                        next_preparation_id.as_ref(),
                                    ) else {
                                        let _ = send_error(
                                            &outbound,
                                            ErrorCode::ResourceExhausted,
                                            "source preparation identity space is exhausted",
                                            committed_revision,
                                            false,
                                        )
                                        .await;
                                        break;
                                    };
                                    if source_call(source_operation_timeout, provider.prepare_desired_state(
                                        preparation_id, committed, persisted.acknowledged_cursor.as_ref(),
                                    )).await.is_err() {
                                        metrics.inner.source_errors.fetch_add(1, Ordering::Relaxed);
                                        if source_call(
                                            source_operation_timeout,
                                            provider.abort_desired_state(preparation_id, committed),
                                        )
                                        .await
                                        .is_err()
                                        {
                                            metrics.inner.source_errors.fetch_add(1, Ordering::Relaxed);
                                        }
                                        if !send_source_unavailable(&outbound).await { break; }
                                        break;
                                    }
                                    Some((preparation_id, committed))
                                } else {
                                    None
                                };
                                if let Some((preparation_id, committed)) = prepared
                                    && source_call(source_operation_timeout, provider.activate_desired_state(
                                        preparation_id,
                                        committed,
                                        persisted.acknowledged_cursor.as_ref(),
                                    )).await.is_err()
                                {
                                    metrics.inner.source_errors.fetch_add(1, Ordering::Relaxed);
                                    let _ = source_call(source_operation_timeout, provider.abort_desired_state(preparation_id, committed)).await;
                                    if !send_source_unavailable(&outbound).await { break; }
                                    break;
                                }
                                identity = Some((hello.session_id.clone(), hello.chain_id));
                                position_confirmed = hello_position == HelloPosition::Confirmed;
                                metrics.inner.sessions_accepted.fetch_add(1, Ordering::Relaxed);
                                desired_state = persisted.desired_state.clone();
                                cursor = persisted.acknowledged_cursor.clone();
                                required_reorg_anchor = persisted.expected_reorg_tip.clone();
                                has_pending = persisted.pending_delivery.is_some();
                                pending_was_sent = has_pending;
                                delivery_requested = false;
                                if !send_outbound(&outbound, ServerMessage {
                                    message: Some(server_message::Message::HelloAccepted(HelloAccepted {
                                        protocol_version: PROTOCOL_VERSION,
                                        session_id: hello.session_id,
                                        chain_id: hello.chain_id,
                                        committed_revision,
                                        acknowledged_cursor: cursor.clone(),
                                        desired_state: desired_state.clone(),
                                        capabilities: Some(provider.capabilities_for_chain(hello.chain_id)),
                                        service_limits: Some(limits.wire()),
                                        runtime_checkpoint_position: Some(RuntimeCheckpointPosition {
                                            cursor: persisted.runtime_checkpoint_cursor.clone(),
                                        }),
                                    })),
                                }, client_send_timeout).await { break; }
                                // Pending deliveries are replayed only in response to
                                // DeliveryDemand. This keeps one replay per connection
                                // demand and avoids duplicate durable items after reconnect.
                            }
                            Some(client_message::Message::ApplyDesiredState(request)) => {
                                let Some((session_id, chain_id)) = &identity else {
                                    if !send_error(&outbound, ErrorCode::ProtocolVersion, "Hello must be sent first", 0, false).await { break; }
                                    continue;
                                };
                                if &request.session_id != session_id || request.chain_id != *chain_id {
                                    if !send_error(&outbound, ErrorCode::InvalidInterest, "desired state does not match negotiated session", 0, false).await { break; }
                                    continue;
                                }
                                if !position_confirmed {
                                    if !send_error(&outbound, ErrorCode::RevisionConflict, "runtime checkpoint proof is required on a new session", desired_state.as_ref().map_or(0, |state| state.new_revision), false).await { break; }
                                    continue;
                                }
                                if let Err(error) = validate_limits(&request, limits) {
                                    if !send_error(&outbound, ErrorCode::ResourceExhausted, error, request.expected_revision, false).await { break; }
                                    continue;
                                }
                                if let Err(error) = validate_desired_state(&request) {
                                    let (code, message) = desired_state_error(&error);
                                    if !send_error(&outbound, code, message, request.expected_revision, false).await { break; }
                                    continue;
                                }
                                if request.expected_revision > i64::MAX as u64
                                    || request.new_revision > i64::MAX as u64
                                {
                                    if !send_error(
                                        &outbound,
                                        ErrorCode::InvalidInterest,
                                        "desired-state revision exceeds the durable storage range",
                                        request.expected_revision,
                                        false,
                                    ).await { break; }
                                    continue;
                                }
                                let Some(preparation_id) = allocate_preparation_id(
                                    next_preparation_id.as_ref(),
                                ) else {
                                    if !send_error(
                                        &outbound,
                                        ErrorCode::ResourceExhausted,
                                        "source preparation identity space is exhausted",
                                        request.expected_revision,
                                        false,
                                    ).await { break; }
                                    continue;
                                };
                                let prepared_cursor = match source_call(source_operation_timeout, provider.prepare_desired_state(
                                    preparation_id,
                                    &request,
                                    cursor.as_ref(),
                                )).await {
                                    Ok(cursor) => cursor,
                                    Err(error) => {
                                        metrics.inner.source_errors.fetch_add(1, Ordering::Relaxed);
                                        let abort_failed = source_call(
                                            source_operation_timeout,
                                            provider.abort_desired_state(preparation_id, &request),
                                        )
                                        .await
                                        .is_err();
                                        if abort_failed {
                                            metrics.inner.source_errors.fetch_add(1, Ordering::Relaxed);
                                            // Preparation may have partially mutated provider
                                            // state. Fence this generation and let bounded
                                            // session cleanup decide whether the lease can move.
                                            let _ = send_source_unavailable(&outbound).await;
                                            break;
                                        }
                                        let (code, message, retryable) = source_error_response(&error);
                                        if !send_error(&outbound, code, message, request.expected_revision, retryable).await { break; }
                                        continue;
                                    }
                                };
                                let result = store.lock().await.apply_desired_state_with_cursor_and_limit(
                                    request.clone(),
                                    prepared_cursor.as_ref(),
                                    delivery_limit(limits),
                                    limits.max_persisted_sessions,
                                );
                                match result {
                                    Ok((applied, newly_committed)) => {
                                        if newly_committed {
                                            metrics.inner.desired_states_committed.fetch_add(1, Ordering::Relaxed);
                                            metrics.inner.deliveries_persisted.fetch_add(1, Ordering::Relaxed);
                                        }
                                        if source_call(source_operation_timeout, provider.activate_desired_state(
                                            preparation_id,
                                            &request,
                                            cursor.as_ref(),
                                        )).await.is_err() {
                                            metrics.inner.source_errors.fetch_add(1, Ordering::Relaxed);
                                            let _ = source_call(source_operation_timeout, provider.abort_desired_state(preparation_id, &request)).await;
                                            // The desired state is already durable. Closing the
                                            // stream makes this an explicitly uncertain result;
                                            // reconnect will restore and reactivate that revision.
                                            if !send_source_unavailable(&outbound).await { break; }
                                            break;
                                        }
                                        desired_state = Some(request.clone());
                                        let pending = match store.lock().await.load(session_id, *chain_id) {
                                            Ok(persisted) => persisted.pending_delivery,
                                            Err(_) => {
                                                let _ = send_internal(&outbound).await;
                                                break;
                                            }
                                        };
                                        has_pending = pending.is_some();
                                        pending_was_sent = !newly_committed && has_pending;
                                        delivery_requested = false;
                                        if !send_outbound(&outbound, ServerMessage {
                                            message: Some(server_message::Message::DesiredStateApplied(applied)),
                                        }, client_send_timeout).await { break; }
                                        // The activation remains in the durable outbox and
                                        // is delivered on the client's next DeliveryDemand.
                                    }
                                    Err(error) => {
                                        if source_call(source_operation_timeout, provider.abort_desired_state(preparation_id, &request)).await.is_err() {
                                            metrics.inner.source_errors.fetch_add(1, Ordering::Relaxed);
                                            // The durable CAS rejected the candidate, but failed
                                            // compensation leaves provider state indeterminate.
                                            // Never accept another operation on this generation.
                                            let _ = send_source_unavailable(&outbound).await;
                                            break;
                                        }
                                        let (code, committed, retryable) = store_error_code(&error);
                                        if code == ErrorCode::Internal {
                                            let _ = send_internal(&outbound).await;
                                            break;
                                        }
                                        if !send_error(&outbound, code, store_error_message(&error), committed, retryable).await { break; }
                                    }
                                }
                            }
                            Some(client_message::Message::Acknowledge(acknowledgement)) => {
                                let Some((session_id, chain_id)) = &identity else {
                                    if !send_error(&outbound, ErrorCode::ProtocolVersion, "Hello must be sent first", 0, false).await { break; }
                                    continue;
                                };
                                if &acknowledgement.session_id != session_id {
                                    if !send_error(&outbound, ErrorCode::InvalidInterest, "acknowledgement does not match negotiated session", 0, false).await { break; }
                                    continue;
                                }
                                if !position_confirmed {
                                    if !send_error(&outbound, ErrorCode::RevisionConflict, "runtime checkpoint proof is required on a new session", desired_state.as_ref().map_or(0, |state| state.new_revision), false).await { break; }
                                    continue;
                                }
                                let acknowledgement_result = {
                                    let mut authority = store.lock().await;
                                    match authority.acknowledge_with_status(*chain_id, &acknowledgement) {
                                        Ok((committed_cursor, newly_committed)) => authority
                                            .load(session_id, *chain_id)
                                            .map(|persisted| {
                                                (
                                                    committed_cursor,
                                                    newly_committed,
                                                    persisted.expected_reorg_tip,
                                                )
                                            }),
                                        Err(error) => Err(error),
                                    }
                                };
                                match acknowledgement_result {
                                    Ok((committed_cursor, newly_committed, committed_reorg_anchor)) => {
                                        required_reorg_anchor = committed_reorg_anchor;
                                        if newly_committed {
                                            metrics
                                                .inner
                                                .acknowledgements_committed
                                                .fetch_add(1, Ordering::Relaxed);
                                        }
                                        if source_call(source_operation_timeout, provider.acknowledge(
                                            *chain_id,
                                            &acknowledgement,
                                            &committed_cursor,
                                        )).await.is_err()
                                        {
                                            metrics.inner.source_errors.fetch_add(1, Ordering::Relaxed);
                                            if !send_source_unavailable(&outbound).await { break; }
                                            break;
                                        }
                                        if !send_outbound(&outbound, ServerMessage {
                                            message: Some(server_message::Message::AcknowledgementCommitted(
                                                AcknowledgementCommitted {
                                                    session_id: session_id.clone(),
                                                    sequence: acknowledgement.sequence,
                                                    cursor: Some(committed_cursor.clone()),
                                                },
                                            )),
                                        }, client_send_timeout).await {
                                            break;
                                        }
                                        cursor = Some(committed_cursor);
                                        has_pending = false;
                                        pending_was_sent = false;
                                        delivery_requested = false;
                                        interval.reset_immediately();
                                    }
                                    Err(_) => {
                                        let _ = send_internal(&outbound).await;
                                        break;
                                    }
                                }
                            }
                            Some(client_message::Message::Heartbeat(heartbeat)) => {
                                if identity.is_none() {
                                    if !send_error(&outbound, ErrorCode::ProtocolVersion, "Hello must be sent first", 0, false).await { break; }
                                    continue;
                                }
                                if !send_outbound(&outbound, ServerMessage {
                                    message: Some(server_message::Message::Heartbeat(heartbeat)),
                                }, client_send_timeout).await { break; }
                            }
                            Some(client_message::Message::DeliveryDemand(demand)) => {
                                let Some((session_id, chain_id)) = &identity else {
                                    if !send_error(&outbound, ErrorCode::ProtocolVersion, "Hello must be sent first", 0, false).await { break; }
                                    continue;
                                };
                                if &demand.session_id != session_id {
                                    if !send_error(&outbound, ErrorCode::InvalidInterest, "delivery demand does not match negotiated session", 0, false).await { break; }
                                    continue;
                                }
                                if !position_confirmed {
                                    if !send_error(&outbound, ErrorCode::RevisionConflict, "runtime checkpoint proof is required on a new session", desired_state.as_ref().map_or(0, |state| state.new_revision), false).await { break; }
                                    continue;
                                }
                                if has_pending {
                                    let pending = store.lock().await.load(session_id, *chain_id)
                                        .ok()
                                        .and_then(|persisted| persisted.pending_delivery);
                                    match pending {
                                        Some(delivery) if encoded_delivery_len(&delivery) <= MAX_MESSAGE_SIZE_BYTES => {
                                            if pending_was_sent {
                                                metrics.inner.deliveries_replayed.fetch_add(1, Ordering::Relaxed);
                                            }
                                            if !send_outbound(&outbound, ServerMessage {
                                                message: Some(server_message::Message::Delivery(delivery)),
                                            }, client_send_timeout).await { break; }
                                            pending_was_sent = true;
                                        }
                                        Some(_) => {
                                            if !send_error(&outbound, ErrorCode::ResourceExhausted, "durable pending delivery exceeds the hard transport limit", desired_state.as_ref().map_or(0, |state| state.new_revision), false).await { break; }
                                        }
                                        None => {
                                            has_pending = false;
                                            pending_was_sent = false;
                                            delivery_requested = true;
                                            interval.reset_immediately();
                                        }
                                    }
                                } else {
                                    delivery_requested = true;
                                    interval.reset_immediately();
                                }
                            }
                            None => {
                                if !send_error(&outbound, ErrorCode::InvalidInterest, "empty client message", 0, false).await { break; }
                            }
                        }
                    }
                    _ = interval.tick(), if identity.is_some() && position_confirmed && desired_state.is_some() && delivery_requested && !has_pending => {
                        let Some(desired) = desired_state.as_ref() else {
                            continue;
                        };
                        let request = DeliveryRequest::new(desired, cursor.as_ref())
                            .with_required_reorg_anchor(required_reorg_anchor.as_ref());
                        match source_call(source_operation_timeout, provider.next_delivery(request)).await {
                            Ok(Some(delivery)) => {
                                if encoded_delivery_len(&delivery) > delivery_limit(limits) {
                                    delivery_requested = false;
                                    if !send_error(&outbound, ErrorCode::ResourceExhausted, "source delivery exceeds the configured encoded-byte limit", desired.new_revision, false).await { break; }
                                    continue;
                                }
                                let save_result = {
                                    store.lock().await.save_pending_with_status(desired, &delivery)
                                };
                                match save_result {
                                    Ok(newly_persisted) => {
                                        if newly_persisted {
                                            metrics.inner.deliveries_persisted.fetch_add(1, Ordering::Relaxed);
                                        }
                                        has_pending = true;
                                        pending_was_sent = true;
                                        delivery_requested = false;
                                        if !send_outbound(&outbound, ServerMessage {
                                            message: Some(server_message::Message::Delivery(delivery)),
                                        }, client_send_timeout).await { break; }
                                    }
                                    Err(_) => {
                                        let _ = send_internal(&outbound).await;
                                        break;
                                    }
                                }
                            }
                            Ok(None) => {}
                            Err(error) => {
                                metrics.inner.source_errors.fetch_add(1, Ordering::Relaxed);
                                delivery_requested = false;
                                let (code, message, retryable) = source_error_response(&error);
                                if !send_error(&outbound, code, message, desired.new_revision, retryable).await { break; }
                            }
                        }
                    }
                    update = wait_for_provider_update(
                        provider.as_ref(),
                        update_desired_state.as_ref(),
                        update_cursor.as_ref(),
                        update_reorg_anchor.as_ref(),
                    ), if identity.is_some() && position_confirmed && delivery_requested && !has_pending => {
                        match update {
                            Ok(()) => {
                                let Some(desired) = desired_state.as_ref() else {
                                    continue;
                                };
                                let request = DeliveryRequest::new(desired, cursor.as_ref())
                                    .with_required_reorg_anchor(required_reorg_anchor.as_ref());
                                match source_call(source_operation_timeout, provider.next_delivery(request)).await {
                                    Ok(Some(delivery)) => {
                                        if encoded_delivery_len(&delivery) > delivery_limit(limits) {
                                            delivery_requested = false;
                                            if !send_error(&outbound, ErrorCode::ResourceExhausted, "source delivery exceeds the configured encoded-byte limit", desired.new_revision, false).await { break; }
                                            continue;
                                        }
                                        let save_result = {
                                            store.lock().await.save_pending_with_status(desired, &delivery)
                                        };
                                        match save_result {
                                            Ok(newly_persisted) => {
                                                if newly_persisted {
                                                    metrics.inner.deliveries_persisted.fetch_add(1, Ordering::Relaxed);
                                                }
                                                has_pending = true;
                                                pending_was_sent = true;
                                                delivery_requested = false;
                                                if !send_outbound(&outbound, ServerMessage {
                                                    message: Some(server_message::Message::Delivery(delivery)),
                                                }, client_send_timeout).await { break; }
                                            }
                                            Err(_) => {
                                                let _ = send_internal(&outbound).await;
                                                break;
                                            }
                                        }
                                    }
                                    Ok(None) => {}
                                    Err(error) => {
                                        metrics.inner.source_errors.fetch_add(1, Ordering::Relaxed);
                                        delivery_requested = false;
                                        let (code, message, retryable) = source_error_response(&error);
                                        if !send_error(&outbound, code, message, desired.new_revision, retryable).await { break; }
                                    }
                                }
                            }
                            Err(error) => {
                                metrics.inner.source_errors.fetch_add(1, Ordering::Relaxed);
                                delivery_requested = false;
                                let (code, message, retryable) = source_error_response(&error);
                                if !send_error(&outbound, code, message, desired_state.as_ref().map_or(0, |state| state.new_revision), retryable).await { break; }
                            }
                        }
                    }
                }
            }
            if let Some(lease) = session_lease.as_mut() {
                let release = tokio::time::timeout(
                    source_operation_timeout,
                    provider.release_session(&lease.key.0, lease.key.1),
                )
                .await;
                if !matches!(release, Ok(Ok(()))) {
                    if matches!(release, Ok(Err(_))) {
                        metrics.inner.source_errors.fetch_add(1, Ordering::Relaxed);
                    }
                    // Failed or canceled cleanup may have partially mutated
                    // provider state. Preserve the lease so a replacement
                    // generation cannot race it.
                    lease.retain_on_drop();
                }
            }
            drop(session_lease);
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

struct SessionLease {
    registry: Arc<StdMutex<HashSet<(String, u64)>>>,
    key: (String, u64),
    metrics: EventServiceMetrics,
    retain_registry_entry: bool,
}

impl SessionLease {
    fn acquire(
        registry: Arc<StdMutex<HashSet<(String, u64)>>>,
        key: (String, u64),
        max_active_sessions: usize,
        metrics: EventServiceMetrics,
    ) -> Result<Self, LeaseError> {
        let mut sessions = registry.lock().map_err(|_| LeaseError::Poisoned)?;
        if sessions.contains(&key) {
            return Err(LeaseError::InUse);
        }
        if sessions.len() >= max_active_sessions {
            return Err(LeaseError::Capacity);
        }
        sessions.insert(key.clone());
        drop(sessions);
        metrics
            .inner
            .active_sessions
            .fetch_add(1, Ordering::Relaxed);
        Ok(Self {
            registry,
            key,
            metrics,
            retain_registry_entry: false,
        })
    }

    fn retain_on_drop(&mut self) {
        self.retain_registry_entry = true;
    }
}

impl Drop for SessionLease {
    fn drop(&mut self) {
        if self.retain_registry_entry {
            return;
        }
        if let Ok(mut sessions) = self.registry.lock()
            && sessions.remove(&self.key)
        {
            self.metrics
                .inner
                .active_sessions
                .fetch_sub(1, Ordering::Relaxed);
        }
    }
}

enum LeaseError {
    InUse,
    Capacity,
    Poisoned,
}

fn allocate_preparation_id(next: &AtomicU64) -> Option<PreparationId> {
    next.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        value.checked_add(1)
    })
    .ok()
    .map(PreparationId::new)
}

async fn source_call<T>(
    timeout: Duration,
    operation: impl std::future::Future<Output = Result<T, EventSourceError>>,
) -> Result<T, EventSourceError> {
    tokio::time::timeout(timeout, operation)
        .await
        .map_err(|_| EventSourceError::unavailable("source operation timed out"))?
}

async fn wait_for_service_shutdown(mut shutdown: watch::Receiver<bool>) {
    if *shutdown.borrow_and_update() {
        return;
    }
    loop {
        if shutdown.changed().await.is_err() {
            return;
        }
        if *shutdown.borrow_and_update() {
            return;
        }
    }
}

async fn wait_for_provider_update<P>(
    provider: &P,
    desired_state: Option<&ApplyDesiredState>,
    acknowledged_cursor: Option<&Cursor>,
    required_reorg_anchor: Option<&BlockRef>,
) -> Result<(), EventSourceError>
where
    P: EventSource,
{
    let Some(desired_state) = desired_state else {
        return std::future::pending().await;
    };
    provider
        .wait_for_update(
            DeliveryRequest::new(desired_state, acknowledged_cursor)
                .with_required_reorg_anchor(required_reorg_anchor),
        )
        .await
}

async fn send_internal(sender: &mpsc::Sender<Result<ServerMessage, Status>>) -> bool {
    sender
        .try_send(Err(Status::unavailable("internal event service failure")))
        .is_ok()
}

async fn send_source_unavailable(sender: &mpsc::Sender<Result<ServerMessage, Status>>) -> bool {
    sender
        .try_send(Err(Status::unavailable("event source unavailable")))
        .is_ok()
}

async fn send_outbound(
    sender: &mpsc::Sender<Result<ServerMessage, Status>>,
    message: ServerMessage,
    timeout: Duration,
) -> bool {
    matches!(
        tokio::time::timeout(timeout, sender.send(Ok(message))).await,
        Ok(Ok(()))
    )
}

async fn send_error(
    sender: &mpsc::Sender<Result<ServerMessage, Status>>,
    code: ErrorCode,
    message: &str,
    committed_revision: u64,
    retryable: bool,
) -> bool {
    sender
        .try_send(Ok(ServerMessage {
            message: Some(server_message::Message::Error(ProtocolError {
                code: code.into(),
                message: message.to_owned(),
                committed_revision,
                retryable,
            })),
        }))
        .is_ok()
}

fn store_error_code(error: &SessionStoreError) -> (ErrorCode, u64, bool) {
    match error {
        SessionStoreError::RevisionConflict { committed, .. } => {
            (ErrorCode::RevisionConflict, *committed, false)
        }
        SessionStoreError::DesiredState(DesiredStateError::ProtocolVersion { .. }) => {
            (ErrorCode::ProtocolVersion, 0, false)
        }
        SessionStoreError::DesiredState(DesiredStateError::UnsupportedInterest { .. }) => {
            (ErrorCode::UnsupportedInterest, 0, false)
        }
        SessionStoreError::DesiredState(DesiredStateError::InvalidState(_)) => {
            (ErrorCode::InvalidInterest, 0, false)
        }
        SessionStoreError::PendingDelivery => (ErrorCode::PendingDelivery, 0, true),
        SessionStoreError::PersistedSessionLimit { .. } => (ErrorCode::ResourceExhausted, 0, false),
        SessionStoreError::SequenceOverflow => (ErrorCode::ResourceExhausted, 0, false),
        SessionStoreError::IntegerRange(_) => (ErrorCode::InvalidInterest, 0, false),
        SessionStoreError::DeliveryTooLarge { .. } => (ErrorCode::ResourceExhausted, 0, false),
        _ => (ErrorCode::Internal, 0, true),
    }
}

fn store_error_message(error: &SessionStoreError) -> &'static str {
    match error {
        SessionStoreError::RevisionConflict { .. } => "desired-state revision conflict",
        SessionStoreError::DesiredState(DesiredStateError::ProtocolVersion { .. }) => {
            "unsupported protocol version"
        }
        SessionStoreError::DesiredState(DesiredStateError::UnsupportedInterest { .. }) => {
            "desired state contains an unsupported interest"
        }
        SessionStoreError::DesiredState(DesiredStateError::InvalidState(_)) => {
            "desired state is invalid"
        }
        SessionStoreError::PendingDelivery => {
            "pending delivery must be acknowledged before desired state changes"
        }
        SessionStoreError::PersistedSessionLimit { .. } => {
            "persisted session capacity is exhausted"
        }
        SessionStoreError::SequenceOverflow => "delivery sequence space is exhausted",
        SessionStoreError::IntegerRange(_) => "value exceeds the durable storage range",
        SessionStoreError::DeliveryTooLarge { .. } => {
            "activation delivery exceeds the configured encoded-byte limit"
        }
        _ => "internal event service failure",
    }
}

fn desired_state_error(error: &DesiredStateError) -> (ErrorCode, &'static str) {
    match error {
        DesiredStateError::ProtocolVersion { .. } => {
            (ErrorCode::ProtocolVersion, "unsupported protocol version")
        }
        DesiredStateError::UnsupportedInterest { .. } => (
            ErrorCode::UnsupportedInterest,
            "desired state contains an unsupported interest",
        ),
        DesiredStateError::InvalidState(_) => {
            (ErrorCode::InvalidInterest, "desired state is invalid")
        }
        DesiredStateError::RevisionConflict { .. } => (
            ErrorCode::RevisionConflict,
            "desired-state revision conflict",
        ),
    }
}

fn source_error_response(error: &EventSourceError) -> (ErrorCode, &'static str, bool) {
    match error.kind {
        EventSourceErrorKind::InvalidRequest => (
            ErrorCode::InvalidInterest,
            "event source rejected the request",
            false,
        ),
        EventSourceErrorKind::Unsupported => (
            ErrorCode::UnsupportedInterest,
            "event source does not support the request",
            false,
        ),
        EventSourceErrorKind::ResourceExhausted => (
            ErrorCode::ResourceExhausted,
            "event source resource limit exceeded",
            true,
        ),
        EventSourceErrorKind::Unavailable => (
            ErrorCode::SourceUnavailable,
            "event source is temporarily unavailable",
            true,
        ),
        EventSourceErrorKind::Internal => {
            (ErrorCode::Internal, "internal event source failure", true)
        }
    }
}

fn encoded_delivery_len(delivery: &Delivery) -> usize {
    let delivery_bytes = delivery.encoded_len();
    delivery_bytes
        .saturating_add(prost::length_delimiter_len(delivery_bytes))
        .saturating_add(1)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HelloPosition {
    Confirmed,
    DiscoveryOnly,
}

fn validate_hello_position(
    hello: &Hello,
    persisted: &PersistedSession,
) -> Result<HelloPosition, &'static str> {
    let transport_sequence = persisted
        .acknowledged_cursor
        .as_ref()
        .map_or(0, |cursor| cursor.batch_sequence);
    let runtime_sequence = persisted
        .runtime_checkpoint_cursor
        .as_ref()
        .map_or(0, |cursor| cursor.batch_sequence);
    let Some(resume) = hello.pending_delivery_resume.as_ref() else {
        return if hello.acknowledged_sequence == runtime_sequence {
            Ok(HelloPosition::Confirmed)
        } else if hello.acknowledged_sequence == 0 {
            // A proof-free sequence-zero connection may inspect authority so a
            // synchronous runtime restore hook can discover the exact cursor.
            // The stream remains server-gated until a new proof-bearing Hello.
            Ok(HelloPosition::DiscoveryOnly)
        } else {
            Err("client position does not equal runtime checkpoint authority")
        };
    };

    if hello.acknowledged_sequence == runtime_sequence {
        let cursor = persisted
            .runtime_checkpoint_cursor
            .as_ref()
            .ok_or("zero sequence cannot carry a pending-delivery proof")?;
        resume_matches_cursor(
            resume,
            hello.acknowledged_sequence.to_be_bytes().as_slice(),
            cursor,
        )?;
        return Ok(HelloPosition::Confirmed);
    }

    let expected_pending = transport_sequence
        .checked_add(1)
        .ok_or("durable delivery sequence is exhausted")?;
    let pending = persisted
        .pending_delivery
        .as_ref()
        .ok_or("service has no pending delivery matching the restored position")?;
    let cursor = pending
        .cursor
        .as_ref()
        .ok_or("pending delivery cursor is missing")?;
    if hello.acknowledged_sequence != expected_pending
        || pending.sequence != expected_pending
        || pending.delivery_token != resume.delivery_token
    {
        return Err("pending delivery sequence or token does not match");
    }
    resume_matches_cursor(resume, &pending.delivery_token, cursor)?;
    Ok(HelloPosition::Confirmed)
}

fn resume_matches_cursor(
    resume: &PendingDeliveryResume,
    expected_token: &[u8],
    cursor: &Cursor,
) -> Result<(), &'static str> {
    let checkpoint_matches = if cursor.provider_checkpoint.is_empty() {
        resume.provider_checkpoint.is_none()
    } else {
        resume.provider_checkpoint.as_deref() == Some(cursor.provider_checkpoint.as_slice())
    };
    if resume.delivery_token != expected_token
        || !checkpoint_matches
        || resume.coverage_head.as_ref() != cursor.canonical_head.as_ref()
    {
        return Err("pending-delivery proof does not match its cursor");
    }
    Ok(())
}

const fn delivery_limit(limits: EventServiceLimits) -> usize {
    if limits.max_delivery_bytes < MAX_MESSAGE_SIZE_BYTES {
        limits.max_delivery_bytes
    } else {
        MAX_MESSAGE_SIZE_BYTES
    }
}

fn validate_limits(
    request: &ApplyDesiredState,
    limits: EventServiceLimits,
) -> Result<(), &'static str> {
    if request.session_id.len() > limits.max_identifier_bytes {
        return Err("session identifier exceeds the configured byte limit");
    }
    if request.owners.len() > limits.max_owners {
        return Err("owner count exceeds the configured limit");
    }
    if request.encoded_len() > limits.max_desired_state_bytes {
        return Err("desired state exceeds the configured encoded-byte limit");
    }
    let mut total_interests = 0usize;
    let mut total_filter_values = 0usize;
    for owner in &request.owners {
        if owner.owner_id.len() > limits.max_identifier_bytes {
            return Err("owner identifier exceeds the configured byte limit");
        }
        if owner.interests.len() > limits.max_interests_per_owner {
            return Err("owner interest count exceeds the configured limit");
        }
        total_interests = total_interests.saturating_add(owner.interests.len());
        if total_interests > limits.max_total_interests {
            return Err("total interest count exceeds the configured limit");
        }
        if owner.backfill.as_ref().is_some_and(|backfill| {
            backfill.to_block_excl.is_some_and(|end| {
                end.saturating_sub(backfill.from_block) > limits.max_bounded_backfill_blocks
            })
        }) {
            return Err("bounded backfill exceeds the configured block limit");
        }
        for interest in &owner.interests {
            if let Some(portable_interest::Kind::Log(log)) = &interest.kind {
                total_filter_values = total_filter_values.saturating_add(log.addresses.len());
                total_filter_values = total_filter_values.saturating_add(
                    log.topics
                        .iter()
                        .map(|topic| topic.values.len())
                        .fold(0usize, usize::saturating_add),
                );
                if total_filter_values > limits.max_total_filter_values {
                    return Err("total log filter value count exceeds the configured limit");
                }
                if log.addresses.len() > limits.max_addresses_per_log_interest {
                    return Err("log address count exceeds the configured limit");
                }
                if log
                    .topics
                    .iter()
                    .any(|topic| topic.values.len() > limits.max_values_per_topic)
                {
                    return Err("log topic value count exceeds the configured limit");
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use super::allocate_preparation_id;

    #[test]
    fn preparation_identifiers_never_wrap_or_repeat() {
        let next = AtomicU64::new(u64::MAX - 1);
        assert!(allocate_preparation_id(&next).is_some());
        assert!(allocate_preparation_id(&next).is_none());
        assert!(allocate_preparation_id(&next).is_none());
    }
}
