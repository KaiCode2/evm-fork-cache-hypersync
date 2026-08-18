//! Live, ignored acceptance coverage for the complete Hybrid restart contract.
//!
//! Cargo does not load `.env` files. Load credentials in the calling shell and
//! run this test serially:
//!
//! ```text
//! cargo test -p evm-fork-cache-hypersync --test live_hybrid \
//!   live_hybrid_restart_reconciles_durable_child_before_repoll -- \
//!   --ignored --exact --nocapture --test-threads=1
//! ```

use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use alloy_consensus::BlockHeader;
use alloy_eips::BlockId;
use alloy_primitives::{B256, address};
use alloy_provider::{Provider, ProviderBuilder, WsConnect, network::AnyNetwork};
use alloy_rpc_types_eth::Filter;
use evm_fork_cache::{
    DurableCheckpointIdentity, DurableCheckpointStore, EvmCache, ReactiveRuntime,
    events::StateView,
    reactive::{
        AlloySubscriber, BlockRef, CheckpointedIngest, EventSubscriber, HandlerError, HandlerId,
        HandlerOutcome, InputSource, InterestOwnerSubscriber, LogInterest,
        ReactiveCanonicalBaseline, ReactiveConfig, ReactiveContext, ReactiveHandler, ReactiveInput,
        ReactiveInterest, StateEffectQuality, SubscriberBackfill, SubscriberCapabilities,
        SubscriberConfig, SubscriberDeliveryToken, SubscriberError, SubscriberMode,
        SubscriberNextBatch, SubscriberOperation, SubscriberResumePosition,
    },
};
use evm_fork_cache_hypersync::{
    ChainDataSource, EventService, HyperSyncDataSource, HyperSyncSourceFactory,
    ManagedEventProvider, PersistedSession, SessionStore,
};
use evm_fork_cache_remote::{
    GrpcEventTransport, HybridConfig, HybridPhase, HybridSubscriber, RemoteSubscriber,
};
use tokio::sync::{Mutex, oneshot};
use tokio_stream::wrappers::TcpListenerStream;

const SESSION_ID: &str = "live-hybrid-restart-acceptance";
const HANDLER_ID: &str = "live-hybrid-log-observer";
const HANDLER_SET_ID: &str = "live-hybrid-log-observer-v1";
const DEFAULT_BACKFILL_BLOCKS: u64 = 12;
const SETUP_TIMEOUT: Duration = Duration::from_secs(30);
const REGISTRATION_TIMEOUT: Duration = Duration::from_secs(120);
const FIRST_DURABLE_LOG_TIMEOUT: Duration = Duration::from_secs(120);
const RESTORE_RECONCILIATION_TIMEOUT: Duration = Duration::from_secs(120);
const LIVE_CUTOVER_TIMEOUT: Duration = Duration::from_secs(240);
const RPC_CANONICAL_WAIT_TIMEOUT: Duration = Duration::from_secs(45);
const RPC_CANONICAL_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const RPC_CANONICAL_RETRY_DELAY: Duration = Duration::from_millis(250);
const RETRYABLE_SOURCE_DELAY: Duration = Duration::from_millis(250);
const STABLE_CANONICAL_CONFLICT_ATTEMPTS: usize = 3;

type SharedStore = Arc<Mutex<SessionStore>>;

struct RunningService {
    endpoint: String,
    shutdown: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

impl RunningService {
    async fn stop(self) {
        let _ = self.shutdown.send(());
        tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .expect("event service shutdown timeout")
            .expect("join event service");
    }
}

async fn spawn_live_service(store: SharedStore, token: String) -> RunningService {
    let provider = Arc::new(ManagedEventProvider::new(
        HyperSyncSourceFactory::new(token),
        128,
    ));
    let service =
        EventService::new(store, provider, Duration::from_millis(10)).expect("poll interval");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind event service");
    let address = listener.local_addr().expect("event service address");
    let (shutdown, shutdown_receiver) = oneshot::channel();
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service.into_server())
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_receiver.await;
            })
            .await
            .expect("serve event service");
    });
    RunningService {
        endpoint: format!("http://{address}"),
        shutdown,
        task,
    }
}

fn chain_id() -> u64 {
    std::env::var("HYPERSYNC_TEST_CHAIN_ID")
        .map_or(Ok(1_u64), |value| value.parse())
        .expect("HYPERSYNC_TEST_CHAIN_ID must be a u64")
}

fn backfill_blocks() -> u64 {
    std::env::var("HYBRID_LIVE_BACKFILL_BLOCKS")
        .map_or(Ok(DEFAULT_BACKFILL_BLOCKS), |value| value.parse())
        .expect("HYBRID_LIVE_BACKFILL_BLOCKS must be a u64")
        .max(2)
}

fn verified_live_subscriber_config() -> SubscriberConfig {
    SubscriberConfig {
        verify_log_block_context: true,
        ..SubscriberConfig::default()
    }
}

fn live_log_filter(chain_id: u64) -> Filter {
    if chain_id == 1 {
        // WETH emits frequently enough to exercise live cutover without making
        // the historical acceptance query an unbounded match-all mainnet scan.
        Filter::new().address(address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"))
    } else {
        Filter::new()
    }
}

fn http_url() -> String {
    let url = std::env::var("RPC_URL").expect("RPC_URL");
    if let Some(rest) = url.strip_prefix("wss://") {
        format!("https://{rest}")
    } else if let Some(rest) = url.strip_prefix("ws://") {
        format!("http://{rest}")
    } else {
        url
    }
}

fn websocket_url() -> String {
    let url = std::env::var("WS_RPC_URL")
        .or_else(|_| std::env::var("RPC_URL"))
        .expect("WS_RPC_URL or RPC_URL");
    if let Some(rest) = url.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = url.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        url
    }
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn is_retryable_source_unavailable(error: &impl std::fmt::Display) -> bool {
    let rendered = error.to_string();
    rendered.contains("SourceUnavailable") && rendered.contains("temporarily unavailable")
}

async fn wait_for_rpc_canonical_hash<P>(provider: &P, number: u64, expected: B256, label: &str)
where
    P: Provider<AnyNetwork>,
{
    tokio::time::timeout(RPC_CANONICAL_WAIT_TIMEOUT, async {
        let mut repeated_conflict = None::<(B256, usize)>;
        loop {
            match tokio::time::timeout(
                RPC_CANONICAL_REQUEST_TIMEOUT,
                provider.get_block_by_number(number.into()),
            )
            .await
            {
                Ok(Ok(Some(block))) if block.header.hash == expected => return,
                Ok(Ok(Some(block))) => {
                    let actual = block.header.hash;
                    let attempts = repeated_conflict
                        .filter(|(hash, _)| *hash == actual)
                        .map_or(1, |(_, attempts)| attempts.saturating_add(1));
                    repeated_conflict = Some((actual, attempts));
                    assert!(
                        attempts < STABLE_CANONICAL_CONFLICT_ATTEMPTS,
                        "{label} has a stable canonical conflict at block {number}: \
                         expected {expected:#x}, RPC returned {actual:#x} \
                         {attempts} consecutive times"
                    );
                }
                Ok(Ok(None) | Err(_)) | Err(_) => {
                    repeated_conflict = None;
                }
            }
            tokio::time::sleep(RPC_CANONICAL_RETRY_DELAY).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "{label} was not available at the expected canonical hash within \
             {RPC_CANONICAL_WAIT_TIMEOUT:?}: block={number} expected={expected:#x}"
        )
    });
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct LogIdentity {
    block_hash: B256,
    transaction_hash: B256,
    log_index: u64,
}

#[derive(Clone, Copy, Debug)]
struct ObservedLog {
    identity: LogIdentity,
    block_number: u64,
    source: InputSource,
}

#[derive(Default)]
struct ObservationState {
    identities: HashSet<LogIdentity>,
    logs: Vec<ObservedLog>,
    duplicates: usize,
}

struct LogObserver {
    filter: Filter,
    calls: Arc<AtomicUsize>,
    observations: Arc<StdMutex<ObservationState>>,
}

impl ReactiveHandler for LogObserver {
    fn id(&self) -> HandlerId {
        HandlerId::new(HANDLER_ID)
    }

    fn interests(&self) -> Vec<ReactiveInterest> {
        vec![ReactiveInterest::Logs(LogInterest {
            provider_filter: self.filter.clone(),
            local_matcher: None,
            route_key: None,
        })]
    }

    fn handle(
        &self,
        context: &ReactiveContext,
        input: &ReactiveInput,
        _state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        let ReactiveInput::Log(log) = input else {
            panic!("match-all log observer received a non-log input");
        };
        let identity = LogIdentity {
            block_hash: log.block_hash.expect("live log block hash"),
            transaction_hash: log.transaction_hash.expect("live log transaction hash"),
            log_index: log.log_index.expect("live log index"),
        };
        let block_number = context
            .block
            .as_ref()
            .map(|block| block.number)
            .or(log.block_number)
            .expect("live log block number");
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut observations = self.observations.lock().expect("observation lock");
        if !observations.identities.insert(identity) {
            observations.duplicates += 1;
        }
        observations.logs.push(ObservedLog {
            identity,
            block_number,
            source: context.source,
        });
        Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect))
    }
}

fn install_log_observer(
    runtime: &mut ReactiveRuntime,
    calls: Arc<AtomicUsize>,
    observations: Arc<StdMutex<ObservationState>>,
) -> (HandlerId, Vec<ReactiveInterest>) {
    let observer = Arc::new(LogObserver {
        filter: live_log_filter(chain_id()),
        calls,
        observations,
    });
    let id = observer.id();
    let interests = observer.interests();
    runtime
        .register_handler(observer)
        .expect("register live Hybrid observer");
    (id, interests)
}

/// Injects exactly one lost outer acknowledgement for a historical log batch.
///
/// The core has already atomically persisted its checkpoint when this hook is
/// called. Returning before delegating leaves the real event-service outbox
/// pending, reproducing the crash window without modifying production code.
struct FailOnceOuterAck<S> {
    inner: S,
    checkpoint_store: DurableCheckpointStore,
    armed_token: Option<Vec<u8>>,
    failed: bool,
    failure_observed: Arc<AtomicBool>,
}

impl<S> FailOnceOuterAck<S> {
    fn new(inner: S, checkpoint_store: DurableCheckpointStore) -> (Self, Arc<AtomicBool>) {
        let failure_observed = Arc::new(AtomicBool::new(false));
        (
            Self {
                inner,
                checkpoint_store,
                armed_token: None,
                failed: false,
                failure_observed: Arc::clone(&failure_observed),
            },
            failure_observed,
        )
    }

    fn without_failure(inner: S, checkpoint_store: DurableCheckpointStore) -> Self {
        Self {
            inner,
            checkpoint_store,
            armed_token: None,
            failed: true,
            failure_observed: Arc::new(AtomicBool::new(false)),
        }
    }

    const fn inner(&self) -> &S {
        &self.inner
    }

    fn inner_mut(&mut self) -> &mut S {
        &mut self.inner
    }
}

impl<S> EventSubscriber for FailOnceOuterAck<S>
where
    S: EventSubscriber,
{
    fn chain_id(&self) -> Option<u64> {
        self.inner.chain_id()
    }

    fn capabilities(&self) -> SubscriberCapabilities {
        self.inner.capabilities()
    }

    fn register_interests(
        &mut self,
        interests: &[ReactiveInterest],
    ) -> SubscriberOperation<'_, ()> {
        self.inner.register_interests(interests)
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, alloy_network::Ethereum> {
        Box::pin(async move {
            let batch = self.inner.next_batch().await?;
            if !self.failed
                && self.armed_token.is_none()
                && let Some(candidate) = batch.as_ref()
                && candidate
                    .records()
                    .iter()
                    .any(|record| record.context.source == InputSource::Backfill)
            {
                let token = candidate.delivery_token().ok_or_else(|| {
                    SubscriberError::Provider(
                        "Hybrid historical log delivery was unexpectedly tokenless".into(),
                    )
                })?;
                self.armed_token = Some(token.as_bytes().to_vec());
            }
            Ok(batch)
        })
    }

    fn restore_position(
        &mut self,
        position: &SubscriberResumePosition,
    ) -> Result<(), SubscriberError> {
        self.inner.restore_position(position)
    }

    fn acknowledge_delivery(
        &mut self,
        token: SubscriberDeliveryToken,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            if !self.failed
                && self
                    .armed_token
                    .as_deref()
                    .is_some_and(|armed| armed == token.as_bytes())
            {
                let saved = self
                    .checkpoint_store
                    .load()
                    .expect("load checkpoint before injected outer ACK failure")
                    .expect("checkpoint exists before injected outer ACK failure");
                assert!(
                    saved.metadata().delivery_witness.is_some(),
                    "outer ACK was attempted before the delivery witness was durable"
                );
                assert_eq!(
                    saved.metadata().delivery_token.as_deref(),
                    Some(token.as_bytes()),
                    "outer ACK token must match the just-persisted checkpoint"
                );
                self.failed = true;
                self.failure_observed.store(true, Ordering::SeqCst);
                return Err(SubscriberError::Provider(
                    "injected lost outer Hybrid acknowledgement".into(),
                ));
            }
            self.inner.acknowledge_delivery(token).await
        })
    }
}

impl<S> InterestOwnerSubscriber for FailOnceOuterAck<S>
where
    S: InterestOwnerSubscriber,
{
    fn upsert_interest_owners(
        &mut self,
        owners: Vec<(HandlerId, Vec<ReactiveInterest>)>,
    ) -> SubscriberOperation<'_, ()> {
        self.inner.upsert_interest_owners(owners)
    }

    fn replace_interest_owners(
        &mut self,
        owners: Vec<(HandlerId, Vec<ReactiveInterest>)>,
    ) -> SubscriberOperation<'_, ()> {
        self.inner.replace_interest_owners(owners)
    }

    fn replace_interest_owners_with_global_backfill(
        &mut self,
        owners: Vec<(HandlerId, Vec<ReactiveInterest>)>,
        backfill: SubscriberBackfill,
    ) -> SubscriberOperation<'_, ()> {
        self.inner
            .replace_interest_owners_with_global_backfill(owners, backfill)
    }

    fn add_interest_owner(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest],
    ) -> SubscriberOperation<'_, ()> {
        self.inner.add_interest_owner(owner, interests)
    }

    fn add_interest_owner_with_backfill(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest],
        backfill: SubscriberBackfill,
    ) -> SubscriberOperation<'_, ()> {
        self.inner
            .add_interest_owner_with_backfill(owner, interests, backfill)
    }

    fn add_interest_owner_with_canonical_catchup(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest],
        retained: BlockRef,
    ) -> SubscriberOperation<'_, ()> {
        self.inner
            .add_interest_owner_with_canonical_catchup(owner, interests, retained)
    }

    fn remove_interest_owner(
        &mut self,
        owner: &HandlerId,
    ) -> SubscriberOperation<'_, Option<Vec<ReactiveInterest>>> {
        self.inner.remove_interest_owner(owner)
    }

    fn owner_interests(&self, owner: &HandlerId) -> Option<&[ReactiveInterest]> {
        self.inner.owner_interests(owner)
    }
}

#[derive(Clone, Copy)]
enum PollProbeRole {
    Historical,
    Live,
}

impl PollProbeRole {
    const fn label(self) -> &'static str {
        match self {
            Self::Historical => "historical",
            Self::Live => "live",
        }
    }
}

/// Shared proof boundary consulted by both restored children before their
/// first poll. Hybrid's production ordering must durably ACK the historical
/// child first, without invoking the rebuilt handler.
struct RestorePollBoundary {
    store: SharedStore,
    chain_id: u64,
    restored_handler_calls: Arc<AtomicUsize>,
    initial_source_poll_verified: Mutex<bool>,
    historical_acknowledgements: AtomicUsize,
    historical_polls: AtomicUsize,
    live_polls: AtomicUsize,
    boundary_checks: AtomicUsize,
}

impl RestorePollBoundary {
    async fn verify_first_poll(&self, role: PollProbeRole) -> Result<(), SubscriberError> {
        // Serialize the one strict pre-poll check so cancellation cannot leave
        // a claimed-but-unverified boundary behind. The other child's first
        // poll may legitimately happen after later deliveries have been
        // handled, so only the first source poll requires the exact initial
        // ACK count and zero handler calls.
        let mut initial_source_poll_verified = self.initial_source_poll_verified.lock().await;
        let is_initial_source_poll = !*initial_source_poll_verified;
        let persisted = self
            .store
            .lock()
            .await
            .load(SESSION_ID, self.chain_id)
            .map_err(|error| SubscriberError::Provider(error.to_string()))?;
        if persisted.pending_delivery.is_some() {
            return Err(SubscriberError::Provider(format!(
                "Hybrid polled the restored {} child before clearing the historical outbox",
                role.label()
            )));
        }
        let historical_acknowledgements = self.historical_acknowledgements.load(Ordering::SeqCst);
        if is_initial_source_poll {
            if self.restored_handler_calls.load(Ordering::SeqCst) != 0 {
                return Err(SubscriberError::Provider(format!(
                    "Hybrid invoked the restored handler before its first source poll ({})",
                    role.label()
                )));
            }
            if historical_acknowledgements != 1 {
                return Err(SubscriberError::Provider(format!(
                    "Hybrid did not commit exactly one restored historical child ACK before its first source poll ({} child)",
                    role.label()
                )));
            }
            *initial_source_poll_verified = true;
        } else if historical_acknowledgements == 0 {
            return Err(SubscriberError::Provider(format!(
                "Hybrid did not retain proof of the restored historical child ACK before the first {} child poll",
                role.label()
            )));
        }
        match role {
            PollProbeRole::Historical => {
                self.historical_polls.fetch_add(1, Ordering::SeqCst);
            }
            PollProbeRole::Live => {
                self.live_polls.fetch_add(1, Ordering::SeqCst);
            }
        }
        self.boundary_checks.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Transparent first-poll gate installed around each restored child.
struct FirstPollGate<S> {
    inner: S,
    role: PollProbeRole,
    boundary: Arc<RestorePollBoundary>,
    first_poll_pending: bool,
}

impl<S> FirstPollGate<S> {
    fn new(inner: S, role: PollProbeRole, boundary: Arc<RestorePollBoundary>) -> Self {
        Self {
            inner,
            role,
            boundary,
            first_poll_pending: true,
        }
    }
}

impl<S> EventSubscriber for FirstPollGate<S>
where
    S: EventSubscriber,
{
    fn chain_id(&self) -> Option<u64> {
        self.inner.chain_id()
    }

    fn capabilities(&self) -> SubscriberCapabilities {
        self.inner.capabilities()
    }

    fn register_interests(
        &mut self,
        interests: &[ReactiveInterest],
    ) -> SubscriberOperation<'_, ()> {
        self.inner.register_interests(interests)
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, alloy_network::Ethereum> {
        Box::pin(async move {
            if self.first_poll_pending {
                self.boundary.verify_first_poll(self.role).await?;
                self.first_poll_pending = false;
            }
            self.inner.next_batch().await
        })
    }

    fn restore_position(
        &mut self,
        position: &SubscriberResumePosition,
    ) -> Result<(), SubscriberError> {
        self.inner.restore_position(position)
    }

    fn acknowledge_delivery(
        &mut self,
        token: SubscriberDeliveryToken,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.inner.acknowledge_delivery(token).await?;
            if matches!(self.role, PollProbeRole::Historical) {
                self.boundary
                    .historical_acknowledgements
                    .fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        })
    }
}

impl<S> InterestOwnerSubscriber for FirstPollGate<S>
where
    S: InterestOwnerSubscriber,
{
    fn upsert_interest_owners(
        &mut self,
        owners: Vec<(HandlerId, Vec<ReactiveInterest>)>,
    ) -> SubscriberOperation<'_, ()> {
        self.inner.upsert_interest_owners(owners)
    }

    fn replace_interest_owners(
        &mut self,
        owners: Vec<(HandlerId, Vec<ReactiveInterest>)>,
    ) -> SubscriberOperation<'_, ()> {
        self.inner.replace_interest_owners(owners)
    }

    fn replace_interest_owners_with_global_backfill(
        &mut self,
        owners: Vec<(HandlerId, Vec<ReactiveInterest>)>,
        backfill: SubscriberBackfill,
    ) -> SubscriberOperation<'_, ()> {
        self.inner
            .replace_interest_owners_with_global_backfill(owners, backfill)
    }

    fn add_interest_owner(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest],
    ) -> SubscriberOperation<'_, ()> {
        self.inner.add_interest_owner(owner, interests)
    }

    fn add_interest_owner_with_backfill(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest],
        backfill: SubscriberBackfill,
    ) -> SubscriberOperation<'_, ()> {
        self.inner
            .add_interest_owner_with_backfill(owner, interests, backfill)
    }

    fn add_interest_owner_with_canonical_catchup(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest],
        retained: BlockRef,
    ) -> SubscriberOperation<'_, ()> {
        self.inner
            .add_interest_owner_with_canonical_catchup(owner, interests, retained)
    }

    fn remove_interest_owner(
        &mut self,
        owner: &HandlerId,
    ) -> SubscriberOperation<'_, Option<Vec<ReactiveInterest>>> {
        self.inner.remove_interest_owner(owner)
    }

    fn owner_interests(&self, owner: &HandlerId) -> Option<&[ReactiveInterest]> {
        self.inner.owner_interests(owner)
    }
}

async fn assert_service_pending(store: &SharedStore, chain_id: u64, expected: bool) {
    let persisted = load_session(store, chain_id).await;
    assert_eq!(
        persisted.pending_delivery.is_some(),
        expected,
        "unexpected durable event-service outbox state"
    );
}

async fn load_session(store: &SharedStore, chain_id: u64) -> PersistedSession {
    store
        .lock()
        .await
        .load(SESSION_ID, chain_id)
        .expect("load live Hybrid session")
}

fn assert_exact_persisted_session(actual: &PersistedSession, expected: &PersistedSession) {
    assert_eq!(
        actual.desired_state, expected.desired_state,
        "desired state/revision changed across SQLite reopen"
    );
    assert_eq!(
        actual.acknowledged_cursor, expected.acknowledged_cursor,
        "acknowledged cursor changed across SQLite reopen"
    );
    assert_eq!(
        actual.runtime_checkpoint_cursor, expected.runtime_checkpoint_cursor,
        "runtime checkpoint cursor changed across SQLite reopen"
    );
    assert_eq!(
        actual.pending_delivery, expected.pending_delivery,
        "pending delivery changed across SQLite reopen"
    );
    assert_eq!(
        actual.expected_reorg_tip, expected.expected_reorg_tip,
        "expected reorg tip changed across SQLite reopen"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires ENVIO_API_TOKEN, RPC_URL, WebSocket access, and live network access"]
async fn live_hybrid_restart_reconciles_durable_child_before_repoll() {
    let total_started = Instant::now();
    let token = std::env::var("ENVIO_API_TOKEN").expect("ENVIO_API_TOKEN");
    let chain_id = chain_id();

    let identity_started = Instant::now();
    let source = HyperSyncDataSource::new(chain_id, &token).expect("HyperSync client");
    let archive_height = tokio::time::timeout(SETUP_TIMEOUT, source.height())
        .await
        .expect("HyperSync height timeout")
        .expect("HyperSync height");
    let rpc = Arc::new(
        ProviderBuilder::new()
            .network::<AnyNetwork>()
            .connect_http(http_url().parse().expect("RPC_URL format")),
    );
    let rpc_chain_id = tokio::time::timeout(SETUP_TIMEOUT, rpc.get_chain_id())
        .await
        .expect("RPC chain-id timeout")
        .expect("RPC chain id");
    assert_eq!(
        rpc_chain_id, chain_id,
        "RPC_URL and HYPERSYNC_TEST_CHAIN_ID must target the same chain"
    );
    let rpc_head = tokio::time::timeout(SETUP_TIMEOUT, rpc.get_block_number())
        .await
        .expect("RPC head timeout")
        .expect("RPC head");
    let archive_tip = archive_height.checked_sub(1).expect(
        "HyperSync returned exclusive height zero; the live Hybrid test requires an actively \
         producing chain with archived blocks",
    );
    let common_head = rpc_head.min(archive_tip);
    let requested_backfill_blocks = backfill_blocks();
    let baseline_number = common_head.checked_sub(requested_backfill_blocks).unwrap_or_else(|| {
        panic!(
            "the live Hybrid test requires at least {requested_backfill_blocks} archived blocks \
             before common head {common_head}"
        )
    });
    let baseline_block = tokio::time::timeout(
        SETUP_TIMEOUT,
        rpc.get_block_by_number(baseline_number.into()),
    )
    .await
    .expect("baseline block timeout")
    .expect("baseline block request")
    .expect("baseline block");
    let baseline = BlockRef {
        number: baseline_block.header.number(),
        hash: baseline_block.header.hash,
        parent_hash: Some(baseline_block.header.parent_hash()),
        timestamp: Some(baseline_block.header.timestamp()),
    };
    let identity_elapsed = identity_started.elapsed();

    let workspace = tempfile::tempdir().expect("temporary live Hybrid workspace");
    let database_path = workspace.path().join("sessions.sqlite");
    let checkpoint_path = workspace.path().join("runtime.checkpoint");
    let checkpoint_store = DurableCheckpointStore::new(&checkpoint_path);
    let checkpoint_identity = DurableCheckpointIdentity::new(chain_id, SESSION_ID, HANDLER_SET_ID);

    let first_service_started = Instant::now();
    let first_store = Arc::new(Mutex::new(
        SessionStore::open(&database_path).expect("first session store"),
    ));
    let first_service = spawn_live_service(Arc::clone(&first_store), token.clone()).await;
    let first_service_elapsed = first_service_started.elapsed();

    let first_ws_started = Instant::now();
    let first_ws_provider = tokio::time::timeout(
        SETUP_TIMEOUT,
        ProviderBuilder::new().connect_ws(WsConnect::new(websocket_url())),
    )
    .await
    .expect("first WebSocket connect timeout")
    .expect("first WebSocket provider");
    let first_ws_chain_id = tokio::time::timeout(SETUP_TIMEOUT, first_ws_provider.get_chain_id())
        .await
        .expect("first WebSocket chain-id timeout")
        .expect("first WebSocket chain id");
    assert_eq!(first_ws_chain_id, chain_id, "WebSocket chain mismatch");
    let first_ws_elapsed = first_ws_started.elapsed();

    let first_remote_started = Instant::now();
    let first_historical = tokio::time::timeout(
        SETUP_TIMEOUT,
        RemoteSubscriber::connect(&first_service.endpoint, SESSION_ID, chain_id),
    )
    .await
    .expect("first historical gRPC connect timeout")
    .expect("connect first historical subscriber");
    let first_remote_elapsed = first_remote_started.elapsed();
    let first_live = AlloySubscriber::new(
        first_ws_provider,
        SubscriberMode::PubSub,
        verified_live_subscriber_config(),
    )
    .with_log_verification_provider(
        ProviderBuilder::new().connect_http(http_url().parse().expect("RPC_URL format")),
    );
    let first_hybrid = HybridSubscriber::new(first_historical, first_live, HybridConfig::default())
        .expect("construct first Hybrid");
    let (subscriber, failure_observed) =
        FailOnceOuterAck::new(first_hybrid, checkpoint_store.clone());

    let observations = Arc::new(StdMutex::new(ObservationState::default()));
    let first_calls = Arc::new(AtomicUsize::new(0));
    let mut first_runtime = ReactiveRuntime::new(ReactiveConfig::default());
    install_log_observer(
        &mut first_runtime,
        Arc::clone(&first_calls),
        Arc::clone(&observations),
    );
    let mut first_engine = evm_fork_cache::reactive::ReactiveEngine::new(first_runtime, subscriber);

    let first_cache_started = Instant::now();
    let exact_baseline = BlockId::from((baseline.hash, Some(true)));
    let mut first_cache = tokio::time::timeout(
        SETUP_TIMEOUT,
        EvmCache::at_block(Arc::clone(&rpc), exact_baseline),
    )
    .await
    .expect("first exact-baseline cache setup timeout");
    first_engine
        .adopt_canonical_baseline(
            &first_cache,
            ReactiveCanonicalBaseline::new(chain_id, baseline),
        )
        .expect("adopt exact RPC baseline");
    let first_cache_elapsed = first_cache_started.elapsed();

    let registration_started = Instant::now();
    tokio::time::timeout(
        REGISTRATION_TIMEOUT,
        first_engine.sync_handler_interests_with_backfill(),
    )
    .await
    .expect("Hybrid lifecycle registration timeout")
    .expect("install Hybrid lifecycle with global backfill");
    let registration_elapsed = registration_started.elapsed();

    let failed_commit_started = Instant::now();
    let acknowledgement_error = tokio::time::timeout(FIRST_DURABLE_LOG_TIMEOUT, async {
        loop {
            match first_engine
                .next_ingest_checkpointed(&mut first_cache, &checkpoint_store, &checkpoint_identity)
                .await
            {
                Ok(Some(_)) => {}
                Ok(None) => tokio::task::yield_now().await,
                Err(error)
                    if error
                        .to_string()
                        .contains("injected lost outer Hybrid acknowledgement") =>
                {
                    break error;
                }
                Err(error) if is_retryable_source_unavailable(&error) => {
                    tokio::time::sleep(RETRYABLE_SOURCE_DELAY).await;
                }
                Err(error) => panic!("unexpected first-process Hybrid failure: {error}"),
            }
        }
    })
    .await
    .expect("historical log/checkpoint/failed-ACK phase timeout");
    assert!(
        acknowledgement_error
            .to_string()
            .contains("injected lost outer Hybrid acknowledgement")
    );
    let failed_commit_elapsed = failed_commit_started.elapsed();
    assert!(failure_observed.load(Ordering::SeqCst));
    assert!(
        checkpoint_path.is_file(),
        "lost acknowledgement must leave a durable checkpoint"
    );
    assert!(
        first_calls.load(Ordering::SeqCst) > 0,
        "the failed outer ACK must follow a real historical log delivery"
    );
    let pre_crash_session = load_session(&first_store, chain_id).await;
    assert!(
        pre_crash_session.pending_delivery.is_some(),
        "the injected lost outer ACK must leave the exact historical child delivery pending"
    );
    assert!(
        pre_crash_session.desired_state.is_some(),
        "the pre-crash event-service registration must be durable"
    );

    let loaded = checkpoint_store
        .load()
        .expect("load durable Hybrid checkpoint")
        .expect("durable Hybrid checkpoint exists");
    let metadata = loaded.metadata().clone();
    assert_eq!(metadata.identity, checkpoint_identity);
    assert!(
        metadata
            .delivery_token
            .as_ref()
            .is_some_and(|token| !token.is_empty())
    );
    assert!(metadata.delivery_witness.is_some());
    assert!(
        metadata
            .runtime_checkpoint
            .as_ref()
            .is_some_and(|checkpoint| !checkpoint.is_empty())
    );
    assert!(
        metadata
            .subscriber_checkpoint
            .as_ref()
            .is_some_and(|checkpoint| !checkpoint.is_empty())
    );
    let checkpoint_rpc_block = tokio::time::timeout(
        SETUP_TIMEOUT,
        rpc.get_block_by_number(metadata.block.number.into()),
    )
    .await
    .expect("checkpoint canonical lookup timeout")
    .expect("checkpoint RPC block request")
    .expect("checkpoint RPC block");
    assert_eq!(
        checkpoint_rpc_block.header.hash, metadata.block.hash,
        "durable checkpoint must remain canonical before restart"
    );
    let checkpoint_block = BlockRef {
        number: metadata.block.number,
        hash: metadata.block.hash,
        parent_hash: metadata.block.parent_hash,
        timestamp: metadata.block.timestamp,
    };
    assert!(
        checkpoint_block.number > baseline.number,
        "the durable checkpoint must advance beyond the distinct cold baseline"
    );
    let pre_crash_calls = first_calls.load(Ordering::SeqCst);
    drop(loaded);

    let first_shutdown_started = Instant::now();
    drop(first_engine);
    drop(first_cache);
    first_service.stop().await;
    drop(first_store);
    drop(rpc);
    drop(checkpoint_store);
    let first_shutdown_elapsed = first_shutdown_started.elapsed();

    let reopen_started = Instant::now();
    let restarted_checkpoint_store = DurableCheckpointStore::new(&checkpoint_path);
    let restarted_loaded = restarted_checkpoint_store
        .load()
        .expect("reload durable Hybrid checkpoint from a new store")
        .expect("durable Hybrid checkpoint survives process teardown");
    let restarted_metadata = restarted_loaded.metadata().clone();
    assert_eq!(
        restarted_metadata, metadata,
        "checkpoint bytes changed across durable-store reconstruction"
    );
    let restarted_store = Arc::new(Mutex::new(
        SessionStore::open(&database_path).expect("reopened session store"),
    ));
    let sqlite_reopen_elapsed = reopen_started.elapsed();
    let reopened_session = load_session(&restarted_store, chain_id).await;
    assert_exact_persisted_session(&reopened_session, &pre_crash_session);
    assert!(
        reopened_session.pending_delivery.is_some(),
        "the exact pending outbox delivery must survive SQLite reopen"
    );

    let restarted_service_started = Instant::now();
    let restarted_service = spawn_live_service(Arc::clone(&restarted_store), token).await;
    let restarted_service_elapsed = restarted_service_started.elapsed();

    let restarted_rpc_started = Instant::now();
    let restarted_rpc = Arc::new(
        ProviderBuilder::new()
            .network::<AnyNetwork>()
            .connect_http(http_url().parse().expect("RPC_URL format")),
    );
    let restarted_rpc_chain_id = tokio::time::timeout(SETUP_TIMEOUT, restarted_rpc.get_chain_id())
        .await
        .expect("restarted RPC chain-id timeout")
        .expect("restarted RPC chain id");
    assert_eq!(restarted_rpc_chain_id, chain_id, "restarted RPC mismatch");
    let restored_canonical = tokio::time::timeout(
        SETUP_TIMEOUT,
        restarted_rpc.get_block_by_number(checkpoint_block.number.into()),
    )
    .await
    .expect("restored checkpoint canonical lookup timeout")
    .expect("restored canonical request")
    .expect("restored canonical block");
    assert_eq!(
        restored_canonical.header.hash, checkpoint_block.hash,
        "checkpoint canonical hash changed before restore"
    );
    let restarted_rpc_elapsed = restarted_rpc_started.elapsed();

    let restarted_ws_started = Instant::now();
    let restarted_ws_provider = tokio::time::timeout(
        SETUP_TIMEOUT,
        ProviderBuilder::new().connect_ws(WsConnect::new(websocket_url())),
    )
    .await
    .expect("restarted WebSocket connect timeout")
    .expect("restarted WebSocket provider");
    assert_eq!(
        tokio::time::timeout(SETUP_TIMEOUT, restarted_ws_provider.get_chain_id())
            .await
            .expect("restarted WebSocket chain-id timeout")
            .expect("restarted WebSocket chain id"),
        chain_id,
        "restarted WebSocket mismatch"
    );
    let restarted_ws_elapsed = restarted_ws_started.elapsed();

    let restarted_remote_started = Instant::now();
    let restarted_remote = tokio::time::timeout(
        SETUP_TIMEOUT,
        RemoteSubscriber::<GrpcEventTransport>::connect(
            &restarted_service.endpoint,
            SESSION_ID,
            chain_id,
        ),
    )
    .await
    .expect("restored historical gRPC connect timeout")
    .expect("connect restored historical subscriber");
    let restarted_remote_elapsed = restarted_remote_started.elapsed();
    let restored_calls = Arc::new(AtomicUsize::new(0));
    let restore_poll_boundary = Arc::new(RestorePollBoundary {
        store: Arc::clone(&restarted_store),
        chain_id,
        restored_handler_calls: Arc::clone(&restored_calls),
        initial_source_poll_verified: Mutex::new(false),
        historical_acknowledgements: AtomicUsize::new(0),
        historical_polls: AtomicUsize::new(0),
        live_polls: AtomicUsize::new(0),
        boundary_checks: AtomicUsize::new(0),
    });
    let restarted_historical = FirstPollGate::new(
        restarted_remote,
        PollProbeRole::Historical,
        Arc::clone(&restore_poll_boundary),
    );
    let restarted_live = FirstPollGate::new(
        AlloySubscriber::new(
            restarted_ws_provider,
            SubscriberMode::PubSub,
            verified_live_subscriber_config(),
        )
        .with_log_verification_provider(
            ProviderBuilder::new().connect_http(http_url().parse().expect("RPC_URL format")),
        ),
        PollProbeRole::Live,
        Arc::clone(&restore_poll_boundary),
    );
    let restarted_hybrid = HybridSubscriber::new(
        restarted_historical,
        restarted_live,
        HybridConfig::default(),
    )
    .expect("construct restored Hybrid");
    let restarted_subscriber =
        FailOnceOuterAck::without_failure(restarted_hybrid, restarted_checkpoint_store.clone());
    let mut restarted_runtime = ReactiveRuntime::new(ReactiveConfig::default());
    let (owner, owner_interests) = install_log_observer(
        &mut restarted_runtime,
        Arc::clone(&restored_calls),
        Arc::clone(&observations),
    );
    let mut restarted_engine =
        evm_fork_cache::reactive::ReactiveEngine::new(restarted_runtime, restarted_subscriber);

    let restored_cache_started = Instant::now();
    let mut restored_cache = tokio::time::timeout(
        SETUP_TIMEOUT,
        EvmCache::at_block(
            Arc::clone(&restarted_rpc),
            BlockId::from((baseline.hash, Some(true))),
        ),
    )
    .await
    .expect("restored cache setup timeout");
    assert_eq!(
        restored_cache.block(),
        BlockId::from((baseline.hash, Some(true))),
        "the rebuilt cache must begin at the original distinct baseline"
    );
    assert_eq!(
        restored_cache.block_number(),
        Some(baseline.number),
        "the rebuilt cache baseline must have an exact block number"
    );
    assert_eq!(
        restored_cache.timestamp(),
        baseline.timestamp,
        "the rebuilt cache baseline must have the exact RPC timestamp"
    );
    assert_eq!(
        restored_cache.chain_id(),
        chain_id,
        "the rebuilt cache baseline must retain chain identity"
    );
    let restored_cache_elapsed = restored_cache_started.elapsed();

    let preview_started = Instant::now();
    let position = restarted_engine
        .preview_durable_resume_position(&restarted_metadata)
        .expect("preview exact Hybrid restore position");
    assert_eq!(position.chain_id, chain_id);
    assert_eq!(position.coverage_head, checkpoint_block);
    assert_eq!(
        position.canonical_history.last(),
        Some(&checkpoint_block),
        "the decoded durable runtime history must end at the checkpoint"
    );
    assert!(
        position
            .canonical_history
            .iter()
            .all(|block| block.number <= checkpoint_block.number),
        "durable runtime history must not contain a block beyond its coverage head"
    );
    let preview_elapsed = preview_started.elapsed();

    let prepare_started = Instant::now();
    tokio::time::timeout(
        REGISTRATION_TIMEOUT,
        restarted_engine
            .subscriber_mut()
            .inner_mut()
            .prepare_restore_lifecycle(
                &position,
                &[],
                vec![(owner.clone(), owner_interests.clone())],
            ),
    )
    .await
    .expect("Hybrid async restore preparation timeout")
    .expect("prepare ephemeral WebSocket lifecycle");
    let prepare_elapsed = prepare_started.elapsed();
    assert_service_pending(&restarted_store, chain_id, true).await;
    assert_eq!(
        restore_poll_boundary
            .historical_acknowledgements
            .load(Ordering::SeqCst),
        0
    );
    assert_eq!(
        restore_poll_boundary
            .historical_polls
            .load(Ordering::SeqCst),
        0
    );
    assert_eq!(restore_poll_boundary.live_polls.load(Ordering::SeqCst), 0);
    assert_eq!(
        restore_poll_boundary.boundary_checks.load(Ordering::SeqCst),
        0
    );
    assert_eq!(restored_calls.load(Ordering::SeqCst), 0);

    let atomic_restore_started = Instant::now();
    let restored_metadata = restarted_engine
        .restore_durable_checkpoint(&mut restored_cache, restarted_loaded, &checkpoint_identity)
        .expect("atomically restore cache, runtime, and Hybrid");
    let atomic_restore_elapsed = atomic_restore_started.elapsed();
    assert_eq!(restored_metadata, restarted_metadata);
    assert_eq!(
        restored_cache.block(),
        BlockId::from((checkpoint_block.hash, Some(true))),
        "atomic restore must replace the distinct cache baseline with the exact checkpoint hash"
    );
    assert_eq!(
        restored_cache.block_number(),
        Some(checkpoint_block.number),
        "atomic restore must install the checkpoint block number"
    );
    assert_eq!(
        restored_cache.timestamp(),
        checkpoint_block.timestamp,
        "atomic restore must install the checkpoint timestamp"
    );
    assert_eq!(restored_cache.chain_id(), chain_id);
    assert_eq!(
        restarted_engine.runtime().last_canonical_block(),
        Some(checkpoint_block),
        "atomic restore must install the runtime canonical head"
    );
    assert_eq!(
        position.canonical_history.last(),
        restarted_engine.runtime().last_canonical_block().as_ref(),
        "the restored runtime head must equal the terminal durable history entry"
    );
    assert_eq!(
        restarted_engine.subscriber().inner().phase(),
        HybridPhase::Recovering,
        "Hybrid must remain in recovery until the durable child ACK is reconciled"
    );
    assert_service_pending(&restarted_store, chain_id, true).await;
    assert_eq!(
        restore_poll_boundary
            .historical_acknowledgements
            .load(Ordering::SeqCst),
        0
    );
    assert_eq!(
        restore_poll_boundary
            .historical_polls
            .load(Ordering::SeqCst),
        0
    );
    assert_eq!(restore_poll_boundary.live_polls.load(Ordering::SeqCst), 0);
    assert_eq!(
        restore_poll_boundary.boundary_checks.load(Ordering::SeqCst),
        0
    );
    assert_eq!(restored_calls.load(Ordering::SeqCst), 0);

    let reconciliation_started = Instant::now();
    let first_restored_outcome = tokio::time::timeout(RESTORE_RECONCILIATION_TIMEOUT, async {
        loop {
            match restarted_engine
                .next_ingest_checkpointed(
                    &mut restored_cache,
                    &restarted_checkpoint_store,
                    &checkpoint_identity,
                )
                .await
            {
                Ok(Some(outcome)) => break outcome,
                Ok(None) => tokio::task::yield_now().await,
                Err(error) if is_retryable_source_unavailable(&error) => {
                    tokio::time::sleep(RETRYABLE_SOURCE_DELAY).await;
                }
                Err(error) => panic!("restored Hybrid ingest: {error}"),
            }
        }
    })
    .await
    .expect("restored ACK reconciliation/later delivery timeout");
    let reconciliation_elapsed = reconciliation_started.elapsed();
    assert!(
        matches!(first_restored_outcome, CheckpointedIngest::Applied(_)),
        "Hybrid restore must ACK the child internally and expose only a later delivery"
    );
    assert!(
        restore_poll_boundary
            .historical_acknowledgements
            .load(Ordering::SeqCst)
            >= 1
    );
    assert!(restore_poll_boundary.boundary_checks.load(Ordering::SeqCst) >= 1);
    assert!(
        restore_poll_boundary
            .historical_polls
            .load(Ordering::SeqCst)
            >= 1
    );
    assert_service_pending(&restarted_store, chain_id, false).await;

    let cutover_started = Instant::now();
    let live_observation = tokio::time::timeout(LIVE_CUTOVER_TIMEOUT, async {
        loop {
            let phase_before = restarted_engine.subscriber().inner().phase();
            assert_ne!(phase_before, HybridPhase::Poisoned);
            assert!(
                restarted_engine
                    .subscriber()
                    .inner()
                    .poison_reason()
                    .is_none()
            );
            let observation_count_before =
                observations.lock().expect("observation lock").logs.len();
            match restarted_engine
                .next_ingest_checkpointed(
                    &mut restored_cache,
                    &restarted_checkpoint_store,
                    &checkpoint_identity,
                )
                .await
            {
                Ok(Some(_)) => {}
                Ok(None) => {
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(error) if is_retryable_source_unavailable(&error) => {
                    tokio::time::sleep(RETRYABLE_SOURCE_DELAY).await;
                    continue;
                }
                Err(error) => panic!("post-restore Hybrid ingest: {error}"),
            }
            let phase_after = restarted_engine.subscriber().inner().phase();
            assert_ne!(phase_after, HybridPhase::Poisoned);
            if phase_before != HybridPhase::Live {
                continue;
            }
            let observations = observations.lock().expect("observation lock");
            if let Some(observation) = observations.logs[observation_count_before..]
                .iter()
                .copied()
                .find(|observation| {
                    observation.source == InputSource::Subscription
                        && observation.block_number > checkpoint_block.number
                })
            {
                break observation;
            }
        }
    })
    .await
    .expect("Hybrid live cutover and later WebSocket log timeout");
    let cutover_elapsed = cutover_started.elapsed();

    assert_eq!(
        first_calls.load(Ordering::SeqCst),
        pre_crash_calls,
        "dropped first engine must not receive post-restart callbacks"
    );
    assert!(
        restored_calls.load(Ordering::SeqCst) > 0,
        "restored handler must eventually receive later delivery"
    );
    let unique_logs = {
        let observations = observations.lock().expect("observation lock");
        assert_eq!(
            observations.duplicates, 0,
            "duplicate log identities observed"
        );
        assert_eq!(observations.identities.len(), observations.logs.len());
        assert!(observations.identities.contains(&live_observation.identity));
        observations.identities.len()
    };
    assert_eq!(
        restarted_engine.subscriber().inner().phase(),
        HybridPhase::Live
    );
    assert!(
        restarted_engine
            .subscriber()
            .inner()
            .poison_reason()
            .is_none()
    );
    assert_eq!(
        restore_poll_boundary
            .historical_polls
            .load(Ordering::SeqCst),
        1,
        "historical first-poll proof must execute exactly once"
    );
    assert_eq!(
        restore_poll_boundary.live_polls.load(Ordering::SeqCst),
        1,
        "live first-poll proof must execute exactly once"
    );
    assert_eq!(
        restore_poll_boundary.boundary_checks.load(Ordering::SeqCst),
        2,
        "both child sources must cross the restored ACK-before-poll boundary"
    );
    let final_canonical = restarted_engine
        .runtime()
        .last_canonical_block()
        .expect("restored runtime canonical head");
    assert!(
        final_canonical.number > checkpoint_block.number,
        "restored runtime must advance beyond its checkpoint"
    );
    wait_for_rpc_canonical_hash(
        restarted_rpc.as_ref(),
        live_observation.block_number,
        live_observation.identity.block_hash,
        "accepted live WebSocket log",
    )
    .await;
    wait_for_rpc_canonical_hash(
        restarted_rpc.as_ref(),
        final_canonical.number,
        final_canonical.hash,
        "restored runtime head",
    )
    .await;

    println!(
        "hybrid_summary chain_id={chain_id} rpc_head={rpc_head} archive_height_exclusive={archive_height} archive_tip={archive_tip} common_head={common_head} requested_backfill_blocks={requested_backfill_blocks} baseline_range={}..={} baseline_hash={:#x} checkpoint_block={} checkpoint_hash={:#x} final_block={} final_hash={:#x} live_log_block={} live_log_block_hash={:#x} live_log_tx_hash={:#x} live_log_index={} pre_crash_log_calls={} restored_log_calls={} unique_logs={} historical_first_polls={} live_first_polls={} identity_setup_ms={:.2} first_service_setup_ms={:.2} first_ws_connect_ms={:.2} first_grpc_connect_ms={:.2} first_cache_baseline_ms={:.2} lifecycle_registration_ms={:.2} checkpoint_and_failed_outer_ack_ms={:.2} first_process_shutdown_ms={:.2} sqlite_reopen_ms={:.2} restarted_service_ms={:.2} restarted_rpc_identity_ms={:.2} restarted_ws_connect_ms={:.2} restarted_grpc_connect_ms={:.2} restored_cache_setup_ms={:.2} resume_preview_ms={:.2} async_prepare_ms={:.2} atomic_restore_ms={:.2} ack_before_repoll_and_first_later_delivery_ms={:.2} live_cutover_ms={:.2} total_ms={:.2}",
        baseline_number
            .checked_add(1)
            .expect("baseline successor must exist"),
        common_head,
        baseline.hash,
        checkpoint_block.number,
        checkpoint_block.hash,
        final_canonical.number,
        final_canonical.hash,
        live_observation.block_number,
        live_observation.identity.block_hash,
        live_observation.identity.transaction_hash,
        live_observation.identity.log_index,
        pre_crash_calls,
        restored_calls.load(Ordering::SeqCst),
        unique_logs,
        restore_poll_boundary
            .historical_polls
            .load(Ordering::SeqCst),
        restore_poll_boundary.live_polls.load(Ordering::SeqCst),
        millis(identity_elapsed),
        millis(first_service_elapsed),
        millis(first_ws_elapsed),
        millis(first_remote_elapsed),
        millis(first_cache_elapsed),
        millis(registration_elapsed),
        millis(failed_commit_elapsed),
        millis(first_shutdown_elapsed),
        millis(sqlite_reopen_elapsed),
        millis(restarted_service_elapsed),
        millis(restarted_rpc_elapsed),
        millis(restarted_ws_elapsed),
        millis(restarted_remote_elapsed),
        millis(restored_cache_elapsed),
        millis(preview_elapsed),
        millis(prepare_elapsed),
        millis(atomic_restore_elapsed),
        millis(reconciliation_elapsed),
        millis(cutover_elapsed),
        millis(total_started.elapsed()),
    );

    drop(restarted_engine);
    restarted_service.stop().await;
}
