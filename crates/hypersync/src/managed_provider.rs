use std::{
    collections::{BTreeMap, HashMap, HashSet},
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use evm_fork_cache_event_protocol::v1::{
    Acknowledge, ApplyDesiredState, Capability, Cursor, Delivery, SourceCapabilities,
    SourceDescriptor, SourceRole, portable_interest,
};
use evm_fork_cache_event_service::{DeliveryRequest, EventSource, EventSourceError, PreparationId};
use futures::StreamExt;
use hypersync_client::net_types::{BlockField, Query};
use tokio::{
    sync::{Mutex, Notify},
    task::AbortHandle,
};

use crate::{
    ChainDataSource, ChainHeightStream, HyperSyncDataSource, MAX_DELIVERY_SIZE_BYTES,
    QueryPlanError, SourceEngine, SourceEngineError, SourceError, SourceResponseLimits,
    SourceResume,
    normalize::{DecodedSourceCheckpoint, block_ref, decode_source_checkpoint},
    page_validation::validate_source_page,
    source_engine::validate_response_counts,
};

const SOURCE_REQUEST_ATTEMPTS: u32 = 3;
const MAX_SOURCE_REQUEST_ATTEMPT: Duration = Duration::from_secs(8);

/// Factory for independently configured per-chain data sources.
pub trait ChainDataSourceFactory: Send + Sync + 'static {
    /// Source implementation produced for each managed session.
    type Source: ChainDataSource + 'static;

    /// Create a source for one EVM chain.
    ///
    /// # Errors
    ///
    /// Returns [`SourceError`] when the chain is unsupported or its
    /// provider/client configuration cannot be constructed.
    fn create(&self, chain_id: u64) -> Result<Self::Source, SourceError>;

    /// Whether this factory can reconstruct and hash-verify complete headers on a chain.
    fn supports_full_headers(&self, _chain_id: u64) -> bool {
        false
    }

    /// Whether any configured chain supports complete hash-verified headers.
    fn supports_full_headers_on_any_chain(&self) -> bool {
        false
    }
}

/// HyperSync source factory using one Envio API token across configured chains.
#[derive(Clone)]
pub struct HyperSyncSourceFactory {
    api_token: String,
    full_header_chains: HashSet<u64>,
}

impl HyperSyncSourceFactory {
    /// Store the API token used when constructing per-chain clients.
    pub fn new(api_token: impl Into<String>) -> Self {
        Self {
            api_token: api_token.into(),
            full_header_chains: HashSet::new(),
        }
    }

    /// Opt a chain into full headers after independently proving the provider schema is complete.
    ///
    /// The current HyperSync schema omits `requests_hash`, so this must not be
    /// enabled for post-Prague Ethereum blocks.
    pub fn with_hash_verified_headers_for_chain(mut self, chain_id: u64) -> Self {
        self.full_header_chains.insert(chain_id);
        self
    }
}

impl ChainDataSourceFactory for HyperSyncSourceFactory {
    type Source = HyperSyncDataSource;

    fn create(&self, chain_id: u64) -> Result<Self::Source, SourceError> {
        HyperSyncDataSource::new(chain_id, &self.api_token)
    }

    fn supports_full_headers(&self, chain_id: u64) -> bool {
        self.full_header_chains.contains(&chain_id)
    }

    fn supports_full_headers_on_any_chain(&self) -> bool {
        !self.full_header_chains.is_empty()
    }
}

struct ManagedEngine<S> {
    revision: u64,
    engine: Arc<Mutex<SourceEngine<S>>>,
    height_driver: Option<Arc<HeightDriver>>,
}

type SessionKey = (String, u64);
type EngineMap<S> = HashMap<SessionKey, ManagedEngine<S>>;
type PreparedEngineMap<S> = HashMap<(SessionKey, PreparationId), ManagedEngine<S>>;

struct ProviderState<S> {
    engines: EngineMap<S>,
    prepared: PreparedEngineMap<S>,
}

impl<S> Default for ProviderState<S> {
    fn default() -> Self {
        Self {
            engines: HashMap::new(),
            prepared: HashMap::new(),
        }
    }
}

struct HeightDriver {
    latest_hint: Arc<AtomicU64>,
    update: Arc<Notify>,
    abort: AbortHandle,
}

impl HeightDriver {
    fn spawn(mut updates: ChainHeightStream) -> Arc<Self> {
        let latest_hint = Arc::new(AtomicU64::new(0));
        let update = Arc::new(Notify::new());
        let task_latest = Arc::clone(&latest_hint);
        let task_update = Arc::clone(&update);
        let task = tokio::spawn(async move {
            while let Some(height) = updates.next().await {
                task_latest.store(height, Ordering::Release);
                task_update.notify_one();
            }
        });
        let abort = task.abort_handle();
        drop(task);
        Arc::new(Self {
            latest_hint,
            update,
            abort,
        })
    }
}

impl Drop for HeightDriver {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

impl<S> Clone for ManagedEngine<S> {
    fn clone(&self) -> Self {
        Self {
            revision: self.revision,
            engine: Arc::clone(&self.engine),
            height_driver: self.height_driver.as_ref().map(Arc::clone),
        }
    }
}

/// Maintains one in-memory source engine per durable session while SQLite
/// remains authoritative across process restarts.
pub struct ManagedEventProvider<F>
where
    F: ChainDataSourceFactory,
{
    factory: F,
    reorg_depth: usize,
    request_timeout: Duration,
    max_delivery_bytes: usize,
    response_limits: SourceResponseLimits,
    max_resident_sessions: usize,
    state: Mutex<ProviderState<F::Source>>,
}

impl<F> ManagedEventProvider<F>
where
    F: ChainDataSourceFactory,
{
    /// Create a provider retaining `reorg_depth` canonical blocks per session.
    ///
    /// # Panics
    ///
    /// Panics when `reorg_depth` is zero. Configuration-facing callers should
    /// prefer [`Self::try_new`] to report that invalid boundary without panic.
    pub fn new(factory: F, reorg_depth: usize) -> Self {
        Self::try_new(factory, reorg_depth)
            .expect("managed source reorg depth must be greater than zero")
    }

    /// Fallibly create a provider with a nonzero retained canonical history.
    ///
    /// # Errors
    ///
    /// Returns [`SourceError::InvalidReorgDepth`] when `reorg_depth` is zero.
    pub fn try_new(factory: F, reorg_depth: usize) -> Result<Self, SourceError> {
        if reorg_depth == 0 {
            return Err(SourceError::InvalidReorgDepth);
        }
        Ok(Self {
            factory,
            reorg_depth,
            request_timeout: Duration::from_secs(45),
            max_delivery_bytes: MAX_DELIVERY_SIZE_BYTES,
            response_limits: SourceResponseLimits::default(),
            max_resident_sessions: 4_096,
            state: Mutex::new(ProviderState::default()),
        })
    }

    /// Bound each provider height/query operation, including its internal retries.
    ///
    /// # Errors
    ///
    /// Returns [`SourceError::InvalidRequestTimeout`] when the timeout is zero.
    pub fn with_request_timeout(mut self, request_timeout: Duration) -> Result<Self, SourceError> {
        if request_timeout.is_zero() {
            return Err(SourceError::InvalidRequestTimeout);
        }
        self.request_timeout = request_timeout;
        Ok(self)
    }

    /// Bound the encoded source delivery before the service adds its outer envelope.
    ///
    /// # Errors
    ///
    /// Returns [`SourceError::InvalidDeliverySizeLimit`] when the limit is zero
    /// or exceeds [`MAX_DELIVERY_SIZE_BYTES`].
    pub fn with_max_delivery_bytes(
        mut self,
        max_delivery_bytes: usize,
    ) -> Result<Self, SourceError> {
        if max_delivery_bytes == 0 || max_delivery_bytes > MAX_DELIVERY_SIZE_BYTES {
            return Err(SourceError::InvalidDeliverySizeLimit {
                requested: max_delivery_bytes,
                maximum: MAX_DELIVERY_SIZE_BYTES,
            });
        }
        self.max_delivery_bytes = max_delivery_bytes;
        Ok(self)
    }

    /// Override hard local decoded-response limits independently of provider soft targets.
    pub fn with_response_limits(mut self, response_limits: SourceResponseLimits) -> Self {
        self.response_limits = response_limits;
        self
    }

    /// Bound the number of distinct session/chain engines retained in memory.
    ///
    /// # Errors
    ///
    /// Returns [`SourceError::InvalidSessionLimit`] when the limit is zero.
    pub fn with_max_resident_sessions(
        mut self,
        max_resident_sessions: usize,
    ) -> Result<Self, SourceError> {
        if max_resident_sessions == 0 {
            return Err(SourceError::InvalidSessionLimit);
        }
        self.max_resident_sessions = max_resident_sessions;
        Ok(self)
    }

    async fn build_engine(
        &self,
        desired_state: &ApplyDesiredState,
        acknowledged_cursor: Option<&Cursor>,
    ) -> Result<ManagedEngine<F::Source>, EventSourceError> {
        if requests_full_headers(desired_state)
            && !self.factory.supports_full_headers(desired_state.chain_id)
        {
            return Err(EventSourceError::unsupported(format!(
                "complete hash-verified block headers are unavailable for chain {}",
                desired_state.chain_id
            )));
        }
        let source = self
            .factory
            .create(desired_state.chain_id)
            .map_err(source_error)?;
        let backfill_start = desired_state
            .owners
            .iter()
            .filter_map(|owner| owner.backfill.as_ref().map(|backfill| backfill.from_block))
            .min();
        let global_backfill = desired_state
            .owners
            .iter()
            .find(|owner| owner.canonical)
            .and_then(|owner| owner.backfill.as_ref());
        let (start_block, computed_activation_block) = match acknowledged_cursor {
            Some(cursor) if cursor.query_revision < desired_state.new_revision => (
                backfill_start.map_or(cursor.next_block, |from| from.min(cursor.next_block)),
                cursor.next_block,
            ),
            Some(cursor) => (cursor.next_block, cursor.next_block),
            None => match backfill_start {
                Some(from_block) => {
                    let head = available_height(&source, self.request_timeout)
                        .await
                        .map_err(source_error)?;
                    (from_block, head)
                }
                None => {
                    let head = available_height(&source, self.request_timeout)
                        .await
                        .map_err(source_error)?;
                    (head, head)
                }
            },
        };
        let committed_sequence = acknowledged_cursor.map_or(0, |cursor| cursor.batch_sequence);
        let decoded_checkpoint = acknowledged_cursor
            .map(checkpoints_from_cursor)
            .transpose()?
            .unwrap_or_default();
        let checkpoint =
            if acknowledged_cursor.is_some_and(|cursor| cursor.next_block == start_block) {
                decoded_checkpoint.clone()
            } else {
                DecodedSourceCheckpoint::default()
            };
        verify_retained_backfill_baselines(
            &source,
            desired_state,
            &checkpoint.canonical_blocks,
            self.request_timeout,
            self.response_limits,
        )
        .await?;
        let global_baseline =
            global_backfill.and_then(|backfill| backfill.retained_baseline.clone());
        if let (Some(acknowledged), Some(baseline)) = (
            acknowledged_cursor.and_then(|cursor| cursor.canonical_head.as_ref()),
            global_baseline.as_ref(),
        ) && acknowledged != baseline
        {
            return Err(EventSourceError::invalid(
                "global backfill baseline does not match acknowledged canonical coverage",
            ));
        }
        let mut canonical_blocks = checkpoint.canonical_blocks;
        if canonical_blocks.is_empty()
            && let Some(baseline) = global_baseline.clone()
        {
            canonical_blocks.push(baseline);
        }
        let activation_block = acknowledged_cursor
            .filter(|cursor| cursor.query_revision == desired_state.new_revision)
            .and(decoded_checkpoint.activation_block)
            .unwrap_or(computed_activation_block);
        let owner_backfill_activation_block = acknowledged_cursor
            .filter(|cursor| cursor.query_revision == desired_state.new_revision)
            .map_or(Some(computed_activation_block), |cursor| {
                cursor.owner_backfill_activation_block
            });
        let height_driver = source.height_stream().map(HeightDriver::spawn);
        let engine = SourceEngine::restore(
            source,
            desired_state.clone(),
            SourceResume {
                next_block: start_block,
                sequence: committed_sequence,
                activation_block,
                owner_backfill_activation_block,
                canonical_blocks,
                provider_checkpoint: checkpoint.rollback_guard,
                coverage_head: acknowledged_cursor
                    .and_then(|cursor| cursor.canonical_head.clone())
                    .or(global_baseline),
            },
            self.reorg_depth,
        )
        .map_err(|error| EventSourceError::unavailable(error.to_string()))?
        .with_max_delivery_bytes(self.max_delivery_bytes)
        .map_err(source_engine_error)?
        .with_response_limits(self.response_limits);
        let managed = ManagedEngine {
            revision: desired_state.new_revision,
            engine: Arc::new(Mutex::new(engine)),
            height_driver,
        };
        Ok(managed)
    }

    async fn engine(
        &self,
        desired_state: &ApplyDesiredState,
        acknowledged_cursor: Option<&Cursor>,
    ) -> Result<ManagedEngine<F::Source>, EventSourceError> {
        let key = (desired_state.session_id.clone(), desired_state.chain_id);
        if let Some(existing) = self.state.lock().await.engines.get(&key).cloned()
            && existing.revision == desired_state.new_revision
        {
            return Ok(existing);
        }
        let managed = self
            .build_engine(desired_state, acknowledged_cursor)
            .await?;
        let mut state = self.state.lock().await;
        if let Some(existing) = state.engines.get(&key).cloned()
            && existing.revision == desired_state.new_revision
        {
            return Ok(existing);
        }
        ensure_session_capacity(&state, &key, self.max_resident_sessions)?;
        state.engines.insert(key, managed.clone());
        Ok(managed)
    }
}

#[async_trait]
impl<F> EventSource for ManagedEventProvider<F>
where
    F: ChainDataSourceFactory,
{
    fn capabilities(&self) -> SourceCapabilities {
        self.source_capabilities(self.factory.supports_full_headers_on_any_chain())
    }

    fn capabilities_for_chain(&self, chain_id: u64) -> SourceCapabilities {
        self.source_capabilities(self.factory.supports_full_headers(chain_id))
    }

    async fn prepare_desired_state(
        &self,
        preparation_id: PreparationId,
        desired_state: &ApplyDesiredState,
        acknowledged_cursor: Option<&Cursor>,
    ) -> Result<Option<Cursor>, EventSourceError> {
        let key = (desired_state.session_id.clone(), desired_state.chain_id);
        {
            let state = self.state.lock().await;
            ensure_session_capacity(&state, &key, self.max_resident_sessions)?;
        }
        let managed = self
            .build_engine(desired_state, acknowledged_cursor)
            .await?;
        let activation_cursor = managed
            .engine
            .lock()
            .await
            .activation_cursor()
            .map_err(source_engine_error)?;
        let mut state = self.state.lock().await;
        ensure_session_capacity(&state, &key, self.max_resident_sessions)?;
        state
            .prepared
            .retain(|(prepared_key, _), _| prepared_key != &key);
        state.prepared.insert((key, preparation_id), managed);
        Ok(Some(activation_cursor))
    }

    async fn activate_desired_state(
        &self,
        preparation_id: PreparationId,
        desired_state: &ApplyDesiredState,
        _acknowledged_cursor: Option<&Cursor>,
    ) -> Result<(), EventSourceError> {
        let key = (desired_state.session_id.clone(), desired_state.chain_id);
        let mut state = self.state.lock().await;
        let managed = state
            .prepared
            .remove(&(key.clone(), preparation_id))
            .ok_or_else(|| EventSourceError::internal("prepared source candidate is missing"))?;
        if managed.revision != desired_state.new_revision {
            return Err(EventSourceError::internal(
                "prepared source candidate revision does not match activation",
            ));
        }
        state.engines.insert(key, managed);
        Ok(())
    }

    async fn abort_desired_state(
        &self,
        preparation_id: PreparationId,
        desired_state: &ApplyDesiredState,
    ) -> Result<(), EventSourceError> {
        let key = (desired_state.session_id.clone(), desired_state.chain_id);
        self.state
            .lock()
            .await
            .prepared
            .remove(&(key, preparation_id));
        Ok(())
    }

    async fn release_session(
        &self,
        session_id: &str,
        chain_id: u64,
    ) -> Result<(), EventSourceError> {
        let key = (session_id.to_owned(), chain_id);
        let mut state = self.state.lock().await;
        state.engines.remove(&key);
        state
            .prepared
            .retain(|(prepared_key, _), _| prepared_key != &key);
        Ok(())
    }

    async fn next_delivery(
        &self,
        request: DeliveryRequest<'_>,
    ) -> Result<Option<Delivery>, EventSourceError> {
        let desired_state = request.desired_state();
        let acknowledged_cursor = request.acknowledged_cursor();
        let required_target = required_reorg_target(request)?;
        let managed = self.engine(desired_state, acknowledged_cursor).await?;
        let mut engine = managed.engine.lock().await;
        // SSE archive heights are wakeup hints only: reconnects, provider
        // corrections, and cross-replica lag can move them in either direction.
        // Reconcile every query target against the authoritative REST height.
        let archive_height = available_height(engine.source(), self.request_timeout)
            .await
            .map_err(source_error)?;
        if let Some(driver) = managed.height_driver.as_ref() {
            driver.latest_hint.store(archive_height, Ordering::Release);
        }
        if archive_height <= engine.committed_next_block() && required_target.is_none() {
            return Ok(None);
        }
        let target = match required_target {
            Some(target) if archive_height < target => return Ok(None),
            Some(target) => target,
            None => archive_height,
        };
        tokio::time::timeout(self.request_timeout, engine.next_batch(target))
            .await
            .map_err(|_| {
                source_error(SourceError::RequestTimeout {
                    millis: self.request_timeout.as_millis(),
                })
            })?
            .map_err(source_engine_error)
    }

    async fn wait_for_update(&self, request: DeliveryRequest<'_>) -> Result<(), EventSourceError> {
        let desired_state = request.desired_state();
        let acknowledged_cursor = request.acknowledged_cursor();
        let required_target = required_reorg_target(request)?;
        let managed = self.engine(desired_state, acknowledged_cursor).await?;
        let Some(driver) = managed.height_driver.as_ref() else {
            return std::future::pending().await;
        };
        let committed_next_block = managed.engine.lock().await.committed_next_block();
        loop {
            let latest_hint = driver.latest_hint.load(Ordering::Acquire);
            let ready = required_target.map_or(latest_hint > committed_next_block, |target| {
                latest_hint >= target
            });
            if ready {
                return Ok(());
            }
            driver.update.notified().await;
        }
    }

    async fn acknowledge(
        &self,
        chain_id: u64,
        acknowledgement: &Acknowledge,
        committed_cursor: &Cursor,
    ) -> Result<(), EventSourceError> {
        let key = (acknowledgement.session_id.clone(), chain_id);
        let managed = self.state.lock().await.engines.get(&key).cloned();
        let Some(managed) = managed else {
            // SQLite remains authoritative. A replayed pending batch can be
            // acknowledged before its in-memory engine is restored.
            return Ok(());
        };
        let mut engine = managed.engine.lock().await;
        if acknowledgement.sequence <= engine.committed_sequence() {
            return Ok(());
        }
        match engine.acknowledge(&acknowledgement.delivery_token) {
            Ok(()) => Ok(()),
            Err(SourceEngineError::NoPendingDelivery)
                if committed_cursor.query_revision == managed.revision =>
            {
                let checkpoint = checkpoints_from_cursor(committed_cursor)?;
                let activation_block = checkpoint
                    .activation_block
                    .unwrap_or_else(|| engine.activation_block());
                engine
                    .synchronize_committed_cursor(SourceResume {
                        next_block: committed_cursor.next_block,
                        sequence: acknowledgement.sequence,
                        activation_block,
                        owner_backfill_activation_block: committed_cursor
                            .owner_backfill_activation_block,
                        canonical_blocks: checkpoint.canonical_blocks,
                        provider_checkpoint: checkpoint.rollback_guard,
                        coverage_head: committed_cursor.canonical_head.clone(),
                    })
                    .map_err(source_engine_error)
            }
            Err(error) => Err(source_engine_error(error)),
        }
    }
}

impl<F> ManagedEventProvider<F>
where
    F: ChainDataSourceFactory,
{
    fn source_capabilities(&self, supports_full_headers: bool) -> SourceCapabilities {
        let mut capabilities = vec![
            Capability::Historical.into(),
            Capability::Logs.into(),
            Capability::ServerFiltering.into(),
            Capability::DynamicFilters.into(),
            Capability::ExplicitReorgs.into(),
            Capability::NativeCheckpoint.into(),
            Capability::OwnerScopedDelivery.into(),
            Capability::DurableReplay.into(),
        ];
        if supports_full_headers {
            capabilities.push(Capability::Headers.into());
        }
        SourceCapabilities {
            capabilities: capabilities.clone(),
            sources: vec![SourceDescriptor {
                source_id: "hypersync".into(),
                role: SourceRole::Historical.into(),
                capabilities,
            }],
        }
    }
}

fn checkpoints_from_cursor(cursor: &Cursor) -> Result<DecodedSourceCheckpoint, EventSourceError> {
    let mut checkpoint = decode_source_checkpoint(&cursor.provider_checkpoint)
        .map_err(|error| EventSourceError::invalid(error.to_string()))?;
    if checkpoint.activation_block.is_some()
        && cursor.owner_backfill_activation_block.is_some()
        && checkpoint.activation_block != cursor.owner_backfill_activation_block
    {
        return Err(EventSourceError::invalid(
            "portable owner backfill activation boundary conflicts with provider checkpoint",
        ));
    }
    if checkpoint.activation_block.is_none() {
        checkpoint.activation_block = cursor.owner_backfill_activation_block;
    }
    if checkpoint.canonical_blocks.is_empty()
        && let Some(head) = cursor.canonical_head.clone()
        && head.number.checked_add(1) == Some(cursor.next_block)
    {
        checkpoint.canonical_blocks.push(head);
    }
    Ok(checkpoint)
}

fn requests_full_headers(desired_state: &ApplyDesiredState) -> bool {
    desired_state.owners.iter().any(|owner| {
        owner.interests.iter().any(|interest| {
            matches!(
                interest.kind.as_ref(),
                Some(portable_interest::Kind::Block(_))
            )
        })
    })
}

async fn verify_retained_backfill_baselines<S: ChainDataSource>(
    source: &S,
    desired_state: &ApplyDesiredState,
    retained_history: &[evm_fork_cache_event_protocol::v1::BlockRef],
    request_timeout: Duration,
    response_limits: SourceResponseLimits,
) -> Result<(), EventSourceError> {
    let mut baselines = BTreeMap::new();
    for baseline in desired_state
        .owners
        .iter()
        .filter_map(|owner| owner.backfill.as_ref())
        .filter_map(|backfill| backfill.retained_baseline.as_ref())
    {
        if let Some(existing) = baselines.insert(baseline.number, baseline)
            && existing != baseline
        {
            return Err(EventSourceError::invalid(
                "retained backfill baselines conflict at the same block height",
            ));
        }
    }
    for baseline in baselines.into_values() {
        if baseline.hash.len() != 32 || baseline.parent_hash.len() != 32 {
            return Err(EventSourceError::invalid(
                "retained backfill baseline hashes must be 32 bytes",
            ));
        }
        if let Some(retained) = retained_history
            .iter()
            .find(|retained| retained.number == baseline.number)
        {
            if retained != baseline {
                return Err(EventSourceError::invalid(
                    "retained backfill baseline conflicts with durable canonical history",
                ));
            }
            continue;
        }
        let end = baseline.number.checked_add(1).ok_or_else(|| {
            EventSourceError::invalid("retained backfill baseline has no successor")
        })?;
        let mut query = Query::new()
            .from_block(baseline.number)
            .to_block_excl(end)
            .include_all_blocks()
            .select_block_fields([
                BlockField::Number,
                BlockField::Hash,
                BlockField::ParentHash,
                BlockField::Timestamp,
            ]);
        query.max_num_blocks = Some(1);
        query.max_num_logs = Some(1);
        let page = retry_source_request(request_timeout, || source.query(query.clone()))
            .await
            .map_err(source_error)?;
        validate_response_counts(&page, response_limits).map_err(source_engine_error)?;
        validate_source_page(&page, baseline.number, end)
            .map_err(|error| EventSourceError::unavailable(error.to_string()))?;
        let observed =
            block_ref(page.blocks.first().ok_or_else(|| {
                EventSourceError::unavailable("retained baseline is unavailable")
            })?)
            .map_err(|error| EventSourceError::unavailable(error.to_string()))?;
        if &observed != baseline {
            return Err(EventSourceError::invalid(
                "retained backfill baseline does not match provider canonical identity",
            ));
        }
    }
    Ok(())
}

fn ensure_session_capacity<S>(
    state: &ProviderState<S>,
    key: &SessionKey,
    limit: usize,
) -> Result<(), EventSourceError> {
    let already_resident = state.engines.contains_key(key)
        || state
            .prepared
            .keys()
            .any(|(prepared_key, _)| prepared_key == key);
    if already_resident {
        return Ok(());
    }
    let mut resident = HashSet::with_capacity(state.engines.len() + state.prepared.len());
    resident.extend(state.engines.keys().cloned());
    resident.extend(
        state
            .prepared
            .keys()
            .map(|(prepared_key, _)| prepared_key.clone()),
    );
    if resident.len() >= limit {
        return Err(EventSourceError::resource_exhausted(format!(
            "managed source reached its {limit}-session resident limit"
        )));
    }
    Ok(())
}

fn required_reorg_target(request: DeliveryRequest<'_>) -> Result<Option<u64>, EventSourceError> {
    let Some(anchor) = request.required_reorg_anchor() else {
        return Ok(None);
    };
    if anchor.hash.len() != 32 || anchor.parent_hash.len() != 32 {
        return Err(EventSourceError::invalid(
            "required reorg anchor hashes must be 32 bytes",
        ));
    }
    let target = anchor
        .number
        .checked_add(1)
        .ok_or_else(|| EventSourceError::invalid("required reorg anchor has no successor"))?;
    let cursor = request.acknowledged_cursor().ok_or_else(|| {
        EventSourceError::invalid(
            "required reorg anchor is missing its acknowledged predecessor cursor",
        )
    })?;
    if cursor.next_block > anchor.number
        || cursor
            .canonical_head
            .as_ref()
            .is_some_and(|head| head.number >= anchor.number)
    {
        return Err(EventSourceError::invalid(
            "required reorg anchor does not follow the acknowledged source position",
        ));
    }
    if let Some(head) = cursor.canonical_head.as_ref()
        && head
            .number
            .checked_add(1)
            .is_some_and(|successor| successor == anchor.number)
        && anchor.parent_hash != head.hash
    {
        return Err(EventSourceError::invalid(
            "required reorg anchor does not descend from the acknowledged canonical head",
        ));
    }
    Ok(Some(target))
}

fn source_error(error: SourceError) -> EventSourceError {
    match error {
        SourceError::InvalidRequestTimeout
        | SourceError::InvalidSessionLimit
        | SourceError::InvalidReorgDepth
        | SourceError::InvalidDeliverySizeLimit { .. }
        | SourceError::InvalidResponseLimit { .. } => EventSourceError::invalid(error.to_string()),
        SourceError::Request(_) => {
            EventSourceError::unavailable("upstream chain data source request failed")
        }
        SourceError::UnavailableHeight | SourceError::RequestTimeout { .. } => {
            EventSourceError::unavailable(error.to_string())
        }
    }
}

fn source_engine_error(error: SourceEngineError) -> EventSourceError {
    match error {
        SourceEngineError::QueryPlan(QueryPlanError::TooManyCompiledLogFilters { .. })
        | SourceEngineError::DeliveryTooLarge { .. }
        | SourceEngineError::ResponseLimitExceeded { .. }
        | SourceEngineError::SequenceExhausted => {
            EventSourceError::resource_exhausted(error.to_string())
        }
        SourceEngineError::QueryPlan(_) => EventSourceError::invalid(error.to_string()),
        SourceEngineError::Source(error) => source_error(error),
        SourceEngineError::Normalize(_)
        | SourceEngineError::InvalidPage(_)
        | SourceEngineError::Canonical(_)
        | SourceEngineError::NoProgress { .. }
        | SourceEngineError::MissingRollbackGuard
        | SourceEngineError::OwnerCatchupReorg { .. }
        | SourceEngineError::ResumeCursorMismatch { .. }
        | SourceEngineError::ResumeGuardMismatch(_)
        | SourceEngineError::CoverageCursorMismatch { .. }
        | SourceEngineError::CoverageBoundaryConflict { .. } => {
            EventSourceError::unavailable(error.to_string())
        }
        SourceEngineError::InvalidDeliverySizeLimit { .. }
        | SourceEngineError::NoPendingDelivery
        | SourceEngineError::DeliveryTokenMismatch
        | SourceEngineError::MissingCursor
        | SourceEngineError::PendingDelivery
        | SourceEngineError::SequenceRegression { .. }
        | SourceEngineError::BlockNumberOverflow
        | SourceEngineError::BlockTimestampOverflow(_) => {
            EventSourceError::internal(error.to_string())
        }
    }
}

async fn available_height<S: ChainDataSource>(
    source: &S,
    request_timeout: Duration,
) -> Result<u64, SourceError> {
    let height = retry_source_request(request_timeout, || source.height()).await?;
    if height == 0 {
        return Err(SourceError::UnavailableHeight);
    }
    Ok(height)
}

async fn retry_source_request<T, F, Fut>(
    request_timeout: Duration,
    mut request: F,
) -> Result<T, SourceError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, SourceError>>,
{
    let deadline = tokio::time::Instant::now() + request_timeout;
    let mut last_error = None;

    for attempt in 0..SOURCE_REQUEST_ATTEMPTS {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }
        let attempts_left = SOURCE_REQUEST_ATTEMPTS - attempt;
        let fair_share = (deadline - now) / attempts_left;
        let attempt_timeout = fair_share.min(MAX_SOURCE_REQUEST_ATTEMPT);
        match tokio::time::timeout(attempt_timeout, request()).await {
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(error)) => last_error = Some(error),
            Err(_) => last_error = None,
        }
    }

    Err(last_error.unwrap_or(SourceError::RequestTimeout {
        millis: request_timeout.as_millis(),
    }))
}

#[cfg(test)]
mod source_error_tests {
    use super::*;

    #[test]
    fn upstream_request_details_are_not_exposed_to_protocol_clients() {
        let secret = "https://user:secret-token@indexer.invalid/private";
        let public = source_error(SourceError::Request(secret.into()));
        assert_eq!(
            public.kind,
            evm_fork_cache_event_service::EventSourceErrorKind::Unavailable
        );
        assert!(!public.message.contains(secret));
        assert!(!public.to_string().contains(secret));
        assert_eq!(public.message, "upstream chain data source request failed");
    }
}
