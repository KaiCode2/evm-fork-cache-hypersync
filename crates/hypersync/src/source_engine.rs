use std::pin::Pin;

use async_trait::async_trait;
use evm_fork_cache_event_protocol::{
    MAX_MESSAGE_SIZE_BYTES,
    v1::{ApplyDesiredState, Barrier, BlockRef, Cursor, Delivery, Reorg, delivery},
};
use futures::Stream;
use hypersync_client::{
    format::Hash,
    net_types::{BlockField, Query, RollbackGuard},
};
use prost::Message;

use crate::{
    CanonicalError, CanonicalTracker, NormalizeError, QueryPlanError, ReorgDelta, SourcePage,
    SourcePageError,
    normalize::{block_ref, encode_source_checkpoint, normalize_page_at},
    page_validation::validate_source_page,
    query_plan::{MAX_BLOCKS_PER_QUERY, MAX_LOGS_PER_QUERY, compile_query_with_limits},
};

/// Maximum encoded delivery accepted from this adapter before the gRPC envelope is added.
pub const MAX_DELIVERY_SIZE_BYTES: usize = MAX_MESSAGE_SIZE_BYTES - 64 * 1024;
/// Hard local block-row ceiling, independent of HyperSync's soft query target.
pub const MAX_BLOCKS_PER_RESPONSE: usize = MAX_BLOCKS_PER_QUERY * 2;
/// Hard local log-row ceiling, independent of HyperSync's soft query target.
pub const MAX_LOGS_PER_RESPONSE: usize = MAX_LOGS_PER_QUERY * 2;
/// Hard local ceiling for provider-owned row and field bytes in one decoded response.
pub const MAX_DYNAMIC_BYTES_PER_RESPONSE: usize = 16 * 1024 * 1024;

/// Hard local admission policy applied immediately after a provider response is decoded.
///
/// HyperSync's `max_num_blocks` and `max_num_logs` query fields are batching
/// targets, not response guarantees. These limits are deliberately separate:
/// modest soft-target overshoot remains interoperable, while a buggy or
/// compromised provider cannot make this adapter sort, clone, or normalize an
/// unbounded decoded page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct SourceResponseLimits {
    max_blocks: usize,
    max_logs: usize,
    max_dynamic_bytes: usize,
}

impl SourceResponseLimits {
    /// Create a nonzero hard response policy.
    ///
    /// # Errors
    ///
    /// Returns [`SourceError::InvalidResponseLimit`] when any limit is zero.
    pub fn new(
        max_blocks: usize,
        max_logs: usize,
        max_dynamic_bytes: usize,
    ) -> Result<Self, SourceError> {
        if max_blocks == 0 {
            return Err(SourceError::InvalidResponseLimit { resource: "blocks" });
        }
        if max_logs == 0 {
            return Err(SourceError::InvalidResponseLimit { resource: "logs" });
        }
        if max_dynamic_bytes == 0 {
            return Err(SourceError::InvalidResponseLimit {
                resource: "dynamic bytes",
            });
        }
        Ok(Self {
            max_blocks,
            max_logs,
            max_dynamic_bytes,
        })
    }

    /// Maximum decoded block rows admitted from one provider response.
    pub const fn max_blocks(&self) -> usize {
        self.max_blocks
    }

    /// Maximum decoded log rows admitted from one provider response.
    pub const fn max_logs(&self) -> usize {
        self.max_logs
    }

    /// Maximum provider-owned row and field bytes admitted from one decoded response.
    pub const fn max_dynamic_bytes(&self) -> usize {
        self.max_dynamic_bytes
    }
}

impl Default for SourceResponseLimits {
    fn default() -> Self {
        Self {
            max_blocks: MAX_BLOCKS_PER_RESPONSE,
            max_logs: MAX_LOGS_PER_RESPONSE,
            max_dynamic_bytes: MAX_DYNAMIC_BYTES_PER_RESPONSE,
        }
    }
}

/// Provider-neutral stream of exclusive archive-height updates.
pub type ChainHeightStream = Pin<Box<dyn Stream<Item = u64> + Send + 'static>>;

/// Async boundary around HyperSync, mockable at the external service edge.
#[async_trait]
pub trait ChainDataSource: Send + Sync {
    /// Return the current exclusive archive height.
    ///
    /// Blocks with numbers strictly below this height are available to query.
    ///
    /// # Errors
    ///
    /// Returns [`SourceError`] when the external provider is unavailable,
    /// rejects the request, times out, or cannot supply a usable height.
    async fn height(&self) -> Result<u64, SourceError>;

    /// Execute one bounded query page.
    ///
    /// # Errors
    ///
    /// Returns [`SourceError`] when the external provider/transport rejects,
    /// times out, or cannot complete the query.
    async fn query(&self, query: Query) -> Result<SourcePage, SourceError>;

    /// Subscribe to archive-height updates when the provider offers a push
    /// channel. Returning `None` leaves the service on its polling fallback.
    fn height_stream(&self) -> Option<ChainHeightStream> {
        None
    }
}

/// One-in-flight source engine with at-least-once replay semantics.
pub struct SourceEngine<S> {
    source: S,
    desired_state: ApplyDesiredState,
    committed_next_block: u64,
    committed_sequence: u64,
    activation_block: u64,
    owner_backfill_activation_block: Option<u64>,
    coverage_head: Option<BlockRef>,
    canonical: CanonicalTracker,
    previous_guard: Option<RollbackGuard>,
    max_delivery_bytes: usize,
    response_limits: SourceResponseLimits,
    pending: Option<PendingDelivery>,
    queued: Option<PendingDelivery>,
}

struct PendingDelivery {
    delivery: Delivery,
    rollback_guard: Option<RollbackGuard>,
    advances_source: bool,
}

/// Durable state used to rebuild a source engine after process restart.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct SourceResume {
    /// Exclusive provider block cursor.
    pub next_block: u64,
    /// Last committed delivery sequence.
    pub sequence: u64,
    /// First block routed to all live owners for this desired-state revision.
    pub activation_block: u64,
    /// Portable activation boundary preserved exactly for this revision.
    pub owner_backfill_activation_block: Option<u64>,
    /// Retained canonical suffix used for post-restart reorg resolution.
    pub canonical_blocks: Vec<BlockRef>,
    /// Provider-native rollback/checkpoint state.
    pub provider_checkpoint: Option<RollbackGuard>,
    /// Runtime-global canonical coverage, independent of the provider scan cursor.
    pub coverage_head: Option<BlockRef>,
}

impl SourceResume {
    /// Create a resume position without retained canonical or provider-native metadata.
    pub fn new(next_block: u64, sequence: u64, activation_block: u64) -> Self {
        Self {
            next_block,
            sequence,
            activation_block,
            owner_backfill_activation_block: Some(activation_block),
            canonical_blocks: Vec::new(),
            provider_checkpoint: None,
            coverage_head: None,
        }
    }

    /// Attach the retained normalized canonical suffix.
    pub fn with_canonical_blocks(mut self, canonical_blocks: Vec<BlockRef>) -> Self {
        self.canonical_blocks = canonical_blocks;
        self
    }

    /// Attach the provider-native checkpoint that agrees with the normalized suffix.
    pub fn with_provider_checkpoint(mut self, provider_checkpoint: Option<RollbackGuard>) -> Self {
        self.provider_checkpoint = provider_checkpoint;
        self
    }

    /// Attach the runtime-global canonical coverage restored from the durable cursor.
    pub fn with_coverage_head(mut self, coverage_head: Option<BlockRef>) -> Self {
        self.coverage_head = coverage_head;
        self
    }
}

impl<S> SourceEngine<S>
where
    S: ChainDataSource,
{
    /// Create an engine at an acknowledged block cursor.
    pub fn new(
        source: S,
        desired_state: ApplyDesiredState,
        committed_next_block: u64,
        activation_block: u64,
        reorg_depth: usize,
    ) -> Self {
        Self {
            source,
            desired_state,
            committed_next_block,
            committed_sequence: 0,
            activation_block,
            owner_backfill_activation_block: Some(activation_block),
            coverage_head: None,
            canonical: CanonicalTracker::new(reorg_depth),
            previous_guard: None,
            max_delivery_bytes: MAX_DELIVERY_SIZE_BYTES,
            response_limits: SourceResponseLimits::default(),
            pending: None,
            queued: None,
        }
    }

    /// Restore an engine from a mutually consistent cursor, canonical suffix,
    /// and provider checkpoint.
    ///
    /// # Errors
    ///
    /// Returns [`SourceEngineError`] when retained canonical history,
    /// provider checkpoint, scan cursor, activation metadata, or coverage head
    /// are malformed or mutually inconsistent.
    pub fn restore(
        source: S,
        desired_state: ApplyDesiredState,
        resume: SourceResume,
        reorg_depth: usize,
    ) -> Result<Self, SourceEngineError> {
        let mut canonical = CanonicalTracker::new(reorg_depth);
        if !resume.canonical_blocks.is_empty() {
            canonical.apply_blocks(resume.canonical_blocks)?;
        }
        let previous_guard =
            validate_resume_state(&canonical, resume.next_block, resume.provider_checkpoint)?;
        validate_coverage_state(&canonical, resume.next_block, resume.coverage_head.as_ref())?;
        Ok(Self {
            source,
            desired_state,
            committed_next_block: resume.next_block,
            committed_sequence: resume.sequence,
            activation_block: resume.activation_block,
            owner_backfill_activation_block: resume.owner_backfill_activation_block,
            coverage_head: resume.coverage_head,
            canonical,
            previous_guard,
            max_delivery_bytes: MAX_DELIVERY_SIZE_BYTES,
            response_limits: SourceResponseLimits::default(),
            pending: None,
            queued: None,
        })
    }

    /// Override the encoded delivery budget, primarily for stricter deployments and tests.
    ///
    /// # Errors
    ///
    /// Returns [`SourceEngineError::InvalidDeliverySizeLimit`] when the limit
    /// is zero or exceeds [`MAX_DELIVERY_SIZE_BYTES`].
    pub fn with_max_delivery_bytes(
        mut self,
        max_delivery_bytes: usize,
    ) -> Result<Self, SourceEngineError> {
        if max_delivery_bytes == 0 || max_delivery_bytes > MAX_DELIVERY_SIZE_BYTES {
            return Err(SourceEngineError::InvalidDeliverySizeLimit {
                requested: max_delivery_bytes,
                maximum: MAX_DELIVERY_SIZE_BYTES,
            });
        }
        self.max_delivery_bytes = max_delivery_bytes;
        Ok(self)
    }

    /// Override the hard local decoded-response admission policy.
    pub fn with_response_limits(mut self, response_limits: SourceResponseLimits) -> Self {
        self.response_limits = response_limits;
        self
    }

    /// Fetch the next page up to an exclusive target, replaying an unacknowledged
    /// page without another provider request.
    ///
    /// # Errors
    ///
    /// Returns [`SourceEngineError`] for query/source failure, invalid or
    /// oversized provider data, unresolvable continuity/reorg state, sequence
    /// exhaustion, or an owner-only fork protocol v1 cannot represent.
    pub async fn next_batch(
        &mut self,
        to_block_excl: u64,
    ) -> Result<Option<Delivery>, SourceEngineError> {
        if let Some(pending) = &self.pending {
            return Ok(Some(pending.delivery.clone()));
        }
        if self.committed_next_block >= to_block_excl {
            return Ok(None);
        }
        let sequence = self
            .committed_sequence
            .checked_add(1)
            .ok_or(SourceEngineError::SequenceExhausted)?;

        let mut query_limits = QueryLimits::for_range(self.committed_next_block, to_block_excl);
        let mut initial_anchor = None;
        let (mut delivery, rollback_guard, canonical, reorg) = loop {
            let page = self
                .query_page(self.committed_next_block, to_block_excl, query_limits)
                .await?;
            if initial_anchor.is_none() {
                initial_anchor = self.query_canonical_anchor(&page).await?;
            }
            let prepared = if self.rollback_detected(&page) {
                self.recover_rollback(to_block_excl, sequence, query_limits)
                    .await?
            } else {
                self.prepare_batch(page, sequence, initial_anchor.as_ref())?
            };
            let encoded_len = prepared.0.encoded_len();
            if encoded_len <= self.max_delivery_bytes {
                break prepared;
            }
            if !query_limits.shrink() {
                return Err(SourceEngineError::DeliveryTooLarge {
                    encoded_len,
                    limit: self.max_delivery_bytes,
                });
            }
        };
        if let Some(reorg) = reorg {
            // Every block on the previously delivered branch was owner-only.
            // The v1 protocol intentionally has no owner-scoped reorg control,
            // so emitting its global Reorg here would rewind unrelated runtime
            // coverage. Do not silently send replacement records either: an
            // acknowledged owner may already have applied the displaced branch.
            // Fail closed until a fresh desired-state revision restarts that
            // owner's catch-up from an authoritative position.
            if reorg.old_tip.number < self.activation_block
                && !global_backfill_contains(&self.desired_state, reorg.old_tip.number)
            {
                return Err(SourceEngineError::OwnerCatchupReorg {
                    activation_block: self.activation_block,
                    common_ancestor: reorg.common_ancestor.number,
                    old_tip: reorg.old_tip.number,
                    new_tip: reorg.new_tip.number,
                });
            }
            let replacement_sequence = sequence
                .checked_add(1)
                .ok_or(SourceEngineError::SequenceExhausted)?;
            set_delivery_sequence(&mut delivery, replacement_sequence)?;
            let control = self.reorg_delivery(sequence, reorg)?;
            let encoded_len = control.encoded_len();
            if encoded_len > self.max_delivery_bytes {
                return Err(SourceEngineError::DeliveryTooLarge {
                    encoded_len,
                    limit: self.max_delivery_bytes,
                });
            }
            self.canonical = canonical;
            self.queued = Some(PendingDelivery {
                delivery,
                rollback_guard,
                advances_source: true,
            });
            self.pending = Some(PendingDelivery {
                delivery: control.clone(),
                rollback_guard: self.previous_guard.clone(),
                advances_source: false,
            });
            Ok(Some(control))
        } else {
            self.canonical = canonical;
            self.pending = Some(PendingDelivery {
                delivery: delivery.clone(),
                rollback_guard,
                advances_source: true,
            });
            Ok(Some(delivery))
        }
    }

    /// Commit the only in-flight delivery token and advance the durable cursor.
    ///
    /// # Errors
    ///
    /// Returns [`SourceEngineError::NoPendingDelivery`] when nothing is in
    /// flight, [`SourceEngineError::DeliveryTokenMismatch`] for the wrong
    /// token, or [`SourceEngineError::MissingCursor`] for an invalid pending
    /// delivery.
    pub fn acknowledge(&mut self, delivery_token: &[u8]) -> Result<(), SourceEngineError> {
        let pending = self
            .pending
            .as_ref()
            .ok_or(SourceEngineError::NoPendingDelivery)?;
        if pending.delivery.delivery_token != delivery_token {
            return Err(SourceEngineError::DeliveryTokenMismatch);
        }
        let cursor = pending
            .delivery
            .cursor
            .as_ref()
            .ok_or(SourceEngineError::MissingCursor)?;
        let committed_next_block = cursor.next_block;
        let committed_sequence = pending.delivery.sequence;
        let advances_source = pending.advances_source;
        let rollback_guard = pending.rollback_guard.clone();
        let coverage_head = cursor.canonical_head.clone();
        if advances_source {
            self.committed_next_block = committed_next_block;
            self.previous_guard = rollback_guard;
        }
        self.committed_sequence = committed_sequence;
        self.coverage_head = coverage_head;
        self.pending = self.queued.take();
        Ok(())
    }

    /// Exclusive block cursor durably acknowledged by the consumer.
    pub fn committed_next_block(&self) -> u64 {
        self.committed_next_block
    }

    /// Cursor persisted on the desired-state activation barrier before the
    /// first provider page is requested.
    ///
    /// # Errors
    ///
    /// Returns [`SourceEngineError`] when retained canonical/provider
    /// checkpoint metadata cannot be encoded safely.
    pub fn activation_cursor(&self) -> Result<Cursor, SourceEngineError> {
        Ok(Cursor {
            chain_id: self.desired_state.chain_id,
            query_revision: self.desired_state.new_revision,
            next_block: self.committed_next_block,
            canonical_head: self.coverage_head.clone(),
            batch_sequence: self.committed_sequence,
            provider_checkpoint: encode_source_checkpoint(
                self.previous_guard.as_ref(),
                self.activation_block,
                self.canonical.blocks(),
            )?,
            owner_backfill_activation_block: self.owner_backfill_activation_block,
        })
    }

    /// Last batch sequence durably acknowledged by the consumer.
    pub fn committed_sequence(&self) -> u64 {
        self.committed_sequence
    }

    /// Advance sequence across a service-generated control that does not touch
    /// this source's provider cursor.
    ///
    /// # Errors
    ///
    /// Returns [`SourceEngineError::PendingDelivery`] while a source delivery
    /// is in flight, or [`SourceEngineError::SequenceRegression`] if `sequence`
    /// moves behind committed authority.
    pub fn acknowledge_external_control(&mut self, sequence: u64) -> Result<(), SourceEngineError> {
        if self.pending.is_some() {
            return Err(SourceEngineError::PendingDelivery);
        }
        if sequence < self.committed_sequence {
            return Err(SourceEngineError::SequenceRegression {
                committed: self.committed_sequence,
                received: sequence,
            });
        }
        self.committed_sequence = sequence;
        Ok(())
    }

    /// Synchronize an engine rebuilt from an older acknowledged cursor after a
    /// separately persisted outbox delivery is committed.
    ///
    /// # Errors
    ///
    /// Returns [`SourceEngineError`] when a delivery is pending, sequence
    /// authority regresses, or the replacement cursor/checkpoint/history/
    /// coverage state is malformed or inconsistent. Validation completes
    /// before engine state is replaced.
    pub fn synchronize_committed_cursor(
        &mut self,
        resume: SourceResume,
    ) -> Result<(), SourceEngineError> {
        if self.pending.is_some() {
            return Err(SourceEngineError::PendingDelivery);
        }
        if resume.sequence < self.committed_sequence {
            return Err(SourceEngineError::SequenceRegression {
                committed: self.committed_sequence,
                received: resume.sequence,
            });
        }
        let mut canonical = self.canonical.clone();
        canonical.restore_blocks(resume.canonical_blocks)?;
        let previous_guard =
            validate_resume_state(&canonical, resume.next_block, resume.provider_checkpoint)?;
        validate_coverage_state(&canonical, resume.next_block, resume.coverage_head.as_ref())?;
        self.canonical = canonical;
        self.committed_next_block = resume.next_block;
        self.committed_sequence = resume.sequence;
        self.activation_block = resume.activation_block;
        self.previous_guard = previous_guard;
        self.coverage_head = resume.coverage_head;
        self.owner_backfill_activation_block = resume.owner_backfill_activation_block;
        Ok(())
    }

    /// First block routed to all live owners for the active revision.
    pub fn activation_block(&self) -> u64 {
        self.activation_block
    }

    /// Access the source, primarily for lifecycle and height-stream drivers.
    pub fn source(&self) -> &S {
        &self.source
    }

    async fn query_page(
        &self,
        from_block: u64,
        to_block_excl: u64,
        limits: QueryLimits,
    ) -> Result<SourcePage, SourceEngineError> {
        let query = compile_query_with_limits(
            &self.desired_state,
            from_block,
            Some(to_block_excl),
            limits.max_blocks,
            limits.max_logs,
        )?;
        let page = self.source.query(query).await?;
        validate_response_counts(&page, self.response_limits)?;
        if page.next_block <= from_block {
            return Err(SourceEngineError::NoProgress {
                from_block,
                next_block: page.next_block,
            });
        }
        validate_source_page(&page, from_block, to_block_excl)?;
        Ok(page)
    }

    async fn query_canonical_anchor(
        &self,
        page: &SourcePage,
    ) -> Result<Option<BlockRef>, SourceEngineError> {
        if !self.canonical.is_empty() {
            return Ok(None);
        }
        let Some(guard) = page.rollback_guard.as_ref() else {
            return Ok(None);
        };
        let Some(anchor_number) = guard.first_block_number.checked_sub(1) else {
            return Ok(None);
        };
        let anchor_end = anchor_number
            .checked_add(1)
            .ok_or(SourceEngineError::BlockNumberOverflow)?;
        let mut query = Query::new()
            .from_block(anchor_number)
            .to_block_excl(anchor_end)
            .include_all_blocks()
            .select_block_fields([
                BlockField::Number,
                BlockField::Hash,
                BlockField::ParentHash,
                BlockField::Timestamp,
            ]);
        query.max_num_blocks = Some(1);
        query.max_num_logs = Some(1);
        let anchor_page = self.source.query(query).await?;
        validate_response_counts(&anchor_page, self.response_limits)?;
        if anchor_page.next_block <= anchor_number {
            return Err(SourceEngineError::NoProgress {
                from_block: anchor_number,
                next_block: anchor_page.next_block,
            });
        }
        validate_source_page(&anchor_page, anchor_number, anchor_end)?;
        let anchor = block_ref(
            anchor_page
                .blocks
                .first()
                .expect("validated one-block anchor page"),
        )?;
        if anchor.hash.as_slice() != guard.first_parent_hash.as_ref() {
            return Err(CanonicalError::UnknownParent {
                number: guard.first_block_number,
                parent_hash: guard.first_parent_hash.as_ref().to_vec(),
            }
            .into());
        }
        Ok(Some(anchor))
    }

    fn rollback_detected(&self, page: &SourcePage) -> bool {
        match (&self.previous_guard, &page.rollback_guard) {
            (Some(previous), Some(current)) => {
                current.first_block_number <= previous.block_number.saturating_add(1)
                    && current.first_parent_hash != previous.hash
            }
            _ => false,
        }
    }

    async fn recover_rollback(
        &self,
        to_block_excl: u64,
        sequence: u64,
        limits: QueryLimits,
    ) -> Result<PreparedPage, SourceEngineError> {
        let previous = self
            .previous_guard
            .as_ref()
            .ok_or(SourceEngineError::MissingRollbackGuard)?;
        let oldest = self
            .canonical
            .blocks()
            .next()
            .map(|block| block.number)
            .ok_or(CanonicalError::HistoryExhausted {
                required_block: previous.block_number,
            })?;
        let newest = self
            .canonical
            .tip()
            .map(|block| block.number.min(previous.block_number))
            .ok_or(CanonicalError::HistoryExhausted {
                required_block: previous.block_number,
            })?;

        // A canonical fork is a shared prefix followed by a replacement
        // suffix. Probe that monotonic boundary instead of issuing one query
        // per retained height. Exact BlockRef equality is required: a hash may
        // not silently acquire a different parent or timestamp.
        let mut low = oldest;
        let mut high = newest;
        let mut common_ancestor = None;
        while low <= high {
            let candidate = low + (high - low) / 2;
            let observed = self.query_canonical_block(candidate).await?;
            let expected = self
                .canonical
                .block(candidate)
                .expect("binary probe stays inside retained canonical history");
            if observed.hash == expected.hash {
                if observed != *expected {
                    return Err(
                        CanonicalError::ConflictingBlockMetadata { number: candidate }.into(),
                    );
                }
                common_ancestor = Some(candidate);
                let Some(next) = candidate.checked_add(1) else {
                    break;
                };
                low = next;
            } else if candidate == 0 {
                break;
            } else {
                high = candidate - 1;
            }
        }
        let common_ancestor = common_ancestor.ok_or(CanonicalError::HistoryExhausted {
            required_block: oldest.saturating_sub(1),
        })?;
        let replacement_start = common_ancestor
            .checked_add(1)
            .ok_or(SourceEngineError::BlockNumberOverflow)?;
        let page = self
            .query_page(replacement_start, to_block_excl, limits)
            .await?;
        self.prepare_batch(page, sequence, None)
    }

    async fn query_canonical_block(&self, number: u64) -> Result<BlockRef, SourceEngineError> {
        let end = number
            .checked_add(1)
            .ok_or(SourceEngineError::BlockNumberOverflow)?;
        let mut query = Query::new()
            .from_block(number)
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
        let page = self.source.query(query).await?;
        validate_response_counts(&page, self.response_limits)?;
        if page.next_block <= number {
            return Err(SourceEngineError::NoProgress {
                from_block: number,
                next_block: page.next_block,
            });
        }
        validate_source_page(&page, number, end)?;
        block_ref(page.blocks.first().expect("validated single-block page")).map_err(Into::into)
    }

    fn prepare_batch(
        &self,
        page: SourcePage,
        sequence: u64,
        initial_anchor: Option<&BlockRef>,
    ) -> Result<PreparedPage, SourceEngineError> {
        let rollback_guard = page.rollback_guard.clone();
        let page_blocks = page
            .blocks
            .iter()
            .map(block_ref)
            .collect::<Result<Vec<_>, _>>()?;
        // Retained source history normally proves continuity itself. A
        // coverage-only restore (or owner rescan below that coverage) has no
        // such tracker edge, so compare the page directly to the durable
        // coverage identity. Do not apply this shortcut during rollback
        // recovery: its replacement page is expected to conflict with the
        // displaced tracked tip and is validated by `CanonicalTracker`.
        let crosses_untracked_coverage_boundary =
            self.coverage_head.as_ref().is_some_and(|coverage| {
                self.canonical.is_empty() || self.committed_next_block <= coverage.number
            });
        if initial_anchor.is_some() || crosses_untracked_coverage_boundary {
            validate_coverage_boundary(self.coverage_head.as_ref(), &page_blocks, initial_anchor)?;
        }
        let mut delivery =
            normalize_page_at(&self.desired_state, sequence, page, self.activation_block)?;
        let mut canonical = self.canonical.clone();
        if canonical.is_empty()
            && let Some(anchor) = initial_anchor
        {
            canonical.apply_blocks([anchor.clone()])?;
        }
        let reorg = canonical.apply_blocks(page_blocks)?;
        delivery
            .cursor
            .as_mut()
            .ok_or(SourceEngineError::MissingCursor)?
            .provider_checkpoint = encode_source_checkpoint(
            rollback_guard.as_ref(),
            self.activation_block,
            canonical.blocks(),
        )?;
        let cursor = delivery
            .cursor
            .as_mut()
            .expect("delivery cursor was validated above");
        cursor.owner_backfill_activation_block = self.owner_backfill_activation_block;
        if cursor.canonical_head.is_none() {
            cursor.canonical_head = self.coverage_head.clone();
        }
        if matches!(
            delivery.payload.as_ref(),
            Some(delivery::Payload::Data(data)) if data.records.is_empty()
        ) {
            let progress_block = delivery
                .cursor
                .as_ref()
                .and_then(|cursor| cursor.canonical_head.clone())
                .filter(|head| self.coverage_head.as_ref() != Some(head));
            delivery.payload = Some(delivery::Payload::Barrier(Barrier {
                id: format!(
                    "source-progress:{}:{}",
                    self.desired_state.new_revision,
                    delivery
                        .cursor
                        .as_ref()
                        .expect("validated cursor")
                        .next_block
                )
                .into_bytes(),
                block: progress_block.clone(),
            }));
            delivery.checkpoint_neutral = progress_block.is_none();
        }
        Ok((delivery, rollback_guard, canonical, reorg))
    }

    fn reorg_delivery(
        &self,
        sequence: u64,
        reorg: ReorgDelta,
    ) -> Result<Delivery, SourceEngineError> {
        let next_block = reorg
            .common_ancestor
            .number
            .checked_add(1)
            .ok_or(SourceEngineError::BlockNumberOverflow)?;
        let rollback_guard = rollback_guard_at(&reorg.common_ancestor)?;
        let canonical_prefix: Vec<_> = self
            .canonical
            .blocks()
            .take_while(|block| block.number <= reorg.common_ancestor.number)
            .collect();
        Ok(Delivery {
            session_id: self.desired_state.session_id.clone(),
            sequence,
            query_revision: self.desired_state.new_revision,
            delivery_token: sequence.to_be_bytes().to_vec(),
            cursor: Some(Cursor {
                chain_id: self.desired_state.chain_id,
                query_revision: self.desired_state.new_revision,
                next_block,
                canonical_head: Some(reorg.common_ancestor.clone()),
                batch_sequence: sequence,
                provider_checkpoint: encode_source_checkpoint(
                    Some(&rollback_guard),
                    self.activation_block,
                    canonical_prefix.into_iter(),
                )?,
                owner_backfill_activation_block: self.owner_backfill_activation_block,
            }),
            payload: Some(delivery::Payload::Reorg(Reorg {
                common_ancestor: Some(reorg.common_ancestor),
                old_tip: Some(reorg.old_tip),
                new_tip: Some(reorg.new_tip),
            })),
            checkpoint_neutral: false,
        })
    }
}

fn global_backfill_contains(desired_state: &ApplyDesiredState, block_number: u64) -> bool {
    desired_state.owners.iter().any(|owner| {
        owner.canonical
            && owner.backfill.as_ref().is_some_and(|backfill| {
                block_number >= backfill.from_block
                    && backfill.to_block_excl.is_none_or(|end| block_number < end)
            })
    })
}

fn validate_coverage_state(
    canonical: &CanonicalTracker,
    next_block: u64,
    coverage_head: Option<&BlockRef>,
) -> Result<(), SourceEngineError> {
    let Some(coverage_head) = coverage_head else {
        return Ok(());
    };
    let mut validator = CanonicalTracker::new(1);
    validator.apply_blocks([coverage_head.clone()])?;
    let coverage_successor = coverage_head
        .number
        .checked_add(1)
        .ok_or(SourceEngineError::BlockNumberOverflow)?;
    if next_block > coverage_successor {
        return Err(SourceEngineError::CoverageCursorMismatch {
            coverage_successor,
            received: next_block,
        });
    }
    if let Some(scanned) = canonical.block(coverage_head.number)
        && scanned != coverage_head
    {
        return Err(SourceEngineError::CoverageBoundaryConflict {
            number: coverage_head.number,
        });
    }
    Ok(())
}

fn validate_coverage_boundary(
    coverage_head: Option<&BlockRef>,
    page_blocks: &[BlockRef],
    initial_anchor: Option<&BlockRef>,
) -> Result<(), SourceEngineError> {
    let Some(coverage_head) = coverage_head else {
        return Ok(());
    };
    if let Some(observed) = page_blocks
        .iter()
        .find(|block| block.number == coverage_head.number)
        .or_else(|| initial_anchor.filter(|block| block.number == coverage_head.number))
        && observed != coverage_head
    {
        return Err(SourceEngineError::CoverageBoundaryConflict {
            number: coverage_head.number,
        });
    }
    if let Some(first) = page_blocks.first()
        && first.number == coverage_head.number.saturating_add(1)
        && first.parent_hash != coverage_head.hash
    {
        return Err(SourceEngineError::CoverageBoundaryConflict {
            number: coverage_head.number,
        });
    }
    Ok(())
}

pub(crate) fn validate_response_counts(
    page: &SourcePage,
    limits: SourceResponseLimits,
) -> Result<(), SourceEngineError> {
    if page.blocks.len() > limits.max_blocks {
        return Err(SourceEngineError::ResponseLimitExceeded {
            resource: "blocks",
            observed: page.blocks.len(),
            limit: limits.max_blocks,
        });
    }
    if page.logs.len() > limits.max_logs {
        return Err(SourceEngineError::ResponseLimitExceeded {
            resource: "logs",
            observed: page.logs.len(),
            limit: limits.max_logs,
        });
    }
    validate_response_dynamic_bytes(page, limits.max_dynamic_bytes)?;
    Ok(())
}

fn validate_response_dynamic_bytes(
    page: &SourcePage,
    limit: usize,
) -> Result<(), SourceEngineError> {
    // Row storage includes every inline field, notably Log's fixed-capacity
    // ArrayVec<Option<LogArgument>, 4>. Only heap-owned fields are added below;
    // charging `log.topics.as_slice()` again would double-count inline bytes.
    let mut observed = std::mem::size_of::<hypersync_client::simple_types::Block>()
        .saturating_mul(page.blocks.capacity())
        .saturating_add(
            std::mem::size_of::<hypersync_client::simple_types::Log>()
                .saturating_mul(page.logs.capacity()),
        );
    check_dynamic_bytes(observed, limit)?;

    macro_rules! add_bytes {
        ($value:expr) => {
            if let Some(value) = $value.as_ref() {
                observed = observed.saturating_add(value.as_ref().len());
                check_dynamic_bytes(observed, limit)?;
            }
        };
    }

    for block in &page.blocks {
        add_bytes!(block.hash);
        add_bytes!(block.parent_hash);
        add_bytes!(block.nonce);
        add_bytes!(block.sha3_uncles);
        add_bytes!(block.logs_bloom);
        add_bytes!(block.transactions_root);
        add_bytes!(block.state_root);
        add_bytes!(block.receipts_root);
        add_bytes!(block.miner);
        add_bytes!(block.difficulty);
        add_bytes!(block.total_difficulty);
        add_bytes!(block.extra_data);
        add_bytes!(block.size);
        add_bytes!(block.gas_limit);
        add_bytes!(block.gas_used);
        add_bytes!(block.timestamp);
        add_bytes!(block.base_fee_per_gas);
        add_bytes!(block.blob_gas_used);
        add_bytes!(block.excess_blob_gas);
        add_bytes!(block.parent_beacon_block_root);
        add_bytes!(block.withdrawals_root);
        add_bytes!(block.send_count);
        add_bytes!(block.send_root);
        add_bytes!(block.mix_hash);
        if let Some(uncles) = &block.uncles {
            observed = observed
                .saturating_add(std::mem::size_of::<Hash>().saturating_mul(uncles.capacity()));
            check_dynamic_bytes(observed, limit)?;
            for uncle in uncles {
                observed = observed.saturating_add(uncle.as_ref().len());
                check_dynamic_bytes(observed, limit)?;
            }
        }
        if let Some(withdrawals) = &block.withdrawals {
            observed = observed.saturating_add(
                std::mem::size_of::<hypersync_client::format::Withdrawal>()
                    .saturating_mul(withdrawals.capacity()),
            );
            check_dynamic_bytes(observed, limit)?;
            for withdrawal in withdrawals {
                add_bytes!(withdrawal.index);
                add_bytes!(withdrawal.validator_index);
                add_bytes!(withdrawal.address);
                add_bytes!(withdrawal.amount);
            }
        }
    }
    for log in &page.logs {
        add_bytes!(log.transaction_hash);
        add_bytes!(log.block_hash);
        add_bytes!(log.address);
        add_bytes!(log.data);
        for topic in &log.topics {
            add_bytes!(topic);
        }
    }
    if let Some(guard) = &page.rollback_guard {
        observed = observed
            .saturating_add(guard.hash.as_ref().len())
            .saturating_add(guard.first_parent_hash.as_ref().len());
        check_dynamic_bytes(observed, limit)?;
    }
    Ok(())
}

fn check_dynamic_bytes(observed: usize, limit: usize) -> Result<(), SourceEngineError> {
    if observed > limit {
        return Err(SourceEngineError::ResponseLimitExceeded {
            resource: "dynamic bytes",
            observed,
            limit,
        });
    }
    Ok(())
}

fn rollback_guard_at(block: &BlockRef) -> Result<RollbackGuard, SourceEngineError> {
    let hash: [u8; 32] =
        block
            .hash
            .as_slice()
            .try_into()
            .map_err(|_| CanonicalError::InvalidHashWidth {
                field: "hash",
                width: block.hash.len(),
            })?;
    let parent_hash: [u8; 32] =
        block
            .parent_hash
            .as_slice()
            .try_into()
            .map_err(|_| CanonicalError::InvalidHashWidth {
                field: "parent_hash",
                width: block.parent_hash.len(),
            })?;
    Ok(RollbackGuard {
        block_number: block.number,
        timestamp: i64::try_from(block.timestamp)
            .map_err(|_| SourceEngineError::BlockTimestampOverflow(block.timestamp))?,
        hash: Hash::from(hash),
        first_block_number: block.number,
        first_parent_hash: Hash::from(parent_hash),
    })
}

fn validate_resume_state(
    canonical: &CanonicalTracker,
    next_block: u64,
    provider_checkpoint: Option<RollbackGuard>,
) -> Result<Option<RollbackGuard>, SourceEngineError> {
    if provider_checkpoint
        .as_ref()
        .is_some_and(|guard| guard.first_block_number > guard.block_number)
    {
        return Err(SourceEngineError::ResumeGuardMismatch("first_block_number"));
    }
    let Some(tip) = canonical.tip() else {
        if let Some(guard) = provider_checkpoint {
            if guard.timestamp < 0 {
                return Err(SourceEngineError::ResumeGuardMismatch("timestamp"));
            }
            let expected = guard
                .block_number
                .checked_add(1)
                .ok_or(SourceEngineError::BlockNumberOverflow)?;
            if next_block != expected {
                return Err(SourceEngineError::ResumeCursorMismatch {
                    expected,
                    received: next_block,
                });
            }
            return Ok(Some(guard));
        }
        return Ok(None);
    };

    let expected = tip
        .number
        .checked_add(1)
        .ok_or(SourceEngineError::BlockNumberOverflow)?;
    if next_block != expected {
        return Err(SourceEngineError::ResumeCursorMismatch {
            expected,
            received: next_block,
        });
    }
    let Some(guard) = provider_checkpoint else {
        return rollback_guard_at(tip).map(Some);
    };
    if guard.block_number != tip.number {
        return Err(SourceEngineError::ResumeGuardMismatch("block_number"));
    }
    if guard.hash.as_ref() != tip.hash {
        return Err(SourceEngineError::ResumeGuardMismatch("hash"));
    }
    let timestamp = i64::try_from(tip.timestamp)
        .map_err(|_| SourceEngineError::BlockTimestampOverflow(tip.timestamp))?;
    if guard.timestamp != timestamp {
        return Err(SourceEngineError::ResumeGuardMismatch("timestamp"));
    }
    Ok(Some(guard))
}

type PreparedPage = (
    Delivery,
    Option<RollbackGuard>,
    CanonicalTracker,
    Option<ReorgDelta>,
);

#[derive(Clone, Copy)]
struct QueryLimits {
    max_blocks: usize,
    max_logs: usize,
}

impl QueryLimits {
    fn for_range(from_block: u64, to_block_excl: u64) -> Self {
        let span = to_block_excl.saturating_sub(from_block);
        let max_blocks = usize::try_from(span)
            .unwrap_or(MAX_BLOCKS_PER_QUERY)
            .clamp(1, MAX_BLOCKS_PER_QUERY);
        Self {
            max_blocks,
            max_logs: MAX_LOGS_PER_QUERY,
        }
    }

    fn shrink(&mut self) -> bool {
        if self.max_blocks == 1 && self.max_logs == 1 {
            return false;
        }
        self.max_blocks = self.max_blocks.div_ceil(2).max(1);
        self.max_logs = self.max_logs.div_ceil(2).max(1);
        true
    }
}

fn set_delivery_sequence(delivery: &mut Delivery, sequence: u64) -> Result<(), SourceEngineError> {
    delivery.sequence = sequence;
    delivery.delivery_token = sequence.to_be_bytes().to_vec();
    delivery
        .cursor
        .as_mut()
        .ok_or(SourceEngineError::MissingCursor)?
        .batch_sequence = sequence;
    Ok(())
}

/// External source request failure.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SourceError {
    /// HyperSync or its transport rejected the request.
    ///
    /// This internal diagnostic may retain provider-supplied sensitive detail.
    /// Service adapters must map it to a static public error before crossing a
    /// remote trust boundary.
    #[error("chain data source request failed: {0}")]
    Request(String),
    /// The provider did not return a usable archive height for an established chain.
    #[error("chain data source archive height is unavailable")]
    UnavailableHeight,
    /// A source operation exceeded the configured end-to-end request deadline.
    #[error("chain data source request timed out after {millis}ms")]
    RequestTimeout {
        /// Configured deadline in milliseconds.
        millis: u128,
    },
    /// A zero request deadline would make every source operation fail immediately.
    #[error("chain data source request timeout must be greater than zero")]
    InvalidRequestTimeout,
    /// A zero resident-session limit would reject every source session.
    #[error("managed source resident-session limit must be greater than zero")]
    InvalidSessionLimit,
    /// A zero reorg history cannot certify or resolve even a one-block fork.
    #[error("managed source reorg depth must be greater than zero")]
    InvalidReorgDepth,
    /// A provider delivery budget must fit inside the protocol-safe envelope.
    #[error("source delivery size limit {requested} must be within 1..={maximum} bytes")]
    InvalidDeliverySizeLimit {
        /// Requested source delivery budget.
        requested: usize,
        /// Largest protocol-safe source delivery budget.
        maximum: usize,
    },
    /// A hard local provider-response limit must be nonzero.
    #[error("source response limit for {resource} must be greater than zero")]
    InvalidResponseLimit {
        /// Resource whose limit was invalid.
        resource: &'static str,
    },
}

impl SourceError {
    /// Retain an external provider or transport failure for trusted local diagnostics.
    ///
    /// This constructor does not sanitize its argument. `ManagedEventProvider`
    /// deliberately converts the error to a static unavailable response before
    /// the event service exposes it over gRPC.
    pub fn request(message: impl Into<String>) -> Self {
        Self::Request(message.into())
    }

    /// Report that the provider has no usable nonzero archive height yet.
    pub const fn unavailable_height() -> Self {
        Self::UnavailableHeight
    }

    /// Report expiration of one configured end-to-end source deadline.
    pub fn request_timeout(timeout: std::time::Duration) -> Self {
        Self::RequestTimeout {
            millis: timeout.as_millis(),
        }
    }
}

/// Source engine could not produce or commit an ordered delivery.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SourceEngineError {
    /// Portable interests could not compile.
    #[error(transparent)]
    QueryPlan(#[from] QueryPlanError),
    /// The external data source failed.
    #[error(transparent)]
    Source(#[from] SourceError),
    /// Provider data was incomplete.
    #[error(transparent)]
    Normalize(#[from] NormalizeError),
    /// A provider page violated its requested bounded range or canonical identity.
    #[error(transparent)]
    InvalidPage(#[from] SourcePageError),
    /// Provider blocks could not extend canonical history.
    #[error(transparent)]
    Canonical(#[from] CanonicalError),
    /// A page did not advance its exclusive cursor.
    #[error("source page made no progress from {from_block}: next_block={next_block}")]
    NoProgress {
        /// Query start.
        from_block: u64,
        /// Cursor returned by the source.
        next_block: u64,
    },
    /// No batch is currently awaiting acknowledgement.
    #[error("no delivery is awaiting acknowledgement")]
    NoPendingDelivery,
    /// The acknowledgement does not identify the in-flight batch.
    #[error("delivery token does not match the in-flight batch")]
    DeliveryTokenMismatch,
    /// A delivered batch unexpectedly omitted its cursor.
    #[error("delivered batch is missing its cursor")]
    MissingCursor,
    /// Internal rollback recovery was requested without an acknowledged guard.
    #[error("rollback recovery is missing its previous continuity guard")]
    MissingRollbackGuard,
    /// A fork affected only owner catch-up history, for which v1 has no scoped rollback control.
    #[error(
        "owner catch-up fork before activation block {activation_block} cannot be emitted as a global reorg (common ancestor {common_ancestor}, old tip {old_tip}, new tip {new_tip})"
    )]
    OwnerCatchupReorg {
        /// First block eligible for global canonical delivery in this revision.
        activation_block: u64,
        /// Last block shared by the displaced and replacement owner-only branches.
        common_ancestor: u64,
        /// Last block on the previously delivered owner-only branch.
        old_tip: u64,
        /// Last block on the observed replacement branch.
        new_tip: u64,
    },
    /// A service-generated control cannot pass an in-flight source delivery.
    #[error("cannot acknowledge an external control while a source delivery is pending")]
    PendingDelivery,
    /// A service-generated control attempted to move sequence backwards.
    #[error("delivery sequence regressed from {committed} to {received}")]
    SequenceRegression {
        /// Last committed sequence.
        committed: u64,
        /// Proposed sequence.
        received: u64,
    },
    /// The durable cursor was not the successor of its canonical checkpoint.
    #[error("resume cursor is {received}, expected canonical successor {expected}")]
    ResumeCursorMismatch {
        /// Successor implied by the canonical tip or provider guard.
        expected: u64,
        /// Persisted exclusive cursor.
        received: u64,
    },
    /// Provider-native resume metadata conflicted with the normalized canonical tip.
    #[error("resume provider guard conflicts with canonical `{0}`")]
    ResumeGuardMismatch(&'static str),
    /// The provider scan cursor advanced beyond the successor of preserved global coverage.
    #[error(
        "resume scan cursor is {received}, beyond preserved coverage successor {coverage_successor}"
    )]
    CoverageCursorMismatch {
        /// Successor of the preserved global coverage head.
        coverage_successor: u64,
        /// Persisted provider scan cursor.
        received: u64,
    },
    /// A rescan contradicted the exact block identity of preserved global coverage.
    #[error("rescanned canonical identity conflicts with preserved coverage at block {number}")]
    CoverageBoundaryConflict {
        /// Preserved boundary block number.
        number: u64,
    },
    /// The durable delivery sequence cannot advance without reusing a token.
    #[error("delivery sequence is exhausted")]
    SequenceExhausted,
    /// A canonical cursor could not represent the successor of the common ancestor.
    #[error("canonical block number cannot advance beyond u64::MAX")]
    BlockNumberOverflow,
    /// A canonical timestamp could not fit HyperSync's signed rollback-guard representation.
    #[error("canonical block timestamp {0} does not fit in i64")]
    BlockTimestampOverflow(u64),
    /// The configured delivery budget was zero or exceeded the protocol-safe maximum.
    #[error("delivery size limit {requested} must be within 1..={maximum} bytes")]
    InvalidDeliverySizeLimit {
        /// Requested limit.
        requested: usize,
        /// Largest safe adapter payload.
        maximum: usize,
    },
    /// Even the smallest provider page could not fit the configured wire budget.
    #[error("encoded delivery is {encoded_len} bytes, exceeding the {limit}-byte limit")]
    DeliveryTooLarge {
        /// Encoded protobuf delivery length.
        encoded_len: usize,
        /// Configured adapter limit.
        limit: usize,
    },
    /// A decoded provider response exceeded the independent hard local admission policy.
    #[error(
        "provider response contains {observed} {resource}, exceeding the hard limit of {limit}"
    )]
    ResponseLimitExceeded {
        /// Bounded resource.
        resource: &'static str,
        /// Decoded amount observed.
        observed: usize,
        /// Configured hard limit.
        limit: usize,
    },
}
