//! Coordinated historical catch-up and low-latency live delivery.

mod codec;
mod transcript;

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use alloy_network::{Ethereum, Network};
use alloy_primitives::{B256, Keccak256, keccak256};
use evm_fork_cache::reactive::{
    BlockRef, CanonicalSequenceError, CanonicalSequenceMutation, CanonicalSequenceState,
    ChainControl, ChainStatus, DeliveryAudience, DeliveryScope, EventSubscriber, HandlerId,
    InputRef, InputSource, InterestOwnerSubscriber, ReactiveContext, ReactiveInput,
    ReactiveInputBatch, ReactiveInputDelivery, ReactiveInputIdentity, ReactiveInputKind,
    ReactiveInputRecord, ReactiveInterest, SubscriberBackfill, SubscriberCapabilities,
    SubscriberCapability, SubscriberCheckpoint, SubscriberDeliveryToken, SubscriberError,
    SubscriberNextBatch, SubscriberOperation, SubscriberPayloadCommitment,
    SubscriberResumePosition, normalize_and_validate_canonical_sequence_diagnostic,
};
use evm_fork_cache_event_protocol::v1::{BlockMode, portable_interest};
use serde::{Deserialize, Serialize};

use self::transcript::CanonicalHasher;
use crate::compile_portable_interests;

const TOKEN_MAGIC: &[u8; 8] = b"EFCHYB2\0";
#[cfg(test)]
const CHECKPOINT_MAGIC: &[u8; 8] = codec::CHECKPOINT_MAGIC_V5;
#[cfg(test)]
const CHECKPOINT_VERSION: u16 = codec::CHECKPOINT_VERSION_V5;
#[cfg(test)]
const MAX_CHECKPOINT_BYTES: usize = codec::MAX_CHECKPOINT_PAYLOAD_BYTES;
const MAX_HEADER_CANONICALIZATION_BYTES: usize = 256 * 1024;
const BATCH_ACCOUNTING_OVERHEAD: usize = 256;
const MIN_CONTROL_ACCOUNTING_BYTES: usize = 256;
/// Largest durable recent-input identity window accepted by the v5 codec.
pub const HYBRID_MAX_RECENT_INPUTS: usize = codec::MAX_RECENT_INPUTS;
/// Largest durable canonical-history window accepted by the v5 codec.
pub const HYBRID_MAX_CANONICAL_HISTORY: usize = codec::MAX_CANONICAL_HISTORY;
/// Largest configurable sum of owner-generation associations across retained
/// recent-input witnesses. The codec reserves additional space for the
/// topology maps themselves.
pub const HYBRID_MAX_RECENT_OWNER_ENTRIES: usize = 65_536;
/// Largest handler identifier accepted by the durable v5 wire contract.
pub const HYBRID_MAX_HANDLER_ID_BYTES: usize = codec::MAX_HANDLER_ID_BYTES;
/// Largest per-source delivery-token budget accepted by [`HybridConfig`].
///
/// The complete coordinator checkpoint still has one shared 16 MiB envelope,
/// so active topologies can require a materially smaller value once both child
/// cursors, canonical proof state, and one protected delivery are combined.
pub const HYBRID_MAX_SOURCE_DELIVERY_TOKEN_BYTES: usize = codec::MAX_CHECKPOINT_PAYLOAD_BYTES;
/// Largest per-source opaque checkpoint budget accepted by [`HybridConfig`].
///
/// This is a hard field ceiling, not a promise that two child checkpoints of
/// this size can coexist in one coordinator checkpoint. Active lifecycle
/// preflight proves the configured combination before mutating either child.
pub const HYBRID_MAX_SOURCE_CHECKPOINT_BYTES: usize = codec::MAX_CHECKPOINT_PAYLOAD_BYTES;
const LIFECYCLE_FINGERPRINT_DOMAIN: &[u8] = b"EFCHY-LIFECYCLE-V2";
const EMPTY_LIFECYCLE_BARRIER_DOMAIN: &[u8] = b"EFCHY-EMPTY-LIFECYCLE-BARRIER-V1";
const HEADER_WITNESS_DOMAIN: &[u8] = b"EFCHY-HEADER-CANONICAL-JSON-V1";
const EXCLUSIVE_TOPOLOGY_ERROR: &str = "hybrid base/unowned interests and owner-managed interests are mutually exclusive; after coverage, either mode may be cleared to effective-empty and allowed to reach Live, then base-to-owner activation requires replace_interest_owners_with_global_backfill; owner-to-base activation has no EventSubscriber global-backfill primitive and requires a fresh coordinator restored at an authoritative checkpoint";
static NEXT_EPOCH_NONCE: AtomicU64 = AtomicU64::new(1);

/// Tunables for [`HybridSubscriber`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct HybridConfig {
    /// Maximum number of live batches retained while the historical source
    /// catches up. Exceeding it fails closed instead of silently dropping head
    /// events.
    pub max_buffered_live_batches: usize,
    /// Maximum number of records retained across buffered live batches. The
    /// same value is also the per-batch ingress ceiling for either child, which
    /// is enforced before replay-witness encoding allocates secondary buffers.
    pub max_buffered_live_records: usize,
    /// Maximum accounted bytes retained across buffered live batches. The same
    /// value bounds one batch from either child before replay-witness encoding.
    /// Log data/topics and serialized network-generic header bodies are
    /// measured exactly; fixed-size record overhead is charged conservatively.
    /// A conservative control-count ceiling is derived from this value before
    /// walking attacker-controlled control vectors.
    pub max_buffered_live_bytes: usize,
    /// Maximum opaque delivery-token bytes accepted from either child.
    ///
    /// Every durable or restorable forwarded child token is checked against
    /// this bound. Active lifecycle preflight reserves this many bytes for both
    /// source positions and duplicates the committing source's token in the
    /// coordinator's last-commit proof using a worst-case RLP byte pattern. A
    /// separate synthetic-token probe reserves its fixed eight-byte sequence
    /// even when this configured forwarded-token bound is smaller. The default
    /// is 64 KiB.
    pub max_source_delivery_token_bytes: usize,
    /// Maximum opaque checkpoint bytes accepted from either child.
    ///
    /// Every durable or restorable child checkpoint is checked against this
    /// bound. Active lifecycle preflight reserves this many bytes for both
    /// sources using a worst-case RLP byte pattern, so choosing a value near the
    /// wire maximum will normally make an active topology fail closed under the
    /// shared checkpoint envelope. The default is 1 MiB.
    pub max_source_checkpoint_bytes: usize,
    /// Number of recently committed canonical input identities and complete
    /// payload witnesses retained for cross-source overlap suppression.
    ///
    /// This must cover the largest live/history overlap expected after a
    /// restart or recovery. If historical replay reaches an identity that has
    /// aged out, the coordinator requires a full resynchronization instead of
    /// guessing that equal positions imply equal payloads.
    pub recent_input_capacity: usize,
    /// Maximum total number of `(handler, generation)` associations retained
    /// across recent-input payload witnesses.
    ///
    /// This budget is independent of [`Self::recent_input_capacity`]. When
    /// fanout would exceed it, the oldest complete witnesses are evicted in a
    /// deterministic order while every identity emitted by the current batch
    /// remains protected. Consequently `recent_input_capacity` is an identity
    /// ceiling, not a guaranteed effective window under dense owner fanout.
    /// The same value bounds projected owner-routing work for one child batch
    /// before payload filtering or transcript construction. `AllExcept` charges
    /// both installed topology size and explicit exclusions.
    pub max_recent_owner_entries: usize,
    /// Number of acknowledged canonical block identities retained for overlap
    /// validation and shallow reorg reconciliation. Size this for the deepest
    /// supported source reorg and keep the consuming runtime's canonical
    /// journal at least as deep as every upstream recovery authority. Active
    /// lifecycle admission reserves this full capacity simultaneously in the
    /// coordinator and both source positions; unusually large combinations
    /// with the opaque cursor budgets may therefore fail before child mutation.
    pub canonical_history_capacity: usize,
}

impl Default for HybridConfig {
    fn default() -> Self {
        Self {
            max_buffered_live_batches: 2_048,
            max_buffered_live_records: 262_144,
            max_buffered_live_bytes: 64 * 1024 * 1024,
            max_source_delivery_token_bytes: 64 * 1024,
            max_source_checkpoint_bytes: 1024 * 1024,
            // The witness-bearing v5 journal remains below the hard 16 MiB
            // envelope for a representative single-owner entry at this size.
            recent_input_capacity: 32_768,
            max_recent_owner_entries: HYBRID_MAX_RECENT_OWNER_ENTRIES,
            canonical_history_capacity: 512,
        }
    }
}

/// Observable coordinator phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum HybridPhase {
    /// Live delivery is buffered while historical pages approach a fixed fence.
    CatchingUp,
    /// The acknowledged historical fence has committed and buffered live
    /// overlap is being drained.
    DrainingLive,
    /// WebSocket/live delivery owns the head.
    Live,
    /// The live source failed; historical delivery is filling the gap while a
    /// recovered live batch establishes a new cutover fence.
    Recovering,
    /// An invariant required for safe continuation could not be proven.
    /// Reconstruct the coordinator after repairing the source or restoring an
    /// authoritative checkpoint; poisoned instances never resume delivery.
    Poisoned,
}

/// Source encoded into coordinator delivery tokens.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum HybridSource {
    /// Durable historical/catch-up source (normally HyperSync).
    Historical,
    /// Low-latency head source (normally Alloy WebSocket).
    Live,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
enum HybridTokenKind {
    Forwarded,
    Synthetic,
}

impl HybridTokenKind {
    const fn tag(self) -> u8 {
        match self {
            Self::Forwarded => 1,
            Self::Synthetic => 2,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, SubscriberError> {
        match tag {
            1 => Ok(Self::Forwarded),
            2 => Ok(Self::Synthetic),
            _ => Err(SubscriberError::Provider(
                "invalid hybrid delivery-token kind".into(),
            )),
        }
    }
}

impl HybridSource {
    const fn tag(self) -> u8 {
        match self {
            Self::Historical => 1,
            Self::Live => 2,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, SubscriberError> {
        match tag {
            1 => Ok(Self::Historical),
            2 => Ok(Self::Live),
            _ => Err(SubscriberError::Provider(
                "invalid hybrid delivery-token source".into(),
            )),
        }
    }
}

/// One subscriber that uses a durable historical source for catch-up/recovery
/// and a low-latency source at the canonical head.
///
/// Registration is live-first so head events begin buffering before the
/// historical desired state commits. The first live canonical batch fixes the
/// catch-up fence at the preceding block. A historical page reaching that
/// fence does not cut over immediately: its delivery token must first be
/// acknowledged, which lets a caller place an atomic cache checkpoint between
/// ingestion and cutover. Buffered live overlap is then suppressed by block
/// boundary, stable input identity, and a complete retained payload witness.
/// If that historical proof is acknowledged before the first live item fixes
/// the fence, it is retained for the current lifecycle generation and consumed
/// when the late fence arrives.
/// A generic block header contributes its complete handler-visible body through
/// bounded, key-canonical JSON hashing and exact encoded-byte accounting, in
/// addition to the child batch's mandatory payload commitment.
/// Every source batch and record must carry the historical source's exact chain
/// id. Unsupported full-block and hydrated-pending representations fail closed.
/// Child payload commitments are preserved unchanged and included in the
/// coordinator's replay fingerprint even though outer token/checkpoint metadata
/// is rewritten.
/// A child envelope with no records, controls, or delivery token is invalid;
/// stream idleness is represented by a pending poll or `Ok(None)`, never by an
/// empty `Some(batch)`. A forwarded child token must accompany restorable
/// canonical coverage. Child token bytes are immutable identifiers within that
/// child's durable cursor namespace and must never be recycled after an
/// intervening token; Hybrid cannot retain an unbounded history of raw child
/// tokens to detect an `A -> B -> A` protocol violation locally.
///
/// Owner-only catch-up is accepted only at an exact retained canonical block;
/// it cannot advance the global head. Post-anchor registration catch-up must be
/// globally routed by the historical child so it enters the runtime's ordinary
/// rollback journal. Exact owner replacement without history is bootstrap-only
/// once no canonical coverage exists; post-coverage replacement requires the
/// global-backfill form. An uncertain destructive reset restores the previous
/// live topology, globally backfills the previous historical topology from the
/// retained canonical successor, and latches recovery until that gap is
/// certified. Ordinary base, owner, and bulk compensation has no equivalent
/// certification contract: if it changed a previously active live filter after
/// acknowledged coverage, successful rollback still poisons the coordinator
/// because an event may have crossed that mutation window. Adding a new owner
/// or activating a previously empty owner has no prior live filter to interrupt
/// and remains retryable after compensation.
///
/// Base/unowned interests and owner-managed interests are separate lifecycle
/// modes. Hybrid never combines two non-empty modes because its generic child
/// contract has no atomic mixed-topology rollback. After coverage, either mode
/// may be cleared to effective-empty and that lifecycle revision immediately
/// reaches [`HybridPhase::Live`] without polling either child. One
/// coordinator-local synthetic barrier first makes the revision durable: it
/// replays byte-identically until ACK, preserves existing child resume
/// checkpoints and canonical suffixes while clearing acknowledged raw tokens,
/// and never claims new source coverage. Only after that ACK does polling yield
/// idle `None`. A subsequent base-to-owner activation is safe only through
/// [`InterestOwnerSubscriber::replace_interest_owners_with_global_backfill`];
/// the inverse owner-to-base activation has no equivalent [`EventSubscriber`]
/// global-backfill operation and therefore requires a fresh coordinator restored
/// at an authoritative checkpoint.
///
/// The coordinator's [`SubscriberCapability::DurableReplay`] means it can
/// reconstruct a gap-free canonical position through the durable historical
/// child. It does not promise byte-identical replay of an ephemeral live
/// transport envelope. Restoring a non-empty base lifecycle requires
/// [`Self::prepare_restore_base_lifecycle`]. Every owner-managed topology uses
/// [`Self::prepare_restore_lifecycle`], including a topology whose installed
/// owners all have empty filters; owner identity is durable lifecycle state even
/// when there is no source traffic to poll. Both run before the core restore
/// hook so a fresh ephemeral live child is subscribed before historical
/// recovery begins. Preparation and installation normalize the same exact
/// restore candidate: runtime canonical history is merged first and only a
/// non-durable live cursor is discarded. Every active topology must then encode
/// four conservative one-record transitions: Historical and Live, each with a
/// forwarded or synthetic delivery token. The proof preserves exact lifecycle
/// state but saturates the coordinator and both source histories to
/// [`HybridConfig::canonical_history_capacity`], places every variable-width
/// scalar and opaque cursor at its maximum encoded width, and advances the real
/// commit path from `u64::MAX - 1` to `u64::MAX`. This fieldwise upper bound
/// covers monotonic progress, terminal-height reorg replacement, and divergent
/// retained source suffixes without selecting a speculative future ancestor.
/// It includes exact audience fanout, protected witness retention,
/// canonical/source progress, finality/certification, and both forwarded and
/// synthetic last-commit proofs. Effective-empty topologies reserve no
/// source-delivery space because they cannot poll either source until a
/// separately preflighted lifecycle activation. Unusually large history and
/// cursor budgets may therefore reject an active topology before child
/// mutation even when its current checkpoint is sparse.
pub struct HybridSubscriber<H, L, N: Network = Ethereum> {
    historical: H,
    live: L,
    chain_id: u64,
    config: HybridConfig,
    phase: HybridPhase,
    fence: Option<u64>,
    drain_through: Option<u64>,
    pending_cutover: Option<PendingCutover>,
    acknowledged_historical_through: Option<u64>,
    live_buffer: VecDeque<BufferedBatch<N>>,
    buffered_live_records: usize,
    buffered_live_bytes: usize,
    recent_inputs: HashMap<ReactiveInputIdentity, AudienceCoverage>,
    recent_order: VecDeque<ReactiveInputIdentity>,
    pending_inputs: HashMap<(HybridSource, HybridTokenKind, Vec<u8>), PendingCoordinatorCommit>,
    pending_output: Option<ReactiveInputBatch<N>>,
    pending_live_rollback: Option<LiveRollback<N>>,
    epoch: [u8; 16],
    next_synthetic_token: u64,
    lifecycle_generation: u64,
    owner_generations: BTreeMap<HandlerId, u64>,
    restored_source_replays: HashSet<HybridSource>,
    canonical_history: VecDeque<BlockRef>,
    coverage_head: Option<BlockRef>,
    safe_head: Option<BlockRef>,
    finalized_head: Option<BlockRef>,
    certified_historical: Option<CertifiedHistoricalCoverage>,
    recovery_anchor: Option<BlockRef>,
    historical_position: SourcePosition,
    live_position: SourcePosition,
    last_committed_token: Option<StoredCommittedToken>,
    pending_restore: Option<PendingRestore>,
    pending_restore_preparation: Option<PendingRestorePreparation<N>>,
    prepared_restore_position: Option<SubscriberResumePosition>,
    poisoned: Option<String>,
    base_interests: Vec<ReactiveInterest<N>>,
    owners: HashMap<HandlerId, Vec<ReactiveInterest<N>>>,
    lifecycle_intent: LifecycleIntent,
}

struct PendingCutover {
    historical_token: Vec<u8>,
    through: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct AudienceCoverage {
    base: bool,
    owners: BTreeMap<HandlerId, u64>,
    block: Option<BlockRef>,
    witness: Option<RecordWitness>,
}

#[derive(Clone, Debug)]
struct AudienceCommit {
    identity: ReactiveInputIdentity,
    audience: DeliveryAudience,
    block: Option<BlockRef>,
    witness: RecordWitness,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum WitnessLifecycle {
    Included,
    Safe,
    Finalized,
    Reorg,
    Pending,
}

/// Bounded, network-generic proof that an identity was previously committed.
/// Hash-addressed block/transaction representations use their consensus hash;
/// logs additionally commit to their complete body and required positions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct RecordWitness {
    payload_digest: [u8; 32],
    chain_id: u64,
    lifecycle: WitnessLifecycle,
    block: Option<BlockRef>,
    transaction_index: Option<u64>,
    log_index: Option<u64>,
    log_block_timestamp: Option<u64>,
}

#[derive(Clone, Debug)]
enum CanonicalMutation {
    Rewind(BlockRef),
    Reset,
    Advance(BlockRef),
}

#[derive(Clone, Debug)]
struct PendingCoordinatorCommit {
    audiences: Vec<AudienceCommit>,
    canonical: Vec<CanonicalMutation>,
    source: HybridSource,
    source_token: Option<Vec<u8>>,
    source_checkpoint: Option<Vec<u8>>,
    source_progress: Option<BlockRef>,
    source_observed_through: Option<BlockRef>,
    token_kind: HybridTokenKind,
    token_bytes: Vec<u8>,
    source_delivery_digest: [u8; 32],
    next_safe_head: Option<BlockRef>,
    next_finalized_head: Option<BlockRef>,
    next_canonical_history: Vec<BlockRef>,
    next_coverage_head: Option<BlockRef>,
}

/// Fully validated local state staged before an effective-empty topology is
/// installed in either child. Keeping this plan across the two child
/// mutations makes every coordinator-side failure happen before those
/// mutations; installation after both children commit is infallible.
struct PreparedEmptyLifecycleBarrier<N: Network> {
    base_state: HybridCheckpointV5,
    pending: PendingCoordinatorCommit,
    output: ReactiveInputBatch<N>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct SourcePosition {
    delivery_token: Option<Vec<u8>>,
    checkpoint: Option<Vec<u8>>,
    coverage_head: Option<BlockRef>,
    canonical_history: Vec<BlockRef>,
    delivery_digest: Option<[u8; 32]>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StoredRecentInput {
    identity: ReactiveInputIdentity,
    coverage: AudienceCoverage,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StoredCommittedToken {
    source: HybridSource,
    kind: HybridTokenKind,
    inner: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CertifiedHistoricalCoverage {
    lifecycle_generation: u64,
    through: BlockRef,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct HybridCheckpointV5 {
    chain_id: u64,
    epoch: [u8; 16],
    next_synthetic_token: u64,
    lifecycle_generation: u64,
    owner_generations: BTreeMap<HandlerId, u64>,
    lifecycle_intent: LifecycleIntent,
    recent_inputs: Vec<StoredRecentInput>,
    canonical_history: Vec<BlockRef>,
    coverage_head: Option<BlockRef>,
    safe_head: Option<BlockRef>,
    finalized_head: Option<BlockRef>,
    certified_historical: Option<CertifiedHistoricalCoverage>,
    historical_position: SourcePosition,
    live_position: SourcePosition,
    last_committed_token: Option<StoredCommittedToken>,
}

#[derive(Clone, Debug)]
struct PendingRestore {
    position: SubscriberResumePosition,
    state: HybridCheckpointV5,
    historical_resume: Option<SubscriberResumePosition>,
    live_resume: Option<SubscriberResumePosition>,
    historical_restored: bool,
    live_restored: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct LifecycleIntent {
    base: [u8; 32],
    owners: BTreeMap<HandlerId, [u8; 32]>,
}

impl LifecycleIntent {
    fn has_active_interests(&self) -> bool {
        self.base != empty_interest_fingerprint()
            || self
                .owners
                .values()
                .any(|fingerprint| *fingerprint != empty_interest_fingerprint())
    }

    fn requires_restore_preparation(&self) -> bool {
        self.has_active_interests() || !self.owners.is_empty()
    }
}

#[derive(Clone)]
struct PendingRestorePreparation<N: Network> {
    position: SubscriberResumePosition,
    intent: LifecycleIntent,
    base_interests: Vec<ReactiveInterest<N>>,
    owners: Vec<(HandlerId, Vec<ReactiveInterest<N>>)>,
    live_topology_installed: bool,
}

enum OwnerTopologyTransition {
    /// Exact replacement installs a new child namespace and then runs
    /// `reset_delivery_state`, which discards replay-only cursor state.
    DestructiveReset,
    /// Owner-scoped changes preserve source cursors and the recent journal.
    /// Removed owners alone are pruned from existing audience witnesses.
    Incremental { removed_owners: Vec<HandlerId> },
}

#[derive(Clone)]
enum LiveRollback<N: Network> {
    Base {
        previous: Vec<ReactiveInterest<N>>,
        gap_anchor: Option<BlockRef>,
    },
    Topology {
        base: Vec<ReactiveInterest<N>>,
        owners: Vec<(HandlerId, Vec<ReactiveInterest<N>>)>,
        recovery_anchor: Option<BlockRef>,
        gap_anchor: Option<BlockRef>,
    },
    Owner {
        owner: HandlerId,
        previous: Option<Vec<ReactiveInterest<N>>>,
        gap_anchor: Option<BlockRef>,
    },
    Bulk {
        previous: Vec<(HandlerId, Option<Vec<ReactiveInterest<N>>>)>,
        gap_anchor: Option<BlockRef>,
    },
    Recovery {
        anchor: BlockRef,
    },
}

impl<N: Network> LiveRollback<N> {
    const fn gap_anchor(&self) -> Option<BlockRef> {
        match self {
            Self::Base { gap_anchor, .. }
            | Self::Topology { gap_anchor, .. }
            | Self::Owner { gap_anchor, .. }
            | Self::Bulk { gap_anchor, .. } => *gap_anchor,
            Self::Recovery { .. } => None,
        }
    }

    const fn gap_scope(&self) -> &'static str {
        match self {
            Self::Base { .. } => "base subscription",
            Self::Topology { .. } => "owner topology",
            Self::Owner { .. } => "owner subscription",
            Self::Bulk { .. } => "bulk owner subscription",
            Self::Recovery { .. } => "topology recovery",
        }
    }
}

struct BufferedBatch<N: Network> {
    records: Vec<RoutedRecord<N>>,
    controls: Vec<ChainControl>,
    token: Option<SubscriberDeliveryToken>,
    checkpoint: Option<SubscriberCheckpoint>,
    payload_commitment: Option<SubscriberPayloadCommitment>,
    max_canonical_block: Option<u64>,
    source_progress: Option<BlockRef>,
    accounted_bytes: usize,
    delivery_digest: [u8; 32],
    canonical: Vec<CanonicalMutation>,
    next_safe_head: Option<BlockRef>,
    next_finalized_head: Option<BlockRef>,
    next_canonical_history: Vec<BlockRef>,
    next_coverage_head: Option<BlockRef>,
}

#[derive(Clone, Copy)]
struct HybridIngressLimits {
    max_records: usize,
    max_accounted_bytes: usize,
    max_delivery_token_bytes: usize,
    max_checkpoint_bytes: usize,
    max_projected_owner_associations: usize,
}

#[derive(Clone, Copy)]
struct CapacityProbeLimits {
    history_capacity: usize,
    max_delivery_token_bytes: usize,
    max_checkpoint_bytes: usize,
}

impl From<&HybridConfig> for HybridIngressLimits {
    fn from(config: &HybridConfig) -> Self {
        Self {
            max_records: config.max_buffered_live_records,
            max_accounted_bytes: config.max_buffered_live_bytes,
            max_delivery_token_bytes: config.max_source_delivery_token_bytes,
            max_checkpoint_bytes: config.max_source_checkpoint_bytes,
            max_projected_owner_associations: config.max_recent_owner_entries,
        }
    }
}

const fn maximum_ingress_control_count(max_accounted_bytes: usize) -> usize {
    max_accounted_bytes.saturating_sub(BATCH_ACCOUNTING_OVERHEAD) / MIN_CONTROL_ACCOUNTING_BYTES
}

#[cfg(test)]
fn maximum_admitted_canonical_advances(config: &HybridConfig) -> usize {
    maximum_ingress_control_count(config.max_buffered_live_bytes)
        .saturating_add(1)
        .min(config.canonical_history_capacity)
}

type RoutedRecord<N> = (ReactiveInputRecord<N>, DeliveryAudience, DeliveryScope);

impl<N: Network> BufferedBatch<N> {
    fn from_batch(
        batch: ReactiveInputBatch<N>,
        expected_chain_id: u64,
        limits: HybridIngressLimits,
        owner_generations: &BTreeMap<HandlerId, u64>,
    ) -> Result<Self, SubscriberError> {
        let HybridIngressLimits {
            max_records,
            max_accounted_bytes,
            max_delivery_token_bytes,
            max_checkpoint_bytes,
            max_projected_owner_associations,
        } = limits;
        let parts = batch.into_parts();
        if parts.chain_id != Some(expected_chain_id) {
            return Err(SubscriberError::Provider(format!(
                "hybrid child batch has chain {:?}; expected {expected_chain_id}",
                parts.chain_id
            )));
        }
        if parts.deliveries.is_empty()
            && parts.chain_controls.is_empty()
            && parts.delivery_token.is_none()
        {
            return Err(SubscriberError::Provider(
                "hybrid source emitted an empty child batch; return None for idle/keepalive state or emit an explicit chain control"
                    .into(),
            ));
        }
        if let Some(token) = parts.delivery_token.as_ref()
            && token.as_bytes().len() > max_delivery_token_bytes
        {
            return Err(SubscriberError::Provider(format!(
                "hybrid child delivery token exceeds the configured opaque cursor bound ({}/{max_delivery_token_bytes} bytes)",
                token.as_bytes().len()
            )));
        }
        if let Some(checkpoint) = parts.subscriber_checkpoint.as_ref()
            && checkpoint.as_bytes().len() > max_checkpoint_bytes
        {
            return Err(SubscriberError::Provider(format!(
                "hybrid child checkpoint exceeds the configured opaque cursor bound ({}/{max_checkpoint_bytes} bytes)",
                checkpoint.as_bytes().len()
            )));
        }
        if parts.deliveries.len() > max_records {
            return Err(SubscriberError::Provider(format!(
                "hybrid child batch exceeds configured per-batch ingress record bound ({}/{max_records})",
                parts.deliveries.len()
            )));
        }
        let max_controls = maximum_ingress_control_count(max_accounted_bytes);
        if parts.chain_controls.len() > max_controls {
            return Err(SubscriberError::Provider(format!(
                "hybrid child batch exceeds the derived per-batch ingress control bound ({}/{max_controls})",
                parts.chain_controls.len()
            )));
        }
        preflight_ingress_routing(
            &parts.deliveries,
            owner_generations,
            max_projected_owner_associations,
        )?;
        if parts.deliveries.iter().any(|delivery| {
            matches!(
                delivery.record().input,
                ReactiveInput::FullBlock(_) | ReactiveInput::PendingTx(_)
            )
        }) {
            return Err(SubscriberError::Unsupported(
                "hybrid delivery does not accept full blocks or hydrated pending transactions until their complete bodies can be verified generically",
            ));
        }
        if parts.payload_commitment.is_none()
            && parts
                .deliveries
                .iter()
                .any(|delivery| matches!(delivery.record().input, ReactiveInput::BlockHeader(_)))
        {
            return Err(SubscriberError::Unsupported(
                "hybrid block-header delivery requires a subscriber payload commitment covering the exact canonical wire body",
            ));
        }
        let mut accounted_bytes = BATCH_ACCOUNTING_OVERHEAD;
        for delivery in &parts.deliveries {
            let remaining = max_accounted_bytes.saturating_sub(accounted_bytes);
            let record_bytes =
                accounted_record_bytes(delivery.record(), delivery.audience(), remaining)?;
            accounted_bytes = accounted_bytes.checked_add(record_bytes).ok_or_else(|| {
                SubscriberError::Provider(
                    "hybrid child batch ingress byte accounting overflowed".into(),
                )
            })?;
            if accounted_bytes > max_accounted_bytes {
                return Err(SubscriberError::Provider(format!(
                    "hybrid child batch exceeds configured per-batch ingress byte bound ({accounted_bytes}/{max_accounted_bytes})"
                )));
            }
        }
        for control in &parts.chain_controls {
            accounted_bytes = accounted_bytes
                .checked_add(accounted_control_bytes(control))
                .ok_or_else(|| {
                    SubscriberError::Provider(
                        "hybrid child batch ingress byte accounting overflowed".into(),
                    )
                })?;
            if accounted_bytes > max_accounted_bytes {
                return Err(SubscriberError::Provider(format!(
                    "hybrid child batch exceeds configured per-batch ingress byte bound ({accounted_bytes}/{max_accounted_bytes})"
                )));
            }
        }
        if let Some(token) = parts.delivery_token.as_ref() {
            accounted_bytes = accounted_bytes
                .checked_add(token.as_bytes().len())
                .ok_or_else(|| {
                    SubscriberError::Provider(
                        "hybrid child batch ingress byte accounting overflowed".into(),
                    )
                })?;
        }
        if let Some(checkpoint) = parts.subscriber_checkpoint.as_ref() {
            accounted_bytes = accounted_bytes
                .checked_add(checkpoint.as_bytes().len())
                .ok_or_else(|| {
                    SubscriberError::Provider(
                        "hybrid child batch ingress byte accounting overflowed".into(),
                    )
                })?;
        }
        if parts.payload_commitment.is_some() {
            accounted_bytes = accounted_bytes.checked_add(32).ok_or_else(|| {
                SubscriberError::Provider(
                    "hybrid child batch ingress byte accounting overflowed".into(),
                )
            })?;
        }
        if accounted_bytes > max_accounted_bytes {
            return Err(SubscriberError::Provider(format!(
                "hybrid child batch exceeds configured per-batch ingress byte bound ({accounted_bytes}/{max_accounted_bytes})"
            )));
        }
        let token = parts.delivery_token;
        let checkpoint = parts.subscriber_checkpoint;
        let payload_commitment = parts.payload_commitment;
        let controls = parts.chain_controls;
        let records = parts
            .deliveries
            .into_iter()
            .map(ReactiveInputDelivery::into_parts)
            .collect::<Vec<_>>();
        for (record, audience, scope) in &records {
            validated_record(record, expected_chain_id)?;
            debug_assert!(validate_routing(audience, *scope).is_ok());
        }
        validate_chain_controls(&controls)?;
        let delivery_digest = source_delivery_digest(
            &records,
            &controls,
            checkpoint.as_ref(),
            payload_commitment.as_ref(),
            expected_chain_id,
        )?;
        let max_canonical_block = records
            .iter()
            .filter(|(_, _, scope)| scope_advances_canonical(*scope))
            .filter_map(|(record, _, _)| coverage_record_block_number(record))
            .chain(controls.iter().filter_map(coverage_control_block_number))
            .max();
        Ok(Self {
            records,
            controls,
            token,
            checkpoint,
            payload_commitment,
            max_canonical_block,
            source_progress: None,
            accounted_bytes,
            delivery_digest,
            canonical: Vec::new(),
            next_safe_head: None,
            next_finalized_head: None,
            next_canonical_history: Vec::new(),
            next_coverage_head: None,
        })
    }
}

#[derive(Debug)]
enum CanonicalPlanError {
    /// The live source observed a branch transition that cannot be proven from
    /// the retained suffix alone. The durable historical source must recover
    /// the gap before any live payload is exposed.
    NeedsHistoricalRecovery(String),
    /// The source batch is intrinsically invalid and cannot become safe by
    /// consulting more history.
    Invalid(SubscriberError),
}

impl CanonicalPlanError {
    fn into_subscriber_error(self) -> SubscriberError {
        match self {
            Self::NeedsHistoricalRecovery(message) => SubscriberError::Provider(message),
            Self::Invalid(error) => error,
        }
    }
}

impl<H, L, N> HybridSubscriber<H, L, N>
where
    H: EventSubscriber<N>,
    L: EventSubscriber<N>,
    N: Network,
{
    /// Construct a coordinator. `historical` should provide durable,
    /// acknowledgeable batches; `live` should prioritize head latency.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriberError::InvalidConfig`] when a configured bound is
    /// zero or exceeds the V5 wire limit, the historical source does not expose
    /// an authoritative chain and the required durable-backfill/barrier
    /// capabilities, the live source is not live-capable, or the two resolved
    /// chain ids differ. Lifecycle fingerprint construction failures are also
    /// returned without mutating either source.
    pub fn new(historical: H, live: L, config: HybridConfig) -> Result<Self, SubscriberError>
    where
        H: EventSubscriber<N>,
        L: EventSubscriber<N>,
    {
        if config.max_buffered_live_batches == 0 {
            return Err(SubscriberError::InvalidConfig(
                "hybrid live buffer capacity must be non-zero",
            ));
        }
        if config.recent_input_capacity == 0 {
            return Err(SubscriberError::InvalidConfig(
                "hybrid deduplication capacity must be non-zero",
            ));
        }
        if config.recent_input_capacity > HYBRID_MAX_RECENT_INPUTS {
            return Err(SubscriberError::InvalidConfig(
                "hybrid deduplication capacity exceeds the v5 durable checkpoint limit",
            ));
        }
        if config.max_recent_owner_entries > HYBRID_MAX_RECENT_OWNER_ENTRIES {
            return Err(SubscriberError::InvalidConfig(
                "hybrid recent owner-entry budget exceeds the v5 durable checkpoint limit",
            ));
        }
        if config.max_buffered_live_records == 0 {
            return Err(SubscriberError::InvalidConfig(
                "hybrid live record capacity must be non-zero",
            ));
        }
        if config.max_buffered_live_bytes == 0 {
            return Err(SubscriberError::InvalidConfig(
                "hybrid live byte capacity must be non-zero",
            ));
        }
        if config.max_source_delivery_token_bytes == 0 {
            return Err(SubscriberError::InvalidConfig(
                "hybrid source delivery-token byte budget must be non-zero",
            ));
        }
        if config.max_source_delivery_token_bytes > HYBRID_MAX_SOURCE_DELIVERY_TOKEN_BYTES {
            return Err(SubscriberError::InvalidConfig(
                "hybrid source delivery-token byte budget exceeds the v5 durable checkpoint limit",
            ));
        }
        if config.max_source_checkpoint_bytes == 0 {
            return Err(SubscriberError::InvalidConfig(
                "hybrid source checkpoint byte budget must be non-zero",
            ));
        }
        if config.max_source_checkpoint_bytes > HYBRID_MAX_SOURCE_CHECKPOINT_BYTES {
            return Err(SubscriberError::InvalidConfig(
                "hybrid source checkpoint byte budget exceeds the v5 durable checkpoint limit",
            ));
        }
        if config.canonical_history_capacity == 0 {
            return Err(SubscriberError::InvalidConfig(
                "hybrid canonical history capacity must be non-zero",
            ));
        }
        if config.canonical_history_capacity > HYBRID_MAX_CANONICAL_HISTORY {
            return Err(SubscriberError::InvalidConfig(
                "hybrid canonical history capacity exceeds the v5 durable checkpoint limit",
            ));
        }
        let chain_id = historical.chain_id().ok_or(SubscriberError::InvalidConfig(
            "hybrid historical source must expose one authoritative chain id",
        ))?;
        if live
            .chain_id()
            .is_some_and(|live_chain_id| live_chain_id != chain_id)
        {
            return Err(SubscriberError::InvalidConfig(
                "hybrid historical and live sources must target the same chain",
            ));
        }
        let historical_capabilities = historical.capabilities();
        for required in [
            SubscriberCapability::HistoricalBackfill,
            SubscriberCapability::DurableReplay,
            SubscriberCapability::Barriers,
        ] {
            if !historical_capabilities.supports(required) {
                return Err(SubscriberError::InvalidConfig(
                    "hybrid historical source must provide durable backfill and explicit coverage barriers",
                ));
            }
        }
        if !live.capabilities().supports(SubscriberCapability::Live) {
            return Err(SubscriberError::InvalidConfig(
                "hybrid live source must advertise live canonical delivery",
            ));
        }
        let lifecycle_intent = lifecycle_intent::<N>(&[], &HashMap::new())?;
        Ok(Self {
            historical,
            live,
            chain_id,
            config,
            phase: HybridPhase::Live,
            fence: None,
            drain_through: None,
            pending_cutover: None,
            acknowledged_historical_through: None,
            live_buffer: VecDeque::new(),
            buffered_live_records: 0,
            buffered_live_bytes: 0,
            recent_inputs: HashMap::new(),
            recent_order: VecDeque::new(),
            pending_inputs: HashMap::new(),
            pending_output: None,
            pending_live_rollback: None,
            epoch: fresh_epoch(),
            next_synthetic_token: 1,
            lifecycle_generation: 1,
            owner_generations: BTreeMap::new(),
            restored_source_replays: HashSet::new(),
            canonical_history: VecDeque::new(),
            coverage_head: None,
            safe_head: None,
            finalized_head: None,
            certified_historical: None,
            recovery_anchor: None,
            historical_position: SourcePosition::default(),
            live_position: SourcePosition::default(),
            last_committed_token: None,
            pending_restore: None,
            pending_restore_preparation: None,
            prepared_restore_position: None,
            poisoned: None,
            base_interests: Vec::new(),
            owners: HashMap::new(),
            lifecycle_intent,
        })
    }

    /// Current delivery phase.
    pub const fn phase(&self) -> HybridPhase {
        self.phase
    }

    fn has_active_interests(&self) -> bool {
        !self.base_interests.is_empty()
            || self.owners.values().any(|interests| !interests.is_empty())
    }

    fn validate_checkpoint_config_limits(
        &self,
        state: &HybridCheckpointV5,
    ) -> Result<(), SubscriberError> {
        self.validate_checkpoint_config_limits_for_restore(state, true)
    }

    fn validate_checkpoint_config_limits_for_restore(
        &self,
        state: &HybridCheckpointV5,
        retain_live_cursor: bool,
    ) -> Result<(), SubscriberError> {
        if state.recent_inputs.len() > self.config.recent_input_capacity
            || recent_owner_entry_count(&state.recent_inputs)?
                > self.config.max_recent_owner_entries
            || state.canonical_history.len() > self.config.canonical_history_capacity
            || state.historical_position.canonical_history.len()
                > self.config.canonical_history_capacity
            || state.live_position.canonical_history.len() > self.config.canonical_history_capacity
        {
            return Err(SubscriberError::Provider(
                "hybrid checkpoint exceeds this coordinator's configured durable verification budgets"
                    .into(),
            ));
        }
        for (label, position) in [
            ("historical", Some(&state.historical_position)),
            ("live", retain_live_cursor.then_some(&state.live_position)),
        ] {
            let Some(position) = position else {
                continue;
            };
            if position
                .delivery_token
                .as_ref()
                .is_some_and(|token| token.len() > self.config.max_source_delivery_token_bytes)
            {
                return Err(SubscriberError::Provider(format!(
                    "hybrid {label} source delivery token exceeds this coordinator's configured opaque cursor bound"
                )));
            }
            if position.checkpoint.as_ref().is_some_and(|checkpoint| {
                checkpoint.len() > self.config.max_source_checkpoint_bytes
            }) {
                return Err(SubscriberError::Provider(format!(
                    "hybrid {label} source checkpoint exceeds this coordinator's configured opaque cursor bound"
                )));
            }
        }
        if let Some(committed) = state.last_committed_token.as_ref()
            && committed.kind == HybridTokenKind::Forwarded
            && (committed.source != HybridSource::Live || retain_live_cursor)
            && committed.inner.len() > self.config.max_source_delivery_token_bytes
        {
            return Err(SubscriberError::Provider(
                "hybrid last committed forwarded token exceeds this coordinator's configured opaque cursor bound"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Validate every caller-controlled part of a restore position without
    /// mutating either child. The returned history is enriched with compatible
    /// checkpoint metadata and is ready to install. The asynchronous
    /// preparation methods invoke this before installing an ephemeral live
    /// topology, while `restore_position` uses the returned history directly so
    /// validation and activation cannot drift into separate implementations.
    fn validate_restore_position_preflight(
        &self,
        position: &SubscriberResumePosition,
        state: &HybridCheckpointV5,
        live_is_durable: bool,
    ) -> Result<Vec<BlockRef>, SubscriberError> {
        if position.chain_id != self.chain_id || state.chain_id != self.chain_id {
            return Err(SubscriberError::Provider(format!(
                "hybrid restore targets the wrong chain; expected {}",
                self.chain_id
            )));
        }
        self.validate_checkpoint_config_limits_for_restore(state, live_is_durable)?;

        let token = position.delivery_token.as_ref().ok_or_else(|| {
            SubscriberError::Provider(
                "hybrid restore checkpoint is missing its matching delivery token".into(),
            )
        })?;
        let (token_epoch, source, kind, inner) = unwrap_token(token.clone())?;
        if token_epoch != state.epoch {
            return Err(SubscriberError::Provider(
                "hybrid checkpoint and delivery-token epochs differ".into(),
            ));
        }
        let committed = state.last_committed_token.as_ref().ok_or_else(|| {
            SubscriberError::Provider(
                "hybrid checkpoint is missing its last committed outer token".into(),
            )
        })?;
        if committed.source != source
            || committed.kind != kind
            || committed.inner.as_slice() != inner.as_bytes()
        {
            return Err(SubscriberError::Provider(
                "hybrid checkpoint does not match its last committed outer delivery token".into(),
            ));
        }
        match kind {
            HybridTokenKind::Synthetic => {
                let sequence = u64::from_be_bytes(inner.as_bytes().try_into().map_err(|_| {
                    SubscriberError::Provider(
                        "hybrid synthetic token has an invalid sequence width".into(),
                    )
                })?);
                if state.next_synthetic_token
                    != sequence.checked_add(1).ok_or_else(|| {
                        SubscriberError::Provider(
                            "restored hybrid synthetic token exhausted its sequence space".into(),
                        )
                    })?
                {
                    return Err(SubscriberError::Provider(
                        "hybrid checkpoint does not match its last synthetic delivery token".into(),
                    ));
                }
            }
            HybridTokenKind::Forwarded => {
                let source_position = match source {
                    HybridSource::Historical => &state.historical_position,
                    HybridSource::Live => &state.live_position,
                };
                if source_position.delivery_token.as_deref() != Some(inner.as_bytes()) {
                    return Err(SubscriberError::Provider(
                        "hybrid checkpoint does not match its last forwarded delivery token".into(),
                    ));
                }
            }
        }

        if let Some(checkpoint_head) = state.coverage_head.as_ref()
            && compatible_block_ref(checkpoint_head, &position.coverage_head).is_err()
        {
            return Err(SubscriberError::Provider(format!(
                "hybrid checkpoint coverage {} does not match restored runtime coverage {}",
                checkpoint_head.number, position.coverage_head.number
            )));
        }
        let mut runtime_history = position.canonical_history.clone();
        if runtime_history
            .last()
            .is_none_or(|last| last.number < position.coverage_head.number)
        {
            runtime_history.push(position.coverage_head);
        }
        validate_checkpoint_history(
            "runtime resume",
            &runtime_history,
            Some(&position.coverage_head),
        )?;
        for runtime_block in &mut runtime_history {
            if let Some(checkpoint_block) = state
                .canonical_history
                .iter()
                .find(|block| block.number == runtime_block.number)
            {
                *runtime_block = compatible_block_ref(checkpoint_block, runtime_block).map_err(
                    |_| {
                        SubscriberError::Provider(format!(
                            "runtime canonical history conflicts with the hybrid checkpoint at block {}",
                            runtime_block.number
                        ))
                    },
                )?;
            }
        }
        for entry in &state.recent_inputs {
            if let Some(block) = entry.coverage.block.as_ref()
                && let Some(runtime_block) = runtime_history
                    .iter()
                    .find(|runtime| runtime.number == block.number)
                && compatible_block_ref(runtime_block, block).is_err()
            {
                return Err(SubscriberError::Provider(format!(
                    "runtime canonical history conflicts with hybrid input coverage at block {}",
                    block.number
                )));
            }
        }
        Ok(runtime_history)
    }

    /// Apply every deterministic restore transformation except epoch rotation.
    ///
    /// Preparation and installation both use this path so capacity is proven
    /// against the state that will actually be staged. Durable historical
    /// state and both source canonical suffixes remain opaque. A non-durable
    /// live source alone loses the token/checkpoint namespace it cannot
    /// authoritatively replay; the caller rotates the fixed-size epoch only
    /// after all fallible size checks have succeeded.
    fn normalize_restore_candidate(
        &self,
        position: &SubscriberResumePosition,
        state: &mut HybridCheckpointV5,
        live_is_durable: bool,
    ) -> Result<bool, SubscriberError> {
        let mut runtime_history =
            self.validate_restore_position_preflight(position, state, live_is_durable)?;
        truncate_front_to(&mut runtime_history, self.config.canonical_history_capacity);
        state.coverage_head = Some(match state.coverage_head.as_ref() {
            Some(checkpoint_head) => {
                compatible_block_ref(checkpoint_head, &position.coverage_head)?
            }
            None => position.coverage_head,
        });
        state.canonical_history = runtime_history;

        let mut rotate_epoch = false;
        if !live_is_durable {
            let retain_empty_barrier_ack = is_committed_empty_lifecycle_barrier(state);
            state.live_position.delivery_token = None;
            state.live_position.checkpoint = None;
            if !retain_empty_barrier_ack {
                state.live_position.delivery_digest = None;
                state.last_committed_token = None;
                rotate_epoch = true;
            }
        }
        self.validate_checkpoint_config_limits(state)?;
        validate_checkpoint_state(state)?;
        Ok(rotate_epoch)
    }

    fn preflight_owner_topology(
        &self,
        next_owners: &HashMap<HandlerId, Vec<ReactiveInterest<N>>>,
        next_intent: &LifecycleIntent,
        generation: u64,
        next_owner_generations: &BTreeMap<HandlerId, u64>,
        transition: OwnerTopologyTransition,
    ) -> Result<(), SubscriberError> {
        validate_handler_ids(next_owners.keys())?;
        if !next_intent.has_active_interests() {
            // The exact synthetic-barrier candidate is preflighted separately
            // before an effective-empty transition mutates either child.
            return Ok(());
        }
        let mut state = self.checkpoint_state();
        state.lifecycle_generation = generation;
        state.owner_generations = next_owner_generations.clone();
        state.lifecycle_intent = next_intent.clone();
        state.certified_historical = None;
        match transition {
            OwnerTopologyTransition::DestructiveReset => {
                // `reset_delivery_state` rotates the fixed-width epoch and
                // restarts the synthetic sequence at one after commit.
                state.next_synthetic_token = 1;
                state.recent_inputs.clear();
                state.historical_position.delivery_token = None;
                state.historical_position.checkpoint = None;
                state.historical_position.delivery_digest = None;
                state.live_position.delivery_token = None;
                state.live_position.checkpoint = None;
                state.live_position.delivery_digest = None;
                state.last_committed_token = None;
            }
            OwnerTopologyTransition::Incremental { removed_owners } => {
                for entry in &mut state.recent_inputs {
                    for owner in &removed_owners {
                        entry.coverage.owners.remove(owner);
                    }
                }
            }
        }
        self.preflight_maximum_canonical_log_delivery(&state, "hybrid owner topology")
    }

    fn preflight_base_topology(
        &self,
        next_intent: &LifecycleIntent,
        generation: u64,
    ) -> Result<(), SubscriberError> {
        if !next_intent.has_active_interests() {
            // The exact synthetic-barrier candidate is preflighted separately
            // before an effective-empty transition mutates either child.
            return Ok(());
        }
        let mut state = self.checkpoint_state();
        state.next_synthetic_token = 1;
        state.lifecycle_generation = generation;
        state.owner_generations.clear();
        state.lifecycle_intent = next_intent.clone();
        state.recent_inputs.clear();
        state.certified_historical = None;
        state.historical_position.delivery_token = None;
        state.historical_position.checkpoint = None;
        state.historical_position.delivery_digest = None;
        state.live_position.delivery_token = None;
        state.live_position.checkpoint = None;
        state.live_position.delivery_digest = None;
        state.last_committed_token = None;
        self.preflight_maximum_canonical_log_delivery(&state, "hybrid base topology")
    }

    /// Prove that an active, normalized restore candidate can absorb one
    /// supported post-restore delivery before any child is mutated.
    ///
    /// The clone retains every field present in the exact normalized install
    /// candidate, including durable historical/durable-live cursors, source
    /// histories, finality/certification proofs, and lifecycle maps. Only the
    /// simulated journal may evict its oldest witnesses through the ordinary
    /// deterministic fit routine; the candidate installed later remains
    /// untouched.
    fn preflight_restore_delivery_capacity(
        &self,
        candidate: &HybridCheckpointV5,
    ) -> Result<(), SubscriberError> {
        if !candidate.lifecycle_intent.has_active_interests() {
            return Ok(());
        }
        self.preflight_maximum_canonical_log_delivery(candidate, "hybrid restored checkpoint")
    }

    /// Prove one-record headroom from every source that may next own delivery.
    ///
    /// Each independent simulation preserves the exact candidate's chain,
    /// epoch, lifecycle generation, and lifecycle/owner maps while placing the
    /// mutable synthetic sequence at its maximum encoded width. It removes only
    /// old recent witnesses that the ordinary fit path may evict, then installs
    /// a valid, fully populated canonical suffix at the configured capacity in
    /// the coordinator and both source positions.
    /// Both opaque cursor pairs and historical certification are present at
    /// their maximum encoded widths. Starting from that validated saturated
    /// state, the simulation drives one maximum canonical-log record through
    /// [`apply_commit_to_checkpoint`] to exercise real fanout, protected
    /// witness retention, source progress, finality/certification, cursor
    /// replacement, and both forwarded and synthetic
    /// `last_committed_token` representations.
    ///
    /// Saturating all three histories is a fieldwise upper bound independent
    /// of the candidate's current height or source divergence. It therefore
    /// covers forward progress, terminal-height replacement after a reorg, and
    /// arbitrary retained source suffix shapes without predicting one future
    /// reorg ancestor.
    fn preflight_maximum_canonical_log_delivery(
        &self,
        candidate: &HybridCheckpointV5,
        context: &str,
    ) -> Result<(), SubscriberError> {
        for source in [HybridSource::Historical, HybridSource::Live] {
            let source_label = match source {
                HybridSource::Historical => "historical",
                HybridSource::Live => "live",
            };
            for kind in [HybridTokenKind::Forwarded, HybridTokenKind::Synthetic] {
                let kind_label = match kind {
                    HybridTokenKind::Forwarded => "forwarded-token",
                    HybridTokenKind::Synthetic => "synthetic-token",
                };
                self.encode_maximum_canonical_log_delivery_variant(candidate, source, kind)
                    .map_err(|error| {
                        SubscriberError::Provider(format!(
                            "{context} cannot retain one protected delivery witness for the maximum canonical-log delivery ({kind_label}) from the {source_label} source: {error}"
                        ))
                    })?;
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn encode_maximum_canonical_log_delivery(
        &self,
        candidate: &HybridCheckpointV5,
        source: HybridSource,
    ) -> Result<Vec<u8>, SubscriberError> {
        self.encode_maximum_canonical_log_delivery_variant(
            candidate,
            source,
            HybridTokenKind::Forwarded,
        )
    }

    fn encode_maximum_canonical_log_delivery_variant(
        &self,
        candidate: &HybridCheckpointV5,
        source: HybridSource,
        kind: HybridTokenKind,
    ) -> Result<Vec<u8>, SubscriberError> {
        let mut simulated = saturated_capacity_probe_state(candidate, &self.config)?;
        if kind == HybridTokenKind::Synthetic {
            simulated.next_synthetic_token = u64::MAX - 1;
        }
        self.validate_checkpoint_config_limits(&simulated)?;
        validate_checkpoint_state(&simulated)?;
        let mut commit = maximum_canonical_log_commit(
            &simulated,
            source,
            self.config.canonical_history_capacity,
            1,
            self.config.max_source_delivery_token_bytes,
            self.config.max_source_checkpoint_bytes,
        )?;
        if kind == HybridTokenKind::Synthetic {
            let sequence = simulated.next_synthetic_token;
            simulated.next_synthetic_token = simulated
                .next_synthetic_token
                .checked_add(1)
                .ok_or_else(|| {
                    SubscriberError::Provider(
                        "hybrid synthetic capacity-probe token space exhausted".into(),
                    )
                })?;
            commit.source_token = None;
            commit.token_kind = HybridTokenKind::Synthetic;
            commit.token_bytes = sequence.to_be_bytes().to_vec();
        }
        let probe_block = commit.next_coverage_head.ok_or_else(|| {
            SubscriberError::Provider(
                "hybrid saturated canonical-log capacity probe did not advance coverage".into(),
            )
        })?;
        reserve_maximum_source_cursors(&mut simulated, &self.config, source, probe_block)?;
        apply_commit_to_checkpoint(
            &mut simulated,
            &commit,
            self.config.canonical_history_capacity,
        )?;
        fit_checkpoint_to_durable_limits(
            &mut simulated,
            1,
            self.config.recent_input_capacity,
            self.config.max_recent_owner_entries,
        )?;
        self.validate_checkpoint_config_limits(&simulated)?;
        validate_checkpoint_state(&simulated)?;
        encode_hybrid_checkpoint(&simulated)
    }

    #[cfg(test)]
    fn encode_canonical_log_delivery_capacity_probe(
        &self,
        candidate: &HybridCheckpointV5,
        source: HybridSource,
        maximum_advances: usize,
    ) -> Result<Vec<u8>, SubscriberError> {
        let mut simulated = candidate.clone();
        let commit = maximum_canonical_log_commit(
            &simulated,
            source,
            self.config.canonical_history_capacity,
            maximum_advances,
            self.config.max_source_delivery_token_bytes,
            self.config.max_source_checkpoint_bytes,
        )?;
        let probe_block = commit.next_coverage_head.ok_or_else(|| {
            SubscriberError::Provider(
                "hybrid maximum canonical-log capacity probe did not advance coverage".into(),
            )
        })?;
        reserve_maximum_source_cursors(&mut simulated, &self.config, source, probe_block)?;
        apply_commit_to_checkpoint(
            &mut simulated,
            &commit,
            self.config.canonical_history_capacity,
        )?;
        fit_checkpoint_to_durable_limits(
            &mut simulated,
            1,
            self.config.recent_input_capacity,
            self.config.max_recent_owner_entries,
        )?;
        self.validate_checkpoint_config_limits(&simulated)?;
        validate_checkpoint_state(&simulated)?;
        encode_hybrid_checkpoint(&simulated)
    }

    /// Current historical cutover fence, when a live canonical batch has fixed one.
    pub const fn fence(&self) -> Option<u64> {
        self.fence
    }

    /// Number of live batches buffered behind historical catch-up.
    pub fn buffered_live_batches(&self) -> usize {
        self.live_buffer.len()
    }

    /// Number of records currently retained behind historical catch-up.
    pub const fn buffered_live_records(&self) -> usize {
        self.buffered_live_records
    }

    /// Accounted bytes currently retained behind historical catch-up.
    pub const fn buffered_live_bytes(&self) -> usize {
        self.buffered_live_bytes
    }

    /// Irrecoverable lifecycle divergence, when present.
    pub fn poison_reason(&self) -> Option<&str> {
        self.poisoned.as_deref()
    }

    /// Borrow the historical source.
    pub const fn historical(&self) -> &H {
        &self.historical
    }

    /// Borrow the live source.
    pub const fn live(&self) -> &L {
        &self.live
    }

    /// Split the coordinator back into its sources.
    pub fn into_sources(self) -> (H, L) {
        (self.historical, self.live)
    }

    /// Recreate local lifecycle mirrors and, when needed, an ephemeral live
    /// child's subscriptions before restoring a durable Hybrid checkpoint.
    ///
    /// The historical child is deliberately not mutated: its durable cursor
    /// remains the authority that proves the checkpoint's lifecycle revision.
    /// The supplied base/owner interests are reduced to the same portable
    /// provider topology committed by the checkpoint and must match exactly.
    /// Once this operation succeeds, callers must pass the same `position` to
    /// [`EventSubscriber::restore_position`] (normally through
    /// `ReactiveEngine::resume_from_durable_checkpoint`) before polling, changing
    /// interests, or acknowledging delivery.
    ///
    /// This is required for every owner-managed restored lifecycle, even when
    /// every installed owner has an empty filter. A durable live child is not
    /// mutated and restores its own cursor; an ephemeral live child is
    /// registered here before historical recovery starts. The operation is
    /// sized against the exact normalized install candidate, including the
    /// caller's enriched runtime history and the cursor fields restore really
    /// preserves. Active topologies must encode the fully saturated configured
    /// durable state plus one real canonical-log commit for each eligible
    /// source and for both forwarded and synthetic token forms. All three
    /// histories, both child cursor budgets, finality/certification, the
    /// synthetic counter, exact fanout, and the protected witness are included;
    /// effective-empty owner topology still requires preparation but reserves
    /// no source-delivery space until its next activation preflight.
    /// The operation is cancellation-safe: retry the exact same arguments to
    /// finish an interrupted live registration.
    /// `base_interests` and `owners` cannot both be non-empty; mixed topology
    /// has no atomic generic rollback primitive.
    ///
    /// Base-only callers should prefer [`Self::prepare_restore_base_lifecycle`],
    /// which deliberately requires only [`EventSubscriber`] children. This
    /// combined compatibility entry point requires owner-capable children
    /// because a non-empty `owners` argument needs atomic exact replacement.
    ///
    /// # Errors
    ///
    /// Fails before child mutation when the checkpoint, outer token, chain,
    /// runtime history, lifecycle fingerprint, owner ids, or configured durable
    /// budgets do not agree. It also fails when the coordinator is not fresh,
    /// another restore/lifecycle operation is pending, or live topology
    /// installation cannot commit. After an uncertain installation, retry only
    /// the exact same arguments so reconciliation remains authoritative.
    pub fn prepare_restore_lifecycle<'a>(
        &'a mut self,
        position: &SubscriberResumePosition,
        base_interests: &[ReactiveInterest<N>],
        owners: Vec<(HandlerId, Vec<ReactiveInterest<N>>)>,
    ) -> SubscriberOperation<'a, ()>
    where
        H: InterestOwnerSubscriber<N>,
        L: InterestOwnerSubscriber<N>,
        N: Send + 'static,
    {
        let position = position.clone();
        let base_interests = base_interests.to_vec();
        Box::pin(async move {
            if owners.is_empty() {
                return self
                    .prepare_restore_base_lifecycle(&position, &base_interests)
                    .await;
            }
            self.ensure_healthy()?;
            if self.pending_restore.is_some() {
                return Err(SubscriberError::Provider(
                    "hybrid child restore reconciliation is already pending".into(),
                ));
            }
            if self.pending_cutover.is_some()
                || !self.pending_inputs.is_empty()
                || self.pending_output.is_some()
                || !self.live_buffer.is_empty()
            {
                return Err(SubscriberError::Provider(
                    "hybrid live restore preparation requires a fresh coordinator with no in-flight delivery"
                        .into(),
                ));
            }
            let intent = lifecycle_intent_from_entries(&base_interests, &owners)?;
            let checkpoint = position.subscriber_checkpoint.as_ref().ok_or_else(|| {
                SubscriberError::Provider(
                    "hybrid live restore preparation requires its coordinator checkpoint".into(),
                )
            })?;
            let mut state = decode_hybrid_checkpoint(checkpoint.as_bytes())?;
            let live_is_durable = self
                .live
                .capabilities()
                .supports(SubscriberCapability::DurableReplay);
            self.normalize_restore_candidate(&position, &mut state, live_is_durable)?;
            if intent != state.lifecycle_intent {
                return Err(SubscriberError::Provider(
                    "restored runtime interests do not match the hybrid checkpoint lifecycle intent"
                        .into(),
                ));
            }
            let owner_topology = owners.iter().cloned().collect::<HashMap<_, _>>();
            validate_handler_ids(owner_topology.keys())?;
            self.preflight_restore_delivery_capacity(&state)?;
            if let Some(prepared) = self.prepared_restore_position.as_ref() {
                if prepared == &position && self.lifecycle_intent == intent {
                    return Ok(());
                }
                return Err(SubscriberError::Provider(
                    "hybrid live source is already prepared for a different restore position"
                        .into(),
                ));
            }
            if self.pending_restore_preparation.is_none() {
                if self.lifecycle_generation != 1
                    || !self.base_interests.is_empty()
                    || !self.owners.is_empty()
                {
                    return Err(SubscriberError::Provider(
                        "hybrid live restore preparation requires a newly constructed coordinator"
                            .into(),
                    ));
                }
                self.pending_restore_preparation = Some(PendingRestorePreparation {
                    position: position.clone(),
                    intent: intent.clone(),
                    base_interests: base_interests.clone(),
                    owners: owners.clone(),
                    live_topology_installed: false,
                });
            } else if self
                .pending_restore_preparation
                .as_ref()
                .is_none_or(|pending| pending.position != position || pending.intent != intent)
            {
                return Err(SubscriberError::Provider(
                    "hybrid live restore preparation is pending for different interests or a different position"
                        .into(),
                ));
            }

            let pending_preparation = self
                .pending_restore_preparation
                .as_ref()
                .expect("restore preparation exists");
            let staged_base = pending_preparation.base_interests.clone();
            let staged_owners = pending_preparation.owners.clone();
            if !live_is_durable
                && !self
                    .pending_restore_preparation
                    .as_ref()
                    .expect("restore preparation exists")
                    .live_topology_installed
            {
                if staged_owners.is_empty() {
                    // `register_interests` is the generic exact base-topology
                    // replacement and clears any stale owner-scoped state.
                    self.live.register_interests(&staged_base).await?;
                } else {
                    // Exact owner replacement also removes stale owners and
                    // base/unowned interests. Upsert would make a reused live
                    // child silently retain subscriptions absent from the
                    // durable checkpoint.
                    self.live
                        .replace_interest_owners(staged_owners.clone())
                        .await?;
                }
                self.pending_restore_preparation
                    .as_mut()
                    .expect("restore preparation survives registration")
                    .live_topology_installed = true;
            }
            let staged_active = !base_interests.is_empty()
                || staged_owners
                    .iter()
                    .any(|(_, interests)| !interests.is_empty());
            if staged_active {
                self.ensure_source_chain(HybridSource::Live)?;
            }

            self.base_interests = base_interests;
            self.owners = staged_owners.into_iter().collect();
            self.lifecycle_intent = intent;
            self.prepared_restore_position = Some(position);
            self.pending_restore_preparation = None;
            Ok(())
        })
    }

    /// Recreate a base/unowned lifecycle for a durable restore without
    /// requiring either child to implement [`InterestOwnerSubscriber`].
    ///
    /// The durable historical child is validation authority and is never
    /// mutated here. A non-durable live child receives one exact base-interest
    /// replacement; a durable live child restores its own desired state and is
    /// left untouched. Retry the exact same arguments after cancellation, then
    /// pass the same `position` to [`EventSubscriber::restore_position`]. That
    /// restore retains canonical overlap but rotates the outer epoch and clears
    /// an ephemeral live child's raw token/checkpoint witness, so transport
    /// token reuse cannot be mistaken for ACK-only replay. Both preparation and
    /// restore size the exact normalized candidate and require active state to
    /// encode the fully saturated configured durable state and one real
    /// canonical-log commit for Historical and Live in both forwarded-token
    /// and synthetic-token forms before touching a child. The saturated
    /// histories end at the terminal numeric boundary, so the proof also
    /// reserves reorg replacement headroom without depending on the restored
    /// checkpoint's current height or retained source shapes.
    ///
    /// # Errors
    ///
    /// Fails before child mutation when the checkpoint, outer token, chain,
    /// runtime history, lifecycle fingerprint, or configured durable budgets do
    /// not agree. It also fails when the coordinator is not fresh, another
    /// restore/lifecycle operation is pending, or an ephemeral live child's
    /// exact registration cannot commit. Retry only the exact same arguments
    /// after cancellation or an uncertain registration result.
    pub fn prepare_restore_base_lifecycle<'a>(
        &'a mut self,
        position: &SubscriberResumePosition,
        base_interests: &[ReactiveInterest<N>],
    ) -> SubscriberOperation<'a, ()>
    where
        N: Send + 'static,
    {
        let position = position.clone();
        let base_interests = base_interests.to_vec();
        Box::pin(async move {
            self.ensure_healthy()?;
            if self.pending_restore.is_some() {
                return Err(SubscriberError::Provider(
                    "hybrid child restore reconciliation is already pending".into(),
                ));
            }
            if self.pending_cutover.is_some()
                || !self.pending_inputs.is_empty()
                || self.pending_output.is_some()
                || !self.live_buffer.is_empty()
            {
                return Err(SubscriberError::Provider(
                    "hybrid live restore preparation requires a fresh coordinator with no in-flight delivery"
                        .into(),
                ));
            }
            let intent = lifecycle_intent::<N>(&base_interests, &HashMap::new())?;
            let checkpoint = position.subscriber_checkpoint.as_ref().ok_or_else(|| {
                SubscriberError::Provider(
                    "hybrid live restore preparation requires its coordinator checkpoint".into(),
                )
            })?;
            let mut state = decode_hybrid_checkpoint(checkpoint.as_bytes())?;
            let live_is_durable = self
                .live
                .capabilities()
                .supports(SubscriberCapability::DurableReplay);
            self.normalize_restore_candidate(&position, &mut state, live_is_durable)?;
            if intent != state.lifecycle_intent {
                return Err(SubscriberError::Provider(
                    "restored runtime interests do not match the hybrid checkpoint lifecycle intent"
                        .into(),
                ));
            }
            self.preflight_restore_delivery_capacity(&state)?;
            if let Some(prepared) = self.prepared_restore_position.as_ref() {
                if prepared == &position && self.lifecycle_intent == intent {
                    return Ok(());
                }
                return Err(SubscriberError::Provider(
                    "hybrid live source is already prepared for a different restore position"
                        .into(),
                ));
            }
            if self.pending_restore_preparation.is_none() {
                if self.lifecycle_generation != 1
                    || !self.base_interests.is_empty()
                    || !self.owners.is_empty()
                {
                    return Err(SubscriberError::Provider(
                        "hybrid live restore preparation requires a newly constructed coordinator"
                            .into(),
                    ));
                }
                self.pending_restore_preparation = Some(PendingRestorePreparation {
                    position: position.clone(),
                    intent: intent.clone(),
                    base_interests: base_interests.clone(),
                    owners: Vec::new(),
                    live_topology_installed: false,
                });
            } else if self
                .pending_restore_preparation
                .as_ref()
                .is_none_or(|pending| pending.position != position || pending.intent != intent)
            {
                return Err(SubscriberError::Provider(
                    "hybrid live restore preparation is pending for different interests or a different position"
                        .into(),
                ));
            }

            if !live_is_durable
                && !self
                    .pending_restore_preparation
                    .as_ref()
                    .expect("base restore preparation exists")
                    .live_topology_installed
            {
                self.live.register_interests(&base_interests).await?;
                self.pending_restore_preparation
                    .as_mut()
                    .expect("base restore preparation survives registration")
                    .live_topology_installed = true;
            }
            if !base_interests.is_empty() {
                self.ensure_source_chain(HybridSource::Live)?;
            }

            self.base_interests = base_interests;
            self.owners.clear();
            self.lifecycle_intent = intent;
            self.prepared_restore_position = Some(position);
            self.pending_restore_preparation = None;
            Ok(())
        })
    }

    fn prepare_empty_lifecycle_barrier(
        &self,
        lifecycle_generation: u64,
        owner_generations: BTreeMap<HandlerId, u64>,
        lifecycle_intent: LifecycleIntent,
    ) -> Result<PreparedEmptyLifecycleBarrier<N>, SubscriberError> {
        if lifecycle_intent.has_active_interests() {
            return Err(SubscriberError::Provider(
                "hybrid empty-lifecycle barrier was requested for an active topology".into(),
            ));
        }

        let mut base_state = self.checkpoint_state();
        base_state.epoch = fresh_epoch();
        let sequence = 1_u64;
        base_state.next_synthetic_token = sequence.checked_add(1).ok_or_else(|| {
            SubscriberError::Provider("hybrid synthetic token space exhausted".into())
        })?;
        base_state.lifecycle_generation = lifecycle_generation;
        base_state.owner_generations = owner_generations;
        base_state.lifecycle_intent = lifecycle_intent;
        base_state.recent_inputs.clear();
        base_state.certified_historical = None;
        base_state.last_committed_token = None;
        for position in [
            &mut base_state.historical_position,
            &mut base_state.live_position,
        ] {
            // Topology changes are allowed only when the coordinator has no
            // in-flight delivery, so these raw child tokens have already been
            // acknowledged. The opaque checkpoint and canonical suffix remain
            // the child's restorable cursor and must survive the idle revision.
            position.delivery_token = None;
        }
        validate_checkpoint_state(&base_state)?;

        let barrier_id = empty_lifecycle_barrier_id(&base_state)?;
        let control = ChainControl::Barrier {
            id: barrier_id,
            block: base_state.coverage_head,
        };
        let barrier_digest = source_delivery_digest::<N>(
            &[],
            std::slice::from_ref(&control),
            None,
            None,
            self.chain_id,
        )?;
        let raw_token = SubscriberDeliveryToken::new(sequence.to_be_bytes().to_vec());
        let pending = PendingCoordinatorCommit {
            audiences: Vec::new(),
            canonical: Vec::new(),
            // Synthetic tokens are never forwarded. Live is only the existing
            // outer-token routing tag; this barrier does not claim live-source
            // progress or coverage.
            source: HybridSource::Live,
            source_token: None,
            source_checkpoint: base_state.live_position.checkpoint.clone(),
            source_progress: None,
            source_observed_through: None,
            token_kind: HybridTokenKind::Synthetic,
            token_bytes: raw_token.as_bytes().to_vec(),
            // Preserve an existing child replay witness byte-for-byte. A
            // never-used live source has no witness to preserve, so the local
            // barrier digest supplies the invariant required by a synthetic
            // last-commit token without creating source coverage.
            source_delivery_digest: base_state
                .live_position
                .delivery_digest
                .unwrap_or(barrier_digest),
            next_safe_head: base_state.safe_head,
            next_finalized_head: base_state.finalized_head,
            next_canonical_history: base_state.canonical_history.clone(),
            next_coverage_head: base_state.coverage_head,
        };
        let mut committed_state = base_state.clone();
        apply_commit_to_checkpoint(
            &mut committed_state,
            &pending,
            self.config.canonical_history_capacity,
        )?;
        fit_checkpoint_to_durable_limits(
            &mut committed_state,
            0,
            self.config.recent_input_capacity,
            self.config.max_recent_owner_entries,
        )?;
        self.validate_checkpoint_config_limits(&committed_state)?;
        validate_checkpoint_state(&committed_state)?;
        let checkpoint = encode_hybrid_checkpoint(&committed_state)?;
        let output = ReactiveInputBatch::new(Vec::new())
            .with_chain_id(self.chain_id)
            .with_chain_controls([control])
            .with_subscriber_checkpoint(SubscriberCheckpoint::new(checkpoint))
            .with_delivery_token(wrap_token(
                base_state.epoch,
                HybridSource::Live,
                HybridTokenKind::Synthetic,
                raw_token,
            ));

        Ok(PreparedEmptyLifecycleBarrier {
            base_state,
            pending,
            output,
        })
    }

    fn install_empty_lifecycle_barrier(&mut self, prepared: PreparedEmptyLifecycleBarrier<N>) {
        let PreparedEmptyLifecycleBarrier {
            base_state,
            pending,
            output,
        } = prepared;
        self.install_checkpoint(base_state, false);
        self.phase = HybridPhase::Live;
        self.fence = None;
        self.drain_through = None;
        self.pending_cutover = None;
        self.acknowledged_historical_through = None;
        self.live_buffer.clear();
        self.buffered_live_records = 0;
        self.buffered_live_bytes = 0;
        self.pending_inputs.clear();
        self.pending_output = None;
        self.pending_live_rollback = None;
        self.restored_source_replays.clear();
        self.recovery_anchor = None;
        self.pending_restore_preparation = None;
        self.prepared_restore_position = None;

        let key = (
            pending.source,
            pending.token_kind,
            pending.token_bytes.clone(),
        );
        self.pending_inputs.insert(key, pending);
        self.pending_output = Some(output);
    }

    /// Reset a successful topology transition that remains active. Empty
    /// revisions use `prepare_empty_lifecycle_barrier` so the revision itself
    /// becomes durable before the coordinator becomes idle.
    fn reset_delivery_state(&mut self) {
        debug_assert!(self.has_active_interests());
        self.epoch = fresh_epoch();
        self.next_synthetic_token = 1;
        self.phase = HybridPhase::CatchingUp;
        self.fence = None;
        self.drain_through = None;
        self.pending_cutover = None;
        self.acknowledged_historical_through = None;
        self.certified_historical = None;
        self.live_buffer.clear();
        self.buffered_live_records = 0;
        self.buffered_live_bytes = 0;
        self.recent_inputs.clear();
        self.recent_order.clear();
        self.pending_inputs.clear();
        self.pending_output = None;
        self.pending_live_rollback = None;
        self.restored_source_replays.clear();
        self.historical_position.delivery_token = None;
        self.historical_position.checkpoint = None;
        self.historical_position.delivery_digest = None;
        self.live_position.delivery_token = None;
        self.live_position.checkpoint = None;
        self.live_position.delivery_digest = None;
        self.last_committed_token = None;
        self.pending_restore_preparation = None;
        self.prepared_restore_position = None;
    }

    fn ensure_healthy(&self) -> Result<(), SubscriberError> {
        match &self.poisoned {
            Some(reason) => Err(SubscriberError::Provider(format!(
                "hybrid coordinator is poisoned: {reason}"
            ))),
            None => Ok(()),
        }
    }

    fn ensure_no_pending_restore(&self) -> Result<(), SubscriberError> {
        if self.pending_restore.is_some() {
            return Err(SubscriberError::Provider(
                "hybrid child restore reconciliation is pending; retry the exact same restore position before any other operation"
                    .into(),
            ));
        }
        if self.pending_restore_preparation.is_some() {
            return Err(SubscriberError::Provider(
                "hybrid ephemeral-live restore preparation is pending; retry that exact preparation before any other operation"
                    .into(),
            ));
        }
        if self.prepared_restore_position.is_some() {
            return Err(SubscriberError::Provider(
                "hybrid ephemeral live source is prepared; restore the exact durable position before any other operation"
                    .into(),
            ));
        }
        Ok(())
    }

    async fn reconcile_restored_source_acknowledgements(&mut self) -> Result<(), SubscriberError>
    where
        H: EventSubscriber<N>,
        L: EventSubscriber<N>,
    {
        for source in [HybridSource::Historical, HybridSource::Live] {
            if !self.restored_source_replays.contains(&source) {
                continue;
            }
            let token = match source {
                HybridSource::Historical => self.historical_position.delivery_token.clone(),
                HybridSource::Live => self.live_position.delivery_token.clone(),
            }
            .ok_or_else(|| {
                SubscriberError::Provider(format!(
                    "hybrid restored {source:?} acknowledgement is missing its child token"
                ))
            })?;
            match source {
                HybridSource::Historical => {
                    self.historical
                        .acknowledge_delivery(SubscriberDeliveryToken::new(token))
                        .await?;
                }
                HybridSource::Live => {
                    self.live
                        .acknowledge_delivery(SubscriberDeliveryToken::new(token))
                        .await?;
                }
            }
            self.restored_source_replays.remove(&source);
        }
        Ok(())
    }

    fn ensure_owner_managed_mode(&self) -> Result<(), SubscriberError> {
        if self.base_interests.is_empty() {
            Ok(())
        } else {
            Err(SubscriberError::InvalidConfig(EXCLUSIVE_TOPOLOGY_ERROR))
        }
    }

    fn ensure_base_interest_mode(&self) -> Result<(), SubscriberError> {
        if self.owners.is_empty() {
            Ok(())
        } else {
            Err(SubscriberError::InvalidConfig(EXCLUSIVE_TOPOLOGY_ERROR))
        }
    }

    fn ensure_source_chain(&self, source: HybridSource) -> Result<(), SubscriberError>
    where
        H: EventSubscriber<N>,
        L: EventSubscriber<N>,
    {
        let observed = match source {
            HybridSource::Historical => self.historical.chain_id(),
            HybridSource::Live => self.live.chain_id(),
        };
        match observed {
            Some(chain_id) if chain_id == self.chain_id => Ok(()),
            Some(chain_id) => Err(SubscriberError::Provider(format!(
                "hybrid {source:?} source changed to chain {chain_id}; expected {}",
                self.chain_id
            ))),
            None => Err(SubscriberError::Provider(format!(
                "hybrid {source:?} source has not resolved required chain {}",
                self.chain_id
            ))),
        }
    }

    fn ensure_reconfigurable(&self) -> Result<(), SubscriberError> {
        self.ensure_no_pending_restore()?;
        if self.pending_cutover.is_some() || !self.pending_inputs.is_empty() {
            return Err(SubscriberError::Provider(
                "hybrid registration cannot change while a delivery is unacknowledged".into(),
            ));
        }
        if !self.live_buffer.is_empty() {
            return Err(SubscriberError::Provider(
                "hybrid registration cannot change while any buffered live delivery is pending"
                    .into(),
            ));
        }
        if self.lifecycle_generation > 1 && self.phase != HybridPhase::Live {
            return Err(SubscriberError::Provider(
                "hybrid registration cannot change until the previous lifecycle revision has fully caught up and drained to Live"
                    .into(),
            ));
        }
        Ok(())
    }

    fn begin_reconfiguration_catchup(&mut self) {
        self.certified_historical = None;
        if !self.has_active_interests() {
            self.phase = HybridPhase::Live;
            self.fence = None;
            self.drain_through = None;
            self.pending_cutover = None;
            self.acknowledged_historical_through = None;
            self.recovery_anchor = None;
            return;
        }
        self.phase = HybridPhase::CatchingUp;
        self.drain_through = None;
        self.pending_cutover = None;
        self.acknowledged_historical_through = None;
        self.fence = self
            .live_buffer
            .front()
            .and_then(|batch| batch.max_canonical_block)
            .map(|head| head.saturating_sub(1));
    }

    fn activate_lifecycle_recovery(&mut self) {
        let Some(LiveRollback::Recovery { anchor }) = self.pending_live_rollback.as_ref() else {
            return;
        };
        let anchor = *anchor;
        self.pending_live_rollback = None;
        self.phase = HybridPhase::Recovering;
        self.recovery_anchor = Some(anchor);
        self.fence = None;
        self.drain_through = None;
        self.pending_cutover = None;
        self.acknowledged_historical_through = None;
        self.certified_historical = None;
        self.live_buffer.clear();
        self.buffered_live_records = 0;
        self.buffered_live_bytes = 0;
        // Destructive child replacement invalidates both opaque child token
        // namespaces even when canonical coverage is retained for recovery.
        // Rotate the outer namespace and discard every replay comparison that
        // could otherwise misclassify a valid reused child token as corruption.
        self.epoch = fresh_epoch();
        self.next_synthetic_token = 1;
        self.pending_inputs.clear();
        self.pending_output = None;
        self.restored_source_replays.clear();
        self.last_committed_token = None;
        for position in [&mut self.historical_position, &mut self.live_position] {
            position.delivery_token = None;
            position.checkpoint = None;
            position.delivery_digest = None;
        }
    }

    fn poison(&mut self, reason: String) {
        self.phase = HybridPhase::Poisoned;
        self.poisoned = Some(reason);
    }

    fn finish_acknowledged_cutover_if_ready(&mut self) {
        let Some((_fence, through)) = self
            .fence
            .zip(self.acknowledged_historical_through)
            .filter(|(fence, through)| through >= fence)
        else {
            return;
        };
        if self.pending_cutover.is_some()
            || !matches!(
                self.phase,
                HybridPhase::CatchingUp | HybridPhase::Recovering
            )
        {
            return;
        }

        // The historical delivery was already durably acknowledged before a
        // post-registration live item established the fence. That proof is as
        // strong as acknowledging it after the fence exists; retain it for the
        // lifecycle generation instead of waiting for another historical page.
        self.phase = HybridPhase::DrainingLive;
        self.drain_through = Some(through);
        self.recovery_anchor = None;
        self.acknowledged_historical_through = None;
    }

    fn buffer_live(&mut self, batch: ReactiveInputBatch<N>) -> Result<(), SubscriberError> {
        if let Err(error) = self.ensure_source_chain(HybridSource::Live) {
            self.poison(error.to_string());
            return Err(error);
        }
        let batch = match BufferedBatch::from_batch(
            batch,
            self.chain_id,
            HybridIngressLimits::from(&self.config),
            &self.owner_generations,
        ) {
            Ok(batch) => batch,
            Err(error) => {
                self.poison(error.to_string());
                return Err(error);
            }
        };
        if let Err(error) = self.validate_owner_catchup_overlap(&batch) {
            self.poison(error.to_string());
            return Err(error);
        }
        if batch.token.is_some() && batch.max_canonical_block.is_none() {
            let message = "hybrid cannot buffer a tokened live delivery without canonical coverage; a durable live child must co-sequence finality and cursor-only controls with an acknowledgeable canonical batch";
            self.poison(message.into());
            return Err(SubscriberError::Unsupported(message));
        }
        if let Some(token) = batch.token.as_ref()
            && let Some(existing) = self.live_buffer.iter().find(|buffered| {
                buffered
                    .token
                    .as_ref()
                    .is_some_and(|candidate| candidate.as_bytes() == token.as_bytes())
            })
        {
            if same_buffered_delivery(existing, &batch) {
                return Ok(());
            }
            let message = "hybrid live source reused one delivery token for different data";
            self.poison(message.into());
            return Err(SubscriberError::Provider(message.into()));
        }
        let record_count = batch.records.len();
        let next_records = self.buffered_live_records.saturating_add(record_count);
        let next_bytes = self
            .buffered_live_bytes
            .saturating_add(batch.accounted_bytes);
        if self.live_buffer.len() >= self.config.max_buffered_live_batches
            || next_records > self.config.max_buffered_live_records
            || next_bytes > self.config.max_buffered_live_bytes
        {
            let message = format!(
                "hybrid live buffer exceeded configured bounds before cutover \
                 (batches {}/{}, records {}/{}, accounted bytes {}/{})",
                self.live_buffer.len().saturating_add(1),
                self.config.max_buffered_live_batches,
                next_records,
                self.config.max_buffered_live_records,
                next_bytes,
                self.config.max_buffered_live_bytes,
            );
            self.poison(message.clone());
            return Err(SubscriberError::Provider(message));
        }
        if self.fence.is_none()
            && let Some(head) = batch.max_canonical_block
        {
            self.fence = Some(head.saturating_sub(1));
        }
        self.buffered_live_records = next_records;
        self.buffered_live_bytes = next_bytes;
        self.live_buffer.push_back(batch);
        self.finish_acknowledged_cutover_if_ready();
        Ok(())
    }

    fn emit_historical(
        &mut self,
        batch: ReactiveInputBatch<N>,
    ) -> Result<Option<ReactiveInputBatch<N>>, SubscriberError> {
        if let Err(error) = self.ensure_source_chain(HybridSource::Historical) {
            self.poison(error.to_string());
            return Err(error);
        }
        let mut batch = match BufferedBatch::from_batch(
            batch,
            self.chain_id,
            HybridIngressLimits::from(&self.config),
            &self.owner_generations,
        ) {
            Ok(batch) => batch,
            Err(error) => {
                self.poison(error.to_string());
                return Err(error);
            }
        };
        if let Err(error) = self.validate_owner_catchup_overlap(&batch) {
            self.poison(error.to_string());
            return Err(error);
        }
        if let Err(error) = self.reconcile_historical_recovery(&mut batch) {
            self.poison(error.to_string());
            return Err(error);
        }
        if let Err(error) = self.prepare_canonical_batch(&mut batch) {
            let error = error.into_subscriber_error();
            self.poison(error.to_string());
            return Err(error);
        }
        let original_max = batch.max_canonical_block;
        let token = batch.token.as_ref().ok_or_else(|| {
            SubscriberError::Provider(
                "hybrid durable historical delivery is missing its acknowledgement token".into(),
            )
        })?;

        if self.pending_cutover.is_none()
            && self
                .fence
                .zip(original_max)
                .is_some_and(|(fence, observed)| observed >= fence)
        {
            self.pending_cutover = Some(PendingCutover {
                historical_token: token.as_bytes().to_vec(),
                through: original_max.expect("cutover condition has a block"),
            });
        }

        let result = self.build_output(HybridSource::Historical, batch);
        if let Err(error) = &result {
            self.poison(error.to_string());
        }
        result
    }

    fn reconcile_historical_recovery(
        &mut self,
        batch: &mut BufferedBatch<N>,
    ) -> Result<(), SubscriberError> {
        let Some(mut anchor) = self.recovery_anchor else {
            return Ok(());
        };

        let mut controls = Vec::with_capacity(batch.controls.len());
        for mut control in std::mem::take(&mut batch.controls) {
            match &mut control {
                ChainControl::Reorg {
                    common_ancestor,
                    old_tip,
                    ..
                } => {
                    self.validate_reorg_ancestor(common_ancestor)?;
                    if let Some(current_tip) = self.coverage_head.as_ref() {
                        *old_tip = *current_tip;
                    }
                    anchor = *common_ancestor;
                    self.recovery_anchor = Some(anchor);
                    controls.push(control);
                }
                ChainControl::CanonicalProgress(block) => {
                    if block.number <= anchor.number {
                        self.validate_overlap_block(block)?;
                    } else {
                        controls.push(control);
                    }
                }
                ChainControl::Barrier {
                    block: Some(block), ..
                } if block.number <= anchor.number => {
                    self.validate_overlap_block(block)?;
                }
                ChainControl::Safe(block) | ChainControl::Finalized(block)
                    if block.number <= anchor.number =>
                {
                    self.validate_overlap_block(block)?;
                    controls.push(control);
                }
                _ => controls.push(control),
            }
        }
        batch.controls = controls;

        let mut records = Vec::with_capacity(batch.records.len());
        for (record, audience, scope) in std::mem::take(&mut batch.records) {
            let suppress = scope_advances_canonical(scope)
                && canonical_block_ref(&record).is_some_and(|block| block.number <= anchor.number);
            if suppress {
                if let Some(block) = canonical_block_ref(&record) {
                    self.validate_overlap_block(block)?;
                }
                self.validate_recent_witness(&record)?;
            } else {
                records.push((record, audience, scope));
            }
        }
        batch.records = records;
        Ok(())
    }

    fn validate_overlap_block(&self, block: &BlockRef) -> Result<(), SubscriberError> {
        let known = self
            .canonical_history
            .iter()
            .find(|known| known.number == block.number)
            .or_else(|| {
                self.coverage_head
                    .as_ref()
                    .filter(|known| known.number == block.number)
            });
        let Some(known) = known else {
            return Err(SubscriberError::Provider(format!(
                "canonical overlap at block {} is outside retained hybrid history; increase canonical_history_capacity or perform a full resynchronization",
                block.number
            )));
        };
        compatible_block_ref(known, block).map(|_| ()).map_err(|_| {
            SubscriberError::Provider(format!(
                "historical/live overlap conflicts with committed canonical metadata at block {} without a usable reorg control",
                block.number
            ))
        })
    }

    fn validate_recent_witness(
        &self,
        record: &ReactiveInputRecord<N>,
    ) -> Result<(), SubscriberError> {
        let (identity, witness) = validated_record(record, self.chain_id)?;
        let coverage = self.recent_inputs.get(&identity).ok_or_else(|| {
            SubscriberError::Provider(
                "historical overlap is outside the retained hybrid payload-witness window; a full resynchronization is required"
                    .into(),
            )
        })?;
        let stored = coverage.witness.as_ref().ok_or_else(|| {
            SubscriberError::Provider(
                "hybrid recent-input journal is missing its payload witness".into(),
            )
        })?;
        ensure_witness_compatible(stored, &witness)?;
        Ok(())
    }

    fn validate_owner_catchup_overlap(
        &self,
        batch: &BufferedBatch<N>,
    ) -> Result<(), SubscriberError> {
        for (record, _, scope) in &batch.records {
            if *scope != DeliveryScope::OwnerCatchup {
                continue;
            }
            let block = canonical_block_ref(record).ok_or_else(|| {
                SubscriberError::Provider(
                    "hybrid owner-catchup input is missing an included canonical block".into(),
                )
            })?;
            self.validate_overlap_block(block).map_err(|error| {
                SubscriberError::Provider(format!(
                    "hybrid owner-catchup block {} is not an exact retained canonical overlap; owner-only effects require a pre-existing core rollback-journal entry and cannot advance canonical state ({error})",
                    block.number
                ))
            })?;
        }
        Ok(())
    }

    fn validate_reorg_ancestor(&self, ancestor: &BlockRef) -> Result<(), SubscriberError> {
        if let Some(known) = self
            .canonical_history
            .iter()
            .find(|known| known.number == ancestor.number)
        {
            if known.hash == ancestor.hash {
                return Ok(());
            }
            return Err(SubscriberError::Provider(format!(
                "historical recovery reorg has a conflicting ancestor hash at block {}",
                ancestor.number
            )));
        }
        let Some(earliest) = self.canonical_history.front() else {
            return Err(SubscriberError::Provider(
                "historical recovery reorg has no retained rollback horizon".into(),
            ));
        };
        if ancestor.number < earliest.number {
            return Err(SubscriberError::Provider(format!(
                "historical recovery reorg ancestor {} is older than retained hybrid history; full resynchronization is required",
                ancestor.number
            )));
        }
        Ok(())
    }

    fn prepare_canonical_batch(
        &self,
        batch: &mut BufferedBatch<N>,
    ) -> Result<(), CanonicalPlanError> {
        let validation_batch = ReactiveInputBatch::from_deliveries(batch.records.iter().map(
            |(record, audience, scope)| {
                ReactiveInputDelivery::new(record.clone(), audience.clone(), *scope)
            },
        ))
        .with_chain_id(self.chain_id)
        .with_chain_controls(batch.controls.clone());
        let sequence_state = CanonicalSequenceState::new(
            self.canonical_history.iter().copied().collect(),
            self.coverage_head,
            self.safe_head,
            self.finalized_head,
        );
        let validation = normalize_and_validate_canonical_sequence_diagnostic(
            &sequence_state,
            &validation_batch,
        )
        .map_err(classify_canonical_validation_error)?;
        batch.controls = validation.normalized_chain_controls().to_vec();
        batch.next_safe_head = validation.next_state().safe_head().copied();
        batch.next_finalized_head = validation.next_state().finalized_head().copied();
        batch.next_canonical_history = validation
            .next_state()
            .retained_canonical_history()
            .to_vec();
        batch.next_coverage_head = validation.next_state().coverage_head().copied();
        if let Some(coverage_head) = batch.next_coverage_head {
            // Core deliberately permits a sparse retained tail below an
            // authenticated coverage head (for example, removing retained 105
            // from [100, 105, 106] proves synthetic parent 104). V5 keeps the
            // stronger invariant that its durable history ends at coverage,
            // so retain or enrich that authenticated head before bounding.
            if let Some(retained) = batch
                .next_canonical_history
                .iter_mut()
                .find(|retained| retained.number == coverage_head.number)
            {
                *retained = compatible_block_ref(retained, &coverage_head)
                    .map_err(CanonicalPlanError::Invalid)?;
            } else {
                batch.next_canonical_history.push(coverage_head);
                batch
                    .next_canonical_history
                    .sort_by_key(|block| block.number);
            }
        }
        truncate_front_to(
            &mut batch.next_canonical_history,
            self.config.canonical_history_capacity,
        );
        batch.canonical.clear();
        for mutation in validation.mutations() {
            let projected = match mutation {
                CanonicalSequenceMutation::Rewind {
                    common_ancestor: Some(ancestor),
                    ..
                } => Some(CanonicalMutation::Rewind(*ancestor)),
                CanonicalSequenceMutation::Rewind {
                    common_ancestor: None,
                    ..
                } => Some(CanonicalMutation::Reset),
                CanonicalSequenceMutation::Canonical(block) => {
                    Some(CanonicalMutation::Advance(*block))
                }
                CanonicalSequenceMutation::Safe(_) | CanonicalSequenceMutation::Finalized(_) => {
                    None
                }
                _ => {
                    return Err(CanonicalPlanError::Invalid(SubscriberError::Unsupported(
                        "hybrid core validator returned an unknown canonical mutation",
                    )));
                }
            };
            if let Some(projected) = projected {
                batch.canonical.push(projected);
            }
        }
        batch.source_progress = batch
            .canonical
            .iter()
            .rev()
            .find_map(|mutation| match mutation {
                CanonicalMutation::Rewind(block) | CanonicalMutation::Advance(block) => {
                    Some(*block)
                }
                CanonicalMutation::Reset => None,
            });
        Ok(())
    }

    fn emit_buffered_live(&mut self) -> Result<Option<ReactiveInputBatch<N>>, SubscriberError> {
        while let Some(mut batch) = self.live_buffer.pop_front() {
            self.buffered_live_records = self
                .buffered_live_records
                .saturating_sub(batch.records.len());
            self.buffered_live_bytes = self
                .buffered_live_bytes
                .saturating_sub(batch.accounted_bytes);
            let through = self.drain_through;
            if let Err(error) = self.reconcile_buffered_controls(&mut batch.controls, through) {
                self.poison(error.to_string());
                return Err(error);
            }
            let mut retained_records = Vec::with_capacity(batch.records.len());
            for record in std::mem::take(&mut batch.records) {
                let suppress_canonical_overlap = through.is_some_and(|through| {
                    scope_advances_canonical(record.2)
                        && canonical_block_number(&record.0).is_some_and(|number| number <= through)
                });
                let suppress_resolved_drop = through.is_some_and(|through| {
                    scope_advances_canonical(record.2)
                        && dropped_block_ref(&record.0).is_some_and(|dropped| {
                            dropped.number <= through
                                && !self.canonical_history.iter().any(|known| {
                                    known.number == dropped.number && known.hash == dropped.hash
                                })
                        })
                });
                if suppress_canonical_overlap {
                    let block = canonical_block_ref(&record.0).expect("canonical number has block");
                    if let Err(error) = self
                        .validate_overlap_block(block)
                        .and_then(|()| self.validate_recent_witness(&record.0))
                    {
                        self.poison(error.to_string());
                        return Err(error);
                    }
                } else if suppress_resolved_drop {
                    // Historical recovery already certified a replacement
                    // branch through this height. A delayed removed log for
                    // the displaced live branch must not rewind it again.
                } else {
                    retained_records.push(record);
                }
            }
            batch.records = retained_records;
            if let Err(error) = self.prepare_canonical_batch(&mut batch) {
                match error {
                    CanonicalPlanError::NeedsHistoricalRecovery(message) => {
                        self.phase = HybridPhase::Recovering;
                        self.recovery_anchor = self.coverage_head;
                        return Err(SubscriberError::Provider(message));
                    }
                    CanonicalPlanError::Invalid(error) => {
                        self.poison(error.to_string());
                        return Err(error);
                    }
                }
            }
            let output = self.build_output(HybridSource::Live, batch);
            let output = match output {
                Ok(output) => output,
                Err(error) => {
                    self.poison(error.to_string());
                    return Err(error);
                }
            };
            if let Some(output) = output {
                return Ok(Some(output));
            }
        }
        self.phase = HybridPhase::Live;
        self.fence = None;
        self.drain_through = None;
        self.acknowledged_historical_through = None;
        Ok(None)
    }

    fn reconcile_buffered_controls(
        &self,
        controls: &mut Vec<ChainControl>,
        through: Option<u64>,
    ) -> Result<(), SubscriberError> {
        let Some(through) = through else {
            return Ok(());
        };
        let mut retained = Vec::with_capacity(controls.len());
        for control in std::mem::take(controls) {
            let keep = match &control {
                ChainControl::CanonicalProgress(block) if block.number <= through => {
                    self.validate_overlap_block(block)?;
                    false
                }
                ChainControl::CanonicalProgress(_) => true,
                ChainControl::Barrier {
                    block: Some(block), ..
                } if block.number <= through => {
                    self.validate_overlap_block(block)?;
                    false
                }
                ChainControl::Reorg {
                    common_ancestor,
                    old_tip,
                    ..
                } => {
                    // Historical catch-up already selected the canonical branch
                    // through this boundary. A buffered transition wholly at or
                    // crossing that boundary must not roll the runtime back a
                    // second time; records above the boundary still undergo
                    // parent-hash validation when committed.
                    old_tip.number > through && common_ancestor.number >= through
                }
                ChainControl::Safe(block) | ChainControl::Finalized(block)
                    if block.number <= through =>
                {
                    self.validate_overlap_block(block)?;
                    true
                }
                _ => true,
            };
            if keep {
                retained.push(control);
            }
        }
        *controls = retained;
        Ok(())
    }

    fn emit_live(
        &mut self,
        batch: ReactiveInputBatch<N>,
    ) -> Result<Option<ReactiveInputBatch<N>>, SubscriberError> {
        if let Err(error) = self.ensure_source_chain(HybridSource::Live) {
            self.poison(error.to_string());
            return Err(error);
        }
        let batch = match BufferedBatch::from_batch(
            batch,
            self.chain_id,
            HybridIngressLimits::from(&self.config),
            &self.owner_generations,
        ) {
            Ok(batch) => batch,
            Err(error) => {
                self.poison(error.to_string());
                return Err(error);
            }
        };
        if let Err(error) = self.validate_owner_catchup_overlap(&batch) {
            self.poison(error.to_string());
            return Err(error);
        }
        let mut batch = batch;
        if let Err(error) = self.prepare_canonical_batch(&mut batch) {
            return match error {
                CanonicalPlanError::NeedsHistoricalRecovery(message) => {
                    self.phase = HybridPhase::Recovering;
                    self.recovery_anchor = self.coverage_head;
                    Err(SubscriberError::Provider(message))
                }
                CanonicalPlanError::Invalid(error) => {
                    self.poison(error.to_string());
                    Err(error)
                }
            };
        }
        let result = self.build_output(HybridSource::Live, batch);
        if let Err(error) = &result {
            self.poison(error.to_string());
        }
        result
    }

    fn filter_recent(
        &self,
        records: Vec<RoutedRecord<N>>,
    ) -> Result<(Vec<RoutedRecord<N>>, Vec<AudienceCommit>), SubscriberError> {
        let mut merged = Vec::<(RoutedRecord<N>, ReactiveInputIdentity, RecordWitness)>::new();
        let mut positions = HashMap::<ReactiveInputIdentity, usize>::new();
        for (record, audience, scope) in records {
            let (identity, witness) = validated_record(&record, self.chain_id)?;
            if let Some(index) = positions.get(&identity).copied() {
                let ((retained, retained_audience, retained_scope), _, retained_witness) =
                    &mut merged[index];
                ensure_witness_compatible(retained_witness, &witness)?;
                let merged_duplicate = retained
                    .merge_compatible_duplicate(&record)
                    .map_err(|error| SubscriberError::Provider(error.to_string()))?;
                if !merged_duplicate {
                    // A full block or hydrated pending transaction has no
                    // complete generic body-equivalence proof. Preserve both
                    // representations instead of collapsing them by hash.
                    merged.push(((record, audience, scope), identity, witness));
                    continue;
                }
                *retained_witness = record_witness(retained, self.chain_id)?;
                merge_audience(retained_audience, audience)?;
                merge_scope(retained_scope, scope)?;
            } else {
                positions.insert(identity, merged.len());
                merged.push(((record, audience, scope), identity, witness));
            }
        }

        let mut staged = HashMap::<ReactiveInputIdentity, AudienceCoverage>::new();
        let mut commits = Vec::new();
        let mut output = Vec::new();
        for ((record, audience, scope), identity, witness) in merged {
            if !deduplicable(&record) {
                output.push((record, audience, scope));
                continue;
            }
            let coverage = match staged.entry(identity) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let coverage = match self.recent_inputs.get(&identity) {
                        Some(existing) => {
                            let mut existing = existing.clone();
                            let stored = existing.witness.as_mut().ok_or_else(|| {
                                SubscriberError::Provider(
                                    "hybrid recent-input journal is missing its payload witness"
                                        .into(),
                                )
                            })?;
                            merge_witness(stored, &witness)?;
                            existing
                        }
                        None => AudienceCoverage {
                            witness: Some(witness.clone()),
                            ..AudienceCoverage::default()
                        },
                    };
                    entry.insert(coverage)
                }
            };
            let Some(residual) = residual_audience(coverage, &audience, &self.owner_generations)
            else {
                // Refresh the bounded LRU and durably retain compatible
                // metadata enrichment even when every audience already saw
                // the handler-visible input.
                commits.push(AudienceCommit {
                    identity,
                    audience: DeliveryAudience::Owners(Vec::new()),
                    block: canonical_block_ref(&record).cloned(),
                    witness: coverage
                        .witness
                        .clone()
                        .expect("restored coverage has a validated witness"),
                });
                continue;
            };
            apply_audience_coverage(coverage, &residual, &self.owner_generations);
            commits.push(AudienceCommit {
                identity,
                audience: residual.clone(),
                block: canonical_block_ref(&record).cloned(),
                witness: coverage
                    .witness
                    .clone()
                    .expect("new and restored coverage has a validated witness"),
            });
            output.push((record, residual, scope));
        }
        Ok((output, commits))
    }

    fn build_output(
        &mut self,
        source: HybridSource,
        batch: BufferedBatch<N>,
    ) -> Result<Option<ReactiveInputBatch<N>>, SubscriberError> {
        let BufferedBatch {
            records,
            controls,
            token,
            checkpoint,
            payload_commitment,
            max_canonical_block,
            source_progress: source_progress_hint,
            delivery_digest: source_delivery_digest,
            mut canonical,
            mut next_safe_head,
            mut next_finalized_head,
            mut next_canonical_history,
            mut next_coverage_head,
            ..
        } = batch;
        let source_checkpoint = checkpoint.map(SubscriberCheckpoint::into_bytes);
        let source_position = match source {
            HybridSource::Historical => &self.historical_position,
            HybridSource::Live => &self.live_position,
        };
        let restored_replay = token.as_ref().is_some_and(|token| {
            source_position.delivery_token.as_deref() == Some(token.as_bytes())
        });
        if restored_replay {
            if source_position.delivery_digest != Some(source_delivery_digest) {
                let message = "hybrid source reused a committed delivery token for different data";
                self.poison(message.into());
                return Err(SubscriberError::Provider(message.into()));
            }
            if !self.restored_source_replays.contains(&source) {
                let message = "hybrid source reused one delivery token after it was committed";
                self.poison(message.into());
                return Err(SubscriberError::Provider(message.into()));
            }
        } else {
            self.restored_source_replays.remove(&source);
        }
        let (records, commits, controls, source_progress_hint) = if restored_replay {
            // The runtime checkpoint already embodies this exact child item.
            // Emit only its wrapped token so the lost source ACK can be
            // reconciled without reapplying records or chain controls.
            canonical.clear();
            next_safe_head = self.safe_head;
            next_finalized_head = self.finalized_head;
            next_canonical_history = self.canonical_history.iter().copied().collect();
            next_coverage_head = self.coverage_head;
            (Vec::new(), Vec::new(), Vec::new(), None)
        } else {
            let (records, commits) = match self.filter_recent(records) {
                Ok(filtered) => filtered,
                Err(error) => {
                    self.poison(error.to_string());
                    return Err(error);
                }
            };
            (records, commits, controls, source_progress_hint)
        };
        if records.is_empty() && controls.is_empty() && token.is_none() {
            return Ok(None);
        }
        let derived_source_progress = canonical.iter().rev().find_map(|mutation| match mutation {
            CanonicalMutation::Rewind(block) | CanonicalMutation::Advance(block) => Some(*block),
            CanonicalMutation::Reset => None,
        });
        let source_progress = source_progress_hint.or(derived_source_progress);
        let mut output =
            ReactiveInputBatch::from_deliveries(records.into_iter().map(
                |(record, audience, scope)| ReactiveInputDelivery::new(record, audience, scope),
            ))
            .with_chain_id(self.chain_id)
            .with_chain_controls(controls);
        if let Some(payload_commitment) = payload_commitment {
            output = output.with_payload_commitment(payload_commitment);
        }
        let (kind, token) = match token {
            Some(token) => (HybridTokenKind::Forwarded, token),
            None => {
                let sequence = self.next_synthetic_token;
                self.next_synthetic_token =
                    self.next_synthetic_token.checked_add(1).ok_or_else(|| {
                        SubscriberError::Provider("hybrid synthetic token space exhausted".into())
                    })?;
                (
                    HybridTokenKind::Synthetic,
                    SubscriberDeliveryToken::new(sequence.to_be_bytes().to_vec()),
                )
            }
        };
        let source_token = (kind == HybridTokenKind::Forwarded).then(|| token.as_bytes().to_vec());
        let pending = PendingCoordinatorCommit {
            audiences: commits,
            canonical,
            source,
            source_token,
            source_checkpoint,
            source_progress,
            source_observed_through: max_canonical_block.and_then(|number| {
                next_canonical_history
                    .iter()
                    .find(|block| block.number == number)
                    .copied()
                    .or_else(|| next_coverage_head.filter(|block| block.number == number))
            }),
            token_kind: kind,
            token_bytes: token.as_bytes().to_vec(),
            source_delivery_digest,
            next_safe_head,
            next_finalized_head,
            next_canonical_history,
            next_coverage_head,
        };
        let durable_checkpoint = self.encode_checkpoint_after(&pending)?;
        self.pending_inputs
            .insert((source, kind, token.as_bytes().to_vec()), pending);
        output = output
            .with_subscriber_checkpoint(durable_checkpoint)
            .with_delivery_token(wrap_token(self.epoch, source, kind, token));
        self.pending_output = Some(output.clone());
        if restored_replay {
            self.restored_source_replays.remove(&source);
        }
        Ok(Some(output))
    }

    fn commit_coordinator(
        &mut self,
        commit: PendingCoordinatorCommit,
    ) -> Result<(), SubscriberError> {
        let rewound = commit.canonical.iter().any(|mutation| {
            matches!(
                mutation,
                CanonicalMutation::Rewind(_) | CanonicalMutation::Reset
            )
        });
        let state = self.checkpoint_after(&commit)?;
        self.install_checkpoint(state, false);
        if rewound {
            self.restored_source_replays.clear();
        }
        Ok(())
    }

    fn encode_checkpoint_after(
        &self,
        commit: &PendingCoordinatorCommit,
    ) -> Result<SubscriberCheckpoint, SubscriberError> {
        let state = self.checkpoint_after(commit)?;
        encode_hybrid_checkpoint(&state).map(SubscriberCheckpoint::new)
    }

    fn checkpoint_after(
        &self,
        commit: &PendingCoordinatorCommit,
    ) -> Result<HybridCheckpointV5, SubscriberError> {
        let mut state = self.checkpoint_state();
        apply_commit_to_checkpoint(&mut state, commit, self.config.canonical_history_capacity)?;
        let protected_recent_inputs = commit
            .audiences
            .iter()
            .map(|audience| audience.identity)
            .collect::<HashSet<_>>()
            .len();
        fit_checkpoint_to_durable_limits(
            &mut state,
            protected_recent_inputs,
            self.config.recent_input_capacity,
            self.config.max_recent_owner_entries,
        )?;
        self.validate_checkpoint_config_limits(&state)?;
        validate_checkpoint_state(&state)?;
        Ok(state)
    }

    fn checkpoint_state(&self) -> HybridCheckpointV5 {
        HybridCheckpointV5 {
            chain_id: self.chain_id,
            epoch: self.epoch,
            next_synthetic_token: self.next_synthetic_token,
            lifecycle_generation: self.lifecycle_generation,
            owner_generations: self.owner_generations.clone(),
            lifecycle_intent: self.lifecycle_intent.clone(),
            recent_inputs: self
                .recent_order
                .iter()
                .filter_map(|identity| {
                    self.recent_inputs
                        .get(identity)
                        .cloned()
                        .map(|coverage| StoredRecentInput {
                            identity: *identity,
                            coverage,
                        })
                })
                .collect(),
            canonical_history: self.canonical_history.iter().cloned().collect(),
            coverage_head: self.coverage_head,
            safe_head: self.safe_head,
            finalized_head: self.finalized_head,
            certified_historical: self.certified_historical,
            historical_position: self.historical_position.clone(),
            live_position: self.live_position.clone(),
            last_committed_token: self.last_committed_token.clone(),
        }
    }

    fn install_checkpoint(&mut self, state: HybridCheckpointV5, restored: bool) {
        self.epoch = state.epoch;
        self.next_synthetic_token = state.next_synthetic_token;
        self.lifecycle_generation = state.lifecycle_generation;
        self.owner_generations = state.owner_generations;
        self.lifecycle_intent = state.lifecycle_intent;
        self.recent_order = state
            .recent_inputs
            .iter()
            .map(|entry| entry.identity)
            .collect();
        self.recent_inputs = state
            .recent_inputs
            .into_iter()
            .map(|entry| (entry.identity, entry.coverage))
            .collect();
        self.canonical_history = state.canonical_history.into_iter().collect();
        self.coverage_head = state.coverage_head;
        self.safe_head = state.safe_head;
        self.finalized_head = state.finalized_head;
        self.certified_historical = state.certified_historical;
        self.historical_position = state.historical_position;
        self.live_position = state.live_position;
        self.last_committed_token = state.last_committed_token;
        if restored {
            self.restored_source_replays.clear();
            if self.historical_position.delivery_token.is_some() {
                self.restored_source_replays
                    .insert(HybridSource::Historical);
            }
            if self.live_position.delivery_token.is_some()
                && self
                    .live
                    .capabilities()
                    .supports(SubscriberCapability::DurableReplay)
            {
                self.restored_source_replays.insert(HybridSource::Live);
            }
        }
    }

    fn reconcile_pending_restore(&mut self) -> Result<(), SubscriberError>
    where
        H: EventSubscriber<N>,
        L: EventSubscriber<N>,
    {
        let mut pending = self
            .pending_restore
            .take()
            .expect("restore reconciliation requires an intent");
        if !pending.historical_restored {
            let resume = pending
                .historical_resume
                .as_ref()
                .expect("unfinished historical restore has a position");
            if let Err(error) = self.historical.restore_position(resume) {
                self.pending_restore = Some(pending);
                return Err(error);
            }
            pending.historical_restored = true;
        }
        if !pending.live_restored {
            let resume = pending
                .live_resume
                .as_ref()
                .expect("unfinished live restore has a position");
            if let Err(error) = self.live.restore_position(resume) {
                self.pending_restore = Some(pending);
                return Err(error);
            }
            pending.live_restored = true;
        }
        if let Err(error) = self.ensure_source_chain(HybridSource::Historical) {
            self.pending_restore = Some(pending);
            return Err(error);
        }
        let lifecycle_nonempty = pending.state.lifecycle_intent.has_active_interests();
        if (pending.live_resume.is_some() || lifecycle_nonempty)
            && let Err(error) = self.ensure_source_chain(HybridSource::Live)
        {
            self.pending_restore = Some(pending);
            return Err(error);
        }

        let coverage_head = pending.position.coverage_head;
        self.install_checkpoint(pending.state, true);
        self.phase = if lifecycle_nonempty {
            HybridPhase::Recovering
        } else {
            HybridPhase::Live
        };
        self.fence = None;
        self.drain_through = None;
        self.pending_cutover = None;
        // Only an acknowledged historical commit certified for this exact
        // lifecycle generation can prove a late live fence after restart.
        // Source positions survive topology changes and are not authority for
        // a new generation's cutover.
        self.acknowledged_historical_through = self
            .certified_historical
            .filter(|proof| proof.lifecycle_generation == self.lifecycle_generation)
            .map(|proof| proof.through.number);
        self.recovery_anchor = lifecycle_nonempty.then_some(coverage_head);
        self.buffered_live_records = 0;
        self.buffered_live_bytes = 0;
        self.prepared_restore_position = None;
        Ok(())
    }
}

fn classify_canonical_validation_error(error: CanonicalSequenceError) -> CanonicalPlanError {
    let requires_history = error.requires_history();
    let error = error.into_reactive_error();
    if requires_history {
        return CanonicalPlanError::NeedsHistoricalRecovery(format!(
            "hybrid canonical sequence requires durable historical recovery: {error}"
        ));
    }
    CanonicalPlanError::Invalid(SubscriberError::Provider(format!(
        "hybrid canonical sequence validation failed: {error}"
    )))
}

impl<H, L, N> EventSubscriber<N> for HybridSubscriber<H, L, N>
where
    H: EventSubscriber<N>,
    L: EventSubscriber<N>,
    N: Network + Send + 'static,
{
    fn chain_id(&self) -> Option<u64> {
        Some(self.chain_id)
    }

    fn capabilities(&self) -> SubscriberCapabilities {
        let historical = self.historical.capabilities();
        let live = self.live.capabilities();
        let mut capabilities = Vec::new();
        if historical.supports(SubscriberCapability::HistoricalBackfill) {
            capabilities.push(SubscriberCapability::HistoricalBackfill);
        }
        if live.supports(SubscriberCapability::Live) {
            capabilities.push(SubscriberCapability::Live);
        }
        // The coordinator's durable checkpoint and historical replay source
        // bridge an ephemeral low-latency child after restart. This promises a
        // recoverable canonical position, not byte-identical replay of a lost
        // live transport envelope.
        if historical.supports(SubscriberCapability::DurableReplay) {
            capabilities.push(SubscriberCapability::DurableReplay);
        }
        if historical.supports(SubscriberCapability::Barriers) {
            capabilities.push(SubscriberCapability::Barriers);
        }
        for capability in [
            SubscriberCapability::Logs,
            SubscriberCapability::PendingTransactionHashes,
            SubscriberCapability::OwnerScopedDelivery,
            SubscriberCapability::DynamicInterests,
            SubscriberCapability::ExplicitReorgs,
            SubscriberCapability::FinalityUpdates,
        ] {
            if historical.supports(capability) && live.supports(capability) {
                capabilities.push(capability);
            }
        }
        // A generic Network header has no provider-neutral proof that the
        // child's payload commitment covers the exact canonical wire body.
        // Explicitly supplied committed headers are still verified, but the
        // coordinator does not advertise registrations it cannot guarantee.
        SubscriberCapabilities::new(capabilities)
    }

    fn register_interests(
        &mut self,
        interests: &[ReactiveInterest<N>],
    ) -> SubscriberOperation<'_, ()> {
        let interests = interests.to_vec();
        Box::pin(async move {
            self.ensure_healthy()?;
            self.ensure_no_pending_restore()?;
            reconcile_pending_base_lifecycle(
                &mut self.historical,
                &mut self.live,
                &mut self.pending_live_rollback,
                &mut self.phase,
                &mut self.poisoned,
            )
            .await?;
            self.ensure_base_interest_mode()?;
            self.ensure_reconfigurable()?;
            let next_intent = lifecycle_intent(&interests, &HashMap::new())?;
            if next_intent == self.lifecycle_intent {
                return Ok(());
            }
            let next_active = !interests.is_empty();
            if next_active {
                self.ensure_source_chain(HybridSource::Historical)?;
            }
            if self.coverage_head.is_some() && next_active {
                return Err(SubscriberError::Unsupported(
                    "hybrid cannot replace a base/unowned topology after canonical coverage exists because EventSubscriber has no atomic global-backfill rollback primitive; use handler-owned interests or restore a fresh coordinator",
                ));
            }
            let next_generation = self.lifecycle_generation.checked_add(1).ok_or_else(|| {
                SubscriberError::Provider("hybrid lifecycle generation exhausted".into())
            })?;
            self.preflight_base_topology(&next_intent, next_generation)?;
            let empty_barrier = if next_active {
                None
            } else {
                Some(self.prepare_empty_lifecycle_barrier(
                    next_generation,
                    BTreeMap::new(),
                    next_intent.clone(),
                )?)
            };
            let previous = self.base_interests.clone();
            let gap_anchor = self.coverage_head.filter(|_| !previous.is_empty());
            self.pending_live_rollback = Some(LiveRollback::Base {
                previous: previous.clone(),
                gap_anchor,
            });
            self.live.register_interests(&interests).await?;
            if next_active && let Err(chain_error) = self.ensure_source_chain(HybridSource::Live) {
                return match reconcile_pending_base_lifecycle(
                    &mut self.historical,
                    &mut self.live,
                    &mut self.pending_live_rollback,
                    &mut self.phase,
                    &mut self.poisoned,
                )
                .await
                {
                    Ok(()) => Err(chain_error),
                    Err(rollback) => Err(SubscriberError::Provider(format!(
                        "live chain validation failed ({chain_error}); {rollback}"
                    ))),
                };
            }
            if let Err(history_error) = self.historical.register_interests(&interests).await {
                return match reconcile_pending_base_lifecycle(
                    &mut self.historical,
                    &mut self.live,
                    &mut self.pending_live_rollback,
                    &mut self.phase,
                    &mut self.poisoned,
                )
                .await
                {
                    Ok(()) => Err(history_error),
                    Err(rollback) => Err(SubscriberError::Provider(format!(
                        "historical registration failed ({history_error}); {rollback}"
                    ))),
                };
            }
            self.pending_live_rollback = None;
            self.base_interests = interests;
            self.owners.clear();
            self.lifecycle_generation = next_generation;
            self.owner_generations.clear();
            self.lifecycle_intent = next_intent;
            if let Some(prepared) = empty_barrier {
                self.install_empty_lifecycle_barrier(prepared);
            } else {
                self.reset_delivery_state();
            }
            Ok(())
        })
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, N> {
        Box::pin(async move {
            self.ensure_healthy()?;
            self.ensure_no_pending_restore()?;
            // The outer checkpoint proves these child deliveries were already
            // ingested and persisted. Retry their idempotent ACKs before any
            // source is polled so a lost ACK cannot force an empty replay under
            // an immutable outer delivery token.
            self.reconcile_restored_source_acknowledgements().await?;
            self.activate_lifecycle_recovery();
            if self.pending_live_rollback.is_some() {
                return Err(SubscriberError::Provider(
                    "hybrid lifecycle reconciliation is pending; retry the cancelled lifecycle operation before delivery".into(),
                ));
            }
            if let Some(output) = self.pending_output.as_ref() {
                return Ok(Some(output.clone()));
            }
            // An effective-empty lifecycle is an intentional idle state, not a
            // failed live stream. A successful topology revision queued its
            // one-shot durable barrier in `pending_output`; reaching this branch
            // therefore proves that barrier was ACKed (or that the coordinator
            // has never left its initial empty topology). No child traffic is
            // needed until an active interest is installed.
            if !self.has_active_interests() {
                return Ok(None);
            }
            loop {
                match self.phase {
                    HybridPhase::DrainingLive => {
                        if let Some(batch) = self.emit_buffered_live()? {
                            return Ok(Some(batch));
                        }
                    }
                    HybridPhase::Live => match self.live.next_batch().await {
                        Ok(Some(batch)) => {
                            if let Some(batch) = self.emit_live(batch)? {
                                return Ok(Some(batch));
                            }
                        }
                        Ok(None) => {
                            self.phase = HybridPhase::Recovering;
                            self.recovery_anchor = self.coverage_head;
                            return Err(SubscriberError::Provider(
                                "hybrid live source ended; entering historical recovery".into(),
                            ));
                        }
                        Err(error) => {
                            self.phase = HybridPhase::Recovering;
                            self.recovery_anchor = self.coverage_head;
                            return Err(error);
                        }
                    },
                    HybridPhase::CatchingUp | HybridPhase::Recovering => {
                        // A durable source normally replays its one in-flight
                        // item until ACK. Once such an item is buffered, polling
                        // it again can spin forever and repeatedly cancel a
                        // slower historical future. Hold that live delivery and
                        // drive history exclusively until cutover makes it
                        // acknowledgeable.
                        if self.live_buffer.iter().any(|batch| batch.token.is_some()) {
                            match self.historical.next_batch().await? {
                                Some(batch) => {
                                    if let Some(batch) = self.emit_historical(batch)? {
                                        return Ok(Some(batch));
                                    }
                                }
                                None => {
                                    return Err(SubscriberError::Provider(
                                        "hybrid historical source ended without an acknowledged coverage proof for the live fence".into(),
                                    ));
                                }
                            }
                            continue;
                        }
                        let (historical, live) = (&mut self.historical, &mut self.live);
                        tokio::select! {
                            historical_result = historical.next_batch() => {
                                match historical_result? {
                                    Some(batch) => {
                                        if let Some(batch) = self.emit_historical(batch)? {
                                            return Ok(Some(batch));
                                        }
                                    }
                                    None => {
                                        return Err(SubscriberError::Provider(
                                            "hybrid historical source ended without an acknowledged coverage proof for the live fence".into(),
                                        ));
                                    }
                                }
                            }
                            live_result = live.next_batch() => {
                                match live_result {
                                    Ok(Some(batch)) => self.buffer_live(batch)?,
                                    Ok(None) => {
                                        self.phase = HybridPhase::Recovering;
                                        self.recovery_anchor = self.coverage_head;
                                        return Err(SubscriberError::Provider(
                                            "hybrid live source ended during catch-up/recovery".into(),
                                        ));
                                    }
                                    Err(error) => {
                                        self.phase = HybridPhase::Recovering;
                                        self.recovery_anchor = self.coverage_head;
                                        return Err(error);
                                    }
                                }
                            }
                        }
                    }
                    HybridPhase::Poisoned => self.ensure_healthy()?,
                }
            }
        })
    }

    fn restore_position(
        &mut self,
        position: &SubscriberResumePosition,
    ) -> Result<(), SubscriberError> {
        self.ensure_healthy()?;
        if self.pending_restore_preparation.is_some() {
            return Err(SubscriberError::Provider(
                "hybrid ephemeral-live restore preparation is incomplete".into(),
            ));
        }
        if let Some(pending) = self.pending_restore.as_ref() {
            if pending.position != *position {
                return Err(SubscriberError::Provider(
                    "hybrid child restore reconciliation is pending for a different position"
                        .into(),
                ));
            }
            return self.reconcile_pending_restore();
        }
        if self.pending_cutover.is_some()
            || !self.pending_inputs.is_empty()
            || self.pending_output.is_some()
            || !self.live_buffer.is_empty()
        {
            return Err(SubscriberError::Provider(
                "hybrid position restore requires a coordinator with no in-flight delivery".into(),
            ));
        }
        if position.chain_id != self.chain_id {
            return Err(SubscriberError::Provider(format!(
                "hybrid restore targets chain {}, but this coordinator targets {}",
                position.chain_id, self.chain_id
            )));
        }
        let checkpoint = position.subscriber_checkpoint.as_ref().ok_or_else(|| {
            SubscriberError::Provider(
                "hybrid restore requires its versioned coordinator checkpoint".into(),
            )
        })?;
        let mut state = decode_hybrid_checkpoint(checkpoint.as_bytes())?;
        let live_is_durable = self
            .live
            .capabilities()
            .supports(SubscriberCapability::DurableReplay);
        let rotate_epoch =
            self.normalize_restore_candidate(position, &mut state, live_is_durable)?;
        if let Some(prepared) = self.prepared_restore_position.as_ref()
            && prepared != position
        {
            return Err(SubscriberError::Provider(
                "hybrid live source was prepared for a different restore position".into(),
            ));
        }
        let lifecycle_requires_preparation = state.lifecycle_intent.requires_restore_preparation();
        if lifecycle_requires_preparation
            && self.prepared_restore_position.as_ref() != Some(position)
        {
            return Err(SubscriberError::Provider(
                "restoring a non-empty Hybrid lifecycle or owner topology requires prepare_restore_base_lifecycle (or owner-capable prepare_restore_lifecycle) first"
                    .into(),
            ));
        }
        if let Some(prepared) = self.prepared_restore_position.as_ref()
            && (prepared != position || self.lifecycle_intent != state.lifecycle_intent)
        {
            return Err(SubscriberError::Provider(
                "prepared live subscriptions do not match the Hybrid checkpoint".into(),
            ));
        }
        // Re-run the exact fit immediately before staging either child. This
        // keeps a successful asynchronous preparation from becoming a stale
        // size promise if restore behavior changes independently.
        self.preflight_restore_delivery_capacity(&state)?;
        if rotate_epoch {
            state.epoch = fresh_epoch();
        }
        validate_checkpoint_state(&state)?;

        let historical_resume = source_resume_position(self.chain_id, &state.historical_position);
        let live_resume = live_is_durable
            .then(|| source_resume_position(self.chain_id, &state.live_position))
            .flatten();
        self.pending_restore = Some(PendingRestore {
            position: position.clone(),
            state,
            historical_restored: historical_resume.is_none(),
            live_restored: live_resume.is_none(),
            historical_resume,
            live_resume,
        });
        self.reconcile_pending_restore()
    }

    fn acknowledge_delivery(
        &mut self,
        token: SubscriberDeliveryToken,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.ensure_healthy()?;
            self.ensure_no_pending_restore()?;
            if self.pending_live_rollback.is_some() {
                return Err(SubscriberError::Provider(
                    "hybrid lifecycle reconciliation is pending; retry the cancelled lifecycle operation before acknowledgement".into(),
                ));
            }
            let (epoch, source, kind, inner) = unwrap_token(token)?;
            if epoch != self.epoch {
                return Err(SubscriberError::Provider(
                    "hybrid delivery token belongs to a different coordinator epoch".into(),
                ));
            }
            let token_key = (source, kind, inner.as_bytes().to_vec());
            if !self.pending_inputs.contains_key(&token_key) {
                if self.last_committed_token.as_ref()
                    == Some(&StoredCommittedToken {
                        source,
                        kind,
                        inner: inner.as_bytes().to_vec(),
                    })
                {
                    return Ok(());
                }
                return Err(SubscriberError::Provider(
                    "hybrid acknowledgement does not match the in-flight delivery".into(),
                ));
            }
            let completes_cutover = kind == HybridTokenKind::Forwarded
                && source == HybridSource::Historical
                && self
                    .pending_cutover
                    .as_ref()
                    .is_some_and(|pending| pending.historical_token == inner.as_bytes());
            if kind == HybridTokenKind::Forwarded {
                match source {
                    HybridSource::Historical => {
                        self.historical.acknowledge_delivery(inner.clone()).await?;
                    }
                    HybridSource::Live => self.live.acknowledge_delivery(inner).await?,
                }
            }
            if let Some(commit) = self.pending_inputs.get(&token_key).cloned() {
                let acknowledged_historical_through = (source == HybridSource::Historical
                    && matches!(
                        self.phase,
                        HybridPhase::CatchingUp | HybridPhase::Recovering
                    ))
                .then_some(commit.source_observed_through)
                .flatten()
                .map(|block| block.number);
                self.commit_coordinator(commit)?;
                self.pending_inputs.remove(&token_key);
                self.pending_output = None;
                if let Some(through) = acknowledged_historical_through {
                    self.acknowledged_historical_through = Some(
                        self.acknowledged_historical_through
                            .map_or(through, |current| current.max(through)),
                    );
                }
            }
            if completes_cutover {
                let pending = self
                    .pending_cutover
                    .take()
                    .expect("matched pending cutover");
                self.drain_through = Some(pending.through);
                self.recovery_anchor = None;
                self.phase = HybridPhase::DrainingLive;
                self.acknowledged_historical_through = None;
            }
            if self.phase == HybridPhase::DrainingLive && self.live_buffer.is_empty() {
                self.phase = HybridPhase::Live;
                self.fence = None;
                self.drain_through = None;
                self.acknowledged_historical_through = None;
            }
            Ok(())
        })
    }
}

impl<H, L, N> InterestOwnerSubscriber<N> for HybridSubscriber<H, L, N>
where
    H: InterestOwnerSubscriber<N>,
    L: InterestOwnerSubscriber<N>,
    N: Network + Send + 'static,
{
    fn replace_interest_owners(
        &mut self,
        owners: Vec<(HandlerId, Vec<ReactiveInterest<N>>)>,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.ensure_healthy()?;
            self.ensure_no_pending_restore()?;
            reconcile_pending_owner_lifecycle(
                &mut self.historical,
                &mut self.live,
                &mut self.pending_live_rollback,
                &mut self.phase,
                &mut self.poisoned,
            )
            .await?;
            self.ensure_owner_managed_mode()?;
            self.ensure_reconfigurable()?;
            let next_owners = exact_owner_topology(
                &owners,
                "hybrid exact owner replacement contains a duplicate owner id",
            )?;
            let next_intent = lifecycle_intent(&[], &next_owners)?;
            if next_intent == self.lifecycle_intent {
                return Ok(());
            }
            let next_active = next_owners.values().any(|interests| !interests.is_empty());
            if next_active {
                self.ensure_source_chain(HybridSource::Historical)?;
            }
            if self.coverage_head.is_some() && next_active {
                return Err(SubscriberError::Unsupported(
                    "hybrid exact owner replacement after canonical coverage requires replace_interest_owners_with_global_backfill so a destructive child reset cannot create an event gap",
                ));
            }
            let generation = self.lifecycle_generation.checked_add(1).ok_or_else(|| {
                SubscriberError::Provider("hybrid lifecycle generation exhausted".into())
            })?;
            let next_owner_generations = next_owners
                .keys()
                .cloned()
                .map(|owner| (owner, generation))
                .collect::<BTreeMap<_, _>>();
            self.preflight_owner_topology(
                &next_owners,
                &next_intent,
                generation,
                &next_owner_generations,
                OwnerTopologyTransition::DestructiveReset,
            )?;
            let empty_barrier = if next_active {
                None
            } else {
                Some(self.prepare_empty_lifecycle_barrier(
                    generation,
                    next_owner_generations.clone(),
                    next_intent.clone(),
                )?)
            };
            let previous = LiveRollback::Topology {
                base: self.base_interests.clone(),
                owners: sorted_owner_entries(&self.owners),
                recovery_anchor: None,
                gap_anchor: self.coverage_head.filter(|_| {
                    self.lifecycle_intent
                        .owners
                        .values()
                        .any(|fingerprint| *fingerprint != empty_interest_fingerprint())
                }),
            };
            self.pending_live_rollback = Some(previous);
            self.live.replace_interest_owners(owners.clone()).await?;
            if next_active && let Err(chain_error) = self.ensure_source_chain(HybridSource::Live) {
                return match reconcile_pending_owner_lifecycle(
                    &mut self.historical,
                    &mut self.live,
                    &mut self.pending_live_rollback,
                    &mut self.phase,
                    &mut self.poisoned,
                )
                .await
                {
                    Ok(()) => Err(chain_error),
                    Err(rollback) => Err(SubscriberError::Provider(format!(
                        "live chain validation failed ({chain_error}); {rollback}"
                    ))),
                };
            }
            if let Err(history_error) = self
                .historical
                .replace_interest_owners(owners.clone())
                .await
            {
                return match reconcile_pending_owner_lifecycle(
                    &mut self.historical,
                    &mut self.live,
                    &mut self.pending_live_rollback,
                    &mut self.phase,
                    &mut self.poisoned,
                )
                .await
                {
                    Ok(()) => Err(history_error),
                    Err(rollback) => Err(SubscriberError::Provider(format!(
                        "historical exact owner replacement failed ({history_error}); {rollback}"
                    ))),
                };
            }
            self.pending_live_rollback = None;
            self.base_interests.clear();
            self.owners = next_owners;
            self.lifecycle_generation = generation;
            self.owner_generations = next_owner_generations;
            self.lifecycle_intent = next_intent;
            if let Some(prepared) = empty_barrier {
                self.install_empty_lifecycle_barrier(prepared);
            } else {
                self.reset_delivery_state();
            }
            Ok(())
        })
    }

    fn replace_interest_owners_with_global_backfill(
        &mut self,
        owners: Vec<(HandlerId, Vec<ReactiveInterest<N>>)>,
        backfill: SubscriberBackfill,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.ensure_healthy()?;
            self.ensure_no_pending_restore()?;
            reconcile_pending_owner_lifecycle(
                &mut self.historical,
                &mut self.live,
                &mut self.pending_live_rollback,
                &mut self.phase,
                &mut self.poisoned,
            )
            .await?;
            self.ensure_owner_managed_mode()?;
            self.ensure_reconfigurable()?;
            let next_owners = exact_owner_topology(
                &owners,
                "hybrid global owner replacement contains a duplicate owner id",
            )?;
            let next_intent = lifecycle_intent(&[], &next_owners)?;
            let next_active = next_owners.values().any(|interests| !interests.is_empty());
            if next_active {
                self.ensure_source_chain(HybridSource::Historical)?;
            }
            let generation = self.lifecycle_generation.checked_add(1).ok_or_else(|| {
                SubscriberError::Provider("hybrid lifecycle generation exhausted".into())
            })?;
            let next_owner_generations = next_owners
                .keys()
                .cloned()
                .map(|owner| (owner, generation))
                .collect::<BTreeMap<_, _>>();
            self.preflight_owner_topology(
                &next_owners,
                &next_intent,
                generation,
                &next_owner_generations,
                OwnerTopologyTransition::DestructiveReset,
            )?;
            let empty_barrier = if next_active {
                None
            } else {
                Some(self.prepare_empty_lifecycle_barrier(
                    generation,
                    next_owner_generations.clone(),
                    next_intent.clone(),
                )?)
            };
            let previous = LiveRollback::Topology {
                base: self.base_interests.clone(),
                owners: sorted_owner_entries(&self.owners),
                recovery_anchor: self.coverage_head,
                gap_anchor: None,
            };
            self.pending_live_rollback = Some(previous);
            self.live.replace_interest_owners(owners.clone()).await?;
            if next_active && let Err(chain_error) = self.ensure_source_chain(HybridSource::Live) {
                return match reconcile_pending_owner_lifecycle(
                    &mut self.historical,
                    &mut self.live,
                    &mut self.pending_live_rollback,
                    &mut self.phase,
                    &mut self.poisoned,
                )
                .await
                {
                    Ok(()) => Err(chain_error),
                    Err(rollback) => Err(SubscriberError::Provider(format!(
                        "live chain validation failed ({chain_error}); {rollback}"
                    ))),
                };
            }
            let history_result = if next_active {
                self.historical
                    .replace_interest_owners_with_global_backfill(owners.clone(), backfill)
                    .await
            } else {
                self.historical
                    .replace_interest_owners(owners.clone())
                    .await
            };
            if let Err(history_error) = history_result {
                return match reconcile_pending_owner_lifecycle(
                    &mut self.historical,
                    &mut self.live,
                    &mut self.pending_live_rollback,
                    &mut self.phase,
                    &mut self.poisoned,
                )
                .await
                {
                    Ok(()) => Err(history_error),
                    Err(rollback) => Err(SubscriberError::Provider(format!(
                        "historical global owner replacement failed ({history_error}); {rollback}"
                    ))),
                };
            }
            self.pending_live_rollback = None;
            self.base_interests.clear();
            self.owners = next_owners;
            self.lifecycle_generation = generation;
            self.owner_generations = next_owner_generations;
            self.lifecycle_intent = next_intent;
            if let Some(prepared) = empty_barrier {
                self.install_empty_lifecycle_barrier(prepared);
            } else {
                self.reset_delivery_state();
            }
            Ok(())
        })
    }

    fn upsert_interest_owners(
        &mut self,
        owners: Vec<(HandlerId, Vec<ReactiveInterest<N>>)>,
    ) -> SubscriberOperation<'_, ()> {
        Box::pin(async move {
            self.ensure_healthy()?;
            self.ensure_no_pending_restore()?;
            reconcile_pending_owner_lifecycle(
                &mut self.historical,
                &mut self.live,
                &mut self.pending_live_rollback,
                &mut self.phase,
                &mut self.poisoned,
            )
            .await?;
            self.ensure_owner_managed_mode()?;
            self.ensure_reconfigurable()?;
            if owners.is_empty() {
                return Ok(());
            }
            let mut unique_owners = HashSet::with_capacity(owners.len().min(1_024));
            if owners
                .iter()
                .any(|(owner, _)| !unique_owners.insert(owner.clone()))
            {
                return Err(SubscriberError::Provider(
                    "hybrid bulk owner lifecycle contains a duplicate owner id".into(),
                ));
            }
            let mut next_owners = self.owners.clone();
            for (owner, interests) in &owners {
                next_owners.insert(owner.clone(), interests.clone());
            }
            let next_intent = lifecycle_intent(&self.base_interests, &next_owners)?;
            if next_intent == self.lifecycle_intent {
                return Ok(());
            }
            let next_active = next_owners.values().any(|interests| !interests.is_empty());
            if next_active {
                self.ensure_source_chain(HybridSource::Historical)?;
            }
            let generation = self.lifecycle_generation.checked_add(1).ok_or_else(|| {
                SubscriberError::Provider("hybrid lifecycle generation exhausted".into())
            })?;
            let mut next_owner_generations = self.owner_generations.clone();
            for (owner, _) in &owners {
                next_owner_generations.insert(owner.clone(), generation);
            }
            self.preflight_owner_topology(
                &next_owners,
                &next_intent,
                generation,
                &next_owner_generations,
                OwnerTopologyTransition::Incremental {
                    removed_owners: Vec::new(),
                },
            )?;
            let empty_barrier = if next_active {
                None
            } else {
                Some(self.prepare_empty_lifecycle_barrier(
                    generation,
                    next_owner_generations.clone(),
                    next_intent.clone(),
                )?)
            };
            let previous = owners
                .iter()
                .map(|(owner, _)| (owner.clone(), self.owners.get(owner).cloned()))
                .collect::<Vec<_>>();
            let gap_anchor = self.coverage_head.filter(|_| {
                owners.iter().any(|(owner, _)| {
                    self.lifecycle_intent
                        .owners
                        .get(owner)
                        .zip(next_intent.owners.get(owner))
                        .is_some_and(|(current, next)| {
                            current != next && *current != empty_interest_fingerprint()
                        })
                })
            });
            self.pending_live_rollback = Some(LiveRollback::Bulk {
                previous: previous.clone(),
                gap_anchor,
            });
            self.live.upsert_interest_owners(owners.clone()).await?;
            if next_active && let Err(chain_error) = self.ensure_source_chain(HybridSource::Live) {
                return match reconcile_pending_owner_lifecycle(
                    &mut self.historical,
                    &mut self.live,
                    &mut self.pending_live_rollback,
                    &mut self.phase,
                    &mut self.poisoned,
                )
                .await
                {
                    Ok(()) => Err(chain_error),
                    Err(rollback) => Err(SubscriberError::Provider(format!(
                        "live chain validation failed ({chain_error}); {rollback}"
                    ))),
                };
            }
            if let Err(history_error) = self.historical.upsert_interest_owners(owners.clone()).await
            {
                return match reconcile_pending_owner_lifecycle(
                    &mut self.historical,
                    &mut self.live,
                    &mut self.pending_live_rollback,
                    &mut self.phase,
                    &mut self.poisoned,
                )
                .await
                {
                    Ok(()) => Err(history_error),
                    Err(rollback) => Err(SubscriberError::Provider(format!(
                        "historical bulk owner registration failed ({history_error}); {rollback}"
                    ))),
                };
            }
            self.pending_live_rollback = None;
            self.lifecycle_generation = generation;
            for (owner, interests) in owners {
                self.owner_generations.insert(owner.clone(), generation);
                self.owners.insert(owner, interests);
            }
            self.lifecycle_intent = next_intent;
            if let Some(prepared) = empty_barrier {
                self.install_empty_lifecycle_barrier(prepared);
            } else {
                self.begin_reconfiguration_catchup();
            }
            Ok(())
        })
    }

    fn add_interest_owner(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<N>],
    ) -> SubscriberOperation<'_, ()> {
        let interests = interests.to_vec();
        Box::pin(async move {
            self.ensure_healthy()?;
            self.ensure_no_pending_restore()?;
            reconcile_pending_owner_lifecycle(
                &mut self.historical,
                &mut self.live,
                &mut self.pending_live_rollback,
                &mut self.phase,
                &mut self.poisoned,
            )
            .await?;
            self.ensure_owner_managed_mode()?;
            self.ensure_reconfigurable()?;
            let mut next_owners = self.owners.clone();
            next_owners.insert(owner.clone(), interests.clone());
            let next_intent = lifecycle_intent(&self.base_interests, &next_owners)?;
            if next_intent == self.lifecycle_intent {
                return Ok(());
            }
            let next_active = next_owners.values().any(|interests| !interests.is_empty());
            if next_active {
                self.ensure_source_chain(HybridSource::Historical)?;
            }
            let generation = self.lifecycle_generation.checked_add(1).ok_or_else(|| {
                SubscriberError::Provider("hybrid lifecycle generation exhausted".into())
            })?;
            let mut next_owner_generations = self.owner_generations.clone();
            next_owner_generations.insert(owner.clone(), generation);
            self.preflight_owner_topology(
                &next_owners,
                &next_intent,
                generation,
                &next_owner_generations,
                OwnerTopologyTransition::Incremental {
                    removed_owners: Vec::new(),
                },
            )?;
            let empty_barrier = if next_active {
                None
            } else {
                Some(self.prepare_empty_lifecycle_barrier(
                    generation,
                    next_owner_generations,
                    next_intent.clone(),
                )?)
            };
            let previous = self.owners.get(&owner).cloned();
            self.pending_live_rollback = Some(LiveRollback::Owner {
                owner: owner.clone(),
                previous: previous.clone(),
                gap_anchor: self.coverage_head.filter(|_| {
                    previous
                        .as_ref()
                        .is_some_and(|interests| !interests.is_empty())
                }),
            });
            self.live
                .add_interest_owner(owner.clone(), &interests)
                .await?;
            if next_active && let Err(chain_error) = self.ensure_source_chain(HybridSource::Live) {
                return match reconcile_pending_owner_lifecycle(
                    &mut self.historical,
                    &mut self.live,
                    &mut self.pending_live_rollback,
                    &mut self.phase,
                    &mut self.poisoned,
                )
                .await
                {
                    Ok(()) => Err(chain_error),
                    Err(rollback) => Err(SubscriberError::Provider(format!(
                        "live chain validation failed ({chain_error}); {rollback}"
                    ))),
                };
            }
            if let Err(history_error) = self
                .historical
                .add_interest_owner(owner.clone(), &interests)
                .await
            {
                return match reconcile_pending_owner_lifecycle(
                    &mut self.historical,
                    &mut self.live,
                    &mut self.pending_live_rollback,
                    &mut self.phase,
                    &mut self.poisoned,
                )
                .await
                {
                    Ok(()) => Err(history_error),
                    Err(rollback) => Err(SubscriberError::Provider(format!(
                        "historical owner registration failed ({history_error}); {rollback}"
                    ))),
                };
            }
            self.pending_live_rollback = None;
            self.lifecycle_generation = generation;
            self.owner_generations
                .insert(owner.clone(), self.lifecycle_generation);
            self.owners.insert(owner, interests);
            self.lifecycle_intent = next_intent;
            if let Some(prepared) = empty_barrier {
                self.install_empty_lifecycle_barrier(prepared);
            } else {
                self.begin_reconfiguration_catchup();
            }
            Ok(())
        })
    }

    fn add_interest_owner_with_backfill(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<N>],
        backfill: SubscriberBackfill,
    ) -> SubscriberOperation<'_, ()> {
        let interests = interests.to_vec();
        Box::pin(async move {
            self.ensure_healthy()?;
            self.ensure_no_pending_restore()?;
            reconcile_pending_owner_lifecycle(
                &mut self.historical,
                &mut self.live,
                &mut self.pending_live_rollback,
                &mut self.phase,
                &mut self.poisoned,
            )
            .await?;
            self.ensure_owner_managed_mode()?;
            self.ensure_reconfigurable()?;
            let mut next_owners = self.owners.clone();
            next_owners.insert(owner.clone(), interests.clone());
            let next_intent = lifecycle_intent(&self.base_interests, &next_owners)?;
            if next_intent == self.lifecycle_intent {
                return Ok(());
            }
            let next_active = next_owners.values().any(|interests| !interests.is_empty());
            if next_active {
                self.ensure_source_chain(HybridSource::Historical)?;
            }
            let generation = self.lifecycle_generation.checked_add(1).ok_or_else(|| {
                SubscriberError::Provider("hybrid lifecycle generation exhausted".into())
            })?;
            let mut next_owner_generations = self.owner_generations.clone();
            next_owner_generations.insert(owner.clone(), generation);
            self.preflight_owner_topology(
                &next_owners,
                &next_intent,
                generation,
                &next_owner_generations,
                OwnerTopologyTransition::Incremental {
                    removed_owners: Vec::new(),
                },
            )?;
            let empty_barrier = if next_active {
                None
            } else {
                Some(self.prepare_empty_lifecycle_barrier(
                    generation,
                    next_owner_generations,
                    next_intent.clone(),
                )?)
            };
            let previous = self.owners.get(&owner).cloned();
            self.pending_live_rollback = Some(LiveRollback::Owner {
                owner: owner.clone(),
                previous: previous.clone(),
                gap_anchor: self.coverage_head.filter(|_| {
                    previous
                        .as_ref()
                        .is_some_and(|interests| !interests.is_empty())
                }),
            });
            // Historical range work belongs exclusively to the durable child;
            // the low-latency child only installs the new live filter.
            self.live
                .add_interest_owner(owner.clone(), &interests)
                .await?;
            if next_active && let Err(chain_error) = self.ensure_source_chain(HybridSource::Live) {
                return match reconcile_pending_owner_lifecycle(
                    &mut self.historical,
                    &mut self.live,
                    &mut self.pending_live_rollback,
                    &mut self.phase,
                    &mut self.poisoned,
                )
                .await
                {
                    Ok(()) => Err(chain_error),
                    Err(rollback) => Err(SubscriberError::Provider(format!(
                        "live chain validation failed ({chain_error}); {rollback}"
                    ))),
                };
            }
            let history_result = if next_active {
                self.historical
                    .add_interest_owner_with_backfill(owner.clone(), &interests, backfill)
                    .await
            } else {
                self.historical
                    .add_interest_owner(owner.clone(), &interests)
                    .await
            };
            if let Err(history_error) = history_result {
                return match reconcile_pending_owner_lifecycle(
                    &mut self.historical,
                    &mut self.live,
                    &mut self.pending_live_rollback,
                    &mut self.phase,
                    &mut self.poisoned,
                )
                .await
                {
                    Ok(()) => Err(history_error),
                    Err(rollback) => Err(SubscriberError::Provider(format!(
                        "historical owner registration failed ({history_error}); {rollback}"
                    ))),
                };
            }
            self.pending_live_rollback = None;
            self.lifecycle_generation = generation;
            self.owner_generations
                .insert(owner.clone(), self.lifecycle_generation);
            self.owners.insert(owner, interests);
            self.lifecycle_intent = next_intent;
            if let Some(prepared) = empty_barrier {
                self.install_empty_lifecycle_barrier(prepared);
            } else {
                self.begin_reconfiguration_catchup();
            }
            Ok(())
        })
    }

    fn add_interest_owner_with_canonical_catchup(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<N>],
        retained: BlockRef,
    ) -> SubscriberOperation<'_, ()> {
        let interests = interests.to_vec();
        Box::pin(async move {
            self.ensure_healthy()?;
            self.ensure_no_pending_restore()?;
            reconcile_pending_owner_lifecycle(
                &mut self.historical,
                &mut self.live,
                &mut self.pending_live_rollback,
                &mut self.phase,
                &mut self.poisoned,
            )
            .await?;
            self.ensure_owner_managed_mode()?;
            self.ensure_reconfigurable()?;
            let mut next_owners = self.owners.clone();
            next_owners.insert(owner.clone(), interests.clone());
            let next_intent = lifecycle_intent(&self.base_interests, &next_owners)?;
            if next_intent == self.lifecycle_intent {
                return Ok(());
            }
            let next_active = next_owners.values().any(|interests| !interests.is_empty());
            if next_active {
                self.ensure_source_chain(HybridSource::Historical)?;
                self.validate_overlap_block(&retained).map_err(|error| {
                    SubscriberError::Provider(format!(
                        "hybrid canonical owner activation baseline {} is not an exact retained canonical block ({error})",
                        retained.number
                    ))
                })?;
            }
            let generation = self.lifecycle_generation.checked_add(1).ok_or_else(|| {
                SubscriberError::Provider("hybrid lifecycle generation exhausted".into())
            })?;
            let mut next_owner_generations = self.owner_generations.clone();
            next_owner_generations.insert(owner.clone(), generation);
            self.preflight_owner_topology(
                &next_owners,
                &next_intent,
                generation,
                &next_owner_generations,
                OwnerTopologyTransition::Incremental {
                    removed_owners: Vec::new(),
                },
            )?;
            let empty_barrier = if next_active {
                None
            } else {
                Some(self.prepare_empty_lifecycle_barrier(
                    generation,
                    next_owner_generations,
                    next_intent.clone(),
                )?)
            };
            let previous = self.owners.get(&owner).cloned();
            self.pending_live_rollback = Some(LiveRollback::Owner {
                owner: owner.clone(),
                previous: previous.clone(),
                gap_anchor: self.coverage_head.filter(|_| {
                    previous
                        .as_ref()
                        .is_some_and(|interests| !interests.is_empty())
                }),
            });

            // Subscribe the low-latency child first, but never ask it to perform
            // historical work. The durable child owns the exact retained-C
            // owner page and globally routed C+1 activation window.
            self.live
                .add_interest_owner(owner.clone(), &interests)
                .await?;
            if next_active && let Err(chain_error) = self.ensure_source_chain(HybridSource::Live) {
                return match reconcile_pending_owner_lifecycle(
                    &mut self.historical,
                    &mut self.live,
                    &mut self.pending_live_rollback,
                    &mut self.phase,
                    &mut self.poisoned,
                )
                .await
                {
                    Ok(()) => Err(chain_error),
                    Err(rollback) => Err(SubscriberError::Provider(format!(
                        "live chain validation failed ({chain_error}); {rollback}"
                    ))),
                };
            }
            let history_result = if next_active {
                self.historical
                    .add_interest_owner_with_canonical_catchup(owner.clone(), &interests, retained)
                    .await
            } else {
                self.historical
                    .add_interest_owner(owner.clone(), &interests)
                    .await
            };
            if let Err(history_error) = history_result {
                return match reconcile_pending_owner_lifecycle(
                    &mut self.historical,
                    &mut self.live,
                    &mut self.pending_live_rollback,
                    &mut self.phase,
                    &mut self.poisoned,
                )
                .await
                {
                    Ok(()) => Err(history_error),
                    Err(rollback) => Err(SubscriberError::Provider(format!(
                        "historical canonical owner registration failed ({history_error}); {rollback}"
                    ))),
                };
            }

            self.pending_live_rollback = None;
            self.lifecycle_generation = generation;
            self.owner_generations
                .insert(owner.clone(), self.lifecycle_generation);
            self.owners.insert(owner, interests);
            self.lifecycle_intent = next_intent;
            if let Some(prepared) = empty_barrier {
                self.install_empty_lifecycle_barrier(prepared);
            } else {
                self.begin_reconfiguration_catchup();
            }
            Ok(())
        })
    }

    fn remove_interest_owner(
        &mut self,
        owner: &HandlerId,
    ) -> SubscriberOperation<'_, Option<Vec<ReactiveInterest<N>>>> {
        let owner = owner.clone();
        Box::pin(async move {
            self.ensure_healthy()?;
            self.ensure_no_pending_restore()?;
            reconcile_pending_owner_lifecycle(
                &mut self.historical,
                &mut self.live,
                &mut self.pending_live_rollback,
                &mut self.phase,
                &mut self.poisoned,
            )
            .await?;
            self.ensure_owner_managed_mode()?;
            self.ensure_reconfigurable()?;
            if !self.owners.contains_key(&owner) {
                return Ok(None);
            }
            let mut next_owners = self.owners.clone();
            next_owners.remove(&owner);
            let next_intent = lifecycle_intent(&self.base_interests, &next_owners)?;
            let next_active = next_owners.values().any(|interests| !interests.is_empty());
            if next_active {
                self.ensure_source_chain(HybridSource::Historical)?;
            }
            let generation = self.lifecycle_generation.checked_add(1).ok_or_else(|| {
                SubscriberError::Provider("hybrid lifecycle generation exhausted".into())
            })?;
            let mut next_owner_generations = self.owner_generations.clone();
            next_owner_generations.remove(&owner);
            self.preflight_owner_topology(
                &next_owners,
                &next_intent,
                generation,
                &next_owner_generations,
                OwnerTopologyTransition::Incremental {
                    removed_owners: vec![owner.clone()],
                },
            )?;
            let empty_barrier = if next_active {
                None
            } else {
                Some(self.prepare_empty_lifecycle_barrier(
                    generation,
                    next_owner_generations,
                    next_intent.clone(),
                )?)
            };
            let previous = self.owners.get(&owner).cloned();
            self.pending_live_rollback = Some(LiveRollback::Owner {
                owner: owner.clone(),
                previous: previous.clone(),
                gap_anchor: self.coverage_head.filter(|_| {
                    previous
                        .as_ref()
                        .is_some_and(|interests| !interests.is_empty())
                }),
            });
            self.live.remove_interest_owner(&owner).await?;
            if next_active && let Err(chain_error) = self.ensure_source_chain(HybridSource::Live) {
                return match reconcile_pending_owner_lifecycle(
                    &mut self.historical,
                    &mut self.live,
                    &mut self.pending_live_rollback,
                    &mut self.phase,
                    &mut self.poisoned,
                )
                .await
                {
                    Ok(()) => Err(chain_error),
                    Err(rollback) => Err(SubscriberError::Provider(format!(
                        "live chain validation failed ({chain_error}); {rollback}"
                    ))),
                };
            }
            if let Err(history_error) = self.historical.remove_interest_owner(&owner).await {
                return match reconcile_pending_owner_lifecycle(
                    &mut self.historical,
                    &mut self.live,
                    &mut self.pending_live_rollback,
                    &mut self.phase,
                    &mut self.poisoned,
                )
                .await
                {
                    Ok(()) => Err(history_error),
                    Err(rollback) => Err(SubscriberError::Provider(format!(
                        "historical owner removal failed ({history_error}); {rollback}"
                    ))),
                };
            }
            self.pending_live_rollback = None;
            let removed = self.owners.remove(&owner);
            self.lifecycle_generation = generation;
            self.owner_generations.remove(&owner);
            self.certified_historical = None;
            for coverage in self.recent_inputs.values_mut() {
                coverage.owners.remove(&owner);
            }
            self.lifecycle_intent = next_intent;
            if let Some(prepared) = empty_barrier {
                self.install_empty_lifecycle_barrier(prepared);
            }
            Ok(removed)
        })
    }

    fn owner_interests(&self, owner: &HandlerId) -> Option<&[ReactiveInterest<N>]> {
        self.owners.get(owner).map(Vec::as_slice)
    }
}

fn merge_audience(
    current: &mut DeliveryAudience,
    incoming: DeliveryAudience,
) -> Result<(), SubscriberError> {
    let merged = match (current.clone(), incoming) {
        (DeliveryAudience::All, _) | (_, DeliveryAudience::All) => DeliveryAudience::All,
        (DeliveryAudience::Owners(mut left), DeliveryAudience::Owners(right)) => {
            left.extend(right);
            normalize_owners(&mut left);
            DeliveryAudience::Owners(left)
        }
        (DeliveryAudience::AllExcept(mut excluded), DeliveryAudience::Owners(included))
        | (DeliveryAudience::Owners(included), DeliveryAudience::AllExcept(mut excluded)) => {
            let included = included.iter().collect::<HashSet<_>>();
            excluded.retain(|owner| !included.contains(owner));
            normalize_owners(&mut excluded);
            DeliveryAudience::AllExcept(excluded)
        }
        (DeliveryAudience::AllExcept(mut left), DeliveryAudience::AllExcept(right)) => {
            let right = right.iter().collect::<HashSet<_>>();
            left.retain(|owner| right.contains(owner));
            normalize_owners(&mut left);
            DeliveryAudience::AllExcept(left)
        }
        _ => {
            return Err(SubscriberError::Unsupported(
                "hybrid cannot merge an unknown delivery-audience variant",
            ));
        }
    };
    *current = merged;
    Ok(())
}

fn merge_scope(
    current: &mut DeliveryScope,
    incoming: DeliveryScope,
) -> Result<(), SubscriberError> {
    *current = match (*current, incoming) {
        (DeliveryScope::Canonical, _) | (_, DeliveryScope::Canonical) => DeliveryScope::Canonical,
        (DeliveryScope::CanonicalProgress, _) | (_, DeliveryScope::CanonicalProgress) => {
            DeliveryScope::CanonicalProgress
        }
        (DeliveryScope::OwnerCatchup, DeliveryScope::OwnerCatchup) => DeliveryScope::OwnerCatchup,
        _ => {
            return Err(SubscriberError::Unsupported(
                "hybrid cannot merge an unknown delivery-scope variant",
            ));
        }
    };
    Ok(())
}

fn preflight_ingress_routing<N: Network>(
    deliveries: &[ReactiveInputDelivery<N>],
    owner_generations: &BTreeMap<HandlerId, u64>,
    max_projected_owner_associations: usize,
) -> Result<(), SubscriberError> {
    let mut projected = 0usize;
    for delivery in deliveries {
        let audience = delivery.audience();
        let explicit_owners: &[HandlerId] = match audience {
            DeliveryAudience::Owners(owners) | DeliveryAudience::AllExcept(owners) => owners,
            DeliveryAudience::All => &[],
            _ => {
                return Err(SubscriberError::Unsupported(
                    "hybrid source emitted an unknown delivery-audience variant",
                ));
            }
        };
        if explicit_owners.len() > max_projected_owner_associations {
            return Err(SubscriberError::Provider(format!(
                "hybrid child batch exceeds the explicit owner-audience ingress bound ({}/{max_projected_owner_associations})",
                explicit_owners.len()
            )));
        }

        let associations = match audience {
            DeliveryAudience::All => owner_generations.len(),
            DeliveryAudience::AllExcept(excluded) => owner_generations
                .len()
                .checked_add(excluded.len())
                .ok_or_else(|| {
                    SubscriberError::Provider(
                        "hybrid child batch projected AllExcept owner work overflowed".into(),
                    )
                })?,
            DeliveryAudience::Owners(owners) => owners.len(),
            _ => unreachable!("unknown audience rejected above"),
        };
        projected = projected.checked_add(associations).ok_or_else(|| {
            SubscriberError::Provider(
                "hybrid child batch projected owner associations overflowed".into(),
            )
        })?;
        if projected > max_projected_owner_associations {
            return Err(SubscriberError::Provider(format!(
                "hybrid child batch exceeds the projected owner associations ingress bound ({projected}/{max_projected_owner_associations})"
            )));
        }

        validate_routing(audience, delivery.scope())?;
        if let Some(owner) = explicit_owners
            .iter()
            .find(|owner| !owner_generations.contains_key(*owner))
        {
            return Err(SubscriberError::Provider(format!(
                "hybrid source routed a delivery through unknown owner {:?}",
                owner.as_str()
            )));
        }
    }
    Ok(())
}

fn validate_routing(
    audience: &DeliveryAudience,
    scope: DeliveryScope,
) -> Result<(), SubscriberError> {
    match audience {
        DeliveryAudience::All => {}
        DeliveryAudience::Owners(owners) => {
            validate_owner_list(owners, "owner audience")?;
        }
        DeliveryAudience::AllExcept(owners) => {
            validate_handler_ids(owners.iter())?;
            if has_duplicate_owners(owners) {
                return Err(SubscriberError::Provider(
                    "hybrid source emitted a duplicate owner in an exclusion audience".into(),
                ));
            }
        }
        _ => {
            return Err(SubscriberError::Unsupported(
                "hybrid source emitted an unknown delivery-audience variant",
            ));
        }
    }
    match scope {
        DeliveryScope::Canonical | DeliveryScope::CanonicalProgress => Ok(()),
        DeliveryScope::OwnerCatchup if matches!(audience, DeliveryAudience::Owners(_)) => Ok(()),
        DeliveryScope::OwnerCatchup => Err(SubscriberError::Provider(
            "hybrid owner-catchup delivery requires a non-empty exact owner audience".into(),
        )),
        _ => Err(SubscriberError::Unsupported(
            "hybrid source emitted an unknown delivery-scope variant",
        )),
    }
}

fn validate_owner_list(owners: &[HandlerId], label: &str) -> Result<(), SubscriberError> {
    if owners.is_empty() {
        return Err(SubscriberError::Provider(format!(
            "hybrid source emitted an empty {label}"
        )));
    }
    if has_duplicate_owners(owners) {
        return Err(SubscriberError::Provider(format!(
            "hybrid source emitted a duplicate owner in {label}"
        )));
    }
    validate_handler_ids(owners.iter())
}

fn has_duplicate_owners(owners: &[HandlerId]) -> bool {
    let mut unique = HashSet::with_capacity(owners.len().min(1_024));
    owners.iter().any(|owner| !unique.insert(owner))
}

fn validate_handler_ids<'a>(
    owners: impl IntoIterator<Item = &'a HandlerId>,
) -> Result<(), SubscriberError> {
    if let Some(owner) = owners
        .into_iter()
        .find(|owner| owner.as_str().len() > HYBRID_MAX_HANDLER_ID_BYTES)
    {
        return Err(SubscriberError::Provider(format!(
            "hybrid handler id {:?} exceeds the durable v5 limit of {} bytes",
            owner.as_str(),
            HYBRID_MAX_HANDLER_ID_BYTES
        )));
    }
    Ok(())
}

fn residual_audience(
    coverage: &AudienceCoverage,
    requested: &DeliveryAudience,
    owner_generations: &BTreeMap<HandlerId, u64>,
) -> Option<DeliveryAudience> {
    let (base_requested, mut requested_owners, mut explicitly_excluded) = match requested {
        DeliveryAudience::All => (
            true,
            owner_generations.keys().cloned().collect::<Vec<_>>(),
            Vec::new(),
        ),
        DeliveryAudience::Owners(owners) => (false, owners.clone(), Vec::new()),
        DeliveryAudience::AllExcept(excluded) => {
            let excluded_set = excluded.iter().collect::<HashSet<_>>();
            (
                true,
                owner_generations
                    .keys()
                    .filter(|owner| !excluded_set.contains(owner))
                    .cloned()
                    .collect::<Vec<_>>(),
                excluded.clone(),
            )
        }
        _ => return Some(requested.clone()),
    };
    normalize_owners(&mut requested_owners);
    normalize_owners(&mut explicitly_excluded);

    let mut covered = Vec::new();
    let mut uncovered = Vec::new();
    for owner in requested_owners {
        let generation = owner_generations.get(&owner).copied().unwrap_or(0);
        if coverage.owners.get(&owner) == Some(&generation) {
            covered.push(owner);
        } else {
            uncovered.push(owner);
        }
    }

    if base_requested && !coverage.base {
        explicitly_excluded.extend(covered);
        normalize_owners(&mut explicitly_excluded);
        return Some(DeliveryAudience::AllExcept(explicitly_excluded));
    }
    if base_requested || matches!(requested, DeliveryAudience::Owners(_)) {
        normalize_owners(&mut uncovered);
        return (!uncovered.is_empty()).then_some(DeliveryAudience::Owners(uncovered));
    }
    None
}

fn apply_audience_coverage(
    coverage: &mut AudienceCoverage,
    audience: &DeliveryAudience,
    owner_generations: &BTreeMap<HandlerId, u64>,
) {
    match audience {
        DeliveryAudience::All => {
            coverage.base = true;
            for (owner, generation) in owner_generations {
                coverage.owners.insert(owner.clone(), *generation);
            }
        }
        DeliveryAudience::Owners(owners) => {
            for owner in owners {
                coverage.owners.insert(
                    owner.clone(),
                    owner_generations.get(owner).copied().unwrap_or(0),
                );
            }
        }
        DeliveryAudience::AllExcept(excluded) => {
            coverage.base = true;
            let excluded = excluded.iter().collect::<HashSet<_>>();
            for (owner, generation) in owner_generations {
                if !excluded.contains(owner) {
                    coverage.owners.insert(owner.clone(), *generation);
                }
            }
        }
        _ => {}
    }
}

fn normalize_owners(owners: &mut Vec<HandlerId>) {
    owners.sort();
    owners.dedup();
}

fn lifecycle_intent<N: Network>(
    base_interests: &[ReactiveInterest<N>],
    owners: &HashMap<HandlerId, Vec<ReactiveInterest<N>>>,
) -> Result<LifecycleIntent, SubscriberError> {
    validate_handler_ids(owners.keys())?;
    if !base_interests.is_empty() && !owners.is_empty() {
        return Err(SubscriberError::InvalidConfig(EXCLUSIVE_TOPOLOGY_ERROR));
    }
    let base = interest_fingerprint(base_interests)?;
    let owners = owners
        .iter()
        .map(|(owner, interests)| {
            interest_fingerprint(interests).map(|fingerprint| (owner.clone(), fingerprint))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    Ok(LifecycleIntent { base, owners })
}

fn lifecycle_intent_from_entries<N: Network>(
    base_interests: &[ReactiveInterest<N>],
    owners: &[(HandlerId, Vec<ReactiveInterest<N>>)],
) -> Result<LifecycleIntent, SubscriberError> {
    let mut unique = HashSet::with_capacity(owners.len().min(1_024));
    if owners
        .iter()
        .any(|(owner, _)| !unique.insert(owner.clone()))
    {
        return Err(SubscriberError::Provider(
            "hybrid lifecycle intent contains a duplicate owner id".into(),
        ));
    }
    let owners = owners.iter().cloned().collect::<HashMap<_, _>>();
    lifecycle_intent(base_interests, &owners)
}

fn interest_fingerprint<N: Network>(
    interests: &[ReactiveInterest<N>],
) -> Result<[u8; 32], SubscriberError> {
    let portable = compile_portable_interests(interests).map_err(|error| {
        SubscriberError::Provider(format!(
            "hybrid durable lifecycle intent is not provider-portable: {error}"
        ))
    })?;
    let mut encoded = portable
        .into_iter()
        .map(|interest| {
            let mut hasher = CanonicalHasher::new(b"EFCHY-LIFECYCLE-INTEREST-V2");
            match interest.kind {
                Some(portable_interest::Kind::Log(log)) => {
                    hasher.tag(1);
                    hasher
                        .sequence_len(log.addresses.len())
                        .map_err(transcript_error)?;
                    for address in log.addresses {
                        hasher.bytes(&address).map_err(transcript_error)?;
                    }
                    hasher
                        .sequence_len(log.topics.len())
                        .map_err(transcript_error)?;
                    for topic in log.topics {
                        hasher
                            .sequence_len(topic.values.len())
                            .map_err(transcript_error)?;
                        for value in topic.values {
                            hasher.bytes(&value).map_err(transcript_error)?;
                        }
                    }
                }
                Some(portable_interest::Kind::Block(block)) => {
                    hasher.tag(2);
                    let mode = BlockMode::try_from(block.mode).map_err(|_| {
                        SubscriberError::Provider(
                            "hybrid lifecycle interest has an unknown block mode".into(),
                        )
                    })?;
                    hasher.tag(match mode {
                        BlockMode::Header => 1,
                        BlockMode::FullBlock => 2,
                        BlockMode::Unspecified => 0,
                    });
                }
                None => {
                    return Err(SubscriberError::Provider(
                        "hybrid lifecycle interest is missing its portable kind".into(),
                    ));
                }
            }
            Ok(hasher.finish())
        })
        .collect::<Result<Vec<[u8; 32]>, SubscriberError>>()?;
    encoded.sort();
    let mut hasher = CanonicalHasher::new(LIFECYCLE_FINGERPRINT_DOMAIN);
    hasher
        .sequence_len(encoded.len())
        .map_err(transcript_error)?;
    for interest in encoded {
        hasher.hash(&alloy_primitives::B256::from(interest));
    }
    Ok(hasher.finish())
}

fn empty_interest_fingerprint() -> [u8; 32] {
    let mut hasher = CanonicalHasher::new(LIFECYCLE_FINGERPRINT_DOMAIN);
    hasher.u64(0);
    hasher.finish()
}

fn empty_lifecycle_barrier_id(state: &HybridCheckpointV5) -> Result<Vec<u8>, SubscriberError> {
    let mut hasher = CanonicalHasher::new(EMPTY_LIFECYCLE_BARRIER_DOMAIN);
    hasher.u64(state.chain_id);
    hasher.bytes(&state.epoch).map_err(transcript_error)?;
    hasher.u64(state.lifecycle_generation);
    hasher.hash(&B256::from(state.lifecycle_intent.base));
    hasher
        .sequence_len(state.lifecycle_intent.owners.len())
        .map_err(transcript_error)?;
    for (owner, fingerprint) in &state.lifecycle_intent.owners {
        hasher.handler_id(owner).map_err(transcript_error)?;
        hasher.hash(&B256::from(*fingerprint));
    }
    Ok(hasher.finish().to_vec())
}

fn is_committed_empty_lifecycle_barrier(state: &HybridCheckpointV5) -> bool {
    !state.lifecycle_intent.has_active_interests()
        && state.next_synthetic_token == 2
        && state.last_committed_token.as_ref().is_some_and(|token| {
            token.source == HybridSource::Live
                && token.kind == HybridTokenKind::Synthetic
                && token.inner.as_slice() == 1_u64.to_be_bytes()
        })
}

fn validate_chain_controls(controls: &[ChainControl]) -> Result<(), SubscriberError> {
    let first_post_record = controls
        .iter()
        .position(|control| !matches!(control, ChainControl::Reorg { .. }))
        .unwrap_or(controls.len());
    if controls[first_post_record..]
        .iter()
        .any(|control| matches!(control, ChainControl::Reorg { .. }))
    {
        return Err(SubscriberError::Provider(
            "hybrid reorg controls must precede records and all post-record controls in a batch"
                .into(),
        ));
    }
    for control in controls {
        match control {
            ChainControl::Barrier { id, .. } if id.is_empty() => {
                return Err(SubscriberError::Provider(
                    "hybrid source emitted an empty barrier id".into(),
                ));
            }
            ChainControl::Reorg {
                common_ancestor,
                old_tip,
                new_tip,
            } => {
                if common_ancestor.number > old_tip.number
                    || common_ancestor.number > new_tip.number
                    || (common_ancestor.number == old_tip.number
                        && common_ancestor.hash != old_tip.hash)
                    || (common_ancestor.number == new_tip.number
                        && common_ancestor.hash != new_tip.hash)
                {
                    return Err(SubscriberError::Provider(
                        "hybrid source emitted an invalid reorg ancestry triple".into(),
                    ));
                }
                compatible_same_height_metadata(common_ancestor, old_tip)?;
                compatible_same_height_metadata(common_ancestor, new_tip)?;
            }
            ChainControl::CanonicalProgress(_)
            | ChainControl::Safe(_)
            | ChainControl::Finalized(_)
            | ChainControl::Barrier { .. } => {}
            _ => {
                return Err(SubscriberError::Unsupported(
                    "hybrid source emitted an unknown chain-control variant",
                ));
            }
        }
    }
    Ok(())
}

fn compatible_same_height_metadata(
    left: &BlockRef,
    right: &BlockRef,
) -> Result<(), SubscriberError> {
    if left.number == right.number {
        compatible_block_ref(left, right)?;
    }
    Ok(())
}

fn apply_commit_to_checkpoint(
    state: &mut HybridCheckpointV5,
    commit: &PendingCoordinatorCommit,
    history_capacity: usize,
) -> Result<(), SubscriberError> {
    for mutation in &commit.canonical {
        match mutation {
            CanonicalMutation::Rewind(ancestor) => {
                state.recent_inputs.retain(|entry| {
                    entry
                        .coverage
                        .block
                        .as_ref()
                        .is_none_or(|block| block.number <= ancestor.number)
                });
            }
            CanonicalMutation::Reset => {
                state
                    .recent_inputs
                    .retain(|entry| entry.coverage.block.is_none());
            }
            CanonicalMutation::Advance(_) => {}
        }
    }
    let old_order = state
        .recent_inputs
        .iter()
        .map(|entry| entry.identity)
        .collect::<Vec<_>>();
    let mut entries = std::mem::take(&mut state.recent_inputs)
        .into_iter()
        .map(|entry| (entry.identity, entry))
        .collect::<HashMap<_, _>>();
    let mut touched = HashSet::with_capacity(commit.audiences.len());
    let mut refreshed = Vec::with_capacity(commit.audiences.len());
    for audience in &commit.audiences {
        let mut entry = entries
            .remove(&audience.identity)
            .or_else(|| {
                refreshed
                    .iter()
                    .position(|entry: &StoredRecentInput| entry.identity == audience.identity)
                    .map(|index| refreshed.remove(index))
            })
            .unwrap_or_else(|| StoredRecentInput {
                identity: audience.identity,
                coverage: AudienceCoverage::default(),
            });
        let coverage = &mut entry.coverage;
        match coverage.witness.as_mut() {
            Some(stored) => merge_witness(stored, &audience.witness)?,
            None => coverage.witness = Some(audience.witness.clone()),
        }
        apply_audience_coverage(coverage, &audience.audience, &state.owner_generations);
        if audience.block.is_some() {
            coverage.block = match (coverage.block.as_ref(), audience.block.as_ref()) {
                (Some(stored), Some(incoming)) => Some(compatible_block_ref(stored, incoming)?),
                (None, Some(incoming)) => Some(*incoming),
                (Some(stored), None) => Some(*stored),
                (None, None) => None,
            };
        }
        touched.insert(audience.identity);
        refreshed.push(entry);
    }
    state.recent_inputs = old_order
        .into_iter()
        .filter(|identity| !touched.contains(identity))
        .filter_map(|identity| entries.remove(&identity))
        .chain(refreshed)
        .collect();
    let reorg_ancestors = commit
        .canonical
        .iter()
        .filter_map(|mutation| match mutation {
            CanonicalMutation::Rewind(ancestor) => Some(*ancestor),
            CanonicalMutation::Reset | CanonicalMutation::Advance(_) => None,
        })
        .collect::<Vec<_>>();
    if commit
        .canonical
        .iter()
        .any(|mutation| matches!(mutation, CanonicalMutation::Reset))
        || reorg_ancestors.iter().any(|ancestor| {
            state
                .certified_historical
                .is_some_and(|proof| proof.through.number > ancestor.number)
        })
    {
        state.certified_historical = None;
    }
    state.canonical_history = commit.next_canonical_history.clone();
    state.coverage_head = commit.next_coverage_head;
    state.safe_head = commit.next_safe_head;
    state.finalized_head = commit.next_finalized_head;
    let (source_position, other_position) = match commit.source {
        HybridSource::Historical => (&mut state.historical_position, &mut state.live_position),
        HybridSource::Live => (&mut state.live_position, &mut state.historical_position),
    };
    seed_reorg_ancestors(source_position, &reorg_ancestors, history_capacity)?;
    rewind_other_source(other_position, &reorg_ancestors, history_capacity)?;
    if commit
        .canonical
        .iter()
        .any(|mutation| matches!(mutation, CanonicalMutation::Reset))
    {
        other_position.canonical_history.clear();
        other_position.coverage_head = None;
        other_position.delivery_token = None;
        other_position.checkpoint = None;
        other_position.delivery_digest = None;
    }
    source_position.delivery_token = commit.source_token.clone();
    source_position.checkpoint = commit.source_checkpoint.clone();
    source_position.delivery_digest = Some(commit.source_delivery_digest);
    apply_canonical_mutations_to(
        &mut source_position.canonical_history,
        &mut source_position.coverage_head,
        &commit.canonical,
        history_capacity,
    )?;
    if let Some(progress) = commit.source_progress.as_ref()
        && source_position
            .coverage_head
            .as_ref()
            .is_none_or(|head| progress.number > head.number)
    {
        apply_canonical_mutations_to(
            &mut source_position.canonical_history,
            &mut source_position.coverage_head,
            &[CanonicalMutation::Advance(*progress)],
            history_capacity,
        )?;
    }
    if let Some(observed) = commit.source_observed_through.as_ref()
        && source_position
            .coverage_head
            .as_ref()
            .is_none_or(|head| observed.number >= head.number)
    {
        apply_canonical_mutations_to(
            &mut source_position.canonical_history,
            &mut source_position.coverage_head,
            &[CanonicalMutation::Advance(*observed)],
            history_capacity,
        )?;
    }
    if commit.source == HybridSource::Historical
        && let Some(through) = commit.source_observed_through
    {
        let next = CertifiedHistoricalCoverage {
            lifecycle_generation: state.lifecycle_generation,
            through,
        };
        // Historical pages may be acknowledged out of height order after
        // overlap/recovery races. Within one lifecycle and surviving branch,
        // certification is a monotonic proof: a lower page cannot revoke a
        // higher page, while an equal-height page may enrich but never
        // contradict identity metadata. Crossing reorg/reset mutations cleared
        // the old proof above before this merge runs.
        state.certified_historical = Some(match state.certified_historical {
            Some(current)
                if current.lifecycle_generation == next.lifecycle_generation
                    && current.through.number > next.through.number =>
            {
                current
            }
            Some(current)
                if current.lifecycle_generation == next.lifecycle_generation
                    && current.through.number == next.through.number =>
            {
                CertifiedHistoricalCoverage {
                    lifecycle_generation: current.lifecycle_generation,
                    through: compatible_block_ref(&current.through, &next.through)?,
                }
            }
            Some(_) | None => next,
        });
    }
    state.last_committed_token = Some(StoredCommittedToken {
        source: commit.source,
        kind: commit.token_kind,
        inner: commit.token_bytes.clone(),
    });
    Ok(())
}

fn reserve_maximum_source_cursors(
    state: &mut HybridCheckpointV5,
    config: &HybridConfig,
    committing_source: HybridSource,
    block: BlockRef,
) -> Result<(), SubscriberError> {
    for (source, label, token_fill, checkpoint_fill, position) in [
        (
            HybridSource::Historical,
            "historical",
            0xe1,
            0xe2,
            &mut state.historical_position,
        ),
        (
            HybridSource::Live,
            "live",
            0x81,
            0x82,
            &mut state.live_position,
        ),
    ] {
        position.delivery_token = Some(capacity_probe_bytes(
            config.max_source_delivery_token_bytes,
            token_fill,
            label,
            "delivery token",
        )?);
        position.checkpoint = Some(capacity_probe_bytes(
            config.max_source_checkpoint_bytes,
            checkpoint_fill,
            label,
            "checkpoint",
        )?);
        if source != committing_source {
            // The other child may already have retained replay state that must
            // coexist with the committing source's maximum sequence. Seed only
            // its final observed tip: pre-seeding the committing source would
            // make the earlier simulated advances look stale.
            apply_canonical_mutations_to(
                &mut position.canonical_history,
                &mut position.coverage_head,
                &[CanonicalMutation::Advance(block)],
                config.canonical_history_capacity,
            )?;
        }
        position.delivery_digest = Some([token_fill; 32]);
    }
    Ok(())
}

fn saturated_capacity_probe_state(
    candidate: &HybridCheckpointV5,
    config: &HybridConfig,
) -> Result<HybridCheckpointV5, SubscriberError> {
    let history = saturated_capacity_probe_history(config.canonical_history_capacity)?;
    let head = *history.last().ok_or_else(|| {
        SubscriberError::Provider(
            "hybrid saturated capacity probe requires retained canonical history".into(),
        )
    })?;
    let mut state = candidate.clone();

    // Old witnesses are not protected by the next delivery and the real fit
    // path may evict all of them. Starting from the minimal retained suffix is
    // therefore exact for the one-record guarantee, while the real commit
    // below installs and protects the full maximum-width replacement witness.
    state.recent_inputs.clear();
    state.next_synthetic_token = u64::MAX;
    state.canonical_history = history.clone();
    state.coverage_head = Some(head);
    state.safe_head = Some(head);
    state.finalized_head = Some(head);
    state.certified_historical = Some(CertifiedHistoricalCoverage {
        lifecycle_generation: state.lifecycle_generation,
        through: head,
    });
    state.historical_position =
        saturated_capacity_probe_source_position(&history, head, config, "historical", 0xe1, 0xe2)?;
    state.live_position =
        saturated_capacity_probe_source_position(&history, head, config, "live", 0x81, 0x82)?;

    // The real commit installs the maximum forwarded token and its duplicated
    // last-commit proof. Retaining the candidate's old proof here could only
    // conflict with the deliberately replaced cursor namespace.
    state.last_committed_token = None;
    Ok(state)
}

fn saturated_capacity_probe_source_position(
    history: &[BlockRef],
    head: BlockRef,
    config: &HybridConfig,
    label: &str,
    token_fill: u8,
    checkpoint_fill: u8,
) -> Result<SourcePosition, SubscriberError> {
    Ok(SourcePosition {
        delivery_token: Some(capacity_probe_bytes(
            config.max_source_delivery_token_bytes,
            token_fill,
            label,
            "delivery token",
        )?),
        checkpoint: Some(capacity_probe_bytes(
            config.max_source_checkpoint_bytes,
            checkpoint_fill,
            label,
            "checkpoint",
        )?),
        coverage_head: Some(head),
        canonical_history: history.to_vec(),
        delivery_digest: Some([token_fill; 32]),
    })
}

fn saturated_capacity_probe_history(capacity: usize) -> Result<Vec<BlockRef>, SubscriberError> {
    let capacity_u64 = u64::try_from(capacity).map_err(|_| {
        SubscriberError::Provider("hybrid saturated canonical-history capacity exceeds u64".into())
    })?;
    let first_number = u64::MAX.checked_sub(capacity_u64).ok_or_else(|| {
        SubscriberError::Provider("hybrid saturated canonical-history capacity underflowed".into())
    })?;
    let mut history = Vec::new();
    history.try_reserve_exact(capacity).map_err(|_| {
        SubscriberError::Provider(
            "hybrid could not allocate the saturated canonical-history capacity probe".into(),
        )
    })?;
    let mut parent_hash = capacity_probe_sequence_hash(0xc0, u64::MAX);
    for offset in 0..capacity {
        let offset = u64::try_from(offset).map_err(|_| {
            SubscriberError::Provider(
                "hybrid saturated canonical-history offset exceeds u64".into(),
            )
        })?;
        let number = first_number.checked_add(offset).ok_or_else(|| {
            SubscriberError::Provider("hybrid saturated canonical-history height overflowed".into())
        })?;
        let hash = capacity_probe_sequence_hash(0xc1, offset);
        history.push(BlockRef {
            number,
            hash,
            parent_hash: Some(parent_hash),
            timestamp: Some(u64::MAX),
        });
        parent_hash = hash;
    }
    Ok(history)
}

fn capacity_probe_sequence_hash(fill: u8, sequence: u64) -> B256 {
    let mut bytes = [fill; 32];
    bytes[24..].copy_from_slice(&sequence.to_be_bytes());
    B256::from(bytes)
}

fn capacity_probe_bytes(
    len: usize,
    fill: u8,
    source: &str,
    field: &str,
) -> Result<Vec<u8>, SubscriberError> {
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(len).map_err(|_| {
        SubscriberError::Provider(format!(
            "hybrid could not allocate the configured {source} source {field} capacity probe"
        ))
    })?;
    bytes.resize(len, fill);
    Ok(bytes)
}

fn maximum_canonical_log_commit(
    state: &HybridCheckpointV5,
    source: HybridSource,
    history_capacity: usize,
    maximum_advances: usize,
    max_delivery_token_bytes: usize,
    max_checkpoint_bytes: usize,
) -> Result<PendingCoordinatorCommit, SubscriberError> {
    let blocks = maximum_canonical_log_probe_blocks(state, maximum_advances)?;
    let record_block = *blocks.first().ok_or_else(|| {
        SubscriberError::Provider(
            "hybrid maximum canonical-log capacity probe produced no record block".into(),
        )
    })?;
    let coverage_tip = *blocks.last().ok_or_else(|| {
        SubscriberError::Provider(
            "hybrid maximum canonical-log capacity probe produced no coverage tip".into(),
        )
    })?;
    let canonical = blocks
        .iter()
        .copied()
        .map(CanonicalMutation::Advance)
        .collect::<Vec<_>>();
    maximum_canonical_log_commit_for_transition(
        state,
        source,
        canonical,
        record_block,
        coverage_tip,
        CapacityProbeLimits {
            history_capacity,
            max_delivery_token_bytes,
            max_checkpoint_bytes,
        },
    )
}

fn maximum_canonical_log_commit_for_transition(
    state: &HybridCheckpointV5,
    source: HybridSource,
    canonical: Vec<CanonicalMutation>,
    record_block: BlockRef,
    coverage_tip: BlockRef,
    limits: CapacityProbeLimits,
) -> Result<PendingCoordinatorCommit, SubscriberError> {
    let CapacityProbeLimits {
        history_capacity,
        max_delivery_token_bytes,
        max_checkpoint_bytes,
    } = limits;
    let mut next_canonical_history = state.canonical_history.clone();
    let mut next_coverage_head = state.coverage_head;
    apply_canonical_mutations_to(
        &mut next_canonical_history,
        &mut next_coverage_head,
        &canonical,
        history_capacity,
    )?;

    let existing = state
        .recent_inputs
        .iter()
        .map(|entry| entry.identity)
        .collect::<HashSet<_>>();
    let identity = (0..=existing.len())
        .find_map(|sequence| {
            let mut transaction_hash = [0xff; 32];
            transaction_hash[24..].copy_from_slice(&(sequence as u64).to_be_bytes());
            ReactiveInputIdentity::try_from_parts(
                InputRef::Log {
                    chain_id: Some(state.chain_id),
                    block_hash: record_block.hash,
                    transaction_hash: B256::from(transaction_hash),
                    log_index: u64::MAX,
                },
                ReactiveInputKind::CanonicalLog,
            )
            .ok()
            .filter(|identity| !existing.contains(identity))
        })
        .ok_or_else(|| {
            SubscriberError::Provider(
                "hybrid could not construct a unique durable capacity-probe identity".into(),
            )
        })?;

    let audience = if state.lifecycle_intent.base != empty_interest_fingerprint() {
        DeliveryAudience::All
    } else {
        DeliveryAudience::Owners(state.owner_generations.keys().cloned().collect())
    };
    let source_label = match source {
        HybridSource::Historical => "historical",
        HybridSource::Live => "live",
    };
    let source_token = capacity_probe_bytes(
        max_delivery_token_bytes,
        match source {
            HybridSource::Historical => 0x91,
            HybridSource::Live => 0xa1,
        },
        source_label,
        "committing delivery token",
    )?;
    let source_checkpoint = capacity_probe_bytes(
        max_checkpoint_bytes,
        match source {
            HybridSource::Historical => 0x92,
            HybridSource::Live => 0xa2,
        },
        source_label,
        "committing checkpoint",
    )?;
    Ok(PendingCoordinatorCommit {
        audiences: vec![AudienceCommit {
            identity,
            audience,
            block: Some(record_block),
            witness: RecordWitness {
                payload_digest: [0xff; 32],
                chain_id: state.chain_id,
                lifecycle: WitnessLifecycle::Finalized,
                block: Some(record_block),
                transaction_index: Some(u64::MAX),
                log_index: Some(u64::MAX),
                log_block_timestamp: Some(u64::MAX),
            },
        }],
        canonical,
        source,
        source_token: Some(source_token.clone()),
        source_checkpoint: Some(source_checkpoint),
        source_progress: Some(coverage_tip),
        source_observed_through: Some(coverage_tip),
        token_kind: HybridTokenKind::Forwarded,
        token_bytes: source_token,
        source_delivery_digest: [0xfe; 32],
        next_safe_head: Some(coverage_tip),
        next_finalized_head: Some(coverage_tip),
        next_canonical_history,
        next_coverage_head,
    })
}

#[cfg(test)]
fn maximum_canonical_log_probe_block(
    state: &HybridCheckpointV5,
) -> Result<BlockRef, SubscriberError> {
    maximum_canonical_log_probe_blocks(state, 1)?
        .pop()
        .ok_or_else(|| {
            SubscriberError::Provider(
                "hybrid maximum canonical-log capacity probe produced no block".into(),
            )
        })
}

fn maximum_canonical_log_probe_blocks(
    state: &HybridCheckpointV5,
    maximum_advances: usize,
) -> Result<Vec<BlockRef>, SubscriberError> {
    if maximum_advances == 0 {
        return Err(SubscriberError::Provider(
            "hybrid maximum canonical-log capacity probe requires at least one advance".into(),
        ));
    }
    let mut known_hashes = capacity_probe_known_block_hashes(state)?;
    match state.coverage_head {
        Some(head) if head.number < u64::MAX => {
            let available = u64::MAX - head.number;
            let advance_count =
                maximum_advances.min(usize::try_from(available).unwrap_or(usize::MAX).max(1));
            maximum_canonical_log_probe_suffix(&mut known_hashes, Some(head), advance_count)
        }
        Some(head) => {
            let mut merged = head;
            for candidate in state
                .canonical_history
                .iter()
                .chain(state.historical_position.canonical_history.iter())
                .chain(state.live_position.canonical_history.iter())
                .chain(state.historical_position.coverage_head.iter())
                .chain(state.live_position.coverage_head.iter())
                .chain(state.safe_head.iter())
                .chain(state.finalized_head.iter())
                .chain(
                    state
                        .certified_historical
                        .iter()
                        .map(|proof| &proof.through),
                )
                .chain(
                    state
                        .recent_inputs
                        .iter()
                        .filter_map(|entry| entry.coverage.block.as_ref()),
                )
                .chain(
                    state
                        .recent_inputs
                        .iter()
                        .filter_map(|entry| entry.coverage.witness.as_ref()?.block.as_ref()),
                )
                .filter(|candidate| candidate.number == head.number)
            {
                merged = compatible_block_ref(&merged, candidate)?;
            }
            let predecessor = state
                .canonical_history
                .iter()
                .chain(state.historical_position.canonical_history.iter())
                .chain(state.live_position.canonical_history.iter())
                .rev()
                .find(|block| block.number.checked_add(1) == Some(head.number))
                .map(|block| block.hash);
            Ok(vec![BlockRef {
                parent_hash: Some(
                    merged
                        .parent_hash
                        .or(predecessor)
                        .map_or_else(|| unique_capacity_probe_hash(&known_hashes, 0xd2), Ok)?,
                ),
                timestamp: merged.timestamp.or(Some(u64::MAX)),
                ..merged
            }])
        }
        None => maximum_canonical_log_probe_suffix(&mut known_hashes, None, maximum_advances),
    }
}

fn maximum_canonical_log_probe_suffix(
    known_hashes: &mut HashSet<B256>,
    prior_head: Option<BlockRef>,
    advance_count: usize,
) -> Result<Vec<BlockRef>, SubscriberError> {
    let count_minus_one = advance_count.checked_sub(1).ok_or_else(|| {
        SubscriberError::Provider(
            "hybrid maximum canonical-log capacity probe requires a non-empty suffix".into(),
        )
    })?;
    let count_minus_one = u64::try_from(count_minus_one).map_err(|_| {
        SubscriberError::Provider(
            "hybrid maximum canonical-log capacity probe suffix exceeds u64".into(),
        )
    })?;
    let first_number = u64::MAX.checked_sub(count_minus_one).ok_or_else(|| {
        SubscriberError::Provider(
            "hybrid maximum canonical-log capacity probe suffix underflowed".into(),
        )
    })?;
    let mut parent_hash = match prior_head {
        Some(head) if head.number.checked_add(1) == Some(first_number) => head.hash,
        Some(_) | None => {
            let parent = unique_capacity_probe_hash(known_hashes, 0xd2)?;
            known_hashes.insert(parent);
            parent
        }
    };
    let mut blocks = Vec::new();
    blocks.try_reserve_exact(advance_count).map_err(|_| {
        SubscriberError::Provider(
            "hybrid could not allocate the maximum canonical-log history probe".into(),
        )
    })?;
    for offset in 0..advance_count {
        let number = first_number
            .checked_add(u64::try_from(offset).map_err(|_| {
                SubscriberError::Provider(
                    "hybrid maximum canonical-log capacity probe offset exceeds u64".into(),
                )
            })?)
            .ok_or_else(|| {
                SubscriberError::Provider(
                    "hybrid maximum canonical-log capacity probe height overflowed".into(),
                )
            })?;
        let hash = unique_capacity_probe_hash(known_hashes, 0xd1)?;
        known_hashes.insert(hash);
        blocks.push(BlockRef {
            number,
            hash,
            parent_hash: Some(parent_hash),
            timestamp: Some(u64::MAX),
        });
        parent_hash = hash;
    }
    Ok(blocks)
}

fn capacity_probe_state_blocks(state: &HybridCheckpointV5) -> impl Iterator<Item = &BlockRef> {
    state
        .canonical_history
        .iter()
        .chain(state.historical_position.canonical_history.iter())
        .chain(state.live_position.canonical_history.iter())
        .chain(state.coverage_head.iter())
        .chain(state.historical_position.coverage_head.iter())
        .chain(state.live_position.coverage_head.iter())
        .chain(state.safe_head.iter())
        .chain(state.finalized_head.iter())
        .chain(
            state
                .certified_historical
                .iter()
                .map(|proof| &proof.through),
        )
        .chain(
            state
                .recent_inputs
                .iter()
                .filter_map(|entry| entry.coverage.block.as_ref()),
        )
        .chain(
            state
                .recent_inputs
                .iter()
                .filter_map(|entry| entry.coverage.witness.as_ref()?.block.as_ref()),
        )
}

fn capacity_probe_known_block_hashes(
    state: &HybridCheckpointV5,
) -> Result<HashSet<B256>, SubscriberError> {
    let expected = state
        .canonical_history
        .len()
        .saturating_add(state.historical_position.canonical_history.len())
        .saturating_add(state.live_position.canonical_history.len())
        .saturating_add(state.recent_inputs.len().saturating_mul(3))
        .saturating_add(8);
    let mut known = HashSet::new();
    known.try_reserve(expected).map_err(|_| {
        SubscriberError::Provider(
            "hybrid could not allocate the maximum canonical-log block-hash probe".into(),
        )
    })?;
    for block in capacity_probe_state_blocks(state) {
        known.insert(block.hash);
    }
    for entry in &state.recent_inputs {
        match entry.identity.input_ref() {
            InputRef::Log { block_hash, .. }
            | InputRef::Block {
                hash: block_hash, ..
            } => {
                known.insert(block_hash);
            }
            InputRef::PendingTx { .. } => {}
        }
    }
    Ok(known)
}

fn unique_capacity_probe_hash(known: &HashSet<B256>, fill: u8) -> Result<B256, SubscriberError> {
    (0..=known.len())
        .map(|sequence| {
            let mut bytes = [fill; 32];
            bytes[24..].copy_from_slice(&(sequence as u64).to_be_bytes());
            B256::from(bytes)
        })
        .find(|candidate| !known.contains(candidate))
        .ok_or_else(|| {
            SubscriberError::Provider(
                "hybrid could not construct a unique maximum canonical-log block hash".into(),
            )
        })
}

#[cfg(test)]
fn append_legacy_pending_capacity_probe(
    state: &mut HybridCheckpointV5,
    chain_id: u64,
    base: bool,
    owners: BTreeMap<HandlerId, u64>,
) -> Result<(), SubscriberError> {
    let existing = state
        .recent_inputs
        .iter()
        .map(|entry| entry.identity)
        .collect::<HashSet<_>>();
    let identity = (0..=existing.len())
        .find_map(|sequence| {
            let mut hash = [0x52; 32];
            hash[24..].copy_from_slice(&(sequence as u64).to_be_bytes());
            ReactiveInputIdentity::try_from_parts(
                InputRef::PendingTx {
                    chain_id: Some(chain_id),
                    hash: B256::from(hash),
                },
                ReactiveInputKind::PendingTxHash,
            )
            .ok()
            .filter(|identity| !existing.contains(identity))
        })
        .ok_or_else(|| {
            SubscriberError::Provider(
                "hybrid could not construct a unique legacy capacity-probe identity".into(),
            )
        })?;
    state.recent_inputs.push(StoredRecentInput {
        identity,
        coverage: AudienceCoverage {
            base,
            owners,
            block: None,
            witness: Some(RecordWitness {
                payload_digest: [0x52; 32],
                chain_id,
                lifecycle: WitnessLifecycle::Pending,
                block: None,
                transaction_index: None,
                log_index: None,
                log_block_timestamp: None,
            }),
        },
    });
    Ok(())
}

fn fit_checkpoint_to_durable_limits(
    state: &mut HybridCheckpointV5,
    protected_recent_inputs: usize,
    recent_input_capacity: usize,
    max_recent_owner_entries: usize,
) -> Result<(), SubscriberError> {
    if protected_recent_inputs > state.recent_inputs.len() {
        return Err(SubscriberError::Provider(
            "hybrid current delivery has an invalid protected witness suffix".into(),
        ));
    }
    let max_drop = state.recent_inputs.len() - protected_recent_inputs;
    let mut required_drop = state
        .recent_inputs
        .len()
        .saturating_sub(recent_input_capacity);
    let mut retained_owner_entries = recent_owner_entry_count(&state.recent_inputs)?;
    for entry in state.recent_inputs.iter().take(required_drop) {
        retained_owner_entries = retained_owner_entries
            .checked_sub(entry.coverage.owners.len())
            .ok_or_else(|| {
                SubscriberError::Provider("hybrid recent owner-entry accounting underflowed".into())
            })?;
    }
    while retained_owner_entries > max_recent_owner_entries {
        let Some(entry) = state.recent_inputs.get(required_drop) else {
            break;
        };
        retained_owner_entries = retained_owner_entries
            .checked_sub(entry.coverage.owners.len())
            .ok_or_else(|| {
                SubscriberError::Provider("hybrid recent owner-entry accounting underflowed".into())
            })?;
        required_drop += 1;
    }
    if required_drop > max_drop {
        return Err(SubscriberError::Provider(format!(
            "hybrid current delivery needs {} protected recent witnesses and {retained_owner_entries} owner associations, exceeding the configured durable journal budgets",
            protected_recent_inputs
        )));
    }

    let fits = |dropped: usize| -> Result<bool, SubscriberError> {
        if codec::validate_hybrid_checkpoint_v5_limits_after_dropping_recent(state, dropped)
            .is_err()
        {
            return Ok(false);
        }
        Ok(
            codec::checkpoint_payload_len_after_dropping_recent(state, dropped)?
                <= codec::MAX_CHECKPOINT_PAYLOAD_BYTES,
        )
    };

    if !fits(max_drop)? {
        // Re-run the strongest minimal candidate checks to return the precise
        // hard-limit reason when possible.
        codec::validate_hybrid_checkpoint_v5_limits_after_dropping_recent(state, max_drop)?;
        let minimal_payload = codec::checkpoint_payload_len_after_dropping_recent(state, max_drop)?;
        return Err(SubscriberError::Provider(format!(
            "hybrid current delivery cannot fit its protected witness suffix in the {}-byte durable checkpoint payload (needs at least {minimal_payload} bytes)",
            codec::MAX_CHECKPOINT_PAYLOAD_BYTES
        )));
    }

    let mut low = required_drop;
    let mut high = max_drop;
    while low < high {
        let middle = low + (high - low) / 2;
        if fits(middle)? {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    if low != 0 {
        state.recent_inputs.drain(..low);
    }
    codec::validate_hybrid_checkpoint_v5_state(state)?;
    Ok(())
}

fn recent_owner_entry_count(entries: &[StoredRecentInput]) -> Result<usize, SubscriberError> {
    entries.iter().try_fold(0usize, |total, entry| {
        total
            .checked_add(entry.coverage.owners.len())
            .ok_or_else(|| {
                SubscriberError::Provider("hybrid recent owner-entry accounting overflowed".into())
            })
    })
}

fn seed_reorg_ancestors(
    source: &mut SourcePosition,
    ancestors: &[BlockRef],
    capacity: usize,
) -> Result<(), SubscriberError> {
    for ancestor in ancestors {
        if source
            .coverage_head
            .as_ref()
            .is_some_and(|head| head.number > ancestor.number)
            && !source
                .canonical_history
                .iter()
                .any(|known| known.number == ancestor.number)
        {
            source.canonical_history.push(*ancestor);
            source.canonical_history.sort_by_key(|block| block.number);
            truncate_front_to(&mut source.canonical_history, capacity);
        }
        if let Some(known) = source
            .canonical_history
            .iter()
            .find(|known| known.number == ancestor.number)
        {
            compatible_block_ref(known, ancestor)?;
        }
    }
    Ok(())
}

fn rewind_other_source(
    source: &mut SourcePosition,
    ancestors: &[BlockRef],
    capacity: usize,
) -> Result<(), SubscriberError> {
    for ancestor in ancestors {
        let Some(head_number) = source.coverage_head.as_ref().map(|head| head.number) else {
            continue;
        };
        if head_number < ancestor.number {
            continue;
        }
        seed_reorg_ancestors(source, std::slice::from_ref(ancestor), capacity)?;
        apply_canonical_mutations_to(
            &mut source.canonical_history,
            &mut source.coverage_head,
            &[CanonicalMutation::Rewind(*ancestor)],
            capacity,
        )?;
        if head_number > ancestor.number {
            source.delivery_token = None;
            source.checkpoint = None;
            source.delivery_digest = None;
        }
    }
    Ok(())
}

fn apply_canonical_mutations_to(
    history: &mut Vec<BlockRef>,
    coverage_head: &mut Option<BlockRef>,
    mutations: &[CanonicalMutation],
    capacity: usize,
) -> Result<(), SubscriberError> {
    for mutation in mutations {
        match mutation {
            CanonicalMutation::Reset => {
                history.clear();
                *coverage_head = None;
            }
            CanonicalMutation::Rewind(ancestor) => {
                let known_ancestor = history.iter().find(|block| block.number == ancestor.number);
                if coverage_head.is_some()
                    && known_ancestor.is_none()
                    && history
                        .first()
                        .is_none_or(|oldest| oldest.number > ancestor.number)
                {
                    return Err(SubscriberError::Provider(format!(
                        "hybrid reorg ancestor {} is older than the retained canonical effect window",
                        ancestor.number
                    )));
                }
                if let Some(known) = known_ancestor
                    && compatible_block_ref(known, ancestor).is_err()
                {
                    return Err(SubscriberError::Provider(format!(
                        "hybrid reorg ancestor hash or metadata mismatch at block {}",
                        ancestor.number
                    )));
                }
                if let Some(head) = coverage_head.as_ref()
                    && ancestor.number > head.number
                {
                    return Err(SubscriberError::Provider(
                        "hybrid reorg ancestor is ahead of committed coverage".into(),
                    ));
                }
                history.retain(|block| block.number <= ancestor.number);
                if let Some(known) = history
                    .last_mut()
                    .filter(|block| block.number == ancestor.number)
                {
                    *known = compatible_block_ref(known, ancestor)?;
                } else {
                    history.push(*ancestor);
                }
                *coverage_head = Some(match coverage_head.as_ref() {
                    Some(head) if head.number == ancestor.number => {
                        compatible_block_ref(head, ancestor)?
                    }
                    _ => *ancestor,
                });
            }
            CanonicalMutation::Advance(block) => {
                if let Some(head) = coverage_head.as_ref() {
                    if block.number < head.number {
                        let known = history
                            .iter_mut()
                            .find(|known| known.number == block.number)
                            .ok_or_else(|| {
                                SubscriberError::Provider(format!(
                                    "hybrid canonical block {} is outside retained history",
                                    block.number
                                ))
                            })?;
                        *known = compatible_block_ref(known, block)?;
                        continue;
                    }
                    if block.number == head.number {
                        let merged = compatible_block_ref(head, block)?;
                        *coverage_head = Some(merged);
                        if let Some(known) = history
                            .iter_mut()
                            .find(|known| known.number == block.number)
                        {
                            *known = compatible_block_ref(known, &merged)?;
                        }
                        continue;
                    }
                    if block.number == head.number.saturating_add(1)
                        && block.parent_hash.is_some_and(|parent| parent != head.hash)
                    {
                        return Err(SubscriberError::Provider(format!(
                            "hybrid canonical parent mismatch at block {}",
                            block.number
                        )));
                    }
                }
                if let Some(known) = history
                    .iter_mut()
                    .find(|known| known.number == block.number)
                {
                    *known = compatible_block_ref(known, block)?;
                } else {
                    history.push(*block);
                    history.sort_by_key(|known| known.number);
                }
                *coverage_head = Some(match coverage_head.as_ref() {
                    Some(head) if head.number == block.number => compatible_block_ref(head, block)?,
                    _ => *block,
                });
            }
        }
        if history.len() > capacity {
            let excess = history.len() - capacity;
            history.drain(..excess);
        }
    }
    Ok(())
}

fn encode_hybrid_checkpoint(state: &HybridCheckpointV5) -> Result<Vec<u8>, SubscriberError> {
    codec::encode_hybrid_checkpoint_v5(state)
}

#[cfg(test)]
fn encode_hybrid_checkpoint_with_limit(
    state: &HybridCheckpointV5,
    payload_limit: usize,
) -> Result<Vec<u8>, SubscriberError> {
    codec::encode_hybrid_checkpoint_v5_with_limit(state, payload_limit)
}

fn decode_hybrid_checkpoint(bytes: &[u8]) -> Result<HybridCheckpointV5, SubscriberError> {
    codec::decode_hybrid_checkpoint_v5(bytes)
}

fn validate_checkpoint_state(state: &HybridCheckpointV5) -> Result<(), SubscriberError> {
    if state.next_synthetic_token == 0 {
        return Err(SubscriberError::Provider(
            "hybrid checkpoint synthetic token sequence must be non-zero".into(),
        ));
    }
    if state.lifecycle_generation == 0 {
        return Err(SubscriberError::Provider(
            "hybrid checkpoint lifecycle generation must be non-zero".into(),
        ));
    }
    if !state.lifecycle_intent.owners.is_empty()
        && state.lifecycle_intent.base != empty_interest_fingerprint()
    {
        return Err(SubscriberError::Provider(
            "hybrid checkpoint combines base/unowned and owner-managed interests; mixed topology cannot be restored atomically"
                .into(),
        ));
    }
    if state
        .owner_generations
        .values()
        .any(|generation| *generation == 0 || *generation > state.lifecycle_generation)
    {
        return Err(SubscriberError::Provider(
            "hybrid checkpoint contains an invalid owner generation".into(),
        ));
    }
    if state
        .owner_generations
        .keys()
        .ne(state.lifecycle_intent.owners.keys())
    {
        return Err(SubscriberError::Provider(
            "hybrid checkpoint owner generations do not match its lifecycle intent".into(),
        ));
    }
    let mut inputs = HashSet::with_capacity(state.recent_inputs.len().min(65_536));
    if state
        .recent_inputs
        .iter()
        .any(|entry| !inputs.insert(entry.identity))
    {
        return Err(SubscriberError::Provider(
            "hybrid checkpoint contains duplicate recent input identities".into(),
        ));
    }
    for entry in &state.recent_inputs {
        let chain_id = input_ref_chain_id(entry.identity.input_ref());
        if chain_id != Some(state.chain_id) {
            return Err(SubscriberError::Provider(
                "hybrid checkpoint contains a recent identity for the wrong or missing chain"
                    .into(),
            ));
        }
        let witness = entry.coverage.witness.as_ref().ok_or_else(|| {
            SubscriberError::Provider(
                "hybrid checkpoint recent-input coverage is missing its payload witness".into(),
            )
        })?;
        if witness.chain_id != state.chain_id {
            return Err(SubscriberError::Provider(
                "hybrid checkpoint payload witness targets the wrong chain".into(),
            ));
        }
        if entry.coverage.owners.iter().any(|(owner, generation)| {
            *generation == 0
                || *generation > state.lifecycle_generation
                || state
                    .owner_generations
                    .get(owner)
                    .is_none_or(|current| generation > current)
        }) {
            return Err(SubscriberError::Provider(
                "hybrid checkpoint recent-input coverage has an invalid owner generation".into(),
            ));
        }
        match entry.identity.kind() {
            ReactiveInputKind::CanonicalLog | ReactiveInputKind::BlockHeader => {
                if !matches!(
                    witness.lifecycle,
                    WitnessLifecycle::Included
                        | WitnessLifecycle::Safe
                        | WitnessLifecycle::Finalized
                ) || witness.block.is_none()
                {
                    return Err(SubscriberError::Provider(
                        "hybrid checkpoint canonical input witness has an invalid lifecycle".into(),
                    ));
                }
            }
            ReactiveInputKind::ReorgSignalLog => {
                if witness.lifecycle != WitnessLifecycle::Reorg {
                    return Err(SubscriberError::Provider(
                        "hybrid checkpoint reorg input witness has an invalid lifecycle".into(),
                    ));
                }
            }
            ReactiveInputKind::PendingTxHash => {
                if witness.lifecycle != WitnessLifecycle::Pending || witness.block.is_some() {
                    return Err(SubscriberError::Provider(
                        "hybrid checkpoint pending input witness has an invalid lifecycle".into(),
                    ));
                }
            }
            ReactiveInputKind::FullBlock | ReactiveInputKind::PendingTx => {
                return Err(SubscriberError::Provider(
                    "hybrid checkpoint contains a representation without a complete payload proof"
                        .into(),
                ));
            }
            _ => {
                return Err(SubscriberError::Provider(
                    "hybrid checkpoint contains an unknown input representation".into(),
                ));
            }
        }
        if let (Some(coverage_block), Some(witness_block)) =
            (entry.coverage.block.as_ref(), witness.block.as_ref())
        {
            compatible_block_ref(coverage_block, witness_block)?;
        }
        match entry.identity.input_ref() {
            InputRef::Log { block_hash, .. } => {
                if witness
                    .block
                    .as_ref()
                    .is_some_and(|block| block.hash != block_hash)
                {
                    return Err(SubscriberError::Provider(
                        "hybrid checkpoint log identity conflicts with its witness block".into(),
                    ));
                }
            }
            InputRef::Block { hash, number, .. } => {
                if witness
                    .block
                    .as_ref()
                    .is_none_or(|block| block.hash != hash || block.number != number)
                {
                    return Err(SubscriberError::Provider(
                        "hybrid checkpoint block identity conflicts with its witness".into(),
                    ));
                }
            }
            InputRef::PendingTx { .. } => {}
        }
        if let Some(block) = entry.coverage.block.as_ref() {
            if state
                .coverage_head
                .as_ref()
                .is_none_or(|head| block.number > head.number)
            {
                return Err(SubscriberError::Provider(
                    "hybrid checkpoint recent-input coverage is ahead of canonical coverage".into(),
                ));
            }
            if let Some(known) = state
                .canonical_history
                .iter()
                .find(|known| known.number == block.number)
            {
                compatible_block_ref(known, block)?;
            }
        }
    }
    validate_checkpoint_history(
        "coordinator",
        &state.canonical_history,
        state.coverage_head.as_ref(),
    )?;
    validate_checkpoint_history(
        "historical source",
        &state.historical_position.canonical_history,
        state.historical_position.coverage_head.as_ref(),
    )?;
    validate_checkpoint_history(
        "live source",
        &state.live_position.canonical_history,
        state.live_position.coverage_head.as_ref(),
    )?;
    CanonicalSequenceState::new(
        state.canonical_history.clone(),
        state.coverage_head,
        state.safe_head,
        state.finalized_head,
    )
    .validate()
    .map_err(|error| {
        SubscriberError::Provider(format!(
            "hybrid checkpoint canonical sequence is invalid: {error}"
        ))
    })?;
    if let Some(proof) = state.certified_historical {
        if proof.lifecycle_generation != state.lifecycle_generation {
            return Err(SubscriberError::Provider(
                "hybrid checkpoint historical coverage proof belongs to a different lifecycle generation"
                    .into(),
            ));
        }
        for (label, head) in [
            ("coordinator", state.coverage_head),
            ("historical source", state.historical_position.coverage_head),
        ] {
            let head = head.ok_or_else(|| {
                SubscriberError::Provider(format!(
                    "hybrid checkpoint historical coverage proof has no {label} coverage head"
                ))
            })?;
            if proof.through.number > head.number {
                return Err(SubscriberError::Provider(format!(
                    "hybrid checkpoint historical coverage proof is ahead of the {label}"
                )));
            }
            if proof.through.number == head.number {
                compatible_block_ref(&proof.through, &head)?;
            }
        }
        if let Some(known) = state
            .historical_position
            .canonical_history
            .iter()
            .find(|known| known.number == proof.through.number)
        {
            compatible_block_ref(known, &proof.through)?;
        }
    }
    validate_source_against_coordinator("historical source", &state.historical_position, state)?;
    validate_source_against_coordinator("live source", &state.live_position, state)?;
    if let Some(committed) = state.last_committed_token.as_ref() {
        if committed.inner.is_empty() {
            return Err(SubscriberError::Provider(
                "hybrid checkpoint contains an empty committed token".into(),
            ));
        }
        let source = match committed.source {
            HybridSource::Historical => &state.historical_position,
            HybridSource::Live => &state.live_position,
        };
        if source.delivery_digest.is_none() {
            return Err(SubscriberError::Provider(
                "hybrid committed token is missing its source delivery digest".into(),
            ));
        }
        match committed.kind {
            HybridTokenKind::Forwarded
                if source.delivery_token.as_deref() != Some(committed.inner.as_slice()) =>
            {
                return Err(SubscriberError::Provider(
                    "hybrid checkpoint committed token does not match its source position".into(),
                ));
            }
            HybridTokenKind::Synthetic if source.delivery_token.is_some() => {
                return Err(SubscriberError::Provider(
                    "hybrid synthetic commit retained a stale child delivery token".into(),
                ));
            }
            HybridTokenKind::Forwarded | HybridTokenKind::Synthetic => {}
        }
    }
    Ok(())
}

fn validate_checkpoint_history(
    label: &str,
    history: &[BlockRef],
    coverage_head: Option<&BlockRef>,
) -> Result<(), SubscriberError> {
    if history.is_empty() {
        if coverage_head.is_some() {
            return Err(SubscriberError::Provider(format!(
                "hybrid {label} checkpoint has coverage without retained canonical history"
            )));
        }
        return Ok(());
    }
    if coverage_head.is_none() {
        return Err(SubscriberError::Provider(format!(
            "hybrid {label} checkpoint history has no coverage head"
        )));
    }
    for pair in history.windows(2) {
        let [previous, block] = pair else {
            unreachable!("window has exactly two entries")
        };
        if block.number <= previous.number {
            return Err(SubscriberError::Provider(format!(
                "hybrid {label} checkpoint history is not strictly ordered"
            )));
        }
        if block.number == previous.number.saturating_add(1)
            && block
                .parent_hash
                .is_some_and(|parent| parent != previous.hash)
        {
            return Err(SubscriberError::Provider(format!(
                "hybrid {label} checkpoint history has a parent-hash discontinuity at block {}",
                block.number
            )));
        }
    }
    let head = coverage_head.expect("checked above");
    let last = history.last().expect("non-empty history");
    if compatible_block_ref(last, head).is_err() {
        return Err(SubscriberError::Provider(format!(
            "hybrid {label} checkpoint history does not end at its coverage head"
        )));
    }
    Ok(())
}

fn validate_source_against_coordinator(
    label: &str,
    source: &SourcePosition,
    state: &HybridCheckpointV5,
) -> Result<(), SubscriberError> {
    if source.delivery_token.is_some() && source.delivery_digest.is_none() {
        return Err(SubscriberError::Provider(format!(
            "hybrid {label} has a delivery token without its payload digest"
        )));
    }
    if source.delivery_token.is_some() && source.coverage_head.is_none() {
        return Err(SubscriberError::Provider(format!(
            "hybrid {label} has a forwarded delivery token without restorable canonical coverage"
        )));
    }
    if let Some(source_head) = source.coverage_head.as_ref() {
        let coordinator_head = state.coverage_head.as_ref().ok_or_else(|| {
            SubscriberError::Provider(format!(
                "hybrid {label} has coverage without coordinator coverage"
            ))
        })?;
        if source_head.number > coordinator_head.number {
            return Err(SubscriberError::Provider(format!(
                "hybrid {label} coverage is ahead of coordinator coverage"
            )));
        }
        if let Some(known) = state
            .canonical_history
            .iter()
            .find(|known| known.number == source_head.number)
        {
            compatible_block_ref(known, source_head)?;
        }
    }
    Ok(())
}

fn compatible_block_ref(left: &BlockRef, right: &BlockRef) -> Result<BlockRef, SubscriberError> {
    if left.number != right.number || left.hash != right.hash {
        return Err(SubscriberError::Provider(format!(
            "hybrid canonical block identity conflicts at height {}",
            left.number
        )));
    }
    let parent_hash = merge_optional_metadata(
        left.parent_hash,
        right.parent_hash,
        "parent hash",
        left.number,
    )?;
    let timestamp =
        merge_optional_metadata(left.timestamp, right.timestamp, "timestamp", left.number)?;
    Ok(BlockRef {
        number: left.number,
        hash: left.hash,
        parent_hash,
        timestamp,
    })
}

fn merge_optional_metadata<T: Copy + PartialEq>(
    left: Option<T>,
    right: Option<T>,
    label: &str,
    block_number: u64,
) -> Result<Option<T>, SubscriberError> {
    match (left, right) {
        (Some(left), Some(right)) if left != right => Err(SubscriberError::Provider(format!(
            "hybrid canonical {label} conflicts at block {block_number}"
        ))),
        (Some(value), _) | (_, Some(value)) => Ok(Some(value)),
        (None, None) => Ok(None),
    }
}

fn input_ref_chain_id(input_ref: InputRef) -> Option<u64> {
    match input_ref {
        InputRef::Log { chain_id, .. }
        | InputRef::PendingTx { chain_id, .. }
        | InputRef::Block { chain_id, .. } => chain_id,
    }
}

fn source_delivery_digest<N: Network>(
    records: &[RoutedRecord<N>],
    controls: &[ChainControl],
    checkpoint: Option<&SubscriberCheckpoint>,
    payload_commitment: Option<&SubscriberPayloadCommitment>,
    expected_chain_id: u64,
) -> Result<[u8; 32], SubscriberError> {
    let mut hasher = CanonicalHasher::new(b"EFCHY-SOURCE-DELIVERY-V1");
    hasher.u64(expected_chain_id);
    hasher
        .sequence_len(records.len())
        .map_err(transcript_error)?;
    for (record, audience, scope) in records {
        let (identity, witness) = validated_record(record, expected_chain_id)?;
        append_input_identity(&mut hasher, identity)?;
        append_record_witness(&mut hasher, &witness);
        append_exact_context(&mut hasher, &record.context)?;
        append_audience(&mut hasher, audience)?;
        append_scope(&mut hasher, *scope)?;
    }
    hasher
        .sequence_len(controls.len())
        .map_err(transcript_error)?;
    for control in controls {
        append_chain_control(&mut hasher, control)?;
    }
    match checkpoint {
        Some(checkpoint) => {
            hasher.bool(true);
            hasher
                .bytes(checkpoint.as_bytes())
                .map_err(transcript_error)?;
        }
        None => hasher.bool(false),
    }
    hasher.bool(payload_commitment.is_some());
    if let Some(commitment) = payload_commitment {
        hasher.hash(&commitment.digest());
    }
    Ok(hasher.finish())
}

fn append_input_identity(
    hasher: &mut CanonicalHasher,
    identity: ReactiveInputIdentity,
) -> Result<(), SubscriberError> {
    match identity.input_ref() {
        InputRef::Log {
            chain_id,
            block_hash,
            transaction_hash,
            log_index,
        } => {
            hasher.tag(1);
            hasher.option_u64(chain_id);
            hasher.hash(&block_hash);
            hasher.hash(&transaction_hash);
            hasher.u64(log_index);
        }
        InputRef::PendingTx { chain_id, hash } => {
            hasher.tag(2);
            hasher.option_u64(chain_id);
            hasher.hash(&hash);
        }
        InputRef::Block {
            chain_id,
            hash,
            number,
        } => {
            hasher.tag(3);
            hasher.option_u64(chain_id);
            hasher.hash(&hash);
            hasher.u64(number);
        }
    }
    hasher.tag(match identity.kind() {
        ReactiveInputKind::CanonicalLog => 1,
        ReactiveInputKind::ReorgSignalLog => 2,
        ReactiveInputKind::BlockHeader => 3,
        ReactiveInputKind::FullBlock => 4,
        ReactiveInputKind::PendingTxHash => 5,
        ReactiveInputKind::PendingTx => 6,
        _ => {
            return Err(SubscriberError::Unsupported(
                "hybrid cannot transcript an unknown reactive-input kind",
            ));
        }
    });
    Ok(())
}

fn append_record_witness(hasher: &mut CanonicalHasher, witness: &RecordWitness) {
    hasher
        .bytes(&witness.payload_digest)
        .expect("fixed digest length");
    hasher.u64(witness.chain_id);
    hasher.tag(match witness.lifecycle {
        WitnessLifecycle::Included => 1,
        WitnessLifecycle::Safe => 2,
        WitnessLifecycle::Finalized => 3,
        WitnessLifecycle::Reorg => 4,
        WitnessLifecycle::Pending => 5,
    });
    hasher.option_block_ref(witness.block.as_ref());
    hasher.option_u64(witness.transaction_index);
    hasher.option_u64(witness.log_index);
    hasher.option_u64(witness.log_block_timestamp);
}

fn append_exact_context(
    hasher: &mut CanonicalHasher,
    context: &ReactiveContext,
) -> Result<(), SubscriberError> {
    hasher.option_u64(context.chain_id);
    hasher.tag(match context.source {
        InputSource::Batch => 1,
        InputSource::Subscription => 2,
        InputSource::Poll => 3,
        InputSource::Backfill => 4,
        InputSource::Synthetic => 5,
        _ => {
            return Err(SubscriberError::Unsupported(
                "hybrid cannot transcript an unknown input source",
            ));
        }
    });
    match &context.chain_status {
        ChainStatus::Pending => hasher.tag(1),
        ChainStatus::Included {
            block,
            confirmations,
        } => {
            hasher.tag(2);
            hasher.block_ref(block);
            hasher.u64(*confirmations);
        }
        ChainStatus::Safe { block } => {
            hasher.tag(3);
            hasher.block_ref(block);
        }
        ChainStatus::Finalized { block } => {
            hasher.tag(4);
            hasher.block_ref(block);
        }
        ChainStatus::Reorged { dropped_from } => {
            hasher.tag(5);
            hasher.block_ref(dropped_from);
        }
        _ => {
            return Err(SubscriberError::Unsupported(
                "hybrid cannot transcript an unknown chain status",
            ));
        }
    }
    hasher.option_block_ref(context.block.as_ref());
    hasher.option_u64(context.transaction_index);
    hasher.option_u64(context.log_index);
    Ok(())
}

fn append_audience(
    hasher: &mut CanonicalHasher,
    audience: &DeliveryAudience,
) -> Result<(), SubscriberError> {
    let (tag, owners) = match audience {
        DeliveryAudience::All => (1, None),
        DeliveryAudience::Owners(owners) => (2, Some(owners)),
        DeliveryAudience::AllExcept(owners) => (3, Some(owners)),
        _ => {
            return Err(SubscriberError::Unsupported(
                "hybrid cannot transcript an unknown delivery audience",
            ));
        }
    };
    hasher.tag(tag);
    if let Some(owners) = owners {
        hasher
            .sequence_len(owners.len())
            .map_err(transcript_error)?;
        for owner in owners {
            hasher.handler_id(owner).map_err(transcript_error)?;
        }
    }
    Ok(())
}

fn append_scope(hasher: &mut CanonicalHasher, scope: DeliveryScope) -> Result<(), SubscriberError> {
    hasher.tag(match scope {
        DeliveryScope::Canonical => 1,
        DeliveryScope::CanonicalProgress => 2,
        DeliveryScope::OwnerCatchup => 3,
        _ => {
            return Err(SubscriberError::Unsupported(
                "hybrid cannot transcript an unknown delivery scope",
            ));
        }
    });
    Ok(())
}

fn append_chain_control(
    hasher: &mut CanonicalHasher,
    control: &ChainControl,
) -> Result<(), SubscriberError> {
    match control {
        ChainControl::Reorg {
            common_ancestor,
            old_tip,
            new_tip,
        } => {
            hasher.tag(1);
            hasher.block_ref(common_ancestor);
            hasher.block_ref(old_tip);
            hasher.block_ref(new_tip);
        }
        ChainControl::CanonicalProgress(block) => {
            hasher.tag(2);
            hasher.block_ref(block);
        }
        ChainControl::Safe(block) => {
            hasher.tag(3);
            hasher.block_ref(block);
        }
        ChainControl::Finalized(block) => {
            hasher.tag(4);
            hasher.block_ref(block);
        }
        ChainControl::Barrier { id, block } => {
            hasher.tag(5);
            hasher.bytes(id).map_err(transcript_error)?;
            hasher.option_block_ref(block.as_ref());
        }
        _ => {
            return Err(SubscriberError::Unsupported(
                "hybrid cannot transcript an unknown chain control",
            ));
        }
    }
    Ok(())
}

fn transcript_error(error: transcript::TranscriptLengthError) -> SubscriberError {
    SubscriberError::Provider(error.to_string())
}

fn validated_record<N: Network>(
    record: &ReactiveInputRecord<N>,
    expected_chain_id: u64,
) -> Result<(ReactiveInputIdentity, RecordWitness), SubscriberError> {
    if matches!(
        record.input,
        ReactiveInput::FullBlock(_) | ReactiveInput::PendingTx(_)
    ) {
        return Err(SubscriberError::Unsupported(
            "hybrid delivery does not accept full blocks or hydrated pending transactions until their complete bodies can be verified generically",
        ));
    }
    if record.context.chain_id != Some(expected_chain_id) {
        return Err(SubscriberError::Provider(format!(
            "hybrid source record has chain {:?}; expected {expected_chain_id}",
            record.context.chain_id
        )));
    }
    let identity = record
        .validated_identity()
        .map_err(|error| SubscriberError::Provider(error.to_string()))?;
    if input_ref_chain_id(identity.input_ref()) != Some(expected_chain_id) {
        return Err(SubscriberError::Provider(
            "hybrid validated input identity has a wrong or missing chain id".into(),
        ));
    }
    let witness = record_witness(record, expected_chain_id)?;
    Ok((identity, witness))
}

fn record_witness<N: Network>(
    record: &ReactiveInputRecord<N>,
    expected_chain_id: u64,
) -> Result<RecordWitness, SubscriberError> {
    let identity = record
        .validated_identity()
        .map_err(|error| SubscriberError::Provider(error.to_string()))?;
    let lifecycle = match &record.context.chain_status {
        ChainStatus::Pending => WitnessLifecycle::Pending,
        ChainStatus::Included { .. } => WitnessLifecycle::Included,
        ChainStatus::Safe { .. } => WitnessLifecycle::Safe,
        ChainStatus::Finalized { .. } => WitnessLifecycle::Finalized,
        ChainStatus::Reorged { .. } => WitnessLifecycle::Reorg,
        _ => {
            return Err(SubscriberError::Provider(
                "hybrid does not support an unknown chain lifecycle variant".into(),
            ));
        }
    };
    let mut payload = CanonicalHasher::new(b"EFCHY-RECORD-PAYLOAD-V1");
    append_input_identity(&mut payload, identity)?;
    let log_block_timestamp = match &record.input {
        ReactiveInput::Log(log) => {
            payload.tag(1);
            payload
                .bytes(log.inner.address.as_slice())
                .map_err(transcript_error)?;
            payload
                .sequence_len(log.topics().len())
                .map_err(transcript_error)?;
            for topic in log.topics() {
                payload.hash(topic);
            }
            payload
                .bytes(&log.inner.data.data)
                .map_err(transcript_error)?;
            payload.hash(log.block_hash.as_ref().expect("validated log block hash"));
            payload.u64(log.block_number.expect("validated log block"));
            payload.hash(
                log.transaction_hash
                    .as_ref()
                    .expect("validated log transaction hash"),
            );
            payload.u64(log.transaction_index.expect("validated transaction index"));
            payload.u64(log.log_index.expect("validated log index"));
            payload.bool(log.removed);
            log.block_timestamp
        }
        ReactiveInput::BlockHeader(header) => {
            payload.tag(2);
            let (header_digest, _) = header_body_digest(header, usize::MAX)?;
            payload.hash(&header_digest);
            None
        }
        ReactiveInput::PendingTxHash(hash) => {
            payload.tag(3);
            payload.hash(hash);
            None
        }
        ReactiveInput::FullBlock(_) | ReactiveInput::PendingTx(_) => None,
    };
    Ok(RecordWitness {
        payload_digest: payload.finish(),
        chain_id: expected_chain_id,
        lifecycle,
        block: record.context.block,
        transaction_index: record.context.transaction_index,
        log_index: record.context.log_index,
        log_block_timestamp,
    })
}

fn ensure_witness_compatible(
    left: &RecordWitness,
    right: &RecordWitness,
) -> Result<(), SubscriberError> {
    let mut merged = left.clone();
    merge_witness(&mut merged, right)
}

fn merge_witness(left: &mut RecordWitness, right: &RecordWitness) -> Result<(), SubscriberError> {
    if left.payload_digest != right.payload_digest
        || left.chain_id != right.chain_id
        || left.transaction_index != right.transaction_index
        || left.log_index != right.log_index
    {
        return Err(SubscriberError::Provider(
            "hybrid duplicate identity carries conflicting payload or required positions".into(),
        ));
    }
    left.lifecycle = merge_witness_lifecycle(left.lifecycle, right.lifecycle)?;
    left.block = match (left.block.as_ref(), right.block.as_ref()) {
        (Some(left), Some(right)) => Some(compatible_block_ref(left, right)?),
        (Some(left), None) => Some(*left),
        (None, Some(right)) => Some(*right),
        (None, None) => None,
    };
    left.log_block_timestamp = merge_optional_metadata(
        left.log_block_timestamp,
        right.log_block_timestamp,
        "log timestamp",
        left.block.as_ref().map_or(0, |block| block.number),
    )?;
    Ok(())
}

fn merge_witness_lifecycle(
    left: WitnessLifecycle,
    right: WitnessLifecycle,
) -> Result<WitnessLifecycle, SubscriberError> {
    use WitnessLifecycle::{Finalized, Included, Pending, Reorg, Safe};
    match (left, right) {
        (Pending, Pending) => Ok(Pending),
        (Reorg, Reorg) => Ok(Reorg),
        (Included | Safe | Finalized, Included | Safe | Finalized) => Ok(match (left, right) {
            (Finalized, _) | (_, Finalized) => Finalized,
            (Safe, _) | (_, Safe) => Safe,
            _ => Included,
        }),
        _ => Err(SubscriberError::Provider(
            "hybrid duplicate identity carries conflicting lifecycle context".into(),
        )),
    }
}

fn truncate_front_to<T>(values: &mut Vec<T>, capacity: usize) {
    if values.len() > capacity {
        values.drain(..values.len() - capacity);
    }
}

fn fresh_epoch() -> [u8; 16] {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let nonce = NEXT_EPOCH_NONCE.fetch_add(1, Ordering::Relaxed);
    let mut seed = Vec::with_capacity(16 + 4 + 8);
    seed.extend_from_slice(&now.to_be_bytes());
    seed.extend_from_slice(&std::process::id().to_be_bytes());
    seed.extend_from_slice(&nonce.to_be_bytes());
    let hash = keccak256(seed);
    hash[..16].try_into().expect("fixed hash prefix")
}

fn source_resume_position(
    chain_id: u64,
    source: &SourcePosition,
) -> Option<SubscriberResumePosition> {
    let coverage_head = source.coverage_head?;
    let canonical_history = if source.canonical_history.is_empty() {
        vec![coverage_head]
    } else {
        source.canonical_history.clone()
    };
    Some(SubscriberResumePosition::new(
        chain_id,
        coverage_head,
        canonical_history,
        source
            .delivery_token
            .clone()
            .map(SubscriberDeliveryToken::new),
        source.checkpoint.clone().map(SubscriberCheckpoint::new),
    ))
}

async fn reconcile_pending_owner_lifecycle<H, L, N>(
    historical: &mut H,
    live: &mut L,
    pending: &mut Option<LiveRollback<N>>,
    phase: &mut HybridPhase,
    poisoned: &mut Option<String>,
) -> Result<(), SubscriberError>
where
    H: InterestOwnerSubscriber<N>,
    L: InterestOwnerSubscriber<N>,
    N: Network,
{
    let Some(rollback) = pending.clone() else {
        return Ok(());
    };
    if matches!(rollback, LiveRollback::Recovery { .. }) {
        return Err(SubscriberError::Provider(
            "hybrid destructive lifecycle compensation is awaiting historical gap certification; poll the coordinator until it returns to Live before retrying registration"
                .into(),
        ));
    }
    let historical_result = apply_historical_owner_lifecycle_rollback(historical, &rollback).await;
    let live_result = apply_owner_lifecycle_rollback(live, &rollback).await;
    if historical_result.is_ok() && live_result.is_ok() {
        if let LiveRollback::Topology {
            base,
            owners,
            recovery_anchor: Some(anchor),
            ..
        } = &rollback
            && base.is_empty()
            && !owners.is_empty()
        {
            *pending = Some(LiveRollback::Recovery { anchor: *anchor });
            return Err(SubscriberError::Provider(
                "hybrid previous topology was restored after a destructive live reset; historical gap certification is required before Live"
                    .into(),
            ));
        }
        if let Some(anchor) = rollback.gap_anchor() {
            let scope = rollback.gap_scope();
            *pending = None;
            return Err(poison_uncertifiable_lifecycle_gap(
                phase, poisoned, anchor, scope,
            ));
        }
        *pending = None;
        return Ok(());
    }
    Err(SubscriberError::Provider(format!(
        "hybrid lifecycle rollback remains pending (historical: {}; live: {})",
        result_description(&historical_result),
        result_description(&live_result),
    )))
}

async fn apply_owner_lifecycle_rollback<S, N>(
    subscriber: &mut S,
    rollback: &LiveRollback<N>,
) -> Result<(), SubscriberError>
where
    S: InterestOwnerSubscriber<N>,
    N: Network,
{
    match rollback {
        LiveRollback::Base { previous, .. } => subscriber.register_interests(previous).await,
        LiveRollback::Topology { base, owners, .. } => {
            if !base.is_empty() && !owners.is_empty() {
                return Err(SubscriberError::InvalidConfig(EXCLUSIVE_TOPOLOGY_ERROR));
            }
            if base.is_empty() {
                subscriber.replace_interest_owners(owners.clone()).await
            } else {
                subscriber.register_interests(base).await
            }
        }
        LiveRollback::Owner {
            owner, previous, ..
        } => rollback_owner(subscriber, owner, previous.as_deref()).await,
        LiveRollback::Bulk { previous, .. } => rollback_bulk(subscriber, previous).await,
        LiveRollback::Recovery { .. } => Err(SubscriberError::Provider(
            "hybrid lifecycle recovery cannot be applied as a child topology mutation".into(),
        )),
    }
}

async fn apply_historical_owner_lifecycle_rollback<S, N>(
    subscriber: &mut S,
    rollback: &LiveRollback<N>,
) -> Result<(), SubscriberError>
where
    S: InterestOwnerSubscriber<N>,
    N: Network,
{
    match rollback {
        LiveRollback::Topology {
            base,
            owners,
            recovery_anchor: Some(anchor),
            ..
        } if base.is_empty() && !owners.is_empty() => {
            let backfill = SubscriberBackfill::after_canonical_block(*anchor)?;
            subscriber
                .replace_interest_owners_with_global_backfill(owners.clone(), backfill)
                .await
        }
        LiveRollback::Recovery { .. } => Err(SubscriberError::Provider(
            "hybrid historical lifecycle recovery is already staged".into(),
        )),
        _ => apply_owner_lifecycle_rollback(subscriber, rollback).await,
    }
}

fn exact_owner_topology<N: Network>(
    owners: &[(HandlerId, Vec<ReactiveInterest<N>>)],
    operation: &'static str,
) -> Result<HashMap<HandlerId, Vec<ReactiveInterest<N>>>, SubscriberError> {
    let mut exact = HashMap::with_capacity(owners.len());
    for (owner, interests) in owners {
        if exact.insert(owner.clone(), interests.clone()).is_some() {
            return Err(SubscriberError::InvalidConfig(operation));
        }
    }
    Ok(exact)
}

fn sorted_owner_entries<N: Network>(
    owners: &HashMap<HandlerId, Vec<ReactiveInterest<N>>>,
) -> Vec<(HandlerId, Vec<ReactiveInterest<N>>)> {
    let mut entries = owners
        .iter()
        .map(|(owner, interests)| (owner.clone(), interests.clone()))
        .collect::<Vec<_>>();
    entries.sort_by(|(left, _), (right, _)| left.cmp(right));
    entries
}

async fn reconcile_pending_base_lifecycle<H, L, N>(
    historical: &mut H,
    live: &mut L,
    pending: &mut Option<LiveRollback<N>>,
    phase: &mut HybridPhase,
    poisoned: &mut Option<String>,
) -> Result<(), SubscriberError>
where
    H: EventSubscriber<N>,
    L: EventSubscriber<N>,
    N: Network,
{
    let Some(LiveRollback::Base {
        previous,
        gap_anchor,
    }) = pending.as_ref()
    else {
        return if pending.is_none() {
            Ok(())
        } else {
            Err(SubscriberError::Provider(
                "hybrid owner lifecycle is pending; retry an owner lifecycle operation".into(),
            ))
        };
    };
    let previous = previous.clone();
    let gap_anchor = *gap_anchor;
    let historical_result = historical.register_interests(&previous).await;
    let live_result = live.register_interests(&previous).await;
    if historical_result.is_ok() && live_result.is_ok() {
        if let Some(anchor) = gap_anchor {
            *pending = None;
            return Err(poison_uncertifiable_lifecycle_gap(
                phase,
                poisoned,
                anchor,
                "base subscription",
            ));
        }
        *pending = None;
        return Ok(());
    }
    Err(SubscriberError::Provider(format!(
        "hybrid base lifecycle rollback remains pending (historical: {}; live: {})",
        result_description(&historical_result),
        result_description(&live_result),
    )))
}

fn poison_uncertifiable_lifecycle_gap(
    phase: &mut HybridPhase,
    poisoned: &mut Option<String>,
    anchor: BlockRef,
    scope: &str,
) -> SubscriberError {
    let reason = format!(
        "{scope} changed on the live source after acknowledged coverage at block {}, but the durable historical commit did not complete; ordinary compensation restored the prior filters without proving the intervening live mutation window is free of an event gap. Reconstruct the coordinator from an authoritative checkpoint",
        anchor.number
    );
    *phase = HybridPhase::Poisoned;
    *poisoned = Some(reason.clone());
    SubscriberError::Provider(format!("hybrid coordinator is poisoned: {reason}"))
}

fn result_description(result: &Result<(), SubscriberError>) -> String {
    match result {
        Ok(()) => "restored".into(),
        Err(error) => error.to_string(),
    }
}

async fn rollback_bulk<S, N>(
    subscriber: &mut S,
    previous: &[(HandlerId, Option<Vec<ReactiveInterest<N>>>)],
) -> Result<(), SubscriberError>
where
    S: InterestOwnerSubscriber<N>,
    N: Network,
{
    let restore = previous
        .iter()
        .filter_map(|(owner, interests)| {
            interests
                .as_ref()
                .map(|interests| (owner.clone(), interests.clone()))
        })
        .collect::<Vec<_>>();
    if !restore.is_empty() {
        subscriber.upsert_interest_owners(restore).await?;
    }
    for (owner, interests) in previous {
        if interests.is_none() {
            subscriber.remove_interest_owner(owner).await?;
        }
    }
    Ok(())
}

async fn rollback_owner<S, N>(
    subscriber: &mut S,
    owner: &HandlerId,
    previous: Option<&[ReactiveInterest<N>]>,
) -> Result<(), SubscriberError>
where
    S: InterestOwnerSubscriber<N>,
    N: Network,
{
    if let Some(previous) = previous {
        subscriber.add_interest_owner(owner.clone(), previous).await
    } else {
        subscriber.remove_interest_owner(owner).await.map(|_| ())
    }
}

fn canonical_block_number<N: Network>(record: &ReactiveInputRecord<N>) -> Option<u64> {
    canonical_block_ref(record).map(|block| block.number)
}

fn canonical_block_ref<N: Network>(record: &ReactiveInputRecord<N>) -> Option<&BlockRef> {
    match &record.context.chain_status {
        ChainStatus::Included { block, .. }
        | ChainStatus::Safe { block }
        | ChainStatus::Finalized { block } => Some(block),
        ChainStatus::Pending | ChainStatus::Reorged { .. } => None,
        _ => None,
    }
}

fn dropped_block_ref<N: Network>(record: &ReactiveInputRecord<N>) -> Option<&BlockRef> {
    match &record.context.chain_status {
        ChainStatus::Reorged { dropped_from } => Some(dropped_from),
        _ if matches!(&record.input, ReactiveInput::Log(log) if log.removed) => {
            record.context.block.as_ref()
        }
        _ => None,
    }
}

fn coverage_record_block_number<N: Network>(record: &ReactiveInputRecord<N>) -> Option<u64> {
    canonical_block_ref(record).map(|block| block.number)
}

fn coverage_control_block_number(control: &ChainControl) -> Option<u64> {
    match control {
        ChainControl::CanonicalProgress(block) => Some(block.number),
        ChainControl::Barrier { block, .. } => block.as_ref().map(|block| block.number),
        _ => None,
    }
}

fn scope_advances_canonical(scope: DeliveryScope) -> bool {
    matches!(
        scope,
        DeliveryScope::Canonical | DeliveryScope::CanonicalProgress
    )
}

struct BoundedHeaderBuffer {
    bytes: Vec<u8>,
    encoded_limit: usize,
    limit_exceeded: bool,
}

impl BoundedHeaderBuffer {
    fn new(encoded_limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            encoded_limit,
            limit_exceeded: false,
        }
    }
}

impl std::io::Write for BoundedHeaderBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let Some(next_len) = self.bytes.len().checked_add(bytes.len()) else {
            self.limit_exceeded = true;
            return Err(std::io::Error::other(
                "serialized block-header length overflow",
            ));
        };
        if next_len > self.encoded_limit {
            self.limit_exceeded = true;
            return Err(std::io::Error::other(
                "serialized block header exceeds ingress byte bound",
            ));
        }
        self.bytes
            .try_reserve(bytes.len())
            .map_err(|_| std::io::Error::other("serialized block-header allocation failed"))?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct HeaderDigestWriter {
    hasher: Keccak256,
    encoded_len: usize,
    encoded_limit: usize,
    limit_exceeded: bool,
}

fn write_canonical_json(
    value: &serde_json::Value,
    writer: &mut HeaderDigestWriter,
) -> std::io::Result<()> {
    use std::io::Write as _;

    match value {
        serde_json::Value::Array(values) => {
            writer.write_all(b"[")?;
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    writer.write_all(b",")?;
                }
                write_canonical_json(value, writer)?;
            }
            writer.write_all(b"]")
        }
        serde_json::Value::Object(values) => {
            let mut entries = Vec::new();
            entries
                .try_reserve(values.len())
                .map_err(|_| std::io::Error::other("block-header key allocation failed"))?;
            entries.extend(values.iter());
            entries.sort_unstable_by_key(|(key, _)| *key);

            writer.write_all(b"{")?;
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index != 0 {
                    writer.write_all(b",")?;
                }
                serde_json::to_writer(&mut *writer, key).map_err(std::io::Error::other)?;
                writer.write_all(b":")?;
                write_canonical_json(value, writer)?;
            }
            writer.write_all(b"}")
        }
        _ => serde_json::to_writer(writer, value).map_err(std::io::Error::other),
    }
}

impl HeaderDigestWriter {
    fn new(encoded_limit: usize) -> Self {
        let mut hasher = Keccak256::new();
        hasher.update(HEADER_WITNESS_DOMAIN);
        Self {
            hasher,
            encoded_len: 0,
            encoded_limit,
            limit_exceeded: false,
        }
    }
}

impl std::io::Write for HeaderDigestWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let Some(next_len) = self.encoded_len.checked_add(bytes.len()) else {
            self.limit_exceeded = true;
            return Err(std::io::Error::other(
                "serialized block-header length overflow",
            ));
        };
        if next_len > self.encoded_limit {
            self.limit_exceeded = true;
            return Err(std::io::Error::other(
                "serialized block header exceeds ingress byte bound",
            ));
        }
        self.hasher.update(bytes);
        self.encoded_len = next_len;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn header_body_digest<T: Serialize + ?Sized>(
    header: &T,
    encoded_limit: usize,
) -> Result<(alloy_primitives::B256, usize), SubscriberError> {
    // Canonical key ordering currently requires a parsed JSON value. Keep both
    // the encoded buffer and that bounded secondary representation small even
    // when the caller's aggregate ingress budget is much larger.
    let encoded_limit = encoded_limit.min(MAX_HEADER_CANONICALIZATION_BYTES);
    let mut bounded = BoundedHeaderBuffer::new(encoded_limit);
    if let Err(error) = serde_json::to_writer(&mut bounded, header) {
        if bounded.limit_exceeded {
            return Err(SubscriberError::Provider(format!(
                "hybrid serialized block header exceeds the remaining ingress byte bound ({encoded_limit} bytes)"
            )));
        }
        return Err(SubscriberError::Provider(format!(
            "failed to encode exact hybrid block-header witness: {error}"
        )));
    }
    let value = serde_json::from_slice(&bounded.bytes).map_err(|error| {
        SubscriberError::Provider(format!(
            "failed to canonicalize exact hybrid block-header witness: {error}"
        ))
    })?;
    let mut writer = HeaderDigestWriter::new(encoded_limit);
    if let Err(error) = write_canonical_json(&value, &mut writer) {
        if writer.limit_exceeded {
            return Err(SubscriberError::Provider(format!(
                "hybrid serialized block header exceeds the remaining ingress byte bound ({encoded_limit} bytes)"
            )));
        }
        return Err(SubscriberError::Provider(format!(
            "failed to canonicalize exact hybrid block-header witness: {error}"
        )));
    }
    Ok((writer.hasher.finalize(), writer.encoded_len))
}

fn accounted_record_bytes<N: Network>(
    record: &ReactiveInputRecord<N>,
    audience: &DeliveryAudience,
    remaining: usize,
) -> Result<usize, SubscriberError> {
    const RECORD_OVERHEAD: usize = 512;
    let audience_bytes = accounted_audience_bytes(audience);
    let input_bytes = match &record.input {
        ReactiveInput::Log(log) => RECORD_OVERHEAD
            .saturating_add(log.topics().len().saturating_mul(32))
            .saturating_add(log.inner.data.data.len()),
        ReactiveInput::BlockHeader(header) => {
            let fixed = RECORD_OVERHEAD.saturating_add(audience_bytes);
            let header_limit = remaining.saturating_sub(fixed);
            let (_, encoded_len) = header_body_digest(header, header_limit)?;
            RECORD_OVERHEAD.saturating_add(encoded_len)
        }
        ReactiveInput::PendingTxHash(_) => RECORD_OVERHEAD,
        // These payloads are network-generic and can contain unbounded dynamic
        // transaction data. Hybrid currently does not advertise them as a safe
        // buffering surface, so fail the byte budget closed if a source violates
        // that contract during catch-up.
        ReactiveInput::FullBlock(_) | ReactiveInput::PendingTx(_) => usize::MAX / 2,
    };
    Ok(input_bytes.saturating_add(audience_bytes))
}

fn accounted_audience_bytes(audience: &DeliveryAudience) -> usize {
    const AUDIENCE_OVERHEAD: usize = 64;
    match audience {
        DeliveryAudience::All => AUDIENCE_OVERHEAD,
        DeliveryAudience::Owners(owners) | DeliveryAudience::AllExcept(owners) => owners
            .iter()
            .map(|owner| owner.as_str().len().saturating_add(32))
            .fold(AUDIENCE_OVERHEAD, usize::saturating_add),
        _ => usize::MAX / 2,
    }
}

fn accounted_control_bytes(control: &ChainControl) -> usize {
    const CONTROL_OVERHEAD: usize = 256;
    match control {
        ChainControl::Barrier { id, .. } => CONTROL_OVERHEAD.saturating_add(id.len()),
        ChainControl::Reorg { .. }
        | ChainControl::Safe(_)
        | ChainControl::Finalized(_)
        | ChainControl::CanonicalProgress(_) => CONTROL_OVERHEAD,
        _ => usize::MAX / 2,
    }
}

fn same_buffered_delivery<N: Network>(left: &BufferedBatch<N>, right: &BufferedBatch<N>) -> bool {
    left.records.len() == right.records.len()
        && left.records.iter().zip(&right.records).all(
            |(
                (left_record, left_audience, left_scope),
                (right_record, right_audience, right_scope),
            )| {
                same_supported_record(left_record, right_record)
                    && left_audience == right_audience
                    && left_scope == right_scope
            },
        )
        && left.controls == right.controls
        && left.checkpoint == right.checkpoint
        && left.payload_commitment == right.payload_commitment
}

fn same_supported_record<N: Network>(
    left: &ReactiveInputRecord<N>,
    right: &ReactiveInputRecord<N>,
) -> bool {
    if left.context != right.context || left.input_ref() != right.input_ref() {
        return false;
    }
    match (&left.input, &right.input) {
        (ReactiveInput::Log(left), ReactiveInput::Log(right)) => left == right,
        (ReactiveInput::PendingTxHash(left), ReactiveInput::PendingTxHash(right)) => left == right,
        // `RpcObject` guarantees `Serialize`, so compare the exact
        // handler-visible network response body without imposing a
        // provider-specific `Eq` bound.
        (ReactiveInput::BlockHeader(left), ReactiveInput::BlockHeader(right)) => {
            match (
                header_body_digest(left, usize::MAX),
                header_body_digest(right, usize::MAX),
            ) {
                (Ok((left, left_len)), Ok((right, right_len))) => {
                    left_len == right_len && left == right
                }
                _ => false,
            }
        }
        // Full blocks and transaction bodies are not an advertised hybrid
        // buffering surface. Never silently accept an identity-only replay for
        // payloads whose dynamic body cannot be compared generically.
        (ReactiveInput::FullBlock(_), ReactiveInput::FullBlock(_))
        | (ReactiveInput::PendingTx(_), ReactiveInput::PendingTx(_)) => false,
        _ => false,
    }
}

fn deduplicable<N: Network>(record: &ReactiveInputRecord<N>) -> bool {
    // Reorg signals are lifecycle transitions, not immutable provider objects.
    // The same log can be removed, become canonical again, and be removed a
    // second time. Suppressing the later signal would still stage Hybrid's
    // checkpoint rewind while hiding the rollback from the runtime.
    record.is_payload_deduplicable()
        && !matches!(record.context.chain_status, ChainStatus::Reorged { .. })
        && !matches!(&record.input, ReactiveInput::Log(log) if log.removed)
}

fn wrap_token(
    epoch: [u8; 16],
    source: HybridSource,
    kind: HybridTokenKind,
    token: SubscriberDeliveryToken,
) -> SubscriberDeliveryToken {
    let inner = token.into_bytes();
    let mut bytes = Vec::with_capacity(TOKEN_MAGIC.len() + 16 + 2 + inner.len());
    bytes.extend_from_slice(TOKEN_MAGIC);
    bytes.extend_from_slice(&epoch);
    bytes.push(source.tag());
    bytes.push(kind.tag());
    bytes.extend_from_slice(&inner);
    SubscriberDeliveryToken::new(bytes)
}

fn unwrap_token(
    token: SubscriberDeliveryToken,
) -> Result<
    (
        [u8; 16],
        HybridSource,
        HybridTokenKind,
        SubscriberDeliveryToken,
    ),
    SubscriberError,
> {
    let bytes = token.into_bytes();
    const PREFIX_LEN: usize = 8 + 16;
    if bytes.len() < PREFIX_LEN + 2 || &bytes[..TOKEN_MAGIC.len()] != TOKEN_MAGIC {
        return Err(SubscriberError::Provider(
            "invalid hybrid delivery-token envelope".into(),
        ));
    }
    let epoch = bytes[8..PREFIX_LEN]
        .try_into()
        .expect("validated epoch width");
    let source = HybridSource::from_tag(bytes[PREFIX_LEN])?;
    let kind = HybridTokenKind::from_tag(bytes[PREFIX_LEN + 1])?;
    Ok((
        epoch,
        source,
        kind,
        SubscriberDeliveryToken::new(bytes[(PREFIX_LEN + 2)..].to_vec()),
    ))
}

#[cfg(test)]
mod token_tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
    };

    use alloy_primitives::{Address, B256, Bytes, Log as PrimitiveLog};
    use alloy_rpc_types_eth::{Filter, Log};
    use evm_fork_cache::reactive::{CanonicalRollbackKind, LogInterest};

    struct RestoreCountingSource {
        historical: bool,
        durable: bool,
        polls: Arc<AtomicUsize>,
        mutations: Arc<AtomicUsize>,
    }

    impl EventSubscriber<Ethereum> for RestoreCountingSource {
        fn chain_id(&self) -> Option<u64> {
            Some(1)
        }

        fn capabilities(&self) -> SubscriberCapabilities {
            let mut capabilities = vec![
                SubscriberCapability::Logs,
                SubscriberCapability::OwnerScopedDelivery,
                SubscriberCapability::DynamicInterests,
                SubscriberCapability::Barriers,
            ];
            if self.durable {
                capabilities.push(SubscriberCapability::DurableReplay);
            }
            capabilities.push(if self.historical {
                SubscriberCapability::HistoricalBackfill
            } else {
                SubscriberCapability::Live
            });
            SubscriberCapabilities::new(capabilities)
        }

        fn register_interests(
            &mut self,
            _interests: &[ReactiveInterest<Ethereum>],
        ) -> SubscriberOperation<'_, ()> {
            Box::pin(async move {
                self.mutations.fetch_add(1, AtomicOrdering::SeqCst);
                Ok(())
            })
        }

        fn next_batch(&mut self) -> SubscriberNextBatch<'_, Ethereum> {
            Box::pin(async move {
                self.polls.fetch_add(1, AtomicOrdering::SeqCst);
                Ok(None)
            })
        }

        fn restore_position(
            &mut self,
            _position: &SubscriberResumePosition,
        ) -> Result<(), SubscriberError> {
            Ok(())
        }
    }

    impl InterestOwnerSubscriber<Ethereum> for RestoreCountingSource {
        fn upsert_interest_owners(
            &mut self,
            _owners: Vec<(HandlerId, Vec<ReactiveInterest<Ethereum>>)>,
        ) -> SubscriberOperation<'_, ()> {
            Box::pin(async move {
                self.mutations.fetch_add(1, AtomicOrdering::SeqCst);
                Ok(())
            })
        }

        fn replace_interest_owners(
            &mut self,
            _owners: Vec<(HandlerId, Vec<ReactiveInterest<Ethereum>>)>,
        ) -> SubscriberOperation<'_, ()> {
            Box::pin(async move {
                self.mutations.fetch_add(1, AtomicOrdering::SeqCst);
                Ok(())
            })
        }

        fn add_interest_owner(
            &mut self,
            _owner: HandlerId,
            _interests: &[ReactiveInterest<Ethereum>],
        ) -> SubscriberOperation<'_, ()> {
            Box::pin(async move {
                self.mutations.fetch_add(1, AtomicOrdering::SeqCst);
                Ok(())
            })
        }

        fn add_interest_owner_with_backfill(
            &mut self,
            owner: HandlerId,
            interests: &[ReactiveInterest<Ethereum>],
            _backfill: SubscriberBackfill,
        ) -> SubscriberOperation<'_, ()> {
            self.add_interest_owner(owner, interests)
        }

        fn remove_interest_owner(
            &mut self,
            _owner: &HandlerId,
        ) -> SubscriberOperation<'_, Option<Vec<ReactiveInterest<Ethereum>>>> {
            Box::pin(async move {
                self.mutations.fetch_add(1, AtomicOrdering::SeqCst);
                Ok(None)
            })
        }

        fn owner_interests(&self, _owner: &HandlerId) -> Option<&[ReactiveInterest<Ethereum>]> {
            None
        }
    }

    struct OrderedHeader<'a>(&'a [(&'a str, u64)]);

    impl Serialize for OrderedHeader<'_> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let mut map = serializer.serialize_map(Some(self.0.len()))?;
            for (key, value) in self.0 {
                serde::ser::SerializeMap::serialize_entry(&mut map, key, value)?;
            }
            serde::ser::SerializeMap::end(map)
        }
    }

    fn checkpoint() -> HybridCheckpointV5 {
        HybridCheckpointV5 {
            chain_id: 1,
            epoch: [9; 16],
            next_synthetic_token: 7,
            lifecycle_generation: 3,
            owner_generations: BTreeMap::new(),
            lifecycle_intent: lifecycle_intent::<Ethereum>(&[], &HashMap::new())
                .expect("empty lifecycle"),
            recent_inputs: Vec::new(),
            canonical_history: Vec::new(),
            coverage_head: None,
            safe_head: None,
            finalized_head: None,
            certified_historical: None,
            historical_position: SourcePosition::default(),
            live_position: SourcePosition::default(),
            last_committed_token: None,
        }
    }

    fn golden_block(number: u64, hash: u8, parent_hash: u8) -> BlockRef {
        BlockRef {
            number,
            hash: B256::repeat_byte(hash),
            parent_hash: Some(B256::repeat_byte(parent_hash)),
            timestamp: Some(number.saturating_mul(12)),
        }
    }

    fn golden_identity(input_ref: InputRef, kind: ReactiveInputKind) -> ReactiveInputIdentity {
        ReactiveInputIdentity::try_from_parts(input_ref, kind)
            .expect("golden identity must pair a compatible reference and representation")
    }

    fn golden_witness(
        payload: u8,
        lifecycle: WitnessLifecycle,
        block: Option<BlockRef>,
        transaction_index: Option<u64>,
        log_index: Option<u64>,
        log_block_timestamp: Option<u64>,
    ) -> RecordWitness {
        RecordWitness {
            payload_digest: [payload; 32],
            chain_id: 1,
            lifecycle,
            block,
            transaction_index,
            log_index,
            log_block_timestamp,
        }
    }

    fn fully_populated_checkpoint(
        committed_source: HybridSource,
        committed_kind: HybridTokenKind,
    ) -> HybridCheckpointV5 {
        let block_100 = golden_block(100, 0x64, 0x63);
        let block_101 = golden_block(101, 0x65, 0x64);
        let block_102 = golden_block(102, 0x66, 0x65);
        let owner_alpha = HandlerId::new("owner-alpha");
        let owner_beta = HandlerId::new("owner-beta");
        let owner_generations = BTreeMap::from([(owner_alpha.clone(), 2), (owner_beta.clone(), 3)]);
        let owner_digests = BTreeMap::from([
            (owner_alpha.clone(), [0xa1; 32]),
            (owner_beta.clone(), [0xb2; 32]),
        ]);
        let recent_inputs = vec![
            StoredRecentInput {
                identity: golden_identity(
                    InputRef::Log {
                        chain_id: Some(1),
                        block_hash: block_102.hash,
                        transaction_hash: B256::repeat_byte(0xc1),
                        log_index: 7,
                    },
                    ReactiveInputKind::CanonicalLog,
                ),
                coverage: AudienceCoverage {
                    base: true,
                    owners: BTreeMap::new(),
                    block: Some(block_102),
                    witness: Some(golden_witness(
                        0x11,
                        WitnessLifecycle::Included,
                        Some(block_102),
                        Some(2),
                        Some(7),
                        Some(1_224),
                    )),
                },
            },
            StoredRecentInput {
                identity: golden_identity(
                    InputRef::Log {
                        chain_id: Some(1),
                        block_hash: block_100.hash,
                        transaction_hash: B256::repeat_byte(0xc2),
                        log_index: 8,
                    },
                    ReactiveInputKind::CanonicalLog,
                ),
                coverage: AudienceCoverage {
                    base: false,
                    owners: BTreeMap::from([(owner_alpha.clone(), 2)]),
                    block: Some(block_100),
                    witness: Some(golden_witness(
                        0x22,
                        WitnessLifecycle::Finalized,
                        Some(block_100),
                        Some(3),
                        Some(8),
                        None,
                    )),
                },
            },
            StoredRecentInput {
                identity: golden_identity(
                    InputRef::Block {
                        chain_id: Some(1),
                        hash: block_101.hash,
                        number: block_101.number,
                    },
                    ReactiveInputKind::BlockHeader,
                ),
                coverage: AudienceCoverage {
                    base: true,
                    owners: BTreeMap::from([(owner_alpha.clone(), 2), (owner_beta.clone(), 3)]),
                    block: Some(block_101),
                    witness: Some(golden_witness(
                        0x33,
                        WitnessLifecycle::Safe,
                        Some(block_101),
                        None,
                        None,
                        None,
                    )),
                },
            },
            StoredRecentInput {
                identity: golden_identity(
                    InputRef::Log {
                        chain_id: Some(1),
                        block_hash: block_101.hash,
                        transaction_hash: B256::repeat_byte(0xc4),
                        log_index: 9,
                    },
                    ReactiveInputKind::ReorgSignalLog,
                ),
                coverage: AudienceCoverage {
                    base: false,
                    owners: BTreeMap::from([(owner_beta.clone(), 3)]),
                    block: Some(block_101),
                    witness: Some(golden_witness(
                        0x44,
                        WitnessLifecycle::Reorg,
                        Some(block_101),
                        Some(4),
                        Some(9),
                        Some(1_212),
                    )),
                },
            },
            StoredRecentInput {
                identity: golden_identity(
                    InputRef::PendingTx {
                        chain_id: Some(1),
                        hash: B256::repeat_byte(0xd5),
                    },
                    ReactiveInputKind::PendingTxHash,
                ),
                coverage: AudienceCoverage {
                    base: false,
                    owners: BTreeMap::new(),
                    block: None,
                    witness: Some(golden_witness(
                        0x55,
                        WitnessLifecycle::Pending,
                        None,
                        None,
                        None,
                        None,
                    )),
                },
            },
        ];
        let historical_token = vec![0xf1, 0xf2, 0xf3];
        let live_token = vec![0xe1, 0xe2, 0xe3, 0xe4];
        let mut historical_position = SourcePosition {
            delivery_token: Some(historical_token.clone()),
            checkpoint: Some(vec![0xa0, 0xa1, 0xa2]),
            coverage_head: Some(block_102),
            canonical_history: vec![block_100, block_101, block_102],
            delivery_digest: Some([0xa5; 32]),
        };
        let mut live_position = SourcePosition {
            delivery_token: Some(live_token.clone()),
            checkpoint: Some(vec![0xb0, 0xb1]),
            coverage_head: Some(block_101),
            canonical_history: vec![block_100, block_101],
            delivery_digest: Some([0xb5; 32]),
        };
        let committed_inner = match (committed_source, committed_kind) {
            (HybridSource::Historical, HybridTokenKind::Forwarded) => historical_token,
            (HybridSource::Live, HybridTokenKind::Forwarded) => live_token,
            (HybridSource::Historical, HybridTokenKind::Synthetic) => {
                historical_position.delivery_token = None;
                5_u64.to_be_bytes().to_vec()
            }
            (HybridSource::Live, HybridTokenKind::Synthetic) => {
                live_position.delivery_token = None;
                6_u64.to_be_bytes().to_vec()
            }
        };
        HybridCheckpointV5 {
            chain_id: 1,
            epoch: [0x9a; 16],
            next_synthetic_token: 7,
            lifecycle_generation: 3,
            owner_generations,
            lifecycle_intent: LifecycleIntent {
                base: empty_interest_fingerprint(),
                owners: owner_digests,
            },
            recent_inputs,
            canonical_history: vec![block_100, block_101, block_102],
            coverage_head: Some(block_102),
            safe_head: Some(block_101),
            finalized_head: Some(block_100),
            certified_historical: Some(CertifiedHistoricalCoverage {
                lifecycle_generation: 3,
                through: block_102,
            }),
            historical_position,
            live_position,
            last_committed_token: Some(StoredCommittedToken {
                source: committed_source,
                kind: committed_kind,
                inner: committed_inner,
            }),
        }
    }

    fn restore_test_interest() -> ReactiveInterest<Ethereum> {
        ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new().address(Address::repeat_byte(0xaa)),
            local_matcher: None,
            route_key: None,
        })
    }

    fn restore_position_for(state: &HybridCheckpointV5) -> SubscriberResumePosition {
        let committed = state
            .last_committed_token
            .as_ref()
            .expect("restore fixture carries a committed token");
        SubscriberResumePosition::new(
            state.chain_id,
            state
                .coverage_head
                .expect("restore fixture carries coverage"),
            state.canonical_history.clone(),
            Some(wrap_token(
                state.epoch,
                committed.source,
                committed.kind,
                SubscriberDeliveryToken::new(committed.inner.clone()),
            )),
            Some(SubscriberCheckpoint::new(
                encode_hybrid_checkpoint(state).expect("encode restore fixture"),
            )),
        )
    }

    fn fill_opaque_checkpoint_to_v5_limit(state: &mut HybridCheckpointV5) {
        let mut low = 0usize;
        let mut high = codec::MAX_CHECKPOINT_PAYLOAD_BYTES + 1;
        while low + 1 < high {
            let middle = low + (high - low) / 2;
            state.historical_position.checkpoint = Some(vec![0x7a; middle]);
            if codec::checkpoint_payload_len_after_dropping_recent(state, 0)
                .expect("measure checkpoint fixture")
                <= codec::MAX_CHECKPOINT_PAYLOAD_BYTES
            {
                low = middle;
            } else {
                high = middle;
            }
        }
        state.historical_position.checkpoint = Some(vec![0x7a; low]);
        encode_hybrid_checkpoint(state).expect("near-limit checkpoint remains valid");
    }

    fn fill_ephemeral_live_checkpoint_to_v5_limit(state: &mut HybridCheckpointV5) {
        let mut low = 0usize;
        let mut high = codec::MAX_CHECKPOINT_PAYLOAD_BYTES + 1;
        while low + 1 < high {
            let middle = low + (high - low) / 2;
            state.live_position.checkpoint = Some(vec![0x6c; middle]);
            if codec::checkpoint_payload_len_after_dropping_recent(state, 0)
                .expect("measure live checkpoint fixture")
                <= codec::MAX_CHECKPOINT_PAYLOAD_BYTES
            {
                low = middle;
            } else {
                high = middle;
            }
        }
        state.live_position.checkpoint = Some(vec![0x6c; low]);
        encode_hybrid_checkpoint(state).expect("near-limit live checkpoint remains valid");
    }

    fn maximum_opaque_checkpoint_config() -> HybridConfig {
        HybridConfig {
            max_source_checkpoint_bytes: HYBRID_MAX_SOURCE_CHECKPOINT_BYTES,
            ..HybridConfig::default()
        }
    }

    fn restore_counting_sources(
        live_mutations: Arc<AtomicUsize>,
    ) -> (
        RestoreCountingSource,
        RestoreCountingSource,
        Arc<AtomicUsize>,
    ) {
        let historical_mutations = Arc::new(AtomicUsize::new(0));
        (
            RestoreCountingSource {
                historical: true,
                durable: true,
                polls: Arc::new(AtomicUsize::new(0)),
                mutations: Arc::clone(&historical_mutations),
            },
            RestoreCountingSource {
                historical: false,
                durable: false,
                polls: Arc::new(AtomicUsize::new(0)),
                mutations: live_mutations,
            },
            historical_mutations,
        )
    }

    fn checkpoint_hex(bytes: &[u8]) -> String {
        use std::fmt::Write as _;

        let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
        for byte in bytes {
            write!(encoded, "{byte:02x}").expect("writing to String cannot fail");
        }
        encoded
    }

    #[test]
    fn every_structured_incomplete_rollback_requests_historical_recovery() {
        for kind in [
            CanonicalRollbackKind::Explicit,
            CanonicalRollbackKind::Removed,
            CanonicalRollbackKind::ImplicitParent,
            CanonicalRollbackKind::MissingReplacement,
        ] {
            let classified =
                classify_canonical_validation_error(CanonicalSequenceError::IncompleteRollback {
                    common_ancestor: 99,
                    oldest_retained: Some(100),
                    kind,
                });
            assert!(matches!(
                classified,
                CanonicalPlanError::NeedsHistoricalRecovery(_)
            ));
        }
    }

    #[test]
    fn intrinsically_invalid_canonical_input_remains_fatal() {
        let classified = classify_canonical_validation_error(CanonicalSequenceError::Invalid(
            evm_fork_cache::reactive::ReactiveError::InvalidChainControl {
                message: "contradictory test input".into(),
            },
        ));
        assert!(matches!(classified, CanonicalPlanError::Invalid(_)));
    }

    fn stored_recent_input(
        block_number: u64,
        owners: BTreeMap<HandlerId, u64>,
    ) -> StoredRecentInput {
        let block = BlockRef {
            number: block_number,
            hash: B256::repeat_byte(block_number as u8),
            parent_hash: None,
            timestamp: None,
        };
        let record = ReactiveInputRecord::<Ethereum>::new(
            ReactiveInput::Log(Log {
                inner: PrimitiveLog::new_unchecked(
                    Address::repeat_byte(0xaa),
                    vec![B256::repeat_byte(0xbb)],
                    Bytes::new(),
                ),
                block_hash: Some(block.hash),
                block_number: Some(block.number),
                block_timestamp: None,
                transaction_hash: Some(B256::repeat_byte(block_number.wrapping_add(1) as u8)),
                transaction_index: Some(0),
                log_index: Some(block_number),
                removed: false,
            }),
            ReactiveContext {
                chain_id: Some(1),
                source: InputSource::Backfill,
                chain_status: ChainStatus::Included {
                    block,
                    confirmations: 0,
                },
                block: Some(block),
                transaction_index: Some(0),
                log_index: Some(block_number),
            },
        );
        StoredRecentInput {
            identity: record.validated_identity().expect("valid test identity"),
            coverage: AudienceCoverage {
                base: false,
                owners,
                block: Some(block),
                witness: Some(record_witness(&record, 1).expect("test witness")),
            },
        }
    }

    #[test]
    fn token_envelope_round_trips_source_and_opaque_bytes() {
        for source in [HybridSource::Historical, HybridSource::Live] {
            for kind in [HybridTokenKind::Forwarded, HybridTokenKind::Synthetic] {
                let wrapped = wrap_token(
                    [7; 16],
                    source,
                    kind,
                    SubscriberDeliveryToken::new(vec![0, 1, 2, 255]),
                );
                let (epoch, decoded_source, decoded_kind, inner) =
                    unwrap_token(wrapped).expect("decode token");
                assert_eq!(epoch, [7; 16]);
                assert_eq!(decoded_source, source);
                assert_eq!(decoded_kind, kind);
                assert_eq!(inner.as_bytes(), &[0, 1, 2, 255]);
            }
        }
    }

    #[test]
    fn header_witness_is_stable_across_object_field_order() {
        let (left, left_len) =
            header_body_digest(&OrderedHeader(&[("beta", 2), ("alpha", 1)]), 1_024)
                .expect("left witness");
        let (right, right_len) =
            header_body_digest(&OrderedHeader(&[("alpha", 1), ("beta", 2)]), 1_024)
                .expect("right witness");
        let (changed, _) = header_body_digest(&OrderedHeader(&[("alpha", 1), ("beta", 3)]), 1_024)
            .expect("changed witness");

        assert_eq!(left, right);
        assert_eq!(left_len, right_len);
        assert_ne!(left, changed);
    }

    #[test]
    fn header_canonicalization_scratch_has_an_independent_hard_cap() {
        let oversized = "x".repeat(MAX_HEADER_CANONICALIZATION_BYTES + 1);
        let error = header_body_digest(&oversized, usize::MAX)
            .expect_err("canonicalization scratch must remain bounded");
        assert!(
            error
                .to_string()
                .contains("serialized block header exceeds")
        );
        assert!(
            error
                .to_string()
                .contains(&MAX_HEADER_CANONICALIZATION_BYTES.to_string())
        );
    }

    #[test]
    fn checkpoint_rejects_corruption_truncation_and_unknown_versions() {
        let encoded = encode_hybrid_checkpoint(&checkpoint()).expect("encode checkpoint");
        assert_eq!(
            decode_hybrid_checkpoint(&encoded)
                .expect("decode checkpoint")
                .next_synthetic_token,
            7
        );

        let mut corrupt = encoded.clone();
        *corrupt.last_mut().expect("payload") ^= 1;
        assert!(
            decode_hybrid_checkpoint(&corrupt)
                .expect_err("checksum mismatch")
                .to_string()
                .contains("checksum")
        );

        assert!(
            decode_hybrid_checkpoint(&encoded[..encoded.len() - 1])
                .expect_err("truncated checkpoint")
                .to_string()
                .contains("payload length")
        );

        let mut future = encoded;
        let mut legacy = future.clone();
        legacy[8..10].copy_from_slice(&4_u16.to_be_bytes());
        let legacy_error = decode_hybrid_checkpoint(&legacy).expect_err("legacy checkpoint");
        assert!(
            legacy_error.to_string().contains("version 4")
                && legacy_error.to_string().contains("resynchronization")
        );

        future[8..10].copy_from_slice(&(CHECKPOINT_VERSION + 1).to_be_bytes());
        assert!(
            decode_hybrid_checkpoint(&future)
                .expect_err("future checkpoint")
                .to_string()
                .contains("unsupported hybrid checkpoint version")
        );

        let mut semantically_invalid = checkpoint();
        semantically_invalid.next_synthetic_token = 0;
        assert!(
            encode_hybrid_checkpoint(&semantically_invalid)
                .expect_err("zero synthetic sequence")
                .to_string()
                .contains("synthetic token sequence")
        );

        let mut trailing = encode_hybrid_checkpoint(&checkpoint()).expect("encode checkpoint");
        let payload_start = 18;
        trailing.push(0xff);
        let payload_len = trailing.len() - payload_start;
        trailing[10..14].copy_from_slice(&(payload_len as u32).to_be_bytes());
        let checksum = crc32fast::hash(&trailing[payload_start..]);
        trailing[14..18].copy_from_slice(&checksum.to_be_bytes());
        assert!(
            decode_hybrid_checkpoint(&trailing)
                .expect_err("trailing payload")
                .to_string()
                .contains("trailing bytes")
        );

        let mut oversized = vec![0_u8; 18 + MAX_CHECKPOINT_BYTES + 1];
        oversized[..8].copy_from_slice(CHECKPOINT_MAGIC);
        oversized[8..10].copy_from_slice(&CHECKPOINT_VERSION.to_be_bytes());
        oversized[10..14].copy_from_slice(&((MAX_CHECKPOINT_BYTES + 1) as u32).to_be_bytes());
        assert!(
            decode_hybrid_checkpoint(&oversized)
                .expect_err("oversized checkpoint")
                .to_string()
                .contains("oversized")
        );
    }

    #[test]
    fn checkpoint_encoding_stops_at_the_bound_before_materializing_oversized_payload() {
        const TEST_LIMIT: usize = 64;
        let state = checkpoint();
        let error = encode_hybrid_checkpoint_with_limit(&state, TEST_LIMIT)
            .expect_err("bounded production encoder must fail closed");
        assert!(error.to_string().contains("exceeds 64 bytes"));
    }

    #[test]
    fn dense_owner_fanout_evicts_oldest_witnesses_but_protects_the_current_delivery() {
        let owner_a = HandlerId::new("owner-a");
        let owner_b = HandlerId::new("owner-b");
        let owners = BTreeMap::from([(owner_a.clone(), 3), (owner_b.clone(), 3)]);
        let mut state = checkpoint();
        state.owner_generations = owners.clone();
        state.lifecycle_intent.owners =
            BTreeMap::from([(owner_a, [0x11; 32]), (owner_b, [0x22; 32])]);
        state.recent_inputs = (0..3)
            .map(|block| stored_recent_input(block, owners.clone()))
            .collect();
        let head = BlockRef {
            number: 2,
            hash: B256::repeat_byte(2),
            parent_hash: None,
            timestamp: None,
        };
        state.canonical_history.push(head);
        state.coverage_head = Some(head);

        fit_checkpoint_to_durable_limits(&mut state, 1, 10, 2)
            .expect("newest two-owner witness fits");
        assert_eq!(state.recent_inputs.len(), 1);
        assert_eq!(
            state.recent_inputs[0]
                .coverage
                .block
                .as_ref()
                .unwrap()
                .number,
            2
        );

        let error = fit_checkpoint_to_durable_limits(&mut state, 1, 10, 1)
            .expect_err("protected fanout cannot be silently discarded");
        assert!(error.to_string().contains("protected recent witnesses"));
    }

    #[test]
    fn saturated_capacity_preflight_encodes_the_real_maximum_commit_for_each_source() {
        let config = HybridConfig {
            max_source_delivery_token_bytes: 19,
            max_source_checkpoint_bytes: 23,
            canonical_history_capacity: 4,
            ..HybridConfig::default()
        };
        let live_mutations = Arc::new(AtomicUsize::new(0));
        let (history, live, _) = restore_counting_sources(live_mutations);
        let hybrid = HybridSubscriber::new(history, live, config).expect("coordinator");
        let mut state = checkpoint();
        state.lifecycle_intent.base = [0x41; 32];
        let head = golden_block(100, 0x64, 0x63);
        state.canonical_history = vec![head];
        state.coverage_head = Some(head);
        state.historical_position.canonical_history = vec![head];
        state.historical_position.coverage_head = Some(head);
        state.live_position.canonical_history = vec![head];
        state.live_position.coverage_head = Some(head);

        hybrid
            .preflight_maximum_canonical_log_delivery(&state, "test checkpoint")
            .expect("both eligible source commits fit");

        for source in [HybridSource::Historical, HybridSource::Live] {
            let encoded = hybrid
                .encode_maximum_canonical_log_delivery(&state, source)
                .expect("the exact simulated commit encodes");
            let committed =
                decode_hybrid_checkpoint(&encoded).expect("the saturated simulated commit decodes");
            assert_eq!(committed.chain_id, state.chain_id);
            assert_eq!(committed.epoch, state.epoch);
            assert_eq!(committed.next_synthetic_token, u64::MAX);
            assert_eq!(committed.lifecycle_generation, state.lifecycle_generation);
            assert_eq!(committed.owner_generations, state.owner_generations);
            assert_eq!(committed.lifecycle_intent, state.lifecycle_intent);
            assert_eq!(
                committed.canonical_history.len(),
                config.canonical_history_capacity
            );
            let block = committed
                .coverage_head
                .expect("maximum commit advances coverage");
            assert_eq!(block.number, u64::MAX);
            assert!(block.parent_hash.is_some());
            assert_eq!(block.timestamp, Some(u64::MAX));
            let record_block = block;
            assert_eq!(
                committed.canonical_history.first().unwrap().number,
                u64::MAX - 3
            );
            for pair in committed.canonical_history.windows(2) {
                assert_eq!(pair[1].number, pair[0].number + 1);
                assert_eq!(pair[1].parent_hash, Some(pair[0].hash));
            }
            assert!(
                committed
                    .canonical_history
                    .iter()
                    .all(|retained| retained.timestamp == Some(u64::MAX))
            );
            assert_eq!(committed.safe_head, Some(block));
            assert_eq!(committed.finalized_head, Some(block));

            for position in [&committed.historical_position, &committed.live_position] {
                assert_eq!(
                    position
                        .delivery_token
                        .as_ref()
                        .expect("reserved delivery token")
                        .len(),
                    config.max_source_delivery_token_bytes
                );
                assert_eq!(
                    position
                        .checkpoint
                        .as_ref()
                        .expect("reserved child checkpoint")
                        .len(),
                    config.max_source_checkpoint_bytes
                );
                assert!(position.delivery_digest.is_some());
            }
            let source_position = match source {
                HybridSource::Historical => &committed.historical_position,
                HybridSource::Live => &committed.live_position,
            };
            let other_position = match source {
                HybridSource::Historical => &committed.live_position,
                HybridSource::Live => &committed.historical_position,
            };
            assert_eq!(source_position.coverage_head, Some(block));
            assert_eq!(
                source_position.canonical_history,
                committed.canonical_history
            );
            assert_eq!(other_position.coverage_head, Some(block));
            assert_eq!(
                other_position.canonical_history,
                committed.canonical_history
            );
            let last = committed
                .last_committed_token
                .as_ref()
                .expect("forwarded last-commit proof");
            assert_eq!(last.source, source);
            assert_eq!(last.kind, HybridTokenKind::Forwarded);
            assert_eq!(
                last.inner,
                source_position
                    .delivery_token
                    .clone()
                    .expect("matching source token")
            );
            assert_eq!(
                committed.certified_historical,
                if source == HybridSource::Historical {
                    Some(CertifiedHistoricalCoverage {
                        lifecycle_generation: state.lifecycle_generation,
                        through: block,
                    })
                } else {
                    Some(CertifiedHistoricalCoverage {
                        lifecycle_generation: state.lifecycle_generation,
                        through: committed.canonical_history[config.canonical_history_capacity - 2],
                    })
                }
            );

            assert_eq!(committed.recent_inputs.len(), 1);
            let recent = committed
                .recent_inputs
                .last()
                .expect("protected canonical-log witness");
            assert!(recent.coverage.base);
            assert!(recent.coverage.owners.is_empty());
            assert_eq!(recent.coverage.block, Some(record_block));
            match recent.identity.input_ref() {
                InputRef::Log {
                    chain_id,
                    block_hash,
                    log_index,
                    ..
                } => {
                    assert_eq!(chain_id, Some(state.chain_id));
                    assert_eq!(block_hash, record_block.hash);
                    assert_eq!(log_index, u64::MAX);
                }
                _ => panic!("maximum probe must use a canonical-log identity"),
            }
            assert_eq!(recent.identity.kind(), ReactiveInputKind::CanonicalLog);
            let witness = recent
                .coverage
                .witness
                .as_ref()
                .expect("complete maximum witness");
            assert_eq!(witness.lifecycle, WitnessLifecycle::Finalized);
            assert_eq!(witness.block, Some(record_block));
            assert_eq!(witness.transaction_index, Some(u64::MAX));
            assert_eq!(witness.log_index, Some(u64::MAX));
            assert_eq!(witness.log_block_timestamp, Some(u64::MAX));
        }
    }

    #[test]
    fn saturated_capacity_preflight_covers_synthetic_and_single_byte_rlp_boundaries() {
        let config = HybridConfig {
            max_source_delivery_token_bytes: 1,
            max_source_checkpoint_bytes: 1,
            canonical_history_capacity: 3,
            ..HybridConfig::default()
        };
        let mut state = checkpoint();
        state.lifecycle_intent.base = [0x41; 32];

        let high_byte_state =
            saturated_capacity_probe_state(&state, &config).expect("saturated cursor state");
        let mut low_byte_state = high_byte_state.clone();
        low_byte_state.historical_position.delivery_token = Some(vec![0x71]);
        low_byte_state.historical_position.checkpoint = Some(vec![0x72]);
        let high_byte_len =
            codec::checkpoint_payload_len_after_dropping_recent(&high_byte_state, 0)
                .expect("measure high-byte RLP payload");
        let low_byte_len = codec::checkpoint_payload_len_after_dropping_recent(&low_byte_state, 0)
            .expect("measure low-byte RLP payload");
        assert!(
            high_byte_len >= low_byte_len + 2,
            "two one-byte opaque vectors at or above 0x80 need RLP prefixes"
        );

        let (history, live, _) = restore_counting_sources(Arc::new(AtomicUsize::new(0)));
        let hybrid = HybridSubscriber::new(history, live, config).expect("coordinator");
        hybrid
            .preflight_maximum_canonical_log_delivery(&state, "single-byte test checkpoint")
            .expect("all four source/token-kind probes fit");

        for source in [HybridSource::Historical, HybridSource::Live] {
            for kind in [HybridTokenKind::Forwarded, HybridTokenKind::Synthetic] {
                let encoded = hybrid
                    .encode_maximum_canonical_log_delivery_variant(&state, source, kind)
                    .expect("saturated source/token-kind simulation encodes");
                let committed = decode_hybrid_checkpoint(&encoded)
                    .expect("decode saturated source/token-kind simulation");
                assert_eq!(committed.next_synthetic_token, u64::MAX);
                assert_eq!(
                    committed.canonical_history.len(),
                    config.canonical_history_capacity
                );
                for position in [&committed.historical_position, &committed.live_position] {
                    assert_eq!(position.canonical_history, committed.canonical_history);
                    assert!(
                        position
                            .checkpoint
                            .as_ref()
                            .is_some_and(|bytes| bytes.len() == 1 && bytes[0] >= 0x80)
                    );
                }

                let committing = match source {
                    HybridSource::Historical => &committed.historical_position,
                    HybridSource::Live => &committed.live_position,
                };
                let other = match source {
                    HybridSource::Historical => &committed.live_position,
                    HybridSource::Live => &committed.historical_position,
                };
                assert!(
                    other
                        .delivery_token
                        .as_ref()
                        .is_some_and(|bytes| bytes.len() == 1 && bytes[0] >= 0x80)
                );
                let last = committed
                    .last_committed_token
                    .as_ref()
                    .expect("last-commit proof");
                assert_eq!(last.source, source);
                assert_eq!(last.kind, kind);
                match kind {
                    HybridTokenKind::Forwarded => {
                        assert!(
                            committing
                                .delivery_token
                                .as_ref()
                                .is_some_and(|bytes| bytes.len() == 1 && bytes[0] >= 0x80)
                        );
                        assert_eq!(last.inner.len(), 1);
                    }
                    HybridTokenKind::Synthetic => {
                        assert!(committing.delivery_token.is_none());
                        assert_eq!(last.inner, (u64::MAX - 1).to_be_bytes());
                        assert_eq!(last.inner.len(), 8);
                    }
                }
            }
        }
    }

    #[test]
    fn maximum_capacity_probe_reserves_every_admitted_canonical_history_entry() {
        let config = HybridConfig {
            max_buffered_live_bytes: 1_024,
            max_source_delivery_token_bytes: 19,
            max_source_checkpoint_bytes: 23,
            canonical_history_capacity: 3,
            ..HybridConfig::default()
        };
        let live_mutations = Arc::new(AtomicUsize::new(0));
        let (history, live, _) = restore_counting_sources(live_mutations);
        let hybrid = HybridSubscriber::new(history, live, config).expect("coordinator");
        let mut state = checkpoint();
        state.lifecycle_intent.base = [0x41; 32];
        let head = golden_block(100, 0x64, 0x63);
        state.canonical_history = vec![head];
        state.coverage_head = Some(head);

        for source in [HybridSource::Historical, HybridSource::Live] {
            let encoded = hybrid
                .encode_maximum_canonical_log_delivery(&state, source)
                .expect("maximum admitted canonical sequence encodes");
            let committed =
                decode_hybrid_checkpoint(&encoded).expect("maximum canonical sequence decodes");
            assert_eq!(
                committed.canonical_history.len(),
                config.canonical_history_capacity
            );
            let source_position = match source {
                HybridSource::Historical => &committed.historical_position,
                HybridSource::Live => &committed.live_position,
            };
            assert_eq!(
                source_position.canonical_history.len(),
                config.canonical_history_capacity
            );
            for pair in committed.canonical_history.windows(2) {
                assert_eq!(pair[1].number, pair[0].number + 1);
                assert_eq!(pair[1].parent_hash, Some(pair[0].hash));
            }
            assert!(
                committed
                    .canonical_history
                    .iter()
                    .all(|block| block.timestamp == Some(u64::MAX))
            );
        }
    }

    #[test]
    fn capacity_probe_uses_the_same_derived_control_ceiling_as_ingress() {
        assert_eq!(maximum_ingress_control_count(255), 0);
        assert_eq!(maximum_ingress_control_count(256), 0);
        assert_eq!(maximum_ingress_control_count(511), 0);
        assert_eq!(maximum_ingress_control_count(512), 1);
        assert_eq!(maximum_ingress_control_count(1_024), 3);

        let mut config = HybridConfig {
            max_buffered_live_bytes: 1_024,
            canonical_history_capacity: 8,
            ..HybridConfig::default()
        };
        assert_eq!(maximum_admitted_canonical_advances(&config), 4);
        config.canonical_history_capacity = 3;
        assert_eq!(maximum_admitted_canonical_advances(&config), 3);
    }

    #[test]
    fn maximum_capacity_probe_handles_sparse_adjacent_and_terminal_heights() {
        let sparse_head = golden_block(100, 0x64, 0x63);
        let mut sparse = checkpoint();
        sparse.canonical_history = vec![sparse_head];
        sparse.coverage_head = Some(sparse_head);
        let sparse_probe =
            maximum_canonical_log_probe_block(&sparse).expect("construct sparse probe");
        assert_eq!(sparse_probe.number, u64::MAX);
        assert_ne!(sparse_probe.parent_hash, Some(sparse_head.hash));

        let adjacent_head = golden_block(u64::MAX - 1, 0x71, 0x70);
        let mut adjacent = checkpoint();
        adjacent.canonical_history = vec![adjacent_head];
        adjacent.coverage_head = Some(adjacent_head);
        let adjacent_probe =
            maximum_canonical_log_probe_block(&adjacent).expect("construct adjacent probe");
        assert_eq!(adjacent_probe.number, u64::MAX);
        assert_eq!(adjacent_probe.parent_hash, Some(adjacent_head.hash));

        let terminal_head = BlockRef {
            number: u64::MAX,
            hash: B256::repeat_byte(0x81),
            parent_hash: None,
            timestamp: None,
        };
        let mut terminal = checkpoint();
        terminal.canonical_history = vec![terminal_head];
        terminal.coverage_head = Some(terminal_head);
        let terminal_probe =
            maximum_canonical_log_probe_block(&terminal).expect("enrich terminal probe");
        assert_eq!(terminal_probe.number, u64::MAX);
        assert_eq!(terminal_probe.hash, terminal_head.hash);
        assert!(terminal_probe.parent_hash.is_some());
        assert_eq!(terminal_probe.timestamp, Some(u64::MAX));
    }

    #[test]
    fn saturated_capacity_probe_subsumes_divergent_source_histories_for_both_sources() {
        let config = HybridConfig {
            max_source_delivery_token_bytes: 7,
            max_source_checkpoint_bytes: 11,
            canonical_history_capacity: 3,
            ..HybridConfig::default()
        };
        let coordinator_oldest = golden_block(0, 0x01, 0x00);
        let coordinator_tip = golden_block(u64::MAX, 0xf1, 0xf0);
        let historical_tip = golden_block(u64::MAX - 1, 0xe1, 0xe0);
        let live_history = vec![
            golden_block(0, 0x11, 0x10),
            golden_block(1, 0x12, 0x11),
            golden_block(2, 0x13, 0x12),
        ];
        let mut state = checkpoint();
        state.lifecycle_intent.base = [0x41; 32];
        state.canonical_history = vec![coordinator_oldest, coordinator_tip];
        state.coverage_head = Some(coordinator_tip);
        state.historical_position.canonical_history = vec![historical_tip];
        state.historical_position.coverage_head = Some(historical_tip);
        state.live_position.canonical_history = live_history.clone();
        state.live_position.coverage_head = live_history.last().copied();
        validate_checkpoint_state(&state).expect("divergent source candidate is valid");

        let (history, live, _) = restore_counting_sources(Arc::new(AtomicUsize::new(0)));
        let hybrid = HybridSubscriber::new(history, live, config).expect("coordinator");
        for source in [HybridSource::Historical, HybridSource::Live] {
            let encoded = hybrid
                .encode_maximum_canonical_log_delivery(&state, source)
                .expect("saturated simulation succeeds");
            let committed =
                decode_hybrid_checkpoint(&encoded).expect("decode saturated simulation");
            let head = committed.coverage_head.expect("saturated head");
            assert_eq!(head.number, u64::MAX);
            for position in [&committed.historical_position, &committed.live_position] {
                assert_eq!(position.canonical_history, committed.canonical_history);
                assert_eq!(position.coverage_head, Some(head));
            }
            assert_eq!(
                committed.canonical_history.len(),
                config.canonical_history_capacity
            );
        }
    }

    #[test]
    fn saturated_history_builder_populates_the_wire_maximum_in_one_pass() {
        let history = saturated_capacity_probe_history(HYBRID_MAX_CANONICAL_HISTORY)
            .expect("maximum retained history builds");
        assert_eq!(history.len(), HYBRID_MAX_CANONICAL_HISTORY);
        assert_eq!(
            history.first().expect("first saturated block").number,
            u64::MAX - HYBRID_MAX_CANONICAL_HISTORY as u64
        );
        assert_eq!(
            history.last().expect("last saturated block").number,
            u64::MAX - 1
        );
        assert_eq!(
            history
                .iter()
                .map(|block| block.hash)
                .collect::<HashSet<_>>()
                .len(),
            HYBRID_MAX_CANONICAL_HISTORY
        );
        assert!(
            history
                .iter()
                .all(|block| { block.parent_hash.is_some() && block.timestamp == Some(u64::MAX) })
        );
        assert!(history.windows(2).all(|pair| {
            pair[1].number == pair[0].number + 1 && pair[1].parent_hash == Some(pair[0].hash)
        }));
    }

    #[test]
    fn maximum_owner_capacity_commit_persists_exact_installed_fanout() {
        let config = HybridConfig {
            max_source_delivery_token_bytes: 7,
            max_source_checkpoint_bytes: 11,
            ..HybridConfig::default()
        };
        let live_mutations = Arc::new(AtomicUsize::new(0));
        let (history, live, _) = restore_counting_sources(live_mutations);
        let hybrid = HybridSubscriber::new(history, live, config).expect("coordinator");
        let owner_a = HandlerId::new("capacity-owner-a");
        let owner_b = HandlerId::new("capacity-owner-b");
        let mut state = checkpoint();
        state.owner_generations = BTreeMap::from([(owner_a.clone(), 3), (owner_b.clone(), 2)]);
        state.lifecycle_intent.owners = BTreeMap::from([
            (owner_a.clone(), [0xa1; 32]),
            (owner_b.clone(), empty_interest_fingerprint()),
        ]);

        let encoded = hybrid
            .encode_maximum_canonical_log_delivery(&state, HybridSource::Historical)
            .expect("maximum owner-fanout commit encodes");
        let committed = decode_hybrid_checkpoint(&encoded).expect("decode maximum owner fanout");
        let coverage = &committed
            .recent_inputs
            .last()
            .expect("protected owner witness")
            .coverage;

        assert!(!coverage.base);
        assert_eq!(coverage.owners, state.owner_generations);
    }

    #[test]
    fn checkpoint_rejects_mixed_base_and_owner_topology() {
        assert_eq!(
            empty_interest_fingerprint(),
            interest_fingerprint::<Ethereum>(&[]).expect("empty fingerprint")
        );
        let mut mixed = checkpoint();
        let owner = HandlerId::new("mixed-owner");
        mixed.lifecycle_intent.base = [0x41; 32];
        mixed
            .lifecycle_intent
            .owners
            .insert(owner.clone(), [0x42; 32]);
        mixed.owner_generations.insert(owner, 3);
        let error = encode_hybrid_checkpoint(&mixed)
            .expect_err("mixed topology cannot be encoded atomically");
        assert!(error.to_string().contains("base/unowned"));
        assert!(error.to_string().contains("owner-managed"));
    }

    #[test]
    fn checkpoint_v5_encoding_has_a_stable_golden_digest() {
        let encoded = encode_hybrid_checkpoint(&checkpoint()).expect("encode checkpoint");
        assert_eq!(&encoded[..8], CHECKPOINT_MAGIC);
        assert_eq!(&encoded[8..10], &CHECKPOINT_VERSION.to_be_bytes());
        assert_eq!(
            keccak256(&encoded),
            B256::from([
                0x48, 0xc8, 0x96, 0x5b, 0x9d, 0x19, 0x8b, 0xf8, 0xe4, 0x8a, 0x32, 0xe0, 0x22, 0xa9,
                0x0f, 0x17, 0x39, 0x7a, 0xae, 0xcd, 0xb9, 0x97, 0x71, 0x80, 0x9c, 0x60, 0xb3, 0x7b,
                0x22, 0x18, 0x53, 0x86,
            ])
        );
    }

    #[test]
    fn fully_populated_v5_checkpoints_match_permanent_wire_fixtures() {
        let cases = [
            (
                "forwarded historical",
                fully_populated_checkpoint(HybridSource::Historical, HybridTokenKind::Forwarded),
                include_str!("../testdata/hybrid_checkpoint_v5_forwarded_historical.hex"),
            ),
            (
                "synthetic live",
                fully_populated_checkpoint(HybridSource::Live, HybridTokenKind::Synthetic),
                include_str!("../testdata/hybrid_checkpoint_v5_synthetic_live.hex"),
            ),
        ];

        for (label, expected_state, fixture) in cases {
            let encoded = encode_hybrid_checkpoint(&expected_state)
                .unwrap_or_else(|error| panic!("encode {label} checkpoint: {error}"));
            assert_eq!(checkpoint_hex(&encoded), fixture.trim(), "{label} bytes");
            let decoded = decode_hybrid_checkpoint(&encoded)
                .unwrap_or_else(|error| panic!("decode {label} checkpoint: {error}"));
            assert_eq!(decoded, expected_state, "{label} decoded state");
            assert_eq!(
                encode_hybrid_checkpoint(&decoded)
                    .unwrap_or_else(|error| panic!("re-encode {label} checkpoint: {error}")),
                encoded,
                "{label} byte-for-byte re-encoding"
            );
        }
    }

    #[tokio::test]
    async fn active_base_restore_rejects_a_checkpoint_without_one_record_headroom_before_mutation()
    {
        let interest = restore_test_interest();
        let mut state =
            fully_populated_checkpoint(HybridSource::Historical, HybridTokenKind::Forwarded);
        state.owner_generations.clear();
        state.lifecycle_intent = LifecycleIntent {
            base: interest_fingerprint::<Ethereum>(std::slice::from_ref(&interest))
                .expect("base fingerprint"),
            owners: BTreeMap::new(),
        };
        state.recent_inputs.clear();
        fill_opaque_checkpoint_to_v5_limit(&mut state);
        let original_recent = state.recent_inputs.clone();
        let position = restore_position_for(&state);
        let live_mutations = Arc::new(AtomicUsize::new(0));
        let (history, live, historical_mutations) =
            restore_counting_sources(Arc::clone(&live_mutations));
        let mut hybrid = HybridSubscriber::new(history, live, maximum_opaque_checkpoint_config())
            .expect("coordinator");

        let error = hybrid
            .prepare_restore_base_lifecycle(&position, std::slice::from_ref(&interest))
            .await
            .expect_err("one protected base witness cannot fit");

        assert!(
            error
                .to_string()
                .contains("cannot retain one protected delivery witness"),
            "unexpected capacity error: {error}"
        );
        assert_eq!(historical_mutations.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(live_mutations.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(
            decode_hybrid_checkpoint(
                position
                    .subscriber_checkpoint
                    .as_ref()
                    .expect("checkpoint")
                    .as_bytes()
            )
            .expect("decode unchanged checkpoint")
            .recent_inputs,
            original_recent
        );
    }

    #[tokio::test]
    async fn active_restore_rejects_when_only_the_legacy_pending_probe_would_fit() {
        let interest = restore_test_interest();
        let mut state =
            fully_populated_checkpoint(HybridSource::Historical, HybridTokenKind::Forwarded);
        state.owner_generations.clear();
        state.lifecycle_intent = LifecycleIntent {
            base: interest_fingerprint::<Ethereum>(std::slice::from_ref(&interest))
                .expect("base fingerprint"),
            owners: BTreeMap::new(),
        };
        state.recent_inputs.clear();
        fill_opaque_checkpoint_to_v5_limit(&mut state);
        let opaque = state
            .historical_position
            .checkpoint
            .as_mut()
            .expect("opaque historical checkpoint");
        opaque.truncate(opaque.len().saturating_sub(512));
        encode_hybrid_checkpoint(&state).expect("legacy-probe-only fixture remains valid");
        let mut legacy_simulation = state.clone();
        append_legacy_pending_capacity_probe(
            &mut legacy_simulation,
            state.chain_id,
            true,
            BTreeMap::new(),
        )
        .expect("construct legacy pending probe");
        fit_checkpoint_to_durable_limits(
            &mut legacy_simulation,
            1,
            HybridConfig::default().recent_input_capacity,
            HybridConfig::default().max_recent_owner_entries,
        )
        .expect("the legacy tiny pending probe fits this fixture");
        encode_hybrid_checkpoint(&legacy_simulation)
            .expect("the legacy tiny pending probe encodes");
        let position = restore_position_for(&state);
        let live_mutations = Arc::new(AtomicUsize::new(0));
        let (history, live, historical_mutations) =
            restore_counting_sources(Arc::clone(&live_mutations));
        let mut hybrid = HybridSubscriber::new(history, live, maximum_opaque_checkpoint_config())
            .expect("coordinator");

        let error = hybrid
            .prepare_restore_base_lifecycle(&position, std::slice::from_ref(&interest))
            .await
            .expect_err(
                "the real maximum canonical-log commit must not inherit the legacy tiny-probe result",
            );

        assert!(
            error.to_string().contains("maximum canonical-log delivery"),
            "unexpected capacity error: {error}"
        );
        assert_eq!(historical_mutations.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(live_mutations.load(AtomicOrdering::SeqCst), 0);
    }

    #[tokio::test]
    async fn active_restore_rejects_when_one_advance_fits_but_admitted_history_does_not() {
        let interest = restore_test_interest();
        let head = golden_block(100, 0x64, 0x63);
        let mut state = checkpoint();
        state.lifecycle_intent = LifecycleIntent {
            base: interest_fingerprint::<Ethereum>(std::slice::from_ref(&interest))
                .expect("base fingerprint"),
            owners: BTreeMap::new(),
        };
        state.canonical_history = vec![head];
        state.coverage_head = Some(head);
        state.safe_head = Some(head);
        state.finalized_head = Some(head);
        state.certified_historical = Some(CertifiedHistoricalCoverage {
            lifecycle_generation: state.lifecycle_generation,
            through: head,
        });
        state.historical_position = SourcePosition {
            delivery_token: Some(vec![0x11]),
            checkpoint: Some(vec![0x12]),
            delivery_digest: Some([0x13; 32]),
            coverage_head: Some(head),
            canonical_history: vec![head],
        };
        state.live_position = SourcePosition {
            delivery_token: Some(vec![0x21]),
            checkpoint: Some(vec![0x22]),
            delivery_digest: Some([0x23; 32]),
            coverage_head: Some(head),
            canonical_history: vec![head],
        };
        state.last_committed_token = Some(StoredCommittedToken {
            source: HybridSource::Historical,
            kind: HybridTokenKind::Forwarded,
            inner: vec![0x11],
        });

        let one_advance_config = HybridConfig {
            max_buffered_live_bytes: 1_024,
            max_source_delivery_token_bytes: 1,
            max_source_checkpoint_bytes: 1,
            canonical_history_capacity: 3,
            ..HybridConfig::default()
        };
        let (history, live, _) = restore_counting_sources(Arc::new(AtomicUsize::new(0)));
        let one_advance_hybrid =
            HybridSubscriber::new(history, live, one_advance_config).expect("coordinator");
        let encoded = one_advance_hybrid
            .encode_canonical_log_delivery_capacity_probe(&state, HybridSource::Historical, 1)
            .expect("legacy one-advance simulation fits");
        let mut one_advance =
            decode_hybrid_checkpoint(&encoded).expect("decode one-advance simulation");
        assert_eq!(one_advance.canonical_history.len(), 2);

        let mut low = 1usize;
        let mut high = codec::MAX_CHECKPOINT_PAYLOAD_BYTES / 2;
        while low + 1 < high {
            let middle = low + (high - low) / 2;
            one_advance.historical_position.checkpoint = Some(vec![0x92; middle]);
            one_advance.live_position.checkpoint = Some(vec![0x82; middle]);
            if codec::checkpoint_payload_len_after_dropping_recent(&one_advance, 0)
                .expect("measure one-advance checkpoint")
                <= codec::MAX_CHECKPOINT_PAYLOAD_BYTES
            {
                low = middle;
            } else {
                high = middle;
            }
        }
        one_advance.historical_position.checkpoint = Some(vec![0x92; low]);
        one_advance.live_position.checkpoint = Some(vec![0x82; low]);
        encode_hybrid_checkpoint(&one_advance)
            .expect("legacy one-advance checkpoint fits exactly below the bound");

        state.historical_position.checkpoint = Some(vec![0x12; low]);
        state.live_position.checkpoint = Some(vec![0x22; low]);
        let position = restore_position_for(&state);
        let historical_mutations = Arc::new(AtomicUsize::new(0));
        let live_mutations = Arc::new(AtomicUsize::new(0));
        let history = RestoreCountingSource {
            historical: true,
            durable: true,
            polls: Arc::new(AtomicUsize::new(0)),
            mutations: Arc::clone(&historical_mutations),
        };
        let live = RestoreCountingSource {
            historical: false,
            durable: true,
            polls: Arc::new(AtomicUsize::new(0)),
            mutations: Arc::clone(&live_mutations),
        };
        let config = HybridConfig {
            max_buffered_live_bytes: 1_024,
            max_source_delivery_token_bytes: 1,
            max_source_checkpoint_bytes: low,
            canonical_history_capacity: 3,
            ..HybridConfig::default()
        };
        let mut hybrid = HybridSubscriber::new(history, live, config).expect("coordinator");

        let error = hybrid
            .prepare_restore_base_lifecycle(&position, std::slice::from_ref(&interest))
            .await
            .expect_err("the full admitted history must be reserved before child mutation");

        assert!(
            error.to_string().contains("maximum canonical-log delivery"),
            "unexpected capacity error: {error}"
        );
        assert_eq!(historical_mutations.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(live_mutations.load(AtomicOrdering::SeqCst), 0);
    }

    #[tokio::test]
    async fn terminal_restore_reserves_reorg_replacement_history_before_child_mutation() {
        let interest = restore_test_interest();
        let ancestor = golden_block(u64::MAX - 3, 0x91, 0x90);
        let old_tip = golden_block(u64::MAX, 0x94, 0x93);
        let mut state = checkpoint();
        state.lifecycle_intent = LifecycleIntent {
            base: interest_fingerprint::<Ethereum>(std::slice::from_ref(&interest))
                .expect("base fingerprint"),
            owners: BTreeMap::new(),
        };
        state.canonical_history = vec![ancestor, old_tip];
        state.coverage_head = Some(old_tip);
        state.safe_head = Some(ancestor);
        state.finalized_head = Some(ancestor);
        state.certified_historical = Some(CertifiedHistoricalCoverage {
            lifecycle_generation: state.lifecycle_generation,
            through: ancestor,
        });
        state.historical_position = SourcePosition {
            delivery_token: Some(vec![0x11]),
            checkpoint: Some(vec![0x12]),
            delivery_digest: Some([0x13; 32]),
            coverage_head: Some(old_tip),
            canonical_history: vec![ancestor, old_tip],
        };
        state.live_position = SourcePosition {
            delivery_token: Some(vec![0x21]),
            checkpoint: Some(vec![0x22]),
            delivery_digest: Some([0x23; 32]),
            coverage_head: Some(old_tip),
            canonical_history: vec![ancestor, old_tip],
        };
        state.last_committed_token = Some(StoredCommittedToken {
            source: HybridSource::Historical,
            kind: HybridTokenKind::Forwarded,
            inner: vec![0x11],
        });

        let terminal_config = HybridConfig {
            // The derived ceiling admits one Reorg plus two progress controls.
            max_buffered_live_bytes: 1_024,
            max_source_delivery_token_bytes: 1,
            max_source_checkpoint_bytes: 1,
            canonical_history_capacity: 4,
            ..HybridConfig::default()
        };
        let (history, live, _) = restore_counting_sources(Arc::new(AtomicUsize::new(0)));
        let terminal_hybrid =
            HybridSubscriber::new(history, live, terminal_config).expect("coordinator");
        let encoded = terminal_hybrid
            .encode_maximum_canonical_log_delivery(&state, HybridSource::Historical)
            .expect("terminal same-height enrichment fits");
        let mut terminal_enrichment =
            decode_hybrid_checkpoint(&encoded).expect("decode terminal enrichment");
        assert_eq!(
            terminal_enrichment.canonical_history.len(),
            terminal_config.canonical_history_capacity
        );

        let mut low = 1usize;
        let mut high = codec::MAX_CHECKPOINT_PAYLOAD_BYTES / 2;
        while low + 1 < high {
            let middle = low + (high - low) / 2;
            terminal_enrichment.historical_position.checkpoint = Some(vec![0x92; middle]);
            terminal_enrichment.live_position.checkpoint = Some(vec![0x82; middle]);
            if codec::checkpoint_payload_len_after_dropping_recent(&terminal_enrichment, 0)
                .expect("measure terminal-enrichment checkpoint")
                <= codec::MAX_CHECKPOINT_PAYLOAD_BYTES
            {
                low = middle;
            } else {
                high = middle;
            }
        }
        terminal_enrichment.historical_position.checkpoint = Some(vec![0x92; low]);
        terminal_enrichment.live_position.checkpoint = Some(vec![0x82; low]);
        encode_hybrid_checkpoint(&terminal_enrichment)
            .expect("terminal enrichment fits exactly below the bound");

        state.historical_position.checkpoint = Some(vec![0x12; low]);
        state.live_position.checkpoint = Some(vec![0x22; low]);
        let position = restore_position_for(&state);
        let historical_mutations = Arc::new(AtomicUsize::new(0));
        let live_mutations = Arc::new(AtomicUsize::new(0));
        let history = RestoreCountingSource {
            historical: true,
            durable: true,
            polls: Arc::new(AtomicUsize::new(0)),
            mutations: Arc::clone(&historical_mutations),
        };
        let live = RestoreCountingSource {
            historical: false,
            durable: true,
            polls: Arc::new(AtomicUsize::new(0)),
            mutations: Arc::clone(&live_mutations),
        };
        let config = HybridConfig {
            max_buffered_live_bytes: 1_024,
            max_source_delivery_token_bytes: 1,
            max_source_checkpoint_bytes: low,
            canonical_history_capacity: 4,
            ..HybridConfig::default()
        };
        let mut hybrid = HybridSubscriber::new(history, live, config).expect("coordinator");

        let error = hybrid
            .prepare_restore_base_lifecycle(&position, std::slice::from_ref(&interest))
            .await
            .expect_err("terminal reorg replacement must be reserved before child mutation");

        assert!(
            error.to_string().contains("maximum canonical-log delivery"),
            "unexpected terminal reorg capacity error: {error}"
        );
        assert_eq!(historical_mutations.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(live_mutations.load(AtomicOrdering::SeqCst), 0);
    }

    #[tokio::test]
    async fn synthetic_capacity_boundary_rejects_restore_before_child_mutation() {
        let interest = restore_test_interest();
        let mut state =
            fully_populated_checkpoint(HybridSource::Historical, HybridTokenKind::Forwarded);
        state.owner_generations.clear();
        state.lifecycle_intent = LifecycleIntent {
            base: interest_fingerprint::<Ethereum>(std::slice::from_ref(&interest))
                .expect("base fingerprint"),
            owners: BTreeMap::new(),
        };
        state.recent_inputs.clear();
        state.historical_position.delivery_token = Some(vec![0xe1]);
        state.historical_position.checkpoint = Some(vec![0xe2]);
        state.live_position.delivery_token = Some(vec![0x81]);
        state.live_position.checkpoint = Some(vec![0x82]);
        state.last_committed_token = Some(StoredCommittedToken {
            source: HybridSource::Historical,
            kind: HybridTokenKind::Forwarded,
            inner: vec![0xe1],
        });
        validate_checkpoint_state(&state).expect("single-byte restore candidate is valid");

        let sizing_config = HybridConfig {
            max_source_delivery_token_bytes: 1,
            max_source_checkpoint_bytes: 1,
            canonical_history_capacity: 3,
            ..HybridConfig::default()
        };
        let (history, live, _) = restore_counting_sources(Arc::new(AtomicUsize::new(0)));
        let sizing_hybrid =
            HybridSubscriber::new(history, live, sizing_config).expect("sizing coordinator");
        let encoded = sizing_hybrid
            .encode_maximum_canonical_log_delivery_variant(
                &state,
                HybridSource::Historical,
                HybridTokenKind::Forwarded,
            )
            .expect("one-byte forwarded probe initially fits");
        let mut forwarded =
            decode_hybrid_checkpoint(&encoded).expect("decode forwarded capacity probe");

        let mut low = 1usize;
        let mut high = codec::MAX_CHECKPOINT_PAYLOAD_BYTES / 2;
        while low + 1 < high {
            let middle = low + (high - low) / 2;
            forwarded
                .historical_position
                .checkpoint
                .as_mut()
                .expect("historical capacity-probe checkpoint")
                .resize(middle, 0xe2);
            forwarded
                .live_position
                .checkpoint
                .as_mut()
                .expect("live capacity-probe checkpoint")
                .resize(middle, 0x82);
            if codec::checkpoint_payload_len_after_dropping_recent(&forwarded, 0)
                .expect("measure forwarded boundary")
                <= codec::MAX_CHECKPOINT_PAYLOAD_BYTES
            {
                low = middle;
            } else {
                high = middle;
            }
        }
        drop(forwarded);

        let boundary_config = HybridConfig {
            max_source_delivery_token_bytes: 1,
            max_source_checkpoint_bytes: low,
            canonical_history_capacity: 3,
            ..HybridConfig::default()
        };
        let (history, live, _) = restore_counting_sources(Arc::new(AtomicUsize::new(0)));
        let boundary_hybrid =
            HybridSubscriber::new(history, live, boundary_config).expect("boundary coordinator");
        boundary_hybrid
            .encode_maximum_canonical_log_delivery_variant(
                &state,
                HybridSource::Historical,
                HybridTokenKind::Forwarded,
            )
            .expect("maximum forwarded-token state still fits");
        boundary_hybrid
            .encode_maximum_canonical_log_delivery_variant(
                &state,
                HybridSource::Historical,
                HybridTokenKind::Synthetic,
            )
            .expect_err("larger synthetic-token state alone must exceed the envelope");

        let position = restore_position_for(&state);
        let historical_mutations = Arc::new(AtomicUsize::new(0));
        let live_mutations = Arc::new(AtomicUsize::new(0));
        let history = RestoreCountingSource {
            historical: true,
            durable: true,
            polls: Arc::new(AtomicUsize::new(0)),
            mutations: Arc::clone(&historical_mutations),
        };
        let live = RestoreCountingSource {
            historical: false,
            durable: true,
            polls: Arc::new(AtomicUsize::new(0)),
            mutations: Arc::clone(&live_mutations),
        };
        let mut hybrid =
            HybridSubscriber::new(history, live, boundary_config).expect("restore coordinator");
        let error = hybrid
            .prepare_restore_base_lifecycle(&position, std::slice::from_ref(&interest))
            .await
            .expect_err("synthetic-token headroom must be proven before child mutation");

        assert!(
            error.to_string().contains("synthetic-token"),
            "unexpected synthetic capacity error: {error}"
        );
        assert_eq!(historical_mutations.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(live_mutations.load(AtomicOrdering::SeqCst), 0);
    }

    #[tokio::test]
    async fn owner_restore_rejects_a_checkpoint_without_one_record_headroom_before_mutation() {
        let interest = restore_test_interest();
        let owner = HandlerId::new("near-limit-owner");
        let mut state =
            fully_populated_checkpoint(HybridSource::Historical, HybridTokenKind::Forwarded);
        state.owner_generations = BTreeMap::from([(owner.clone(), state.lifecycle_generation)]);
        state.lifecycle_intent = LifecycleIntent {
            base: empty_interest_fingerprint(),
            owners: BTreeMap::from([(
                owner.clone(),
                interest_fingerprint::<Ethereum>(std::slice::from_ref(&interest))
                    .expect("owner fingerprint"),
            )]),
        };
        state.recent_inputs.clear();
        fill_opaque_checkpoint_to_v5_limit(&mut state);
        let original_recent = state.recent_inputs.clone();
        let position = restore_position_for(&state);
        let live_mutations = Arc::new(AtomicUsize::new(0));
        let (history, live, historical_mutations) =
            restore_counting_sources(Arc::clone(&live_mutations));
        let mut hybrid = HybridSubscriber::new(history, live, maximum_opaque_checkpoint_config())
            .expect("coordinator");

        let error = hybrid
            .prepare_restore_lifecycle(&position, &[], vec![(owner, vec![interest])])
            .await
            .expect_err("one protected owner witness cannot fit");

        assert!(
            error
                .to_string()
                .contains("cannot retain one protected delivery witness"),
            "unexpected capacity error: {error}"
        );
        assert_eq!(historical_mutations.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(live_mutations.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(
            decode_hybrid_checkpoint(
                position
                    .subscriber_checkpoint
                    .as_ref()
                    .expect("checkpoint")
                    .as_bytes()
            )
            .expect("decode unchanged checkpoint")
            .recent_inputs,
            original_recent
        );
    }

    #[tokio::test]
    async fn restore_preflight_accounts_for_runtime_history_growth_before_live_mutation() {
        let interest = restore_test_interest();
        let mut state =
            fully_populated_checkpoint(HybridSource::Historical, HybridTokenKind::Forwarded);
        state.owner_generations.clear();
        state.lifecycle_intent = LifecycleIntent {
            base: interest_fingerprint::<Ethereum>(std::slice::from_ref(&interest))
                .expect("base fingerprint"),
            owners: BTreeMap::new(),
        };
        state.recent_inputs.clear();
        state.canonical_history = vec![state.coverage_head.expect("fixture coverage")];
        fill_opaque_checkpoint_to_v5_limit(&mut state);
        let opaque = state
            .historical_position
            .checkpoint
            .as_mut()
            .expect("opaque historical checkpoint");
        opaque.truncate(opaque.len().saturating_sub(4_096));
        encode_hybrid_checkpoint(&state).expect("short-history fixture remains valid");

        let mut position = restore_position_for(&state);
        position.canonical_history = (0..=102)
            .map(|number| {
                let mut block = golden_block(number, number as u8, number.saturating_sub(1) as u8);
                if number == 0 {
                    block.parent_hash = None;
                }
                block
            })
            .collect();
        let live_mutations = Arc::new(AtomicUsize::new(0));
        let (history, live, historical_mutations) =
            restore_counting_sources(Arc::clone(&live_mutations));
        let mut hybrid = HybridSubscriber::new(history, live, maximum_opaque_checkpoint_config())
            .expect("coordinator");

        let error = hybrid
            .prepare_restore_base_lifecycle(&position, std::slice::from_ref(&interest))
            .await
            .expect_err("installed runtime history must leave one-record durable headroom");

        assert!(
            error.to_string().contains("protected delivery witness"),
            "unexpected capacity error: {error}"
        );
        assert_eq!(historical_mutations.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(live_mutations.load(AtomicOrdering::SeqCst), 0);
    }

    #[tokio::test]
    async fn restore_preflight_discards_only_an_ephemeral_live_cursor_before_capacity_proof() {
        let interest = restore_test_interest();
        let mut state =
            fully_populated_checkpoint(HybridSource::Historical, HybridTokenKind::Forwarded);
        state.owner_generations.clear();
        state.lifecycle_intent = LifecycleIntent {
            base: interest_fingerprint::<Ethereum>(std::slice::from_ref(&interest))
                .expect("base fingerprint"),
            owners: BTreeMap::new(),
        };
        state.recent_inputs.clear();
        fill_ephemeral_live_checkpoint_to_v5_limit(&mut state);
        let position = restore_position_for(&state);
        let live_mutations = Arc::new(AtomicUsize::new(0));
        let (history, live, historical_mutations) =
            restore_counting_sources(Arc::clone(&live_mutations));
        let mut hybrid =
            HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");

        hybrid
            .prepare_restore_base_lifecycle(&position, std::slice::from_ref(&interest))
            .await
            .expect("discarded ephemeral cursor leaves delivery headroom");
        hybrid
            .restore_position(&position)
            .expect("restore normalized candidate");

        let restored = hybrid.checkpoint_state();
        assert!(restored.live_position.delivery_token.is_none());
        assert!(restored.live_position.checkpoint.is_none());
        assert!(restored.live_position.delivery_digest.is_none());
        assert_eq!(historical_mutations.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(live_mutations.load(AtomicOrdering::SeqCst), 1);
    }

    #[tokio::test]
    async fn restore_rejects_an_oversized_historical_cursor_before_child_mutation() {
        let interest = restore_test_interest();
        let mut state =
            fully_populated_checkpoint(HybridSource::Historical, HybridTokenKind::Forwarded);
        state.owner_generations.clear();
        state.lifecycle_intent = LifecycleIntent {
            base: interest_fingerprint::<Ethereum>(std::slice::from_ref(&interest))
                .expect("base fingerprint"),
            owners: BTreeMap::new(),
        };
        state.recent_inputs.clear();
        state.historical_position.delivery_token = Some(vec![0x51; 33]);
        state.last_committed_token = Some(StoredCommittedToken {
            source: HybridSource::Historical,
            kind: HybridTokenKind::Forwarded,
            inner: vec![0x51; 33],
        });
        let position = restore_position_for(&state);
        let live_mutations = Arc::new(AtomicUsize::new(0));
        let (history, live, historical_mutations) =
            restore_counting_sources(Arc::clone(&live_mutations));
        let config = HybridConfig {
            max_source_delivery_token_bytes: 32,
            ..HybridConfig::default()
        };
        let mut hybrid = HybridSubscriber::new(history, live, config).expect("coordinator");

        let error = hybrid
            .prepare_restore_base_lifecycle(&position, std::slice::from_ref(&interest))
            .await
            .expect_err("oversized historical token must fail pure restore validation");

        assert!(error.to_string().contains("opaque cursor bound"));
        assert_eq!(historical_mutations.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(live_mutations.load(AtomicOrdering::SeqCst), 0);
    }

    #[tokio::test]
    async fn restore_rejects_an_oversized_durable_live_cursor_before_child_mutation() {
        let interest = restore_test_interest();
        let mut state =
            fully_populated_checkpoint(HybridSource::Historical, HybridTokenKind::Forwarded);
        state.owner_generations.clear();
        state.lifecycle_intent = LifecycleIntent {
            base: interest_fingerprint::<Ethereum>(std::slice::from_ref(&interest))
                .expect("base fingerprint"),
            owners: BTreeMap::new(),
        };
        state.recent_inputs.clear();
        state.live_position.checkpoint = Some(vec![0x61; 33]);
        let position = restore_position_for(&state);
        let historical_mutations = Arc::new(AtomicUsize::new(0));
        let live_mutations = Arc::new(AtomicUsize::new(0));
        let history = RestoreCountingSource {
            historical: true,
            durable: true,
            polls: Arc::new(AtomicUsize::new(0)),
            mutations: Arc::clone(&historical_mutations),
        };
        let live = RestoreCountingSource {
            historical: false,
            durable: true,
            polls: Arc::new(AtomicUsize::new(0)),
            mutations: Arc::clone(&live_mutations),
        };
        let config = HybridConfig {
            max_source_checkpoint_bytes: 32,
            ..HybridConfig::default()
        };
        let mut hybrid = HybridSubscriber::new(history, live, config).expect("coordinator");

        let error = hybrid
            .prepare_restore_base_lifecycle(&position, std::slice::from_ref(&interest))
            .await
            .expect_err("oversized durable-live checkpoint must fail pure restore validation");

        assert!(error.to_string().contains("opaque cursor bound"));
        assert_eq!(historical_mutations.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(live_mutations.load(AtomicOrdering::SeqCst), 0);
    }

    #[tokio::test]
    async fn restore_capacity_simulation_never_evicts_installed_recent_witnesses() {
        let interest = restore_test_interest();
        let owner = HandlerId::new("preserved-owner");
        let mut state =
            fully_populated_checkpoint(HybridSource::Historical, HybridTokenKind::Forwarded);
        state.owner_generations = BTreeMap::from([(owner.clone(), state.lifecycle_generation)]);
        state.lifecycle_intent = LifecycleIntent {
            base: empty_interest_fingerprint(),
            owners: BTreeMap::from([(
                owner.clone(),
                interest_fingerprint::<Ethereum>(std::slice::from_ref(&interest))
                    .expect("owner fingerprint"),
            )]),
        };
        for entry in &mut state.recent_inputs {
            entry.coverage.base = false;
            entry.coverage.owners = BTreeMap::from([(owner.clone(), state.lifecycle_generation)]);
        }
        let original_recent = state.recent_inputs.clone();
        let position = restore_position_for(&state);
        let live_mutations = Arc::new(AtomicUsize::new(0));
        let (history, live, _) = restore_counting_sources(Arc::clone(&live_mutations));
        let mut hybrid =
            HybridSubscriber::new(history, live, HybridConfig::default()).expect("coordinator");

        hybrid
            .prepare_restore_lifecycle(&position, &[], vec![(owner, vec![interest])])
            .await
            .expect("prepare owner restore");
        hybrid
            .restore_position(&position)
            .expect("install prepared restore");

        assert_eq!(hybrid.checkpoint_state().recent_inputs, original_recent);
        assert_eq!(live_mutations.load(AtomicOrdering::SeqCst), 1);
    }

    #[tokio::test]
    async fn near_limit_restored_effective_empty_owner_topology_does_not_poll_sources() {
        let owner = HandlerId::new("restored-empty-owner");
        let mut state =
            fully_populated_checkpoint(HybridSource::Historical, HybridTokenKind::Forwarded);
        state.owner_generations = BTreeMap::from([(owner.clone(), state.lifecycle_generation)]);
        state.lifecycle_intent = LifecycleIntent {
            base: empty_interest_fingerprint(),
            owners: BTreeMap::from([(owner.clone(), empty_interest_fingerprint())]),
        };
        state.recent_inputs.clear();
        fill_opaque_checkpoint_to_v5_limit(&mut state);
        let checkpoint = SubscriberCheckpoint::new(
            encode_hybrid_checkpoint(&state).expect("encode empty-owner checkpoint"),
        );
        let token = wrap_token(
            state.epoch,
            HybridSource::Historical,
            HybridTokenKind::Forwarded,
            SubscriberDeliveryToken::new(vec![0xf1, 0xf2, 0xf3]),
        );
        let position = SubscriberResumePosition::new(
            state.chain_id,
            state.coverage_head.expect("golden coverage"),
            state.canonical_history.clone(),
            Some(token),
            Some(checkpoint),
        );
        let historical_polls = Arc::new(AtomicUsize::new(0));
        let live_polls = Arc::new(AtomicUsize::new(0));
        let historical_mutations = Arc::new(AtomicUsize::new(0));
        let live_mutations = Arc::new(AtomicUsize::new(0));
        let history = RestoreCountingSource {
            historical: true,
            durable: true,
            polls: Arc::clone(&historical_polls),
            mutations: Arc::clone(&historical_mutations),
        };
        let live = RestoreCountingSource {
            historical: false,
            durable: false,
            polls: Arc::clone(&live_polls),
            mutations: Arc::clone(&live_mutations),
        };
        let mut hybrid = HybridSubscriber::new(history, live, maximum_opaque_checkpoint_config())
            .expect("coordinator");

        hybrid
            .prepare_restore_lifecycle(&position, &[], vec![(owner, Vec::new())])
            .await
            .expect("prepare empty owner topology");
        hybrid
            .restore_position(&position)
            .expect("restore empty owner topology");
        let delivery = hybrid.next_batch().await.expect("poll restored topology");

        assert!(delivery.is_none());
        assert_eq!(historical_polls.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(live_polls.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(hybrid.phase(), HybridPhase::Live);

        let historical_before = historical_mutations.load(AtomicOrdering::SeqCst);
        let live_before = live_mutations.load(AtomicOrdering::SeqCst);
        let error = hybrid
            .add_interest_owner(
                HandlerId::new("activated-after-empty-restore"),
                &[restore_test_interest()],
            )
            .await
            .expect_err("activation must re-prove exact preserved checkpoint headroom");
        assert!(
            error.to_string().contains("one protected delivery witness"),
            "unexpected activation capacity error: {error}"
        );
        assert_eq!(
            historical_mutations.load(AtomicOrdering::SeqCst),
            historical_before
        );
        assert_eq!(live_mutations.load(AtomicOrdering::SeqCst), live_before);
    }

    #[tokio::test]
    async fn base_destructive_and_incremental_capacity_failures_precede_child_mutation() {
        let historical_mutations = Arc::new(AtomicUsize::new(0));
        let live_mutations = Arc::new(AtomicUsize::new(0));
        let history = RestoreCountingSource {
            historical: true,
            durable: true,
            polls: Arc::new(AtomicUsize::new(0)),
            mutations: Arc::clone(&historical_mutations),
        };
        let live = RestoreCountingSource {
            historical: false,
            durable: false,
            polls: Arc::new(AtomicUsize::new(0)),
            mutations: Arc::clone(&live_mutations),
        };
        let config = HybridConfig {
            max_source_checkpoint_bytes: HYBRID_MAX_SOURCE_CHECKPOINT_BYTES,
            ..HybridConfig::default()
        };
        let mut hybrid = HybridSubscriber::new(history, live, config).expect("coordinator");

        let error = hybrid
            .register_interests(&[restore_test_interest()])
            .await
            .expect_err("effective-empty base activation must preflight maximum cursor reserve");

        assert!(
            error.to_string().contains("maximum canonical-log delivery"),
            "unexpected base preflight error: {error}"
        );
        assert_eq!(historical_mutations.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(live_mutations.load(AtomicOrdering::SeqCst), 0);
        assert!(hybrid.base_interests.is_empty());

        for destructive in [true, false] {
            let historical_mutations = Arc::new(AtomicUsize::new(0));
            let live_mutations = Arc::new(AtomicUsize::new(0));
            let history = RestoreCountingSource {
                historical: true,
                durable: true,
                polls: Arc::new(AtomicUsize::new(0)),
                mutations: Arc::clone(&historical_mutations),
            };
            let live = RestoreCountingSource {
                historical: false,
                durable: false,
                polls: Arc::new(AtomicUsize::new(0)),
                mutations: Arc::clone(&live_mutations),
            };
            let config = HybridConfig {
                max_source_checkpoint_bytes: HYBRID_MAX_SOURCE_CHECKPOINT_BYTES,
                ..HybridConfig::default()
            };
            let mut hybrid = HybridSubscriber::new(history, live, config).expect("coordinator");
            let owner = HandlerId::new(if destructive {
                "destructive-capacity-owner"
            } else {
                "incremental-capacity-owner"
            });
            let interest = restore_test_interest();

            let error = if destructive {
                hybrid
                    .replace_interest_owners(vec![(owner, vec![interest])])
                    .await
                    .expect_err("destructive replacement must preflight maximum cursor reserve")
            } else {
                hybrid
                    .add_interest_owner(owner, &[interest])
                    .await
                    .expect_err("incremental change must preflight maximum cursor reserve")
            };

            assert!(
                error.to_string().contains("maximum canonical-log delivery"),
                "unexpected topology preflight error: {error}"
            );
            assert_eq!(historical_mutations.load(AtomicOrdering::SeqCst), 0);
            assert_eq!(live_mutations.load(AtomicOrdering::SeqCst), 0);
            assert!(hybrid.owners.is_empty());
        }
    }

    #[test]
    fn empty_owner_topology_still_requires_restore_preparation() {
        let intent = LifecycleIntent {
            base: empty_interest_fingerprint(),
            owners: BTreeMap::from([(HandlerId::new("empty-owner"), empty_interest_fingerprint())]),
        };

        assert!(!intent.has_active_interests());
        assert!(intent.requires_restore_preparation());
    }

    #[test]
    fn older_historical_page_cannot_regress_certified_coverage() {
        let mut state =
            fully_populated_checkpoint(HybridSource::Historical, HybridTokenKind::Forwarded);
        let higher = state
            .certified_historical
            .expect("fixture carries higher historical proof");
        let lower = state.canonical_history[1];
        let commit = PendingCoordinatorCommit {
            audiences: Vec::new(),
            canonical: Vec::new(),
            source: HybridSource::Historical,
            source_token: Some(vec![0x11]),
            source_checkpoint: Some(vec![0x22]),
            source_progress: Some(lower),
            source_observed_through: Some(lower),
            token_kind: HybridTokenKind::Forwarded,
            token_bytes: vec![0x11],
            source_delivery_digest: [0x33; 32],
            next_safe_head: state.safe_head,
            next_finalized_head: state.finalized_head,
            next_canonical_history: state.canonical_history.clone(),
            next_coverage_head: state.coverage_head,
        };

        apply_commit_to_checkpoint(&mut state, &commit, 32).expect("apply older page");
        let encoded = encode_hybrid_checkpoint(&state).expect("persist monotonic proof");
        let restored = decode_hybrid_checkpoint(&encoded).expect("restore monotonic proof");

        assert_eq!(restored.certified_historical, Some(higher));
    }

    #[test]
    fn same_height_historical_proof_merges_compatible_metadata() {
        let mut state =
            fully_populated_checkpoint(HybridSource::Historical, HybridTokenKind::Forwarded);
        let through = state.coverage_head.expect("fixture coverage");
        state.certified_historical = Some(CertifiedHistoricalCoverage {
            lifecycle_generation: state.lifecycle_generation,
            through: BlockRef {
                parent_hash: None,
                timestamp: None,
                ..through
            },
        });
        let commit = PendingCoordinatorCommit {
            audiences: Vec::new(),
            canonical: Vec::new(),
            source: HybridSource::Historical,
            source_token: Some(vec![0x12]),
            source_checkpoint: Some(vec![0x23]),
            source_progress: Some(through),
            source_observed_through: Some(through),
            token_kind: HybridTokenKind::Forwarded,
            token_bytes: vec![0x12],
            source_delivery_digest: [0x34; 32],
            next_safe_head: state.safe_head,
            next_finalized_head: state.finalized_head,
            next_canonical_history: state.canonical_history.clone(),
            next_coverage_head: state.coverage_head,
        };

        apply_commit_to_checkpoint(&mut state, &commit, 32).expect("merge proof metadata");

        assert_eq!(
            state.certified_historical,
            Some(CertifiedHistoricalCoverage {
                lifecycle_generation: state.lifecycle_generation,
                through,
            })
        );
        encode_hybrid_checkpoint(&state).expect("merged proof remains durably valid");
    }

    #[test]
    fn same_height_historical_proof_rejects_conflicting_metadata() {
        let partial = BlockRef {
            number: 10,
            hash: B256::repeat_byte(0xaa),
            parent_hash: None,
            timestamp: None,
        };
        let certified = BlockRef {
            timestamp: Some(100),
            ..partial
        };
        let conflicting = BlockRef {
            timestamp: Some(101),
            ..partial
        };
        let mut state = checkpoint();
        state.canonical_history = vec![partial];
        state.coverage_head = Some(partial);
        state.historical_position = SourcePosition {
            coverage_head: Some(partial),
            canonical_history: vec![partial],
            ..SourcePosition::default()
        };
        state.certified_historical = Some(CertifiedHistoricalCoverage {
            lifecycle_generation: state.lifecycle_generation,
            through: certified,
        });
        validate_checkpoint_state(&state).expect("partial checkpoint is valid");
        let commit = PendingCoordinatorCommit {
            audiences: Vec::new(),
            canonical: Vec::new(),
            source: HybridSource::Historical,
            source_token: None,
            source_checkpoint: None,
            source_progress: Some(conflicting),
            source_observed_through: Some(conflicting),
            token_kind: HybridTokenKind::Synthetic,
            token_bytes: 1_u64.to_be_bytes().to_vec(),
            source_delivery_digest: [0x35; 32],
            next_safe_head: None,
            next_finalized_head: None,
            next_canonical_history: vec![conflicting],
            next_coverage_head: Some(conflicting),
        };

        let error = apply_commit_to_checkpoint(&mut state, &commit, 32)
            .expect_err("conflicting proof metadata must fail closed");

        assert!(error.to_string().contains("timestamp conflicts"));
    }

    #[test]
    fn default_recent_window_fits_the_durable_checkpoint_envelope_for_one_owner() {
        let config = HybridConfig::default();
        let owner = HandlerId::new("representative-owner");
        let mut state = checkpoint();
        state.owner_generations.insert(owner.clone(), 3);
        state.lifecycle_intent.owners.insert(
            owner.clone(),
            interest_fingerprint::<Ethereum>(&[]).expect("empty owner interests"),
        );
        state.recent_inputs = (0..config.recent_input_capacity)
            .map(|index| {
                let block = BlockRef {
                    number: index as u64,
                    hash: B256::repeat_byte(index as u8),
                    parent_hash: None,
                    timestamp: None,
                };
                let record = ReactiveInputRecord::<Ethereum>::new(
                    ReactiveInput::Log(Log {
                        inner: PrimitiveLog::new_unchecked(
                            Address::repeat_byte(0xaa),
                            vec![B256::repeat_byte(0xbb)],
                            Bytes::new(),
                        ),
                        block_hash: Some(block.hash),
                        block_number: Some(block.number),
                        block_timestamp: None,
                        transaction_hash: Some(B256::repeat_byte(index.wrapping_add(1) as u8)),
                        transaction_index: Some(0),
                        log_index: Some(index as u64),
                        removed: false,
                    }),
                    evm_fork_cache::reactive::ReactiveContext {
                        chain_id: Some(1),
                        source: evm_fork_cache::reactive::InputSource::Backfill,
                        chain_status: ChainStatus::Included {
                            block,
                            confirmations: 0,
                        },
                        block: Some(block),
                        transaction_index: Some(0),
                        log_index: Some(index as u64),
                    },
                );
                StoredRecentInput {
                    identity: record.validated_identity().expect("valid test identity"),
                    coverage: AudienceCoverage {
                        base: false,
                        owners: BTreeMap::from([(owner.clone(), 3)]),
                        block: Some(block),
                        witness: Some(record_witness(&record, 1).expect("test witness")),
                    },
                }
            })
            .collect();
        let head = BlockRef {
            number: config.recent_input_capacity.saturating_sub(1) as u64,
            hash: B256::repeat_byte(config.recent_input_capacity.saturating_sub(1) as u8),
            parent_hash: None,
            timestamp: None,
        };
        state.canonical_history.push(head);
        state.coverage_head = Some(head);

        let encoded =
            encode_hybrid_checkpoint(&state).expect("default journal must remain checkpointable");
        decode_hybrid_checkpoint(&encoded)
            .expect("the bounded decoder must accept every default-sized encoded journal");
    }
}
