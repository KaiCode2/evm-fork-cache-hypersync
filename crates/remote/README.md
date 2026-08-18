# evm-fork-cache-remote

Remote and hybrid `EventSubscriber` implementations for
[`evm-fork-cache`](https://crates.io/crates/evm-fork-cache).

`RemoteSubscriber` connects to an `evm-fork-cache-event-service`, commits
revisioned interest changes, reconnects interrupted tonic streams, restores the
service-authoritative session, maps portable deliveries into ordered reactive
inputs, and acknowledges only the delivery token committed by the runtime.
Persisted runtimes should use `connect_from_position` (or its bearer variant):
the restored eight-byte big-endian delivery sequence plus its token/checkpoint
proof is sent in `Hello`. If the runtime saved delivery N before its service ACK
committed, the service may be exactly one sequence behind and replays its exact
pending N; any other cursor/checkpoint mismatch fails closed. The lower-level
tonic transport also exposes configurable connect, handshake, absolute
whole-operation control-response, HTTP/2 keepalive, and reconnect settings.
Heartbeats and reconnects cannot extend a control deadline. Idle delivery waits
remain intentionally unbounded, while HTTP/2 PINGs detect half-open channels.
Reconnects require the same session, revision, cursor, desired state, and source
capabilities. Advertised service limits are mutable admission policy rather
than durable authority; the latest reconnect snapshot replaces the prior one
for subsequent operations.
The convenience `connect` constructors may inspect an existing session so the
core or a hybrid coordinator can then call its synchronous `restore_position`
hook, but they cannot apply interests, poll, or acknowledge when the service is
already ahead of sequence zero. This prevents a fresh cache that accidentally
reuses a durable session id from silently skipping earlier effects.

Provider-neutral transports implement `RemoteEventTransport::capabilities` to
publish the runtime guarantees negotiated with their authority. The generic
`RemoteSubscriber` constructors snapshot those capabilities, allowing a custom
gRPC, message-bus, or future stream transport to participate in Hybrid's
historical/durable capability checks. The default capability set is empty and
therefore fails closed; return only guarantees the transport enforces end to
end, and reconstruct the subscriber after renegotiating a materially different
set.

Transport trust does not extend to record routing. Every noncanonical audience
is checked against the subscriber's complete authoritative desired state before
decoding, including deliveries supplied by custom in-process transports; an
empty, duplicated, malformed, or unknown owner identity fails closed.

Remote cursors keep the provider's exclusive scan position separate from the
runtime-global canonical head. A new-revision, blockless activation barrier may
rewind only the scan for owner catch-up while preserving that head. Subsequent
owner-only pages and exact `source-progress:<revision>:<next_block>` barriers
may advance the scan without claiming global coverage; every other head drop,
rewind, or same-height identity change fails closed. Every decoded data or
control batch is stamped with the negotiated chain id before it reaches the
runtime.

Only exact blockless `desired-state:<revision>` activation barriers and exact
blockless scan-only `source-progress:<revision>:<next_block>` barriers are
checkpoint-neutral. The remote subscriber acknowledges both classes internally
so they advance transport authority without becoming runtime/cache inputs or
runtime checkpoint positions; all other deliveries are checkpoint-bearing.

Owner backfill intent is durable lifecycle state. `RemoteSubscriber` restores
it from the service-authoritative desired state and keeps later lifecycle
changes fenced until the durable acknowledged cursor proves catch-up reached
the preserved head successor. It then omits the completed backfill from the
next full-state replacement. Repeating the exact last committed delivery token
is idempotent, while stale or mismatched tokens remain errors.

`HybridSubscriber` combines any durable historical subscriber with a
low-latency live subscriber. It buffers live input during catch-up, cuts over at
an acknowledgement-gated canonical fence, deduplicates only committed overlap,
and re-enters historical recovery when the live source fails. Its versioned
checkpoint retains both runtime-visible source positions, canonical history,
token epoch, lifecycle fingerprints, owner generations, and complete
audience-aware payload witnesses across a runtime restart. Hybrid checkpoint
format V5 is independent of gRPC protocol V1 and uses a private canonical RLP
schema inside an 18-byte magic/version/length/CRC32 envelope. Its payload is
limited to 16 MiB; decoding also enforces count, identifier, node, and 64 MiB
accounted-heap budgets. The heap budget is conservative codec accounting, not a
process RSS ceiling; allocator overhead, stack state, shared input buffers, and
the surrounding runtime are additional. Unpublished V4 bincode checkpoints are
rejected and require authoritative resynchronization. CRC32 detects accidental
corruption only; it is not a MAC or signature. The durable checkpoint path is
trusted state and must be protected from attacker writes. Restore validates
that state against the runtime's retained canonical history, configured
windows, opaque child-cursor budgets, and exact outer token before either child
may expose data. `max_source_delivery_token_bytes` and
`max_source_checkpoint_bytes` are per-child ingress and restore ceilings (64
KiB and 1 MiB by default); the hard configurable ceiling for either field is
the V5 16-MiB payload limit, but the combined checkpoint must still fit its
single shared envelope. Buffer overflow, committed-token reuse, conflicting
payloads, unverifiable overlap/reorg history, malformed routing/control
metadata, or a chain mismatch poisons delivery and fails closed.

Replay and lifecycle fingerprints use separate domain-separated canonical
transcripts rather than Rust or Protobuf object serialization. Delivery
fingerprints include exact source, confirmations, context positions, routing,
scope, controls, child checkpoint, and payload commitment.

Removed/reorg lifecycle signals are deliberately excluded from cross-batch
deduplication, so a remove/re-include/remove sequence remains fully observable.

After restore, Hybrid retries any child acknowledgement already proven by the
outer checkpoint before polling either source. This closes the lost-ACK window
without emitting an empty replacement batch under the committed outer token.
An effective-empty topology revision emits one coordinator-local synthetic
lifecycle barrier without polling either child. The byte-identical barrier
replays until ACK and durably commits the new generation; only then does
`next_batch` return `Ok(None)`. Its checkpoint preserves child resume
checkpoints/canonical suffixes, clears already-ACKed raw tokens, and adds no
source coverage. A restored, already-ACKed effective-empty lifecycle returns
`Ok(None)` immediately without source polling. A newly constructed coordinator
whose lifecycle has never changed is already empty and returns `Ok(None)`
without manufacturing a revision barrier.

Hybrid does not advertise block-header registration, even when both children
do. A manually supplied header is accepted only as a defensive path with the
child's payload commitment. Generic block-header equality is then bound to the
complete handler-visible response body, not only its hash and selected context
fields. Hybrid first bounds its compact JSON representation by both the
configured ingress ceiling and a hard 256 KiB canonicalization ceiling,
canonicalizes object-key order, then streams the canonical representation into a
domain-separated digest and exact byte counter. Same-batch, cross-source, and
restored-replay bodies with one identity but different fields therefore fail
closed, and unusually large headers consume their real encoded bytes instead
of a fixed allowance. This witness is checked in addition to the child's batch
payload commitment, not as a substitute for it.

Child chain controls use the core runtime's strict phase order: every explicit
`Reorg` must form a prefix before replacement records, while
`CanonicalProgress`, blockful barriers, safe, and finalized controls are
post-record. Hybrid commits checkpoint mutations in that same order. This keeps
sparse records through block `G` followed by a zero-log progress tail `H`
pinned at `H` and rejects ambiguous progress-then-reorg envelopes.
Explicit reorgs must name the current old tip, describe different non-empty
branches, and preserve parent adjacency. Retained history may be sparse: an
unretained common ancestor is accepted only when it lies within the retained
rollback horizon and does not conflict with an identity known at that height.
Sparse gaps are therefore a trust boundary, not a locally reconstructed ancestry
proof: the sequenced durable historical source remains authoritative between
known heights. Only core's structured `IncompleteRollback` diagnostic enters
recovery; structured `Invalid` contradictions poison immediately, without
matching error strings.
Alloy removed logs commonly omit `parent_hash`; Hybrid resolves those only by
matching the removed block's exact retained number/hash, then rewinds to the
last retained predecessor even when the journal is sparse. If retained history
cannot prove that predecessor, delivery enters historical recovery rather than
guessing. A delayed removed log for a branch already displaced by certified
history is suppressed instead of rewinding the replacement branch again.

The historical half must advertise historical backfill, durable replay, and
barriers and must tokenize every delivery. If the live half is itself
acknowledgement-gated, the coordinator polls it only once while buffered and
temporarily blocks lifecycle changes until that item drains. Safe/finalized
signals and blockless barriers do not establish catch-up coverage. Forwarded
tokens require restorable canonical coverage, so a tokened blockless historical
barrier is rejected. A tokened blockless live item during catch-up is likewise
rejected instead of deadlocking behind an unprovable fence. The coordinator does advertise durable replay when the
historical half provides it: this guarantees recovery to the same canonical
position through history, not byte-identical replay of an ephemeral live
envelope.
If an acknowledged historical page reaches the required boundary before the
first post-registration live item establishes its fence, Hybrid retains that
proof for the current lifecycle generation and cuts over when the late fence
arrives. It does not require a redundant historical page merely because the two
sources won the race in that order. A later lower historical page cannot reduce
that certified height on the same lifecycle/branch; equal-height metadata is
merged only when compatible, while a crossing reorg/reset clears the proof.

Restoring active base state or any owner topology requires an asynchronous
preparation step before the synchronous core restore hook. Load and
RPC-validate the checkpoint, reconstruct the same fresh engine and exact runtime
lifecycle mode, then obtain the position with
`ReactiveEngine::preview_durable_resume_position`. Base-only
integrations call
`prepare_restore_base_lifecycle(&position, base_interests).await`, which needs
only `EventSubscriber` children. Owner-mode integrations always call
`prepare_restore_lifecycle(&position, &[], owners).await`, then restore the
loaded checkpoint through `ReactiveEngine::restore_durable_checkpoint` (or pass
the same metadata to `resume_from_durable_checkpoint` when cache restoration is
already complete), even when every installed owner's filter list is empty.
Preview, preparation, and restore must use one unmutated fresh engine and the
same metadata. Preparation checks deterministic
provider-portable interest fingerprints, exactly replaces the selected topology
on an ephemeral live child (removing stale base or owner state) before recovery,
and deliberately does not mutate the durable historical authority. A durable
live child is restored from its own cursor. Partial preparation/restore is
fenced and accepts only an exact retry. For an ephemeral live child, restore
also clears the pre-crash raw delivery token/checkpoint/digest and rotates the
outer epoch while retaining canonical overlap; a new process may therefore
reuse child token `1` without being mistaken for ACK-only replay.
Preparation and restore share one normalization path: the runtime canonical
history is merged and bounded first, durable child cursor state is preserved,
and only an ephemeral live cursor is removed. Active topology then constructs
a valid fully saturated durable state: coordinator, historical-source, and
live-source histories all contain `canonical_history_capacity` distinct,
linked, fully populated block references ending at `u64::MAX - 1`;
safe/finalized and historical certification are present; both opaque cursor
pairs use their configured maximum lengths and worst-case RLP byte form; and
the mutable synthetic counter has maximum encoded width. From that validated
state, four independent simulations drive the real one-record checkpoint
commit: Historical and Live, each with forwarded and synthetic token forms.
They exercise exact base or all-installed-owner fanout, the protected payload
witness, source progress, finality/certification, cursor replacement, the
duplicated forwarded token, and the eight-byte synthetic last token while
advancing all three histories to `u64::MAX`.

This fieldwise proof subsumes monotonic progress, terminal-height reorg
replacement, and arbitrary divergence among the candidate's retained source
suffixes without predicting one future ancestor. Both lifecycle activation and
restore must pass it before either child is mutated. The stronger contract may
reject unusually large history/cursor configurations even when the current
checkpoint is sparse; the default capacity of 512 is intended for this reserve.
It guarantees one largest supported record from the fully saturated configured
state, not an arbitrary multi-record batch; every real batch still receives its
exact checkpoint-fit check before output. Effective-empty owners still require
lifecycle preparation, but reserve no source-delivery space; a later base or
owner activation reruns the complete proof against the exact preserved
lifecycle state before child mutation.

Base/unowned interests and handler-owned interests are mutually exclusive
Hybrid lifecycle modes. Mixed restore input, direct non-empty mode replacement,
and mixed checkpoint state fail before either child is mutated. An effective-
empty topology itself enters `Live` immediately and does not poll either source,
but it first exposes one ACK-gated local lifecycle barrier. After that barrier
commits, the coordinator is idle. Empty owner identities remain part of the
durable topology. Only a completely
empty base/no-owner checkpoint may restore without preparation. After coverage exists, a
cleared base topology may activate owner mode only through global backfill;
owner-to-base activation requires a fresh coordinator at an authoritative
checkpoint because `EventSubscriber` has no symmetric base global-backfill
primitive. The standard `ReactiveEngine` path remains owner-only.

Lifecycle mutations are live-first. The complete effective-empty barrier and
its bounded V5 checkpoint are preflighted before either child is mutated, so a
coordinator-side encoding or budget failure cannot strand a committed child
topology. Cancellation or any uncertain child result
blocks delivery and ACK until a retry reconciles the previous complete state on
both children before applying the requested mutation again. Lifecycle intent is
durable once the local lifecycle barrier is checkpointed and ACKed (or, for an
active revision, once a later Hybrid source delivery contains it). If a remote
historical service committed a newer desired-state revision before that cache
checkpoint, its cursor/revision proof rejects restoration of the older state.
After canonical coverage, base-interest replacement is rejected because the
generic `EventSubscriber` contract has no atomic global-backfill rollback
primitive. Handler-owned topology replacement must use the global-backfill
form. If a destructive child reset then reports an uncertain partial commit,
Hybrid exact-restores the prior live topology, re-registers the prior historical
topology with a global `C + 1` backfill, discards the uncertain live queue, and
latches recovery until history certifies the gap. No delivery or ACK can escape
that compensation window.

That recoverable path is exclusive to the explicit topology-wide
global-backfill contract. For an ordinary base, owner, or bulk mutation after
acknowledged coverage, restoring the previous filters cannot certify that no
event crossed the interval where a previously active live filter changed or
disappeared. Once ordinary compensation succeeds, Hybrid therefore poisons
instead of reporting `Live`; reconstruct it from an authoritative checkpoint.
A brand-new owner or a previously empty owner remains safely retryable because
there was no prior active filter to interrupt.

For a handler added at canonical block `C`, the coordinated historical revision
uses owner-only routing only for the exact retained `C` anchor. The union of all
active interests from `C + 1` to the activation head is global canonical
progress, so old and new handler effects share the normal rollback journal.
Hybrid rejects owner-only input whose exact block number/hash is ahead of or
outside its retained canonical overlap before exposing or acknowledging it; the
core additionally requires the exact block to remain in its effect journal.
This path is deliberately not a deep-backfill mechanism.
`ReactiveEngine::register_handler` invokes it at a non-empty head: Hybrid
subscribes live first, forwards the retained baseline only through history, and
then enters acknowledgement-gated catch-up.

Exact full-topology replacement is exposed separately for bootstrap. Use the
no-history form only before the coordinator has canonical coverage. If the
runtime already embodies a canonical checkpoint but the subscriber service is
new or lost, use the core's global-backfill synchronization helper so the
replacement and one global post-baseline backfill commit together. Hybrid sends
the exact topology to live, the global backfill revision to history, removes
stale owner/base state, and recovers the complete prior topology plus any live
gap on an uncertain partial commit.

Subscriber payload commitments are preserved unchanged from the selected child
while Hybrid rewrites its outer token/checkpoint metadata. The commitment also
participates in the child-delivery fingerprint retained by the coordinator, so
a restored token whose canonical wire payload changed only in commitment still
fails closed. It is mandatory for every block-header batch: a header's hash and
context do not prove complete equality of a generic provider response body, and
Hybrid turns even a tokenless child delivery into an acknowledged outer batch.

Child implementations may move an internal transport or provider-scan cursor
during lifecycle work, including consuming a transport-only blockless barrier,
without producing a runtime batch. Hybrid records only emitted runtime commit
positions; it does not require internal cursor movements to appear in every
outer checkpoint. A durable child must keep those cursor roles distinct and
validate the runtime position supplied on restore.

The coordinator does not advertise full blocks, pending transactions, or
accept hydrated pending transactions. Compact block envelopes are
canonical-progress controls, never fabricated EVM headers; a `BlockHeader`
input is emitted only from complete hash-verified consensus-header RLP. Other
record capabilities, including finality, are advertised only when both children
support them. Record audience and delivery scope remain independent so owner
catch-up cannot move global chain state. Duplicate exact-owner lists, duplicate
exclusions, unknown owner ids, and owner-catch-up records without a non-empty
exact owner audience are rejected before buffering. An empty exact-owner list is
valid and removes every owner. A child `Some(batch)` with no records, controls,
or token is invalid; idleness is a pending poll or `Ok(None)`. A forwarded token
without canonical coverage is also invalid because it cannot produce a
restorable source position.

Size `recent_input_capacity` for the number of events across the maximum
live/history restart overlap, not merely for a number of blocks. Separately,
`max_recent_owner_entries` bounds the total `(handler, generation)` fanout
retained across those witnesses. The oldest complete witnesses are evicted
deterministically to satisfy both configured budgets and the hard V5
decode/16-MiB payload limits, but identities from the current delivery are
protected; a protected suffix that cannot fit fails before output. Thus dense
owner fanout can make the effective witness window shorter than
`recent_input_capacity`. Size `canonical_history_capacity` and the core
`ReactiveConfig::journal_depth` for the deepest supported reorg; the runtime
journal must also be at least the historical service's reorg window. An
overlap whose payload witness or block identity has aged out requires full
resynchronization rather than best-effort deduplication.

`max_buffered_live_records` and `max_buffered_live_bytes` also act as per-batch
ingress ceilings for both children. Hybrid computes the conservative count/byte
charge before validation constructs replay-digest buffers, charges block
headers by their exact streamed serialization, then applies the same limits
cumulatively to queued live overlap. This prevents an untrusted generic child
from forcing an unbounded second allocation with one delivery. Before walking
all controls, Hybrid derives a conservative control-count ceiling from the byte
budget and exits on the first byte overflow. It also bounds projected routing
work across the complete batch: `All` charges installed topology size,
`Owners` charges explicit ids, and `AllExcept` charges installed topology plus
explicit exclusions with checked addition. Filtering cannot be used to evade
that ingress budget. Delivery-token and nested-checkpoint lengths are checked
against their independent opaque-cursor ceilings before record witness or
transcript construction.

Child delivery-token bytes are immutable within the child's durable cursor
namespace. They must not be recycled after an intervening token, even for an
identical payload. Hybrid detects pending and immediately committed conflicts,
but does not retain an unbounded child-token history capable of recognizing
every `A -> B -> A` protocol violation; adapters must enforce this contract.

Licensed under MIT OR Apache-2.0.
