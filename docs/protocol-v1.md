# Event protocol v1 contract

Protocol v1 is provider-neutral. A runtime never receives a HyperSync query,
rollback guard, Firehose cursor, Kafka offset, or node-specific structure.

## Session authority

`Hello` identifies a durable `(session_id, chain_id)` pair. `HelloAccepted`
returns the service-authoritative revision, complete desired state,
acknowledged cursor, aggregate capabilities, source topology, and enforced
resource limits. A client whose local view differs restores the complete owner
set before its next full-state replacement rather than assuming an empty
service.

Authentication has two stages. Transport metadata is authenticated before the
stream is accepted, then `SessionAuthorizer::authorize_session` authorizes that
principal for the `(session_id, chain_id)` supplied by `Hello`. A bearer or mTLS
identity must not be treated as permission to lease every persisted session.
Persisted runtimes put the last checkpointed eight-byte sequence in `Hello` and
attach `PendingDeliveryResume`: the exact delivery token, optional opaque
provider checkpoint, and restored coverage head. Normally that sequence equals
the service's acknowledged cursor. The only permitted ahead state is exactly
one sequence whose token and cursor proof match the service's durable pending
outbox item. This is the crash window where runtime/cache state N was synced but
its service ACK did not commit; the service accepts the session and replays N so
the runtime can recognize and ACK it without re-ingestion. Missing, stale,
mismatched, or farther-ahead proofs fail closed. The proof is a consistency
check, not an authentication credential.

When the core restore hook runs after an initial convenience connection, the
client closes that request stream and renegotiates `Hello` with the restored
sequence and proof before allowing any apply, delivery, or acknowledgement
operation.
If that initial sequence-zero connection discovers a nonzero runtime-checkpoint
cursor, all three operation classes remain disabled until `restore_position`
proves it exactly. A higher transport cursor composed only of
checkpoint-neutral activation or scan-progress barriers does not require a
runtime proof because those barriers never mutated runtime/cache state. A fresh
cache therefore cannot attach past an old runtime-affecting delivery and begin
at its successor.

Desired state is a complete owner-scoped replacement guarded by
`expected_revision`. The event source prepares the candidate before SQLite is
mutated and may return the portable cursor that represents its provider-native
activation boundary. The store then commits the desired state and an activation
barrier seeded with that cursor in one transaction. The barrier has the exact
identifier `desired-state:<revision>` and no block claim. Its cursor may rewind
the provider scan position for a new owner backfill while preserving the prior
global canonical head byte-for-byte. This is the only non-reorg revision/scan
rewind admitted by the store and client. No data for the new revision can pass
that barrier until the runtime has ingested and acknowledged it.

Only one transport connection may lease a `(session_id, chain_id)` at a time.
This prevents two runtimes from consuming the same durable outbox. Preparation
candidates additionally carry unique process-local identities so concurrent or
aborted setup cannot activate another candidate's source engine.
The lease is acquired before persisted source preparation, and the source's
idempotent, cancellation-safe `release_session` hook runs at most once when a
negotiated leased stream exits, after all other source calls stop. Cleanup is
deadline-bounded. If it times out, the service fails closed by retaining that
lease until process restart, preventing a replacement generation from racing
partially released provider state.

If activation fails after SQLite commits a revision, the service closes the
stream with an unavailable status rather than reporting a definitive protocol
rejection. The client retains the exact candidate, reconnects, restores the
service-authoritative revision, and replays the idempotent apply.

## Delivery envelope

Every chain transition uses `Delivery`:

- monotonically increasing session sequence;
- delivery token equal to the unsigned sequence encoded as exactly eight
  big-endian bytes, acknowledged only after runtime ingestion;
- portable block/hash cursor for validation and observability;
- opaque provider checkpoint for native resume state;
- exactly one data, reorg, finality, or barrier payload.

The service maintains a one-item durable outbox. Delivery is demand-driven:
`next_batch` sends `DeliveryDemand`, and the service fetches or waits only while
that demand is outstanding. An idle historical source therefore cannot occupy
its outbox and block a later desired-state replacement. A reconnect replays the
exact encoded delivery without requiring new demand.
Repeated demand on the same stream also replays that exact outbox item until a
matching acknowledgement commits. A client retains its in-flight delivery so a
dropped ingestion future can repoll without reconnecting.

A runtime crash after its atomic cache save but before ACK leaves the service
cursor at N-1 and outbox at N. Resume keeps N-1 as the transport's delivery
base, replays the exact N envelope, and advances to N only after
`AcknowledgementCommitted`. It never treats the runtime's locally applied N as
already acknowledged by the service.

Acknowledging a reorg control durably records its exact `new_tip` as an
outstanding replacement promise. A reconnect or service restart cannot forget
that obligation. Checkpoint-neutral desired-state activation barriers may pass
without clearing it, but the next source delivery must certify that exact block
identity as an explicit full-header or compact-progress replacement anchor. A
data batch may end at a later head only when it explicitly carries every
continuous parent-linked descendant from that anchor through its terminal
cursor. A blockful progress barrier must stop at the exact anchor. The promise
remains present while that replacement is in the outbox and is cleared only by
the replacement's matching acknowledgement. A second reorg, scan-only barrier,
omitted/conflicting anchor, gapped suffix, or unproven descendant fails closed.

An acknowledgement atomically advances the cursor and clears the outbox before
the source is asked to commit its provider checkpoint. The client reports
acknowledgement success only after receiving the service's
`AcknowledgementCommitted` response. Apply and ACK requests remain recorded in
the transport object until their matching confirmation arrives, so cancellation
resumes the exact operation without exposing a deferred delivery. Reconnect may
retransmit an identical request; service transitions and source cleanup are
therefore idempotent. The contract does not promise one physical wire send.
The ACK confirmation must echo the delivery's complete cursor exactly, not only
its chain and sequence. A changed provider checkpoint, head, next block, or
revision is a protocol violation.
After an ACK has committed, repeating that exact eight-byte token is an
idempotent success at the subscriber boundary. Any older, future, or otherwise
different token remains an error.

Data records include both delivery audience and `DeliveryScope`. Audience
selects handlers; scope independently selects canonical authority:

- `CANONICAL` is ordinary authoritative live delivery;
- `CANONICAL_PROGRESS` is authoritative historical/recovery progress;
- `OWNER_CATCHUP` is routed replay that must not advance or rewind global
  canonical state.

`canonical_audience` broadcasts
through normal runtime routing; otherwise `owner_ids` restrict routing to the
exact handlers whose portable interests matched the record. This preserves
provenance when several owners share an upstream superset query.
Session and owner identifiers are exact UTF-8 strings: byte-identical
duplicates are rejected, while visually similar or canonically equivalent but
differently encoded strings remain distinct. Operators should use stable ASCII
identifiers where humans inspect or authorize those identities. Core
`HandlerId` values are non-empty by invariant. On the wire an empty
`owner_id` is reserved exclusively for the one `canonical=true` base-interest
entry; noncanonical desired-state owners and record-level owner audiences reject
empty identifiers. This keeps canonical broadcast distinct from every handler
namespace even when a peer is not implemented with the core type.
Records with neither canonical audience nor owner audience are omitted. If a
provider page advances only the source scan and therefore leaves no deliverable
records, the adapter emits a blockless barrier with the deterministic identifier
`source-progress:<revision>:<next_block>` instead of an empty `Data` payload.
Exactly these blockless scan-progress barriers and exact blockless
`desired-state:<revision>` activation barriers carry `checkpoint_neutral=true`.
The remote subscriber acknowledges them internally: they advance durable
transport authority but never become a runtime/cache input or runtime
checkpoint position. Every other payload must carry `checkpoint_neutral=false`.
Backfill activation boundaries and retained canonical history live inside the
opaque provider checkpoint, so owner scoping and shallow-reorg recovery survive
service restart mid-page.

`BlockHeaderEvent` always carries complete, hash-verified consensus-header RLP.
Compact source envelope rows use `BlockProgressEvent` instead and become
`ChainControl::CanonicalProgress`; consumers never synthesize EVM header fields
from compact progress. Applying canonical progress exact-hash-pins lazy reads
and installs `NUMBER` plus the provider-proven timestamp at the committed head.
It cannot supply `BASEFEE`, `COINBASE`, `PREVRANDAO`, or `GASLIMIT`, so those
header-only fields are cleared. A caught-up event cursor therefore proves
event-state coverage, not full-header readiness. Consumers requiring a complete
EVM environment must refresh a verified full header or live source before
simulation. Provider-internal rows used only for cursor or rollback validation
are not wire records.
At one height, an optional full header sorts first, logs sort by transaction and
log index, and one final compact progress record sorts last. A header and final
progress certificate may therefore name the same height, but their `BlockRef`
values must match exactly; duplicate headers or duplicate progress records do
not become valid merely because their bytes agree.

The portable cursor is source-authored but not source-trusted. It carries two
independent positions: `next_block` is the provider's exclusive scan position,
while `canonical_head` is runtime-global canonical coverage. Ordinarily the
scan follows the head successor. During a bounded or dynamic owner backfill it
may be behind that successor, but only an exact new-revision activation barrier,
an owner-catchup-only data page, or the exact deterministic scan-progress
barrier may advance that scan while preserving the acknowledged global head.
An explicit `Reorg` remains the only way to rewind global coverage.

Before an outbox commit, the service validates identity and successor sequence,
requires the cursor head to agree exactly with the canonical data or control
payload, rejects head drops and same-height identity changes, and requires a
reorg cursor to point at its declared common ancestor. When a rewound scan
crosses the preserved coverage boundary, the observed block and successor
parent must agree with that preserved `BlockRef`; a conflict fails closed rather
than silently replacing global authority. The remote transport repeats the same
checks against its durable acknowledged cursor so an independently implemented
or compromised service cannot bypass them.

The subscriber retains owner backfill intent across activation, reconnect, and
restart. It fences further desired-state mutations until an acknowledged cursor
for that revision proves the scan reached the preserved head's successor (or a
new canonical head advanced beyond it), then clears the completed backfill from
the next full-state replacement. Merely acknowledging activation, owner data,
safe/finalized controls, or a non-durable delivery is not completion proof.

Desired-state entries mark canonical interests explicitly. Handler identifiers
remain opaque strings, including the valid literal `$base`; no owner name is
reserved as a wire sentinel.

## Source contract

`evm-fork-cache-event-service::EventSource` exposes only protocol types:

1. advertise capabilities and topology;
2. prepare desired state and optionally return its activation cursor, then
   activate or abort the prepared candidate;
3. produce the next normalized delivery or wait for a wakeup;
4. acknowledge a delivery after the service cursor is durable.

The production and wakeup methods both receive `DeliveryRequest`, whose private
fields are exposed through accessors so the request can evolve without making
source implementations destructure a version-sensitive shape. The request is
built exclusively from committed service state: active desired state,
acknowledged cursor, and the optional replacement anchor installed by a reorg
ACK. When the anchor is present, returning a later quiet-range barrier without
the anchor is invalid. A source may instead return `None` until the exact anchor
can be certified, or a typed `Unsupported` error when that guarantee is outside
its capabilities. The service revalidates source output before durable outbox
admission; this is a safety boundary, not merely an adapter hint.

Provider-native planning, paging, push streams, deduplication, and rollback
metadata belong behind that boundary. HyperSync implements it through a managed
per-session source engine; future Firehose, Reth, Kafka, or webhook adapters do
not need to emulate HyperSync queries.

Every source operation except the intentionally idle update wait has a
configurable deadline. Source errors carry a stable class (invalid,
unsupported, exhausted, unavailable, or internal); public responses use
sanitized messages and retry disposition rather than provider error strings.
`wait_for_update` receives the same request so a push-capable source can wait for
the constrained height rather than repeatedly waking too early. Polling still
retries independently. If a provider permanently loses the promised anchor,
the session stalls safely and requires operator resynchronization; liveness is
never recovered by silently skipping durable replacement authority.

## Resource and storage invariants

The service enforces cheap identifier/owner quotas before deep structural
validation or attacker-sized preallocation, then checks per-owner and aggregate
interest/filter counts and encoded desired
state bytes, active sessions, and encoded delivery bytes. The delivery limit is
checked on the complete `ServerMessage` before source output or a prepared
activation barrier can become durable. A durable
outbox item admitted under an earlier, larger configured limit remains
replayable up to the immutable hard transport limit. It is not cleared and
regenerated: sequence-only tokens cannot safely identify changed content. An
item above the hard transport limit remains durable and fails closed for
operator recovery.

SQLite rows are decoded and cross-checked against their composite row key,
revision, cursor, successor sequence, payload, deterministic token, lifecycle
variant, block/hash widths, log shape, and header encoding presence. A decodable
protobuf with contradictory identity is corruption, not valid state.
Chain identifiers and activation sequences retain the protocol's full `uint64`
domain through reversible bit-pattern encodings in their signed SQLite
columns. Delivery-sequence arithmetic is checked in `u64` and fails closed when
no successor exists. Revisions remain restricted to SQLite's signed 64-bit
domain because they participate directly in durable compare-and-swap authority;
none of these values are silently wrapped.

## Evolution rules

Protocol v1 follows protobuf's append-only compatibility rules: never reuse or
change the meaning or type of a shipped field number or enum value; add optional
fields with new numbers; reserve removed numbers and names; and treat unknown
enum values or lifecycle variants as fail-closed at authority boundaries.
`Cursor` tag 5 and `ServerMessage` tag 5 are explicitly reserved;
`Hello.pending_delivery_resume` uses the append-only tag 5. Capabilities are
negotiated per chain and are not permission to send an unimplemented payload
shape. A semantic change that cannot be represented by optional fields requires
a new protocol version and package.
