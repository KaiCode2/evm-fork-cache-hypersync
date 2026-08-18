# Operating the event service

## Deployment invariants

- Run one event-service process per SQLite database. Session leases are shared
  across tonic service clones in that process, not across hosts.
- Put the database on durable local storage. SQLite runs in WAL mode with
  `synchronous=FULL`, a five-second busy timeout, and versioned startup
  migrations.
- Use a stable, unique session id per runtime and chain. A second live
connection for the same pair receives `SESSION_IN_USE`; after the owning
stream closes, the lease and its resident source engine, prepared candidates,
and height-stream reader are released. A failed or timed-out source release
retains the process-local lease until restart rather than admitting an
overlapping source generation.
- `max_active_sessions` applies only after a valid `Hello` acquires a durable
  lease; it is not a TCP connection or HTTP/2 stream cap. Put an explicit
  concurrent-connection/per-client limit at the ingress or tonic deployment
  layer. `EventService::with_client_hello_timeout` bounds each unnegotiated
  stream (10 seconds by default), so silent clients cannot occupy a stream
  indefinitely, but the surrounding listener still owns admission control.
- Delivery is demand-driven. An idle remote or the historical half of a live
  hybrid does not fetch a new page until its next delivery is polled, preserving
  the one-item outbox for lifecycle changes.
- Durable session creation is capped by
  `EVM_FORK_CACHE_EVENT_MAX_PERSISTED_SESSIONS` (65,536 by default), so churned
  identities cannot grow SQLite without bound. Reuse stable authenticated
  session ids and prune retired rows during an offline administrative window;
  existing identities remain usable when the creation cap is full.
- The SQLite authority stores chain ids, revisions, and activation sequences in
  signed 64-bit integer columns. Chain ids and activation sequences use a
  reversible bit-pattern mapping so their complete public `u64` domains remain
  available. Revisions are bounded by the signed storage domain, while checked
  delivery-sequence arithmetic fails closed at `u64::MAX` rather than wrapping
  to zero.
- Keep the configured reorg depth larger than the chain's operational rollback
  horizon. Recovery fails closed if the common ancestor is outside the durable
  canonical suffix. Inside the suffix, rollback discovery uses logarithmic
  exact-header probes plus one replacement-page query, so request count grows
  sublinearly with the retention window.
  The consuming runtime's `ReactiveConfig::journal_depth` must be at least the
  service's `EVM_FORK_CACHE_REORG_DEPTH`; otherwise the service can correctly
  emit a reorg that the runtime can no longer prove against its retained
  journal. Both shipped defaults are 64. Raise the service retention and the
  runtime journal together when a longer rollback horizon is required.
  Protocol V1 does not negotiate these numeric reorg depths; compatibility is
  a deployment invariant that operators must check whenever either side's
  configuration changes.
- Treat a persisted reorg replacement promise as recovery authority, not
  advisory metadata. After the reorg ACK, the service retains the advertised
  `new_tip` through restart and unrelated activation-barrier ACKs, rejects a
  conflicting source transition, and clears it only after a replacement ACK.
  That replacement must explicitly contain the exact promised anchor; if its
  terminal head is newer, the same data delivery must prove every continuous
  descendant from the anchor to that head. If startup reports that this durable
  state is inconsistent, stop and restore the SQLite database from a known-good
  backup rather than deleting the promise or advancing the cursor manually.
- Expect one compact predecessor query when a fresh source engine first receives
  a rollback-guarded page above genesis. This establishes a complete canonical
  anchor before any delivery is persisted; an absent, malformed, or
  hash-inconsistent anchor fails closed instead of fabricating ancestor fields.
- Treat an archive height of zero as unavailable, not as an implicit genesis
  cursor. Registration fails until the source reports a usable exclusive
  archive height.
- The default HyperSync factory is a log plus compact-progress source. Do not
  advertise full headers unless the chain's complete consensus schema has been
  independently verified; HyperSync client 1.4 omits `requests_hash` and cannot
  reconstruct post-Prague Ethereum headers. Compact progress exact-hash-pins
  lazy reads and installs `NUMBER` plus the provider-proven timestamp, but it
  clears unavailable `BASEFEE`, `COINBASE`, `PREVRANDAO`, and `GASLIMIT`.
  Caught-up event state is not full-header readiness: refresh a verified full
  header or live source before simulation when the complete EVM environment is
  required.

## Security

For direct TLS termination, set both:

```text
EVM_FORK_CACHE_EVENT_TLS_CERT=/run/secrets/tls.crt
EVM_FORK_CACHE_EVENT_TLS_KEY=/run/secrets/tls.key
```

For a direct non-loopback listener, set
`EVM_FORK_CACHE_EVENT_BEARER_TOKEN` in addition to TLS, then connect with
`RemoteSubscriber::connect_with_bearer_token`. The authorization value travels
in HTTP metadata, not the event protocol, and is retained by the client for
reconnect. A service mesh can provide mTLS and implement `SessionAuthorizer`
from verified gateway metadata instead.
The bundled remote client uses the platform's native root store for direct
server-authenticated TLS. Direct custom-CA injection and client certificates
are not convenience-constructor options; use custom tonic transport
composition or terminate those policies in an authenticated proxy/service
mesh.
The included binary refuses a non-loopback listener unless direct TLS and bearer
authentication are both configured. Server-only TLS does not authenticate a
client, while a bearer token over plaintext exposes the credential. When a
trusted service mesh supplies missing encryption and client authentication
before forwarding to the process, set
`EVM_FORK_CACHE_EVENT_TRUSTED_MESH=true` explicitly; startup then emits a warning
that the process is relying on that boundary. This escape hatch does not itself
provide encryption or authentication.
The included binary's single shared bearer token is an admission check for one
trust domain, not tenant isolation. If mutually untrusted clients share a
service, compose `EventService` with a custom authorizer that also implements
`authorize_session` and binds the authenticated principal to the requested
session and chain. Validating a shared token alone does not prevent one valid
client from leasing another client's offline durable session.
Present-but-empty and whitespace-only API or bearer-token environment values are
configuration errors; they never silently enable an empty credential.

Never commit `.env`, API tokens, bearer tokens, certificates, private keys, or
SQLite databases. The workspace ignore rules cover `.env`, SQLite files, and
build output.

## Resource policy

`EventServiceLimits` bounds active sessions, owners, interests per owner,
aggregate interests/filter values, log addresses, topic values, identifier
bytes, encoded desired-state/delivery bytes, and bounded-backfill length.
Defaults are advertised
in `HelloAccepted.service_limits`; requests over a limit fail before provider
preparation or durable mutation. Tonic decoding and encoding are also capped by
`MAX_MESSAGE_SIZE_BYTES`. HyperSync starts with at most 1,000 blocks, 5,000
logs, and 4,096 distinct canonicalized filters per query. Before persistence it
measures the encoded delivery; an oversized page is requeried with progressively
smaller block and log limits. If one block/log unit still cannot fit, the source
fails with resource exhaustion without advancing or persisting its cursor.
The HyperSync row targets are not hard response guarantees. Independently, the
adapter rejects decoded responses above 2,000 block rows, 10,000 log rows, or
16 MiB of row storage plus dynamically owned field bytes by default. These
checks run before structural validation, sorting, cloning, canonical tracking,
or normalization. Configure them with
`EVM_FORK_CACHE_SOURCE_MAX_RESPONSE_BLOCKS`,
`EVM_FORK_CACHE_SOURCE_MAX_RESPONSE_LOGS`, and
`EVM_FORK_CACHE_SOURCE_MAX_RESPONSE_DYNAMIC_BYTES`; every value must be nonzero.
This deliberate separation accepts modest provider soft-target overshoot while
bounding adapter work even when an upstream response is malformed. The checks
run after `hypersync-client` and its HTTP stack have decoded the response; they
do not cap provider/network buffers or allocations internal to that upstream
client. Enforce those separately at the client, proxy, and process boundary.
`EVM_FORK_CACHE_EVENT_MAX_DELIVERY_BYTES` (default `33554432`) lowers the
service's advertised/enforced limit and the source budget together; the source
reserves 64 KiB for the outer protocol envelope.
The configured limit applies when admitting new outbox items. A delivery
already admitted under an older, larger configuration is replayed unchanged up
to the hard tonic limit so a sequence token can never name regenerated content.

`SessionStore` deliberately uses one synchronous `rusqlite::Connection` behind
one async mutex. This gives simple, auditable transaction boundaries, but every
session's load/CAS/outbox/ACK operation is globally serialized for that service
instance, and an SQLite busy wait or `synchronous=FULL` fsync blocks the Tokio
worker executing it. The normalization microbenchmarks do not measure this
path, so they are not a service-throughput claim. Run the service on a
multi-thread Tokio runtime, benchmark the database and storage medium with the
expected concurrent-session/fsync workload, and scale by sharding sessions
across independent service processes and database files. Do not share one file
between processes as a substitute for sharding. A future storage actor or
`spawn_blocking` design can remove worker blocking, but is not part of v1.

`EVM_FORK_CACHE_SOURCE_REQUEST_TIMEOUT_MS` (default `45000`) bounds each height
lookup and complete adaptive page attempt, including the initial canonical
anchor and upstream retries. Set it above normal provider tail latency but below
the deployment's liveness budget.
Provider update waits remain cancelable and intentionally unbounded so the poll
fallback can race them safely. SSE heights are only wakeup hints and may move
downward; every delivery attempt obtains a fresh REST height before selecting a
query target, which corrects stale or erroneously high streamed values.
`EVM_FORK_CACHE_SOURCE_MAX_RESIDENT_SESSIONS` (default `4096`) bounds distinct
session/chain engines; excess sessions receive resource exhaustion until a
leased stream exits and releases its source state.

The remote client's HTTP/2 keepalive interval, keepalive acknowledgement
timeout, and idle-PING behavior are part of `GrpcTransportConfig` (defaults:
30 seconds, 10 seconds, and enabled). Keep both durations nonzero and align
ingress idle timeouts so intermediaries do not silently discard a healthy
long-lived stream. `control_response_timeout` is an absolute deadline for the
entire apply or ACK operation; heartbeats and reconnect attempts do not reset
it. The client handshake and control defaults are both 60 seconds so they
exceed the binary's 45-second source-operation default. Deployments that raise
the server source timeout must also keep both client deadlines strictly above
it, including source baseline proof and desired-state preparation time.
Reconnect backoff always clamps the initial delay to the configured maximum.
Both reconnect delay bounds must be nonzero.

The included binary handles both Ctrl-C and Unix SIGTERM through the same tonic
graceful-shutdown path. The signal also closes every service session loop before
tonic waits for active streams, and each source release is bounded by the
configured source-operation timeout. Non-Unix builds retain portable Ctrl-C
handling.

## Health and metrics

`EventService::metrics()` exposes monotonic counters and an active-session
gauge through `EventServiceMetrics::snapshot()`:

- sessions accepted, authentication failures, and lease conflicts;
- desired states committed;
- deliveries persisted and replayed;
- acknowledgements durably committed;
- source errors.

`GrpcEventTransport::reconnect_count()` exposes client-side reconnects, and
`HybridSubscriber::{phase, poison_reason, buffered_live_batches,
buffered_live_records, buffered_live_bytes}` exposes coordinator health. The
runnable binary logs a final counter summary. Production deployments can export
the same snapshot through their existing metrics stack; the crate intentionally
does not force an HTTP exporter.

Hybrid checkpoints include the bounded recent-input audience journal and are
rewritten with each checkpointed delivery. Their size and encoding/write cost
therefore scale with `recent_input_capacity`, `max_recent_owner_entries`,
retained owner-id lengths, canonical history, and both children's opaque token
and checkpoint sizes. Measure this path with representative filters, fanout,
and real child cursors before increasing the defaults. Checkpoint format V5 is
independent of gRPC protocol V1 and uses
canonical RLP inside an 18-byte magic/version/length/CRC32 envelope. The RLP
payload has a hard 16 MiB limit; decoding additionally enforces collection,
identifier, node, and 64 MiB accounted-heap budgets. Unpublished V4 bincode
checkpoints are rejected and require authoritative resynchronization. CRC32
detects accidental corruption only; it is not authentication. Protect the
durable checkpoint path from attacker writes. Live buffering separately
enforces batch, record, and accounted byte limits; accounted bytes include log
data/topics, audience ids, controls, delivery tokens, and nested source
checkpoints.

The 64 MiB decode budget is a conservative accounting model for heap owned by
decoded checkpoint collections and byte/string payloads. It is not a process
RSS limit and does not include allocator bookkeeping, stack frames, the shared
encoded input, temporary surrounding runtime state, or memory owned by either
child. Enforce a process/container memory limit independently when checkpoints
cross a less-trusted operational boundary.

The record and accounted-byte limits are also enforced against each individual
historical or live child batch before Hybrid builds record witnesses or replay
digest buffers. A batch above either ingress ceiling is rejected and remains
unacknowledged. The raw delivery token and nested child checkpoint are first
checked against `max_source_delivery_token_bytes` and
`max_source_checkpoint_bytes`; these independent per-child ceilings default to
64 KiB and 1 MiB. A derived control-count ceiling is checked before walking
every control, and byte accounting returns on the first overflow. Projected
routing work is bounded across the whole batch before payload witnesses or
transcripts are built: `All` charges installed owner count, `Owners` charges
explicit ids, and `AllExcept` charges installed owners plus explicit exclusions
with checked addition. Unknown owner ids fail closed even if later local
filtering would drop the record. Hybrid does not advertise block-header
registration. A manually supplied header is accepted only as a defensive path
when the child supplies a `SubscriberPayloadCommitment`; the complete
handler-visible JSON body must also fit the hard 256 KiB canonicalization
ceiling because Hybrid's outer delivery is always tokened and independently
replay-witnessed.

Treat child envelopes and tokens as a protocol boundary. `Some(batch)` must
contain a record, a control, or a delivery token; use a pending poll or
`Ok(None)` for idleness. A forwarded token must carry canonical coverage so its
source position can be restored. Within one durable child cursor namespace,
token bytes are immutable and can never be recycled after an intervening token.
Hybrid does not retain an unbounded raw-token history, so each child must prevent
`A -> B -> A` reuse itself.

Treat the Hybrid's six retention and cursor dimensions as different safety
proofs:

- `ReactiveConfig::journal_depth` retains runtime canonical state needed to
  execute a reorg. It must be at least the historical service's configured
  reorg depth.
- `HybridConfig::canonical_history_capacity` retains block identities used to
  validate cross-source overlap and recovery ancestors. It must cover the same
  maximum reorg horizon; the default is 512 blocks.
- `HybridConfig::recent_input_capacity` retains complete payload witnesses and
  audience generations. It counts events, not blocks, and must cover the event
  volume across the longest plausible live/history restart lag. The default is
  32,768 witnesses.
- `HybridConfig::max_recent_owner_entries` bounds the total
  `(handler, generation)` associations across those witnesses. Its default and
  hard configurable ceiling are 65,536. Hybrid deterministically evicts the
  oldest complete witnesses until this budget and the V5 decode/payload limits
  fit, while preserving every identity in the current delivery. Dense fanout
  can therefore shorten the effective `recent_input_capacity` window; a
  protected current-delivery suffix that cannot fit fails before runtime
  output.
- `HybridConfig::max_source_delivery_token_bytes` bounds each opaque forwarded
  child token on ingress and restore. The default is 64 KiB. Active lifecycle
  preflight reserves this amount for both source positions and a second copy
  for the committing source's `last_committed_token`.
- `HybridConfig::max_source_checkpoint_bytes` bounds each nested child
  checkpoint on ingress and restore. The default is 1 MiB. Active lifecycle
  preflight reserves this amount for both sources. Although either configured
  hard ceiling is 16 MiB, their combination must fit the one shared 16-MiB V5
  payload and can therefore fail activation at much lower individual values.

Exceeding a live buffer bound poisons the coordinator. Aging out a canonical
ancestor or payload witness makes a later overlap unverifiable and requires a
full resynchronization. Neither condition permits block-number-only
deduplication. Load-test the 16-MiB encoded checkpoint bound and durable-store
latency with the deployment's high-volume blocks, owner fanout, and owner-id
lengths. Owner ids are also capped at 4,096 bytes, and prospective topology
changes are durability-preflighted before either child is mutated.

Canonical validation uses the core crate's structured diagnostics. Only a
rollback whose required ancestor is outside retained history enters historical
recovery. A contradiction against a known height/parent is intrinsically
invalid and poisons the coordinator; changing provider error text cannot change
that decision. Retained history can be sparse: known identities and adjacent
parents are checked exactly, but a gap is not a locally reconstructed ancestry
proof. The sequenced durable historical source is the authority inside that
gap, so deploy only a source whose canonical/reorg checkpoint contract is
trusted. Historical coverage certification never decreases within one
lifecycle/branch when an older page is later acknowledged; equal-height
metadata must be compatible, while a reorg/reset crossing the proof clears it.

For persisted Hybrid startup, use this exact order:

1. Load the durable checkpoint without mutating the cache, validate its stored
   block hash against an authoritative RPC source, construct the durable
   historical child and a fresh live child, then construct the Hybrid with
   matching authoritative chain ids and rebuild the runtime's exact handler
   topology.
2. On that same fresh engine, call
   `preview_durable_resume_position(&metadata)`. This validates the runtime
   checkpoint and returns the exact subscriber position core restore will use;
   do not mutate the engine between preview and restore.
3. For non-empty base state,
   call `prepare_restore_base_lifecycle(&position, base_interests).await`. For
   every owner topology, including installed owners with empty filter lists,
   call
   `prepare_restore_lifecycle(&position, &[], owners).await`. This
   exact-replaces the selected topology on an ephemeral live child first and
   validates portable lifecycle fingerprints without mutating historical
   desired state.
4. Prefer `ReactiveEngine::restore_durable_checkpoint` to atomically install the
   cache, runtime, and subscriber state from the loaded checkpoint. If the cache
   was restored separately, pass the same metadata to
   `resume_from_durable_checkpoint`. Both paths must derive the position used in
   steps 2 and 3. If either child restore is partial, retry only that exact
   position; polling, ACK, and reconfiguration remain fenced.
5. Start polling. Hybrid first retries child ACKs proven committed by the outer
   checkpoint, without repolling either source or emitting an empty replay.
   Historical recovery then verifies every suppressed overlap payload while the
   already-active live child establishes the new cutover fence.

A durable live child is not changed during preparation and restores its own
cursor. Only a completely empty base/no-owner lifecycle may skip preparation;
an all-empty owner topology still carries durable owner identity. Any mismatched
interest topology or position fails before child mutation or data delivery.
Preparation merges the runtime history into the exact install candidate and
applies only the live-cursor cleanup that restore will perform. Active topology
must then prove that its fully saturated configured durable state fits before
registration; restore repeats the same proof before staging either child. The
simulation fills the coordinator and both source histories to
`canonical_history_capacity` with distinct, linked, fully populated block
references ending at `u64::MAX - 1`, installs maximum-width
safe/finalized/certification state, reserves both configured cursor pairs with
worst-case RLP payload bytes, and puts the synthetic sequence at its maximum
encoded width.

From that validated state, four independent simulations use the real commit
function for Historical/Live and forwarded/synthetic token forms. Each advances
all three histories to `u64::MAX` and includes exact audience fanout, the
protected payload witness, finality/historical certification, cursor
replacement, a duplicated maximum forwarded token, or the eight-byte synthetic
last token. This fieldwise upper bound covers monotonic progress,
terminal-height reorg replacement, and divergent retained source histories
without guessing a future ancestor. It guarantees one largest supported record
from a fully saturated configured state; it does not guarantee arbitrary
multi-record batches, which still receive their exact fit check before output.
Consequently, unusually large history/cursor settings may reject active
topology before child mutation even when the current checkpoint is sparse.
Effective-empty topology reserves no source-delivery space, but its next base
or owner activation reruns the complete proof against the preserved lifecycle
state before either source is mutated.

Operate Hybrid in exactly one lifecycle mode: base/unowned interests or
handler-owned interests. Do not combine them or directly replace one non-empty
mode with the other. Those requests, mixed restore arguments, and mixed
checkpoint state are rejected before child mutation because there is no atomic
generic rollback for a base-plus-owner topology. Clearing either mode to an
effective-empty topology enters `Live` without polling either source, but first
emits one coordinator-local synthetic lifecycle barrier. Persist and ACK that
barrier before another lifecycle change; it replays byte-identically until ACK.
Its checkpoint carries the new generation, preserves child cursor/history,
clears acknowledged raw child tokens, and does not invent source coverage.
Subsequent polls return `Ok(None)`. After canonical
coverage, base-to-owner activation must use global backfill; owner-to-base
activation requires a fresh coordinator at an authoritative checkpoint because
the generic API has no symmetric base global-backfill primitive.
`ReactiveEngine` normally remains handler-owned for its lifetime.

Ordinary lifecycle compensation is not a historical certification primitive.
If a live-first base, owner, or bulk mutation changed a previously active
filter after acknowledged coverage, a successful rollback still leaves an
unprovable event window. Hybrid enters `Poisoned`; reconstruct it from an
authoritative checkpoint. New and previously empty owners remain retryable
because no active subscription was interrupted. Only explicit topology-wide
global backfill may enter `Recovering` after compensation, and it returns to
`Live` only after the historical source certifies the gap.

### V5 wire-fixture maintenance

The permanent exact-byte fixtures live at
`crates/remote/testdata/hybrid_checkpoint_v5_forwarded_historical.hex` and
`crates/remote/testdata/hybrid_checkpoint_v5_synthetic_live.hex`. These two
fully populated fixtures cover the forwarded-historical and synthetic-live
shapes, including both source positions and representative durable input,
witness, audience, and lifecycle variants. Separate codec round-trip,
corruption, limit, and stable-digest tests cover absent optional fields and
empty collection branches. The fixture unit test decodes each vector into an
explicitly constructed state and requires byte-for-byte re-encoding.

Do not update those vectors as a routine snapshot refresh. First decide whether
the wire change is compatible; after publication, any field-order, tag, or
meaning change that alters bytes requires a new checkpoint version (and an
explicit migration or fail-closed rejection), not silently rewritten V5 bytes.
For an intentional pre-publication fixture change, inspect the complete test
diff, update the explicit expected state and matching hex together, then run
`fully_populated_v5_checkpoints_match_permanent_wire_fixtures`. Before packaging,
run `cargo package -p evm-fork-cache-remote --list` and confirm both `testdata/`
files are present, then run package verification from the generated archive.

Do not couple a child's internal transport/provider-scan cursor to the Hybrid's
runtime-visible source position. Lifecycle preparation and transport-only
`Barrier(None)` handling may advance the former without producing an outer
batch. The Hybrid checkpoint intentionally retains only emitted runtime commit
positions. Durable children must preserve the dual-cursor proof and accept or
reject the supplied runtime position explicitly on restore; they must not
silently substitute a newer internal scan position.

Lifecycle calls are cancellation-safe through reconciliation, not through an
assumption that a dropped future made no change. If any live or historical
mutation has an uncertain result, delivery and ACK remain fenced. Retry the
same class of lifecycle operation: the coordinator restores the previous state
on both children, then applies the requested mutation anew. A lifecycle change
is not crash-durable in the cache until a later Hybrid delivery checkpoint
contains its fingerprint. If the historical service committed a newer desired
state before that checkpoint, restoration of the older cache state must fail on
the service cursor/revision proof.

Treat owner catch-up as a narrow journal-attachment operation. If a handler is
added at canonical block `C`, only an exact retained block-`C` delivery may be
owner scoped. The same historical revision must route `C + 1` through its
activation head globally for the complete interest union; this covers chain
advance during registration and ensures a later reorg rolls back both existing
and newly added handler effects. Hybrid rejects ahead, hash-mismatched, or
aged-out owner-only records before delivery/ACK, and the runtime rejects an
anchor whose effect journal entry has expired. Recover by globally
resynchronizing or increasing the configured windows for a future run; never
relabel the page as canonical progress.

Use `ReactiveEngine::register_handler` for this coordinated non-empty-head
registration. Hybrid subscribes the live child first and forwards the retained
`C` boundary only to the historical child. If either commit is uncertain,
delivery and ACK remain fenced until both children restore the prior owner
topology; do not retry through the ordinary deep-backfill method.

Whole-runtime startup is a different operation. A fresh aligned runtime uses
exact no-history owner replacement. A restored runtime paired with a new or lost
subscriber service uses `sync_handler_interests_with_backfill`, which removes
stale owners/base interests and creates one global post-baseline backfill in the
same desired-state revision. Per-owner catch-up is not a safe substitute.

## Recovery playbook

1. On a transport disconnect, let the remote subscriber reconnect. An
   unacknowledged outbox item replays exactly; a lost ACK confirmation retries
   idempotently. If an apply or ACK future was cancelled, retry that operation;
   delivery remains fail-closed until the exact pending control operation is
   reconciled.
   For a persisted runtime, construct the client with
   `RemoteSubscriber::connect_from_position` (or its bearer variant) so `Hello`
   carries the restored eight-byte delivery sequence and its token/checkpoint
   proof. Exact equality is normal. If runtime state N was saved before its ACK,
   service state N-1 plus the exact pending outbox N is also accepted: N replays
   and is ACKed without re-ingestion. Every other ahead/behind or proof mismatch
   fails closed instead of silently attaching stale cache state.
2. On service restart, reopen the same database. Desired state, acknowledged
   cursor, pending delivery, activation boundary, and canonical suffix restore
   before new source data is fetched.
   A head-only registration must restore at its source-prepared archive cursor;
   a block-zero cursor indicates an incompatible or corrupt checkpoint and
   should be investigated before consumption resumes.
   If a reorg control was acknowledged just before the crash, its durable cursor
   points to `common_ancestor + 1` and its checkpoint contains only the shared
   canonical prefix. Restart therefore refetches the replacement branch instead
   of skipping its first block. The service passes the promised `new_tip` to
   both source polling paths. HyperSync caps the first replacement fetch at
   `new_tip + 1`, even if the archive head advanced while the service was down,
   so an event-free replacement is certified by an exact blockful barrier
   before ordinary catch-up resumes. A custom source that cannot honor this
   constraint must return `Unsupported` or remain caught up with `None`; output
   that skips the anchor is rejected before outbox persistence. A permanently
   unavailable anchor is a fail-closed resynchronization condition, not a
   reason to advance the cursor manually.
3. On `SESSION_IN_USE`, terminate the stale runtime or wait for its stream to
   close. Do not change session ids to bypass the lease unless a second,
   independent consumer is intentional.
   If source cleanup exceeded its configured deadline, the service retains the
   lease intentionally and the active-session gauge remains nonzero. Restart
   that service instance after investigating the source cleanup; changing the
   session id would bypass a safety fence and can race partially released state.
4. On a poisoned hybrid coordinator, reconstruct it after repairing the failed
   source. Poison indicates an unsafe condition such as chain/routing metadata
   mismatch, bounded-buffer exhaustion, conflicting payload reuse, or overlap
   outside a retained proof window. It also includes ordinary lifecycle
   compensation after a previously active live filter changed beyond an
   acknowledged coverage anchor: restored filters do not prove that mutation
   interval was event-free. Further delivery is deliberately disabled.
   A merely uncertain lifecycle operation is fenced rather than immediately
   poisoned: retry that lifecycle class so both sources reconcile. If
   reconciliation succeeds and Hybrid reports this uncertifiable gap, rebuild
   from an authoritative checkpoint. The explicit topology-wide global-backfill
   path instead enters `Recovering` and may resume only after history certifies
   the gap. If reconciliation itself continues to fail, repair the child service
   before retrying; do not bypass the fence with a new owner or a manual cursor
   edit.
5. On history exhaustion during reorg recovery, stop and resynchronize from an
   authoritative cache checkpoint or increase retained depth for the next run.
   The same response applies when Hybrid reports overlap outside its retained
   payload-witness or canonical-history window. Do not ACK the historical page;
   the coordinator intentionally fails before advancing that source position.
   The same resynchronization rule applies to an `owner catch-up fork before
   activation block` error. Pre-activation pages deliberately carry no global
   canonical head, and protocol v1 has no owner-scoped rollback control. Create
   a new desired-state revision whose backfill starts from an authoritative
   position; do not reinterpret the error as a global reorg or manually advance
   the persisted scan cursor.
6. If a previously persisted delivery exceeds a newly lowered configured byte
   limit, the service still replays that exact durable item as long as it fits
   the hard transport limit. Raise the configured limit before admitting later
   pages if necessary. A corrupt legacy item above the hard limit remains in the
   outbox and fails closed; restore a valid database snapshot or use a
   purpose-built migration after verifying downstream checkpoint state. Never
   delete it or advance the cursor manually.

Back up the SQLite database only with SQLite-aware snapshot tooling or while the
service is stopped. Copying only the main file while WAL writes are active can
omit committed state.
