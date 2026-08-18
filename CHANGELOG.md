# Changelog

All notable changes to this workspace are documented here. The project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html) and uses one version
for all four packages during the initial provider-neutral protocol series.

## [Unreleased]

### Added

- An ignored live Hybrid restart release gate is designed to exercise a
  hash-pinned RPC cold start, durable HyperSync catch-up, ephemeral Alloy
  WebSocket cutover, a failed outer acknowledgement after checkpoint
  persistence, and complete service/SQLite/durable-store/cache/runtime/engine/
  WebSocket reconstruction. A successful run must prove the restored child
  acknowledgement clears the exact durable outbox before either source is
  first polled, emits no synthetic replay envelope, preserves exact checkpoint
  metadata and cache/runtime context, and resumes duplicate-free live canonical
  delivery. Current-harness live evidence remains a pre-publication gate.

### Fixed

- Hybrid restore capacity now uses the exact normalized install candidate,
  including bounded runtime history and ephemeral-live cursor cleanup, and is
  rechecked before child restore. Active base and owner topology now prove a
  conservative fully saturated durable state: coordinator and both source
  histories are filled to the configured capacity with maximum-width linked
  blocks, finality/certification and the synthetic counter are at maximum
  encoded width, and both configured opaque cursor pairs use worst-case RLP
  payload bytes. Four real commit simulations cover Historical/Live and
  forwarded/synthetic token forms, including exact fanout, the protected
  payload witness, cursor replacement, the duplicated maximum forwarded token,
  and the eight-byte synthetic last token. The fieldwise proof subsumes
  terminal-height reorg replacement and divergent source histories without a
  speculative ancestor selector. Per-child token/checkpoint budgets are
  enforced on ingress and durable restore. Effective-empty state remains
  restorable without source reserve and reruns the complete proof on later
  activation before child mutation. Unusually large history/cursor settings may
  fail this stronger admission contract even when current state is sparse.
- Ordinary compensated lifecycle mutations that interrupt a previously active
  post-coverage live filter now poison instead of falsely returning to `Live`.
  Brand-new and previously empty owners remain retryable, while the explicit
  topology-wide global-backfill contract retains its history-certified
  `Recovering` path.

## [0.1.0-alpha.1] - 2026-08-18

### Added

- Versioned provider-neutral desired-state, capability, delivery, cursor,
  checkpoint, reorg, finality, barrier, demand, and acknowledgement protocol.
- Provider-neutral tonic event service with source preparation, activation
  barriers, bounded resource policy, authorization, process-local session
  leases, metrics, and a durable SQLite cursor/outbox.
- Reconnecting `RemoteSubscriber` and acknowledgement-gated
  `HybridSubscriber` implementations for `evm-fork-cache` 0.4.
- HyperSync query planning, bounded paging, deterministic normalization,
  streamed-height wakeup, rollback recovery, and a runnable authenticated/TLS
  service binary.
- Independent hard decoded-response limits, REST-verified fallible SSE height
  hints, logarithmic rollback probing, same-hash metadata conflict detection,
  evolution-safe provider builders/errors, and Unix SIGTERM shutdown handling.
- Activation-scoped canonical cursors for owner backfill, plus fail-closed
  handling when a fork affects a wholly pre-activation owner branch that cannot
  be represented by protocol v1's global reorg control.
- Cancellation and restart fault tests, live Ethereum acceptance tests, and
  exact-identity HyperSync versus WebSocket comparison benchmarks.
- Alloy dependency bounds that preserve the declared Rust 1.90 MSRV under a
  fresh dependency resolution.
- Durable reorg replacement-tip promises across service restarts and lifecycle
  revisions, proof-gated sequence-zero authority discovery, provider cleanup
  fencing after failed compensation, and authoritative owner-audience checks
  for every remote transport.
- Evolution-safe `DeliveryRequest` constraints shared by source fetch and
  wakeup paths, exact quiet-range reorg certification after restart, and
  fail-closed validation for custom sources that ignore durable anchors.
- Hybrid checkpoint format V5: a bounded canonical-RLP schema with an explicit
  magic/version/length/CRC32 envelope, stable domain-separated replay and
  lifecycle transcripts, exact source/context/confirmation commitments, hard
  decode budgets, and explicit rejection of the unpublished V4 bincode format.
- Stateful Hybrid canonical/finality validation through core's provider-neutral
  sequence contract, including sparse-ancestor proofs, same-head metadata
  enrichment, generation-bound historical coverage, repeated removal-signal
  delivery, durable retention of sparse authenticated coverage heads, and
  fail-closed cross-source identity/finality conflicts.
- Crash-safe Hybrid child-ACK reconciliation before repoll, destructive-reset
  token/checkpoint namespace rotation, preflighted ACK-gated synthetic barriers
  for effective-empty lifecycle revisions, preserved durable child cursors, and
  exact base/owner topology recovery rules. Block-header capability advertising
  is disabled until a provider-neutral body-commitment contract exists; the
  defensive manual path is payload-committed and bounded to 256 KiB.
- Structured rollback classification between historical recovery and fatal
  canonical contradictions; monotonic generation-bound historical coverage;
  pre-mutation restore/token/history/topology validation; and restore support
  for installed all-empty owner topologies without source polling.
- Fail-closed rejection of semantically empty child envelopes and forwarded
  tokens without canonical coverage, plus pre-transcript control-count and
  checked projected-owner-work ingress limits (including `AllExcept` explicit
  exclusions and unknown-owner validation).
- Permanent fully populated V5 exact-wire fixtures for forwarded-historical and
  synthetic-live states, with decode-to-expected-state and byte-identical
  re-encoding coverage.

[Unreleased]: https://github.com/KaiCode2/evm-fork-cache-hypersync/compare/v0.1.0-alpha.1...HEAD
[0.1.0-alpha.1]: https://github.com/KaiCode2/evm-fork-cache-hypersync/releases/tag/v0.1.0-alpha.1
