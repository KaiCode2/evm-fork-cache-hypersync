# evm-fork-cache-hypersync

Envio HyperSync source adapter and runnable durable event service for
[`evm-fork-cache`](https://crates.io/crates/evm-fork-cache).

The adapter compiles provider-portable owner interests into bounded HyperSync
queries, emits compact canonical-progress controls and matched logs, persists
opaque rollback state, recovers shallow reorgs explicitly, and wakes on
HyperSync height streaming with REST polling as a fallback. Provider-neutral
session, replay, and gRPC behavior lives in `evm-fork-cache-event-service`.
When a fresh engine receives its first rollback-guarded page, it also fetches
the immediately preceding compact block as a canonical anchor. Reorg controls
therefore contain only provider-proven block hashes, parents, and timestamps;
the adapter never persists a synthetic ancestor. Deep retained rollbacks locate
the exact common ancestor with logarithmic single-header probes before fetching
the replacement suffix, rather than issuing one replacement query per height.

HyperSync's `max_num_blocks` and `max_num_logs` values are soft batching targets.
The adapter independently enforces configurable hard decoded-response ceilings
for block rows, log rows, and dynamically owned bytes before structural
validation, sorting, cloning, or normalization. Small provider target overshoot
is accepted; crossing a hard local ceiling fails with resource exhaustion.
These limits apply after `hypersync-client` has decoded its response; they do
not bound upstream HTTP/client-internal allocations, which require separate
process or proxy controls.
SSE archive heights are likewise treated only as fallible wakeup hints. REST
height reconciliation establishes every actual query target and recovers both
erroneously high hints and downward corrections.

Owner catch-up pages below the revision's activation block advance only the
opaque provider scan cursor and checkpoint; their normalized canonical head is
empty, so they cannot advance runtime-global coverage. Because protocol v1 has
no owner-scoped rollback control, a fork discovered while the previously
delivered branch is still wholly pre-activation fails closed instead of
emitting a global reorg or silently replaying a replacement owner branch. Start
a fresh desired-state revision from an authoritative position to recover.

Full block-header interests are fail-closed by default. HyperSync client 1.4's
block schema does not expose `requests_hash`, so it cannot reconstruct current
post-Prague Ethereum header RLP. Embedded deployments may opt a chain in only
after proving every consensus field is available and the reconstructed RLP
hashes to the provider's block hash; the included binary does not opt in any
chain. Compact progress still exact-hash-pins lazy reads and installs `NUMBER`
and the provider-proven timestamp at the committed canonical head, but it
cannot supply `BASEFEE`, `COINBASE`, `PREVRANDAO`, or `GASLIMIT`; those
header-only fields are cleared. A caught-up event cursor proves event-state
coverage, not full-header readiness. Refresh a verified full header or live
source before any simulation that requires the complete EVM environment.

Run the service with:

```text
ENVIO_API_TOKEN=... \
EVM_FORK_CACHE_EVENT_LISTEN=127.0.0.1:50051 \
EVM_FORK_CACHE_EVENT_DB=/var/lib/evm-fork-cache/events.sqlite \
evm-fork-cache-hypersync
```

Use bearer authentication and TLS together (or a trusted service mesh) across machine
boundaries. The included shared bearer token assumes one trust domain; use a
session-binding custom authorizer for mutually untrusted clients. The
bundled client trusts platform-native server roots; custom CAs or direct client
certificates require custom transport composition or a proxy/service mesh. The
binary refuses a non-loopback listener unless both direct TLS and bearer
authentication are configured, or
`EVM_FORK_CACHE_EVENT_TRUSTED_MESH=true` explicitly assigns missing encryption
or authentication to the mesh. It warns at startup when that escape hatch is
active. The
binary also caps durable SQLite identities at 65,536 by default; reuse stable
authenticated session ids and prune retired identities offline instead of
churning names. The
[operations guide](https://github.com/KaiCode2/evm-fork-cache-hypersync/blob/main/docs/operations.md)
documents all settings and recovery invariants. Review the
[security policy](https://github.com/KaiCode2/evm-fork-cache-hypersync/blob/main/SECURITY.md),
including the narrowly scoped upstream Cap'n Proto advisory, before deployment
and every release.

Licensed under MIT OR Apache-2.0.
