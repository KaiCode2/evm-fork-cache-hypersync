# evm-fork-cache-event-service

Provider-neutral durable event sessions for
[`evm-fork-cache`](https://crates.io/crates/evm-fork-cache).

The crate owns authoritative desired-state revisions, source preparation and
activation, a SQLite-backed one-item outbox, exact reconnect replay, durable
acknowledgements, session leases, resource limits, authorization hooks, and the
bidirectional tonic service. Source implementations conform to `EventSource`
and exchange only protocol-owned types, so HyperSync, Firehose, Reth, Kafka, or
webhook sources can sit behind the same remote subscriber.

A proof-free sequence-zero `Hello` may inspect an existing session's authority,
but the server gates desired-state changes, delivery demand, acknowledgement,
and source polling until a new proof-bearing stream confirms the runtime
checkpoint. An acknowledged reorg also installs a durable replacement anchor:
the first later source delivery must explicitly contain its exact `new_tip`,
plus every continuous descendant when the batch ends at a newer head. The
anchor survives restart and unrelated activation acknowledgements and clears
only with the certified replacement acknowledgement.

Both `EventSource::next_delivery` and `EventSource::wait_for_update` receive the
same provider-neutral `DeliveryRequest`. It contains the committed desired
state, acknowledged cursor, and an optional durable reorg anchor. A custom
source must return `None` until it can honor that anchor, return a typed
`Unsupported` error if it cannot, or produce a delivery that explicitly
certifies it. The service independently validates every returned delivery
before writing the outbox, so an adapter that ignores the constraint cannot
advance or clear durable authority. The update wait is only a fallible liveness
hint; periodic polling remains authoritative and carries the identical request.

`EventSource` failures are typed and sanitized at the public boundary. Source
operations are deadline-bounded, capabilities are negotiated per chain, and an
idempotent, cancellation-safe `release_session` hook releases process-local
engines on every leased stream exit. If that cleanup fails or exceeds its
deadline, the service deliberately retains the lease until restart so a new
source generation cannot race partially released state.
Failed preparation compensation closes the stream and runs the same bounded
release path; the generation is never reused after an abort failure.
`SessionAuthorizer::authorize_session` must bind authenticated
metadata to the requested durable session/chain in multi-tenant deployments.
Encoded delivery size is checked before source output or a prepared activation
barrier can become durable; row identity, revision,
cursor, sequence, token, payload shape, and EVM field widths are revalidated on
every load. New streams also have a configurable pre-`Hello` deadline. Client
response backpressure and source control operations have independent
configurable deadlines. A delivery already admitted to the durable
outbox remains replayable after a configured byte-limit reduction; it is never
regenerated under the same replay token.

Cursor validation treats source scan progress and global canonical coverage as
separate axes. Only an exact blockless activation for the next revision,
owner-catchup-only data, or an exact blockless deterministic scan-progress
barrier may advance a rewound scan while preserving the acknowledged canonical
head. Empty data, head loss, same-height identity changes, and all other
non-reorg regressions fail closed. This exception is persisted in the same
SQLite cursor/outbox path, so restart does not discard the activation boundary.

Deploy one service process per SQLite database. See the
[operations guide](https://github.com/KaiCode2/evm-fork-cache-hypersync/blob/main/docs/operations.md)
for storage, TLS, authentication, recovery, and observability requirements.

Licensed under MIT OR Apache-2.0.
