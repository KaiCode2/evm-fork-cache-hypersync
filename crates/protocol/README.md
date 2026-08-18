# evm-fork-cache-event-protocol

Provider-neutral protobuf and tonic definitions for remote
[`evm-fork-cache`](https://crates.io/crates/evm-fork-cache) event subscribers.

Protocol v1 carries complete revisioned desired state, portable interests,
capability negotiation, demand, ordered deliveries, explicit reorg/finality
controls, opaque provider checkpoints, and post-ingest acknowledgements. It does
not expose HyperSync, Firehose, Reth, Kafka, or other provider-native types.
Delivery tokens are the session sequence encoded as eight big-endian bytes;
provider checkpoints alone are opaque. `Hello.pending_delivery_resume` proves
the exact token, provider checkpoint, and coverage head already embodied by a
restored runtime when that delivery is still in the service outbox. Per-record
audience and delivery scope are orthogonal, and compact canonical progress is
distinct from a complete, hash-verified block header.

Most applications should depend on
[`evm-fork-cache-remote`](https://crates.io/crates/evm-fork-cache-remote) or
[`evm-fork-cache-event-service`](https://crates.io/crates/evm-fork-cache-event-service)
instead of constructing wire messages directly.

See the [protocol contract](https://github.com/KaiCode2/evm-fork-cache-hypersync/blob/main/docs/protocol-v1.md)
for lifecycle, durability, and compatibility guarantees.

Licensed under MIT OR Apache-2.0.
