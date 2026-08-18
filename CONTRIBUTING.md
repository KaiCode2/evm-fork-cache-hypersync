# Contributing

Thank you for helping improve the `evm-fork-cache` event-source ecosystem.
Changes here can affect durable replay and canonical chain state, so correctness
and compatibility take priority over convenience.

## Development setup

This workspace targets the exact published `evm-fork-cache 0.4.0-alpha.4`
contract. Use Rust 1.90 or newer. A sibling core checkout is not used by normal
builds, CI, or packaging. Never commit `.env`, provider/RPC credentials, bearer
tokens, TLS keys, or SQLite files and sidecars.

Before submitting a change, run the deterministic workspace gates documented in
[`docs/releasing.md`](docs/releasing.md). Provider-facing changes should include
fixtures or mocks; credentials are not required for the ordinary test suite.
Live ignored tests are an additional acceptance gate for maintainers, not a
substitute for deterministic coverage.

Every public fallible function and trait method must document its failure
contract under `# Errors`; CI enforces this with Clippy in addition to
rustdoc's missing-documentation and broken-link checks.

## Correctness expectations

- Write a failing regression test before changing event, cursor, reorg,
  checkpoint, lifecycle, or acknowledgement behavior.
- Keep provider-native query and checkpoint types behind the `EventSource`
  boundary. Protocol, service, and remote crates must remain provider-neutral.
- Preserve one-in-flight, at-least-once delivery semantics. A delivery is
  acknowledged only after runtime/cache state and its subscriber checkpoint are
  durably committed together.
- Treat provider scan position, runtime-global canonical coverage, finality, and
  owner-scoped catch-up as separate authorities.
- Bound attacker- or provider-controlled counts and encoded bytes before large
  allocations or durable writes, and fail closed when continuity cannot be
  proven.

## Protocol evolution

Protocol v1 is append-only. Do not reuse field numbers or enum values, change a
shipped field's meaning, or expose provider-specific structures. Reserve removed
tags and names. A semantic change that cannot be represented by a new optional
field requires a new protocol package/version and an explicit negotiation path.
See [`docs/protocol-v1.md`](docs/protocol-v1.md).

## Security reports

Do not open a public issue for a suspected vulnerability. Follow
[`SECURITY.md`](SECURITY.md) and include the affected package, trust boundary,
and a minimal reproduction when possible.
