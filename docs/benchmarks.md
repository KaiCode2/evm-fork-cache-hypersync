# Local performance baseline

Recorded 2026-07-22 on an Apple M1 Pro (arm64, 16 GiB) with
`rustc 1.96.0-nightly (900485642 2026-04-08)`.

```bash
cargo bench -p evm-fork-cache-hypersync --bench normalization -- \
  --warm-up-time 1 --measurement-time 2 --sample-size 10
```

| Benchmark | Input | Estimate |
| --- | --- | ---: |
| `normalize_progress_100_blocks_1000_logs` | 100 compact progress records and 1,000 logs | 499.17 us midpoint (430.69-617.57 us) |
| `normalize_and_protobuf_encode_progress_100_blocks_1000_logs` | The same delivery, including protobuf encoding | 523.40 us midpoint (502.26-547.93 us) |

The midpoints are approximately 2.00 million normalized logs/second before
protobuf encoding and 1.91 million logs/second through protobuf encoding. The
10-sample normalization run contained one high severe outlier, which is why the
full estimate interval is retained rather than presenting only the midpoint.
Criterion's batched setup clones the synthetic source page outside the timed
routine. The first measurement includes sorting, fixed-width conversion,
interest matching, delivery-scope/audience selection, wire-event allocation,
and cursor/token construction; the second additionally encodes the complete
delivery.

This benchmark calls `normalize_page_unchecked` directly and therefore does
**not** time the source engine's hard decoded-response admission, strict page
validation, canonical tracking, provider request, adaptive page splitting,
persistence, gRPC framing, or acknowledgement. The function name is explicit:
production adapters must route untrusted pages through `SourceEngine`. Those
stages must not be inferred from this CPU microbenchmark.

This is a local CPU baseline, not an end-to-end throughput claim. A meaningful
capacity benchmark still needs representative log filters, protobuf
encode/decode under sustained load, explicit SQLite fsync policy, and concurrent
sessions.
In particular, the v1 event service serializes synchronous SQLite operations
through one connection and async mutex; no database-throughput figure has been
measured here. See `docs/operations.md` before sizing or sharding a deployment.

## Alpha release live acceptance snapshot

Recorded 2026-08-18 from the same development machine against Ethereum mainnet,
using authenticated HyperSync and paid RPC/WebSocket endpoints plus a localhost
gRPC service. The suite exercised the final `0.1.0-alpha.1` code against the
published `evm-fork-cache 0.4.0-alpha.4` artifact. These are bounded acceptance
samples, not latency SLOs or statistically stable benchmarks.

```bash
cargo test -p evm-fork-cache-hypersync \
  --test live_hypersync --test live_service --test live_hybrid -- \
  --ignored --nocapture --test-threads=1
```

| Path | Observed result |
| --- | ---: |
| First HyperSync SSE archive-height event | 271.73 ms |
| HyperSync recent-page query plus normalization | 192.08 ms, 1,135 events |
| HyperSync height versus RPC head | exclusive height equalled RPC head; latest archived block was one block behind |
| Local gRPC session negotiation | 1.53 ms |
| Remote desired-state registration and post-baseline backfill | 127.15 ms |
| RPC exact-identity and head lookup | 221.84 ms |
| RPC-backed cache initialization | 48.10 ms |
| Delivery, runtime ingest, and durable ACK | 307.25 ms |
| First durable delivery in restart/replay test | 270.87 ms, 1,133 events |
| SQLite reopen, service restart, and gRPC reconnect | 4.08 ms, 0.16 ms, and 24.06 ms |
| Exact persisted outbox replay after restart | 30.66 ms |

The registration measurement includes the authoritative CAS round trip and the
service's concurrent source setup/backfill work. Because the resulting batch
can arrive while registration is being acknowledged, the later
delivery/ingest/ACK measurement is mostly local transport, runtime ingestion,
and durable commit. The replay measurement uses the already-persisted outbox and
therefore does not contact HyperSync.

The runtime acceptance started from exact RPC block `25781356`, registered
post-baseline interests, and advanced through HyperSync to canonical block
`25781358`; the delivered identity matched the RPC baseline ancestry and the
durable service cursor advanced only after runtime ingestion. The replay test
verified that an unacknowledged batch survives a complete service shutdown and
SQLite reopen, then replays as an exactly equal protocol object before its
acknowledgement advances the durable cursor. Run-to-run network latency and
chain-head timing will vary.

`live_hybrid` is the release gate for extending that acceptance boundary through
the full coordinator. It is designed to hash-pin an RPC cache below the common
RPC/HyperSync head, catch up through the durable remote child, cut over to a real
Alloy WebSocket child, and fail one outer acknowledgement only after the matching
cache/runtime/Hybrid checkpoint is durable. A successful current-harness run
must survive complete local-service, SQLite, durable-store, cache, runtime,
engine, and WebSocket reconstruction; prove that Hybrid clears the exact
restored child outbox before either source is first polled or the rebuilt handler
runs; expose a later `Applied` delivery rather than an invented replay envelope;
and finish with duplicate-free live logs, canonical RPC hash agreement, and a
healthy `Live` phase. Its `hybrid_summary` line separates setup, reopen,
reconnect, preview, async preparation, atomic restore, ACK reconciliation, and
cutover timings. The 2026-08-18 run covered RPC blocks
`25781358..=25781369`, checkpointed block `25781369`, then restored and cut over
through block `25781372`. All checkpoint, final-block, and observed-log hashes
matched the canonical RPC identities; 485 unique logs were delivered with no
duplicates. Lifecycle registration took 1.08 s, the durable checkpoint plus
injected failed outer ACK took 533.74 ms, ACK reconciliation plus the first
later delivery took 6.77 s, live cutover took 11.99 s, and the complete test
took 23.05 s.

## HyperSync versus Alloy WebSocket

Recorded 2026-08-18 from the same machine and credentials as the live acceptance
snapshot. The comparison has two deliberately separate workloads because
"fastest subscriber" means different things at the live tip and during recovery.

### Historical catch-up

The benchmark queried four high-volume Ethereum addresses through the configured
WebSocket RPC and HyperSync. The current harness compares sorted vectors of the
complete shared normalized log payload before accepting a timing: block number
and hash, transaction hash and index, log index, address, topics, data, and
removed status. Comparing vectors also exposes duplicates instead of erasing
them through set semantics. HyperSync fetches the canonical block rows used for
timestamp association and compact progress, so its query does more work than
the RPC log-only result.

The RPC provider rejects more than 20,000 logs in one response. The current
`AlloySubscriber` backfill issues one `eth_getLogs` request for the full range,
so the unmodified path failed at 1,000 blocks with JSON-RPC error `-32005`. To
produce a useful best-effort WebSocket baseline, the benchmark uses sequential
100-block RPC chunks. That chunking is benchmark-only and is not present in the
current subscriber.

| Range | Exactly matched logs | WebSocket RPC median | HyperSync median | HyperSync speedup |
| ---: | ---: | ---: | ---: | ---: |
| 100 blocks | 3,919 | 588.34 ms | 133.78 ms | 4.40x |
| 1,000 blocks | 43,850 | 6.86 s | 1.04 s | 6.59x |

The sampled ranges were `25781256..=25781355` and
`25780356..=25781355`. Both providers returned exactly equal normalized log
vectors, including block and transaction identities, for all three repetitions;
the shared terminal block hash was
`0x91cbb981ebae8c357dabf8412f992d1414561bc58e9de556aeb458c48ef654c2`.

Both rows are medians of three alternating-order repetitions from the final
acceptance run. Counts differ between runs because each range is sampled
relative to the then-current common archive/RPC head.

An attempted 10,000-block RPC comparison spent more than 14 minutes in the
sequential RPC leg and was stopped rather than presented as a completed sample.
That exposed an unbounded benchmark wait. The default acceptance run now covers
100 and 1,000 blocks; 10,000 blocks is opt-in through
`SUBSCRIBER_BENCH_HISTORICAL_RANGES`. Every provider request has a 30-second
timeout and every complete provider sample has a configurable 120-second
default deadline. Failures identify the provider and affected block range. The
production HyperSync query plan independently caps each resumable page at 1,000
blocks and 5,000 logs, preventing one dense catch-up page from growing without
bound before durable delivery and acknowledgement.

### Live head

The live benchmark runs the real Alloy pubsub subscriber and the complete
HyperSync path (SSE archive-height wakeup, compact-progress normalization, event
service, and localhost gRPC client) together. Independent reader tasks timestamp
each provider immediately when its event is received, so HyperSync
acknowledgement cannot delay WebSocket polling. It pairs identical block
numbers, asserts equal hashes, discards the first shared block as warm-up, and
measures the requested number of subsequent blocks. The final alpha harness
sampled three new canonical blocks and matched every block number and hash
across the two sources before recording the delta.

| Metric | HyperSync minus WebSocket |
| --- | ---: |
| Median | +15.44 s |
| Mean | +15.52 s |
| Minimum | +14.87 s |
| Maximum | +16.25 s |

A positive delta means WebSocket arrived first. Local normalization is far below
one block interval, so the observed live gap is dominated by HyperSync archive
availability rather than the extension's local CPU work.

### Reproduce

With `ENVIO_API_TOKEN` and a WebSocket-capable `RPC_URL` or `WS_RPC_URL` in the
environment, set `HYPERSYNC_TEST_CHAIN_ID` when testing a chain other than the
deliberate Ethereum-mainnet default (`1`). The RPC/WebSocket endpoint and
HyperSync chain must agree. Cargo does not read `.env` automatically, so export
it into the current shell with shell tracing disabled before invoking the
harness:

The live service and Hybrid tests require an actively producing chain with logs
inside the selected historical window and new logged blocks after WebSocket
cutover. Quiet chains and inactive custom filters should time out. The Hybrid
harness uses WETH logs on Ethereum mainnet to keep the query active but below
the decoded-response limit, and `HYBRID_LIVE_BACKFILL_BLOCKS=12` by default.
Override the distance while keeping it below the shared RPC/HyperSync archive
tip. HyperSync height is an exclusive upper bound.

```bash
cargo test -p evm-fork-cache-hypersync --test live_comparison \
  compare_historical_catchup_for_identical_ranges_and_filters -- \
  --ignored --nocapture --test-threads=1

cargo test -p evm-fork-cache-hypersync --test live_comparison \
  compare_live_block_arrival_for_identical_hashes -- \
  --ignored --nocapture --test-threads=1

cargo test -p evm-fork-cache-hypersync --test live_hybrid \
  live_hybrid_restart_reconciles_durable_child_before_repoll -- \
  --ignored --exact --nocapture --test-threads=1
```

The built-in historical addresses are Ethereum-mainnet contracts. A non-mainnet
run must set `SUBSCRIBER_BENCH_HISTORICAL_ADDRESSES` to a comma-separated list;
this prevents an irrelevant zero-log comparison from looking successful.
`SUBSCRIBER_BENCH_HISTORICAL_RANGES`,
`SUBSCRIBER_BENCH_HISTORICAL_REPETITIONS`, and
`SUBSCRIBER_BENCH_LIVE_BLOCKS` tune the sample;
`SUBSCRIBER_BENCH_HISTORICAL_SAMPLE_TIMEOUT_SECS` bounds each provider sample,
and `SUBSCRIBER_BENCH_LIVE_TIMEOUT_SECS` bounds the paired-head wait. Historical
RPC and HyperSync requests each have a fixed 30-second timeout. The test derives
`wss://` from an `https://` RPC URL without logging credentials. These small
live-network samples are directional evidence, not provider-independent SLOs.

The historical result supports a hybrid production strategy: WebSocket is
materially fresher at the head, while HyperSync is materially faster and more
reliable for large catch-up ranges and supplies the durable remote delivery
boundary. A pure-HyperSync deployment remains appropriate when the observed
approximately one-Ethereum-block archive lag is acceptable. Live behavior
remains provider- and chain-dependent and must be remeasured for each
deployment.
