use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use alloy_consensus::BlockHeader;
use alloy_primitives::{Address, B256, address};
use alloy_provider::{Provider, ProviderBuilder, WsConnect};
use alloy_rpc_types_eth::Filter;
use evm_fork_cache::reactive::{
    AlloySubscriber, BlockInterest, ChainControl, EventSubscriber, LogInterest, ReactiveInput,
    ReactiveInputBatch, ReactiveInterest, SubscriberConfig, SubscriberMode,
};
use evm_fork_cache_event_protocol::{
    PROTOCOL_VERSION,
    v1::{
        ApplyDesiredState, LogInterest as WireLogInterest, OwnerInterests, PortableInterest,
        portable_interest,
    },
};
use evm_fork_cache_hypersync::{
    ChainDataSource, EventService, HyperSyncDataSource, HyperSyncSourceFactory,
    ManagedEventProvider, SessionStore, compile_query,
};
use evm_fork_cache_remote::RemoteSubscriber;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_stream::wrappers::TcpListenerStream;

const DEFAULT_LIVE_SAMPLES: usize = 3;
const DEFAULT_HISTORICAL_REPETITIONS: usize = 3;
const HISTORICAL_RANGES: [u64; 2] = [100, 1_000];
const WEBSOCKET_HISTORY_CHUNK_BLOCKS: u64 = 100;
const HISTORY_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_HISTORY_SAMPLE_TIMEOUT_SECS: usize = 120;
const SETUP_TIMEOUT: Duration = Duration::from_secs(30);
const REGISTRATION_TIMEOUT: Duration = Duration::from_secs(60);
const ACKNOWLEDGEMENT_TIMEOUT: Duration = Duration::from_secs(15);
const READER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

struct RunningService {
    endpoint: String,
    shutdown: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

impl RunningService {
    async fn stop(self) {
        let _ = self.shutdown.send(());
        tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .expect("event service shutdown timeout")
            .expect("join event service");
    }
}

async fn spawn_live_service(token: String) -> RunningService {
    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let provider = Arc::new(ManagedEventProvider::new(
        HyperSyncSourceFactory::new(token),
        128,
    ));
    let service =
        EventService::new(store, provider, Duration::from_secs(60)).expect("valid poll interval");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind event service");
    let address = listener.local_addr().expect("event service address");
    let (shutdown, shutdown_receiver) = oneshot::channel();
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service.into_server())
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_receiver.await;
            })
            .await
            .expect("serve event service");
    });
    RunningService {
        endpoint: format!("http://{address}"),
        shutdown,
        task,
    }
}

fn websocket_url() -> String {
    let url = std::env::var("WS_RPC_URL")
        .or_else(|_| std::env::var("RPC_URL"))
        .expect("WS_RPC_URL or RPC_URL");
    if let Some(rest) = url.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = url.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        url
    }
}

fn chain_id() -> u64 {
    std::env::var("HYPERSYNC_TEST_CHAIN_ID")
        .map_or(Ok(1_u64), |value| value.parse())
        .expect("HYPERSYNC_TEST_CHAIN_ID")
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .map_or(Ok(default), |value| value.parse())
        .unwrap_or_else(|_| panic!("{name} must be a positive integer"))
        .max(1)
}

fn historical_ranges() -> Vec<u64> {
    let ranges = std::env::var("SUBSCRIBER_BENCH_HISTORICAL_RANGES").map_or_else(
        |_| HISTORICAL_RANGES.to_vec(),
        |value| {
            value
                .split(',')
                .map(|range| {
                    range
                        .trim()
                        .parse::<u64>()
                        .unwrap_or_else(|_| panic!("invalid historical range `{range}`"))
                })
                .collect()
        },
    );
    assert!(
        !ranges.is_empty() && ranges.iter().all(|range| *range > 0),
        "SUBSCRIBER_BENCH_HISTORICAL_RANGES must contain at least one positive range"
    );
    ranges
}

fn batch_blocks(batch: &ReactiveInputBatch) -> Vec<(u64, B256)> {
    let mut blocks = Vec::new();
    for record in batch.records() {
        if let ReactiveInput::BlockHeader(header) = &record.input {
            blocks.push((header.number(), header.hash));
        }
    }
    for control in batch.chain_controls() {
        if let ChainControl::CanonicalProgress(block) = control {
            blocks.push((block.number, block.hash));
        }
    }
    blocks.sort_unstable();
    blocks.dedup();
    blocks
}

fn median_millis(values: &[f64]) -> f64 {
    let mut values = values.to_vec();
    values.sort_by(|left, right| left.total_cmp(right));
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        (values[middle - 1] + values[middle]) / 2.0
    } else {
        values[middle]
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires ENVIO_API_TOKEN, WebSocket-capable RPC_URL, and live network access"]
async fn compare_live_block_arrival_for_identical_hashes() {
    let token = std::env::var("ENVIO_API_TOKEN").expect("ENVIO_API_TOKEN");
    let chain_id = chain_id();
    let sample_count = env_usize("SUBSCRIBER_BENCH_LIVE_BLOCKS", DEFAULT_LIVE_SAMPLES);
    let timeout_secs = env_usize("SUBSCRIBER_BENCH_LIVE_TIMEOUT_SECS", 180) as u64;

    let live_event_timeout = Duration::from_secs(timeout_secs);
    let websocket_provider = tokio::time::timeout(
        SETUP_TIMEOUT,
        ProviderBuilder::new().connect_ws(WsConnect::new(websocket_url())),
    )
    .await
    .expect("WebSocket provider connect timeout")
    .expect("connect websocket provider");
    let rpc_chain_id = tokio::time::timeout(SETUP_TIMEOUT, websocket_provider.get_chain_id())
        .await
        .expect("WebSocket RPC chain-id timeout")
        .expect("WebSocket RPC chain id");
    assert_eq!(
        rpc_chain_id, chain_id,
        "RPC_URL/WS_RPC_URL and HYPERSYNC_TEST_CHAIN_ID must target the same chain"
    );
    let mut websocket = AlloySubscriber::new(
        websocket_provider,
        SubscriberMode::PubSub,
        SubscriberConfig::default(),
    );
    tokio::time::timeout(
        REGISTRATION_TIMEOUT,
        websocket.register_interests(&[ReactiveInterest::Blocks(BlockInterest::default())]),
    )
    .await
    .expect("WebSocket block registration timeout")
    .expect("register websocket block interest");

    let service = spawn_live_service(token).await;
    let mut hypersync = tokio::time::timeout(
        SETUP_TIMEOUT,
        RemoteSubscriber::connect(&service.endpoint, "live-comparison", chain_id),
    )
    .await
    .expect("HyperSync remote subscriber connect timeout")
    .expect("connect HyperSync remote subscriber");
    tokio::time::timeout(
        REGISTRATION_TIMEOUT,
        hypersync.register_interests(&[ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new(),
            local_matcher: None,
            route_key: None,
        })]),
    )
    .await
    .expect("HyperSync compact-progress registration timeout")
    .expect("register HyperSync compact-progress interest");

    let origin = Instant::now();
    #[derive(Clone, Copy)]
    enum ArrivalSource {
        WebSocket,
        HyperSync,
    }
    let (arrival_sender, mut arrival_receiver) = mpsc::channel(64);
    let websocket_sender = arrival_sender.clone();
    let websocket_task = tokio::spawn(async move {
        loop {
            let batch = tokio::time::timeout(live_event_timeout, websocket.next_batch())
                .await
                .expect("websocket live-event timeout")
                .expect("websocket delivery")
                .expect("websocket stream remains active");
            let elapsed = origin.elapsed();
            for (number, hash) in batch_blocks(&batch) {
                if websocket_sender
                    .send((ArrivalSource::WebSocket, number, hash, elapsed))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
    });
    let hypersync_sender = arrival_sender.clone();
    let hypersync_task = tokio::spawn(async move {
        loop {
            let batch = tokio::time::timeout(live_event_timeout, hypersync.next_batch())
                .await
                .expect("HyperSync live-event timeout")
                .expect("HyperSync delivery")
                .expect("HyperSync stream remains active");
            let elapsed = origin.elapsed();
            let blocks = batch_blocks(&batch);
            if let Some(token) = batch.delivery_token().cloned() {
                tokio::time::timeout(
                    ACKNOWLEDGEMENT_TIMEOUT,
                    hypersync.acknowledge_delivery(token),
                )
                .await
                .expect("HyperSync acknowledgement timeout")
                .expect("acknowledge HyperSync delivery");
            }
            for (number, hash) in blocks {
                if hypersync_sender
                    .send((ArrivalSource::HyperSync, number, hash, elapsed))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
    });
    drop(arrival_sender);
    let mut websocket_arrivals = BTreeMap::new();
    let mut hypersync_arrivals = BTreeMap::new();
    let mut paired = BTreeSet::new();
    let mut samples = Vec::with_capacity(sample_count);
    let required_pairs = sample_count
        .checked_add(1)
        .expect("live sample count is too large");

    let paired_result = tokio::time::timeout(live_event_timeout, async {
        while paired.len() < required_pairs {
            let Some((source, number, hash, elapsed)) = arrival_receiver.recv().await else {
                return Err(
                    "both live benchmark readers terminated before enough paired blocks".to_owned(),
                );
            };
            match source {
                ArrivalSource::WebSocket => {
                    websocket_arrivals.entry(number).or_insert((hash, elapsed));
                }
                ArrivalSource::HyperSync => {
                    hypersync_arrivals.entry(number).or_insert((hash, elapsed));
                }
            }

            for (&number, &(websocket_hash, websocket_elapsed)) in &websocket_arrivals {
                let Some(&(hypersync_hash, hypersync_elapsed)) = hypersync_arrivals.get(&number)
                else {
                    continue;
                };
                if websocket_hash != hypersync_hash {
                    return Err(format!(
                        "providers disagreed on block {number}: \
                         websocket={websocket_hash:#x} hypersync={hypersync_hash:#x}"
                    ));
                }
                if paired.insert(number) && paired.len() > 1 && samples.len() < sample_count {
                    let delta_ms = hypersync_elapsed.as_secs_f64() * 1_000.0
                        - websocket_elapsed.as_secs_f64() * 1_000.0;
                    samples.push(delta_ms);
                    println!(
                        "block={number} hash={hash:#x} websocket_ms={:.2} hypersync_ms={:.2} hypersync_minus_websocket_ms={delta_ms:.2}",
                        websocket_elapsed.as_secs_f64() * 1_000.0,
                        hypersync_elapsed.as_secs_f64() * 1_000.0,
                    );
                }
            }
        }
        Ok::<(), String>(())
    })
    .await;
    let paired_failure = match paired_result {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(error),
        Err(_) => Some(format!(
            "paired live block benchmark timeout: websocket_blocks={} hypersync_blocks={} paired={} websocket_latest={:?} hypersync_latest={:?}",
            websocket_arrivals.len(),
            hypersync_arrivals.len(),
            paired.len(),
            websocket_arrivals
                .last_key_value()
                .map(|(number, _)| number),
            hypersync_arrivals
                .last_key_value()
                .map(|(number, _)| number),
        )),
    };

    websocket_task.abort();
    hypersync_task.abort();
    let _ = tokio::time::timeout(READER_SHUTDOWN_TIMEOUT, websocket_task)
        .await
        .expect("WebSocket reader task shutdown timeout");
    let _ = tokio::time::timeout(READER_SHUTDOWN_TIMEOUT, hypersync_task)
        .await
        .expect("HyperSync reader task shutdown timeout");
    service.stop().await;

    if let Some(error) = paired_failure {
        panic!("{error}");
    }

    assert_eq!(samples.len(), sample_count);
    let mean = samples.iter().sum::<f64>() / samples.len() as f64;
    println!(
        "live_summary samples={} hypersync_minus_websocket_mean_ms={mean:.2} median_ms={:.2} min_ms={:.2} max_ms={:.2}",
        samples.len(),
        median_millis(&samples),
        samples.iter().copied().fold(f64::INFINITY, f64::min),
        samples.iter().copied().fold(f64::NEG_INFINITY, f64::max),
    );
}

fn tracked_addresses(chain_id: u64) -> Vec<Address> {
    if let Ok(configured) = std::env::var("SUBSCRIBER_BENCH_HISTORICAL_ADDRESSES") {
        let mut addresses = configured
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| {
                value.parse::<Address>().unwrap_or_else(|_| {
                    panic!("invalid address in SUBSCRIBER_BENCH_HISTORICAL_ADDRESSES")
                })
            })
            .collect::<Vec<_>>();
        addresses.sort_unstable();
        addresses.dedup();
        assert!(
            !addresses.is_empty(),
            "SUBSCRIBER_BENCH_HISTORICAL_ADDRESSES must contain at least one address"
        );
        return addresses;
    }
    assert_eq!(
        chain_id, 1,
        "non-mainnet historical comparisons must set SUBSCRIBER_BENCH_HISTORICAL_ADDRESSES"
    );
    vec![
        address!("B4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc"),
        address!("88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"),
        address!("BA12222222228d8Ba445958a75a0704d566BF2C8"),
        address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),
    ]
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct HistoricalLog {
    block_number: u64,
    block_hash: B256,
    transaction_hash: B256,
    transaction_index: u64,
    log_index: u64,
    address: Address,
    topics: Vec<B256>,
    data: Vec<u8>,
    removed: bool,
}

async fn websocket_history<P: Provider>(
    provider: &P,
    addresses: &[Address],
    from_block: u64,
    to_block_excl: u64,
) -> Vec<HistoricalLog> {
    let mut cursor = from_block;
    let mut normalized = Vec::new();
    while cursor < to_block_excl {
        let chunk_end = cursor
            .saturating_add(WEBSOCKET_HISTORY_CHUNK_BLOCKS)
            .min(to_block_excl);
        let logs = tokio::time::timeout(
            HISTORY_REQUEST_TIMEOUT,
            provider.get_logs(
                &Filter::new()
                    .address(addresses.to_vec())
                    .from_block(cursor)
                    .to_block(chunk_end - 1),
            ),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "WebSocket RPC eth_getLogs timed out after {}s for block range {cursor}..{chunk_end}",
                HISTORY_REQUEST_TIMEOUT.as_secs()
            )
        })
        .unwrap_or_else(|error| {
            panic!(
                "WebSocket RPC eth_getLogs failed for block range {cursor}..{chunk_end}: {error}"
            )
        });
        normalized.extend(logs.into_iter().map(|log| HistoricalLog {
            block_number: log.block_number.expect("RPC log block number"),
            block_hash: log.block_hash.expect("RPC log block hash"),
            transaction_hash: log.transaction_hash.expect("RPC log transaction hash"),
            transaction_index: log.transaction_index.expect("RPC transaction index"),
            log_index: log.log_index.expect("RPC log index"),
            address: log.address(),
            topics: log.topics().to_vec(),
            data: log.inner.data.data.to_vec(),
            removed: log.removed,
        }));
        cursor = chunk_end;
    }
    normalized.sort_unstable();
    normalized
}

async fn hypersync_history(
    source: &HyperSyncDataSource,
    desired_state: &ApplyDesiredState,
    from_block: u64,
    to_block_excl: u64,
) -> Vec<HistoricalLog> {
    let mut cursor = from_block;
    let mut normalized = Vec::new();
    while cursor < to_block_excl {
        let query = compile_query(desired_state, cursor, Some(to_block_excl))
            .expect("compile HyperSync history query");
        let query_end = query.to_block.unwrap_or(to_block_excl);
        let page = tokio::time::timeout(HISTORY_REQUEST_TIMEOUT, source.query(query))
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "HyperSync query timed out after {}s for block range {cursor}..{query_end}",
                    HISTORY_REQUEST_TIMEOUT.as_secs()
                )
            })
            .unwrap_or_else(|error| {
                panic!("HyperSync query failed for block range {cursor}..{query_end}: {error}")
            });
        assert!(
            page.next_block > cursor,
            "HyperSync history made no progress"
        );
        normalized.extend(page.logs.into_iter().map(|log| {
            HistoricalLog {
                block_number: u64::from(log.block_number.expect("HyperSync log block number")),
                block_hash: B256::from_slice(
                    log.block_hash.expect("HyperSync log block hash").as_ref(),
                ),
                transaction_hash: B256::from_slice(
                    log.transaction_hash
                        .expect("HyperSync log transaction hash")
                        .as_ref(),
                ),
                transaction_index: u64::from(
                    log.transaction_index.expect("HyperSync transaction index"),
                ),
                log_index: u64::from(log.log_index.expect("HyperSync log index")),
                address: Address::from_slice(log.address.expect("HyperSync log address").as_ref()),
                topics: log
                    .topics
                    .into_iter()
                    .flatten()
                    .map(|topic| B256::from_slice(topic.as_ref()))
                    .collect(),
                data: log.data.expect("HyperSync log data").as_ref().to_vec(),
                removed: log.removed.expect("HyperSync removed flag"),
            }
        }));
        cursor = page.next_block.min(to_block_excl);
    }
    normalized.sort_unstable();
    normalized
}

async fn measure_history_sample<T>(
    provider: &str,
    from_block: u64,
    to_block_excl: u64,
    sample_timeout: Duration,
    future: impl Future<Output = T>,
) -> (T, f64) {
    let started = Instant::now();
    let output = tokio::time::timeout(sample_timeout, future)
        .await
        .unwrap_or_else(|_| {
            panic!(
                "{provider} historical sample timed out after {}s for block range {from_block}..{to_block_excl}",
                sample_timeout.as_secs()
            )
        });
    (output, started.elapsed().as_secs_f64() * 1_000.0)
}

async fn canonical_hash<P: Provider>(provider: &P, number: u64) -> B256 {
    tokio::time::timeout(
        HISTORY_REQUEST_TIMEOUT,
        provider.get_block_by_number(number.into()),
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "RPC block lookup timed out after {}s at block {number}",
            HISTORY_REQUEST_TIMEOUT.as_secs()
        )
    })
    .unwrap_or_else(|error| panic!("RPC block lookup failed at block {number}: {error}"))
    .unwrap_or_else(|| panic!("RPC block {number} is unavailable"))
    .header
    .hash
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires ENVIO_API_TOKEN, WebSocket-capable RPC_URL, and live network access"]
async fn compare_historical_catchup_for_identical_ranges_and_filters() {
    let token = std::env::var("ENVIO_API_TOKEN").expect("ENVIO_API_TOKEN");
    let chain_id = chain_id();
    let repetitions = env_usize(
        "SUBSCRIBER_BENCH_HISTORICAL_REPETITIONS",
        DEFAULT_HISTORICAL_REPETITIONS,
    );
    let sample_timeout = Duration::from_secs(env_usize(
        "SUBSCRIBER_BENCH_HISTORICAL_SAMPLE_TIMEOUT_SECS",
        DEFAULT_HISTORY_SAMPLE_TIMEOUT_SECS,
    ) as u64);
    let addresses = tracked_addresses(chain_id);
    let websocket_provider = tokio::time::timeout(
        SETUP_TIMEOUT,
        ProviderBuilder::new().connect_ws(WsConnect::new(websocket_url())),
    )
    .await
    .expect("historical WebSocket provider connect timeout")
    .expect("connect websocket provider");
    let rpc_chain_id = tokio::time::timeout(SETUP_TIMEOUT, websocket_provider.get_chain_id())
        .await
        .expect("historical WebSocket RPC chain-id timeout")
        .expect("WebSocket RPC chain id");
    assert_eq!(
        rpc_chain_id, chain_id,
        "RPC_URL/WS_RPC_URL and HYPERSYNC_TEST_CHAIN_ID must target the same chain"
    );
    let source = HyperSyncDataSource::new(chain_id, token).expect("HyperSync client");
    let rpc_head = tokio::time::timeout(SETUP_TIMEOUT, websocket_provider.get_block_number())
        .await
        .expect("historical RPC head timeout")
        .expect("RPC block number");
    let archive_height = tokio::time::timeout(SETUP_TIMEOUT, source.height())
        .await
        .expect("HyperSync archive-height timeout")
        .expect("HyperSync archive height");
    let common_end = archive_height
        .min(rpc_head.saturating_add(1))
        .saturating_sub(5);
    let desired_state = ApplyDesiredState {
        protocol_version: PROTOCOL_VERSION,
        session_id: "historical-comparison".into(),
        chain_id,
        expected_revision: 0,
        new_revision: 1,
        owners: vec![OwnerInterests {
            owner_id: "tracked-amms".into(),
            interests: vec![PortableInterest {
                kind: Some(portable_interest::Kind::Log(WireLogInterest {
                    addresses: addresses
                        .iter()
                        .map(|address| address.as_slice().to_vec())
                        .collect(),
                    topics: Vec::new(),
                })),
            }],
            backfill: None,
            canonical: false,
        }],
    };

    for range in historical_ranges() {
        assert!(
            common_end >= range,
            "requested historical range has {range} blocks, but only {common_end} blocks are \
             available before the shared comparison end"
        );
        let from_block = common_end
            .checked_sub(range)
            .expect("validated historical range subtraction");
        let actual_blocks = common_end - from_block;
        assert_eq!(
            actual_blocks, range,
            "historical benchmark must execute the full requested range"
        );
        assert!(
            from_block < common_end,
            "historical range {range} has no blocks at common head {common_end}"
        );
        let from_hash = canonical_hash(&websocket_provider, from_block).await;
        let to_block_incl = common_end - 1;
        let to_hash = canonical_hash(&websocket_provider, to_block_incl).await;
        let mut websocket_timings = Vec::with_capacity(repetitions);
        let mut hypersync_timings = Vec::with_capacity(repetitions);
        let mut expected_count = None;

        for repetition in 0..repetitions {
            let (websocket_logs, hypersync_logs) = if repetition % 2 == 0 {
                let (websocket_logs, websocket_elapsed) = measure_history_sample(
                    "WebSocket RPC",
                    from_block,
                    common_end,
                    sample_timeout,
                    websocket_history(&websocket_provider, &addresses, from_block, common_end),
                )
                .await;
                websocket_timings.push(websocket_elapsed);

                let (hypersync_logs, hypersync_elapsed) = measure_history_sample(
                    "HyperSync",
                    from_block,
                    common_end,
                    sample_timeout,
                    hypersync_history(&source, &desired_state, from_block, common_end),
                )
                .await;
                hypersync_timings.push(hypersync_elapsed);
                (websocket_logs, hypersync_logs)
            } else {
                let (hypersync_logs, hypersync_elapsed) = measure_history_sample(
                    "HyperSync",
                    from_block,
                    common_end,
                    sample_timeout,
                    hypersync_history(&source, &desired_state, from_block, common_end),
                )
                .await;
                hypersync_timings.push(hypersync_elapsed);

                let (websocket_logs, websocket_elapsed) = measure_history_sample(
                    "WebSocket RPC",
                    from_block,
                    common_end,
                    sample_timeout,
                    websocket_history(&websocket_provider, &addresses, from_block, common_end),
                )
                .await;
                websocket_timings.push(websocket_elapsed);
                (websocket_logs, hypersync_logs)
            };

            assert_eq!(
                websocket_logs, hypersync_logs,
                "providers returned different normalized logs for {range} blocks"
            );
            assert!(
                !websocket_logs.is_empty(),
                "historical comparison matched zero logs for {from_block}..{common_end}; choose active addresses"
            );
            expected_count = Some(websocket_logs.len());
        }

        let websocket_median = median_millis(&websocket_timings);
        let hypersync_median = median_millis(&hypersync_timings);
        println!(
            "history_summary chain_id={chain_id} archive_height_exclusive={archive_height} rpc_head={rpc_head} from_block={from_block} from_hash={from_hash:#x} to_block_incl={to_block_incl} to_hash={to_hash:#x} requested_blocks={range} actual_blocks={actual_blocks} logs={} repetitions={repetitions} websocket_chunk_blocks={WEBSOCKET_HISTORY_CHUNK_BLOCKS} websocket_requests_per_sample={} websocket_median_ms={websocket_median:.2} hypersync_median_ms={hypersync_median:.2} speedup_x={:.2} websocket_samples_ms={websocket_timings:?} hypersync_samples_ms={hypersync_timings:?}",
            expected_count.expect("historical log count"),
            actual_blocks.div_ceil(WEBSOCKET_HISTORY_CHUNK_BLOCKS),
            websocket_median / hypersync_median,
        );
    }
}
