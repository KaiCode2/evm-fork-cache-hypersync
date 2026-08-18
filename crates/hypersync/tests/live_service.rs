use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use alloy_consensus::BlockHeader;
use alloy_eips::BlockId;
use alloy_provider::{Provider, ProviderBuilder, network::AnyNetwork};
use alloy_rpc_types_eth::Filter;
use evm_fork_cache::{
    cache::EvmCache,
    events::StateView,
    reactive::{
        BlockRef, HandlerError, HandlerId, HandlerOutcome, LogInterest, ReactiveCanonicalBaseline,
        ReactiveConfig, ReactiveEngine, ReactiveHandler, ReactiveInput, ReactiveInterest,
        ReactiveRuntime, StateEffectQuality,
    },
};
use evm_fork_cache_event_protocol::{
    PROTOCOL_VERSION,
    v1::{
        Acknowledge, ApplyDesiredState, Backfill, LogInterest as WireLogInterest, OwnerInterests,
        PortableInterest, delivery, portable_interest,
    },
};
use evm_fork_cache_hypersync::{
    ChainDataSource, EventService, HyperSyncDataSource, HyperSyncSourceFactory,
    ManagedEventProvider, SessionStore,
};
use evm_fork_cache_remote::{GrpcEventTransport, RemoteEventTransport, RemoteSubscriber};
use tokio::sync::{Mutex, oneshot};
use tokio_stream::wrappers::TcpListenerStream;

type SharedStore = Arc<Mutex<SessionStore>>;

const SETUP_TIMEOUT: Duration = Duration::from_secs(30);
const REGISTRATION_TIMEOUT: Duration = Duration::from_secs(60);
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(20);
const ACKNOWLEDGEMENT_TIMEOUT: Duration = Duration::from_secs(15);

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

async fn spawn_live_service(store: SharedStore, token: String) -> RunningService {
    let provider = Arc::new(ManagedEventProvider::new(
        HyperSyncSourceFactory::new(token),
        128,
    ));
    let service =
        EventService::new(store, provider, Duration::from_millis(10)).expect("valid poll interval");
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

struct LogObserver;

impl ReactiveHandler for LogObserver {
    fn id(&self) -> HandlerId {
        HandlerId::new("live-log-observer")
    }

    fn interests(&self) -> Vec<ReactiveInterest> {
        vec![ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new(),
            local_matcher: None,
            route_key: None,
        })]
    }

    fn handle(
        &self,
        _context: &evm_fork_cache::reactive::ReactiveContext,
        input: &ReactiveInput,
        _state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        assert!(matches!(input, ReactiveInput::Log(_)));
        Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires ENVIO_API_TOKEN, RPC_URL, and live network access"]
async fn live_service_delivers_through_remote_subscriber_and_runtime_ack() {
    let token = std::env::var("ENVIO_API_TOKEN").expect("ENVIO_API_TOKEN");
    let rpc_url = std::env::var("RPC_URL").expect("RPC_URL");
    let chain_id = std::env::var("HYPERSYNC_TEST_CHAIN_ID")
        .map_or(Ok(1_u64), |value| value.parse())
        .expect("HYPERSYNC_TEST_CHAIN_ID");

    let source = HyperSyncDataSource::new(chain_id, &token).expect("HyperSync client");
    let archive_height = tokio::time::timeout(SETUP_TIMEOUT, source.height())
        .await
        .expect("archive height timeout")
        .expect("archive height");
    let from_block = archive_height.checked_sub(2).expect(
        "live service test requires an actively producing chain with at least two archived blocks",
    );
    let baseline_number = from_block
        .checked_sub(1)
        .expect("live service test requires a predecessor baseline");

    let rpc = Arc::new(
        ProviderBuilder::new()
            .network::<AnyNetwork>()
            .connect_http(rpc_url.parse().expect("RPC_URL format")),
    );
    let rpc_identity_started = Instant::now();
    let rpc_chain_id = tokio::time::timeout(SETUP_TIMEOUT, rpc.get_chain_id())
        .await
        .expect("RPC chain-id timeout")
        .expect("RPC chain id");
    assert_eq!(
        rpc_chain_id, chain_id,
        "RPC_URL and HYPERSYNC_TEST_CHAIN_ID must target the same chain"
    );
    let rpc_head = tokio::time::timeout(SETUP_TIMEOUT, rpc.get_block_number())
        .await
        .expect("RPC head timeout")
        .expect("RPC block number");
    let baseline_block = tokio::time::timeout(
        SETUP_TIMEOUT,
        rpc.get_block_by_number(baseline_number.into()),
    )
    .await
    .expect("baseline block timeout")
    .expect("baseline block request")
    .expect("baseline block");
    let baseline = BlockRef {
        number: baseline_block.header.number(),
        hash: baseline_block.header.hash,
        parent_hash: Some(baseline_block.header.parent_hash()),
        timestamp: Some(baseline_block.header.timestamp()),
    };
    let rpc_identity_elapsed = rpc_identity_started.elapsed();

    let store = Arc::new(Mutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let service = spawn_live_service(Arc::clone(&store), token).await;

    let connect_started = Instant::now();
    let subscriber = tokio::time::timeout(
        SETUP_TIMEOUT,
        RemoteSubscriber::connect(&service.endpoint, "live-runtime-acceptance", chain_id),
    )
    .await
    .expect("remote subscriber connect timeout")
    .expect("connect remote subscriber");
    let connect_elapsed = connect_started.elapsed();
    let mut runtime = ReactiveRuntime::new(ReactiveConfig::default());
    runtime
        .register_handler(Arc::new(LogObserver))
        .expect("install runtime handler before subscriber synchronization");
    let mut engine = ReactiveEngine::new(runtime, subscriber);
    let cache_started = Instant::now();
    let exact_baseline = BlockId::from((baseline.hash, Some(true)));
    let mut cache = tokio::time::timeout(
        SETUP_TIMEOUT,
        EvmCache::at_block(Arc::clone(&rpc), exact_baseline),
    )
    .await
    .expect("RPC cache initialization timeout");
    engine
        .adopt_canonical_baseline(&cache, ReactiveCanonicalBaseline::new(chain_id, baseline))
        .expect("adopt exact RPC baseline");
    let cache_elapsed = cache_started.elapsed();
    let registration_started = Instant::now();
    tokio::time::timeout(
        REGISTRATION_TIMEOUT,
        engine.sync_handler_interests_with_backfill(),
    )
    .await
    .expect("remote registration timeout")
    .expect("register global post-baseline backfill remotely");
    let registration_elapsed = registration_started.elapsed();
    let ingest_started = Instant::now();
    tokio::time::timeout(DELIVERY_TIMEOUT, async {
        while engine
            .runtime()
            .last_canonical_block()
            .is_none_or(|block| block.number < from_block)
        {
            engine
                .next_ingest(&mut cache)
                .await
                .expect("remote ingestion")
                .expect("live delivery");
        }
    })
    .await
    .expect("live delivery timeout");
    let ingest_elapsed = ingest_started.elapsed();

    let canonical = engine
        .runtime()
        .last_canonical_block()
        .expect("runtime canonical position");
    assert!(canonical.number >= from_block);

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let persisted = store
                .lock()
                .await
                .load("live-runtime-acceptance", chain_id)
                .expect("load session");
            if persisted.acknowledged_cursor.is_some() && persisted.pending_delivery.is_none() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("durable runtime acknowledgement");

    let persisted = store
        .lock()
        .await
        .load("live-runtime-acceptance", chain_id)
        .expect("load acknowledged session");
    let cursor = persisted.acknowledged_cursor.expect("acknowledged cursor");
    assert!(cursor.next_block > from_block);
    assert!(cursor.batch_sequence >= 2);
    println!(
        "chain_id={chain_id} archive_height_exclusive={archive_height} rpc_head={rpc_head} archive_lag_blocks={} from_block={from_block} canonical_block={} canonical_hash={:#x} next_block={} grpc_connect_ms={:.2} registration_ms={:.2} rpc_identity_head_ms={:.2} rpc_cache_init_ms={:.2} delivery_ingest_ack_ms={:.2}",
        rpc_head.saturating_add(1).saturating_sub(archive_height),
        canonical.number,
        canonical.hash,
        cursor.next_block,
        connect_elapsed.as_secs_f64() * 1_000.0,
        registration_elapsed.as_secs_f64() * 1_000.0,
        rpc_identity_elapsed.as_secs_f64() * 1_000.0,
        cache_elapsed.as_secs_f64() * 1_000.0,
        ingest_elapsed.as_secs_f64() * 1_000.0,
    );

    drop(engine);
    service.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires ENVIO_API_TOKEN and live network access"]
async fn live_pending_batch_replays_exactly_after_service_restart() {
    let token = std::env::var("ENVIO_API_TOKEN").expect("ENVIO_API_TOKEN");
    let chain_id = std::env::var("HYPERSYNC_TEST_CHAIN_ID")
        .map_or(Ok(1_u64), |value| value.parse())
        .expect("HYPERSYNC_TEST_CHAIN_ID");
    let source = HyperSyncDataSource::new(chain_id, &token).expect("HyperSync client");
    let archive_height = tokio::time::timeout(SETUP_TIMEOUT, source.height())
        .await
        .expect("archive height timeout")
        .expect("archive height");
    let from_block = archive_height.checked_sub(2).expect(
        "live restart test requires an actively producing chain with at least two archived blocks",
    );
    let database_directory = tempfile::tempdir().expect("temporary database directory");
    let database = database_directory.path().join("sessions.sqlite");
    let session_id = "live-restart-replay";

    let first_store = Arc::new(Mutex::new(
        SessionStore::open(&database).expect("first session store"),
    ));
    let first_service = spawn_live_service(Arc::clone(&first_store), token.clone()).await;
    let mut first_transport = tokio::time::timeout(
        SETUP_TIMEOUT,
        GrpcEventTransport::connect(&first_service.endpoint, session_id, chain_id, 0),
    )
    .await
    .expect("first service connect timeout")
    .expect("connect first service");
    tokio::time::timeout(
        REGISTRATION_TIMEOUT,
        first_transport.apply_desired_state(ApplyDesiredState {
            protocol_version: PROTOCOL_VERSION,
            session_id: session_id.into(),
            chain_id,
            expected_revision: 0,
            new_revision: 1,
            owners: vec![OwnerInterests {
                owner_id: "logs".into(),
                interests: vec![PortableInterest {
                    kind: Some(portable_interest::Kind::Log(WireLogInterest {
                        addresses: Vec::new(),
                        topics: Vec::new(),
                    })),
                }],
                backfill: Some(Backfill {
                    from_block,
                    to_block_excl: None,
                    retained_baseline: None,
                }),
                canonical: false,
            }],
        }),
    )
    .await
    .expect("live desired-state registration timeout")
    .expect("apply live desired state");
    let activation = tokio::time::timeout(DELIVERY_TIMEOUT, first_transport.next_delivery())
        .await
        .expect("activation delivery timeout")
        .expect("activation stream")
        .expect("activation barrier");
    tokio::time::timeout(
        ACKNOWLEDGEMENT_TIMEOUT,
        first_transport.acknowledge(Acknowledge {
            session_id: session_id.into(),
            sequence: activation.sequence,
            delivery_token: activation.delivery_token,
        }),
    )
    .await
    .expect("activation acknowledgement timeout")
    .expect("acknowledge activation");
    let first_delivery_started = Instant::now();
    let first_batch = tokio::time::timeout(DELIVERY_TIMEOUT, first_transport.next_delivery())
        .await
        .expect("first delivery timeout")
        .expect("first delivery stream")
        .expect("first live batch");
    let first_delivery_elapsed = first_delivery_started.elapsed();
    let event_count = match first_batch.payload.as_ref().expect("data payload") {
        delivery::Payload::Data(data) => data.records.len(),
        other => panic!("expected data delivery, got {other:?}"),
    };
    assert!(event_count > 0);

    drop(first_transport);
    first_service.stop().await;
    let persisted = first_store
        .lock()
        .await
        .load(session_id, chain_id)
        .expect("load pending delivery");
    assert_eq!(persisted.pending_delivery.as_ref(), Some(&first_batch));
    assert_eq!(
        persisted
            .acknowledged_cursor
            .as_ref()
            .expect("activation cursor")
            .batch_sequence,
        1
    );
    drop(first_store);

    let reopen_started = Instant::now();
    let restarted_store = Arc::new(Mutex::new(
        SessionStore::open(&database).expect("restarted session store"),
    ));
    let reopen_elapsed = reopen_started.elapsed();
    let service_restart_started = Instant::now();
    let restarted_service = spawn_live_service(Arc::clone(&restarted_store), token).await;
    let service_restart_elapsed = service_restart_started.elapsed();
    let reconnect_started = Instant::now();
    let mut restarted_transport = tokio::time::timeout(
        SETUP_TIMEOUT,
        GrpcEventTransport::connect(&restarted_service.endpoint, session_id, chain_id, 0),
    )
    .await
    .expect("restarted service connect timeout")
    .expect("connect restarted service");
    let reconnect_elapsed = reconnect_started.elapsed();
    let replay_started = Instant::now();
    let replay = tokio::time::timeout(Duration::from_secs(5), restarted_transport.next_delivery())
        .await
        .expect("replay timeout")
        .expect("replay stream")
        .expect("replayed batch");
    let replay_elapsed = replay_started.elapsed();
    assert_eq!(replay, first_batch);

    tokio::time::timeout(
        ACKNOWLEDGEMENT_TIMEOUT,
        restarted_transport.acknowledge(Acknowledge {
            session_id: session_id.into(),
            sequence: replay.sequence,
            delivery_token: replay.delivery_token,
        }),
    )
    .await
    .expect("replay acknowledgement request timeout")
    .expect("acknowledge replay");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let persisted = restarted_store
                .lock()
                .await
                .load(session_id, chain_id)
                .expect("load restarted session");
            if persisted.acknowledged_cursor.is_some() && persisted.pending_delivery.is_none() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("replay acknowledgement timeout");

    let committed = restarted_store
        .lock()
        .await
        .load(session_id, chain_id)
        .expect("load committed replay");
    assert_eq!(
        committed
            .acknowledged_cursor
            .expect("acknowledged replay cursor")
            .batch_sequence,
        2
    );
    println!(
        "chain_id={chain_id} first_delivery_ms={:.2} sqlite_reopen_ms={:.2} service_restart_ms={:.2} grpc_reconnect_ms={:.2} persisted_replay_ms={:.2} events={}",
        first_delivery_elapsed.as_secs_f64() * 1_000.0,
        reopen_elapsed.as_secs_f64() * 1_000.0,
        service_restart_elapsed.as_secs_f64() * 1_000.0,
        reconnect_elapsed.as_secs_f64() * 1_000.0,
        replay_elapsed.as_secs_f64() * 1_000.0,
        event_count,
    );

    drop(restarted_transport);
    restarted_service.stop().await;
}
