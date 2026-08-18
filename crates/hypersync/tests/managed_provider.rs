use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use evm_fork_cache_event_protocol::{
    PROTOCOL_VERSION,
    v1::{
        Acknowledge, ApplyDesiredState, Backfill, BlockInterest, BlockMode, BlockRef, Capability,
        Cursor, DeliveryScope, LogInterest, OwnerInterests, PortableInterest, chain_event,
        delivery, portable_interest,
    },
};
use evm_fork_cache_hypersync::{
    ChainDataSource, ChainDataSourceFactory, ChainHeightStream, DeliveryRequest, EventService,
    EventSource, EventSourceErrorKind, HyperSyncSourceFactory, ManagedEventProvider, SessionStore,
    SourceError, SourcePage, SourceResponseLimits,
};
use evm_fork_cache_remote::{GrpcEventTransport, RemoteEventTransport};
use hypersync_client::{
    format::{Address, Data, Hash, Quantity},
    net_types::{Query, RollbackGuard},
    simple_types::{Block, Log},
};
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};

macro_rules! source_page {
    (
        archive_height: $archive_height:expr,
        next_block: $next_block:expr,
        blocks: $blocks:expr,
        logs: $logs:expr,
        rollback_guard: $rollback_guard:expr $(,)?
    ) => {
        SourcePage::new($next_block, $blocks, $logs)
            .with_archive_height($archive_height)
            .with_rollback_guard($rollback_guard)
    };
}

#[derive(Clone)]
struct FakeFactory {
    queries: Arc<Mutex<Vec<Query>>>,
}

struct FakeSource {
    queries: Arc<Mutex<Vec<Query>>>,
}

impl ChainDataSourceFactory for FakeFactory {
    type Source = FakeSource;

    fn create(&self, _chain_id: u64) -> Result<Self::Source, SourceError> {
        Ok(FakeSource {
            queries: Arc::clone(&self.queries),
        })
    }
}

#[async_trait]
impl ChainDataSource for FakeSource {
    async fn height(&self) -> Result<u64, SourceError> {
        Ok(12)
    }

    async fn query(&self, query: Query) -> Result<SourcePage, SourceError> {
        self.queries
            .lock()
            .expect("queries lock")
            .push(query.clone());
        let to = query.to_block.expect("bounded provider query");
        let blocks: Vec<_> = (query.from_block..to)
            .map(|number| Block {
                number: Some(number),
                hash: Some(Hash::from([number as u8; 32])),
                parent_hash: Some(Hash::from([number.saturating_sub(1) as u8; 32])),
                timestamp: Some(Quantity::from(1_700_000_000_u64 + number)),
                ..Default::default()
            })
            .collect();
        Ok(source_page! {
            archive_height: Some(to),
            next_block: to,
            blocks: blocks,
            logs: (query.from_block..to)
                .map(|number| Log {
                    removed: Some(false),
                    log_index: Some(0_u64.into()),
                    transaction_index: Some(0_u64.into()),
                    transaction_hash: Some(Hash::from([number as u8 ^ 0x55; 32])),
                    block_hash: Some(Hash::from([number as u8; 32])),
                    block_number: Some(number.into()),
                    address: Some(Address::from([0x33; 20])),
                    data: Some(Data::from(vec![0xaa])),
                    ..Default::default()
                })
                .collect(),
            rollback_guard: Some(RollbackGuard {
                block_number: to - 1,
                timestamp: i64::try_from(1_700_000_000_u64 + to - 1)
                    .expect("fixture timestamp fits i64"),
                hash: Hash::from([(to - 1) as u8; 32]),
                first_block_number: query.from_block,
                first_parent_hash: Hash::from([query.from_block.saturating_sub(1) as u8; 32]),
            }),
        })
    }
}

fn desired_state(revision: u64, from_block: u64) -> ApplyDesiredState {
    ApplyDesiredState {
        protocol_version: PROTOCOL_VERSION,
        session_id: "runtime-a".into(),
        chain_id: 1,
        expected_revision: revision - 1,
        new_revision: revision,
        owners: vec![OwnerInterests {
            owner_id: "pool-a".into(),
            interests: vec![PortableInterest {
                kind: Some(portable_interest::Kind::Log(LogInterest {
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
    }
}

fn delivery_request<'a>(
    desired_state: &'a ApplyDesiredState,
    acknowledged_cursor: Option<&'a Cursor>,
) -> DeliveryRequest<'a> {
    DeliveryRequest::new(desired_state, acknowledged_cursor)
}

fn retained_baseline(number: u64) -> BlockRef {
    BlockRef {
        number,
        hash: vec![number as u8; 32],
        parent_hash: vec![number.saturating_sub(1) as u8; 32],
        timestamp: 1_700_000_000 + number,
    }
}

fn global_desired_state(baseline: BlockRef) -> ApplyDesiredState {
    ApplyDesiredState {
        protocol_version: PROTOCOL_VERSION,
        session_id: "global-restore".into(),
        chain_id: 1,
        expected_revision: 0,
        new_revision: 1,
        owners: vec![OwnerInterests {
            owner_id: String::new(),
            interests: vec![PortableInterest {
                kind: Some(portable_interest::Kind::Log(LogInterest {
                    addresses: Vec::new(),
                    topics: Vec::new(),
                })),
            }],
            backfill: Some(Backfill {
                from_block: baseline.number + 1,
                to_block_excl: None,
                retained_baseline: Some(baseline),
            }),
            canonical: true,
        }],
    }
}

fn block_owner(owner_id: &str, backfill: Option<Backfill>) -> OwnerInterests {
    OwnerInterests {
        owner_id: owner_id.into(),
        interests: vec![PortableInterest {
            kind: Some(portable_interest::Kind::Log(LogInterest {
                addresses: Vec::new(),
                topics: Vec::new(),
            })),
        }],
        backfill,
        canonical: false,
    }
}

#[tokio::test]
async fn managed_provider_maps_hard_response_admission_to_resource_exhaustion() {
    let provider = ManagedEventProvider::new(
        FakeFactory {
            queries: Arc::new(Mutex::new(Vec::new())),
        },
        16,
    )
    .with_response_limits(
        SourceResponseLimits::new(10, 10, 1).expect("nonzero hard response limits"),
    );

    let error = provider
        .next_delivery(delivery_request(&desired_state(1, 10), None))
        .await
        .expect_err("hard local response rejection must reach the service as resource exhaustion");

    assert_eq!(error.kind, EventSourceErrorKind::ResourceExhausted);
    assert!(error.message.contains("dynamic bytes"));
}

#[tokio::test]
async fn first_global_restore_proves_and_extends_the_exact_retained_baseline() {
    let queries = Arc::new(Mutex::new(Vec::new()));
    let provider = ManagedEventProvider::new(
        FakeFactory {
            queries: Arc::clone(&queries),
        },
        16,
    );
    let baseline = retained_baseline(10);

    let delivery = provider
        .next_delivery(delivery_request(
            &global_desired_state(baseline.clone()),
            None,
        ))
        .await
        .expect("prove global baseline")
        .expect("post-baseline delivery");

    assert!(!delivery.checkpoint_neutral);
    assert_eq!(
        delivery
            .cursor
            .as_ref()
            .and_then(|cursor| cursor.canonical_head.as_ref())
            .map(|head| head.number),
        Some(11)
    );
    let queries = queries.lock().expect("queries");
    assert_eq!(queries.len(), 2);
    assert_eq!((queries[0].from_block, queries[0].to_block), (10, Some(11)));
    assert_eq!((queries[1].from_block, queries[1].to_block), (11, Some(12)));
}

#[derive(Clone)]
struct FlakyHeightFactory {
    attempts: Arc<AtomicUsize>,
}

struct FlakyHeightSource {
    attempts: Arc<AtomicUsize>,
}

impl ChainDataSourceFactory for FlakyHeightFactory {
    type Source = FlakyHeightSource;

    fn create(&self, _chain_id: u64) -> Result<Self::Source, SourceError> {
        Ok(FlakyHeightSource {
            attempts: Arc::clone(&self.attempts),
        })
    }
}

#[async_trait]
impl ChainDataSource for FlakyHeightSource {
    async fn height(&self) -> Result<u64, SourceError> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return std::future::pending().await;
        }
        Ok(12)
    }

    async fn query(&self, query: Query) -> Result<SourcePage, SourceError> {
        FakeSource {
            queries: Arc::new(Mutex::new(Vec::new())),
        }
        .query(query)
        .await
    }
}

#[tokio::test]
async fn managed_provider_retries_a_stalled_archive_height_lookup_within_its_deadline() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let provider = ManagedEventProvider::new(
        FlakyHeightFactory {
            attempts: Arc::clone(&attempts),
        },
        16,
    )
    .with_request_timeout(Duration::from_millis(90))
    .expect("valid timeout");

    let delivery = tokio::time::timeout(
        Duration::from_millis(500),
        provider.next_delivery(delivery_request(&desired_state(1, 10), None)),
    )
    .await
    .expect("provider remains bounded")
    .expect("a transient stalled height lookup is retried")
    .expect("backfill delivery");

    assert!(delivery.payload.is_some());
    assert!(
        attempts.load(Ordering::SeqCst) >= 2,
        "the stalled first lookup must be retried"
    );
}

#[derive(Clone)]
struct FlakyBaselineFactory {
    query_attempts: Arc<AtomicUsize>,
}

struct FlakyBaselineSource {
    query_attempts: Arc<AtomicUsize>,
}

impl ChainDataSourceFactory for FlakyBaselineFactory {
    type Source = FlakyBaselineSource;

    fn create(&self, _chain_id: u64) -> Result<Self::Source, SourceError> {
        Ok(FlakyBaselineSource {
            query_attempts: Arc::clone(&self.query_attempts),
        })
    }
}

#[async_trait]
impl ChainDataSource for FlakyBaselineSource {
    async fn height(&self) -> Result<u64, SourceError> {
        Ok(12)
    }

    async fn query(&self, query: Query) -> Result<SourcePage, SourceError> {
        if self.query_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return std::future::pending().await;
        }
        FakeSource {
            queries: Arc::new(Mutex::new(Vec::new())),
        }
        .query(query)
        .await
    }
}

#[tokio::test]
async fn managed_provider_retries_a_stalled_retained_baseline_proof_within_its_deadline() {
    let query_attempts = Arc::new(AtomicUsize::new(0));
    let provider = ManagedEventProvider::new(
        FlakyBaselineFactory {
            query_attempts: Arc::clone(&query_attempts),
        },
        16,
    )
    .with_request_timeout(Duration::from_millis(90))
    .expect("valid timeout");

    let delivery = tokio::time::timeout(
        Duration::from_millis(500),
        provider.next_delivery(delivery_request(
            &global_desired_state(retained_baseline(10)),
            None,
        )),
    )
    .await
    .expect("provider remains bounded")
    .expect("a transient stalled baseline proof is retried")
    .expect("post-baseline delivery");

    assert!(delivery.payload.is_some());
    assert!(
        query_attempts.load(Ordering::SeqCst) >= 3,
        "the stalled proof must be retried before the delivery query"
    );
}

#[tokio::test]
async fn global_restore_rejects_mutated_or_unavailable_baseline_proofs() {
    for mutate in ["hash", "parent"] {
        let queries = Arc::new(Mutex::new(Vec::new()));
        let provider = ManagedEventProvider::new(FakeFactory { queries }, 16);
        let mut baseline = retained_baseline(10);
        if mutate == "hash" {
            baseline.hash = vec![0xee; 32];
        } else {
            baseline.parent_hash = vec![0xee; 32];
        }
        let error = provider
            .next_delivery(delivery_request(&global_desired_state(baseline), None))
            .await
            .expect_err("mutated baseline must fail before engine exposure");
        assert_eq!(error.kind, EventSourceErrorKind::InvalidRequest);
        assert!(
            error
                .message
                .contains("does not match provider canonical identity")
        );
    }

    let provider = ManagedEventProvider::new(MissingBaselineFactory, 16);
    let error = provider
        .next_delivery(delivery_request(
            &global_desired_state(retained_baseline(10)),
            None,
        ))
        .await
        .expect_err("pruned baseline must fail before engine exposure");
    assert_eq!(error.kind, EventSourceErrorKind::Unavailable);
    assert_eq!(
        error.message, "upstream chain data source request failed",
        "raw provider failures must remain behind the source boundary"
    );
    assert!(!error.message.contains("baseline is pruned"));
}

#[tokio::test]
async fn global_baseline_proof_obeys_the_dynamic_response_byte_limit() {
    let provider = ManagedEventProvider::new(
        FakeFactory {
            queries: Arc::new(Mutex::new(Vec::new())),
        },
        16,
    )
    .with_response_limits(
        SourceResponseLimits::new(10, 10, 1).expect("nonzero hard response limits"),
    );
    let error = provider
        .next_delivery(delivery_request(
            &global_desired_state(retained_baseline(10)),
            None,
        ))
        .await
        .expect_err("baseline proof must share hard decoded-response limits");
    assert_eq!(error.kind, EventSourceErrorKind::ResourceExhausted);
    assert!(error.message.contains("dynamic bytes"));
}

#[derive(Clone, Copy)]
struct MissingBaselineFactory;

struct MissingBaselineSource;

impl ChainDataSourceFactory for MissingBaselineFactory {
    type Source = MissingBaselineSource;

    fn create(&self, _chain_id: u64) -> Result<Self::Source, SourceError> {
        Ok(MissingBaselineSource)
    }
}

#[async_trait]
impl ChainDataSource for MissingBaselineSource {
    async fn height(&self) -> Result<u64, SourceError> {
        Ok(12)
    }

    async fn query(&self, _query: Query) -> Result<SourcePage, SourceError> {
        Err(SourceError::request("retained baseline is pruned"))
    }
}

#[tokio::test]
async fn activation_ack_preserves_a_no_backfill_source_at_archive_head() {
    let queries = Arc::new(Mutex::new(Vec::new()));
    let provider = Arc::new(ManagedEventProvider::new(
        FakeFactory {
            queries: Arc::clone(&queries),
        },
        16,
    ));
    let store = Arc::new(AsyncMutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let service = EventService::new(Arc::clone(&store), provider, Duration::from_millis(1))
        .expect("valid service");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind service");
    let address = listener.local_addr().expect("service address");
    let (shutdown, shutdown_receiver) = oneshot::channel();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service.into_server())
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_receiver.await;
            })
            .await
            .expect("serve event service");
    });
    let mut transport = GrpcEventTransport::connect(format!("http://{address}"), "head-only", 1, 0)
        .await
        .expect("connect transport");
    transport
        .apply_desired_state(ApplyDesiredState {
            protocol_version: PROTOCOL_VERSION,
            session_id: "head-only".into(),
            chain_id: 1,
            expected_revision: 0,
            new_revision: 1,
            owners: vec![block_owner("live", None)],
        })
        .await
        .expect("apply desired state");
    let activation = transport.next_delivery().await.unwrap().unwrap();
    let activation_cursor = activation.cursor.as_ref().expect("activation cursor");
    assert_eq!(activation_cursor.next_block, 12);
    assert!(!activation_cursor.provider_checkpoint.is_empty());
    transport
        .acknowledge(Acknowledge {
            session_id: activation.session_id,
            sequence: activation.sequence,
            delivery_token: activation.delivery_token,
        })
        .await
        .expect("ack activation");
    assert_eq!(
        store
            .lock()
            .await
            .load("head-only", 1)
            .expect("load session")
            .acknowledged_cursor
            .expect("acknowledged activation")
            .next_block,
        12
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(30), transport.next_delivery())
            .await
            .is_err(),
        "a no-backfill subscription should wait at archive head"
    );
    assert!(queries.lock().expect("queries").is_empty());

    drop(transport);
    let _ = shutdown.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn revision_backfill_routes_old_blocks_only_to_the_requesting_owner() {
    let provider = ManagedEventProvider::new(
        FakeFactory {
            queries: Arc::new(Mutex::new(Vec::new())),
        },
        16,
    );
    let desired = ApplyDesiredState {
        protocol_version: PROTOCOL_VERSION,
        session_id: "scoped-backfill".into(),
        chain_id: 1,
        expected_revision: 1,
        new_revision: 2,
        owners: vec![
            block_owner("existing", None),
            block_owner(
                "requesting",
                Some(Backfill {
                    from_block: 8,
                    to_block_excl: Some(10),
                    retained_baseline: None,
                }),
            ),
        ],
    };
    let acknowledged = Cursor {
        chain_id: 1,
        query_revision: 1,
        next_block: 12,
        canonical_head: None,
        batch_sequence: 7,
        provider_checkpoint: Vec::new(),
        owner_backfill_activation_block: None,
    };

    let delivery = provider
        .next_delivery(delivery_request(&desired, Some(&acknowledged)))
        .await
        .expect("provider delivery")
        .expect("backfill page");
    let data = match delivery.payload.expect("payload") {
        delivery::Payload::Data(data) => data,
        other => panic!("expected data delivery, got {other:?}"),
    };
    let owner_ids: Vec<_> = data
        .records
        .into_iter()
        .map(|record| record.owner_ids)
        .collect();
    assert_eq!(
        owner_ids,
        [vec!["requesting"], vec!["requesting"]],
        "records outside the bounded owner's range have no downstream audience and must be omitted"
    );
}

#[derive(Clone)]
struct PagedFactory;

struct PagedSource;

impl ChainDataSourceFactory for PagedFactory {
    type Source = PagedSource;

    fn create(&self, _chain_id: u64) -> Result<Self::Source, SourceError> {
        Ok(PagedSource)
    }
}

#[async_trait]
impl ChainDataSource for PagedSource {
    async fn height(&self) -> Result<u64, SourceError> {
        Ok(1_000)
    }

    async fn query(&self, query: Query) -> Result<SourcePage, SourceError> {
        let requested_end = query.to_block.expect("bounded query");
        let to = query.from_block.saturating_add(100).min(requested_end);
        Ok(source_page! {
            archive_height: Some(1_000),
            next_block: to,
            blocks: (query.from_block..to)
                .map(|number| Block {
                    number: Some(number),
                    hash: Some(Hash::from([number as u8; 32])),
                    parent_hash: Some(Hash::from([number.saturating_sub(1) as u8; 32])),
                    timestamp: Some(Quantity::from(1_700_000_000_u64 + number)),
                    ..Default::default()
                })
                .collect(),
            logs: (query.from_block..to)
                .map(|number| Log {
                    removed: Some(false),
                    log_index: Some(0_u64.into()),
                    transaction_index: Some(0_u64.into()),
                    transaction_hash: Some(Hash::from([number as u8 ^ 0x55; 32])),
                    block_hash: Some(Hash::from([number as u8; 32])),
                    block_number: Some(number.into()),
                    address: Some(Address::from([0x33; 20])),
                    data: Some(Data::from(vec![0xaa])),
                    ..Default::default()
                })
                .collect(),
            rollback_guard: Some(RollbackGuard {
                block_number: to - 1,
                timestamp: i64::try_from(1_700_000_000_u64 + to - 1)
                    .expect("fixture timestamp fits i64"),
                hash: Hash::from([(to - 1) as u8; 32]),
                first_block_number: query.from_block,
                first_parent_hash: Hash::from([query.from_block.saturating_sub(1) as u8; 32]),
            }),
        })
    }
}

#[tokio::test]
async fn activation_boundary_survives_restart_mid_backfill() {
    let desired = ApplyDesiredState {
        protocol_version: PROTOCOL_VERSION,
        session_id: "restart-backfill".into(),
        chain_id: 1,
        expected_revision: 1,
        new_revision: 2,
        owners: vec![
            block_owner("existing", None),
            block_owner(
                "bounded",
                Some(Backfill {
                    from_block: 100,
                    to_block_excl: Some(150),
                    retained_baseline: None,
                }),
            ),
        ],
    };
    let prior_revision = Cursor {
        chain_id: 1,
        query_revision: 1,
        next_block: 1_000,
        canonical_head: Some(BlockRef {
            number: 999,
            hash: vec![999_u64 as u8; 32],
            parent_hash: vec![998_u64 as u8; 32],
            timestamp: 1_700_000_999,
        }),
        batch_sequence: 4,
        provider_checkpoint: Vec::new(),
        owner_backfill_activation_block: Some(1_000),
    };
    let first_provider = ManagedEventProvider::new(PagedFactory, 16);
    let first = first_provider
        .next_delivery(delivery_request(&desired, Some(&prior_revision)))
        .await
        .expect("initial backfill page")
        .expect("initial delivery");
    let first_cursor = first.cursor.as_ref().expect("cursor");
    assert_eq!(first_cursor.next_block, 200);
    assert_eq!(
        first_cursor.canonical_head, prior_revision.canonical_head,
        "provider scan progress below activation must preserve the prior global coverage"
    );

    let restarted_provider = ManagedEventProvider::new(PagedFactory, 16);
    let after_restart = restarted_provider
        .next_delivery(delivery_request(&desired, first.cursor.as_ref()))
        .await
        .expect("restored backfill page")
        .expect("restored delivery");
    let barrier = match after_restart.payload.expect("payload") {
        delivery::Payload::Barrier(barrier) => barrier,
        other => panic!("expected scan-progress barrier, got {other:?}"),
    };
    assert_eq!(barrier.id, b"source-progress:2:300");
    assert!(
        barrier.block.is_none(),
        "scan-only owner catch-up progress must not claim a new canonical boundary"
    );
    assert_eq!(
        after_restart
            .cursor
            .as_ref()
            .expect("progress cursor")
            .canonical_head,
        prior_revision.canonical_head,
        "restart must restore scan history and the distinct global coverage head"
    );

    let mut cursor = after_restart.cursor.expect("progress cursor");
    for expected_next in (400..=1_000).step_by(100) {
        let restarted_provider = ManagedEventProvider::new(PagedFactory, 16);
        let delivery = restarted_provider
            .next_delivery(delivery_request(&desired, Some(&cursor)))
            .await
            .expect("restored backfill page")
            .expect("restored progress delivery");
        let progress = match delivery.payload.expect("payload") {
            delivery::Payload::Barrier(barrier) => barrier,
            other => panic!("expected scan-progress barrier, got {other:?}"),
        };
        assert_eq!(
            progress.id,
            format!("source-progress:2:{expected_next}").into_bytes()
        );
        assert!(progress.block.is_none());
        cursor = delivery.cursor.expect("progress cursor");
        assert_eq!(cursor.next_block, expected_next);
        assert_eq!(cursor.canonical_head, prior_revision.canonical_head);
    }

    assert!(
        ManagedEventProvider::new(PagedFactory, 16)
            .next_delivery(delivery_request(&desired, Some(&cursor)))
            .await
            .expect("activation boundary lookup")
            .is_none(),
        "the preserved checkpoint must reach the original activation boundary after repeated restarts"
    );
}

#[tokio::test]
async fn pre_activation_backfill_persists_without_advancing_canonical_coverage() {
    let store = Arc::new(AsyncMutex::new(
        SessionStore::open_in_memory().expect("session store"),
    ));
    let provider = Arc::new(ManagedEventProvider::new(PagedFactory, 16));
    let service = EventService::new(Arc::clone(&store), provider, Duration::from_millis(1))
        .expect("valid service");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind service");
    let address = listener.local_addr().expect("service address");
    let (shutdown, shutdown_receiver) = oneshot::channel();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service.into_server())
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_receiver.await;
            })
            .await
            .expect("serve event service");
    });
    let mut transport =
        GrpcEventTransport::connect(format!("http://{address}"), "owner-backfill", 1, 0)
            .await
            .expect("connect transport");
    transport
        .apply_desired_state(ApplyDesiredState {
            protocol_version: PROTOCOL_VERSION,
            session_id: "owner-backfill".into(),
            chain_id: 1,
            expected_revision: 0,
            new_revision: 1,
            owners: vec![block_owner(
                "historical-owner",
                Some(Backfill {
                    from_block: 100,
                    to_block_excl: Some(150),
                    retained_baseline: None,
                }),
            )],
        })
        .await
        .expect("apply desired state");

    let activation = tokio::time::timeout(Duration::from_secs(2), transport.next_delivery())
        .await
        .expect("activation delivery timeout")
        .expect("activation transport")
        .expect("activation barrier");
    transport
        .acknowledge(Acknowledge {
            session_id: activation.session_id,
            sequence: activation.sequence,
            delivery_token: activation.delivery_token,
        })
        .await
        .expect("ack activation");

    let backfill = tokio::time::timeout(Duration::from_secs(2), transport.next_delivery())
        .await
        .expect("backfill delivery timeout")
        .expect("backfill transport")
        .expect("backfill page");
    let cursor = backfill.cursor.as_ref().expect("backfill cursor");
    assert_eq!(cursor.next_block, 200, "provider scan position advances");
    assert!(
        cursor.canonical_head.is_none(),
        "owner-only history cannot declare global canonical coverage"
    );
    let data = match backfill.payload.as_ref().expect("backfill payload") {
        delivery::Payload::Data(data) => data,
        other => panic!("expected data delivery, got {other:?}"),
    };
    assert!(!data.records.is_empty());
    assert!(data.records.iter().all(|record| {
        record.scope == i32::from(DeliveryScope::OwnerCatchup)
            && !record.canonical_audience
            && record.owner_ids == ["historical-owner"]
    }));
    transport
        .acknowledge(Acknowledge {
            session_id: backfill.session_id,
            sequence: backfill.sequence,
            delivery_token: backfill.delivery_token,
        })
        .await
        .expect("ack persisted owner backfill");

    let persisted = store
        .lock()
        .await
        .load("owner-backfill", 1)
        .expect("load session")
        .acknowledged_cursor
        .expect("acknowledged backfill cursor");
    assert_eq!(persisted.next_block, 200);
    assert!(persisted.canonical_head.is_none());

    drop(transport);
    let _ = shutdown.send(());
    server.await.expect("join server");
}

#[tokio::test]
async fn managed_provider_restores_cursor_and_rewinds_only_for_a_new_revision_backfill() {
    let queries = Arc::new(Mutex::new(Vec::new()));
    let provider = ManagedEventProvider::new(
        FakeFactory {
            queries: Arc::clone(&queries),
        },
        16,
    );
    let revision_one = desired_state(1, 10);
    let first = provider
        .next_delivery(delivery_request(&revision_one, None))
        .await
        .expect("provider batch")
        .expect("historical batch");
    assert_eq!(first.sequence, 1);
    assert_eq!(queries.lock().expect("queries lock")[0].from_block, 10);
    provider
        .acknowledge(
            1,
            &Acknowledge {
                session_id: first.session_id.clone(),
                sequence: first.sequence,
                delivery_token: first.delivery_token.clone(),
            },
            first.cursor.as_ref().expect("first cursor"),
        )
        .await
        .expect("provider acknowledgement");

    let revision_two = desired_state(2, 8);
    let second = provider
        .next_delivery(delivery_request(&revision_two, first.cursor.as_ref()))
        .await
        .expect("revision two provider batch")
        .expect("revision backfill batch");
    assert_eq!(second.sequence, 2);
    assert_eq!(second.query_revision, 2);
    assert_eq!(
        queries
            .lock()
            .expect("queries lock")
            .iter()
            .filter(|query| !query.logs.is_empty())
            .map(|query| query.from_block)
            .collect::<Vec<_>>(),
        [10, 8],
        "one-block anchor queries must not change the revision's data cursor"
    );
}

#[derive(Clone)]
struct UpdatingFactory {
    receiver: Arc<Mutex<Option<mpsc::Receiver<u64>>>>,
    queries: Arc<Mutex<Vec<Query>>>,
    height_calls: Arc<Mutex<usize>>,
    rest_height: Arc<AtomicU64>,
}

struct UpdatingSource {
    receiver: Arc<Mutex<Option<mpsc::Receiver<u64>>>>,
    queries: Arc<Mutex<Vec<Query>>>,
    height_calls: Arc<Mutex<usize>>,
    rest_height: Arc<AtomicU64>,
}

impl ChainDataSourceFactory for UpdatingFactory {
    type Source = UpdatingSource;

    fn create(&self, _chain_id: u64) -> Result<Self::Source, SourceError> {
        Ok(UpdatingSource {
            receiver: Arc::clone(&self.receiver),
            queries: Arc::clone(&self.queries),
            height_calls: Arc::clone(&self.height_calls),
            rest_height: Arc::clone(&self.rest_height),
        })
    }
}

#[async_trait]
impl ChainDataSource for UpdatingSource {
    async fn height(&self) -> Result<u64, SourceError> {
        *self.height_calls.lock().expect("height calls lock") += 1;
        Ok(self.rest_height.load(Ordering::Acquire))
    }

    async fn query(&self, query: Query) -> Result<SourcePage, SourceError> {
        self.queries
            .lock()
            .expect("queries lock")
            .push(query.clone());
        let to = query.to_block.expect("bounded provider query");
        Ok(source_page! {
            archive_height: Some(to),
            next_block: to,
            blocks: (query.from_block..to)
                .map(|number| Block {
                    number: Some(number),
                    hash: Some(Hash::from([number as u8; 32])),
                    parent_hash: Some(Hash::from([number.saturating_sub(1) as u8; 32])),
                    timestamp: Some(Quantity::from(1_700_000_000_u64 + number)),
                    ..Default::default()
                })
                .collect(),
            logs: Vec::new(),
            rollback_guard: None,
        })
    }

    fn height_stream(&self) -> Option<ChainHeightStream> {
        let receiver = self.receiver.lock().expect("height receiver lock").take()?;
        Some(Box::pin(ReceiverStream::new(receiver)))
    }
}

#[tokio::test]
async fn managed_provider_treats_streamed_height_as_a_fallible_wakeup_hint() {
    let (height_sender, height_receiver) = mpsc::channel(1);
    let queries = Arc::new(Mutex::new(Vec::new()));
    let height_calls = Arc::new(Mutex::new(0));
    let rest_height = Arc::new(AtomicU64::new(10));
    let provider = ManagedEventProvider::new(
        UpdatingFactory {
            receiver: Arc::new(Mutex::new(Some(height_receiver))),
            queries: Arc::clone(&queries),
            height_calls: Arc::clone(&height_calls),
            rest_height: Arc::clone(&rest_height),
        },
        16,
    );
    let desired = desired_state(1, 10);

    height_sender
        .send(20)
        .await
        .expect("erroneously high stream height update");
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        provider.wait_for_update(delivery_request(&desired, None)),
    )
    .await
    .expect("height stream wake timeout")
    .expect("height stream wake");
    let high_hint_result = provider
        .next_delivery(delivery_request(&desired, None))
        .await
        .expect("REST reconciliation after high stream hint");
    assert!(
        high_hint_result.is_none(),
        "an unverified high SSE hint must not extend the provider query target"
    );
    assert!(queries.lock().expect("queries lock").is_empty());

    height_sender
        .send(9)
        .await
        .expect("downward stream correction");
    rest_height.store(12, Ordering::Release);
    let batch = provider
        .next_delivery(delivery_request(&desired, None))
        .await
        .expect("poll fallback after downward stream correction")
        .expect("REST-discovered data");

    assert_eq!(batch.cursor.expect("batch cursor").next_block, 12);
    assert_eq!(queries.lock().expect("queries lock")[0].to_block, Some(12));
    assert_eq!(
        *height_calls.lock().expect("height calls lock"),
        3,
        "activation plus both source attempts reconcile against authoritative REST height"
    );
}

#[derive(Clone)]
struct RestartAfterReorgFactory {
    canonical_branch: Arc<AtomicUsize>,
    queries: Arc<Mutex<Vec<Query>>>,
}

struct RestartAfterReorgSource {
    canonical_branch: Arc<AtomicUsize>,
    queries: Arc<Mutex<Vec<Query>>>,
}

fn branch_log(block_number: u64, block_hash: u8) -> Log {
    Log {
        removed: Some(false),
        log_index: Some(0_u64.into()),
        transaction_index: Some(0_u64.into()),
        transaction_hash: Some(Hash::from([block_hash ^ 0x55; 32])),
        block_hash: Some(Hash::from([block_hash; 32])),
        block_number: Some(block_number.into()),
        address: Some(Address::from([0x33; 20])),
        data: Some(Data::from(vec![0xaa])),
        ..Default::default()
    }
}

impl ChainDataSourceFactory for RestartAfterReorgFactory {
    type Source = RestartAfterReorgSource;

    fn create(&self, _chain_id: u64) -> Result<Self::Source, SourceError> {
        Ok(RestartAfterReorgSource {
            canonical_branch: Arc::clone(&self.canonical_branch),
            queries: Arc::clone(&self.queries),
        })
    }
}

#[async_trait]
impl ChainDataSource for RestartAfterReorgSource {
    async fn height(&self) -> Result<u64, SourceError> {
        Ok(if self.canonical_branch.load(Ordering::Acquire) == 0 {
            101
        } else {
            102
        })
    }

    async fn query(&self, query: Query) -> Result<SourcePage, SourceError> {
        self.queries
            .lock()
            .expect("queries lock")
            .push(query.clone());
        let replacement = self.canonical_branch.load(Ordering::Acquire) != 0;
        match (replacement, query.from_block, query.to_block) {
            (false, 99, Some(100)) => Ok(source_page! {
                archive_height: Some(101),
                next_block: 100,
                blocks: vec![Block {
                    number: Some(99),
                    hash: Some(Hash::from([0x99; 32])),
                    parent_hash: Some(Hash::from([0x98; 32])),
                    timestamp: Some(Quantity::from(1_700_000_099_u64)),
                    ..Default::default()
                }],
                logs: Vec::new(),
                rollback_guard: Some(RollbackGuard {
                    block_number: 99,
                    timestamp: 1_700_000_099,
                    hash: Hash::from([0x99; 32]),
                    first_block_number: 99,
                    first_parent_hash: Hash::from([0x98; 32]),
                }),
            }),
            (false, 100, Some(101)) => Ok(source_page! {
                archive_height: Some(101),
                next_block: 101,
                blocks: vec![Block {
                    number: Some(100),
                    hash: Some(Hash::from([0xa0; 32])),
                    parent_hash: Some(Hash::from([0x99; 32])),
                    timestamp: Some(Quantity::from(1_700_000_100_u64)),
                    ..Default::default()
                }],
                logs: vec![branch_log(100, 0xa0)],
                rollback_guard: Some(RollbackGuard {
                    block_number: 100,
                    timestamp: 1_700_000_100,
                    hash: Hash::from([0xa0; 32]),
                    first_block_number: 100,
                    first_parent_hash: Hash::from([0x99; 32]),
                }),
            }),
            (true, 101, Some(102)) => Ok(source_page! {
                archive_height: Some(102),
                next_block: 102,
                blocks: vec![Block {
                    number: Some(101),
                    hash: Some(Hash::from([0xc1; 32])),
                    parent_hash: Some(Hash::from([0xb0; 32])),
                    timestamp: Some(Quantity::from(1_700_000_101_u64)),
                    ..Default::default()
                }],
                logs: vec![branch_log(101, 0xc1)],
                rollback_guard: Some(RollbackGuard {
                    block_number: 101,
                    timestamp: 1_700_000_101,
                    hash: Hash::from([0xc1; 32]),
                    first_block_number: 101,
                    first_parent_hash: Hash::from([0xb0; 32]),
                }),
            }),
            (true, 99, Some(100)) => Ok(source_page! {
                archive_height: Some(102),
                next_block: 100,
                blocks: vec![Block {
                    number: Some(99),
                    hash: Some(Hash::from([0x99; 32])),
                    parent_hash: Some(Hash::from([0x98; 32])),
                    timestamp: Some(Quantity::from(1_700_000_099_u64)),
                    ..Default::default()
                }],
                logs: Vec::new(),
                rollback_guard: Some(RollbackGuard {
                    block_number: 99,
                    timestamp: 1_700_000_099,
                    hash: Hash::from([0x99; 32]),
                    first_block_number: 99,
                    first_parent_hash: Hash::from([0x98; 32]),
                }),
            }),
            (true, 100, Some(101)) => Ok(source_page! {
                archive_height: Some(102),
                next_block: 101,
                blocks: vec![Block {
                    number: Some(100),
                    hash: Some(Hash::from([0xb0; 32])),
                    parent_hash: Some(Hash::from([0x99; 32])),
                    timestamp: Some(Quantity::from(1_700_000_100_u64)),
                    ..Default::default()
                }],
                logs: vec![branch_log(100, 0xb0)],
                rollback_guard: Some(RollbackGuard {
                    block_number: 100,
                    timestamp: 1_700_000_100,
                    hash: Hash::from([0xb0; 32]),
                    first_block_number: 100,
                    first_parent_hash: Hash::from([0x99; 32]),
                }),
            }),
            (true, 100, Some(102)) => Ok(source_page! {
                archive_height: Some(102),
                next_block: 102,
                blocks: vec![
                    Block {
                        number: Some(100),
                        hash: Some(Hash::from([0xb0; 32])),
                        parent_hash: Some(Hash::from([0x99; 32])),
                        timestamp: Some(Quantity::from(1_700_000_100_u64)),
                        ..Default::default()
                    },
                    Block {
                        number: Some(101),
                        hash: Some(Hash::from([0xc1; 32])),
                        parent_hash: Some(Hash::from([0xb0; 32])),
                        timestamp: Some(Quantity::from(1_700_000_101_u64)),
                        ..Default::default()
                    },
                ],
                logs: vec![branch_log(100, 0xb0), branch_log(101, 0xc1)],
                rollback_guard: Some(RollbackGuard {
                    block_number: 101,
                    timestamp: 1_700_000_101,
                    hash: Hash::from([0xc1; 32]),
                    first_block_number: 100,
                    first_parent_hash: Hash::from([0x99; 32]),
                }),
            }),
            other => panic!("unexpected branch/query start {other:?}"),
        }
    }
}

#[derive(Clone)]
struct QuietReplacementFactory {
    queries: Arc<Mutex<Vec<Query>>>,
}

struct QuietReplacementSource {
    queries: Arc<Mutex<Vec<Query>>>,
}

impl ChainDataSourceFactory for QuietReplacementFactory {
    type Source = QuietReplacementSource;

    fn create(&self, _chain_id: u64) -> Result<Self::Source, SourceError> {
        Ok(QuietReplacementSource {
            queries: Arc::clone(&self.queries),
        })
    }
}

#[async_trait]
impl ChainDataSource for QuietReplacementSource {
    async fn height(&self) -> Result<u64, SourceError> {
        // The archive has advanced well past the replacement tip. The durable
        // request must still cap the first post-reorg query at tip + 1.
        Ok(105)
    }

    async fn query(&self, query: Query) -> Result<SourcePage, SourceError> {
        self.queries
            .lock()
            .expect("queries lock")
            .push(query.clone());
        assert_eq!(query.from_block, 100);
        assert_eq!(query.to_block, Some(102));
        Ok(source_page! {
            archive_height: Some(105),
            next_block: 102,
            blocks: vec![
                Block {
                    number: Some(100),
                    hash: Some(Hash::from([0xb0; 32])),
                    parent_hash: Some(Hash::from([0x99; 32])),
                    timestamp: Some(Quantity::from(1_700_000_100_u64)),
                    ..Default::default()
                },
                Block {
                    number: Some(101),
                    hash: Some(Hash::from([0xc1; 32])),
                    parent_hash: Some(Hash::from([0xb0; 32])),
                    timestamp: Some(Quantity::from(1_700_000_101_u64)),
                    ..Default::default()
                },
            ],
            logs: Vec::new(),
            rollback_guard: Some(RollbackGuard {
                block_number: 101,
                timestamp: 1_700_000_101,
                hash: Hash::from([0xc1; 32]),
                first_block_number: 100,
                first_parent_hash: Hash::from([0x99; 32]),
            }),
        })
    }
}

#[tokio::test]
async fn restarted_quiet_reorg_catchup_stops_at_the_durable_replacement_tip() {
    let queries = Arc::new(Mutex::new(Vec::new()));
    let provider = ManagedEventProvider::new(
        QuietReplacementFactory {
            queries: Arc::clone(&queries),
        },
        8,
    );
    let mut desired = desired_state(1, 100);
    desired.owners.clear();
    let acknowledged = Cursor {
        chain_id: 1,
        query_revision: 1,
        next_block: 100,
        canonical_head: Some(BlockRef {
            number: 99,
            hash: vec![0x99; 32],
            parent_hash: vec![0x98; 32],
            timestamp: 1_700_000_099,
        }),
        batch_sequence: 3,
        provider_checkpoint: Vec::new(),
        owner_backfill_activation_block: None,
    };
    let anchor = BlockRef {
        number: 101,
        hash: vec![0xc1; 32],
        parent_hash: vec![0xb0; 32],
        timestamp: 1_700_000_101,
    };

    let replacement = provider
        .next_delivery(
            delivery_request(&desired, Some(&acknowledged))
                .with_required_reorg_anchor(Some(&anchor)),
        )
        .await
        .expect("constrained replacement query")
        .expect("quiet replacement proof");
    let delivery::Payload::Data(data) = replacement.payload.expect("replacement payload") else {
        panic!("quiet replacement must be certified by compact block progress")
    };
    let certified = data
        .records
        .last()
        .and_then(|record| record.event.as_ref())
        .and_then(|event| event.event.as_ref())
        .and_then(|event| match event {
            chain_event::Event::BlockProgress(progress) => progress.block.as_ref(),
            _ => None,
        });
    assert_eq!(certified, Some(&anchor));
    let cursor = replacement.cursor.expect("replacement cursor");
    assert_eq!(cursor.next_block, 102);
    assert_eq!(cursor.canonical_head, Some(anchor));
    assert_eq!(queries.lock().expect("queries lock").len(), 1);
}

#[tokio::test]
async fn managed_provider_rejects_overflowing_or_inconsistent_reorg_constraints() {
    let queries = Arc::new(Mutex::new(Vec::new()));
    let provider = ManagedEventProvider::new(
        QuietReplacementFactory {
            queries: Arc::clone(&queries),
        },
        8,
    );
    let desired = desired_state(1, 10);
    let predecessor = Cursor {
        chain_id: 1,
        query_revision: 1,
        next_block: 10,
        canonical_head: Some(BlockRef {
            number: 9,
            hash: vec![0x09; 32],
            parent_hash: vec![0x08; 32],
            timestamp: 9,
        }),
        batch_sequence: 3,
        provider_checkpoint: Vec::new(),
        owner_backfill_activation_block: None,
    };
    let overflow = BlockRef {
        number: u64::MAX,
        hash: vec![0xff; 32],
        parent_hash: vec![0xfe; 32],
        timestamp: u64::MAX,
    };
    let error = provider
        .next_delivery(
            delivery_request(&desired, Some(&predecessor))
                .with_required_reorg_anchor(Some(&overflow)),
        )
        .await
        .expect_err("overflowing anchor must fail before source access");
    assert_eq!(error.kind, EventSourceErrorKind::InvalidRequest);

    let disconnected = BlockRef {
        number: 10,
        hash: vec![0x10; 32],
        parent_hash: vec![0xee; 32],
        timestamp: 10,
    };
    let error = provider
        .wait_for_update(
            delivery_request(&desired, Some(&predecessor))
                .with_required_reorg_anchor(Some(&disconnected)),
        )
        .await
        .expect_err("disconnected anchor must fail before waiting");
    assert_eq!(error.kind, EventSourceErrorKind::InvalidRequest);
    assert!(queries.lock().expect("queries lock").is_empty());
}

#[tokio::test]
async fn reorg_control_ack_restart_refetches_from_the_common_ancestor_successor() {
    let canonical_branch = Arc::new(AtomicUsize::new(0));
    let queries = Arc::new(Mutex::new(Vec::new()));
    let factory = RestartAfterReorgFactory {
        canonical_branch: Arc::clone(&canonical_branch),
        queries: Arc::clone(&queries),
    };
    let desired = desired_state(1, 100);
    let first_provider = ManagedEventProvider::new(factory.clone(), 8);
    let prior_cursor = Cursor {
        chain_id: 1,
        query_revision: 0,
        next_block: 100,
        canonical_head: Some(BlockRef {
            number: 99,
            hash: vec![0x99; 32],
            parent_hash: vec![0x98; 32],
            timestamp: 1_700_000_099,
        }),
        batch_sequence: 0,
        provider_checkpoint: Vec::new(),
        owner_backfill_activation_block: Some(100),
    };

    let old_branch = first_provider
        .next_delivery(delivery_request(&desired, Some(&prior_cursor)))
        .await
        .expect("old branch query")
        .expect("old branch delivery");
    let old_cursor = old_branch.cursor.as_ref().expect("old cursor");
    first_provider
        .acknowledge(
            1,
            &Acknowledge {
                session_id: old_branch.session_id.clone(),
                sequence: old_branch.sequence,
                delivery_token: old_branch.delivery_token.clone(),
            },
            old_cursor,
        )
        .await
        .expect("acknowledge old branch");

    canonical_branch.store(1, Ordering::Release);
    let reorg = first_provider
        .next_delivery(delivery_request(&desired, Some(old_cursor)))
        .await
        .expect("detect replacement branch")
        .expect("reorg control");
    assert!(matches!(reorg.payload, Some(delivery::Payload::Reorg(_))));
    let reorg_cursor = reorg.cursor.as_ref().expect("reorg cursor");
    assert_eq!(
        reorg_cursor.next_block, 100,
        "the durable reorg cursor must rewind to common_ancestor + 1"
    );
    assert_eq!(
        reorg_cursor
            .canonical_head
            .as_ref()
            .expect("common ancestor")
            .number,
        99
    );
    first_provider
        .acknowledge(
            1,
            &Acknowledge {
                session_id: reorg.session_id.clone(),
                sequence: reorg.sequence,
                delivery_token: reorg.delivery_token.clone(),
            },
            reorg_cursor,
        )
        .await
        .expect("acknowledge reorg control");

    drop(first_provider);
    let restarted = ManagedEventProvider::new(factory, 8);
    let replacement = restarted
        .next_delivery(delivery_request(&desired, Some(reorg_cursor)))
        .await
        .expect("restart replacement query")
        .expect("replacement delivery");
    let replacement_data = match replacement.payload.expect("replacement payload") {
        delivery::Payload::Data(data) => data,
        other => panic!("expected replacement data, got {other:?}"),
    };
    let replacement_blocks = replacement_data
        .records
        .iter()
        .filter_map(|record| match record.event.as_ref()?.event.as_ref()? {
            evm_fork_cache_event_protocol::v1::chain_event::Event::BlockProgress(progress) => {
                progress.block.as_ref().map(|block| block.number)
            }
            evm_fork_cache_event_protocol::v1::chain_event::Event::Log(log) => {
                Some(log.block_number)
            }
            _ => None,
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        replacement_blocks,
        std::collections::BTreeSet::from([100, 101])
    );
    assert_eq!(
        queries
            .lock()
            .expect("queries lock")
            .iter()
            .map(|query| query.from_block)
            .collect::<Vec<_>>(),
        [100, 101, 99, 100, 100, 100]
    );
}

#[derive(Clone)]
struct UnavailableHeightFactory;

struct UnavailableHeightSource;

impl ChainDataSourceFactory for UnavailableHeightFactory {
    type Source = UnavailableHeightSource;

    fn create(&self, _chain_id: u64) -> Result<Self::Source, SourceError> {
        Ok(UnavailableHeightSource)
    }
}

#[async_trait]
impl ChainDataSource for UnavailableHeightSource {
    async fn height(&self) -> Result<u64, SourceError> {
        Ok(0)
    }

    async fn query(&self, _query: Query) -> Result<SourcePage, SourceError> {
        panic!("an unavailable activation height must fail before querying")
    }
}

#[tokio::test]
async fn head_only_activation_rejects_an_unavailable_zero_archive_height() {
    let provider = ManagedEventProvider::new(UnavailableHeightFactory, 8);
    let desired = ApplyDesiredState {
        protocol_version: PROTOCOL_VERSION,
        session_id: "unavailable-height".into(),
        chain_id: 1,
        expected_revision: 0,
        new_revision: 1,
        owners: vec![block_owner("head-only", None)],
    };

    let error = provider
        .next_delivery(delivery_request(&desired, None))
        .await
        .expect_err("zero cannot silently become a genesis activation cursor");

    assert!(error.to_string().contains("archive height is unavailable"));
}

#[derive(Clone)]
struct HangingFactory;

struct HangingSource;

impl ChainDataSourceFactory for HangingFactory {
    type Source = HangingSource;

    fn create(&self, _chain_id: u64) -> Result<Self::Source, SourceError> {
        Ok(HangingSource)
    }
}

#[async_trait]
impl ChainDataSource for HangingSource {
    async fn height(&self) -> Result<u64, SourceError> {
        std::future::pending().await
    }

    async fn query(&self, _query: Query) -> Result<SourcePage, SourceError> {
        std::future::pending().await
    }
}

#[tokio::test]
async fn managed_provider_bounds_a_stalled_source_request() {
    let provider = ManagedEventProvider::new(HangingFactory, 8)
        .with_request_timeout(Duration::from_millis(10))
        .expect("valid timeout");
    let desired = ApplyDesiredState {
        protocol_version: PROTOCOL_VERSION,
        session_id: "stalled-source".into(),
        chain_id: 1,
        expected_revision: 0,
        new_revision: 1,
        owners: vec![block_owner("head-only", None)],
    };

    let error = tokio::time::timeout(
        Duration::from_millis(100),
        provider.next_delivery(delivery_request(&desired, None)),
    )
    .await
    .expect("provider enforces its own request deadline")
    .expect_err("stalled source must return a bounded error");

    assert!(error.to_string().contains("timed out after 10ms"));
}

#[derive(Clone)]
struct HangingQueryFactory;

struct HangingQuerySource;

impl ChainDataSourceFactory for HangingQueryFactory {
    type Source = HangingQuerySource;

    fn create(&self, _chain_id: u64) -> Result<Self::Source, SourceError> {
        Ok(HangingQuerySource)
    }
}

#[async_trait]
impl ChainDataSource for HangingQuerySource {
    async fn height(&self) -> Result<u64, SourceError> {
        Ok(11)
    }

    async fn query(&self, _query: Query) -> Result<SourcePage, SourceError> {
        std::future::pending().await
    }
}

#[tokio::test]
async fn managed_provider_bounds_a_stalled_page_query_without_advancing_the_cursor() {
    let provider = ManagedEventProvider::new(HangingQueryFactory, 8)
        .with_request_timeout(Duration::from_millis(10))
        .expect("valid timeout");
    let desired = desired_state(1, 10);

    for _ in 0..2 {
        let error = tokio::time::timeout(
            Duration::from_millis(100),
            provider.next_delivery(delivery_request(&desired, None)),
        )
        .await
        .expect("provider enforces its own query deadline")
        .expect_err("stalled query must return a bounded error");
        assert!(error.to_string().contains("timed out after 10ms"));
    }
}

#[tokio::test]
async fn managed_provider_enforces_and_releases_its_resident_session_quota() {
    let provider = ManagedEventProvider::new(
        FakeFactory {
            queries: Arc::new(Mutex::new(Vec::new())),
        },
        8,
    )
    .with_max_resident_sessions(2)
    .expect("valid resident-session limit");
    let mut first = desired_state(1, 10);
    first.session_id = "quota-a".into();
    let mut second = desired_state(1, 10);
    second.session_id = "quota-b".into();
    let mut third = desired_state(1, 10);
    third.session_id = "quota-c".into();

    provider
        .next_delivery(delivery_request(&first, None))
        .await
        .expect("first resident session");
    provider
        .next_delivery(delivery_request(&second, None))
        .await
        .expect("second resident session");
    let exhausted = provider
        .next_delivery(delivery_request(&third, None))
        .await
        .expect_err("third resident session must fail closed");
    assert_eq!(exhausted.kind, EventSourceErrorKind::ResourceExhausted);

    provider
        .release_session(&first.session_id, 1)
        .await
        .expect("release first resident session");
    provider
        .next_delivery(delivery_request(&third, None))
        .await
        .expect("release hook frees capacity for another durable session");
    provider
        .release_session(&first.session_id, 1)
        .await
        .expect("repeated release is idempotent");
}

#[tokio::test]
async fn default_hypersync_factory_does_not_claim_or_accept_incomplete_full_headers() {
    let provider = ManagedEventProvider::new(
        HyperSyncSourceFactory::new("00000000-0000-0000-0000-000000000000"),
        8,
    );
    let capabilities = provider.capabilities_for_chain(1);
    assert!(
        !capabilities
            .capabilities
            .contains(&i32::from(Capability::Headers))
    );
    let desired = ApplyDesiredState {
        protocol_version: PROTOCOL_VERSION,
        session_id: "unsupported-headers".into(),
        chain_id: 1,
        expected_revision: 0,
        new_revision: 1,
        owners: vec![OwnerInterests {
            owner_id: "headers".into(),
            interests: vec![PortableInterest {
                kind: Some(portable_interest::Kind::Block(BlockInterest {
                    mode: BlockMode::Header.into(),
                })),
            }],
            backfill: None,
            canonical: false,
        }],
    };

    let error = provider
        .next_delivery(delivery_request(&desired, None))
        .await
        .expect_err("unproven full-header schema must fail during preparation");

    assert_eq!(error.kind, EventSourceErrorKind::Unsupported);
}
