use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use evm_fork_cache_event_protocol::{
    PROTOCOL_VERSION,
    v1::{
        ApplyDesiredState, Backfill, BlockRef, Delivery, LogInterest, OwnerInterests,
        PortableInterest, chain_event, delivery, portable_interest,
    },
};
use evm_fork_cache_hypersync::{
    ChainDataSource, MAX_DELIVERY_SIZE_BYTES, SourceEngine, SourceEngineError, SourceError,
    SourcePage, SourcePageError, SourceResponseLimits, SourceResume,
};
use hypersync_client::{
    format::{Address, Data, Hash, LogArgument, Quantity},
    net_types::{Query, RollbackGuard},
    simple_types::{Block, Log},
};
use prost::Message;

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

macro_rules! source_resume {
    (
        next_block: $next_block:expr,
        sequence: $sequence:expr,
        activation_block: $activation_block:expr,
        canonical_blocks: $canonical_blocks:expr,
        provider_checkpoint: $provider_checkpoint:expr $(,)?
    ) => {
        SourceResume::new($next_block, $sequence, $activation_block)
            .with_canonical_blocks($canonical_blocks)
            .with_provider_checkpoint($provider_checkpoint)
    };
}

fn compact_progress_numbers(delivery: &Delivery) -> Vec<u64> {
    let Some(delivery::Payload::Data(data)) = delivery.payload.as_ref() else {
        return Vec::new();
    };
    data.records
        .iter()
        .filter_map(|record| record.event.as_ref())
        .filter_map(|event| event.event.as_ref())
        .filter_map(|event| match event {
            chain_event::Event::BlockProgress(progress) => {
                progress.block.as_ref().map(|block| block.number)
            }
            _ => None,
        })
        .collect()
}

#[derive(Clone)]
struct RecordingSource {
    queries: Arc<Mutex<Vec<Query>>>,
    page: SourcePage,
}

#[derive(Clone)]
struct PageQueueSource {
    pages: Arc<Mutex<VecDeque<SourcePage>>>,
}

#[async_trait]
impl ChainDataSource for PageQueueSource {
    async fn height(&self) -> Result<u64, SourceError> {
        Ok(101)
    }

    async fn query(&self, _query: Query) -> Result<SourcePage, SourceError> {
        Ok(self
            .pages
            .lock()
            .expect("pages lock")
            .pop_front()
            .expect("queued source page"))
    }
}

#[async_trait]
impl ChainDataSource for RecordingSource {
    async fn height(&self) -> Result<u64, SourceError> {
        Ok(100)
    }

    async fn query(&self, query: Query) -> Result<SourcePage, SourceError> {
        self.queries.lock().expect("queries lock").push(query);
        Ok(self.page.clone())
    }
}

fn desired_state() -> ApplyDesiredState {
    ApplyDesiredState {
        protocol_version: PROTOCOL_VERSION,
        session_id: "runtime-a".into(),
        chain_id: 1,
        expected_revision: 0,
        new_revision: 1,
        owners: Vec::new(),
    }
}

fn matching_log_desired_state() -> ApplyDesiredState {
    let mut desired = desired_state();
    desired.owners.push(OwnerInterests {
        owner_id: "logs".into(),
        interests: vec![PortableInterest {
            kind: Some(portable_interest::Kind::Log(LogInterest {
                addresses: vec![vec![0x33; 20]],
                topics: Vec::new(),
            })),
        }],
        backfill: None,
        canonical: false,
    });
    desired
}

fn page() -> SourcePage {
    source_page! {
        archive_height: Some(101),
        next_block: 101,
        blocks: vec![Block {
            number: Some(100),
            hash: Some(Hash::from([0x10; 32])),
            parent_hash: Some(Hash::from([0x0f; 32])),
            timestamp: Some(Quantity::from(1_700_000_100_u64)),
            ..Default::default()
        }],
        logs: Vec::new(),
        rollback_guard: None,
    }
}

#[tokio::test]
async fn source_engine_replays_unacknowledged_batch_without_advancing_or_refetching() {
    let queries = Arc::new(Mutex::new(Vec::new()));
    let source = RecordingSource {
        queries: Arc::clone(&queries),
        page: page(),
    };
    let mut engine = SourceEngine::new(source, desired_state(), 100, 100, 32);

    let first = engine
        .next_batch(101)
        .await
        .expect("source query")
        .expect("batch");
    let replay = engine
        .next_batch(101)
        .await
        .expect("replay")
        .expect("batch");

    assert_eq!(first.delivery_token, replay.delivery_token);
    assert_eq!(queries.lock().expect("queries lock").len(), 1);
    assert_eq!(engine.committed_next_block(), 100);

    engine
        .acknowledge(&first.delivery_token)
        .expect("matching token should commit");
    assert_eq!(engine.committed_next_block(), 101);
}

#[tokio::test]
async fn owner_scan_preserves_global_coverage_until_activation_then_advances_it() {
    let coverage = BlockRef {
        number: 99,
        hash: vec![0x99; 32],
        parent_hash: vec![0x98; 32],
        timestamp: 1_700_000_099,
    };
    let source = PageQueueSource {
        pages: Arc::new(Mutex::new(VecDeque::from([
            source_page! {
                archive_height: Some(101),
                next_block: 100,
                blocks: vec![block(98, 0x98, 0x97), block(99, 0x99, 0x98)],
                logs: Vec::new(),
                rollback_guard: None,
            },
            source_page! {
                archive_height: Some(101),
                next_block: 101,
                blocks: vec![block(100, 0x10, 0x99)],
                logs: Vec::new(),
                rollback_guard: None,
            },
        ]))),
    };
    let mut engine = SourceEngine::restore(
        source,
        desired_state(),
        SourceResume::new(98, 4, 100).with_coverage_head(Some(coverage.clone())),
        8,
    )
    .expect("restore rewound scan with separate coverage");

    let owner_scan = engine
        .next_batch(100)
        .await
        .expect("owner scan")
        .expect("scan progress");
    assert_eq!(
        owner_scan.cursor.as_ref().expect("cursor").canonical_head,
        Some(coverage)
    );
    assert!(matches!(
        owner_scan.payload.as_ref(),
        Some(delivery::Payload::Barrier(barrier)) if barrier.block.is_none()
    ));
    assert!(owner_scan.checkpoint_neutral);
    engine
        .acknowledge(&owner_scan.delivery_token)
        .expect("ack owner scan");

    let canonical = engine
        .next_batch(101)
        .await
        .expect("canonical continuation")
        .expect("canonical page");
    assert_eq!(
        canonical
            .cursor
            .as_ref()
            .and_then(|cursor| cursor.canonical_head.as_ref())
            .map(|head| head.number),
        Some(100)
    );
    assert_eq!(compact_progress_numbers(&canonical), [100]);
    assert!(!canonical.checkpoint_neutral);
}

#[tokio::test]
async fn empty_canonical_page_emits_non_neutral_compact_progress() {
    let source = RecordingSource {
        queries: Arc::new(Mutex::new(Vec::new())),
        page: source_page! {
            archive_height: Some(101),
            next_block: 101,
            blocks: vec![block(100, 0x10, 0x0f)],
            logs: Vec::new(),
            rollback_guard: None,
        },
    };
    let mut engine = SourceEngine::new(source, desired_state(), 100, 100, 8);

    let delivery = engine
        .next_batch(101)
        .await
        .expect("query")
        .expect("canonical progress");

    assert_eq!(compact_progress_numbers(&delivery), [100]);
    assert!(!delivery.checkpoint_neutral);
    assert_eq!(
        delivery
            .cursor
            .as_ref()
            .and_then(|cursor| cursor.canonical_head.as_ref())
            .map(|block| block.number),
        Some(100)
    );
}

#[tokio::test]
async fn owner_rescan_fails_closed_when_the_preserved_coverage_identity_conflicts() {
    let source = RecordingSource {
        queries: Arc::new(Mutex::new(Vec::new())),
        page: source_page! {
            archive_height: Some(100),
            next_block: 100,
            blocks: vec![block(98, 0x98, 0x97), block(99, 0xff, 0x98)],
            logs: Vec::new(),
            rollback_guard: None,
        },
    };
    let mut engine = SourceEngine::restore(
        source,
        desired_state(),
        SourceResume::new(98, 4, 100).with_coverage_head(Some(BlockRef {
            number: 99,
            hash: vec![0x99; 32],
            parent_hash: vec![0x98; 32],
            timestamp: 1_700_000_099,
        })),
        8,
    )
    .expect("restore rewound scan");

    let error = engine
        .next_batch(100)
        .await
        .expect_err("conflicting preserved coverage must fail closed");
    assert!(matches!(
        error,
        SourceEngineError::CoverageBoundaryConflict { number: 99 }
    ));
}

#[tokio::test]
async fn restored_successor_without_a_guard_must_extend_the_preserved_coverage_head() {
    let source = RecordingSource {
        queries: Arc::new(Mutex::new(Vec::new())),
        page: source_page! {
            archive_height: Some(102),
            next_block: 102,
            blocks: vec![block(101, 0x11, 0xee)],
            logs: Vec::new(),
            rollback_guard: None,
        },
    };
    let mut engine = SourceEngine::restore(
        source,
        desired_state(),
        SourceResume::new(101, 4, 101).with_coverage_head(Some(BlockRef {
            number: 100,
            hash: vec![0x10; 32],
            parent_hash: vec![0x0f; 32],
            timestamp: 1_700_000_100,
        })),
        8,
    )
    .expect("restore exact coverage without retained source history");

    let error = engine
        .next_batch(102)
        .await
        .expect_err("the first restored page must extend the durable coverage identity");
    assert!(matches!(
        error,
        SourceEngineError::CoverageBoundaryConflict { number: 100 }
    ));
}

#[tokio::test]
async fn replayed_sqlite_outbox_ack_synchronizes_a_rebuilt_engine_cursor() {
    let queries = Arc::new(Mutex::new(Vec::new()));
    let source = RecordingSource {
        queries: Arc::clone(&queries),
        page: page(),
    };
    let mut engine = SourceEngine::new(source, desired_state(), 100, 100, 32);
    let resume = SourceResume::new(101, 2, 100)
        .with_canonical_blocks(vec![evm_fork_cache_event_protocol::v1::BlockRef {
            number: 100,
            hash: vec![0x10; 32],
            parent_hash: vec![0x0f; 32],
            timestamp: 1_700_000_100,
        }])
        .with_provider_checkpoint(Some(guard(100, 0x10, 100, 0x0f)))
        .with_coverage_head(Some(evm_fork_cache_event_protocol::v1::BlockRef {
            number: 100,
            hash: vec![0x10; 32],
            parent_hash: vec![0x0f; 32],
            timestamp: 1_700_000_100,
        }));
    engine
        .synchronize_committed_cursor(resume)
        .expect("synchronize committed SQLite cursor");

    assert!(
        engine.next_batch(101).await.expect("no refetch").is_none(),
        "the range committed by a replayed durable delivery must not be queried again"
    );
    assert!(queries.lock().expect("queries lock").is_empty());
    assert_eq!(engine.committed_next_block(), 101);
    assert_eq!(engine.committed_sequence(), 2);
}

#[derive(Clone)]
struct ReorgSource {
    queries: Arc<Mutex<Vec<Query>>>,
}

#[async_trait]
impl ChainDataSource for ReorgSource {
    async fn height(&self) -> Result<u64, SourceError> {
        Ok(101)
    }

    async fn query(&self, query: Query) -> Result<SourcePage, SourceError> {
        let mut queries = self.queries.lock().expect("queries lock");
        let invocation = queries.len();
        let from_block = query.from_block;
        queries.push(query);
        match (invocation, from_block) {
            (0, 100) => Ok(reorg_page(
                101,
                vec![block(100, 0xa0, 0x99)],
                guard(100, 0xa0, 100, 0x99),
            )),
            (1, 99) => Ok(reorg_page(
                100,
                vec![block(99, 0x99, 0x98)],
                guard(99, 0x99, 99, 0x98),
            )),
            (2, 101) => Ok(reorg_page(
                102,
                vec![block(101, 0xc1, 0xb0)],
                guard(101, 0xc1, 101, 0xb0),
            )),
            (3, 99) => Ok(reorg_page(
                100,
                vec![block(99, 0x99, 0x98)],
                guard(99, 0x99, 99, 0x98),
            )),
            (4, 100) => Ok(reorg_page(
                101,
                vec![block(100, 0xb0, 0x99)],
                guard(100, 0xb0, 100, 0x99),
            )),
            (5, 100) => Ok(reorg_page(
                102,
                vec![block(100, 0xb0, 0x99), block(101, 0xc1, 0xb0)],
                guard(101, 0xc1, 100, 0x99),
            )),
            other => panic!("unexpected source query {other:?}"),
        }
    }
}

fn block(number: u64, hash: u8, parent_hash: u8) -> Block {
    Block {
        number: Some(number),
        hash: Some(Hash::from([hash; 32])),
        parent_hash: Some(Hash::from([parent_hash; 32])),
        timestamp: Some(Quantity::from(1_700_000_000_u64 + number)),
        ..Default::default()
    }
}

fn guard(
    block_number: u64,
    hash: u8,
    first_block_number: u64,
    first_parent_hash: u8,
) -> RollbackGuard {
    RollbackGuard {
        block_number,
        timestamp: i64::try_from(1_700_000_000_u64 + block_number)
            .expect("fixture timestamp fits i64"),
        hash: Hash::from([hash; 32]),
        first_block_number,
        first_parent_hash: Hash::from([first_parent_hash; 32]),
    }
}

fn numbered_hash(number: u64, branch: u8) -> Hash {
    let mut bytes = [0_u8; 32];
    bytes[0] = branch;
    bytes[24..].copy_from_slice(&number.to_be_bytes());
    Hash::from(bytes)
}

fn numbered_block(number: u64, common_ancestor: u64) -> Block {
    let branch = u8::from(number > common_ancestor);
    let parent_branch = u8::from(number.saturating_sub(1) > common_ancestor);
    Block {
        number: Some(number),
        hash: Some(numbered_hash(number, branch)),
        parent_hash: Some(numbered_hash(number.saturating_sub(1), parent_branch)),
        timestamp: Some(Quantity::from(1_700_000_000_u64 + number)),
        ..Default::default()
    }
}

fn numbered_guard(blocks: &[Block]) -> RollbackGuard {
    let first = blocks.first().expect("guarded page is nonempty");
    let last = blocks.last().expect("guarded page is nonempty");
    RollbackGuard {
        block_number: last.number.expect("block number"),
        timestamp: i64::try_from(
            last.timestamp
                .as_ref()
                .expect("timestamp")
                .as_ref()
                .iter()
                .fold(0_u64, |value, byte| (value << 8) | u64::from(*byte)),
        )
        .expect("timestamp fits i64"),
        hash: last.hash.clone().expect("hash"),
        first_block_number: first.number.expect("block number"),
        first_parent_hash: first.parent_hash.clone().expect("parent hash"),
    }
}

#[derive(Clone)]
struct DeepRollbackSource {
    queries: Arc<Mutex<Vec<Query>>>,
    common_ancestor: u64,
    target: u64,
}

#[async_trait]
impl ChainDataSource for DeepRollbackSource {
    async fn height(&self) -> Result<u64, SourceError> {
        Ok(self.target)
    }

    async fn query(&self, query: Query) -> Result<SourcePage, SourceError> {
        self.queries
            .lock()
            .expect("queries lock")
            .push(query.clone());
        let requested_end = query.to_block.expect("bounded query");
        let end = if query.from_block == self.common_ancestor + 1 && requested_end == self.target {
            self.target
        } else {
            query.from_block + 1
        };
        let blocks: Vec<_> = (query.from_block..end)
            .map(|number| numbered_block(number, self.common_ancestor))
            .collect();
        let rollback_guard = Some(numbered_guard(&blocks));
        Ok(source_page! {
            archive_height: Some(self.target),
            next_block: end,
            blocks: blocks,
            logs: Vec::new(),
            rollback_guard: rollback_guard,
        })
    }
}

#[tokio::test]
async fn rollback_recovery_finds_a_deep_common_ancestor_with_sublinear_requests() {
    let oldest = 1_000_u64;
    let old_tip = 2_023_u64;
    let target = old_tip + 2;
    let queries = Arc::new(Mutex::new(Vec::new()));
    let source = DeepRollbackSource {
        queries: Arc::clone(&queries),
        common_ancestor: oldest,
        target,
    };
    let canonical_blocks = (oldest..=old_tip)
        .map(|number| evm_fork_cache_event_protocol::v1::BlockRef {
            number,
            hash: numbered_hash(number, 0).as_ref().to_vec(),
            parent_hash: numbered_hash(number - 1, 0).as_ref().to_vec(),
            timestamp: 1_700_000_000 + number,
        })
        .collect();
    let mut engine = SourceEngine::restore(
        source,
        desired_state(),
        source_resume! {
            next_block: old_tip + 1,
            sequence: 9,
            activation_block: oldest,
            canonical_blocks: canonical_blocks,
            provider_checkpoint: Some(RollbackGuard {
                block_number: old_tip,
                timestamp: i64::try_from(1_700_000_000 + old_tip).expect("timestamp"),
                hash: numbered_hash(old_tip, 0),
                first_block_number: old_tip,
                first_parent_hash: numbered_hash(old_tip - 1, 0),
            }),
        },
        1_024,
    )
    .expect("restore deep canonical suffix");

    let delivery = engine
        .next_batch(target)
        .await
        .expect("bounded rollback recovery")
        .expect("reorg control");
    let reorg = match delivery.payload.expect("payload") {
        delivery::Payload::Reorg(reorg) => reorg,
        other => panic!("expected reorg, got {other:?}"),
    };

    assert_eq!(reorg.common_ancestor.expect("ancestor").number, oldest);
    assert!(
        queries.lock().expect("queries lock").len() <= 14,
        "a 1,024-block retention window should require logarithmic probing plus data queries"
    );
}

fn reorg_page(next_block: u64, blocks: Vec<Block>, guard: RollbackGuard) -> SourcePage {
    source_page! {
        archive_height: Some(102),
        next_block: next_block,
        blocks: blocks,
        logs: Vec::new(),
        rollback_guard: Some(guard),
    }
}

#[tokio::test]
async fn source_engine_backtracks_and_delivers_the_replacement_branch_on_rollback() {
    let queries = Arc::new(Mutex::new(Vec::new()));
    let source = ReorgSource {
        queries: Arc::clone(&queries),
    };
    let mut engine = SourceEngine::new(source, desired_state(), 100, 100, 8);

    let initial = engine
        .next_batch(101)
        .await
        .expect("initial page")
        .expect("initial batch");
    engine
        .acknowledge(&initial.delivery_token)
        .expect("commit initial page");

    let reorg = engine
        .next_batch(102)
        .await
        .expect("rollback recovery")
        .expect("reorg control");
    let common_ancestor = match reorg.payload.as_ref().expect("reorg payload") {
        delivery::Payload::Reorg(reorg) => reorg
            .common_ancestor
            .as_ref()
            .expect("reorg common ancestor"),
        other => panic!("expected reorg, got {other:?}"),
    };
    assert_eq!(common_ancestor.number, 99);
    assert_eq!(common_ancestor.hash, vec![0x99; 32]);
    assert_eq!(common_ancestor.parent_hash, vec![0x98; 32]);
    assert_eq!(common_ancestor.timestamp, 1_700_000_099);
    engine
        .acknowledge(&reorg.delivery_token)
        .expect("commit reorg control");
    let replacement = engine
        .next_batch(102)
        .await
        .expect("queued replacement")
        .expect("replacement progress");
    assert_eq!(compact_progress_numbers(&replacement), [100, 101]);
    assert!(!replacement.checkpoint_neutral);
    assert_eq!(
        replacement
            .cursor
            .as_ref()
            .and_then(|cursor| cursor.canonical_head.as_ref())
            .expect("replacement cursor tip")
            .number,
        101
    );
    assert_eq!(
        queries
            .lock()
            .expect("queries lock")
            .iter()
            .map(|query| query.from_block)
            .collect::<Vec<_>>(),
        vec![100, 99, 101, 99, 100, 100]
    );
}

#[derive(Clone)]
struct RestartReorgSource {
    queries: Arc<Mutex<Vec<Query>>>,
}

#[async_trait]
impl ChainDataSource for RestartReorgSource {
    async fn height(&self) -> Result<u64, SourceError> {
        Ok(102)
    }

    async fn query(&self, query: Query) -> Result<SourcePage, SourceError> {
        let mut queries = self.queries.lock().expect("queries lock");
        let from_block = query.from_block;
        let to_block = query.to_block;
        queries.push(query);
        match (from_block, to_block) {
            (101, Some(102)) => Ok(reorg_page(
                102,
                vec![block(101, 0xc1, 0xb0)],
                guard(101, 0xc1, 101, 0xb0),
            )),
            (99, Some(100)) => Ok(reorg_page(
                100,
                vec![block(99, 0x99, 0x98)],
                guard(99, 0x99, 99, 0x98),
            )),
            (100, Some(101)) => Ok(reorg_page(
                101,
                vec![block(100, 0xb0, 0x99)],
                guard(100, 0xb0, 100, 0x99),
            )),
            (100, Some(102)) => Ok(reorg_page(
                102,
                vec![block(100, 0xb0, 0x99), block(101, 0xc1, 0xb0)],
                guard(101, 0xc1, 100, 0x99),
            )),
            other => panic!("unexpected restart reorg query {other:?}"),
        }
    }
}

#[tokio::test]
async fn restored_canonical_history_resolves_a_one_block_reorg() {
    let queries = Arc::new(Mutex::new(Vec::new()));
    let source = RestartReorgSource {
        queries: Arc::clone(&queries),
    };
    let mut engine = SourceEngine::restore(
        source,
        desired_state(),
        source_resume! {
            next_block: 101,
            sequence: 4,
            activation_block: 100,
            canonical_blocks: vec![
                evm_fork_cache_event_protocol::v1::BlockRef {
                    number: 99,
                    hash: vec![0x99; 32],
                    parent_hash: vec![0x98; 32],
                    timestamp: 1_700_000_099,
                },
                evm_fork_cache_event_protocol::v1::BlockRef {
                    number: 100,
                    hash: vec![0xa0; 32],
                    parent_hash: vec![0x99; 32],
                    timestamp: 1_700_000_100,
                },
            ],
            provider_checkpoint: Some(guard(100, 0xa0, 100, 0x99)),
        },
        8,
    )
    .expect("restore canonical checkpoint");

    let reorg = engine
        .next_batch(102)
        .await
        .expect("recover after restart")
        .expect("reorg control");
    assert!(matches!(reorg.payload, Some(delivery::Payload::Reorg(_))));
    assert_eq!(
        queries
            .lock()
            .expect("queries lock")
            .iter()
            .map(|query| query.from_block)
            .collect::<Vec<_>>(),
        vec![101, 99, 100, 100]
    );
}

#[tokio::test]
async fn owner_only_rollback_never_emits_a_global_reorg_control() {
    let source = RestartReorgSource {
        queries: Arc::new(Mutex::new(Vec::new())),
    };
    let mut engine = SourceEngine::restore(
        source,
        desired_state(),
        source_resume! {
            next_block: 101,
            sequence: 4,
            activation_block: 102,
            canonical_blocks: vec![
                evm_fork_cache_event_protocol::v1::BlockRef {
                    number: 99,
                    hash: vec![0x99; 32],
                    parent_hash: vec![0x98; 32],
                    timestamp: 1_700_000_099,
                },
                evm_fork_cache_event_protocol::v1::BlockRef {
                    number: 100,
                    hash: vec![0xa0; 32],
                    parent_hash: vec![0x99; 32],
                    timestamp: 1_700_000_100,
                },
            ],
            provider_checkpoint: Some(guard(100, 0xa0, 100, 0x99)),
        },
        8,
    )
    .expect("restore owner-only checkpoint");

    let error = engine
        .next_batch(102)
        .await
        .expect_err("an owner-only fork cannot be represented by a global reorg control");

    assert!(matches!(
        error,
        SourceEngineError::OwnerCatchupReorg {
            activation_block: 102,
            common_ancestor: 99,
            old_tip: 100,
            new_tip: 101,
        }
    ));
    assert_eq!(engine.committed_next_block(), 101);
    assert_eq!(engine.committed_sequence(), 4);
}

#[tokio::test]
async fn global_backfill_rollback_emits_a_reorg_before_replacement_delivery() {
    let source = RestartReorgSource {
        queries: Arc::new(Mutex::new(Vec::new())),
    };
    let mut desired = desired_state();
    desired.owners = vec![OwnerInterests {
        owner_id: String::new(),
        interests: Vec::new(),
        backfill: Some(Backfill {
            from_block: 100,
            to_block_excl: None,
            retained_baseline: Some(BlockRef {
                number: 99,
                hash: vec![0x99; 32],
                parent_hash: vec![0x98; 32],
                timestamp: 1_700_000_099,
            }),
        }),
        canonical: true,
    }];
    let mut engine = SourceEngine::restore(
        source,
        desired,
        source_resume! {
            next_block: 101,
            sequence: 4,
            activation_block: 102,
            canonical_blocks: vec![
                BlockRef {
                    number: 99,
                    hash: vec![0x99; 32],
                    parent_hash: vec![0x98; 32],
                    timestamp: 1_700_000_099,
                },
                BlockRef {
                    number: 100,
                    hash: vec![0xa0; 32],
                    parent_hash: vec![0x99; 32],
                    timestamp: 1_700_000_100,
                },
            ],
            provider_checkpoint: Some(guard(100, 0xa0, 100, 0x99)),
        },
        8,
    )
    .expect("restore global backfill checkpoint");

    let delivery = engine
        .next_batch(102)
        .await
        .expect("global rollback recovery")
        .expect("reorg control");
    assert!(matches!(
        delivery.payload,
        Some(delivery::Payload::Reorg(ref reorg))
            if reorg.common_ancestor.as_ref().map(|block| block.number) == Some(99)
    ));
}

#[tokio::test]
async fn source_engine_fails_closed_before_delivery_sequence_reuse() {
    let queries = Arc::new(Mutex::new(Vec::new()));
    let source = RecordingSource {
        queries: Arc::clone(&queries),
        page: page(),
    };
    let mut engine = SourceEngine::restore(
        source,
        desired_state(),
        source_resume! {
            next_block: 100,
            sequence: u64::MAX,
            activation_block: 100,
            canonical_blocks: Vec::new(),
            provider_checkpoint: None,
        },
        8,
    )
    .expect("restore exhausted sequence");

    let error = engine
        .next_batch(101)
        .await
        .expect_err("sequence exhaustion must fail closed");
    assert!(matches!(error, SourceEngineError::SequenceExhausted));
    assert!(
        queries.lock().expect("queries lock").is_empty(),
        "an exhausted sequence must fail before fetching data that cannot be tokenized"
    );
}

#[tokio::test]
async fn source_engine_rejects_a_page_cursor_beyond_the_requested_target() {
    let queries = Arc::new(Mutex::new(Vec::new()));
    let source = RecordingSource {
        queries: Arc::clone(&queries),
        page: source_page! {
            archive_height: Some(102),
            next_block: 102,
            blocks: vec![block(100, 0x10, 0x0f), block(101, 0x11, 0x10)],
            logs: Vec::new(),
            rollback_guard: None,
        },
    };
    let mut engine = SourceEngine::new(source, desired_state(), 100, 100, 8);

    let error = engine
        .next_batch(101)
        .await
        .expect_err("a provider must not advance beyond the requested range");
    assert!(matches!(
        error,
        SourceEngineError::InvalidPage(
            evm_fork_cache_hypersync::SourcePageError::CursorBeyondTarget {
                next_block: 102,
                to_block_excl: 101,
            }
        )
    ));
    assert_eq!(engine.committed_next_block(), 100);
}

#[derive(Clone)]
struct SizeAdaptiveSource {
    queries: Arc<Mutex<Vec<Query>>>,
}

#[async_trait]
impl ChainDataSource for SizeAdaptiveSource {
    async fn height(&self) -> Result<u64, SourceError> {
        Ok(104)
    }

    async fn query(&self, query: Query) -> Result<SourcePage, SourceError> {
        self.queries
            .lock()
            .expect("queries lock")
            .push(query.clone());
        let requested_end = query.to_block.expect("bounded query");
        let next_block = query
            .from_block
            .saturating_add(query.max_num_blocks.expect("block limit") as u64)
            .min(requested_end);
        let blocks = (query.from_block..next_block)
            .map(|number| block(number, number as u8, number.saturating_sub(1) as u8))
            .collect();
        let logs = (query.from_block..next_block)
            .map(|number| Log {
                removed: Some(false),
                log_index: Some(0_u64.into()),
                transaction_index: Some(0_u64.into()),
                transaction_hash: Some(Hash::from([number as u8 ^ 0x55; 32])),
                block_hash: Some(Hash::from([number as u8; 32])),
                block_number: Some(number.into()),
                address: Some(Address::from([0x33; 20])),
                data: Some(Data::from(vec![0xaa; 800])),
                ..Default::default()
            })
            .collect();
        Ok(source_page! {
            archive_height: Some(104),
            next_block: next_block,
            blocks: blocks,
            logs: logs,
            rollback_guard: None,
        })
    }
}

#[tokio::test]
async fn source_engine_requeries_with_smaller_pages_before_persisting_an_oversized_delivery() {
    let queries = Arc::new(Mutex::new(Vec::new()));
    let source = SizeAdaptiveSource {
        queries: Arc::clone(&queries),
    };
    let mut engine = SourceEngine::new(source, matching_log_desired_state(), 100, 100, 8)
        .with_max_delivery_bytes(2_500)
        .expect("valid delivery limit");

    let delivery = engine
        .next_batch(104)
        .await
        .expect("adapt oversized page")
        .expect("bounded delivery");

    assert!(delivery.encoded_len() <= 2_500);
    let block_limits: Vec<_> = queries
        .lock()
        .expect("queries lock")
        .iter()
        .map(|query| query.max_num_blocks.expect("block limit"))
        .collect();
    assert_eq!(block_limits, [4, 2]);
    assert_eq!(delivery.cursor.expect("cursor").next_block, 102);
}

#[tokio::test]
async fn source_engine_fails_before_persisting_an_irreducible_oversized_record() {
    let queries = Arc::new(Mutex::new(Vec::new()));
    let source = SizeAdaptiveSource {
        queries: Arc::clone(&queries),
    };
    let mut engine = SourceEngine::new(source, matching_log_desired_state(), 100, 100, 8)
        .with_max_delivery_bytes(300)
        .expect("valid delivery limit");

    let error = engine
        .next_batch(101)
        .await
        .expect_err("one oversized log cannot be split into a safe delivery");

    assert!(matches!(
        error,
        SourceEngineError::DeliveryTooLarge { limit: 300, .. }
    ));
    assert_eq!(engine.committed_next_block(), 100);
    let limits: Vec<_> = queries
        .lock()
        .expect("queries lock")
        .iter()
        .map(|query| {
            (
                query.max_num_blocks.expect("block limit"),
                query.max_num_logs.expect("log limit"),
            )
        })
        .collect();
    assert_eq!(limits.last(), Some(&(1, 1)));
}

fn source_log(block_number: u64, block_hash: u8, transaction_hash: u8, log_index: u64) -> Log {
    Log {
        removed: Some(false),
        log_index: Some(log_index.into()),
        transaction_index: Some(0_u64.into()),
        transaction_hash: Some(Hash::from([transaction_hash; 32])),
        block_hash: Some(Hash::from([block_hash; 32])),
        block_number: Some(block_number.into()),
        address: Some(Address::from([0x33; 20])),
        data: Some(Data::from(vec![0xaa])),
        ..Default::default()
    }
}

async fn invalid_page_error(page: SourcePage, target: u64) -> SourceEngineError {
    let source = RecordingSource {
        queries: Arc::new(Mutex::new(Vec::new())),
        page,
    };
    SourceEngine::new(source, desired_state(), 100, 100, 8)
        .next_batch(target)
        .await
        .expect_err("malformed source page must fail closed")
}

#[tokio::test]
async fn source_engine_rejects_a_response_above_the_hard_log_count_before_normalization() {
    let mut malformed = source_log(100, 0x10, 0x20, 0);
    malformed.data = None;
    let source = RecordingSource {
        queries: Arc::new(Mutex::new(Vec::new())),
        page: source_page! {
            archive_height: Some(101),
            next_block: 101,
            blocks: vec![block(100, 0x10, 0x0f)],
            logs: vec![malformed.clone(), malformed],
            rollback_guard: None,
        },
    };
    let limits = SourceResponseLimits::new(4, 1, 1_000_000).expect("valid hard limits");
    let mut engine =
        SourceEngine::new(source, desired_state(), 100, 100, 8).with_response_limits(limits);

    let error = engine
        .next_batch(101)
        .await
        .expect_err("hard row-count policy must run before page normalization");

    assert!(matches!(
        error,
        SourceEngineError::ResponseLimitExceeded {
            resource: "logs",
            observed: 2,
            limit: 1,
        }
    ));
}

#[tokio::test]
async fn source_engine_rejects_a_response_above_the_hard_block_count() {
    let source = RecordingSource {
        queries: Arc::new(Mutex::new(Vec::new())),
        page: source_page! {
            archive_height: Some(102),
            next_block: 102,
            blocks: vec![block(100, 0x10, 0x0f), block(101, 0x11, 0x10)],
            logs: Vec::new(),
            rollback_guard: None,
        },
    };
    let limits = SourceResponseLimits::new(1, 4, 1_000_000).expect("valid hard limits");
    let mut engine =
        SourceEngine::new(source, desired_state(), 100, 100, 8).with_response_limits(limits);

    let error = engine
        .next_batch(102)
        .await
        .expect_err("hard block-row policy must reject provider overshoot");

    assert!(matches!(
        error,
        SourceEngineError::ResponseLimitExceeded {
            resource: "blocks",
            observed: 2,
            limit: 1,
        }
    ));
}

#[test]
fn hard_response_limits_require_a_nonzero_bound_for_every_resource() {
    for (blocks, logs, bytes, resource) in [
        (0, 1, 1, "blocks"),
        (1, 0, 1, "logs"),
        (1, 1, 0, "dynamic bytes"),
    ] {
        let error = SourceResponseLimits::new(blocks, logs, bytes)
            .expect_err("every hard local bound must be nonzero");
        assert!(matches!(
            error,
            SourceError::InvalidResponseLimit { resource: actual } if actual == resource
        ));
    }
}

#[tokio::test]
async fn source_engine_rejects_excess_dynamic_response_bytes_before_page_validation() {
    let mut malformed = source_log(100, 0x10, 0x20, 0);
    malformed.data = Some(Data::from(vec![0xaa; 512]));
    let source = RecordingSource {
        queries: Arc::new(Mutex::new(Vec::new())),
        page: source_page! {
            archive_height: Some(101),
            next_block: 101,
            blocks: vec![block(100, 0x10, 0x0f)],
            logs: vec![malformed],
            rollback_guard: None,
        },
    };
    let limits = SourceResponseLimits::new(4, 4, 256).expect("valid hard limits");
    let mut engine =
        SourceEngine::new(source, desired_state(), 100, 100, 8).with_response_limits(limits);

    let error = engine
        .next_batch(101)
        .await
        .expect_err("dynamic response bytes must have a hard pre-normalization bound");

    assert!(matches!(
        error,
        SourceEngineError::ResponseLimitExceeded {
            resource: "dynamic bytes",
            observed,
            limit: 256,
        } if observed > 256
    ));
}

#[tokio::test]
async fn source_engine_allows_provider_soft_target_overshoot_within_the_hard_policy() {
    let queries = Arc::new(Mutex::new(Vec::new()));
    let logs = (0_u64..=5_000)
        .map(|index| source_log(100, 0x10, 0x20, index))
        .collect();
    let source = RecordingSource {
        queries: Arc::clone(&queries),
        page: source_page! {
            archive_height: Some(101),
            next_block: 101,
            blocks: vec![block(100, 0x10, 0x0f)],
            logs: logs,
            rollback_guard: None,
        },
    };
    let mut engine = SourceEngine::new(source, desired_state(), 100, 100, 8);

    let delivery = engine
        .next_batch(101)
        .await
        .expect("soft-target overshoot is allowed below the hard local policy")
        .expect("delivery");

    assert!(delivery.encoded_len() <= MAX_DELIVERY_SIZE_BYTES);
    assert_eq!(
        queries.lock().expect("queries lock")[0].max_num_logs,
        Some(5_000),
        "the response deliberately exceeded the provider's documented soft target"
    );
}

#[tokio::test]
async fn source_engine_rejects_incomplete_duplicate_and_disconnected_headers() {
    let incomplete = invalid_page_error(
        source_page! {
            archive_height: Some(102),
            next_block: 102,
            blocks: vec![block(100, 0x10, 0x0f)],
            logs: Vec::new(),
            rollback_guard: None,
        },
        102,
    )
    .await;
    assert!(matches!(
        incomplete,
        SourceEngineError::InvalidPage(SourcePageError::IncompleteBlockRange {
            expected_next_block: 101,
            next_block: 102,
        })
    ));

    let duplicate = invalid_page_error(
        source_page! {
            archive_height: Some(102),
            next_block: 102,
            blocks: vec![block(100, 0x10, 0x0f), block(100, 0x10, 0x0f)],
            logs: Vec::new(),
            rollback_guard: None,
        },
        102,
    )
    .await;
    assert!(matches!(
        duplicate,
        SourceEngineError::InvalidPage(SourcePageError::NonContiguousBlock {
            expected: 101,
            received: 100,
        })
    ));

    let disconnected = invalid_page_error(
        source_page! {
            archive_height: Some(102),
            next_block: 102,
            blocks: vec![block(100, 0x10, 0x0f), block(101, 0x11, 0xff)],
            logs: Vec::new(),
            rollback_guard: None,
        },
        102,
    )
    .await;
    assert!(matches!(
        disconnected,
        SourceEngineError::InvalidPage(SourcePageError::ParentHashMismatch { number: 101 })
    ));
}

#[tokio::test]
async fn source_engine_rejects_logs_without_exact_header_identity() {
    let outside = invalid_page_error(
        source_page! {
            archive_height: Some(101),
            next_block: 101,
            blocks: vec![block(100, 0x10, 0x0f)],
            logs: vec![source_log(101, 0x11, 0x20, 0)],
            rollback_guard: None,
        },
        101,
    )
    .await;
    assert!(matches!(
        outside,
        SourceEngineError::InvalidPage(SourcePageError::LogOutsidePage {
            block_number: 101,
            ..
        })
    ));

    let wrong_hash = invalid_page_error(
        source_page! {
            archive_height: Some(101),
            next_block: 101,
            blocks: vec![block(100, 0x10, 0x0f)],
            logs: vec![source_log(100, 0xff, 0x20, 0)],
            rollback_guard: None,
        },
        101,
    )
    .await;
    assert!(matches!(
        wrong_hash,
        SourceEngineError::InvalidPage(SourcePageError::LogBlockHashMismatch { block_number: 100 })
    ));

    let duplicate_log = source_log(100, 0x10, 0x20, 0);
    let duplicate = invalid_page_error(
        source_page! {
            archive_height: Some(101),
            next_block: 101,
            blocks: vec![block(100, 0x10, 0x0f)],
            logs: vec![duplicate_log.clone(), duplicate_log],
            rollback_guard: None,
        },
        101,
    )
    .await;
    assert!(matches!(
        duplicate,
        SourceEngineError::InvalidPage(SourcePageError::DuplicateLog { log_index: 0 })
    ));
}

#[tokio::test]
async fn source_engine_rejects_removed_logs_from_historical_canonical_pages() {
    let mut removed = source_log(100, 0x10, 0x20, 0);
    removed.removed = Some(true);
    let error = invalid_page_error(
        source_page! {
            archive_height: Some(101),
            next_block: 101,
            blocks: vec![block(100, 0x10, 0x0f)],
            logs: vec![removed],
            rollback_guard: None,
        },
        101,
    )
    .await;
    assert!(matches!(
        error,
        SourceEngineError::InvalidPage(SourcePageError::RemovedLogInCanonicalPage {
            block_number: 100,
        })
    ));
}

#[tokio::test]
async fn source_engine_rejects_incomplete_or_gapped_log_payloads() {
    let mut missing_data = source_log(100, 0x10, 0x20, 0);
    missing_data.data = None;
    let error = invalid_page_error(
        source_page! {
            archive_height: Some(101),
            next_block: 101,
            blocks: vec![block(100, 0x10, 0x0f)],
            logs: vec![missing_data],
            rollback_guard: None,
        },
        101,
    )
    .await;
    assert!(matches!(
        error,
        SourceEngineError::InvalidPage(SourcePageError::MissingField("log.data"))
    ));

    let mut gapped_topics = source_log(100, 0x10, 0x20, 0);
    gapped_topics.topics.push(None);
    gapped_topics
        .topics
        .push(Some(LogArgument::from([0x44; 32])));
    let error = invalid_page_error(
        source_page! {
            archive_height: Some(101),
            next_block: 101,
            blocks: vec![block(100, 0x10, 0x0f)],
            logs: vec![gapped_topics],
            rollback_guard: None,
        },
        101,
    )
    .await;
    assert!(matches!(
        error,
        SourceEngineError::InvalidPage(SourcePageError::NonContiguousLogTopics)
    ));
}

#[tokio::test]
async fn source_engine_requires_a_valid_timestamp_for_every_covered_header() {
    let mut missing_timestamp = block(100, 0x10, 0x0f);
    missing_timestamp.timestamp = None;
    let missing = invalid_page_error(
        source_page! {
            archive_height: Some(101),
            next_block: 101,
            blocks: vec![missing_timestamp],
            logs: Vec::new(),
            rollback_guard: None,
        },
        101,
    )
    .await;
    assert!(matches!(
        missing,
        SourceEngineError::InvalidPage(SourcePageError::MissingField("block.timestamp"))
    ));

    let mut overflowing_timestamp = block(100, 0x10, 0x0f);
    overflowing_timestamp.timestamp = Some(Quantity::from(vec![0x01; 9]));
    let overflow = invalid_page_error(
        source_page! {
            archive_height: Some(101),
            next_block: 101,
            blocks: vec![overflowing_timestamp],
            logs: Vec::new(),
            rollback_guard: None,
        },
        101,
    )
    .await;
    assert!(matches!(
        overflow,
        SourceEngineError::InvalidPage(SourcePageError::QuantityOverflow("block.timestamp"))
    ));
}

#[tokio::test]
async fn source_engine_rejects_rollback_guards_that_do_not_describe_the_returned_page() {
    let cases = [
        (
            guard(100, 0xff, 100, 0x0f),
            SourcePageError::RollbackGuardHashMismatch { number: 100 },
        ),
        (
            guard(99, 0x10, 100, 0x0f),
            SourcePageError::RollbackGuardTipMismatch {
                expected: 100,
                received: 99,
            },
        ),
        (
            guard(100, 0x10, 99, 0x0f),
            SourcePageError::RollbackGuardStartMismatch {
                expected: 100,
                received: 99,
            },
        ),
        (
            guard(100, 0x10, 100, 0xff),
            SourcePageError::RollbackGuardParentMismatch { number: 100 },
        ),
    ];

    for (rollback_guard, expected) in cases {
        let error = invalid_page_error(
            source_page! {
                archive_height: Some(101),
                next_block: 101,
                blocks: vec![block(100, 0x10, 0x0f)],
                logs: Vec::new(),
                rollback_guard: Some(rollback_guard),
            },
            101,
        )
        .await;
        assert!(
            matches!(error, SourceEngineError::InvalidPage(ref actual) if actual == &expected),
            "unexpected malformed-guard error: {error:?}"
        );
    }

    let mut timestamp_guard = guard(100, 0x10, 100, 0x0f);
    timestamp_guard.timestamp = 1_700_000_099;
    let error = invalid_page_error(
        source_page! {
            archive_height: Some(101),
            next_block: 101,
            blocks: vec![block(100, 0x10, 0x0f)],
            logs: Vec::new(),
            rollback_guard: Some(timestamp_guard),
        },
        101,
    )
    .await;
    assert!(matches!(
        error,
        SourceEngineError::InvalidPage(SourcePageError::RollbackGuardTimestampMismatch {
            number: 100,
        })
    ));
}

#[tokio::test]
async fn reorg_sequence_exhaustion_does_not_install_an_undelivered_replacement_branch() {
    let queries = Arc::new(Mutex::new(Vec::new()));
    let source = RestartReorgSource {
        queries: Arc::clone(&queries),
    };
    let resume = source_resume! {
        next_block: 101,
        sequence: u64::MAX - 1,
        activation_block: 100,
        canonical_blocks: vec![
            evm_fork_cache_event_protocol::v1::BlockRef {
                number: 99,
                hash: vec![0x99; 32],
                parent_hash: vec![0x98; 32],
                timestamp: 1_700_000_099,
            },
            evm_fork_cache_event_protocol::v1::BlockRef {
                number: 100,
                hash: vec![0xa0; 32],
                parent_hash: vec![0x99; 32],
                timestamp: 1_700_000_100,
            },
        ],
        provider_checkpoint: Some(guard(100, 0xa0, 100, 0x99)),
    };
    let mut engine = SourceEngine::restore(source, desired_state(), resume, 8)
        .expect("restore nearly exhausted sequence");

    for _ in 0..2 {
        let error = engine
            .next_batch(102)
            .await
            .expect_err("a reorg requires distinct control and replacement sequences");
        assert!(matches!(error, SourceEngineError::SequenceExhausted));
        assert_eq!(engine.committed_next_block(), 101);
        assert_eq!(engine.committed_sequence(), u64::MAX - 1);
    }
    assert_eq!(
        queries
            .lock()
            .expect("queries lock")
            .iter()
            .map(|query| query.from_block)
            .collect::<Vec<_>>(),
        [101, 99, 100, 100, 101, 99, 100, 100],
        "retry must rediscover the reorg from the acknowledged branch"
    );
}

#[test]
fn restore_rejects_a_durable_cursor_that_is_not_the_canonical_tip_successor() {
    let source = RecordingSource {
        queries: Arc::new(Mutex::new(Vec::new())),
        page: page(),
    };
    let error = SourceEngine::restore(
        source,
        desired_state(),
        source_resume! {
            next_block: 102,
            sequence: 1,
            activation_block: 100,
            canonical_blocks: vec![evm_fork_cache_event_protocol::v1::BlockRef {
                number: 100,
                hash: vec![0x10; 32],
                parent_hash: vec![0x0f; 32],
                timestamp: 1_700_000_100,
            }],
            provider_checkpoint: Some(guard(100, 0x10, 100, 0x0f)),
        },
        8,
    );
    let error = match error {
        Ok(_) => panic!("a durable cursor gap must not be silently restored"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        SourceEngineError::ResumeCursorMismatch {
            expected: 101,
            received: 102,
        }
    ));
}

#[test]
fn restore_rejects_a_provider_guard_that_conflicts_with_durable_canonical_history() {
    let source = RecordingSource {
        queries: Arc::new(Mutex::new(Vec::new())),
        page: page(),
    };
    let error = SourceEngine::restore(
        source,
        desired_state(),
        source_resume! {
            next_block: 101,
            sequence: 1,
            activation_block: 100,
            canonical_blocks: vec![evm_fork_cache_event_protocol::v1::BlockRef {
                number: 100,
                hash: vec![0x10; 32],
                parent_hash: vec![0x0f; 32],
                timestamp: 1_700_000_100,
            }],
            provider_checkpoint: Some(guard(100, 0xff, 100, 0x0f)),
        },
        8,
    );
    let error = match error {
        Ok(_) => panic!("a conflicting provider guard must not control rollback detection"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        SourceEngineError::ResumeGuardMismatch("hash")
    ));
}

#[test]
fn restore_rejects_a_negative_guard_timestamp_without_canonical_history() {
    let source = RecordingSource {
        queries: Arc::new(Mutex::new(Vec::new())),
        page: page(),
    };
    let mut checkpoint = guard(100, 0x10, 100, 0x0f);
    checkpoint.timestamp = -1;
    let error = SourceEngine::restore(
        source,
        desired_state(),
        source_resume! {
            next_block: 101,
            sequence: 1,
            activation_block: 100,
            canonical_blocks: Vec::new(),
            provider_checkpoint: Some(checkpoint),
        },
        8,
    );
    let error = match error {
        Ok(_) => panic!("a negative provider timestamp must not be restored"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        SourceEngineError::ResumeGuardMismatch("timestamp")
    ));
}

#[test]
fn restore_rejects_a_guard_whose_range_starts_after_its_tip() {
    let source = RecordingSource {
        queries: Arc::new(Mutex::new(Vec::new())),
        page: page(),
    };
    let checkpoint = guard(100, 0x10, 101, 0x0f);
    let error = SourceEngine::restore(
        source,
        desired_state(),
        source_resume! {
            next_block: 101,
            sequence: 1,
            activation_block: 100,
            canonical_blocks: Vec::new(),
            provider_checkpoint: Some(checkpoint),
        },
        8,
    );
    let error = match error {
        Ok(_) => panic!("an inverted provider guard must not be restored"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        SourceEngineError::ResumeGuardMismatch("first_block_number")
    ));
}
