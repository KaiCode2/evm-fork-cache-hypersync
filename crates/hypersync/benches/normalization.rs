use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use evm_fork_cache_event_protocol::{
    PROTOCOL_VERSION,
    v1::{ApplyDesiredState, LogInterest, OwnerInterests, PortableInterest, portable_interest},
};
use evm_fork_cache_hypersync::{SourcePage, normalize_page_unchecked};
use hypersync_client::{
    format::{Address, Data, Hash, LogArgument, Quantity},
    net_types::RollbackGuard,
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

fn synthetic_page() -> SourcePage {
    let mut blocks = Vec::with_capacity(100);
    let mut logs = Vec::with_capacity(1_000);
    for number in 1_000_u64..1_100 {
        let hash = Hash::from([(number & 0xff) as u8; 32]);
        blocks.push(Block {
            number: Some(number),
            hash: Some(hash.clone()),
            parent_hash: Some(Hash::from([(number.saturating_sub(1) & 0xff) as u8; 32])),
            timestamp: Some(Quantity::from(1_700_000_000_u64 + number)),
            ..Default::default()
        });
        for log_index in 0_u64..10 {
            let mut log = Log {
                removed: Some(false),
                log_index: Some(log_index.into()),
                transaction_index: Some(log_index.into()),
                transaction_hash: Some(Hash::from([log_index as u8; 32])),
                block_hash: Some(hash.clone()),
                block_number: Some(number.into()),
                address: Some(Address::from([0x33; 20])),
                data: Some(Data::from(vec![0xaa; 64])),
                ..Default::default()
            };
            log.topics.push(Some(LogArgument::from([0x44; 32])));
            logs.push(log);
        }
    }
    source_page! {
        archive_height: Some(1_100),
        next_block: 1_100,
        blocks: blocks,
        logs: logs,
        rollback_guard: Some(RollbackGuard {
            block_number: 1_099,
            timestamp: 1_700_001_099,
            hash: Hash::from([(1_099 & 0xff) as u8; 32]),
            first_block_number: 1_000,
            first_parent_hash: Hash::from([(999 & 0xff) as u8; 32]),
        }),
    }
}

fn normalization(c: &mut Criterion) {
    let page = synthetic_page();
    let desired_state = ApplyDesiredState {
        protocol_version: PROTOCOL_VERSION,
        session_id: "benchmark".into(),
        chain_id: 1,
        expected_revision: 0,
        new_revision: 1,
        owners: vec![OwnerInterests {
            owner_id: "benchmark-owner".into(),
            interests: vec![PortableInterest {
                kind: Some(portable_interest::Kind::Log(LogInterest {
                    addresses: vec![vec![0x33; 20]],
                    topics: Vec::new(),
                })),
            }],
            backfill: None,
            canonical: false,
        }],
    };
    c.bench_function("normalize_progress_100_blocks_1000_logs", |bencher| {
        bencher.iter_batched(
            || page.clone(),
            |page| {
                std::hint::black_box(
                    normalize_page_unchecked(&desired_state, 1, page)
                        .expect("synthetic page is complete"),
                );
            },
            BatchSize::SmallInput,
        );
    });
    c.bench_function(
        "normalize_and_protobuf_encode_progress_100_blocks_1000_logs",
        |bencher| {
            bencher.iter_batched(
                || page.clone(),
                |page| {
                    let delivery = normalize_page_unchecked(&desired_state, 1, page)
                        .expect("synthetic page is complete");
                    std::hint::black_box(delivery.encode_to_vec());
                },
                BatchSize::SmallInput,
            );
        },
    );
}

criterion_group!(benches, normalization);
criterion_main!(benches);
