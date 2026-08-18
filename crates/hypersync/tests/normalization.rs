use alloy_consensus::Header as ConsensusHeader;
use alloy_primitives::{Address as AlloyAddress, B64, B256, Bloom, Bytes, U256};
use alloy_rlp::Decodable;
use evm_fork_cache_event_protocol::{
    PROTOCOL_VERSION,
    v1::{
        ApplyDesiredState, BlockInterest, BlockMode, DeliveryScope, EventRecord, LogInterest,
        OwnerInterests, PortableInterest, TopicValues, chain_event, delivery, portable_interest,
    },
};
use evm_fork_cache_hypersync::{NormalizeError, SourcePage, normalize_page_unchecked};
use hypersync_client::{
    format::{Address, BloomFilter, Data, Hash, LogArgument, Nonce, Quantity},
    net_types::RollbackGuard,
    simple_types::{Block, Log},
};

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

fn event_kinds(events: &[EventRecord]) -> Vec<&'static str> {
    events
        .iter()
        .map(
            |record| match record.event.as_ref().and_then(|event| event.event.as_ref()) {
                Some(chain_event::Event::BlockHeader(_)) => "block",
                Some(chain_event::Event::BlockProgress(_)) => "progress",
                Some(chain_event::Event::Log(_)) => "log",
                None => "missing",
            },
        )
        .collect()
}

fn complete_block(number: u64, parent_hash: [u8; 32]) -> (Block, ConsensusHeader) {
    let consensus = ConsensusHeader {
        parent_hash: B256::from(parent_hash),
        ommers_hash: B256::from([0x02; 32]),
        beneficiary: AlloyAddress::from([0x03; 20]),
        state_root: B256::from([0x04; 32]),
        transactions_root: B256::from([0x05; 32]),
        receipts_root: B256::from([0x06; 32]),
        logs_bloom: Bloom::from([0x07; 256]),
        difficulty: U256::from(8_u64),
        number,
        gas_limit: 30_000_000,
        gas_used: 15_000_000,
        timestamp: 1_700_000_000 + number,
        extra_data: Bytes::from_static(b"evm-fork-cache"),
        mix_hash: B256::from([0x08; 32]),
        nonce: B64::from([0x09; 8]),
        base_fee_per_gas: Some(1_000_000_000),
        withdrawals_root: Some(B256::from([0x0a; 32])),
        blob_gas_used: Some(131_072),
        excess_blob_gas: Some(262_144),
        parent_beacon_block_root: Some(B256::from([0x0b; 32])),
        requests_hash: None,
    };
    let hash: [u8; 32] = consensus.hash_slow().into();
    let block = Block {
        number: Some(number),
        hash: Some(Hash::from(hash)),
        parent_hash: Some(Hash::from(parent_hash)),
        nonce: Some(Nonce::from([0x09; 8])),
        sha3_uncles: Some(Hash::from([0x02; 32])),
        logs_bloom: Some(BloomFilter::from([0x07; 256])),
        transactions_root: Some(Hash::from([0x05; 32])),
        state_root: Some(Hash::from([0x04; 32])),
        receipts_root: Some(Hash::from([0x06; 32])),
        miner: Some(Address::from([0x03; 20])),
        difficulty: Some(Quantity::from(8_u64)),
        total_difficulty: Some(Quantity::from(16_u64)),
        extra_data: Some(Data::from(b"evm-fork-cache".to_vec())),
        size: Some(Quantity::from(1_024_u64)),
        gas_limit: Some(Quantity::from(30_000_000_u64)),
        gas_used: Some(Quantity::from(15_000_000_u64)),
        timestamp: Some(Quantity::from(1_700_000_000 + number)),
        base_fee_per_gas: Some(Quantity::from(1_000_000_000_u64)),
        blob_gas_used: Some(Quantity::from(131_072_u64)),
        excess_blob_gas: Some(Quantity::from(262_144_u64)),
        parent_beacon_block_root: Some(Hash::from([0x0b; 32])),
        withdrawals_root: Some(Hash::from([0x0a; 32])),
        mix_hash: Some(Hash::from([0x08; 32])),
        ..Default::default()
    };
    (block, consensus)
}

#[test]
fn response_normalization_orders_block_before_logs_and_builds_durable_cursor() {
    let parent_hash = Hash::from([0x0f; 32]);
    let (block, consensus) = complete_block(100, [0x0f; 32]);
    let block_hash = block.hash.clone().expect("complete block hash");
    let tx_hash = Hash::from([0x20; 32]);
    let mut log = Log {
        removed: Some(false),
        log_index: Some(0_u64.into()),
        transaction_index: Some(1_u64.into()),
        transaction_hash: Some(tx_hash),
        block_hash: Some(block_hash.clone()),
        block_number: Some(100_u64.into()),
        address: Some(Address::from([0x33; 20])),
        data: Some(Data::from(vec![0xaa, 0xbb])),
        ..Default::default()
    };
    log.topics.push(Some(LogArgument::from([0x44; 32])));
    let page = source_page! {
        archive_height: Some(101),
        next_block: 101,
        blocks: vec![block],
        logs: vec![log],
        rollback_guard: Some(RollbackGuard {
            block_number: 100,
            timestamp: 1_700_000_100,
            hash: block_hash.clone(),
            first_block_number: 100,
            first_parent_hash: parent_hash,
        }),
    };

    let desired = ApplyDesiredState {
        protocol_version: PROTOCOL_VERSION,
        session_id: "runtime-a".into(),
        chain_id: 1,
        expected_revision: 6,
        new_revision: 7,
        owners: vec![
            OwnerInterests {
                owner_id: "headers".into(),
                interests: vec![PortableInterest {
                    kind: Some(portable_interest::Kind::Block(BlockInterest {
                        mode: BlockMode::Header.into(),
                    })),
                }],
                backfill: None,
                canonical: false,
            },
            OwnerInterests {
                owner_id: "pool-a".into(),
                interests: vec![PortableInterest {
                    kind: Some(portable_interest::Kind::Log(LogInterest {
                        addresses: vec![vec![0x33; 20]],
                        topics: vec![TopicValues {
                            values: vec![vec![0x44; 32]],
                        }],
                    })),
                }],
                backfill: None,
                canonical: false,
            },
        ],
    };
    let batch =
        normalize_page_unchecked(&desired, 9, page).expect("complete response should normalize");

    assert_eq!(batch.sequence, 9);
    assert_eq!(batch.delivery_token, 9_u64.to_be_bytes());
    let data = match batch.payload.as_ref().expect("payload") {
        delivery::Payload::Data(data) => data,
        other => panic!("expected data, got {other:?}"),
    };
    assert_eq!(event_kinds(&data.records), vec!["block", "log", "progress"]);
    assert_eq!(data.records[0].owner_ids, ["headers"]);
    assert_eq!(data.records[1].owner_ids, ["pool-a"]);
    assert!(data.records[2].canonical_audience);
    assert!(data.records[2].owner_ids.is_empty());
    assert_eq!(
        DeliveryScope::try_from(data.records[0].scope).expect("header scope"),
        DeliveryScope::CanonicalProgress
    );
    assert_eq!(
        DeliveryScope::try_from(data.records[2].scope).expect("progress scope"),
        DeliveryScope::CanonicalProgress
    );
    let header = match data.records[0]
        .event
        .as_ref()
        .and_then(|event| event.event.as_ref())
        .expect("header event")
    {
        chain_event::Event::BlockHeader(header) => header,
        other => panic!("expected full header, got {other:?}"),
    };
    let mut encoded = header.consensus_header_rlp.as_slice();
    let decoded = ConsensusHeader::decode(&mut encoded).expect("decode consensus header RLP");
    assert!(encoded.is_empty());
    assert_eq!(decoded, consensus);
    let cursor = batch.cursor.expect("cursor");
    assert_eq!(cursor.chain_id, 1);
    assert_eq!(cursor.query_revision, 7);
    assert_eq!(cursor.next_block, 101);
    assert_eq!(cursor.canonical_head.as_ref().expect("head").number, 100);
    assert_eq!(
        cursor.canonical_head.as_ref().expect("head").hash,
        block_hash.as_ref()
    );
    assert_eq!(cursor.provider_checkpoint.len(), 88);
}

#[test]
fn implicit_canonical_rows_use_compact_progress_without_fabricating_a_header() {
    let (block, _) = complete_block(100, [0x0f; 32]);
    let block_hash = block.hash.clone().expect("block hash");
    let log = Log {
        removed: Some(false),
        log_index: Some(0_u64.into()),
        transaction_index: Some(0_u64.into()),
        transaction_hash: Some(Hash::from([0x20; 32])),
        block_hash: Some(block_hash.clone()),
        block_number: Some(100_u64.into()),
        address: Some(Address::from([0x33; 20])),
        data: Some(Data::from(vec![0xaa])),
        ..Default::default()
    };
    let desired = ApplyDesiredState {
        protocol_version: PROTOCOL_VERSION,
        session_id: "compact-progress".into(),
        chain_id: 1,
        expected_revision: 0,
        new_revision: 1,
        owners: vec![OwnerInterests {
            owner_id: "logs".into(),
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
    let page = source_page! {
        archive_height: Some(101),
        next_block: 101,
        blocks: vec![block],
        logs: vec![log],
        rollback_guard: Some(RollbackGuard {
            block_number: 100,
            timestamp: 1_700_000_100,
            hash: block_hash,
            first_block_number: 100,
            first_parent_hash: Hash::from([0x0f; 32]),
        }),
    };

    let delivery = normalize_page_unchecked(&desired, 1, page).expect("normalize compact progress");
    let data = match delivery.payload.expect("payload") {
        delivery::Payload::Data(data) => data,
        other => panic!("expected data, got {other:?}"),
    };

    assert_eq!(event_kinds(&data.records), ["log", "progress"]);
    assert_eq!(data.records[0].owner_ids, ["logs"]);
    assert_eq!(
        DeliveryScope::try_from(data.records[1].scope).expect("progress scope"),
        DeliveryScope::CanonicalProgress
    );
    assert!(data.records[1].canonical_audience);
    assert!(data.records[1].owner_ids.is_empty());
}

#[test]
fn compact_canonical_progress_certifies_each_archived_block_in_order() {
    let (block_100, _) = complete_block(100, [0x0f; 32]);
    let hash_100 = block_100.hash.clone().expect("block 100 hash");
    let (block_101, _) = complete_block(101, hash_100.as_ref().try_into().expect("hash width"));
    let hash_101 = block_101.hash.clone().expect("block 101 hash");
    let log = |number: u64, block_hash: Hash, transaction_hash: u8| Log {
        removed: Some(false),
        log_index: Some(0_u64.into()),
        transaction_index: Some(0_u64.into()),
        transaction_hash: Some(Hash::from([transaction_hash; 32])),
        block_hash: Some(block_hash),
        block_number: Some(number.into()),
        address: Some(Address::from([0x33; 20])),
        data: Some(Data::from(vec![0xaa])),
        ..Default::default()
    };
    let desired = ApplyDesiredState {
        protocol_version: PROTOCOL_VERSION,
        session_id: "multi-block-compact-progress".into(),
        chain_id: 1,
        expected_revision: 0,
        new_revision: 1,
        owners: vec![OwnerInterests {
            owner_id: "logs".into(),
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
    let page = source_page! {
        archive_height: Some(102),
        next_block: 102,
        blocks: vec![block_100, block_101],
        logs: vec![
            log(100, hash_100.clone(), 0x20),
            log(101, hash_101.clone(), 0x21),
        ],
        rollback_guard: Some(RollbackGuard {
            block_number: 101,
            timestamp: 1_700_000_101,
            hash: hash_101,
            first_block_number: 100,
            first_parent_hash: Hash::from([0x0f; 32]),
        }),
    };

    let delivery =
        normalize_page_unchecked(&desired, 1, page).expect("normalize compact block sequence");
    let data = match delivery.payload.expect("payload") {
        delivery::Payload::Data(data) => data,
        other => panic!("expected data, got {other:?}"),
    };

    assert_eq!(
        event_kinds(&data.records),
        ["log", "progress", "log", "progress"]
    );
    let progress = data
        .records
        .iter()
        .filter_map(|record| {
            let chain_event::Event::BlockProgress(progress) =
                record.event.as_ref()?.event.as_ref()?
            else {
                return None;
            };
            progress.block.as_ref()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        progress
            .iter()
            .map(|block| block.number)
            .collect::<Vec<_>>(),
        [100, 101]
    );
    assert_eq!(progress[1].parent_hash, progress[0].hash);
}

#[test]
fn explicit_header_normalization_fails_closed_when_the_source_omits_a_fork_field() {
    let (mut block, mut consensus) = complete_block(100, [0x0f; 32]);
    consensus.requests_hash = Some(B256::from([0x0c; 32]));
    block.hash = Some(Hash::from(<[u8; 32]>::from(consensus.hash_slow())));
    let desired = ApplyDesiredState {
        protocol_version: PROTOCOL_VERSION,
        session_id: "post-prague-header".into(),
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

    let error = normalize_page_unchecked(
        &desired,
        1,
        source_page! {
            archive_height: Some(101),
            next_block: 101,
            blocks: vec![block],
            logs: Vec::new(),
            rollback_guard: None,
        },
    )
    .expect_err("an unrepresented requests_hash must never produce fabricated header RLP");

    assert!(matches!(error, NormalizeError::HeaderHashMismatch { .. }));
}

#[test]
fn normalization_does_not_invent_a_zero_timestamp_for_a_log_without_its_block() {
    let desired = ApplyDesiredState {
        protocol_version: PROTOCOL_VERSION,
        session_id: "orphan-log".into(),
        chain_id: 1,
        expected_revision: 0,
        new_revision: 1,
        owners: vec![OwnerInterests {
            owner_id: "logs".into(),
            interests: vec![PortableInterest {
                kind: Some(portable_interest::Kind::Log(LogInterest {
                    addresses: Vec::new(),
                    topics: Vec::new(),
                })),
            }],
            backfill: None,
            canonical: false,
        }],
    };
    let log = Log {
        removed: Some(false),
        log_index: Some(0_u64.into()),
        transaction_index: Some(0_u64.into()),
        transaction_hash: Some(Hash::from([0x20; 32])),
        block_hash: Some(Hash::from([0x10; 32])),
        block_number: Some(100_u64.into()),
        address: Some(Address::from([0x33; 20])),
        data: Some(Data::from(vec![0xaa])),
        ..Default::default()
    };

    let error = normalize_page_unchecked(
        &desired,
        1,
        source_page! {
            archive_height: Some(101),
            next_block: 101,
            blocks: Vec::new(),
            logs: vec![log],
            rollback_guard: None,
        },
    )
    .expect_err("every normalized log needs canonical block timing");

    assert_eq!(error, NormalizeError::LogWithoutBlock { block_number: 100 });
}

#[test]
fn normalization_does_not_invent_a_removed_status() {
    let (block, _) = complete_block(100, [0x0f; 32]);
    let block_hash = block.hash.clone().expect("block hash");
    let desired = ApplyDesiredState {
        protocol_version: PROTOCOL_VERSION,
        session_id: "missing-removed".into(),
        chain_id: 1,
        expected_revision: 0,
        new_revision: 1,
        owners: Vec::new(),
    };
    let log = Log {
        removed: None,
        log_index: Some(0_u64.into()),
        transaction_index: Some(0_u64.into()),
        transaction_hash: Some(Hash::from([0x20; 32])),
        block_hash: Some(block_hash),
        block_number: Some(100_u64.into()),
        address: Some(Address::from([0x33; 20])),
        data: Some(Data::from(vec![0xaa])),
        ..Default::default()
    };

    let error = normalize_page_unchecked(
        &desired,
        1,
        source_page! {
            archive_height: Some(101),
            next_block: 101,
            blocks: vec![block],
            logs: vec![log],
            rollback_guard: None,
        },
    )
    .expect_err("missing provider removal status must fail closed");

    assert_eq!(error, NormalizeError::MissingField("log.removed"));
}

#[test]
fn canonical_audience_subsumes_overlapping_named_owners() {
    let (block, _) = complete_block(100, [0x0f; 32]);
    let block_hash = block.hash.clone().expect("block hash");
    let log = Log {
        removed: Some(false),
        log_index: Some(0_u64.into()),
        transaction_index: Some(0_u64.into()),
        transaction_hash: Some(Hash::from([0x20; 32])),
        block_hash: Some(block_hash),
        block_number: Some(100_u64.into()),
        address: Some(Address::from([0x33; 20])),
        data: Some(Data::from(vec![0xaa])),
        ..Default::default()
    };
    let interest = PortableInterest {
        kind: Some(portable_interest::Kind::Log(LogInterest {
            addresses: vec![vec![0x33; 20]],
            topics: Vec::new(),
        })),
    };
    let desired = ApplyDesiredState {
        protocol_version: PROTOCOL_VERSION,
        session_id: "canonical-overlap".into(),
        chain_id: 1,
        expected_revision: 0,
        new_revision: 1,
        owners: vec![
            OwnerInterests {
                owner_id: "canonical".into(),
                interests: vec![interest.clone()],
                backfill: None,
                canonical: true,
            },
            OwnerInterests {
                owner_id: "named".into(),
                interests: vec![interest],
                backfill: None,
                canonical: false,
            },
        ],
    };

    let delivery = normalize_page_unchecked(
        &desired,
        1,
        source_page! {
            archive_height: Some(101),
            next_block: 101,
            blocks: vec![block],
            logs: vec![log],
            rollback_guard: None,
        },
    )
    .expect("normalize canonical overlap");
    let data = match delivery.payload.expect("payload") {
        delivery::Payload::Data(data) => data,
        other => panic!("expected data, got {other:?}"),
    };
    let log = data
        .records
        .iter()
        .find(|record| {
            matches!(
                record.event.as_ref().and_then(|event| event.event.as_ref()),
                Some(chain_event::Event::Log(_))
            )
        })
        .expect("log record");
    assert!(log.canonical_audience);
    assert!(log.owner_ids.is_empty());
}
