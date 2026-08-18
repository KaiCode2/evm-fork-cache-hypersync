use evm_fork_cache_event_protocol::{
    PROTOCOL_VERSION,
    v1::{
        ApplyDesiredState, LogInterest, OwnerInterests, PortableInterest, TopicValues,
        portable_interest,
    },
};
use evm_fork_cache_hypersync::{
    MAX_BLOCKS_PER_QUERY, MAX_COMPILED_LOG_FILTERS, MAX_LOGS_PER_QUERY, QueryPlanError,
    compile_query,
};
use hypersync_client::net_types::{BlockField, LogField};

#[test]
fn query_plan_compiles_owner_log_filters_and_canonical_block_fields() {
    let address = vec![0x11; 20];
    let topic0 = vec![0x22; 32];
    let desired_state = ApplyDesiredState {
        protocol_version: PROTOCOL_VERSION,
        session_id: "runtime-a".into(),
        chain_id: 1,
        expected_revision: 0,
        new_revision: 1,
        owners: vec![OwnerInterests {
            owner_id: "pool-a".into(),
            interests: vec![PortableInterest {
                kind: Some(portable_interest::Kind::Log(LogInterest {
                    addresses: vec![address.clone()],
                    topics: vec![TopicValues {
                        values: vec![topic0.clone()],
                    }],
                })),
            }],
            backfill: None,
            canonical: false,
        }],
    };

    let query = compile_query(&desired_state, 100, Some(110)).expect("compile query");

    assert_eq!(query.from_block, 100);
    assert_eq!(query.to_block, Some(110));
    assert_eq!(query.logs.len(), 1);
    assert_eq!(query.max_num_blocks, Some(MAX_BLOCKS_PER_QUERY));
    assert_eq!(query.max_num_logs, Some(MAX_LOGS_PER_QUERY));
    assert_eq!(query.logs[0].include.address[0].as_ref(), address);
    assert_eq!(query.logs[0].include.topics[0][0].as_ref(), topic0);
    assert!(query.include_all_blocks, "headers drive canonical progress");
    for field in [
        BlockField::Number,
        BlockField::Hash,
        BlockField::ParentHash,
        BlockField::Timestamp,
    ] {
        assert!(query.field_selection.block.contains(&field));
    }
    for field in [
        LogField::Address,
        LogField::Topic0,
        LogField::Topic1,
        LogField::Topic2,
        LogField::Topic3,
        LogField::Data,
        LogField::BlockNumber,
        LogField::BlockHash,
        LogField::TransactionHash,
        LogField::TransactionIndex,
        LogField::LogIndex,
        LogField::Removed,
    ] {
        assert!(query.field_selection.log.contains(&field));
    }
}

#[test]
fn query_plan_canonicalizes_and_deduplicates_equivalent_log_filters() {
    let interest = PortableInterest {
        kind: Some(portable_interest::Kind::Log(LogInterest {
            addresses: vec![vec![0x22; 20], vec![0x11; 20], vec![0x22; 20]],
            topics: vec![TopicValues {
                values: vec![vec![0x44; 32], vec![0x33; 32], vec![0x44; 32]],
            }],
        })),
    };
    let desired_state = ApplyDesiredState {
        protocol_version: PROTOCOL_VERSION,
        session_id: "deduplicated-filters".into(),
        chain_id: 1,
        expected_revision: 0,
        new_revision: 1,
        owners: vec![
            OwnerInterests {
                owner_id: "a".into(),
                interests: vec![interest.clone()],
                backfill: None,
                canonical: false,
            },
            OwnerInterests {
                owner_id: "b".into(),
                interests: vec![interest],
                backfill: None,
                canonical: false,
            },
        ],
    };

    let query = compile_query(&desired_state, 10, Some(11)).expect("compile query");

    assert_eq!(query.logs.len(), 1);
    assert_eq!(query.logs[0].include.address[0].as_ref(), &[0x11; 20]);
    assert_eq!(query.logs[0].include.address[1].as_ref(), &[0x22; 20]);
    assert_eq!(query.logs[0].include.topics[0][0].as_ref(), &[0x33; 32]);
    assert_eq!(query.logs[0].include.topics[0][1].as_ref(), &[0x44; 32]);
}

#[test]
fn query_plan_rejects_provider_filter_amplification() {
    let interests = (0..=MAX_COMPILED_LOG_FILTERS)
        .map(|index| {
            let mut address = vec![0_u8; 20];
            address[12..].copy_from_slice(&(index as u64).to_be_bytes());
            PortableInterest {
                kind: Some(portable_interest::Kind::Log(LogInterest {
                    addresses: vec![address],
                    topics: Vec::new(),
                })),
            }
        })
        .collect();
    let desired_state = ApplyDesiredState {
        protocol_version: PROTOCOL_VERSION,
        session_id: "filter-amplification".into(),
        chain_id: 1,
        expected_revision: 0,
        new_revision: 1,
        owners: vec![OwnerInterests {
            owner_id: "many-filters".into(),
            interests,
            backfill: None,
            canonical: false,
        }],
    };

    let error = compile_query(&desired_state, 10, Some(11))
        .expect_err("provider filter amplification must fail before a query is allocated");

    assert_eq!(
        error,
        QueryPlanError::TooManyCompiledLogFilters {
            limit: MAX_COMPILED_LOG_FILTERS,
        }
    );
}

#[test]
fn query_plan_rejects_an_empty_or_reversed_bounded_range() {
    let desired_state = ApplyDesiredState {
        protocol_version: PROTOCOL_VERSION,
        session_id: "invalid-range".into(),
        chain_id: 1,
        expected_revision: 0,
        new_revision: 1,
        owners: Vec::new(),
    };

    for to_block_excl in [99, 100] {
        let error = compile_query(&desired_state, 100, Some(to_block_excl))
            .expect_err("a bounded query must make forward progress");
        assert_eq!(
            error,
            QueryPlanError::InvalidRange {
                from_block: 100,
                to_block_excl,
            }
        );
    }
}

#[test]
fn match_all_filter_subsumes_other_filters_without_order_dependent_amplification() {
    let mut interests: Vec<_> = (0..=MAX_COMPILED_LOG_FILTERS)
        .map(|index| {
            let mut address = vec![0_u8; 20];
            address[12..].copy_from_slice(&(index as u64).to_be_bytes());
            PortableInterest {
                kind: Some(portable_interest::Kind::Log(LogInterest {
                    addresses: vec![address],
                    topics: Vec::new(),
                })),
            }
        })
        .collect();
    interests.push(PortableInterest {
        kind: Some(portable_interest::Kind::Log(LogInterest {
            addresses: Vec::new(),
            topics: Vec::new(),
        })),
    });
    let desired_state = ApplyDesiredState {
        protocol_version: PROTOCOL_VERSION,
        session_id: "match-all".into(),
        chain_id: 1,
        expected_revision: 0,
        new_revision: 1,
        owners: vec![OwnerInterests {
            owner_id: "all-logs".into(),
            interests,
            backfill: None,
            canonical: false,
        }],
    };

    let query = compile_query(&desired_state, 100, Some(101))
        .expect("a match-all filter collapses every narrower filter");

    assert_eq!(query.logs.len(), 1);
    assert!(query.logs[0].include.address.is_empty());
    assert!(query.logs[0].include.topics.iter().all(Vec::is_empty));
}
