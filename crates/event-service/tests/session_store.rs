use evm_fork_cache_event_protocol::{
    MAX_MESSAGE_SIZE_BYTES, PROTOCOL_VERSION,
    v1::{
        Acknowledge, ApplyDesiredState, Backfill, Barrier, BlockHeaderEvent, BlockProgressEvent,
        BlockRef, ChainEvent, Cursor, DataPayload, Delivery, DeliveryScope, EventRecord, LogEvent,
        LogInterest, OwnerInterests, PortableInterest, Reorg, chain_event, delivery,
        portable_interest,
    },
};
use evm_fork_cache_event_service::{SessionStore, SessionStoreError};
use prost::Message;
use rusqlite::{Connection, params};

use alloy_consensus::Header as ConsensusHeader;

fn desired_state(expected_revision: u64, new_revision: u64) -> ApplyDesiredState {
    ApplyDesiredState {
        protocol_version: PROTOCOL_VERSION,
        session_id: "runtime-a".into(),
        chain_id: 1,
        expected_revision,
        new_revision,
        owners: Vec::new(),
    }
}

fn data_delivery() -> Delivery {
    Delivery {
        session_id: "runtime-a".into(),
        sequence: 2,
        query_revision: 1,
        delivery_token: 2_u64.to_be_bytes().to_vec(),
        cursor: Some(Cursor {
            chain_id: 1,
            query_revision: 1,
            next_block: 101,
            canonical_head: Some(BlockRef {
                number: 100,
                hash: vec![0x10; 32],
                parent_hash: vec![0x0f; 32],
                timestamp: 100,
            }),
            batch_sequence: 2,
            provider_checkpoint: b"opaque".to_vec(),
            owner_backfill_activation_block: None,
        }),
        payload: Some(delivery::Payload::Data(DataPayload {
            records: vec![EventRecord {
                event: Some(ChainEvent {
                    event: Some(chain_event::Event::BlockProgress(BlockProgressEvent {
                        block: Some(BlockRef {
                            number: 100,
                            hash: vec![0x10; 32],
                            parent_hash: vec![0x0f; 32],
                            timestamp: 100,
                        }),
                    })),
                }),
                canonical_audience: true,
                owner_ids: Vec::new(),
                scope: DeliveryScope::CanonicalProgress.into(),
            }],
        })),
        checkpoint_neutral: false,
    }
}

#[test]
fn sqlite_store_accepts_first_global_activation_at_a_retained_baseline() {
    let mut store = SessionStore::open_in_memory().expect("in-memory store");
    let baseline = BlockRef {
        number: 100,
        hash: vec![0x10; 32],
        parent_hash: vec![0x0f; 32],
        timestamp: 100,
    };
    let request = ApplyDesiredState {
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
                retained_baseline: Some(baseline.clone()),
            }),
            canonical: true,
        }],
    };
    let prepared = Cursor {
        chain_id: 1,
        query_revision: 1,
        next_block: baseline.number + 1,
        canonical_head: Some(baseline.clone()),
        batch_sequence: 0,
        provider_checkpoint: b"verified retained baseline".to_vec(),
        owner_backfill_activation_block: Some(120),
    };

    store
        .apply_desired_state_with_cursor(request, Some(&prepared))
        .expect("first global activation may begin at its verified retained baseline");

    let activation = store
        .load("global-restore", 1)
        .expect("load global activation")
        .pending_delivery
        .expect("global activation barrier");
    assert_eq!(
        activation
            .cursor
            .as_ref()
            .and_then(|cursor| cursor.canonical_head.as_ref()),
        Some(&baseline)
    );
    let delivery::Payload::Barrier(barrier) =
        activation.payload.as_ref().expect("activation payload")
    else {
        panic!("activation must be a barrier")
    };
    assert!(
        barrier.block.is_none(),
        "the desired state and prepared cursor already carry the retained baseline proof"
    );
}

fn canonical_header_and_progress_records(
    number: u64,
    parent_hash: [u8; 32],
    timestamp: u64,
) -> (BlockRef, Vec<EventRecord>) {
    let header = ConsensusHeader {
        parent_hash: parent_hash.into(),
        number,
        timestamp,
        ..Default::default()
    };
    let block = BlockRef {
        number,
        hash: header.hash_slow().to_vec(),
        parent_hash: parent_hash.to_vec(),
        timestamp,
    };
    let record = |event| EventRecord {
        event: Some(ChainEvent { event: Some(event) }),
        canonical_audience: true,
        owner_ids: Vec::new(),
        scope: DeliveryScope::CanonicalProgress.into(),
    };
    (
        block.clone(),
        vec![
            record(chain_event::Event::BlockHeader(BlockHeaderEvent {
                block: Some(block.clone()),
                consensus_header_rlp: alloy_rlp::encode(&header),
                total_difficulty: Vec::new(),
                size: Vec::new(),
            })),
            record(chain_event::Event::BlockProgress(BlockProgressEvent {
                block: Some(block),
            })),
        ],
    )
}

#[test]
fn sqlite_store_accepts_one_header_and_final_progress_certificate_at_the_same_height() {
    let mut store = SessionStore::open_in_memory().expect("in-memory store");
    let active = desired_state(0, 1);
    store
        .apply_desired_state(active.clone())
        .expect("active desired state");
    let activation = store
        .load("runtime-a", 1)
        .expect("load activation")
        .pending_delivery
        .expect("activation");
    store
        .acknowledge(
            1,
            &Acknowledge {
                session_id: "runtime-a".into(),
                sequence: activation.sequence,
                delivery_token: activation.delivery_token,
            },
        )
        .expect("ack activation");

    let (block, records) = canonical_header_and_progress_records(100, [0x0f; 32], 100);
    let mut certified = data_delivery();
    certified.cursor.as_mut().expect("cursor").canonical_head = Some(block);
    certified.payload = Some(delivery::Payload::Data(DataPayload {
        records: records.clone(),
    }));

    let mut duplicate_header = certified.clone();
    let delivery::Payload::Data(payload) = duplicate_header.payload.as_mut().expect("data payload")
    else {
        panic!("fixture must contain data")
    };
    payload.records.insert(1, records[0].clone());
    assert!(matches!(
        store.save_pending(&active, &duplicate_header),
        Err(SessionStoreError::InvalidDelivery(_))
    ));

    let mut duplicate_progress = certified.clone();
    let delivery::Payload::Data(payload) =
        duplicate_progress.payload.as_mut().expect("data payload")
    else {
        panic!("fixture must contain data")
    };
    payload.records.push(records[1].clone());
    assert!(matches!(
        store.save_pending(&active, &duplicate_progress),
        Err(SessionStoreError::InvalidDelivery(_))
    ));

    store
        .save_pending(&active, &certified)
        .expect("one header plus its final progress certificate is valid");
}

#[test]
fn sqlite_store_persists_revision_pending_delivery_and_atomic_ack() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("sessions.sqlite");
    {
        let mut store = SessionStore::open(&path).expect("open store");
        let applied = store
            .apply_desired_state(desired_state(0, 1))
            .expect("apply desired state");
        assert_eq!(applied.revision, 1);
        let activation = store
            .load("runtime-a", 1)
            .expect("load activation")
            .pending_delivery
            .expect("activation barrier");
        store
            .acknowledge(
                1,
                &Acknowledge {
                    session_id: "runtime-a".into(),
                    sequence: activation.sequence,
                    delivery_token: activation.delivery_token,
                },
            )
            .expect("ack activation barrier");
        store
            .save_pending(&desired_state(0, 1), &data_delivery())
            .expect("persist pending delivery");
    }

    let mut store = SessionStore::open(&path).expect("reopen store");
    let state = store.load("runtime-a", 1).expect("load session");
    assert_eq!(state.desired_state.expect("desired state").new_revision, 1);
    assert_eq!(
        state.pending_delivery.expect("pending delivery").sequence,
        2
    );

    let acknowledgement = Acknowledge {
        session_id: "runtime-a".into(),
        sequence: 2,
        delivery_token: 2_u64.to_be_bytes().to_vec(),
    };
    let cursor = store
        .acknowledge(1, &acknowledgement)
        .expect("commit acknowledgement");
    assert_eq!(cursor.next_block, 101);
    store
        .acknowledge(1, &acknowledgement)
        .expect("duplicate acknowledgement is idempotent");

    let state = store.load("runtime-a", 1).expect("load committed session");
    assert!(state.pending_delivery.is_none());
    assert_eq!(state.acknowledged_cursor.expect("cursor").batch_sequence, 2);
}

#[test]
fn sqlite_store_rejects_stale_revision_without_mutating_committed_state() {
    let mut store = SessionStore::open_in_memory().expect("in-memory store");
    store
        .apply_desired_state(desired_state(0, 1))
        .expect("initial desired state");

    let mut stale_replacement = desired_state(0, 1);
    stale_replacement.owners.push(OwnerInterests {
        owner_id: "different-state".into(),
        interests: Vec::new(),
        backfill: None,
        canonical: false,
    });
    let error = store
        .apply_desired_state(stale_replacement)
        .expect_err("stale compare-and-swap must fail");
    assert!(matches!(
        error,
        SessionStoreError::RevisionConflict {
            expected: 0,
            committed: 1
        }
    ));
    assert_eq!(
        store
            .load("runtime-a", 1)
            .expect("load session")
            .desired_state
            .expect("desired state")
            .new_revision,
        1
    );
}

#[test]
fn sqlite_store_replays_a_lost_identical_desired_state_ack_idempotently() {
    let mut store = SessionStore::open_in_memory().expect("in-memory store");
    let request = desired_state(0, 1);
    store
        .apply_desired_state(request.clone())
        .expect("initial desired state");

    let replayed = store
        .apply_desired_state(request)
        .expect("identical retry after a lost ACK");
    assert_eq!(replayed.revision, 1);
}

#[test]
fn sqlite_activation_barrier_preserves_the_source_prepared_cursor() {
    let mut store = SessionStore::open_in_memory().expect("in-memory store");
    let request = desired_state(0, 1);
    let prepared = Cursor {
        chain_id: 1,
        query_revision: 1,
        next_block: 25_000_000,
        canonical_head: None,
        batch_sequence: 0,
        provider_checkpoint: b"source activation boundary".to_vec(),
        owner_backfill_activation_block: None,
    };
    store
        .apply_desired_state_with_cursor(request, Some(&prepared))
        .expect("apply prepared desired state");

    let activation = store
        .load("runtime-a", 1)
        .expect("load activation")
        .pending_delivery
        .expect("activation barrier");
    let cursor = activation.cursor.expect("activation cursor");
    assert_eq!(cursor.next_block, prepared.next_block);
    assert_eq!(cursor.query_revision, prepared.query_revision);
    assert_eq!(cursor.provider_checkpoint, prepared.provider_checkpoint);
    assert_eq!(cursor.batch_sequence, activation.sequence);
}

#[test]
fn sqlite_activation_allows_only_a_new_revision_rewind_with_preserved_global_coverage() {
    let mut store = SessionStore::open_in_memory().expect("in-memory store");
    let revision_one = desired_state(0, 1);
    store
        .apply_desired_state(revision_one.clone())
        .expect("apply first revision");
    let activation = store
        .load("runtime-a", 1)
        .expect("load first activation")
        .pending_delivery
        .expect("first activation");
    store
        .acknowledge(
            1,
            &Acknowledge {
                session_id: "runtime-a".into(),
                sequence: activation.sequence,
                delivery_token: activation.delivery_token,
            },
        )
        .expect("ack first activation");
    let canonical = data_delivery();
    store
        .save_pending(&revision_one, &canonical)
        .expect("persist canonical coverage");
    let coverage = store
        .acknowledge(
            1,
            &Acknowledge {
                session_id: "runtime-a".into(),
                sequence: canonical.sequence,
                delivery_token: canonical.delivery_token,
            },
        )
        .expect("ack canonical coverage");

    let mut revision_two = desired_state(1, 2);
    revision_two.owners.push(OwnerInterests {
        owner_id: "bounded".into(),
        interests: Vec::new(),
        backfill: Some(evm_fork_cache_event_protocol::v1::Backfill {
            from_block: 50,
            to_block_excl: Some(75),
            retained_baseline: None,
        }),
        canonical: false,
    });
    let prepared = Cursor {
        chain_id: 1,
        query_revision: 2,
        next_block: 50,
        canonical_head: coverage.canonical_head.clone(),
        batch_sequence: coverage.batch_sequence,
        provider_checkpoint: b"rewound scan checkpoint".to_vec(),
        owner_backfill_activation_block: Some(100),
    };
    store
        .apply_desired_state_with_cursor(revision_two.clone(), Some(&prepared))
        .expect("the exact new-revision activation may rewind only its scan position");

    let activation = store
        .load("runtime-a", 1)
        .expect("load rewound activation")
        .pending_delivery
        .expect("rewound activation barrier");
    let cursor = activation.cursor.as_ref().expect("activation cursor");
    assert_eq!(cursor.next_block, 50);
    assert_eq!(cursor.canonical_head, coverage.canonical_head);
    assert_eq!(cursor.owner_backfill_activation_block, Some(100));
    let delivery::Payload::Barrier(barrier) =
        activation.payload.as_ref().expect("activation payload")
    else {
        panic!("activation must be a barrier")
    };
    assert!(
        barrier.block.is_none(),
        "the preserved old head is cursor authority, not a new catch-up proof"
    );

    store
        .acknowledge(
            1,
            &Acknowledge {
                session_id: "runtime-a".into(),
                sequence: activation.sequence,
                delivery_token: activation.delivery_token,
            },
        )
        .expect("ack rewound activation");
    let owner_catchup = Delivery {
        session_id: "runtime-a".into(),
        sequence: 4,
        query_revision: 2,
        delivery_token: 4_u64.to_be_bytes().to_vec(),
        cursor: Some(Cursor {
            chain_id: 1,
            query_revision: 2,
            next_block: 60,
            canonical_head: coverage.canonical_head.clone(),
            batch_sequence: 4,
            provider_checkpoint: b"owner scan 60".to_vec(),
            owner_backfill_activation_block: Some(100),
        }),
        payload: Some(delivery::Payload::Data(DataPayload {
            records: vec![EventRecord {
                event: Some(ChainEvent {
                    event: Some(chain_event::Event::Log(LogEvent {
                        address: vec![0x11; 20],
                        topics: Vec::new(),
                        data: Vec::new(),
                        block_number: 59,
                        block_hash: vec![0x59; 32],
                        transaction_hash: vec![0x44; 32],
                        transaction_index: 0,
                        log_index: 0,
                        block_timestamp: 59,
                        removed: false,
                    })),
                }),
                canonical_audience: false,
                owner_ids: vec!["bounded".into()],
                scope: DeliveryScope::OwnerCatchup.into(),
            }],
        })),
        checkpoint_neutral: false,
    };
    store
        .save_pending(&revision_two, &owner_catchup)
        .expect("owner-only data may advance scan position behind preserved coverage");
    store
        .acknowledge(
            1,
            &Acknowledge {
                session_id: "runtime-a".into(),
                sequence: 4,
                delivery_token: 4_u64.to_be_bytes().to_vec(),
            },
        )
        .expect("ack owner catch-up data");

    let progress = Delivery {
        session_id: "runtime-a".into(),
        sequence: 5,
        query_revision: 2,
        delivery_token: 5_u64.to_be_bytes().to_vec(),
        cursor: Some(Cursor {
            chain_id: 1,
            query_revision: 2,
            next_block: 90,
            canonical_head: coverage.canonical_head.clone(),
            batch_sequence: 5,
            provider_checkpoint: b"owner scan 90".to_vec(),
            owner_backfill_activation_block: Some(100),
        }),
        payload: Some(delivery::Payload::Barrier(Barrier {
            id: b"source-progress:2:90".to_vec(),
            block: None,
        })),
        checkpoint_neutral: true,
    };
    store
        .save_pending(&revision_two, &progress)
        .expect("blockless scan-progress barrier may advance behind preserved coverage");
    store
        .acknowledge(
            1,
            &Acknowledge {
                session_id: "runtime-a".into(),
                sequence: 5,
                delivery_token: 5_u64.to_be_bytes().to_vec(),
            },
        )
        .expect("ack scan progress");

    let mut changed_boundary = progress.clone();
    changed_boundary.sequence = 6;
    changed_boundary.delivery_token = 6_u64.to_be_bytes().to_vec();
    let cursor = changed_boundary.cursor.as_mut().expect("cursor");
    cursor.batch_sequence = 6;
    cursor.next_block = 91;
    cursor.owner_backfill_activation_block = Some(101);
    changed_boundary.payload = Some(delivery::Payload::Barrier(Barrier {
        id: b"source-progress:2:91".to_vec(),
        block: None,
    }));
    assert!(matches!(
        store.save_pending(&revision_two, &changed_boundary),
        Err(SessionStoreError::InvalidDelivery(_))
    ));

    let mut unauthorized = progress;
    unauthorized.sequence = 6;
    unauthorized.delivery_token = 6_u64.to_be_bytes().to_vec();
    unauthorized.cursor.as_mut().expect("cursor").batch_sequence = 6;
    unauthorized.cursor.as_mut().expect("cursor").next_block = 40;
    unauthorized.payload = Some(delivery::Payload::Barrier(Barrier {
        id: b"ordinary-control".to_vec(),
        block: None,
    }));
    assert!(matches!(
        store.save_pending(&revision_two, &unauthorized),
        Err(SessionStoreError::InvalidDelivery(_))
    ));
}

#[test]
fn sqlite_store_requires_a_portable_boundary_for_open_owner_backfill_activation() {
    let mut store = SessionStore::open_in_memory().expect("in-memory store");
    let mut request = desired_state(0, 1);
    request.owners.push(OwnerInterests {
        owner_id: "open".into(),
        interests: Vec::new(),
        backfill: Some(evm_fork_cache_event_protocol::v1::Backfill {
            from_block: 50,
            to_block_excl: None,
            retained_baseline: None,
        }),
        canonical: false,
    });
    let prepared = Cursor {
        chain_id: 1,
        query_revision: 1,
        next_block: 50,
        canonical_head: None,
        batch_sequence: 0,
        provider_checkpoint: b"opaque".to_vec(),
        owner_backfill_activation_block: None,
    };

    let error = store
        .apply_desired_state_with_cursor(request, Some(&prepared))
        .expect_err("open backfill without a portable target must fail closed");
    assert!(error.to_string().contains("portable boundary"));
}

#[test]
fn sqlite_store_rejects_a_malformed_source_prepared_cursor_before_commit() {
    let mut store = SessionStore::open_in_memory().expect("in-memory store");
    let request = desired_state(0, 1);
    let malformed = Cursor {
        chain_id: 1,
        query_revision: 1,
        next_block: 101,
        canonical_head: Some(BlockRef {
            number: 100,
            hash: vec![0x10; 31],
            parent_hash: vec![0x0f; 32],
            timestamp: 100,
        }),
        batch_sequence: 0,
        provider_checkpoint: b"opaque".to_vec(),
        owner_backfill_activation_block: None,
    };

    assert!(matches!(
        store.apply_desired_state_with_cursor(request, Some(&malformed)),
        Err(SessionStoreError::InvalidDelivery(_))
    ));
    let persisted = store
        .load("runtime-a", 1)
        .expect("rejected activation leaves a readable session");
    assert!(persisted.desired_state.is_none());
    assert!(persisted.pending_delivery.is_none());
}

#[test]
fn sqlite_store_rejects_a_delivery_that_skips_the_next_sequence() {
    let mut store = SessionStore::open_in_memory().expect("in-memory store");
    store
        .apply_desired_state(desired_state(0, 1))
        .expect("initial desired state");
    let activation = store
        .load("runtime-a", 1)
        .expect("load activation")
        .pending_delivery
        .expect("activation");
    store
        .acknowledge(
            1,
            &Acknowledge {
                session_id: "runtime-a".into(),
                sequence: activation.sequence,
                delivery_token: activation.delivery_token,
            },
        )
        .expect("ack activation");
    let mut skipped = data_delivery();
    skipped.sequence = 3;
    skipped.delivery_token = 3_u64.to_be_bytes().to_vec();
    skipped.cursor.as_mut().expect("cursor").batch_sequence = 3;
    assert!(matches!(
        store.save_pending(&desired_state(0, 1), &skipped),
        Err(SessionStoreError::InvalidDelivery(_))
    ));
}

#[test]
fn sqlite_store_rejects_empty_data_before_it_can_advance_the_cursor() {
    let mut store = SessionStore::open_in_memory().expect("in-memory store");
    let active = desired_state(0, 1);
    store
        .apply_desired_state(active.clone())
        .expect("active desired state");
    let activation = store
        .load("runtime-a", 1)
        .expect("load activation")
        .pending_delivery
        .expect("activation");
    store
        .acknowledge(
            1,
            &Acknowledge {
                session_id: "runtime-a".into(),
                sequence: activation.sequence,
                delivery_token: activation.delivery_token,
            },
        )
        .expect("ack activation");
    let mut empty = data_delivery();
    empty.payload = Some(delivery::Payload::Data(DataPayload {
        records: Vec::new(),
    }));

    assert!(matches!(
        store.save_pending(&active, &empty),
        Err(SessionStoreError::InvalidDelivery(_))
    ));
    assert!(
        store
            .load("runtime-a", 1)
            .expect("active session")
            .pending_delivery
            .is_none()
    );
}

#[test]
fn sqlite_store_rejects_canonical_logs_without_a_coverage_certificate() {
    let mut store = SessionStore::open_in_memory().expect("in-memory store");
    let active = desired_state(0, 1);
    store
        .apply_desired_state(active.clone())
        .expect("active desired state");
    let activation = store
        .load("runtime-a", 1)
        .expect("load activation")
        .pending_delivery
        .expect("activation");
    store
        .acknowledge(
            1,
            &Acknowledge {
                session_id: "runtime-a".into(),
                sequence: activation.sequence,
                delivery_token: activation.delivery_token,
            },
        )
        .expect("ack activation");

    let mut uncertified = data_delivery();
    let log = LogEvent {
        address: vec![0x11; 20],
        topics: Vec::new(),
        data: Vec::new(),
        block_number: 100,
        block_hash: vec![0x10; 32],
        transaction_hash: vec![0x20; 32],
        transaction_index: 0,
        log_index: 0,
        block_timestamp: 100,
        removed: false,
    };
    uncertified.payload = Some(delivery::Payload::Data(DataPayload {
        records: vec![EventRecord {
            event: Some(ChainEvent {
                event: Some(chain_event::Event::Log(log.clone())),
            }),
            canonical_audience: true,
            owner_ids: Vec::new(),
            scope: DeliveryScope::Canonical.into(),
        }],
    }));
    let cursor = uncertified.cursor.as_mut().expect("cursor");
    cursor.canonical_head = None;
    cursor.next_block = 101;

    assert!(matches!(
        store.save_pending(&active, &uncertified),
        Err(SessionStoreError::InvalidDelivery(
            "canonical data is not certified by a final block identity at or above every canonical record"
        ))
    ));
    assert!(
        store
            .load("runtime-a", 1)
            .expect("active session")
            .pending_delivery
            .is_none()
    );

    let mut certified = data_delivery();
    let delivery::Payload::Data(data) = certified.payload.as_mut().expect("data") else {
        panic!("test delivery must carry data")
    };
    data.records.insert(
        0,
        EventRecord {
            event: Some(ChainEvent {
                event: Some(chain_event::Event::Log(log)),
            }),
            canonical_audience: true,
            owner_ids: Vec::new(),
            scope: DeliveryScope::Canonical.into(),
        },
    );
    store
        .save_pending(&active, &certified)
        .expect("later progress at the same height certifies the log");
}

#[test]
fn sqlite_store_rejects_oversized_delivery_before_deep_record_validation() {
    let mut store = SessionStore::open_in_memory().expect("in-memory store");
    let active = desired_state(0, 1);
    store
        .apply_desired_state(active.clone())
        .expect("active desired state");
    let activation = store
        .load("runtime-a", 1)
        .expect("load activation")
        .pending_delivery
        .expect("activation");
    store
        .acknowledge(
            1,
            &Acknowledge {
                session_id: "runtime-a".into(),
                sequence: activation.sequence,
                delivery_token: activation.delivery_token,
            },
        )
        .expect("ack activation");
    let mut oversized_and_malformed = data_delivery();
    oversized_and_malformed.payload = Some(delivery::Payload::Data(DataPayload {
        records: vec![EventRecord {
            event: Some(ChainEvent {
                event: Some(chain_event::Event::Log(LogEvent {
                    // Structural validation would reject this width. The hard
                    // envelope limit must win first without allocating sets.
                    address: vec![0x11; 19],
                    topics: Vec::new(),
                    data: vec![0xaa; MAX_MESSAGE_SIZE_BYTES],
                    block_number: 100,
                    block_hash: vec![0x10; 32],
                    transaction_hash: vec![0x20; 32],
                    transaction_index: 0,
                    log_index: 0,
                    block_timestamp: 100,
                    removed: false,
                })),
            }),
            canonical_audience: true,
            owner_ids: Vec::new(),
            scope: DeliveryScope::Canonical.into(),
        }],
    }));

    assert!(matches!(
        store.save_pending(&active, &oversized_and_malformed),
        Err(SessionStoreError::DeliveryTooLarge { .. })
    ));
}

#[test]
fn sqlite_store_rejects_data_whose_cursor_head_disagrees_with_its_records() {
    let mut store = SessionStore::open_in_memory().expect("in-memory store");
    let active = desired_state(0, 1);
    store
        .apply_desired_state(active.clone())
        .expect("active desired state");
    let activation = store
        .load("runtime-a", 1)
        .expect("load activation")
        .pending_delivery
        .expect("activation");
    store
        .acknowledge(
            1,
            &Acknowledge {
                session_id: "runtime-a".into(),
                sequence: activation.sequence,
                delivery_token: activation.delivery_token,
            },
        )
        .expect("ack activation");
    let mut contradictory = data_delivery();
    contradictory
        .cursor
        .as_mut()
        .expect("cursor")
        .canonical_head
        .as_mut()
        .expect("head")
        .hash = vec![0x20; 32];

    assert!(matches!(
        store.save_pending(&active, &contradictory),
        Err(SessionStoreError::InvalidDelivery(_))
    ));
}

#[test]
fn sqlite_store_rejects_internally_consistent_non_reorg_progress_regression() {
    let mut store = SessionStore::open_in_memory().expect("in-memory store");
    let active = desired_state(0, 1);
    store
        .apply_desired_state(active.clone())
        .expect("active desired state");
    let activation = store
        .load("runtime-a", 1)
        .expect("load activation")
        .pending_delivery
        .expect("activation");
    store
        .acknowledge(
            1,
            &Acknowledge {
                session_id: "runtime-a".into(),
                sequence: activation.sequence,
                delivery_token: activation.delivery_token,
            },
        )
        .expect("ack activation");
    let forward = data_delivery();
    store.save_pending(&active, &forward).expect("forward data");
    store
        .acknowledge(
            1,
            &Acknowledge {
                session_id: "runtime-a".into(),
                sequence: forward.sequence,
                delivery_token: forward.delivery_token,
            },
        )
        .expect("ack forward data");

    let regressed_head = BlockRef {
        number: 99,
        hash: vec![0x09; 32],
        parent_hash: vec![0x08; 32],
        timestamp: 99,
    };
    let mut regressed = data_delivery();
    regressed.sequence = 3;
    regressed.delivery_token = 3_u64.to_be_bytes().to_vec();
    regressed.cursor = Some(Cursor {
        chain_id: 1,
        query_revision: 1,
        next_block: 100,
        canonical_head: Some(regressed_head.clone()),
        batch_sequence: 3,
        provider_checkpoint: b"regressed".to_vec(),
        owner_backfill_activation_block: None,
    });
    regressed.payload = Some(delivery::Payload::Data(DataPayload {
        records: vec![EventRecord {
            event: Some(ChainEvent {
                event: Some(chain_event::Event::BlockProgress(BlockProgressEvent {
                    block: Some(regressed_head),
                })),
            }),
            canonical_audience: true,
            owner_ids: Vec::new(),
            scope: DeliveryScope::CanonicalProgress.into(),
        }],
    }));

    assert!(matches!(
        store.save_pending(&active, &regressed),
        Err(SessionStoreError::InvalidDelivery(_))
    ));
}

#[test]
fn sqlite_store_allows_an_explicit_reorg_to_rewind_to_its_common_ancestor() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("reorg-authority.sqlite");
    let mut store = SessionStore::open(&path).expect("store");
    let active = desired_state(0, 1);
    store
        .apply_desired_state(active.clone())
        .expect("active desired state");
    let activation = store
        .load("runtime-a", 1)
        .expect("load activation")
        .pending_delivery
        .expect("activation");
    store
        .acknowledge(
            1,
            &Acknowledge {
                session_id: "runtime-a".into(),
                sequence: activation.sequence,
                delivery_token: activation.delivery_token,
            },
        )
        .expect("ack activation");
    let forward = data_delivery();
    store.save_pending(&active, &forward).expect("forward data");
    store
        .acknowledge(
            1,
            &Acknowledge {
                session_id: "runtime-a".into(),
                sequence: forward.sequence,
                delivery_token: forward.delivery_token,
            },
        )
        .expect("ack forward data");

    let ancestor = BlockRef {
        number: 99,
        hash: vec![0x0f; 32],
        parent_hash: vec![0x08; 32],
        timestamp: 99,
    };
    let old_tip = BlockRef {
        number: 100,
        hash: vec![0x10; 32],
        parent_hash: ancestor.hash.clone(),
        timestamp: 100,
    };
    let new_tip = BlockRef {
        number: 100,
        hash: vec![0x20; 32],
        parent_hash: ancestor.hash.clone(),
        timestamp: 100,
    };
    let reorg = Delivery {
        session_id: "runtime-a".into(),
        sequence: 3,
        query_revision: 1,
        delivery_token: 3_u64.to_be_bytes().to_vec(),
        cursor: Some(Cursor {
            chain_id: 1,
            query_revision: 1,
            next_block: 100,
            canonical_head: Some(ancestor.clone()),
            batch_sequence: 3,
            provider_checkpoint: b"reorg".to_vec(),
            owner_backfill_activation_block: None,
        }),
        payload: Some(delivery::Payload::Reorg(Reorg {
            common_ancestor: Some(ancestor),
            old_tip: Some(old_tip),
            new_tip: Some(new_tip.clone()),
        })),
        checkpoint_neutral: false,
    };

    let mut identical_tips = reorg.clone();
    let delivery::Payload::Reorg(payload) = identical_tips.payload.as_mut().expect("reorg") else {
        panic!("test delivery must carry a reorg")
    };
    payload.new_tip.as_mut().expect("new tip").hash =
        payload.old_tip.as_ref().expect("old tip").hash.clone();
    assert!(matches!(
        store.save_pending(&active, &identical_tips),
        Err(SessionStoreError::InvalidDelivery(
            "reorg tips do not describe two distinct descendant branches"
        ))
    ));

    let mut wrong_parent = reorg.clone();
    let delivery::Payload::Reorg(payload) = wrong_parent.payload.as_mut().expect("reorg") else {
        panic!("test delivery must carry a reorg")
    };
    payload.new_tip.as_mut().expect("new tip").parent_hash = vec![0x77; 32];
    assert!(matches!(
        store.save_pending(&active, &wrong_parent),
        Err(SessionStoreError::InvalidDelivery(
            "reorg tips do not describe two distinct descendant branches"
        ))
    ));

    let mut stale_timestamp = reorg.clone();
    let delivery::Payload::Reorg(payload) = stale_timestamp.payload.as_mut().expect("reorg") else {
        panic!("test delivery must carry a reorg")
    };
    payload.new_tip.as_mut().expect("new tip").timestamp = payload
        .common_ancestor
        .as_ref()
        .expect("ancestor")
        .timestamp;
    assert!(matches!(
        store.save_pending(&active, &stale_timestamp),
        Err(SessionStoreError::InvalidDelivery(
            "reorg tips do not describe two distinct descendant branches"
        ))
    ));

    store
        .save_pending(&active, &reorg)
        .expect("explicit reorg may rewind progress");
    store
        .acknowledge(
            1,
            &Acknowledge {
                session_id: "runtime-a".into(),
                sequence: reorg.sequence,
                delivery_token: reorg.delivery_token,
            },
        )
        .expect("acknowledge reorg authority");
    assert_eq!(
        store
            .load("runtime-a", 1)
            .expect("load reorg promise")
            .expected_reorg_tip,
        Some(new_tip.clone())
    );

    drop(store);
    let mut store = SessionStore::open(&path).expect("restart store with reorg promise");
    let active = desired_state(1, 2);
    store
        .apply_desired_state(active.clone())
        .expect("apply a lifecycle revision while replacement is outstanding");
    let activation = store
        .load("runtime-a", 1)
        .expect("load lifecycle activation")
        .pending_delivery
        .expect("lifecycle activation");
    store
        .acknowledge(
            1,
            &Acknowledge {
                session_id: "runtime-a".into(),
                sequence: activation.sequence,
                delivery_token: activation.delivery_token,
            },
        )
        .expect("acknowledge lifecycle activation");
    assert_eq!(
        store
            .load("runtime-a", 1)
            .expect("promise survives unrelated acknowledgement")
            .expected_reorg_tip,
        Some(new_tip.clone())
    );
    let descendant = BlockRef {
        number: 101,
        hash: vec![0x30; 32],
        parent_hash: new_tip.hash.clone(),
        timestamp: 101,
    };
    let progress_record = |block| EventRecord {
        event: Some(ChainEvent {
            event: Some(chain_event::Event::BlockProgress(BlockProgressEvent {
                block: Some(block),
            })),
        }),
        canonical_audience: true,
        owner_ids: Vec::new(),
        scope: DeliveryScope::CanonicalProgress.into(),
    };
    let replacement = Delivery {
        session_id: "runtime-a".into(),
        sequence: 5,
        query_revision: 2,
        delivery_token: 5_u64.to_be_bytes().to_vec(),
        cursor: Some(Cursor {
            chain_id: 1,
            query_revision: 2,
            next_block: 102,
            canonical_head: Some(descendant.clone()),
            batch_sequence: 5,
            provider_checkpoint: b"replacement".to_vec(),
            owner_backfill_activation_block: None,
        }),
        payload: Some(delivery::Payload::Data(DataPayload {
            records: vec![
                progress_record(new_tip.clone()),
                progress_record(descendant.clone()),
            ],
        })),
        checkpoint_neutral: false,
    };

    let conflicting_tip = BlockRef {
        hash: vec![0x21; 32],
        ..new_tip.clone()
    };
    let conflicting_barrier = Delivery {
        cursor: Some(Cursor {
            next_block: 101,
            canonical_head: Some(conflicting_tip.clone()),
            ..replacement.cursor.clone().expect("replacement cursor")
        }),
        payload: Some(delivery::Payload::Barrier(Barrier {
            id: b"source-progress:2:101".to_vec(),
            block: Some(conflicting_tip),
        })),
        ..replacement.clone()
    };
    assert!(matches!(
        store.save_pending(&active, &conflicting_barrier),
        Err(SessionStoreError::InvalidDelivery(
            "delivery does not certify the promised reorg replacement tip"
        ))
    ));

    let mut omitted_anchor = replacement.clone();
    let delivery::Payload::Data(payload) =
        omitted_anchor.payload.as_mut().expect("replacement data")
    else {
        panic!("replacement fixture must contain data")
    };
    payload.records.remove(0);
    assert!(matches!(
        store.save_pending(&active, &omitted_anchor),
        Err(SessionStoreError::InvalidDelivery(
            "delivery does not certify the promised reorg replacement tip"
        ))
    ));

    let mut disconnected_descendant = replacement.clone();
    let delivery::Payload::Data(payload) = disconnected_descendant
        .payload
        .as_mut()
        .expect("replacement data")
    else {
        panic!("replacement fixture must contain data")
    };
    let chain_event::Event::BlockProgress(progress) = payload.records[1]
        .event
        .as_mut()
        .and_then(|event| event.event.as_mut())
        .expect("descendant progress")
    else {
        panic!("replacement fixture must contain progress")
    };
    progress
        .block
        .as_mut()
        .expect("descendant block")
        .parent_hash = vec![0xee; 32];
    disconnected_descendant
        .cursor
        .as_mut()
        .and_then(|cursor| cursor.canonical_head.as_mut())
        .expect("descendant cursor head")
        .parent_hash = vec![0xee; 32];
    assert!(matches!(
        store.save_pending(&active, &disconnected_descendant),
        Err(SessionStoreError::InvalidDelivery(
            "delivery does not certify the promised reorg replacement tip"
        ))
    ));

    let skipped_block = BlockRef {
        number: 102,
        hash: vec![0x32; 32],
        parent_hash: vec![0x31; 32],
        timestamp: 102,
    };
    let mut skipped_explicit_height = replacement.clone();
    let delivery::Payload::Data(payload) = skipped_explicit_height
        .payload
        .as_mut()
        .expect("replacement data")
    else {
        panic!("replacement fixture must contain data")
    };
    payload.records[1] = EventRecord {
        event: Some(ChainEvent {
            event: Some(chain_event::Event::Log(LogEvent {
                address: vec![0x33; 20],
                topics: Vec::new(),
                data: Vec::new(),
                block_number: 101,
                block_hash: vec![0x31; 32],
                transaction_hash: vec![0x41; 32],
                transaction_index: 0,
                log_index: 0,
                block_timestamp: 101,
                removed: false,
            })),
        }),
        canonical_audience: true,
        owner_ids: Vec::new(),
        scope: DeliveryScope::CanonicalProgress.into(),
    };
    payload.records.push(progress_record(skipped_block.clone()));
    let skipped_cursor = skipped_explicit_height.cursor.as_mut().expect("cursor");
    skipped_cursor.next_block = 103;
    skipped_cursor.canonical_head = Some(skipped_block);
    assert!(matches!(
        store.save_pending(&active, &skipped_explicit_height),
        Err(SessionStoreError::InvalidDelivery(
            "delivery does not certify the promised reorg replacement tip"
        ))
    ));

    let rejected_state = store
        .load("runtime-a", 1)
        .expect("rejected replacements retain durable authority");
    assert_eq!(rejected_state.expected_reorg_tip, Some(new_tip.clone()));
    assert!(rejected_state.pending_delivery.is_none());

    store
        .save_pending(&active, &replacement)
        .expect("exact replacement anchor with a continuous descendant");
    assert_eq!(
        store
            .load("runtime-a", 1)
            .expect("promise remains until replacement acknowledgement")
            .expected_reorg_tip,
        Some(new_tip.clone())
    );
    store
        .acknowledge(
            1,
            &Acknowledge {
                session_id: "runtime-a".into(),
                sequence: replacement.sequence,
                delivery_token: replacement.delivery_token,
            },
        )
        .expect("acknowledge certified replacement");
    assert!(
        store
            .load("runtime-a", 1)
            .expect("load certified replacement")
            .expected_reorg_tip
            .is_none()
    );
}

#[test]
fn sqlite_store_rejects_source_output_for_a_different_leased_session() {
    let mut store = SessionStore::open_in_memory().expect("in-memory store");
    let active = desired_state(0, 1);
    store
        .apply_desired_state(active.clone())
        .expect("active desired state");
    let activation = store
        .load("runtime-a", 1)
        .expect("load activation")
        .pending_delivery
        .expect("activation");
    store
        .acknowledge(
            1,
            &Acknowledge {
                session_id: "runtime-a".into(),
                sequence: activation.sequence,
                delivery_token: activation.delivery_token,
            },
        )
        .expect("ack activation");

    let mut foreign = data_delivery();
    foreign.session_id = "runtime-b".into();
    assert!(matches!(
        store.save_pending(&active, &foreign),
        Err(SessionStoreError::InvalidDelivery(_))
    ));
    assert!(
        store
            .load("runtime-a", 1)
            .expect("active session")
            .pending_delivery
            .is_none()
    );
    assert!(
        store
            .load("runtime-b", 1)
            .expect("foreign session")
            .pending_delivery
            .is_none()
    );
}

#[test]
fn sqlite_store_rejects_malformed_source_records_before_they_enter_the_outbox() {
    let mut store = SessionStore::open_in_memory().expect("in-memory store");
    let active = desired_state(0, 1);
    store
        .apply_desired_state(active.clone())
        .expect("active desired state");
    let activation = store
        .load("runtime-a", 1)
        .expect("load activation")
        .pending_delivery
        .expect("activation");
    store
        .acknowledge(
            1,
            &Acknowledge {
                session_id: "runtime-a".into(),
                sequence: activation.sequence,
                delivery_token: activation.delivery_token,
            },
        )
        .expect("ack activation");

    let mut malformed = data_delivery();
    malformed.payload = Some(delivery::Payload::Data(DataPayload {
        records: vec![EventRecord {
            event: Some(ChainEvent {
                event: Some(chain_event::Event::Log(LogEvent {
                    address: vec![0x11; 19],
                    topics: vec![vec![0x22; 32]],
                    data: Vec::new(),
                    block_number: 100,
                    block_hash: vec![0x33; 32],
                    transaction_hash: vec![0x44; 32],
                    transaction_index: 0,
                    log_index: 0,
                    block_timestamp: 100,
                    removed: false,
                })),
            }),
            canonical_audience: true,
            owner_ids: Vec::new(),
            scope: DeliveryScope::Canonical.into(),
        }],
    }));

    assert!(matches!(
        store.save_pending(&active, &malformed),
        Err(SessionStoreError::InvalidDelivery(_))
    ));
    assert!(
        store
            .load("runtime-a", 1)
            .expect("active session")
            .pending_delivery
            .is_none()
    );
}

#[test]
fn sqlite_store_rejects_data_that_the_remote_decoder_cannot_safely_apply() {
    let mut store = SessionStore::open_in_memory().expect("in-memory store");
    let active = desired_state(0, 1);
    store
        .apply_desired_state(active.clone())
        .expect("active desired state");
    let activation = store
        .load("runtime-a", 1)
        .expect("load activation")
        .pending_delivery
        .expect("activation");
    store
        .acknowledge(
            1,
            &Acknowledge {
                session_id: "runtime-a".into(),
                sequence: activation.sequence,
                delivery_token: activation.delivery_token,
            },
        )
        .expect("ack activation");

    let log = LogEvent {
        address: vec![0x11; 20],
        topics: vec![vec![0x22; 32]],
        data: Vec::new(),
        block_number: 100,
        block_hash: vec![0x33; 32],
        transaction_hash: vec![0x44; 32],
        transaction_index: 0,
        log_index: 0,
        block_timestamp: 100,
        removed: false,
    };
    let record = |event| EventRecord {
        event: Some(ChainEvent { event: Some(event) }),
        canonical_audience: true,
        owner_ids: Vec::new(),
        scope: DeliveryScope::Canonical.into(),
    };
    let delivery = |records| {
        let mut delivery = data_delivery();
        delivery.payload = Some(delivery::Payload::Data(DataPayload { records }));
        delivery
    };

    let duplicate_log = delivery(vec![
        record(chain_event::Event::Log(log.clone())),
        record(chain_event::Event::Log(log.clone())),
    ]);
    assert!(matches!(
        store.save_pending(&active, &duplicate_log),
        Err(SessionStoreError::InvalidDelivery(_))
    ));

    let mut second_transaction_position = log.clone();
    second_transaction_position.transaction_index = 1;
    second_transaction_position.log_index = 1;
    let conflicting_transaction_identity = delivery(vec![
        record(chain_event::Event::Log(log.clone())),
        record(chain_event::Event::Log(second_transaction_position)),
    ]);
    assert!(matches!(
        store.save_pending(&active, &conflicting_transaction_identity),
        Err(SessionStoreError::InvalidDelivery(
            "logs disagree on transaction hash/index identity within a block"
        ))
    ));

    let mut conflicting_log = log.clone();
    conflicting_log.block_hash = vec![0x55; 32];
    let conflicting_block_identity = delivery(vec![
        record(chain_event::Event::Log(log.clone())),
        record(chain_event::Event::Log(conflicting_log)),
    ]);
    assert!(matches!(
        store.save_pending(&active, &conflicting_block_identity),
        Err(SessionStoreError::InvalidDelivery(_))
    ));

    let block = BlockRef {
        number: 100,
        hash: vec![0x33; 32],
        parent_hash: vec![0x32; 32],
        timestamp: 100,
    };
    let duplicate_progress = delivery(vec![
        record(chain_event::Event::BlockProgress(BlockProgressEvent {
            block: Some(block.clone()),
        })),
        record(chain_event::Event::BlockProgress(BlockProgressEvent {
            block: Some(block.clone()),
        })),
    ]);
    assert!(matches!(
        store.save_pending(&active, &duplicate_progress),
        Err(SessionStoreError::InvalidDelivery(_))
    ));

    let out_of_order = delivery(vec![
        record(chain_event::Event::BlockProgress(BlockProgressEvent {
            block: Some(block.clone()),
        })),
        record(chain_event::Event::Log(log.clone())),
    ]);
    assert!(matches!(
        store.save_pending(&active, &out_of_order),
        Err(SessionStoreError::InvalidDelivery(
            "data records are not in canonical event order"
        ))
    ));

    let invalid_header = delivery(vec![record(chain_event::Event::BlockHeader(
        BlockHeaderEvent {
            block: Some(block),
            consensus_header_rlp: vec![0x01],
            total_difficulty: Vec::new(),
            size: Vec::new(),
        },
    ))]);
    assert!(matches!(
        store.save_pending(&active, &invalid_header),
        Err(SessionStoreError::InvalidDelivery(_))
    ));

    assert!(
        store
            .load("runtime-a", 1)
            .expect("active session")
            .pending_delivery
            .is_none(),
        "malformed source output must never poison the durable outbox"
    );
}

#[test]
fn sqlite_store_migrates_the_pre_outbox_schema_without_losing_pending_data() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("legacy.sqlite");
    let connection = Connection::open(&path).expect("legacy connection");
    connection
        .execute_batch(
            "CREATE TABLE sessions (
                session_id TEXT NOT NULL,
                chain_id INTEGER NOT NULL,
                revision INTEGER NOT NULL,
                desired_state BLOB,
                acknowledged_cursor BLOB,
                pending_batch BLOB,
                PRIMARY KEY (session_id, chain_id)
            );",
        )
        .expect("legacy schema");
    connection
        .execute(
            "INSERT INTO sessions (
                session_id, chain_id, revision, desired_state, acknowledged_cursor, pending_batch
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                "runtime-a",
                1_i64,
                1_i64,
                desired_state(0, 1).encode_to_vec(),
                Cursor {
                    chain_id: 1,
                    query_revision: 1,
                    next_block: 100,
                    canonical_head: None,
                    batch_sequence: 1,
                    provider_checkpoint: Vec::new(),
                    owner_backfill_activation_block: None,
                }
                .encode_to_vec(),
                data_delivery().encode_to_vec(),
            ],
        )
        .expect("legacy row");
    drop(connection);

    let store = SessionStore::open(&path).expect("migrate legacy database");
    let state = store.load("runtime-a", 1).expect("load migrated state");
    assert_eq!(state.desired_state.expect("desired state").new_revision, 1);
    assert!(state.expected_reorg_tip.is_none());
    assert_eq!(
        state
            .pending_delivery
            .expect("migrated pending batch")
            .sequence,
        2
    );
    let connection = Connection::open(path).expect("inspect schema version");
    let version: u32 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("schema version");
    assert_eq!(
        version,
        evm_fork_cache_event_service::SESSION_SCHEMA_VERSION
    );
    let has_reorg_promise_column: bool = connection
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM pragma_table_info('sessions')
                WHERE name = 'expected_reorg_tip'
            )",
            [],
            |row| row.get(0),
        )
        .expect("inspect migrated reorg promise column");
    assert!(has_reorg_promise_column);
}

#[test]
fn sqlite_store_rejects_a_reorg_promise_behind_acknowledged_canonical_progress() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("stale-reorg-promise.sqlite");
    let mut store = SessionStore::open(&path).expect("open store");
    store
        .apply_desired_state(desired_state(0, 1))
        .expect("desired state");
    let activation = store
        .load("runtime-a", 1)
        .expect("session")
        .pending_delivery
        .expect("activation");
    store
        .acknowledge(
            1,
            &Acknowledge {
                session_id: "runtime-a".into(),
                sequence: activation.sequence,
                delivery_token: activation.delivery_token,
            },
        )
        .expect("ack activation");
    drop(store);

    let expected_tip = BlockRef {
        number: 10,
        hash: vec![0x10; 32],
        parent_hash: vec![0x0f; 32],
        timestamp: 10,
    };
    let acknowledged = Cursor {
        chain_id: 1,
        query_revision: 1,
        next_block: 12,
        canonical_head: Some(BlockRef {
            number: 11,
            hash: vec![0x11; 32],
            parent_hash: expected_tip.hash.clone(),
            timestamp: 11,
        }),
        batch_sequence: 1,
        provider_checkpoint: Vec::new(),
        owner_backfill_activation_block: None,
    };
    Connection::open(&path)
        .expect("database")
        .execute(
            "UPDATE sessions
             SET acknowledged_cursor = ?1, expected_reorg_tip = ?2
             WHERE session_id = ?3 AND chain_id = ?4",
            params![
                acknowledged.encode_to_vec(),
                expected_tip.encode_to_vec(),
                "runtime-a",
                1_i64,
            ],
        )
        .expect("install inconsistent durable row");

    let store = SessionStore::open(&path).expect("reopen store");
    assert!(matches!(
        store.load("runtime-a", 1),
        Err(SessionStoreError::InvalidDelivery(
            "durable reorg replacement promise is at or behind acknowledged canonical progress"
        ))
    ));
}

#[test]
fn sqlite_store_rejects_decodable_rows_whose_identity_is_not_the_row_key() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("corrupt.sqlite");
    {
        let mut store = SessionStore::open(&path).expect("open store");
        store
            .apply_desired_state(desired_state(0, 1))
            .expect("desired state");
    }
    let mut foreign = desired_state(0, 1);
    foreign.session_id = "runtime-b".into();
    Connection::open(&path)
        .expect("database")
        .execute(
            "UPDATE sessions SET desired_state = ?1 WHERE session_id = ?2 AND chain_id = ?3",
            params![foreign.encode_to_vec(), "runtime-a", 1_i64],
        )
        .expect("corrupt row");

    let store = SessionStore::open(&path).expect("reopen store");
    assert!(matches!(
        store.load("runtime-a", 1),
        Err(SessionStoreError::InvalidDelivery(_))
    ));
}

#[test]
fn sqlite_store_rejects_a_desired_revision_whose_activation_barrier_disappeared() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("missing-activation.sqlite");
    {
        let mut store = SessionStore::open(&path).expect("open store");
        store
            .apply_desired_state(desired_state(0, 1))
            .expect("desired state");
    }
    Connection::open(&path)
        .expect("database")
        .execute(
            "UPDATE sessions SET pending_delivery = NULL
             WHERE session_id = ?1 AND chain_id = ?2",
            params!["runtime-a", 1_i64],
        )
        .expect("remove required activation");

    let store = SessionStore::open(&path).expect("reopen store");
    assert!(matches!(
        store.load("runtime-a", 1),
        Err(SessionStoreError::InvalidDelivery(_))
    ));
}

#[test]
fn sqlite_store_rejects_a_new_revision_backed_only_by_the_previous_revision_cursor() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory
        .path()
        .join("missing-replacement-activation.sqlite");
    {
        let mut store = SessionStore::open(&path).expect("open store");
        store
            .apply_desired_state(desired_state(0, 1))
            .expect("initial desired state");
        let activation = store
            .load("runtime-a", 1)
            .expect("load initial activation")
            .pending_delivery
            .expect("initial activation");
        store
            .acknowledge(
                1,
                &Acknowledge {
                    session_id: "runtime-a".into(),
                    sequence: activation.sequence,
                    delivery_token: activation.delivery_token,
                },
            )
            .expect("ack initial activation");
        store
            .apply_desired_state(desired_state(1, 2))
            .expect("replacement desired state");
    }
    Connection::open(&path)
        .expect("database")
        .execute(
            "UPDATE sessions SET pending_delivery = NULL
             WHERE session_id = ?1 AND chain_id = ?2",
            params!["runtime-a", 1_i64],
        )
        .expect("remove replacement activation");

    let store = SessionStore::open(&path).expect("reopen store");
    assert!(matches!(
        store.load("runtime-a", 1),
        Err(SessionStoreError::InvalidDelivery(_))
    ));
}

#[test]
fn duplicate_acknowledgement_still_validates_the_deterministic_token() {
    let mut store = SessionStore::open_in_memory().expect("store");
    store
        .apply_desired_state(desired_state(0, 1))
        .expect("desired state");
    let activation = store
        .load("runtime-a", 1)
        .expect("load")
        .pending_delivery
        .expect("activation");
    let acknowledgement = Acknowledge {
        session_id: "runtime-a".into(),
        sequence: activation.sequence,
        delivery_token: activation.delivery_token,
    };
    store.acknowledge(1, &acknowledgement).expect("first ack");

    let mut forged = acknowledgement;
    forged.delivery_token = b"forged!".to_vec();
    assert!(matches!(
        store.acknowledge(1, &forged),
        Err(SessionStoreError::DeliveryTokenMismatch)
    ));
}

#[test]
fn sqlite_store_rejects_legacy_table_without_the_required_composite_key() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("invalid-key.sqlite");
    let connection = Connection::open(&path).expect("legacy connection");
    connection
        .execute_batch(
            "CREATE TABLE sessions (
                session_id TEXT NOT NULL,
                chain_id INTEGER NOT NULL
            );",
        )
        .expect("legacy schema");
    drop(connection);

    let error = match SessionStore::open(&path) {
        Ok(_) => panic!("incompatible key shape was accepted"),
        Err(error) => error,
    };
    assert!(matches!(error, SessionStoreError::Schema(_)));
    assert!(error.to_string().contains("PRIMARY KEY"));
}

#[test]
fn sqlite_store_rejects_revisions_outside_its_signed_integer_domain() {
    let mut store = SessionStore::open_in_memory().expect("store");
    let error = store
        .apply_desired_state(desired_state(i64::MAX as u64, i64::MAX as u64 + 1))
        .expect_err("revision above SQLite's signed range must fail closed");
    assert!(matches!(
        error,
        SessionStoreError::IntegerRange("new revision")
            | SessionStoreError::IntegerRange("expected revision")
    ));
}

#[test]
fn sqlite_store_round_trips_the_full_unsigned_chain_id_domain() {
    let mut store = SessionStore::open_in_memory().expect("store");
    let mut desired = desired_state(0, 1);
    desired.session_id = "full-width-chain".into();
    desired.chain_id = u64::MAX;

    store
        .apply_desired_state(desired.clone())
        .expect("persist a full-width chain id");
    let activation = store
        .load("full-width-chain", u64::MAX)
        .expect("load the full-width identity")
        .pending_delivery
        .expect("activation delivery");
    assert_eq!(
        activation
            .cursor
            .as_ref()
            .expect("activation cursor")
            .chain_id,
        u64::MAX
    );
    store
        .acknowledge(
            u64::MAX,
            &Acknowledge {
                session_id: "full-width-chain".into(),
                sequence: activation.sequence,
                delivery_token: activation.delivery_token,
            },
        )
        .expect("acknowledge the full-width identity");

    let persisted = store
        .load("full-width-chain", u64::MAX)
        .expect("reload the full-width identity");
    assert_eq!(persisted.desired_state, Some(desired));
    assert_eq!(
        persisted
            .acknowledged_cursor
            .expect("acknowledged cursor")
            .chain_id,
        u64::MAX
    );
    assert!(
        store
            .load("full-width-chain", 1)
            .expect("distinct ordinary-chain identity")
            .desired_state
            .is_none()
    );
}

#[test]
fn sqlite_store_round_trips_activation_sequences_above_the_signed_integer_domain() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("unsigned-activation-sequence.sqlite");
    let mut acknowledged = {
        let mut store = SessionStore::open(&path).expect("open store");
        store
            .apply_desired_state(desired_state(0, 1))
            .expect("desired state");
        let activation = store
            .load("runtime-a", 1)
            .expect("load activation")
            .pending_delivery
            .expect("activation");
        store
            .acknowledge(
                1,
                &Acknowledge {
                    session_id: "runtime-a".into(),
                    sequence: activation.sequence,
                    delivery_token: activation.delivery_token,
                },
            )
            .expect("ack activation")
    };
    acknowledged.batch_sequence = i64::MAX as u64;
    Connection::open(&path)
        .expect("database")
        .execute(
            "UPDATE sessions SET acknowledged_cursor = ?1
             WHERE session_id = ?2 AND chain_id = ?3",
            params![acknowledged.encode_to_vec(), "runtime-a", 1_i64],
        )
        .expect("install signed-boundary sequence authority");

    let mut store = SessionStore::open(&path).expect("reopen store");
    let request = desired_state(1, 2);
    let applied = store
        .apply_desired_state(request.clone())
        .expect("cross the signed activation-sequence boundary");
    assert_eq!(applied.activation_sequence, i64::MAX as u64 + 1);
    assert_eq!(
        store
            .load("runtime-a", 1)
            .expect("reload unsigned activation sequence")
            .pending_delivery
            .expect("activation delivery")
            .sequence,
        i64::MAX as u64 + 1
    );
    assert_eq!(
        store
            .apply_desired_state(request)
            .expect("identical retry preserves unsigned activation sequence")
            .activation_sequence,
        i64::MAX as u64 + 1
    );
}

#[test]
fn sqlite_store_rejects_delivery_sequence_wraparound() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("sequence-overflow.sqlite");
    let mut acknowledged = {
        let mut store = SessionStore::open(&path).expect("open store");
        store
            .apply_desired_state(desired_state(0, 1))
            .expect("desired state");
        let activation = store
            .load("runtime-a", 1)
            .expect("load activation")
            .pending_delivery
            .expect("activation");
        store
            .acknowledge(
                1,
                &Acknowledge {
                    session_id: "runtime-a".into(),
                    sequence: activation.sequence,
                    delivery_token: activation.delivery_token,
                },
            )
            .expect("ack activation")
    };
    acknowledged.batch_sequence = u64::MAX;
    Connection::open(&path)
        .expect("database")
        .execute(
            "UPDATE sessions SET acknowledged_cursor = ?1
             WHERE session_id = ?2 AND chain_id = ?3",
            params![acknowledged.encode_to_vec(), "runtime-a", 1_i64],
        )
        .expect("install exhausted sequence authority");

    let mut store = SessionStore::open(&path).expect("reopen store");
    let error = store
        .apply_desired_state(desired_state(1, 2))
        .expect_err("delivery sequence must not wrap to zero");
    assert!(matches!(error, SessionStoreError::SequenceOverflow));
}
