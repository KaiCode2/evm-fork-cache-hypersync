use evm_fork_cache_event_protocol::{
    PROTOCOL_VERSION, v1,
    v1::{
        AcknowledgementCommitted, ApplyDesiredState, Backfill, Barrier, BlockProgressEvent,
        BlockRef, Capability, ChainEvent, Cursor, Delivery, DeliveryScope, EventRecord, Hello,
        OwnerInterests, PendingDeliveryResume, PortableInterest, SourceCapabilities,
        SourceDescriptor, SourceRole, chain_event, client_message, delivery, portable_interest,
    },
};
use prost::Message;

#[test]
fn owner_backfill_carries_an_exact_retained_canonical_baseline() {
    let baseline = BlockRef {
        number: 41,
        hash: vec![0x41; 32],
        parent_hash: vec![0x40; 32],
        timestamp: 1_700_000_041,
    };
    let backfill = Backfill {
        from_block: 42,
        to_block_excl: None,
        retained_baseline: Some(baseline.clone()),
    };

    let decoded = Backfill::decode(backfill.encode_to_vec().as_slice()).expect("round trip");
    assert_eq!(decoded.retained_baseline, Some(baseline));
}

#[test]
fn acknowledgement_commit_carries_the_authoritative_cursor() {
    let acknowledgement = AcknowledgementCommitted {
        session_id: "runtime-a".into(),
        sequence: 42,
        cursor: Some(Cursor {
            chain_id: 1,
            query_revision: 3,
            next_block: 10_001,
            canonical_head: None,
            batch_sequence: 42,
            provider_checkpoint: vec![1, 2, 3],
            owner_backfill_activation_block: None,
        }),
    };
    let decoded = AcknowledgementCommitted::decode(acknowledgement.encode_to_vec().as_slice())
        .expect("round trip");
    assert_eq!(decoded, acknowledgement);
}

#[test]
fn desired_state_round_trips_with_version_and_owner_identity() {
    let state = ApplyDesiredState {
        protocol_version: PROTOCOL_VERSION,
        session_id: "runtime-a".into(),
        chain_id: 1,
        expected_revision: 3,
        new_revision: 4,
        owners: vec![OwnerInterests {
            owner_id: "pool-a".into(),
            interests: vec![PortableInterest {
                kind: Some(portable_interest::Kind::Block(v1::BlockInterest {
                    mode: v1::BlockMode::Header.into(),
                })),
            }],
            backfill: None,
            canonical: false,
        }],
    };

    let decoded = ApplyDesiredState::decode(state.encode_to_vec().as_slice())
        .expect("generated protocol should round-trip");

    assert_eq!(decoded.protocol_version, PROTOCOL_VERSION);
    assert_eq!(decoded.session_id, "runtime-a");
    assert_eq!(decoded.expected_revision, 3);
    assert_eq!(decoded.new_revision, 4);
    assert_eq!(decoded.owners[0].owner_id, "pool-a");
}

#[test]
fn every_chain_transition_uses_the_same_sequenced_checkpointed_envelope() {
    let delivery = Delivery {
        session_id: "runtime-a".into(),
        sequence: 42,
        query_revision: 4,
        delivery_token: 42_u64.to_be_bytes().to_vec(),
        cursor: Some(Cursor {
            chain_id: 1,
            query_revision: 4,
            next_block: 101,
            canonical_head: None,
            batch_sequence: 42,
            provider_checkpoint: b"provider-native-cursor".to_vec(),
            owner_backfill_activation_block: None,
        }),
        payload: Some(delivery::Payload::Barrier(Barrier {
            id: b"cutover".to_vec(),
            block: None,
        })),
        checkpoint_neutral: false,
    };

    let decoded = Delivery::decode(delivery.encode_to_vec().as_slice()).expect("round trip");
    assert_eq!(decoded.sequence, 42);
    assert_eq!(
        decoded.cursor.expect("cursor").provider_checkpoint,
        b"provider-native-cursor"
    );
    assert!(matches!(
        decoded.payload,
        Some(delivery::Payload::Barrier(_))
    ));
}

#[test]
fn source_capabilities_describe_each_composed_role() {
    let capabilities = SourceCapabilities {
        capabilities: vec![Capability::DurableReplay.into()],
        sources: vec![SourceDescriptor {
            source_id: "archive".into(),
            role: SourceRole::Historical.into(),
            capabilities: vec![Capability::Historical.into()],
        }],
    };
    let decoded =
        SourceCapabilities::decode(capabilities.encode_to_vec().as_slice()).expect("round trip");
    assert_eq!(decoded.sources[0].source_id, "archive");
    assert_eq!(
        SourceRole::try_from(decoded.sources[0].role),
        Ok(SourceRole::Historical)
    );
}

#[test]
fn data_records_preserve_delivery_scope_and_compact_progress() {
    let record = EventRecord {
        event: Some(ChainEvent {
            event: Some(chain_event::Event::BlockProgress(BlockProgressEvent {
                block: Some(BlockRef {
                    number: 42,
                    hash: vec![0x42; 32],
                    parent_hash: vec![0x41; 32],
                    timestamp: 1_700_000_042,
                }),
            })),
        }),
        canonical_audience: false,
        owner_ids: vec!["pool-a".into()],
        scope: DeliveryScope::OwnerCatchup.into(),
    };

    let decoded = EventRecord::decode(record.encode_to_vec().as_slice()).expect("round trip");
    assert_eq!(
        DeliveryScope::try_from(decoded.scope),
        Ok(DeliveryScope::OwnerCatchup)
    );
    assert!(matches!(
        decoded.event.and_then(|event| event.event),
        Some(chain_event::Event::BlockProgress(_))
    ));
}

#[test]
fn hello_round_trips_an_exact_pending_delivery_resume_proof() {
    let hello = Hello {
        protocol_version: PROTOCOL_VERSION,
        session_id: "runtime-a".into(),
        chain_id: 1,
        acknowledged_sequence: 42,
        pending_delivery_resume: Some(PendingDeliveryResume {
            delivery_token: 42_u64.to_be_bytes().to_vec(),
            provider_checkpoint: Some(b"provider-native-cursor".to_vec()),
            coverage_head: Some(BlockRef {
                number: 100,
                hash: vec![0x64; 32],
                parent_hash: vec![0x63; 32],
                timestamp: 1_700_000_100,
            }),
        }),
    };

    let decoded = Hello::decode(hello.encode_to_vec().as_slice()).expect("round trip");
    assert_eq!(decoded, hello);
}

#[test]
fn golden_hello_and_client_envelope_bytes_lock_v1_field_numbers() {
    let hello = Hello {
        protocol_version: 1,
        session_id: "s".into(),
        chain_id: 1,
        acknowledged_sequence: 2,
        pending_delivery_resume: None,
    };
    let hello_bytes = vec![0x08, 0x01, 0x12, 0x01, b's', 0x18, 0x01, 0x20, 0x02];
    assert_eq!(hello.encode_to_vec(), hello_bytes);
    assert_eq!(
        v1::ClientMessage {
            message: Some(client_message::Message::Hello(hello)),
        }
        .encode_to_vec(),
        [vec![0x0a, 0x09], hello_bytes].concat()
    );
}

#[test]
fn golden_cursor_bytes_preserve_the_reserved_tag_and_batch_sequence_tag_six() {
    let cursor = Cursor {
        chain_id: 1,
        query_revision: 2,
        next_block: 3,
        canonical_head: None,
        batch_sequence: 4,
        provider_checkpoint: vec![0xaa],
        owner_backfill_activation_block: None,
    };
    assert_eq!(
        cursor.encode_to_vec(),
        vec![
            0x08, 0x01, // chain_id = field 1
            0x10, 0x02, // query_revision = field 2
            0x18, 0x03, // next_block = field 3
            0x30, 0x04, // batch_sequence = field 6; field 5 stays reserved
            0x3a, 0x01, 0xaa, // provider_checkpoint = field 7
        ]
    );
}

#[test]
fn cursor_carries_an_optional_portable_owner_backfill_activation_boundary() {
    let cursor = Cursor {
        chain_id: 1,
        query_revision: 2,
        next_block: 50,
        canonical_head: None,
        batch_sequence: 4,
        provider_checkpoint: Vec::new(),
        owner_backfill_activation_block: Some(100),
    };
    assert_eq!(cursor.encode_to_vec().last().copied(), Some(100));
    assert!(
        cursor
            .encode_to_vec()
            .windows(2)
            .any(|field| field == [0x40, 0x64]),
        "activation boundary must use append-only cursor field 8"
    );
    assert_eq!(
        Cursor::decode(cursor.encode_to_vec().as_slice())
            .expect("round trip")
            .owner_backfill_activation_block,
        Some(100)
    );
}

#[test]
fn handshake_and_delivery_distinguish_transport_from_runtime_checkpoint_progress() {
    let runtime_cursor = Cursor {
        chain_id: 1,
        query_revision: 1,
        next_block: 100,
        canonical_head: None,
        batch_sequence: 7,
        provider_checkpoint: Vec::new(),
        owner_backfill_activation_block: Some(100),
    };
    let accepted = v1::HelloAccepted {
        protocol_version: PROTOCOL_VERSION,
        session_id: "runtime-a".into(),
        chain_id: 1,
        committed_revision: 2,
        acknowledged_cursor: Some(Cursor {
            batch_sequence: 8,
            query_revision: 2,
            ..runtime_cursor.clone()
        }),
        desired_state: None,
        capabilities: None,
        service_limits: None,
        runtime_checkpoint_position: Some(v1::RuntimeCheckpointPosition {
            cursor: Some(runtime_cursor.clone()),
        }),
    };
    assert_eq!(
        v1::HelloAccepted::decode(accepted.encode_to_vec().as_slice())
            .expect("round trip")
            .runtime_checkpoint_position
            .expect("presence-bearing position")
            .cursor,
        Some(runtime_cursor)
    );

    let delivery = Delivery {
        session_id: "runtime-a".into(),
        sequence: 8,
        query_revision: 2,
        delivery_token: 8_u64.to_be_bytes().to_vec(),
        cursor: accepted.acknowledged_cursor,
        payload: Some(delivery::Payload::Barrier(Barrier {
            id: b"desired-state:2".to_vec(),
            block: None,
        })),
        checkpoint_neutral: true,
    };
    assert!(
        Delivery::decode(delivery.encode_to_vec().as_slice())
            .expect("round trip")
            .checkpoint_neutral
    );
}

#[test]
fn golden_delivery_bytes_lock_cursor_and_barrier_oneof_tags() {
    let delivery = Delivery {
        session_id: "s".into(),
        sequence: 1,
        query_revision: 1,
        delivery_token: 1_u64.to_be_bytes().to_vec(),
        cursor: Some(Cursor {
            chain_id: 1,
            query_revision: 1,
            next_block: 2,
            canonical_head: None,
            batch_sequence: 1,
            provider_checkpoint: Vec::new(),
            owner_backfill_activation_block: None,
        }),
        payload: Some(delivery::Payload::Barrier(Barrier {
            id: b"b".to_vec(),
            block: None,
        })),
        checkpoint_neutral: false,
    };
    assert_eq!(
        delivery.encode_to_vec(),
        vec![
            0x0a, 0x01, b's', // session_id = field 1
            0x10, 0x01, // sequence = field 2
            0x18, 0x01, // query_revision = field 3
            0x22, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x01, // delivery_token = field 4
            0x2a, 0x08, 0x08, 0x01, 0x10, 0x01, 0x18, 0x02, 0x30, 0x01, // cursor = field 5
            0x4a, 0x03, 0x0a, 0x01, b'b', // barrier oneof = field 9
        ]
    );
}

#[test]
fn golden_event_record_bytes_lock_audience_and_scope_tags() {
    let record = EventRecord {
        event: None,
        canonical_audience: false,
        owner_ids: vec!["o".into()],
        scope: DeliveryScope::OwnerCatchup.into(),
    };
    assert_eq!(record.encode_to_vec(), vec![0x1a, 0x01, b'o', 0x20, 0x03]);
    assert_eq!(DeliveryScope::Canonical as i32, 1);
    assert_eq!(DeliveryScope::CanonicalProgress as i32, 2);
    assert_eq!(DeliveryScope::OwnerCatchup as i32, 3);
}
