use std::collections::VecDeque;

use alloy_consensus::Header as ConsensusHeader;
use alloy_network::Ethereum;
use alloy_primitives::{Address, B256, keccak256};
use alloy_rpc_types_eth::Filter;
use async_trait::async_trait;
use evm_fork_cache::reactive::{
    BlockRef as RuntimeBlockRef, ChainControl, DeliveryAudience, DeliveryScope, EventSubscriber,
    HandlerId, InterestOwnerSubscriber, LogInterest as RuntimeLogInterest, ReactiveInput,
    ReactiveInterest, SubscriberBackfill, SubscriberDeliveryToken,
};
use evm_fork_cache_event_protocol::v1::{
    Acknowledge, ApplyDesiredState, Barrier, BlockHeaderEvent, BlockProgressEvent, BlockRef,
    ChainEvent, Cursor, DataPayload, Delivery, DesiredStateApplied, EventRecord, Finality,
    FinalityKind, LogEvent, OwnerInterests, Reorg, chain_event, delivery,
};
use evm_fork_cache_remote::{RemoteEventTransport, RemoteSubscriber, RemoteTransportError};
use prost::Message;

#[derive(Default)]
struct BatchTransport {
    deliveries: VecDeque<Delivery>,
    acknowledgements: Vec<Acknowledge>,
    applied: Vec<ApplyDesiredState>,
    acknowledged_cursor: Option<Cursor>,
}

struct RestoredPendingTransport {
    acknowledged_cursor: Cursor,
    pending_cursor: Cursor,
    acknowledgements: Vec<Acknowledge>,
}

#[async_trait]
impl RemoteEventTransport for RestoredPendingTransport {
    async fn apply_desired_state(
        &mut self,
        _request: ApplyDesiredState,
    ) -> Result<DesiredStateApplied, RemoteTransportError> {
        panic!("restored pending acknowledgement must not change desired state")
    }

    async fn next_delivery(&mut self) -> Result<Option<Delivery>, RemoteTransportError> {
        panic!("restored pending acknowledgement must precede polling")
    }

    async fn acknowledge(
        &mut self,
        acknowledgement: Acknowledge,
    ) -> Result<(), RemoteTransportError> {
        assert_eq!(acknowledgement.sequence, self.pending_cursor.batch_sequence);
        self.acknowledgements.push(acknowledgement);
        self.acknowledged_cursor = self.pending_cursor.clone();
        Ok(())
    }

    fn durable_acknowledged_sequence(&self) -> Option<u64> {
        Some(self.acknowledged_cursor.batch_sequence)
    }

    fn durable_acknowledged_cursor(&self) -> Option<Cursor> {
        Some(self.acknowledged_cursor.clone())
    }
}

#[async_trait]
impl RemoteEventTransport for BatchTransport {
    async fn apply_desired_state(
        &mut self,
        request: ApplyDesiredState,
    ) -> Result<DesiredStateApplied, RemoteTransportError> {
        let applied = DesiredStateApplied {
            session_id: request.session_id.clone(),
            revision: request.new_revision,
            activation_sequence: self
                .acknowledgements
                .last()
                .map_or(1, |acknowledgement| acknowledgement.sequence + 1),
        };
        self.applied.push(request);
        Ok(applied)
    }

    async fn next_delivery(&mut self) -> Result<Option<Delivery>, RemoteTransportError> {
        Ok(self.deliveries.pop_front())
    }

    async fn acknowledge(
        &mut self,
        acknowledgement: Acknowledge,
    ) -> Result<(), RemoteTransportError> {
        self.acknowledgements.push(acknowledgement);
        Ok(())
    }

    fn durable_acknowledged_sequence(&self) -> Option<u64> {
        self.acknowledged_cursor
            .as_ref()
            .map(|cursor| cursor.batch_sequence)
    }

    fn durable_acknowledged_cursor(&self) -> Option<Cursor> {
        self.acknowledged_cursor.clone()
    }
}

fn subscriber_with_authoritative_owners(
    transport: BatchTransport,
    owners: &[&str],
) -> RemoteSubscriber<BatchTransport, Ethereum> {
    let desired = ApplyDesiredState {
        protocol_version: evm_fork_cache_event_protocol::PROTOCOL_VERSION,
        session_id: "runtime-a".into(),
        chain_id: 1,
        expected_revision: 0,
        new_revision: 1,
        owners: owners
            .iter()
            .map(|owner| OwnerInterests {
                owner_id: (*owner).into(),
                interests: Vec::new(),
                backfill: None,
                canonical: false,
            })
            .collect(),
    };
    RemoteSubscriber::new_from_authoritative("runtime-a", 1, transport, Some(desired), 1)
        .expect("authoritative owner state")
}

fn cursor(sequence: u64, head: BlockRef) -> Cursor {
    Cursor {
        chain_id: 1,
        query_revision: 1,
        next_block: head.number + 1,
        canonical_head: Some(head),
        batch_sequence: sequence,
        provider_checkpoint: b"opaque-provider-checkpoint".to_vec(),
        owner_backfill_activation_block: Some(100),
    }
}

fn delivery(sequence: u64, cursor: Cursor, payload: delivery::Payload) -> Delivery {
    Delivery {
        session_id: "runtime-a".into(),
        sequence,
        query_revision: 1,
        delivery_token: sequence.to_be_bytes().to_vec(),
        cursor: Some(cursor),
        payload: Some(payload),
        checkpoint_neutral: false,
    }
}

fn owner_log(transaction_hash: u8, transaction_index: u64, log_index: u64) -> EventRecord {
    EventRecord {
        event: Some(ChainEvent {
            event: Some(chain_event::Event::Log(LogEvent {
                address: vec![0x33; 20],
                topics: Vec::new(),
                data: Vec::new(),
                block_number: 100,
                block_hash: vec![0x10; 32],
                transaction_hash: vec![transaction_hash; 32],
                transaction_index,
                log_index,
                block_timestamp: 100,
                removed: false,
            })),
        }),
        canonical_audience: false,
        owner_ids: vec!["owner-a".into()],
        scope: evm_fork_cache_event_protocol::v1::DeliveryScope::OwnerCatchup.into(),
    }
}

#[tokio::test]
async fn restored_pending_delivery_can_be_acknowledged_before_repolling() {
    let head = BlockRef {
        number: 100,
        hash: vec![0x10; 32],
        parent_hash: vec![0x0f; 32],
        timestamp: 100,
    };
    let acknowledged = Cursor {
        chain_id: 1,
        query_revision: 1,
        next_block: 100,
        canonical_head: None,
        batch_sequence: 1,
        provider_checkpoint: b"acknowledged".to_vec(),
        owner_backfill_activation_block: None,
    };
    let pending = Cursor {
        chain_id: 1,
        query_revision: 1,
        next_block: 101,
        canonical_head: Some(head.clone()),
        batch_sequence: 2,
        provider_checkpoint: b"pending".to_vec(),
        owner_backfill_activation_block: None,
    };
    let transport = RestoredPendingTransport {
        acknowledged_cursor: acknowledged,
        pending_cursor: pending,
        acknowledgements: Vec::new(),
    };
    let desired = ApplyDesiredState {
        protocol_version: evm_fork_cache_event_protocol::PROTOCOL_VERSION,
        session_id: "runtime-a".into(),
        chain_id: 1,
        expected_revision: 0,
        new_revision: 1,
        owners: Vec::new(),
    };
    let mut subscriber =
        RemoteSubscriber::new_from_authoritative("runtime-a", 1, transport, Some(desired), 1)
            .expect("authoritative remote subscriber");
    subscriber
        .restore_position(&evm_fork_cache::reactive::SubscriberResumePosition::new(
            1,
            RuntimeBlockRef {
                number: head.number,
                hash: B256::from_slice(&head.hash),
                parent_hash: Some(B256::from_slice(&head.parent_hash)),
                timestamp: Some(head.timestamp),
            },
            Vec::new(),
            Some(SubscriberDeliveryToken::new(2_u64.to_be_bytes().to_vec())),
            Some(evm_fork_cache::reactive::SubscriberCheckpoint::new(
                b"pending".to_vec(),
            )),
        ))
        .expect("restore exact pending delivery proof");

    subscriber
        .acknowledge_delivery(SubscriberDeliveryToken::new(2_u64.to_be_bytes().to_vec()))
        .await
        .expect("acknowledge the restored delivery before polling");

    assert_eq!(subscriber.transport().acknowledgements.len(), 1);
    assert_eq!(
        subscriber.transport().durable_acknowledged_sequence(),
        Some(2)
    );
}

fn owner_scan_cursor(sequence: u64) -> Cursor {
    Cursor {
        chain_id: 1,
        query_revision: 1,
        next_block: 101,
        canonical_head: None,
        batch_sequence: sequence,
        provider_checkpoint: format!("owner-page-{sequence}").into_bytes(),
        owner_backfill_activation_block: Some(101),
    }
}

#[tokio::test]
async fn remote_delivery_decodes_ordered_scoped_inputs_and_commits_wire_token() {
    let header = ConsensusHeader {
        parent_hash: B256::repeat_byte(0x0f),
        number: 100,
        timestamp: 1_700_000_100,
        ..Default::default()
    };
    let block = BlockRef {
        number: 100,
        hash: header.hash_slow().to_vec(),
        parent_hash: vec![0x0f; 32],
        timestamp: 1_700_000_100,
    };
    let data = DataPayload {
        records: vec![
            EventRecord {
                event: Some(ChainEvent {
                    event: Some(chain_event::Event::BlockHeader(BlockHeaderEvent {
                        block: Some(block.clone()),
                        consensus_header_rlp: alloy_rlp::encode(&header),
                        total_difficulty: Vec::new(),
                        size: Vec::new(),
                    })),
                }),
                canonical_audience: true,
                owner_ids: Vec::new(),
                scope: evm_fork_cache_event_protocol::v1::DeliveryScope::Canonical.into(),
            },
            EventRecord {
                event: Some(ChainEvent {
                    event: Some(chain_event::Event::Log(LogEvent {
                        address: vec![0x33; 20],
                        topics: vec![vec![0x44; 32]],
                        data: vec![0xaa, 0xbb],
                        block_number: 100,
                        block_hash: block.hash.clone(),
                        transaction_hash: vec![0x20; 32],
                        transaction_index: 1,
                        log_index: 0,
                        block_timestamp: 1_700_000_100,
                        removed: false,
                    })),
                }),
                canonical_audience: false,
                owner_ids: vec!["pool-a".into()],
                scope: evm_fork_cache_event_protocol::v1::DeliveryScope::OwnerCatchup.into(),
            },
            EventRecord {
                event: Some(ChainEvent {
                    event: Some(chain_event::Event::BlockProgress(BlockProgressEvent {
                        block: Some(block.clone()),
                    })),
                }),
                canonical_audience: true,
                owner_ids: Vec::new(),
                scope: evm_fork_cache_event_protocol::v1::DeliveryScope::CanonicalProgress.into(),
            },
        ],
    };
    let wire_delivery = delivery(1, cursor(1, block), delivery::Payload::Data(data));
    let expected_commitment = keccak256(wire_delivery.encode_to_vec());
    let mut transport = BatchTransport::default();
    transport.deliveries.push_back(wire_delivery);
    let mut subscriber = subscriber_with_authoritative_owners(transport, &["pool-a"]);

    let decoded = subscriber
        .next_batch()
        .await
        .expect("decode remote delivery")
        .expect("batch");

    assert!(matches!(
        decoded.records()[0].input,
        ReactiveInput::BlockHeader(_)
    ));
    assert!(matches!(decoded.records()[1].input, ReactiveInput::Log(_)));
    assert!(matches!(
        decoded.chain_controls(),
        [ChainControl::CanonicalProgress(progress)] if progress.number == 100
    ));
    assert_eq!(decoded.record_audience(0), Some(&DeliveryAudience::All));
    assert_eq!(
        decoded.record_delivery_scope(0),
        Some(DeliveryScope::Canonical)
    );
    assert_eq!(
        decoded.record_audience(1),
        Some(&DeliveryAudience::Owners(vec![HandlerId::new("pool-a")]))
    );
    assert_eq!(
        decoded.record_delivery_scope(1),
        Some(DeliveryScope::OwnerCatchup)
    );
    assert_eq!(
        decoded
            .subscriber_checkpoint()
            .expect("opaque checkpoint")
            .as_bytes(),
        b"opaque-provider-checkpoint"
    );
    assert_eq!(decoded.chain_id(), Some(1));
    assert_eq!(
        decoded
            .payload_commitment()
            .expect("wire payload commitment")
            .digest(),
        expected_commitment
    );
    let delivery_token = decoded.delivery_token().expect("delivery token").clone();
    subscriber
        .acknowledge_delivery(delivery_token.clone())
        .await
        .expect("acknowledge");
    subscriber
        .acknowledge_delivery(delivery_token)
        .await
        .expect("an exact repeated durable acknowledgement is idempotent");
    assert_eq!(subscriber.transport().acknowledgements.len(), 1);
    assert_eq!(subscriber.transport().acknowledgements[0].sequence, 1);
    assert_eq!(
        subscriber.transport().acknowledgements[0].delivery_token,
        1_u64.to_be_bytes()
    );
}

#[tokio::test]
async fn final_block_log_uses_the_later_progress_records_complete_coverage_identity() {
    let head = BlockRef {
        number: 100,
        hash: vec![0x10; 32],
        parent_hash: vec![0x0f; 32],
        timestamp: 1_700_000_100,
    };
    let data = DataPayload {
        records: vec![
            EventRecord {
                event: Some(ChainEvent {
                    event: Some(chain_event::Event::Log(LogEvent {
                        address: vec![0x33; 20],
                        topics: Vec::new(),
                        data: vec![0xaa],
                        block_number: 100,
                        block_hash: head.hash.clone(),
                        transaction_hash: vec![0x20; 32],
                        transaction_index: 0,
                        log_index: 0,
                        block_timestamp: head.timestamp,
                        removed: false,
                    })),
                }),
                canonical_audience: true,
                owner_ids: Vec::new(),
                scope: evm_fork_cache_event_protocol::v1::DeliveryScope::Canonical.into(),
            },
            EventRecord {
                event: Some(ChainEvent {
                    event: Some(chain_event::Event::BlockProgress(BlockProgressEvent {
                        block: Some(head.clone()),
                    })),
                }),
                canonical_audience: true,
                owner_ids: Vec::new(),
                scope: evm_fork_cache_event_protocol::v1::DeliveryScope::CanonicalProgress.into(),
            },
        ],
    };
    let mut transport = BatchTransport::default();
    transport.deliveries.push_back(delivery(
        1,
        cursor(1, head.clone()),
        delivery::Payload::Data(data),
    ));
    let mut subscriber =
        RemoteSubscriber::<_, Ethereum>::new_at_revision("runtime-a", 1, 1, transport);

    let batch = subscriber
        .next_batch()
        .await
        .expect("decode")
        .expect("batch");
    let expected = RuntimeBlockRef {
        number: 100,
        hash: B256::repeat_byte(0x10),
        parent_hash: Some(B256::repeat_byte(0x0f)),
        timestamp: Some(1_700_000_100),
    };
    assert_eq!(batch.records()[0].context.block, Some(expected));
    assert!(matches!(
        batch.chain_controls(),
        [ChainControl::CanonicalProgress(block)] if *block == expected
    ));
    assert_eq!(
        batch
            .subscriber_checkpoint()
            .expect("resume checkpoint")
            .as_bytes(),
        b"opaque-provider-checkpoint"
    );
}

#[tokio::test]
async fn multi_block_data_compacts_superseded_progress_to_the_highest_coverage() {
    let first = BlockRef {
        number: 100,
        hash: vec![0x10; 32],
        parent_hash: vec![0x0f; 32],
        timestamp: 1_700_000_100,
    };
    let second = BlockRef {
        number: 101,
        hash: vec![0x11; 32],
        parent_hash: first.hash.clone(),
        timestamp: 1_700_000_101,
    };
    let records = [first.clone(), second.clone()]
        .into_iter()
        .enumerate()
        .flat_map(|(index, block)| {
            [
                EventRecord {
                    event: Some(ChainEvent {
                        event: Some(chain_event::Event::Log(LogEvent {
                            address: vec![0x33; 20],
                            topics: Vec::new(),
                            data: vec![index as u8],
                            block_number: block.number,
                            block_hash: block.hash.clone(),
                            transaction_hash: vec![0x20 + index as u8; 32],
                            transaction_index: 0,
                            log_index: 0,
                            block_timestamp: block.timestamp,
                            removed: false,
                        })),
                    }),
                    canonical_audience: true,
                    owner_ids: Vec::new(),
                    scope: evm_fork_cache_event_protocol::v1::DeliveryScope::Canonical.into(),
                },
                EventRecord {
                    event: Some(ChainEvent {
                        event: Some(chain_event::Event::BlockProgress(BlockProgressEvent {
                            block: Some(block),
                        })),
                    }),
                    canonical_audience: true,
                    owner_ids: Vec::new(),
                    scope: evm_fork_cache_event_protocol::v1::DeliveryScope::CanonicalProgress
                        .into(),
                },
            ]
        })
        .collect();
    let mut transport = BatchTransport::default();
    transport.deliveries.push_back(delivery(
        1,
        cursor(1, second.clone()),
        delivery::Payload::Data(DataPayload { records }),
    ));
    let mut subscriber =
        RemoteSubscriber::<_, Ethereum>::new_at_revision("runtime-a", 1, 1, transport);

    let batch = subscriber.next_batch().await.unwrap().unwrap();

    assert_eq!(batch.records().len(), 2);
    assert!(matches!(
        batch.chain_controls(),
        [ChainControl::CanonicalProgress(block)]
            if block.number == second.number && block.hash == B256::repeat_byte(0x11)
    ));
}

#[tokio::test]
async fn remote_rejects_out_of_order_and_conflicting_transaction_identities() {
    let cases = [
        (
            vec![owner_log(0x20, 1, 1), owner_log(0x21, 0, 0)],
            "canonical event order",
        ),
        (
            vec![owner_log(0x20, 0, 0), owner_log(0x20, 1, 1)],
            "transaction hash/index identity",
        ),
        (
            vec![owner_log(0x20, 0, 0), owner_log(0x21, 0, 0)],
            "appears more than once",
        ),
    ];

    for (records, expected) in cases {
        let mut transport = BatchTransport::default();
        transport.deliveries.push_back(delivery(
            1,
            owner_scan_cursor(1),
            delivery::Payload::Data(DataPayload { records }),
        ));
        let mut subscriber = subscriber_with_authoritative_owners(transport, &["owner-a"]);
        let error = subscriber
            .next_batch()
            .await
            .expect_err("ambiguous ordering or identity must fail closed");
        assert!(
            error.to_string().contains(expected),
            "expected {expected:?} in {error}"
        );
    }
}

#[tokio::test]
async fn remote_rejects_an_empty_noncanonical_audience_identity() {
    let mut record = owner_log(0x20, 0, 0);
    record.owner_ids = vec![String::new()];
    let mut transport = BatchTransport::default();
    transport.deliveries.push_back(delivery(
        1,
        owner_scan_cursor(1),
        delivery::Payload::Data(DataPayload {
            records: vec![record],
        }),
    ));
    let mut subscriber = subscriber_with_authoritative_owners(transport, &["owner-a"]);
    let error = subscriber
        .next_batch()
        .await
        .expect_err("empty owner audience must fail closed");
    assert!(error.to_string().contains("audience"));
}

#[tokio::test]
async fn custom_transport_cannot_deliver_to_an_owner_outside_authoritative_state() {
    let mut record = owner_log(0x20, 0, 0);
    record.owner_ids = vec!["intruder".into()];
    let mut transport = BatchTransport::default();
    transport.deliveries.push_back(delivery(
        1,
        owner_scan_cursor(1),
        delivery::Payload::Data(DataPayload {
            records: vec![record],
        }),
    ));
    let desired = ApplyDesiredState {
        protocol_version: evm_fork_cache_event_protocol::PROTOCOL_VERSION,
        session_id: "runtime-a".into(),
        chain_id: 1,
        expected_revision: 0,
        new_revision: 1,
        owners: vec![OwnerInterests {
            owner_id: "owner-a".into(),
            interests: Vec::new(),
            backfill: None,
            canonical: false,
        }],
    };
    let mut subscriber = RemoteSubscriber::<_, Ethereum>::new_from_authoritative(
        "runtime-a",
        1,
        transport,
        Some(desired),
        1,
    )
    .expect("authoritative owner state");

    let error = subscriber
        .next_batch()
        .await
        .expect_err("a custom transport cannot invent delivery owners");
    assert!(error.to_string().contains("audience"));
}

#[tokio::test]
async fn remote_rejects_canonical_logs_without_a_coverage_certificate() {
    let mut record = owner_log(0x20, 0, 0);
    record.canonical_audience = true;
    record.owner_ids.clear();
    record.scope = evm_fork_cache_event_protocol::v1::DeliveryScope::Canonical.into();
    let mut transport = BatchTransport::default();
    transport.deliveries.push_back(delivery(
        1,
        Cursor {
            owner_backfill_activation_block: None,
            ..owner_scan_cursor(1)
        },
        delivery::Payload::Data(DataPayload {
            records: vec![record],
        }),
    ));
    let mut subscriber =
        RemoteSubscriber::<_, Ethereum>::new_at_revision("runtime-a", 1, 1, transport);
    let error = subscriber
        .next_batch()
        .await
        .expect_err("uncertified canonical log must fail closed");
    assert!(error.to_string().contains("not certified"));
}

#[tokio::test]
async fn remote_owner_backfill_fences_lifecycle_until_an_acknowledged_coverage_cursor() {
    let mut subscriber =
        RemoteSubscriber::<_, Ethereum>::new("runtime-a", 1, BatchTransport::default());
    subscriber
        .add_interest_owner_with_backfill(
            HandlerId::new("historical"),
            &[ReactiveInterest::Logs(RuntimeLogInterest {
                provider_filter: Filter::new().address(Address::repeat_byte(0x11)),
                local_matcher: None,
                route_key: None,
            })],
            SubscriberBackfill::from_block(50),
        )
        .await
        .expect("commit backfill revision");

    subscriber.transport_mut().deliveries.push_back(Delivery {
        session_id: "runtime-a".into(),
        sequence: 1,
        query_revision: 1,
        delivery_token: 1_u64.to_be_bytes().to_vec(),
        cursor: Some(Cursor {
            chain_id: 1,
            query_revision: 1,
            next_block: 50,
            canonical_head: None,
            batch_sequence: 1,
            provider_checkpoint: b"activation".to_vec(),
            owner_backfill_activation_block: Some(100),
        }),
        payload: Some(delivery::Payload::Barrier(Barrier {
            id: b"desired-state:1".to_vec(),
            block: None,
        })),
        checkpoint_neutral: true,
    });
    assert!(
        subscriber.next_batch().await.unwrap().is_none(),
        "checkpoint-neutral activation barriers are acknowledged internally"
    );
    assert_eq!(subscriber.transport().acknowledgements.len(), 1);
    subscriber
        .add_interest_owner(
            HandlerId::new("too-early"),
            &[ReactiveInterest::Logs(RuntimeLogInterest {
                provider_filter: Filter::new().address(Address::repeat_byte(0x22)),
                local_matcher: None,
                route_key: None,
            })],
        )
        .await
        .expect_err("activation acknowledgement alone cannot abandon catch-up");

    subscriber.transport_mut().deliveries.push_back(Delivery {
        session_id: "runtime-a".into(),
        sequence: 2,
        query_revision: 1,
        delivery_token: 2_u64.to_be_bytes().to_vec(),
        cursor: Some(Cursor {
            chain_id: 1,
            query_revision: 1,
            next_block: 75,
            canonical_head: None,
            batch_sequence: 2,
            provider_checkpoint: b"owner-page".to_vec(),
            owner_backfill_activation_block: Some(100),
        }),
        payload: Some(delivery::Payload::Data(DataPayload {
            records: vec![EventRecord {
                event: Some(ChainEvent {
                    event: Some(chain_event::Event::Log(LogEvent {
                        address: vec![0x11; 20],
                        topics: Vec::new(),
                        data: Vec::new(),
                        block_number: 74,
                        block_hash: vec![0x74; 32],
                        transaction_hash: vec![0x44; 32],
                        transaction_index: 0,
                        log_index: 0,
                        block_timestamp: 74,
                        removed: false,
                    })),
                }),
                canonical_audience: false,
                owner_ids: vec!["historical".into()],
                scope: evm_fork_cache_event_protocol::v1::DeliveryScope::OwnerCatchup.into(),
            }],
        })),
        checkpoint_neutral: false,
    });
    let owner_page = subscriber.next_batch().await.unwrap().unwrap();
    subscriber
        .acknowledge_delivery(owner_page.delivery_token().expect("token").clone())
        .await
        .expect("ack owner page");

    let head = BlockRef {
        number: 99,
        hash: vec![0x99; 32],
        parent_hash: vec![0x98; 32],
        timestamp: 99,
    };
    subscriber.transport_mut().deliveries.push_back(delivery(
        3,
        cursor(3, head.clone()),
        delivery::Payload::Data(DataPayload {
            records: vec![EventRecord {
                event: Some(ChainEvent {
                    event: Some(chain_event::Event::BlockProgress(BlockProgressEvent {
                        block: Some(head),
                    })),
                }),
                canonical_audience: true,
                owner_ids: Vec::new(),
                scope: evm_fork_cache_event_protocol::v1::DeliveryScope::CanonicalProgress.into(),
            }],
        }),
    ));
    let coverage = subscriber.next_batch().await.unwrap().unwrap();
    subscriber
        .acknowledge_delivery(coverage.delivery_token().expect("token").clone())
        .await
        .expect("ack coverage proof");
    subscriber
        .acknowledge_delivery(SubscriberDeliveryToken::new(2_u64.to_be_bytes().to_vec()))
        .await
        .expect_err("a stale formerly valid token must not be accepted");

    subscriber
        .add_interest_owner(
            HandlerId::new("live"),
            &[ReactiveInterest::Logs(RuntimeLogInterest {
                provider_filter: Filter::new().address(Address::repeat_byte(0x22)),
                local_matcher: None,
                route_key: None,
            })],
        )
        .await
        .expect("lifecycle mutation after proven catch-up");
    let historical = subscriber.transport().applied[1]
        .owners
        .iter()
        .find(|owner| owner.owner_id == "historical")
        .expect("historical owner remains registered");
    assert!(historical.backfill.is_none());
}

#[tokio::test]
async fn remote_accepts_rewound_scan_cursors_only_for_exact_owner_catchup_progress() {
    let head = BlockRef {
        number: 99,
        hash: vec![0x99; 32],
        parent_hash: vec![0x98; 32],
        timestamp: 99,
    };
    let mut transport = BatchTransport {
        acknowledged_cursor: Some(Cursor {
            chain_id: 1,
            query_revision: 1,
            next_block: 100,
            canonical_head: Some(head.clone()),
            batch_sequence: 7,
            provider_checkpoint: b"revision-one".to_vec(),
            owner_backfill_activation_block: Some(100),
        }),
        ..BatchTransport::default()
    };
    transport.deliveries.extend([
        Delivery {
            session_id: "runtime-a".into(),
            sequence: 8,
            query_revision: 2,
            delivery_token: 8_u64.to_be_bytes().to_vec(),
            cursor: Some(Cursor {
                chain_id: 1,
                query_revision: 2,
                next_block: 50,
                canonical_head: Some(head.clone()),
                batch_sequence: 8,
                provider_checkpoint: b"activation".to_vec(),
                owner_backfill_activation_block: Some(100),
            }),
            payload: Some(delivery::Payload::Barrier(Barrier {
                id: b"desired-state:2".to_vec(),
                block: None,
            })),
            checkpoint_neutral: true,
        },
        Delivery {
            session_id: "runtime-a".into(),
            sequence: 9,
            query_revision: 2,
            delivery_token: 9_u64.to_be_bytes().to_vec(),
            cursor: Some(Cursor {
                chain_id: 1,
                query_revision: 2,
                next_block: 75,
                canonical_head: Some(head.clone()),
                batch_sequence: 9,
                provider_checkpoint: b"owner-page".to_vec(),
                owner_backfill_activation_block: Some(100),
            }),
            payload: Some(delivery::Payload::Data(DataPayload {
                records: vec![EventRecord {
                    event: Some(ChainEvent {
                        event: Some(chain_event::Event::Log(LogEvent {
                            address: vec![0x11; 20],
                            topics: Vec::new(),
                            data: Vec::new(),
                            block_number: 74,
                            block_hash: vec![0x74; 32],
                            transaction_hash: vec![0x44; 32],
                            transaction_index: 0,
                            log_index: 0,
                            block_timestamp: 74,
                            removed: false,
                        })),
                    }),
                    canonical_audience: false,
                    owner_ids: vec!["historical".into()],
                    scope: evm_fork_cache_event_protocol::v1::DeliveryScope::OwnerCatchup.into(),
                }],
            })),
            checkpoint_neutral: false,
        },
        Delivery {
            session_id: "runtime-a".into(),
            sequence: 10,
            query_revision: 2,
            delivery_token: 10_u64.to_be_bytes().to_vec(),
            cursor: Some(Cursor {
                chain_id: 1,
                query_revision: 2,
                next_block: 100,
                canonical_head: Some(head),
                batch_sequence: 10,
                provider_checkpoint: b"activation-reached".to_vec(),
                owner_backfill_activation_block: Some(100),
            }),
            payload: Some(delivery::Payload::Barrier(Barrier {
                id: b"source-progress:2:100".to_vec(),
                block: None,
            })),
            checkpoint_neutral: true,
        },
    ]);
    let desired = ApplyDesiredState {
        protocol_version: evm_fork_cache_event_protocol::PROTOCOL_VERSION,
        session_id: "runtime-a".into(),
        chain_id: 1,
        expected_revision: 1,
        new_revision: 2,
        owners: vec![evm_fork_cache_event_protocol::v1::OwnerInterests {
            owner_id: "historical".into(),
            interests: Vec::new(),
            backfill: Some(evm_fork_cache_event_protocol::v1::Backfill {
                from_block: 50,
                to_block_excl: None,
                retained_baseline: None,
            }),
            canonical: false,
        }],
    };
    let mut subscriber =
        RemoteSubscriber::new_from_authoritative("runtime-a", 1, transport, Some(desired), 2)
            .expect("restore desired state with incomplete catch-up");

    let owner_page = subscriber.next_batch().await.unwrap().unwrap();
    assert_eq!(owner_page.chain_id(), Some(1));
    subscriber
        .acknowledge_delivery(owner_page.delivery_token().expect("token").clone())
        .await
        .expect("ack legitimate catch-up cursor");
    assert!(
        subscriber.next_batch().await.unwrap().is_none(),
        "activation and exact source-progress barriers are acknowledged internally"
    );
    assert_eq!(subscriber.transport().acknowledgements.len(), 3);
}

#[tokio::test]
async fn authoritative_restart_clears_mixed_backfills_only_at_their_explicit_targets() {
    let desired = ApplyDesiredState {
        protocol_version: evm_fork_cache_event_protocol::PROTOCOL_VERSION,
        session_id: "runtime-a".into(),
        chain_id: 1,
        expected_revision: 0,
        new_revision: 1,
        owners: vec![
            evm_fork_cache_event_protocol::v1::OwnerInterests {
                owner_id: "bounded".into(),
                interests: Vec::new(),
                backfill: Some(evm_fork_cache_event_protocol::v1::Backfill {
                    from_block: 50,
                    to_block_excl: Some(75),
                    retained_baseline: None,
                }),
                canonical: false,
            },
            evm_fork_cache_event_protocol::v1::OwnerInterests {
                owner_id: "open".into(),
                interests: Vec::new(),
                backfill: Some(evm_fork_cache_event_protocol::v1::Backfill {
                    from_block: 40,
                    to_block_excl: None,
                    retained_baseline: None,
                }),
                canonical: false,
            },
        ],
    };
    let activation_cursor = Cursor {
        chain_id: 1,
        query_revision: 1,
        next_block: 40,
        canonical_head: None,
        batch_sequence: 1,
        provider_checkpoint: b"activation".to_vec(),
        owner_backfill_activation_block: Some(100),
    };
    let mut transport = BatchTransport {
        acknowledged_cursor: Some(activation_cursor),
        ..BatchTransport::default()
    };
    transport.deliveries.extend([
        Delivery {
            session_id: "runtime-a".into(),
            sequence: 2,
            query_revision: 1,
            delivery_token: 2_u64.to_be_bytes().to_vec(),
            cursor: Some(Cursor {
                chain_id: 1,
                query_revision: 1,
                next_block: 75,
                canonical_head: None,
                batch_sequence: 2,
                provider_checkpoint: b"bounded-complete".to_vec(),
                owner_backfill_activation_block: Some(100),
            }),
            payload: Some(delivery::Payload::Data(DataPayload {
                records: vec![EventRecord {
                    event: Some(ChainEvent {
                        event: Some(chain_event::Event::Log(LogEvent {
                            address: vec![0x11; 20],
                            topics: Vec::new(),
                            data: Vec::new(),
                            block_number: 74,
                            block_hash: vec![0x74; 32],
                            transaction_hash: vec![0x44; 32],
                            transaction_index: 0,
                            log_index: 0,
                            block_timestamp: 74,
                            removed: false,
                        })),
                    }),
                    canonical_audience: false,
                    owner_ids: vec!["bounded".into(), "open".into()],
                    scope: evm_fork_cache_event_protocol::v1::DeliveryScope::OwnerCatchup.into(),
                }],
            })),
            checkpoint_neutral: false,
        },
        Delivery {
            session_id: "runtime-a".into(),
            sequence: 3,
            query_revision: 1,
            delivery_token: 3_u64.to_be_bytes().to_vec(),
            cursor: Some(Cursor {
                chain_id: 1,
                query_revision: 1,
                next_block: 100,
                canonical_head: None,
                batch_sequence: 3,
                provider_checkpoint: b"halted-activation-boundary".to_vec(),
                owner_backfill_activation_block: Some(100),
            }),
            payload: Some(delivery::Payload::Barrier(Barrier {
                id: b"source-progress:1:100".to_vec(),
                block: None,
            })),
            checkpoint_neutral: true,
        },
    ]);
    let mut subscriber =
        RemoteSubscriber::new_from_authoritative("runtime-a", 1, transport, Some(desired), 1)
            .expect("restore mid-catch-up");

    let bounded = subscriber.next_batch().await.unwrap().unwrap();
    subscriber
        .acknowledge_delivery(bounded.delivery_token().expect("token").clone())
        .await
        .expect("ack bounded target");
    subscriber
        .add_interest_owner(HandlerId::new("too-early"), &[])
        .await
        .expect_err("the open owner still fences lifecycle mutation");

    assert!(
        subscriber.next_batch().await.unwrap().is_none(),
        "the portable open target is an internally acknowledged neutral barrier"
    );
    subscriber
        .add_interest_owner(HandlerId::new("live"), &[])
        .await
        .expect("all explicit catch-up targets are durable");
    assert!(
        subscriber.transport().applied[0]
            .owners
            .iter()
            .all(|owner| owner.backfill.is_none())
    );
}

#[tokio::test]
async fn remote_rejects_missing_or_mutated_open_backfill_activation_boundaries() {
    let mut subscriber =
        RemoteSubscriber::<_, Ethereum>::new("runtime-a", 1, BatchTransport::default());
    subscriber
        .add_interest_owner_with_backfill(
            HandlerId::new("open"),
            &[],
            SubscriberBackfill::from_block(50),
        )
        .await
        .expect("commit open backfill");

    let activation = |boundary| Delivery {
        session_id: "runtime-a".into(),
        sequence: 1,
        query_revision: 1,
        delivery_token: 1_u64.to_be_bytes().to_vec(),
        cursor: Some(Cursor {
            chain_id: 1,
            query_revision: 1,
            next_block: 50,
            canonical_head: None,
            batch_sequence: 1,
            provider_checkpoint: b"activation".to_vec(),
            owner_backfill_activation_block: boundary,
        }),
        payload: Some(delivery::Payload::Barrier(Barrier {
            id: b"desired-state:1".to_vec(),
            block: None,
        })),
        checkpoint_neutral: true,
    };
    subscriber
        .transport_mut()
        .deliveries
        .push_back(activation(None));
    let missing = subscriber.next_batch().await.expect_err("missing boundary");
    assert!(
        missing
            .to_string()
            .contains("missing its portable boundary")
    );

    subscriber
        .transport_mut()
        .deliveries
        .push_back(activation(Some(100)));
    assert!(
        subscriber.next_batch().await.unwrap().is_none(),
        "valid activation barriers are acknowledged internally"
    );
    subscriber.transport_mut().deliveries.push_back(Delivery {
        session_id: "runtime-a".into(),
        sequence: 2,
        query_revision: 1,
        delivery_token: 2_u64.to_be_bytes().to_vec(),
        cursor: Some(Cursor {
            chain_id: 1,
            query_revision: 1,
            next_block: 60,
            canonical_head: None,
            batch_sequence: 2,
            provider_checkpoint: b"mutated".to_vec(),
            owner_backfill_activation_block: Some(101),
        }),
        payload: Some(delivery::Payload::Barrier(Barrier {
            id: b"source-progress:1:60".to_vec(),
            block: None,
        })),
        checkpoint_neutral: true,
    });
    let changed = subscriber.next_batch().await.expect_err("mutated boundary");
    assert!(changed.to_string().contains("changed within one revision"));
}

#[tokio::test]
async fn remote_accepts_first_global_activation_at_a_retained_baseline() {
    let runtime_baseline = RuntimeBlockRef {
        number: 41,
        hash: B256::repeat_byte(0x41),
        parent_hash: Some(B256::repeat_byte(0x40)),
        timestamp: Some(41),
    };
    let wire_baseline = BlockRef {
        number: 41,
        hash: vec![0x41; 32],
        parent_hash: vec![0x40; 32],
        timestamp: 41,
    };
    let mut subscriber =
        RemoteSubscriber::<_, Ethereum>::new("runtime-a", 1, BatchTransport::default());
    subscriber
        .replace_interest_owners_with_global_backfill(
            vec![(HandlerId::new("logs"), Vec::new())],
            SubscriberBackfill::after_canonical_block(runtime_baseline).expect("C + 1"),
        )
        .await
        .expect("install retained-baseline backfill");
    subscriber.transport_mut().deliveries.push_back(Delivery {
        session_id: "runtime-a".into(),
        sequence: 1,
        query_revision: 1,
        delivery_token: 1_u64.to_be_bytes().to_vec(),
        cursor: Some(Cursor {
            chain_id: 1,
            query_revision: 1,
            next_block: 42,
            canonical_head: Some(wire_baseline),
            batch_sequence: 1,
            provider_checkpoint: b"verified retained baseline".to_vec(),
            owner_backfill_activation_block: Some(100),
        }),
        payload: Some(delivery::Payload::Barrier(Barrier {
            id: b"desired-state:1".to_vec(),
            block: None,
        })),
        checkpoint_neutral: true,
    });

    assert!(
        subscriber
            .next_batch()
            .await
            .expect("activation delivery")
            .is_none(),
        "valid activation barriers are acknowledged internally"
    );
    assert_eq!(subscriber.transport().acknowledgements.len(), 1);
}

#[tokio::test]
async fn remote_delivery_maps_reorg_finality_and_barrier_controls_in_band() {
    let ancestor = BlockRef {
        number: 100,
        hash: vec![0x10; 32],
        parent_hash: vec![0x0f; 32],
        timestamp: 100,
    };
    let old_tip = BlockRef {
        number: 101,
        hash: vec![0x11; 32],
        parent_hash: ancestor.hash.clone(),
        timestamp: 101,
    };
    let new_tip = BlockRef {
        number: 101,
        hash: vec![0x21; 32],
        parent_hash: ancestor.hash.clone(),
        timestamp: 101,
    };
    let mut transport = BatchTransport {
        acknowledged_cursor: Some(cursor(0, old_tip.clone())),
        ..BatchTransport::default()
    };
    transport.deliveries.extend([
        delivery(
            1,
            cursor(1, ancestor.clone()),
            delivery::Payload::Reorg(Reorg {
                common_ancestor: Some(ancestor.clone()),
                old_tip: Some(old_tip),
                new_tip: Some(new_tip.clone()),
            }),
        ),
        delivery(
            2,
            cursor(2, ancestor.clone()),
            delivery::Payload::Finality(Finality {
                kind: FinalityKind::Finalized.into(),
                block: Some(ancestor.clone()),
            }),
        ),
        delivery(
            3,
            cursor(3, ancestor.clone()),
            delivery::Payload::Barrier(Barrier {
                id: b"caught-up".to_vec(),
                block: Some(ancestor),
            }),
        ),
    ]);
    let mut subscriber = RemoteSubscriber::new_from_authoritative(
        "runtime-a",
        1,
        transport,
        Some(ApplyDesiredState {
            protocol_version: evm_fork_cache_event_protocol::PROTOCOL_VERSION,
            session_id: "runtime-a".into(),
            chain_id: 1,
            expected_revision: 0,
            new_revision: 1,
            owners: Vec::new(),
        }),
        1,
    )
    .expect("restore acknowledged reorg authority");

    let reorg = subscriber.next_batch().await.unwrap().unwrap();
    assert!(matches!(
        reorg.chain_controls(),
        [ChainControl::Reorg { .. }]
    ));
    assert_eq!(reorg.chain_id(), Some(1));
    subscriber
        .acknowledge_delivery(reorg.delivery_token().expect("token").clone())
        .await
        .expect("ack reorg");
    let finality = subscriber.next_batch().await.unwrap().unwrap();
    assert!(matches!(
        finality.chain_controls(),
        [ChainControl::Finalized(_)]
    ));
    assert_eq!(finality.chain_id(), Some(1));
    subscriber
        .acknowledge_delivery(finality.delivery_token().expect("token").clone())
        .await
        .expect("ack finality");
    let barrier = subscriber.next_batch().await.unwrap().unwrap();
    assert!(matches!(
        barrier.chain_controls(),
        [ChainControl::Barrier { .. }]
    ));
    assert_eq!(barrier.chain_id(), Some(1));
}

#[tokio::test]
async fn remote_rejects_a_reorg_whose_tips_do_not_name_distinct_branches() {
    let ancestor = BlockRef {
        number: 100,
        hash: vec![0x10; 32],
        parent_hash: vec![0x0f; 32],
        timestamp: 100,
    };
    let old_tip = BlockRef {
        number: 101,
        hash: vec![0x11; 32],
        parent_hash: ancestor.hash.clone(),
        timestamp: 101,
    };
    let mut transport = BatchTransport {
        acknowledged_cursor: Some(cursor(0, old_tip.clone())),
        ..BatchTransport::default()
    };
    transport.deliveries.push_back(delivery(
        1,
        cursor(1, ancestor.clone()),
        delivery::Payload::Reorg(Reorg {
            common_ancestor: Some(ancestor),
            old_tip: Some(old_tip.clone()),
            new_tip: Some(old_tip),
        }),
    ));
    let mut subscriber = RemoteSubscriber::new_from_authoritative(
        "runtime-a",
        1,
        transport,
        Some(ApplyDesiredState {
            protocol_version: evm_fork_cache_event_protocol::PROTOCOL_VERSION,
            session_id: "runtime-a".into(),
            chain_id: 1,
            expected_revision: 0,
            new_revision: 1,
            owners: Vec::new(),
        }),
        1,
    )
    .expect("restore acknowledged authority");

    let error = subscriber
        .next_batch()
        .await
        .expect_err("same-tip no-op is not a reorg");
    assert!(error.to_string().contains("distinct descendant branches"));
}

#[tokio::test]
async fn remote_delivery_rejects_oversized_header_quantities_without_panicking() {
    let header = ConsensusHeader {
        parent_hash: B256::repeat_byte(0x0f),
        number: 1,
        timestamp: 1_700_000_001,
        ..Default::default()
    };
    let block = BlockRef {
        number: 1,
        hash: header.hash_slow().to_vec(),
        parent_hash: vec![0x0f; 32],
        timestamp: 1_700_000_001,
    };
    let data = DataPayload {
        records: vec![EventRecord {
            event: Some(ChainEvent {
                event: Some(chain_event::Event::BlockHeader(BlockHeaderEvent {
                    block: Some(block.clone()),
                    consensus_header_rlp: alloy_rlp::encode(&header),
                    total_difficulty: vec![0xff; 33],
                    size: Vec::new(),
                })),
            }),
            canonical_audience: true,
            owner_ids: Vec::new(),
            scope: evm_fork_cache_event_protocol::v1::DeliveryScope::Canonical.into(),
        }],
    };
    let mut transport = BatchTransport::default();
    transport
        .deliveries
        .push_back(delivery(1, cursor(1, block), delivery::Payload::Data(data)));
    let mut subscriber =
        RemoteSubscriber::<_, Ethereum>::new_at_revision("runtime-a", 1, 1, transport);

    let error = subscriber
        .next_batch()
        .await
        .expect_err("oversized quantity must be rejected");
    assert!(error.to_string().contains("at most 32 bytes"));
}

#[tokio::test]
async fn compact_block_progress_is_a_control_and_never_a_fabricated_header() {
    let block = BlockRef {
        number: 77,
        hash: vec![0x77; 32],
        parent_hash: vec![0x76; 32],
        timestamp: 1_700_000_077,
    };
    let data = DataPayload {
        records: vec![EventRecord {
            event: Some(ChainEvent {
                event: Some(chain_event::Event::BlockProgress(BlockProgressEvent {
                    block: Some(block.clone()),
                })),
            }),
            canonical_audience: true,
            owner_ids: Vec::new(),
            scope: evm_fork_cache_event_protocol::v1::DeliveryScope::CanonicalProgress.into(),
        }],
    };
    let mut transport = BatchTransport::default();
    transport
        .deliveries
        .push_back(delivery(1, cursor(1, block), delivery::Payload::Data(data)));
    let mut subscriber =
        RemoteSubscriber::<_, Ethereum>::new_at_revision("runtime-a", 1, 1, transport);

    let batch = subscriber.next_batch().await.unwrap().unwrap();
    assert!(batch.records().is_empty());
    assert!(matches!(
        batch.chain_controls(),
        [ChainControl::CanonicalProgress(progress)] if progress.number == 77
    ));
}

#[tokio::test]
async fn empty_consensus_rlp_is_rejected_instead_of_fabricating_header_defaults() {
    let block = BlockRef {
        number: 1,
        hash: vec![0x10; 32],
        parent_hash: vec![0x0f; 32],
        timestamp: 1,
    };
    let data = DataPayload {
        records: vec![EventRecord {
            event: Some(ChainEvent {
                event: Some(chain_event::Event::BlockHeader(BlockHeaderEvent {
                    block: Some(block.clone()),
                    consensus_header_rlp: Vec::new(),
                    total_difficulty: Vec::new(),
                    size: Vec::new(),
                })),
            }),
            canonical_audience: true,
            owner_ids: Vec::new(),
            scope: evm_fork_cache_event_protocol::v1::DeliveryScope::Canonical.into(),
        }],
    };
    let mut transport = BatchTransport::default();
    transport
        .deliveries
        .push_back(delivery(1, cursor(1, block), delivery::Payload::Data(data)));
    let mut subscriber =
        RemoteSubscriber::<_, Ethereum>::new_at_revision("runtime-a", 1, 1, transport);

    let error = subscriber.next_batch().await.expect_err("must reject");
    assert!(error.to_string().contains("missing consensus header RLP"));
}

#[tokio::test]
async fn empty_data_delivery_is_rejected_instead_of_advancing_its_checkpoint() {
    let block = BlockRef {
        number: 1,
        hash: vec![0x10; 32],
        parent_hash: vec![0x0f; 32],
        timestamp: 1,
    };
    let mut transport = BatchTransport::default();
    transport.deliveries.push_back(delivery(
        1,
        cursor(1, block),
        delivery::Payload::Data(DataPayload {
            records: Vec::new(),
        }),
    ));
    let mut subscriber =
        RemoteSubscriber::<_, Ethereum>::new_at_revision("runtime-a", 1, 1, transport);

    let error = subscriber
        .next_batch()
        .await
        .expect_err("empty data must not become acknowledgeable");
    assert!(error.to_string().contains("contains no records"));
}

#[tokio::test]
async fn empty_barrier_identifier_is_rejected_instead_of_becoming_a_control() {
    let block = BlockRef {
        number: 1,
        hash: vec![0x10; 32],
        parent_hash: vec![0x0f; 32],
        timestamp: 1,
    };
    let mut transport = BatchTransport::default();
    transport.deliveries.push_back(delivery(
        1,
        cursor(1, block),
        delivery::Payload::Barrier(Barrier {
            id: Vec::new(),
            block: None,
        }),
    ));
    let mut subscriber =
        RemoteSubscriber::<_, Ethereum>::new_at_revision("runtime-a", 1, 1, transport);

    let error = subscriber
        .next_batch()
        .await
        .expect_err("empty barrier id must not become acknowledgeable");
    assert!(error.to_string().contains("barrier identifier is empty"));
}

#[tokio::test]
async fn remote_rejects_a_cursor_head_that_disagrees_with_canonical_progress() {
    let block = BlockRef {
        number: 77,
        hash: vec![0x77; 32],
        parent_hash: vec![0x76; 32],
        timestamp: 1_700_000_077,
    };
    let data = DataPayload {
        records: vec![EventRecord {
            event: Some(ChainEvent {
                event: Some(chain_event::Event::BlockProgress(BlockProgressEvent {
                    block: Some(block.clone()),
                })),
            }),
            canonical_audience: true,
            owner_ids: Vec::new(),
            scope: evm_fork_cache_event_protocol::v1::DeliveryScope::CanonicalProgress.into(),
        }],
    };
    let mut contradictory_cursor = cursor(1, block);
    contradictory_cursor
        .canonical_head
        .as_mut()
        .expect("cursor head")
        .hash = vec![0x88; 32];
    let mut transport = BatchTransport::default();
    transport.deliveries.push_back(delivery(
        1,
        contradictory_cursor,
        delivery::Payload::Data(data),
    ));
    let mut subscriber =
        RemoteSubscriber::<_, Ethereum>::new_at_revision("runtime-a", 1, 1, transport);

    let error = subscriber
        .next_batch()
        .await
        .expect_err("cursor/payload contradiction must not become acknowledgeable");
    assert!(error.to_string().contains("canonical head disagrees"));
}
