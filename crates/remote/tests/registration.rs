use alloy_network::Ethereum;
use alloy_primitives::{Address, B256};
use alloy_rpc_types_eth::Filter;
use async_trait::async_trait;
use evm_fork_cache::reactive::{
    BlockRef as RuntimeBlockRef, EventSubscriber, HandlerId, InterestOwnerSubscriber, LogInterest,
    ReactiveInterest, RouteKeySpec, SubscriberBackfill, SubscriberCapabilities,
    SubscriberCapability,
};
use evm_fork_cache_event_protocol::v1::{
    Acknowledge, ApplyDesiredState, Backfill, BlockRef, Cursor, Delivery, DesiredStateApplied,
    LogInterest as WireLogInterest, OwnerInterests, PortableInterest, portable_interest,
};
use evm_fork_cache_remote::{RemoteEventTransport, RemoteSubscriber, RemoteTransportError};

#[derive(Default)]
struct RecordingTransport {
    applied: Vec<ApplyDesiredState>,
    fail_apply: bool,
    acknowledged_cursor: Option<Cursor>,
    capabilities: SubscriberCapabilities,
}

#[derive(Default)]
struct CancellationTransport {
    applied: Vec<ApplyDesiredState>,
}

#[tokio::test]
async fn retained_canonical_baseline_survives_remote_desired_state_round_trip() {
    let baseline = RuntimeBlockRef {
        number: 41,
        hash: B256::repeat_byte(0x41),
        parent_hash: Some(B256::repeat_byte(0x40)),
        timestamp: Some(41),
    };
    let mut subscriber =
        RemoteSubscriber::<_, Ethereum>::new("runtime-a", 1, RecordingTransport::default());
    subscriber
        .add_interest_owner_with_backfill(
            HandlerId::new("restored-owner"),
            &log_interest(Address::repeat_byte(0x33)),
            SubscriberBackfill::after_canonical_block(baseline).expect("C + 1"),
        )
        .await
        .expect("register post-baseline catch-up");

    let desired = subscriber.transport().applied[0].clone();
    let wire = desired.owners[0].backfill.as_ref().expect("wire backfill");
    assert_eq!(wire.from_block, 42);
    assert_eq!(wire.to_block_excl, None);
    assert_eq!(
        wire.retained_baseline.as_ref().map(|block| block.number),
        Some(41)
    );
    assert_eq!(
        wire.retained_baseline
            .as_ref()
            .map(|block| block.hash.as_slice()),
        Some(B256::repeat_byte(0x41).as_slice())
    );

    let mut restored = RemoteSubscriber::new_from_authoritative(
        "runtime-a",
        1,
        RecordingTransport::default(),
        Some(desired),
        1,
    )
    .expect("restore exact baseline");
    restored
        .add_interest_owner(
            HandlerId::new("other-owner"),
            &log_interest(Address::repeat_byte(0x44)),
        )
        .await
        .expect_err("restored post-baseline catch-up remains durably fenced");
}

#[test]
fn authoritative_backfill_rejects_a_malformed_retained_baseline() {
    let baseline = BlockRef {
        number: 41,
        hash: vec![0x41; 32],
        parent_hash: vec![0x40; 32],
        timestamp: 41,
    };
    let desired = |baseline: BlockRef| ApplyDesiredState {
        protocol_version: evm_fork_cache_event_protocol::PROTOCOL_VERSION,
        session_id: "runtime-a".into(),
        chain_id: 1,
        expected_revision: 0,
        new_revision: 1,
        owners: vec![OwnerInterests {
            owner_id: "owner-a".into(),
            interests: Vec::new(),
            backfill: Some(Backfill {
                from_block: 42,
                to_block_excl: None,
                retained_baseline: Some(baseline),
            }),
            canonical: false,
        }],
    };

    for malformed in [
        BlockRef {
            number: 40,
            ..baseline.clone()
        },
        BlockRef {
            hash: vec![0x41; 31],
            ..baseline.clone()
        },
        BlockRef {
            parent_hash: vec![0x40; 31],
            ..baseline
        },
    ] {
        RemoteSubscriber::<RecordingTransport, Ethereum>::new_from_authoritative(
            "runtime-a",
            1,
            RecordingTransport::default(),
            Some(desired(malformed)),
            1,
        )
        .err()
        .expect("malformed retained baseline must fail closed");
    }
}

#[test]
fn authoritative_wire_reserves_the_empty_owner_id_for_canonical_state() {
    let desired = |owner: OwnerInterests| ApplyDesiredState {
        protocol_version: evm_fork_cache_event_protocol::PROTOCOL_VERSION,
        session_id: "runtime-a".into(),
        chain_id: 1,
        expected_revision: 0,
        new_revision: 1,
        owners: vec![owner],
    };
    for malformed in [
        OwnerInterests {
            owner_id: String::new(),
            interests: Vec::new(),
            backfill: None,
            canonical: false,
        },
        OwnerInterests {
            owner_id: "named-canonical-owner".into(),
            interests: Vec::new(),
            backfill: None,
            canonical: true,
        },
    ] {
        let error = RemoteSubscriber::<RecordingTransport, Ethereum>::new_from_authoritative(
            "runtime-a",
            1,
            RecordingTransport::default(),
            Some(desired(malformed)),
            1,
        )
        .err()
        .expect("ambiguous canonical/owner identity must fail closed");
        assert!(error.to_string().contains("owner id"));
    }
}

#[tokio::test]
async fn owner_backfill_blocks_a_second_revision_until_cursor_proves_completion() {
    let mut subscriber =
        RemoteSubscriber::<_, Ethereum>::new("runtime-a", 1, RecordingTransport::default());
    subscriber
        .add_interest_owner_with_backfill(
            HandlerId::new("historical-owner"),
            &log_interest(Address::repeat_byte(0x11)),
            SubscriberBackfill::range(100, 200),
        )
        .await
        .expect("backfill registration");
    let error = subscriber
        .add_interest_owner(
            HandlerId::new("live-owner"),
            &log_interest(Address::repeat_byte(0x22)),
        )
        .await
        .expect_err("an incomplete owner catch-up must fence lifecycle mutation");
    assert!(
        error.to_string().contains("backfill"),
        "the rejection should identify the durable catch-up fence: {error}"
    );
    assert_eq!(subscriber.transport().applied.len(), 1);
}

#[tokio::test]
async fn handler_named_base_remains_distinct_from_canonical_interests() {
    let mut subscriber =
        RemoteSubscriber::<_, Ethereum>::new("runtime-a", 1, RecordingTransport::default());
    subscriber
        .register_interests(&log_interest(Address::repeat_byte(0x11)))
        .await
        .expect("register canonical interests");
    let owner = HandlerId::new("$base");
    subscriber
        .add_interest_owner(owner.clone(), &log_interest(Address::repeat_byte(0x22)))
        .await
        .expect("register valid handler identifier");

    let desired = subscriber.transport().applied[1].clone();
    assert_eq!(desired.owners.len(), 2);
    assert!(
        desired
            .owners
            .iter()
            .any(|entry| entry.canonical && entry.owner_id.is_empty())
    );
    assert!(
        desired
            .owners
            .iter()
            .any(|entry| !entry.canonical && entry.owner_id == "$base")
    );

    let restored = RemoteSubscriber::new_from_authoritative(
        "runtime-a",
        1,
        RecordingTransport::default(),
        Some(desired),
        2,
    )
    .expect("hydrate both namespaces");
    assert!(restored.owner_interests(&owner).is_some());
}

#[tokio::test]
async fn authoritative_reconnect_hydrates_and_preserves_existing_owners() {
    let desired = ApplyDesiredState {
        protocol_version: evm_fork_cache_event_protocol::PROTOCOL_VERSION,
        session_id: "runtime-a".into(),
        chain_id: 1,
        expected_revision: 2,
        new_revision: 3,
        owners: vec![OwnerInterests {
            owner_id: "restored-owner".into(),
            interests: vec![PortableInterest {
                kind: Some(portable_interest::Kind::Log(WireLogInterest {
                    addresses: vec![vec![0x33; 20]],
                    topics: Vec::new(),
                })),
            }],
            backfill: Some(Backfill {
                from_block: 10,
                to_block_excl: Some(20),
                retained_baseline: None,
            }),
            canonical: false,
        }],
    };
    let mut subscriber = RemoteSubscriber::new_from_authoritative(
        "runtime-a",
        1,
        RecordingTransport::default(),
        Some(desired),
        3,
    )
    .expect("hydrate authoritative state");
    assert!(
        subscriber
            .owner_interests(&HandlerId::new("restored-owner"))
            .is_some(),
        "the reconnecting subscriber mirrors committed owner registrations"
    );

    let error = subscriber
        .add_interest_owner(
            HandlerId::new("new-owner"),
            &log_interest(Address::repeat_byte(0x44)),
        )
        .await
        .expect_err("authoritative reconnect must retain incomplete catch-up state");
    assert!(
        error.to_string().contains("backfill"),
        "reconnect lifecycle fence should identify the incomplete backfill: {error}"
    );
    assert!(subscriber.transport().applied.is_empty());
}

#[async_trait]
impl RemoteEventTransport for RecordingTransport {
    fn capabilities(&self) -> SubscriberCapabilities {
        self.capabilities.clone()
    }

    async fn apply_desired_state(
        &mut self,
        request: ApplyDesiredState,
    ) -> Result<DesiredStateApplied, RemoteTransportError> {
        if self.fail_apply {
            return Err(RemoteTransportError::Unavailable("forced failure".into()));
        }
        let applied = DesiredStateApplied {
            session_id: request.session_id.clone(),
            revision: request.new_revision,
            activation_sequence: self
                .acknowledged_cursor
                .as_ref()
                .map_or(1, |cursor| cursor.batch_sequence + 1),
        };
        self.applied.push(request);
        Ok(applied)
    }

    async fn next_delivery(&mut self) -> Result<Option<Delivery>, RemoteTransportError> {
        Ok(None)
    }

    async fn acknowledge(
        &mut self,
        _acknowledgement: Acknowledge,
    ) -> Result<(), RemoteTransportError> {
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

#[test]
fn generic_transport_capabilities_reach_the_remote_subscriber() {
    let capabilities = SubscriberCapabilities::new([
        SubscriberCapability::HistoricalBackfill,
        SubscriberCapability::DurableReplay,
        SubscriberCapability::Barriers,
    ]);
    let subscriber = RemoteSubscriber::<_, Ethereum>::new(
        "custom-transport",
        1,
        RecordingTransport {
            capabilities,
            ..Default::default()
        },
    );

    assert!(
        subscriber
            .capabilities()
            .supports(SubscriberCapability::HistoricalBackfill)
    );
    assert!(
        subscriber
            .capabilities()
            .supports(SubscriberCapability::DurableReplay)
    );
    assert!(
        subscriber
            .capabilities()
            .supports(SubscriberCapability::Barriers)
    );
}

#[tokio::test]
async fn authoritative_reconnect_allows_mutation_only_after_durable_completion_proof() {
    let desired = ApplyDesiredState {
        protocol_version: evm_fork_cache_event_protocol::PROTOCOL_VERSION,
        session_id: "runtime-a".into(),
        chain_id: 1,
        expected_revision: 2,
        new_revision: 3,
        owners: vec![OwnerInterests {
            owner_id: "restored-owner".into(),
            interests: vec![PortableInterest {
                kind: Some(portable_interest::Kind::Log(WireLogInterest {
                    addresses: vec![vec![0x33; 20]],
                    topics: Vec::new(),
                })),
            }],
            backfill: Some(Backfill {
                from_block: 10,
                to_block_excl: Some(20),
                retained_baseline: None,
            }),
            canonical: false,
        }],
    };
    let head = BlockRef {
        number: 20,
        hash: vec![0x20; 32],
        parent_hash: vec![0x19; 32],
        timestamp: 20,
    };
    let transport = RecordingTransport {
        acknowledged_cursor: Some(Cursor {
            chain_id: 1,
            query_revision: 3,
            next_block: 21,
            canonical_head: Some(head),
            batch_sequence: 5,
            provider_checkpoint: b"completed".to_vec(),
            owner_backfill_activation_block: None,
        }),
        ..RecordingTransport::default()
    };
    let mut subscriber =
        RemoteSubscriber::new_from_authoritative("runtime-a", 1, transport, Some(desired), 3)
            .expect("hydrate completed authoritative state");

    subscriber
        .add_interest_owner(
            HandlerId::new("new-owner"),
            &log_interest(Address::repeat_byte(0x44)),
        )
        .await
        .expect("durable cursor proves catch-up completion");
    let restored = subscriber.transport().applied[0]
        .owners
        .iter()
        .find(|owner| owner.owner_id == "restored-owner")
        .expect("restored owner remains registered");
    assert!(restored.backfill.is_none());
}

#[tokio::test]
async fn bulk_owner_upsert_uses_one_revision_and_preserves_unrelated_owners() {
    let mut subscriber =
        RemoteSubscriber::<_, Ethereum>::new("runtime-a", 1, RecordingTransport::default());
    subscriber
        .add_interest_owner(
            HandlerId::new("existing"),
            &log_interest(Address::repeat_byte(0x10)),
        )
        .await
        .expect("existing owner");
    subscriber
        .upsert_interest_owners(vec![
            (
                HandlerId::new("owner-a"),
                log_interest(Address::repeat_byte(0x11)),
            ),
            (
                HandlerId::new("owner-b"),
                log_interest(Address::repeat_byte(0x12)),
            ),
        ])
        .await
        .expect("bulk owner upsert");

    assert_eq!(subscriber.transport().applied.len(), 2);
    let request = &subscriber.transport().applied[1];
    assert_eq!(request.expected_revision, 1);
    assert_eq!(request.new_revision, 2);
    assert_eq!(request.owners.len(), 3);
    for owner in ["existing", "owner-a", "owner-b"] {
        assert!(request.owners.iter().any(|entry| entry.owner_id == owner));
    }
}

#[tokio::test]
async fn exact_owner_replacement_removes_base_stale_and_incomplete_backfill_state() {
    let mut subscriber =
        RemoteSubscriber::<_, Ethereum>::new("runtime-a", 1, RecordingTransport::default());
    subscriber
        .register_interests(&log_interest(Address::repeat_byte(0x10)))
        .await
        .expect("base interests");
    subscriber
        .add_interest_owner_with_backfill(
            HandlerId::new("stale"),
            &log_interest(Address::repeat_byte(0x11)),
            SubscriberBackfill::range(10, 20),
        )
        .await
        .expect("stale owner with incomplete backfill");

    subscriber
        .replace_interest_owners(vec![(
            HandlerId::new("fresh"),
            log_interest(Address::repeat_byte(0x22)),
        )])
        .await
        .expect("exact replacement supersedes stale topology");

    let replacement = subscriber.transport().applied.last().expect("replacement");
    assert_eq!(replacement.owners.len(), 1);
    assert_eq!(replacement.owners[0].owner_id, "fresh");
    assert!(!replacement.owners[0].canonical);
    assert!(replacement.owners[0].backfill.is_none());
}

#[tokio::test]
async fn global_replacement_encodes_one_canonical_backfill_with_exact_baseline() {
    let baseline = RuntimeBlockRef {
        number: 41,
        hash: B256::repeat_byte(0x41),
        parent_hash: Some(B256::repeat_byte(0x40)),
        timestamp: Some(1_700_000_041),
    };
    let mut subscriber =
        RemoteSubscriber::<_, Ethereum>::new("runtime-a", 1, RecordingTransport::default());
    subscriber
        .add_interest_owner(
            HandlerId::new("stale"),
            &log_interest(Address::repeat_byte(0x11)),
        )
        .await
        .expect("stale owner");
    subscriber
        .replace_interest_owners_with_global_backfill(
            vec![(
                HandlerId::new("fresh"),
                log_interest(Address::repeat_byte(0x22)),
            )],
            SubscriberBackfill::after_canonical_block(baseline).expect("C + 1"),
        )
        .await
        .expect("global replacement");

    let replacement = subscriber.transport().applied.last().expect("replacement");
    assert_eq!(replacement.owners.len(), 2);
    assert!(
        !replacement
            .owners
            .iter()
            .any(|owner| owner.owner_id == "stale")
    );
    let canonical = replacement
        .owners
        .iter()
        .find(|owner| owner.canonical)
        .expect("canonical backfill entry");
    assert!(canonical.owner_id.is_empty());
    let backfill = canonical.backfill.as_ref().expect("global backfill");
    assert_eq!(backfill.from_block, 42);
    assert_eq!(
        backfill
            .retained_baseline
            .as_ref()
            .map(|block| block.number),
        Some(41)
    );
    let fresh = replacement
        .owners
        .iter()
        .find(|owner| owner.owner_id == "fresh")
        .expect("fresh owner");
    assert!(!fresh.canonical);
    assert!(fresh.backfill.is_none());
}

fn log_interest(address: Address) -> Vec<ReactiveInterest<Ethereum>> {
    vec![ReactiveInterest::Logs(LogInterest {
        provider_filter: Filter::new().address(address),
        local_matcher: None,
        route_key: Some(RouteKeySpec::EmitterAddress),
    })]
}

#[tokio::test]
async fn remote_registration_commits_only_after_authoritative_revision_ack() {
    let mut subscriber =
        RemoteSubscriber::<_, Ethereum>::new("runtime-a", 1, RecordingTransport::default());
    let owner = HandlerId::new("pool-a");
    let original = log_interest(Address::repeat_byte(0x11));

    subscriber
        .add_interest_owner(owner.clone(), &original)
        .await
        .expect("first registration");

    assert_eq!(subscriber.committed_revision(), 1);
    assert_eq!(subscriber.transport().applied[0].expected_revision, 0);
    assert_eq!(subscriber.transport().applied[0].new_revision, 1);
    assert_eq!(
        subscriber.transport().applied[0].owners[0].owner_id,
        "pool-a"
    );

    subscriber.transport_mut().fail_apply = true;
    let replacement = log_interest(Address::repeat_byte(0x22));
    subscriber
        .add_interest_owner(owner.clone(), &replacement)
        .await
        .expect_err("failed service apply must surface");

    assert_eq!(subscriber.committed_revision(), 1);
    let committed = subscriber.owner_interests(&owner).expect("old owner state");
    let ReactiveInterest::Logs(committed) = &committed[0] else {
        panic!("committed interest should remain a log")
    };
    assert!(
        committed
            .provider_filter
            .address
            .contains(&Address::repeat_byte(0x11))
    );
}

#[async_trait]
impl RemoteEventTransport for CancellationTransport {
    async fn apply_desired_state(
        &mut self,
        request: ApplyDesiredState,
    ) -> Result<DesiredStateApplied, RemoteTransportError> {
        self.applied.push(request.clone());
        if self.applied.len() == 1 {
            return std::future::pending().await;
        }
        Ok(DesiredStateApplied {
            session_id: request.session_id,
            revision: request.new_revision,
            activation_sequence: 1,
        })
    }

    async fn next_delivery(&mut self) -> Result<Option<Delivery>, RemoteTransportError> {
        Ok(None)
    }

    async fn acknowledge(
        &mut self,
        _acknowledgement: Acknowledge,
    ) -> Result<(), RemoteTransportError> {
        Ok(())
    }
}

#[tokio::test]
async fn cancelled_registration_reconciles_the_exact_candidate_before_delivery() {
    let mut subscriber =
        RemoteSubscriber::<_, Ethereum>::new("runtime-a", 1, CancellationTransport::default());
    let owner = HandlerId::new("cancelled-owner");
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(10),
            subscriber
                .add_interest_owner(owner.clone(), &log_interest(Address::repeat_byte(0x55)),),
        )
        .await
        .is_err()
    );

    assert!(subscriber.next_batch().await.unwrap().is_none());
    assert_eq!(subscriber.committed_revision(), 1);
    assert!(subscriber.owner_interests(&owner).is_some());
    assert_eq!(subscriber.transport().applied.len(), 2);
    assert_eq!(
        subscriber.transport().applied[0],
        subscriber.transport().applied[1],
        "reconciliation must retry the exact candidate, not a same-revision replacement"
    );
}
