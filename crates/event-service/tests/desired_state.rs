use evm_fork_cache_event_protocol::{
    PROTOCOL_VERSION,
    v1::{
        ApplyDesiredState, Backfill, BlockInterest, BlockMode, BlockRef, OwnerInterests,
        PortableInterest, portable_interest,
    },
};
use evm_fork_cache_event_service::{DesiredStateError, DesiredStateRegistry};

fn request(expected_revision: u64, new_revision: u64) -> ApplyDesiredState {
    ApplyDesiredState {
        protocol_version: PROTOCOL_VERSION,
        session_id: "runtime-a".into(),
        chain_id: 1,
        expected_revision,
        new_revision,
        owners: Vec::new(),
    }
}

#[test]
fn desired_state_compare_and_swap_preserves_committed_revision_on_conflict() {
    let mut registry = DesiredStateRegistry::new();

    let applied = registry
        .apply(request(0, 1))
        .expect("first desired state should commit");
    assert_eq!(applied.revision, 1);

    let error = registry
        .apply(request(0, 2))
        .expect_err("stale expected revision must fail");
    assert_eq!(
        error,
        DesiredStateError::RevisionConflict {
            expected: 0,
            committed: 1,
        }
    );
    assert_eq!(
        registry
            .committed("runtime-a", 1)
            .expect("previous state remains committed")
            .new_revision,
        1
    );
}

#[test]
fn desired_state_rejects_unknown_protocol_version_without_committing() {
    let mut registry = DesiredStateRegistry::new();
    let mut state = request(0, 1);
    state.protocol_version = PROTOCOL_VERSION + 1;

    let error = registry
        .apply(state)
        .expect_err("unknown protocol version must fail");

    assert_eq!(
        error,
        DesiredStateError::ProtocolVersion {
            received: PROTOCOL_VERSION + 1,
            supported: PROTOCOL_VERSION,
        }
    );
    assert!(registry.committed("runtime-a", 1).is_none());
}

#[test]
fn desired_state_rejects_full_block_interest_without_committing() {
    let mut registry = DesiredStateRegistry::new();
    let mut state = request(0, 1);
    state.owners.push(OwnerInterests {
        owner_id: "full-block-consumer".into(),
        interests: vec![PortableInterest {
            kind: Some(portable_interest::Kind::Block(BlockInterest {
                mode: BlockMode::FullBlock.into(),
            })),
        }],
        backfill: None,
        canonical: false,
    });

    let error = registry
        .apply(state)
        .expect_err("full block delivery is outside the initial scope");

    assert_eq!(
        error,
        DesiredStateError::UnsupportedInterest {
            owner_id: "full-block-consumer".into(),
            interest: "full block",
        }
    );
    assert!(registry.committed("runtime-a", 1).is_none());
}

#[test]
fn retained_backfill_baseline_must_be_the_exact_predecessor_with_complete_identity() {
    let mut valid = request(0, 1);
    valid.owners.push(OwnerInterests {
        owner_id: "owner-a".into(),
        interests: Vec::new(),
        backfill: Some(Backfill {
            from_block: 10,
            to_block_excl: None,
            retained_baseline: Some(BlockRef {
                number: 9,
                hash: vec![0x09; 32],
                parent_hash: vec![0x08; 32],
                timestamp: 9,
            }),
        }),
        canonical: false,
    });
    DesiredStateRegistry::new()
        .apply(valid.clone())
        .expect("exact predecessor baseline is portable");

    for mutate in [
        |backfill: &mut Backfill| {
            backfill
                .retained_baseline
                .as_mut()
                .expect("baseline")
                .number = 8;
        },
        |backfill: &mut Backfill| {
            backfill.retained_baseline.as_mut().expect("baseline").hash = vec![0x09; 31];
        },
        |backfill: &mut Backfill| {
            backfill
                .retained_baseline
                .as_mut()
                .expect("baseline")
                .parent_hash = vec![0x08; 31];
        },
    ] {
        let mut malformed = valid.clone();
        mutate(malformed.owners[0].backfill.as_mut().expect("backfill"));
        DesiredStateRegistry::new()
            .apply(malformed)
            .expect_err("malformed retained baseline must fail closed");
    }
}

#[test]
fn desired_state_rejects_revision_wraparound() {
    let error = DesiredStateRegistry::new()
        .apply(request(u64::MAX, 0))
        .expect_err("revision successor arithmetic must not wrap");
    assert!(matches!(error, DesiredStateError::InvalidState(_)));
}

#[test]
fn owner_ids_use_exact_utf8_identity_and_reject_exact_unicode_duplicates() {
    let owner = |owner_id: &str| OwnerInterests {
        owner_id: owner_id.into(),
        interests: Vec::new(),
        backfill: None,
        canonical: false,
    };
    let mut duplicate = request(0, 1);
    duplicate.owners = vec![owner("池"), owner("池")];
    assert!(matches!(
        DesiredStateRegistry::new().apply(duplicate),
        Err(DesiredStateError::InvalidState(_))
    ));

    let mut distinct_scalars = request(0, 1);
    distinct_scalars.owners = vec![owner("é"), owner("e\u{301}")];
    DesiredStateRegistry::new()
        .apply(distinct_scalars)
        .expect("distinct UTF-8 scalar sequences are distinct owner ids");
}
