use evm_fork_cache_event_protocol::v1::BlockRef;
use evm_fork_cache_hypersync::{CanonicalError, CanonicalTracker};

fn block(number: u64, hash: u8, parent_hash: u8) -> BlockRef {
    BlockRef {
        number,
        hash: vec![hash; 32],
        parent_hash: vec![parent_hash; 32],
        timestamp: 1_700_000_000 + number,
    }
}

#[test]
fn canonical_tracker_replaces_reorged_suffix_and_reports_common_ancestor() {
    let mut tracker = CanonicalTracker::new(16);
    tracker
        .apply_blocks([
            block(100, 0x10, 0x0f),
            block(101, 0x11, 0x10),
            block(102, 0x12, 0x11),
        ])
        .expect("seed canonical history");

    let reorg = tracker
        .apply_blocks([block(102, 0x22, 0x11), block(103, 0x23, 0x22)])
        .expect("replacement branch should apply")
        .expect("replacement should report a reorg");

    assert_eq!(reorg.common_ancestor, block(101, 0x11, 0x10));
    assert_eq!(reorg.old_tip, block(102, 0x12, 0x11));
    assert_eq!(reorg.new_tip, block(103, 0x23, 0x22));
    assert_eq!(tracker.tip(), Some(&block(103, 0x23, 0x22)));
    assert_eq!(tracker.block(102), Some(&block(102, 0x22, 0x11)));
}

#[test]
fn canonical_retention_supports_a_reorg_at_the_oldest_retained_ancestor() {
    let mut tracker = CanonicalTracker::new(3);
    tracker
        .apply_blocks([
            block(100, 0x10, 0x0f),
            block(101, 0x11, 0x10),
            block(102, 0x12, 0x11),
        ])
        .expect("seed exact retention window");

    let reorg = tracker
        .apply_blocks([block(101, 0x21, 0x10), block(102, 0x22, 0x21)])
        .expect("oldest retained ancestor remains recoverable")
        .expect("replacement reports a reorg");

    assert_eq!(reorg.common_ancestor.number, 100);
    assert_eq!(tracker.tip(), Some(&block(102, 0x22, 0x21)));
}

#[test]
fn canonical_retention_fails_closed_one_block_beyond_its_ancestor_horizon() {
    let mut tracker = CanonicalTracker::new(3);
    tracker
        .apply_blocks([
            block(100, 0x10, 0x0f),
            block(101, 0x11, 0x10),
            block(102, 0x12, 0x11),
            block(103, 0x13, 0x12),
        ])
        .expect("seed and evict block 100");

    let error = tracker
        .apply_blocks([block(101, 0x21, 0x10)])
        .expect_err("ancestor outside retention must fail closed");

    assert_eq!(
        error,
        CanonicalError::HistoryExhausted {
            required_block: 100,
        }
    );
}

#[test]
fn canonical_tracker_rejects_out_of_order_and_duplicate_input_instead_of_sorting_it() {
    let mut tracker = CanonicalTracker::new(4);
    let error = tracker
        .apply_blocks([block(101, 0x11, 0x10), block(100, 0x10, 0x0f)])
        .expect_err("provider order is part of the canonical page contract");
    assert_eq!(
        error,
        CanonicalError::NonContiguousInput {
            expected: 102,
            received: 100,
        }
    );

    let error = tracker
        .apply_blocks([block(100, 0x10, 0x0f), block(100, 0x10, 0x0f)])
        .expect_err("duplicate canonical positions must fail closed");
    assert_eq!(
        error,
        CanonicalError::NonContiguousInput {
            expected: 101,
            received: 100,
        }
    );
}

#[test]
fn failed_restore_leaves_the_previous_canonical_history_intact() {
    let mut tracker = CanonicalTracker::new(4);
    tracker
        .apply_blocks([block(100, 0x10, 0x0f), block(101, 0x11, 0x10)])
        .expect("seed canonical history");

    tracker
        .restore_blocks([block(200, 0x20, 0x1f), block(202, 0x22, 0x21)])
        .expect_err("invalid durable history must not partially replace known-good state");

    assert_eq!(tracker.block(100), Some(&block(100, 0x10, 0x0f)));
    assert_eq!(tracker.tip(), Some(&block(101, 0x11, 0x10)));
}

#[test]
fn canonical_tracker_rejects_conflicting_metadata_for_the_same_block_hash() {
    let mut tracker = CanonicalTracker::new(4);
    tracker
        .apply_blocks([block(100, 0x10, 0x0f)])
        .expect("seed canonical history");

    let mut conflicting = block(100, 0x10, 0x0f);
    conflicting.timestamp += 1;
    let error = tracker
        .apply_blocks([conflicting])
        .expect_err("one hash must identify exactly one complete block reference");

    assert_eq!(
        error,
        CanonicalError::ConflictingBlockMetadata { number: 100 }
    );
    assert_eq!(tracker.tip(), Some(&block(100, 0x10, 0x0f)));
}
