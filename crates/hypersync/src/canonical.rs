use std::collections::VecDeque;

use evm_fork_cache_event_protocol::v1::BlockRef;

/// Bounded canonical block history used to locate and apply replacement forks.
#[derive(Clone, Debug)]
pub struct CanonicalTracker {
    depth: usize,
    blocks: VecDeque<BlockRef>,
}

impl CanonicalTracker {
    /// Create a tracker retaining at least one canonical block.
    pub fn new(depth: usize) -> Self {
        Self {
            depth: depth.max(1),
            blocks: VecDeque::new(),
        }
    }

    /// Apply ordered canonical blocks, replacing a conflicting retained suffix.
    ///
    /// # Errors
    ///
    /// Returns [`CanonicalError`] when block references are malformed,
    /// non-contiguous, conflict at the same hash, contain a gap/unknown parent,
    /// or require history older than the retained suffix. Failure leaves the
    /// tracker unchanged.
    pub fn apply_blocks(
        &mut self,
        blocks: impl IntoIterator<Item = BlockRef>,
    ) -> Result<Option<ReorgDelta>, CanonicalError> {
        let blocks: Vec<_> = blocks.into_iter().collect();
        validate_input_order(&blocks)?;
        let mut candidate = self.clone();
        let reorg = candidate.apply_blocks_in_place(blocks)?;
        *self = candidate;
        Ok(reorg)
    }

    fn apply_blocks_in_place(
        &mut self,
        blocks: Vec<BlockRef>,
    ) -> Result<Option<ReorgDelta>, CanonicalError> {
        let old_tip = self.tip().cloned();
        let mut common_ancestor = None;

        for block in blocks {
            validate_block(&block)?;
            if let Some(existing) = self.block(block.number).cloned() {
                if existing.hash == block.hash {
                    if existing != block {
                        return Err(CanonicalError::ConflictingBlockMetadata {
                            number: block.number,
                        });
                    }
                    continue;
                }
                let ancestor_number =
                    block
                        .number
                        .checked_sub(1)
                        .ok_or(CanonicalError::UnknownParent {
                            number: block.number,
                            parent_hash: block.parent_hash.clone(),
                        })?;
                let ancestor = self.block(ancestor_number).cloned().ok_or({
                    CanonicalError::HistoryExhausted {
                        required_block: ancestor_number,
                    }
                })?;
                if ancestor.hash != block.parent_hash {
                    return Err(CanonicalError::UnknownParent {
                        number: block.number,
                        parent_hash: block.parent_hash,
                    });
                }
                if common_ancestor.is_none() {
                    common_ancestor = Some(ancestor.clone());
                }
                while self.tip().is_some_and(|tip| tip.number > ancestor.number) {
                    self.blocks.pop_back();
                }
                self.push(block);
                continue;
            }

            match self.tip() {
                None => self.push(block),
                Some(tip)
                    if block.number == tip.number.saturating_add(1)
                        && block.parent_hash == tip.hash =>
                {
                    self.push(block)
                }
                Some(tip) if block.number > tip.number.saturating_add(1) => {
                    return Err(CanonicalError::Gap {
                        expected: tip.number.saturating_add(1),
                        received: block.number,
                    });
                }
                Some(_) => {
                    return Err(CanonicalError::HistoryExhausted {
                        required_block: block.number,
                    });
                }
            }
        }

        Ok(common_ancestor.map(|common_ancestor| ReorgDelta {
            common_ancestor,
            old_tip: old_tip.expect("a reorg requires existing history"),
            new_tip: self.tip().expect("replacement branch has a tip").clone(),
        }))
    }

    /// Borrow the current canonical tip.
    pub fn tip(&self) -> Option<&BlockRef> {
        self.blocks.back()
    }

    /// Whether no canonical checkpoint has been observed yet.
    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    /// Borrow a retained canonical block by number.
    pub fn block(&self, number: u64) -> Option<&BlockRef> {
        self.blocks.iter().find(|block| block.number == number)
    }

    /// Find a retained canonical block by hash.
    pub fn block_by_hash(&self, hash: &[u8]) -> Option<&BlockRef> {
        self.blocks.iter().find(|block| block.hash == hash)
    }

    /// Borrow retained canonical history in ascending block order.
    pub fn blocks(&self) -> impl ExactSizeIterator<Item = &BlockRef> {
        self.blocks.iter()
    }

    /// Replace retained history from a durable source checkpoint.
    ///
    /// # Errors
    ///
    /// Returns [`CanonicalError`] when the replacement suffix is malformed or
    /// cannot form one contiguous canonical chain. Failure preserves the
    /// previously retained history.
    pub fn restore_blocks(
        &mut self,
        blocks: impl IntoIterator<Item = BlockRef>,
    ) -> Result<(), CanonicalError> {
        let mut replacement = Self::new(self.depth);
        replacement.apply_blocks(blocks)?;
        *self = replacement;
        Ok(())
    }

    fn push(&mut self, block: BlockRef) {
        self.blocks.push_back(block);
        while self.blocks.len() > self.depth {
            self.blocks.pop_front();
        }
    }
}

fn validate_input_order(blocks: &[BlockRef]) -> Result<(), CanonicalError> {
    for pair in blocks.windows(2) {
        let expected = pair[0]
            .number
            .checked_add(1)
            .ok_or(CanonicalError::BlockNumberOverflow)?;
        if pair[1].number != expected {
            return Err(CanonicalError::NonContiguousInput {
                expected,
                received: pair[1].number,
            });
        }
    }
    Ok(())
}

fn validate_block(block: &BlockRef) -> Result<(), CanonicalError> {
    if block.hash.len() != 32 {
        return Err(CanonicalError::InvalidHashWidth {
            field: "hash",
            width: block.hash.len(),
        });
    }
    if block.parent_hash.len() != 32 {
        return Err(CanonicalError::InvalidHashWidth {
            field: "parent_hash",
            width: block.parent_hash.len(),
        });
    }
    Ok(())
}

/// Canonical suffix replaced by a newly observed fork.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReorgDelta {
    /// Last block shared by the old and new branches.
    pub common_ancestor: BlockRef,
    /// Previous canonical tip.
    pub old_tip: BlockRef,
    /// New canonical tip after applying the replacement.
    pub new_tip: BlockRef,
}

/// Canonical block history could not apply an incoming page safely.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum CanonicalError {
    /// A hash that was already retained was returned with different parent or timestamp data.
    #[error("block {number} reused a canonical hash with conflicting metadata")]
    ConflictingBlockMetadata {
        /// Height of the inconsistent block reference.
        number: u64,
    },
    /// Incoming provider or checkpoint blocks were not strictly contiguous and ascending.
    #[error("canonical input is not contiguous: expected {expected}, received {received}")]
    NonContiguousInput {
        /// Required following block number.
        expected: u64,
        /// Returned following block number.
        received: u64,
    },
    /// Incoming blocks skipped a required height.
    #[error("canonical block gap: expected {expected}, received {received}")]
    Gap {
        /// Next required block number.
        expected: u64,
        /// First block received after the gap.
        received: u64,
    },
    /// A replacement branch starts before retained history.
    #[error("canonical history does not retain required block {required_block}")]
    HistoryExhausted {
        /// Block required to locate the common ancestor.
        required_block: u64,
    },
    /// The incoming parent is not present at the preceding retained height.
    #[error("block {number} references unknown parent {parent_hash:?}")]
    UnknownParent {
        /// Incoming block number.
        number: u64,
        /// Incoming parent hash.
        parent_hash: Vec<u8>,
    },
    /// A canonical hash was not 32 bytes.
    #[error("canonical {field} must be 32 bytes, got {width}")]
    InvalidHashWidth {
        /// Invalid field name.
        field: &'static str,
        /// Supplied byte width.
        width: usize,
    },
    /// A canonical sequence attempted to advance beyond the representable block range.
    #[error("canonical block number cannot advance beyond u64::MAX")]
    BlockNumberOverflow,
}
