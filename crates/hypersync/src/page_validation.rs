use std::collections::{HashMap, HashSet};

use hypersync_client::simple_types::Log;

use crate::SourcePage;

/// Validate the provider page against the exact bounded query that produced it.
pub(crate) fn validate_source_page(
    page: &SourcePage,
    from_block: u64,
    to_block_excl: u64,
) -> Result<(), SourcePageError> {
    if page.next_block > to_block_excl {
        return Err(SourcePageError::CursorBeyondTarget {
            next_block: page.next_block,
            to_block_excl,
        });
    }
    if let Some(archive_height) = page.archive_height
        && archive_height < page.next_block
    {
        return Err(SourcePageError::ArchiveBehindCursor {
            archive_height,
            next_block: page.next_block,
        });
    }

    let mut expected_number = from_block;
    let mut previous_hash: Option<&[u8]> = None;
    let mut block_hashes = Vec::with_capacity(page.blocks.len());
    for block in &page.blocks {
        let number = block
            .number
            .ok_or(SourcePageError::MissingField("block.number"))?;
        if number != expected_number {
            return Err(SourcePageError::NonContiguousBlock {
                expected: expected_number,
                received: number,
            });
        }
        let hash = required_width(block.hash.as_ref(), "block.hash", 32)?;
        let parent_hash = required_width(block.parent_hash.as_ref(), "block.parent_hash", 32)?;
        if let Some(previous_hash) = previous_hash
            && parent_hash != previous_hash
        {
            return Err(SourcePageError::ParentHashMismatch { number });
        }
        let timestamp = block
            .timestamp
            .as_ref()
            .ok_or(SourcePageError::MissingField("block.timestamp"))?;
        if timestamp.as_ref().len() > 8 {
            return Err(SourcePageError::QuantityOverflow("block.timestamp"));
        }
        block_hashes.push((number, hash.to_vec()));
        previous_hash = Some(hash);
        expected_number = expected_number
            .checked_add(1)
            .ok_or(SourcePageError::BlockNumberOverflow)?;
    }
    if expected_number != page.next_block {
        return Err(SourcePageError::IncompleteBlockRange {
            expected_next_block: expected_number,
            next_block: page.next_block,
        });
    }
    if let Some(guard) = &page.rollback_guard {
        validate_rollback_guard(guard, &page.blocks)?;
    }

    let mut log_identities = HashSet::with_capacity(page.logs.len());
    let mut transaction_indexes = HashMap::new();
    let mut transaction_hashes = HashMap::new();
    for log in &page.logs {
        validate_log(log, from_block, page.next_block, &block_hashes)?;
        let block_hash = required_width(log.block_hash.as_ref(), "log.block_hash", 32)?;
        let transaction_hash =
            required_width(log.transaction_hash.as_ref(), "log.transaction_hash", 32)?;
        let transaction_index = log
            .transaction_index
            .map(u64::from)
            .ok_or(SourcePageError::MissingField("log.transaction_index"))?;
        let log_index = log
            .log_index
            .map(u64::from)
            .ok_or(SourcePageError::MissingField("log.log_index"))?;
        if !log_identities.insert((block_hash.to_vec(), log_index)) {
            return Err(SourcePageError::DuplicateLog { log_index });
        }
        if transaction_indexes
            .insert(
                (block_hash.to_vec(), transaction_hash.to_vec()),
                transaction_index,
            )
            .is_some_and(|known| known != transaction_index)
            || transaction_hashes
                .insert(
                    (block_hash.to_vec(), transaction_index),
                    transaction_hash.to_vec(),
                )
                .is_some_and(|known| known.as_slice() != transaction_hash)
        {
            return Err(SourcePageError::TransactionIdentityConflict {
                block_number: log
                    .block_number
                    .map(u64::from)
                    .expect("validated log block number"),
            });
        }
    }
    Ok(())
}

fn validate_rollback_guard(
    guard: &hypersync_client::net_types::RollbackGuard,
    blocks: &[hypersync_client::simple_types::Block],
) -> Result<(), SourcePageError> {
    let first = blocks
        .first()
        .ok_or(SourcePageError::MissingRollbackGuardRange)?;
    let last = blocks
        .last()
        .ok_or(SourcePageError::MissingRollbackGuardRange)?;
    let first_number = first
        .number
        .ok_or(SourcePageError::MissingField("block.number"))?;
    let last_number = last
        .number
        .ok_or(SourcePageError::MissingField("block.number"))?;
    if guard.first_block_number != first_number {
        return Err(SourcePageError::RollbackGuardStartMismatch {
            expected: first_number,
            received: guard.first_block_number,
        });
    }
    if guard.block_number != last_number {
        return Err(SourcePageError::RollbackGuardTipMismatch {
            expected: last_number,
            received: guard.block_number,
        });
    }
    let first_parent = required_width(first.parent_hash.as_ref(), "block.parent_hash", 32)?;
    if guard.first_parent_hash.as_ref() != first_parent {
        return Err(SourcePageError::RollbackGuardParentMismatch {
            number: first_number,
        });
    }
    let last_hash = required_width(last.hash.as_ref(), "block.hash", 32)?;
    if guard.hash.as_ref() != last_hash {
        return Err(SourcePageError::RollbackGuardHashMismatch {
            number: last_number,
        });
    }
    let timestamp = quantity_u64(
        last.timestamp
            .as_ref()
            .ok_or(SourcePageError::MissingField("block.timestamp"))?,
        "block.timestamp",
    )?;
    let timestamp = i64::try_from(timestamp).map_err(|_| {
        SourcePageError::RollbackGuardTimestampOutOfRange {
            number: last_number,
        }
    })?;
    if guard.timestamp != timestamp {
        return Err(SourcePageError::RollbackGuardTimestampMismatch {
            number: last_number,
        });
    }
    Ok(())
}

fn validate_log(
    log: &Log,
    from_block: u64,
    next_block: u64,
    block_hashes: &[(u64, Vec<u8>)],
) -> Result<(), SourcePageError> {
    let block_number = log
        .block_number
        .map(u64::from)
        .ok_or(SourcePageError::MissingField("log.block_number"))?;
    if block_number < from_block || block_number >= next_block {
        return Err(SourcePageError::LogOutsidePage {
            block_number,
            from_block,
            next_block,
        });
    }
    let block_hash = required_width(log.block_hash.as_ref(), "log.block_hash", 32)?;
    let expected_hash = block_hashes
        .get((block_number - from_block) as usize)
        .filter(|(number, _)| *number == block_number)
        .map(|(_, hash)| hash.as_slice())
        .ok_or(SourcePageError::LogWithoutBlock { block_number })?;
    if block_hash != expected_hash {
        return Err(SourcePageError::LogBlockHashMismatch { block_number });
    }
    let removed = log
        .removed
        .ok_or(SourcePageError::MissingField("log.removed"))?;
    if removed {
        return Err(SourcePageError::RemovedLogInCanonicalPage { block_number });
    }
    required_width(log.address.as_ref(), "log.address", 20)?;
    required_width(log.transaction_hash.as_ref(), "log.transaction_hash", 32)?;
    log.data
        .as_ref()
        .ok_or(SourcePageError::MissingField("log.data"))?;
    if log.topics.len() > 4 {
        return Err(SourcePageError::TooManyLogTopics {
            received: log.topics.len(),
        });
    }
    let mut saw_missing_topic = false;
    for topic in &log.topics {
        match topic.as_ref() {
            Some(_) if saw_missing_topic => {
                return Err(SourcePageError::NonContiguousLogTopics);
            }
            Some(topic) => {
                required_width(Some(topic), "log.topic", 32)?;
            }
            None => saw_missing_topic = true,
        }
    }
    log.transaction_index
        .ok_or(SourcePageError::MissingField("log.transaction_index"))?;
    log.log_index
        .ok_or(SourcePageError::MissingField("log.log_index"))?;
    Ok(())
}

fn quantity_u64(
    value: &hypersync_client::format::Quantity,
    field: &'static str,
) -> Result<u64, SourcePageError> {
    let bytes = value.as_ref();
    if bytes.len() > 8 {
        return Err(SourcePageError::QuantityOverflow(field));
    }
    let mut buffer = [0_u8; 8];
    buffer[8 - bytes.len()..].copy_from_slice(bytes);
    Ok(u64::from_be_bytes(buffer))
}

fn required_width<'a, T: AsRef<[u8]>>(
    value: Option<&'a T>,
    field: &'static str,
    expected: usize,
) -> Result<&'a [u8], SourcePageError> {
    let value = value.ok_or(SourcePageError::MissingField(field))?.as_ref();
    if value.len() != expected {
        return Err(SourcePageError::InvalidWidth {
            field,
            expected,
            received: value.len(),
        });
    }
    Ok(value)
}

/// A provider page violated the bounded, contiguous source contract.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SourcePageError {
    /// The provider advanced beyond the requested exclusive target.
    #[error("source cursor {next_block} exceeds requested target {to_block_excl}")]
    CursorBeyondTarget {
        /// Returned exclusive cursor.
        next_block: u64,
        /// Requested exclusive target.
        to_block_excl: u64,
    },
    /// The provider's advertised archive height cannot contain the returned page.
    #[error("archive height {archive_height} is behind returned cursor {next_block}")]
    ArchiveBehindCursor {
        /// Provider archive height.
        archive_height: u64,
        /// Returned exclusive cursor.
        next_block: u64,
    },
    /// A required canonical or log identity field was absent.
    #[error("source page is missing required field `{0}`")]
    MissingField(&'static str),
    /// A fixed-width identity field had the wrong size.
    #[error("source field `{field}` must be {expected} bytes, got {received}")]
    InvalidWidth {
        /// Field name.
        field: &'static str,
        /// Required width.
        expected: usize,
        /// Returned width.
        received: usize,
    },
    /// A block number did not immediately follow the previous returned block.
    #[error("source block range is not contiguous: expected {expected}, received {received}")]
    NonContiguousBlock {
        /// Required number.
        expected: u64,
        /// Returned number.
        received: u64,
    },
    /// Adjacent returned blocks did not form one chain.
    #[error("block {number} does not reference the preceding returned block")]
    ParentHashMismatch {
        /// Invalid block number.
        number: u64,
    },
    /// The returned exclusive cursor did not match complete header coverage.
    #[error("source header coverage ends at {expected_next_block}, but cursor is {next_block}")]
    IncompleteBlockRange {
        /// Cursor implied by the returned headers.
        expected_next_block: u64,
        /// Provider cursor.
        next_block: u64,
    },
    /// A rollback guard was present without any returned block range to guard.
    #[error("source rollback guard is present without returned blocks")]
    MissingRollbackGuardRange,
    /// The rollback guard did not start at the first returned block.
    #[error("rollback guard starts at {received}, expected {expected}")]
    RollbackGuardStartMismatch {
        /// First returned block.
        expected: u64,
        /// Guard start.
        received: u64,
    },
    /// The rollback guard did not end at the final returned block.
    #[error("rollback guard ends at {received}, expected {expected}")]
    RollbackGuardTipMismatch {
        /// Last returned block.
        expected: u64,
        /// Guard tip.
        received: u64,
    },
    /// The rollback guard did not preserve the first returned block's parent.
    #[error("rollback guard parent does not match the first returned block {number}")]
    RollbackGuardParentMismatch {
        /// First returned block.
        number: u64,
    },
    /// The rollback guard tip hash did not match the final returned block.
    #[error("rollback guard hash does not match the final returned block {number}")]
    RollbackGuardHashMismatch {
        /// Final returned block.
        number: u64,
    },
    /// The rollback guard timestamp did not match the final returned block.
    #[error("rollback guard timestamp does not match the final returned block {number}")]
    RollbackGuardTimestampMismatch {
        /// Final returned block.
        number: u64,
    },
    /// A block timestamp could not be represented by HyperSync's signed guard.
    #[error("block {number} timestamp does not fit the rollback guard")]
    RollbackGuardTimestampOutOfRange {
        /// Final returned block.
        number: u64,
    },
    /// A log was outside the page's proven canonical range.
    #[error("log block {block_number} is outside page {from_block}..{next_block}")]
    LogOutsidePage {
        /// Log block.
        block_number: u64,
        /// Query start.
        from_block: u64,
        /// Page cursor.
        next_block: u64,
    },
    /// A log did not have a corresponding returned header.
    #[error("log block {block_number} has no corresponding header")]
    LogWithoutBlock {
        /// Log block.
        block_number: u64,
    },
    /// A log claimed a hash other than its returned header.
    #[error("log block hash does not match header at block {block_number}")]
    LogBlockHashMismatch {
        /// Log block.
        block_number: u64,
    },
    /// A historical row claimed removal while matching the page's current canonical header.
    #[error("removed log at block {block_number} contradicts the canonical historical page")]
    RemovedLogInCanonicalPage {
        /// Contradictory log block.
        block_number: u64,
    },
    /// A stable log identity occurred more than once in the page.
    #[error("source page contains duplicate log identity at log index {log_index}")]
    DuplicateLog {
        /// Duplicate log index.
        log_index: u64,
    },
    /// Logs disagree on transaction hash/index identity within one block.
    #[error("source logs conflict on transaction identity in block {block_number}")]
    TransactionIdentityConflict {
        /// Block containing the contradictory transaction identity.
        block_number: u64,
    },
    /// An EVM log contained more than four indexed topics.
    #[error("source log contains {received} topics; EVM logs support at most 4")]
    TooManyLogTopics {
        /// Returned topic count.
        received: usize,
    },
    /// A later topic was present after an earlier topic position was absent.
    #[error("source log topics are not a contiguous prefix")]
    NonContiguousLogTopics,
    /// A source quantity could not fit the protocol's fixed width.
    #[error("source quantity `{0}` does not fit in u64")]
    QuantityOverflow(&'static str),
    /// Header iteration could not represent the following block.
    #[error("source block number cannot advance beyond u64::MAX")]
    BlockNumberOverflow,
}

#[cfg(test)]
mod tests {
    use hypersync_client::{
        format::{Address, Data, Hash, Quantity},
        simple_types::{Block, Log},
    };

    use super::*;

    fn block() -> Block {
        Block {
            number: Some(100),
            hash: Some(Hash::from([0x10; 32])),
            parent_hash: Some(Hash::from([0x0f; 32])),
            timestamp: Some(Quantity::from(100_u64)),
            ..Default::default()
        }
    }

    fn log(transaction_hash: u8, transaction_index: u64, log_index: u64) -> Log {
        Log {
            removed: Some(false),
            log_index: Some(log_index.into()),
            transaction_index: Some(transaction_index.into()),
            transaction_hash: Some(Hash::from([transaction_hash; 32])),
            block_hash: Some(Hash::from([0x10; 32])),
            block_number: Some(100_u64.into()),
            address: Some(Address::from([0x33; 20])),
            data: Some(Data::from(Vec::new())),
            ..Default::default()
        }
    }

    fn page(logs: Vec<Log>) -> SourcePage {
        SourcePage::new(101, vec![block()], logs).with_archive_height(Some(101))
    }

    #[test]
    fn source_page_rejects_ambiguous_log_and_transaction_identities() {
        assert!(matches!(
            validate_source_page(&page(vec![log(0x20, 0, 0), log(0x20, 1, 1)]), 100, 101),
            Err(SourcePageError::TransactionIdentityConflict { block_number: 100 })
        ));
        assert!(matches!(
            validate_source_page(&page(vec![log(0x20, 0, 0), log(0x21, 0, 1)]), 100, 101),
            Err(SourcePageError::TransactionIdentityConflict { block_number: 100 })
        ));
        assert!(matches!(
            validate_source_page(&page(vec![log(0x20, 0, 0), log(0x21, 1, 0)]), 100, 101),
            Err(SourcePageError::DuplicateLog { log_index: 0 })
        ));
    }

    #[test]
    fn source_page_rejects_removed_logs_from_a_canonical_page() {
        let mut removed = log(0x20, 0, 0);
        removed.removed = Some(true);
        assert!(matches!(
            validate_source_page(&page(vec![removed]), 100, 101),
            Err(SourcePageError::RemovedLogInCanonicalPage { block_number: 100 })
        ));
    }
}
