use std::collections::BTreeMap;

use evm_fork_cache_event_protocol::v1::{ApplyDesiredState, portable_interest};
use hypersync_client::net_types::{BlockField, LogField, LogFilter, Query};

/// Bound one provider response so catch-up remains resumable and deliverable over gRPC.
pub const MAX_BLOCKS_PER_QUERY: usize = 1_000;
/// Bound high-density log responses rather than relying only on a block span.
pub const MAX_LOGS_PER_QUERY: usize = 5_000;
/// Maximum number of distinct provider-side log filters compiled for one revision.
pub const MAX_COMPILED_LOG_FILTERS: usize = 4_096;

type CanonicalLogFilter = (Vec<Vec<u8>>, Vec<Vec<Vec<u8>>>);

/// Compile one committed desired state into a bounded HyperSync query.
///
/// # Errors
///
/// Returns [`QueryPlanError`] for an empty/backward range, invalid address or
/// topic widths, more than four topic positions, too many distinct compiled
/// filters, or a filter rejected by the HyperSync query builder.
pub fn compile_query(
    desired_state: &ApplyDesiredState,
    from_block: u64,
    to_block_excl: Option<u64>,
) -> Result<Query, QueryPlanError> {
    compile_query_with_limits(
        desired_state,
        from_block,
        to_block_excl,
        MAX_BLOCKS_PER_QUERY,
        MAX_LOGS_PER_QUERY,
    )
}

pub(crate) fn compile_query_with_limits(
    desired_state: &ApplyDesiredState,
    from_block: u64,
    to_block_excl: Option<u64>,
    max_blocks: usize,
    max_logs: usize,
) -> Result<Query, QueryPlanError> {
    if let Some(to_block_excl) = to_block_excl
        && to_block_excl <= from_block
    {
        return Err(QueryPlanError::InvalidRange {
            from_block,
            to_block_excl,
        });
    }
    let mut canonical_filters = BTreeMap::new();
    let mut filter_limit_exceeded = false;
    let mut match_all = false;
    let mut explicit_headers = false;

    for owner in &desired_state.owners {
        for interest in &owner.interests {
            match interest.kind.as_ref() {
                Some(portable_interest::Kind::Log(log)) => {
                    if log.topics.len() > 4 {
                        return Err(QueryPlanError::TooManyTopicPositions {
                            owner_id: owner.owner_id.clone(),
                            positions: log.topics.len(),
                        });
                    }
                    validate_log_filter(&owner.owner_id, &log.addresses, &log.topics)?;
                    let key = canonical_log_filter(log.addresses.clone(), &log.topics);
                    if key.0.is_empty() && key.1.is_empty() {
                        match_all = true;
                        canonical_filters.clear();
                        canonical_filters.insert(key, owner.owner_id.clone());
                    } else if !match_all && !canonical_filters.contains_key(&key) {
                        if canonical_filters.len() == MAX_COMPILED_LOG_FILTERS {
                            filter_limit_exceeded = true;
                        } else {
                            canonical_filters.insert(key, owner.owner_id.clone());
                        }
                    }
                }
                Some(portable_interest::Kind::Block(_)) => explicit_headers = true,
                None => {}
            }
        }
    }

    if filter_limit_exceeded && !match_all {
        return Err(QueryPlanError::TooManyCompiledLogFilters {
            limit: MAX_COMPILED_LOG_FILTERS,
        });
    }
    let filters = canonical_filters
        .into_iter()
        .map(|((addresses, topics), owner_id)| {
            let mut filter = LogFilter::all();
            if !addresses.is_empty() {
                filter = filter.and_address(addresses).map_err(|error| {
                    QueryPlanError::InvalidLogFilter {
                        owner_id: owner_id.clone(),
                        message: error.to_string(),
                    }
                })?;
            }
            for (index, values) in topics.into_iter().enumerate() {
                if values.is_empty() {
                    continue;
                }
                let result = match index {
                    0 => filter.and_topic0(values),
                    1 => filter.and_topic1(values),
                    2 => filter.and_topic2(values),
                    3 => filter.and_topic3(values),
                    _ => unreachable!("canonicalized topics are bounded above"),
                };
                filter = result.map_err(|error| QueryPlanError::InvalidLogFilter {
                    owner_id: owner_id.clone(),
                    message: error.to_string(),
                })?;
            }
            Ok(filter)
        })
        .collect::<Result<Vec<_>, QueryPlanError>>()?;

    let mut query = Query::new()
        .from_block(from_block)
        .include_all_blocks()
        .select_log_fields(LogField::all());
    query.max_num_blocks = Some(max_blocks.max(1));
    query.max_num_logs = Some(max_logs.max(1));
    if let Some(to_block_excl) = to_block_excl {
        query = query.to_block_excl(to_block_excl);
    }
    query.logs = filters.into_iter().map(Into::into).collect();
    query = if explicit_headers {
        query.select_block_fields(BlockField::all())
    } else {
        query.select_block_fields([
            BlockField::Number,
            BlockField::Hash,
            BlockField::ParentHash,
            BlockField::Timestamp,
        ])
    };
    Ok(query)
}

fn validate_log_filter(
    owner_id: &str,
    addresses: &[Vec<u8>],
    topics: &[evm_fork_cache_event_protocol::v1::TopicValues],
) -> Result<(), QueryPlanError> {
    if let Some(address) = addresses.iter().find(|address| address.len() != 20) {
        return Err(QueryPlanError::InvalidLogFilter {
            owner_id: owner_id.to_owned(),
            message: format!("address must be 20 bytes, got {}", address.len()),
        });
    }
    if let Some(topic) = topics
        .iter()
        .flat_map(|position| &position.values)
        .find(|topic| topic.len() != 32)
    {
        return Err(QueryPlanError::InvalidLogFilter {
            owner_id: owner_id.to_owned(),
            message: format!("topic must be 32 bytes, got {}", topic.len()),
        });
    }
    Ok(())
}

fn canonical_log_filter(
    mut addresses: Vec<Vec<u8>>,
    topics: &[evm_fork_cache_event_protocol::v1::TopicValues],
) -> CanonicalLogFilter {
    addresses.sort();
    addresses.dedup();
    let mut topics: Vec<Vec<Vec<u8>>> = topics
        .iter()
        .map(|values| {
            let mut values = values.values.clone();
            values.sort();
            values.dedup();
            values
        })
        .collect();
    while topics.last().is_some_and(Vec::is_empty) {
        topics.pop();
    }
    (addresses, topics)
}

/// Failure compiling portable interests into a HyperSync query.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum QueryPlanError {
    /// A bounded range was empty or moved backwards.
    #[error("invalid query range {from_block}..{to_block_excl}")]
    InvalidRange {
        /// Inclusive query start.
        from_block: u64,
        /// Exclusive query end.
        to_block_excl: u64,
    },
    /// A portable address or topic had an invalid byte width.
    #[error("owner `{owner_id}` has an invalid log filter: {message}")]
    InvalidLogFilter {
        /// Owner that supplied the filter.
        owner_id: String,
        /// Conversion failure.
        message: String,
    },
    /// HyperSync supports at most four indexed topic positions.
    #[error("owner `{owner_id}` supplied {positions} topic positions; at most 4 are supported")]
    TooManyTopicPositions {
        /// Owner that supplied the filter.
        owner_id: String,
        /// Number of supplied positions.
        positions: usize,
    },
    /// The desired state expands to more distinct provider filters than allowed.
    #[error("desired state exceeds the provider limit of {limit} distinct log filters")]
    TooManyCompiledLogFilters {
        /// Maximum distinct filters accepted by this adapter.
        limit: usize,
    },
}
