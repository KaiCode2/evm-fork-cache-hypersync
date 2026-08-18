use std::collections::HashMap;

use alloy_consensus::Header as ConsensusHeader;
use alloy_primitives::{Address as AlloyAddress, B64, B256, Bloom, Bytes, U256};
use alloy_rlp::Encodable;
use evm_fork_cache_event_protocol::v1::{
    ApplyDesiredState, BlockHeaderEvent, BlockProgressEvent, BlockRef, ChainEvent, Cursor,
    DataPayload, Delivery, DeliveryScope, EventRecord, LogEvent, PortableInterest, chain_event,
    delivery, portable_interest,
};
use hypersync_client::{
    format::{Hash, Quantity},
    net_types::RollbackGuard,
    simple_types::{Block, Log},
};

const SOURCE_CHECKPOINT_MAGIC: &[u8; 8] = b"EFCHSCP2";
const ENCODED_BLOCK_REF_LEN: usize = 80;

#[derive(Clone, Default)]
pub(crate) struct DecodedSourceCheckpoint {
    pub rollback_guard: Option<RollbackGuard>,
    pub activation_block: Option<u64>,
    pub canonical_blocks: Vec<BlockRef>,
}

/// Provider-neutral page returned by a chain data source.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct SourcePage {
    /// Height available in the source archive when the query ran.
    pub archive_height: Option<u64>,
    /// Exclusive block cursor for the next page.
    pub next_block: u64,
    /// Blocks returned for the covered range.
    pub blocks: Vec<Block>,
    /// Logs returned for the covered range.
    pub logs: Vec<Log>,
    /// HyperSync continuity metadata for this page.
    pub rollback_guard: Option<RollbackGuard>,
}

impl SourcePage {
    /// Create a page with no archive-height or provider-checkpoint metadata.
    pub fn new(next_block: u64, blocks: Vec<Block>, logs: Vec<Log>) -> Self {
        Self {
            archive_height: None,
            next_block,
            blocks,
            logs,
            rollback_guard: None,
        }
    }

    /// Attach the source's latest-height observation from the same response.
    pub fn with_archive_height(mut self, archive_height: Option<u64>) -> Self {
        self.archive_height = archive_height;
        self
    }

    /// Attach provider-native continuity metadata from the same response.
    pub fn with_rollback_guard(mut self, rollback_guard: Option<RollbackGuard>) -> Self {
        self.rollback_guard = rollback_guard;
        self
    }
}

/// Normalize an already-admitted provider page into one ordered wire delivery.
///
/// # Integrity boundary
///
/// This low-level helper deliberately does **not** validate the requested range,
/// canonical continuity, duplicate identities, or hard response limits. It is
/// exposed for deterministic tooling and benchmarks whose fixtures are already
/// trusted. Production adapters must route untrusted provider data through
/// [`crate::SourceEngine`], which performs hard admission and structural
/// validation before calling the same normalization implementation.
///
/// # Errors
///
/// Returns [`NormalizeError`] when required block/log fields are missing,
/// malformed, overflow their protocol representation, cannot be encoded into a
/// valid delivery/cursor, or when a complete reconstructed header does not
/// reproduce the provider's block hash.
pub fn normalize_page_unchecked(
    desired_state: &ApplyDesiredState,
    sequence: u64,
    page: SourcePage,
) -> Result<Delivery, NormalizeError> {
    normalize_page_at(desired_state, sequence, page, 0)
}

pub(crate) fn normalize_page_at(
    desired_state: &ApplyDesiredState,
    sequence: u64,
    mut page: SourcePage,
    activation_block: u64,
) -> Result<Delivery, NormalizeError> {
    page.blocks.sort_by_key(|block| block.number);
    page.logs.sort_by_key(|log| {
        (
            log.block_number.map(u64::from),
            log.transaction_index.map(u64::from),
            log.log_index.map(u64::from),
        )
    });

    let mut events = Vec::with_capacity(page.blocks.len() + page.logs.len());
    let mut block_timestamps = HashMap::with_capacity(page.blocks.len());
    for block in &page.blocks {
        let block_ref = block_ref(block)?;
        let block_number = block_ref.number;
        block_timestamps.insert(block_number, block_ref.timestamp);
        if has_active_block_interest(desired_state, block_number, activation_block) {
            events.push((
                block_number,
                0_u8,
                0_u64,
                0_u64,
                ChainEvent {
                    event: Some(chain_event::Event::BlockHeader(BlockHeaderEvent {
                        block: Some(block_ref.clone()),
                        consensus_header_rlp: encode_consensus_header(block)?,
                        total_difficulty: block
                            .total_difficulty
                            .as_ref()
                            .map_or_else(Vec::new, |value| value.as_ref().to_vec()),
                        size: block
                            .size
                            .as_ref()
                            .map_or_else(Vec::new, |value| value.as_ref().to_vec()),
                    })),
                },
                delivery_scope(desired_state, block_number, activation_block),
            ));
        }
        if block_number >= activation_block || global_backfill_active(desired_state, block_number) {
            events.push((
                block_number,
                2_u8,
                0_u64,
                0_u64,
                ChainEvent {
                    event: Some(chain_event::Event::BlockProgress(BlockProgressEvent {
                        block: Some(block_ref),
                    })),
                },
                DeliveryScope::CanonicalProgress,
            ));
        }
    }

    for log in page.logs {
        let block_number = required_u64(log.block_number, "log.block_number")?;
        let transaction_index = required_u64(log.transaction_index, "log.transaction_index")?;
        let log_index = required_u64(log.log_index, "log.log_index")?;
        let address = required_fixed::<20, _>(log.address.as_ref(), "log.address")?.to_vec();
        let block_hash =
            required_fixed::<32, _>(log.block_hash.as_ref(), "log.block_hash")?.to_vec();
        let transaction_hash =
            required_fixed::<32, _>(log.transaction_hash.as_ref(), "log.transaction_hash")?
                .to_vec();
        let data = required_bytes(log.data.as_ref(), "log.data")?;
        let removed = log
            .removed
            .ok_or(NormalizeError::MissingField("log.removed"))?;
        let topics = normalize_topics(&log.topics)?;
        let block_timestamp = block_timestamps
            .get(&block_number)
            .copied()
            .ok_or(NormalizeError::LogWithoutBlock { block_number })?;
        events.push((
            block_number,
            1_u8,
            transaction_index,
            log_index,
            ChainEvent {
                event: Some(chain_event::Event::Log(LogEvent {
                    address,
                    topics,
                    data,
                    block_number,
                    block_hash,
                    transaction_hash,
                    transaction_index,
                    log_index,
                    block_timestamp,
                    removed,
                })),
            },
            delivery_scope(desired_state, block_number, activation_block),
        ));
    }
    events.sort_by_key(|(block, kind, transaction, log, _, _)| (*block, *kind, *transaction, *log));

    // `next_block` and the provider checkpoint describe this source's scan
    // position. They may advance during an owner-specific historical catch-up.
    // Global canonical coverage is narrower: only blocks at or beyond this
    // revision's activation boundary are visible to the canonical audience.
    let canonical_head = page
        .blocks
        .iter()
        .rev()
        .find(|block| {
            block.number.is_some_and(|number| {
                number >= activation_block || global_backfill_active(desired_state, number)
            })
        })
        .map(block_ref)
        .transpose()?;
    let provider_checkpoint = page
        .rollback_guard
        .as_ref()
        .map_or_else(Vec::new, encode_rollback_guard);
    let mut records: Vec<EventRecord> = events
        .into_iter()
        .filter_map(|(_, _, _, _, event, scope)| {
            let (canonical_audience, owner_ids) =
                event_audience(desired_state, &event, activation_block);
            (canonical_audience || !owner_ids.is_empty()).then_some(EventRecord {
                event: Some(event),
                canonical_audience,
                owner_ids,
                scope: scope.into(),
            })
        })
        .collect();
    if let Some(head) = canonical_head.as_ref()
        && !records.is_empty()
        && !records.iter().any(|record| {
            record.canonical_audience
                && record
                    .event
                    .as_ref()
                    .and_then(|event| event.event.as_ref())
                    .and_then(|event| match event {
                        chain_event::Event::BlockHeader(header) => header.block.as_ref(),
                        chain_event::Event::BlockProgress(progress) => progress.block.as_ref(),
                        chain_event::Event::Log(_) => None,
                    })
                    .is_some_and(|block| block == head)
        })
    {
        records.push(EventRecord {
            event: Some(ChainEvent {
                event: Some(chain_event::Event::BlockProgress(BlockProgressEvent {
                    block: Some(head.clone()),
                })),
            }),
            canonical_audience: true,
            owner_ids: Vec::new(),
            scope: DeliveryScope::CanonicalProgress.into(),
        });
    }

    Ok(Delivery {
        session_id: desired_state.session_id.clone(),
        sequence,
        query_revision: desired_state.new_revision,
        delivery_token: sequence.to_be_bytes().to_vec(),
        cursor: Some(Cursor {
            chain_id: desired_state.chain_id,
            query_revision: desired_state.new_revision,
            next_block: page.next_block,
            canonical_head,
            batch_sequence: sequence,
            provider_checkpoint,
            owner_backfill_activation_block: Some(activation_block),
        }),
        payload: Some(delivery::Payload::Data(DataPayload { records })),
        checkpoint_neutral: false,
    })
}

fn delivery_scope(
    desired_state: &ApplyDesiredState,
    block_number: u64,
    activation_block: u64,
) -> DeliveryScope {
    if block_number >= activation_block || global_backfill_active(desired_state, block_number) {
        DeliveryScope::CanonicalProgress
    } else {
        DeliveryScope::OwnerCatchup
    }
}

fn has_active_block_interest(
    desired_state: &ApplyDesiredState,
    block_number: u64,
    activation_block: u64,
) -> bool {
    let global_backfill = global_backfill_active(desired_state, block_number);
    desired_state.owners.iter().any(|owner| {
        let active = global_backfill
            || block_number >= activation_block
            || owner.backfill.as_ref().is_some_and(|backfill| {
                block_number >= backfill.from_block
                    && backfill.to_block_excl.is_none_or(|end| block_number < end)
            });
        active
            && owner.interests.iter().any(|interest| {
                matches!(
                    interest.kind.as_ref(),
                    Some(portable_interest::Kind::Block(_))
                )
            })
    })
}

fn event_audience(
    desired_state: &ApplyDesiredState,
    event: &ChainEvent,
    activation_block: u64,
) -> (bool, Vec<String>) {
    let mut canonical = false;
    let mut owners = Vec::new();
    let block_number = event_block_number(event);
    let canonical_phase = block_number >= activation_block;
    let global_backfill = global_backfill_active(desired_state, block_number);
    if matches!(
        event.event.as_ref(),
        Some(chain_event::Event::BlockProgress(_))
    ) && (canonical_phase || global_backfill)
    {
        return (true, Vec::new());
    }
    for owner in &desired_state.owners {
        let active = global_backfill
            || canonical_phase
            || owner.backfill.as_ref().is_some_and(|backfill| {
                block_number >= backfill.from_block
                    && backfill.to_block_excl.is_none_or(|end| block_number < end)
            });
        if !active {
            continue;
        }
        if owner
            .interests
            .iter()
            .any(|interest| interest_matches_event(interest, event))
        {
            if global_backfill || (owner.canonical && canonical_phase) {
                canonical = true;
            } else {
                owners.push(owner.owner_id.clone());
            }
        }
    }
    owners.sort();
    owners.dedup();
    if canonical {
        owners.clear();
    }
    (canonical, owners)
}

fn global_backfill_active(desired_state: &ApplyDesiredState, block_number: u64) -> bool {
    desired_state.owners.iter().any(|owner| {
        owner.canonical
            && owner.backfill.as_ref().is_some_and(|backfill| {
                block_number >= backfill.from_block
                    && backfill.to_block_excl.is_none_or(|end| block_number < end)
            })
    })
}

fn event_block_number(event: &ChainEvent) -> u64 {
    match event.event.as_ref() {
        Some(chain_event::Event::Log(log)) => log.block_number,
        Some(chain_event::Event::BlockHeader(header)) => {
            header.block.as_ref().map_or(0, |block| block.number)
        }
        Some(chain_event::Event::BlockProgress(progress)) => {
            progress.block.as_ref().map_or(0, |block| block.number)
        }
        None => 0,
    }
}

fn interest_matches_event(interest: &PortableInterest, event: &ChainEvent) -> bool {
    match (&interest.kind, &event.event) {
        (Some(portable_interest::Kind::Block(_)), Some(chain_event::Event::BlockHeader(_))) => true,
        (Some(portable_interest::Kind::Log(interest)), Some(chain_event::Event::Log(log))) => {
            (interest.addresses.is_empty() || interest.addresses.contains(&log.address))
                && interest.topics.iter().enumerate().all(|(index, accepted)| {
                    accepted.values.is_empty()
                        || log
                            .topics
                            .get(index)
                            .is_some_and(|topic| accepted.values.contains(topic))
                })
        }
        _ => false,
    }
}

pub(crate) fn encode_rollback_guard(guard: &RollbackGuard) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(88);
    encoded.extend_from_slice(&guard.block_number.to_be_bytes());
    encoded.extend_from_slice(guard.hash.as_ref());
    encoded.extend_from_slice(&guard.timestamp.to_be_bytes());
    encoded.extend_from_slice(&guard.first_block_number.to_be_bytes());
    encoded.extend_from_slice(guard.first_parent_hash.as_ref());
    encoded
}

pub(crate) fn decode_rollback_guard(
    encoded: &[u8],
) -> Result<Option<RollbackGuard>, NormalizeError> {
    if encoded.is_empty() {
        return Ok(None);
    }
    if encoded.len() != 88 {
        return Err(NormalizeError::CheckpointWidth(encoded.len()));
    }
    let block_number = u64::from_be_bytes(encoded[0..8].try_into().expect("fixed slice"));
    let hash = Hash::from(<[u8; 32]>::try_from(&encoded[8..40]).expect("fixed slice"));
    let timestamp = i64::from_be_bytes(encoded[40..48].try_into().expect("fixed slice"));
    let first_block_number = u64::from_be_bytes(encoded[48..56].try_into().expect("fixed slice"));
    let first_parent_hash =
        Hash::from(<[u8; 32]>::try_from(&encoded[56..88]).expect("fixed slice"));
    Ok(Some(RollbackGuard {
        block_number,
        timestamp,
        hash,
        first_block_number,
        first_parent_hash,
    }))
}

pub(crate) fn encode_source_checkpoint<'a>(
    guard: Option<&RollbackGuard>,
    activation_block: u64,
    blocks: impl ExactSizeIterator<Item = &'a BlockRef>,
) -> Result<Vec<u8>, NormalizeError> {
    let block_count = u32::try_from(blocks.len())
        .map_err(|_| NormalizeError::InvalidCheckpoint("canonical history is too large"))?;
    let mut encoded = Vec::with_capacity(
        SOURCE_CHECKPOINT_MAGIC.len()
            + 1
            + usize::from(guard.is_some()) * 88
            + 8
            + 4
            + block_count as usize * ENCODED_BLOCK_REF_LEN,
    );
    encoded.extend_from_slice(SOURCE_CHECKPOINT_MAGIC);
    encoded.push(u8::from(guard.is_some()));
    if let Some(guard) = guard {
        encoded.extend_from_slice(&encode_rollback_guard(guard));
    }
    encoded.extend_from_slice(&activation_block.to_be_bytes());
    encoded.extend_from_slice(&block_count.to_be_bytes());
    for block in blocks {
        if block.hash.len() != 32 || block.parent_hash.len() != 32 {
            return Err(NormalizeError::InvalidCheckpoint(
                "canonical history contains an invalid hash width",
            ));
        }
        encoded.extend_from_slice(&block.number.to_be_bytes());
        encoded.extend_from_slice(&block.hash);
        encoded.extend_from_slice(&block.parent_hash);
        encoded.extend_from_slice(&block.timestamp.to_be_bytes());
    }
    Ok(encoded)
}

pub(crate) fn decode_source_checkpoint(
    encoded: &[u8],
) -> Result<DecodedSourceCheckpoint, NormalizeError> {
    if encoded.is_empty() {
        return Ok(DecodedSourceCheckpoint::default());
    }
    if encoded.len() == 88 {
        return Ok(DecodedSourceCheckpoint {
            rollback_guard: decode_rollback_guard(encoded)?,
            ..DecodedSourceCheckpoint::default()
        });
    }
    if encoded.len() < SOURCE_CHECKPOINT_MAGIC.len() + 1 + 4
        || &encoded[..SOURCE_CHECKPOINT_MAGIC.len()] != SOURCE_CHECKPOINT_MAGIC
    {
        return Err(NormalizeError::InvalidCheckpoint(
            "unknown checkpoint encoding",
        ));
    }
    let mut offset = SOURCE_CHECKPOINT_MAGIC.len();
    let guard = match encoded[offset] {
        0 => {
            offset += 1;
            None
        }
        1 => {
            offset += 1;
            let end = offset.saturating_add(88);
            let guard = encoded
                .get(offset..end)
                .ok_or(NormalizeError::InvalidCheckpoint(
                    "truncated rollback guard",
                ))?;
            offset = end;
            decode_rollback_guard(guard)?
        }
        _ => {
            return Err(NormalizeError::InvalidCheckpoint(
                "invalid rollback-guard tag",
            ));
        }
    };
    let activation_block = read_u64(encoded, &mut offset)?;
    let count_end = offset.saturating_add(4);
    let count = u32::from_be_bytes(
        encoded
            .get(offset..count_end)
            .ok_or(NormalizeError::InvalidCheckpoint(
                "missing canonical-history count",
            ))?
            .try_into()
            .map_err(|_| NormalizeError::InvalidCheckpoint("invalid history count"))?,
    ) as usize;
    offset = count_end;
    let expected = offset.saturating_add(count.saturating_mul(ENCODED_BLOCK_REF_LEN));
    if encoded.len() != expected {
        return Err(NormalizeError::InvalidCheckpoint(
            "canonical-history length does not match its count",
        ));
    }
    let mut blocks = Vec::with_capacity(count);
    for _ in 0..count {
        let number = read_u64(encoded, &mut offset)?;
        let hash = read_fixed(encoded, &mut offset, 32)?.to_vec();
        let parent_hash = read_fixed(encoded, &mut offset, 32)?.to_vec();
        let timestamp = read_u64(encoded, &mut offset)?;
        blocks.push(BlockRef {
            number,
            hash,
            parent_hash,
            timestamp,
        });
    }
    Ok(DecodedSourceCheckpoint {
        rollback_guard: guard,
        activation_block: Some(activation_block),
        canonical_blocks: blocks,
    })
}

fn read_u64(encoded: &[u8], offset: &mut usize) -> Result<u64, NormalizeError> {
    Ok(u64::from_be_bytes(
        read_fixed(encoded, offset, 8)?
            .try_into()
            .map_err(|_| NormalizeError::InvalidCheckpoint("invalid integer width"))?,
    ))
}

fn read_fixed<'a>(
    encoded: &'a [u8],
    offset: &mut usize,
    width: usize,
) -> Result<&'a [u8], NormalizeError> {
    let end = offset.saturating_add(width);
    let bytes = encoded
        .get(*offset..end)
        .ok_or(NormalizeError::InvalidCheckpoint("truncated checkpoint"))?;
    *offset = end;
    Ok(bytes)
}

pub(crate) fn block_ref(block: &Block) -> Result<BlockRef, NormalizeError> {
    Ok(BlockRef {
        number: block
            .number
            .ok_or(NormalizeError::MissingField("block.number"))?,
        hash: required_fixed::<32, _>(block.hash.as_ref(), "block.hash")?.to_vec(),
        parent_hash: required_fixed::<32, _>(block.parent_hash.as_ref(), "block.parent_hash")?
            .to_vec(),
        timestamp: block
            .timestamp
            .as_ref()
            .map(quantity_to_u64)
            .transpose()?
            .ok_or(NormalizeError::MissingField("block.timestamp"))?,
    })
}

fn normalize_topics(
    topics: &[Option<hypersync_client::format::LogArgument>],
) -> Result<Vec<Vec<u8>>, NormalizeError> {
    let mut normalized = Vec::with_capacity(topics.len());
    let mut saw_missing = false;
    for topic in topics {
        match topic.as_ref() {
            Some(_) if saw_missing => return Err(NormalizeError::NonContiguousLogTopics),
            Some(topic) => {
                normalized.push(required_fixed::<32, _>(Some(topic), "log.topic")?.to_vec())
            }
            None => saw_missing = true,
        }
    }
    Ok(normalized)
}

fn encode_consensus_header(block: &Block) -> Result<Vec<u8>, NormalizeError> {
    let extra_data = block
        .extra_data
        .as_ref()
        .ok_or(NormalizeError::MissingField("block.extra_data"))?
        .as_ref();
    if extra_data.len() > 32 {
        return Err(NormalizeError::InvalidField(
            "block.extra_data exceeds 32 bytes",
        ));
    }
    let header = ConsensusHeader {
        parent_hash: B256::from(required_fixed(
            block.parent_hash.as_ref(),
            "block.parent_hash",
        )?),
        ommers_hash: B256::from(required_fixed(
            block.sha3_uncles.as_ref(),
            "block.sha3_uncles",
        )?),
        beneficiary: AlloyAddress::from(required_fixed(block.miner.as_ref(), "block.miner")?),
        state_root: B256::from(required_fixed(
            block.state_root.as_ref(),
            "block.state_root",
        )?),
        transactions_root: B256::from(required_fixed(
            block.transactions_root.as_ref(),
            "block.transactions_root",
        )?),
        receipts_root: B256::from(required_fixed(
            block.receipts_root.as_ref(),
            "block.receipts_root",
        )?),
        logs_bloom: Bloom::from(required_fixed(
            block.logs_bloom.as_ref(),
            "block.logs_bloom",
        )?),
        difficulty: quantity_to_u256(
            block
                .difficulty
                .as_ref()
                .ok_or(NormalizeError::MissingField("block.difficulty"))?,
            "block.difficulty",
        )?,
        number: block
            .number
            .ok_or(NormalizeError::MissingField("block.number"))?,
        gas_limit: required_quantity_u64(block.gas_limit.as_ref(), "block.gas_limit")?,
        gas_used: required_quantity_u64(block.gas_used.as_ref(), "block.gas_used")?,
        timestamp: required_quantity_u64(block.timestamp.as_ref(), "block.timestamp")?,
        extra_data: Bytes::copy_from_slice(extra_data),
        mix_hash: B256::from(required_fixed(block.mix_hash.as_ref(), "block.mix_hash")?),
        nonce: B64::from(required_fixed(block.nonce.as_ref(), "block.nonce")?),
        base_fee_per_gas: optional_quantity_u64(
            block.base_fee_per_gas.as_ref(),
            "block.base_fee_per_gas",
        )?,
        withdrawals_root: optional_b256(block.withdrawals_root.as_ref(), "block.withdrawals_root")?,
        blob_gas_used: optional_quantity_u64(block.blob_gas_used.as_ref(), "block.blob_gas_used")?,
        excess_blob_gas: optional_quantity_u64(
            block.excess_blob_gas.as_ref(),
            "block.excess_blob_gas",
        )?,
        parent_beacon_block_root: optional_b256(
            block.parent_beacon_block_root.as_ref(),
            "block.parent_beacon_block_root",
        )?,
        requests_hash: None,
    };
    let expected_hash = B256::from(required_fixed(block.hash.as_ref(), "block.hash")?);
    let actual_hash = header.hash_slow();
    if actual_hash != expected_hash {
        return Err(NormalizeError::HeaderHashMismatch {
            expected: expected_hash.as_slice().to_vec(),
            actual: actual_hash.as_slice().to_vec(),
        });
    }
    let mut encoded = Vec::new();
    header.encode(&mut encoded);
    Ok(encoded)
}

fn required_fixed<const N: usize, T: AsRef<[u8]>>(
    value: Option<&T>,
    field: &'static str,
) -> Result<[u8; N], NormalizeError> {
    let bytes = value.ok_or(NormalizeError::MissingField(field))?.as_ref();
    bytes.try_into().map_err(|_| NormalizeError::InvalidWidth {
        field,
        expected: N,
        received: bytes.len(),
    })
}

fn optional_b256<T: AsRef<[u8]>>(
    value: Option<&T>,
    field: &'static str,
) -> Result<Option<B256>, NormalizeError> {
    value
        .map(|value| required_fixed(Some(value), field).map(B256::from))
        .transpose()
}

fn required_quantity_u64(
    value: Option<&Quantity>,
    field: &'static str,
) -> Result<u64, NormalizeError> {
    value
        .ok_or(NormalizeError::MissingField(field))
        .and_then(|value| quantity_to_u64_field(value, field))
}

fn optional_quantity_u64(
    value: Option<&Quantity>,
    field: &'static str,
) -> Result<Option<u64>, NormalizeError> {
    value
        .map(|value| quantity_to_u64_field(value, field))
        .transpose()
}

fn quantity_to_u64_field(value: &Quantity, field: &'static str) -> Result<u64, NormalizeError> {
    let bytes = value.as_ref();
    if bytes.len() > 8 {
        return Err(NormalizeError::QuantityOverflow(field));
    }
    let mut buffer = [0_u8; 8];
    buffer[8 - bytes.len()..].copy_from_slice(bytes);
    Ok(u64::from_be_bytes(buffer))
}

fn quantity_to_u256(value: &Quantity, field: &'static str) -> Result<U256, NormalizeError> {
    let bytes = value.as_ref();
    if bytes.len() > 32 {
        return Err(NormalizeError::QuantityOverflow(field));
    }
    Ok(U256::from_be_slice(bytes))
}

fn required_bytes<T: AsRef<[u8]>>(
    value: Option<&T>,
    field: &'static str,
) -> Result<Vec<u8>, NormalizeError> {
    value
        .map(|value| value.as_ref().to_vec())
        .ok_or(NormalizeError::MissingField(field))
}

fn required_u64<T: Into<u64>>(
    value: Option<T>,
    field: &'static str,
) -> Result<u64, NormalizeError> {
    value
        .map(Into::into)
        .ok_or(NormalizeError::MissingField(field))
}

fn quantity_to_u64(value: &Quantity) -> Result<u64, NormalizeError> {
    let bytes = value.as_ref();
    if bytes.len() > 8 {
        return Err(NormalizeError::QuantityOverflow("block.timestamp"));
    }
    let mut buffer = [0_u8; 8];
    buffer[8 - bytes.len()..].copy_from_slice(bytes);
    Ok(u64::from_be_bytes(buffer))
}

/// Source data could not be represented by the event protocol.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum NormalizeError {
    /// A field required for canonical identity or runtime input was absent.
    #[error("source response is missing required field `{0}`")]
    MissingField(&'static str),
    /// A log was returned without its canonical block and timestamp.
    #[error("source log at block {block_number} has no corresponding block")]
    LogWithoutBlock {
        /// Orphaned log block number.
        block_number: u64,
    },
    /// A later indexed topic was present after an earlier position was absent.
    #[error("source log topics are not a contiguous prefix")]
    NonContiguousLogTopics,
    /// A fixed-width consensus field had the wrong encoded size.
    #[error("source field `{field}` must be {expected} bytes, got {received}")]
    InvalidWidth {
        /// Field name.
        field: &'static str,
        /// Consensus width.
        expected: usize,
        /// Provider width.
        received: usize,
    },
    /// A consensus field violated a structural invariant.
    #[error("invalid source field: {0}")]
    InvalidField(&'static str),
    /// A numeric source value did not fit the protocol's fixed width.
    #[error("source quantity `{0}` does not fit in u64")]
    QuantityOverflow(&'static str),
    /// An opaque HyperSync checkpoint had an unexpected encoded width.
    #[error("HyperSync checkpoint must be 88 bytes, got {0}")]
    CheckpointWidth(usize),
    /// An opaque source checkpoint was corrupt or from an unknown format.
    #[error("invalid HyperSync source checkpoint: {0}")]
    InvalidCheckpoint(&'static str),
    /// Reconstructed consensus fields did not reproduce the provider's block hash.
    #[error("reconstructed header hash {actual:?} does not match provider hash {expected:?}")]
    HeaderHashMismatch {
        /// Hash returned by the provider.
        expected: Vec<u8>,
        /// Hash computed from the reconstructed RLP.
        actual: Vec<u8>,
    },
}
