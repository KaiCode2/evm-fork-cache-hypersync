//! Canonical, bounded durable-checkpoint codec for the Hybrid coordinator.
//!
//! Version 5 deliberately uses a private RLP schema rather than serializing
//! Rust structs. The outer envelope provides cheap corruption detection while
//! canonical RLP and explicit sorted-map checks make the payload byte-stable.

use std::{collections::BTreeMap, mem::size_of};

use alloy_primitives::B256;
use alloy_rlp::{BufMut, Decodable, Encodable, Header};
use evm_fork_cache::reactive::{
    BlockRef, HandlerId, InputRef, ReactiveInputIdentity, ReactiveInputKind, SubscriberError,
};

use super::{
    AudienceCoverage, CertifiedHistoricalCoverage, HybridCheckpointV5, HybridSource,
    HybridTokenKind, LifecycleIntent, RecordWitness, SourcePosition, StoredCommittedToken,
    StoredRecentInput, WitnessLifecycle,
};

pub(super) const CHECKPOINT_MAGIC_V5: &[u8; 8] = b"EFCHYCP\0";
pub(super) const CHECKPOINT_VERSION_V5: u16 = 5;
pub(super) const MAX_CHECKPOINT_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
pub(super) const CHECKPOINT_ENVELOPE_BYTES: usize = 8 + 2 + 4 + 4;

const LEGACY_BINCODE_VERSION: u16 = 4;
pub(super) const MAX_RECENT_INPUTS: usize = 65_536;
pub(super) const MAX_CANONICAL_HISTORY: usize = 65_536;
const MAX_OWNER_ENTRIES: usize = 262_144;
pub(super) const MAX_HANDLER_ID_BYTES: usize = 4 * 1024;
const MAX_DECODE_NODES: usize = 2_097_152;
const MAX_DECODE_HEAP_BYTES: usize = 64 * 1024 * 1024;

type CodecResult<T> = Result<T, SubscriberError>;

/// Encode a complete v5 state into the versioned outer checkpoint envelope.
pub(super) fn encode_hybrid_checkpoint_v5(state: &HybridCheckpointV5) -> CodecResult<Vec<u8>> {
    encode_hybrid_checkpoint_v5_with_limit(state, MAX_CHECKPOINT_PAYLOAD_BYTES)
}

pub(super) fn encode_hybrid_checkpoint_v5_with_limit(
    state: &HybridCheckpointV5,
    payload_limit: usize,
) -> CodecResult<Vec<u8>> {
    validate_encodable_state(state)?;
    let wire = CheckpointRef(state);
    let payload_len = wire.length();
    let payload_limit = payload_limit.min(MAX_CHECKPOINT_PAYLOAD_BYTES);
    if payload_len > payload_limit {
        return Err(provider(format!(
            "hybrid checkpoint exceeds {payload_limit} bytes"
        )));
    }
    let total_len = CHECKPOINT_ENVELOPE_BYTES
        .checked_add(payload_len)
        .ok_or_else(|| provider("hybrid checkpoint length overflow"))?;
    let mut encoded = Vec::new();
    encoded
        .try_reserve_exact(total_len)
        .map_err(|_| provider("hybrid checkpoint allocation failed"))?;
    encoded.resize(CHECKPOINT_ENVELOPE_BYTES, 0);
    wire.encode(&mut encoded);
    if encoded.len() != total_len {
        return Err(provider("hybrid checkpoint encoder length mismatch"));
    }
    encoded[..8].copy_from_slice(CHECKPOINT_MAGIC_V5);
    encoded[8..10].copy_from_slice(&CHECKPOINT_VERSION_V5.to_be_bytes());
    encoded[10..14].copy_from_slice(
        &u32::try_from(payload_len)
            .map_err(|_| provider("hybrid checkpoint length does not fit u32"))?
            .to_be_bytes(),
    );
    let checksum = crc32fast::hash(&encoded[CHECKPOINT_ENVELOPE_BYTES..]);
    encoded[14..18].copy_from_slice(&checksum.to_be_bytes());
    Ok(encoded)
}

/// Return the exact canonical RLP payload length after dropping an oldest
/// prefix of recent-input witnesses. This performs no allocation and is used
/// by the coordinator to choose one deterministic durable suffix before it
/// exposes a delivery to the runtime.
pub(super) fn checkpoint_payload_len_after_dropping_recent(
    state: &HybridCheckpointV5,
    dropped: usize,
) -> CodecResult<usize> {
    let recent_inputs = state
        .recent_inputs
        .get(dropped..)
        .ok_or_else(|| provider("hybrid checkpoint recent-input drop exceeds journal length"))?;
    Ok(CheckpointFieldsRef(state, recent_inputs).length())
}

/// Validate every codec-side count, identifier, semantic, and decode-budget
/// invariant without materializing the outer checkpoint bytes.
pub(super) fn validate_hybrid_checkpoint_v5_state(state: &HybridCheckpointV5) -> CodecResult<()> {
    validate_encodable_state(state)
}

/// Validate codec counts and decode budgets for a prospective oldest-prefix
/// eviction. The complete state must already satisfy semantic validation;
/// removing old recent witnesses cannot invalidate those semantics.
pub(super) fn validate_hybrid_checkpoint_v5_limits_after_dropping_recent(
    state: &HybridCheckpointV5,
    dropped: usize,
) -> CodecResult<()> {
    let recent_inputs = state
        .recent_inputs
        .get(dropped..)
        .ok_or_else(|| provider("hybrid checkpoint recent-input drop exceeds journal length"))?;
    validate_codec_limits(state, recent_inputs)
}

/// Decode one exact v5 checkpoint envelope with hard allocation ceilings.
///
/// Instance-specific recent-input and history capacities must additionally be
/// checked by the coordinator before installing the returned state.
pub(super) fn decode_hybrid_checkpoint_v5(bytes: &[u8]) -> CodecResult<HybridCheckpointV5> {
    let payload = decode_envelope(bytes)?;
    let mut budget = DecodeBudget::default();
    let mut input = payload;
    let state = decode_checkpoint_payload(&mut input, &mut budget)?;
    if !input.is_empty() {
        return Err(provider(
            "hybrid checkpoint payload contains trailing bytes",
        ));
    }
    Ok(state)
}

fn decode_envelope(bytes: &[u8]) -> CodecResult<&[u8]> {
    if bytes.len() < CHECKPOINT_ENVELOPE_BYTES || &bytes[..8] != CHECKPOINT_MAGIC_V5 {
        return Err(provider(
            "subscriber checkpoint is not a hybrid coordinator checkpoint",
        ));
    }
    let version = u16::from_be_bytes([bytes[8], bytes[9]]);
    if version == LEGACY_BINCODE_VERSION {
        return Err(provider(
            "hybrid checkpoint version 4 used the unpublished bincode format; discard it and perform an authoritative resynchronization",
        ));
    }
    if version != CHECKPOINT_VERSION_V5 {
        return Err(provider(format!(
            "unsupported hybrid checkpoint version {version}"
        )));
    }
    let payload_len = u32::from_be_bytes(
        bytes[10..14]
            .try_into()
            .expect("checkpoint envelope contains four length bytes"),
    ) as usize;
    if payload_len > MAX_CHECKPOINT_PAYLOAD_BYTES
        || bytes.len() != CHECKPOINT_ENVELOPE_BYTES.saturating_add(payload_len)
    {
        return Err(provider(
            "hybrid checkpoint has an invalid or oversized payload length",
        ));
    }
    let expected_crc = u32::from_be_bytes(
        bytes[14..18]
            .try_into()
            .expect("checkpoint envelope contains four checksum bytes"),
    );
    let payload = &bytes[CHECKPOINT_ENVELOPE_BYTES..];
    if crc32fast::hash(payload) != expected_crc {
        return Err(provider("hybrid checkpoint checksum mismatch"));
    }
    Ok(payload)
}

#[derive(Default)]
struct DecodeBudget {
    nodes: usize,
    heap_bytes: usize,
    owner_entries: usize,
}

impl DecodeBudget {
    fn claim_nodes(&mut self, count: usize) -> CodecResult<()> {
        self.nodes = self
            .nodes
            .checked_add(count)
            .ok_or_else(|| provider("hybrid checkpoint decode-node count overflow"))?;
        if self.nodes > MAX_DECODE_NODES {
            return Err(provider("hybrid checkpoint exceeds its decode-node budget"));
        }
        Ok(())
    }

    fn claim_heap<T>(&mut self, count: usize) -> CodecResult<()> {
        let bytes = size_of::<T>()
            .checked_mul(count)
            .ok_or_else(|| provider("hybrid checkpoint decoded heap accounting overflow"))?;
        self.claim_heap_bytes(bytes)
    }

    fn claim_heap_bytes(&mut self, bytes: usize) -> CodecResult<()> {
        self.heap_bytes = self
            .heap_bytes
            .checked_add(bytes)
            .ok_or_else(|| provider("hybrid checkpoint decoded heap accounting overflow"))?;
        if self.heap_bytes > MAX_DECODE_HEAP_BYTES {
            return Err(provider(
                "hybrid checkpoint exceeds its decoded heap budget",
            ));
        }
        Ok(())
    }

    fn claim_owners(&mut self, count: usize) -> CodecResult<()> {
        self.owner_entries = self
            .owner_entries
            .checked_add(count)
            .ok_or_else(|| provider("hybrid checkpoint owner count overflow"))?;
        if self.owner_entries > MAX_OWNER_ENTRIES {
            return Err(provider("hybrid checkpoint exceeds its owner-entry budget"));
        }
        Ok(())
    }
}

fn provider(message: impl Into<String>) -> SubscriberError {
    SubscriberError::Provider(message.into())
}

fn rlp_error(context: &str, error: alloy_rlp::Error) -> SubscriberError {
    provider(format!("invalid hybrid checkpoint {context}: {error}"))
}

fn take_list<'a>(input: &mut &'a [u8], context: &str) -> CodecResult<&'a [u8]> {
    Header::decode_bytes(input, true).map_err(|error| rlp_error(context, error))
}

fn finish_list(input: &[u8], context: &str) -> CodecResult<()> {
    if input.is_empty() {
        Ok(())
    } else {
        Err(provider(format!(
            "hybrid checkpoint {context} contains trailing list fields"
        )))
    }
}

fn count_items(mut payload: &[u8], max: usize, context: &str) -> CodecResult<usize> {
    let mut count = 0usize;
    while !payload.is_empty() {
        count = count
            .checked_add(1)
            .ok_or_else(|| provider(format!("hybrid checkpoint {context} count overflow")))?;
        if count > max {
            return Err(provider(format!(
                "hybrid checkpoint {context} exceeds {max} entries"
            )));
        }
        let header = Header::decode(&mut payload).map_err(|error| rlp_error(context, error))?;
        payload = payload
            .get(header.payload_length..)
            .ok_or_else(|| provider(format!("truncated hybrid checkpoint {context}")))?;
    }
    Ok(count)
}

fn decode_vec<T>(
    input: &mut &[u8],
    max: usize,
    context: &str,
    budget: &mut DecodeBudget,
    mut decode: impl FnMut(&mut &[u8], &mut DecodeBudget) -> CodecResult<T>,
) -> CodecResult<Vec<T>> {
    let mut payload = take_list(input, context)?;
    let count = count_items(payload, max, context)?;
    budget.claim_nodes(count)?;
    budget.claim_heap::<T>(count)?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| provider(format!("hybrid checkpoint {context} allocation failed")))?;
    while !payload.is_empty() {
        values.push(decode(&mut payload, budget)?);
    }
    Ok(values)
}

fn decode_u8(input: &mut &[u8], context: &str) -> CodecResult<u8> {
    u8::decode(input).map_err(|error| rlp_error(context, error))
}

fn decode_u64(input: &mut &[u8], context: &str) -> CodecResult<u64> {
    u64::decode(input).map_err(|error| rlp_error(context, error))
}

fn decode_bool(input: &mut &[u8], context: &str) -> CodecResult<bool> {
    bool::decode(input).map_err(|error| rlp_error(context, error))
}

fn decode_array<const N: usize>(input: &mut &[u8], context: &str) -> CodecResult<[u8; N]> {
    <[u8; N]>::decode(input).map_err(|error| rlp_error(context, error))
}

fn decode_hash(input: &mut &[u8], context: &str) -> CodecResult<B256> {
    Ok(B256::from(decode_array::<32>(input, context)?))
}

fn decode_bytes(
    input: &mut &[u8],
    max: usize,
    context: &str,
    budget: &mut DecodeBudget,
) -> CodecResult<Vec<u8>> {
    let bytes = Header::decode_bytes(input, false).map_err(|error| rlp_error(context, error))?;
    if bytes.len() > max {
        return Err(provider(format!(
            "hybrid checkpoint {context} exceeds {max} bytes"
        )));
    }
    budget.claim_nodes(1)?;
    budget.claim_heap_bytes(bytes.len())?;
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(bytes.len())
        .map_err(|_| provider(format!("hybrid checkpoint {context} allocation failed")))?;
    owned.extend_from_slice(bytes);
    Ok(owned)
}

fn decode_string(
    input: &mut &[u8],
    max: usize,
    context: &str,
    budget: &mut DecodeBudget,
) -> CodecResult<String> {
    let bytes = Header::decode_bytes(input, false).map_err(|error| rlp_error(context, error))?;
    if bytes.len() > max {
        return Err(provider(format!(
            "hybrid checkpoint {context} exceeds {max} bytes"
        )));
    }
    let value = std::str::from_utf8(bytes)
        .map_err(|_| provider(format!("hybrid checkpoint {context} is not UTF-8")))?;
    budget.claim_nodes(1)?;
    budget.claim_heap_bytes(value.len())?;
    let mut owned = String::new();
    owned
        .try_reserve_exact(value.len())
        .map_err(|_| provider(format!("hybrid checkpoint {context} allocation failed")))?;
    owned.push_str(value);
    Ok(owned)
}

fn decode_option<T>(
    input: &mut &[u8],
    context: &str,
    budget: &mut DecodeBudget,
    decode: impl FnOnce(&mut &[u8], &mut DecodeBudget) -> CodecResult<T>,
) -> CodecResult<Option<T>> {
    let mut payload = take_list(input, context)?;
    budget.claim_nodes(1)?;
    if payload.is_empty() {
        return Ok(None);
    }
    let value = decode(&mut payload, budget)?;
    finish_list(payload, context)?;
    Ok(Some(value))
}

fn decode_option_u64(
    input: &mut &[u8],
    context: &str,
    budget: &mut DecodeBudget,
) -> CodecResult<Option<u64>> {
    decode_option(input, context, budget, |input, _| {
        decode_u64(input, context)
    })
}

fn decode_option_hash(
    input: &mut &[u8],
    context: &str,
    budget: &mut DecodeBudget,
) -> CodecResult<Option<B256>> {
    decode_option(input, context, budget, |input, _| {
        decode_hash(input, context)
    })
}

fn decode_block_ref(input: &mut &[u8], budget: &mut DecodeBudget) -> CodecResult<BlockRef> {
    let mut payload = take_list(input, "block reference")?;
    budget.claim_nodes(1)?;
    let block = BlockRef {
        number: decode_u64(&mut payload, "block number")?,
        hash: decode_hash(&mut payload, "block hash")?,
        parent_hash: decode_option_hash(&mut payload, "block parent hash", budget)?,
        timestamp: decode_option_u64(&mut payload, "block timestamp", budget)?,
    };
    finish_list(payload, "block reference")?;
    Ok(block)
}

fn decode_option_block_ref(
    input: &mut &[u8],
    context: &str,
    budget: &mut DecodeBudget,
) -> CodecResult<Option<BlockRef>> {
    decode_option(input, context, budget, decode_block_ref)
}

fn decode_handler_id(
    input: &mut &[u8],
    context: &str,
    budget: &mut DecodeBudget,
) -> CodecResult<HandlerId> {
    let value = decode_string(input, MAX_HANDLER_ID_BYTES, context, budget)?;
    HandlerId::try_new(value).map_err(|_| provider(format!("hybrid checkpoint {context} is empty")))
}

fn decode_owner_generations(
    input: &mut &[u8],
    budget: &mut DecodeBudget,
) -> CodecResult<BTreeMap<HandlerId, u64>> {
    decode_owner_map(input, "owner generations", budget, |input, _| {
        decode_u64(input, "owner generation")
    })
}

fn decode_owner_digests(
    input: &mut &[u8],
    budget: &mut DecodeBudget,
) -> CodecResult<BTreeMap<HandlerId, [u8; 32]>> {
    decode_owner_map(input, "lifecycle owner fingerprints", budget, |input, _| {
        decode_array::<32>(input, "lifecycle owner fingerprint")
    })
}

fn decode_owner_map<V>(
    input: &mut &[u8],
    context: &str,
    budget: &mut DecodeBudget,
    mut decode_value: impl FnMut(&mut &[u8], &mut DecodeBudget) -> CodecResult<V>,
) -> CodecResult<BTreeMap<HandlerId, V>> {
    let mut payload = take_list(input, context)?;
    let count = count_items(payload, MAX_OWNER_ENTRIES, context)?;
    budget.claim_nodes(count.saturating_add(1))?;
    budget.claim_owners(count)?;
    // Account conservatively for the key/value storage and tree links. String
    // payload capacities are charged separately by `decode_handler_id`.
    budget.claim_heap_bytes(
        size_of::<(HandlerId, V)>()
            .saturating_add(4 * size_of::<usize>())
            .saturating_mul(count),
    )?;
    let mut values = BTreeMap::new();
    while !payload.is_empty() {
        let mut entry = take_list(&mut payload, context)?;
        budget.claim_nodes(1)?;
        let owner = decode_handler_id(&mut entry, "handler id", budget)?;
        if values
            .last_key_value()
            .is_some_and(|(previous, _)| previous >= &owner)
        {
            return Err(provider(format!(
                "hybrid checkpoint {context} is not strictly sorted by handler id"
            )));
        }
        let value = decode_value(&mut entry, budget)?;
        finish_list(entry, context)?;
        values.insert(owner, value);
    }
    Ok(values)
}

fn decode_input_ref(input: &mut &[u8], budget: &mut DecodeBudget) -> CodecResult<InputRef> {
    let mut payload = take_list(input, "input reference")?;
    budget.claim_nodes(1)?;
    let tag = decode_u8(&mut payload, "input-reference tag")?;
    let input_ref = match tag {
        1 => InputRef::Log {
            chain_id: decode_option_u64(&mut payload, "log chain id", budget)?,
            block_hash: decode_hash(&mut payload, "log block hash")?,
            transaction_hash: decode_hash(&mut payload, "log transaction hash")?,
            log_index: decode_u64(&mut payload, "log index")?,
        },
        2 => InputRef::PendingTx {
            chain_id: decode_option_u64(&mut payload, "pending transaction chain id", budget)?,
            hash: decode_hash(&mut payload, "pending transaction hash")?,
        },
        3 => InputRef::Block {
            chain_id: decode_option_u64(&mut payload, "block input chain id", budget)?,
            hash: decode_hash(&mut payload, "block input hash")?,
            number: decode_u64(&mut payload, "block input number")?,
        },
        _ => {
            return Err(provider(format!(
                "hybrid checkpoint contains unknown input-reference tag {tag}"
            )));
        }
    };
    finish_list(payload, "input reference")?;
    Ok(input_ref)
}

fn decode_input_kind(input: &mut &[u8]) -> CodecResult<ReactiveInputKind> {
    match decode_u8(input, "reactive-input kind")? {
        1 => Ok(ReactiveInputKind::CanonicalLog),
        2 => Ok(ReactiveInputKind::ReorgSignalLog),
        3 => Ok(ReactiveInputKind::BlockHeader),
        4 => Ok(ReactiveInputKind::FullBlock),
        5 => Ok(ReactiveInputKind::PendingTxHash),
        6 => Ok(ReactiveInputKind::PendingTx),
        tag => Err(provider(format!(
            "hybrid checkpoint contains unknown reactive-input kind {tag}"
        ))),
    }
}

fn decode_input_identity(
    input: &mut &[u8],
    budget: &mut DecodeBudget,
) -> CodecResult<ReactiveInputIdentity> {
    let mut payload = take_list(input, "reactive-input identity")?;
    budget.claim_nodes(1)?;
    let input_ref = decode_input_ref(&mut payload, budget)?;
    let kind = decode_input_kind(&mut payload)?;
    finish_list(payload, "reactive-input identity")?;
    ReactiveInputIdentity::try_from_parts(input_ref, kind).map_err(|error| {
        provider(format!(
            "hybrid checkpoint contains an incompatible reactive-input identity: {error}"
        ))
    })
}

fn decode_witness_lifecycle(input: &mut &[u8]) -> CodecResult<WitnessLifecycle> {
    match decode_u8(input, "record-witness lifecycle")? {
        1 => Ok(WitnessLifecycle::Included),
        2 => Ok(WitnessLifecycle::Safe),
        3 => Ok(WitnessLifecycle::Finalized),
        4 => Ok(WitnessLifecycle::Reorg),
        5 => Ok(WitnessLifecycle::Pending),
        tag => Err(provider(format!(
            "hybrid checkpoint contains unknown record-witness lifecycle {tag}"
        ))),
    }
}

fn decode_record_witness(
    input: &mut &[u8],
    budget: &mut DecodeBudget,
) -> CodecResult<RecordWitness> {
    let mut payload = take_list(input, "record witness")?;
    budget.claim_nodes(1)?;
    let witness = RecordWitness {
        payload_digest: decode_array::<32>(&mut payload, "record payload digest")?,
        chain_id: decode_u64(&mut payload, "record witness chain id")?,
        lifecycle: decode_witness_lifecycle(&mut payload)?,
        block: decode_option_block_ref(&mut payload, "record witness block", budget)?,
        transaction_index: decode_option_u64(&mut payload, "record transaction index", budget)?,
        log_index: decode_option_u64(&mut payload, "record log index", budget)?,
        log_block_timestamp: decode_option_u64(&mut payload, "record log block timestamp", budget)?,
    };
    finish_list(payload, "record witness")?;
    Ok(witness)
}

fn decode_audience_coverage(
    input: &mut &[u8],
    budget: &mut DecodeBudget,
) -> CodecResult<AudienceCoverage> {
    let mut payload = take_list(input, "audience coverage")?;
    budget.claim_nodes(1)?;
    let coverage = AudienceCoverage {
        base: decode_bool(&mut payload, "base audience coverage")?,
        owners: decode_owner_generations(&mut payload, budget)?,
        block: decode_option_block_ref(&mut payload, "audience coverage block", budget)?,
        witness: decode_option(
            &mut payload,
            "audience coverage witness",
            budget,
            decode_record_witness,
        )?,
    };
    finish_list(payload, "audience coverage")?;
    Ok(coverage)
}

fn decode_stored_recent_input(
    input: &mut &[u8],
    budget: &mut DecodeBudget,
) -> CodecResult<StoredRecentInput> {
    let mut payload = take_list(input, "recent input")?;
    budget.claim_nodes(1)?;
    let value = StoredRecentInput {
        identity: decode_input_identity(&mut payload, budget)?,
        coverage: decode_audience_coverage(&mut payload, budget)?,
    };
    finish_list(payload, "recent input")?;
    Ok(value)
}

fn decode_source_position(
    input: &mut &[u8],
    budget: &mut DecodeBudget,
) -> CodecResult<SourcePosition> {
    let mut payload = take_list(input, "source position")?;
    budget.claim_nodes(1)?;
    let value = SourcePosition {
        delivery_token: decode_option(
            &mut payload,
            "source delivery token",
            budget,
            |input, budget| {
                decode_bytes(
                    input,
                    MAX_CHECKPOINT_PAYLOAD_BYTES,
                    "source delivery token",
                    budget,
                )
            },
        )?,
        checkpoint: decode_option(
            &mut payload,
            "child subscriber checkpoint",
            budget,
            |input, budget| {
                decode_bytes(
                    input,
                    MAX_CHECKPOINT_PAYLOAD_BYTES,
                    "child subscriber checkpoint",
                    budget,
                )
            },
        )?,
        coverage_head: decode_option_block_ref(&mut payload, "source coverage head", budget)?,
        canonical_history: decode_vec(
            &mut payload,
            MAX_CANONICAL_HISTORY,
            "source canonical history",
            budget,
            decode_block_ref,
        )?,
        delivery_digest: decode_option(
            &mut payload,
            "source delivery digest",
            budget,
            |input, _| decode_array::<32>(input, "source delivery digest"),
        )?,
    };
    finish_list(payload, "source position")?;
    Ok(value)
}

fn decode_hybrid_source(input: &mut &[u8]) -> CodecResult<HybridSource> {
    match decode_u8(input, "hybrid source")? {
        1 => Ok(HybridSource::Historical),
        2 => Ok(HybridSource::Live),
        tag => Err(provider(format!(
            "hybrid checkpoint contains unknown source tag {tag}"
        ))),
    }
}

fn decode_hybrid_token_kind(input: &mut &[u8]) -> CodecResult<HybridTokenKind> {
    match decode_u8(input, "hybrid token kind")? {
        1 => Ok(HybridTokenKind::Forwarded),
        2 => Ok(HybridTokenKind::Synthetic),
        tag => Err(provider(format!(
            "hybrid checkpoint contains unknown token-kind tag {tag}"
        ))),
    }
}

fn decode_stored_committed_token(
    input: &mut &[u8],
    budget: &mut DecodeBudget,
) -> CodecResult<StoredCommittedToken> {
    let mut payload = take_list(input, "committed token")?;
    budget.claim_nodes(1)?;
    let value = StoredCommittedToken {
        source: decode_hybrid_source(&mut payload)?,
        kind: decode_hybrid_token_kind(&mut payload)?,
        inner: decode_bytes(
            &mut payload,
            MAX_CHECKPOINT_PAYLOAD_BYTES,
            "committed token payload",
            budget,
        )?,
    };
    finish_list(payload, "committed token")?;
    Ok(value)
}

fn decode_lifecycle_intent(
    input: &mut &[u8],
    budget: &mut DecodeBudget,
) -> CodecResult<LifecycleIntent> {
    let mut payload = take_list(input, "lifecycle intent")?;
    budget.claim_nodes(1)?;
    let value = LifecycleIntent {
        base: decode_array::<32>(&mut payload, "base lifecycle fingerprint")?,
        owners: decode_owner_digests(&mut payload, budget)?,
    };
    finish_list(payload, "lifecycle intent")?;
    Ok(value)
}

fn decode_certified_historical_coverage(
    input: &mut &[u8],
    budget: &mut DecodeBudget,
) -> CodecResult<CertifiedHistoricalCoverage> {
    let mut payload = take_list(input, "certified historical coverage")?;
    budget.claim_nodes(1)?;
    let value = CertifiedHistoricalCoverage {
        lifecycle_generation: decode_u64(
            &mut payload,
            "certified historical lifecycle generation",
        )?,
        through: decode_block_ref(&mut payload, budget)?,
    };
    finish_list(payload, "certified historical coverage")?;
    Ok(value)
}

/// V5 payload field order is a permanent wire contract:
///
/// 1. chain id; 2. epoch; 3. next synthetic token; 4. lifecycle generation;
/// 5. owner generations; 6. lifecycle intent; 7. recent inputs;
/// 8. canonical history; 9. coverage head; 10. safe head;
/// 11. finalized head; 12. certified historical coverage;
/// 13. historical position; 14. live position; 15. last committed token.
fn decode_checkpoint_payload(
    input: &mut &[u8],
    budget: &mut DecodeBudget,
) -> CodecResult<HybridCheckpointV5> {
    let mut payload = take_list(input, "v5 payload")?;
    budget.claim_nodes(1)?;
    let state = HybridCheckpointV5 {
        chain_id: decode_u64(&mut payload, "chain id")?,
        epoch: decode_array::<16>(&mut payload, "epoch")?,
        next_synthetic_token: decode_u64(&mut payload, "next synthetic token")?,
        lifecycle_generation: decode_u64(&mut payload, "lifecycle generation")?,
        owner_generations: decode_owner_generations(&mut payload, budget)?,
        lifecycle_intent: decode_lifecycle_intent(&mut payload, budget)?,
        recent_inputs: decode_vec(
            &mut payload,
            MAX_RECENT_INPUTS,
            "recent inputs",
            budget,
            decode_stored_recent_input,
        )?,
        canonical_history: decode_vec(
            &mut payload,
            MAX_CANONICAL_HISTORY,
            "coordinator canonical history",
            budget,
            decode_block_ref,
        )?,
        coverage_head: decode_option_block_ref(&mut payload, "coverage head", budget)?,
        safe_head: decode_option_block_ref(&mut payload, "safe head", budget)?,
        finalized_head: decode_option_block_ref(&mut payload, "finalized head", budget)?,
        certified_historical: decode_option(
            &mut payload,
            "certified historical coverage",
            budget,
            decode_certified_historical_coverage,
        )?,
        historical_position: decode_source_position(&mut payload, budget)?,
        live_position: decode_source_position(&mut payload, budget)?,
        last_committed_token: decode_option(
            &mut payload,
            "last committed token",
            budget,
            decode_stored_committed_token,
        )?,
    };
    finish_list(payload, "v5 payload")?;
    super::validate_checkpoint_state(&state)?;
    Ok(state)
}

fn fields_payload_length(fields: &[&dyn Encodable]) -> usize {
    fields
        .iter()
        .fold(0usize, |total, field| total.saturating_add(field.length()))
}

fn list_length_from_payload(payload_length: usize) -> usize {
    Header {
        list: true,
        payload_length,
    }
    .length()
    .saturating_add(payload_length)
}

fn fields_length(fields: &[&dyn Encodable]) -> usize {
    list_length_from_payload(fields_payload_length(fields))
}

fn encode_fields(fields: &[&dyn Encodable], out: &mut dyn BufMut) {
    Header {
        list: true,
        payload_length: fields_payload_length(fields),
    }
    .encode(out);
    for field in fields {
        field.encode(out);
    }
}

struct BytesRef<'a>(&'a [u8]);

impl Encodable for BytesRef<'_> {
    fn length(&self) -> usize {
        self.0.length()
    }

    fn encode(&self, out: &mut dyn BufMut) {
        self.0.encode(out);
    }
}

struct HashRef<'a>(&'a B256);

impl Encodable for HashRef<'_> {
    fn length(&self) -> usize {
        self.0.as_slice().length()
    }

    fn encode(&self, out: &mut dyn BufMut) {
        self.0.as_slice().encode(out);
    }
}

/// `None` is the empty list; `Some(value)` is a one-item list. This keeps
/// absent values distinct from every valid RLP value, including zero and an
/// empty byte string.
struct Optional<T>(Option<T>);

impl<T: Encodable> Encodable for Optional<T> {
    fn length(&self) -> usize {
        self.0
            .as_ref()
            .map_or(1, |value| list_length_from_payload(value.length()))
    }

    fn encode(&self, out: &mut dyn BufMut) {
        match self.0.as_ref() {
            Some(value) => {
                Header {
                    list: true,
                    payload_length: value.length(),
                }
                .encode(out);
                value.encode(out);
            }
            None => Header {
                list: true,
                payload_length: 0,
            }
            .encode(out),
        }
    }
}

struct BlockRefRef<'a>(&'a BlockRef);

impl Encodable for BlockRefRef<'_> {
    fn length(&self) -> usize {
        fields_length(&[
            &self.0.number,
            &HashRef(&self.0.hash),
            &Optional(self.0.parent_hash.as_ref().map(HashRef)),
            &Optional(self.0.timestamp),
        ])
    }

    fn encode(&self, out: &mut dyn BufMut) {
        encode_fields(
            &[
                &self.0.number,
                &HashRef(&self.0.hash),
                &Optional(self.0.parent_hash.as_ref().map(HashRef)),
                &Optional(self.0.timestamp),
            ],
            out,
        );
    }
}

struct BlockRefsRef<'a>(&'a [BlockRef]);

impl Encodable for BlockRefsRef<'_> {
    fn length(&self) -> usize {
        let payload_length = self.0.iter().fold(0usize, |total, block| {
            total.saturating_add(BlockRefRef(block).length())
        });
        list_length_from_payload(payload_length)
    }

    fn encode(&self, out: &mut dyn BufMut) {
        let payload_length = self.0.iter().fold(0usize, |total, block| {
            total.saturating_add(BlockRefRef(block).length())
        });
        Header {
            list: true,
            payload_length,
        }
        .encode(out);
        for block in self.0 {
            BlockRefRef(block).encode(out);
        }
    }
}

struct OwnerGenerationEntryRef<'a>(&'a HandlerId, &'a u64);

impl Encodable for OwnerGenerationEntryRef<'_> {
    fn length(&self) -> usize {
        fields_length(&[&self.0.as_str(), self.1])
    }

    fn encode(&self, out: &mut dyn BufMut) {
        encode_fields(&[&self.0.as_str(), self.1], out);
    }
}

struct OwnerGenerationsRef<'a>(&'a BTreeMap<HandlerId, u64>);

impl Encodable for OwnerGenerationsRef<'_> {
    fn length(&self) -> usize {
        let payload_length = self.0.iter().fold(0usize, |total, (owner, value)| {
            total.saturating_add(OwnerGenerationEntryRef(owner, value).length())
        });
        list_length_from_payload(payload_length)
    }

    fn encode(&self, out: &mut dyn BufMut) {
        let payload_length = self.0.iter().fold(0usize, |total, (owner, value)| {
            total.saturating_add(OwnerGenerationEntryRef(owner, value).length())
        });
        Header {
            list: true,
            payload_length,
        }
        .encode(out);
        for (owner, value) in self.0 {
            OwnerGenerationEntryRef(owner, value).encode(out);
        }
    }
}

struct OwnerDigestEntryRef<'a>(&'a HandlerId, &'a [u8; 32]);

impl Encodable for OwnerDigestEntryRef<'_> {
    fn length(&self) -> usize {
        fields_length(&[&self.0.as_str(), self.1])
    }

    fn encode(&self, out: &mut dyn BufMut) {
        encode_fields(&[&self.0.as_str(), self.1], out);
    }
}

struct OwnerDigestsRef<'a>(&'a BTreeMap<HandlerId, [u8; 32]>);

impl Encodable for OwnerDigestsRef<'_> {
    fn length(&self) -> usize {
        let payload_length = self.0.iter().fold(0usize, |total, (owner, value)| {
            total.saturating_add(OwnerDigestEntryRef(owner, value).length())
        });
        list_length_from_payload(payload_length)
    }

    fn encode(&self, out: &mut dyn BufMut) {
        let payload_length = self.0.iter().fold(0usize, |total, (owner, value)| {
            total.saturating_add(OwnerDigestEntryRef(owner, value).length())
        });
        Header {
            list: true,
            payload_length,
        }
        .encode(out);
        for (owner, value) in self.0 {
            OwnerDigestEntryRef(owner, value).encode(out);
        }
    }
}

struct InputRefRef<'a>(&'a InputRef);

impl Encodable for InputRefRef<'_> {
    fn length(&self) -> usize {
        match self.0 {
            InputRef::Log {
                chain_id,
                block_hash,
                transaction_hash,
                log_index,
            } => fields_length(&[
                &1u8,
                &Optional(*chain_id),
                &HashRef(block_hash),
                &HashRef(transaction_hash),
                log_index,
            ]),
            InputRef::PendingTx { chain_id, hash } => {
                fields_length(&[&2u8, &Optional(*chain_id), &HashRef(hash)])
            }
            InputRef::Block {
                chain_id,
                hash,
                number,
            } => fields_length(&[&3u8, &Optional(*chain_id), &HashRef(hash), number]),
        }
    }

    fn encode(&self, out: &mut dyn BufMut) {
        match self.0 {
            InputRef::Log {
                chain_id,
                block_hash,
                transaction_hash,
                log_index,
            } => encode_fields(
                &[
                    &1u8,
                    &Optional(*chain_id),
                    &HashRef(block_hash),
                    &HashRef(transaction_hash),
                    log_index,
                ],
                out,
            ),
            InputRef::PendingTx { chain_id, hash } => {
                encode_fields(&[&2u8, &Optional(*chain_id), &HashRef(hash)], out);
            }
            InputRef::Block {
                chain_id,
                hash,
                number,
            } => encode_fields(&[&3u8, &Optional(*chain_id), &HashRef(hash), number], out),
        }
    }
}

fn input_kind_tag(kind: ReactiveInputKind) -> Option<u8> {
    match kind {
        ReactiveInputKind::CanonicalLog => Some(1),
        ReactiveInputKind::ReorgSignalLog => Some(2),
        ReactiveInputKind::BlockHeader => Some(3),
        ReactiveInputKind::FullBlock => Some(4),
        ReactiveInputKind::PendingTxHash => Some(5),
        ReactiveInputKind::PendingTx => Some(6),
        _ => None,
    }
}

struct InputIdentityRef<'a>(&'a ReactiveInputIdentity);

impl Encodable for InputIdentityRef<'_> {
    fn length(&self) -> usize {
        let input_ref = self.0.input_ref();
        let tag = input_kind_tag(self.0.kind()).expect("input kind validated before encoding");
        fields_length(&[&InputRefRef(&input_ref), &tag])
    }

    fn encode(&self, out: &mut dyn BufMut) {
        let input_ref = self.0.input_ref();
        let tag = input_kind_tag(self.0.kind()).expect("input kind validated before encoding");
        encode_fields(&[&InputRefRef(&input_ref), &tag], out);
    }
}

fn witness_lifecycle_tag(lifecycle: WitnessLifecycle) -> u8 {
    match lifecycle {
        WitnessLifecycle::Included => 1,
        WitnessLifecycle::Safe => 2,
        WitnessLifecycle::Finalized => 3,
        WitnessLifecycle::Reorg => 4,
        WitnessLifecycle::Pending => 5,
    }
}

struct RecordWitnessRef<'a>(&'a RecordWitness);

impl Encodable for RecordWitnessRef<'_> {
    fn length(&self) -> usize {
        fields_length(&[
            &self.0.payload_digest,
            &self.0.chain_id,
            &witness_lifecycle_tag(self.0.lifecycle),
            &Optional(self.0.block.as_ref().map(BlockRefRef)),
            &Optional(self.0.transaction_index),
            &Optional(self.0.log_index),
            &Optional(self.0.log_block_timestamp),
        ])
    }

    fn encode(&self, out: &mut dyn BufMut) {
        encode_fields(
            &[
                &self.0.payload_digest,
                &self.0.chain_id,
                &witness_lifecycle_tag(self.0.lifecycle),
                &Optional(self.0.block.as_ref().map(BlockRefRef)),
                &Optional(self.0.transaction_index),
                &Optional(self.0.log_index),
                &Optional(self.0.log_block_timestamp),
            ],
            out,
        );
    }
}

struct AudienceCoverageRef<'a>(&'a AudienceCoverage);

impl Encodable for AudienceCoverageRef<'_> {
    fn length(&self) -> usize {
        fields_length(&[
            &self.0.base,
            &OwnerGenerationsRef(&self.0.owners),
            &Optional(self.0.block.as_ref().map(BlockRefRef)),
            &Optional(self.0.witness.as_ref().map(RecordWitnessRef)),
        ])
    }

    fn encode(&self, out: &mut dyn BufMut) {
        encode_fields(
            &[
                &self.0.base,
                &OwnerGenerationsRef(&self.0.owners),
                &Optional(self.0.block.as_ref().map(BlockRefRef)),
                &Optional(self.0.witness.as_ref().map(RecordWitnessRef)),
            ],
            out,
        );
    }
}

struct StoredRecentInputRef<'a>(&'a StoredRecentInput);

impl Encodable for StoredRecentInputRef<'_> {
    fn length(&self) -> usize {
        fields_length(&[
            &InputIdentityRef(&self.0.identity),
            &AudienceCoverageRef(&self.0.coverage),
        ])
    }

    fn encode(&self, out: &mut dyn BufMut) {
        encode_fields(
            &[
                &InputIdentityRef(&self.0.identity),
                &AudienceCoverageRef(&self.0.coverage),
            ],
            out,
        );
    }
}

struct StoredRecentInputsRef<'a>(&'a [StoredRecentInput]);

impl Encodable for StoredRecentInputsRef<'_> {
    fn length(&self) -> usize {
        let payload_length = self.0.iter().fold(0usize, |total, value| {
            total.saturating_add(StoredRecentInputRef(value).length())
        });
        list_length_from_payload(payload_length)
    }

    fn encode(&self, out: &mut dyn BufMut) {
        let payload_length = self.0.iter().fold(0usize, |total, value| {
            total.saturating_add(StoredRecentInputRef(value).length())
        });
        Header {
            list: true,
            payload_length,
        }
        .encode(out);
        for value in self.0 {
            StoredRecentInputRef(value).encode(out);
        }
    }
}

struct SourcePositionRef<'a>(&'a SourcePosition);

impl Encodable for SourcePositionRef<'_> {
    fn length(&self) -> usize {
        fields_length(&[
            &Optional(self.0.delivery_token.as_deref().map(BytesRef)),
            &Optional(self.0.checkpoint.as_deref().map(BytesRef)),
            &Optional(self.0.coverage_head.as_ref().map(BlockRefRef)),
            &BlockRefsRef(&self.0.canonical_history),
            &Optional(self.0.delivery_digest.as_ref()),
        ])
    }

    fn encode(&self, out: &mut dyn BufMut) {
        encode_fields(
            &[
                &Optional(self.0.delivery_token.as_deref().map(BytesRef)),
                &Optional(self.0.checkpoint.as_deref().map(BytesRef)),
                &Optional(self.0.coverage_head.as_ref().map(BlockRefRef)),
                &BlockRefsRef(&self.0.canonical_history),
                &Optional(self.0.delivery_digest.as_ref()),
            ],
            out,
        );
    }
}

fn hybrid_source_tag(source: HybridSource) -> u8 {
    match source {
        HybridSource::Historical => 1,
        HybridSource::Live => 2,
    }
}

fn hybrid_token_kind_tag(kind: HybridTokenKind) -> u8 {
    match kind {
        HybridTokenKind::Forwarded => 1,
        HybridTokenKind::Synthetic => 2,
    }
}

struct StoredCommittedTokenRef<'a>(&'a StoredCommittedToken);

impl Encodable for StoredCommittedTokenRef<'_> {
    fn length(&self) -> usize {
        fields_length(&[
            &hybrid_source_tag(self.0.source),
            &hybrid_token_kind_tag(self.0.kind),
            &BytesRef(&self.0.inner),
        ])
    }

    fn encode(&self, out: &mut dyn BufMut) {
        encode_fields(
            &[
                &hybrid_source_tag(self.0.source),
                &hybrid_token_kind_tag(self.0.kind),
                &BytesRef(&self.0.inner),
            ],
            out,
        );
    }
}

struct LifecycleIntentRef<'a>(&'a LifecycleIntent);

impl Encodable for LifecycleIntentRef<'_> {
    fn length(&self) -> usize {
        fields_length(&[&self.0.base, &OwnerDigestsRef(&self.0.owners)])
    }

    fn encode(&self, out: &mut dyn BufMut) {
        encode_fields(&[&self.0.base, &OwnerDigestsRef(&self.0.owners)], out);
    }
}

struct CertifiedHistoricalCoverageRef<'a>(&'a CertifiedHistoricalCoverage);

impl Encodable for CertifiedHistoricalCoverageRef<'_> {
    fn length(&self) -> usize {
        fields_length(&[&self.0.lifecycle_generation, &BlockRefRef(&self.0.through)])
    }

    fn encode(&self, out: &mut dyn BufMut) {
        encode_fields(
            &[&self.0.lifecycle_generation, &BlockRefRef(&self.0.through)],
            out,
        );
    }
}

struct CheckpointRef<'a>(&'a HybridCheckpointV5);

impl Encodable for CheckpointRef<'_> {
    fn length(&self) -> usize {
        CheckpointFieldsRef(self.0, &self.0.recent_inputs).length()
    }

    fn encode(&self, out: &mut dyn BufMut) {
        CheckpointFieldsRef(self.0, &self.0.recent_inputs).encode(out);
    }
}

/// Encoding view used both by the full writer and by the allocation-free
/// durable-window fitter. Only `recent_inputs` may differ from the state.
struct CheckpointFieldsRef<'a>(&'a HybridCheckpointV5, &'a [StoredRecentInput]);

impl Encodable for CheckpointFieldsRef<'_> {
    fn length(&self) -> usize {
        fields_length(&[
            &self.0.chain_id,
            &self.0.epoch,
            &self.0.next_synthetic_token,
            &self.0.lifecycle_generation,
            &OwnerGenerationsRef(&self.0.owner_generations),
            &LifecycleIntentRef(&self.0.lifecycle_intent),
            &StoredRecentInputsRef(self.1),
            &BlockRefsRef(&self.0.canonical_history),
            &Optional(self.0.coverage_head.as_ref().map(BlockRefRef)),
            &Optional(self.0.safe_head.as_ref().map(BlockRefRef)),
            &Optional(self.0.finalized_head.as_ref().map(BlockRefRef)),
            &Optional(
                self.0
                    .certified_historical
                    .as_ref()
                    .map(CertifiedHistoricalCoverageRef),
            ),
            &SourcePositionRef(&self.0.historical_position),
            &SourcePositionRef(&self.0.live_position),
            &Optional(
                self.0
                    .last_committed_token
                    .as_ref()
                    .map(StoredCommittedTokenRef),
            ),
        ])
    }

    fn encode(&self, out: &mut dyn BufMut) {
        encode_fields(
            &[
                &self.0.chain_id,
                &self.0.epoch,
                &self.0.next_synthetic_token,
                &self.0.lifecycle_generation,
                &OwnerGenerationsRef(&self.0.owner_generations),
                &LifecycleIntentRef(&self.0.lifecycle_intent),
                &StoredRecentInputsRef(self.1),
                &BlockRefsRef(&self.0.canonical_history),
                &Optional(self.0.coverage_head.as_ref().map(BlockRefRef)),
                &Optional(self.0.safe_head.as_ref().map(BlockRefRef)),
                &Optional(self.0.finalized_head.as_ref().map(BlockRefRef)),
                &Optional(
                    self.0
                        .certified_historical
                        .as_ref()
                        .map(CertifiedHistoricalCoverageRef),
                ),
                &SourcePositionRef(&self.0.historical_position),
                &SourcePositionRef(&self.0.live_position),
                &Optional(
                    self.0
                        .last_committed_token
                        .as_ref()
                        .map(StoredCommittedTokenRef),
                ),
            ],
            out,
        );
    }
}

fn validate_encodable_state(state: &HybridCheckpointV5) -> CodecResult<()> {
    super::validate_checkpoint_state(state)?;
    validate_codec_limits(state, &state.recent_inputs)
}

fn validate_codec_limits(
    state: &HybridCheckpointV5,
    recent_inputs: &[StoredRecentInput],
) -> CodecResult<()> {
    if recent_inputs.len() > MAX_RECENT_INPUTS {
        return Err(provider(format!(
            "hybrid checkpoint recent inputs exceed {MAX_RECENT_INPUTS} entries"
        )));
    }
    for (label, history) in [
        ("coordinator", state.canonical_history.as_slice()),
        (
            "historical source",
            state.historical_position.canonical_history.as_slice(),
        ),
        (
            "live source",
            state.live_position.canonical_history.as_slice(),
        ),
    ] {
        if history.len() > MAX_CANONICAL_HISTORY {
            return Err(provider(format!(
                "hybrid checkpoint {label} history exceeds {MAX_CANONICAL_HISTORY} entries"
            )));
        }
    }

    let mut owner_entries = 0usize;
    validate_owner_map(
        &state.owner_generations,
        "owner generations",
        &mut owner_entries,
    )?;
    validate_owner_map(
        &state.lifecycle_intent.owners,
        "lifecycle intent",
        &mut owner_entries,
    )?;
    for entry in recent_inputs {
        validate_owner_map(
            &entry.coverage.owners,
            "recent-input coverage",
            &mut owner_entries,
        )?;
        let kind = entry.identity.kind();
        if input_kind_tag(kind).is_none()
            || ReactiveInputIdentity::try_from_parts(entry.identity.input_ref(), kind).is_err()
        {
            return Err(provider(
                "hybrid checkpoint contains an unsupported reactive-input identity",
            ));
        }
    }

    validate_opaque_bytes(
        state.historical_position.delivery_token.as_deref(),
        "historical delivery token",
    )?;
    validate_opaque_bytes(
        state.historical_position.checkpoint.as_deref(),
        "historical child checkpoint",
    )?;
    validate_opaque_bytes(
        state.live_position.delivery_token.as_deref(),
        "live delivery token",
    )?;
    validate_opaque_bytes(
        state.live_position.checkpoint.as_deref(),
        "live child checkpoint",
    )?;
    validate_opaque_bytes(
        state
            .last_committed_token
            .as_ref()
            .map(|token| token.inner.as_slice()),
        "last committed token",
    )?;
    validate_decode_budget_for_state(state, recent_inputs)
}

/// Mirror the decoder's conservative allocation accounting over an in-memory
/// state. An encoder must never produce a checkpoint that its own bounded
/// decoder would reject.
fn validate_decode_budget_for_state(
    state: &HybridCheckpointV5,
    recent_inputs: &[StoredRecentInput],
) -> CodecResult<()> {
    let mut budget = DecodeBudget::default();
    budget.claim_nodes(1)?; // top-level payload
    charge_owner_map(&state.owner_generations, &mut budget)?;
    budget.claim_nodes(1)?; // lifecycle intent
    charge_owner_map(&state.lifecycle_intent.owners, &mut budget)?;

    budget.claim_nodes(recent_inputs.len())?;
    budget.claim_heap::<StoredRecentInput>(recent_inputs.len())?;
    for entry in recent_inputs {
        // recent entry, identity, input ref, optional chain id, audience,
        // optional coverage block, and optional witness.
        budget.claim_nodes(7)?;
        charge_owner_map(&entry.coverage.owners, &mut budget)?;
        if entry.coverage.block.is_some() {
            charge_block_ref(&mut budget)?;
        }
        if let Some(witness) = entry.coverage.witness.as_ref() {
            // witness value, optional witness block, and three optional u64s.
            budget.claim_nodes(5)?;
            if witness.block.is_some() {
                charge_block_ref(&mut budget)?;
            }
        }
    }

    charge_block_vec(&state.canonical_history, &mut budget)?;
    for block in [
        &state.coverage_head,
        &state.safe_head,
        &state.finalized_head,
    ] {
        budget.claim_nodes(1)?;
        if block.is_some() {
            charge_block_ref(&mut budget)?;
        }
    }
    budget.claim_nodes(1)?; // optional certified historical coverage
    if state.certified_historical.is_some() {
        budget.claim_nodes(1)?;
        charge_block_ref(&mut budget)?;
    }
    charge_source_position(&state.historical_position, &mut budget)?;
    charge_source_position(&state.live_position, &mut budget)?;
    budget.claim_nodes(1)?; // optional last committed token
    if let Some(token) = state.last_committed_token.as_ref() {
        budget.claim_nodes(2)?; // committed-token value and opaque bytes
        budget.claim_heap_bytes(token.inner.len())?;
    }
    Ok(())
}

fn charge_owner_map<V>(
    owners: &BTreeMap<HandlerId, V>,
    budget: &mut DecodeBudget,
) -> CodecResult<()> {
    let count = owners.len();
    budget.claim_nodes(count.saturating_mul(3).saturating_add(1))?;
    budget.claim_owners(count)?;
    budget.claim_heap_bytes(
        size_of::<(HandlerId, V)>()
            .saturating_add(4 * size_of::<usize>())
            .saturating_mul(count),
    )?;
    for owner in owners.keys() {
        budget.claim_heap_bytes(owner.as_str().len())?;
    }
    Ok(())
}

fn charge_block_ref(budget: &mut DecodeBudget) -> CodecResult<()> {
    // BlockRef value plus its parent-hash and timestamp option wrappers.
    budget.claim_nodes(3)
}

fn charge_block_vec(blocks: &[BlockRef], budget: &mut DecodeBudget) -> CodecResult<()> {
    budget.claim_nodes(blocks.len())?;
    budget.claim_heap::<BlockRef>(blocks.len())?;
    for _ in blocks {
        charge_block_ref(budget)?;
    }
    Ok(())
}

fn charge_optional_bytes(value: Option<&[u8]>, budget: &mut DecodeBudget) -> CodecResult<()> {
    budget.claim_nodes(1)?;
    if let Some(value) = value {
        budget.claim_nodes(1)?;
        budget.claim_heap_bytes(value.len())?;
    }
    Ok(())
}

fn charge_source_position(source: &SourcePosition, budget: &mut DecodeBudget) -> CodecResult<()> {
    budget.claim_nodes(1)?;
    charge_optional_bytes(source.delivery_token.as_deref(), budget)?;
    charge_optional_bytes(source.checkpoint.as_deref(), budget)?;
    budget.claim_nodes(1)?;
    if source.coverage_head.is_some() {
        charge_block_ref(budget)?;
    }
    charge_block_vec(&source.canonical_history, budget)?;
    budget.claim_nodes(1)?; // optional delivery digest
    Ok(())
}

fn validate_owner_map<V>(
    owners: &BTreeMap<HandlerId, V>,
    context: &str,
    owner_entries: &mut usize,
) -> CodecResult<()> {
    *owner_entries = owner_entries
        .checked_add(owners.len())
        .ok_or_else(|| provider("hybrid checkpoint owner count overflow"))?;
    if *owner_entries > MAX_OWNER_ENTRIES {
        return Err(provider(format!(
            "hybrid checkpoint exceeds {MAX_OWNER_ENTRIES} total owner entries"
        )));
    }
    if let Some(owner) = owners
        .keys()
        .find(|owner| owner.as_str().len() > MAX_HANDLER_ID_BYTES)
    {
        return Err(provider(format!(
            "hybrid checkpoint {context} handler id {:?} exceeds {MAX_HANDLER_ID_BYTES} bytes",
            owner.as_str()
        )));
    }
    Ok(())
}

fn validate_opaque_bytes(value: Option<&[u8]>, context: &str) -> CodecResult<()> {
    if value.is_some_and(|value| value.len() > MAX_CHECKPOINT_PAYLOAD_BYTES) {
        return Err(provider(format!(
            "hybrid checkpoint {context} exceeds {MAX_CHECKPOINT_PAYLOAD_BYTES} bytes"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use super::*;

    fn checkpoint() -> HybridCheckpointV5 {
        HybridCheckpointV5 {
            chain_id: 1,
            epoch: [9; 16],
            next_synthetic_token: 7,
            lifecycle_generation: 3,
            owner_generations: BTreeMap::new(),
            lifecycle_intent: LifecycleIntent {
                base: super::super::empty_interest_fingerprint(),
                owners: BTreeMap::new(),
            },
            recent_inputs: Vec::new(),
            canonical_history: Vec::new(),
            coverage_head: None,
            safe_head: None,
            finalized_head: None,
            certified_historical: None,
            historical_position: SourcePosition::default(),
            live_position: SourcePosition::default(),
            last_committed_token: None,
        }
    }

    fn envelope(payload: &[u8]) -> Vec<u8> {
        let mut encoded = vec![0; CHECKPOINT_ENVELOPE_BYTES];
        encoded.extend_from_slice(payload);
        encoded[..8].copy_from_slice(CHECKPOINT_MAGIC_V5);
        encoded[8..10].copy_from_slice(&CHECKPOINT_VERSION_V5.to_be_bytes());
        encoded[10..14].copy_from_slice(&(payload.len() as u32).to_be_bytes());
        encoded[14..18].copy_from_slice(&crc32fast::hash(payload).to_be_bytes());
        encoded
    }

    fn rewrite_top_level_payload(
        encoded: &[u8],
        rewrite: impl FnOnce(&[u8]) -> Vec<u8>,
    ) -> Vec<u8> {
        let payload = &encoded[CHECKPOINT_ENVELOPE_BYTES..];
        let mut fields = payload;
        let header = Header::decode(&mut fields).expect("valid top-level RLP list");
        assert!(header.list);
        assert_eq!(header.payload_length, fields.len());
        let fields = rewrite(fields);
        let mut rewritten = Vec::new();
        Header {
            list: true,
            payload_length: fields.len(),
        }
        .encode(&mut rewritten);
        rewritten.extend_from_slice(&fields);
        envelope(&rewritten)
    }

    fn owner_checkpoint() -> HybridCheckpointV5 {
        let mut state = checkpoint();
        for name in ["owner-a", "owner-b"] {
            let owner = HandlerId::new(name);
            state.owner_generations.insert(owner.clone(), 3);
            state.lifecycle_intent.owners.insert(owner, [0x42; 32]);
        }
        state
    }

    #[test]
    fn canonical_v5_bytes_round_trip_exactly() {
        let encoded = encode_hybrid_checkpoint_v5(&owner_checkpoint()).expect("encode v5");
        let decoded = decode_hybrid_checkpoint_v5(&encoded).expect("decode v5");
        let reencoded = encode_hybrid_checkpoint_v5(&decoded).expect("re-encode v5");
        assert_eq!(reencoded, encoded);
    }

    #[test]
    fn non_canonical_rlp_integer_is_rejected() {
        let encoded = encode_hybrid_checkpoint_v5(&checkpoint()).expect("encode v5");
        let malformed = rewrite_top_level_payload(&encoded, |fields| {
            assert_eq!(fields[0], 1, "fixture chain id uses one canonical byte");
            let mut rewritten = vec![0x81, 0x01];
            rewritten.extend_from_slice(&fields[1..]);
            rewritten
        });
        let error = decode_hybrid_checkpoint_v5(&malformed).expect_err("non-canonical integer");
        assert!(error.to_string().contains("canonical"), "{error}");
    }

    #[test]
    fn top_level_trailing_field_is_rejected() {
        let encoded = encode_hybrid_checkpoint_v5(&checkpoint()).expect("encode v5");
        let malformed = rewrite_top_level_payload(&encoded, |fields| {
            let mut rewritten = fields.to_vec();
            rewritten.push(0x80);
            rewritten
        });
        let error = decode_hybrid_checkpoint_v5(&malformed).expect_err("trailing list field");
        assert!(
            error.to_string().contains("trailing list fields"),
            "{error}"
        );
    }

    #[test]
    fn owner_maps_must_be_strictly_sorted_and_unique() {
        let encoded = encode_hybrid_checkpoint_v5(&owner_checkpoint()).expect("encode owners");
        let first = encoded
            .windows(b"owner-a".len())
            .position(|window| window == b"owner-a")
            .expect("first owner");
        let second = encoded
            .windows(b"owner-b".len())
            .position(|window| window == b"owner-b")
            .expect("second owner");
        assert!(first < second);

        let mut unsorted = encoded.clone();
        unsorted[first + b"owner-a".len() - 1] = b'b';
        unsorted[second + b"owner-b".len() - 1] = b'a';
        let checksum = crc32fast::hash(&unsorted[CHECKPOINT_ENVELOPE_BYTES..]);
        unsorted[14..18].copy_from_slice(&checksum.to_be_bytes());
        let error = decode_hybrid_checkpoint_v5(&unsorted).expect_err("unsorted owner map");
        assert!(error.to_string().contains("strictly sorted"), "{error}");

        let mut duplicate = encoded;
        duplicate[second + b"owner-b".len() - 1] = b'a';
        let checksum = crc32fast::hash(&duplicate[CHECKPOINT_ENVELOPE_BYTES..]);
        duplicate[14..18].copy_from_slice(&checksum.to_be_bytes());
        let error = decode_hybrid_checkpoint_v5(&duplicate).expect_err("duplicate owner map");
        assert!(error.to_string().contains("strictly sorted"), "{error}");
    }

    #[test]
    fn count_handler_and_heap_limits_fail_closed() {
        let too_many_items = vec![0x80; MAX_RECENT_INPUTS + 1];
        assert!(
            count_items(&too_many_items, MAX_RECENT_INPUTS, "test entries")
                .expect_err("count limit")
                .to_string()
                .contains("exceeds")
        );

        let mut oversized_handler = checkpoint();
        let owner = HandlerId::new("x".repeat(MAX_HANDLER_ID_BYTES + 1));
        oversized_handler.owner_generations.insert(owner.clone(), 3);
        oversized_handler
            .lifecycle_intent
            .owners
            .insert(owner, [0x11; 32]);
        assert!(
            encode_hybrid_checkpoint_v5(&oversized_handler)
                .expect_err("handler limit")
                .to_string()
                .contains("handler id")
        );

        let mut budget = DecodeBudget::default();
        assert!(
            budget
                .claim_heap_bytes(MAX_DECODE_HEAP_BYTES + 1)
                .expect_err("heap limit")
                .to_string()
                .contains("heap budget")
        );
    }

    #[test]
    fn corrupt_and_truncated_corpus_never_panics() {
        let encoded = encode_hybrid_checkpoint_v5(&owner_checkpoint()).expect("encode corpus seed");
        for end in 0..encoded.len() {
            let result = catch_unwind(AssertUnwindSafe(|| {
                let _ = decode_hybrid_checkpoint_v5(&encoded[..end]);
            }));
            assert!(result.is_ok(), "decoder panicked at truncation {end}");
        }
        for index in 0..encoded.len() {
            let mut corrupt = encoded.clone();
            corrupt[index] ^= 0x5a;
            let result = catch_unwind(AssertUnwindSafe(|| {
                let _ = decode_hybrid_checkpoint_v5(&corrupt);
            }));
            assert!(result.is_ok(), "decoder panicked at corrupt byte {index}");
        }

        let mut seed = 0x9e37_79b9_u32;
        for len in 0..256usize {
            let mut payload = Vec::with_capacity(len);
            for _ in 0..len {
                seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                payload.push((seed >> 24) as u8);
            }
            let candidate = envelope(&payload);
            let result = catch_unwind(AssertUnwindSafe(|| {
                let _ = decode_hybrid_checkpoint_v5(&candidate);
            }));
            assert!(result.is_ok(), "decoder panicked on corpus length {len}");
        }
    }
}
