//! Canonical, allocation-light transcripts for durable Hybrid witnesses.
//!
//! This module deliberately does not serialize Rust values through Serde or
//! Protocol Buffers. Every primitive has one fixed representation and every
//! caller must assign explicit variant tags. That keeps replay fingerprints
//! stable across dependency upgrades and prevents concatenation ambiguity.

use alloy_primitives::{B256, Keccak256};
use evm_fork_cache::reactive::{BlockRef, HandlerId};

const TRANSCRIPT_PREFIX: &[u8] = b"EFCHY-CANONICAL-TRANSCRIPT\0";

/// Failure to represent a host-sized collection length in the fixed wire
/// transcript.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct TranscriptLengthError;

impl core::fmt::Display for TranscriptLengthError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str("hybrid transcript length does not fit u64")
    }
}

impl std::error::Error for TranscriptLengthError {}

/// Domain-separated Keccak transcript with explicit, prefix-free primitives.
///
/// The encoding is intentionally small:
///
/// - variants, booleans, and option presence use one byte;
/// - integers are fixed-width big endian;
/// - byte strings and sequences carry an eight-byte big-endian length;
/// - hashes are exactly 32 bytes.
///
/// Sequence elements and struct fields remain the caller's responsibility and
/// must be emitted in their documented order. Set-like collections must be
/// sorted before emission.
pub(super) struct CanonicalHasher {
    inner: Keccak256,
}

impl CanonicalHasher {
    /// Start a new transcript under a stable, versioned domain.
    pub(super) fn new(domain: &'static [u8]) -> Self {
        let mut this = Self {
            inner: Keccak256::new(),
        };
        this.inner.update(TRANSCRIPT_PREFIX);
        // Static domains necessarily fit u64 on every supported target.
        this.u64(domain.len() as u64);
        this.inner.update(domain);
        this
    }

    /// Append one explicit enum/union tag.
    pub(super) fn tag(&mut self, tag: u8) {
        self.inner.update([tag]);
    }

    /// Append one canonical boolean.
    pub(super) fn bool(&mut self, value: bool) {
        self.tag(u8::from(value));
    }

    /// Append one fixed-width unsigned integer.
    pub(super) fn u64(&mut self, value: u64) {
        self.inner.update(value.to_be_bytes());
    }

    /// Append a host-sized collection count as a fixed-width integer.
    pub(super) fn sequence_len(&mut self, len: usize) -> Result<(), TranscriptLengthError> {
        self.u64(u64::try_from(len).map_err(|_| TranscriptLengthError)?);
        Ok(())
    }

    /// Append one unambiguous length-delimited byte string.
    pub(super) fn bytes(&mut self, bytes: &[u8]) -> Result<(), TranscriptLengthError> {
        self.sequence_len(bytes.len())?;
        self.inner.update(bytes);
        Ok(())
    }

    /// Append one UTF-8 string.
    pub(super) fn string(&mut self, value: &str) -> Result<(), TranscriptLengthError> {
        self.bytes(value.as_bytes())
    }

    /// Append one handler identifier using its exact UTF-8 representation.
    pub(super) fn handler_id(&mut self, owner: &HandlerId) -> Result<(), TranscriptLengthError> {
        self.string(owner.as_str())
    }

    /// Append one exact 32-byte hash without an additional length prefix.
    pub(super) fn hash(&mut self, hash: &B256) {
        self.inner.update(hash.as_slice());
    }

    /// Append an optional integer.
    pub(super) fn option_u64(&mut self, value: Option<u64>) {
        self.bool(value.is_some());
        if let Some(value) = value {
            self.u64(value);
        }
    }

    /// Append an optional 32-byte hash.
    pub(super) fn option_hash(&mut self, value: Option<&B256>) {
        self.bool(value.is_some());
        if let Some(value) = value {
            self.hash(value);
        }
    }

    /// Append the complete canonical metadata of one block reference.
    pub(super) fn block_ref(&mut self, block: &BlockRef) {
        self.u64(block.number);
        self.hash(&block.hash);
        self.option_hash(block.parent_hash.as_ref());
        self.option_u64(block.timestamp);
    }

    /// Append an optional block reference.
    pub(super) fn option_block_ref(&mut self, block: Option<&BlockRef>) {
        self.bool(block.is_some());
        if let Some(block) = block {
            self.block_ref(block);
        }
    }

    /// Consume the transcript and return its Keccak-256 digest.
    pub(super) fn finish(self) -> [u8; 32] {
        self.inner.finalize().into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn framing_separates_ambiguous_concatenations() {
        let mut left = CanonicalHasher::new(b"test-v1");
        left.sequence_len(2).unwrap();
        left.bytes(b"a").unwrap();
        left.bytes(b"bc").unwrap();

        let mut right = CanonicalHasher::new(b"test-v1");
        right.sequence_len(2).unwrap();
        right.bytes(b"ab").unwrap();
        right.bytes(b"c").unwrap();

        assert_ne!(left.finish(), right.finish());
    }

    #[test]
    fn domains_and_option_presence_are_committed() {
        let mut none = CanonicalHasher::new(b"left-v1");
        none.option_u64(None);

        let mut zero = CanonicalHasher::new(b"left-v1");
        zero.option_u64(Some(0));

        let mut other_domain = CanonicalHasher::new(b"right-v1");
        other_domain.option_u64(None);

        let none = none.finish();
        assert_ne!(none, zero.finish());
        assert_ne!(none, other_domain.finish());
    }
}
