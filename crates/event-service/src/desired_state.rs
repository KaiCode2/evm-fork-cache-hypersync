use std::collections::{HashMap, HashSet};

use evm_fork_cache_event_protocol::{
    PROTOCOL_VERSION,
    v1::{ApplyDesiredState, BlockMode, DesiredStateApplied, portable_interest},
};

/// In-memory authority for committed session desired state.
#[derive(Debug, Default)]
pub struct DesiredStateRegistry {
    sessions: HashMap<(String, u64), ApplyDesiredState>,
}

impl DesiredStateRegistry {
    /// Create an empty desired-state registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Atomically replace a session's desired state when its revision matches.
    ///
    /// # Errors
    ///
    /// Returns [`DesiredStateError`] when the request is structurally invalid,
    /// uses an unsupported protocol/interest, or loses the revision
    /// compare-and-swap. Failure leaves the committed registry unchanged.
    pub fn apply(
        &mut self,
        request: ApplyDesiredState,
    ) -> Result<DesiredStateApplied, DesiredStateError> {
        validate_desired_state(&request)?;
        let key = (request.session_id.clone(), request.chain_id);
        let existing = self.sessions.get(&key);
        let committed = existing.map_or(0, |state| state.new_revision);
        if request.expected_revision != committed {
            if existing == Some(&request) {
                return Ok(DesiredStateApplied {
                    session_id: request.session_id,
                    revision: request.new_revision,
                    activation_sequence: 0,
                });
            }
            return Err(DesiredStateError::RevisionConflict {
                expected: request.expected_revision,
                committed,
            });
        }
        if committed.checked_add(1) != Some(request.new_revision) {
            return Err(DesiredStateError::InvalidState(
                "new revision must immediately follow the committed revision",
            ));
        }

        let applied = DesiredStateApplied {
            session_id: request.session_id.clone(),
            revision: request.new_revision,
            activation_sequence: 0,
        };
        self.sessions.insert(key, request);
        Ok(applied)
    }

    /// Borrow the committed state for a session and chain.
    pub fn committed(&self, session_id: &str, chain_id: u64) -> Option<&ApplyDesiredState> {
        self.sessions.get(&(session_id.to_owned(), chain_id))
    }
}

/// Validate protocol identity, owner topology, filters, and backfill bounds for
/// one complete desired-state replacement.
///
/// # Errors
///
/// Returns [`DesiredStateError`] for an incompatible protocol version,
/// malformed identity/topology/filter/backfill, or an interest protocol v1
/// cannot represent.
pub fn validate_desired_state(request: &ApplyDesiredState) -> Result<(), DesiredStateError> {
    if request.protocol_version != PROTOCOL_VERSION {
        return Err(DesiredStateError::ProtocolVersion {
            received: request.protocol_version,
            supported: PROTOCOL_VERSION,
        });
    }
    if request.session_id.is_empty() {
        return Err(DesiredStateError::InvalidState("session id is empty"));
    }
    if request.expected_revision == u64::MAX {
        return Err(DesiredStateError::InvalidState(
            "expected revision has no representable successor",
        ));
    }
    // Service-level quotas run before this validator. Keep the standalone
    // validator free of attacker-sized eager allocation as well.
    let mut owner_ids = HashSet::new();
    let mut has_canonical_interests = false;
    for owner in &request.owners {
        if owner.canonical {
            if !owner.owner_id.is_empty() {
                return Err(DesiredStateError::InvalidState(
                    "canonical interests must not carry an owner id",
                ));
            }
            if has_canonical_interests {
                return Err(DesiredStateError::InvalidState(
                    "canonical interests appear more than once",
                ));
            }
            has_canonical_interests = true;
        } else if owner.owner_id.is_empty() {
            return Err(DesiredStateError::InvalidState("owner id is empty"));
        } else if !owner_ids.insert(owner.owner_id.as_str()) {
            return Err(DesiredStateError::InvalidState("owner ids must be unique"));
        }
        if let Some(backfill) = owner.backfill.as_ref() {
            if backfill.to_block_excl.is_some_and(|end| {
                end < backfill.from_block
                    || (end == backfill.from_block && backfill.retained_baseline.is_none())
            }) {
                return Err(DesiredStateError::InvalidState(
                    "backfill end precedes its start or encodes an uncertified empty range",
                ));
            }
            if let Some(baseline) = backfill.retained_baseline.as_ref() {
                if baseline.number.checked_add(1) != Some(backfill.from_block) {
                    return Err(DesiredStateError::InvalidState(
                        "retained backfill baseline must immediately precede its start",
                    ));
                }
                if baseline.hash.len() != 32 || baseline.parent_hash.len() != 32 {
                    return Err(DesiredStateError::InvalidState(
                        "retained backfill baseline hashes must be 32 bytes",
                    ));
                }
            }
        }
        for interest in &owner.interests {
            match interest.kind.as_ref() {
                Some(portable_interest::Kind::Block(block)) => {
                    match BlockMode::try_from(block.mode) {
                        Ok(BlockMode::Header) => {}
                        Ok(BlockMode::FullBlock) => {
                            return Err(DesiredStateError::UnsupportedInterest {
                                owner_id: owner.owner_id.clone(),
                                interest: "full block",
                            });
                        }
                        _ => {
                            return Err(DesiredStateError::InvalidState(
                                "block interest mode is unspecified or unknown",
                            ));
                        }
                    }
                }
                Some(portable_interest::Kind::Log(log)) => {
                    if log.addresses.iter().any(|address| address.len() != 20) {
                        return Err(DesiredStateError::InvalidState(
                            "log addresses must be 20 bytes",
                        ));
                    }
                    if log.topics.len() > 4 {
                        return Err(DesiredStateError::InvalidState(
                            "log interests support at most four topic positions",
                        ));
                    }
                    if log
                        .topics
                        .iter()
                        .flat_map(|topic| &topic.values)
                        .any(|topic| topic.len() != 32)
                    {
                        return Err(DesiredStateError::InvalidState(
                            "log topics must be 32 bytes",
                        ));
                    }
                }
                None => {
                    return Err(DesiredStateError::InvalidState(
                        "portable interest kind is missing",
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Desired-state validation or compare-and-swap failure.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DesiredStateError {
    /// The desired state is structurally invalid.
    #[error("invalid desired state: {0}")]
    InvalidState(&'static str),
    /// The client and service do not share a compatible wire version.
    #[error("unsupported protocol version {received}; service supports {supported}")]
    ProtocolVersion {
        /// Version supplied by the client.
        received: u32,
        /// Version supported by this service.
        supported: u32,
    },
    /// A syntactically valid interest is outside the service's supported set.
    #[error("owner `{owner_id}` requested unsupported {interest} interest")]
    UnsupportedInterest {
        /// Owner that requested the interest.
        owner_id: String,
        /// Human-readable interest kind.
        interest: &'static str,
    },
    /// The request was based on a stale committed revision.
    #[error("desired-state revision conflict: expected {expected}, committed {committed}")]
    RevisionConflict {
        /// Revision supplied by the client.
        expected: u64,
        /// Current authoritative revision.
        committed: u64,
    },
}
