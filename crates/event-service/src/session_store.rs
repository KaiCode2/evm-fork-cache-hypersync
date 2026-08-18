use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::Path,
    time::Duration,
};

use alloy_consensus::Header as ConsensusHeader;
use alloy_rlp::Decodable;
use evm_fork_cache_event_protocol::{
    MAX_MESSAGE_SIZE_BYTES,
    v1::{
        Acknowledge, ApplyDesiredState, Barrier, BlockRef, Cursor, Delivery, DeliveryScope,
        DesiredStateApplied, FinalityKind, chain_event, delivery,
    },
};
use prost::Message;
use rusqlite::{Connection, OptionalExtension, params};

use crate::{DesiredStateError, validate_desired_state};

/// Current durable session schema understood by this crate.
pub const SESSION_SCHEMA_VERSION: u32 = 3;

/// Durable state for one runtime session and chain.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct PersistedSession {
    /// Last authoritative desired state.
    pub desired_state: Option<ApplyDesiredState>,
    /// Last cursor committed by a post-ingest acknowledgement.
    pub acknowledged_cursor: Option<Cursor>,
    /// Last acknowledged cursor whose delivery affected runtime/cache state.
    pub runtime_checkpoint_cursor: Option<Cursor>,
    /// One sequenced delivery sent but not yet acknowledged.
    pub pending_delivery: Option<Delivery>,
    /// Replacement tip promised by the last acknowledged reorg control until
    /// an exact canonical replacement delivery is acknowledged.
    pub expected_reorg_tip: Option<BlockRef>,
}

/// SQLite-backed session authority and delivery outbox.
pub struct SessionStore {
    connection: Connection,
}

impl SessionStore {
    /// Open or create a durable SQLite session database.
    ///
    /// # Errors
    ///
    /// Returns [`SessionStoreError`] if SQLite cannot open/configure the file,
    /// the schema is newer or malformed, or an atomic migration fails.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SessionStoreError> {
        Self::from_connection(Connection::open(path)?)
    }

    /// Create an isolated in-memory store, primarily for tests.
    ///
    /// # Errors
    ///
    /// Returns [`SessionStoreError`] if SQLite initialization, schema creation,
    /// or connection configuration fails.
    pub fn open_in_memory() -> Result<Self, SessionStoreError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(mut connection: Connection) -> Result<Self, SessionStoreError> {
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = FULL;
             PRAGMA foreign_keys = ON;",
        )?;
        let schema_version: u32 =
            connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if schema_version > SESSION_SCHEMA_VERSION {
            return Err(SessionStoreError::Schema(format!(
                "database schema version {schema_version} is newer than supported version {SESSION_SCHEMA_VERSION}"
            )));
        }
        let transaction = connection.transaction()?;
        let table_exists = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'sessions')",
            [],
            |row| row.get::<_, bool>(0),
        )?;
        if !table_exists {
            transaction.execute_batch(
                "CREATE TABLE sessions (
                 session_id TEXT NOT NULL,
                 chain_id INTEGER NOT NULL,
                 revision INTEGER NOT NULL,
                 desired_state BLOB,
                 acknowledged_cursor BLOB,
                 runtime_checkpoint_cursor BLOB,
                 pending_delivery BLOB,
                 activation_sequence INTEGER NOT NULL DEFAULT 0,
                 expected_reorg_tip BLOB,
                 PRIMARY KEY (session_id, chain_id)
             );",
            )?;
        } else {
            migrate_sessions_table(&transaction)?;
        }
        transaction.pragma_update(None, "user_version", SESSION_SCHEMA_VERSION)?;
        transaction.commit()?;
        Ok(Self { connection })
    }

    /// Load a session, returning an empty state when it has never connected.
    ///
    /// # Errors
    ///
    /// Returns [`SessionStoreError`] for a database/decode failure or when the
    /// persisted row violates its identity, revision, cursor, delivery, or
    /// reorg invariants.
    pub fn load(
        &self,
        session_id: &str,
        chain_id: u64,
    ) -> Result<PersistedSession, SessionStoreError> {
        let chain_id = chain_id_to_sql_integer(chain_id);
        let row = self
            .connection
            .query_row(
                "SELECT revision, desired_state, acknowledged_cursor,
                        runtime_checkpoint_cursor, pending_delivery, activation_sequence,
                        expected_reorg_tip
                 FROM sessions WHERE session_id = ?1 AND chain_id = ?2",
                params![session_id, chain_id],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Option<Vec<u8>>>(1)?,
                        row.get::<_, Option<Vec<u8>>>(2)?,
                        row.get::<_, Option<Vec<u8>>>(3)?,
                        row.get::<_, Option<Vec<u8>>>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, Option<Vec<u8>>>(6)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            revision,
            desired_state,
            acknowledged_cursor,
            runtime_checkpoint_cursor,
            pending_delivery,
            activation_sequence,
            expected_reorg_tip,
        )) = row
        else {
            return Ok(PersistedSession::default());
        };
        let persisted = PersistedSession {
            desired_state: decode_optional(desired_state, "desired state")?,
            acknowledged_cursor: decode_optional(acknowledged_cursor, "acknowledged cursor")?,
            runtime_checkpoint_cursor: decode_optional(
                runtime_checkpoint_cursor,
                "runtime checkpoint cursor",
            )?,
            pending_delivery: decode_optional(pending_delivery, "pending delivery")?,
            expected_reorg_tip: decode_optional(expected_reorg_tip, "expected reorg tip")?,
        };
        validate_persisted_session(
            session_id,
            chain_id_from_sql_integer(chain_id),
            from_sql_integer(revision, "revision")?,
            sequence_from_sql_integer(activation_sequence),
            &persisted,
        )?;
        Ok(persisted)
    }

    /// Atomically compare-and-swap desired state and enqueue its activation barrier.
    ///
    /// # Errors
    ///
    /// Returns [`SessionStoreError`] when validation or revision comparison
    /// fails, another delivery is pending, sequence/integer/size bounds are
    /// exceeded, or SQLite cannot commit the transaction. Failure performs no
    /// partial desired-state transition.
    pub fn apply_desired_state(
        &mut self,
        request: ApplyDesiredState,
    ) -> Result<DesiredStateApplied, SessionStoreError> {
        self.apply_desired_state_with_cursor(request, None)
    }

    /// Atomically replace desired state using the source-prepared activation
    /// cursor that will become authoritative when its barrier is acknowledged.
    ///
    /// # Errors
    ///
    /// Returns [`SessionStoreError`] for any invalid desired state or prepared
    /// cursor, revision/pending-delivery conflict, exhausted bound, or database
    /// failure. Failure leaves both authority and outbox unchanged.
    pub fn apply_desired_state_with_cursor(
        &mut self,
        request: ApplyDesiredState,
        prepared_cursor: Option<&Cursor>,
    ) -> Result<DesiredStateApplied, SessionStoreError> {
        self.apply_desired_state_with_cursor_and_limit(
            request,
            prepared_cursor,
            MAX_MESSAGE_SIZE_BYTES,
            usize::MAX,
        )
        .map(|(applied, _)| applied)
    }

    pub(crate) fn apply_desired_state_with_cursor_and_limit(
        &mut self,
        request: ApplyDesiredState,
        prepared_cursor: Option<&Cursor>,
        max_delivery_bytes: usize,
        max_persisted_sessions: usize,
    ) -> Result<(DesiredStateApplied, bool), SessionStoreError> {
        validate_desired_state(&request)?;
        // Validate any existing row before using it as CAS authority. Protobuf
        // decode success alone is insufficient when identity/revision fields
        // contradict the composite key.
        let _ = self.load(&request.session_id, request.chain_id)?;
        if prepared_cursor.is_some_and(|cursor| {
            cursor.chain_id != request.chain_id || cursor.query_revision != request.new_revision
        }) {
            return Err(SessionStoreError::InvalidDelivery(
                "prepared activation cursor does not match desired state",
            ));
        }
        let chain_id = chain_id_to_sql_integer(request.chain_id);
        let expected = to_sql_integer(request.expected_revision, "expected revision")?;
        let revision = to_sql_integer(request.new_revision, "new revision")?;
        let transaction = self.connection.transaction()?;
        let committed_row = transaction
            .query_row(
                "SELECT revision, desired_state, acknowledged_cursor,
                        pending_delivery, activation_sequence
                 FROM sessions
                 WHERE session_id = ?1 AND chain_id = ?2",
                params![request.session_id, chain_id],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Option<Vec<u8>>>(1)?,
                        row.get::<_, Option<Vec<u8>>>(2)?,
                        row.get::<_, Option<Vec<u8>>>(3)?,
                        row.get::<_, i64>(4)?,
                    ))
                },
            )
            .optional()?;
        let committed = committed_row.as_ref().map_or(0, |row| row.0);
        if committed_row.is_none() {
            let persisted_count: i64 =
                transaction.query_row("SELECT COUNT(*) FROM sessions", [], |row| row.get(0))?;
            let persisted_count = usize::try_from(persisted_count)
                .map_err(|_| SessionStoreError::IntegerRange("persisted session count"))?;
            if persisted_count >= max_persisted_sessions {
                return Err(SessionStoreError::PersistedSessionLimit {
                    limit: max_persisted_sessions,
                });
            }
        }
        if committed != expected {
            if committed == revision
                && let Some(row) = committed_row
                && let Some(encoded) = row.1
                && ApplyDesiredState::decode(encoded.as_slice())? == request
            {
                return Ok((
                    DesiredStateApplied {
                        session_id: request.session_id,
                        revision: request.new_revision,
                        activation_sequence: sequence_from_sql_integer(row.4),
                    },
                    false,
                ));
            }
            return Err(SessionStoreError::RevisionConflict {
                expected: request.expected_revision,
                committed: from_sql_integer(committed, "committed revision")?,
            });
        }
        if request.expected_revision.checked_add(1) != Some(request.new_revision) {
            return Err(SessionStoreError::DesiredState(
                DesiredStateError::InvalidState(
                    "new revision must immediately follow the committed revision",
                ),
            ));
        }
        if committed_row.as_ref().is_some_and(|row| row.3.is_some()) {
            return Err(SessionStoreError::PendingDelivery);
        }
        let acknowledged_cursor: Option<Cursor> = committed_row
            .as_ref()
            .and_then(|row| row.2.clone())
            .map(|encoded| Cursor::decode(encoded.as_slice()))
            .transpose()?;
        let activation_sequence = acknowledged_cursor.as_ref().map_or(Ok(1), |cursor| {
            cursor
                .batch_sequence
                .checked_add(1)
                .ok_or(SessionStoreError::SequenceOverflow)
        })?;
        let mut cursor = prepared_cursor.cloned().unwrap_or_else(|| {
            acknowledged_cursor.clone().unwrap_or_else(|| Cursor {
                chain_id: request.chain_id,
                query_revision: request.new_revision,
                next_block: request
                    .owners
                    .iter()
                    .filter_map(|owner| owner.backfill.as_ref().map(|backfill| backfill.from_block))
                    .min()
                    .unwrap_or_default(),
                canonical_head: None,
                batch_sequence: 0,
                provider_checkpoint: Vec::new(),
                owner_backfill_activation_block: None,
            })
        });
        cursor.query_revision = request.new_revision;
        cursor.batch_sequence = activation_sequence;
        let activation = Delivery {
            session_id: request.session_id.clone(),
            sequence: activation_sequence,
            query_revision: request.new_revision,
            delivery_token: activation_sequence.to_be_bytes().to_vec(),
            cursor: Some(cursor.clone()),
            payload: Some(delivery::Payload::Barrier(Barrier {
                id: format!("desired-state:{}", request.new_revision).into_bytes(),
                block: None,
            })),
            checkpoint_neutral: true,
        };
        validate_delivery_size(&activation, max_delivery_bytes.min(MAX_MESSAGE_SIZE_BYTES))?;
        validate_delivery_payload(&request, &activation)?;
        validate_delivery_progress(acknowledged_cursor.as_ref(), &request, &activation)?;
        let activation_sequence_sql = sequence_to_sql_integer(activation_sequence);
        transaction.execute(
            "INSERT INTO sessions (
                 session_id, chain_id, revision, desired_state,
                 pending_delivery, activation_sequence
             )
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(session_id, chain_id) DO UPDATE SET
                 revision = excluded.revision,
                 desired_state = excluded.desired_state,
                 pending_delivery = excluded.pending_delivery,
                 activation_sequence = excluded.activation_sequence",
            params![
                request.session_id,
                chain_id,
                revision,
                request.encode_to_vec(),
                activation.encode_to_vec(),
                activation_sequence_sql,
            ],
        )?;
        transaction.commit()?;
        Ok((
            DesiredStateApplied {
                session_id: request.session_id,
                revision: request.new_revision,
                activation_sequence,
            },
            true,
        ))
    }

    /// Persist the only delivery that may be in flight for a session.
    ///
    /// # Errors
    ///
    /// Returns [`SessionStoreError`] when the session is unknown, the desired
    /// state is not authoritative, the delivery is invalid/oversized, a
    /// different delivery is pending, or SQLite cannot persist it.
    pub fn save_pending(
        &mut self,
        desired_state: &ApplyDesiredState,
        delivery: &Delivery,
    ) -> Result<(), SessionStoreError> {
        self.save_pending_with_status(desired_state, delivery)
            .map(|_| ())
    }

    pub(crate) fn save_pending_with_status(
        &mut self,
        desired_state: &ApplyDesiredState,
        delivery: &Delivery,
    ) -> Result<bool, SessionStoreError> {
        let cursor = delivery
            .cursor
            .as_ref()
            .ok_or(SessionStoreError::InvalidDelivery(
                "delivery cursor is missing",
            ))?;
        if delivery.session_id != desired_state.session_id
            || cursor.chain_id != desired_state.chain_id
            || delivery.query_revision != desired_state.new_revision
            || cursor.query_revision != desired_state.new_revision
        {
            return Err(SessionStoreError::InvalidDelivery(
                "delivery identity or revision does not match the active desired state",
            ));
        }
        let chain_id = chain_id_to_sql_integer(desired_state.chain_id);
        let existing = self.load(&desired_state.session_id, desired_state.chain_id)?;
        let desired = existing
            .desired_state
            .ok_or(SessionStoreError::UnknownSession)?;
        if desired != *desired_state {
            return Err(SessionStoreError::InvalidDelivery(
                "delivery desired state is not authoritative",
            ));
        }
        if delivery.sequence != cursor.batch_sequence
            || delivery.delivery_token != delivery.sequence.to_be_bytes()
            || delivery.payload.is_none()
            || delivery.sequence
                != existing
                    .acknowledged_cursor
                    .as_ref()
                    .map_or(Ok(1), |cursor| {
                        cursor
                            .batch_sequence
                            .checked_add(1)
                            .ok_or(SessionStoreError::SequenceOverflow)
                    })?
        {
            return Err(SessionStoreError::InvalidDelivery(
                "delivery sequence, token, cursor, payload, or predecessor is invalid",
            ));
        }
        validate_delivery_size(delivery, MAX_MESSAGE_SIZE_BYTES)?;
        validate_delivery_payload(desired_state, delivery)?;
        validate_delivery_progress(
            existing.acknowledged_cursor.as_ref(),
            desired_state,
            delivery,
        )?;
        if existing
            .expected_reorg_tip
            .as_ref()
            .is_some_and(|expected| !delivery_certifies_reorg_tip(delivery, expected))
        {
            return Err(SessionStoreError::InvalidDelivery(
                "delivery does not certify the promised reorg replacement tip",
            ));
        }
        if let Some(pending) = existing.pending_delivery {
            if pending == *delivery {
                return Ok(false);
            }
            return Err(SessionStoreError::PendingDelivery);
        }
        self.connection.execute(
            "UPDATE sessions SET pending_delivery = ?3
             WHERE session_id = ?1 AND chain_id = ?2",
            params![desired_state.session_id, chain_id, delivery.encode_to_vec()],
        )?;
        Ok(true)
    }

    /// Commit a matching delivery and cursor, clearing the durable outbox.
    ///
    /// # Errors
    ///
    /// Returns [`SessionStoreError`] when the session or pending delivery is
    /// missing, the token/sequence does not match, persisted data is invalid,
    /// or SQLite cannot atomically commit the cursor and clear the outbox.
    pub fn acknowledge(
        &mut self,
        chain_id: u64,
        acknowledgement: &Acknowledge,
    ) -> Result<Cursor, SessionStoreError> {
        self.acknowledge_with_status(chain_id, acknowledgement)
            .map(|(cursor, _)| cursor)
    }

    pub(crate) fn acknowledge_with_status(
        &mut self,
        chain_id: u64,
        acknowledgement: &Acknowledge,
    ) -> Result<(Cursor, bool), SessionStoreError> {
        let _ = self.load(&acknowledgement.session_id, chain_id)?;
        let sql_chain_id = chain_id_to_sql_integer(chain_id);
        let transaction = self.connection.transaction()?;
        let row = transaction
            .query_row(
                "SELECT acknowledged_cursor, runtime_checkpoint_cursor, pending_delivery,
                        expected_reorg_tip FROM sessions
                 WHERE session_id = ?1 AND chain_id = ?2",
                params![acknowledgement.session_id, sql_chain_id],
                |row| {
                    Ok((
                        row.get::<_, Option<Vec<u8>>>(0)?,
                        row.get::<_, Option<Vec<u8>>>(1)?,
                        row.get::<_, Option<Vec<u8>>>(2)?,
                        row.get::<_, Option<Vec<u8>>>(3)?,
                    ))
                },
            )
            .optional()?
            .ok_or(SessionStoreError::UnknownSession)?;
        let acknowledged: Option<Cursor> = decode_optional(row.0, "acknowledged cursor")?;
        let runtime_checkpoint: Option<Cursor> =
            decode_optional(row.1, "runtime checkpoint cursor")?;
        let expected_reorg_tip: Option<BlockRef> = decode_optional(row.3, "expected reorg tip")?;
        let Some(encoded_pending) = row.2 else {
            if let Some(cursor) = acknowledged
                && cursor.batch_sequence == acknowledgement.sequence
                && acknowledgement.delivery_token == acknowledgement.sequence.to_be_bytes()
            {
                return Ok((cursor, false));
            }
            return if acknowledgement.delivery_token != acknowledgement.sequence.to_be_bytes() {
                Err(SessionStoreError::DeliveryTokenMismatch)
            } else {
                Err(SessionStoreError::NoPendingDelivery)
            };
        };
        let pending = Delivery::decode(encoded_pending.as_slice())?;
        if pending.sequence != acknowledgement.sequence
            || pending.delivery_token != acknowledgement.delivery_token
        {
            return Err(SessionStoreError::DeliveryTokenMismatch);
        }
        let next_expected_reorg_tip =
            expected_reorg_tip_after_ack(expected_reorg_tip.as_ref(), &pending)?;
        let cursor = pending.cursor.ok_or(SessionStoreError::InvalidDelivery(
            "delivery cursor is missing",
        ))?;
        let encoded_cursor = cursor.encode_to_vec();
        let encoded_runtime_checkpoint = if pending.checkpoint_neutral {
            runtime_checkpoint.map(|cursor| cursor.encode_to_vec())
        } else {
            Some(encoded_cursor.clone())
        };
        transaction.execute(
            "UPDATE sessions
             SET acknowledged_cursor = ?3,
                 runtime_checkpoint_cursor = ?4,
                 pending_delivery = NULL,
                 expected_reorg_tip = ?5
             WHERE session_id = ?1 AND chain_id = ?2",
            params![
                acknowledgement.session_id,
                sql_chain_id,
                encoded_cursor,
                encoded_runtime_checkpoint,
                next_expected_reorg_tip.map(|tip| tip.encode_to_vec()),
            ],
        )?;
        transaction.commit()?;
        Ok((cursor, true))
    }
}

fn validate_delivery_size(
    delivery: &Delivery,
    max_delivery_bytes: usize,
) -> Result<(), SessionStoreError> {
    let delivery_bytes = delivery.encoded_len();
    // `ServerMessage.delivery` is length-delimited field 3: one-byte tag,
    // varint length, then the already-counted Delivery bytes. This is exact
    // and avoids cloning attacker-sized nested vectors merely to measure them.
    let encoded_bytes = delivery_bytes
        .saturating_add(prost::length_delimiter_len(delivery_bytes))
        .saturating_add(1);
    if encoded_bytes > max_delivery_bytes {
        return Err(SessionStoreError::DeliveryTooLarge {
            encoded_bytes,
            max_delivery_bytes,
        });
    }
    Ok(())
}

fn validate_delivery_payload(
    desired_state: &ApplyDesiredState,
    delivery: &Delivery,
) -> Result<(), SessionStoreError> {
    if let Some(head) = delivery
        .cursor
        .as_ref()
        .and_then(|cursor| cursor.canonical_head.as_ref())
    {
        validate_block_ref(head)?;
    }
    let Some(payload) = delivery.payload.as_ref() else {
        return Err(SessionStoreError::InvalidDelivery(
            "delivery payload is missing",
        ));
    };
    let is_neutral_control = is_activation_delivery(delivery, desired_state.new_revision)
        || is_cursor_progress_barrier(delivery);
    if delivery.checkpoint_neutral != is_neutral_control {
        return Err(SessionStoreError::InvalidDelivery(
            "checkpoint-neutral marker must identify exactly an activation or scan-only progress barrier",
        ));
    }
    let delivery::Payload::Data(data) = payload else {
        return match payload {
            delivery::Payload::Reorg(reorg) => {
                let ancestor =
                    reorg
                        .common_ancestor
                        .as_ref()
                        .ok_or(SessionStoreError::InvalidDelivery(
                            "reorg is missing its common ancestor",
                        ))?;
                let old_tip = reorg
                    .old_tip
                    .as_ref()
                    .ok_or(SessionStoreError::InvalidDelivery(
                        "reorg is missing its old tip",
                    ))?;
                let new_tip = reorg
                    .new_tip
                    .as_ref()
                    .ok_or(SessionStoreError::InvalidDelivery(
                        "reorg is missing its new tip",
                    ))?;
                for block in [ancestor, old_tip, new_tip] {
                    validate_block_ref(block)?;
                }
                validate_reorg_shape(ancestor, old_tip, new_tip)
            }
            delivery::Payload::Finality(finality) => {
                if matches!(
                    FinalityKind::try_from(finality.kind),
                    Err(_) | Ok(FinalityKind::Unspecified)
                ) {
                    return Err(SessionStoreError::InvalidDelivery(
                        "finality kind is unspecified or unknown",
                    ));
                }
                validate_block_ref(finality.block.as_ref().ok_or(
                    SessionStoreError::InvalidDelivery("finality block is missing"),
                )?)
            }
            delivery::Payload::Barrier(barrier) => {
                if barrier.id.is_empty() {
                    return Err(SessionStoreError::InvalidDelivery(
                        "barrier identifier is empty",
                    ));
                }
                if let Some(block) = barrier.block.as_ref() {
                    validate_block_ref(block)?;
                }
                Ok(())
            }
            delivery::Payload::Data(_) => unreachable!("data payload was matched above"),
        };
    };
    if data.records.is_empty() {
        return Err(SessionStoreError::InvalidDelivery(
            "data delivery contains no records",
        ));
    }
    let known_owners: HashSet<&str> = desired_state
        .owners
        .iter()
        .filter(|owner| !owner.canonical)
        .map(|owner| owner.owner_id.as_str())
        .collect();
    let mut explicit_headers = HashSet::new();
    let mut explicit_progress = HashSet::new();
    let mut block_identities = HashMap::<u64, (Vec<u8>, u64)>::new();
    let mut log_identities = HashSet::new();
    let mut transaction_indexes = HashMap::<(Vec<u8>, Vec<u8>), u64>::new();
    let mut transaction_hashes = HashMap::<(Vec<u8>, u64), Vec<u8>>::new();
    let mut last_event_order = None;
    let mut highest_canonical_record = None;
    let mut highest_canonical_coverage = None;
    for record in &data.records {
        let scope = DeliveryScope::try_from(record.scope).map_err(|_| {
            SessionStoreError::InvalidDelivery("data record has an unknown delivery scope")
        })?;
        if scope == DeliveryScope::Unspecified {
            return Err(SessionStoreError::InvalidDelivery(
                "data record has an unspecified delivery scope",
            ));
        }
        if record.canonical_audience {
            if !record.owner_ids.is_empty() {
                return Err(SessionStoreError::InvalidDelivery(
                    "canonical audience also names owners",
                ));
            }
        } else {
            let mut seen = HashSet::with_capacity(record.owner_ids.len());
            if record.owner_ids.is_empty()
                || record.owner_ids.iter().any(|owner| {
                    owner.is_empty()
                        || !known_owners.contains(owner.as_str())
                        || !seen.insert(owner.as_str())
                })
            {
                return Err(SessionStoreError::InvalidDelivery(
                    "owner audience is empty, duplicated, or not authoritative",
                ));
            }
        }
        if scope == DeliveryScope::OwnerCatchup && record.canonical_audience {
            return Err(SessionStoreError::InvalidDelivery(
                "owner catch-up cannot use canonical broadcast audience",
            ));
        }
        let event = record
            .event
            .as_ref()
            .and_then(|event| event.event.as_ref())
            .ok_or(SessionStoreError::InvalidDelivery(
                "data record is missing its chain event",
            ))?;
        let event_order = match event {
            chain_event::Event::BlockHeader(header) => (
                header.block.as_ref().map_or(0, |block| block.number),
                0_u8,
                0,
                0,
            ),
            chain_event::Event::Log(log) => {
                (log.block_number, 1_u8, log.transaction_index, log.log_index)
            }
            chain_event::Event::BlockProgress(progress) => (
                progress.block.as_ref().map_or(0, |block| block.number),
                2_u8,
                0,
                0,
            ),
        };
        if scope != DeliveryScope::OwnerCatchup {
            highest_canonical_record = Some(
                highest_canonical_record
                    .map_or(event_order.0, |known: u64| known.max(event_order.0)),
            );
            if matches!(
                event,
                chain_event::Event::BlockHeader(_) | chain_event::Event::BlockProgress(_)
            ) {
                highest_canonical_coverage = Some(
                    highest_canonical_coverage
                        .map_or(event_order.0, |known: u64| known.max(event_order.0)),
                );
            }
        }
        if last_event_order.is_some_and(|previous| event_order < previous) {
            return Err(SessionStoreError::InvalidDelivery(
                "data records are not in canonical event order",
            ));
        }
        last_event_order = Some(event_order);
        match event {
            chain_event::Event::Log(log) => {
                if log.address.len() != 20
                    || log.block_hash.len() != 32
                    || log.transaction_hash.len() != 32
                    || log.topics.len() > 4
                    || log.topics.iter().any(|topic| topic.len() != 32)
                {
                    return Err(SessionStoreError::InvalidDelivery(
                        "log record has an invalid address, hash, or topic shape",
                    ));
                }
                if !log_identities.insert((log.block_hash.clone(), log.log_index)) {
                    return Err(SessionStoreError::InvalidDelivery(
                        "delivery contains the same log more than once",
                    ));
                }
                if transaction_indexes
                    .insert(
                        (log.block_hash.clone(), log.transaction_hash.clone()),
                        log.transaction_index,
                    )
                    .is_some_and(|known| known != log.transaction_index)
                    || transaction_hashes
                        .insert(
                            (log.block_hash.clone(), log.transaction_index),
                            log.transaction_hash.clone(),
                        )
                        .is_some_and(|known| known != log.transaction_hash)
                {
                    return Err(SessionStoreError::InvalidDelivery(
                        "logs disagree on transaction hash/index identity within a block",
                    ));
                }
                validate_block_identity(
                    &mut block_identities,
                    log.block_number,
                    &log.block_hash,
                    log.block_timestamp,
                )?;
            }
            chain_event::Event::BlockHeader(header) => {
                let block = header
                    .block
                    .as_ref()
                    .ok_or(SessionStoreError::InvalidDelivery(
                        "block header reference is missing",
                    ))?;
                validate_block_ref(block)?;
                if header.consensus_header_rlp.is_empty()
                    || header.total_difficulty.len() > 32
                    || header.size.len() > 32
                {
                    return Err(SessionStoreError::InvalidDelivery(
                        "block header encoding or quantity width is invalid",
                    ));
                }
                if !explicit_headers.insert(block.number) {
                    return Err(SessionStoreError::InvalidDelivery(
                        "delivery contains more than one full header record at a height",
                    ));
                }
                validate_block_identity(
                    &mut block_identities,
                    block.number,
                    &block.hash,
                    block.timestamp,
                )?;
                validate_consensus_header(&header.consensus_header_rlp, block)?;
            }
            chain_event::Event::BlockProgress(progress) => {
                let block = progress
                    .block
                    .as_ref()
                    .ok_or(SessionStoreError::InvalidDelivery(
                        "block progress reference is missing",
                    ))?;
                validate_block_ref(block)?;
                if !explicit_progress.insert(block.number) {
                    return Err(SessionStoreError::InvalidDelivery(
                        "delivery contains more than one compact progress record at a height",
                    ));
                }
                validate_block_identity(
                    &mut block_identities,
                    block.number,
                    &block.hash,
                    block.timestamp,
                )?;
            }
        }
        if scope == DeliveryScope::OwnerCatchup
            && matches!(event, chain_event::Event::BlockProgress(_))
        {
            return Err(SessionStoreError::InvalidDelivery(
                "owner catch-up cannot advance canonical block progress",
            ));
        }
    }
    if highest_canonical_record
        .zip(highest_canonical_coverage)
        .is_none_or(|(record, coverage)| coverage < record)
        && highest_canonical_record.is_some()
    {
        return Err(SessionStoreError::InvalidDelivery(
            "canonical data is not certified by a final block identity at or above every canonical record",
        ));
    }
    Ok(())
}

fn validate_delivery_progress(
    acknowledged_cursor: Option<&Cursor>,
    desired_state: &ApplyDesiredState,
    delivery: &Delivery,
) -> Result<(), SessionStoreError> {
    let cursor = delivery
        .cursor
        .as_ref()
        .ok_or(SessionStoreError::InvalidDelivery(
            "delivery cursor is missing",
        ))?;
    let payload = delivery
        .payload
        .as_ref()
        .ok_or(SessionStoreError::InvalidDelivery(
            "delivery payload is missing",
        ))?;
    let is_reorg = matches!(payload, delivery::Payload::Reorg(_));
    let is_activation = is_exact_activation(acknowledged_cursor, desired_state, delivery);
    validate_owner_backfill_activation_boundary(
        acknowledged_cursor,
        desired_state,
        cursor,
        is_activation,
    )?;
    if !is_reorg
        && acknowledged_cursor.is_some_and(|acknowledged| {
            (cursor.next_block < acknowledged.next_block && !is_activation)
                || canonical_head_regresses_or_changes(acknowledged, cursor)
        })
    {
        return Err(SessionStoreError::InvalidDelivery(
            "non-reorg delivery cursor regresses canonical progress",
        ));
    }
    if let delivery::Payload::Reorg(reorg) = payload {
        let ancestor = reorg
            .common_ancestor
            .as_ref()
            .ok_or(SessionStoreError::InvalidDelivery(
                "reorg is missing its common ancestor",
            ))?;
        let old_tip = reorg
            .old_tip
            .as_ref()
            .ok_or(SessionStoreError::InvalidDelivery(
                "reorg is missing its old tip",
            ))?;
        let new_tip = reorg
            .new_tip
            .as_ref()
            .ok_or(SessionStoreError::InvalidDelivery(
                "reorg is missing its new tip",
            ))?;
        if acknowledged_cursor.and_then(|cursor| cursor.canonical_head.as_ref()) != Some(old_tip)
            || old_tip.number <= ancestor.number
            || new_tip.number <= ancestor.number
            || ancestor.number.checked_add(1) != Some(cursor.next_block)
        {
            return Err(SessionStoreError::InvalidDelivery(
                "reorg ancestry, prior tip, or cursor successor is invalid",
            ));
        }
    }

    let declared_head = match payload {
        delivery::Payload::Data(data) => {
            let mut head = None;
            let mut last_number = None;
            for record in &data.records {
                let scope = DeliveryScope::try_from(record.scope).map_err(|_| {
                    SessionStoreError::InvalidDelivery("data record has an unknown delivery scope")
                })?;
                if scope == DeliveryScope::OwnerCatchup {
                    continue;
                }
                let event = record
                    .event
                    .as_ref()
                    .and_then(|event| event.event.as_ref())
                    .ok_or(SessionStoreError::InvalidDelivery(
                        "data record is missing its chain event",
                    ))?;
                let block = match event {
                    chain_event::Event::BlockHeader(header) => header.block.as_ref(),
                    chain_event::Event::BlockProgress(progress) => progress.block.as_ref(),
                    chain_event::Event::Log(_) => None,
                };
                if let Some(block) = block {
                    if last_number.is_some_and(|previous| block.number < previous) {
                        return Err(SessionStoreError::InvalidDelivery(
                            "canonical block records are not ordered by height",
                        ));
                    }
                    last_number = Some(block.number);
                    head = Some(block);
                }
            }
            head.or_else(|| {
                data.records
                    .iter()
                    .all(|record| record.scope == i32::from(DeliveryScope::OwnerCatchup))
                    .then(|| {
                        acknowledged_cursor
                            .and_then(|acknowledged| acknowledged.canonical_head.as_ref())
                    })
                    .flatten()
            })
        }
        delivery::Payload::Reorg(reorg) => reorg.common_ancestor.as_ref(),
        delivery::Payload::Finality(_) => {
            acknowledged_cursor.and_then(|acknowledged| acknowledged.canonical_head.as_ref())
        }
        delivery::Payload::Barrier(barrier) => barrier
            .block
            .as_ref()
            .or_else(|| {
                acknowledged_cursor.and_then(|acknowledged| acknowledged.canonical_head.as_ref())
            })
            .or({
                // A first global activation has no acknowledged predecessor.
                // Its retained baseline is nevertheless exact authority because
                // `is_exact_activation` matched the cursor against desired state.
                if is_activation {
                    cursor.canonical_head.as_ref()
                } else {
                    None
                }
            }),
    };

    if cursor.canonical_head.as_ref() != declared_head {
        return Err(SessionStoreError::InvalidDelivery(
            "delivery cursor canonical head disagrees with its payload",
        ));
    }
    let cursor_is_behind_coverage = cursor
        .canonical_head
        .as_ref()
        .is_some_and(|head| cursor.next_block <= head.number);
    let is_owner_catchup_data = matches!(payload, delivery::Payload::Data(data)
    if data.records.iter().all(|record| {
        record.scope == i32::from(DeliveryScope::OwnerCatchup)
    }));
    let is_scan_progress = is_cursor_progress_barrier(delivery);
    let preserves_acknowledged_head = acknowledged_cursor
        .is_none_or(|acknowledged| cursor.canonical_head == acknowledged.canonical_head);
    if cursor_is_behind_coverage
        && !(preserves_acknowledged_head
            && (is_activation || is_owner_catchup_data || is_scan_progress))
    {
        return Err(SessionStoreError::InvalidDelivery(
            "delivery cursor next block does not follow its canonical head",
        ));
    }
    Ok(())
}

fn validate_owner_backfill_activation_boundary(
    acknowledged_cursor: Option<&Cursor>,
    desired_state: &ApplyDesiredState,
    cursor: &Cursor,
    is_activation: bool,
) -> Result<(), SessionStoreError> {
    if is_activation {
        let has_open_backfill = desired_state.owners.iter().any(|owner| {
            owner
                .backfill
                .as_ref()
                .is_some_and(|backfill| backfill.to_block_excl.is_none())
        });
        if has_open_backfill && cursor.owner_backfill_activation_block.is_none() {
            return Err(SessionStoreError::InvalidDelivery(
                "open owner backfill activation is missing its portable boundary",
            ));
        }
        if cursor
            .owner_backfill_activation_block
            .is_some_and(|activation| activation < cursor.next_block)
        {
            return Err(SessionStoreError::InvalidDelivery(
                "owner backfill activation boundary precedes its scan cursor",
            ));
        }
    } else if acknowledged_cursor.is_some_and(|acknowledged| {
        acknowledged.query_revision == cursor.query_revision
            && acknowledged.owner_backfill_activation_block
                != cursor.owner_backfill_activation_block
    }) {
        return Err(SessionStoreError::InvalidDelivery(
            "owner backfill activation boundary changed within one revision",
        ));
    }
    Ok(())
}

fn validate_block_identity(
    identities: &mut HashMap<u64, (Vec<u8>, u64)>,
    number: u64,
    hash: &[u8],
    timestamp: u64,
) -> Result<(), SessionStoreError> {
    if let Some((known_hash, known_timestamp)) = identities.get(&number) {
        if known_hash.as_slice() != hash || *known_timestamp != timestamp {
            return Err(SessionStoreError::InvalidDelivery(
                "records for one block height disagree on hash or timestamp",
            ));
        }
    } else {
        identities.insert(number, (hash.to_vec(), timestamp));
    }
    Ok(())
}

fn validate_consensus_header(encoded: &[u8], block: &BlockRef) -> Result<(), SessionStoreError> {
    let mut remaining = encoded;
    let header = ConsensusHeader::decode(&mut remaining).map_err(|_| {
        SessionStoreError::InvalidDelivery("block header contains invalid consensus RLP")
    })?;
    if !remaining.is_empty()
        || header.hash_slow().as_slice() != block.hash.as_slice()
        || header.number != block.number
        || header.parent_hash.as_slice() != block.parent_hash.as_slice()
        || header.timestamp != block.timestamp
    {
        return Err(SessionStoreError::InvalidDelivery(
            "block header RLP does not match its block reference",
        ));
    }
    Ok(())
}

fn validate_block_ref(block: &BlockRef) -> Result<(), SessionStoreError> {
    if block.hash.len() != 32 || block.parent_hash.len() != 32 {
        return Err(SessionStoreError::InvalidDelivery(
            "block reference hashes must be 32 bytes",
        ));
    }
    Ok(())
}

fn validate_reorg_shape(
    ancestor: &BlockRef,
    old_tip: &BlockRef,
    new_tip: &BlockRef,
) -> Result<(), SessionStoreError> {
    let direct_old_parent_mismatch =
        old_tip.number == ancestor.number.saturating_add(1) && old_tip.parent_hash != ancestor.hash;
    let direct_new_parent_mismatch =
        new_tip.number == ancestor.number.saturating_add(1) && new_tip.parent_hash != ancestor.hash;
    if old_tip.number <= ancestor.number
        || new_tip.number <= ancestor.number
        || old_tip.timestamp <= ancestor.timestamp
        || new_tip.timestamp <= ancestor.timestamp
        || old_tip.hash == new_tip.hash
        || old_tip.hash == ancestor.hash
        || new_tip.hash == ancestor.hash
        || direct_old_parent_mismatch
        || direct_new_parent_mismatch
    {
        return Err(SessionStoreError::InvalidDelivery(
            "reorg tips do not describe two distinct descendant branches",
        ));
    }
    Ok(())
}

fn validate_persisted_session(
    session_id: &str,
    chain_id: u64,
    revision: u64,
    activation_sequence: u64,
    persisted: &PersistedSession,
) -> Result<(), SessionStoreError> {
    let desired_state = match persisted.desired_state.as_ref() {
        Some(desired) => {
            validate_desired_state(desired)?;
            if desired.session_id != session_id
                || desired.chain_id != chain_id
                || desired.new_revision != revision
            {
                return Err(SessionStoreError::InvalidDelivery(
                    "durable desired state does not match its row identity or revision",
                ));
            }
            desired
        }
        None if revision == 0
            && persisted.acknowledged_cursor.is_none()
            && persisted.runtime_checkpoint_cursor.is_none()
            && persisted.pending_delivery.is_none()
            && persisted.expected_reorg_tip.is_none() =>
        {
            return Ok(());
        }
        None => {
            return Err(SessionStoreError::InvalidDelivery(
                "durable revision is missing its desired state",
            ));
        }
    };

    if let Some(cursor) = persisted.acknowledged_cursor.as_ref() {
        if cursor.chain_id != chain_id
            || cursor.query_revision > revision
            || cursor.batch_sequence == 0
        {
            return Err(SessionStoreError::InvalidDelivery(
                "durable acknowledged cursor does not match its row",
            ));
        }
        if let Some(head) = cursor.canonical_head.as_ref() {
            validate_block_ref(head)?;
        }
    }
    if let Some(runtime_cursor) = persisted.runtime_checkpoint_cursor.as_ref() {
        if runtime_cursor.chain_id != chain_id
            || runtime_cursor.query_revision > revision
            || runtime_cursor.batch_sequence == 0
        {
            return Err(SessionStoreError::InvalidDelivery(
                "durable runtime checkpoint cursor does not match its row",
            ));
        }
        if let Some(head) = runtime_cursor.canonical_head.as_ref() {
            validate_block_ref(head)?;
        }
        let transport_cursor =
            persisted
                .acknowledged_cursor
                .as_ref()
                .ok_or(SessionStoreError::InvalidDelivery(
                    "runtime checkpoint cursor exists without transport acknowledgement",
                ))?;
        if runtime_cursor.batch_sequence > transport_cursor.batch_sequence
            || (runtime_cursor.batch_sequence == transport_cursor.batch_sequence
                && runtime_cursor != transport_cursor)
        {
            return Err(SessionStoreError::InvalidDelivery(
                "runtime checkpoint cursor is ahead of or conflicts with transport authority",
            ));
        }
    }
    if let Some(expected_tip) = persisted.expected_reorg_tip.as_ref() {
        validate_block_ref(expected_tip)?;
        if persisted
            .acknowledged_cursor
            .as_ref()
            .and_then(|cursor| cursor.canonical_head.as_ref())
            .is_some_and(|head| head.number >= expected_tip.number)
        {
            return Err(SessionStoreError::InvalidDelivery(
                "durable reorg replacement promise is at or behind acknowledged canonical progress",
            ));
        }
    }
    if let Some(pending) = persisted.pending_delivery.as_ref() {
        let cursor = pending
            .cursor
            .as_ref()
            .ok_or(SessionStoreError::InvalidDelivery(
                "durable pending delivery is missing its cursor",
            ))?;
        let predecessor = persisted
            .acknowledged_cursor
            .as_ref()
            .map_or(Ok(1), |cursor| {
                cursor
                    .batch_sequence
                    .checked_add(1)
                    .ok_or(SessionStoreError::SequenceOverflow)
            })?;
        if pending.session_id != session_id
            || pending.query_revision != revision
            || pending.sequence != predecessor
            || pending.sequence != cursor.batch_sequence
            || pending.delivery_token != pending.sequence.to_be_bytes()
            || pending.payload.is_none()
            || cursor.chain_id != chain_id
            || cursor.query_revision != revision
        {
            return Err(SessionStoreError::InvalidDelivery(
                "durable pending delivery violates its row invariants",
            ));
        }
        validate_delivery_size(pending, MAX_MESSAGE_SIZE_BYTES)?;
        validate_delivery_payload(
            persisted
                .desired_state
                .as_ref()
                .expect("validated desired state exists"),
            pending,
        )?;
        validate_delivery_progress(
            persisted.acknowledged_cursor.as_ref(),
            desired_state,
            pending,
        )?;
        if persisted
            .expected_reorg_tip
            .as_ref()
            .is_some_and(|expected| {
                !is_activation_delivery(pending, desired_state.new_revision)
                    && !delivery_certifies_reorg_tip(pending, expected)
            })
        {
            return Err(SessionStoreError::InvalidDelivery(
                "durable pending delivery does not certify the promised reorg replacement tip",
            ));
        }
        if persisted.expected_reorg_tip.is_some()
            && matches!(pending.payload, Some(delivery::Payload::Reorg(_)))
        {
            return Err(SessionStoreError::InvalidDelivery(
                "durable session contains a second reorg before its promised replacement",
            ));
        }
    }
    let acknowledged_sequence = persisted
        .acknowledged_cursor
        .as_ref()
        .map_or(0, |cursor| cursor.batch_sequence);
    let current_sequence = persisted
        .pending_delivery
        .as_ref()
        .map_or(acknowledged_sequence, |delivery| delivery.sequence);
    if activation_sequence > current_sequence {
        return Err(SessionStoreError::InvalidDelivery(
            "durable activation sequence is ahead of session sequence",
        ));
    }
    let pending_is_activation = persisted
        .pending_delivery
        .as_ref()
        .is_some_and(|delivery| is_activation_delivery(delivery, desired_state.new_revision));
    if activation_sequence == 0 {
        // Rows migrated from the pre-activation-sequence schema can only prove
        // authority through an exact pending activation or an acknowledged
        // cursor already on the current revision.
        if !pending_is_activation
            && persisted
                .acknowledged_cursor
                .as_ref()
                .is_none_or(|cursor| cursor.query_revision != revision)
        {
            return Err(SessionStoreError::InvalidDelivery(
                "legacy durable desired state has no acknowledged or pending activation",
            ));
        }
    } else if acknowledged_sequence < activation_sequence {
        if persisted.pending_delivery.as_ref().is_none_or(|delivery| {
            delivery.sequence != activation_sequence || !pending_is_activation
        }) {
            return Err(SessionStoreError::InvalidDelivery(
                "durable desired state is missing its pending activation barrier",
            ));
        }
    } else if persisted
        .acknowledged_cursor
        .as_ref()
        .is_none_or(|cursor| cursor.query_revision != revision)
    {
        return Err(SessionStoreError::InvalidDelivery(
            "durable desired state is not acknowledged at its current revision",
        ));
    }
    Ok(())
}

fn is_activation_delivery(delivery: &Delivery, revision: u64) -> bool {
    if delivery.cursor.is_none() {
        return false;
    }
    matches!(
        delivery.payload.as_ref(),
        Some(delivery::Payload::Barrier(barrier))
            if barrier.id == format!("desired-state:{revision}").as_bytes()
                && barrier.block.is_none()
    )
}

fn is_exact_activation(
    acknowledged_cursor: Option<&Cursor>,
    desired_state: &ApplyDesiredState,
    delivery: &Delivery,
) -> bool {
    let expected_head = acknowledged_cursor
        .and_then(|cursor| cursor.canonical_head.clone())
        .or_else(|| {
            desired_state
                .owners
                .iter()
                .find(|owner| owner.canonical)
                .and_then(|owner| owner.backfill.as_ref())
                .and_then(|backfill| backfill.retained_baseline.clone())
        });
    is_activation_delivery(delivery, desired_state.new_revision)
        && desired_state.expected_revision
            == acknowledged_cursor.map_or(0, |cursor| cursor.query_revision)
        && delivery.query_revision == desired_state.new_revision
        && delivery
            .cursor
            .as_ref()
            .is_some_and(|cursor| cursor.canonical_head == expected_head)
}

fn is_cursor_progress_barrier(delivery: &Delivery) -> bool {
    let Some(cursor) = delivery.cursor.as_ref() else {
        return false;
    };
    matches!(
        delivery.payload.as_ref(),
        Some(delivery::Payload::Barrier(barrier))
            if barrier.block.is_none()
                && barrier.id
                    == format!(
                        "source-progress:{}:{}",
                        delivery.query_revision, cursor.next_block
                    )
                    .as_bytes()
    )
}

fn delivery_certifies_reorg_tip(delivery: &Delivery, expected_tip: &BlockRef) -> bool {
    let Some(cursor) = delivery.cursor.as_ref() else {
        return false;
    };
    match delivery.payload.as_ref() {
        Some(delivery::Payload::Data(data)) => {
            // A provider can advance after the reorg control is acknowledged
            // but before a restarted service refetches its replacement page.
            // Bind the page to the promised tip as an explicit anchor, then
            // require every later explicit block through the terminal cursor
            // to be a contiguous descendant. Header plus final progress at one
            // height intentionally collapse to the same exact BlockRef here.
            let mut canonical_blocks = BTreeMap::<u64, &BlockRef>::new();
            for record in &data.records {
                let Ok(scope) = DeliveryScope::try_from(record.scope) else {
                    return false;
                };
                if scope == DeliveryScope::OwnerCatchup {
                    continue;
                }
                let Some(event) = record.event.as_ref().and_then(|event| event.event.as_ref())
                else {
                    return false;
                };
                let block = match event {
                    chain_event::Event::BlockHeader(header) => header.block.as_ref(),
                    chain_event::Event::BlockProgress(progress) => progress.block.as_ref(),
                    chain_event::Event::Log(_) => None,
                };
                if let Some(block) = block
                    && canonical_blocks
                        .insert(block.number, block)
                        .is_some_and(|known| known != block)
                {
                    return false;
                }
            }
            if canonical_blocks.get(&expected_tip.number).copied() != Some(expected_tip) {
                return false;
            }
            let mut suffix = canonical_blocks
                .range(expected_tip.number..)
                .map(|(_, block)| *block);
            let Some(mut previous) = suffix.next() else {
                return false;
            };
            for block in suffix {
                if previous.number.checked_add(1) != Some(block.number)
                    || block.parent_hash != previous.hash
                {
                    return false;
                }
                previous = block;
            }
            cursor.canonical_head.as_ref() == Some(previous)
                && previous
                    .number
                    .checked_add(1)
                    .is_some_and(|successor| cursor.next_block == successor)
        }
        Some(delivery::Payload::Barrier(barrier)) => {
            barrier.block.as_ref() == Some(expected_tip)
                && cursor.canonical_head.as_ref() == Some(expected_tip)
                && expected_tip
                    .number
                    .checked_add(1)
                    .is_some_and(|successor| cursor.next_block == successor)
        }
        _ => false,
    }
}

fn expected_reorg_tip_after_ack(
    current: Option<&BlockRef>,
    pending: &Delivery,
) -> Result<Option<BlockRef>, SessionStoreError> {
    if let Some(delivery::Payload::Reorg(reorg)) = pending.payload.as_ref() {
        if current.is_some() {
            return Err(SessionStoreError::InvalidDelivery(
                "a second reorg cannot replace an unfulfilled replacement promise",
            ));
        }
        return reorg
            .new_tip
            .clone()
            .map(Some)
            .ok_or(SessionStoreError::InvalidDelivery(
                "reorg is missing its new tip",
            ));
    }
    Ok(match current {
        Some(expected) if delivery_certifies_reorg_tip(pending, expected) => None,
        Some(expected) => Some(expected.clone()),
        None => None,
    })
}

fn canonical_head_regresses_or_changes(previous: &Cursor, current: &Cursor) -> bool {
    match (
        previous.canonical_head.as_ref(),
        current.canonical_head.as_ref(),
    ) {
        (Some(previous), Some(current)) => {
            current.number < previous.number
                || (current.number == previous.number && current != previous)
        }
        (Some(_), None) => true,
        _ => false,
    }
}

fn decode_optional<M: Message + Default>(
    encoded: Option<Vec<u8>>,
    _field: &'static str,
) -> Result<Option<M>, SessionStoreError> {
    encoded
        .map(|encoded| M::decode(encoded.as_slice()).map_err(SessionStoreError::from))
        .transpose()
}

// SQLite INTEGER is signed, while chain identifiers are a full-width `u64` on
// the public wire API. Reinterpreting the same 64 bits preserves every chain
// identifier and keeps the existing on-disk representation unchanged for the
// ordinary `0..=i64::MAX` domain. Ordering by this column is deliberately not
// part of the durable contract; it is an opaque component of the primary key.
fn chain_id_to_sql_integer(value: u64) -> i64 {
    i64::from_ne_bytes(value.to_ne_bytes())
}

fn chain_id_from_sql_integer(value: i64) -> u64 {
    u64::from_ne_bytes(value.to_ne_bytes())
}

// Activation sequences are opaque equality metadata in their INTEGER column;
// arithmetic is performed with checked `u64` operations before persistence.
// Preserve the complete protocol domain just as the protobuf outbox does.
fn sequence_to_sql_integer(value: u64) -> i64 {
    i64::from_ne_bytes(value.to_ne_bytes())
}

fn sequence_from_sql_integer(value: i64) -> u64 {
    u64::from_ne_bytes(value.to_ne_bytes())
}

fn to_sql_integer(value: u64, field: &'static str) -> Result<i64, SessionStoreError> {
    i64::try_from(value).map_err(|_| SessionStoreError::IntegerRange(field))
}

fn from_sql_integer(value: i64, field: &'static str) -> Result<u64, SessionStoreError> {
    u64::try_from(value).map_err(|_| SessionStoreError::IntegerRange(field))
}

/// Durable session transition failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SessionStoreError {
    /// SQLite operation failed.
    #[error(transparent)]
    Database(#[from] rusqlite::Error),
    /// A protobuf value in the database was corrupt.
    #[error(transparent)]
    Decode(#[from] prost::DecodeError),
    /// Durable schema is incompatible or structurally incomplete.
    #[error("session database schema error: {0}")]
    Schema(String),
    /// Desired-state validation failed.
    #[error(transparent)]
    DesiredState(#[from] DesiredStateError),
    /// Desired-state compare-and-swap used a stale revision.
    #[error("desired-state revision conflict: expected {expected}, committed {committed}")]
    RevisionConflict {
        /// Revision supplied as the compare-and-swap precondition.
        expected: u64,
        /// Revision currently committed by the durable session.
        committed: u64,
    },
    /// A u64 cannot be represented by SQLite's signed integer storage.
    #[error("{0} exceeds SQLite integer range")]
    IntegerRange(&'static str),
    /// The ordered delivery sequence has no representable successor.
    #[error("delivery sequence is exhausted")]
    SequenceOverflow,
    /// No durable session exists for this identity.
    #[error("unknown event session")]
    UnknownSession,
    /// A different delivery already occupies the one-item outbox.
    #[error("a different delivery is already pending")]
    PendingDelivery,
    /// Creating another durable session would exceed the configured database bound.
    #[error("persisted session limit {limit} is exhausted")]
    PersistedSessionLimit {
        /// Maximum durable session identities allowed in the database.
        limit: usize,
    },
    /// No delivery is awaiting acknowledgement.
    #[error("no delivery is awaiting acknowledgement")]
    NoPendingDelivery,
    /// The acknowledgement does not match the durable outbox item.
    #[error("delivery acknowledgement does not match the pending batch")]
    DeliveryTokenMismatch,
    /// A delivery violates a session invariant.
    #[error("invalid delivery: {0}")]
    InvalidDelivery(&'static str),
    /// A delivery cannot fit within the configured or hard transport limit.
    #[error(
        "encoded delivery is {encoded_bytes} bytes, exceeding the {max_delivery_bytes}-byte limit"
    )]
    DeliveryTooLarge {
        /// Actual protobuf-encoded delivery size.
        encoded_bytes: usize,
        /// Configured maximum encoded delivery size.
        max_delivery_bytes: usize,
    },
}

fn migrate_sessions_table(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), SessionStoreError> {
    let columns = {
        let mut statement = transaction.prepare("PRAGMA table_info(sessions)")?;
        let columns = statement.query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, i64>(5)?))
        })?;
        columns.collect::<Result<HashMap<_, _>, _>>()?
    };
    for required in ["session_id", "chain_id"] {
        if !columns.contains_key(required) {
            return Err(SessionStoreError::Schema(format!(
                "sessions table is missing required `{required}` column"
            )));
        }
    }
    if columns.get("session_id") != Some(&1)
        || columns.get("chain_id") != Some(&2)
        || columns.values().any(|position| *position > 2)
    {
        return Err(SessionStoreError::Schema(
            "sessions table must use PRIMARY KEY (session_id, chain_id)".into(),
        ));
    }
    let had_runtime_checkpoint_cursor = columns.contains_key("runtime_checkpoint_cursor");
    let additions = [
        ("revision", "INTEGER NOT NULL DEFAULT 0"),
        ("desired_state", "BLOB"),
        ("acknowledged_cursor", "BLOB"),
        ("runtime_checkpoint_cursor", "BLOB"),
        ("pending_delivery", "BLOB"),
        ("activation_sequence", "INTEGER NOT NULL DEFAULT 0"),
        ("expected_reorg_tip", "BLOB"),
    ];
    for (column, declaration) in additions {
        if !columns.contains_key(column) {
            transaction.execute_batch(&format!(
                "ALTER TABLE sessions ADD COLUMN {column} {declaration};"
            ))?;
        }
    }
    if columns.contains_key("pending_batch") && !columns.contains_key("pending_delivery") {
        transaction.execute(
            "UPDATE sessions SET pending_delivery = pending_batch WHERE pending_delivery IS NULL",
            [],
        )?;
    }
    if !had_runtime_checkpoint_cursor {
        // Every delivery acknowledged by pre-v2 services was runtime-visible;
        // only v2 can advance transport authority through checkpoint-neutral
        // activation controls.
        transaction.execute(
            "UPDATE sessions
             SET runtime_checkpoint_cursor = acknowledged_cursor
             WHERE runtime_checkpoint_cursor IS NULL",
            [],
        )?;
    }
    Ok(())
}
