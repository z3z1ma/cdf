use cdf_dest_sql::{
    LoadMirrorKey, LoadMirrorMutation, LoadMirrorRow, MirrorInsertOutcome, QuarantineMirrorKey,
    QuarantineMirrorMutation, QuarantineMirrorRow, SegmentMirrorMutation, SegmentMirrorRow,
    StateMirrorKey, StateMirrorMutation, StateMirrorRow, TransactionalMirrorBackend,
};
use cdf_kernel::{
    CheckpointId, IdempotencyToken, PackageHash, PipelineId, Receipt, ReceiptId, ResourceId,
    Result, SchemaHash, ScopeKey, SegmentId, SourcePosition, TargetName,
};
use cdf_package_contract::QuarantineObservedValue;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::error::classify_sqlite_error;
use crate::mapping::SqliteColumn;
use crate::receipts::QuarantineEvidence;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SqliteProvenanceEvidence {
    pub(crate) index_name: String,
    pub(crate) target: String,
    pub(crate) row_key_column: String,
    pub(crate) unique: bool,
    pub(crate) partial: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SqliteCommitEvidence {
    pub(crate) version: u16,
    pub(crate) target_schema: Option<Vec<SqliteColumn>>,
    pub(crate) provenance: Option<SqliteProvenanceEvidence>,
    pub(crate) segments: Vec<StoredSegment>,
    pub(crate) state: Option<StoredState>,
    pub(crate) quarantine: QuarantineEvidence,
}

impl SqliteCommitEvidence {
    pub(crate) fn sha256(&self) -> Result<String> {
        use sha2::{Digest, Sha256};
        let encoded = serde_json::to_vec(self).map_err(|error| {
            cdf_kernel::CdfError::internal(format!(
                "encode SQLite immutable commit evidence: {error}"
            ))
        })?;
        Ok(format!("{:x}", Sha256::digest(encoded)))
    }
}

pub(crate) fn create_system_tables(connection: &Connection) -> Result<()> {
    connection
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS _cdf_loads (
                target TEXT NOT NULL,
                package_hash TEXT NOT NULL,
                idempotency_token TEXT NOT NULL,
                resource_id TEXT,
                receipt_json TEXT NOT NULL,
                PRIMARY KEY (target, package_hash, idempotency_token)
            ) STRICT;
            CREATE TABLE IF NOT EXISTS _cdf_state (
                pipeline_id TEXT NOT NULL,
                resource_id TEXT NOT NULL,
                scope_json TEXT NOT NULL,
                state_json TEXT NOT NULL,
                PRIMARY KEY (pipeline_id, resource_id, scope_json)
            ) STRICT;
            CREATE TABLE IF NOT EXISTS _cdf_state_history (
                receipt_id TEXT NOT NULL PRIMARY KEY,
                state_json TEXT NOT NULL
            ) STRICT;
            CREATE TABLE IF NOT EXISTS _cdf_segments (
                target TEXT NOT NULL,
                package_hash TEXT NOT NULL,
                idempotency_token TEXT NOT NULL,
                segment_id TEXT NOT NULL,
                row_key_start INTEGER,
                row_key_end INTEGER,
                segment_json TEXT NOT NULL,
                PRIMARY KEY (target, package_hash, idempotency_token, segment_id),
                CHECK ((row_key_start IS NULL) = (row_key_end IS NULL)),
                CHECK (row_key_start IS NULL OR row_key_start <= row_key_end)
            ) STRICT;
            CREATE UNIQUE INDEX IF NOT EXISTS _cdf_segments_row_range_start
                ON _cdf_segments(target, row_key_start)
                WHERE row_key_start IS NOT NULL;
            CREATE TABLE IF NOT EXISTS _cdf_quarantine (
                target TEXT NOT NULL,
                package_hash TEXT NOT NULL,
                source_row_ordinal INTEGER NOT NULL,
                rule_id TEXT NOT NULL,
                error_code TEXT NOT NULL,
                quarantine_json TEXT NOT NULL,
                PRIMARY KEY (target, package_hash, source_row_ordinal, rule_id, error_code)
            ) STRICT;
            CREATE TABLE IF NOT EXISTS _cdf_commit_evidence (
                receipt_id TEXT NOT NULL PRIMARY KEY,
                evidence_json TEXT NOT NULL
            ) STRICT;
            CREATE TABLE IF NOT EXISTS _cdf_row_key_allocator (
                singleton INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
                next_row_key INTEGER NOT NULL CHECK (next_row_key >= 0)
            ) STRICT;
            INSERT OR IGNORE INTO _cdf_row_key_allocator(singleton, next_row_key) VALUES (1, 0);",
        )
        .map_err(|error| {
            classify_sqlite_error("create SQLite destination system mirrors", error)
        })?;
    Ok(())
}

pub(crate) struct SqliteMirrorBackend<'a> {
    connection: &'a Connection,
    cancellation: cdf_runtime::RunCancellation,
}

impl<'a> SqliteMirrorBackend<'a> {
    pub(crate) fn new(
        connection: &'a Connection,
        cancellation: cdf_runtime::RunCancellation,
    ) -> Self {
        Self {
            connection,
            cancellation,
        }
    }
}

pub(crate) fn insert_commit_evidence(
    connection: &Connection,
    receipt_id: &ReceiptId,
    evidence: &SqliteCommitEvidence,
) -> Result<()> {
    let evidence_json = encode_json("encode SQLite immutable commit evidence", evidence)?;
    let inserted = connection
        .execute(
            "INSERT OR IGNORE INTO _cdf_commit_evidence(receipt_id, evidence_json) VALUES (?1, ?2)",
            params![receipt_id.as_str(), evidence_json],
        )
        .map_err(|error| classify_sqlite_error("insert SQLite immutable commit evidence", error))?;
    if inserted == 0 {
        let stored = read_commit_evidence(connection, receipt_id)?;
        if stored.as_ref() != Some(evidence) {
            return Err(cdf_kernel::CdfError::destination(
                "SQLite immutable commit evidence conflicts with an existing receipt",
            ));
        }
    }
    Ok(())
}

pub(crate) fn read_commit_evidence(
    connection: &Connection,
    receipt_id: &ReceiptId,
) -> Result<Option<SqliteCommitEvidence>> {
    connection
        .query_row(
            "SELECT evidence_json FROM _cdf_commit_evidence WHERE receipt_id = ?1",
            [receipt_id.as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|error| classify_sqlite_error("read SQLite immutable commit evidence", error))?
        .map(|json| decode_json("decode SQLite immutable commit evidence", &json))
        .transpose()
}

impl TransactionalMirrorBackend for SqliteMirrorBackend<'_> {
    fn read_load(&mut self, key: &LoadMirrorKey) -> Result<Option<LoadMirrorRow>> {
        self.cancellation.check()?;
        self.connection
            .query_row(
                "SELECT receipt_json FROM _cdf_loads
                 WHERE target = ?1 AND package_hash = ?2 AND idempotency_token = ?3",
                params![
                    key.target.as_str(),
                    key.package_hash.as_str(),
                    key.idempotency_token.as_str()
                ],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| classify_sqlite_error("read SQLite load mirror", error))?
            .map(|json| decode_json::<Receipt>("decode SQLite load mirror", &json))
            .transpose()
            .map(|receipt| receipt.map(|receipt| LoadMirrorRow { receipt }))
    }

    fn insert_load(
        &mut self,
        mutation: &LoadMirrorMutation,
    ) -> Result<MirrorInsertOutcome<LoadMirrorRow>> {
        self.cancellation.check()?;
        let receipt_json = encode_json("encode SQLite load mirror", &mutation.receipt)?;
        let inserted = self
            .connection
            .execute(
                "INSERT OR IGNORE INTO _cdf_loads(
                    target, package_hash, idempotency_token, resource_id, receipt_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    mutation.receipt.target.as_str(),
                    mutation.receipt.package_hash.as_str(),
                    mutation.receipt.idempotency_token.as_str(),
                    mutation.resource_id.as_ref().map(ResourceId::as_str),
                    receipt_json,
                ],
            )
            .map_err(|error| classify_sqlite_error("insert SQLite load mirror", error))?;
        if inserted == 0 {
            return Ok(MirrorInsertOutcome::Conflict);
        }
        let stored = self.read_load(&mutation.key())?.ok_or_else(|| {
            cdf_kernel::CdfError::destination("SQLite load mirror disappeared after insert")
        })?;
        Ok(MirrorInsertOutcome::Inserted(stored))
    }

    fn read_state(&mut self, key: &StateMirrorKey) -> Result<Option<StateMirrorRow>> {
        self.cancellation.check()?;
        let scope_json = encode_json("encode SQLite state mirror scope", &key.scope)?;
        self.connection
            .query_row(
                "SELECT state_json FROM _cdf_state
                 WHERE pipeline_id = ?1 AND resource_id = ?2 AND scope_json = ?3",
                params![
                    key.pipeline_id.as_str(),
                    key.resource_id.as_str(),
                    scope_json
                ],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| classify_sqlite_error("read SQLite state mirror", error))?
            .map(|json| decode_json::<StoredState>("decode SQLite state mirror", &json))
            .transpose()
            .map(|stored| stored.map(StoredState::into_row))
    }

    fn upsert_state(
        &mut self,
        mutation: &StateMirrorMutation,
    ) -> Result<MirrorInsertOutcome<StateMirrorRow>> {
        self.cancellation.check()?;
        let scope_json = encode_json("encode SQLite state mirror scope", &mutation.key.scope)?;
        let stored_state = StoredState::from(mutation);
        let state_json = encode_json("encode SQLite state mirror", &stored_state)?;
        let history_inserted = self
            .connection
            .execute(
                "INSERT OR IGNORE INTO _cdf_state_history(receipt_id, state_json) VALUES (?1, ?2)",
                params![mutation.receipt_id.as_str(), &state_json],
            )
            .map_err(|error| classify_sqlite_error("insert SQLite state history", error))?;
        if history_inserted == 0 {
            let existing = read_state_history(self.connection, &mutation.receipt_id)?;
            if existing.as_ref() != Some(&stored_state) {
                return Err(cdf_kernel::CdfError::destination(
                    "SQLite state history conflicts with an existing receipt",
                ));
            }
        }
        self.connection
            .execute(
                "INSERT INTO _cdf_state(pipeline_id, resource_id, scope_json, state_json)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(pipeline_id, resource_id, scope_json)
                 DO UPDATE SET state_json = excluded.state_json",
                params![
                    mutation.key.pipeline_id.as_str(),
                    mutation.key.resource_id.as_str(),
                    scope_json,
                    state_json,
                ],
            )
            .map_err(|error| classify_sqlite_error("upsert SQLite state mirror", error))?;
        let stored = self.read_state(&mutation.key)?.ok_or_else(|| {
            cdf_kernel::CdfError::destination("SQLite state mirror disappeared after upsert")
        })?;
        Ok(MirrorInsertOutcome::Inserted(stored))
    }

    fn insert_segment(
        &mut self,
        mutation: &SegmentMirrorMutation,
    ) -> Result<MirrorInsertOutcome<SegmentMirrorRow>> {
        self.cancellation.check()?;
        let stored = StoredSegment::from(mutation);
        let segment_json = encode_json("encode SQLite segment mirror", &stored)?;
        let (row_key_start, row_key_end) = stored
            .row_range
            .as_ref()
            .map(|range| (Some(range.row_key_start), Some(range.row_key_end)))
            .unwrap_or((None, None));
        let inserted = self
            .connection
            .execute(
                "INSERT OR IGNORE INTO _cdf_segments(
                    target, package_hash, idempotency_token, segment_id,
                    row_key_start, row_key_end, segment_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    mutation.target.as_str(),
                    mutation.package_hash.as_str(),
                    mutation.idempotency_token.as_str(),
                    mutation.segment_id.as_str(),
                    to_sqlite_i64(row_key_start, "SQLite segment row-key start")?,
                    to_sqlite_i64(row_key_end, "SQLite segment row-key end")?,
                    segment_json,
                ],
            )
            .map_err(|error| classify_sqlite_error("insert SQLite segment mirror", error))?;
        if inserted == 0 {
            return Ok(MirrorInsertOutcome::Conflict);
        }
        let stored = self.read_mirror_segment(mutation)?.ok_or_else(|| {
            cdf_kernel::CdfError::destination("SQLite segment mirror disappeared after insert")
        })?;
        Ok(MirrorInsertOutcome::Inserted(stored))
    }

    fn read_mirror_segment(
        &mut self,
        mutation: &SegmentMirrorMutation,
    ) -> Result<Option<SegmentMirrorRow>> {
        self.cancellation.check()?;
        self.connection
            .query_row(
                "SELECT segment_json FROM _cdf_segments
                 WHERE target = ?1 AND package_hash = ?2
                   AND idempotency_token = ?3 AND segment_id = ?4",
                params![
                    mutation.target.as_str(),
                    mutation.package_hash.as_str(),
                    mutation.idempotency_token.as_str(),
                    mutation.segment_id.as_str(),
                ],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| classify_sqlite_error("read SQLite segment mirror", error))?
            .map(|json| decode_json::<StoredSegment>("decode SQLite segment mirror", &json))
            .transpose()
            .map(|stored| stored.map(StoredSegment::into_row))
    }

    fn insert_quarantine(
        &mut self,
        mutation: &QuarantineMirrorMutation,
    ) -> Result<MirrorInsertOutcome<QuarantineMirrorRow>> {
        self.cancellation.check()?;
        let quarantine_json = encode_json(
            "encode SQLite quarantine mirror",
            &StoredQuarantine::from(mutation),
        )?;
        let ordinal = to_sqlite_i64(
            Some(mutation.key.source_row_ordinal),
            "SQLite quarantine row ordinal",
        )?;
        let inserted = self
            .connection
            .execute(
                "INSERT OR IGNORE INTO _cdf_quarantine(
                    target, package_hash, source_row_ordinal, rule_id, error_code, quarantine_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    mutation.key.target.as_str(),
                    mutation.key.package_hash.as_str(),
                    ordinal,
                    mutation.key.rule_id,
                    mutation.key.error_code,
                    quarantine_json,
                ],
            )
            .map_err(|error| classify_sqlite_error("insert SQLite quarantine mirror", error))?;
        if inserted == 0 {
            return Ok(MirrorInsertOutcome::Conflict);
        }
        let stored = self.read_quarantine(&mutation.key)?.ok_or_else(|| {
            cdf_kernel::CdfError::destination("SQLite quarantine mirror disappeared after insert")
        })?;
        Ok(MirrorInsertOutcome::Inserted(stored))
    }

    fn read_quarantine(
        &mut self,
        key: &QuarantineMirrorKey,
    ) -> Result<Option<QuarantineMirrorRow>> {
        self.cancellation.check()?;
        let ordinal = to_sqlite_i64(
            Some(key.source_row_ordinal),
            "SQLite quarantine row ordinal",
        )?;
        self.connection
            .query_row(
                "SELECT quarantine_json FROM _cdf_quarantine
                 WHERE target = ?1 AND package_hash = ?2 AND source_row_ordinal = ?3
                   AND rule_id = ?4 AND error_code = ?5",
                params![
                    key.target.as_str(),
                    key.package_hash.as_str(),
                    ordinal,
                    key.rule_id,
                    key.error_code,
                ],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| classify_sqlite_error("read SQLite quarantine mirror", error))?
            .map(|json| decode_json::<StoredQuarantine>("decode SQLite quarantine mirror", &json))
            .transpose()
            .map(|stored| stored.map(StoredQuarantine::into_row))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredState {
    pub(crate) pipeline_id: PipelineId,
    pub(crate) resource_id: ResourceId,
    pub(crate) scope: ScopeKey,
    pub(crate) state_version: u16,
    pub(crate) checkpoint_id: CheckpointId,
    pub(crate) parent_checkpoint_id: Option<CheckpointId>,
    pub(crate) package_hash: PackageHash,
    pub(crate) schema_hash: SchemaHash,
    pub(crate) output_position: SourcePosition,
    pub(crate) receipt_id: ReceiptId,
    pub(crate) committed_at_ms: i64,
}

impl From<&StateMirrorMutation> for StoredState {
    fn from(mutation: &StateMirrorMutation) -> Self {
        Self {
            pipeline_id: mutation.key.pipeline_id.clone(),
            resource_id: mutation.key.resource_id.clone(),
            scope: mutation.key.scope.clone(),
            state_version: mutation.state_version,
            checkpoint_id: mutation.checkpoint_id.clone(),
            parent_checkpoint_id: mutation.parent_checkpoint_id.clone(),
            package_hash: mutation.package_hash.clone(),
            schema_hash: mutation.schema_hash.clone(),
            output_position: mutation.output_position.clone(),
            receipt_id: mutation.receipt_id.clone(),
            committed_at_ms: mutation.committed_at_ms,
        }
    }
}

impl StoredState {
    fn into_row(self) -> StateMirrorRow {
        StateMirrorRow {
            mutation: StateMirrorMutation {
                key: StateMirrorKey {
                    pipeline_id: self.pipeline_id,
                    resource_id: self.resource_id,
                    scope: self.scope,
                },
                state_version: self.state_version,
                checkpoint_id: self.checkpoint_id,
                parent_checkpoint_id: self.parent_checkpoint_id,
                package_hash: self.package_hash,
                schema_hash: self.schema_hash,
                output_position: self.output_position,
                receipt_id: self.receipt_id,
                committed_at_ms: self.committed_at_ms,
            },
        }
    }
}

pub(crate) fn read_state_history(
    connection: &Connection,
    receipt_id: &ReceiptId,
) -> Result<Option<StoredState>> {
    connection
        .query_row(
            "SELECT state_json FROM _cdf_state_history WHERE receipt_id = ?1",
            [receipt_id.as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|error| classify_sqlite_error("read SQLite state history", error))?
        .map(|json| decode_json("decode SQLite state history", &json))
        .transpose()
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredRowRange {
    pub(crate) segment_id: SegmentId,
    pub(crate) row_key_start: u64,
    pub(crate) row_key_end: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredSegment {
    pub(crate) target: TargetName,
    pub(crate) package_hash: PackageHash,
    pub(crate) idempotency_token: IdempotencyToken,
    pub(crate) segment_id: SegmentId,
    pub(crate) scope: Option<ScopeKey>,
    pub(crate) output_position: Option<SourcePosition>,
    pub(crate) row_count: u64,
    pub(crate) byte_count: u64,
    pub(crate) committed_at_ms: i64,
    pub(crate) row_range: Option<StoredRowRange>,
}

impl From<&SegmentMirrorMutation> for StoredSegment {
    fn from(mutation: &SegmentMirrorMutation) -> Self {
        Self {
            target: mutation.target.clone(),
            package_hash: mutation.package_hash.clone(),
            idempotency_token: mutation.idempotency_token.clone(),
            segment_id: mutation.segment_id.clone(),
            scope: mutation.scope.clone(),
            output_position: mutation.output_position.clone(),
            row_count: mutation.row_count,
            byte_count: mutation.byte_count,
            committed_at_ms: mutation.committed_at_ms,
            row_range: mutation.row_range.as_ref().map(|range| StoredRowRange {
                segment_id: range.segment_id.clone(),
                row_key_start: range.row_key_start,
                row_key_end: range.row_key_end,
            }),
        }
    }
}

impl StoredSegment {
    fn into_row(self) -> SegmentMirrorRow {
        SegmentMirrorRow {
            mutation: SegmentMirrorMutation {
                target: self.target,
                package_hash: self.package_hash,
                idempotency_token: self.idempotency_token,
                segment_id: self.segment_id,
                scope: self.scope,
                output_position: self.output_position,
                row_count: self.row_count,
                byte_count: self.byte_count,
                committed_at_ms: self.committed_at_ms,
                row_range: self.row_range.map(|range| cdf_dest_sql::SegmentRowRange {
                    segment_id: range.segment_id,
                    row_key_start: range.row_key_start,
                    row_key_end: range.row_key_end,
                }),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredQuarantine {
    target: TargetName,
    package_hash: PackageHash,
    source_row_ordinal: u64,
    rule_id: String,
    error_code: String,
    receipt_id: ReceiptId,
    source_position: Option<SourcePosition>,
    observed_value_redacted: QuarantineObservedValue,
    committed_at_ms: i64,
}

impl From<&QuarantineMirrorMutation> for StoredQuarantine {
    fn from(mutation: &QuarantineMirrorMutation) -> Self {
        Self {
            target: mutation.key.target.clone(),
            package_hash: mutation.key.package_hash.clone(),
            source_row_ordinal: mutation.key.source_row_ordinal,
            rule_id: mutation.key.rule_id.clone(),
            error_code: mutation.key.error_code.clone(),
            receipt_id: mutation.receipt_id.clone(),
            source_position: mutation.source_position.clone(),
            observed_value_redacted: mutation.observed_value_redacted.clone(),
            committed_at_ms: mutation.committed_at_ms,
        }
    }
}

impl StoredQuarantine {
    fn into_row(self) -> QuarantineMirrorRow {
        QuarantineMirrorRow {
            mutation: QuarantineMirrorMutation {
                key: QuarantineMirrorKey {
                    target: self.target,
                    package_hash: self.package_hash,
                    source_row_ordinal: self.source_row_ordinal,
                    rule_id: self.rule_id,
                    error_code: self.error_code,
                },
                receipt_id: self.receipt_id,
                source_position: self.source_position,
                observed_value_redacted: self.observed_value_redacted,
                committed_at_ms: self.committed_at_ms,
            },
        }
    }

    fn into_record(self) -> cdf_package_contract::QuarantineRecord {
        cdf_package_contract::QuarantineRecord {
            source_row_ordinal: self.source_row_ordinal,
            rule_id: self.rule_id,
            error_code: self.error_code,
            source_position: self.source_position,
            observed_value_redacted: self.observed_value_redacted,
        }
    }
}

pub(crate) fn stored_quarantine_evidence(
    connection: &Connection,
    target: &TargetName,
    package_hash: &PackageHash,
) -> Result<QuarantineEvidence> {
    let mut statement = connection
        .prepare(
            "SELECT quarantine_json FROM _cdf_quarantine
             WHERE target = ?1 AND package_hash = ?2",
        )
        .map_err(|error| classify_sqlite_error("prepare SQLite quarantine verification", error))?;
    let rows = statement
        .query_map(params![target.as_str(), package_hash.as_str()], |row| {
            row.get::<_, String>(0)
        })
        .map_err(|error| classify_sqlite_error("query SQLite quarantine verification", error))?;
    let mut evidence = QuarantineEvidence::default();
    for row in rows {
        let json =
            row.map_err(|error| classify_sqlite_error("decode SQLite quarantine row", error))?;
        let stored = decode_json::<StoredQuarantine>(
            "decode SQLite quarantine verification evidence",
            &json,
        )?;
        evidence.observe(&stored.into_record())?;
    }
    evidence.canonicalize();
    Ok(evidence)
}

fn encode_json<T: Serialize>(action: &str, value: &T) -> Result<String> {
    serde_json::to_string(value)
        .map_err(|error| cdf_kernel::CdfError::internal(format!("{action}: {error}")))
}

fn decode_json<T: DeserializeOwned>(action: &str, value: &str) -> Result<T> {
    serde_json::from_str(value)
        .map_err(|error| cdf_kernel::CdfError::destination(format!("{action}: {error}")))
}

fn to_sqlite_i64(value: Option<u64>, label: &str) -> Result<Option<i64>> {
    value
        .map(|value| {
            i64::try_from(value).map_err(|_| {
                cdf_kernel::CdfError::data(format!("{label} exceeds SQLite INTEGER range"))
            })
        })
        .transpose()
}
