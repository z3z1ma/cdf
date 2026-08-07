use std::collections::{BTreeMap, BTreeSet};

use cdf_dest_sql::{
    MirrorCommit, SegmentMirrorPolicy, SegmentRowRange, TransactionalMirrorManager,
};
use cdf_kernel::{
    CdfError, CommitCounts, CommitSession, Receipt, Result, SegmentAck, SegmentId, WriteDisposition,
};
use rusqlite::{Connection, OpenFlags};

use crate::{
    error::{classify_sqlite_error, classify_sqlite_open_error},
    mirrors::{
        SqliteCommitEvidence, SqliteMirrorBackend, SqliteProvenanceEvidence, StoredRowRange,
        StoredSegment, StoredState, create_system_tables, insert_commit_evidence,
        stored_quarantine_evidence,
    },
    models::{SqliteCommitRequest, SqliteExpectedSegment, SqliteLoadPlan, SqliteReceiptInput},
    receipts::{
        build_receipt, expected_counts, package_quarantine_evidence, receipt_id,
        transaction_metadata,
    },
};

#[cfg(test)]
use super::writer::{TEST_EXIT_DURING_MIRRORS_CODE, exit_before_commit_for_test};
use super::{
    verifier::verify_receipt_on_connection,
    writer::{
        ROW_KEY_COLUMN, allocate_row_keys, count_target_rows, find_duplicate_receipt,
        finish_payload, install_progress_handler, package_row_count, prepare_stage, prepare_target,
        require_complete_segments, row_key_index_name, target_exists, validate_package_segment,
        write_segment,
    },
};

#[cfg(test)]
pub(crate) const TEST_EXIT_AFTER_COMMIT_ENV: &str = "CDF_SQLITE_TEST_EXIT_AFTER_COMMIT";
#[cfg(test)]
pub(crate) const TEST_EXIT_AFTER_COMMIT_CODE: i32 = 86;

pub(crate) struct SqliteCommitSession {
    database_path: std::path::PathBuf,
    execution: cdf_runtime::ExecutionServices,
    package: cdf_package_contract::SharedVerifiedPackageAccess,
    plan: SqliteLoadPlan,
    expected_segments: BTreeMap<SegmentId, SqliteExpectedSegment>,
    accepted_segments: BTreeSet<SegmentId>,
    connection: Option<Connection>,
    phase: SessionPhase,
    journal_mode: Option<String>,
    synchronous: Option<i64>,
    duplicate_receipt: Option<Receipt>,
    receipt: Option<Receipt>,
    first_row_key: Option<u64>,
    target_prepared: bool,
    cancellation: cdf_runtime::RunCancellation,
}

pub(crate) struct ManagedSqliteCommitSession {
    inner: Option<SqliteCommitSession>,
    execution: cdf_runtime::ExecutionServices,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionPhase {
    Begun,
    MigrationsApplied,
    Written,
}

impl SqliteCommitSession {
    pub(crate) fn new(
        database_path: std::path::PathBuf,
        execution: cdf_runtime::ExecutionServices,
        request: SqliteCommitRequest,
    ) -> Self {
        let cancellation = execution.run_cancellation();
        Self {
            database_path,
            execution,
            package: request.package,
            plan: request.plan,
            expected_segments: request.segments.expected,
            accepted_segments: BTreeSet::new(),
            connection: None,
            phase: SessionPhase::Begun,
            journal_mode: None,
            synchronous: None,
            duplicate_receipt: None,
            receipt: None,
            first_row_key: None,
            target_prepared: false,
            cancellation,
        }
    }

    fn write_complete_receipt(
        &mut self,
        counts: CommitCounts,
        row_ranges: Vec<SegmentRowRange>,
    ) -> Result<()> {
        self.cancellation.check()?;
        if let Some(receipt) = self.duplicate_receipt.clone() {
            self.receipt = Some(receipt);
            self.phase = SessionPhase::Written;
            return Ok(());
        }
        let committed_at_ms = now_ms(&self.execution)?;
        let journal_mode = self
            .journal_mode
            .clone()
            .ok_or_else(|| CdfError::internal("SQLite session has no journal-mode evidence"))?;
        let synchronous = self
            .synchronous
            .ok_or_else(|| CdfError::internal("SQLite session has no synchronous evidence"))?;
        let quarantine = package_quarantine_evidence(self.package.as_ref())?;
        let evidence = self.commit_evidence(committed_at_ms, row_ranges.clone(), quarantine)?;
        let receipt = build_receipt(
            &self.plan,
            SqliteReceiptInput {
                committed_at_ms,
                counts,
                transaction: transaction_metadata(journal_mode, synchronous, false, &evidence)?,
            },
        )?;
        let resource_id = self.plan.resource_id.clone().or_else(|| {
            self.plan
                .state_delta
                .as_ref()
                .map(|delta| delta.resource_id.clone())
        });
        let commit = MirrorCommit::new(
            receipt.clone(),
            resource_id,
            self.plan.state_delta.as_ref(),
            &self.plan.segments,
            row_ranges,
            SegmentMirrorPolicy::Persist {
                require_row_ranges: !receipt.segment_acks.is_empty(),
            },
        )?;
        let connection = self
            .connection
            .as_ref()
            .ok_or_else(|| CdfError::internal("SQLite session has no open payload transaction"))?;
        let mut backend = SqliteMirrorBackend::new(connection, self.cancellation.clone());
        TransactionalMirrorManager::new(&mut backend)
            .apply_with_quarantines(commit, |visitor| {
                self.package.for_each_quarantine_record(visitor)
            })?;
        #[cfg(test)]
        exit_before_commit_for_test("mirrors", TEST_EXIT_DURING_MIRRORS_CODE);
        insert_commit_evidence(connection, &receipt.receipt_id, &evidence)?;
        if stored_quarantine_evidence(connection, &receipt.target, &receipt.package_hash)?
            != evidence.quarantine
        {
            return Err(CdfError::destination(
                "SQLite quarantine mirror evidence differs from the finalized package",
            ));
        }
        verify_receipt_on_connection(connection, &receipt, &self.cancellation)?;
        self.cancellation.check()?;
        self.receipt = Some(receipt);
        self.phase = SessionPhase::Written;
        Ok(())
    }

    fn commit_evidence(
        &self,
        committed_at_ms: i64,
        row_ranges: Vec<SegmentRowRange>,
        quarantine: crate::receipts::QuarantineEvidence,
    ) -> Result<SqliteCommitEvidence> {
        let receipt_id = receipt_id(&self.plan)?;
        let ranges = row_ranges
            .into_iter()
            .map(|range| (range.segment_id.clone(), range))
            .collect::<BTreeMap<_, _>>();
        let segments = self
            .plan
            .segments
            .iter()
            .map(|segment| StoredSegment {
                target: self.plan.kernel.target.clone(),
                package_hash: self.plan.package_hash.clone(),
                idempotency_token: self.plan.idempotency_token.clone(),
                segment_id: segment.segment_id.clone(),
                scope: Some(segment.scope.clone()),
                output_position: Some(segment.output_position.clone()),
                row_count: segment.row_count,
                byte_count: segment.byte_count,
                committed_at_ms,
                row_range: ranges.get(&segment.segment_id).map(|range| StoredRowRange {
                    segment_id: range.segment_id.clone(),
                    row_key_start: range.row_key_start,
                    row_key_end: range.row_key_end,
                }),
            })
            .collect();
        let state = self.plan.state_delta.as_ref().map(|delta| StoredState {
            pipeline_id: delta.pipeline_id.clone(),
            resource_id: delta.resource_id.clone(),
            scope: delta.scope.clone(),
            state_version: delta.state_version,
            checkpoint_id: delta.checkpoint_id.clone(),
            parent_checkpoint_id: delta.parent_checkpoint_id.clone(),
            package_hash: delta.package_hash.clone(),
            schema_hash: delta.schema_hash.clone(),
            output_position: delta.output_position.clone(),
            receipt_id,
            committed_at_ms,
        });
        Ok(SqliteCommitEvidence {
            version: 1,
            target_schema: self.target_prepared.then(|| self.plan.columns.clone()),
            provenance: self.target_prepared.then(|| SqliteProvenanceEvidence {
                index_name: row_key_index_name(self.plan.target.as_str()),
                target: self.plan.target.as_str().to_owned(),
                row_key_column: ROW_KEY_COLUMN.to_owned(),
                unique: true,
                partial: true,
            }),
            segments,
            state,
            quarantine,
        })
    }

    fn finalize_receipt(mut self) -> Result<Receipt> {
        self.cancellation.check()?;
        if self.phase != SessionPhase::Written {
            return Err(CdfError::destination(format!(
                "cannot finalize SQLite commit before every segment is written: accepted {} of {}",
                self.accepted_segments.len(),
                self.expected_segments.len()
            )));
        }
        let receipt = self
            .receipt
            .take()
            .ok_or_else(|| CdfError::internal("SQLite commit session has no receipt"))?;
        let connection = self
            .connection
            .take()
            .ok_or_else(|| CdfError::internal("SQLite commit session has no transaction"))?;
        connection.execute_batch("COMMIT").map_err(|error| {
            classify_sqlite_error("commit SQLite destination transaction", error)
        })?;
        #[cfg(test)]
        exit_after_commit_for_test();
        Ok(receipt)
    }

    fn rollback_open_transaction(&mut self) -> Result<()> {
        let Some(connection) = self.connection.take() else {
            return Ok(());
        };
        connection
            .execute_batch("ROLLBACK")
            .map_err(|error| classify_sqlite_error("abort SQLite destination transaction", error))
    }
}

impl ManagedSqliteCommitSession {
    pub(crate) fn new(
        inner: SqliteCommitSession,
        execution: cdf_runtime::ExecutionServices,
    ) -> Self {
        Self {
            inner: Some(inner),
            execution,
        }
    }

    fn with_inner<T, F>(&mut self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut SqliteCommitSession) -> Result<T> + Send + 'static,
    {
        let mut inner = self
            .inner
            .take()
            .ok_or_else(|| CdfError::internal("managed SQLite session lost its inner state"))?;
        let (inner, result) =
            self.execution
                .run_blocking("sqlite.destination.sync", move || {
                    let result = operation(&mut inner);
                    Ok((inner, result))
                })?;
        self.inner = Some(inner);
        result
    }
}

impl CommitSession for SqliteCommitSession {
    fn apply_migrations(&mut self) -> Result<()> {
        self.cancellation.check()?;
        if self.phase != SessionPhase::Begun {
            return Err(CdfError::destination(
                "SQLite migrations have already been applied",
            ));
        }
        let connection = Connection::open_with_flags(
            &self.database_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|error| {
            classify_sqlite_open_error("open SQLite destination", &self.database_path, error)
        })?;
        install_progress_handler(&connection, &self.cancellation)?;
        connection
            .busy_timeout(std::time::Duration::ZERO)
            .map_err(|error| classify_sqlite_error("disable SQLite busy waiting", error))?;
        let journal_mode = connection
            .query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))
            .map_err(|error| classify_sqlite_error("read SQLite journal_mode", error))?;
        let synchronous = connection
            .query_row("PRAGMA synchronous", [], |row| row.get::<_, i64>(0))
            .map_err(|error| classify_sqlite_error("read SQLite synchronous", error))?;
        connection
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|error| {
                classify_sqlite_error("begin SQLite destination transaction", error)
            })?;
        create_system_tables(&connection)?;
        let duplicate = find_duplicate_receipt(&connection, &self.plan, &self.cancellation)?;
        if duplicate.is_none() && !self.expected_segments.is_empty() {
            prepare_target(&connection, &self.plan)?;
            self.target_prepared = true;
            if self.plan.kernel.disposition == WriteDisposition::Merge {
                prepare_stage(&connection, &self.plan)?;
            }
            let rows = package_row_count(&self.expected_segments)?;
            self.first_row_key = Some(allocate_row_keys(&connection, rows)?);
        }
        let zero_counts = if self.expected_segments.is_empty() {
            Some(
                if duplicate.is_none()
                    && self.plan.kernel.disposition == WriteDisposition::Replace
                    && target_exists(&connection, self.plan.target.as_str())?
                {
                    prepare_target(&connection, &self.plan)?;
                    self.target_prepared = true;
                    let deleted_rows = count_target_rows(&connection, &self.plan)?;
                    connection
                        .execute(&format!("DELETE FROM {}", self.plan.target.quoted()), [])
                        .map_err(|error| classify_sqlite_error("replace SQLite target", error))?;
                    CommitCounts::rows(0, Some(0), Some(0), Some(deleted_rows))
                } else {
                    match self.plan.kernel.disposition {
                        WriteDisposition::Merge => CommitCounts::keyed_changes(
                            cdf_kernel::KeyedEffectCounts::default(),
                            Some(0),
                            Some(0),
                            None,
                            None,
                            None,
                            None,
                        ),
                        _ => CommitCounts::default(),
                    }
                },
            )
        } else {
            None
        };
        self.connection = Some(connection);
        self.journal_mode = Some(journal_mode);
        self.synchronous = Some(synchronous);
        self.duplicate_receipt = duplicate;
        self.phase = SessionPhase::MigrationsApplied;
        if let Some(counts) = zero_counts {
            self.write_complete_receipt(counts, Vec::new())?;
        }
        Ok(())
    }

    fn write_segments(
        &mut self,
        segments: cdf_kernel::CommitSegmentIterator,
    ) -> Result<Vec<SegmentAck>> {
        self.cancellation.check()?;
        if self.phase == SessionPhase::Written {
            return Err(CdfError::destination(
                "SQLite commit session has already accepted all segments",
            ));
        }
        if self.phase != SessionPhase::MigrationsApplied {
            return Err(CdfError::destination(
                "SQLite commit session must apply migrations before writing",
            ));
        }
        if !self.accepted_segments.is_empty() {
            return Err(CdfError::destination(
                "SQLite finalized package segments have already been submitted",
            ));
        }
        let connection = self
            .connection
            .as_ref()
            .ok_or_else(|| CdfError::internal("SQLite commit session has no open transaction"))?;
        let first_row_key = self.first_row_key.unwrap_or(0);
        let mut acknowledgements = Vec::with_capacity(self.expected_segments.len());
        let mut row_ranges = Vec::with_capacity(self.expected_segments.len());
        let deleted_rows = if self.duplicate_receipt.is_none()
            && self.plan.kernel.disposition == WriteDisposition::Replace
        {
            let count = count_target_rows(connection, &self.plan)?;
            connection
                .execute(&format!("DELETE FROM {}", self.plan.target.quoted()), [])
                .map_err(|error| classify_sqlite_error("replace SQLite target", error))?;
            count
        } else {
            0
        };
        for segment in segments {
            self.cancellation.check()?;
            let segment = segment?;
            let expected = validate_package_segment(
                &segment,
                &self.expected_segments,
                &self.plan,
                &mut self.accepted_segments,
            )?;
            if self.duplicate_receipt.is_none() {
                write_segment(
                    connection,
                    &self.plan,
                    &segment,
                    first_row_key,
                    &self.cancellation,
                )?;
                let start = first_row_key
                    .checked_add(expected.package_row_ord_start)
                    .ok_or_else(|| CdfError::data("SQLite segment row-key range overflowed"))?;
                let end = start
                    .checked_add(expected.state.row_count)
                    .ok_or_else(|| CdfError::data("SQLite segment row-key range overflowed"))?;
                row_ranges.push(SegmentRowRange {
                    segment_id: expected.state.segment_id.clone(),
                    row_key_start: start,
                    row_key_end: end,
                });
            }
            acknowledgements.push(SegmentAck {
                kind: expected.state.kind,
                segment_id: expected.state.segment_id.clone(),
                row_count: expected.state.row_count,
                byte_count: expected.state.byte_count,
            });
        }
        require_complete_segments(&self.accepted_segments, &self.expected_segments)?;
        let rows = acknowledgements.iter().try_fold(0_u64, |total, ack| {
            total
                .checked_add(ack.row_count)
                .ok_or_else(|| CdfError::data("SQLite acknowledged row count overflowed"))
        })?;
        let counts = if let Some(stored) = &self.duplicate_receipt {
            expected_counts(&self.plan, stored)?
        } else {
            finish_payload(
                connection,
                &self.plan,
                rows,
                deleted_rows,
                &self.cancellation,
            )?
        };
        self.write_complete_receipt(counts, row_ranges)?;
        Ok(acknowledgements)
    }

    fn finalize(self: Box<Self>) -> Result<Receipt> {
        (*self).finalize_receipt()
    }

    fn abort(mut self: Box<Self>) -> Result<()> {
        self.rollback_open_transaction()
    }
}

impl CommitSession for ManagedSqliteCommitSession {
    fn apply_migrations(&mut self) -> Result<()> {
        self.with_inner(CommitSession::apply_migrations)
    }

    fn write_segments(
        &mut self,
        segments: cdf_kernel::CommitSegmentIterator,
    ) -> Result<Vec<SegmentAck>> {
        self.with_inner(move |inner| CommitSession::write_segments(inner, segments))
    }

    fn finalize(mut self: Box<Self>) -> Result<Receipt> {
        let inner = self
            .inner
            .take()
            .ok_or_else(|| CdfError::internal("managed SQLite session lost its inner state"))?;
        self.execution
            .run_blocking("sqlite.destination.sync", move || inner.finalize_receipt())
    }

    fn abort(mut self: Box<Self>) -> Result<()> {
        let mut inner = self
            .inner
            .take()
            .ok_or_else(|| CdfError::internal("managed SQLite session lost its inner state"))?;
        self.execution
            .run_blocking("sqlite.destination.sync", move || {
                inner.rollback_open_transaction()
            })
    }
}

pub(crate) fn validate_session_begin_inputs(
    request: &cdf_kernel::DestinationCommitRequest,
    plan: &cdf_kernel::CommitPlan,
    load_plan: &SqliteLoadPlan,
) -> Result<()> {
    if plan != &load_plan.kernel
        || request.target != load_plan.kernel.target
        || request.disposition != load_plan.kernel.disposition
        || request.package_hash != load_plan.package_hash
        || request.idempotency_token != load_plan.idempotency_token
        || request.segments != load_plan.segments
    {
        return Err(CdfError::destination(
            "SQLite commit request does not match prepared load plan",
        ));
    }
    Ok(())
}

#[cfg(test)]
fn exit_after_commit_for_test() {
    if std::env::var_os(TEST_EXIT_AFTER_COMMIT_ENV).is_some() {
        std::process::exit(TEST_EXIT_AFTER_COMMIT_CODE);
    }
}

fn now_ms(execution: &cdf_runtime::ExecutionServices) -> Result<i64> {
    i64::try_from(execution.unix_now().as_millis())
        .map_err(|_| CdfError::internal("execution host Unix milliseconds exceed i64"))
}
