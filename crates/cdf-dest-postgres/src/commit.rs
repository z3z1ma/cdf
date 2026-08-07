use std::collections::{BTreeMap, BTreeSet};

use postgres::{Client, NoTls, Row};

use cdf_dest_sql::{
    LoadMirrorKey, LoadMirrorMutation, LoadMirrorRow, MirrorCommit, MirrorInsertOutcome,
    QuarantineMirrorKey, QuarantineMirrorMutation, QuarantineMirrorRow, SegmentMirrorMutation,
    SegmentMirrorPolicy, SegmentMirrorRow, SegmentRowRange, StateMirrorKey, StateMirrorMutation,
    StateMirrorRow, TransactionalMirrorBackend, TransactionalMirrorManager,
};
use cdf_kernel::{
    CdfError, CheckpointId, CommitCounts, CommitPlan, CommitSegment, CommitSession,
    DestinationCommitRequest, IdempotencyToken, PackageHash, Receipt, ReceiptId, ResourceId,
    Result, SchemaHash, SegmentAck, SegmentId, WriteDisposition,
};
use cdf_postgres::{PostgresIdentifier, PostgresTarget, quote_identifier};

use crate::{
    CDF_QUARANTINE_TABLE, CDF_ROW_KEY_ALLOCATOR_TABLE, CDF_SEGMENTS_TABLE, CDF_STATE_TABLE,
    api::{PostgresCommitRequest, PostgresReceiptVerification, build_receipt},
    binary_copy::BinaryCopyEncoder,
    identifiers::{quote_identifier_unchecked, quote_user_identifier, validated_target_sql},
    mirrors::decode_postgres_load_row,
    models::PostgresDestination,
    package::PostgresExpectedSegment,
    plan::{PostgresLoadPlan, PostgresReceiptInput, PostgresStatement, StatementExpectation},
    rows::validate_schema_matches_plan,
    sheet::postgres_destination_sheet,
    validate::{disposition_name, plan_segment_acks, token_suffix},
};

impl PostgresDestination {
    pub fn connect(database_url: impl Into<String>) -> Result<Self> {
        let database_url = database_url.into();
        if database_url.trim().is_empty() {
            return Err(CdfError::contract("Postgres database URL cannot be empty"));
        }
        Ok(Self {
            sheet: postgres_destination_sheet(),
            database_url: Some(database_url),
            pending_correction: None,
            execution: None,
        })
    }

    pub fn database_url(&self) -> Option<&str> {
        self.database_url.as_deref()
    }

    pub(crate) fn begin_commit_session(
        &self,
        request: PostgresCommitRequest,
    ) -> Result<PostgresCommitSession> {
        let database_url = self.database_url.as_deref().ok_or_else(|| {
            CdfError::contract(
                "Postgres destination ingress requires a connected destination runtime",
            )
        })?;
        Ok(PostgresCommitSession {
            database_url: database_url.to_owned(),
            execution: self.execution.clone().ok_or_else(|| {
                CdfError::contract(
                    "Postgres commit execution requires injected ExecutionServices for receipt time",
                )
            })?,
            package: request.package,
            plan: request.plan,
            client: None,
            phase: PostgresCommitSessionPhase::Begun,
            duplicate_receipt: None,
            receipt: None,
            expected_segments: request.segments.expected,
            accepted_segments: BTreeSet::new(),
            first_row_key: None,
        })
    }

    pub fn verify_receipt(&self, receipt: &Receipt) -> Result<PostgresReceiptVerification> {
        let database_url = self.database_url.as_deref().ok_or_else(|| {
            CdfError::contract(
                "PostgresDestination::verify_receipt requires PostgresDestination::connect",
            )
        })?;
        let mut client = Client::connect(database_url, NoTls)
            .map_err(|error| postgres_error("connect to Postgres", error))?;
        match verify_receipt_with_client(&mut client, receipt) {
            Ok(()) => Ok(PostgresReceiptVerification {
                verified: true,
                receipt_id: receipt.receipt_id.clone(),
                reason: None,
            }),
            Err(error) => Ok(PostgresReceiptVerification {
                verified: false,
                receipt_id: receipt.receipt_id.clone(),
                reason: Some(error.to_string()),
            }),
        }
    }
}

pub(crate) struct PostgresCommitSession {
    database_url: String,
    execution: cdf_runtime::ExecutionServices,
    package: cdf_package_contract::SharedVerifiedPackageAccess,
    plan: PostgresLoadPlan,
    client: Option<Client>,
    phase: PostgresCommitSessionPhase,
    duplicate_receipt: Option<Receipt>,
    receipt: Option<Receipt>,
    expected_segments: BTreeMap<SegmentId, PostgresExpectedSegment>,
    accepted_segments: BTreeSet<SegmentId>,
    first_row_key: Option<i64>,
}

pub(crate) struct ManagedPostgresCommitSession {
    inner: Option<PostgresCommitSession>,
    execution: cdf_runtime::ExecutionServices,
}

impl ManagedPostgresCommitSession {
    pub(crate) fn new(
        inner: PostgresCommitSession,
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
        F: FnOnce(&mut PostgresCommitSession) -> Result<T> + Send + 'static,
    {
        let mut inner = self
            .inner
            .take()
            .ok_or_else(|| CdfError::internal("managed Postgres session lost its inner state"))?;
        let (inner, result) = self.execution.run_blocking("postgres.sync", move || {
            let result = operation(&mut inner);
            Ok((inner, result))
        })?;
        self.inner = Some(inner);
        result
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PostgresCommitSessionPhase {
    Begun,
    MigrationsApplied,
    Written,
}

impl PostgresCommitSession {
    fn finalize_receipt(mut self) -> Result<Receipt> {
        if self.phase != PostgresCommitSessionPhase::Written {
            return Err(CdfError::destination(format!(
                "cannot finalize Postgres commit session before all segments are written: accepted {} of {}",
                self.accepted_segments.len(),
                self.expected_segments.len()
            )));
        }
        let duplicate = self.duplicate_receipt.is_some();
        let receipt = self
            .duplicate_receipt
            .take()
            .or_else(|| self.receipt.take())
            .ok_or_else(|| CdfError::internal("Postgres commit session has no receipt"))?;
        let mut client = self
            .client
            .take()
            .ok_or_else(|| CdfError::internal("Postgres commit session has no transaction"))?;
        let context = if duplicate {
            "commit duplicate Postgres transaction"
        } else {
            "commit Postgres transaction"
        };
        client
            .batch_execute("COMMIT")
            .map_err(|error| postgres_error(context, error))?;

        Ok(receipt)
    }

    fn rollback_open_transaction(&mut self) -> Result<()> {
        let Some(mut client) = self.client.take() else {
            return Ok(());
        };
        client
            .batch_execute("ROLLBACK")
            .map_err(|error| postgres_error("abort Postgres transaction", error))
    }

    fn write_accepted_segments(
        &mut self,
        copied_rows: u64,
        deleted_rows: u64,
        row_ranges: Vec<SegmentRowRange>,
    ) -> Result<()> {
        if self.duplicate_receipt.is_some() {
            self.phase = PostgresCommitSessionPhase::Written;
            return Ok(());
        }

        let mut client = self
            .client
            .take()
            .ok_or_else(|| CdfError::internal("Postgres commit session has no transaction"))?;
        let xid = query_xid(&mut client, &self.plan)?;
        let committed_at_ms = now_ms(&self.execution)?;
        let counts = if self.expected_segments.is_empty() {
            CommitCounts::default()
        } else {
            apply_write_plan_after_payload(&mut client, &self.plan, copied_rows, deleted_rows)?
        };
        execute_statements(&mut client, &self.plan.post_write_ddl)?;
        let receipt = build_receipt(
            &self.plan,
            PostgresReceiptInput {
                receipt_id: receipt_id(&self.plan)?,
                xid,
                committed_at_ms,
                counts,
                duplicate: false,
            },
        )?;
        apply_mirror_commit(
            &mut client,
            self.package.as_ref(),
            &self.plan,
            &receipt,
            row_ranges,
        )?;
        verify_receipt_in_transaction(&mut client, &receipt)?;
        self.receipt = Some(receipt);
        self.client = Some(client);
        self.phase = PostgresCommitSessionPhase::Written;
        Ok(())
    }
}

impl CommitSession for PostgresCommitSession {
    fn apply_migrations(&mut self) -> Result<()> {
        if self.phase != PostgresCommitSessionPhase::Begun {
            return Err(CdfError::destination(
                "Postgres migrations have already been applied",
            ));
        }
        let mut client = Client::connect(&self.database_url, NoTls)
            .map_err(|error| postgres_error("connect to Postgres", error))?;
        client
            .batch_execute("BEGIN")
            .map_err(|error| postgres_error("begin Postgres transaction", error))?;
        set_target_schema_search_path(&mut client, &self.plan.target)?;
        execute_statements(&mut client, &self.plan.system_ddl)?;
        self.duplicate_receipt = find_duplicate_receipt(&mut client, &self.plan)?;
        if self.duplicate_receipt.is_none() && !self.expected_segments.is_empty() {
            execute_statements(&mut client, &self.plan.target_ddl)?;
            create_stage_table_if_required(&mut client, &self.plan)?;
            let package_rows =
                self.expected_segments
                    .values()
                    .try_fold(0_u64, |total, segment| {
                        total
                            .checked_add(segment.state.row_count)
                            .ok_or_else(|| CdfError::data("Postgres package row count overflow"))
                    })?;
            self.first_row_key = Some(allocate_row_key_range(&mut client, package_rows)?);
        }
        self.client = Some(client);
        self.phase = PostgresCommitSessionPhase::MigrationsApplied;
        if self.expected_segments.is_empty() {
            self.write_accepted_segments(0, 0, Vec::new())?;
        }
        Ok(())
    }

    fn write_segments(
        &mut self,
        segments: cdf_kernel::CommitSegmentIterator,
    ) -> Result<Vec<SegmentAck>> {
        if self.phase == PostgresCommitSessionPhase::Written {
            return Err(CdfError::destination(
                "Postgres commit session has already accepted all segments",
            ));
        }
        if self.phase != PostgresCommitSessionPhase::MigrationsApplied {
            return Err(CdfError::destination(
                "Postgres commit session must apply migrations before writing",
            ));
        }
        if !self.accepted_segments.is_empty() {
            return Err(CdfError::destination(
                "Postgres finalized package segments have already been submitted",
            ));
        }

        let mut accepted_segments = BTreeSet::new();
        let outcome = if self.duplicate_receipt.is_some() {
            let acknowledgements = validate_package_segments(
                segments,
                &self.expected_segments,
                &self.plan,
                &mut accepted_segments,
            )?;
            let copied_rows = acknowledgements.iter().try_fold(0_u64, |total, ack| {
                total
                    .checked_add(ack.row_count)
                    .ok_or_else(|| CdfError::data("Postgres duplicate row count overflowed"))
            })?;
            PayloadWriteOutcome {
                copied_rows,
                deleted_rows: 0,
                acknowledgements,
                row_ranges: Vec::new(),
            }
        } else {
            let package_row_key_start = self.first_row_key.ok_or_else(|| {
                CdfError::internal("Postgres package row-key allocator is not initialized")
            })?;
            let execution = &self.execution;
            let client = self
                .client
                .as_mut()
                .ok_or_else(|| CdfError::internal("Postgres commit session has no transaction"))?;
            prepare_and_copy_package_rows(
                client,
                &self.plan,
                segments,
                &self.expected_segments,
                &mut accepted_segments,
                package_row_key_start,
                execution,
            )?
        };
        require_complete_package_segments(&accepted_segments, &self.expected_segments)?;
        self.accepted_segments = accepted_segments;
        self.write_accepted_segments(
            outcome.copied_rows,
            outcome.deleted_rows,
            outcome.row_ranges,
        )?;
        Ok(outcome.acknowledgements)
    }

    fn finalize(self: Box<Self>) -> Result<Receipt> {
        (*self).finalize_receipt()
    }

    fn abort(mut self: Box<Self>) -> Result<()> {
        self.rollback_open_transaction()
    }
}

impl CommitSession for ManagedPostgresCommitSession {
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
            .ok_or_else(|| CdfError::internal("managed Postgres session lost its inner state"))?;
        self.execution
            .run_blocking("postgres.sync", move || inner.finalize_receipt())
    }

    fn abort(mut self: Box<Self>) -> Result<()> {
        let mut inner = self
            .inner
            .take()
            .ok_or_else(|| CdfError::internal("managed Postgres session lost its inner state"))?;
        self.execution
            .run_blocking("postgres.sync", move || inner.rollback_open_transaction())
    }
}

fn validate_commit_segment(
    segment: &CommitSegment,
    expected: &PostgresExpectedSegment,
    plan: &PostgresLoadPlan,
) -> Result<()> {
    if segment.state != expected.state {
        return Err(CdfError::data(format!(
            "Postgres commit segment {} state does not match destination commit request",
            segment.state.segment_id.as_str()
        )));
    }
    if segment.package_byte_count != expected.package_byte_count {
        return Err(CdfError::data(format!(
            "Postgres commit segment {} package byte count {} differs from manifest {}",
            segment.state.segment_id.as_str(),
            segment.package_byte_count,
            expected.package_byte_count
        )));
    }

    let mut row_count = 0_u64;
    let mut schema: Option<arrow_schema::SchemaRef> = None;
    for batch in &segment.batches {
        if let Some(expected_schema) = &schema {
            if batch.schema().as_ref() != expected_schema.as_ref() {
                return Err(CdfError::data(format!(
                    "Postgres commit segment {} contains mixed schemas",
                    segment.state.segment_id.as_str()
                )));
            }
        } else {
            schema = Some(batch.schema());
        }
        row_count += batch.num_rows() as u64;
    }
    if let Some(schema) = &schema {
        let logical = cdf_package_contract::logical_output_schema(schema.as_ref())?;
        validate_schema_matches_plan(&logical, &plan.columns)?;
    }
    if row_count != expected.state.row_count {
        return Err(CdfError::data(format!(
            "Postgres commit segment {} has {} payload rows but request expects {}",
            segment.state.segment_id.as_str(),
            row_count,
            expected.state.row_count
        )));
    }
    cdf_package_contract::validate_package_row_ord_batches(
        &segment.batches,
        expected.package_row_ord_start,
        expected.state.row_count,
    )?;
    Ok(())
}

pub(crate) fn validate_session_begin_inputs(
    request: &DestinationCommitRequest,
    plan: &CommitPlan,
    load_plan: &PostgresLoadPlan,
) -> Result<()> {
    if plan != &load_plan.kernel {
        return Err(CdfError::destination(
            "Postgres commit session plan does not match prepared load plan",
        ));
    }
    if request.target != load_plan.kernel.target
        || request.disposition != load_plan.kernel.disposition
        || request.package_hash != load_plan.package_hash
        || request.idempotency_token != load_plan.idempotency_token
    {
        return Err(CdfError::destination(
            "Postgres commit request does not match prepared load plan",
        ));
    }

    let request_segments = request
        .segments
        .iter()
        .map(|segment| {
            (
                segment.segment_id.as_str(),
                (segment.row_count, segment.byte_count),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let plan_segments = plan_segment_acks(load_plan)
        .into_iter()
        .map(|ack| {
            (
                ack.segment_id.as_str().to_owned(),
                (ack.row_count, ack.byte_count),
            )
        })
        .collect::<BTreeMap<_, _>>();
    if request_segments.len() != plan_segments.len() {
        return Err(CdfError::destination(
            "Postgres commit request segment count does not match prepared load plan",
        ));
    }
    for (segment_id, counts) in request_segments {
        if plan_segments.get(segment_id) != Some(&counts) {
            return Err(CdfError::destination(format!(
                "Postgres commit request segment {segment_id} does not match prepared load plan"
            )));
        }
    }
    Ok(())
}

fn execute_statements(client: &mut Client, statements: &[PostgresStatement]) -> Result<()> {
    for statement in statements {
        client
            .batch_execute(&statement.sql)
            .map_err(|error| postgres_error(format!("execute {}", statement.name), error))?;
    }
    Ok(())
}

fn find_duplicate_receipt(client: &mut Client, plan: &PostgresLoadPlan) -> Result<Option<Receipt>> {
    let mut backend = PostgresMirrorBackend { client, plan };
    TransactionalMirrorManager::new(&mut backend).find_duplicate(
        &LoadMirrorKey {
            target: plan.kernel.target.clone(),
            package_hash: plan.package_hash.clone(),
            idempotency_token: plan.idempotency_token.clone(),
        },
        |stored| expected_postgres_duplicate(plan, stored),
    )
}

fn expected_postgres_duplicate(plan: &PostgresLoadPlan, stored: &Receipt) -> Result<Receipt> {
    validate_postgres_duplicate_counts(stored)?;
    let xid = stored
        .transaction
        .as_ref()
        .and_then(|transaction| transaction.values.get("xid"))
        .cloned()
        .ok_or_else(|| CdfError::data("Postgres duplicate receipt is missing xid evidence"))?;
    build_receipt(
        plan,
        PostgresReceiptInput {
            receipt_id: receipt_id(plan)?,
            xid,
            committed_at_ms: stored.committed_at_ms,
            counts: stored.counts.clone(),
            duplicate: false,
        },
    )
}

pub(crate) fn validate_postgres_duplicate_counts(stored: &Receipt) -> Result<()> {
    let rows = stored.segment_acks.iter().try_fold(0_u64, |total, ack| {
        total
            .checked_add(ack.row_count)
            .ok_or_else(|| CdfError::data("Postgres duplicate row count overflowed"))
    })?;
    let counts = &stored.counts;
    let valid = if stored.segment_acks.is_empty() {
        counts == &CommitCounts::default()
    } else {
        counts.rows_written == rows
            && match stored.disposition {
                WriteDisposition::Append => {
                    counts.rows_inserted == Some(rows)
                        && counts.rows_updated == Some(0)
                        && counts.rows_deleted == Some(0)
                }
                WriteDisposition::Replace => {
                    counts.rows_inserted == Some(rows)
                        && counts.rows_updated == Some(0)
                        && counts.rows_deleted.is_some()
                }
                WriteDisposition::Merge => counts
                    .rows_inserted
                    .zip(counts.rows_updated)
                    .is_some_and(|(inserted, updated)| {
                        inserted.checked_add(updated) == Some(rows)
                            && counts.rows_deleted == Some(0)
                    }),
                WriteDisposition::CdcApply => false,
            }
    };
    if valid {
        Ok(())
    } else {
        Err(CdfError::destination(
            "Postgres duplicate receipt counts contradict its segment acknowledgements",
        ))
    }
}

fn query_xid(client: &mut Client, plan: &PostgresLoadPlan) -> Result<String> {
    client
        .query_one(&plan.xid_probe.sql, &[])
        .map(|row| row.get(0))
        .map_err(|error| postgres_error("query Postgres xid", error))
}

fn create_stage_table_if_required(client: &mut Client, plan: &PostgresLoadPlan) -> Result<()> {
    let Some(statement) = plan
        .write_sql
        .iter()
        .find(|statement| statement.name == "create_stage")
    else {
        return Ok(());
    };
    client
        .batch_execute(&statement.sql)
        .map_err(|error| postgres_error("create Postgres stage table", error))
}

fn apply_write_plan_after_payload(
    client: &mut Client,
    plan: &PostgresLoadPlan,
    copied_rows: u64,
    deleted_rows: u64,
) -> Result<CommitCounts> {
    let rows_deleted = Some(deleted_rows);
    let mut rows_inserted = None;
    let mut rows_updated = Some(0_u64);
    let mut rows_written = 0_u64;

    for statement in &plan.write_sql {
        match statement.name.as_str() {
            "create_stage" | "copy_stage_binary" | "truncate_target_for_replace" => {}
            "copy_target_binary" => {
                rows_inserted = Some(copied_rows);
                rows_written = copied_rows;
            }
            "merge_duplicate_key_guard" => {
                let duplicates = client.query(&statement.sql, &[]).map_err(|error| {
                    postgres_error("query Postgres merge duplicate guard", error)
                })?;
                if !duplicates.is_empty() {
                    return Err(CdfError::data(
                        "Postgres finalized merge package contains duplicate merge keys",
                    ));
                }
            }
            "merge_from_stage" => {
                let source_rows = count_merge_source_rows(client, plan)?;
                let updated = count_merge_updates(client, plan)?;
                execute_count(client, statement)?;
                rows_written = source_rows;
                rows_inserted = Some(source_rows.saturating_sub(updated));
                rows_updated = Some(updated);
            }
            other => {
                return Err(CdfError::internal(format!(
                    "unsupported Postgres write statement {other}"
                )));
            }
        }
    }

    Ok(CommitCounts {
        rows_written,
        rows_inserted,
        rows_updated,
        rows_deleted,
    })
}

struct PayloadWriteOutcome {
    acknowledgements: Vec<SegmentAck>,
    copied_rows: u64,
    deleted_rows: u64,
    row_ranges: Vec<SegmentRowRange>,
}

fn prepare_and_copy_package_rows(
    client: &mut Client,
    plan: &PostgresLoadPlan,
    segments: cdf_kernel::CommitSegmentIterator,
    expected_segments: &BTreeMap<SegmentId, PostgresExpectedSegment>,
    accepted_segments: &mut BTreeSet<SegmentId>,
    package_row_key_start: i64,
    execution: &cdf_runtime::ExecutionServices,
) -> Result<PayloadWriteOutcome> {
    let deleted_rows = prepare_payload_target(client, plan)?;
    let copy = plan
        .write_sql
        .iter()
        .find(|statement| statement.expectation == StatementExpectation::CopyBinary)
        .ok_or_else(|| CdfError::internal("Postgres write plan omits binary COPY"))?;
    let writer = client
        .copy_in(&copy.sql)
        .map_err(|error| postgres_error(format!("open Postgres {}", copy.name), error))?;
    let mut encoder = BinaryCopyEncoder::new(writer, plan.columns.len())?;
    let mut acknowledgements = Vec::with_capacity(expected_segments.len());
    let mut segment_ranges = Vec::with_capacity(expected_segments.len());
    for segment in segments {
        let segment = segment?;
        let (expected, acknowledgement) =
            validate_package_segment(&segment, expected_segments, plan, accepted_segments)?;
        let loaded_at_ms = now_ms(execution)?;
        for batch in segment.into_batches()? {
            encoder.write_batch(&batch.batch, package_row_key_start, loaded_at_ms)?;
        }
        let segment_row_key_start = package_row_key_start
            .checked_add(
                i64::try_from(expected.package_row_ord_start)
                    .map_err(|_| CdfError::data("Postgres package row ordinal exceeds BIGINT"))?,
            )
            .ok_or_else(|| CdfError::data("Postgres segment row key overflowed BIGINT"))?;
        let row_key_start = u64::try_from(segment_row_key_start).map_err(|_| {
            CdfError::internal("Postgres row-key allocator returned a negative key")
        })?;
        let row_key_end = row_key_start
            .checked_add(expected.state.row_count)
            .ok_or_else(|| CdfError::data("Postgres segment row-key range overflowed"))?;
        segment_ranges.push(SegmentRowRange {
            segment_id: expected.state.segment_id.clone(),
            row_key_start,
            row_key_end,
        });
        acknowledgements.push(acknowledgement);
    }
    let (writer, encoded_rows) = encoder.finish()?;
    let copied = writer
        .finish()
        .map_err(|error| postgres_copy_error(format!("finish Postgres {}", copy.name), error))?;
    if copied != encoded_rows {
        return Err(CdfError::destination(format!(
            "Postgres binary COPY accepted {copied} rows but encoded {encoded_rows}"
        )));
    }
    let acknowledged_rows = acknowledgements.iter().try_fold(0_u64, |total, ack| {
        total
            .checked_add(ack.row_count)
            .ok_or_else(|| CdfError::data("Postgres acknowledged row count overflowed"))
    })?;
    if copied != acknowledged_rows {
        return Err(CdfError::destination(format!(
            "Postgres binary COPY accepted {copied} rows but segment acknowledgements cover {acknowledged_rows}"
        )));
    }
    Ok(PayloadWriteOutcome {
        acknowledgements,
        copied_rows: copied,
        deleted_rows,
        row_ranges: segment_ranges,
    })
}

fn prepare_payload_target(client: &mut Client, plan: &PostgresLoadPlan) -> Result<u64> {
    if plan.kernel.disposition != WriteDisposition::Replace {
        return Ok(0);
    }
    let deleted_rows = count_target_rows(client, &plan.target)?;
    let truncate = plan
        .write_sql
        .iter()
        .find(|statement| statement.name == "truncate_target_for_replace")
        .ok_or_else(|| CdfError::internal("Postgres replace plan omits target truncation"))?;
    client
        .batch_execute(&truncate.sql)
        .map_err(|error| postgres_error("truncate Postgres target", error))?;
    Ok(deleted_rows)
}

fn validate_package_segments(
    segments: cdf_kernel::CommitSegmentIterator,
    expected_segments: &BTreeMap<SegmentId, PostgresExpectedSegment>,
    plan: &PostgresLoadPlan,
    accepted_segments: &mut BTreeSet<SegmentId>,
) -> Result<Vec<SegmentAck>> {
    segments
        .map(|segment| {
            let segment = segment?;
            validate_package_segment(&segment, expected_segments, plan, accepted_segments)
                .map(|(_, acknowledgement)| acknowledgement)
        })
        .collect()
}

fn validate_package_segment<'a>(
    segment: &CommitSegment,
    expected_segments: &'a BTreeMap<SegmentId, PostgresExpectedSegment>,
    plan: &PostgresLoadPlan,
    accepted_segments: &mut BTreeSet<SegmentId>,
) -> Result<(&'a PostgresExpectedSegment, SegmentAck)> {
    let segment_id = &segment.state.segment_id;
    if accepted_segments.contains(segment_id) {
        return Err(CdfError::data(format!(
            "Postgres commit session received duplicate segment {}",
            segment_id.as_str()
        )));
    }
    let expected = expected_segments.get(segment_id).ok_or_else(|| {
        CdfError::data(format!(
            "Postgres commit segment {} is not in the planned package request",
            segment_id.as_str()
        ))
    })?;
    validate_commit_segment(segment, expected, plan)?;
    accepted_segments.insert(segment_id.clone());
    Ok((
        expected,
        SegmentAck {
            kind: expected.state.kind,
            segment_id: expected.state.segment_id.clone(),
            row_count: expected.state.row_count,
            byte_count: expected.state.byte_count,
        },
    ))
}

fn require_complete_package_segments(
    accepted_segments: &BTreeSet<SegmentId>,
    expected_segments: &BTreeMap<SegmentId, PostgresExpectedSegment>,
) -> Result<()> {
    if accepted_segments.len() == expected_segments.len() {
        return Ok(());
    }
    let missing = expected_segments
        .keys()
        .find(|segment_id| !accepted_segments.contains(*segment_id))
        .ok_or_else(|| {
            CdfError::internal("Postgres package segment cardinality is inconsistent")
        })?;
    Err(CdfError::data(format!(
        "Postgres finalized package stream omitted segment {}",
        missing.as_str()
    )))
}

fn allocate_row_key_range(client: &mut Client, row_count: u64) -> Result<i64> {
    let row_count = i64::try_from(row_count)
        .map_err(|_| CdfError::data("Postgres segment row count exceeds BIGINT"))?;
    if row_count <= 0 {
        return Err(CdfError::data(
            "Postgres cannot allocate a row-key range for an empty segment",
        ));
    }
    let sql = format!(
        "UPDATE {} SET \"next_key\" = \"next_key\" + $1 WHERE \"singleton\" RETURNING \"next_key\" - $1",
        quote_identifier_unchecked(CDF_ROW_KEY_ALLOCATOR_TABLE)
    );
    client
        .query_one(&sql, &[&row_count])
        .map(|row| row.get(0))
        .map_err(|error| postgres_error("allocate Postgres row-key range", error))
}

fn execute_count(client: &mut Client, statement: &PostgresStatement) -> Result<u64> {
    client
        .execute(&statement.sql, &[])
        .map_err(|error| postgres_error(format!("execute {}", statement.name), error))
}

fn count_target_rows(client: &mut Client, target: &PostgresTarget) -> Result<u64> {
    let sql = format!("SELECT COUNT(*)::bigint FROM {}", target.sql());
    let count: i64 = client
        .query_one(&sql, &[])
        .map(|row| row.get(0))
        .map_err(|error| postgres_error("count Postgres target rows", error))?;
    u64::try_from(count).map_err(|_| CdfError::internal("Postgres count was negative"))
}

fn count_merge_source_rows(client: &mut Client, plan: &PostgresLoadPlan) -> Result<u64> {
    let stage_table = merge_stage_table(plan)?;
    let sql = format!("SELECT COUNT(*)::bigint FROM {}", stage_table.quoted());
    query_count(client, &sql, "count Postgres merge source rows")
}

fn count_merge_updates(client: &mut Client, plan: &PostgresLoadPlan) -> Result<u64> {
    let stage_table = merge_stage_table(plan)?;
    let sql = format!(
        "SELECT COUNT(*)::bigint FROM {} AS \"target\" WHERE EXISTS (SELECT 1 FROM {} AS \"stage\" WHERE {})",
        validated_target_sql(&plan.target)?,
        stage_table.quoted(),
        merge_match_predicate(&plan.merge_keys)?
    );
    query_count(client, &sql, "count Postgres merge updates")
}

fn query_count(client: &mut Client, sql: &str, context: &str) -> Result<u64> {
    let count: i64 = client
        .query_one(sql, &[])
        .map(|row| row.get(0))
        .map_err(|error| postgres_error(context, error))?;
    u64::try_from(count).map_err(|_| CdfError::internal("Postgres count was negative"))
}

fn merge_stage_table(plan: &PostgresLoadPlan) -> Result<&PostgresIdentifier> {
    plan.stage_table
        .as_ref()
        .ok_or_else(|| CdfError::internal("Postgres merge plan omits its stage table"))
}

fn merge_match_predicate(keys: &[PostgresIdentifier]) -> Result<String> {
    keys.iter()
        .map(|key| {
            let key = quote_user_identifier(key)?;
            Ok(format!(
                "\"target\".{key} IS NOT DISTINCT FROM \"stage\".{key}"
            ))
        })
        .collect::<Result<Vec<_>>>()
        .map(|predicates| predicates.join(" AND "))
}

fn apply_mirror_commit(
    client: &mut Client,
    package: &dyn cdf_package_contract::VerifiedPackageAccess,
    plan: &PostgresLoadPlan,
    receipt: &Receipt,
    row_ranges: Vec<SegmentRowRange>,
) -> Result<()> {
    let resource_id = plan.resource_id.clone().or_else(|| {
        plan.state_delta
            .as_ref()
            .map(|delta| delta.resource_id.clone())
    });
    let commit = MirrorCommit::new(
        receipt.clone(),
        resource_id,
        plan.state_delta.as_ref(),
        &plan.segments,
        row_ranges,
        SegmentMirrorPolicy::Persist {
            require_row_ranges: !receipt.segment_acks.is_empty(),
        },
    )?;
    let mut backend = PostgresMirrorBackend { client, plan };
    TransactionalMirrorManager::new(&mut backend)
        .apply_with_quarantines(commit, |visitor| {
            package.for_each_quarantine_record(visitor)
        })
        .map(|_| ())
}

struct PostgresMirrorBackend<'a> {
    client: &'a mut Client,
    plan: &'a PostgresLoadPlan,
}

impl TransactionalMirrorBackend for PostgresMirrorBackend<'_> {
    fn read_load(&mut self, key: &LoadMirrorKey) -> Result<Option<LoadMirrorRow>> {
        self.client
            .query_one(
                &self.plan.idempotency_lock.sql,
                &[&key.target.as_str(), &key.package_hash.as_str()],
            )
            .map_err(|error| postgres_error("lock Postgres load idempotency key", error))?;
        self.client
            .query_opt(
                &self.plan.idempotency_check.sql,
                &[
                    &key.target.as_str(),
                    &key.package_hash.as_str(),
                    &key.idempotency_token.as_str(),
                ],
            )
            .map_err(|error| postgres_error("query Postgres _cdf_loads idempotency", error))?
            .map(decode_postgres_load_row)
            .transpose()
    }

    fn insert_load(
        &mut self,
        mutation: &LoadMirrorMutation,
    ) -> Result<MirrorInsertOutcome<LoadMirrorRow>> {
        insert_load_mirror(self.client, self.plan, mutation)
    }

    fn read_state(&mut self, key: &StateMirrorKey) -> Result<Option<StateMirrorRow>> {
        read_state_mirror(self.client, key)
    }

    fn upsert_state(
        &mut self,
        mutation: &StateMirrorMutation,
    ) -> Result<MirrorInsertOutcome<StateMirrorRow>> {
        upsert_state_mirror(self.client, self.plan, mutation)
    }

    fn insert_segment(
        &mut self,
        mutation: &SegmentMirrorMutation,
    ) -> Result<MirrorInsertOutcome<SegmentMirrorRow>> {
        insert_segment_mirror(self.client, mutation)
    }

    fn read_mirror_segment(
        &mut self,
        mutation: &SegmentMirrorMutation,
    ) -> Result<Option<SegmentMirrorRow>> {
        read_segment_mirror(self.client, mutation)
    }

    fn insert_quarantine(
        &mut self,
        mutation: &QuarantineMirrorMutation,
    ) -> Result<MirrorInsertOutcome<QuarantineMirrorRow>> {
        insert_quarantine_mirror(self.client, self.plan, mutation)
    }

    fn read_quarantine(
        &mut self,
        key: &QuarantineMirrorKey,
    ) -> Result<Option<QuarantineMirrorRow>> {
        read_quarantine_mirror(self.client, key)
    }
}

fn insert_load_mirror(
    client: &mut Client,
    plan: &PostgresLoadPlan,
    mutation: &LoadMirrorMutation,
) -> Result<MirrorInsertOutcome<LoadMirrorRow>> {
    let receipt = &mutation.receipt;
    let statement = plan
        .mirror_sql
        .iter()
        .find(|statement| statement.name == "record_cdf_load")
        .ok_or_else(|| CdfError::internal("Postgres plan missing record_cdf_load statement"))?;
    let migrations_json = serde_json::to_string(&receipt.migrations).map_err(json_error)?;
    let receipt_json = serde_json::to_string(receipt).map_err(json_error)?;
    let xid = receipt
        .transaction
        .as_ref()
        .and_then(|metadata| metadata.values.get("xid"))
        .ok_or_else(|| CdfError::internal("Postgres receipt missing xid"))?;
    let duplicate = mutation.duplicate;
    let resource_id = mutation.resource_id.as_ref().map(ResourceId::as_str);
    let target = receipt.target.as_str();
    let package_hash = receipt.package_hash.as_str();
    let idempotency_token = receipt.idempotency_token.as_str();
    let disposition = disposition_name(&receipt.disposition);
    let schema_hash = receipt.schema_hash.as_str();
    let rows_written = to_i64(receipt.counts.rows_written, "rows_written")?;
    let rows_inserted = optional_to_i64(receipt.counts.rows_inserted, "rows_inserted")?;
    let rows_updated = optional_to_i64(receipt.counts.rows_updated, "rows_updated")?;
    let rows_deleted = optional_to_i64(receipt.counts.rows_deleted, "rows_deleted")?;
    let segment_count = to_i64(receipt.segment_acks.len() as u64, "segment_count")?;
    client
        .query_opt(
            &statement.sql,
            &[
                &receipt.receipt_id.as_str(),
                &target,
                &package_hash,
                &resource_id,
                &idempotency_token,
                &disposition,
                &schema_hash,
                &rows_written,
                &rows_inserted,
                &rows_updated,
                &rows_deleted,
                &segment_count,
                &migrations_json,
                &receipt_json,
                &xid,
                &duplicate,
                &receipt.committed_at_ms,
            ],
        )
        .map_err(|error| postgres_error("insert Postgres _cdf_loads mirror", error))?
        .map(|row| {
            let json: String = row.get(0);
            serde_json::from_str(&json)
                .map(|receipt| MirrorInsertOutcome::Inserted(LoadMirrorRow { receipt }))
                .map_err(json_error)
        })
        .transpose()
        .map(|outcome| outcome.unwrap_or(MirrorInsertOutcome::Conflict))
}

fn upsert_state_mirror(
    client: &mut Client,
    plan: &PostgresLoadPlan,
    mutation: &StateMirrorMutation,
) -> Result<MirrorInsertOutcome<StateMirrorRow>> {
    let statement = plan
        .mirror_sql
        .iter()
        .find(|statement| statement.name == "upsert_cdf_state")
        .ok_or_else(|| CdfError::internal("Postgres plan missing upsert_cdf_state statement"))?;
    let scope_json = serde_json::to_string(&mutation.key.scope).map_err(json_error)?;
    let output_position_json =
        serde_json::to_string(&mutation.output_position).map_err(json_error)?;
    let state_version = i32::from(mutation.state_version);
    let parent_checkpoint_id = mutation
        .parent_checkpoint_id
        .as_ref()
        .map(CheckpointId::as_str);
    client
        .query_opt(
            &statement.sql,
            &[
                &mutation.key.pipeline_id.as_str(),
                &mutation.key.resource_id.as_str(),
                &scope_json,
                &state_version,
                &mutation.checkpoint_id.as_str(),
                &parent_checkpoint_id,
                &mutation.package_hash.as_str(),
                &mutation.schema_hash.as_str(),
                &output_position_json,
                &mutation.receipt_id.as_str(),
                &mutation.committed_at_ms,
            ],
        )
        .map_err(|error| postgres_error("upsert Postgres _cdf_state mirror", error))?
        .map(|row| decode_state_row(row, &mutation.key))
        .transpose()
        .map(|row| {
            row.map(MirrorInsertOutcome::Inserted)
                .unwrap_or(MirrorInsertOutcome::Conflict)
        })
}

fn read_state_mirror(client: &mut Client, key: &StateMirrorKey) -> Result<Option<StateMirrorRow>> {
    let scope_json = serde_json::to_string(&key.scope).map_err(json_error)?;
    client
        .query_opt(
            &format!(
                "SELECT \"state_version\", \"checkpoint_id\", \"parent_checkpoint_id\", \"package_hash\", \"schema_hash\", \"output_position_json\"::text, \"receipt_id\", \"committed_at_ms\" FROM {} WHERE \"pipeline_id\" = $1 AND \"resource_id\" = $2 AND \"scope\" = $3",
                quote_identifier_unchecked(CDF_STATE_TABLE)
            ),
            &[
                &key.pipeline_id.as_str(),
                &key.resource_id.as_str(),
                &scope_json,
            ],
        )
        .map_err(|error| postgres_error("read Postgres _cdf_state mirror", error))?
        .map(|row| decode_state_row(row, key))
        .transpose()
}

fn decode_state_row(row: Row, key: &StateMirrorKey) -> Result<StateMirrorRow> {
    let state_version = u16::try_from(row.get::<_, i32>(0))
        .map_err(|_| CdfError::data("Postgres state_version exceeds u16 authority"))?;
    let parent_checkpoint_id = row
        .get::<_, Option<String>>(2)
        .map(CheckpointId::new)
        .transpose()?;
    let output_position_json: String = row.get(5);
    Ok(StateMirrorRow {
        mutation: StateMirrorMutation {
            key: key.clone(),
            state_version,
            checkpoint_id: CheckpointId::new(row.get::<_, String>(1))?,
            parent_checkpoint_id,
            package_hash: PackageHash::new(row.get::<_, String>(3))?,
            schema_hash: SchemaHash::new(row.get::<_, String>(4))?,
            output_position: serde_json::from_str(&output_position_json).map_err(json_error)?,
            receipt_id: ReceiptId::new(row.get::<_, String>(6))?,
            committed_at_ms: row.get(7),
        },
    })
}

fn insert_segment_mirror(
    client: &mut Client,
    mutation: &SegmentMirrorMutation,
) -> Result<MirrorInsertOutcome<SegmentMirrorRow>> {
    let Some(range) = &mutation.row_range else {
        return Ok(MirrorInsertOutcome::Inserted(SegmentMirrorRow::from(
            mutation,
        )));
    };
    let row_key_start = i64::try_from(range.row_key_start)
        .map_err(|_| CdfError::data("Postgres segment row key exceeds BIGINT"))?;
    let row_key_end = i64::try_from(range.row_key_end)
        .map_err(|_| CdfError::data("Postgres segment row key exceeds BIGINT"))?;
    let scope_json = mutation
        .scope
        .as_ref()
        .map(|scope| serde_json::to_string(scope).map_err(json_error))
        .transpose()?;
    let output_position_json = mutation
        .output_position
        .as_ref()
        .map(|position| serde_json::to_string(position).map_err(json_error))
        .transpose()?;
    let row_count = to_i64(mutation.row_count, "segment row_count")?;
    let byte_count = to_i64(mutation.byte_count, "segment byte_count")?;
    let sql = format!(
        "INSERT INTO {} (\"row_key_start\", \"row_key_end\", \"target\", \"package_hash\", \"idempotency_token\", \"segment_id\", \"scope_json\", \"output_position_json\", \"row_count\", \"byte_count\", \"committed_at_ms\") \
         SELECT $1, $2, $3, $4, $5, $6, $7::text::jsonb, $8::text::jsonb, $9, $10, $11 \
         WHERE NOT EXISTS ( \
             SELECT 1 FROM {} WHERE \"row_key_start\" < $2 AND \"row_key_end\" > $1 \
         ) \
         RETURNING \"row_key_start\", \"row_key_end\", \"idempotency_token\", \"scope_json\"::text, \"output_position_json\"::text, \"row_count\", \"byte_count\", \"committed_at_ms\"",
        quote_identifier_unchecked(CDF_SEGMENTS_TABLE),
        quote_identifier_unchecked(CDF_SEGMENTS_TABLE)
    );
    client
        .query_opt(
            &sql,
            &[
                &row_key_start,
                &row_key_end,
                &mutation.target.as_str(),
                &mutation.package_hash.as_str(),
                &mutation.idempotency_token.as_str(),
                &mutation.segment_id.as_str(),
                &scope_json,
                &output_position_json,
                &row_count,
                &byte_count,
                &mutation.committed_at_ms,
            ],
        )
        .map_err(|error| postgres_error("record Postgres segment row-key range", error))?
        .map(|row| decode_segment_row(row, mutation))
        .transpose()
        .map(|row| {
            row.map(MirrorInsertOutcome::Inserted)
                .unwrap_or(MirrorInsertOutcome::Conflict)
        })
}

fn read_segment_mirror(
    client: &mut Client,
    mutation: &SegmentMirrorMutation,
) -> Result<Option<SegmentMirrorRow>> {
    client
        .query_opt(
            &format!(
                "SELECT \"row_key_start\", \"row_key_end\", \"idempotency_token\", \"scope_json\"::text, \"output_position_json\"::text, \"row_count\", \"byte_count\", \"committed_at_ms\" FROM {} WHERE \"target\" = $1 AND \"package_hash\" = $2 AND \"segment_id\" = $3",
                quote_identifier_unchecked(CDF_SEGMENTS_TABLE)
            ),
            &[
                &mutation.target.as_str(),
                &mutation.package_hash.as_str(),
                &mutation.segment_id.as_str(),
            ],
        )
        .map_err(|error| postgres_error("read Postgres _cdf_segments mirror", error))?
        .map(|row| decode_segment_row(row, mutation))
        .transpose()
}

fn decode_segment_row(row: Row, mutation: &SegmentMirrorMutation) -> Result<SegmentMirrorRow> {
    let scope_json: Option<String> = row.get(3);
    let output_position_json: Option<String> = row.get(4);
    Ok(SegmentMirrorRow {
        mutation: SegmentMirrorMutation {
            target: mutation.target.clone(),
            package_hash: mutation.package_hash.clone(),
            idempotency_token: IdempotencyToken::new(row.get::<_, String>(2))?,
            segment_id: mutation.segment_id.clone(),
            scope: scope_json
                .map(|json| serde_json::from_str(&json).map_err(json_error))
                .transpose()?,
            output_position: output_position_json
                .map(|json| serde_json::from_str(&json).map_err(json_error))
                .transpose()?,
            row_count: from_i64(row.get(5), "segment row_count")?,
            byte_count: from_i64(row.get(6), "segment byte_count")?,
            committed_at_ms: row.get(7),
            row_range: Some(SegmentRowRange {
                segment_id: mutation.segment_id.clone(),
                row_key_start: from_i64(row.get(0), "segment row_key_start")?,
                row_key_end: from_i64(row.get(1), "segment row_key_end")?,
            }),
        },
    })
}

fn insert_quarantine_mirror(
    client: &mut Client,
    plan: &PostgresLoadPlan,
    mutation: &QuarantineMirrorMutation,
) -> Result<MirrorInsertOutcome<QuarantineMirrorRow>> {
    let statement = plan
        .mirror_sql
        .iter()
        .find(|statement| statement.name == "record_cdf_quarantine")
        .ok_or_else(|| {
            CdfError::internal("Postgres plan missing record_cdf_quarantine statement")
        })?;
    let source_row_ordinal = to_i64(mutation.key.source_row_ordinal, "source_row_ordinal")?;
    let source_position_json = mutation
        .source_position
        .as_ref()
        .map(|position| serde_json::to_string(position).map_err(json_error))
        .transpose()?;
    let observed_value_json =
        serde_json::to_string(&mutation.observed_value_redacted).map_err(json_error)?;
    client
        .query_opt(
            &statement.sql,
            &[
                &mutation.key.target.as_str(),
                &mutation.key.package_hash.as_str(),
                &mutation.receipt_id.as_str(),
                &source_row_ordinal,
                &mutation.key.rule_id.as_str(),
                &mutation.key.error_code.as_str(),
                &source_position_json,
                &observed_value_json,
                &mutation.committed_at_ms,
            ],
        )
        .map_err(|error| postgres_error("insert Postgres _cdf_quarantine mirror", error))?
        .map(|row| decode_quarantine_row(row, &mutation.key))
        .transpose()
        .map(|row| {
            row.map(MirrorInsertOutcome::Inserted)
                .unwrap_or(MirrorInsertOutcome::Conflict)
        })
}

fn read_quarantine_mirror(
    client: &mut Client,
    key: &QuarantineMirrorKey,
) -> Result<Option<QuarantineMirrorRow>> {
    let source_row_ordinal = to_i64(key.source_row_ordinal, "source_row_ordinal")?;
    client
        .query_opt(
            &format!(
                "SELECT \"receipt_id\", \"source_position_json\"::text, \"observed_value_json\"::text, \"committed_at_ms\" FROM {} WHERE \"target\" = $1 AND \"package_hash\" = $2 AND \"source_row_ordinal\" = $3 AND \"rule_id\" = $4 AND \"error_code\" = $5",
                quote_identifier_unchecked(CDF_QUARANTINE_TABLE)
            ),
            &[
                &key.target.as_str(),
                &key.package_hash.as_str(),
                &source_row_ordinal,
                &key.rule_id.as_str(),
                &key.error_code.as_str(),
            ],
        )
        .map_err(|error| postgres_error("read Postgres _cdf_quarantine mirror", error))?
        .map(|row| decode_quarantine_row(row, key))
        .transpose()
}

fn decode_quarantine_row(row: Row, key: &QuarantineMirrorKey) -> Result<QuarantineMirrorRow> {
    let source_position_json: Option<String> = row.get(1);
    let observed_value_json: String = row.get(2);
    Ok(QuarantineMirrorRow {
        mutation: QuarantineMirrorMutation {
            key: key.clone(),
            receipt_id: ReceiptId::new(row.get::<_, String>(0))?,
            source_position: source_position_json
                .map(|json| serde_json::from_str(&json).map_err(json_error))
                .transpose()?,
            observed_value_redacted: serde_json::from_str(&observed_value_json)
                .map_err(json_error)?,
            committed_at_ms: row.get(3),
        },
    })
}

fn verify_receipt_in_transaction(client: &mut Client, receipt: &Receipt) -> Result<()> {
    let row = client
        .query_opt(
            &receipt.verify.statement,
            &[
                &verify_receipt_parameter(receipt, "target")?,
                &verify_receipt_parameter(receipt, "package_hash")?,
                &verify_receipt_parameter(receipt, "idempotency_token")?,
                &verify_receipt_parameter(receipt, "schema_hash")?,
            ],
        )
        .map_err(|error| postgres_error("verify Postgres receipt in transaction", error))?
        .ok_or_else(|| CdfError::destination("receipt is absent from Postgres _cdf_loads"))?;
    let stored = receipt_from_verify_row(row)?;
    if &stored == receipt {
        Ok(())
    } else {
        Err(CdfError::destination(
            "Postgres receipt verification read different receipt JSON",
        ))
    }
}

fn verify_receipt_with_client(client: &mut Client, receipt: &Receipt) -> Result<()> {
    set_receipt_schema_search_path(client, receipt)?;
    let rows = client
        .query(
            &receipt.verify.statement,
            &[
                &verify_receipt_parameter(receipt, "target")?,
                &verify_receipt_parameter(receipt, "package_hash")?,
                &verify_receipt_parameter(receipt, "idempotency_token")?,
                &verify_receipt_parameter(receipt, "schema_hash")?,
            ],
        )
        .map_err(|error| postgres_error("query Postgres receipt verification", error))?;
    let Some(row) = rows.into_iter().next() else {
        return Err(CdfError::destination(
            "receipt is absent from Postgres _cdf_loads",
        ));
    };
    let stored = receipt_from_verify_row(row)?;
    if &stored == receipt {
        Ok(())
    } else {
        Err(CdfError::destination(
            "stored Postgres receipt JSON differs from supplied receipt",
        ))
    }
}

fn set_target_schema_search_path(client: &mut Client, target: &PostgresTarget) -> Result<()> {
    let Some(schema) = &target.schema else {
        return Ok(());
    };
    client
        .batch_execute(&format!(
            "SET LOCAL search_path = {}, public",
            schema.quoted()
        ))
        .map_err(|error| postgres_error("set Postgres transaction search_path", error))?;
    Ok(())
}

fn set_receipt_schema_search_path(client: &mut Client, receipt: &Receipt) -> Result<()> {
    let Some(schema) = receipt.verify.parameters.get("target_schema") else {
        return Ok(());
    };
    let quoted = quote_identifier(schema)?;
    client
        .batch_execute(&format!("SET search_path = {quoted}, public"))
        .map_err(|error| postgres_error("set Postgres receipt search_path", error))?;
    Ok(())
}

fn receipt_from_verify_row(row: Row) -> Result<Receipt> {
    let json: String = row.get("receipt_json");
    serde_json::from_str(&json).map_err(json_error)
}

fn receipt_id(plan: &PostgresLoadPlan) -> Result<ReceiptId> {
    ReceiptId::new(format!(
        "postgres:{}:{}",
        plan.kernel.target.as_str(),
        token_suffix(plan.idempotency_token.as_str())
    ))
}

fn verify_receipt_parameter(receipt: &Receipt, name: &str) -> Result<String> {
    receipt
        .verify
        .parameters
        .get(name)
        .cloned()
        .ok_or_else(|| CdfError::internal(format!("verify clause missing {name}")))
}

fn to_i64(value: u64, name: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| CdfError::internal(format!("{name} exceeds i64")))
}

fn from_i64(value: i64, name: &str) -> Result<u64> {
    u64::try_from(value).map_err(|_| CdfError::data(format!("{name} is negative")))
}

fn optional_to_i64(value: Option<u64>, name: &str) -> Result<Option<i64>> {
    value.map(|value| to_i64(value, name)).transpose()
}

fn now_ms(execution: &cdf_runtime::ExecutionServices) -> Result<i64> {
    i64::try_from(execution.unix_now().as_millis())
        .map_err(|_| CdfError::internal("execution host Unix milliseconds exceed i64"))
}

fn postgres_error(context: impl Into<String>, error: postgres::Error) -> CdfError {
    CdfError::destination(format!("{}: {}", context.into(), error))
}

fn postgres_copy_error(context: impl Into<String>, error: postgres::Error) -> CdfError {
    let context = context.into();
    if error
        .code()
        .is_some_and(|code| code.code().starts_with("22"))
    {
        let (column, server_message, location) = error.as_db_error().map_or_else(
            || (String::new(), error.to_string(), String::new()),
            |db_error| {
                (
                    db_error
                        .column()
                        .map(|column| format!(" for column `{column}`"))
                        .unwrap_or_default(),
                    db_error.message().to_owned(),
                    db_error
                        .where_()
                        .map(|location| format!(" ({location})"))
                        .unwrap_or_default(),
                )
            },
        );
        return CdfError::data(format!(
            "{context}: PostgreSQL rejected a package value{column}: {server_message}{location}; repair the source value or choose a target declaration that admits it"
        ));
    }
    postgres_error(context, error)
}

fn json_error(error: serde_json::Error) -> CdfError {
    CdfError::data(error.to_string())
}
