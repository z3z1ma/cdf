use std::{
    collections::{BTreeMap, BTreeSet},
    time::{SystemTime, UNIX_EPOCH},
};

use postgres::{Client, NoTls, Row};

use crate::{
    binary_copy::BinaryCopyEncoder, dml::*, package::*, rows::validate_schema_matches_plan,
    validate::*, *,
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

    fn write_accepted_segments(&mut self, copied_rows: u64, deleted_rows: u64) -> Result<()> {
        if self.duplicate_receipt.is_some() {
            self.phase = PostgresCommitSessionPhase::Written;
            return Ok(());
        }

        let mut client = self
            .client
            .take()
            .ok_or_else(|| CdfError::internal("Postgres commit session has no transaction"))?;
        let xid = query_xid(&mut client, &self.plan)?;
        let committed_at_ms = now_ms()?;
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
        insert_load_mirror(&mut client, &self.plan, &receipt)?;
        if let Some(delta) = &self.plan.state_delta {
            upsert_state_mirror(&mut client, &self.plan, &receipt, delta)?;
        }
        insert_quarantine_mirror(&mut client, self.package.as_ref(), &self.plan, &receipt)?;
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
            self.write_accepted_segments(0, 0)?;
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
            }
        } else {
            let package_row_key_start = self.first_row_key.ok_or_else(|| {
                CdfError::internal("Postgres package row-key allocator is not initialized")
            })?;
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
            )?
        };
        require_complete_package_segments(&accepted_segments, &self.expected_segments)?;
        self.accepted_segments = accepted_segments;
        self.write_accepted_segments(outcome.copied_rows, outcome.deleted_rows)?;
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
        || request.package_hash.as_str() != verify_parameter(load_plan, "package_hash")?
        || request.idempotency_token.as_str() != verify_parameter(load_plan, "idempotency_token")?
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
    let target = plan.kernel.target.as_str();
    let package_hash = verify_parameter(plan, "package_hash")?;
    let row = client
        .query_opt(&plan.idempotency_check.sql, &[&target, &package_hash])
        .map_err(|error| postgres_error("query Postgres _cdf_loads idempotency", error))?;
    row.map(|row| {
        let json: String = row.get(0);
        serde_json::from_str(&json).map_err(json_error)
    })
    .transpose()
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
                        "Postgres merge package contains duplicate merge keys and dedup policy is fail",
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
}

fn prepare_and_copy_package_rows(
    client: &mut Client,
    plan: &PostgresLoadPlan,
    segments: cdf_kernel::CommitSegmentIterator,
    expected_segments: &BTreeMap<SegmentId, PostgresExpectedSegment>,
    accepted_segments: &mut BTreeSet<SegmentId>,
    package_row_key_start: i64,
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
        let loaded_at_ms = now_ms()?;
        for batch in segment.into_batches()? {
            encoder.write_batch(&batch.batch, package_row_key_start, loaded_at_ms)?;
        }
        let segment_row_key_start = package_row_key_start
            .checked_add(
                i64::try_from(expected.package_row_ord_start)
                    .map_err(|_| CdfError::data("Postgres package row ordinal exceeds BIGINT"))?,
            )
            .ok_or_else(|| CdfError::data("Postgres segment row key overflowed BIGINT"))?;
        segment_ranges.push((
            expected.state.segment_id.clone(),
            segment_row_key_start,
            expected.state.row_count,
        ));
        acknowledgements.push(acknowledgement);
    }
    let (writer, encoded_rows) = encoder.finish()?;
    let copied = writer
        .finish()
        .map_err(|error| postgres_error(format!("finish Postgres {}", copy.name), error))?;
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
    for (segment_id, row_key_start, row_count) in segment_ranges {
        insert_segment_range(client, plan, &segment_id, row_key_start, row_count)?;
    }
    Ok(PayloadWriteOutcome {
        acknowledgements,
        copied_rows: copied,
        deleted_rows,
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

fn insert_segment_range(
    client: &mut Client,
    plan: &PostgresLoadPlan,
    segment_id: &SegmentId,
    row_key_start: i64,
    row_count: u64,
) -> Result<()> {
    let row_count = i64::try_from(row_count)
        .map_err(|_| CdfError::data("Postgres segment row count exceeds BIGINT"))?;
    let row_key_end = row_key_start
        .checked_add(row_count)
        .ok_or_else(|| CdfError::data("Postgres segment row-key range overflowed BIGINT"))?;
    let sql = format!(
        "INSERT INTO {} (\"row_key_start\", \"row_key_end\", \"target\", \"package_hash\", \"segment_id\") VALUES ($1, $2, $3, $4, $5)",
        quote_identifier_unchecked(CDF_SEGMENTS_TABLE)
    );
    client
        .execute(
            &sql,
            &[
                &row_key_start,
                &row_key_end,
                &plan.kernel.target.as_str(),
                &verify_parameter(plan, "package_hash")?,
                &segment_id.as_str(),
            ],
        )
        .map(|_| ())
        .map_err(|error| postgres_error("record Postgres segment row-key range", error))
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
    let sql = match plan.dedup {
        MergeDedupPolicy::First | MergeDedupPolicy::Last => format!(
            "{}SELECT COUNT(*)::bigint FROM \"_cdf_dedup\"",
            merge_dedup_cte(plan)?
        ),
        MergeDedupPolicy::Fail => {
            format!("SELECT COUNT(*)::bigint FROM {}", stage_table.quoted())
        }
    };
    query_count(client, &sql, "count Postgres merge source rows")
}

fn count_merge_updates(client: &mut Client, plan: &PostgresLoadPlan) -> Result<u64> {
    let stage_table = merge_stage_table(plan)?;
    let (cte, source) = match plan.dedup {
        MergeDedupPolicy::First | MergeDedupPolicy::Last => {
            (merge_dedup_cte(plan)?, "\"_cdf_dedup\"".to_owned())
        }
        MergeDedupPolicy::Fail => (String::new(), stage_table.quoted()),
    };
    let sql = format!(
        "{cte}SELECT COUNT(*)::bigint FROM {} AS \"target\" WHERE EXISTS (SELECT 1 FROM {source} AS \"stage\" WHERE {})",
        plan.target.sql(),
        merge_match_predicate(&plan.merge_keys)
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

fn merge_dedup_cte(plan: &PostgresLoadPlan) -> Result<String> {
    let stage_table = merge_stage_table(plan)?;
    let conflict_columns = plan
        .merge_keys
        .iter()
        .map(PostgresIdentifier::quoted)
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!(
        "WITH \"_cdf_ranked\" AS (\n  SELECT {}, ROW_NUMBER() OVER (PARTITION BY {} ORDER BY {}, {}) AS \"_cdf_rank\"\n  FROM {}\n), \"_cdf_dedup\" AS (\n  SELECT * FROM \"_cdf_ranked\" WHERE \"_cdf_rank\" = 1\n)\n",
        stage_select_list(&plan.columns),
        conflict_columns,
        order_expression(CDF_ROW_KEY_COLUMN, &plan.dedup),
        order_expression(CDF_LOADED_AT_COLUMN, &plan.dedup),
        stage_table.quoted()
    ))
}

fn merge_stage_table(plan: &PostgresLoadPlan) -> Result<&PostgresIdentifier> {
    plan.stage_table
        .as_ref()
        .ok_or_else(|| CdfError::internal("Postgres merge plan omits its stage table"))
}

fn merge_match_predicate(keys: &[PostgresIdentifier]) -> String {
    keys.iter()
        .map(|key| {
            format!(
                "\"target\".{} IS NOT DISTINCT FROM \"stage\".{}",
                key.quoted(),
                key.quoted()
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn insert_load_mirror(
    client: &mut Client,
    plan: &PostgresLoadPlan,
    receipt: &Receipt,
) -> Result<()> {
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
    let duplicate = false;
    let resource_id = plan_resource_id(plan);
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
        .execute(
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
        .map_err(|error| postgres_error("insert Postgres _cdf_loads mirror", error))?;
    Ok(())
}

fn upsert_state_mirror(
    client: &mut Client,
    plan: &PostgresLoadPlan,
    receipt: &Receipt,
    delta: &StateDelta,
) -> Result<()> {
    let statement = plan
        .mirror_sql
        .iter()
        .find(|statement| statement.name == "upsert_cdf_state")
        .ok_or_else(|| CdfError::internal("Postgres plan missing upsert_cdf_state statement"))?;
    let scope_json = serde_json::to_string(&delta.scope).map_err(json_error)?;
    let output_position_json = serde_json::to_string(&delta.output_position).map_err(json_error)?;
    let state_version = i32::from(delta.state_version);
    client
        .execute(
            &statement.sql,
            &[
                &delta.pipeline_id.as_str(),
                &delta.resource_id.as_str(),
                &scope_json,
                &state_version,
                &delta.checkpoint_id.as_str(),
                &receipt.package_hash.as_str(),
                &receipt.schema_hash.as_str(),
                &output_position_json,
                &receipt.receipt_id.as_str(),
                &receipt.committed_at_ms,
            ],
        )
        .map_err(|error| postgres_error("upsert Postgres _cdf_state mirror", error))?;
    Ok(())
}

fn insert_quarantine_mirror(
    client: &mut Client,
    package: &dyn cdf_package_contract::VerifiedPackageAccess,
    plan: &PostgresLoadPlan,
    receipt: &Receipt,
) -> Result<()> {
    let records = package.quarantine_records()?;
    if records.is_empty() {
        return Ok(());
    }
    let statement = plan
        .mirror_sql
        .iter()
        .find(|statement| statement.name == "record_cdf_quarantine")
        .ok_or_else(|| {
            CdfError::internal("Postgres plan missing record_cdf_quarantine statement")
        })?;
    let target = receipt.target.as_str();
    let package_hash = receipt.package_hash.as_str();
    let receipt_id = receipt.receipt_id.as_str();
    for record in records {
        let source_row_ordinal = to_i64(record.source_row_ordinal, "source_row_ordinal")?;
        let source_position_json = record
            .source_position
            .map(|position| serde_json::to_string(&position).map_err(json_error))
            .transpose()?;
        let observed_value_json =
            serde_json::to_string(&record.observed_value_redacted).map_err(json_error)?;
        client
            .execute(
                &statement.sql,
                &[
                    &target,
                    &package_hash,
                    &receipt_id,
                    &source_row_ordinal,
                    &record.rule_id.as_str(),
                    &record.error_code.as_str(),
                    &source_position_json,
                    &observed_value_json,
                    &receipt.committed_at_ms,
                ],
            )
            .map_err(|error| postgres_error("insert Postgres _cdf_quarantine mirror", error))?;
    }
    Ok(())
}

fn verify_receipt_in_transaction(client: &mut Client, receipt: &Receipt) -> Result<()> {
    let row = query_verify_row(client, receipt)?;
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

fn query_verify_row(client: &mut Client, receipt: &Receipt) -> Result<Row> {
    client
        .query_opt(
            &receipt.verify.statement,
            &[
                &verify_receipt_parameter(receipt, "target")?,
                &verify_receipt_parameter(receipt, "package_hash")?,
                &verify_receipt_parameter(receipt, "idempotency_token")?,
                &verify_receipt_parameter(receipt, "schema_hash")?,
            ],
        )
        .map_err(|error| postgres_error("query Postgres receipt verification", error))?
        .ok_or_else(|| CdfError::destination("receipt is absent from Postgres _cdf_loads"))
}

fn receipt_from_verify_row(row: Row) -> Result<Receipt> {
    let json: String = row.get("receipt_json");
    serde_json::from_str(&json).map_err(json_error)
}

fn receipt_id(plan: &PostgresLoadPlan) -> Result<ReceiptId> {
    ReceiptId::new(format!(
        "postgres:{}:{}",
        plan.kernel.target.as_str(),
        token_suffix(verify_parameter(plan, "idempotency_token")?.as_str())
    ))
}

fn plan_resource_id(plan: &PostgresLoadPlan) -> Option<&str> {
    plan.resource_id
        .as_ref()
        .map(ResourceId::as_str)
        .or_else(|| {
            plan.state_delta
                .as_ref()
                .map(|delta| delta.resource_id.as_str())
        })
}

fn verify_parameter(plan: &PostgresLoadPlan, name: &str) -> Result<String> {
    plan.verify
        .parameters
        .get(name)
        .cloned()
        .ok_or_else(|| CdfError::internal(format!("verify clause missing {name}")))
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

fn optional_to_i64(value: Option<u64>, name: &str) -> Result<Option<i64>> {
    value.map(|value| to_i64(value, name)).transpose()
}

fn now_ms() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            CdfError::internal(format!("system clock is before UNIX_EPOCH: {error}"))
        })?;
    i64::try_from(duration.as_millis())
        .map_err(|_| CdfError::internal("system time milliseconds exceed i64"))
}

fn postgres_error(context: impl Into<String>, error: postgres::Error) -> CdfError {
    CdfError::destination(format!("{}: {}", context.into(), error))
}

pub(crate) fn io_error(context: impl Into<String>, error: std::io::Error) -> CdfError {
    CdfError::destination(format!("{}: {}", context.into(), error))
}

fn json_error(error: serde_json::Error) -> CdfError {
    CdfError::data(error.to_string())
}
