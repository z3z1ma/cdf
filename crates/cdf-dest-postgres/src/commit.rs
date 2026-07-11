use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
    time::{SystemTime, UNIX_EPOCH},
};

use postgres::{Client, NoTls, Row};

use crate::{dml::*, package::*, rows::validate_schema_matches_plan, validate::*, *};

impl PostgresDestination {
    pub fn connect(database_url: impl Into<String>) -> Result<Self> {
        let database_url = database_url.into();
        if database_url.trim().is_empty() {
            return Err(CdfError::contract("Postgres database URL cannot be empty"));
        }
        Ok(Self {
            sheet: postgres_destination_sheet(),
            database_url: Some(database_url),
            pending_commit: None,
            pending_correction: None,
            execution: None,
        })
    }

    pub fn database_url(&self) -> Option<&str> {
        self.database_url.as_deref()
    }

    pub fn commit_package(&self, request: PostgresCommitRequest) -> Result<PostgresCommitOutcome> {
        self.begin_commit_session(request, None)?.run_to_outcome()
    }

    pub(crate) fn begin_commit_session(
        &self,
        request: PostgresCommitRequest,
        commit_request: Option<DestinationCommitRequest>,
    ) -> Result<PostgresCommitSession> {
        let database_url = self.database_url.as_deref().ok_or_else(|| {
            CdfError::contract(
                "PostgresDestination::commit_package requires PostgresDestination::connect",
            )
        })?;
        let session_segments = expected_segments_for_session(
            &request.package_dir,
            &request.plan,
            commit_request.as_ref(),
        )?;
        Ok(PostgresCommitSession {
            database_url: database_url.to_owned(),
            package_dir: request.package_dir,
            plan: request.plan,
            client: None,
            phase: PostgresCommitSessionPhase::Begun,
            duplicate_receipt: None,
            receipt: None,
            expected_segments: session_segments.expected,
            expected_order: session_segments.order,
            accepted_segments: BTreeSet::new(),
            staged_segments: Vec::new(),
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
    package_dir: std::path::PathBuf,
    plan: PostgresLoadPlan,
    client: Option<Client>,
    phase: PostgresCommitSessionPhase,
    duplicate_receipt: Option<Receipt>,
    receipt: Option<Receipt>,
    expected_segments: BTreeMap<SegmentId, PostgresExpectedSegment>,
    expected_order: Vec<SegmentId>,
    accepted_segments: BTreeSet<SegmentId>,
    staged_segments: Vec<CommitSegment>,
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
    fn run_to_outcome(mut self) -> Result<PostgresCommitOutcome> {
        let segments = read_commit_segments_for_plan(&self.package_dir, &self.plan)?;
        self.apply_migrations()?;
        for segment in segments {
            self.write_segment(segment)?;
        }
        self.finalize_outcome()
    }

    fn finalize_outcome(mut self) -> Result<PostgresCommitOutcome> {
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

        let (recorded, record_error) =
            record_package_receipt_best_effort(&self.package_dir, &receipt);
        Ok(PostgresCommitOutcome {
            receipt,
            duplicate,
            plan: self.plan,
            package_receipt_recorded: recorded,
            package_receipt_error: record_error,
        })
    }

    fn rollback_open_transaction(&mut self) -> Result<()> {
        let Some(mut client) = self.client.take() else {
            return Ok(());
        };
        client
            .batch_execute("ROLLBACK")
            .map_err(|error| postgres_error("abort Postgres transaction", error))
    }

    fn write_accepted_segments(&mut self) -> Result<()> {
        if self.duplicate_receipt.is_some() {
            self.phase = PostgresCommitSessionPhase::Written;
            return Ok(());
        }

        let package =
            package_data_from_commit_segments(self.ordered_staged_segments()?, &self.plan)?;
        let mut client = self
            .client
            .take()
            .ok_or_else(|| CdfError::internal("Postgres commit session has no transaction"))?;
        let xid = query_xid(&mut client, &self.plan)?;
        let committed_at_ms = now_ms()?;
        let counts = if self.expected_segments.is_empty() {
            CommitCounts::default()
        } else {
            execute_statements(&mut client, &self.plan.target_ddl)?;
            apply_write_plan(&mut client, &self.plan, &package, committed_at_ms)?
        };
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
        insert_quarantine_mirror(&mut client, &self.package_dir, &self.plan, &receipt)?;
        verify_receipt_in_transaction(&mut client, &receipt)?;
        self.receipt = Some(receipt);
        self.client = Some(client);
        self.phase = PostgresCommitSessionPhase::Written;
        Ok(())
    }

    fn ordered_staged_segments(&self) -> Result<Vec<CommitSegment>> {
        let mut staged_by_id = BTreeMap::new();
        for segment in &self.staged_segments {
            staged_by_id.insert(segment.state.segment_id.clone(), segment);
        }
        let mut ordered = Vec::with_capacity(self.expected_order.len());
        for segment_id in &self.expected_order {
            let segment = staged_by_id.get(segment_id).ok_or_else(|| {
                CdfError::internal(format!(
                    "accepted Postgres segment {} is missing from staged payloads",
                    segment_id.as_str()
                ))
            })?;
            ordered.push((*segment).clone());
        }
        Ok(ordered)
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
        self.client = Some(client);
        self.phase = PostgresCommitSessionPhase::MigrationsApplied;
        if self.expected_segments.is_empty() {
            self.write_accepted_segments()?;
        }
        Ok(())
    }

    fn write_segment(&mut self, segment: CommitSegment) -> Result<SegmentAck> {
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

        let segment_id = segment.state.segment_id.clone();
        let expected = self.expected_segments.get(&segment_id).ok_or_else(|| {
            CdfError::data(format!(
                "Postgres commit segment {} is not in the planned package request",
                segment_id.as_str()
            ))
        })?;
        if self.accepted_segments.contains(&segment_id) {
            return Err(CdfError::data(format!(
                "Postgres commit session received duplicate segment {}",
                segment_id.as_str()
            )));
        }
        validate_commit_segment(&segment, expected, &self.plan)?;

        let ack = SegmentAck {
            segment_id: expected.state.segment_id.clone(),
            row_count: expected.state.row_count,
            byte_count: expected.state.byte_count,
        };
        self.accepted_segments.insert(segment_id);
        self.staged_segments.push(segment);
        if self.accepted_segments.len() == self.expected_segments.len() {
            self.write_accepted_segments()?;
        }
        Ok(ack)
    }

    fn finalize(self: Box<Self>) -> Result<Receipt> {
        Ok((*self).finalize_outcome()?.receipt)
    }

    fn abort(mut self: Box<Self>) -> Result<()> {
        self.rollback_open_transaction()
    }
}

impl CommitSession for ManagedPostgresCommitSession {
    fn apply_migrations(&mut self) -> Result<()> {
        self.with_inner(CommitSession::apply_migrations)
    }

    fn write_segment(&mut self, segment: CommitSegment) -> Result<SegmentAck> {
        self.with_inner(move |inner| CommitSession::write_segment(inner, segment))
    }

    fn finalize(mut self: Box<Self>) -> Result<Receipt> {
        let inner = self
            .inner
            .take()
            .ok_or_else(|| CdfError::internal("managed Postgres session lost its inner state"))?;
        self.execution.run_blocking("postgres.sync", move || {
            inner.finalize_outcome().map(|outcome| outcome.receipt)
        })
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
        validate_schema_matches_plan(schema.as_ref(), &plan.columns)?;
    }
    if row_count != expected.state.row_count {
        return Err(CdfError::data(format!(
            "Postgres commit segment {} has {} payload rows but request expects {}",
            segment.state.segment_id.as_str(),
            row_count,
            expected.state.row_count
        )));
    }
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

fn record_package_receipt_best_effort(
    package_dir: &std::path::Path,
    receipt: &Receipt,
) -> (bool, Option<String>) {
    match record_package_receipt_once(package_dir, receipt) {
        Ok(recorded) => (recorded, None),
        Err(error) => (false, Some(error.to_string())),
    }
}

fn query_xid(client: &mut Client, plan: &PostgresLoadPlan) -> Result<String> {
    client
        .query_one(&plan.xid_probe.sql, &[])
        .map(|row| row.get(0))
        .map_err(|error| postgres_error("query Postgres xid", error))
}

fn apply_write_plan(
    client: &mut Client,
    plan: &PostgresLoadPlan,
    package: &PostgresPackageData,
    loaded_at_ms: i64,
) -> Result<CommitCounts> {
    let mut rows_deleted = Some(0_u64);
    let mut rows_inserted = None;
    let mut rows_updated = Some(0_u64);
    let mut rows_written = 0_u64;

    for statement in &plan.write_sql {
        match statement.name.as_str() {
            "create_stage" => {
                client
                    .batch_execute(&statement.sql)
                    .map_err(|error| postgres_error("create Postgres stage table", error))?;
                copy_stage_rows(client, plan, package, loaded_at_ms)?;
            }
            "truncate_target_for_replace" => {
                rows_deleted = Some(count_target_rows(client, &plan.target)?);
                client
                    .batch_execute(&statement.sql)
                    .map_err(|error| postgres_error("truncate Postgres target", error))?;
            }
            "append_from_stage" | "replace_from_stage" => {
                let inserted = execute_count(client, statement)?;
                rows_inserted = Some(inserted);
                rows_written = inserted;
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

fn copy_stage_rows(
    client: &mut Client,
    plan: &PostgresLoadPlan,
    package: &PostgresPackageData,
    loaded_at_ms: i64,
) -> Result<u64> {
    let mut columns = quoted_column_names(&plan.columns);
    columns.extend(quoted_system_target_column_names());
    let copy_sql = format!(
        "COPY {} ({}) FROM STDIN WITH (FORMAT csv, NULL '\\N')",
        plan.stage_table.quoted(),
        columns.join(", ")
    );
    let mut writer = client
        .copy_in(&copy_sql)
        .map_err(|error| postgres_error("open Postgres COPY into stage", error))?;
    // Row provenance is the immutable original package identity. The package
    // token may differ from an operator-supplied idempotency token.
    let load = verify_parameter(plan, "package_hash")?;
    for row in &package.rows {
        writer
            .write_all(row.csv_line(&load, loaded_at_ms).as_bytes())
            .map_err(|error| io_error("write Postgres COPY row", error))?;
    }
    writer
        .finish()
        .map_err(|error| postgres_error("finish Postgres COPY into stage", error))
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
    let sql = match plan.dedup {
        MergeDedupPolicy::First | MergeDedupPolicy::Last => format!(
            "{}SELECT COUNT(*)::bigint FROM \"_cdf_dedup\"",
            merge_dedup_cte(plan)
        ),
        MergeDedupPolicy::Fail => {
            format!("SELECT COUNT(*)::bigint FROM {}", plan.stage_table.quoted())
        }
    };
    query_count(client, &sql, "count Postgres merge source rows")
}

fn count_merge_updates(client: &mut Client, plan: &PostgresLoadPlan) -> Result<u64> {
    let (cte, source) = match plan.dedup {
        MergeDedupPolicy::First | MergeDedupPolicy::Last => {
            (merge_dedup_cte(plan), "\"_cdf_dedup\"".to_owned())
        }
        MergeDedupPolicy::Fail => (String::new(), plan.stage_table.quoted()),
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

fn merge_dedup_cte(plan: &PostgresLoadPlan) -> String {
    let conflict_columns = plan
        .merge_keys
        .iter()
        .map(PostgresIdentifier::quoted)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "WITH \"_cdf_ranked\" AS (\n  SELECT {}, ROW_NUMBER() OVER (PARTITION BY {} ORDER BY {}, {}) AS \"_cdf_rank\"\n  FROM {}\n), \"_cdf_dedup\" AS (\n  SELECT * FROM \"_cdf_ranked\" WHERE \"_cdf_rank\" = 1\n)\n",
        stage_select_list(&plan.columns),
        conflict_columns,
        order_expression(CDF_SEGMENT_COLUMN, &plan.dedup),
        order_expression(CDF_ROW_COLUMN, &plan.dedup),
        plan.stage_table.quoted()
    )
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
    package_dir: &std::path::Path,
    plan: &PostgresLoadPlan,
    receipt: &Receipt,
) -> Result<()> {
    let records = cdf_package::PackageReader::open(package_dir)?.read_quarantine_records()?;
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

fn io_error(context: impl Into<String>, error: std::io::Error) -> CdfError {
    CdfError::destination(format!("{}: {}", context.into(), error))
}

fn json_error(error: serde_json::Error) -> CdfError {
    CdfError::data(error.to_string())
}
