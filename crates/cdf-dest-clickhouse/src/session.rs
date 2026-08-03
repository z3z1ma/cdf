use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, OnceLock},
};

use cdf_kernel::{
    CdfError, CommitCounts, CommitPlan, CommitSession, DestinationCommitRequest, Receipt, Result,
    SegmentAck, SegmentId, WriteDisposition,
};
use cdf_memory::{ConsumerKey, MemoryClass, ReservationRequest, reserve};
use clickhouse::Row;
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::{
    client::{
        ARROW_WRITER_BYTES, AuthorizedClickHouseClient, ClickHouseConnectionOptions,
        shared_authorized_client,
    },
    error::classify_clickhouse_error,
    identifier::{ClickHouseIdentifier, qualified, string_literal},
    mapping::{ClickHouseColumn, normalized_type, physical_columns},
    models::{
        ClickHouseCommitRequest, ClickHouseExpectedSegment, ClickHouseLoadPlan,
        ClickHouseMergeMode, TargetCapabilities,
    },
    package::{
        MAXIMUM_SEGMENTS_PER_PACKAGE, MAXIMUM_STATE_JSON_BYTES, add_package_hash, package_hash_hex,
        state_sha256, validate_commit_segment,
    },
    plan::{mirror_token, segment_token},
    receipt::{build_receipt, transaction_metadata},
};

const LOADS_TABLE: &str = "_cdf_loads";
const SEGMENTS_TABLE: &str = "_cdf_segments";
const STATE_TABLE: &str = "_cdf_state";
const PACKAGE_MARKER_PREFIX: &str = "cdf:package:";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionPhase {
    Begun,
    Prepared,
    Written,
}

pub(crate) struct ClickHouseCommitSession {
    connection: ClickHouseConnectionOptions,
    execution: cdf_runtime::ExecutionServices,
    client_cache: Arc<OnceLock<AuthorizedClickHouseClient>>,
    plan: ClickHouseLoadPlan,
    expected_segments: BTreeMap<SegmentId, ClickHouseExpectedSegment>,
    phase: SessionPhase,
    client: Option<AuthorizedClickHouseClient>,
    _writer_lease: Option<cdf_memory::MemoryLease>,
    capabilities: Option<TargetCapabilities>,
    existing_target_rows: u64,
    duplicate_receipt: Option<Receipt>,
    atomic_publication_visible: bool,
    atomic_stage_complete: bool,
    receipt: Option<Receipt>,
}

impl ClickHouseCommitSession {
    pub(crate) fn new(
        connection: ClickHouseConnectionOptions,
        execution: cdf_runtime::ExecutionServices,
        client_cache: Arc<OnceLock<AuthorizedClickHouseClient>>,
        request: ClickHouseCommitRequest,
    ) -> Self {
        Self {
            connection,
            execution,
            client_cache,
            plan: request.plan,
            expected_segments: request.segments.expected,
            phase: SessionPhase::Begun,
            client: None,
            _writer_lease: None,
            capabilities: None,
            existing_target_rows: 0,
            duplicate_receipt: None,
            atomic_publication_visible: false,
            atomic_stage_complete: false,
            receipt: None,
        }
    }

    fn marker(&self) -> String {
        format!("cdf:package:{}", self.plan.package_hash.as_str())
    }

    fn prepare(&mut self) -> Result<()> {
        if self.phase != SessionPhase::Begun {
            return Err(CdfError::destination(
                "ClickHouse migrations have already been applied",
            ));
        }
        self.execution.run_cancellation().check()?;
        let memory = self.execution.memory();
        let writer_request = ReservationRequest::new(
            ConsumerKey::new("clickhouse-arrowstream-writer", MemoryClass::Destination)?,
            ARROW_WRITER_BYTES,
        )?
        .as_minimum_working_set();
        let client_cache = self.client_cache.clone();
        let connection = self.connection.clone();
        let (client, writer) = self.execution.run_io(async move {
            let client = shared_authorized_client(client_cache, connection, memory.clone()).await?;
            let writer = reserve(memory, writer_request).await?;
            Ok((client, writer))
        })?;
        let plan = self.plan.clone();
        let database = self.connection.database.clone();
        let marker = self.marker();
        let (
            capabilities,
            existing_target_rows,
            duplicate_receipt,
            atomic_publication_visible,
            atomic_stage_complete,
        ) =
            self.execution.run_io({
                let client = client.clone();
                async move {
                    let capabilities = inspect_target(&client, &database, &plan.target).await?;
                    validate_target_capabilities(&plan, &capabilities)?;
                    validate_target_columns(&client, &database, &plan.target, &plan.columns)
                        .await?;
                    create_and_validate_mirrors(&client, &database, plan.segments.len()).await?;
                    let duplicate_receipt =
                        find_duplicate_receipt(&client, &database, &plan).await?;
                    let atomic_publication_visible = is_atomic_publication(&plan)
                        && marker_is_visible(&capabilities.table_comment, &marker);
                    if is_atomic_publication(&plan)
                        && duplicate_receipt.is_some()
                        && !atomic_publication_visible
                    {
                        return Err(CdfError::destination(
                            "ClickHouse atomic publication receipt exists but its immutable package marker is not visible on the target",
                        ));
                    }
                    if plan.kernel.disposition == WriteDisposition::Replace
                        && duplicate_receipt.is_none()
                        && !atomic_publication_visible
                    {
                        prepare_replace_stage(
                            &client,
                            &database,
                            &plan,
                            &publication_comment(&capabilities.table_comment, &marker),
                        )
                        .await?;
                    }
                    let existing_target_rows = if plan.kernel.disposition
                        == WriteDisposition::Replace
                        && duplicate_receipt.is_none()
                        && !atomic_publication_visible
                    {
                        query_count(
                            &client,
                            &format!(
                                "SELECT count() AS rows FROM {}",
                                qualified(&database, &plan.target)
                            ),
                            "count ClickHouse target rows for replacement",
                        )
                        .await?
                    } else {
                        0
                    };
                    let atomic_stage_complete = if is_atomic_merge(&plan)
                        && duplicate_receipt.is_none()
                        && !atomic_publication_visible
                    {
                        prepare_atomic_merge_stages(&client, &database, &plan, &marker).await?
                    } else {
                        false
                    };
                    Ok((
                        capabilities,
                        existing_target_rows,
                        duplicate_receipt,
                        atomic_publication_visible,
                        atomic_stage_complete,
                    ))
                }
            })?;
        self.client = Some(client);
        self._writer_lease = Some(writer);
        self.capabilities = Some(capabilities);
        self.existing_target_rows = existing_target_rows;
        self.duplicate_receipt = duplicate_receipt;
        self.atomic_publication_visible = atomic_publication_visible;
        self.atomic_stage_complete = atomic_stage_complete;
        self.phase = SessionPhase::Prepared;
        Ok(())
    }

    fn write(&mut self, segments: cdf_kernel::CommitSegmentIterator) -> Result<Vec<SegmentAck>> {
        if self.phase != SessionPhase::Prepared {
            return Err(CdfError::destination(
                "ClickHouse commit must apply migrations exactly once before writing",
            ));
        }
        self.execution.run_cancellation().check()?;
        let client = self
            .client
            .clone()
            .ok_or_else(|| CdfError::internal("ClickHouse session has no authorized client"))?;
        let capabilities = self
            .capabilities
            .clone()
            .ok_or_else(|| CdfError::internal("ClickHouse session has no target capabilities"))?;
        let existing_target_rows = self.existing_target_rows;
        let database = self.connection.database.clone();
        let plan = self.plan.clone();
        let expected = self.expected_segments.clone();
        let duplicate = self.duplicate_receipt.clone();
        let atomic_publication_visible = self.atomic_publication_visible;
        let atomic_stage_complete = self.atomic_stage_complete;
        let marker = self.marker();
        let cancellation = self.execution.run_cancellation();
        let committed_at_ms = i64::try_from(self.execution.unix_now().as_millis())
            .map_err(|_| CdfError::internal("ClickHouse commit time exceeds i64 milliseconds"))?;
        let (acknowledgements, receipt) = self.execution.run_io(async move {
            let mut accepted = BTreeSet::new();
            let mut acknowledgements = Vec::with_capacity(expected.len());
            for segment in segments {
                cancellation.check()?;
                let segment = segment?;
                let segment_id = segment.state.segment_id.clone();
                if !accepted.insert(segment_id.clone()) {
                    return Err(CdfError::data(format!(
                        "ClickHouse commit received duplicate segment {segment_id}"
                    )));
                }
                let expected_segment = expected.get(&segment_id).ok_or_else(|| {
                    CdfError::data(format!(
                        "ClickHouse commit received unplanned segment {segment_id}"
                    ))
                })?;
                validate_commit_segment(&segment, expected_segment, &plan)?;
                if duplicate.is_none() && !atomic_publication_visible {
                    let sql = insert_arrow_sql(
                        &database,
                        payload_table(&plan),
                        &physical_columns(&plan.columns)?,
                    );
                    let batches = segment
                        .batches
                        .into_iter()
                        .map(|batch| add_package_hash(batch, &plan.package_hash));
                    let written = client
                        .insert_arrow_batches(
                            &sql,
                            &segment_token(&plan, &segment_id),
                            batches,
                            "insert ClickHouse ArrowStream segment",
                        )
                        .await?;
                    if written != expected_segment.state.row_count {
                        return Err(CdfError::destination(format!(
                            "ClickHouse ArrowStream inserted {written} rows for segment {segment_id}, expected {}",
                            expected_segment.state.row_count
                        )));
                    }
                }
                acknowledgements.push(SegmentAck {
                    segment_id,
                    row_count: expected_segment.state.row_count,
                    byte_count: expected_segment.state.byte_count,
                });
            }
            require_complete_segments(&accepted, &expected)?;
            if let Some(receipt) = duplicate {
                validate_duplicate_receipt(&plan, &receipt)?;
                if receipt.segment_acks != acknowledgements {
                    return Err(CdfError::destination(
                        "ClickHouse duplicate receipt acknowledgements differ from the package",
                    ));
                }
                cleanup_publication_stages(&client, &database, &plan).await?;
                return Ok((acknowledgements, receipt));
            }
            let mut atomic_merge_counts = None;
            if plan.kernel.disposition == WriteDisposition::Replace && !atomic_publication_visible {
                exchange_replace_stage(&client, &database, &plan, &marker).await?;
            }
            if is_atomic_merge(&plan) {
                let complete_comment =
                    publication_comment(&capabilities.table_comment, &marker);
                atomic_merge_counts = Some(if atomic_publication_visible {
                    inspect_published_atomic_merge_counts(&client, &database, &plan).await?
                } else if atomic_stage_complete {
                    publish_completed_atomic_merge(&client, &database, &plan, &marker).await?
                } else {
                    publish_atomic_merge(
                        &client,
                        &database,
                        &plan,
                        &marker,
                        &complete_comment,
                    )
                    .await?
                });
            }
            let state_hash = state_sha256(plan.state_delta.as_ref())?;
            let replaced_rows = if plan.kernel.disposition == WriteDisposition::Replace
                && atomic_publication_visible
            {
                query_existing_stage_rows(&client, &database, &plan).await?
            } else {
                existing_target_rows
            };
            let counts = commit_counts(&plan, replaced_rows, atomic_merge_counts)?;
            let transaction = transaction_metadata(
                database.as_str(),
                &capabilities.table_engine,
                &capabilities.database_engine,
                &marker,
                &state_hash,
                false,
                plan.merge_mode,
            );
            let receipt = build_receipt(&plan, committed_at_ms, counts, transaction)?;
            futures_util::future::try_join(
                verify_package_rows(&client, &database, &plan.target, &plan),
                settle_evidence_mirrors(
                    &client,
                    &database,
                    &plan,
                    &expected,
                    &receipt,
                    &state_hash,
                ),
            )
            .await?;
            settle_load_mirror(&client, &database, &plan, &receipt, &state_hash).await?;
            cleanup_publication_stages(&client, &database, &plan).await?;
            // The generic finalized-package gate independently calls DestinationRuntime::verify_receipt
            // before checkpoint publication. Repeating that complete readback here would verify the
            // same immutable marker twice on every successful package.
            Ok((acknowledgements, receipt))
        })?;
        self.receipt = Some(receipt);
        self.phase = SessionPhase::Written;
        Ok(acknowledgements)
    }
}

impl CommitSession for ClickHouseCommitSession {
    fn apply_migrations(&mut self) -> Result<()> {
        self.prepare()
    }

    fn write_segments(
        &mut self,
        segments: cdf_kernel::CommitSegmentIterator,
    ) -> Result<Vec<SegmentAck>> {
        self.write(segments)
    }

    fn finalize(mut self: Box<Self>) -> Result<Receipt> {
        if self.phase != SessionPhase::Written {
            return Err(CdfError::destination(
                "cannot finalize ClickHouse commit before every segment settles",
            ));
        }
        self.receipt
            .take()
            .ok_or_else(|| CdfError::internal("ClickHouse session has no verified receipt"))
    }

    fn abort(self: Box<Self>) -> Result<()> {
        // ClickHouse payload requests are individually atomic and deterministically redriven.
        // There is no honest cross-request rollback to perform.
        Ok(())
    }
}

pub(crate) fn validate_session_begin_inputs(
    request: &DestinationCommitRequest,
    plan: &CommitPlan,
    load_plan: &ClickHouseLoadPlan,
) -> Result<()> {
    if plan != &load_plan.kernel
        || request.target != load_plan.kernel.target
        || request.disposition != load_plan.kernel.disposition
        || request.package_hash != load_plan.package_hash
        || request.idempotency_token != load_plan.idempotency_token
        || request.segments != load_plan.segments
    {
        return Err(CdfError::destination(
            "ClickHouse commit request does not match its prepared load plan",
        ));
    }
    Ok(())
}

pub(crate) fn verify_receipt(
    destination: &crate::models::ClickHouseDestination,
    receipt: &Receipt,
) -> Result<()> {
    if receipt.disposition == WriteDisposition::Merge {
        let receipt_mode = receipt
            .transaction
            .as_ref()
            .and_then(|transaction| transaction.values.get("merge_mode"))
            .ok_or_else(|| CdfError::destination("ClickHouse merge receipt has no merge mode"))?;
        if receipt_mode != destination.merge_mode.as_str() {
            return Err(CdfError::contract(format!(
                "ClickHouse receipt merge mode {receipt_mode} differs from resolved policy {}",
                destination.merge_mode.as_str()
            )));
        }
    }
    let connection = destination.connection.clone().ok_or_else(|| {
        CdfError::contract("ClickHouse receipt verification requires a resolved connection")
    })?;
    let execution = destination.execution.clone().ok_or_else(|| {
        CdfError::contract("ClickHouse receipt verification requires ExecutionServices")
    })?;
    let memory = execution.memory();
    let client_cache = destination.client.clone();
    let receipt = receipt.clone();
    execution.run_io(async move {
        let client = shared_authorized_client(client_cache, connection.clone(), memory).await?;
        verify_receipt_on_client(&client, &connection.database, &receipt).await
    })
}

fn validate_duplicate_receipt(plan: &ClickHouseLoadPlan, receipt: &Receipt) -> Result<()> {
    let receipt_mode = receipt
        .transaction
        .as_ref()
        .and_then(|transaction| transaction.values.get("merge_mode"))
        .map(String::as_str)
        .unwrap_or("replacing_merge_tree");
    if receipt.target != plan.kernel.target
        || receipt.package_hash != plan.package_hash
        || receipt.idempotency_token != plan.idempotency_token
        || receipt.disposition != plan.kernel.disposition
        || receipt.schema_hash != plan.schema_hash
        || receipt.migrations != plan.kernel.migrations
        || (receipt.disposition == WriteDisposition::Merge
            && receipt_mode != plan.merge_mode.as_str())
    {
        return Err(CdfError::destination(
            "ClickHouse duplicate receipt differs from the prepared package plan",
        ));
    }
    Ok(())
}

#[derive(Debug, Deserialize, Row)]
struct TargetRow {
    database_engine: String,
    table_engine: String,
    engine_full: String,
    create_table_query: String,
    table_comment: String,
    sorting_key: String,
    primary_key: String,
    partition_key: String,
    sampling_key: String,
    dependencies: u64,
}

#[derive(Debug, Deserialize, Row)]
struct TableCommentRow {
    table_comment: String,
}

#[derive(Debug, Deserialize, Row)]
struct CountRow {
    rows: u64,
}

#[derive(Debug, Deserialize, Row)]
struct PackageRowsRow {
    rows: u64,
    unique_ordinals: u64,
    minimum_ordinal: Option<u64>,
    maximum_ordinal: Option<u64>,
}

#[derive(Debug, Deserialize, Row)]
struct ColumnRow {
    name: String,
    r#type: String,
    default_kind: String,
}

#[derive(Debug, Deserialize, Row)]
struct JsonRow {
    receipt_json: String,
}

#[derive(Debug, Deserialize, Row)]
struct SegmentMirrorRow {
    segment_id: String,
    row_count: u64,
    byte_count: u64,
    ordinal_start: u64,
    ordinal_end: u64,
}

#[derive(Debug, Deserialize, Row)]
struct StateEvidenceRow {
    state_sha256: String,
    state_json: String,
}

#[derive(Debug, Deserialize, Row)]
struct LoadEvidenceRow {
    receipt_json: String,
    state_sha256: String,
}

async fn inspect_target(
    client: &AuthorizedClickHouseClient,
    database: &ClickHouseIdentifier,
    table: &ClickHouseIdentifier,
) -> Result<TargetCapabilities> {
    let row = client
        .query(
            "SELECT d.engine AS database_engine, t.engine AS table_engine, t.engine_full AS engine_full, t.create_table_query AS create_table_query, t.comment AS table_comment, t.sorting_key AS sorting_key, t.primary_key AS primary_key, t.partition_key AS partition_key, t.sampling_key AS sampling_key, length(t.dependencies_table) AS dependencies FROM system.tables AS t INNER JOIN system.databases AS d ON d.name = t.database WHERE t.database = ? AND t.name = ? LIMIT 2",
        )
        .bind(database.as_str())
        .bind(table.as_str())
        .fetch_all::<TargetRow>()
        .await
        .map_err(|error| classify_clickhouse_error("inspect ClickHouse target", error))?;
    if row.len() != 1 {
        return Err(CdfError::contract(format!(
            "ClickHouse target {}.{} must already exist exactly once",
            database, table
        )));
    }
    let row = row
        .into_iter()
        .next()
        .ok_or_else(|| CdfError::internal("ClickHouse target inspection lost its row"))?;
    Ok(TargetCapabilities {
        database_engine: row.database_engine,
        table_engine: row.table_engine,
        create_table_query: row.create_table_query,
        engine_full: row.engine_full,
        sorting_key: row.sorting_key,
        primary_key: row.primary_key,
        partition_key: row.partition_key,
        sampling_key: row.sampling_key,
        table_comment: row.table_comment,
        dependencies: row.dependencies,
    })
}

async fn inspect_optional_table_comment(
    client: &AuthorizedClickHouseClient,
    database: &ClickHouseIdentifier,
    table: &ClickHouseIdentifier,
) -> Result<Option<String>> {
    let rows = client
        .query(
            "SELECT comment AS table_comment FROM system.tables WHERE database = ? AND name = ? LIMIT 2",
        )
        .bind(database.as_str())
        .bind(table.as_str())
        .fetch_all::<TableCommentRow>()
        .await
        .map_err(|error| classify_clickhouse_error("inspect ClickHouse stage marker", error))?;
    if rows.len() > 1 {
        return Err(CdfError::internal(
            "ClickHouse system.tables returned duplicate stage metadata",
        ));
    }
    Ok(rows.into_iter().next().map(|row| row.table_comment))
}

fn validate_target_capabilities(
    plan: &ClickHouseLoadPlan,
    capabilities: &TargetCapabilities,
) -> Result<()> {
    match plan.kernel.disposition {
        WriteDisposition::Append
            if !matches!(
                capabilities.table_engine.as_str(),
                "MergeTree" | "ReplicatedMergeTree"
            ) =>
        {
            return Err(CdfError::contract(
                "ClickHouse append requires a row-preserving MergeTree or ReplicatedMergeTree target",
            ));
        }
        WriteDisposition::Replace if capabilities.table_engine != "MergeTree" => {
            return Err(CdfError::contract(
                "ClickHouse replace requires a row-preserving non-replicated MergeTree target",
            ));
        }
        WriteDisposition::Merge
            if is_atomic_merge(plan) && capabilities.table_engine != "MergeTree" =>
        {
            return Err(CdfError::contract(
                "ClickHouse atomic merge requires a row-preserving non-replicated MergeTree target",
            ));
        }
        _ => {}
    }
    if capabilities.dependencies != 0 {
        return Err(CdfError::contract(
            "ClickHouse target has dependent materialized views; their deduplication guarantee is not proven",
        ));
    }
    let replicated = capabilities.table_engine.starts_with("Replicated");
    let setting = if replicated {
        "replicated_deduplication_window"
    } else {
        "non_replicated_deduplication_window"
    };
    let required_window = u64::try_from(plan.segments.len())
        .map_err(|_| CdfError::contract("ClickHouse segment count exceeds u64"))?;
    if table_setting_u64(&capabilities.create_table_query, setting)
        .is_none_or(|window| window < required_window)
    {
        return Err(CdfError::contract(format!(
            "ClickHouse target requires explicit {setting} >= {required_window} for package-token replay"
        )));
    }
    if plan.kernel.disposition == WriteDisposition::Replace || is_atomic_merge(plan) {
        if capabilities.database_engine != "Atomic" {
            return Err(CdfError::contract(
                "ClickHouse atomic publication requires an Atomic database for EXCHANGE TABLES",
            ));
        }
        if replicated {
            return Err(CdfError::contract(
                "ClickHouse atomic publication is not advertised for replicated targets without a cluster-wide exchange proof",
            ));
        }
    }
    if is_native_merge(plan) {
        if !matches!(
            capabilities.table_engine.as_str(),
            "ReplacingMergeTree" | "ReplicatedReplacingMergeTree"
        ) || !is_unversioned_replacing_merge_tree(&capabilities.engine_full)
        {
            return Err(CdfError::contract(
                "ClickHouse native merge requires an existing unversioned ReplacingMergeTree or ReplicatedReplacingMergeTree target",
            ));
        }
        let sorting_key = simple_key_identifiers(&capabilities.sorting_key, "sorting")?;
        if sorting_key != plan.merge_keys {
            return Err(CdfError::contract(format!(
                "ClickHouse native merge sorting key {:?} must exactly equal merge keys {:?}",
                sorting_key, plan.merge_keys
            )));
        }
        let partition_key = simple_key_identifiers(&capabilities.partition_key, "partition")?;
        if partition_key
            .iter()
            .any(|key| !plan.merge_keys.contains(key))
        {
            return Err(CdfError::contract(
                "ClickHouse native merge partition keys must be a simple subset of merge keys",
            ));
        }
    }
    Ok(())
}

fn is_native_merge(plan: &ClickHouseLoadPlan) -> bool {
    plan.kernel.disposition == WriteDisposition::Merge
        && plan.merge_mode == ClickHouseMergeMode::ReplacingMergeTree
}

fn is_atomic_merge(plan: &ClickHouseLoadPlan) -> bool {
    plan.kernel.disposition == WriteDisposition::Merge
        && plan.merge_mode == ClickHouseMergeMode::AtomicCopyOnWrite
}

fn is_atomic_publication(plan: &ClickHouseLoadPlan) -> bool {
    plan.kernel.disposition == WriteDisposition::Replace || is_atomic_merge(plan)
}

fn marker_is_visible(comment: &str, marker: &str) -> bool {
    package_marker_suffix(comment).is_some_and(|visible| visible == marker)
}

fn publication_comment(existing: &str, marker: &str) -> String {
    let base = existing
        .rsplit_once(' ')
        .and_then(|(base, _)| package_marker_suffix(existing).map(|_| base))
        .or_else(|| {
            package_marker_suffix(existing)
                .is_none()
                .then_some(existing)
        })
        .unwrap_or_default();
    if base.is_empty() {
        marker.to_owned()
    } else {
        format!("{base} {marker}")
    }
}

fn package_marker_suffix(comment: &str) -> Option<&str> {
    let marker = comment
        .rsplit_once(' ')
        .map_or(comment, |(_, suffix)| suffix);
    let hash = marker.strip_prefix(PACKAGE_MARKER_PREFIX)?;
    let hex = hash.strip_prefix("sha256:").unwrap_or(hash);
    (hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit())).then_some(marker)
}

fn is_unversioned_replacing_merge_tree(engine_full: &str) -> bool {
    let engine = engine_full.trim_start();
    if let Some(remainder) = engine.strip_prefix("ReplicatedReplacingMergeTree") {
        return engine_argument_count(remainder) == Some(2);
    }
    engine
        .strip_prefix("ReplacingMergeTree")
        .is_some_and(|remainder| engine_argument_count(remainder) == Some(0))
}

fn engine_argument_count(remainder: &str) -> Option<usize> {
    let remainder = remainder.trim_start();
    if !remainder.starts_with('(') {
        return Some(0);
    }
    let mut quoted = false;
    let mut escaped = false;
    let mut depth = 0_usize;
    let mut commas = 0_usize;
    let mut has_argument = false;
    for character in remainder.chars() {
        if escaped {
            escaped = false;
            if depth == 1 && quoted {
                has_argument = true;
            }
            continue;
        }
        if character == '\\' && quoted {
            escaped = true;
            continue;
        }
        if character == '\'' {
            quoted = !quoted;
            if depth == 1 {
                has_argument = true;
            }
            continue;
        }
        if quoted {
            if depth == 1 && !character.is_whitespace() {
                has_argument = true;
            }
            continue;
        }
        match character {
            '(' => depth = depth.checked_add(1)?,
            ')' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(if has_argument { commas + 1 } else { 0 });
                }
            }
            ',' if depth == 1 => commas = commas.checked_add(1)?,
            character if depth == 1 && !character.is_whitespace() => has_argument = true,
            _ => {}
        }
    }
    None
}

fn simple_key_identifiers(value: &str, kind: &str) -> Result<Vec<ClickHouseIdentifier>> {
    let value = value.trim();
    if value.is_empty() || value == "tuple()" {
        return Ok(Vec::new());
    }
    value
        .split(',')
        .map(|part| {
            let part = part.trim();
            if part.is_empty()
                || part.contains(['(', ')', '\'', '"'])
                || (part.starts_with('`') != part.ends_with('`'))
            {
                return Err(CdfError::contract(format!(
                    "ClickHouse native merge {kind} key must contain only simple identifiers"
                )));
            }
            let part = part
                .strip_prefix('`')
                .and_then(|part| part.strip_suffix('`'))
                .unwrap_or(part)
                .replace("\\`", "`")
                .replace("\\\\", "\\");
            ClickHouseIdentifier::user(part)
        })
        .collect()
}

fn table_setting_u64(create: &str, setting: &str) -> Option<u64> {
    let settings = top_level_keyword(create, "SETTINGS")?;
    let settings = &create[settings + "SETTINGS".len()..];
    split_top_level(settings, ',')
        .into_iter()
        .find_map(|entry| {
            let (name, value) = entry.split_once('=')?;
            (name.trim() == setting)
                .then(|| {
                    value
                        .trim()
                        .split_ascii_whitespace()
                        .next()?
                        .parse::<u64>()
                        .ok()
                })
                .flatten()
        })
}

fn top_level_keyword(value: &str, keyword: &str) -> Option<usize> {
    let bytes = value.as_bytes();
    let mut depth = 0_usize;
    let mut quoted = false;
    let mut escaped = false;
    let mut index = 0_usize;
    while index + keyword.len() <= bytes.len() {
        let byte = bytes[index];
        if escaped {
            escaped = false;
            index += 1;
            continue;
        }
        if quoted && byte == b'\\' {
            escaped = true;
            index += 1;
            continue;
        }
        if byte == b'\'' {
            quoted = !quoted;
            index += 1;
            continue;
        }
        if !quoted {
            match byte {
                b'(' => depth += 1,
                b')' => depth = depth.saturating_sub(1),
                _ => {}
            }
            if depth == 0
                && bytes[index..].starts_with(keyword.as_bytes())
                && (index == 0 || !bytes[index - 1].is_ascii_alphanumeric())
                && (index + keyword.len() == bytes.len()
                    || !bytes[index + keyword.len()].is_ascii_alphanumeric())
            {
                return Some(index);
            }
        }
        index += 1;
    }
    None
}

fn split_top_level(text: &str, separator: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0_usize;
    let mut depth = 0_usize;
    let mut quoted = false;
    let mut escaped = false;
    for (index, character) in text.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if quoted && character == '\\' {
            escaped = true;
            continue;
        }
        if character == '\'' {
            quoted = !quoted;
            continue;
        }
        if !quoted {
            match character {
                '(' => depth += 1,
                ')' => depth = depth.saturating_sub(1),
                candidate if candidate == separator && depth == 0 => {
                    parts.push(&text[start..index]);
                    start = index + character.len_utf8();
                }
                _ => {}
            }
        }
    }
    parts.push(&text[start..]);
    parts
}

async fn validate_target_columns(
    client: &AuthorizedClickHouseClient,
    database: &ClickHouseIdentifier,
    table: &ClickHouseIdentifier,
    logical: &[ClickHouseColumn],
) -> Result<()> {
    let actual = client
        .query(
            "SELECT name, type, default_kind FROM system.columns WHERE database = ? AND table = ? ORDER BY position LIMIT 4097",
        )
        .bind(database.as_str())
        .bind(table.as_str())
        .fetch_all::<ColumnRow>()
        .await
        .map_err(|error| classify_clickhouse_error("inspect ClickHouse target columns", error))?;
    if actual.len() > 4_096 {
        return Err(CdfError::contract(
            "ClickHouse target exceeds the 4,096-column inspection ceiling",
        ));
    }
    let expected = physical_columns(logical)?;
    for column in &expected {
        let actual = actual
            .iter()
            .find(|candidate| candidate.name == column.name.as_str())
            .ok_or_else(|| {
                CdfError::contract(format!(
                    "ClickHouse target is missing required column {} {}",
                    column.name, column.clickhouse_type
                ))
            })?;
        if normalized_type(&actual.r#type) != normalized_type(&column.clickhouse_type) {
            return Err(CdfError::contract(format!(
                "ClickHouse target column {} has type {}, expected {}",
                column.name, actual.r#type, column.clickhouse_type
            )));
        }
    }
    let expected_names = expected
        .iter()
        .map(|column| column.name.as_str())
        .collect::<BTreeSet<_>>();
    if let Some(column) = actual.iter().find(|column| {
        !expected_names.contains(column.name.as_str()) && column.default_kind.is_empty()
    }) {
        return Err(CdfError::contract(format!(
            "ClickHouse target has unmapped required column {} without DEFAULT/MATERIALIZED/ALIAS authority",
            column.name
        )));
    }
    Ok(())
}

async fn create_and_validate_mirrors(
    client: &AuthorizedClickHouseClient,
    database: &ClickHouseIdentifier,
    required_dedup_window: usize,
) -> Result<()> {
    let loads = qualified(database, &ClickHouseIdentifier::framework(LOADS_TABLE)?);
    let segments = qualified(database, &ClickHouseIdentifier::framework(SEGMENTS_TABLE)?);
    let state = qualified(database, &ClickHouseIdentifier::framework(STATE_TABLE)?);
    client
        .execute(
            &format!(
                "CREATE TABLE IF NOT EXISTS {loads} (target String, package_hash String, idempotency_token String, receipt_id String, receipt_json String, state_sha256 FixedString(64), committed_at_ms Int64) ENGINE = MergeTree ORDER BY (target, package_hash) SETTINGS non_replicated_deduplication_window = 100000"
            ),
            "create ClickHouse load mirror",
        )
        .await?;
    client
        .execute(
            &format!(
                "CREATE TABLE IF NOT EXISTS {segments} (target String, package_hash String, idempotency_token String, segment_id String, row_count UInt64, byte_count UInt64, ordinal_start UInt64, ordinal_end UInt64, committed_at_ms Int64) ENGINE = MergeTree ORDER BY (target, package_hash, segment_id) SETTINGS non_replicated_deduplication_window = 100000"
            ),
            "create ClickHouse segment mirror",
        )
        .await?;
    client
        .execute(
            &format!(
                "CREATE TABLE IF NOT EXISTS {state} (receipt_id String, state_sha256 FixedString(64), state_json String, committed_at_ms Int64) ENGINE = MergeTree ORDER BY receipt_id SETTINGS non_replicated_deduplication_window = 100000"
            ),
            "create ClickHouse state mirror",
        )
        .await?;
    validate_mirror_table(
        client,
        database,
        LOADS_TABLE,
        &["target", "package_hash"],
        required_dedup_window,
        &[
            ("target", "String"),
            ("package_hash", "String"),
            ("idempotency_token", "String"),
            ("receipt_id", "String"),
            ("receipt_json", "String"),
            ("state_sha256", "FixedString(64)"),
            ("committed_at_ms", "Int64"),
        ],
    )
    .await?;
    validate_mirror_table(
        client,
        database,
        SEGMENTS_TABLE,
        &["target", "package_hash", "segment_id"],
        required_dedup_window,
        &[
            ("target", "String"),
            ("package_hash", "String"),
            ("idempotency_token", "String"),
            ("segment_id", "String"),
            ("row_count", "UInt64"),
            ("byte_count", "UInt64"),
            ("ordinal_start", "UInt64"),
            ("ordinal_end", "UInt64"),
            ("committed_at_ms", "Int64"),
        ],
    )
    .await?;
    validate_mirror_table(
        client,
        database,
        STATE_TABLE,
        &["receipt_id"],
        required_dedup_window,
        &[
            ("receipt_id", "String"),
            ("state_sha256", "FixedString(64)"),
            ("state_json", "String"),
            ("committed_at_ms", "Int64"),
        ],
    )
    .await
}

async fn validate_mirror_table(
    client: &AuthorizedClickHouseClient,
    database: &ClickHouseIdentifier,
    table: &str,
    expected_key: &[&str],
    required_dedup_window: usize,
    expected: &[(&str, &str)],
) -> Result<()> {
    let table = ClickHouseIdentifier::framework(table)?;
    let capabilities = inspect_target(client, database, &table).await?;
    let sorting_key = simple_key_identifiers(&capabilities.sorting_key, "mirror sorting")?;
    let primary_key = simple_key_identifiers(&capabilities.primary_key, "mirror primary")?;
    let expected_key = expected_key
        .iter()
        .map(|name| ClickHouseIdentifier::user(*name))
        .collect::<Result<Vec<_>>>()?;
    let required_dedup_window = u64::try_from(required_dedup_window)
        .map_err(|_| CdfError::contract("ClickHouse mirror segment count exceeds u64"))?;
    if capabilities.table_engine != "MergeTree"
        || sorting_key != expected_key
        || primary_key != expected_key
        || !capabilities.partition_key.is_empty()
        || !capabilities.sampling_key.is_empty()
        || capabilities.dependencies != 0
        || !capabilities.table_comment.is_empty()
        || table_setting_u64(
            &capabilities.create_table_query,
            "non_replicated_deduplication_window",
        )
        .is_none_or(|window| window < required_dedup_window)
        || top_level_keyword(&capabilities.create_table_query, "TTL").is_some()
    {
        return Err(CdfError::contract(format!(
            "ClickHouse settlement mirror {table} does not preserve the canonical CDF key, engine, or deduplication contract"
        )));
    }
    let actual = client
        .query(
            "SELECT name, type, default_kind FROM system.columns WHERE database = ? AND table = ? ORDER BY position LIMIT 4097",
        )
        .bind(database.as_str())
        .bind(table.as_str())
        .fetch_all::<ColumnRow>()
        .await
        .map_err(|error| classify_clickhouse_error("validate ClickHouse mirror schema", error))?;
    if actual.len() != expected.len()
        || actual.iter().zip(expected).any(|(actual, expected)| {
            actual.name != expected.0
                || normalized_type(&actual.r#type) != normalized_type(expected.1)
                || !actual.default_kind.is_empty()
        })
    {
        return Err(CdfError::contract(format!(
            "ClickHouse settlement mirror {table} does not have the exact CDF v1 schema"
        )));
    }
    Ok(())
}

async fn prepare_replace_stage(
    client: &AuthorizedClickHouseClient,
    database: &ClickHouseIdentifier,
    plan: &ClickHouseLoadPlan,
    complete_comment: &str,
) -> Result<()> {
    let stage = qualified(database, &plan.stage);
    let target = qualified(database, &plan.target);
    client
        .execute(
            &format!("DROP TABLE IF EXISTS {stage} SYNC"),
            "drop stale ClickHouse replace stage",
        )
        .await?;
    client
        .execute(
            &format!("CREATE TABLE {stage} AS {target}"),
            "create ClickHouse replace stage",
        )
        .await?;
    client
        .execute(
            &format!(
                "ALTER TABLE {stage} MODIFY COMMENT {}",
                string_literal(complete_comment)?
            ),
            "mark ClickHouse replace stage",
        )
        .await?;
    validate_structural_clone(client, database, &plan.target, &plan.stage).await
}

async fn prepare_atomic_merge_stages(
    client: &AuthorizedClickHouseClient,
    database: &ClickHouseIdentifier,
    plan: &ClickHouseLoadPlan,
    marker: &str,
) -> Result<bool> {
    let target = qualified(database, &plan.target);
    let incoming = qualified(database, &plan.incoming_stage);
    let publish = qualified(database, &plan.stage);
    let incoming_marker = format!("cdf:incoming:{}", plan.package_hash.as_str());
    let incoming_ready = inspect_optional_table_comment(client, database, &plan.incoming_stage)
        .await?
        .as_deref()
        == Some(incoming_marker.as_str());
    if !incoming_ready {
        for (table, action) in [
            (&incoming, "drop stale ClickHouse merge incoming stage"),
            (&publish, "drop stale ClickHouse merge publication stage"),
        ] {
            client
                .execute(&format!("DROP TABLE IF EXISTS {table} SYNC"), action)
                .await?;
        }
        client
            .execute(
                &format!("CREATE TABLE {incoming} AS {target}"),
                "create ClickHouse merge incoming stage",
            )
            .await?;
        client
            .execute(
                &format!(
                    "ALTER TABLE {incoming} MODIFY COMMENT {}",
                    string_literal(&incoming_marker)?
                ),
                "mark ClickHouse merge incoming stage",
            )
            .await?;
    }

    validate_target_columns(client, database, &plan.incoming_stage, &plan.columns).await?;
    validate_structural_clone(client, database, &plan.target, &plan.incoming_stage).await?;
    let publish_complete = incoming_ready
        && inspect_optional_table_comment(client, database, &plan.stage)
            .await?
            .as_deref()
            .is_some_and(|comment| marker_is_visible(comment, marker));
    if publish_complete {
        validate_target_columns(client, database, &plan.stage, &plan.columns).await?;
        validate_structural_clone(client, database, &plan.target, &plan.stage).await?;
        return Ok(true);
    }

    client
        .execute(
            &format!("DROP TABLE IF EXISTS {publish} SYNC"),
            "drop stale ClickHouse merge publication stage",
        )
        .await?;
    client
        .execute(
            &format!("CREATE TABLE {publish} AS {target}"),
            "create ClickHouse merge publication stage",
        )
        .await?;
    client
        .execute(
            &format!(
                "ALTER TABLE {publish} MODIFY COMMENT {}",
                string_literal(&format!("cdf:building:{}", plan.package_hash.as_str()))?
            ),
            "mark ClickHouse merge publication stage as building",
        )
        .await?;
    let publish_capabilities = inspect_target(client, database, &plan.stage).await?;
    validate_structural_clone(client, database, &plan.target, &plan.stage).await?;
    if marker_is_visible(&publish_capabilities.table_comment, marker) {
        return Err(CdfError::internal(
            "new ClickHouse merge publication stage unexpectedly carries a complete marker",
        ));
    }
    Ok(false)
}

async fn exchange_replace_stage(
    client: &AuthorizedClickHouseClient,
    database: &ClickHouseIdentifier,
    plan: &ClickHouseLoadPlan,
    marker: &str,
) -> Result<()> {
    let stage = qualified(database, &plan.stage);
    let target = qualified(database, &plan.target);
    validate_structural_clone(client, database, &plan.target, &plan.stage).await?;
    client
        .execute(
            &format!("EXCHANGE TABLES {target} AND {stage}"),
            "atomically exchange ClickHouse replacement",
        )
        .await?;
    let visible = inspect_target(client, database, &plan.target).await?;
    if !marker_is_visible(&visible.table_comment, marker) {
        return Err(CdfError::destination(
            "ClickHouse atomic exchange did not expose the expected immutable package marker",
        ));
    }
    Ok(())
}

async fn validate_structural_clone(
    client: &AuthorizedClickHouseClient,
    database: &ClickHouseIdentifier,
    target: &ClickHouseIdentifier,
    clone: &ClickHouseIdentifier,
) -> Result<()> {
    let target = inspect_target(client, database, target).await?;
    let clone = inspect_target(client, database, clone).await?;
    if normalized_table_structure(&target.create_table_query)?
        != normalized_table_structure(&clone.create_table_query)?
        || target.table_engine != clone.table_engine
        || target.engine_full != clone.engine_full
        || target.sorting_key != clone.sorting_key
        || target.primary_key != clone.primary_key
        || target.partition_key != clone.partition_key
        || target.sampling_key != clone.sampling_key
    {
        return Err(CdfError::contract(
            "ClickHouse publication stage does not preserve the target table structure",
        ));
    }
    Ok(())
}

fn normalized_table_structure(create: &str) -> Result<String> {
    let column_start = create.find('(').ok_or_else(|| {
        CdfError::contract("ClickHouse canonical CREATE TABLE query has no column list")
    })?;
    let structure = &create[column_start..];
    let Some(comment_start) = top_level_keyword(structure, "COMMENT") else {
        return Ok(structure.trim().to_owned());
    };
    let after_keyword = &structure[comment_start + "COMMENT".len()..];
    let quote_start = after_keyword.find('\'').ok_or_else(|| {
        CdfError::contract("ClickHouse table COMMENT clause has no string literal")
    })?;
    let mut escaped = false;
    let mut quote_end = None;
    for (index, character) in after_keyword[quote_start + 1..].char_indices() {
        if escaped {
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character == '\'' {
            quote_end = Some(quote_start + 1 + index + character.len_utf8());
            break;
        }
    }
    let quote_end = quote_end.ok_or_else(|| {
        CdfError::contract("ClickHouse table COMMENT string literal is unterminated")
    })?;
    let before = structure[..comment_start].trim_end();
    let after = after_keyword[quote_end..].trim_start();
    Ok(if after.is_empty() {
        before.to_owned()
    } else {
        format!("{before} {after}")
    })
}

async fn cleanup_publication_stages(
    client: &AuthorizedClickHouseClient,
    database: &ClickHouseIdentifier,
    plan: &ClickHouseLoadPlan,
) -> Result<()> {
    let stages = match plan.kernel.disposition {
        WriteDisposition::Replace => vec![&plan.stage],
        WriteDisposition::Merge if is_atomic_merge(plan) => {
            vec![&plan.stage, &plan.incoming_stage]
        }
        WriteDisposition::Append | WriteDisposition::Merge | WriteDisposition::CdcApply => {
            return Ok(());
        }
    };
    for stage in stages {
        client
            .execute(
                &format!("DROP TABLE IF EXISTS {} SYNC", qualified(database, stage)),
                "drop settled ClickHouse publication stage",
            )
            .await?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AtomicMergeCounts {
    incoming_rows: u64,
    updated_rows: u64,
}

async fn publish_atomic_merge(
    client: &AuthorizedClickHouseClient,
    database: &ClickHouseIdentifier,
    plan: &ClickHouseLoadPlan,
    marker: &str,
    complete_comment: &str,
) -> Result<AtomicMergeCounts> {
    let incoming = qualified(database, &plan.incoming_stage);
    let publish = qualified(database, &plan.stage);
    let target = logical_table_ref(client, database, &plan.target).await?;
    let keys = quoted_merge_keys(plan)?;
    validate_unique_keys(
        client,
        &incoming,
        &keys,
        "incoming ClickHouse merge package",
    )
    .await?;
    validate_unique_keys(client, &target, &keys, "existing ClickHouse merge target").await?;
    let incoming_rows = query_count(
        client,
        &format!("SELECT count() AS rows FROM {incoming}"),
        "count ClickHouse merge incoming rows",
    )
    .await?;
    let existing_rows = query_count(
        client,
        &format!("SELECT count() AS rows FROM {target}"),
        "count ClickHouse merge existing rows",
    )
    .await?;
    let updated_rows = query_count(
        client,
        &format!(
            "SELECT count() AS rows FROM {incoming} AS incoming INNER JOIN (SELECT {keys} FROM {target} GROUP BY {keys}) AS target USING ({keys})"
        ),
        "count ClickHouse merge updated keys",
    )
    .await?;
    client
        .query(&format!(
            "INSERT INTO {publish} SELECT target.* FROM {target} AS target LEFT ANTI JOIN {incoming} AS incoming USING ({keys})"
        ))
        .with_setting(
            "insert_deduplication_token",
            mirror_token(plan, "merge-copy-existing", plan.target.as_str()),
        )
        .execute()
        .await
        .map_err(|error| classify_clickhouse_error("copy unmatched ClickHouse merge rows", error))?;
    client
        .query(&format!("INSERT INTO {publish} SELECT * FROM {incoming}"))
        .with_setting(
            "insert_deduplication_token",
            mirror_token(plan, "merge-copy-incoming", plan.incoming_stage.as_str()),
        )
        .execute()
        .await
        .map_err(|error| classify_clickhouse_error("copy incoming ClickHouse merge rows", error))?;
    let expected_rows = existing_rows
        .checked_sub(updated_rows)
        .and_then(|rows| rows.checked_add(incoming_rows))
        .ok_or_else(|| CdfError::destination("ClickHouse atomic merge row counts overflowed"))?;
    let published_rows = query_count(
        client,
        &format!("SELECT count() AS rows FROM {publish}"),
        "count ClickHouse merged publication rows",
    )
    .await?;
    if published_rows != expected_rows {
        return Err(CdfError::destination(format!(
            "ClickHouse atomic merge built {published_rows} rows, expected {expected_rows}"
        )));
    }
    validate_unique_keys(client, &publish, &keys, "ClickHouse merged publication").await?;
    client
        .execute(
            &format!(
                "ALTER TABLE {publish} MODIFY COMMENT {}",
                string_literal(complete_comment)?
            ),
            "mark complete ClickHouse merge publication",
        )
        .await?;
    exchange_replace_stage(client, database, plan, marker).await?;
    Ok(AtomicMergeCounts {
        incoming_rows,
        updated_rows,
    })
}

async fn publish_completed_atomic_merge(
    client: &AuthorizedClickHouseClient,
    database: &ClickHouseIdentifier,
    plan: &ClickHouseLoadPlan,
    marker: &str,
) -> Result<AtomicMergeCounts> {
    let publish = qualified(database, &plan.stage);
    let counts = inspect_unpublished_atomic_merge_counts(client, database, plan).await?;
    let previous = logical_table_ref(client, database, &plan.target).await?;
    let existing_rows = query_count(
        client,
        &format!("SELECT count() AS rows FROM {previous}"),
        "count recovered ClickHouse merge target rows",
    )
    .await?;
    let expected_rows = existing_rows
        .checked_sub(counts.updated_rows)
        .and_then(|rows| rows.checked_add(counts.incoming_rows))
        .ok_or_else(|| CdfError::destination("ClickHouse atomic merge row counts overflowed"))?;
    let published_rows = query_count(
        client,
        &format!("SELECT count() AS rows FROM {publish}"),
        "count recovered ClickHouse merged publication rows",
    )
    .await?;
    if published_rows != expected_rows {
        return Err(CdfError::destination(format!(
            "recovered ClickHouse atomic merge contains {published_rows} rows, expected {expected_rows}"
        )));
    }
    let keys = quoted_merge_keys(plan)?;
    validate_unique_keys(
        client,
        &publish,
        &keys,
        "recovered ClickHouse merged publication",
    )
    .await?;
    let publication = inspect_target(client, database, &plan.stage).await?;
    if !marker_is_visible(&publication.table_comment, marker) {
        return Err(CdfError::destination(
            "ClickHouse completed merge stage lost its immutable package marker",
        ));
    }
    exchange_replace_stage(client, database, plan, marker).await?;
    Ok(counts)
}

async fn inspect_unpublished_atomic_merge_counts(
    client: &AuthorizedClickHouseClient,
    database: &ClickHouseIdentifier,
    plan: &ClickHouseLoadPlan,
) -> Result<AtomicMergeCounts> {
    let incoming = qualified(database, &plan.incoming_stage);
    let target = logical_table_ref(client, database, &plan.target).await?;
    let keys = quoted_merge_keys(plan)?;
    validate_unique_keys(
        client,
        &incoming,
        &keys,
        "recovered ClickHouse merge incoming stage",
    )
    .await?;
    validate_unique_keys(client, &target, &keys, "recovered ClickHouse merge target").await?;
    let incoming_rows = query_count(
        client,
        &format!("SELECT count() AS rows FROM {incoming}"),
        "count recovered ClickHouse merge incoming rows",
    )
    .await?;
    let updated_rows = query_count(
        client,
        &format!(
            "SELECT count() AS rows FROM {incoming} AS incoming INNER JOIN (SELECT {keys} FROM {target} GROUP BY {keys}) AS target USING ({keys})"
        ),
        "count recovered ClickHouse merge updated keys",
    )
    .await?;
    Ok(AtomicMergeCounts {
        incoming_rows,
        updated_rows,
    })
}

async fn inspect_published_atomic_merge_counts(
    client: &AuthorizedClickHouseClient,
    database: &ClickHouseIdentifier,
    plan: &ClickHouseLoadPlan,
) -> Result<AtomicMergeCounts> {
    let incoming = qualified(database, &plan.incoming_stage);
    let previous = logical_table_ref(client, database, &plan.stage).await?;
    let keys = quoted_merge_keys(plan)?;
    validate_unique_keys(
        client,
        &incoming,
        &keys,
        "recovered ClickHouse merge incoming stage",
    )
    .await?;
    validate_unique_keys(
        client,
        &previous,
        &keys,
        "recovered ClickHouse prior target",
    )
    .await?;
    let incoming_rows = query_count(
        client,
        &format!("SELECT count() AS rows FROM {incoming}"),
        "count recovered ClickHouse merge incoming rows",
    )
    .await?;
    let updated_rows = query_count(
        client,
        &format!(
            "SELECT count() AS rows FROM {incoming} AS incoming INNER JOIN (SELECT {keys} FROM {previous} GROUP BY {keys}) AS target USING ({keys})"
        ),
        "count recovered ClickHouse merge updated keys",
    )
    .await?;
    Ok(AtomicMergeCounts {
        incoming_rows,
        updated_rows,
    })
}

async fn logical_table_ref(
    client: &AuthorizedClickHouseClient,
    database: &ClickHouseIdentifier,
    table: &ClickHouseIdentifier,
) -> Result<String> {
    let capabilities = inspect_target(client, database, table).await?;
    let table = qualified(database, table);
    if matches!(
        capabilities.table_engine.as_str(),
        "ReplacingMergeTree" | "ReplicatedReplacingMergeTree"
    ) {
        Ok(format!("{table} FINAL"))
    } else {
        Ok(table)
    }
}

fn quoted_merge_keys(plan: &ClickHouseLoadPlan) -> Result<String> {
    if plan.merge_keys.is_empty() {
        return Err(CdfError::internal(
            "ClickHouse atomic merge reached execution without merge keys",
        ));
    }
    Ok(plan
        .merge_keys
        .iter()
        .map(ClickHouseIdentifier::quoted)
        .collect::<Vec<_>>()
        .join(", "))
}

async fn validate_unique_keys(
    client: &AuthorizedClickHouseClient,
    table: &str,
    keys: &str,
    label: &str,
) -> Result<()> {
    let duplicates = query_count(
        client,
        &format!(
            "SELECT count() AS rows FROM (SELECT 1 FROM {table} GROUP BY {keys} HAVING count() > 1 LIMIT 1)"
        ),
        "validate ClickHouse merge key uniqueness",
    )
    .await?;
    if duplicates != 0 {
        return Err(CdfError::destination(format!(
            "{label} contains duplicate merge keys"
        )));
    }
    Ok(())
}

fn payload_table(plan: &ClickHouseLoadPlan) -> &ClickHouseIdentifier {
    match plan.kernel.disposition {
        WriteDisposition::Replace => &plan.stage,
        WriteDisposition::Merge if is_atomic_merge(plan) => &plan.incoming_stage,
        WriteDisposition::Append | WriteDisposition::Merge => &plan.target,
        WriteDisposition::CdcApply => &plan.target,
    }
}

fn evidence_table(
    database: &ClickHouseIdentifier,
    table: &ClickHouseIdentifier,
    plan: &ClickHouseLoadPlan,
) -> String {
    let table = qualified(database, table);
    if is_native_merge(plan) && table == qualified(database, &plan.target) {
        format!("{table} FINAL")
    } else {
        table
    }
}

fn insert_arrow_sql(
    database: &ClickHouseIdentifier,
    table: &ClickHouseIdentifier,
    columns: &[ClickHouseColumn],
) -> String {
    format!(
        "INSERT INTO {} ({}) FORMAT ArrowStream",
        qualified(database, table),
        columns
            .iter()
            .map(|column| column.name.quoted())
            .collect::<Vec<_>>()
            .join(", ")
    )
}

async fn verify_package_rows(
    client: &AuthorizedClickHouseClient,
    database: &ClickHouseIdentifier,
    table: &ClickHouseIdentifier,
    plan: &ClickHouseLoadPlan,
) -> Result<()> {
    let expected = plan.segments.iter().try_fold(0_u64, |total, segment| {
        total
            .checked_add(segment.row_count)
            .ok_or_else(|| CdfError::data("ClickHouse package row count overflowed"))
    })?;
    let row = client
        .query(&format!(
            "SELECT count() AS rows, uniqExact(_cdf_package_row_ord) AS unique_ordinals, minOrNull(_cdf_package_row_ord) AS minimum_ordinal, maxOrNull(_cdf_package_row_ord) AS maximum_ordinal FROM {} WHERE hex(_cdf_package_hash) = ?",
            evidence_table(database, table, plan)
        ))
        .bind(package_hash_hex(&plan.package_hash)?)
        .fetch_one::<PackageRowsRow>()
        .await
        .map_err(|error| classify_clickhouse_error("verify ClickHouse package rows", error))?;
    let expected_maximum = expected.checked_sub(1);
    if row.rows != expected
        || row.unique_ordinals != expected
        || row.minimum_ordinal != (expected > 0).then_some(0)
        || row.maximum_ordinal != expected_maximum
    {
        return Err(CdfError::destination(format!(
            "ClickHouse target package provenance is not the exact canonical ordinal set for {}",
            plan.package_hash
        )));
    }
    if plan.kernel.disposition == WriteDisposition::Replace {
        let total = query_count(
            client,
            &format!("SELECT count() AS rows FROM {}", qualified(database, table)),
            "verify ClickHouse replacement row count",
        )
        .await?;
        if total != expected {
            return Err(CdfError::destination(
                "ClickHouse replacement target contains rows outside the exchanged package",
            ));
        }
    }
    Ok(())
}

fn require_complete_segments(
    accepted: &BTreeSet<SegmentId>,
    expected: &BTreeMap<SegmentId, ClickHouseExpectedSegment>,
) -> Result<()> {
    if accepted.len() == expected.len() {
        return Ok(());
    }
    let missing = expected
        .keys()
        .find(|segment| !accepted.contains(*segment))
        .ok_or_else(|| CdfError::internal("ClickHouse segment cardinality is inconsistent"))?;
    Err(CdfError::data(format!(
        "ClickHouse finalized package omitted segment {missing}"
    )))
}

fn commit_counts(
    plan: &ClickHouseLoadPlan,
    existing_rows: u64,
    atomic_merge: Option<AtomicMergeCounts>,
) -> Result<CommitCounts> {
    let rows = plan.segments.iter().try_fold(0_u64, |total, segment| {
        total
            .checked_add(segment.row_count)
            .ok_or_else(|| CdfError::data("ClickHouse commit row count overflowed"))
    })?;
    match plan.kernel.disposition {
        WriteDisposition::Append => Ok(CommitCounts {
            rows_written: rows,
            rows_inserted: Some(rows),
            rows_updated: Some(0),
            rows_deleted: Some(0),
        }),
        WriteDisposition::Replace => Ok(CommitCounts {
            rows_written: rows,
            rows_inserted: Some(rows),
            rows_updated: Some(0),
            rows_deleted: Some(existing_rows),
        }),
        WriteDisposition::Merge if is_native_merge(plan) => Ok(CommitCounts {
            rows_written: rows,
            rows_inserted: None,
            rows_updated: None,
            rows_deleted: Some(0),
        }),
        WriteDisposition::Merge => {
            let counts = atomic_merge.ok_or_else(|| {
                CdfError::internal("ClickHouse atomic merge omitted exact count evidence")
            })?;
            if counts.incoming_rows != rows {
                return Err(CdfError::internal(
                    "ClickHouse atomic merge count evidence differs from package rows",
                ));
            }
            Ok(CommitCounts {
                rows_written: rows,
                rows_inserted: Some(rows.saturating_sub(counts.updated_rows)),
                rows_updated: Some(counts.updated_rows),
                rows_deleted: Some(0),
            })
        }
        WriteDisposition::CdcApply => Err(CdfError::internal(
            "unsupported ClickHouse CDC disposition reached commit counts",
        )),
    }
}

async fn query_existing_stage_rows(
    client: &AuthorizedClickHouseClient,
    database: &ClickHouseIdentifier,
    plan: &ClickHouseLoadPlan,
) -> Result<u64> {
    let stage_exists = client
        .query("SELECT count() AS rows FROM system.tables WHERE database = ? AND name = ?")
        .bind(database.as_str())
        .bind(plan.stage.as_str())
        .fetch_one::<CountRow>()
        .await
        .map_err(|error| {
            classify_clickhouse_error("inspect exchanged ClickHouse replace stage", error)
        })?
        .rows;
    if stage_exists != 1 {
        return Err(CdfError::destination(
            "ClickHouse replacement marker is visible but the exchanged prior target is unavailable for exact delete counts",
        ));
    }
    query_count(
        client,
        &format!(
            "SELECT count() AS rows FROM {}",
            qualified(database, &plan.stage)
        ),
        "count exchanged ClickHouse prior target rows",
    )
    .await
}

async fn settle_evidence_mirrors(
    client: &AuthorizedClickHouseClient,
    database: &ClickHouseIdentifier,
    plan: &ClickHouseLoadPlan,
    expected: &BTreeMap<SegmentId, ClickHouseExpectedSegment>,
    receipt: &Receipt,
    state_hash: &str,
) -> Result<()> {
    let segments = qualified(database, &ClickHouseIdentifier::framework(SEGMENTS_TABLE)?);
    let state = qualified(database, &ClickHouseIdentifier::framework(STATE_TABLE)?);
    let state_json = serde_json::to_string(&plan.state_delta)
        .map_err(|error| CdfError::internal(format!("encode ClickHouse state mirror: {error}")))?;
    let settle_segments = async {
        for segment in &plan.segments {
            let expected_segment = expected.get(&segment.segment_id).ok_or_else(|| {
                CdfError::internal("ClickHouse settlement lost package segment authority")
            })?;
            let ordinal_start = expected_segment.package_row_ord_start;
            let ordinal_end = ordinal_start
                .checked_add(segment.row_count)
                .ok_or_else(|| CdfError::data("ClickHouse mirror ordinal range overflowed"))?;
            client
                .query(&format!(
                    "INSERT INTO {segments} (target, package_hash, idempotency_token, segment_id, row_count, byte_count, ordinal_start, ordinal_end, committed_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                ))
                .with_setting(
                    "insert_deduplication_token",
                    mirror_token(plan, "segment-mirror", segment.segment_id.as_str()),
                )
                .bind(plan.target.as_str())
                .bind(plan.package_hash.as_str())
                .bind(plan.idempotency_token.as_str())
                .bind(segment.segment_id.as_str())
                .bind(segment.row_count)
                .bind(segment.byte_count)
                .bind(ordinal_start)
                .bind(ordinal_end)
                .bind(receipt.committed_at_ms)
                .execute()
                .await
                .map_err(|error| {
                    classify_clickhouse_error("settle ClickHouse segment mirror", error)
                })?;
        }
        Ok::<(), CdfError>(())
    };
    let settle_state = async {
        client
            .query(&format!(
                "INSERT INTO {state} (receipt_id, state_sha256, state_json, committed_at_ms) VALUES (?, ?, ?, ?)"
            ))
            .with_setting(
                "insert_deduplication_token",
                mirror_token(plan, "state", receipt.receipt_id.as_str()),
            )
            .bind(receipt.receipt_id.as_str())
            .bind(state_hash)
            .bind(state_json)
            .bind(receipt.committed_at_ms)
            .execute()
            .await
            .map_err(|error| classify_clickhouse_error("settle ClickHouse state mirror", error))
    };
    futures_util::future::try_join(settle_segments, settle_state).await?;

    Ok(())
}

async fn settle_load_mirror(
    client: &AuthorizedClickHouseClient,
    database: &ClickHouseIdentifier,
    plan: &ClickHouseLoadPlan,
    receipt: &Receipt,
    state_hash: &str,
) -> Result<()> {
    // The receipt row is the settlement marker. Publishing it only after payload verification and
    // evidence acknowledgement makes its presence proof that the complete commit settled first.
    let receipt_json = serde_json::to_string(receipt).map_err(|error| {
        CdfError::internal(format!("encode ClickHouse receipt mirror: {error}"))
    })?;
    let loads = qualified(database, &ClickHouseIdentifier::framework(LOADS_TABLE)?);
    client
        .query(&format!(
            "INSERT INTO {loads} (target, package_hash, idempotency_token, receipt_id, receipt_json, state_sha256, committed_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?)"
        ))
        .with_setting(
            "insert_deduplication_token",
            mirror_token(plan, "load", receipt.receipt_id.as_str()),
        )
        .bind(plan.target.as_str())
        .bind(plan.package_hash.as_str())
        .bind(plan.idempotency_token.as_str())
        .bind(receipt.receipt_id.as_str())
        .bind(receipt_json)
        .bind(state_hash)
        .bind(receipt.committed_at_ms)
        .execute()
        .await
        .map_err(|error| classify_clickhouse_error("settle ClickHouse load mirror", error))
}

async fn find_duplicate_receipt(
    client: &AuthorizedClickHouseClient,
    database: &ClickHouseIdentifier,
    plan: &ClickHouseLoadPlan,
) -> Result<Option<Receipt>> {
    let loads = qualified(database, &ClickHouseIdentifier::framework(LOADS_TABLE)?);
    let rows = client
        .query(&format!(
            "SELECT receipt_json FROM {loads} WHERE target = ? AND package_hash = ? AND idempotency_token = ? LIMIT 2"
        ))
        .bind(plan.target.as_str())
        .bind(plan.package_hash.as_str())
        .bind(plan.idempotency_token.as_str())
        .fetch_all::<JsonRow>()
        .await
        .map_err(|error| classify_clickhouse_error("read ClickHouse duplicate receipt", error))?;
    if rows.len() > 1 {
        return Err(CdfError::destination(
            "ClickHouse load mirror contains duplicate settlement rows",
        ));
    }
    rows.into_iter()
        .next()
        .map(|row| {
            serde_json::from_str(&row.receipt_json).map_err(|error| {
                CdfError::destination(format!("decode ClickHouse mirrored receipt: {error}"))
            })
        })
        .transpose()
}

async fn verify_receipt_on_client(
    client: &AuthorizedClickHouseClient,
    database: &ClickHouseIdentifier,
    receipt: &Receipt,
) -> Result<()> {
    let transaction = receipt
        .transaction
        .as_ref()
        .ok_or_else(|| CdfError::destination("ClickHouse receipt has no transaction metadata"))?;
    let state_hash = transaction
        .values
        .get("state_sha256")
        .ok_or_else(|| CdfError::destination("ClickHouse receipt has no state hash"))?;
    let merge_mode = transaction
        .values
        .get("merge_mode")
        .map(String::as_str)
        .unwrap_or("replacing_merge_tree");
    let native_merge =
        receipt.disposition == WriteDisposition::Merge && merge_mode == "replacing_merge_tree";
    let loads = qualified(database, &ClickHouseIdentifier::framework(LOADS_TABLE)?);
    let segments = qualified(database, &ClickHouseIdentifier::framework(SEGMENTS_TABLE)?);
    let state = qualified(database, &ClickHouseIdentifier::framework(STATE_TABLE)?);
    let target = ClickHouseIdentifier::user(receipt.target.as_str())?;
    let target_table = qualified(database, &target);
    let package_hash = package_hash_hex(&receipt.package_hash)?;
    let load_verification = async {
        client
            .query(&format!(
                "SELECT receipt_json, toString(state_sha256) AS state_sha256 FROM {loads} WHERE target = ? AND package_hash = ? AND idempotency_token = ? LIMIT 2"
            ))
            .bind(receipt.target.as_str())
            .bind(receipt.package_hash.as_str())
            .bind(receipt.idempotency_token.as_str())
            .fetch_all::<LoadEvidenceRow>()
            .await
            .map_err(|error| classify_clickhouse_error("verify ClickHouse load mirror", error))
    };
    let segment_verification = async {
        client
            .query(&format!(
                "SELECT segment_id, row_count, byte_count, ordinal_start, ordinal_end FROM {segments} WHERE target = ? AND package_hash = ? AND idempotency_token = ? ORDER BY segment_id LIMIT 100001"
            ))
            .bind(receipt.target.as_str())
            .bind(receipt.package_hash.as_str())
            .bind(receipt.idempotency_token.as_str())
            .fetch_all::<SegmentMirrorRow>()
            .await
            .map_err(|error| classify_clickhouse_error("verify ClickHouse segment mirror", error))
    };
    let state_verification = async {
        client
            .query(&format!(
                "SELECT toString(state_sha256) AS state_sha256, state_json FROM {state} WHERE receipt_id = ? LIMIT 2"
            ))
            .bind(receipt.receipt_id.as_str())
            .fetch_all::<StateEvidenceRow>()
            .await
            .map_err(|error| classify_clickhouse_error("verify ClickHouse state mirror", error))
    };
    let target_verification = async {
        if native_merge || is_atomic_receipt(receipt, merge_mode) {
            return Ok(None);
        }
        client
            .query(&format!(
                "SELECT count() AS rows FROM {target_table} WHERE hex(_cdf_package_hash) = ?"
            ))
            .bind(package_hash)
            .fetch_one::<CountRow>()
            .await
            .map(|row| Some(row.rows))
            .map_err(|error| classify_clickhouse_error("verify ClickHouse receipt target", error))
    };
    let (rows, mirrored, state_rows, package_rows) = futures_util::future::try_join4(
        load_verification,
        segment_verification,
        state_verification,
        target_verification,
    )
    .await?;
    if rows.len() != 1 {
        return Err(CdfError::destination(
            "ClickHouse receipt does not have exactly one load mirror row",
        ));
    }
    let stored: Receipt = serde_json::from_str(&rows[0].receipt_json).map_err(|error| {
        CdfError::destination(format!("decode ClickHouse verification receipt: {error}"))
    })?;
    if &stored != receipt {
        return Err(CdfError::destination(
            "ClickHouse load mirror differs from the expected receipt",
        ));
    }
    if mirrored.len() != receipt.segment_acks.len() || mirrored.len() > MAXIMUM_SEGMENTS_PER_PACKAGE
    {
        return Err(CdfError::destination(
            "ClickHouse segment mirror count differs from receipt acknowledgements",
        ));
    }
    let mirrored = mirrored
        .into_iter()
        .map(|segment| (segment.segment_id.clone(), segment))
        .collect::<BTreeMap<_, _>>();
    if mirrored.len() != receipt.segment_acks.len() {
        return Err(CdfError::destination(
            "ClickHouse segment mirror contains duplicate segment identifiers",
        ));
    }
    let mut ordinal_start = 0_u64;
    for acknowledgement in &receipt.segment_acks {
        let segment = mirrored
            .get(acknowledgement.segment_id.as_str())
            .ok_or_else(|| {
                CdfError::destination(format!(
                    "ClickHouse segment mirror omits receipt segment {}",
                    acknowledgement.segment_id
                ))
            })?;
        let ordinal_end = ordinal_start
            .checked_add(acknowledgement.row_count)
            .ok_or_else(|| CdfError::destination("ClickHouse receipt ordinal range overflowed"))?;
        if segment.row_count != acknowledgement.row_count
            || segment.byte_count != acknowledgement.byte_count
            || segment.ordinal_start != ordinal_start
            || segment.ordinal_end != ordinal_end
        {
            return Err(CdfError::destination(format!(
                "ClickHouse segment mirror evidence differs for receipt segment {}",
                acknowledgement.segment_id
            )));
        }
        ordinal_start = ordinal_end;
    }
    if ordinal_start != receipt.counts.rows_written {
        return Err(CdfError::destination(
            "ClickHouse segment mirror ranges do not cover the receipt row count",
        ));
    }
    if rows[0].state_sha256 != *state_hash
        || state_rows.len() != 1
        || state_rows[0].state_sha256 != *state_hash
    {
        return Err(CdfError::destination(
            "ClickHouse state mirror differs from receipt evidence",
        ));
    }
    let state_json = state_rows[0].state_json.as_bytes();
    if state_json.len() > MAXIMUM_STATE_JSON_BYTES
        || format!("{:x}", Sha256::digest(state_json)) != *state_hash
    {
        return Err(CdfError::destination(
            "ClickHouse state mirror JSON does not match its receipt-bound hash",
        ));
    }
    let state_delta: Option<cdf_kernel::StateDelta> =
        serde_json::from_slice(state_json).map_err(|error| {
            CdfError::destination(format!("decode ClickHouse mirrored state: {error}"))
        })?;
    if let Some(state_delta) = &state_delta {
        state_delta.validate().map_err(|error| {
            CdfError::destination(format!(
                "validate ClickHouse mirrored state authority: {error}"
            ))
        })?;
        if state_delta.package_hash != receipt.package_hash
            || state_delta.schema_hash != receipt.schema_hash
        {
            return Err(CdfError::destination(
                "ClickHouse mirrored state authority differs from its receipt",
            ));
        }
    }
    if native_merge {
        // Native merge receipts describe an immutable completed write. A later package may
        // legitimately supersede these keys, so historical verification is mirror-backed rather
        // than a false claim that the old values remain current.
        return Ok(());
    }
    if !is_atomic_receipt(receipt, merge_mode) && package_rows != Some(receipt.counts.rows_written)
    {
        return Err(CdfError::destination(
            "ClickHouse target package rows differ from receipt counts",
        ));
    }
    if is_atomic_receipt(receipt, merge_mode) {
        let target_capabilities = inspect_target(client, database, &target).await?;
        let marker = transaction
            .values
            .get("replace_marker")
            .ok_or_else(|| CdfError::destination("ClickHouse receipt has no replace marker"))?;
        if marker_is_visible(&target_capabilities.table_comment, marker) {
            let package_rows = query_count(
                client,
                &format!(
                    "SELECT count() AS rows FROM {target_table} WHERE hex(_cdf_package_hash) = '{}'",
                    package_hash_hex(&receipt.package_hash)?
                ),
                "verify ClickHouse atomic publication package rows",
            )
            .await?;
            let row_count_matches = if receipt.disposition == WriteDisposition::Replace {
                query_count(
                    client,
                    &format!("SELECT count() AS rows FROM {target_table}"),
                    "verify ClickHouse replacement target rows",
                )
                .await?
                    == receipt.counts.rows_written
            } else {
                true
            };
            if package_rows != receipt.counts.rows_written || !row_count_matches {
                return Err(CdfError::destination(
                    "ClickHouse atomic publication target rows differ from its receipt",
                ));
            }
        } else {
            verify_later_atomic_publication(
                client,
                database,
                receipt,
                &target_capabilities.table_comment,
            )
            .await?;
        }
    }
    Ok(())
}

fn is_atomic_receipt(receipt: &Receipt, merge_mode: &str) -> bool {
    receipt.disposition == WriteDisposition::Replace
        || (receipt.disposition == WriteDisposition::Merge && merge_mode == "atomic_copy_on_write")
}

async fn verify_later_atomic_publication(
    client: &AuthorizedClickHouseClient,
    database: &ClickHouseIdentifier,
    receipt: &Receipt,
    current_comment: &str,
) -> Result<()> {
    let current_marker = package_marker_suffix(current_comment).ok_or_else(|| {
        CdfError::destination(
            "ClickHouse atomic publication was superseded without a valid CDF package marker",
        )
    })?;
    let current_hash = current_marker
        .strip_prefix(PACKAGE_MARKER_PREFIX)
        .ok_or_else(|| CdfError::internal("validated ClickHouse package marker lost its prefix"))?;
    if current_hash == receipt.package_hash.as_str() {
        return Err(CdfError::destination(
            "ClickHouse atomic publication marker differs only in malformed receipt metadata",
        ));
    }
    let loads = qualified(database, &ClickHouseIdentifier::framework(LOADS_TABLE)?);
    let rows = client
        .query(&format!(
            "SELECT receipt_json FROM {loads} WHERE target = ? AND package_hash = ? AND idempotency_token = ? LIMIT 2"
        ))
        .bind(receipt.target.as_str())
        .bind(current_hash)
        .bind(current_hash)
        .fetch_all::<JsonRow>()
        .await
        .map_err(|error| {
            classify_clickhouse_error("verify superseding ClickHouse publication", error)
        })?;
    if rows.len() != 1 {
        return Err(CdfError::destination(
            "ClickHouse superseding publication has no unique settled receipt",
        ));
    }
    let current: Receipt = serde_json::from_str(&rows[0].receipt_json).map_err(|error| {
        CdfError::destination(format!("decode superseding ClickHouse receipt: {error}"))
    })?;
    let current_transaction = current.transaction.as_ref().ok_or_else(|| {
        CdfError::destination("superseding ClickHouse receipt has no transaction metadata")
    })?;
    let current_mode = current_transaction
        .values
        .get("merge_mode")
        .map(String::as_str)
        .unwrap_or("replacing_merge_tree");
    if current.target != receipt.target
        || current.package_hash.as_str() != current_hash
        || current.idempotency_token.as_str() != current_hash
        || !is_atomic_receipt(&current, current_mode)
        || current_transaction
            .values
            .get("replace_marker")
            .is_none_or(|marker| marker != current_marker)
    {
        return Err(CdfError::destination(
            "ClickHouse superseding publication receipt does not authorize the visible marker",
        ));
    }
    Ok(())
}

async fn query_count(client: &AuthorizedClickHouseClient, sql: &str, action: &str) -> Result<u64> {
    client
        .query(sql)
        .fetch_one::<CountRow>()
        .await
        .map(|row| row.rows)
        .map_err(|error| classify_clickhouse_error(action, error))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replacing_merge_tree_version_detection_is_fail_closed() {
        assert!(is_unversioned_replacing_merge_tree(
            "ReplacingMergeTree ORDER BY id"
        ));
        assert!(is_unversioned_replacing_merge_tree(
            "ReplacingMergeTree() ORDER BY id"
        ));
        assert!(!is_unversioned_replacing_merge_tree(
            "ReplacingMergeTree(version) ORDER BY id"
        ));
        assert!(is_unversioned_replacing_merge_tree(
            "ReplicatedReplacingMergeTree('/clickhouse/{shard}/table', '{replica}') ORDER BY id"
        ));
        assert!(!is_unversioned_replacing_merge_tree(
            "ReplicatedReplacingMergeTree('/clickhouse/{shard}/table', '{replica}', version) ORDER BY id"
        ));
    }

    #[test]
    fn simple_key_parser_rejects_expression_authority() {
        assert_eq!(
            simple_key_identifiers("id, `region`", "sorting").unwrap(),
            [
                ClickHouseIdentifier::user("id").unwrap(),
                ClickHouseIdentifier::user("region").unwrap(),
            ]
        );
        assert!(simple_key_identifiers("toYYYYMM(ts)", "partition").is_err());
        assert!(simple_key_identifiers("tuple(id, region)", "sorting").is_err());
        assert!(
            simple_key_identifiers("tuple()", "partition")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn publication_markers_preserve_the_operator_comment_and_replace_prior_markers() {
        let first_marker = format!("cdf:package:{}", "11".repeat(32));
        let second_marker = format!("cdf:package:sha256:{}", "22".repeat(32));
        let first = publication_comment("operator comment", &first_marker);
        assert_eq!(first, format!("operator comment {first_marker}"));
        assert!(marker_is_visible(&first, &first_marker));
        let second = publication_comment(&first, &second_marker);
        assert_eq!(second, format!("operator comment {second_marker}"));
        assert!(!marker_is_visible(&second, &first_marker));
        assert!(marker_is_visible(&second, &second_marker));
        assert_eq!(
            publication_comment("operator cdf:package:not-a-hash", &first_marker),
            format!("operator cdf:package:not-a-hash {first_marker}")
        );
    }

    #[test]
    fn table_settings_and_structure_are_parsed_fail_closed() {
        let ddl = "CREATE TABLE db.events (id UInt64, note String DEFAULT 'SETTINGS fake = 9') ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192, non_replicated_deduplication_window = 100000 COMMENT 'operator note'";
        assert_eq!(
            table_setting_u64(ddl, "non_replicated_deduplication_window"),
            Some(100_000)
        );
        assert_eq!(
            table_setting_u64(ddl, "replicated_deduplication_window"),
            None
        );
        assert_eq!(
            normalized_table_structure(ddl).unwrap(),
            "(id UInt64, note String DEFAULT 'SETTINGS fake = 9') ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192, non_replicated_deduplication_window = 100000"
        );
    }
}
