use std::{
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use cdf_kernel::{CdfError, ScopeKey, TrustLevel};
use cdf_package::PackageReader;
use cdf_state_sqlite::{
    SqliteCheckpointStore, SqliteErrorContext, SqliteRunLedger, classify_sqlite_error,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, Row, params};
use serde::Serialize;

use crate::{context::ProjectCompilationContext, error_catalog, output::CliError};

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct StatusReport {
    pub freshness_resources: Vec<StatusResource>,
    pub summary: StatusSummary,
}

impl StatusReport {
    pub(crate) fn exit_code(&self) -> i32 {
        if self.summary.stale > 0 {
            1
        } else if self.summary.non_evaluable > 0 {
            78
        } else {
            0
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct StatusSummary {
    pub total: usize,
    pub fresh: usize,
    pub stale: usize,
    pub non_evaluable: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct StatusResource {
    pub resource_id: String,
    pub trust_level: String,
    pub state_scope: serde_json::Value,
    pub max_age_ms: u64,
    pub freshness_state: FreshnessState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<ObservedCheckpoint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub non_evaluable_reason: Option<NonEvaluableReason>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matching_committed_heads: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receipt_freshness: Option<ReceiptFreshnessObservation>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ObservedCheckpoint {
    pub checkpoint_id: String,
    pub pipeline_id: String,
    pub package_hash: String,
    pub schema_hash: String,
    pub receipt_id: String,
    pub committed_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FreshnessState {
    Fresh,
    Stale,
    NonEvaluable,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum NonEvaluableReason {
    StateDatabaseMissing,
    CheckpointTableMissing,
    RunLedgerMissing,
    CommittedHeadMissing,
    AmbiguousCommittedHeads,
    ReceiptMissing,
    ReceiptCorrupt,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ReceiptFreshnessObservation {
    pub state: ReceiptFreshnessState,
    pub source: ReceiptFreshnessSource,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receipt_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub package_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_ledger_recorded_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub package_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub package_receipt_committed_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReceiptFreshnessState {
    MissingRunLedger,
    MissingReceipt,
    FreshReceipt,
    StaleReceipt,
    CorruptReceipt,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReceiptFreshnessSource {
    CheckpointCommittedHead,
    RunLedger,
    RunLedgerReceipt,
    PackageReceipt,
}

pub(crate) fn evaluate(context: &ProjectCompilationContext) -> Result<StatusReport, CliError> {
    let resources = context
        .compilation
        .lock
        .as_ref()
        .into_iter()
        .flat_map(|lock| lock.resources.values())
        .filter_map(|resource| {
            let descriptor = &resource.descriptor;
            let freshness = descriptor.freshness.as_ref()?;
            if descriptor.trust_level != TrustLevel::Serving {
                return None;
            }
            Some(ServingFreshnessResource {
                resource_id: descriptor.resource_id.to_string(),
                trust_level: trust_level_name(&descriptor.trust_level).to_owned(),
                state_scope: descriptor.state_scope.clone(),
                max_age_ms: freshness.max_age_ms,
            })
        })
        .collect::<Vec<_>>();

    if resources.is_empty() {
        return Ok(StatusReport {
            freshness_resources: Vec::new(),
            summary: StatusSummary {
                total: 0,
                fresh: 0,
                stale: 0,
                non_evaluable: 0,
            },
        });
    }

    let state_path = context.state_store_path()?;
    let now_ms = now_ms()?;
    let ledger = LocalLedger::open(&state_path, context.state_store_path_ownership())?;
    let freshness_resources = resources
        .into_iter()
        .map(|resource| ledger.evaluate_resource(resource, &context.root, now_ms))
        .collect::<Result<Vec<_>, _>>()?;
    let summary = summarize(&freshness_resources);

    Ok(StatusReport {
        freshness_resources,
        summary,
    })
}

pub(crate) fn human_summary(report: &StatusReport) -> String {
    if report.summary.total == 0 {
        return "no freshness SLO resources to evaluate".to_owned();
    }
    if report.summary.stale > 0 {
        return format!(
            "freshness SLO breach: {} stale, {} fresh, {} non-evaluable",
            report.summary.stale, report.summary.fresh, report.summary.non_evaluable
        );
    }
    if report.summary.non_evaluable > 0 {
        return format!(
            "freshness SLO status non-evaluable: {} resource(s), {} fresh",
            report.summary.non_evaluable, report.summary.fresh
        );
    }
    format!(
        "freshness SLO status fresh: {} resource(s)",
        report.summary.fresh
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ServingFreshnessResource {
    resource_id: String,
    trust_level: String,
    state_scope: ScopeKey,
    max_age_ms: u64,
}

enum LocalLedger {
    MissingDatabase,
    MissingCheckpointTable,
    Checkpoints(Connection),
}

impl LocalLedger {
    fn open(
        path: &Path,
        ownership: cdf_state_sqlite::StateStorePathOwnership,
    ) -> Result<Self, CliError> {
        if !freshness_state_database_exists(path, ownership)? {
            return Ok(Self::MissingDatabase);
        }
        let open_path = cdf_state_sqlite::database_open_path(path, ownership)?;
        let conn = Connection::open_with_flags(
            open_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )
        .map_err(sqlite_cli_error)?;
        let has_checkpoints = conn
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'cdf_checkpoints'",
                [],
                |_| Ok(()),
            )
            .optional()
            .map_err(sqlite_cli_error)?
            .is_some();
        let checkpoint_footprint =
            has_checkpoints || component_marker_exists(&conn, "checkpoint_store")?;
        if checkpoint_footprint {
            SqliteCheckpointStore::open_read_only_with_path_ownership(path, ownership)
                .and_then(|store| store.validate_integrity())
                .map_err(freshness_store_error)?;
        }
        let run_footprint = table_exists(&conn, "cdf_runs")?
            || table_exists(&conn, "cdf_run_events")?
            || component_marker_exists(&conn, "run_ledger")?;
        if run_footprint {
            SqliteRunLedger::open_read_only_with_path_ownership(path, ownership)
                .and_then(|ledger| ledger.validate_integrity())
                .map_err(freshness_store_error)?;
        }
        if has_checkpoints {
            Ok(Self::Checkpoints(conn))
        } else {
            Ok(Self::MissingCheckpointTable)
        }
    }

    fn evaluate_resource(
        &self,
        resource: ServingFreshnessResource,
        project_root: &Path,
        now_ms: i64,
    ) -> Result<StatusResource, CliError> {
        match self {
            Self::MissingDatabase => Ok(non_evaluable(
                resource,
                NonEvaluableReason::StateDatabaseMissing,
                None,
                None,
                None,
            )?),
            Self::MissingCheckpointTable => Ok(non_evaluable(
                resource,
                NonEvaluableReason::CheckpointTableMissing,
                None,
                None,
                None,
            )?),
            Self::Checkpoints(conn) => {
                let scope_json =
                    serde_json::to_string(&resource.state_scope).map_err(status_internal)?;
                let heads = committed_heads(conn, &resource.resource_id, &scope_json)?;
                match heads.len() {
                    0 => receipt_only_resource(conn, resource, project_root, &scope_json, now_ms),
                    1 => evaluable_head(
                        conn,
                        resource,
                        project_root,
                        heads.into_iter().next().unwrap(),
                        now_ms,
                    ),
                    count => Ok(non_evaluable(
                        resource,
                        NonEvaluableReason::AmbiguousCommittedHeads,
                        Some(count),
                        None,
                        None,
                    )?),
                }
            }
        }
    }
}

fn freshness_state_database_exists(
    path: &Path,
    ownership: cdf_state_sqlite::StateStorePathOwnership,
) -> Result<bool, CliError> {
    match cdf_state_sqlite::database_path_exists(path, ownership) {
        Ok(exists) => Ok(exists),
        Err(error) if error.kind == cdf_kernel::ErrorKind::Internal => {
            Err(CliError::mapped(error, error_catalog::STATUS_FRESHNESS))
        }
        Err(error) => Err(error.into()),
    }
}

fn committed_heads(
    conn: &Connection,
    resource_id: &str,
    scope_json: &str,
) -> Result<Vec<ObservedCheckpoint>, CliError> {
    let mut stmt = conn
        .prepare(
            "
            SELECT
                checkpoint_id,
                pipeline_id,
                package_hash,
                schema_hash,
                receipt_id,
                committed_at_ms
            FROM cdf_checkpoints
            WHERE resource_id = ?
              AND scope_json = ?
              AND status = 'committed'
              AND is_head = 1
            ORDER BY pipeline_id, checkpoint_id
            ",
        )
        .map_err(sqlite_cli_error)?;
    let rows = stmt
        .query_map(params![resource_id, scope_json], observed_checkpoint)
        .map_err(sqlite_cli_error)?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(sqlite_cli_error)
}

fn observed_checkpoint(row: &Row<'_>) -> rusqlite::Result<ObservedCheckpoint> {
    decode_observed_checkpoint(row).map_err(private_status_from_sql)
}

fn decode_observed_checkpoint(row: &Row<'_>) -> Result<ObservedCheckpoint, CdfError> {
    let checkpoint_id: String = row.get("checkpoint_id").map_err(raw_status_row_error)?;
    let pipeline_id: String = row.get("pipeline_id").map_err(raw_status_row_error)?;
    let package_hash: String = row.get("package_hash").map_err(raw_status_row_error)?;
    let schema_hash: String = row.get("schema_hash").map_err(raw_status_row_error)?;
    let receipt_id: String = row.get("receipt_id").map_err(raw_status_row_error)?;
    let committed_at_ms: i64 = row.get("committed_at_ms").map_err(raw_status_row_error)?;
    cdf_kernel::CheckpointId::new(checkpoint_id.clone())
        .map_err(|error| private_status_value_error("checkpoint id", error))?;
    cdf_kernel::PipelineId::new(pipeline_id.clone())
        .map_err(|error| private_status_value_error("pipeline id", error))?;
    cdf_kernel::PackageHash::new(package_hash.clone())
        .map_err(|error| private_status_value_error("package hash", error))?;
    cdf_kernel::SchemaHash::new(schema_hash.clone())
        .map_err(|error| private_status_value_error("schema hash", error))?;
    cdf_kernel::ReceiptId::new(receipt_id.clone())
        .map_err(|error| private_status_value_error("receipt id", error))?;
    if committed_at_ms < 0 {
        return Err(CdfError::internal(
            "decode CDF-managed freshness checkpoint: committed timestamp cannot be negative",
        ));
    }
    Ok(ObservedCheckpoint {
        checkpoint_id,
        pipeline_id,
        package_hash,
        schema_hash,
        receipt_id,
        committed_at_ms,
    })
}

fn evaluable_head(
    conn: &Connection,
    resource: ServingFreshnessResource,
    project_root: &Path,
    checkpoint: ObservedCheckpoint,
    now_ms: i64,
) -> Result<StatusResource, CliError> {
    let committed_at_ms = checkpoint.committed_at_ms;
    let receipt_freshness = committed_head_receipt_freshness(
        conn,
        project_root,
        &checkpoint,
        now_ms,
        resource.max_age_ms,
    )?;
    evaluable(
        resource,
        Some(checkpoint),
        committed_at_ms,
        now_ms,
        receipt_freshness,
    )
}

fn receipt_only_resource(
    conn: &Connection,
    resource: ServingFreshnessResource,
    project_root: &Path,
    scope_json: &str,
    now_ms: i64,
) -> Result<StatusResource, CliError> {
    if !table_exists(conn, "cdf_run_events")? {
        return non_evaluable(
            resource,
            NonEvaluableReason::RunLedgerMissing,
            Some(0),
            None,
            Some(missing_run_ledger_observation(None, now_ms)),
        );
    }

    let receipt_facts = receipt_facts_for_resource(conn, &resource.resource_id, scope_json)?;
    if receipt_facts.is_empty() {
        return non_evaluable(
            resource,
            NonEvaluableReason::CommittedHeadMissing,
            Some(0),
            None,
            None,
        );
    }

    match matching_package_receipt(&receipt_facts, project_root)? {
        PackageReceiptLookup::Found(receipt) => {
            let observed_at_ms = receipt.committed_at_ms;
            let receipt_freshness = Some(receipt_observation(
                ReceiptObservationInput {
                    source: ReceiptFreshnessSource::PackageReceipt,
                    receipt_id: Some(receipt.receipt_id),
                    package_hash: Some(receipt.package_hash),
                    observed_at_ms,
                    run_ledger_recorded_at_ms: Some(receipt.run_ledger_recorded_at_ms),
                    package_path: Some(receipt.package_path),
                    package_receipt_committed_at_ms: Some(observed_at_ms),
                    reason: None,
                },
                now_ms,
                resource.max_age_ms,
            ));
            evaluable(resource, None, observed_at_ms, now_ms, receipt_freshness)
        }
        PackageReceiptLookup::Missing(fact) => non_evaluable(
            resource,
            NonEvaluableReason::ReceiptMissing,
            Some(0),
            None,
            Some(missing_receipt_observation(
                fact.as_ref(),
                None,
                "no package receipt artifact corroborates the receipt-only run-ledger fact",
                now_ms,
            )),
        ),
        PackageReceiptLookup::Corrupt { fact, reason } => non_evaluable(
            resource,
            NonEvaluableReason::ReceiptCorrupt,
            Some(0),
            None,
            Some(corrupt_receipt_observation(
                CorruptReceiptObservationInput {
                    source: ReceiptFreshnessSource::RunLedgerReceipt,
                    receipt_id: Some(fact.receipt_id),
                    package_hash: Some(fact.package_hash),
                    observed_at_ms: None,
                    run_ledger_recorded_at_ms: Some(fact.recorded_at_ms),
                    package_path: fact.package_path,
                    package_receipt_committed_at_ms: None,
                    reason,
                },
                now_ms,
            )),
        ),
    }
}

fn evaluable(
    resource: ServingFreshnessResource,
    checkpoint: Option<ObservedCheckpoint>,
    observed_at_ms: i64,
    now_ms: i64,
    receipt_freshness: Option<ReceiptFreshnessObservation>,
) -> Result<StatusResource, CliError> {
    let age_ms = age_ms(now_ms, observed_at_ms);
    let freshness_state = if age_ms <= resource.max_age_ms {
        FreshnessState::Fresh
    } else {
        FreshnessState::Stale
    };
    Ok(StatusResource {
        resource_id: resource.resource_id,
        trust_level: resource.trust_level,
        state_scope: serde_json::to_value(resource.state_scope).map_err(status_internal)?,
        max_age_ms: resource.max_age_ms,
        freshness_state,
        checkpoint,
        age_ms: Some(age_ms),
        non_evaluable_reason: None,
        matching_committed_heads: None,
        receipt_freshness,
    })
}

fn non_evaluable(
    resource: ServingFreshnessResource,
    reason: NonEvaluableReason,
    matching_committed_heads: Option<usize>,
    checkpoint: Option<ObservedCheckpoint>,
    receipt_freshness: Option<ReceiptFreshnessObservation>,
) -> Result<StatusResource, CliError> {
    Ok(StatusResource {
        resource_id: resource.resource_id,
        trust_level: resource.trust_level,
        state_scope: serde_json::to_value(resource.state_scope).map_err(status_internal)?,
        max_age_ms: resource.max_age_ms,
        freshness_state: FreshnessState::NonEvaluable,
        checkpoint,
        age_ms: None,
        non_evaluable_reason: Some(reason),
        matching_committed_heads,
        receipt_freshness,
    })
}

fn committed_head_receipt_freshness(
    conn: &Connection,
    project_root: &Path,
    checkpoint: &ObservedCheckpoint,
    now_ms: i64,
    max_age_ms: u64,
) -> Result<Option<ReceiptFreshnessObservation>, CliError> {
    if !table_exists(conn, "cdf_run_events")? {
        return Ok(Some(missing_run_ledger_observation(
            Some(checkpoint),
            now_ms,
        )));
    }

    let receipt_facts = matching_receipt_facts(conn, checkpoint)?;
    if receipt_facts.is_empty() {
        return Ok(Some(missing_receipt_observation(
            None,
            Some(checkpoint),
            "run ledger has no destination receipt recorded for the committed checkpoint head",
            now_ms,
        )));
    }

    match matching_package_receipt(&receipt_facts, project_root)? {
        PackageReceiptLookup::Found(receipt)
            if receipt.committed_at_ms == checkpoint.committed_at_ms =>
        {
            Ok(Some(receipt_observation(
                ReceiptObservationInput {
                    source: ReceiptFreshnessSource::PackageReceipt,
                    receipt_id: Some(checkpoint.receipt_id.clone()),
                    package_hash: Some(checkpoint.package_hash.clone()),
                    observed_at_ms: checkpoint.committed_at_ms,
                    run_ledger_recorded_at_ms: Some(receipt.run_ledger_recorded_at_ms),
                    package_path: Some(receipt.package_path),
                    package_receipt_committed_at_ms: Some(receipt.committed_at_ms),
                    reason: None,
                },
                now_ms,
                max_age_ms,
            )))
        }
        PackageReceiptLookup::Found(receipt) => Ok(Some(corrupt_receipt_observation(
            CorruptReceiptObservationInput {
                source: ReceiptFreshnessSource::PackageReceipt,
                receipt_id: Some(checkpoint.receipt_id.clone()),
                package_hash: Some(checkpoint.package_hash.clone()),
                observed_at_ms: Some(checkpoint.committed_at_ms),
                run_ledger_recorded_at_ms: Some(receipt.run_ledger_recorded_at_ms),
                package_path: Some(receipt.package_path),
                package_receipt_committed_at_ms: Some(receipt.committed_at_ms),
                reason: format!(
                    "package receipt committed_at_ms {} does not match checkpoint committed_at_ms {}",
                    receipt.committed_at_ms, checkpoint.committed_at_ms
                ),
            },
            now_ms,
        ))),
        PackageReceiptLookup::Missing(fact) => Ok(Some(missing_receipt_observation(
            fact.as_ref(),
            Some(checkpoint),
            "package receipt artifact is missing for the committed checkpoint receipt",
            now_ms,
        ))),
        PackageReceiptLookup::Corrupt { fact, reason } => Ok(Some(corrupt_receipt_observation(
            CorruptReceiptObservationInput {
                source: ReceiptFreshnessSource::RunLedgerReceipt,
                receipt_id: Some(checkpoint.receipt_id.clone()),
                package_hash: Some(checkpoint.package_hash.clone()),
                observed_at_ms: Some(checkpoint.committed_at_ms),
                run_ledger_recorded_at_ms: Some(fact.recorded_at_ms),
                package_path: fact.package_path,
                package_receipt_committed_at_ms: None,
                reason,
            },
            now_ms,
        ))),
    }
}

fn missing_run_ledger_observation(
    checkpoint: Option<&ObservedCheckpoint>,
    now_ms: i64,
) -> ReceiptFreshnessObservation {
    let observed_at_ms = checkpoint.map(|checkpoint| checkpoint.committed_at_ms);
    ReceiptFreshnessObservation {
        state: ReceiptFreshnessState::MissingRunLedger,
        source: if checkpoint.is_some() {
            ReceiptFreshnessSource::CheckpointCommittedHead
        } else {
            ReceiptFreshnessSource::RunLedger
        },
        receipt_id: checkpoint.map(|checkpoint| checkpoint.receipt_id.clone()),
        package_hash: checkpoint.map(|checkpoint| checkpoint.package_hash.clone()),
        observed_at_ms,
        age_ms: observed_at_ms.map(|observed_at_ms| age_ms(now_ms, observed_at_ms)),
        run_ledger_recorded_at_ms: None,
        package_path: None,
        package_receipt_committed_at_ms: None,
        reason: Some("run ledger table is missing".to_owned()),
    }
}

fn missing_receipt_observation(
    fact: Option<&RunReceiptFact>,
    checkpoint: Option<&ObservedCheckpoint>,
    reason: &str,
    now_ms: i64,
) -> ReceiptFreshnessObservation {
    let observed_at_ms = checkpoint.map(|checkpoint| checkpoint.committed_at_ms);
    ReceiptFreshnessObservation {
        state: ReceiptFreshnessState::MissingReceipt,
        source: if fact.is_some() {
            ReceiptFreshnessSource::RunLedgerReceipt
        } else {
            ReceiptFreshnessSource::CheckpointCommittedHead
        },
        receipt_id: fact
            .map(|fact| fact.receipt_id.clone())
            .or_else(|| checkpoint.map(|checkpoint| checkpoint.receipt_id.clone())),
        package_hash: fact
            .map(|fact| fact.package_hash.clone())
            .or_else(|| checkpoint.map(|checkpoint| checkpoint.package_hash.clone())),
        observed_at_ms,
        age_ms: observed_at_ms.map(|observed_at_ms| age_ms(now_ms, observed_at_ms)),
        run_ledger_recorded_at_ms: fact.map(|fact| fact.recorded_at_ms),
        package_path: fact.and_then(|fact| fact.package_path.clone()),
        package_receipt_committed_at_ms: None,
        reason: Some(reason.to_owned()),
    }
}

struct ReceiptObservationInput {
    source: ReceiptFreshnessSource,
    receipt_id: Option<String>,
    package_hash: Option<String>,
    observed_at_ms: i64,
    run_ledger_recorded_at_ms: Option<i64>,
    package_path: Option<String>,
    package_receipt_committed_at_ms: Option<i64>,
    reason: Option<String>,
}

fn receipt_observation(
    input: ReceiptObservationInput,
    now_ms: i64,
    max_age_ms: u64,
) -> ReceiptFreshnessObservation {
    let age_ms = age_ms(now_ms, input.observed_at_ms);
    let state = if age_ms <= max_age_ms {
        ReceiptFreshnessState::FreshReceipt
    } else {
        ReceiptFreshnessState::StaleReceipt
    };
    ReceiptFreshnessObservation {
        state,
        source: input.source,
        receipt_id: input.receipt_id,
        package_hash: input.package_hash,
        observed_at_ms: Some(input.observed_at_ms),
        age_ms: Some(age_ms),
        run_ledger_recorded_at_ms: input.run_ledger_recorded_at_ms,
        package_path: input.package_path,
        package_receipt_committed_at_ms: input.package_receipt_committed_at_ms,
        reason: input.reason,
    }
}

struct CorruptReceiptObservationInput {
    source: ReceiptFreshnessSource,
    receipt_id: Option<String>,
    package_hash: Option<String>,
    observed_at_ms: Option<i64>,
    run_ledger_recorded_at_ms: Option<i64>,
    package_path: Option<String>,
    package_receipt_committed_at_ms: Option<i64>,
    reason: String,
}

fn corrupt_receipt_observation(
    input: CorruptReceiptObservationInput,
    now_ms: i64,
) -> ReceiptFreshnessObservation {
    ReceiptFreshnessObservation {
        state: ReceiptFreshnessState::CorruptReceipt,
        source: input.source,
        receipt_id: input.receipt_id,
        package_hash: input.package_hash,
        observed_at_ms: input.observed_at_ms,
        age_ms: input
            .observed_at_ms
            .map(|observed_at_ms| age_ms(now_ms, observed_at_ms)),
        run_ledger_recorded_at_ms: input.run_ledger_recorded_at_ms,
        package_path: input.package_path,
        package_receipt_committed_at_ms: input.package_receipt_committed_at_ms,
        reason: Some(input.reason),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RunReceiptFact {
    receipt_id: String,
    package_hash: String,
    recorded_at_ms: i64,
    package_path: Option<String>,
}

fn matching_receipt_facts(
    conn: &Connection,
    checkpoint: &ObservedCheckpoint,
) -> Result<Vec<RunReceiptFact>, CliError> {
    let mut stmt = conn
        .prepare(
            "
            SELECT receipt_id, package_hash, timestamp_ms, package_path
            FROM cdf_run_events
            WHERE kind = 'destination_receipt_recorded'
              AND package_hash = ?
              AND receipt_id = ?
            ORDER BY timestamp_ms DESC, sequence DESC
            ",
        )
        .map_err(sqlite_cli_error)?;
    let rows = stmt
        .query_map(
            params![
                checkpoint.package_hash.as_str(),
                checkpoint.receipt_id.as_str()
            ],
            run_receipt_fact,
        )
        .map_err(sqlite_cli_error)?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(sqlite_cli_error)
}

fn receipt_facts_for_resource(
    conn: &Connection,
    resource_id: &str,
    scope_json: &str,
) -> Result<Vec<RunReceiptFact>, CliError> {
    let mut stmt = conn
        .prepare(
            "
            SELECT receipt_id, package_hash, timestamp_ms, package_path
            FROM cdf_run_events
            WHERE kind = 'destination_receipt_recorded'
              AND resource_id = ?
              AND scope_json = ?
              AND receipt_id IS NOT NULL
              AND package_hash IS NOT NULL
            ORDER BY timestamp_ms DESC, sequence DESC
            ",
        )
        .map_err(sqlite_cli_error)?;
    let rows = stmt
        .query_map(params![resource_id, scope_json], run_receipt_fact)
        .map_err(sqlite_cli_error)?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(sqlite_cli_error)
}

fn run_receipt_fact(row: &Row<'_>) -> rusqlite::Result<RunReceiptFact> {
    decode_run_receipt_fact(row).map_err(private_status_from_sql)
}

fn decode_run_receipt_fact(row: &Row<'_>) -> Result<RunReceiptFact, CdfError> {
    let receipt_id: String = row.get("receipt_id").map_err(raw_status_row_error)?;
    let package_hash: String = row.get("package_hash").map_err(raw_status_row_error)?;
    let recorded_at_ms: i64 = row.get("timestamp_ms").map_err(raw_status_row_error)?;
    let package_path: Option<String> = row.get("package_path").map_err(raw_status_row_error)?;
    cdf_kernel::ReceiptId::new(receipt_id.clone())
        .map_err(|error| private_status_value_error("run receipt id", error))?;
    cdf_kernel::PackageHash::new(package_hash.clone())
        .map_err(|error| private_status_value_error("run package hash", error))?;
    if recorded_at_ms < 0 {
        return Err(CdfError::internal(
            "decode CDF-managed freshness run receipt: recorded timestamp cannot be negative",
        ));
    }
    if package_path
        .as_deref()
        .is_some_and(|path| path.trim().is_empty())
    {
        return Err(CdfError::internal(
            "decode CDF-managed freshness run receipt: package path cannot be empty",
        ));
    }
    Ok(RunReceiptFact {
        receipt_id,
        package_hash,
        recorded_at_ms,
        package_path,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PackageReceiptFact {
    receipt_id: String,
    package_hash: String,
    committed_at_ms: i64,
    run_ledger_recorded_at_ms: i64,
    package_path: String,
}

#[derive(Debug)]
enum PackageReceiptLookup {
    Found(PackageReceiptFact),
    Missing(Option<RunReceiptFact>),
    Corrupt {
        fact: RunReceiptFact,
        reason: String,
    },
}

fn matching_package_receipt(
    facts: &[RunReceiptFact],
    project_root: &Path,
) -> Result<PackageReceiptLookup, CliError> {
    for fact in facts {
        let Some(package_path) = &fact.package_path else {
            continue;
        };
        let package_dir = resolve_package_path(project_root, package_path)?;
        let reader = match PackageReader::open(&package_dir) {
            Ok(reader) => reader,
            Err(error) => {
                return package_receipt_lookup_error(fact, &package_dir, error);
            }
        };
        let mut matching = None;
        if let Err(error) = reader.for_each_receipt(&mut |receipt| {
            if receipt.package_hash.as_str() == fact.package_hash
                && receipt.receipt_id.as_str() == fact.receipt_id
            {
                matching = Some(receipt);
            }
            Ok(())
        }) {
            return package_receipt_lookup_error(fact, &package_dir, error);
        }
        if let Some(receipt) = matching {
            return Ok(PackageReceiptLookup::Found(PackageReceiptFact {
                receipt_id: fact.receipt_id.clone(),
                package_hash: fact.package_hash.clone(),
                committed_at_ms: receipt.committed_at_ms,
                run_ledger_recorded_at_ms: fact.recorded_at_ms,
                package_path: package_path.clone(),
            }));
        }
    }
    Ok(PackageReceiptLookup::Missing(facts.first().cloned()))
}

fn package_receipt_lookup_error(
    fact: &RunReceiptFact,
    package_dir: &Path,
    error: CdfError,
) -> Result<PackageReceiptLookup, CliError> {
    if error.kind == cdf_kernel::ErrorKind::Data {
        Ok(PackageReceiptLookup::Corrupt {
            fact: fact.clone(),
            reason: format!(
                "read package receipts from {}: {error}",
                package_dir.display()
            ),
        })
    } else {
        Err(error.into())
    }
}

fn table_exists(conn: &Connection, name: &str) -> Result<bool, CliError> {
    conn.query_row(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        params![name],
        |_| Ok(()),
    )
    .optional()
    .map(|value| value.is_some())
    .map_err(sqlite_cli_error)
}

fn component_marker_exists(conn: &Connection, component: &str) -> Result<bool, CliError> {
    if !table_exists(conn, "cdf_sqlite_schema_versions")? {
        return Ok(false);
    }
    conn.query_row(
        "SELECT 1 FROM cdf_sqlite_schema_versions WHERE component = ?",
        params![component],
        |_| Ok(()),
    )
    .optional()
    .map(|value| value.is_some())
    .map_err(sqlite_cli_error)
}

fn resolve_package_path(project_root: &Path, value: &str) -> Result<PathBuf, CliError> {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        return Ok(path);
    }
    match std::fs::metadata(&path) {
        Ok(_) => Ok(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            match std::fs::symlink_metadata(&path) {
                Ok(_) => Ok(path),
                Err(link_error) if link_error.kind() == std::io::ErrorKind::NotFound => {
                    Ok(project_root.join(path))
                }
                Err(link_error)
                    if link_error.kind() == std::io::ErrorKind::NotADirectory
                        || cdf_kernel::is_filesystem_loop(&link_error) =>
                {
                    Ok(path)
                }
                Err(link_error) => Err(status_environment(format!(
                    "inspect CDF-managed freshness package path `{value}`: {link_error}; check package-path permissions, device availability, and process file limits before retrying"
                ))),
            }
        }
        Err(error)
            if error.kind() == std::io::ErrorKind::NotADirectory
                || cdf_kernel::is_filesystem_loop(&error) =>
        {
            Ok(path)
        }
        Err(error) => Err(status_environment(format!(
            "inspect CDF-managed freshness package path `{value}`: {error}; check package-path permissions, device availability, and process file limits before retrying"
        ))),
    }
}

fn raw_status_row_error(error: rusqlite::Error) -> CdfError {
    CdfError::internal(format!("decode CDF-managed freshness row column: {error}"))
}

fn private_status_value_error(field: &str, error: CdfError) -> CdfError {
    CdfError::internal(format!(
        "decode CDF-managed freshness {field}: {}",
        error.message
    ))
}

fn private_status_from_sql(error: CdfError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
}

fn summarize(resources: &[StatusResource]) -> StatusSummary {
    StatusSummary {
        total: resources.len(),
        fresh: resources
            .iter()
            .filter(|resource| resource.freshness_state == FreshnessState::Fresh)
            .count(),
        stale: resources
            .iter()
            .filter(|resource| resource.freshness_state == FreshnessState::Stale)
            .count(),
        non_evaluable: resources
            .iter()
            .filter(|resource| resource.freshness_state == FreshnessState::NonEvaluable)
            .count(),
    }
}

fn age_ms(now_ms: i64, committed_at_ms: i64) -> u64 {
    let age = i128::from(now_ms) - i128::from(committed_at_ms);
    if age <= 0 {
        0
    } else {
        u64::try_from(age).unwrap_or(u64::MAX)
    }
}

fn now_ms() -> Result<i64, CliError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            status_environment(format!(
                "read the host clock for freshness evaluation: {error}; correct the system clock and retry"
            ))
        })?;
    i64::try_from(elapsed.as_millis()).map_err(|error| {
        status_environment(format!(
            "represent the host clock for freshness evaluation: {error}; correct the system clock and retry"
        ))
    })
}

fn trust_level_name(trust_level: &TrustLevel) -> &'static str {
    match trust_level {
        TrustLevel::Experimental => "experimental",
        TrustLevel::Governed => "governed",
        TrustLevel::Financial => "financial",
        TrustLevel::Serving => "serving",
    }
}

fn sqlite_cli_error(error: rusqlite::Error) -> CliError {
    let error = classify_sqlite_error(
        SqliteErrorContext::ManagedState,
        "read the CDF freshness state store",
        error,
    );
    if error.kind == cdf_kernel::ErrorKind::Internal {
        CliError::mapped(error, error_catalog::STATUS_FRESHNESS)
    } else {
        error.into()
    }
}

fn freshness_store_error(error: CdfError) -> CliError {
    if error.kind == cdf_kernel::ErrorKind::Internal {
        CliError::mapped(error, error_catalog::STATUS_FRESHNESS)
    } else {
        error.into()
    }
}

fn status_environment(message: impl Into<String>) -> CliError {
    CdfError::environment(message.into()).into()
}

fn status_internal(error: impl std::fmt::Display) -> CliError {
    CliError::mapped(
        CdfError::internal(error.to_string()),
        error_catalog::STATUS_FRESHNESS,
    )
}

#[cfg(test)]
mod tests {
    use cdf_kernel::ErrorKind;

    use super::*;

    #[test]
    fn freshness_sqlite_host_failure_uses_environment_catalog_mapping() {
        let error = sqlite_cli_error(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CANTOPEN),
            None,
        ));

        assert_eq!(error.kind, ErrorKind::Environment);
        assert_eq!(error.code, "CDF-ENV-HOST");
        assert!(error.message.contains("state path"));
        assert!(error.remediation.is_some());
    }

    #[test]
    fn freshness_sqlite_query_invariant_keeps_status_mapping() {
        let error = sqlite_cli_error(rusqlite::Error::InvalidQuery);

        assert_eq!(error.kind, ErrorKind::Internal);
        assert_eq!(error.code, error_catalog::STATUS_FRESHNESS.code);
    }

    #[test]
    fn freshness_sqlite_extended_shape_and_contention_keep_distinct_ownership() {
        let wrong_shape = sqlite_cli_error(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CANTOPEN_ISDIR),
            None,
        ));
        assert_eq!(wrong_shape.kind, ErrorKind::Internal);
        assert_eq!(wrong_shape.code, error_catalog::STATUS_FRESHNESS.code);

        let contention = sqlite_cli_error(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
            None,
        ));
        assert_eq!(contention.kind, ErrorKind::Transient);
        assert_eq!(contention.code, "CDF-RUN-TRANSIENT");
    }

    #[test]
    fn freshness_configured_state_parent_file_is_contract_owned() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("state-parent");
        std::fs::write(&parent, b"not a directory").unwrap();

        let error = freshness_state_database_exists(
            &parent.join("state.db"),
            cdf_state_sqlite::StateStorePathOwnership::Configured,
        )
        .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Contract);
    }

    #[test]
    fn package_receipt_host_failure_propagates_instead_of_becoming_corrupt_data() {
        let fact = RunReceiptFact {
            receipt_id: "receipt".to_owned(),
            package_hash: "sha256:hash".to_owned(),
            recorded_at_ms: 1,
            package_path: Some("package".to_owned()),
        };

        let error = package_receipt_lookup_error(
            &fact,
            Path::new("package"),
            CdfError::environment("permission denied"),
        )
        .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Environment);
    }

    #[test]
    fn freshness_rejects_incomplete_checkpoint_and_run_component_footprints() {
        let root = tempfile::tempdir().unwrap();
        let checkpoint_path = root.path().join(".cdf").join("checkpoint.db");
        std::fs::create_dir_all(checkpoint_path.parent().unwrap()).unwrap();
        let conn = Connection::open(&checkpoint_path).unwrap();
        conn.execute_batch(
            "
            CREATE TABLE cdf_sqlite_schema_versions (
                component TEXT PRIMARY KEY,
                version INTEGER NOT NULL,
                recorded_at_ms INTEGER NOT NULL
            );
            INSERT INTO cdf_sqlite_schema_versions (component, version, recorded_at_ms)
            VALUES ('checkpoint_store', 1, 1);
            ",
        )
        .unwrap();
        drop(conn);
        let error = LocalLedger::open(
            &checkpoint_path,
            cdf_state_sqlite::StateStorePathOwnership::CdfManaged,
        )
        .err()
        .expect("checkpoint marker without table must fail");
        assert_eq!(error.kind, ErrorKind::Internal);

        let run_path = root.path().join(".cdf").join("run.db");
        let conn = Connection::open(&run_path).unwrap();
        conn.execute_batch("CREATE TABLE cdf_run_events (sequence INTEGER)")
            .unwrap();
        drop(conn);
        let error = LocalLedger::open(
            &run_path,
            cdf_state_sqlite::StateStorePathOwnership::CdfManaged,
        )
        .err()
        .expect("orphan run-event table must fail");
        assert_eq!(error.kind, ErrorKind::Internal);
    }
}
