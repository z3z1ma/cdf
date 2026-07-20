use std::{
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use cdf_kernel::{CdfError, ScopeKey, TrustLevel};
use cdf_package::PackageReader;
use rusqlite::{Connection, OpenFlags, OptionalExtension, Row, params};
use serde::Serialize;

use crate::{context::ProjectContext, error_catalog, output::CliError};

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

pub(crate) fn evaluate(context: &ProjectContext) -> Result<StatusReport, CliError> {
    let resources = context
        .resources
        .iter()
        .filter_map(|resource| {
            let descriptor = resource.descriptor();
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
    let ledger = LocalLedger::open(&state_path)?;
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
    fn open(path: &Path) -> Result<Self, CliError> {
        if !path.exists() {
            return Ok(Self::MissingDatabase);
        }
        let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
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
    Ok(ObservedCheckpoint {
        checkpoint_id: row.get("checkpoint_id")?,
        pipeline_id: row.get("pipeline_id")?,
        package_hash: row.get("package_hash")?,
        schema_hash: row.get("schema_hash")?,
        receipt_id: row.get("receipt_id")?,
        committed_at_ms: row.get("committed_at_ms")?,
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

    match matching_package_receipt(&receipt_facts, project_root) {
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

    match matching_package_receipt(&receipt_facts, project_root) {
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
            |row| {
                Ok(RunReceiptFact {
                    receipt_id: row.get("receipt_id")?,
                    package_hash: row.get("package_hash")?,
                    recorded_at_ms: row.get("timestamp_ms")?,
                    package_path: row.get("package_path")?,
                })
            },
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
        .query_map(params![resource_id, scope_json], |row| {
            Ok(RunReceiptFact {
                receipt_id: row.get("receipt_id")?,
                package_hash: row.get("package_hash")?,
                recorded_at_ms: row.get("timestamp_ms")?,
                package_path: row.get("package_path")?,
            })
        })
        .map_err(sqlite_cli_error)?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(sqlite_cli_error)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PackageReceiptFact {
    receipt_id: String,
    package_hash: String,
    committed_at_ms: i64,
    run_ledger_recorded_at_ms: i64,
    package_path: String,
}

enum PackageReceiptLookup {
    Found(PackageReceiptFact),
    Missing(Option<RunReceiptFact>),
    Corrupt {
        fact: RunReceiptFact,
        reason: String,
    },
}

fn matching_package_receipt(facts: &[RunReceiptFact], project_root: &Path) -> PackageReceiptLookup {
    for fact in facts {
        let Some(package_path) = &fact.package_path else {
            continue;
        };
        let package_dir = resolve_package_path(project_root, package_path);
        let reader = match PackageReader::open(&package_dir) {
            Ok(reader) => reader,
            Err(error) => {
                return PackageReceiptLookup::Corrupt {
                    fact: fact.clone(),
                    reason: format!(
                        "read package receipts from {}: {error}",
                        package_dir.display()
                    ),
                };
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
            return PackageReceiptLookup::Corrupt {
                fact: fact.clone(),
                reason: format!(
                    "read package receipts from {}: {error}",
                    package_dir.display()
                ),
            };
        }
        if let Some(receipt) = matching {
            return PackageReceiptLookup::Found(PackageReceiptFact {
                receipt_id: fact.receipt_id.clone(),
                package_hash: fact.package_hash.clone(),
                committed_at_ms: receipt.committed_at_ms,
                run_ledger_recorded_at_ms: fact.recorded_at_ms,
                package_path: package_path.clone(),
            });
        }
    }
    PackageReceiptLookup::Missing(facts.first().cloned())
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

fn resolve_package_path(project_root: &Path, value: &str) -> PathBuf {
    let path = PathBuf::from(value);
    if path.is_absolute() || path.exists() {
        path
    } else {
        project_root.join(path)
    }
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
        .map_err(status_internal)?;
    i64::try_from(elapsed.as_millis()).map_err(status_internal)
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
    CliError::mapped(
        CdfError::internal(error.to_string()),
        error_catalog::STATUS_FRESHNESS,
    )
}

fn status_internal(error: impl std::fmt::Display) -> CliError {
    CliError::mapped(
        CdfError::internal(error.to_string()),
        error_catalog::STATUS_FRESHNESS,
    )
}
