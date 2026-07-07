use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use cdf_kernel::{CdfError, ScopeKey, TrustLevel};
use rusqlite::{Connection, OpenFlags, OptionalExtension, Row, params};
use serde::Serialize;

use crate::{context::ProjectContext, output::CliError};

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
    CommittedHeadMissing,
    AmbiguousCommittedHeads,
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
        .map(|resource| ledger.evaluate_resource(resource, now_ms))
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
        now_ms: i64,
    ) -> Result<StatusResource, CliError> {
        match self {
            Self::MissingDatabase => Ok(non_evaluable(
                resource,
                NonEvaluableReason::StateDatabaseMissing,
                None,
            )?),
            Self::MissingCheckpointTable => Ok(non_evaluable(
                resource,
                NonEvaluableReason::CheckpointTableMissing,
                None,
            )?),
            Self::Checkpoints(conn) => {
                let scope_json = serde_json::to_string(&resource.state_scope)
                    .map_err(|error| CliError::from(CdfError::internal(error.to_string())))?;
                let heads = committed_heads(conn, &resource.resource_id, &scope_json)?;
                match heads.len() {
                    0 => Ok(non_evaluable(
                        resource,
                        NonEvaluableReason::CommittedHeadMissing,
                        Some(0),
                    )?),
                    1 => evaluable(resource, heads.into_iter().next().unwrap(), now_ms),
                    count => Ok(non_evaluable(
                        resource,
                        NonEvaluableReason::AmbiguousCommittedHeads,
                        Some(count),
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

fn evaluable(
    resource: ServingFreshnessResource,
    checkpoint: ObservedCheckpoint,
    now_ms: i64,
) -> Result<StatusResource, CliError> {
    let age_ms = age_ms(now_ms, checkpoint.committed_at_ms);
    let freshness_state = if age_ms <= resource.max_age_ms {
        FreshnessState::Fresh
    } else {
        FreshnessState::Stale
    };
    Ok(StatusResource {
        resource_id: resource.resource_id,
        trust_level: resource.trust_level,
        state_scope: serde_json::to_value(resource.state_scope)
            .map_err(|error| CliError::from(CdfError::internal(error.to_string())))?,
        max_age_ms: resource.max_age_ms,
        freshness_state,
        checkpoint: Some(checkpoint),
        age_ms: Some(age_ms),
        non_evaluable_reason: None,
        matching_committed_heads: None,
    })
}

fn non_evaluable(
    resource: ServingFreshnessResource,
    reason: NonEvaluableReason,
    matching_committed_heads: Option<usize>,
) -> Result<StatusResource, CliError> {
    Ok(StatusResource {
        resource_id: resource.resource_id,
        trust_level: resource.trust_level,
        state_scope: serde_json::to_value(resource.state_scope)
            .map_err(|error| CliError::from(CdfError::internal(error.to_string())))?,
        max_age_ms: resource.max_age_ms,
        freshness_state: FreshnessState::NonEvaluable,
        checkpoint: None,
        age_ms: None,
        non_evaluable_reason: Some(reason),
        matching_committed_heads,
    })
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
        .map_err(|error| CliError::from(CdfError::internal(error.to_string())))?;
    i64::try_from(elapsed.as_millis())
        .map_err(|error| CliError::from(CdfError::internal(error.to_string())))
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
    CliError::from(CdfError::internal(error.to_string()))
}
