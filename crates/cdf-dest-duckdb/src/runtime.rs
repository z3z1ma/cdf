use std::{collections::BTreeMap, path::Path};

use arrow_schema::Schema;
use cdf_kernel::{
    DestinationCommitRequest, DestinationProtocol, ResourceStream, Result, SchemaHash,
};
use cdf_package::{PackageReader, PackageReplayInputs};
use cdf_runtime::{
    DestinationCommitPlanningInputs, DestinationCommitPlanningOutcome, DestinationDescription,
    DestinationDriver, DestinationHealthProbe, DestinationHealthResult, DestinationHealthStatus,
    DestinationIngressMode, DestinationInspection, DestinationPlanningContext,
    DestinationReceiptReportingPolicy, DestinationResolutionContext, DestinationRuntime,
    DestinationRuntimeCapabilities, DestinationWriterModel, PreparedDestinationCommit,
    absolute_under_root, artifact_hash, local_uri_path, reject_unexpected_pending_context,
};

use crate::{DuckDbCommitRequest, DuckDbDestination};

pub struct DuckDbRuntimeDriver;

impl DestinationDriver for DuckDbRuntimeDriver {
    fn schemes(&self) -> &'static [&'static str] {
        &["duckdb"]
    }

    fn inspect(
        &self,
        uri: &str,
        context: &DestinationResolutionContext<'_>,
    ) -> Result<DestinationInspection> {
        let path = absolute_under_root(context.project_root()?, local_uri_path(uri, "duckdb")?);
        let destination = DuckDbDestination::new(&path)?;
        let sheet_artifact = destination.sheet_artifact()?;
        Ok(DestinationInspection {
            description: destination.describe(),
            sheet_artifact_hash: artifact_hash(&sheet_artifact)?,
            sheet_artifact,
            runtime: destination.runtime_capabilities(),
            health_probes: vec![DestinationHealthProbe {
                probe_id: "database_open".to_owned(),
                description: format!("open DuckDB database {}", path.display()),
                requires_credentials: false,
                mutates_destination: false,
            }],
        })
    }

    fn resolve(
        &self,
        uri: &str,
        context: &DestinationResolutionContext<'_>,
    ) -> Result<Box<dyn DestinationRuntime>> {
        let path = absolute_under_root(context.project_root()?, local_uri_path(uri, "duckdb")?);
        Ok(Box::new(DuckDbDestination::new_with_execution(
            path,
            context.execution_services().cloned(),
        )?))
    }

    fn health(
        &self,
        uri: &str,
        context: &DestinationResolutionContext<'_>,
    ) -> Result<Vec<DestinationHealthResult>> {
        let path = absolute_under_root(context.project_root()?, local_uri_path(uri, "duckdb")?);
        let destination = DuckDbDestination::new(&path)?;
        let mut destination_details = BTreeMap::new();
        destination_details.insert("kind".to_owned(), serde_json::json!("duck_db"));
        destination_details.insert(
            "database_path".to_owned(),
            serde_json::json!(path.display().to_string()),
        );
        let mut results = vec![DestinationHealthResult {
            probe_id: "destination".to_owned(),
            status: DestinationHealthStatus::Passed,
            message: "DuckDB destination capabilities loaded".to_owned(),
            details: destination_details,
        }];
        let (status, message, available, diagnostic) = if !path.exists() {
            (
                DestinationHealthStatus::Skipped,
                "DuckDB database does not exist; probe would create it".to_owned(),
                false,
                None,
            )
        } else {
            match destination.probe_icu() {
                Ok(probe) if probe.available => (
                    DestinationHealthStatus::Passed,
                    "ICU probe passed".to_owned(),
                    true,
                    None,
                ),
                Ok(probe) => (
                    DestinationHealthStatus::Failed,
                    probe
                        .error
                        .unwrap_or_else(|| "DuckDB ICU probe returned unavailable".to_owned()),
                    false,
                    None,
                ),
                Err(error) => (
                    DestinationHealthStatus::Failed,
                    error.to_string(),
                    false,
                    Some(error.to_string()),
                ),
            }
        };
        let mut details = BTreeMap::new();
        details.insert(
            "database_path".to_owned(),
            serde_json::json!(path.display().to_string()),
        );
        details.insert(
            "database_exists".to_owned(),
            serde_json::json!(path.exists()),
        );
        details.insert("probe".to_owned(), serde_json::json!("icu_sort_key"));
        details.insert("available".to_owned(), serde_json::json!(available));
        if let Some(diagnostic) = diagnostic {
            details.insert("diagnostic".to_owned(), serde_json::json!(diagnostic));
        }
        results.push(DestinationHealthResult {
            probe_id: "duckdb_icu".to_owned(),
            status,
            message,
            details,
        });
        Ok(results)
    }
}

impl DestinationRuntime for DuckDbDestination {
    fn protocol(&self) -> &dyn DestinationProtocol {
        self
    }

    fn describe(&self) -> DestinationDescription {
        DestinationDescription::new(
            self.sheet().destination.clone(),
            &["duckdb"],
            self.database_path().display().to_string(),
        )
    }

    fn runtime_capabilities(&self) -> DestinationRuntimeCapabilities {
        DestinationRuntimeCapabilities {
            blocking_lanes: vec![cdf_runtime::BlockingLaneSpec {
                lane_id: "duckdb.connection".to_owned(),
                maximum_concurrency: 1,
                cpu_slot_cost: 1,
                native_internal_parallelism: 1,
                affinity: cdf_runtime::LaneAffinity::Pinned,
                interruption: cdf_runtime::InterruptionSafety::CooperativeOnly,
            }],
            ingress_mode: DestinationIngressMode::FinalizedPackageOnly,
            staged_ingress: None,
            writer_model: DestinationWriterModel::SingleWriter,
            max_in_flight_segments: Some(1),
            max_in_flight_bytes: None,
            bulk_path: Some("arrow_ipc_package_rows".to_owned()),
            bulk_evidence_version: None,
            replay_requires_explicit_target: false,
            replay_target_hint: None,
            replay_policy_values: Default::default(),
        }
    }

    fn plan_resource_commit(
        &mut self,
        _resource: &dyn ResourceStream,
        output_schema: &Schema,
        inputs: &DestinationCommitPlanningInputs,
    ) -> Result<DestinationCommitPlanningOutcome> {
        let plan = self.plan_schema_commit(&inputs.destination_commit, output_schema)?;
        Ok(DestinationCommitPlanningOutcome::new(
            self.sheet().clone(),
            plan.kernel,
        ))
    }

    fn prepare_package_commit(
        &mut self,
        package_dir: &Path,
        _reader: &PackageReader,
        inputs: &PackageReplayInputs,
        _context: &DestinationPlanningContext<'_>,
    ) -> Result<PreparedDestinationCommit> {
        let request = DuckDbCommitRequest {
            package_dir: package_dir.to_path_buf(),
            commit: inputs.destination_commit.clone(),
            schema_hash: inputs.schema_hash.clone(),
            merge_keys: inputs.merge_keys.clone(),
        };
        let duplicate = has_duplicate_receipt(self, &request.commit)?;
        let plan = if request.commit.segments.is_empty() {
            self.plan_empty_package_commit(&request)?
        } else {
            self.plan_package_commit(&request)?
        };
        Ok(PreparedDestinationCommit::new(
            request.commit,
            plan.kernel,
            DestinationReceiptReportingPolicy::DestinationCommit { duplicate },
        ))
    }

    fn bind_prepared_commit(&mut self, prepared: &mut PreparedDestinationCommit) -> Result<()> {
        reject_unexpected_pending_context(prepared, "DuckDB")
    }

    fn validate_run_preflight(
        &mut self,
        _resource: &dyn ResourceStream,
        _output_schema: &Schema,
        _schema_hash: &SchemaHash,
    ) -> Result<()> {
        Ok(())
    }
}

fn has_duplicate_receipt(
    destination: &DuckDbDestination,
    request: &DestinationCommitRequest,
) -> Result<bool> {
    if !destination.database_path().exists() {
        return Ok(false);
    }
    let snapshot = destination.read_mirror_snapshot_read_only()?;
    Ok(snapshot.loads.into_iter().any(|load| {
        load.target == request.target.as_str()
            && load.idempotency_token == request.idempotency_token.as_str()
            && load.package_hash == request.package_hash.as_str()
    }))
}
