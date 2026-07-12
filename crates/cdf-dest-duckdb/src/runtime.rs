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
            staged_ingress_lane: Some("duckdb.connection".to_owned()),
            final_binding_lane: Some("duckdb.connection".to_owned()),
            ingress_mode: DestinationIngressMode::StagedDurableSegments,
            staged_ingress: Some(cdf_runtime::StagedIngressCapabilities {
                recovery: cdf_runtime::StagingRecoveryMode::RollbackRedrive,
                visibility: cdf_runtime::StagingVisibility::IsolatedUntilFinalBinding,
                abort_idempotent: true,
                lifecycle_cleanup: true,
                final_binding_requires_exclusive_writer: true,
            }),
            writer_model: DestinationWriterModel::SingleWriter,
            commit_payload_mode: cdf_runtime::DestinationCommitPayloadMode::SegmentStreaming,
            max_in_flight_segments: Some(1),
            max_in_flight_bytes: Some(64 * 1024 * 1024),
            bulk_paths: vec![cdf_runtime::BulkPathDescriptor {
                path_id: "arrow_record_batch_appender".to_owned(),
                version: 1,
                ingress_mode: DestinationIngressMode::FinalizedPackageOnly,
                writer_model: DestinationWriterModel::SingleWriter,
                ordering: cdf_runtime::BulkOrdering::ManifestOrder,
                rows: cdf_runtime::BulkSizeRange {
                    minimum: 8 * 1024,
                    preferred: 64 * 1024,
                    maximum: 64 * 1024,
                },
                bytes: cdf_runtime::BulkSizeRange {
                    minimum: 1024 * 1024,
                    preferred: 16 * 1024 * 1024,
                    maximum: 64 * 1024 * 1024,
                },
                max_useful_writers: 1,
                blocking_lane: Some("duckdb.connection".to_owned()),
                native_internal_parallelism: 1,
                external_staging: false,
                fallback: cdf_runtime::BulkFallbackMode::Forbidden,
                measured_evidence_version: None,
            }],
            bulk_path: Some("arrow_record_batch_appender".to_owned()),
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

    fn begin_staged_ingress(
        &mut self,
        request: cdf_runtime::StagedIngressRequest,
    ) -> Result<Box<dyn cdf_runtime::StagedIngressSession>> {
        self.begin_staged_ingress_session(request)
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
