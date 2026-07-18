use std::collections::BTreeMap;

use arrow_schema::Schema;
use cdf_kernel::{DestinationProtocol, ResourceStream, Result, SchemaHash, WriteDisposition};
use cdf_runtime::{
    DestinationCommitPlanningInputs, DestinationCommitPlanningOutcome, DestinationDescription,
    DestinationDriver, DestinationHealthProbe, DestinationHealthResult, DestinationHealthStatus,
    DestinationIngressMode, DestinationInspection, DestinationResolutionContext,
    DestinationRuntime, DestinationRuntimeCapabilities, DestinationWriterModel,
    absolute_under_root, artifact_hash, local_uri_path,
};

use crate::{
    DUCKDB_BULK_PATH_APPENDER, DUCKDB_BULK_PATH_STREAM_SCAN, DuckDbDestination,
    DuckDbStagedIngressPathPreference,
    package::{field_plan, validate_user_schema_fields},
};

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
        let mut destination = DuckDbDestination::new(path)?;
        if let Some(execution) = context.execution_services() {
            destination = destination.with_execution_services(execution)?;
        }
        Ok(Box::new(destination))
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

    fn ingress(&mut self) -> cdf_runtime::DestinationIngress<'_> {
        cdf_runtime::DestinationIngress::StagedSegments(self)
    }

    fn bind_execution_services(
        &mut self,
        execution: &cdf_runtime::ExecutionServices,
    ) -> Result<()> {
        *self = self.clone().with_execution_services(execution)?;
        Ok(())
    }

    fn describe(&self) -> DestinationDescription {
        DestinationDescription::new(
            self.sheet().destination.clone(),
            &["duckdb"],
            self.database_path().display().to_string(),
        )
        .with_product_location_field("database_path")
        .with_product_receipt_source("duck_db_commit")
    }

    fn runtime_capabilities(&self) -> DestinationRuntimeCapabilities {
        let selected_path_id = self.staged_ingress_path.selected_path_id().to_owned();
        let selected_evidence_version =
            bulk_path_evidence_version(self.staged_ingress_path).to_owned();
        DestinationRuntimeCapabilities {
            blocking_lanes: vec![cdf_runtime::BlockingLaneSpec {
                lane_id: "duckdb.connection".to_owned(),
                maximum_concurrency: 1,
                cpu_slot_cost: 1,
                native_internal_parallelism: native_internal_parallelism_u16(
                    self.native_resources.internal_threads,
                ),
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
            max_in_flight_segments: Some(2),
            max_in_flight_bytes: Some(128 * 1024 * 1024),
            bulk_paths: duckdb_bulk_path_descriptors(native_internal_parallelism_u16(
                self.native_resources.internal_threads,
            )),
            bulk_path: Some(selected_path_id),
            bulk_evidence_version: Some(selected_evidence_version),
            replay_requires_explicit_target: false,
            replay_target_hint: None,
            replay_policy_values: Default::default(),
        }
    }

    fn prepare_bulk_paths(
        &mut self,
        input: &cdf_runtime::BulkPathPreparationInput<'_>,
    ) -> Result<cdf_runtime::BulkPathPreparation> {
        validate_user_schema_fields(input.output_schema)?;
        for field in input.output_schema.fields() {
            field_plan(field.as_ref())?;
        }
        let capabilities = self.runtime_capabilities();
        let mut preparation = cdf_runtime::BulkPathPreparation::from_capabilities(&capabilities)?;
        if matches!(
            input.commit.map(|commit| &commit.disposition),
            Some(WriteDisposition::Merge | WriteDisposition::CdcApply)
        ) {
            preparation.selected_path_id = DUCKDB_BULK_PATH_APPENDER.to_owned();
        }
        preparation.validate()?;
        Ok(preparation)
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

    fn validate_run_preflight(
        &mut self,
        _resource: &dyn ResourceStream,
        _output_schema: &Schema,
        _schema_hash: &SchemaHash,
    ) -> Result<()> {
        Ok(())
    }
}

fn duckdb_bulk_path_descriptors(
    native_internal_parallelism: u16,
) -> Vec<cdf_runtime::BulkPathDescriptor> {
    let appender = cdf_runtime::BulkPathDescriptor {
        path_id: DUCKDB_BULK_PATH_APPENDER.to_owned(),
        version: 1,
        ingress_mode: DestinationIngressMode::StagedDurableSegments,
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
        native_internal_parallelism,
        external_staging: true,
        fallback: cdf_runtime::BulkFallbackMode::Forbidden,
        schema_preflight_version: "duckdb-arrow-mapping@1".to_owned(),
        measured_evidence_version: Some(
            bulk_path_evidence_version(DuckDbStagedIngressPathPreference::Appender).to_owned(),
        ),
    };
    let stream_scan = cdf_runtime::BulkPathDescriptor {
        path_id: DUCKDB_BULK_PATH_STREAM_SCAN.to_owned(),
        version: 1,
        ingress_mode: DestinationIngressMode::StagedDurableSegments,
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
        native_internal_parallelism,
        external_staging: true,
        fallback: cdf_runtime::BulkFallbackMode::Forbidden,
        schema_preflight_version: "duckdb-arrow-mapping@1".to_owned(),
        measured_evidence_version: Some(
            bulk_path_evidence_version(DuckDbStagedIngressPathPreference::StreamScan).to_owned(),
        ),
    };
    vec![appender, stream_scan]
}

fn bulk_path_evidence_version(path: DuckDbStagedIngressPathPreference) -> &'static str {
    match path {
        DuckDbStagedIngressPathPreference::Appender => "p3-f2-2026-07-14-v2",
        DuckDbStagedIngressPathPreference::StreamScan => {
            "p3-g4-2026-07-18-arrow-stream-scan-reference-v1"
        }
    }
}

fn native_internal_parallelism_u16(value: i64) -> u16 {
    u16::try_from(value.max(1)).unwrap_or(u16::MAX)
}

impl cdf_runtime::StagedSegmentIngress for DuckDbDestination {
    fn begin_staged_ingress(
        &mut self,
        request: cdf_runtime::StagedIngressRequest,
    ) -> Result<Box<dyn cdf_runtime::StagedIngressSession>> {
        self.runtime_capabilities()
            .validate_prepared_bulk_path(request.bulk_path())?;
        self.begin_staged_ingress_session(request)
    }

    fn inspect_staged_ingress(
        &mut self,
        _attempt_id: &cdf_runtime::LoadAttemptId,
    ) -> Result<Option<cdf_runtime::StagingSnapshot>> {
        Ok(None)
    }
}
