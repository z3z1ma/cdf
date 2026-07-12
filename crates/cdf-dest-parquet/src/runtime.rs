use std::path::{Path, PathBuf};

use arrow_schema::Schema;
use cdf_kernel::{
    CapabilitySupport, CdfError, DestinationId, DestinationProtocol, DestinationSheet,
    ResourceStream, Result, WriteDisposition,
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

use crate::{ParquetCommitRequest, ParquetDestination};

pub struct ParquetRuntimeDriver;

impl DestinationDriver for ParquetRuntimeDriver {
    fn schemes(&self) -> &'static [&'static str] {
        &["parquet"]
    }

    fn inspect(
        &self,
        uri: &str,
        context: &DestinationResolutionContext<'_>,
    ) -> Result<DestinationInspection> {
        let root = absolute_under_root(context.project_root()?, local_uri_path(uri, "parquet")?);
        let sheet_artifact = ParquetDestination::destination_sheet_artifact()?;
        Ok(DestinationInspection {
            description: filesystem_description(&root),
            sheet_artifact_hash: artifact_hash(&sheet_artifact)?,
            sheet_artifact,
            runtime: parquet_runtime_capabilities(),
            health_probes: vec![DestinationHealthProbe {
                probe_id: "filesystem_root".to_owned(),
                description: format!("inspect Parquet filesystem root {}", root.display()),
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
        let root = absolute_under_root(context.project_root()?, local_uri_path(uri, "parquet")?);
        Ok(Box::new(FilesystemParquetRuntime {
            destination: None,
            root,
            execution: context.execution_services().cloned(),
        }))
    }

    fn health(
        &self,
        uri: &str,
        context: &DestinationResolutionContext<'_>,
    ) -> Result<Vec<DestinationHealthResult>> {
        let root = absolute_under_root(context.project_root()?, local_uri_path(uri, "parquet")?);
        Ok(vec![DestinationHealthResult {
            probe_id: "destination".to_owned(),
            status: DestinationHealthStatus::Passed,
            message: "Parquet destination capabilities loaded".to_owned(),
            details: [(
                "filesystem_root".to_owned(),
                serde_json::json!(root.display().to_string()),
            )]
            .into_iter()
            .collect(),
        }])
    }
}

impl DestinationRuntime for ParquetDestination {
    fn protocol(&self) -> &dyn DestinationProtocol {
        self
    }

    fn describe(&self) -> DestinationDescription {
        DestinationDescription::new(
            self.sheet().destination.clone(),
            &["parquet"],
            "parquet object store",
        )
    }

    fn runtime_capabilities(&self) -> DestinationRuntimeCapabilities {
        parquet_runtime_capabilities()
    }

    fn prepare_package_commit(
        &mut self,
        package_dir: &Path,
        _reader: &PackageReader,
        inputs: &PackageReplayInputs,
        _context: &DestinationPlanningContext<'_>,
    ) -> Result<PreparedDestinationCommit> {
        prepare_parquet_commit(self, package_dir, inputs)
    }

    fn bind_prepared_commit(&mut self, prepared: &mut PreparedDestinationCommit) -> Result<()> {
        reject_unexpected_pending_context(prepared, "Parquet")
    }
}

pub struct FilesystemParquetRuntime {
    destination: Option<ParquetDestination>,
    root: PathBuf,
    execution: Option<cdf_runtime::ExecutionServices>,
}

impl FilesystemParquetRuntime {
    pub fn new(root: PathBuf) -> Self {
        Self {
            destination: None,
            root,
            execution: None,
        }
    }

    pub fn with_execution_services(
        root: PathBuf,
        execution: cdf_runtime::ExecutionServices,
    ) -> Self {
        Self {
            destination: None,
            root,
            execution: Some(execution),
        }
    }

    fn destination(&mut self) -> Result<&ParquetDestination> {
        if self.destination.is_none() {
            let execution = self.execution.clone().ok_or_else(|| {
                CdfError::contract(
                    "Parquet destination execution requires injected ExecutionServices",
                )
            })?;
            self.destination = Some(ParquetDestination::new_filesystem(&self.root, execution)?);
        }
        Ok(self.destination.as_ref().expect("destination was just set"))
    }
}

impl DestinationRuntime for FilesystemParquetRuntime {
    fn protocol(&self) -> &dyn DestinationProtocol {
        self.destination
            .as_ref()
            .expect("filesystem Parquet destination must be materialized before protocol use")
    }

    fn describe(&self) -> DestinationDescription {
        filesystem_description(&self.root)
    }

    fn runtime_capabilities(&self) -> DestinationRuntimeCapabilities {
        parquet_runtime_capabilities()
    }

    fn destination_sheet(&self) -> Result<DestinationSheet> {
        ParquetDestination::destination_sheet()
    }

    fn supported_dispositions(&self) -> &[WriteDisposition] {
        static SUPPORTED: [WriteDisposition; 2] =
            [WriteDisposition::Append, WriteDisposition::Replace];
        &SUPPORTED
    }

    fn quarantine_table_support(&self) -> CapabilitySupport {
        CapabilitySupport::Unsupported
    }

    fn plan_resource_commit(
        &mut self,
        _resource: &dyn ResourceStream,
        _output_schema: &Schema,
        inputs: &DestinationCommitPlanningInputs,
    ) -> Result<DestinationCommitPlanningOutcome> {
        let (sheet, plan) = ParquetDestination::dry_plan_commit(&inputs.destination_commit)?;
        Ok(DestinationCommitPlanningOutcome::new(sheet, plan))
    }

    fn prepare_package_commit(
        &mut self,
        package_dir: &Path,
        _reader: &PackageReader,
        inputs: &PackageReplayInputs,
        _context: &DestinationPlanningContext<'_>,
    ) -> Result<PreparedDestinationCommit> {
        let destination = self.destination()?;
        prepare_parquet_commit(destination, package_dir, inputs)
    }

    fn bind_prepared_commit(&mut self, prepared: &mut PreparedDestinationCommit) -> Result<()> {
        reject_unexpected_pending_context(prepared, "Parquet")
    }

    fn ensure_protocol_ready(&mut self) -> Result<()> {
        self.destination().map(|_| ())
    }
}

fn filesystem_description(root: &Path) -> DestinationDescription {
    DestinationDescription::new(
        DestinationId::new("parquet_object_store").expect("static destination id"),
        &["parquet"],
        root.display().to_string(),
    )
}

fn prepare_parquet_commit(
    destination: &ParquetDestination,
    package_dir: &Path,
    inputs: &PackageReplayInputs,
) -> Result<PreparedDestinationCommit> {
    let request = ParquetCommitRequest {
        package_dir: package_dir.to_path_buf(),
        commit: inputs.destination_commit.clone(),
        schema_hash: inputs.schema_hash.clone(),
    };
    let plan = destination.plan_package_commit(&request)?;
    Ok(PreparedDestinationCommit::new(
        request.commit,
        plan.kernel,
        DestinationReceiptReportingPolicy::DestinationCommit {
            duplicate: plan.duplicate,
        },
    ))
}

pub(crate) fn parquet_runtime_capabilities() -> DestinationRuntimeCapabilities {
    DestinationRuntimeCapabilities {
        blocking_lanes: vec![cdf_runtime::BlockingLaneSpec {
            lane_id: "parquet.encode".to_owned(),
            maximum_concurrency: 2,
            cpu_slot_cost: 1,
            native_internal_parallelism: 1,
            affinity: cdf_runtime::LaneAffinity::Shared,
            interruption: cdf_runtime::InterruptionSafety::CooperativeOnly,
        }],
        staged_ingress_lane: None,
        final_binding_lane: None,
        ingress_mode: DestinationIngressMode::FinalizedPackageOnly,
        staged_ingress: None,
        writer_model: DestinationWriterModel::SingleWriter,
        commit_payload_mode: cdf_runtime::DestinationCommitPayloadMode::SegmentStreaming,
        max_in_flight_segments: Some(1),
        max_in_flight_bytes: Some(64 * 1024 * 1024),
        bulk_paths: vec![cdf_runtime::BulkPathDescriptor {
            path_id: "arrow_ipc_to_parquet".to_owned(),
            version: 1,
            ingress_mode: DestinationIngressMode::FinalizedPackageOnly,
            writer_model: DestinationWriterModel::SingleWriter,
            ordering: cdf_runtime::BulkOrdering::ManifestOrder,
            rows: cdf_runtime::BulkSizeRange {
                minimum: 8 * 1024,
                preferred: 64 * 1024,
                maximum: 1024 * 1024,
            },
            bytes: cdf_runtime::BulkSizeRange {
                minimum: 1024 * 1024,
                preferred: 16 * 1024 * 1024,
                maximum: 64 * 1024 * 1024,
            },
            max_useful_writers: 1,
            blocking_lane: None,
            native_internal_parallelism: 1,
            external_staging: true,
            fallback: cdf_runtime::BulkFallbackMode::PreflightOnly,
            measured_evidence_version: None,
        }],
        bulk_path: Some("arrow_ipc_to_parquet".to_owned()),
        bulk_evidence_version: None,
        replay_requires_explicit_target: false,
        replay_target_hint: None,
        replay_policy_values: Default::default(),
    }
}
