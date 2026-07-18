use std::path::{Path, PathBuf};

use arrow_schema::Schema;
use cdf_kernel::{
    CapabilitySupport, CdfError, DestinationId, DestinationProtocol, DestinationSheet,
    ResourceStream, Result, WriteDisposition,
};
use cdf_runtime::{
    DestinationCommitPlanningInputs, DestinationCommitPlanningOutcome, DestinationDescription,
    DestinationDriver, DestinationHealthProbe, DestinationHealthResult, DestinationHealthStatus,
    DestinationIngressMode, DestinationInspection, DestinationResolutionContext,
    DestinationRuntime, DestinationRuntimeCapabilities, DestinationWriterModel,
    absolute_under_root, artifact_hash, local_uri_path,
};

use crate::ParquetDestination;

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

    fn ingress(&mut self) -> cdf_runtime::DestinationIngress<'_> {
        cdf_runtime::DestinationIngress::StagedSegments(self)
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

    fn prepare_bulk_paths(
        &mut self,
        input: &cdf_runtime::BulkPathPreparationInput<'_>,
    ) -> Result<cdf_runtime::BulkPathPreparation> {
        prepare_parquet_bulk_paths(input, &self.runtime_capabilities())
    }
}

impl cdf_runtime::StagedSegmentIngress for ParquetDestination {
    fn begin_staged_ingress(
        &mut self,
        request: cdf_runtime::StagedIngressRequest,
    ) -> Result<Box<dyn cdf_runtime::StagedIngressSession>> {
        self.runtime_capabilities()
            .validate_prepared_bulk_path(request.bulk_path())?;
        Ok(Box::new(crate::staging::ParquetStagedIngressSession::new(
            self.clone(),
            request,
        )?))
    }

    fn inspect_staged_ingress(
        &mut self,
        attempt_id: &cdf_runtime::LoadAttemptId,
    ) -> Result<Option<cdf_runtime::StagingSnapshot>> {
        // Parquet has no reattachable in-process encoder state. Generic orchestration separately
        // enumerates durable candidates and holds an exact cleanup lease before deleting them.
        let _ = attempt_id;
        Ok(None)
    }

    fn staging_cleanup_candidates(
        &mut self,
        target: &cdf_kernel::TargetName,
    ) -> Result<Vec<cdf_runtime::StagingCleanupCandidate>> {
        ParquetDestination::staging_cleanup_candidates(self, target)
    }

    fn cleanup_expired_staging(
        &mut self,
        candidate: &cdf_runtime::StagingCleanupCandidate,
        proof: &cdf_runtime::ExpiredStagingLeaseProof,
        mutation_guard: &cdf_runtime::StagingMutationGuard,
    ) -> Result<u64> {
        self.cleanup_expired_staging_candidate(candidate, proof, mutation_guard)
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

    fn ingress(&mut self) -> cdf_runtime::DestinationIngress<'_> {
        cdf_runtime::DestinationIngress::StagedSegments(self)
    }

    fn bind_execution_services(
        &mut self,
        execution: &cdf_runtime::ExecutionServices,
    ) -> Result<()> {
        self.execution = Some(execution.clone());
        self.destination = None;
        Ok(())
    }

    fn describe(&self) -> DestinationDescription {
        filesystem_description(&self.root)
    }

    fn runtime_capabilities(&self) -> DestinationRuntimeCapabilities {
        parquet_runtime_capabilities()
    }

    fn prepare_bulk_paths(
        &mut self,
        input: &cdf_runtime::BulkPathPreparationInput<'_>,
    ) -> Result<cdf_runtime::BulkPathPreparation> {
        prepare_parquet_bulk_paths(input, &self.runtime_capabilities())
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

    fn ensure_protocol_ready(&mut self) -> Result<()> {
        self.destination().map(|_| ())
    }
}

impl cdf_runtime::StagedSegmentIngress for FilesystemParquetRuntime {
    fn begin_staged_ingress(
        &mut self,
        request: cdf_runtime::StagedIngressRequest,
    ) -> Result<Box<dyn cdf_runtime::StagedIngressSession>> {
        self.runtime_capabilities()
            .validate_prepared_bulk_path(request.bulk_path())?;
        Ok(Box::new(crate::staging::ParquetStagedIngressSession::new(
            self.destination()?.clone(),
            request,
        )?))
    }

    fn inspect_staged_ingress(
        &mut self,
        _attempt_id: &cdf_runtime::LoadAttemptId,
    ) -> Result<Option<cdf_runtime::StagingSnapshot>> {
        Ok(None)
    }

    fn staging_cleanup_candidates(
        &mut self,
        target: &cdf_kernel::TargetName,
    ) -> Result<Vec<cdf_runtime::StagingCleanupCandidate>> {
        self.destination()?.staging_cleanup_candidates(target)
    }

    fn cleanup_expired_staging(
        &mut self,
        candidate: &cdf_runtime::StagingCleanupCandidate,
        proof: &cdf_runtime::ExpiredStagingLeaseProof,
        mutation_guard: &cdf_runtime::StagingMutationGuard,
    ) -> Result<u64> {
        self.destination()?
            .cleanup_expired_staging_candidate(candidate, proof, mutation_guard)
    }
}

fn filesystem_description(root: &Path) -> DestinationDescription {
    DestinationDescription::new(
        DestinationId::new("parquet_object_store").expect("static destination id"),
        &["parquet"],
        root.display().to_string(),
    )
    .with_product_location_field("root")
}

pub(crate) fn parquet_runtime_capabilities() -> DestinationRuntimeCapabilities {
    DestinationRuntimeCapabilities {
        blocking_lanes: vec![
            cdf_runtime::BlockingLaneSpec {
                lane_id: "parquet.ingress".to_owned(),
                maximum_concurrency: 1,
                cpu_slot_cost: 1,
                native_internal_parallelism: 1,
                affinity: cdf_runtime::LaneAffinity::Shared,
                interruption: cdf_runtime::InterruptionSafety::CooperativeOnly,
            },
            cdf_runtime::BlockingLaneSpec {
                lane_id: "parquet.encode".to_owned(),
                maximum_concurrency: 2,
                cpu_slot_cost: 1,
                native_internal_parallelism: 1,
                affinity: cdf_runtime::LaneAffinity::Shared,
                interruption: cdf_runtime::InterruptionSafety::CooperativeOnly,
            },
        ],
        staged_ingress_lane: Some("parquet.ingress".to_owned()),
        final_binding_lane: Some("parquet.ingress".to_owned()),
        ingress_mode: DestinationIngressMode::StagedDurableSegments,
        staged_ingress: Some(cdf_runtime::StagedIngressCapabilities {
            recovery: cdf_runtime::StagingRecoveryMode::RollbackRedrive,
            visibility: cdf_runtime::StagingVisibility::IsolatedUntilFinalBinding,
            abort_idempotent: true,
            lifecycle_cleanup: true,
            final_binding_requires_exclusive_writer: false,
        }),
        writer_model: DestinationWriterModel::ConcurrentSegments,
        commit_payload_mode: cdf_runtime::DestinationCommitPayloadMode::SegmentStreaming,
        max_in_flight_segments: Some(2),
        max_in_flight_bytes: Some(128 * 1024 * 1024),
        bulk_paths: vec![cdf_runtime::BulkPathDescriptor {
            path_id: "arrow_ipc_to_parquet".to_owned(),
            version: 5,
            ingress_mode: DestinationIngressMode::StagedDurableSegments,
            writer_model: DestinationWriterModel::ConcurrentSegments,
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
            max_useful_writers: 2,
            blocking_lane: Some("parquet.encode".to_owned()),
            native_internal_parallelism: 1,
            external_staging: true,
            fallback: cdf_runtime::BulkFallbackMode::Forbidden,
            schema_preflight_version: "parquet-arrow-mapping@1".to_owned(),
            measured_evidence_version: Some("p3-d8-2026-07-15-v5".to_owned()),
        }],
        bulk_path: Some("arrow_ipc_to_parquet".to_owned()),
        bulk_evidence_version: Some("p3-d8-2026-07-15-v5".to_owned()),
        replay_requires_explicit_target: false,
        replay_target_hint: None,
        replay_policy_values: Default::default(),
    }
}

fn prepare_parquet_bulk_paths(
    input: &cdf_runtime::BulkPathPreparationInput<'_>,
    capabilities: &DestinationRuntimeCapabilities,
) -> Result<cdf_runtime::BulkPathPreparation> {
    cdf_package::validate_parquet_schema(input.output_schema)?;
    let mut preparation = cdf_runtime::BulkPathPreparation::from_capabilities(capabilities)?;
    let available_writers = input
        .execution
        .as_ref()
        .map_or(1, |execution| execution.logical_cpu_slots.max(1));
    for path in &mut preparation.eligible {
        path.writers = available_writers.min(path.descriptor.max_useful_writers);
    }
    preparation.validate()?;
    Ok(preparation)
}
