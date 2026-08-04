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
    absolute_under_root, artifact_hash,
};

use crate::{
    compression::{PHYSICAL_PLAN_VERSION, ParquetCompression},
    models::ParquetDestination,
};

pub struct ParquetRuntimeDriver;

pub(crate) fn parse_parquet_destination_uri(uri: &str) -> Result<(String, ParquetCompression)> {
    let raw = uri.strip_prefix("parquet://").ok_or_else(|| {
        CdfError::contract(format!(
            "destination URI `{uri}` is unsupported; expected parquet://path"
        ))
    })?;
    let (path, query) = raw
        .split_once('?')
        .map_or((raw, None), |(path, query)| (path, Some(query)));
    if path.trim().is_empty() || path.contains("://") || path.contains('#') {
        return Err(CdfError::contract(format!(
            "destination URI `{uri}` is malformed or non-local; expected parquet://path"
        )));
    }
    let mut compression = ParquetCompression::default();
    let mut compression_seen = false;
    if let Some(query) = query {
        if query.is_empty() || query.contains('#') {
            return Err(CdfError::contract(
                "Parquet destination query must contain compression=none|snappy|lz4|zstd",
            ));
        }
        for option in query.split('&') {
            let (key, value) = option.split_once('=').ok_or_else(|| {
                CdfError::contract(format!(
                    "Parquet destination option `{option}` must use key=value syntax"
                ))
            })?;
            if key != "compression" {
                return Err(CdfError::contract(format!(
                    "Parquet destination option `{key}` is unsupported; expected compression"
                )));
            }
            if compression_seen {
                return Err(CdfError::contract(
                    "Parquet destination compression option may appear only once",
                ));
            }
            compression = ParquetCompression::from_name(value)?;
            compression_seen = true;
        }
    }
    Ok((path.to_owned(), compression))
}

impl DestinationDriver for ParquetRuntimeDriver {
    fn schemes(&self) -> &'static [&'static str] {
        &["parquet"]
    }

    fn inspect(
        &self,
        uri: &str,
        context: &DestinationResolutionContext<'_>,
    ) -> Result<DestinationInspection> {
        let (path, compression) = parse_parquet_destination_uri(uri)?;
        let root = absolute_under_root(context.project_root()?, &path);
        let sheet_artifact = ParquetDestination::destination_sheet_artifact()?;
        Ok(DestinationInspection {
            description: filesystem_description(&root),
            sheet_artifact_hash: artifact_hash(&sheet_artifact)?,
            sheet_artifact,
            runtime: parquet_runtime_capabilities(compression),
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
        let (path, compression) = parse_parquet_destination_uri(uri)?;
        let root = absolute_under_root(context.project_root()?, &path);
        Ok(Box::new(FilesystemParquetRuntime {
            destination: None,
            root,
            execution: None,
            compression,
        }))
    }

    fn health(
        &self,
        uri: &str,
        context: &DestinationResolutionContext<'_>,
    ) -> Result<Vec<DestinationHealthResult>> {
        let (path, compression) = parse_parquet_destination_uri(uri)?;
        let root = absolute_under_root(context.project_root()?, &path);
        Ok(vec![DestinationHealthResult {
            probe_id: "destination".to_owned(),
            status: DestinationHealthStatus::Passed,
            message: "Parquet destination capabilities loaded".to_owned(),
            details: [
                (
                    "filesystem_root".to_owned(),
                    serde_json::json!(root.display().to_string()),
                ),
                (
                    "compression".to_owned(),
                    serde_json::json!(compression.name()),
                ),
            ]
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

    fn bind_execution_services(
        &mut self,
        execution: &cdf_runtime::ExecutionServices,
    ) -> Result<()> {
        self.rebind_execution_services(execution)
    }

    fn describe(&self) -> DestinationDescription {
        DestinationDescription::new(
            self.sheet().destination.clone(),
            &["parquet"],
            "parquet object store",
        )
    }

    fn runtime_capabilities(&self) -> DestinationRuntimeCapabilities {
        parquet_runtime_capabilities(self.compression())
    }

    fn prepare_bulk_paths(
        &mut self,
        input: &cdf_runtime::BulkPathPreparationInput<'_>,
    ) -> Result<cdf_runtime::BulkPathPreparation> {
        prepare_parquet_bulk_paths(input, &self.runtime_capabilities(), Some(self.execution()))
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
    compression: ParquetCompression,
}

impl FilesystemParquetRuntime {
    pub fn new(root: PathBuf) -> Self {
        Self {
            destination: None,
            root,
            execution: None,
            compression: ParquetCompression::default(),
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
            compression: ParquetCompression::default(),
        }
    }

    fn destination(&mut self) -> Result<&ParquetDestination> {
        if self.destination.is_none() {
            let execution = self.execution.clone().ok_or_else(|| {
                CdfError::contract(
                    "Parquet destination execution requires injected ExecutionServices",
                )
            })?;
            self.destination = Some(
                ParquetDestination::new_filesystem(&self.root, execution)?
                    .with_compression(self.compression),
            );
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
        parquet_runtime_capabilities(self.compression)
    }

    fn prepare_bulk_paths(
        &mut self,
        input: &cdf_runtime::BulkPathPreparationInput<'_>,
    ) -> Result<cdf_runtime::BulkPathPreparation> {
        prepare_parquet_bulk_paths(input, &self.runtime_capabilities(), self.execution.as_ref())
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

pub(crate) fn parquet_runtime_capabilities(
    compression: ParquetCompression,
) -> DestinationRuntimeCapabilities {
    DestinationRuntimeCapabilities {
        blocking_lanes: vec![
            cdf_runtime::BlockingLaneSpec {
                lane_id: "parquet.ingress".to_owned(),
                binding: cdf_runtime::BlockingLaneBinding::Static,
                maximum_concurrency: 1,
                cpu_slot_cost: 1,
                native_internal_parallelism: 1,
                affinity: cdf_runtime::LaneAffinity::Shared,
                interruption: cdf_runtime::InterruptionSafety::CooperativeOnly,
            },
            cdf_runtime::BlockingLaneSpec {
                lane_id: "parquet.encode".to_owned(),
                binding: cdf_runtime::BlockingLaneBinding::Static,
                maximum_concurrency: u16::MAX,
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
        bulk_paths: ParquetCompression::ALL
            .into_iter()
            .map(|candidate| cdf_runtime::BulkPathDescriptor {
                path_id: candidate.path_id().to_owned(),
                version: PHYSICAL_PLAN_VERSION,
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
                max_useful_writers: u16::MAX,
                blocking_lane: Some("parquet.encode".to_owned()),
                native_internal_parallelism: 1,
                external_staging: true,
                fallback: cdf_runtime::BulkFallbackMode::Forbidden,
                schema_preflight_version: "parquet-arrow-mapping@2".to_owned(),
                measured_evidence_version: Some("p3-parquet-compression-2026-07-26-v1".to_owned()),
            })
            .collect(),
        bulk_path: Some(compression.path_id().to_owned()),
        bulk_evidence_version: Some("p3-parquet-compression-2026-07-26-v1".to_owned()),
        replay_requires_explicit_target: false,
        replay_target_hint: None,
    }
}

fn prepare_parquet_bulk_paths(
    input: &cdf_runtime::BulkPathPreparationInput<'_>,
    capabilities: &DestinationRuntimeCapabilities,
    execution: Option<&cdf_runtime::ExecutionServices>,
) -> Result<cdf_runtime::BulkPathPreparation> {
    cdf_package::validate_parquet_schema(input.output_schema)?;
    let mut preparation = cdf_runtime::BulkPathPreparation::from_capabilities(capabilities)?;
    let host_writers = input
        .execution
        .as_ref()
        .map_or(1, |execution| execution.logical_cpu_slots.max(1));
    for path in &mut preparation.eligible {
        let settings = crate::package::ParquetWriterSettings {
            rows_per_batch: path.rows_per_batch,
            bytes_per_batch: path.bytes_per_batch,
            compression: ParquetCompression::from_path_id(&path.descriptor.path_id)?,
        };
        let worker_bytes = crate::package::parquet_worker_working_set_bytes(settings)?;
        let memory_writers = execution.map_or(1, |execution| {
            let snapshot = execution.memory().snapshot();
            (snapshot.budget_bytes / worker_bytes).clamp(1, u64::from(u16::MAX)) as u16
        });
        let run_writers = execution
            .map(cdf_runtime::ExecutionServices::run_job_ceiling)
            .transpose()?
            .flatten()
            .unwrap_or(host_writers);
        path.writers = host_writers
            .min(run_writers)
            .min(memory_writers)
            .min(path.descriptor.max_useful_writers)
            .max(1);
    }
    preparation.validate()?;
    Ok(preparation)
}
