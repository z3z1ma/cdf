use std::path::{Path, PathBuf};

use arrow_schema::Schema;
use cdf_kernel::{
    CapabilitySupport, DestinationId, DestinationProtocol, DestinationSheet, ResourceStream,
    Result, WriteDisposition,
};
use cdf_package::{PackageReader, PackageReplayInputs};
use cdf_runtime::{
    DestinationCommitPlanningInputs, DestinationCommitPlanningOutcome, DestinationDescription,
    DestinationDriver, DestinationHealthProbe, DestinationIngressMode, DestinationInspection,
    DestinationPlanningContext, DestinationReceiptReportingPolicy, DestinationResolutionContext,
    DestinationRuntime, DestinationRuntimeCapabilities, DestinationWriterModel,
    PreparedDestinationCommit, absolute_under_root, artifact_hash, local_uri_path,
    reject_unexpected_pending_context,
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
        let runtime = FilesystemParquetRuntime::new(root.clone());
        let sheet_artifact = ParquetDestination::destination_sheet_artifact()?;
        Ok(DestinationInspection {
            description: runtime.describe(),
            sheet_artifact_hash: artifact_hash(&sheet_artifact)?,
            sheet_artifact,
            runtime: runtime.runtime_capabilities(),
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
        Ok(Box::new(FilesystemParquetRuntime::new(root)))
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
}

impl FilesystemParquetRuntime {
    pub fn new(root: PathBuf) -> Self {
        Self {
            destination: None,
            root,
        }
    }

    fn destination(&mut self) -> Result<&ParquetDestination> {
        if self.destination.is_none() {
            self.destination = Some(ParquetDestination::new_filesystem(&self.root)?);
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
        DestinationDescription::new(
            DestinationId::new("parquet_object_store").expect("static destination id"),
            &["parquet"],
            self.root.display().to_string(),
        )
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

fn parquet_runtime_capabilities() -> DestinationRuntimeCapabilities {
    DestinationRuntimeCapabilities {
        ingress_mode: DestinationIngressMode::FinalizedPackageOnly,
        writer_model: DestinationWriterModel::SingleWriter,
        max_in_flight_segments: Some(1),
        max_in_flight_bytes: None,
        bulk_path: Some("arrow_ipc_package_rows_to_parquet".to_owned()),
        bulk_evidence_version: None,
    }
}
