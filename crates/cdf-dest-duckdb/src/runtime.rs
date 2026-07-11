use std::path::Path;

use arrow_schema::Schema;
use cdf_kernel::{
    DestinationCommitRequest, DestinationProtocol, ResourceStream, Result, SchemaHash,
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
        Ok(Box::new(DuckDbDestination::new(path)?))
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
            ingress_mode: DestinationIngressMode::FinalizedPackageOnly,
            writer_model: DestinationWriterModel::SingleWriter,
            max_in_flight_segments: Some(1),
            max_in_flight_bytes: None,
            bulk_path: Some("arrow_ipc_package_rows".to_owned()),
            bulk_evidence_version: None,
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
