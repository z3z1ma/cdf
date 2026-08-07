use std::path::{Component, Path, PathBuf};

use arrow_schema::Schema;
use cdf_kernel::{DestinationProtocol, ResourceStream, Result, SchemaHash, WriteDisposition};
use cdf_package_contract::{PackageReplayInputs, VerifiedPackageAccess};
use cdf_runtime::{
    DestinationCommitPlanningInputs, DestinationCommitPlanningOutcome, DestinationDescription,
    DestinationDriver, DestinationHealthProbe, DestinationHealthResult, DestinationHealthStatus,
    DestinationIngressMode, DestinationInspection, DestinationPlanningContext,
    DestinationReceiptReportingPolicy, DestinationResolutionContext, DestinationRuntime,
    DestinationRuntimeCapabilities, DestinationWriterModel, PreparedDestinationCommit,
    artifact_hash,
};

use crate::{
    error::classify_destination_io,
    identifier::SqliteIdentifier,
    mapping::columns_for_schema,
    models::{SqliteCommitRequest, SqliteDestination, SqliteLoadPlanInput},
    package::expected_segments_for_session,
    transaction::{ManagedSqliteCommitSession, SqliteCommitSession, validate_session_begin_inputs},
};

pub struct SqliteRuntimeDriver;

const SQLITE_SCHEMES: &[&str] = &["sqlite"];

impl DestinationDriver for SqliteRuntimeDriver {
    fn schemes(&self) -> &'static [&'static str] {
        SQLITE_SCHEMES
    }

    fn inspect(
        &self,
        uri: &str,
        context: &DestinationResolutionContext<'_>,
    ) -> Result<DestinationInspection> {
        resolve_sqlite_path(uri, context.project_root()?)?;
        let destination = SqliteDestination::connect(PathBuf::from("<inspection>"))?;
        let sheet_artifact = destination.sheet_artifact()?;
        Ok(DestinationInspection {
            description: destination_description(&destination),
            sheet_artifact_hash: artifact_hash(&sheet_artifact)?,
            sheet_artifact,
            runtime: sqlite_runtime_capabilities(),
            health_probes: vec![DestinationHealthProbe {
                probe_id: "file".to_owned(),
                description: "open and inspect the local SQLite destination file".to_owned(),
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
        let path = resolve_sqlite_path(uri, context.project_root()?)?;
        let target = SqliteIdentifier::user(context.target()?.as_str())?;
        Ok(Box::new(SqliteRuntime {
            destination: SqliteDestination::for_runtime(path, target)?,
        }))
    }

    fn health(
        &self,
        uri: &str,
        context: &DestinationResolutionContext<'_>,
    ) -> Result<Vec<DestinationHealthResult>> {
        let path = resolve_sqlite_path(uri, context.project_root()?)?;
        let exists = path.try_exists().map_err(|error| {
            classify_destination_io("inspect SQLite destination health metadata", &error)
        })?;
        let status = if exists {
            DestinationHealthStatus::Passed
        } else {
            DestinationHealthStatus::Unsupported
        };
        Ok(vec![DestinationHealthResult {
            probe_id: "file".to_owned(),
            status,
            message: if exists {
                "SQLite destination file is accessible".to_owned()
            } else {
                "SQLite destination file will be created on first commit".to_owned()
            },
            details: Default::default(),
        }])
    }
}

pub(crate) struct SqliteRuntime {
    destination: SqliteDestination,
}

impl DestinationRuntime for SqliteRuntime {
    fn protocol(&self) -> &dyn DestinationProtocol {
        &self.destination
    }

    fn ingress(&mut self) -> cdf_runtime::DestinationIngress<'_> {
        cdf_runtime::DestinationIngress::FinalizedPackage(self)
    }

    fn bind_execution_services(
        &mut self,
        execution: &cdf_runtime::ExecutionServices,
    ) -> Result<()> {
        self.destination = self
            .destination
            .clone()
            .with_execution_services(Some(execution.clone()));
        Ok(())
    }

    fn describe(&self) -> DestinationDescription {
        destination_description(&self.destination)
    }

    fn runtime_capabilities(&self) -> DestinationRuntimeCapabilities {
        sqlite_runtime_capabilities()
    }

    fn prepare_bulk_paths(
        &mut self,
        input: &cdf_runtime::BulkPathPreparationInput<'_>,
    ) -> Result<cdf_runtime::BulkPathPreparation> {
        columns_for_schema(input.output_schema)?;
        cdf_runtime::BulkPathPreparation::from_capabilities(&self.runtime_capabilities())
    }

    fn validate_run_preflight(
        &mut self,
        resource: &dyn ResourceStream,
        output_schema: &Schema,
        _schema_hash: &SchemaHash,
    ) -> Result<()> {
        columns_for_schema(output_schema)?;
        let descriptor = resource.descriptor();
        if descriptor.write_disposition == WriteDisposition::Merge
            && descriptor.merge_key.is_empty()
        {
            return Err(cdf_kernel::CdfError::contract(
                "SQLite merge requires at least one normalized merge key",
            ));
        }
        for key in &descriptor.merge_key {
            SqliteIdentifier::user(key)?;
        }
        Ok(())
    }

    fn plan_resource_commit(
        &mut self,
        resource: &dyn ResourceStream,
        output_schema: &Schema,
        inputs: &DestinationCommitPlanningInputs,
    ) -> Result<DestinationCommitPlanningOutcome> {
        let load_plan = self.destination.plan_load(load_plan_input(
            &PackageReplayInputs {
                input_checkpoint: None,
                state_delta: inputs.state_delta.clone(),
                destination_commit: inputs.destination_commit.clone(),
                schema_hash: inputs.schema_hash.clone(),
                merge_keys: resource.descriptor().merge_key.clone(),
                destination_policy: Default::default(),
                run_schema_authority: None,
            },
            &self.destination,
            columns_for_schema(output_schema)?,
        )?)?;
        Ok(DestinationCommitPlanningOutcome::new(
            self.destination.sheet().clone(),
            load_plan.kernel,
        ))
    }
}

impl cdf_runtime::FinalizedPackageIngress for SqliteRuntime {
    fn prepare_package_commit(
        &mut self,
        inputs: &PackageReplayInputs,
        context: &DestinationPlanningContext<'_>,
    ) -> Result<PreparedDestinationCommit> {
        self.runtime_capabilities()
            .validate_prepared_bulk_path(context.bulk_path)?;
        let columns = columns_from_package(context.verified_package.as_ref())?;
        let load_plan =
            self.destination
                .plan_load(load_plan_input(inputs, &self.destination, columns)?)?;
        let segments = expected_segments_for_session(
            context.verified_package.as_ref(),
            &load_plan,
            &inputs.destination_commit,
        )?;
        let request = SqliteCommitRequest {
            package: context.verified_package.clone(),
            plan: load_plan.clone(),
            segments,
        };
        Ok(PreparedDestinationCommit::from_verified_inputs(
            inputs,
            load_plan.kernel,
            context.bulk_path.clone(),
            DestinationReceiptReportingPolicy::DestinationCommitReceiptOnly,
        )?
        .with_pending_context(request))
    }

    fn begin_prepared_commit(
        &mut self,
        prepared: &mut PreparedDestinationCommit,
    ) -> Result<Box<dyn cdf_kernel::CommitSession + '_>> {
        let request = prepared.take_pending_context::<SqliteCommitRequest>("SQLite")?;
        validate_session_begin_inputs(prepared.commit(), prepared.plan(), &request.plan)?;
        let execution = self.destination.execution.clone().ok_or_else(|| {
            cdf_kernel::CdfError::contract("SQLite commit requires injected ExecutionServices")
        })?;
        let session = SqliteCommitSession::new(
            self.destination.database_path()?.to_path_buf(),
            execution.clone(),
            request,
        );
        Ok(Box::new(ManagedSqliteCommitSession::new(
            session, execution,
        )))
    }
}

fn load_plan_input(
    inputs: &PackageReplayInputs,
    destination: &SqliteDestination,
    columns: Vec<crate::mapping::SqliteColumn>,
) -> Result<SqliteLoadPlanInput> {
    let target = destination
        .target
        .clone()
        .ok_or_else(|| cdf_kernel::CdfError::internal("SQLite runtime has no resolved target"))?;
    if target.as_str() != inputs.destination_commit.target.as_str() {
        return Err(cdf_kernel::CdfError::contract(
            "explicit SQLite replay target does not match package destination commit target",
        ));
    }
    Ok(SqliteLoadPlanInput {
        package_hash: inputs.state_delta.package_hash.clone(),
        idempotency_token: inputs.destination_commit.idempotency_token.clone(),
        target,
        disposition: inputs.destination_commit.disposition.clone(),
        schema_hash: inputs.schema_hash.clone(),
        segments: inputs.state_delta.segments.clone(),
        columns,
        merge_keys: inputs
            .merge_keys
            .iter()
            .map(|key| SqliteIdentifier::user(key))
            .collect::<Result<Vec<_>>>()?,
        resource_id: Some(inputs.state_delta.resource_id.clone()),
        state_delta: Some(inputs.state_delta.clone()),
    })
}

fn columns_from_package(
    package: &dyn VerifiedPackageAccess,
) -> Result<Vec<crate::mapping::SqliteColumn>> {
    let schema = package.runtime_arrow_schema()?;
    columns_for_schema(schema.as_ref())
}

fn destination_description(destination: &SqliteDestination) -> DestinationDescription {
    DestinationDescription::new(
        destination.sheet().destination.clone(),
        SQLITE_SCHEMES,
        "sqlite",
    )
    .with_product_location_field("database")
}

pub(crate) fn sqlite_runtime_capabilities() -> DestinationRuntimeCapabilities {
    DestinationRuntimeCapabilities {
        blocking_lanes: vec![cdf_runtime::BlockingLaneSpec {
            lane_id: "sqlite.destination.sync".to_owned(),
            binding: cdf_runtime::BlockingLaneBinding::Static,
            maximum_concurrency: 1,
            cpu_slot_cost: 1,
            native_internal_parallelism: 1,
            affinity: cdf_runtime::LaneAffinity::Pinned,
            interruption: cdf_runtime::InterruptionSafety::CooperativeOnly,
        }],
        staged_ingress_lane: None,
        final_binding_lane: Some("sqlite.destination.sync".to_owned()),
        ingress_mode: DestinationIngressMode::FinalizedPackageOnly,
        staged_ingress: None,
        writer_model: DestinationWriterModel::SingleWriter,
        commit_payload_mode: cdf_runtime::DestinationCommitPayloadMode::SegmentStreaming,
        max_in_flight_segments: Some(1),
        max_in_flight_bytes: Some(64 * 1024 * 1024),
        bulk_paths: vec![cdf_runtime::BulkPathDescriptor {
            path_id: "prepared_statement".to_owned(),
            version: 1,
            ingress_mode: DestinationIngressMode::FinalizedPackageOnly,
            writer_model: DestinationWriterModel::SingleWriter,
            ordering: cdf_runtime::BulkOrdering::ManifestOrder,
            rows: cdf_runtime::BulkSizeRange {
                minimum: 1,
                preferred: 8 * 1024,
                maximum: 64 * 1024,
            },
            bytes: cdf_runtime::BulkSizeRange {
                minimum: 64 * 1024,
                preferred: 8 * 1024 * 1024,
                maximum: 64 * 1024 * 1024,
            },
            max_useful_writers: 1,
            blocking_lane: Some("sqlite.destination.sync".to_owned()),
            native_internal_parallelism: 1,
            external_staging: false,
            fallback: cdf_runtime::BulkFallbackMode::Forbidden,
            schema_preflight_version: "sqlite-arrow-mapping@1".to_owned(),
            measured_evidence_version: Some("sqlite-destination-roofline-v1".to_owned()),
        }],
        bulk_path: Some("prepared_statement".to_owned()),
        bulk_evidence_version: Some("sqlite-destination-roofline-v1".to_owned()),
        replay_requires_explicit_target: true,
        replay_target_hint: Some("table".to_owned()),
    }
}

fn resolve_sqlite_path(uri: &str, project_root: &Path) -> Result<PathBuf> {
    let raw = uri.strip_prefix("sqlite://").ok_or_else(|| {
        cdf_kernel::CdfError::contract("SQLite destination URI must begin with `sqlite://`")
    })?;
    if raw.is_empty() || raw.contains(['?', '#', '%']) || raw.chars().any(char::is_control) {
        return Err(cdf_kernel::CdfError::contract(
            "SQLite destination URI must contain a nonempty literal local path without query, fragment, percent escapes, or control characters",
        ));
    }
    let path = Path::new(raw);
    if path
        .components()
        .any(|component| component == Component::ParentDir)
    {
        return Err(cdf_kernel::CdfError::contract(
            "SQLite destination path must not contain parent traversal",
        ));
    }
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(project_root.join(path))
    }
}
