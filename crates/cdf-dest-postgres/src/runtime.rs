use arrow_schema::Schema;
use cdf_http::SecretUri;
use cdf_kernel::{
    CHECKPOINT_STATE_VERSION, CheckpointId, CursorPosition, CursorValue,
    DestinationCorrectionCommitPlan, DestinationCorrectionCommitRequest, DestinationProtocol,
    PackageHash, PipelineId, ResourceStream, Result, SchemaHash, SourcePosition, StateDelta,
    StateSegment, TargetName,
};
use cdf_package_contract::{PackageReplayInputs, VerifiedPackageAccess};
use cdf_runtime::{
    DestinationCommitPlanningInputs, DestinationCommitPlanningOutcome, DestinationDescription,
    DestinationDriver, DestinationHealthProbe, DestinationHealthResult, DestinationHealthStatus,
    DestinationIngressMode, DestinationInspection, DestinationPlanningContext,
    DestinationReceiptReportingPolicy, DestinationResolutionContext, DestinationRuntime,
    DestinationRuntimeCapabilities, DestinationWriterModel, PreparedDestinationCommit,
    artifact_hash, commit_request,
};

use crate::{
    MergeDedupPolicy, PostgresColumn, PostgresCommitRequest, PostgresCorrectionCommitRequest,
    PostgresCorrectionPlanInput, PostgresDestination, PostgresExistingTable, PostgresIdentifier,
    PostgresLoadPlanInput, PostgresTarget, binary_copy::BINARY_COPY_BUFFER_BYTES,
    postgres_columns_for_schema,
};

pub struct PostgresRuntimeDriver;

const POSTGRES_SCHEMES: &[&str] = &["postgres", "postgresql"];

impl DestinationDriver for PostgresRuntimeDriver {
    fn schemes(&self) -> &'static [&'static str] {
        POSTGRES_SCHEMES
    }

    fn inspect(
        &self,
        uri: &str,
        _context: &DestinationResolutionContext<'_>,
    ) -> Result<DestinationInspection> {
        validate_postgres_uri(uri)?;
        let destination = PostgresDestination::new();
        let sheet_artifact = destination.sheet_artifact()?;
        Ok(DestinationInspection {
            description: destination_description(&destination),
            sheet_artifact_hash: artifact_hash(&sheet_artifact)?,
            sheet_artifact,
            runtime: postgres_runtime_capabilities(),
            health_probes: vec![DestinationHealthProbe {
                probe_id: "connection".to_owned(),
                description: "connect and inspect Postgres catalog".to_owned(),
                requires_credentials: true,
                mutates_destination: false,
            }],
        })
    }

    fn resolve(
        &self,
        uri: &str,
        context: &DestinationResolutionContext<'_>,
    ) -> Result<Box<dyn DestinationRuntime>> {
        let raw = validate_postgres_uri(uri)?;
        let dedup = match context.policy_value("postgres", "merge_dedup") {
            Ok("fail") => MergeDedupPolicy::Fail,
            _ => {
                return Err(cdf_kernel::CdfError::contract(format!(
                    "Postgres cdf run requires [environments.{}.destination_policy.postgres] merge_dedup = \"fail\"",
                    context.environment_name()
                )));
            }
        };
        let (database_url, secret_redaction) = if raw.starts_with("secret://") {
            let secret = SecretUri::new(raw.to_owned())?;
            let value = context
                .secret_provider()?
                .resolve(&secret)?
                .as_str()?
                .to_owned();
            (value.clone(), Some(value))
        } else {
            (uri.to_owned(), None)
        };
        let target = PostgresTarget::parse(context.target()?.as_str())?;
        let destination = PostgresDestination::connect(database_url)?
            .with_execution_services(context.execution_services().cloned());
        Ok(Box::new(
            PostgresRuntime::for_replay(&destination, target, dedup, None)
                .with_secret_redaction(secret_redaction),
        ))
    }

    fn health(
        &self,
        uri: &str,
        _context: &DestinationResolutionContext<'_>,
    ) -> Result<Vec<DestinationHealthResult>> {
        validate_postgres_uri(uri)?;
        Ok(vec![DestinationHealthResult {
            probe_id: "destination".to_owned(),
            status: DestinationHealthStatus::Passed,
            message: "Postgres destination capabilities loaded".to_owned(),
            details: Default::default(),
        }])
    }

    fn replay_target(&self, target: &str) -> Result<TargetName> {
        TargetName::new(PostgresTarget::parse(target)?.display_name())
    }
}

pub struct PostgresRuntime {
    destination: PostgresDestination,
    replay: Option<PostgresReplayPlanning>,
    secret_redaction: Option<String>,
}

#[derive(Clone)]
struct PostgresReplayPlanning {
    target: PostgresTarget,
    dedup: MergeDedupPolicy,
    existing_table: Option<PostgresExistingTable>,
}

impl PostgresRuntime {
    pub fn for_replay(
        destination: &PostgresDestination,
        target: PostgresTarget,
        dedup: MergeDedupPolicy,
        existing_table: Option<PostgresExistingTable>,
    ) -> Self {
        Self {
            destination: destination.clone(),
            replay: Some(PostgresReplayPlanning {
                target,
                dedup,
                existing_table,
            }),
            secret_redaction: None,
        }
    }

    pub fn with_secret_redaction(mut self, secret_redaction: Option<String>) -> Self {
        self.secret_redaction = secret_redaction;
        self
    }
}

impl DestinationRuntime for PostgresRuntime {
    fn protocol(&self) -> &dyn DestinationProtocol {
        &self.destination
    }

    fn ingress(&mut self) -> cdf_runtime::DestinationIngress<'_> {
        cdf_runtime::DestinationIngress::FinalizedPackage(self)
    }

    fn describe(&self) -> DestinationDescription {
        destination_description(&self.destination)
    }

    fn runtime_capabilities(&self) -> DestinationRuntimeCapabilities {
        postgres_runtime_capabilities()
    }

    fn prepare_bulk_paths(
        &mut self,
        input: &cdf_runtime::BulkPathPreparationInput<'_>,
    ) -> Result<cdf_runtime::BulkPathPreparation> {
        postgres_columns_for_schema(input.output_schema)?;
        cdf_runtime::BulkPathPreparation::from_capabilities(&self.runtime_capabilities())
    }

    fn validate_run_preflight(
        &mut self,
        resource: &dyn ResourceStream,
        output_schema: &Schema,
        schema_hash: &SchemaHash,
    ) -> Result<()> {
        let replay = self.replay.as_ref().ok_or_else(|| {
            cdf_kernel::CdfError::internal("Postgres project run requires replay planning inputs")
        })?;
        let delta = postgres_preflight_delta(resource, schema_hash)?;
        let target = TargetName::new(replay.target.display_name())?;
        let commit = commit_request(
            &delta,
            target,
            resource.descriptor().write_disposition.clone(),
        )?;
        let inputs = PackageReplayInputs {
            input_checkpoint: None,
            state_delta: delta,
            destination_commit: commit,
            schema_hash: schema_hash.clone(),
            merge_keys: resource.descriptor().merge_key.clone(),
        };
        let input = load_plan_input_from_artifacts(
            &inputs,
            replay.target.clone(),
            replay.dedup.clone(),
            replay.existing_table.clone(),
            postgres_columns_for_schema(output_schema)?,
        )?;
        self.destination.plan_load(input)?;
        Ok(())
    }

    fn plan_resource_commit(
        &mut self,
        resource: &dyn ResourceStream,
        output_schema: &Schema,
        inputs: &DestinationCommitPlanningInputs,
    ) -> Result<DestinationCommitPlanningOutcome> {
        let replay = self.replay.as_ref().ok_or_else(|| {
            cdf_kernel::CdfError::internal("Postgres project planning requires replay inputs")
        })?;
        let replay_inputs = PackageReplayInputs {
            input_checkpoint: None,
            state_delta: inputs.state_delta.clone(),
            destination_commit: inputs.destination_commit.clone(),
            schema_hash: inputs.schema_hash.clone(),
            merge_keys: resource.descriptor().merge_key.clone(),
        };
        let load_input = load_plan_input_from_artifacts(
            &replay_inputs,
            replay.target.clone(),
            replay.dedup.clone(),
            replay.existing_table.clone(),
            postgres_columns_for_schema(output_schema)?,
        )?;
        let load_plan = self.destination.plan_load(load_input)?;
        Ok(DestinationCommitPlanningOutcome::new(
            self.destination.sheet().clone(),
            load_plan.kernel,
        ))
    }

    fn prepare_correction_commit(
        &mut self,
        package: cdf_package_contract::SharedVerifiedPackageAccess,
        request: &DestinationCorrectionCommitRequest,
    ) -> Result<DestinationCorrectionCommitPlan> {
        let replay = self.replay.as_ref().ok_or_else(|| {
            cdf_kernel::CdfError::internal("Postgres correction requires planning inputs")
        })?;
        if replay.target.display_name() != request.target.as_str() {
            return Err(cdf_kernel::CdfError::contract(
                "Postgres correction target does not match the resolved destination target",
            ));
        }
        let existing_table = self.destination.inspect_correction_target(&replay.target)?;
        let plan = self
            .destination
            .plan_addressed_correction(PostgresCorrectionPlanInput {
                request: request.clone(),
                existing_table,
            })?;
        let kernel = plan.kernel.clone();
        self.destination = self
            .destination
            .clone()
            .with_correction_request(PostgresCorrectionCommitRequest { package, plan });
        Ok(kernel)
    }

    fn secret_redaction(&self) -> Option<&str> {
        self.secret_redaction.as_deref()
    }
}

impl cdf_runtime::FinalizedPackageIngress for PostgresRuntime {
    fn prepare_package_commit(
        &mut self,
        inputs: &PackageReplayInputs,
        context: &DestinationPlanningContext<'_>,
    ) -> Result<PreparedDestinationCommit> {
        self.runtime_capabilities()
            .validate_prepared_bulk_path(context.bulk_path)?;
        let replay = self.replay.as_ref().ok_or_else(|| {
            cdf_kernel::CdfError::internal("Postgres package replay requires planning inputs")
        })?;
        let load_input = load_plan_input_from_artifacts(
            inputs,
            replay.target.clone(),
            replay.dedup.clone(),
            replay.existing_table.clone(),
            columns_from_package(context.verified_package.as_ref())?,
        )?;
        let load_plan = self.destination.plan_load(load_input)?;
        let segments = crate::package::expected_segments_for_session(
            context.verified_package.as_ref(),
            &load_plan,
            &inputs.destination_commit,
        )?;
        let request = PostgresCommitRequest {
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
        let request = prepared.take_pending_context::<PostgresCommitRequest>("Postgres")?;
        crate::commit::validate_session_begin_inputs(
            prepared.commit(),
            prepared.plan(),
            &request.plan,
        )?;
        let session = self.destination.begin_commit_session(request)?;
        match self.destination.execution.clone() {
            Some(execution) => Ok(Box::new(crate::commit::ManagedPostgresCommitSession::new(
                session, execution,
            ))),
            None => Ok(Box::new(session)),
        }
    }
}

fn destination_description(destination: &PostgresDestination) -> DestinationDescription {
    DestinationDescription::new(
        destination.sheet().destination.clone(),
        POSTGRES_SCHEMES,
        "postgres",
    )
}

fn postgres_runtime_capabilities() -> DestinationRuntimeCapabilities {
    DestinationRuntimeCapabilities {
        blocking_lanes: vec![cdf_runtime::BlockingLaneSpec {
            lane_id: "postgres.sync".to_owned(),
            binding: cdf_runtime::BlockingLaneBinding::Static,
            maximum_concurrency: 4,
            cpu_slot_cost: 1,
            native_internal_parallelism: 1,
            affinity: cdf_runtime::LaneAffinity::Shared,
            interruption: cdf_runtime::InterruptionSafety::CooperativeOnly,
        }],
        staged_ingress_lane: None,
        final_binding_lane: Some("postgres.sync".to_owned()),
        ingress_mode: DestinationIngressMode::FinalizedPackageOnly,
        staged_ingress: None,
        writer_model: DestinationWriterModel::SingleWriter,
        commit_payload_mode: cdf_runtime::DestinationCommitPayloadMode::SegmentStreaming,
        max_in_flight_segments: Some(1),
        max_in_flight_bytes: Some(64 * 1024 * 1024),
        bulk_paths: vec![cdf_runtime::BulkPathDescriptor {
            path_id: "copy_binary".to_owned(),
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
                minimum: BINARY_COPY_BUFFER_BYTES as u64,
                preferred: 16 * 1024 * 1024,
                maximum: 64 * 1024 * 1024,
            },
            max_useful_writers: 1,
            blocking_lane: Some("postgres.sync".to_owned()),
            native_internal_parallelism: 1,
            external_staging: true,
            fallback: cdf_runtime::BulkFallbackMode::Forbidden,
            schema_preflight_version: "postgres-binary-copy-mapping@2".to_owned(),
            measured_evidence_version: Some("p3-d3-2026-07-11-v1".to_owned()),
        }],
        bulk_path: Some("copy_binary".to_owned()),
        bulk_evidence_version: Some("p3-d3-2026-07-11-v1".to_owned()),
        replay_requires_explicit_target: true,
        replay_target_hint: Some("schema.table".to_owned()),
        replay_policy_values: [("merge_dedup".to_owned(), vec!["fail".to_owned()])]
            .into_iter()
            .collect(),
    }
}

fn validate_postgres_uri(uri: &str) -> Result<&str> {
    let raw = uri
        .strip_prefix("postgres://")
        .or_else(|| uri.strip_prefix("postgresql://"))
        .ok_or_else(|| {
        cdf_kernel::CdfError::contract(format!(
            "destination URI `{uri}` is unsupported; expected postgres://... or postgresql://..."
        ))
    })?;
    if raw.trim().is_empty() {
        return Err(cdf_kernel::CdfError::contract(
            "Postgres destination URI is malformed; expected postgres://database-url, postgresql://database-url, or postgres://secret://provider/key",
        ));
    }
    Ok(raw)
}

fn postgres_preflight_delta(
    resource: &dyn ResourceStream,
    schema_hash: &SchemaHash,
) -> Result<StateDelta> {
    let descriptor = resource.descriptor();
    let segment = StateSegment {
        segment_id: cdf_kernel::SegmentId::new("seg-postgres-preflight")?,
        scope: descriptor.state_scope.clone(),
        output_position: SourcePosition::Cursor(CursorPosition {
            version: 1,
            field: "preflight".to_owned(),
            value: CursorValue::I64(0),
        }),
        row_count: 1,
        byte_count: 1,
    };
    Ok(StateDelta {
        checkpoint_id: CheckpointId::new("checkpoint-postgres-preflight")?,
        pipeline_id: PipelineId::new("pipeline-postgres-preflight")?,
        resource_id: descriptor.resource_id.clone(),
        scope: descriptor.state_scope.clone(),
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: None,
        input_position: None,
        output_position: segment.output_position.clone(),
        output_watermark: None,
        partition_watermarks: Vec::new(),
        late_data_carryover: Vec::new(),
        source_continuation: None,
        package_hash: PackageHash::new("sha256:postgres-preflight")?,
        schema_hash: schema_hash.clone(),
        segments: vec![segment],
    })
}

fn load_plan_input_from_artifacts(
    inputs: &PackageReplayInputs,
    target: PostgresTarget,
    dedup: MergeDedupPolicy,
    existing_table: Option<PostgresExistingTable>,
    columns: Vec<PostgresColumn>,
) -> Result<PostgresLoadPlanInput> {
    validate_replay_target(&target, &inputs.destination_commit.target)?;
    Ok(PostgresLoadPlanInput {
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
            .map(PostgresIdentifier::user)
            .collect::<Result<Vec<_>>>()?,
        dedup,
        existing_table,
        resource_id: Some(inputs.state_delta.resource_id.clone()),
        state_delta: Some(inputs.state_delta.clone()),
    })
}

pub fn validate_replay_target(target: &PostgresTarget, package_target: &TargetName) -> Result<()> {
    let explicit = target.display_name();
    if explicit != package_target.as_str() {
        return Err(cdf_kernel::CdfError::contract(format!(
            "explicit Postgres replay target {explicit} does not match package destination commit target {package_target}"
        )));
    }
    Ok(())
}

fn columns_from_package(package: &dyn VerifiedPackageAccess) -> Result<Vec<PostgresColumn>> {
    let schema = package.runtime_arrow_schema()?;
    postgres_columns_for_schema(schema.as_ref())
}
