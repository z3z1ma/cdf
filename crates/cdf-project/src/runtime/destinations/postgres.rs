use super::*;
use crate::{PostgresMergeDedupPolicy, SecretRef};

pub struct PostgresProjectDestinationDriver;

impl ProjectDestinationDriver for PostgresProjectDestinationDriver {
    fn schemes(&self) -> &'static [&'static str] {
        &["postgres"]
    }

    fn resolve(
        &self,
        uri: &str,
        context: &ProjectResolutionContext<'_>,
    ) -> Result<Box<dyn ProjectDestinationRuntime>> {
        let raw = uri.strip_prefix("postgres://").ok_or_else(|| {
            CdfError::contract(format!(
                "destination URI `{uri}` is unsupported; expected postgres://..."
            ))
        })?;
        if raw.trim().is_empty() {
            return Err(CdfError::contract(
                "Postgres destination URI is malformed; expected postgres://database-url or postgres://secret://provider/key",
            ));
        }
        let policy = context
            .destination_policy()?
            .postgres
            .as_ref()
            .ok_or_else(|| {
                CdfError::contract(format!(
                    "Postgres cdf run requires [environments.{}.destination_policy.postgres] merge_dedup = \"fail\"",
                    context.environment_name()
                ))
            })?;
        let dedup = match policy.merge_dedup {
            PostgresMergeDedupPolicy::Fail => MergeDedupPolicy::Fail,
        };
        let (database_url, secret_redaction) = if raw.starts_with("secret://") {
            let secret = SecretRef::new(raw.to_owned())?;
            let value = context
                .secret_provider()?
                .resolve(&secret.to_secret_uri()?)?
                .as_str()?
                .to_owned();
            (value.clone(), Some(value))
        } else {
            (uri.to_owned(), None)
        };
        let target = PostgresTarget::parse(context.target()?.as_str())?;
        let destination = PostgresDestination::connect(database_url)?;
        Ok(Box::new(
            PostgresProjectDestinationRuntime::for_replay(&destination, target, dedup, None)
                .with_secret_redaction(secret_redaction),
        ))
    }
}

pub(crate) struct PostgresProjectDestinationRuntime {
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

impl PostgresProjectDestinationRuntime {
    pub(crate) fn for_replay(
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

    pub(crate) fn for_recovery(destination: &PostgresDestination) -> Self {
        Self {
            destination: destination.clone(),
            replay: None,
            secret_redaction: None,
        }
    }

    pub(crate) fn with_secret_redaction(mut self, secret_redaction: Option<String>) -> Self {
        self.secret_redaction = secret_redaction;
        self
    }
}

impl ProjectDestinationRuntime for PostgresProjectDestinationRuntime {
    fn protocol(&self) -> &dyn DestinationProtocol {
        &self.destination
    }

    fn describe(&self) -> ProjectDestinationDescription {
        ProjectDestinationDescription {
            destination_id: self.destination.sheet().destination.clone(),
            schemes: &["postgres"],
            label: "postgres".to_owned(),
        }
    }

    fn validate_run_preflight(
        &mut self,
        resource: &dyn ResourceStream,
        schema_hash: &SchemaHash,
    ) -> Result<()> {
        let replay = self.replay.as_ref().ok_or_else(|| {
            CdfError::internal("Postgres project run requires replay planning inputs")
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
        let input = postgres_load_plan_input_from_artifacts(
            &inputs,
            replay.target.clone(),
            replay.dedup.clone(),
            replay.existing_table.clone(),
            postgres_columns_from_schema(resource)?,
        )?;
        self.destination.plan_load(input)?;
        Ok(())
    }

    fn prepare_package_commit(
        &mut self,
        package_dir: &Path,
        reader: &PackageReader,
        inputs: &PackageReplayInputs,
        _context: &DestinationPlanningContext<'_>,
    ) -> Result<PreparedDestinationCommit> {
        let replay = self.replay.as_ref().ok_or_else(|| {
            CdfError::internal("Postgres package replay requires replay planning inputs")
        })?;
        let load_input = postgres_load_plan_input_from_artifacts(
            inputs,
            replay.target.clone(),
            replay.dedup.clone(),
            replay.existing_table.clone(),
            postgres_columns_from_package(reader)?,
        )?;
        let load_plan = self.destination.plan_load(load_input)?;
        let request = PostgresCommitRequest {
            package_dir: package_dir.to_path_buf(),
            plan: load_plan.clone(),
        };
        Ok(PreparedDestinationCommit::new(
            inputs.destination_commit.clone(),
            load_plan.kernel,
            DestinationReceiptReportingPolicy::DestinationCommitReceiptOnly,
        )
        .with_pending_context(request))
    }

    fn bind_prepared_commit(&mut self, prepared: &mut PreparedDestinationCommit) -> Result<()> {
        let request = prepared.take_pending_context::<PostgresCommitRequest>("Postgres")?;
        self.destination = self.destination.clone().with_commit_request(request);
        Ok(())
    }

    fn secret_redaction(&self) -> Option<&str> {
        self.secret_redaction.as_deref()
    }
}

fn postgres_preflight_delta(
    resource: &dyn ResourceStream,
    schema_hash: &SchemaHash,
) -> Result<StateDelta> {
    let descriptor = resource.descriptor();
    let segment = StateSegment {
        segment_id: SegmentId::new("seg-postgres-preflight")?,
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
        package_hash: PackageHash::new("sha256:postgres-preflight")?,
        schema_hash: schema_hash.clone(),
        segments: vec![segment],
    })
}

fn postgres_load_plan_input_from_artifacts(
    inputs: &PackageReplayInputs,
    target: PostgresTarget,
    dedup: MergeDedupPolicy,
    existing_table: Option<PostgresExistingTable>,
    columns: Vec<PostgresColumn>,
) -> Result<PostgresLoadPlanInput> {
    validate_postgres_replay_target(&target, &inputs.destination_commit.target)?;
    Ok(PostgresLoadPlanInput {
        package_hash: inputs.state_delta.package_hash.clone(),
        idempotency_token: inputs.destination_commit.idempotency_token.clone(),
        target,
        disposition: inputs.destination_commit.disposition.clone(),
        schema_hash: inputs.schema_hash.clone(),
        segments: inputs.state_delta.segments.clone(),
        columns,
        merge_keys: postgres_merge_keys_from_artifacts(&inputs.merge_keys)?,
        dedup,
        existing_table,
        resource_id: Some(inputs.state_delta.resource_id.clone()),
        state_delta: Some(inputs.state_delta.clone()),
    })
}

pub(crate) fn validate_postgres_replay_target(
    target: &PostgresTarget,
    package_target: &TargetName,
) -> Result<()> {
    let explicit = target.display_name();
    if explicit != package_target.as_str() {
        return Err(CdfError::contract(format!(
            "explicit Postgres replay target {explicit} does not match package destination commit target {package_target}"
        )));
    }
    Ok(())
}

fn postgres_merge_keys_from_artifacts(keys: &[String]) -> Result<Vec<PostgresIdentifier>> {
    keys.iter().map(PostgresIdentifier::user).collect()
}

fn postgres_columns_from_schema(resource: &dyn ResourceStream) -> Result<Vec<PostgresColumn>> {
    postgres_columns_for_schema(resource.schema().as_ref())
}

fn postgres_columns_from_package(reader: &PackageReader) -> Result<Vec<PostgresColumn>> {
    let segments = reader.read_all_segments()?;
    let schema = segments
        .iter()
        .flat_map(|(_, batches)| batches.iter())
        .next()
        .map(|batch| batch.schema())
        .ok_or_else(|| {
            CdfError::data("Postgres destination requires at least one package batch")
        })?;
    postgres_columns_for_schema(schema.as_ref())
}
