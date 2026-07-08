use super::{
    destinations::{
        commit_request, postgres_columns_from_schema, postgres_load_plan_input, postgres_target,
    },
    prelude::*,
    resources::ProjectRunResource,
    types::*,
};

pub(super) fn validate_project_run_request(request: &ProjectRunRequest<'_>) -> Result<()> {
    request.resource.validate_supported()?;
    validate_checkpointable_source_position(request.resource)?;
    validate_run_plan(
        request.resource.stream(),
        &request.plan,
        &request.package_id,
    )?;
    match &request.destination {
        ProjectRunDestination::DuckDb { database_path, .. } => {
            let destination = DuckDbDestination::new(database_path)?;
            if !destination
                .sheet()
                .supported_dispositions
                .contains(&request.resource.descriptor().write_disposition)
            {
                return Err(CdfError::contract(format!(
                    "DuckDB destination does not support {:?}",
                    request.resource.descriptor().write_disposition
                )));
            }
        }
        ProjectRunDestination::ParquetFilesystem { .. } => {
            if !matches!(
                request.resource.descriptor().write_disposition,
                WriteDisposition::Append | WriteDisposition::Replace
            ) {
                return Err(CdfError::contract(format!(
                    "Parquet destination does not support {:?}; append and replace are supported in this slice",
                    request.resource.descriptor().write_disposition
                )));
            }
        }
        ProjectRunDestination::Postgres { database_url, .. } => {
            PostgresDestination::connect(database_url.clone())?;
            validate_postgres_preflight(request)?;
        }
    }
    Ok(())
}

fn validate_postgres_preflight(request: &ProjectRunRequest<'_>) -> Result<()> {
    let resource = request.resource.stream();
    let schema_hash = declared_schema_hash(resource)?;
    let delta = postgres_preflight_delta(resource, &schema_hash)?;
    let commit = commit_request(
        &delta,
        postgres_target(request)?,
        resource.descriptor().write_disposition.clone(),
    )?;
    let replay = PackageReplayInputs {
        input_checkpoint: None,
        state_delta: delta,
        destination_commit: commit,
        schema_hash,
        merge_keys: Vec::new(),
    };
    let input =
        postgres_load_plan_input(request, &replay, postgres_columns_from_schema(resource)?)?;
    PostgresDestination::new().plan_load(input)?;
    Ok(())
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

fn validate_checkpointable_source_position(resource: ProjectRunResource<'_>) -> Result<()> {
    match resource {
        ProjectRunResource::LocalFile(_) => Ok(()),
        ProjectRunResource::Rest(_) | ProjectRunResource::Sql(_) => {
            let descriptor = resource.descriptor();
            let cursor = descriptor.cursor.as_ref().ok_or_else(|| {
                CdfError::contract(format!(
                    "cdf run requires non-file resource `{}` to declare an ordered cursor; page-token-only checkpoint semantics are not ratified",
                    descriptor.resource_id
                ))
            })?;
            if cursor.ordering == CursorOrderingClaim::Unordered {
                return Err(CdfError::contract(format!(
                    "cdf run requires non-file resource `{}` to declare an ordered cursor for checkpoint advancement",
                    descriptor.resource_id
                )));
            }
            Ok(())
        }
    }
}

pub(super) fn validate_local_file_run_resource(resource: &CompiledResource) -> Result<()> {
    match resource.plan() {
        CompiledResourcePlan::Files(_) => Ok(()),
        CompiledResourcePlan::Rest(_) => Err(CdfError::contract(
            "cdf run local-file resource input supports only declarative local file resources; use RestResource for REST execution",
        )),
        CompiledResourcePlan::Sql(_) => Err(CdfError::contract(
            "cdf run local-file resource input supports only declarative local file resources; use SqlResource for SQL execution",
        )),
    }
}

fn validate_run_plan(
    resource: &dyn ResourceStream,
    plan: &EnginePlan,
    package_id: &str,
) -> Result<()> {
    let descriptor = resource.descriptor();
    if plan.scan.request.resource_id != descriptor.resource_id {
        return Err(CdfError::contract(format!(
            "run plan resource {} does not match selected resource {}",
            plan.scan.request.resource_id, descriptor.resource_id
        )));
    }
    if plan.package_id != package_id {
        return Err(CdfError::contract(format!(
            "run plan package id {} does not match explicit package id {}",
            plan.package_id, package_id
        )));
    }
    if plan.scan.request.scope != descriptor.state_scope {
        return Err(CdfError::contract(
            "run plan scope must come from the current resource descriptor state scope",
        ));
    }
    Ok(())
}

pub(super) fn declared_schema_hash(resource: &dyn ResourceStream) -> Result<SchemaHash> {
    match &resource.descriptor().schema_source {
        SchemaSource::Declared { schema_hash, .. } => Ok(schema_hash.clone()),
        SchemaSource::Discovered { schema_hash: None } => Err(CdfError::contract(
            "cdf run requires a declared schema with a concrete schema hash; discovered schema resources are unsupported in this slice",
        )),
        SchemaSource::Discovered {
            schema_hash: Some(_),
        } => Err(CdfError::contract(
            "cdf run requires SchemaSource::Declared; discovered schema hashes are unsupported in this slice",
        )),
        SchemaSource::Contract { .. } => Err(CdfError::contract(
            "cdf run requires SchemaSource::Declared; contract-sourced schemas are unsupported in this slice",
        )),
    }
}

pub(super) fn refuse_existing_package_dir(package_dir: &Path) -> Result<()> {
    if package_dir.exists() {
        return Err(CdfError::data(format!(
            "package directory already exists at {}; explicit run package ids must not overwrite existing packages",
            package_dir.display()
        )));
    }
    Ok(())
}

pub(super) fn ensure_parent_directory(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| CdfError::internal(format!("create {}: {error}", parent.display())))?;
    }
    Ok(())
}

pub(super) fn validate_explicit_package_id(package_id: &str) -> Result<()> {
    if package_id.trim().is_empty() {
        return Err(CdfError::contract("run package id cannot be empty"));
    }
    let mut components = Path::new(package_id).components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(_)), None) => Ok(()),
        _ => Err(CdfError::contract(
            "run package id must be one path component under the environment package root",
        )),
    }
}
