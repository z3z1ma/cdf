use super::{
    destinations::ProjectDestinationDescription, prelude::*, resources::ProjectRunSource, types::*,
};

pub(super) fn validate_project_run_request(request: &mut ProjectRunRequest<'_>) -> Result<()> {
    request.resource.validate_supported()?;
    validate_checkpointable_source_position(request.resource)?;
    validate_run_plan(
        request.resource.stream(),
        &request.plan,
        &request.package_id,
    )?;
    let disposition = &request.resource.descriptor().write_disposition;
    let description = request.destination.describe();
    if !request
        .destination
        .runtime_mut()
        .supported_dispositions()
        .contains(disposition)
    {
        return Err(CdfError::contract(format!(
            "{} destination does not support {:?}",
            destination_validation_name(&description),
            disposition
        )));
    }
    let schema_hash = declared_schema_hash(request.resource.stream())?;
    request
        .destination
        .runtime_mut()
        .validate_run_preflight(request.resource.stream(), &schema_hash)?;
    Ok(())
}

fn destination_validation_name(description: &ProjectDestinationDescription) -> &str {
    match description.schemes.first().copied() {
        Some("duckdb") => "DuckDB",
        Some("parquet") => "Parquet",
        Some("postgres") => "Postgres",
        _ => description.label.as_str(),
    }
}

fn validate_checkpointable_source_position(resource: ProjectRunSource<'_>) -> Result<()> {
    if resource.capabilities().incremental == IncrementalShape::File {
        return Ok(());
    }
    let descriptor = resource.descriptor();
    let cursor = descriptor.cursor.as_ref().ok_or_else(|| {
        CdfError::contract(format!(
            "cdf run requires resource `{}` without file-incremental capability to declare an ordered cursor; page-token-only checkpoint semantics are not ratified",
            descriptor.resource_id
        ))
    })?;
    if cursor.ordering == CursorOrderingClaim::Unordered {
        return Err(CdfError::contract(format!(
            "cdf run requires resource `{}` without file-incremental capability to declare an ordered cursor for checkpoint advancement",
            descriptor.resource_id
        )));
    }
    Ok(())
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
