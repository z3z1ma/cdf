use super::{prelude::*, resources::ProjectRunSource, types::*};
use cdf_contract::{ObservedSchema, normalize_schema};

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
            description.destination_id, disposition
        )));
    }
    let output = request.destination.output_schema(&request.plan)?;
    request.destination.runtime_mut().validate_run_preflight(
        request.resource.stream(),
        output.schema.as_ref(),
        &output.schema_hash,
    )?;
    Ok(())
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

fn validate_run_plan(
    resource: &dyn ResourceStream,
    plan: &EnginePlan,
    package_id: &str,
) -> Result<()> {
    plan.validate_compiled_expression_plan()?;
    plan.validate_partition_schedule()?;
    plan.validate_compiled_source_resource(resource)?;
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
    validate_normalization_program(resource, plan)?;
    cdf_engine::validate_plan_schema_authority(resource, plan)?;
    if plan.scan.request.scope != descriptor.state_scope {
        return Err(CdfError::contract(
            "run plan scope must come from the current resource descriptor state scope",
        ));
    }
    Ok(())
}

fn validate_normalization_program(resource: &dyn ResourceStream, plan: &EnginePlan) -> Result<()> {
    let program = &plan.validation_program;
    if program.normalizer_version != program.identifier_policy.version {
        return Err(CdfError::contract(format!(
            "run plan normalization program is stale: normalizer_version {:?} does not match identifier policy version {:?}; rebuild the plan for the selected destination",
            program.normalizer_version, program.identifier_policy.version
        )));
    }

    let observed = ObservedSchema::from_arrow(resource.schema().as_ref());
    let expected = normalize_schema(&observed, &program.identifier_policy)?;
    if program.column_programs.len() != expected.fields.len() {
        return Err(CdfError::contract(format!(
            "run plan normalization program is stale: planned {} columns but resource schema has {}; rebuild the plan for the selected destination",
            program.column_programs.len(),
            expected.fields.len()
        )));
    }

    for (index, (planned, expected)) in program
        .column_programs
        .iter()
        .zip(expected.fields.iter())
        .enumerate()
    {
        if planned.source_name != expected.source_name
            || planned.output_name != expected.output_name
        {
            return Err(CdfError::contract(format!(
                "run plan normalization program is stale at column {index}: resource source {:?} must normalize to {:?} under the serialized identifier policy, but the plan names source {:?} and output {:?}; rebuild the plan for the selected destination",
                expected.source_name,
                expected.output_name,
                planned.source_name,
                planned.output_name
            )));
        }
    }

    Ok(())
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
