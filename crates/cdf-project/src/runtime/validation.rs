use super::{resources::ProjectRunSource, types::ProjectRunRequest};
use cdf_contract::{ObservedSchema, normalize_schema};
use cdf_engine::EnginePlan;
use cdf_kernel::{CdfError, CursorOrderingClaim, ResourceStream, Result};
use cdf_state_sqlite::StateStorePathOwnership;
use std::{
    fs,
    path::{Component, Path, PathBuf},
};

pub(super) fn validate_project_run_request(request: &mut ProjectRunRequest<'_>) -> Result<()> {
    request.resource.validate_supported()?;
    validate_checkpointable_source_position(request.resource, &request.plan)?;
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
    request
        .destination
        .validate_output_schema_mappings(request.resource.stream(), output.schema.as_ref())?;
    request.destination.runtime_mut().validate_run_preflight(
        request.resource.stream(),
        output.schema.as_ref(),
        &output.schema_hash,
    )?;
    Ok(())
}

fn validate_checkpointable_source_position(
    resource: ProjectRunSource<'_>,
    plan: &EnginePlan,
) -> Result<()> {
    let descriptor = resource.descriptor();
    if let Some(cursor) = descriptor.cursor.as_ref() {
        if cursor.ordering == CursorOrderingClaim::Unordered {
            return Err(CdfError::contract(format!(
                "resource `{}` declares cursor `{}` but its ordering is unproven; use an ordered cursor or remove it from this bounded resource",
                descriptor.resource_id, cursor.field
            )));
        }
        return Ok(());
    }
    if plan.execution_extent.is_bounded() {
        return Ok(());
    }
    let has_non_cursor_frontier = plan
        .compiled_source_execution
        .as_ref()
        .and_then(|source| source.stream_capabilities())
        .is_some_and(|stream| {
            stream.source_frontiers.iter().any(|frontier| {
                !matches!(
                    frontier,
                    cdf_runtime::SourceFrontierCapability::Cursor { .. }
                        | cdf_runtime::SourceFrontierCapability::PageToken
                )
            })
        });
    if has_non_cursor_frontier {
        return Ok(());
    }
    Err(CdfError::contract(format!(
        "drain execution for resource `{}` requires an ordered declared cursor or a compiled source frontier such as a log, resume token, file manifest, snapshot, or foreign state; bounded runs and replace disposition do not inherently require cursors",
        descriptor.resource_id
    )))
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

    let relational_output = plan
        .relational_expression_plan
        .as_ref()
        .map(|relational| relational.output_schema.to_arrow())
        .transpose()?;
    let resource_schema = resource.schema();
    let observed_schema = relational_output
        .as_ref()
        .unwrap_or(resource_schema.as_ref());
    let observed = ObservedSchema::from_arrow(observed_schema);
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
    match fs::symlink_metadata(package_dir) {
        Ok(_) => Err(CdfError::data(format!(
            "package directory already exists at {}; explicit run package ids must not overwrite existing packages",
            package_dir.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            validate_absent_package_ancestors(package_dir)
        }
        Err(error)
            if error.kind() == std::io::ErrorKind::NotADirectory
                || cdf_kernel::is_filesystem_loop(&error) =>
        {
            Err(CdfError::data(format!(
                "package directory {} has an invalid filesystem shape: {error}",
                package_dir.display()
            )))
        }
        Err(error) => Err(CdfError::environment(format!(
            "inspect package directory {}: {error}; check path permissions, device availability, and process file limits before retrying",
            package_dir.display()
        ))),
    }
}

pub(super) fn package_directory_exists(package_dir: &Path) -> Result<bool> {
    match fs::symlink_metadata(package_dir) {
        Ok(metadata) if metadata.is_dir() => Ok(true),
        Ok(_) => Err(CdfError::data(format!(
            "package directory {} is not a real directory",
            package_dir.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            validate_absent_package_ancestors(package_dir)?;
            Ok(false)
        }
        Err(error)
            if error.kind() == std::io::ErrorKind::NotADirectory
                || cdf_kernel::is_filesystem_loop(&error) =>
        {
            Err(CdfError::data(format!(
                "package directory {} has an invalid filesystem shape: {error}",
                package_dir.display()
            )))
        }
        Err(error) => Err(CdfError::environment(format!(
            "inspect package directory {}: {error}; check path permissions, device availability, and process file limits before retrying",
            package_dir.display()
        ))),
    }
}

fn validate_absent_package_ancestors(package_dir: &Path) -> Result<()> {
    let mut cursor = package_dir.parent();
    while let Some(parent) = cursor {
        if parent.as_os_str().is_empty() {
            return Ok(());
        }
        match fs::metadata(parent) {
            Ok(metadata) if !metadata.is_dir() => {
                return Err(CdfError::data(format!(
                    "package directory ancestor {} is not a real directory",
                    parent.display()
                )));
            }
            Ok(_) => return Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match fs::symlink_metadata(parent) {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        return Err(CdfError::data(format!(
                            "package directory ancestor {} is a dangling symlink",
                            parent.display()
                        )));
                    }
                    Ok(_) => {
                        return Err(CdfError::data(format!(
                            "package directory ancestor {} changed filesystem shape during inspection",
                            parent.display()
                        )));
                    }
                    Err(link_error) if link_error.kind() == std::io::ErrorKind::NotFound => {
                        cursor = parent.parent();
                    }
                    Err(link_error)
                        if link_error.kind() == std::io::ErrorKind::NotADirectory
                            || cdf_kernel::is_filesystem_loop(&link_error) =>
                    {
                        return Err(CdfError::data(format!(
                            "package directory ancestor {} has an invalid filesystem shape: {link_error}",
                            parent.display()
                        )));
                    }
                    Err(link_error) => {
                        return Err(CdfError::environment(format!(
                            "inspect package directory ancestor {}: {link_error}; check path permissions, device availability, and process file limits before retrying",
                            parent.display()
                        )));
                    }
                }
            }
            Err(error)
                if error.kind() == std::io::ErrorKind::NotADirectory
                    || cdf_kernel::is_filesystem_loop(&error) =>
            {
                return Err(CdfError::data(format!(
                    "package directory ancestor {} has an invalid filesystem shape: {error}",
                    parent.display()
                )));
            }
            Err(error) => {
                return Err(CdfError::environment(format!(
                    "inspect package directory ancestor {}: {error}; check path permissions, device availability, and process file limits before retrying",
                    parent.display()
                )));
            }
        }
    }
    Ok(())
}

pub fn ensure_parent_directory(path: &Path, ownership: StateStorePathOwnership) -> Result<()> {
    if let Some(parent) = path.parent() {
        match ownership {
            StateStorePathOwnership::CdfManaged => {
                let trusted_base = managed_state_trusted_base(parent).ok_or_else(|| {
                    CdfError::internal(format!(
                        "CDF-managed state-store parent {} has no .cdf authority boundary",
                        parent.display()
                    ))
                })?;
                ensure_managed_state_parent_from(&trusted_base, parent)?;
            }
            StateStorePathOwnership::Configured => ensure_configured_state_parent(parent)?,
        }
    }
    Ok(())
}

fn managed_state_trusted_base(parent: &Path) -> Option<PathBuf> {
    let mut trusted_base = PathBuf::new();
    for component in parent.components() {
        if component.as_os_str() == ".cdf" {
            return Some(trusted_base);
        }
        trusted_base.push(component.as_os_str());
    }
    None
}

fn ensure_configured_state_parent(parent: &Path) -> Result<()> {
    let mut missing = Vec::new();
    let mut cursor = Some(parent);
    let existing = loop {
        let path = cursor.ok_or_else(|| {
            CdfError::internal(format!(
                "configured state-store parent {} has no existing filesystem ancestor",
                parent.display()
            ))
        })?;
        if path.as_os_str().is_empty() {
            break Path::new(".");
        }
        match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => break path,
            Ok(metadata) if metadata.file_type().is_symlink() => match fs::metadata(path) {
                Ok(target) if target.is_dir() => break path,
                Ok(_) => {
                    return Err(CdfError::contract(format!(
                        "configured state-store parent {} resolves to a non-directory",
                        path.display()
                    )));
                }
                Err(error) => return Err(configured_state_parent_error(path, error)),
            },
            Ok(_) => {
                return Err(CdfError::contract(format!(
                    "configured state-store parent {} is not a directory",
                    path.display()
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let component = path.file_name().ok_or_else(|| {
                    CdfError::internal(format!(
                        "derive missing configured state-store component from {}",
                        path.display()
                    ))
                })?;
                missing.push(component.to_os_string());
                cursor = path.parent();
            }
            Err(error) => return Err(configured_state_parent_error(path, error)),
        }
    };
    let mut current = fs::canonicalize(existing)
        .map_err(|error| configured_state_parent_error(existing, error))?;
    for component in missing.into_iter().rev() {
        current.push(component);
        match fs::create_dir(&current) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let metadata = fs::symlink_metadata(&current)
                    .map_err(|error| configured_state_parent_error(&current, error))?;
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    return Err(CdfError::contract(format!(
                        "configured state-store parent {} changed to a non-directory or symlink during creation",
                        current.display()
                    )));
                }
            }
            Err(error) => return Err(configured_state_parent_error(&current, error)),
        }
    }
    Ok(())
}

fn ensure_managed_state_parent_from(trusted_base: &Path, parent: &Path) -> Result<()> {
    let relative = parent.strip_prefix(trusted_base).map_err(|error| {
        CdfError::internal(format!(
            "derive CDF-managed state-store ancestry for {}: {error}",
            parent.display()
        ))
    })?;
    let mut current = trusted_base.to_path_buf();
    for component in relative.components() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Ok(_) => {
                return Err(CdfError::internal(format!(
                    "CDF-managed state-store parent {} is not a real directory",
                    current.display()
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match fs::create_dir(&current) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        let metadata = fs::symlink_metadata(&current)
                            .map_err(|error| state_store_parent_error(&current, error))?;
                        if metadata.file_type().is_symlink() || !metadata.is_dir() {
                            return Err(CdfError::internal(format!(
                                "CDF-managed state-store parent {} changed to a non-directory or symlink during creation",
                                current.display()
                            )));
                        }
                    }
                    Err(error) => return Err(state_store_parent_error(&current, error)),
                }
            }
            Err(error) => return Err(state_store_parent_error(&current, error)),
        }
    }
    Ok(())
}

fn state_store_parent_error(parent: &Path, error: std::io::Error) -> CdfError {
    if matches!(
        error.kind(),
        std::io::ErrorKind::NotFound
            | std::io::ErrorKind::NotADirectory
            | std::io::ErrorKind::IsADirectory
            | std::io::ErrorKind::AlreadyExists
            | std::io::ErrorKind::InvalidInput
            | std::io::ErrorKind::InvalidData
    ) || cdf_kernel::is_filesystem_loop(&error)
    {
        CdfError::internal(format!(
            "CDF-managed state-store parent {} has an invalid filesystem shape: {error}",
            parent.display()
        ))
    } else {
        CdfError::environment(format!(
            "create state-store parent directory {}: {error}; check path permissions, free space, device availability, and process file limits before retrying",
            parent.display()
        ))
    }
}

fn configured_state_parent_error(parent: &Path, error: std::io::Error) -> CdfError {
    if matches!(
        error.kind(),
        std::io::ErrorKind::NotFound
            | std::io::ErrorKind::NotADirectory
            | std::io::ErrorKind::IsADirectory
            | std::io::ErrorKind::AlreadyExists
            | std::io::ErrorKind::InvalidInput
            | std::io::ErrorKind::InvalidData
    ) || cdf_kernel::is_filesystem_loop(&error)
    {
        CdfError::contract(format!(
            "configured state-store parent {} has an invalid filesystem shape: {error}",
            parent.display()
        ))
    } else {
        CdfError::environment(format!(
            "create configured state-store parent directory {}: {error}; check path permissions, free space, device availability, and process file limits before retrying",
            parent.display()
        ))
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_store_parent_wrong_shape_is_internal() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join(".cdf");
        fs::write(&parent, b"not a directory").unwrap();

        let error = ensure_parent_directory(
            &parent.join("state.db"),
            StateStorePathOwnership::CdfManaged,
        )
        .unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Internal);
        assert!(error.message.contains("state-store parent"));
    }

    #[test]
    fn configured_state_store_parent_wrong_shape_is_contract_owned() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("custom/.cdf");
        fs::create_dir(root.path().join("custom")).unwrap();
        fs::write(&parent, b"not a directory").unwrap();

        let error = ensure_parent_directory(
            &parent.join("state.db"),
            StateStorePathOwnership::Configured,
        )
        .unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
        assert!(error.message.contains("configured state-store parent"));
    }

    #[test]
    fn state_store_parent_host_failure_is_environment_owned() {
        let error = state_store_parent_error(
            Path::new(".cdf/state"),
            std::io::Error::from(std::io::ErrorKind::PermissionDenied),
        );

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Environment);
        assert!(error.message.contains("state-store parent"));
    }

    #[test]
    fn package_parent_wrong_shape_is_data_owned() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("packages");
        fs::write(&parent, b"not a directory").unwrap();

        let error = refuse_existing_package_dir(&parent.join("run")).unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    }

    #[cfg(unix)]
    #[test]
    fn dangling_package_directory_symlink_is_data_owned() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let package_dir = root.path().join("run");
        symlink(root.path().join("missing"), &package_dir).unwrap();

        let error = refuse_existing_package_dir(&package_dir).unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
        assert!(error.message.contains("already exists"));
    }

    #[cfg(unix)]
    #[test]
    fn dangling_package_directory_ancestor_is_data_owned() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let package_root = root.path().join("packages");
        symlink(root.path().join("missing"), &package_root).unwrap();

        let error = refuse_existing_package_dir(&package_root.join("run")).unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
        assert!(error.message.contains("ancestor"));
    }

    #[cfg(unix)]
    #[test]
    fn state_store_parent_rejects_symlink_ancestor_without_writing_outside() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let managed = root.path().join(".cdf");
        symlink(outside.path(), &managed).unwrap();

        let error = ensure_parent_directory(
            &managed.join("state/state.db"),
            StateStorePathOwnership::CdfManaged,
        )
        .unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Internal);
        assert!(!outside.path().join("state").exists());
    }

    #[cfg(unix)]
    #[test]
    fn state_store_parent_rejects_live_symlink_ancestor_with_existing_subtree() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        fs::create_dir(outside.path().join("state")).unwrap();
        let managed = root.path().join(".cdf");
        symlink(outside.path(), &managed).unwrap();

        let error = ensure_parent_directory(
            &managed.join("state/state.db"),
            StateStorePathOwnership::CdfManaged,
        )
        .unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Internal);
        assert!(!outside.path().join("state/state.db").exists());
    }

    #[cfg(unix)]
    #[test]
    fn custom_state_parent_resolves_configured_symlink_before_creating() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        fs::create_dir(outside.path().join("existing")).unwrap();
        let configured_parent = root.path().join("custom");
        fs::create_dir(&configured_parent).unwrap();
        let link = configured_parent.join(".cdf");
        symlink(outside.path(), &link).unwrap();

        ensure_parent_directory(
            &link.join("existing/missing/state.db"),
            StateStorePathOwnership::Configured,
        )
        .unwrap();

        assert!(outside.path().join("existing/missing").is_dir());
    }
}
