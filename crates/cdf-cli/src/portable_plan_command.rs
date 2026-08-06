use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use cdf_kernel::{CdfError, CheckpointId, PipelineId, TargetName};
use cdf_project::{
    CdfLock, LOCK_FILE_NAME, PortableDestinationBinding, PortableInlineArtifact,
    PortablePlanArtifact, PortablePlanResource, PortableSchemaAuthority,
};
use serde::Serialize;

use crate::{
    context::{ProjectContext, project_authority_read_error},
    destination_uri::EnvironmentDestination,
    output::CliError,
    project_run_resource::PreparedRuntimeResourceForCli,
    run_command::DEFAULT_RUN_PIPELINE_ID,
};

pub(crate) struct PortablePlanResourceMaterial {
    pub(crate) resource: PortablePlanResource,
    proposed_lock: Option<CdfLock>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct PortablePlanWriteReport {
    pub(crate) path: String,
    pub(crate) plan_hash: String,
    pub(crate) bytes: u64,
    pub(crate) resources: usize,
    pub(crate) status: PortablePlanWriteStatus,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PortablePlanWriteStatus {
    Created,
    Unchanged,
}

pub(crate) fn build_resource_material(
    context: &ProjectContext,
    prepared: &PreparedRuntimeResourceForCli,
    engine_plan: cdf_engine::EnginePlan,
    resolved: &EnvironmentDestination,
    target: &TargetName,
    destination_uri: &str,
    artifact_root: &Path,
) -> Result<PortablePlanResourceMaterial, CliError> {
    let source_task_set = engine_plan
        .scan
        .external_task_set()
        .map(|reference| {
            let path = artifact_root
                .join(".cdf")
                .join(reference.store_namespace.as_str())
                .join(reference.object_key.as_str());
            let bytes =
                read_optional_regular_file(&path, "planned source task set")?.ok_or_else(|| {
                    CdfError::data(format!(
                        "planned source task set {} disappeared before portable export",
                        path.display()
                    ))
                })?;
            cdf_project::PortableTaskSetArtifact::new(reference.clone(), &bytes)
                .map_err(CliError::from)
        })
        .transpose()?;
    let sources = crate::source_registry::builtin_source_registry()?;
    sources.validate_inline_portable_source_plan(prepared.resource.source_plan())?;
    let sheet = resolved.destination.destination_sheet_artifact()?;
    let sheet_hash = cdf_runtime::artifact_hash(&sheet)?;
    let destination_id = resolved.destination.describe().destination_id.to_string();
    let compilation = crate::compile_command::prepare_portable_compilation(
        context,
        &prepared.compiled_resource,
        &destination_id,
        sheet.clone(),
    )?;
    let schema_authority = match compilation.proposed_lock.as_ref() {
        Some(_) => PortableSchemaAuthority::ProposedFirstUse {
            lock_binding: compilation.lock_binding,
            artifacts: prepared
                .schema_artifact_files
                .iter()
                .map(|(path, bytes)| PortableInlineArtifact::new(path, bytes.clone()))
                .collect::<cdf_kernel::Result<Vec<_>>>()?,
        },
        None => PortableSchemaAuthority::Locked {
            lock_binding: compilation.lock_binding,
        },
    };
    let source_plan_hash = cdf_runtime::artifact_hash(prepared.resource.source_plan())?;
    let destination = PortableDestinationBinding {
        uri: destination_uri.to_owned(),
        configuration_hash: cdf_runtime::artifact_hash(&(
            destination_uri,
            &context.environment.destination_policy,
            target,
        ))?,
        destination_id,
        sheet_hash,
        sheet,
        runtime_capabilities: resolved.destination.runtime_capabilities(),
        target: target.clone(),
    };
    let resource_id = prepared
        .compiled_resource
        .descriptor()
        .resource_id
        .to_string();
    Ok(PortablePlanResourceMaterial {
        resource: PortablePlanResource {
            resource_id: resource_id.clone(),
            schema_authority,
            compiled_resource: compilation.artifact,
            compiled_source_plan_hash: source_plan_hash,
            source_task_set,
            input_checkpoint_head: engine_plan.initial_committed_frontier.clone(),
            engine_plan,
            destination,
            pipeline_id: PipelineId::new(DEFAULT_RUN_PIPELINE_ID)?,
            checkpoint_id: CheckpointId::new(format!("portable-plan-{resource_id}"))?,
        },
        proposed_lock: compilation.proposed_lock,
    })
}

pub(crate) fn build_artifact(
    root: &Path,
    config: &cdf_project::ProjectConfig,
    environment: &cdf_project::EffectiveEnvironment,
    selection: cdf_project::ProjectResourceSelection,
    materials: Vec<PortablePlanResourceMaterial>,
) -> Result<PortablePlanArtifact, CliError> {
    let lock_bytes = read_optional_regular_file(&root.join(LOCK_FILE_NAME), "cdf.lock")?;
    let current_lock = lock_bytes
        .as_deref()
        .map(|bytes| {
            std::str::from_utf8(bytes)
                .map_err(|error| CdfError::data(format!("cdf.lock is not UTF-8: {error}")))
                .and_then(cdf_project::parse_lock)
        })
        .transpose()?;
    let mut proposed_lock = None;
    let mut resources = Vec::with_capacity(materials.len());
    for material in materials {
        let resource_id = material.resource.resource_id.clone();
        let current_binding = current_lock
            .as_ref()
            .and_then(|lock| lock.resources.get(&resource_id));
        match &material.resource.schema_authority {
            PortableSchemaAuthority::Locked { .. } if current_binding.is_none() => {
                return Err(replan_error(&resource_id, "its lock entry disappeared"));
            }
            PortableSchemaAuthority::ProposedFirstUse { .. } if current_binding.is_some() => {
                return Err(replan_error(
                    &resource_id,
                    "a lock entry appeared during planning",
                ));
            }
            _ => {}
        }
        cdf_project::validate_compiled_resource_artifact_current(
            root,
            config,
            environment,
            &material.resource.compiled_resource,
            current_binding,
        )?;
        if let Some(candidate) = material.proposed_lock {
            validate_candidate_base(current_lock.as_ref(), &candidate, &resource_id)?;
            let final_lock = proposed_lock.get_or_insert_with(|| candidate.clone());
            final_lock.resources.insert(
                resource_id.clone(),
                candidate.resources[&resource_id].clone(),
            );
        }
        resources.push(material.resource);
    }
    PortablePlanArtifact::new(
        env!("CARGO_PKG_VERSION"),
        config.project.name.clone(),
        environment.name.clone(),
        cdf_project::effective_environment_binding_hash(environment)?,
        selection,
        cdf_project::portable_plan_lock_precondition(lock_bytes.as_deref()),
        proposed_lock,
        resources,
    )
    .map_err(Into::into)
}

pub(crate) fn publish_artifact(
    path: &Path,
    artifact: &PortablePlanArtifact,
) -> Result<PortablePlanWriteReport, CliError> {
    let bytes = artifact.canonical_json_bytes()?;
    let status = match read_optional_regular_file(path, "portable plan output")? {
        Some(existing) if existing == bytes => PortablePlanWriteStatus::Unchanged,
        Some(_) => {
            return Err(CdfError::contract(format!(
                "portable plan output {} already exists with different content; choose a new --out path",
                path.display()
            ))
            .into());
        }
        None => {
            publish_new_file(path, &bytes)?;
            PortablePlanWriteStatus::Created
        }
    };
    Ok(PortablePlanWriteReport {
        path: crate::render::redaction::redact_uri_userinfo(path.display().to_string()),
        plan_hash: artifact.plan_hash.clone(),
        bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        resources: artifact.resources.len(),
        status,
    })
}

pub(crate) fn load_artifact(path: &Path) -> Result<PortablePlanArtifact, CliError> {
    let bytes = read_optional_regular_file(path, "portable plan")?.ok_or_else(|| {
        CdfError::contract(format!("portable plan {} does not exist", path.display()))
    })?;
    cdf_project::parse_portable_plan(&bytes).map_err(Into::into)
}

pub(crate) fn current_lock_matches(
    root: &Path,
    precondition: &cdf_project::PortableLockPrecondition,
) -> Result<Option<Vec<u8>>, CliError> {
    let bytes = read_optional_regular_file(&root.join(LOCK_FILE_NAME), "cdf.lock")?;
    let current = cdf_project::portable_plan_lock_precondition(bytes.as_deref());
    if &current != precondition {
        return Err(CdfError::contract(
            "cdf.lock changed after the portable plan was created; run `cdf plan <selector> --out <path>` again",
        )
        .into());
    }
    Ok(bytes)
}

pub(crate) fn publish_proposed_authority(
    context: &crate::context::ProjectCompilationContext,
    artifact: &PortablePlanArtifact,
) -> Result<bool, CliError> {
    let Some(proposed_lock) = artifact.proposed_lock.as_ref() else {
        return Ok(false);
    };
    let current_lock = current_lock_matches(&context.root, &artifact.lock_precondition)?;
    let index_path = context
        .root
        .join(cdf_project::COMPILATION_INDEX_RELATIVE_PATH);
    let current_index = read_optional_regular_file(&index_path, "compilation index")?;
    let mut next_index = context.compilation.index.clone();
    let mut writes = Vec::new();
    let mut guards = BTreeMap::<String, Vec<u8>>::new();
    for resource in artifact
        .resources
        .iter()
        .filter(|resource| resource.schema_authority.is_proposed_first_use())
    {
        for inline in resource.schema_authority.inline_artifacts() {
            writes.push(
                cdf_project::ProjectFileWrite::new(
                    &inline.path,
                    inline.content.as_bytes(),
                    cdf_project::ProjectFileExpectation::AbsentOrExact(
                        inline.content.as_bytes().to_vec(),
                    ),
                )
                .owner_only(),
            );
        }
        let compiled_path = cdf_project::compiled_resource_artifact_path(
            &resource.resource_id,
            &resource.compiled_resource.artifact_hash,
        )?;
        let compiled_bytes = resource.compiled_resource.canonical_json_bytes()?;
        writes.push(
            cdf_project::ProjectFileWrite::new(
                compiled_path,
                compiled_bytes.clone(),
                cdf_project::ProjectFileExpectation::AbsentOrExact(compiled_bytes),
            )
            .owner_only(),
        );
        next_index.record_current(&resource.compiled_resource)?;
        for input in &resource.compiled_resource.inputs {
            if let cdf_project::ManifestInputLocation::ProjectRelativePath { path } =
                &input.location
            {
                let bytes = fs::read(context.root.join(path)).map_err(|error| {
                    project_authority_read_error(
                        "read portable plan authored input guard",
                        &context.root.join(path),
                        error,
                    )
                })?;
                guards.insert(path.clone(), bytes);
            }
        }
    }
    let next_index_bytes = next_index.canonical_json_bytes()?;
    writes.push(cdf_project::ProjectFileWrite::new(
        cdf_project::COMPILATION_INDEX_RELATIVE_PATH,
        next_index_bytes,
        current_index.map_or(
            cdf_project::ProjectFileExpectation::Absent,
            cdf_project::ProjectFileExpectation::Exact,
        ),
    ));
    let lock_bytes = cdf_project::lock_to_toml(proposed_lock)?.into_bytes();
    writes.push(cdf_project::ProjectFileWrite::new(
        cdf_project::LOCK_FILE_NAME,
        lock_bytes,
        current_lock.map_or(
            cdf_project::ProjectFileExpectation::Absent,
            cdf_project::ProjectFileExpectation::Exact,
        ),
    ));
    let guards = guards
        .into_iter()
        .map(|(path, bytes)| cdf_project::ProjectFileGuard::exact(path, bytes))
        .collect();
    cdf_project::publish_project_files_transactionally_guarded(
        &context.root,
        cdf_project::LOCK_FILE_NAME,
        guards,
        writes,
    )?;
    Ok(true)
}

fn validate_candidate_base(
    current: Option<&CdfLock>,
    candidate: &CdfLock,
    candidate_resource: &str,
) -> Result<(), CliError> {
    let mut base = candidate.clone();
    base.resources.remove(candidate_resource);
    match current {
        Some(current) if &base == current => Ok(()),
        None if base.resources.is_empty() => Ok(()),
        _ => Err(replan_error(
            candidate_resource,
            "its proposed lock was built from different project authority",
        )),
    }
}

fn replan_error(resource_id: &str, reason: &str) -> CliError {
    CdfError::contract(format!(
        "portable plan resource `{resource_id}` is stale because {reason}; run `cdf plan {resource_id} --out <path>` again"
    ))
    .into()
}

fn read_optional_regular_file(path: &Path, label: &str) -> Result<Option<Vec<u8>>, CliError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
            if metadata.len() > cdf_project::PORTABLE_PLAN_MAX_BYTES as u64 {
                return Err(CdfError::data(format!(
                    "{label} {} exceeds the {}-byte bound",
                    path.display(),
                    cdf_project::PORTABLE_PLAN_MAX_BYTES
                ))
                .into());
            }
            fs::read(path)
                .map(Some)
                .map_err(|error| project_authority_read_error("read", path, error).into())
        }
        Ok(_) => Err(CdfError::data(format!(
            "{label} {} must be a regular non-symlink file",
            path.display()
        ))
        .into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(project_authority_read_error("inspect", path, error).into()),
    }
}

fn publish_new_file(path: &Path, bytes: &[u8]) -> Result<(), CliError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    if !parent.is_dir() {
        return Err(CdfError::environment(format!(
            "portable plan output parent {} does not exist or is not a directory; create it and retry",
            parent.display()
        ))
        .into());
    }
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| CdfError::contract("portable plan output requires a UTF-8 file name"))?;
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            CdfError::environment(format!("read clock for portable plan publication: {error}"))
        })?
        .as_nanos();
    let temporary = parent.join(format!(".{name}.{}.{}.tmp", std::process::id(), stamp));
    let result: Result<(), CdfError> = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|error| {
                project_authority_read_error("create portable plan temporary", &temporary, error)
            })?;
        file.write_all(bytes)
            .and_then(|()| file.sync_all())
            .map_err(|error| {
                project_authority_read_error("write portable plan temporary", &temporary, error)
            })?;
        fs::hard_link(&temporary, path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                CdfError::contract(format!(
                    "portable plan output {} appeared concurrently; retry with a new --out path",
                    path.display()
                ))
            } else {
                project_authority_read_error("publish portable plan", path, error)
            }
        })?;
        Ok(())
    })();
    let _ = fs::remove_file(&temporary);
    result.map_err(Into::into)
}
