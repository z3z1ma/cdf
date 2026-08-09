use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use cdf_kernel::{CdfError, CheckpointId, PipelineId};
use cdf_project::{
    PortableDestinationBinding, PortableInlineArtifact, PortablePlanArtifact, PortablePlanResource,
    PortableSchemaAuthority,
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
    prepared_authority: &crate::schema_authority::PreparedSchemaAuthority,
    engine_plan: cdf_engine::EnginePlan,
    resolved: &EnvironmentDestination,
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
        prepared_authority,
        sheet.clone(),
    )?;
    let schema_authority = match prepared_authority {
        crate::schema_authority::PreparedSchemaAuthority::Proposed { establishment } => {
            PortableSchemaAuthority::ProposedFirstUse {
                key: establishment.key.clone(),
                version: Box::new(establishment.version.clone()),
                artifacts: prepared
                    .schema_artifact_files
                    .iter()
                    .map(|(path, bytes)| PortableInlineArtifact::new(path, bytes.clone()))
                    .collect::<cdf_kernel::Result<Vec<_>>>()?,
            }
        }
        crate::schema_authority::PreparedSchemaAuthority::Active { head } => {
            PortableSchemaAuthority::Active {
                authority: cdf_project::CompiledSchemaAuthority::from_head(head)?,
            }
        }
    };
    let source_plan_hash = prepared
        .resource
        .source_plan()
        .compiled_source_plan_hash()?
        .to_string();
    let destination = PortableDestinationBinding {
        uri: destination_uri.to_owned(),
        configuration_hash: cdf_runtime::artifact_hash(&(
            destination_uri,
            &context.environment.destination_policy,
            resolved.destination.target(),
        ))?,
        destination_id,
        sheet_hash,
        sheet,
        runtime_capabilities: resolved.destination.runtime_capabilities(),
        target: resolved.destination.target().clone(),
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
    })
}

pub(crate) fn build_artifact(
    root: &Path,
    config: &cdf_project::ProjectConfig,
    environment: &cdf_project::EffectiveEnvironment,
    selection: cdf_project::ProjectResourceSelection,
    materials: Vec<PortablePlanResourceMaterial>,
) -> Result<PortablePlanArtifact, CliError> {
    let resources = materials
        .into_iter()
        .map(|material| material.resource)
        .collect::<Vec<_>>();
    preflight_state(root, environment, &resources)?;
    for resource in &resources {
        cdf_project::validate_compiled_resource_artifact_current(
            root,
            config,
            environment,
            &resource.compiled_resource,
            &resource.schema_authority.compiled_authority(),
        )?;
    }
    PortablePlanArtifact::new(
        env!("CARGO_PKG_VERSION"),
        config.project.id.clone(),
        config.project.name.clone(),
        environment.name.clone(),
        cdf_project::effective_environment_binding_hash(environment)?,
        selection,
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

pub(crate) fn establish_portable_authority(
    context: &crate::context::ProjectCompilationContext,
    artifact: &PortablePlanArtifact,
) -> Result<bool, CliError> {
    preflight_state(&context.root, &context.environment, &artifact.resources)?;
    let first = artifact
        .resources
        .first()
        .ok_or_else(|| CdfError::data("portable plan must contain at least one resource"))?;
    let domain = first.schema_authority.key().authority_domain_id.clone();
    let state_path = context.state_store_path()?;
    let ownership = context.state_store_path_ownership();
    cdf_project::ensure_state_parent_directory(&state_path, ownership)?;
    let store = cdf_state_sqlite::SqliteSchemaAuthorityStore::open_with_authority_domain_and_path_ownership(
        state_path,
        &domain,
        ownership,
    )?;
    let checks = artifact
        .resources
        .iter()
        .map(|resource| {
            cdf_kernel::SchemaAuthorityCheck::new(
                resource.schema_authority.key().clone(),
                resource.schema_authority.precondition(),
            )
        })
        .collect::<cdf_kernel::Result<Vec<_>>>()?;
    let proposals = artifact
        .resources
        .iter()
        .filter_map(|resource| {
            resource.schema_authority.proposed_version().map(|version| {
                cdf_kernel::SchemaAuthorityEstablishment::new(
                    resource.schema_authority.key().clone(),
                    version.clone(),
                )
            })
        })
        .collect::<cdf_kernel::Result<Vec<_>>>()?;
    let changed = !proposals.is_empty();
    cdf_kernel::SchemaAuthorityStore::establish_batch_checked(&store, checks, proposals)?;
    Ok(changed)
}

pub(crate) fn preflight_portable_authority(
    context: &crate::context::ProjectCompilationContext,
    artifact: &PortablePlanArtifact,
) -> Result<(), CliError> {
    preflight_state(&context.root, &context.environment, &artifact.resources)
}

fn preflight_state(
    root: &Path,
    environment: &cdf_project::EffectiveEnvironment,
    resources: &[PortablePlanResource],
) -> Result<(), CliError> {
    use cdf_kernel::SchemaAuthorityStore as _;

    let path = crate::context::sqlite_uri_path(root, &environment.state)?;
    let ownership = crate::context::state_store_path_ownership(&environment.state);
    let state = cdf_state_sqlite::SqliteSchemaAuthorityStore::inspect_state(&path, ownership)?;
    let ready_store = match &state {
        cdf_state_sqlite::SqliteSchemaAuthorityState::Ready { .. } => Some(
            cdf_state_sqlite::SqliteSchemaAuthorityStore::open_read_only_with_path_ownership(
                &path, ownership,
            )?,
        ),
        _ => None,
    };
    for resource in resources {
        let expected = &resource.schema_authority;
        let observed_domain = match &state {
            cdf_state_sqlite::SqliteSchemaAuthorityState::Missing => None,
            cdf_state_sqlite::SqliteSchemaAuthorityState::Uninitialized {
                authority_domain_id,
            } => authority_domain_id.as_ref(),
            cdf_state_sqlite::SqliteSchemaAuthorityState::Ready {
                authority_domain_id,
            } => Some(authority_domain_id),
        };
        if observed_domain.is_some_and(|domain| domain != &expected.key().authority_domain_id) {
            return Err(replan_error(
                &resource.resource_id,
                "the state authority domain changed",
            ));
        }
        let current = ready_store
            .as_ref()
            .map(|store| store.head(expected.key()))
            .transpose()?
            .flatten();
        match (expected.precondition(), current) {
            (cdf_kernel::SchemaAuthorityPrecondition::Absent, None) => {}
            (
                cdf_kernel::SchemaAuthorityPrecondition::Exact {
                    generation,
                    schema_hash,
                },
                Some(head),
            ) if head.generation == generation
                && head.schema_hash == schema_hash
                && matches!(head.status, cdf_kernel::SchemaHeadStatus::Active) => {}
            _ => {
                return Err(replan_error(
                    &resource.resource_id,
                    "its state-backed schema authority changed",
                ));
            }
        }
    }
    Ok(())
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
