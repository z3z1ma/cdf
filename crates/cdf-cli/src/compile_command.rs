mod render;

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use cdf_kernel::CdfError;
use cdf_project::{
    CompiledProjectResource, LOCK_FILE_NAME, ManifestInputKind, ManifestSemanticSource,
    PROJECT_FILE_NAME, PROJECT_MANIFEST_RELATIVE_PATH, ProjectCompilationMode,
    ProjectFileExpectation, ProjectFileGuard, ProjectFileTransactionReport, ProjectFileWrite,
    ProjectManifestAuthoredInput, ProjectManifestCompileRequest, compile_project_manifest,
    current_dependency_tuple, finalize_query_project_resource,
    generate_lockfile_with_destination_artifacts, lock_to_toml,
    publish_project_files_transactionally_guarded,
    publish_project_files_transactionally_guarded_without_recovery,
};
use serde::Serialize;

use crate::{
    args::{Cli, CompileArgs},
    context::ProjectContext,
    output::{CliError, CommandOutput},
};

pub(crate) fn compile(cli: &Cli, args: CompileArgs) -> Result<CommandOutput, CliError> {
    if args.refresh {
        compile_refresh(cli)
    } else {
        compile_offline(cli)
    }
}

fn compile_offline(cli: &Cli) -> Result<CommandOutput, CliError> {
    let context =
        ProjectContext::load_for_command("compile", cli.project.as_ref(), cli.env.as_deref())?;
    let lock = context.lock.as_ref().ok_or_else(|| {
        CdfError::contract(format!(
            "{LOCK_FILE_NAME} is missing under {}",
            context.root.display()
        ))
    })?;
    let lock_bytes = context
        .lock_authority
        .as_ref()
        .ok_or_else(|| CdfError::internal("typed cdf.lock lost its byte authority"))?
        .bytes
        .clone();
    let entries = compiled_project_entries(&context)?;
    let authored = captured_authored_inputs(&context, &entries)?;
    let selected_destination_id =
        cdf_builtin_drivers::builtin_destination_id_for_uri(&context.environment.destination)?;
    let manifest = compile_project_manifest(ProjectManifestCompileRequest {
        config: &context.config,
        environment: &context.environment,
        lock,
        lock_bytes: &lock_bytes,
        resources: &entries,
        authored_inputs: manifest_inputs(&authored),
        semantic_catalog: &context.semantic_catalog,
        semantic_sources: BTreeMap::new(),
        selected_destination_id: &selected_destination_id,
        compilation_mode: ProjectCompilationMode::LockedOffline,
        generated_at_unix_ms: None,
        diagnostics: Vec::new(),
    })?;
    let prior_manifest = optional_public_file_bytes(
        &context.root.join(PROJECT_MANIFEST_RELATIVE_PATH),
        "project manifest",
    )?;
    let publication = publish_offline(
        &context.root,
        &manifest,
        lock_bytes,
        prior_manifest,
        &authored,
    )?;
    finish_report(
        &context,
        &manifest,
        ProjectCompilationMode::LockedOffline,
        0,
        &publication,
    )
}

fn compile_refresh(cli: &Cli) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load_for_command_with_recovery(
        "compile --refresh",
        cli.project.as_ref(),
        cli.env.as_deref(),
    )?;
    let (_, execution) = crate::commands::default_services(cli)?;
    let mut entries = compiled_project_entries(&context)?;
    let authored = captured_authored_inputs(&context, &entries)?;
    let mut artifact_files = BTreeMap::<String, Vec<u8>>::new();
    let mut source_observations = 0_usize;

    for entry in &mut entries {
        if entry
            .resource
            .descriptor()
            .schema_source
            .without_pinned_snapshot()
            .is_none()
        {
            continue;
        }
        let artifacts = crate::schema_command::discover_artifacts_for_cli(
            &context,
            &entry.resource,
            &execution,
        )?;
        source_observations = source_observations
            .checked_add(1)
            .ok_or_else(|| CdfError::internal("source observation count overflowed usize"))?;
        for (path, bytes) in artifacts.canonical_artifact_files()? {
            if let Some(previous) = artifact_files.insert(path.clone(), bytes.clone())
                && previous != bytes
            {
                return Err(CdfError::internal(format!(
                    "refresh produced conflicting bytes for schema artifact `{path}`"
                ))
                .into());
            }
        }
        let schema_source = entry
            .resource
            .descriptor()
            .schema_source
            .with_pinned_snapshot(artifacts.discovery.snapshot.reference.clone())
            .ok_or_else(|| {
                CdfError::internal("refreshable schema source rejected its discovered snapshot")
            })?;
        entry.resource = entry
            .resource
            .with_schema_source_and_schema(schema_source, artifacts.discovery.normalized_schema);
        *entry = finalize_query_project_resource(entry.clone(), &context.semantic_catalog)?;
    }

    let resources = entries
        .iter()
        .map(|entry| entry.resource.clone())
        .collect::<Vec<_>>();
    let destinations = crate::destination_registry::builtin_destination_registry()?;
    let destination_artifacts = crate::destination_registry::inspect_destination_artifacts(
        &destinations,
        &context,
        &context.environment.destination,
    )?;
    let lock = generate_lockfile_with_destination_artifacts(
        &context.config,
        &resources,
        current_dependency_tuple(),
        &destination_artifacts,
        BTreeMap::new(),
        &context.semantic_catalog,
    )?;
    let lock_bytes = lock_to_toml(&lock)?.into_bytes();
    let selected_destination_id =
        cdf_builtin_drivers::builtin_destination_id_for_uri(&context.environment.destination)?;
    let manifest = compile_project_manifest(ProjectManifestCompileRequest {
        config: &context.config,
        environment: &context.environment,
        lock: &lock,
        lock_bytes: &lock_bytes,
        resources: &entries,
        authored_inputs: manifest_inputs(&authored),
        semantic_catalog: &context.semantic_catalog,
        semantic_sources: BTreeMap::<String, ManifestSemanticSource>::new(),
        selected_destination_id: &selected_destination_id,
        compilation_mode: ProjectCompilationMode::Refresh,
        generated_at_unix_ms: None,
        diagnostics: Vec::new(),
    })?;
    let publication = publish_refresh(
        &context.root,
        &manifest,
        lock_bytes,
        context
            .lock_authority
            .as_ref()
            .map(|authority| authority.bytes.clone()),
        artifact_files,
        &authored,
    )?;
    finish_report(
        &context,
        &manifest,
        ProjectCompilationMode::Refresh,
        source_observations,
        &publication,
    )
}

fn compiled_project_entries(
    context: &ProjectContext,
) -> Result<Vec<CompiledProjectResource>, CliError> {
    let mut entries = context
        .resources
        .iter()
        .cloned()
        .zip(context.resource_queries.iter().cloned())
        .map(|(resource, query)| CompiledProjectResource { resource, query })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| {
        left.resource
            .descriptor()
            .resource_id
            .cmp(&right.resource.descriptor().resource_id)
    });
    for pair in entries.windows(2) {
        if pair[0].resource.descriptor().resource_id == pair[1].resource.descriptor().resource_id {
            return Err(CdfError::contract(format!(
                "project compiles resource `{}` more than once",
                pair[0].resource.descriptor().resource_id
            ))
            .into());
        }
    }
    Ok(entries)
}

struct CapturedAuthoredInput {
    path: String,
    manifest: ProjectManifestAuthoredInput,
    bytes: Vec<u8>,
}

fn captured_authored_inputs(
    context: &ProjectContext,
    entries: &[CompiledProjectResource],
) -> Result<Vec<CapturedAuthoredInput>, CliError> {
    let mut inputs = vec![captured_authored_input(
        PROJECT_FILE_NAME,
        ManifestInputKind::Project,
        context.project_bytes.clone(),
        "cdf-project-toml",
    )?];
    for entry in entries {
        inputs.push(captured_authored_input(
            &entry.query.relative_path,
            ManifestInputKind::ResourceSql,
            entry.query.authored_sql.as_bytes().to_vec(),
            "cdf-resource-sql",
        )?);
    }
    Ok(inputs)
}

fn captured_authored_input(
    path: &str,
    kind: ManifestInputKind,
    bytes: Vec<u8>,
    parser: &str,
) -> Result<CapturedAuthoredInput, CliError> {
    let path = path.to_owned();
    Ok(CapturedAuthoredInput {
        manifest: ProjectManifestAuthoredInput::explicit_file(&path, kind, &bytes, parser, 1)?,
        path,
        bytes,
    })
}

fn manifest_inputs(inputs: &[CapturedAuthoredInput]) -> Vec<ProjectManifestAuthoredInput> {
    inputs.iter().map(|input| input.manifest.clone()).collect()
}

fn authored_input_guards(inputs: &[CapturedAuthoredInput]) -> Vec<ProjectFileGuard> {
    inputs
        .iter()
        .map(|input| ProjectFileGuard::exact(&input.path, input.bytes.clone()))
        .collect()
}

fn publish_offline(
    root: &Path,
    manifest: &cdf_project::ProjectManifest,
    lock_bytes: Vec<u8>,
    prior_manifest: Option<Vec<u8>>,
    authored: &[CapturedAuthoredInput],
) -> Result<ProjectFileTransactionReport, CliError> {
    let guards = authored_input_guards(authored);
    let writes = vec![
        ProjectFileWrite::new(
            LOCK_FILE_NAME,
            lock_bytes.clone(),
            ProjectFileExpectation::Exact(lock_bytes),
        ),
        ProjectFileWrite::new(
            PROJECT_MANIFEST_RELATIVE_PATH,
            manifest.canonical_json_bytes()?,
            expectation(prior_manifest),
        )
        .owner_only(),
    ];
    Ok(
        publish_project_files_transactionally_guarded_without_recovery(
            root,
            PROJECT_MANIFEST_RELATIVE_PATH,
            guards,
            writes,
        )?,
    )
}

fn publish_refresh(
    root: &Path,
    manifest: &cdf_project::ProjectManifest,
    lock_bytes: Vec<u8>,
    prior_lock_bytes: Option<Vec<u8>>,
    artifacts: BTreeMap<String, Vec<u8>>,
    authored: &[CapturedAuthoredInput],
) -> Result<ProjectFileTransactionReport, CliError> {
    let guards = authored_input_guards(authored);
    let mut writes = artifacts
        .into_iter()
        .map(|(path, bytes)| {
            ProjectFileWrite::new(
                path,
                bytes.clone(),
                ProjectFileExpectation::AbsentOrExact(bytes),
            )
            .owner_only()
        })
        .collect::<Vec<_>>();
    let manifest_path = root.join(PROJECT_MANIFEST_RELATIVE_PATH);
    writes.push(
        ProjectFileWrite::new(
            PROJECT_MANIFEST_RELATIVE_PATH,
            manifest.canonical_json_bytes()?,
            expectation(optional_public_file_bytes(
                &manifest_path,
                "project manifest",
            )?),
        )
        .owner_only(),
    );
    writes.push(ProjectFileWrite::new(
        LOCK_FILE_NAME,
        lock_bytes,
        expectation(prior_lock_bytes),
    ));
    Ok(publish_project_files_transactionally_guarded(
        root,
        LOCK_FILE_NAME,
        guards,
        writes,
    )?)
}

fn expectation(previous: Option<Vec<u8>>) -> ProjectFileExpectation {
    previous.map_or(
        ProjectFileExpectation::Absent,
        ProjectFileExpectation::Exact,
    )
}

fn optional_public_file_bytes(path: &Path, label: &str) -> Result<Option<Vec<u8>>, CliError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
            Ok(Some(fs::read(path).map_err(|error| {
                crate::context::project_authority_read_error("read", path, error)
            })?))
        }
        Ok(_) => Err(CdfError::data(format!(
            "{label} {} must be a regular non-symlink file",
            path.display()
        ))
        .into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(crate::context::project_authority_read_error(
            &format!("inspect {label}"),
            path,
            error,
        )
        .into()),
    }
}

fn finish_report(
    context: &ProjectContext,
    manifest: &cdf_project::ProjectManifest,
    mode: ProjectCompilationMode,
    source_observations: usize,
    publication: &ProjectFileTransactionReport,
) -> Result<CommandOutput, CliError> {
    let manifest_path = PathBuf::from(PROJECT_MANIFEST_RELATIVE_PATH);
    let lock_path = PathBuf::from(LOCK_FILE_NAME);
    let report = CompileReport {
        project: context.config.project.name.clone(),
        environment: context.environment.name.clone(),
        mode,
        manifest_path: PROJECT_MANIFEST_RELATIVE_PATH.to_owned(),
        manifest_hash: manifest.manifest_hash.as_str().to_owned(),
        resources: manifest.resources.len(),
        semantic_definitions: manifest.semantics.len(),
        semantic_references: manifest
            .semantics
            .iter()
            .map(|definition| definition.references.len())
            .sum(),
        source_observations,
        writes: CompileWrites {
            manifest: publication.installed_paths.contains(&manifest_path),
            lockfile: publication.installed_paths.contains(&lock_path),
            schema_artifacts: publication
                .installed_paths
                .iter()
                .filter(|path| **path != manifest_path && **path != lock_path)
                .count(),
            destination: false,
            state: false,
            package: false,
            receipt: false,
            checkpoint: false,
        },
        next_command: "cdf sql \"select * from manifest_resources\"".to_owned(),
    };
    CommandOutput::rendered("compile", render::document(&report), report)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct CompileReport {
    project: String,
    environment: String,
    mode: ProjectCompilationMode,
    manifest_path: String,
    manifest_hash: String,
    resources: usize,
    semantic_definitions: usize,
    semantic_references: usize,
    source_observations: usize,
    writes: CompileWrites,
    next_command: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct CompileWrites {
    manifest: bool,
    lockfile: bool,
    schema_artifacts: usize,
    destination: bool,
    state: bool,
    package: bool,
    receipt: bool,
    checkpoint: bool,
}
