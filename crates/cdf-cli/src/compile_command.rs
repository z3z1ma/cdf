mod render;

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use cdf_kernel::CdfError;
use cdf_project::{
    CompiledProjectResource, LOCK_FILE_NAME, ManifestInputKind, ManifestSemanticSource,
    PROJECT_FILE_NAME, PROJECT_MANIFEST_RELATIVE_PATH, ProjectCompilationMode,
    ProjectFileExpectation, ProjectFileTransactionReport, ProjectFileWrite,
    ProjectManifestAuthoredInput, ProjectManifestCompileRequest, ProjectResourceOrigin,
    ResourceSourceKind, compile_project_manifest, current_dependency_tuple,
    generate_lockfile_with_destination_artifacts, lock_to_toml,
    publish_project_files_transactionally, publish_project_manifest,
};
use serde::Serialize;

use crate::{
    args::{Cli, CompileArgs},
    context::ProjectContext,
    output::{CliError, CommandOutput},
};

pub(crate) fn compile(cli: &Cli, args: CompileArgs) -> Result<CommandOutput, CliError> {
    let result = if args.refresh {
        compile_refresh(cli)
    } else {
        compile_offline(cli)
    };
    result.map_err(with_compile_remediation)
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
    let inputs = authored_inputs(&context.root, &entries)?;
    let selected_destination_id =
        cdf_builtin_drivers::builtin_destination_id_for_uri(&context.environment.destination)?;
    let manifest = compile_project_manifest(ProjectManifestCompileRequest {
        config: &context.config,
        environment: &context.environment,
        lock,
        lock_bytes: &lock_bytes,
        resources: &entries,
        authored_inputs: inputs,
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
    let publication =
        publish_project_manifest(&context.root, &manifest, lock, lock_bytes, prior_manifest)?;
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
        authored_inputs: authored_inputs(&context.root, &entries)?,
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
        .zip(context.resource_origins.iter().cloned())
        .map(|(resource, origin)| CompiledProjectResource { resource, origin })
        .collect::<Vec<_>>();
    for (resource_id, mapping) in &context.config.resources {
        if !matches!(mapping.source_kind(), ResourceSourceKind::Reference { .. }) {
            continue;
        }
        let resource = crate::project_run_resource::build_project_resource_for_inspection(
            context,
            resource_id,
        )?
        .ok_or_else(|| CdfError::internal("source reference did not compile"))?;
        entries.push(CompiledProjectResource {
            origin: ProjectResourceOrigin {
                source_name: resource.source_name().to_owned(),
                resource_name: resource.resource_name().to_owned(),
                source_file: None,
                mapping_pattern: resource_id.clone(),
                mapping_status: "matched".to_owned(),
            },
            resource,
        });
    }
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

fn authored_inputs(
    root: &Path,
    entries: &[CompiledProjectResource],
) -> Result<Vec<ProjectManifestAuthoredInput>, CliError> {
    let mut inputs = vec![ProjectManifestAuthoredInput::explicit_file(
        PROJECT_FILE_NAME,
        ManifestInputKind::Project,
        &read_project_input(root, PROJECT_FILE_NAME)?,
        "cdf-project-toml",
        1,
    )?];
    let source_files = entries
        .iter()
        .filter_map(|entry| entry.origin.source_file.as_deref())
        .collect::<BTreeSet<_>>();
    for source_file in source_files {
        let bytes = read_project_input(root, source_file)?;
        let parser = match Path::new(source_file)
            .extension()
            .and_then(|extension| extension.to_str())
        {
            Some("yaml" | "yml") => "cdf-declarative-yaml",
            _ => "cdf-declarative-toml",
        };
        inputs.push(ProjectManifestAuthoredInput::explicit_file(
            source_file,
            ManifestInputKind::Declarative,
            &bytes,
            parser,
            1,
        )?);
    }
    Ok(inputs)
}

fn read_project_input(root: &Path, relative: &str) -> Result<Vec<u8>, CliError> {
    let relative_path = Path::new(relative);
    if relative_path.is_absolute()
        || relative_path
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return Err(CdfError::contract(format!(
            "project compiler input `{relative}` must be a normalized project-relative path"
        ))
        .into());
    }
    let path = root.join(relative_path);
    let canonical_root = fs::canonicalize(root).map_err(|error| {
        crate::context::project_authority_read_error("resolve project root", root, error)
    })?;
    let canonical_path = fs::canonicalize(&path).map_err(|error| {
        crate::context::project_authority_read_error("resolve project compiler input", &path, error)
    })?;
    if !canonical_path.starts_with(&canonical_root) {
        return Err(CdfError::contract(format!(
            "project compiler input `{relative}` resolves outside the project root"
        ))
        .into());
    }
    let metadata = fs::metadata(&canonical_path).map_err(|error| {
        crate::context::project_authority_read_error(
            "inspect project compiler input",
            &canonical_path,
            error,
        )
    })?;
    if !metadata.is_file() {
        return Err(CdfError::contract(format!(
            "project compiler input `{relative}` must resolve to a regular file"
        ))
        .into());
    }
    fs::read(&canonical_path).map_err(|error| {
        crate::context::project_authority_read_error(
            "read project compiler input",
            &canonical_path,
            error,
        )
        .into()
    })
}

fn publish_refresh(
    root: &Path,
    manifest: &cdf_project::ProjectManifest,
    lock_bytes: Vec<u8>,
    prior_lock_bytes: Option<Vec<u8>>,
    artifacts: BTreeMap<String, Vec<u8>>,
) -> Result<ProjectFileTransactionReport, CliError> {
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
    Ok(publish_project_files_transactionally(
        root,
        LOCK_FILE_NAME,
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

fn with_compile_remediation(mut error: CliError) -> CliError {
    if matches!(
        error.kind,
        cdf_kernel::ErrorKind::Contract | cdf_kernel::ErrorKind::Data
    ) && !error.message.contains("cdf compile --refresh")
    {
        error
            .message
            .push_str("; run `cdf compile --refresh` to refresh locked source authority");
    }
    error
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
