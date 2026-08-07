mod render;

use std::{collections::BTreeMap, fs, path::Path};

use cdf_kernel::{CdfError, ErrorKind};
use cdf_project::{
    COMPILATION_INDEX_RELATIVE_PATH, CompilationDiagnostic, CompilationIndex,
    CompiledArtifactInput, CompiledProjectResource, CompiledResourceArtifactRequest,
    ManifestInputKind, ManifestSemanticSource, PROJECT_FILE_NAME, ProjectFileExpectation,
    ProjectFileGuard, ProjectFileWrite, ProjectResourcePath, ProjectResourceSelectionError,
    compile_resource_artifact, compiled_resource_artifact_path, finalize_query_project_resource,
    parse_cdf_toml, parse_compilation_index, publish_project_files_transactionally_guarded,
    resolve_project_resource_selection, validate_compilation_index_authority,
};
use serde::Serialize;

use crate::{
    args::{Cli, CompileArgs},
    context::{ProjectContext, project_authority_read_error, project_location},
    output::{CliError, CommandOutput},
    suggestions,
};

pub(crate) fn compile(
    cli: &Cli,
    args: CompileArgs,
    destinations: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    let (root, project_file) = project_location(cli.project.as_ref())?;
    let project_bytes = fs::read(&project_file).map_err(|error| {
        project_authority_read_error("read project configuration", &project_file, error)
    })?;
    let config = parse_cdf_toml(std::str::from_utf8(&project_bytes).map_err(|error| {
        CdfError::contract(format!("project configuration is not UTF-8: {error}"))
    })?)?;
    let environment_name = cli
        .env
        .as_deref()
        .unwrap_or(&config.project.default_environment);
    let environment = config.effective_environment(environment_name)?;
    let selection = resolve_project_resource_selection(&root, &args.selectors, &args.exclude)
        .map_err(|error| resource_selection_error("cdf compile", error))?;
    let (_, execution) = crate::commands::default_services(cli)?;
    crate::schema_authority::reset_proposal_seed();
    let mut results = Vec::with_capacity(selection.resources.len());

    for selected in &selection.resources {
        let result = compile_one(cli, selected, args.locked, destinations, &execution);
        match result {
            Ok(success) => results.push(success),
            Err(error) => {
                if !args.locked {
                    let _ = record_failed_index_entry(
                        &root,
                        &config,
                        &environment,
                        selected,
                        &project_bytes,
                        &error,
                    );
                }
                results.push(CompileResourceReport {
                    resource_id: selected.resource_id.to_string(),
                    path: selected.relative_path.clone(),
                    status: CompileResourceStatus::Failed,
                    artifact_path: None,
                    artifact_hash: None,
                    discovered_schema: false,
                    schema_authority: None,
                    error: Some(CompileResourceError {
                        code: error.code,
                        kind: error_kind_name(&error.kind).to_owned(),
                        message: error.message,
                    }),
                });
            }
        }
    }
    if args.selectors.is_empty() {
        let reconciliation = reconcile_absent_index_entries(
            &root,
            &config,
            &environment,
            &selection.selection.resolved,
            &project_bytes,
        );
        if let Err(error) = reconciliation
            && results
                .iter()
                .all(|result| result.status == CompileResourceStatus::Compiled)
        {
            return Err(error);
        }
    }

    let succeeded = results
        .iter()
        .filter(|result| result.status == CompileResourceStatus::Compiled)
        .count();
    let failed = results.len() - succeeded;
    let report = CompileReport {
        project: config.project.name,
        environment: environment.name,
        locked: args.locked,
        selection: selection.selection.resolved,
        counts: CompileCounts {
            selected: results.len(),
            compiled: succeeded,
            failed,
        },
        resources: results,
        index_path: COMPILATION_INDEX_RELATIVE_PATH.to_owned(),
        next_command: "cdf plan <selector>".to_owned(),
    };
    CommandOutput::rendered_with_exit_code(
        "compile",
        render::document(&report),
        &report,
        i32::from(failed != 0),
    )
}

fn compile_one(
    cli: &Cli,
    selected: &ProjectResourcePath,
    locked_only: bool,
    destinations: &cdf_runtime::DestinationRegistry,
    execution: &cdf_runtime::ExecutionServices,
) -> Result<CompileResourceReport, CliError> {
    let context = ProjectContext::load_selected_for_mutation(
        cli.project.as_ref(),
        cli.env.as_deref(),
        selected.resource_id.as_str(),
        destinations,
    )?;
    let mut entry = compiled_entry(&context, selected.resource_id.as_str())?;
    let prepared = crate::scan_command::prepare_resource_schema_for_cli(
        &context,
        &entry.resource,
        false,
        Some(execution),
        &context.root,
    )?;
    let discovered_schema = prepared.schema_snapshot.is_some();
    let schema_files = prepared
        .schema_artifact_files
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    entry.resource = prepared.resource;
    entry = finalize_query_project_resource(entry, &context.semantic_catalog)?;

    let (_, destination_artifacts) =
        crate::destination_registry::inspect_destination_artifacts_and_id(
            destinations,
            &context,
            &context.environment.destination,
        )?;
    let [destination] = destination_artifacts.as_slice() else {
        return Err(CdfError::internal(
            "destination inspection did not produce exactly one capability sheet",
        )
        .into());
    };
    let schema_authority = crate::schema_authority::prepare(&context, &entry.resource)?;
    if locked_only && schema_authority.proposal().is_some() {
        return Err(CdfError::contract(format!(
            "resource `{}` has no established state-backed schema authority; run `cdf compile {}` first",
            selected.resource_id, selected.resource_id
        ))
        .into());
    }

    let authored = captured_authored_inputs(&entry)?;
    let artifact = compile_resource_artifact(CompiledResourceArtifactRequest {
        config: &context.config,
        environment: &context.environment,
        schema_authority: schema_authority.compiled_authority()?,
        resource: &entry,
        authored_inputs: authored
            .iter()
            .map(|input| input.manifest.clone())
            .collect(),
        semantic_catalog: &context.semantic_catalog,
        semantic_sources: BTreeMap::<String, ManifestSemanticSource>::new(),
        destination,
        diagnostics: Vec::new(),
    })?;
    let compiled_authority = schema_authority.compiled_authority()?;
    let artifact_path =
        compiled_resource_artifact_path(selected.resource_id.as_str(), &artifact.artifact_hash)?;
    publish_success(&context, &artifact, &artifact_path, schema_files, &authored)?;
    crate::schema_authority::commit_one_idempotent(&context, &schema_authority)?;
    Ok(CompileResourceReport {
        resource_id: selected.resource_id.to_string(),
        path: selected.relative_path.clone(),
        status: CompileResourceStatus::Compiled,
        artifact_path: Some(artifact_path),
        artifact_hash: Some(artifact.artifact_hash),
        discovered_schema,
        schema_authority: Some(CompileSchemaAuthorityReport {
            status: if schema_authority.proposal().is_some() {
                "established"
            } else {
                "active"
            },
            generation: compiled_authority.generation,
            schema_hash: compiled_authority.schema_hash.to_string(),
        }),
        error: None,
    })
}

pub(crate) struct PortableCompilationMaterial {
    pub(crate) artifact: cdf_project::CompiledResourceArtifact,
}

pub(crate) fn prepare_portable_compilation(
    context: &ProjectContext,
    resource: &cdf_declarative::CompiledResource,
    schema_authority: &crate::schema_authority::PreparedSchemaAuthority,
    destination_sheet: cdf_kernel::DestinationSheetArtifact,
) -> Result<PortableCompilationMaterial, CliError> {
    let resource_id = resource.descriptor().resource_id.as_str();
    let mut entry = compiled_entry(context, resource_id)?;
    entry.resource = resource.clone();
    entry.query.relational_plan = resource.relational_expression_plan().cloned();
    let authored = captured_authored_inputs(&entry)?;
    let artifact = compile_resource_artifact(CompiledResourceArtifactRequest {
        config: &context.config,
        environment: &context.environment,
        schema_authority: schema_authority.compiled_authority()?,
        resource: &entry,
        authored_inputs: authored
            .iter()
            .map(|input| input.manifest.clone())
            .collect(),
        semantic_catalog: &context.semantic_catalog,
        semantic_sources: BTreeMap::<String, ManifestSemanticSource>::new(),
        destination: &destination_sheet,
        diagnostics: Vec::new(),
    })?;
    Ok(PortableCompilationMaterial { artifact })
}

fn compiled_entry(
    context: &ProjectContext,
    resource_id: &str,
) -> Result<CompiledProjectResource, CliError> {
    let resource = context.resource(resource_id)?.clone();
    let query = context
        .resource_query(resource_id)
        .cloned()
        .ok_or_else(|| {
            CdfError::internal(format!(
                "selected resource `{resource_id}` lost its query compilation"
            ))
        })?;
    Ok(CompiledProjectResource { resource, query })
}

struct CapturedInput {
    path: String,
    bytes: Vec<u8>,
    manifest: CompiledArtifactInput,
}

fn captured_authored_inputs(
    entry: &CompiledProjectResource,
) -> Result<Vec<CapturedInput>, CliError> {
    Ok(vec![captured_input(
        &entry.query.relative_path,
        ManifestInputKind::ResourceSql,
        entry.query.authored_sql.as_bytes().to_vec(),
        "cdf-resource-sql",
    )?])
}

fn captured_input(
    path: &str,
    kind: ManifestInputKind,
    bytes: Vec<u8>,
    parser: &str,
) -> Result<CapturedInput, CliError> {
    Ok(CapturedInput {
        path: path.to_owned(),
        manifest: CompiledArtifactInput::explicit_file(path, kind, &bytes, parser, 1)?,
        bytes,
    })
}

fn publish_success(
    context: &ProjectContext,
    artifact: &cdf_project::CompiledResourceArtifact,
    artifact_path: &str,
    schema_files: BTreeMap<String, Vec<u8>>,
    authored: &[CapturedInput],
) -> Result<(), CliError> {
    let prior_index_bytes = optional_file_bytes(
        &context.root.join(COMPILATION_INDEX_RELATIVE_PATH),
        "compilation index",
    )?;
    let mut index = match prior_index_bytes.as_deref() {
        Some(bytes) => {
            let index = parse_compilation_index(bytes)?;
            validate_compilation_index_authority(&index, &context.config, &context.environment)?;
            index
        }
        None => CompilationIndex::empty(&context.config, &context.environment)?,
    };
    index.record_current(artifact)?;
    let mut writes = schema_files
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
    let artifact_bytes = artifact.canonical_json_bytes()?;
    writes.push(
        ProjectFileWrite::new(
            artifact_path,
            artifact_bytes.clone(),
            ProjectFileExpectation::AbsentOrExact(artifact_bytes),
        )
        .owner_only(),
    );
    writes.push(
        ProjectFileWrite::new(
            COMPILATION_INDEX_RELATIVE_PATH,
            index.canonical_json_bytes()?,
            expectation(prior_index_bytes),
        )
        .owner_only(),
    );
    let mut guards = vec![ProjectFileGuard::exact(
        PROJECT_FILE_NAME,
        context.project_bytes.clone(),
    )];
    guards.extend(
        authored
            .iter()
            .map(|input| ProjectFileGuard::exact(&input.path, input.bytes.clone())),
    );
    publish_project_files_transactionally_guarded(
        &context.root,
        COMPILATION_INDEX_RELATIVE_PATH,
        guards,
        writes,
    )?;
    Ok(())
}

fn record_failed_index_entry(
    root: &Path,
    config: &cdf_project::ProjectConfig,
    environment: &cdf_project::EffectiveEnvironment,
    selected: &ProjectResourcePath,
    project_bytes: &[u8],
    error: &CliError,
) -> Result<(), CliError> {
    let prior = optional_file_bytes(
        &root.join(COMPILATION_INDEX_RELATIVE_PATH),
        "compilation index",
    )?;
    let mut index = match prior.as_deref() {
        Some(bytes) => {
            let index = parse_compilation_index(bytes)?;
            validate_compilation_index_authority(&index, config, environment)?;
            index
        }
        None => CompilationIndex::empty(config, environment)?,
    };
    let authored = optional_file_bytes(&selected.absolute_path, "authored resource")?;
    let authored_hash = authored.as_deref().map(sha256);
    index.record_failure(
        selected.resource_id.as_str(),
        &selected.relative_path,
        authored_hash,
        CompilationDiagnostic {
            code: error.code.clone(),
            kind: error_kind_name(&error.kind).to_owned(),
            message: "selected resource did not compile; rerun cdf compile for the resource to inspect the current diagnostic"
                .to_owned(),
        },
    )?;
    let mut guards = vec![ProjectFileGuard::exact(
        PROJECT_FILE_NAME,
        project_bytes.to_vec(),
    )];
    if let Some(bytes) = authored {
        guards.push(ProjectFileGuard::exact(&selected.relative_path, bytes));
    }
    publish_project_files_transactionally_guarded(
        root,
        COMPILATION_INDEX_RELATIVE_PATH,
        guards,
        vec![
            ProjectFileWrite::new(
                COMPILATION_INDEX_RELATIVE_PATH,
                index.canonical_json_bytes()?,
                expectation(prior),
            )
            .owner_only(),
        ],
    )?;
    Ok(())
}

fn reconcile_absent_index_entries(
    root: &Path,
    config: &cdf_project::ProjectConfig,
    environment: &cdf_project::EffectiveEnvironment,
    selected: &[String],
    project_bytes: &[u8],
) -> Result<(), CliError> {
    let prior = optional_file_bytes(
        &root.join(COMPILATION_INDEX_RELATIVE_PATH),
        "compilation index",
    )?;
    let Some(prior_bytes) = prior.as_deref() else {
        return Ok(());
    };
    let mut index = parse_compilation_index(prior_bytes)?;
    validate_compilation_index_authority(&index, config, environment)?;
    let selected = selected
        .iter()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    let absent = index
        .resources
        .iter()
        .filter(|(resource_id, entry)| {
            !selected.contains(resource_id.as_str())
                && entry.status != cdf_project::CompilationStatus::Absent
        })
        .map(|(resource_id, entry)| (resource_id.clone(), entry.path.clone()))
        .collect::<Vec<_>>();
    if absent.is_empty() {
        return Ok(());
    }
    for (resource_id, path) in absent {
        index.record_absent(&resource_id, &path)?;
    }
    publish_project_files_transactionally_guarded(
        root,
        COMPILATION_INDEX_RELATIVE_PATH,
        vec![ProjectFileGuard::exact(
            PROJECT_FILE_NAME,
            project_bytes.to_vec(),
        )],
        vec![
            ProjectFileWrite::new(
                COMPILATION_INDEX_RELATIVE_PATH,
                index.canonical_json_bytes()?,
                expectation(prior),
            )
            .owner_only(),
        ],
    )?;
    Ok(())
}

fn optional_file_bytes(path: &Path, label: &str) -> Result<Option<Vec<u8>>, CliError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
            Ok(Some(fs::read(path).map_err(|error| {
                project_authority_read_error("read", path, error)
            })?))
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

fn expectation(previous: Option<Vec<u8>>) -> ProjectFileExpectation {
    previous.map_or(
        ProjectFileExpectation::Absent,
        ProjectFileExpectation::Exact,
    )
}

fn sha256(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("sha256:{:x}", Sha256::digest(bytes))
}

pub(crate) fn resource_selection_error(
    command: &str,
    error: ProjectResourceSelectionError,
) -> CliError {
    match error {
        ProjectResourceSelectionError::Project(error) => error.into(),
        ProjectResourceSelectionError::ExactNoMatch {
            selector,
            candidates,
        } => CliError::usage(format!(
            "resource selector {selector:?} matched no resource"
        ))
        .with_suggestions(
            suggestions::nearest(&selector, candidates)
                .into_iter()
                .map(|candidate| format!("{command} {candidate}"))
                .collect(),
        ),
        error => CliError::usage(error.to_string()),
    }
}

fn error_kind_name(kind: &ErrorKind) -> &'static str {
    match kind {
        ErrorKind::Transient => "transient",
        ErrorKind::RateLimited => "rate_limited",
        ErrorKind::Auth => "auth",
        ErrorKind::Contract => "contract",
        ErrorKind::Data => "data",
        ErrorKind::Destination => "destination",
        ErrorKind::Environment => "environment",
        ErrorKind::Internal => "internal",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct CompileReport {
    pub project: String,
    pub environment: String,
    pub locked: bool,
    pub selection: Vec<String>,
    pub counts: CompileCounts,
    pub resources: Vec<CompileResourceReport>,
    pub index_path: String,
    pub next_command: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct CompileCounts {
    pub selected: usize,
    pub compiled: usize,
    pub failed: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum CompileResourceStatus {
    Compiled,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct CompileResourceReport {
    pub resource_id: String,
    pub path: String,
    pub status: CompileResourceStatus,
    pub artifact_path: Option<String>,
    pub artifact_hash: Option<String>,
    pub discovered_schema: bool,
    pub schema_authority: Option<CompileSchemaAuthorityReport>,
    pub error: Option<CompileResourceError>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct CompileSchemaAuthorityReport {
    pub status: &'static str,
    pub generation: u64,
    pub schema_hash: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct CompileResourceError {
    pub code: String,
    pub kind: String,
    pub message: String,
}
