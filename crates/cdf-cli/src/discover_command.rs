use std::{collections::BTreeMap, fs, path::Path, sync::Arc};

use cdf_cli_core::render::{
    RenderDocument,
    primitives::{KeyValuePanel, StatusKind, StatusLine},
};
use cdf_kernel::{CanonicalArrowSchema, CdfError};
use cdf_project::{
    DefaultSecretProvider, EnvSecretProvider, FileSecretProvider, PROJECT_FILE_NAME,
    ProjectFileExpectation, ProjectFileGuard, ProjectFileWrite, effective_project_source_config,
    parse_cdf_toml, project_file_transaction_generation,
    publish_project_files_transactionally_guarded, recover_project_file_transaction,
    resolve_project_resource_selection,
};
use cdf_runtime::{SourceCatalogCandidate, SourceCatalogDiscovery, SourceCatalogRequest};
use serde::Serialize;

use crate::{
    args::{Cli, DiscoverCommand, DiscoverResourceArgs, DiscoverSourceArgs},
    context::{ProjectContext, project_location},
    error_catalog,
    output::{CliError, CommandOutput},
};

const SOURCE_CATALOG_MAXIMUM_CANDIDATES: usize = 10_000;
const DISCOVERY_ARTIFACT_VERSION: u16 = 1;

pub(crate) fn discover(
    cli: &Cli,
    command: DiscoverCommand,
    execution: &cdf_runtime::ExecutionServices,
    destinations: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    match command {
        DiscoverCommand::Source(args) => discover_source(cli, &args, execution),
        DiscoverCommand::Resource(args) => discover_resources(cli, &args, execution, destinations),
    }
}

#[derive(Clone)]
struct SourceProject {
    root: std::path::PathBuf,
    project_bytes: Vec<u8>,
    project_name: String,
    environment: String,
    source_type: String,
    source_options: BTreeMap<String, serde_json::Value>,
    driver_options: BTreeMap<String, serde_json::Value>,
}

fn load_source_project(
    cli: &Cli,
    configured_source: &str,
    mutate: bool,
) -> Result<SourceProject, CliError> {
    let (root, project_path) = project_location(cli.project.as_ref())?;
    if mutate {
        recover_project_file_transaction(&root)?;
    } else {
        project_file_transaction_generation(&root)?;
    }
    let project_bytes = fs::read(&project_path).map_err(|error| {
        CdfError::environment(format!(
            "read project configuration {}: {error}",
            project_path.display()
        ))
    })?;
    let config = parse_cdf_toml(std::str::from_utf8(&project_bytes).map_err(|error| {
        CdfError::contract(format!("parse {PROJECT_FILE_NAME} as UTF-8: {error}"))
    })?)?;
    let environment = cli
        .env
        .as_deref()
        .unwrap_or(&config.project.default_environment)
        .to_owned();
    let source = effective_project_source_config(&config, &environment, configured_source)?;
    Ok(SourceProject {
        root,
        project_bytes,
        project_name: config.project.name,
        environment,
        source_type: source.source_type,
        source_options: source.options,
        driver_options: config.driver_options,
    })
}

fn discover_source(
    cli: &Cli,
    args: &DiscoverSourceArgs,
    execution: &cdf_runtime::ExecutionServices,
) -> Result<CommandOutput, CliError> {
    validate_resource_token("configured source", &args.configured_source)?;
    if let Some(namespace) = &args.namespace {
        validate_resource_token("namespace", namespace)?;
    }
    let project = load_source_project(cli, &args.configured_source, args.generate)?;
    let registry = crate::source_registry::builtin_source_registry()?;
    let request = SourceCatalogRequest {
        configured_source: args.configured_source.clone(),
        source_options: project.source_options.clone(),
        maximum_candidates: SOURCE_CATALOG_MAXIMUM_CANDIDATES,
    };
    let discovery = source_catalog(&project, registry, &request, execution)?;
    let mut selected = select_source_candidates(&discovery, &args.selectors)?
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    if args.generate && !discovery.complete {
        return Err(CliError::usage_with(
            "source catalog discovery was truncated; narrow the relation selectors before generating resources",
            error_catalog::USAGE,
        ));
    }
    let mut schema_errors = BTreeMap::new();
    for candidate in &mut selected {
        if candidate.schema.is_some() {
            continue;
        }
        match source_catalog_schema(&project, registry, &request, candidate, execution) {
            Ok(schema) => candidate.schema = schema,
            Err(error) => {
                schema_errors.insert(candidate.relation_id.clone(), error.message);
            }
        }
    }
    let source_hash = cdf_runtime::artifact_hash(&(
        &args.configured_source,
        &project.source_type,
        &project.source_options,
    ))?;
    let mut report = SourceDiscoveryReport {
        scope: "source",
        project: project.project_name.clone(),
        environment: project.environment.clone(),
        configured_source: args.configured_source.clone(),
        source_driver: project.source_type.clone(),
        source_hash,
        identity_space: discovery.identity_space.clone(),
        selectors: args.selectors.clone(),
        complete: discovery.complete,
        continuation: discovery.continuation.clone(),
        generation: discovery.generation.clone(),
        candidates: selected
            .iter()
            .map(|candidate| {
                SourceCandidateReport::from_candidate(
                    candidate,
                    schema_errors.get(&candidate.relation_id).cloned(),
                )
            })
            .collect(),
        effects: Vec::new(),
        artifact: None,
    };
    if args.generate {
        report.effects = generate_resources(
            &project, args, registry, execution, &request, &discovery, &selected,
        )?;
    }
    if let Some(path) = &args.out {
        report.artifact = Some(publish_discovery_artifact(path, "source", &report)?);
    }
    let failed = report
        .effects
        .iter()
        .any(|effect| matches!(effect.outcome, "conflicted" | "manual_naming_required"));
    CommandOutput::rendered_with_exit_code(
        "discover",
        render_source(&report),
        &report,
        i32::from(failed),
    )
}

fn source_catalog(
    project: &SourceProject,
    registry: &cdf_runtime::SourceRegistry,
    request: &SourceCatalogRequest,
    execution: &cdf_runtime::ExecutionServices,
) -> Result<SourceCatalogDiscovery, CliError> {
    let temporary = tempfile::tempdir().map_err(|error| {
        CdfError::environment(format!("create source discovery workspace: {error}"))
    })?;
    let secrets = DefaultSecretProvider::new(
        EnvSecretProvider::process(),
        FileSecretProvider::new(project.root.clone()),
    );
    let context = cdf_runtime::SourceResolutionContext::new(
        &project.root,
        Arc::new(secrets),
        execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    )
    .with_artifact_root(temporary.path())
    .with_driver_options(project.driver_options.clone());
    registry
        .discover_catalog(&project.source_type, request, &context)
        .map_err(Into::into)
}

#[derive(Clone, Debug)]
pub(crate) struct ConfiguredSourceReadiness {
    pub(crate) configured_source: String,
    pub(crate) source_driver: String,
    pub(crate) candidate_count: usize,
    pub(crate) complete: bool,
    pub(crate) identity_space: String,
}

pub(crate) fn probe_configured_source(
    cli: &Cli,
    configured_source: &str,
    execution: &cdf_runtime::ExecutionServices,
) -> Result<ConfiguredSourceReadiness, CliError> {
    validate_resource_token("configured source", configured_source)?;
    let project = load_source_project(cli, configured_source, false)?;
    let registry = crate::source_registry::builtin_source_registry()?;
    let request = SourceCatalogRequest {
        configured_source: configured_source.to_owned(),
        source_options: project.source_options.clone(),
        maximum_candidates: SOURCE_CATALOG_MAXIMUM_CANDIDATES,
    };
    let discovery = source_catalog(&project, registry, &request, execution)?;
    Ok(ConfiguredSourceReadiness {
        configured_source: configured_source.to_owned(),
        source_driver: project.source_type,
        candidate_count: discovery.candidates.len(),
        complete: discovery.complete,
        identity_space: discovery.identity_space,
    })
}

fn source_catalog_schema(
    project: &SourceProject,
    registry: &cdf_runtime::SourceRegistry,
    request: &SourceCatalogRequest,
    candidate: &SourceCatalogCandidate,
    execution: &cdf_runtime::ExecutionServices,
) -> Result<Option<CanonicalArrowSchema>, CliError> {
    let temporary = tempfile::tempdir().map_err(|error| {
        CdfError::environment(format!("create source schema discovery workspace: {error}"))
    })?;
    let secrets = DefaultSecretProvider::new(
        EnvSecretProvider::process(),
        FileSecretProvider::new(project.root.clone()),
    );
    let context = cdf_runtime::SourceResolutionContext::new(
        &project.root,
        Arc::new(secrets),
        execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    )
    .with_artifact_root(temporary.path())
    .with_driver_options(project.driver_options.clone());
    registry
        .discover_catalog_schema(&project.source_type, request, candidate, &context)
        .map_err(Into::into)
}

fn select_source_candidates<'a>(
    discovery: &'a SourceCatalogDiscovery,
    selectors: &[String],
) -> Result<Vec<&'a SourceCatalogCandidate>, CliError> {
    if selectors.is_empty() {
        return Ok(discovery.candidates.iter().collect());
    }
    let patterns = selectors
        .iter()
        .map(|selector| {
            glob::Pattern::new(selector).map_err(|error| {
                CliError::usage_with(
                    format!("invalid relation selector {selector:?}: {error}"),
                    error_catalog::USAGE,
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    for (selector, pattern) in selectors.iter().zip(&patterns) {
        if !discovery
            .candidates
            .iter()
            .any(|candidate| pattern.matches(&candidate.relation_id))
        {
            return Err(CliError::usage_with(
                format!(
                    "relation selector {selector:?} matched no candidates in {} identity space",
                    discovery.identity_space
                ),
                error_catalog::USAGE,
            ));
        }
    }
    Ok(discovery
        .candidates
        .iter()
        .filter(|candidate| {
            patterns
                .iter()
                .any(|pattern| pattern.matches(&candidate.relation_id))
        })
        .collect())
}

fn generate_resources(
    project: &SourceProject,
    args: &DiscoverSourceArgs,
    registry: &cdf_runtime::SourceRegistry,
    execution: &cdf_runtime::ExecutionServices,
    request: &SourceCatalogRequest,
    discovery: &SourceCatalogDiscovery,
    selected: &[SourceCatalogCandidate],
) -> Result<Vec<GenerationEffect>, CliError> {
    let namespace = args.namespace.as_deref().unwrap_or(&args.configured_source);
    let mut proposals = Vec::new();
    let mut path_counts = BTreeMap::<String, usize>::new();
    for candidate in selected {
        let Some(token) = candidate.resource_token.as_deref() else {
            proposals.push((candidate, None, None));
            continue;
        };
        let path = format!("cdf/{namespace}/{token}.cdf.sql");
        *path_counts.entry(path.clone()).or_default() += 1;
        let sql = source_resource_sql(
            &args.configured_source,
            &candidate.resource_options,
            candidate.schema.as_ref(),
        )?;
        proposals.push((candidate, Some(path), Some(sql)));
    }

    let mut effects = Vec::with_capacity(proposals.len());
    let mut writes = Vec::new();
    for (candidate, path, sql) in proposals {
        let Some(path) = path else {
            effects.push(GenerationEffect::new(
                candidate,
                None,
                "manual_naming_required",
                candidate.schema.is_none(),
            ));
            continue;
        };
        let sql = sql.expect("generated path has SQL");
        if path_counts[&path] > 1 {
            effects.push(GenerationEffect::new(
                candidate,
                Some(path),
                "conflicted",
                candidate.schema.is_none(),
            ));
            continue;
        }
        let absolute = project.root.join(&path);
        let outcome = match fs::symlink_metadata(&absolute) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => "created",
            Ok(metadata) if metadata.file_type().is_file() => {
                let existing = fs::read(&absolute).map_err(|error| {
                    CdfError::environment(format!(
                        "read generated resource target {}: {error}",
                        absolute.display()
                    ))
                })?;
                if existing == sql.as_bytes() {
                    "unchanged"
                } else {
                    "conflicted"
                }
            }
            Ok(_) => "conflicted",
            Err(error) => {
                return Err(CdfError::environment(format!(
                    "inspect generated resource target {}: {error}",
                    absolute.display()
                ))
                .into());
            }
        };
        if outcome == "created" {
            writes.push(ProjectFileWrite::new(
                &path,
                sql.as_bytes().to_vec(),
                ProjectFileExpectation::AbsentOrExact(sql.as_bytes().to_vec()),
            ));
        }
        effects.push(GenerationEffect::new(
            candidate,
            Some(path),
            outcome,
            candidate.schema.is_none(),
        ));
    }

    if !writes.is_empty() {
        let rediscovered = source_catalog(project, registry, request, execution)?;
        if rediscovered.generation != discovery.generation {
            return Err(CdfError::contract(
                "source catalog changed between discovery and generation; rerun `cdf discover source --generate`",
            )
            .into());
        }
        for candidate in selected {
            if candidate.schema.is_none() {
                continue;
            }
            let current = rediscovered
                .candidates
                .iter()
                .find(|current| current.relation_id == candidate.relation_id)
                .ok_or_else(|| {
                    CdfError::contract(
                        "selected source relation disappeared before generation; rerun discovery",
                    )
                })?;
            let current_schema = if current.schema.is_some() {
                current.schema.clone()
            } else {
                source_catalog_schema(project, registry, request, current, execution)?
            };
            if current_schema != candidate.schema {
                return Err(CdfError::contract(format!(
                    "source schema for relation {:?} changed before generation; rerun discovery",
                    candidate.relation_id
                ))
                .into());
            }
        }
        let commit_path = effects
            .iter()
            .filter(|effect| effect.outcome == "created")
            .filter_map(|effect| effect.path.as_deref())
            .next_back()
            .ok_or_else(|| CdfError::internal("generated writes lost their commit path"))?;
        publish_project_files_transactionally_guarded(
            &project.root,
            commit_path,
            vec![ProjectFileGuard::exact(
                PROJECT_FILE_NAME,
                project.project_bytes.clone(),
            )],
            writes,
        )?;
    }
    Ok(effects)
}

pub(crate) fn source_resource_sql(
    configured_source: &str,
    resource_options: &BTreeMap<String, serde_json::Value>,
    schema: Option<&CanonicalArrowSchema>,
) -> Result<String, CliError> {
    validate_resource_token("configured source", configured_source)?;
    let mut sql = String::from("SELECT");
    match schema.filter(|schema| !schema.fields.is_empty()) {
        Some(schema) => {
            sql.push('\n');
            for (index, field) in schema.fields.iter().enumerate() {
                sql.push_str("  \"");
                sql.push_str(&field.name.replace('"', "\"\""));
                sql.push('"');
                if index + 1 != schema.fields.len() {
                    sql.push(',');
                }
                sql.push('\n');
            }
        }
        None => sql.push_str(" *\n"),
    }
    sql.push_str("FROM upstream(\n  source => '");
    sql.push_str(&configured_source.replace('\'', "''"));
    sql.push('\'');
    for (name, value) in resource_options {
        validate_resource_token("resource option", name)?;
        sql.push_str(",\n  ");
        sql.push_str(name);
        sql.push_str(" => ");
        sql.push_str(&crate::add_command::sql_value(value)?);
    }
    sql.push_str("\n);\n");
    Ok(sql)
}

fn validate_resource_token(label: &str, value: &str) -> Result<(), CliError> {
    let mut bytes = value.bytes();
    if value.len() > 128
        || !bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
        || bytes.any(|byte| !(byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'))
    {
        return Err(CliError::usage_with(
            format!("{label} {value:?} must match [a-z][a-z0-9_]{{0,127}}"),
            error_catalog::USAGE,
        ));
    }
    Ok(())
}

fn discover_resources(
    cli: &Cli,
    args: &DiscoverResourceArgs,
    execution: &cdf_runtime::ExecutionServices,
    destinations: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    let (root, _) = project_location(cli.project.as_ref())?;
    project_file_transaction_generation(&root)?;
    let selection = resolve_project_resource_selection(&root, &args.selectors, &args.exclude)
        .map_err(|error| match error {
            cdf_project::ProjectResourceSelectionError::Project(error) => CliError::from(error),
            error => CliError::usage_with(error.to_string(), error_catalog::USAGE),
        })?;
    let mut resources = Vec::with_capacity(selection.selection.resolved.len());
    for selected in &selection.resources {
        let resource_id = selected.resource_id.as_str();
        resources.push(
            discover_one_resource(cli, resource_id, execution, destinations)
                .unwrap_or_else(|error| ResourceDiscoveryItem::failed(resource_id, error)),
        );
    }
    let project_text = fs::read_to_string(root.join(PROJECT_FILE_NAME))
        .map_err(|error| CdfError::environment(format!("read project configuration: {error}")))?;
    let config = parse_cdf_toml(&project_text)?;
    let mut report = ResourceDiscoveryReport {
        scope: "resource",
        project: config.project.name,
        environment: cli
            .env
            .clone()
            .unwrap_or(config.project.default_environment),
        selectors: args.selectors.clone(),
        exclude: args.exclude.clone(),
        resources,
        artifact: None,
    };
    if let Some(path) = &args.out {
        report.artifact = Some(publish_discovery_artifact(path, "resource", &report)?);
    }
    let failed = report
        .resources
        .iter()
        .any(|resource| resource.status == "failed");
    CommandOutput::rendered_with_exit_code(
        "discover",
        render_resources(&report),
        &report,
        i32::from(failed),
    )
}

fn discover_one_resource(
    cli: &Cli,
    resource_id: &str,
    execution: &cdf_runtime::ExecutionServices,
    destinations: &cdf_runtime::DestinationRegistry,
) -> Result<ResourceDiscoveryItem, CliError> {
    let context = ProjectContext::load_selected_read_only(
        cli.project.as_ref(),
        cli.env.as_deref(),
        resource_id,
        destinations,
    )?;
    let temporary = tempfile::tempdir().map_err(|error| {
        CdfError::environment(format!("create resource discovery workspace: {error}"))
    })?;
    let compiled = context.resource(resource_id)?;
    let prepared = crate::scan_command::prepare_resource_schema_for_cli(
        &context,
        compiled,
        false,
        Some(execution),
        temporary.path(),
    )?;
    Ok(ResourceDiscoveryItem {
        resource_id: resource_id.to_owned(),
        status: "discovered",
        configured_source: context
            .resource_query(resource_id)
            .map(|query| query.configured_source.configured_source.clone()),
        source_driver: Some(prepared.source_plan.driver.driver_id.as_str().to_owned()),
        schema: Some(CanonicalArrowSchema::from_arrow(
            &prepared.source_plan.schema,
        )?),
        coverage: prepared
            .schema_snapshot
            .and_then(|snapshot| snapshot.discovery),
        error: None,
    })
}

fn publish_discovery_artifact<T: Serialize>(
    path: &Path,
    scope: &'static str,
    report: &T,
) -> Result<ArtifactEffect, CliError> {
    let report_value = serde_json::to_value(report)
        .map_err(|error| CdfError::internal(format!("serialize discovery report: {error}")))?;
    let content_hash =
        cdf_runtime::artifact_hash(&(DISCOVERY_ARTIFACT_VERSION, scope, &report_value))?;
    let artifact = DiscoveryArtifact {
        artifact_version: DISCOVERY_ARTIFACT_VERSION,
        scope,
        content_hash: &content_hash,
        report: &report_value,
    };
    let mut bytes = serde_json::to_vec_pretty(&artifact)
        .map_err(|error| CdfError::internal(format!("encode discovery artifact: {error}")))?;
    bytes.push(b'\n');
    match fs::read(path) {
        Ok(existing) if existing == bytes => {
            return Ok(ArtifactEffect {
                path: path.display().to_string(),
                content_hash,
                outcome: "unchanged",
            });
        }
        Ok(_) => {
            return Err(CdfError::contract(format!(
                "discovery artifact {} already exists with different bytes; choose a new --out path",
                path.display()
            ))
            .into());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(CdfError::environment(format!(
                "inspect discovery artifact {}: {error}",
                path.display()
            ))
            .into());
        }
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    if let Some(parent) = parent {
        fs::create_dir_all(parent).map_err(|error| {
            CdfError::environment(format!(
                "create discovery artifact directory {}: {error}",
                parent.display()
            ))
        })?;
    }
    let parent = parent.unwrap_or_else(|| Path::new("."));
    let mut temporary = tempfile::NamedTempFile::new_in(parent).map_err(|error| {
        CdfError::environment(format!("create discovery artifact staging file: {error}"))
    })?;
    use std::io::Write as _;
    temporary.write_all(&bytes).map_err(|error| {
        CdfError::environment(format!("write discovery artifact staging file: {error}"))
    })?;
    temporary.persist_noclobber(path).map_err(|error| {
        CdfError::environment(format!(
            "publish discovery artifact {}: {}",
            path.display(),
            error.error
        ))
    })?;
    Ok(ArtifactEffect {
        path: path.display().to_string(),
        content_hash,
        outcome: "created",
    })
}

#[derive(Clone, Debug, Serialize)]
struct SourceDiscoveryReport {
    scope: &'static str,
    project: String,
    environment: String,
    configured_source: String,
    source_driver: String,
    source_hash: String,
    identity_space: String,
    selectors: Vec<String>,
    complete: bool,
    continuation: Option<String>,
    generation: String,
    candidates: Vec<SourceCandidateReport>,
    effects: Vec<GenerationEffect>,
    artifact: Option<ArtifactEffect>,
}

#[derive(Clone, Debug, Serialize)]
struct SourceCandidateReport {
    relation_id: String,
    label: String,
    relation_kind: String,
    resource_token: Option<String>,
    schema_fields: Option<usize>,
    schema_error: Option<String>,
    resource_options: BTreeMap<String, serde_json::Value>,
}

impl SourceCandidateReport {
    fn from_candidate(candidate: &SourceCatalogCandidate, schema_error: Option<String>) -> Self {
        Self {
            relation_id: candidate.relation_id.clone(),
            label: candidate.display_label.clone(),
            relation_kind: candidate.relation_kind.clone(),
            resource_token: candidate.resource_token.clone(),
            schema_fields: candidate.schema.as_ref().map(|schema| schema.fields.len()),
            schema_error,
            resource_options: candidate.resource_options.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct GenerationEffect {
    relation_id: String,
    path: Option<String>,
    outcome: &'static str,
    projection_fallback: bool,
}

impl GenerationEffect {
    fn new(
        candidate: &SourceCatalogCandidate,
        path: Option<String>,
        outcome: &'static str,
        projection_fallback: bool,
    ) -> Self {
        Self {
            relation_id: candidate.relation_id.clone(),
            path,
            outcome,
            projection_fallback,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct ResourceDiscoveryReport {
    scope: &'static str,
    project: String,
    environment: String,
    selectors: Vec<String>,
    exclude: Vec<String>,
    resources: Vec<ResourceDiscoveryItem>,
    artifact: Option<ArtifactEffect>,
}

#[derive(Clone, Debug, Serialize)]
struct ResourceDiscoveryItem {
    resource_id: String,
    status: &'static str,
    configured_source: Option<String>,
    source_driver: Option<String>,
    schema: Option<CanonicalArrowSchema>,
    coverage: Option<crate::reports::DiscoveryCoverageReport>,
    error: Option<ResourceDiscoveryFailure>,
}

impl ResourceDiscoveryItem {
    fn failed(resource_id: &str, error: CliError) -> Self {
        Self {
            resource_id: resource_id.to_owned(),
            status: "failed",
            configured_source: None,
            source_driver: None,
            schema: None,
            coverage: None,
            error: Some(ResourceDiscoveryFailure {
                code: error.code,
                kind: format!("{:?}", error.kind).to_ascii_lowercase(),
                message: cdf_cli_core::render::redaction::redact_uri_userinfo(error.message),
            }),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct ResourceDiscoveryFailure {
    code: String,
    kind: String,
    message: String,
}

#[derive(Clone, Debug, Serialize)]
struct ArtifactEffect {
    path: String,
    content_hash: String,
    outcome: &'static str,
}

#[derive(Serialize)]
struct DiscoveryArtifact<'a> {
    artifact_version: u16,
    scope: &'static str,
    content_hash: &'a str,
    report: &'a serde_json::Value,
}

fn render_source(report: &SourceDiscoveryReport) -> RenderDocument {
    let mut document = RenderDocument::new()
        .push(StatusLine::new(
            StatusKind::Success,
            format!(
                "discovered {} relation(s) from {}",
                report.candidates.len(),
                report.configured_source
            ),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Source discovery")
                .row("source", report.configured_source.clone())
                .row("driver", report.source_driver.clone())
                .row("identity space", report.identity_space.clone())
                .row("complete", report.complete.to_string())
                .row("generation", report.generation.clone()),
        );
    for candidate in &report.candidates {
        let mut panel = KeyValuePanel::new(candidate.relation_id.clone())
            .row("kind", candidate.relation_kind.clone())
            .row(
                "resource token",
                candidate
                    .resource_token
                    .clone()
                    .unwrap_or_else(|| "manual naming required".to_owned()),
            )
            .row(
                "schema fields",
                candidate
                    .schema_fields
                    .map(|fields| fields.to_string())
                    .unwrap_or_else(|| "unavailable".to_owned()),
            );
        if let Some(error) = &candidate.schema_error {
            panel = panel.row("schema note", error.clone());
        }
        document = document.blank_line().push(panel);
    }
    for effect in &report.effects {
        let kind = if matches!(effect.outcome, "created" | "unchanged") {
            StatusKind::Success
        } else {
            StatusKind::Error
        };
        document = document.push(StatusLine::new(
            kind,
            format!(
                "{} {} -> {}{}",
                effect.outcome,
                effect.relation_id,
                effect.path.as_deref().unwrap_or("no safe generated path"),
                if effect.projection_fallback {
                    " (SELECT * fallback)"
                } else {
                    ""
                }
            ),
        ));
    }
    document
}

fn render_resources(report: &ResourceDiscoveryReport) -> RenderDocument {
    let discovered = report
        .resources
        .iter()
        .filter(|resource| resource.status == "discovered")
        .count();
    let failed = report.resources.len() - discovered;
    let mut document = RenderDocument::new().push(StatusLine::new(
        if failed == 0 {
            StatusKind::Success
        } else {
            StatusKind::Error
        },
        format!(
            "discovered schema for {discovered}/{} resource(s)",
            report.resources.len()
        ),
    ));
    for resource in &report.resources {
        let mut panel = KeyValuePanel::new(resource.resource_id.clone())
            .row("status", resource.status.to_owned());
        if let Some(source) = &resource.configured_source {
            panel = panel.row("configured source", source.clone());
        }
        if let Some(driver) = &resource.source_driver {
            panel = panel.row("driver", driver.clone());
        }
        if let Some(schema) = &resource.schema {
            panel = panel.row("schema fields", schema.fields.len().to_string());
        }
        if let Some(error) = &resource.error {
            panel = panel.row("error", format!("{}: {}", error.code, error.message));
        }
        document = document.blank_line().push(panel);
    }
    document
}
