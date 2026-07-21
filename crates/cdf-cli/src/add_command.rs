use std::{collections::BTreeMap, env, fs, path::PathBuf, sync::Arc};

use cdf_declarative::{
    CompiledResource, compile_document_with_project_root, parse_toml as parse_declarative_toml,
};
use cdf_http::{SecretProvider, SecretUri, SecretValue};
use cdf_kernel::{CdfError, SchemaSource};
use cdf_project::{
    LOCK_FILE_NAME, PROJECT_FILE_NAME, ProjectFileExpectation, ProjectFileWrite,
    ResourceSchemaDiscoveryArtifacts, SchemaSnapshotArtifact, SchemaSnapshotDataType,
    SchemaSnapshotField, freeze_contract_snapshots, lock_to_toml, parse_cdf_toml, parse_lock,
    publish_project_files_transactionally,
};
use cdf_runtime::{PlannedSourceAdd, SourceAddPrivateFile, SourceAddRequest, SourceRegistry};
use serde::Serialize;

use crate::{
    args::{AddArgs, Cli},
    context::ProjectContext,
    error_catalog,
    output::{CliError, CommandOutput},
    render::{
        RenderDocument,
        primitives::{KeyValuePanel, NextCommand, SectionRule, StatusKind, StatusLine, Table},
    },
};

pub(crate) fn add(
    cli: &Cli,
    args: AddArgs,
    execution: &cdf_runtime::ExecutionServices,
    destinations: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    let context =
        ProjectContext::load_for_command("add", cli.project.as_ref(), cli.env.as_deref())?;
    let registry = crate::source_registry::builtin_source_registry()?;
    let request = AddResourceRequest::from_args(&context, registry, &args)?;
    let proposed = build_proposed_resource(&context, registry, &request)?;
    ensure_add_is_available(&context, &request, &proposed)?;

    let add_secrets = AddSecretProvider {
        fallback: context.secret_provider(),
        private_files: request.plan.proposal.private_files.clone(),
    };
    let inspection_root = args
        .dry_run
        .then(|| {
            tempfile::Builder::new()
                .prefix("cdf-add-dry-run-")
                .tempdir()
        })
        .transpose()
        .map_err(|error| {
            CdfError::internal(format!("create add dry-run artifact root: {error}"))
        })?;
    let artifact_root = inspection_root
        .as_ref()
        .map_or(context.root.as_path(), tempfile::TempDir::path);
    let artifacts = discover_for_add(
        &context,
        registry,
        &proposed.resource,
        add_secrets,
        execution,
        artifact_root,
    )?;
    let discovery = &artifacts.discovery;
    let pinned_resource = proposed.resource.with_schema_source_and_schema(
        SchemaSource::Discovered {
            snapshot: discovery.snapshot.reference.clone(),
        },
        Arc::clone(&discovery.normalized_schema),
    );
    let report = AddReport::from_parts(&context, &request, &proposed, &discovery.snapshot.artifact);

    if !args.dry_run {
        write_add_artifacts(
            destinations,
            &context,
            &request,
            &proposed,
            &pinned_resource,
            &artifacts,
        )?;
    }

    CommandOutput::rendered("add", add_document(&report), report)
}

fn build_proposed_resource(
    context: &ProjectContext,
    registry: &SourceRegistry,
    request: &AddResourceRequest,
) -> Result<ProposedResource, CliError> {
    let resource_toml = resource_toml(request)?;
    let document = parse_declarative_toml(&resource_toml)?;
    let mut resources = compile_document_with_project_root(registry, &document, &context.root)?;
    if resources.len() != 1 {
        return Err(CliError::mapped(
            CdfError::internal(format!(
                "cdf add expected one compiled resource from generated TOML, got {}",
                resources.len()
            )),
            error_catalog::PROJECT_IO,
        ));
    }
    let resource = resources.remove(0);
    if resource.descriptor().resource_id.as_str() != request.resource_id {
        return Err(CliError::mapped(
            CdfError::internal(format!(
                "generated resource id `{}` did not match requested id `{}`",
                resource.descriptor().resource_id,
                request.resource_id
            )),
            error_catalog::PROJECT_IO,
        ));
    }
    if resource.source_plan().driver != request.plan.driver {
        return Err(CliError::mapped(
            CdfError::internal("generated resource compiled through a different source driver"),
            error_catalog::PROJECT_IO,
        ));
    }
    let (project_prior, project_toml) = appended_project_mapping(context, request)?;
    Ok(ProposedResource {
        resource,
        resource_toml,
        project_toml,
        project_prior,
    })
}

fn ensure_add_is_available(
    context: &ProjectContext,
    request: &AddResourceRequest,
    proposed: &ProposedResource,
) -> Result<(), CliError> {
    if context
        .resources
        .iter()
        .any(|resource| resource.descriptor().resource_id.as_str() == request.resource_id)
    {
        return Err(CliError::usage_with(
            format!(
                "resource `{}` is already compiled; use a new `<source>.<resource>` id",
                request.resource_id
            ),
            error_catalog::USAGE,
        ));
    }
    if context.config.resources.contains_key(&request.resource_id) {
        return Err(CliError::usage_with(
            format!(
                "{} already contains [resources.\"{}\"]",
                PROJECT_FILE_NAME, request.resource_id
            ),
            error_catalog::PROJECT_RESOURCE_MAPPING,
        ));
    }
    if request.config_path_abs.exists() {
        return Err(CliError::usage_with(
            format!(
                "cdf add would overwrite {}; choose a different source id or edit that file explicitly",
                request.config_path_rel
            ),
            error_catalog::PROJECT_IO,
        ));
    }
    for private_file in &request.plan.proposal.private_files {
        if context.root.join(&private_file.relative_path).exists() {
            return Err(CliError::usage_with(
                format!(
                    "cdf add would overwrite private source state for source `{}`",
                    request.source
                ),
                error_catalog::PROJECT_IO,
            ));
        }
    }
    parse_cdf_toml(&proposed.project_toml)?;
    Ok(())
}

fn discover_for_add(
    context: &ProjectContext,
    registry: &SourceRegistry,
    resource: &CompiledResource,
    secret_provider: AddSecretProvider,
    execution: &cdf_runtime::ExecutionServices,
    artifact_root: &std::path::Path,
) -> Result<ResourceSchemaDiscoveryArtifacts, CliError> {
    let options = cdf_project::SchemaDiscoveryExecutionOptions::new();
    let source_plan = resource.source_plan().clone();
    let resolution = cdf_runtime::SourceResolutionContext::new(
        &context.root,
        Arc::new(secret_provider),
        execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    )
    .with_artifact_root(artifact_root)
    .with_driver_options(context.config.driver_options.clone());
    Ok(cdf_project::discover_resource_schema_with_source_registry(
        resource,
        registry,
        &source_plan,
        &resolution,
        options,
    )?)
}

struct AddSecretProvider {
    fallback: cdf_project::DefaultSecretProvider,
    private_files: Vec<SourceAddPrivateFile>,
}

impl SecretProvider for AddSecretProvider {
    fn resolve(&self, uri: &SecretUri) -> cdf_kernel::Result<SecretValue> {
        if let Some(private_file) = self
            .private_files
            .iter()
            .find(|private_file| &private_file.reference == uri)
        {
            return Ok(private_file.value.clone());
        }
        self.fallback.resolve(uri)
    }
}

fn write_add_artifacts(
    destinations: &cdf_runtime::DestinationRegistry,
    context: &ProjectContext,
    request: &AddResourceRequest,
    proposed: &ProposedResource,
    pinned_resource: &CompiledResource,
    artifacts: &ResourceSchemaDiscoveryArtifacts,
) -> Result<(), CliError> {
    let lock_path = context.root.join(LOCK_FILE_NAME);
    let lock_expectation = match (context.lock.as_ref(), fs::read(&lock_path)) {
        (Some(expected), Ok(bytes)) => {
            let text = std::str::from_utf8(&bytes).map_err(|error| {
                CliError::mapped(
                    CdfError::contract(format!("parse {LOCK_FILE_NAME} as UTF-8: {error}")),
                    error_catalog::PROJECT_IO,
                )
            })?;
            if &parse_lock(text)? != expected {
                return Err(CliError::mapped(
                    CdfError::contract(
                        "cdf.lock changed after the add command loaded project authority; retry against the current project",
                    ),
                    error_catalog::PROJECT_IO,
                ));
            }
            ProjectFileExpectation::Exact(bytes)
        }
        (None, Err(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            ProjectFileExpectation::Absent
        }
        (Some(_), Err(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(CliError::mapped(
                CdfError::contract(
                    "cdf.lock disappeared after the add command loaded project authority; retry against the current project",
                ),
                error_catalog::PROJECT_IO,
            ));
        }
        (None, Ok(_)) => {
            return Err(CliError::mapped(
                CdfError::contract(
                    "cdf.lock appeared after the add command loaded project authority; retry against the current project",
                ),
                error_catalog::PROJECT_IO,
            ));
        }
        (_, Err(error)) => {
            return Err(CliError::mapped(
                CdfError::contract(format!("read {LOCK_FILE_NAME}: {error}")),
                error_catalog::PROJECT_IO,
            ));
        }
    };
    let mut resources = context.resources.clone();
    resources.push(pinned_resource.clone());
    let updated_config = parse_cdf_toml(&proposed.project_toml)?;
    let destination_artifacts = crate::destination_registry::inspect_destination_artifacts(
        destinations,
        context,
        &context.environment.destination,
    )?;
    let (lock, _) = freeze_contract_snapshots(
        &updated_config,
        &resources,
        context.lock.as_ref(),
        &destination_artifacts,
        Some(request.resource_id.as_str()),
    )?;
    let lock_toml = lock_to_toml(&lock)?;

    let mut writes = Vec::new();
    for (path, bytes) in artifacts.canonical_artifact_files()? {
        writes.push(ProjectFileWrite::new(
            path,
            bytes.clone(),
            ProjectFileExpectation::AbsentOrExact(bytes),
        ));
    }
    for private_file in &request.plan.proposal.private_files {
        writes.push(
            ProjectFileWrite::new(
                private_file.relative_path.clone(),
                private_file.value.as_str()?.as_bytes().to_vec(),
                ProjectFileExpectation::Absent,
            )
            .owner_only(),
        );
    }
    writes.push(ProjectFileWrite::new(
        &request.config_path_rel,
        proposed.resource_toml.as_bytes().to_vec(),
        ProjectFileExpectation::Absent,
    ));
    writes.push(ProjectFileWrite::new(
        PROJECT_FILE_NAME,
        proposed.project_toml.as_bytes().to_vec(),
        ProjectFileExpectation::Exact(proposed.project_prior.clone()),
    ));
    writes.push(ProjectFileWrite::new(
        LOCK_FILE_NAME,
        lock_toml.into_bytes(),
        lock_expectation,
    ));
    publish_project_files_transactionally(&context.root, LOCK_FILE_NAME, writes)?;
    Ok(())
}

#[derive(Clone, Debug)]
struct AddResourceRequest {
    resource_id: String,
    source: String,
    resource: String,
    plan: PlannedSourceAdd,
    config_path_rel: String,
    config_path_abs: PathBuf,
    dry_run: bool,
}

impl AddResourceRequest {
    fn from_args(
        context: &ProjectContext,
        registry: &SourceRegistry,
        args: &AddArgs,
    ) -> Result<Self, CliError> {
        let (source, resource) = split_resource_id(&args.resource_id)?;
        let current_dir = env::current_dir().map_err(|error| {
            CliError::mapped(
                CdfError::internal(format!("read current directory: {error}")),
                error_catalog::PROJECT_IO,
            )
        })?;
        let plan = registry
            .plan_add(
                SourceAddRequest {
                    source_name: source.clone(),
                    resource_name: resource.clone(),
                    location: args.location.clone(),
                    project_root: context.root.clone(),
                    current_dir,
                    options: args.options.clone(),
                    project_options: None,
                },
                &context.config.driver_options,
            )
            .map_err(|error| CliError::usage_with(error.message, error_catalog::USAGE))?;
        let config_path_rel = format!("resources/{source}.toml");
        let config_path_abs = context.root.join(&config_path_rel);
        Ok(Self {
            resource_id: args.resource_id.clone(),
            source,
            resource,
            plan,
            config_path_rel,
            config_path_abs,
            dry_run: args.dry_run,
        })
    }
}

struct ProposedResource {
    resource: CompiledResource,
    resource_toml: String,
    project_toml: String,
    project_prior: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct AddReport {
    project: String,
    environment: String,
    resource_id: String,
    source: String,
    resource: String,
    source_driver: String,
    config_path: String,
    schema_hash: String,
    schema_snapshot_path: String,
    location: String,
    selection: String,
    write_disposition: &'static str,
    schema_source: &'static str,
    fields: Vec<AddFieldReport>,
    cursor: Option<String>,
    cursor_candidates: Vec<String>,
    writes: AddWrites,
    next_command: String,
}

impl AddReport {
    fn from_parts(
        context: &ProjectContext,
        request: &AddResourceRequest,
        proposed: &ProposedResource,
        snapshot: &SchemaSnapshotArtifact,
    ) -> Self {
        let cursor = proposed
            .resource
            .descriptor()
            .cursor
            .as_ref()
            .map(|cursor| cursor.field.clone());
        let cursor_candidates = snapshot
            .schema
            .fields
            .iter()
            .filter(|field| {
                matches!(
                    field.data_type,
                    SchemaSnapshotDataType::Int { .. }
                        | SchemaSnapshotDataType::Timestamp { .. }
                        | SchemaSnapshotDataType::Date { .. }
                ) && cursor.as_deref() != Some(field.name.as_str())
            })
            .map(|field| field.name.clone())
            .collect();
        Self {
            project: context.root.display().to_string(),
            environment: context.environment.name.clone(),
            resource_id: request.resource_id.clone(),
            source: request.source.clone(),
            resource: request.resource.clone(),
            source_driver: request.plan.driver.driver_id.as_str().to_owned(),
            config_path: request.config_path_rel.clone(),
            schema_hash: snapshot.schema_hash.to_string(),
            schema_snapshot_path: snapshot.path.clone(),
            location: request.plan.proposal.display_location.as_str().to_owned(),
            selection: request.plan.proposal.display_selection.clone(),
            write_disposition: "append",
            schema_source: "discovered",
            fields: snapshot
                .schema
                .fields
                .iter()
                .map(AddFieldReport::from_field)
                .collect(),
            cursor,
            cursor_candidates,
            writes: AddWrites {
                resource_config: !request.dry_run,
                project_config: !request.dry_run,
                schema_snapshot: !request.dry_run,
                lockfile: !request.dry_run,
                package: false,
                destination: false,
                checkpoint: false,
            },
            next_command: format!("cdf run {}", request.resource_id),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct AddFieldReport {
    name: String,
    data_type: SchemaSnapshotDataType,
    nullable: bool,
    source_name: Option<String>,
}

impl AddFieldReport {
    fn from_field(field: &SchemaSnapshotField) -> Self {
        Self {
            name: field.name.clone(),
            data_type: field.data_type.clone(),
            nullable: field.nullable,
            source_name: field.metadata.get("cdf:source_name").cloned(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct AddWrites {
    resource_config: bool,
    project_config: bool,
    schema_snapshot: bool,
    lockfile: bool,
    package: bool,
    destination: bool,
    checkpoint: bool,
}

fn add_document(report: &AddReport) -> RenderDocument {
    let field_table = report.fields.iter().fold(
        Table::new(["field", "type", "nullable", "source"]),
        |table, field| {
            table.row([
                field.name.clone(),
                field_type_label(&field.data_type),
                yes_no(field.nullable).to_owned(),
                field.source_name.clone().unwrap_or_else(|| "-".to_owned()),
            ])
        },
    );
    RenderDocument::new()
        .push(SectionRule::new())
        .push(StatusLine::new(
            StatusKind::Success,
            if report.writes.resource_config {
                format!("added resource {}", report.resource_id)
            } else {
                format!("prepared resource {} (dry run)", report.resource_id)
            },
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Resource")
                .row("id", report.resource_id.clone())
                .row("driver", report.source_driver.clone())
                .row("config", report.config_path.clone())
                .row("location", report.location.clone())
                .row("selection", report.selection.clone())
                .row("disposition", report.write_disposition.to_owned())
                .row("schema", report.schema_hash.clone())
                .row("snapshot", report.schema_snapshot_path.clone()),
        )
        .push(
            KeyValuePanel::new("Suggestions")
                .row(
                    "cursor",
                    report.cursor.clone().unwrap_or_else(|| "none".to_owned()),
                )
                .row(
                    "cursor candidates",
                    if report.cursor_candidates.is_empty() {
                        "none".to_owned()
                    } else {
                        format!("{} (not selected)", report.cursor_candidates.join(", "))
                    },
                ),
        )
        .blank_line()
        .push(field_table)
        .blank_line()
        .push(NextCommand::new(report.next_command.clone()))
}

#[derive(Serialize)]
struct GeneratedResourceDocument<'a> {
    source: BTreeMap<&'a str, GeneratedSource<'a>>,
    resource: BTreeMap<&'a str, GeneratedResource<'a>>,
}

#[derive(Serialize)]
struct GeneratedSource<'a> {
    kind: &'a str,
    #[serde(flatten)]
    options: &'a BTreeMap<String, serde_json::Value>,
}

#[derive(Serialize)]
struct GeneratedResource<'a> {
    #[serde(flatten)]
    options: &'a BTreeMap<String, serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<GeneratedCursor>,
    write_disposition: &'static str,
    trust: &'static str,
}

#[derive(Serialize)]
struct GeneratedCursor {
    field: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    param: Option<String>,
    ordering: &'static str,
    lag: String,
}

fn resource_toml(request: &AddResourceRequest) -> Result<String, CliError> {
    registered_source_resource_toml(&request.source, &request.resource, &request.plan)
}

pub(crate) fn registered_source_resource_toml(
    source: &str,
    resource: &str,
    plan: &PlannedSourceAdd,
) -> Result<String, CliError> {
    let proposal = &plan.proposal;
    let cursor = proposal.cursor.as_ref().map(|cursor| GeneratedCursor {
        field: cursor.field.clone(),
        param: cursor.parameter.clone(),
        ordering: match cursor.ordering {
            cdf_runtime::SourceAddCursorOrdering::Exact => "exact",
            cdf_runtime::SourceAddCursorOrdering::Inexact => "inexact",
            cdf_runtime::SourceAddCursorOrdering::BestEffort => "best_effort",
            cdf_runtime::SourceAddCursorOrdering::Unordered => "unordered",
        },
        lag: format!("{}ms", cursor.lag_tolerance_ms),
    });
    toml::to_string_pretty(&GeneratedResourceDocument {
        source: BTreeMap::from([(
            source,
            GeneratedSource {
                kind: &proposal.source_kind,
                options: &proposal.source_options,
            },
        )]),
        resource: BTreeMap::from([(
            resource,
            GeneratedResource {
                options: &proposal.resource_options,
                cursor,
                write_disposition: "append",
                trust: "governed",
            },
        )]),
    })
    .map_err(|error| {
        CliError::mapped(
            CdfError::internal(format!("serialize registered source add proposal: {error}")),
            error_catalog::PROJECT_IO,
        )
    })
}

fn appended_project_mapping(
    context: &ProjectContext,
    request: &AddResourceRequest,
) -> Result<(Vec<u8>, String), CliError> {
    let project_path = context.root.join(PROJECT_FILE_NAME);
    let mut project = fs::read_to_string(&project_path).map_err(|error| {
        CliError::mapped(
            CdfError::contract(format!("read {}: {error}", project_path.display())),
            error_catalog::PROJECT_IO,
        )
    })?;
    let prior = project.as_bytes().to_vec();
    while project.ends_with(['\n', '\r']) {
        project.pop();
    }
    project.push_str(&format!(
        "\n\n[resources.\"{}\"]\nsource = {}\n",
        request.resource_id,
        toml_string(&request.config_path_rel)?
    ));
    Ok((prior, project))
}

fn split_resource_id(id: &str) -> Result<(String, String), CliError> {
    let mut parts = id.split('.');
    let source = parts.next().unwrap_or_default();
    let resource = parts.next().unwrap_or_default();
    if source.is_empty() || resource.is_empty() || parts.next().is_some() {
        return Err(CliError::usage_with(
            "cdf add resource id must be exactly `<source>.<resource>`",
            error_catalog::USAGE,
        ));
    }
    for (label, value) in [("source", source), ("resource", resource)] {
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        {
            return Err(CliError::usage_with(
                format!(
                    "cdf add {label} component `{value}` must use only ASCII letters, digits, `_`, or `-`"
                ),
                error_catalog::USAGE,
            ));
        }
    }
    Ok((source.to_owned(), resource.to_owned()))
}

fn toml_string(value: &str) -> Result<String, CliError> {
    serde_json::to_string(value).map_err(crate::commands::json_cli_error)
}

fn field_type_label(data_type: &SchemaSnapshotDataType) -> String {
    match data_type {
        SchemaSnapshotDataType::Null => "null".to_owned(),
        SchemaSnapshotDataType::Boolean => "bool".to_owned(),
        SchemaSnapshotDataType::Int { signed, bits } => {
            format!("{}int{bits}", if *signed { "" } else { "u" })
        }
        SchemaSnapshotDataType::Float { bits } => format!("float{bits}"),
        SchemaSnapshotDataType::Decimal {
            bits,
            precision,
            scale,
        } => {
            format!("decimal{bits}({precision},{scale})")
        }
        SchemaSnapshotDataType::Timestamp { unit, timezone } => match timezone {
            Some(timezone) => format!("timestamp({unit:?}, {timezone})").to_lowercase(),
            None => format!("timestamp({unit:?})").to_lowercase(),
        },
        SchemaSnapshotDataType::Date { unit } => format!("date({unit:?})").to_lowercase(),
        SchemaSnapshotDataType::Time { unit, bits } => {
            format!("time{bits}({unit:?})").to_lowercase()
        }
        SchemaSnapshotDataType::Duration { unit } => format!("duration({unit:?})").to_lowercase(),
        SchemaSnapshotDataType::Interval { unit } => format!("interval({unit:?})").to_lowercase(),
        SchemaSnapshotDataType::Binary { offset_width } => format!("binary{offset_width}"),
        SchemaSnapshotDataType::FixedSizeBinary { byte_width } => {
            format!("fixed_size_binary({byte_width})")
        }
        SchemaSnapshotDataType::BinaryView => "binary_view".to_owned(),
        SchemaSnapshotDataType::Utf8 { offset_width } => {
            if *offset_width == 64 {
                "large_utf8".to_owned()
            } else {
                "utf8".to_owned()
            }
        }
        SchemaSnapshotDataType::Utf8View => "utf8_view".to_owned(),
        SchemaSnapshotDataType::List { field, .. } => {
            format!("list<{}>", field_type_label(&field.data_type))
        }
        SchemaSnapshotDataType::FixedSizeList { field, length } => {
            format!(
                "fixed_size_list<{}; {length}>",
                field_type_label(&field.data_type)
            )
        }
        SchemaSnapshotDataType::Struct { fields } => format!("struct<{} fields>", fields.len()),
        SchemaSnapshotDataType::Union { mode, fields } => {
            format!("union({mode:?}, {} fields)", fields.len()).to_lowercase()
        }
        SchemaSnapshotDataType::Dictionary {
            key_type,
            value_type,
        } => {
            format!(
                "dictionary<{}, {}>",
                field_type_label(key_type),
                field_type_label(value_type)
            )
        }
        SchemaSnapshotDataType::Map { field, .. } => {
            format!("map<{}>", field_type_label(&field.data_type))
        }
        SchemaSnapshotDataType::RunEndEncoded { run_ends, values } => {
            format!(
                "run_end_encoded<{}, {}>",
                field_type_label(&run_ends.data_type),
                field_type_label(&values.data_type)
            )
        }
        SchemaSnapshotDataType::Other { display } => display.clone(),
    }
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}
