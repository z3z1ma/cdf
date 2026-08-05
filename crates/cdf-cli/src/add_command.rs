use std::{collections::BTreeMap, env, fs};

use cdf_kernel::CdfError;
use cdf_project::{
    PROJECT_FILE_NAME, ProjectFileExpectation, ProjectFileWrite, parse_cdf_toml,
    parse_resource_file, publish_project_files_transactionally,
};
use cdf_runtime::{PlannedSourceAdd, SourceAddRequest, SourceRegistry};
use serde::Serialize;

use crate::{
    args::{AddArgs, Cli},
    context::{ProjectContext, project_authority_read_error},
    error_catalog,
    output::{CliError, CommandOutput},
};

pub(crate) fn add(
    cli: &Cli,
    args: AddArgs,
    _execution: &cdf_runtime::ExecutionServices,
    _destinations: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    let context = if args.dry_run {
        ProjectContext::load_for_command("add", cli.project.as_ref(), cli.env.as_deref())?
    } else {
        ProjectContext::load_for_command_with_recovery(
            "add",
            cli.project.as_ref(),
            cli.env.as_deref(),
        )?
    };
    let registry = crate::source_registry::builtin_source_registry()?;
    let request = AddResourceRequest::from_args(&context, registry, &args)?;
    ensure_add_is_available(&context, &request)?;
    let proposal = ProposedResource::compile(&context, registry, &request)?;
    let report = AddReport::from_parts(&context, &request, &proposal);
    if !args.dry_run {
        publish_add(&context, &request, &proposal)?;
    }
    CommandOutput::rendered("add", render::document(&report), report)
}

#[derive(Clone, Debug)]
struct AddResourceRequest {
    resource_id: String,
    namespace: String,
    resource: String,
    configured_source: String,
    plan: PlannedSourceAdd,
    resource_path: String,
    dry_run: bool,
}

impl AddResourceRequest {
    fn from_args(
        context: &ProjectContext,
        registry: &SourceRegistry,
        args: &AddArgs,
    ) -> Result<Self, CliError> {
        let (namespace, resource) = split_resource_id(&args.resource_id)?;
        let current_dir = env::current_dir().map_err(|error| {
            CdfError::environment(format!(
                "read current directory: {error}; change to an accessible directory before retrying"
            ))
        })?;
        let plan = registry
            .plan_add(
                SourceAddRequest {
                    source_name: namespace.clone(),
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
        Ok(Self {
            resource_id: args.resource_id.clone(),
            configured_source: namespace.clone(),
            resource_path: format!("cdf/{namespace}/{resource}.cdf.sql"),
            namespace,
            resource,
            plan,
            dry_run: args.dry_run,
        })
    }
}

struct ProposedResource {
    resource_sql: String,
    project_toml: String,
    project_prior: Vec<u8>,
    writes_source_config: bool,
}

impl ProposedResource {
    fn compile(
        context: &ProjectContext,
        registry: &SourceRegistry,
        request: &AddResourceRequest,
    ) -> Result<Self, CliError> {
        let project_path = context.root.join(PROJECT_FILE_NAME);
        let project_prior = fs::read(&project_path).map_err(|error| {
            add_project_read_error("read project configuration", &project_path, error)
        })?;
        let project_text = std::str::from_utf8(&project_prior).map_err(|error| {
            CliError::mapped(
                CdfError::contract(format!("parse {PROJECT_FILE_NAME} as UTF-8: {error}")),
                error_catalog::PROJECT_IO,
            )
        })?;
        let proposal = &request.plan.proposal;
        registry.validate_source_configuration(&proposal.source_kind, &proposal.source_options)?;
        registry
            .validate_resource_configuration(&proposal.source_kind, &proposal.resource_options)?;
        let (project_toml, writes_source_config) =
            project_with_source(context, project_text, request)?;
        parse_cdf_toml(&project_toml)?;
        let resource_sql = resource_sql(request)?;
        let authored = parse_resource_file(&resource_sql, &request.resource_path)?;
        let parsed = cdf_engine::parse_project_query_at(
            &authored.query_sql,
            &request.resource_path,
            authored.query_span.start_line,
            authored.query_span.start_column,
        )?;
        if parsed.upstream.configured_source != request.configured_source
            || parsed.upstream.resource_options != proposal.resource_options
        {
            return Err(CdfError::internal(
                "generated query-first resource did not round-trip through the project SQL parser",
            )
            .into());
        }
        Ok(Self {
            resource_sql,
            project_toml,
            project_prior,
            writes_source_config,
        })
    }
}

fn ensure_add_is_available(
    context: &ProjectContext,
    request: &AddResourceRequest,
) -> Result<(), CliError> {
    if context.has_resource(&request.resource_id) {
        return Err(CliError::usage_with(
            format!(
                "resource `{}` is already compiled; choose a new `<namespace>.<resource>` id",
                request.resource_id
            ),
            error_catalog::USAGE,
        ));
    }
    let resource_path = context.root.join(&request.resource_path);
    if add_target_exists(&resource_path)? {
        return Err(CliError::usage_with(
            format!(
                "cdf add would overwrite {}; choose a different resource id or edit that file explicitly",
                request.resource_path
            ),
            error_catalog::PROJECT_IO,
        ));
    }
    for private_file in &request.plan.proposal.private_files {
        if add_target_exists(&context.root.join(&private_file.relative_path))? {
            return Err(CliError::usage_with(
                format!(
                    "cdf add would overwrite private source state for configured source `{}`",
                    request.configured_source
                ),
                error_catalog::PROJECT_IO,
            ));
        }
    }
    Ok(())
}

fn project_with_source(
    context: &ProjectContext,
    project_text: &str,
    request: &AddResourceRequest,
) -> Result<(String, bool), CliError> {
    let proposal = &request.plan.proposal;
    if let Some(existing) = context.config.sources.get(&request.configured_source) {
        if existing.source_type != proposal.source_kind
            || existing.options != proposal.source_options
        {
            return Err(CliError::usage_with(
                format!(
                    "configured source `{}` already exists with different type or options; choose a distinct namespace/resource id or edit its shared [sources.{}] configuration explicitly",
                    request.configured_source, request.configured_source
                ),
                error_catalog::USAGE,
            ));
        }
        return Ok((project_text.to_owned(), false));
    }
    let source = toml::to_string_pretty(&GeneratedSourceDocument {
        sources: BTreeMap::from([(
            request.configured_source.as_str(),
            GeneratedConfiguredSource {
                source_type: &proposal.source_kind,
                options: &proposal.source_options,
            },
        )]),
    })
    .map_err(|error| CdfError::internal(format!("serialize configured source: {error}")))?;
    let mut project = project_text.trim_end_matches(['\n', '\r']).to_owned();
    project.push_str("\n\n");
    project.push_str(source.trim());
    project.push('\n');
    Ok((project, true))
}

fn resource_sql(request: &AddResourceRequest) -> Result<String, CliError> {
    registered_source_resource_sql(&request.configured_source, &request.plan)
}

pub(crate) fn registered_source_resource_sql(
    configured_source: &str,
    plan: &PlannedSourceAdd,
) -> Result<String, CliError> {
    validate_sql_name("configured source", configured_source)?;
    let proposal = &plan.proposal;
    let mut sql = String::from("RESOURCE\nDISPOSITION APPEND\n");
    if let Some(cursor) = &proposal.cursor {
        validate_sql_name("cursor field", &cursor.field)?;
        sql.push_str("CURSOR ");
        sql.push_str(&cursor.field);
        sql.push('\n');
    }
    sql.push_str("TRUST GOVERNED\nEXECUTION BOUNDED\nAS\nSELECT *\nFROM upstream(\n  source => '");
    sql.push_str(&sql_string(configured_source));
    sql.push('\'');
    for (name, value) in &proposal.resource_options {
        validate_sql_name("resource option", name)?;
        sql.push_str(",\n  ");
        sql.push_str(name);
        sql.push_str(" => ");
        sql.push_str(&sql_value(value)?);
    }
    sql.push_str("\n);\n");
    Ok(sql)
}

fn sql_value(value: &serde_json::Value) -> Result<String, CliError> {
    Ok(match value {
        serde_json::Value::Null => "NULL".to_owned(),
        serde_json::Value::Bool(value) => if *value { "TRUE" } else { "FALSE" }.to_owned(),
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::String(value) => {
            if value.starts_with("secret://") {
                return Err(CdfError::contract(
                    "resource arguments cannot contain secret references; put credentials in the configured source",
                )
                .into());
            }
            format!("'{}'", sql_string(value))
        }
        serde_json::Value::Array(values) => format!(
            "ARRAY [{}]",
            values
                .iter()
                .map(sql_value)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        ),
        serde_json::Value::Object(values) => {
            let mut members = Vec::with_capacity(values.len());
            for (name, value) in values {
                validate_sql_name("object key", name)?;
                members.push(format!("{name} => {}", sql_value(value)?));
            }
            format!("OBJECT({})", members.join(", "))
        }
    })
}

fn sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

fn validate_sql_name(label: &str, value: &str) -> Result<(), CliError> {
    let mut bytes = value.bytes();
    if value.len() > 128
        || !bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
        || bytes.any(|byte| !(byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'))
    {
        return Err(CliError::usage_with(
            format!(
                "{label} {value:?} cannot be represented by the project identifier grammar [a-z][a-z0-9_]{{0,127}}"
            ),
            error_catalog::USAGE,
        ));
    }
    Ok(())
}

fn publish_add(
    context: &ProjectContext,
    request: &AddResourceRequest,
    proposal: &ProposedResource,
) -> Result<(), CliError> {
    let mut writes = request
        .plan
        .proposal
        .private_files
        .iter()
        .map(|file| {
            Ok(ProjectFileWrite::new(
                file.relative_path.clone(),
                file.value.as_str()?.as_bytes().to_vec(),
                ProjectFileExpectation::Absent,
            )
            .owner_only())
        })
        .collect::<cdf_kernel::Result<Vec<_>>>()?;
    writes.push(ProjectFileWrite::new(
        &request.resource_path,
        proposal.resource_sql.as_bytes().to_vec(),
        ProjectFileExpectation::Absent,
    ));
    let commit_point = if proposal.writes_source_config {
        writes.push(ProjectFileWrite::new(
            PROJECT_FILE_NAME,
            proposal.project_toml.as_bytes().to_vec(),
            ProjectFileExpectation::Exact(proposal.project_prior.clone()),
        ));
        PROJECT_FILE_NAME
    } else {
        request.resource_path.as_str()
    };
    publish_project_files_transactionally(&context.root, commit_point, writes)?;
    Ok(())
}

#[derive(Serialize)]
struct GeneratedSourceDocument<'a> {
    sources: BTreeMap<&'a str, GeneratedConfiguredSource<'a>>,
}

#[derive(Serialize)]
struct GeneratedConfiguredSource<'a> {
    #[serde(rename = "type")]
    source_type: &'a str,
    #[serde(flatten)]
    options: &'a BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct AddReport {
    project: String,
    environment: String,
    resource_id: String,
    namespace: String,
    resource: String,
    configured_source: String,
    source_driver: String,
    resource_path: String,
    location: String,
    selection: String,
    write_disposition: &'static str,
    cursor: Option<String>,
    writes: AddWrites,
    next_command: String,
}

impl AddReport {
    fn from_parts(
        context: &ProjectContext,
        request: &AddResourceRequest,
        proposal: &ProposedResource,
    ) -> Self {
        Self {
            project: context.root.display().to_string(),
            environment: context.environment.name.clone(),
            resource_id: request.resource_id.clone(),
            namespace: request.namespace.clone(),
            resource: request.resource.clone(),
            configured_source: request.configured_source.clone(),
            source_driver: request.plan.driver.driver_id.as_str().to_owned(),
            resource_path: request.resource_path.clone(),
            location: request.plan.proposal.display_location.as_str().to_owned(),
            selection: request.plan.proposal.display_selection.clone(),
            write_disposition: "append",
            cursor: request
                .plan
                .proposal
                .cursor
                .as_ref()
                .map(|cursor| cursor.field.clone()),
            writes: AddWrites {
                resource_sql: !request.dry_run,
                configured_source: !request.dry_run && proposal.writes_source_config,
                private_source_state: !request.dry_run
                    && !request.plan.proposal.private_files.is_empty(),
                lockfile: false,
            },
            next_command: "cdf compile --refresh".to_owned(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct AddWrites {
    resource_sql: bool,
    configured_source: bool,
    private_source_state: bool,
    lockfile: bool,
}

fn add_target_exists(path: &std::path::Path) -> Result<bool, CliError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(add_project_read_error("inspect add target", path, error)),
    }
}

fn add_project_read_error(action: &str, path: &std::path::Path, error: std::io::Error) -> CliError {
    let error = project_authority_read_error(action, path, error);
    if error.kind == cdf_kernel::ErrorKind::Contract {
        CliError::mapped(error, error_catalog::PROJECT_IO)
    } else {
        error.into()
    }
}

fn split_resource_id(id: &str) -> Result<(String, String), CliError> {
    let Some((namespace, resource)) = id.split_once('.') else {
        return Err(CliError::usage_with(
            "cdf add resource id must be exactly `<namespace>.<resource>`",
            error_catalog::USAGE,
        ));
    };
    if resource.contains('.') {
        return Err(CliError::usage_with(
            "cdf add resource id must be exactly `<namespace>.<resource>`",
            error_catalog::USAGE,
        ));
    }
    validate_sql_name("resource namespace", namespace)?;
    validate_sql_name("resource", resource)?;
    Ok((namespace.to_owned(), resource.to_owned()))
}

mod render;
