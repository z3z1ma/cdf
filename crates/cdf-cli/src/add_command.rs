use std::{
    env, fs,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

use cdf_declarative::{
    CompiledResource, CompiledResourcePlan, compile_document_with_project_root,
    parse_toml as parse_declarative_toml,
};
use cdf_http::{SecretProvider, SecretUri, SecretValue};
use cdf_kernel::{CdfError, SchemaSource};
use cdf_project::{
    LOCK_FILE_NAME, PROJECT_FILE_NAME, ResourceSchemaDiscoveryArtifacts, SchemaSnapshotArtifact,
    SchemaSnapshotDataType, SchemaSnapshotField, freeze_contract_snapshots, lock_to_toml,
    parse_cdf_toml, write_schema_discovery_artifacts,
};
use serde::Serialize;

use crate::{
    args::{AddArgs, Cli},
    context::ProjectContext,
    error_catalog,
    http_transport::ReqwestHttpTransport,
    output::{CliError, CommandOutput},
    project_run_resource::file_runtime_dependencies,
    render::{
        RenderDocument,
        primitives::{KeyValuePanel, NextCommand, SectionRule, StatusKind, StatusLine, Table},
    },
};

pub(crate) fn add(
    cli: &Cli,
    args: AddArgs,
    execution: &cdf_runtime::ExecutionServices,
) -> Result<CommandOutput, CliError> {
    let context =
        ProjectContext::load_for_command("add", cli.project.as_ref(), cli.env.as_deref())?;
    let request = AddResourceRequest::from_args(&context, &args)?;
    let proposed = build_proposed_resource(&context, &request)?;
    ensure_add_is_available(&context, &request, &proposed)?;

    let direct_secret = match &request.target {
        AddRequestTarget::Postgres(target) => {
            Some((target.secret_ref.as_str(), target.dsn.as_str()))
        }
        AddRequestTarget::File(_) | AddRequestTarget::Rest(_) => None,
    };
    let add_secrets = AddSecretProvider {
        fallback: context.secret_provider(),
        direct: direct_secret,
    };
    let artifacts = discover_for_add(&context, &proposed.resource, &add_secrets, execution)?;
    let discovery = &artifacts.discovery;
    let pinned_resource = proposed.resource.with_schema_source_and_schema(
        SchemaSource::Discovered {
            snapshot: discovery.snapshot.reference.clone(),
        },
        Arc::clone(&discovery.normalized_schema),
    );
    let report = AddReport::from_parts(&context, &request, &proposed, &discovery.snapshot.artifact);

    if !args.dry_run {
        write_add_artifacts(&context, &request, &proposed, &pinned_resource, &artifacts)?;
    }

    CommandOutput::rendered("add", add_document(&report), report)
}

fn build_proposed_resource(
    context: &ProjectContext,
    request: &AddResourceRequest,
) -> Result<ProposedResource, CliError> {
    let resource_toml = resource_toml(request)?;
    let document = parse_declarative_toml(&resource_toml)?;
    let mut resources = compile_document_with_project_root(&document, &context.root)?;
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
    Ok(ProposedResource {
        resource,
        resource_toml,
        project_toml: appended_project_mapping(context, request)?,
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
    if let AddRequestTarget::Postgres(target) = &request.target
        && context.root.join(&target.secret_path).exists()
    {
        return Err(CliError::usage_with(
            format!(
                "cdf add would overwrite private secret state for source `{}`",
                request.source
            ),
            error_catalog::PROJECT_IO,
        ));
    }
    parse_cdf_toml(&proposed.project_toml)?;
    Ok(())
}

fn discover_for_add(
    context: &ProjectContext,
    resource: &CompiledResource,
    secret_provider: &dyn SecretProvider,
    execution: &cdf_runtime::ExecutionServices,
) -> Result<ResourceSchemaDiscoveryArtifacts, CliError> {
    match resource.plan() {
        CompiledResourcePlan::Files(plan)
            if ["http://", "https://", "s3://", "gs://", "az://"]
                .iter()
                .any(|scheme| plan.root.starts_with(scheme)) =>
        {
            Ok(
                cdf_project::discover_resource_schema_with_file_dependencies_artifacts(
                    resource,
                    secret_provider,
                    file_runtime_dependencies(context, Some(execution))?,
                    Default::default(),
                )?,
            )
        }
        CompiledResourcePlan::Files(_) => Ok(cdf_project::discover_resource_schema_artifacts(
            resource,
            secret_provider,
            Default::default(),
        )?),
        CompiledResourcePlan::Sql(_) => Ok(cdf_project::discover_resource_schema_artifacts(
            resource,
            secret_provider,
            Default::default(),
        )?),
        CompiledResourcePlan::Rest(_) => {
            let transport = ReqwestHttpTransport::new()?;
            Ok(ResourceSchemaDiscoveryArtifacts::new(
                cdf_project::discover_resource_schema_with_rest_transport(
                    resource,
                    secret_provider,
                    &transport,
                )?,
                None,
            ))
        }
    }
}

struct AddSecretProvider<'a> {
    fallback: cdf_project::DefaultSecretProvider,
    direct: Option<(&'a str, &'a str)>,
}

impl SecretProvider for AddSecretProvider<'_> {
    fn resolve(&self, uri: &SecretUri) -> cdf_kernel::Result<SecretValue> {
        if let Some((reference, value)) = self.direct
            && uri.as_str() == reference
        {
            return Ok(SecretValue::new(value));
        }
        self.fallback.resolve(uri)
    }
}

fn write_add_artifacts(
    context: &ProjectContext,
    request: &AddResourceRequest,
    proposed: &ProposedResource,
    pinned_resource: &CompiledResource,
    artifacts: &ResourceSchemaDiscoveryArtifacts,
) -> Result<(), CliError> {
    let mut resources = context.resources.clone();
    resources.push(pinned_resource.clone());
    let updated_config = parse_cdf_toml(&proposed.project_toml)?;
    let destination_artifacts = crate::destination_registry::inspect_destination_artifacts(
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

    if let AddRequestTarget::Postgres(target) = &request.target {
        let path = context.root.join(&target.secret_path);
        fs::create_dir_all(path.parent().expect("secret path has parent")).map_err(|error| {
            CliError::mapped(
                CdfError::contract(format!("create private source secret directory: {error}")),
                error_catalog::PROJECT_IO,
            )
        })?;
        let mut file = open_private_source_secret(&path).map_err(|error| {
            CliError::mapped(
                CdfError::contract(format!("create private source secret: {error}")),
                error_catalog::PROJECT_IO,
            )
        })?;
        file.write_all(target.dsn.as_bytes()).map_err(|error| {
            CliError::mapped(
                CdfError::contract(format!("write private source secret: {error}")),
                error_catalog::PROJECT_IO,
            )
        })?;
        file.sync_all().map_err(|error| {
            CliError::mapped(
                CdfError::contract(format!("sync private source secret: {error}")),
                error_catalog::PROJECT_IO,
            )
        })?;
    }

    write_schema_discovery_artifacts(&context.root, artifacts)?;
    fs::create_dir_all(request.config_path_abs.parent().ok_or_else(|| {
        CliError::mapped(
            CdfError::internal("generated resource config path has no parent"),
            error_catalog::PROJECT_IO,
        )
    })?)
    .map_err(|error| {
        CliError::mapped(
            CdfError::contract(format!(
                "create {}: {error}",
                request
                    .config_path_abs
                    .parent()
                    .expect("checked above")
                    .display()
            )),
            error_catalog::PROJECT_IO,
        )
    })?;
    fs::write(&request.config_path_abs, &proposed.resource_toml).map_err(|error| {
        CliError::mapped(
            CdfError::contract(format!("write {}: {error}", request.config_path_rel)),
            error_catalog::PROJECT_IO,
        )
    })?;
    fs::write(context.root.join(PROJECT_FILE_NAME), &proposed.project_toml).map_err(|error| {
        CliError::mapped(
            CdfError::contract(format!("write {}: {error}", PROJECT_FILE_NAME)),
            error_catalog::PROJECT_IO,
        )
    })?;
    cdf_project::write_lock_file_guarded(
        context.root.join(LOCK_FILE_NAME),
        context.lock_authority.as_ref(),
        lock_toml,
    )
    .map_err(|error| CliError::mapped(error, error_catalog::PROJECT_IO))?;
    Ok(())
}

#[cfg(unix)]
fn open_private_source_secret(path: &Path) -> std::io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;

    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
}

#[cfg(not(unix))]
fn open_private_source_secret(_path: &Path) -> std::io::Result<File> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "direct DSN persistence requires an owner-only file-permission implementation on this platform; configure the source with an existing secret reference",
    ))
}

#[derive(Clone, Debug)]
struct AddResourceRequest {
    resource_id: String,
    source: String,
    resource: String,
    target: AddRequestTarget,
    config_path_rel: String,
    config_path_abs: PathBuf,
    dry_run: bool,
}

impl AddResourceRequest {
    fn from_args(context: &ProjectContext, args: &AddArgs) -> Result<Self, CliError> {
        let (source, resource) = split_resource_id(&args.resource_id)?;
        let rest_options = [&args.records, &args.cursor, &args.cursor_param];
        let rest_option_count = rest_options.iter().filter(|value| value.is_some()).count();
        if rest_option_count != 0 && rest_option_count != rest_options.len() {
            return Err(CliError::usage_with(
                "REST cdf add requires --records, --cursor, and --cursor-param together",
                error_catalog::USAGE,
            ));
        }
        let target = if args.location.starts_with("postgres://")
            || args.location.starts_with("postgresql://")
        {
            AddRequestTarget::Postgres(PostgresAddTarget::from_dsn(&source, &args.location)?)
        } else if rest_option_count == rest_options.len() {
            AddRequestTarget::Rest(RestAddTarget::from_url(
                &args.location,
                args.records.as_deref().expect("count checked"),
                args.cursor.as_deref().expect("count checked"),
                args.cursor_param.as_deref().expect("count checked"),
            )?)
        } else {
            AddRequestTarget::File(AddTarget::from_location(context, &args.location)?)
        };
        let config_path_rel = format!("resources/{source}.toml");
        let config_path_abs = context.root.join(&config_path_rel);
        Ok(Self {
            resource_id: args.resource_id.clone(),
            source,
            resource,
            target,
            config_path_rel,
            config_path_abs,
            dry_run: args.dry_run,
        })
    }
}

#[derive(Clone, Debug)]
enum AddRequestTarget {
    File(AddTarget),
    Postgres(PostgresAddTarget),
    Rest(RestAddTarget),
}

#[derive(Clone, Debug)]
struct RestAddTarget {
    base_url: String,
    path: String,
    records: String,
    cursor: String,
    cursor_param: String,
    host: String,
}

impl RestAddTarget {
    fn from_url(
        url: &str,
        records: &str,
        cursor: &str,
        cursor_param: &str,
    ) -> Result<Self, CliError> {
        let parsed = reqwest::Url::parse(url).map_err(|error| {
            CliError::usage_with(
                format!(
                    "cdf add could not parse REST URL `{}`: {error}",
                    redact_url(url)
                ),
                error_catalog::USAGE,
            )
        })?;
        match parsed.scheme() {
            "https" => {}
            "http" if is_loopback_host(&parsed) => {}
            other => {
                return Err(CliError::usage_with(
                    format!(
                        "cdf add REST endpoints require HTTPS (or loopback HTTP for local development); `{other}` is not supported"
                    ),
                    error_catalog::USAGE,
                ));
            }
        }
        if !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(CliError::usage_with(
                "cdf add REST URL must not contain userinfo, query secrets, or fragments; declare stable params/auth in the generated resource",
                error_catalog::USAGE,
            ));
        }
        if records.trim().is_empty() || cursor.trim().is_empty() || cursor_param.trim().is_empty() {
            return Err(CliError::usage_with(
                "REST --records, --cursor, and --cursor-param values must be non-empty",
                error_catalog::USAGE,
            ));
        }
        let host = parsed.host_str().ok_or_else(|| {
            CliError::usage_with("cdf add REST URL must contain a host", error_catalog::USAGE)
        })?;
        let mut origin = parsed.clone();
        origin.set_path("");
        origin.set_query(None);
        origin.set_fragment(None);
        let path = if parsed.path().is_empty() {
            "/".to_owned()
        } else {
            parsed.path().to_owned()
        };
        Ok(Self {
            base_url: origin.as_str().trim_end_matches('/').to_owned(),
            path,
            records: records.to_owned(),
            cursor: cursor.to_owned(),
            cursor_param: cursor_param.to_owned(),
            host: host.to_owned(),
        })
    }
}

#[derive(Clone, Debug)]
struct PostgresAddTarget {
    dsn: String,
    display_dsn: String,
    table: String,
    secret_path: String,
    secret_ref: String,
}

impl PostgresAddTarget {
    fn from_dsn(source: &str, dsn: &str) -> Result<Self, CliError> {
        let mut parsed = reqwest::Url::parse(dsn).map_err(|error| {
            CliError::usage_with(
                format!("cdf add could not parse Postgres DSN: {error}"),
                error_catalog::USAGE,
            )
        })?;
        if parsed.scheme() != "postgres" && parsed.scheme() != "postgresql" {
            return Err(CliError::usage_with(
                "cdf add SQL source must use postgres:// or postgresql://",
                error_catalog::USAGE,
            ));
        }
        let mut segments = parsed
            .path_segments()
            .map(|segments| {
                segments
                    .filter(|segment| !segment.is_empty())
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if segments.len() < 2 {
            return Err(CliError::usage_with(
                "cdf add Postgres DSN must end with `/database/table`",
                error_catalog::USAGE,
            ));
        }
        let table = segments.pop().unwrap();
        parsed.set_path(&format!("/{}", segments.join("/")));
        let dsn = parsed.to_string();
        let display_dsn = redact_url(&dsn);
        let secret_path = format!(".cdf/secrets/sources/{source}.dsn");
        Ok(Self {
            dsn,
            display_dsn,
            table,
            secret_ref: format!("secret://file/{secret_path}"),
            secret_path,
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AddTarget {
    pub(crate) source_root: String,
    pub(crate) display_source_root: String,
    pub(crate) glob: String,
    pub(crate) canonical_location: String,
    pub(crate) is_http: bool,
}

impl AddTarget {
    fn from_location(context: &ProjectContext, location: &str) -> Result<Self, CliError> {
        Self::from_location_for("cdf add", context, location)
    }

    pub(crate) fn from_adhoc_location(
        context: &ProjectContext,
        location: &str,
    ) -> Result<Self, CliError> {
        if looks_like_http_url(location) {
            return Self::from_http_url("cdf run ad-hoc", location);
        }
        Self::from_local_path("cdf run ad-hoc", context, location, true)
    }

    pub(crate) fn from_location_for(
        command: &str,
        context: &ProjectContext,
        location: &str,
    ) -> Result<Self, CliError> {
        if looks_like_http_url(location) {
            return Self::from_http_url(command, location);
        }
        Self::from_local_path(command, context, location, false)
    }

    fn from_http_url(command: &str, location: &str) -> Result<Self, CliError> {
        let parsed = reqwest::Url::parse(location).map_err(|error| {
            CliError::usage_with(
                format!(
                    "{command} could not parse URL `{}`: {error}",
                    redact_url(location)
                ),
                error_catalog::USAGE,
            )
        })?;
        match parsed.scheme() {
            "https" => {}
            "http" if is_loopback_host(&parsed) => {}
            other => {
                return Err(CliError::usage_with(
                    format!(
                        "{command} supports HTTPS Parquet URLs in this slice; `{other}` is not supported"
                    ),
                    error_catalog::USAGE,
                ));
            }
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(CliError::usage_with(
                format!(
                    "{command} does not accept URL userinfo credentials; use a stable public HTTPS URL or a later secret-backed source"
                ),
                error_catalog::USAGE,
            ));
        }
        if parsed.query().is_some() || parsed.fragment().is_some() {
            return Err(CliError::usage_with(
                format!(
                    "{command} does not write signed URL query or fragment material into project config; use a stable HTTPS URL or a later secret-backed source path (`{}`)",
                    redact_url(location)
                ),
                error_catalog::USAGE,
            ));
        }
        let glob = parsed
            .path_segments()
            .and_then(|mut segments| segments.next_back())
            .filter(|segment| !segment.is_empty())
            .ok_or_else(|| {
                CliError::usage_with(
                    format!(
                        "{command} URL `{}` does not name a Parquet file",
                        redact_url(location)
                    ),
                    error_catalog::USAGE,
                )
            })?
            .to_owned();
        ensure_parquet_name(command, &glob, &redact_url(location))?;

        let canonical_location = parsed.to_string();

        let mut root = parsed.clone();
        let mut parent_segments = parsed
            .path_segments()
            .map(|segments| segments.collect::<Vec<_>>())
            .unwrap_or_default();
        parent_segments.pop();
        let parent_path = if parent_segments.is_empty() {
            "/".to_owned()
        } else {
            format!("/{}", parent_segments.join("/"))
        };
        root.set_path(&parent_path);
        root.set_query(None);
        root.set_fragment(None);
        let source_root = root.as_str().trim_end_matches('/').to_owned();
        let source_root = if source_root.ends_with("://") {
            format!("{source_root}/")
        } else {
            source_root
        };
        Ok(Self {
            display_source_root: redact_url(&source_root),
            source_root,
            glob,
            canonical_location,
            is_http: true,
        })
    }

    fn from_local_path(
        command: &str,
        context: &ProjectContext,
        location: &str,
        redact_path: bool,
    ) -> Result<Self, CliError> {
        let input = PathBuf::from(location);
        let cwd = env::current_dir().map_err(|error| {
            CliError::mapped(
                CdfError::internal(format!("read current directory: {error}")),
                error_catalog::PROJECT_IO,
            )
        })?;
        let candidates = if input.is_absolute() {
            vec![input.clone()]
        } else {
            vec![cwd.join(&input), context.root.join(&input)]
        };
        let file = candidates
            .into_iter()
            .find(|candidate| candidate.is_file())
            .ok_or_else(|| {
                CliError::usage_with(
                    format!(
                        "{command} could not find local Parquet file `{}`",
                        local_path_display(location, redact_path)
                    ),
                    error_catalog::USAGE,
                )
            })?;
        let canonical_file = fs::canonicalize(&file).map_err(|error| {
            CliError::mapped(
                CdfError::contract(format!(
                    "canonicalize {}: {error}",
                    local_path_display(&file.display().to_string(), redact_path)
                )),
                error_catalog::PROJECT_IO,
            )
        })?;
        let glob = canonical_file
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                CliError::usage_with(
                    format!(
                        "{command} local path `{}` has no UTF-8 file name",
                        local_path_display(&file.display().to_string(), redact_path)
                    ),
                    error_catalog::USAGE,
                )
            })?
            .to_owned();
        ensure_parquet_name(
            command,
            &glob,
            &local_path_display(&file.display().to_string(), redact_path),
        )?;
        let parent = canonical_file.parent().ok_or_else(|| {
            CliError::usage_with(
                format!(
                    "{command} local path `{}` has no parent directory",
                    local_path_display(&file.display().to_string(), redact_path)
                ),
                error_catalog::USAGE,
            )
        })?;
        let project_root = fs::canonicalize(&context.root).map_err(|error| {
            CliError::mapped(
                CdfError::contract(format!(
                    "canonicalize project root {}: {error}",
                    local_path_display(&context.root.display().to_string(), redact_path)
                )),
                error_catalog::PROJECT_IO,
            )
        })?;
        let source_root = match parent.strip_prefix(&project_root) {
            Ok(relative) if relative.as_os_str().is_empty() => ".".to_owned(),
            Ok(relative) => path_to_toml_string_for(relative, redact_path)?,
            Err(_) => path_to_toml_string_for(parent, redact_path)?,
        };
        Ok(Self {
            display_source_root: source_root.clone(),
            source_root,
            glob,
            canonical_location: path_to_toml_string_for(&canonical_file, redact_path)?,
            is_http: false,
        })
    }
}

struct ProposedResource {
    resource: CompiledResource,
    resource_toml: String,
    project_toml: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct AddReport {
    project: String,
    environment: String,
    resource_id: String,
    source: String,
    resource: String,
    config_path: String,
    schema_hash: String,
    schema_snapshot_path: String,
    source_root: String,
    glob: String,
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
        let _ = proposed;
        let (source_root, glob) = match &request.target {
            AddRequestTarget::File(target) => {
                (target.display_source_root.clone(), target.glob.clone())
            }
            AddRequestTarget::Postgres(target) => {
                (target.display_dsn.clone(), target.table.clone())
            }
            AddRequestTarget::Rest(target) => (target.base_url.clone(), target.path.clone()),
        };
        let cursor_candidates = if matches!(&request.target, AddRequestTarget::Postgres(_)) {
            snapshot
                .schema
                .fields
                .iter()
                .filter(|field| {
                    matches!(
                        field.data_type,
                        SchemaSnapshotDataType::Int { .. }
                            | SchemaSnapshotDataType::Timestamp { .. }
                            | SchemaSnapshotDataType::Date { .. }
                    )
                })
                .map(|field| field.name.clone())
                .collect()
        } else {
            Vec::new()
        };
        Self {
            project: context.root.display().to_string(),
            environment: context.environment.name.clone(),
            resource_id: request.resource_id.clone(),
            source: request.source.clone(),
            resource: request.resource.clone(),
            config_path: request.config_path_rel.clone(),
            schema_hash: snapshot.schema_hash.to_string(),
            schema_snapshot_path: snapshot.path.clone(),
            source_root,
            glob,
            write_disposition: "append",
            schema_source: "discovered",
            fields: snapshot
                .schema
                .fields
                .iter()
                .map(AddFieldReport::from_field)
                .collect(),
            cursor: match &request.target {
                AddRequestTarget::Rest(target) => Some(target.cursor.clone()),
                AddRequestTarget::File(_) | AddRequestTarget::Postgres(_) => None,
            },
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
                .row("config", report.config_path.clone())
                .row("root", report.source_root.clone())
                .row("glob", report.glob.clone())
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

fn resource_toml(request: &AddResourceRequest) -> Result<String, CliError> {
    match &request.target {
        AddRequestTarget::File(target) => {
            parquet_resource_toml(&request.source, &request.resource, target)
        }
        AddRequestTarget::Postgres(target) => Ok(format!(
            "[source.{}]\nkind = \"sql\"\nconnection = {}\n\n[resource.{}]\ntable = {}\nwrite_disposition = \"append\"\ntrust = \"governed\"\n",
            request.source,
            toml_string(&target.secret_ref)?,
            request.resource,
            toml_string(&target.table)?,
        )),
        AddRequestTarget::Rest(target) => Ok(format!(
            "[source.{}]\nkind = \"rest\"\nbase_url = {}\negress_allowlist = [{}]\n\n[resource.{}]\npath = {}\nrecords = {}\ncursor = {{ field = {}, param = {}, ordering = \"best_effort\", lag = \"0ms\" }}\nwrite_disposition = \"append\"\ntrust = \"governed\"\n",
            request.source,
            toml_string(&target.base_url)?,
            toml_string(&target.host)?,
            request.resource,
            toml_string(&target.path)?,
            toml_string(&target.records)?,
            toml_string(&target.cursor)?,
            toml_string(&target.cursor_param)?,
        )),
    }
}

pub(crate) fn parquet_resource_toml(
    source: &str,
    resource: &str,
    target: &AddTarget,
) -> Result<String, CliError> {
    let mut source_lines = format!(
        "[source.{}]\nkind = \"files\"\nroot = {}\n",
        source,
        toml_string(&target.source_root)?
    );
    if let Some(host) = http_host(&target.source_root) {
        source_lines.push_str(&format!("egress_allowlist = [{}]\n", toml_string(&host)?));
    }
    Ok(format!(
        "{}\n[resource.{}]\nglob = {}\nformat = \"parquet\"\nwrite_disposition = \"append\"\ntrust = \"governed\"\n",
        source_lines,
        resource,
        toml_string(&target.glob)?
    ))
}

fn appended_project_mapping(
    context: &ProjectContext,
    request: &AddResourceRequest,
) -> Result<String, CliError> {
    let project_path = context.root.join(PROJECT_FILE_NAME);
    let mut project = fs::read_to_string(&project_path).map_err(|error| {
        CliError::mapped(
            CdfError::contract(format!("read {}: {error}", project_path.display())),
            error_catalog::PROJECT_IO,
        )
    })?;
    while project.ends_with(['\n', '\r']) {
        project.pop();
    }
    project.push_str(&format!(
        "\n\n[resources.\"{}\"]\nsource = {}\n",
        request.resource_id,
        toml_string(&request.config_path_rel)?
    ));
    Ok(project)
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
        if !is_bare_toml_key(value) {
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

fn is_bare_toml_key(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

fn ensure_parquet_name(command: &str, name: &str, display: &str) -> Result<(), CliError> {
    if name.to_ascii_lowercase().ends_with(".parquet") {
        return Ok(());
    }
    Err(CliError::usage_with(
        format!(
            "{command} supports single-file Parquet in this slice; `{display}` is not .parquet"
        ),
        error_catalog::USAGE,
    ))
}

fn looks_like_http_url(value: &str) -> bool {
    value.starts_with("http://") || value.starts_with("https://")
}

fn is_loopback_host(url: &reqwest::Url) -> bool {
    matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "::1"))
}

fn http_host(value: &str) -> Option<String> {
    reqwest::Url::parse(value)
        .ok()
        .filter(|url| matches!(url.scheme(), "http" | "https"))
        .and_then(|url| url.host_str().map(ToOwned::to_owned))
}

fn redact_url(value: &str) -> String {
    match reqwest::Url::parse(value) {
        Ok(mut url) => {
            if !url.username().is_empty() || url.password().is_some() {
                let _ = url.set_username("");
                let _ = url.set_password(None);
            }
            if url.query().is_some() {
                url.set_query(Some("[redacted]"));
            }
            if url.fragment().is_some() {
                url.set_fragment(Some("[redacted]"));
            }
            url.to_string()
        }
        Err(_) => "[redacted-url]".to_owned(),
    }
}

fn local_path_display(value: &str, redact: bool) -> String {
    if redact {
        "[redacted-local-parquet-path]".to_owned()
    } else {
        value.to_owned()
    }
}

fn path_to_toml_string(path: &Path) -> Result<String, CliError> {
    path.to_str()
        .map(|value| value.replace(std::path::MAIN_SEPARATOR, "/"))
        .ok_or_else(|| {
            CliError::usage_with(
                format!("path `{}` is not valid UTF-8", path.display()),
                error_catalog::USAGE,
            )
        })
}

fn path_to_toml_string_for(path: &Path, redact: bool) -> Result<String, CliError> {
    if !redact {
        return path_to_toml_string(path);
    }
    path.to_str()
        .map(|value| value.replace(std::path::MAIN_SEPARATOR, "/"))
        .ok_or_else(|| {
            CliError::usage_with(
                "path `[redacted-local-parquet-path]` is not valid UTF-8",
                error_catalog::USAGE,
            )
        })
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
