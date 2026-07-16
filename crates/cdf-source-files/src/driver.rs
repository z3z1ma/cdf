use std::{collections::BTreeMap, sync::Arc};

use cdf_http::{AuthScheme, EgressAllowlist, SecretProvider, SecretUri};
use cdf_kernel::{CdfError, QueryableResource, ResourceStream, Result, ScanRequest};
use cdf_runtime::{
    CompiledFormatBinding, CompiledSourcePlan, ExecutionServices, FormatDiscoveryKind,
    FormatRegistry, SourceAttestationStrength, SourceCompileRequest, SourceDiscoveryCandidate,
    SourceDiscoveryKind, SourceDiscoveryRequest, SourceDiscoverySession, SourceDriver,
    SourceDriverDescriptor, SourceDriverId, SourceExecutionCapabilities, SourceExecutorClass,
    SourceResolutionContext, SourceRetryGranularity, SourceSchemaObservation, artifact_hash,
};
use serde::{Deserialize, Serialize};

use crate::{
    BoundedSchemaDiscoveryRequest, FileCompressionDeclaration, FileFormatDeclaration, FileResource,
    FileResourceDefinition, FileResourcePlan, FileRuntimeDependencies, FileTransportLocation,
    FileTransportResource, discover_local_binary_schema_bounded,
    discover_transport_binary_schema_bounded, file_source_blocking_lane,
    local_file_discovery_candidates,
};

type RuntimeFactory = dyn Fn(Arc<dyn SecretProvider + Send + Sync>, ExecutionServices) -> Result<FileRuntimeDependencies>
    + Send
    + Sync
    + 'static;

#[derive(Clone)]
pub struct FileSourceDriver {
    descriptor: SourceDriverDescriptor,
    option_schema: serde_json::Value,
    formats: Arc<FormatRegistry>,
    runtime_factory: Arc<RuntimeFactory>,
}

impl std::fmt::Debug for FileSourceDriver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FileSourceDriver")
            .field("descriptor", &self.descriptor)
            .finish_non_exhaustive()
    }
}

impl FileSourceDriver {
    pub fn new<F>(formats: Arc<FormatRegistry>, runtime_factory: F) -> Result<Self>
    where
        F: Fn(
                Arc<dyn SecretProvider + Send + Sync>,
                ExecutionServices,
            ) -> Result<FileRuntimeDependencies>
            + Send
            + Sync
            + 'static,
    {
        let option_schema = option_schema();
        Ok(Self {
            descriptor: SourceDriverDescriptor {
                driver_id: SourceDriverId::new("files")?,
                driver_version: "1.0.0".to_owned(),
                option_schema_hash: artifact_hash(&option_schema)?,
                kinds: vec!["files".to_owned()],
                schemes: vec![
                    "file".to_owned(),
                    "s3".to_owned(),
                    "gs".to_owned(),
                    "az".to_owned(),
                    "http".to_owned(),
                    "https".to_owned(),
                ],
            },
            option_schema,
            formats,
            runtime_factory: Arc::new(runtime_factory),
        })
    }
}

impl SourceDriver for FileSourceDriver {
    fn descriptor(&self) -> &SourceDriverDescriptor {
        &self.descriptor
    }

    fn option_schema(&self) -> &serde_json::Value {
        &self.option_schema
    }

    fn compile(&self, request: SourceCompileRequest) -> Result<CompiledSourcePlan> {
        request.context.validate()?;
        let source_name = request.context.source_name.clone();
        let source: FileSourceOptions = decode_options("file source", request.source_options)?;
        let resource: FileResourceOptions =
            decode_options("file resource", request.resource_options)?;
        let unresolved = FileResourcePlan {
            source: source_name.clone(),
            root: source.root.clone(),
            glob: resource.glob.clone(),
            format: resource.format.clone(),
            format_declared: resource.format.is_some(),
            format_options: resource.format_options.clone(),
            compression: resource.compression.clone(),
            auth: source
                .auth
                .as_ref()
                .map(AuthOptions::to_runtime)
                .transpose()?,
            credentials: source
                .credentials
                .as_ref()
                .map(|value| SecretUri::new(value.clone()))
                .transpose()?,
            allowlist: if source.egress_allowlist.is_empty() {
                EgressAllowlist::allow_any()
            } else {
                EgressAllowlist::from_hosts(source.egress_allowlist.clone())
            },
        };
        let (resource_plan, compiled_format) =
            compile_file_resource_plan(&unresolved, self.formats.as_ref())?;
        let resolved_format = resource_plan.resolved_format()?.clone();
        let physical = FilePhysicalPlan {
            source_name,
            source,
            resource: CompiledFileResourceOptions {
                glob: resource_plan.glob,
                format: resolved_format,
                format_declared: resource_plan.format_declared,
                format_options: resource_plan.format_options,
                compression: resource_plan.compression,
            },
            compiled_format,
        };
        physical.validate()?;
        CompiledSourcePlan::new(
            self.descriptor.clone(),
            crate::file_resource_capabilities(&physical.compiled_format.descriptor),
            execution_capabilities(),
            cdf_runtime::CompiledSourcePlanInput {
                descriptor: request.descriptor,
                schema: request.schema,
                type_policy_allowances: request.type_policy_allowances,
                effective_schema_runtime: request.effective_schema_runtime,
                baseline_observation_schema_catalog: request.baseline_observation_schema_catalog,
                redacted_options: serde_json::to_value(&physical).map_err(serialize_error)?,
                physical_plan: serde_json::to_value(&physical).map_err(serialize_error)?,
            },
        )
    }

    fn discovery_session(
        &self,
        plan: &CompiledSourcePlan,
        context: &SourceResolutionContext<'_>,
    ) -> Result<Box<dyn SourceDiscoverySession>> {
        plan.validate()?;
        let physical: FilePhysicalPlan = serde_json::from_value(plan.physical_plan.clone())
            .map_err(|error| CdfError::contract(format!("invalid file source plan: {error}")))?;
        validate_compiled_capabilities(plan, &physical.compiled_format)?;
        let dependencies = (self.runtime_factory)(
            Arc::clone(context.secret_provider()),
            context.execution().clone(),
        )?
        .with_prepared_payloads(context.prepared_payloads().clone());
        physical.compiled_format.verify(dependencies.formats())?;
        let runtime_plan = physical.to_runtime_plan(context.project_root())?;
        let entries = file_discovery_entries(
            plan,
            &runtime_plan,
            &physical.compiled_format,
            &dependencies,
        )?;
        Ok(Box::new(FileDriverDiscoverySession {
            resource_id: plan.descriptor.resource_id.clone(),
            plan: runtime_plan,
            compiled_format: physical.compiled_format,
            dependencies,
            entries,
        }))
    }

    fn resolve(
        &self,
        plan: &CompiledSourcePlan,
        context: &SourceResolutionContext<'_>,
    ) -> Result<Arc<dyn QueryableResource>> {
        let physical: FilePhysicalPlan = serde_json::from_value(plan.physical_plan.clone())
            .map_err(|error| CdfError::contract(format!("invalid file source plan: {error}")))?;
        validate_compiled_capabilities(plan, &physical.compiled_format)?;
        let dependencies = (self.runtime_factory)(
            Arc::clone(context.secret_provider()),
            context.execution().clone(),
        )?
        .with_prepared_payloads(context.prepared_payloads().clone());
        physical.compiled_format.verify(dependencies.formats())?;
        Ok(Arc::new(
            FileResource::new(
                FileResourceDefinition {
                    descriptor: plan.descriptor.clone(),
                    schema: Arc::new(plan.schema.clone()),
                    plan: physical.to_runtime_plan(context.project_root())?,
                    type_policy_allowances: plan.type_policy_allowances,
                    effective_schema_runtime: plan.effective_schema_runtime.clone(),
                    compiled_format: physical.compiled_format,
                },
                dependencies,
            )?
            .with_compiled_source_plan_hash(cdf_runtime::artifact_hash(plan)?),
        ))
    }
}

fn validate_compiled_capabilities(
    plan: &CompiledSourcePlan,
    format: &CompiledFormatBinding,
) -> Result<()> {
    let executable = crate::file_resource_capabilities(&format.descriptor);
    if plan.resource_capabilities != executable {
        return Err(CdfError::contract(format!(
            "compiled file source capabilities do not match format `{}` execution capabilities",
            format.descriptor.format_id
        )));
    }
    Ok(())
}

#[derive(Clone)]
struct FileDriverDiscoveryEntry {
    candidate: SourceDiscoveryCandidate,
    compression: String,
    source: FileDriverDiscoverySource,
}

#[derive(Clone)]
enum FileDriverDiscoverySource {
    Local {
        path: std::path::PathBuf,
        selection_bytes_read: u64,
    },
    Transport(FileTransportResource),
}

struct FileDriverDiscoverySession {
    resource_id: cdf_kernel::ResourceId,
    plan: FileResourcePlan,
    compiled_format: CompiledFormatBinding,
    dependencies: FileRuntimeDependencies,
    entries: Vec<FileDriverDiscoveryEntry>,
}

impl SourceDiscoverySession for FileDriverDiscoverySession {
    fn kind(&self) -> SourceDiscoveryKind {
        match self.compiled_format.descriptor.discovery_kind {
            FormatDiscoveryKind::FormatMetadata => SourceDiscoveryKind::SchemaMetadata,
            FormatDiscoveryKind::BoundedContent => SourceDiscoveryKind::BoundedContent,
            FormatDiscoveryKind::FullContent => SourceDiscoveryKind::FullContent,
        }
    }

    fn candidates(&self) -> Result<Vec<SourceDiscoveryCandidate>> {
        Ok(self
            .entries
            .iter()
            .map(|entry| entry.candidate.clone())
            .collect())
    }

    fn observe(
        &self,
        candidate: &SourceDiscoveryCandidate,
        request: &SourceDiscoveryRequest,
    ) -> Result<SourceSchemaObservation> {
        request.validate()?;
        let entry = self
            .entries
            .iter()
            .find(|entry| entry.candidate == *candidate)
            .ok_or_else(|| {
                CdfError::contract(format!(
                    "file discovery candidate `{}` was not produced by the compiled inventory",
                    candidate.canonical_location
                ))
            })?;
        let format = self.plan.resolved_format()?;
        let probe_request = BoundedSchemaDiscoveryRequest {
            resource_id: &self.resource_id,
            format,
            format_declared: self.plan.format_declared,
            format_options: &self.compiled_format.canonical_options,
            transform_name: &entry.compression,
            maximum_bytes: request.maximum_bytes,
            maximum_records: request.maximum_records,
        };
        let probe = match &entry.source {
            FileDriverDiscoverySource::Local {
                path,
                selection_bytes_read,
            } => discover_local_binary_schema_bounded(
                path,
                &candidate.canonical_location,
                &self.dependencies,
                *selection_bytes_read,
                probe_request,
            )?,
            FileDriverDiscoverySource::Transport(resource) => {
                discover_transport_binary_schema_bounded(
                    resource.clone(),
                    &self.dependencies,
                    probe_request,
                )?
            }
        };
        SourceSchemaObservation::new(
            candidate,
            probe.schema.as_ref().clone(),
            probe.source_identity,
            probe.probe_bytes_read,
            probe.probe_records_read,
        )
    }
}

fn file_discovery_entries(
    source_plan: &CompiledSourcePlan,
    plan: &FileResourcePlan,
    compiled_format: &CompiledFormatBinding,
    dependencies: &FileRuntimeDependencies,
) -> Result<Vec<FileDriverDiscoveryEntry>> {
    if !uses_transport_inventory(&plan.root)? {
        return local_file_discovery_candidates(
            &source_plan.descriptor.resource_id,
            plan,
            dependencies.formats(),
            dependencies.transforms(),
        )?
        .into_iter()
        .map(|candidate| {
            let modified_at_ms = candidate.modified_at_ms();
            let mut identity =
                BTreeMap::from([("compression".to_owned(), candidate.compression.clone())]);
            identity.insert(
                "selection_bytes_read".to_owned(),
                candidate.selection_bytes_read.to_string(),
            );
            Ok(FileDriverDiscoveryEntry {
                candidate: SourceDiscoveryCandidate::new(
                    candidate.relative_path,
                    Some(candidate.size_bytes),
                    modified_at_ms,
                    identity,
                )?,
                compression: candidate.compression,
                source: FileDriverDiscoverySource::Local {
                    path: candidate.path,
                    selection_bytes_read: candidate.selection_bytes_read,
                },
            })
        })
        .collect();
    }

    let runtime = FileResource::new(
        FileResourceDefinition {
            descriptor: source_plan.descriptor.clone(),
            schema: Arc::new(source_plan.schema.clone()),
            plan: plan.clone(),
            type_policy_allowances: source_plan.type_policy_allowances,
            effective_schema_runtime: source_plan.effective_schema_runtime.clone(),
            compiled_format: compiled_format.clone(),
        },
        dependencies.clone(),
    )?;
    let partitions = runtime.plan_partitions(&ScanRequest {
        resource_id: source_plan.descriptor.resource_id.clone(),
        projection: None,
        filters: Vec::new(),
        limit: None,
        order_by: Vec::new(),
        scope: source_plan.descriptor.state_scope.clone(),
    })?;
    partitions
        .into_iter()
        .map(|partition| {
            let location = partition.metadata.get("path").cloned().ok_or_else(|| {
                CdfError::internal("file discovery partition omitted path metadata")
            })?;
            let size_bytes = partition
                .metadata
                .get("bytes")
                .ok_or_else(|| CdfError::internal("file discovery partition omitted bytes"))?
                .parse::<u64>()
                .map_err(|error| CdfError::data(format!("invalid file size: {error}")))?;
            let modified_at_ms = partition
                .metadata
                .get("modified_ms")
                .map(|value| {
                    value.parse::<i64>().map_err(|error| {
                        CdfError::data(format!("invalid file modification time: {error}"))
                    })
                })
                .transpose()?;
            let compression = partition
                .metadata
                .get("compression")
                .cloned()
                .unwrap_or_else(|| "none".to_owned());
            let resource = transport_resource_for_location(&location, plan)?;
            Ok(FileDriverDiscoveryEntry {
                candidate: SourceDiscoveryCandidate::new(
                    location,
                    Some(size_bytes),
                    modified_at_ms,
                    partition.metadata,
                )?,
                compression,
                source: FileDriverDiscoverySource::Transport(resource),
            })
        })
        .collect()
}

fn uses_transport_inventory(root: &str) -> Result<bool> {
    file_transport_scheme(root).map(|scheme| scheme.is_some())
}

fn transport_resource_for_location(
    location: &str,
    plan: &FileResourcePlan,
) -> Result<FileTransportResource> {
    let mut resource = match file_transport_scheme(location)? {
        Some(FileTransportScheme::Http | FileTransportScheme::Https) => FileTransportResource {
            location: FileTransportLocation::HttpUrl {
                url: location.to_owned(),
            },
            egress_allowlist: plan.allowlist.clone(),
            auth: plan.auth.clone(),
            credentials: plan.credentials.clone(),
        },
        Some(FileTransportScheme::File) => FileTransportResource::file_url(location),
        Some(FileTransportScheme::S3 | FileTransportScheme::Gs | FileTransportScheme::Az) => {
            FileTransportResource::object_store_url(location)
                .with_egress_allowlist(plan.allowlist.clone())
        }
        None => {
            return Err(CdfError::contract(format!(
                "file transport location {location:?} does not contain a supported URI scheme"
            )));
        }
    };
    if let Some(credentials) = &plan.credentials {
        resource = resource.with_credentials(credentials.clone());
    }
    Ok(resource)
}

fn option_schema() -> serde_json::Value {
    let auth = serde_json::json!({
        "oneOf": [
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["kind", "token"],
                "properties": {
                    "kind": {"const": "bearer"},
                    "token": {"type": "string", "pattern": "^secret://"}
                }
            },
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["kind", "name", "value"],
                "properties": {
                    "kind": {"const": "header"},
                    "name": {"type": "string", "minLength": 1},
                    "value": {"type": "string", "pattern": "^secret://"}
                }
            }
        ]
    });
    serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "source": {
            "type": "object",
            "additionalProperties": false,
            "required": ["root"],
            "properties": {
                "root": {"type": "string", "minLength": 1},
                "auth": auth,
                "credentials": {"type": "string", "pattern": "^secret://"},
                "egress_allowlist": {"type": "array", "items": {"type": "string"}, "uniqueItems": true}
            }
        },
        "resource": {
            "type": "object",
            "additionalProperties": false,
            "required": ["glob", "compression"],
            "properties": {
                "glob": {"type": "string", "minLength": 1},
                "format": {"type": "string", "minLength": 1},
                "format_options": {"type": "object"},
                "compression": {"type": "string", "minLength": 1}
            }
        }
    })
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileSourceOptions {
    root: String,
    #[serde(default)]
    auth: Option<AuthOptions>,
    #[serde(default)]
    credentials: Option<String>,
    #[serde(default)]
    egress_allowlist: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileResourceOptions {
    glob: String,
    #[serde(default)]
    format: Option<FileFormatDeclaration>,
    #[serde(default = "empty_format_options")]
    format_options: serde_json::Value,
    compression: FileCompressionDeclaration,
}

fn empty_format_options() -> serde_json::Value {
    serde_json::json!({})
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum AuthOptions {
    Bearer { token: String },
    Header { name: String, value: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FilePhysicalPlan {
    source_name: String,
    source: FileSourceOptions,
    resource: CompiledFileResourceOptions,
    compiled_format: CompiledFormatBinding,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CompiledFileResourceOptions {
    glob: String,
    format: FileFormatDeclaration,
    format_declared: bool,
    format_options: serde_json::Value,
    compression: FileCompressionDeclaration,
}

impl FilePhysicalPlan {
    fn validate(&self) -> Result<()> {
        self.resource.format.validate()?;
        self.resource.compression.validate()?;
        validate_portable_root(&self.source.root)
    }

    fn to_runtime_plan(&self, project_root: &std::path::Path) -> Result<FileResourcePlan> {
        self.validate()?;
        Ok(FileResourcePlan {
            source: self.source_name.clone(),
            root: resolve_runtime_root(&self.source.root, project_root)?,
            glob: self.resource.glob.clone(),
            format: Some(self.resource.format.clone()),
            format_declared: self.resource.format_declared,
            format_options: self.resource.format_options.clone(),
            compression: self.resource.compression.clone(),
            auth: self
                .source
                .auth
                .as_ref()
                .map(AuthOptions::to_runtime)
                .transpose()?,
            credentials: self
                .source
                .credentials
                .as_ref()
                .map(|value| SecretUri::new(value.clone()))
                .transpose()?,
            allowlist: if self.source.egress_allowlist.is_empty() {
                EgressAllowlist::allow_any()
            } else {
                EgressAllowlist::from_hosts(self.source.egress_allowlist.clone())
            },
        })
    }
}

fn validate_portable_root(root: &str) -> Result<()> {
    if root.trim().is_empty() {
        return Err(CdfError::contract("file source root cannot be empty"));
    }
    if file_transport_scheme(root)?.is_some() {
        return Ok(());
    }
    if std::path::Path::new(root).is_absolute() {
        return Ok(());
    }
    if std::path::Path::new(root)
        .components()
        .any(|component| component == std::path::Component::ParentDir)
    {
        return Err(CdfError::contract(
            "relative file source root must stay under the project root and cannot contain `..`",
        ));
    }
    Ok(())
}

fn resolve_runtime_root(root: &str, project_root: &std::path::Path) -> Result<String> {
    validate_portable_root(root)?;
    if let Some(scheme) = file_transport_scheme(root)? {
        let colon = root
            .find(':')
            .ok_or_else(|| CdfError::internal("parsed file transport scheme has no separator"))?;
        return Ok(format!("{}{}", scheme.as_str(), &root[colon..]));
    }
    if std::path::Path::new(root).is_absolute() {
        return Ok(root.to_owned());
    }
    let project_root = if project_root.is_absolute() {
        project_root.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| {
                CdfError::internal(format!("resolve current project directory: {error}"))
            })?
            .join(project_root)
    };
    project_root
        .join(root)
        .to_str()
        .map(str::to_owned)
        .ok_or_else(|| CdfError::data("file source root is not valid UTF-8"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FileTransportScheme {
    File,
    Http,
    Https,
    S3,
    Gs,
    Az,
}

impl FileTransportScheme {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Http => "http",
            Self::Https => "https",
            Self::S3 => "s3",
            Self::Gs => "gs",
            Self::Az => "az",
        }
    }
}

pub(crate) fn file_transport_scheme(value: &str) -> Result<Option<FileTransportScheme>> {
    let Some(colon) = value.find(':') else {
        return Ok(None);
    };
    let scheme = &value[..colon];
    let mut characters = scheme.chars();
    let Some(first) = characters.next() else {
        return Ok(None);
    };
    if !first.is_ascii_alphabetic()
        || !characters.all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '+' | '-' | '.')
        })
        || (colon == 1
            && value
                .as_bytes()
                .get(colon + 1)
                .is_some_and(|byte| matches!(byte, b'/' | b'\\')))
    {
        return Ok(None);
    }
    let scheme = match scheme.to_ascii_lowercase().as_str() {
        "file" => FileTransportScheme::File,
        "http" => FileTransportScheme::Http,
        "https" => FileTransportScheme::Https,
        "s3" => FileTransportScheme::S3,
        "gs" => FileTransportScheme::Gs,
        "az" => FileTransportScheme::Az,
        scheme => {
            return Err(CdfError::contract(format!(
                "unsupported file transport scheme {scheme:?}; supported schemes are file, http, https, s3, gs, and az"
            )));
        }
    };
    Ok(Some(scheme))
}

pub fn compile_file_resource_plan(
    plan: &FileResourcePlan,
    formats: &FormatRegistry,
) -> Result<(FileResourcePlan, CompiledFormatBinding)> {
    let (driver, format_declared) = match plan.format.as_ref() {
        Some(format) => (formats.resolve(format.as_str())?, plan.format_declared),
        None if plan.format_declared => {
            return Err(CdfError::internal(
                "file resource records an explicitly declared format without a format id",
            ));
        }
        None => (infer_format_from_glob(&plan.glob, formats)?, false),
    };
    let compiled_format = CompiledFormatBinding::compile(
        formats,
        driver.descriptor().format_id.as_str(),
        plan.format_options.clone(),
    )?;
    let mut resolved = plan.clone();
    resolved.format = Some(FileFormatDeclaration::named(
        compiled_format.descriptor.format_id.as_str().to_owned(),
    )?);
    resolved.format_declared = format_declared;
    Ok((resolved, compiled_format))
}

fn infer_format_from_glob(
    glob: &str,
    formats: &FormatRegistry,
) -> Result<Arc<dyn cdf_runtime::FormatDriver>> {
    let file_pattern = glob.rsplit('/').next().unwrap_or(glob);
    let extensions = file_pattern.split('.').skip(1).collect::<Vec<_>>();
    for extension in extensions.into_iter().rev() {
        let extension = extension.to_ascii_lowercase();
        if extension.is_empty()
            || extension
                .bytes()
                .any(|byte| !byte.is_ascii_alphanumeric() && byte != b'_' && byte != b'-')
        {
            continue;
        }
        if let Some(driver) = formats.by_extension(&extension) {
            return Ok(driver);
        }
    }
    let registered = formats
        .descriptors()
        .into_iter()
        .map(|descriptor| descriptor.format_id.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    Err(CdfError::contract(format!(
        "file glob `{glob}` does not contain an extension owned by an installed format driver; add `format = \"...\"` (registered: {registered})"
    )))
}

impl AuthOptions {
    fn to_runtime(&self) -> Result<AuthScheme> {
        match self {
            Self::Bearer { token } => Ok(AuthScheme::Bearer {
                token_uri: SecretUri::new(token.clone())?,
            }),
            Self::Header { name, value } => Ok(AuthScheme::Header {
                name: name.clone(),
                value_uri: SecretUri::new(value.clone())?,
            }),
        }
    }
}

fn decode_options<T: for<'de> Deserialize<'de>>(
    label: &str,
    options: BTreeMap<String, serde_json::Value>,
) -> Result<T> {
    serde_json::from_value(serde_json::Value::Object(options.into_iter().collect()))
        .map_err(|error| CdfError::contract(format!("{label} options are invalid: {error}")))
}

fn serialize_error(error: serde_json::Error) -> CdfError {
    CdfError::internal(format!("serialize file source plan: {error}"))
}

fn execution_capabilities() -> SourceExecutionCapabilities {
    SourceExecutionCapabilities {
        minimum_poll_bytes: 8 * 1024,
        maximum_poll_bytes: 32 * 1024 * 1024,
        minimum_decode_bytes: 8 * 1024,
        maximum_decode_bytes: 32 * 1024 * 1024,
        maximum_concurrency: 16,
        useful_concurrency: 16,
        executor_class: SourceExecutorClass::BlockingLane,
        blocking_lane: Some(file_source_blocking_lane()),
        pausable: true,
        spillable: true,
        idempotent_reads: true,
        reopenable: true,
        resumable: true,
        speculative_safe: true,
        retry_granularity: SourceRetryGranularity::Partition,
        retryable_errors: vec![
            cdf_kernel::ErrorKind::Transient,
            cdf_kernel::ErrorKind::RateLimited,
        ],
        retry_policy: Some(cdf_runtime::SourceRetryPolicy::default()),
        attestation: SourceAttestationStrength::ImmutableContent,
        rate_limit_per_second: None,
        quota_authority: None,
        canonical_order: true,
        bounded: true,
        batch_memory: cdf_runtime::SourceBatchMemoryContract::Preaccounted,
        telemetry_version: "v1".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FileTransportFacade;
    use arrow_schema::Schema;
    use cdf_http::{SecretProvider, SecretUri, SecretValue};
    use cdf_kernel::{
        ResourceDescriptor, ResourceId, SchemaHash, SchemaSource, ScopeKey, TrustLevel,
        WriteDisposition,
    };

    struct NoopSecretProvider;

    impl SecretProvider for NoopSecretProvider {
        fn resolve(&self, _uri: &SecretUri) -> Result<SecretValue> {
            Err(CdfError::auth(
                "file discovery test does not resolve secrets",
            ))
        }
    }

    fn compile_request() -> SourceCompileRequest {
        SourceCompileRequest {
            source_kind: "files".to_owned(),
            context: cdf_runtime::SourceCompileContext {
                source_name: "events".to_owned(),
                project_root: None,
                cursor_pushdown: None,
            },
            source_options: BTreeMap::from([
                (
                    "root".to_owned(),
                    serde_json::Value::String("/tmp/events".to_owned()),
                ),
                ("egress_allowlist".to_owned(), serde_json::json!([])),
            ]),
            resource_options: BTreeMap::from([
                (
                    "glob".to_owned(),
                    serde_json::Value::String("*.parquet.gz".to_owned()),
                ),
                ("format_options".to_owned(), serde_json::json!({})),
                (
                    "compression".to_owned(),
                    serde_json::Value::String("auto".to_owned()),
                ),
            ]),
            descriptor: ResourceDescriptor {
                resource_id: ResourceId::new("events.raw").unwrap(),
                schema_source: SchemaSource::Declared {
                    schema_hash: SchemaHash::new(format!("sha256:{}", "a".repeat(64))).unwrap(),
                    source: "test".to_owned(),
                },
                primary_key: Vec::new(),
                merge_key: Vec::new(),
                cursor: None,
                write_disposition: WriteDisposition::Append,
                deduplication: None,
                contract: None,
                state_scope: ScopeKey::Resource,
                freshness: None,
                trust_level: TrustLevel::Governed,
            },
            schema: Schema::empty(),
            type_policy_allowances: Default::default(),
            effective_schema_runtime: None,
            baseline_observation_schema_catalog: Vec::new(),
        }
    }

    #[test]
    fn compiled_file_plan_pins_complete_format_driver_semantics() {
        let formats = crate::test_format_registry();
        let driver = FileSourceDriver::new(Arc::clone(&formats), |_, _| {
            Err(CdfError::internal("compile-only test runtime factory"))
        })
        .unwrap();
        let plan = driver.compile(compile_request()).unwrap();
        let physical: FilePhysicalPlan =
            serde_json::from_value(plan.physical_plan.clone()).unwrap();

        assert_eq!(physical.source_name, "events");
        assert!(
            driver.option_schema()["source"]["properties"]
                .get("source_name")
                .is_none()
        );
        assert_eq!(
            physical.compiled_format.descriptor.format_id.as_str(),
            "parquet"
        );
        assert_eq!(
            physical.compiled_format.descriptor.semantic_version,
            "1.0.0"
        );
        assert_eq!(
            physical.compiled_format.descriptor.decode_unit_policy,
            "row_group"
        );
        assert_eq!(
            physical.compiled_format.descriptor.detection_probe,
            cdf_runtime::FormatDetectionProbe {
                prefix_bytes: 4,
                suffix_bytes: 4,
            }
        );
        assert_eq!(
            physical.compiled_format.canonical_options,
            serde_json::json!({})
        );
        physical.compiled_format.verify(formats.as_ref()).unwrap();

        let mut incompatible = physical.compiled_format;
        incompatible.descriptor.semantic_version = "2.0.0".to_owned();
        let error = match incompatible.verify(formats.as_ref()) {
            Ok(_) => panic!("incompatible compiled format plan must fail verification"),
            Err(error) => error,
        };
        assert!(
            error
                .message
                .contains("does not match the registered driver")
        );
    }

    #[test]
    fn compiled_file_capabilities_must_match_the_pinned_format() {
        let formats = crate::test_format_registry();
        let driver = FileSourceDriver::new(Arc::clone(&formats), |_, _| {
            Err(CdfError::internal("compile-only test runtime factory"))
        })
        .unwrap();
        let mut plan = driver.compile(compile_request()).unwrap();
        let physical: FilePhysicalPlan =
            serde_json::from_value(plan.physical_plan.clone()).unwrap();
        plan.resource_capabilities.projection = cdf_kernel::CapabilitySupport::Unsupported;

        let error = validate_compiled_capabilities(&plan, &physical.compiled_format).unwrap_err();
        assert!(error.message.contains("execution capabilities"));
    }

    #[test]
    fn portable_roots_normalize_supported_schemes_and_reject_unknown_schemes() {
        assert_eq!(
            file_transport_scheme("HTTPS://example.test/data").unwrap(),
            Some(FileTransportScheme::Https)
        );
        assert_eq!(
            resolve_runtime_root(
                "HTTPS://example.test/data",
                std::path::Path::new("/project")
            )
            .unwrap(),
            "https://example.test/data"
        );
        let error =
            resolve_runtime_root("custom+v1://cluster/data", std::path::Path::new("/project"))
                .unwrap_err();
        assert!(error.message.contains("unsupported file transport scheme"));
        assert_eq!(file_transport_scheme("C:\\data\\events").unwrap(), None);
    }

    #[test]
    fn compiled_relative_file_plan_is_portable_across_project_roots() {
        let formats = crate::test_format_registry();
        let driver = FileSourceDriver::new(Arc::clone(&formats), |_, _| {
            Err(CdfError::internal("compile-only test runtime factory"))
        })
        .unwrap();
        let mut first = compile_request();
        first.source_options.insert(
            "root".to_owned(),
            serde_json::Value::String("data".to_owned()),
        );
        first.context.project_root = Some("/tmp/first-project".into());
        let mut second = first.clone();
        second.context.project_root = Some("/tmp/second-project".into());

        let first = driver.compile(first).unwrap();
        let second = driver.compile(second).unwrap();
        assert_eq!(first.physical_plan, second.physical_plan);
        assert_eq!(first.physical_plan_hash, second.physical_plan_hash);
        assert_eq!(
            first.schema_binding_stable_hash().unwrap(),
            second.schema_binding_stable_hash().unwrap()
        );

        let physical: FilePhysicalPlan = serde_json::from_value(first.physical_plan).unwrap();
        assert_eq!(physical.source.root, "data");
        assert_eq!(
            physical
                .to_runtime_plan(std::path::Path::new("/tmp/first-project"))
                .unwrap()
                .root,
            "/tmp/first-project/data"
        );
        assert_eq!(
            physical
                .to_runtime_plan(std::path::Path::new("/tmp/second-project"))
                .unwrap()
                .root,
            "/tmp/second-project/data"
        );
    }

    #[test]
    fn registry_descriptors_own_undeclared_format_inference() {
        let formats = crate::test_format_registry();
        let driver = FileSourceDriver::new(Arc::clone(&formats), |_, _| {
            Err(CdfError::internal("compile-only test runtime factory"))
        })
        .unwrap();

        let mut arrow = compile_request();
        arrow.resource_options.insert(
            "glob".to_owned(),
            serde_json::Value::String("events.feather".to_owned()),
        );
        let arrow = driver.compile(arrow).unwrap();
        let physical: FilePhysicalPlan = serde_json::from_value(arrow.physical_plan).unwrap();
        assert_eq!(physical.resource.format.as_str(), "arrow_ipc");
        assert!(!physical.resource.format_declared);

        let mut explicit = compile_request();
        explicit.resource_options.insert(
            "format".to_owned(),
            serde_json::Value::String("json".to_owned()),
        );
        let explicit = driver.compile(explicit).unwrap();
        let physical: FilePhysicalPlan = serde_json::from_value(explicit.physical_plan).unwrap();
        assert_eq!(physical.resource.format.as_str(), "json");
        assert!(physical.resource.format_declared);

        let mut unknown = compile_request();
        unknown.resource_options.insert(
            "glob".to_owned(),
            serde_json::Value::String("events.unknown".to_owned()),
        );
        let error = driver.compile(unknown).unwrap_err();
        assert!(error.message.contains("installed format driver"));
        assert!(error.message.contains("format = \"...\""));
    }

    #[test]
    fn recompiling_an_inferred_format_preserves_its_provenance() {
        let formats = crate::test_format_registry();
        let unresolved = FileResourcePlan {
            source: "events".to_owned(),
            root: "/tmp".to_owned(),
            glob: "events.parquet".to_owned(),
            format: None,
            format_declared: false,
            format_options: serde_json::json!({}),
            compression: FileCompressionDeclaration::auto(),
            auth: None,
            credentials: None,
            allowlist: EgressAllowlist::allow_any(),
        };

        let (compiled, _) = compile_file_resource_plan(&unresolved, formats.as_ref()).unwrap();
        let (recompiled, _) = compile_file_resource_plan(&compiled, formats.as_ref()).unwrap();

        assert_eq!(recompiled.resolved_format().unwrap().as_str(), "parquet");
        assert!(!recompiled.format_declared);
    }

    #[test]
    fn driver_discovery_session_inventories_and_observes_local_ndjson() {
        let root = tempfile::tempdir().unwrap();
        let payload = b"{\"id\":1}\n{\"id\":2}\n";
        std::fs::write(root.path().join("events.ndjson"), payload).unwrap();
        let formats = crate::test_format_registry();
        let runtime_formats = Arc::clone(&formats);
        let transforms = crate::test_transform_registry();
        let driver = FileSourceDriver::new(Arc::clone(&formats), move |_, execution| {
            Ok(FileRuntimeDependencies::new(
                FileTransportFacade::new().with_execution_services(execution.clone()),
                execution,
                Arc::clone(&runtime_formats),
                Arc::clone(&transforms),
            ))
        })
        .unwrap();
        let mut request = compile_request();
        request.source_options.insert(
            "root".to_owned(),
            serde_json::json!(root.path().display().to_string()),
        );
        request
            .resource_options
            .insert("glob".to_owned(), serde_json::json!("events.ndjson"));
        request
            .resource_options
            .insert("format".to_owned(), serde_json::json!("ndjson"));
        request
            .resource_options
            .insert("compression".to_owned(), serde_json::json!("none"));
        let plan = driver.compile(request).unwrap();
        let execution = crate::test_execution_services();
        let context =
            SourceResolutionContext::new(root.path(), Arc::new(NoopSecretProvider), &execution);
        let session = driver.discovery_session(&plan, &context).unwrap();

        assert_eq!(session.kind(), SourceDiscoveryKind::BoundedContent);
        let candidates = session.candidates().unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].canonical_location, "events.ndjson");
        let observation = session
            .observe(
                &candidates[0],
                &SourceDiscoveryRequest::new(1024 * 1024, 10).unwrap(),
            )
            .unwrap();
        observation.validate().unwrap();
        assert_eq!(observation.bytes_read, payload.len() as u64);
        assert_eq!(observation.records_read, 2);
        assert_eq!(observation.schema.fields()[0].name(), "id");
    }
}
