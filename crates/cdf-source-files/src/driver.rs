use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

use cdf_http::{AuthScheme, EgressAllowlist, SecretProvider, SecretUri};
use cdf_kernel::{CdfError, CompiledScanIntent, PayloadRetention, QueryableResource, Result};
use cdf_memory::{ConsumerKey, MemoryClass, ReservationRequest};
use cdf_runtime::{
    CompiledFormatBinding, CompiledSourcePlan, ExecutionServices, FormatDiscoveryKind,
    FormatRegistry, PreparedSourcePayload, PreparedSourcePayloadKey, SourceAddPlanner,
    SourceAddProposal, SourceAddRequest, SourceAttestationStrength, SourceCompileRequest,
    SourceDiscoveryCandidate, SourceDiscoveryKind, SourceDiscoveryRequest, SourceDiscoverySession,
    SourceDriver, SourceDriverDescriptor, SourceDriverId, SourceEvidenceLocation,
    SourceExecutionCapabilities, SourceExecutorClass, SourceHealthRequest, SourceHealthResult,
    SourceHealthStatus, SourceResolutionContext, SourceRetryGranularity, SourceSchemaObservation,
    artifact_hash,
};
use serde::{Deserialize, Serialize};

use crate::{
    FILE_SOURCE_ADVERTISED_PARALLELISM, FileCompressionDeclaration, FileFormatDeclaration,
    FilePayloadCache, FileResource, FileResourceDefinition, FileResourcePlan,
    FileRuntimeDependencies, FileTransportControl, FileTransportLocation, FileTransportResource,
    SchemaDiscoveryRequest, discover_local_binary_schema, discover_transport_binary_schema,
    file_source_blocking_lane,
};

type RuntimeFactory = dyn Fn(
        Arc<dyn SecretProvider + Send + Sync>,
        ExecutionServices,
        cdf_runtime::SourceEgressScope,
    ) -> Result<FileRuntimeDependencies>
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
                cdf_runtime::SourceEgressScope,
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

    fn validate_project_options(&self, options: &serde_json::Value) -> Result<()> {
        decode_file_project_options(options).map(|_| ())
    }

    fn add_planner(&self) -> Option<&dyn SourceAddPlanner> {
        Some(self)
    }

    fn health(
        &self,
        request: SourceHealthRequest,
        context: &SourceResolutionContext<'_>,
        output: &mut dyn cdf_runtime::SourceHealthSink,
    ) -> Result<()> {
        if request.compiled_plans.is_empty() {
            return output.emit(SourceHealthResult {
                probe_id: "inventory".to_owned(),
                status: SourceHealthStatus::Skipped,
                message: "no file resources are compiled".to_owned(),
                details: serde_json::json!({"resources": 0}),
            });
        }
        for plan in &request.compiled_plans {
            request.budget.consume_work(1)?;
            let resource_id = plan.descriptor.resource_id.as_str();
            let maximum_entries =
                usize::try_from(request.budget.remaining_list_entries()?).unwrap_or(usize::MAX);
            let control = FileTransportControl::new(
                request.budget.cancellation(),
                Some(request.budget.deadline()),
            );
            let result = match self
                .discovery_session_with_limit(plan, context, maximum_entries, false, &control)
                .and_then(|session| session.candidates())
            {
                Ok(candidates) => {
                    if candidates.is_empty() {
                        SourceHealthResult::failed(
                            resource_id,
                            "file source inventory matched no candidates",
                            &plan.descriptor.resource_id,
                            &CdfError::data("configured file resource matched no files"),
                        )
                    } else {
                        request.budget.consume_list_entries(
                            u64::try_from(candidates.len()).unwrap_or(u64::MAX),
                        )?;
                        SourceHealthResult {
                            probe_id: resource_id.to_owned(),
                            status: SourceHealthStatus::Passed,
                            message: "file source inventory probe passed".to_owned(),
                            details: serde_json::json!({
                                "resource_id": resource_id,
                                "candidates": candidates.len(),
                            }),
                        }
                    }
                }
                Err(error) => {
                    request.budget.check()?;
                    SourceHealthResult::failed(
                        resource_id,
                        "file source inventory probe failed",
                        &plan.descriptor.resource_id,
                        &error,
                    )
                }
            };
            output.emit(result)?;
        }
        Ok(())
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
            schema_discovery: resource.schema_discovery,
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
        let schema_discovery = resource_plan.resolved_schema_discovery()?;
        let physical = FilePhysicalPlan {
            source_name,
            source,
            resource: CompiledFileResourceOptions {
                glob: resource_plan.glob,
                format: resolved_format,
                format_declared: resource_plan.format_declared,
                format_options: resource_plan.format_options,
                schema_discovery,
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
        let control = FileTransportControl::new(context.cancellation(), None);
        Ok(Box::new(self.discovery_session_with_limit(
            plan,
            context,
            usize::MAX,
            true,
            &control,
        )?))
    }

    fn resolve(
        &self,
        plan: &CompiledSourcePlan,
        context: &SourceResolutionContext<'_>,
    ) -> Result<Arc<dyn QueryableResource>> {
        let physical: FilePhysicalPlan = serde_json::from_value(plan.physical_plan.clone())
            .map_err(|error| CdfError::contract(format!("invalid file source plan: {error}")))?;
        validate_compiled_capabilities(plan, &physical.compiled_format)?;
        let dependencies = configure_runtime_dependencies(
            (self.runtime_factory)(
                Arc::clone(context.secret_provider()),
                context.execution().clone(),
                context.egress_scope(&plan.driver.driver_id),
            )?
            .with_prepared_payloads(context.prepared_payloads().clone()),
            context,
        )?;
        let prepared_inventory_key = prepared_file_inventory_key(plan)?;
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
            .with_transport_control(FileTransportControl::new(context.cancellation(), None))
            .with_prepared_inventory_key(prepared_inventory_key)
            .with_compiled_source_plan_hash(cdf_runtime::artifact_hash(plan)?),
        ))
    }
}

impl FileSourceDriver {
    fn discovery_session_with_limit(
        &self,
        plan: &CompiledSourcePlan,
        context: &SourceResolutionContext<'_>,
        maximum_entries: usize,
        retain_inventory: bool,
        control: &FileTransportControl,
    ) -> Result<FileDriverDiscoverySession> {
        plan.validate()?;
        let physical: FilePhysicalPlan = serde_json::from_value(plan.physical_plan.clone())
            .map_err(|error| CdfError::contract(format!("invalid file source plan: {error}")))?;
        validate_compiled_capabilities(plan, &physical.compiled_format)?;
        let dependencies = configure_runtime_dependencies(
            (self.runtime_factory)(
                Arc::clone(context.secret_provider()),
                context.execution().clone(),
                context.egress_scope(&plan.driver.driver_id),
            )?
            .with_prepared_payloads(context.prepared_payloads().clone()),
            context,
        )?;
        physical.compiled_format.verify(dependencies.formats())?;
        let runtime_plan = physical.to_runtime_plan(context.project_root())?;
        let discovery_kind = runtime_plan.resolved_schema_discovery()?;
        let entries = file_discovery_entries(
            plan,
            &runtime_plan,
            &physical.compiled_format,
            &dependencies,
            maximum_entries,
            retain_inventory,
            control,
        )?;
        Ok(FileDriverDiscoverySession {
            resource_id: plan.descriptor.resource_id.clone(),
            plan: runtime_plan,
            compiled_format: physical.compiled_format,
            dependencies,
            entries,
            discovery_kind,
        })
    }
}

impl SourceAddPlanner for FileSourceDriver {
    fn propose_add(&self, request: &SourceAddRequest) -> Result<Option<SourceAddProposal>> {
        request.validate()?;
        if !request.options.is_empty() {
            return Ok(None);
        }
        let target = if request.location.contains("://") {
            let parsed = url::Url::parse(&request.location).map_err(|error| {
                CdfError::contract(format!(
                    "cdf add could not parse file URL `[redacted-url]`: {error}"
                ))
            })?;
            if !self
                .descriptor
                .schemes
                .iter()
                .any(|scheme| scheme == parsed.scheme())
            {
                return Ok(None);
            }
            AddFileTarget::from_url(parsed)?
        } else {
            AddFileTarget::from_local(request)?
        };
        let mut source_options = BTreeMap::from([(
            "root".to_owned(),
            serde_json::Value::String(target.root.clone()),
        )]);
        if let Some(host) = target.egress_host {
            source_options.insert("egress_allowlist".to_owned(), serde_json::json!([host]));
        }
        Ok(Some(SourceAddProposal {
            source_kind: "files".to_owned(),
            source_options,
            resource_options: BTreeMap::from([
                (
                    "glob".to_owned(),
                    serde_json::Value::String(target.file_name.clone()),
                ),
                (
                    "format".to_owned(),
                    serde_json::Value::String(infer_add_format(&target.file_name)?.to_owned()),
                ),
            ]),
            cursor: None,
            display_location: SourceEvidenceLocation::from_operational(&target.root)?,
            display_selection: target.file_name,
            private_files: Vec::new(),
        }))
    }
}

struct AddFileTarget {
    root: String,
    file_name: String,
    egress_host: Option<String>,
}

impl AddFileTarget {
    fn from_url(parsed: url::Url) -> Result<Self> {
        let display = SourceEvidenceLocation::from_operational(parsed.as_str())?;
        match parsed.scheme() {
            "https" | "s3" | "gs" | "az" => {}
            "http" if is_loopback(&parsed) => {}
            "file" => {
                return Err(CdfError::contract(
                    "cdf add file:// URLs are not accepted; pass the local path directly",
                ));
            }
            scheme => {
                return Err(CdfError::contract(format!(
                    "cdf add does not accept `{scheme}` file URLs"
                )));
            }
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(CdfError::contract(format!(
                "cdf add does not accept URL userinfo credentials in `{}`; configure credentials through secret references",
                display.as_str()
            )));
        }
        if parsed.query().is_some() || parsed.fragment().is_some() {
            return Err(CdfError::contract(format!(
                "cdf add file URL `{}` must not contain query secrets or fragments; configure credentials through secret references",
                display.as_str()
            )));
        }
        let file_name = parsed
            .path_segments()
            .and_then(|mut segments| segments.next_back())
            .filter(|segment| !segment.is_empty())
            .ok_or_else(|| CdfError::contract("cdf add file URL must name a file"))?
            .to_owned();
        infer_add_format(&file_name)?;
        let egress_host = matches!(parsed.scheme(), "http" | "https")
            .then(|| parsed.host_str().map(ToOwned::to_owned))
            .flatten();
        let mut root = parsed;
        let mut segments = root
            .path_segments()
            .map(|segments| segments.collect::<Vec<_>>())
            .unwrap_or_default();
        segments.pop();
        let parent = if segments.is_empty() {
            "/".to_owned()
        } else {
            format!("/{}", segments.join("/"))
        };
        root.set_path(&parent);
        let root = root.as_str().trim_end_matches('/').to_owned();
        let root = if root.ends_with(":/") {
            format!("{root}/")
        } else {
            root
        };
        Ok(Self {
            root,
            file_name,
            egress_host,
        })
    }

    fn from_local(request: &SourceAddRequest) -> Result<Self> {
        let path = PathBuf::from(&request.location);
        let candidates = if path.is_absolute() {
            vec![path]
        } else {
            vec![
                request.current_dir.join(&path),
                request.project_root.join(&path),
            ]
        };
        let file = candidates
            .into_iter()
            .find(|candidate| candidate.is_file())
            .ok_or_else(|| {
                CdfError::contract(format!(
                    "cdf add could not find local file `{}`",
                    request.location
                ))
            })?;
        let file = fs::canonicalize(file)
            .map_err(|error| CdfError::contract(format!("canonicalize add source: {error}")))?;
        let file_name = file
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| CdfError::contract("cdf add local source requires a UTF-8 file name"))?
            .to_owned();
        infer_add_format(&file_name)?;
        let parent = file
            .parent()
            .ok_or_else(|| CdfError::contract("cdf add local source has no parent directory"))?;
        let project_root = fs::canonicalize(&request.project_root).map_err(|error| {
            CdfError::contract(format!("canonicalize cdf project root: {error}"))
        })?;
        let root = parent.strip_prefix(&project_root).map_or_else(
            |_| portable_path(parent),
            |relative| {
                if relative.as_os_str().is_empty() {
                    Ok(".".to_owned())
                } else {
                    portable_path(relative)
                }
            },
        )?;
        Ok(Self {
            root,
            file_name,
            egress_host: None,
        })
    }
}

fn portable_path(path: &Path) -> Result<String> {
    path.to_str()
        .map(|value| value.replace(std::path::MAIN_SEPARATOR, "/"))
        .ok_or_else(|| CdfError::contract("cdf add local source path must be valid UTF-8"))
}

fn infer_add_format(file_name: &str) -> Result<&'static str> {
    let lower = file_name.to_ascii_lowercase();
    let stem = [".gz", ".zst", ".zstd", ".bz2", ".xz", ".lz4", ".snappy"]
        .iter()
        .find_map(|suffix| lower.strip_suffix(suffix))
        .unwrap_or(&lower);
    if stem.ends_with(".parquet") || stem.ends_with(".pq") {
        Ok("parquet")
    } else if stem.ends_with(".ndjson") || stem.ends_with(".jsonl") {
        Ok("ndjson")
    } else if stem.ends_with(".json") {
        Ok("json")
    } else if stem.ends_with(".csv") {
        Ok("csv")
    } else if stem.ends_with(".tsv") || stem.ends_with(".tab") {
        Ok("tsv")
    } else if stem.ends_with(".psv") {
        Ok("psv")
    } else if stem.ends_with(".arrow") || stem.ends_with(".ipc") || stem.ends_with(".feather") {
        Ok("arrow_ipc")
    } else {
        Err(CdfError::contract(format!(
            "cdf add cannot infer a registered file format from `{file_name}`"
        )))
    }
}

fn is_loopback(url: &url::Url) -> bool {
    matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "::1"))
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
    discovery_kind: FormatDiscoveryKind,
}

impl SourceDiscoverySession for FileDriverDiscoverySession {
    fn kind(&self) -> SourceDiscoveryKind {
        match self.discovery_kind {
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
        let probe_request = SchemaDiscoveryRequest {
            resource_id: &self.resource_id,
            format,
            format_declared: self.plan.format_declared,
            format_options: &self.compiled_format.canonical_options,
            discovery_kind: self.discovery_kind,
            transform_name: &entry.compression,
            maximum_bytes: request.maximum_bytes,
            maximum_records: request.maximum_records,
            cancellation: request.cancellation.clone(),
        };
        let probe = match &entry.source {
            FileDriverDiscoverySource::Local {
                path,
                selection_bytes_read,
            } => discover_local_binary_schema(
                path,
                &candidate.canonical_location,
                &self.dependencies,
                *selection_bytes_read,
                probe_request,
            )?,
            FileDriverDiscoverySource::Transport(resource) => discover_transport_binary_schema(
                resource.clone(),
                &self.dependencies,
                probe_request,
            )?,
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
    maximum_entries: usize,
    retain_inventory: bool,
    control: &FileTransportControl,
) -> Result<Vec<FileDriverDiscoveryEntry>> {
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
    let partitions = runtime.partitions_for_intent_with_inventory_limit(
        &CompiledScanIntent::full_scan(),
        maximum_entries,
        control,
    )?;
    if retain_inventory {
        install_prepared_file_inventory(source_plan, dependencies, &partitions)?;
    }
    let local_root = match file_transport_scheme(&plan.root)? {
        None => Some(PathBuf::from(&plan.root)),
        Some(FileTransportScheme::File) => Some(crate::transport::file_url_path(&plan.root)?),
        Some(
            FileTransportScheme::Http | FileTransportScheme::Https | FileTransportScheme::Remote(_),
        ) => None,
    };
    let transport_inventory = local_root.is_none();
    partitions
        .iter()
        .map(|partition| {
            let file = partition
                .planned_file()?
                .ok_or_else(|| {
                    CdfError::internal("file discovery partition omitted typed file position")
                })?
                .clone();
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
            let source = if transport_inventory {
                FileDriverDiscoverySource::Transport(transport_resource_for_location(
                    &file.path, plan,
                )?)
            } else {
                FileDriverDiscoverySource::Local {
                    path: local_root
                        .as_ref()
                        .expect("local inventory has a local root")
                        .join(&file.path),
                    selection_bytes_read: 0,
                }
            };
            let mut identity = if transport_inventory {
                partition.metadata.clone()
            } else {
                BTreeMap::from([
                    ("compression".to_owned(), compression.clone()),
                    ("selection_bytes_read".to_owned(), "0".to_owned()),
                ])
            };
            if transport_inventory {
                if let Some(source_generation) = &file.source_generation {
                    identity.insert("source_generation".to_owned(), source_generation.clone());
                }
                if let Some(etag) = &file.etag {
                    identity.insert("etag".to_owned(), etag.clone());
                }
                if let Some(version) = &file.object_version {
                    identity.insert("version".to_owned(), version.clone());
                }
                if let Some(sha256) = &file.sha256 {
                    identity.insert("sha256".to_owned(), sha256.clone());
                }
            }
            Ok(FileDriverDiscoveryEntry {
                candidate: SourceDiscoveryCandidate::new(
                    file.path.clone(),
                    Some(file.size_bytes),
                    modified_at_ms,
                    identity,
                )?,
                compression,
                source,
            })
        })
        .collect()
}

fn prepared_file_inventory_key(plan: &CompiledSourcePlan) -> Result<PreparedSourcePayloadKey> {
    PreparedSourcePayloadKey::new(
        plan.descriptor.resource_id.clone(),
        plan.driver.driver_id.clone(),
        artifact_hash(&serde_json::json!({
            "kind": "file_partition_inventory_v1",
            "source_discovery_binding": plan.discovery_binding_hash()?,
        }))?,
    )
}

fn install_prepared_file_inventory(
    plan: &CompiledSourcePlan,
    dependencies: &FileRuntimeDependencies,
    partitions: &[cdf_kernel::PartitionPlan],
) -> Result<()> {
    let mut counter = CountingWriter::default();
    serde_json::to_writer(&mut counter, partitions)
        .map_err(|error| CdfError::internal(format!("size prepared file inventory: {error}")))?;
    let encoded_bytes = counter.bytes;
    if encoded_bytes == 0 {
        return Err(CdfError::internal(
            "prepared file inventory encoded to zero bytes",
        ));
    }
    let request = ReservationRequest::new(
        ConsumerKey::new("prepared-file-inventory", MemoryClass::Discovery)?,
        encoded_bytes,
    )?;
    let lease = dependencies
        .execution()
        .memory()
        .try_reserve(&request)?
        .ok_or_else(|| {
            CdfError::data(format!(
                "prepared file inventory requires {encoded_bytes} bytes but the discovery memory budget cannot admit it"
            ))
        })?;
    let capacity = usize::try_from(encoded_bytes)
        .map_err(|_| CdfError::data("prepared file inventory length exceeds usize"))?;
    let mut encoded = Vec::with_capacity(capacity);
    serde_json::to_writer(&mut encoded, partitions)
        .map_err(|error| CdfError::internal(format!("encode prepared file inventory: {error}")))?;
    let observed = u64::try_from(encoded.len())
        .map_err(|_| CdfError::data("prepared file inventory length exceeds u64"))?;
    if observed != encoded_bytes {
        return Err(CdfError::internal(
            "prepared file inventory sizing pass was not deterministic",
        ));
    }
    let retention = PayloadRetention::new(Arc::new(lease), encoded_bytes)?;
    dependencies.prepared_payloads().install(
        prepared_file_inventory_key(plan)?,
        PreparedSourcePayload::new(encoded, retention),
    )
}

#[derive(Default)]
struct CountingWriter {
    bytes: u64,
}

impl Write for CountingWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(u64::try_from(buffer.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| std::io::Error::other("prepared file inventory length overflow"))?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
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
        Some(FileTransportScheme::Remote(_)) => FileTransportResource::remote_url(location)
            .with_egress_allowlist(plan.allowlist.clone()),
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

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileProjectOptions {
    #[serde(default)]
    payload_cache: Option<FilePayloadCacheOptions>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FilePayloadCacheOptions {
    location: String,
    max_entries: usize,
    max_bytes: u64,
}

fn decode_file_project_options(options: &serde_json::Value) -> Result<FileProjectOptions> {
    let decoded: FileProjectOptions = serde_json::from_value(options.clone())
        .map_err(|error| CdfError::contract(format!("invalid file driver options: {error}")))?;
    if let Some(cache) = &decoded.payload_cache {
        if cache.location.trim().is_empty()
            || cache.location.chars().any(char::is_control)
            || cache.max_entries == 0
            || cache.max_bytes == 0
        {
            return Err(CdfError::contract(
                "file payload cache requires a safe location plus positive max_entries and max_bytes",
            ));
        }
        let path = Path::new(&cache.location);
        if path
            .components()
            .any(|component| component == std::path::Component::ParentDir)
        {
            return Err(CdfError::contract(
                "relative file payload cache location must stay under the project root",
            ));
        }
    }
    Ok(decoded)
}

fn configure_runtime_dependencies(
    dependencies: FileRuntimeDependencies,
    context: &SourceResolutionContext<'_>,
) -> Result<FileRuntimeDependencies> {
    let driver_id = SourceDriverId::new("files")?;
    let options = context
        .driver_options(&driver_id)
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let options = decode_file_project_options(&options)?;
    let Some(cache) = options.payload_cache else {
        return Ok(dependencies);
    };
    let configured = PathBuf::from(cache.location);
    let root =
        crate::payload_cache::resolve_project_cache_root(context.project_root(), &configured)?;
    Ok(dependencies.with_payload_cache(FilePayloadCache::new(
        root.join("v1"),
        cache.max_entries,
        cache.max_bytes,
    )?))
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
            "required": ["glob"],
            "properties": {
                "glob": {"type": "string", "minLength": 1},
                "format": {"type": "string", "minLength": 1},
                "format_options": {"type": "object"},
                "schema_discovery": {
                    "type": "string",
                    "enum": ["format_metadata", "bounded_content", "full_content"]
                },
                "compression": {"type": "string", "minLength": 1, "default": "auto"}
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
    #[serde(default)]
    schema_discovery: Option<FormatDiscoveryKind>,
    #[serde(default)]
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
    schema_discovery: FormatDiscoveryKind,
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
            schema_discovery: Some(self.resource.schema_discovery),
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
        let url = url::Url::parse(root)
            .map_err(|error| CdfError::contract(format!("invalid file source URL: {error}")))?;
        if !url.username().is_empty() || url.password().is_some() {
            return Err(CdfError::contract(
                "file source URL must not contain user information; use secret:// credentials",
            ));
        }
        if url.query().is_some() || url.fragment().is_some() {
            return Err(CdfError::contract(
                "file source URL must not contain query parameters or a fragment; use transport credentials and resource selection fields",
            ));
        }
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum FileTransportScheme {
    File,
    Http,
    Https,
    Remote(String),
}

impl FileTransportScheme {
    pub(crate) fn as_str(&self) -> &str {
        match self {
            Self::File => "file",
            Self::Http => "http",
            Self::Https => "https",
            Self::Remote(scheme) => scheme,
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
        scheme => FileTransportScheme::Remote(scheme.to_owned()),
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
    let schema_discovery = plan
        .schema_discovery
        .unwrap_or(compiled_format.descriptor.discovery.default_kind);
    if !compiled_format
        .descriptor
        .discovery
        .supports(schema_discovery)
    {
        return Err(CdfError::contract(format!(
            "file format `{}` does not support schema_discovery = `{}`; supported values: {}",
            compiled_format.descriptor.format_id,
            schema_discovery,
            compiled_format
                .descriptor
                .discovery
                .supported_kinds
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    let mut resolved = plan.clone();
    resolved.format = Some(FileFormatDeclaration::named(
        compiled_format.descriptor.format_id.as_str().to_owned(),
    )?);
    resolved.format_declared = format_declared;
    resolved.schema_discovery = Some(schema_discovery);
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
        maximum_concurrency: FILE_SOURCE_ADVERTISED_PARALLELISM,
        useful_concurrency: FILE_SOURCE_ADVERTISED_PARALLELISM,
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
        rate_limit: None,
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

    #[derive(Default)]
    struct TestHealthSink(Vec<SourceHealthResult>);

    #[test]
    fn execution_capabilities_share_advertised_parallelism_with_blocking_lane() {
        let capabilities = execution_capabilities();
        capabilities.validate().unwrap();
        assert_eq!(
            capabilities.maximum_concurrency,
            FILE_SOURCE_ADVERTISED_PARALLELISM
        );
        assert_eq!(
            capabilities.useful_concurrency,
            FILE_SOURCE_ADVERTISED_PARALLELISM
        );
        assert_eq!(
            capabilities
                .blocking_lane
                .as_ref()
                .unwrap()
                .maximum_concurrency,
            FILE_SOURCE_ADVERTISED_PARALLELISM
        );
    }

    impl cdf_runtime::SourceHealthSink for TestHealthSink {
        fn emit(&mut self, result: SourceHealthResult) -> Result<()> {
            self.0.push(result);
            Ok(())
        }
    }

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
        let driver = FileSourceDriver::new(Arc::clone(&formats), |_, _, _| {
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
            "1.1.0"
        );
        assert_eq!(
            physical.compiled_format.descriptor.predicate_pushdown,
            cdf_kernel::PushdownFidelity::Exact
        );
        assert_eq!(
            physical.compiled_format.descriptor.predicate_operators,
            ["=", "!=", ">", ">=", "<", "<="]
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
    fn payload_cache_requires_complete_explicit_bounded_project_policy() {
        let formats = crate::test_format_registry();
        let driver = FileSourceDriver::new(Arc::clone(&formats), |_, _, _| {
            Err(CdfError::internal("validation-only test runtime factory"))
        })
        .unwrap();

        driver
            .validate_project_options(&serde_json::json!({}))
            .unwrap();
        driver
            .validate_project_options(&serde_json::json!({
                "payload_cache": {
                    "location": ".cdf/cache/file-payloads",
                    "max_entries": 32,
                    "max_bytes": 1_073_741_824_u64,
                }
            }))
            .unwrap();

        for invalid in [
            serde_json::json!({"payload_cache": {"location": "cache", "max_entries": 1}}),
            serde_json::json!({
                "payload_cache": {"location": "cache", "max_entries": 0, "max_bytes": 1}
            }),
            serde_json::json!({
                "payload_cache": {"location": "../cache", "max_entries": 1, "max_bytes": 1}
            }),
            serde_json::json!({"payload_cache": {
                "location": "cache",
                "max_entries": 1,
                "max_bytes": 1,
                "unknown": true
            }}),
        ] {
            assert!(driver.validate_project_options(&invalid).is_err());
        }

        let project = tempfile::tempdir().unwrap();
        let execution = crate::test_execution_services();
        let context = SourceResolutionContext::new(
            project.path(),
            Arc::new(NoopSecretProvider),
            &execution,
            Arc::new(cdf_http::EgressAllowlist::allow_any()),
        )
        .with_driver_options(BTreeMap::from([(
            "files".to_owned(),
            serde_json::json!({
                "payload_cache": {
                    "location": ".cdf/cache/file-payloads",
                    "max_entries": 32,
                    "max_bytes": 1_073_741_824_u64,
                }
            }),
        )]));
        let dependencies = FileRuntimeDependencies::new(
            FileTransportFacade::new(),
            execution.clone(),
            crate::test_format_registry(),
            crate::test_transform_registry(),
            crate::test_egress_scope(),
        );
        let configured = configure_runtime_dependencies(dependencies, &context).unwrap();
        assert_eq!(
            configured.payload_cache().unwrap().root(),
            std::fs::canonicalize(project.path())
                .unwrap()
                .join(".cdf/cache/file-payloads/v1")
        );

        #[cfg(unix)]
        {
            let outside = tempfile::tempdir().unwrap();
            std::os::unix::fs::symlink(outside.path(), project.path().join("cache-escape"))
                .unwrap();
            let escape_context = SourceResolutionContext::new(
                project.path(),
                Arc::new(NoopSecretProvider),
                &execution,
                Arc::new(cdf_http::EgressAllowlist::allow_any()),
            )
            .with_driver_options(BTreeMap::from([(
                "files".to_owned(),
                serde_json::json!({
                    "payload_cache": {
                        "location": "cache-escape/payloads",
                        "max_entries": 1,
                        "max_bytes": 1,
                    }
                }),
            )]));
            let dependencies = FileRuntimeDependencies::new(
                FileTransportFacade::new(),
                execution.clone(),
                crate::test_format_registry(),
                crate::test_transform_registry(),
                crate::test_egress_scope(),
            );
            assert!(configure_runtime_dependencies(dependencies, &escape_context).is_err());
        }
    }

    #[test]
    fn compiled_file_capabilities_must_match_the_pinned_format() {
        let formats = crate::test_format_registry();
        let driver = FileSourceDriver::new(Arc::clone(&formats), |_, _, _| {
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
    fn portable_roots_normalize_builtin_schemes_and_preserve_external_schemes() {
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
        assert_eq!(
            resolve_runtime_root("CUSTOM+V1://cluster/data", std::path::Path::new("/project"))
                .unwrap(),
            "custom+v1://cluster/data"
        );
        assert!(
            resolve_runtime_root(
                "https://alice:secret@example.test/data",
                std::path::Path::new("/project")
            )
            .unwrap_err()
            .message
            .contains("must not contain user information")
        );
        assert!(
            resolve_runtime_root(
                "https://example.test/data?token=secret",
                std::path::Path::new("/project")
            )
            .unwrap_err()
            .message
            .contains("must not contain query parameters")
        );
        assert_eq!(file_transport_scheme("C:\\data\\events").unwrap(), None);
    }

    #[test]
    fn compiled_relative_file_plan_is_portable_across_project_roots() {
        let formats = crate::test_format_registry();
        let driver = FileSourceDriver::new(Arc::clone(&formats), |_, _, _| {
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
        let driver = FileSourceDriver::new(Arc::clone(&formats), |_, _, _| {
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
    fn compiled_format_capabilities_govern_schema_discovery_selection() {
        let formats = crate::test_format_registry();
        let driver = FileSourceDriver::new(Arc::clone(&formats), |_, _, _| {
            Err(CdfError::internal("compile-only test runtime factory"))
        })
        .unwrap();

        let default = driver.compile(compile_request()).unwrap();
        let physical: FilePhysicalPlan = serde_json::from_value(default.physical_plan).unwrap();
        assert_eq!(
            physical.resource.schema_discovery,
            FormatDiscoveryKind::FormatMetadata
        );

        let mut full = compile_request();
        full.resource_options
            .insert("glob".to_owned(), serde_json::json!("events.ndjson"));
        full.resource_options.insert(
            "schema_discovery".to_owned(),
            serde_json::json!("full_content"),
        );
        let full = driver.compile(full).unwrap();
        let physical: FilePhysicalPlan = serde_json::from_value(full.physical_plan).unwrap();
        assert_eq!(
            physical.resource.schema_discovery,
            FormatDiscoveryKind::FullContent
        );

        let mut unsupported = compile_request();
        unsupported.resource_options.insert(
            "schema_discovery".to_owned(),
            serde_json::json!("full_content"),
        );
        let error = driver.compile(unsupported).unwrap_err();
        assert!(error.message.contains("file format `parquet`"));
        assert!(error.message.contains("schema_discovery = `full_content`"));
        assert!(error.message.contains("supported values: format_metadata"));
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
            schema_discovery: None,
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
    fn add_infers_registered_delimited_format_ids_by_extension() {
        assert_eq!(infer_add_format("events.csv").unwrap(), "csv");
        assert_eq!(infer_add_format("events.tsv").unwrap(), "tsv");
        assert_eq!(infer_add_format("events.tab.gz").unwrap(), "tsv");
        assert_eq!(infer_add_format("events.psv.zst").unwrap(), "psv");
    }

    #[test]
    fn driver_discovery_session_inventories_and_observes_local_ndjson() {
        let root = tempfile::tempdir().unwrap();
        let payload = b"{\"id\":1}\n{\"id\":2}\n";
        std::fs::write(root.path().join("events.ndjson"), payload).unwrap();
        let formats = crate::test_format_registry();
        let runtime_formats = Arc::clone(&formats);
        let transforms = crate::test_transform_registry();
        let driver = FileSourceDriver::new(Arc::clone(&formats), move |_, execution, egress| {
            Ok(FileRuntimeDependencies::new(
                FileTransportFacade::new().with_execution_services(execution.clone()),
                execution,
                Arc::clone(&runtime_formats),
                Arc::clone(&transforms),
                egress,
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
            .insert("glob".to_owned(), serde_json::json!("*.ndjson"));
        request
            .resource_options
            .insert("format".to_owned(), serde_json::json!("ndjson"));
        request
            .resource_options
            .insert("compression".to_owned(), serde_json::json!("none"));
        let plan = driver.compile(request).unwrap();
        let execution = crate::test_execution_services();
        let context = SourceResolutionContext::new(
            root.path(),
            Arc::new(NoopSecretProvider),
            &execution,
            Arc::new(cdf_http::EgressAllowlist::allow_any()),
        );
        let mut health = TestHealthSink::default();
        driver
            .health(
                SourceHealthRequest {
                    compiled_plans: vec![plan.clone()],
                    budget: cdf_runtime::SourceHealthBudget::new(
                        cdf_runtime::SourceHealthLimits::default(),
                        execution.clone(),
                        cdf_runtime::RunCancellation::default(),
                    )
                    .unwrap(),
                },
                &context,
                &mut health,
            )
            .unwrap();
        assert_eq!(health.0.len(), 1);
        assert_eq!(health.0[0].status, SourceHealthStatus::Passed);
        assert_eq!(health.0[0].details["candidates"], 1);
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

        std::fs::write(root.path().join("later.ndjson"), payload).unwrap();
        let mut bounded_health = TestHealthSink::default();
        driver
            .health(
                SourceHealthRequest {
                    compiled_plans: vec![plan],
                    budget: cdf_runtime::SourceHealthBudget::new(
                        cdf_runtime::SourceHealthLimits {
                            maximum_list_entries: 1,
                            ..cdf_runtime::SourceHealthLimits::default()
                        },
                        execution.clone(),
                        cdf_runtime::RunCancellation::default(),
                    )
                    .unwrap(),
                },
                &context,
                &mut bounded_health,
            )
            .unwrap();
        assert_eq!(bounded_health.0.len(), 1);
        assert_eq!(bounded_health.0[0].status, SourceHealthStatus::Failed);
    }

    #[test]
    fn full_content_file_discovery_ignores_bounded_probe_limits_and_records_coverage() {
        let root = tempfile::tempdir().unwrap();
        let payload = b"{\"id\":1}\n{\"id\":2,\"late\":\"observed\"}\n";
        std::fs::write(root.path().join("events.ndjson"), payload).unwrap();
        let formats = crate::test_format_registry();
        let runtime_formats = Arc::clone(&formats);
        let transforms = crate::test_transform_registry();
        let driver = FileSourceDriver::new(Arc::clone(&formats), move |_, execution, egress| {
            Ok(FileRuntimeDependencies::new(
                FileTransportFacade::new().with_execution_services(execution.clone()),
                execution,
                Arc::clone(&runtime_formats),
                Arc::clone(&transforms),
                egress,
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
            .insert("glob".to_owned(), serde_json::json!("*.ndjson"));
        request
            .resource_options
            .insert("format".to_owned(), serde_json::json!("ndjson"));
        request
            .resource_options
            .insert("compression".to_owned(), serde_json::json!("none"));
        request.resource_options.insert(
            "schema_discovery".to_owned(),
            serde_json::json!("full_content"),
        );
        let plan = driver.compile(request).unwrap();
        let execution = crate::test_execution_services();
        let context = SourceResolutionContext::new(
            root.path(),
            Arc::new(NoopSecretProvider),
            &execution,
            Arc::new(cdf_http::EgressAllowlist::allow_any()),
        );
        let session = driver.discovery_session(&plan, &context).unwrap();
        assert_eq!(session.kind(), SourceDiscoveryKind::FullContent);
        let candidate = session.candidates().unwrap().remove(0);
        let observation = session
            .observe(&candidate, &SourceDiscoveryRequest::new(8, 1).unwrap())
            .unwrap();

        assert_eq!(observation.bytes_read, payload.len() as u64);
        assert_eq!(observation.records_read, 2);
        assert_eq!(observation.schema.field(1).name(), "late");
        assert_eq!(
            observation.source_identity["content_coverage"],
            "full_content"
        );
    }
}
