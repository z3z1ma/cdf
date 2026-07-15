use std::{
    collections::BTreeMap,
    fmt, fs,
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
    time::UNIX_EPOCH,
};

use arrow_schema::SchemaRef;
use cdf_kernel::{
    Batch, BatchStream, BoxFuture, CdfError, DeliveryGuarantee, EffectiveSchemaRuntime,
    OpenedPartitionStream, PLAN_PHYSICAL_SCHEMA_HASH_KEY, PLAN_SCHEMA_OBSERVATION_BINDING_KEY,
    PLAN_SCHEMA_OBSERVATION_ID_KEY, PartitionAttestation, PartitionId, PartitionPlan,
    PayloadRetention, PlanId, QueryableResource, ResourceCapabilities, ResourceDescriptor,
    ResourceId, ResourceStream, Result, ScanPlan, ScanRequest, SchemaHash, ScopeKey,
    SourcePosition, TypePolicyAllowances, WriteDisposition,
};
use cdf_memory::{ConsumerKey, MemoryClass};
use cdf_runtime::{
    AccountedByteStream, ByteExtent, ByteSource, ByteSourceCapabilities, ByteTransformId,
    ByteTransformRegistry, CanonicalStreamOpener, CompiledFormatBinding, ContentIdentity,
    DecodePlanningRequest, ExecutionServices, FormatDetection, FormatDetectionConfidence,
    FormatDiscoveryRequest, FormatDriver, FormatProbe, FormatRegistry, GenerationStrength,
    PhysicalDecodeRequest, PreparedSourcePayload, PreparedSourcePayloadKey, PreparedSourcePayloads,
    ReadOptions, SequentialReadRequest, SourceContentDigest, SourceDriverId, TransformSourceConfig,
    TransformedByteSource, canonical_stream_frontier, resolve_decode_unit_concurrency,
};
#[cfg(test)]
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

use crate::{
    FileCompressionDeclaration, FileFormatDeclaration, FileIdentityMetadata, FileResourcePlan,
    FileTransport, FileTransportFacade, FileTransportLocation, FileTransportResource,
    LocalByteSource, growing_spool_byte_source::start_growing_spool,
    local_byte_source::local_source_generation,
};

const NATIVE_TARGET_BATCH_ROWS: usize = 64 * 1024;
const NATIVE_TARGET_BATCH_BYTES: u64 = 16 * 1024 * 1024;
const NATIVE_STREAM_ITEMS: usize = 2;
const NATIVE_UNIT_STREAM_ITEMS: usize = 1;
const NATIVE_UNIT_BUFFERED_BATCHES: u16 = 2;

#[derive(Clone)]
pub struct FileRuntimeDependencies {
    transport: Arc<dyn FileTransport>,
    execution: ExecutionServices,
    formats: Arc<FormatRegistry>,
    transforms: Arc<ByteTransformRegistry>,
    prepared_payloads: PreparedSourcePayloads,
    max_spool_bytes: u64,
}

const DEFAULT_MAX_FILE_SPOOL_BYTES: u64 = 64 * 1024 * 1024 * 1024;

impl FileRuntimeDependencies {
    pub fn new(
        transport: impl FileTransport + 'static,
        execution: ExecutionServices,
        formats: Arc<FormatRegistry>,
        transforms: Arc<ByteTransformRegistry>,
    ) -> Self {
        Self::from_boxed_transport(Box::new(transport), execution, formats, transforms)
    }

    pub fn from_boxed_transport(
        transport: Box<dyn FileTransport>,
        execution: ExecutionServices,
        formats: Arc<FormatRegistry>,
        transforms: Arc<ByteTransformRegistry>,
    ) -> Self {
        Self {
            transport: Arc::from(transport),
            execution,
            formats,
            transforms,
            prepared_payloads: PreparedSourcePayloads::default(),
            max_spool_bytes: DEFAULT_MAX_FILE_SPOOL_BYTES,
        }
    }

    pub fn with_prepared_payloads(mut self, prepared_payloads: PreparedSourcePayloads) -> Self {
        self.prepared_payloads = prepared_payloads;
        self
    }

    pub fn with_max_spool_bytes(mut self, max_spool_bytes: u64) -> Result<Self> {
        if max_spool_bytes == 0 {
            return Err(CdfError::contract(
                "file spool budget must be greater than zero",
            ));
        }
        self.max_spool_bytes = max_spool_bytes;
        Ok(self)
    }

    pub fn max_spool_bytes(&self) -> u64 {
        self.max_spool_bytes
    }

    fn execution(&self) -> &ExecutionServices {
        &self.execution
    }

    pub fn formats(&self) -> &Arc<FormatRegistry> {
        &self.formats
    }

    pub fn transforms(&self) -> &Arc<ByteTransformRegistry> {
        &self.transforms
    }

    pub fn prepared_payloads(&self) -> &PreparedSourcePayloads {
        &self.prepared_payloads
    }

    #[cfg(test)]
    fn transport(&self) -> Arc<dyn FileTransport> {
        Arc::clone(&self.transport)
    }

    pub fn with_transport<R>(&self, f: impl FnOnce(&dyn FileTransport) -> Result<R>) -> Result<R> {
        f(self.transport.as_ref())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalFileDiscoveryCandidate {
    pub path: PathBuf,
    pub relative_path: String,
    pub size_bytes: u64,
    pub compression: String,
    pub selection_bytes_read: u64,
}

#[derive(Clone, Debug)]
pub struct BoundedBinarySchemaProbe {
    pub schema: SchemaRef,
    pub source_identity: BTreeMap<String, String>,
    pub probe_bytes_read: u64,
    pub probe_records_read: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct BoundedSchemaDiscoveryRequest<'a> {
    pub resource_id: &'a ResourceId,
    pub format: &'a FileFormatDeclaration,
    pub format_declared: bool,
    pub format_options: &'a serde_json::Value,
    pub transform_name: &'a str,
    pub maximum_bytes: u64,
    pub maximum_records: u64,
}

pub fn discover_local_binary_schema_bounded(
    path: impl AsRef<Path>,
    location: &str,
    dependencies: &FileRuntimeDependencies,
    initial_bytes_read: u64,
    request: BoundedSchemaDiscoveryRequest<'_>,
) -> Result<BoundedBinarySchemaProbe> {
    let path = path.as_ref().to_path_buf();
    let source_size = fs::metadata(&path)
        .map_err(|error| CdfError::data(format!("stat {} for discovery: {error}", path.display())))?
        .len();
    let driver = dependencies.formats().resolve(request.format.as_str())?;
    let upstream: Arc<dyn ByteSource> = Arc::new(LocalByteSource::open(
        &path,
        dependencies.execution().memory(),
    )?);
    let upstream_identity = upstream.identity().clone();
    let transform_id = (request.transform_name != "none")
        .then(|| {
            dependencies
                .transforms()
                .resolve_name(request.transform_name)
        })
        .transpose()?
        .map(|driver| driver.descriptor().transform_id.clone());
    let needs_spool = transform_id.is_some()
        && driver.descriptor().source_access != cdf_runtime::FormatSourceAccess::Sequential;
    let retain_sequential = retains_sequential_discovery_payload(driver.descriptor());
    let extraction_content_hash =
        (needs_spool || retain_sequential).then(SourceContentDigest::default);
    let upstream = match extraction_content_hash.as_ref() {
        Some(observation) => {
            Arc::new(HashingByteSource::new(upstream, observation.clone())) as Arc<dyn ByteSource>
        }
        None => upstream,
    };
    let source = match transform_id.as_ref() {
        Some(transform_id) => transformed_byte_source(upstream, transform_id, dependencies)?,
        None => upstream,
    };
    let logical_source_identity = source.identity().clone();
    let options = driver.canonical_options(request.format_options.clone())?;
    let prepared_payload_key = prepared_file_payload_key(
        PreparedFilePayloadKeyInput {
            resource_id: request.resource_id,
            location,
            size_bytes: source_size,
            source_generation: upstream_identity.generation.as_deref(),
            etag: None,
            object_version: None,
            sha256: upstream_identity.checksum.as_deref(),
            driver: driver.as_ref(),
            canonical_format_options: &options,
            transform_name: request.transform_name,
        },
        dependencies,
    )?;
    let discovery_memory = dependencies.execution().memory();
    let confirmation = FormatConfirmationContext {
        resource_id: request.resource_id.clone(),
        location: location.to_owned(),
        format_declared: request.format_declared,
        transform_name: request.transform_name.to_owned(),
    };
    let maximum_bytes = request.maximum_bytes;
    let maximum_records = request.maximum_records;
    let observation = dependencies.execution().run_io({
        let dependencies = dependencies.clone();
        let driver = Arc::clone(&driver);
        let source = Arc::clone(&source);
        let extraction_content_hash = extraction_content_hash.clone();
        async move {
            let mut spool = None;
            let mut sequential_capture = None;
            let source = if needs_spool {
                let accounted = Arc::new(
                    spool_byte_source_async(
                        source,
                        None,
                        &dependencies,
                        cdf_runtime::RunCancellation::default(),
                    )
                    .await?,
                );
                let local: Arc<dyn ByteSource> = Arc::new(LocalByteSource::open(
                    accounted.path(),
                    dependencies.execution().memory(),
                )?);
                spool = Some(accounted);
                local
            } else if retain_sequential {
                let capture = SequentialPayloadCapture::new(source, &dependencies).await?;
                let source = capture.discovery_source();
                sequential_capture = Some(capture);
                source
            } else {
                source
            };
            let logical_size = match spool.as_ref() {
                Some(spool) => fs::metadata(spool.path())
                    .map_err(|error| {
                        CdfError::data(format!(
                            "stat transformed discovery spool for {}: {error}",
                            confirmation.location
                        ))
                    })?
                    .len(),
                None => source_size,
            };
            let confirmation_bytes = confirm_registered_format(
                source.as_ref(),
                logical_size,
                &driver,
                dependencies.formats(),
                &confirmation,
            )
            .await?;
            let discovery_bytes = discovery_budget_after_confirmation(
                maximum_bytes,
                confirmation_bytes,
                &confirmation,
            )?;
            let observation = driver
                .discover(
                    Arc::clone(&source),
                    FormatDiscoveryRequest {
                        options,
                        maximum_bytes: discovery_bytes,
                        maximum_records,
                        memory: discovery_memory,
                        cancellation: cdf_runtime::RunCancellation::default(),
                    },
                )
                .await?;
            let probe_bytes_read = if spool.is_some() {
                source_size
            } else {
                observation.sampled_bytes.saturating_add(confirmation_bytes)
            };
            if let Some(capture) = sequential_capture {
                let payload = capture.finish(extraction_content_hash.clone()).await?;
                dependencies
                    .prepared_payloads()
                    .install(prepared_payload_key, payload)?;
            } else if let Some(spool) = spool {
                let retention = retain_spool(&spool, logical_size)?;
                dependencies.prepared_payloads().install(
                    prepared_payload_key,
                    prepared_file_payload(source, retention, extraction_content_hash.clone())?,
                )?;
            }
            Ok::<_, CdfError>((observation, probe_bytes_read))
        }
    })?;
    let (observation, probe_bytes_read) = observation;
    let probe_records_read = observation.sampled_records;
    let schema = observation.arrow_schema;
    let schema_hash = cdf_kernel::canonical_arrow_schema_hash(schema.as_ref())?;
    let mut source_identity = BTreeMap::from([
        ("stable_id".to_owned(), logical_source_identity.stable_id),
        ("format".to_owned(), request.format.as_str().to_owned()),
        (
            "format_driver_version".to_owned(),
            driver.descriptor().semantic_version.clone(),
        ),
        ("schema_hash".to_owned(), schema_hash.to_string()),
        ("size_bytes".to_owned(), source_size.to_string()),
    ]);
    merge_discovery_evidence(&mut source_identity, observation.evidence)?;
    if let Some(generation) = observation.identity.generation {
        source_identity.insert("generation".to_owned(), generation);
    }
    if let Some(checksum) = observation.identity.checksum {
        source_identity.insert("checksum".to_owned(), checksum);
    }
    source_identity.insert("path".to_owned(), path.to_string_lossy().into_owned());
    source_identity.insert("compression".to_owned(), request.transform_name.to_owned());
    source_identity.insert("source_size_bytes".to_owned(), source_size.to_string());
    Ok(BoundedBinarySchemaProbe {
        schema,
        source_identity,
        probe_bytes_read: initial_bytes_read.saturating_add(probe_bytes_read),
        probe_records_read,
    })
}

pub fn discover_transport_binary_schema_bounded(
    resource: FileTransportResource,
    dependencies: &FileRuntimeDependencies,
    request: BoundedSchemaDiscoveryRequest<'_>,
) -> Result<BoundedBinarySchemaProbe> {
    let observation = dependencies.with_transport(|transport| transport.metadata(&resource))?;
    let access_resource = observation.access_resource(&resource);
    let metadata = observation.into_identity();
    let size_bytes = metadata.size_bytes.ok_or_else(|| {
        CdfError::data(format!(
            "remote binary discovery for `{}` did not receive byte-size metadata",
            diagnostic_location(&metadata.location)
        ))
    })?;
    let driver = dependencies.formats().resolve(request.format.as_str())?;
    let upstream = dependencies.with_transport(|transport| {
        transport.open_byte_source(
            &access_resource,
            &metadata,
            dependencies.execution().memory(),
        )
    })?;
    let transform_id = (request.transform_name != "none")
        .then(|| {
            dependencies
                .transforms()
                .resolve_name(request.transform_name)
        })
        .transpose()?
        .map(|driver| driver.descriptor().transform_id.clone());
    let needs_spool = driver.descriptor().source_access
        != cdf_runtime::FormatSourceAccess::Sequential
        && (!upstream.capabilities().seekable || transform_id.is_some());
    let retain_sequential = retains_sequential_discovery_payload(driver.descriptor());
    let extraction_content_hash = ((needs_spool || retain_sequential)
        && metadata.generation_strength() == GenerationStrength::Weak)
        .then(SourceContentDigest::default);
    let upstream = match extraction_content_hash.as_ref() {
        Some(observation) => {
            Arc::new(HashingByteSource::new(upstream, observation.clone())) as Arc<dyn ByteSource>
        }
        None => upstream,
    };
    let source = match transform_id.as_ref() {
        Some(transform_id) => transformed_byte_source(upstream, transform_id, dependencies)?,
        None => upstream,
    };
    let logical_source_identity = source.identity().clone();
    let execution = dependencies.execution().clone();
    let memory = execution.memory();
    let options = driver.canonical_options(request.format_options.clone())?;
    let source_generation = (metadata.generation_strength() == GenerationStrength::Weak)
        .then_some(metadata.modified.as_deref())
        .flatten();
    let prepared_payload_key = prepared_file_payload_key(
        PreparedFilePayloadKeyInput {
            resource_id: request.resource_id,
            location: &metadata.location,
            size_bytes,
            source_generation,
            etag: metadata.etag.as_deref(),
            object_version: metadata.version.as_deref(),
            sha256: metadata.sha256(),
            driver: driver.as_ref(),
            canonical_format_options: &options,
            transform_name: request.transform_name,
        },
        dependencies,
    )?;
    let confirmation = FormatConfirmationContext {
        resource_id: request.resource_id.clone(),
        location: diagnostic_location(&metadata.location),
        format_declared: request.format_declared,
        transform_name: request.transform_name.to_owned(),
    };
    let maximum_bytes = request.maximum_bytes;
    let maximum_records = request.maximum_records;
    let observation = execution.run_io({
        let dependencies = dependencies.clone();
        let driver = Arc::clone(&driver);
        let extraction_content_hash = extraction_content_hash.clone();
        async move {
            let mut spool = None;
            let mut sequential_capture = None;
            let source = if needs_spool {
                let accounted = Arc::new(
                    spool_byte_source_async(
                        source,
                        None,
                        &dependencies,
                        cdf_runtime::RunCancellation::default(),
                    )
                    .await?,
                );
                let local: Arc<dyn ByteSource> = Arc::new(LocalByteSource::open(
                    accounted.path(),
                    dependencies.execution().memory(),
                )?);
                spool = Some(accounted);
                local
            } else if retain_sequential {
                let capture = SequentialPayloadCapture::new(source, &dependencies).await?;
                let source = capture.discovery_source();
                sequential_capture = Some(capture);
                source
            } else {
                source
            };
            let logical_size = match spool.as_ref() {
                Some(spool) => fs::metadata(spool.path())
                    .map_err(|error| {
                        CdfError::data(format!(
                            "stat transformed discovery spool for {}: {error}",
                            confirmation.location
                        ))
                    })?
                    .len(),
                None => size_bytes,
            };
            let confirmation_bytes = confirm_registered_format(
                source.as_ref(),
                logical_size,
                &driver,
                dependencies.formats(),
                &confirmation,
            )
            .await?;
            let discovery_bytes = discovery_budget_after_confirmation(
                maximum_bytes,
                confirmation_bytes,
                &confirmation,
            )?;
            let observation = driver
                .discover(
                    Arc::clone(&source),
                    FormatDiscoveryRequest {
                        options,
                        maximum_bytes: discovery_bytes,
                        maximum_records,
                        memory,
                        cancellation: cdf_runtime::RunCancellation::default(),
                    },
                )
                .await?;
            let probe_bytes_read = if spool.is_some() {
                size_bytes
            } else {
                observation.sampled_bytes.saturating_add(confirmation_bytes)
            };
            if let Some(capture) = sequential_capture {
                let payload = capture.finish(extraction_content_hash.clone()).await?;
                dependencies
                    .prepared_payloads()
                    .install(prepared_payload_key, payload)?;
            } else if let Some(spool) = spool {
                let retention = retain_spool(&spool, logical_size)?;
                dependencies.prepared_payloads().install(
                    prepared_payload_key,
                    prepared_file_payload(source, retention, extraction_content_hash.clone())?,
                )?;
            }
            Ok::<_, CdfError>((observation, probe_bytes_read))
        }
    })?;
    let (observation, probe_bytes_read) = observation;
    let probe_records_read = observation.sampled_records;
    let schema_hash = cdf_kernel::canonical_arrow_schema_hash(observation.arrow_schema.as_ref())?;
    let mut source_identity = BTreeMap::from([
        ("stable_id".to_owned(), logical_source_identity.stable_id),
        ("format".to_owned(), request.format.as_str().to_owned()),
        (
            "format_driver_version".to_owned(),
            driver.descriptor().semantic_version.clone(),
        ),
        ("schema_hash".to_owned(), schema_hash.to_string()),
        ("compression".to_owned(), request.transform_name.to_owned()),
        ("source_size_bytes".to_owned(), size_bytes.to_string()),
        ("size_bytes".to_owned(), size_bytes.to_string()),
    ]);
    merge_discovery_evidence(&mut source_identity, observation.evidence)?;
    let mut probe = BoundedBinarySchemaProbe {
        schema: observation.arrow_schema,
        source_identity,
        probe_bytes_read,
        probe_records_read,
    };
    probe
        .source_identity
        .insert("url".to_owned(), metadata.location.clone());
    if let Some(etag) = &metadata.etag {
        probe
            .source_identity
            .insert("etag".to_owned(), etag.clone());
    }
    if let Some(version) = &metadata.version {
        probe
            .source_identity
            .insert("version".to_owned(), version.clone());
    }
    if let Some(sha256) = metadata.sha256() {
        probe
            .source_identity
            .insert("sha256".to_owned(), sha256.to_owned());
    }
    Ok(probe)
}

struct FormatConfirmationContext {
    resource_id: ResourceId,
    location: String,
    format_declared: bool,
    transform_name: String,
}

fn discovery_budget_after_confirmation(
    maximum_bytes: u64,
    confirmation_bytes: u64,
    context: &FormatConfirmationContext,
) -> Result<u64> {
    let remaining = maximum_bytes.checked_sub(confirmation_bytes).ok_or_else(|| {
        CdfError::data(format!(
            "format confirmation for resource `{}`, file `{}` requires {confirmation_bytes} bytes, exceeding the configured {maximum_bytes}-byte discovery budget",
            context.resource_id, context.location
        ))
    })?;
    if remaining == 0 {
        return Err(CdfError::data(format!(
            "format confirmation for resource `{}`, file `{}` consumes the configured {maximum_bytes}-byte discovery budget; increase the discovery byte budget to leave room for schema observation",
            context.resource_id, context.location
        )));
    }
    Ok(remaining)
}

async fn confirm_registered_format(
    source: &dyn ByteSource,
    source_size: u64,
    driver: &Arc<dyn FormatDriver>,
    formats: &FormatRegistry,
    context: &FormatConfirmationContext,
) -> Result<u64> {
    if driver.descriptor().magic.is_empty() || source_size == 0 {
        return Ok(0);
    }
    if !source.capabilities().exact_ranges {
        return Err(CdfError::contract(format!(
            "format `{}` requires bounded magic confirmation but byte source `{}` does not support exact ranges; spool the admitted stream before format discovery",
            driver.descriptor().format_id,
            source.identity().stable_id
        )));
    }
    let descriptors = formats.descriptors();
    let prefix_length = descriptors
        .iter()
        .map(|descriptor| u64::from(descriptor.detection_probe.prefix_bytes))
        .max()
        .unwrap_or(0)
        .min(source_size);
    let suffix_length = descriptors
        .iter()
        .map(|descriptor| u64::from(descriptor.detection_probe.suffix_bytes))
        .max()
        .unwrap_or(0)
        .min(source_size);
    let cancellation = cdf_runtime::RunCancellation::default();
    let prefix = if prefix_length == 0 {
        None
    } else {
        Some(
            source
                .read_exact_range(ByteExtent::new(0, prefix_length)?, cancellation.clone())
                .await?,
        )
    };
    let suffix = if suffix_length == 0 {
        None
    } else {
        Some(
            source
                .read_exact_range(
                    ByteExtent::new(source_size - suffix_length, suffix_length)?,
                    cancellation,
                )
                .await?,
        )
    };
    let extension = discovery_format_extension(&context.location, &context.transform_name);
    let probe = FormatProbe {
        extension: extension.clone(),
        mime_type: None,
        prefix: prefix
            .as_ref()
            .map(|bytes| bytes.payload().to_vec())
            .unwrap_or_default(),
        suffix: suffix
            .as_ref()
            .map(|bytes| bytes.payload().to_vec())
            .unwrap_or_default(),
    };
    let selected_detection = driver.detect(&probe)?;
    let strong_magic = formats.detect_strong_magic(&probe.prefix)?;
    let strong_magic_id = strong_magic
        .as_ref()
        .map(|detected| detected.descriptor().format_id.as_str());
    let selected_id = driver.descriptor().format_id.as_str();
    if strong_magic_id.is_some_and(|detected| detected != selected_id)
        || selected_detection.confidence == FormatDetectionConfidence::None
    {
        let declared = if context.format_declared {
            selected_id
        } else {
            "<omitted>"
        };
        let magic = strong_magic_id.unwrap_or("none");
        return Err(CdfError::data(format!(
            "file format confirmation failed for resource `{}`, file `{}`: declared format `{declared}`, inferred format `{selected_id}`, extension signal `{}`, magic bytes signal `{magic}`; use `format = \"{selected_id}\"` only when the bytes match, or correct the file/extension",
            context.resource_id,
            context.location,
            extension.as_deref().unwrap_or("none")
        )));
    }
    Ok(prefix_length.saturating_add(suffix_length))
}

fn discovery_format_extension(location: &str, transform_name: &str) -> Option<String> {
    let location = location
        .split('#')
        .next()
        .unwrap_or(location)
        .split('?')
        .next()
        .unwrap_or(location);
    let mut pieces = location.rsplit('.');
    let outer = pieces.next()?;
    if transform_name == "none" {
        return Some(outer.to_ascii_lowercase());
    }
    pieces.next().map(str::to_ascii_lowercase)
}

fn merge_discovery_evidence(
    source_identity: &mut BTreeMap<String, String>,
    evidence: BTreeMap<String, String>,
) -> Result<()> {
    for (key, value) in evidence {
        if source_identity.contains_key(&key) {
            return Err(CdfError::contract(format!(
                "format discovery evidence key `{key}` conflicts with source identity authority"
            )));
        }
        source_identity.insert(key, value);
    }
    Ok(())
}

impl LocalFileDiscoveryCandidate {
    pub fn modified_at_ms(&self) -> Option<i64> {
        fs::metadata(&self.path)
            .ok()?
            .modified()
            .ok()?
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|duration| i64::try_from(duration.as_millis()).ok())
    }
}

pub fn local_file_discovery_candidates(
    resource_id: &ResourceId,
    plan: &FileResourcePlan,
    formats: &FormatRegistry,
    transforms: &ByteTransformRegistry,
) -> Result<Vec<LocalFileDiscoveryCandidate>> {
    if is_http_root(&plan.root) {
        return Err(CdfError::contract(
            "local file discovery candidate enumeration does not support HTTP(S) roots",
        ));
    }

    let root = PathBuf::from(&plan.root);
    if !root.is_absolute() {
        return Err(CdfError::contract(format!(
            "file source root `{}` must be absolute before discovery; compile with an explicit project root or declare an absolute file source root",
            plan.root
        )));
    }
    let components = pattern_components(&plan.glob)?;
    let mut matches = Vec::new();
    collect_matches(&root, &components, &mut matches)?;
    matches.sort();
    matches.dedup();

    contained_matches(&root, matches)?
        .into_iter()
        .map(|path| {
            local_file_discovery_candidate(resource_id, &root, path, plan, formats, transforms)
        })
        .collect()
}

impl fmt::Debug for FileRuntimeDependencies {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FileRuntimeDependencies")
            .field("transport", &"<explicit>")
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct FileResource {
    descriptor: ResourceDescriptor,
    schema: SchemaRef,
    capabilities: ResourceCapabilities,
    plan: FileResourcePlan,
    type_policy_allowances: TypePolicyAllowances,
    effective_schema_runtime: Option<Arc<EffectiveSchemaRuntime>>,
    compiled_format: CompiledFormatBinding,
    dependencies: FileRuntimeDependencies,
}

#[derive(Clone, Debug)]
pub struct FileResourceDefinition {
    pub descriptor: ResourceDescriptor,
    pub schema: SchemaRef,
    pub capabilities: ResourceCapabilities,
    pub plan: FileResourcePlan,
    pub type_policy_allowances: TypePolicyAllowances,
    pub effective_schema_runtime: Option<EffectiveSchemaRuntime>,
    pub compiled_format: CompiledFormatBinding,
}

impl FileResource {
    pub fn new(
        definition: FileResourceDefinition,
        dependencies: FileRuntimeDependencies,
    ) -> Result<Self> {
        let FileResourceDefinition {
            descriptor,
            schema,
            capabilities,
            mut plan,
            type_policy_allowances,
            effective_schema_runtime,
            compiled_format,
        } = definition;
        let planned_driver = dependencies
            .formats()
            .resolve(plan.resolved_format()?.as_str())?;
        if compiled_format.descriptor.format_id != planned_driver.descriptor().format_id {
            return Err(CdfError::contract(format!(
                "compiled format `{}` does not match file plan format selection `{}`",
                compiled_format.descriptor.format_id,
                plan.resolved_format()?.as_str()
            )));
        }
        compiled_format.verify(dependencies.formats())?;
        plan.format = Some(FileFormatDeclaration::named(
            compiled_format.descriptor.format_id.as_str().to_owned(),
        )?);
        Ok(Self {
            descriptor,
            schema,
            capabilities,
            plan,
            type_policy_allowances,
            effective_schema_runtime: effective_schema_runtime.map(Arc::new),
            compiled_format,
            dependencies,
        })
    }

    pub fn validate_runtime_dependencies(&self) -> Result<()> {
        Ok(())
    }

    pub fn open_preview(
        &self,
        partition: PartitionPlan,
    ) -> BoxFuture<'_, Result<OpenedPartitionStream>> {
        open_file_resource_with_dependencies(self.clone(), partition)
    }
}

impl ResourceStream for FileResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn validate_runtime_dependencies(&self) -> Result<()> {
        FileResource::validate_runtime_dependencies(self)
    }

    fn type_policy_allowances(&self) -> cdf_kernel::TypePolicyAllowances {
        self.type_policy_allowances
    }

    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        if request.resource_id != self.descriptor.resource_id {
            return Err(CdfError::contract(format!(
                "scan request resource `{}` does not match compiled file resource `{}`",
                request.resource_id, self.descriptor.resource_id
            )));
        }
        self.dependencies.with_transport(|transport| {
            file_partitions_for_plan_with_transport(
                &self.descriptor,
                &self.plan,
                transport,
                self.dependencies.formats(),
                self.dependencies.transforms(),
            )
        })
    }

    fn open(&self, partition: PartitionPlan) -> BoxFuture<'_, Result<OpenedPartitionStream>> {
        open_file_resource_with_dependencies(self.clone(), partition)
    }

    fn attest_partition(
        &self,
        partition: &PartitionPlan,
    ) -> BoxFuture<'_, Result<Option<cdf_kernel::PartitionAttestation>>> {
        let descriptor = self.descriptor.clone();
        let plan = self.plan.clone();
        let partition = partition.clone();
        let dependencies = self.dependencies.clone();
        Box::pin(async move {
            let resolved = dependencies.with_transport(|transport| {
                validate_partition(
                    &descriptor,
                    &plan,
                    &partition,
                    transport,
                    dependencies.formats(),
                    dependencies.transforms(),
                )
            })?;
            let processed_position = SourcePosition::FileManifest(cdf_kernel::FileManifest {
                version: 1,
                files: vec![cdf_kernel::FilePosition {
                    path: resolved.path_text,
                    size_bytes: resolved.size_bytes,
                    source_generation: resolved.source_generation,
                    etag: resolved.etag,
                    object_version: resolved.version,
                    sha256: resolved.sha256,
                }],
            });
            Ok(Some(cdf_kernel::PartitionAttestation::new(
                processed_position,
                None,
            )))
        })
    }

    fn effective_schema_runtime(&self) -> Option<&EffectiveSchemaRuntime> {
        self.effective_schema_runtime.as_deref()
    }
}

impl QueryableResource for FileResource {
    fn capabilities(&self) -> &cdf_kernel::ResourceCapabilities {
        &self.capabilities
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        let partitions = self.plan_partitions(request)?;
        Ok(ScanPlan {
            plan_id: PlanId::new(format!("plan-{}", self.descriptor.resource_id))?,
            request: request.clone(),
            partitions,
            pushed_predicates: Vec::new(),
            unsupported_predicates: request.filters.clone(),
            estimated_rows: None,
            estimated_bytes: None,
            delivery_guarantee: delivery_guarantee(&self.descriptor),
        })
    }
}

fn delivery_guarantee(descriptor: &ResourceDescriptor) -> DeliveryGuarantee {
    match descriptor.write_disposition {
        WriteDisposition::Append => DeliveryGuarantee::AtLeastOnceDuplicateRisk,
        WriteDisposition::Replace => DeliveryGuarantee::EffectivelyOncePerTarget,
        WriteDisposition::Merge => DeliveryGuarantee::EffectivelyOncePerKey,
        WriteDisposition::CdcApply => DeliveryGuarantee::EffectivelyOncePerPosition,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ResolvedFileMatch {
    open: ResolvedFileOpen,
    path_text: String,
    size_bytes: u64,
    source_generation: Option<String>,
    identity_strength: GenerationStrength,
    sha256: Option<String>,
    etag: Option<String>,
    version: Option<String>,
    modified_ms: Option<String>,
    bytes_loaded: Option<u64>,
    compression: CompressionEvidence,
    format: FormatEvidence,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ResolvedFileOpen {
    LocalPath(PathBuf),
    Transport(FileTransportResource),
}

#[derive(Clone, Default)]
struct PhysicalSchemaAuthority {
    hash: Option<SchemaHash>,
    schema: Option<SchemaRef>,
}

enum PreparedFileInput {
    Source(Arc<dyn ByteSource>),
    SpoolSource {
        source: Arc<dyn ByteSource>,
        size_bytes: Option<u64>,
    },
}

struct PreparedInput {
    input: PreparedFileInput,
    extraction_content_hash: Option<SourceContentDigest>,
    hash_sweep_source: Option<Arc<dyn ByteSource>>,
    payload_retention: Option<PayloadRetention>,
}

fn retains_sequential_discovery_payload(descriptor: &cdf_runtime::FormatDriverDescriptor) -> bool {
    descriptor.source_access == cdf_runtime::FormatSourceAccess::Sequential
        && matches!(
            descriptor.discovery_kind,
            cdf_runtime::FormatDiscoveryKind::BoundedContent
                | cdf_runtime::FormatDiscoveryKind::FullContent
        )
}

struct PreparedFilePayload {
    source: Arc<dyn ByteSource>,
    source_content_digest: Option<SourceContentDigest>,
}

struct AccountedSpool {
    file: tempfile::NamedTempFile,
    _reservation: cdf_runtime::SpillReservation,
    bytes: u64,
}

struct SequentialPayloadCapture {
    source: Arc<CapturingSequentialByteSource>,
    state: Arc<tokio::sync::Mutex<SequentialCaptureState>>,
    memory: Arc<dyn cdf_memory::MemoryCoordinator>,
}

struct SequentialCaptureState {
    upstream: Option<Arc<tokio::sync::Mutex<AccountedByteStream>>>,
    output: Option<tokio::fs::File>,
    spool_file: Option<tempfile::NamedTempFile>,
    reservation: Option<cdf_runtime::SpillReservation>,
    captured_bytes: u64,
    opened: bool,
    maximum_spool_bytes: u64,
}

struct CapturingSequentialByteSource {
    upstream: Arc<dyn ByteSource>,
    capabilities: ByteSourceCapabilities,
    state: Arc<tokio::sync::Mutex<SequentialCaptureState>>,
}

struct ReplayThenContinueByteSource {
    identity: ContentIdentity,
    capabilities: ByteSourceCapabilities,
    state: Arc<Mutex<Option<ReplayContinuation>>>,
    memory: Arc<dyn cdf_memory::MemoryCoordinator>,
}

struct ReplayContinuation {
    spool_path: PathBuf,
    continuation: Arc<tokio::sync::Mutex<AccountedByteStream>>,
}

#[derive(Clone)]
struct HashingByteSource {
    inner: Arc<dyn ByteSource>,
    observation: SourceContentDigest,
}

impl HashingByteSource {
    fn new(inner: Arc<dyn ByteSource>, observation: SourceContentDigest) -> Self {
        Self { inner, observation }
    }
}

impl ByteSource for HashingByteSource {
    fn identity(&self) -> &cdf_runtime::ContentIdentity {
        self.inner.identity()
    }

    fn capabilities(&self) -> &cdf_runtime::ByteSourceCapabilities {
        self.inner.capabilities()
    }

    fn open_sequential(
        &self,
        request: SequentialReadRequest,
    ) -> BoxFuture<'_, Result<AccountedByteStream>> {
        Box::pin(async move {
            let input = self.inner.open_sequential(request).await?;
            let state = (input, Sha256::new(), self.observation.clone());
            Ok(Box::pin(futures_util::stream::try_unfold(
                state,
                |(mut input, mut hasher, observation)| async move {
                    match input.try_next().await? {
                        Some(chunk) => {
                            hasher.update(chunk.payload());
                            Ok(Some((chunk, (input, hasher, observation))))
                        }
                        None => {
                            observation.record(format!("sha256:{:x}", hasher.finalize()))?;
                            Ok(None)
                        }
                    }
                },
            )) as AccountedByteStream)
        })
    }

    fn read_exact_range(
        &self,
        extent: ByteExtent,
        cancellation: cdf_runtime::RunCancellation,
    ) -> BoxFuture<'_, Result<cdf_memory::AccountedBytes>> {
        self.inner.read_exact_range(extent, cancellation)
    }
}

impl AccountedSpool {
    fn path(&self) -> &Path {
        self.file.path()
    }

    fn bytes(&self) -> u64 {
        self.bytes
    }
}

fn retain_spool(spool: &Arc<AccountedSpool>, bytes: u64) -> Result<PayloadRetention> {
    let owner: Arc<dyn std::any::Any + Send + Sync> = spool.clone();
    PayloadRetention::new(owner, bytes)
}

fn prepared_file_payload(
    source: Arc<dyn ByteSource>,
    retention: PayloadRetention,
    source_content_digest: Option<SourceContentDigest>,
) -> Result<PreparedSourcePayload> {
    source.identity().validate()?;
    source.capabilities().validate()?;
    Ok(PreparedSourcePayload::new(
        PreparedFilePayload {
            source,
            source_content_digest,
        },
        retention,
    ))
}

impl SequentialPayloadCapture {
    async fn new(
        upstream: Arc<dyn ByteSource>,
        dependencies: &FileRuntimeDependencies,
    ) -> Result<Self> {
        let mut reservation = dependencies
            .execution()
            .spill()
            .try_reserve(1)?
            .ok_or_else(|| {
                let snapshot = dependencies.execution().spill().snapshot();
                CdfError::data(format!(
                    "retained discovery window requires spill capacity but {} of {} bytes are already reserved; raise the spill budget or reduce discovery concurrency",
                    snapshot.current_bytes, snapshot.budget_bytes
                ))
            })?;
        if reservation.bytes() == 0 && !reservation.try_grow(1)? {
            return Err(CdfError::data(
                "retained discovery window could not reserve its initial spill byte",
            ));
        }
        let spool_file = tempfile::NamedTempFile::new().map_err(|error| {
            CdfError::data(format!("create retained discovery window: {error}"))
        })?;
        let output = tokio::fs::File::create(spool_file.path())
            .await
            .map_err(|error| CdfError::data(format!("open retained discovery window: {error}")))?;
        let state = Arc::new(tokio::sync::Mutex::new(SequentialCaptureState {
            upstream: None,
            output: Some(output),
            spool_file: Some(spool_file),
            reservation: Some(reservation),
            captured_bytes: 0,
            opened: false,
            maximum_spool_bytes: dependencies.max_spool_bytes(),
        }));
        let mut capabilities = upstream.capabilities().clone();
        capabilities.reopenable = false;
        capabilities.validate()?;
        Ok(Self {
            source: Arc::new(CapturingSequentialByteSource {
                upstream,
                capabilities,
                state: Arc::clone(&state),
            }),
            state,
            memory: dependencies.execution().memory(),
        })
    }

    fn discovery_source(&self) -> Arc<dyn ByteSource> {
        Arc::clone(&self.source) as Arc<dyn ByteSource>
    }

    async fn finish(
        self,
        source_content_digest: Option<SourceContentDigest>,
    ) -> Result<PreparedSourcePayload> {
        let mut state = self.state.lock().await;
        if !state.opened {
            return Err(CdfError::internal(
                "format discovery did not open its retained sequential source",
            ));
        }
        let mut output = state.output.take().ok_or_else(|| {
            CdfError::internal("retained discovery window output was already finalized")
        })?;
        output
            .flush()
            .await
            .map_err(|error| CdfError::data(format!("flush retained discovery window: {error}")))?;
        drop(output);
        let captured_bytes = state.captured_bytes;
        if captured_bytes == 0 {
            return Err(CdfError::data(
                "format discovery retained no source bytes for execution",
            ));
        }
        let continuation = state.upstream.take().ok_or_else(|| {
            CdfError::internal("retained discovery window omitted its live continuation")
        })?;
        let spool_file = state.spool_file.take().ok_or_else(|| {
            CdfError::internal("retained discovery window omitted its spool file")
        })?;
        let reservation = state.reservation.take().ok_or_else(|| {
            CdfError::internal("retained discovery window omitted its spill reservation")
        })?;
        drop(state);

        let spool = Arc::new(AccountedSpool {
            file: spool_file,
            _reservation: reservation,
            bytes: captured_bytes,
        });
        let upstream_capabilities = self.source.capabilities();
        let capabilities = ByteSourceCapabilities {
            known_length: upstream_capabilities.known_length,
            reopenable: false,
            seekable: false,
            exact_ranges: false,
            useful_range_concurrency: 0,
            minimum_chunk_bytes: upstream_capabilities.minimum_chunk_bytes,
            maximum_chunk_bytes: upstream_capabilities.maximum_chunk_bytes,
        };
        capabilities.validate()?;
        let replay: Arc<dyn ByteSource> = Arc::new(ReplayThenContinueByteSource {
            identity: self.source.identity().clone(),
            capabilities,
            state: Arc::new(Mutex::new(Some(ReplayContinuation {
                spool_path: spool.path().to_path_buf(),
                continuation,
            }))),
            memory: self.memory,
        });
        prepared_file_payload(
            replay,
            retain_spool(&spool, captured_bytes)?,
            source_content_digest,
        )
    }
}

impl ByteSource for CapturingSequentialByteSource {
    fn identity(&self) -> &ContentIdentity {
        self.upstream.identity()
    }

    fn capabilities(&self) -> &ByteSourceCapabilities {
        &self.capabilities
    }

    fn open_sequential(
        &self,
        request: SequentialReadRequest,
    ) -> BoxFuture<'_, Result<AccountedByteStream>> {
        Box::pin(async move {
            {
                let mut state = self.state.lock().await;
                if state.opened {
                    return Err(CdfError::contract(
                        "retained discovery source may be opened only once",
                    ));
                }
                // Claim the single invocation before crossing the source boundary. A
                // duplicate open must not contact the transport and then fail locally.
                state.opened = true;
            }
            let input = self.upstream.open_sequential(request).await?;
            {
                let mut state = self.state.lock().await;
                state.upstream = Some(Arc::new(tokio::sync::Mutex::new(input)));
            }
            let state = Arc::clone(&self.state);
            Ok(Box::pin(futures_util::stream::try_unfold(
                state,
                |state| async move {
                    let upstream = {
                        let state_guard = state.lock().await;
                        Arc::clone(state_guard.upstream.as_ref().ok_or_else(|| {
                            CdfError::internal("retained discovery source lost its upstream stream")
                        })?)
                    };
                    let next = upstream.lock().await.try_next().await?;
                    let Some(chunk) = next else {
                        return Ok(None);
                    };
                    let chunk_bytes = u64::try_from(chunk.payload().len())
                        .map_err(|_| CdfError::data("retained discovery chunk exceeds u64"))?;
                    let mut state_guard = state.lock().await;
                    let next_bytes = state_guard
                        .captured_bytes
                        .checked_add(chunk_bytes)
                        .ok_or_else(|| {
                            CdfError::data("retained discovery byte count overflowed")
                        })?;
                    if next_bytes > state_guard.maximum_spool_bytes {
                        return Err(CdfError::data(
                            "retained discovery window exceeded the configured spool budget",
                        ));
                    }
                    let reservation = state_guard.reservation.as_mut().ok_or_else(|| {
                        CdfError::internal("retained discovery spill reservation was finalized")
                    })?;
                    if next_bytes > reservation.bytes()
                        && !reservation.try_grow(next_bytes - reservation.bytes())?
                    {
                        return Err(CdfError::data(
                            "retained discovery window exhausted the shared spill budget",
                        ));
                    }
                    state_guard
                        .output
                        .as_mut()
                        .ok_or_else(|| {
                            CdfError::internal("retained discovery output was finalized")
                        })?
                        .write_all(chunk.payload())
                        .await
                        .map_err(|error| {
                            CdfError::data(format!("write retained discovery window: {error}"))
                        })?;
                    state_guard.captured_bytes = next_bytes;
                    drop(state_guard);
                    Ok(Some((chunk, state)))
                },
            )) as AccountedByteStream)
        })
    }

    fn read_exact_range(
        &self,
        extent: ByteExtent,
        cancellation: cdf_runtime::RunCancellation,
    ) -> BoxFuture<'_, Result<cdf_memory::AccountedBytes>> {
        self.upstream.read_exact_range(extent, cancellation)
    }
}

impl ByteSource for ReplayThenContinueByteSource {
    fn identity(&self) -> &ContentIdentity {
        &self.identity
    }

    fn capabilities(&self) -> &ByteSourceCapabilities {
        &self.capabilities
    }

    fn open_sequential(
        &self,
        request: SequentialReadRequest,
    ) -> BoxFuture<'_, Result<AccountedByteStream>> {
        Box::pin(async move {
            request.cancellation.check()?;
            let continuation = self
                .state
                .lock()
                .map_err(|_| CdfError::internal("retained payload state was poisoned"))?
                .take()
                .ok_or_else(|| {
                    CdfError::contract("retained source payload may be consumed only once")
                })?;
            let replay_source =
                LocalByteSource::open(&continuation.spool_path, Arc::clone(&self.memory))?;
            let replay_chunk_bytes = request.preferred_chunk_bytes.clamp(
                replay_source.capabilities().minimum_chunk_bytes,
                replay_source.capabilities().maximum_chunk_bytes,
            );
            let replay = replay_source
                .open_sequential(SequentialReadRequest {
                    preferred_chunk_bytes: replay_chunk_bytes,
                    cancellation: request.cancellation.clone(),
                })
                .await?;
            let state = (
                replay,
                continuation.continuation,
                false,
                request.cancellation,
            );
            Ok(Box::pin(futures_util::stream::try_unfold(
                state,
                |(mut replay, continuation, replay_done, cancellation)| async move {
                    cancellation.check()?;
                    if !replay_done && let Some(chunk) = replay.try_next().await? {
                        return Ok(Some((chunk, (replay, continuation, false, cancellation))));
                    }
                    let next = continuation.lock().await.try_next().await?;
                    Ok(next.map(|chunk| (chunk, (replay, continuation, true, cancellation))))
                },
            )) as AccountedByteStream)
        })
    }

    fn read_exact_range(
        &self,
        _extent: ByteExtent,
        _cancellation: cdf_runtime::RunCancellation,
    ) -> BoxFuture<'_, Result<cdf_memory::AccountedBytes>> {
        Box::pin(async {
            Err(CdfError::contract(
                "retained sequential payload does not support independent ranges",
            ))
        })
    }
}

struct PreparedFilePartition {
    resolved: ResolvedFileMatch,
    input: PreparedFileInput,
    options: ReadOptions,
    admission_schema: SchemaRef,
    physical_schema_authority: PhysicalSchemaAuthority,
    canonical_format_options: serde_json::Value,
    driver: Arc<dyn FormatDriver>,
    extraction_content_hash: Option<SourceContentDigest>,
    hash_sweep_source: Option<Arc<dyn ByteSource>>,
    payload_retention: Option<PayloadRetention>,
}

struct PreparedFilePayloadKeyInput<'a> {
    resource_id: &'a ResourceId,
    location: &'a str,
    size_bytes: u64,
    source_generation: Option<&'a str>,
    etag: Option<&'a str>,
    object_version: Option<&'a str>,
    sha256: Option<&'a str>,
    driver: &'a dyn FormatDriver,
    canonical_format_options: &'a serde_json::Value,
    transform_name: &'a str,
}

fn prepared_file_payload_key(
    input: PreparedFilePayloadKeyInput<'_>,
    dependencies: &FileRuntimeDependencies,
) -> Result<PreparedSourcePayloadKey> {
    let transform = if input.transform_name == "none" {
        serde_json::json!({"id": "none", "version": "none"})
    } else {
        let transform = dependencies
            .transforms()
            .resolve_name(input.transform_name)?;
        serde_json::json!({
            "id": transform.descriptor().transform_id.as_str(),
            "version": transform.descriptor().semantic_version,
        })
    };
    let payload_hash = cdf_runtime::artifact_hash(&serde_json::json!({
        "version": 1,
        "resource_id": input.resource_id.as_str(),
        "location": input.location,
        "size_bytes": input.size_bytes,
        "source_generation": input.source_generation,
        "etag": input.etag,
        "object_version": input.object_version,
        "sha256": input.sha256,
        "format": {
            "id": input.driver.descriptor().format_id.as_str(),
            "version": input.driver.descriptor().semantic_version,
            "options": input.canonical_format_options,
        },
        "transform": transform,
    }))?;
    PreparedSourcePayloadKey::new(
        input.resource_id.clone(),
        SourceDriverId::new("files")?,
        payload_hash,
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CompressionEvidence {
    transform_id: Option<ByteTransformId>,
    extension_signal: CompressionSignal,
    magic_signal: CompressionSignal,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct CompressionSignal(Option<ByteTransformId>);

#[derive(Clone, Debug, PartialEq, Eq)]
struct FormatEvidence {
    format_id: String,
    driver_version: String,
    extension: Option<String>,
    detection: FormatDetection,
}

impl CompressionSignal {
    fn as_str(&self) -> &str {
        self.0.as_ref().map_or("none", ByteTransformId::as_str)
    }

    fn transform_id(&self) -> Option<&ByteTransformId> {
        self.0.as_ref()
    }
}

impl CompressionEvidence {
    fn mode_name(&self) -> &str {
        self.transform_id
            .as_ref()
            .map_or("none", ByteTransformId::as_str)
    }
}

pub fn file_partitions_for_plan(
    descriptor: &ResourceDescriptor,
    plan: &FileResourcePlan,
    formats: &FormatRegistry,
    transforms: &ByteTransformRegistry,
) -> Result<Vec<PartitionPlan>> {
    let transport = FileTransportFacade::new();
    file_partitions_for_plan_with_transport(descriptor, plan, &transport, formats, transforms)
}

pub(crate) fn file_partitions_for_plan_with_transport(
    descriptor: &ResourceDescriptor,
    plan: &FileResourcePlan,
    transport: &dyn FileTransport,
    formats: &FormatRegistry,
    transforms: &ByteTransformRegistry,
) -> Result<Vec<PartitionPlan>> {
    let matches = resolve_file_matches(
        &descriptor.resource_id,
        plan,
        transport,
        formats,
        transforms,
    )?;
    if matches.is_empty() {
        return Err(no_file_matches_error(&descriptor.resource_id, plan));
    }

    let total_matches = matches.len();
    matches
        .iter()
        .map(|file| partition_for_file_match(descriptor, plan, file, total_matches))
        .collect()
}

fn open_file_resource_with_dependencies(
    resource: FileResource,
    partition: PartitionPlan,
) -> BoxFuture<'static, Result<OpenedPartitionStream>> {
    let FileResource {
        descriptor,
        schema,
        capabilities: _,
        plan,
        type_policy_allowances: _,
        effective_schema_runtime,
        compiled_format,
        dependencies,
    } = resource;
    let prepared = match prepare_file_partition(
        &descriptor,
        &plan,
        &partition,
        schema,
        &dependencies,
        effective_schema_runtime.as_deref(),
        &compiled_format,
    ) {
        Ok(prepared) => prepared,
        Err(error) => return Box::pin(async move { Err(error) }),
    };
    let execution = dependencies.execution().clone();
    let extraction_content_hash = prepared.extraction_content_hash.clone();
    let hash_sweep_source = prepared.hash_sweep_source.clone();
    let completed_position = cdf_kernel::FilePosition {
        path: prepared.resolved.path_text.clone(),
        size_bytes: prepared.resolved.size_bytes,
        source_generation: prepared.resolved.source_generation.clone(),
        etag: prepared.resolved.etag.clone(),
        object_version: prepared.resolved.version.clone(),
        sha256: prepared.resolved.sha256.clone(),
    };
    let (completion_sender, completion_receiver) = tokio::sync::oneshot::channel();
    let mut scope_hasher = Sha256::new();
    scope_hasher.update(descriptor.resource_id.as_str().as_bytes());
    scope_hasher.update([0]);
    scope_hasher.update(partition.partition_id.as_str().as_bytes());
    let scope_id = format!("file-open-{}", &hex::encode(scope_hasher.finalize())[..16]);
    let stream = execution.spawn_io_stream(
        &scope_id,
        NATIVE_STREAM_ITEMS,
        move |mut sender, cancellation| async move {
            let decode = async {
                let prepared_stream =
                    stream_prepared_file_match(prepared, &dependencies, cancellation.clone())
                        .await?;
                let PreparedFormatStream {
                    mut batches,
                    source_completion,
                } = prepared_stream;
                let forward = async {
                    while let Some(batch) = batches.try_next().await? {
                        sender.send(batch).await?;
                    }
                    Ok::<_, CdfError>(())
                };
                if let Some(source_completion) = source_completion {
                    tokio::try_join!(forward, source_completion)?;
                } else {
                    forward.await?;
                }
                Ok::<_, CdfError>(())
            };
            let hash_sweep = complete_hash_sweep(hash_sweep_source, cancellation.clone());
            tokio::try_join!(decode, hash_sweep)?;
            let completion = if let Some(extraction_content_hash) = extraction_content_hash {
                let mut completed_position = completed_position;
                completed_position.sha256 = Some(extraction_content_hash.completed()?);
                Some(PartitionAttestation::new(
                    SourcePosition::FileManifest(cdf_kernel::FileManifest {
                        version: 1,
                        files: vec![completed_position],
                    }),
                    None,
                ))
            } else {
                None
            };
            let _ = completion_sender.send(completion);
            Ok(())
        },
    );
    Box::pin(async move {
        let stream = Box::pin(stream?) as BatchStream;
        let completion = Box::pin(async move {
            completion_receiver.await.map_err(|_| {
                CdfError::internal(
                    "partition stream ended without publishing its invocation completion",
                )
            })
        });
        Ok(OpenedPartitionStream::with_completion(stream, completion))
    })
}

async fn complete_hash_sweep(
    source: Option<Arc<dyn ByteSource>>,
    cancellation: cdf_runtime::RunCancellation,
) -> Result<()> {
    let Some(source) = source else {
        return Ok(());
    };
    let preferred_chunk_bytes = (4 * 1024 * 1024_u64).clamp(
        source.capabilities().minimum_chunk_bytes,
        source.capabilities().maximum_chunk_bytes,
    );
    let mut stream = source
        .open_sequential(SequentialReadRequest {
            preferred_chunk_bytes,
            cancellation: cancellation.clone(),
        })
        .await?;
    while stream.try_next().await?.is_some() {
        cancellation.check()?;
    }
    Ok(())
}

fn prepare_file_partition(
    descriptor: &ResourceDescriptor,
    plan: &FileResourcePlan,
    partition: &PartitionPlan,
    admission_schema: SchemaRef,
    dependencies: &FileRuntimeDependencies,
    effective_schema_runtime: Option<&EffectiveSchemaRuntime>,
    compiled_format: &CompiledFormatBinding,
) -> Result<PreparedFilePartition> {
    let resolved = dependencies.with_transport(|transport| {
        validate_partition(
            descriptor,
            plan,
            partition,
            transport,
            dependencies.formats(),
            dependencies.transforms(),
        )
    })?;
    let planned_physical_schema_hash = partition
        .metadata
        .get(PLAN_PHYSICAL_SCHEMA_HASH_KEY)
        .map(|value| SchemaHash::new(value.clone()))
        .transpose()?;
    let planned_physical_schema = planned_physical_schema_hash
        .as_ref()
        .and_then(|hash| effective_schema_runtime.and_then(|runtime| runtime.physical_schema(hash)))
        .cloned();
    let options = ReadOptions::new(
        descriptor.resource_id.clone(),
        partition.partition_id.clone(),
    );
    let driver = compiled_format.verify(dependencies.formats())?;
    let source_access = driver.descriptor().source_access;
    let prepared_input = prepare_file_input(
        &descriptor.resource_id,
        &resolved,
        source_access,
        driver.as_ref(),
        &compiled_format.canonical_options,
        dependencies,
    )?;
    Ok(PreparedFilePartition {
        resolved,
        input: prepared_input.input,
        options,
        admission_schema,
        physical_schema_authority: PhysicalSchemaAuthority {
            hash: planned_physical_schema_hash,
            schema: planned_physical_schema,
        },
        canonical_format_options: compiled_format.canonical_options.clone(),
        driver,
        extraction_content_hash: prepared_input.extraction_content_hash,
        hash_sweep_source: prepared_input.hash_sweep_source,
        payload_retention: prepared_input.payload_retention,
    })
}

fn prepare_file_input(
    resource_id: &ResourceId,
    resolved: &ResolvedFileMatch,
    source_access: cdf_runtime::FormatSourceAccess,
    driver: &dyn FormatDriver,
    canonical_format_options: &serde_json::Value,
    dependencies: &FileRuntimeDependencies,
) -> Result<PreparedInput> {
    let prepared_payload_key = prepared_file_payload_key(
        PreparedFilePayloadKeyInput {
            resource_id,
            location: &resolved.path_text,
            size_bytes: resolved.size_bytes,
            source_generation: resolved.source_generation.as_deref(),
            etag: resolved.etag.as_deref(),
            object_version: resolved.version.as_deref(),
            sha256: resolved.sha256.as_deref(),
            driver,
            canonical_format_options,
            transform_name: resolved.compression.mode_name(),
        },
        dependencies,
    )?;
    if let Some(payload) = dependencies
        .prepared_payloads()
        .take(&prepared_payload_key)?
    {
        let (payload, retention) =
            payload.into_typed::<PreparedFilePayload>("file source execution")?;
        return Ok(PreparedInput {
            input: PreparedFileInput::Source(payload.source),
            extraction_content_hash: payload.source_content_digest,
            hash_sweep_source: None,
            payload_retention: Some(retention),
        });
    }
    if resolved.compression.transform_id.is_none() {
        let expected = expected_file_identity(resolved);
        let (source, extraction_content_hash): (Arc<dyn ByteSource>, Option<SourceContentDigest>) =
            match &resolved.open {
                ResolvedFileOpen::LocalPath(path) => {
                    let local: Arc<dyn ByteSource> = Arc::new(LocalByteSource::open(
                        path,
                        dependencies.execution().memory(),
                    )?);
                    verify_opened_local_generation(resolved, local.as_ref())?;
                    let observation = SourceContentDigest::default();
                    (
                        Arc::new(HashingByteSource::new(local, observation.clone())),
                        Some(observation),
                    )
                }
                ResolvedFileOpen::Transport(resource) => {
                    let upstream = dependencies.with_transport(|transport| {
                        transport.open_byte_source(
                            resource,
                            &expected,
                            dependencies.execution().memory(),
                        )
                    })?;
                    if resolved.identity_strength == GenerationStrength::Weak {
                        let observation = SourceContentDigest::default();
                        (
                            Arc::new(HashingByteSource::new(upstream, observation.clone())),
                            Some(observation),
                        )
                    } else {
                        (upstream, None)
                    }
                }
            };
        let transport_spool = matches!(resolved.open, ResolvedFileOpen::Transport(_))
            && source_access == cdf_runtime::FormatSourceAccess::Adaptive;
        let hash_sweep_source = (extraction_content_hash.is_some()
            && source_access != cdf_runtime::FormatSourceAccess::Sequential
            && !transport_spool)
            .then(|| Arc::clone(&source));
        let input = if transport_spool {
            PreparedFileInput::SpoolSource {
                source,
                size_bytes: Some(resolved.size_bytes),
            }
        } else {
            PreparedFileInput::Source(source)
        };
        return Ok(PreparedInput {
            input,
            extraction_content_hash,
            hash_sweep_source,
            payload_retention: None,
        });
    }
    if let Some(transform_id) = &resolved.compression.transform_id {
        let expected = expected_file_identity(resolved);
        let (upstream, extraction_content_hash): (
            Arc<dyn ByteSource>,
            Option<SourceContentDigest>,
        ) = match &resolved.open {
            ResolvedFileOpen::LocalPath(path) => {
                let local: Arc<dyn ByteSource> = Arc::new(LocalByteSource::open(
                    path,
                    dependencies.execution().memory(),
                )?);
                verify_opened_local_generation(resolved, local.as_ref())?;
                let observation = SourceContentDigest::default();
                (
                    Arc::new(HashingByteSource::new(local, observation.clone())),
                    Some(observation),
                )
            }
            ResolvedFileOpen::Transport(resource) => {
                let upstream = dependencies.with_transport(|transport| {
                    transport.open_byte_source(
                        resource,
                        &expected,
                        dependencies.execution().memory(),
                    )
                })?;
                if resolved.identity_strength == GenerationStrength::Weak {
                    let observation = SourceContentDigest::default();
                    (
                        Arc::new(HashingByteSource::new(upstream, observation.clone())),
                        Some(observation),
                    )
                } else {
                    (upstream, None)
                }
            }
        };
        let transformed = transformed_byte_source(upstream, transform_id, dependencies)?;
        let input = if source_access != cdf_runtime::FormatSourceAccess::Sequential {
            PreparedFileInput::SpoolSource {
                source: transformed,
                size_bytes: None,
            }
        } else {
            PreparedFileInput::Source(transformed)
        };
        return Ok(PreparedInput {
            input,
            extraction_content_hash,
            hash_sweep_source: None,
            payload_retention: None,
        });
    }
    Err(CdfError::internal(
        "file preparation reached an unclassified compression state",
    ))
}

fn verify_opened_local_generation(
    resolved: &ResolvedFileMatch,
    source: &dyn ByteSource,
) -> Result<()> {
    let observed = source.identity();
    if observed.size_bytes != Some(resolved.size_bytes)
        || observed.generation.as_ref() != resolved.source_generation.as_ref()
    {
        return Err(CdfError::data(format!(
            "local file `{}` changed between planning and open; re-plan before retrying",
            resolved.path_text
        )));
    }
    Ok(())
}

fn expected_file_identity(resolved: &ResolvedFileMatch) -> FileIdentityMetadata {
    FileIdentityMetadata {
        location: resolved.path_text.clone(),
        size_bytes: Some(resolved.size_bytes),
        checksum: resolved.sha256.as_ref().map(|sha256| crate::FileChecksum {
            algorithm: "sha256".to_owned(),
            value: sha256.clone(),
        }),
        etag: resolved.etag.clone(),
        version: resolved.version.clone(),
        modified: resolved.source_generation.clone(),
    }
}

async fn stream_prepared_file_match(
    prepared: PreparedFilePartition,
    dependencies: &FileRuntimeDependencies,
    cancellation: cdf_runtime::RunCancellation,
) -> Result<PreparedFormatStream> {
    let PreparedFilePartition {
        resolved,
        input: prepared,
        options,
        admission_schema,
        physical_schema_authority,
        canonical_format_options,
        driver,
        extraction_content_hash: _,
        hash_sweep_source: _,
        payload_retention,
    } = prepared;
    let position = Some(SourcePosition::FileManifest(cdf_kernel::FileManifest {
        version: 1,
        files: vec![cdf_kernel::FilePosition {
            path: resolved.path_text.clone(),
            size_bytes: resolved.size_bytes,
            source_generation: resolved.source_generation.clone(),
            etag: resolved.etag.clone(),
            object_version: resolved.version.clone(),
            sha256: resolved.sha256.clone(),
        }],
    }));
    let ReadyFileInput {
        source,
        payload_retention,
        source_completion,
    } = match prepared {
        PreparedFileInput::Source(source) => ReadyFileInput {
            source,
            payload_retention,
            source_completion: None,
        },
        PreparedFileInput::SpoolSource { source, size_bytes } => {
            if payload_retention.is_some() {
                return Err(CdfError::internal(
                    "prepared source payload cannot request a second spool",
                ));
            }
            if let Some(size_bytes) = size_bytes
                && source.identity().strength != GenerationStrength::Weak
                && source.capabilities().exact_ranges
            {
                let growing = start_growing_spool(
                    Arc::clone(&source),
                    size_bytes,
                    dependencies.max_spool_bytes(),
                    dependencies.execution().spill(),
                    dependencies.execution().memory(),
                    cancellation.clone(),
                )?;
                if let Some(growing) = growing {
                    ReadyFileInput {
                        source: growing.source,
                        payload_retention: Some(growing.retention),
                        source_completion: Some(growing.completion),
                    }
                } else {
                    ReadyFileInput {
                        source,
                        payload_retention: None,
                        source_completion: None,
                    }
                }
            } else {
                let spool = Arc::new(
                    spool_byte_source_async(source, size_bytes, dependencies, cancellation.clone())
                        .await?,
                );
                let local = Arc::new(LocalByteSource::open(
                    spool.path(),
                    dependencies.execution().memory(),
                )?);
                let retention = retain_spool(&spool, spool.bytes())?;
                ReadyFileInput {
                    source: local,
                    payload_retention: Some(retention),
                    source_completion: None,
                }
            }
        }
    };

    let batches = stream_registered_format(
        RegisteredFormatStreamRequest {
            source,
            payload_retention,
            driver,
            options,
            admission_schema,
            canonical_format_options,
            source_position: position,
            physical_schema_authority,
        },
        dependencies,
    )?;
    Ok(PreparedFormatStream {
        batches,
        source_completion,
    })
}

struct PreparedFormatStream {
    batches: BatchStream,
    source_completion: Option<BoxFuture<'static, Result<()>>>,
}

struct ReadyFileInput {
    source: Arc<dyn ByteSource>,
    payload_retention: Option<PayloadRetention>,
    source_completion: Option<BoxFuture<'static, Result<()>>>,
}

#[cfg(test)]
fn stream_file_match_blocking(
    resolved: &ResolvedFileMatch,
    declaration: &FileFormatDeclaration,
    options: ReadOptions,
    dependencies: &FileRuntimeDependencies,
    admission_schema: SchemaRef,
    physical_schema_authority: PhysicalSchemaAuthority,
) -> Result<BatchStream> {
    let driver = dependencies.formats().resolve(declaration.as_str())?;
    let canonical_format_options = driver.canonical_options(serde_json::json!({}))?;
    let prepared_input = prepare_file_input(
        &options.resource_id,
        resolved,
        driver.descriptor().source_access,
        driver.as_ref(),
        &canonical_format_options,
        dependencies,
    )?;
    let prepared = PreparedFilePartition {
        resolved: resolved.clone(),
        input: prepared_input.input,
        options,
        admission_schema,
        physical_schema_authority,
        canonical_format_options,
        driver,
        extraction_content_hash: prepared_input.extraction_content_hash,
        hash_sweep_source: prepared_input.hash_sweep_source,
        payload_retention: prepared_input.payload_retention,
    };
    let dependencies = dependencies.clone();
    let execution = dependencies.execution().clone();
    let prepared_stream = execution.run_io(async move {
        stream_prepared_file_match(
            prepared,
            &dependencies,
            cdf_runtime::RunCancellation::default(),
        )
        .await
    })?;
    let PreparedFormatStream {
        mut batches,
        source_completion,
    } = prepared_stream;
    let Some(source_completion) = source_completion else {
        return Ok(batches);
    };
    let stream = execution.spawn_io_stream(
        "file-test-spool-completion",
        NATIVE_STREAM_ITEMS,
        move |mut sender, _| async move {
            let forward = async {
                while let Some(batch) = batches.try_next().await? {
                    sender.send(batch).await?;
                }
                Ok::<_, CdfError>(())
            };
            tokio::try_join!(forward, source_completion)?;
            Ok(())
        },
    )?;
    Ok(Box::pin(stream))
}

async fn spool_byte_source_async(
    source: Arc<dyn ByteSource>,
    size_bytes: Option<u64>,
    dependencies: &FileRuntimeDependencies,
    cancellation: cdf_runtime::RunCancellation,
) -> Result<AccountedSpool> {
    if size_bytes.is_some_and(|bytes| bytes > dependencies.max_spool_bytes()) {
        return Err(CdfError::data(format!(
            "file requires {} spool bytes, exceeding the configured {}-byte disk budget; increase the spool budget or use a streaming format runtime",
            size_bytes.unwrap_or_default(),
            dependencies.max_spool_bytes()
        )));
    }
    let initially_reserved = size_bytes.unwrap_or(1).max(1);
    let mut reservation = dependencies
        .execution()
        .spill()
        .try_reserve(initially_reserved)?
        .ok_or_else(|| {
            let snapshot = dependencies.execution().spill().snapshot();
            CdfError::data(format!(
                "file spool requires {initially_reserved} bytes but the shared spill budget has {} of {} bytes in use; increase the spill budget or reduce concurrent files",
                snapshot.current_bytes, snapshot.budget_bytes
            ))
        })?;
    let file = tempfile::NamedTempFile::new()
        .map_err(|error| CdfError::data(format!("create accounted file spool: {error}")))?;
    let mut output = tokio::fs::File::create(file.path())
        .await
        .map_err(|error| CdfError::data(format!("open accounted file spool: {error}")))?;
    let capabilities = source.capabilities();
    let chunk_bytes = (4 * 1024 * 1024_u64).clamp(
        capabilities.minimum_chunk_bytes,
        capabilities.maximum_chunk_bytes,
    );
    let mut input = source
        .open_sequential(SequentialReadRequest {
            preferred_chunk_bytes: chunk_bytes,
            cancellation: cancellation.clone(),
        })
        .await?;
    let mut transferred = 0_u64;
    let expected_checksum = source.identity().checksum.clone();
    let mut hasher = expected_checksum.as_ref().map(|_| Sha256::new());
    while let Some(chunk) = input.try_next().await? {
        cancellation.check()?;
        let chunk_bytes = u64::try_from(chunk.payload().len())
            .map_err(|_| CdfError::data("file spool chunk exceeds u64"))?;
        let next_transferred = transferred
            .checked_add(chunk_bytes)
            .ok_or_else(|| CdfError::data("file spool byte count overflowed"))?;
        if next_transferred > dependencies.max_spool_bytes() {
            return Err(CdfError::data(
                "file spool exceeded its configured disk bound",
            ));
        }
        if next_transferred > reservation.bytes()
            && !reservation.try_grow(next_transferred - reservation.bytes())?
        {
            return Err(CdfError::data(
                "file spool exhausted the shared spill budget while streaming transformed output",
            ));
        }
        if size_bytes.is_some_and(|expected| next_transferred > expected) {
            return Err(CdfError::data(
                "file spool exceeded its planned generation length",
            ));
        }
        output
            .write_all(chunk.payload())
            .await
            .map_err(|error| CdfError::data(format!("write accounted file spool: {error}")))?;
        if let Some(hasher) = &mut hasher {
            hasher.update(chunk.payload());
        }
        transferred = next_transferred;
    }
    output
        .flush()
        .await
        .map_err(|error| CdfError::data(format!("flush accounted file spool: {error}")))?;
    if let Some(size_bytes) = size_bytes
        && transferred != size_bytes
    {
        return Err(CdfError::data(format!(
            "file spool wrote {transferred} bytes for a planned {size_bytes}-byte generation"
        )));
    }
    if let (Some(expected), Some(hasher)) = (expected_checksum.as_deref(), hasher) {
        let observed = hex::encode(hasher.finalize());
        let expected = expected.strip_prefix("sha256:").unwrap_or(expected);
        if observed != expected {
            return Err(CdfError::data(
                "file spool checksum does not match planned content identity",
            ));
        }
    }
    cancellation.check()?;
    Ok(AccountedSpool {
        file,
        _reservation: reservation,
        bytes: transferred,
    })
}

fn transformed_byte_source(
    upstream: Arc<dyn ByteSource>,
    transform_id: &ByteTransformId,
    dependencies: &FileRuntimeDependencies,
) -> Result<Arc<dyn ByteSource>> {
    const TRANSFORM_CHUNK_BYTES: u64 = 1024 * 1024;

    let transform = dependencies.transforms().resolve(transform_id)?;
    let descriptor = transform.descriptor().clone();
    let preferred_input_chunk_bytes = TRANSFORM_CHUNK_BYTES.clamp(
        upstream.capabilities().minimum_chunk_bytes,
        upstream.capabilities().maximum_chunk_bytes,
    );
    Ok(Arc::new(TransformedByteSource::new(
        upstream,
        transform,
        TransformSourceConfig {
            preferred_input_chunk_bytes,
            maximum_expanded_bytes: descriptor.maximum_expanded_bytes,
            maximum_expansion_ratio: descriptor.maximum_expansion_ratio,
            memory: dependencies.execution().memory(),
            consumer: ConsumerKey::new(
                format!("file-transform-{}", descriptor.transform_id.as_str()),
                MemoryClass::Transform,
            )?,
        },
    )?))
}

struct RegisteredFormatStreamRequest {
    source: Arc<dyn ByteSource>,
    payload_retention: Option<PayloadRetention>,
    driver: Arc<dyn FormatDriver>,
    options: ReadOptions,
    admission_schema: SchemaRef,
    canonical_format_options: serde_json::Value,
    source_position: Option<SourcePosition>,
    physical_schema_authority: PhysicalSchemaAuthority,
}

fn stream_registered_format(
    request: RegisteredFormatStreamRequest,
    dependencies: &FileRuntimeDependencies,
) -> Result<BatchStream> {
    let RegisteredFormatStreamRequest {
        source,
        payload_retention,
        driver,
        options,
        admission_schema,
        canonical_format_options,
        source_position,
        physical_schema_authority,
    } = request;
    let execution = dependencies.execution().clone();
    let memory = execution.memory();
    let scope_id = format!(
        "format-{}-{}",
        driver.descriptor().format_id,
        options.batch_id_prefix
    );
    let unit_execution = execution.clone();
    let unit_scope_prefix = scope_id.clone();
    let stream = execution.spawn_io_stream(
        &scope_id,
        NATIVE_STREAM_ITEMS,
        move |mut sender, cancellation| async move {
            let _payload_retention = payload_retention;
            let options_json = driver.canonical_options(canonical_format_options)?;
            let decode_schema = match physical_schema_authority.schema {
                Some(schema) => {
                    let schema_hash =
                        cdf_kernel::canonical_arrow_schema_hash(schema.as_ref())?;
                    if let Some(planned_hash) = &physical_schema_authority.hash
                        && planned_hash != &schema_hash
                    {
                        return Err(CdfError::data(format!(
                            "plan physical schema catalog hash {schema_hash} does not match partition authority {planned_hash}"
                        )));
                    }
                    cdf_runtime::DecodeSchemaPlan::verified_physical(schema)
                }
                None => cdf_runtime::DecodeSchemaPlan::fixed_admission(admission_schema),
            };
            let session = driver
                .prepare_decode(
                    source.clone(),
                    DecodePlanningRequest {
                        options: options_json.clone(),
                        projection: None,
                        predicates: Vec::new(),
                        target_batch_rows: NATIVE_TARGET_BATCH_ROWS,
                        target_batch_bytes: NATIVE_TARGET_BATCH_BYTES,
                        cancellation: cancellation.clone(),
                    },
                )
                .await?;
            let units = session.units().to_vec();
            if units.is_empty() {
                return Err(CdfError::contract(
                    "prepared format session must contain at least one decode unit",
                ));
            }
            let memory_snapshot = memory.snapshot();
            let available_memory = memory_snapshot
                .budget_bytes
                .saturating_sub(memory_snapshot.current_bytes);
            let unit_jobs = if units.len() == 1 {
                1
            } else {
                match resolve_decode_unit_concurrency(
                    &units,
                    &unit_execution.capabilities(),
                    available_memory,
                    source.capabilities().useful_range_concurrency.max(1),
                    NATIVE_TARGET_BATCH_BYTES,
                    NATIVE_UNIT_BUFFERED_BATCHES,
                ) {
                    Ok(resolution) => usize::from(resolution.jobs),
                    Err(error) if error.kind == cdf_kernel::ErrorKind::Data => 1,
                    Err(error) => return Err(error),
                }
            };
            if unit_jobs == 1 {
                for unit in units {
                    let mut decoded = session
                        .decode(PhysicalDecodeRequest {
                            unit,
                            resource_id: options.resource_id.clone(),
                            partition_id: options.partition_id.clone(),
                            batch_id_prefix: options.batch_id_prefix.clone(),
                            schema: decode_schema.clone(),
                            source_position: source_position.clone(),
                            projection: None,
                            predicates: Vec::new(),
                            target_batch_rows: NATIVE_TARGET_BATCH_ROWS,
                            target_batch_bytes: NATIVE_TARGET_BATCH_BYTES,
                            memory: Arc::clone(&memory),
                            cancellation: cancellation.clone(),
                        })
                        .await?;
                    while let Some(batch) = decoded.try_next().await? {
                        sender.send(batch.into_batch()?).await?;
                    }
                }
                return Ok(());
            }

            let units = Arc::new(units);
            let unit_count = units.len();
            let opener_session = Arc::clone(&session);
            let opener_units = Arc::clone(&units);
            let opener_execution = unit_execution.clone();
            let opener_memory = Arc::clone(&memory);
            let opener_options = options.clone();
            let opener_schema = decode_schema.clone();
            let opener_position = source_position.clone();
            let opener_scope_prefix = unit_scope_prefix.clone();
            let opener: CanonicalStreamOpener<Batch> = Box::new(move |ordinal| {
                let unit = opener_units.get(ordinal).cloned().ok_or_else(|| {
                    CdfError::internal("decode-unit frontier ordinal is outside its session")
                })?;
                let session = Arc::clone(&opener_session);
                let memory = Arc::clone(&opener_memory);
                let options = opener_options.clone();
                let schema = opener_schema.clone();
                let source_position = opener_position.clone();
                let unit_stream = opener_execution.spawn_io_stream(
                    &format!("{opener_scope_prefix}-unit-{ordinal:08}"),
                    NATIVE_UNIT_STREAM_ITEMS,
                    move |mut unit_sender, unit_cancellation| async move {
                        let mut decoded = session
                            .decode(PhysicalDecodeRequest {
                                unit,
                                resource_id: options.resource_id,
                                partition_id: options.partition_id,
                                batch_id_prefix: options.batch_id_prefix,
                                schema,
                                source_position,
                                projection: None,
                                predicates: Vec::new(),
                                target_batch_rows: NATIVE_TARGET_BATCH_ROWS,
                                target_batch_bytes: NATIVE_TARGET_BATCH_BYTES,
                                memory,
                                cancellation: unit_cancellation,
                            })
                            .await?;
                        while let Some(batch) = decoded.try_next().await? {
                            unit_sender.send(batch.into_batch()?).await?;
                        }
                        Ok(())
                    },
                )?;
                Ok(Box::pin(unit_stream))
            });
            let mut decoded = canonical_stream_frontier(unit_count, unit_jobs, opener)?;
            while let Some(batch) = decoded.try_next().await? {
                cancellation.check()?;
                sender.send(batch).await?;
            }
            Ok(())
        },
    )?;
    Ok(Box::pin(stream))
}

fn validate_partition(
    descriptor: &ResourceDescriptor,
    plan: &FileResourcePlan,
    partition: &PartitionPlan,
    transport: &dyn FileTransport,
    formats: &FormatRegistry,
    transforms: &ByteTransformRegistry,
) -> Result<ResolvedFileMatch> {
    if partition.metadata.get("kind").map(String::as_str) != Some("files") {
        return Err(CdfError::contract(format!(
            "declarative file resource `{}` expected a file partition plan",
            descriptor.resource_id
        )));
    }
    if partition.metadata.get("resource_id").map(String::as_str)
        != Some(descriptor.resource_id.as_str())
    {
        return Err(CdfError::contract(format!(
            "declarative file partition resource id does not match `{}`",
            descriptor.resource_id
        )));
    }
    if partition.metadata.get("glob").map(String::as_str) != Some(plan.glob.as_str()) {
        return Err(CdfError::contract(format!(
            "declarative file partition glob does not match `{}`",
            plan.glob
        )));
    }
    let path = partition.metadata.get("path").ok_or_else(|| {
        CdfError::contract(format!(
            "declarative file resource `{}` expected file partition path metadata",
            descriptor.resource_id
        ))
    })?;
    let expected_scope = ScopeKey::File { path: path.clone() };
    if partition.scope != expected_scope {
        return Err(CdfError::contract(format!(
            "declarative file partition scope does not match file path `{path}`",
        )));
    }
    let matches = resolve_file_matches(
        &descriptor.resource_id,
        plan,
        transport,
        formats,
        transforms,
    )?;
    let match_count = matches.len();
    let Some(resolved) = matches.into_iter().find(|file| file.path_text == *path) else {
        return Err(CdfError::contract(format!(
            "declarative file partition path `{path}` was not produced by glob `{}` under `{}`",
            plan.glob, plan.root
        )));
    };
    let expected_partition_id = if match_count == 1 {
        "files".to_owned()
    } else {
        file_partition_id(&resolved.path_text)
    };
    if partition.partition_id.as_str() != expected_partition_id.as_str() {
        return Err(CdfError::contract(format!(
            "declarative file partition id `{}` does not match file path `{path}`",
            partition.partition_id
        )));
    }
    let expected_size = resolved.size_bytes.to_string();
    if partition.metadata.get("bytes").map(String::as_str) != Some(expected_size.as_str()) {
        return Err(CdfError::data(format!(
            "declarative file partition `{path}` changed size after planning"
        )));
    }
    match (
        &resolved.sha256,
        &resolved.etag,
        &resolved.version,
        &resolved.source_generation,
    ) {
        (Some(sha256), _, _, _) => {
            if partition.metadata.get("sha256").map(String::as_str) != Some(sha256.as_str()) {
                return Err(CdfError::data(format!(
                    "declarative file partition `{path}` changed checksum after planning"
                )));
            }
        }
        (None, Some(etag), _, _) => {
            if partition.metadata.get("etag").map(String::as_str) != Some(etag.as_str()) {
                return Err(CdfError::data(format!(
                    "declarative file partition `{path}` changed ETag after planning"
                )));
            }
        }
        (None, None, Some(version), _) => {
            if partition.metadata.get("version").map(String::as_str) != Some(version.as_str()) {
                return Err(CdfError::data(format!(
                    "declarative file partition `{path}` changed object version after planning"
                )));
            }
        }
        (None, None, None, Some(source_generation)) => {
            if partition
                .metadata
                .get("source_generation")
                .map(String::as_str)
                != Some(source_generation.as_str())
            {
                return Err(CdfError::data(format!(
                    "declarative file partition `{path}` changed source generation after planning"
                )));
            }
        }
        (None, None, None, None) => {
            if resolved.identity_strength != GenerationStrength::Weak {
                return Err(CdfError::internal(format!(
                    "declarative file partition `{path}` omitted generation evidence despite non-weak identity"
                )));
            }
            for forbidden in ["sha256", "etag", "version", "source_generation"] {
                if partition.metadata.contains_key(forbidden) {
                    return Err(CdfError::data(format!(
                        "declarative file partition `{path}` retained stale `{forbidden}` identity metadata"
                    )));
                }
            }
        }
    }
    validate_partition_metadata_value(
        partition,
        "identity_strength",
        identity_strength_name(resolved.identity_strength),
        path,
    )?;
    validate_compression_metadata(partition, &resolved, &plan.compression, path)?;
    validate_partition_metadata_value(partition, "format", &resolved.format.format_id, path)?;
    validate_partition_metadata_value(
        partition,
        "format_driver_version",
        &resolved.format.driver_version,
        path,
    )?;
    validate_partition_metadata_value(
        partition,
        "format_declared",
        if plan.format_declared {
            "true"
        } else {
            "false"
        },
        path,
    )?;
    validate_partition_metadata_value(
        partition,
        "format_extension",
        resolved.format.extension.as_deref().unwrap_or("none"),
        path,
    )?;
    validate_partition_metadata_value(
        partition,
        "format_detection",
        format_detection_confidence_name(resolved.format.detection.confidence),
        path,
    )?;
    validate_partition_metadata_value(
        partition,
        "format_detection_reason",
        &resolved.format.detection.reason,
        path,
    )?;
    Ok(resolved)
}

fn validate_compression_metadata(
    partition: &PartitionPlan,
    resolved: &ResolvedFileMatch,
    declared: &FileCompressionDeclaration,
    path: &str,
) -> Result<()> {
    let expects_metadata = records_compression_metadata(resolved, declared);
    if expects_metadata {
        validate_partition_metadata_value(
            partition,
            "compression",
            resolved.compression.mode_name(),
            path,
        )?;
        validate_partition_metadata_value(
            partition,
            "compression_declared",
            declared.as_str(),
            path,
        )?;
        validate_partition_metadata_value(
            partition,
            "compression_extension",
            resolved.compression.extension_signal.as_str(),
            path,
        )?;
        validate_partition_metadata_value(
            partition,
            "compression_magic",
            resolved.compression.magic_signal.as_str(),
            path,
        )?;
        return Ok(());
    }

    if partition.metadata.contains_key("compression") {
        validate_partition_metadata_value(
            partition,
            "compression",
            resolved.compression.mode_name(),
            path,
        )?;
    }
    Ok(())
}

fn validate_partition_metadata_value(
    partition: &PartitionPlan,
    key: &str,
    expected: &str,
    path: &str,
) -> Result<()> {
    if partition.metadata.get(key).map(String::as_str) != Some(expected) {
        return Err(CdfError::data(format!(
            "declarative file partition `{path}` changed {key} after planning"
        )));
    }
    Ok(())
}

fn partition_for_file_match(
    descriptor: &ResourceDescriptor,
    plan: &FileResourcePlan,
    file: &ResolvedFileMatch,
    total_matches: usize,
) -> Result<PartitionPlan> {
    let mut metadata = BTreeMap::new();
    metadata.insert("kind".to_owned(), "files".to_owned());
    metadata.insert("glob".to_owned(), plan.glob.clone());
    metadata.insert("resource_id".to_owned(), descriptor.resource_id.to_string());
    metadata.insert("path".to_owned(), file.path_text.clone());
    metadata.insert(
        PLAN_SCHEMA_OBSERVATION_ID_KEY.to_owned(),
        file.path_text.clone(),
    );
    metadata.insert(
        PLAN_SCHEMA_OBSERVATION_BINDING_KEY.to_owned(),
        file_schema_observation_binding(file),
    );
    metadata.insert("bytes".to_owned(), file.size_bytes.to_string());
    metadata.insert(
        "identity_strength".to_owned(),
        identity_strength_name(file.identity_strength).to_owned(),
    );
    if let Some(source_generation) = &file.source_generation {
        metadata.insert("source_generation".to_owned(), source_generation.clone());
    }
    if let Some(sha256) = &file.sha256 {
        metadata.insert("sha256".to_owned(), sha256.clone());
    }
    if let Some(etag) = &file.etag {
        metadata.insert("etag".to_owned(), etag.clone());
    }
    if let Some(version) = &file.version {
        metadata.insert("version".to_owned(), version.clone());
    }
    if let Some(modified_ms) = &file.modified_ms {
        metadata.insert("modified_ms".to_owned(), modified_ms.clone());
    }
    if let Some(bytes_loaded) = file.bytes_loaded {
        metadata.insert("bytes_loaded".to_owned(), bytes_loaded.to_string());
    }
    metadata.insert("format".to_owned(), file.format.format_id.clone());
    metadata.insert(
        "format_driver_version".to_owned(),
        file.format.driver_version.clone(),
    );
    metadata.insert(
        "format_declared".to_owned(),
        plan.format_declared.to_string(),
    );
    metadata.insert(
        "format_extension".to_owned(),
        file.format
            .extension
            .clone()
            .unwrap_or_else(|| "none".to_owned()),
    );
    metadata.insert(
        "format_detection".to_owned(),
        format_detection_confidence_name(file.format.detection.confidence).to_owned(),
    );
    metadata.insert(
        "format_detection_reason".to_owned(),
        file.format.detection.reason.clone(),
    );
    if records_compression_metadata(file, &plan.compression) {
        metadata.insert(
            "compression".to_owned(),
            file.compression.mode_name().to_owned(),
        );
        metadata.insert(
            "compression_declared".to_owned(),
            plan.compression.as_str().to_owned(),
        );
        metadata.insert(
            "compression_extension".to_owned(),
            file.compression.extension_signal.as_str().to_owned(),
        );
        metadata.insert(
            "compression_magic".to_owned(),
            file.compression.magic_signal.as_str().to_owned(),
        );
    }

    let partition_id = if total_matches == 1 {
        "files".to_owned()
    } else {
        file_partition_id(&file.path_text)
    };

    Ok(PartitionPlan {
        partition_id: PartitionId::new(partition_id)?,
        scope: ScopeKey::File {
            path: file.path_text.clone(),
        },
        start_position: None,
        metadata,
    })
}

fn file_schema_observation_binding(file: &ResolvedFileMatch) -> String {
    let mut hasher = Sha256::new();
    let size = file.size_bytes.to_string();
    for value in [
        file.path_text.as_str(),
        size.as_str(),
        file.source_generation.as_deref().unwrap_or_default(),
        identity_strength_name(file.identity_strength),
        file.etag.as_deref().unwrap_or_default(),
        file.version.as_deref().unwrap_or_default(),
        file.sha256.as_deref().unwrap_or_default(),
        file.modified_ms.as_deref().unwrap_or_default(),
    ] {
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(value.as_bytes());
    }
    format!("sha256:{:x}", hasher.finalize())
}

fn resolve_file_matches(
    resource_id: &ResourceId,
    plan: &FileResourcePlan,
    transport: &dyn FileTransport,
    formats: &FormatRegistry,
    transforms: &ByteTransformRegistry,
) -> Result<Vec<ResolvedFileMatch>> {
    if is_http_root(&plan.root) {
        return resolve_http_file_match(resource_id, plan, transport, formats, transforms);
    }
    if is_object_store_root(&plan.root) {
        return resolve_object_store_matches(resource_id, plan, transport, formats, transforms);
    }

    let root = PathBuf::from(&plan.root);
    if !root.is_absolute() {
        return Err(CdfError::contract(format!(
            "file source root `{}` for resource `{resource_id}` must be absolute before runtime open; compile with an explicit project root or declare an absolute file source root",
            plan.root
        )));
    }

    let components = pattern_components(&plan.glob)?;
    let mut matches = Vec::new();
    collect_matches(&root, &components, &mut matches)?;
    matches.sort();
    matches.dedup();

    let matches = contained_matches(&root, matches)?;
    matches
        .into_iter()
        .map(|path| resolved_file_match(resource_id, &root, path, plan, formats, transforms))
        .collect()
}

fn resolve_object_store_matches(
    resource_id: &ResourceId,
    plan: &FileResourcePlan,
    transport: &dyn FileTransport,
    formats: &FormatRegistry,
    transforms: &ByteTransformRegistry,
) -> Result<Vec<ResolvedFileMatch>> {
    let root_resource = FileTransportResource::object_store_url(plan.root.clone())
        .with_egress_allowlist(plan.allowlist.clone());
    let root_resource = match &plan.credentials {
        Some(credentials) => root_resource.with_credentials(credentials.clone()),
        None => root_resource,
    };
    let components = pattern_components(&plan.glob)?;
    let mut matches = Vec::new();
    for metadata in transport.list(&root_resource)? {
        let relative = object_store_relative_path(&plan.root, &metadata.location)?;
        if !glob_path_matches(&components, &relative) {
            continue;
        }
        let resource = FileTransportResource::object_store_url(metadata.location.clone())
            .with_egress_allowlist(plan.allowlist.clone());
        let resource = match &plan.credentials {
            Some(credentials) => resource.with_credentials(credentials.clone()),
            None => resource,
        };
        let compression = resolve_transport_compression(plan, &metadata.location, transforms)?;
        let format =
            resolve_transport_format(resource_id, plan, &metadata.location, &compression, formats)?;
        matches.push(resolved_transport_file_match(
            resource,
            metadata,
            compression,
            format,
        )?);
    }
    matches.sort_by(|left, right| left.path_text.cmp(&right.path_text));
    Ok(matches)
}

fn object_store_relative_path(root: &str, location: &str) -> Result<String> {
    let prefix = format!("{}/", root.trim_end_matches('/'));
    location
        .strip_prefix(&prefix)
        .map(str::to_owned)
        .ok_or_else(|| CdfError::data("object-store listing escaped its configured root prefix"))
}

fn resolve_http_file_match(
    resource_id: &ResourceId,
    plan: &FileResourcePlan,
    transport: &dyn FileTransport,
    formats: &FormatRegistry,
    transforms: &ByteTransformRegistry,
) -> Result<Vec<ResolvedFileMatch>> {
    let globs = expand_http_glob(resource_id, &plan.glob)?;
    let mut matches = Vec::with_capacity(globs.len());
    for glob in globs {
        let url = join_http_root_and_glob(&plan.root, &glob);
        let resource = FileTransportResource {
            location: FileTransportLocation::HttpUrl { url },
            egress_allowlist: plan.allowlist.clone(),
            auth: plan.auth.clone(),
            credentials: plan.credentials.clone(),
        };
        let Some(observation) = transport.metadata_if_exists(&resource)? else {
            continue;
        };
        let logical_location = match &resource.location {
            FileTransportLocation::HttpUrl { url } => url.as_str(),
            _ => unreachable!("HTTP resolver constructed an HTTP transport resource"),
        };
        let compression = resolve_transport_compression(plan, logical_location, transforms)?;
        let format =
            resolve_transport_format(resource_id, plan, logical_location, &compression, formats)?;
        let access_resource = observation.access_resource(&resource);
        matches.push(resolved_transport_file_match(
            access_resource,
            observation.into_identity(),
            compression,
            format,
        )?);
    }
    matches.sort_by(|left, right| left.path_text.cmp(&right.path_text));
    if matches.is_empty() {
        Err(no_file_matches_error(resource_id, plan))
    } else {
        Ok(matches)
    }
}

fn no_file_matches_error(resource_id: &ResourceId, plan: &FileResourcePlan) -> CdfError {
    CdfError::data(format!(
        "declarative file resource `{resource_id}` matched no files under `{}` for glob `{}`",
        plan.root, plan.glob
    ))
}

fn pattern_components(pattern: &str) -> Result<Vec<String>> {
    let path = Path::new(pattern);
    if path.is_absolute() {
        return Err(CdfError::contract(
            "file resource glob must be relative to its file source root",
        ));
    }

    let mut components = Vec::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(value) => {
                components.push(value.to_str().ok_or_else(|| {
                    CdfError::contract(format!("file resource glob is not valid UTF-8: {pattern}"))
                })?);
            }
            Component::ParentDir => {
                return Err(CdfError::contract(
                    "file resource glob must stay under its file source root and cannot contain `..`",
                ));
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(CdfError::contract(
                    "file resource glob must be relative to its file source root",
                ));
            }
        }
    }

    if components.is_empty() {
        return Err(CdfError::contract("file resource glob cannot be empty"));
    }

    Ok(components.into_iter().map(str::to_owned).collect())
}

fn collect_matches(
    current: &Path,
    components: &[String],
    matches: &mut Vec<PathBuf>,
) -> Result<()> {
    let Some((component, rest)) = components.split_first() else {
        return collect_leaf_match(current, matches);
    };

    if component == "**" {
        return collect_recursive_matches(current, components, rest, matches);
    }

    if has_wildcards(component) {
        return collect_wildcard_matches(current, component, rest, matches);
    }

    collect_literal_matches(current, component, rest, matches)
}

fn collect_leaf_match(current: &Path, matches: &mut Vec<PathBuf>) -> Result<()> {
    if current.is_file() {
        matches.push(current.to_path_buf());
    }
    Ok(())
}

fn collect_recursive_matches(
    current: &Path,
    components: &[String],
    rest: &[String],
    matches: &mut Vec<PathBuf>,
) -> Result<()> {
    collect_matches(current, rest, matches)?;
    for path in read_dir_paths(current)? {
        if is_physical_dir(&path)? {
            collect_matches(&path, components, matches)?;
        }
    }
    Ok(())
}

fn collect_wildcard_matches(
    current: &Path,
    component: &str,
    rest: &[String],
    matches: &mut Vec<PathBuf>,
) -> Result<()> {
    for path in read_dir_paths(current)? {
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if glob_component_matches(component, name) && can_descend_for_rest(&path, rest)? {
            collect_matches(&path, rest, matches)?;
        }
    }
    Ok(())
}

fn collect_literal_matches(
    current: &Path,
    component: &str,
    rest: &[String],
    matches: &mut Vec<PathBuf>,
) -> Result<()> {
    let next = current.join(component);
    if can_descend_for_rest(&next, rest)? {
        collect_matches(&next, rest, matches)
    } else {
        Ok(())
    }
}

fn can_descend_for_rest(path: &Path, rest: &[String]) -> Result<bool> {
    Ok(rest.is_empty() || is_physical_dir(path)?)
}

fn read_dir_paths(path: &Path) -> Result<Vec<PathBuf>> {
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
            ) =>
        {
            return Ok(Vec::new());
        }
        Err(error) => {
            return Err(CdfError::data(format!(
                "read file source directory {}: {error}",
                path.display()
            )));
        }
    };

    let mut paths = entries
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|error| {
            CdfError::data(format!(
                "read file source directory {}: {error}",
                path.display()
            ))
        })?;
    paths.sort();
    Ok(paths)
}

fn is_physical_dir(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(metadata.file_type().is_dir()),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(CdfError::data(format!(
            "inspect file source path {}: {error}",
            path.display()
        ))),
    }
}

fn contained_matches(root: &Path, matches: Vec<PathBuf>) -> Result<Vec<PathBuf>> {
    if matches.is_empty() {
        return Ok(matches);
    }

    let canonical_root = fs::canonicalize(root).map_err(|error| {
        CdfError::data(format!(
            "canonicalize file source root {}: {error}",
            root.display()
        ))
    })?;

    matches
        .into_iter()
        .map(|path| {
            let canonical_path = fs::canonicalize(&path).map_err(|error| {
                CdfError::data(format!(
                    "canonicalize matched file {}: {error}",
                    path.display()
                ))
            })?;
            if !canonical_path.starts_with(&canonical_root) {
                return Err(CdfError::contract(format!(
                    "matched file {} escapes declared file source root {}",
                    path.display(),
                    root.display()
                )));
            }
            Ok(canonical_path)
        })
        .collect()
}

fn resolved_file_match(
    resource_id: &ResourceId,
    root: &Path,
    path: PathBuf,
    plan: &FileResourcePlan,
    formats: &FormatRegistry,
    transforms: &ByteTransformRegistry,
) -> Result<ResolvedFileMatch> {
    let metadata = fs::metadata(&path).map_err(|error| {
        CdfError::data(format!("stat matched file {}: {error}", path.display()))
    })?;
    let modified_ms = metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis().to_string());
    let canonical_root = fs::canonicalize(root).map_err(|error| {
        CdfError::data(format!(
            "canonicalize file source root {}: {error}",
            root.display()
        ))
    })?;
    let relative_path = path.strip_prefix(&canonical_root).map_err(|error| {
        CdfError::internal(format!(
            "matched file {} did not remain relative to canonical root {}: {error}",
            path.display(),
            canonical_root.display()
        ))
    })?;
    let path_text = relative_path.to_str().map(str::to_owned).ok_or_else(|| {
        CdfError::data(format!(
            "matched file path is not valid UTF-8: {}",
            relative_path.display()
        ))
    })?;
    let path_text = path_text.replace(std::path::MAIN_SEPARATOR, "/");
    let compression = resolve_local_compression(&path_text, &plan.compression, transforms)?;
    let (format, _) = resolve_local_format(resource_id, plan, &path_text, &compression, formats)?;
    let source_generation = local_source_generation(&path)?;
    Ok(ResolvedFileMatch {
        open: ResolvedFileOpen::LocalPath(path),
        path_text,
        size_bytes: metadata.len(),
        source_generation: Some(source_generation),
        identity_strength: GenerationStrength::Weak,
        sha256: None,
        etag: None,
        version: None,
        modified_ms,
        bytes_loaded: None,
        compression,
        format,
    })
}

fn local_file_discovery_candidate(
    resource_id: &ResourceId,
    root: &Path,
    path: PathBuf,
    plan: &FileResourcePlan,
    formats: &FormatRegistry,
    transforms: &ByteTransformRegistry,
) -> Result<LocalFileDiscoveryCandidate> {
    let metadata = fs::metadata(&path).map_err(|error| {
        CdfError::data(format!("stat matched file {}: {error}", path.display()))
    })?;
    let canonical_root = fs::canonicalize(root).map_err(|error| {
        CdfError::data(format!(
            "canonicalize file source root {}: {error}",
            root.display()
        ))
    })?;
    let relative_path = path.strip_prefix(&canonical_root).map_err(|error| {
        CdfError::internal(format!(
            "matched file {} did not remain relative to canonical root {}: {error}",
            path.display(),
            canonical_root.display()
        ))
    })?;
    let relative_path = relative_path.to_str().map(str::to_owned).ok_or_else(|| {
        CdfError::data(format!(
            "matched file path is not valid UTF-8: {}",
            relative_path.display()
        ))
    })?;
    let relative_path = relative_path.replace(std::path::MAIN_SEPARATOR, "/");
    let compression = resolve_local_compression(&relative_path, &plan.compression, transforms)?;
    resolve_local_format(resource_id, plan, &relative_path, &compression, formats)?;
    Ok(LocalFileDiscoveryCandidate {
        path,
        relative_path,
        size_bytes: metadata.len(),
        compression: compression.mode_name().to_owned(),
        selection_bytes_read: 0,
    })
}

fn resolved_transport_file_match(
    resource: FileTransportResource,
    metadata: FileIdentityMetadata,
    compression: CompressionEvidence,
    format: FormatEvidence,
) -> Result<ResolvedFileMatch> {
    let size_bytes = metadata.size_bytes.ok_or_else(|| {
        CdfError::data(format!(
            "HTTP(S) file metadata for `{}` did not include Content-Length",
            metadata.location
        ))
    })?;
    let sha256 = metadata.sha256().map(str::to_owned);
    let identity_strength = metadata.generation_strength();
    let source_generation = (identity_strength == GenerationStrength::Weak)
        .then(|| metadata.modified.clone())
        .flatten();
    Ok(ResolvedFileMatch {
        open: ResolvedFileOpen::Transport(resource),
        path_text: metadata.location,
        size_bytes,
        source_generation,
        identity_strength,
        sha256,
        etag: metadata.etag,
        version: metadata.version,
        modified_ms: metadata
            .modified
            .as_deref()
            .and_then(|modified| modified.strip_prefix("unix_ms:"))
            .map(str::to_owned),
        bytes_loaded: None,
        compression,
        format,
    })
}

fn identity_strength_name(strength: GenerationStrength) -> &'static str {
    match strength {
        GenerationStrength::Weak => "weak",
        GenerationStrength::Strong => "strong",
        GenerationStrength::ContentAddressed => "content_addressed",
    }
}

fn resolve_local_format(
    resource_id: &ResourceId,
    plan: &FileResourcePlan,
    path_text: &str,
    compression: &CompressionEvidence,
    formats: &FormatRegistry,
) -> Result<(FormatEvidence, u64)> {
    let driver = formats.resolve(plan.resolved_format()?.as_str())?;
    let extension = format_extension(path_text, compression);
    validate_format_extension(resource_id, plan, path_text, extension.as_deref(), formats)?;
    Ok((deferred_format_evidence(driver.as_ref(), extension), 0))
}

fn resolve_transport_format(
    resource_id: &ResourceId,
    plan: &FileResourcePlan,
    location: &str,
    compression: &CompressionEvidence,
    formats: &FormatRegistry,
) -> Result<FormatEvidence> {
    let driver = formats.resolve(plan.resolved_format()?.as_str())?;
    let extension = format_extension(location, compression);
    let diagnostic = diagnostic_location(location);
    validate_format_extension(
        resource_id,
        plan,
        &diagnostic,
        extension.as_deref(),
        formats,
    )?;
    Ok(deferred_format_evidence(driver.as_ref(), extension))
}

fn validate_format_extension(
    resource_id: &ResourceId,
    plan: &FileResourcePlan,
    path_text: &str,
    extension: Option<&str>,
    formats: &FormatRegistry,
) -> Result<()> {
    if plan.format_declared {
        return Ok(());
    }
    let Some(extension) = extension else {
        return Err(CdfError::data(format!(
            "file `{path_text}` for resource `{resource_id}` has no extension that can attest inferred format `{}`; declare `format` explicitly",
            plan.resolved_format()?.as_str()
        )));
    };
    let Some(extension_driver) = formats.by_extension(extension) else {
        return Err(CdfError::data(format!(
            "file `{path_text}` for resource `{resource_id}` has unregistered extension `.{extension}` for inferred format `{}`; declare `format` explicitly or register the extension",
            plan.resolved_format()?.as_str()
        )));
    };
    if extension_driver.descriptor().format_id.as_str() == plan.resolved_format()?.as_str() {
        return Ok(());
    }
    Err(CdfError::data(format!(
        "file format mismatch for resource `{resource_id}`, file `{path_text}`: extension `.{extension}` selects `{}` but the compiled resource selects `{}`; change `format` or the file extension",
        extension_driver.descriptor().format_id,
        plan.resolved_format()?.as_str(),
    )))
}

fn deferred_format_evidence(
    driver: &dyn FormatDriver,
    extension: Option<String>,
) -> FormatEvidence {
    FormatEvidence {
        format_id: driver.descriptor().format_id.to_string(),
        driver_version: driver.descriptor().semantic_version.clone(),
        extension,
        detection: FormatDetection {
            confidence: FormatDetectionConfidence::None,
            reason: "content detection is deferred to the admitted decode stream".to_owned(),
        },
    }
}

fn format_detection_confidence_name(confidence: FormatDetectionConfidence) -> &'static str {
    match confidence {
        FormatDetectionConfidence::None => "none",
        FormatDetectionConfidence::Weak => "weak",
        FormatDetectionConfidence::Strong => "strong",
    }
}

fn format_extension(path_text: &str, compression: &CompressionEvidence) -> Option<String> {
    let path_without_fragment = path_text.split('#').next().unwrap_or(path_text);
    let mut path_without_query = path_without_fragment
        .split('?')
        .next()
        .unwrap_or(path_without_fragment)
        .to_ascii_lowercase();
    if compression.transform_id.is_some()
        && let Some((inner, _)) = path_without_query.rsplit_once('.')
    {
        path_without_query.truncate(inner.len());
    }
    path_without_query
        .rsplit_once('.')
        .map(|(_, extension)| extension.to_owned())
}

fn diagnostic_location(location: &str) -> String {
    let path = location.split('?').next().unwrap_or(location);
    if path == location {
        path.to_owned()
    } else {
        format!("{path}?<redacted>")
    }
}

fn resolve_local_compression(
    path_text: &str,
    declared: &FileCompressionDeclaration,
    transforms: &ByteTransformRegistry,
) -> Result<CompressionEvidence> {
    let extension_signal = compression_extension_signal(path_text, transforms);
    resolve_compression_signals(
        path_text,
        declared,
        extension_signal,
        CompressionSignal::default(),
        transforms,
    )
}

fn resolve_transport_compression(
    plan: &FileResourcePlan,
    location: &str,
    transforms: &ByteTransformRegistry,
) -> Result<CompressionEvidence> {
    let extension_signal = compression_extension_signal(location, transforms);
    resolve_compression_signals(
        &diagnostic_location(location),
        &plan.compression,
        extension_signal,
        CompressionSignal::default(),
        transforms,
    )
}

fn resolve_compression_signals(
    path_text: &str,
    declared: &FileCompressionDeclaration,
    extension_signal: CompressionSignal,
    magic_signal: CompressionSignal,
    transforms: &ByteTransformRegistry,
) -> Result<CompressionEvidence> {
    let transform_id = if declared.is_auto() {
        extension_signal.transform_id().cloned()
    } else if declared.is_none() {
        if magic_signal.transform_id().is_some() {
            return Err(compression_signal_error(
                path_text,
                declared,
                &extension_signal,
                &magic_signal,
            ));
        }
        None
    } else {
        let declared_id = ByteTransformId::new(declared.as_str().to_owned())?;
        transforms.resolve(&declared_id)?;
        if magic_signal
            .transform_id()
            .is_some_and(|magic| magic != &declared_id)
        {
            return Err(compression_signal_error(
                path_text,
                declared,
                &extension_signal,
                &magic_signal,
            ));
        }
        Some(declared_id)
    };

    Ok(CompressionEvidence {
        transform_id,
        extension_signal,
        magic_signal,
    })
}

fn compression_extension_signal(
    path_text: &str,
    transforms: &ByteTransformRegistry,
) -> CompressionSignal {
    let path_without_fragment = path_text.split('#').next().unwrap_or(path_text);
    let lower = path_without_fragment
        .split('?')
        .next()
        .unwrap_or(path_without_fragment)
        .to_ascii_lowercase();
    let extension = lower.rsplit_once('.').map(|(_, extension)| extension);
    CompressionSignal(extension.and_then(|extension| {
        transforms
            .by_extension(extension)
            .map(|driver| driver.descriptor().transform_id.clone())
    }))
}

fn compression_signal_error(
    path_text: &str,
    declared: &FileCompressionDeclaration,
    extension_signal: &CompressionSignal,
    magic_signal: &CompressionSignal,
) -> CdfError {
    CdfError::data(format!(
        "file `{path_text}` compression mismatch: declared `{}`, extension signal `{}`, magic bytes signal `{}`",
        declared.as_str(),
        extension_signal.as_str(),
        magic_signal.as_str()
    ))
}

fn records_compression_metadata(
    file: &ResolvedFileMatch,
    declared: &FileCompressionDeclaration,
) -> bool {
    file.compression.transform_id.is_some()
        || !declared.is_auto()
        || file.compression.extension_signal.transform_id().is_some()
        || file.compression.magic_signal.transform_id().is_some()
}

fn expand_http_glob(resource_id: &ResourceId, glob: &str) -> Result<Vec<String>> {
    if let Some(months) = expand_http_year_month_glob(glob) {
        return Ok(months);
    }
    let components = pattern_components(glob)?;
    if components
        .iter()
        .any(|component| component == "**" || has_wildcards(component))
    {
        return Err(CdfError::contract(format!(
            "HTTP(S) file resource `{resource_id}` cannot enumerate unbounded glob `{glob}` because HTTP has no LIST operation; use an explicit file or a finite numeric template such as `{{01..12}}`"
        )));
    }
    let Some(open) = glob.find('{') else {
        if glob.contains('}') {
            return Err(CdfError::contract(format!(
                "HTTP(S) file resource `{resource_id}` has an unmatched `}}` in glob `{glob}`"
            )));
        }
        return Ok(vec![glob.to_owned()]);
    };
    let close = glob[open + 1..]
        .find('}')
        .map(|offset| open + 1 + offset)
        .ok_or_else(|| {
            CdfError::contract(format!(
                "HTTP(S) file resource `{resource_id}` has an unmatched `{{` in glob `{glob}`"
            ))
        })?;
    if glob[close + 1..].contains('{')
        || glob[close + 1..].contains('}')
        || glob[..open].contains('}')
    {
        return Err(CdfError::contract(format!(
            "HTTP(S) file resource `{resource_id}` supports one numeric range template per glob; got `{glob}`"
        )));
    }
    let range = &glob[open + 1..close];
    let (start_text, end_text) = range.split_once("..").ok_or_else(|| {
        CdfError::contract(format!(
            "HTTP(S) file resource `{resource_id}` template `{{{range}}}` must be an inclusive numeric range such as `{{01..12}}`"
        ))
    })?;
    if start_text.is_empty()
        || end_text.is_empty()
        || !start_text.bytes().all(|byte| byte.is_ascii_digit())
        || !end_text.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(CdfError::contract(format!(
            "HTTP(S) file resource `{resource_id}` template `{{{range}}}` must contain decimal integers"
        )));
    }
    let start = start_text.parse::<u64>().map_err(|error| {
        CdfError::contract(format!(
            "invalid HTTP template start `{start_text}`: {error}"
        ))
    })?;
    let end = end_text.parse::<u64>().map_err(|error| {
        CdfError::contract(format!("invalid HTTP template end `{end_text}`: {error}"))
    })?;
    if start > end {
        return Err(CdfError::contract(format!(
            "HTTP(S) file resource `{resource_id}` template start {start} exceeds end {end}"
        )));
    }
    let count = end - start + 1;
    if count > 1_000_000 {
        return Err(CdfError::contract(format!(
            "HTTP(S) file resource `{resource_id}` template expands to {count} files; split it into ranges of at most 1000000 files"
        )));
    }
    let width = if start_text.starts_with('0') || end_text.starts_with('0') {
        start_text.len().max(end_text.len())
    } else {
        0
    };
    let mut expanded = Vec::with_capacity(count as usize);
    for value in start..=end {
        let value = if width == 0 {
            value.to_string()
        } else {
            format!("{value:0width$}")
        };
        expanded.push(format!("{}{}{}", &glob[..open], value, &glob[close + 1..]));
    }
    Ok(expanded)
}

fn expand_http_year_month_glob(glob: &str) -> Option<Vec<String>> {
    if glob.matches('*').count() != 1
        || glob.contains("**")
        || glob.contains('?')
        || glob.contains('[')
        || glob.contains(']')
    {
        return None;
    }
    let star = glob.find('*')?;
    let prefix = &glob[..star];
    let year = prefix.strip_suffix('-')?.rsplit(['/', '_', '-']).next()?;
    if year.len() != 4 || !year.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some(
        (1..=12)
            .map(|month| format!("{}{:02}{}", prefix, month, &glob[star + 1..]))
            .collect(),
    )
}

fn join_http_root_and_glob(root: &str, glob: &str) -> String {
    format!(
        "{}/{}",
        root.trim_end_matches('/'),
        glob.trim_start_matches('/')
    )
}

fn is_http_root(root: &str) -> bool {
    root.starts_with("http://") || root.starts_with("https://")
}

fn is_object_store_root(root: &str) -> bool {
    root.starts_with("s3://") || root.starts_with("gs://") || root.starts_with("az://")
}

fn file_partition_id(path: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(path.as_bytes());
    let digest = hex::encode(hasher.finalize());
    format!("file-{}", &digest[..16])
}

fn has_wildcards(pattern: &str) -> bool {
    pattern.contains('*') || pattern.contains('?')
}

fn glob_component_matches(pattern: &str, candidate: &str) -> bool {
    let pattern = pattern.as_bytes();
    let candidate = candidate.as_bytes();
    let mut table = vec![vec![false; candidate.len() + 1]; pattern.len() + 1];
    table[0][0] = true;

    for i in 1..=pattern.len() {
        if pattern[i - 1] == b'*' {
            table[i][0] = table[i - 1][0];
        }
    }

    for i in 1..=pattern.len() {
        for j in 1..=candidate.len() {
            table[i][j] = match pattern[i - 1] {
                b'*' => table[i - 1][j] || table[i][j - 1],
                b'?' => table[i - 1][j - 1],
                byte => byte == candidate[j - 1] && table[i - 1][j - 1],
            };
        }
    }

    table[pattern.len()][candidate.len()]
}

fn glob_path_matches(pattern: &[String], candidate: &str) -> bool {
    let candidate = candidate
        .split('/')
        .filter(|component| !component.is_empty())
        .collect::<Vec<_>>();
    let mut table = vec![vec![false; candidate.len() + 1]; pattern.len() + 1];
    table[0][0] = true;
    for pattern_index in 1..=pattern.len() {
        if pattern[pattern_index - 1] == "**" {
            table[pattern_index][0] = table[pattern_index - 1][0];
        }
        for candidate_index in 1..=candidate.len() {
            table[pattern_index][candidate_index] = if pattern[pattern_index - 1] == "**" {
                table[pattern_index - 1][candidate_index]
                    || table[pattern_index][candidate_index - 1]
            } else {
                table[pattern_index - 1][candidate_index - 1]
                    && glob_component_matches(
                        &pattern[pattern_index - 1],
                        candidate[candidate_index - 1],
                    )
            };
        }
    }
    table[pattern.len()][candidate.len()]
}

#[cfg(test)]
mod tests {
    use std::{
        io::Write,
        sync::{
            Arc, Barrier,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_ipc::writer::FileWriter;
    use arrow_schema::{DataType, Field, Schema};
    use flate2::{Compression, write::GzEncoder};
    use object_store::{ObjectStoreExt, PutPayload, memory::InMemory, path::Path as ObjectPath};
    use parquet::arrow::ArrowWriter;
    use tempfile::TempDir;

    use super::*;

    #[derive(Debug)]
    struct ExternalMockFormat {
        descriptor: cdf_runtime::FormatDriverDescriptor,
    }

    impl ExternalMockFormat {
        fn new() -> Self {
            Self {
                descriptor: cdf_runtime::FormatDriverDescriptor {
                    format_id: cdf_runtime::FormatId::new("external_mock").unwrap(),
                    semantic_version: "1.0.0".to_owned(),
                    aliases: Vec::new(),
                    extensions: vec!["mock".to_owned()],
                    mime_types: Vec::new(),
                    magic: Vec::new(),
                    detection_probe: cdf_runtime::FormatDetectionProbe {
                        prefix_bytes: 4,
                        suffix_bytes: 0,
                    },
                    option_schema: serde_json::json!({
                        "type": "object",
                        "additionalProperties": false
                    }),
                    projection_pushdown: cdf_kernel::PushdownFidelity::Unsupported,
                    predicate_pushdown: cdf_kernel::PushdownFidelity::Unsupported,
                    source_access: cdf_runtime::FormatSourceAccess::Sequential,
                    discovery_kind: cdf_runtime::FormatDiscoveryKind::BoundedContent,
                    decode_unit_policy: "whole_mock_file".to_owned(),
                    error_isolation: cdf_runtime::FormatErrorIsolation::DecodeUnit,
                    minimum_working_set_bytes: 64,
                    maximum_working_set_bytes: 1024 * 1024,
                },
            }
        }

        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )]))
        }
    }

    impl cdf_runtime::FormatDriver for ExternalMockFormat {
        fn descriptor(&self) -> &cdf_runtime::FormatDriverDescriptor {
            &self.descriptor
        }

        fn canonical_options(&self, options: serde_json::Value) -> Result<serde_json::Value> {
            if options.as_object().is_some_and(serde_json::Map::is_empty) {
                Ok(options)
            } else {
                Err(CdfError::contract("external mock options must be empty"))
            }
        }

        fn detect(&self, probe: &cdf_runtime::FormatProbe) -> Result<cdf_runtime::FormatDetection> {
            Ok(cdf_runtime::FormatDetection {
                confidence: if probe.prefix.starts_with(b"MOCK") {
                    cdf_runtime::FormatDetectionConfidence::Strong
                } else {
                    cdf_runtime::FormatDetectionConfidence::None
                },
                reason: "external mock framing".to_owned(),
            })
        }

        fn discover(
            &self,
            source: Arc<dyn cdf_runtime::ByteSource>,
            request: cdf_runtime::FormatDiscoveryRequest,
        ) -> cdf_kernel::BoxFuture<'_, Result<cdf_runtime::PhysicalSchemaObservation>> {
            Box::pin(async move {
                request.cancellation.check()?;
                let preferred_chunk_bytes = (8 * 1024_u64).clamp(
                    source.capabilities().minimum_chunk_bytes,
                    source.capabilities().maximum_chunk_bytes,
                );
                let input = source
                    .open_sequential(cdf_runtime::SequentialReadRequest {
                        preferred_chunk_bytes,
                        cancellation: request.cancellation,
                    })
                    .await?;
                let mut cursor = cdf_runtime::AccountedByteCursor::new(input);
                if cursor.read_exact(4, "external mock magic").await? != b"MOCK" {
                    return Err(CdfError::data("external mock magic mismatch"));
                }
                let schema = Self::schema();
                Ok(cdf_runtime::PhysicalSchemaObservation {
                    identity: source.identity().clone(),
                    arrow_schema: schema,
                    sampled_bytes: 4,
                    sampled_records: 0,
                    evidence: BTreeMap::new(),
                })
            })
        }

        fn prepare_decode(
            &self,
            source: Arc<dyn cdf_runtime::ByteSource>,
            request: cdf_runtime::DecodePlanningRequest,
        ) -> cdf_kernel::BoxFuture<'_, Result<Arc<dyn cdf_runtime::FormatDecodeSession>>> {
            Box::pin(async move {
                request.cancellation.check()?;
                let units = vec![cdf_runtime::DecodeUnitPlan {
                    unit_id: "mock-file".to_owned(),
                    ordinal: 0,
                    extent: None,
                    estimated_working_set_bytes: 64,
                    independently_retryable: true,
                }];
                Ok(Arc::new(ExternalMockDecodeSession { source, units })
                    as Arc<dyn cdf_runtime::FormatDecodeSession>)
            })
        }
    }

    struct ExternalMockDecodeSession {
        source: Arc<dyn cdf_runtime::ByteSource>,
        units: Vec<cdf_runtime::DecodeUnitPlan>,
    }

    impl cdf_runtime::FormatDecodeSession for ExternalMockDecodeSession {
        fn units(&self) -> &[cdf_runtime::DecodeUnitPlan] {
            &self.units
        }

        fn decode(
            &self,
            request: cdf_runtime::PhysicalDecodeRequest,
        ) -> cdf_kernel::BoxFuture<'_, Result<cdf_runtime::PhysicalDecodeStream>> {
            Box::pin(async move {
                request.cancellation.check()?;
                self.validate_unit(&request.unit)?;
                let preferred_chunk_bytes = (8 * 1024_u64).clamp(
                    self.source.capabilities().minimum_chunk_bytes,
                    self.source.capabilities().maximum_chunk_bytes,
                );
                let input = self
                    .source
                    .open_sequential(cdf_runtime::SequentialReadRequest {
                        preferred_chunk_bytes,
                        cancellation: request.cancellation.clone(),
                    })
                    .await?;
                let mut cursor = cdf_runtime::AccountedByteCursor::new(input);
                if cursor.read_exact(5, "external mock payload").await? != b"MOCK\n" {
                    return Err(CdfError::data("external mock payload mismatch"));
                }
                let record_batch = RecordBatch::try_new(
                    ExternalMockFormat::schema(),
                    vec![Arc::new(Int64Array::from(vec![42]))],
                )
                .map_err(|error| CdfError::data(format!("external mock batch: {error}")))?;
                let lease = cdf_memory::reserve(
                    Arc::clone(&request.memory),
                    cdf_memory::ReservationRequest::new(
                        cdf_memory::ConsumerKey::new(
                            "external-mock-decode",
                            cdf_memory::MemoryClass::Decode,
                        )?,
                        1024,
                    )?,
                )
                .await?;
                let mut batch = cdf_kernel::Batch::from_record_batch(
                    cdf_kernel::BatchId::new("external-mock-batch")?,
                    request.resource_id,
                    request.partition_id,
                    cdf_kernel::canonical_arrow_schema_hash(
                        request.schema.decoder_schema.as_ref(),
                    )?,
                    record_batch,
                )?;
                batch.header.source_position = request.source_position;
                let physical = cdf_runtime::AccountedPhysicalBatch::new(batch, lease)?;
                Ok(
                    Box::pin(futures_util::stream::once(async move { Ok(physical) }))
                        as cdf_runtime::PhysicalDecodeStream,
                )
            })
        }
    }

    #[derive(Debug)]
    struct ExternalPassthroughTransform(cdf_runtime::ByteTransformDescriptor);

    impl ExternalPassthroughTransform {
        fn new() -> Self {
            Self(cdf_runtime::ByteTransformDescriptor {
                transform_id: cdf_runtime::ByteTransformId::new("external_passthrough").unwrap(),
                semantic_version: "1.0.0".to_owned(),
                extensions: vec!["mt".to_owned()],
                magic: Vec::new(),
                preserves_random_access: false,
                splittable: false,
                supports_concatenated_members: false,
                maximum_output_chunk_bytes: 1024 * 1024,
                maximum_working_set_bytes: 1024 * 1024,
                maximum_expanded_bytes: 1024 * 1024,
                maximum_expansion_ratio: 1,
                checksum: cdf_runtime::TransformChecksumBehavior::None,
            })
        }
    }

    impl cdf_runtime::ByteTransformDriver for ExternalPassthroughTransform {
        fn descriptor(&self) -> &cdf_runtime::ByteTransformDescriptor {
            &self.0
        }

        fn transform(
            &self,
            input: cdf_runtime::AccountedByteStream,
            request: cdf_runtime::ByteTransformRequest,
        ) -> Result<cdf_runtime::AccountedByteStream> {
            request.validate_for(&self.0)?;
            Ok(input)
        }
    }

    struct PayloadOpenCountingTransport {
        inner: FileTransportFacade,
        payload_opens: Arc<AtomicUsize>,
    }

    impl FileTransport for PayloadOpenCountingTransport {
        fn metadata(
            &self,
            resource: &FileTransportResource,
        ) -> Result<crate::FileMetadataObservation> {
            self.inner.metadata(resource)
        }

        fn metadata_if_exists(
            &self,
            resource: &FileTransportResource,
        ) -> Result<Option<crate::FileMetadataObservation>> {
            self.inner.metadata_if_exists(resource)
        }

        fn list(&self, resource: &FileTransportResource) -> Result<Vec<FileIdentityMetadata>> {
            self.inner.list(resource)
        }

        fn open_byte_source(
            &self,
            resource: &FileTransportResource,
            expected: &FileIdentityMetadata,
            memory: Arc<dyn cdf_memory::MemoryCoordinator>,
        ) -> Result<Arc<dyn cdf_runtime::ByteSource>> {
            self.payload_opens.fetch_add(1, Ordering::Relaxed);
            self.inner.open_byte_source(resource, expected, memory)
        }
    }

    #[test]
    fn external_format_and_transform_compose_without_runtime_dispatch_edits() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("events.mock.mt");
        std::fs::write(&path, b"MOCK\n").unwrap();
        let mut formats = cdf_runtime::FormatRegistry::default();
        formats
            .register(Arc::new(ExternalMockFormat::new()))
            .unwrap();
        let mut transforms = cdf_runtime::ByteTransformRegistry::default();
        transforms
            .register(Arc::new(ExternalPassthroughTransform::new()))
            .unwrap();
        let dependencies = FileRuntimeDependencies::new(
            FileTransportFacade::new(),
            crate::test_execution_services(),
            Arc::new(formats),
            Arc::new(transforms),
        );
        let plan = FileResourcePlan {
            source: "external".to_owned(),
            root: root.path().to_string_lossy().into_owned(),
            glob: "events.mock.mt".to_owned(),
            format: Some(FileFormatDeclaration::named("external_mock").unwrap()),
            format_declared: true,
            format_options: serde_json::json!({}),
            compression: FileCompressionDeclaration::auto(),
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let resource_id = ResourceId::new("external.events").unwrap();
        let resolved = dependencies
            .with_transport(|transport| {
                resolve_file_matches(
                    &resource_id,
                    &plan,
                    transport,
                    dependencies.formats(),
                    dependencies.transforms(),
                )
            })
            .unwrap();
        assert_eq!(resolved[0].compression.mode_name(), "external_passthrough");
        let probe = discover_local_binary_schema_bounded(
            &path,
            "events.mock.mt",
            &dependencies,
            0,
            BoundedSchemaDiscoveryRequest {
                resource_id: &resource_id,
                format: plan.resolved_format().unwrap(),
                format_declared: plan.format_declared,
                format_options: &plan.format_options,
                transform_name: "external_passthrough",
                maximum_bytes: 1024,
                maximum_records: 1_000,
            },
        )
        .unwrap();
        assert_eq!(probe.schema.as_ref(), ExternalMockFormat::schema().as_ref());
        let stream = stream_file_match_blocking(
            &resolved[0],
            plan.resolved_format().unwrap(),
            ReadOptions::new(resource_id, PartitionId::new("external-file").unwrap()),
            &dependencies,
            Arc::clone(&probe.schema),
            PhysicalSchemaAuthority::default(),
        )
        .unwrap();
        let batches = futures_executor::block_on(stream.collect::<Vec<_>>())
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].record_batch().unwrap().num_rows(), 1);
        assert!(matches!(
            batches[0].header.source_position,
            Some(SourcePosition::FileManifest(_))
        ));
        drop(batches);
        assert_eq!(
            dependencies.execution().memory().snapshot().current_bytes,
            0
        );
    }

    #[test]
    fn format_discovery_evidence_cannot_override_source_identity() {
        let mut identity = BTreeMap::from([("format".to_owned(), "parquet".to_owned())]);
        let error = merge_discovery_evidence(
            &mut identity,
            BTreeMap::from([("format".to_owned(), "forged".to_owned())]),
        )
        .unwrap_err();
        assert!(error.message.contains("conflicts with source identity"));
        assert_eq!(identity["format"], "parquet");
    }

    #[test]
    fn shared_transport_dependency_does_not_serialize_independent_io() {
        let dependencies = Arc::new(FileRuntimeDependencies::new(
            FileTransportFacade::new(),
            crate::test_execution_services(),
            crate::test_format_registry(),
            crate::test_transform_registry(),
        ));
        let start = Arc::new(Barrier::new(3));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let workers = (0..2)
            .map(|_| {
                let dependencies = Arc::clone(&dependencies);
                let start = Arc::clone(&start);
                let active = Arc::clone(&active);
                let peak = Arc::clone(&peak);
                std::thread::spawn(move || {
                    start.wait();
                    dependencies
                        .with_transport(|_| {
                            let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                            peak.fetch_max(current, Ordering::SeqCst);
                            std::thread::sleep(Duration::from_millis(50));
                            active.fetch_sub(1, Ordering::SeqCst);
                            Ok(())
                        })
                        .unwrap();
                })
            })
            .collect::<Vec<_>>();
        start.wait();
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(peak.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn local_parquet_uses_registered_native_driver_as_bounded_stream() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, Arc::clone(&schema), None).unwrap();
        for start in [0_i64, 50_000, 100_000] {
            let record_batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from_iter_values(start..start + 50_000)) as ArrayRef,
                    Arc::new(StringArray::from_iter_values(
                        (start..start + 50_000).map(|value| format!("name-{value}")),
                    )) as ArrayRef,
                ],
            )
            .unwrap();
            writer.write(&record_batch).unwrap();
            writer.flush().unwrap();
        }
        writer.close().unwrap();
        let temp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(temp.path(), bytes).unwrap();
        let dependencies = FileRuntimeDependencies::new(
            FileTransportFacade::new(),
            crate::test_execution_services(),
            crate::test_format_registry(),
            crate::test_transform_registry(),
        );
        let driver = dependencies.formats().resolve("parquet").unwrap();
        let stream = stream_registered_format(
            RegisteredFormatStreamRequest {
                source: Arc::new(
                    LocalByteSource::open(temp.path(), dependencies.execution().memory()).unwrap(),
                ),
                payload_retention: None,
                driver,
                options: ReadOptions::new(
                    ResourceId::new("events").unwrap(),
                    PartitionId::new("file-0").unwrap(),
                ),
                canonical_format_options: serde_json::json!({}),
                source_position: None,
                admission_schema: Arc::clone(&schema),
                physical_schema_authority: PhysicalSchemaAuthority::default(),
            },
            &dependencies,
        )
        .unwrap();

        let batches = futures_executor::block_on(stream.collect::<Vec<_>>())
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(batches.len(), 3);
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.header.row_count)
                .sum::<u64>(),
            150_000
        );
        assert!(
            batches
                .iter()
                .all(|batch| batch.header.row_count <= NATIVE_TARGET_BATCH_ROWS as u64)
        );
        assert!(dependencies.execution().memory().snapshot().current_bytes > 0);
        drop(batches);
        assert_eq!(
            dependencies.execution().memory().snapshot().current_bytes,
            0
        );
    }

    #[test]
    fn gzip_parquet_composes_transform_spool_with_registered_format_driver() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(0..10_000))],
        )
        .unwrap();
        let parquet = cdf_package::transcode_record_batches_to_parquet_bytes(&[batch]).unwrap();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&parquet).unwrap();
        let compressed = encoder.finish().unwrap();
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("events.parquet.gz"), compressed).unwrap();
        let dependencies = FileRuntimeDependencies::new(
            FileTransportFacade::new(),
            crate::test_execution_services(),
            crate::test_format_registry(),
            crate::test_transform_registry(),
        );
        let plan = FileResourcePlan {
            source: "events".to_owned(),
            root: root.path().to_string_lossy().into_owned(),
            glob: "events.parquet.gz".to_owned(),
            format: Some(FileFormatDeclaration::parquet()),
            format_declared: true,
            format_options: serde_json::json!({}),
            compression: FileCompressionDeclaration::auto(),
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let resource_id = ResourceId::new("events.raw").unwrap();
        let resolved = dependencies
            .with_transport(|transport| {
                resolve_file_matches(
                    &resource_id,
                    &plan,
                    transport,
                    dependencies.formats(),
                    dependencies.transforms(),
                )
            })
            .unwrap();
        assert_eq!(resolved[0].compression.mode_name(), "gzip");
        assert_eq!(resolved[0].format.extension.as_deref(), Some("parquet"));
        let probe = discover_local_binary_schema_bounded(
            root.path().join("events.parquet.gz"),
            "events.parquet.gz",
            &dependencies,
            0,
            BoundedSchemaDiscoveryRequest {
                resource_id: &resource_id,
                format: plan.resolved_format().unwrap(),
                format_declared: plan.format_declared,
                format_options: &plan.format_options,
                transform_name: "gzip",
                maximum_bytes: 64 * 1024 * 1024,
                maximum_records: 1_000,
            },
        )
        .unwrap();
        assert_eq!(probe.schema.as_ref(), schema.as_ref());
        assert_eq!(probe.source_identity.get("compression").unwrap(), "gzip");
        let stable_id = probe.source_identity.get("stable_id").unwrap();
        assert!(stable_id.ends_with("events.parquet.gz#transform:gzip"));
        assert_eq!(dependencies.prepared_payloads().pending_count().unwrap(), 1);
        std::fs::remove_file(root.path().join("events.parquet.gz")).unwrap();
        let stream = stream_file_match_blocking(
            &resolved[0],
            plan.resolved_format().unwrap(),
            ReadOptions::new(resource_id, PartitionId::new("file-events").unwrap()),
            &dependencies,
            Arc::clone(&probe.schema),
            PhysicalSchemaAuthority::default(),
        )
        .unwrap();
        assert_eq!(dependencies.prepared_payloads().pending_count().unwrap(), 0);
        let batches = futures_executor::block_on(stream.collect::<Vec<_>>())
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.header.row_count)
                .sum::<u64>(),
            10_000
        );
        drop(batches);
        assert_eq!(
            dependencies.execution().memory().snapshot().current_bytes,
            0
        );
    }

    #[test]
    fn remote_arrow_ipc_file_streams_directly_through_registered_driver() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let mut bytes = Vec::new();
        let mut writer = FileWriter::try_new(&mut bytes, schema.as_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        let store = Arc::new(InMemory::new());
        futures_executor::block_on(store.put(
            &ObjectPath::from("prod/events.arrow"),
            PutPayload::from(bytes.clone()),
        ))
        .unwrap();
        let facade = FileTransportFacade::new()
            .with_object_store("s3://ipc", store)
            .with_execution_services(crate::test_execution_services());
        let dependencies = FileRuntimeDependencies::new(
            facade,
            crate::test_execution_services(),
            crate::test_format_registry(),
            crate::test_transform_registry(),
        )
        .with_max_spool_bytes(1)
        .unwrap();
        let plan = FileResourcePlan {
            source: "ipc".to_owned(),
            root: "s3://ipc/prod".to_owned(),
            glob: "events.arrow".to_owned(),
            format: Some(FileFormatDeclaration::arrow_ipc()),
            format_declared: true,
            format_options: serde_json::json!({}),
            compression: FileCompressionDeclaration::none(),
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let resource_id = ResourceId::new("ipc.events").unwrap();
        let resolved = dependencies
            .with_transport(|transport| {
                resolve_object_store_matches(
                    &resource_id,
                    &plan,
                    transport,
                    crate::test_format_registry().as_ref(),
                    crate::test_transform_registry().as_ref(),
                )
            })
            .unwrap();
        let stream = stream_file_match_blocking(
            &resolved[0],
            plan.resolved_format().unwrap(),
            ReadOptions::new(resource_id, PartitionId::new("file-ipc").unwrap()),
            &dependencies,
            Arc::clone(&schema),
            PhysicalSchemaAuthority::default(),
        )
        .unwrap();
        let batches = futures_executor::block_on(stream.collect::<Vec<_>>())
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].header.row_count, 3);
    }

    #[test]
    fn local_open_rejects_planned_generation_mismatch_before_hashing() {
        let root = TempDir::new().unwrap();
        let path = root.path().join("events.ndjson");
        fs::write(&path, b"{\"id\":1}\n").unwrap();
        let plan = FileResourcePlan {
            source: "local".to_owned(),
            root: root.path().to_string_lossy().into_owned(),
            glob: "events.ndjson".to_owned(),
            format: Some(FileFormatDeclaration::ndjson()),
            format_declared: true,
            format_options: serde_json::json!({}),
            compression: FileCompressionDeclaration::none(),
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let dependencies = FileRuntimeDependencies::new(
            FileTransportFacade::new(),
            crate::test_execution_services(),
            crate::test_format_registry(),
            crate::test_transform_registry(),
        );
        let resource_id = ResourceId::new("local.events").unwrap();
        let mut resolved = resolved_file_match(
            &resource_id,
            root.path(),
            fs::canonicalize(&path).unwrap(),
            &plan,
            dependencies.formats(),
            dependencies.transforms(),
        )
        .unwrap();
        resolved.source_generation = Some("local-v1:stale-planned-generation".to_owned());
        let driver = dependencies.formats().resolve("ndjson").unwrap();
        let canonical_options = driver.canonical_options(serde_json::json!({})).unwrap();

        let error = prepare_file_input(
            &resource_id,
            &resolved,
            cdf_runtime::FormatSourceAccess::Sequential,
            driver.as_ref(),
            &canonical_options,
            &dependencies,
        )
        .err()
        .expect("stale local plan must fail before extraction hashing");

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
        assert!(error.message.contains("changed between planning and open"));
    }

    #[test]
    fn remote_parquet_uses_admitted_spool_or_generation_bound_ranges() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(0..100_000))],
        )
        .unwrap();
        let bytes = cdf_package::transcode_record_batches_to_parquet_bytes(&[batch]).unwrap();
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        futures_executor::block_on(store.put(
            &ObjectPath::from("prod/events.parquet"),
            PutPayload::from(bytes.clone()),
        ))
        .unwrap();
        let facade = FileTransportFacade::new()
            .with_object_store("s3://parquet", Arc::clone(&store))
            .with_execution_services(crate::test_execution_services());
        let dependencies = FileRuntimeDependencies::new(
            facade,
            crate::test_execution_services(),
            crate::test_format_registry(),
            crate::test_transform_registry(),
        )
        .with_max_spool_bytes(bytes.len() as u64)
        .unwrap();
        let plan = FileResourcePlan {
            source: "parquet".to_owned(),
            root: "s3://parquet/prod".to_owned(),
            glob: "events.parquet".to_owned(),
            format: Some(FileFormatDeclaration::parquet()),
            format_declared: true,
            format_options: serde_json::json!({}),
            compression: FileCompressionDeclaration::none(),
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let resource_id = ResourceId::new("parquet.events").unwrap();
        let resolved = dependencies
            .with_transport(|transport| {
                resolve_object_store_matches(
                    &resource_id,
                    &plan,
                    transport,
                    dependencies.formats(),
                    dependencies.transforms(),
                )
            })
            .unwrap();
        let driver = dependencies.formats().resolve("parquet").unwrap();
        let canonical_options = driver.canonical_options(serde_json::json!({})).unwrap();
        assert!(matches!(
            prepare_file_input(
                &resource_id,
                &resolved[0],
                cdf_runtime::FormatSourceAccess::Adaptive,
                driver.as_ref(),
                &canonical_options,
                &dependencies,
            )
            .unwrap()
            .input,
            PreparedFileInput::SpoolSource { .. }
        ));
        let stream = stream_file_match_blocking(
            &resolved[0],
            plan.resolved_format().unwrap(),
            ReadOptions::new(
                resource_id.clone(),
                PartitionId::new("file-parquet").unwrap(),
            ),
            &dependencies,
            Arc::clone(&schema),
            PhysicalSchemaAuthority::default(),
        )
        .unwrap();
        let batches = futures_executor::block_on(stream.collect::<Vec<_>>())
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.header.row_count)
                .sum::<u64>(),
            100_000
        );
        drop(batches);
        assert_eq!(
            dependencies.execution().memory().snapshot().current_bytes,
            0
        );
        let spill = dependencies.execution().spill().snapshot();
        assert!(spill.peak_bytes >= bytes.len() as u64);
        assert_eq!(spill.current_bytes, 0);

        let constrained = FileRuntimeDependencies::new(
            FileTransportFacade::new()
                .with_object_store("s3://parquet", Arc::clone(&store))
                .with_execution_services(crate::test_execution_services()),
            crate::test_execution_services(),
            crate::test_format_registry(),
            crate::test_transform_registry(),
        )
        .with_max_spool_bytes(1)
        .unwrap();
        let constrained_matches = constrained
            .with_transport(|transport| {
                resolve_object_store_matches(
                    &resource_id,
                    &plan,
                    transport,
                    constrained.formats(),
                    constrained.transforms(),
                )
            })
            .unwrap();
        let driver = constrained.formats().resolve("parquet").unwrap();
        let canonical_options = driver.canonical_options(serde_json::json!({})).unwrap();
        assert!(matches!(
            prepare_file_input(
                &resource_id,
                &constrained_matches[0],
                cdf_runtime::FormatSourceAccess::Adaptive,
                driver.as_ref(),
                &canonical_options,
                &constrained,
            )
            .unwrap()
            .input,
            PreparedFileInput::SpoolSource { .. }
        ));
        let stream = stream_file_match_blocking(
            &constrained_matches[0],
            plan.resolved_format().unwrap(),
            ReadOptions::new(resource_id, PartitionId::new("file-parquet-range").unwrap()),
            &constrained,
            schema,
            PhysicalSchemaAuthority::default(),
        )
        .unwrap();
        let batches = futures_executor::block_on(stream.collect::<Vec<_>>())
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.header.row_count)
                .sum::<u64>(),
            100_000
        );
        drop(batches);
        let spill = constrained.execution().spill().snapshot();
        assert_eq!(spill.current_bytes, 0);
        assert_eq!(spill.peak_bytes, 0);

        let contended = FileRuntimeDependencies::new(
            FileTransportFacade::new()
                .with_object_store("s3://parquet", store)
                .with_execution_services(crate::test_execution_services()),
            crate::test_execution_services(),
            crate::test_format_registry(),
            crate::test_transform_registry(),
        )
        .with_max_spool_bytes(bytes.len() as u64)
        .unwrap();
        let contended_matches = contended
            .with_transport(|transport| {
                resolve_object_store_matches(
                    &ResourceId::new("parquet.contended").unwrap(),
                    &plan,
                    transport,
                    contended.formats(),
                    contended.transforms(),
                )
            })
            .unwrap();
        let spill = contended.execution().spill();
        let budget = spill.snapshot().budget_bytes;
        let remaining = u64::try_from(bytes.len()).unwrap().saturating_sub(1);
        let held = spill
            .try_reserve(budget.saturating_sub(remaining))
            .unwrap()
            .unwrap();
        let stream = stream_file_match_blocking(
            &contended_matches[0],
            plan.resolved_format().unwrap(),
            ReadOptions::new(
                ResourceId::new("parquet.contended").unwrap(),
                PartitionId::new("file-parquet-contended").unwrap(),
            ),
            &contended,
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            PhysicalSchemaAuthority::default(),
        )
        .unwrap();
        let batches = futures_executor::block_on(stream.collect::<Vec<_>>())
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.header.row_count)
                .sum::<u64>(),
            100_000
        );
        drop(batches);
        assert_eq!(spill.snapshot().current_bytes, held.bytes());
        drop(held);
        assert_eq!(spill.snapshot().current_bytes, 0);
    }

    #[test]
    fn object_store_recursive_glob_resolves_stable_multi_file_partitions() {
        let store = Arc::new(InMemory::new());
        for path in [
            "prod/2026/01/events.parquet",
            "prod/2026/02/nested/events.parquet",
            "prod/2025/events.parquet",
        ] {
            futures_executor::block_on(store.put(
                &ObjectPath::from(path),
                PutPayload::from_static(b"PAR1payloadPAR1"),
            ))
            .unwrap();
        }
        let transport = FileTransportFacade::new()
            .with_object_store("s3://acme-events", store)
            .with_execution_services(crate::test_execution_services());
        let plan = FileResourcePlan {
            source: "events".to_owned(),
            root: "s3://acme-events/prod".to_owned(),
            glob: "2026/**/*.parquet".to_owned(),
            format: Some(FileFormatDeclaration::parquet()),
            format_declared: true,
            format_options: serde_json::json!({}),
            compression: FileCompressionDeclaration::none(),
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let resource_id = ResourceId::new("events.raw").unwrap();

        let matches = resolve_object_store_matches(
            &resource_id,
            &plan,
            &transport,
            crate::test_format_registry().as_ref(),
            crate::test_transform_registry().as_ref(),
        )
        .unwrap();
        assert_eq!(matches.len(), 2);
        assert_eq!(
            matches
                .iter()
                .map(|file| file.path_text.as_str())
                .collect::<Vec<_>>(),
            vec![
                "s3://acme-events/prod/2026/01/events.parquet",
                "s3://acme-events/prod/2026/02/nested/events.parquet",
            ]
        );
        assert!(matches.iter().all(|file| file.etag.is_some()));
    }

    #[test]
    fn remote_inventory_never_reads_payload_for_format_or_compression_detection() {
        let store = Arc::new(InMemory::new());
        futures_executor::block_on(store.put(
            &ObjectPath::from("prod/events.ndjson.gz"),
            PutPayload::from_static(b"not payload CDF should inspect during inventory"),
        ))
        .unwrap();
        let payload_opens = Arc::new(AtomicUsize::new(0));
        let transport = PayloadOpenCountingTransport {
            inner: FileTransportFacade::new()
                .with_object_store("s3://events", store)
                .with_execution_services(crate::test_execution_services()),
            payload_opens: Arc::clone(&payload_opens),
        };
        let plan = FileResourcePlan {
            source: "events".to_owned(),
            root: "s3://events/prod".to_owned(),
            glob: "events.ndjson.gz".to_owned(),
            format: Some(FileFormatDeclaration::ndjson()),
            format_declared: true,
            format_options: serde_json::json!({}),
            compression: FileCompressionDeclaration::auto(),
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };

        let matches = resolve_object_store_matches(
            &ResourceId::new("events.raw").unwrap(),
            &plan,
            &transport,
            crate::test_format_registry().as_ref(),
            crate::test_transform_registry().as_ref(),
        )
        .unwrap();

        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].compression.mode_name(), "gzip");
        assert_eq!(matches[0].format.format_id, "ndjson");
        assert_eq!(
            matches[0].format.detection.confidence,
            FormatDetectionConfidence::None
        );
        assert_eq!(payload_opens.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn http_numeric_template_expands_finitely_and_preserves_width() {
        let resource_id = ResourceId::new("tlc.yellow").unwrap();
        assert_eq!(
            expand_http_glob(&resource_id, "yellow_tripdata_2024-{01..03}.parquet").unwrap(),
            vec![
                "yellow_tripdata_2024-01.parquet",
                "yellow_tripdata_2024-02.parquet",
                "yellow_tripdata_2024-03.parquet",
            ]
        );
        assert_eq!(
            expand_http_glob(&resource_id, "yellow_tripdata_2024-*.parquet").unwrap(),
            (1..=12)
                .map(|month| format!("yellow_tripdata_2024-{month:02}.parquet"))
                .collect::<Vec<_>>()
        );
        let error = expand_http_glob(&resource_id, "yellow_tripdata_*.parquet").unwrap_err();
        assert!(error.message.contains("HTTP has no LIST operation"));
    }

    #[test]
    fn object_store_gzip_ndjson_streams_without_spill_and_preserves_remote_position() {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(b"{\"id\":1}\n{\"id\":2}\n").unwrap();
        let encoded = encoder.finish().unwrap();
        let store = Arc::new(InMemory::new());
        futures_executor::block_on(store.put(
            &ObjectPath::from("prod/2026/events.ndjson.gz"),
            PutPayload::from(encoded.clone()),
        ))
        .unwrap();
        let facade = FileTransportFacade::new()
            .with_object_store("s3://acme-events", store)
            .with_execution_services(crate::test_execution_services());
        let dependencies = FileRuntimeDependencies::new(
            facade,
            crate::test_execution_services(),
            crate::test_format_registry(),
            crate::test_transform_registry(),
        )
        .with_max_spool_bytes(encoded.len() as u64)
        .unwrap();
        let transport = dependencies.transport();
        let plan = FileResourcePlan {
            source: "events".to_owned(),
            root: "s3://acme-events/prod".to_owned(),
            glob: "2026/**/*.ndjson.gz".to_owned(),
            format: Some(FileFormatDeclaration::ndjson()),
            format_declared: true,
            format_options: serde_json::json!({}),
            compression: FileCompressionDeclaration::auto(),
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let resource_id = ResourceId::new("events.raw").unwrap();
        let resolved = resolve_object_store_matches(
            &resource_id,
            &plan,
            transport.as_ref(),
            crate::test_format_registry().as_ref(),
            crate::test_transform_registry().as_ref(),
        )
        .unwrap();
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].compression.mode_name(), "gzip");
        let options = ReadOptions::new(resource_id, PartitionId::new("file-events").unwrap());
        let admission_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let stream = stream_file_match_blocking(
            &resolved[0],
            plan.resolved_format().unwrap(),
            options.clone(),
            &dependencies,
            Arc::clone(&admission_schema),
            PhysicalSchemaAuthority::default(),
        )
        .unwrap();
        let batches = futures_executor::block_on(stream.collect::<Vec<_>>())
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.header.row_count)
                .sum::<u64>(),
            2
        );
        let SourcePosition::FileManifest(position) =
            batches[0].header.source_position.as_ref().unwrap()
        else {
            panic!("expected remote file manifest position")
        };
        assert_eq!(
            position.files[0].path,
            "s3://acme-events/prod/2026/events.ndjson.gz"
        );

        let constrained = dependencies.with_max_spool_bytes(1).unwrap();
        let stream = stream_file_match_blocking(
            &resolved[0],
            plan.resolved_format().unwrap(),
            options,
            &constrained,
            admission_schema,
            PhysicalSchemaAuthority::default(),
        )
        .unwrap();
        let constrained_batches = futures_executor::block_on(stream.collect::<Vec<_>>())
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            constrained_batches
                .iter()
                .map(|batch| batch.header.row_count)
                .sum::<u64>(),
            2
        );
        drop(constrained_batches);
        let spill = constrained.execution().spill().snapshot();
        assert_eq!(spill.current_bytes, 0);
        assert_eq!(spill.peak_bytes, 0);
    }

    #[test]
    fn local_csv_discovers_and_streams_through_registered_driver() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("events.csv");
        std::fs::write(&path, b"id,name\n1,alpha\n2,beta\n").unwrap();
        let dependencies = FileRuntimeDependencies::new(
            FileTransportFacade::new(),
            crate::test_execution_services(),
            crate::test_format_registry(),
            crate::test_transform_registry(),
        );
        let plan = FileResourcePlan {
            source: "events".to_owned(),
            root: root.path().to_string_lossy().into_owned(),
            glob: "events.csv".to_owned(),
            format: Some(FileFormatDeclaration::csv()),
            format_declared: true,
            format_options: serde_json::json!({}),
            compression: FileCompressionDeclaration::none(),
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let resource_id = ResourceId::new("events.csv").unwrap();
        let resolved = dependencies
            .with_transport(|transport| {
                resolve_file_matches(
                    &resource_id,
                    &plan,
                    transport,
                    dependencies.formats(),
                    dependencies.transforms(),
                )
            })
            .unwrap();
        let probe = discover_local_binary_schema_bounded(
            &path,
            "events.csv",
            &dependencies,
            0,
            BoundedSchemaDiscoveryRequest {
                resource_id: &resource_id,
                format: plan.resolved_format().unwrap(),
                format_declared: plan.format_declared,
                format_options: &plan.format_options,
                transform_name: "none",
                maximum_bytes: 1024 * 1024,
                maximum_records: 1_000,
            },
        )
        .unwrap();
        assert_eq!(probe.schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(probe.schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(dependencies.prepared_payloads().pending_count().unwrap(), 1);
        std::fs::remove_file(&path).unwrap();
        let stream = stream_file_match_blocking(
            &resolved[0],
            plan.resolved_format().unwrap(),
            ReadOptions::new(resource_id, PartitionId::new("csv-file").unwrap()),
            &dependencies,
            Arc::clone(&probe.schema),
            PhysicalSchemaAuthority::default(),
        )
        .unwrap();
        let batches = futures_executor::block_on(stream.collect::<Vec<_>>())
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.header.row_count)
                .sum::<u64>(),
            2
        );
        assert!(matches!(
            batches[0].header.source_position,
            Some(SourcePosition::FileManifest(_))
        ));
        assert_eq!(dependencies.prepared_payloads().pending_count().unwrap(), 0);
        drop(batches);
        assert_eq!(
            dependencies.execution().memory().snapshot().current_bytes,
            0
        );
    }

    #[test]
    fn local_ndjson_discovery_replays_and_continues_the_same_source() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("events.ndjson");
        std::fs::write(
            &path,
            b"{\"id\":1,\"name\":\"alpha\"}\n{\"id\":2,\"name\":\"beta\"}\n",
        )
        .unwrap();
        let dependencies = FileRuntimeDependencies::new(
            FileTransportFacade::new(),
            crate::test_execution_services(),
            crate::test_format_registry(),
            crate::test_transform_registry(),
        );
        let plan = FileResourcePlan {
            source: "events".to_owned(),
            root: root.path().to_string_lossy().into_owned(),
            glob: "events.ndjson".to_owned(),
            format: Some(FileFormatDeclaration::ndjson()),
            format_declared: true,
            format_options: serde_json::json!({}),
            compression: FileCompressionDeclaration::none(),
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let resource_id = ResourceId::new("events.ndjson").unwrap();
        let resolved = dependencies
            .with_transport(|transport| {
                resolve_file_matches(
                    &resource_id,
                    &plan,
                    transport,
                    dependencies.formats(),
                    dependencies.transforms(),
                )
            })
            .unwrap();
        let probe = discover_local_binary_schema_bounded(
            &path,
            "events.ndjson",
            &dependencies,
            0,
            BoundedSchemaDiscoveryRequest {
                resource_id: &resource_id,
                format: plan.resolved_format().unwrap(),
                format_declared: plan.format_declared,
                format_options: &plan.format_options,
                transform_name: "none",
                maximum_bytes: 1024 * 1024,
                maximum_records: 1_000,
            },
        )
        .unwrap();
        assert_eq!(probe.schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(probe.schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(dependencies.prepared_payloads().pending_count().unwrap(), 1);
        std::fs::remove_file(&path).unwrap();

        let stream = stream_file_match_blocking(
            &resolved[0],
            plan.resolved_format().unwrap(),
            ReadOptions::new(resource_id, PartitionId::new("ndjson-file").unwrap()),
            &dependencies,
            Arc::clone(&probe.schema),
            PhysicalSchemaAuthority::default(),
        )
        .unwrap();
        let batches = futures_executor::block_on(stream.collect::<Vec<_>>())
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.header.row_count)
                .sum::<u64>(),
            2
        );
        assert!(matches!(
            batches[0].header.source_position,
            Some(SourcePosition::FileManifest(_))
        ));
        assert_eq!(dependencies.prepared_payloads().pending_count().unwrap(), 0);
        drop(batches);
        assert_eq!(
            dependencies.execution().memory().snapshot().current_bytes,
            0
        );
    }

    #[test]
    fn retained_sequential_window_replays_then_continues_one_source_invocation() {
        struct ChunkedTestSource {
            identity: ContentIdentity,
            capabilities: ByteSourceCapabilities,
            payload: Arc<Vec<u8>>,
            memory: Arc<dyn cdf_memory::MemoryCoordinator>,
            opens: Arc<AtomicUsize>,
            chunk_bytes: usize,
        }

        impl ByteSource for ChunkedTestSource {
            fn identity(&self) -> &ContentIdentity {
                &self.identity
            }

            fn capabilities(&self) -> &ByteSourceCapabilities {
                &self.capabilities
            }

            fn open_sequential(
                &self,
                request: SequentialReadRequest,
            ) -> BoxFuture<'_, Result<AccountedByteStream>> {
                let payload = Arc::clone(&self.payload);
                let memory = Arc::clone(&self.memory);
                let opens = Arc::clone(&self.opens);
                let chunk_bytes = self.chunk_bytes;
                Box::pin(async move {
                    request.cancellation.check()?;
                    if opens.fetch_add(1, Ordering::Relaxed) != 0 {
                        return Err(CdfError::data("test source was opened more than once"));
                    }
                    let state = (0_usize, payload, memory, request.cancellation);
                    Ok(Box::pin(futures_util::stream::try_unfold(
                        state,
                        move |(offset, payload, memory, cancellation)| async move {
                            cancellation.check()?;
                            if offset == payload.len() {
                                return Ok(None);
                            }
                            let end = offset.saturating_add(chunk_bytes).min(payload.len());
                            let bytes = bytes::Bytes::copy_from_slice(&payload[offset..end]);
                            let lease = cdf_memory::reserve(
                                Arc::clone(&memory),
                                cdf_memory::ReservationRequest::new(
                                    cdf_memory::ConsumerKey::new(
                                        "retained-window-test-source",
                                        MemoryClass::Source,
                                    )?,
                                    u64::try_from(bytes.len()).map_err(|_| {
                                        CdfError::data("test source chunk exceeds u64")
                                    })?,
                                )?,
                            )
                            .await?;
                            let chunk = cdf_memory::AccountedBytes::new(bytes, lease)?;
                            Ok(Some((chunk, (end, payload, memory, cancellation))))
                        },
                    )) as AccountedByteStream)
                })
            }

            fn read_exact_range(
                &self,
                _extent: ByteExtent,
                _cancellation: cdf_runtime::RunCancellation,
            ) -> BoxFuture<'_, Result<cdf_memory::AccountedBytes>> {
                Box::pin(async {
                    Err(CdfError::contract(
                        "sequential test source does not support ranges",
                    ))
                })
            }
        }

        let dependencies = FileRuntimeDependencies::new(
            FileTransportFacade::new(),
            crate::test_execution_services(),
            crate::test_format_registry(),
            crate::test_transform_registry(),
        );
        let payload = b"first-window|second-window|live-continuation".to_vec();
        let opens = Arc::new(AtomicUsize::new(0));
        let source: Arc<dyn ByteSource> = Arc::new(ChunkedTestSource {
            identity: ContentIdentity {
                stable_id: "retained-window-test".to_owned(),
                size_bytes: Some(payload.len() as u64),
                generation: Some("test-generation".to_owned()),
                checksum: None,
                strength: GenerationStrength::Strong,
            },
            capabilities: ByteSourceCapabilities {
                known_length: true,
                reopenable: false,
                seekable: false,
                exact_ranges: false,
                useful_range_concurrency: 0,
                minimum_chunk_bytes: 1,
                maximum_chunk_bytes: 1024,
            },
            payload: Arc::new(payload.clone()),
            memory: dependencies.execution().memory(),
            opens: Arc::clone(&opens),
            chunk_bytes: 13,
        });
        let observed = dependencies
            .execution()
            .run_io({
                let dependencies = dependencies.clone();
                async move {
                    let capture = SequentialPayloadCapture::new(source, &dependencies).await?;
                    let discovery_source = capture.discovery_source();
                    assert!(!discovery_source.capabilities().reopenable);
                    let mut discovery = discovery_source
                        .open_sequential(SequentialReadRequest {
                            preferred_chunk_bytes: 13,
                            cancellation: cdf_runtime::RunCancellation::default(),
                        })
                        .await?;
                    let first = discovery.try_next().await?.ok_or_else(|| {
                        CdfError::internal("test discovery stream omitted first chunk")
                    })?;
                    assert_eq!(first.payload(), b"first-window|");
                    drop(first);
                    drop(discovery);

                    let prepared = capture.finish(None).await?;
                    let (prepared, retention) = prepared
                        .into_typed::<PreparedFilePayload>("retained-window test execution")?;
                    assert_eq!(retention.bytes(), 13);
                    assert!(prepared.source_content_digest.is_none());
                    let mut execution = prepared
                        .source
                        .open_sequential(SequentialReadRequest {
                            preferred_chunk_bytes: 7,
                            cancellation: cdf_runtime::RunCancellation::default(),
                        })
                        .await?;
                    let mut observed = Vec::new();
                    while let Some(chunk) = execution.try_next().await? {
                        observed.extend_from_slice(chunk.payload());
                    }
                    drop(execution);
                    drop(retention);
                    Ok::<_, CdfError>(observed)
                }
            })
            .unwrap();
        assert_eq!(observed, payload);
        assert_eq!(opens.load(Ordering::Relaxed), 1);
        assert_eq!(
            dependencies.execution().memory().snapshot().current_bytes,
            0
        );
        assert_eq!(dependencies.execution().spill().snapshot().current_bytes, 0);
    }

    #[test]
    fn bounded_and_full_content_drivers_share_the_retained_stream_handoff() {
        let mut descriptor = ExternalMockFormat::new().descriptor().clone();
        assert!(retains_sequential_discovery_payload(&descriptor));

        descriptor.discovery_kind = cdf_runtime::FormatDiscoveryKind::FullContent;
        assert!(retains_sequential_discovery_payload(&descriptor));

        descriptor.discovery_kind = cdf_runtime::FormatDiscoveryKind::FormatMetadata;
        assert!(!retains_sequential_discovery_payload(&descriptor));
        descriptor.discovery_kind = cdf_runtime::FormatDiscoveryKind::FullContent;
        descriptor.source_access = cdf_runtime::FormatSourceAccess::Adaptive;
        assert!(!retains_sequential_discovery_payload(&descriptor));
    }

    #[test]
    fn local_json_document_discovers_and_streams_through_registered_driver() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("events.json");
        std::fs::write(
            &path,
            br#"[{"id":1,"name":"alpha },["},{"id":2,"name":"beta"}]"#,
        )
        .unwrap();
        let dependencies = FileRuntimeDependencies::new(
            FileTransportFacade::new(),
            crate::test_execution_services(),
            crate::test_format_registry(),
            crate::test_transform_registry(),
        );
        let plan = FileResourcePlan {
            source: "events".to_owned(),
            root: root.path().to_string_lossy().into_owned(),
            glob: "events.json".to_owned(),
            format: Some(FileFormatDeclaration::json()),
            format_declared: true,
            format_options: serde_json::json!({}),
            compression: FileCompressionDeclaration::none(),
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let resource_id = ResourceId::new("events.json").unwrap();
        let resolved = dependencies
            .with_transport(|transport| {
                resolve_file_matches(
                    &resource_id,
                    &plan,
                    transport,
                    dependencies.formats(),
                    dependencies.transforms(),
                )
            })
            .unwrap();
        let probe = discover_local_binary_schema_bounded(
            &path,
            "events.json",
            &dependencies,
            0,
            BoundedSchemaDiscoveryRequest {
                resource_id: &resource_id,
                format: plan.resolved_format().unwrap(),
                format_declared: plan.format_declared,
                format_options: &plan.format_options,
                transform_name: "none",
                maximum_bytes: 1024 * 1024,
                maximum_records: 1_000,
            },
        )
        .unwrap();
        assert_eq!(probe.schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(probe.schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(dependencies.prepared_payloads().pending_count().unwrap(), 1);
        std::fs::remove_file(&path).unwrap();
        let stream = stream_file_match_blocking(
            &resolved[0],
            plan.resolved_format().unwrap(),
            ReadOptions::new(resource_id, PartitionId::new("json-file").unwrap()),
            &dependencies,
            Arc::clone(&probe.schema),
            PhysicalSchemaAuthority::default(),
        )
        .unwrap();
        let batches = futures_executor::block_on(stream.collect::<Vec<_>>())
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.header.row_count)
                .sum::<u64>(),
            2
        );
        assert!(matches!(
            batches[0].header.source_position,
            Some(SourcePosition::FileManifest(_))
        ));
        assert_eq!(dependencies.prepared_payloads().pending_count().unwrap(), 0);
        drop(batches);
        assert_eq!(
            dependencies.execution().memory().snapshot().current_bytes,
            0
        );
    }

    #[test]
    fn explicit_format_wins_over_an_ambiguous_extension() {
        let formats = crate::test_format_registry();
        let resource_id = ResourceId::new("events.rows").unwrap();
        let mut plan = FileResourcePlan {
            source: "events".to_owned(),
            root: ".".to_owned(),
            glob: "events.json".to_owned(),
            format: Some(FileFormatDeclaration::ndjson()),
            format_declared: true,
            format_options: serde_json::json!({}),
            compression: FileCompressionDeclaration::none(),
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };

        validate_format_extension(
            &resource_id,
            &plan,
            "events.json",
            Some("json"),
            formats.as_ref(),
        )
        .unwrap();

        plan.format_declared = false;
        let error = validate_format_extension(
            &resource_id,
            &plan,
            "events.json",
            Some("json"),
            formats.as_ref(),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("extension `.json` selects `json`")
        );
    }
}
#[test]
fn format_confirmation_shares_the_configured_discovery_byte_budget() {
    let context = FormatConfirmationContext {
        resource_id: ResourceId::new("events.raw").unwrap(),
        location: "https://example.test/events.parquet".to_owned(),
        format_declared: false,
        transform_name: "none".to_owned(),
    };
    assert_eq!(
        discovery_budget_after_confirmation(1_024, 24, &context).unwrap(),
        1_000
    );
    for confirmation_bytes in [1_024, 1_025] {
        assert!(discovery_budget_after_confirmation(1_024, confirmation_bytes, &context).is_err());
    }
}
