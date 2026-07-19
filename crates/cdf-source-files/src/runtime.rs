use std::{
    collections::BTreeMap,
    fmt, fs,
    io::Read,
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
    time::UNIX_EPOCH,
};

use arrow_schema::{Schema, SchemaRef};
use cdf_kernel::{
    BackpressureSupport, Batch, BatchStream, BoxFuture, CapabilitySupport, CdfError,
    CompiledScanIntent, DeliveryGuarantee, EffectiveSchemaRuntime, EstimateSupport, ExpressionNode,
    FilterCapabilities, IncrementalShape, PLAN_SCHEMA_OBSERVATION_BINDING_KEY,
    PLAN_SCHEMA_OBSERVATION_ID_KEY, PartitionAttestation, PartitionCompletion, PartitionId,
    PartitionPlan, PartitioningCapabilities, PayloadRetention, PlanId, PushdownFidelity,
    PushedPredicate, QueryableResource, ReplaySupport, ResourceCapabilities, ResourceDescriptor,
    ResourceId, ResourceStream, Result, ScanPlan, ScanRequest, SchemaHash, ScopeKey, ScopeKind,
    SourcePosition, SourceReadMode, TypePolicyAllowances, WriteDisposition,
    partition_schema_observation_id, source_name,
};
use cdf_memory::{ConsumerKey, MemoryClass};
use cdf_runtime::{
    AccountedByteStream, BlockingLaneSpec, ByteExtent, ByteSource, ByteSourceCapabilities,
    ByteTransformId, ByteTransformRegistry, CanonicalStreamCompletion, CanonicalStreamOpener,
    CompiledFormatBinding, ContentIdentity, DecodePlanningRequest, ExecutionServices,
    FormatDetection, FormatDetectionConfidence, FormatDiscoveryRequest, FormatDriver, FormatProbe,
    FormatRegistry, GenerationStrength, InterruptionSafety, LaneAffinity, ObservedByteSource,
    PhysicalDecodeRequest, PreparedSourcePayload, PreparedSourcePayloadKey, PreparedSourcePayloads,
    ReadOptions, SequentialReadRequest, SourceContentDigest, SourceDriverId, SourceEgressScope,
    SourceEvidenceLocation, SourceIoObserver, TransformSourceConfig, TransformedByteSource,
    canonical_stream_frontier_with_completion, decode_unit_no_lookback_frontiers,
    resolve_decode_unit_concurrency,
};
#[cfg(test)]
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

#[cfg(test)]
use crate::FileTransportFacade;
use crate::{
    FileCompressionDeclaration, FileFormatDeclaration, FileIdentityMetadata, FileResourcePlan,
    FileTransport, FileTransportControl, FileTransportLocation, FileTransportResource,
    LocalByteSource,
    driver::{FileTransportScheme, file_transport_scheme},
    evicting_spool_byte_source::start_evicting_spool,
    growing_spool_byte_source::start_growing_spool,
    local_byte_source::local_source_generation,
};

const NATIVE_TARGET_BATCH_ROWS: usize = 64 * 1024;
const NATIVE_TARGET_BATCH_BYTES: u64 = 16 * 1024 * 1024;
const NATIVE_STREAM_ITEMS: usize = 2;
const NATIVE_UNIT_STREAM_ITEMS: usize = 1;
const NATIVE_UNIT_BUFFERED_BATCHES: u16 = 2;
pub const FILE_SOURCE_BLOCKING_LANE_ID: &str = "file-source.control";
pub const FILE_SOURCE_ADVERTISED_PARALLELISM: u16 = 16;

pub fn file_source_blocking_lane() -> BlockingLaneSpec {
    BlockingLaneSpec {
        lane_id: FILE_SOURCE_BLOCKING_LANE_ID.to_owned(),
        maximum_concurrency: FILE_SOURCE_ADVERTISED_PARALLELISM,
        cpu_slot_cost: 1,
        native_internal_parallelism: 1,
        affinity: LaneAffinity::Shared,
        interruption: InterruptionSafety::CooperativeOnly,
    }
}

#[derive(Clone)]
pub struct FileRuntimeDependencies {
    transport: Arc<dyn FileTransport>,
    execution: ExecutionServices,
    formats: Arc<FormatRegistry>,
    transforms: Arc<ByteTransformRegistry>,
    prepared_payloads: PreparedSourcePayloads,
    payload_cache: Option<crate::FilePayloadCache>,
    egress: SourceEgressScope,
    max_spool_bytes: u64,
}

const DEFAULT_MAX_FILE_SPOOL_BYTES: u64 = 64 * 1024 * 1024 * 1024;

impl FileRuntimeDependencies {
    pub fn new(
        transport: impl FileTransport + 'static,
        execution: ExecutionServices,
        formats: Arc<FormatRegistry>,
        transforms: Arc<ByteTransformRegistry>,
        egress: SourceEgressScope,
    ) -> Self {
        Self::from_boxed_transport(Box::new(transport), execution, formats, transforms, egress)
    }

    pub fn from_boxed_transport(
        transport: Box<dyn FileTransport>,
        execution: ExecutionServices,
        formats: Arc<FormatRegistry>,
        transforms: Arc<ByteTransformRegistry>,
        egress: SourceEgressScope,
    ) -> Self {
        Self {
            transport: Arc::from(transport),
            execution,
            formats,
            transforms,
            prepared_payloads: PreparedSourcePayloads::default(),
            payload_cache: None,
            egress,
            max_spool_bytes: DEFAULT_MAX_FILE_SPOOL_BYTES,
        }
    }

    pub fn with_prepared_payloads(mut self, prepared_payloads: PreparedSourcePayloads) -> Self {
        self.prepared_payloads = prepared_payloads;
        self
    }

    pub fn with_payload_cache(mut self, payload_cache: crate::FilePayloadCache) -> Self {
        self.payload_cache = Some(payload_cache);
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

    pub(crate) fn execution(&self) -> &ExecutionServices {
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

    pub fn payload_cache(&self) -> Option<&crate::FilePayloadCache> {
        self.payload_cache.as_ref()
    }

    #[cfg(test)]
    fn transport(&self) -> Arc<dyn FileTransport> {
        Arc::clone(&self.transport)
    }

    pub fn with_transport<R>(
        &self,
        f: impl FnOnce(&dyn FileTransport, &SourceEgressScope) -> Result<R>,
    ) -> Result<R> {
        f(self.transport.as_ref(), &self.egress)
    }

    fn transport_and_egress(&self) -> (Arc<dyn FileTransport>, SourceEgressScope) {
        (Arc::clone(&self.transport), self.egress.clone())
    }
}

#[derive(Clone, Debug)]
pub struct BinarySchemaProbe {
    pub schema: SchemaRef,
    pub source_identity: BTreeMap<String, String>,
    pub probe_bytes_read: u64,
    pub probe_records_read: u64,
}

#[derive(Clone, Debug)]
pub struct SchemaDiscoveryRequest<'a> {
    pub resource_id: &'a ResourceId,
    pub format: &'a FileFormatDeclaration,
    pub format_declared: bool,
    pub format_options: &'a serde_json::Value,
    pub discovery_kind: cdf_runtime::FormatDiscoveryKind,
    pub transform_name: &'a str,
    pub maximum_bytes: u64,
    pub maximum_records: u64,
    pub cancellation: cdf_runtime::RunCancellation,
}

pub fn discover_local_binary_schema(
    path: impl AsRef<Path>,
    location: &str,
    dependencies: &FileRuntimeDependencies,
    initial_bytes_read: u64,
    request: SchemaDiscoveryRequest<'_>,
) -> Result<BinarySchemaProbe> {
    request.cancellation.check()?;
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
    let retain_sequential =
        retains_sequential_discovery_payload(driver.descriptor(), request.discovery_kind);
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
    let discovery_kind = request.discovery_kind;
    let cancellation = request.cancellation.clone();
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
                        None,
                        &dependencies,
                        cancellation.clone(),
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
                cancellation.clone(),
            )
            .await?;
            let discovery_bytes = schema_observation_byte_limit(
                maximum_bytes,
                confirmation_bytes,
                &confirmation,
                discovery_kind,
            )?;
            let observation = driver
                .discover(
                    Arc::clone(&source),
                    FormatDiscoveryRequest {
                        options,
                        discovery_kind,
                        maximum_bytes: discovery_bytes,
                        maximum_records,
                        memory: discovery_memory,
                        cancellation: cancellation.clone(),
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
        (
            "stable_id".to_owned(),
            diagnostic_location(&logical_source_identity.stable_id)?,
        ),
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
    Ok(BinarySchemaProbe {
        schema,
        source_identity,
        probe_bytes_read: initial_bytes_read.saturating_add(probe_bytes_read),
        probe_records_read,
    })
}

pub fn discover_transport_binary_schema(
    resource: FileTransportResource,
    dependencies: &FileRuntimeDependencies,
    request: SchemaDiscoveryRequest<'_>,
) -> Result<BinarySchemaProbe> {
    let control = FileTransportControl::new(request.cancellation.clone(), None);
    let observation = dependencies
        .with_transport(|transport, egress| transport.metadata(egress, &resource, &control))?;
    let access_resource = observation.access_resource(&resource);
    let metadata = observation.into_identity();
    let evidence_location = diagnostic_location(&metadata.location)?;
    let size_bytes = metadata.size_bytes.ok_or_else(|| {
        CdfError::data(format!(
            "remote binary discovery for `{}` did not receive byte-size metadata",
            evidence_location
        ))
    })?;
    let driver = dependencies.formats().resolve(request.format.as_str())?;
    let upstream = dependencies.with_transport(|transport, egress| {
        transport.open_byte_source(
            egress,
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
    let retain_sequential =
        retains_sequential_discovery_payload(driver.descriptor(), request.discovery_kind);
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
        location: evidence_location.clone(),
        format_declared: request.format_declared,
        transform_name: request.transform_name.to_owned(),
    };
    let maximum_bytes = request.maximum_bytes;
    let maximum_records = request.maximum_records;
    let discovery_kind = request.discovery_kind;
    let cancellation = request.cancellation.clone();
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
                        None,
                        &dependencies,
                        cancellation.clone(),
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
                cancellation.clone(),
            )
            .await?;
            let discovery_bytes = schema_observation_byte_limit(
                maximum_bytes,
                confirmation_bytes,
                &confirmation,
                discovery_kind,
            )?;
            let observation = driver
                .discover(
                    Arc::clone(&source),
                    FormatDiscoveryRequest {
                        options,
                        discovery_kind,
                        maximum_bytes: discovery_bytes,
                        maximum_records,
                        memory,
                        cancellation: cancellation.clone(),
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
        (
            "stable_id".to_owned(),
            diagnostic_location(&logical_source_identity.stable_id)?,
        ),
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
    let mut probe = BinarySchemaProbe {
        schema: observation.arrow_schema,
        source_identity,
        probe_bytes_read,
        probe_records_read,
    };
    probe
        .source_identity
        .insert("url".to_owned(), evidence_location);
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

fn schema_observation_byte_limit(
    maximum_bytes: u64,
    confirmation_bytes: u64,
    context: &FormatConfirmationContext,
    discovery_kind: cdf_runtime::FormatDiscoveryKind,
) -> Result<u64> {
    if discovery_kind == cdf_runtime::FormatDiscoveryKind::FullContent {
        return Ok(maximum_bytes);
    }
    discovery_budget_after_confirmation(maximum_bytes, confirmation_bytes, context)
}

async fn confirm_registered_format(
    source: &dyn ByteSource,
    source_size: u64,
    driver: &Arc<dyn FormatDriver>,
    formats: &FormatRegistry,
    context: &FormatConfirmationContext,
    cancellation: cdf_runtime::RunCancellation,
) -> Result<u64> {
    cancellation.check()?;
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
        let selected_format_id = driver.descriptor().format_id.clone();
        let alternate = formats
            .detect_best_alternate(&probe, &selected_format_id)?
            .map(|(id, detection)| {
                format!(
                    "; alternate format `{id}` detected with {} confidence: {}",
                    format_detection_confidence_name(detection.confidence),
                    detection.reason
                )
            })
            .unwrap_or_default();
        return Err(CdfError::data(format!(
            "file format confirmation failed for resource `{}`, file `{}`: declared format `{declared}`, inferred format `{selected_id}`, extension signal `{}`, magic bytes signal `{magic}`{alternate}; use `format = \"{selected_id}\"` only when the bytes match, or correct the file/extension",
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
    prepared_inventory_key: Option<PreparedSourcePayloadKey>,
    compiled_source_plan_hash: Option<String>,
    transport_control: FileTransportControl,
}

#[derive(Clone, Debug)]
pub struct FileResourceDefinition {
    pub descriptor: ResourceDescriptor,
    pub schema: SchemaRef,
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
        let capabilities = file_resource_capabilities(&compiled_format.descriptor);
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
            prepared_inventory_key: None,
            compiled_source_plan_hash: None,
            transport_control: FileTransportControl::default(),
        })
    }

    pub(crate) fn with_prepared_inventory_key(mut self, key: PreparedSourcePayloadKey) -> Self {
        self.prepared_inventory_key = Some(key);
        self
    }

    pub(crate) fn with_transport_control(mut self, control: FileTransportControl) -> Self {
        self.transport_control = control;
        self
    }

    pub fn with_compiled_source_plan_hash(mut self, hash: String) -> Self {
        self.compiled_source_plan_hash = Some(hash);
        self
    }

    fn partitions_for_intent(
        &self,
        scan_intent: &CompiledScanIntent,
    ) -> Result<Vec<PartitionPlan>> {
        self.partitions_for_intent_with_inventory_limit(
            scan_intent,
            usize::MAX,
            &self.transport_control,
        )
    }

    pub(crate) fn partitions_for_intent_with_inventory_limit(
        &self,
        scan_intent: &CompiledScanIntent,
        maximum_matches: usize,
        control: &FileTransportControl,
    ) -> Result<Vec<PartitionPlan>> {
        if let Some(key) = &self.prepared_inventory_key
            && let Some(payload) = self.dependencies.prepared_payloads().take(key)?
        {
            let (encoded, _retention) =
                payload.into_typed::<Vec<u8>>("file partition inventory")?;
            let mut partitions: Vec<PartitionPlan> =
                serde_json::from_slice(&encoded).map_err(|error| {
                    CdfError::internal(format!("decode prepared file inventory: {error}"))
                })?;
            if partitions.is_empty() {
                return Err(CdfError::internal(
                    "prepared file inventory did not contain any partitions",
                ));
            }
            if partitions.len() > maximum_matches {
                return Err(CdfError::data(format!(
                    "file inventory exceeds the {maximum_matches}-entry boundary"
                )));
            }
            for partition in &mut partitions {
                if partition.metadata.get("resource_id").map(String::as_str)
                    != Some(self.descriptor.resource_id.as_str())
                    || partition.metadata.get("glob").map(String::as_str)
                        != Some(self.plan.glob.as_str())
                {
                    return Err(CdfError::internal(
                        "prepared file inventory does not match its compiled resource plan",
                    ));
                }
                partition.scan_intent = scan_intent.clone();
            }
            return Ok(partitions);
        }
        let execution = self.dependencies.execution().clone();
        execution.ensure_blocking_lanes(&[file_source_blocking_lane()])?;
        let (transport, egress) = self.dependencies.transport_and_egress();
        let descriptor = self.descriptor.clone();
        let plan = self.plan.clone();
        let scan_intent = scan_intent.clone();
        let formats = Arc::clone(self.dependencies.formats());
        let transforms = Arc::clone(self.dependencies.transforms());
        let control = control.clone();
        execution
            .clone()
            .run_blocking(FILE_SOURCE_BLOCKING_LANE_ID, move || {
                file_partitions_for_plan_with_transport(
                    &descriptor,
                    &plan,
                    &scan_intent,
                    FilePlanningContext {
                        transport: transport.as_ref(),
                        egress: &egress,
                        formats: formats.as_ref(),
                        transforms: transforms.as_ref(),
                        maximum_matches,
                        control: &control,
                        execution,
                    },
                )
            })
    }

    pub fn validate_runtime_dependencies(&self) -> Result<()> {
        Ok(())
    }

    pub fn open_preview(&self, partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
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

    fn compiled_source_plan_hash(&self) -> Option<&str> {
        self.compiled_source_plan_hash.as_deref()
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
        self.partitions_for_intent(&CompiledScanIntent::full_scan())
    }

    fn open(&self, partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        open_file_resource_with_dependencies(self.clone(), partition)
    }

    fn attest_partition(
        &self,
        partition: PartitionPlan,
    ) -> cdf_kernel::PartitionAttestationAttempt<'_> {
        let descriptor = self.descriptor.clone();
        let plan = self.plan.clone();
        let dependencies = self.dependencies.clone();
        let execution = dependencies.execution().clone();
        if let Err(error) = execution.ensure_blocking_lanes(&[file_source_blocking_lane()]) {
            return cdf_kernel::PartitionAttestationAttempt::materialized(Box::pin(async move {
                Err(error)
            }));
        }
        let mut scope_hasher = Sha256::new();
        scope_hasher.update(descriptor.resource_id.as_str().as_bytes());
        scope_hasher.update([0]);
        scope_hasher.update(partition.partition_id.as_str().as_bytes());
        let scope_id = format!(
            "file-attest-{}",
            &hex::encode(scope_hasher.finalize())[..16]
        );
        let task = execution.spawn_blocking_value(
            &scope_id,
            FILE_SOURCE_BLOCKING_LANE_ID,
            move |cancellation| {
                cancellation.check()?;
                let control = FileTransportControl::new(cancellation.clone(), None);
                let resolved = dependencies.with_transport(|transport, egress| {
                    validate_partition(
                        &descriptor,
                        &plan,
                        &partition,
                        FileResolutionContext {
                            transport,
                            egress,
                            formats: dependencies.formats(),
                            transforms: dependencies.transforms(),
                            control: &control,
                        },
                    )
                })?;
                cancellation.check()?;
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
            },
        );
        let task = match task {
            Ok(task) => task,
            Err(error) => {
                return cdf_kernel::PartitionAttestationAttempt::materialized(Box::pin(
                    async move { Err(error) },
                ));
            }
        };
        let termination = task.termination();
        cdf_kernel::PartitionAttestationAttempt::with_termination(Box::pin(task), termination)
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
        if request.resource_id != self.descriptor.resource_id {
            return Err(CdfError::contract(format!(
                "scan request resource `{}` does not match compiled file resource `{}`",
                request.resource_id, self.descriptor.resource_id
            )));
        }
        let negotiation = compile_file_scan(
            request,
            &self.compiled_format.descriptor,
            self.schema.as_ref(),
        )?;
        let mut partitions = self.partitions_for_intent(&negotiation.intent)?;
        let negotiation = reconcile_exact_file_predicates(
            negotiation,
            &partitions,
            self.schema.as_ref(),
            self.effective_schema_runtime.as_deref(),
        )?;
        for partition in &mut partitions {
            partition.scan_intent = negotiation.intent.clone();
        }
        Ok(ScanPlan {
            plan_id: PlanId::new(format!("plan-{}", self.descriptor.resource_id))?,
            request: request.clone(),
            partitions,
            pushed_predicates: negotiation.pushed_predicates,
            unsupported_predicates: negotiation.unsupported_predicates,
            estimated_rows: None,
            estimated_bytes: None,
            delivery_guarantee: delivery_guarantee(&self.descriptor),
        })
    }
}

struct FileScanNegotiation {
    intent: CompiledScanIntent,
    pushed_predicates: Vec<PushedPredicate>,
    unsupported_predicates: Vec<cdf_kernel::ScanPredicate>,
}

fn compile_file_scan(
    request: &ScanRequest,
    descriptor: &cdf_runtime::FormatDriverDescriptor,
    schema: &Schema,
) -> Result<FileScanNegotiation> {
    let projection = (descriptor.projection_pushdown == PushdownFidelity::Exact)
        .then(|| request.projection.clone())
        .flatten();
    let mut pushed_predicates = Vec::new();
    let mut unsupported_predicates = Vec::new();
    for predicate in &request.filters {
        let operator_supported = predicate
            .canonical_expression
            .comparison_operator()
            .is_some_and(|operator| {
                descriptor
                    .predicate_operators
                    .iter()
                    .any(|item| item == operator)
            });
        let lowering_supported = descriptor.predicate_pushdown != PushdownFidelity::Exact
            || cdf_expression::bind_boolean_expression(
                &predicate.canonical_expression.root,
                schema,
            )
            .is_ok();
        let supported = operator_supported && lowering_supported;
        if supported && descriptor.predicate_pushdown != PushdownFidelity::Unsupported {
            pushed_predicates.push(PushedPredicate {
                predicate: predicate.clone(),
                fidelity: descriptor.predicate_pushdown.clone(),
            });
        } else {
            unsupported_predicates.push(predicate.clone());
        }
    }
    let intent = CompiledScanIntent {
        version: cdf_kernel::COMPILED_SCAN_INTENT_VERSION,
        projection,
        predicates: pushed_predicates.clone(),
        limit: None,
        order_by: Vec::new(),
    };
    intent.validate()?;
    Ok(FileScanNegotiation {
        intent,
        pushed_predicates,
        unsupported_predicates,
    })
}

fn reconcile_exact_file_predicates(
    negotiation: FileScanNegotiation,
    partitions: &[PartitionPlan],
    effective_schema: &Schema,
    runtime: Option<&EffectiveSchemaRuntime>,
) -> Result<FileScanNegotiation> {
    let mut pushed_predicates = Vec::with_capacity(negotiation.pushed_predicates.len());
    let mut unsupported_predicates = negotiation.unsupported_predicates;
    for pushed in negotiation.pushed_predicates {
        let exact_for_every_partition = pushed.fidelity != PushdownFidelity::Exact
            || exact_predicate_is_partition_equivalent(
                &pushed.predicate,
                partitions,
                effective_schema,
                runtime,
            )?;
        if exact_for_every_partition {
            pushed_predicates.push(pushed);
        } else {
            unsupported_predicates.push(pushed.predicate);
        }
    }
    let intent = CompiledScanIntent {
        predicates: pushed_predicates.clone(),
        ..negotiation.intent
    };
    Ok(FileScanNegotiation {
        intent,
        pushed_predicates,
        unsupported_predicates,
    })
}

fn exact_predicate_is_partition_equivalent(
    predicate: &cdf_kernel::ScanPredicate,
    partitions: &[PartitionPlan],
    effective_schema: &Schema,
    runtime: Option<&EffectiveSchemaRuntime>,
) -> Result<bool> {
    let Some(runtime) = runtime else {
        return Ok(false);
    };
    if partitions.is_empty() {
        return Ok(false);
    }
    for partition in partitions {
        let Some(observation) = runtime
            .evidence
            .observation(partition_schema_observation_id(partition))
        else {
            return Ok(false);
        };
        let Some(physical_schema) = runtime.physical_schema(&observation.physical_schema_hash)
        else {
            return Ok(false);
        };
        for logical_name in predicate.canonical_expression.column_dependencies() {
            let effective_field = effective_schema.field_with_name(&logical_name).map_err(|_| {
                CdfError::contract(format!(
                    "compiled file predicate field {logical_name:?} is absent from the effective schema"
                ))
            })?;
            let physical_name =
                source_name(effective_field).unwrap_or_else(|| effective_field.name());
            let Ok(physical_field) = physical_schema.field_with_name(physical_name) else {
                return Ok(false);
            };
            // Exact physical pushdown is deliberately conservative. Any type
            // reconciliation, even a lossless width change, is evaluated after
            // admission so filtering cannot bypass coercion or quarantine.
            if physical_field.data_type() != effective_field.data_type() {
                return Ok(false);
            }
        }
        let physical =
            physical_expression_node(effective_schema, &predicate.canonical_expression.root)?;
        if cdf_expression::bind_boolean_expression(&physical, physical_schema.as_ref()).is_err() {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) fn file_resource_capabilities(
    descriptor: &cdf_runtime::FormatDriverDescriptor,
) -> ResourceCapabilities {
    ResourceCapabilities {
        projection: if descriptor.projection_pushdown == PushdownFidelity::Exact {
            CapabilitySupport::Supported
        } else {
            CapabilitySupport::Unsupported
        },
        filters: FilterCapabilities {
            default_fidelity: descriptor.predicate_pushdown.clone(),
            supported_operators: descriptor.predicate_operators.clone(),
        },
        limits: CapabilitySupport::Unsupported,
        ordering: CapabilitySupport::Unsupported,
        partitioning: PartitioningCapabilities {
            parallel_partitions: true,
            supported_scopes: vec![ScopeKind::File],
        },
        incremental: IncrementalShape::File,
        replay: ReplaySupport::ExactRecordedBatches,
        idempotent_reads: true,
        backpressure: BackpressureSupport::Pausable,
        estimates: EstimateSupport::Bytes,
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
    exact_ranges: bool,
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
    source_io: SourceIoObserver,
    extraction_content_hash: Option<SourceContentDigest>,
    hash_sweep_source: Option<Arc<dyn ByteSource>>,
    payload_retention: Option<PayloadRetention>,
    payload_cache_key: Option<crate::payload_cache::FilePayloadCacheKey>,
}

fn retains_sequential_discovery_payload(
    descriptor: &cdf_runtime::FormatDriverDescriptor,
    discovery_kind: cdf_runtime::FormatDiscoveryKind,
) -> bool {
    descriptor.source_access == cdf_runtime::FormatSourceAccess::Sequential
        && matches!(
            discovery_kind,
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
    sha256: Option<String>,
    cache_staged: bool,
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

    fn supports_local_range_replay(&self) -> bool {
        self.inner.supports_local_range_replay()
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

    fn release_before(&self, frontier: u64) -> Result<()> {
        self.inner.release_before(frontier)
    }
}

impl AccountedSpool {
    fn path(&self) -> &Path {
        self.file.path()
    }

    fn bytes(&self) -> u64 {
        self.bytes
    }

    fn sha256(&self) -> Option<&str> {
        self.sha256.as_deref()
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
            sha256: None,
            cache_staged: false,
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
    scan_intent: CompiledScanIntent,
    options: ReadOptions,
    admission_schema: SchemaRef,
    physical_schema_authority: PhysicalSchemaAuthority,
    canonical_format_options: serde_json::Value,
    driver: Arc<dyn FormatDriver>,
    source_io: SourceIoObserver,
    extraction_content_hash: Option<SourceContentDigest>,
    hash_sweep_source: Option<Arc<dyn ByteSource>>,
    payload_retention: Option<PayloadRetention>,
    payload_cache_key: Option<crate::payload_cache::FilePayloadCacheKey>,
    spool_mode: crate::FileSpoolMode,
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
    let transform = file_transform_identity(input.transform_name, dependencies)?;
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

fn file_transform_identity(
    transform_name: &str,
    dependencies: &FileRuntimeDependencies,
) -> Result<serde_json::Value> {
    Ok(if transform_name == "none" {
        serde_json::json!({"id": "none", "version": "none"})
    } else {
        let transform = dependencies.transforms().resolve_name(transform_name)?;
        serde_json::json!({
            "id": transform.descriptor().transform_id.as_str(),
            "version": transform.descriptor().semantic_version,
        })
    })
}

fn file_payload_cache_key(
    resolved: &ResolvedFileMatch,
    dependencies: &FileRuntimeDependencies,
) -> Result<crate::payload_cache::FilePayloadCacheKey> {
    let transform = file_transform_identity(resolved.compression.mode_name(), dependencies)?;
    crate::payload_cache::FilePayloadCacheKey::new(cdf_runtime::artifact_hash(
        &serde_json::json!({
            "version": 1,
            "location": &resolved.path_text,
            "size_bytes": resolved.size_bytes,
            "source_generation": &resolved.source_generation,
            "etag": &resolved.etag,
            "object_version": &resolved.version,
            "sha256": &resolved.sha256,
            "transform": transform,
        }),
    )?)
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

#[derive(Clone)]
struct FilePlanningContext<'a> {
    transport: &'a dyn FileTransport,
    egress: &'a SourceEgressScope,
    formats: &'a FormatRegistry,
    transforms: &'a ByteTransformRegistry,
    maximum_matches: usize,
    control: &'a FileTransportControl,
    execution: ExecutionServices,
}

#[derive(Clone, Copy)]
struct FileResolutionContext<'a> {
    transport: &'a dyn FileTransport,
    egress: &'a SourceEgressScope,
    formats: &'a FormatRegistry,
    transforms: &'a ByteTransformRegistry,
    control: &'a FileTransportControl,
}

struct FilePartitionPreparation<'a> {
    admission_schema: SchemaRef,
    dependencies: &'a FileRuntimeDependencies,
    effective_schema_runtime: Option<&'a EffectiveSchemaRuntime>,
    compiled_format: &'a CompiledFormatBinding,
    control: &'a FileTransportControl,
}

fn file_partitions_for_plan_with_transport(
    descriptor: &ResourceDescriptor,
    plan: &FileResourcePlan,
    scan_intent: &CompiledScanIntent,
    context: FilePlanningContext<'_>,
) -> Result<Vec<PartitionPlan>> {
    let matches = resolve_file_matches_bounded(&descriptor.resource_id, plan, context)?;
    if matches.is_empty() {
        return Err(no_file_matches_error(&descriptor.resource_id, plan));
    }

    let total_matches = matches.len();
    matches
        .iter()
        .map(|file| partition_for_file_match(descriptor, plan, scan_intent, file, total_matches))
        .collect()
}

fn open_file_resource_with_dependencies(
    resource: FileResource,
    partition: PartitionPlan,
) -> cdf_kernel::PartitionOpenAttempt<'static> {
    let FileResource {
        descriptor,
        schema,
        capabilities: _,
        plan,
        type_policy_allowances: _,
        effective_schema_runtime,
        compiled_format,
        dependencies,
        prepared_inventory_key: _,
        compiled_source_plan_hash: _,
        transport_control: _,
    } = resource;
    if let Err(error) = validate_partition_plan_shape(&descriptor, &plan, &partition) {
        return cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async move { Err(error) }));
    }
    let execution = dependencies.execution().clone();
    if let Err(error) = execution.ensure_blocking_lanes(&[file_source_blocking_lane()]) {
        return cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async move { Err(error) }));
    }
    let (completion_sender, completion_receiver) = tokio::sync::oneshot::channel();
    let mut scope_hasher = Sha256::new();
    scope_hasher.update(descriptor.resource_id.as_str().as_bytes());
    scope_hasher.update([0]);
    scope_hasher.update(partition.partition_id.as_str().as_bytes());
    let scope_id = format!("file-open-{}", &hex::encode(scope_hasher.finalize())[..16]);
    let prepare_dependencies = dependencies.clone();
    let stream_dependencies = dependencies;
    let stream = execution.spawn_blocking_prepared_io_stream(
        &scope_id,
        FILE_SOURCE_BLOCKING_LANE_ID,
        NATIVE_STREAM_ITEMS,
        move |cancellation| {
            cancellation.check()?;
            let control = FileTransportControl::new(cancellation.clone(), None);
            let prepared = prepare_file_partition(
                &descriptor,
                &plan,
                &partition,
                FilePartitionPreparation {
                    admission_schema: schema,
                    dependencies: &prepare_dependencies,
                    effective_schema_runtime: effective_schema_runtime.as_deref(),
                    compiled_format: &compiled_format,
                    control: &control,
                },
            )?;
            cancellation.check()?;
            Ok(prepared)
        },
        move |prepared, mut sender, cancellation| async move {
            let source_io = prepared.source_io.clone();
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
            let decode = async {
                let prepared_stream = stream_prepared_file_match(
                    prepared,
                    &stream_dependencies,
                    cancellation.clone(),
                )
                .await?;
                let PreparedFormatStream {
                    mut batches,
                    source_completion,
                    post_decode_completion,
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
                if let Some(post_decode_completion) = post_decode_completion {
                    post_decode_completion.await?;
                }
                Ok::<_, CdfError>(())
            };
            let hash_sweep = complete_hash_sweep(hash_sweep_source, cancellation.clone());
            tokio::try_join!(decode, hash_sweep)?;
            let mut completed_position = completed_position;
            if let Some(extraction_content_hash) = extraction_content_hash {
                completed_position.sha256 = Some(extraction_content_hash.completed()?);
            }
            let attestation = Some(PartitionAttestation::new(
                SourcePosition::FileManifest(cdf_kernel::FileManifest {
                    version: 1,
                    files: vec![completed_position],
                }),
                None,
            ));
            let completion = PartitionCompletion::new(attestation, Some(source_io.snapshot()));
            let _ = completion_sender.send(completion);
            Ok(())
        },
    );
    let stream = match stream {
        Ok(stream) => stream,
        Err(error) => {
            return cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(
                async move { Err(error) },
            ));
        }
    };
    let termination = stream.termination();
    let opening = Box::pin(async move {
        let stream = Box::pin(stream) as BatchStream;
        let completion = Box::pin(async move {
            completion_receiver.await.map_err(|_| {
                CdfError::internal(
                    "partition stream ended without publishing its invocation completion",
                )
            })
        });
        Ok(cdf_kernel::PartitionStreamPayload::new(stream, completion))
    });
    cdf_kernel::PartitionOpenAttempt::with_termination(opening, termination)
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
    preparation: FilePartitionPreparation<'_>,
) -> Result<PreparedFilePartition> {
    partition.scan_intent.validate()?;
    let resolved = preparation
        .dependencies
        .with_transport(|transport, egress| {
            validate_partition(
                descriptor,
                plan,
                partition,
                FileResolutionContext {
                    transport,
                    egress,
                    formats: preparation.dependencies.formats(),
                    transforms: preparation.dependencies.transforms(),
                    control: preparation.control,
                },
            )
        })?;
    let planned_physical_schema_hash = preparation
        .effective_schema_runtime
        .and_then(|runtime| {
            partition
                .metadata
                .get(PLAN_SCHEMA_OBSERVATION_ID_KEY)
                .and_then(|observation_id| runtime.evidence.observation(observation_id))
        })
        .map(|observation| observation.physical_schema_hash.clone());
    let planned_physical_schema = planned_physical_schema_hash
        .as_ref()
        .and_then(|hash| {
            preparation
                .effective_schema_runtime
                .and_then(|runtime| runtime.physical_schema(hash))
        })
        .cloned();
    let options = ReadOptions::new(
        descriptor.resource_id.clone(),
        partition.partition_id.clone(),
    );
    let driver = preparation
        .compiled_format
        .verify(preparation.dependencies.formats())?;
    let source_access = driver.descriptor().source_access;
    let access_coverage =
        planned_file_access_coverage(&partition.scan_intent, &preparation.admission_schema);
    let prepared_input = prepare_file_input(PrepareFileInputRequest {
        resource_id: &descriptor.resource_id,
        resolved: &resolved,
        source_access,
        access_coverage,
        driver: driver.as_ref(),
        canonical_format_options: &preparation.compiled_format.canonical_options,
        dependencies: preparation.dependencies,
        cancellation: &preparation.control.cancellation(),
    })?;
    Ok(PreparedFilePartition {
        resolved,
        input: prepared_input.input,
        scan_intent: partition.scan_intent.clone(),
        options,
        admission_schema: preparation.admission_schema,
        physical_schema_authority: PhysicalSchemaAuthority {
            hash: planned_physical_schema_hash,
            schema: planned_physical_schema,
        },
        canonical_format_options: preparation.compiled_format.canonical_options.clone(),
        driver,
        source_io: prepared_input.source_io,
        extraction_content_hash: prepared_input.extraction_content_hash,
        hash_sweep_source: prepared_input.hash_sweep_source,
        payload_retention: prepared_input.payload_retention,
        payload_cache_key: prepared_input.payload_cache_key,
        spool_mode: plan.spool_mode,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PlannedFileAccessCoverage {
    Full,
    Selective,
}

fn planned_file_access_coverage(
    scan_intent: &CompiledScanIntent,
    admission_schema: &Schema,
) -> PlannedFileAccessCoverage {
    // Exact ranges are worthwhile only when the compiled projection proves that
    // at least one physical root can be omitted. Predicate selectivity is unknown
    // without recorded statistics, so predicate-only scans retain sequential spool.
    if scan_intent
        .projection
        .as_ref()
        .is_some_and(|projection| projection.len() < admission_schema.fields().len())
    {
        PlannedFileAccessCoverage::Selective
    } else {
        PlannedFileAccessCoverage::Full
    }
}

struct PrepareFileInputRequest<'a> {
    resource_id: &'a ResourceId,
    resolved: &'a ResolvedFileMatch,
    source_access: cdf_runtime::FormatSourceAccess,
    access_coverage: PlannedFileAccessCoverage,
    driver: &'a dyn FormatDriver,
    canonical_format_options: &'a serde_json::Value,
    dependencies: &'a FileRuntimeDependencies,
    cancellation: &'a cdf_runtime::RunCancellation,
}

fn prepare_file_input(request: PrepareFileInputRequest<'_>) -> Result<PreparedInput> {
    let PrepareFileInputRequest {
        resource_id,
        resolved,
        source_access,
        access_coverage,
        driver,
        canonical_format_options,
        dependencies,
        cancellation,
    } = request;
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
    let payload_cache_key = (dependencies.payload_cache().is_some()
        && matches!(resolved.open, ResolvedFileOpen::Transport(_))
        && resolved.identity_strength != GenerationStrength::Weak
        && resolved.compression.transform_id.is_none()
        && source_access == cdf_runtime::FormatSourceAccess::Adaptive
        && access_coverage == PlannedFileAccessCoverage::Full
        && dependencies
            .payload_cache()
            .is_some_and(|cache| resolved.size_bytes <= cache.policy().maximum_bytes))
    .then(|| file_payload_cache_key(resolved, dependencies))
    .transpose()?;
    if let Some(payload) = dependencies
        .prepared_payloads()
        .take(&prepared_payload_key)?
    {
        let (payload, retention) =
            payload.into_typed::<PreparedFilePayload>("file source execution")?;
        let observed = Arc::new(ObservedByteSource::new(payload.source));
        let source_io = observed.observer();
        return Ok(PreparedInput {
            input: PreparedFileInput::Source(observed),
            source_io,
            extraction_content_hash: payload.source_content_digest,
            hash_sweep_source: None,
            payload_retention: Some(retention),
            payload_cache_key: None,
        });
    }
    if let (Some(cache), Some(cache_key)) = (dependencies.payload_cache(), &payload_cache_key) {
        match cache.lookup(
            cache_key,
            &resolved.path_text,
            resolved.size_bytes,
            cancellation,
            dependencies.execution().memory(),
        ) {
            Ok(crate::payload_cache::FilePayloadCacheLookup::Hit(hit)) => {
                let observed = Arc::new(ObservedByteSource::new(hit.source));
                let source_io = observed.observer();
                source_io.set_mode(SourceReadMode::PayloadCache)?;
                return Ok(PreparedInput {
                    input: PreparedFileInput::Source(observed),
                    source_io,
                    extraction_content_hash: None,
                    hash_sweep_source: None,
                    payload_retention: Some(hit.retention),
                    payload_cache_key: None,
                });
            }
            Ok(crate::payload_cache::FilePayloadCacheLookup::Miss) => {}
            Err(error) if cancellation.is_cancelled() => return Err(error),
            Err(_) => {}
        }
    }
    if resolved.compression.transform_id.is_none() {
        let opened = open_file_byte_source(resolved, dependencies)?;
        let source = opened.source;
        let extraction_content_hash = opened.content_digest;
        let transport_spool = matches!(resolved.open, ResolvedFileOpen::Transport(_))
            && source_access == cdf_runtime::FormatSourceAccess::Adaptive
            && (access_coverage == PlannedFileAccessCoverage::Full
                || source.identity().strength == GenerationStrength::Weak
                || !source.capabilities().exact_ranges);
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
            source_io: opened.observer,
            extraction_content_hash,
            hash_sweep_source,
            payload_retention: None,
            payload_cache_key,
        });
    }
    if let Some(transform_id) = &resolved.compression.transform_id {
        let opened = open_file_byte_source(resolved, dependencies)?;
        let transformed = transformed_byte_source(opened.source, transform_id, dependencies)?;
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
            source_io: opened.observer,
            extraction_content_hash: opened.content_digest,
            hash_sweep_source: None,
            payload_retention: None,
            payload_cache_key,
        });
    }
    Err(CdfError::internal(
        "file preparation reached an unclassified compression state",
    ))
}

struct OpenedFileByteSource {
    source: Arc<dyn ByteSource>,
    observer: SourceIoObserver,
    content_digest: Option<SourceContentDigest>,
}

fn open_file_byte_source(
    resolved: &ResolvedFileMatch,
    dependencies: &FileRuntimeDependencies,
) -> Result<OpenedFileByteSource> {
    let raw: Arc<dyn ByteSource> = match &resolved.open {
        ResolvedFileOpen::LocalPath(path) => {
            let local: Arc<dyn ByteSource> = Arc::new(LocalByteSource::open(
                path,
                dependencies.execution().memory(),
            )?);
            verify_opened_local_generation(resolved, local.as_ref())?;
            local
        }
        ResolvedFileOpen::Transport(resource) => {
            let expected = expected_file_identity(resolved);
            dependencies.with_transport(|transport, egress| {
                transport.open_byte_source(
                    egress,
                    resource,
                    &expected,
                    dependencies.execution().memory(),
                )
            })?
        }
    };
    let observed = Arc::new(ObservedByteSource::new(raw));
    let observer = observed.observer();
    let requires_content_digest = matches!(resolved.open, ResolvedFileOpen::LocalPath(_))
        || resolved.identity_strength == GenerationStrength::Weak;
    let (source, content_digest): (Arc<dyn ByteSource>, Option<SourceContentDigest>) =
        if requires_content_digest {
            let digest = SourceContentDigest::default();
            (
                Arc::new(HashingByteSource::new(observed, digest.clone())),
                Some(digest),
            )
        } else {
            (observed, None)
        };
    Ok(OpenedFileByteSource {
        source,
        observer,
        content_digest,
    })
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
        exact_ranges: resolved.exact_ranges,
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
        scan_intent,
        options,
        admission_schema,
        physical_schema_authority,
        canonical_format_options,
        driver,
        source_io,
        extraction_content_hash: _,
        hash_sweep_source: _,
        payload_retention,
        payload_cache_key,
        spool_mode,
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
        post_decode_completion,
    } = match prepared {
        PreparedFileInput::Source(source) => ReadyFileInput {
            source,
            payload_retention,
            source_completion: None,
            post_decode_completion: None,
        },
        PreparedFileInput::SpoolSource { source, size_bytes } => {
            if payload_retention.is_some() {
                return Err(CdfError::internal(
                    "prepared source payload cannot request a second spool",
                ));
            }
            ready_spooled_file_input(SpoolInputRequest {
                source,
                size_bytes,
                mode: spool_mode,
                source_io,
                payload_cache_key,
                dependencies,
                cancellation,
            })
            .await?
        }
    };

    let batches = stream_registered_format(
        RegisteredFormatStreamRequest {
            source,
            payload_retention,
            driver,
            scan_intent,
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
        post_decode_completion,
    })
}

struct SpoolInputRequest<'a> {
    source: Arc<dyn ByteSource>,
    size_bytes: Option<u64>,
    mode: crate::FileSpoolMode,
    source_io: SourceIoObserver,
    payload_cache_key: Option<crate::payload_cache::FilePayloadCacheKey>,
    dependencies: &'a FileRuntimeDependencies,
    cancellation: cdf_runtime::RunCancellation,
}

async fn ready_spooled_file_input(request: SpoolInputRequest<'_>) -> Result<ReadyFileInput> {
    let SpoolInputRequest {
        source,
        size_bytes,
        mode,
        source_io,
        payload_cache_key,
        dependencies,
        cancellation,
    } = request;
    let cache_staging_root = payload_cache_key
        .as_ref()
        .and_then(|_| dependencies.payload_cache())
        .map(crate::FilePayloadCache::staging_root);
    let strong_seekable = size_bytes.is_some()
        && source.identity().strength != GenerationStrength::Weak
        && source.capabilities().exact_ranges;
    if strong_seekable && mode == crate::FileSpoolMode::Complete {
        let spool = spool_byte_source_async(
            Arc::clone(&source),
            size_bytes,
            cache_staging_root.as_deref(),
            dependencies,
            cancellation.clone(),
        )
        .await?;
        source_io.set_mode(SourceReadMode::FullSpool)?;
        return ready_materialized_spool(
            spool,
            source.identity().clone(),
            payload_cache_key,
            dependencies,
            cancellation,
        );
    }

    if let Some(size_bytes) = size_bytes
        && strong_seekable
    {
        let growing = start_growing_spool(
            Arc::clone(&source),
            size_bytes,
            dependencies.max_spool_bytes(),
            dependencies.execution().spill(),
            dependencies.execution().memory(),
            cache_staging_root.as_deref(),
            cancellation.clone(),
        )?;
        if let Some(growing) = growing {
            source_io.set_mode(SourceReadMode::GrowingSpool)?;
            let source_identity = growing.source.identity().clone();
            let observed_sha256 = growing
                .cache_staged
                .then(|| Arc::new(std::sync::Mutex::new(None)));
            let completion = growing_spool_completion(growing.completion, observed_sha256.clone());
            let post_decode_completion = match (
                growing.cache_staged,
                dependencies.payload_cache().cloned(),
                payload_cache_key,
                observed_sha256,
            ) {
                (true, Some(cache), Some(cache_key), Some(observed_sha256)) => {
                    Some(payload_cache_post_decode_completion(
                        PayloadCachePromotionRequest {
                            spool_path: growing.spool_path,
                            identity: source_identity,
                            size_bytes,
                            sha256: None,
                            cache,
                            cache_key,
                            execution: dependencies.execution().clone(),
                            cancellation: cancellation.clone(),
                            _retention: growing.retention.clone(),
                        },
                        observed_sha256,
                    ))
                }
                _ => None,
            };
            return Ok(ReadyFileInput {
                source: growing.source,
                payload_retention: Some(growing.retention),
                source_completion: Some(completion),
                post_decode_completion,
            });
        }
        let evicting = start_evicting_spool(
            Arc::clone(&source),
            size_bytes,
            dependencies.max_spool_bytes(),
            dependencies.execution().spill(),
            dependencies.execution().memory(),
            cancellation,
        )?;
        if let Some(evicting) = evicting {
            source_io.set_mode(SourceReadMode::EvictingSpool)?;
            return Ok(ReadyFileInput {
                source: evicting.source,
                payload_retention: Some(evicting.retention),
                source_completion: Some(evicting.completion),
                post_decode_completion: None,
            });
        }
        source_io.set_mode(SourceReadMode::ExactRanges)?;
        return Ok(ReadyFileInput {
            source,
            payload_retention: None,
            source_completion: None,
            post_decode_completion: None,
        });
    }

    source_io.set_mode(SourceReadMode::FullSpool)?;
    let source_identity = source.identity().clone();
    let spool = spool_byte_source_async(
        source,
        size_bytes,
        cache_staging_root.as_deref(),
        dependencies,
        cancellation.clone(),
    )
    .await?;
    ready_materialized_spool(
        spool,
        source_identity,
        payload_cache_key,
        dependencies,
        cancellation,
    )
}

fn ready_materialized_spool(
    spool: AccountedSpool,
    source_identity: ContentIdentity,
    payload_cache_key: Option<crate::payload_cache::FilePayloadCacheKey>,
    dependencies: &FileRuntimeDependencies,
    cancellation: cdf_runtime::RunCancellation,
) -> Result<ReadyFileInput> {
    let spool = Arc::new(spool);
    let materialized_identity = materialized_spool_identity(source_identity, spool.bytes())?;
    let retention = retain_spool(&spool, spool.bytes())?;
    let cache_promotion = match (
        spool.cache_staged,
        dependencies.payload_cache().cloned(),
        payload_cache_key,
        spool.sha256(),
    ) {
        (true, Some(cache), Some(cache_key), Some(sha256)) => {
            Some(payload_cache_promotion_completion(
                PayloadCachePromotionRequest {
                    spool_path: spool.path().to_path_buf(),
                    identity: materialized_identity.clone(),
                    size_bytes: spool.bytes(),
                    sha256: None,
                    cache,
                    cache_key,
                    execution: dependencies.execution().clone(),
                    cancellation,
                    _retention: retention.clone(),
                }
                .with_sha256(sha256.to_owned()),
            ))
        }
        _ => None,
    };
    let local = crate::local_byte_source::open_identity_preserving_local_source(
        spool.path(),
        materialized_identity,
        spool.bytes(),
        dependencies.execution().memory(),
    )?;
    Ok(ReadyFileInput {
        source: local,
        payload_retention: Some(retention),
        source_completion: None,
        post_decode_completion: cache_promotion,
    })
}

fn materialized_spool_identity(
    mut identity: ContentIdentity,
    size_bytes: u64,
) -> Result<ContentIdentity> {
    identity.size_bytes = Some(size_bytes);
    identity.validate()?;
    Ok(identity)
}

fn growing_spool_completion(
    completion: BoxFuture<'static, Result<Option<String>>>,
    observed_sha256: Option<Arc<std::sync::Mutex<Option<String>>>>,
) -> BoxFuture<'static, Result<()>> {
    Box::pin(async move {
        let sha256 = completion.await?;
        if let Some(observed_sha256) = observed_sha256 {
            *observed_sha256.lock().map_err(|_| {
                CdfError::internal("growing spool content hash authority was poisoned")
            })? = sha256;
        }
        Ok(())
    })
}

struct PayloadCachePromotionRequest {
    spool_path: PathBuf,
    identity: ContentIdentity,
    size_bytes: u64,
    sha256: Option<String>,
    cache: crate::FilePayloadCache,
    cache_key: crate::payload_cache::FilePayloadCacheKey,
    execution: ExecutionServices,
    cancellation: cdf_runtime::RunCancellation,
    _retention: PayloadRetention,
}

impl PayloadCachePromotionRequest {
    fn with_sha256(mut self, sha256: String) -> Self {
        self.sha256 = Some(sha256);
        self
    }
}

fn payload_cache_promotion_completion(
    request: PayloadCachePromotionRequest,
) -> BoxFuture<'static, Result<()>> {
    Box::pin(promote_payload_cache(request))
}

fn payload_cache_post_decode_completion(
    mut request: PayloadCachePromotionRequest,
    observed_sha256: Arc<std::sync::Mutex<Option<String>>>,
) -> BoxFuture<'static, Result<()>> {
    Box::pin(async move {
        request.sha256 = observed_sha256
            .lock()
            .map_err(|_| CdfError::internal("growing spool content hash authority was poisoned"))?
            .take();
        promote_payload_cache(request).await
    })
}

async fn promote_payload_cache(mut request: PayloadCachePromotionRequest) -> Result<()> {
    request.identity = materialized_spool_identity(request.identity, request.size_bytes)?;
    let PayloadCachePromotionRequest {
        spool_path,
        identity,
        size_bytes,
        sha256,
        cache,
        cache_key,
        execution,
        cancellation,
        _retention,
    } = request;
    let Some(sha256) = sha256 else {
        return Ok(());
    };
    cancellation.check()?;
    let operation_cancellation = cancellation.clone();
    let task = match execution.spawn_blocking_value(
        "file-payload-cache-promotion",
        FILE_SOURCE_BLOCKING_LANE_ID,
        move |task_cancellation| {
            operation_cancellation.check()?;
            task_cancellation.check()?;
            cache.promote(
                &cache_key,
                &spool_path,
                identity,
                size_bytes,
                &sha256,
                &task_cancellation,
            )
        },
    ) {
        Ok(task) => task,
        Err(error) if cancellation.is_cancelled() => return Err(error),
        Err(_) => return Ok(()),
    };
    match task.await {
        Ok(_) => Ok(()),
        Err(error) if cancellation.is_cancelled() => Err(error),
        Err(_) => Ok(()),
    }
}

struct PreparedFormatStream {
    batches: BatchStream,
    source_completion: Option<BoxFuture<'static, Result<()>>>,
    post_decode_completion: Option<BoxFuture<'static, Result<()>>>,
}

struct ReadyFileInput {
    source: Arc<dyn ByteSource>,
    payload_retention: Option<PayloadRetention>,
    source_completion: Option<BoxFuture<'static, Result<()>>>,
    post_decode_completion: Option<BoxFuture<'static, Result<()>>>,
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
    stream_file_match_with_options_blocking(
        resolved,
        declaration,
        serde_json::json!({}),
        options,
        dependencies,
        admission_schema,
        physical_schema_authority,
    )
}

#[cfg(test)]
fn stream_file_match_with_options_blocking(
    resolved: &ResolvedFileMatch,
    declaration: &FileFormatDeclaration,
    format_options: serde_json::Value,
    options: ReadOptions,
    dependencies: &FileRuntimeDependencies,
    admission_schema: SchemaRef,
    physical_schema_authority: PhysicalSchemaAuthority,
) -> Result<BatchStream> {
    let driver = dependencies.formats().resolve(declaration.as_str())?;
    let canonical_format_options = driver.canonical_options(format_options)?;
    let prepared_input = prepare_file_input(PrepareFileInputRequest {
        resource_id: &options.resource_id,
        resolved,
        source_access: driver.descriptor().source_access,
        access_coverage: PlannedFileAccessCoverage::Full,
        driver: driver.as_ref(),
        canonical_format_options: &canonical_format_options,
        dependencies,
        cancellation: &cdf_runtime::RunCancellation::default(),
    })?;
    let prepared = PreparedFilePartition {
        resolved: resolved.clone(),
        input: prepared_input.input,
        scan_intent: CompiledScanIntent::full_scan(),
        options,
        admission_schema,
        physical_schema_authority,
        canonical_format_options,
        driver,
        source_io: prepared_input.source_io,
        extraction_content_hash: prepared_input.extraction_content_hash,
        hash_sweep_source: prepared_input.hash_sweep_source,
        payload_retention: prepared_input.payload_retention,
        payload_cache_key: prepared_input.payload_cache_key,
        spool_mode: crate::FileSpoolMode::Overlap,
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
        post_decode_completion,
    } = prepared_stream;
    if source_completion.is_none() && post_decode_completion.is_none() {
        return Ok(batches);
    }
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
            if let Some(source_completion) = source_completion {
                tokio::try_join!(forward, source_completion)?;
            } else {
                forward.await?;
            }
            if let Some(post_decode_completion) = post_decode_completion {
                post_decode_completion.await?;
            }
            Ok(())
        },
    )?;
    Ok(Box::pin(stream))
}

async fn spool_byte_source_async(
    source: Arc<dyn ByteSource>,
    size_bytes: Option<u64>,
    cache_staging_root: Option<&Path>,
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
    let (file, cache_staged) = if let Some(staging_root) = cache_staging_root {
        match tempfile::NamedTempFile::new_in(staging_root) {
            Ok(file) => (file, true),
            Err(_) => (
                tempfile::NamedTempFile::new().map_err(|error| {
                    CdfError::data(format!("create accounted file spool: {error}"))
                })?,
                false,
            ),
        }
    } else {
        (
            tempfile::NamedTempFile::new()
                .map_err(|error| CdfError::data(format!("create accounted file spool: {error}")))?,
            false,
        )
    };
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
    let mut hasher = (expected_checksum.is_some() || cache_staged).then(Sha256::new);
    while let Some(chunk) = cancellation.await_or_cancel(input.try_next()).await? {
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
    let observed_sha256 = hasher.map(|hasher| format!("sha256:{}", hex::encode(hasher.finalize())));
    if let (Some(expected), Some(observed)) =
        (expected_checksum.as_deref(), observed_sha256.as_deref())
    {
        let expected = expected.strip_prefix("sha256:").unwrap_or(expected);
        let observed = observed.strip_prefix("sha256:").unwrap_or(observed);
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
        sha256: observed_sha256,
        cache_staged,
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
    scan_intent: CompiledScanIntent,
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
        scan_intent,
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
    scan_intent.validate()?;
    let stream = execution.spawn_io_stream(
        &scope_id,
        NATIVE_STREAM_ITEMS,
        move |mut sender, cancellation| async move {
            let _payload_retention = payload_retention;
            let options_json = driver.canonical_options(canonical_format_options)?;
            let decode_cpu = driver.descriptor().decode_cpu.clone();
            let projection = physical_projection_names(
                admission_schema.as_ref(),
                scan_intent.projection.as_deref(),
            )?;
            let predicates = physical_predicates(
                admission_schema.as_ref(),
                &scan_intent.pushed_predicates(),
            )?;
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
                        projection: projection.clone(),
                        predicates: predicates.clone(),
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
            let no_lookback_frontiers = decode_unit_no_lookback_frontiers(&units)?;
            let memory_snapshot = memory.snapshot();
            let available_memory = memory_snapshot
                .budget_bytes
                .saturating_sub(memory_snapshot.current_bytes);
            let unit_jobs = usize::from(resolve_decode_unit_concurrency(
                &units,
                &unit_execution.capabilities(),
                &decode_cpu,
                available_memory,
                source.capabilities().useful_range_concurrency.max(1),
                NATIVE_TARGET_BATCH_BYTES,
                NATIVE_UNIT_BUFFERED_BATCHES,
            )?
            .jobs);

            let units = Arc::new(units);
            let unit_count = units.len();
            let opener_session = Arc::clone(&session);
            let opener_units = Arc::clone(&units);
            let opener_execution = unit_execution.clone();
            let opener_memory = Arc::clone(&memory);
            let opener_options = options.clone();
            let opener_schema = decode_schema.clone();
            let opener_position = source_position.clone();
            let opener_projection = projection.clone();
            let opener_predicates = predicates.clone();
            let opener_scope_prefix = unit_scope_prefix.clone();
            let opener_cpu = decode_cpu;
            let opener: CanonicalStreamOpener<Batch> = Box::new(move |ordinal| {
                let unit = opener_units.get(ordinal).cloned().ok_or_else(|| {
                    CdfError::internal("decode-unit frontier ordinal is outside its session")
                })?;
                let session = Arc::clone(&opener_session);
                let memory = Arc::clone(&opener_memory);
                let options = opener_options.clone();
                let schema = opener_schema.clone();
                let source_position = opener_position.clone();
                let projection = opener_projection.clone();
                let predicates = opener_predicates.clone();
                let work_execution = opener_execution.clone();
                let cpu = opener_cpu.clone();
                let unit_stream = opener_execution.spawn_cpu_stream(
                    &format!("{opener_scope_prefix}-unit-{ordinal:08}"),
                    cpu,
                    NATIVE_UNIT_STREAM_ITEMS,
                    move |mut unit_sender, unit_cancellation| async move {
                        let mut decoded = {
                            let _work = work_execution
                                .acquire_run_work(unit_cancellation.clone())
                                .await?;
                            session
                                .decode(PhysicalDecodeRequest {
                                    unit,
                                    resource_id: options.resource_id,
                                    partition_id: options.partition_id,
                                    batch_id_prefix: options.batch_id_prefix,
                                    schema,
                                    source_position,
                                    projection,
                                    predicates,
                                    target_batch_rows: NATIVE_TARGET_BATCH_ROWS,
                                    target_batch_bytes: NATIVE_TARGET_BATCH_BYTES,
                                    memory,
                                    cancellation: unit_cancellation.clone(),
                                })
                                .await?
                        };
                        loop {
                            let next = {
                                let _work = work_execution
                                    .acquire_run_work(unit_cancellation.clone())
                                    .await?;
                                decoded.try_next().await?
                            };
                            let Some(batch) = next else {
                                break;
                            };
                            // A run-work permit owns active leaf computation, not bounded-channel
                            // residence. Releasing it before publication prevents a later
                            // canonical unit from monopolizing every run slot while its output is
                            // intentionally held behind an earlier partition.
                            unit_sender.send(batch.into_batch()?).await?;
                        }
                        Ok(())
                    },
                )?;
                Ok(Box::pin(unit_stream))
            });
            let release_source = Arc::clone(&source);
            let release_frontiers = no_lookback_frontiers;
            let completion: CanonicalStreamCompletion = Box::new(move |ordinal| {
                if let Some(frontiers) = &release_frontiers {
                    let frontier = frontiers.get(ordinal).copied().ok_or_else(|| {
                        CdfError::internal("decode-unit release frontier ordinal is outside its session")
                    })?;
                    release_source.release_before(frontier)?;
                }
                Ok(())
            });
            let mut decoded = canonical_stream_frontier_with_completion(
                unit_count,
                unit_jobs,
                opener,
                completion,
            )?;
            while let Some(batch) = decoded.try_next().await? {
                cancellation.check()?;
                sender.send(batch).await?;
            }
            if let Some(size_bytes) = source.identity().size_bytes {
                source.release_before(size_bytes)?;
            }
            Ok(())
        },
    )?;
    Ok(Box::pin(stream))
}

fn physical_projection_names(
    effective_schema: &Schema,
    projection: Option<&[String]>,
) -> Result<Option<Vec<String>>> {
    projection
        .map(|fields| {
            fields
                .iter()
                .map(|logical_name| {
                    let field = effective_schema.field_with_name(logical_name).map_err(|_| {
                        CdfError::contract(format!(
                            "compiled file projection field {logical_name:?} is absent from the effective schema"
                        ))
                    })?;
                    Ok(source_name(field).unwrap_or_else(|| field.name()).to_owned())
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()
}

fn physical_predicates(
    effective_schema: &Schema,
    predicates: &[cdf_kernel::ScanPredicate],
) -> Result<Vec<cdf_kernel::ScanPredicate>> {
    predicates
        .iter()
        .map(|predicate| {
            let mut physical = predicate.clone();
            physical.canonical_expression = cdf_kernel::Expression::new(physical_expression_node(
                effective_schema,
                &predicate.canonical_expression.root,
            )?);
            physical.canonical_expression.validate()?;
            Ok(physical)
        })
        .collect()
}

fn physical_expression_node(
    effective_schema: &Schema,
    node: &ExpressionNode,
) -> Result<ExpressionNode> {
    match node {
        ExpressionNode::Column { name } => {
            let field = effective_schema.field_with_name(name).map_err(|_| {
                CdfError::contract(format!(
                    "compiled file predicate field {name:?} is absent from the effective schema"
                ))
            })?;
            Ok(ExpressionNode::Column {
                name: source_name(field)
                    .unwrap_or_else(|| field.name())
                    .to_owned(),
            })
        }
        ExpressionNode::Literal { value } => Ok(ExpressionNode::Literal {
            value: value.clone(),
        }),
        ExpressionNode::Call {
            function,
            arguments,
        } => Ok(ExpressionNode::Call {
            function: function.clone(),
            arguments: arguments
                .iter()
                .map(|argument| physical_expression_node(effective_schema, argument))
                .collect::<Result<Vec<_>>>()?,
        }),
        _ => Err(CdfError::contract(
            "compiled file predicate contains an unsupported expression node",
        )),
    }
}

fn validate_partition(
    descriptor: &ResourceDescriptor,
    plan: &FileResourcePlan,
    partition: &PartitionPlan,
    context: FileResolutionContext<'_>,
) -> Result<ResolvedFileMatch> {
    let (path, match_count) = validate_partition_plan_shape(descriptor, plan, partition)?;
    let resolved = resolve_planned_file_match(descriptor, plan, path, context)?;
    let planned = partition.planned_file()?.ok_or_else(|| {
        CdfError::contract(format!(
            "file partition `{}` omitted its typed planned position",
            partition.partition_id
        ))
    })?;
    let observed = cdf_kernel::FilePosition {
        path: resolved.path_text.clone(),
        size_bytes: resolved.size_bytes,
        source_generation: resolved.source_generation.clone(),
        etag: resolved.etag.clone(),
        object_version: resolved.version.clone(),
        sha256: resolved.sha256.clone(),
    };
    cdf_kernel::merge_file_position_evidence(planned, &observed)?;
    debug_assert!(match_count > 0);
    validate_resolved_partition_metadata(partition, &resolved, plan, path)?;
    Ok(resolved)
}

fn validate_partition_plan_shape<'a>(
    descriptor: &ResourceDescriptor,
    plan: &FileResourcePlan,
    partition: &'a PartitionPlan,
) -> Result<(&'a str, usize)> {
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
    let path = partition
        .planned_file()?
        .ok_or_else(|| {
            CdfError::contract(format!(
                "file partition `{}` omitted its typed planned position",
                partition.partition_id
            ))
        })?
        .path
        .as_str();
    let expected_scope = ScopeKey::File {
        path: path.to_owned(),
    };
    if partition.scope != expected_scope {
        return Err(CdfError::contract(format!(
            "declarative file partition scope does not match file path `{path}`",
        )));
    }
    let match_count = partition
        .metadata
        .get("match_count")
        .ok_or_else(|| CdfError::contract("file partition omitted planned match count"))?
        .parse::<usize>()
        .map_err(|_| CdfError::contract("file partition match count is invalid"))?;
    if match_count == 0 {
        return Err(CdfError::contract(
            "file partition match count must be greater than zero",
        ));
    }
    let expected_partition_id = if match_count == 1 {
        "files".to_owned()
    } else {
        file_partition_id(path)
    };
    if partition.partition_id.as_str() != expected_partition_id.as_str() {
        return Err(CdfError::contract(format!(
            "declarative file partition id `{}` does not match file path `{path}`",
            partition.partition_id
        )));
    }
    let expected_path_binding =
        planned_file_path_binding(&descriptor.resource_id, &plan.glob, path, match_count)?;
    if partition
        .metadata
        .get("plan_path_binding")
        .map(String::as_str)
        != Some(expected_path_binding.as_str())
    {
        return Err(CdfError::contract(format!(
            "declarative file partition path `{path}` was not produced by glob `{}` under `{}` or does not match its compiled plan binding",
            plan.glob, plan.root
        )));
    }
    Ok((path, match_count))
}

fn validate_resolved_partition_metadata(
    partition: &PartitionPlan,
    resolved: &ResolvedFileMatch,
    plan: &FileResourcePlan,
    path: &str,
) -> Result<()> {
    if resolved.identity_strength != GenerationStrength::Weak
        && resolved.sha256.is_none()
        && resolved.etag.is_none()
        && resolved.version.is_none()
        && resolved.source_generation.is_none()
    {
        return Err(CdfError::internal(format!(
            "declarative file partition `{path}` omitted generation evidence despite non-weak identity"
        )));
    }
    validate_partition_metadata_value(
        partition,
        "identity_strength",
        identity_strength_name(resolved.identity_strength),
        path,
    )?;
    validate_compression_metadata(partition, resolved, &plan.compression, path)?;
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
    Ok(())
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
    scan_intent: &CompiledScanIntent,
    file: &ResolvedFileMatch,
    total_matches: usize,
) -> Result<PartitionPlan> {
    let mut metadata = BTreeMap::new();
    metadata.insert("kind".to_owned(), "files".to_owned());
    metadata.insert("glob".to_owned(), plan.glob.clone());
    metadata.insert("resource_id".to_owned(), descriptor.resource_id.to_string());
    metadata.insert("match_count".to_owned(), total_matches.to_string());
    metadata.insert(
        "plan_path_binding".to_owned(),
        planned_file_path_binding(
            &descriptor.resource_id,
            &plan.glob,
            &file.path_text,
            total_matches,
        )?,
    );
    metadata.insert(
        PLAN_SCHEMA_OBSERVATION_ID_KEY.to_owned(),
        file.path_text.clone(),
    );
    metadata.insert(
        PLAN_SCHEMA_OBSERVATION_BINDING_KEY.to_owned(),
        file_schema_observation_binding(file),
    );
    metadata.insert(
        "identity_strength".to_owned(),
        identity_strength_name(file.identity_strength).to_owned(),
    );
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
        planned_position: Some(SourcePosition::FileManifest(cdf_kernel::FileManifest {
            version: 1,
            files: vec![cdf_kernel::FilePosition {
                path: file.path_text.clone(),
                size_bytes: file.size_bytes,
                source_generation: file.source_generation.clone(),
                etag: file.etag.clone(),
                object_version: file.version.clone(),
                sha256: file.sha256.clone(),
            }],
        })),
        start_position: None,
        scan_intent: scan_intent.clone(),
        retry_safety: match file.identity_strength {
            GenerationStrength::Weak => cdf_kernel::PartitionRetrySafety::Forbidden,
            GenerationStrength::Strong | GenerationStrength::ContentAddressed => {
                cdf_kernel::PartitionRetrySafety::ImmutableContent
            }
        },
        metadata,
    })
}

fn planned_file_path_binding(
    resource_id: &ResourceId,
    glob: &str,
    path: &str,
    match_count: usize,
) -> Result<String> {
    cdf_runtime::artifact_hash(&serde_json::json!({
        "resource_id": resource_id,
        "glob": glob,
        "path": path,
        "match_count": match_count,
    }))
}

/// Revalidates exactly one planned file without listing or resolving the resource glob again.
///
/// Planning owns enumeration. Open and attestation own generation validation for the selected
/// partition only, keeping N-file execution O(N) rather than O(N²).
fn resolve_planned_file_match(
    descriptor: &ResourceDescriptor,
    plan: &FileResourcePlan,
    path: &str,
    context: FileResolutionContext<'_>,
) -> Result<ResolvedFileMatch> {
    let components = pattern_components(&plan.glob)?;
    match file_transport_scheme(&plan.root)? {
        Some(FileTransportScheme::Http | FileTransportScheme::Https) => {
            let root_prefix = format!("{}/", plan.root.trim_end_matches('/'));
            let relative_path = path.strip_prefix(&root_prefix).ok_or_else(|| {
                CdfError::contract(format!(
                    "file partition `{path}` is outside HTTP root `{}`",
                    plan.root
                ))
            })?;
            let expected = http_glob_contains(&descriptor.resource_id, &plan.glob, relative_path)?;
            if !expected {
                return Err(CdfError::contract(format!(
                    "file partition `{path}` is outside the compiled HTTP enumeration"
                )));
            }
            let logical = FileTransportResource {
                location: FileTransportLocation::HttpUrl {
                    url: path.to_owned(),
                },
                egress_allowlist: plan.allowlist.clone(),
                auth: plan.auth.clone(),
                credentials: plan.credentials.clone(),
            };
            let observation =
                context
                    .transport
                    .metadata(context.egress, &logical, context.control)?;
            let compression = resolve_transport_compression(plan, path, context.transforms)?;
            let format = resolve_transport_format(
                &descriptor.resource_id,
                plan,
                path,
                &compression,
                context.formats,
            )?;
            resolved_transport_file_match(
                observation.access_resource(&logical),
                observation.into_identity(),
                compression,
                format,
            )
        }
        Some(FileTransportScheme::Remote(_)) => {
            let relative = remote_relative_path(&plan.root, path)?;
            if !glob_path_matches(&components, &relative) {
                return Err(CdfError::contract(format!(
                    "file partition `{path}` is outside glob `{}`",
                    plan.glob
                )));
            }
            let logical = FileTransportResource::remote_url(path.to_owned())
                .with_egress_allowlist(plan.allowlist.clone());
            let logical = match &plan.credentials {
                Some(credentials) => logical.with_credentials(credentials.clone()),
                None => logical,
            };
            let observation =
                context
                    .transport
                    .metadata(context.egress, &logical, context.control)?;
            let compression = resolve_transport_compression(plan, path, context.transforms)?;
            let format = resolve_transport_format(
                &descriptor.resource_id,
                plan,
                path,
                &compression,
                context.formats,
            )?;
            resolved_transport_file_match(
                observation.access_resource(&logical),
                observation.into_identity(),
                compression,
                format,
            )
        }
        Some(FileTransportScheme::File) => {
            let mut local_plan = plan.clone();
            local_plan.root = crate::transport::file_url_path(&plan.root)?
                .to_str()
                .map(str::to_owned)
                .ok_or_else(|| CdfError::data("file URL path is not valid UTF-8"))?;
            resolve_planned_local_file_match(
                descriptor,
                &local_plan,
                path,
                &components,
                context.formats,
                context.transforms,
            )
        }
        None => resolve_planned_local_file_match(
            descriptor,
            plan,
            path,
            &components,
            context.formats,
            context.transforms,
        ),
    }
}

fn resolve_planned_local_file_match(
    descriptor: &ResourceDescriptor,
    plan: &FileResourcePlan,
    path: &str,
    components: &[String],
    formats: &FormatRegistry,
    transforms: &ByteTransformRegistry,
) -> Result<ResolvedFileMatch> {
    if !glob_path_matches(components, path) {
        return Err(CdfError::contract(format!(
            "file partition `{path}` is outside glob `{}`",
            plan.glob
        )));
    }
    let root = PathBuf::from(&plan.root);
    if !root.is_absolute() {
        return Err(CdfError::contract(format!(
            "file source root `{}` for resource `{}` must be absolute before runtime open",
            plan.root, descriptor.resource_id
        )));
    }
    let canonical_root = fs::canonicalize(&root).map_err(|error| {
        CdfError::data(format!(
            "canonicalize file source root {}: {error}",
            root.display()
        ))
    })?;
    let candidate = fs::canonicalize(canonical_root.join(path)).map_err(|error| {
        CdfError::data(format!("resolve planned file partition `{path}`: {error}"))
    })?;
    if !candidate.starts_with(&canonical_root) {
        return Err(CdfError::contract(format!(
            "file partition `{path}` escapes its compiled source root"
        )));
    }
    resolved_file_match(
        &descriptor.resource_id,
        &canonical_root,
        candidate,
        plan,
        formats,
        transforms,
    )
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

#[cfg(test)]
fn resolve_file_matches(
    resource_id: &ResourceId,
    plan: &FileResourcePlan,
    transport: &dyn FileTransport,
    egress: &SourceEgressScope,
    formats: &FormatRegistry,
    transforms: &ByteTransformRegistry,
) -> Result<Vec<ResolvedFileMatch>> {
    resolve_file_matches_bounded(
        resource_id,
        plan,
        FilePlanningContext {
            transport,
            egress,
            formats,
            transforms,
            maximum_matches: usize::MAX,
            control: &FileTransportControl::default(),
            execution: crate::test_execution_services(),
        },
    )
}

fn resolve_file_matches_bounded(
    resource_id: &ResourceId,
    plan: &FileResourcePlan,
    context: FilePlanningContext<'_>,
) -> Result<Vec<ResolvedFileMatch>> {
    match file_transport_scheme(&plan.root)? {
        Some(FileTransportScheme::Http | FileTransportScheme::Https) => {
            if context.maximum_matches == 0 {
                return Err(CdfError::data(
                    "file inventory exceeds the 0-entry boundary",
                ));
            }
            return resolve_http_file_match(
                resource_id,
                plan,
                context.transport,
                context.egress,
                context.formats,
                context.transforms,
                context.control,
            );
        }
        Some(FileTransportScheme::Remote(_)) => {
            return resolve_remote_matches_bounded(resource_id, plan, context);
        }
        Some(FileTransportScheme::File) => {
            let mut local_plan = plan.clone();
            local_plan.root = crate::transport::file_url_path(&plan.root)?
                .to_str()
                .map(str::to_owned)
                .ok_or_else(|| CdfError::data("file URL path is not valid UTF-8"))?;
            return resolve_file_matches_bounded(resource_id, &local_plan, context);
        }
        None => {}
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
    let mut budget = LocalInventoryBudget::new(
        context.maximum_matches,
        context.control.clone(),
        context.execution.clone(),
    );
    collect_matches(&root, &components, &mut matches, &mut budget)?;
    matches.sort();
    matches.dedup();

    let matches = contained_matches(&root, matches)?;
    matches
        .into_iter()
        .map(|path| {
            resolved_file_match(
                resource_id,
                &root,
                path,
                plan,
                context.formats,
                context.transforms,
            )
        })
        .collect()
}

#[cfg(test)]
fn resolve_remote_matches(
    resource_id: &ResourceId,
    plan: &FileResourcePlan,
    transport: &dyn FileTransport,
    egress: &SourceEgressScope,
    formats: &FormatRegistry,
    transforms: &ByteTransformRegistry,
) -> Result<Vec<ResolvedFileMatch>> {
    resolve_remote_matches_bounded(
        resource_id,
        plan,
        FilePlanningContext {
            transport,
            egress,
            formats,
            transforms,
            maximum_matches: usize::MAX,
            control: &FileTransportControl::default(),
            execution: crate::test_execution_services(),
        },
    )
}

fn resolve_remote_matches_bounded(
    resource_id: &ResourceId,
    plan: &FileResourcePlan,
    context: FilePlanningContext<'_>,
) -> Result<Vec<ResolvedFileMatch>> {
    let root_resource = FileTransportResource::remote_url(plan.root.clone())
        .with_egress_allowlist(plan.allowlist.clone());
    let root_resource = match &plan.credentials {
        Some(credentials) => root_resource.with_credentials(credentials.clone()),
        None => root_resource,
    };
    let components = pattern_components(&plan.glob)?;
    let mut matches = Vec::new();
    let listing = context.transport.list(
        context.egress,
        &root_resource,
        context.maximum_matches,
        context.control,
    )?;
    let termination = listing.termination();
    let root = plan.root.clone();
    let components_for_listing = components.clone();
    let matched_metadata = context.execution.run_io(async move {
        let mut listing = listing;
        let mut matched = Vec::new();
        let result: Result<Vec<FileIdentityMetadata>> = async {
            while let Some(identity) = listing.try_next().await? {
                let metadata = identity.into_identity();
                let relative = remote_relative_path(&root, &metadata.location)?;
                if glob_path_matches(&components_for_listing, &relative) {
                    matched.push(metadata);
                }
            }
            termination.join().await?;
            Ok(matched)
        }
        .await;
        match result {
            Ok(matched) => Ok(matched),
            Err(mut error) => {
                if let Err(cleanup) = termination.terminate_and_join().await {
                    error.message = format!(
                        "{}; file listing termination also failed: {}",
                        error.message, cleanup.message
                    );
                }
                Err(error)
            }
        }
    })?;
    for metadata in matched_metadata {
        let resource = FileTransportResource::remote_url(metadata.location.clone())
            .with_egress_allowlist(plan.allowlist.clone());
        let resource = match &plan.credentials {
            Some(credentials) => resource.with_credentials(credentials.clone()),
            None => resource,
        };
        let compression =
            resolve_transport_compression(plan, &metadata.location, context.transforms)?;
        let format = resolve_transport_format(
            resource_id,
            plan,
            &metadata.location,
            &compression,
            context.formats,
        )?;
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

fn remote_relative_path(root: &str, location: &str) -> Result<String> {
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
    egress: &SourceEgressScope,
    formats: &FormatRegistry,
    transforms: &ByteTransformRegistry,
    control: &FileTransportControl,
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
        let Some(observation) = transport.metadata_if_exists(egress, &resource, control)? else {
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

struct LocalInventoryBudget {
    maximum_entries: usize,
    observed_entries: usize,
    control: FileTransportControl,
    execution: ExecutionServices,
}

impl LocalInventoryBudget {
    fn new(
        maximum_entries: usize,
        control: FileTransportControl,
        execution: ExecutionServices,
    ) -> Self {
        Self {
            maximum_entries,
            observed_entries: 0,
            control,
            execution,
        }
    }

    fn check(&self) -> Result<()> {
        self.control.check(Some(&self.execution))
    }

    fn observe_entry(&mut self) -> Result<()> {
        self.check()?;
        if self.observed_entries >= self.maximum_entries {
            return Err(CdfError::data(format!(
                "file inventory exceeds the {}-entry boundary",
                self.maximum_entries
            )));
        }
        self.observed_entries = self.observed_entries.saturating_add(1);
        Ok(())
    }

    fn admit_match(&self, matches: &[PathBuf]) -> Result<()> {
        self.check()?;
        if matches.len() >= self.maximum_entries {
            return Err(CdfError::data(format!(
                "file inventory exceeds the {}-entry boundary",
                self.maximum_entries
            )));
        }
        Ok(())
    }
}

fn collect_matches(
    current: &Path,
    components: &[String],
    matches: &mut Vec<PathBuf>,
    budget: &mut LocalInventoryBudget,
) -> Result<()> {
    budget.check()?;
    let Some((component, rest)) = components.split_first() else {
        return collect_leaf_match(current, matches, budget);
    };

    if component == "**" {
        return collect_recursive_matches(current, components, rest, matches, budget);
    }

    if has_wildcards(component) {
        return collect_wildcard_matches(current, component, rest, matches, budget);
    }

    collect_literal_matches(current, component, rest, matches, budget)
}

fn collect_leaf_match(
    current: &Path,
    matches: &mut Vec<PathBuf>,
    budget: &LocalInventoryBudget,
) -> Result<()> {
    if current.is_file() {
        budget.admit_match(matches)?;
        matches.push(current.to_path_buf());
    }
    Ok(())
}

fn collect_recursive_matches(
    current: &Path,
    components: &[String],
    rest: &[String],
    matches: &mut Vec<PathBuf>,
    budget: &mut LocalInventoryBudget,
) -> Result<()> {
    collect_matches(current, rest, matches, budget)?;
    for path in read_dir_paths(current, budget)? {
        if is_physical_dir(&path)? {
            collect_matches(&path, components, matches, budget)?;
        }
    }
    Ok(())
}

fn collect_wildcard_matches(
    current: &Path,
    component: &str,
    rest: &[String],
    matches: &mut Vec<PathBuf>,
    budget: &mut LocalInventoryBudget,
) -> Result<()> {
    for path in read_dir_paths(current, budget)? {
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if glob_component_matches(component, name) && can_descend_for_rest(&path, rest)? {
            collect_matches(&path, rest, matches, budget)?;
        }
    }
    Ok(())
}

fn collect_literal_matches(
    current: &Path,
    component: &str,
    rest: &[String],
    matches: &mut Vec<PathBuf>,
    budget: &mut LocalInventoryBudget,
) -> Result<()> {
    let next = current.join(component);
    if can_descend_for_rest(&next, rest)? {
        collect_matches(&next, rest, matches, budget)
    } else {
        Ok(())
    }
}

fn can_descend_for_rest(path: &Path, rest: &[String]) -> Result<bool> {
    Ok(rest.is_empty() || is_physical_dir(path)?)
}

fn read_dir_paths(path: &Path, budget: &mut LocalInventoryBudget) -> Result<Vec<PathBuf>> {
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

    let mut paths = Vec::new();
    for entry in entries {
        budget.observe_entry()?;
        paths.push(
            entry
                .map_err(|error| {
                    CdfError::data(format!(
                        "read file source directory {}: {error}",
                        path.display()
                    ))
                })?
                .path(),
        );
    }
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
    let magic_signal = local_compression_magic_signal(&path, metadata.len(), transforms)?;
    let compression =
        resolve_local_compression(&path_text, &plan.compression, magic_signal, transforms)?;
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
        exact_ranges: true,
        bytes_loaded: None,
        compression,
        format,
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
    let exact_ranges = metadata.exact_ranges;
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
        exact_ranges,
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
    let diagnostic = diagnostic_location(location)?;
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

fn diagnostic_location(location: &str) -> Result<String> {
    Ok(SourceEvidenceLocation::from_operational(location)?
        .as_str()
        .to_owned())
}

fn resolve_local_compression(
    path_text: &str,
    declared: &FileCompressionDeclaration,
    magic_signal: CompressionSignal,
    transforms: &ByteTransformRegistry,
) -> Result<CompressionEvidence> {
    let extension_signal = compression_extension_signal(path_text, transforms);
    resolve_compression_signals(
        path_text,
        declared,
        extension_signal,
        magic_signal,
        transforms,
    )
}

fn resolve_transport_compression(
    plan: &FileResourcePlan,
    location: &str,
    transforms: &ByteTransformRegistry,
) -> Result<CompressionEvidence> {
    let extension_signal = compression_extension_signal(location, transforms);
    let diagnostic = diagnostic_location(location)?;
    resolve_compression_signals(
        &diagnostic,
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
        match (extension_signal.transform_id(), magic_signal.transform_id()) {
            (Some(extension), Some(magic)) if extension != magic => {
                return Err(compression_signal_error(
                    path_text,
                    declared,
                    &extension_signal,
                    &magic_signal,
                ));
            }
            (_, Some(magic)) => Some(magic.clone()),
            (Some(extension), None) => Some(extension.clone()),
            (None, None) => None,
        }
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

fn local_compression_magic_signal(
    path: &Path,
    size_bytes: u64,
    transforms: &ByteTransformRegistry,
) -> Result<CompressionSignal> {
    let probe_bytes = transforms.maximum_strong_magic_probe_bytes()?;
    let probe_bytes = probe_bytes.min(size_bytes);
    if probe_bytes == 0 {
        return Ok(CompressionSignal::default());
    }
    let probe_bytes = usize::try_from(probe_bytes)
        .map_err(|_| CdfError::contract("byte-transform magic probe length exceeds usize"))?;
    let mut prefix = vec![0_u8; probe_bytes];
    let mut file = fs::File::open(path).map_err(|error| {
        CdfError::data(format!("open matched file {}: {error}", path.display()))
    })?;
    file.read_exact(&mut prefix).map_err(|error| {
        CdfError::data(format!(
            "read compression magic prefix for {}: {error}",
            path.display()
        ))
    })?;
    let Some(driver) = transforms.detect_strong_magic(&prefix)? else {
        return Ok(CompressionSignal::default());
    };
    Ok(CompressionSignal(Some(
        driver.descriptor().transform_id.clone(),
    )))
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
    let Some(template) = parse_http_numeric_template(resource_id, glob)? else {
        return Ok(vec![glob.to_owned()]);
    };
    let mut expanded = Vec::with_capacity(template.count as usize);
    for value in template.start..=template.end {
        expanded.push(template.render(value));
    }
    Ok(expanded)
}

fn http_glob_contains(resource_id: &ResourceId, glob: &str, candidate: &str) -> Result<bool> {
    if let Some(months) = expand_http_year_month_glob(glob) {
        return Ok(months.iter().any(|month| month == candidate));
    }
    let Some(template) = parse_http_numeric_template(resource_id, glob)? else {
        return Ok(glob == candidate);
    };
    let Some(value_text) = candidate
        .strip_prefix(template.prefix)
        .and_then(|candidate| candidate.strip_suffix(template.suffix))
    else {
        return Ok(false);
    };
    if value_text.is_empty() || !value_text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Ok(false);
    }
    let Ok(value) = value_text.parse::<u64>() else {
        return Ok(false);
    };
    Ok(value >= template.start
        && value <= template.end
        && template.render_value(value) == value_text)
}

struct HttpNumericTemplate<'a> {
    prefix: &'a str,
    suffix: &'a str,
    start: u64,
    end: u64,
    width: usize,
    count: u64,
}

impl HttpNumericTemplate<'_> {
    fn render_value(&self, value: u64) -> String {
        if self.width == 0 {
            value.to_string()
        } else {
            format!("{value:0width$}", width = self.width)
        }
    }

    fn render(&self, value: u64) -> String {
        format!("{}{}{}", self.prefix, self.render_value(value), self.suffix)
    }
}

fn parse_http_numeric_template<'a>(
    resource_id: &ResourceId,
    glob: &'a str,
) -> Result<Option<HttpNumericTemplate<'a>>> {
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
        return Ok(None);
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
    Ok(Some(HttpNumericTemplate {
        prefix: &glob[..open],
        suffix: &glob[close + 1..],
        start,
        end,
        width,
        count,
    }))
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
    use cdf_memory::MemoryCoordinator;
    use flate2::{Compression, write::GzEncoder};
    use object_store::{ObjectStoreExt, PutPayload, memory::InMemory, path::Path as ObjectPath};
    use parquet::arrow::ArrowWriter;
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn file_source_blocking_lane_matches_advertised_parallelism() {
        let lane = file_source_blocking_lane();
        assert_eq!(lane.lane_id, FILE_SOURCE_BLOCKING_LANE_ID);
        assert_eq!(lane.maximum_concurrency, FILE_SOURCE_ADVERTISED_PARALLELISM);
        assert_eq!(lane.cpu_slot_cost, 1);
        assert_eq!(lane.native_internal_parallelism, 1);
    }

    fn physical_runtime(
        descriptor: &ResourceDescriptor,
        effective_schema: SchemaRef,
        physical_schema: SchemaRef,
        observation_id: impl Into<String>,
    ) -> EffectiveSchemaRuntime {
        let effective_hash =
            cdf_kernel::canonical_arrow_schema_hash(effective_schema.as_ref()).unwrap();
        let physical_hash =
            cdf_kernel::canonical_arrow_schema_hash(physical_schema.as_ref()).unwrap();
        let evidence = cdf_kernel::EffectiveSchemaEvidence::new(
            descriptor.schema_source.baseline_reference().unwrap(),
            effective_hash,
            cdf_kernel::DiscoveryManifestReference {
                manifest_hash: cdf_kernel::DiscoveryManifestHash::new(
                    "test-exact-physical-manifest",
                )
                .unwrap(),
                path: ".cdf/discovery/test-exact-physical.json".to_owned(),
            },
            vec![cdf_kernel::EffectiveSchemaObservationEvidence::new(
                observation_id,
                physical_hash.clone(),
            )],
        )
        .unwrap();
        EffectiveSchemaRuntime::new(
            evidence,
            vec![cdf_kernel::EffectiveSchemaCatalogEntry::new(
                physical_hash,
                physical_schema,
            )],
        )
        .unwrap()
    }

    #[derive(Debug)]
    struct ExternalMockFormat {
        descriptor: cdf_runtime::FormatDriverDescriptor,
        batches_per_unit: usize,
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
                    predicate_operators: Vec::new(),
                    source_access: cdf_runtime::FormatSourceAccess::Sequential,
                    discovery: cdf_runtime::FormatDiscoveryCapabilities::only(
                        cdf_runtime::FormatDiscoveryKind::BoundedContent,
                    ),
                    decode_unit_policy: "whole_mock_file".to_owned(),
                    error_isolation: cdf_runtime::FormatErrorIsolation::DecodeUnit,
                    decode_cpu: cdf_runtime::CpuTaskSpec {
                        task_kind: "format.external_mock.decode".to_owned(),
                        cpu_slot_cost: 1,
                        native_internal_parallelism: 1,
                    },
                    minimum_working_set_bytes: 64,
                    maximum_working_set_bytes: 1024 * 1024,
                },
                batches_per_unit: 1,
            }
        }

        fn with_batches_per_unit(mut self, batches_per_unit: usize) -> Self {
            self.batches_per_unit = batches_per_unit;
            self
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
                Ok(Arc::new(ExternalMockDecodeSession {
                    source,
                    units,
                    batches_per_unit: self.batches_per_unit,
                })
                    as Arc<dyn cdf_runtime::FormatDecodeSession>)
            })
        }
    }

    struct ExternalMockDecodeSession {
        source: Arc<dyn cdf_runtime::ByteSource>,
        units: Vec<cdf_runtime::DecodeUnitPlan>,
        batches_per_unit: usize,
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
                let schema_hash = cdf_kernel::canonical_arrow_schema_hash(
                    request.schema.decoder_schema.as_ref(),
                )?;
                let mut batches = Vec::with_capacity(self.batches_per_unit);
                for index in 0..self.batches_per_unit {
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
                        cdf_kernel::BatchId::new(format!("external-mock-batch-{index}"))?,
                        request.resource_id.clone(),
                        request.partition_id.clone(),
                        schema_hash.clone(),
                        record_batch,
                    )?;
                    batch.header.source_position = request.source_position.clone();
                    batches.push(cdf_runtime::AccountedPhysicalBatch::new(batch, lease));
                }
                Ok(Box::pin(futures_util::stream::iter(batches))
                    as cdf_runtime::PhysicalDecodeStream)
            })
        }
    }

    #[test]
    fn blocked_decode_publication_releases_shared_run_work() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("events.mock");
        std::fs::write(&path, b"MOCK\n").unwrap();
        let mut formats = cdf_runtime::FormatRegistry::default();
        formats
            .register(Arc::new(ExternalMockFormat::new().with_batches_per_unit(8)))
            .unwrap();
        let services = crate::test_execution_services()
            .with_run_job_ceiling(1)
            .unwrap();
        let dependencies = FileRuntimeDependencies::new(
            FileTransportFacade::new(),
            services.clone(),
            Arc::new(formats),
            crate::test_transform_registry(),
            crate::test_egress_scope(),
        );
        let driver = dependencies.formats().resolve("external_mock").unwrap();
        let open = |partition: &str| {
            stream_registered_format(
                RegisteredFormatStreamRequest {
                    source: Arc::new(
                        LocalByteSource::open(&path, dependencies.execution().memory()).unwrap(),
                    ),
                    payload_retention: None,
                    driver: Arc::clone(&driver),
                    scan_intent: CompiledScanIntent::full_scan(),
                    options: ReadOptions::new(
                        ResourceId::new("events").unwrap(),
                        PartitionId::new(partition).unwrap(),
                    ),
                    admission_schema: ExternalMockFormat::schema(),
                    canonical_format_options: serde_json::json!({}),
                    source_position: None,
                    physical_schema_authority: PhysicalSchemaAuthority::default(),
                },
                &dependencies,
            )
            .unwrap()
        };
        let mut first = open("first");
        let second = open("second");
        let first_batch = futures_executor::block_on(first.next())
            .expect("first stream must publish")
            .unwrap();

        // Let the first producer fill both bounded publication channels. It may retain decoded
        // bytes there, but it must not retain the sole run-work permit while waiting for demand.
        std::thread::sleep(Duration::from_millis(100));
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        let worker = std::thread::spawn(move || {
            sender
                .send(futures_executor::block_on(second.into_future()).0)
                .unwrap();
        });
        let second_batch = receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("a blocked later publication must not monopolize shared run work")
            .expect("second stream must publish")
            .unwrap();
        drop(second_batch);
        drop(first_batch);
        drop(first);
        worker.join().unwrap();
        assert_eq!(services.run_job_ceiling().unwrap(), Some(1));
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
        metadata_reads: Arc<AtomicUsize>,
        listings: Arc<AtomicUsize>,
    }

    struct ExternalSchemeTransport {
        memory: Arc<dyn cdf_memory::MemoryCoordinator>,
    }

    impl FileTransport for ExternalSchemeTransport {
        fn metadata(
            &self,
            _egress: &cdf_runtime::SourceEgressScope,
            _resource: &FileTransportResource,
            _control: &FileTransportControl,
        ) -> Result<crate::FileMetadataObservation> {
            Err(CdfError::internal(
                "external scheme fixture does not use metadata",
            ))
        }

        fn list(
            &self,
            _egress: &cdf_runtime::SourceEgressScope,
            resource: &FileTransportResource,
            _maximum_results: usize,
            _control: &FileTransportControl,
        ) -> Result<crate::FileIdentityStream> {
            assert!(matches!(
                &resource.location,
                FileTransportLocation::RemoteUrl { url } if url.starts_with("mock://")
            ));
            let lease = futures_executor::block_on(cdf_memory::reserve(
                Arc::clone(&self.memory),
                cdf_memory::ReservationRequest::new(
                    cdf_memory::ConsumerKey::new(
                        "external-file-transport-metadata",
                        cdf_memory::MemoryClass::Discovery,
                    )?,
                    crate::transport::FILE_IDENTITY_MEMORY_ENVELOPE_BYTES,
                )?,
            ))?;
            let identity = crate::AccountedFileIdentity::new(
                FileIdentityMetadata {
                    location: "mock://catalog/data/events.parquet".to_owned(),
                    size_bytes: Some(4),
                    checksum: None,
                    etag: Some("\"mock-generation\"".to_owned()),
                    version: None,
                    modified: None,
                    exact_ranges: true,
                },
                lease,
            )?;
            Ok(crate::FileIdentityStream::materialized(
                futures_util::stream::iter([Ok(identity)]),
            ))
        }

        fn open_byte_source(
            &self,
            _egress: &cdf_runtime::SourceEgressScope,
            _resource: &FileTransportResource,
            _expected: &FileIdentityMetadata,
            _memory: Arc<dyn cdf_memory::MemoryCoordinator>,
        ) -> Result<Arc<dyn cdf_runtime::ByteSource>> {
            Err(CdfError::internal(
                "external scheme fixture does not open payload",
            ))
        }
    }

    impl FileTransport for PayloadOpenCountingTransport {
        fn metadata(
            &self,
            egress: &cdf_runtime::SourceEgressScope,
            resource: &FileTransportResource,
            control: &FileTransportControl,
        ) -> Result<crate::FileMetadataObservation> {
            self.metadata_reads.fetch_add(1, Ordering::Relaxed);
            self.inner.metadata(egress, resource, control)
        }

        fn metadata_if_exists(
            &self,
            egress: &cdf_runtime::SourceEgressScope,
            resource: &FileTransportResource,
            control: &FileTransportControl,
        ) -> Result<Option<crate::FileMetadataObservation>> {
            self.inner.metadata_if_exists(egress, resource, control)
        }

        fn list(
            &self,
            egress: &cdf_runtime::SourceEgressScope,
            resource: &FileTransportResource,
            maximum_results: usize,
            control: &FileTransportControl,
        ) -> Result<crate::FileIdentityStream> {
            self.listings.fetch_add(1, Ordering::Relaxed);
            self.inner.list(egress, resource, maximum_results, control)
        }

        fn open_byte_source(
            &self,
            egress: &cdf_runtime::SourceEgressScope,
            resource: &FileTransportResource,
            expected: &FileIdentityMetadata,
            memory: Arc<dyn cdf_memory::MemoryCoordinator>,
        ) -> Result<Arc<dyn cdf_runtime::ByteSource>> {
            self.payload_opens.fetch_add(1, Ordering::Relaxed);
            self.inner
                .open_byte_source(egress, resource, expected, memory)
        }
    }

    #[test]
    fn external_remote_scheme_requires_no_file_runtime_dispatch_branch() {
        let coordinator = Arc::new(
            cdf_memory::DeterministicMemoryCoordinator::new(
                crate::transport::FILE_IDENTITY_MEMORY_ENVELOPE_BYTES,
                BTreeMap::new(),
            )
            .unwrap(),
        );
        let transport = ExternalSchemeTransport {
            memory: coordinator.clone(),
        };
        let plan = FileResourcePlan {
            source: "events".to_owned(),
            root: "mock://catalog/data".to_owned(),
            glob: "*.parquet".to_owned(),
            format: Some(FileFormatDeclaration::parquet()),
            format_declared: true,
            format_options: serde_json::json!({}),
            schema_discovery: None,
            compression: FileCompressionDeclaration::none(),
            spool_mode: crate::FileSpoolMode::Overlap,
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };

        let matches = resolve_remote_matches(
            &ResourceId::new("events.raw").unwrap(),
            &plan,
            &transport,
            &crate::test_egress_scope(),
            crate::test_format_registry().as_ref(),
            crate::test_transform_registry().as_ref(),
        )
        .unwrap();

        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].path_text, "mock://catalog/data/events.parquet");
        assert_eq!(coordinator.snapshot().current_bytes, 0);
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
            crate::test_egress_scope(),
        );
        let plan = FileResourcePlan {
            source: "external".to_owned(),
            root: root.path().to_string_lossy().into_owned(),
            glob: "events.mock.mt".to_owned(),
            format: Some(FileFormatDeclaration::named("external_mock").unwrap()),
            format_declared: true,
            format_options: serde_json::json!({}),
            schema_discovery: None,
            compression: FileCompressionDeclaration::auto(),
            spool_mode: crate::FileSpoolMode::Overlap,
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let resource_id = ResourceId::new("external.events").unwrap();
        let resolved = dependencies
            .with_transport(|transport, egress| {
                resolve_file_matches(
                    &resource_id,
                    &plan,
                    transport,
                    egress,
                    dependencies.formats(),
                    dependencies.transforms(),
                )
            })
            .unwrap();
        assert_eq!(resolved[0].compression.mode_name(), "external_passthrough");
        let probe = discover_local_binary_schema(
            &path,
            "events.mock.mt",
            &dependencies,
            0,
            SchemaDiscoveryRequest {
                resource_id: &resource_id,
                format: plan.resolved_format().unwrap(),
                format_declared: plan.format_declared,
                format_options: &plan.format_options,
                discovery_kind: cdf_runtime::FormatDiscoveryKind::BoundedContent,
                transform_name: "external_passthrough",
                maximum_bytes: 1024,
                maximum_records: 1_000,
                cancellation: cdf_runtime::RunCancellation::default(),
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
            crate::test_egress_scope(),
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
                        .with_transport(|_, _| {
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
            crate::test_egress_scope(),
        );
        let driver = dependencies.formats().resolve("parquet").unwrap();
        let stream = stream_registered_format(
            RegisteredFormatStreamRequest {
                source: Arc::new(
                    LocalByteSource::open(temp.path(), dependencies.execution().memory()).unwrap(),
                ),
                payload_retention: None,
                driver,
                scan_intent: CompiledScanIntent::full_scan(),
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
    fn negotiated_parquet_projection_and_predicate_reach_production_decode() {
        let physical_schema = Arc::new(Schema::new(vec![
            Field::new("VendorID", DataType::Int64, false),
            Field::new("Name", DataType::Utf8, false),
            Field::new("Ignored", DataType::Int64, false),
        ]));
        let schema = Arc::new(Schema::new(vec![
            cdf_kernel::with_source_name(
                Field::new("vendor_id", DataType::Int64, false),
                "VendorID",
            ),
            cdf_kernel::with_source_name(Field::new("name", DataType::Utf8, false), "Name"),
            cdf_kernel::with_source_name(Field::new("ignored", DataType::Int64, false), "Ignored"),
        ]));
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("events.parquet");
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(&path).unwrap(),
            Arc::clone(&physical_schema),
            None,
        )
        .unwrap();
        writer
            .write(
                &RecordBatch::try_new(
                    Arc::clone(&physical_schema),
                    vec![
                        Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                        Arc::new(StringArray::from(vec!["one", "two"])) as ArrayRef,
                        Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
                    ],
                )
                .unwrap(),
            )
            .unwrap();
        writer.close().unwrap();

        let formats = crate::test_format_registry();
        let unresolved = FileResourcePlan {
            source: "events".to_owned(),
            root: temp.path().to_string_lossy().into_owned(),
            glob: "events.parquet".to_owned(),
            format: Some(FileFormatDeclaration::parquet()),
            format_declared: true,
            format_options: serde_json::json!({}),
            schema_discovery: None,
            compression: FileCompressionDeclaration::none(),
            spool_mode: crate::FileSpoolMode::Overlap,
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let (plan, compiled_format) =
            crate::compile_file_resource_plan(&unresolved, formats.as_ref()).unwrap();
        let descriptor = ResourceDescriptor {
            resource_id: ResourceId::new("events").unwrap(),
            schema_source: cdf_kernel::SchemaSource::Declared {
                schema_hash: cdf_kernel::canonical_arrow_schema_hash(schema.as_ref()).unwrap(),
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
            trust_level: cdf_kernel::TrustLevel::Governed,
        };
        assert_eq!(
            file_resource_capabilities(&compiled_format.descriptor).projection,
            CapabilitySupport::Supported
        );
        assert_eq!(
            file_resource_capabilities(&compiled_format.descriptor)
                .filters
                .default_fidelity,
            PushdownFidelity::Exact
        );
        let resource = FileResource::new(
            FileResourceDefinition {
                descriptor: descriptor.clone(),
                schema: Arc::clone(&schema),
                plan,
                type_policy_allowances: TypePolicyAllowances::default(),
                effective_schema_runtime: Some(physical_runtime(
                    &descriptor,
                    Arc::clone(&schema),
                    Arc::clone(&physical_schema),
                    "events.parquet",
                )),
                compiled_format,
            },
            FileRuntimeDependencies::new(
                FileTransportFacade::new(),
                crate::test_execution_services(),
                formats,
                crate::test_transform_registry(),
                crate::test_egress_scope(),
            ),
        )
        .unwrap();
        let request = ScanRequest {
            resource_id: descriptor.resource_id.clone(),
            projection: Some(vec!["vendor_id".to_owned()]),
            filters: vec![
                cdf_kernel::ScanPredicate::new(
                    cdf_kernel::PredicateId::new("id-is-greater-than-one").unwrap(),
                    "vendor_id > 1",
                )
                .unwrap(),
                cdf_kernel::ScanPredicate::new(
                    cdf_kernel::PredicateId::new("name-is-two").unwrap(),
                    "name = 'two'",
                )
                .unwrap(),
            ],
            limit: None,
            order_by: Vec::new(),
            scope: ScopeKey::Resource,
        };
        let tier_a = resource.plan_partitions(&request).unwrap();
        assert_eq!(tier_a[0].scan_intent, CompiledScanIntent::full_scan());
        let scan = resource.negotiate(&request).unwrap();
        assert_eq!(
            scan.partitions[0].scan_intent.projection.as_deref(),
            Some(["vendor_id".to_owned()].as_slice())
        );
        assert_eq!(scan.partitions[0].scan_intent.predicates.len(), 2);
        assert_eq!(scan.pushed_predicates.len(), 2);
        assert!(scan.unsupported_predicates.is_empty());
        cdf_kernel::validate_compiled_scan_intents(&scan).unwrap();

        let widened_physical = Arc::new(Schema::new(vec![
            Field::new("VendorID", DataType::Int32, false),
            Field::new("Name", DataType::Utf8, false),
            Field::new("Ignored", DataType::Int64, false),
        ]));
        let widened_runtime = physical_runtime(
            &descriptor,
            Arc::clone(&schema),
            widened_physical,
            "events.parquet",
        );
        assert!(
            !exact_predicate_is_partition_equivalent(
                &request.filters[0],
                &scan.partitions,
                schema.as_ref(),
                Some(&widened_runtime),
            )
            .unwrap()
        );

        let opened = futures_executor::block_on(resource.open(scan.partitions[0].clone())).unwrap();
        let batches = futures_executor::block_on(opened.collect::<Vec<_>>())
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(batches.len(), 1);
        let projected = batches[0].record_batch().unwrap();
        assert_eq!(projected.schema().fields().len(), 1);
        assert_eq!(projected.schema().field(0).name(), "VendorID");
        assert_eq!(projected.num_rows(), 1);
        assert_eq!(
            projected
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            2
        );
    }

    #[test]
    fn compiled_logical_projection_maps_to_physical_source_names() {
        let schema = Schema::new(vec![cdf_kernel::with_source_name(
            Field::new("vendor_id", DataType::Int64, false),
            "VendorID",
        )]);
        assert_eq!(
            physical_projection_names(&schema, Some(&["vendor_id".to_owned()])).unwrap(),
            Some(vec!["VendorID".to_owned()])
        );
    }

    #[test]
    fn compiled_logical_predicate_maps_to_physical_source_names() {
        let schema = Schema::new(vec![cdf_kernel::with_source_name(
            Field::new("vendor_id", DataType::Int64, false),
            "VendorID",
        )]);
        let predicate = cdf_kernel::ScanPredicate::new(
            cdf_kernel::PredicateId::new("vendor-filter").unwrap(),
            "vendor_id = 7",
        )
        .unwrap();
        let physical = physical_predicates(&schema, &[predicate]).unwrap();
        assert_eq!(
            physical[0]
                .canonical_expression
                .comparison()
                .map(|(name, _, _)| name),
            Some("VendorID")
        );
    }

    #[test]
    fn adaptive_range_policy_requires_a_strict_subset_projection() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ]);
        assert_eq!(
            planned_file_access_coverage(&CompiledScanIntent::full_scan(), &schema),
            PlannedFileAccessCoverage::Full
        );
        let predicate = cdf_kernel::ScanPredicate::new(
            cdf_kernel::PredicateId::new("id-filter").unwrap(),
            "id > 7",
        )
        .unwrap();
        let predicate_only = CompiledScanIntent {
            version: cdf_kernel::COMPILED_SCAN_INTENT_VERSION,
            projection: None,
            predicates: vec![PushedPredicate {
                predicate,
                fidelity: PushdownFidelity::Exact,
            }],
            limit: None,
            order_by: Vec::new(),
        };
        assert_eq!(
            planned_file_access_coverage(&predicate_only, &schema),
            PlannedFileAccessCoverage::Full
        );
        let all_columns = CompiledScanIntent {
            projection: Some(vec!["id".to_owned(), "payload".to_owned()]),
            ..CompiledScanIntent::full_scan()
        };
        assert_eq!(
            planned_file_access_coverage(&all_columns, &schema),
            PlannedFileAccessCoverage::Full
        );
        let subset = CompiledScanIntent {
            projection: Some(vec!["id".to_owned()]),
            ..CompiledScanIntent::full_scan()
        };
        assert_eq!(
            planned_file_access_coverage(&subset, &schema),
            PlannedFileAccessCoverage::Selective
        );
    }

    #[test]
    fn exact_predicate_negotiation_requires_the_shared_physical_lowering() {
        let descriptor = cdf_format_parquet::ParquetFormatDriver::new()
            .unwrap()
            .descriptor()
            .clone();
        let integer_schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let integer_request = ScanRequest {
            resource_id: ResourceId::new("events").unwrap(),
            projection: None,
            filters: vec![
                cdf_kernel::ScanPredicate::new(
                    cdf_kernel::PredicateId::new("id-filter").unwrap(),
                    "id > 7",
                )
                .unwrap(),
            ],
            limit: None,
            order_by: Vec::new(),
            scope: ScopeKey::Resource,
        };
        let integer = compile_file_scan(&integer_request, &descriptor, &integer_schema).unwrap();
        assert_eq!(integer.pushed_predicates.len(), 1);
        assert!(integer.unsupported_predicates.is_empty());

        let timestamp_schema = Schema::new(vec![Field::new(
            "observed_at",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            false,
        )]);
        let timestamp_request = ScanRequest {
            filters: vec![
                cdf_kernel::ScanPredicate::new(
                    cdf_kernel::PredicateId::new("time-filter").unwrap(),
                    "observed_at >= '2026-07-16T00:00:00Z'",
                )
                .unwrap(),
            ],
            ..integer_request.clone()
        };
        let timestamp =
            compile_file_scan(&timestamp_request, &descriptor, &timestamp_schema).unwrap();
        assert!(timestamp.pushed_predicates.is_empty());
        assert_eq!(timestamp.unsupported_predicates.len(), 1);

        let hostile = cdf_kernel::ScanPredicate::from_expression(
            cdf_kernel::PredicateId::new("hostile-version").unwrap(),
            "id = 7",
            cdf_kernel::Expression::new(ExpressionNode::Call {
                function: cdf_kernel::FunctionReference {
                    namespace: "other".to_owned(),
                    name: "eq".to_owned(),
                    version: "999".to_owned(),
                },
                arguments: vec![
                    ExpressionNode::Column {
                        name: "id".to_owned(),
                    },
                    ExpressionNode::Literal {
                        value: cdf_kernel::ExpressionLiteral::Signed(7),
                    },
                ],
            }),
        )
        .unwrap();
        let hostile_request = ScanRequest {
            filters: vec![hostile],
            ..integer_request
        };
        let hostile = compile_file_scan(&hostile_request, &descriptor, &integer_schema).unwrap();
        assert!(hostile.pushed_predicates.is_empty());
        assert_eq!(hostile.unsupported_predicates.len(), 1);
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
            crate::test_egress_scope(),
        );
        let plan = FileResourcePlan {
            source: "events".to_owned(),
            root: root.path().to_string_lossy().into_owned(),
            glob: "events.parquet.gz".to_owned(),
            format: Some(FileFormatDeclaration::parquet()),
            format_declared: true,
            format_options: serde_json::json!({}),
            schema_discovery: None,
            compression: FileCompressionDeclaration::auto(),
            spool_mode: crate::FileSpoolMode::Overlap,
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let resource_id = ResourceId::new("events.raw").unwrap();
        let resolved = dependencies
            .with_transport(|transport, egress| {
                resolve_file_matches(
                    &resource_id,
                    &plan,
                    transport,
                    egress,
                    dependencies.formats(),
                    dependencies.transforms(),
                )
            })
            .unwrap();
        assert_eq!(resolved[0].compression.mode_name(), "gzip");
        assert_eq!(resolved[0].format.extension.as_deref(), Some("parquet"));
        let probe = discover_local_binary_schema(
            root.path().join("events.parquet.gz"),
            "events.parquet.gz",
            &dependencies,
            0,
            SchemaDiscoveryRequest {
                resource_id: &resource_id,
                format: plan.resolved_format().unwrap(),
                format_declared: plan.format_declared,
                format_options: &plan.format_options,
                discovery_kind: cdf_runtime::FormatDiscoveryKind::FormatMetadata,
                transform_name: "gzip",
                maximum_bytes: 64 * 1024 * 1024,
                maximum_records: 1_000,
                cancellation: cdf_runtime::RunCancellation::default(),
            },
        )
        .unwrap();
        assert_eq!(probe.schema.as_ref(), schema.as_ref());
        assert_eq!(probe.source_identity.get("compression").unwrap(), "gzip");
        let stable_id = probe.source_identity.get("stable_id").unwrap();
        assert!(
            stable_id.ends_with("events.parquet.gz") && !stable_id.contains('#'),
            "unexpected transformed stable id: {stable_id}"
        );
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
            crate::test_egress_scope(),
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
            schema_discovery: None,
            compression: FileCompressionDeclaration::none(),
            spool_mode: crate::FileSpoolMode::Overlap,
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let resource_id = ResourceId::new("ipc.events").unwrap();
        let resolved = dependencies
            .with_transport(|transport, egress| {
                resolve_remote_matches(
                    &resource_id,
                    &plan,
                    transport,
                    egress,
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
            schema_discovery: None,
            compression: FileCompressionDeclaration::none(),
            spool_mode: crate::FileSpoolMode::Overlap,
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let dependencies = FileRuntimeDependencies::new(
            FileTransportFacade::new(),
            crate::test_execution_services(),
            crate::test_format_registry(),
            crate::test_transform_registry(),
            crate::test_egress_scope(),
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

        let error = prepare_file_input(PrepareFileInputRequest {
            resource_id: &resource_id,
            resolved: &resolved,
            source_access: cdf_runtime::FormatSourceAccess::Sequential,
            access_coverage: PlannedFileAccessCoverage::Full,
            driver: driver.as_ref(),
            canonical_format_options: &canonical_options,
            dependencies: &dependencies,
            cancellation: &cdf_runtime::RunCancellation::default(),
        })
        .err()
        .expect("stale local plan must fail before extraction hashing");

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
        assert!(error.message.contains("changed between planning and open"));
    }

    #[test]
    fn remote_parquet_uses_admitted_spool_or_generation_bound_ranges() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from_iter_values(0..100_000)),
                Arc::new(Int64Array::from_iter_values(100_000..200_000)),
            ],
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
            crate::test_egress_scope(),
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
            schema_discovery: None,
            compression: FileCompressionDeclaration::none(),
            spool_mode: crate::FileSpoolMode::Overlap,
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let resource_id = ResourceId::new("parquet.events").unwrap();
        let resolved = dependencies
            .with_transport(|transport, egress| {
                resolve_remote_matches(
                    &resource_id,
                    &plan,
                    transport,
                    egress,
                    dependencies.formats(),
                    dependencies.transforms(),
                )
            })
            .unwrap();
        let driver = dependencies.formats().resolve("parquet").unwrap();
        let canonical_options = driver.canonical_options(serde_json::json!({})).unwrap();
        assert!(matches!(
            prepare_file_input(PrepareFileInputRequest {
                resource_id: &resource_id,
                resolved: &resolved[0],
                source_access: cdf_runtime::FormatSourceAccess::Adaptive,
                access_coverage: PlannedFileAccessCoverage::Selective,
                driver: driver.as_ref(),
                canonical_format_options: &canonical_options,
                dependencies: &dependencies,
                cancellation: &cdf_runtime::RunCancellation::default(),
            })
            .unwrap()
            .input,
            PreparedFileInput::Source(_)
        ));
        assert!(matches!(
            prepare_file_input(PrepareFileInputRequest {
                resource_id: &resource_id,
                resolved: &resolved[0],
                source_access: cdf_runtime::FormatSourceAccess::Adaptive,
                access_coverage: PlannedFileAccessCoverage::Full,
                driver: driver.as_ref(),
                canonical_format_options: &canonical_options,
                dependencies: &dependencies,
                cancellation: &cdf_runtime::RunCancellation::default(),
            })
            .unwrap()
            .input,
            PreparedFileInput::SpoolSource { .. }
        ));

        let prepared = prepare_file_input(PrepareFileInputRequest {
            resource_id: &resource_id,
            resolved: &resolved[0],
            source_access: cdf_runtime::FormatSourceAccess::Adaptive,
            access_coverage: PlannedFileAccessCoverage::Full,
            driver: driver.as_ref(),
            canonical_format_options: &canonical_options,
            dependencies: &dependencies,
            cancellation: &cdf_runtime::RunCancellation::default(),
        })
        .unwrap();
        let source_io = prepared.source_io.clone();
        let PreparedFileInput::SpoolSource { source, size_bytes } = prepared.input else {
            panic!("full remote Parquet scan must select a seekable spool input")
        };
        let dependencies_for_complete = dependencies.clone();
        dependencies
            .execution()
            .run_io(async move {
                ready_spooled_file_input(SpoolInputRequest {
                    source,
                    size_bytes,
                    mode: crate::FileSpoolMode::Complete,
                    source_io: source_io.clone(),
                    payload_cache_key: None,
                    dependencies: &dependencies_for_complete,
                    cancellation: cdf_runtime::RunCancellation::default(),
                })
                .await
                .map(|ready| (ready, source_io))
            })
            .map(|(ready, source_io)| {
                assert_eq!(source_io.snapshot().mode, Some(SourceReadMode::FullSpool));
                assert!(ready.source_completion.is_none());
            })
            .unwrap();

        struct WeakHttpTransport {
            path: PathBuf,
        }

        impl crate::transport::HttpFileTransport for WeakHttpTransport {
            fn send_headers(
                &self,
                _request: crate::transport::HttpFileRequest,
            ) -> BoxFuture<'static, Result<crate::transport::HttpFileResponse>> {
                Box::pin(async { Err(CdfError::internal("unused weak HTTP metadata probe")) })
            }

            fn open_byte_source(
                &self,
                _resource: &FileTransportResource,
                _expected: &FileIdentityMetadata,
                _auth: Option<crate::ResolvedHttpAuth>,
                memory: Arc<dyn cdf_memory::MemoryCoordinator>,
            ) -> Result<Arc<dyn ByteSource>> {
                Ok(Arc::new(LocalByteSource::open(&self.path, memory)?))
            }
        }

        let weak_file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(weak_file.path(), &bytes).unwrap();
        let weak_dependencies = FileRuntimeDependencies::new(
            FileTransportFacade::new().with_http_transport(WeakHttpTransport {
                path: weak_file.path().to_path_buf(),
            }),
            crate::test_execution_services(),
            crate::test_format_registry(),
            crate::test_transform_registry(),
            crate::test_egress_scope(),
        )
        .with_max_spool_bytes(bytes.len() as u64)
        .unwrap();
        let weak = ResolvedFileMatch {
            open: ResolvedFileOpen::Transport(FileTransportResource::http_url(
                "https://weak.example/events.parquet",
            )),
            path_text: "https://weak.example/events.parquet".to_owned(),
            size_bytes: bytes.len() as u64,
            source_generation: None,
            identity_strength: GenerationStrength::Weak,
            sha256: None,
            etag: None,
            version: None,
            modified_ms: None,
            exact_ranges: false,
            bytes_loaded: None,
            compression: resolved[0].compression.clone(),
            format: resolved[0].format.clone(),
        };
        let weak_driver = weak_dependencies.formats().resolve("parquet").unwrap();
        let weak_options = weak_driver
            .canonical_options(serde_json::json!({}))
            .unwrap();
        let weak_input = prepare_file_input(PrepareFileInputRequest {
            resource_id: &resource_id,
            resolved: &weak,
            source_access: cdf_runtime::FormatSourceAccess::Adaptive,
            access_coverage: PlannedFileAccessCoverage::Selective,
            driver: weak_driver.as_ref(),
            canonical_format_options: &weak_options,
            dependencies: &weak_dependencies,
            cancellation: &cdf_runtime::RunCancellation::default(),
        })
        .unwrap();
        assert!(matches!(
            &weak_input.input,
            PreparedFileInput::SpoolSource { .. }
        ));
        let weak_partition = PreparedFilePartition {
            resolved: weak,
            input: weak_input.input,
            scan_intent: CompiledScanIntent {
                projection: Some(vec!["id".to_owned()]),
                ..CompiledScanIntent::full_scan()
            },
            options: ReadOptions::new(
                resource_id.clone(),
                PartitionId::new("file-parquet-weak").unwrap(),
            ),
            admission_schema: Arc::clone(&schema),
            physical_schema_authority: PhysicalSchemaAuthority::default(),
            canonical_format_options: weak_options,
            driver: weak_driver,
            source_io: weak_input.source_io,
            extraction_content_hash: weak_input.extraction_content_hash,
            hash_sweep_source: weak_input.hash_sweep_source,
            payload_retention: weak_input.payload_retention,
            payload_cache_key: weak_input.payload_cache_key,
            spool_mode: crate::FileSpoolMode::Overlap,
        };
        let dependencies_for_weak = weak_dependencies.clone();
        let weak_stream = weak_dependencies
            .execution()
            .run_io(async move {
                stream_prepared_file_match(
                    weak_partition,
                    &dependencies_for_weak,
                    cdf_runtime::RunCancellation::default(),
                )
                .await
            })
            .unwrap();
        assert!(weak_stream.source_completion.is_none());
        let weak_batches = futures_executor::block_on(weak_stream.batches.collect::<Vec<_>>())
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            weak_batches
                .iter()
                .map(|batch| batch.header.row_count)
                .sum::<u64>(),
            100_000
        );
        assert!(weak_batches.iter().all(|batch| {
            batch
                .record_batch()
                .is_some_and(|batch| batch.schema().fields().len() == 1)
        }));
        drop(weak_batches);
        assert_eq!(
            weak_dependencies
                .execution()
                .memory()
                .snapshot()
                .current_bytes,
            0
        );
        let weak_spill = weak_dependencies.execution().spill().snapshot();
        assert!(weak_spill.peak_bytes >= bytes.len() as u64);
        assert_eq!(weak_spill.current_bytes, 0);
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
            crate::test_egress_scope(),
        )
        .with_max_spool_bytes(1)
        .unwrap();
        let constrained_matches = constrained
            .with_transport(|transport, egress| {
                resolve_remote_matches(
                    &resource_id,
                    &plan,
                    transport,
                    egress,
                    constrained.formats(),
                    constrained.transforms(),
                )
            })
            .unwrap();
        let driver = constrained.formats().resolve("parquet").unwrap();
        let canonical_options = driver.canonical_options(serde_json::json!({})).unwrap();
        assert!(matches!(
            prepare_file_input(PrepareFileInputRequest {
                resource_id: &resource_id,
                resolved: &constrained_matches[0],
                source_access: cdf_runtime::FormatSourceAccess::Adaptive,
                access_coverage: PlannedFileAccessCoverage::Full,
                driver: driver.as_ref(),
                canonical_format_options: &canonical_options,
                dependencies: &constrained,
                cancellation: &cdf_runtime::RunCancellation::default(),
            })
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
            crate::test_egress_scope(),
        )
        .with_max_spool_bytes(bytes.len() as u64)
        .unwrap();
        let contended_matches = contended
            .with_transport(|transport, egress| {
                resolve_remote_matches(
                    &ResourceId::new("parquet.contended").unwrap(),
                    &plan,
                    transport,
                    egress,
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
    fn opt_in_payload_cache_reuses_strong_remote_generation_and_misses_after_change() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(0..100_000))],
        )
        .unwrap();
        let first_bytes = cdf_package::transcode_record_batches_to_parquet_bytes(&[batch]).unwrap();
        let changed_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(100_000..200_000))],
        )
        .unwrap();
        let changed_bytes =
            cdf_package::transcode_record_batches_to_parquet_bytes(&[changed_batch]).unwrap();
        let store = Arc::new(InMemory::new());
        let object_path = ObjectPath::from("prod/events.parquet");
        futures_executor::block_on(store.put(&object_path, PutPayload::from(first_bytes.clone())))
            .unwrap();
        let payload_opens = Arc::new(AtomicUsize::new(0));
        let listings = Arc::new(AtomicUsize::new(0));
        let transport = PayloadOpenCountingTransport {
            inner: FileTransportFacade::new()
                .with_object_store("s3://cache", store.clone())
                .with_execution_services(crate::test_execution_services()),
            payload_opens: Arc::clone(&payload_opens),
            metadata_reads: Arc::new(AtomicUsize::new(0)),
            listings: Arc::clone(&listings),
        };
        let cache_root = tempfile::tempdir().unwrap();
        let maximum_object_bytes = first_bytes.len().max(changed_bytes.len()) as u64;
        let dependencies = FileRuntimeDependencies::new(
            transport,
            crate::test_execution_services(),
            crate::test_format_registry(),
            crate::test_transform_registry(),
            crate::test_egress_scope(),
        )
        .with_max_spool_bytes(maximum_object_bytes)
        .unwrap()
        .with_payload_cache(
            crate::FilePayloadCache::new(
                cache_root.path().join("v1"),
                4,
                maximum_object_bytes.saturating_mul(4),
            )
            .unwrap(),
        );
        let plan = FileResourcePlan {
            source: "cache".to_owned(),
            root: "s3://cache/prod".to_owned(),
            glob: "events.parquet".to_owned(),
            format: Some(FileFormatDeclaration::parquet()),
            format_declared: true,
            format_options: serde_json::json!({}),
            schema_discovery: None,
            compression: FileCompressionDeclaration::none(),
            spool_mode: crate::FileSpoolMode::Overlap,
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let resource_id = ResourceId::new("cache.events").unwrap();
        let resolve_current = || {
            dependencies
                .with_transport(|transport, egress| {
                    resolve_remote_matches(
                        &resource_id,
                        &plan,
                        transport,
                        egress,
                        dependencies.formats(),
                        dependencies.transforms(),
                    )
                })
                .unwrap()
                .remove(0)
        };
        let run = |resolved: &ResolvedFileMatch, partition: &str| {
            let stream = stream_file_match_blocking(
                resolved,
                plan.resolved_format().unwrap(),
                ReadOptions::new(resource_id.clone(), PartitionId::new(partition).unwrap()),
                &dependencies,
                Arc::clone(&schema),
                PhysicalSchemaAuthority::default(),
            )
            .unwrap();
            futures_executor::block_on(stream.collect::<Vec<_>>())
                .into_iter()
                .collect::<Result<Vec<_>>>()
                .unwrap()
        };

        let first_resolved = resolve_current();
        let first = run(&first_resolved, "file-cache-first");
        assert_eq!(payload_opens.load(Ordering::Relaxed), 1);

        let second_resolved = resolve_current();
        let driver = dependencies.formats().resolve("parquet").unwrap();
        let canonical_options = driver.canonical_options(serde_json::json!({})).unwrap();
        let cached_input = prepare_file_input(PrepareFileInputRequest {
            resource_id: &resource_id,
            resolved: &second_resolved,
            source_access: cdf_runtime::FormatSourceAccess::Adaptive,
            access_coverage: PlannedFileAccessCoverage::Full,
            driver: driver.as_ref(),
            canonical_format_options: &canonical_options,
            dependencies: &dependencies,
            cancellation: &cdf_runtime::RunCancellation::default(),
        })
        .unwrap();
        assert_eq!(
            cached_input.source_io.snapshot().mode,
            Some(SourceReadMode::PayloadCache)
        );
        assert!(matches!(cached_input.input, PreparedFileInput::Source(_)));
        let second = run(&second_resolved, "file-cache-second");
        assert_eq!(payload_opens.load(Ordering::Relaxed), 1);
        assert!(listings.load(Ordering::Relaxed) >= 2);
        assert_eq!(first.len(), second.len());
        for (left, right) in first.iter().zip(&second) {
            assert_eq!(left.record_batch(), right.record_batch());
        }

        let cache_object = std::fs::read_dir(cache_root.path().join("v1/objects"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&cache_object, std::fs::Permissions::from_mode(0o600))
                .unwrap();
        }
        std::fs::write(&cache_object, vec![0_u8; first_bytes.len()]).unwrap();
        let corrupt_resolved = resolve_current();
        let recovered = run(&corrupt_resolved, "file-cache-corrupt-fallback");
        assert_eq!(payload_opens.load(Ordering::Relaxed), 2);
        assert_eq!(first.len(), recovered.len());
        for (left, right) in first.iter().zip(&recovered) {
            assert_eq!(left.record_batch(), right.record_batch());
        }

        futures_executor::block_on(store.put(&object_path, PutPayload::from(changed_bytes)))
            .unwrap();
        let changed_resolved = resolve_current();
        assert_ne!(first_resolved.etag, changed_resolved.etag);
        let changed = run(&changed_resolved, "file-cache-changed");
        assert_eq!(payload_opens.load(Ordering::Relaxed), 3);
        let first_value = first[0]
            .record_batch()
            .unwrap()
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let changed_value = changed[0]
            .record_batch()
            .unwrap()
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_ne!(first_value, changed_value);
        drop((first, second, recovered, changed));
        assert_eq!(
            dependencies.execution().memory().snapshot().current_bytes,
            0
        );
        assert_eq!(dependencies.execution().spill().snapshot().current_bytes, 0);
    }

    #[test]
    fn disabled_payload_cache_adds_no_spool_hash_pass() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), b"uncached-hot-path").unwrap();
        let dependencies = FileRuntimeDependencies::new(
            FileTransportFacade::new(),
            crate::test_execution_services(),
            crate::test_format_registry(),
            crate::test_transform_registry(),
            crate::test_egress_scope(),
        );
        let source: Arc<dyn ByteSource> = Arc::new(
            LocalByteSource::open(file.path(), dependencies.execution().memory()).unwrap(),
        );
        let execution = dependencies.execution().clone();
        let spool_dependencies = dependencies.clone();
        let spool = execution
            .run_io(async move {
                spool_byte_source_async(
                    source,
                    Some(17),
                    None,
                    &spool_dependencies,
                    cdf_runtime::RunCancellation::default(),
                )
                .await
            })
            .unwrap();

        assert_eq!(spool.bytes(), 17);
        assert!(spool.sha256().is_none());
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
            schema_discovery: None,
            compression: FileCompressionDeclaration::none(),
            spool_mode: crate::FileSpoolMode::Overlap,
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let resource_id = ResourceId::new("events.raw").unwrap();

        let matches = resolve_remote_matches(
            &resource_id,
            &plan,
            &transport,
            &crate::test_egress_scope(),
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

    struct StreamingOnlyRemoteListingTransport {
        memory: Arc<cdf_memory::DeterministicMemoryCoordinator>,
        locations: Vec<String>,
    }

    impl FileTransport for StreamingOnlyRemoteListingTransport {
        fn metadata(
            &self,
            _egress: &cdf_runtime::SourceEgressScope,
            _resource: &FileTransportResource,
            _control: &FileTransportControl,
        ) -> Result<crate::FileMetadataObservation> {
            Err(CdfError::internal(
                "streaming-listing fixture does not support metadata",
            ))
        }

        fn list(
            &self,
            _egress: &cdf_runtime::SourceEgressScope,
            _resource: &FileTransportResource,
            _maximum_results: usize,
            _control: &FileTransportControl,
        ) -> Result<crate::FileIdentityStream> {
            let memory = Arc::clone(&self.memory);
            let stream = futures_util::stream::iter(self.locations.clone().into_iter().enumerate())
                .then(move |(index, location)| {
                    let memory = Arc::clone(&memory);
                    async move {
                        let lease = cdf_memory::reserve(
                            memory,
                            cdf_memory::ReservationRequest::new(
                                cdf_memory::ConsumerKey::new(
                                    format!("streaming-listing-{index}"),
                                    cdf_memory::MemoryClass::Discovery,
                                )?,
                                crate::transport::FILE_IDENTITY_MEMORY_ENVELOPE_BYTES,
                            )?,
                        )
                        .await?;
                        crate::AccountedFileIdentity::new(
                            FileIdentityMetadata {
                                location,
                                size_bytes: Some(4),
                                checksum: None,
                                etag: Some(format!("\"listing-{index}\"")),
                                version: None,
                                modified: None,
                                exact_ranges: true,
                            },
                            lease,
                        )
                    }
                });
            Ok(crate::FileIdentityStream::materialized(stream))
        }

        fn open_byte_source(
            &self,
            _egress: &cdf_runtime::SourceEgressScope,
            _resource: &FileTransportResource,
            _expected: &FileIdentityMetadata,
            _memory: Arc<dyn cdf_memory::MemoryCoordinator>,
        ) -> Result<Arc<dyn ByteSource>> {
            Err(CdfError::internal(
                "streaming-listing fixture does not open payload",
            ))
        }
    }

    #[test]
    fn remote_listing_filters_without_materializing_all_metadata() {
        let memory = Arc::new(
            cdf_memory::DeterministicMemoryCoordinator::new(
                crate::transport::FILE_IDENTITY_MEMORY_ENVELOPE_BYTES,
                BTreeMap::new(),
            )
            .unwrap(),
        );
        let mut locations = (0..64)
            .map(|index| format!("s3://acme-events/prod/2025/nonmatch-{index:02}.parquet"))
            .collect::<Vec<_>>();
        locations.push("s3://acme-events/prod/2026/keep.parquet".to_owned());
        locations.extend(
            (0..64).map(|index| format!("s3://acme-events/prod/2027/nonmatch-{index:02}.parquet")),
        );
        let transport = StreamingOnlyRemoteListingTransport {
            memory: Arc::clone(&memory),
            locations,
        };
        let plan = FileResourcePlan {
            source: "events".to_owned(),
            root: "s3://acme-events/prod".to_owned(),
            glob: "2026/*.parquet".to_owned(),
            format: Some(FileFormatDeclaration::parquet()),
            format_declared: true,
            format_options: serde_json::json!({}),
            schema_discovery: None,
            compression: FileCompressionDeclaration::none(),
            spool_mode: crate::FileSpoolMode::Overlap,
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let resource_id = ResourceId::new("events.raw").unwrap();

        let matches = resolve_remote_matches(
            &resource_id,
            &plan,
            &transport,
            &crate::test_egress_scope(),
            crate::test_format_registry().as_ref(),
            crate::test_transform_registry().as_ref(),
        )
        .unwrap();

        assert_eq!(matches.len(), 1);
        assert_eq!(
            matches[0].path_text,
            "s3://acme-events/prod/2026/keep.parquet"
        );
        assert_eq!(memory.snapshot().current_bytes, 0);
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
        let metadata_reads = Arc::new(AtomicUsize::new(0));
        let listings = Arc::new(AtomicUsize::new(0));
        let transport = PayloadOpenCountingTransport {
            inner: FileTransportFacade::new()
                .with_object_store("s3://events", store)
                .with_execution_services(crate::test_execution_services()),
            payload_opens: Arc::clone(&payload_opens),
            metadata_reads,
            listings,
        };
        let plan = FileResourcePlan {
            source: "events".to_owned(),
            root: "s3://events/prod".to_owned(),
            glob: "events.ndjson.gz".to_owned(),
            format: Some(FileFormatDeclaration::ndjson()),
            format_declared: true,
            format_options: serde_json::json!({}),
            schema_discovery: None,
            compression: FileCompressionDeclaration::auto(),
            spool_mode: crate::FileSpoolMode::Overlap,
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };

        let matches = resolve_remote_matches(
            &ResourceId::new("events.raw").unwrap(),
            &plan,
            &transport,
            &crate::test_egress_scope(),
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
    fn planned_object_partitions_revalidate_exact_objects_without_relisting_the_glob() {
        let store = Arc::new(InMemory::new());
        for path in ["prod/2026/01/events.parquet", "prod/2026/02/events.parquet"] {
            futures_executor::block_on(store.put(
                &ObjectPath::from(path),
                PutPayload::from_static(b"PAR1fixture"),
            ))
            .unwrap();
        }
        let listings = Arc::new(AtomicUsize::new(0));
        let metadata_reads = Arc::new(AtomicUsize::new(0));
        let transport = PayloadOpenCountingTransport {
            inner: FileTransportFacade::new()
                .with_object_store("s3://events", store)
                .with_execution_services(crate::test_execution_services()),
            payload_opens: Arc::new(AtomicUsize::new(0)),
            metadata_reads: Arc::clone(&metadata_reads),
            listings: Arc::clone(&listings),
        };
        let plan = FileResourcePlan {
            source: "events".to_owned(),
            root: "s3://events/prod".to_owned(),
            glob: "2026/**/*.parquet".to_owned(),
            format: Some(FileFormatDeclaration::parquet()),
            format_declared: true,
            format_options: serde_json::json!({}),
            schema_discovery: None,
            compression: FileCompressionDeclaration::none(),
            spool_mode: crate::FileSpoolMode::Overlap,
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let empty_schema = Schema::empty();
        let descriptor = ResourceDescriptor {
            resource_id: ResourceId::new("events.raw").unwrap(),
            schema_source: cdf_kernel::SchemaSource::Declared {
                schema_hash: cdf_kernel::canonical_arrow_schema_hash(&empty_schema).unwrap(),
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
            trust_level: cdf_kernel::TrustLevel::Governed,
        };
        let formats = crate::test_format_registry();
        let transforms = crate::test_transform_registry();
        let partitions = file_partitions_for_plan_with_transport(
            &descriptor,
            &plan,
            &CompiledScanIntent::full_scan(),
            FilePlanningContext {
                transport: &transport,
                egress: &crate::test_egress_scope(),
                formats: formats.as_ref(),
                transforms: transforms.as_ref(),
                maximum_matches: usize::MAX,
                control: &FileTransportControl::default(),
                execution: crate::test_execution_services(),
            },
        )
        .unwrap();
        assert_eq!(partitions.len(), 2);
        assert_eq!(listings.load(Ordering::Relaxed), 1);

        for partition in &partitions {
            let egress = crate::test_egress_scope();
            let control = FileTransportControl::default();
            validate_partition(
                &descriptor,
                &plan,
                partition,
                FileResolutionContext {
                    transport: &transport,
                    egress: &egress,
                    formats: formats.as_ref(),
                    transforms: transforms.as_ref(),
                    control: &control,
                },
            )
            .unwrap();
        }
        assert_eq!(listings.load(Ordering::Relaxed), 1);
        assert_eq!(metadata_reads.load(Ordering::Relaxed), 2);
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
    fn http_numeric_template_membership_revalidates_one_path_without_expansion() {
        let resource_id = ResourceId::new("archive.events").unwrap();
        let glob = "part-{000000..999999}.parquet";

        assert!(http_glob_contains(&resource_id, glob, "part-000000.parquet").unwrap());
        assert!(http_glob_contains(&resource_id, glob, "part-999999.parquet").unwrap());
        assert!(!http_glob_contains(&resource_id, glob, "part-1000000.parquet").unwrap());
        assert!(!http_glob_contains(&resource_id, glob, "part-1.parquet").unwrap());
        assert!(!http_glob_contains(&resource_id, glob, "other-000001.parquet").unwrap());
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
            crate::test_egress_scope(),
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
            schema_discovery: None,
            compression: FileCompressionDeclaration::auto(),
            spool_mode: crate::FileSpoolMode::Overlap,
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let resource_id = ResourceId::new("events.raw").unwrap();
        let resolved = resolve_remote_matches(
            &resource_id,
            &plan,
            transport.as_ref(),
            &crate::test_egress_scope(),
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
            crate::test_egress_scope(),
        );
        let plan = FileResourcePlan {
            source: "events".to_owned(),
            root: root.path().to_string_lossy().into_owned(),
            glob: "events.csv".to_owned(),
            format: Some(FileFormatDeclaration::csv()),
            format_declared: true,
            format_options: serde_json::json!({}),
            schema_discovery: None,
            compression: FileCompressionDeclaration::none(),
            spool_mode: crate::FileSpoolMode::Overlap,
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let resource_id = ResourceId::new("events.csv").unwrap();
        let resolved = dependencies
            .with_transport(|transport, egress| {
                resolve_file_matches(
                    &resource_id,
                    &plan,
                    transport,
                    egress,
                    dependencies.formats(),
                    dependencies.transforms(),
                )
            })
            .unwrap();
        let probe = discover_local_binary_schema(
            &path,
            "events.csv",
            &dependencies,
            0,
            SchemaDiscoveryRequest {
                resource_id: &resource_id,
                format: plan.resolved_format().unwrap(),
                format_declared: plan.format_declared,
                format_options: &plan.format_options,
                discovery_kind: cdf_runtime::FormatDiscoveryKind::BoundedContent,
                transform_name: "none",
                maximum_bytes: 1024 * 1024,
                maximum_records: 1_000,
                cancellation: cdf_runtime::RunCancellation::default(),
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
    fn local_fixed_width_streams_through_registered_driver() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("events.fixed"),
            b"0001 Alice\n0002 Bob  \n",
        )
        .unwrap();
        let dependencies = FileRuntimeDependencies::new(
            FileTransportFacade::new(),
            crate::test_execution_services(),
            crate::test_format_registry(),
            crate::test_transform_registry(),
            crate::test_egress_scope(),
        );
        let plan = FileResourcePlan {
            source: "events".to_owned(),
            root: root.path().to_string_lossy().into_owned(),
            glob: "events.fixed".to_owned(),
            format: Some(FileFormatDeclaration::named("fixed_width").unwrap()),
            format_declared: true,
            format_options: serde_json::json!({
                "layout_version": 1,
                "unit": "bytes",
                "encoding": "utf8",
                "line_ending": "lf",
                "trim": "ascii",
                "null_tokens": [],
                "record_width": 10,
                "fields": [
                    {"name": "id", "start": 0, "end": 4},
                    {"name": "name", "start": 5, "end": 10}
                ],
                "required_gaps": [{"start": 4, "end": 5}],
                "max_record_bytes": 1024
            }),
            schema_discovery: Some(cdf_runtime::FormatDiscoveryKind::FormatMetadata),
            compression: FileCompressionDeclaration::none(),
            spool_mode: crate::FileSpoolMode::Overlap,
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let resource_id = ResourceId::new("events.fixed").unwrap();
        let resolved = dependencies
            .with_transport(|transport, egress| {
                resolve_file_matches(
                    &resource_id,
                    &plan,
                    transport,
                    egress,
                    dependencies.formats(),
                    dependencies.transforms(),
                )
            })
            .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let stream = stream_file_match_with_options_blocking(
            &resolved[0],
            plan.resolved_format().unwrap(),
            plan.format_options.clone(),
            ReadOptions::new(resource_id, PartitionId::new("fixed-file").unwrap()),
            &dependencies,
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
            2
        );
        let record_batch = batches[0].record_batch().unwrap();
        assert_eq!(
            record_batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(1),
            2
        );
        assert_eq!(
            record_batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(1),
            "Bob"
        );
        drop(batches);
        assert_eq!(
            dependencies.execution().memory().snapshot().current_bytes,
            0
        );
    }

    #[test]
    fn local_ndjson_full_content_discovery_replays_the_same_source_for_extraction() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("events.ndjson");
        std::fs::write(
            &path,
            b"{\"id\":1,\"name\":\"alpha\"}\n{\"id\":2,\"name\":\"beta\",\"late\":true}\n",
        )
        .unwrap();
        let dependencies = FileRuntimeDependencies::new(
            FileTransportFacade::new(),
            crate::test_execution_services(),
            crate::test_format_registry(),
            crate::test_transform_registry(),
            crate::test_egress_scope(),
        );
        let plan = FileResourcePlan {
            source: "events".to_owned(),
            root: root.path().to_string_lossy().into_owned(),
            glob: "events.ndjson".to_owned(),
            format: Some(FileFormatDeclaration::ndjson()),
            format_declared: true,
            format_options: serde_json::json!({}),
            schema_discovery: Some(cdf_runtime::FormatDiscoveryKind::FullContent),
            compression: FileCompressionDeclaration::none(),
            spool_mode: crate::FileSpoolMode::Overlap,
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let resource_id = ResourceId::new("events.ndjson").unwrap();
        let resolved = dependencies
            .with_transport(|transport, egress| {
                resolve_file_matches(
                    &resource_id,
                    &plan,
                    transport,
                    egress,
                    dependencies.formats(),
                    dependencies.transforms(),
                )
            })
            .unwrap();
        let probe = discover_local_binary_schema(
            &path,
            "events.ndjson",
            &dependencies,
            0,
            SchemaDiscoveryRequest {
                resource_id: &resource_id,
                format: plan.resolved_format().unwrap(),
                format_declared: plan.format_declared,
                format_options: &plan.format_options,
                discovery_kind: cdf_runtime::FormatDiscoveryKind::FullContent,
                transform_name: "none",
                maximum_bytes: 8,
                maximum_records: 1,
                cancellation: cdf_runtime::RunCancellation::default(),
            },
        )
        .unwrap();
        assert_eq!(probe.schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(probe.schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(probe.schema.field(2).data_type(), &DataType::Boolean);
        assert_eq!(probe.source_identity["content_coverage"], "full_content");
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
            crate::test_egress_scope(),
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
        assert!(retains_sequential_discovery_payload(
            &descriptor,
            cdf_runtime::FormatDiscoveryKind::BoundedContent
        ));

        descriptor.discovery = cdf_runtime::FormatDiscoveryCapabilities::only(
            cdf_runtime::FormatDiscoveryKind::FullContent,
        );
        assert!(retains_sequential_discovery_payload(
            &descriptor,
            cdf_runtime::FormatDiscoveryKind::FullContent
        ));

        descriptor.discovery = cdf_runtime::FormatDiscoveryCapabilities::only(
            cdf_runtime::FormatDiscoveryKind::FormatMetadata,
        );
        assert!(!retains_sequential_discovery_payload(
            &descriptor,
            cdf_runtime::FormatDiscoveryKind::FormatMetadata
        ));
        descriptor.discovery = cdf_runtime::FormatDiscoveryCapabilities::only(
            cdf_runtime::FormatDiscoveryKind::FullContent,
        );
        descriptor.source_access = cdf_runtime::FormatSourceAccess::Adaptive;
        assert!(!retains_sequential_discovery_payload(
            &descriptor,
            cdf_runtime::FormatDiscoveryKind::FullContent
        ));
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
            crate::test_egress_scope(),
        );
        let plan = FileResourcePlan {
            source: "events".to_owned(),
            root: root.path().to_string_lossy().into_owned(),
            glob: "events.json".to_owned(),
            format: Some(FileFormatDeclaration::json()),
            format_declared: true,
            format_options: serde_json::json!({}),
            schema_discovery: None,
            compression: FileCompressionDeclaration::none(),
            spool_mode: crate::FileSpoolMode::Overlap,
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let resource_id = ResourceId::new("events.json").unwrap();
        let resolved = dependencies
            .with_transport(|transport, egress| {
                resolve_file_matches(
                    &resource_id,
                    &plan,
                    transport,
                    egress,
                    dependencies.formats(),
                    dependencies.transforms(),
                )
            })
            .unwrap();
        let probe = discover_local_binary_schema(
            &path,
            "events.json",
            &dependencies,
            0,
            SchemaDiscoveryRequest {
                resource_id: &resource_id,
                format: plan.resolved_format().unwrap(),
                format_declared: plan.format_declared,
                format_options: &plan.format_options,
                discovery_kind: cdf_runtime::FormatDiscoveryKind::BoundedContent,
                transform_name: "none",
                maximum_bytes: 1024 * 1024,
                maximum_records: 1_000,
                cancellation: cdf_runtime::RunCancellation::default(),
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
            schema_discovery: None,
            compression: FileCompressionDeclaration::none(),
            spool_mode: crate::FileSpoolMode::Overlap,
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
    assert_eq!(
        schema_observation_byte_limit(
            1,
            1_024,
            &context,
            cdf_runtime::FormatDiscoveryKind::FullContent,
        )
        .unwrap(),
        1
    );
}
