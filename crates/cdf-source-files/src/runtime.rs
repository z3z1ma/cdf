use std::{
    collections::BTreeMap,
    fmt,
    fs::{self, File},
    io::{Read, Seek, SeekFrom},
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::UNIX_EPOCH,
};

use arrow_schema::SchemaRef;
use cdf_formats::ReadOptions;
use cdf_kernel::{
    BatchStream, BoxFuture, CdfError, DeliveryGuarantee, EffectiveSchemaRuntime,
    PLAN_PHYSICAL_SCHEMA_HASH_KEY, PLAN_SCHEMA_OBSERVATION_BINDING_KEY,
    PLAN_SCHEMA_OBSERVATION_ID_KEY, PartitionId, PartitionPlan, PlanId, QueryableResource,
    ResourceCapabilities, ResourceDescriptor, ResourceId, ResourceStream, Result, ScanPlan,
    ScanRequest, SchemaHash, ScopeKey, SourcePosition, TypePolicyAllowances, WriteDisposition,
};
use cdf_memory::{ConsumerKey, MemoryClass};
use cdf_runtime::{
    ByteSource, ByteTransformId, ByteTransformRegistry, DecodePlanningRequest, ExecutionServices,
    FormatDiscoveryRequest, FormatDriver, FormatRegistry, PhysicalDecodeRequest,
    SequentialReadRequest, TransformSourceConfig, TransformedByteSource,
};
#[cfg(test)]
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

use crate::{
    ByteRange, FileCompressionDeclaration, FileFormatDeclaration, FileIdentityMetadata,
    FileResourcePlan, FileTransport, FileTransportFacade, FileTransportLocation,
    FileTransportResource, LocalByteSource,
};

const NATIVE_TARGET_BATCH_ROWS: usize = 64 * 1024;
const NATIVE_TARGET_BATCH_BYTES: u64 = 16 * 1024 * 1024;
const NATIVE_STREAM_ITEMS: usize = 2;

#[derive(Clone)]
pub struct FileRuntimeDependencies {
    transport: Arc<dyn FileTransport>,
    execution: ExecutionServices,
    formats: Arc<FormatRegistry>,
    transforms: Arc<ByteTransformRegistry>,
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
            max_spool_bytes: DEFAULT_MAX_FILE_SPOOL_BYTES,
        }
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

    pub(crate) fn formats(&self) -> &Arc<FormatRegistry> {
        &self.formats
    }

    pub fn transforms(&self) -> &Arc<ByteTransformRegistry> {
        &self.transforms
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
}

pub fn discover_local_binary_schema_bounded(
    path: impl AsRef<Path>,
    dependencies: &FileRuntimeDependencies,
    format: &FileFormatDeclaration,
    transform_name: &str,
    initial_bytes_read: u64,
    max_metadata_bytes: u64,
) -> Result<BoundedBinarySchemaProbe> {
    let path = path.as_ref();
    let source_size = fs::metadata(path)
        .map_err(|error| CdfError::data(format!("stat {} for discovery: {error}", path.display())))?
        .len();
    let driver = dependencies.formats().resolve(format.as_str())?;
    let upstream: Arc<dyn ByteSource> = Arc::new(LocalByteSource::open(
        path,
        dependencies.execution().memory(),
    )?);
    let transform_id = (transform_name != "none")
        .then(|| dependencies.transforms().resolve_name(transform_name))
        .transpose()?
        .map(|driver| driver.descriptor().transform_id.clone());
    let source = match transform_id.as_ref() {
        Some(transform_id) => transformed_byte_source(upstream, transform_id, dependencies)?,
        None => upstream,
    };
    let logical_source_identity = source.identity().clone();
    let needs_spool = transform_id.is_some()
        && driver.descriptor().source_access != cdf_runtime::FormatSourceAccess::Sequential;
    let options = driver.canonical_options(serde_json::json!({}))?;
    let discovery_memory = dependencies.execution().memory();
    let observation = dependencies.execution().run_io({
        let dependencies = dependencies.clone();
        let driver = Arc::clone(&driver);
        let source = Arc::clone(&source);
        async move {
            let mut spool = None;
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
            } else {
                source
            };
            let observation = driver
                .discover(
                    source,
                    FormatDiscoveryRequest {
                        options,
                        maximum_bytes: max_metadata_bytes,
                        maximum_records: 1_000,
                        memory: discovery_memory,
                        cancellation: cdf_runtime::RunCancellation::default(),
                    },
                )
                .await?;
            drop(spool);
            Ok::<_, CdfError>(observation)
        }
    })?;
    let schema = observation.arrow_schema;
    let inner_probe_bytes = observation.sampled_bytes;
    let mut source_identity = BTreeMap::from([
        ("stable_id".to_owned(), logical_source_identity.stable_id),
        ("format".to_owned(), format.as_str().to_owned()),
        (
            "format_driver_version".to_owned(),
            driver.descriptor().semantic_version.clone(),
        ),
    ]);
    merge_discovery_evidence(&mut source_identity, observation.evidence)?;
    if let Some(generation) = observation.identity.generation {
        source_identity.insert("generation".to_owned(), generation);
    }
    if let Some(checksum) = observation.identity.checksum {
        source_identity.insert("checksum".to_owned(), checksum);
    }
    source_identity.insert("path".to_owned(), path.to_string_lossy().into_owned());
    source_identity.insert("compression".to_owned(), transform_name.to_owned());
    source_identity.insert("source_size_bytes".to_owned(), source_size.to_string());
    Ok(BoundedBinarySchemaProbe {
        schema,
        source_identity,
        probe_bytes_read: initial_bytes_read.saturating_add(if transform_id.is_some() {
            source_size.saturating_add(inner_probe_bytes)
        } else {
            inner_probe_bytes
        }),
    })
}

pub fn discover_transport_binary_schema_bounded(
    resource: FileTransportResource,
    dependencies: &FileRuntimeDependencies,
    format: &FileFormatDeclaration,
    transform_name: &str,
    max_metadata_bytes: u64,
) -> Result<BoundedBinarySchemaProbe> {
    let metadata = dependencies.with_transport(|transport| transport.metadata(&resource))?;
    let size_bytes = metadata.size_bytes.ok_or_else(|| {
        CdfError::data(format!(
            "remote binary discovery for `{}` did not receive byte-size metadata",
            diagnostic_location(&metadata.location)
        ))
    })?;
    let driver = dependencies.formats().resolve(format.as_str())?;
    let upstream = dependencies
        .with_transport(|transport| {
            transport.open_byte_source(&resource, &metadata, dependencies.execution().memory())
        })?
        .ok_or_else(|| {
            CdfError::contract(format!(
                "file transport for `{}` does not expose the required byte-source runtime",
                diagnostic_location(&metadata.location)
            ))
        })?;
    let transform_id = (transform_name != "none")
        .then(|| dependencies.transforms().resolve_name(transform_name))
        .transpose()?
        .map(|driver| driver.descriptor().transform_id.clone());
    let source = match transform_id.as_ref() {
        Some(transform_id) => transformed_byte_source(upstream, transform_id, dependencies)?,
        None => upstream,
    };
    let logical_source_identity = source.identity().clone();
    let needs_spool = driver.descriptor().source_access
        != cdf_runtime::FormatSourceAccess::Sequential
        && (!source.capabilities().seekable || transform_id.is_some());
    let execution = dependencies.execution().clone();
    let memory = execution.memory();
    let options = driver.canonical_options(serde_json::json!({}))?;
    let observation = execution.run_io({
        let dependencies = dependencies.clone();
        let driver = Arc::clone(&driver);
        async move {
            let mut spool = None;
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
            } else {
                source
            };
            let observation = driver
                .discover(
                    source,
                    FormatDiscoveryRequest {
                        options,
                        maximum_bytes: max_metadata_bytes,
                        maximum_records: 1_000,
                        memory,
                        cancellation: cdf_runtime::RunCancellation::default(),
                    },
                )
                .await?;
            drop(spool);
            Ok::<_, CdfError>(observation)
        }
    })?;
    let mut source_identity = BTreeMap::from([
        ("stable_id".to_owned(), logical_source_identity.stable_id),
        ("format".to_owned(), format.as_str().to_owned()),
        (
            "format_driver_version".to_owned(),
            driver.descriptor().semantic_version.clone(),
        ),
        ("compression".to_owned(), transform_name.to_owned()),
        ("source_size_bytes".to_owned(), size_bytes.to_string()),
        ("size_bytes".to_owned(), size_bytes.to_string()),
    ]);
    merge_discovery_evidence(&mut source_identity, observation.evidence)?;
    let mut probe = BoundedBinarySchemaProbe {
        schema: observation.arrow_schema,
        source_identity,
        probe_bytes_read: observation.sampled_bytes,
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
    if transform_name == "none" {
        probe.probe_bytes_read = size_bytes.saturating_add(probe.probe_bytes_read);
    }
    Ok(probe)
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
        .map(|path| local_file_discovery_candidate(resource_id, &root, path, plan, transforms))
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
    dependencies: FileRuntimeDependencies,
}

impl FileResource {
    pub fn new(
        descriptor: ResourceDescriptor,
        schema: SchemaRef,
        capabilities: ResourceCapabilities,
        plan: FileResourcePlan,
        type_policy_allowances: TypePolicyAllowances,
        effective_schema_runtime: Option<EffectiveSchemaRuntime>,
        dependencies: FileRuntimeDependencies,
    ) -> Result<Self> {
        Ok(Self {
            descriptor,
            schema,
            capabilities,
            plan,
            type_policy_allowances,
            effective_schema_runtime: effective_schema_runtime.map(Arc::new),
            dependencies,
        })
    }

    pub fn validate_runtime_dependencies(&self) -> Result<()> {
        Ok(())
    }

    pub fn open_preview(&self, partition: PartitionPlan) -> BoxFuture<'_, Result<BatchStream>> {
        let descriptor = self.descriptor.clone();
        let schema = Arc::clone(&self.schema);
        let plan = self.plan.clone();
        let dependencies = self.dependencies.clone();
        open_file_resource_with_dependencies(
            &descriptor,
            schema,
            &plan,
            partition,
            dependencies,
            self.type_policy_allowances,
            self.effective_schema_runtime.clone(),
        )
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
                self.dependencies.transforms(),
            )
        })
    }

    fn open(&self, partition: PartitionPlan) -> BoxFuture<'_, Result<BatchStream>> {
        let descriptor = self.descriptor.clone();
        let schema = Arc::clone(&self.schema);
        let plan = self.plan.clone();
        open_file_resource_with_dependencies(
            &descriptor,
            schema,
            &plan,
            partition,
            self.dependencies.clone(),
            self.type_policy_allowances,
            self.effective_schema_runtime.clone(),
        )
    }

    fn attest_partition(
        &self,
        partition: &PartitionPlan,
    ) -> BoxFuture<'_, Result<Option<cdf_kernel::PartitionAttestation>>> {
        let descriptor = self.descriptor.clone();
        let plan = self.plan.clone();
        let partition = partition.clone();
        let dependencies = self.dependencies.clone();
        let discovery_budget = self
            .effective_schema_runtime
            .as_ref()
            .and_then(|runtime| runtime.discovery_executor_budget.clone());
        Box::pin(async move {
            let resolved = dependencies.with_transport(|transport| {
                validate_partition(
                    &descriptor,
                    &plan,
                    &partition,
                    transport,
                    dependencies.transforms(),
                )
            })?;
            let transform_name = resolved.compression.mode_name().to_owned();
            let processed_position = SourcePosition::FileManifest(cdf_kernel::FileManifest {
                version: 1,
                files: vec![cdf_kernel::FilePosition {
                    path: resolved.path_text,
                    size_bytes: resolved.size_bytes,
                    etag: resolved.etag,
                    object_version: resolved.version,
                    sha256: resolved.sha256,
                }],
            });
            let physical_schema_hash = match (&resolved.open, plan.format.as_str()) {
                (ResolvedFileOpen::LocalPath(path), format @ ("parquet" | "arrow_ipc")) => {
                    let budget = discovery_budget.as_ref().ok_or_else(|| {
                        CdfError::data(
                            "schema-observation attestation requires the plan-recorded discovery executor budget",
                        )
                    })?;
                    let probe = discover_local_binary_schema_bounded(
                        path,
                        &dependencies,
                        &FileFormatDeclaration::named(format)?,
                        &transform_name,
                        0,
                        budget.max_metadata_bytes_per_file,
                    )?;
                    Some(cdf_formats::schema_hash(probe.schema.as_ref())?)
                }
                _ => None,
            };
            Ok(Some(cdf_kernel::PartitionAttestation::new(
                processed_position,
                physical_schema_hash,
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

struct AccountedSpool {
    file: tempfile::NamedTempFile,
    _reservation: cdf_runtime::SpillReservation,
}

impl AccountedSpool {
    fn path(&self) -> &Path {
        self.file.path()
    }
}

struct PreparedFilePartition {
    resolved: ResolvedFileMatch,
    input: PreparedFileInput,
    options: ReadOptions,
    physical_schema_authority: PhysicalSchemaAuthority,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CompressionEvidence {
    transform_id: Option<ByteTransformId>,
    extension_signal: CompressionSignal,
    magic_signal: CompressionSignal,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct CompressionSignal(Option<ByteTransformId>);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FormatSignal {
    Unknown,
    Parquet,
    ArrowIpc,
    ArrowIpcStream,
}

impl FormatSignal {
    fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Parquet => "parquet",
            Self::ArrowIpc => "arrow_ipc",
            Self::ArrowIpcStream => "arrow_ipc_stream",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FormatEvidence {
    extension_signal: FormatSignal,
    magic_signal: FormatSignal,
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
    transforms: &ByteTransformRegistry,
) -> Result<Vec<PartitionPlan>> {
    let transport = FileTransportFacade::new();
    file_partitions_for_plan_with_transport(descriptor, plan, &transport, transforms)
}

pub(crate) fn file_partitions_for_plan_with_transport(
    descriptor: &ResourceDescriptor,
    plan: &FileResourcePlan,
    transport: &dyn FileTransport,
    transforms: &ByteTransformRegistry,
) -> Result<Vec<PartitionPlan>> {
    let matches = resolve_file_matches(&descriptor.resource_id, plan, transport, transforms)?;
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
    descriptor: &ResourceDescriptor,
    _declared_schema: SchemaRef,
    plan: &FileResourcePlan,
    partition: PartitionPlan,
    dependencies: FileRuntimeDependencies,
    allowances: cdf_kernel::TypePolicyAllowances,
    effective_schema_runtime: Option<Arc<EffectiveSchemaRuntime>>,
) -> BoxFuture<'static, Result<BatchStream>> {
    let descriptor = descriptor.clone();
    let plan = plan.clone();
    let prepared = match prepare_file_partition(
        &descriptor,
        &plan,
        &partition,
        &dependencies,
        allowances,
        effective_schema_runtime.as_deref(),
    ) {
        Ok(prepared) => prepared,
        Err(error) => return Box::pin(async move { Err(error) }),
    };
    let execution = dependencies.execution().clone();
    let mut scope_hasher = Sha256::new();
    scope_hasher.update(descriptor.resource_id.as_str().as_bytes());
    scope_hasher.update([0]);
    scope_hasher.update(partition.partition_id.as_str().as_bytes());
    let scope_id = format!("file-open-{}", &hex::encode(scope_hasher.finalize())[..16]);
    let stream = execution.spawn_io_stream(
        &scope_id,
        NATIVE_STREAM_ITEMS,
        move |mut sender, _cancellation| async move {
            let mut batches =
                stream_prepared_file_match(prepared, &plan.format, &dependencies, _cancellation)
                    .await?;
            while let Some(batch) = batches.try_next().await? {
                sender.send(batch).await?;
            }
            Ok(())
        },
    );
    Box::pin(async move { Ok(Box::pin(stream?) as BatchStream) })
}

fn prepare_file_partition(
    descriptor: &ResourceDescriptor,
    plan: &FileResourcePlan,
    partition: &PartitionPlan,
    dependencies: &FileRuntimeDependencies,
    _allowances: cdf_kernel::TypePolicyAllowances,
    effective_schema_runtime: Option<&EffectiveSchemaRuntime>,
) -> Result<PreparedFilePartition> {
    let resolved = dependencies.with_transport(|transport| {
        validate_partition(
            descriptor,
            plan,
            partition,
            transport,
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
    let source_access = dependencies
        .formats()
        .resolve(file_format_name(&plan.format))?
        .descriptor()
        .source_access;
    let input = prepare_file_input(&resolved, source_access, dependencies)?;
    Ok(PreparedFilePartition {
        resolved,
        input,
        options,
        physical_schema_authority: PhysicalSchemaAuthority {
            hash: planned_physical_schema_hash,
            schema: planned_physical_schema,
        },
    })
}

fn prepare_file_input(
    resolved: &ResolvedFileMatch,
    source_access: cdf_runtime::FormatSourceAccess,
    dependencies: &FileRuntimeDependencies,
) -> Result<PreparedFileInput> {
    if resolved.compression.transform_id.is_none() {
        let expected = expected_file_identity(resolved);
        let source: Arc<dyn ByteSource> = match &resolved.open {
            ResolvedFileOpen::LocalPath(path) => Arc::new(LocalByteSource::open(
                path,
                dependencies.execution().memory(),
            )?),
            ResolvedFileOpen::Transport(resource) => dependencies
                .with_transport(|transport| {
                    transport.open_byte_source(
                        resource,
                        &expected,
                        dependencies.execution().memory(),
                    )
                })?
                .ok_or_else(|| missing_byte_source_runtime(resolved))?,
        };
        return Ok(
            if matches!(resolved.open, ResolvedFileOpen::Transport(_))
                && source_access == cdf_runtime::FormatSourceAccess::Adaptive
            {
                PreparedFileInput::SpoolSource {
                    source,
                    size_bytes: Some(resolved.size_bytes),
                }
            } else {
                PreparedFileInput::Source(source)
            },
        );
    }
    if let Some(transform_id) = &resolved.compression.transform_id {
        let expected = expected_file_identity(resolved);
        let upstream: Arc<dyn ByteSource> = match &resolved.open {
            ResolvedFileOpen::LocalPath(path) => Arc::new(LocalByteSource::open(
                path,
                dependencies.execution().memory(),
            )?),
            ResolvedFileOpen::Transport(resource) => dependencies
                .with_transport(|transport| {
                    transport.open_byte_source(
                        resource,
                        &expected,
                        dependencies.execution().memory(),
                    )
                })?
                .ok_or_else(|| missing_byte_source_runtime(resolved))?,
        };
        let transformed = transformed_byte_source(upstream, transform_id, dependencies)?;
        return Ok(
            if source_access == cdf_runtime::FormatSourceAccess::Adaptive {
                PreparedFileInput::SpoolSource {
                    source: transformed,
                    size_bytes: None,
                }
            } else {
                PreparedFileInput::Source(transformed)
            },
        );
    }
    Err(CdfError::internal(
        "file preparation reached an unclassified compression state",
    ))
}

fn missing_byte_source_runtime(resolved: &ResolvedFileMatch) -> CdfError {
    CdfError::contract(format!(
        "file transport for `{}` does not expose the required byte-source runtime",
        diagnostic_location(&resolved.path_text)
    ))
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
        modified: resolved.modified_ms.clone(),
    }
}

async fn stream_prepared_file_match(
    prepared: PreparedFilePartition,
    declaration: &FileFormatDeclaration,
    dependencies: &FileRuntimeDependencies,
    cancellation: cdf_runtime::RunCancellation,
) -> Result<BatchStream> {
    let PreparedFilePartition {
        resolved,
        input: prepared,
        options,
        physical_schema_authority,
    } = prepared;
    let position = Some(SourcePosition::FileManifest(cdf_kernel::FileManifest {
        version: 1,
        files: vec![cdf_kernel::FilePosition {
            path: resolved.path_text.clone(),
            size_bytes: resolved.size_bytes,
            etag: resolved.etag.clone(),
            object_version: resolved.version.clone(),
            sha256: resolved.sha256.clone(),
        }],
    }));
    let (source, spool_guard): (Arc<dyn ByteSource>, Option<Arc<AccountedSpool>>) = match prepared {
        PreparedFileInput::Source(source) => (source, None),
        PreparedFileInput::SpoolSource { source, size_bytes } => {
            let spool = Arc::new(
                spool_byte_source_async(source, size_bytes, dependencies, cancellation.clone())
                    .await?,
            );
            let local = Arc::new(LocalByteSource::open(
                spool.path(),
                dependencies.execution().memory(),
            )?);
            (local, Some(spool))
        }
    };

    stream_registered_format(
        source,
        spool_guard,
        dependencies
            .formats()
            .resolve(file_format_name(declaration))?,
        options,
        position,
        physical_schema_authority,
        dependencies,
    )
}

#[cfg(test)]
fn stream_file_match_blocking(
    resolved: &ResolvedFileMatch,
    declaration: &FileFormatDeclaration,
    options: ReadOptions,
    dependencies: &FileRuntimeDependencies,
    physical_schema_authority: PhysicalSchemaAuthority,
) -> Result<BatchStream> {
    let prepared = PreparedFilePartition {
        resolved: resolved.clone(),
        input: prepare_file_input(
            resolved,
            dependencies
                .formats()
                .resolve(file_format_name(declaration))?
                .descriptor()
                .source_access,
            dependencies,
        )?,
        options,
        physical_schema_authority,
    };
    let declaration = declaration.clone();
    let dependencies = dependencies.clone();
    let execution = dependencies.execution().clone();
    execution.run_io(async move {
        stream_prepared_file_match(
            prepared,
            &declaration,
            &dependencies,
            cdf_runtime::RunCancellation::default(),
        )
        .await
    })
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
    let mut hasher = Sha256::new();
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
        hasher.update(chunk.payload());
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
    if let Some(expected) = &source.identity().checksum {
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

fn stream_registered_format(
    source: Arc<dyn ByteSource>,
    spool_guard: Option<Arc<AccountedSpool>>,
    driver: Arc<dyn FormatDriver>,
    options: ReadOptions,
    source_position: Option<SourcePosition>,
    physical_schema_authority: PhysicalSchemaAuthority,
    dependencies: &FileRuntimeDependencies,
) -> Result<BatchStream> {
    let memory = dependencies.execution().memory();
    let scope_id = format!(
        "format-{}-{}",
        driver.descriptor().format_id,
        options.batch_id_prefix
    );
    let stream = dependencies.execution().spawn_io_stream(
        &scope_id,
        NATIVE_STREAM_ITEMS,
        move |mut sender, cancellation| async move {
            let _spool_guard = spool_guard;
            let options_json = driver.canonical_options(serde_json::json!({}))?;
            let physical_schema = match physical_schema_authority.schema {
                Some(schema) => {
                    let schema_hash =
                        cdf_contract::canonical_arrow_schema_hash(schema.as_ref())?;
                    if let Some(planned_hash) = &physical_schema_authority.hash
                        && planned_hash != &schema_hash
                    {
                        return Err(CdfError::data(format!(
                            "plan physical schema catalog hash {schema_hash} does not match partition authority {planned_hash}"
                        )));
                    }
                    schema
                }
                None => {
                    let observation = driver
                        .discover(
                            source.clone(),
                            FormatDiscoveryRequest {
                                options: options_json.clone(),
                                maximum_bytes: 16 * 1024 * 1024,
                                maximum_records: 1_000,
                                memory: Arc::clone(&memory),
                                cancellation: cancellation.clone(),
                            },
                        )
                        .await?;
                    let observed_hash = cdf_contract::canonical_arrow_schema_hash(
                        observation.arrow_schema.as_ref(),
                    )?;
                    if let Some(planned_hash) = &physical_schema_authority.hash
                        && planned_hash != &observed_hash
                    {
                        return Err(CdfError::data(format!(
                            "physical schema changed before decode: planned {planned_hash}, observed {observed_hash}"
                        )));
                    }
                    observation.arrow_schema
                }
            };
            let units = driver
                .plan_decode_units(
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
            for unit in units {
                let mut decoded = driver
                    .decode(
                        source.clone(),
                        PhysicalDecodeRequest {
                            options: options_json.clone(),
                            unit,
                            resource_id: options.resource_id.clone(),
                            partition_id: options.partition_id.clone(),
                            batch_id_prefix: options.batch_id_prefix.clone(),
                            physical_schema: Arc::clone(&physical_schema),
                            source_position: source_position.clone(),
                            projection: None,
                            predicates: Vec::new(),
                            target_batch_rows: NATIVE_TARGET_BATCH_ROWS,
                            target_batch_bytes: NATIVE_TARGET_BATCH_BYTES,
                            memory: Arc::clone(&memory),
                            cancellation: cancellation.clone(),
                        },
                    )
                    .await?;
                while let Some(batch) = decoded.try_next().await? {
                    sender.send(batch.into_batch()?).await?;
                }
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
    let matches = resolve_file_matches(&descriptor.resource_id, plan, transport, transforms)?;
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
    match (&resolved.sha256, &resolved.etag, &resolved.version) {
        (Some(sha256), _, _) => {
            if partition.metadata.get("sha256").map(String::as_str) != Some(sha256.as_str()) {
                return Err(CdfError::data(format!(
                    "declarative file partition `{path}` changed checksum after planning"
                )));
            }
        }
        (None, Some(etag), _) => {
            if partition.metadata.get("etag").map(String::as_str) != Some(etag.as_str()) {
                return Err(CdfError::data(format!(
                    "declarative file partition `{path}` changed ETag after planning"
                )));
            }
        }
        (None, None, Some(version)) => {
            if partition.metadata.get("version").map(String::as_str) != Some(version.as_str()) {
                return Err(CdfError::data(format!(
                    "declarative file partition `{path}` changed object version after planning"
                )));
            }
        }
        (None, None, None) => {
            return Err(CdfError::contract(format!(
                "declarative file partition `{path}` requires checksum, ETag, or object version metadata"
            )));
        }
    }
    validate_compression_metadata(partition, &resolved, &plan.compression, path)?;
    if records_format_metadata(plan) {
        validate_partition_metadata_value(
            partition,
            "format",
            file_format_name(&plan.format),
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
            resolved.format.extension_signal.as_str(),
            path,
        )?;
        validate_partition_metadata_value(
            partition,
            "format_magic",
            resolved.format.magic_signal.as_str(),
            path,
        )?;
    }
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
    if records_format_metadata(plan) {
        metadata.insert(
            "format".to_owned(),
            file_format_name(&plan.format).to_owned(),
        );
        metadata.insert(
            "format_declared".to_owned(),
            plan.format_declared.to_string(),
        );
        metadata.insert(
            "format_extension".to_owned(),
            file.format.extension_signal.as_str().to_owned(),
        );
        metadata.insert(
            "format_magic".to_owned(),
            file.format.magic_signal.as_str().to_owned(),
        );
    }
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
    transforms: &ByteTransformRegistry,
) -> Result<Vec<ResolvedFileMatch>> {
    if is_http_root(&plan.root) {
        return resolve_http_file_match(resource_id, plan, transport, transforms);
    }
    if is_object_store_root(&plan.root) {
        return resolve_object_store_matches(resource_id, plan, transport, transforms);
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
        .map(|path| resolved_file_match(resource_id, &root, path, plan, transforms))
        .collect()
}

fn resolve_object_store_matches(
    resource_id: &ResourceId,
    plan: &FileResourcePlan,
    transport: &dyn FileTransport,
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
        let compression =
            resolve_transport_compression(plan, transport, &resource, &metadata, transforms)?;
        let format = resolve_transport_format(
            resource_id,
            plan,
            transport,
            &resource,
            &metadata,
            &compression,
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
        let Some(metadata) = transport.metadata_if_exists(&resource)? else {
            continue;
        };
        let compression =
            resolve_transport_compression(plan, transport, &resource, &metadata, transforms)?;
        let format = resolve_transport_format(
            resource_id,
            plan,
            transport,
            &resource,
            &metadata,
            &compression,
        )?;
        matches.push(resolved_transport_file_match(
            resource,
            metadata,
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
    let compression = resolve_local_compression(&path_text, &path, &plan.compression, transforms)?;
    let (format, _) = resolve_local_format(resource_id, plan, &path_text, &path, &compression)?;
    let sha256 = file_sha256(&path)?;
    Ok(ResolvedFileMatch {
        open: ResolvedFileOpen::LocalPath(path),
        path_text,
        size_bytes: metadata.len(),
        sha256: Some(sha256),
        etag: None,
        version: None,
        modified_ms,
        bytes_loaded: Some(metadata.len()),
        compression,
        format,
    })
}

fn local_file_discovery_candidate(
    resource_id: &ResourceId,
    root: &Path,
    path: PathBuf,
    plan: &FileResourcePlan,
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
    let compression =
        resolve_local_compression(&relative_path, &path, &plan.compression, transforms)?;
    let (_, format_bytes_read) =
        resolve_local_format(resource_id, plan, &relative_path, &path, &compression)?;
    Ok(LocalFileDiscoveryCandidate {
        path,
        relative_path,
        size_bytes: metadata.len(),
        compression: compression.mode_name().to_owned(),
        selection_bytes_read: metadata.len().min(4) + format_bytes_read,
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
    if sha256.is_none() && metadata.etag.is_none() && metadata.version.is_none() {
        return Err(CdfError::data(format!(
            "remote file metadata for `{}` must include an ETag, object version, or checksum for FileManifest identity",
            metadata.location
        )));
    }
    Ok(ResolvedFileMatch {
        open: ResolvedFileOpen::Transport(resource),
        path_text: metadata.location,
        size_bytes,
        sha256,
        etag: metadata.etag,
        version: metadata.version,
        modified_ms: metadata
            .modified
            .as_deref()
            .and_then(|modified| modified.strip_prefix("unix_ms:"))
            .map(str::to_owned),
        bytes_loaded: Some(size_bytes),
        compression,
        format,
    })
}

fn resolve_local_format(
    resource_id: &ResourceId,
    plan: &FileResourcePlan,
    path_text: &str,
    path: &Path,
    compression: &CompressionEvidence,
) -> Result<(FormatEvidence, u64)> {
    let extension_signal = format_extension_signal(path_text, compression);
    if !requires_binary_format_confirmation(plan, extension_signal) {
        return Ok((
            FormatEvidence {
                extension_signal,
                magic_signal: FormatSignal::Unknown,
            },
            0,
        ));
    }
    if compression.transform_id.is_some() {
        validate_compressed_format_extension(resource_id, plan, path_text, extension_signal)?;
        return Ok((
            FormatEvidence {
                extension_signal,
                magic_signal: FormatSignal::Unknown,
            },
            0,
        ));
    }
    let (magic_signal, bytes_read) = local_format_magic_signal(path)?;
    validate_format_evidence(resource_id, plan, path_text, extension_signal, magic_signal)?;
    Ok((
        FormatEvidence {
            extension_signal,
            magic_signal,
        },
        bytes_read,
    ))
}

fn resolve_transport_format(
    resource_id: &ResourceId,
    plan: &FileResourcePlan,
    transport: &dyn FileTransport,
    resource: &FileTransportResource,
    metadata: &FileIdentityMetadata,
    compression: &CompressionEvidence,
) -> Result<FormatEvidence> {
    let extension_signal = format_extension_signal(&metadata.location, compression);
    if !requires_binary_format_confirmation(plan, extension_signal) {
        return Ok(FormatEvidence {
            extension_signal,
            magic_signal: FormatSignal::Unknown,
        });
    }
    if compression.transform_id.is_some() {
        validate_compressed_format_extension(
            resource_id,
            plan,
            &diagnostic_location(&metadata.location),
            extension_signal,
        )?;
        return Ok(FormatEvidence {
            extension_signal,
            magic_signal: FormatSignal::Unknown,
        });
    }
    let size_bytes = metadata.size_bytes.ok_or_else(|| {
        CdfError::data(format!(
            "HTTP(S) file metadata for `{}` did not include Content-Length for format confirmation",
            diagnostic_location(&metadata.location)
        ))
    })?;
    let magic_signal = transport_format_magic_signal(transport, resource, size_bytes)?;
    validate_format_evidence(
        resource_id,
        plan,
        &diagnostic_location(&metadata.location),
        extension_signal,
        magic_signal,
    )?;
    Ok(FormatEvidence {
        extension_signal,
        magic_signal,
    })
}

fn requires_binary_format_confirmation(
    plan: &FileResourcePlan,
    extension_signal: FormatSignal,
) -> bool {
    matches!(plan.format.as_str(), "parquet" | "arrow_ipc")
        || extension_signal != FormatSignal::Unknown
}

fn validate_compressed_format_extension(
    resource_id: &ResourceId,
    plan: &FileResourcePlan,
    path_text: &str,
    extension_signal: FormatSignal,
) -> Result<()> {
    let expected = format_declaration_signal(&plan.format);
    if expected.is_none()
        || expected == Some(extension_signal)
        || (plan.format_declared && extension_signal == FormatSignal::Unknown)
    {
        return Ok(());
    }
    Err(CdfError::data(format!(
        "compressed file format mismatch for resource `{resource_id}`, file `{path_text}`: inner extension signal `{}` does not match declared `{}`",
        extension_signal.as_str(),
        file_format_name(&plan.format),
    )))
}

fn validate_format_evidence(
    resource_id: &ResourceId,
    plan: &FileResourcePlan,
    path_text: &str,
    extension_signal: FormatSignal,
    magic_signal: FormatSignal,
) -> Result<()> {
    let expected_signal = format_declaration_signal(&plan.format);
    let extension_agrees = expected_signal == Some(extension_signal)
        || (plan.format_declared && extension_signal == FormatSignal::Unknown);
    if extension_agrees && expected_signal == Some(magic_signal) {
        return Ok(());
    }

    let declared = if plan.format_declared {
        file_format_name(&plan.format)
    } else {
        "<omitted>"
    };
    let mut format_requirement = match plan.format.as_str() {
        "parquet" => "expected Parquet file framing with PAR1 header/footer; ",
        "arrow_ipc" => "expected Arrow IPC file framing with ARROW1 header/footer; ",
        _ => "",
    };
    if magic_signal == FormatSignal::ArrowIpcStream {
        format_requirement = "Arrow IPC stream framing is unsupported; expected Arrow IPC file framing with ARROW1 header/footer; ";
    }
    Err(CdfError::data(format!(
        "file format confirmation failed for resource `{resource_id}`, file `{path_text}`: declared format `{declared}`, inferred format `{}`, resolved format `{}`, extension signal `{}`, magic bytes signal `{}`; {format_requirement}make the file extension and magic agree with `format = \"{}\"`, or change the explicit format to match the file",
        extension_signal.as_str(),
        file_format_name(&plan.format),
        extension_signal.as_str(),
        magic_signal.as_str(),
        file_format_name(&plan.format),
    )))
}

fn format_declaration_signal(format: &FileFormatDeclaration) -> Option<FormatSignal> {
    match format.as_str() {
        "parquet" => Some(FormatSignal::Parquet),
        "arrow_ipc" => Some(FormatSignal::ArrowIpc),
        _ => None,
    }
}

fn file_format_name(format: &FileFormatDeclaration) -> &str {
    format.as_str()
}

fn records_format_metadata(plan: &FileResourcePlan) -> bool {
    matches!(plan.format.as_str(), "parquet" | "arrow_ipc")
}

fn format_extension_signal(path_text: &str, compression: &CompressionEvidence) -> FormatSignal {
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
    if path_without_query.ends_with(".parquet") {
        FormatSignal::Parquet
    } else if path_without_query.ends_with(".arrow") {
        FormatSignal::ArrowIpc
    } else {
        FormatSignal::Unknown
    }
}

fn local_format_magic_signal(path: &Path) -> Result<(FormatSignal, u64)> {
    let mut file = File::open(path).map_err(|error| {
        CdfError::data(format!(
            "open matched file {} for format detection: {error}",
            path.display()
        ))
    })?;
    let size_bytes = file
        .metadata()
        .map_err(|error| {
            CdfError::data(format!(
                "stat matched file {} for format detection: {error}",
                path.display()
            ))
        })?
        .len();
    let mut head = [0_u8; 6];
    let head_read = file.read(&mut head).map_err(|error| {
        CdfError::data(format!(
            "read matched file {} header for format detection: {error}",
            path.display()
        ))
    })?;
    let tail_length = size_bytes.min(6);
    file.seek(SeekFrom::Start(size_bytes.saturating_sub(tail_length)))
        .map_err(|error| {
            CdfError::data(format!(
                "seek matched file {} footer for format detection: {error}",
                path.display()
            ))
        })?;
    let mut tail = [0_u8; 6];
    let tail_read = file.read(&mut tail).map_err(|error| {
        CdfError::data(format!(
            "read matched file {} footer for format detection: {error}",
            path.display()
        ))
    })?;
    Ok((
        format_magic_signal(&head[..head_read], &tail[..tail_read]),
        (head_read + tail_read) as u64,
    ))
}

fn transport_format_magic_signal(
    transport: &dyn FileTransport,
    resource: &FileTransportResource,
    size_bytes: u64,
) -> Result<FormatSignal> {
    if size_bytes == 0 {
        return Ok(FormatSignal::Unknown);
    }
    let head_length = size_bytes.min(6);
    let tail_length = size_bytes.min(6);
    let head = transport.read_range(resource, ByteRange::new(0, head_length)?)?;
    let tail = transport.read_range(
        resource,
        ByteRange::new(size_bytes - tail_length, tail_length)?,
    )?;
    Ok(format_magic_signal(&head, &tail))
}

fn format_magic_signal(head: &[u8], tail: &[u8]) -> FormatSignal {
    if head.starts_with(b"PAR1") && tail.ends_with(b"PAR1") {
        FormatSignal::Parquet
    } else if head.starts_with(b"ARROW1") && tail.ends_with(b"ARROW1") {
        FormatSignal::ArrowIpc
    } else if head.starts_with(&[0xff, 0xff, 0xff, 0xff]) {
        FormatSignal::ArrowIpcStream
    } else {
        FormatSignal::Unknown
    }
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
    path: &Path,
    declared: &FileCompressionDeclaration,
    transforms: &ByteTransformRegistry,
) -> Result<CompressionEvidence> {
    let extension_signal = compression_extension_signal(path_text, transforms);
    let magic_signal = compression_magic_signal(path, transforms)?;
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
    transport: &dyn FileTransport,
    resource: &FileTransportResource,
    metadata: &FileIdentityMetadata,
    transforms: &ByteTransformRegistry,
) -> Result<CompressionEvidence> {
    let extension_signal = compression_extension_signal(&metadata.location, transforms);
    let size_bytes = metadata.size_bytes.ok_or_else(|| {
        CdfError::data(format!(
            "remote file metadata for `{}` omitted byte size for compression confirmation",
            diagnostic_location(&metadata.location)
        ))
    })?;
    let length = size_bytes.min(transforms.maximum_strong_magic_probe_bytes()?);
    let magic_signal = if length == 0 {
        CompressionSignal::default()
    } else {
        let magic = transport.read_range(resource, ByteRange::new(0, length)?)?;
        compression_magic_signal_from_bytes(&magic, transforms)?
    };
    resolve_compression_signals(
        &diagnostic_location(&metadata.location),
        &plan.compression,
        extension_signal,
        magic_signal,
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
            (Some(extension), None) => {
                let driver = transforms.resolve(extension)?;
                if driver.descriptor().magic.iter().any(|magic| magic.strong) {
                    return Err(compression_signal_error(
                        path_text,
                        declared,
                        &extension_signal,
                        &magic_signal,
                    ));
                }
                Some(extension.clone())
            }
            (None, None) => None,
        }
    } else if declared.is_none() {
        if extension_signal.transform_id().is_some() || magic_signal.transform_id().is_some() {
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
        let driver = transforms.resolve(&declared_id)?;
        if extension_signal
            .transform_id()
            .is_some_and(|extension| extension != &declared_id)
            || magic_signal
                .transform_id()
                .is_some_and(|magic| magic != &declared_id)
            || (magic_signal.transform_id().is_none()
                && driver.descriptor().magic.iter().any(|magic| magic.strong))
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

fn compression_magic_signal(
    path: &Path,
    transforms: &ByteTransformRegistry,
) -> Result<CompressionSignal> {
    let mut file = File::open(path).map_err(|error| {
        CdfError::data(format!(
            "open matched file {} for compression detection: {error}",
            path.display()
        ))
    })?;
    let probe_bytes = usize::try_from(transforms.maximum_strong_magic_probe_bytes()?)
        .map_err(|_| CdfError::contract("byte-transform magic probe exceeds usize"))?;
    let mut magic = vec![0_u8; probe_bytes];
    let bytes_read = file.read(&mut magic).map_err(|error| {
        CdfError::data(format!(
            "read matched file {} for compression detection: {error}",
            path.display()
        ))
    })?;
    compression_magic_signal_from_bytes(&magic[..bytes_read], transforms)
}

fn compression_magic_signal_from_bytes(
    magic: &[u8],
    transforms: &ByteTransformRegistry,
) -> Result<CompressionSignal> {
    Ok(CompressionSignal(
        transforms
            .detect_strong_magic(magic)?
            .map(|driver| driver.descriptor().transform_id.clone()),
    ))
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

fn file_sha256(path: &Path) -> Result<String> {
    let mut file = File::open(path).map_err(|error| {
        CdfError::data(format!("open matched file {}: {error}", path.display()))
    })?;
    let mut hasher = Sha256::new();
    std::io::copy(&mut file, &mut hasher).map_err(|error| {
        CdfError::data(format!("hash matched file {}: {error}", path.display()))
    })?;
    Ok(hex::encode(hasher.finalize()))
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
                    option_schema: serde_json::json!({
                        "type": "object",
                        "additionalProperties": false
                    }),
                    projection_pushdown: cdf_kernel::PushdownFidelity::Unsupported,
                    predicate_pushdown: cdf_kernel::PushdownFidelity::Unsupported,
                    source_access: cdf_runtime::FormatSourceAccess::Sequential,
                    decode_unit_policy: "whole_mock_file".to_owned(),
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
                    observed_schema: cdf_contract::ObservedSchema::from_arrow(schema.as_ref()),
                    arrow_schema: schema,
                    sampled_bytes: 4,
                    sampled_records: 0,
                    evidence: BTreeMap::new(),
                })
            })
        }

        fn plan_decode_units(
            &self,
            _source: Arc<dyn cdf_runtime::ByteSource>,
            request: cdf_runtime::DecodePlanningRequest,
        ) -> cdf_kernel::BoxFuture<'_, Result<Vec<cdf_runtime::DecodeUnitPlan>>> {
            Box::pin(async move {
                request.cancellation.check()?;
                Ok(vec![cdf_runtime::DecodeUnitPlan {
                    unit_id: "mock-file".to_owned(),
                    ordinal: 0,
                    extent: None,
                    estimated_working_set_bytes: 64,
                    independently_retryable: true,
                }])
            })
        }

        fn decode(
            &self,
            source: Arc<dyn cdf_runtime::ByteSource>,
            request: cdf_runtime::PhysicalDecodeRequest,
        ) -> cdf_kernel::BoxFuture<'_, Result<cdf_runtime::PhysicalDecodeStream>> {
            Box::pin(async move {
                request.cancellation.check()?;
                let preferred_chunk_bytes = (8 * 1024_u64).clamp(
                    source.capabilities().minimum_chunk_bytes,
                    source.capabilities().maximum_chunk_bytes,
                );
                let input = source
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
                    Self::schema(),
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
                    cdf_contract::canonical_arrow_schema_hash(request.physical_schema.as_ref())?,
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
            format: FileFormatDeclaration::named("external_mock").unwrap(),
            format_declared: true,
            compression: FileCompressionDeclaration::auto(),
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let resource_id = ResourceId::new("external.events").unwrap();
        let resolved = dependencies
            .with_transport(|transport| {
                resolve_file_matches(&resource_id, &plan, transport, dependencies.transforms())
            })
            .unwrap();
        assert_eq!(resolved[0].compression.mode_name(), "external_passthrough");
        let probe = discover_local_binary_schema_bounded(
            &path,
            &dependencies,
            &plan.format,
            "external_passthrough",
            0,
            1024,
        )
        .unwrap();
        assert_eq!(probe.schema.as_ref(), ExternalMockFormat::schema().as_ref());
        let stream = stream_file_match_blocking(
            &resolved[0],
            &plan.format,
            ReadOptions::new(resource_id, PartitionId::new("external-file").unwrap()),
            &dependencies,
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
        let record_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from_iter_values(0..150_000)) as ArrayRef,
                Arc::new(StringArray::from_iter_values(
                    (0..150_000).map(|value| format!("name-{value}")),
                )) as ArrayRef,
            ],
        )
        .unwrap();
        let bytes =
            cdf_package::transcode_record_batches_to_parquet_bytes(&[record_batch]).unwrap();
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
            Arc::new(
                LocalByteSource::open(temp.path(), dependencies.execution().memory()).unwrap(),
            ),
            None,
            driver,
            ReadOptions::new(
                ResourceId::new("events").unwrap(),
                PartitionId::new("file-0").unwrap(),
            ),
            None,
            PhysicalSchemaAuthority::default(),
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
            format: FileFormatDeclaration::parquet(),
            format_declared: true,
            compression: FileCompressionDeclaration::auto(),
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let resource_id = ResourceId::new("events.raw").unwrap();
        let resolved = dependencies
            .with_transport(|transport| {
                resolve_file_matches(&resource_id, &plan, transport, dependencies.transforms())
            })
            .unwrap();
        assert_eq!(resolved[0].compression.mode_name(), "gzip");
        assert_eq!(resolved[0].format.extension_signal, FormatSignal::Parquet);
        let probe = discover_local_binary_schema_bounded(
            root.path().join("events.parquet.gz"),
            &dependencies,
            &FileFormatDeclaration::parquet(),
            "gzip",
            0,
            64 * 1024 * 1024,
        )
        .unwrap();
        assert_eq!(probe.schema.as_ref(), schema.as_ref());
        assert_eq!(probe.source_identity.get("compression").unwrap(), "gzip");
        let stable_id = probe.source_identity.get("stable_id").unwrap();
        assert!(stable_id.ends_with("events.parquet.gz#transform:gzip"));
        let stream = stream_file_match_blocking(
            &resolved[0],
            &plan.format,
            ReadOptions::new(resource_id, PartitionId::new("file-events").unwrap()),
            &dependencies,
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
            format: FileFormatDeclaration::arrow_ipc(),
            format_declared: true,
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
                    crate::test_transform_registry().as_ref(),
                )
            })
            .unwrap();
        let stream = stream_file_match_blocking(
            &resolved[0],
            &plan.format,
            ReadOptions::new(resource_id, PartitionId::new("file-ipc").unwrap()),
            &dependencies,
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
    fn remote_parquet_full_scan_uses_verified_sequential_spool() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from_iter_values(0..100_000))],
        )
        .unwrap();
        let bytes = cdf_package::transcode_record_batches_to_parquet_bytes(&[batch]).unwrap();
        let store = Arc::new(InMemory::new());
        futures_executor::block_on(store.put(
            &ObjectPath::from("prod/events.parquet"),
            PutPayload::from(bytes.clone()),
        ))
        .unwrap();
        let facade = FileTransportFacade::new()
            .with_object_store("s3://parquet", store)
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
            format: FileFormatDeclaration::parquet(),
            format_declared: true,
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
                    dependencies.transforms(),
                )
            })
            .unwrap();
        assert!(matches!(
            prepare_file_input(
                &resolved[0],
                cdf_runtime::FormatSourceAccess::Adaptive,
                &dependencies,
            )
            .unwrap(),
            PreparedFileInput::SpoolSource { .. }
        ));
        let stream = stream_file_match_blocking(
            &resolved[0],
            &plan.format,
            ReadOptions::new(resource_id, PartitionId::new("file-parquet").unwrap()),
            &dependencies,
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
            format: FileFormatDeclaration::parquet(),
            format_declared: true,
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
            format: FileFormatDeclaration::ndjson(),
            format_declared: true,
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
            crate::test_transform_registry().as_ref(),
        )
        .unwrap();
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].compression.mode_name(), "gzip");
        let options = ReadOptions::new(resource_id, PartitionId::new("file-events").unwrap());
        let stream = stream_file_match_blocking(
            &resolved[0],
            &plan.format,
            options.clone(),
            &dependencies,
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
            &plan.format,
            options,
            &constrained,
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
            format: FileFormatDeclaration::csv(),
            format_declared: true,
            compression: FileCompressionDeclaration::none(),
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let resource_id = ResourceId::new("events.csv").unwrap();
        let resolved = dependencies
            .with_transport(|transport| {
                resolve_file_matches(&resource_id, &plan, transport, dependencies.transforms())
            })
            .unwrap();
        let probe = discover_local_binary_schema_bounded(
            &path,
            &dependencies,
            &plan.format,
            "none",
            0,
            1024 * 1024,
        )
        .unwrap();
        assert_eq!(probe.schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(probe.schema.field(1).data_type(), &DataType::Utf8);
        let stream = stream_file_match_blocking(
            &resolved[0],
            &plan.format,
            ReadOptions::new(resource_id, PartitionId::new("csv-file").unwrap()),
            &dependencies,
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
        drop(batches);
        assert_eq!(
            dependencies.execution().memory().snapshot().current_bytes,
            0
        );
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
            format: FileFormatDeclaration::json(),
            format_declared: true,
            compression: FileCompressionDeclaration::none(),
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let resource_id = ResourceId::new("events.json").unwrap();
        let resolved = dependencies
            .with_transport(|transport| {
                resolve_file_matches(&resource_id, &plan, transport, dependencies.transforms())
            })
            .unwrap();
        let probe = discover_local_binary_schema_bounded(
            &path,
            &dependencies,
            &plan.format,
            "none",
            0,
            1024 * 1024,
        )
        .unwrap();
        assert_eq!(probe.schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(probe.schema.field(1).data_type(), &DataType::Utf8);
        let stream = stream_file_match_blocking(
            &resolved[0],
            &plan.format,
            ReadOptions::new(resource_id, PartitionId::new("json-file").unwrap()),
            &dependencies,
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
        drop(batches);
        assert_eq!(
            dependencies.execution().memory().snapshot().current_bytes,
            0
        );
    }
}
