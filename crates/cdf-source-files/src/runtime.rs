use std::{
    collections::BTreeMap,
    fmt,
    fs::{self, File},
    io::{Read, Seek, SeekFrom},
    path::{Component, Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::UNIX_EPOCH,
};

use arrow_schema::SchemaRef;
use cdf_contract::{ContractPolicy, TypePolicy};
use cdf_formats::{
    CsvOptions, FileCompression, FileFormat, JsonOptions, RangeChunkReader, ReadOptions,
    stream_file_source_path_with_declared_schema_and_type_policy,
};
use cdf_kernel::{
    BatchStream, BoxFuture, CdfError, DeliveryGuarantee, EffectiveSchemaRuntime,
    PLAN_PHYSICAL_SCHEMA_HASH_KEY, PLAN_SCHEMA_OBSERVATION_BINDING_KEY,
    PLAN_SCHEMA_OBSERVATION_ID_KEY, PartitionId, PartitionPlan, PlanId, QueryableResource,
    ResourceCapabilities, ResourceDescriptor, ResourceId, ResourceStream, Result, ScanPlan,
    ScanRequest, SchemaHash, ScopeKey, SourcePosition, TypePolicyAllowances, WriteDisposition,
};
use cdf_runtime::{
    DecodePlanningRequest, ExecutionServices, FormatDiscoveryRequest, FormatDriver, FormatRegistry,
    PhysicalDecodeRequest,
};
use futures_util::{StreamExt, TryStreamExt};
use sha2::{Digest, Sha256};

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
    max_spool_bytes: u64,
}

const DEFAULT_MAX_REMOTE_SPOOL_BYTES: u64 = 64 * 1024 * 1024 * 1024;

impl FileRuntimeDependencies {
    pub fn new(
        transport: impl FileTransport + 'static,
        execution: ExecutionServices,
        formats: Arc<FormatRegistry>,
    ) -> Self {
        Self::from_boxed_transport(Box::new(transport), execution, formats)
    }

    pub fn from_boxed_transport(
        transport: Box<dyn FileTransport>,
        execution: ExecutionServices,
        formats: Arc<FormatRegistry>,
    ) -> Self {
        Self {
            transport: Arc::from(transport),
            execution,
            formats,
            max_spool_bytes: DEFAULT_MAX_REMOTE_SPOOL_BYTES,
        }
    }

    pub fn with_max_spool_bytes(mut self, max_spool_bytes: u64) -> Result<Self> {
        if max_spool_bytes == 0 {
            return Err(CdfError::contract(
                "remote file spool budget must be greater than zero",
            ));
        }
        self.max_spool_bytes = max_spool_bytes;
        Ok(self)
    }

    pub fn max_spool_bytes(&self) -> u64 {
        self.max_spool_bytes
    }

    fn transport(&self) -> Arc<dyn FileTransport> {
        Arc::clone(&self.transport)
    }

    fn execution(&self) -> &ExecutionServices {
        &self.execution
    }

    fn formats(&self) -> &Arc<FormatRegistry> {
        &self.formats
    }

    pub fn with_transport<R>(&self, f: impl FnOnce(&dyn FileTransport) -> Result<R>) -> Result<R> {
        f(self.transport.as_ref())
    }

    pub fn range_reader(
        &self,
        resource: FileTransportResource,
        size_bytes: u64,
    ) -> RangeChunkReader {
        transport_range_reader(self.transport(), resource, size_bytes)
    }

    pub fn bounded_range_reader(
        &self,
        resource: FileTransportResource,
        size_bytes: u64,
        max_bytes: u64,
    ) -> (RangeChunkReader, Arc<AtomicU64>) {
        let bytes_read = Arc::new(AtomicU64::new(0));
        let counter = Arc::clone(&bytes_read);
        let transport = self.transport();
        let reader = RangeChunkReader::new(size_bytes, move |start, length| {
            let length = u64::try_from(length).map_err(|error| {
                CdfError::internal(format!("range length conversion failed: {error}"))
            })?;
            counter
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    current.checked_add(length).filter(|total| *total <= max_bytes)
                })
                .map_err(|current| {
                    CdfError::data(format!(
                        "remote schema metadata probe exceeded its {max_bytes}-byte per-file budget after {current} bytes"
                    ))
                })?;
            let range = ByteRange::new(start, length)?;
            match transport.read_range(&resource, range) {
                Ok(bytes) => Ok(bytes),
                Err(error) => {
                    counter.fetch_sub(length, Ordering::Relaxed);
                    Err(error)
                }
            }
        });
        (reader, bytes_read)
    }

    pub fn bounded_sequential_reader(
        &self,
        resource: FileTransportResource,
        size_bytes: u64,
        max_bytes: u64,
    ) -> (Box<dyn Read + Send>, Arc<AtomicU64>) {
        let bytes_read = Arc::new(AtomicU64::new(0));
        let reader = TransportSequentialReader {
            transport: self.transport(),
            resource,
            size_bytes,
            max_bytes,
            offset: 0,
            bytes_read: Arc::clone(&bytes_read),
        };
        (Box::new(reader), bytes_read)
    }
}

struct TransportSequentialReader {
    transport: Arc<dyn FileTransport>,
    resource: FileTransportResource,
    size_bytes: u64,
    max_bytes: u64,
    offset: u64,
    bytes_read: Arc<AtomicU64>,
}

impl Read for TransportSequentialReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() || self.offset == self.size_bytes {
            return Ok(0);
        }
        let remaining_file = self.size_bytes - self.offset;
        let remaining_budget = self.max_bytes.saturating_sub(self.offset);
        if remaining_budget == 0 {
            return Err(std::io::Error::other(format!(
                "remote sequential read exceeded its {}-byte budget",
                self.max_bytes
            )));
        }
        let length = remaining_file
            .min(remaining_budget)
            .min(buffer.len() as u64);
        let bytes = self
            .transport
            .read_range(
                &self.resource,
                ByteRange::new(self.offset, length).map_err(std::io::Error::other)?,
            )
            .map_err(std::io::Error::other)?;
        buffer[..bytes.len()].copy_from_slice(&bytes);
        self.offset += bytes.len() as u64;
        self.bytes_read
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(bytes.len())
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
        .map(|path| local_file_discovery_candidate(resource_id, &root, path, plan))
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
    effective_schema_runtime: Option<EffectiveSchemaRuntime>,
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
            effective_schema_runtime,
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
            file_partitions_for_plan_with_transport(&self.descriptor, &self.plan, transport)
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
                validate_partition(&descriptor, &plan, &partition, transport)
            })?;
            let processed_position = SourcePosition::FileManifest(cdf_kernel::FileManifest {
                version: 1,
                files: vec![cdf_kernel::FilePosition {
                    path: resolved.path_text,
                    size_bytes: resolved.size_bytes,
                    etag: resolved.etag,
                    sha256: resolved.sha256,
                }],
            });
            let physical_schema_hash = match (&resolved.open, &plan.format) {
                (ResolvedFileOpen::LocalPath(path), FileFormatDeclaration::Parquet) => {
                    let budget = discovery_budget.as_ref().ok_or_else(|| {
                        CdfError::data(
                            "schema-observation attestation requires the plan-recorded discovery executor budget",
                        )
                    })?;
                    let probe = cdf_formats::discover_local_parquet_schema_bounded(
                        path,
                        0,
                        budget.max_metadata_bytes_per_file,
                    )?;
                    Some(cdf_formats::schema_hash(probe.schema.as_ref())?)
                }
                (ResolvedFileOpen::LocalPath(path), FileFormatDeclaration::ArrowIpc) => {
                    let budget = discovery_budget.as_ref().ok_or_else(|| {
                        CdfError::data(
                            "schema-observation attestation requires the plan-recorded discovery executor budget",
                        )
                    })?;
                    let probe = cdf_formats::discover_local_arrow_ipc_schema_bounded(
                        path,
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
        self.effective_schema_runtime.as_ref()
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct CompressionEvidence {
    mode: FileCompression,
    extension_signal: CompressionSignal,
    magic_signal: CompressionSignal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CompressionSignal {
    None,
    Gzip,
    Zstd,
}

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
    fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Gzip => "gzip",
            Self::Zstd => "zstd",
        }
    }

    fn compression(self) -> Option<FileCompression> {
        match self {
            Self::None => None,
            Self::Gzip => Some(FileCompression::Gzip),
            Self::Zstd => Some(FileCompression::Zstd),
        }
    }
}

pub fn file_partitions_for_plan(
    descriptor: &ResourceDescriptor,
    plan: &FileResourcePlan,
) -> Result<Vec<PartitionPlan>> {
    let transport = FileTransportFacade::new();
    file_partitions_for_plan_with_transport(descriptor, plan, &transport)
}

pub(crate) fn file_partitions_for_plan_with_transport(
    descriptor: &ResourceDescriptor,
    plan: &FileResourcePlan,
    transport: &dyn FileTransport,
) -> Result<Vec<PartitionPlan>> {
    let matches = resolve_file_matches(&descriptor.resource_id, plan, transport)?;
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
    declared_schema: SchemaRef,
    plan: &FileResourcePlan,
    partition: PartitionPlan,
    dependencies: FileRuntimeDependencies,
    allowances: cdf_kernel::TypePolicyAllowances,
) -> BoxFuture<'static, Result<BatchStream>> {
    let descriptor = descriptor.clone();
    let declared_schema = declared_schema.clone();
    let plan = plan.clone();
    if !is_http_root(&plan.root) && !is_object_store_root(&plan.root) {
        return Box::pin(async move {
            open_file_partition(
                &descriptor,
                declared_schema,
                &plan,
                &partition,
                &dependencies,
                allowances,
            )
        });
    }
    let execution = dependencies.execution().clone();
    let mut scope_hasher = Sha256::new();
    scope_hasher.update(descriptor.resource_id.as_str().as_bytes());
    scope_hasher.update([0]);
    scope_hasher.update(partition.partition_id.as_str().as_bytes());
    let scope_id = format!("file-open-{}", &hex::encode(scope_hasher.finalize())[..16]);
    Box::pin(async move {
        let stream = execution.spawn_io_stream(
            &scope_id,
            NATIVE_STREAM_ITEMS,
            move |mut sender, _cancellation| async move {
                let mut batches = open_file_partition(
                    &descriptor,
                    declared_schema,
                    &plan,
                    &partition,
                    &dependencies,
                    allowances,
                )?;
                while let Some(batch) = batches.try_next().await? {
                    sender.send(batch).await?;
                }
                Ok(())
            },
        )?;
        Ok(Box::pin(stream) as BatchStream)
    })
}

fn open_file_partition(
    descriptor: &ResourceDescriptor,
    declared_schema: SchemaRef,
    plan: &FileResourcePlan,
    partition: &PartitionPlan,
    dependencies: &FileRuntimeDependencies,
    allowances: cdf_kernel::TypePolicyAllowances,
) -> Result<BatchStream> {
    let resolved = dependencies
        .with_transport(|transport| validate_partition(descriptor, plan, partition, transport))?;
    let planned_physical_schema_hash = partition
        .metadata
        .get(PLAN_PHYSICAL_SCHEMA_HASH_KEY)
        .map(|value| SchemaHash::new(value.clone()))
        .transpose()?;
    let options = ReadOptions::new(
        descriptor.resource_id.clone(),
        partition.partition_id.clone(),
    );
    let mut type_policy = ContractPolicy::default().types;
    type_policy.coerce_types = allowances.coerce_types;
    type_policy.allow_lossy_mapping = allowances.allow_lossy_mapping;
    stream_file_match(
        &resolved,
        &plan.format,
        options,
        declared_schema,
        dependencies,
        &type_policy,
        planned_physical_schema_hash,
    )
}

fn stream_file_match(
    resolved: &ResolvedFileMatch,
    declaration: &FileFormatDeclaration,
    options: ReadOptions,
    declared_schema: SchemaRef,
    dependencies: &FileRuntimeDependencies,
    type_policy: &TypePolicy,
    planned_physical_schema_hash: Option<SchemaHash>,
) -> Result<BatchStream> {
    let position = Some(SourcePosition::FileManifest(cdf_kernel::FileManifest {
        version: 1,
        files: vec![cdf_kernel::FilePosition {
            path: resolved.path_text.clone(),
            size_bytes: resolved.size_bytes,
            etag: resolved.etag.clone(),
            sha256: resolved.sha256.clone(),
        }],
    }));
    let native_driver = (resolved.compression.mode == FileCompression::None)
        .then(|| dependencies.formats().get(file_format_name(declaration)))
        .flatten();
    match &resolved.open {
        ResolvedFileOpen::LocalPath(path) if native_driver.is_some() => stream_registered_format(
            path.clone(),
            None,
            native_driver.expect("registered driver guard was checked"),
            options,
            position,
            planned_physical_schema_hash,
            dependencies,
        ),
        ResolvedFileOpen::LocalPath(path) => {
            stream_file_source_path_with_declared_schema_and_type_policy(
                path,
                compile_format(declaration)?,
                resolved.compression.mode,
                options,
                declared_schema,
                type_policy,
                position,
            )
        }
        ResolvedFileOpen::Transport(resource) => {
            let expected = FileIdentityMetadata {
                location: resolved.path_text.clone(),
                size_bytes: Some(resolved.size_bytes),
                checksum: resolved.sha256.as_ref().map(|sha256| crate::FileChecksum {
                    algorithm: "sha256".to_owned(),
                    value: sha256.clone(),
                }),
                etag: resolved.etag.clone(),
                modified: resolved.modified_ms.clone(),
            };
            let spool = Arc::new(spool_transport_file(
                &dependencies.transport(),
                resource,
                &expected,
                dependencies.max_spool_bytes(),
            )?);
            if let Some(driver) = native_driver {
                stream_registered_format(
                    spool.path().to_path_buf(),
                    Some(spool),
                    driver,
                    options,
                    position,
                    planned_physical_schema_hash,
                    dependencies,
                )
            } else {
                let stream = stream_file_source_path_with_declared_schema_and_type_policy(
                    spool.path(),
                    compile_format(declaration)?,
                    resolved.compression.mode,
                    options,
                    declared_schema,
                    type_policy,
                    position,
                )?;
                Ok(Box::pin(stream.map(move |batch| {
                    let _keep_spool_alive = &spool;
                    batch
                })) as BatchStream)
            }
        }
    }
}

fn stream_registered_format(
    path: PathBuf,
    spool: Option<Arc<tempfile::NamedTempFile>>,
    driver: Arc<dyn FormatDriver>,
    options: ReadOptions,
    source_position: Option<SourcePosition>,
    planned_physical_schema_hash: Option<SchemaHash>,
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
            let _spool = spool;
            let source = Arc::new(LocalByteSource::open(path, Arc::clone(&memory))?);
            let options_json = driver.canonical_options(serde_json::json!({}))?;
            let observed_schema_hash = match planned_physical_schema_hash {
                Some(hash) => hash,
                None => {
                    let observation = driver
                        .discover(
                            source.clone(),
                            FormatDiscoveryRequest {
                                options: options_json.clone(),
                                maximum_bytes: 16 * 1024 * 1024,
                                maximum_records: 0,
                                cancellation: cancellation.clone(),
                            },
                        )
                        .await?;
                    cdf_contract::canonical_arrow_schema_hash(observation.arrow_schema.as_ref())?
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
                            observed_schema_hash: observed_schema_hash.clone(),
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

fn spool_transport_file(
    transport: &Arc<dyn FileTransport>,
    resource: &FileTransportResource,
    expected: &FileIdentityMetadata,
    max_spool_bytes: u64,
) -> Result<tempfile::NamedTempFile> {
    let size_bytes = expected
        .size_bytes
        .ok_or_else(|| CdfError::data("remote file spool requires a planned content length"))?;
    if size_bytes > max_spool_bytes {
        return Err(CdfError::data(format!(
            "remote file requires {size_bytes} spool bytes, exceeding the configured {max_spool_bytes}-byte disk budget; increase the spool budget or use a streaming format runtime"
        )));
    }
    let spool = tempfile::NamedTempFile::new()
        .map_err(|error| CdfError::data(format!("create remote file spool: {error}")))?;
    transport.download_to_path(resource, expected, spool.path())?;
    Ok(spool)
}

fn compile_format(format: &FileFormatDeclaration) -> Result<FileFormat> {
    match format {
        FileFormatDeclaration::Csv => Ok(FileFormat::Csv(CsvOptions::default())),
        FileFormatDeclaration::Json => Ok(FileFormat::Json(JsonOptions::default())),
        FileFormatDeclaration::Ndjson => Ok(FileFormat::Ndjson(JsonOptions::default())),
        FileFormatDeclaration::Parquet => Ok(FileFormat::Parquet),
        FileFormatDeclaration::ArrowIpc => Err(CdfError::contract(
            "Arrow IPC file execution requires its registered native format driver",
        )),
    }
}

fn validate_partition(
    descriptor: &ResourceDescriptor,
    plan: &FileResourcePlan,
    partition: &PartitionPlan,
    transport: &dyn FileTransport,
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
    let matches = resolve_file_matches(&descriptor.resource_id, plan, transport)?;
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
    match (&resolved.sha256, &resolved.etag) {
        (Some(sha256), _) => {
            if partition.metadata.get("sha256").map(String::as_str) != Some(sha256.as_str()) {
                return Err(CdfError::data(format!(
                    "declarative file partition `{path}` changed checksum after planning"
                )));
            }
        }
        (None, Some(etag)) => {
            if partition.metadata.get("etag").map(String::as_str) != Some(etag.as_str()) {
                return Err(CdfError::data(format!(
                    "declarative file partition `{path}` changed ETag after planning"
                )));
            }
        }
        (None, None) => {
            return Err(CdfError::contract(format!(
                "declarative file partition `{path}` requires checksum or ETag metadata"
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
            resolved.compression.mode.as_str(),
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
            resolved.compression.mode.as_str(),
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
            file.compression.mode.as_str().to_owned(),
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
) -> Result<Vec<ResolvedFileMatch>> {
    if is_http_root(&plan.root) {
        return resolve_http_file_match(resource_id, plan, transport);
    }
    if is_object_store_root(&plan.root) {
        return resolve_object_store_matches(resource_id, plan, transport);
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
        .map(|path| resolved_file_match(resource_id, &root, path, plan))
        .collect()
}

fn resolve_object_store_matches(
    resource_id: &ResourceId,
    plan: &FileResourcePlan,
    transport: &dyn FileTransport,
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
        let compression = resolve_transport_compression(plan, transport, &resource, &metadata)?;
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
        let compression = resolve_transport_compression(plan, transport, &resource, &metadata)?;
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
    let compression = resolve_local_compression(&path_text, &path, &plan.compression)?;
    let (format, _) = resolve_local_format(resource_id, plan, &path_text, &path, &compression)?;
    let sha256 = file_sha256(&path)?;
    Ok(ResolvedFileMatch {
        open: ResolvedFileOpen::LocalPath(path),
        path_text,
        size_bytes: metadata.len(),
        sha256: Some(sha256),
        etag: None,
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
    let compression = resolve_local_compression(&relative_path, &path, &plan.compression)?;
    let (_, format_bytes_read) =
        resolve_local_format(resource_id, plan, &relative_path, &path, &compression)?;
    Ok(LocalFileDiscoveryCandidate {
        path,
        relative_path,
        size_bytes: metadata.len(),
        compression: compression.mode.as_str().to_owned(),
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
    if sha256.is_none() && metadata.etag.is_none() {
        return Err(CdfError::data(format!(
            "HTTP(S) file metadata for `{}` must include an ETag or checksum for FileManifest identity",
            metadata.location
        )));
    }
    Ok(ResolvedFileMatch {
        open: ResolvedFileOpen::Transport(resource),
        path_text: metadata.location,
        size_bytes,
        sha256,
        etag: metadata.etag,
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
    let extension_signal = format_extension_signal(path_text);
    if !requires_binary_format_confirmation(plan, extension_signal) {
        return Ok((
            FormatEvidence {
                extension_signal,
                magic_signal: FormatSignal::Unknown,
            },
            0,
        ));
    }
    reject_compressed_binary_format(resource_id, plan, path_text, compression)?;
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
    let extension_signal = format_extension_signal(&metadata.location);
    if !requires_binary_format_confirmation(plan, extension_signal) {
        return Ok(FormatEvidence {
            extension_signal,
            magic_signal: FormatSignal::Unknown,
        });
    }
    reject_compressed_binary_format(
        resource_id,
        plan,
        &diagnostic_location(&metadata.location),
        compression,
    )?;
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
    matches!(
        plan.format,
        FileFormatDeclaration::Parquet | FileFormatDeclaration::ArrowIpc
    ) || extension_signal != FormatSignal::Unknown
}

fn reject_compressed_binary_format(
    resource_id: &ResourceId,
    plan: &FileResourcePlan,
    path_text: &str,
    compression: &CompressionEvidence,
) -> Result<()> {
    if compression.mode == FileCompression::None {
        return Ok(());
    }
    let exclusion = match plan.format {
        FileFormatDeclaration::Parquet => "compressed Parquet discovery is excluded",
        FileFormatDeclaration::ArrowIpc => "compressed Arrow IPC discovery is excluded",
        FileFormatDeclaration::Csv
        | FileFormatDeclaration::Json
        | FileFormatDeclaration::Ndjson => "compressed binary discovery is excluded",
    };
    Err(CdfError::contract(format!(
        "file resource `{resource_id}` cannot confirm binary format `{}` for compressed file `{path_text}`: {exclusion}; compressed Parquet and Arrow IPC are not supported; use an uncompressed `.parquet` or `.arrow` file",
        file_format_name(&plan.format)
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
    let mut format_requirement = match plan.format {
        FileFormatDeclaration::Parquet => "expected Parquet file framing with PAR1 header/footer; ",
        FileFormatDeclaration::ArrowIpc => {
            "expected Arrow IPC file framing with ARROW1 header/footer; "
        }
        FileFormatDeclaration::Csv
        | FileFormatDeclaration::Json
        | FileFormatDeclaration::Ndjson => "",
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
    match format {
        FileFormatDeclaration::Parquet => Some(FormatSignal::Parquet),
        FileFormatDeclaration::ArrowIpc => Some(FormatSignal::ArrowIpc),
        FileFormatDeclaration::Csv
        | FileFormatDeclaration::Json
        | FileFormatDeclaration::Ndjson => None,
    }
}

fn file_format_name(format: &FileFormatDeclaration) -> &'static str {
    match format {
        FileFormatDeclaration::Csv => "csv",
        FileFormatDeclaration::Json => "json",
        FileFormatDeclaration::Ndjson => "ndjson",
        FileFormatDeclaration::Parquet => "parquet",
        FileFormatDeclaration::ArrowIpc => "arrow_ipc",
    }
}

fn records_format_metadata(plan: &FileResourcePlan) -> bool {
    matches!(
        plan.format,
        FileFormatDeclaration::Parquet | FileFormatDeclaration::ArrowIpc
    )
}

fn format_extension_signal(path_text: &str) -> FormatSignal {
    let path_without_fragment = path_text.split('#').next().unwrap_or(path_text);
    let path_without_query = path_without_fragment
        .split('?')
        .next()
        .unwrap_or(path_without_fragment)
        .to_ascii_lowercase();
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
) -> Result<CompressionEvidence> {
    let extension_signal = compression_extension_signal(path_text);
    let magic_signal = compression_magic_signal(path)?;
    resolve_compression_signals(path_text, declared, extension_signal, magic_signal)
}

fn resolve_transport_compression(
    plan: &FileResourcePlan,
    transport: &dyn FileTransport,
    resource: &FileTransportResource,
    metadata: &FileIdentityMetadata,
) -> Result<CompressionEvidence> {
    let extension_signal = compression_extension_signal(&metadata.location);
    let size_bytes = metadata.size_bytes.ok_or_else(|| {
        CdfError::data(format!(
            "remote file metadata for `{}` omitted byte size for compression confirmation",
            diagnostic_location(&metadata.location)
        ))
    })?;
    let length = size_bytes.min(4);
    let magic_signal = if length == 0 {
        CompressionSignal::None
    } else {
        let magic = transport.read_range(resource, ByteRange::new(0, length)?)?;
        compression_magic_signal_from_bytes(&magic)
    };
    resolve_compression_signals(
        &diagnostic_location(&metadata.location),
        &plan.compression,
        extension_signal,
        magic_signal,
    )
}

fn resolve_compression_signals(
    path_text: &str,
    declared: &FileCompressionDeclaration,
    extension_signal: CompressionSignal,
    magic_signal: CompressionSignal,
) -> Result<CompressionEvidence> {
    let mode = match declared {
        FileCompressionDeclaration::Auto => {
            match (extension_signal.compression(), magic_signal.compression()) {
                (Some(extension), Some(magic)) if extension != magic => {
                    return Err(compression_signal_error(
                        path_text,
                        declared,
                        extension_signal,
                        magic_signal,
                    ));
                }
                (_, Some(magic)) => magic,
                (Some(_), None) => {
                    return Err(compression_signal_error(
                        path_text,
                        declared,
                        extension_signal,
                        magic_signal,
                    ));
                }
                (None, None) => FileCompression::None,
            }
        }
        FileCompressionDeclaration::None => {
            if magic_signal.compression().is_some() {
                return Err(compression_signal_error(
                    path_text,
                    declared,
                    extension_signal,
                    magic_signal,
                ));
            }
            FileCompression::None
        }
        FileCompressionDeclaration::Gzip => {
            if magic_signal != CompressionSignal::Gzip {
                return Err(compression_signal_error(
                    path_text,
                    declared,
                    extension_signal,
                    magic_signal,
                ));
            }
            FileCompression::Gzip
        }
        FileCompressionDeclaration::Zstd => {
            if magic_signal != CompressionSignal::Zstd {
                return Err(compression_signal_error(
                    path_text,
                    declared,
                    extension_signal,
                    magic_signal,
                ));
            }
            FileCompression::Zstd
        }
    };

    Ok(CompressionEvidence {
        mode,
        extension_signal,
        magic_signal,
    })
}

fn compression_extension_signal(path_text: &str) -> CompressionSignal {
    let path_without_fragment = path_text.split('#').next().unwrap_or(path_text);
    let lower = path_without_fragment
        .split('?')
        .next()
        .unwrap_or(path_without_fragment)
        .to_ascii_lowercase();
    if lower.ends_with(".gz") || lower.ends_with(".gzip") {
        CompressionSignal::Gzip
    } else if lower.ends_with(".zst") || lower.ends_with(".zstd") {
        CompressionSignal::Zstd
    } else {
        CompressionSignal::None
    }
}

fn compression_magic_signal(path: &Path) -> Result<CompressionSignal> {
    let mut file = File::open(path).map_err(|error| {
        CdfError::data(format!(
            "open matched file {} for compression detection: {error}",
            path.display()
        ))
    })?;
    let mut magic = [0_u8; 4];
    let bytes_read = file.read(&mut magic).map_err(|error| {
        CdfError::data(format!(
            "read matched file {} for compression detection: {error}",
            path.display()
        ))
    })?;
    Ok(compression_magic_signal_from_bytes(&magic[..bytes_read]))
}

fn compression_magic_signal_from_bytes(magic: &[u8]) -> CompressionSignal {
    if magic.len() >= 2 && magic[..2] == [0x1f, 0x8b] {
        return CompressionSignal::Gzip;
    }
    if magic.len() >= 4 && magic[..4] == [0x28, 0xb5, 0x2f, 0xfd] {
        return CompressionSignal::Zstd;
    }
    CompressionSignal::None
}

fn compression_signal_error(
    path_text: &str,
    declared: &FileCompressionDeclaration,
    extension_signal: CompressionSignal,
    magic_signal: CompressionSignal,
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
    file.compression.mode != FileCompression::None
        || !matches!(declared, FileCompressionDeclaration::Auto)
        || file.compression.extension_signal != CompressionSignal::None
        || file.compression.magic_signal != CompressionSignal::None
}

impl FileCompressionDeclaration {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::None => "none",
            Self::Gzip => "gzip",
            Self::Zstd => "zstd",
        }
    }
}

fn transport_range_reader(
    transport: Arc<dyn FileTransport>,
    resource: FileTransportResource,
    size_bytes: u64,
) -> RangeChunkReader {
    RangeChunkReader::new(size_bytes, move |start, length| {
        let length = u64::try_from(length).map_err(|error| {
            CdfError::internal(format!("range length conversion failed: {error}"))
        })?;
        let range = ByteRange::new(start, length)?;
        transport.read_range(&resource, range)
    })
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

    #[test]
    fn shared_transport_dependency_does_not_serialize_independent_io() {
        let dependencies = Arc::new(FileRuntimeDependencies::new(
            FileTransportFacade::new(),
            crate::test_execution_services(),
            crate::test_format_registry(),
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
        );
        let driver = dependencies.formats().resolve("parquet").unwrap();
        let stream = stream_registered_format(
            temp.path().to_path_buf(),
            None,
            driver,
            ReadOptions::new(
                ResourceId::new("events").unwrap(),
                PartitionId::new("file-0").unwrap(),
            ),
            None,
            None,
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
    fn remote_arrow_ipc_file_spools_and_streams_through_registered_driver() {
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
        )
        .with_max_spool_bytes(bytes.len() as u64)
        .unwrap();
        let plan = FileResourcePlan {
            source: "ipc".to_owned(),
            root: "s3://ipc/prod".to_owned(),
            glob: "events.arrow".to_owned(),
            format: FileFormatDeclaration::ArrowIpc,
            format_declared: true,
            compression: FileCompressionDeclaration::None,
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let resource_id = ResourceId::new("ipc.events").unwrap();
        let resolved = dependencies
            .with_transport(|transport| {
                resolve_object_store_matches(&resource_id, &plan, transport)
            })
            .unwrap();
        let stream = stream_file_match(
            &resolved[0],
            &plan.format,
            ReadOptions::new(resource_id, PartitionId::new("file-ipc").unwrap()),
            schema,
            &dependencies,
            &ContractPolicy::default().types,
            None,
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
            format: FileFormatDeclaration::Parquet,
            format_declared: true,
            compression: FileCompressionDeclaration::None,
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let resource_id = ResourceId::new("events.raw").unwrap();

        let matches = resolve_object_store_matches(&resource_id, &plan, &transport).unwrap();
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
    fn object_store_gzip_ndjson_spools_under_budget_and_preserves_remote_position() {
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
        )
        .with_max_spool_bytes(encoded.len() as u64)
        .unwrap();
        let transport = dependencies.transport();
        let plan = FileResourcePlan {
            source: "events".to_owned(),
            root: "s3://acme-events/prod".to_owned(),
            glob: "2026/**/*.ndjson.gz".to_owned(),
            format: FileFormatDeclaration::Ndjson,
            format_declared: true,
            compression: FileCompressionDeclaration::Auto,
            auth: None,
            credentials: None,
            allowlist: cdf_http::EgressAllowlist::allow_any(),
        };
        let resource_id = ResourceId::new("events.raw").unwrap();
        let resolved =
            resolve_object_store_matches(&resource_id, &plan, transport.as_ref()).unwrap();
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].compression.mode, FileCompression::Gzip);
        let declared = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let options = ReadOptions::new(resource_id, PartitionId::new("file-events").unwrap());
        let type_policy = ContractPolicy::default().types;
        let stream = stream_file_match(
            &resolved[0],
            &plan.format,
            options.clone(),
            declared.clone(),
            &dependencies,
            &type_policy,
            None,
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
        let error = match stream_file_match(
            &resolved[0],
            &plan.format,
            options,
            declared,
            &constrained,
            &type_policy,
            None,
        ) {
            Ok(_) => panic!("undersized spool budget should reject the stream"),
            Err(error) => error,
        };
        assert!(error.message.contains("disk budget"));
        assert!(error.message.contains(&encoded.len().to_string()));
    }
}
