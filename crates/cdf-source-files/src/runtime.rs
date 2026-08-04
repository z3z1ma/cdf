use std::{collections::BTreeMap, fmt, sync::Arc};

use arrow_schema::{Schema, SchemaRef};
use cdf_kernel::{
    BackpressureSupport, CapabilitySupport, CdfError, CompiledScanIntent, DeliveryGuarantee,
    EffectiveSchemaRuntime, EstimateSupport, ExecutablePartition, FilterCapabilities,
    IncrementalShape, PartitionAuthority, PartitionPlan, PartitioningCapabilities,
    PayloadRetention, PlanId, PlannedPartitionReader, PlannedTaskSetReference, PushdownFidelity,
    PushedPredicate, QueryableResource, ReplaySupport, ResourceCapabilities, ResourceDescriptor,
    ResourceStream, Result, ScanPlan, ScanRequest, ScopeKind, SourcePosition, TypePolicyAllowances,
    WriteDisposition, partition_schema_observation_id, source_name,
};
#[cfg(test)]
use cdf_object_access::{
    AccountedFileIdentity, FILE_IDENTITY_MEMORY_ENVELOPE_BYTES, FileIdentityStream,
    FileMetadataObservation, FileTransportFacade, HttpFileRequest, HttpFileResponse,
    HttpFileTransport, ResolvedHttpAuth,
};
use cdf_object_access::{FilePayloadCache, FileTransport, FileTransportControl};
use cdf_runtime::{
    BlockingLaneSpec, ByteTransformRegistry, CompiledFormatBinding, ExecutionServices,
    FormatRegistry, GenerationStrength, InterruptionSafety, LaneAffinity, PreparedSourcePayloadKey,
    PreparedSourcePayloads, SourceEgressScope,
};
use cdf_task_store::{CanonicalTaskSetLimits, ExternalTaskStore, TaskSetLimits};
#[cfg(test)]
use futures_util::StreamExt;
use sha2::{Digest, Sha256};

use crate::{FileFormatDeclaration, FileResourcePlan};

const NATIVE_TARGET_BATCH_ROWS: usize = 64 * 1024;
const NATIVE_TARGET_BATCH_BYTES: u64 = 16 * 1024 * 1024;
const NATIVE_STREAM_ITEMS: usize = 2;
const NATIVE_UNIT_STREAM_ITEMS: usize = 1;
const NATIVE_UNIT_BUFFERED_BATCHES: u16 = 2;
pub const FILE_SOURCE_BLOCKING_LANE_ID: &str = "file-source.control";
pub const FILE_SOURCE_ADVERTISED_PARALLELISM: u16 = 16;
const FILE_PARTITION_TASK_TYPE: &str = "file-partition-v1";
const FILE_INVENTORY_TASK_TYPE: &str = "file-inventory-v1";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileTaskStoreOptions {
    pub maximum_task_bytes: u64,
    pub maximum_authority_bytes: u64,
    pub maximum_sort_key_bytes: u64,
    pub index_cache_bytes: u64,
    pub writer_buffer_bytes: usize,
    pub spill_growth_bytes: u64,
    pub metadata_parse_amplification_bps: u32,
}

impl FileTaskStoreOptions {
    fn canonical_limits(&self) -> CanonicalTaskSetLimits {
        CanonicalTaskSetLimits {
            tasks: TaskSetLimits {
                maximum_task_bytes: self.maximum_task_bytes,
                maximum_authority_bytes: self.maximum_authority_bytes,
                writer_buffer_bytes: self.writer_buffer_bytes,
            },
            maximum_sort_key_bytes: self.maximum_sort_key_bytes,
            index_cache_bytes: self.index_cache_bytes,
            spill_growth_bytes: self.spill_growth_bytes,
            minimum_initial_spill_bytes: 8192,
        }
    }
}

pub fn file_source_blocking_lane() -> BlockingLaneSpec {
    BlockingLaneSpec {
        lane_id: FILE_SOURCE_BLOCKING_LANE_ID.to_owned(),
        binding: cdf_runtime::BlockingLaneBinding::Static,
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
    payload_cache: Option<FilePayloadCache>,
    egress: SourceEgressScope,
    max_spool_bytes: u64,
    task_store: Option<ExternalTaskStore>,
    task_store_options: Option<FileTaskStoreOptions>,
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
            task_store: None,
            task_store_options: None,
        }
    }

    pub fn with_prepared_payloads(mut self, prepared_payloads: PreparedSourcePayloads) -> Self {
        self.prepared_payloads = prepared_payloads;
        self
    }

    pub fn with_payload_cache(mut self, payload_cache: FilePayloadCache) -> Self {
        self.payload_cache = Some(payload_cache);
        self
    }

    pub fn with_task_store(
        mut self,
        task_store: ExternalTaskStore,
        options: FileTaskStoreOptions,
    ) -> Result<Self> {
        options.canonical_limits().validate()?;
        if options.metadata_parse_amplification_bps < 10_000 {
            return Err(CdfError::contract(
                "file task metadata parse amplification must be at least 10000 basis points",
            ));
        }
        self.task_store = Some(task_store);
        self.task_store_options = Some(options);
        Ok(self)
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

    pub fn payload_cache(&self) -> Option<&FilePayloadCache> {
        self.payload_cache.as_ref()
    }

    fn task_store(&self) -> Result<(&ExternalTaskStore, &FileTaskStoreOptions)> {
        self.task_store
            .as_ref()
            .zip(self.task_store_options.as_ref())
            .ok_or_else(|| {
                CdfError::contract(
                    "file planning requires an injected external task-store authority",
                )
            })
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

mod decode;
mod discovery;
mod input;
mod model;
mod planning;
mod resolution;
pub(crate) mod task;
#[cfg(test)]
mod tests;
mod validation;

pub use discovery::{
    BinarySchemaProbe, SchemaDiscoveryRequest, discover_local_binary_schema,
    discover_transport_binary_schema,
};
use planning::{
    build_file_inventory_with_transport, open_file_resource_with_dependencies,
    planned_physical_schema_authority,
};
use resolution::{FilePlanningContext, FileResolutionContext};
use task::{
    FileInventoryTaskAuthority, FileInventoryTaskBuilder, FileInventoryTaskReader,
    FilePartitionTaskAuthority, FilePlannedPartitionReader, PlannedFileInventory, RetainedFileTask,
};
use validation::{partition_for_file_record, physical_expression_node, validate_partition};

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
    baseline_observation_schema_catalog: Vec<cdf_kernel::EffectiveSchemaCatalogEntry>,
    compiled_format: CompiledFormatBinding,
    dependencies: FileRuntimeDependencies,
    prepared_inventory_key: Option<PreparedSourcePayloadKey>,
    source_discovery_binding_hash: cdf_kernel::SourceDiscoveryBinding,
    compiled_source_plan_hash: cdf_kernel::CompiledSourcePlanHash,
    transport_control: FileTransportControl,
}

#[derive(Clone, Debug)]
pub struct FileResourceDefinition {
    pub descriptor: ResourceDescriptor,
    pub schema: SchemaRef,
    pub plan: FileResourcePlan,
    pub type_policy_allowances: TypePolicyAllowances,
    pub effective_schema_runtime: Option<EffectiveSchemaRuntime>,
    pub baseline_observation_schema_catalog: Vec<cdf_kernel::EffectiveSchemaCatalogEntry>,
    pub compiled_format: CompiledFormatBinding,
}

impl FileResource {
    pub(crate) fn new(
        definition: FileResourceDefinition,
        dependencies: FileRuntimeDependencies,
        identities: &cdf_runtime::CompiledSourceIdentities,
    ) -> Result<Self> {
        Self::new_with_source_identities(
            definition,
            dependencies,
            identities.discovery_binding().clone(),
            identities.compiled_plan().clone(),
        )
    }

    fn new_with_source_identities(
        definition: FileResourceDefinition,
        dependencies: FileRuntimeDependencies,
        source_discovery_binding_hash: cdf_kernel::SourceDiscoveryBinding,
        compiled_source_plan_hash: cdf_kernel::CompiledSourcePlanHash,
    ) -> Result<Self> {
        let FileResourceDefinition {
            descriptor,
            schema,
            mut plan,
            type_policy_allowances,
            effective_schema_runtime,
            baseline_observation_schema_catalog,
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
            baseline_observation_schema_catalog,
            compiled_format,
            dependencies,
            prepared_inventory_key: None,
            source_discovery_binding_hash,
            compiled_source_plan_hash,
            transport_control: FileTransportControl::default(),
        })
    }

    #[cfg(test)]
    fn new_for_test(
        definition: FileResourceDefinition,
        dependencies: FileRuntimeDependencies,
        source_discovery_binding_hash: cdf_kernel::SourceDiscoveryBinding,
        compiled_source_plan_hash: cdf_kernel::CompiledSourcePlanHash,
    ) -> Result<Self> {
        Self::new_with_source_identities(
            definition,
            dependencies,
            source_discovery_binding_hash,
            compiled_source_plan_hash,
        )
    }

    pub(crate) fn with_prepared_inventory_key(mut self, key: PreparedSourcePayloadKey) -> Self {
        self.prepared_inventory_key = Some(key);
        self
    }

    pub(crate) fn with_transport_control(mut self, control: FileTransportControl) -> Self {
        self.transport_control = control;
        self
    }

    fn inventory_builder(&self) -> Result<FileInventoryTaskBuilder> {
        let (store, options) = self.dependencies.task_store()?;
        Ok(FileInventoryTaskBuilder {
            inner: store.canonical_builder(
                FILE_INVENTORY_TASK_TYPE,
                options.canonical_limits(),
                self.dependencies.execution().memory(),
                self.dependencies.execution().spill(),
            )?,
            authority: FileInventoryTaskAuthority {
                version: 1,
                resource_id: self.descriptor.resource_id.clone(),
                source_discovery_binding_hash: self.source_discovery_binding_hash.clone(),
            },
            planned_source_bytes: 0,
        })
    }

    fn inventory_reader(
        &self,
        reference: PlannedTaskSetReference,
    ) -> Result<FileInventoryTaskReader> {
        FileInventoryTaskReader::open(self, reference)
    }

    fn inventory_reference_with_limit(
        &self,
        maximum_matches: usize,
        control: &FileTransportControl,
    ) -> Result<PlannedFileInventory> {
        if let Some(key) = &self.prepared_inventory_key
            && let Some(payload) = self.dependencies.prepared_payloads().take(key)?
        {
            let (inventory, _retention) =
                payload.into_typed::<PlannedFileInventory>("file inventory task reference")?;
            if inventory.task_set.task_count > u64::try_from(maximum_matches).unwrap_or(u64::MAX) {
                return Err(CdfError::data(format!(
                    "file inventory exceeds the {maximum_matches}-entry boundary"
                )));
            }
            let _verified = self.inventory_reader(inventory.task_set.clone())?;
            return Ok(inventory);
        }
        let execution = self.dependencies.execution().clone();
        execution.ensure_blocking_lanes(&[file_source_blocking_lane()])?;
        let (transport, egress) = self.dependencies.transport_and_egress();
        let resource_id = self.descriptor.resource_id.clone();
        let plan = self.plan.clone();
        let formats = Arc::clone(self.dependencies.formats());
        let transforms = Arc::clone(self.dependencies.transforms());
        let control = control.clone();
        let builder = self.inventory_builder()?;
        execution
            .clone()
            .run_blocking(FILE_SOURCE_BLOCKING_LANE_ID, move || {
                build_file_inventory_with_transport(
                    &resource_id,
                    &plan,
                    FilePlanningContext {
                        transport: transport.as_ref(),
                        egress: &egress,
                        formats: formats.as_ref(),
                        transforms: transforms.as_ref(),
                        maximum_matches,
                        control: &control,
                        execution,
                    },
                    builder,
                )
            })
    }

    pub(crate) fn discovery_partitions_with_inventory(
        &self,
        maximum_matches: usize,
        control: &FileTransportControl,
    ) -> Result<(PlannedFileInventory, Vec<PartitionPlan>)> {
        let inventory = self.inventory_reference_with_limit(maximum_matches, control)?;
        let mut reader = self.inventory_reader(inventory.task_set.clone())?;
        let mut partitions = Vec::with_capacity(
            usize::try_from(inventory.task_set.task_count)
                .unwrap_or(maximum_matches)
                .min(maximum_matches),
        );
        while let Some(decoded) = reader.next_record()? {
            partitions.push(partition_for_file_record(
                &self.descriptor,
                &self.plan,
                &CompiledScanIntent::full_scan(),
                &decoded.record,
                inventory.task_set.task_count,
            )?);
        }
        Ok((inventory, partitions))
    }

    fn partition_tasks_from_inventory(
        &self,
        request: &ScanRequest,
        scan_intent: &CompiledScanIntent,
        inventory: &PlannedTaskSetReference,
    ) -> Result<PlannedTaskSetReference> {
        let (store, options) = self.dependencies.task_store()?;
        let compiled_source_plan_hash = self.compiled_source_plan_hash.clone();
        let authority = FilePartitionTaskAuthority {
            version: 1,
            resource_id: self.descriptor.resource_id.clone(),
            compiled_source_plan_hash,
            request_hash: cdf_runtime::artifact_hash(request)?,
        };
        let mut input = self.inventory_reader(inventory.clone())?;
        let mut output = store.writer(
            FILE_PARTITION_TASK_TYPE,
            options.canonical_limits().tasks,
            self.dependencies.execution().memory(),
            self.dependencies.execution().spill().as_ref(),
        )?;
        let mut ordinal = 0_u64;
        while let Some(decoded) = input.next_record()? {
            let partition = partition_for_file_record(
                &self.descriptor,
                &self.plan,
                scan_intent,
                &decoded.record,
                inventory.task_count,
            )?;
            output.push_with(ordinal, |writer| {
                serde_json::to_writer(writer, &partition)
                    .map_err(|error| CdfError::data(format!("encode file partition task: {error}")))
            })?;
            ordinal = ordinal
                .checked_add(1)
                .ok_or_else(|| CdfError::data("file partition task count exceeds u64"))?;
        }
        if ordinal != inventory.task_count {
            return Err(CdfError::data(
                "file inventory task count changed while producing partition authority",
            ));
        }
        output
            .finalize(|writer| {
                serde_json::to_writer(writer, &authority).map_err(|error| {
                    CdfError::data(format!("encode file partition task authority: {error}"))
                })
            })
            .map(|artifact| artifact.reference)
    }

    fn reconcile_exact_predicates_from_inventory(
        &self,
        negotiation: FileScanNegotiation,
        inventory: &PlannedTaskSetReference,
    ) -> Result<FileScanNegotiation> {
        let mut exact_for_every_partition = vec![true; negotiation.pushed_predicates.len()];
        let mut input = self.inventory_reader(inventory.clone())?;
        while let Some(decoded) = input.next_record()? {
            let partition = partition_for_file_record(
                &self.descriptor,
                &self.plan,
                &negotiation.intent,
                &decoded.record,
                inventory.task_count,
            )?;
            for (index, pushed) in negotiation.pushed_predicates.iter().enumerate() {
                if exact_for_every_partition[index] && pushed.fidelity == PushdownFidelity::Exact {
                    exact_for_every_partition[index] = exact_predicate_is_partition_equivalent(
                        &pushed.predicate,
                        std::slice::from_ref(&partition),
                        self.schema.as_ref(),
                        self.effective_schema_runtime.as_deref(),
                    )?;
                }
            }
        }
        let mut pushed_predicates = Vec::with_capacity(negotiation.pushed_predicates.len());
        let mut unsupported_predicates = negotiation.unsupported_predicates;
        for (pushed, exact) in negotiation
            .pushed_predicates
            .into_iter()
            .zip(exact_for_every_partition)
        {
            if pushed.fidelity != PushdownFidelity::Exact || exact {
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

    pub fn validate_runtime_dependencies(&self) -> Result<()> {
        self.dependencies.task_store().map(|_| ())
    }

    pub fn open_preview(&self, partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        open_file_resource_with_dependencies(self.clone(), partition, None)
    }

    fn attest_partition_with_retention(
        &self,
        partition: PartitionPlan,
        task_retention: Option<PayloadRetention>,
    ) -> cdf_kernel::PartitionAttestationAttempt<'_> {
        let descriptor = self.descriptor.clone();
        let plan = self.plan.clone();
        let dependencies = self.dependencies.clone();
        let effective_schema_runtime = self.effective_schema_runtime.clone();
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
                let _task_retention = task_retention;
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
                let physical_schema_hash = if resolved.identity_strength == GenerationStrength::Weak
                {
                    None
                } else {
                    planned_physical_schema_authority(
                        effective_schema_runtime.as_deref(),
                        &partition,
                    )?
                    .hash
                };
                let processed_position = SourcePosition::FileManifest(cdf_kernel::FileManifest {
                    version: cdf_kernel::SOURCE_POSITION_VERSION,
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
                    physical_schema_hash,
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
}

impl ResourceStream for FileResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn compiled_source_plan_hash(&self) -> Option<&cdf_kernel::CompiledSourcePlanHash> {
        Some(&self.compiled_source_plan_hash)
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
        Err(CdfError::contract(
            "file resources require query negotiation so their canonical partition authority can remain external",
        ))
    }

    fn planned_partition_reader(
        &self,
        reference: &PlannedTaskSetReference,
    ) -> Result<Box<dyn PlannedPartitionReader>> {
        Ok(Box::new(FilePlannedPartitionReader::open(
            self.clone(),
            reference.clone(),
        )?))
    }

    fn rebind_scan_for_resume(
        &self,
        scan: ScanPlan,
        committed_frontier: &SourcePosition,
    ) -> Result<ScanPlan> {
        committed_frontier.validate()?;
        let SourcePosition::FileManifest(committed) = committed_frontier else {
            return Err(CdfError::contract(format!(
                "file resource `{}` requires a committed file manifest for incremental partition selection",
                self.descriptor.resource_id
            )));
        };
        let reference = scan.external_task_set().cloned().ok_or_else(|| {
            CdfError::contract(
                "file incremental partition selection requires external task authority",
            )
        })?;
        let mut previous = BTreeMap::new();
        for file in &committed.files {
            if previous.insert(file.path.as_str(), file).is_some() {
                return Err(CdfError::data(format!(
                    "committed file manifest contains duplicate path `{}`",
                    file.path
                )));
            }
        }
        let (store, options) = self.dependencies.task_store()?;
        let mut input = FilePlannedPartitionReader::open(self.clone(), reference)?;
        let mut output = store.writer(
            FILE_PARTITION_TASK_TYPE,
            options.canonical_limits().tasks,
            self.dependencies.execution().memory(),
            self.dependencies.execution().spill().as_ref(),
        )?;
        let mut input_ordinal = 0_u64;
        let mut output_ordinal = 0_u64;
        let mut selected_bytes = 0_u64;
        while let Some(executable) = input.next_partition(input_ordinal)? {
            let partition = executable.plan();
            let file = partition.planned_file()?.ok_or_else(|| {
                CdfError::contract("file partition task omitted its typed planned file position")
            })?;
            if previous
                .get(file.path.as_str())
                .is_none_or(|prior| !cdf_kernel::same_file_position_identity(prior, file))
            {
                output.push_with(output_ordinal, |writer| {
                    serde_json::to_writer(writer, partition).map_err(|error| {
                        CdfError::data(format!("encode selected file partition task: {error}"))
                    })
                })?;
                output_ordinal = output_ordinal
                    .checked_add(1)
                    .ok_or_else(|| CdfError::data("selected file partition count exceeds u64"))?;
                selected_bytes = selected_bytes
                    .checked_add(file.size_bytes)
                    .ok_or_else(|| CdfError::data("selected file bytes exceed u64"))?;
            }
            input_ordinal = input_ordinal
                .checked_add(1)
                .ok_or_else(|| CdfError::data("file partition count exceeds u64"))?;
        }
        let compiled_source_plan_hash = self.compiled_source_plan_hash.clone();
        let authority = FilePartitionTaskAuthority {
            version: 1,
            resource_id: self.descriptor.resource_id.clone(),
            compiled_source_plan_hash,
            request_hash: cdf_runtime::artifact_hash(&scan.request)?,
        };
        let reference = output
            .finalize(|writer| {
                serde_json::to_writer(writer, &authority).map_err(|error| {
                    CdfError::data(format!("encode selected file partition authority: {error}"))
                })
            })?
            .reference;
        let mut rebound = scan.try_map_partition_authority(|planned| match planned {
            PartitionAuthority::External(_) => Ok(PartitionAuthority::External(reference)),
            PartitionAuthority::Inline(_) => Err(CdfError::contract(
                "file incremental partition selection requires external task authority",
            )),
        })?;
        rebound.planned_source_bytes = Some(cdf_kernel::PlannedSourceBytes::new(selected_bytes));
        Ok(rebound)
    }

    fn open(&self, partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        open_file_resource_with_dependencies(self.clone(), partition, None)
    }

    fn open_executable(
        &self,
        partition: ExecutablePartition,
    ) -> cdf_kernel::PartitionOpenAttempt<'_> {
        let retention = partition.retention().cloned();
        if retention
            .as_ref()
            .and_then(PayloadRetention::downcast_ref::<RetainedFileTask>)
            .is_none()
        {
            return cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async {
                Err(CdfError::contract(
                    "file executable partition omitted its retained canonical task payload",
                ))
            }));
        }
        open_file_resource_with_dependencies(self.clone(), partition.into_plan(), retention)
    }

    fn attest_partition(
        &self,
        partition: PartitionPlan,
    ) -> cdf_kernel::PartitionAttestationAttempt<'_> {
        self.attest_partition_with_retention(partition, None)
    }

    fn attest_executable(
        &self,
        partition: ExecutablePartition,
    ) -> cdf_kernel::PartitionAttestationAttempt<'_> {
        let retention = partition.retention().cloned();
        if retention
            .as_ref()
            .and_then(PayloadRetention::downcast_ref::<RetainedFileTask>)
            .is_none()
        {
            return cdf_kernel::PartitionAttestationAttempt::materialized(Box::pin(async {
                Err(CdfError::contract(
                    "file executable attestation omitted its retained canonical task payload",
                ))
            }));
        }
        self.attest_partition_with_retention(partition.into_plan(), retention)
    }

    fn effective_schema_runtime(&self) -> Option<&EffectiveSchemaRuntime> {
        self.effective_schema_runtime.as_deref()
    }

    fn baseline_observation_schema_catalog(&self) -> &[cdf_kernel::EffectiveSchemaCatalogEntry] {
        &self.baseline_observation_schema_catalog
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
        let inventory = self.inventory_reference_with_limit(usize::MAX, &self.transport_control)?;
        let negotiation =
            self.reconcile_exact_predicates_from_inventory(negotiation, inventory.task_set())?;
        let planned_task_set = self.partition_tasks_from_inventory(
            request,
            &negotiation.intent,
            inventory.task_set(),
        )?;
        Ok(ScanPlan::from_partition_authority(
            PlanId::new(format!("plan-{}", self.descriptor.resource_id))?,
            request.clone(),
            PartitionAuthority::External(planned_task_set),
            negotiation.pushed_predicates,
            negotiation.unsupported_predicates,
            None,
            Some(inventory.planned_source_bytes().get()),
            delivery_guarantee(&self.descriptor),
        ))
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
