use std::sync::Arc;

use cdf_kernel::{
    CdfError, ExecutablePartition, PartitionPlan, PayloadRetention, PlannedPartitionReader,
    PlannedTaskSetReference, ResourceId, Result,
};
use cdf_memory::{
    AccountedBytes, ConsumerKey, MemoryClass, MemoryCoordinator, MemoryLease, ReservationRequest,
    reserve_blocking,
};
use cdf_task_store::{CanonicalTaskSetBuilder, ExternalTaskSetReader};
use serde::{Deserialize, Serialize};

use super::{
    FILE_INVENTORY_TASK_TYPE, FILE_PARTITION_TASK_TYPE, FileResource,
    model::{FileInventoryRecord, ResolvedFileMatch},
    resolution::FileMatchSink,
    validation::validate_partition_plan_shape,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FilePartitionTaskAuthority {
    pub(super) version: u16,
    pub(super) resource_id: ResourceId,
    pub(super) compiled_source_plan_hash: cdf_kernel::CompiledSourcePlanHash,
    pub(super) request_hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FileInventoryTaskAuthority {
    pub(super) version: u16,
    pub(super) resource_id: ResourceId,
    pub(super) source_discovery_binding_hash: cdf_kernel::SourceDiscoveryBinding,
}

impl FileInventoryTaskAuthority {
    pub(super) fn validate_against(&self, resource: &FileResource) -> Result<()> {
        if self.version != 1
            || self.resource_id != resource.descriptor.resource_id
            || self.source_discovery_binding_hash != resource.source_discovery_binding_hash
        {
            return Err(CdfError::data(
                "file inventory task authority does not match the compiled resource",
            ));
        }
        cdf_runtime::validate_artifact_hash(
            "file inventory source discovery binding",
            self.source_discovery_binding_hash.as_str(),
        )
    }
}

pub(super) struct FileInventoryTaskBuilder {
    pub(super) inner: CanonicalTaskSetBuilder,
    pub(super) authority: FileInventoryTaskAuthority,
    pub(super) planned_source_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PlannedFileInventory {
    pub(super) task_set: PlannedTaskSetReference,
    pub(super) planned_source_bytes: cdf_kernel::PlannedSourceBytes,
}

impl PlannedFileInventory {
    pub(crate) fn task_set(&self) -> &PlannedTaskSetReference {
        &self.task_set
    }

    pub(crate) fn planned_source_bytes(&self) -> cdf_kernel::PlannedSourceBytes {
        self.planned_source_bytes
    }
}

impl FileInventoryTaskBuilder {
    pub(super) fn push(&mut self, file: &ResolvedFileMatch) -> Result<()> {
        self.planned_source_bytes = self
            .planned_source_bytes
            .checked_add(file.size_bytes)
            .ok_or_else(|| CdfError::data("planned file source bytes exceed u64"))?;
        let record = FileInventoryRecord::from(file);
        self.inner
            .push_idempotent_with(file.path_text.as_bytes(), |output| {
                serde_json::to_writer(output, &record)
                    .map_err(|error| CdfError::data(format!("encode file inventory task: {error}")))
            })
            .map(|_| ())
    }

    pub(super) fn task_count(&self) -> u64 {
        self.inner.task_count()
    }

    pub(super) fn finalize(self) -> Result<PlannedFileInventory> {
        let planned_source_bytes = self.planned_source_bytes;
        self.inner
            .finalize(|output| {
                serde_json::to_writer(output, &self.authority).map_err(|error| {
                    CdfError::data(format!("encode file inventory task authority: {error}"))
                })
            })
            .map(|artifact| PlannedFileInventory {
                task_set: artifact.reference,
                planned_source_bytes: cdf_kernel::PlannedSourceBytes::new(planned_source_bytes),
            })
    }
}

impl FileMatchSink for FileInventoryTaskBuilder {
    fn admit(&mut self, file: ResolvedFileMatch) -> Result<()> {
        self.push(&file)
    }

    fn admitted_count(&self) -> u64 {
        self.task_count()
    }
}

impl FilePartitionTaskAuthority {
    fn validate_against(&self, resource: &FileResource) -> Result<()> {
        if self.version != 1
            || self.resource_id != resource.descriptor.resource_id
            || self.compiled_source_plan_hash != resource.compiled_source_plan_hash
        {
            return Err(CdfError::data(
                "file partition task authority does not match the compiled resource",
            ));
        }
        cdf_runtime::validate_artifact_hash("file partition request", &self.request_hash)
    }
}

pub(super) struct RetainedFileTask {
    _encoded: AccountedBytes,
    _parse: MemoryLease,
}

pub(super) struct DecodedFileInventoryRecord {
    pub(super) record: FileInventoryRecord,
    pub(super) _encoded: AccountedBytes,
    pub(super) _parse: MemoryLease,
}

pub(super) struct FileInventoryTaskReader {
    reader: ExternalTaskSetReader,
    memory: Arc<dyn MemoryCoordinator>,
    parse_amplification_bps: u32,
    _authority_parse: MemoryLease,
}

impl FileInventoryTaskReader {
    pub(super) fn open(
        resource: &FileResource,
        reference: PlannedTaskSetReference,
    ) -> Result<Self> {
        let (store, options) = resource.dependencies.task_store()?;
        let memory = resource.dependencies.execution().memory();
        let reader = store.reader(
            reference,
            FILE_INVENTORY_TASK_TYPE,
            options.maximum_task_bytes,
            options.maximum_authority_bytes,
            Arc::clone(&memory),
        )?;
        let authority_parse = reserve_file_task_parse_memory(
            Arc::clone(&memory),
            u64::try_from(reader.authority().payload().len())
                .map_err(|_| CdfError::data("file inventory authority exceeds u64"))?,
            options.metadata_parse_amplification_bps,
            "file-inventory-authority-parse",
        )?;
        let authority: FileInventoryTaskAuthority =
            serde_json::from_slice(reader.authority().payload()).map_err(|error| {
                CdfError::data(format!("decode file inventory task authority: {error}"))
            })?;
        authority.validate_against(resource)?;
        Ok(Self {
            reader,
            memory,
            parse_amplification_bps: options.metadata_parse_amplification_bps,
            _authority_parse: authority_parse,
        })
    }

    pub(super) fn next_record(&mut self) -> Result<Option<DecodedFileInventoryRecord>> {
        let Some(record) = self.reader.next_record()? else {
            return Ok(None);
        };
        let encoded_bytes = u64::try_from(record.payload.payload().len())
            .map_err(|_| CdfError::data("file inventory task exceeds u64"))?;
        let parse = reserve_file_task_parse_memory(
            Arc::clone(&self.memory),
            encoded_bytes,
            self.parse_amplification_bps,
            "file-inventory-task-parse",
        )?;
        let decoded = serde_json::from_slice(record.payload.payload())
            .map_err(|error| CdfError::data(format!("decode file inventory task: {error}")))?;
        Ok(Some(DecodedFileInventoryRecord {
            record: decoded,
            _encoded: record.payload,
            _parse: parse,
        }))
    }
}

pub(super) struct FilePlannedPartitionReader {
    reader: ExternalTaskSetReader,
    resource: FileResource,
    memory: Arc<dyn MemoryCoordinator>,
    parse_amplification_bps: u32,
    _authority_parse: MemoryLease,
}

impl FilePlannedPartitionReader {
    pub(super) fn open(resource: FileResource, reference: PlannedTaskSetReference) -> Result<Self> {
        let (store, options) = resource.dependencies.task_store()?;
        let maximum_task_bytes = options.maximum_task_bytes;
        let maximum_authority_bytes = options.maximum_authority_bytes;
        let parse_amplification_bps = options.metadata_parse_amplification_bps;
        let memory = resource.dependencies.execution().memory();
        let reader = store.reader(
            reference,
            FILE_PARTITION_TASK_TYPE,
            maximum_task_bytes,
            maximum_authority_bytes,
            Arc::clone(&memory),
        )?;
        let authority_parse = reserve_file_task_parse_memory(
            Arc::clone(&memory),
            u64::try_from(reader.authority().payload().len())
                .map_err(|_| CdfError::data("file task authority exceeds u64"))?,
            parse_amplification_bps,
            "file-task-authority-parse",
        )?;
        let authority: FilePartitionTaskAuthority =
            serde_json::from_slice(reader.authority().payload()).map_err(|error| {
                CdfError::data(format!("decode file partition task authority: {error}"))
            })?;
        authority.validate_against(&resource)?;
        Ok(Self {
            reader,
            resource,
            memory,
            parse_amplification_bps,
            _authority_parse: authority_parse,
        })
    }
}

impl PlannedPartitionReader for FilePlannedPartitionReader {
    fn next_partition(&mut self, expected_ordinal: u64) -> Result<Option<ExecutablePartition>> {
        let Some(record) = self.reader.next_record()? else {
            return Ok(None);
        };
        if record.canonical_ordinal != expected_ordinal {
            return Err(CdfError::data(format!(
                "file task reader returned ordinal {} while execution requested {expected_ordinal}",
                record.canonical_ordinal
            )));
        }
        let encoded_bytes = u64::try_from(record.payload.payload().len())
            .map_err(|_| CdfError::data("file partition task exceeds u64"))?;
        let parse = reserve_file_task_parse_memory(
            Arc::clone(&self.memory),
            encoded_bytes,
            self.parse_amplification_bps,
            "file-partition-task-parse",
        )?;
        let partition: PartitionPlan = serde_json::from_slice(record.payload.payload())
            .map_err(|error| CdfError::data(format!("decode file partition task: {error}")))?;
        validate_partition_plan_shape(&self.resource.descriptor, &self.resource.plan, &partition)?;
        let retained_bytes = encoded_bytes
            .checked_add(parse.bytes())
            .ok_or_else(|| CdfError::data("file retained task bytes exceed u64"))?;
        Ok(Some(ExecutablePartition::retained(
            partition,
            PayloadRetention::new(
                Arc::new(RetainedFileTask {
                    _encoded: record.payload,
                    _parse: parse,
                }),
                retained_bytes,
            )?,
        )))
    }
}

pub(super) fn reserve_file_task_parse_memory(
    memory: Arc<dyn MemoryCoordinator>,
    encoded_bytes: u64,
    amplification_bps: u32,
    consumer: &str,
) -> Result<MemoryLease> {
    let bytes = encoded_bytes
        .checked_mul(u64::from(amplification_bps))
        .and_then(|bytes| bytes.checked_add(9_999))
        .map(|bytes| bytes / 10_000)
        .ok_or_else(|| CdfError::data("file task parse budget overflowed u64"))?
        .max(1);
    reserve_blocking(
        memory,
        &ReservationRequest::new(ConsumerKey::new(consumer, MemoryClass::Control)?, bytes)?,
    )
}
