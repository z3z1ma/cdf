use std::sync::Arc;

use cdf_kernel::{
    CdfError, CompiledScanIntent, ExecutablePartition, FileManifest, PartitionId, PartitionPlan,
    PartitionRetrySafety, PayloadRetention, PlannedPartitionReader, PlannedTaskSetReference,
    Result, ScopeKey, SourcePosition,
};
use cdf_memory::{AccountedBytes, MemoryCoordinator, MemoryLease};
use cdf_task_store::{ExternalTaskSetReader, ExternalTaskStore};

use crate::{GLUE_TASK_SET_TYPE, GlueObjectTask, GlueSourceOptions, GlueTaskAuthority};

pub(crate) struct RetainedGlueAuthority {
    model: GlueTaskAuthority,
    _encoded: AccountedBytes,
    _parse: MemoryLease,
}

#[derive(Clone)]
pub(crate) struct GlueExecutableTask {
    pub(crate) task: GlueObjectTask,
    pub(crate) authority: Arc<RetainedGlueAuthority>,
    _encoded: AccountedBytes,
    _parse: MemoryLease,
}

impl GlueExecutableTask {
    pub(crate) fn authority(&self) -> &GlueTaskAuthority {
        &self.authority.model
    }
}

pub(crate) struct GluePlannedPartitionReader {
    reader: ExternalTaskSetReader,
    authority: Arc<RetainedGlueAuthority>,
    memory: Arc<dyn MemoryCoordinator>,
}

impl GluePlannedPartitionReader {
    pub(crate) fn open(
        store: &ExternalTaskStore,
        reference: PlannedTaskSetReference,
        source: &GlueSourceOptions,
        memory: Arc<dyn MemoryCoordinator>,
    ) -> Result<Self> {
        let reader = store.reader(
            reference,
            GLUE_TASK_SET_TYPE,
            source.maximum_task_bytes,
            source.maximum_task_authority_bytes,
            Arc::clone(&memory),
        )?;
        let encoded = reader.authority().clone();
        let parse = reserve_parse(
            Arc::clone(&memory),
            u64::try_from(encoded.payload().len()).unwrap_or(u64::MAX),
            "glue-task-authority-parse",
        )?;
        let authority: GlueTaskAuthority = serde_json::from_slice(encoded.payload())
            .map_err(|error| CdfError::data(format!("decode Glue task authority: {error}")))?;
        authority.validate()?;
        if authority.content_sha256()? != reader.authority_sha256() {
            return Err(CdfError::data(
                "Glue task authority does not match its task-store identity",
            ));
        }
        Ok(Self {
            reader,
            authority: Arc::new(RetainedGlueAuthority {
                model: authority,
                _encoded: encoded,
                _parse: parse,
            }),
            memory,
        })
    }

    fn decode_task(
        &self,
        record: cdf_task_store::ExternalTaskRecord,
    ) -> Result<ExecutablePartition> {
        let encoded_bytes = u64::try_from(record.payload.payload().len())
            .map_err(|_| CdfError::data("Glue task payload exceeds u64"))?;
        let parse = reserve_parse(
            Arc::clone(&self.memory),
            encoded_bytes,
            "glue-task-record-parse",
        )?;
        let task: GlueObjectTask = serde_json::from_slice(record.payload.payload())
            .map_err(|error| CdfError::data(format!("decode Glue object task: {error}")))?;
        task.validate_against(&self.authority.model)?;
        if task.canonical_ordinal != record.canonical_ordinal
            || task.content_sha256()? != record.content_sha256
        {
            return Err(CdfError::data(
                "Glue object task ordinal or content does not match its task-store record",
            ));
        }
        let partition_id =
            PartitionId::new(format!("glue-object-{:020}", record.canonical_ordinal))?;
        let planned_position = SourcePosition::FileManifest(FileManifest {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            files: vec![task.file.clone()],
        });
        let plan = PartitionPlan {
            partition_id: partition_id.clone(),
            scope: ScopeKey::File {
                path: task.file.path.clone(),
            },
            planned_position: Some(planned_position),
            start_position: None,
            scan_intent: CompiledScanIntent {
                version: self.authority.model.scan_intent.version,
                projection: self.authority.model.scan_intent.projection.clone(),
                predicates: self.authority.model.scan_intent.predicates.clone(),
                limit: self.authority.model.scan_intent.limit,
                order_by: self.authority.model.scan_intent.order_by.clone(),
            },
            retry_safety: PartitionRetrySafety::ImmutableContent,
            metadata: std::collections::BTreeMap::from([(
                "cdf:external_task_sha256".to_owned(),
                record.content_sha256,
            )]),
        };
        let retained_bytes = encoded_bytes
            .checked_add(parse.bytes())
            .ok_or_else(|| CdfError::data("Glue retained task bytes overflow u64"))?;
        Ok(ExecutablePartition::retained(
            plan,
            PayloadRetention::new(
                Arc::new(GlueExecutableTask {
                    task,
                    authority: Arc::clone(&self.authority),
                    _encoded: record.payload,
                    _parse: parse,
                }),
                retained_bytes,
            )?,
        ))
    }
}

impl PlannedPartitionReader for GluePlannedPartitionReader {
    fn next_partition(&mut self, expected_ordinal: u64) -> Result<Option<ExecutablePartition>> {
        let Some(record) = self.reader.next_record()? else {
            return Ok(None);
        };
        if record.canonical_ordinal != expected_ordinal {
            return Err(CdfError::data(format!(
                "Glue task reader returned ordinal {} while execution requested {expected_ordinal}",
                record.canonical_ordinal
            )));
        }
        self.decode_task(record).map(Some)
    }
}

fn reserve_parse(
    memory: Arc<dyn MemoryCoordinator>,
    encoded_bytes: u64,
    consumer: &str,
) -> Result<MemoryLease> {
    let bytes = encoded_bytes
        .checked_mul(4)
        .and_then(|value| value.checked_add(4096))
        .ok_or_else(|| CdfError::data("Glue task parse reservation overflowed u64"))?;
    cdf_memory::reserve_blocking(
        memory,
        &cdf_memory::ReservationRequest::new(
            cdf_memory::ConsumerKey::new(consumer, cdf_memory::MemoryClass::Control)?,
            bytes,
        )?,
    )
}
