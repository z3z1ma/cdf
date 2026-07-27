use std::sync::Arc;

use cdf_kernel::{
    CdfError, CompiledScanIntent, ExecutablePartition, FileManifest, PartitionId, PartitionPlan,
    PartitionRetrySafety, PayloadRetention, PlannedPartitionReader, PlannedTaskSetReference,
    Result, ScopeKey, SourcePosition,
};
use cdf_memory::{MemoryClass, MemoryCoordinator};
use cdf_task_store::{
    ExternalTaskParseMemory, ExternalTaskSetCodec, ExternalTaskStore, RetainedExternalTask,
    TypedExternalTaskSetReader, TypedExternalTaskSetReaderConfig,
};

use crate::{GLUE_TASK_SET_TYPE, GlueObjectTask, GlueSourceOptions, GlueTaskAuthority};

#[derive(Clone)]
pub(crate) struct GlueExecutableTask {
    retained: RetainedExternalTask<GlueTaskAuthority, GlueObjectTask>,
}

impl GlueExecutableTask {
    pub(crate) fn task(&self) -> &GlueObjectTask {
        self.retained.task()
    }

    pub(crate) fn authority(&self) -> &GlueTaskAuthority {
        self.retained.authority()
    }
}

struct GlueTaskCodec;

impl ExternalTaskSetCodec for GlueTaskCodec {
    type Authority = GlueTaskAuthority;
    type Task = GlueObjectTask;

    fn decode_authority(&self, payload: &[u8]) -> Result<Self::Authority> {
        let authority: GlueTaskAuthority = serde_json::from_slice(payload)
            .map_err(|error| CdfError::data(format!("decode Glue task authority: {error}")))?;
        authority.validate()?;
        Ok(authority)
    }

    fn authority_content_sha256(&self, authority: &Self::Authority) -> Result<String> {
        authority.content_sha256()
    }

    fn decode_task(&self, payload: &[u8], authority: &Self::Authority) -> Result<Self::Task> {
        let task: GlueObjectTask = serde_json::from_slice(payload)
            .map_err(|error| CdfError::data(format!("decode Glue object task: {error}")))?;
        task.validate_against(authority)?;
        Ok(task)
    }

    fn task_canonical_ordinal(&self, task: &Self::Task) -> u64 {
        task.canonical_ordinal
    }

    fn task_content_sha256(&self, task: &Self::Task) -> Result<String> {
        task.content_sha256()
    }
}

pub(crate) struct GluePlannedPartitionReader {
    reader: TypedExternalTaskSetReader<GlueTaskCodec>,
}

impl GluePlannedPartitionReader {
    pub(crate) fn open(
        store: &ExternalTaskStore,
        reference: PlannedTaskSetReference,
        source: &GlueSourceOptions,
        memory: Arc<dyn MemoryCoordinator>,
        cancellation: cdf_runtime::RunCancellation,
    ) -> Result<Self> {
        let authority_parse = ExternalTaskParseMemory::blocking(
            "glue-task-authority-parse",
            MemoryClass::Control,
            40_000,
            4096,
        )?;
        let task_parse = ExternalTaskParseMemory::blocking(
            "glue-task-record-parse",
            MemoryClass::Control,
            40_000,
            4096,
        )?;
        let config = TypedExternalTaskSetReaderConfig::new(
            GLUE_TASK_SET_TYPE,
            source.maximum_task_bytes,
            source.maximum_task_authority_bytes,
            authority_parse,
            task_parse,
        )?;
        Ok(Self {
            reader: TypedExternalTaskSetReader::open(
                store,
                reference,
                memory,
                cancellation,
                config,
                GlueTaskCodec,
            )?,
        })
    }

    fn decode_task(
        &self,
        retained: RetainedExternalTask<GlueTaskAuthority, GlueObjectTask>,
    ) -> Result<ExecutablePartition> {
        let task = retained.task();
        let authority = retained.authority();
        let partition_id =
            PartitionId::new(format!("glue-object-{:020}", retained.canonical_ordinal()))?;
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
                version: authority.scan_intent.version,
                projection: authority.scan_intent.projection.clone(),
                predicates: authority.scan_intent.predicates.clone(),
                limit: authority.scan_intent.limit,
                order_by: authority.scan_intent.order_by.clone(),
            },
            retry_safety: PartitionRetrySafety::ImmutableContent,
            metadata: std::collections::BTreeMap::from([(
                "cdf:external_task_sha256".to_owned(),
                retained.content_sha256().to_owned(),
            )]),
        };
        let retained_bytes = retained.retained_bytes();
        Ok(ExecutablePartition::retained(
            plan,
            PayloadRetention::new(Arc::new(GlueExecutableTask { retained }), retained_bytes)?,
        ))
    }
}

impl PlannedPartitionReader for GluePlannedPartitionReader {
    fn next_partition(&mut self, expected_ordinal: u64) -> Result<Option<ExecutablePartition>> {
        let Some(task) = self.reader.next_task(expected_ordinal)? else {
            return Ok(None);
        };
        self.decode_task(task).map(Some)
    }
}
