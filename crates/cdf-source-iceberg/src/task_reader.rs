use std::{collections::BTreeMap, sync::Arc};

use cdf_kernel::{
    CdfError, CompiledScanIntent, ExecutablePartition, PartitionId, PartitionPlan,
    PartitionRetrySafety, PayloadRetention, PlannedPartitionReader, PlannedTaskSetReference,
    Result, ScopeKey, SourcePosition,
};
use cdf_memory::{AccountedBytes, MemoryCoordinator, MemoryLease};
use cdf_task_store::{ExternalTaskSetReader, ExternalTaskStore};

use crate::{
    ICEBERG_TASK_SET_TYPE, IcebergScanTask, IcebergSourceOptions, IcebergTaskSetAuthority,
    catalog::reserve_parse_memory,
};

const TASK_CONTENT_HASH_KEY: &str = "cdf:external_task_sha256";

struct RetainedTaskAuthority {
    model: IcebergTaskSetAuthority,
    _encoded: AccountedBytes,
    _parse: MemoryLease,
}

/// Source-private payload carried through bounded scheduler lookahead.
pub(crate) struct IcebergExecutableTask {
    pub(crate) task: IcebergScanTask,
    authority: Arc<RetainedTaskAuthority>,
    _encoded: AccountedBytes,
    _parse: MemoryLease,
}

impl IcebergExecutableTask {
    pub(crate) fn authority(&self) -> &IcebergTaskSetAuthority {
        &self.authority.model
    }
}

pub(crate) struct IcebergPlannedPartitionReader {
    reader: ExternalTaskSetReader,
    authority: Arc<RetainedTaskAuthority>,
    memory: Arc<dyn MemoryCoordinator>,
    parse_amplification_bps: u32,
}

impl IcebergPlannedPartitionReader {
    pub(crate) fn open(
        store: &ExternalTaskStore,
        reference: PlannedTaskSetReference,
        source: &IcebergSourceOptions,
        memory: Arc<dyn MemoryCoordinator>,
    ) -> Result<Self> {
        let reader = store.reader(
            reference,
            ICEBERG_TASK_SET_TYPE,
            source.maximum_task_bytes,
            source.maximum_task_authority_bytes,
            Arc::clone(&memory),
        )?;
        let encoded = reader.authority().clone();
        let parse = reserve_parse_memory(
            Arc::clone(&memory),
            u64::try_from(encoded.payload().len())
                .map_err(|_| CdfError::data("Iceberg task authority exceeds u64"))?,
            source.metadata_parse_amplification_bps,
            "iceberg-task-authority-parse",
        )?;
        let model: IcebergTaskSetAuthority = serde_json::from_slice(encoded.payload())
            .map_err(|error| CdfError::data(format!("decode Iceberg task authority: {error}")))?;
        model.validate()?;
        if model.content_sha256()? != reader.authority_sha256() {
            return Err(CdfError::data(
                "Iceberg task authority model does not match its task-store identity",
            ));
        }
        Ok(Self {
            reader,
            authority: Arc::new(RetainedTaskAuthority {
                model,
                _encoded: encoded,
                _parse: parse,
            }),
            memory,
            parse_amplification_bps: source.metadata_parse_amplification_bps,
        })
    }

    fn decode_task(
        &self,
        record: cdf_task_store::ExternalTaskRecord,
    ) -> Result<ExecutablePartition> {
        let encoded_bytes = u64::try_from(record.payload.payload().len())
            .map_err(|_| CdfError::data("Iceberg task payload exceeds u64"))?;
        let parse = reserve_parse_memory(
            Arc::clone(&self.memory),
            encoded_bytes,
            self.parse_amplification_bps,
            "iceberg-task-record-parse",
        )?;
        let task: IcebergScanTask = serde_json::from_slice(record.payload.payload())
            .map_err(|error| CdfError::data(format!("decode Iceberg scan task: {error}")))?;
        task.validate_against(&self.authority.model)?;
        if task.canonical_ordinal != record.canonical_ordinal
            || task.content_sha256(&self.authority.model)? != record.content_sha256
        {
            return Err(CdfError::data(
                "Iceberg scan task ordinal or content does not match its task-store record",
            ));
        }
        let partition_id =
            PartitionId::new(format!("iceberg-task-{:020}", record.canonical_ordinal))?;
        let planned_position = self
            .authority
            .model
            .snapshot
            .clone()
            .map(|snapshot| SourcePosition::TableSnapshot(Box::new(snapshot)));
        let mut metadata = BTreeMap::new();
        metadata.insert(TASK_CONTENT_HASH_KEY.to_owned(), record.content_sha256);
        let plan = PartitionPlan {
            partition_id: partition_id.clone(),
            scope: ScopeKey::Partition { partition_id },
            planned_position,
            start_position: None,
            scan_intent: CompiledScanIntent {
                version: self.authority.model.scan_intent.version,
                projection: self.authority.model.scan_intent.projection.clone(),
                predicates: self.authority.model.scan_intent.predicates.clone(),
                limit: self.authority.model.scan_intent.limit,
                order_by: self.authority.model.scan_intent.order_by.clone(),
            },
            retry_safety: PartitionRetrySafety::Snapshot,
            metadata,
        };
        let retained_bytes = encoded_bytes
            .checked_add(parse.bytes())
            .ok_or_else(|| CdfError::data("Iceberg retained task bytes overflowed u64"))?;
        let retained = IcebergExecutableTask {
            task,
            authority: Arc::clone(&self.authority),
            _encoded: record.payload,
            _parse: parse,
        };
        Ok(ExecutablePartition::retained(
            plan,
            PayloadRetention::new(Arc::new(retained), retained_bytes)?,
        ))
    }
}

impl PlannedPartitionReader for IcebergPlannedPartitionReader {
    fn next_partition(&mut self, expected_ordinal: u64) -> Result<Option<ExecutablePartition>> {
        let Some(record) = self.reader.next_record()? else {
            return Ok(None);
        };
        if record.canonical_ordinal != expected_ordinal {
            return Err(CdfError::data(format!(
                "Iceberg task reader returned ordinal {} while execution requested {expected_ordinal}",
                record.canonical_ordinal
            )));
        }
        self.decode_task(record).map(Some)
    }
}
