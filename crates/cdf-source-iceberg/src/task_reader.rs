use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use cdf_kernel::{
    CdfError, CompiledScanIntent, ExecutablePartition, PLAN_SCHEMA_OBSERVATION_BINDING_KEY,
    PLAN_SCHEMA_OBSERVATION_ID_KEY, PartitionId, PartitionPlan, PartitionRetrySafety,
    PayloadRetention, PlannedPartitionReader, PlannedTaskSetReference, Result, ScopeKey,
    SourcePosition, derive_partition_schema_observation_binding,
};
use cdf_memory::{AccountedBytes, MemoryCoordinator, MemoryLease};
use cdf_task_store::{ExternalTaskSetReader, ExternalTaskStore};

use crate::{
    ICEBERG_TASK_SET_TYPE, IcebergScanTask, IcebergSourceOptions, IcebergTaskSetAuthority,
    ValidatedIcebergTaskSetAuthority, catalog::reserve_parse_memory,
};

const TASK_CONTENT_HASH_KEY: &str = "cdf:external_task_sha256";
const TASK_SET_AUTHORITY_HASH_KEY: &str = "cdf:external_task_set_authority_sha256";
const GENERATION_ATTESTATION_MEMORY_BYTES: u64 = 256;

pub(crate) fn derived_partition_observation_binding(
    plan: &PartitionPlan,
) -> Result<cdf_kernel::SchemaObservationBinding> {
    derive_partition_schema_observation_binding(plan)
}

pub(crate) fn validate_partition_observation_authority(plan: &PartitionPlan) -> Result<()> {
    let recorded = plan
        .metadata
        .get(PLAN_SCHEMA_OBSERVATION_BINDING_KEY)
        .ok_or_else(|| {
            CdfError::contract(format!(
                "Iceberg partition `{}` omitted its schema-observation binding",
                plan.partition_id
            ))
        })?;
    let recorded = cdf_kernel::SchemaObservationBinding::new(recorded.clone())?;
    let derived = derived_partition_observation_binding(plan)?;
    if recorded != derived {
        return Err(CdfError::contract(format!(
            "Iceberg partition `{}` schema-observation binding does not match its immutable task authority",
            plan.partition_id
        )));
    }
    Ok(())
}

struct RetainedTaskAuthority {
    model: ValidatedIcebergTaskSetAuthority,
    _encoded: AccountedBytes,
    _parse: MemoryLease,
}

struct IcebergTaskGenerationAttestation {
    observed_hash: Mutex<Option<String>>,
    _memory: MemoryLease,
}

/// Source-private payload carried through bounded scheduler lookahead.
#[derive(Clone)]
pub(crate) struct IcebergExecutableTask {
    pub(crate) task: IcebergScanTask,
    authority: Arc<RetainedTaskAuthority>,
    generation_attestation: Arc<IcebergTaskGenerationAttestation>,
    _encoded: AccountedBytes,
    _parse: MemoryLease,
}

impl IcebergExecutableTask {
    pub(crate) fn authority(&self) -> &ValidatedIcebergTaskSetAuthority {
        &self.authority.model
    }

    pub(crate) fn attest_attempt_generation(&self, observed_hash: &str) -> Result<()> {
        cdf_runtime::validate_artifact_hash("Iceberg attempt generation", observed_hash)?;
        let mut retained = self
            .generation_attestation
            .observed_hash
            .lock()
            .map_err(|_| CdfError::internal("Iceberg generation attestation is poisoned"))?;
        match retained.as_deref() {
            Some(expected) if expected != observed_hash => Err(CdfError::data(
                "Iceberg object generation changed between attempts for one immutable scan task",
            )),
            Some(_) => Ok(()),
            None => {
                *retained = Some(observed_hash.to_owned());
                Ok(())
            }
        }
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
        let model = model.into_validated()?;
        if model.content_sha256() != reader.authority_sha256() {
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
            || task.content_sha256()? != record.content_sha256
        {
            return Err(CdfError::data(
                "Iceberg scan task ordinal or content does not match its task-store record",
            ));
        }
        let generation_memory = reserve_parse_memory(
            Arc::clone(&self.memory),
            GENERATION_ATTESTATION_MEMORY_BYTES,
            10_000,
            "iceberg-task-generation-attestation",
        )?;
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
        metadata.insert(
            TASK_SET_AUTHORITY_HASH_KEY.to_owned(),
            self.authority.model.content_sha256().to_owned(),
        );
        metadata.insert(
            PLAN_SCHEMA_OBSERVATION_ID_KEY.to_owned(),
            partition_id.to_string(),
        );
        let mut plan = PartitionPlan {
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
        let observation_binding = derived_partition_observation_binding(&plan)?;
        plan.metadata.insert(
            PLAN_SCHEMA_OBSERVATION_BINDING_KEY.to_owned(),
            observation_binding.to_string(),
        );
        validate_partition_observation_authority(&plan)?;
        let retained_bytes = encoded_bytes
            .checked_add(parse.bytes())
            .and_then(|bytes| bytes.checked_add(generation_memory.bytes()))
            .ok_or_else(|| CdfError::data("Iceberg retained task bytes overflowed u64"))?;
        let retained = IcebergExecutableTask {
            task,
            authority: Arc::clone(&self.authority),
            generation_attestation: Arc::new(IcebergTaskGenerationAttestation {
                observed_hash: Mutex::new(None),
                _memory: generation_memory,
            }),
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
