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
use cdf_memory::{MemoryClass, MemoryCoordinator, MemoryLease};
use cdf_task_store::{
    ExternalTaskParseMemory, ExternalTaskPlanningCodec, ExternalTaskSetCodec, ExternalTaskStore,
    RetainedExternalTask, TypedExternalTaskSetReader, TypedExternalTaskSetReaderConfig,
};

use crate::{
    ICEBERG_TASK_SET_TYPE, IcebergScanTask, IcebergSourceOptions, IcebergTaskSetAuthority,
    catalog::reserve_parse_memory, scan_task::ValidatedIcebergTaskSetAuthority,
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

struct IcebergTaskGenerationAttestation {
    observed_hash: Mutex<Option<String>>,
    _memory: MemoryLease,
}

/// Source-private payload carried through bounded scheduler lookahead.
#[derive(Clone)]
pub(crate) struct IcebergExecutableTask {
    retained: RetainedExternalTask<ValidatedIcebergTaskSetAuthority, IcebergScanTask>,
    generation_attestation: Arc<IcebergTaskGenerationAttestation>,
}

impl IcebergExecutableTask {
    pub(crate) fn task(&self) -> &IcebergScanTask {
        self.retained.task()
    }

    pub(crate) fn authority(&self) -> &ValidatedIcebergTaskSetAuthority {
        self.retained.authority()
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

pub(crate) struct IcebergTaskCodec;

impl ExternalTaskSetCodec for IcebergTaskCodec {
    type Authority = ValidatedIcebergTaskSetAuthority;
    type Task = IcebergScanTask;

    fn decode_authority(&self, payload: &[u8]) -> Result<Self::Authority> {
        serde_json::from_slice::<IcebergTaskSetAuthority>(payload)
            .map_err(|error| CdfError::data(format!("decode Iceberg task authority: {error}")))?
            .into_validated()
    }

    fn authority_content_sha256(&self, authority: &Self::Authority) -> Result<String> {
        Ok(authority.content_sha256().to_owned())
    }

    fn decode_task(&self, payload: &[u8], authority: &Self::Authority) -> Result<Self::Task> {
        let task: IcebergScanTask = serde_json::from_slice(payload)
            .map_err(|error| CdfError::data(format!("decode Iceberg scan task: {error}")))?;
        task.validate_against(authority)?;
        Ok(task)
    }

    fn task_canonical_ordinal(&self, task: &Self::Task) -> u64 {
        task.canonical_ordinal
    }

    fn encode_task(&self, task: &Self::Task, output: &mut dyn std::io::Write) -> Result<()> {
        task.validate()?;
        serde_json::to_writer(output, task)
            .map_err(|error| CdfError::data(format!("encode canonical Iceberg task: {error}")))
    }
}

impl ExternalTaskPlanningCodec for IcebergTaskCodec {
    fn set_task_canonical_ordinal(&self, task: &mut Self::Task, ordinal: u64) {
        task.canonical_ordinal = ordinal;
    }

    fn encode_authority(
        &self,
        authority: &Self::Authority,
        output: &mut dyn std::io::Write,
    ) -> Result<()> {
        authority.encode_to(output)
    }
}

pub(crate) struct IcebergPlannedPartitionReader {
    reader: TypedExternalTaskSetReader<IcebergTaskCodec>,
    memory: Arc<dyn MemoryCoordinator>,
}

impl IcebergPlannedPartitionReader {
    pub(crate) fn open(
        store: &ExternalTaskStore,
        reference: PlannedTaskSetReference,
        source: &IcebergSourceOptions,
        memory: Arc<dyn MemoryCoordinator>,
        cancellation: cdf_runtime::RunCancellation,
    ) -> Result<Self> {
        let authority_parse = ExternalTaskParseMemory::fail_fast(
            "iceberg-task-authority-parse",
            MemoryClass::Discovery,
            source.metadata_parse_amplification_bps,
            0,
        )?;
        let task_parse = ExternalTaskParseMemory::fail_fast(
            "iceberg-task-record-parse",
            MemoryClass::Discovery,
            source.metadata_parse_amplification_bps,
            0,
        )?;
        let config = TypedExternalTaskSetReaderConfig::new(
            ICEBERG_TASK_SET_TYPE,
            source.maximum_task_bytes,
            source.maximum_task_authority_bytes,
            authority_parse,
            task_parse,
        )?;
        Ok(Self {
            reader: TypedExternalTaskSetReader::open(
                store,
                reference,
                Arc::clone(&memory),
                cancellation,
                config,
                IcebergTaskCodec,
            )?,
            memory,
        })
    }

    fn decode_task(
        &self,
        retained: RetainedExternalTask<ValidatedIcebergTaskSetAuthority, IcebergScanTask>,
    ) -> Result<ExecutablePartition> {
        let authority = retained.authority();
        let canonical_ordinal = retained.canonical_ordinal();
        let content_sha256 = retained.content_sha256().to_owned();
        let generation_memory = reserve_parse_memory(
            Arc::clone(&self.memory),
            GENERATION_ATTESTATION_MEMORY_BYTES,
            10_000,
            "iceberg-task-generation-attestation",
        )?;
        let partition_id = PartitionId::new(format!("iceberg-task-{canonical_ordinal:020}"))?;
        let planned_position = authority
            .snapshot
            .clone()
            .map(|snapshot| SourcePosition::TableSnapshot(Box::new(snapshot)));
        let mut metadata = BTreeMap::new();
        metadata.insert(TASK_CONTENT_HASH_KEY.to_owned(), content_sha256);
        metadata.insert(
            TASK_SET_AUTHORITY_HASH_KEY.to_owned(),
            authority.content_sha256().to_owned(),
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
                version: authority.scan_intent.version,
                projection: authority.scan_intent.projection.clone(),
                predicates: authority.scan_intent.predicates.clone(),
                limit: authority.scan_intent.limit,
                order_by: authority.scan_intent.order_by.clone(),
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
        let retained_bytes = retained
            .retained_bytes()
            .checked_add(generation_memory.bytes())
            .ok_or_else(|| CdfError::data("Iceberg retained task bytes overflowed u64"))?;
        let executable = IcebergExecutableTask {
            retained,
            generation_attestation: Arc::new(IcebergTaskGenerationAttestation {
                observed_hash: Mutex::new(None),
                _memory: generation_memory,
            }),
        };
        Ok(ExecutablePartition::retained(
            plan,
            PayloadRetention::new(Arc::new(executable), retained_bytes)?,
        ))
    }
}

impl PlannedPartitionReader for IcebergPlannedPartitionReader {
    fn next_partition(&mut self, expected_ordinal: u64) -> Result<Option<ExecutablePartition>> {
        let Some(task) = self.reader.next_task(expected_ordinal)? else {
            return Ok(None);
        };
        self.decode_task(task).map(Some)
    }
}
