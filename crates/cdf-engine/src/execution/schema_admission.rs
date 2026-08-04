//! Execution-time schema admission ownership.

use arrow_array::RecordBatch;
use arrow_schema::Schema;

use cdf_kernel::{
    CdfError, PLAN_PHYSICAL_SCHEMA_HASH_KEY, PLAN_SCHEMA_OBSERVATION_BINDING_KEY, ResourceStream,
    Result,
};

use crate::{
    EffectiveSchemaObservationCoercion, EffectiveSchemaPlanEvidence, EnginePlan,
    PhysicalObservationEvidence,
};

pub(super) struct AdmittedBatchSchema {
    pub(super) record_batch: RecordBatch,
    pub(super) coercion_plan: Option<cdf_contract::SchemaCoercionPlan>,
    pub(super) observation_id: Option<String>,
    pub(super) physical_observation: Option<PhysicalObservationEvidence>,
    pub(super) extra_field_evidence: ExtraFieldEvidence,
}

#[derive(Clone, Copy)]
pub(super) enum ExtraFieldEvidence {
    AlreadyCaptured,
    CaptureFromPhysicalBatch,
}

pub(super) enum BatchSchemaDisposition {
    Admitted(AdmittedBatchSchema),
    Quarantined {
        quarantine: Box<cdf_kernel::TerminalSchemaObservationQuarantine>,
        physical_observation: PhysicalObservationEvidence,
    },
}

pub(super) struct BatchSchemaAdmissionContext<'a> {
    pub(super) planned_observation_id: &'a str,
    pub(super) expected: Option<&'a EffectiveSchemaObservationCoercion>,
    pub(super) expected_physical_observation: Option<&'a PhysicalObservationEvidence>,
    pub(super) effective_schema: &'a Schema,
}

pub(super) enum PartitionSchemaDisposition {
    Admitted(EffectiveSchemaObservationCoercion),
    Quarantined(Box<cdf_kernel::TerminalSchemaObservationQuarantine>),
    Unobserved,
}

pub(super) fn validate_effective_schema_plan<'a, R>(
    plan: &'a EnginePlan,
    resource: &R,
) -> Result<Option<&'a EffectiveSchemaPlanEvidence>>
where
    R: ResourceStream + ?Sized,
{
    let Some(evidence) = plan.effective_schema_evidence.as_ref() else {
        if resource.effective_schema_runtime().is_some() {
            return Err(CdfError::data(
                "resource carries effective-schema evidence but the serialized engine plan omitted it",
            ));
        }
        return Ok(None);
    };
    let schema_authority = plan.schema_authority();
    if schema_authority.baseline_schema_hash != *evidence.authority.baseline.schema_hash()
        || schema_authority.effective_schema_hash != evidence.authority.effective_schema_hash
    {
        return Err(CdfError::data(
            "engine plan schema authority does not match effective-schema evidence",
        ));
    }
    evidence
        .authority
        .validate_for_resource(resource.descriptor())?;
    let effective_arrow_schema_hash =
        cdf_kernel::canonical_arrow_schema_hash(resource.schema().as_ref())?;
    if evidence.effective_arrow_schema_hash != effective_arrow_schema_hash {
        return Err(CdfError::data(format!(
            "serialized effective Arrow schema hash {} does not match execution resource schema {}",
            evidence.effective_arrow_schema_hash, effective_arrow_schema_hash
        )));
    }
    if resource
        .effective_schema_runtime()
        .map(|runtime| &runtime.evidence)
        != Some(&evidence.authority)
    {
        return Err(CdfError::data(
            "serialized engine plan effective-schema evidence does not match the execution resource",
        ));
    }
    if resource
        .effective_schema_runtime()
        .map(|runtime| runtime.terminal_quarantines.as_slice())
        != Some(evidence.terminal_quarantines.as_slice())
    {
        return Err(CdfError::data(
            "serialized terminal schema-observation evidence does not match the execution resource",
        ));
    }
    if resource
        .effective_schema_runtime()
        .and_then(|runtime| runtime.discovery_executor_budget.as_ref())
        != evidence.discovery_executor_budget.as_ref()
    {
        return Err(CdfError::data(
            "serialized discovery executor budget does not match the execution resource",
        ));
    }
    for partition in plan.scan.inline_partitions().unwrap_or_default() {
        partition_schema_disposition(partition, evidence, false)?;
    }
    Ok(Some(evidence))
}

fn validate_plan_metadata(
    partition: &cdf_kernel::PartitionPlan,
    key: &str,
    expected: &str,
) -> Result<()> {
    if partition.metadata.get(key).map(String::as_str) != Some(expected) {
        return Err(CdfError::data(format!(
            "planned partition {} has missing or spoofed {key} effective-schema evidence",
            partition.partition_id
        )));
    }
    Ok(())
}

pub(super) fn partition_schema_disposition(
    partition: &cdf_kernel::PartitionPlan,
    evidence: &EffectiveSchemaPlanEvidence,
    external_task_identity_authority: bool,
) -> Result<PartitionSchemaDisposition> {
    let observation_id = cdf_kernel::partition_schema_observation_id(partition);
    let expected_binding = evidence.observation_bindings.get(observation_id);
    if let Some(expected_binding) = expected_binding {
        validate_plan_metadata(
            partition,
            PLAN_SCHEMA_OBSERVATION_BINDING_KEY,
            expected_binding.as_str(),
        )?;
    } else if evidence.authority.observation(observation_id).is_some()
        || evidence
            .terminal_quarantines
            .binary_search_by(|item| item.observation_id().cmp(observation_id))
            .is_ok()
    {
        return Err(CdfError::data(format!(
            "effective-schema evidence omitted source identity binding for known observation {observation_id:?}"
        )));
    } else {
        if partition
            .metadata
            .contains_key(PLAN_PHYSICAL_SCHEMA_HASH_KEY)
            || (!external_task_identity_authority
                && partition
                    .metadata
                    .contains_key(PLAN_SCHEMA_OBSERVATION_BINDING_KEY))
        {
            return Err(CdfError::data(format!(
                "unobserved schema candidate {observation_id:?} carries spoofed pre-observation evidence"
            )));
        }
        if external_task_identity_authority {
            cdf_kernel::partition_schema_observation_binding(partition)?;
        }
        return Ok(PartitionSchemaDisposition::Unobserved);
    }
    if let Some(quarantine) = evidence
        .terminal_quarantines
        .binary_search_by(|item| item.observation_id().cmp(observation_id))
        .ok()
        .map(|index| &evidence.terminal_quarantines[index])
    {
        if !external_task_identity_authority
            || partition
                .metadata
                .contains_key(PLAN_PHYSICAL_SCHEMA_HASH_KEY)
        {
            validate_plan_metadata(
                partition,
                PLAN_PHYSICAL_SCHEMA_HASH_KEY,
                quarantine.physical_schema_hash().as_str(),
            )?;
        }
        return Ok(PartitionSchemaDisposition::Quarantined(Box::new(
            quarantine.clone(),
        )));
    }
    let observation = evidence
        .observations
        .binary_search_by(|observation| observation.observation_id.as_str().cmp(observation_id))
        .ok()
        .map(|index| &evidence.observations[index]);
    let Some(observation) = observation else {
        if partition
            .metadata
            .contains_key(PLAN_PHYSICAL_SCHEMA_HASH_KEY)
        {
            return Err(CdfError::data(format!(
                "unobserved schema candidate {observation_id:?} carries spoofed physical-schema evidence"
            )));
        }
        return Ok(PartitionSchemaDisposition::Unobserved);
    };
    if !external_task_identity_authority
        || partition
            .metadata
            .contains_key(PLAN_PHYSICAL_SCHEMA_HASH_KEY)
    {
        validate_plan_metadata(
            partition,
            PLAN_PHYSICAL_SCHEMA_HASH_KEY,
            observation.physical_schema_hash.as_str(),
        )?;
    }
    Ok(PartitionSchemaDisposition::Admitted(observation.clone()))
}
