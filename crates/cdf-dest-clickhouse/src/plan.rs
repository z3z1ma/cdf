use cdf_kernel::{
    CdfError, CommitPlan, DeliveryGuarantee, DestinationCommitRequest, IdempotencySupport,
    MigrationRecord, PlanId, Result, WriteDisposition,
};
use sha2::{Digest, Sha256};

use crate::{
    CLICKHOUSE_DESTINATION_ID,
    identifier::ClickHouseIdentifier,
    models::{ClickHouseLoadPlan, ClickHouseLoadPlanInput, ClickHouseMergeMode},
    package::package_hash_hex,
    receipt::verify_clause,
};

pub(crate) fn plan_clickhouse_load(input: ClickHouseLoadPlanInput) -> Result<ClickHouseLoadPlan> {
    ensure_supported_disposition(&input.disposition)?;
    validate_merge_shape(&input)?;
    let merge_mode = if input.disposition == WriteDisposition::Merge {
        input.merge_mode
    } else {
        ClickHouseMergeMode::default()
    };
    if input.idempotency_token.as_str() != input.package_hash.as_str() {
        return Err(CdfError::contract(
            "ClickHouse destination idempotency token must equal the finalized package hash",
        ));
    }
    if input.columns.iter().any(|column| {
        column.name.as_str().starts_with("_cdf_")
            && !(column.framework_owned
                && column.name.as_str() == cdf_contract::VARIANT_COLUMN_NAME)
    }) {
        return Err(CdfError::contract(
            "ClickHouse destination user schema cannot use the reserved _cdf_ prefix",
        ));
    }
    let target = input.target.as_str();
    let kernel_target = cdf_kernel::TargetName::new(target)?;
    let stage = stage_name("publish", &input.package_hash)?;
    let incoming_stage = stage_name("incoming", &input.package_hash)?;
    let kernel = CommitPlan {
        plan_id: plan_id(
            target,
            &input.disposition,
            merge_mode,
            input.package_hash.as_str(),
        )?,
        target: kernel_target.clone(),
        disposition: input.disposition.clone(),
        idempotency: IdempotencySupport::PackageToken,
        migrations: vec![MigrationRecord {
            migration_id: "clickhouse.system_mirrors.v1".to_owned(),
            description:
                "create typed _cdf_loads/_cdf_segments/_cdf_state settlement mirrors if absent"
                    .to_owned(),
        }],
        delivery_guarantee: delivery_guarantee(&input.disposition),
    };
    let verify = verify_clause(
        &kernel_target,
        &input.package_hash,
        &input.idempotency_token,
        &input.schema_hash,
        &input.segments,
    );
    Ok(ClickHouseLoadPlan {
        kernel,
        package_hash: input.package_hash,
        content: input.content,
        idempotency_token: input.idempotency_token,
        schema_hash: input.schema_hash,
        segments: input.segments,
        target: input.target,
        columns: input.columns,
        merge_keys: input.merge_keys,
        merge_mode,
        stage,
        incoming_stage,
        resource_id: input.resource_id,
        state_delta: input.state_delta,
        verify,
    })
}

pub(crate) fn plan_clickhouse_commit(
    request: &DestinationCommitRequest,
    configured_merge_mode: ClickHouseMergeMode,
) -> Result<CommitPlan> {
    ensure_supported_disposition(&request.disposition)?;
    if request.idempotency_token.as_str() != request.package_hash.as_str() {
        return Err(CdfError::contract(
            "ClickHouse destination idempotency token must equal the finalized package hash",
        ));
    }
    let merge_mode = if request.disposition == WriteDisposition::Merge {
        configured_merge_mode
    } else {
        ClickHouseMergeMode::default()
    };
    Ok(CommitPlan {
        plan_id: plan_id(
            request.target.as_str(),
            &request.disposition,
            merge_mode,
            request.package_hash.as_str(),
        )?,
        target: request.target.clone(),
        disposition: request.disposition.clone(),
        idempotency: IdempotencySupport::PackageToken,
        migrations: vec![MigrationRecord {
            migration_id: "clickhouse.system_mirrors.v1".to_owned(),
            description: "create ClickHouse destination settlement mirrors if absent".to_owned(),
        }],
        delivery_guarantee: delivery_guarantee(&request.disposition),
    })
}

pub(crate) fn ensure_supported_disposition(disposition: &WriteDisposition) -> Result<()> {
    match disposition {
        WriteDisposition::Append | WriteDisposition::Replace | WriteDisposition::Merge => Ok(()),
        WriteDisposition::CdcApply => Err(CdfError::contract(
            "ClickHouse destination does not support cdc_apply",
        )),
    }
}

pub(crate) fn delivery_guarantee(disposition: &WriteDisposition) -> DeliveryGuarantee {
    match disposition {
        WriteDisposition::Append => DeliveryGuarantee::EffectivelyOncePerPackage,
        WriteDisposition::Replace => DeliveryGuarantee::EffectivelyOncePerTarget,
        WriteDisposition::Merge => DeliveryGuarantee::EffectivelyOncePerKey,
        WriteDisposition::CdcApply => DeliveryGuarantee::AtLeastOnceDuplicateRisk,
    }
}

pub(crate) fn segment_token(plan: &ClickHouseLoadPlan, segment: &cdf_kernel::SegmentId) -> String {
    digest_token(
        "segment",
        plan.target.as_str(),
        plan.package_hash.as_str(),
        &format!("{}:{}", plan.merge_mode.as_str(), segment.as_str()),
    )
}

pub(crate) fn mirror_token(plan: &ClickHouseLoadPlan, kind: &str, identity: &str) -> String {
    digest_token(
        kind,
        plan.target.as_str(),
        plan.package_hash.as_str(),
        &format!("{}:{identity}", plan.merge_mode.as_str()),
    )
}

fn digest_token(kind: &str, target: &str, package_hash: &str, identity: &str) -> String {
    let mut hasher = Sha256::new();
    for part in [
        CLICKHOUSE_DESTINATION_ID,
        kind,
        target,
        package_hash,
        identity,
    ] {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    format!("cdf-{:x}", hasher.finalize())
}

fn stage_name(kind: &str, package_hash: &cdf_kernel::PackageHash) -> Result<ClickHouseIdentifier> {
    let hash = package_hash_hex(package_hash)?;
    let prefix = hash
        .get(..16)
        .ok_or_else(|| CdfError::contract("ClickHouse package hash is shorter than 16 bytes"))?;
    ClickHouseIdentifier::framework(format!("_cdf_{kind}_{}", prefix.to_ascii_lowercase()))
}

fn plan_id(
    target: &str,
    disposition: &WriteDisposition,
    merge_mode: ClickHouseMergeMode,
    package_hash: &str,
) -> Result<PlanId> {
    let mut hasher = Sha256::new();
    hasher.update(CLICKHOUSE_DESTINATION_ID.as_bytes());
    hasher.update([0]);
    hasher.update(target.as_bytes());
    hasher.update([0]);
    hasher.update(format!("{disposition:?}").as_bytes());
    hasher.update([0]);
    hasher.update(merge_mode.as_str().as_bytes());
    hasher.update([0]);
    hasher.update(package_hash.as_bytes());
    PlanId::new(format!("clickhouse-{:x}", hasher.finalize()))
}

fn validate_merge_shape(input: &ClickHouseLoadPlanInput) -> Result<()> {
    if input.disposition != WriteDisposition::Merge {
        if input.merge_keys.is_empty() {
            return Ok(());
        }
        return Err(CdfError::contract(
            "ClickHouse merge keys are valid only for merge disposition",
        ));
    }
    if input.merge_keys.is_empty() {
        return Err(CdfError::contract(
            "ClickHouse merge requires at least one normalized merge key",
        ));
    }
    let mut distinct = std::collections::BTreeSet::new();
    for key in &input.merge_keys {
        if !distinct.insert(key) {
            return Err(CdfError::contract(format!(
                "ClickHouse merge key {key} is declared more than once"
            )));
        }
        let column = input
            .columns
            .iter()
            .find(|column| column.name == *key)
            .ok_or_else(|| {
                CdfError::contract(format!(
                    "ClickHouse merge key {key} is absent from the output schema"
                ))
            })?;
        if column.nullable {
            return Err(CdfError::contract(format!(
                "ClickHouse merge key {key} must be non-nullable"
            )));
        }
    }
    Ok(())
}
