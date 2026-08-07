use cdf_kernel::{
    CdfError, CommitPlan, DeliveryGuarantee, DestinationCommitRequest, IdempotencySupport,
    MigrationRecord, PlanId, Result, WriteDisposition,
};
use sha2::{Digest, Sha256};

use crate::{
    SQLITE_DESTINATION_ID,
    models::{SqliteLoadPlan, SqliteLoadPlanInput},
    receipts::verify_clause,
};

pub(crate) fn plan_sqlite_load(input: SqliteLoadPlanInput) -> Result<SqliteLoadPlan> {
    ensure_supported_disposition(&input.disposition)?;
    if input.idempotency_token.as_str() != input.package_hash.as_str() {
        return Err(CdfError::contract(
            "SQLite destination idempotency token must equal the finalized package hash",
        ));
    }
    if input.columns.iter().any(|column| {
        column.name.as_str().starts_with("_cdf_")
            && !(column.framework_owned
                && column.name.as_str() == cdf_contract::VARIANT_COLUMN_NAME)
    }) {
        return Err(CdfError::contract(
            "SQLite destination user schema cannot use the reserved _cdf_ prefix",
        ));
    }
    if input.columns.iter().any(|column| {
        column.framework_owned && column.name.as_str() != cdf_contract::VARIANT_COLUMN_NAME
    }) {
        return Err(CdfError::internal(
            "SQLite destination plan contains an unknown framework-owned column",
        ));
    }
    if input.disposition == WriteDisposition::Merge {
        if input.merge_keys.is_empty() {
            return Err(CdfError::contract(
                "SQLite merge requires at least one normalized merge key",
            ));
        }
        for key in &input.merge_keys {
            if !input.columns.iter().any(|column| column.name == *key) {
                return Err(CdfError::contract(format!(
                    "SQLite merge key `{key}` is absent from the output schema"
                )));
            }
        }
    } else if !input.merge_keys.is_empty() {
        return Err(CdfError::contract(
            "SQLite merge keys are valid only for merge disposition",
        ));
    }
    let target = input.target.as_str();
    let kernel_target = cdf_kernel::TargetName::new(target)?;
    let migrations = vec![
        MigrationRecord {
            migration_id: "sqlite.system_mirrors.v1".to_owned(),
            description: "create typed _cdf_loads/_cdf_state/_cdf_segments/_cdf_quarantine mirrors and row-key allocator if absent".to_owned(),
        },
        MigrationRecord {
            migration_id: format!("sqlite.target.{target}.v1"),
            description: format!(
                "create or validate SQLite target {target} and compact _cdf_row_key provenance"
            ),
        },
    ];
    let kernel = CommitPlan {
        plan_id: plan_id(target, &input.disposition, input.package_hash.as_str())?,
        target: kernel_target.clone(),
        disposition: input.disposition.clone(),
        idempotency: IdempotencySupport::PackageToken,
        migrations,
        delivery_guarantee: delivery_guarantee(&input.disposition),
    };
    let verify = verify_clause(
        &kernel_target,
        &input.package_hash,
        &input.idempotency_token,
        &input.schema_hash,
        &input.segments,
    );
    Ok(SqliteLoadPlan {
        kernel,
        package_hash: input.package_hash,
        content: input.content,
        idempotency_token: input.idempotency_token,
        schema_hash: input.schema_hash,
        segments: input.segments,
        target: input.target,
        columns: input.columns,
        merge_keys: input.merge_keys,
        resource_id: input.resource_id,
        state_delta: input.state_delta,
        verify,
    })
}

pub(crate) fn plan_sqlite_commit(request: &DestinationCommitRequest) -> Result<CommitPlan> {
    ensure_supported_disposition(&request.disposition)?;
    if request.idempotency_token.as_str() != request.package_hash.as_str() {
        return Err(CdfError::contract(
            "SQLite destination idempotency token must equal the finalized package hash",
        ));
    }
    Ok(CommitPlan {
        plan_id: plan_id(
            request.target.as_str(),
            &request.disposition,
            request.package_hash.as_str(),
        )?,
        target: request.target.clone(),
        disposition: request.disposition.clone(),
        idempotency: IdempotencySupport::PackageToken,
        migrations: vec![MigrationRecord {
            migration_id: "sqlite.system_mirrors.v1".to_owned(),
            description: "create SQLite destination system mirrors if absent".to_owned(),
        }],
        delivery_guarantee: delivery_guarantee(&request.disposition),
    })
}

pub(crate) fn ensure_supported_disposition(disposition: &WriteDisposition) -> Result<()> {
    if matches!(
        disposition,
        WriteDisposition::Append | WriteDisposition::Replace | WriteDisposition::Merge
    ) {
        Ok(())
    } else {
        Err(CdfError::contract(format!(
            "SQLite destination does not support {disposition:?}; use append, replace, or merge"
        )))
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

fn plan_id(target: &str, disposition: &WriteDisposition, package_hash: &str) -> Result<PlanId> {
    let mut hasher = Sha256::new();
    hasher.update(SQLITE_DESTINATION_ID.as_bytes());
    hasher.update([0]);
    hasher.update(target.as_bytes());
    hasher.update([0]);
    hasher.update(format!("{disposition:?}").as_bytes());
    hasher.update([0]);
    hasher.update(package_hash.as_bytes());
    PlanId::new(format!("sqlite-{:x}", hasher.finalize()))
}
