use std::collections::BTreeSet;

use cdf_kernel::{
    CdfError, DeliveryGuarantee, PackageHash, PlanId, Result, SegmentAck, StateSegment, TargetName,
    WriteDisposition,
};
use cdf_postgres::PostgresIdentifier;

use crate::{
    identifiers::PostgresColumn,
    plan::{PostgresLoadPlan, PostgresLoadPlanInput},
};

pub(crate) fn plan_segments_in_receipt_order(plan: &PostgresLoadPlan) -> Vec<StateSegment> {
    plan.segments.clone()
}

pub(crate) fn plan_segment_acks(plan: &PostgresLoadPlan) -> Vec<SegmentAck> {
    plan_segments_in_receipt_order(plan)
        .iter()
        .map(|segment| SegmentAck {
            kind: segment.kind,
            segment_id: segment.segment_id.clone(),
            row_count: segment.row_count,
            byte_count: segment.byte_count,
        })
        .collect()
}

pub(crate) fn ensure_supported_disposition(disposition: &WriteDisposition) -> Result<()> {
    match disposition {
        WriteDisposition::Append
        | WriteDisposition::Replace
        | WriteDisposition::Merge
        | WriteDisposition::CdcApply => Ok(()),
    }
}

pub(crate) fn delivery_guarantee(disposition: &WriteDisposition) -> DeliveryGuarantee {
    match disposition {
        WriteDisposition::Append => DeliveryGuarantee::EffectivelyOncePerPackage,
        WriteDisposition::Replace => DeliveryGuarantee::EffectivelyOncePerTarget,
        WriteDisposition::Merge => DeliveryGuarantee::EffectivelyOncePerKey,
        WriteDisposition::CdcApply => DeliveryGuarantee::EffectivelyOncePerPosition,
    }
}

pub(crate) fn plan_id(
    target: &TargetName,
    disposition: &WriteDisposition,
    token: &str,
) -> Result<PlanId> {
    PlanId::new(format!(
        "postgres:{}:{}:{}",
        disposition_name(disposition),
        target.as_str().replace('.', "_"),
        token_suffix(token)
    ))
}

pub(crate) fn disposition_name(disposition: &WriteDisposition) -> &'static str {
    match disposition {
        WriteDisposition::Append => "append",
        WriteDisposition::Replace => "replace",
        WriteDisposition::Merge => "merge",
        WriteDisposition::CdcApply => "cdc_apply",
    }
}

pub(crate) fn token_suffix(token: &str) -> String {
    let mut suffix = token
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .map(|character| character.to_ascii_lowercase())
        .take(24)
        .collect::<String>();
    if suffix.is_empty() {
        suffix.push_str("token");
    }
    suffix
}

pub(crate) fn validate_columns(columns: &[PostgresColumn]) -> Result<()> {
    if columns.is_empty() {
        return Err(CdfError::contract(
            "Postgres destination requires at least one data column",
        ));
    }
    let mut names = BTreeSet::new();
    for column in columns {
        if !names.insert(column.name.as_str()) {
            return Err(CdfError::contract(format!(
                "duplicate Postgres column {}",
                column.name.as_str()
            )));
        }
    }
    Ok(())
}

pub(crate) fn validate_merge_shape(input: &PostgresLoadPlanInput) -> Result<()> {
    if !matches!(
        input.disposition,
        WriteDisposition::Merge | WriteDisposition::CdcApply
    ) {
        return Ok(());
    }
    if input.merge_keys.is_empty() {
        return Err(CdfError::contract(format!(
            "Postgres {} requires a nonempty ordered effect key",
            disposition_name(&input.disposition)
        )));
    }

    let columns = input
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<BTreeSet<_>>();
    for key in &input.merge_keys {
        if !columns.contains(key.as_str()) {
            return Err(CdfError::contract(format!(
                "Postgres {} key {} is not a planned column",
                disposition_name(&input.disposition),
                key.as_str()
            )));
        }
    }

    if let Some(existing) = &input.existing_table {
        let existing_key = existing
            .primary_key
            .iter()
            .map(PostgresIdentifier::as_str)
            .collect::<Vec<_>>();
        let requested_key = input
            .merge_keys
            .iter()
            .map(PostgresIdentifier::as_str)
            .collect::<Vec<_>>();
        if existing_key != requested_key {
            return Err(CdfError::destination(format!(
                "existing Postgres primary key {:?} does not match {} keys {:?}",
                existing_key,
                disposition_name(&input.disposition),
                requested_key
            )));
        }
    }

    if input.disposition == WriteDisposition::CdcApply {
        validate_cdc_content(input)?;
    }

    Ok(())
}

pub(crate) fn validate_cdc_content(input: &PostgresLoadPlanInput) -> Result<()> {
    use cdf_kernel::{
        DeleteApplicationAuthority, DeleteApplicationPolicy, PackageContentAuthority,
    };

    let PackageContentAuthority::KeyedChanges {
        logical_schema_hash,
        key,
        deletion_capture,
        delete_application,
        ..
    } = &input.content
    else {
        return Err(CdfError::contract(
            "Postgres cdc_apply requires package-native keyed-change content",
        ));
    };
    input.content.validate()?;
    if logical_schema_hash != &input.schema_hash {
        return Err(CdfError::data(
            "Postgres cdc_apply logical schema differs from destination schema authority",
        ));
    }
    let planned = input
        .merge_keys
        .iter()
        .map(PostgresIdentifier::as_str)
        .collect::<Vec<_>>();
    let package = key.fields.iter().map(String::as_str).collect::<Vec<_>>();
    if package != planned {
        return Err(CdfError::contract(format!(
            "Postgres cdc_apply package key {package:?} differs from planned ordered key {planned:?}; recompile the resource and package with one key authority"
        )));
    }
    if !deletion_capture.enabled {
        return Err(CdfError::contract(
            "Postgres cdc_apply requires enabled deletion capture",
        ));
    }
    let DeleteApplicationAuthority::Apply { policy } = delete_application else {
        return Err(CdfError::contract(
            "Postgres cdc_apply requires an explicit DELETE IGNORE, DELETE HARD, or DELETE SOFT(marker) policy",
        ));
    };
    if let DeleteApplicationPolicy::Soft { marker_field } = policy {
        if key.fields.iter().any(|key| key == marker_field) {
            return Err(CdfError::contract(format!(
                "Postgres soft-delete marker `{marker_field}` cannot also be an effect key"
            )));
        }
        if input
            .columns
            .iter()
            .any(|column| column.name.as_str() == marker_field)
        {
            return Err(CdfError::contract(format!(
                "Postgres soft-delete marker `{marker_field}` is destination-owned and cannot be a source/output column"
            )));
        }
    }
    Ok(())
}

pub(crate) fn stage_table_name(package_hash: &PackageHash) -> Result<PostgresIdentifier> {
    PostgresIdentifier::system(format!(
        "_cdf_stage_{}",
        token_suffix(package_hash.as_str())
    ))
}
