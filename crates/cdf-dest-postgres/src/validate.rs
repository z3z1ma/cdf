use crate::*;

pub(crate) fn plan_segment_acks(plan: &PostgresLoadPlan) -> Vec<SegmentAck> {
    plan.verify
        .parameters
        .iter()
        .filter_map(|(key, value)| {
            key.strip_prefix("segment.")
                .and_then(|segment_id| {
                    value
                        .split_once(':')
                        .map(|(rows, bytes)| (segment_id, rows, bytes))
                })
                .and_then(|(segment_id, rows, bytes)| {
                    Some(SegmentAck {
                        segment_id: cdf_kernel::SegmentId::new(segment_id).ok()?,
                        row_count: rows.parse().ok()?,
                        byte_count: bytes.parse().ok()?,
                    })
                })
        })
        .collect()
}

pub(crate) fn ensure_supported_disposition(disposition: &WriteDisposition) -> Result<()> {
    match disposition {
        WriteDisposition::Append | WriteDisposition::Replace | WriteDisposition::Merge => Ok(()),
        WriteDisposition::CdcApply => Err(CdfError::destination(
            "Postgres cdc_apply is reserved for the log-CDC ticket",
        )),
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

pub(crate) fn validate_type_fragment(data_type: &str) -> Result<()> {
    let trimmed = data_type.trim();
    if trimmed.is_empty()
        || trimmed.contains(';')
        || trimmed.contains("--")
        || trimmed.contains("/*")
        || trimmed.contains("*/")
        || trimmed.contains('"')
        || trimmed.contains('\'')
    {
        return Err(CdfError::contract(format!(
            "Postgres type fragment {data_type:?} is not allowed"
        )));
    }
    Ok(())
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
    if input.disposition != WriteDisposition::Merge {
        return Ok(());
    }
    if input.merge_keys.is_empty() {
        return Err(CdfError::contract(
            "Postgres merge requires primary or merge keys",
        ));
    }

    let columns = input
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<BTreeSet<_>>();
    for key in &input.merge_keys {
        if !columns.contains(key.as_str()) {
            return Err(CdfError::contract(format!(
                "Postgres merge key {} is not a planned column",
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
                "existing Postgres primary key {:?} does not match merge keys {:?}",
                existing_key, requested_key
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
