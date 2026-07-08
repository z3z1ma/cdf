use super::prelude::*;

pub(super) fn validate_destination_receipt_before_checkpoint(
    delta: &StateDelta,
    target: &TargetName,
    disposition: &WriteDisposition,
    receipt: &Receipt,
) -> Result<()> {
    validate_receipt_identity(delta, target, disposition, receipt)
}

fn validate_receipt_identity(
    delta: &StateDelta,
    target: &TargetName,
    disposition: &WriteDisposition,
    receipt: &Receipt,
) -> Result<()> {
    if receipt.package_hash != delta.package_hash {
        return Err(CdfError::contract(format!(
            "receipt {} package hash {} does not match StateDelta package hash {}",
            receipt.receipt_id, receipt.package_hash, delta.package_hash
        )));
    }
    if receipt.schema_hash != delta.schema_hash {
        return Err(CdfError::contract(format!(
            "receipt {} schema hash {} does not match StateDelta schema hash {}",
            receipt.receipt_id, receipt.schema_hash, delta.schema_hash
        )));
    }
    if &receipt.target != target {
        return Err(CdfError::contract(format!(
            "receipt {} target {} does not match explicit target {}",
            receipt.receipt_id, receipt.target, target
        )));
    }
    if &receipt.disposition != disposition {
        return Err(CdfError::contract(format!(
            "receipt {} disposition {:?} does not match explicit disposition {:?}",
            receipt.receipt_id, receipt.disposition, disposition
        )));
    }
    if receipt.idempotency_token.as_str() != delta.package_hash.as_str() {
        return Err(CdfError::contract(format!(
            "receipt {} idempotency token {} does not match package hash {}",
            receipt.receipt_id, receipt.idempotency_token, delta.package_hash
        )));
    }
    validate_segment_acks(delta, receipt)
}

fn validate_segment_acks(delta: &StateDelta, receipt: &Receipt) -> Result<()> {
    if receipt.segment_acks.len() != delta.segments.len() {
        return Err(CdfError::contract(format!(
            "receipt {} acknowledges {} segment(s) but StateDelta has {} segment(s)",
            receipt.receipt_id,
            receipt.segment_acks.len(),
            delta.segments.len()
        )));
    }

    let acks = receipt
        .segment_acks
        .iter()
        .map(|ack| (&ack.segment_id, ack))
        .collect::<BTreeMap<_, _>>();
    if acks.len() != receipt.segment_acks.len() {
        return Err(CdfError::contract(format!(
            "receipt {} contains duplicate segment acknowledgements",
            receipt.receipt_id
        )));
    }

    for segment in &delta.segments {
        let Some(ack) = acks.get(&segment.segment_id) else {
            return Err(CdfError::contract(format!(
                "receipt {} does not acknowledge segment {}",
                receipt.receipt_id, segment.segment_id
            )));
        };
        if ack.row_count != segment.row_count || ack.byte_count != segment.byte_count {
            return Err(CdfError::contract(format!(
                "receipt {} acknowledges segment {} as {} rows/{} bytes but StateDelta has {} rows/{} bytes",
                receipt.receipt_id,
                segment.segment_id,
                ack.row_count,
                ack.byte_count,
                segment.row_count,
                segment.byte_count
            )));
        }
    }

    Ok(())
}
