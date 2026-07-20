use arrow_array::{
    Array, BooleanArray, Date32Array, Date64Array, Decimal128Array, Decimal256Array, Int8Array,
    Int16Array, Int32Array, Int64Array, RecordBatch, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_select::filter::filter_record_batch;
use cdf_kernel::{
    CdfError, EventTimeDomain, LateDataAction, PartitionId, Result, SourcePosition, WatermarkClaim,
    WatermarkValue,
};
use cdf_package_contract::LateDataRecord;

pub(crate) struct LateDataClassification {
    pub(crate) admitted: RecordBatch,
    pub(crate) admitted_source_rows: Vec<usize>,
    pub(crate) recaptured: Option<RecordBatch>,
    pub(crate) records: Vec<LateDataRecord>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn classify_late_data(
    batch: RecordBatch,
    source_rows: Vec<usize>,
    field_name: &str,
    watermark: &WatermarkClaim,
    action: LateDataAction,
    partition_id: &PartitionId,
    source_position: Option<&SourcePosition>,
    source_row_base: u64,
) -> Result<LateDataClassification> {
    if source_rows.len() != batch.num_rows() {
        return Err(CdfError::internal(
            "late-data admission requires one source ordinal per transformed row",
        ));
    }
    if watermark.event_time_field.as_ref() != field_name {
        return Err(CdfError::internal(
            "late-data admission field differs from its watermark authority",
        ));
    }
    let field_index = batch.schema().index_of(field_name).map_err(|_| {
        CdfError::data(format!(
            "watermark field `{field_name}` is absent after a watermark-preserving operator; declare that operator as drop/transform or retain the field"
        ))
    })?;
    let values = batch.column(field_index);
    if !watermark.domain.matches_arrow_type(values.data_type()) {
        return Err(CdfError::data(format!(
            "watermark field `{field_name}` has Arrow type {} but the compiled event-time domain is {:?}",
            values.data_type(),
            watermark.domain
        )));
    }

    let mut late = Vec::with_capacity(batch.num_rows());
    let mut records = Vec::new();
    for (row, source_row) in source_rows.iter().copied().enumerate() {
        let event_time = event_time_at(values.as_ref(), &watermark.domain, row)?;
        let is_late = event_time
            .as_ref()
            .is_some_and(|event_time| watermark_value_lt(event_time, &watermark.value));
        late.push(is_late);
        if let Some(event_time) = event_time.filter(|_| is_late) {
            records.push(LateDataRecord {
                source_row_ordinal: source_row_base
                    .checked_add(
                        u64::try_from(source_row).map_err(|_| {
                            CdfError::data("late-data source row ordinal exceeds u64")
                        })?,
                    )
                    .ok_or_else(|| CdfError::data("late-data source row ordinal overflow"))?,
                partition_id: partition_id.clone(),
                source_position: source_position.cloned(),
                event_time,
                effective_watermark: watermark.clone(),
                action,
            });
        }
    }

    if records.is_empty() || action == LateDataAction::AdmitWithAnnotation {
        return Ok(LateDataClassification {
            admitted: batch,
            admitted_source_rows: source_rows,
            recaptured: None,
            records,
        });
    }

    let admitted_mask = BooleanArray::from(late.iter().map(|late| !late).collect::<Vec<_>>());
    let admitted = filter_record_batch(&batch, &admitted_mask).map_err(CdfError::from)?;
    let admitted_source_rows = source_rows
        .iter()
        .copied()
        .zip(&late)
        .filter_map(|(row, late)| (!late).then_some(row))
        .collect();
    let recaptured = if action == LateDataAction::RecaptureNextEpoch {
        let late_mask = BooleanArray::from(late);
        Some(filter_record_batch(&batch, &late_mask).map_err(CdfError::from)?)
    } else {
        None
    };
    Ok(LateDataClassification {
        admitted,
        admitted_source_rows,
        recaptured,
        records,
    })
}

fn event_time_at(
    array: &dyn Array,
    domain: &EventTimeDomain,
    row: usize,
) -> Result<Option<WatermarkValue>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let value = match domain {
        EventTimeDomain::SignedInteger => signed_value(array, row)?,
        EventTimeDomain::UnsignedInteger => unsigned_value(array, row)?,
        EventTimeDomain::Decimal { .. } => decimal_value(array, row)?,
        EventTimeDomain::Date32 => {
            WatermarkValue::Date32(downcast::<Date32Array>(array)?.value(row))
        }
        EventTimeDomain::Date64 => {
            WatermarkValue::Date64(downcast::<Date64Array>(array)?.value(row))
        }
        EventTimeDomain::Timestamp { .. } => timestamp_value(array, row)?,
    };
    Ok(Some(value))
}

fn signed_value(array: &dyn Array, row: usize) -> Result<WatermarkValue> {
    macro_rules! signed {
        ($ty:ty) => {
            if let Some(array) = array.as_any().downcast_ref::<$ty>() {
                return Ok(WatermarkValue::Signed(i64::from(array.value(row))));
            }
        };
    }
    signed!(Int8Array);
    signed!(Int16Array);
    signed!(Int32Array);
    if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(WatermarkValue::Signed(array.value(row)));
    }
    Err(type_mismatch(array, "signed integer"))
}

fn unsigned_value(array: &dyn Array, row: usize) -> Result<WatermarkValue> {
    macro_rules! unsigned {
        ($ty:ty) => {
            if let Some(array) = array.as_any().downcast_ref::<$ty>() {
                return Ok(WatermarkValue::Unsigned(u64::from(array.value(row))));
            }
        };
    }
    unsigned!(UInt8Array);
    unsigned!(UInt16Array);
    unsigned!(UInt32Array);
    if let Some(array) = array.as_any().downcast_ref::<UInt64Array>() {
        return Ok(WatermarkValue::Unsigned(array.value(row)));
    }
    Err(type_mismatch(array, "unsigned integer"))
}

fn decimal_value(array: &dyn Array, row: usize) -> Result<WatermarkValue> {
    if let Some(array) = array.as_any().downcast_ref::<Decimal128Array>() {
        return Ok(WatermarkValue::Decimal(array.value(row)));
    }
    if let Some(array) = array.as_any().downcast_ref::<Decimal256Array>() {
        let bytes = array.value(row).to_le_bytes();
        let sign = if bytes[31] & 0x80 == 0 { 0 } else { u8::MAX };
        if bytes[16..].iter().any(|byte| *byte != sign) || (bytes[15] & 0x80 == 0) != (sign == 0) {
            return Err(CdfError::data(
                "decimal256 watermark value exceeds the governed decimal(38) domain",
            ));
        }
        let mut narrowed = [0_u8; 16];
        narrowed.copy_from_slice(&bytes[..16]);
        return Ok(WatermarkValue::Decimal(i128::from_le_bytes(narrowed)));
    }
    Err(type_mismatch(array, "decimal"))
}

fn timestamp_value(array: &dyn Array, row: usize) -> Result<WatermarkValue> {
    macro_rules! timestamp {
        ($ty:ty) => {
            if let Some(array) = array.as_any().downcast_ref::<$ty>() {
                return Ok(WatermarkValue::Timestamp(array.value(row)));
            }
        };
    }
    timestamp!(TimestampSecondArray);
    timestamp!(TimestampMillisecondArray);
    timestamp!(TimestampMicrosecondArray);
    timestamp!(TimestampNanosecondArray);
    Err(type_mismatch(array, "timestamp"))
}

fn watermark_value_lt(left: &WatermarkValue, right: &WatermarkValue) -> bool {
    match (left, right) {
        (WatermarkValue::Signed(left), WatermarkValue::Signed(right)) => left < right,
        (WatermarkValue::Unsigned(left), WatermarkValue::Unsigned(right)) => left < right,
        (WatermarkValue::Decimal(left), WatermarkValue::Decimal(right)) => left < right,
        (WatermarkValue::Date32(left), WatermarkValue::Date32(right)) => left < right,
        (WatermarkValue::Date64(left), WatermarkValue::Date64(right))
        | (WatermarkValue::Timestamp(left), WatermarkValue::Timestamp(right)) => left < right,
        _ => false,
    }
}

fn downcast<T: Array + 'static>(array: &dyn Array) -> Result<&T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| type_mismatch(array, std::any::type_name::<T>()))
}

fn type_mismatch(array: &dyn Array, expected: &str) -> CdfError {
    CdfError::data(format!(
        "watermark event-time array {} cannot be read as {expected}",
        array.data_type()
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use cdf_kernel::{
        CursorPosition, CursorValue, EventTimeDomain, PartitionId, SOURCE_POSITION_VERSION,
        STREAM_EPOCH_POLICY_VERSION, SourcePosition, WATERMARK_CLAIM_VERSION, WatermarkAuthority,
        WatermarkObservationContext,
    };

    use super::*;

    #[test]
    fn all_three_actions_partition_rows_without_loss() {
        for action in [
            LateDataAction::Quarantine,
            LateDataAction::RecaptureNextEpoch,
            LateDataAction::AdmitWithAnnotation,
        ] {
            let result = classify_late_data(
                batch(),
                vec![0, 1, 2, 3],
                "occurred_at",
                &watermark(20),
                action,
                &PartitionId::new("p0").unwrap(),
                Some(&position(4)),
                10,
            )
            .unwrap();
            assert_eq!(result.records.len(), 1);
            assert_eq!(result.records[0].source_row_ordinal, 10);
            assert_eq!(
                result.admitted.num_rows()
                    + result.recaptured.as_ref().map_or(0, RecordBatch::num_rows),
                if action == LateDataAction::Quarantine {
                    3
                } else {
                    4
                }
            );
            match action {
                LateDataAction::Quarantine => {
                    assert!(result.recaptured.is_none());
                    assert_eq!(result.admitted_source_rows, vec![1, 2, 3]);
                }
                LateDataAction::RecaptureNextEpoch => {
                    assert_eq!(result.recaptured.unwrap().num_rows(), 1);
                    assert_eq!(result.admitted_source_rows, vec![1, 2, 3]);
                }
                LateDataAction::AdmitWithAnnotation => {
                    assert!(result.recaptured.is_none());
                    assert_eq!(result.admitted_source_rows, vec![0, 1, 2, 3]);
                }
            }
        }
    }

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("occurred_at", DataType::Int64, true),
            Field::new("payload", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(10), Some(20), None, Some(30)])) as ArrayRef,
                Arc::new(StringArray::from(vec!["late", "edge", "null", "fresh"])) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn watermark(value: i64) -> WatermarkClaim {
        WatermarkClaim {
            version: WATERMARK_CLAIM_VERSION,
            policy_version: STREAM_EPOCH_POLICY_VERSION,
            event_time_field: "occurred_at".into(),
            domain: EventTimeDomain::SignedInteger,
            value: WatermarkValue::Signed(value),
            partition_id: PartitionId::new("p0").unwrap(),
            source_position: position(u64::try_from(value).unwrap()),
            authority: WatermarkAuthority::Source,
            observation_context: WatermarkObservationContext::EpochBarrier,
        }
    }

    fn position(value: u64) -> SourcePosition {
        SourcePosition::Cursor(CursorPosition {
            version: SOURCE_POSITION_VERSION,
            field: "offset".to_owned(),
            value: CursorValue::U64(value),
        })
    }
}
