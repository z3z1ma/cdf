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
use cdf_package_contract::{LateDataPayloadLocation, LateDataRecord};

pub(crate) struct LateDataClassification {
    pub(crate) admitted: RecordBatch,
    pub(crate) recaptured: Option<RecordBatch>,
    pub(crate) quarantined: Option<RecordBatch>,
    pub(crate) records: Vec<LateDataRecord>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn classify_late_data(
    batch: RecordBatch,
    source_rows: &[usize],
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

    let (late, records) = classify_array(
        values.as_ref(),
        &watermark.domain,
        &watermark.value,
        source_rows,
        watermark,
        action,
        partition_id,
        source_position,
        source_row_base,
    )?;

    if records.is_empty() || action == LateDataAction::AdmitWithAnnotation {
        return Ok(LateDataClassification {
            admitted: batch,
            recaptured: None,
            quarantined: None,
            records,
        });
    }

    let admitted_mask = BooleanArray::from(late.iter().map(|late| !late).collect::<Vec<_>>());
    let admitted = filter_record_batch(&batch, &admitted_mask).map_err(CdfError::from)?;
    let late_mask = BooleanArray::from(late);
    let withheld = filter_record_batch(&batch, &late_mask).map_err(CdfError::from)?;
    let (recaptured, quarantined) = match action {
        LateDataAction::RecaptureNextEpoch => (Some(withheld), None),
        LateDataAction::Quarantine => (None, Some(withheld)),
        LateDataAction::AdmitWithAnnotation => unreachable!("annotation returned above"),
    };
    Ok(LateDataClassification {
        admitted,
        recaptured,
        quarantined,
        records,
    })
}

#[allow(clippy::too_many_arguments)]
fn classify_array(
    array: &dyn Array,
    domain: &EventTimeDomain,
    watermark_value: &WatermarkValue,
    source_rows: &[usize],
    watermark: &WatermarkClaim,
    action: LateDataAction,
    partition_id: &PartitionId,
    source_position: Option<&SourcePosition>,
    source_row_base: u64,
) -> Result<(Vec<bool>, Vec<LateDataRecord>)> {
    macro_rules! classify {
        ($array:ty, $threshold:expr, $map:expr, $variant:path) => {{
            let values = downcast::<$array>(array)?;
            classify_ordered_values(
                values.iter().map(|value| Ok(value.map($map))),
                $threshold,
                source_rows,
                watermark,
                action,
                partition_id,
                source_position,
                source_row_base,
                $variant,
            )
        }};
    }

    match (domain, watermark_value) {
        (EventTimeDomain::SignedInteger, WatermarkValue::Signed(threshold)) => {
            if array.as_any().is::<Int8Array>() {
                classify!(Int8Array, *threshold, i64::from, WatermarkValue::Signed)
            } else if array.as_any().is::<Int16Array>() {
                classify!(Int16Array, *threshold, i64::from, WatermarkValue::Signed)
            } else if array.as_any().is::<Int32Array>() {
                classify!(Int32Array, *threshold, i64::from, WatermarkValue::Signed)
            } else {
                classify!(
                    Int64Array,
                    *threshold,
                    |value| value,
                    WatermarkValue::Signed
                )
            }
        }
        (EventTimeDomain::UnsignedInteger, WatermarkValue::Unsigned(threshold)) => {
            if array.as_any().is::<UInt8Array>() {
                classify!(UInt8Array, *threshold, u64::from, WatermarkValue::Unsigned)
            } else if array.as_any().is::<UInt16Array>() {
                classify!(UInt16Array, *threshold, u64::from, WatermarkValue::Unsigned)
            } else if array.as_any().is::<UInt32Array>() {
                classify!(UInt32Array, *threshold, u64::from, WatermarkValue::Unsigned)
            } else {
                classify!(
                    UInt64Array,
                    *threshold,
                    |value| value,
                    WatermarkValue::Unsigned
                )
            }
        }
        (EventTimeDomain::Decimal { .. }, WatermarkValue::Decimal(threshold)) => {
            if let Some(values) = array.as_any().downcast_ref::<Decimal128Array>() {
                classify_ordered_values(
                    values.iter().map(Ok),
                    *threshold,
                    source_rows,
                    watermark,
                    action,
                    partition_id,
                    source_position,
                    source_row_base,
                    WatermarkValue::Decimal,
                )
            } else {
                let values = downcast::<Decimal256Array>(array)?;
                classify_ordered_values(
                    values.iter().map(|value| {
                        value
                            .map(|value| {
                                let bytes = value.to_le_bytes();
                                let sign = if bytes[31] & 0x80 == 0 { 0 } else { u8::MAX };
                                if bytes[16..].iter().any(|byte| *byte != sign)
                                    || (bytes[15] & 0x80 == 0) != (sign == 0)
                                {
                                    return Err(CdfError::data(
                                        "decimal256 watermark value exceeds the governed decimal(38) domain",
                                    ));
                                }
                                let mut narrowed = [0_u8; 16];
                                narrowed.copy_from_slice(&bytes[..16]);
                                Ok(i128::from_le_bytes(narrowed))
                            })
                            .transpose()
                    }),
                    *threshold,
                    source_rows,
                    watermark,
                    action,
                    partition_id,
                    source_position,
                    source_row_base,
                    WatermarkValue::Decimal,
                )
            }
        }
        (EventTimeDomain::Date32, WatermarkValue::Date32(threshold)) => {
            classify!(
                Date32Array,
                *threshold,
                |value| value,
                WatermarkValue::Date32
            )
        }
        (EventTimeDomain::Date64, WatermarkValue::Date64(threshold)) => {
            classify!(
                Date64Array,
                *threshold,
                |value| value,
                WatermarkValue::Date64
            )
        }
        (EventTimeDomain::Timestamp { .. }, WatermarkValue::Timestamp(threshold)) => {
            if array.as_any().is::<TimestampSecondArray>() {
                classify!(
                    TimestampSecondArray,
                    *threshold,
                    |value| value,
                    WatermarkValue::Timestamp
                )
            } else if array.as_any().is::<TimestampMillisecondArray>() {
                classify!(
                    TimestampMillisecondArray,
                    *threshold,
                    |value| value,
                    WatermarkValue::Timestamp
                )
            } else if array.as_any().is::<TimestampMicrosecondArray>() {
                classify!(
                    TimestampMicrosecondArray,
                    *threshold,
                    |value| value,
                    WatermarkValue::Timestamp
                )
            } else {
                classify!(
                    TimestampNanosecondArray,
                    *threshold,
                    |value| value,
                    WatermarkValue::Timestamp
                )
            }
        }
        _ => Err(CdfError::data(
            "watermark value kind does not match its compiled event-time domain",
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn classify_ordered_values<I, T, F>(
    values: I,
    threshold: T,
    source_rows: &[usize],
    watermark: &WatermarkClaim,
    action: LateDataAction,
    partition_id: &PartitionId,
    source_position: Option<&SourcePosition>,
    source_row_base: u64,
    to_watermark: F,
) -> Result<(Vec<bool>, Vec<LateDataRecord>)>
where
    I: Iterator<Item = Result<Option<T>>>,
    T: Copy + Ord,
    F: Fn(T) -> WatermarkValue,
{
    let mut late = Vec::with_capacity(source_rows.len());
    let mut records = Vec::new();
    for (row, value) in values.enumerate() {
        let value = value?;
        let is_late = value.is_some_and(|value| value < threshold);
        late.push(is_late);
        if is_late {
            let source_row = source_rows.get(row).copied().ok_or_else(|| {
                CdfError::internal("late-data value count exceeds source-row tracking")
            })?;
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
                event_time: to_watermark(value.expect("late values are non-null")),
                effective_watermark: watermark.clone(),
                action,
                payload: LateDataPayloadLocation::AdmittedOutput,
            });
        }
    }
    if late.len() != source_rows.len() {
        return Err(CdfError::internal(
            "late-data value count differs from source-row tracking",
        ));
    }
    Ok((late, records))
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
                &[0, 1, 2, 3],
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
                    assert_eq!(result.quarantined.unwrap().num_rows(), 1);
                }
                LateDataAction::RecaptureNextEpoch => {
                    assert_eq!(result.recaptured.unwrap().num_rows(), 1);
                    assert!(result.quarantined.is_none());
                }
                LateDataAction::AdmitWithAnnotation => {
                    assert!(result.recaptured.is_none());
                    assert!(result.quarantined.is_none());
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
