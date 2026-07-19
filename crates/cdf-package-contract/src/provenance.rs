use std::{collections::HashMap, sync::Arc};

use arrow_array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use cdf_kernel::{CdfError, Result, SEMANTIC_METADATA_KEY};

use crate::SegmentEntry;

pub const CDF_PACKAGE_ROW_ORD_FIELD: &str = "_cdf_package_row_ord";
pub const CDF_PACKAGE_ROW_ORD_SEMANTIC: &str = "package-row-ord-v1";
pub const CDF_VISIBILITY_METADATA_KEY: &str = "cdf:visibility";
pub const CDF_INTERNAL_VISIBILITY: &str = "internal";

pub fn package_row_ord_field() -> Field {
    Field::new(CDF_PACKAGE_ROW_ORD_FIELD, DataType::UInt64, false).with_metadata(HashMap::from([
        (
            SEMANTIC_METADATA_KEY.to_owned(),
            CDF_PACKAGE_ROW_ORD_SEMANTIC.to_owned(),
        ),
        (
            CDF_VISIBILITY_METADATA_KEY.to_owned(),
            CDF_INTERNAL_VISIBILITY.to_owned(),
        ),
    ]))
}

pub fn is_package_row_ord_field(field: &Field) -> bool {
    field.name() == CDF_PACKAGE_ROW_ORD_FIELD
        && field.data_type() == &DataType::UInt64
        && !field.is_nullable()
        && field
            .metadata()
            .get(SEMANTIC_METADATA_KEY)
            .is_some_and(|value| value == CDF_PACKAGE_ROW_ORD_SEMANTIC)
        && field
            .metadata()
            .get(CDF_VISIBILITY_METADATA_KEY)
            .is_some_and(|value| value == CDF_INTERNAL_VISIBILITY)
}

pub fn validate_logical_output_schema(schema: &Schema) -> Result<()> {
    if schema
        .fields()
        .iter()
        .any(|field| field.name() == CDF_PACKAGE_ROW_ORD_FIELD)
    {
        return Err(CdfError::contract(format!(
            "logical output schema uses reserved framework field {CDF_PACKAGE_ROW_ORD_FIELD:?}"
        )));
    }
    Ok(())
}

pub fn canonical_segment_schema(logical_schema: &Schema) -> Result<Schema> {
    validate_logical_output_schema(logical_schema)?;
    let mut fields = logical_schema.fields().to_vec();
    fields.push(package_row_ord_field().into());
    Ok(Schema::new_with_metadata(
        fields,
        logical_schema.metadata().clone(),
    ))
}

pub fn logical_output_schema(segment_schema: &Schema) -> Result<Schema> {
    let (ordinal, logical) = segment_schema
        .fields()
        .split_last()
        .ok_or_else(|| CdfError::data("canonical segment schema has no fields"))?;
    if !is_package_row_ord_field(ordinal) {
        return Err(CdfError::data(format!(
            "canonical segment schema must end with exact framework field {CDF_PACKAGE_ROW_ORD_FIELD:?}"
        )));
    }
    let schema = Schema::new_with_metadata(logical.to_vec(), segment_schema.metadata().clone());
    validate_logical_output_schema(&schema)?;
    Ok(schema)
}

pub fn validate_segment_ordinal_manifest(segments: &[SegmentEntry]) -> Result<u64> {
    let mut next = 0_u64;
    for segment in segments {
        if segment.row_count == 0 {
            return Err(CdfError::data(format!(
                "canonical segment {} must contain at least one row",
                segment.segment_id
            )));
        }
        if segment.package_row_ord_start != next {
            return Err(CdfError::data(format!(
                "canonical segment {} package row ordinal starts at {} but manifest order requires {next}",
                segment.segment_id, segment.package_row_ord_start
            )));
        }
        next = next
            .checked_add(segment.row_count)
            .ok_or_else(|| CdfError::data("package row ordinal range overflow"))?;
    }
    Ok(next)
}

pub fn append_package_row_ord(batches: Vec<RecordBatch>, start: u64) -> Result<Vec<RecordBatch>> {
    let first = batches
        .first()
        .ok_or_else(|| CdfError::data("canonical segment must contain at least one batch"))?;
    let logical_schema = first.schema();
    let segment_schema = Arc::new(canonical_segment_schema(logical_schema.as_ref())?);
    let mut next = start;
    batches
        .into_iter()
        .map(|batch| {
            if batch.schema().as_ref() != logical_schema.as_ref() {
                return Err(CdfError::data(
                    "canonical segment logical batches must share one schema",
                ));
            }
            let rows = u64::try_from(batch.num_rows())
                .map_err(|_| CdfError::data("canonical segment batch rows exceed u64"))?;
            let end = next
                .checked_add(rows)
                .ok_or_else(|| CdfError::data("package row ordinal overflow"))?;
            let ordinal = Arc::new(UInt64Array::from_iter_values(next..end)) as ArrayRef;
            next = end;
            let mut columns = batch.columns().to_vec();
            columns.push(ordinal);
            RecordBatch::try_new(Arc::clone(&segment_schema), columns).map_err(CdfError::from)
        })
        .collect()
}

pub fn package_row_ord_array(batch: &RecordBatch) -> Result<&UInt64Array> {
    let schema = batch.schema();
    let (index, field) = schema
        .fields()
        .iter()
        .enumerate()
        .next_back()
        .ok_or_else(|| CdfError::data("canonical segment batch has no fields"))?;
    if !is_package_row_ord_field(field) {
        return Err(CdfError::data(format!(
            "canonical segment batch must end with exact framework field {CDF_PACKAGE_ROW_ORD_FIELD:?}"
        )));
    }
    batch
        .column(index)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| CdfError::data("canonical package row ordinal array is not UInt64"))
}

pub fn validate_package_row_ord_batches(
    batches: &[RecordBatch],
    start: u64,
    expected_rows: u64,
) -> Result<()> {
    let first = batches
        .first()
        .ok_or_else(|| CdfError::data("canonical segment must contain at least one batch"))?;
    logical_output_schema(first.schema().as_ref())?;
    let mut next = start;
    for batch in batches {
        if batch.schema().as_ref() != first.schema().as_ref() {
            return Err(CdfError::data(
                "canonical segment batches must share one storage schema",
            ));
        }
        let ordinal = package_row_ord_array(batch)?;
        if ordinal.null_count() != 0 {
            return Err(CdfError::data(
                "canonical package row ordinal values must be non-null",
            ));
        }
        for value in ordinal.values() {
            if *value != next {
                return Err(CdfError::data(format!(
                    "canonical package row ordinal expected {next}, observed {value}"
                )));
            }
            next = next
                .checked_add(1)
                .ok_or_else(|| CdfError::data("package row ordinal overflow"))?;
        }
    }
    let rows = next
        .checked_sub(start)
        .ok_or_else(|| CdfError::internal("package row ordinal moved backwards"))?;
    if rows != expected_rows {
        return Err(CdfError::data(format!(
            "canonical segment ordinal evidence contains {rows} rows but manifest expects {expected_rows}"
        )));
    }
    Ok(())
}

pub fn strip_package_row_ord(batch: RecordBatch) -> Result<RecordBatch> {
    let logical_schema = Arc::new(logical_output_schema(batch.schema().as_ref())?);
    package_row_ord_array(&batch)?;
    let logical_columns = batch
        .columns()
        .get(..batch.num_columns().saturating_sub(1))
        .ok_or_else(|| CdfError::data("canonical segment batch has no logical columns"))?
        .to_vec();
    RecordBatch::try_new(logical_schema, logical_columns).map_err(CdfError::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use cdf_kernel::SegmentId;

    fn segment(id: &str, start: u64, rows: u64) -> SegmentEntry {
        SegmentEntry {
            segment_id: SegmentId::new(id).unwrap(),
            path: format!("data/{id}.arrow"),
            package_row_ord_start: start,
            row_count: rows,
            byte_count: 1,
            sha256: "0".repeat(64),
        }
    }

    fn batch(values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .unwrap()
    }

    #[test]
    fn exact_field_contract_round_trips_logical_schema() {
        let logical = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let canonical = canonical_segment_schema(&logical).unwrap();
        assert!(is_package_row_ord_field(canonical.field(1)));
        assert_eq!(logical_output_schema(&canonical).unwrap(), logical);
    }

    #[test]
    fn classifier_rejects_near_misses() {
        let exact = package_row_ord_field();
        let impostors = [
            Field::new(CDF_PACKAGE_ROW_ORD_FIELD, DataType::UInt64, false),
            Field::new(CDF_PACKAGE_ROW_ORD_FIELD, DataType::Int64, false)
                .with_metadata(exact.metadata().clone()),
            Field::new(CDF_PACKAGE_ROW_ORD_FIELD, DataType::UInt64, true)
                .with_metadata(exact.metadata().clone()),
            Field::new("package_row_ord", DataType::UInt64, false)
                .with_metadata(exact.metadata().clone()),
        ];
        assert!(is_package_row_ord_field(&exact));
        assert!(
            impostors
                .iter()
                .all(|field| !is_package_row_ord_field(field))
        );
    }

    #[test]
    fn manifest_requires_dense_package_ranges() {
        assert_eq!(
            validate_segment_ordinal_manifest(&[segment("seg-0", 0, 2), segment("seg-1", 2, 3)])
                .unwrap(),
            5
        );
        assert!(
            validate_segment_ordinal_manifest(&[segment("seg-0", 0, 2), segment("seg-1", 3, 3)])
                .is_err()
        );
    }

    #[test]
    fn assignment_is_dense_across_batch_boundaries_and_strippable() {
        let batches = append_package_row_ord(vec![batch(&[1, 2]), batch(&[3])], 7).unwrap();
        assert_eq!(
            package_row_ord_array(&batches[0]).unwrap().values(),
            &[7, 8]
        );
        assert_eq!(package_row_ord_array(&batches[1]).unwrap().values(), &[9]);
        validate_package_row_ord_batches(&batches, 7, 3).unwrap();
        let logical = strip_package_row_ord(batches[0].clone()).unwrap();
        assert_eq!(logical.num_columns(), 1);
        assert_eq!(logical.schema().field(0).name(), "id");
    }

    #[test]
    fn validation_rejects_tampered_values() {
        let mut batches = append_package_row_ord(vec![batch(&[1, 2])], 0).unwrap();
        let mut columns = batches[0].columns().to_vec();
        columns[1] = Arc::new(UInt64Array::from(vec![0, 2]));
        batches[0] = RecordBatch::try_new(batches[0].schema(), columns).unwrap();
        assert!(validate_package_row_ord_batches(&batches, 0, 2).is_err());
    }
}
