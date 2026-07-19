use crate::*;
use crate::{api::*, sql::*};

pub(crate) fn persistence_fields(user_fields: &[FieldPlan]) -> Vec<FieldPlan> {
    let mut fields = user_fields.to_vec();
    fields.push(FieldPlan {
        name: CDF_ROW_KEY_COLUMN.to_owned(),
        sql_type: "UBIGINT".to_owned(),
        nullable: false,
    });
    fields
}

pub(crate) fn persistence_batch(
    batch: RecordBatch,
    package_row_key_start: u64,
    include_stage_order: bool,
) -> Result<RecordBatch> {
    let package_row_ord = cdf_package_contract::package_row_ord_array(&batch)?.clone();
    let batch = cdf_package_contract::strip_package_row_ord(batch)?;
    let row_keys = package_row_ord
        .values()
        .iter()
        .map(|ordinal| {
            package_row_key_start
                .checked_add(*ordinal)
                .ok_or_else(|| CdfError::data("DuckDB row provenance key overflowed"))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Arc::new(Field::new(
        CDF_ROW_KEY_COLUMN,
        DataType::UInt64,
        false,
    )));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(UInt64Array::from(row_keys)));
    if include_stage_order {
        fields.push(Arc::new(Field::new(
            CDF_STAGE_ORDER_COLUMN,
            DataType::UInt64,
            false,
        )));
        columns.push(Arc::new(package_row_ord));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|error| CdfError::data(format!("build DuckDB persistence batch: {error}")))
}

pub(crate) fn validate_field_names(fields: &[FieldPlan]) -> Result<()> {
    let mut seen = BTreeSet::new();
    for field in fields {
        validate_ident(&field.name)?;
        if !seen.insert(field.name.clone()) {
            return Err(CdfError::contract(format!(
                "duplicate destination column name {}",
                field.name
            )));
        }
    }
    Ok(())
}

pub(crate) fn validate_user_schema_fields(schema: &Schema) -> Result<()> {
    for field in schema.fields() {
        if field.name().starts_with("_cdf_") && !is_framework_variant_field(field.as_ref()) {
            return Err(CdfError::contract(format!(
                "DuckDB destination column {:?} uses the reserved `_cdf_*` namespace; rename the user field before planning",
                field.name()
            )));
        }
    }
    Ok(())
}

pub(crate) fn field_plan(field: &Field) -> Result<FieldPlan> {
    Ok(FieldPlan {
        name: field.name().clone(),
        sql_type: duckdb_type(field.data_type())?,
        nullable: field.is_nullable(),
    })
}

pub(crate) fn duckdb_type(data_type: &DataType) -> Result<String> {
    let ty = match data_type {
        DataType::Boolean => "BOOLEAN",
        DataType::Int8 => "TINYINT",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::UInt8 => "UTINYINT",
        DataType::UInt16 => "USMALLINT",
        DataType::UInt32 => "UINTEGER",
        DataType::UInt64 => "UBIGINT",
        DataType::Float32 => "FLOAT",
        DataType::Float64 => "DOUBLE",
        DataType::Utf8 | DataType::LargeUtf8 => "VARCHAR",
        DataType::Binary | DataType::LargeBinary => "BLOB",
        DataType::FixedSizeBinary(_) => "BLOB",
        DataType::Decimal128(precision, scale) if *scale >= 0 && *precision <= 38 => {
            return Ok(format!("DECIMAL({precision},{scale})"));
        }
        DataType::Decimal128(_, _) => {
            return Err(CdfError::contract(format!(
                "DuckDB DECIMAL requires precision <= 38 and nonnegative scale; Arrow type is {data_type:?}"
            )));
        }
        DataType::Decimal256(_, _) => {
            return Err(CdfError::contract(
                "DuckDB's pinned Arrow appender maps Decimal256 through DOUBLE and cannot preserve it losslessly",
            ));
        }
        DataType::Date32 => "DATE",
        DataType::Time32(TimeUnit::Second | TimeUnit::Millisecond)
        | DataType::Time64(TimeUnit::Microsecond) => "TIME",
        DataType::Time32(TimeUnit::Microsecond | TimeUnit::Nanosecond)
        | DataType::Time64(TimeUnit::Second | TimeUnit::Millisecond | TimeUnit::Nanosecond) => {
            return Err(CdfError::contract(format!(
                "DuckDB TIME cannot losslessly support Arrow type {data_type:?}"
            )));
        }
        DataType::Timestamp(
            TimeUnit::Second | TimeUnit::Millisecond | TimeUnit::Microsecond,
            None,
        ) => "TIMESTAMP",
        DataType::Timestamp(_, Some(_)) => {
            return Err(CdfError::contract(
                "DuckDB timezone-aware timestamp commits require a ratified ICU-enabled path",
            ));
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            return Err(CdfError::contract(
                "DuckDB timestamp nanosecond commits would lose precision",
            ));
        }
        DataType::List(field) | DataType::LargeList(field) => {
            return Ok(format!("{}[]", duckdb_type(field.data_type())?));
        }
        DataType::FixedSizeList(field, size) if *size > 0 => {
            return Ok(format!("{}[{size}]", duckdb_type(field.data_type())?));
        }
        DataType::FixedSizeList(_, _) => {
            return Err(CdfError::contract(
                "DuckDB fixed-size Arrow lists require a positive element count",
            ));
        }
        DataType::Struct(fields) if !fields.is_empty() => {
            let fields = fields
                .iter()
                .map(|field| {
                    validate_ident(field.name())?;
                    Ok(format!(
                        "{} {}",
                        quote_ident(field.name()),
                        duckdb_type(field.data_type())?
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            return Ok(format!("STRUCT({})", fields.join(", ")));
        }
        DataType::Struct(_) => {
            return Err(CdfError::contract(
                "DuckDB cannot persist an empty Arrow struct",
            ));
        }
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(CdfError::contract(
                    "Arrow map entries must be a struct<key,value> for DuckDB",
                ));
            };
            if fields.len() != 2 {
                return Err(CdfError::contract(
                    "Arrow map entries must contain exactly key and value fields for DuckDB",
                ));
            }
            return Ok(format!(
                "MAP({}, {})",
                duckdb_type(fields[0].data_type())?,
                duckdb_type(fields[1].data_type())?
            ));
        }
        other => {
            return Err(CdfError::contract(format!(
                "DuckDB destination does not support Arrow type {other:?}"
            )));
        }
    };
    Ok(ty.to_owned())
}

#[cfg(test)]
mod tests {
    use arrow_array::{Array, Int64Array};

    use super::*;

    #[test]
    fn persistence_derives_row_key_and_merge_order_from_canonical_ordinal() {
        let logical = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2]))],
        )
        .unwrap();
        let canonical = cdf_package_contract::append_package_row_ord(vec![logical], 7)
            .unwrap()
            .pop()
            .unwrap();

        let persisted = persistence_batch(canonical, 100, true).unwrap();

        assert!(
            persisted
                .schema()
                .field_with_name(cdf_package_contract::CDF_PACKAGE_ROW_ORD_FIELD)
                .is_err()
        );
        let row_keys = persisted
            .column_by_name(CDF_ROW_KEY_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let stage_order = persisted
            .column_by_name(CDF_STAGE_ORDER_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(row_keys.values(), &[107, 108]);
        assert_eq!(stage_order.values(), &[7, 8]);
        assert_eq!(stage_order.null_count(), 0);
    }
}
