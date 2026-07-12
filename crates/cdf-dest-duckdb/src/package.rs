use crate::*;
use crate::{api::*, sql::*};

pub(crate) fn persistence_fields(user_fields: &[FieldPlan]) -> Vec<FieldPlan> {
    let mut fields = user_fields.to_vec();
    fields.extend([
        FieldPlan {
            name: CDF_LOAD_COLUMN.to_owned(),
            sql_type: "VARCHAR".to_owned(),
            nullable: false,
        },
        FieldPlan {
            name: CDF_SEGMENT_COLUMN.to_owned(),
            sql_type: "VARCHAR".to_owned(),
            nullable: false,
        },
        FieldPlan {
            name: CDF_ROW_COLUMN.to_owned(),
            sql_type: "UBIGINT".to_owned(),
            nullable: false,
        },
    ]);
    fields
}

pub(crate) fn ingress_batch(
    batch: RecordBatch,
    segment_row_start: u64,
    stage_row_start: Option<u64>,
) -> Result<RecordBatch> {
    let row_count = u64::try_from(batch.num_rows())
        .map_err(|_| CdfError::data("DuckDB Arrow batch row count exceeds u64"))?;
    let segment_row_end = segment_row_start
        .checked_add(row_count)
        .ok_or_else(|| CdfError::data("DuckDB segment row ordinal overflowed"))?;
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Arc::new(Field::new(
        CDF_ROW_COLUMN,
        DataType::UInt64,
        false,
    )));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(UInt64Array::from_iter_values(
        segment_row_start..segment_row_end,
    )));
    if let Some(stage_row_start) = stage_row_start {
        let stage_row_end = stage_row_start
            .checked_add(row_count)
            .ok_or_else(|| CdfError::data("DuckDB stage row ordinal overflowed"))?;
        fields.push(Arc::new(Field::new(
            CDF_STAGE_ORDER_COLUMN,
            DataType::UInt64,
            false,
        )));
        columns.push(Arc::new(UInt64Array::from_iter_values(
            stage_row_start..stage_row_end,
        )));
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
        other => {
            return Err(CdfError::contract(format!(
                "DuckDB destination does not support Arrow type {other:?}"
            )));
        }
    };
    Ok(ty.to_owned())
}
