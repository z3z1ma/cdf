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
        DataType::Null => "VARCHAR",
        DataType::Boolean => "BOOLEAN",
        DataType::Int8 => "TINYINT",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::UInt8 => "UTINYINT",
        DataType::UInt16 => "USMALLINT",
        DataType::UInt32 => "UINTEGER",
        DataType::UInt64 => "UBIGINT",
        DataType::Float16 => {
            return Err(CdfError::contract(
                "DuckDB's Arrow C importer does not accept Float16; cast the field losslessly to float32 before destination planning",
            ));
        }
        DataType::Float32 => "FLOAT",
        DataType::Float64 => "DOUBLE",
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "VARCHAR",
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "BLOB",
        DataType::FixedSizeBinary(_) => "BLOB",
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
            if *scale >= 0 && *precision <= 38 =>
        {
            return Ok(format!("DECIMAL({precision},{scale})"));
        }
        DataType::Decimal32(_, _) | DataType::Decimal64(_, _) | DataType::Decimal128(_, _) => {
            return Err(CdfError::contract(format!(
                "DuckDB DECIMAL requires precision <= 38 and nonnegative scale; Arrow type is {data_type:?}"
            )));
        }
        DataType::Decimal256(_, _) => {
            return Err(CdfError::contract(
                "DuckDB DECIMAL precision is limited to 38; Decimal256 cannot be preserved losslessly",
            ));
        }
        DataType::Date32 | DataType::Date64 => "DATE",
        DataType::Time32(TimeUnit::Second | TimeUnit::Millisecond)
        | DataType::Time64(TimeUnit::Microsecond) => "TIME",
        DataType::Time64(TimeUnit::Nanosecond) => "TIME_NS",
        DataType::Time32(TimeUnit::Microsecond | TimeUnit::Nanosecond)
        | DataType::Time64(TimeUnit::Second | TimeUnit::Millisecond) => {
            return Err(CdfError::contract(format!(
                "DuckDB TIME cannot losslessly support Arrow type {data_type:?}"
            )));
        }
        DataType::Timestamp(
            TimeUnit::Second | TimeUnit::Millisecond | TimeUnit::Microsecond,
            None,
        ) => "TIMESTAMP",
        DataType::Timestamp(
            TimeUnit::Second | TimeUnit::Millisecond | TimeUnit::Microsecond,
            Some(_),
        ) => "TIMESTAMPTZ",
        DataType::Timestamp(TimeUnit::Nanosecond, None) => "TIMESTAMP_NS",
        DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone)) => {
            return Err(CdfError::contract(format!(
                "DuckDB TIMESTAMPTZ stores microseconds and cannot losslessly persist Arrow nanosecond timestamp timezone {timezone:?}; cast the field to timestamp(microsecond, {timezone}) in the compiled schema or choose a nanosecond-preserving destination"
            )));
        }
        DataType::Duration(_) | DataType::Interval(_) => "INTERVAL",
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => {
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
        DataType::Union(fields, arrow_schema::UnionMode::Sparse) if !fields.is_empty() => {
            let members = fields
                .iter()
                .map(|(_, field)| {
                    validate_ident(field.name())?;
                    Ok(format!(
                        "{} {}",
                        quote_ident(field.name()),
                        duckdb_type(field.data_type())?
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            return Ok(format!("UNION({})", members.join(", ")));
        }
        DataType::Union(_, arrow_schema::UnionMode::Sparse) => {
            return Err(CdfError::contract(
                "DuckDB cannot persist an empty Arrow sparse union",
            ));
        }
        DataType::Union(_, arrow_schema::UnionMode::Dense) => {
            return Err(CdfError::contract(
                "DuckDB's Arrow C importer does not accept dense unions; use sparse union encoding or an allowance-gated JSON projection",
            ));
        }
        DataType::Dictionary(_, value) => return duckdb_type(value),
        other => {
            return Err(CdfError::contract(format!(
                "DuckDB destination does not support Arrow type {other:?}"
            )));
        }
    };
    Ok(ty.to_owned())
}
