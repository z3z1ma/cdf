use crate::*;
use crate::{api::*, rows::*, sql::*};

pub(crate) fn load_package_data(package_dir: &Path) -> Result<PackageData> {
    let reader = PackageReader::open(package_dir)?;
    reader.verify()?;
    let segments = reader.read_all_segments()?;
    package_data_from_segments(segments, false)
}

pub(crate) fn package_data_from_commit_segments(
    segments: Vec<CommitSegment>,
) -> Result<PackageData> {
    let segments = segments
        .into_iter()
        .map(|segment| {
            (
                SegmentEntry {
                    segment_id: segment.state.segment_id,
                    path: String::new(),
                    row_count: segment.state.row_count,
                    byte_count: segment.package_byte_count,
                    sha256: String::new(),
                },
                segment.batches,
            )
        })
        .collect::<Vec<_>>();
    package_data_from_segments(segments, true)
}

fn package_data_from_segments(
    segments: Vec<(SegmentEntry, Vec<RecordBatch>)>,
    validate_entry_rows: bool,
) -> Result<PackageData> {
    if segments.is_empty() {
        return Err(CdfError::data(
            "DuckDB destination requires at least one package segment",
        ));
    }

    let schema = first_schema(&segments)?;
    validate_user_schema_fields(schema.as_ref())?;
    let fields = schema
        .fields()
        .iter()
        .map(|field| field_plan(field.as_ref()))
        .collect::<Result<Vec<_>>>()?;
    validate_field_names(&fields)?;

    let mut loaded_segments = Vec::new();
    let mut rows = Vec::new();
    for (entry, batches) in segments {
        let mut row_count = 0_u64;
        for batch in batches {
            if batch.schema().as_ref() != schema.as_ref() {
                return Err(CdfError::data(
                    "DuckDB destination requires all package segments to share one schema",
                ));
            }
            row_count += batch.num_rows() as u64;
            rows.extend(batch_rows(&batch)?);
        }
        if validate_entry_rows && row_count != entry.row_count {
            return Err(CdfError::data(format!(
                "DuckDB commit segment {} has {} payload rows but segment metadata has {}",
                entry.segment_id.as_str(),
                row_count,
                entry.row_count
            )));
        }
        loaded_segments.push(LoadedSegment { entry, row_count });
    }

    Ok(PackageData {
        fields,
        segments: loaded_segments,
        rows,
    })
}

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

pub(crate) fn persistence_rows(
    package: &PackageData,
    package_hash: &cdf_kernel::PackageHash,
) -> Result<Vec<RowValues>> {
    let expected_rows = package
        .segments
        .iter()
        .try_fold(0_u64, |total, segment| total.checked_add(segment.row_count))
        .ok_or_else(|| CdfError::data("DuckDB package row count overflowed"))?;
    if expected_rows != package.rows.len() as u64 {
        return Err(CdfError::data(format!(
            "DuckDB package segment rows total {expected_rows} but decoded payload has {} rows",
            package.rows.len()
        )));
    }

    let mut rows = Vec::with_capacity(package.rows.len());
    let mut offset = 0_usize;
    for segment in &package.segments {
        for ordinal in 0..segment.row_count {
            let mut row = package.rows[offset].clone();
            row.push(text_cell(package_hash.as_str()));
            row.push(text_cell(segment.entry.segment_id.as_str()));
            row.push(u64_cell(ordinal));
            rows.push(row);
            offset += 1;
        }
    }
    Ok(rows)
}

fn text_cell(value: &str) -> CellValue {
    CellValue {
        value: Value::Text(value.to_owned()),
        key: CellKey::Text(value.to_owned()),
    }
}

fn u64_cell(value: u64) -> CellValue {
    CellValue {
        value: Value::UBigInt(value),
        key: CellKey::U64(value),
    }
}

pub(crate) fn first_schema(segments: &[(SegmentEntry, Vec<RecordBatch>)]) -> Result<SchemaRef> {
    segments
        .iter()
        .flat_map(|(_, batches)| batches.iter())
        .next()
        .map(RecordBatch::schema)
        .ok_or_else(|| CdfError::data("DuckDB destination found no record batches in package"))
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
