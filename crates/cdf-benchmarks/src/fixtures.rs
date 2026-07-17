use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use arrow_select::filter::filter_record_batch;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

use crate::{BenchResult, bench_error, catalog::FixtureSpec, matrix::LocalFormat};

pub const FIXTURE_GENERATOR_VERSION: &str = "fixture-orders-generator-v1";

pub fn write_all_local_fixture_formats(
    root: &Path,
    spec: &FixtureSpec,
) -> BenchResult<BTreeMap<String, Vec<u8>>> {
    fs::create_dir_all(root)?;
    let mut files = BTreeMap::new();
    for format in LocalFormat::all() {
        let path = write_local_fixture_file(root, spec, format)?;
        files.insert(format.label().to_owned(), fs::read(path)?);
    }
    let ipc_path = root.join("orders.arrow");
    fs::write(&ipc_path, arrow_ipc_stream_bytes(spec)?)?;
    files.insert("arrow_ipc_stream".to_owned(), fs::read(ipc_path)?);
    Ok(files)
}

pub(crate) fn record_batches_for_spec(spec: &FixtureSpec) -> BenchResult<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    let mut start = 0;
    while start < spec.rows {
        let len = spec.batch_size.min(spec.rows - start);
        batches.push(record_batch_range(spec, start, len)?);
        start += len;
    }
    Ok(batches)
}

pub(crate) fn record_batch_range(
    spec: &FixtureSpec,
    start: usize,
    len: usize,
) -> BenchResult<RecordBatch> {
    let schema = Arc::new(schema_for_spec(spec));
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(4 + spec.wide_columns);
    let ids = (start..start + len)
        .map(|row| row as i64)
        .collect::<Vec<_>>();
    columns.push(Arc::new(Int64Array::from(ids.clone())) as ArrayRef);
    columns.push(Arc::new(BooleanArray::from(
        ids.iter().map(|id| active_for_id(*id)).collect::<Vec<_>>(),
    )) as ArrayRef);
    columns.push(Arc::new(StringArray::from(
        ids.iter()
            .map(|id| category_for_id(*id))
            .collect::<Vec<_>>(),
    )) as ArrayRef);
    columns.push(Arc::new(Float64Array::from(
        ids.iter().map(|id| amount_for_id(*id)).collect::<Vec<_>>(),
    )) as ArrayRef);
    for column in 0..spec.wide_columns {
        columns.push(Arc::new(Int64Array::from(
            ids.iter()
                .map(|id| metric_for_id(*id, column))
                .collect::<Vec<_>>(),
        )) as ArrayRef);
    }
    Ok(RecordBatch::try_new(schema, columns)?)
}

pub(crate) fn schema_for_spec(spec: &FixtureSpec) -> Schema {
    let mut fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("active", DataType::Boolean, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
    ];
    for column in 0..spec.wide_columns {
        fields.push(Field::new(
            format!("metric_{column:03}"),
            DataType::Int64,
            false,
        ));
    }
    Schema::new(fields)
}

pub(crate) fn arrow_filter_project(batch: &RecordBatch) -> BenchResult<RecordBatch> {
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| bench_error("id column was not Int64"))?;
    let active = batch
        .column(1)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| bench_error("active column was not Boolean"))?;
    let mask = BooleanArray::from(
        (0..batch.num_rows())
            .map(|row| active.value(row) && ids.value(row) >= 0)
            .collect::<Vec<_>>(),
    );
    let filtered = filter_record_batch(batch, &mask)?;
    Ok(filtered.project(&[0, 2])?)
}

pub(crate) fn write_local_fixture_file(
    root: &Path,
    spec: &FixtureSpec,
    format: LocalFormat,
) -> BenchResult<PathBuf> {
    fs::create_dir_all(root)?;
    let path = root.join(format!("orders.{}", format.extension()));
    match format {
        LocalFormat::Csv => fs::write(&path, csv_fixture(spec)?)?,
        LocalFormat::Json => fs::write(&path, json_fixture(spec, true))?,
        LocalFormat::Ndjson => fs::write(&path, json_fixture(spec, false))?,
        LocalFormat::Parquet => write_parquet_fixture(&path, spec)?,
    }
    Ok(path)
}

pub(crate) fn rest_fixture_body(spec: &FixtureSpec) -> Vec<u8> {
    let mut output = String::from(r#"{"items":["#);
    for row in 0..spec.rows {
        if row > 0 {
            output.push(',');
        }
        let id = row as i64;
        output.push_str(&format!(
            r#"{{"id":{id},"active":{},"category":"{}"}}"#,
            active_for_id(id),
            category_for_id(id)
        ));
    }
    output.push_str("]}");
    output.into_bytes()
}

pub(crate) fn startup_ndjson(spec: &FixtureSpec) -> Vec<u8> {
    let mut output = String::new();
    for row in 0..spec.rows {
        output.push_str(&format!(
            r#"{{"id":{},"updated_at":{}}}"#,
            row,
            1_783_296_000_000_000_i64 + row as i64
        ));
        output.push('\n');
    }
    output.into_bytes()
}

pub(crate) fn arrow_ipc_stream_bytes(spec: &FixtureSpec) -> BenchResult<Vec<u8>> {
    let batches = record_batches_for_spec(spec)?;
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, &schema_for_spec(spec))?;
        for batch in batches {
            writer.write(&batch)?;
        }
        writer.finish()?;
    }
    Ok(bytes)
}

pub(crate) fn active_for_id(id: i64) -> bool {
    id % 3 != 0
}

pub(crate) fn category_for_id(id: i64) -> String {
    format!("group-{:02}", id.rem_euclid(8))
}

fn csv_fixture(spec: &FixtureSpec) -> BenchResult<Vec<u8>> {
    let mut output = String::new();
    output.push_str("id,active,category,amount");
    for column in 0..spec.wide_columns {
        output.push_str(&format!(",metric_{column:03}"));
    }
    output.push('\n');
    for row in 0..spec.rows {
        let id = row as i64;
        output.push_str(&format!(
            "{id},{},{},{:.2}",
            active_for_id(id),
            category_for_id(id),
            amount_for_id(id)
        ));
        for column in 0..spec.wide_columns {
            output.push_str(&format!(",{}", metric_for_id(id, column)));
        }
        output.push('\n');
    }
    Ok(output.into_bytes())
}

fn json_fixture(spec: &FixtureSpec, top_level_array: bool) -> Vec<u8> {
    let mut output = String::new();
    if top_level_array {
        output.push('[');
    }
    for row in 0..spec.rows {
        if row > 0 && top_level_array {
            output.push(',');
        }
        write_json_row(&mut output, spec, row as i64);
        if !top_level_array {
            output.push('\n');
        }
    }
    if top_level_array {
        output.push(']');
    }
    output.into_bytes()
}

fn write_json_row(output: &mut String, spec: &FixtureSpec, id: i64) {
    output.push_str(&format!(
        r#"{{"id":{id},"active":{},"category":"{}","amount":{:.2}"#,
        active_for_id(id),
        category_for_id(id),
        amount_for_id(id)
    ));
    for column in 0..spec.wide_columns {
        output.push_str(&format!(
            r#","metric_{column:03}":{}"#,
            metric_for_id(id, column)
        ));
    }
    output.push('}');
}

fn write_parquet_fixture(path: &Path, spec: &FixtureSpec) -> BenchResult<()> {
    let file = fs::File::create(path)?;
    let properties = WriterProperties::builder()
        .set_created_by("cdf benchmark fixture writer".to_owned())
        .build();
    let mut writer = ArrowWriter::try_new(file, Arc::new(schema_for_spec(spec)), Some(properties))?;
    for batch in record_batches_for_spec(spec)? {
        writer.write(&batch)?;
    }
    writer.close()?;
    Ok(())
}

fn amount_for_id(id: i64) -> f64 {
    (id.rem_euclid(10_000) as f64) / 10.0
}

fn metric_for_id(id: i64, column: usize) -> i64 {
    (id * (column as i64 + 1)).rem_euclid(1_000_003)
}
