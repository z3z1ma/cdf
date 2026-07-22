use std::{
    ffi::{CStr, CString},
    fs,
    hint::black_box,
    io::{BufReader, BufWriter, Read, Write},
    mem::{ManuallyDrop, align_of, size_of},
    os::raw::{c_char, c_void},
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    ptr,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use arrow_array::{
    Array, ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatchReader, StringArray,
    StructArray, TimestampMicrosecondArray, UInt64Array, ffi::FFI_ArrowArray,
    ffi_stream::FFI_ArrowArrayStream,
};
use arrow_csv::reader::{Format as CsvFormat, ReaderBuilder as CsvReaderBuilder};
use arrow_ipc::{
    CompressionType as IpcCompressionType,
    reader::FileReader as IpcFileReader,
    writer::{FileWriter as IpcFileWriter, IpcWriteOptions},
};
use arrow_json::reader::{ReaderBuilder as JsonReaderBuilder, infer_json_schema};
use arrow_schema::{
    ArrowError, DataType, Field, Schema, SchemaRef, TimeUnit, ffi::FFI_ArrowSchema,
};
use parquet::{
    arrow::{ArrowWriter, ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::properties::{EnabledStatistics, WriterProperties},
};
use serde::{Deserialize, Serialize};

use crate::{BenchResult, WorkerMeasurement, bench_error};
use crate::{
    Capability, ChildCommand, ChildObservationStatus, HostCapabilityProvider, ToolIdentity,
};

const POLARS_PROBE: &str = "import polars; print(polars.__version__)";
const POLARS_WORKER: &str = r#"
import json, os, polars as pl, sys, time
path, kind = sys.argv[1], sys.argv[2]
started = time.perf_counter_ns()
if kind == "parquet":
    frame = pl.scan_parquet(path).collect()
elif kind == "csv":
    frame = pl.scan_csv(path).collect()
elif kind == "ndjson":
    frame = pl.read_ndjson(path)
else:
    raise ValueError("unsupported Polars reference format")
elapsed = time.perf_counter_ns() - started
print(json.dumps({"timed_wall_time_ns": elapsed, "rows": frame.height, "logical_bytes": frame.estimated_size(), "physical_bytes": os.path.getsize(path), "spill_bytes": 0, "phases": []}, separators=(",", ":")))
"#;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExternalFileFormat {
    Parquet,
    Csv,
    Ndjson,
}

impl ExternalFileFormat {
    fn as_str(self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
            Self::Csv => "csv",
            Self::Ndjson => "ndjson",
        }
    }
}

pub fn discover_polars(
    provider: &dyn HostCapabilityProvider,
) -> BenchResult<Capability<ToolIdentity>> {
    let python = match provider.discover_tool("python3") {
        Capability::Supported { value, .. } => value,
        Capability::Unavailable {
            reason,
            method,
            provider_version,
        } => {
            return Ok(Capability::Unavailable {
                reason,
                method,
                provider_version,
            });
        }
        Capability::Failed {
            error,
            method,
            provider_version,
        } => {
            return Ok(Capability::Failed {
                error,
                method,
                provider_version,
            });
        }
    };
    let command = ChildCommand {
        program: python.executable.clone().into(),
        args: vec!["-c".to_owned(), POLARS_PROBE.to_owned()],
        environment: std::collections::BTreeMap::new(),
        current_dir: None,
    };
    match provider.observe_child(&command, std::time::Duration::from_secs(10))? {
        ChildObservationStatus::Completed(observation) => {
            let version = String::from_utf8(observation.stdout)?;
            let version = version.trim();
            if version.is_empty()
                || version.contains('/')
                || version.contains('\\')
                || version.contains('@')
            {
                return Ok(Capability::Failed {
                    error: "Polars version probe returned invalid output".to_owned(),
                    method: "isolated-python-module-probe".to_owned(),
                    provider_version: "polars-reference-v1".to_owned(),
                });
            }
            Ok(Capability::Supported {
                value: ToolIdentity {
                    name: "polars".to_owned(),
                    version: version.to_owned(),
                    executable: python.executable,
                },
                method: "isolated-python-module-probe".to_owned(),
                provider_version: "polars-reference-v1".to_owned(),
            })
        }
        ChildObservationStatus::Failed { .. } => Ok(Capability::Unavailable {
            reason: "Polars Python module is not available".to_owned(),
            method: "isolated-python-module-probe".to_owned(),
            provider_version: "polars-reference-v1".to_owned(),
        }),
        ChildObservationStatus::TimedOut => Ok(Capability::Failed {
            error: "Polars Python module probe timed out".to_owned(),
            method: "isolated-python-module-probe".to_owned(),
            provider_version: "polars-reference-v1".to_owned(),
        }),
    }
}

pub fn polars_scan_command(
    identity: &ToolIdentity,
    path: PathBuf,
    format: ExternalFileFormat,
) -> ChildCommand {
    ChildCommand {
        program: identity.executable.clone().into(),
        args: vec![
            "-c".to_owned(),
            POLARS_WORKER.to_owned(),
            path.display().to_string(),
            format.as_str().to_owned(),
        ],
        environment: std::collections::BTreeMap::new(),
        current_dir: None,
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReferenceWorkload {
    SequentialRead {
        path: PathBuf,
        buffer_bytes: usize,
    },
    SequentialWrite {
        path: PathBuf,
        logical_bytes: u64,
        buffer_bytes: usize,
        sync: bool,
    },
    Memcpy {
        logical_bytes: u64,
        buffer_bytes: usize,
    },
    ArrowParquet {
        path: PathBuf,
        batch_rows: usize,
    },
    ArrowParquetFiles {
        paths: Vec<PathBuf>,
        projection: Vec<String>,
        batch_rows: usize,
        jobs: usize,
    },
    ArrowParquetRewrite {
        path: PathBuf,
        output: PathBuf,
        read_batch_rows: usize,
        write_batch_rows: usize,
        write_batch_bytes: usize,
        sync: bool,
    },
    ArrowCsv {
        path: PathBuf,
        batch_rows: usize,
        has_header: bool,
        infer_rows: usize,
    },
    ArrowNdjson {
        path: PathBuf,
        batch_rows: usize,
        infer_rows: usize,
    },
    DuckDbParquet {
        path: PathBuf,
    },
    DuckDbParquetIngest {
        paths: Vec<PathBuf>,
        output: PathBuf,
    },
    DuckDbParquetIngestWithRowKey {
        paths: Vec<PathBuf>,
        output: PathBuf,
        checkpoint: bool,
    },
    DuckDbArrowAppend {
        output: PathBuf,
        rows: u64,
        batch_rows: usize,
        include_row_key: bool,
        checkpoint: bool,
    },
    DuckDbArrowDataChunkAppend {
        output: PathBuf,
        rows: u64,
        batch_rows: usize,
        include_row_key: bool,
        checkpoint: bool,
    },
    DuckDbArrowStreamScanIngest {
        output: PathBuf,
        rows: u64,
        batch_rows: usize,
        include_row_key: bool,
        checkpoint: bool,
        verify_rowid: bool,
        duckdb_threads: Option<i64>,
        duckdb_memory_limit_bytes: Option<u64>,
        duckdb_temp_directory_budget_bytes: Option<u64>,
    },
    DuckDbArrowIpcExistingRead {
        paths: Vec<PathBuf>,
        output: PathBuf,
        extension: DuckDbArrowExtension,
        #[serde(default)]
        row_key_start: Option<u64>,
        #[serde(default)]
        row_key_not_null: bool,
        checkpoint: bool,
    },
    DuckDbArrowIpcTableFunctionIngest {
        paths: Vec<PathBuf>,
        output: PathBuf,
        profiling_directory: Option<PathBuf>,
        row_key_start: u64,
        checkpoint: bool,
        duckdb_threads: Option<usize>,
        scan_threads: Option<usize>,
        duckdb_memory_limit_bytes: Option<u64>,
        duckdb_temp_directory_budget_bytes: Option<u64>,
    },
    DuckDbArrowIpcHandoffIngest {
        output: PathBuf,
        staging_dir: PathBuf,
        rows: u64,
        batch_rows: usize,
        rows_per_file: u64,
        include_row_key: bool,
        compression: ArrowIpcCompression,
        extension: DuckDbArrowExtension,
        checkpoint: bool,
    },
    DuckDbParquetStagedIngest {
        output: PathBuf,
        staging: PathBuf,
        rows: u64,
        batch_rows: usize,
        row_group_rows: usize,
        row_group_bytes: usize,
        include_row_key: bool,
        checkpoint: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DuckDbArrowExtension {
    Nanoarrow,
    Arrow,
    Loadable {
        path: PathBuf,
        expected_nanoarrow_version: String,
    },
}

impl DuckDbArrowExtension {
    fn community_extension_name(&self) -> Option<&'static str> {
        match self {
            Self::Nanoarrow => Some("nanoarrow"),
            Self::Arrow => Some("arrow"),
            Self::Loadable { .. } => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArrowIpcCompression {
    None,
    Lz4Frame,
}

pub fn run_reference(workload: &ReferenceWorkload) -> BenchResult<WorkerMeasurement> {
    match workload {
        ReferenceWorkload::SequentialRead { path, buffer_bytes } => {
            require_buffer(*buffer_bytes)?;
            let physical_bytes = fs::metadata(path)?.len();
            let mut reader = BufReader::with_capacity(*buffer_bytes, fs::File::open(path)?);
            let mut buffer = vec![0_u8; *buffer_bytes];
            let mut logical_bytes = 0_u64;
            loop {
                let read = reader.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                logical_bytes = logical_bytes.saturating_add(read as u64);
                black_box(&buffer[..read]);
            }
            measurement(0, logical_bytes, physical_bytes)
        }
        ReferenceWorkload::SequentialWrite {
            path,
            logical_bytes,
            buffer_bytes,
            sync,
        } => {
            require_buffer(*buffer_bytes)?;
            let file = fs::File::create(path)?;
            let mut writer = BufWriter::with_capacity(*buffer_bytes, file);
            let buffer = vec![0xA5_u8; *buffer_bytes];
            let mut remaining = *logical_bytes;
            while remaining > 0 {
                let count = usize::try_from(remaining.min(*buffer_bytes as u64))?;
                writer.write_all(&buffer[..count])?;
                remaining -= count as u64;
            }
            writer.flush()?;
            if *sync {
                writer.get_ref().sync_all()?;
            }
            measurement(0, *logical_bytes, fs::metadata(path)?.len())
        }
        ReferenceWorkload::Memcpy {
            logical_bytes,
            buffer_bytes,
        } => {
            require_buffer(*buffer_bytes)?;
            let source = vec![0x5A_u8; *buffer_bytes];
            let mut destination = vec![0_u8; *buffer_bytes];
            let mut remaining = *logical_bytes;
            while remaining > 0 {
                let count = usize::try_from(remaining.min(*buffer_bytes as u64))?;
                destination[..count].copy_from_slice(&source[..count]);
                black_box(&destination[..count]);
                remaining -= count as u64;
            }
            measurement(0, *logical_bytes, *logical_bytes)
        }
        ReferenceWorkload::ArrowParquet { path, batch_rows } => {
            require_batch(*batch_rows)?;
            let physical_bytes = fs::metadata(path)?.len();
            let reader = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(path)?)?
                .with_batch_size(*batch_rows)
                .build()?;
            collect_arrow(reader, physical_bytes)
        }
        ReferenceWorkload::ArrowParquetFiles {
            paths,
            projection,
            batch_rows,
            jobs,
        } => run_arrow_parquet_files(paths, projection, *batch_rows, *jobs),
        ReferenceWorkload::ArrowParquetRewrite {
            path,
            output,
            read_batch_rows,
            write_batch_rows,
            write_batch_bytes,
            sync,
        } => {
            require_batch(*read_batch_rows)?;
            require_batch(*write_batch_rows)?;
            require_buffer(*write_batch_bytes)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(path)?)?
                .with_batch_size(*read_batch_rows);
            let schema = builder.schema().clone();
            let reader = builder.build()?;
            let properties = WriterProperties::builder()
                .set_created_by("cdf benchmark direct arrow-rs rewrite".to_owned())
                .set_write_batch_size(*write_batch_rows)
                .set_data_page_row_count_limit((*write_batch_rows).min(64 * 1024))
                .set_data_page_size_limit((*write_batch_bytes).min(8 * 1024 * 1024))
                .set_max_row_group_row_count(Some(*write_batch_rows))
                .set_max_row_group_bytes(Some(*write_batch_bytes))
                .set_dictionary_enabled(false)
                .set_statistics_enabled(EnabledStatistics::None)
                .build();
            let file = fs::File::create(output)?;
            let mut output_writer = BufWriter::with_capacity(1024 * 1024, file);
            let mut writer = ArrowWriter::try_new(&mut output_writer, schema, Some(properties))?;
            let mut rows = 0_u64;
            let mut logical_bytes = 0_u64;
            for batch in reader {
                let batch = batch?;
                rows = rows.saturating_add(batch.num_rows() as u64);
                logical_bytes =
                    logical_bytes.saturating_add(u64::try_from(batch.get_array_memory_size())?);
                writer.write(&batch)?;
            }
            writer.close()?;
            output_writer.flush()?;
            if *sync {
                output_writer.get_ref().sync_all()?;
            }
            measurement(rows, logical_bytes, fs::metadata(output)?.len())
        }
        ReferenceWorkload::ArrowCsv {
            path,
            batch_rows,
            has_header,
            infer_rows,
        } => {
            require_batch(*batch_rows)?;
            if *infer_rows == 0 {
                return Err(bench_error("CSV reference infer_rows must be positive"));
            }
            let physical_bytes = fs::metadata(path)?.len();
            let format = CsvFormat::default().with_header(*has_header);
            // The CDF comparison path decodes against an already-pinned schema. Keep the
            // reference's inference bounded as discovery would be, rather than charging it a
            // hidden full-file pass before the measured decode.
            let (schema, _) = format.infer_schema(fs::File::open(path)?, Some(*infer_rows))?;
            let reader = CsvReaderBuilder::new(Arc::new(schema))
                .with_format(format)
                .with_batch_size(*batch_rows)
                .build(fs::File::open(path)?)?;
            collect_arrow(reader, physical_bytes)
        }
        ReferenceWorkload::ArrowNdjson {
            path,
            batch_rows,
            infer_rows,
        } => {
            require_batch(*batch_rows)?;
            if *infer_rows == 0 {
                return Err(bench_error("NDJSON reference infer_rows must be positive"));
            }
            let physical_bytes = fs::metadata(path)?.len();
            let (schema, _) =
                infer_json_schema(BufReader::new(fs::File::open(path)?), Some(*infer_rows))?;
            let reader = JsonReaderBuilder::new(Arc::new(schema))
                .with_batch_size(*batch_rows)
                .build(BufReader::new(fs::File::open(path)?))?;
            collect_arrow(reader, physical_bytes)
        }
        ReferenceWorkload::DuckDbParquet { path } => {
            let physical_bytes = fs::metadata(path)?.len();
            let connection = duckdb::Connection::open_in_memory()?;
            let rows = connection.query_row(
                "SELECT count(*) FROM read_parquet(?)",
                [path.display().to_string()],
                |row| row.get::<_, i64>(0),
            )?;
            measurement(u64::try_from(rows)?, physical_bytes, physical_bytes)
        }
        ReferenceWorkload::DuckDbParquetIngest { paths, output } => {
            let physical_bytes = input_bytes(paths)?;
            if let Some(parent) = output.parent() {
                fs::create_dir_all(parent)?;
            }
            remove_if_exists(output)?;
            remove_if_exists(&duckdb_wal_path(output))?;
            let connection = duckdb::Connection::open(output)?;
            connection.execute_batch(&format!(
                "CREATE TABLE native_ingest AS SELECT * FROM read_parquet({})",
                duckdb_parquet_input_sql(paths)?
            ))?;
            let rows = connection.query_row("SELECT count(*) FROM native_ingest", [], |row| {
                row.get::<_, i64>(0)
            })?;
            connection.execute_batch("CHECKPOINT")?;
            measurement(u64::try_from(rows)?, physical_bytes, physical_bytes)
        }
        ReferenceWorkload::DuckDbParquetIngestWithRowKey {
            paths,
            output,
            checkpoint,
        } => run_duckdb_parquet_ingest_with_row_key(paths, output, *checkpoint),
        ReferenceWorkload::DuckDbArrowAppend {
            output,
            rows,
            batch_rows,
            include_row_key,
            checkpoint,
        } => run_duckdb_arrow_append(output, *rows, *batch_rows, *include_row_key, *checkpoint),
        ReferenceWorkload::DuckDbArrowDataChunkAppend {
            output,
            rows,
            batch_rows,
            include_row_key,
            checkpoint,
        } => run_duckdb_arrow_data_chunk_append(
            output,
            *rows,
            *batch_rows,
            *include_row_key,
            *checkpoint,
        ),
        ReferenceWorkload::DuckDbArrowStreamScanIngest {
            output,
            rows,
            batch_rows,
            include_row_key,
            checkpoint,
            verify_rowid,
            duckdb_threads,
            duckdb_memory_limit_bytes,
            duckdb_temp_directory_budget_bytes,
        } => run_duckdb_arrow_stream_scan_ingest(
            output,
            *rows,
            *batch_rows,
            DuckDbArrowStreamScanOptions {
                include_row_key: *include_row_key,
                checkpoint: *checkpoint,
                verify_rowid: *verify_rowid,
                duckdb_threads: *duckdb_threads,
                duckdb_memory_limit_bytes: *duckdb_memory_limit_bytes,
                duckdb_temp_directory_budget_bytes: *duckdb_temp_directory_budget_bytes,
            },
        ),
        ReferenceWorkload::DuckDbArrowIpcExistingRead {
            paths,
            output,
            extension,
            row_key_start,
            row_key_not_null,
            checkpoint,
        } => run_duckdb_arrow_ipc_existing_read(
            paths,
            output,
            extension,
            *row_key_start,
            *row_key_not_null,
            *checkpoint,
        ),
        ReferenceWorkload::DuckDbArrowIpcTableFunctionIngest {
            paths,
            output,
            profiling_directory,
            row_key_start,
            checkpoint,
            duckdb_threads,
            scan_threads,
            duckdb_memory_limit_bytes,
            duckdb_temp_directory_budget_bytes,
        } => run_duckdb_arrow_ipc_table_function_ingest(
            paths,
            output,
            DuckDbArrowIpcTableFunctionOptions {
                profiling_directory: profiling_directory.as_deref(),
                row_key_start: *row_key_start,
                checkpoint: *checkpoint,
                duckdb_threads: *duckdb_threads,
                scan_threads: *scan_threads,
                duckdb_memory_limit_bytes: *duckdb_memory_limit_bytes,
                duckdb_temp_directory_budget_bytes: *duckdb_temp_directory_budget_bytes,
            },
        ),
        ReferenceWorkload::DuckDbArrowIpcHandoffIngest {
            output,
            staging_dir,
            rows,
            batch_rows,
            rows_per_file,
            include_row_key,
            compression,
            extension,
            checkpoint,
        } => run_duckdb_arrow_ipc_handoff_ingest(
            output,
            staging_dir,
            *rows,
            *batch_rows,
            *rows_per_file,
            DuckDbArrowIpcHandoffOptions {
                include_row_key: *include_row_key,
                compression: *compression,
                extension,
                checkpoint: *checkpoint,
            },
        ),
        ReferenceWorkload::DuckDbParquetStagedIngest {
            output,
            staging,
            rows,
            batch_rows,
            row_group_rows,
            row_group_bytes,
            include_row_key,
            checkpoint,
        } => run_duckdb_parquet_staged_ingest(
            output,
            staging,
            *rows,
            *batch_rows,
            DuckDbParquetStagedOptions {
                row_group_rows: *row_group_rows,
                row_group_bytes: *row_group_bytes,
                include_row_key: *include_row_key,
                checkpoint: *checkpoint,
            },
        ),
    }
}

fn run_arrow_parquet_files(
    paths: &[PathBuf],
    projection: &[String],
    batch_rows: usize,
    jobs: usize,
) -> BenchResult<WorkerMeasurement> {
    require_batch(batch_rows)?;
    if paths.is_empty() {
        return Err(bench_error(
            "Arrow Parquet files reference requires at least one input path",
        ));
    }
    if projection.is_empty() {
        return Err(bench_error(
            "Arrow Parquet files reference requires a nonempty projection",
        ));
    }
    if jobs == 0 {
        return Err(bench_error(
            "Arrow Parquet files reference jobs must be nonzero",
        ));
    }
    let physical_bytes = paths.iter().try_fold(0_u64, |total, path| {
        total
            .checked_add(fs::metadata(path)?.len())
            .ok_or_else(|| bench_error("Arrow Parquet files physical byte count overflowed"))
    })?;
    let next = AtomicUsize::new(0);
    let worker_count = jobs.min(paths.len());
    let partials = std::thread::scope(|scope| {
        let handles = (0..worker_count)
            .map(|_| {
                scope.spawn(|| -> BenchResult<(u64, u64)> {
                    let mut rows = 0_u64;
                    let mut logical_bytes = 0_u64;
                    loop {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        let Some(path) = paths.get(index) else {
                            break;
                        };
                        let builder =
                            ParquetRecordBatchReaderBuilder::try_new(fs::File::open(path)?)?;
                        let indices = projection
                            .iter()
                            .map(|name| builder.schema().index_of(name).map_err(Into::into))
                            .collect::<BenchResult<Vec<_>>>()?;
                        let mask = ProjectionMask::roots(builder.parquet_schema(), indices);
                        let reader = builder
                            .with_projection(mask)
                            .with_batch_size(batch_rows)
                            .build()?;
                        for batch in reader {
                            let batch = batch?;
                            rows = rows.saturating_add(batch.num_rows() as u64);
                            logical_bytes = logical_bytes
                                .saturating_add(u64::try_from(batch.get_array_memory_size())?);
                            black_box(batch);
                        }
                    }
                    Ok((rows, logical_bytes))
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .map_err(|_| bench_error("Arrow Parquet files reference worker panicked"))?
            })
            .collect::<BenchResult<Vec<_>>>()
    })?;
    let (rows, logical_bytes) = partials.into_iter().fold(
        (0_u64, 0_u64),
        |(rows, bytes), (partial_rows, partial_bytes)| {
            (
                rows.saturating_add(partial_rows),
                bytes.saturating_add(partial_bytes),
            )
        },
    );
    measurement(rows, logical_bytes, physical_bytes)
}

fn input_bytes(paths: &[PathBuf]) -> BenchResult<u64> {
    if paths.is_empty() {
        return Err(bench_error(
            "DuckDB Parquet ingest reference requires at least one input path",
        ));
    }
    paths.iter().try_fold(0_u64, |total, path| {
        Ok(total.saturating_add(fs::metadata(path)?.len()))
    })
}

fn duckdb_parquet_input_sql(paths: &[PathBuf]) -> BenchResult<String> {
    if paths.is_empty() {
        return Err(bench_error(
            "DuckDB Parquet reference requires at least one input path",
        ));
    }
    if paths.len() == 1 {
        return Ok(duckdb_string_literal(&paths[0]));
    }
    let mut sql = String::from("[");
    for (index, path) in paths.iter().enumerate() {
        if index > 0 {
            sql.push(',');
        }
        sql.push_str(&duckdb_string_literal(path));
    }
    sql.push(']');
    Ok(sql)
}

fn run_duckdb_parquet_ingest_with_row_key(
    paths: &[PathBuf],
    output: &Path,
    checkpoint: bool,
) -> BenchResult<WorkerMeasurement> {
    let physical_bytes = input_bytes(paths)?;
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    remove_if_exists(output)?;
    remove_if_exists(&duckdb_wal_path(output))?;
    let connection = duckdb::Connection::open(output)?;
    connection.execute_batch(&format!(
        "CREATE TABLE native_ingest_with_row_key AS \
         SELECT *, CAST(row_number() OVER () AS UBIGINT) AS {} \
         FROM read_parquet({})",
        duckdb_ident(cdf_dest_duckdb::CDF_ROW_KEY_COLUMN),
        duckdb_parquet_input_sql(paths)?
    ))?;
    if checkpoint {
        connection.execute_batch("CHECKPOINT")?;
    }
    let rows = connection.query_row(
        "SELECT count(*) FROM native_ingest_with_row_key",
        [],
        |row| row.get::<_, i64>(0),
    )?;
    measurement(u64::try_from(rows)?, physical_bytes, physical_bytes)
}

fn run_duckdb_arrow_append(
    output: &Path,
    rows: u64,
    batch_rows: usize,
    include_row_key: bool,
    checkpoint: bool,
) -> BenchResult<WorkerMeasurement> {
    if rows == 0 {
        return Err(bench_error(
            "DuckDB Arrow append reference requires at least one row",
        ));
    }
    require_batch(batch_rows)?;
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    remove_if_exists(output)?;
    remove_if_exists(&duckdb_wal_path(output))?;
    let connection = duckdb::Connection::open(output)?;
    let mut columns = tlc_duckdb_columns();
    if include_row_key {
        columns.push((cdf_dest_duckdb::CDF_ROW_KEY_COLUMN, "UBIGINT"));
    }
    let column_sql = columns
        .iter()
        .map(|(name, sql_type)| format!("{} {} NOT NULL", duckdb_ident(name), sql_type))
        .collect::<Vec<_>>()
        .join(", ");
    connection.execute_batch(&format!("CREATE TABLE arrow_append ({column_sql})"))?;
    connection.execute_batch("BEGIN TRANSACTION")?;
    let mut appender = connection.appender("arrow_append")?;
    let mut remaining = rows;
    let mut row_start = 1_u64;
    let mut logical_bytes = 0_u64;
    while remaining > 0 {
        let current_rows = usize::try_from(remaining.min(batch_rows as u64))?;
        let batch = tlc_duckdb_arrow_batch(current_rows, row_start, include_row_key)?;
        logical_bytes = logical_bytes.saturating_add(u64::try_from(batch.get_array_memory_size())?);
        appender.append_record_batch(batch)?;
        row_start = row_start
            .checked_add(u64::try_from(current_rows)?)
            .ok_or_else(|| bench_error("DuckDB Arrow append row offset overflowed"))?;
        remaining -= u64::try_from(current_rows)?;
    }
    appender.flush()?;
    drop(appender);
    connection.execute_batch("COMMIT")?;
    if checkpoint {
        connection.execute_batch("CHECKPOINT")?;
    }
    let observed_rows = connection.query_row("SELECT count(*) FROM arrow_append", [], |row| {
        row.get::<_, i64>(0)
    })?;
    let observed_rows = u64::try_from(observed_rows)?;
    if observed_rows != rows {
        return Err(bench_error(format!(
            "DuckDB Arrow append row count mismatch: expected {rows}, observed {observed_rows}"
        )));
    }
    Ok(WorkerMeasurement {
        timed_wall_time_ns: None,
        rows,
        logical_bytes,
        physical_bytes: duckdb_database_bytes(output)?,
        spill_bytes: 0,
        phases: Vec::new(),
    })
}

fn run_duckdb_arrow_data_chunk_append(
    output: &Path,
    rows: u64,
    batch_rows: usize,
    include_row_key: bool,
    checkpoint: bool,
) -> BenchResult<WorkerMeasurement> {
    if rows == 0 {
        return Err(bench_error(
            "DuckDB Arrow data-chunk append reference requires at least one row",
        ));
    }
    require_batch(batch_rows)?;
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    remove_if_exists(output)?;
    remove_if_exists(&duckdb_wal_path(output))?;

    let mut connection = RawDuckDbConnection::open(output)?;
    let mut columns = tlc_duckdb_columns();
    if include_row_key {
        columns.push((cdf_dest_duckdb::CDF_ROW_KEY_COLUMN, "UBIGINT"));
    }
    let column_sql = columns
        .iter()
        .map(|(name, sql_type)| format!("{} {} NOT NULL", duckdb_ident(name), sql_type))
        .collect::<Vec<_>>()
        .join(", ");
    connection.query(&format!(
        "CREATE TABLE arrow_data_chunk_append ({column_sql})"
    ))?;
    connection.query("BEGIN TRANSACTION")?;

    let mut converted_schema = DuckDbArrowConvertedSchema::from_arrow(
        connection.handle(),
        &tlc_arrow_schema(include_row_key),
    )?;
    let mut appender = RawDuckDbAppender::create(connection.handle(), "arrow_data_chunk_append")?;
    let mut remaining = rows;
    let mut row_start = 1_u64;
    let mut logical_bytes = 0_u64;
    while remaining > 0 {
        let current_rows = usize::try_from(remaining.min(batch_rows as u64))?;
        let batch = tlc_arrow_batch(current_rows, row_start, include_row_key)?;
        logical_bytes = logical_bytes.saturating_add(u64::try_from(batch.get_array_memory_size())?);
        append_arrow_batch_as_duckdb_data_chunk(
            connection.handle(),
            appender.handle(),
            converted_schema.handle(),
            batch,
        )?;
        row_start = row_start
            .checked_add(u64::try_from(current_rows)?)
            .ok_or_else(|| bench_error("DuckDB Arrow data-chunk append row offset overflowed"))?;
        remaining -= u64::try_from(current_rows)?;
    }
    appender.flush()?;
    drop(appender);
    drop(converted_schema);
    connection.query("COMMIT")?;
    if checkpoint {
        connection.query("CHECKPOINT")?;
    }
    drop(connection);

    let connection = duckdb::Connection::open(output)?;
    let observed_rows =
        connection.query_row("SELECT count(*) FROM arrow_data_chunk_append", [], |row| {
            row.get::<_, i64>(0)
        })?;
    let observed_rows = u64::try_from(observed_rows)?;
    if observed_rows != rows {
        return Err(bench_error(format!(
            "DuckDB Arrow data-chunk append row count mismatch: expected {rows}, observed {observed_rows}"
        )));
    }
    Ok(WorkerMeasurement {
        timed_wall_time_ns: None,
        rows,
        logical_bytes,
        physical_bytes: duckdb_database_bytes(output)?,
        spill_bytes: 0,
        phases: Vec::new(),
    })
}

#[derive(Clone, Copy)]
struct DuckDbArrowStreamScanOptions {
    include_row_key: bool,
    checkpoint: bool,
    verify_rowid: bool,
    duckdb_threads: Option<i64>,
    duckdb_memory_limit_bytes: Option<u64>,
    duckdb_temp_directory_budget_bytes: Option<u64>,
}

fn run_duckdb_arrow_stream_scan_ingest(
    output: &Path,
    rows: u64,
    batch_rows: usize,
    options: DuckDbArrowStreamScanOptions,
) -> BenchResult<WorkerMeasurement> {
    if rows == 0 {
        return Err(bench_error(
            "DuckDB Arrow stream-scan ingest reference requires at least one row",
        ));
    }
    require_batch(batch_rows)?;
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    remove_if_exists(output)?;
    remove_if_exists(&duckdb_wal_path(output))?;

    let logical_bytes = Arc::new(AtomicU64::new(0));
    let reader = TlcArrowBatchReader::new(
        rows,
        batch_rows,
        options.include_row_key,
        logical_bytes.clone(),
    );
    let mut stream = FFI_ArrowArrayStream::new(Box::new(reader));
    let mut connection = RawDuckDbConnection::open(output)?;
    configure_duckdb_parallel_scan(
        &mut connection,
        options.duckdb_threads,
        options.duckdb_memory_limit_bytes,
        options.duckdb_temp_directory_budget_bytes,
    )?;
    register_duckdb_arrow_stream_scan(connection.handle(), "cdf_arrow_stream", &mut stream)?;
    connection.query("CREATE TABLE arrow_stream_scan AS SELECT * FROM cdf_arrow_stream")?;
    if options.checkpoint {
        connection.query("CHECKPOINT")?;
    }
    drop(connection);
    drop(stream);

    let connection = duckdb::Connection::open(output)?;
    let observed_rows =
        connection.query_row("SELECT count(*) FROM arrow_stream_scan", [], |row| {
            row.get::<_, i64>(0)
        })?;
    let observed_rows = u64::try_from(observed_rows)?;
    if observed_rows != rows {
        return Err(bench_error(format!(
            "DuckDB Arrow stream-scan row count mismatch: expected {rows}, observed {observed_rows}"
        )));
    }
    if options.verify_rowid {
        let (count, distinct_count, min_rowid, max_rowid) = connection.query_row(
            "SELECT count(*), count(DISTINCT rowid), min(rowid), max(rowid) \
             FROM arrow_stream_scan",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )?;
        let rows_i64 = i64::try_from(rows)?;
        if count != rows_i64 || distinct_count != rows_i64 || min_rowid < 0 || max_rowid < min_rowid
        {
            return Err(bench_error(format!(
                "DuckDB Arrow stream-scan rowid verification failed: count={count}, \
                 distinct={distinct_count}, min={min_rowid}, max={max_rowid}, expected={rows}"
            )));
        }
    }

    Ok(WorkerMeasurement {
        timed_wall_time_ns: None,
        rows,
        logical_bytes: logical_bytes.load(Ordering::Relaxed),
        physical_bytes: duckdb_database_bytes(output)?,
        spill_bytes: 0,
        phases: Vec::new(),
    })
}

fn run_duckdb_arrow_ipc_existing_read(
    paths: &[PathBuf],
    output: &Path,
    extension: &DuckDbArrowExtension,
    row_key_start: Option<u64>,
    row_key_not_null: bool,
    checkpoint: bool,
) -> BenchResult<WorkerMeasurement> {
    if paths.is_empty() {
        return Err(bench_error(
            "DuckDB Arrow IPC existing-read reference requires at least one input path",
        ));
    }
    let physical_input_bytes = input_bytes(paths)?;
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    remove_if_exists(output)?;
    remove_if_exists(&duckdb_wal_path(output))?;
    let connection = open_duckdb_arrow_database(output, extension)?;
    load_duckdb_arrow_extension(&connection, extension)?;
    connection.execute_batch("SET preserve_insertion_order = true")?;
    connection.execute_batch(&format!(
        "CREATE TABLE arrow_ipc_read AS SELECT * FROM read_arrow({})",
        duckdb_path_list(paths)
    ))?;
    let rows = connection.query_row("SELECT count(*) FROM arrow_ipc_read", [], |row| {
        row.get::<_, u64>(0)
    })?;
    if let Some(first_row_key) = row_key_start {
        connection.execute_batch("ALTER TABLE arrow_ipc_read ADD COLUMN _cdf_row_key UBIGINT")?;
        connection.execute_batch(&format!(
            "UPDATE arrow_ipc_read SET _cdf_row_key = CAST(rowid + {first_row_key} AS UBIGINT)"
        ))?;
        if row_key_not_null {
            connection.execute_batch(
                "ALTER TABLE arrow_ipc_read ALTER COLUMN _cdf_row_key SET NOT NULL",
            )?;
        }
        let (minimum, maximum) = connection.query_row(
            "SELECT min(_cdf_row_key), max(_cdf_row_key) FROM arrow_ipc_read",
            [],
            |row| Ok((row.get::<_, Option<u64>>(0)?, row.get::<_, Option<u64>>(1)?)),
        )?;
        let expected_maximum = match rows.checked_sub(1) {
            Some(last_offset) => Some(
                first_row_key
                    .checked_add(last_offset)
                    .ok_or_else(|| bench_error("DuckDB Arrow IPC row key overflow"))?,
            ),
            None => None,
        };
        if minimum != rows.checked_sub(1).map(|_| first_row_key) || maximum != expected_maximum {
            return Err(bench_error(format!(
                "DuckDB Arrow IPC row-key verification failed: min={minimum:?}, max={maximum:?}, expected start={first_row_key}, expected max={expected_maximum:?}, rows={rows}"
            )));
        }
    } else if row_key_not_null {
        return Err(bench_error(
            "DuckDB Arrow IPC row-key NOT NULL verification requires row_key_start",
        ));
    }
    if checkpoint {
        connection.execute_batch("CHECKPOINT")?;
    }
    measurement(
        rows,
        physical_input_bytes,
        physical_input_bytes.saturating_add(duckdb_database_bytes(output)?),
    )
}

#[derive(Clone, Copy)]
struct DuckDbArrowIpcTableFunctionOptions<'a> {
    profiling_directory: Option<&'a Path>,
    row_key_start: u64,
    checkpoint: bool,
    duckdb_threads: Option<usize>,
    scan_threads: Option<usize>,
    duckdb_memory_limit_bytes: Option<u64>,
    duckdb_temp_directory_budget_bytes: Option<u64>,
}

fn run_duckdb_arrow_ipc_table_function_ingest(
    paths: &[PathBuf],
    output: &Path,
    options: DuckDbArrowIpcTableFunctionOptions<'_>,
) -> BenchResult<WorkerMeasurement> {
    if paths.is_empty() {
        return Err(bench_error(
            "DuckDB Arrow IPC table-function reference requires at least one input path",
        ));
    }
    let physical_input_bytes = input_bytes(paths)?;
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    remove_if_exists(output)?;
    remove_if_exists(&duckdb_wal_path(output))?;

    let first_reader = IpcFileReader::try_new_buffered(fs::File::open(&paths[0])?, None)?;
    let schema = first_reader.schema();
    drop(first_reader);
    cdf_package_contract::logical_output_schema(schema.as_ref())?;
    let resolved_threads = match options.duckdb_threads {
        Some(0) => {
            return Err(bench_error(
                "DuckDB Arrow IPC table-function duckdb_threads must be positive",
            ));
        }
        Some(threads) => threads,
        None => std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1),
    };
    let scan_threads = match options.scan_threads {
        Some(0) => {
            return Err(bench_error(
                "DuckDB Arrow IPC table-function scan_threads must be positive",
            ));
        }
        Some(threads) => threads,
        None => resolved_threads,
    };

    let mut connection = RawDuckDbConnection::open(output)?;
    configure_duckdb_parallel_scan(
        &mut connection,
        Some(i64::try_from(resolved_threads)?),
        options.duckdb_memory_limit_bytes,
        options.duckdb_temp_directory_budget_bytes,
    )?;
    let telemetry = register_duckdb_arrow_ipc_table_function(
        connection.handle(),
        "cdf_arrow_ipc_scan",
        paths,
        schema,
        scan_threads,
    )?;
    let profile = ReferenceDuckDbProfileCapture::start(
        &mut connection,
        options.profiling_directory,
        scan_threads,
    )?;
    let materialization = connection.query(&format!(
        "CREATE TABLE arrow_ipc_table_function AS \
         SELECT * EXCLUDE ({}), \
         CAST({} + {} AS UBIGINT) AS {} \
         FROM cdf_arrow_ipc_scan()",
        duckdb_ident(cdf_package_contract::CDF_PACKAGE_ROW_ORD_FIELD),
        options.row_key_start,
        duckdb_ident(cdf_package_contract::CDF_PACKAGE_ROW_ORD_FIELD),
        duckdb_ident(cdf_dest_duckdb::CDF_ROW_KEY_COLUMN),
    ));
    let profile = profile
        .map(|profile| profile.finish(&mut connection))
        .transpose();
    match (materialization, profile) {
        (Ok(()), Ok(_)) => {}
        (Err(error), Ok(_)) | (Ok(()), Err(error)) => return Err(error),
        (Err(error), Err(profile_error)) => {
            return Err(bench_error(format!(
                "{error}; DuckDB reference profile capture also failed: {profile_error}"
            )));
        }
    }
    if options.checkpoint {
        connection.query("CHECKPOINT")?;
    }
    let logical_bytes = telemetry.logical_bytes.load(Ordering::Relaxed);
    let decoded_rows = telemetry.rows.load(Ordering::Relaxed);
    drop(connection);

    let connection = duckdb::Connection::open(output)?;
    let (rows, minimum, maximum) = connection.query_row(
        "SELECT count(*), min(_cdf_row_key), max(_cdf_row_key) \
         FROM arrow_ipc_table_function",
        [],
        |row| {
            Ok((
                row.get::<_, u64>(0)?,
                row.get::<_, Option<u64>>(1)?,
                row.get::<_, Option<u64>>(2)?,
            ))
        },
    )?;
    if rows != decoded_rows {
        return Err(bench_error(format!(
            "DuckDB Arrow IPC table-function row count mismatch: decoded {decoded_rows}, materialized {rows}"
        )));
    }
    let expected_maximum = rows
        .checked_sub(1)
        .map(|last| {
            options
                .row_key_start
                .checked_add(last)
                .ok_or_else(|| bench_error("DuckDB Arrow IPC table-function row key overflow"))
        })
        .transpose()?;
    if minimum != rows.checked_sub(1).map(|_| options.row_key_start) || maximum != expected_maximum
    {
        return Err(bench_error(format!(
            "DuckDB Arrow IPC table-function row-key verification failed: min={minimum:?}, max={maximum:?}, expected start={}, expected max={expected_maximum:?}, rows={rows}",
            options.row_key_start
        )));
    }

    Ok(WorkerMeasurement {
        timed_wall_time_ns: None,
        rows,
        logical_bytes,
        physical_bytes: physical_input_bytes.saturating_add(duckdb_database_bytes(output)?),
        spill_bytes: 0,
        phases: Vec::new(),
    })
}

#[derive(Clone, Copy)]
struct DuckDbArrowIpcHandoffOptions<'a> {
    include_row_key: bool,
    compression: ArrowIpcCompression,
    extension: &'a DuckDbArrowExtension,
    checkpoint: bool,
}

fn run_duckdb_arrow_ipc_handoff_ingest(
    output: &Path,
    staging_dir: &Path,
    rows: u64,
    batch_rows: usize,
    rows_per_file: u64,
    options: DuckDbArrowIpcHandoffOptions<'_>,
) -> BenchResult<WorkerMeasurement> {
    if rows == 0 {
        return Err(bench_error(
            "DuckDB Arrow IPC handoff reference requires at least one row",
        ));
    }
    require_batch(batch_rows)?;
    if rows_per_file == 0 {
        return Err(bench_error(
            "DuckDB Arrow IPC handoff reference requires positive rows_per_file",
        ));
    }
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    if staging_dir.exists() {
        fs::remove_dir_all(staging_dir)?;
    }
    fs::create_dir_all(staging_dir)?;
    remove_if_exists(output)?;
    remove_if_exists(&duckdb_wal_path(output))?;

    let mut remaining = rows;
    let mut row_start = 1_u64;
    let mut logical_bytes = 0_u64;
    let mut physical_input_bytes = 0_u64;
    let mut paths = Vec::new();
    let schema = Arc::new(tlc_arrow_schema(options.include_row_key));
    let mut file_index = 0_u64;
    while remaining > 0 {
        let file_rows = remaining.min(rows_per_file);
        let path = staging_dir.join(format!("part-{file_index:05}.arrow"));
        let write = write_tlc_arrow_ipc_file(
            &path,
            schema.clone(),
            file_rows,
            batch_rows,
            row_start,
            options.include_row_key,
            options.compression,
        )?;
        logical_bytes = logical_bytes.saturating_add(write.logical_bytes);
        physical_input_bytes = physical_input_bytes.saturating_add(write.physical_bytes);
        paths.push(path);
        row_start = row_start
            .checked_add(file_rows)
            .ok_or_else(|| bench_error("DuckDB Arrow IPC handoff row offset overflowed"))?;
        remaining -= file_rows;
        file_index += 1;
    }

    let connection = open_duckdb_arrow_database(output, options.extension)?;
    load_duckdb_arrow_extension(&connection, options.extension)?;
    connection.execute_batch(&format!(
        "CREATE TABLE arrow_ipc_handoff AS SELECT * FROM read_arrow({})",
        duckdb_path_list(&paths)
    ))?;
    if options.checkpoint {
        connection.execute_batch("CHECKPOINT")?;
    }
    let observed_rows =
        connection.query_row("SELECT count(*) FROM arrow_ipc_handoff", [], |row| {
            row.get::<_, i64>(0)
        })?;
    let observed_rows = u64::try_from(observed_rows)?;
    if observed_rows != rows {
        return Err(bench_error(format!(
            "DuckDB Arrow IPC handoff row count mismatch: expected {rows}, observed {observed_rows}"
        )));
    }
    Ok(WorkerMeasurement {
        timed_wall_time_ns: None,
        rows,
        logical_bytes,
        physical_bytes: physical_input_bytes.saturating_add(duckdb_database_bytes(output)?),
        spill_bytes: 0,
        phases: Vec::new(),
    })
}

struct ArrowIpcWriteMeasurement {
    logical_bytes: u64,
    physical_bytes: u64,
}

fn write_tlc_arrow_ipc_file(
    path: &Path,
    schema: SchemaRef,
    rows: u64,
    batch_rows: usize,
    row_start: u64,
    include_row_key: bool,
    compression: ArrowIpcCompression,
) -> BenchResult<ArrowIpcWriteMeasurement> {
    let file = fs::File::create(path)?;
    let mut output = BufWriter::with_capacity(1024 * 1024, file);
    let options = match compression {
        ArrowIpcCompression::None => IpcWriteOptions::default(),
        ArrowIpcCompression::Lz4Frame => {
            IpcWriteOptions::default().try_with_compression(Some(IpcCompressionType::LZ4_FRAME))?
        }
    };
    let mut writer = IpcFileWriter::try_new_with_options(&mut output, schema.as_ref(), options)?;
    let mut remaining = rows;
    let mut current_row = row_start;
    let mut logical_bytes = 0_u64;
    while remaining > 0 {
        let current_rows = usize::try_from(remaining.min(batch_rows as u64))?;
        let batch = tlc_arrow_batch(current_rows, current_row, include_row_key)?;
        logical_bytes = logical_bytes.saturating_add(u64::try_from(batch.get_array_memory_size())?);
        writer.write(&batch)?;
        current_row = current_row
            .checked_add(u64::try_from(current_rows)?)
            .ok_or_else(|| bench_error("DuckDB Arrow IPC handoff batch row offset overflowed"))?;
        remaining -= u64::try_from(current_rows)?;
    }
    writer.finish()?;
    drop(writer);
    output.flush()?;
    Ok(ArrowIpcWriteMeasurement {
        logical_bytes,
        physical_bytes: fs::metadata(path)?.len(),
    })
}

#[derive(Clone, Copy)]
struct DuckDbParquetStagedOptions {
    row_group_rows: usize,
    row_group_bytes: usize,
    include_row_key: bool,
    checkpoint: bool,
}

fn run_duckdb_parquet_staged_ingest(
    output: &Path,
    staging: &Path,
    rows: u64,
    batch_rows: usize,
    options: DuckDbParquetStagedOptions,
) -> BenchResult<WorkerMeasurement> {
    if rows == 0 {
        return Err(bench_error(
            "DuckDB Parquet staged ingest reference requires at least one row",
        ));
    }
    require_batch(batch_rows)?;
    if options.row_group_rows == 0 {
        return Err(bench_error(
            "DuckDB Parquet staged ingest reference requires positive row_group_rows",
        ));
    }
    if options.row_group_bytes == 0 {
        return Err(bench_error(
            "DuckDB Parquet staged ingest reference requires positive row_group_bytes",
        ));
    }
    if let Some(parent) = staging.parent() {
        fs::create_dir_all(parent)?;
    }
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    remove_if_exists(staging)?;
    remove_if_exists(output)?;
    remove_if_exists(&duckdb_wal_path(output))?;

    let schema = Arc::new(tlc_arrow_schema(options.include_row_key));
    let properties = WriterProperties::builder()
        .set_created_by("cdf benchmark DuckDB Parquet staged ingest reference".to_owned())
        .set_write_batch_size(batch_rows)
        .set_data_page_row_count_limit(batch_rows.min(64 * 1024))
        .set_data_page_size_limit(options.row_group_bytes.min(8 * 1024 * 1024))
        .set_max_row_group_row_count(Some(options.row_group_rows))
        .set_max_row_group_bytes(Some(options.row_group_bytes))
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .build();
    let file = fs::File::create(staging)?;
    let mut output_writer = BufWriter::with_capacity(1024 * 1024, file);
    let mut writer = ArrowWriter::try_new(&mut output_writer, schema, Some(properties))?;
    let mut remaining = rows;
    let mut row_start = 1_u64;
    let mut logical_bytes = 0_u64;
    while remaining > 0 {
        let current_rows = usize::try_from(remaining.min(batch_rows as u64))?;
        let batch = tlc_arrow_batch(current_rows, row_start, options.include_row_key)?;
        logical_bytes = logical_bytes.saturating_add(u64::try_from(batch.get_array_memory_size())?);
        writer.write(&batch)?;
        row_start = row_start
            .checked_add(u64::try_from(current_rows)?)
            .ok_or_else(|| bench_error("DuckDB Parquet staged ingest row offset overflowed"))?;
        remaining -= u64::try_from(current_rows)?;
    }
    writer.close()?;
    output_writer.flush()?;

    let connection = duckdb::Connection::open(output)?;
    connection.execute_batch(&format!(
        "CREATE TABLE parquet_stage AS SELECT * FROM read_parquet({})",
        duckdb_string_literal(staging)
    ))?;
    if options.checkpoint {
        connection.execute_batch("CHECKPOINT")?;
    }
    let observed_rows = connection.query_row("SELECT count(*) FROM parquet_stage", [], |row| {
        row.get::<_, i64>(0)
    })?;
    let observed_rows = u64::try_from(observed_rows)?;
    if observed_rows != rows {
        return Err(bench_error(format!(
            "DuckDB Parquet staged ingest row count mismatch: expected {rows}, observed {observed_rows}"
        )));
    }
    Ok(WorkerMeasurement {
        timed_wall_time_ns: None,
        rows,
        logical_bytes,
        physical_bytes: fs::metadata(staging)?
            .len()
            .saturating_add(duckdb_database_bytes(output)?),
        spill_bytes: 0,
        phases: Vec::new(),
    })
}

fn tlc_duckdb_columns() -> Vec<(&'static str, &'static str)> {
    vec![
        ("vendor_id", "INTEGER"),
        ("tpep_pickup_datetime", "TIMESTAMP"),
        ("tpep_dropoff_datetime", "TIMESTAMP"),
        ("passenger_count", "BIGINT"),
        ("trip_distance", "DOUBLE"),
        ("ratecode_id", "BIGINT"),
        ("store_and_fwd_flag", "VARCHAR"),
        ("pu_location_id", "INTEGER"),
        ("do_location_id", "INTEGER"),
        ("payment_type", "BIGINT"),
        ("fare_amount", "DOUBLE"),
        ("extra", "DOUBLE"),
        ("mta_tax", "DOUBLE"),
        ("tip_amount", "DOUBLE"),
        ("tolls_amount", "DOUBLE"),
        ("improvement_surcharge", "DOUBLE"),
        ("total_amount", "DOUBLE"),
        ("congestion_surcharge", "DOUBLE"),
        ("airport_fee", "DOUBLE"),
    ]
}

fn tlc_arrow_schema(include_row_key: bool) -> Schema {
    let mut fields = vec![
        Field::new("vendor_id", DataType::Int32, false),
        Field::new(
            "tpep_pickup_datetime",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new(
            "tpep_dropoff_datetime",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("passenger_count", DataType::Int64, false),
        Field::new("trip_distance", DataType::Float64, false),
        Field::new("ratecode_id", DataType::Int64, false),
        Field::new("store_and_fwd_flag", DataType::Utf8, false),
        Field::new("pu_location_id", DataType::Int32, false),
        Field::new("do_location_id", DataType::Int32, false),
        Field::new("payment_type", DataType::Int64, false),
    ];
    fields.extend(
        [
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "total_amount",
            "congestion_surcharge",
            "airport_fee",
        ]
        .map(|name| Field::new(name, DataType::Float64, false)),
    );
    if include_row_key {
        fields.push(Field::new(
            cdf_dest_duckdb::CDF_ROW_KEY_COLUMN,
            DataType::UInt64,
            false,
        ));
    }
    Schema::new(fields)
}

fn tlc_arrow_columns(rows: usize, row_key_start: u64) -> Vec<ArrayRef> {
    let int32 = || {
        Arc::new(Int32Array::from_iter_values((0..rows).map(|row| {
            let absolute = row_key_start.saturating_add(row as u64);
            (absolute % 265) as i32
        }))) as ArrayRef
    };
    let int64 = || {
        Arc::new(Int64Array::from_iter_values((0..rows).map(|row| {
            let absolute = row_key_start.saturating_add(row as u64);
            (absolute % 8) as i64
        }))) as ArrayRef
    };
    let float64 = || {
        Arc::new(Float64Array::from_iter_values((0..rows).map(|row| {
            let absolute = row_key_start.saturating_add(row as u64);
            (absolute % 10_000) as f64 / 100.0
        }))) as ArrayRef
    };
    let timestamp = || {
        Arc::new(TimestampMicrosecondArray::from_iter_values(
            (0..rows).map(|row| 1_704_067_200_000_000_i64.saturating_add(row as i64)),
        )) as ArrayRef
    };
    let mut columns = vec![
        int32(),
        timestamp(),
        timestamp(),
        int64(),
        float64(),
        int64(),
        Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            "N", rows,
        ))) as ArrayRef,
        int32(),
        int32(),
        int64(),
    ];
    columns.extend((0..9).map(|_| float64()));
    columns
}

fn tlc_arrow_batch(
    rows: usize,
    row_key_start: u64,
    include_row_key: bool,
) -> BenchResult<arrow_array::RecordBatch> {
    let row_key_end = row_key_start
        .checked_add(u64::try_from(rows)?)
        .ok_or_else(|| bench_error("TLC benchmark row key overflowed"))?;
    let mut columns = tlc_arrow_columns(rows, row_key_start);
    if include_row_key {
        columns
            .push(Arc::new(UInt64Array::from_iter_values(row_key_start..row_key_end)) as ArrayRef);
    }
    arrow_array::RecordBatch::try_new(Arc::new(tlc_arrow_schema(include_row_key)), columns)
        .map_err(Into::into)
}

struct TlcArrowBatchReader {
    schema: SchemaRef,
    remaining: u64,
    batch_rows: usize,
    row_start: u64,
    include_row_key: bool,
    logical_bytes: Arc<AtomicU64>,
}

impl TlcArrowBatchReader {
    fn new(
        rows: u64,
        batch_rows: usize,
        include_row_key: bool,
        logical_bytes: Arc<AtomicU64>,
    ) -> Self {
        Self {
            schema: Arc::new(tlc_arrow_schema(include_row_key)),
            remaining: rows,
            batch_rows,
            row_start: 1,
            include_row_key,
            logical_bytes,
        }
    }
}

impl Iterator for TlcArrowBatchReader {
    type Item = Result<arrow_array::RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let current_rows = match usize::try_from(self.remaining.min(self.batch_rows as u64)) {
            Ok(value) => value,
            Err(error) => return Some(Err(ArrowError::ComputeError(error.to_string()))),
        };
        let batch = match tlc_arrow_batch(current_rows, self.row_start, self.include_row_key) {
            Ok(batch) => batch,
            Err(error) => return Some(Err(ArrowError::ComputeError(error.to_string()))),
        };
        match u64::try_from(current_rows)
            .ok()
            .and_then(|rows| self.row_start.checked_add(rows))
        {
            Some(next_start) => self.row_start = next_start,
            None => {
                return Some(Err(ArrowError::ComputeError(
                    "TLC Arrow stream-scan row offset overflowed".to_owned(),
                )));
            }
        }
        self.remaining -= current_rows as u64;
        let batch_bytes = match u64::try_from(batch.get_array_memory_size()) {
            Ok(value) => value,
            Err(error) => return Some(Err(ArrowError::ComputeError(error.to_string()))),
        };
        self.logical_bytes.fetch_add(batch_bytes, Ordering::Relaxed);
        Some(Ok(batch))
    }
}

impl RecordBatchReader for TlcArrowBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

type DuckArrayRef = Arc<dyn duckdb::arrow::array::Array>;

fn tlc_duckdb_arrow_batch(
    rows: usize,
    row_key_start: u64,
    include_row_key: bool,
) -> BenchResult<duckdb::arrow::record_batch::RecordBatch> {
    use duckdb::arrow::{
        array::{
            Float64Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
            UInt64Array,
        },
        datatypes::{DataType, Field, Schema, TimeUnit},
        record_batch::RecordBatch,
    };

    let mut fields = vec![
        Field::new("vendor_id", DataType::Int32, false),
        Field::new(
            "tpep_pickup_datetime",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new(
            "tpep_dropoff_datetime",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("passenger_count", DataType::Int64, false),
        Field::new("trip_distance", DataType::Float64, false),
        Field::new("ratecode_id", DataType::Int64, false),
        Field::new("store_and_fwd_flag", DataType::Utf8, false),
        Field::new("pu_location_id", DataType::Int32, false),
        Field::new("do_location_id", DataType::Int32, false),
        Field::new("payment_type", DataType::Int64, false),
    ];
    fields.extend(
        [
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "total_amount",
            "congestion_surcharge",
            "airport_fee",
        ]
        .map(|name| Field::new(name, DataType::Float64, false)),
    );
    let int32 = || {
        Arc::new(Int32Array::from_iter_values((0..rows).map(|row| {
            let absolute = row_key_start.saturating_add(row as u64);
            (absolute % 265) as i32
        }))) as DuckArrayRef
    };
    let int64 = || {
        Arc::new(Int64Array::from_iter_values((0..rows).map(|row| {
            let absolute = row_key_start.saturating_add(row as u64);
            (absolute % 8) as i64
        }))) as DuckArrayRef
    };
    let float64 = || {
        Arc::new(Float64Array::from_iter_values((0..rows).map(|row| {
            let absolute = row_key_start.saturating_add(row as u64);
            (absolute % 10_000) as f64 / 100.0
        }))) as DuckArrayRef
    };
    let timestamp = || {
        Arc::new(TimestampMicrosecondArray::from_iter_values(
            (0..rows).map(|row| 1_704_067_200_000_000_i64.saturating_add(row as i64)),
        )) as DuckArrayRef
    };
    let mut columns = vec![
        int32(),
        timestamp(),
        timestamp(),
        int64(),
        float64(),
        int64(),
        Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            "N", rows,
        ))) as DuckArrayRef,
        int32(),
        int32(),
        int64(),
    ];
    columns.extend((0..9).map(|_| float64()));
    if include_row_key {
        let row_key_end = row_key_start
            .checked_add(u64::try_from(rows)?)
            .ok_or_else(|| bench_error("DuckDB Arrow append row key overflowed"))?;
        fields.push(Field::new(
            cdf_dest_duckdb::CDF_ROW_KEY_COLUMN,
            DataType::UInt64,
            false,
        ));
        columns.push(
            Arc::new(UInt64Array::from_iter_values(row_key_start..row_key_end)) as DuckArrayRef,
        );
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(Into::into)
}

struct RawDuckDbConnection {
    database: duckdb::ffi::duckdb_database,
    connection: duckdb::ffi::duckdb_connection,
}

struct ReferenceDuckDbProfileCapture {
    output_path: PathBuf,
    scratch_path: PathBuf,
}

impl ReferenceDuckDbProfileCapture {
    fn start(
        connection: &mut RawDuckDbConnection,
        directory: Option<&Path>,
        scan_threads: usize,
    ) -> BenchResult<Option<Self>> {
        let Some(directory) = directory else {
            return Ok(None);
        };
        fs::create_dir_all(directory)?;
        let started_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let name = format!(
            "duckdb-reference-p{}-{started_ns}-scan-{scan_threads}.json",
            std::process::id()
        );
        let output_path = directory.join(&name);
        let scratch_path = directory.join(format!(".{name}.capture.json"));
        connection.query(&format!(
            "CALL enable_profiling(format := 'json', save_location := {}, coverage := 'all', mode := 'detailed')",
            duckdb_string_literal(&scratch_path)
        ))?;
        Ok(Some(Self {
            output_path,
            scratch_path,
        }))
    }

    fn finish(self, connection: &mut RawDuckDbConnection) -> BenchResult<PathBuf> {
        let capture = (|| -> BenchResult<Vec<u8>> {
            let bytes = fs::read(&self.scratch_path)?;
            serde_json::from_slice::<serde_json::Value>(&bytes)?;
            Ok(bytes)
        })();
        let disable = connection.query("CALL disable_profiling()");
        let _ = fs::remove_file(&self.scratch_path);
        let bytes = match (capture, disable) {
            (Ok(bytes), Ok(())) => bytes,
            (Err(error), Ok(())) | (Ok(_), Err(error)) => return Err(error),
            (Err(error), Err(disable_error)) => {
                return Err(bench_error(format!(
                    "{error}; DuckDB reference profiler cleanup also failed: {disable_error}"
                )));
            }
        };
        let publish_path = self.output_path.with_extension("json.publish");
        fs::write(&publish_path, bytes)?;
        fs::rename(&publish_path, &self.output_path)?;
        Ok(self.output_path)
    }
}

impl RawDuckDbConnection {
    fn open(path: &Path) -> BenchResult<Self> {
        let path = CString::new(path.display().to_string())?;
        let mut database = ptr::null_mut();
        let mut connection = ptr::null_mut();
        // SAFETY: `path` is a live NUL-terminated string for this call, and
        // DuckDB initializes the output handles or reports an error. The
        // wrapper owns successful handles and releases them in `Drop`.
        let open_state = unsafe { duckdb::ffi::duckdb_open(path.as_ptr(), &mut database) };
        if open_state != duckdb::ffi::DuckDBSuccess {
            return Err(bench_error("DuckDB raw open failed"));
        }
        // SAFETY: `database` is a valid handle returned by `duckdb_open`.
        let connect_state = unsafe { duckdb::ffi::duckdb_connect(database, &mut connection) };
        if connect_state != duckdb::ffi::DuckDBSuccess {
            // SAFETY: `database` was returned by `duckdb_open` and has not
            // been closed yet.
            unsafe {
                duckdb::ffi::duckdb_close(&mut database);
            }
            return Err(bench_error("DuckDB raw connect failed"));
        }
        Ok(Self {
            database,
            connection,
        })
    }

    fn handle(&mut self) -> duckdb::ffi::duckdb_connection {
        self.connection
    }

    fn query(&mut self, sql: &str) -> BenchResult<()> {
        let sql = CString::new(sql)?;
        let mut result = unsafe { std::mem::zeroed::<duckdb::ffi::duckdb_result>() };
        // SAFETY: the connection is owned by this wrapper and `sql` is a live
        // NUL-terminated string. DuckDB initializes `result`; it is destroyed
        // below on every path as required by the C API.
        let state =
            unsafe { duckdb::ffi::duckdb_query(self.connection, sql.as_ptr(), &mut result) };
        let error = if state == duckdb::ffi::DuckDBSuccess {
            None
        } else {
            Some(duckdb_result_error_message(&mut result))
        };
        // SAFETY: `duckdb_query` requires result destruction even when the
        // state is an error.
        unsafe {
            duckdb::ffi::duckdb_destroy_result(&mut result);
        }
        match error {
            Some(message) => Err(bench_error(format!("DuckDB raw query failed: {message}"))),
            None => Ok(()),
        }
    }
}

impl Drop for RawDuckDbConnection {
    fn drop(&mut self) {
        // SAFETY: both handles are owned by this wrapper and DuckDB accepts
        // null handles, so double-drop is avoided by setting them to null.
        unsafe {
            if !self.connection.is_null() {
                duckdb::ffi::duckdb_disconnect(&mut self.connection);
            }
            if !self.database.is_null() {
                duckdb::ffi::duckdb_close(&mut self.database);
            }
        }
    }
}

struct DuckDbArrowConvertedSchema {
    schema: duckdb::ffi::duckdb_arrow_converted_schema,
}

impl DuckDbArrowConvertedSchema {
    fn from_arrow(
        connection: duckdb::ffi::duckdb_connection,
        schema: &Schema,
    ) -> BenchResult<Self> {
        assert_arrow_duckdb_c_data_layout();
        let mut arrow_schema = FFI_ArrowSchema::try_from(schema)?;
        let mut converted_schema = ptr::null_mut();
        // SAFETY: arrow-rs and libduckdb-sys define ABI-identical C Data
        // Interface schemas; the assertion above guards size/alignment. The
        // Arrow schema stays alive for the call and is released by its Drop
        // implementation afterward. DuckDB returns a converted schema owned by
        // this wrapper.
        let error = unsafe {
            duckdb::ffi::duckdb_schema_from_arrow(
                connection,
                (&mut arrow_schema as *mut FFI_ArrowSchema).cast::<duckdb::ffi::ArrowSchema>(),
                &mut converted_schema,
            )
        };
        duckdb_error_data_result(error, "DuckDB Arrow schema conversion")?;
        if converted_schema.is_null() {
            return Err(bench_error(
                "DuckDB Arrow schema conversion returned a null converted schema",
            ));
        }
        Ok(Self {
            schema: converted_schema,
        })
    }

    fn handle(&mut self) -> duckdb::ffi::duckdb_arrow_converted_schema {
        self.schema
    }

    fn shared_handle(&self) -> duckdb::ffi::duckdb_arrow_converted_schema {
        self.schema
    }
}

impl Drop for DuckDbArrowConvertedSchema {
    fn drop(&mut self) {
        // SAFETY: the converted schema is owned by this wrapper and is
        // destroyed exactly once.
        unsafe {
            if !self.schema.is_null() {
                duckdb::ffi::duckdb_destroy_arrow_converted_schema(&mut self.schema);
            }
        }
    }
}

struct RawDuckDbAppender {
    appender: duckdb::ffi::duckdb_appender,
}

impl RawDuckDbAppender {
    fn create(connection: duckdb::ffi::duckdb_connection, table: &str) -> BenchResult<Self> {
        let table = CString::new(table)?;
        let mut appender = ptr::null_mut();
        // SAFETY: `connection` is live for the appender lifetime, the default
        // schema pointer is null by DuckDB contract, and `table` is a live
        // NUL-terminated string for this call.
        let state = unsafe {
            duckdb::ffi::duckdb_appender_create(
                connection,
                ptr::null(),
                table.as_ptr(),
                &mut appender,
            )
        };
        if state != duckdb::ffi::DuckDBSuccess {
            return Err(bench_error("DuckDB raw appender creation failed"));
        }
        if appender.is_null() {
            return Err(bench_error("DuckDB raw appender creation returned null"));
        }
        Ok(Self { appender })
    }

    fn handle(&mut self) -> duckdb::ffi::duckdb_appender {
        self.appender
    }

    fn flush(&mut self) -> BenchResult<()> {
        // SAFETY: this wrapper owns a live appender handle.
        let state = unsafe { duckdb::ffi::duckdb_appender_flush(self.appender) };
        if state == duckdb::ffi::DuckDBSuccess {
            Ok(())
        } else {
            Err(bench_error(format!(
                "DuckDB raw appender flush failed: {}",
                self.error_message()
            )))
        }
    }

    fn error_message(&self) -> String {
        // SAFETY: this wrapper owns a live appender handle; DuckDB owns the
        // returned error data and the helper destroys it.
        unsafe {
            let error = duckdb::ffi::duckdb_appender_error_data(self.appender);
            duckdb_error_data_message_take(error).unwrap_or_else(|| "unknown error".to_owned())
        }
    }
}

impl Drop for RawDuckDbAppender {
    fn drop(&mut self) {
        // SAFETY: the appender is owned by this wrapper and is destroyed once.
        unsafe {
            if !self.appender.is_null() {
                let _ = duckdb::ffi::duckdb_appender_destroy(&mut self.appender);
            }
        }
    }
}

fn append_arrow_batch_as_duckdb_data_chunk(
    connection: duckdb::ffi::duckdb_connection,
    appender: duckdb::ffi::duckdb_appender,
    converted_schema: duckdb::ffi::duckdb_arrow_converted_schema,
    batch: arrow_array::RecordBatch,
) -> BenchResult<()> {
    assert_arrow_duckdb_c_data_layout();
    let struct_array = StructArray::from(batch);
    let mut arrow_array = ManuallyDrop::new(FFI_ArrowArray::new(&struct_array.to_data()));
    let mut chunk = ptr::null_mut();
    // SAFETY: arrow-rs and libduckdb-sys define ABI-identical C Data Interface
    // arrays. DuckDB takes ownership of the exported Arrow array's private
    // data on successful conversion; the `ManuallyDrop` prevents Rust from
    // releasing it prematurely. The resulting DuckDB data chunk is destroyed
    // after append.
    let error = unsafe {
        duckdb::ffi::duckdb_data_chunk_from_arrow(
            connection,
            (&mut *arrow_array as *mut FFI_ArrowArray).cast::<duckdb::ffi::ArrowArray>(),
            converted_schema,
            &mut chunk,
        )
    };
    match duckdb_error_data_result(error, "DuckDB Arrow data-chunk conversion") {
        Ok(()) => {}
        Err(error) => {
            // SAFETY: conversion failed, so this benchmark keeps ownership of
            // the exported Arrow array and must release it.
            unsafe {
                ManuallyDrop::drop(&mut arrow_array);
            }
            return Err(error);
        }
    }
    if chunk.is_null() {
        return Err(bench_error(
            "DuckDB Arrow data-chunk conversion returned a null chunk",
        ));
    }
    // SAFETY: `appender` and `chunk` are live handles. DuckDB appends from the
    // chunk before it is destroyed below.
    let append_state = unsafe { duckdb::ffi::duckdb_append_data_chunk(appender, chunk) };
    // SAFETY: `chunk` is owned by this function after successful conversion.
    unsafe {
        duckdb::ffi::duckdb_destroy_data_chunk(&mut chunk);
    }
    if append_state == duckdb::ffi::DuckDBSuccess {
        Ok(())
    } else {
        Err(bench_error("DuckDB raw data-chunk append failed"))
    }
}

fn configure_duckdb_parallel_scan(
    connection: &mut RawDuckDbConnection,
    threads: Option<i64>,
    memory_limit_bytes: Option<u64>,
    temp_directory_budget_bytes: Option<u64>,
) -> BenchResult<()> {
    if let Some(threads) = threads {
        if threads <= 0 {
            return Err(bench_error(
                "DuckDB parallel-scan reference duckdb_threads must be positive",
            ));
        }
        connection.query(&format!("SET threads = {threads}"))?;
    }
    if let Some(bytes) = memory_limit_bytes {
        if bytes == 0 {
            return Err(bench_error(
                "DuckDB parallel-scan reference duckdb_memory_limit_bytes must be positive",
            ));
        }
        connection.query(&format!("SET memory_limit = '{}B'", bytes))?;
    }
    if let Some(bytes) = temp_directory_budget_bytes {
        if bytes == 0 {
            return Err(bench_error(
                "DuckDB parallel-scan reference duckdb_temp_directory_budget_bytes must be positive",
            ));
        }
        connection.query(&format!("SET max_temp_directory_size = '{}B'", bytes))?;
    }
    connection.query("SET preserve_insertion_order = false")
}

struct DuckDbIpcTableFunctionTelemetry {
    rows: AtomicU64,
    logical_bytes: AtomicU64,
}

struct DuckDbIpcTableFunctionContext {
    paths: Vec<PathBuf>,
    schema: SchemaRef,
    connection: duckdb::ffi::duckdb_connection,
    converted_schema: DuckDbArrowConvertedSchema,
    next_path: AtomicUsize,
    max_threads: usize,
    telemetry: Arc<DuckDbIpcTableFunctionTelemetry>,
}

struct DuckDbIpcTableFunctionLocalState {
    reader: Option<IpcFileReader<BufReader<fs::File>>>,
    batch: Option<arrow_array::RecordBatch>,
    batch_offset: usize,
}

impl DuckDbIpcTableFunctionLocalState {
    fn new() -> Self {
        Self {
            reader: None,
            batch: None,
            batch_offset: 0,
        }
    }

    fn next_slice(
        &mut self,
        context: &DuckDbIpcTableFunctionContext,
        vector_rows: usize,
    ) -> BenchResult<Option<arrow_array::RecordBatch>> {
        loop {
            if let Some(batch) = self.batch.as_ref() {
                if self.batch_offset < batch.num_rows() {
                    let rows = vector_rows.min(batch.num_rows() - self.batch_offset);
                    let slice = batch.slice(self.batch_offset, rows);
                    self.batch_offset += rows;
                    return Ok(Some(slice));
                }
                self.batch = None;
                self.batch_offset = 0;
            }

            if let Some(reader) = self.reader.as_mut() {
                match reader.next() {
                    Some(Ok(batch)) => {
                        if batch.schema().as_ref() != context.schema.as_ref() {
                            return Err(bench_error(
                                "DuckDB Arrow IPC table-function encountered a segment batch schema that differs from the bound canonical schema",
                            ));
                        }
                        let batch_rows = u64::try_from(batch.num_rows())?;
                        let batch_bytes = u64::try_from(batch.get_array_memory_size())?;
                        context
                            .telemetry
                            .rows
                            .fetch_add(batch_rows, Ordering::Relaxed);
                        context
                            .telemetry
                            .logical_bytes
                            .fetch_add(batch_bytes, Ordering::Relaxed);
                        self.batch = Some(batch);
                        continue;
                    }
                    Some(Err(error)) => return Err(error.into()),
                    None => self.reader = None,
                }
            }

            let path_index = context.next_path.fetch_add(1, Ordering::Relaxed);
            let Some(path) = context.paths.get(path_index) else {
                return Ok(None);
            };
            let reader = IpcFileReader::try_new_buffered(fs::File::open(path)?, None)?;
            if reader.schema().as_ref() != context.schema.as_ref() {
                return Err(bench_error(format!(
                    "DuckDB Arrow IPC table-function segment {} does not match the bound canonical schema",
                    path.display()
                )));
            }
            self.reader = Some(reader);
        }
    }
}

struct DuckDbLogicalType {
    handle: duckdb::ffi::duckdb_logical_type,
}

impl DuckDbLogicalType {
    fn from_arrow(data_type: &DataType) -> BenchResult<Self> {
        use duckdb::ffi::{
            DUCKDB_TYPE_DUCKDB_TYPE_BIGINT, DUCKDB_TYPE_DUCKDB_TYPE_BLOB,
            DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN, DUCKDB_TYPE_DUCKDB_TYPE_DATE,
            DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE, DUCKDB_TYPE_DUCKDB_TYPE_FLOAT,
            DUCKDB_TYPE_DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT,
            DUCKDB_TYPE_DUCKDB_TYPE_TIME, DUCKDB_TYPE_DUCKDB_TYPE_TIME_NS,
            DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP, DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS,
            DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS, DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S,
            DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ, DUCKDB_TYPE_DUCKDB_TYPE_TINYINT,
            DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER,
            DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT, DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT,
            DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
        };

        let primitive = match data_type {
            DataType::Boolean => Some(DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN),
            DataType::Int8 => Some(DUCKDB_TYPE_DUCKDB_TYPE_TINYINT),
            DataType::Int16 => Some(DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT),
            DataType::Int32 => Some(DUCKDB_TYPE_DUCKDB_TYPE_INTEGER),
            DataType::Int64 => Some(DUCKDB_TYPE_DUCKDB_TYPE_BIGINT),
            DataType::UInt8 => Some(DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT),
            DataType::UInt16 => Some(DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT),
            DataType::UInt32 => Some(DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER),
            DataType::UInt64 => Some(DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT),
            DataType::Float32 => Some(DUCKDB_TYPE_DUCKDB_TYPE_FLOAT),
            DataType::Float64 => Some(DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                Some(DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR)
            }
            DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_) => Some(DUCKDB_TYPE_DUCKDB_TYPE_BLOB),
            DataType::Date32 => Some(DUCKDB_TYPE_DUCKDB_TYPE_DATE),
            DataType::Time32(_) | DataType::Time64(TimeUnit::Microsecond) => {
                Some(DUCKDB_TYPE_DUCKDB_TYPE_TIME)
            }
            DataType::Time64(TimeUnit::Nanosecond) => Some(DUCKDB_TYPE_DUCKDB_TYPE_TIME_NS),
            DataType::Timestamp(unit, timezone) => Some(if timezone.is_some() {
                DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ
            } else {
                match unit {
                    TimeUnit::Second => DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S,
                    TimeUnit::Millisecond => DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS,
                    TimeUnit::Microsecond => DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP,
                    TimeUnit::Nanosecond => DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS,
                }
            }),
            DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
                if *precision > 38 || *scale < 0 || *scale > i8::try_from(*precision)? {
                    return Err(bench_error(format!(
                        "DuckDB Arrow IPC table-function cannot bind Arrow decimal({precision},{scale})"
                    )));
                }
                // SAFETY: validated DuckDB decimal width/scale are passed by value;
                // the returned logical type is owned by this wrapper.
                let handle = unsafe {
                    duckdb::ffi::duckdb_create_decimal_type(*precision, u8::try_from(*scale)?)
                };
                return Self::from_handle(handle, data_type);
            }
            _ => None,
        };
        let primitive = primitive.ok_or_else(|| {
            bench_error(format!(
                "DuckDB Arrow IPC table-function does not support Arrow type {data_type:?} in this falsification"
            ))
        })?;
        // SAFETY: `primitive` is a supported primitive DuckDB type; the
        // returned logical type is owned by this wrapper.
        let handle = unsafe { duckdb::ffi::duckdb_create_logical_type(primitive) };
        Self::from_handle(handle, data_type)
    }

    fn from_handle(
        handle: duckdb::ffi::duckdb_logical_type,
        data_type: &DataType,
    ) -> BenchResult<Self> {
        if handle.is_null() {
            Err(bench_error(format!(
                "DuckDB logical type creation returned null for Arrow type {data_type:?}"
            )))
        } else {
            Ok(Self { handle })
        }
    }
}

impl Drop for DuckDbLogicalType {
    fn drop(&mut self) {
        // SAFETY: this wrapper owns the logical type and destroys it once.
        unsafe {
            if !self.handle.is_null() {
                duckdb::ffi::duckdb_destroy_logical_type(&mut self.handle);
            }
        }
    }
}

fn register_duckdb_arrow_ipc_table_function(
    connection: duckdb::ffi::duckdb_connection,
    name: &str,
    paths: &[PathBuf],
    schema: SchemaRef,
    max_threads: usize,
) -> BenchResult<Arc<DuckDbIpcTableFunctionTelemetry>> {
    assert_arrow_duckdb_c_data_layout();
    let telemetry = Arc::new(DuckDbIpcTableFunctionTelemetry {
        rows: AtomicU64::new(0),
        logical_bytes: AtomicU64::new(0),
    });
    let converted_schema = DuckDbArrowConvertedSchema::from_arrow(connection, schema.as_ref())?;
    let context = Box::new(DuckDbIpcTableFunctionContext {
        paths: paths.to_vec(),
        schema,
        connection,
        converted_schema,
        next_path: AtomicUsize::new(0),
        max_threads,
        telemetry: telemetry.clone(),
    });
    let name = CString::new(name)?;
    // SAFETY: all callback pointers have the C ABI and remain valid for the
    // process lifetime. The table-function object owns `context` through its
    // registered destructor. DuckDB copies the registered function into its
    // catalog before the temporary registration object is destroyed.
    unsafe {
        let mut table_function = duckdb::ffi::duckdb_create_table_function();
        if table_function.is_null() {
            return Err(bench_error("DuckDB table-function creation returned null"));
        }
        duckdb::ffi::duckdb_table_function_set_name(table_function, name.as_ptr());
        duckdb::ffi::duckdb_table_function_set_extra_info(
            table_function,
            Box::into_raw(context).cast::<c_void>(),
            Some(drop_duckdb_ipc_table_function_context),
        );
        duckdb::ffi::duckdb_table_function_set_bind(
            table_function,
            Some(bind_duckdb_ipc_table_function),
        );
        duckdb::ffi::duckdb_table_function_set_init(
            table_function,
            Some(init_duckdb_ipc_table_function),
        );
        duckdb::ffi::duckdb_table_function_set_local_init(
            table_function,
            Some(local_init_duckdb_ipc_table_function),
        );
        duckdb::ffi::duckdb_table_function_set_function(
            table_function,
            Some(scan_duckdb_ipc_table_function),
        );
        let state = duckdb::ffi::duckdb_register_table_function(connection, table_function);
        duckdb::ffi::duckdb_destroy_table_function(&mut table_function);
        if state != duckdb::ffi::DuckDBSuccess {
            return Err(bench_error("DuckDB table-function registration failed"));
        }
    }
    Ok(telemetry)
}

unsafe extern "C" fn drop_duckdb_ipc_table_function_context(data: *mut c_void) {
    if !data.is_null() {
        // SAFETY: `data` came from exactly one `Box::into_raw` call during
        // table-function registration and DuckDB invokes this destructor once.
        unsafe {
            drop(Box::from_raw(data.cast::<DuckDbIpcTableFunctionContext>()));
        }
    }
}

unsafe extern "C" fn drop_duckdb_ipc_table_function_local_state(data: *mut c_void) {
    if !data.is_null() {
        // SAFETY: `data` came from exactly one `Box::into_raw` call in the
        // local-init callback and DuckDB invokes this destructor once.
        unsafe {
            drop(Box::from_raw(
                data.cast::<DuckDbIpcTableFunctionLocalState>(),
            ));
        }
    }
}

unsafe extern "C" fn bind_duckdb_ipc_table_function(info: duckdb::ffi::duckdb_bind_info) {
    let result = catch_unwind(AssertUnwindSafe(|| -> BenchResult<()> {
        // SAFETY: DuckDB passes the extra-info pointer installed at
        // registration and keeps it alive for this callback.
        let context = unsafe {
            duckdb_ipc_table_function_context(duckdb::ffi::duckdb_bind_get_extra_info(info))?
        };
        for field in context.schema.fields() {
            let name = CString::new(field.name().as_str())?;
            let logical_type = DuckDbLogicalType::from_arrow(field.data_type())?;
            // SAFETY: `info` is live for this callback; DuckDB copies the
            // column name and logical type into the bind result.
            unsafe {
                duckdb::ffi::duckdb_bind_add_result_column(
                    info,
                    name.as_ptr(),
                    logical_type.handle,
                );
            }
        }
        Ok(())
    }));
    if let Err(message) = ffi_callback_result(result) {
        set_duckdb_bind_error(info, &message);
    }
}

unsafe extern "C" fn init_duckdb_ipc_table_function(info: duckdb::ffi::duckdb_init_info) {
    let result = catch_unwind(AssertUnwindSafe(|| -> BenchResult<()> {
        // SAFETY: DuckDB passes the registered extra-info pointer and keeps it
        // alive for this callback.
        let context = unsafe {
            duckdb_ipc_table_function_context(duckdb::ffi::duckdb_init_get_extra_info(info))?
        };
        let threads = context.max_threads.min(context.paths.len()).max(1);
        // SAFETY: `info` is live for this callback and the value is positive.
        unsafe {
            duckdb::ffi::duckdb_init_set_max_threads(info, u64::try_from(threads)?);
        }
        Ok(())
    }));
    if let Err(message) = ffi_callback_result(result) {
        set_duckdb_init_error(info, &message);
    }
}

unsafe extern "C" fn local_init_duckdb_ipc_table_function(info: duckdb::ffi::duckdb_init_info) {
    let result = catch_unwind(AssertUnwindSafe(|| -> BenchResult<()> {
        let state = Box::new(DuckDbIpcTableFunctionLocalState::new());
        // SAFETY: DuckDB owns the boxed local state after this call and uses
        // the matching destructor exactly once.
        unsafe {
            duckdb::ffi::duckdb_init_set_init_data(
                info,
                Box::into_raw(state).cast::<c_void>(),
                Some(drop_duckdb_ipc_table_function_local_state),
            );
        }
        Ok(())
    }));
    if let Err(message) = ffi_callback_result(result) {
        set_duckdb_init_error(info, &message);
    }
}

unsafe extern "C" fn scan_duckdb_ipc_table_function(
    info: duckdb::ffi::duckdb_function_info,
    output: duckdb::ffi::duckdb_data_chunk,
) {
    let result = catch_unwind(AssertUnwindSafe(|| -> BenchResult<()> {
        // SAFETY: DuckDB supplies both registered pointers for the duration of
        // the callback; each worker receives its own local state.
        let context = unsafe {
            duckdb_ipc_table_function_context(duckdb::ffi::duckdb_function_get_extra_info(info))?
        };
        let local_state = unsafe {
            duckdb_ipc_table_function_local_state(
                duckdb::ffi::duckdb_function_get_local_init_data(info),
            )?
        };
        // SAFETY: this process is linked against DuckDB and the returned
        // vector size is its active output-chunk capacity.
        let vector_rows = usize::try_from(unsafe { duckdb::ffi::duckdb_vector_size() })?;
        let Some(batch) = local_state.next_slice(context, vector_rows)? else {
            // SAFETY: `output` is the live output chunk for this callback.
            unsafe {
                duckdb::ffi::duckdb_data_chunk_set_size(output, 0);
            }
            return Ok(());
        };
        reference_arrow_batch_into_duckdb_output(context, batch, output)
    }));
    if let Err(message) = ffi_callback_result(result) {
        set_duckdb_function_error(info, &message);
    }
}

fn reference_arrow_batch_into_duckdb_output(
    context: &DuckDbIpcTableFunctionContext,
    batch: arrow_array::RecordBatch,
    output: duckdb::ffi::duckdb_data_chunk,
) -> BenchResult<()> {
    assert_arrow_duckdb_c_data_layout();
    let rows = batch.num_rows();
    let struct_array = StructArray::from(batch);
    let mut arrow_array = ManuallyDrop::new(FFI_ArrowArray::new(&struct_array.to_data()));
    let mut converted_chunk = ptr::null_mut();
    // SAFETY: Arrow and DuckDB C Data structs are ABI-checked above. DuckDB
    // takes ownership of the exported Arrow array on successful conversion;
    // `converted_chunk` is destroyed after its vectors are referenced.
    let error = unsafe {
        duckdb::ffi::duckdb_data_chunk_from_arrow(
            context.connection,
            (&mut *arrow_array as *mut FFI_ArrowArray).cast::<duckdb::ffi::ArrowArray>(),
            context.converted_schema.shared_handle(),
            &mut converted_chunk,
        )
    };
    match duckdb_error_data_result(error, "DuckDB table-function Arrow conversion") {
        Ok(()) => {}
        Err(error) => {
            // SAFETY: conversion failed before DuckDB accepted ownership.
            unsafe {
                ManuallyDrop::drop(&mut arrow_array);
            }
            return Err(error);
        }
    }
    if converted_chunk.is_null() {
        return Err(bench_error(
            "DuckDB table-function Arrow conversion returned a null chunk",
        ));
    }
    // SAFETY: both chunks are live and DuckDB returns their column counts.
    let input_columns = unsafe { duckdb::ffi::duckdb_data_chunk_get_column_count(converted_chunk) };
    let output_columns = unsafe { duckdb::ffi::duckdb_data_chunk_get_column_count(output) };
    if input_columns != output_columns {
        // SAFETY: this function owns the converted chunk.
        unsafe {
            duckdb::ffi::duckdb_destroy_data_chunk(&mut converted_chunk);
        }
        return Err(bench_error(format!(
            "DuckDB table-function Arrow conversion produced {input_columns} columns for a {output_columns}-column output"
        )));
    }
    for column in 0..input_columns {
        // SAFETY: `column` is within both chunks' verified column counts.
        unsafe {
            let source = duckdb::ffi::duckdb_data_chunk_get_vector(converted_chunk, column);
            let destination = duckdb::ffi::duckdb_data_chunk_get_vector(output, column);
            duckdb::ffi::duckdb_vector_reference_vector(destination, source);
        }
    }
    // SAFETY: vector references retain shared ownership after the source chunk
    // is destroyed; the output row count does not exceed DuckDB vector size.
    unsafe {
        duckdb::ffi::duckdb_data_chunk_set_size(output, u64::try_from(rows)?);
        duckdb::ffi::duckdb_destroy_data_chunk(&mut converted_chunk);
    }
    Ok(())
}

unsafe fn duckdb_ipc_table_function_context<'a>(
    pointer: *mut c_void,
) -> BenchResult<&'a DuckDbIpcTableFunctionContext> {
    // SAFETY: callers obtain `pointer` from DuckDB's registered extra-info
    // accessor, and the registration owns it for the callback lifetime.
    unsafe { pointer.cast::<DuckDbIpcTableFunctionContext>().as_ref() }.ok_or_else(|| {
        bench_error("DuckDB Arrow IPC table-function callback received null context")
    })
}

unsafe fn duckdb_ipc_table_function_local_state<'a>(
    pointer: *mut c_void,
) -> BenchResult<&'a mut DuckDbIpcTableFunctionLocalState> {
    // SAFETY: callers obtain `pointer` from DuckDB's thread-local init-data
    // accessor; DuckDB gives each worker exclusive callback access.
    unsafe { pointer.cast::<DuckDbIpcTableFunctionLocalState>().as_mut() }.ok_or_else(|| {
        bench_error("DuckDB Arrow IPC table-function callback received null local state")
    })
}

fn ffi_callback_result(
    result: Result<BenchResult<()>, Box<dyn std::any::Any + Send>>,
) -> Result<(), String> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(error.to_string()),
        Err(payload) => Err(if let Some(message) = payload.downcast_ref::<&str>() {
            format!("DuckDB Arrow IPC table-function callback panicked: {message}")
        } else if let Some(message) = payload.downcast_ref::<String>() {
            format!("DuckDB Arrow IPC table-function callback panicked: {message}")
        } else {
            "DuckDB Arrow IPC table-function callback panicked".to_owned()
        }),
    }
}

fn callback_error_message(message: &str) -> CString {
    CString::new(message.replace('\0', "\\0"))
        .expect("replacing NUL bytes produces a valid DuckDB callback error")
}

fn set_duckdb_bind_error(info: duckdb::ffi::duckdb_bind_info, message: &str) {
    let message = callback_error_message(message);
    // SAFETY: `info` is live for the callback and DuckDB copies the message.
    unsafe {
        duckdb::ffi::duckdb_bind_set_error(info, message.as_ptr());
    }
}

fn set_duckdb_init_error(info: duckdb::ffi::duckdb_init_info, message: &str) {
    let message = callback_error_message(message);
    // SAFETY: `info` is live for the callback and DuckDB copies the message.
    unsafe {
        duckdb::ffi::duckdb_init_set_error(info, message.as_ptr());
    }
}

fn set_duckdb_function_error(info: duckdb::ffi::duckdb_function_info, message: &str) {
    let message = callback_error_message(message);
    // SAFETY: `info` is live for the callback and DuckDB copies the message.
    unsafe {
        duckdb::ffi::duckdb_function_set_error(info, message.as_ptr());
    }
}

fn register_duckdb_arrow_stream_scan(
    connection: duckdb::ffi::duckdb_connection,
    view_name: &str,
    stream: &mut FFI_ArrowArrayStream,
) -> BenchResult<()> {
    let view_name = CString::new(view_name)?;
    // SAFETY: this is a lab-only diagnostic around DuckDB's deprecated C API.
    // The bundled DuckDB implementation immediately casts `duckdb_arrow_stream`
    // to `ArrowArrayStream *`, so the arrow-rs C stream is passed with the same
    // ABI pointer value. DuckDB borrows the stream while registering/executing
    // the view; the caller keeps `stream` alive until after CTAS completes and
    // releases it with arrow-rs, not DuckDB's destroy function.
    let state = unsafe {
        duckdb::ffi::duckdb_arrow_scan(
            connection,
            view_name.as_ptr(),
            (stream as *mut FFI_ArrowArrayStream).cast::<duckdb::ffi::_duckdb_arrow_stream>(),
        )
    };
    if state == duckdb::ffi::DuckDBSuccess {
        Ok(())
    } else {
        Err(bench_error("DuckDB Arrow stream-scan registration failed"))
    }
}

fn assert_arrow_duckdb_c_data_layout() {
    assert_eq!(
        size_of::<FFI_ArrowArray>(),
        size_of::<duckdb::ffi::ArrowArray>(),
        "ArrowArray ABI size changed"
    );
    assert_eq!(
        align_of::<FFI_ArrowArray>(),
        align_of::<duckdb::ffi::ArrowArray>(),
        "ArrowArray ABI alignment changed"
    );
    assert_eq!(
        size_of::<FFI_ArrowSchema>(),
        size_of::<duckdb::ffi::ArrowSchema>(),
        "ArrowSchema ABI size changed"
    );
    assert_eq!(
        align_of::<FFI_ArrowSchema>(),
        align_of::<duckdb::ffi::ArrowSchema>(),
        "ArrowSchema ABI alignment changed"
    );
}

fn duckdb_result_error_message(result: *mut duckdb::ffi::duckdb_result) -> String {
    // SAFETY: `result` is the initialized result object from a failed
    // `duckdb_query`; the returned pointer is owned by DuckDB until result
    // destruction.
    let pointer = unsafe { duckdb::ffi::duckdb_result_error(result) };
    cstr_message(pointer)
}

fn duckdb_error_data_result(
    error_data: duckdb::ffi::duckdb_error_data,
    context: &str,
) -> BenchResult<()> {
    // SAFETY: the DuckDB C API returns owned error data for Arrow conversion
    // calls; this helper reads and destroys it exactly once.
    let message = unsafe { duckdb_error_data_message_take(error_data) };
    match message {
        Some(message) => Err(bench_error(format!("{context} failed: {message}"))),
        None => Ok(()),
    }
}

unsafe fn duckdb_error_data_message_take(
    mut error_data: duckdb::ffi::duckdb_error_data,
) -> Option<String> {
    if error_data.is_null() {
        return None;
    }
    // SAFETY: `error_data` is non-null and owned by this helper.
    let has_error = unsafe { duckdb::ffi::duckdb_error_data_has_error(error_data) };
    let message = if has_error {
        // SAFETY: DuckDB owns the returned message pointer until the error data
        // is destroyed below.
        let pointer = unsafe { duckdb::ffi::duckdb_error_data_message(error_data) };
        Some(cstr_message(pointer))
    } else {
        None
    };
    // SAFETY: `error_data` is owned and destroyed once.
    unsafe {
        duckdb::ffi::duckdb_destroy_error_data(&mut error_data);
    }
    message
}

fn cstr_message(pointer: *const c_char) -> String {
    if pointer.is_null() {
        return "unknown error".to_owned();
    }
    // SAFETY: DuckDB returns NUL-terminated diagnostic strings for these APIs.
    unsafe { CStr::from_ptr(pointer) }
        .to_string_lossy()
        .into_owned()
}

fn remove_if_exists(path: &Path) -> BenchResult<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn duckdb_wal_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(".wal");
    PathBuf::from(value)
}

fn duckdb_database_bytes(path: &Path) -> BenchResult<u64> {
    let database = match fs::metadata(path) {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => return Err(error.into()),
    };
    let wal = match fs::metadata(duckdb_wal_path(path)) {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => return Err(error.into()),
    };
    Ok(database.saturating_add(wal))
}

fn duckdb_string_literal(path: &Path) -> String {
    let raw = path.display().to_string();
    format!("'{}'", raw.replace('\'', "''"))
}

fn duckdb_path_list(paths: &[PathBuf]) -> String {
    if paths.len() == 1 {
        duckdb_string_literal(&paths[0])
    } else {
        format!(
            "[{}]",
            paths
                .iter()
                .map(|path| duckdb_string_literal(path))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

fn load_duckdb_arrow_extension(
    connection: &duckdb::Connection,
    extension: &DuckDbArrowExtension,
) -> BenchResult<()> {
    if let Some(extension_name) = extension.community_extension_name() {
        return connection
            .execute_batch(&format!(
                "INSTALL {extension_name} FROM community; LOAD {extension_name};"
            ))
            .map_err(|error| {
                bench_error(format!(
                    "DuckDB {extension_name} extension install/load failed: {error}"
                ))
            });
    }

    let DuckDbArrowExtension::Loadable {
        path,
        expected_nanoarrow_version,
    } = extension
    else {
        unreachable!("community extensions returned above")
    };
    if !path.is_absolute() || !path.is_file() {
        return Err(bench_error(format!(
            "DuckDB loadable Arrow extension must be an existing absolute file: {}",
            path.display()
        )));
    }
    connection
        .execute_batch(&format!("LOAD {};", duckdb_string_literal(path)))
        .map_err(|error| {
            bench_error(format!(
                "DuckDB loadable Arrow extension {} failed to load: {error}",
                path.display()
            ))
        })?;
    let observed_version = connection.query_row("SELECT nanoarrow_version()", [], |row| {
        row.get::<_, String>(0)
    })?;
    if observed_version != *expected_nanoarrow_version {
        return Err(bench_error(format!(
            "DuckDB loadable Arrow extension reports nanoarrow {observed_version}, expected {expected_nanoarrow_version}"
        )));
    }
    Ok(())
}

fn open_duckdb_arrow_database(
    output: &Path,
    extension: &DuckDbArrowExtension,
) -> BenchResult<duckdb::Connection> {
    if matches!(extension, DuckDbArrowExtension::Loadable { .. }) {
        return Ok(duckdb::Connection::open_with_flags(
            output,
            duckdb::Config::default().allow_unsigned_extensions()?,
        )?);
    }
    Ok(duckdb::Connection::open(output)?)
}

fn duckdb_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn collect_arrow<I, E>(reader: I, physical_bytes: u64) -> BenchResult<WorkerMeasurement>
where
    I: IntoIterator<Item = Result<arrow_array::RecordBatch, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    let mut rows = 0_u64;
    let mut logical_bytes = 0_u64;
    for batch in reader {
        let batch = batch?;
        rows = rows.saturating_add(batch.num_rows() as u64);
        logical_bytes = logical_bytes.saturating_add(u64::try_from(batch.get_array_memory_size())?);
        black_box(batch);
    }
    measurement(rows, logical_bytes, physical_bytes)
}

fn measurement(
    rows: u64,
    logical_bytes: u64,
    physical_bytes: u64,
) -> BenchResult<WorkerMeasurement> {
    Ok(WorkerMeasurement {
        timed_wall_time_ns: None,
        rows,
        logical_bytes,
        physical_bytes,
        spill_bytes: 0,
        phases: Vec::new(),
    })
}

fn require_buffer(value: usize) -> BenchResult<()> {
    if value == 0 || value > 64 * 1024 * 1024 {
        return Err(bench_error(
            "reference buffer_bytes must be between 1 and 64 MiB",
        ));
    }
    Ok(())
}

fn require_batch(value: usize) -> BenchResult<()> {
    if value == 0 || value > 1_048_576 {
        return Err(bench_error(
            "reference batch_rows must be between 1 and 1048576",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_ipc::reader::FileReader;
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn parquet_rewrite_reference_uses_the_declared_writer_policy() {
        let temp = tempfile::tempdir().unwrap();
        let input = temp.path().join("input.parquet");
        let output = temp.path().join("output.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("text", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["one", "two", "three"])),
            ],
        )
        .unwrap();
        let mut source =
            ArrowWriter::try_new(fs::File::create(&input).unwrap(), schema, None).unwrap();
        source.write(&batch).unwrap();
        source.close().unwrap();

        let measurement = run_reference(&ReferenceWorkload::ArrowParquetRewrite {
            path: input,
            output: output.clone(),
            read_batch_rows: 1024,
            write_batch_rows: 8192,
            write_batch_bytes: 1024 * 1024,
            sync: true,
        })
        .unwrap();

        assert_eq!(measurement.rows, 3);
        assert_eq!(
            measurement.physical_bytes,
            fs::metadata(output).unwrap().len()
        );
        assert!(measurement.logical_bytes > 0);
    }

    #[test]
    fn parquet_files_reference_projects_and_decodes_files_concurrently() {
        let temp = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("text", DataType::Utf8, false),
        ]));
        let paths = (0..2)
            .map(|index| {
                let path = temp.path().join(format!("part-{index}.parquet"));
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(vec![1, 2, 3])),
                        Arc::new(StringArray::from(vec!["one", "two", "three"])),
                    ],
                )
                .unwrap();
                let mut writer =
                    ArrowWriter::try_new(fs::File::create(&path).unwrap(), schema.clone(), None)
                        .unwrap();
                writer.write(&batch).unwrap();
                writer.close().unwrap();
                path
            })
            .collect::<Vec<_>>();

        let measurement = run_reference(&ReferenceWorkload::ArrowParquetFiles {
            paths: paths.clone(),
            projection: vec!["id".to_owned()],
            batch_rows: 1024,
            jobs: 2,
        })
        .unwrap();

        assert_eq!(measurement.rows, 6);
        assert_eq!(measurement.physical_bytes, input_bytes(&paths).unwrap());
        assert!(measurement.logical_bytes > 0);
    }

    #[test]
    fn duckdb_parquet_ingest_reference_materializes_table() {
        let temp = tempfile::tempdir().unwrap();
        let input = temp.path().join("input.parquet");
        let output = temp.path().join("native.duckdb");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("text", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["one", "two", "three"])),
            ],
        )
        .unwrap();
        let mut source =
            ArrowWriter::try_new(fs::File::create(&input).unwrap(), schema, None).unwrap();
        source.write(&batch).unwrap();
        source.close().unwrap();

        let measurement = run_reference(&ReferenceWorkload::DuckDbParquetIngest {
            paths: vec![input.clone()],
            output: output.clone(),
        })
        .unwrap();

        assert_eq!(measurement.rows, 3);
        assert_eq!(
            measurement.physical_bytes,
            fs::metadata(input).unwrap().len()
        );
        assert!(fs::metadata(output).unwrap().len() > 0);
    }

    #[test]
    fn duckdb_parquet_ingest_with_row_key_reference_materializes_table() {
        let temp = tempfile::tempdir().unwrap();
        let input = temp.path().join("input.parquet");
        let output = temp.path().join("native-row-key.duckdb");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("text", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["one", "two", "three"])),
            ],
        )
        .unwrap();
        let mut source =
            ArrowWriter::try_new(fs::File::create(&input).unwrap(), schema, None).unwrap();
        source.write(&batch).unwrap();
        source.close().unwrap();

        let measurement = run_reference(&ReferenceWorkload::DuckDbParquetIngestWithRowKey {
            paths: vec![input.clone()],
            output: output.clone(),
            checkpoint: true,
        })
        .unwrap();

        assert_eq!(measurement.rows, 3);
        assert_eq!(
            measurement.physical_bytes,
            fs::metadata(input).unwrap().len()
        );
        let connection = duckdb::Connection::open(output).unwrap();
        let rows = connection
            .query_row(
                "SELECT count(*), min(_cdf_row_key), max(_cdf_row_key) \
                 FROM native_ingest_with_row_key",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, u64>(1)?,
                        row.get::<_, u64>(2)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(rows, (3, 1, 3));
    }

    #[test]
    fn duckdb_arrow_append_reference_materializes_persistent_table() {
        let temp = tempfile::tempdir().unwrap();
        let output = temp.path().join("arrow-append.duckdb");

        let measurement = run_reference(&ReferenceWorkload::DuckDbArrowAppend {
            output: output.clone(),
            rows: 2048,
            batch_rows: 512,
            include_row_key: true,
            checkpoint: true,
        })
        .unwrap();

        assert_eq!(measurement.rows, 2048);
        assert!(measurement.logical_bytes > 0);
        assert!(measurement.physical_bytes > 0);
        let connection = duckdb::Connection::open(output).unwrap();
        let rows = connection
            .query_row(
                "SELECT count(*), min(_cdf_row_key), max(_cdf_row_key) FROM arrow_append",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, u64>(1)?,
                        row.get::<_, u64>(2)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(rows, (2048, 1, 2048));
    }

    #[test]
    fn duckdb_arrow_data_chunk_append_reference_materializes_persistent_table() {
        let temp = tempfile::tempdir().unwrap();
        let output = temp.path().join("arrow-data-chunk-append.duckdb");

        let measurement = run_reference(&ReferenceWorkload::DuckDbArrowDataChunkAppend {
            output: output.clone(),
            rows: 2048,
            batch_rows: 512,
            include_row_key: true,
            checkpoint: true,
        })
        .unwrap();

        assert_eq!(measurement.rows, 2048);
        assert!(measurement.logical_bytes > 0);
        assert!(measurement.physical_bytes > 0);
        let connection = duckdb::Connection::open(output).unwrap();
        let rows = connection
            .query_row(
                "SELECT count(*), min(_cdf_row_key), max(_cdf_row_key) \
                 FROM arrow_data_chunk_append",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, u64>(1)?,
                        row.get::<_, u64>(2)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(rows, (2048, 1, 2048));
    }

    #[test]
    fn duckdb_arrow_stream_scan_reference_materializes_persistent_table_with_rowids() {
        let temp = tempfile::tempdir().unwrap();
        let output = temp.path().join("arrow-stream-scan.duckdb");

        let measurement = run_reference(&ReferenceWorkload::DuckDbArrowStreamScanIngest {
            output: output.clone(),
            rows: 2048,
            batch_rows: 512,
            include_row_key: false,
            checkpoint: true,
            verify_rowid: true,
            duckdb_threads: Some(1),
            duckdb_memory_limit_bytes: None,
            duckdb_temp_directory_budget_bytes: None,
        })
        .unwrap();

        assert_eq!(measurement.rows, 2048);
        assert!(measurement.logical_bytes > 0);
        assert!(measurement.physical_bytes > 0);
        let connection = duckdb::Connection::open(output).unwrap();
        let rows = connection
            .query_row(
                "SELECT count(*), count(DISTINCT rowid), min(rowid), max(rowid) \
                 FROM arrow_stream_scan",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(rows, (2048, 2048, 0, 2047));
    }

    #[test]
    fn duckdb_arrow_ipc_table_function_materializes_canonical_segments() {
        let temp = tempfile::tempdir().unwrap();
        let mut paths = Vec::new();
        for (index, package_row_ord_start) in [0_u64, 1024].into_iter().enumerate() {
            let path = temp.path().join(format!("segment-{index}.arrow"));
            let batch = tlc_arrow_batch(1024, package_row_ord_start + 1, false).unwrap();
            let batch =
                cdf_package_contract::append_package_row_ord(vec![batch], package_row_ord_start)
                    .unwrap()
                    .pop()
                    .unwrap();
            let mut writer = IpcFileWriter::try_new_with_options(
                fs::File::create(&path).unwrap(),
                batch.schema().as_ref(),
                IpcWriteOptions::default()
                    .try_with_compression(Some(IpcCompressionType::LZ4_FRAME))
                    .unwrap(),
            )
            .unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
            paths.push(path);
        }
        let output = temp.path().join("arrow-ipc-table-function.duckdb");

        let measurement = run_reference(&ReferenceWorkload::DuckDbArrowIpcTableFunctionIngest {
            paths,
            output: output.clone(),
            profiling_directory: Some(temp.path().join("profiles")),
            row_key_start: 1,
            checkpoint: true,
            duckdb_threads: Some(2),
            scan_threads: Some(2),
            duckdb_memory_limit_bytes: None,
            duckdb_temp_directory_budget_bytes: None,
        })
        .unwrap();

        assert_eq!(measurement.rows, 2048);
        assert!(measurement.logical_bytes > 0);
        assert!(measurement.physical_bytes > 0);
        let profile = fs::read_dir(temp.path().join("profiles"))
            .unwrap()
            .filter_map(std::result::Result::ok)
            .map(|entry| entry.path())
            .find(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "json")
            })
            .expect("reference profile");
        let summary = crate::read_duckdb_profile(&profile).unwrap();
        assert!(
            summary
                .query_name
                .contains("CREATE TABLE arrow_ipc_table_function")
        );
        assert!(summary.system_peak_buffer_memory_bytes.is_some());
        assert!(summary.system_peak_temp_directory_bytes.is_some());
        assert!(!summary.operators.is_empty());
        let connection = duckdb::Connection::open(output).unwrap();
        let observed = connection
            .query_row(
                "SELECT count(*), min(_cdf_row_key), max(_cdf_row_key) \
                 FROM arrow_ipc_table_function",
                [],
                |row| {
                    Ok((
                        row.get::<_, u64>(0)?,
                        row.get::<_, u64>(1)?,
                        row.get::<_, u64>(2)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(observed, (2048, 1, 2048));
        let leaked_ordinal = connection
            .query_row(
                "SELECT count(*) FROM pragma_table_info('arrow_ipc_table_function') \
                 WHERE name = '_cdf_package_row_ord'",
                [],
                |row| row.get::<_, u64>(0),
            )
            .unwrap();
        assert_eq!(leaked_ordinal, 0);
    }

    #[test]
    fn duckdb_reference_failed_capture_disables_profiling() {
        let temp = tempfile::tempdir().unwrap();
        let output = temp.path().join("profile-cleanup.duckdb");
        let profile_directory = temp.path().join("profiles");
        let mut connection = RawDuckDbConnection::open(&output).unwrap();
        let capture =
            ReferenceDuckDbProfileCapture::start(&mut connection, Some(&profile_directory), 2)
                .unwrap()
                .unwrap();
        connection.query("SELECT * FROM range(32)").unwrap();
        let scratch_path = capture.scratch_path.clone();
        fs::remove_file(&scratch_path).unwrap();

        assert!(capture.finish(&mut connection).is_err());
        connection.query("SELECT * FROM range(16)").unwrap();
        assert!(
            !scratch_path.exists(),
            "a query after failed reference capture must not recreate the profile output"
        );
    }

    #[test]
    fn arrow_ipc_handoff_writer_emits_readable_files() {
        for compression in [ArrowIpcCompression::None, ArrowIpcCompression::Lz4Frame] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join(format!("{compression:?}.arrow"));
            let measurement = write_tlc_arrow_ipc_file(
                &path,
                Arc::new(tlc_arrow_schema(true)),
                2048,
                512,
                1,
                true,
                compression,
            )
            .unwrap();
            assert!(measurement.logical_bytes > 0);
            assert!(measurement.physical_bytes > 0);
            let reader = FileReader::try_new(fs::File::open(&path).unwrap(), None).unwrap();
            let mut rows = 0_usize;
            for batch in reader {
                rows += batch.unwrap().num_rows();
            }
            assert_eq!(rows, 2048);
        }
    }

    #[test]
    fn duckdb_parquet_staged_ingest_reference_materializes_persistent_table() {
        let temp = tempfile::tempdir().unwrap();
        let output = temp.path().join("parquet-stage.duckdb");
        let staging = temp.path().join("stage.parquet");

        let measurement = run_reference(&ReferenceWorkload::DuckDbParquetStagedIngest {
            output: output.clone(),
            staging: staging.clone(),
            rows: 2048,
            batch_rows: 512,
            row_group_rows: 512,
            row_group_bytes: 1024 * 1024,
            include_row_key: true,
            checkpoint: false,
        })
        .unwrap();

        assert_eq!(measurement.rows, 2048);
        assert!(measurement.logical_bytes > 0);
        assert!(measurement.physical_bytes >= fs::metadata(&staging).unwrap().len());
        let connection = duckdb::Connection::open(output).unwrap();
        let rows = connection
            .query_row(
                "SELECT count(*), min(_cdf_row_key), max(_cdf_row_key) FROM parquet_stage",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, u64>(1)?,
                        row.get::<_, u64>(2)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(rows, (2048, 1, 2048));
    }
}
