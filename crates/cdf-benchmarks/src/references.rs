use std::{
    fs,
    hint::black_box,
    io::{BufReader, BufWriter, Read, Write},
    path::PathBuf,
    sync::Arc,
};

use arrow_csv::reader::{Format as CsvFormat, ReaderBuilder as CsvReaderBuilder};
use arrow_json::reader::{ReaderBuilder as JsonReaderBuilder, infer_json_schema};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
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
    },
    ArrowNdjson {
        path: PathBuf,
        batch_rows: usize,
        infer_rows: usize,
    },
    DuckDbParquet {
        path: PathBuf,
    },
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
        } => {
            require_batch(*batch_rows)?;
            let physical_bytes = fs::metadata(path)?.len();
            let format = CsvFormat::default().with_header(*has_header);
            let (schema, _) = format.infer_schema(fs::File::open(path)?, None)?;
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
    }
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
}
