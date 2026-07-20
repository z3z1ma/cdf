use std::{hint::black_box, sync::Arc, time::Instant};

use arrow_array::{
    ArrayRef, BinaryArray, Decimal128Array, Float64Array, Int64Array, ListArray, RecordBatch,
    StringArray, TimestampMicrosecondArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use cdf_bench_core::{BenchResult, HostFingerprint, bench_error};
use cdf_kernel::BatchStats;
use serde::{Deserialize, Serialize};

pub const STATISTICS_ENVELOPE_SCHEMA_VERSION: u16 = 1;
const TARGET_INSPECTED_BYTES_PER_SAMPLE: u64 = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StatisticsEnvelopeConfig {
    pub samples: usize,
    pub target_rows_per_sample: usize,
}

impl StatisticsEnvelopeConfig {
    fn validate(self) -> BenchResult<Self> {
        if !(3..=31).contains(&self.samples) {
            return Err(bench_error(
                "statistics envelope samples must be between 3 and 31",
            ));
        }
        if !(65_536..=16_777_216).contains(&self.target_rows_per_sample) {
            return Err(bench_error(
                "statistics envelope target rows must be between 65536 and 16777216",
            ));
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatisticsEnvelopeReport {
    pub schema_version: u16,
    pub host: HostFingerprint,
    pub measurement: StatisticsMeasurementIdentity,
    pub cells: Vec<StatisticsEnvelopeCell>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatisticsMeasurementIdentity {
    pub method: String,
    pub version: String,
    pub timing_scope: String,
    pub inspected_bytes_authority: String,
    pub allocation_authority: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatisticsEnvelopeCell {
    pub workload: StatisticsWorkload,
    pub rows: usize,
    pub columns: usize,
    pub iterations_per_sample: usize,
    pub input_bytes_per_iteration: u64,
    pub inspected_bytes_per_iteration: Option<u64>,
    pub retained_statistics_bytes: u64,
    pub elapsed_ns: Vec<u64>,
    pub median_elapsed_ns: u64,
    pub median_absolute_deviation_ns: u64,
    pub rows_per_second: u64,
    pub inspected_bytes_per_second: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatisticsWorkload {
    PrimitiveMixed,
    WidePrimitive,
    VariableWidth,
    UnsupportedNested,
}

pub fn run_statistics_envelope(
    host: HostFingerprint,
    config: StatisticsEnvelopeConfig,
) -> BenchResult<StatisticsEnvelopeReport> {
    let config = config.validate()?;
    let cases = [
        (
            StatisticsWorkload::PrimitiveMixed,
            primitive_mixed(config.target_rows_per_sample)?,
        ),
        (
            StatisticsWorkload::WidePrimitive,
            wide_primitive(config.target_rows_per_sample)?,
        ),
        (
            StatisticsWorkload::VariableWidth,
            variable_width(config.target_rows_per_sample)?,
        ),
        (
            StatisticsWorkload::UnsupportedNested,
            unsupported_nested(config.target_rows_per_sample)?,
        ),
    ];
    let mut cells = Vec::with_capacity(cases.len());
    for (workload, batch) in cases {
        cells.push(measure_case(workload, batch, config.samples)?);
    }
    Ok(StatisticsEnvelopeReport {
        schema_version: STATISTICS_ENVELOPE_SCHEMA_VERSION,
        host,
        measurement: StatisticsMeasurementIdentity {
            method: "in-process monotonic-clock median-of-n".to_owned(),
            version: "p3-j0-v1".to_owned(),
            timing_scope: "cdf_kernel::BatchStats::compute over one immutable Arrow RecordBatch"
                .to_owned(),
            inspected_bytes_authority: "supported scalar arrays use Arrow RecordBatch::get_array_memory_size; unsupported nested classification records no inspected-byte rate"
                .to_owned(),
            allocation_authority:
                "typed statistics retained bytes reported by the production model".to_owned(),
        },
        cells,
    })
}

fn measure_case(
    workload: StatisticsWorkload,
    batch: RecordBatch,
    samples: usize,
) -> BenchResult<StatisticsEnvelopeCell> {
    let input_bytes_per_iteration = u64::try_from(batch.get_array_memory_size())
        .map_err(|_| bench_error("statistics batch bytes exceed u64"))?
        .max(1);
    let iterations_per_sample_u64 = TARGET_INSPECTED_BYTES_PER_SAMPLE
        .div_ceil(input_bytes_per_iteration)
        .max(1);
    let iterations_per_sample = usize::try_from(iterations_per_sample_u64)
        .map_err(|_| bench_error("statistics iteration count exceeds usize"))?;
    black_box(BatchStats::compute(&batch).map_err(|error| bench_error(error.to_string()))?);
    let mut elapsed_ns = Vec::with_capacity(samples);
    let mut retained_statistics_bytes = 0;
    for _ in 0..samples {
        let started = Instant::now();
        for _ in 0..iterations_per_sample {
            let stats = BatchStats::compute(black_box(&batch))
                .map_err(|error| bench_error(error.to_string()))?;
            retained_statistics_bytes = stats
                .retained_bytes()
                .map_err(|error| bench_error(error.to_string()))?;
            black_box(stats);
        }
        elapsed_ns.push(
            u64::try_from(started.elapsed().as_nanos())
                .map_err(|_| bench_error("statistics sample elapsed time exceeds u64"))?,
        );
    }
    let (median_elapsed_ns, median_absolute_deviation_ns) = distribution(&elapsed_ns)?;
    let inspected_bytes_per_iteration =
        (workload != StatisticsWorkload::UnsupportedNested).then_some(input_bytes_per_iteration);
    let inspected_bytes_per_second = inspected_bytes_per_iteration
        .map(|bytes| {
            bytes
                .checked_mul(iterations_per_sample_u64)
                .and_then(|bytes| bytes.checked_mul(1_000_000_000))
                .ok_or_else(|| bench_error("statistics throughput numerator overflow"))
        })
        .transpose()?
        .map(|bytes| bytes / median_elapsed_ns.max(1));
    let rows_per_second = u64::try_from(batch.num_rows())
        .ok()
        .and_then(|rows| rows.checked_mul(iterations_per_sample_u64))
        .and_then(|rows| rows.checked_mul(1_000_000_000))
        .ok_or_else(|| bench_error("statistics row throughput numerator overflow"))?
        / median_elapsed_ns.max(1);
    Ok(StatisticsEnvelopeCell {
        workload,
        rows: batch.num_rows(),
        columns: batch.num_columns(),
        iterations_per_sample,
        input_bytes_per_iteration,
        inspected_bytes_per_iteration,
        retained_statistics_bytes,
        elapsed_ns,
        median_elapsed_ns,
        median_absolute_deviation_ns,
        rows_per_second,
        inspected_bytes_per_second,
    })
}

fn distribution(values: &[u64]) -> BenchResult<(u64, u64)> {
    if values.is_empty() {
        return Err(bench_error("statistics distribution requires samples"));
    }
    let mut ordered = values.to_vec();
    ordered.sort_unstable();
    let median = ordered[ordered.len() / 2];
    let mut deviations = ordered
        .iter()
        .map(|value| value.abs_diff(median))
        .collect::<Vec<_>>();
    deviations.sort_unstable();
    Ok((median, deviations[deviations.len() / 2]))
}

fn primitive_mixed(rows: usize) -> BenchResult<RecordBatch> {
    let signed = (0..rows)
        .map(|row| (row % 11 != 0).then_some(row as i64 - rows as i64 / 2))
        .collect::<Vec<_>>();
    let unsigned = (0..rows)
        .map(|row| (row % 13 != 0).then_some(row as u64))
        .collect::<Vec<_>>();
    let floats = (0..rows)
        .map(|row| (row % 17 != 0).then_some(row as f64 * 0.125 - 4096.0))
        .collect::<Vec<_>>();
    let decimals = Decimal128Array::from(
        (0..rows)
            .map(|row| (row % 19 != 0).then_some(row as i128 * 10_003 - 7_000_000))
            .collect::<Vec<_>>(),
    )
    .with_precision_and_scale(38, 9)
    .map_err(|error| bench_error(error.to_string()))?;
    let timestamps = TimestampMicrosecondArray::from(
        (0..rows)
            .map(|row| (row % 23 != 0).then_some(1_700_000_000_000_000_i64 + row as i64))
            .collect::<Vec<_>>(),
    )
    .with_timezone("UTC");
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("signed", DataType::Int64, true),
            Field::new("unsigned", DataType::UInt64, true),
            Field::new("float", DataType::Float64, true),
            Field::new("decimal", DataType::Decimal128(38, 9), true),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ])),
        vec![
            Arc::new(Int64Array::from(signed)),
            Arc::new(UInt64Array::from(unsigned)),
            Arc::new(Float64Array::from(floats)),
            Arc::new(decimals),
            Arc::new(timestamps),
        ],
    )
    .map_err(|error| bench_error(error.to_string()))
}

fn wide_primitive(rows: usize) -> BenchResult<RecordBatch> {
    let fields = (0..64)
        .map(|column| Field::new(format!("value_{column:02}"), DataType::Int64, false))
        .collect::<Vec<_>>();
    let columns = (0..64)
        .map(|column| {
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(move |row| row as i64 + column),
            )) as ArrayRef
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|error| bench_error(error.to_string()))
}

fn variable_width(rows: usize) -> BenchResult<RecordBatch> {
    let strings = StringArray::from_iter((0..rows).map(|row| {
        (row % 7 != 0).then_some(if row % 2 == 0 {
            "cdf-statistics-short"
        } else {
            "cdf-statistics-a-somewhat-longer-variable-width-value"
        })
    }));
    let binaries = BinaryArray::from_iter((0..rows).map(|row| {
        (row % 11 != 0).then_some(if row % 2 == 0 {
            b"short".as_slice()
        } else {
            b"a-longer-binary-statistics-value".as_slice()
        })
    }));
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("text", DataType::Utf8, true),
            Field::new("bytes", DataType::Binary, true),
        ])),
        vec![Arc::new(strings), Arc::new(binaries)],
    )
    .map_err(|error| bench_error(error.to_string()))
}

fn unsupported_nested(rows: usize) -> BenchResult<RecordBatch> {
    let list = ListArray::from_iter_primitive::<arrow_array::types::Int64Type, _, _>(
        (0..rows).map(|row| Some(vec![Some(row as i64), Some(row as i64 + 1)])),
    );
    RecordBatch::try_from_iter(vec![("nested", Arc::new(list) as ArrayRef)])
        .map_err(|error| bench_error(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cdf_bench_core::HostCapabilityProvider;

    #[test]
    fn statistics_envelope_validates_shape_and_records_all_workloads() {
        let host = crate::provider().fingerprint().unwrap();
        let report = run_statistics_envelope(
            host,
            StatisticsEnvelopeConfig {
                samples: 3,
                target_rows_per_sample: 65_536,
            },
        )
        .unwrap();
        assert_eq!(report.cells.len(), 4);
        assert!(report.cells.iter().all(|cell| {
            cell.median_elapsed_ns > 0
                && cell.rows_per_second > 0
                && cell.retained_statistics_bytes > 0
        }));
        assert!(
            report
                .cells
                .iter()
                .find(|cell| cell.workload == StatisticsWorkload::UnsupportedNested)
                .unwrap()
                .inspected_bytes_per_second
                .is_none()
        );
    }
}
