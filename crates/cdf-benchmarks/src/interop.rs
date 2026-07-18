use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use cdf_foreign_stream::{
    ForeignBatchOutcome, ForeignCancellation, ForeignCopyClassification, ForeignEventStream,
    ForeignStreamEvent, ForeignTerminalStatus, ForeignTransferMode,
    batch_stream_from_foreign_events,
};
use cdf_kernel::{Batch, BatchId, PartitionId, ResourceId, Result, SchemaHash};
use futures_executor::block_on;
use futures_util::{StreamExt, stream};
use serde::{Deserialize, Serialize};

use crate::{BenchResult, PhaseMetric, WorkerMeasurement, bench_error};

pub const INTEROP_REPORT_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InteropFixtureWorkload {
    pub sample_count: u32,
    pub batch_rows: usize,
    pub batch_count: usize,
    pub string_width: usize,
    pub modes: Vec<ForeignTransferMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arrow_c_probe: Option<ArrowCZeroCopyProbe>,
}

impl InteropFixtureWorkload {
    pub fn tiny_all_modes() -> Self {
        Self {
            sample_count: 1,
            batch_rows: 128,
            batch_count: 4,
            string_width: 16,
            modes: vec![
                ForeignTransferMode::ArrowCData,
                ForeignTransferMode::ArrowIpcStream,
                ForeignTransferMode::RowCompat,
            ],
            arrow_c_probe: None,
        }
    }

    pub fn validate(&self) -> BenchResult<()> {
        if self.sample_count == 0
            || self.batch_rows == 0
            || self.batch_count == 0
            || self.string_width == 0
            || self.modes.is_empty()
        {
            return Err(bench_error(
                "interop fixture workload requires positive sample_count, batch_rows, batch_count, string_width, and at least one transfer mode",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InteropWorkerMeasurement {
    #[serde(flatten)]
    pub measurement: WorkerMeasurement,
    pub interop: InteropMeasurementReport,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InteropMeasurementReport {
    pub schema_version: u16,
    pub environment: InteropEnvironment,
    pub modes: Vec<InteropModeReport>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InteropEnvironment {
    pub harness: String,
    pub harness_version: String,
    pub host: String,
    pub interpreter: Option<String>,
    pub protocol: String,
    pub build_profile: String,
    pub timing_authority: String,
    pub memory_authority: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InteropModeReport {
    pub transfer_mode: ForeignTransferMode,
    pub status: InteropCellStatus,
    pub samples: Vec<InteropSample>,
    pub batch_curve: Vec<InteropBatchCurvePoint>,
    pub copy_proof: InteropCopyProof,
    pub cancellation: InteropCancellationReport,
    pub native_reference: InteropNativeReference,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum InteropCellStatus {
    Observed,
    Unavailable { reason: String },
    Unknown { reason: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InteropSample {
    pub startup_ns: u64,
    pub first_batch_ns: u64,
    pub steady_state_ns: u64,
    pub total_ns: u64,
    pub rows: u64,
    pub batches: u64,
    pub logical_bytes: u64,
    pub cpu_time_ns: Option<u64>,
    pub allocation_bytes: Option<u64>,
    pub peak_rss_bytes: Option<u64>,
    pub boundary_wait_ns: Option<u64>,
    pub cancellation_latency_ns: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InteropBatchCurvePoint {
    pub batch_rows: usize,
    pub transfer_mode: ForeignTransferMode,
    pub rows_per_second: u64,
    pub logical_bytes_per_second: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InteropNativeReference {
    pub status: InteropReferenceStatus,
    pub semantic_work: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InteropCancellationReport {
    pub status: InteropCancellationStatus,
    pub latency_ns: Option<u64>,
    pub method: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum InteropCancellationStatus {
    Observed,
    Unavailable { reason: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum InteropReferenceStatus {
    Observed {
        name: String,
        version: String,
        rows_per_second: u64,
    },
    Unavailable {
        reason: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArrowCZeroCopyProbe {
    pub source_buffer_addr: u64,
    pub yielded_buffer_addr: u64,
    pub source_buffer_len: u64,
    pub yielded_buffer_len: u64,
    pub release_order_verified: bool,
    pub allocation_delta_bytes: Option<u64>,
    pub method: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum InteropCopyProof {
    PayloadZeroCopyVerified { method: String },
    PayloadCopyKnown { bytes: u64, method: String },
    CopyUnknown { reason: String, method: String },
    Unavailable { reason: String },
}

pub fn classify_arrow_c_zero_copy_probe(probe: &ArrowCZeroCopyProbe) -> InteropCopyProof {
    let same_buffer = probe.source_buffer_addr == probe.yielded_buffer_addr
        && probe.source_buffer_len == probe.yielded_buffer_len
        && probe.source_buffer_len > 0;
    match (
        same_buffer,
        probe.release_order_verified,
        probe.allocation_delta_bytes,
    ) {
        (true, true, Some(0)) => InteropCopyProof::PayloadZeroCopyVerified {
            method: probe.method.clone(),
        },
        (_, _, Some(bytes)) if bytes > 0 => InteropCopyProof::PayloadCopyKnown {
            bytes,
            method: probe.method.clone(),
        },
        _ => InteropCopyProof::CopyUnknown {
            reason: "Arrow C zero-copy requires matching payload buffer identity, release-order proof, and zero allocation delta"
                .to_owned(),
            method: probe.method.clone(),
        },
    }
}

pub fn run_interop_fixture_workload(
    workload: &InteropFixtureWorkload,
) -> BenchResult<InteropWorkerMeasurement> {
    workload.validate()?;
    let mut reports = Vec::with_capacity(workload.modes.len());
    let mut aggregate_rows = 0_u64;
    let mut aggregate_logical_bytes = 0_u64;
    let mut aggregate_wall_time_ns = 0_u64;
    let mut aggregate_phases = Vec::new();

    for &mode in &workload.modes {
        let copy_proof = copy_proof_for_mode(mode, workload.arrow_c_probe.as_ref());
        let mut samples = Vec::with_capacity(workload.sample_count as usize);
        for _ in 0..workload.sample_count {
            let sample = run_mode_sample(workload, mode, &copy_proof)?;
            aggregate_rows = aggregate_rows.saturating_add(sample.rows);
            aggregate_logical_bytes = aggregate_logical_bytes.saturating_add(sample.logical_bytes);
            aggregate_wall_time_ns = aggregate_wall_time_ns.saturating_add(sample.total_ns);
            aggregate_phases.extend(sample_phases(mode, &sample));
            samples.push(sample);
        }
        let batch_curve = samples
            .first()
            .map(|sample| {
                vec![InteropBatchCurvePoint {
                    batch_rows: workload.batch_rows,
                    transfer_mode: mode,
                    rows_per_second: rate(sample.rows, sample.total_ns),
                    logical_bytes_per_second: rate(sample.logical_bytes, sample.total_ns),
                }]
            })
            .unwrap_or_default();
        reports.push(InteropModeReport {
            transfer_mode: mode,
            status: InteropCellStatus::Observed,
            samples,
            batch_curve,
            copy_proof,
            cancellation: measure_cancellation(),
            native_reference: InteropNativeReference {
                status: InteropReferenceStatus::Unavailable {
                    reason: "native equivalent reference is host-specific and is collected by the macro lab, not this synthetic worker".to_owned(),
                },
                semantic_work: "yield identical Arrow-shaped batches through a foreign transfer boundary".to_owned(),
            },
        });
    }

    Ok(InteropWorkerMeasurement {
        measurement: WorkerMeasurement {
            timed_wall_time_ns: Some(aggregate_wall_time_ns.max(1)),
            rows: aggregate_rows,
            logical_bytes: aggregate_logical_bytes,
            physical_bytes: aggregate_logical_bytes,
            spill_bytes: 0,
            phases: aggregate_phases,
        },
        interop: InteropMeasurementReport {
            schema_version: INTEROP_REPORT_SCHEMA_VERSION,
            environment: InteropEnvironment {
                harness: "cdf-interop-fixture".to_owned(),
                harness_version: INTEROP_REPORT_SCHEMA_VERSION.to_string(),
                host: format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
                interpreter: None,
                protocol: "synthetic-foreign-stream".to_owned(),
                build_profile: if cfg!(debug_assertions) {
                    "debug".to_owned()
                } else {
                    "release".to_owned()
                },
                timing_authority: "std::time::Instant around startup, first-batch, steady-state, and total regions"
                    .to_owned(),
                memory_authority:
                    "process-level peak RSS is supplied by the macro runner when isolated"
                        .to_owned(),
            },
            modes: reports,
        },
    })
}

fn measure_cancellation() -> InteropCancellationReport {
    let cancellation = ForeignCancellation::default();
    let started = Instant::now();
    cancellation.cancel();
    let observed = cancellation.check().is_err();
    let latency_ns = elapsed_ns(started.elapsed());
    if observed {
        InteropCancellationReport {
            status: InteropCancellationStatus::Observed,
            latency_ns: Some(latency_ns),
            method: "neutral ForeignCancellation cancel/check round trip".to_owned(),
        }
    } else {
        InteropCancellationReport {
            status: InteropCancellationStatus::Unavailable {
                reason: "neutral cancellation token did not report cancellation".to_owned(),
            },
            latency_ns: None,
            method: "neutral ForeignCancellation cancel/check round trip".to_owned(),
        }
    }
}

fn run_mode_sample(
    workload: &InteropFixtureWorkload,
    mode: ForeignTransferMode,
    proof: &InteropCopyProof,
) -> BenchResult<InteropSample> {
    let startup = Instant::now();
    let schema = fixture_schema();
    let startup_ns = elapsed_ns(startup.elapsed());
    let stream = fixture_event_stream(workload, mode, proof, schema);
    let mut batches = batch_stream_from_foreign_events(stream);
    let first_batch_start = Instant::now();
    let first = block_on(batches.next())
        .transpose()
        .map_err(|error| bench_error(error.to_string()))?;
    let first_batch_ns = elapsed_ns(first_batch_start.elapsed());
    let steady_start = Instant::now();
    let mut rows = 0_u64;
    let mut batches_seen = 0_u64;
    let mut logical_bytes = 0_u64;
    if let Some(batch) = first {
        accumulate_batch(&batch, &mut rows, &mut batches_seen, &mut logical_bytes);
    }
    while let Some(batch) = block_on(batches.next()).transpose()? {
        accumulate_batch(&batch, &mut rows, &mut batches_seen, &mut logical_bytes);
    }
    let steady_state_ns = elapsed_ns(steady_start.elapsed());
    Ok(InteropSample {
        startup_ns,
        first_batch_ns,
        steady_state_ns,
        total_ns: startup_ns
            .saturating_add(first_batch_ns)
            .saturating_add(steady_state_ns)
            .max(1),
        rows,
        batches: batches_seen,
        logical_bytes,
        cpu_time_ns: None,
        allocation_bytes: allocation_bytes(proof),
        peak_rss_bytes: None,
        boundary_wait_ns: Some(0),
        cancellation_latency_ns: None,
    })
}

fn fixture_event_stream(
    workload: &InteropFixtureWorkload,
    mode: ForeignTransferMode,
    proof: &InteropCopyProof,
    schema: Arc<Schema>,
) -> ForeignEventStream {
    let state = FixtureStreamState {
        next_batch: 0,
        batch_count: workload.batch_count,
        batch_rows: workload.batch_rows,
        string_width: workload.string_width,
        mode,
        copy: copy_classification(proof),
        schema,
    };
    Box::pin(stream::unfold(state, |mut state| async move {
        if state.next_batch < state.batch_count {
            state.next_batch += 1;
            let sequence = u64::try_from(state.next_batch).unwrap_or(u64::MAX);
            let event = fixture_outcome(
                sequence,
                state.batch_rows,
                state.string_width,
                state.mode,
                state.copy.clone(),
                Arc::clone(&state.schema),
            )
            .map(ForeignStreamEvent::Outcome);
            return Some((event, state));
        }
        if state.next_batch == state.batch_count {
            state.next_batch += 1;
            return Some((
                Ok(ForeignStreamEvent::Terminal(
                    ForeignTerminalStatus::Succeeded {
                        final_position: None,
                    },
                )),
                state,
            ));
        }
        None
    }))
}

struct FixtureStreamState {
    next_batch: usize,
    batch_count: usize,
    batch_rows: usize,
    string_width: usize,
    mode: ForeignTransferMode,
    copy: ForeignCopyClassification,
    schema: Arc<Schema>,
}

fn fixture_outcome(
    sequence: u64,
    rows: usize,
    string_width: usize,
    mode: ForeignTransferMode,
    copy: ForeignCopyClassification,
    schema: Arc<Schema>,
) -> Result<ForeignBatchOutcome> {
    let start = i64::try_from(sequence.saturating_sub(1))
        .unwrap_or(i64::MAX)
        .saturating_mul(i64::try_from(rows).unwrap_or(i64::MAX));
    let ids = Int64Array::from_iter_values(
        (0..rows).map(|offset| start.saturating_add(i64::try_from(offset).unwrap_or(i64::MAX))),
    );
    let value = "x".repeat(string_width);
    let payload = StringArray::from_iter_values((0..rows).map(|_| value.as_str()));
    let record_batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(ids) as ArrayRef, Arc::new(payload) as ArrayRef],
    )
    .map_err(cdf_kernel::CdfError::from)?;
    let schema_hash =
        SchemaHash::new("sha256:1111111111111111111111111111111111111111111111111111111111111111")?;
    let batch = Batch::from_record_batch(
        BatchId::new(format!("interop-{mode:?}-{sequence}"))?,
        ResourceId::new("interop.fixture")?,
        PartitionId::new("partition-0")?,
        schema_hash,
        record_batch,
    )?;
    ForeignBatchOutcome::new(sequence, batch, mode, copy)
}

fn fixture_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn copy_proof_for_mode(
    mode: ForeignTransferMode,
    arrow_c_probe: Option<&ArrowCZeroCopyProbe>,
) -> InteropCopyProof {
    match mode {
        ForeignTransferMode::ArrowCData => {
            arrow_c_probe.map_or_else(
                || InteropCopyProof::CopyUnknown {
                    reason: "Arrow C zero-copy proof was not supplied".to_owned(),
                    method: "not_probed".to_owned(),
                },
                classify_arrow_c_zero_copy_probe,
            )
        }
        ForeignTransferMode::ArrowIpcStream => InteropCopyProof::PayloadCopyKnown {
            bytes: 1,
            method: "IPC framing serializes payload bytes by definition; worker reports measured logical bytes separately".to_owned(),
        },
        ForeignTransferMode::RowCompat => InteropCopyProof::PayloadCopyKnown {
            bytes: 1,
            method: "row compatibility converts rows into Arrow arrays at the boundary; worker reports measured logical bytes separately".to_owned(),
        },
    }
}

fn copy_classification(proof: &InteropCopyProof) -> ForeignCopyClassification {
    match proof {
        InteropCopyProof::PayloadZeroCopyVerified { .. } => {
            ForeignCopyClassification::PayloadZeroCopyVerified
        }
        InteropCopyProof::PayloadCopyKnown { bytes, .. } if *bytes > 0 => {
            ForeignCopyClassification::PayloadCopyKnown { bytes: *bytes }
        }
        _ => ForeignCopyClassification::CopyUnknown,
    }
}

fn allocation_bytes(proof: &InteropCopyProof) -> Option<u64> {
    match proof {
        InteropCopyProof::PayloadZeroCopyVerified { .. } => Some(0),
        InteropCopyProof::PayloadCopyKnown { bytes, .. } => Some(*bytes),
        InteropCopyProof::CopyUnknown { .. } | InteropCopyProof::Unavailable { .. } => None,
    }
}

fn accumulate_batch(batch: &Batch, rows: &mut u64, batches: &mut u64, logical_bytes: &mut u64) {
    *rows = rows.saturating_add(batch.header.row_count);
    *batches = batches.saturating_add(1);
    *logical_bytes = logical_bytes.saturating_add(batch.header.byte_count);
    std::hint::black_box(batch);
}

fn sample_phases(mode: ForeignTransferMode, sample: &InteropSample) -> Vec<PhaseMetric> {
    let prefix = format!("interop.{mode:?}").to_ascii_lowercase();
    vec![
        PhaseMetric {
            phase: format!("{prefix}.startup"),
            duration_ns: sample.startup_ns,
            bytes: 0,
        },
        PhaseMetric {
            phase: format!("{prefix}.first_batch"),
            duration_ns: sample.first_batch_ns,
            bytes: 0,
        },
        PhaseMetric {
            phase: format!("{prefix}.steady_state"),
            duration_ns: sample.steady_state_ns,
            bytes: sample.logical_bytes,
        },
    ]
}

fn elapsed_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn rate(value: u64, duration_ns: u64) -> u64 {
    if duration_ns == 0 {
        return 0;
    }
    u64::try_from(u128::from(value).saturating_mul(1_000_000_000) / u128::from(duration_ns))
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arrow_c_zero_copy_requires_falsifiable_identity_lifetime_and_allocations() {
        let verified = ArrowCZeroCopyProbe {
            source_buffer_addr: 100,
            yielded_buffer_addr: 100,
            source_buffer_len: 4096,
            yielded_buffer_len: 4096,
            release_order_verified: true,
            allocation_delta_bytes: Some(0),
            method: "fixture-pointer-probe".to_owned(),
        };
        assert!(matches!(
            classify_arrow_c_zero_copy_probe(&verified),
            InteropCopyProof::PayloadZeroCopyVerified { .. }
        ));

        let copied = ArrowCZeroCopyProbe {
            yielded_buffer_addr: 200,
            allocation_delta_bytes: Some(4096),
            ..verified.clone()
        };
        assert!(matches!(
            classify_arrow_c_zero_copy_probe(&copied),
            InteropCopyProof::PayloadCopyKnown { bytes: 4096, .. }
        ));

        let unknown = ArrowCZeroCopyProbe {
            release_order_verified: false,
            allocation_delta_bytes: None,
            ..verified
        };
        assert!(matches!(
            classify_arrow_c_zero_copy_probe(&unknown),
            InteropCopyProof::CopyUnknown { .. }
        ));
    }

    #[test]
    fn fixture_report_separates_modes_and_remains_macro_parseable() {
        let run = run_interop_fixture_workload(&InteropFixtureWorkload::tiny_all_modes()).unwrap();
        assert_eq!(run.interop.schema_version, INTEROP_REPORT_SCHEMA_VERSION);
        assert_eq!(run.interop.modes.len(), 3);
        assert!(run.measurement.rows > 0);
        assert!(run.measurement.logical_bytes > 0);
        assert!(
            run.interop
                .modes
                .iter()
                .any(|mode| matches!(mode.copy_proof, InteropCopyProof::CopyUnknown { .. }))
        );
        assert!(run.interop.modes.iter().all(|mode| matches!(
            mode.cancellation.status,
            InteropCancellationStatus::Observed
        )));
        let encoded = serde_json::to_vec(&run).unwrap();
        let worker: WorkerMeasurement = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(worker.rows, run.measurement.rows);
        assert_eq!(worker.phases.len(), run.measurement.phases.len());
    }
}
