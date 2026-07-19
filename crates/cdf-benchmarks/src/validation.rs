use std::{hint::black_box, sync::Arc, time::Instant};

use arrow_array::{
    Array, ArrayRef, Decimal128Array, Decimal256Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray, StructArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_buffer::{NullBuffer, i256};
use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use cdf_contract::{
    ContractEvaluationContext, ContractPolicy, ObservedSchema, RedactedObservedValue, RowRule,
    bind_vector_validation_plan, compile_validation_program,
};
use cdf_kernel::TrustLevel;
use serde::{Deserialize, Serialize};

use crate::{BenchResult, HostFingerprint, bench_error};

pub const VALIDATION_ENVELOPE_SCHEMA_VERSION: u16 = 1;
pub const VALIDATION_TARGET_BYTES_PER_SECOND: u64 = 1_000_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValidationEnvelopeConfig {
    pub samples: usize,
    pub target_rows_per_sample: usize,
}

impl ValidationEnvelopeConfig {
    pub fn validate(self) -> BenchResult<Self> {
        if !(3..=31).contains(&self.samples) {
            return Err(bench_error(
                "validation envelope samples must be between 3 and 31",
            ));
        }
        if !(65_536..=1_073_741_824).contains(&self.target_rows_per_sample) {
            return Err(bench_error(
                "validation envelope target rows per sample must be between 65536 and 1073741824",
            ));
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationEnvelopeReport {
    pub schema_version: u16,
    pub host: HostFingerprint,
    pub target_bytes_per_second_per_core: u64,
    pub measurement: ValidationMeasurementIdentity,
    pub memory_copy_roofline: ThroughputDistribution,
    pub cells: Vec<ValidationEnvelopeCell>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationMeasurementIdentity {
    pub method: String,
    pub version: String,
    pub timing_scope: String,
    pub inspected_bytes_authority: String,
    pub allocation_authority: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationEnvelopeCell {
    pub workload: ValidationWorkload,
    pub stage: ValidationStage,
    pub batch_rows: usize,
    pub violation_density: ViolationDensity,
    pub rule_count: usize,
    pub iterations_per_sample: usize,
    pub inspected_bytes_per_iteration: u64,
    pub retained_mask_bytes: u64,
    pub selected_evidence_rows: u64,
    pub selected_evidence_string_bytes: u64,
    pub distribution: ThroughputDistribution,
    pub memory_copy_ratio_ppm: Option<u64>,
    pub gate: ValidationGate,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThroughputDistribution {
    pub sample_count: usize,
    pub elapsed_ns: Vec<u64>,
    pub median_elapsed_ns: u64,
    pub median_absolute_deviation_ns: u64,
    pub rows_per_second: u64,
    pub inspected_bytes_per_second: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationWorkload {
    NumericWidthsRange,
    StringDomain,
    TimestampFreshness,
    MixedTlcWidth,
    DecimalNullabilityBoundary,
    NestedVariantBoundary,
    NumericSelectedEvidence,
}

impl ValidationWorkload {
    fn is_hot_kernel(self) -> bool {
        matches!(
            self,
            Self::NumericWidthsRange
                | Self::StringDomain
                | Self::TimestampFreshness
                | Self::MixedTlcWidth
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationStage {
    KernelMasks,
    SelectedEvidence,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ViolationDensity {
    ZeroPercent,
    OneRow,
    OneHundredPercent,
}

impl ViolationDensity {
    fn violates(self, row: usize, rows: usize) -> bool {
        match self {
            Self::ZeroPercent => false,
            Self::OneRow => row == rows / 2,
            Self::OneHundredPercent => true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ValidationGate {
    Passed {
        threshold_bytes_per_second: u64,
    },
    Failed {
        threshold_bytes_per_second: u64,
    },
    Inconclusive {
        threshold_bytes_per_second: u64,
        reason: String,
    },
    TrendOnly {
        reason: String,
    },
}

struct PreparedValidationCase {
    batch: RecordBatch,
    context: ContractEvaluationContext,
    program: cdf_contract::ValidationProgram,
    inspected_bytes: u64,
}

pub fn run_validation_envelope(
    host: HostFingerprint,
    config: ValidationEnvelopeConfig,
) -> BenchResult<ValidationEnvelopeReport> {
    let config = config.validate()?;
    let roofline = measure_memory_copy_roofline(config.samples)?;
    let mut cells = Vec::new();
    let batch_rows = [8_192, 16_384, 65_536];
    let densities = [
        ViolationDensity::ZeroPercent,
        ViolationDensity::OneRow,
        ViolationDensity::OneHundredPercent,
    ];
    let workloads = [
        ValidationWorkload::NumericWidthsRange,
        ValidationWorkload::StringDomain,
        ValidationWorkload::TimestampFreshness,
        ValidationWorkload::MixedTlcWidth,
        ValidationWorkload::DecimalNullabilityBoundary,
        ValidationWorkload::NestedVariantBoundary,
    ];

    for workload in workloads {
        for rows in batch_rows {
            for density in densities {
                cells.push(measure_kernel_cell(
                    workload, rows, density, config, &roofline,
                )?);
            }
        }
    }
    for density in densities {
        cells.push(measure_selected_evidence_cell(
            65_536,
            density,
            config.samples,
            &roofline,
        )?);
    }

    Ok(ValidationEnvelopeReport {
        schema_version: VALIDATION_ENVELOPE_SCHEMA_VERSION,
        host,
        target_bytes_per_second_per_core: VALIDATION_TARGET_BYTES_PER_SECOND,
        measurement: ValidationMeasurementIdentity {
            method: "single-threaded-median-of-n".to_owned(),
            version: "validation-envelope-v1".to_owned(),
            timing_scope: "prebound Arrow vector evaluation; fixture and plan binding excluded"
                .to_owned(),
            inspected_bytes_authority: "sum of value/offset/validity bytes read by each native rule; unreferenced batch columns excluded"
                .to_owned(),
            allocation_authority: "retained mask bytes plus selected evidence row/string counters"
                .to_owned(),
        },
        memory_copy_roofline: roofline,
        cells,
    })
}

pub fn validation_envelope_passes(report: &ValidationEnvelopeReport) -> bool {
    let gated = report
        .cells
        .iter()
        .filter(|cell| !matches!(cell.gate, ValidationGate::TrendOnly { .. }))
        .collect::<Vec<_>>();
    gated.len() == 12
        && gated
            .iter()
            .all(|cell| matches!(cell.gate, ValidationGate::Passed { .. }))
}

fn measure_kernel_cell(
    workload: ValidationWorkload,
    rows: usize,
    density: ViolationDensity,
    config: ValidationEnvelopeConfig,
    roofline: &ThroughputDistribution,
) -> BenchResult<ValidationEnvelopeCell> {
    let prepared = prepare_case(workload, rows, density)?;
    let plan = bind_vector_validation_plan(&prepared.program, prepared.batch.schema())?;
    let observed = plan.evaluate_masks(&prepared.context, &prepared.batch)?;
    let retained_mask_bytes = observed.accepted_rows.values().len()
        + observed.quarantined_rows.values().len()
        + observed
            .rule_masks
            .iter()
            .map(|rule| rule.violations.values().len())
            .sum::<usize>();
    let rule_count = observed.rule_masks.len();
    drop(observed);
    let iterations = config.target_rows_per_sample.div_ceil(rows).max(1);
    black_box(plan.evaluate_masks(&prepared.context, &prepared.batch)?);
    let elapsed_ns = measure_samples(config.samples, || {
        for _ in 0..iterations {
            black_box(plan.evaluate_masks(&prepared.context, &prepared.batch)?);
        }
        Ok(())
    })?;
    let distribution = distribution(
        elapsed_ns,
        rows.saturating_mul(iterations),
        prepared
            .inspected_bytes
            .checked_mul(iterations as u64)
            .ok_or_else(|| bench_error("validation inspected-byte count overflowed"))?,
    );
    let target_applies = workload.is_hot_kernel() && rows == 65_536;
    let gate = classify_gate(&distribution, target_applies);
    Ok(ValidationEnvelopeCell {
        workload,
        stage: ValidationStage::KernelMasks,
        batch_rows: rows,
        violation_density: density,
        rule_count,
        iterations_per_sample: iterations,
        inspected_bytes_per_iteration: prepared.inspected_bytes,
        retained_mask_bytes: retained_mask_bytes as u64,
        selected_evidence_rows: 0,
        selected_evidence_string_bytes: 0,
        memory_copy_ratio_ppm: throughput_ratio(&distribution, roofline),
        distribution,
        gate,
    })
}

fn measure_selected_evidence_cell(
    rows: usize,
    density: ViolationDensity,
    samples: usize,
    roofline: &ThroughputDistribution,
) -> BenchResult<ValidationEnvelopeCell> {
    let prepared = prepare_selected_evidence(rows, density)?;
    let plan = bind_vector_validation_plan(&prepared.program, prepared.batch.schema())?;
    let observed = plan.evaluate(&prepared.context, &prepared.batch)?;
    let selected_evidence_rows = observed.quarantine_candidates.len() as u64;
    let selected_evidence_string_bytes = observed
        .quarantine_candidates
        .iter()
        .map(|candidate| {
            candidate.rule_id.len()
                + candidate.error_code.len()
                + redacted_value_string_bytes(&candidate.observed_value_redacted)
        })
        .sum::<usize>() as u64;
    drop(observed);
    let masks = plan.evaluate_masks(&prepared.context, &prepared.batch)?;
    let retained_mask_bytes = masks.accepted_rows.values().len()
        + masks.quarantined_rows.values().len()
        + masks
            .rule_masks
            .iter()
            .map(|rule| rule.violations.values().len())
            .sum::<usize>();
    let rule_count = masks.rule_masks.len();
    drop(masks);
    let elapsed_ns = measure_samples(samples, || {
        black_box(plan.evaluate(&prepared.context, &prepared.batch)?);
        Ok(())
    })?;
    let distribution = distribution(elapsed_ns, rows, prepared.inspected_bytes);
    Ok(ValidationEnvelopeCell {
        workload: ValidationWorkload::NumericSelectedEvidence,
        stage: ValidationStage::SelectedEvidence,
        batch_rows: rows,
        violation_density: density,
        rule_count,
        iterations_per_sample: 1,
        inspected_bytes_per_iteration: prepared.inspected_bytes,
        retained_mask_bytes: retained_mask_bytes as u64,
        selected_evidence_rows,
        selected_evidence_string_bytes,
        memory_copy_ratio_ppm: throughput_ratio(&distribution, roofline),
        distribution,
        gate: ValidationGate::TrendOnly {
            reason: "selected-row evidence cost is visible but is not the kernel throughput claim"
                .to_owned(),
        },
    })
}

fn classify_gate(distribution: &ThroughputDistribution, applies: bool) -> ValidationGate {
    if !applies {
        return ValidationGate::TrendOnly {
            reason: "the ratified throughput gate applies to 64k data-inspecting native kernels"
                .to_owned(),
        };
    }
    let Some(rate) = distribution.inspected_bytes_per_second else {
        return ValidationGate::Inconclusive {
            threshold_bytes_per_second: VALIDATION_TARGET_BYTES_PER_SECOND,
            reason: "cell has no inspected-byte authority".to_owned(),
        };
    };
    if rate >= VALIDATION_TARGET_BYTES_PER_SECOND {
        return ValidationGate::Passed {
            threshold_bytes_per_second: VALIDATION_TARGET_BYTES_PER_SECOND,
        };
    }
    let elapsed = distribution.median_elapsed_ns.max(1);
    let optimistic_elapsed =
        elapsed.saturating_sub(distribution.median_absolute_deviation_ns.saturating_mul(2));
    let optimistic_rate = rate.saturating_mul(elapsed) / optimistic_elapsed.max(1);
    if optimistic_rate >= VALIDATION_TARGET_BYTES_PER_SECOND {
        ValidationGate::Inconclusive {
            threshold_bytes_per_second: VALIDATION_TARGET_BYTES_PER_SECOND,
            reason: "median is below target but the two-MAD interval crosses it".to_owned(),
        }
    } else {
        ValidationGate::Failed {
            threshold_bytes_per_second: VALIDATION_TARGET_BYTES_PER_SECOND,
        }
    }
}

fn measure_memory_copy_roofline(samples: usize) -> BenchResult<ThroughputDistribution> {
    const CHUNK_BYTES: usize = 64 * 1024 * 1024;
    const ITERATIONS: usize = 8;
    let source = vec![0x5a_u8; CHUNK_BYTES];
    let mut destination = vec![0_u8; CHUNK_BYTES];
    destination.copy_from_slice(black_box(&source));
    let elapsed_ns = measure_samples(samples, || {
        for _ in 0..ITERATIONS {
            destination.copy_from_slice(black_box(&source));
            black_box(&destination);
        }
        Ok(())
    })?;
    Ok(distribution(
        elapsed_ns,
        CHUNK_BYTES.saturating_mul(ITERATIONS),
        (CHUNK_BYTES as u64).saturating_mul(ITERATIONS as u64),
    ))
}

fn measure_samples(
    samples: usize,
    mut operation: impl FnMut() -> BenchResult<()>,
) -> BenchResult<Vec<u64>> {
    let mut elapsed = Vec::with_capacity(samples);
    for _ in 0..samples {
        let started = Instant::now();
        operation()?;
        elapsed.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
    }
    Ok(elapsed)
}

fn distribution(elapsed_ns: Vec<u64>, rows: usize, inspected_bytes: u64) -> ThroughputDistribution {
    let mut ordered = elapsed_ns.clone();
    ordered.sort_unstable();
    let median = ordered[ordered.len() / 2].max(1);
    let mut deviations = ordered
        .iter()
        .map(|sample| sample.abs_diff(median))
        .collect::<Vec<_>>();
    deviations.sort_unstable();
    let mad = deviations[deviations.len() / 2];
    ThroughputDistribution {
        sample_count: elapsed_ns.len(),
        elapsed_ns,
        median_elapsed_ns: median,
        median_absolute_deviation_ns: mad,
        rows_per_second: (rows as u64).saturating_mul(1_000_000_000) / median,
        inspected_bytes_per_second: (inspected_bytes != 0)
            .then_some(inspected_bytes.saturating_mul(1_000_000_000) / median),
    }
}

fn throughput_ratio(
    distribution: &ThroughputDistribution,
    roofline: &ThroughputDistribution,
) -> Option<u64> {
    distribution.inspected_bytes_per_second.map(|throughput| {
        throughput.saturating_mul(1_000_000)
            / roofline.inspected_bytes_per_second.unwrap_or(1).max(1)
    })
}

fn redacted_value_string_bytes(value: &RedactedObservedValue) -> usize {
    match value {
        RedactedObservedValue::Null | RedactedObservedValue::Omitted => 0,
        RedactedObservedValue::Preserved { value } | RedactedObservedValue::Masked { value } => {
            value.len()
        }
        RedactedObservedValue::Hashed { algorithm, value } => algorithm.len() + value.len(),
    }
}

fn prepare_case(
    workload: ValidationWorkload,
    rows: usize,
    density: ViolationDensity,
) -> BenchResult<PreparedValidationCase> {
    match workload {
        ValidationWorkload::NumericWidthsRange => prepare_numeric_widths(rows, density),
        ValidationWorkload::StringDomain => prepare_string_domain(rows, density),
        ValidationWorkload::TimestampFreshness => prepare_timestamps(rows, density),
        ValidationWorkload::MixedTlcWidth => prepare_mixed_tlc(rows, density),
        ValidationWorkload::DecimalNullabilityBoundary => prepare_decimals(rows, density),
        ValidationWorkload::NestedVariantBoundary => prepare_nested_variant(rows, density),
        ValidationWorkload::NumericSelectedEvidence => prepare_selected_evidence(rows, density),
    }
}

fn compile_case(
    schema: Schema,
    arrays: Vec<ArrayRef>,
    rules: Vec<RowRule>,
    context: ContractEvaluationContext,
    inspected_bytes: u64,
) -> BenchResult<PreparedValidationCase> {
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = rules;
    let program = compile_validation_program(&policy, &ObservedSchema::from_arrow(&schema))?;
    let batch = RecordBatch::try_new(Arc::new(schema), arrays)?;
    Ok(PreparedValidationCase {
        batch,
        context,
        program,
        inspected_bytes,
    })
}

fn prepare_numeric_widths(
    rows: usize,
    density: ViolationDensity,
) -> BenchResult<PreparedValidationCase> {
    let names_and_types = [
        ("i8", DataType::Int8),
        ("i16", DataType::Int16),
        ("i32", DataType::Int32),
        ("i64", DataType::Int64),
        ("u8", DataType::UInt8),
        ("u16", DataType::UInt16),
        ("u32", DataType::UInt32),
        ("u64", DataType::UInt64),
        ("f32", DataType::Float32),
        ("f64", DataType::Float64),
    ];
    let fields = names_and_types
        .iter()
        .map(|(name, data_type)| Field::new(*name, data_type.clone(), false))
        .collect::<Vec<_>>();
    let signed = || {
        (0..rows)
            .map(|row| if density.violates(row, rows) { 11 } else { 5 })
            .collect::<Vec<_>>()
    };
    let unsigned = || {
        (0..rows)
            .map(|row| if density.violates(row, rows) { 11 } else { 5 })
            .collect::<Vec<_>>()
    };
    let float = || {
        (0..rows)
            .map(|row| {
                if density.violates(row, rows) {
                    11.0
                } else {
                    5.0
                }
            })
            .collect::<Vec<_>>()
    };
    let arrays = vec![
        Arc::new(Int8Array::from(
            signed().into_iter().map(|v| v as i8).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(Int16Array::from(
            signed().into_iter().map(|v| v as i16).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(Int32Array::from(signed())) as ArrayRef,
        Arc::new(Int64Array::from(
            signed().into_iter().map(i64::from).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(UInt8Array::from(
            unsigned().into_iter().map(|v| v as u8).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(UInt16Array::from(
            unsigned().into_iter().map(|v| v as u16).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(UInt32Array::from(
            unsigned().into_iter().map(|v| v as u32).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(UInt64Array::from(
            unsigned().into_iter().map(|v| v as u64).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(Float32Array::from(
            float().into_iter().map(|v| v as f32).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(Float64Array::from(float())) as ArrayRef,
    ];
    let rules = names_and_types
        .iter()
        .map(|(name, _)| RowRule::Range {
            column: (*name).to_owned(),
            min: Some("0".to_owned()),
            max: Some("10".to_owned()),
        })
        .collect();
    compile_case(
        Schema::new(fields),
        arrays,
        rules,
        ContractEvaluationContext::default(),
        (rows as u64).saturating_mul(42),
    )
}

fn prepare_string_domain(
    rows: usize,
    density: ViolationDensity,
) -> BenchResult<PreparedValidationCase> {
    const PASS: &str = "abcdefghijklmnop";
    const FAIL: &str = "qrstuvwxyzabcdef";
    let values = (0..rows)
        .map(|row| {
            if density.violates(row, rows) {
                FAIL
            } else {
                PASS
            }
        })
        .collect::<Vec<_>>();
    compile_case(
        Schema::new(vec![
            Field::new("utf8", DataType::Utf8, false),
            Field::new("large_utf8", DataType::LargeUtf8, false),
        ]),
        vec![
            Arc::new(StringArray::from(values.clone())) as ArrayRef,
            Arc::new(LargeStringArray::from(values)) as ArrayRef,
        ],
        vec![
            RowRule::Domain {
                column: "utf8".to_owned(),
                allowed: vec![PASS.to_owned()],
            },
            RowRule::Domain {
                column: "large_utf8".to_owned(),
                allowed: vec![PASS.to_owned()],
            },
        ],
        ContractEvaluationContext::default(),
        (rows as u64)
            .saturating_mul((PASS.len() * 2 + 12) as u64)
            .saturating_add(12),
    )
}

fn prepare_timestamps(
    rows: usize,
    density: ViolationDensity,
) -> BenchResult<PreparedValidationCase> {
    let raw = (0..rows)
        .map(|row| !density.violates(row, rows))
        .collect::<Vec<_>>();
    let fields = [
        ("seconds", TimeUnit::Second),
        ("millis", TimeUnit::Millisecond),
        ("micros", TimeUnit::Microsecond),
        ("nanos", TimeUnit::Nanosecond),
    ]
    .into_iter()
    .map(|(name, unit)| Field::new(name, DataType::Timestamp(unit, Some("UTC".into())), false))
    .collect::<Vec<_>>();
    let arrays = vec![
        Arc::new(
            TimestampSecondArray::from(
                raw.iter()
                    .map(|pass| if *pass { 10 } else { 0 })
                    .collect::<Vec<_>>(),
            )
            .with_timezone("UTC"),
        ) as ArrayRef,
        Arc::new(
            TimestampMillisecondArray::from(
                raw.iter()
                    .map(|pass| if *pass { 10_000 } else { 0 })
                    .collect::<Vec<_>>(),
            )
            .with_timezone("UTC"),
        ) as ArrayRef,
        Arc::new(
            TimestampMicrosecondArray::from(
                raw.iter()
                    .map(|pass| if *pass { 10_000_000 } else { 0 })
                    .collect::<Vec<_>>(),
            )
            .with_timezone("UTC"),
        ) as ArrayRef,
        Arc::new(
            TimestampNanosecondArray::from(
                raw.iter()
                    .map(|pass| if *pass { 10_000_000_000 } else { 0 })
                    .collect::<Vec<_>>(),
            )
            .with_timezone("UTC"),
        ) as ArrayRef,
    ];
    let rules = ["seconds", "millis", "micros", "nanos"]
        .into_iter()
        .map(|column| RowRule::Freshness {
            column: column.to_owned(),
            max_age_ms: 1_000,
        })
        .collect();
    compile_case(
        Schema::new(fields),
        arrays,
        rules,
        ContractEvaluationContext::observed_at(10_000),
        (rows as u64).saturating_mul(32),
    )
}

fn prepare_mixed_tlc(
    rows: usize,
    density: ViolationDensity,
) -> BenchResult<PreparedValidationCase> {
    const PASS: &str = "cardpass";
    const FAIL: &str = "cashfail";
    let violates = (0..rows)
        .map(|row| density.violates(row, rows))
        .collect::<Vec<_>>();
    let fields = vec![
        Field::new("vendor_id", DataType::Int32, false),
        Field::new(
            "pickup",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("passengers", DataType::Int64, false),
        Field::new("fare", DataType::Float64, false),
        Field::new("zone", DataType::Int32, false),
        Field::new("payment", DataType::Utf8, false),
    ];
    let arrays = vec![
        Arc::new(Int32Array::from(
            violates
                .iter()
                .map(|bad| if *bad { 9 } else { 2 })
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(
            TimestampMicrosecondArray::from(
                violates
                    .iter()
                    .map(|bad| if *bad { 0 } else { 10_000_000 })
                    .collect::<Vec<_>>(),
            )
            .with_timezone("UTC"),
        ) as ArrayRef,
        Arc::new(Int64Array::from(
            violates
                .iter()
                .map(|bad| if *bad { 99 } else { 2 })
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(Float64Array::from(
            violates
                .iter()
                .map(|bad| if *bad { 1_001.0 } else { 12.5 })
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(Int32Array::from(
            violates
                .iter()
                .map(|bad| if *bad { 999 } else { 132 })
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(StringArray::from(
            violates
                .iter()
                .map(|bad| if *bad { FAIL } else { PASS })
                .collect::<Vec<_>>(),
        )) as ArrayRef,
    ];
    let rules = vec![
        RowRule::Range {
            column: "vendor_id".to_owned(),
            min: Some("1".to_owned()),
            max: Some("6".to_owned()),
        },
        RowRule::Freshness {
            column: "pickup".to_owned(),
            max_age_ms: 1_000,
        },
        RowRule::Range {
            column: "passengers".to_owned(),
            min: Some("0".to_owned()),
            max: Some("8".to_owned()),
        },
        RowRule::Range {
            column: "fare".to_owned(),
            min: Some("0".to_owned()),
            max: Some("1000".to_owned()),
        },
        RowRule::Range {
            column: "zone".to_owned(),
            min: Some("1".to_owned()),
            max: Some("265".to_owned()),
        },
        RowRule::Domain {
            column: "payment".to_owned(),
            allowed: vec![PASS.to_owned()],
        },
    ];
    compile_case(
        Schema::new(fields),
        arrays,
        rules,
        ContractEvaluationContext::observed_at(10_000),
        (rows as u64).saturating_mul(44).saturating_add(4),
    )
}

fn prepare_decimals(rows: usize, density: ViolationDensity) -> BenchResult<PreparedValidationCase> {
    let values128 = (0..rows)
        .map(|row| (!density.violates(row, rows)).then_some(123_456_789_i128))
        .collect::<Vec<_>>();
    let values256 = (0..rows)
        .map(|row| (!density.violates(row, rows)).then_some(i256::from_i128(123_456_789)))
        .collect::<Vec<_>>();
    let decimal128 = Decimal128Array::from(values128).with_precision_and_scale(38, 9)?;
    let decimal256 = Decimal256Array::from(values256).with_precision_and_scale(76, 9)?;
    let validity_bytes = decimal128.nulls().map_or(0, |nulls| nulls.buffer().len())
        + decimal256.nulls().map_or(0, |nulls| nulls.buffer().len());
    compile_case(
        Schema::new(vec![
            Field::new("decimal128", DataType::Decimal128(38, 9), true),
            Field::new("decimal256", DataType::Decimal256(76, 9), true),
        ]),
        vec![Arc::new(decimal128), Arc::new(decimal256)],
        vec![
            RowRule::Nullability {
                column: "decimal128".to_owned(),
            },
            RowRule::Nullability {
                column: "decimal256".to_owned(),
            },
        ],
        ContractEvaluationContext::default(),
        validity_bytes as u64,
    )
}

fn prepare_nested_variant(
    rows: usize,
    density: ViolationDensity,
) -> BenchResult<PreparedValidationCase> {
    const PASS: &str = "{\"kind\":\"known\"}";
    const FAIL: &str = "{\"kind\":\"unknown\"}";
    let valid = (0..rows)
        .map(|row| !density.violates(row, rows))
        .collect::<Vec<_>>();
    let child = Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef;
    let struct_array = StructArray::new(
        Fields::from(vec![Field::new("value", DataType::Int64, false)]),
        vec![child],
        Some(NullBuffer::from(valid.clone())),
    );
    let variant = LargeStringArray::from(
        valid
            .iter()
            .map(|valid| if *valid { PASS } else { FAIL })
            .collect::<Vec<_>>(),
    );
    let inspected = struct_array.nulls().map_or(0, |nulls| nulls.buffer().len()) as u64
        + (rows as u64).saturating_mul(PASS.len() as u64 + 8)
        + 8;
    compile_case(
        Schema::new(vec![
            Field::new(
                "nested",
                DataType::Struct(Fields::from(vec![Field::new(
                    "value",
                    DataType::Int64,
                    false,
                )])),
                true,
            ),
            Field::new("variant_payload", DataType::LargeUtf8, false),
        ]),
        vec![Arc::new(struct_array), Arc::new(variant)],
        vec![
            RowRule::Nullability {
                column: "nested".to_owned(),
            },
            RowRule::Domain {
                column: "variant_payload".to_owned(),
                allowed: vec![PASS.to_owned()],
            },
        ],
        ContractEvaluationContext::default(),
        inspected,
    )
}

fn prepare_selected_evidence(
    rows: usize,
    density: ViolationDensity,
) -> BenchResult<PreparedValidationCase> {
    let values = (0..rows)
        .map(|row| if density.violates(row, rows) { 11 } else { 5 })
        .collect::<Vec<_>>();
    compile_case(
        Schema::new(vec![Field::new("value", DataType::Int64, false)]),
        vec![Arc::new(Int64Array::from(values))],
        vec![RowRule::Range {
            column: "value".to_owned(),
            min: Some("0".to_owned()),
            max: Some("10".to_owned()),
        }],
        ContractEvaluationContext::default(),
        (rows as u64).saturating_mul(8),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Capability;

    fn host() -> HostFingerprint {
        HostFingerprint {
            schema_version: 1,
            architecture: "test".to_owned(),
            cpu_label: "fixture".to_owned(),
            advertised_logical_cores: 1,
            advertised_physical_cores: Capability::Unavailable {
                reason: "fixture".to_owned(),
                method: "fixture".to_owned(),
                provider_version: "fixture-v1".to_owned(),
            },
            advertised_memory_bytes: Capability::Unavailable {
                reason: "fixture".to_owned(),
                method: "fixture".to_owned(),
                provider_version: "fixture-v1".to_owned(),
            },
            effective_cpu: Capability::Unavailable {
                reason: "fixture".to_owned(),
                method: "fixture".to_owned(),
                provider_version: "fixture-v1".to_owned(),
            },
            effective_memory_bytes: Capability::Unavailable {
                reason: "fixture".to_owned(),
                method: "fixture".to_owned(),
                provider_version: "fixture-v1".to_owned(),
            },
            storage: Capability::Unavailable {
                reason: "fixture".to_owned(),
                method: "fixture".to_owned(),
                provider_version: "fixture-v1".to_owned(),
            },
            os: crate::OsFingerprint {
                family: "test".to_owned(),
                version: "test".to_owned(),
                kernel: None,
            },
            rust_version: "test".to_owned(),
            cdf_version: "test".to_owned(),
            dependency_versions: Default::default(),
            benchmark_profile: "test".to_owned(),
        }
    }

    #[test]
    fn validation_envelope_config_rejects_non_representative_measurements() {
        assert!(
            ValidationEnvelopeConfig {
                samples: 2,
                target_rows_per_sample: 65_536,
            }
            .validate()
            .is_err()
        );
        assert!(
            ValidationEnvelopeConfig {
                samples: 3,
                target_rows_per_sample: 1,
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn validation_matrix_covers_ratified_dimensions_and_byte_authority() {
        let config = ValidationEnvelopeConfig {
            samples: 3,
            target_rows_per_sample: 65_536,
        };
        let report = run_validation_envelope(host(), config).unwrap();
        assert_eq!(report.schema_version, VALIDATION_ENVELOPE_SCHEMA_VERSION);
        assert_eq!(report.cells.len(), 57);
        assert!(report.cells.iter().any(|cell| {
            cell.workload == ValidationWorkload::DecimalNullabilityBoundary
                && cell.batch_rows == 65_536
                && cell.violation_density == ViolationDensity::OneHundredPercent
        }));
        assert!(report.cells.iter().any(|cell| {
            cell.workload == ValidationWorkload::NestedVariantBoundary
                && cell.batch_rows == 8_192
                && cell.violation_density == ViolationDensity::OneRow
        }));
        assert!(
            report
                .cells
                .iter()
                .filter(|cell| matches!(
                    cell.gate,
                    ValidationGate::Passed { .. }
                        | ValidationGate::Failed { .. }
                        | ValidationGate::Inconclusive { .. }
                ))
                .all(|cell| {
                    cell.batch_rows == 65_536
                        && cell.workload.is_hot_kernel()
                        && cell.inspected_bytes_per_iteration > 0
                })
        );
        assert!(report.cells.iter().any(|cell| {
            cell.stage == ValidationStage::SelectedEvidence
                && cell.violation_density == ViolationDensity::OneHundredPercent
                && cell.selected_evidence_rows == 65_536
                && cell.selected_evidence_string_bytes > 0
        }));
        serde_json::to_vec(&report).unwrap();
    }
}
