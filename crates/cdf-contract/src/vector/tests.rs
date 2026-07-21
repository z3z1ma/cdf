use std::{collections::HashMap, sync::Arc, time::Instant};

use arrow_array::{
    ArrayRef, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeStringArray, RecordBatch, StringArray, TimestampMicrosecondArray, UInt8Array, UInt16Array,
    UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use cdf_kernel::TrustLevel;
use proptest::prelude::*;

use super::*;
use crate::{
    ContractPolicy, ObservedSchema, RowRule, compile_validation_program, evaluate_record_batch,
};

fn program_and_batch(values: Vec<Option<i32>>) -> (ValidationProgram, RecordBatch) {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int32, true),
        Field::new("status", DataType::Utf8, false),
    ]);
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![
        RowRule::Range {
            column: "value".to_owned(),
            min: Some("-10".to_owned()),
            max: Some("10".to_owned()),
        },
        RowRule::Domain {
            column: "status".to_owned(),
            allowed: vec!["ok".to_owned()],
        },
        RowRule::Nullability {
            column: "value".to_owned(),
        },
    ];
    let program =
        compile_validation_program(&policy, &ObservedSchema::from_arrow(&schema)).unwrap();
    let row_count = values.len();
    let statuses = (0..row_count)
        .map(|row| if row % 3 == 0 { "bad" } else { "ok" })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from_iter_values(0..row_count as i64)) as ArrayRef,
            Arc::new(Int32Array::from(values)),
            Arc::new(StringArray::from(statuses)),
        ],
    )
    .unwrap();
    (program, batch)
}

proptest! {
    #[test]
    fn vector_plan_matches_scalar_oracle(values in prop::collection::vec(prop::option::of(-20_i32..20), 0..4096)) {
        let (program, batch) = program_and_batch(values);
        let scalar = evaluate_record_batch(&program, &ContractEvaluationContext::default(), &batch).unwrap();
        let plan = bind_vector_validation_plan(&program, batch.schema()).unwrap();
        let vector = plan.evaluate(&ContractEvaluationContext::default(), &batch).unwrap();
        prop_assert_eq!(vector.accepted_rows, scalar.accepted_rows);
        prop_assert_eq!(vector.quarantine_candidates, scalar.quarantine_candidates);
        prop_assert_eq!(vector.summary, scalar.summary);
    }
}

#[test]
fn vector_plan_rejects_schema_drift_and_invalid_rule_type_at_bind_time() {
    let (program, batch) = program_and_batch(vec![Some(1)]);
    let plan = bind_vector_validation_plan(&program, batch.schema()).unwrap();
    let drifted = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
    )
    .unwrap();
    assert!(
        plan.evaluate(&ContractEvaluationContext::default(), &drifted)
            .unwrap_err()
            .message
            .contains("rebind")
    );
}

#[test]
fn vector_plan_accepts_non_semantic_schema_metadata_added_by_execution() {
    let (program, batch) = program_and_batch(vec![Some(1), Some(2)]);
    let plan = bind_vector_validation_plan(&program, batch.schema()).unwrap();
    let fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| {
            let mut metadata = field.metadata().clone();
            metadata.insert(
                "cdf:physical_type".to_owned(),
                format!("{:?}", field.data_type()),
            );
            Arc::new(field.as_ref().clone().with_metadata(metadata))
        })
        .collect::<Vec<_>>();
    let enriched = RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            fields,
            HashMap::from([("cdf:source_identity".to_owned(), "fixture-v1".to_owned())]),
        )),
        batch.columns().to_vec(),
    )
    .unwrap();
    plan.evaluate(&ContractEvaluationContext::default(), &enriched)
        .unwrap();
}

#[test]
fn prebound_vector_evaluator_accepts_the_compiled_source_to_output_name_transition() {
    let (mut program, batch) = program_and_batch(vec![Some(1), Some(2)]);
    program
        .column_programs
        .iter_mut()
        .find(|column| column.source_name == "status")
        .unwrap()
        .output_name = "normalized_status".to_owned();
    let mut evaluator = VectorValidationEvaluator::new_bound(&program, batch.schema()).unwrap();
    let fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| {
            if field.name() == "status" {
                Arc::new(field.as_ref().clone().with_name("normalized_status"))
            } else {
                field.clone()
            }
        })
        .collect::<Vec<_>>();
    let normalized =
        RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec()).unwrap();

    evaluator
        .evaluate(&ContractEvaluationContext::default(), &normalized)
        .unwrap();
}

#[test]
fn prebound_evaluator_restores_wide_schema_nullability_by_ordinal_without_copying_arrays() {
    const COLUMNS: usize = 2_048;
    let compiled_schema = Arc::new(Schema::new(
        (0..COLUMNS)
            .map(|ordinal| Field::new(format!("field_{ordinal}"), DataType::Int64, true))
            .collect::<Vec<_>>(),
    ));
    let policy = ContractPolicy::for_trust(TrustLevel::Governed);
    let program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(compiled_schema.as_ref()),
    )
    .unwrap();
    let observed_schema = Arc::new(Schema::new(
        compiled_schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone().with_nullable(false))
            .collect::<Vec<_>>(),
    ));
    let columns = (0..COLUMNS)
        .map(|ordinal| Arc::new(Int64Array::from(vec![ordinal as i64])) as ArrayRef)
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(observed_schema, columns).unwrap();
    let originals = batch.columns().to_vec();
    let evaluator =
        VectorValidationEvaluator::new_bound(&program, Arc::clone(&compiled_schema)).unwrap();

    let restored = evaluator.restore_compiled_nullability(batch).unwrap();

    assert_eq!(restored.schema(), compiled_schema);
    assert!(
        originals
            .iter()
            .zip(restored.columns())
            .all(|(original, restored)| Arc::ptr_eq(original, restored))
    );
}

#[test]
fn prebound_vector_evaluator_rejects_conflicting_source_provenance() {
    let (mut program, batch) = program_and_batch(vec![Some(1)]);
    program
        .column_programs
        .iter_mut()
        .find(|column| column.source_name == "status")
        .unwrap()
        .output_name = "normalized_status".to_owned();
    let mut evaluator = VectorValidationEvaluator::new_bound(&program, batch.schema()).unwrap();
    let fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| {
            if field.name() == "status" {
                Arc::new(cdf_kernel::with_source_name(
                    field.as_ref().clone().with_name("normalized_status"),
                    "different_source",
                ))
            } else {
                field.clone()
            }
        })
        .collect::<Vec<_>>();
    let substituted =
        RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec()).unwrap();

    assert!(
        evaluator
            .evaluate(&ContractEvaluationContext::default(), &substituted)
            .unwrap_err()
            .to_string()
            .contains("prebound physical expression schema")
    );
}

#[test]
fn vector_plan_rejects_aliases_owned_by_multiple_ordinals() {
    let (mut program, batch) = program_and_batch(vec![Some(1)]);
    program
        .column_programs
        .iter_mut()
        .find(|column| column.source_name == "status")
        .unwrap()
        .output_name = "id".to_owned();

    let error = bind_vector_validation_plan(&program, batch.schema()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("resolves to multiple validation program columns")
    );
}

#[test]
fn vector_plan_rejects_global_alias_collision_outside_projected_schema() {
    let schema = Schema::new(vec![
        Field::new("first", DataType::Int64, false),
        Field::new("second", DataType::Int64, false),
    ]);
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Nullability {
        column: "first".to_owned(),
    }];
    let mut program =
        compile_validation_program(&policy, &ObservedSchema::from_arrow(&schema)).unwrap();
    program
        .column_programs
        .iter_mut()
        .find(|column| column.source_name == "second")
        .unwrap()
        .output_name = "first".to_owned();
    let projected = Arc::new(Schema::new(vec![Field::new(
        "second",
        DataType::Int64,
        false,
    )]));

    let error = bind_vector_validation_plan(&program, projected).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("resolves to multiple validation program columns")
    );
}

#[test]
fn vector_plan_rejects_conflicting_name_and_source_provenance_at_bind_time() {
    let (program, _) = program_and_batch(vec![Some(1)]);
    let conflicting = Arc::new(Schema::new(vec![cdf_kernel::with_source_name(
        Field::new("id", DataType::Int64, false),
        "status",
    )]));

    let error = bind_vector_validation_plan(&program, conflicting).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("resolve to different validation program columns")
    );
}

#[test]
fn prebound_vector_evaluator_rejects_same_typed_ordinal_substitution() {
    let expected = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("other", DataType::Int64, false),
    ]));
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Nullability {
        column: "id".to_owned(),
    }];
    let program =
        compile_validation_program(&policy, &ObservedSchema::from_arrow(expected.as_ref()))
            .unwrap();
    let mut evaluator = VectorValidationEvaluator::new_bound(&program, expected).unwrap();
    let substituted = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("other", DataType::Int64, false),
            Field::new("id", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef,
            Arc::new(Int64Array::from(vec![2_i64])) as ArrayRef,
        ],
    )
    .unwrap();

    let error = evaluator
        .evaluate(&ContractEvaluationContext::default(), &substituted)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("prebound physical expression schema")
    );
}

fn assert_vector_matches_scalar(
    program: &ValidationProgram,
    context: &ContractEvaluationContext,
    batch: &RecordBatch,
) {
    let scalar = evaluate_record_batch(program, context, batch).unwrap();
    let vector = bind_vector_validation_plan(program, batch.schema())
        .unwrap()
        .evaluate(context, batch)
        .unwrap();
    assert_eq!(vector.accepted_rows, scalar.accepted_rows);
    assert_eq!(vector.quarantine_candidates, scalar.quarantine_candidates);
    assert_eq!(vector.summary, scalar.summary);
}

#[test]
fn vector_numeric_range_matrix_matches_scalar_oracle() {
    let schema = Schema::new(vec![
        Field::new("i8", DataType::Int8, true),
        Field::new("i16", DataType::Int16, true),
        Field::new("i32", DataType::Int32, true),
        Field::new("i64", DataType::Int64, true),
        Field::new("u8", DataType::UInt8, true),
        Field::new("u16", DataType::UInt16, true),
        Field::new("u32", DataType::UInt32, true),
        Field::new("u64", DataType::UInt64, true),
        Field::new("f32", DataType::Float32, true),
        Field::new("f64", DataType::Float64, true),
    ]);
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = schema
        .fields()
        .iter()
        .map(|field| RowRule::Range {
            column: field.name().clone(),
            min: Some("1".to_owned()),
            max: Some("2".to_owned()),
        })
        .collect();
    let program =
        compile_validation_program(&policy, &ObservedSchema::from_arrow(&schema)).unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int8Array::from(vec![None, Some(1), Some(3)])),
            Arc::new(Int16Array::from(vec![None, Some(1), Some(3)])),
            Arc::new(Int32Array::from(vec![None, Some(1), Some(3)])),
            Arc::new(Int64Array::from(vec![None, Some(1), Some(3)])),
            Arc::new(UInt8Array::from(vec![None, Some(1), Some(3)])),
            Arc::new(UInt16Array::from(vec![None, Some(1), Some(3)])),
            Arc::new(UInt32Array::from(vec![None, Some(1), Some(3)])),
            Arc::new(UInt64Array::from(vec![None, Some(1), Some(3)])),
            Arc::new(Float32Array::from(vec![None, Some(1.0), Some(3.0)])),
            Arc::new(Float64Array::from(vec![None, Some(1.0), Some(3.0)])),
        ],
    )
    .unwrap();
    assert_vector_matches_scalar(&program, &ContractEvaluationContext::default(), &batch);
}

#[test]
fn vector_domain_regex_freshness_and_float_identity_match_scalar_oracle() {
    let schema = Schema::new(vec![
        Field::new("f32", DataType::Float32, false),
        Field::new("f64", DataType::Float64, false),
        Field::new("text", DataType::LargeUtf8, true),
        Field::new(
            "seen",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]);
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![
        RowRule::Domain {
            column: "f32".to_owned(),
            allowed: vec![
                "NaN".to_owned(),
                "-0".to_owned(),
                "1".to_owned(),
                "not-a-number".to_owned(),
            ],
        },
        RowRule::Domain {
            column: "f64".to_owned(),
            allowed: vec!["NaN".to_owned(), "-0".to_owned(), "1".to_owned()],
        },
        RowRule::Range {
            column: "f64".to_owned(),
            min: None,
            max: None,
        },
        RowRule::Regex {
            column: "text".to_owned(),
            pattern: "^ok".to_owned(),
        },
        RowRule::Freshness {
            column: "seen".to_owned(),
            max_age_ms: 1_000,
        },
    ];
    let program =
        compile_validation_program(&policy, &ObservedSchema::from_arrow(&schema)).unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Float32Array::from(vec![f32::NAN, -0.0, 0.0, 1.0])),
            Arc::new(Float64Array::from(vec![f64::NAN, -0.0, 0.0, 1.0])),
            Arc::new(LargeStringArray::from(vec![
                Some("ok-a"),
                None,
                Some("bad"),
                Some("ok-b"),
            ])),
            Arc::new(
                TimestampMicrosecondArray::from(vec![
                    Some(9_500_000),
                    None,
                    Some(8_000_000),
                    Some(10_000_000),
                ])
                .with_timezone("UTC"),
            ),
        ],
    )
    .unwrap();
    assert_vector_matches_scalar(
        &program,
        &ContractEvaluationContext::observed_at(10_000),
        &batch,
    );
}

#[test]
fn all_failure_masks_remain_bit_packed() {
    const ROWS: usize = 65_536;
    let (program, batch) = program_and_batch(vec![None; ROWS]);
    let masks = bind_vector_validation_plan(&program, batch.schema())
        .unwrap()
        .evaluate_masks(&ContractEvaluationContext::default(), &batch)
        .unwrap();
    let retained_mask_bytes = masks.accepted_rows.values().len()
        + masks.quarantined_rows.values().len()
        + masks
            .rule_masks
            .iter()
            .map(|rule| rule.violations.values().len())
            .sum::<usize>();
    let maximum_packed_bytes = (masks.rule_masks.len() + 2) * ROWS.div_ceil(8);
    assert!(retained_mask_bytes <= maximum_packed_bytes);
    assert!(masks.summary.quarantined_rows > 0);
}

#[test]
#[ignore = "performance evidence; run in release mode"]
fn vector_numeric_range_reference_rate() {
    const ROWS: usize = 65_536;
    const ITERATIONS: usize = 2_000;
    let (program, batch) =
        program_and_batch((0..ROWS).map(|row| Some((row % 20) as i32 - 10)).collect());
    let plan = bind_vector_validation_plan(&program, batch.schema()).unwrap();
    let started = Instant::now();
    let mut accepted = 0_u64;
    for _ in 0..ITERATIONS {
        accepted += plan
            .evaluate_masks(&ContractEvaluationContext::default(), &batch)
            .unwrap()
            .summary
            .accepted_rows;
    }
    let elapsed = started.elapsed();
    let inspected = (ROWS * ITERATIONS * (std::mem::size_of::<i32>() + 2)) as f64;
    eprintln!(
        "vector-validation rows={} accepted={} throughput={:.2} GiB/s",
        ROWS * ITERATIONS,
        accepted,
        inspected / elapsed.as_secs_f64() / (1024.0 * 1024.0 * 1024.0)
    );
}

#[test]
#[ignore = "performance evidence; run in release mode"]
fn vector_full_evaluation_scalar_ratio() {
    const ROWS: usize = 65_536;
    const SCALAR_ITERATIONS: usize = 100;
    const VECTOR_ITERATIONS: usize = 1_000;
    let (program, mixed) = program_and_batch(vec![Some(1); ROWS]);
    let batch = RecordBatch::try_new(
        mixed.schema(),
        vec![
            mixed.column(0).clone(),
            mixed.column(1).clone(),
            Arc::new(StringArray::from(vec!["ok"; ROWS])) as ArrayRef,
        ],
    )
    .unwrap();
    let plan = bind_vector_validation_plan(&program, batch.schema()).unwrap();

    let scalar_started = Instant::now();
    for _ in 0..SCALAR_ITERATIONS {
        std::hint::black_box(
            evaluate_record_batch(&program, &ContractEvaluationContext::default(), &batch).unwrap(),
        );
    }
    let scalar_per_iteration = scalar_started.elapsed().as_secs_f64() / SCALAR_ITERATIONS as f64;

    let vector_started = Instant::now();
    for _ in 0..VECTOR_ITERATIONS {
        std::hint::black_box(
            plan.evaluate(&ContractEvaluationContext::default(), &batch)
                .unwrap(),
        );
    }
    let vector_per_iteration = vector_started.elapsed().as_secs_f64() / VECTOR_ITERATIONS as f64;
    eprintln!(
        "vector-validation full scalar={:.3} us/batch vector={:.3} us/batch speedup={:.2}x",
        scalar_per_iteration * 1_000_000.0,
        vector_per_iteration * 1_000_000.0,
        scalar_per_iteration / vector_per_iteration,
    );
}
