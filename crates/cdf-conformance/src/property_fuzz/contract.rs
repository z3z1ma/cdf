use std::collections::BTreeSet;

use arrow_array::{
    Array, ArrayRef, Date32Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray, UInt8Array, UInt16Array,
    UInt32Array, UInt64Array,
};
use arrow_cast::cast::cast;
use arrow_schema::{DataType, Field, Schema};
use cdf_contract::{
    ContractEvaluationContext, ContractPolicy, FieldCoercionDecision, NestedAction, ObservedSchema,
    PromotionPolicy, RowDispositionKind, RowDispositionRule, RowRule, RuleOutcome,
    VARIANT_COLUMN_NAME, VARIANT_SEMANTIC_TAG, ValidationProgram, assert_verdict_lattice_total,
    compile_validation_program, evaluate_record_batch, reconcile_schema,
};
use proptest::prelude::*;
use std::sync::Arc;

fn validation_program(row_dispositions: Vec<RowDispositionRule>) -> ValidationProgram {
    ValidationProgram {
        normalizer_version: "property-fuzz".to_owned(),
        schema_coercion: None,
        schema_verdicts: Vec::new(),
        column_programs: Vec::new(),
        row_rules: Vec::new(),
        explicit_anomalies: Vec::new(),
        row_dispositions,
        transforms: Vec::new(),
        promotion: PromotionPolicy::default(),
        warnings: Vec::new(),
    }
}

fn outcome_strategy() -> impl Strategy<Value = RuleOutcome> {
    prop_oneof![
        Just(RuleOutcome::Pass),
        Just(RuleOutcome::Coerced),
        Just(RuleOutcome::AdmittedAsVariant),
        Just(RuleOutcome::Violation),
        Just(RuleOutcome::Fatal),
    ]
}

fn disposition_strategy() -> impl Strategy<Value = RowDispositionKind> {
    prop_oneof![
        Just(RowDispositionKind::Accept),
        Just(RowDispositionKind::Quarantine),
        Just(RowDispositionKind::RejectBatch),
        Just(RowDispositionKind::RejectRun),
    ]
}

fn row_disposition_strategy() -> impl Strategy<Value = RowDispositionRule> {
    (outcome_strategy(), disposition_strategy()).prop_map(|(outcome, disposition)| {
        RowDispositionRule {
            outcome,
            disposition,
        }
    })
}

fn covers_every_outcome(rules: &[RowDispositionRule]) -> bool {
    let outcomes = rules
        .iter()
        .map(|rule| rule.outcome)
        .collect::<BTreeSet<_>>();
    RuleOutcome::ALL
        .iter()
        .all(|outcome| outcomes.contains(outcome))
}

fn accept_rules_for_all_outcomes() -> Vec<RowDispositionRule> {
    RuleOutcome::ALL
        .iter()
        .copied()
        .map(|outcome| RowDispositionRule {
            outcome,
            disposition: RowDispositionKind::Accept,
        })
        .collect()
}

fn assert_all_permutations_are_total(rules: &mut [RowDispositionRule], start: usize) {
    if start == rules.len() {
        let program = validation_program(rules.to_vec());
        assert_verdict_lattice_total(&program).unwrap();
        return;
    }

    for index in start..rules.len() {
        rules.swap(start, index);
        assert_all_permutations_are_total(rules, start + 1);
        rules.swap(start, index);
    }
}

proptest! {
    #[test]
    fn property_fuzz_verdict_lattice_totality_matches_outcome_coverage(
        row_dispositions in prop::collection::vec(row_disposition_strategy(), 0..=20)
    ) {
        let expected = covers_every_outcome(&row_dispositions);
        let program = validation_program(row_dispositions);

        prop_assert_eq!(assert_verdict_lattice_total(&program).is_ok(), expected);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn property_fuzz_signed_integer_widening_composes_without_value_loss(
        values in prop::collection::vec(any::<i8>(), 0..=128)
    ) {
        assert_widening_decision(&DataType::Int8, &DataType::Int16);
        assert_widening_decision(&DataType::Int16, &DataType::Int32);
        assert_widening_decision(&DataType::Int32, &DataType::Int64);

        let i8_array = Int8Array::from(values.clone());
        let i16_step = cast(&i8_array, &DataType::Int16).unwrap();
        let i32_step = cast(i16_step.as_ref(), &DataType::Int32).unwrap();
        let i64_step = cast(i32_step.as_ref(), &DataType::Int64).unwrap();
        let i64_direct = cast(&i8_array, &DataType::Int64).unwrap();

        let i16_step = i16_step.as_any().downcast_ref::<Int16Array>().unwrap();
        let i32_step = i32_step.as_any().downcast_ref::<Int32Array>().unwrap();
        let i64_step = i64_step.as_any().downcast_ref::<Int64Array>().unwrap();
        let i64_direct = i64_direct.as_any().downcast_ref::<Int64Array>().unwrap();

        for (index, value) in values.iter().copied().enumerate() {
            prop_assert_eq!(i16_step.value(index), i16::from(value));
            prop_assert_eq!(i32_step.value(index), i32::from(value));
            prop_assert_eq!(i64_step.value(index), i64::from(value));
            prop_assert_eq!(i64_direct.value(index), i64_step.value(index));
        }
    }

    #[test]
    fn property_fuzz_unsigned_integer_widening_composes_without_value_loss(
        values in prop::collection::vec(any::<u8>(), 0..=128)
    ) {
        assert_widening_decision(&DataType::UInt8, &DataType::UInt16);
        assert_widening_decision(&DataType::UInt16, &DataType::UInt32);
        assert_widening_decision(&DataType::UInt32, &DataType::UInt64);

        let u8_array = UInt8Array::from(values.clone());
        let u16_step = cast(&u8_array, &DataType::UInt16).unwrap();
        let u32_step = cast(u16_step.as_ref(), &DataType::UInt32).unwrap();
        let u64_step = cast(u32_step.as_ref(), &DataType::UInt64).unwrap();
        let u64_direct = cast(&u8_array, &DataType::UInt64).unwrap();

        let u16_step = u16_step.as_any().downcast_ref::<UInt16Array>().unwrap();
        let u32_step = u32_step.as_any().downcast_ref::<UInt32Array>().unwrap();
        let u64_step = u64_step.as_any().downcast_ref::<UInt64Array>().unwrap();
        let u64_direct = u64_direct.as_any().downcast_ref::<UInt64Array>().unwrap();

        for (index, value) in values.iter().copied().enumerate() {
            prop_assert_eq!(u16_step.value(index), u16::from(value));
            prop_assert_eq!(u32_step.value(index), u32::from(value));
            prop_assert_eq!(u64_step.value(index), u64::from(value));
            prop_assert_eq!(u64_direct.value(index), u64_step.value(index));
        }
    }

    #[test]
    fn property_fuzz_float_widening_preserves_finite_float32_values(
        raw_values in prop::collection::vec(-1_000_000i32..=1_000_000, 0..=128)
    ) {
        assert_widening_decision(&DataType::Float32, &DataType::Float64);

        let values = raw_values
            .into_iter()
            .map(|value| value as f32 / 8.0)
            .collect::<Vec<_>>();
        let float32_array = Float32Array::from(values.clone());
        let float64_array = cast(&float32_array, &DataType::Float64).unwrap();
        let float64_array = float64_array
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        for (index, value) in values.iter().copied().enumerate() {
            prop_assert_eq!(float64_array.value(index), f64::from(value));
        }
    }

    #[test]
    fn property_fuzz_date32_to_timestamp_widening_preserves_day_instants(
        values in prop::collection::vec(-100_000i32..=100_000, 0..=128)
    ) {
        let timestamp_type = DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into()));
        assert_widening_decision(&DataType::Date32, &timestamp_type);

        let date32_array = Date32Array::from(values.clone());
        let timestamp_array = cast(&date32_array, &timestamp_type).unwrap();
        let timestamp_array = timestamp_array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();

        for (index, value) in values.iter().copied().enumerate() {
            prop_assert_eq!(timestamp_array.value(index), i64::from(value) * 86_400_000_000);
        }
    }
}

fn assert_widening_decision(observed_type: &DataType, constraint_type: &DataType) {
    let observed = Schema::new(vec![Field::new("value", observed_type.clone(), false)]);
    let constraint = Schema::new(vec![Field::new("value", constraint_type.clone(), false)]);
    let reconciliation =
        reconcile_schema(&observed, &constraint, &ContractPolicy::default().types).unwrap();
    let decision = reconciliation
        .plan
        .fields
        .iter()
        .find(|field| field.source_name == "value")
        .unwrap();

    assert_eq!(decision.decision, FieldCoercionDecision::Widened);
    assert_eq!(
        reconciliation
            .schema
            .field_with_name("value")
            .unwrap()
            .data_type(),
        constraint_type
    );
}

#[test]
fn property_fuzz_verdict_lattice_accepts_every_outcome_permutation() {
    let mut rules = accept_rules_for_all_outcomes();

    assert_all_permutations_are_total(&mut rules, 0);
}

#[test]
fn conformance_local_contract_evaluator_owns_row_verdict_path() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, false),
    ]);
    let mut policy = ContractPolicy::for_trust(cdf_kernel::TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Domain {
        column: "status".to_owned(),
        allowed: vec!["accepted".to_owned()],
    }];
    let program =
        compile_validation_program(&policy, &ObservedSchema::from_arrow(&schema)).unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["accepted", "rejected"])),
        ],
    )
    .unwrap();

    let evaluation =
        evaluate_record_batch(&program, &ContractEvaluationContext::default(), &batch).unwrap();

    assert_eq!(evaluation.summary.input_rows, 2);
    assert_eq!(evaluation.summary.accepted_rows, 1);
    assert_eq!(evaluation.summary.quarantined_rows, 1);
    assert_eq!(evaluation.quarantine_candidates[0].source_row_ordinal, 1);
}

#[test]
fn conformance_nested_unknown_fields_compile_to_variant_capture() {
    let schema = Schema::new(vec![Field::new_struct(
        "payload",
        vec![Field::new("id", DataType::Int64, false)],
        true,
    )]);
    let program = compile_validation_program(
        &ContractPolicy::for_trust(cdf_kernel::TrustLevel::Experimental),
        &ObservedSchema::from_arrow(&schema),
    )
    .unwrap();

    assert_eq!(
        program.column_programs[0].nested_action,
        NestedAction::CaptureVariant {
            column_name: VARIANT_COLUMN_NAME.to_owned(),
            semantic: VARIANT_SEMANTIC_TAG.to_owned(),
        }
    );
}
