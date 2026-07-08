use std::collections::BTreeSet;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use cdf_contract::{
    ContractEvaluationContext, ContractPolicy, ObservedSchema, PromotionPolicy, RowDispositionKind,
    RowDispositionRule, RowRule, RuleOutcome, ValidationProgram, assert_verdict_lattice_total,
    compile_validation_program, evaluate_record_batch,
};
use proptest::prelude::*;
use std::sync::Arc;

fn validation_program(row_dispositions: Vec<RowDispositionRule>) -> ValidationProgram {
    ValidationProgram {
        normalizer_version: "property-fuzz".to_owned(),
        schema_verdicts: Vec::new(),
        column_programs: Vec::new(),
        row_rules: Vec::new(),
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
