use std::collections::BTreeSet;

use cdf_contract::{
    PromotionPolicy, RowDispositionKind, RowDispositionRule, RuleOutcome, ValidationProgram,
    assert_verdict_lattice_total,
};
use proptest::prelude::*;

fn validation_program(row_dispositions: Vec<RowDispositionRule>) -> ValidationProgram {
    ValidationProgram {
        normalizer_version: "property-fuzz".to_owned(),
        schema_verdicts: Vec::new(),
        column_programs: Vec::new(),
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
