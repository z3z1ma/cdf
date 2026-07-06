use std::collections::BTreeSet;

use firn_kernel::{FirnError, Result};

use crate::program::{RuleOutcome, ValidationProgram};

pub fn assert_verdict_lattice_total(program: &ValidationProgram) -> Result<()> {
    let covered = program
        .row_dispositions
        .iter()
        .map(|rule| rule.outcome)
        .collect::<BTreeSet<_>>();
    for outcome in RuleOutcome::ALL {
        if !covered.contains(&outcome) {
            return Err(FirnError::contract(format!(
                "validation program lacks a row disposition for {outcome:?}"
            )));
        }
    }
    Ok(())
}
