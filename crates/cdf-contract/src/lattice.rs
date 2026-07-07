use std::collections::BTreeSet;

use cdf_kernel::{CdfError, Result};

use crate::program::{RuleOutcome, ValidationProgram};

pub fn assert_verdict_lattice_total(program: &ValidationProgram) -> Result<()> {
    let covered = program
        .row_dispositions
        .iter()
        .map(|rule| rule.outcome)
        .collect::<BTreeSet<_>>();
    for outcome in RuleOutcome::ALL {
        if !covered.contains(&outcome) {
            return Err(CdfError::contract(format!(
                "validation program lacks a row disposition for {outcome:?}"
            )));
        }
    }
    Ok(())
}
