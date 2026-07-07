mod attempt;
mod destination;
mod events;
mod model;
mod report;

use cdf_kernel::{CdfError, RunId};
use cdf_state_sqlite::SqliteRunLedger;

use crate::{
    args::{Cli, ResumeArgs},
    context::ProjectContext,
    output::{CliError, CommandOutput},
};

use self::{attempt::ResumeAttempt, report::finish_resume_report};

pub(crate) fn resume(cli: &Cli, args: ResumeArgs) -> Result<CommandOutput, CliError> {
    let run_id = args
        .run_id
        .ok_or_else(|| CliError::usage("resume requires --run RUN_ID or positional RUN_ID"))?;
    let run_id = RunId::new(run_id)?;
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let state_path = context.state_store_path()?;
    if !state_path.exists() {
        return Err(CliError::from(CdfError::data(format!(
            "run ledger state database {} is missing",
            state_path.display()
        ))));
    }
    let run_ledger = SqliteRunLedger::open(&state_path)?;
    let snapshot = run_ledger.snapshot(&run_id)?.ok_or_else(|| {
        CdfError::data(format!(
            "run {} is not present in the selected environment run ledger",
            run_id
        ))
    })?;
    let attempt = ResumeAttempt::new(&context, &run_ledger, &snapshot)?;
    let outcome = attempt.execute();
    match outcome {
        Ok(report) => finish_resume_report(report),
        Err(error) => {
            let report = attempt.fail_closed("recovery_failed", "fail_closed", error.message);
            let _ = attempt.append_run_failed(&report);
            finish_resume_report(report)
        }
    }
}
