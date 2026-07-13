mod attempt;
mod destination;
mod events;
mod model;
mod report;

use cdf_kernel::{CdfError, RunEventSink, RunId};
use cdf_state_sqlite::SqliteRunLedger;
use rusqlite::{Connection, OpenFlags};
use serde::Serialize;

use crate::{
    args::{Cli, ResumeArgs},
    context::ProjectContext,
    error_catalog,
    output::{CliError, CommandOutput},
    progress::human_progress_sink,
    render::{
        RenderDocument,
        primitives::{KeyValuePanel, SectionRule, StatusKind, StatusLine},
    },
};

use self::{attempt::ResumeAttempt, report::finish_resume_report};

pub(crate) fn resume(
    cli: &Cli,
    args: ResumeArgs,
    execution: &cdf_runtime::ExecutionServices,
) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let state_path = context.state_store_path()?;
    if !state_path.exists() {
        return Err(CliError::mapped(
            CdfError::data(format!(
                "run ledger state database {} is missing",
                state_path.display()
            )),
            error_catalog::RESUME_LEDGER,
        ));
    }
    let run_id = match args.run_id {
        Some(run_id) => RunId::new(run_id)?,
        None => match select_resume_run(&state_path)? {
            ResumeSelection::None => return no_interrupted_runs_report(),
            ResumeSelection::One(run_id) => run_id,
            ResumeSelection::Many(run_ids) => {
                return Err(CliError::not_supported_with(
                    "resume",
                    format!(
                        "bare resume found {} interrupted runs ({}); pass RUN_ID to resume one explicitly",
                        run_ids.len(),
                        run_ids.join(", ")
                    ),
                    "multi-run resume drain",
                    error_catalog::RESUME_MULTI_RUN_NOT_SUPPORTED,
                ));
            }
        },
    };
    resume_run(
        &context,
        &state_path,
        run_id,
        cli.json,
        &cli.terminal,
        execution,
    )
}

fn resume_run(
    context: &ProjectContext,
    state_path: &std::path::Path,
    run_id: RunId,
    json_mode: bool,
    terminal: &crate::terminal::TerminalPolicy,
    execution: &cdf_runtime::ExecutionServices,
) -> Result<CommandOutput, CliError> {
    let run_ledger = SqliteRunLedger::open(state_path)?;
    let snapshot = run_ledger.snapshot(&run_id)?.ok_or_else(|| {
        CdfError::data(format!(
            "run {} is not present in the selected environment run ledger",
            run_id
        ))
    })?;
    let progress = human_progress_sink(json_mode, terminal);
    let event_sink = progress.as_ref().map(|sink| sink as &dyn RunEventSink);
    if let Some(sink) = event_sink {
        for event in &snapshot.events {
            let _ = sink.try_emit(event);
        }
    }
    let attempt = ResumeAttempt::new(context, &run_ledger, &snapshot, event_sink, execution)?;
    let outcome = attempt.execute();
    match outcome {
        Ok(report) => finish_resume_report(report, progress.map(|progress| progress.snapshot())),
        Err(error) => {
            let report = attempt.fail_closed("recovery_failed", "fail_closed", error.message);
            let _ = attempt.append_run_failed(&report);
            finish_resume_report(report, progress.map(|progress| progress.snapshot()))
        }
    }
}

enum ResumeSelection {
    None,
    One(RunId),
    Many(Vec<String>),
}

fn select_resume_run(state_path: &std::path::Path) -> Result<ResumeSelection, CliError> {
    let _ = SqliteRunLedger::open_read_only(state_path)?;
    let conn = Connection::open_with_flags(state_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|error| CdfError::data(format!("open run ledger read-only: {error}")))?;
    let mut stmt = conn
        .prepare(
            "
            SELECT r.run_id
            FROM cdf_runs r
            WHERE COALESCE((
                SELECT e.kind
                FROM cdf_run_events e
                WHERE e.run_id = r.run_id
                ORDER BY e.sequence DESC
                LIMIT 1
            ), '') NOT IN ('run_succeeded', 'run_resumed', 'replay_recorded')
            ORDER BY r.created_at_ms, r.run_id
            ",
        )
        .map_err(|error| CdfError::data(format!("prepare interrupted-run scan: {error}")))?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|error| CdfError::data(format!("scan interrupted runs: {error}")))?;
    let run_ids = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| CdfError::data(format!("read interrupted run id: {error}")))?;
    match run_ids.as_slice() {
        [] => Ok(ResumeSelection::None),
        [run_id] => Ok(ResumeSelection::One(RunId::new(run_id.clone())?)),
        _ => Ok(ResumeSelection::Many(run_ids)),
    }
}

#[derive(Serialize)]
struct BareResumeReport {
    state: &'static str,
    interrupted_runs: Vec<String>,
    writes: crate::reports::WriteEffects,
}

fn no_interrupted_runs_report() -> Result<CommandOutput, CliError> {
    let report = BareResumeReport {
        state: "no_interrupted_runs",
        interrupted_runs: Vec::new(),
        writes: crate::reports::WriteEffects::none(),
    };
    CommandOutput::rendered("resume", bare_resume_document(&report), report)
}

fn bare_resume_document(report: &BareResumeReport) -> RenderDocument {
    RenderDocument::new()
        .push(SectionRule::new())
        .push(StatusLine::new(
            StatusKind::Success,
            "no interrupted runs found",
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Resume")
                .row("state", report.state)
                .row(
                    "interrupted runs",
                    report.interrupted_runs.len().to_string(),
                )
                .row("package written", "no")
                .row("destination written", "no")
                .row("checkpoint written", "no")
                .row(
                    "mutation performed",
                    "none; no package, destination, checkpoint, or run-ledger writes",
                ),
        )
}
