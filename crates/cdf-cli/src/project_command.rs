use std::{collections::BTreeMap, env};

use cdf_kernel::CdfError;
use cdf_project::{
    FileResourceSourceResolver, ProjectScaffoldOptions, ProjectScaffoldReport, generate_lockfile,
    validate_project, write_local_project_scaffold,
};
use serde_json::json;

use crate::{
    args::{Cli, InitArgs},
    commands::output,
    context::{ProjectContext, require_lock},
    output::{CliError, CommandOutput},
};

pub(crate) fn init(args: InitArgs) -> Result<CommandOutput, CliError> {
    let root = args
        .directory
        .unwrap_or(env::current_dir().map_err(|error| {
            CliError::from(CdfError::internal(format!(
                "read current directory: {error}"
            )))
        })?);
    let project_name = match args.name {
        Some(name) if name.trim().is_empty() => {
            return Err(CliError::usage("init --name cannot be empty"));
        }
        other => other,
    };
    let report = write_local_project_scaffold(ProjectScaffoldOptions {
        root,
        project_name,
        force: args.force,
    })?;
    init_output(report)
}

fn init_output(report: ProjectScaffoldReport) -> Result<CommandOutput, CliError> {
    let human = init_human_summary(&report);
    output("init", human, report)
}

fn init_human_summary(report: &ProjectScaffoldReport) -> String {
    format!(
        "initialized CDF project {} at {}: created {}; replaced {}; skipped {}",
        report.project_name,
        report.root,
        path_list(&report.created),
        path_list(&report.replaced),
        path_list(&report.skipped)
    )
}

fn path_list(paths: &[String]) -> String {
    if paths.is_empty() {
        "none".to_owned()
    } else {
        paths.join(", ")
    }
}

pub(crate) fn validate(cli: &Cli) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let resolver = FileResourceSourceResolver::new(&context.root);
    let provider = context.secret_provider();
    let report = validate_project(
        &context.config,
        Some(&context.environment.name),
        &resolver,
        &provider,
    )?;
    let human = format!(
        "validated project {} env {}: {} declarative resource(s), {} external resource(s), {} secret reference(s)",
        context.config.project.name,
        report.environment.name,
        report.declarative_resources,
        report.external_resources,
        report.checked_secrets.len()
    );
    output("validate", human, report)
}

pub(crate) fn diff_schema(cli: &Cli) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let lock = require_lock(&context)?;
    let destination_sheets = lock
        .destinations
        .values()
        .map(|destination| destination.sheet.clone())
        .collect::<Vec<_>>();
    let regenerated = generate_lockfile(
        &context.config,
        &context.resources,
        lock.dependency_tuple.clone(),
        &destination_sheets,
        BTreeMap::new(),
    )?;
    let diffs = cdf_project::diff_lockfiles(lock, &regenerated)?;
    output(
        "diff schema",
        format!("{} lock diff(s)", diffs.len()),
        json!({ "diffs": diffs }),
    )
}
