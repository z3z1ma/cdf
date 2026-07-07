use std::collections::BTreeMap;

use cdf_project::{FileResourceSourceResolver, generate_lockfile, validate_project};
use serde_json::json;

use crate::{
    args::{Cli, InitArgs},
    commands::output,
    context::{ProjectContext, require_lock},
    output::{CliError, CommandOutput},
};

pub(crate) fn init(_args: InitArgs) -> Result<CommandOutput, CliError> {
    Err(CliError::not_supported(
        "init",
        "project scaffold semantics are not exposed by cdf-project yet",
        "project template/write API",
    ))
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
