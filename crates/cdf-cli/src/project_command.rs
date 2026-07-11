mod deep_validate;

use std::{collections::BTreeMap, env};

use cdf_kernel::CdfError;
use cdf_project::{
    FileResourceSourceResolver, LockDiff, LockedDestination, ProjectScaffoldOptions,
    ProjectScaffoldReport, ProjectValidationReport, generate_lockfile_with_destination_artifacts,
    validate_project, write_local_project_scaffold,
};
use serde::Serialize;

use crate::{
    args::{Cli, InitArgs, ValidateArgs},
    context::{ProjectContext, require_lock},
    error_catalog,
    output::{CliError, CommandOutput},
    render::{
        RenderDocument,
        primitives::{KeyValuePanel, NextCommand, SectionRule, StatusKind, StatusLine, Table},
    },
};

pub(crate) fn init(args: InitArgs) -> Result<CommandOutput, CliError> {
    let root = args
        .directory
        .unwrap_or(env::current_dir().map_err(|error| {
            CliError::mapped(
                CdfError::internal(format!("read current directory: {error}")),
                error_catalog::PROJECT_IO,
            )
        })?);
    let project_name = match args.name {
        Some(name) if name.trim().is_empty() => {
            return Err(CliError::usage_with(
                "init --name cannot be empty",
                error_catalog::PROJECT_INIT_ARGUMENT,
            ));
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
    CommandOutput::rendered("init", init_document(&report), report)
}

pub(crate) fn validate(
    cli: &Cli,
    args: ValidateArgs,
    execution: &cdf_runtime::ExecutionServices,
) -> Result<CommandOutput, CliError> {
    if args.deep {
        return deep_validate::run(cli, execution);
    }
    let context =
        ProjectContext::load_for_command("validate", cli.project.as_ref(), cli.env.as_deref())?;
    let resolver = FileResourceSourceResolver::new(&context.root);
    let provider = context.secret_provider();
    let report = validate_project(
        &context.config,
        Some(&context.environment.name),
        &resolver,
        &provider,
    )?;
    CommandOutput::rendered("validate", validate_document(&context, &report), report)
}

pub(crate) fn diff_schema(cli: &Cli) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let lock = require_lock(&context)?;
    let destination_artifacts = lock
        .destinations
        .values()
        .map(LockedDestination::sheet_artifact)
        .collect::<cdf_kernel::Result<Vec<_>>>()?;
    let regenerated = generate_lockfile_with_destination_artifacts(
        &context.config,
        &context.resources,
        lock.dependency_tuple.clone(),
        &destination_artifacts,
        BTreeMap::new(),
    )?;
    let diffs = cdf_project::diff_lockfiles(lock, &regenerated)?;
    let report = DiffSchemaCliReport { diffs };
    CommandOutput::rendered("diff schema", diff_schema_document(&report), report)
}

fn init_document(report: &ProjectScaffoldReport) -> RenderDocument {
    RenderDocument::new()
        .push(SectionRule::new())
        .push(StatusLine::new(
            StatusKind::Success,
            format!("initialized project {}", report.project_name),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Project")
                .row("name", report.project_name.clone())
                .row("root", report.root.clone())
                .row("force", yes_no(report.force))
                .row("created", path_list(&report.created))
                .row("replaced", path_list(&report.replaced))
                .row("skipped", path_list(&report.skipped)),
        )
        .blank_line()
        .push(NextCommand::new("cdf validate"))
}

fn validate_document(context: &ProjectContext, report: &ProjectValidationReport) -> RenderDocument {
    let secret_table = report.checked_secrets.iter().fold(
        Table::new(["secret reference", "status"]),
        |table, secret| {
            table.row([
                secret.uri.as_str().to_owned(),
                format!("{:?}", secret.status).to_lowercase(),
            ])
        },
    );

    let mut document = RenderDocument::new()
        .push(SectionRule::new())
        .push(StatusLine::new(
            StatusKind::Success,
            format!("validated project {}", context.config.project.name),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Project")
                .row("name", context.config.project.name.clone())
                .row("environment", report.environment.name.clone())
                .row(
                    "declarative resources",
                    report.declarative_resources.to_string(),
                )
                .row("external resources", report.external_resources.to_string())
                .row(
                    "secret references",
                    report.checked_secrets.len().to_string(),
                ),
        );

    if !report.checked_secrets.is_empty() {
        document = document.blank_line().push(secret_table);
    }

    document.blank_line().push(NextCommand::new("cdf plan"))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct DiffSchemaCliReport {
    diffs: Vec<LockDiff>,
}

fn diff_schema_document(report: &DiffSchemaCliReport) -> RenderDocument {
    let table = report.diffs.iter().fold(
        Table::new(["kind", "path", "before", "after"]),
        |table, diff| {
            table.row([
                format!("{:?}", diff.kind).to_lowercase(),
                diff.path.clone(),
                diff.before.clone().unwrap_or_else(|| "none".to_owned()),
                diff.after.clone().unwrap_or_else(|| "none".to_owned()),
            ])
        },
    );

    let mut document = RenderDocument::new()
        .push(SectionRule::new())
        .push(StatusLine::new(
            if report.diffs.is_empty() {
                StatusKind::Success
            } else {
                StatusKind::Warning
            },
            format!("{} lock diff(s)", report.diffs.len()),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Schema diff")
                .row("diffs", report.diffs.len().to_string())
                .row(
                    "status",
                    if report.diffs.is_empty() {
                        "lockfile matches project"
                    } else {
                        "lockfile drift detected"
                    },
                ),
        );

    if !report.diffs.is_empty() {
        document = document.blank_line().push(table);
    }

    document
        .blank_line()
        .push(NextCommand::new(if report.diffs.is_empty() {
            "cdf validate"
        } else {
            "cdf contract freeze"
        }))
}

fn path_list(paths: &[String]) -> String {
    if paths.is_empty() {
        "none".to_owned()
    } else {
        paths.join(", ")
    }
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}
