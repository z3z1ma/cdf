mod deep_validate;
mod render;

use std::{collections::BTreeMap, env};

use cdf_kernel::CdfError;
use cdf_project::{
    LockDiff, LockedDestination, ProjectScaffoldOptions, ProjectScaffoldReport,
    ProjectValidationReport, generate_lockfile_with_destination_artifacts, validate_project,
    write_local_project_scaffold,
};
use serde::Serialize;

use crate::{
    args::{Cli, InitArgs, ValidateArgs},
    context::{ProjectContext, require_lock},
    error_catalog,
    output::{CliError, CommandOutput},
};

pub(crate) fn init(args: InitArgs) -> Result<CommandOutput, CliError> {
    let root = args
        .directory
        .unwrap_or(env::current_dir().map_err(|error| {
        CliError::from(CdfError::environment(format!(
            "read current directory: {error}; change to an accessible directory or pass --directory"
        )))
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
    CommandOutput::rendered("init", render::init_document(&report), report)
}

pub(crate) fn validate(
    cli: &Cli,
    args: ValidateArgs,
    execution: &cdf_runtime::ExecutionServices,
    destinations: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    if args.deep {
        return deep_validate::run(cli, execution, destinations);
    }
    let context =
        ProjectContext::load_for_command("validate", cli.project.as_ref(), cli.env.as_deref())?;
    let provider = context.secret_provider();
    let source_registry = crate::source_registry::builtin_source_registry()?;
    let validation = validate_project(
        source_registry,
        &context.config,
        Some(&context.environment.name),
        &context.resources,
        &provider,
    )?;
    let report = ProjectValidationCliReport {
        validation,
        project_name: context.config.project.name,
    };
    CommandOutput::rendered("validate", render::validate_document(&report), report)
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
        &context.semantic_catalog,
    )?;
    let diffs = cdf_project::diff_lockfiles(lock, &regenerated)?;
    let report = DiffSchemaCliReport { diffs };
    CommandOutput::rendered("diff schema", render::diff_schema_document(&report), report)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct ProjectValidationCliReport {
    #[serde(flatten)]
    validation: ProjectValidationReport,
    #[serde(skip)]
    project_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct DiffSchemaCliReport {
    diffs: Vec<LockDiff>,
}
