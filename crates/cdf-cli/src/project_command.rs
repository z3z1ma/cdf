mod render;

use std::{env, fs};

use crate::{
    args::{Cli, InitArgs, ValidateArgs},
    context::{project_authority_read_error, project_location},
    error_catalog,
    output::{CliError, CommandOutput},
    suggestions,
};
use cdf_kernel::CdfError;
use cdf_project::{
    ProjectResourceSelectionError, ProjectScaffoldOptions, ProjectScaffoldReport, parse_cdf_toml,
    resolve_project_resource_selection, validate_project_static, write_local_project_scaffold,
};
use cdf_semantic::SemanticCatalog;

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

pub(crate) fn validate(cli: &Cli, args: ValidateArgs) -> Result<CommandOutput, CliError> {
    let (root, project_file) = project_location(cli.project.as_ref())?;
    let project_text = fs::read_to_string(&project_file).map_err(|error| {
        project_authority_read_error("read project configuration", &project_file, error)
    })?;
    let config = parse_cdf_toml(&project_text)?;
    let environment = cli
        .env
        .as_deref()
        .unwrap_or(&config.project.default_environment);
    config.effective_environment(environment)?;
    let selection = resolve_project_resource_selection(&root, &args.selectors, &args.exclude)
        .map_err(resource_selection_error)?;
    let source_registry = crate::source_registry::builtin_source_registry()?;
    let semantic_catalog = SemanticCatalog::builtins()?;
    let report = validate_project_static(
        source_registry,
        &root,
        &config,
        environment,
        &selection,
        &semantic_catalog,
    )?;
    let exit_code = i32::from(report.counts.errors != 0);
    CommandOutput::rendered_with_exit_code(
        "validate",
        render::validate_document(&report),
        report,
        exit_code,
    )
}

fn resource_selection_error(error: ProjectResourceSelectionError) -> CliError {
    match error {
        ProjectResourceSelectionError::Project(error) => error.into(),
        ProjectResourceSelectionError::ExactNoMatch {
            selector,
            candidates,
        } => {
            let suggestions = suggestions::nearest(&selector, candidates)
                .into_iter()
                .map(|candidate| format!("cdf validate {candidate}"))
                .collect();
            CliError::usage(format!(
                "resource selector {selector:?} matched no resource"
            ))
            .with_suggestions(suggestions)
        }
        error => CliError::usage(error.to_string()),
    }
}
