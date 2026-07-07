use std::{fs, path::PathBuf};

use cdf_declarative::{
    CompiledResource, CompiledResourcePlan, RestResource, RestRuntimeDependencies, SqlResource,
    SqlRuntimeDependencies,
};
use cdf_kernel::{CdfError, CheckpointId, PipelineId, TargetName};
use cdf_project::{ProjectRunDestination, ProjectRunRequest, ProjectRunResource, run_project};

use crate::{
    args::{Cli, RunArgs, ScanArgs},
    commands::{RunCliReport, RunDestinationReport, build_engine_plan, output},
    context::ProjectContext,
    destination_uri::{parquet_filesystem_root, postgres_database_url, redact_error_value},
    http_transport::ReqwestHttpTransport,
    output::{CliError, CommandOutput},
};

pub(crate) fn run(cli: &Cli, args: RunArgs) -> Result<CommandOutput, CliError> {
    if args.loop_mode {
        return Err(CliError::not_supported(
            "run --loop",
            "the local development loop supervisor is excluded from this explicit one-package run slice",
            "later loop/streaming supervisor",
        ));
    }
    let explicit = explicit_run_args(args)?;
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let resource = context.resource(&explicit.resource_id)?;
    let (destination, destination_report, secret_database_url) =
        build_project_run_destination(&context, &explicit.target)?;
    let run_resource = build_project_run_resource(&context, resource)?;
    let state_store_path = context.state_store_path()?;
    let plan = build_engine_plan(
        &context,
        &ScanArgs {
            resource_id: explicit.resource_id.clone(),
            projection: None,
            filters: Vec::new(),
            limit: None,
            order_by: Vec::new(),
            package_id: Some(explicit.package_id.clone()),
        },
    )?;
    let report = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: run_resource.as_project_resource(),
        plan,
        package_root: context.package_root(),
        state_store_path,
        pipeline_id: explicit.pipeline_id.clone(),
        package_id: explicit.package_id.clone(),
        checkpoint_id: explicit.checkpoint_id.clone(),
        destination,
        run_id: None,
        after_receipt_verified: None,
    }))
    .map_err(|error| redact_error_value(error, secret_database_url.as_deref()))?;
    let cli_report = RunCliReport::from_report(&report, destination_report);
    let human = cli_report.human_message();
    output("run", human, cli_report)
}

fn explicit_run_args(args: RunArgs) -> Result<ExplicitRunArgs, CliError> {
    Ok(ExplicitRunArgs {
        resource_id: required_run_arg(args.resource_id, "--resource")?,
        pipeline_id: PipelineId::new(required_run_arg(args.pipeline_id, "--pipeline")?)?,
        target: TargetName::new(required_run_arg(args.target, "--target")?)?,
        package_id: required_run_arg(args.package_id, "--package-id")?,
        checkpoint_id: CheckpointId::new(required_run_arg(args.checkpoint_id, "--checkpoint-id")?)?,
    })
}

fn required_run_arg(value: Option<String>, name: &str) -> Result<String, CliError> {
    value.ok_or_else(|| CliError::usage(format!("run requires {name}")))
}

struct ExplicitRunArgs {
    resource_id: String,
    pipeline_id: PipelineId,
    target: TargetName,
    package_id: String,
    checkpoint_id: CheckpointId,
}

enum CliProjectRunResource<'a> {
    LocalFile(&'a CompiledResource),
    Rest(Box<RestResource>),
    Sql(Box<SqlResource>),
}

impl<'a> CliProjectRunResource<'a> {
    fn as_project_resource(&'a self) -> ProjectRunResource<'a> {
        match self {
            Self::LocalFile(resource) => ProjectRunResource::local_file(resource),
            Self::Rest(resource) => ProjectRunResource::rest(resource.as_ref()),
            Self::Sql(resource) => ProjectRunResource::sql(resource.as_ref()),
        }
    }
}

fn build_project_run_resource<'a>(
    context: &ProjectContext,
    resource: &'a CompiledResource,
) -> Result<CliProjectRunResource<'a>, CliError> {
    match resource.plan() {
        CompiledResourcePlan::Files(_) => Ok(CliProjectRunResource::LocalFile(resource)),
        CompiledResourcePlan::Rest(_) => {
            let dependencies = RestRuntimeDependencies::new(ReqwestHttpTransport::new()?)
                .with_secret_provider(context.secret_provider());
            Ok(CliProjectRunResource::Rest(Box::new(
                resource.to_rest_resource(dependencies)?,
            )))
        }
        CompiledResourcePlan::Sql(_) => {
            let dependencies =
                SqlRuntimeDependencies::new().with_secret_provider(context.secret_provider());
            Ok(CliProjectRunResource::Sql(Box::new(
                resource.to_sql_resource(dependencies)?,
            )))
        }
    }
}

fn build_project_run_destination(
    context: &ProjectContext,
    target: &TargetName,
) -> Result<(ProjectRunDestination, RunDestinationReport, Option<String>), CliError> {
    if context.environment.destination.starts_with("postgres://") {
        let target = cdf_dest_postgres::PostgresTarget::parse(target.as_str())?;
        let policy = context
            .environment
            .destination_policy
            .postgres
            .as_ref()
            .ok_or_else(|| {
                CdfError::contract(format!(
                    "Postgres cdf run requires [environments.{}.destination_policy.postgres] merge_dedup = \"fail\"",
                    context.environment.name
                ))
            })?;
        let dedup = match policy.merge_dedup {
            cdf_project::PostgresMergeDedupPolicy::Fail => {
                cdf_dest_postgres::MergeDedupPolicy::Fail
            }
        };
        let (database_url, secret_backed) =
            postgres_database_url(context, &context.environment.destination, "run")?;
        let secret_database_url = secret_backed.then(|| database_url.clone());
        let target_name = target.display_name();
        return Ok((
            ProjectRunDestination::Postgres {
                database_url,
                target,
                dedup,
                existing_table: None,
            },
            RunDestinationReport::postgres(target_name),
            secret_database_url,
        ));
    }

    if context.environment.destination.starts_with("parquet://") {
        let root = parquet_filesystem_root(context, &context.environment.destination, "run")?;
        return Ok((
            ProjectRunDestination::ParquetFilesystem {
                root: root.clone(),
                target: target.clone(),
            },
            RunDestinationReport::parquet(root.display().to_string(), target.to_string()),
            None,
        ));
    }

    let destination_path = run_duckdb_destination_path(context)?;
    Ok((
        ProjectRunDestination::DuckDb {
            database_path: destination_path.clone(),
            target: target.clone(),
        },
        RunDestinationReport::duckdb(destination_path.display().to_string(), target.to_string()),
        None,
    ))
}

fn run_duckdb_destination_path(context: &ProjectContext) -> Result<PathBuf, CliError> {
    let Some(raw_path) = context.environment.destination.strip_prefix("duckdb://") else {
        return Err(CliError::not_supported(
            "run",
            format!(
                "destination URI `{}` is unsupported for this slice; only local duckdb:// destinations are supported",
                context.environment.destination
            ),
            "local DuckDB destination runtime",
        ));
    };
    if raw_path.trim().is_empty() || raw_path.contains("://") {
        return Err(CliError::not_supported(
            "run",
            format!(
                "destination URI `{}` is malformed or non-local for this slice; expected duckdb://path",
                context.environment.destination
            ),
            "local DuckDB destination path",
        ));
    }
    let destination_path = context
        .duckdb_destination_path()
        .expect("duckdb:// prefix was checked");
    cdf_dest_duckdb::DuckDbDestination::new(&destination_path)?;
    Ok(destination_path)
}

pub(crate) fn ensure_parent_directory(path: &std::path::Path) -> Result<(), CliError> {
    let Some(parent) = path.parent() else {
        return Err(CliError::from(CdfError::internal(format!(
            "{} has no parent directory",
            path.display()
        ))));
    };
    fs::create_dir_all(parent).map_err(|error| {
        CliError::from(CdfError::data(format!(
            "create {}: {error}",
            parent.display()
        )))
    })
}
