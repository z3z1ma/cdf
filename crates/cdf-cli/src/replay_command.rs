use std::path::{Path, PathBuf};

use cdf_kernel::{CdfError, Receipt, RunId, TargetName};
use cdf_package::{PackageReader, PackageReplayInputs};
use cdf_project::{
    DestinationPolicy, PackageArtifactRecoveryRequest, PackageArtifactReplayRequest,
    PackageReplayReport, PostgresDestinationPolicy, PostgresMergeDedupPolicy,
    ProjectResolutionContext, ResolvedProjectDestination, recover_package_from_artifacts,
    replay_package_from_artifacts, resolve_project_run_destination,
};
use cdf_state_sqlite::{RunEventAppend, RunEventKind, SqliteCheckpointStore, SqliteRunLedger};

use crate::{
    args::{Cli, ReplayPackageArgs},
    commands::output,
    context::ProjectContext,
    destination_uri::redact_error_value,
    output::{CliError, CommandOutput},
    reports::{
        PreparedReplayReportRef, ReplayPackageCliReport, RunDestinationReport, replay_event_details,
    },
    run_command::ensure_parent_directory,
};

#[derive(Clone, Copy)]
pub(crate) struct PackageReplayDestinationArgs<'a> {
    pub(crate) destination_uri: Option<&'a str>,
    pub(crate) target: Option<&'a str>,
    pub(crate) merge_dedup: Option<&'a str>,
}

pub(crate) struct ReplayDestination {
    destination: Option<ResolvedProjectDestination>,
    report: RunDestinationReport,
    kind: &'static str,
    secret_redaction: Option<String>,
}

pub(crate) struct PackageReplayContext {
    pub(crate) project: ProjectContext,
    pub(crate) reader: PackageReader,
    pub(crate) package_id: String,
    pub(crate) inputs: PackageReplayInputs,
}

impl ReplayDestination {
    pub(crate) fn replay(
        &mut self,
        package_dir: PathBuf,
        store: &SqliteCheckpointStore,
        run_ledger: &SqliteRunLedger,
        run_id: &RunId,
    ) -> Result<PackageReplayReport, CliError> {
        let destination = self
            .destination
            .take()
            .ok_or_else(|| CdfError::internal("replay destination was already consumed"))?;
        let result = replay_package_from_artifacts(PackageArtifactReplayRequest {
            package_dir,
            destination,
            checkpoint_store: store,
            after_receipt_verified: None,
        })
        .map_err(|error| redact_error_value(error, self.secret_redaction.as_deref()));
        match result {
            Ok(report) => Ok(report),
            Err(error) => {
                let _ =
                    run_ledger.append_event(run_id, RunEventAppend::new(RunEventKind::RunFailed));
                Err(error.into())
            }
        }
    }

    pub(crate) fn recover(
        &mut self,
        package_dir: PathBuf,
        store: &SqliteCheckpointStore,
        receipt: Receipt,
    ) -> Result<PackageReplayReport, CliError> {
        let destination = self
            .destination
            .take()
            .ok_or_else(|| CdfError::internal("replay destination was already consumed"))?;
        recover_package_from_artifacts(PackageArtifactRecoveryRequest {
            package_dir,
            destination,
            checkpoint_store: store,
            receipt,
            after_receipt_verified: None,
        })
        .map_err(|error| redact_error_value(error, self.secret_redaction.as_deref()).into())
    }

    pub(crate) fn report(&self) -> &RunDestinationReport {
        &self.report
    }

    pub(crate) fn kind(&self) -> &'static str {
        self.kind
    }
}

pub(crate) fn load_package_replay_context(
    cli: &Cli,
    package_dir: &Path,
) -> Result<PackageReplayContext, CliError> {
    let project = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let reader = PackageReader::open(package_dir)?;
    let package_id = reader.manifest().identity.package_id.clone();
    let inputs = reader.replay_inputs()?;
    Ok(PackageReplayContext {
        project,
        reader,
        package_id,
        inputs,
    })
}

fn replay_report_ref(report: &PackageReplayReport) -> PreparedReplayReportRef<'_> {
    PreparedReplayReportRef {
        checkpoint: &report.checkpoint,
        receipt: &report.receipt,
        receipt_source: report.receipt_source.clone(),
        package_status: &report.package_status,
    }
}

pub(crate) fn build_replay_destination(
    context: &ProjectContext,
    args: PackageReplayDestinationArgs<'_>,
    inputs: &PackageReplayInputs,
) -> Result<ReplayDestination, CliError> {
    let uri = args
        .destination_uri
        .unwrap_or(context.environment.destination.as_str());
    let secret_provider = context.secret_provider();
    let replay_policy;
    let destination_policy = if uri.starts_with("postgres://") {
        if args.target.is_none() {
            return Err(CliError::usage(
                "replay package to Postgres requires --target schema.table",
            ));
        }
        replay_policy = replay_postgres_policy(args)?;
        &replay_policy
    } else {
        &context.environment.destination_policy
    };
    let target = replay_target(args, inputs, uri)?;
    let destination_context = ProjectResolutionContext::for_project_run(&context.root, &target)
        .with_environment_name(&context.environment.name)
        .with_destination_policy(destination_policy)
        .with_secret_provider(&secret_provider);
    let destination = resolve_project_run_destination(uri, &destination_context)
        .map_err(|error| replay_destination_resolution_error(error, uri))?;
    let report = RunDestinationReport::from_project(&destination.describe(), destination.target());
    let secret_redaction = destination.secret_redaction().map(str::to_owned);
    let kind = match destination
        .describe()
        .schemes
        .first()
        .copied()
        .unwrap_or("destination")
    {
        "duckdb" => "duckdb",
        "parquet" => "parquet",
        "postgres" => "postgres",
        _ => "destination",
    };
    Ok(ReplayDestination {
        destination: Some(destination),
        report,
        kind,
        secret_redaction,
    })
}

fn replay_target(
    args: PackageReplayDestinationArgs<'_>,
    inputs: &PackageReplayInputs,
    uri: &str,
) -> Result<TargetName, CliError> {
    if uri.starts_with("postgres://") {
        let explicit = args.target.ok_or_else(|| {
            CliError::usage("replay package to Postgres requires --target schema.table")
        })?;
        let target = replay_postgres_target(explicit)?;
        if target.display_name() != inputs.destination_commit.target.as_str() {
            return Err(CliError::from(CdfError::contract(format!(
                "explicit Postgres replay target {} does not match package destination commit target {}",
                target.display_name(),
                inputs.destination_commit.target
            ))));
        }
        return TargetName::new(target.display_name()).map_err(CliError::from);
    }
    Ok(inputs.destination_commit.target.clone())
}

fn replay_postgres_target(target: &str) -> Result<cdf_dest_postgres::PostgresTarget, CliError> {
    if target.split('.').count() != 2 {
        return Err(CliError::usage(
            "replay package to Postgres requires --target schema.table",
        ));
    }
    cdf_dest_postgres::PostgresTarget::parse(target).map_err(CliError::from)
}

fn replay_postgres_policy(
    args: PackageReplayDestinationArgs<'_>,
) -> Result<DestinationPolicy, CliError> {
    let merge_dedup = match args.merge_dedup {
        Some("fail") => PostgresMergeDedupPolicy::Fail,
        Some(value) => {
            return Err(CliError::usage(format!(
                "unsupported Postgres replay --merge-dedup `{value}`; supported value is `fail`"
            )));
        }
        None => {
            return Err(CliError::usage(
                "replay package to Postgres requires --merge-dedup fail",
            ));
        }
    };
    Ok(DestinationPolicy {
        postgres: Some(PostgresDestinationPolicy { merge_dedup }),
    })
}

fn replay_destination_resolution_error(error: CdfError, uri: &str) -> CliError {
    if error
        .message
        .contains("no project destination driver registered")
    {
        CliError::not_supported(
            "replay package",
            format!(
                "destination URI `{uri}` is unsupported for package replay; supported destinations are duckdb://path, parquet://root, and postgres://..."
            ),
            "registered project destination driver",
        )
    } else if error.message.contains("malformed or non-local")
        || error.message.contains("is missing a scheme")
    {
        CliError::not_supported(
            "replay package",
            error.message,
            "registered project destination driver",
        )
    } else {
        error.into()
    }
}

pub(crate) fn replay_package(
    cli: &Cli,
    args: ReplayPackageArgs,
) -> Result<CommandOutput, CliError> {
    let package = load_package_replay_context(cli, &args.package_dir)?;
    let mut replay_destination = build_replay_destination(
        &package.project,
        PackageReplayDestinationArgs {
            destination_uri: args.destination_uri.as_deref(),
            target: args.target.as_deref(),
            merge_dedup: args.merge_dedup.as_deref(),
        },
        &package.inputs,
    )?;
    let package_hash = package.inputs.state_delta.package_hash.clone();
    let state_store_path = package.project.state_store_path()?;
    ensure_parent_directory(&state_store_path)?;
    let run_ledger = SqliteRunLedger::open(&state_store_path)?;
    let run = run_ledger.create_run(None)?;
    let store = package.project.state_store()?;

    let replay_report =
        replay_destination.replay(args.package_dir.clone(), &store, &run_ledger, &run.run_id)?;
    let report = replay_report_ref(&replay_report);

    let receipt_source = report.receipt_source.clone();
    let mut event = RunEventAppend::new(RunEventKind::ReplayRecorded);
    event.resource_id = Some(package.inputs.state_delta.resource_id.clone());
    event.scope = Some(package.inputs.state_delta.scope.clone());
    event.package_id = Some(package.package_id.clone());
    event.package_hash = Some(package_hash.clone());
    event.package_path = Some(args.package_dir.display().to_string());
    event.checkpoint_id = Some(report.checkpoint.delta.checkpoint_id.clone());
    event.receipt_id = Some(report.receipt.receipt_id.clone());
    event.destination_id = Some(report.receipt.destination.clone());
    event.details = replay_event_details(
        &receipt_source,
        replay_destination.kind(),
        report.package_status.as_str(),
    );
    run_ledger.append_event(&run.run_id, event)?;
    let ledger_snapshot = run_ledger
        .snapshot(&run.run_id)?
        .ok_or_else(|| CdfError::internal("created replay run is absent from run ledger"))?;

    let destination_report = replay_destination
        .report
        .with_receipt_destination(report.receipt.destination.to_string());
    let cli_report = ReplayPackageCliReport::from_report(
        run.run_id.to_string(),
        package.package_id,
        args.package_dir,
        report,
        receipt_source,
        destination_report,
        &ledger_snapshot,
    );
    output("replay package", cli_report.human_message(), cli_report)
}
