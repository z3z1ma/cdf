use std::path::{Path, PathBuf};

use cdf_kernel::{CdfError, RunId};
use cdf_package::{PackageReader, PackageReplayInputs};
use cdf_project::{
    PackageArtifactDuckDbReplayRequest, PackageArtifactParquetReplayRequest,
    PackageArtifactPostgresReplayRequest, ProjectReceiptSource,
    replay_duckdb_package_from_artifacts, replay_parquet_package_from_artifacts,
    replay_postgres_package_from_artifacts,
};
use cdf_state_sqlite::{RunEventAppend, RunEventKind, SqliteCheckpointStore, SqliteRunLedger};

use crate::{
    args::{Cli, ReplayPackageArgs},
    commands::output,
    context::ProjectContext,
    destination_uri::{parquet_filesystem_root, postgres_database_url, redact_error_value},
    output::{CliError, CommandOutput},
    reports::{
        PreparedReplayReportRef, ReplayPackageCliReport, RunDestinationReport, replay_event_details,
    },
    run_command::ensure_parent_directory,
};

fn replay_duckdb_destination_path(
    context: &ProjectContext,
    uri: &str,
) -> Result<PathBuf, CliError> {
    let Some(raw_path) = uri.strip_prefix("duckdb://") else {
        return Err(CliError::not_supported(
            "replay package",
            format!(
                "destination URI `{uri}` is unsupported for package replay; supported destinations are duckdb://path, parquet://root, and postgres://..."
            ),
            "artifact replay destination",
        ));
    };
    if raw_path.trim().is_empty() || raw_path.contains("://") {
        return Err(CliError::not_supported(
            "replay package",
            format!("destination URI `{uri}` is malformed or non-local; expected duckdb://path"),
            "local DuckDB destination path",
        ));
    }
    let path = PathBuf::from(raw_path);
    Ok(if path.is_absolute() {
        path
    } else {
        context.root.join(path)
    })
}

enum ReplayDestination {
    DuckDb {
        path: PathBuf,
    },
    Parquet {
        root: PathBuf,
    },
    Postgres {
        destination: Box<cdf_dest_postgres::PostgresDestination>,
        target: cdf_dest_postgres::PostgresTarget,
        dedup: cdf_dest_postgres::MergeDedupPolicy,
        secret_backed: bool,
    },
}

impl ReplayDestination {
    fn replay(
        &self,
        package_dir: &Path,
        store: &SqliteCheckpointStore,
        run_ledger: &SqliteRunLedger,
        run_id: &RunId,
    ) -> Result<PreparedReplayReport, CliError> {
        let result = match self {
            Self::DuckDb { path } => {
                ensure_parent_directory(path)?;
                let destination = cdf_dest_duckdb::DuckDbDestination::new(path)?;
                replay_duckdb_package_from_artifacts(PackageArtifactDuckDbReplayRequest {
                    package_dir: package_dir.to_path_buf(),
                    destination: &destination,
                    checkpoint_store: store,
                    after_receipt_verified: None,
                })
                .map(PreparedReplayReport::DuckDb)
            }
            Self::Parquet { root } => {
                let destination = cdf_dest_parquet::ParquetDestination::new_filesystem(root)?;
                replay_parquet_package_from_artifacts(PackageArtifactParquetReplayRequest {
                    package_dir: package_dir.to_path_buf(),
                    destination: &destination,
                    checkpoint_store: store,
                    after_receipt_verified: None,
                })
                .map(PreparedReplayReport::Parquet)
            }
            Self::Postgres {
                destination,
                target,
                dedup,
                secret_backed,
            } => replay_postgres_package_from_artifacts(PackageArtifactPostgresReplayRequest {
                package_dir: package_dir.to_path_buf(),
                destination,
                checkpoint_store: store,
                target: target.clone(),
                dedup: dedup.clone(),
                existing_table: None,
                after_receipt_verified: None,
            })
            .map(PreparedReplayReport::Postgres)
            .map_err(|error| redact_postgres_replay_error(error, destination, *secret_backed)),
        };
        match result {
            Ok(report) => Ok(report),
            Err(error) => {
                let _ =
                    run_ledger.append_event(run_id, RunEventAppend::new(RunEventKind::RunFailed));
                Err(error.into())
            }
        }
    }

    fn report(&self, target: String) -> RunDestinationReport {
        match self {
            Self::DuckDb { path } => {
                RunDestinationReport::duckdb(path.display().to_string(), target)
            }
            Self::Parquet { root } => {
                RunDestinationReport::parquet(root.display().to_string(), target)
            }
            Self::Postgres { .. } => RunDestinationReport::postgres(target),
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::DuckDb { .. } => "duckdb",
            Self::Parquet { .. } => "parquet",
            Self::Postgres { .. } => "postgres",
        }
    }

    fn validate_package_inputs(&self, inputs: &PackageReplayInputs) -> Result<(), CliError> {
        if let Self::Postgres { target, .. } = self {
            let explicit = target.display_name();
            if explicit != inputs.destination_commit.target.as_str() {
                return Err(CliError::from(CdfError::contract(format!(
                    "explicit Postgres replay target {explicit} does not match package destination commit target {}",
                    inputs.destination_commit.target
                ))));
            }
        }
        Ok(())
    }
}

enum PreparedReplayReport {
    DuckDb(cdf_project::PreparedDuckDbReplayReport),
    Parquet(cdf_project::PreparedParquetReplayReport),
    Postgres(cdf_project::PreparedPostgresReplayReport),
}

impl PreparedReplayReport {
    fn report(&self) -> PreparedReplayReportRef<'_> {
        match self {
            Self::DuckDb(report) => PreparedReplayReportRef {
                checkpoint: &report.checkpoint,
                receipt: &report.receipt,
                receipt_source: ProjectReceiptSource::from(report.receipt_source.clone()),
                package_status: &report.package_status,
            },
            Self::Parquet(report) => PreparedReplayReportRef {
                checkpoint: &report.checkpoint,
                receipt: &report.receipt,
                receipt_source: report.receipt_source.clone(),
                package_status: &report.package_status,
            },
            Self::Postgres(report) => PreparedReplayReportRef {
                checkpoint: &report.checkpoint,
                receipt: &report.receipt,
                receipt_source: report.receipt_source.clone(),
                package_status: &report.package_status,
            },
        }
    }
}

fn build_replay_destination(
    context: &ProjectContext,
    args: &ReplayPackageArgs,
) -> Result<ReplayDestination, CliError> {
    let uri = &args.destination_uri;
    if uri.starts_with("postgres://") {
        return build_postgres_replay_destination(context, args);
    }
    if uri.starts_with("parquet://") {
        return Ok(ReplayDestination::Parquet {
            root: parquet_filesystem_root(context, uri, "replay package")?,
        });
    }
    Ok(ReplayDestination::DuckDb {
        path: replay_duckdb_destination_path(context, uri)?,
    })
}

fn build_postgres_replay_destination(
    context: &ProjectContext,
    args: &ReplayPackageArgs,
) -> Result<ReplayDestination, CliError> {
    let target = args.target.as_deref().ok_or_else(|| {
        CliError::usage("replay package to Postgres requires --target schema.table")
    })?;
    if target.split('.').count() != 2 {
        return Err(CliError::usage(
            "replay package to Postgres requires --target schema.table",
        ));
    }
    let target = cdf_dest_postgres::PostgresTarget::parse(target)?;
    let dedup = match args.merge_dedup.as_deref() {
        Some("fail") => cdf_dest_postgres::MergeDedupPolicy::Fail,
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
    let (destination, secret_backed) = postgres_replay_destination(context, &args.destination_uri)?;
    Ok(ReplayDestination::Postgres {
        destination: Box::new(destination),
        target,
        dedup,
        secret_backed,
    })
}

fn postgres_replay_destination(
    context: &ProjectContext,
    uri: &str,
) -> Result<(cdf_dest_postgres::PostgresDestination, bool), CliError> {
    let (database_url, secret_backed) = postgres_database_url(context, uri, "replay package")?;
    Ok((
        cdf_dest_postgres::PostgresDestination::connect(database_url)?,
        secret_backed,
    ))
}

fn redact_postgres_replay_error(
    error: CdfError,
    destination: &cdf_dest_postgres::PostgresDestination,
    secret_backed: bool,
) -> CdfError {
    let secret = secret_backed.then(|| destination.database_url()).flatten();
    redact_error_value(error, secret)
}

pub(crate) fn replay_package(
    cli: &Cli,
    args: ReplayPackageArgs,
) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let replay_destination = build_replay_destination(&context, &args)?;
    let reader = PackageReader::open(&args.package_dir)?;
    let package_id = reader.manifest().identity.package_id.clone();
    let replay_inputs = reader.replay_inputs()?;
    replay_destination.validate_package_inputs(&replay_inputs)?;
    let package_hash = replay_inputs.state_delta.package_hash.clone();
    let state_store_path = context.state_store_path()?;
    ensure_parent_directory(&state_store_path)?;
    let run_ledger = SqliteRunLedger::open(&state_store_path)?;
    let run = run_ledger.create_run(None)?;
    let store = context.state_store()?;

    let replay_report =
        replay_destination.replay(&args.package_dir, &store, &run_ledger, &run.run_id)?;
    let report = replay_report.report();

    let receipt_source = report.receipt_source.clone();
    let mut event = RunEventAppend::new(RunEventKind::ReplayRecorded);
    event.resource_id = Some(replay_inputs.state_delta.resource_id.clone());
    event.scope = Some(replay_inputs.state_delta.scope.clone());
    event.package_id = Some(package_id.clone());
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
        .report(report.receipt.target.to_string())
        .with_receipt_destination(report.receipt.destination.to_string());
    let cli_report = ReplayPackageCliReport::from_report(
        run.run_id.to_string(),
        package_id,
        args.package_dir,
        report,
        receipt_source,
        destination_report,
        &ledger_snapshot,
    );
    output("replay package", cli_report.human_message(), cli_report)
}
