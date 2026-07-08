use std::{
    env,
    path::{Path, PathBuf},
    process::Command,
};

use cdf_kernel::{CdfError, Receipt, Result, TargetName};
use cdf_project::{
    PackageArtifactReplayRequest, RuntimeStage, replay_package_from_artifacts_with_stage_hook,
};
use cdf_state_sqlite::SqliteCheckpointStore;

use super::{ChaosCrashWindow, destinations::ChaosDestinationHandle, fixture::ChaosPackageFixture};

const HELPER_ENV: &str = "CDF_CONFORMANCE_RUNTIME_CHAOS_HELPER";
const HELPER_WINDOW_ENV: &str = "CDF_CONFORMANCE_RUNTIME_CHAOS_WINDOW";
const HELPER_PACKAGE_DIR_ENV: &str = "CDF_CONFORMANCE_RUNTIME_CHAOS_PACKAGE_DIR";
const HELPER_SQLITE_PATH_ENV: &str = "CDF_CONFORMANCE_RUNTIME_CHAOS_SQLITE_PATH";
const HELPER_DESTINATION_ENV: &str = "CDF_CONFORMANCE_RUNTIME_CHAOS_DESTINATION";
const HELPER_TARGET_ENV: &str = "CDF_CONFORMANCE_RUNTIME_CHAOS_TARGET";
const HELPER_DUCKDB_PATH_ENV: &str = "CDF_CONFORMANCE_RUNTIME_CHAOS_DUCKDB_PATH";
const HELPER_PARQUET_ROOT_ENV: &str = "CDF_CONFORMANCE_RUNTIME_CHAOS_PARQUET_ROOT";
const HELPER_POSTGRES_URL_ENV: &str = "CDF_CONFORMANCE_RUNTIME_CHAOS_POSTGRES_URL";
const HELPER_POSTGRES_SCHEMA_ENV: &str = "CDF_CONFORMANCE_RUNTIME_CHAOS_POSTGRES_SCHEMA";
const HELPER_POSTGRES_TABLE_ENV: &str = "CDF_CONFORMANCE_RUNTIME_CHAOS_POSTGRES_TABLE";
const HELPER_TEST_NAME: &str = "runtime_chaos::helper::generic_stage_chaos_helper_process";
const HELPER_EXIT_CODE: i32 = 87;

pub(crate) fn spawn_stage_helper_crash(
    fixture: &ChaosPackageFixture,
    destination: &ChaosDestinationHandle,
    sqlite_path: &Path,
    window: ChaosCrashWindow,
) {
    let helper_exe = env::current_exe().unwrap(); // nosemgrep: rust.lang.security.current-exe.current-exe
    let mut command = Command::new(helper_exe);
    command
        .arg("--exact")
        .arg(HELPER_TEST_NAME)
        .arg("--nocapture")
        .env(HELPER_ENV, "1")
        .env(HELPER_WINDOW_ENV, window.as_str())
        .env(HELPER_PACKAGE_DIR_ENV, &fixture.package_dir)
        .env(HELPER_SQLITE_PATH_ENV, sqlite_path);
    apply_destination_env(&mut command, destination);
    let output = command.output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(HELPER_EXIT_CODE),
        "runtime chaos helper did not exit at {window:?}\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn generic_stage_chaos_helper_process() {
    if env::var_os(HELPER_ENV).is_none() {
        return;
    }
    run_helper_process().unwrap();
    panic!("runtime chaos helper should exit at selected RuntimeStage");
}

fn run_helper_process() -> Result<()> {
    let package_dir = PathBuf::from(env::var(HELPER_PACKAGE_DIR_ENV).unwrap());
    let sqlite_path = PathBuf::from(env::var(HELPER_SQLITE_PATH_ENV).unwrap());
    let destination = destination_from_env()?;
    let store = SqliteCheckpointStore::open(sqlite_path)?;
    let selected_window = parse_window(&env::var(HELPER_WINDOW_ENV).unwrap())?;
    let hook = |stage: RuntimeStage<'_>| -> Result<()> {
        if let Some((window, receipt)) = window_from_stage(stage)
            && window == selected_window
        {
            match window {
                ChaosCrashWindow::PackageReplayVerifiedBeforeDestinationWrite
                | ChaosCrashWindow::CheckpointProposedBeforeDestinationWrite => {
                    assert!(receipt.is_none());
                }
                ChaosCrashWindow::DestinationReceiptRecordedVerifiedBeforeCheckpointCommit
                | ChaosCrashWindow::CheckpointCommittedBeforePackageStatusCheckpointed => {
                    assert!(receipt.is_some());
                }
            }
            std::process::exit(HELPER_EXIT_CODE);
        }
        Ok(())
    };

    replay_package_from_artifacts_with_stage_hook(
        PackageArtifactReplayRequest {
            package_dir,
            destination: destination.resolved()?,
            checkpoint_store: &store,
            after_receipt_verified: None,
        },
        Some(&hook),
    )?;
    Ok(())
}

fn apply_destination_env(command: &mut Command, destination: &ChaosDestinationHandle) {
    match destination {
        ChaosDestinationHandle::DuckDb {
            database_path,
            target,
        } => {
            command
                .env(HELPER_DESTINATION_ENV, "duckdb")
                .env(HELPER_DUCKDB_PATH_ENV, database_path)
                .env(HELPER_TARGET_ENV, target.as_str());
        }
        ChaosDestinationHandle::Parquet { root, target } => {
            command
                .env(HELPER_DESTINATION_ENV, "parquet_filesystem")
                .env(HELPER_PARQUET_ROOT_ENV, root)
                .env(HELPER_TARGET_ENV, target.as_str());
        }
        ChaosDestinationHandle::Postgres {
            database_url,
            schema,
            table,
            ..
        } => {
            command
                .env(HELPER_DESTINATION_ENV, "postgres")
                .env(HELPER_POSTGRES_URL_ENV, database_url)
                .env(HELPER_POSTGRES_SCHEMA_ENV, schema)
                .env(HELPER_POSTGRES_TABLE_ENV, table);
        }
    }
}

fn destination_from_env() -> Result<ChaosDestinationHandle> {
    match env::var(HELPER_DESTINATION_ENV).unwrap().as_str() {
        "duckdb" => Ok(ChaosDestinationHandle::duckdb(
            PathBuf::from(env::var(HELPER_DUCKDB_PATH_ENV).unwrap()),
            TargetName::new(env::var(HELPER_TARGET_ENV).unwrap())?,
        )),
        "parquet_filesystem" => Ok(ChaosDestinationHandle::parquet(
            PathBuf::from(env::var(HELPER_PARQUET_ROOT_ENV).unwrap()),
            TargetName::new(env::var(HELPER_TARGET_ENV).unwrap())?,
        )),
        "postgres" => ChaosDestinationHandle::postgres(
            env::var(HELPER_POSTGRES_URL_ENV).unwrap(),
            env::var(HELPER_POSTGRES_SCHEMA_ENV).unwrap(),
            env::var(HELPER_POSTGRES_TABLE_ENV).unwrap(),
        ),
        other => Err(CdfError::contract(format!(
            "unknown runtime chaos helper destination {other}"
        ))),
    }
}

fn parse_window(value: &str) -> Result<ChaosCrashWindow> {
    for window in [
        ChaosCrashWindow::PackageReplayVerifiedBeforeDestinationWrite,
        ChaosCrashWindow::CheckpointProposedBeforeDestinationWrite,
        ChaosCrashWindow::DestinationReceiptRecordedVerifiedBeforeCheckpointCommit,
        ChaosCrashWindow::CheckpointCommittedBeforePackageStatusCheckpointed,
    ] {
        if value == window.as_str() {
            return Ok(window);
        }
    }
    Err(CdfError::contract(format!(
        "unknown runtime chaos window {value}"
    )))
}

fn window_from_stage(stage: RuntimeStage<'_>) -> Option<(ChaosCrashWindow, Option<&Receipt>)> {
    match stage {
        RuntimeStage::PackageReplayVerified => Some((
            ChaosCrashWindow::PackageReplayVerifiedBeforeDestinationWrite,
            None,
        )),
        RuntimeStage::DestinationWriteReady => Some((
            ChaosCrashWindow::CheckpointProposedBeforeDestinationWrite,
            None,
        )),
        RuntimeStage::DestinationReceiptRecorded { receipt } => Some((
            ChaosCrashWindow::DestinationReceiptRecordedVerifiedBeforeCheckpointCommit,
            Some(receipt),
        )),
        RuntimeStage::CheckpointCommitted { checkpoint } => Some((
            ChaosCrashWindow::CheckpointCommittedBeforePackageStatusCheckpointed,
            checkpoint.receipt.as_ref(),
        )),
        RuntimeStage::CheckpointProposed { .. }
        | RuntimeStage::DestinationCommitStarted { .. }
        | RuntimeStage::PackageStatusUpdated { .. } => None,
    }
}
