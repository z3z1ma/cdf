use std::{
    env,
    path::{Path, PathBuf},
    process::Command,
};

use cdf_kernel::{CdfError, Receipt, Result};
use cdf_project::{
    PackageArtifactReplayRequest, RuntimeStage, replay_package_from_artifacts_with_stage_hook,
};
use cdf_state_sqlite::SqliteCheckpointStore;

use super::{ChaosCrashWindow, destinations::ChaosDestinationHandle, fixture::ChaosPackageFixture};
use crate::destination_catalog::DestinationExecutionSpec;

const HELPER_ENV: &str = "CDF_CONFORMANCE_RUNTIME_CHAOS_HELPER";
const HELPER_WINDOW_ENV: &str = "CDF_CONFORMANCE_RUNTIME_CHAOS_WINDOW";
const HELPER_PACKAGE_DIR_ENV: &str = "CDF_CONFORMANCE_RUNTIME_CHAOS_PACKAGE_DIR";
const HELPER_SQLITE_PATH_ENV: &str = "CDF_CONFORMANCE_RUNTIME_CHAOS_SQLITE_PATH";
const HELPER_DESTINATION_SPEC_ENV: &str = "CDF_CONFORMANCE_RUNTIME_CHAOS_DESTINATION_SPEC";
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
    command.env(
        HELPER_DESTINATION_SPEC_ENV,
        serde_json::to_string(&destination.execution_spec()).unwrap(),
    );
}

fn destination_from_env() -> Result<DestinationExecutionSpec> {
    serde_json::from_str(&env::var(HELPER_DESTINATION_SPEC_ENV).unwrap()).map_err(|error| {
        CdfError::contract(format!(
            "invalid runtime chaos destination execution spec: {error}"
        ))
    })
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
        | RuntimeStage::DestinationSegmentAcknowledged { .. }
        | RuntimeStage::PackageStatusUpdated { .. } => None,
    }
}
