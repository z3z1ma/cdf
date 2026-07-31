use std::fs;

use cdf_kernel::{CheckpointStatus, CheckpointStore, Result};
use cdf_package_contract::PackageStatus;
use cdf_state_sqlite::SqliteCheckpointStore;

use super::{
    ChaosCrashWindow, ChaosDestination, RuntimeChaosOutput, cross_destination_chaos_cases,
    destinations::{
        ConformanceEnvironment, DestinationFootprint, DestinationPayload, destination_for_case,
    },
    fixture::{
        ChaosPackageFixture, ExecutedCaseParts, assert_checkpoint_not_ahead_of_durable_data,
        assert_duplicate_retry_no_second_write, durable_receipt, executed_case, package_status,
        recover_after_crash,
    },
    helper::spawn_stage_helper_crash,
};

const RUNTIME_CHAOS_DESTINATION_ENV: &str = "CDF_RUNTIME_CHAOS_DESTINATION";
const RUNTIME_CHAOS_SHARDS_JSON: &str = include_str!("../../runtime-chaos-shards.json");

#[test]
fn registered_runtime_chaos_shards_cover_destination_catalog() {
    let shards = declared_shards();
    let catalog = crate::destination_catalog::conformance_destinations()
        .into_iter()
        .map(|destination| ChaosDestination::new(destination.as_str()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        shards, catalog,
        "runtime-chaos shards must cover the destination catalog exactly"
    );
    assert_eq!(
        shards
            .iter()
            .map(|destination| {
                cross_destination_chaos_cases()
                    .into_iter()
                    .filter(|(candidate, _)| candidate == destination)
                    .count()
            })
            .sum::<usize>(),
        cross_destination_chaos_cases().len(),
        "sharded and aggregate runtime-chaos coverage must contain the same cases"
    );
}

#[test]
#[ignore = "scheduled destination shard; set CDF_RUNTIME_CHAOS_DESTINATION and run this test explicitly"]
fn registered_destination_shard_runtime_stage_chaos_persists_output() {
    let destination = ChaosDestination::new(
        std::env::var(RUNTIME_CHAOS_DESTINATION_ENV).unwrap_or_else(|_| {
            panic!("{RUNTIME_CHAOS_DESTINATION_ENV} must name a declared shard")
        }),
    )
    .expect("runtime-chaos destination shard must be valid");
    assert!(
        declared_shards().contains(&destination),
        "runtime-chaos destination shard `{}` is not declared in runtime-chaos-shards.json",
        destination.as_str()
    );

    let environment = ConformanceEnvironment::start().expect(
        "C3 runtime chaos requires Postgres coverage; set TEST_DATABASE_URL or install initdb/pg_ctl",
    );
    let mut output = RuntimeChaosOutput::default();

    for (destination, window) in cross_destination_chaos_cases()
        .into_iter()
        .filter(|(candidate, _)| candidate == &destination)
    {
        environment.reset_postgres_schema().unwrap();
        let case_id = format!("{}/{}", destination.as_str(), window.as_str());
        println!("CDF_RUNTIME_CHAOS_CASE_START={case_id}");
        output
            .executed_cases
            .push(execute_case(destination, window, &environment).unwrap());
        println!("CDF_RUNTIME_CHAOS_CASE_PASS={case_id}");
    }

    assert_eq!(output.executed_cases.len(), 4);
    assert!(
        output
            .executed_cases
            .iter()
            .all(|case| case.destination == destination)
    );
    for window in [
        ChaosCrashWindow::PackageReplayVerifiedBeforeDestinationWrite,
        ChaosCrashWindow::CheckpointProposedBeforeDestinationWrite,
        ChaosCrashWindow::DestinationReceiptRecordedVerifiedBeforeCheckpointCommit,
        ChaosCrashWindow::CheckpointCommittedBeforePackageStatusCheckpointed,
    ] {
        assert_eq!(
            output
                .executed_cases
                .iter()
                .filter(|case| case.crash_window == window)
                .count(),
            1
        );
    }

    let serialized = serde_json::to_string(&output).unwrap();
    environment.assert_redacted(&serialized);
    println!("CDF_RUNTIME_CHAOS_OUTPUT={serialized}");
}

fn declared_shards() -> Vec<ChaosDestination> {
    serde_json::from_str::<Vec<String>>(RUNTIME_CHAOS_SHARDS_JSON)
        .expect("runtime-chaos-shards.json must be a string array")
        .into_iter()
        .map(|destination| {
            ChaosDestination::new(destination)
                .expect("declared runtime-chaos destination must be valid")
        })
        .collect()
}

fn execute_case(
    destination_kind: ChaosDestination,
    window: ChaosCrashWindow,
    environment: &ConformanceEnvironment,
) -> Result<super::ExecutedChaosCase> {
    let temp = tempfile::tempdir()
        .map_err(|error| crate::conformance_host_error("create chaos tempdir", error))?;
    fs::create_dir_all(temp.path().join(".cdf")).map_err(|error| {
        crate::conformance_private_io_error("create runtime chaos .cdf dir", error)
    })?;
    let sqlite_path = temp.path().join(".cdf/runtime-chaos-state.sqlite");
    let store = SqliteCheckpointStore::open(&sqlite_path)?;
    let destination = destination_for_case(&destination_kind, window, temp.path(), environment)?;
    let fixture = ChaosPackageFixture::build(
        temp.path(),
        destination_kind.clone(),
        window,
        destination.target_name(),
    )?;

    let initial_footprint = destination.footprint()?;
    assert!(
        !initial_footprint.has_destination_write(),
        "runtime chaos destination must start empty"
    );

    spawn_stage_helper_crash(&fixture, &destination, &sqlite_path, window);

    let receipt = durable_receipt(&fixture.package_dir)?;
    let crash_footprint = destination.footprint()?;
    assert_crash_state(&store, &fixture, window, receipt.as_ref(), &crash_footprint)?;
    let crash_left_checkpoint_head = checkpoint_head_exists(&store, &fixture)?;
    let crash_left_durable_receipt = receipt.is_some();
    let crash_left_destination_write = crash_footprint.has_destination_write();

    let before_recovery_footprint = destination.footprint()?;
    let (report, recovery_path) =
        recover_after_crash(&destination, &store, &fixture, window, receipt.clone())?;
    let after_recovery_footprint = destination.footprint()?;
    let receipt_recovery_avoided_second_destination_write = if receipt.is_some() {
        before_recovery_footprint == after_recovery_footprint
    } else {
        assert_ne!(
            before_recovery_footprint, after_recovery_footprint,
            "pre-write crash recovery must materialize the destination"
        );
        true
    };
    assert!(
        receipt_recovery_avoided_second_destination_write,
        "durable-receipt recovery must not mutate destination footprint"
    );
    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(
        package_status(&fixture.package_dir)?,
        PackageStatus::Checkpointed
    );
    assert_eq!(
        destination.payload_snapshot()?,
        DestinationPayload::prepared_orders(),
        "crash recovery must reproduce the prepared package payload exactly"
    );
    assert_checkpoint_not_ahead_of_durable_data(&destination, &report)?;

    let (duplicate_retry_no_second_destination_write, duplicate_retry_behavior) =
        assert_duplicate_retry_no_second_write(&destination, &fixture, &report, temp.path())?;

    Ok(executed_case(ExecutedCaseParts {
        destination: destination_kind,
        window,
        fixture: &fixture,
        report: &report,
        recovery_path,
        crash_left_durable_receipt,
        crash_left_checkpoint_head,
        crash_left_destination_write,
        receipt_recovery_avoided_second_destination_write,
        duplicate_retry_no_second_destination_write,
        duplicate_retry_behavior,
    }))
}

fn assert_crash_state(
    store: &SqliteCheckpointStore,
    fixture: &ChaosPackageFixture,
    window: ChaosCrashWindow,
    receipt: Option<&cdf_kernel::Receipt>,
    footprint: &DestinationFootprint,
) -> Result<()> {
    let history = store.history(
        &fixture.inputs.state_delta.pipeline_id,
        &fixture.inputs.state_delta.resource_id,
        &fixture.inputs.state_delta.scope,
    )?;
    match window {
        ChaosCrashWindow::PackageReplayVerifiedBeforeDestinationWrite => {
            assert!(history.is_empty());
            assert!(receipt.is_none());
            assert!(!footprint.has_destination_write());
            assert_eq!(
                package_status(&fixture.package_dir)?,
                PackageStatus::Packaged
            );
        }
        ChaosCrashWindow::CheckpointProposedBeforeDestinationWrite => {
            assert_eq!(history.len(), 1);
            assert_eq!(history[0].status, CheckpointStatus::Proposed);
            assert!(!history[0].is_head);
            assert!(receipt.is_none());
            assert!(!footprint.has_destination_write());
            assert_eq!(
                package_status(&fixture.package_dir)?,
                PackageStatus::Loading
            );
        }
        ChaosCrashWindow::DestinationReceiptRecordedVerifiedBeforeCheckpointCommit => {
            assert_eq!(history.len(), 1);
            assert_eq!(history[0].status, CheckpointStatus::Proposed);
            assert!(!history[0].is_head);
            assert!(receipt.is_some());
            assert!(footprint.has_destination_write());
            assert_eq!(
                package_status(&fixture.package_dir)?,
                PackageStatus::Loading
            );
        }
        ChaosCrashWindow::CheckpointCommittedBeforePackageStatusCheckpointed => {
            assert!(receipt.is_some());
            assert!(footprint.has_destination_write());
            assert_eq!(
                package_status(&fixture.package_dir)?,
                PackageStatus::Loading
            );
            let head = store
                .head(
                    &fixture.inputs.state_delta.pipeline_id,
                    &fixture.inputs.state_delta.resource_id,
                    &fixture.inputs.state_delta.scope,
                )?
                .expect("checkpoint head after committed-window crash");
            assert_eq!(head.status, CheckpointStatus::Committed);
            assert!(head.is_head);
            assert_eq!(head.delta, fixture.inputs.state_delta);
            assert_eq!(head.receipt.as_ref(), receipt);
        }
    }
    Ok(())
}

fn checkpoint_head_exists(
    store: &SqliteCheckpointStore,
    fixture: &ChaosPackageFixture,
) -> Result<bool> {
    Ok(store
        .head(
            &fixture.inputs.state_delta.pipeline_id,
            &fixture.inputs.state_delta.resource_id,
            &fixture.inputs.state_delta.scope,
        )?
        .is_some())
}
