use std::{
    fs,
    panic::{AssertUnwindSafe, catch_unwind},
};

use firn_kernel::{CheckpointStatus, CheckpointStore, ReceiptId, ResourceId, ScopeKey};
use firn_project::{PreparedReceiptSource, recover_prepared_duckdb_package};

use super::*;
use crate::package_replay::{
    assert_duplicate_replay_identity, assert_no_checkpoint_head,
    assert_no_second_destination_write, assert_recovery_committed_from_durable_receipt,
    replay_prepared_package_case,
};

#[test]
fn live_local_file_v1_matches_committed_golden_across_100_runs() {
    let expected = live_local_file_v1_expected_evidence().unwrap();

    for run in 0..100 {
        let temp = tempfile::tempdir().unwrap();
        let spec = LiveLocalFileFixtureSpec::live_local_file_v1(temp.path()).unwrap();
        let fixture = futures_executor::block_on(run_live_local_file_fixture(spec)).unwrap();

        assert_live_run_matches_expected(&fixture, &expected);
        assert_eq!(
            fixture.report.receipt.receipt_id.as_str(),
            format!(
                "duckdb:{}:{}",
                LIVE_LOCAL_FILE_V1_TARGET, expected.package.package_hash
            )
        );
        assert_eq!(
            live_run_expected_from_fixture(&fixture).package,
            expected.package,
            "run {run} produced different package evidence"
        );
    }
}

#[test]
fn committed_before_checkpoint_recovers_without_source_file() {
    let temp = tempfile::tempdir().unwrap();
    let spec = LiveLocalFileFixtureSpec::live_local_file_v1(temp.path()).unwrap();
    let package_dir = spec.package_root.join(&spec.package_id);
    let hook = |_receipt: &Receipt| Err(FirnError::internal("injected live conformance failure"));

    let error = futures_executor::block_on(run_live_local_file_fixture_with_hook(
        spec.clone(),
        Some(&hook),
    ))
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("injected live conformance failure")
    );

    let receipt = read_single_live_receipt(&package_dir);
    let destination = DuckDbDestination::new(&spec.destination_path).unwrap();
    assert!(destination.verify_receipt(&receipt).unwrap().verified);
    let snapshot_before = destination.read_mirror_snapshot_read_only().unwrap();
    let store = SqliteCheckpointStore::open(&spec.state_store_path).unwrap();
    assert_single_file_scope_mirror(&snapshot_before);
    let history = store
        .history(
            &spec.pipeline_id,
            &ResourceId::new(LIVE_LOCAL_FILE_V1_RESOURCE_ID).unwrap(),
            &ScopeKey::File {
                path: "events.ndjson".to_owned(),
            },
        )
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].status, CheckpointStatus::Proposed);
    assert_no_checkpoint_head(&store, &history[0].delta);

    fs::remove_file(temp.path().join(LIVE_LOCAL_FILE_V1_SOURCE_PATH)).unwrap();
    let case = live_replay_case(&package_dir, history[0].delta.clone(), spec.target.clone());
    let report = recover_prepared_duckdb_package(case.recovery_request(
        &destination,
        &store,
        receipt.clone(),
        None,
    ))
    .unwrap();
    let snapshot_after = destination.read_mirror_snapshot_read_only().unwrap();

    assert_recovery_committed_from_durable_receipt(
        &case,
        &store,
        &report,
        &receipt,
        &snapshot_before,
        &snapshot_after,
    );
    assert_eq!(
        report.receipt_source,
        PreparedReceiptSource::SuppliedDurableReceipt
    );
    assert_eq!(
        PackageReader::open(&package_dir)
            .unwrap()
            .manifest()
            .lifecycle
            .status,
        PackageStatus::Checkpointed
    );
}

#[test]
fn duplicate_live_package_replay_is_noop_for_destination_and_mirrors() {
    let temp = tempfile::tempdir().unwrap();
    let spec = LiveLocalFileFixtureSpec::live_local_file_v1(temp.path()).unwrap();
    let fixture = futures_executor::block_on(run_live_local_file_fixture(spec.clone())).unwrap();
    let destination = DuckDbDestination::new(&spec.destination_path).unwrap();
    let snapshot_before = destination.read_mirror_snapshot_read_only().unwrap();
    let case = live_replay_case(
        &fixture.report.package_dir,
        fixture.report.checkpoint.delta.clone(),
        spec.target.clone(),
    );
    let duplicate_store =
        SqliteCheckpointStore::open(temp.path().join(".firn/duplicate-state.sqlite")).unwrap();

    let duplicate_report =
        replay_prepared_package_case(&case, &destination, &duplicate_store).unwrap();
    let snapshot_after = destination.read_mirror_snapshot_read_only().unwrap();

    assert_duplicate_replay_identity(
        &case,
        &duplicate_report,
        &fixture.report.receipt,
        &snapshot_after,
    );
    assert_no_second_destination_write(&snapshot_before, &snapshot_after);
    assert_eq!(
        snapshot_after
            .state
            .iter()
            .map(|row| row.row_count)
            .sum::<u64>(),
        LIVE_LOCAL_FILE_V1_ROW_COUNT
    );
}

#[test]
fn negative_self_tests_catch_live_run_harness_gaps() {
    let temp = tempfile::tempdir().unwrap();
    let spec = LiveLocalFileFixtureSpec::live_local_file_v1(temp.path()).unwrap();
    let fixture = futures_executor::block_on(run_live_local_file_fixture(spec.clone())).unwrap();
    let expected = live_run_expected_from_fixture(&fixture);

    let mut corrupted_package = expected.clone();
    corrupted_package.package.package_hash = "sha256:wrong-live-package".to_owned();
    assert_harness_panics(|| assert_live_run_matches_expected(&fixture, &corrupted_package));

    let mut wrong_destination_rows = expected.clone();
    wrong_destination_rows.destination_rows += 1;
    assert_harness_panics(|| assert_live_run_matches_expected(&fixture, &wrong_destination_rows));

    let mut wrong_source_path = expected.clone();
    wrong_source_path.source_path_suffix = "data/other.ndjson".to_owned();
    assert_harness_panics(|| assert_live_run_matches_expected(&fixture, &wrong_source_path));

    let mut wrong_source_size = expected.clone();
    wrong_source_size.source_size_bytes += 1;
    assert_harness_panics(|| assert_live_run_matches_expected(&fixture, &wrong_source_size));

    let mut wrong_source_hash = expected.clone();
    wrong_source_hash.source_sha256 = "bad-source-hash".to_owned();
    assert_harness_panics(|| assert_live_run_matches_expected(&fixture, &wrong_source_hash));

    let proposed_store =
        SqliteCheckpointStore::open(temp.path().join(".firn/proposed-only.sqlite")).unwrap();
    proposed_store
        .propose(fixture.report.checkpoint.delta.clone())
        .unwrap();
    assert_harness_panics(|| {
        assert_checkpoint_head_matches(&proposed_store, &fixture.report.checkpoint.delta);
    });

    let mut missing_receipt = fixture.report.receipt.clone();
    missing_receipt.receipt_id = ReceiptId::new("receipt-missing-live").unwrap();
    assert_harness_panics(|| {
        assert_package_receipt_durable(&fixture.report.package_dir, &missing_receipt);
    });
}

fn assert_single_file_scope_mirror(snapshot: &crate::package_replay::DuckDbMirrorSnapshot) {
    assert_eq!(snapshot.state.len(), 1);
    let scope: ScopeKey = serde_json::from_str(
        snapshot.state[0]
            .scope_json
            .as_deref()
            .expect("scope mirror"),
    )
    .unwrap();
    assert_eq!(
        scope,
        ScopeKey::File {
            path: "events.ndjson".to_owned()
        }
    );
}

fn assert_harness_panics(f: impl FnOnce()) {
    assert!(
        catch_unwind(AssertUnwindSafe(f)).is_err(),
        "corrupted live conformance case passed the harness"
    );
}
