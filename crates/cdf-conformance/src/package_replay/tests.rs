use std::{
    collections::BTreeMap,
    env, fs,
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    process::Command,
};

use cdf_kernel::{
    CheckpointId, CheckpointStatus, CheckpointStore, CommitCounts, DestinationId, IdempotencyToken,
    PackageHash, Receipt, ReceiptId, SegmentAck, TargetName, VerifyClause,
};
use cdf_project::{RuntimeStage, replay_package_from_artifacts_with_stage_hook};

use super::*;

const HELPER_ENV: &str = "CDF_CONFORMANCE_HELPER_AFTER_RECEIPT_EXIT";
const HELPER_FAILPOINT_ENV: &str = "CDF_CONFORMANCE_LIFECYCLE_FAILPOINT";
const HELPER_PACKAGE_DIR_ENV: &str = "CDF_CONFORMANCE_PACKAGE_DIR";
const HELPER_DUCKDB_PATH_ENV: &str = "CDF_CONFORMANCE_DUCKDB_PATH";
const HELPER_SQLITE_PATH_ENV: &str = "CDF_CONFORMANCE_SQLITE_PATH";
const HELPER_TEST_NAME: &str =
    "package_replay::tests::committed_before_checkpointed_helper_process";
const HELPER_EXIT_CODE: i32 = 87;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LifecycleFailpoint {
    PackagedBeforeDestinationWrite,
    CheckpointProposalBeforeDestinationWrite,
    ReceiptVerifiedBeforeCheckpointCommit,
    CheckpointCommitBeforePackageStatusCheckpointed,
}

#[test]
fn packaged_no_receipts_replay_commits_destination_receipt_checkpoint_and_status() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-success");
    let fixture = prepared_fixture(&package_dir, "pkg-success", "checkpoint-success");
    let case = fixture.replay_case().unwrap();
    let destination = DuckDbDestination::new(temp.path().join("local.duckdb")).unwrap();
    let store = SqliteCheckpointStore::open(temp.path().join("state.sqlite")).unwrap();

    assert!(
        PackageReader::open(&case.package_dir)
            .unwrap()
            .receipts()
            .unwrap()
            .is_empty(),
        "fixture must start at the packaged/no-receipts boundary"
    );

    let report = replay_package_case(&case, &destination, &store).unwrap();

    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::DestinationCommit {
            duplicate: false,
            package_receipt_recorded: true
        }
    );
    assert_packaged_replay_committed_without_source_contact(&case, &destination, &store, &report);
}

#[test]
fn package_artifacts_replay_commits_destination_receipt_checkpoint_and_status() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-artifact-success");
    let fixture = prepared_fixture(
        &package_dir,
        "pkg-artifact-success",
        "checkpoint-artifact-success",
    );
    let destination = DuckDbDestination::new(temp.path().join("local.duckdb")).unwrap();
    let store = SqliteCheckpointStore::open(temp.path().join("state.sqlite")).unwrap();

    let report = replay_package_artifacts(&package_dir, &destination, &store).unwrap();
    let case = fixture.replay_case().unwrap();

    assert_packaged_replay_committed_without_source_contact(&case, &destination, &store, &report);
    assert_eq!(
        report.receipt.idempotency_token.as_str(),
        report.checkpoint.delta.package_hash.as_str()
    );
}

#[test]
fn duplicate_replay_returns_noop_receipt_and_single_destination_load() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-duplicate");
    let fixture = prepared_fixture(&package_dir, "pkg-duplicate", "checkpoint-duplicate");
    let first_case = fixture.replay_case().unwrap();
    let destination = DuckDbDestination::new(temp.path().join("local.duckdb")).unwrap();
    let first_store = SqliteCheckpointStore::open(temp.path().join("state-first.sqlite")).unwrap();
    let first_report = replay_package_case(&first_case, &destination, &first_store).unwrap();

    let second_case = fixture.replay_case().unwrap();
    let second_store =
        SqliteCheckpointStore::open(temp.path().join("state-second.sqlite")).unwrap();
    let second_report = replay_package_case(&second_case, &destination, &second_store).unwrap();
    let snapshot = destination.read_mirror_snapshot_read_only().unwrap();

    assert_checkpoint_head_matches(&second_store, &second_case.delta);
    assert_duplicate_replay_identity(
        &second_case,
        &second_report,
        &first_report.receipt,
        &snapshot,
    );
    assert_eq!(
        snapshot.state.len(),
        second_case.delta.segments.len(),
        "duplicate replay must not add state mirror rows"
    );
}

#[test]
fn helper_process_after_packaged_before_destination_write_leaves_no_destination_or_checkpoint() {
    let crashed = stage_helper_crash(
        "pkg-after-packaged",
        "checkpoint-after-packaged",
        LifecycleFailpoint::PackagedBeforeDestinationWrite,
    );

    assert!(!crashed.db_path.exists());
    assert_no_duckdb_destination_write(&crashed.snapshot);
    assert!(crashed.receipt.is_none());
    assert_no_checkpoint_head(&crashed.store, &crashed.case.delta);
    assert!(
        crashed
            .store
            .history(
                &crashed.case.delta.pipeline_id,
                &crashed.case.delta.resource_id,
                &crashed.case.delta.scope,
            )
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        PackageReader::open(&crashed.case.package_dir)
            .unwrap()
            .manifest()
            .lifecycle
            .status,
        PackageStatus::Packaged
    );
}

#[test]
fn helper_process_after_checkpoint_proposal_leaves_no_destination_or_checkpoint_head() {
    let crashed = stage_helper_crash(
        "pkg-after-proposal",
        "checkpoint-after-proposal",
        LifecycleFailpoint::CheckpointProposalBeforeDestinationWrite,
    );

    assert!(!crashed.db_path.exists());
    assert_no_duckdb_destination_write(&crashed.snapshot);
    assert!(crashed.receipt.is_none());
    assert_no_checkpoint_head(&crashed.store, &crashed.case.delta);
    let history = crashed
        .store
        .history(
            &crashed.case.delta.pipeline_id,
            &crashed.case.delta.resource_id,
            &crashed.case.delta.scope,
        )
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].status, CheckpointStatus::Proposed);
    assert!(!history[0].is_head);
    assert_eq!(
        PackageReader::open(&crashed.case.package_dir)
            .unwrap()
            .manifest()
            .lifecycle
            .status,
        PackageStatus::Loading
    );
}

#[test]
fn helper_process_crash_recovers_from_durable_receipt_without_second_load() {
    let crashed = stage_helper_crash(
        "pkg-helper-recovery",
        "checkpoint-helper-recovery",
        LifecycleFailpoint::ReceiptVerifiedBeforeCheckpointCommit,
    );
    let receipt = crashed.receipt.clone().expect("durable receipt");
    let history = crashed
        .store
        .history(
            &crashed.case.delta.pipeline_id,
            &crashed.case.delta.resource_id,
            &crashed.case.delta.scope,
        )
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].status, CheckpointStatus::Proposed);
    assert!(!history[0].is_head);
    assert_no_checkpoint_head(&crashed.store, &crashed.case.delta);
    assert_eq!(
        PackageReader::open(&crashed.case.package_dir)
            .unwrap()
            .manifest()
            .lifecycle
            .status,
        PackageStatus::Loading
    );
    assert_package_receipt_durable(&crashed.case.package_dir, &receipt);
    assert_duckdb_mirror_matches_receipt(&crashed.snapshot, &crashed.case, &receipt);

    let snapshot_before = crashed
        .destination
        .read_mirror_snapshot_read_only()
        .unwrap();
    let report = recover_package_case(
        &crashed.case,
        &crashed.destination,
        &crashed.store,
        receipt.clone(),
    )
    .unwrap();
    let snapshot_after = crashed
        .destination
        .read_mirror_snapshot_read_only()
        .unwrap();

    assert_recovery_committed_from_durable_receipt(
        &crashed.case,
        &crashed.store,
        &report,
        &receipt,
        &snapshot_before,
        &snapshot_after,
    );
    assert_eq!(
        PackageReader::open(&crashed.case.package_dir)
            .unwrap()
            .manifest()
            .lifecycle
            .status,
        PackageStatus::Checkpointed
    );
}

#[test]
fn helper_process_after_checkpoint_commit_finalizes_status_without_second_load() {
    let crashed = stage_helper_crash(
        "pkg-after-checkpoint",
        "checkpoint-after-checkpoint",
        LifecycleFailpoint::CheckpointCommitBeforePackageStatusCheckpointed,
    );
    let receipt = crashed.receipt.clone().expect("durable receipt");
    assert_package_receipt_durable(&crashed.case.package_dir, &receipt);
    assert_duckdb_mirror_matches_receipt(&crashed.snapshot, &crashed.case, &receipt);
    let head = assert_checkpoint_head_matches(&crashed.store, &crashed.case.delta);
    assert_eq!(head.receipt.as_ref(), Some(&receipt));
    assert_eq!(
        PackageReader::open(&crashed.case.package_dir)
            .unwrap()
            .manifest()
            .lifecycle
            .status,
        PackageStatus::Loading
    );

    let snapshot_before = crashed
        .destination
        .read_mirror_snapshot_read_only()
        .unwrap();
    let report = recover_package_case(
        &crashed.case,
        &crashed.destination,
        &crashed.store,
        receipt.clone(),
    )
    .unwrap();
    let snapshot_after = crashed
        .destination
        .read_mirror_snapshot_read_only()
        .unwrap();

    assert_eq!(report.checkpoint, head);
    assert_recovery_committed_from_durable_receipt(
        &crashed.case,
        &crashed.store,
        &report,
        &receipt,
        &snapshot_before,
        &snapshot_after,
    );
    assert_eq!(
        PackageReader::open(&crashed.case.package_dir)
            .unwrap()
            .manifest()
            .lifecycle
            .status,
        PackageStatus::Checkpointed
    );
}

#[test]
fn bad_recovery_inputs_fail_closed_without_checkpoint_head() {
    let crashed = stage_helper_crash(
        "pkg-bad-recovery",
        "checkpoint-bad-recovery",
        LifecycleFailpoint::ReceiptVerifiedBeforeCheckpointCommit,
    );
    let receipt = crashed.receipt.clone().expect("durable receipt");

    let mut missing_ack = receipt.clone();
    missing_ack.segment_acks.clear();
    let error = recover_package_case(
        &crashed.case,
        &crashed.destination,
        &crashed.store,
        missing_ack,
    )
    .unwrap_err();
    assert!(error.to_string().contains("acknowledges 0 segment"));
    assert_no_checkpoint_head(&crashed.store, &crashed.case.delta);

    let mut failed_verification = receipt;
    failed_verification.committed_at_ms += 1;
    let error = recover_package_case(
        &crashed.case,
        &crashed.destination,
        &crashed.store,
        failed_verification,
    )
    .unwrap_err();
    assert!(error.to_string().contains("did not verify"));
    assert_no_checkpoint_head(&crashed.store, &crashed.case.delta);
}

#[test]
fn negative_self_tests_prove_package_replay_harness_checks_required_edges() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-negative");
    let fixture = prepared_fixture(&package_dir, "pkg-negative", "checkpoint-negative");
    let case = fixture.replay_case().unwrap();
    let commit_plan: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(case.package_dir.join("destination/commit_plan.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(commit_plan["target"], DEFAULT_PREPARED_TARGET);
    assert_eq!(commit_plan["disposition"], "append");
    assert_eq!(commit_plan["idempotency_token_source"], "package_hash");
    assert!(commit_plan.get("idempotency_token").is_none());
    assert!(commit_plan.get("package_hash").is_none());

    let missing_receipt = fake_receipt(&case);
    assert_harness_panics(|| assert_package_receipt_durable(&case.package_dir, &missing_receipt));

    let proposed_store = SqliteCheckpointStore::open(temp.path().join("proposed.sqlite")).unwrap();
    proposed_store.propose(case.delta.clone()).unwrap();
    assert_harness_panics(|| {
        assert_checkpoint_head_matches(&proposed_store, &case.delta);
    });

    let destination = DuckDbDestination::new(temp.path().join("local.duckdb")).unwrap();
    let store = SqliteCheckpointStore::open(temp.path().join("state.sqlite")).unwrap();
    let report = replay_package_case(&case, &destination, &store).unwrap();
    let snapshot = destination.read_mirror_snapshot_read_only().unwrap();
    let mut wrong_success = report.clone();
    wrong_success.package_status = PackageStatus::Loading;
    assert_harness_panics(|| {
        assert_packaged_replay_committed_without_source_contact(
            &case,
            &destination,
            &store,
            &wrong_success,
        );
    });
    assert_harness_panics(|| {
        assert_recovery_committed_from_durable_receipt(
            &case,
            &store,
            &report,
            &report.receipt,
            &snapshot,
            &snapshot,
        );
    });
    assert_harness_panics(|| assert_no_checkpoint_head(&store, &case.delta));

    let mut wrong_duplicate = report.clone();
    wrong_duplicate.receipt_source = ProjectReceiptSource::DestinationCommitReceiptOnly {
        package_receipt_recorded: true,
    };
    assert_harness_panics(|| {
        assert_duplicate_replay_identity(&case, &wrong_duplicate, &report.receipt, &snapshot);
    });

    let mut second_write_snapshot = snapshot.clone();
    second_write_snapshot.loads.push(snapshot.loads[0].clone());
    assert_harness_panics(|| {
        assert_no_second_destination_write(&snapshot, &second_write_snapshot);
    });
    assert_harness_panics(|| {
        assert_no_duckdb_destination_write(&snapshot);
    });

    let mut wrong_receipt = report.receipt.clone();
    wrong_receipt.target = TargetName::new("other_orders").unwrap();
    assert_harness_panics(|| assert_receipt_matches_case(&case, &wrong_receipt));

    let package_segments = PackageReader::open(&case.package_dir)
        .unwrap()
        .manifest()
        .identity
        .segments
        .clone();
    let mut wrong_segments = case.delta.segments.clone();
    wrong_segments[0].byte_count += 1;
    assert_harness_panics(|| assert_segments_match(&package_segments, &wrong_segments));

    let mut wrong_load_snapshot = snapshot.clone();
    wrong_load_snapshot.loads[0].idempotency_token = "other-token".to_owned();
    wrong_load_snapshot.loads[0].package_hash = "other-package".to_owned();
    assert_harness_panics(|| {
        assert_duckdb_mirror_matches_receipt(&wrong_load_snapshot, &case, &report.receipt);
    });

    let mut wrong_load_package_hash_snapshot = snapshot.clone();
    wrong_load_package_hash_snapshot.loads[0].package_hash = "other-package".to_owned();
    assert_eq!(
        wrong_load_package_hash_snapshot.loads[0].target,
        case.target.as_str()
    );
    assert_eq!(
        wrong_load_package_hash_snapshot.loads[0].idempotency_token,
        report.receipt.idempotency_token.as_str()
    );
    assert_harness_panics(|| {
        assert_duckdb_mirror_matches_receipt(
            &wrong_load_package_hash_snapshot,
            &case,
            &report.receipt,
        );
    });

    let mut wrong_state_snapshot = snapshot.clone();
    wrong_state_snapshot.state[0].package_hash = "other-package".to_owned();
    assert_harness_panics(|| {
        assert_duckdb_mirror_matches_receipt(&wrong_state_snapshot, &case, &report.receipt);
    });
}

#[test]
fn committed_before_checkpointed_helper_process() {
    if env::var_os(HELPER_ENV).is_none() {
        return;
    }

    let package_dir = PathBuf::from(env::var(HELPER_PACKAGE_DIR_ENV).unwrap());
    let db_path = PathBuf::from(env::var(HELPER_DUCKDB_PATH_ENV).unwrap());
    let sqlite_path = PathBuf::from(env::var(HELPER_SQLITE_PATH_ENV).unwrap());
    let inputs = PackageReader::open(&package_dir)
        .unwrap()
        .replay_inputs()
        .unwrap();
    let case = PreparedPackageReplayCase {
        package_dir,
        delta: inputs.state_delta,
        target: inputs.destination_commit.target,
        disposition: inputs.destination_commit.disposition,
        schema_hash: inputs.schema_hash,
    };
    let destination = DuckDbDestination::new(db_path).unwrap();
    let store = SqliteCheckpointStore::open(sqlite_path).unwrap();
    let selected_failpoint = parse_lifecycle_failpoint(&env::var(HELPER_FAILPOINT_ENV).unwrap());
    let hook = |stage: RuntimeStage<'_>| -> Result<()> {
        if let Some((failpoint, receipt)) = lifecycle_failpoint_from_stage(stage)
            && failpoint == selected_failpoint
        {
            match failpoint {
                LifecycleFailpoint::PackagedBeforeDestinationWrite
                | LifecycleFailpoint::CheckpointProposalBeforeDestinationWrite => {
                    assert!(receipt.is_none());
                }
                LifecycleFailpoint::ReceiptVerifiedBeforeCheckpointCommit
                | LifecycleFailpoint::CheckpointCommitBeforePackageStatusCheckpointed => {
                    assert!(receipt.is_some());
                }
            }
            std::process::exit(HELPER_EXIT_CODE);
        }
        Ok(())
    };

    let _ = replay_package_from_artifacts_with_stage_hook(
        case.replay_request(&destination, &store, None),
        Some(&hook),
    )
    .unwrap();
    panic!("helper hook should exit at lifecycle failpoint");
}

struct CrashedReplay {
    _temp: tempfile::TempDir,
    case: PreparedPackageReplayCase,
    db_path: PathBuf,
    destination: DuckDbDestination,
    store: SqliteCheckpointStore,
    receipt: Option<Receipt>,
    snapshot: DuckDbMirrorSnapshot,
}

fn stage_helper_crash(
    package_id: &str,
    checkpoint_id: &str,
    failpoint: LifecycleFailpoint,
) -> CrashedReplay {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join(package_id);
    let fixture = prepared_fixture(&package_dir, package_id, checkpoint_id);
    let case = fixture.replay_case().unwrap();
    let db_path = temp.path().join("local.duckdb");
    let sqlite_path = temp.path().join("state.sqlite");

    // Test-only helper respawns this exact libtest binary to cross a process boundary.
    let helper_exe = env::current_exe().unwrap(); // nosemgrep: rust.lang.security.current-exe.current-exe
    let output = Command::new(helper_exe)
        .arg("--exact")
        .arg(HELPER_TEST_NAME)
        .arg("--nocapture")
        .env(HELPER_ENV, "1")
        .env(HELPER_FAILPOINT_ENV, lifecycle_failpoint_name(failpoint))
        .env(HELPER_PACKAGE_DIR_ENV, &case.package_dir)
        .env(HELPER_DUCKDB_PATH_ENV, &db_path)
        .env(HELPER_SQLITE_PATH_ENV, &sqlite_path)
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(HELPER_EXIT_CODE),
        "helper did not exit at receipt hook\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let destination = DuckDbDestination::new(&db_path).unwrap();
    let snapshot = read_mirror_snapshot_if_exists(&destination);
    let receipts = PackageReader::open(&case.package_dir)
        .unwrap()
        .receipts()
        .unwrap();
    let store = SqliteCheckpointStore::open(&sqlite_path).unwrap();

    CrashedReplay {
        _temp: temp,
        case,
        db_path,
        destination,
        store,
        receipt: receipts.first().cloned(),
        snapshot,
    }
}

fn read_mirror_snapshot_if_exists(destination: &DuckDbDestination) -> DuckDbMirrorSnapshot {
    if destination.database_path().exists() {
        destination.read_mirror_snapshot_read_only().unwrap()
    } else {
        DuckDbMirrorSnapshot::default()
    }
}

fn prepared_fixture(
    package_dir: &Path,
    package_id: &str,
    checkpoint_id: &str,
) -> PreparedPackageFixture {
    let mut spec = PreparedPackageFixtureSpec::new(package_dir, package_id).unwrap();
    spec.checkpoint_id = CheckpointId::new(checkpoint_id).unwrap();
    build_prepared_package_fixture(spec).unwrap()
}

fn fake_receipt(case: &PreparedPackageReplayCase) -> Receipt {
    Receipt {
        receipt_id: ReceiptId::new(format!("receipt-{}", case.delta.checkpoint_id)).unwrap(),
        destination: DestinationId::new("duckdb").unwrap(),
        target: case.target.clone(),
        package_hash: PackageHash::new(case.delta.package_hash.as_str()).unwrap(),
        segment_acks: case
            .delta
            .segments
            .iter()
            .map(|segment| SegmentAck {
                segment_id: segment.segment_id.clone(),
                row_count: segment.row_count,
                byte_count: segment.byte_count,
            })
            .collect(),
        disposition: case.disposition.clone(),
        idempotency_token: IdempotencyToken::new(case.delta.package_hash.as_str()).unwrap(),
        transaction: None,
        counts: CommitCounts {
            rows_written: case
                .delta
                .segments
                .iter()
                .map(|segment| segment.row_count)
                .sum(),
            rows_inserted: Some(3),
            rows_updated: Some(0),
            rows_deleted: Some(0),
        },
        schema_hash: case.schema_hash.clone(),
        migrations: Vec::new(),
        committed_at_ms: 1_700_000_000_000,
        verify: VerifyClause {
            kind: "sql".to_owned(),
            statement: "select 1".to_owned(),
            parameters: BTreeMap::new(),
        },
    }
}

fn assert_harness_panics(f: impl FnOnce()) {
    assert!(
        catch_unwind(AssertUnwindSafe(f)).is_err(),
        "corrupted conformance case passed the harness"
    );
}

fn parse_lifecycle_failpoint(value: &str) -> LifecycleFailpoint {
    match value {
        "after_packaged_before_destination_write" => {
            LifecycleFailpoint::PackagedBeforeDestinationWrite
        }
        "after_checkpoint_proposal_before_destination_write" => {
            LifecycleFailpoint::CheckpointProposalBeforeDestinationWrite
        }
        "after_receipt_verified_before_checkpoint_commit" => {
            LifecycleFailpoint::ReceiptVerifiedBeforeCheckpointCommit
        }
        "after_checkpoint_commit_before_package_status_checkpointed" => {
            LifecycleFailpoint::CheckpointCommitBeforePackageStatusCheckpointed
        }
        other => panic!("unknown helper lifecycle failpoint {other}"),
    }
}

fn lifecycle_failpoint_name(failpoint: LifecycleFailpoint) -> &'static str {
    match failpoint {
        LifecycleFailpoint::PackagedBeforeDestinationWrite => {
            "after_packaged_before_destination_write"
        }
        LifecycleFailpoint::CheckpointProposalBeforeDestinationWrite => {
            "after_checkpoint_proposal_before_destination_write"
        }
        LifecycleFailpoint::ReceiptVerifiedBeforeCheckpointCommit => {
            "after_receipt_verified_before_checkpoint_commit"
        }
        LifecycleFailpoint::CheckpointCommitBeforePackageStatusCheckpointed => {
            "after_checkpoint_commit_before_package_status_checkpointed"
        }
    }
}

fn lifecycle_failpoint_from_stage(
    stage: RuntimeStage<'_>,
) -> Option<(LifecycleFailpoint, Option<&Receipt>)> {
    match stage {
        RuntimeStage::PackageReplayVerified => {
            Some((LifecycleFailpoint::PackagedBeforeDestinationWrite, None))
        }
        RuntimeStage::DestinationWriteReady => Some((
            LifecycleFailpoint::CheckpointProposalBeforeDestinationWrite,
            None,
        )),
        RuntimeStage::DestinationReceiptRecorded { receipt } => Some((
            LifecycleFailpoint::ReceiptVerifiedBeforeCheckpointCommit,
            Some(receipt),
        )),
        RuntimeStage::CheckpointCommitted { checkpoint } => Some((
            LifecycleFailpoint::CheckpointCommitBeforePackageStatusCheckpointed,
            checkpoint.receipt.as_ref(),
        )),
        RuntimeStage::CheckpointProposed { .. }
        | RuntimeStage::DestinationCommitStarted { .. }
        | RuntimeStage::DestinationSegmentAcknowledged { .. }
        | RuntimeStage::PackageStatusUpdated { .. } => None,
    }
}
