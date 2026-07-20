use std::{cell::Cell, path::Path};

use cdf_engine::EnginePlan;
use cdf_kernel::{
    CdfError, CheckpointStatus, CheckpointStore, IdempotencySupport, PipelineId, QueryableResource,
    Receipt, ResourceId, Result, ScopeKey,
};
use cdf_package::PackageReader;
use cdf_package_contract::PackageStatus;
use cdf_project::{PackageArtifactReplayRequest, ProjectRunReport, replay_package_from_artifacts};
use cdf_state_sqlite::SqliteCheckpointStore;

use super::{
    MatrixDisposition, RunMatrixCell,
    core::{ROW_COUNT, SEGMENT_COUNT},
    destinations::MatrixDestinationHandle,
    test_support::copy_dir_all,
};

pub(crate) fn assert_plan_honesty(
    plan: &EnginePlan,
    resource: &dyn QueryableResource,
    package_id: &str,
) {
    let descriptor = resource.descriptor();
    assert_eq!(plan.scan.request.resource_id, descriptor.resource_id);
    assert_eq!(plan.package_id, package_id);
    assert_eq!(plan.scan.request.scope, descriptor.state_scope);
}

pub(crate) fn receipt_gate<'a>(
    state_store_path: &'a Path,
    pipeline_id: &'a PipelineId,
    resource_id: &'a ResourceId,
    scope: &'a ScopeKey,
    observed: &'a Cell<bool>,
) -> impl Fn(&Receipt) -> Result<()> + 'a {
    move |_receipt: &Receipt| {
        assert_no_checkpoint_head_at_receipt_verified(
            state_store_path,
            pipeline_id,
            resource_id,
            scope,
        )?;
        observed.set(true);
        Ok(())
    }
}

pub(crate) fn assert_run_report(
    cell: RunMatrixCell,
    report: &ProjectRunReport,
    resource_id: &ResourceId,
    scope: &ScopeKey,
    pipeline_id: &PipelineId,
) {
    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert!(report.checkpoint.is_head);
    assert_eq!(report.checkpoint.delta.pipeline_id, *pipeline_id);
    assert_eq!(report.checkpoint.delta.resource_id, *resource_id);
    assert_eq!(report.checkpoint.delta.scope, *scope);
    assert_eq!(
        report.checkpoint.delta.package_hash,
        report.receipt.package_hash
    );
    assert_eq!(
        report.receipt.disposition,
        cell.disposition.to_write_disposition()
    );
    assert_eq!(report.row_count, ROW_COUNT);
    assert_eq!(report.segment_count, SEGMENT_COUNT);
    assert_eq!(report.receipt.counts.rows_written, ROW_COUNT);
    assert_eq!(
        report
            .receipt
            .segment_acks
            .iter()
            .map(|ack| ack.row_count)
            .sum::<u64>(),
        ROW_COUNT
    );
}

pub(crate) fn assert_replay_inputs_match_run(cell: RunMatrixCell, report: &ProjectRunReport) {
    let replay_inputs = PackageReader::open(&report.package_dir)
        .unwrap()
        .replay_inputs()
        .unwrap();
    assert_eq!(replay_inputs.state_delta, report.checkpoint.delta);
    assert_eq!(
        replay_inputs.destination_commit.package_hash,
        report.package_hash
    );
    assert_eq!(
        replay_inputs.destination_commit.target,
        report.receipt.target
    );
    assert_eq!(
        replay_inputs.destination_commit.disposition,
        cell.disposition.to_write_disposition()
    );
    assert_eq!(
        replay_inputs.destination_commit.idempotency_token.as_str(),
        report.package_hash.as_str()
    );
    assert_eq!(replay_inputs.schema_hash, report.receipt.schema_hash);
    let expected_merge_keys = if cell.disposition == MatrixDisposition::Merge {
        vec!["id".to_owned()]
    } else {
        Vec::new()
    };
    assert_eq!(replay_inputs.merge_keys, expected_merge_keys);
}

pub(crate) fn assert_committed_checkpoint(state_store_path: &Path, report: &ProjectRunReport) {
    let store = SqliteCheckpointStore::open(state_store_path).unwrap();
    let head = store
        .head(
            &report.checkpoint.delta.pipeline_id,
            &report.checkpoint.delta.resource_id,
            &report.checkpoint.delta.scope,
        )
        .unwrap()
        .expect("checkpoint head");
    assert_eq!(head.status, CheckpointStatus::Committed);
    assert!(head.is_head);
    assert_eq!(head.delta, report.checkpoint.delta);
    assert_eq!(head.receipt.as_ref(), Some(&report.receipt));
}

pub(crate) fn assert_duplicate_replay_noop(
    cell: RunMatrixCell,
    destination: &MatrixDestinationHandle,
    report: &ProjectRunReport,
    root: &Path,
) -> Result<String> {
    assert_eq!(destination.idempotency()?, IdempotencySupport::PackageToken);
    let before = destination.footprint()?;
    let duplicate_store = SqliteCheckpointStore::open(root.join(format!(
        ".cdf/duplicate-{}.sqlite",
        cell.disposition.as_str()
    )))?;
    let duplicate = replay_package_from_artifacts(PackageArtifactReplayRequest {
        package_dir: report.package_dir.clone(),
        destination: destination.resolved()?,
        checkpoint_store: &duplicate_store,
        after_receipt_verified: None,
    })?;
    let after = destination.footprint()?;

    assert_eq!(
        before, after,
        "duplicate replay must not mutate destination"
    );
    assert_eq!(duplicate.checkpoint.delta, report.checkpoint.delta);
    assert_eq!(duplicate.checkpoint.status, CheckpointStatus::Committed);
    assert_receipt_core_identity(&report.receipt, &duplicate.receipt);
    assert_eq!(duplicate.receipt, report.receipt);
    let duplicate_head = duplicate_store
        .head(
            &duplicate.checkpoint.delta.pipeline_id,
            &duplicate.checkpoint.delta.resource_id,
            &duplicate.checkpoint.delta.scope,
        )?
        .expect("duplicate checkpoint head");
    assert_eq!(duplicate_head.status, CheckpointStatus::Committed);
    destination.assert_receipt_identity(&duplicate.receipt)?;
    destination.verify_trait_receipt(&duplicate.receipt)?;

    Ok(destination.duplicate_retry_behavior(duplicate.receipt_source))
}

pub(crate) fn assert_artifact_replay_identity(
    cell: RunMatrixCell,
    destination: &MatrixDestinationHandle,
    report: &ProjectRunReport,
    root: &Path,
) -> Result<()> {
    let expected_payload = destination.payload_snapshot()?;
    let replay_package_dir = root
        .join(".cdf/replay-packages")
        .join(format!("{}-copy", report.package_id));
    copy_dir_all(&report.package_dir, &replay_package_dir)?;
    let replay_destination = destination.fresh_artifact_replay_destination(root)?;
    let replay_store = SqliteCheckpointStore::open(root.join(format!(
        ".cdf/artifact-{}.sqlite",
        cell.disposition.as_str()
    )))?;
    let replay = replay_package_from_artifacts(PackageArtifactReplayRequest {
        package_dir: replay_package_dir.clone(),
        destination: replay_destination.resolved()?,
        checkpoint_store: &replay_store,
        after_receipt_verified: None,
    })?;

    assert_eq!(replay.package_status, PackageStatus::Checkpointed);
    assert_eq!(replay.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(replay.checkpoint.delta, report.checkpoint.delta);
    assert_receipt_core_identity(&report.receipt, &replay.receipt);
    replay_destination.assert_receipt_identity(&replay.receipt)?;
    replay_destination.verify_trait_receipt(&replay.receipt)?;
    assert_eq!(
        replay_destination.payload_snapshot()?,
        expected_payload,
        "artifact replay must reproduce the destination payload exactly"
    );
    PackageReader::open(&replay_package_dir)?.verify()?;
    let replay_head = replay_store
        .head(
            &replay.checkpoint.delta.pipeline_id,
            &replay.checkpoint.delta.resource_id,
            &replay.checkpoint.delta.scope,
        )?
        .expect("artifact replay checkpoint head");
    assert_eq!(replay_head.status, CheckpointStatus::Committed);
    Ok(())
}

fn assert_no_checkpoint_head_at_receipt_verified(
    state_store_path: &Path,
    pipeline_id: &PipelineId,
    resource_id: &ResourceId,
    scope: &ScopeKey,
) -> Result<()> {
    let store = SqliteCheckpointStore::open(state_store_path)?;
    let history = store.history(pipeline_id, resource_id, scope)?;
    if history.len() != 1 {
        return Err(CdfError::contract(format!(
            "receipt-verified gate expected one proposed checkpoint, found {}",
            history.len()
        )));
    }
    if history[0].status != CheckpointStatus::Proposed || history[0].is_head {
        return Err(CdfError::contract(
            "receipt-verified gate observed a checkpoint that was not proposed-only",
        ));
    }
    if store.head(pipeline_id, resource_id, scope)?.is_some() {
        return Err(CdfError::contract(
            "checkpoint head advanced before receipt verification gate returned",
        ));
    }
    Ok(())
}

fn assert_receipt_core_identity(expected: &Receipt, actual: &Receipt) {
    assert_eq!(actual.receipt_id, expected.receipt_id);
    assert_eq!(actual.destination, expected.destination);
    assert_eq!(actual.target, expected.target);
    assert_eq!(actual.package_hash, expected.package_hash);
    assert_eq!(actual.schema_hash, expected.schema_hash);
    assert_eq!(actual.disposition, expected.disposition);
    assert_eq!(actual.idempotency_token, expected.idempotency_token);
    assert_eq!(actual.segment_acks, expected.segment_acks);
    assert_eq!(actual.counts, expected.counts);
}
