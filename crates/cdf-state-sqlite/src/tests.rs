use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Barrier},
    thread,
};

use cdf_conformance::checkpoint_store::{
    assert_checkpoint_store_conformance, assert_checkpoint_store_send_sync,
};
use cdf_conformance::schema_authority::{
    assert_schema_authority_store_conformance, assert_schema_authority_store_send_sync,
    first_use_schema_authority_establishment, schema_authority_version,
};
use cdf_conformance::scope_lease::{
    ManualScopeLeaseClock, assert_scope_lease_store_conformance, assert_scope_lease_store_send_sync,
};
use cdf_kernel::{
    CHECKPOINT_STATE_VERSION, Checkpoint, CheckpointId, CheckpointStatus, CheckpointStore,
    CommitCounts, CommittedLogPosition, CompositePosition, ContractRef, CursorPosition,
    CursorValue, DestinationId, ErrorKind, EventTimeDomain, FileManifest, FilePosition,
    ForeignState, IdempotencyToken, LeaseOwnerId, MigrationRecord, MongoChangeStreamResumeToken,
    MongoChangeStreamScope, MongoResumeMode, MongoResumeTokenSource, MongoWatchLevel,
    PARTITION_WATERMARK_STATE_VERSION, PackageHash, PageToken, PartitionId,
    PartitionWatermarkState, PipelineId, PlanId, PostgresCommitPosition, PostgresLogScope,
    PromotionId, Receipt, ReceiptId, ResourceId, ResumeTokenPosition, RewindRequest, RunId,
    SOURCE_POSITION_VERSION, STREAM_EPOCH_POLICY_VERSION, SchemaAuthorityStore, SchemaHash,
    SchemaPromotionFence, SchemaPromotionPlanState, SchemaPromotionTarget, SchemaSettlementStore,
    ScopeKey, ScopeLeaseStore, SegmentAck, SegmentId, SourcePosition, StateDelta, StateSegment,
    TableSnapshotPosition, TableSnapshotSelector, TargetName, VerifyClause,
    WATERMARK_CLAIM_VERSION, WatermarkAuthority, WatermarkClaim, WatermarkObservationContext,
    WatermarkValue, WriteDisposition,
};
use rusqlite::{Connection, params};
use tempfile::tempdir;

use crate::sqlite::CHECKPOINT_STORE_SCHEMA_VERSION;
use crate::support::encode_json;

use super::*;

fn pipeline_id() -> PipelineId {
    PipelineId::new("pipeline-1").unwrap()
}

fn resource_id() -> ResourceId {
    ResourceId::new("orders").unwrap()
}

fn other_resource_id() -> ResourceId {
    ResourceId::new("customers").unwrap()
}

fn partition_scope() -> ScopeKey {
    ScopeKey::Partition {
        partition_id: PartitionId::new("p0").unwrap(),
    }
}

fn other_partition_scope() -> ScopeKey {
    ScopeKey::Partition {
        partition_id: PartitionId::new("p1").unwrap(),
    }
}

fn cursor_position(value: i64) -> SourcePosition {
    SourcePosition::Cursor(CursorPosition {
        version: SOURCE_POSITION_VERSION,
        field: "updated_at".to_owned(),
        value: CursorValue::I64(value),
    })
}

fn table_snapshot_position() -> SourcePosition {
    SourcePosition::TableSnapshot(Box::new(TableSnapshotPosition {
        version: SOURCE_POSITION_VERSION,
        protocol: "iceberg".to_owned(),
        catalog: "glue:us-east-1:123456789012".to_owned(),
        namespace: vec!["analytics".to_owned()],
        table: "orders".to_owned(),
        selector: TableSnapshotSelector::Current,
        snapshot_id: 42,
        sequence_number: 7,
        parent_snapshot_id: Some(41),
        metadata_location: "s3://warehouse/analytics/orders/metadata/v7.json".to_owned(),
        metadata_generation: "version-id:v7".to_owned(),
    }))
}

fn postgres_log_position(end_lsn: u64) -> SourcePosition {
    SourcePosition::committed_log(CommittedLogPosition::PostgreSql(PostgresCommitPosition {
        version: SOURCE_POSITION_VERSION,
        scope: PostgresLogScope {
            system_identifier: "7421938841407953395".to_owned(),
            database_oid: 16_384,
            slot: "orders".to_owned(),
            output_plugin: "pgoutput".to_owned(),
            semantics_sha256:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        },
        commit_lsn: end_lsn.saturating_sub(1).max(1),
        end_lsn,
        xid: 7,
    }))
}

fn mongo_resume_token(token_bson_base64: &str, token_sha256: &str) -> SourcePosition {
    SourcePosition::resume_token(ResumeTokenPosition::MongoChangeStream(
        MongoChangeStreamResumeToken {
            version: SOURCE_POSITION_VERSION,
            scope: MongoChangeStreamScope {
                source_binding: "orders-stream".to_owned(),
                watch_level: MongoWatchLevel::Collection,
                database: Some("sales".to_owned()),
                collection: Some("orders".to_owned()),
                pipeline_sha256:
                    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_owned(),
                options_sha256:
                    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                        .to_owned(),
            },
            token_bson_base64: token_bson_base64.to_owned(),
            token_sha256: token_sha256.to_owned(),
            resume_mode: MongoResumeMode::ResumeAfter,
            token_source: MongoResumeTokenSource::Event,
        },
    ))
}

fn delta(
    checkpoint_id: &str,
    parent: Option<&StateDelta>,
    scope: ScopeKey,
    output_position: SourcePosition,
    package_hash: &str,
) -> StateDelta {
    delta_for(
        checkpoint_id,
        parent,
        pipeline_id(),
        resource_id(),
        scope,
        output_position,
        package_hash,
    )
}

fn delta_for(
    checkpoint_id: &str,
    parent: Option<&StateDelta>,
    pipeline_id: PipelineId,
    resource_id: ResourceId,
    scope: ScopeKey,
    output_position: SourcePosition,
    package_hash: &str,
) -> StateDelta {
    let segment = StateSegment {
        kind: cdf_kernel::PackageSegmentKind::Row,
        segment_id: SegmentId::new(format!("{checkpoint_id}-segment")).unwrap(),
        scope: scope.clone(),
        output_position: output_position.clone(),
        row_count: 10,
        byte_count: 80,
    };
    StateDelta {
        checkpoint_id: CheckpointId::new(checkpoint_id).unwrap(),
        pipeline_id,
        resource_id,
        scope,
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: parent.map(|delta| delta.checkpoint_id.clone()),
        input_position: parent.map(|delta| delta.output_position.clone()),
        output_position,
        output_watermark: None,
        partition_watermarks: Vec::new(),
        late_data_carryover: Vec::new(),
        source_continuation: None,
        package_hash: PackageHash::new(package_hash).unwrap(),
        content: cdf_kernel::PackageContentAuthority::rows(
            SchemaHash::new("schema-sha256").unwrap(),
        ),
        schema_hash: SchemaHash::new("schema-sha256").unwrap(),
        segments: vec![segment],
    }
}

fn receipt(delta: &StateDelta) -> Receipt {
    Receipt {
        receipt_id: ReceiptId::new(format!("receipt-{}", delta.checkpoint_id)).unwrap(),
        destination: DestinationId::new("local-test").unwrap(),
        target: TargetName::new("orders").unwrap(),
        package_hash: delta.package_hash.clone(),
        content: delta.content.clone(),
        segment_acks: delta
            .segments
            .iter()
            .map(|segment| SegmentAck {
                kind: segment.kind,
                segment_id: segment.segment_id.clone(),
                row_count: segment.row_count,
                byte_count: segment.byte_count,
            })
            .collect(),
        disposition: WriteDisposition::Merge,
        idempotency_token: IdempotencyToken::new(delta.package_hash.as_str()).unwrap(),
        transaction: None,
        counts: CommitCounts {
            rows_written: 10,
            rows_inserted: Some(10),
            rows_updated: Some(0),
            rows_deleted: Some(0),
        },
        schema_hash: delta.schema_hash.clone(),
        migrations: Vec::<MigrationRecord>::new(),
        committed_at_ms: 1_700_000_000_000,
        verify: VerifyClause {
            kind: "sql".to_owned(),
            statement: "select count(*) from orders where _cdf_package = ?".to_owned(),
            parameters: BTreeMap::new(),
        },
    }
}

fn assert_plausible_created_at(checkpoint: &Checkpoint) {
    assert!(
        checkpoint.created_at_ms > 1_600_000_000_000,
        "checkpoint timestamp should be a plausible positive epoch millisecond"
    );
}

#[test]
fn sqlite_duplicate_caller_checkpoint_id_is_contract_owned() {
    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    let original = delta(
        "checkpoint-duplicate-id",
        None,
        partition_scope(),
        cursor_position(1),
        "package-duplicate-id",
    );
    store.propose(original.clone()).unwrap();

    let error = store.propose(original).unwrap_err();

    assert_eq!(error.kind, ErrorKind::Contract);
    assert!(error.message.contains("already exists"));
}

#[test]
fn in_memory_store_uses_ephemeral_sqlite_error_context() {
    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    store
        .execute_for_test("PRAGMA query_only = ON", [])
        .unwrap();

    let error = store
        .execute_classified_for_test("CREATE TABLE forbidden(value INTEGER)", [])
        .unwrap_err();

    assert_eq!(error.kind, ErrorKind::Internal);
    assert!(
        error
            .message
            .contains("ephemeral in-memory SQLite workspace")
    );
    assert!(!error.message.contains("state path"));
}

#[test]
fn committed_schema_streak_reads_only_the_bounded_newest_suffix() {
    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    let scope = partition_scope();
    let mut parent = None::<StateDelta>;
    for (ordinal, schema) in ["schema-a", "schema-a", "schema-b", "schema-b"]
        .into_iter()
        .enumerate()
    {
        let mut next = delta(
            &format!("streak-{ordinal}"),
            parent.as_ref(),
            scope.clone(),
            cursor_position(i64::try_from(ordinal + 1).unwrap()),
            &format!("package-streak-{ordinal}"),
        );
        next.schema_hash = SchemaHash::new(schema).unwrap();
        let checkpoint_id = next.checkpoint_id.clone();
        store.propose(next.clone()).unwrap();
        store.commit(&checkpoint_id, receipt(&next)).unwrap();
        parent = Some(next);
    }

    assert_eq!(
        store
            .committed_schema_streak(
                &pipeline_id(),
                &resource_id(),
                &scope,
                &SchemaHash::new("schema-b").unwrap(),
                10,
            )
            .unwrap(),
        2
    );
    assert_eq!(
        store
            .committed_schema_streak(
                &pipeline_id(),
                &resource_id(),
                &scope,
                &SchemaHash::new("schema-b").unwrap(),
                1,
            )
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .committed_schema_streak(
                &pipeline_id(),
                &resource_id(),
                &scope,
                &SchemaHash::new("schema-a").unwrap(),
                10,
            )
            .unwrap(),
        0
    );
}

#[test]
fn committed_schema_streak_rejects_scalar_rows_that_disagree_with_private_authority() {
    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    let scope = partition_scope();
    let mut checkpoint = delta(
        "streak-corrupt",
        None,
        scope.clone(),
        cursor_position(1),
        "package-streak-corrupt",
    );
    checkpoint.schema_hash = SchemaHash::new("schema-original").unwrap();
    commit_delta(&store, checkpoint.clone());
    store
        .execute_for_test(
            "UPDATE cdf_checkpoints SET schema_hash = ? WHERE checkpoint_id = ?",
            params!["schema-forged", checkpoint.checkpoint_id.as_str(),],
        )
        .unwrap();

    let error = store
        .committed_schema_streak(
            &checkpoint.pipeline_id,
            &checkpoint.resource_id,
            &scope,
            &SchemaHash::new("schema-forged").unwrap(),
            1,
        )
        .unwrap_err();

    assert_eq!(error.kind, ErrorKind::Internal);
    assert!(
        error
            .message
            .contains("columns do not match serialized state delta"),
        "{}",
        error.message
    );
}

fn commit_delta<S: CheckpointStore>(store: &S, delta: StateDelta) -> Checkpoint {
    let checkpoint_id = delta.checkpoint_id.clone();
    let receipt = receipt(&delta);
    let receipt_committed_at_ms = receipt.committed_at_ms;
    let proposed = store.propose(delta).unwrap();
    assert_plausible_created_at(&proposed);
    assert_eq!(proposed.committed_at_ms, None);
    let committed = store.commit(&checkpoint_id, receipt).unwrap();
    assert_plausible_created_at(&committed);
    assert_eq!(committed.committed_at_ms, Some(receipt_committed_at_ms));
    committed
}

fn with_watermark(mut delta: StateDelta, value: i64) -> StateDelta {
    let claim = WatermarkClaim {
        version: WATERMARK_CLAIM_VERSION,
        policy_version: STREAM_EPOCH_POLICY_VERSION,
        event_time_field: "updated_at".into(),
        domain: EventTimeDomain::SignedInteger,
        value: WatermarkValue::Signed(value),
        partition_id: PartitionId::new("p0").unwrap(),
        source_position: delta.output_position.clone(),
        authority: WatermarkAuthority::Source,
        observation_context: WatermarkObservationContext::EpochBarrier,
    };
    delta.output_watermark = Some(claim.clone());
    delta.partition_watermarks = vec![PartitionWatermarkState {
        version: PARTITION_WATERMARK_STATE_VERSION,
        partition_id: claim.partition_id.clone(),
        claim: Some(claim),
        idleness: None,
    }];
    delta
}

fn assert_store_rejects_watermark_erasure_and_commit_races<S: CheckpointStore>(store: &S) {
    let first = commit_delta(
        store,
        with_watermark(
            delta(
                "checkpoint-watermark-100",
                None,
                partition_scope(),
                cursor_position(1),
                "package-watermark-100",
            ),
            100,
        ),
    );

    let erased = delta(
        "checkpoint-watermark-erased",
        Some(&first.delta),
        partition_scope(),
        cursor_position(2),
        "package-watermark-erased",
    );
    assert!(
        store
            .propose(erased)
            .unwrap_err()
            .message
            .contains("cannot erase")
    );

    let mut partition_regression = with_watermark(
        delta(
            "checkpoint-partition-watermark-regression",
            Some(&first.delta),
            partition_scope(),
            cursor_position(2),
            "package-partition-watermark-regression",
        ),
        100,
    );
    partition_regression.partition_watermarks[0]
        .claim
        .as_mut()
        .unwrap()
        .value = WatermarkValue::Signed(90);
    let error = store.propose(partition_regression).unwrap_err();
    assert!(error.message.contains("regressed"), "{error}");

    let stale = with_watermark(
        delta(
            "checkpoint-watermark-110",
            Some(&first.delta),
            partition_scope(),
            cursor_position(3),
            "package-watermark-110",
        ),
        110,
    );
    store.propose(stale.clone()).unwrap();
    let newer = with_watermark(
        delta(
            "checkpoint-watermark-120",
            Some(&first.delta),
            partition_scope(),
            cursor_position(4),
            "package-watermark-120",
        ),
        120,
    );
    commit_delta(store, newer);

    let error = store
        .commit(&stale.checkpoint_id, receipt(&stale))
        .unwrap_err();
    assert!(error.message.contains("current head"));
    assert_eq!(
        store
            .head(&stale.pipeline_id, &stale.resource_id, &stale.scope)
            .unwrap()
            .unwrap()
            .delta
            .output_watermark
            .unwrap()
            .value,
        WatermarkValue::Signed(120)
    );
}

fn assert_store_rejects_bad_receipts<S: CheckpointStore>(store: &S) {
    let delta = delta(
        "checkpoint-bad-receipt",
        None,
        partition_scope(),
        cursor_position(1),
        "package-sha256",
    );
    let checkpoint_id = delta.checkpoint_id.clone();
    let proposed = store.propose(delta.clone()).unwrap();
    assert_plausible_created_at(&proposed);

    let mut wrong_package = receipt(&delta);
    wrong_package.package_hash = PackageHash::new("other-package-sha256").unwrap();
    assert!(store.commit(&checkpoint_id, wrong_package).is_err());

    let mut missing_segment = receipt(&delta);
    missing_segment.segment_acks.clear();
    assert!(store.commit(&checkpoint_id, missing_segment).is_err());

    let mut wrong_counts = receipt(&delta);
    wrong_counts.segment_acks[0].row_count += 1;
    assert!(store.commit(&checkpoint_id, wrong_counts).is_err());

    assert!(
        store
            .head(&delta.pipeline_id, &delta.resource_id, &delta.scope)
            .unwrap()
            .is_none()
    );
}

fn assert_abandon_keeps_proposed_checkpoint_out_of_head<S: CheckpointStore>(store: &S) {
    let delta = delta(
        "checkpoint-abandon",
        None,
        partition_scope(),
        cursor_position(1),
        "package-abandon",
    );
    let checkpoint_id = delta.checkpoint_id.clone();
    let proposed = store.propose(delta.clone()).unwrap();
    assert_plausible_created_at(&proposed);

    let abandoned = store.abandon(&checkpoint_id).unwrap();
    assert_eq!(abandoned.status, CheckpointStatus::Abandoned);
    assert_plausible_created_at(&abandoned);
    assert_eq!(abandoned.committed_at_ms, None);
    assert!(store.commit(&checkpoint_id, receipt(&delta)).is_err());
    assert!(
        store
            .head(&delta.pipeline_id, &delta.resource_id, &delta.scope)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .history(&delta.pipeline_id, &delta.resource_id, &delta.scope)
            .unwrap(),
        vec![abandoned]
    );
}

fn assert_scope_and_resource_isolation<S: CheckpointStore>(store: &S) {
    let scope = partition_scope();
    let other_scope = other_partition_scope();
    let first = commit_delta(
        store,
        delta(
            "checkpoint-isolation-main",
            None,
            scope.clone(),
            cursor_position(1),
            "package-isolation-main",
        ),
    );
    let other_resource = commit_delta(
        store,
        delta_for(
            "checkpoint-isolation-resource",
            None,
            pipeline_id(),
            other_resource_id(),
            scope.clone(),
            cursor_position(2),
            "package-isolation-resource",
        ),
    );
    let other_scope_checkpoint = commit_delta(
        store,
        delta_for(
            "checkpoint-isolation-scope",
            None,
            pipeline_id(),
            resource_id(),
            other_scope.clone(),
            cursor_position(3),
            "package-isolation-scope",
        ),
    );

    assert_eq!(
        store
            .head(&pipeline_id(), &resource_id(), &scope)
            .unwrap()
            .unwrap()
            .delta
            .checkpoint_id,
        first.delta.checkpoint_id
    );
    assert_eq!(
        store
            .head(&pipeline_id(), &other_resource_id(), &scope)
            .unwrap()
            .unwrap()
            .delta
            .checkpoint_id,
        other_resource.delta.checkpoint_id
    );
    assert_eq!(
        store
            .head(&pipeline_id(), &resource_id(), &other_scope)
            .unwrap()
            .unwrap()
            .delta
            .checkpoint_id,
        other_scope_checkpoint.delta.checkpoint_id
    );
    assert!(
        store
            .head(&pipeline_id(), &other_resource_id(), &other_scope)
            .unwrap()
            .is_none()
    );

    let main_history = store
        .history(&pipeline_id(), &resource_id(), &scope)
        .unwrap();
    assert_eq!(main_history.len(), 1);
    assert_eq!(
        main_history[0].delta.checkpoint_id,
        first.delta.checkpoint_id
    );
    assert_eq!(
        store
            .history(&pipeline_id(), &other_resource_id(), &scope)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .history(&pipeline_id(), &resource_id(), &other_scope)
            .unwrap()
            .len(),
        1
    );
    assert!(
        store
            .history(&pipeline_id(), &other_resource_id(), &other_scope)
            .unwrap()
            .is_empty()
    );

    assert!(
        store
            .rewind(RewindRequest {
                marker_checkpoint_id: CheckpointId::new("rewind-wrong-resource").unwrap(),
                pipeline_id: pipeline_id(),
                resource_id: resource_id(),
                scope: scope.clone(),
                target_checkpoint_id: other_resource.delta.checkpoint_id,
            })
            .is_err()
    );
    assert!(
        store
            .rewind(RewindRequest {
                marker_checkpoint_id: CheckpointId::new("rewind-wrong-scope").unwrap(),
                pipeline_id: pipeline_id(),
                resource_id: resource_id(),
                scope,
                target_checkpoint_id: other_scope_checkpoint.delta.checkpoint_id,
            })
            .is_err()
    );
}

fn assert_rewind_appends_marker_and_reports_packages_ahead<S: CheckpointStore>(store: &S) {
    let scope = partition_scope();
    let first = commit_delta(
        store,
        delta(
            "checkpoint-1",
            None,
            scope.clone(),
            cursor_position(1),
            "package-1",
        ),
    );
    let second = commit_delta(
        store,
        delta(
            "checkpoint-2",
            Some(&first.delta),
            scope.clone(),
            cursor_position(2),
            "package-2",
        ),
    );
    let third = commit_delta(
        store,
        delta(
            "checkpoint-3",
            Some(&second.delta),
            scope.clone(),
            cursor_position(3),
            "package-3",
        ),
    );

    let report = store
        .rewind(RewindRequest {
            marker_checkpoint_id: CheckpointId::new("rewind-marker-1").unwrap(),
            pipeline_id: pipeline_id(),
            resource_id: resource_id(),
            scope: scope.clone(),
            target_checkpoint_id: first.delta.checkpoint_id.clone(),
        })
        .unwrap();

    assert_eq!(report.marker.status, CheckpointStatus::Rewound);
    assert_plausible_created_at(&report.marker);
    assert_eq!(report.marker.committed_at_ms, None);
    assert_eq!(
        report.marker.rewind_target_checkpoint_id,
        Some(first.delta.checkpoint_id.clone())
    );
    assert_eq!(report.head.delta.checkpoint_id, first.delta.checkpoint_id);
    assert_eq!(
        report.packages_ahead,
        vec![third.delta.package_hash, second.delta.package_hash]
    );

    let history = store
        .history(&pipeline_id(), &resource_id(), &scope)
        .unwrap();
    assert_eq!(history.len(), 4);
    let historical_old_head = history
        .iter()
        .find(|checkpoint| checkpoint.delta.checkpoint_id == third.delta.checkpoint_id)
        .unwrap();
    assert_eq!(historical_old_head.status, CheckpointStatus::Committed);
    assert!(
        !historical_old_head.is_head,
        "rewind preserves later committed checkpoints as non-head history"
    );
    let target_history = history
        .iter()
        .find(|checkpoint| checkpoint.delta.checkpoint_id == first.delta.checkpoint_id)
        .unwrap();
    assert!(
        target_history.is_head,
        "rewind target becomes the committed head"
    );
    assert_eq!(
        store
            .head(&pipeline_id(), &resource_id(), &scope)
            .unwrap()
            .unwrap()
            .delta
            .checkpoint_id,
        first.delta.checkpoint_id
    );
}

fn assert_rewind_validation<S: CheckpointStore>(store: &S) {
    let scope = partition_scope();
    let first = commit_delta(
        store,
        delta(
            "checkpoint-rewind-validation-head",
            None,
            scope.clone(),
            cursor_position(1),
            "package-rewind-validation-head",
        ),
    );
    let proposed_delta = delta(
        "checkpoint-rewind-validation-proposed",
        Some(&first.delta),
        scope.clone(),
        cursor_position(2),
        "package-rewind-validation-proposed",
    );
    let proposed_id = proposed_delta.checkpoint_id.clone();
    store.propose(proposed_delta).unwrap();

    assert!(
        store
            .rewind(RewindRequest {
                marker_checkpoint_id: CheckpointId::new("rewind-to-proposed").unwrap(),
                pipeline_id: pipeline_id(),
                resource_id: resource_id(),
                scope: scope.clone(),
                target_checkpoint_id: proposed_id,
            })
            .is_err()
    );
    assert!(
        store
            .rewind(RewindRequest {
                marker_checkpoint_id: CheckpointId::new("rewind-to-missing").unwrap(),
                pipeline_id: pipeline_id(),
                resource_id: resource_id(),
                scope,
                target_checkpoint_id: CheckpointId::new("checkpoint-missing").unwrap(),
            })
            .is_err()
    );
}

fn assert_branch_rewind_reports_only_current_branch_ahead<S: CheckpointStore>(store: &S) {
    let scope = partition_scope();
    let base = commit_delta(
        store,
        delta(
            "checkpoint-branch-base",
            None,
            scope.clone(),
            cursor_position(1),
            "package-branch-base",
        ),
    );
    let target_branch = commit_delta(
        store,
        delta(
            "checkpoint-branch-target",
            Some(&base.delta),
            scope.clone(),
            cursor_position(2),
            "package-branch-target",
        ),
    );
    store
        .rewind(RewindRequest {
            marker_checkpoint_id: CheckpointId::new("rewind-branch-to-base").unwrap(),
            pipeline_id: pipeline_id(),
            resource_id: resource_id(),
            scope: scope.clone(),
            target_checkpoint_id: base.delta.checkpoint_id.clone(),
        })
        .unwrap();
    let current_branch_parent = commit_delta(
        store,
        delta(
            "checkpoint-branch-current-parent",
            Some(&base.delta),
            scope.clone(),
            cursor_position(3),
            "package-branch-current-parent",
        ),
    );
    let current_branch_head = commit_delta(
        store,
        delta(
            "checkpoint-branch-current-head",
            Some(&current_branch_parent.delta),
            scope.clone(),
            cursor_position(4),
            "package-branch-current-head",
        ),
    );

    let report = store
        .rewind(RewindRequest {
            marker_checkpoint_id: CheckpointId::new("rewind-branch-marker").unwrap(),
            pipeline_id: pipeline_id(),
            resource_id: resource_id(),
            scope,
            target_checkpoint_id: target_branch.delta.checkpoint_id.clone(),
        })
        .unwrap();

    assert_eq!(
        report.head.delta.checkpoint_id,
        target_branch.delta.checkpoint_id
    );
    assert_eq!(
        report.packages_ahead,
        vec![
            current_branch_head.delta.package_hash,
            current_branch_parent.delta.package_hash
        ]
    );
    assert!(
        !report.packages_ahead.contains(&base.delta.package_hash),
        "common ancestors of the rewind target are not ahead of state"
    );
    assert!(
        !report
            .packages_ahead
            .contains(&target_branch.delta.package_hash),
        "the target package itself is not ahead of state"
    );
}

#[test]
fn store_types_implement_thread_safe_checkpoint_store() {
    assert_checkpoint_store_send_sync::<InMemoryCheckpointStore>();
    assert_checkpoint_store_send_sync::<SqliteCheckpointStore>();
}

#[test]
fn in_memory_passes_checkpoint_store_conformance() {
    assert_checkpoint_store_conformance(InMemoryCheckpointStore::new);
}

#[test]
fn sqlite_passes_checkpoint_store_conformance() {
    assert_checkpoint_store_conformance(|| SqliteCheckpointStore::open_in_memory().unwrap());
}

#[test]
fn checkpoint_stores_preserve_committed_watermark_monotonicity() {
    assert_store_rejects_watermark_erasure_and_commit_races(&InMemoryCheckpointStore::new());
    assert_store_rejects_watermark_erasure_and_commit_races(
        &SqliteCheckpointStore::open_in_memory().unwrap(),
    );
}

fn assert_cdc_checkpoint_lineage<S: CheckpointStore>(store: &S) {
    let mongo_scope = ScopeKey::Stream {
        name: "orders-stream".to_owned(),
    };
    let first = commit_delta(
        store,
        delta(
            "checkpoint-mongo-first",
            None,
            mongo_scope.clone(),
            mongo_resume_token(
                "FgAAAAJfZGF0YQAGAAAAdG9rZW4AAA==",
                "sha256:2861e1850c87f3c48b875671d9fc0ca97b9c268ad17ff0b713a116989f2a68a2",
            ),
            "package-mongo-first",
        ),
    );
    let second = commit_delta(
        store,
        delta(
            "checkpoint-mongo-second",
            Some(&first.delta),
            mongo_scope,
            mongo_resume_token(
                "FgAAAAJfZGF0YQAGAAAAb3RoZXIAAA==",
                "sha256:bdb91fa8b987e91d9dc90291f0ead9dcf82ee6ca717aaf692525d4493e6d0b21",
            ),
            "package-mongo-second",
        ),
    );
    assert_eq!(
        store
            .head(
                &second.delta.pipeline_id,
                &second.delta.resource_id,
                &second.delta.scope,
            )
            .unwrap()
            .unwrap()
            .delta
            .checkpoint_id,
        second.delta.checkpoint_id
    );

    let postgres_scope = ScopeKey::Stream {
        name: "orders".to_owned(),
    };
    let first = commit_delta(
        store,
        delta(
            "checkpoint-postgres-first",
            None,
            postgres_scope.clone(),
            postgres_log_position(100),
            "package-postgres-first",
        ),
    );
    let regression = delta(
        "checkpoint-postgres-regression",
        Some(&first.delta),
        postgres_scope,
        postgres_log_position(99),
        "package-postgres-regression",
    );
    assert!(
        store
            .propose(regression)
            .unwrap_err()
            .message
            .contains("reach")
    );
}

#[test]
fn checkpoint_stores_enforce_cdc_lineage_and_advance_mongo_tokens() {
    assert_cdc_checkpoint_lineage(&InMemoryCheckpointStore::new());
    assert_cdc_checkpoint_lineage(&SqliteCheckpointStore::open_in_memory().unwrap());
}

#[test]
fn commit_requires_receipt_covering_package_and_segments() {
    assert_store_rejects_bad_receipts(&InMemoryCheckpointStore::new());
    assert_store_rejects_bad_receipts(&SqliteCheckpointStore::open_in_memory().unwrap());
}

#[test]
fn sqlite_committed_package_hashes_reports_only_committed_history() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("state.db");
    let store = SqliteCheckpointStore::open(&db_path).unwrap();
    let committed = commit_delta(
        &store,
        delta(
            "checkpoint-gc-committed",
            None,
            partition_scope(),
            cursor_position(1),
            "package-gc-committed",
        ),
    );
    let proposed = delta(
        "checkpoint-gc-proposed",
        Some(&committed.delta),
        partition_scope(),
        cursor_position(2),
        "package-gc-proposed",
    );
    store.propose(proposed.clone()).unwrap();
    let abandoned = delta(
        "checkpoint-gc-abandoned",
        Some(&committed.delta),
        partition_scope(),
        cursor_position(3),
        "package-gc-abandoned",
    );
    store.propose(abandoned.clone()).unwrap();
    store.abandon(&abandoned.checkpoint_id).unwrap();

    let read_only = SqliteCheckpointStore::open_read_only(&db_path).unwrap();
    assert_eq!(
        read_only.committed_package_hashes().unwrap(),
        BTreeSet::from([PackageHash::new("package-gc-committed").unwrap()])
    );
    let error = read_only
        .propose(delta(
            "checkpoint-read-only-write",
            Some(&committed.delta),
            partition_scope(),
            cursor_position(4),
            "package-read-only-write",
        ))
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Internal);
    assert!(error.message.contains("read-only"));
}

#[test]
fn abandon_keeps_proposed_checkpoint_out_of_head() {
    assert_abandon_keeps_proposed_checkpoint_out_of_head(&InMemoryCheckpointStore::new());
    assert_abandon_keeps_proposed_checkpoint_out_of_head(
        &SqliteCheckpointStore::open_in_memory().unwrap(),
    );
}

#[test]
fn head_history_and_rewind_target_are_isolated_by_resource_and_scope() {
    assert_scope_and_resource_isolation(&InMemoryCheckpointStore::new());
    assert_scope_and_resource_isolation(&SqliteCheckpointStore::open_in_memory().unwrap());
}

#[test]
fn rewind_rejects_non_committed_wrong_tuple_and_missing_targets() {
    assert_rewind_validation(&InMemoryCheckpointStore::new());
    assert_rewind_validation(&SqliteCheckpointStore::open_in_memory().unwrap());
}

#[test]
fn rewind_rejects_committed_target_when_scope_has_no_head() {
    let in_memory = InMemoryCheckpointStore::new();
    let committed = commit_delta(
        &in_memory,
        delta(
            "checkpoint-no-head-memory",
            None,
            partition_scope(),
            cursor_position(1),
            "package-no-head-memory",
        ),
    );
    in_memory.clear_head_for_test(&committed.delta.checkpoint_id);
    assert!(
        in_memory
            .rewind(RewindRequest {
                marker_checkpoint_id: CheckpointId::new("rewind-no-head-memory").unwrap(),
                pipeline_id: committed.delta.pipeline_id.clone(),
                resource_id: committed.delta.resource_id.clone(),
                scope: committed.delta.scope.clone(),
                target_checkpoint_id: committed.delta.checkpoint_id,
            })
            .is_err()
    );

    let sqlite = SqliteCheckpointStore::open_in_memory().unwrap();
    let committed = commit_delta(
        &sqlite,
        delta(
            "checkpoint-no-head-sqlite",
            None,
            partition_scope(),
            cursor_position(1),
            "package-no-head-sqlite",
        ),
    );
    sqlite
        .execute_for_test(
            "UPDATE cdf_checkpoints SET is_head = 0 WHERE checkpoint_id = ?",
            params![committed.delta.checkpoint_id.as_str()],
        )
        .unwrap();
    assert!(
        sqlite
            .rewind(RewindRequest {
                marker_checkpoint_id: CheckpointId::new("rewind-no-head-sqlite").unwrap(),
                pipeline_id: committed.delta.pipeline_id.clone(),
                resource_id: committed.delta.resource_id.clone(),
                scope: committed.delta.scope.clone(),
                target_checkpoint_id: committed.delta.checkpoint_id,
            })
            .is_err()
    );
}

#[test]
fn sqlite_uses_wal_and_single_committed_head_index() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("state.db");
    let store = SqliteCheckpointStore::open(&db_path).unwrap();

    let journal_mode: String = store
        .query_row_for_test("PRAGMA journal_mode", [], |row| row.get(0))
        .unwrap();
    assert_eq!(journal_mode, "wal");

    let index_sql: String = store
        .query_row_for_test(
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'cdf_checkpoints_one_committed_head'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(index_sql.contains("WHERE is_head = 1 AND status = 'committed'"));
}

#[test]
fn sqlite_state_components_reject_unversioned_existing_tables() {
    let dir = tempdir().unwrap();

    let checkpoint_path = dir.path().join("checkpoint.db");
    rusqlite::Connection::open(&checkpoint_path)
        .unwrap()
        .execute_batch("CREATE TABLE cdf_checkpoints (id INTEGER);")
        .unwrap();
    let checkpoint_error = match SqliteCheckpointStore::open(&checkpoint_path) {
        Ok(_) => panic!("checkpoint store accepted an unversioned table"),
        Err(error) => error,
    };
    assert!(checkpoint_error.message.contains("unversioned"));

    let run_path = dir.path().join("run.db");
    rusqlite::Connection::open(&run_path)
        .unwrap()
        .execute_batch("CREATE TABLE cdf_runs (id INTEGER);")
        .unwrap();
    let run_error = match SqliteRunLedger::open(&run_path) {
        Ok(_) => panic!("run ledger accepted an unversioned table"),
        Err(error) => error,
    };
    assert!(run_error.message.contains("unversioned"));

    let lease_path = dir.path().join("lease.db");
    rusqlite::Connection::open(&lease_path)
        .unwrap()
        .execute_batch("CREATE TABLE cdf_scope_leases (id INTEGER);")
        .unwrap();
    let lease_error = match SqliteScopeLeaseStore::open(&lease_path) {
        Ok(_) => panic!("scope lease store accepted an unversioned table"),
        Err(error) => error,
    };
    assert!(lease_error.message.contains("unversioned"));
}

#[test]
fn sqlite_state_components_reject_incomplete_current_schema() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("state.db");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch(
        "
        CREATE TABLE cdf_sqlite_schema_versions (
            component TEXT PRIMARY KEY,
            version INTEGER NOT NULL,
            recorded_at_ms INTEGER NOT NULL
        );
        ",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO cdf_sqlite_schema_versions (component, version, recorded_at_ms) VALUES ('checkpoint_store', ?, 1)",
        [CHECKPOINT_STORE_SCHEMA_VERSION],
    )
    .unwrap();
    drop(conn);

    for error in [
        match SqliteCheckpointStore::open(&path) {
            Ok(_) => panic!("mutating open recreated an incomplete current schema"),
            Err(error) => error,
        },
        match SqliteCheckpointStore::open_read_only(&path) {
            Ok(_) => panic!("read-only open accepted an incomplete current schema"),
            Err(error) => error,
        },
    ] {
        assert!(
            error
                .message
                .contains("required table cdf_checkpoints is missing")
        );
    }
}

#[test]
fn scope_lease_stores_pass_shared_conformance() {
    assert_scope_lease_store_send_sync::<InMemoryScopeLeaseStore>();
    assert_scope_lease_store_send_sync::<SqliteScopeLeaseStore>();
    assert_scope_lease_store_conformance(|clock| InMemoryScopeLeaseStore::with_clock(clock));
    assert_scope_lease_store_conformance(|clock| {
        SqliteScopeLeaseStore::open_in_memory_with_clock(clock).unwrap()
    });
}

#[test]
fn sqlite_scope_lease_authority_domain_is_stable_per_store_and_distinct_between_stores() {
    let dir = tempdir().unwrap();
    let first_path = dir.path().join("first.db");
    let second_path = dir.path().join("second.db");

    let first = SqliteScopeLeaseStore::open(&first_path).unwrap();
    let first_domain = first.authority_domain_id();
    assert_eq!(
        SqliteScopeLeaseStore::open(&first_path)
            .unwrap()
            .authority_domain_id(),
        first_domain
    );
    assert_ne!(
        SqliteScopeLeaseStore::open(&second_path)
            .unwrap()
            .authority_domain_id(),
        first_domain
    );
}

#[test]
fn scope_lease_acquire_is_exclusive_under_concurrent_contention() {
    let clock = Arc::new(ManualScopeLeaseClock::new(10_000));
    assert_concurrent_lease_contention(Arc::new(InMemoryScopeLeaseStore::with_clock(clock)));

    let dir = tempdir().unwrap();
    let path = dir.path().join("leases.db");
    let clock = Arc::new(ManualScopeLeaseClock::new(10_000));
    let first = Arc::new(SqliteScopeLeaseStore::open_with_clock(&path, clock.clone()).unwrap());
    let second = Arc::new(SqliteScopeLeaseStore::open_with_clock(&path, clock).unwrap());
    let barrier = Arc::new(Barrier::new(2));
    let handles = [first, second].map(|store| {
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            barrier.wait();
            store.acquire(
                ScopeKey::SchemaContract {
                    contract: ContractRef::new("concurrent-sqlite").unwrap(),
                },
                LeaseOwnerId::new(format!("owner-{:?}", thread::current().id())).unwrap(),
                1_000,
            )
        })
    });
    let successes = handles
        .into_iter()
        .map(|handle| usize::from(handle.join().unwrap().is_ok()))
        .sum::<usize>();
    assert_eq!(successes, 1);
}

#[test]
fn sqlite_scope_lease_persists_fence_across_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("leases.db");
    let scope = ScopeKey::SchemaContract {
        contract: ContractRef::new("persistent-lease").unwrap(),
    };
    let clock = Arc::new(ManualScopeLeaseClock::new(20_000));
    let first = SqliteScopeLeaseStore::open_with_clock(&path, clock.clone()).unwrap();
    let lease = first
        .acquire(scope.clone(), LeaseOwnerId::new("owner-a").unwrap(), 100)
        .unwrap();
    drop(first);

    let reopened = SqliteScopeLeaseStore::open_with_clock(&path, clock.clone()).unwrap();
    clock.set(20_099);
    assert!(
        reopened
            .acquire(scope.clone(), LeaseOwnerId::new("owner-b").unwrap(), 100,)
            .is_err()
    );
    clock.set(20_100);
    let successor = reopened
        .acquire(scope, LeaseOwnerId::new("owner-b").unwrap(), 100)
        .unwrap();
    assert_eq!(successor.fencing_token.get(), lease.fencing_token.get() + 1);
    clock.set(20_101);
    assert!(reopened.assert_current(&lease).is_err());
}

#[test]
fn expiry_proof_never_treats_a_live_successor_as_abandoned() {
    let clock = Arc::new(ManualScopeLeaseClock::new(30_000));
    let store = InMemoryScopeLeaseStore::with_clock(clock.clone());
    let scope = ScopeKey::DestinationLoad {
        destination: DestinationId::new("parquet").unwrap(),
        target: TargetName::new("events").unwrap(),
    };
    let first = store
        .acquire(scope.clone(), LeaseOwnerId::new("owner-a").unwrap(), 100)
        .unwrap();
    let collector = LeaseOwnerId::new("collector").unwrap();
    assert_eq!(
        store.prove_expired(&first, collector.clone(), 100).unwrap(),
        None
    );

    clock.set(30_100);
    let successor = store
        .acquire(scope, LeaseOwnerId::new("owner-b").unwrap(), 100)
        .unwrap();
    assert_eq!(
        store.prove_expired(&first, collector.clone(), 100).unwrap(),
        None
    );

    store.release(&successor).unwrap();
    let proof = store
        .prove_expired(&first, collector, 100)
        .unwrap()
        .unwrap();
    assert_eq!(proof.expired_lease, first);
    assert_eq!(
        proof.cleanup_lease.fencing_token.get(),
        successor.fencing_token.get() + 1,
        "cleanup atomically fences every inactive generation"
    );
    assert!(
        store
            .acquire(
                successor.scope.clone(),
                LeaseOwnerId::new("owner-c").unwrap(),
                100,
            )
            .is_err(),
        "cleanup ownership blocks a new writer until deletion completes"
    );
    store.release(&proof.cleanup_lease).unwrap();
    let next = store
        .acquire(successor.scope, LeaseOwnerId::new("owner-c").unwrap(), 100)
        .unwrap();
    assert_eq!(
        next.fencing_token.get(),
        proof.cleanup_lease.fencing_token.get() + 1
    );
}

#[test]
fn sqlite_expiry_proof_claim_blocks_other_processes_until_cleanup_finishes() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("cleanup-claim.db");
    let clock = Arc::new(ManualScopeLeaseClock::new(40_000));
    let writer = SqliteScopeLeaseStore::open_with_clock(&path, clock.clone()).unwrap();
    let collector = SqliteScopeLeaseStore::open_with_clock(&path, clock).unwrap();
    let scope = ScopeKey::DestinationLoad {
        destination: DestinationId::new("parquet").unwrap(),
        target: TargetName::new("events").unwrap(),
    };
    let abandoned = writer
        .acquire(scope.clone(), LeaseOwnerId::new("writer").unwrap(), 100)
        .unwrap();
    writer.release(&abandoned).unwrap();

    let proof = collector
        .prove_expired(&abandoned, LeaseOwnerId::new("collector").unwrap(), 100)
        .unwrap()
        .unwrap();
    assert!(
        writer
            .acquire(scope.clone(), LeaseOwnerId::new("new-writer").unwrap(), 100,)
            .is_err(),
        "the transactional cleanup claim is visible to independent connections"
    );
    collector.release(&proof.cleanup_lease).unwrap();
    let successor = writer
        .acquire(scope, LeaseOwnerId::new("new-writer").unwrap(), 100)
        .unwrap();
    assert_eq!(
        successor.fencing_token.get(),
        proof.cleanup_lease.fencing_token.get() + 1
    );
}

fn assert_concurrent_lease_contention<S>(store: Arc<S>)
where
    S: ScopeLeaseStore + 'static,
{
    let barrier = Arc::new(Barrier::new(2));
    let handles = ["owner-a", "owner-b"].map(|owner| {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            barrier.wait();
            store.acquire(
                ScopeKey::SchemaContract {
                    contract: ContractRef::new("concurrent-memory").unwrap(),
                },
                LeaseOwnerId::new(owner).unwrap(),
                1_000,
            )
        })
    });
    let successes = handles
        .into_iter()
        .map(|handle| usize::from(handle.join().unwrap().is_ok()))
        .sum::<usize>();
    assert_eq!(successes, 1);
}

#[test]
fn sqlite_head_move_remains_transactionally_unique_across_connections() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("state.db");
    let first = SqliteCheckpointStore::open(&db_path).unwrap();
    let second = SqliteCheckpointStore::open(&db_path).unwrap();
    let scope = partition_scope();
    let first_delta = delta(
        "checkpoint-1",
        None,
        scope.clone(),
        cursor_position(1),
        "package-1",
    );
    first.propose(first_delta.clone()).unwrap();
    first
        .commit(&first_delta.checkpoint_id, receipt(&first_delta))
        .unwrap();
    let first_successor = delta(
        "checkpoint-successor-1",
        Some(&first_delta),
        scope.clone(),
        cursor_position(2),
        "package-successor-1",
    );
    let second_successor = delta(
        "checkpoint-successor-2",
        Some(&first_delta),
        scope.clone(),
        cursor_position(3),
        "package-successor-2",
    );
    first.propose(first_successor.clone()).unwrap();
    second.propose(second_successor.clone()).unwrap();
    second
        .commit(&second_successor.checkpoint_id, receipt(&second_successor))
        .unwrap();
    let error = first
        .commit(&first_successor.checkpoint_id, receipt(&first_successor))
        .unwrap_err();
    assert!(error.message.contains("current head"), "{error}");

    let head_count: i64 = second
        .query_row_for_test(
                "SELECT COUNT(*) FROM cdf_checkpoints WHERE pipeline_id = ? AND resource_id = ? AND scope_json = ? AND status = 'committed' AND is_head = 1",
                params![
                    pipeline_id().as_str(),
                    resource_id().as_str(),
                    encode_json(&scope).unwrap(),
                    ],
                    |row| row.get(0),
                )
                .unwrap();
    assert_eq!(head_count, 1);
    assert_eq!(
        second
            .head(&pipeline_id(), &resource_id(), &scope)
            .unwrap()
            .unwrap()
            .delta
            .checkpoint_id,
        second_successor.checkpoint_id
    );
}

#[test]
fn sqlite_run_ledger_mints_ids_and_rejects_supplied_collisions() {
    let ledger = SqliteRunLedger::open_in_memory().unwrap();

    let minted = ledger.create_run(None).unwrap();
    assert!(minted.run_id.as_str().starts_with("run-"));
    assert!(ledger.run(&minted.run_id).unwrap().is_some());

    let supplied_id = RunId::new("run-supplied").unwrap();
    let supplied = ledger.create_run(Some(supplied_id.clone())).unwrap();
    assert_eq!(supplied.run_id, supplied_id);

    let collision = ledger.create_run(Some(supplied_id)).unwrap_err();
    assert!(
        collision.to_string().contains("already exists"),
        "caller-supplied run id collisions fail closed"
    );
}

#[test]
fn sqlite_run_events_are_per_run_monotonic_and_query_in_sequence_order() {
    let ledger = SqliteRunLedger::open_in_memory().unwrap();
    let first = ledger
        .create_run(Some(RunId::new("run-first").unwrap()))
        .unwrap();
    let second = ledger
        .create_run(Some(RunId::new("run-second").unwrap()))
        .unwrap();

    let first_started = ledger
        .append_event(&first.run_id, RunEventAppend::new(RunEventKind::RunStarted))
        .unwrap();
    let second_started = ledger
        .append_event(
            &second.run_id,
            RunEventAppend::new(RunEventKind::RunStarted),
        )
        .unwrap();
    let first_plan = ledger
        .append_event(
            &first.run_id,
            RunEventAppend::new(RunEventKind::PlanRecorded),
        )
        .unwrap();
    let first_success = ledger
        .append_event(
            &first.run_id,
            RunEventAppend::new(RunEventKind::RunSucceeded),
        )
        .unwrap();

    assert_eq!(first_started.sequence, 1);
    assert_eq!(second_started.sequence, 1);
    assert_eq!(first_plan.sequence, 2);
    assert_eq!(first_success.sequence, 3);
    assert!(first_started.timestamp_ms > 1_600_000_000_000);

    let events = ledger.events(&first.run_id).unwrap();
    assert_eq!(
        events
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(
        events.iter().map(|event| event.kind).collect::<Vec<_>>(),
        vec![
            RunEventKind::RunStarted,
            RunEventKind::PlanRecorded,
            RunEventKind::RunSucceeded,
        ]
    );
}

#[test]
fn sqlite_run_ledger_records_are_append_only_below_the_rust_api() {
    let ledger = SqliteRunLedger::open_in_memory().unwrap();
    let run = ledger
        .create_run(Some(RunId::new("run-append-only").unwrap()))
        .unwrap();

    let run_update = ledger.execute_for_test(
        "UPDATE cdf_runs SET created_at_ms = created_at_ms + 1 WHERE run_id = ?",
        params![run.run_id.as_str()],
    );
    assert!(run_update.unwrap_err().to_string().contains("append-only"));

    let run_delete = ledger.execute_for_test(
        "DELETE FROM cdf_runs WHERE run_id = ?",
        params![run.run_id.as_str()],
    );
    assert!(run_delete.unwrap_err().to_string().contains("append-only"));

    ledger
        .append_event(&run.run_id, RunEventAppend::new(RunEventKind::RunStarted))
        .unwrap();

    let update = ledger.execute_for_test(
        "UPDATE cdf_run_events SET kind = 'run_failed' WHERE run_id = ?",
        params![run.run_id.as_str()],
    );
    assert!(update.unwrap_err().to_string().contains("append-only"));

    let delete = ledger.execute_for_test(
        "DELETE FROM cdf_run_events WHERE run_id = ?",
        params![run.run_id.as_str()],
    );
    assert!(delete.unwrap_err().to_string().contains("append-only"));

    let events = ledger.events(&run.run_id).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, RunEventKind::RunStarted);
}

#[test]
fn sqlite_run_ledger_rejects_private_timestamp_corruption_and_sequence_overflow() {
    let temp = tempdir().unwrap();
    let run_path = temp.path().join(".cdf").join("negative-run.db");
    std::fs::create_dir_all(run_path.parent().unwrap()).unwrap();
    let ledger = SqliteRunLedger::open(&run_path).unwrap();
    let run = ledger
        .create_run(Some(RunId::new("negative-run").unwrap()))
        .unwrap();
    drop(ledger);
    let connection = Connection::open(&run_path).unwrap();
    connection
        .execute_batch("DROP TRIGGER cdf_runs_no_update")
        .unwrap();
    connection
        .execute(
            "UPDATE cdf_runs SET created_at_ms = -1 WHERE run_id = ?",
            params![run.run_id.as_str()],
        )
        .unwrap();
    drop(connection);
    let ledger = SqliteRunLedger::open(&run_path).unwrap();
    let error = ledger
        .validate_integrity()
        .expect_err("negative private run timestamp must fail integrity validation");
    assert_eq!(error.kind, ErrorKind::Internal);
    assert!(error.message.contains("creation timestamp"));

    let event_path = temp.path().join(".cdf").join("negative-event.db");
    let ledger = SqliteRunLedger::open(&event_path).unwrap();
    let run = ledger
        .create_run(Some(RunId::new("negative-event").unwrap()))
        .unwrap();
    ledger
        .append_event(&run.run_id, RunEventAppend::new(RunEventKind::RunStarted))
        .unwrap();
    drop(ledger);
    let connection = Connection::open(&event_path).unwrap();
    connection
        .execute_batch("DROP TRIGGER cdf_run_events_no_update")
        .unwrap();
    connection
        .execute(
            "UPDATE cdf_run_events SET timestamp_ms = -1 WHERE run_id = ?",
            params![run.run_id.as_str()],
        )
        .unwrap();
    drop(connection);
    let ledger = SqliteRunLedger::open(&event_path).unwrap();
    let error = ledger
        .validate_integrity()
        .expect_err("negative private event timestamp must fail integrity validation");
    assert_eq!(error.kind, ErrorKind::Internal);
    assert!(error.message.contains("event timestamp"));

    let overflow_path = temp.path().join(".cdf").join("overflow-event.db");
    let ledger = SqliteRunLedger::open(&overflow_path).unwrap();
    let run = ledger
        .create_run(Some(RunId::new("overflow-event").unwrap()))
        .unwrap();
    ledger
        .append_event(&run.run_id, RunEventAppend::new(RunEventKind::RunStarted))
        .unwrap();
    ledger
        .execute_for_test("DROP TRIGGER cdf_run_events_no_update", [])
        .unwrap();
    ledger
        .execute_for_test(
            "UPDATE cdf_run_events SET sequence = ? WHERE run_id = ?",
            params![i64::MAX, run.run_id.as_str()],
        )
        .unwrap();
    let error = ledger
        .append_event(&run.run_id, RunEventAppend::new(RunEventKind::RunSucceeded))
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Internal);
    assert!(error.message.contains("sequence overflow"));
}

#[test]
fn sqlite_run_ledger_serializes_required_event_families_with_secret_refs_only() {
    let ledger = SqliteRunLedger::open_in_memory().unwrap();
    let run = ledger
        .create_run(Some(RunId::new("run-event-families").unwrap()))
        .unwrap();
    let secret_ref = SecretReference::new("secret://env/API_TOKEN").unwrap();

    for kind in RunEventKind::ALL {
        let mut append = RunEventAppend::new(kind);
        append.details =
            RunEventDetails::new([("api_token", RunEventValue::SecretRef(secret_ref.clone()))]);
        let event = ledger.append_event(&run.run_id, append).unwrap();
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("secret://env/API_TOKEN"));
        assert!(!json.contains("super-secret-token"));
    }

    let events = ledger.events(&run.run_id).unwrap();
    assert_eq!(events.len(), RunEventKind::ALL.len());
    assert_eq!(
        events.iter().map(|event| event.kind).collect::<Vec<_>>(),
        RunEventKind::ALL
    );
}

#[test]
fn sqlite_run_ledger_rejects_secret_values_and_untyped_secret_references() {
    let ledger = SqliteRunLedger::open_in_memory().unwrap();
    let run = ledger
        .create_run(Some(RunId::new("run-secret-rejection").unwrap()))
        .unwrap();

    let mut raw_secret = RunEventAppend::new(RunEventKind::RunStarted);
    raw_secret.details = RunEventDetails::new([(
        "token",
        RunEventValue::String("super-secret-token".to_owned()),
    )]);
    assert!(ledger.append_event(&run.run_id, raw_secret).is_err());

    let mut untyped_ref = RunEventAppend::new(RunEventKind::RunStarted);
    untyped_ref.details = RunEventDetails::new([(
        "note",
        RunEventValue::String("secret://env/API_TOKEN".to_owned()),
    )]);
    assert!(ledger.append_event(&run.run_id, untyped_ref).is_err());

    let mut typed_ref = RunEventAppend::new(RunEventKind::RunStarted);
    typed_ref.details = RunEventDetails::new([(
        "token",
        RunEventValue::SecretRef(SecretReference::new("secret://env/API_TOKEN").unwrap()),
    )]);
    ledger.append_event(&run.run_id, typed_ref).unwrap();
}

#[test]
fn sqlite_run_snapshot_carries_inspect_and_resume_pointers_without_source_contact() {
    let ledger = SqliteRunLedger::open_in_memory().unwrap();
    let run = ledger
        .create_run(Some(RunId::new("run-query-shape").unwrap()))
        .unwrap();
    let scope = partition_scope();
    let mut plan = RunEventAppend::new(RunEventKind::PlanRecorded);
    plan.resource_id = Some(resource_id());
    plan.scope = Some(scope.clone());
    plan.partition_id = Some(PartitionId::new("p0").unwrap());
    plan.plan_id = Some(PlanId::new("plan-1").unwrap());
    plan.details = RunEventDetails::new([("planned_packages", RunEventValue::U64(1))]);
    ledger.append_event(&run.run_id, plan).unwrap();

    let mut package = RunEventAppend::new(RunEventKind::PackageFinalized);
    package.package_id = Some("pkg-1".to_owned());
    package.package_hash = Some(PackageHash::new("package-query-shape").unwrap());
    package.package_path = Some("packages/pkg-1".to_owned());
    ledger.append_event(&run.run_id, package).unwrap();

    let mut receipt_event = RunEventAppend::new(RunEventKind::DestinationReceiptRecorded);
    receipt_event.destination_id = Some(DestinationId::new("duckdb-local").unwrap());
    receipt_event.receipt_id = Some(ReceiptId::new("receipt-query-shape").unwrap());
    ledger.append_event(&run.run_id, receipt_event).unwrap();

    let mut checkpoint_event = RunEventAppend::new(RunEventKind::CheckpointCommitted);
    checkpoint_event.checkpoint_id = Some(CheckpointId::new("checkpoint-query-shape").unwrap());
    ledger.append_event(&run.run_id, checkpoint_event).unwrap();

    let snapshot = ledger.snapshot(&run.run_id).unwrap().unwrap();
    assert_eq!(snapshot.run.run_id, run.run_id);
    assert_eq!(snapshot.events.len(), 4);
    assert_eq!(snapshot.events[0].resource_id, Some(resource_id()));
    assert_eq!(snapshot.events[0].scope, Some(scope));
    assert_eq!(
        snapshot.events[1].package_hash,
        Some(PackageHash::new("package-query-shape").unwrap())
    );
    assert_eq!(
        snapshot.events[2].receipt_id,
        Some(ReceiptId::new("receipt-query-shape").unwrap())
    );
    assert_eq!(
        snapshot.events[3].checkpoint_id,
        Some(CheckpointId::new("checkpoint-query-shape").unwrap())
    );
    assert!(
        ledger
            .snapshot(&RunId::new("missing-run").unwrap())
            .unwrap()
            .is_none()
    );
}

#[test]
fn sqlite_run_ledger_open_read_only_reads_without_initializing_missing_database() {
    let dir = tempdir().unwrap();
    let missing_path = dir.path().join("missing-state.db");
    let error = match SqliteRunLedger::open_read_only(&missing_path) {
        Ok(_) => panic!("read-only open unexpectedly created missing run ledger"),
        Err(error) => error,
    };
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(
        !missing_path.exists(),
        "read-only open must not create a missing database"
    );

    let db_path = dir.path().join("state.db");
    let ledger = SqliteRunLedger::open(&db_path).unwrap();
    let run = ledger
        .create_run(Some(RunId::new("run-read-only").unwrap()))
        .unwrap();
    let mut event = RunEventAppend::new(RunEventKind::RunStarted);
    event.resource_id = Some(resource_id());
    ledger.append_event(&run.run_id, event).unwrap();
    drop(ledger);

    let read_only = SqliteRunLedger::open_read_only(&db_path).unwrap();
    let snapshot = read_only.snapshot(&run.run_id).unwrap().unwrap();
    assert_eq!(snapshot.run.run_id, run.run_id);
    assert_eq!(snapshot.events.len(), 1);
    assert_eq!(snapshot.events[0].kind, RunEventKind::RunStarted);
    let error = read_only
        .create_run(Some(RunId::new("run-write").unwrap()))
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Internal);
    assert!(error.message.contains("read-only"));
}

#[test]
fn sqlite_run_ledger_open_read_only_rejects_unsupported_schema_version() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("state.db");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch(
        "
        CREATE TABLE cdf_sqlite_schema_versions (
            component TEXT PRIMARY KEY,
            version INTEGER NOT NULL,
            recorded_at_ms INTEGER NOT NULL
        );
        INSERT INTO cdf_sqlite_schema_versions (component, version, recorded_at_ms)
        VALUES ('run_ledger', 6, 1);
        ",
    )
    .unwrap();
    drop(conn);

    for error in [
        match SqliteRunLedger::open_read_only(&db_path) {
            Ok(_) => panic!("read-only open accepted unsupported run ledger schema version"),
            Err(error) => error,
        },
        match SqliteRunLedger::open(&db_path) {
            Ok(_) => panic!("mutating open accepted unsupported run ledger schema version"),
            Err(error) => error,
        },
    ] {
        assert_eq!(error.kind, cdf_kernel::ErrorKind::Internal);
        assert!(
            error
                .message
                .contains("unsupported run ledger SQLite schema version 6")
        );
    }
}

#[test]
fn sqlite_run_ledger_checkpoint_events_do_not_advance_checkpoint_store() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("state.db");
    let ledger = SqliteRunLedger::open(&db_path).unwrap();
    let checkpoint_store = SqliteCheckpointStore::open(&db_path).unwrap();
    let run = ledger
        .create_run(Some(RunId::new("run-checkpoint-isolation").unwrap()))
        .unwrap();
    let delta = delta(
        "checkpoint-ledger-only",
        None,
        partition_scope(),
        cursor_position(1),
        "package-ledger-only",
    );

    let mut event = RunEventAppend::new(RunEventKind::CheckpointCommitted);
    event.checkpoint_id = Some(delta.checkpoint_id.clone());
    ledger.append_event(&run.run_id, event).unwrap();
    let mut transition = RunEventAppend::new(RunEventKind::ValidationDepthTransitionRecorded);
    transition.resource_id = Some(delta.resource_id.clone());
    transition.scope = Some(delta.scope.clone());
    transition.package_hash = Some(delta.package_hash.clone());
    transition.details = RunEventDetails::new([
        (
            "from_depth",
            RunEventValue::String("sampled_fast_path".to_owned()),
        ),
        ("to_depth", RunEventValue::String("full".to_owned())),
        (
            "trigger",
            RunEventValue::String("quarantine_event".to_owned()),
        ),
        (
            "schema_hash",
            RunEventValue::String(delta.schema_hash.as_str().to_owned()),
        ),
    ]);
    ledger.append_event(&run.run_id, transition).unwrap();

    assert!(
        checkpoint_store
            .head(&delta.pipeline_id, &delta.resource_id, &delta.scope)
            .unwrap()
            .is_none(),
        "run ledger events are not checkpoint-state authority"
    );
    assert!(
        checkpoint_store
            .history(&delta.pipeline_id, &delta.resource_id, &delta.scope)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn sqlite_run_ledger_records_schema_version() {
    let ledger = SqliteRunLedger::open_in_memory().unwrap();
    let version: i64 = ledger
        .query_row_for_test(
            "SELECT version FROM cdf_sqlite_schema_versions WHERE component = 'run_ledger'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, 1);
}

#[test]
fn rewind_appends_marker_and_reports_packages_ahead() {
    assert_rewind_appends_marker_and_reports_packages_ahead(&InMemoryCheckpointStore::new());
    assert_rewind_appends_marker_and_reports_packages_ahead(
        &SqliteCheckpointStore::open_in_memory().unwrap(),
    );
}

#[test]
fn branch_rewind_reports_only_current_branch_packages_ahead() {
    assert_branch_rewind_reports_only_current_branch_ahead(&InMemoryCheckpointStore::new());
    assert_branch_rewind_reports_only_current_branch_ahead(
        &SqliteCheckpointStore::open_in_memory().unwrap(),
    );
}

fn assert_sqlite_row_corruption_is_rejected<F>(checkpoint_id: &str, mutate: F)
where
    F: FnOnce(&SqliteCheckpointStore, &StateDelta) -> (PipelineId, ResourceId, ScopeKey),
{
    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    let committed = commit_delta(
        &store,
        delta(
            checkpoint_id,
            None,
            partition_scope(),
            cursor_position(1),
            &format!("package-{checkpoint_id}"),
        ),
    );
    let (pipeline_id, resource_id, scope) = mutate(&store, &committed.delta);
    let error = store
        .head(&pipeline_id, &resource_id, &scope)
        .expect_err("corrupt scalar checkpoint row should be rejected during read");
    assert_eq!(error.kind, ErrorKind::Internal);
}

#[test]
fn sqlite_rejects_rows_when_scalar_columns_disagree_with_delta_json() {
    assert_sqlite_row_corruption_is_rejected("checkpoint-corrupt-id", |store, delta| {
        store
            .execute_for_test(
                "UPDATE cdf_checkpoints SET checkpoint_id = ? WHERE checkpoint_id = ?",
                params!["", delta.checkpoint_id.as_str()],
            )
            .unwrap();
        (
            delta.pipeline_id.clone(),
            delta.resource_id.clone(),
            delta.scope.clone(),
        )
    });
    assert_sqlite_row_corruption_is_rejected("checkpoint-corrupt-pipeline", |store, delta| {
        let corrupt_pipeline = PipelineId::new("pipeline-corrupt").unwrap();
        store
            .execute_for_test(
                "UPDATE cdf_checkpoints SET pipeline_id = ? WHERE checkpoint_id = ?",
                params![corrupt_pipeline.as_str(), delta.checkpoint_id.as_str()],
            )
            .unwrap();
        (
            corrupt_pipeline,
            delta.resource_id.clone(),
            delta.scope.clone(),
        )
    });
    assert_sqlite_row_corruption_is_rejected("checkpoint-corrupt-resource", |store, delta| {
        let corrupt_resource = ResourceId::new("resource-corrupt").unwrap();
        store
            .execute_for_test(
                "UPDATE cdf_checkpoints SET resource_id = ? WHERE checkpoint_id = ?",
                params![corrupt_resource.as_str(), delta.checkpoint_id.as_str()],
            )
            .unwrap();
        (
            delta.pipeline_id.clone(),
            corrupt_resource,
            delta.scope.clone(),
        )
    });
    assert_sqlite_row_corruption_is_rejected("checkpoint-corrupt-scope", |store, delta| {
        let corrupt_scope = other_partition_scope();
        store
            .execute_for_test(
                "UPDATE cdf_checkpoints SET scope_json = ? WHERE checkpoint_id = ?",
                params![
                    encode_json(&corrupt_scope).unwrap(),
                    delta.checkpoint_id.as_str()
                ],
            )
            .unwrap();
        (
            delta.pipeline_id.clone(),
            delta.resource_id.clone(),
            corrupt_scope,
        )
    });
    assert_sqlite_row_corruption_is_rejected("checkpoint-corrupt-state-version", |store, delta| {
        let mut corrupt_delta = delta.clone();
        corrupt_delta.state_version = CHECKPOINT_STATE_VERSION + 1;
        store
            .execute_for_test(
                "UPDATE cdf_checkpoints SET delta_json = ? WHERE checkpoint_id = ?",
                params![
                    encode_json(&corrupt_delta).unwrap(),
                    delta.checkpoint_id.as_str()
                ],
            )
            .unwrap();
        (
            delta.pipeline_id.clone(),
            delta.resource_id.clone(),
            delta.scope.clone(),
        )
    });
    assert_sqlite_row_corruption_is_rejected("checkpoint-corrupt-parent", |store, delta| {
        store
            .execute_for_test(
                "UPDATE cdf_checkpoints SET parent_checkpoint_id = ? WHERE checkpoint_id = ?",
                params!["checkpoint-other-parent", delta.checkpoint_id.as_str()],
            )
            .unwrap();
        (
            delta.pipeline_id.clone(),
            delta.resource_id.clone(),
            delta.scope.clone(),
        )
    });
    assert_sqlite_row_corruption_is_rejected("checkpoint-corrupt-input", |store, delta| {
        store
            .execute_for_test(
                "UPDATE cdf_checkpoints SET input_position_json = ? WHERE checkpoint_id = ?",
                params![
                    encode_json(&cursor_position(99)).unwrap(),
                    delta.checkpoint_id.as_str()
                ],
            )
            .unwrap();
        (
            delta.pipeline_id.clone(),
            delta.resource_id.clone(),
            delta.scope.clone(),
        )
    });
    assert_sqlite_row_corruption_is_rejected("checkpoint-corrupt-output", |store, delta| {
        store
            .execute_for_test(
                "UPDATE cdf_checkpoints SET output_position_json = ? WHERE checkpoint_id = ?",
                params![
                    encode_json(&cursor_position(99)).unwrap(),
                    delta.checkpoint_id.as_str()
                ],
            )
            .unwrap();
        (
            delta.pipeline_id.clone(),
            delta.resource_id.clone(),
            delta.scope.clone(),
        )
    });
    assert_sqlite_row_corruption_is_rejected("checkpoint-corrupt-package", |store, delta| {
        store
            .execute_for_test(
                "UPDATE cdf_checkpoints SET package_hash = ? WHERE checkpoint_id = ?",
                params!["package-corrupt", delta.checkpoint_id.as_str()],
            )
            .unwrap();
        (
            delta.pipeline_id.clone(),
            delta.resource_id.clone(),
            delta.scope.clone(),
        )
    });
    assert_sqlite_row_corruption_is_rejected("checkpoint-corrupt-schema", |store, delta| {
        store
            .execute_for_test(
                "UPDATE cdf_checkpoints SET schema_hash = ? WHERE checkpoint_id = ?",
                params!["schema-corrupt", delta.checkpoint_id.as_str()],
            )
            .unwrap();
        (
            delta.pipeline_id.clone(),
            delta.resource_id.clone(),
            delta.scope.clone(),
        )
    });
}

#[test]
fn sqlite_checkpoint_integrity_rejects_private_sequence_and_head_lifecycle_corruption() {
    let temp = tempdir().unwrap();
    let sequence_path = temp.path().join(".cdf").join("sequence.db");
    std::fs::create_dir_all(sequence_path.parent().unwrap()).unwrap();
    let store = SqliteCheckpointStore::open(&sequence_path).unwrap();
    let proposed = store
        .propose(delta(
            "checkpoint-corrupt-sequence",
            None,
            partition_scope(),
            cursor_position(1),
            "package-corrupt-sequence",
        ))
        .unwrap();
    drop(store);
    let connection = Connection::open(&sequence_path).unwrap();
    connection
        .execute_batch("PRAGMA ignore_check_constraints = ON")
        .unwrap();
    connection
        .execute(
            "UPDATE cdf_checkpoints SET sequence = -1 WHERE checkpoint_id = ?",
            params![proposed.delta.checkpoint_id.as_str()],
        )
        .unwrap();
    drop(connection);
    let store = SqliteCheckpointStore::open(&sequence_path).unwrap();
    let error = store
        .validate_integrity()
        .expect_err("nonpositive private checkpoint sequence must fail integrity validation");
    assert_eq!(error.kind, ErrorKind::Internal);
    assert!(error.message.contains("sequence"));

    let head_path = temp.path().join(".cdf").join("head.db");
    let store = SqliteCheckpointStore::open(&head_path).unwrap();
    let proposed = store
        .propose(delta(
            "checkpoint-corrupt-head",
            None,
            partition_scope(),
            cursor_position(1),
            "package-corrupt-head",
        ))
        .unwrap();
    drop(store);
    let connection = Connection::open(&head_path).unwrap();
    connection
        .execute_batch("PRAGMA ignore_check_constraints = ON")
        .unwrap();
    connection
        .execute(
            "UPDATE cdf_checkpoints SET is_head = 1 WHERE checkpoint_id = ?",
            params![proposed.delta.checkpoint_id.as_str()],
        )
        .unwrap();
    drop(connection);
    let store = SqliteCheckpointStore::open(&head_path).unwrap();
    let error = store
        .validate_integrity()
        .expect_err("non-committed private checkpoint head must fail integrity validation");
    assert_eq!(error.kind, ErrorKind::Internal);
    assert!(error.message.contains("current head"));
}

#[test]
fn sqlite_rejects_committed_rows_when_receipt_id_disagrees_with_receipt_json() {
    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    let committed = commit_delta(
        &store,
        delta(
            "checkpoint-corrupt-receipt-id",
            None,
            partition_scope(),
            cursor_position(1),
            "package-corrupt-receipt-id",
        ),
    );
    store
        .execute_for_test(
            "UPDATE cdf_checkpoints SET receipt_id = ? WHERE checkpoint_id = ?",
            params!["receipt-other", committed.delta.checkpoint_id.as_str()],
        )
        .unwrap();

    assert!(
        store
            .head(
                &committed.delta.pipeline_id,
                &committed.delta.resource_id,
                &committed.delta.scope,
            )
            .is_err()
    );
}

#[test]
fn sqlite_round_trips_position_scope_and_state_json() {
    let mut composite_parts = BTreeMap::new();
    composite_parts.insert("cursor".to_owned(), cursor_position(1));
    composite_parts.insert("log".to_owned(), postgres_log_position(7));

    let positions = vec![
        cursor_position(1),
        postgres_log_position(42),
        SourcePosition::FileManifest(FileManifest {
            version: SOURCE_POSITION_VERSION,
            files: vec![FilePosition {
                path: "orders-1.jsonl".to_owned(),
                size_bytes: 1024,
                source_generation: None,
                etag: Some("etag-1".to_owned()),
                object_version: None,
                sha256: None,
            }],
        }),
        table_snapshot_position(),
        SourcePosition::PageToken(PageToken {
            version: SOURCE_POSITION_VERSION,
            token: "next-page".to_owned(),
        }),
        SourcePosition::Composite(CompositePosition {
            version: SOURCE_POSITION_VERSION,
            positions: composite_parts,
        }),
        SourcePosition::ForeignState(ForeignState {
            version: SOURCE_POSITION_VERSION,
            protocol: "singer".to_owned(),
            opaque_blob: b"{\"bookmarks\":{}}".to_vec(),
            blob_sha256: "sha256:d2c47dce50d89aa04b6e25293cb52db74657d5ec68ac614dd030a4a6595a7cd7"
                .to_owned(),
        }),
    ];
    let scopes = vec![
        ScopeKey::Resource,
        partition_scope(),
        ScopeKey::Window {
            start: "2026-07-01T00:00:00Z".to_owned(),
            end: "2026-07-02T00:00:00Z".to_owned(),
        },
        ScopeKey::File {
            path: "orders-1.jsonl".to_owned(),
        },
        ScopeKey::Stream {
            name: "orders".to_owned(),
        },
        ScopeKey::SchemaContract {
            contract: ContractRef::new("orders-contract").unwrap(),
        },
        ScopeKey::DestinationLoad {
            destination: DestinationId::new("duckdb-local").unwrap(),
            target: TargetName::new("orders").unwrap(),
        },
        ScopeKey::Composite {
            parts: vec![
                partition_scope(),
                ScopeKey::Stream {
                    name: "orders".to_owned(),
                },
            ],
        },
    ];

    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    for (index, position) in positions.into_iter().enumerate() {
        let scope = scopes[index].clone();
        let delta = delta(
            &format!("checkpoint-roundtrip-{index}"),
            None,
            scope.clone(),
            position,
            &format!("package-roundtrip-{index}"),
        );
        let checkpoint = commit_delta(&store, delta.clone());
        let head = store
            .head(&delta.pipeline_id, &delta.resource_id, &scope)
            .unwrap()
            .unwrap();
        assert_eq!(head.delta, checkpoint.delta);
        assert_eq!(head.delta.scope, scope);
        assert_eq!(head.delta.state_version, CHECKPOINT_STATE_VERSION);
    }

    for (index, scope) in scopes.into_iter().enumerate().skip(positions_count()) {
        let delta = delta(
            &format!("checkpoint-scope-roundtrip-{index}"),
            None,
            scope.clone(),
            cursor_position(index as i64),
            &format!("package-scope-roundtrip-{index}"),
        );
        commit_delta(&store, delta.clone());
        assert_eq!(
            store
                .head(&delta.pipeline_id, &delta.resource_id, &scope)
                .unwrap()
                .unwrap()
                .delta
                .scope,
            scope
        );
    }

    let mut unsupported = delta(
        "checkpoint-unsupported-state",
        None,
        partition_scope(),
        cursor_position(99),
        "package-unsupported-state",
    );
    unsupported.state_version = CHECKPOINT_STATE_VERSION + 1;
    assert!(store.propose(unsupported).is_err());
}

#[test]
fn checkpoint_stores_reject_semantically_tampered_table_snapshot_positions() {
    let mut invalid = table_snapshot_position();
    let SourcePosition::TableSnapshot(position) = &mut invalid else {
        unreachable!();
    };
    position.snapshot_id = 0;
    let invalid_delta = delta(
        "checkpoint-invalid-table-snapshot",
        None,
        partition_scope(),
        invalid,
        "package-invalid-table-snapshot",
    );
    let memory = InMemoryCheckpointStore::new();
    assert!(
        memory
            .propose(invalid_delta)
            .unwrap_err()
            .to_string()
            .contains("snapshot id must be positive")
    );

    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    let committed = commit_delta(
        &store,
        delta(
            "checkpoint-tampered-table-snapshot",
            None,
            partition_scope(),
            table_snapshot_position(),
            "package-tampered-table-snapshot",
        ),
    );
    let mut tampered_delta = committed.delta.clone();
    let SourcePosition::TableSnapshot(position) = &mut tampered_delta.output_position else {
        unreachable!();
    };
    position.metadata_generation.clear();
    tampered_delta.segments[0].output_position = tampered_delta.output_position.clone();
    store
        .execute_for_test(
            "UPDATE cdf_checkpoints SET output_position_json = ?, delta_json = ? WHERE checkpoint_id = ?",
            params![
                encode_json(&tampered_delta.output_position).unwrap(),
                encode_json(&tampered_delta).unwrap(),
                committed.delta.checkpoint_id.as_str(),
            ],
        )
        .unwrap();
    let error = store
        .head(
            &committed.delta.pipeline_id,
            &committed.delta.resource_id,
            &committed.delta.scope,
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("metadata generation must be nonempty"),
        "{error}"
    );
}

#[test]
fn in_memory_rejects_unsupported_state_version_without_sqlite_constraints() {
    let store = InMemoryCheckpointStore::new();
    let mut unsupported = delta(
        "checkpoint-memory-unsupported-state",
        None,
        partition_scope(),
        cursor_position(99),
        "package-memory-unsupported-state",
    );
    unsupported.state_version = CHECKPOINT_STATE_VERSION + 1;
    assert!(store.propose(unsupported).is_err());
    assert!(
        store
            .history(&pipeline_id(), &resource_id(), &partition_scope())
            .unwrap()
            .is_empty()
    );
}

fn positions_count() -> usize {
    7
}

#[test]
fn sqlite_schema_authority_passes_shared_conformance() {
    assert_schema_authority_store_send_sync::<SqliteSchemaAuthorityStore>();
    let mut directories = Vec::new();
    assert_schema_authority_store_conformance(|| {
        directories.push(tempdir().unwrap());
        let path = directories.last().unwrap().path().join("schema-state.db");
        let store = SqliteSchemaAuthorityStore::open(&path).unwrap();
        let leases = SqliteScopeLeaseStore::open(&path).unwrap();
        (store, leases)
    });
}

#[test]
fn sqlite_schema_authority_failure_rolls_back_complete_batch() {
    let store = SqliteSchemaAuthorityStore::open_in_memory().unwrap();
    let first = first_use_schema_authority_establishment(&store, "dev", "orders", "order_id");
    let second =
        first_use_schema_authority_establishment(&store, "dev", "customers", "customer_id");

    let error = store
        .establish_batch_with_failure_for_test(vec![first.clone(), second.clone()])
        .unwrap_err();

    assert_eq!(error.kind, ErrorKind::Internal);
    assert!(store.head(&first.key).unwrap().is_none());
    assert!(store.head(&second.key).unwrap().is_none());
    for table in [
        "cdf_schema_versions",
        "cdf_schema_heads",
        "cdf_schema_authority_events",
    ] {
        let count: i64 = store
            .query_row_for_test(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0, "{table} must roll back with the transaction");
    }
}

#[test]
fn sqlite_schema_authority_detects_corrupt_version_bytes_behind_head() {
    let store = SqliteSchemaAuthorityStore::open_in_memory().unwrap();
    let establishment =
        first_use_schema_authority_establishment(&store, "dev", "orders", "order_id");
    store.establish_if_absent(establishment.clone()).unwrap();
    store
        .execute_for_test("DROP TRIGGER cdf_schema_versions_no_update", [])
        .unwrap();
    store
        .execute_for_test(
            "UPDATE cdf_schema_versions SET version_json = 'not-json' WHERE resource_id = ?",
            [establishment.key.resource_id.as_str()],
        )
        .unwrap();

    let error = store.head(&establishment.key).unwrap_err();
    assert_eq!(error.kind, ErrorKind::Internal);
    assert!(error.message.contains("schema version"));
}

#[test]
fn sqlite_schema_authority_detects_missing_version_behind_head() {
    let store = SqliteSchemaAuthorityStore::open_in_memory().unwrap();
    let establishment =
        first_use_schema_authority_establishment(&store, "dev", "orders", "order_id");
    store.establish_if_absent(establishment.clone()).unwrap();
    store
        .execute_for_test("DROP TRIGGER cdf_schema_versions_no_delete", [])
        .unwrap();
    store
        .execute_for_test("PRAGMA foreign_keys = OFF", [])
        .unwrap();
    store
        .execute_for_test(
            "DELETE FROM cdf_schema_versions WHERE resource_id = ?",
            [establishment.key.resource_id.as_str()],
        )
        .unwrap();

    let error = store.head(&establishment.key).unwrap_err();
    assert_eq!(error.kind, ErrorKind::Internal);
    assert!(
        error
            .message
            .contains("references missing or corrupt version")
    );
}

#[test]
fn sqlite_schema_authority_detects_versions_without_a_head() {
    let store = SqliteSchemaAuthorityStore::open_in_memory().unwrap();
    let establishment =
        first_use_schema_authority_establishment(&store, "dev", "orders", "order_id");
    store.establish_if_absent(establishment.clone()).unwrap();
    store
        .execute_for_test(
            "DELETE FROM cdf_schema_heads WHERE resource_id = ?",
            [establishment.key.resource_id.as_str()],
        )
        .unwrap();

    let error = store.head(&establishment.key).unwrap_err();
    assert_eq!(error.kind, ErrorKind::Internal);
    assert!(error.message.contains("has versions but no head"));
}

#[test]
fn sqlite_schema_authority_records_and_requires_current_schema_version() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("schema-version.db");
    let store = SqliteSchemaAuthorityStore::open(&path).unwrap();
    let version: i64 = store
        .query_row_for_test(
            "SELECT version FROM cdf_sqlite_schema_versions WHERE component = 'schema_authority_store'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        version,
        crate::schema_authority::SCHEMA_AUTHORITY_SCHEMA_VERSION
    );
    drop(store);

    Connection::open(&path)
        .unwrap()
        .execute(
            "UPDATE cdf_sqlite_schema_versions SET version = 999 WHERE component = 'schema_authority_store'",
            [],
        )
        .unwrap();
    let error = match SqliteSchemaAuthorityStore::open(&path) {
        Ok(_) => panic!("schema authority store accepted an unsupported schema version"),
        Err(error) => error,
    };
    assert_eq!(error.kind, ErrorKind::Internal);
    assert!(
        error
            .message
            .contains("current schema version 2 is required")
    );
}

#[test]
fn sqlite_schema_authority_rejects_incomplete_current_schema() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("incomplete-schema.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "
        CREATE TABLE cdf_sqlite_schema_versions (
            component TEXT PRIMARY KEY,
            version INTEGER NOT NULL,
            recorded_at_ms INTEGER NOT NULL
        );
        INSERT INTO cdf_sqlite_schema_versions (component, version, recorded_at_ms)
        VALUES ('schema_authority_store', 2, 1);
        ",
    )
    .unwrap();
    drop(conn);

    let error = match SqliteSchemaAuthorityStore::open(&path) {
        Ok(_) => panic!("schema authority store recreated an incomplete current schema"),
        Err(error) => error,
    };
    assert_eq!(error.kind, ErrorKind::Internal);
    assert!(
        error
            .message
            .contains("required table cdf_schema_versions is missing")
    );
}

#[test]
fn sqlite_schema_authority_concurrent_first_use_has_one_winner() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("concurrent-schema.db");
    let first = Arc::new(SqliteSchemaAuthorityStore::open(&path).unwrap());
    let second = Arc::new(SqliteSchemaAuthorityStore::open(&path).unwrap());
    let first_proposal =
        first_use_schema_authority_establishment(first.as_ref(), "dev", "orders", "order_id");
    let second_proposal = first_use_schema_authority_establishment(
        second.as_ref(),
        "dev",
        "orders",
        "different_order_id",
    );
    let barrier = Arc::new(Barrier::new(2));
    let handles = [(first, first_proposal), (second, second_proposal)].map(|(store, proposal)| {
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            barrier.wait();
            store.establish_if_absent(proposal)
        })
    });
    let results = handles.map(|handle| handle.join().unwrap());
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    let losing_error = results
        .iter()
        .find_map(|result| result.as_ref().err().cloned())
        .unwrap();
    assert_eq!(losing_error.kind, ErrorKind::Contract, "{losing_error}");

    let reopened = SqliteSchemaAuthorityStore::open(&path).unwrap();
    let winning_head = results
        .iter()
        .find_map(|result| result.as_ref().ok().cloned())
        .unwrap();
    assert_eq!(
        reopened.head(&winning_head.key).unwrap(),
        Some(winning_head)
    );
}

#[test]
fn schema_settlement_permit_fences_and_atomically_commits_checkpoint() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("schema-settlement.db");
    let clock = Arc::new(ManualScopeLeaseClock::new(1_000));
    let store = SqliteSchemaAuthorityStore::open_with_clock(&path, clock.clone()).unwrap();
    let establishment =
        first_use_schema_authority_establishment(&store, "dev", "orders", "order_id");
    let active = store.establish_if_absent(establishment).unwrap();
    let run_id = RunId::new("run-settlement-1").unwrap();
    let permit = store.acquire_run_permit(&active, run_id, 100).unwrap();
    assert_eq!(permit.expires_at_ms, 1_100);
    clock.set(1_050);
    let permit = store.renew_run_permit(&permit, 100).unwrap();
    assert_eq!(permit.expires_at_ms, 1_150);

    let mut state_delta = delta(
        "checkpoint-schema-settlement",
        None,
        partition_scope(),
        cursor_position(1),
        "package-schema-settlement",
    );
    state_delta.schema_hash = active.schema_hash.clone();
    let checkpoint_store = SqliteCheckpointStore::open(&path).unwrap();
    checkpoint_store.propose(state_delta.clone()).unwrap();
    let committed = store
        .commit_run_checkpoint(&permit, &state_delta.checkpoint_id, receipt(&state_delta))
        .unwrap();

    assert_eq!(committed.status, CheckpointStatus::Committed);
    assert!(store.assert_run_permit(&permit).is_err());
    assert_eq!(
        checkpoint_store
            .head(
                &state_delta.pipeline_id,
                &state_delta.resource_id,
                &state_delta.scope
            )
            .unwrap(),
        Some(committed)
    );
}

#[test]
fn promoting_head_blocks_new_permits_but_drains_an_existing_generation() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("schema-promotion-drain.db");
    let clock = Arc::new(ManualScopeLeaseClock::new(2_000));
    let store = SqliteSchemaAuthorityStore::open_with_clock(&path, clock.clone()).unwrap();
    let leases = SqliteScopeLeaseStore::open_with_clock(&path, clock).unwrap();
    let establishment =
        first_use_schema_authority_establishment(&store, "dev", "orders", "order_id");
    let first_version = establishment.version.clone();
    let active = store.establish_if_absent(establishment).unwrap();
    let permit = store
        .acquire_run_permit(&active, RunId::new("run-before-promotion").unwrap(), 100)
        .unwrap();
    let lease = leases
        .acquire(
            active.key.promotion_scope().unwrap(),
            LeaseOwnerId::new("schema-promoter").unwrap(),
            1_000,
        )
        .unwrap();
    let fence = SchemaPromotionFence::new(
        store.authority_domain_id(),
        PromotionId::new("promotion-01").unwrap(),
        lease,
    )
    .unwrap();
    store
        .begin_promotion(
            &active,
            schema_authority_version("order_total", Some(&first_version)),
            SchemaPromotionPlanState::new(
                fence.promotion_id.clone(),
                "{}".to_owned(),
                vec![SchemaPromotionTarget {
                    destination_id: DestinationId::new("local-test").unwrap(),
                    target: TargetName::new("orders").unwrap(),
                }],
                Vec::new(),
                1_500,
            )
            .unwrap(),
            &fence,
        )
        .unwrap();
    let promoting = store.head(&active.key).unwrap().unwrap();

    assert!(
        store
            .acquire_run_permit(
                &active,
                RunId::new("run-after-promotion-fence").unwrap(),
                100
            )
            .is_err()
    );
    store.assert_run_permit(&permit).unwrap();
    assert!(
        store
            .establish_promotion_cutoff(&promoting, &fence)
            .is_err()
    );
    store.release_run_permit(&permit).unwrap();
    store
        .establish_promotion_cutoff(&promoting, &fence)
        .unwrap();
}

#[test]
fn expired_schema_settlement_permit_cannot_commit() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("expired-schema-settlement.db");
    let clock = Arc::new(ManualScopeLeaseClock::new(3_000));
    let store = SqliteSchemaAuthorityStore::open_with_clock(&path, clock.clone()).unwrap();
    let active = store
        .establish_if_absent(first_use_schema_authority_establishment(
            &store, "dev", "orders", "order_id",
        ))
        .unwrap();
    let permit = store
        .acquire_run_permit(&active, RunId::new("run-expired").unwrap(), 10)
        .unwrap();
    let mut state_delta = delta(
        "checkpoint-expired-settlement",
        None,
        partition_scope(),
        cursor_position(1),
        "package-expired-settlement",
    );
    state_delta.schema_hash = active.schema_hash;
    let checkpoint_store = SqliteCheckpointStore::open(&path).unwrap();
    checkpoint_store.propose(state_delta.clone()).unwrap();
    clock.set(3_010);

    let error = store
        .commit_run_checkpoint(&permit, &state_delta.checkpoint_id, receipt(&state_delta))
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Transient);
    assert_eq!(
        checkpoint_store
            .history(
                &state_delta.pipeline_id,
                &state_delta.resource_id,
                &state_delta.scope
            )
            .unwrap()[0]
            .status,
        CheckpointStatus::Proposed
    );
}

#[test]
fn promotion_publishes_head_only_after_cutoff_and_exact_target_settlement() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("settled-schema-promotion.db");
    let clock = Arc::new(ManualScopeLeaseClock::new(4_000));
    let store = SqliteSchemaAuthorityStore::open_with_clock(&path, clock.clone()).unwrap();
    let leases = SqliteScopeLeaseStore::open_with_clock(&path, clock).unwrap();
    let establishment =
        first_use_schema_authority_establishment(&store, "dev", "orders", "order_id");
    let first_version = establishment.version.clone();
    let active = store.establish_if_absent(establishment).unwrap();
    let proposed = schema_authority_version("order_total", Some(&first_version));
    let lease = leases
        .acquire(
            active.key.promotion_scope().unwrap(),
            LeaseOwnerId::new("settled-promoter").unwrap(),
            1_000,
        )
        .unwrap();
    let fence = SchemaPromotionFence::new(
        store.authority_domain_id(),
        PromotionId::new("promotion-01").unwrap(),
        lease,
    )
    .unwrap();
    let target = SchemaPromotionTarget {
        destination_id: DestinationId::new("local-test").unwrap(),
        target: TargetName::new("orders").unwrap(),
    };
    let plan = SchemaPromotionPlanState::new(
        fence.promotion_id.clone(),
        "{\"resource\":\"orders\"}".to_owned(),
        vec![target.clone()],
        Vec::new(),
        3_900,
    )
    .unwrap();
    store
        .begin_promotion(&active, proposed.clone(), plan.clone(), &fence)
        .unwrap();
    let promoting = store.head(&active.key).unwrap().unwrap();
    let cutoff = store
        .establish_promotion_cutoff(&promoting, &fence)
        .unwrap();
    assert_eq!(cutoff.cutoff.unwrap().checkpoints, Vec::new());
    assert!(store.publish_promotion(&promoting, &fence).is_err());

    let mut correction_delta = delta_for(
        "checkpoint-schema-correction",
        None,
        PipelineId::new("schema-promotion").unwrap(),
        active.key.resource_id.clone(),
        fence.lease.scope.clone(),
        cursor_position(1),
        "package-schema-correction",
    );
    correction_delta.schema_hash = proposed.schema_hash.clone();
    let checkpoints = SqliteCheckpointStore::open(&path).unwrap();
    checkpoints.propose(correction_delta.clone()).unwrap();
    let correction_receipt = receipt(&correction_delta);
    let settled = store
        .commit_promotion_target(
            &promoting,
            &fence,
            &target,
            &correction_delta.checkpoint_id,
            correction_receipt.clone(),
        )
        .unwrap();
    assert_eq!(settled.target_settlements.len(), 1);
    assert_eq!(
        store
            .commit_promotion_target(
                &promoting,
                &fence,
                &target,
                &correction_delta.checkpoint_id,
                correction_receipt,
            )
            .unwrap(),
        settled
    );

    let published = store.publish_promotion(&promoting, &fence).unwrap();
    assert_eq!(published.generation, 2);
    assert_eq!(published.schema_hash, proposed.schema_hash);
    let persisted = store
        .promotion_state(&active.key, &fence.promotion_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        persisted.phase,
        cdf_kernel::SchemaPromotionLifecyclePhase::Published
    );
    assert_eq!(persisted.published_generation, Some(2));
    assert_eq!(store.history(&active.key, 10).unwrap().len(), 5);
}
