use std::collections::BTreeMap;

use cdf_kernel::{
    CHECKPOINT_STATE_VERSION, Checkpoint, CheckpointId, CheckpointStore, CommitCounts,
    CursorPosition, CursorValue, DestinationId, IdempotencyToken, PackageHash, PartitionId,
    PipelineId, Receipt, ReceiptId, ResourceId, SchemaHash, ScopeKey, SegmentAck, SegmentId,
    SourcePosition, StateDelta, StateSegment, TargetName, VerifyClause, WriteDisposition,
};

pub(super) fn pipeline_id() -> PipelineId {
    PipelineId::new("pipeline-1").unwrap()
}

pub(super) fn resource_id() -> ResourceId {
    ResourceId::new("orders").unwrap()
}

pub(super) fn other_resource_id() -> ResourceId {
    ResourceId::new("customers").unwrap()
}

pub(super) fn partition_scope() -> ScopeKey {
    ScopeKey::Partition {
        partition_id: PartitionId::new("p0").unwrap(),
    }
}

pub(super) fn other_partition_scope() -> ScopeKey {
    ScopeKey::Partition {
        partition_id: PartitionId::new("p1").unwrap(),
    }
}

pub(super) fn cursor_position(value: i64) -> SourcePosition {
    SourcePosition::Cursor(CursorPosition {
        version: CHECKPOINT_STATE_VERSION,
        field: "updated_at".to_owned(),
        value: CursorValue::I64(value),
    })
}

pub(super) fn delta(
    checkpoint_id: &str,
    parent_checkpoint_id: Option<&CheckpointId>,
    scope: ScopeKey,
    output_position: SourcePosition,
    package_hash: &str,
) -> StateDelta {
    delta_for(
        checkpoint_id,
        parent_checkpoint_id,
        pipeline_id(),
        resource_id(),
        scope,
        output_position,
        package_hash,
    )
}

pub(super) fn delta_for(
    checkpoint_id: &str,
    parent_checkpoint_id: Option<&CheckpointId>,
    pipeline_id: PipelineId,
    resource_id: ResourceId,
    scope: ScopeKey,
    output_position: SourcePosition,
    package_hash: &str,
) -> StateDelta {
    let segments = vec![
        StateSegment {
            segment_id: SegmentId::new(format!("{checkpoint_id}-segment-a")).unwrap(),
            scope: scope.clone(),
            output_position: output_position.clone(),
            row_count: 10,
            byte_count: 80,
        },
        StateSegment {
            segment_id: SegmentId::new(format!("{checkpoint_id}-segment-b")).unwrap(),
            scope: scope.clone(),
            output_position: output_position.clone(),
            row_count: 7,
            byte_count: 56,
        },
    ];

    StateDelta {
        checkpoint_id: CheckpointId::new(checkpoint_id).unwrap(),
        pipeline_id,
        resource_id,
        scope,
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: parent_checkpoint_id.cloned(),
        input_position: None,
        output_position,
        output_watermark: None,
        source_continuation: None,
        package_hash: PackageHash::new(package_hash).unwrap(),
        schema_hash: SchemaHash::new("schema-sha256").unwrap(),
        segments,
    }
}

pub(super) fn receipt(delta: &StateDelta) -> Receipt {
    let rows_written = delta
        .segments
        .iter()
        .map(|segment| segment.row_count)
        .sum::<u64>();

    Receipt {
        receipt_id: ReceiptId::new(format!("receipt-{}", delta.checkpoint_id)).unwrap(),
        destination: DestinationId::new("local-test").unwrap(),
        target: TargetName::new("orders").unwrap(),
        package_hash: delta.package_hash.clone(),
        segment_acks: delta
            .segments
            .iter()
            .map(|segment| SegmentAck {
                segment_id: segment.segment_id.clone(),
                row_count: segment.row_count,
                byte_count: segment.byte_count,
            })
            .collect(),
        disposition: WriteDisposition::Merge,
        idempotency_token: IdempotencyToken::new(delta.package_hash.as_str()).unwrap(),
        transaction: None,
        counts: CommitCounts {
            rows_written,
            rows_inserted: Some(rows_written),
            rows_updated: Some(0),
            rows_deleted: Some(0),
        },
        schema_hash: delta.schema_hash.clone(),
        migrations: Vec::new(),
        committed_at_ms: 1_700_000_000_000,
        verify: VerifyClause {
            kind: "sql".to_owned(),
            statement: "select count(*) from orders where _cdf_package = ?".to_owned(),
            parameters: BTreeMap::new(),
        },
    }
}

pub(super) fn commit_delta<S: CheckpointStore>(store: &S, delta: StateDelta) -> Checkpoint {
    let checkpoint_id = delta.checkpoint_id.clone();
    let receipt = receipt(&delta);
    let committed_at_ms = receipt.committed_at_ms;
    let proposed = store.propose(delta).unwrap();
    assert_plausible_created_at(&proposed);
    assert_eq!(proposed.committed_at_ms, None);

    let committed = store.commit(&checkpoint_id, receipt).unwrap();
    assert_plausible_created_at(&committed);
    assert_eq!(committed.committed_at_ms, Some(committed_at_ms));
    committed
}

pub(super) fn assert_plausible_created_at(checkpoint: &Checkpoint) {
    assert!(
        checkpoint.created_at_ms > 1_600_000_000_000,
        "checkpoint timestamp should be a plausible positive epoch millisecond"
    );
}
