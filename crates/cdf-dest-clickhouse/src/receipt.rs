use std::collections::BTreeMap;

use cdf_kernel::{
    CommitCounts, DestinationCommitRequest, DestinationId, Receipt, ReceiptId, Result, SegmentAck,
    TransactionMetadata, VerifyClause,
};
use cdf_package_contract::{ReceiptDraft, ReceiptEvidence};
use sha2::{Digest, Sha256};

use crate::{CLICKHOUSE_DESTINATION_ID, models::ClickHouseLoadPlan};

pub(crate) fn receipt_id(plan: &ClickHouseLoadPlan) -> Result<ReceiptId> {
    let mut hasher = Sha256::new();
    hasher.update(CLICKHOUSE_DESTINATION_ID.as_bytes());
    hasher.update([0]);
    hasher.update(plan.kernel.target.as_str().as_bytes());
    hasher.update([0]);
    hasher.update(plan.package_hash.as_str().as_bytes());
    ReceiptId::new(format!("clickhouse-{:x}", hasher.finalize()))
}

pub(crate) fn segment_acks(plan: &ClickHouseLoadPlan) -> Vec<SegmentAck> {
    plan.segments
        .iter()
        .map(|segment| SegmentAck {
            segment_id: segment.segment_id.clone(),
            row_count: segment.row_count,
            byte_count: segment.byte_count,
        })
        .collect()
}

pub(crate) fn build_receipt(
    plan: &ClickHouseLoadPlan,
    committed_at_ms: i64,
    counts: CommitCounts,
    transaction: TransactionMetadata,
) -> Result<Receipt> {
    let request = DestinationCommitRequest {
        package_hash: plan.package_hash.clone(),
        target: plan.kernel.target.clone(),
        disposition: plan.kernel.disposition.clone(),
        segments: plan.segments.clone(),
        idempotency_token: plan.idempotency_token.clone(),
    };
    ReceiptDraft::ordinary(
        receipt_id(plan)?,
        DestinationId::new(CLICKHOUSE_DESTINATION_ID)?,
        &request,
        &plan.kernel,
        segment_acks(plan),
        plan.schema_hash.clone(),
        ReceiptEvidence {
            transaction: Some(transaction),
            counts,
            committed_at_ms,
            verify: plan.verify.clone(),
        },
    )?
    .finalize()
}

pub(crate) fn verify_clause(
    target: &cdf_kernel::TargetName,
    package_hash: &cdf_kernel::PackageHash,
    idempotency_token: &cdf_kernel::IdempotencyToken,
    schema_hash: &cdf_kernel::SchemaHash,
    segments: &[cdf_kernel::StateSegment],
) -> VerifyClause {
    let mut parameters = BTreeMap::from([
        ("target".to_owned(), target.as_str().to_owned()),
        ("package_hash".to_owned(), package_hash.as_str().to_owned()),
        (
            "idempotency_token".to_owned(),
            idempotency_token.as_str().to_owned(),
        ),
        ("schema_hash".to_owned(), schema_hash.as_str().to_owned()),
        ("segment_count".to_owned(), segments.len().to_string()),
    ]);
    for (index, segment) in segments.iter().enumerate() {
        parameters.insert(
            format!("segment.{index}.id"),
            segment.segment_id.to_string(),
        );
        parameters.insert(
            format!("segment.{index}.rows"),
            segment.row_count.to_string(),
        );
        parameters.insert(
            format!("segment.{index}.bytes"),
            segment.byte_count.to_string(),
        );
    }
    VerifyClause {
        kind: "clickhouse_mirror_receipt_v1".to_owned(),
        statement: "SELECT receipt_json FROM _cdf_loads WHERE target = ? AND package_hash = ? AND idempotency_token = ?".to_owned(),
        parameters,
    }
}

pub(crate) fn transaction_metadata(
    database: &str,
    engine: &str,
    database_engine: &str,
    marker: &str,
    state_sha256: &str,
    duplicate: bool,
    merge_mode: crate::models::ClickHouseMergeMode,
) -> TransactionMetadata {
    let merge_visibility = match merge_mode {
        crate::models::ClickHouseMergeMode::ReplacingMergeTree => "logical_final_eventual_physical",
        crate::models::ClickHouseMergeMode::AtomicCopyOnWrite => "immediate_atomic_physical",
    };
    TransactionMetadata {
        system: CLICKHOUSE_DESTINATION_ID.to_owned(),
        values: [
            ("database".to_owned(), database.to_owned()),
            ("table_engine".to_owned(), engine.to_owned()),
            ("database_engine".to_owned(), database_engine.to_owned()),
            ("acknowledgement".to_owned(), "synchronous".to_owned()),
            ("async_insert".to_owned(), "0".to_owned()),
            ("compression".to_owned(), "lz4".to_owned()),
            ("replace_marker".to_owned(), marker.to_owned()),
            ("state_sha256".to_owned(), state_sha256.to_owned()),
            ("duplicate".to_owned(), duplicate.to_string()),
            ("merge_mode".to_owned(), merge_mode.as_str().to_owned()),
            ("merge_visibility".to_owned(), merge_visibility.to_owned()),
            ("loads_table".to_owned(), "_cdf_loads".to_owned()),
            ("segments_table".to_owned(), "_cdf_segments".to_owned()),
            ("state_table".to_owned(), "_cdf_state".to_owned()),
        ]
        .into_iter()
        .collect(),
    }
}
