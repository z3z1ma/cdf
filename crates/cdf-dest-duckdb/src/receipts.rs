use std::collections::BTreeMap;

use cdf_kernel::{
    CommitCounts, CommitPlan, DestinationCommitRequest, DestinationId, Receipt, ReceiptId, Result,
    SchemaHash, SegmentAck, TransactionMetadata, VerifyClause,
};
use cdf_package_contract::{ReceiptDraft, ReceiptEvidence};

use crate::{DESTINATION_ID, DUCKDB_BULK_PATH_SEGMENT_SCAN, models::ReceiptBuildContext};

pub(crate) fn build_receipt(
    commit: &DestinationCommitRequest,
    plan: &CommitPlan,
    schema_hash: &SchemaHash,
    segment_acks: &[SegmentAck],
    counts: CommitCounts,
    context: &ReceiptBuildContext<'_>,
) -> Result<Receipt> {
    let mut transaction_values = BTreeMap::new();
    transaction_values.insert(
        "database_path".to_owned(),
        context.database_path.display().to_string(),
    );
    transaction_values.insert(
        "duckdb_version".to_owned(),
        context.duckdb_version.to_owned(),
    );
    transaction_values.insert(
        "writer_lock".to_owned(),
        context.lock_path.display().to_string(),
    );
    transaction_values.insert(
        "bulk_path".to_owned(),
        DUCKDB_BULK_PATH_SEGMENT_SCAN.to_owned(),
    );

    let mut parameters = BTreeMap::new();
    parameters.insert("target".to_owned(), commit.target.as_str().to_owned());
    parameters.insert(
        "idempotency_token".to_owned(),
        commit.idempotency_token.as_str().to_owned(),
    );
    parameters.insert(
        "package_hash".to_owned(),
        commit.package_hash.as_str().to_owned(),
    );

    ReceiptDraft::ordinary(
        ReceiptId::new(format!(
            "duckdb:{}:{}",
            commit.target.as_str(),
            commit.idempotency_token.as_str()
        ))?,
        DestinationId::new(DESTINATION_ID)?,
        commit,
        plan,
        segment_acks.to_vec(),
        schema_hash.clone(),
        ReceiptEvidence {
            transaction: Some(TransactionMetadata {
                system: "duckdb".to_owned(),
                values: transaction_values,
            }),
            counts,
            committed_at_ms: context.committed_at_ms,
            verify: VerifyClause {
                kind: "duckdb_load_receipt_v1".to_owned(),
                statement: "SELECT receipt_json FROM _cdf_loads WHERE target = ? AND idempotency_token = ? AND package_hash = ?".to_owned(),
                parameters,
            },
        },
    )
    ?
    .finalize()
}
