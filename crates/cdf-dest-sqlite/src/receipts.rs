use std::collections::BTreeMap;

use cdf_kernel::{
    CommitCounts, DestinationCommitRequest, DestinationId, Receipt, ReceiptId, Result, SegmentAck,
    TransactionMetadata, VerifyClause,
};
use cdf_package_contract::{ReceiptDraft, ReceiptEvidence};
use sha2::{Digest, Sha256};

use crate::{
    SQLITE_DESTINATION_ID,
    mirrors::SqliteCommitEvidence,
    models::{SqliteLoadPlan, SqliteReceiptInput},
};

pub(crate) fn receipt_id(plan: &SqliteLoadPlan) -> Result<ReceiptId> {
    let mut hasher = Sha256::new();
    hasher.update(SQLITE_DESTINATION_ID.as_bytes());
    hasher.update([0]);
    hasher.update(plan.kernel.target.as_str().as_bytes());
    hasher.update([0]);
    hasher.update(plan.package_hash.as_str().as_bytes());
    ReceiptId::new(format!("sqlite-{:x}", hasher.finalize()))
}

pub(crate) fn segment_acks(plan: &SqliteLoadPlan) -> Vec<SegmentAck> {
    plan.segments
        .iter()
        .map(|segment| SegmentAck {
            kind: segment.kind,
            segment_id: segment.segment_id.clone(),
            row_count: segment.row_count,
            byte_count: segment.byte_count,
        })
        .collect()
}

pub(crate) fn build_receipt(plan: &SqliteLoadPlan, input: SqliteReceiptInput) -> Result<Receipt> {
    let request = DestinationCommitRequest {
        package_hash: plan.package_hash.clone(),
        content: plan.content.clone(),
        target: plan.kernel.target.clone(),
        disposition: plan.kernel.disposition.clone(),
        segments: plan.segments.clone(),
        idempotency_token: plan.idempotency_token.clone(),
    };
    ReceiptDraft::ordinary(
        receipt_id(plan)?,
        DestinationId::new(SQLITE_DESTINATION_ID)?,
        &request,
        &plan.kernel,
        segment_acks(plan),
        plan.schema_hash.clone(),
        ReceiptEvidence {
            transaction: Some(input.transaction),
            counts: input.counts,
            committed_at_ms: input.committed_at_ms,
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
    let mut parameters = BTreeMap::new();
    parameters.insert("target".to_owned(), target.as_str().to_owned());
    parameters.insert("package_hash".to_owned(), package_hash.as_str().to_owned());
    parameters.insert(
        "idempotency_token".to_owned(),
        idempotency_token.as_str().to_owned(),
    );
    parameters.insert("schema_hash".to_owned(), schema_hash.as_str().to_owned());
    parameters.insert("segment_count".to_owned(), segments.len().to_string());
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
        kind: "sqlite_mirror_receipt_v1".to_owned(),
        statement: "SELECT receipt_json FROM _cdf_loads WHERE target = ?1 AND package_hash = ?2 AND idempotency_token = ?3".to_owned(),
        parameters,
    }
}

pub(crate) fn transaction_metadata(
    journal_mode: String,
    synchronous: i64,
    duplicate: bool,
    evidence: &SqliteCommitEvidence,
) -> Result<TransactionMetadata> {
    Ok(TransactionMetadata {
        system: SQLITE_DESTINATION_ID.to_owned(),
        values: [
            ("connection_scope".to_owned(), "single_file".to_owned()),
            ("journal_mode".to_owned(), journal_mode),
            ("synchronous".to_owned(), synchronous.to_string()),
            ("duplicate".to_owned(), duplicate.to_string()),
            (
                "quarantine_count".to_owned(),
                evidence.quarantine.count.to_string(),
            ),
            (
                "quarantine_multiset_sha256_v1".to_owned(),
                evidence.quarantine.hex_commitment(),
            ),
            ("commit_evidence_sha256_v1".to_owned(), evidence.sha256()?),
            ("loads_table".to_owned(), "_cdf_loads".to_owned()),
            ("state_table".to_owned(), "_cdf_state".to_owned()),
            (
                "state_history_table".to_owned(),
                "_cdf_state_history".to_owned(),
            ),
            ("segments_table".to_owned(), "_cdf_segments".to_owned()),
            (
                "commit_evidence_table".to_owned(),
                "_cdf_commit_evidence".to_owned(),
            ),
        ]
        .into_iter()
        .collect(),
    })
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct QuarantineEvidence {
    pub(crate) count: u64,
    record_sha256: Vec<[u8; 32]>,
}

impl QuarantineEvidence {
    pub(crate) fn observe(
        &mut self,
        record: &cdf_package_contract::QuarantineRecord,
    ) -> Result<()> {
        let encoded = serde_json::to_vec(record).map_err(|error| {
            cdf_kernel::CdfError::internal(format!(
                "encode SQLite quarantine receipt evidence: {error}"
            ))
        })?;
        self.record_sha256.push(Sha256::digest(encoded).into());
        self.count = self.count.checked_add(1).ok_or_else(|| {
            cdf_kernel::CdfError::data("SQLite quarantine evidence count overflowed")
        })?;
        Ok(())
    }

    pub(crate) fn hex_commitment(&self) -> String {
        let mut digests = self.record_sha256.clone();
        digests.sort_unstable();
        let mut hasher = Sha256::new();
        hasher.update(b"cdf.sqlite.quarantine-multiset.v1\0");
        hasher.update(self.count.to_be_bytes());
        for digest in digests {
            hasher.update(digest);
        }
        format!("{:x}", hasher.finalize())
    }

    pub(crate) fn canonicalize(&mut self) {
        self.record_sha256.sort_unstable();
    }
}

pub(crate) fn package_quarantine_evidence(
    package: &dyn cdf_package_contract::VerifiedPackageAccess,
) -> Result<QuarantineEvidence> {
    let mut evidence = QuarantineEvidence::default();
    package.for_each_quarantine_record(&mut |record| evidence.observe(&record))?;
    evidence.canonicalize();
    Ok(evidence)
}

pub(crate) fn duplicate_receipt_input(stored: &Receipt) -> Result<SqliteReceiptInput> {
    Ok(SqliteReceiptInput {
        committed_at_ms: stored.committed_at_ms,
        counts: stored.counts.clone(),
        transaction: stored.transaction.clone().unwrap_or(TransactionMetadata {
            system: SQLITE_DESTINATION_ID.to_owned(),
            values: BTreeMap::new(),
        }),
    })
}

pub(crate) fn expected_counts(plan: &SqliteLoadPlan, stored: &Receipt) -> Result<CommitCounts> {
    let rows = plan.segments.iter().try_fold(0_u64, |total, segment| {
        total.checked_add(segment.row_count).ok_or_else(|| {
            cdf_kernel::CdfError::data("SQLite duplicate receipt row count overflowed")
        })
    })?;
    let settled = match &stored.counts {
        CommitCounts::Rows { rows_written, .. } => *rows_written,
        CommitCounts::KeyedChanges { intent, .. } => intent.total()?,
        CommitCounts::Routed { .. } => stored
            .counts
            .settled_effect_count()
            .ok_or_else(|| cdf_kernel::CdfError::data("routed receipt count overflowed u64"))?,
    };
    if settled != rows && !plan.segments.is_empty() {
        return Err(cdf_kernel::CdfError::destination(
            "SQLite duplicate receipt row count contradicts package segments",
        ));
    }
    Ok(stored.counts.clone())
}
