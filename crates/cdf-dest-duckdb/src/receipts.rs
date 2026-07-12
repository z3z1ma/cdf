use crate::api::*;
use crate::*;

pub(crate) fn build_receipt(
    commit: &DestinationCommitRequest,
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

    Ok(Receipt {
        receipt_id: ReceiptId::new(format!(
            "duckdb:{}:{}",
            commit.target.as_str(),
            commit.idempotency_token.as_str()
        ))?,
        destination: DestinationId::new(DESTINATION_ID)?,
        target: commit.target.clone(),
        package_hash: commit.package_hash.clone(),
        segment_acks: segment_acks.to_vec(),
        disposition: commit.disposition.clone(),
        idempotency_token: commit.idempotency_token.clone(),
        transaction: Some(TransactionMetadata {
            system: "duckdb".to_owned(),
            values: transaction_values,
        }),
        counts,
        schema_hash: schema_hash.clone(),
        migrations: context.migrations.to_vec(),
        committed_at_ms: context.committed_at_ms,
        verify: VerifyClause {
            kind: "duckdb_load_receipt_v1".to_owned(),
            statement: "SELECT receipt_json FROM _cdf_loads WHERE target = ? AND idempotency_token = ? AND package_hash = ?".to_owned(),
            parameters,
        },
    })
}

pub(crate) fn record_package_receipt_once(package_dir: &Path, receipt: &Receipt) -> Result<bool> {
    let reader = PackageReader::open(package_dir)?;
    let receipts = reader.receipts()?;
    if receipts
        .iter()
        .any(|existing| existing.receipt_id == receipt.receipt_id)
    {
        return Ok(false);
    }
    reader.append_receipt(receipt.clone())?;
    Ok(true)
}
