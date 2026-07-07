use crate::api::*;
use crate::*;

pub(crate) fn build_receipt(
    request: &DuckDbCommitRequest,
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
    parameters.insert(
        "target".to_owned(),
        request.commit.target.as_str().to_owned(),
    );
    parameters.insert(
        "idempotency_token".to_owned(),
        request.commit.idempotency_token.as_str().to_owned(),
    );
    parameters.insert(
        "package_hash".to_owned(),
        request.commit.package_hash.as_str().to_owned(),
    );

    Ok(Receipt {
        receipt_id: ReceiptId::new(format!(
            "duckdb:{}:{}",
            request.commit.target.as_str(),
            request.commit.idempotency_token.as_str()
        ))?,
        destination: DestinationId::new(DESTINATION_ID)?,
        target: request.commit.target.clone(),
        package_hash: request.commit.package_hash.clone(),
        segment_acks: segment_acks.to_vec(),
        disposition: request.commit.disposition.clone(),
        idempotency_token: request.commit.idempotency_token.clone(),
        transaction: Some(TransactionMetadata {
            system: "duckdb".to_owned(),
            values: transaction_values,
        }),
        counts,
        schema_hash: request.schema_hash.clone(),
        migrations: context.migrations.to_vec(),
        committed_at_ms: context.committed_at_ms,
        verify: VerifyClause {
            kind: "duckdb_load_receipt_v1".to_owned(),
            statement: "SELECT receipt_json FROM _cdf_loads WHERE target = ? AND idempotency_token = ? AND package_hash = ?".to_owned(),
            parameters,
        },
    })
}

pub(crate) fn segment_acks(requested: &[StateSegment], package: &PackageData) -> Vec<SegmentAck> {
    let requested_by_id = requested
        .iter()
        .map(|segment| (segment.segment_id.as_str(), segment))
        .collect::<BTreeMap<_, _>>();
    package
        .segments
        .iter()
        .map(
            |segment| match requested_by_id.get(segment.entry.segment_id.as_str()) {
                Some(requested) => SegmentAck {
                    segment_id: requested.segment_id.clone(),
                    row_count: requested.row_count,
                    byte_count: requested.byte_count,
                },
                None => SegmentAck {
                    segment_id: segment.entry.segment_id.clone(),
                    row_count: segment.row_count,
                    byte_count: segment.entry.byte_count,
                },
            },
        )
        .collect()
}

pub(crate) fn validate_requested_segments(
    requested: &[StateSegment],
    package: &PackageData,
) -> Result<()> {
    if requested.is_empty() {
        return Ok(());
    }
    let package_segments = package
        .segments
        .iter()
        .map(|segment| (segment.entry.segment_id.as_str(), segment.row_count))
        .collect::<BTreeMap<_, _>>();
    for segment in requested {
        match package_segments.get(segment.segment_id.as_str()) {
            Some(row_count) if *row_count == segment.row_count => {}
            Some(row_count) => {
                return Err(CdfError::data(format!(
                    "requested segment {} has {} rows but package has {}",
                    segment.segment_id.as_str(),
                    segment.row_count,
                    row_count
                )));
            }
            None => {
                return Err(CdfError::data(format!(
                    "requested segment {} is not present in package",
                    segment.segment_id.as_str()
                )));
            }
        }
    }
    Ok(())
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
