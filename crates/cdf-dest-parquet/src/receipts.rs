use crate::manifest::{
    ParquetObjectManifest, ParquetReplacePointerReceipt, ReplacePointer, canonical_json_bytes,
    sha256_hex,
};
use crate::*;

pub(crate) fn build_receipt(
    request: &ParquetCommitRequest,
    plan: &ParquetCommitPlan,
    manifest: &ParquetObjectManifest,
    manifest_etag: Option<String>,
    replace_pointer: Option<ParquetReplacePointerReceipt>,
) -> Result<Receipt> {
    let manifest_bytes = canonical_json_bytes(manifest)?;
    let manifest_sha256 = sha256_hex(&manifest_bytes);
    let mut transaction_values = BTreeMap::new();
    transaction_values.insert("store".to_owned(), plan.manifest_key.clone());
    transaction_values.insert("manifest_key".to_owned(), plan.manifest_key.clone());
    transaction_values.insert("manifest_sha256".to_owned(), manifest_sha256.clone());
    let provenance_key = plan.provenance_manifest_key.clone();
    transaction_values.insert("provenance_manifest_key".to_owned(), provenance_key.clone());
    transaction_values.insert(
        "object_count".to_owned(),
        manifest.objects.len().to_string(),
    );
    transaction_values.insert("row_count".to_owned(), manifest.total_rows.to_string());
    transaction_values.insert(
        "schema_hash".to_owned(),
        request.schema_hash.as_str().to_owned(),
    );
    if let Some(etag) = &manifest_etag {
        transaction_values.insert("manifest_etag".to_owned(), etag.clone());
    }
    if let Some(pointer) = &replace_pointer {
        transaction_values.insert("replace_pointer_key".to_owned(), pointer.key.clone());
        transaction_values.insert("replace_pointer_sha256".to_owned(), pointer.sha256.clone());
        if let Some(etag) = &pointer.etag {
            transaction_values.insert("replace_pointer_etag".to_owned(), etag.clone());
        }
    }

    let mut parameters = BTreeMap::new();
    parameters.insert("manifest_key".to_owned(), plan.manifest_key.clone());
    parameters.insert("manifest_sha256".to_owned(), manifest_sha256);
    parameters.insert("provenance_manifest_key".to_owned(), provenance_key);
    parameters.insert(
        "package_hash".to_owned(),
        request.commit.package_hash.as_str().to_owned(),
    );
    parameters.insert(
        "idempotency_token".to_owned(),
        request.commit.idempotency_token.as_str().to_owned(),
    );
    parameters.insert(
        "target".to_owned(),
        request.commit.target.as_str().to_owned(),
    );

    Ok(Receipt {
        receipt_id: ReceiptId::new(format!(
            "parquet:{}:{}",
            request.commit.target.as_str(),
            request.commit.idempotency_token.as_str()
        ))?,
        destination: DestinationId::new(DESTINATION_ID)?,
        target: request.commit.target.clone(),
        package_hash: request.commit.package_hash.clone(),
        segment_acks: segment_acks(manifest)?,
        disposition: request.commit.disposition.clone(),
        idempotency_token: request.commit.idempotency_token.clone(),
        transaction: Some(TransactionMetadata {
            system: "object_store".to_owned(),
            values: transaction_values,
        }),
        counts: CommitCounts {
            rows_written: manifest.total_rows,
            rows_inserted: Some(manifest.total_rows),
            rows_updated: Some(0),
            rows_deleted: Some(0),
        },
        schema_hash: request.schema_hash.clone(),
        migrations: Vec::new(),
        committed_at_ms: manifest.committed_at_ms,
        verify: VerifyClause {
            kind: "parquet_object_manifest_v1".to_owned(),
            statement: "get manifest_key and verify manifest/object sha256 plus etag-if-present"
                .to_owned(),
            parameters,
        },
    })
}

pub(crate) fn verify_receipt(destination: &ParquetDestination, receipt: &Receipt) -> Result<()> {
    if receipt.destination.as_str() != DESTINATION_ID {
        return Err(CdfError::data(format!(
            "receipt destination {} is not {}",
            receipt.destination.as_str(),
            DESTINATION_ID
        )));
    }
    let manifest_key = receipt
        .verify
        .parameters
        .get("manifest_key")
        .ok_or_else(|| CdfError::data("receipt is missing manifest_key verify parameter"))?;
    let expected_manifest_sha = receipt
        .verify
        .parameters
        .get("manifest_sha256")
        .ok_or_else(|| CdfError::data("receipt is missing manifest_sha256 verify parameter"))?;
    let manifest_bytes = destination
        .store()
        .get_required(destination.execution(), manifest_key)?;
    let actual_manifest_sha = sha256_hex(&manifest_bytes);
    if &actual_manifest_sha != expected_manifest_sha {
        return Err(CdfError::data(format!(
            "manifest {manifest_key} sha256 mismatch: expected {expected_manifest_sha}, got {actual_manifest_sha}"
        )));
    }
    let provenance_key = receipt
        .verify
        .parameters
        .get("provenance_manifest_key")
        .ok_or_else(|| CdfError::data("receipt is missing provenance_manifest_key"))?;
    let provenance_bytes = destination
        .store()
        .get_required(destination.execution(), provenance_key)?;
    if provenance_bytes != manifest_bytes {
        return Err(CdfError::data(format!(
            "provenance manifest {provenance_key} differs from package manifest {manifest_key}"
        )));
    }
    if let Some(expected_etag) = receipt
        .transaction
        .as_ref()
        .and_then(|transaction| transaction.values.get("manifest_etag"))
    {
        let actual_etag = destination
            .store()
            .etag(destination.execution(), manifest_key)?;
        if actual_etag.as_ref() != Some(expected_etag) {
            return Err(CdfError::data(format!(
                "manifest {manifest_key} etag mismatch: expected {:?}, got {:?}",
                expected_etag, actual_etag
            )));
        }
    }

    let manifest: ParquetObjectManifest =
        serde_json::from_slice(&manifest_bytes).map_err(|error| {
            CdfError::data(format!("parse Parquet manifest {manifest_key}: {error}"))
        })?;
    validate_manifest_matches_receipt(&manifest, receipt)?;

    for object in &manifest.objects {
        let digest = destination
            .store()
            .digest(destination.execution(), &object.key)?;
        if digest.sha256 != object.sha256 {
            return Err(CdfError::data(format!(
                "object {} sha256 mismatch: expected {}, got {}",
                object.key, object.sha256, digest.sha256
            )));
        }
        if digest.byte_count != object.parquet_byte_count {
            return Err(CdfError::data(format!(
                "object {} byte count mismatch: expected {}, got {}",
                object.key, object.parquet_byte_count, digest.byte_count
            )));
        }
        if let Some(expected_etag) = &object.etag {
            let actual_etag = destination
                .store()
                .etag(destination.execution(), &object.key)?;
            if actual_etag.as_ref() != Some(expected_etag) {
                return Err(CdfError::data(format!(
                    "object {} etag mismatch: expected {:?}, got {:?}",
                    object.key, expected_etag, actual_etag
                )));
            }
        }
    }

    if receipt.disposition == WriteDisposition::Replace && manifest.objects.is_empty() {
        let has_pointer_evidence = receipt.transaction.as_ref().is_some_and(|transaction| {
            transaction.values.contains_key("replace_pointer_key")
                || transaction.values.contains_key("replace_pointer_sha256")
                || transaction.values.contains_key("replace_pointer_etag")
        });
        if has_pointer_evidence {
            return Err(CdfError::data(
                "zero-data replace receipt must not claim a replace-pointer mutation",
            ));
        }
    } else if receipt.disposition == WriteDisposition::Replace {
        let transaction = receipt
            .transaction
            .as_ref()
            .ok_or_else(|| CdfError::data("replace receipt is missing transaction metadata"))?;
        let pointer_key = transaction
            .values
            .get("replace_pointer_key")
            .ok_or_else(|| CdfError::data("replace receipt is missing replace_pointer_key"))?;
        let pointer_sha256 = transaction
            .values
            .get("replace_pointer_sha256")
            .ok_or_else(|| CdfError::data("replace receipt is missing replace_pointer_sha256"))?;
        let bytes = destination
            .store()
            .get_required(destination.execution(), pointer_key)?;
        let actual = sha256_hex(&bytes);
        if actual != *pointer_sha256 {
            return Err(CdfError::data(format!(
                "replace pointer {} sha256 mismatch: expected {}, got {}",
                pointer_key, pointer_sha256, actual
            )));
        }
        let parsed: ReplacePointer = serde_json::from_slice(&bytes).map_err(|error| {
            CdfError::data(format!("parse replace pointer {pointer_key}: {error}"))
        })?;
        if parsed.manifest_key != *manifest_key
            || parsed.manifest_sha256 != *expected_manifest_sha
            || parsed.target != receipt.target.as_str()
            || parsed.package_hash != receipt.package_hash.as_str()
            || parsed.idempotency_token != receipt.idempotency_token.as_str()
            || parsed.schema_hash != receipt.schema_hash.as_str()
        {
            return Err(CdfError::data(format!(
                "replace pointer {} does not match manifest {} identity",
                pointer_key, manifest_key
            )));
        }
        if let Some(expected_etag) = transaction.values.get("replace_pointer_etag") {
            let actual_etag = destination
                .store()
                .etag(destination.execution(), pointer_key)?;
            if actual_etag.as_ref() != Some(expected_etag) {
                return Err(CdfError::data(format!(
                    "replace pointer {} etag mismatch: expected {:?}, got {:?}",
                    pointer_key, expected_etag, actual_etag
                )));
            }
        }
    }

    Ok(())
}

fn validate_manifest_matches_receipt(
    manifest: &ParquetObjectManifest,
    receipt: &Receipt,
) -> Result<()> {
    if manifest.manifest_version != MANIFEST_VERSION {
        return Err(CdfError::data(format!(
            "unsupported Parquet manifest version {}",
            manifest.manifest_version
        )));
    }
    if manifest.destination != DESTINATION_ID {
        return Err(CdfError::data(format!(
            "manifest destination {} is not {}",
            manifest.destination, DESTINATION_ID
        )));
    }
    if manifest.target != receipt.target.as_str()
        || manifest.package_hash != receipt.package_hash.as_str()
        || manifest.idempotency_token != receipt.idempotency_token.as_str()
        || manifest.disposition != receipt.disposition
        || manifest.schema_hash != receipt.schema_hash.as_str()
    {
        return Err(CdfError::data(
            "manifest identity metadata does not match receipt",
        ));
    }
    if manifest.total_rows != receipt.counts.rows_written {
        return Err(CdfError::data(format!(
            "manifest row count {} does not match receipt {}",
            manifest.total_rows, receipt.counts.rows_written
        )));
    }

    let mut manifest_segments = BTreeMap::new();
    for object in &manifest.objects {
        if object.schema_hash != receipt.schema_hash.as_str() {
            return Err(CdfError::data(format!(
                "object {} schema hash mismatch",
                object.key
            )));
        }
        let mut row_offset = 0_u64;
        let mut byte_count = 0_u64;
        let mut package_byte_count = 0_u64;
        for segment in &object.segments {
            if segment.row_offset != row_offset {
                return Err(CdfError::data(format!(
                    "object {} segment {} row offset {} does not follow {}",
                    object.key, segment.segment_id, segment.row_offset, row_offset
                )));
            }
            row_offset = row_offset
                .checked_add(segment.row_count)
                .ok_or_else(|| CdfError::data("manifest object row count overflow"))?;
            byte_count = byte_count
                .checked_add(segment.byte_count)
                .ok_or_else(|| CdfError::data("manifest object state byte count overflow"))?;
            package_byte_count = package_byte_count
                .checked_add(segment.package_byte_count)
                .ok_or_else(|| CdfError::data("manifest object package byte count overflow"))?;
            if manifest_segments
                .insert(segment.segment_id.as_str(), segment)
                .is_some()
            {
                return Err(CdfError::data(format!(
                    "manifest repeats segment {}",
                    segment.segment_id
                )));
            }
        }
        if object.segments.is_empty()
            || object.row_count != row_offset
            || object.byte_count != byte_count
            || object.package_byte_count != package_byte_count
        {
            return Err(CdfError::data(format!(
                "object {} aggregate counts do not match its segment entries",
                object.key
            )));
        }
    }
    for ack in &receipt.segment_acks {
        let object = manifest_segments
            .get(ack.segment_id.as_str())
            .ok_or_else(|| {
                CdfError::data(format!(
                    "receipt segment {} is missing from manifest",
                    ack.segment_id.as_str()
                ))
            })?;
        if object.row_count != ack.row_count {
            return Err(CdfError::data(format!(
                "segment {} row count mismatch: manifest {}, receipt {}",
                ack.segment_id.as_str(),
                object.row_count,
                ack.row_count
            )));
        }
        if object.byte_count != ack.byte_count {
            return Err(CdfError::data(format!(
                "segment {} state byte count mismatch: manifest {}, receipt {}",
                ack.segment_id.as_str(),
                object.byte_count,
                ack.byte_count
            )));
        }
    }

    if manifest_segments.len() != receipt.segment_acks.len() {
        return Err(CdfError::data(format!(
            "manifest segment count {} does not match receipt segment count {}",
            manifest_segments.len(),
            receipt.segment_acks.len()
        )));
    }

    Ok(())
}

fn segment_acks(manifest: &ParquetObjectManifest) -> Result<Vec<SegmentAck>> {
    manifest
        .objects
        .iter()
        .flat_map(|object| object.segments.iter())
        .map(|segment| {
            Ok(SegmentAck {
                segment_id: cdf_kernel::SegmentId::new(segment.segment_id.clone())?,
                row_count: segment.row_count,
                byte_count: segment.byte_count,
            })
        })
        .collect()
}
