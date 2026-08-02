use cdf_kernel::{CdfError, Receipt, Result};

use crate::{
    DESTINATION_ID, MANIFEST_VERSION, REPLACE_POINTER_VERSION,
    manifest::{
        CurrentReplacePointer, ParquetObjectEntry, ParquetObjectManifest,
        ParquetReplacePointerReceipt, ReplacePointer, canonical_json_bytes, sha256_hex,
    },
    models::{
        CommittedParquetPublication, LoadedManifest, ParquetCommitPlan, ParquetCommitRequest,
        ParquetDestination, ReceiptVerification,
    },
    receipts::{build_receipt, verify_receipt},
    store::{CompareAndSwapOutcome, CreateObjectOutcome, now_ms, provenance_manifest_key},
};

pub(crate) fn existing_verified_manifest(
    destination: &ParquetDestination,
    request: &ParquetCommitRequest,
    plan: &ParquetCommitPlan,
    mutation_guard: &cdf_runtime::StagingMutationGuard,
) -> Result<Option<LoadedManifest>> {
    let Some(mut loaded) = load_manifest_with_etag(destination, &plan.manifest_key)? else {
        return Ok(None);
    };
    let provenance =
        ensure_provenance_manifest(destination, request, &loaded.manifest, mutation_guard)?;
    if provenance != loaded.manifest {
        return Err(CdfError::destination(format!(
            "Parquet package-token manifest {} differs from its immutable provenance authority",
            plan.manifest_key
        )));
    }
    let replace_pointer =
        ensure_replace_settlement(destination, request, plan, &loaded.manifest, mutation_guard)?;
    let receipt = build_receipt(
        request,
        plan,
        &loaded.manifest,
        loaded.manifest_etag.clone(),
        replace_pointer.clone(),
    )?;
    verify_receipt(destination, &receipt).map_err(|error| {
        CdfError::destination(format!(
            "existing Parquet package-token manifest {} failed verification; refusing to overwrite: {error}",
            plan.manifest_key
        ))
    })?;
    loaded.replace_pointer = replace_pointer;
    Ok(Some(loaded))
}

pub(crate) fn duplicate_parquet_receipt(
    request: ParquetCommitRequest,
    plan: ParquetCommitPlan,
    existing: LoadedManifest,
) -> Result<Receipt> {
    build_receipt(
        &request,
        &plan,
        &existing.manifest,
        existing.manifest_etag,
        existing.replace_pointer,
    )
}

pub(crate) fn finalize_parquet_objects(
    destination: &ParquetDestination,
    request: ParquetCommitRequest,
    plan: ParquetCommitPlan,
    object_entries: Vec<ParquetObjectEntry>,
    mutation_guard: &cdf_runtime::StagingMutationGuard,
) -> Result<CommittedParquetPublication> {
    mutation_guard.assert_current()?;
    let committed_at_ms = now_ms(destination.execution())?;
    let object_manifest = ParquetObjectManifest {
        manifest_version: MANIFEST_VERSION,
        destination: DESTINATION_ID.to_owned(),
        target: request.commit.target.as_str().to_owned(),
        package_hash: request.commit.package_hash.as_str().to_owned(),
        idempotency_token: request.commit.idempotency_token.as_str().to_owned(),
        disposition: request.commit.disposition.clone(),
        schema_hash: request.schema_hash.as_str().to_owned(),
        committed_at_ms,
        total_rows: plan.rows_planned,
        objects: object_entries,
    };
    // The provenance key is create-only and selects the authoritative bytes when same-token
    // writers race. This makes the later package manifest byte-identical for every contender,
    // including its recorded commit time.
    let object_manifest =
        ensure_provenance_manifest(destination, &request, &object_manifest, mutation_guard)?;
    let manifest_bytes = canonical_json_bytes(&object_manifest)?;
    mutation_guard.assert_current()?;
    let manifest_put = destination.store().put_create_or_verify(
        destination.execution(),
        &plan.manifest_key,
        manifest_bytes,
    )?;
    mutation_guard.assert_current()?;
    let replace_pointer = ensure_replace_settlement(
        destination,
        &request,
        &plan,
        &object_manifest,
        mutation_guard,
    )?;
    let receipt = build_receipt(
        &request,
        &plan,
        &object_manifest,
        manifest_put.e_tag,
        replace_pointer,
    )?;
    let verification = ReceiptVerification {
        verified: true,
        receipt_id: receipt.receipt_id.clone(),
        reason: None,
    };
    Ok(CommittedParquetPublication {
        receipt,
        verification,
    })
}

pub(crate) fn load_manifest(
    destination: &ParquetDestination,
    key: &str,
) -> Result<Option<ParquetObjectManifest>> {
    load_manifest_with_etag(destination, key).map(|loaded| loaded.map(|loaded| loaded.manifest))
}

fn load_manifest_with_etag(
    destination: &ParquetDestination,
    key: &str,
) -> Result<Option<LoadedManifest>> {
    let Some(bytes) = destination
        .store()
        .get_optional(destination.execution(), key)?
    else {
        return Ok(None);
    };
    let manifest = serde_json::from_slice(&bytes)
        .map_err(|error| CdfError::data(format!("parse Parquet object manifest {key}: {error}")))?;
    let manifest_etag = destination.store().etag(destination.execution(), key)?;
    Ok(Some(LoadedManifest {
        manifest,
        manifest_etag,
        replace_pointer: None,
    }))
}

fn ensure_replace_settlement(
    destination: &ParquetDestination,
    request: &ParquetCommitRequest,
    plan: &ParquetCommitPlan,
    manifest: &ParquetObjectManifest,
    mutation_guard: &cdf_runtime::StagingMutationGuard,
) -> Result<Option<ParquetReplacePointerReceipt>> {
    let Some(pointer_key) = &plan.replace_pointer_key else {
        return Ok(None);
    };
    let pointer = replace_pointer(request, plan, manifest)?;
    let expected = canonical_json_bytes(&pointer)?;
    let existing = destination
        .store()
        .get_optional(destination.execution(), pointer_key)?;
    let stored = match existing {
        Some(_) => None,
        None => {
            ensure_current_replace_pointer(destination, request, plan, manifest, mutation_guard)?;
            mutation_guard.assert_current()?;
            let stored = destination.store().put_create_or_verify(
                destination.execution(),
                pointer_key,
                expected.clone(),
            )?;
            mutation_guard.assert_current()?;
            Some(stored)
        }
    };
    let bytes = destination
        .store()
        .get_required(destination.execution(), pointer_key)?;
    mutation_guard.assert_current()?;
    if bytes != expected {
        return Err(CdfError::data(format!(
            "replace settlement {pointer_key} differs from its package authority"
        )));
    }
    let sha256 = sha256_hex(&bytes);
    let pointer: ReplacePointer = serde_json::from_slice(&bytes)
        .map_err(|error| CdfError::data(format!("parse replace pointer {pointer_key}: {error}")))?;
    let manifest_sha256 = sha256_hex(&canonical_json_bytes(manifest)?);
    if pointer.manifest_key != plan.manifest_key
        || pointer.manifest_sha256 != manifest_sha256
        || pointer.target != request.commit.target.as_str()
        || pointer.package_hash != request.commit.package_hash.as_str()
        || pointer.idempotency_token != request.commit.idempotency_token.as_str()
        || pointer.schema_hash != request.schema_hash.as_str()
    {
        return Err(CdfError::data(format!(
            "replace pointer {pointer_key} does not point at package-token manifest {}",
            plan.manifest_key
        )));
    }
    let etag = stored.and_then(|stored| stored.e_tag).or(destination
        .store()
        .etag(destination.execution(), pointer_key)?);
    Ok(Some(ParquetReplacePointerReceipt {
        key: pointer_key.clone(),
        sha256,
        etag,
    }))
}

fn ensure_current_replace_pointer(
    destination: &ParquetDestination,
    request: &ParquetCommitRequest,
    plan: &ParquetCommitPlan,
    manifest: &ParquetObjectManifest,
    mutation_guard: &cdf_runtime::StagingMutationGuard,
) -> Result<()> {
    let Some(current_key) = &plan.current_pointer_key else {
        return Ok(());
    };
    for _ in 0..32 {
        mutation_guard.assert_current()?;
        let current = destination
            .store()
            .get_optional_versioned(destination.execution(), current_key)?;
        let observed = current
            .as_ref()
            .map(|current| parse_current_replace_pointer(current_key, &current.bytes))
            .transpose()?;
        if observed
            .as_ref()
            .map(|pointer| current_pointer_binds(pointer, request, plan, manifest))
            .transpose()?
            .unwrap_or(false)
        {
            return Ok(());
        }
        let generation = observed.as_ref().map_or(Ok(1), |pointer| {
            pointer
                .generation
                .checked_add(1)
                .ok_or_else(|| CdfError::destination("Parquet replace generation exhausted u64"))
        })?;
        let replacement = canonical_json_bytes(&current_replace_pointer(
            request, plan, manifest, generation,
        )?)?;
        let outcome = destination.store().compare_and_swap(
            destination.execution(),
            current_key,
            current.as_ref(),
            replacement.clone(),
        )?;
        mutation_guard.assert_current()?;
        match outcome {
            CompareAndSwapOutcome::Written(_) => {
                let readback = destination
                    .store()
                    .get_required(destination.execution(), current_key)?;
                mutation_guard.assert_current()?;
                if readback != replacement {
                    return Err(CdfError::destination(format!(
                        "current replace pointer {current_key} changed before exact readback"
                    )));
                }
                return Ok(());
            }
            CompareAndSwapOutcome::Conflict => continue,
        }
    }
    Err(CdfError::destination(format!(
        "current replace pointer {current_key} remained contended after 32 conditional updates"
    )))
}

fn replace_pointer(
    request: &ParquetCommitRequest,
    plan: &ParquetCommitPlan,
    manifest: &ParquetObjectManifest,
) -> Result<ReplacePointer> {
    let manifest_sha256 = sha256_hex(&canonical_json_bytes(manifest)?);
    Ok(ReplacePointer {
        pointer_version: REPLACE_POINTER_VERSION,
        target: request.commit.target.as_str().to_owned(),
        package_hash: request.commit.package_hash.as_str().to_owned(),
        idempotency_token: request.commit.idempotency_token.as_str().to_owned(),
        schema_hash: request.schema_hash.as_str().to_owned(),
        manifest_key: plan.manifest_key.clone(),
        manifest_sha256,
        updated_at_ms: manifest.committed_at_ms,
    })
}

fn current_replace_pointer(
    request: &ParquetCommitRequest,
    plan: &ParquetCommitPlan,
    manifest: &ParquetObjectManifest,
    generation: u64,
) -> Result<CurrentReplacePointer> {
    let settlement_key = plan.replace_pointer_key.as_ref().ok_or_else(|| {
        CdfError::internal("replace current-pointer construction requires a settlement key")
    })?;
    Ok(CurrentReplacePointer {
        pointer_version: REPLACE_POINTER_VERSION,
        generation,
        target: request.commit.target.as_str().to_owned(),
        package_hash: request.commit.package_hash.as_str().to_owned(),
        idempotency_token: request.commit.idempotency_token.as_str().to_owned(),
        schema_hash: request.schema_hash.as_str().to_owned(),
        manifest_key: plan.manifest_key.clone(),
        manifest_sha256: sha256_hex(&canonical_json_bytes(manifest)?),
        settlement_key: settlement_key.clone(),
    })
}

fn parse_current_replace_pointer(key: &str, bytes: &[u8]) -> Result<CurrentReplacePointer> {
    serde_json::from_slice(bytes)
        .map_err(|error| CdfError::data(format!("parse current replace pointer {key}: {error}")))
}

fn current_pointer_binds(
    pointer: &CurrentReplacePointer,
    request: &ParquetCommitRequest,
    plan: &ParquetCommitPlan,
    manifest: &ParquetObjectManifest,
) -> Result<bool> {
    Ok(pointer.pointer_version == REPLACE_POINTER_VERSION
        && pointer.target == request.commit.target.as_str()
        && pointer.package_hash == request.commit.package_hash.as_str()
        && pointer.idempotency_token == request.commit.idempotency_token.as_str()
        && pointer.schema_hash == request.schema_hash.as_str()
        && pointer.manifest_key == plan.manifest_key
        && pointer.manifest_sha256 == sha256_hex(&canonical_json_bytes(manifest)?)
        && plan
            .replace_pointer_key
            .as_ref()
            .is_some_and(|key| pointer.settlement_key == *key))
}

fn ensure_provenance_manifest(
    destination: &ParquetDestination,
    request: &ParquetCommitRequest,
    manifest: &ParquetObjectManifest,
    mutation_guard: &cdf_runtime::StagingMutationGuard,
) -> Result<ParquetObjectManifest> {
    if manifest.target != request.commit.target.as_str()
        || manifest.package_hash != request.commit.package_hash.as_str()
    {
        return Err(CdfError::data(
            "Parquet object manifest cannot bind a different target/package provenance key",
        ));
    }
    let key = provenance_manifest_key(
        destination.object_key_encoder(),
        &request.commit.target,
        &request.commit.package_hash,
    );
    let bytes = canonical_json_bytes(manifest)?;
    mutation_guard.assert_current()?;
    let outcome = destination
        .store()
        .put_create(destination.execution(), &key, bytes)?;
    mutation_guard.assert_current()?;
    match outcome {
        CreateObjectOutcome::Created(_) => Ok(manifest.clone()),
        CreateObjectOutcome::AlreadyExists => {
            let existing_bytes = destination
                .store()
                .get_required(destination.execution(), &key)?;
            let existing: ParquetObjectManifest =
                serde_json::from_slice(&existing_bytes).map_err(|error| {
                    CdfError::data(format!(
                        "parse immutable Parquet provenance manifest {key}: {error}"
                    ))
                })?;
            let mut candidate = manifest.clone();
            candidate.committed_at_ms = existing.committed_at_ms;
            if candidate != existing {
                return Err(CdfError::destination(format!(
                    "immutable Parquet provenance manifest {key} already binds different publication bytes"
                )));
            }
            Ok(existing)
        }
    }
}
