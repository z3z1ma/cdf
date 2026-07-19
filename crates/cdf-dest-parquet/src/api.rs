use crate::*;
use crate::{
    corrections::*,
    manifest::{
        CurrentReplacePointer, ParquetObjectEntry, ParquetObjectManifest,
        ParquetReplacePointerReceipt, ReplacePointer, canonical_json_bytes, sha256_hex,
    },
    receipts::{build_receipt, verify_receipt},
    runtime::parquet_runtime_capabilities,
    sheet::{parquet_protocol_capabilities, parquet_sheet},
    store::{
        ObjectKeyEncoder, StoreClient, current_pointer_key, now_ms, package_manifest_key,
        provenance_manifest_key, publication_attempt_target_prefix, replace_settlement_key,
    },
};

#[derive(Clone)]
pub struct ParquetDestination {
    store: StoreClient,
    execution: cdf_runtime::ExecutionServices,
    sheet: DestinationSheet,
    object_key_encoder: ObjectKeyEncoder,
    pub(crate) pending_corrections: Arc<Mutex<BTreeMap<PlanId, ParquetCorrectionContext>>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParquetRowLocation {
    pub object_key: String,
    pub row_ordinal: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct ParquetCommitRequest {
    pub(crate) commit: DestinationCommitRequest,
    pub(crate) schema_hash: SchemaHash,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ParquetCommitPlan {
    pub kernel: CommitPlan,
    pub manifest_key: String,
    pub provenance_manifest_key: String,
    pub replace_pointer_key: Option<String>,
    pub current_pointer_key: Option<String>,
    pub duplicate: bool,
    pub rows_planned: u64,
    pub bytes_planned: u64,
}

/// Proof that one exact immutable Parquet publication completed before checkpoint admission.
///
/// Construction is private to the publication protocol below: callers cannot synthesize
/// commit-bound verification from a receipt alone.
pub(crate) struct CommittedParquetPublication {
    receipt: Receipt,
    verification: ReceiptVerification,
}

impl CommittedParquetPublication {
    pub(crate) fn into_parts(self) -> (Receipt, ReceiptVerification) {
        (self.receipt, self.verification)
    }
}

pub type ReceiptVerification = cdf_kernel::ReceiptVerification;

impl ParquetDestination {
    pub fn destination_sheet() -> Result<DestinationSheet> {
        parquet_sheet()
    }

    pub fn destination_sheet_artifact() -> Result<cdf_kernel::DestinationSheetArtifact> {
        cdf_kernel::DestinationSheetArtifact::new(parquet_sheet()?, parquet_protocol_capabilities())
    }

    pub fn new_filesystem(
        root: impl AsRef<Path>,
        execution: cdf_runtime::ExecutionServices,
    ) -> Result<Self> {
        Self::from_store(StoreClient::new_filesystem(root.as_ref())?, execution)
    }

    pub fn new_object_store(
        namespace: cdf_kernel::ContentStoreNamespace,
        store: Arc<dyn ObjectStore>,
        root_prefix: impl Into<String>,
        execution: cdf_runtime::ExecutionServices,
    ) -> Result<Self> {
        Self::from_store(
            StoreClient::new_object_store(namespace, store, root_prefix)?,
            execution,
        )
    }

    fn from_store(store: StoreClient, execution: cdf_runtime::ExecutionServices) -> Result<Self> {
        execution.ensure_blocking_lanes(&parquet_runtime_capabilities().blocking_lanes)?;
        let artifact = Self::destination_sheet_artifact()?;
        let sheet = artifact.sheet;
        let protocol_capabilities = artifact.protocol_capabilities;
        let object_key_encoder = ObjectKeyEncoder::from_capabilities(&protocol_capabilities)?;
        Ok(Self {
            store,
            execution,
            sheet,
            object_key_encoder,
            pending_corrections: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    pub fn dry_plan_commit(
        request: &DestinationCommitRequest,
    ) -> Result<(DestinationSheet, CommitPlan)> {
        let sheet = parquet_sheet()?;
        let plan = plan_kernel_commit(&sheet, request)?;
        Ok((sheet, plan))
    }

    #[cfg(test)]
    pub(crate) fn plan_package_commit(
        &self,
        request: &ParquetCommitRequest,
        manifest_segments: &[SegmentEntry],
    ) -> Result<ParquetCommitPlan> {
        validate_manifest_requested_segments(&request.commit.segments, manifest_segments)?;
        let rows_planned = manifest_segments
            .iter()
            .map(|segment| segment.row_count)
            .sum();
        let bytes_planned = manifest_segments
            .iter()
            .map(|segment| segment.byte_count)
            .sum();
        self.plan_package_shape(request, rows_planned, bytes_planned)
    }

    pub fn verify_receipt(&self, receipt: &Receipt) -> Result<ReceiptVerification> {
        match verify_receipt(self, receipt) {
            Ok(()) => Ok(ReceiptVerification {
                verified: true,
                receipt_id: receipt.receipt_id.clone(),
                reason: None,
            }),
            Err(error) => Ok(ReceiptVerification {
                verified: false,
                receipt_id: receipt.receipt_id.clone(),
                reason: Some(error.to_string()),
            }),
        }
    }

    pub(crate) fn store(&self) -> &StoreClient {
        &self.store
    }

    pub(crate) fn execution(&self) -> &cdf_runtime::ExecutionServices {
        &self.execution
    }

    pub(crate) fn object_key_encoder(&self) -> ObjectKeyEncoder {
        self.object_key_encoder
    }

    pub fn reclaim_unreachable_content(
        &self,
        limit: u32,
    ) -> Result<cdf_runtime::ContentReclamationReport> {
        self.execution
            .reclaim_unreachable_content(limit, &self.store.content_deleter())
    }

    pub(crate) fn staging_cleanup_candidates(
        &self,
        target: &TargetName,
    ) -> Result<Vec<cdf_runtime::StagingCleanupCandidate>> {
        const MAX_METADATA_BYTES: u64 = 64 * 1024;
        let mut candidates = Vec::new();
        let staging_prefix = crate::store::staged_target_prefix(self.object_key_encoder, target);
        for object in self.store.list_prefix(self.execution(), &staging_prefix)? {
            if !object.key.ends_with("/attempt.json") {
                continue;
            }
            if object.byte_count > MAX_METADATA_BYTES {
                return Err(CdfError::data(format!(
                    "Parquet staging metadata {} exceeds {} bytes",
                    object.key, MAX_METADATA_BYTES
                )));
            }
            let metadata: crate::staging::StagingAttemptMetadata =
                serde_json::from_slice(&self.store.get_required(self.execution(), &object.key)?)
                    .map_err(|error| {
                        CdfError::data(format!(
                            "decode Parquet staging metadata {}: {error}",
                            object.key
                        ))
                    })?;
            let expected = crate::store::staged_attempt_metadata_key(
                self.object_key_encoder,
                target,
                metadata.staging_lease.authority_domain_id(),
                &metadata.staging_lease.identity.attempt_id,
                metadata.staging_lease.fencing_token(),
            );
            if object.key != expected
                || metadata.staging_lease.identity.target != *target
                || metadata.staging_lease.identity.destination_id != self.sheet.destination
            {
                return Err(CdfError::data(format!(
                    "Parquet staging metadata {} does not bind its exact lease namespace",
                    object.key
                )));
            }
            candidates.push(cdf_runtime::StagingCleanupCandidate::new(
                format!(
                    "parquet-staging:{}",
                    object.key.trim_end_matches("attempt.json")
                ),
                metadata.staging_lease,
            )?);
        }

        let publication_prefix = publication_attempt_target_prefix(self.object_key_encoder, target);
        for object in self
            .store
            .list_prefix(self.execution(), &publication_prefix)?
        {
            if object.byte_count > MAX_METADATA_BYTES {
                return Err(CdfError::data(format!(
                    "Parquet publication metadata {} exceeds {} bytes",
                    object.key, MAX_METADATA_BYTES
                )));
            }
            let metadata: crate::staging::PublicationAttemptMetadata =
                serde_json::from_slice(&self.store.get_required(self.execution(), &object.key)?)
                    .map_err(|error| {
                        CdfError::data(format!(
                            "decode Parquet publication metadata {}: {error}",
                            object.key
                        ))
                    })?;
            let expected_marker_prefix = format!(
                "{}{}/{}/{}/",
                publication_prefix,
                self.object_key_encoder
                    .encode(metadata.staging_lease.authority_domain_id().as_str()),
                self.object_key_encoder
                    .encode(metadata.staging_lease.identity.attempt_id.as_str()),
                metadata.staging_lease.fencing_token()
            );
            if !object.key.starts_with(&expected_marker_prefix)
                || !object.key.ends_with(".json")
                || metadata.staging_lease.identity.target != *target
                || metadata.staging_lease.identity.destination_id != self.sheet.destination
            {
                return Err(CdfError::data(format!(
                    "Parquet publication metadata {} does not bind its target namespace",
                    object.key
                )));
            }
            candidates.push(cdf_runtime::StagingCleanupCandidate::new(
                format!("parquet-publication:{}", object.key),
                metadata.staging_lease,
            )?);
        }
        Ok(candidates)
    }

    pub(crate) fn cleanup_expired_staging_candidate(
        &self,
        candidate: &cdf_runtime::StagingCleanupCandidate,
        proof: &cdf_runtime::ExpiredStagingLeaseProof,
        mutation_guard: &cdf_runtime::StagingMutationGuard,
    ) -> Result<u64> {
        if !proof.proves(candidate.lease()) {
            return Err(CdfError::contract(
                "Parquet staging cleanup proof does not bind the candidate lease generation",
            ));
        }
        proof.assert_cleanup_guard(mutation_guard)?;
        if let Some(prefix) = candidate.namespace().strip_prefix("parquet-staging:") {
            return self.store.delete_prefix_marker_last(
                self.execution(),
                prefix,
                &format!("{prefix}attempt.json"),
                mutation_guard,
            );
        }
        let marker = candidate
            .namespace()
            .strip_prefix("parquet-publication:")
            .ok_or_else(|| CdfError::contract("unknown Parquet staging cleanup namespace"))?;
        let metadata: crate::staging::PublicationAttemptMetadata = serde_json::from_slice(
            &self.store.get_required(self.execution(), marker)?,
        )
        .map_err(|error| {
            CdfError::data(format!(
                "decode Parquet publication metadata {marker}: {error}"
            ))
        })?;
        if !metadata.staging_lease.same_generation(candidate.lease()) {
            return Err(CdfError::contract(
                "Parquet publication marker changed after cleanup candidacy",
            ));
        }
        let reachability = self.execution().content_reachability_store()?;
        let root = reachability
            .root_intent(&metadata.root_id)?
            .ok_or_else(|| {
                CdfError::data("Parquet publication marker references a missing content root")
            })?;
        if root.root.root_generation != metadata.root_generation {
            return Err(CdfError::data(
                "Parquet publication marker references a different content root generation",
            ));
        }
        match root.state {
            cdf_kernel::ContentRootState::Committed => {}
            cdf_kernel::ContentRootState::Prepared
                if !self
                    .store
                    .exists(self.execution(), &metadata.manifest_key)? =>
            {
                reachability.abort_root(&metadata.root_id, metadata.root_generation)?;
            }
            cdf_kernel::ContentRootState::Prepared => {
                // The manifest exists, but only the destination's normal replay path has enough
                // typed commit authority to verify it and settle this root. Retain both rather
                // than guessing from object presence during cleanup.
                return Ok(0);
            }
        }
        proof.assert_cleanup_guard(mutation_guard)?;
        self.store.delete(self.execution(), marker)?;
        mutation_guard.assert_current()?;
        Ok(1)
    }

    pub(crate) fn plan_package_shape(
        &self,
        request: &ParquetCommitRequest,
        rows_planned: u64,
        bytes_planned: u64,
    ) -> Result<ParquetCommitPlan> {
        if !self
            .sheet
            .supported_dispositions
            .contains(&request.commit.disposition)
        {
            return Err(CdfError::contract(format!(
                "Parquet destination does not support {:?}",
                request.commit.disposition
            )));
        }

        let manifest_key = package_manifest_key(
            self.object_key_encoder(),
            &request.commit.target,
            &request.commit.idempotency_token,
        );
        let (replace_pointer_key, current_pointer_key) = match request.commit.disposition {
            _ if request.commit.is_data_noop() => (None, None),
            WriteDisposition::Replace => (
                Some(replace_settlement_key(
                    self.object_key_encoder(),
                    &request.commit.target,
                    &request.commit.idempotency_token,
                )),
                Some(current_pointer_key(
                    self.object_key_encoder(),
                    &request.commit.target,
                )),
            ),
            WriteDisposition::Append => (None, None),
            WriteDisposition::Merge | WriteDisposition::CdcApply => {
                return Err(CdfError::contract(
                    "Parquet destination supports append and replace only",
                ));
            }
        };
        let duplicate = self
            .store
            .exists(self.execution(), &manifest_key)
            .unwrap_or(false);

        Ok(ParquetCommitPlan {
            kernel: self.plan_commit(&request.commit)?,
            manifest_key,
            provenance_manifest_key: provenance_manifest_key(
                self.object_key_encoder(),
                &request.commit.target,
                &request.commit.package_hash,
            ),
            replace_pointer_key,
            current_pointer_key,
            duplicate,
            rows_planned,
            bytes_planned,
        })
    }

    pub(crate) fn existing_verified_manifest(
        &self,
        request: &ParquetCommitRequest,
        plan: &ParquetCommitPlan,
        mutation_guard: &cdf_runtime::StagingMutationGuard,
    ) -> Result<Option<LoadedManifest>> {
        let Some(mut loaded) = self.load_manifest_with_etag(&plan.manifest_key)? else {
            return Ok(None);
        };
        let provenance =
            ensure_provenance_manifest(self, request, &loaded.manifest, mutation_guard)?;
        if provenance != loaded.manifest {
            return Err(CdfError::destination(format!(
                "Parquet package-token manifest {} differs from its immutable provenance authority",
                plan.manifest_key
            )));
        }
        let replace_pointer =
            self.ensure_replace_settlement(request, plan, &loaded.manifest, mutation_guard)?;
        let receipt = build_receipt(
            request,
            plan,
            &loaded.manifest,
            loaded.manifest_etag.clone(),
            replace_pointer.clone(),
        )?;
        verify_receipt(self, &receipt).map_err(|error| {
            CdfError::destination(format!(
                "existing Parquet package-token manifest {} failed verification; refusing to overwrite: {error}",
                plan.manifest_key
            ))
        })?;
        loaded.replace_pointer = replace_pointer;
        Ok(Some(loaded))
    }

    fn ensure_replace_settlement(
        &self,
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
        let existing = self.store.get_optional(self.execution(), pointer_key)?;
        let stored = match existing {
            Some(_) => None,
            None => {
                self.ensure_current_replace_pointer(request, plan, manifest, mutation_guard)?;
                mutation_guard.assert_current()?;
                let stored = self.store.put_create_or_verify(
                    self.execution(),
                    pointer_key,
                    expected.clone(),
                )?;
                mutation_guard.assert_current()?;
                Some(stored)
            }
        };
        let bytes = self.store.get_required(self.execution(), pointer_key)?;
        mutation_guard.assert_current()?;
        if bytes != expected {
            return Err(CdfError::data(format!(
                "replace settlement {pointer_key} differs from its package authority"
            )));
        }
        let sha256 = sha256_hex(&bytes);
        let pointer: ReplacePointer = serde_json::from_slice(&bytes).map_err(|error| {
            CdfError::data(format!("parse replace pointer {pointer_key}: {error}"))
        })?;
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
        let etag = stored
            .and_then(|stored| stored.e_tag)
            .or(self.store.etag(self.execution(), pointer_key)?);
        Ok(Some(ParquetReplacePointerReceipt {
            key: pointer_key.clone(),
            sha256,
            etag,
        }))
    }

    fn ensure_current_replace_pointer(
        &self,
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
            let current = self
                .store
                .get_optional_versioned(self.execution(), current_key)?;
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
                pointer.generation.checked_add(1).ok_or_else(|| {
                    CdfError::destination("Parquet replace generation exhausted u64")
                })
            })?;
            let replacement = canonical_json_bytes(&current_replace_pointer(
                request, plan, manifest, generation,
            )?)?;
            let outcome = self.store.compare_and_swap(
                self.execution(),
                current_key,
                current.as_ref(),
                replacement.clone(),
            )?;
            mutation_guard.assert_current()?;
            match outcome {
                crate::store::CompareAndSwapOutcome::Written(_) => {
                    let readback = self.store.get_required(self.execution(), current_key)?;
                    mutation_guard.assert_current()?;
                    if readback != replacement {
                        return Err(CdfError::destination(format!(
                            "current replace pointer {current_key} changed before exact readback"
                        )));
                    }
                    return Ok(());
                }
                crate::store::CompareAndSwapOutcome::Conflict => continue,
            }
        }
        Err(CdfError::destination(format!(
            "current replace pointer {current_key} remained contended after 32 conditional updates"
        )))
    }

    fn load_manifest(&self, key: &str) -> Result<Option<ParquetObjectManifest>> {
        self.load_manifest_with_etag(key)
            .map(|loaded| loaded.map(|loaded| loaded.manifest))
    }

    pub fn resolve_row_provenance(
        &self,
        target: &TargetName,
        address: &RowProvenanceAddress,
    ) -> Result<Option<ParquetRowLocation>> {
        let key = provenance_manifest_key(
            self.object_key_encoder(),
            target,
            &address.original_package_hash,
        );
        let Some(manifest) = self.load_manifest(&key)? else {
            return Ok(None);
        };
        if manifest.target != target.as_str()
            || manifest.package_hash != address.original_package_hash.as_str()
        {
            return Err(CdfError::data(format!(
                "Parquet provenance manifest {key} contradicts its target/package key"
            )));
        }
        let Some((object, segment)) = manifest.objects.iter().find_map(|object| {
            object
                .segments
                .iter()
                .find(|segment| segment.segment_id == address.original_segment_id.as_str())
                .map(|segment| (object, segment))
        }) else {
            return Ok(None);
        };
        if address.original_row_ordinal >= segment.row_count {
            return Ok(None);
        }
        Ok(Some(ParquetRowLocation {
            object_key: object.key.clone(),
            row_ordinal: segment
                .row_offset
                .checked_add(address.original_row_ordinal)
                .ok_or_else(|| CdfError::data("Parquet provenance row ordinal overflow"))?,
        }))
    }

    fn load_manifest_with_etag(&self, key: &str) -> Result<Option<LoadedManifest>> {
        let Some(bytes) = self.store.get_optional(self.execution(), key)? else {
            return Ok(None);
        };
        let manifest = serde_json::from_slice(&bytes).map_err(|error| {
            CdfError::data(format!("parse Parquet object manifest {key}: {error}"))
        })?;
        let manifest_etag = self.store.etag(self.execution(), key)?;
        Ok(Some(LoadedManifest {
            manifest,
            manifest_etag,
            replace_pointer: None,
        }))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct LoadedManifest {
    pub(crate) manifest: ParquetObjectManifest,
    pub(crate) manifest_etag: Option<String>,
    pub(crate) replace_pointer: Option<ParquetReplacePointerReceipt>,
}

pub(crate) fn duplicate_parquet_receipt(
    request: ParquetCommitRequest,
    plan: ParquetCommitPlan,
    existing: LoadedManifest,
) -> Result<Receipt> {
    let receipt = build_receipt(
        &request,
        &plan,
        &existing.manifest,
        existing.manifest_etag,
        existing.replace_pointer,
    )?;
    Ok(receipt)
}

pub(crate) fn finalize_parquet_objects(
    destination: &ParquetDestination,
    request: ParquetCommitRequest,
    plan: ParquetCommitPlan,
    object_entries: Vec<ParquetObjectEntry>,
    mutation_guard: &cdf_runtime::StagingMutationGuard,
) -> Result<CommittedParquetPublication> {
    mutation_guard.assert_current()?;
    let committed_at_ms = now_ms()?;
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
    let manifest_put = destination.store.put_create_or_verify(
        &destination.execution,
        &plan.manifest_key,
        manifest_bytes.clone(),
    )?;
    mutation_guard.assert_current()?;
    let replace_pointer =
        destination.ensure_replace_settlement(&request, &plan, &object_manifest, mutation_guard)?;
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
        .store
        .put_create(destination.execution(), &key, bytes)?;
    mutation_guard.assert_current()?;
    match outcome {
        crate::store::CreateObjectOutcome::Created(_) => Ok(manifest.clone()),
        crate::store::CreateObjectOutcome::AlreadyExists => {
            let existing_bytes = destination
                .store
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

impl DestinationProtocol for ParquetDestination {
    fn sheet(&self) -> &DestinationSheet {
        &self.sheet
    }

    fn protocol_capabilities(&self) -> cdf_kernel::DestinationProtocolCapabilities {
        parquet_protocol_capabilities()
    }

    fn plan_commit(&self, request: &DestinationCommitRequest) -> Result<CommitPlan> {
        plan_kernel_commit(&self.sheet, request)
    }

    fn verify(&self, receipt: &Receipt) -> Result<ReceiptVerification> {
        self.verify_receipt(receipt)
    }

    fn plan_correction(
        &self,
        request: &DestinationCorrectionCommitRequest,
    ) -> Result<DestinationCorrectionCommitPlan> {
        plan_correction_request(self, request)
    }

    fn begin_correction(
        &self,
        request: DestinationCorrectionCommitRequest,
        plan: DestinationCorrectionCommitPlan,
    ) -> Result<Box<dyn CorrectionCommitSession + '_>> {
        begin_correction_request(self, request, plan)
    }

    fn verify_correction(&self, receipt: &Receipt) -> Result<ReceiptVerification> {
        verify_correction_receipt(self, receipt)
    }
}

fn plan_kernel_commit(
    sheet: &DestinationSheet,
    request: &DestinationCommitRequest,
) -> Result<CommitPlan> {
    if !sheet.supported_dispositions.contains(&request.disposition) {
        return Err(CdfError::contract(format!(
            "Parquet destination does not support {:?}",
            request.disposition
        )));
    }
    Ok(CommitPlan {
        plan_id: PlanId::new(format!(
            "parquet:{}:{}",
            request.target.as_str(),
            request.idempotency_token.as_str()
        ))?,
        target: request.target.clone(),
        disposition: request.disposition.clone(),
        idempotency: IdempotencySupport::PackageToken,
        migrations: Vec::new(),
        delivery_guarantee: match request.disposition {
            WriteDisposition::Append => DeliveryGuarantee::EffectivelyOncePerPackage,
            WriteDisposition::Replace => DeliveryGuarantee::EffectivelyOncePerTarget,
            WriteDisposition::Merge | WriteDisposition::CdcApply => {
                return Err(CdfError::contract(
                    "Parquet destination supports append and replace only",
                ));
            }
        },
    })
}

#[cfg(test)]
fn validate_manifest_requested_segments(
    requested: &[StateSegment],
    manifest: &[SegmentEntry],
) -> Result<()> {
    let package = manifest
        .iter()
        .map(|segment| (segment.segment_id.clone(), segment.row_count))
        .collect::<BTreeMap<_, _>>();
    if package.len() != manifest.len() {
        return Err(CdfError::data(
            "package manifest contains duplicate segment ids",
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    for state in requested {
        if !seen.insert(state.segment_id.clone()) {
            return Err(CdfError::data(format!(
                "destination commit request contains duplicate segment {}",
                state.segment_id
            )));
        }
        match package.get(&state.segment_id) {
            Some(row_count) if *row_count == state.row_count => {}
            Some(row_count) => {
                return Err(CdfError::data(format!(
                    "requested segment {} has {} rows but package manifest has {row_count}",
                    state.segment_id, state.row_count
                )));
            }
            None => {
                return Err(CdfError::data(format!(
                    "destination commit request segment {} is not present in the package manifest",
                    state.segment_id
                )));
            }
        }
    }
    if seen.len() != package.len() {
        return Err(CdfError::data(
            "package manifest segments are not fully covered by destination commit request",
        ));
    }
    Ok(())
}
