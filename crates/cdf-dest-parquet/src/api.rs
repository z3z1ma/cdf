use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex},
};

#[cfg(test)]
use cdf_kernel::StateSegment;
use cdf_kernel::{
    CdfError, CommitPlan, CorrectionCommitSession, DeliveryGuarantee, DestinationCommitRequest,
    DestinationCorrectionCommitPlan, DestinationCorrectionCommitRequest, DestinationProtocol,
    DestinationSheet, IdempotencySupport, PlanId, Receipt, Result, RowProvenanceAddress,
    TargetName, WriteDisposition,
};
#[cfg(test)]
use cdf_package_contract::SegmentEntry;
use object_store::ObjectStore;

use crate::{
    compression::ParquetCompression,
    corrections::{begin_correction_request, plan_correction_request, verify_correction_receipt},
    layout::ParquetObjectLayoutPolicy,
    models::{
        ParquetCommitPlan, ParquetCommitRequest, ParquetDestination, ParquetRowLocation,
        PublicationAttemptMetadata, ReceiptVerification, StagingAttemptMetadata,
    },
    publication::load_manifest,
    receipts::verify_receipt,
    runtime::parquet_runtime_capabilities,
    sheet::{parquet_protocol_capabilities, parquet_sheet},
    store::{
        ObjectKeyEncoder, StoreClient, current_pointer_key, package_manifest_key,
        provenance_manifest_key, publication_attempt_target_prefix, replace_settlement_key,
    },
};

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
        Self::from_store(
            StoreClient::new_filesystem(root.as_ref())?,
            execution,
            ParquetCompression::default(),
            ParquetObjectLayoutPolicy::default(),
        )
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
            ParquetCompression::default(),
            ParquetObjectLayoutPolicy::default(),
        )
    }

    pub fn with_compression(mut self, compression: ParquetCompression) -> Self {
        self.compression = compression;
        self
    }

    pub fn with_object_layout_policy(
        mut self,
        object_layout: ParquetObjectLayoutPolicy,
    ) -> Result<Self> {
        self.object_layout = object_layout.validate()?;
        Ok(self)
    }

    fn from_store(
        store: StoreClient,
        execution: cdf_runtime::ExecutionServices,
        compression: ParquetCompression,
        object_layout: ParquetObjectLayoutPolicy,
    ) -> Result<Self> {
        let object_layout = object_layout.validate()?;
        execution
            .ensure_blocking_lanes(&parquet_runtime_capabilities(compression).blocking_lanes)?;
        let artifact = Self::destination_sheet_artifact()?;
        let sheet = artifact.sheet;
        let protocol_capabilities = artifact.protocol_capabilities;
        let object_key_encoder = ObjectKeyEncoder::from_capabilities(&protocol_capabilities)?;
        Ok(Self {
            store,
            execution,
            sheet,
            object_key_encoder,
            compression,
            object_layout,
            pending_corrections: Arc::new(Mutex::new(BTreeMap::new())),
            #[cfg(test)]
            encode_probe: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn set_encode_probe(
        &mut self,
        probe: Arc<crate::models::ParquetEncodeConcurrencyProbe>,
    ) {
        self.encode_probe = Some(probe);
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

    pub(crate) fn rebind_execution_services(
        &mut self,
        execution: &cdf_runtime::ExecutionServices,
    ) -> Result<()> {
        self.execution = execution.clone();
        Ok(())
    }

    pub(crate) fn object_key_encoder(&self) -> ObjectKeyEncoder {
        self.object_key_encoder
    }

    pub(crate) fn compression(&self) -> ParquetCompression {
        self.compression
    }

    pub(crate) fn object_layout_policy(&self) -> ParquetObjectLayoutPolicy {
        self.object_layout
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
            let metadata: StagingAttemptMetadata =
                serde_json::from_slice(&self.store.get_required(self.execution(), &object.key)?)
                    .map_err(|error| {
                        CdfError::data(format!(
                            "decode Parquet staging metadata {}: {error}",
                            object.key
                        ))
                    })?;
            metadata.validate()?;
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
            let metadata: PublicationAttemptMetadata =
                serde_json::from_slice(&self.store.get_required(self.execution(), &object.key)?)
                    .map_err(|error| {
                        CdfError::data(format!(
                            "decode Parquet publication metadata {}: {error}",
                            object.key
                        ))
                    })?;
            metadata.validate()?;
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
        let metadata: PublicationAttemptMetadata = serde_json::from_slice(
            &self.store.get_required(self.execution(), marker)?,
        )
        .map_err(|error| {
            CdfError::data(format!(
                "decode Parquet publication metadata {marker}: {error}"
            ))
        })?;
        metadata.validate()?;
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
        let Some(manifest) = load_manifest(self, &key)? else {
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
