use crate::*;
use crate::{
    corrections::*,
    manifest::{
        ParquetObjectEntry, ParquetObjectManifest, ParquetReplacePointerReceipt, ReplacePointer,
        canonical_json_bytes, sha256_hex,
    },
    receipts::{build_receipt, verify_receipt},
    runtime::parquet_runtime_capabilities,
    sheet::{parquet_protocol_capabilities, parquet_sheet},
    store::{
        ObjectKeyEncoder, StoreClient, now_ms, package_manifest_key, provenance_manifest_key,
        replace_pointer_key, segment_object_key,
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
    pub object_keys: Vec<String>,
    pub duplicate: bool,
    pub rows_planned: u64,
    pub bytes_planned: u64,
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
        store: Arc<dyn ObjectStore>,
        root_prefix: impl Into<String>,
        execution: cdf_runtime::ExecutionServices,
    ) -> Result<Self> {
        Self::from_store(
            StoreClient::new_object_store(store, root_prefix)?,
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
        let segment_ids = manifest_segments
            .iter()
            .map(|segment| segment.segment_id.clone())
            .collect::<Vec<_>>();
        self.plan_package_shape(request, &segment_ids, rows_planned, bytes_planned)
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

    pub(crate) fn plan_package_shape(
        &self,
        request: &ParquetCommitRequest,
        segment_ids: &[SegmentId],
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
        let replace_pointer_key = match request.commit.disposition {
            _ if request.commit.is_data_noop() => None,
            WriteDisposition::Replace => Some(replace_pointer_key(
                self.object_key_encoder(),
                &request.commit.target,
            )),
            WriteDisposition::Append => None,
            WriteDisposition::Merge | WriteDisposition::CdcApply => {
                return Err(CdfError::contract(
                    "Parquet destination supports append and replace only",
                ));
            }
        };
        let object_keys = segment_ids
            .iter()
            .map(|segment_id| {
                segment_object_key(
                    self.object_key_encoder(),
                    &request.commit.target,
                    &request.commit.idempotency_token,
                    segment_id,
                )
            })
            .collect::<Vec<_>>();
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
            object_keys,
            duplicate,
            rows_planned,
            bytes_planned,
        })
    }

    pub(crate) fn existing_verified_manifest(
        &self,
        request: &ParquetCommitRequest,
        plan: &ParquetCommitPlan,
    ) -> Result<Option<LoadedManifest>> {
        let Some(mut loaded) = self.load_manifest_with_etag(&plan.manifest_key)? else {
            return Ok(None);
        };
        ensure_provenance_manifest(self, request, &loaded.manifest)?;
        let replace_pointer = self.load_replace_pointer_receipt(request, plan, &loaded.manifest)?;
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

    fn load_replace_pointer_receipt(
        &self,
        request: &ParquetCommitRequest,
        plan: &ParquetCommitPlan,
        manifest: &ParquetObjectManifest,
    ) -> Result<Option<ParquetReplacePointerReceipt>> {
        let Some(pointer_key) = &plan.replace_pointer_key else {
            return Ok(None);
        };
        let bytes = self.store.get_required(self.execution(), pointer_key)?;
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
        let etag = self.store.etag(self.execution(), pointer_key)?;
        Ok(Some(ParquetReplacePointerReceipt {
            key: pointer_key.clone(),
            sha256,
            etag,
        }))
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
        let Some(object) = manifest
            .objects
            .iter()
            .find(|object| object.segment_id == address.original_segment_id.as_str())
        else {
            return Ok(None);
        };
        if address.original_row_ordinal >= object.row_count {
            return Ok(None);
        }
        Ok(Some(ParquetRowLocation {
            object_key: object.key.clone(),
            row_ordinal: address.original_row_ordinal,
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
) -> Result<Receipt> {
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
    let manifest_bytes = canonical_json_bytes(&object_manifest)?;
    let manifest_sha256 = sha256_hex(&manifest_bytes);
    let manifest_put = destination.store.put(
        &destination.execution,
        &plan.manifest_key,
        manifest_bytes.clone(),
    )?;
    ensure_provenance_manifest(destination, &request, &object_manifest)?;
    let replace_pointer = if let Some(pointer_key) = &plan.replace_pointer_key {
        let pointer = ReplacePointer {
            pointer_version: REPLACE_POINTER_VERSION,
            target: request.commit.target.as_str().to_owned(),
            package_hash: request.commit.package_hash.as_str().to_owned(),
            idempotency_token: request.commit.idempotency_token.as_str().to_owned(),
            schema_hash: request.schema_hash.as_str().to_owned(),
            manifest_key: plan.manifest_key.clone(),
            manifest_sha256,
            updated_at_ms: committed_at_ms,
        };
        let pointer_bytes = canonical_json_bytes(&pointer)?;
        let pointer_sha256 = sha256_hex(&pointer_bytes);
        let put = destination
            .store
            .put(&destination.execution, pointer_key, pointer_bytes)?;
        Some(ParquetReplacePointerReceipt {
            key: pointer_key.clone(),
            sha256: pointer_sha256,
            etag: put.e_tag,
        })
    } else {
        None
    };
    let persisted_manifest = destination
        .load_manifest(&plan.manifest_key)?
        .ok_or_else(|| CdfError::destination("Parquet object manifest was not written"))?;
    let receipt = build_receipt(
        &request,
        &plan,
        &persisted_manifest,
        manifest_put.e_tag,
        replace_pointer,
    )?;
    Ok(receipt)
}

fn ensure_provenance_manifest(
    destination: &ParquetDestination,
    request: &ParquetCommitRequest,
    manifest: &ParquetObjectManifest,
) -> Result<()> {
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
    destination.store.put_create_or_verify(
        destination.execution(),
        &key,
        canonical_json_bytes(manifest)?,
    )?;
    Ok(())
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
    let mut seen = BTreeSet::new();
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
