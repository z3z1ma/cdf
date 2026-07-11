use crate::store::ObjectKeyEncoder;
use crate::*;
use crate::{
    manifest::{
        ParquetCorrectionSidecar, ParquetCorrectionSidecarManifest, ParquetCorrectionSidecarObject,
        canonical_json_bytes, sha256_hex,
    },
    sheet::parquet_correction_capabilities,
    store::{
        CreateObjectOutcome, correction_receipt_key, correction_sidecar_manifest_key,
        correction_sidecar_object_key, now_ms, replace_pointer_key, version_manifest_key,
    },
};

#[derive(Clone, Debug)]
pub(crate) struct ParquetCorrectionContext {
    pub(crate) request: DestinationCorrectionCommitRequest,
    pub(crate) plan: DestinationCorrectionCommitPlan,
    pub(crate) sidecar_bytes: Vec<u8>,
    pub(crate) manifest: ParquetCorrectionSidecarManifest,
    pub(crate) manifest_bytes: Vec<u8>,
    pub(crate) manifest_key: String,
    pub(crate) manifest_sha256: String,
    pub(crate) receipt_key: String,
    pub(crate) duplicate_receipt: Option<Receipt>,
}

struct ParquetCorrectionSession<'a> {
    destination: &'a ParquetDestination,
    context: ParquetCorrectionContext,
    migrations_applied: bool,
    corrections_applied: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct ParquetVersionedRematerializationRequest {
    pub promotion_id: PromotionId,
    pub target: TargetName,
    pub correction_package_hash: PackageHash,
    pub required_source_packages: Vec<PackageHash>,
    pub target_version: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct ParquetVersionedRematerializationPlan {
    pub promotion_id: PromotionId,
    pub target: TargetName,
    pub correction_package_hash: PackageHash,
    pub required_source_packages: Vec<PackageHash>,
    pub target_version: String,
    pub target_manifest_key: String,
    pub target_pointer_key: String,
    pub atomic_pointer_advance: CapabilitySupport,
    pub executable: bool,
    pub unsupported_reason: String,
}

impl ParquetVersionedRematerializationRequest {
    pub fn new(
        promotion_id: PromotionId,
        target: TargetName,
        correction_package_hash: PackageHash,
        required_source_packages: Vec<PackageHash>,
        target_version: impl Into<String>,
    ) -> Self {
        Self {
            promotion_id,
            target,
            correction_package_hash,
            required_source_packages,
            target_version: target_version.into(),
        }
    }
}

impl ParquetDestination {
    pub fn plan_versioned_rematerialization(
        &self,
        request: ParquetVersionedRematerializationRequest,
    ) -> Result<ParquetVersionedRematerializationPlan> {
        if request.required_source_packages.is_empty() {
            return Err(CdfError::contract(
                "Parquet versioned rematerialization requires at least one verified source package",
            ));
        }
        let mut packages = BTreeSet::new();
        for package in &request.required_source_packages {
            if !packages.insert(package) {
                return Err(CdfError::contract(format!(
                    "Parquet versioned rematerialization repeats source package {package}"
                )));
            }
        }
        if request.target_version.trim().is_empty() {
            return Err(CdfError::contract(
                "Parquet versioned rematerialization target_version cannot be empty",
            ));
        }
        Ok(ParquetVersionedRematerializationPlan {
            promotion_id: request.promotion_id,
            target_manifest_key: version_manifest_key(
                self.object_key_encoder(),
                &request.target,
                &request.target_version,
            ),
            target_pointer_key: replace_pointer_key(self.object_key_encoder(), &request.target),
            target: request.target,
            correction_package_hash: request.correction_package_hash,
            required_source_packages: request.required_source_packages,
            target_version: request.target_version,
            atomic_pointer_advance: CapabilitySupport::Unsupported,
            executable: false,
            unsupported_reason: "the configured object-store adapter has no proven compare-and-swap target-pointer contract; write a version manifest only after all source packages verify, then use a table format or store with a tested atomic pointer advance"
                .to_owned(),
        })
    }
}

pub(crate) fn plan_correction_request(
    destination: &ParquetDestination,
    request: &DestinationCorrectionCommitRequest,
) -> Result<DestinationCorrectionCommitPlan> {
    cdf_contract::validate_destination_correction_commit_request(request)?;
    request.validate_for(
        &parquet_correction_capabilities(),
        &destination.sheet().transactions,
        &destination.sheet().idempotency,
    )?;
    if request.strategy() != CorrectionStrategy::CorrectionSidecar {
        return Err(CdfError::contract(
            "Parquet correction execution supports only correction_sidecar; in-place update and versioned rematerialization are not executable",
        ));
    }
    let mut context = build_correction_context(destination.object_key_encoder(), request)?;
    if let Some(receipt) = load_correction_receipt(destination, &context.receipt_key)? {
        context.plan.validate_receipt(request, &receipt)?;
        verify_sidecar_receipt(destination, &receipt)?;
        context.duplicate_receipt = Some(receipt);
    }
    let plan = context.plan.clone();
    let mut pending = destination
        .pending_corrections
        .lock()
        .map_err(|_| CdfError::internal("Parquet correction context lock was poisoned"))?;
    pending.insert(plan.kernel.plan_id.clone(), context);
    Ok(plan)
}

pub(crate) fn begin_correction_request<'a>(
    destination: &'a ParquetDestination,
    request: DestinationCorrectionCommitRequest,
    plan: DestinationCorrectionCommitPlan,
) -> Result<Box<dyn CorrectionCommitSession + 'a>> {
    cdf_contract::validate_destination_correction_commit_request(&request)?;
    plan.validate_for(
        &request,
        &parquet_correction_capabilities(),
        &destination.sheet().transactions,
        &destination.sheet().idempotency,
    )?;
    let mut pending = destination
        .pending_corrections
        .lock()
        .map_err(|_| CdfError::internal("Parquet correction context lock was poisoned"))?;
    let context = pending.remove(&plan.kernel.plan_id).ok_or_else(|| {
        CdfError::contract(
            "Parquet begin_correction requires a prior plan_correction for the same correction package",
        )
    })?;
    if context.request != request || context.plan != plan {
        return Err(CdfError::contract(
            "Parquet correction begin request or plan does not match planned sidecar authority",
        ));
    }
    drop(pending);
    Ok(Box::new(ParquetCorrectionSession {
        destination,
        context,
        migrations_applied: false,
        corrections_applied: false,
    }))
}

pub(crate) fn verify_correction_receipt(
    destination: &ParquetDestination,
    receipt: &Receipt,
) -> Result<ReceiptVerification> {
    match verify_sidecar_receipt(destination, receipt) {
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

impl CorrectionCommitSession for ParquetCorrectionSession<'_> {
    fn apply_migrations(&mut self) -> Result<()> {
        if !self.context.plan.kernel.migrations.is_empty() {
            return Err(CdfError::destination(
                "Parquet correction sidecars do not mutate the base target schema",
            ));
        }
        self.migrations_applied = true;
        Ok(())
    }

    fn apply_corrections(&mut self) -> Result<CommitCounts> {
        if !self.migrations_applied {
            return Err(CdfError::destination(
                "Parquet correction planning must complete before sidecar publication",
            ));
        }
        self.corrections_applied = true;
        Ok(sidecar_counts(&self.context.request))
    }

    fn finalize(self: Box<Self>) -> Result<Receipt> {
        if !self.migrations_applied || !self.corrections_applied {
            return Err(CdfError::destination(
                "Parquet correction session requires planning and sidecar staging before finalize",
            ));
        }
        commit_correction_sidecar(self.destination, self.context)
    }

    fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

pub(crate) fn build_correction_context(
    object_key_encoder: ObjectKeyEncoder,
    request: &DestinationCorrectionCommitRequest,
) -> Result<ParquetCorrectionContext> {
    let mut operations = request.corrections.clone();
    operations.sort_by(|left, right| {
        let left = &left.correction.request;
        let right = &right.correction.request;
        (&left.original_row, left.promoted_path.as_str())
            .cmp(&(&right.original_row, right.promoted_path.as_str()))
    });
    let sidecar = ParquetCorrectionSidecar {
        sidecar_version: CORRECTION_SIDECAR_VERSION,
        destination: DESTINATION_ID.to_owned(),
        target: request.target.to_string(),
        correction_package_hash: request.correction_package_hash.to_string(),
        idempotency_token: request.idempotency_token.to_string(),
        resource_disposition: request.resource_disposition.clone(),
        promotion_id: request.promotion_id().clone(),
        old_schema_hash: request.old_schema_hash().clone(),
        new_schema_hash: request.new_schema_hash().clone(),
        operations_digest: request.operations_digest.clone(),
        base_target_unchanged: true,
        operations,
    };
    let sidecar_bytes = canonical_json_bytes(&sidecar)?;
    let sidecar_sha256 = content_sha256(&sidecar_bytes);
    let sidecar_key =
        correction_sidecar_object_key(object_key_encoder, &request.target, &sidecar_sha256);
    let sidecar_object = ParquetCorrectionSidecarObject {
        key: sidecar_key,
        sha256: sidecar_sha256,
        byte_count: sidecar_bytes.len() as u64,
        operation_count: request.corrections.len() as u64,
    };
    let manifest = ParquetCorrectionSidecarManifest {
        manifest_version: CORRECTION_SIDECAR_MANIFEST_VERSION,
        destination: DESTINATION_ID.to_owned(),
        target: request.target.to_string(),
        correction_package_hash: request.correction_package_hash.to_string(),
        idempotency_token: request.idempotency_token.to_string(),
        resource_disposition: request.resource_disposition.clone(),
        promotion_id: request.promotion_id().clone(),
        old_schema_hash: request.old_schema_hash().clone(),
        new_schema_hash: request.new_schema_hash().clone(),
        operations_digest: request.operations_digest.clone(),
        operation_count: request.corrections.len() as u64,
        addressed_rows: request.addressed_row_count(),
        segments: request.segment_acks(),
        base_target_unchanged: true,
        objects: vec![sidecar_object],
    };
    let manifest_bytes = canonical_json_bytes(&manifest)?;
    let manifest_sha256 = content_sha256(&manifest_bytes);
    let manifest_key =
        correction_sidecar_manifest_key(object_key_encoder, &request.target, &manifest_sha256);
    let receipt_key = correction_receipt_key(
        object_key_encoder,
        &request.target,
        &request.idempotency_token,
    );
    let plan = DestinationCorrectionCommitPlan {
        kernel: CommitPlan {
            plan_id: PlanId::new(format!(
                "parquet-correction:{}:{}",
                request.target, request.idempotency_token
            ))?,
            target: request.target.clone(),
            disposition: request.resource_disposition.clone(),
            idempotency: IdempotencySupport::PackageToken,
            migrations: Vec::new(),
            delivery_guarantee: DeliveryGuarantee::EffectivelyOncePerPackage,
        },
        correction_package_hash: request.correction_package_hash.clone(),
        promotion_id: request.promotion_id().clone(),
        old_schema_hash: request.old_schema_hash().clone(),
        new_schema_hash: request.new_schema_hash().clone(),
        strategy: CorrectionStrategy::CorrectionSidecar,
        operations_digest: request.operations_digest.clone(),
        correction_count: request.corrections.len() as u64,
    };
    Ok(ParquetCorrectionContext {
        request: request.clone(),
        plan,
        sidecar_bytes,
        manifest,
        manifest_bytes,
        manifest_key,
        manifest_sha256,
        receipt_key,
        duplicate_receipt: None,
    })
}

fn commit_correction_sidecar(
    destination: &ParquetDestination,
    context: ParquetCorrectionContext,
) -> Result<Receipt> {
    if let Some(receipt) = context.duplicate_receipt {
        context.plan.validate_receipt(&context.request, &receipt)?;
        verify_sidecar_receipt(destination, &receipt)?;
        return Ok(receipt);
    }
    let object = &context.manifest.objects[0];
    destination.store().put_create_or_verify(
        destination.execution(),
        &object.key,
        context.sidecar_bytes.clone(),
    )?;
    destination.store().put_create_or_verify(
        destination.execution(),
        &context.manifest_key,
        context.manifest_bytes.clone(),
    )?;

    let receipt = build_correction_receipt(
        &context.request,
        &context.plan,
        &context.manifest,
        &context.manifest_key,
        &context.manifest_sha256,
        &context.receipt_key,
        now_ms()?,
    )?;
    let receipt_bytes = canonical_json_bytes(&receipt)?;
    let receipt = match destination.store().put_create(
        destination.execution(),
        &context.receipt_key,
        receipt_bytes,
    )? {
        CreateObjectOutcome::Created(_) => receipt,
        CreateObjectOutcome::AlreadyExists => {
            load_correction_receipt(destination, &context.receipt_key)?.ok_or_else(|| {
                CdfError::destination(format!(
                    "Parquet correction receipt {} disappeared after create conflict",
                    context.receipt_key
                ))
            })?
        }
    };
    context.plan.validate_receipt(&context.request, &receipt)?;
    verify_sidecar_receipt(destination, &receipt)?;
    Ok(receipt)
}

pub(crate) fn build_correction_receipt(
    request: &DestinationCorrectionCommitRequest,
    plan: &DestinationCorrectionCommitPlan,
    manifest: &ParquetCorrectionSidecarManifest,
    manifest_key: &str,
    manifest_sha256: &str,
    receipt_key: &str,
    committed_at_ms: i64,
) -> Result<Receipt> {
    let sidecar_evidence = DestinationCorrectionSidecarReceiptEvidence {
        version: cdf_kernel::DESTINATION_CORRECTION_SIDECAR_RECEIPT_EVIDENCE_VERSION,
        manifest_key: manifest_key.to_owned(),
        manifest_sha256: manifest_sha256.to_owned(),
        operation_count: manifest.operation_count,
        atomic_manifest_publication: true,
        base_target_unchanged: true,
        objects: manifest
            .objects
            .iter()
            .map(|object| DestinationCorrectionSidecarObjectEvidence {
                key: object.key.clone(),
                sha256: object.sha256.clone(),
                byte_count: object.byte_count,
                operation_count: object.operation_count,
            })
            .collect(),
    };
    let transaction_values = BTreeMap::from([
        (
            DESTINATION_CORRECTION_RECEIPT_EVIDENCE_KEY.to_owned(),
            DestinationCorrectionReceiptEvidence::for_request(request).to_json()?,
        ),
        (
            DESTINATION_CORRECTION_SIDECAR_RECEIPT_EVIDENCE_KEY.to_owned(),
            sidecar_evidence.to_json()?,
        ),
        ("base_target_unchanged".to_owned(), "true".to_owned()),
        (
            "atomic_target_scope".to_owned(),
            "immutable_correction_manifest_only".to_owned(),
        ),
        ("manifest_key".to_owned(), manifest_key.to_owned()),
        ("manifest_sha256".to_owned(), manifest_sha256.to_owned()),
        ("receipt_key".to_owned(), receipt_key.to_owned()),
    ]);
    let parameters = BTreeMap::from([
        ("manifest_key".to_owned(), manifest_key.to_owned()),
        ("manifest_sha256".to_owned(), manifest_sha256.to_owned()),
        ("receipt_key".to_owned(), receipt_key.to_owned()),
        (
            "package_hash".to_owned(),
            request.correction_package_hash.to_string(),
        ),
        ("target".to_owned(), request.target.to_string()),
    ]);
    Ok(Receipt {
        receipt_id: ReceiptId::new(format!(
            "parquet-correction:{}:{}",
            request.target, request.idempotency_token
        ))?,
        destination: DestinationId::new(DESTINATION_ID)?,
        target: request.target.clone(),
        package_hash: request.correction_package_hash.clone(),
        segment_acks: request.segment_acks(),
        disposition: request.resource_disposition.clone(),
        idempotency_token: request.idempotency_token.clone(),
        transaction: Some(TransactionMetadata {
            system: "object_store_correction_sidecar".to_owned(),
            values: transaction_values,
        }),
        counts: sidecar_counts(request),
        schema_hash: request.new_schema_hash().clone(),
        migrations: plan.kernel.migrations.clone(),
        committed_at_ms,
        verify: VerifyClause {
            kind: "parquet_correction_sidecar_manifest_v1".to_owned(),
            statement: "verify the immutable correction manifest and every content-addressed sidecar object; the base target remains unchanged"
                .to_owned(),
            parameters,
        },
    })
}

fn verify_sidecar_receipt(destination: &ParquetDestination, receipt: &Receipt) -> Result<()> {
    if receipt.destination.as_str() != DESTINATION_ID {
        return Err(CdfError::destination(format!(
            "correction receipt destination {} is not {DESTINATION_ID}",
            receipt.destination
        )));
    }
    if receipt.verify.kind != "parquet_correction_sidecar_manifest_v1" {
        return Err(CdfError::destination(
            "Parquet correction receipt verify kind is not parquet_correction_sidecar_manifest_v1",
        ));
    }
    let correction = DestinationCorrectionReceiptEvidence::from_receipt(receipt)?;
    if correction.strategy != CorrectionStrategy::CorrectionSidecar {
        return Err(CdfError::destination(
            "Parquet correction receipt does not declare correction_sidecar",
        ));
    }
    let evidence = DestinationCorrectionSidecarReceiptEvidence::from_receipt(receipt)?;
    if receipt.counts.rows_written != evidence.operation_count
        || receipt.counts.rows_inserted != Some(evidence.operation_count)
        || receipt.counts.rows_updated != Some(0)
        || receipt.counts.rows_deleted != Some(0)
    {
        return Err(CdfError::destination(
            "Parquet correction receipt counts do not match immutable sidecar operations",
        ));
    }
    let transaction = receipt.transaction.as_ref().ok_or_else(|| {
        CdfError::destination("Parquet correction receipt is missing transaction evidence")
    })?;
    if transaction.system != "object_store_correction_sidecar"
        || transaction
            .values
            .get("base_target_unchanged")
            .map(String::as_str)
            != Some("true")
        || transaction
            .values
            .get("atomic_target_scope")
            .map(String::as_str)
            != Some("immutable_correction_manifest_only")
        || transaction.values.get("manifest_key") != Some(&evidence.manifest_key)
        || transaction.values.get("manifest_sha256") != Some(&evidence.manifest_sha256)
    {
        return Err(CdfError::destination(
            "Parquet correction receipt transaction metadata contradicts its closed sidecar evidence",
        ));
    }
    let receipt_key = receipt
        .verify
        .parameters
        .get("receipt_key")
        .ok_or_else(|| {
            CdfError::destination("Parquet correction receipt is missing receipt_key")
        })?;
    if evidence.manifest_key
        != correction_sidecar_manifest_key(
            destination.object_key_encoder(),
            &receipt.target,
            &evidence.manifest_sha256,
        )
        || *receipt_key
            != correction_receipt_key(
                destination.object_key_encoder(),
                &receipt.target,
                &receipt.idempotency_token,
            )
    {
        return Err(CdfError::destination(
            "Parquet correction receipt keys are not derived from their content/package identities",
        ));
    }
    if transaction.values.get("receipt_key") != Some(receipt_key)
        || receipt.verify.parameters.get("manifest_key") != Some(&evidence.manifest_key)
        || receipt.verify.parameters.get("manifest_sha256") != Some(&evidence.manifest_sha256)
        || receipt
            .verify
            .parameters
            .get("package_hash")
            .map(String::as_str)
            != Some(receipt.package_hash.as_str())
        || receipt.verify.parameters.get("target").map(String::as_str)
            != Some(receipt.target.as_str())
    {
        return Err(CdfError::destination(
            "Parquet correction receipt verify parameters contradict its canonical identity",
        ));
    }
    let recorded_receipt = load_correction_receipt(destination, receipt_key)?.ok_or_else(|| {
        CdfError::destination(format!(
            "Parquet correction receipt marker {receipt_key} is missing"
        ))
    })?;
    if &recorded_receipt != receipt {
        return Err(CdfError::destination(format!(
            "Parquet correction receipt marker {receipt_key} does not match the supplied receipt"
        )));
    }

    let manifest_bytes = destination
        .store()
        .get_required(destination.execution(), &evidence.manifest_key)?;
    if content_sha256(&manifest_bytes) != evidence.manifest_sha256 {
        return Err(CdfError::destination(format!(
            "Parquet correction manifest {} hash does not match receipt evidence",
            evidence.manifest_key
        )));
    }
    let manifest: ParquetCorrectionSidecarManifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|error| {
            CdfError::destination(format!(
                "parse Parquet correction manifest {}: {error}",
                evidence.manifest_key
            ))
        })?;
    validate_manifest_receipt(&manifest, receipt, &correction, &evidence)?;
    for (object, object_evidence) in manifest.objects.iter().zip(&evidence.objects) {
        if object.key
            != correction_sidecar_object_key(
                destination.object_key_encoder(),
                &receipt.target,
                &object.sha256,
            )
        {
            return Err(CdfError::destination(format!(
                "Parquet correction sidecar object {} is not content-addressed by its hash",
                object.key
            )));
        }
        let bytes = destination
            .store()
            .get_required(destination.execution(), &object.key)?;
        if bytes.len() as u64 != object.byte_count || content_sha256(&bytes) != object.sha256 {
            return Err(CdfError::destination(format!(
                "Parquet correction sidecar object {} bytes or hash do not match its manifest",
                object.key
            )));
        }
        if object.key != object_evidence.key
            || object.sha256 != object_evidence.sha256
            || object.byte_count != object_evidence.byte_count
            || object.operation_count != object_evidence.operation_count
        {
            return Err(CdfError::destination(
                "Parquet correction sidecar object evidence does not match its manifest",
            ));
        }
        let sidecar: ParquetCorrectionSidecar =
            serde_json::from_slice(&bytes).map_err(|error| {
                CdfError::destination(format!(
                    "parse Parquet correction sidecar {}: {error}",
                    object.key
                ))
            })?;
        validate_sidecar_manifest(&sidecar, &manifest)?;
    }
    Ok(())
}

fn validate_manifest_receipt(
    manifest: &ParquetCorrectionSidecarManifest,
    receipt: &Receipt,
    correction: &DestinationCorrectionReceiptEvidence,
    evidence: &DestinationCorrectionSidecarReceiptEvidence,
) -> Result<()> {
    if manifest.manifest_version != CORRECTION_SIDECAR_MANIFEST_VERSION
        || manifest.destination != DESTINATION_ID
        || manifest.target != receipt.target.as_str()
        || manifest.correction_package_hash != receipt.package_hash.as_str()
        || manifest.idempotency_token != receipt.idempotency_token.as_str()
        || manifest.resource_disposition != receipt.disposition
        || manifest.promotion_id != correction.promotion_id
        || manifest.old_schema_hash != correction.old_schema_hash
        || manifest.new_schema_hash != correction.new_schema_hash
        || receipt.schema_hash != correction.new_schema_hash
        || manifest.operations_digest != correction.operations_digest
        || manifest.operation_count != correction.correction_count
        || manifest.addressed_rows != correction.addressed_rows
        || manifest.segments != receipt.segment_acks
        || !manifest.base_target_unchanged
        || manifest.objects.len() != evidence.objects.len()
        || !receipt.migrations.is_empty()
    {
        return Err(CdfError::destination(
            "Parquet correction manifest identity does not match its canonical receipt evidence",
        ));
    }
    Ok(())
}

fn validate_sidecar_manifest(
    sidecar: &ParquetCorrectionSidecar,
    manifest: &ParquetCorrectionSidecarManifest,
) -> Result<()> {
    let digest = cdf_contract::correction_operations_digest(&sidecar.operations)?;
    if sidecar.sidecar_version != CORRECTION_SIDECAR_VERSION
        || sidecar.destination != manifest.destination
        || sidecar.target != manifest.target
        || sidecar.correction_package_hash != manifest.correction_package_hash
        || sidecar.idempotency_token != manifest.idempotency_token
        || sidecar.resource_disposition != manifest.resource_disposition
        || sidecar.promotion_id != manifest.promotion_id
        || sidecar.old_schema_hash != manifest.old_schema_hash
        || sidecar.new_schema_hash != manifest.new_schema_hash
        || sidecar.operations_digest != manifest.operations_digest
        || digest != sidecar.operations_digest
        || sidecar.operations.len() as u64 != manifest.operation_count
        || !sidecar.base_target_unchanged
    {
        return Err(CdfError::destination(
            "Parquet correction sidecar authority does not match its manifest",
        ));
    }
    for operation in &sidecar.operations {
        cdf_contract::decode_destination_correction_value(operation)?;
    }
    Ok(())
}

fn load_correction_receipt(
    destination: &ParquetDestination,
    receipt_key: &str,
) -> Result<Option<Receipt>> {
    destination
        .store()
        .get_optional(destination.execution(), receipt_key)?
        .map(|bytes| {
            serde_json::from_slice(&bytes).map_err(|error| {
                CdfError::destination(format!(
                    "parse Parquet correction receipt marker {receipt_key}: {error}"
                ))
            })
        })
        .transpose()
}

fn sidecar_counts(request: &DestinationCorrectionCommitRequest) -> CommitCounts {
    let operations = request.corrections.len() as u64;
    CommitCounts {
        rows_written: operations,
        rows_inserted: Some(operations),
        rows_updated: Some(0),
        rows_deleted: Some(0),
    }
}

fn content_sha256(bytes: &[u8]) -> String {
    format!("sha256:{}", sha256_hex(bytes))
}
