use crate::*;
use crate::{
    manifest::{
        ParquetObjectEntry, ParquetObjectManifest, ParquetReplacePointerReceipt, ReplacePointer,
        canonical_json_bytes, sha256_hex,
    },
    package::{PackageData, load_package_data, validate_requested_segments, write_parquet_segment},
    receipts::{build_receipt, record_package_receipt_once, verify_receipt},
    sheet::parquet_sheet,
    store::{StoreClient, now_ms, package_manifest_key, replace_pointer_key, segment_object_key},
};

pub struct ParquetDestination {
    store: StoreClient,
    runtime: Runtime,
    sheet: DestinationSheet,
    pending_sessions: Mutex<BTreeMap<PlanId, ParquetSessionContext>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParquetCapabilities {
    pub sheet: DestinationSheet,
    pub bulk_paths: Vec<ParquetBulkPath>,
    pub object_manifest_receipts: CapabilitySupport,
    pub replace_pointer: CapabilitySupport,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParquetBulkPath {
    ArrowIpcPackageRowsToParquet,
}

#[derive(Clone, Debug)]
pub struct ParquetCommitRequest {
    pub package_dir: PathBuf,
    pub commit: DestinationCommitRequest,
    pub schema_hash: SchemaHash,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParquetCommitPlan {
    pub kernel: CommitPlan,
    pub manifest_key: String,
    pub replace_pointer_key: Option<String>,
    pub object_keys: Vec<String>,
    pub duplicate: bool,
    pub rows_planned: u64,
    pub bytes_planned: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParquetCommitOutcome {
    pub receipt: Receipt,
    pub duplicate: bool,
    pub plan: ParquetCommitPlan,
    pub object_manifest: ParquetObjectManifest,
    pub package_receipt_recorded: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceiptVerification {
    pub verified: bool,
    pub receipt_id: ReceiptId,
    pub reason: Option<String>,
}

impl ParquetDestination {
    pub fn new_filesystem(root: impl AsRef<Path>) -> Result<Self> {
        Self::from_store(StoreClient::new_filesystem(root.as_ref())?)
    }

    pub fn new_object_store(
        store: Arc<dyn ObjectStore>,
        root_prefix: impl Into<String>,
    ) -> Result<Self> {
        Self::from_store(StoreClient::new_object_store(store, root_prefix)?)
    }

    fn from_store(store: StoreClient) -> Result<Self> {
        let runtime = RuntimeBuilder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| CdfError::internal(format!("create Parquet runtime: {error}")))?;
        Ok(Self {
            store,
            runtime,
            sheet: parquet_sheet()?,
            pending_sessions: Mutex::new(BTreeMap::new()),
        })
    }

    pub fn capabilities(&self) -> ParquetCapabilities {
        ParquetCapabilities {
            sheet: self.sheet.clone(),
            bulk_paths: vec![ParquetBulkPath::ArrowIpcPackageRowsToParquet],
            object_manifest_receipts: CapabilitySupport::Supported,
            replace_pointer: CapabilitySupport::Supported,
        }
    }

    pub fn plan_package_commit(&self, request: &ParquetCommitRequest) -> Result<ParquetCommitPlan> {
        let package = load_package_data(&request.package_dir)?;
        validate_requested_segments(&request.commit.segments, &package)?;
        let plan = self.plan_loaded_package(request, &package)?;
        self.remember_session_context(request, &plan)?;
        Ok(plan)
    }

    pub fn commit_package(&self, request: ParquetCommitRequest) -> Result<ParquetCommitOutcome> {
        let package = load_package_data(&request.package_dir)?;
        validate_requested_segments(&request.commit.segments, &package)?;
        let plan = self.plan_loaded_package(&request, &package)?;

        if let Some(existing) = self.existing_verified_manifest(&request, &plan)? {
            let receipt = build_receipt(
                &request,
                &plan,
                &existing.manifest,
                existing.manifest_etag.clone(),
                existing.replace_pointer.clone(),
            )?;
            let recorded = record_package_receipt_once(&request.package_dir, &receipt)?;
            let mut plan = plan;
            plan.duplicate = true;
            return Ok(ParquetCommitOutcome {
                receipt,
                duplicate: true,
                plan,
                object_manifest: existing.manifest,
                package_receipt_recorded: recorded,
            });
        }

        let committed_at_ms = now_ms()?;
        let requested_segments = request
            .commit
            .segments
            .iter()
            .map(|segment| (segment.segment_id.as_str(), segment))
            .collect::<BTreeMap<_, _>>();
        let mut object_entries = Vec::with_capacity(package.segments.len());
        for segment in &package.segments {
            let bytes = write_parquet_segment(segment)?;
            let sha256 = sha256_hex(&bytes);
            let (row_count, byte_count) = requested_segments
                .get(segment.entry.segment_id.as_str())
                .map(|segment| (segment.row_count, segment.byte_count))
                .unwrap_or((segment.row_count, segment.entry.byte_count));
            let key = segment_object_key(
                &request.commit.target,
                &request.commit.idempotency_token,
                &segment.entry.segment_id,
            );
            let put = self.store.put(&self.runtime, &key, bytes)?;
            object_entries.push(ParquetObjectEntry {
                segment_id: segment.entry.segment_id.as_str().to_owned(),
                key,
                row_count,
                byte_count,
                package_byte_count: segment.entry.byte_count,
                parquet_byte_count: put.byte_count,
                sha256,
                etag: put.e_tag,
                schema_hash: request.schema_hash.as_str().to_owned(),
            });
        }

        let object_manifest = ParquetObjectManifest {
            manifest_version: MANIFEST_VERSION,
            destination: DESTINATION_ID.to_owned(),
            target: request.commit.target.as_str().to_owned(),
            package_hash: request.commit.package_hash.as_str().to_owned(),
            idempotency_token: request.commit.idempotency_token.as_str().to_owned(),
            disposition: request.commit.disposition.clone(),
            schema_hash: request.schema_hash.as_str().to_owned(),
            committed_at_ms,
            total_rows: package.rows,
            objects: object_entries,
        };
        let manifest_bytes = canonical_json_bytes(&object_manifest)?;
        let manifest_sha256 = sha256_hex(&manifest_bytes);
        let manifest_put = self
            .store
            .put(&self.runtime, &plan.manifest_key, manifest_bytes)?;
        let mut replace_pointer = None;

        if let Some(pointer_key) = &plan.replace_pointer_key {
            let pointer = ReplacePointer {
                pointer_version: REPLACE_POINTER_VERSION,
                target: request.commit.target.as_str().to_owned(),
                package_hash: request.commit.package_hash.as_str().to_owned(),
                idempotency_token: request.commit.idempotency_token.as_str().to_owned(),
                schema_hash: request.schema_hash.as_str().to_owned(),
                manifest_key: plan.manifest_key.clone(),
                manifest_sha256: manifest_sha256.clone(),
                updated_at_ms: committed_at_ms,
            };
            let pointer_bytes = canonical_json_bytes(&pointer)?;
            let pointer_sha256 = sha256_hex(&pointer_bytes);
            let pointer_put = self.store.put(&self.runtime, pointer_key, pointer_bytes)?;
            replace_pointer = Some(ParquetReplacePointerReceipt {
                key: pointer_key.clone(),
                sha256: pointer_sha256,
                etag: pointer_put.e_tag,
            });
        }

        let object_manifest = self
            .load_manifest(&plan.manifest_key)?
            .ok_or_else(|| CdfError::destination("Parquet object manifest was not written"))?;
        let receipt = build_receipt(
            &request,
            &plan,
            &object_manifest,
            manifest_put.e_tag,
            replace_pointer,
        )?;
        let recorded = record_package_receipt_once(&request.package_dir, &receipt)?;

        Ok(ParquetCommitOutcome {
            receipt,
            duplicate: false,
            plan,
            object_manifest,
            package_receipt_recorded: recorded,
        })
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

    pub(crate) fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    fn plan_loaded_package(
        &self,
        request: &ParquetCommitRequest,
        package: &PackageData,
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

        let manifest_key =
            package_manifest_key(&request.commit.target, &request.commit.idempotency_token);
        let replace_pointer_key = match request.commit.disposition {
            WriteDisposition::Replace => Some(replace_pointer_key(&request.commit.target)),
            WriteDisposition::Append => None,
            WriteDisposition::Merge | WriteDisposition::CdcApply => {
                return Err(CdfError::contract(
                    "Parquet destination supports append and replace only",
                ));
            }
        };
        let object_keys = package
            .segments
            .iter()
            .map(|segment| {
                segment_object_key(
                    &request.commit.target,
                    &request.commit.idempotency_token,
                    &segment.entry.segment_id,
                )
            })
            .collect::<Vec<_>>();
        let duplicate = self
            .store
            .exists(self.runtime(), &manifest_key)
            .unwrap_or(false);

        Ok(ParquetCommitPlan {
            kernel: self.plan_commit(&request.commit)?,
            manifest_key,
            replace_pointer_key,
            object_keys,
            duplicate,
            rows_planned: package.rows,
            bytes_planned: package.bytes,
        })
    }

    fn existing_verified_manifest(
        &self,
        request: &ParquetCommitRequest,
        plan: &ParquetCommitPlan,
    ) -> Result<Option<LoadedManifest>> {
        let Some(mut loaded) = self.load_manifest_with_etag(&plan.manifest_key)? else {
            return Ok(None);
        };
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
        let bytes = self.store.get_required(self.runtime(), pointer_key)?;
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
        let etag = self.store.etag(self.runtime(), pointer_key)?;
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

    fn load_manifest_with_etag(&self, key: &str) -> Result<Option<LoadedManifest>> {
        let Some(bytes) = self.store.get_optional(self.runtime(), key)? else {
            return Ok(None);
        };
        let manifest = serde_json::from_slice(&bytes).map_err(|error| {
            CdfError::data(format!("parse Parquet object manifest {key}: {error}"))
        })?;
        let manifest_etag = self.store.etag(self.runtime(), key)?;
        Ok(Some(LoadedManifest {
            manifest,
            manifest_etag,
            replace_pointer: None,
        }))
    }

    fn remember_session_context(
        &self,
        request: &ParquetCommitRequest,
        plan: &ParquetCommitPlan,
    ) -> Result<()> {
        // Kernel begin currently carries only portable commit metadata; Parquet keeps
        // package path/schema context from its package-aware dry run and consumes it at begin.
        let mut pending = self
            .pending_sessions
            .lock()
            .map_err(|_| CdfError::internal("Parquet commit session context lock was poisoned"))?;
        pending.insert(
            plan.kernel.plan_id.clone(),
            ParquetSessionContext {
                request: request.clone(),
                plan: plan.clone(),
            },
        );
        Ok(())
    }

    fn take_session_context(
        &self,
        request: &DestinationCommitRequest,
        plan: &CommitPlan,
    ) -> Result<ParquetSessionContext> {
        let mut pending = self
            .pending_sessions
            .lock()
            .map_err(|_| CdfError::internal("Parquet commit session context lock was poisoned"))?;
        let context = pending.remove(&plan.plan_id).ok_or_else(|| {
            CdfError::destination(
                "Parquet commit sessions require package context from plan_package_commit",
            )
        })?;
        if &context.request.commit != request || &context.plan.kernel != plan {
            return Err(CdfError::destination(
                "Parquet commit session context does not match destination request and plan",
            ));
        }
        Ok(context)
    }
}

#[derive(Clone, Debug)]
struct LoadedManifest {
    manifest: ParquetObjectManifest,
    manifest_etag: Option<String>,
    replace_pointer: Option<ParquetReplacePointerReceipt>,
}

#[derive(Clone, Debug)]
struct ParquetSessionContext {
    request: ParquetCommitRequest,
    plan: ParquetCommitPlan,
}

struct ParquetCommitSession<'a> {
    destination: &'a ParquetDestination,
    request: ParquetCommitRequest,
    plan: ParquetCommitPlan,
    migrations_applied: bool,
    outcome: Option<ParquetCommitOutcome>,
}

impl<'a> ParquetCommitSession<'a> {
    fn new(destination: &'a ParquetDestination, context: ParquetSessionContext) -> Self {
        Self {
            destination,
            request: context.request,
            plan: context.plan,
            migrations_applied: false,
            outcome: None,
        }
    }
}

impl CommitSession for ParquetCommitSession<'_> {
    fn apply_migrations(&mut self) -> Result<()> {
        if !self.plan.kernel.migrations.is_empty() {
            return Err(CdfError::destination(
                "Parquet destination does not support migrations",
            ));
        }
        self.migrations_applied = true;
        Ok(())
    }

    fn write(&mut self) -> Result<()> {
        if !self.migrations_applied {
            return Err(CdfError::destination(
                "migrations must be applied before writing",
            ));
        }
        if self.outcome.is_some() {
            return Err(CdfError::destination(
                "Parquet commit session has already written this package",
            ));
        }
        self.outcome = Some(self.destination.commit_package(self.request.clone())?);
        Ok(())
    }

    fn finalize(self: Box<Self>) -> Result<Receipt> {
        let outcome = self.outcome.ok_or_else(|| {
            CdfError::destination("cannot finalize before package segments are written")
        })?;
        Ok(outcome.receipt)
    }

    fn abort(self: Box<Self>) -> Result<()> {
        if self.outcome.is_some() {
            return Err(CdfError::destination(
                "cannot abort Parquet commit session after package write completed",
            ));
        }
        Ok(())
    }
}

impl DestinationProtocol for ParquetDestination {
    fn sheet(&self) -> &DestinationSheet {
        &self.sheet
    }

    fn plan_commit(&self, request: &DestinationCommitRequest) -> Result<CommitPlan> {
        if !self
            .sheet
            .supported_dispositions
            .contains(&request.disposition)
        {
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

    fn begin(
        &self,
        request: DestinationCommitRequest,
        plan: CommitPlan,
    ) -> Result<Box<dyn CommitSession + '_>> {
        let expected = self.plan_commit(&request)?;
        if expected != plan {
            return Err(CdfError::destination(
                "commit plan does not match destination request",
            ));
        }
        let context = self.take_session_context(&request, &plan)?;
        Ok(Box::new(ParquetCommitSession::new(self, context)))
    }
}
