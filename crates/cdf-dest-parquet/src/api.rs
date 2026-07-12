use crate::*;
use crate::{
    corrections::*,
    manifest::{
        ParquetObjectEntry, ParquetObjectManifest, ParquetReplacePointerReceipt, ReplacePointer,
        canonical_json_bytes, sha256_hex,
    },
    package::{PackageData, load_package_data, validate_requested_segments, write_parquet_segment},
    receipts::{build_receipt, record_package_receipt_once, verify_receipt},
    sheet::{parquet_protocol_capabilities, parquet_sheet},
    store::{
        ObjectKeyEncoder, StoreClient, now_ms, package_manifest_key, replace_pointer_key,
        segment_object_key,
    },
};

pub struct ParquetDestination {
    store: StoreClient,
    execution: cdf_runtime::ExecutionServices,
    sheet: DestinationSheet,
    object_key_encoder: ObjectKeyEncoder,
    pending_sessions: Mutex<BTreeMap<PlanId, ParquetSessionContext>>,
    pub(crate) pending_corrections: Mutex<BTreeMap<PlanId, ParquetCorrectionContext>>,
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
        let artifact = Self::destination_sheet_artifact()?;
        let sheet = artifact.sheet;
        let protocol_capabilities = artifact.protocol_capabilities;
        let object_key_encoder = ObjectKeyEncoder::from_capabilities(&protocol_capabilities)?;
        Ok(Self {
            store,
            execution,
            sheet,
            object_key_encoder,
            pending_sessions: Mutex::new(BTreeMap::new()),
            pending_corrections: Mutex::new(BTreeMap::new()),
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

    pub fn dry_plan_commit(
        request: &DestinationCommitRequest,
    ) -> Result<(DestinationSheet, CommitPlan)> {
        let sheet = parquet_sheet()?;
        let plan = plan_kernel_commit(&sheet, request)?;
        Ok((sheet, plan))
    }

    pub fn plan_package_commit(&self, request: &ParquetCommitRequest) -> Result<ParquetCommitPlan> {
        let reader = PackageReader::open(&request.package_dir)?;
        reader.verify()?;
        let manifest_segments = &reader.manifest().identity.segments;
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
        let plan = self.plan_package_shape(request, &segment_ids, rows_planned, bytes_planned)?;
        self.remember_session_context(request, &plan)?;
        Ok(plan)
    }

    pub fn commit_package(&self, request: ParquetCommitRequest) -> Result<ParquetCommitOutcome> {
        let package = load_package_data(&request.package_dir)?;
        validate_requested_segments(&request.commit.segments, &package)?;
        let plan = self.plan_loaded_package(&request, &package)?;
        self.commit_loaded_package(request, package, plan)
    }

    fn commit_loaded_package(
        &self,
        request: ParquetCommitRequest,
        package: PackageData,
        plan: ParquetCommitPlan,
    ) -> Result<ParquetCommitOutcome> {
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
                self.object_key_encoder(),
                &request.commit.target,
                &request.commit.idempotency_token,
                &segment.entry.segment_id,
            );
            let put = self.store.put(&self.execution, &key, bytes)?;
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
            .put(&self.execution, &plan.manifest_key, manifest_bytes)?;
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
            let pointer_put = self
                .store
                .put(&self.execution, pointer_key, pointer_bytes)?;
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

    pub(crate) fn execution(&self) -> &cdf_runtime::ExecutionServices {
        &self.execution
    }

    pub(crate) fn object_key_encoder(&self) -> ObjectKeyEncoder {
        self.object_key_encoder
    }

    fn plan_loaded_package(
        &self,
        request: &ParquetCommitRequest,
        package: &PackageData,
    ) -> Result<ParquetCommitPlan> {
        let segment_ids = package
            .segments
            .iter()
            .map(|segment| segment.entry.segment_id.clone())
            .collect::<Vec<_>>();
        self.plan_package_shape(request, &segment_ids, package.rows, package.bytes)
    }

    fn plan_package_shape(
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
            replace_pointer_key,
            object_keys,
            duplicate,
            rows_planned,
            bytes_planned,
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
    expected_segments: BTreeMap<SegmentId, ExpectedSegment>,
    expected_order: Vec<SegmentId>,
    accepted_segments: BTreeSet<SegmentId>,
    object_entries: BTreeMap<SegmentId, ParquetObjectEntry>,
    existing: Option<LoadedManifest>,
    schema: Option<SchemaRef>,
}

#[derive(Clone, Debug)]
struct ExpectedSegment {
    state: StateSegment,
    package_byte_count: u64,
}

impl<'a> ParquetCommitSession<'a> {
    fn new(destination: &'a ParquetDestination, context: ParquetSessionContext) -> Result<Self> {
        let (expected_segments, expected_order) =
            expected_segments_for_context(destination.object_key_encoder(), &context)?;
        let existing = destination.existing_verified_manifest(&context.request, &context.plan)?;
        Ok(Self {
            destination,
            request: context.request,
            plan: context.plan,
            migrations_applied: false,
            expected_segments,
            expected_order,
            accepted_segments: BTreeSet::new(),
            object_entries: BTreeMap::new(),
            existing,
            schema: None,
        })
    }

    fn finalize_outcome(self) -> Result<ParquetCommitOutcome> {
        if !self.migrations_applied {
            return Err(CdfError::destination(
                "migrations must be applied before finalizing",
            ));
        }
        if self.accepted_segments.len() != self.expected_segments.len() {
            return Err(CdfError::destination(format!(
                "cannot finalize Parquet commit session before all segments are written: accepted {} of {}",
                self.accepted_segments.len(),
                self.expected_segments.len()
            )));
        }

        if let Some(existing) = self.existing {
            return duplicate_parquet_outcome(self.request, self.plan, existing);
        }
        let mut object_entries = Vec::with_capacity(self.expected_order.len());
        for segment_id in &self.expected_order {
            let entry = self.object_entries.get(segment_id).ok_or_else(|| {
                CdfError::internal(format!(
                    "accepted Parquet segment {} is missing its durable object entry",
                    segment_id.as_str()
                ))
            })?;
            object_entries.push(entry.clone());
        }
        finalize_parquet_objects(self.destination, self.request, self.plan, object_entries)
    }
}

fn write_commit_segment_object(
    destination: &ParquetDestination,
    request: &ParquetCommitRequest,
    segment: CommitSegment,
) -> Result<ParquetObjectEntry> {
    let CommitSegment {
        state,
        package_byte_count,
        batches,
        ..
    } = segment;
    let loaded = crate::package::LoadedSegment {
        entry: SegmentEntry {
            segment_id: state.segment_id.clone(),
            path: String::new(),
            row_count: state.row_count,
            byte_count: package_byte_count,
            sha256: String::new(),
        },
        row_count: state.row_count,
        batches,
    };
    let bytes = write_parquet_segment(&loaded)?;
    let sha256 = sha256_hex(&bytes);
    let key = segment_object_key(
        destination.object_key_encoder(),
        &request.commit.target,
        &request.commit.idempotency_token,
        &state.segment_id,
    );
    let put = destination.store.put(&destination.execution, &key, bytes)?;
    Ok(ParquetObjectEntry {
        segment_id: state.segment_id.as_str().to_owned(),
        key,
        row_count: state.row_count,
        byte_count: state.byte_count,
        package_byte_count,
        parquet_byte_count: put.byte_count,
        sha256,
        etag: put.e_tag,
        schema_hash: request.schema_hash.as_str().to_owned(),
    })
}

fn duplicate_parquet_outcome(
    request: ParquetCommitRequest,
    mut plan: ParquetCommitPlan,
    existing: LoadedManifest,
) -> Result<ParquetCommitOutcome> {
    let receipt = build_receipt(
        &request,
        &plan,
        &existing.manifest,
        existing.manifest_etag,
        existing.replace_pointer,
    )?;
    let recorded = record_package_receipt_once(&request.package_dir, &receipt)?;
    plan.duplicate = true;
    Ok(ParquetCommitOutcome {
        receipt,
        duplicate: true,
        plan,
        object_manifest: existing.manifest,
        package_receipt_recorded: recorded,
    })
}

fn finalize_parquet_objects(
    destination: &ParquetDestination,
    request: ParquetCommitRequest,
    plan: ParquetCommitPlan,
    object_entries: Vec<ParquetObjectEntry>,
) -> Result<ParquetCommitOutcome> {
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
    let manifest_put =
        destination
            .store
            .put(&destination.execution, &plan.manifest_key, manifest_bytes)?;
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
    let recorded = record_package_receipt_once(&request.package_dir, &receipt)?;
    Ok(ParquetCommitOutcome {
        receipt,
        duplicate: false,
        plan,
        object_manifest: persisted_manifest,
        package_receipt_recorded: recorded,
    })
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

    fn write_segment(&mut self, segment: CommitSegment) -> Result<SegmentAck> {
        if !self.migrations_applied {
            return Err(CdfError::destination(
                "migrations must be applied before writing",
            ));
        }
        let segment_id = segment.state.segment_id.clone();
        let expected = self.expected_segments.get(&segment_id).ok_or_else(|| {
            CdfError::data(format!(
                "Parquet commit segment {} is not in the planned package request",
                segment_id.as_str()
            ))
        })?;
        if self.accepted_segments.contains(&segment_id) {
            return Err(CdfError::data(format!(
                "Parquet commit session received duplicate segment {}",
                segment_id.as_str()
            )));
        }
        let schema = validate_commit_segment(&segment, expected, self.schema.as_ref())?;
        if self.schema.is_none() {
            self.schema = Some(schema);
        }

        let entry = if self.existing.is_some() {
            None
        } else {
            Some(write_commit_segment_object(
                self.destination,
                &self.request,
                segment,
            )?)
        };

        let ack = SegmentAck {
            segment_id: expected.state.segment_id.clone(),
            row_count: expected.state.row_count,
            byte_count: expected.state.byte_count,
        };
        self.accepted_segments.insert(segment_id);
        if let Some(entry) = entry {
            self.object_entries.insert(ack.segment_id.clone(), entry);
        }
        Ok(ack)
    }

    fn finalize(self: Box<Self>) -> Result<Receipt> {
        Ok(self.finalize_outcome()?.receipt)
    }

    fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
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
        Ok(Box::new(ParquetCommitSession::new(self, context)?))
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

fn expected_segments_for_context(
    object_key_encoder: ObjectKeyEncoder,
    context: &ParquetSessionContext,
) -> Result<(BTreeMap<SegmentId, ExpectedSegment>, Vec<SegmentId>)> {
    let reader = PackageReader::open(&context.request.package_dir)?;
    reader.verify()?;

    let mut manifest_by_id = BTreeMap::new();
    let mut expected_order = Vec::new();
    let mut rows_planned = 0_u64;
    let mut bytes_planned = 0_u64;
    for segment in &reader.manifest().identity.segments {
        if manifest_by_id
            .insert(segment.segment_id.clone(), segment)
            .is_some()
        {
            return Err(CdfError::data(format!(
                "package manifest contains duplicate segment {}",
                segment.segment_id.as_str()
            )));
        }
        rows_planned += segment.row_count;
        bytes_planned += segment.byte_count;
        expected_order.push(segment.segment_id.clone());
    }

    if rows_planned != context.plan.rows_planned || bytes_planned != context.plan.bytes_planned {
        return Err(CdfError::destination(
            "Parquet commit plan does not match package manifest totals",
        ));
    }

    let expected_object_keys = expected_order
        .iter()
        .map(|segment_id| {
            segment_object_key(
                object_key_encoder,
                &context.request.commit.target,
                &context.request.commit.idempotency_token,
                segment_id,
            )
        })
        .collect::<Vec<_>>();
    if expected_object_keys != context.plan.object_keys {
        return Err(CdfError::destination(
            "Parquet commit plan object keys do not match package manifest",
        ));
    }

    let mut request_by_id = BTreeMap::new();
    for state in &context.request.commit.segments {
        if request_by_id
            .insert(state.segment_id.clone(), state)
            .is_some()
        {
            return Err(CdfError::data(format!(
                "destination commit request contains duplicate segment {}",
                state.segment_id.as_str()
            )));
        }
    }

    let mut expected_segments = BTreeMap::new();
    for (segment_id, manifest_segment) in &manifest_by_id {
        let state = request_by_id.get(segment_id).ok_or_else(|| {
            CdfError::data(format!(
                "package manifest segment {} is missing from destination commit request",
                segment_id.as_str()
            ))
        })?;
        if state.row_count != manifest_segment.row_count {
            return Err(CdfError::data(format!(
                "destination commit request segment {} has {} rows but package manifest has {} rows",
                segment_id.as_str(),
                state.row_count,
                manifest_segment.row_count
            )));
        }
        expected_segments.insert(
            segment_id.clone(),
            ExpectedSegment {
                state: (*state).clone(),
                package_byte_count: manifest_segment.byte_count,
            },
        );
    }

    for segment_id in request_by_id.keys() {
        if !manifest_by_id.contains_key(segment_id) {
            return Err(CdfError::data(format!(
                "destination commit request segment {} is not present in the package manifest",
                segment_id.as_str()
            )));
        }
    }

    Ok((expected_segments, expected_order))
}

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

fn validate_commit_segment(
    segment: &CommitSegment,
    expected: &ExpectedSegment,
    session_schema: Option<&SchemaRef>,
) -> Result<SchemaRef> {
    if segment.state != expected.state {
        return Err(CdfError::data(format!(
            "Parquet commit segment {} state does not match destination commit request",
            segment.state.segment_id.as_str()
        )));
    }
    if segment.package_byte_count != expected.package_byte_count {
        return Err(CdfError::data(format!(
            "Parquet commit segment {} package byte count {} differs from manifest {}",
            segment.state.segment_id.as_str(),
            segment.package_byte_count,
            expected.package_byte_count
        )));
    }
    if segment.batches.is_empty() {
        return Err(CdfError::data(format!(
            "Parquet commit segment {} contains no record batches",
            segment.state.segment_id.as_str()
        )));
    }

    let schema = segment.batches[0].schema();
    if let Some(session_schema) = session_schema
        && schema.as_ref() != session_schema.as_ref()
    {
        return Err(CdfError::data(
            "Parquet destination requires all package segments to share one schema",
        ));
    }

    let mut row_count = 0_u64;
    for batch in &segment.batches {
        if batch.schema().as_ref() != schema.as_ref() {
            return Err(CdfError::data(format!(
                "Parquet commit segment {} contains mixed schemas",
                segment.state.segment_id.as_str()
            )));
        }
        row_count += batch.num_rows() as u64;
    }
    if row_count != expected.state.row_count {
        return Err(CdfError::data(format!(
            "Parquet commit segment {} has {} payload rows but request expects {}",
            segment.state.segment_id.as_str(),
            row_count,
            expected.state.row_count
        )));
    }

    Ok(schema)
}
