use super::*;

use std::{collections::VecDeque, io::Write};

use crate::{
    corrections::{build_correction_context, build_correction_receipt},
    manifest::{
        CurrentReplacePointer, ParquetCorrectionSidecar, ParquetCorrectionSidecarManifest,
        ParquetObjectManifest, ReplacePointer, canonical_json_bytes, sha256_hex,
    },
    sheet::{parquet_correction_capabilities, parquet_protocol_capabilities, parquet_sheet},
    store::{ObjectKeyEncoder, data_object_key, package_manifest_key, staged_data_object_key},
};
use ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow_array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use cdf_conformance::destination::{
    DestinationConformanceCase, DestinationCorrectionConformanceEvidence,
    assert_destination_conformance, assert_destination_correction_conformance,
    representative_commit_request,
};
use cdf_kernel::{
    CanonicalArrowField, CorrectionStrategy, CursorPosition, CursorValue, DeliveryGuarantee,
    DestinationCorrectionCommitRequest, DestinationCorrectionOperation, DestinationCorrectionPlan,
    DestinationCorrectionReceiptEvidence, DestinationCorrectionRequest,
    DestinationCorrectionSidecarReceiptEvidence, IdempotencyToken, PackageHash, PartitionId,
    PlanId, PromotionId, ResidualCorrectionOperation, ResourceId, RowProvenanceAddress, ScanPlan,
    ScanRequest, ScopeKey, SegmentAck, SegmentId, SourcePosition,
};
use cdf_package::PackageBuilder;
use cdf_package_contract::{
    PackageReplayInputs, PackageStatus, QuarantineRecord, SegmentEntry, VerifiedPackageAccess,
};
use cdf_runtime::DestinationRuntime;
use object_store::{memory::InMemory, path::Path as ObjectPath};

fn test_writer_settings() -> crate::package::ParquetWriterSettings {
    crate::package::ParquetWriterSettings {
        rows_per_batch: 64 * 1024,
        bytes_per_batch: 16 * 1024 * 1024,
    }
}

#[test]
#[ignore = "release-mode Parquet destination write-roofline benchmark"]
fn local_streaming_parquet_reaches_sixty_percent_of_write_roofline() {
    const ROWS: usize = 8 * 1024 * 1024;
    const CHUNK: usize = 8 * 1024 * 1024;
    let root = tempfile::tempdir().unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(
                (0..ROWS).map(|row| (row as i64).wrapping_mul(6_364_136_223_846_793_005)),
            )),
            Arc::new(Float64Array::from_iter_values((0..ROWS).map(|row| {
                f64::from_bits((row as u64).wrapping_mul(11_400_714_819_323_198_485))
            }))),
        ],
    )
    .unwrap();
    let segment = || {
        CommitSegment::new(
            StateSegment {
                segment_id: SegmentId::new("roofline-segment").unwrap(),
                scope: ScopeKey::Resource,
                output_position: SourcePosition::Cursor(CursorPosition {
                    version: 1,
                    field: "id".to_owned(),
                    value: CursorValue::U64(ROWS as u64),
                }),
                row_count: ROWS as u64,
                byte_count: (ROWS * 16) as u64,
            },
            (ROWS * 16) as u64,
            canonical_batches(vec![batch.clone()], 0),
        )
    };
    let (_, services) = cdf_engine::StandaloneExecutionHost::default_services_with_spill(
        512 * 1024 * 1024,
        512 * 1024 * 1024,
    )
    .unwrap();
    let bytes = vec![0_u8; CHUNK];
    let mut observations = Vec::with_capacity(3);
    for _ in 0..3 {
        let started = std::time::Instant::now();
        let (_, _, encoded) = crate::package::write_parquet_segment(
            segment(),
            test_writer_settings(),
            services.memory(),
            services.spill(),
            tempfile::NamedTempFile::new_in(root.path()).unwrap(),
        )
        .unwrap();
        encoded.file.as_file().sync_all().unwrap();
        let parquet_elapsed = started.elapsed();

        let mut raw = tempfile::NamedTempFile::new_in(root.path()).unwrap();
        let started = std::time::Instant::now();
        let mut remaining = encoded.byte_count;
        while remaining > 0 {
            let write = remaining.min(CHUNK as u64) as usize;
            raw.write_all(&bytes[..write]).unwrap();
            remaining -= write as u64;
        }
        raw.as_file().sync_all().unwrap();
        let raw_elapsed = started.elapsed();
        let parquet_rate = encoded.byte_count as f64 / parquet_elapsed.as_secs_f64();
        let raw_rate = encoded.byte_count as f64 / raw_elapsed.as_secs_f64();
        observations.push((
            encoded.byte_count,
            parquet_elapsed,
            raw_elapsed,
            parquet_rate,
            raw_rate,
        ));
    }
    observations.sort_by(|left, right| (left.3 / left.4).total_cmp(&(right.3 / right.4)));
    let (physical_bytes, parquet_elapsed, raw_elapsed, parquet_rate, raw_rate) = observations[1];
    let ratio = parquet_rate / raw_rate;
    eprintln!(
        "parquet_local_write physical_bytes={} wall_time_ns={} raw_wall_time_ns={} parquet_mib_s={:.1} raw_mib_s={:.1} ratio={ratio:.3}",
        physical_bytes,
        parquet_elapsed.as_nanos(),
        raw_elapsed.as_nanos(),
        parquet_rate / (1024.0 * 1024.0),
        raw_rate / (1024.0 * 1024.0),
    );
    assert!(ratio >= 0.60, "Parquet write-roofline ratio was {ratio:.3}");
}

#[derive(Clone, Debug)]
struct BuiltPackage {
    hash: PackageHash,
    segments: Vec<SegmentEntry>,
}

#[derive(Clone, Debug)]
struct StoredJson {
    sha256: String,
    etag: Option<String>,
}

fn test_execution() -> cdf_runtime::ExecutionServices {
    static SERVICES: std::sync::OnceLock<cdf_runtime::ExecutionServices> =
        std::sync::OnceLock::new();
    SERVICES
        .get_or_init(|| {
            // Parallel destination tests share this host; keep their independent
            // memory-pressure assertions from contending for the same logical ledger.
            cdf_engine::StandaloneExecutionHost::default_services(2 * 1024 * 1024 * 1024)
                .unwrap()
                .1
                .with_staging_lease_authority(Arc::new(
                    cdf_runtime::ScopeStagingLeaseAuthority::new(Arc::new(
                        cdf_state_sqlite::InMemoryScopeLeaseStore::new(),
                    )),
                ))
                .unwrap()
                .with_content_reachability_store(Arc::new(
                    cdf_state_sqlite::SqliteContentReachabilityStore::open_in_memory().unwrap(),
                ))
        })
        .clone()
}

fn test_filesystem(root: impl AsRef<Path>) -> Result<ParquetDestination> {
    ParquetDestination::new_filesystem(root, test_execution())
}

fn test_object_store(
    store: Arc<dyn ObjectStore>,
    root_prefix: impl Into<String>,
) -> Result<ParquetDestination> {
    let root_prefix = root_prefix.into();
    ParquetDestination::new_object_store(
        cdf_kernel::ContentStoreNamespace::new(format!("test-store:{root_prefix}")).unwrap(),
        store,
        root_prefix,
        test_execution(),
    )
}

fn sample_batch(ids: Vec<i64>, names: Vec<Option<&str>>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let id: ArrayRef = Arc::new(Int64Array::from(ids));
    let name: ArrayRef = Arc::new(StringArray::from(names));
    RecordBatch::try_new(schema, vec![id, name]).unwrap()
}

fn canonical_batches(batches: Vec<RecordBatch>, start: u64) -> Vec<RecordBatch> {
    cdf_package_contract::append_package_row_ord(batches, start).unwrap()
}

fn correction_operation(
    original_package_hash: &PackageHash,
    row: u64,
    value: i64,
) -> DestinationCorrectionOperation {
    let values = Int64Array::from(vec![value]);
    let authority = cdf_contract::encode_residual_json_v1([cdf_contract::ResidualFieldRef::new(
        ["age"],
        &values,
        0,
    )
    .unwrap()])
    .unwrap();
    DestinationCorrectionOperation {
        correction: DestinationCorrectionPlan {
            request: DestinationCorrectionRequest {
                promotion_id: PromotionId::new("promotion-age").unwrap(),
                original_row: RowProvenanceAddress::new(
                    original_package_hash.clone(),
                    SegmentId::new("seg-000001").unwrap(),
                    row,
                ),
                old_schema_hash: SchemaHash::new("schema-v1").unwrap(),
                new_schema_hash: SchemaHash::new("schema-v2").unwrap(),
                promoted_path: "/age".to_owned(),
                promoted_value_json: value.to_string(),
                residual_operation: ResidualCorrectionOperation::RemovePromotedPath,
                selected_strategy: CorrectionStrategy::CorrectionSidecar,
            },
            transaction_guarantee: TransactionSupport::AtomicTarget,
            idempotency_guarantee: IdempotencySupport::PackageToken,
        },
        output_field: CanonicalArrowField::from_arrow(&Field::new("age", DataType::Int64, true))
            .unwrap(),
        promoted_value_residual_json_v1: authority,
    }
}

fn correction_request(original_package_hash: &PackageHash) -> DestinationCorrectionCommitRequest {
    let operations = vec![
        correction_operation(original_package_hash, 0, 42),
        correction_operation(original_package_hash, 1, 84),
    ];
    let correction_hash = PackageHash::new("sha256:parquet-correction-age").unwrap();
    DestinationCorrectionCommitRequest::new(
        correction_hash.clone(),
        IdempotencyToken::new(correction_hash.as_str()).unwrap(),
        TargetName::new("orders").unwrap(),
        WriteDisposition::Append,
        vec![StateSegment {
            segment_id: SegmentId::new("seg-correction").unwrap(),
            scope: ScopeKey::Resource,
            output_position: SourcePosition::Cursor(CursorPosition {
                version: 1,
                field: "correction".to_owned(),
                value: CursorValue::U64(2),
            }),
            row_count: operations.len() as u64,
            byte_count: 2,
        }],
        operations,
    )
    .unwrap()
}

fn finalize_correction(
    destination: &ParquetDestination,
    request: &DestinationCorrectionCommitRequest,
) -> Receipt {
    let plan = destination.plan_correction(request).unwrap();
    let mut session = destination.begin_correction(request.clone(), plan).unwrap();
    session.apply_migrations().unwrap();
    let counts = session.apply_corrections().unwrap();
    assert_eq!(counts.rows_inserted, Some(request.corrections.len() as u64));
    assert_eq!(counts.rows_updated, Some(0));
    session.finalize().unwrap()
}

fn commit_correction_base(
    destination: &mut ParquetDestination,
    package_dir: &Path,
    package_id: &str,
) -> BuiltPackage {
    let built = build_package(
        package_dir,
        package_id,
        vec![(
            "seg-000001",
            vec![sample_batch(vec![1, 2], vec![Some("ada"), Some("grace")])],
        )],
    );
    commit_through_ingress(
        destination,
        package_dir,
        request(package_dir, &built, WriteDisposition::Append),
    )
    .unwrap();
    built
}

fn build_package<S: AsRef<str>>(
    package_dir: &Path,
    package_id: &str,
    segments: Vec<(S, Vec<RecordBatch>)>,
) -> BuiltPackage {
    let builder = PackageBuilder::create(package_dir, package_id).unwrap();
    builder.update_status(PackageStatus::Extracting).unwrap();
    builder
        .write_json_artifact(
            "plan/resource_plan.json",
            &BTreeMap::from([("resource", "orders")]),
        )
        .unwrap();
    builder
        .write_json_artifact(
            "schema/output.json",
            &BTreeMap::from([("schema_hash", "schema-v1")]),
        )
        .unwrap();
    builder
        .write_json_artifact(
            "destination/commit_plan.json",
            &BTreeMap::from([("target", "orders")]),
        )
        .unwrap();

    let mut package_row_ord_start = 0_u64;
    for (segment_id, batches) in segments {
        let row_count = batches.iter().map(RecordBatch::num_rows).sum::<usize>() as u64;
        let batches = canonical_batches(batches, package_row_ord_start);
        builder
            .write_segment(
                SegmentId::new(segment_id.as_ref()).unwrap(),
                package_row_ord_start,
                &batches,
            )
            .unwrap();
        package_row_ord_start += row_count;
    }

    let manifest = builder.finish().unwrap();
    BuiltPackage {
        hash: PackageHash::new(manifest.package_hash).unwrap(),
        segments: manifest.identity.segments,
    }
}

fn request(
    _package_dir: &Path,
    built: &BuiltPackage,
    disposition: WriteDisposition,
) -> ParquetCommitRequest {
    ParquetCommitRequest {
        commit: DestinationCommitRequest {
            package_hash: built.hash.clone(),
            target: TargetName::new("orders").unwrap(),
            disposition,
            segments: built.segments.iter().map(state_segment).collect(),
            idempotency_token: IdempotencyToken::new(built.hash.as_str()).unwrap(),
        },
        schema_hash: SchemaHash::new("schema-v1").unwrap(),
    }
}

fn state_segment(segment: &SegmentEntry) -> StateSegment {
    StateSegment {
        segment_id: segment.segment_id.clone(),
        scope: ScopeKey::Partition {
            partition_id: PartitionId::new("p0").unwrap(),
        },
        output_position: SourcePosition::Cursor(CursorPosition {
            version: 1,
            field: "id".to_owned(),
            value: CursorValue::I64(segment.row_count as i64),
        }),
        row_count: segment.row_count,
        byte_count: segment.row_count * 16,
    }
}

fn replay_inputs(request: &ParquetCommitRequest) -> PackageReplayInputs {
    let scope = request
        .commit
        .segments
        .first()
        .map_or(ScopeKey::Resource, |segment| segment.scope.clone());
    let output_position = request.commit.segments.last().map_or_else(
        || {
            SourcePosition::Cursor(CursorPosition {
                version: 1,
                field: "id".to_owned(),
                value: CursorValue::U64(0),
            })
        },
        |segment| segment.output_position.clone(),
    );
    PackageReplayInputs {
        input_checkpoint: None,
        state_delta: cdf_kernel::StateDelta {
            checkpoint_id: cdf_kernel::CheckpointId::new("checkpoint-parquet-prepared-test")
                .unwrap(),
            pipeline_id: cdf_kernel::PipelineId::new("pipeline-parquet-prepared-test").unwrap(),
            resource_id: cdf_kernel::ResourceId::new("orders").unwrap(),
            scope,
            state_version: cdf_kernel::CHECKPOINT_STATE_VERSION,
            parent_checkpoint_id: None,
            input_position: None,
            output_position,
            package_hash: request.commit.package_hash.clone(),
            schema_hash: request.schema_hash.clone(),
            segments: request.commit.segments.clone(),
        },
        destination_commit: request.commit.clone(),
        merge_keys: Vec::new(),
        schema_hash: request.schema_hash.clone(),
    }
}

fn parquet_rows(bytes: &[u8]) -> usize {
    let mut temp = tempfile::NamedTempFile::new().unwrap();
    temp.write_all(bytes).unwrap();
    temp.flush().unwrap();

    let file = fs::File::open(temp.path()).unwrap();
    ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap()
        .map(|batch| batch.unwrap().num_rows())
        .sum()
}

fn parquet_field_names(bytes: &[u8]) -> Vec<String> {
    let mut temp = tempfile::NamedTempFile::new().unwrap();
    temp.write_all(bytes).unwrap();
    temp.flush().unwrap();
    let file = fs::File::open(temp.path()).unwrap();
    ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

fn manifest_key(receipt: &Receipt) -> &str {
    receipt
        .verify
        .parameters
        .get("manifest_key")
        .expect("manifest_key verify parameter")
}

fn replace_pointer_key_from_receipt(receipt: &Receipt) -> &str {
    receipt
        .transaction
        .as_ref()
        .expect("transaction metadata")
        .values
        .get("replace_pointer_key")
        .expect("replace pointer key")
}

fn load_manifest(dest: &ParquetDestination, key: &str) -> ParquetObjectManifest {
    let bytes = dest.store().get_required(dest.execution(), key).unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn load_replace_pointer(dest: &ParquetDestination, key: &str) -> ReplacePointer {
    let bytes = dest.store().get_required(dest.execution(), key).unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn store_manifest(
    dest: &ParquetDestination,
    key: &str,
    manifest: &ParquetObjectManifest,
) -> StoredJson {
    let bytes = canonical_json_bytes(manifest).unwrap();
    let sha256 = sha256_hex(&bytes);
    let put = dest.store().put(dest.execution(), key, bytes).unwrap();
    StoredJson {
        sha256,
        etag: put.e_tag,
    }
}

fn store_replace_pointer(
    dest: &ParquetDestination,
    key: &str,
    pointer: &ReplacePointer,
) -> StoredJson {
    let bytes = canonical_json_bytes(pointer).unwrap();
    let sha256 = sha256_hex(&bytes);
    let put = dest.store().put(dest.execution(), key, bytes).unwrap();
    StoredJson {
        sha256,
        etag: put.e_tag,
    }
}

fn receipt_with_manifest_store(receipt: &Receipt, manifest: StoredJson) -> Receipt {
    let mut receipt = receipt.clone();
    receipt
        .verify
        .parameters
        .insert("manifest_sha256".to_owned(), manifest.sha256.clone());
    let transaction = receipt.transaction.as_mut().expect("transaction metadata");
    transaction
        .values
        .insert("manifest_sha256".to_owned(), manifest.sha256);
    if let Some(etag) = manifest.etag {
        transaction.values.insert("manifest_etag".to_owned(), etag);
    } else {
        transaction.values.remove("manifest_etag");
    }
    receipt
}

fn receipt_with_pointer_store(receipt: &Receipt, pointer: StoredJson) -> Receipt {
    let mut receipt = receipt.clone();
    let transaction = receipt.transaction.as_mut().expect("transaction metadata");
    transaction
        .values
        .insert("replace_pointer_sha256".to_owned(), pointer.sha256);
    if let Some(etag) = pointer.etag {
        transaction
            .values
            .insert("replace_pointer_etag".to_owned(), etag);
    } else {
        transaction.values.remove("replace_pointer_etag");
    }
    receipt
}

#[derive(Debug)]
struct CommittedPackage {
    receipt: Receipt,
    duplicate: bool,
    verification: cdf_runtime::DestinationCommitVerification,
    plan: ParquetCommitPlan,
    object_manifest: ParquetObjectManifest,
}

struct TestVerifiedPackage {
    package_hash: String,
    segments: Vec<SegmentEntry>,
    scan: ScanPlan,
    inputs: PackageReplayInputs,
    schema: SchemaRef,
}

impl VerifiedPackageAccess for TestVerifiedPackage {
    fn package_hash(&self) -> &str {
        &self.package_hash
    }

    fn identity_segments(&self) -> &[SegmentEntry] {
        &self.segments
    }

    fn recorded_scan_plan(&self) -> Result<ScanPlan> {
        Ok(self.scan.clone())
    }

    fn replay_inputs(&self) -> Result<PackageReplayInputs> {
        Ok(self.inputs.clone())
    }

    fn runtime_arrow_schema(&self) -> Result<SchemaRef> {
        Ok(Arc::clone(&self.schema))
    }

    fn quarantine_records(&self) -> Result<Vec<QuarantineRecord>> {
        Ok(Vec::new())
    }
}

struct TestSegmentReader {
    identity: cdf_runtime::StagedSegmentIdentity,
    batches: std::vec::IntoIter<RecordBatch>,
}

impl cdf_runtime::DurableSegmentReader for TestSegmentReader {
    fn identity(&self) -> &cdf_runtime::StagedSegmentIdentity {
        &self.identity
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.batches.next())
    }
}

struct TestStagedStream {
    attempt_id: cdf_runtime::LoadAttemptId,
    requests: VecDeque<cdf_runtime::StagedSegmentRequest>,
    in_flight: BTreeMap<SegmentId, cdf_runtime::StagedSegmentIdentity>,
    accepted: BTreeMap<u32, cdf_runtime::StagedSegmentIdentity>,
}

impl cdf_runtime::StagedSegmentStream for TestStagedStream {
    fn next_segment(&mut self) -> Result<Option<cdf_runtime::StagedSegmentRequest>> {
        let Some(request) = self.requests.pop_front() else {
            return Ok(None);
        };
        if self
            .in_flight
            .insert(
                request.identity.segment_id.clone(),
                request.identity.clone(),
            )
            .is_some()
        {
            return Err(CdfError::data("test staged stream repeated a segment id"));
        }
        Ok(Some(request))
    }

    fn acknowledge(&mut self, acknowledgement: cdf_runtime::StagedSegmentAck) -> Result<()> {
        let expected = self
            .in_flight
            .remove(&acknowledgement.identity.segment_id)
            .ok_or_else(|| CdfError::data("test staged stream acknowledged an absent segment"))?;
        if acknowledgement.attempt_id != self.attempt_id || acknowledgement.identity != expected {
            return Err(CdfError::data(
                "test staged stream acknowledgement identity mismatch",
            ));
        }
        self.accepted.insert(expected.ordinal, expected);
        Ok(())
    }
}

fn commit_through_ingress(
    dest: &mut ParquetDestination,
    package_dir: &Path,
    commit: ParquetCommitRequest,
) -> Result<CommittedPackage> {
    let staged_commit = stage_through_ingress(dest, package_dir, commit)?;
    let (outcome, plan) = bind_staged_test_commit(staged_commit)?;
    let duplicate = matches!(
        outcome.reporting_policy,
        cdf_runtime::DestinationReceiptReportingPolicy::DestinationCommit { duplicate: true }
    );
    let verification = outcome.verification;
    let receipt = outcome.receipt;
    let object_manifest = load_manifest(dest, &plan.manifest_key);
    Ok(CommittedPackage {
        receipt,
        duplicate,
        verification,
        plan,
        object_manifest,
    })
}

fn bind_staged_test_commit(
    mut staged_commit: StagedTestCommit,
) -> Result<(cdf_runtime::DestinationCommitOutcome, ParquetCommitPlan)> {
    let binding =
        cdf_runtime::VerifiedFinalBinding::from_verified_package_with_execution_authority(
            staged_commit.attempt_id.clone(),
            staged_commit.execution_plan_id.clone(),
            &staged_commit.package,
            staged_commit.plan.kernel.clone(),
        )?;
    binding.validate_staged_identities(&staged_commit.staged)?;
    let outcome = staged_commit.session.bind_final(binding)?;
    if let Some(lease) = staged_commit.managed_lease.take() {
        lease.finish()?;
    }
    Ok((outcome, staged_commit.plan))
}

struct StagedTestCommit {
    session: Box<dyn cdf_runtime::StagedIngressSession>,
    staged: Vec<cdf_runtime::StagedSegmentIdentity>,
    attempt_id: cdf_runtime::LoadAttemptId,
    staging_lease: cdf_runtime::StagingLease,
    managed_lease: Option<cdf_runtime::ManagedStagingLease>,
    execution_plan_id: PlanId,
    plan: ParquetCommitPlan,
    package: TestVerifiedPackage,
}

fn stage_through_ingress(
    dest: &mut ParquetDestination,
    package_dir: &Path,
    commit: ParquetCommitRequest,
) -> Result<StagedTestCommit> {
    static NEXT_TEST_ATTEMPT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let ordinal = NEXT_TEST_ATTEMPT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let attempt_id = cdf_runtime::LoadAttemptId::new(format!(
        "parquet_test_{ordinal}_{}",
        commit.commit.idempotency_token.as_str().replace(':', "_")
    ))?;
    stage_through_ingress_with_attempt(dest, package_dir, commit, attempt_id)
}

fn stage_through_ingress_with_attempt(
    dest: &mut ParquetDestination,
    package_dir: &Path,
    commit: ParquetCommitRequest,
    attempt_id: cdf_runtime::LoadAttemptId,
) -> Result<StagedTestCommit> {
    let managed_lease =
        dest.execution()
            .acquire_staging_lease(cdf_runtime::StagingLeaseIdentity::new(
                dest.sheet().destination.clone(),
                commit.commit.target.clone(),
                attempt_id.clone(),
            ))?;
    let staging_lease = managed_lease.snapshot()?;
    let mutation_guard = managed_lease.mutation_guard()?;
    stage_through_ingress_with_lease(
        dest,
        package_dir,
        commit,
        attempt_id,
        staging_lease,
        mutation_guard,
        Some(managed_lease),
    )
}

fn stage_through_ingress_with_lease(
    dest: &mut ParquetDestination,
    package_dir: &Path,
    commit: ParquetCommitRequest,
    attempt_id: cdf_runtime::LoadAttemptId,
    staging_lease: cdf_runtime::StagingLease,
    mutation_guard: cdf_runtime::StagingMutationGuard,
    managed_lease: Option<cdf_runtime::ManagedStagingLease>,
) -> Result<StagedTestCommit> {
    let reader = PackageReader::open(package_dir)?;
    let commit_segments = reader.read_commit_segments(&commit.commit.segments)?;
    let output_schema = commit_segments
        .iter()
        .flat_map(|segment| segment.batches.first())
        .next()
        .map(|batch| cdf_package_contract::logical_output_schema(batch.schema().as_ref()).unwrap())
        .unwrap_or_else(|| {
            sample_batch(Vec::new(), Vec::new())
                .schema()
                .as_ref()
                .clone()
        });
    let plan = dest.plan_package_commit(&commit, &reader.manifest().identity.segments)?;
    let inputs = replay_inputs(&commit);
    let capabilities = dest.runtime_capabilities();
    let preparation = cdf_runtime::BulkPathPreparationInput::new(&output_schema)
        .with_execution(dest.execution().capabilities());
    let bulk_path = dest.prepare_selected_bulk_path(&preparation)?;
    let execution_plan_id = PlanId::new("parquet-staged-test-plan")?;
    let staging_request = cdf_runtime::StagedIngressRequest::new(
        attempt_id.clone(),
        cdf_runtime::StagingAttemptBinding {
            destination_id: dest.sheet().destination.clone(),
            target: commit.commit.target.clone(),
            disposition: commit.commit.disposition.clone(),
            schema_hash: commit.schema_hash.clone(),
            output_arrow_schema_hash: cdf_kernel::canonical_arrow_schema_hash(&output_schema)?,
            merge_keys: Vec::new(),
            execution_plan_id: execution_plan_id.clone(),
        },
        staging_lease.clone(),
        mutation_guard,
        bulk_path,
        cdf_runtime::StagingSchedulingContext::new(
            capabilities.max_in_flight_segments.unwrap(),
            capabilities.max_in_flight_bytes.unwrap(),
        )?,
        output_schema.clone(),
    )?;
    let mut session = match dest.ingress() {
        cdf_runtime::DestinationIngress::StagedSegments(ingress) => {
            ingress.begin_staged_ingress(staging_request)?
        }
        cdf_runtime::DestinationIngress::FinalizedPackage(_) => {
            return Err(CdfError::contract(
                "Parquet test destination exposed finalized-package ingress",
            ));
        }
    };
    let requests = reader
        .manifest()
        .identity
        .segments
        .iter()
        .zip(commit_segments)
        .enumerate()
        .map(|(ordinal, (entry, segment))| {
            let identity = cdf_runtime::StagedSegmentIdentity::from_manifest_entry(
                entry,
                commit.schema_hash.clone(),
                u32::try_from(ordinal)
                    .map_err(|_| CdfError::data("test staged ordinal exceeds u32"))?,
            )?;
            cdf_runtime::StagedSegmentRequest::new(
                identity.clone(),
                Box::new(TestSegmentReader {
                    identity,
                    batches: segment.batches.into_iter(),
                }),
            )
        })
        .collect::<Result<VecDeque<_>>>()?;
    let mut stream = TestStagedStream {
        attempt_id: attempt_id.clone(),
        requests,
        in_flight: BTreeMap::new(),
        accepted: BTreeMap::new(),
    };
    session.stage_stream(&mut stream)?;
    let staged = stream.accepted.into_values().collect::<Vec<_>>();
    assert!(stream.in_flight.is_empty());
    assert_eq!(session.snapshot()?.accepted_segments, staged);
    let package = TestVerifiedPackage {
        package_hash: commit.commit.package_hash.as_str().to_owned(),
        segments: reader.manifest().identity.segments.clone(),
        scan: ScanPlan {
            plan_id: execution_plan_id.clone(),
            request: ScanRequest {
                resource_id: ResourceId::new("orders")?,
                projection: None,
                filters: Vec::new(),
                limit: None,
                order_by: Vec::new(),
                scope: ScopeKey::Resource,
            },
            partitions: Vec::new(),
            planned_task_set: None,
            pushed_predicates: Vec::new(),
            unsupported_predicates: Vec::new(),
            estimated_rows: None,
            estimated_bytes: None,
            delivery_guarantee: DeliveryGuarantee::AtLeastOnceDuplicateRisk,
        },
        inputs,
        schema: Arc::new(output_schema),
    };
    Ok(StagedTestCommit {
        session,
        staged,
        attempt_id,
        staging_lease,
        managed_lease,
        execution_plan_id,
        plan,
        package,
    })
}

fn plan_package_for_test(
    dest: &ParquetDestination,
    package_dir: &Path,
    commit: &ParquetCommitRequest,
) -> Result<ParquetCommitPlan> {
    let reader = PackageReader::open(package_dir)?;
    dest.plan_package_commit(commit, &reader.manifest().identity.segments)
}

fn assert_staged_abort_cleans_destination(
    dest: &mut ParquetDestination,
    package_dir: &Path,
    commit: ParquetCommitRequest,
) {
    let staged = stage_through_ingress(dest, package_dir, commit).unwrap();
    let staging_key = staged_data_object_key(
        dest.object_key_encoder(),
        &TargetName::new("orders").unwrap(),
        staged.staging_lease.authority_domain_id(),
        &staged.attempt_id,
        staged.staging_lease.fencing_token(),
        0,
    );
    assert!(
        !dest
            .store()
            .exists(dest.execution(), &staging_key)
            .expect("inspect bounded staged Parquet object")
    );
    let content_before = dest
        .store()
        .list_prefix(dest.execution(), "targets/orders/objects/sha256/")
        .unwrap();
    assert_eq!(content_before.len(), 1);
    let retained_live = dest.reclaim_unreachable_content(8).unwrap();
    assert_eq!(retained_live.retained_live, 1);
    staged.session.abort().unwrap();
    assert!(
        !dest
            .store()
            .exists(dest.execution(), &staging_key)
            .expect("inspect cleaned Parquet staging object")
    );
    assert_eq!(
        dest.store()
            .list_prefix(dest.execution(), "targets/orders/objects/sha256/")
            .unwrap(),
        content_before,
        "abort must retain shared immutable objects for reachability GC"
    );
    assert!(
        !dest
            .store()
            .exists(dest.execution(), &staged.plan.manifest_key)
            .unwrap()
    );
}

#[test]
fn unsupported_arrow_types_fail_before_writing_objects() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-unsupported");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "unsupported_time",
        DataType::Time32(arrow_schema::TimeUnit::Second),
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(arrow_array::Time32SecondArray::from(vec![1]))],
    )
    .unwrap();
    let built = build_package(
        &package_dir,
        "pkg-unsupported",
        vec![("seg-000001", vec![batch])],
    );
    let root = temp.path().join("lake");
    let mut dest = test_filesystem(&root).unwrap();

    let error = commit_through_ingress(
        &mut dest,
        &package_dir,
        request(&package_dir, &built, WriteDisposition::Append),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("does not support Arrow type Time32")
    );
    assert!(!root.join("targets").exists());
}

#[test]
fn sheet_declares_append_replace_and_unsupported_semantics_honestly() {
    let temp = tempfile::tempdir().unwrap();
    let dest = test_filesystem(temp.path()).unwrap();
    let sheet = dest.sheet();

    assert_eq!(sheet.destination.as_str(), "parquet_object_store");
    assert_eq!(sheet.transactions, TransactionSupport::AtomicTarget);
    assert_eq!(sheet.idempotency, IdempotencySupport::PackageToken);
    assert_eq!(sheet.migration_support, CapabilitySupport::Unsupported);
    assert_eq!(sheet.quarantine_tables, CapabilitySupport::Unsupported);
    assert_eq!(sheet.identifier_rules.normalizer, "namecase-v1");
    assert_eq!(sheet.identifier_rules.max_length, None);
    assert_eq!(
        sheet.identifier_rules.allowed_pattern.as_deref(),
        Some("^[a-z_][a-z0-9_]*$")
    );
    assert_eq!(
        dest.protocol_capabilities().object_key_rules(),
        Some(&ObjectKeyRules::component_v1())
    );
    assert!(
        sheet
            .supported_dispositions
            .contains(&WriteDisposition::Append)
    );
    assert!(
        sheet
            .supported_dispositions
            .contains(&WriteDisposition::Replace)
    );
    assert!(
        !sheet
            .supported_dispositions
            .contains(&WriteDisposition::Merge)
    );
    assert!(
        !sheet
            .supported_dispositions
            .contains(&WriteDisposition::CdcApply)
    );
    assert!(
        dest.plan_commit(&DestinationCommitRequest {
            package_hash: PackageHash::new("sha256:test").unwrap(),
            target: TargetName::new("orders").unwrap(),
            disposition: WriteDisposition::Merge,
            segments: Vec::new(),
            idempotency_token: IdempotencyToken::new("sha256:test").unwrap(),
        })
        .is_err()
    );
}

#[test]
fn reusable_destination_conformance_suite_accepts_parquet_sheet_and_plans() {
    let temp = tempfile::tempdir().unwrap();
    let dest = test_filesystem(temp.path()).unwrap();

    assert_destination_conformance(
        &dest,
        [
            DestinationConformanceCase::new(representative_commit_request(
                WriteDisposition::Append,
            )),
            DestinationConformanceCase::new(representative_commit_request(
                WriteDisposition::Replace,
            )),
        ],
    );
    assert_destination_correction_conformance(
        &dest,
        &DestinationCorrectionConformanceEvidence {
            row_provenance_persistence: CapabilitySupport::Supported,
            row_provenance_targetability: CapabilitySupport::Supported,
            residual_readback: CapabilitySupport::Unsupported,
            strategies: parquet_correction_capabilities().strategies,
        },
    );
}

#[test]
fn correction_sidecar_is_content_addressed_verifiable_and_leaves_base_immutable() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-sidecar-base");
    let built = build_package(
        &package_dir,
        "pkg-sidecar-base",
        vec![(
            "seg-000001",
            vec![sample_batch(vec![1, 2], vec![Some("ada"), Some("grace")])],
        )],
    );
    let mut dest = test_filesystem(temp.path().join("lake")).unwrap();
    let base = commit_through_ingress(
        &mut dest,
        &package_dir,
        request(&package_dir, &built, WriteDisposition::Append),
    )
    .unwrap();
    let base_manifest_before = dest
        .store()
        .get_required(dest.execution(), &base.plan.manifest_key)
        .unwrap();
    let base_object_key = base.object_manifest.objects[0].key.clone();
    let base_object_before = dest
        .store()
        .get_required(dest.execution(), &base_object_key)
        .unwrap();
    let first_row =
        RowProvenanceAddress::new(built.hash.clone(), SegmentId::new("seg-000001").unwrap(), 0);
    assert_eq!(
        dest.resolve_row_provenance(&TargetName::new("orders").unwrap(), &first_row)
            .unwrap(),
        Some(ParquetRowLocation {
            object_key: base_object_key.clone(),
            row_ordinal: 0,
        })
    );
    let past_segment =
        RowProvenanceAddress::new(built.hash.clone(), SegmentId::new("seg-000001").unwrap(), 2);
    assert_eq!(
        dest.resolve_row_provenance(&TargetName::new("orders").unwrap(), &past_segment)
            .unwrap(),
        None
    );

    let correction = correction_request(&built.hash);
    let receipt = finalize_correction(&dest, &correction);

    assert_eq!(receipt.counts.rows_written, 2);
    assert_eq!(receipt.counts.rows_inserted, Some(2));
    assert_eq!(receipt.counts.rows_updated, Some(0));
    assert_eq!(receipt.counts.rows_deleted, Some(0));
    assert_eq!(receipt.schema_hash.as_str(), "schema-v2");
    assert_eq!(
        receipt
            .transaction
            .as_ref()
            .unwrap()
            .values
            .get("base_target_unchanged")
            .map(String::as_str),
        Some("true")
    );
    assert_eq!(
        receipt
            .transaction
            .as_ref()
            .unwrap()
            .values
            .get("atomic_target_scope")
            .map(String::as_str),
        Some("immutable_correction_manifest_only")
    );
    let correction_evidence = DestinationCorrectionReceiptEvidence::from_receipt(&receipt).unwrap();
    assert_eq!(
        correction_evidence.strategy,
        CorrectionStrategy::CorrectionSidecar
    );
    let sidecar_evidence =
        DestinationCorrectionSidecarReceiptEvidence::from_receipt(&receipt).unwrap();
    assert!(sidecar_evidence.atomic_manifest_publication);
    assert!(sidecar_evidence.base_target_unchanged);
    assert_eq!(sidecar_evidence.operation_count, 2);
    assert!(
        sidecar_evidence
            .manifest_key
            .contains("/corrections/manifests/sha256~3a")
    );
    assert_eq!(sidecar_evidence.objects.len(), 1);
    assert!(
        sidecar_evidence.objects[0]
            .key
            .contains("/corrections/objects/sha256~3a")
    );

    let manifest_bytes = dest
        .store()
        .get_required(dest.execution(), &sidecar_evidence.manifest_key)
        .unwrap();
    assert_eq!(
        format!("sha256:{}", sha256_hex(&manifest_bytes)),
        sidecar_evidence.manifest_sha256
    );
    let manifest: ParquetCorrectionSidecarManifest =
        serde_json::from_slice(&manifest_bytes).unwrap();
    assert_eq!(
        manifest.correction_package_hash,
        correction.correction_package_hash.as_str()
    );
    assert_eq!(manifest.old_schema_hash.as_str(), "schema-v1");
    assert_eq!(manifest.new_schema_hash.as_str(), "schema-v2");
    assert_eq!(manifest.addressed_rows, 2);
    assert!(manifest.base_target_unchanged);
    let sidecar_bytes = dest
        .store()
        .get_required(dest.execution(), &manifest.objects[0].key)
        .unwrap();
    assert_eq!(
        format!("sha256:{}", sha256_hex(&sidecar_bytes)),
        manifest.objects[0].sha256
    );
    let sidecar: ParquetCorrectionSidecar = serde_json::from_slice(&sidecar_bytes).unwrap();
    assert_eq!(sidecar.operations.len(), 2);
    assert_eq!(
        sidecar.operations[0].correction.request.original_row,
        RowProvenanceAddress::new(built.hash.clone(), SegmentId::new("seg-000001").unwrap(), 0,)
    );
    assert_eq!(
        sidecar.operations[0].correction.request.promoted_path,
        "/age"
    );
    assert_eq!(sidecar.operations[0].output_field.name, "age");
    assert_eq!(
        sidecar.operations[0].correction.request.residual_operation,
        ResidualCorrectionOperation::RemovePromotedPath
    );
    assert!(dest.verify_correction(&receipt).unwrap().verified);
    assert_eq!(
        dest.store()
            .get_required(dest.execution(), &base.plan.manifest_key)
            .unwrap(),
        base_manifest_before
    );
    assert_eq!(
        dest.store()
            .get_required(dest.execution(), &base_object_key)
            .unwrap(),
        base_object_before
    );

    let replay = finalize_correction(&dest, &correction);
    assert_eq!(replay, receipt);
    assert!(dest.verify_correction(&replay).unwrap().verified);
    assert!(
        dest.read_correction_residual(
            &TargetName::new("orders").unwrap(),
            &sidecar.operations[0].correction.request.original_row,
        )
        .unwrap_err()
        .to_string()
        .contains("does not support correction residual readback")
    );
}

#[test]
fn ordinary_objects_and_correction_sidecars_share_column_policy_without_changing_object_keys() {
    let temp = tempfile::tempdir().unwrap();
    let mut dest = test_filesystem(temp.path().join("lake")).unwrap();
    let policy =
        cdf_contract::identifier_policy_from_destination_rules(&dest.sheet().identifier_rules)
            .unwrap();
    let normalized = cdf_contract::normalize_identifier("VendorID", &policy).unwrap();
    assert_eq!(normalized, "vendor_id");

    let schema = Arc::new(Schema::new(vec![cdf_kernel::with_source_name(
        Field::new(&normalized, DataType::Int64, false),
        "VendorID",
    )]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64, 2_i64]))]).unwrap();
    let package_dir = temp.path().join("pkg-normalized-columns");
    let built = build_package(
        &package_dir,
        "pkg-normalized-columns",
        vec![("seg-000001", vec![batch])],
    );
    let base = commit_through_ingress(
        &mut dest,
        &package_dir,
        request(&package_dir, &built, WriteDisposition::Append),
    )
    .unwrap();
    let base_bytes = dest
        .store()
        .get_required(dest.execution(), &base.object_manifest.objects[0].key)
        .unwrap();
    assert_eq!(
        parquet_field_names(&base_bytes),
        std::slice::from_ref(&normalized)
    );

    let mut correction = correction_request(&built.hash);
    let promoted = CanonicalArrowField::from_arrow(&cdf_kernel::with_source_name(
        Field::new(&normalized, DataType::Int64, true),
        "VendorID",
    ))
    .unwrap();
    for operation in &mut correction.corrections {
        operation.output_field = promoted.clone();
    }
    correction = DestinationCorrectionCommitRequest::new(
        correction.correction_package_hash.clone(),
        correction.idempotency_token.clone(),
        correction.target.clone(),
        correction.resource_disposition.clone(),
        correction.segments.clone(),
        correction.corrections,
    )
    .unwrap();
    let receipt = finalize_correction(&dest, &correction);
    let evidence = DestinationCorrectionSidecarReceiptEvidence::from_receipt(&receipt).unwrap();
    let manifest: ParquetCorrectionSidecarManifest = serde_json::from_slice(
        &dest
            .store()
            .get_required(dest.execution(), &evidence.manifest_key)
            .unwrap(),
    )
    .unwrap();
    let sidecar: ParquetCorrectionSidecar = serde_json::from_slice(
        &dest
            .store()
            .get_required(dest.execution(), &manifest.objects[0].key)
            .unwrap(),
    )
    .unwrap();
    assert!(
        sidecar
            .operations
            .iter()
            .all(|operation| operation.output_field.name == normalized)
    );

    let encoded_token = built.hash.as_str().replace(':', "~3a");
    assert_eq!(
        base.plan.manifest_key,
        format!("targets/orders/packages/{encoded_token}/manifest.json")
    );
    assert!(
        evidence
            .manifest_key
            .starts_with("targets/orders/corrections/manifests/")
    );
    assert_eq!(
        dest.protocol_capabilities().object_key_rules(),
        Some(&ObjectKeyRules::component_v1())
    );
}

#[test]
fn object_key_construction_requires_declared_policy_and_preserves_component_v1_bytes() {
    let error = ObjectKeyEncoder::from_capabilities(
        &cdf_kernel::DestinationProtocolCapabilities::default(),
    )
    .unwrap_err();
    assert!(error.message.contains("requires typed object-key rules"));

    let capabilities = parquet_protocol_capabilities();
    capabilities.validate(&parquet_sheet().unwrap()).unwrap();
    let encoder = ObjectKeyEncoder::from_capabilities(&capabilities).unwrap();
    assert_eq!(
        package_manifest_key(
            encoder,
            &TargetName::new("orders/by region").unwrap(),
            &IdempotencyToken::new("sha256:abc/def").unwrap(),
        ),
        "targets/orders~2fby~20region/packages/sha256~3aabc~2fdef/manifest.json"
    );
}

#[test]
fn interrupted_sidecar_publication_reuses_orphan_object_and_publishes_manifest_once() {
    let temp = tempfile::tempdir().unwrap();
    let store = Arc::new(InMemory::default());
    let mut dest = test_object_store(store, "").unwrap();
    let built = commit_correction_base(&mut dest, &temp.path().join("base"), "base");
    let correction = correction_request(&built.hash);
    let context = build_correction_context(dest.object_key_encoder(), &correction).unwrap();
    let object = context.manifest.objects[0].clone();
    dest.store()
        .put_create_or_verify(dest.execution(), &object.key, context.sidecar_bytes.clone())
        .unwrap();
    assert!(dest.store().exists(dest.execution(), &object.key).unwrap());
    assert!(
        !dest
            .store()
            .exists(dest.execution(), &context.manifest_key)
            .unwrap()
    );
    dest.store()
        .put_create_or_verify(
            dest.execution(),
            &context.manifest_key,
            context.manifest_bytes.clone(),
        )
        .unwrap();
    assert!(
        dest.store()
            .exists(dest.execution(), &context.manifest_key)
            .unwrap()
    );
    assert!(
        !dest
            .store()
            .exists(dest.execution(), &context.receipt_key)
            .unwrap()
    );
    let unrecorded = build_correction_receipt(
        &context.request,
        &context.plan,
        &context.manifest,
        &context.manifest_key,
        &context.manifest_sha256,
        &context.receipt_key,
        1,
    )
    .unwrap();
    let verification = dest.verify_correction(&unrecorded).unwrap();
    assert!(!verification.verified);
    assert!(verification.reason.unwrap().contains("marker"));

    let receipt = finalize_correction(&dest, &correction);

    assert!(dest.verify_correction(&receipt).unwrap().verified);
    assert_eq!(
        dest.store()
            .get_required(dest.execution(), &object.key)
            .unwrap(),
        context.sidecar_bytes
    );
    assert_eq!(
        dest.store()
            .get_required(dest.execution(), &context.manifest_key)
            .unwrap(),
        context.manifest_bytes
    );
    assert!(
        dest.store()
            .exists(dest.execution(), &context.receipt_key)
            .unwrap()
    );
}

#[test]
fn correction_abort_writes_nothing_and_tampering_invalidates_receipt() {
    let temp = tempfile::tempdir().unwrap();
    let mut dest = test_object_store(Arc::new(InMemory::default()), "").unwrap();
    let built = commit_correction_base(&mut dest, &temp.path().join("base"), "base");
    let correction = correction_request(&built.hash);
    let context = build_correction_context(dest.object_key_encoder(), &correction).unwrap();
    let plan = dest.plan_correction(&correction).unwrap();
    let mut session = dest.begin_correction(correction.clone(), plan).unwrap();
    session.apply_migrations().unwrap();
    session.apply_corrections().unwrap();
    session.abort().unwrap();
    assert!(
        !dest
            .store()
            .exists(dest.execution(), &context.manifest.objects[0].key)
            .unwrap()
    );
    assert!(
        !dest
            .store()
            .exists(dest.execution(), &context.manifest_key)
            .unwrap()
    );
    assert!(
        !dest
            .store()
            .exists(dest.execution(), &context.receipt_key)
            .unwrap()
    );

    let receipt = finalize_correction(&dest, &correction);
    dest.store()
        .put(
            dest.execution(),
            &context.manifest.objects[0].key,
            b"tampered".to_vec(),
        )
        .unwrap();
    let verification = dest.verify_correction(&receipt).unwrap();
    assert!(!verification.verified);
    assert!(verification.reason.unwrap().contains("bytes or hash"));
}

#[test]
fn versioned_rematerialization_is_an_explicit_non_executable_plan_boundary() {
    let temp = tempfile::tempdir().unwrap();
    let dest = test_filesystem(temp.path()).unwrap();
    let plan = dest
        .plan_versioned_rematerialization(ParquetVersionedRematerializationRequest {
            promotion_id: PromotionId::new("promotion-age").unwrap(),
            target: TargetName::new("orders").unwrap(),
            correction_package_hash: PackageHash::new("sha256:correction").unwrap(),
            required_source_packages: vec![
                PackageHash::new("sha256:base-1").unwrap(),
                PackageHash::new("sha256:base-2").unwrap(),
            ],
            target_version: "schema-v2".to_owned(),
        })
        .unwrap();

    assert_eq!(plan.required_source_packages.len(), 2);
    assert_eq!(plan.target_version, "schema-v2");
    assert_eq!(
        plan.target_manifest_key,
        "targets/orders/versions/schema-v2/manifest.json"
    );
    assert_eq!(plan.target_pointer_key, "targets/orders/current.json");
    assert_eq!(plan.atomic_pointer_advance, CapabilitySupport::Unsupported);
    assert!(!plan.executable);
    assert!(plan.unsupported_reason.contains("compare-and-swap"));
    assert!(
        parquet_correction_capabilities()
            .strategy(CorrectionStrategy::VersionedRematerialization)
            .is_none()
    );
    assert!(!temp.path().join("targets").exists());
}

#[test]
fn filesystem_append_materializes_parquet_and_verifies_receipt() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg");
    let built = build_package(
        &package_dir,
        "pkg-append",
        vec![(
            "seg-000001",
            vec![sample_batch(
                vec![1, 2, 3],
                vec![Some("ada"), Some("grace"), None],
            )],
        )],
    );
    let root = temp.path().join("lake");
    let mut dest = test_filesystem(&root).unwrap();

    let outcome = commit_through_ingress(
        &mut dest,
        &package_dir,
        request(&package_dir, &built, WriteDisposition::Append),
    )
    .unwrap();

    assert!(!outcome.duplicate);
    assert_eq!(outcome.receipt.counts.rows_written, 3);
    assert!(dest.verify_receipt(&outcome.receipt).unwrap().verified);
    assert_eq!(outcome.object_manifest.objects.len(), 1);
    assert_eq!(outcome.object_manifest.objects[0].schema_hash, "schema-v1");
    assert_eq!(outcome.object_manifest.objects[0].byte_count, 48);
    assert_ne!(
        outcome.object_manifest.objects[0].byte_count,
        outcome.object_manifest.objects[0].package_byte_count
    );
    assert_eq!(outcome.receipt.segment_acks[0].byte_count, 48);

    let bytes = dest
        .store()
        .get_required(dest.execution(), &outcome.object_manifest.objects[0].key)
        .unwrap();
    assert_eq!(parquet_rows(&bytes), 3);
    assert_eq!(parquet_field_names(&bytes), vec!["id", "name"]);
    assert_eq!(
        std::fs::read_dir(root.join(".cdf-staging"))
            .unwrap()
            .count(),
        0,
        "successful local install must leave no staged file"
    );

    let receipts = PackageReader::open(&package_dir)
        .unwrap()
        .receipts()
        .unwrap();
    assert!(
        receipts.is_empty(),
        "package receipts belong to orchestration"
    );
}

#[test]
fn staged_segment_ingress_materializes_verifiable_manifest_receipt() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-session");
    let built = build_package(
        &package_dir,
        "pkg-session",
        vec![(
            "seg-000001",
            vec![sample_batch(vec![1, 2], vec![Some("ada"), Some("grace")])],
        )],
    );
    let mut dest = test_object_store(Arc::new(InMemory::default()), "").unwrap();
    let commit = request(&package_dir, &built, WriteDisposition::Append);

    let committed = commit_through_ingress(&mut dest, &package_dir, commit.clone()).unwrap();
    let plan = committed.plan;
    let receipt = committed.receipt;

    assert!(!plan.duplicate);
    assert!(matches!(
        committed.verification,
        cdf_runtime::DestinationCommitVerification::VerifiedAtCommit(_)
    ));
    assert_eq!(receipt.destination.as_str(), DESTINATION_ID);
    assert_eq!(
        receipt.receipt_id.as_str(),
        format!(
            "parquet:orders:{}",
            commit.commit.idempotency_token.as_str()
        )
    );
    assert_eq!(receipt.package_hash, commit.commit.package_hash);
    assert_eq!(receipt.schema_hash.as_str(), "schema-v1");
    assert_eq!(receipt.segment_acks.len(), 1);
    assert_eq!(receipt.counts.rows_written, 2);
    assert!(dest.verify_receipt(&receipt).unwrap().verified);

    let manifest = load_manifest(&dest, manifest_key(&receipt));
    assert_eq!(manifest.objects.len(), 1);
    assert_eq!(
        manifest.objects[0].key,
        data_object_key(
            dest.object_key_encoder(),
            &TargetName::new("orders").unwrap(),
            &manifest.objects[0].sha256,
        )
    );
    assert_eq!(manifest.objects[0].row_count, 2);
    assert_eq!(manifest.objects[0].schema_hash, "schema-v1");

    let receipts = PackageReader::open(&package_dir)
        .unwrap()
        .receipts()
        .unwrap();
    assert!(
        receipts.is_empty(),
        "package receipts belong to orchestration"
    );
}

#[test]
fn staged_segment_abort_cleans_local_and_object_store_attempts() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-staged-abort");
    let built = build_package(
        &package_dir,
        "pkg-staged-abort",
        vec![
            (
                "seg-000001",
                vec![sample_batch(vec![1, 2], vec![Some("left"), Some("right")])],
            ),
            ("seg-000002", vec![sample_batch(vec![3], vec![None])]),
        ],
    );
    let commit = request(&package_dir, &built, WriteDisposition::Append);

    let mut local = test_filesystem(temp.path().join("lake")).unwrap();
    assert_staged_abort_cleans_destination(&mut local, &package_dir, commit.clone());
    let reachability = local.execution().content_reachability_store().unwrap();
    let snapshot = reachability
        .reclamation_candidates(local.store().namespace(), 8)
        .unwrap()
        .pop()
        .unwrap();
    let proof = cdf_kernel::ContentReclamationProof::prove(
        snapshot.candidate,
        snapshot.same_content_claims,
        Vec::new(),
        snapshot.checked_roots,
    )
    .unwrap();
    reachability
        .reserve_reclamation(
            proof,
            cdf_kernel::ContentReclamationReservationId::new("crash-before-delete").unwrap(),
        )
        .unwrap()
        .unwrap();
    let reclaimed = local.reclaim_unreachable_content(8).unwrap();
    assert_eq!(reclaimed.objects_deleted, 1);
    assert_eq!(reclaimed.recovered_reservations, 1);
    assert!(
        local
            .store()
            .list_prefix(local.execution(), "targets/orders/objects/sha256/")
            .unwrap()
            .is_empty()
    );

    let mut object_store = test_object_store(Arc::new(InMemory::default()), "remote").unwrap();
    assert_staged_abort_cleans_destination(&mut object_store, &package_dir, commit);
    let retained = object_store.reclaim_unreachable_content(8).unwrap();
    assert_eq!(retained.unsupported, 1);
}

#[test]
fn content_reclamation_retains_a_replaced_local_generation() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-reclamation-generation");
    let built = build_package(
        &package_dir,
        "pkg-reclamation-generation",
        vec![(
            "seg-000001",
            vec![sample_batch(vec![1, 2], vec![Some("left"), Some("right")])],
        )],
    );
    let commit = request(&package_dir, &built, WriteDisposition::Append);
    let lake = temp.path().join("lake-generation");
    let mut destination = test_filesystem(&lake).unwrap();
    assert_staged_abort_cleans_destination(&mut destination, &package_dir, commit);
    let object = destination
        .store()
        .list_prefix(destination.execution(), "targets/orders/objects/sha256/")
        .unwrap()
        .pop()
        .unwrap();
    fs::write(lake.join(&object.key), b"replacement-generation").unwrap();

    let report = destination.reclaim_unreachable_content(8).unwrap();
    assert_eq!(report.generation_conflicts, 1);
    assert!(lake.join(object.key).exists());
}

#[test]
fn staged_segment_ingress_preserves_manifest_segment_order() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-session-equivalence");
    let built = build_package(
        &package_dir,
        "pkg-session-equivalence",
        vec![
            (
                "seg-000001",
                vec![sample_batch(vec![1, 2], vec![Some("ada"), Some("grace")])],
            ),
            ("seg-000002", vec![sample_batch(vec![3], vec![None])]),
        ],
    );
    let commit = request(&package_dir, &built, WriteDisposition::Append);
    let mut dest = test_object_store(Arc::new(InMemory::default()), "").unwrap();
    let committed = commit_through_ingress(&mut dest, &package_dir, commit).unwrap();
    let manifest = committed.object_manifest;
    let receipt = committed.receipt;
    assert!(!committed.duplicate);
    assert_eq!(
        receipt.segment_acks,
        vec![
            SegmentAck {
                segment_id: SegmentId::new("seg-000001").unwrap(),
                row_count: 2,
                byte_count: 32,
            },
            SegmentAck {
                segment_id: SegmentId::new("seg-000002").unwrap(),
                row_count: 1,
                byte_count: 16,
            },
        ]
    );
    assert_eq!(manifest.objects.len(), 1);
    assert_eq!(manifest.objects[0].segments.len(), 2);
    assert_eq!(manifest.objects[0].segments[0].segment_id, "seg-000001");
    assert_eq!(manifest.objects[0].segments[1].segment_id, "seg-000002");
    assert_eq!(manifest.objects[0].segments[1].row_offset, 2);
    assert!(
        manifest
            .objects
            .iter()
            .all(|object| object.byte_count != object.package_byte_count)
    );
    assert!(dest.verify_receipt(&receipt).unwrap().verified);
    let protocol: &dyn DestinationProtocol = &dest;
    assert!(protocol.verify(&receipt).unwrap().verified);
}

#[test]
fn staged_grouping_materializes_deterministic_eight_eight_one_objects() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-session-17");
    let built = build_package(
        &package_dir,
        "pkg-session-17",
        (0..17)
            .map(|ordinal| {
                (
                    format!("seg-{ordinal:06}"),
                    vec![sample_batch(vec![i64::from(ordinal)], vec![Some("row")])],
                )
            })
            .collect(),
    );
    let commit = request(&package_dir, &built, WriteDisposition::Append);

    let mut first = test_filesystem(temp.path().join("first")).unwrap();
    let first_outcome = commit_through_ingress(&mut first, &package_dir, commit.clone()).unwrap();
    let mut second = test_filesystem(temp.path().join("second")).unwrap();
    let second_outcome = commit_through_ingress(&mut second, &package_dir, commit).unwrap();

    let expected_group_sizes = [8, 8, 1];
    assert_eq!(first_outcome.object_manifest.objects.len(), 3);
    assert_eq!(
        first_outcome
            .object_manifest
            .objects
            .iter()
            .map(|object| object.segments.len())
            .collect::<Vec<_>>(),
        expected_group_sizes
    );
    for object in &first_outcome.object_manifest.objects {
        for (row_offset, segment) in object.segments.iter().enumerate() {
            assert_eq!(segment.row_offset, row_offset as u64);
        }
        assert_eq!(
            object.key,
            data_object_key(
                first.object_key_encoder(),
                &TargetName::new("orders").unwrap(),
                &object.sha256,
            )
        );
    }
    assert_eq!(first_outcome.receipt.segment_acks.len(), 17);
    let logical_objects = |manifest: &ParquetObjectManifest| {
        manifest
            .objects
            .iter()
            .cloned()
            .map(|mut object| {
                object.etag = None;
                object
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        logical_objects(&first_outcome.object_manifest),
        logical_objects(&second_outcome.object_manifest),
        "writer completion order must not affect grouped objects or provenance"
    );
    assert_eq!(
        first_outcome.receipt.segment_acks,
        second_outcome.receipt.segment_acks
    );
    assert!(
        first
            .verify_receipt(&first_outcome.receipt)
            .unwrap()
            .verified
    );
    assert!(
        second
            .verify_receipt(&second_outcome.receipt)
            .unwrap()
            .verified
    );
    assert!(
        first
            .store()
            .list_prefix(first.execution(), "targets/orders/staging/")
            .unwrap()
            .is_empty(),
        "successful final binding must leave no attempt-owned staging bytes"
    );
}

#[test]
fn staged_attempt_records_the_exact_prepared_physical_plan() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-physical-plan");
    let built = build_package(
        &package_dir,
        "pkg-physical-plan",
        vec![(
            "seg-000001",
            vec![sample_batch(vec![1, 2], vec![Some("left"), Some("right")])],
        )],
    );
    let mut destination = test_object_store(Arc::new(InMemory::default()), "lake").unwrap();
    let commit = request(&package_dir, &built, WriteDisposition::Append);
    let staged = stage_through_ingress(&mut destination, &package_dir, commit.clone()).unwrap();
    let metadata_key = crate::store::staged_attempt_metadata_key(
        destination.object_key_encoder(),
        &commit.commit.target,
        staged.staging_lease.authority_domain_id(),
        &staged.attempt_id,
        staged.staging_lease.fencing_token(),
    );
    let metadata: serde_json::Value = serde_json::from_slice(
        &destination
            .store()
            .get_required(destination.execution(), &metadata_key)
            .unwrap(),
    )
    .unwrap();
    assert_eq!(metadata["physical_plan_path"], "arrow_ipc_to_parquet");
    assert_eq!(metadata["physical_plan_version"], 5);
    assert_eq!(
        metadata["object_publication_mode"],
        "atomic_content_create_v1"
    );
    assert_eq!(metadata["writers"], 2);
    assert_eq!(metadata["rows_per_batch"], 64 * 1024);
    assert_eq!(metadata["bytes_per_batch"], 16 * 1024 * 1024);
    assert_eq!(metadata["object_target_package_bytes"], 256 * 1024 * 1024);
    assert_eq!(metadata["max_segments_per_object"], 8);
    staged.session.abort().unwrap();
}

#[test]
fn staged_segment_ingress_duplicate_replay_preserves_existing_manifest() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-session-duplicate");
    let built = build_package(
        &package_dir,
        "pkg-session-duplicate",
        vec![(
            "seg-000001",
            vec![sample_batch(vec![1, 2], vec![Some("left"), Some("right")])],
        )],
    );
    let mut dest = test_object_store(Arc::new(InMemory::default()), "").unwrap();
    let commit = request(&package_dir, &built, WriteDisposition::Append);

    let first = commit_through_ingress(&mut dest, &package_dir, commit.clone()).unwrap();
    let manifest_before = dest
        .store()
        .get_required(dest.execution(), &first.plan.manifest_key)
        .unwrap();
    let duplicate = commit_through_ingress(&mut dest, &package_dir, commit).unwrap();
    let manifest_after = dest
        .store()
        .get_required(dest.execution(), &first.plan.manifest_key)
        .unwrap();

    assert!(duplicate.duplicate);
    assert_eq!(
        duplicate.verification,
        cdf_runtime::DestinationCommitVerification::Independent
    );
    assert_eq!(first.receipt.receipt_id, duplicate.receipt.receipt_id);
    assert_eq!(manifest_before, manifest_after);
    assert!(dest.verify_receipt(&duplicate.receipt).unwrap().verified);

    let receipts = PackageReader::open(&package_dir)
        .unwrap()
        .receipts()
        .unwrap();
    assert!(
        receipts.is_empty(),
        "package receipts belong to orchestration"
    );
}

#[test]
fn in_memory_object_store_duplicate_replay_is_noop() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg");
    let built = build_package(
        &package_dir,
        "pkg-replay",
        vec![(
            "seg-000001",
            vec![sample_batch(vec![1, 2], vec![Some("left"), Some("right")])],
        )],
    );
    let store = Arc::new(InMemory::default());
    let mut dest = test_object_store(store, "lake").unwrap();
    let commit = request(&package_dir, &built, WriteDisposition::Append);

    let first = commit_through_ingress(&mut dest, &package_dir, commit.clone()).unwrap();
    assert!(first.object_manifest.committed_at_ms > 1_700_000_000_000);
    let manifest_before = dest
        .store()
        .get_required(dest.execution(), &first.plan.manifest_key)
        .unwrap();
    let duplicate_plan = plan_package_for_test(&dest, &package_dir, &commit).unwrap();
    assert!(duplicate_plan.duplicate);
    let second = commit_through_ingress(&mut dest, &package_dir, commit).unwrap();
    let manifest_after = dest
        .store()
        .get_required(dest.execution(), &first.plan.manifest_key)
        .unwrap();

    assert!(!first.duplicate);
    assert!(second.duplicate);
    assert!(second.plan.duplicate);
    assert_eq!(first.receipt.receipt_id, second.receipt.receipt_id);
    assert_eq!(manifest_before, manifest_after);
}

#[test]
fn concurrent_same_token_publication_is_immutable_and_independently_verifiable() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-concurrent-token");
    let built = build_package(
        &package_dir,
        "pkg-concurrent-token",
        vec![(
            "seg-000001",
            vec![sample_batch(vec![1, 2], vec![Some("left"), Some("right")])],
        )],
    );
    let store = Arc::new(InMemory::default());
    let mut first_destination = test_object_store(store.clone(), "lake").unwrap();
    let mut second_destination = test_object_store(store.clone(), "lake").unwrap();
    let commit = request(&package_dir, &built, WriteDisposition::Append);
    let first = stage_through_ingress_with_attempt(
        &mut first_destination,
        &package_dir,
        commit.clone(),
        cdf_runtime::LoadAttemptId::new("concurrent-attempt-a").unwrap(),
    )
    .unwrap();
    let second = stage_through_ingress_with_attempt(
        &mut second_destination,
        &package_dir,
        commit.clone(),
        cdf_runtime::LoadAttemptId::new("concurrent-attempt-b").unwrap(),
    )
    .unwrap();

    let first_thread = std::thread::spawn(move || bind_staged_test_commit(first));
    let second_thread = std::thread::spawn(move || bind_staged_test_commit(second));
    let (first, first_plan) = first_thread.join().unwrap().unwrap();
    let (second, second_plan) = second_thread.join().unwrap().unwrap();

    assert_eq!(first_plan.manifest_key, second_plan.manifest_key);
    assert_eq!(first.receipt, second.receipt);
    assert!(matches!(
        first.verification,
        cdf_runtime::DestinationCommitVerification::VerifiedAtCommit(_)
    ));
    assert!(matches!(
        second.verification,
        cdf_runtime::DestinationCommitVerification::VerifiedAtCommit(_)
    ));
    let verifier = test_object_store(store, "lake").unwrap();
    assert!(verifier.verify_receipt(&first.receipt).unwrap().verified);
}

#[test]
fn abandoned_attempt_cleanup_requires_exact_expiry_proof() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-proof-gated-cleanup");
    let built = build_package(
        &package_dir,
        "pkg-proof-gated-cleanup",
        vec![(
            "seg-000001",
            vec![sample_batch(vec![1, 2], vec![Some("left"), Some("right")])],
        )],
    );
    let destination = test_object_store(Arc::new(InMemory::default()), "lake").unwrap();
    let commit = request(&package_dir, &built, WriteDisposition::Append);
    let scopes = Arc::new(cdf_state_sqlite::InMemoryScopeLeaseStore::new());
    let services = destination
        .execution()
        .with_staging_lease_authority(Arc::new(cdf_runtime::ScopeStagingLeaseAuthority::new(
            scopes,
        )))
        .unwrap();
    let attempt_id = cdf_runtime::LoadAttemptId::new("proof-gated-cleanup").unwrap();
    let managed_lease = services
        .acquire_staging_lease(cdf_runtime::StagingLeaseIdentity::new(
            destination.sheet().destination.clone(),
            commit.commit.target.clone(),
            attempt_id.clone(),
        ))
        .unwrap();
    let staging_lease = managed_lease.snapshot().unwrap();
    let metadata_key = crate::store::staged_attempt_metadata_key(
        destination.object_key_encoder(),
        &commit.commit.target,
        staging_lease.authority_domain_id(),
        &attempt_id,
        staging_lease.fencing_token(),
    );
    let staging_key = staged_data_object_key(
        destination.object_key_encoder(),
        &commit.commit.target,
        staging_lease.authority_domain_id(),
        &attempt_id,
        staging_lease.fencing_token(),
        0,
    );
    destination
        .store()
        .put(
            destination.execution(),
            &metadata_key,
            serde_json::to_vec(&serde_json::json!({
                "version": 1,
                "target": commit.commit.target.as_str(),
                "attempt_id": attempt_id.as_str(),
                "physical_plan_path": "arrow_ipc_to_parquet",
                "physical_plan_version": 5,
                "object_publication_mode": "atomic_content_create_v1",
                "writers": 1,
                "rows_per_batch": 65_536,
                "bytes_per_batch": 16_777_216,
                "object_target_package_bytes": 268_435_456,
                "max_segments_per_object": 8,
                "started_at_ms": 1,
                "staging_lease": staging_lease.clone(),
            }))
            .unwrap(),
        )
        .unwrap();
    destination
        .store()
        .put(destination.execution(), &staging_key, vec![1, 2, 3])
        .unwrap();
    managed_lease.finish().unwrap();

    let candidates = destination
        .staging_cleanup_candidates(&commit.commit.target)
        .unwrap();
    let candidate = candidates
        .iter()
        .find(|candidate| candidate.lease() == &staging_lease)
        .unwrap();
    let wrong_managed = services
        .acquire_staging_lease(cdf_runtime::StagingLeaseIdentity::new(
            candidate.lease().identity.destination_id.clone(),
            candidate.lease().identity.target.clone(),
            cdf_runtime::LoadAttemptId::new("different-attempt").unwrap(),
        ))
        .unwrap();
    let wrong_lease = wrong_managed.snapshot().unwrap();
    wrong_managed.finish().unwrap();
    let wrong_proof = services
        .prove_expired_staging_lease(&wrong_lease)
        .unwrap()
        .unwrap();
    assert!(
        destination
            .cleanup_expired_staging_candidate(
                candidate,
                wrong_proof.proof(),
                &wrong_proof.mutation_guard().unwrap(),
            )
            .is_err()
    );
    wrong_proof.finish().unwrap();
    assert!(
        destination
            .store()
            .exists(destination.execution(), &staging_key)
            .unwrap()
    );

    let proof = services
        .prove_expired_staging_lease(candidate.lease())
        .unwrap()
        .unwrap();
    assert!(
        destination
            .cleanup_expired_staging_candidate(
                candidate,
                proof.proof(),
                &proof.mutation_guard().unwrap(),
            )
            .unwrap()
            >= 2
    );
    proof.finish().unwrap();
    assert!(
        !destination
            .store()
            .exists(destination.execution(), &staging_key)
            .unwrap()
    );
}

#[test]
fn independent_lease_domains_cannot_collide_or_collect_each_others_staging() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-domain-fencing");
    let built = build_package(
        &package_dir,
        "pkg-domain-fencing",
        vec![(
            "seg-000001",
            vec![sample_batch(vec![1, 2], vec![Some("left"), Some("right")])],
        )],
    );
    let store = Arc::new(InMemory::default());
    let destination = test_object_store(store, "lake").unwrap();
    let commit = request(&package_dir, &built, WriteDisposition::Append);
    let attempt_id = cdf_runtime::LoadAttemptId::new("shared-attempt").unwrap();
    let services = ["authority-a.db", "authority-b.db"].map(|name| {
        let scopes = Arc::new(
            cdf_state_sqlite::SqliteScopeLeaseStore::open(temp.path().join(name)).unwrap(),
        );
        destination
            .execution()
            .with_staging_lease_authority(Arc::new(cdf_runtime::ScopeStagingLeaseAuthority::new(
                scopes,
            )))
            .unwrap()
    });

    let mut staged_attempts = Vec::new();
    for (index, services) in services.iter().enumerate() {
        let managed = services
            .acquire_staging_lease(cdf_runtime::StagingLeaseIdentity::new(
                destination.sheet().destination.clone(),
                commit.commit.target.clone(),
                attempt_id.clone(),
            ))
            .unwrap();
        let lease = managed.snapshot().unwrap();
        assert_eq!(lease.fencing_token(), 1);
        let key = staged_data_object_key(
            destination.object_key_encoder(),
            &commit.commit.target,
            lease.authority_domain_id(),
            &attempt_id,
            lease.fencing_token(),
            0,
        );
        let metadata_key = crate::store::staged_attempt_metadata_key(
            destination.object_key_encoder(),
            &commit.commit.target,
            lease.authority_domain_id(),
            &attempt_id,
            lease.fencing_token(),
        );
        destination
            .store()
            .put(
                destination.execution(),
                &metadata_key,
                serde_json::to_vec(&serde_json::json!({
                    "version": 1,
                    "target": commit.commit.target.as_str(),
                    "attempt_id": attempt_id.as_str(),
                    "physical_plan_path": "arrow_ipc_to_parquet",
                    "physical_plan_version": 5,
                    "object_publication_mode": "atomic_content_create_v1",
                    "writers": 1,
                    "rows_per_batch": 65_536,
                    "bytes_per_batch": 16_777_216,
                    "object_target_package_bytes": 268_435_456,
                    "max_segments_per_object": 8,
                    "started_at_ms": index,
                    "staging_lease": lease.clone(),
                }))
                .unwrap(),
            )
            .unwrap();
        destination
            .store()
            .put(
                destination.execution(),
                &key,
                vec![u8::try_from(index).unwrap()],
            )
            .unwrap();
        managed.finish().unwrap();
        staged_attempts.push((lease, key));
    }

    assert_ne!(
        staged_attempts[0].0.authority_domain_id(),
        staged_attempts[1].0.authority_domain_id()
    );
    assert_ne!(staged_attempts[0].1, staged_attempts[1].1);
    let candidates = destination
        .staging_cleanup_candidates(&commit.commit.target)
        .unwrap();
    assert_eq!(candidates.len(), 2);
    let first = candidates
        .iter()
        .find(|candidate| candidate.lease() == &staged_attempts[0].0)
        .unwrap();
    assert!(
        services[1]
            .prove_expired_staging_lease(first.lease())
            .unwrap()
            .is_none()
    );
    assert!(
        destination
            .store()
            .exists(destination.execution(), &staged_attempts[0].1)
            .unwrap()
    );

    for (index, services) in services.iter().enumerate() {
        let candidate = candidates
            .iter()
            .find(|candidate| candidate.lease() == &staged_attempts[index].0)
            .unwrap();
        let proof = services
            .prove_expired_staging_lease(candidate.lease())
            .unwrap()
            .unwrap();
        destination
            .cleanup_expired_staging_candidate(
                candidate,
                proof.proof(),
                &proof.mutation_guard().unwrap(),
            )
            .unwrap();
        proof.finish().unwrap();
    }
}

#[cfg(unix)]
#[test]
fn failed_staging_cleanup_retains_attempt_marker_until_payload_deletion_completes() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("lake");
    let destination = test_filesystem(&root).unwrap();
    let target = TargetName::new("orders").unwrap();
    let attempt_id = cdf_runtime::LoadAttemptId::new("marker-last-cleanup").unwrap();
    let managed = destination
        .execution()
        .acquire_staging_lease(cdf_runtime::StagingLeaseIdentity::new(
            destination.sheet().destination.clone(),
            target.clone(),
            attempt_id.clone(),
        ))
        .unwrap();
    let lease = managed.snapshot().unwrap();
    let prefix = crate::store::staged_attempt_prefix(
        destination.object_key_encoder(),
        &target,
        lease.authority_domain_id(),
        &attempt_id,
        lease.fencing_token(),
    );
    let marker = format!("{prefix}attempt.json");
    destination
        .store()
        .put(
            destination.execution(),
            &marker,
            serde_json::to_vec(&serde_json::json!({
                "version": 1,
                "target": target.as_str(),
                "attempt_id": attempt_id.as_str(),
                "physical_plan_path": "arrow_ipc_to_parquet",
                "physical_plan_version": 5,
                "object_publication_mode": "atomic_content_create_v1",
                "writers": 1,
                "rows_per_batch": 65_536,
                "bytes_per_batch": 16_777_216,
                "object_target_package_bytes": 268_435_456,
                "max_segments_per_object": 8,
                "started_at_ms": 1,
                "staging_lease": lease.clone(),
            }))
            .unwrap(),
        )
        .unwrap();
    let blocked_key = format!("{prefix}blocked/payload.parquet");
    destination
        .store()
        .put(destination.execution(), &blocked_key, vec![1, 2, 3])
        .unwrap();
    managed.finish().unwrap();

    let candidate = destination
        .staging_cleanup_candidates(&target)
        .unwrap()
        .into_iter()
        .find(|candidate| candidate.lease() == &lease)
        .unwrap();
    let proof = destination
        .execution()
        .prove_expired_staging_lease(candidate.lease())
        .unwrap()
        .unwrap();
    let blocked_dir = root.join(format!("{prefix}blocked"));
    let original = std::fs::metadata(&blocked_dir).unwrap().permissions();
    std::fs::set_permissions(&blocked_dir, std::fs::Permissions::from_mode(0o500)).unwrap();
    assert!(
        destination
            .cleanup_expired_staging_candidate(
                &candidate,
                proof.proof(),
                &proof.mutation_guard().unwrap(),
            )
            .is_err()
    );
    assert!(
        destination
            .store()
            .exists(destination.execution(), &marker)
            .unwrap(),
        "enumerable attempt marker must survive partial cleanup"
    );

    std::fs::set_permissions(&blocked_dir, original).unwrap();
    destination
        .cleanup_expired_staging_candidate(
            &candidate,
            proof.proof(),
            &proof.mutation_guard().unwrap(),
        )
        .unwrap();
    proof.finish().unwrap();
    assert!(
        !destination
            .store()
            .exists(destination.execution(), &marker)
            .unwrap()
    );
}

#[test]
fn constrained_writer_memory_fails_cleanly_instead_of_waiting_on_its_input() {
    let memory: Arc<dyn cdf_memory::MemoryCoordinator> = Arc::new(
        cdf_memory::DeterministicMemoryCoordinator::new(2 * 1024 * 1024, BTreeMap::new()).unwrap(),
    );
    let blocker = memory
        .try_reserve(
            &cdf_memory::ReservationRequest::new(
                cdf_memory::ConsumerKey::new(
                    "retained-staged-input",
                    cdf_memory::MemoryClass::Destination,
                )
                .unwrap(),
                2 * 1024 * 1024,
            )
            .unwrap(),
        )
        .unwrap()
        .unwrap();
    let spill: Arc<dyn cdf_runtime::SpillBudgetCoordinator> =
        Arc::new(cdf_runtime::FixedSpillBudget::new(64 * 1024 * 1024).unwrap());
    let batch = sample_batch(vec![1, 2], vec![Some("left"), Some("right")]);
    let segment = CommitSegment::new(
        StateSegment {
            segment_id: SegmentId::new("seg-low-memory").unwrap(),
            scope: ScopeKey::Resource,
            output_position: SourcePosition::Cursor(CursorPosition {
                version: 1,
                field: "id".to_owned(),
                value: CursorValue::U64(2),
            }),
            row_count: 2,
            byte_count: 2,
        },
        2,
        canonical_batches(vec![batch], 0),
    );
    let result = crate::package::write_parquet_segment(
        segment,
        test_writer_settings(),
        memory,
        spill,
        tempfile::NamedTempFile::new().unwrap(),
    );
    drop(blocker);
    let error = match result {
        Ok(_) => panic!("constrained writer memory unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("additional accounted writer bytes"),
        "{error}"
    );
}

#[test]
fn replace_writes_current_pointer_to_latest_manifest() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("lake");
    let mut dest = test_filesystem(&root).unwrap();

    let first_dir = temp.path().join("pkg-first");
    let first = build_package(
        &first_dir,
        "pkg-first",
        vec![("seg-000001", vec![sample_batch(vec![1], vec![Some("old")])])],
    );
    let first_request = request(&first_dir, &first, WriteDisposition::Replace);
    let first_outcome =
        commit_through_ingress(&mut dest, &first_dir, first_request.clone()).unwrap();

    let second_dir = temp.path().join("pkg-second");
    let second = build_package(
        &second_dir,
        "pkg-second",
        vec![(
            "seg-000001",
            vec![sample_batch(vec![9, 10], vec![Some("new"), Some("rows")])],
        )],
    );
    let second_outcome = commit_through_ingress(
        &mut dest,
        &second_dir,
        request(&second_dir, &second, WriteDisposition::Replace),
    )
    .unwrap();

    let pointer_key = second_outcome.plan.current_pointer_key.as_ref().unwrap();
    let pointer_bytes = dest
        .store()
        .get_required(dest.execution(), pointer_key)
        .unwrap();
    let pointer: CurrentReplacePointer = serde_json::from_slice(&pointer_bytes).unwrap();

    assert_ne!(
        first_outcome.plan.manifest_key,
        second_outcome.plan.manifest_key
    );
    assert_eq!(pointer.manifest_key, second_outcome.plan.manifest_key);
    assert_eq!(pointer.generation, 2);
    assert!(
        dest.verify_receipt(&second_outcome.receipt)
            .unwrap()
            .verified
    );

    let replayed_first = commit_through_ingress(&mut dest, &first_dir, first_request).unwrap();
    assert!(replayed_first.duplicate);
    let pointer: CurrentReplacePointer = serde_json::from_slice(
        &dest
            .store()
            .get_required(dest.execution(), pointer_key)
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        pointer.manifest_key, second_outcome.plan.manifest_key,
        "an older duplicate replace must not roll the current pointer back"
    );
}

#[test]
fn zero_data_append_and_replace_emit_receipts_without_objects_or_pointer_mutation() {
    let temp = tempfile::tempdir().unwrap();
    let mut dest = test_object_store(Arc::new(InMemory::default()), "").unwrap();

    let data_dir = temp.path().join("pkg-data");
    let data = build_package(
        &data_dir,
        "pkg-data",
        vec![(
            "seg-000001",
            vec![sample_batch(vec![1, 2], vec![Some("old"), Some("rows")])],
        )],
    );
    let seeded = commit_through_ingress(
        &mut dest,
        &data_dir,
        request(&data_dir, &data, WriteDisposition::Replace),
    )
    .unwrap();
    let pointer_key = seeded.plan.current_pointer_key.clone().unwrap();
    let pointer_before = dest
        .store()
        .get_required(dest.execution(), &pointer_key)
        .unwrap();

    for (package_id, disposition) in [
        ("pkg-empty-append", WriteDisposition::Append),
        ("pkg-empty-replace", WriteDisposition::Replace),
    ] {
        let package_dir = temp.path().join(package_id);
        let empty = build_package(
            &package_dir,
            package_id,
            Vec::<(&str, Vec<RecordBatch>)>::new(),
        );
        let commit = request(&package_dir, &empty, disposition.clone());
        let plan = plan_package_for_test(&dest, &package_dir, &commit).unwrap();
        assert!(plan.replace_pointer_key.is_none());

        let outcome = commit_through_ingress(&mut dest, &package_dir, commit).unwrap();
        assert!(outcome.receipt.segment_acks.is_empty());
        assert_eq!(outcome.receipt.counts.rows_written, 0);
        assert!(dest.verify_receipt(&outcome.receipt).unwrap().verified);
    }

    let pointer_after = dest
        .store()
        .get_required(dest.execution(), &pointer_key)
        .unwrap();
    assert_eq!(pointer_after, pointer_before);
}

#[test]
fn dry_run_plan_reports_binding_keys_without_writing() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-plan");
    let built = build_package(
        &package_dir,
        "pkg-plan",
        vec![(
            "seg-000001",
            vec![sample_batch(vec![1], vec![Some("planned")])],
        )],
    );
    let root = temp.path().join("lake");
    let dest = test_filesystem(&root).unwrap();

    let plan = plan_package_for_test(
        &dest,
        &package_dir,
        &request(&package_dir, &built, WriteDisposition::Replace),
    )
    .unwrap();
    let encoded_token = built.hash.as_str().replace(':', "~3a");

    assert_eq!(plan.rows_planned, 1);
    assert_eq!(
        plan.bytes_planned,
        built
            .segments
            .iter()
            .map(|segment| segment.byte_count)
            .sum::<u64>()
    );
    assert_eq!(
        plan.manifest_key,
        format!("targets/orders/packages/{encoded_token}/manifest.json")
    );
    let expected_settlement = format!("targets/orders/packages/{encoded_token}/replace.json");
    assert_eq!(
        plan.replace_pointer_key.as_deref(),
        Some(expected_settlement.as_str())
    );
    assert_eq!(
        plan.current_pointer_key.as_deref(),
        Some("targets/orders/current.json")
    );
    assert!(plan.replace_pointer_key.is_some());
    assert!(
        !dest
            .store()
            .exists(dest.execution(), &plan.manifest_key)
            .unwrap()
    );
    assert!(!root.join("targets").exists());
}

#[test]
fn duplicate_column_names_fail_before_writing_objects() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-duplicate-columns");
    let schema = Arc::new(Schema::new(vec![
        Field::new("duplicate", DataType::Int64, false),
        Field::new("duplicate", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![2])),
        ],
    )
    .unwrap();
    let built = build_package(
        &package_dir,
        "pkg-duplicate-columns",
        vec![("seg-000001", vec![batch])],
    );
    let mut dest = test_object_store(Arc::new(InMemory::default()), "").unwrap();
    let commit = request(&package_dir, &built, WriteDisposition::Append);
    let plan = plan_package_for_test(&dest, &package_dir, &commit).unwrap();

    let error = commit_through_ingress(&mut dest, &package_dir, commit).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("duplicate Parquet column name duplicate")
    );
    assert!(
        !dest
            .store()
            .exists(dest.execution(), &plan.manifest_key)
            .unwrap()
    );
    assert!(
        dest.store()
            .list_prefix(dest.execution(), "targets/orders/objects/sha256/")
            .unwrap()
            .is_empty()
    );
}

#[test]
fn canonical_json_keeps_array_separators_in_order() {
    let bytes = canonical_json_bytes(&serde_json::json!([1, 2, 3])).unwrap();
    assert_eq!(bytes, b"[1,2,3]");
}

#[test]
fn replace_duplicate_replay_requires_immutable_settlement_identity() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-replace-replay");
    let built = build_package(
        &package_dir,
        "pkg-replace-replay",
        vec![(
            "seg-000001",
            vec![sample_batch(vec![1, 2], vec![Some("left"), Some("right")])],
        )],
    );
    let mut dest = test_object_store(Arc::new(InMemory::default()), "").unwrap();
    let commit = request(&package_dir, &built, WriteDisposition::Replace);
    let first = commit_through_ingress(&mut dest, &package_dir, commit.clone()).unwrap();
    let pointer_key = first.plan.replace_pointer_key.as_ref().unwrap().clone();
    let original_pointer = load_replace_pointer(&dest, &pointer_key);

    let replay = commit_through_ingress(&mut dest, &package_dir, commit.clone()).unwrap();
    assert!(replay.duplicate);
    assert!(replay.plan.duplicate);
    assert!(dest.verify_receipt(&replay.receipt).unwrap().verified);

    for field in [
        "manifest_key",
        "manifest_sha256",
        "target",
        "package_hash",
        "idempotency_token",
        "schema_hash",
    ] {
        let mut pointer = original_pointer.clone();
        match field {
            "manifest_key" => pointer.manifest_key.push_str("-stale"),
            "manifest_sha256" => pointer.manifest_sha256.push_str("00"),
            "target" => pointer.target.push_str("_other"),
            "package_hash" => pointer.package_hash.push_str("00"),
            "idempotency_token" => pointer.idempotency_token.push_str("00"),
            "schema_hash" => pointer.schema_hash.push_str("-other"),
            _ => unreachable!(),
        }
        store_replace_pointer(&dest, &pointer_key, &pointer);

        let error = commit_through_ingress(&mut dest, &package_dir, commit.clone()).unwrap_err();
        assert!(error.to_string().contains("replace settlement"), "{error}");
    }
    store_replace_pointer(&dest, &pointer_key, &original_pointer);
}

#[test]
fn verify_receipt_rejects_replace_pointer_identity_mismatch() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-replace-verify");
    let built = build_package(
        &package_dir,
        "pkg-replace-verify",
        vec![(
            "seg-000001",
            vec![sample_batch(vec![1], vec![Some("current")])],
        )],
    );
    let mut dest = test_object_store(Arc::new(InMemory::default()), "").unwrap();
    let outcome = commit_through_ingress(
        &mut dest,
        &package_dir,
        request(&package_dir, &built, WriteDisposition::Replace),
    )
    .unwrap();
    let pointer_key = replace_pointer_key_from_receipt(&outcome.receipt).to_owned();
    let original_pointer = load_replace_pointer(&dest, &pointer_key);

    for field in [
        "manifest_key",
        "manifest_sha256",
        "target",
        "package_hash",
        "idempotency_token",
        "schema_hash",
    ] {
        let mut pointer = original_pointer.clone();
        match field {
            "manifest_key" => pointer.manifest_key.push_str("-other"),
            "manifest_sha256" => pointer.manifest_sha256.push_str("00"),
            "target" => pointer.target.push_str("_other"),
            "package_hash" => pointer.package_hash.push_str("00"),
            "idempotency_token" => pointer.idempotency_token.push_str("00"),
            "schema_hash" => pointer.schema_hash.push_str("-other"),
            _ => unreachable!(),
        }
        let pointer = store_replace_pointer(&dest, &pointer_key, &pointer);
        let receipt = receipt_with_pointer_store(&outcome.receipt, pointer);

        let verification = dest.verify_receipt(&receipt).unwrap();
        assert!(!verification.verified, "{field} mismatch was accepted");
        assert!(
            verification
                .reason
                .unwrap()
                .contains("does not match manifest")
        );
    }
    store_replace_pointer(&dest, &pointer_key, &original_pointer);
}

#[test]
fn verify_receipt_rejects_manifest_identity_mismatch() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-manifest-verify");
    let built = build_package(
        &package_dir,
        "pkg-manifest-verify",
        vec![(
            "seg-000001",
            vec![sample_batch(vec![1], vec![Some("manifest")])],
        )],
    );
    let mut dest = test_object_store(Arc::new(InMemory::default()), "").unwrap();
    let outcome = commit_through_ingress(
        &mut dest,
        &package_dir,
        request(&package_dir, &built, WriteDisposition::Append),
    )
    .unwrap();
    let key = manifest_key(&outcome.receipt).to_owned();
    let original_manifest = load_manifest(&dest, &key);

    for field in [
        "target",
        "package_hash",
        "idempotency_token",
        "disposition",
        "schema_hash",
    ] {
        let mut manifest = original_manifest.clone();
        match field {
            "target" => manifest.target.push_str("_other"),
            "package_hash" => manifest.package_hash.push_str("00"),
            "idempotency_token" => manifest.idempotency_token.push_str("00"),
            "disposition" => manifest.disposition = WriteDisposition::Replace,
            "schema_hash" => manifest.schema_hash.push_str("-other"),
            _ => unreachable!(),
        }
        let manifest = store_manifest(&dest, &key, &manifest);
        let receipt = receipt_with_manifest_store(&outcome.receipt, manifest);

        let verification = dest.verify_receipt(&receipt).unwrap();
        assert!(!verification.verified, "{field} mismatch was accepted");
        assert!(verification.reason.unwrap().contains("manifest"));
    }
    store_manifest(&dest, &key, &original_manifest);
}

#[test]
fn object_store_root_prefix_normalizes_and_rejects_parent_traversal() {
    assert!(test_object_store(Arc::new(InMemory::default()), "lake/../bad").is_err());

    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-prefixed");
    let built = build_package(
        &package_dir,
        "pkg-prefixed",
        vec![(
            "seg-000001",
            vec![sample_batch(vec![1], vec![Some("prefixed")])],
        )],
    );
    let store = Arc::new(InMemory::default());
    let mut dest = test_object_store(store.clone(), "//lake//").unwrap();
    let outcome = commit_through_ingress(
        &mut dest,
        &package_dir,
        request(&package_dir, &built, WriteDisposition::Append),
    )
    .unwrap();
    let object_key = &outcome.object_manifest.objects[0].key;

    let prefixed = ObjectPath::from(format!("lake/{object_key}"));
    let prefixed_store = store.clone();
    let prefixed = dest
        .execution()
        .run_io(async move { Ok(prefixed_store.head(&prefixed).await) })
        .unwrap();
    assert!(prefixed.is_ok());
    let unprefixed = ObjectPath::from(object_key.as_str());
    let unprefixed = dest
        .execution()
        .run_io(async move { Ok(store.head(&unprefixed).await) })
        .unwrap();
    assert!(unprefixed.is_err());
}

#[test]
fn verification_fails_for_tampered_and_missing_objects() {
    let temp = tempfile::tempdir().unwrap();
    let store = Arc::new(InMemory::default());
    let mut dest = test_object_store(store, "").unwrap();

    let tamper_dir = temp.path().join("pkg-tamper");
    let tamper_pkg = build_package(
        &tamper_dir,
        "pkg-tamper",
        vec![(
            "seg-000001",
            vec![sample_batch(vec![1], vec![Some("tamper")])],
        )],
    );
    let tamper = commit_through_ingress(
        &mut dest,
        &tamper_dir,
        request(&tamper_dir, &tamper_pkg, WriteDisposition::Append),
    )
    .unwrap();
    dest.store()
        .put(
            dest.execution(),
            &tamper.object_manifest.objects[0].key,
            b"not parquet anymore".to_vec(),
        )
        .unwrap();
    let verification = dest.verify_receipt(&tamper.receipt).unwrap();
    assert!(!verification.verified);
    assert!(verification.reason.unwrap().contains("sha256 mismatch"));
    let replay_error = commit_through_ingress(
        &mut dest,
        &tamper_dir,
        request(&tamper_dir, &tamper_pkg, WriteDisposition::Append),
    )
    .unwrap_err();
    assert!(
        replay_error
            .to_string()
            .contains("already exists with different bytes"),
        "{replay_error}"
    );

    let missing_dir = temp.path().join("pkg-missing");
    let missing_pkg = build_package(
        &missing_dir,
        "pkg-missing",
        vec![(
            "seg-000001",
            vec![sample_batch(vec![2], vec![Some("missing")])],
        )],
    );
    let missing = commit_through_ingress(
        &mut dest,
        &missing_dir,
        request(&missing_dir, &missing_pkg, WriteDisposition::Append),
    )
    .unwrap();
    dest.store()
        .delete(dest.execution(), &missing.object_manifest.objects[0].key)
        .unwrap();
    let verification = dest.verify_receipt(&missing.receipt).unwrap();
    assert!(!verification.verified);
    assert!(verification.reason.unwrap().contains("is missing"));
}

#[test]
fn requested_segment_validation_rejects_mismatched_segments() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-bad-segment");
    let built = build_package(
        &package_dir,
        "pkg-bad-segment",
        vec![(
            "seg-000001",
            vec![sample_batch(vec![1, 2], vec![Some("a"), Some("b")])],
        )],
    );
    let dest = test_filesystem(temp.path().join("lake")).unwrap();
    let mut bad = request(&package_dir, &built, WriteDisposition::Append);
    bad.commit.segments[0].row_count += 1;

    let error = plan_package_for_test(&dest, &package_dir, &bad).unwrap_err();
    assert!(error.to_string().contains("requested segment"));
}
