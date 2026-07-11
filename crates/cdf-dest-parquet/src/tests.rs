use super::*;

use std::io::Write;

use crate::{
    corrections::{build_correction_context, build_correction_receipt},
    manifest::{
        ParquetCorrectionSidecar, ParquetCorrectionSidecarManifest, ParquetObjectManifest,
        ReplacePointer, canonical_json_bytes, sha256_hex,
    },
    sheet::{parquet_correction_capabilities, parquet_protocol_capabilities, parquet_sheet},
    store::{ObjectKeyEncoder, package_manifest_key},
};
use ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow_array::{ArrayRef, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use cdf_conformance::destination::{
    DestinationConformanceCase, DestinationCorrectionConformanceEvidence,
    assert_destination_conformance, assert_destination_correction_conformance,
    representative_commit_request,
};
use cdf_kernel::{
    CanonicalArrowField, CorrectionStrategy, CursorPosition, CursorValue,
    DestinationCorrectionCommitRequest, DestinationCorrectionOperation, DestinationCorrectionPlan,
    DestinationCorrectionReceiptEvidence, DestinationCorrectionRequest,
    DestinationCorrectionSidecarReceiptEvidence, IdempotencyToken, PackageHash, PartitionId,
    PromotionId, ResidualCorrectionOperation, RowProvenanceAddress, ScopeKey, SegmentAck,
    SegmentId, SourcePosition,
};
use cdf_package::{PackageBuilder, PackageStatus, SegmentEntry};
use object_store::{memory::InMemory, path::Path as ObjectPath};

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
            cdf_engine::StandaloneExecutionHost::default_services(64 * 1024 * 1024)
                .unwrap()
                .1
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
    ParquetDestination::new_object_store(store, root_prefix, test_execution())
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

fn build_package(
    package_dir: &Path,
    package_id: &str,
    segments: Vec<(&str, Vec<RecordBatch>)>,
) -> BuiltPackage {
    let mut builder = PackageBuilder::create(package_dir, package_id).unwrap();
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

    for (segment_id, batches) in segments {
        builder
            .write_segment(SegmentId::new(segment_id).unwrap(), &batches)
            .unwrap();
    }

    let manifest = builder.finish().unwrap();
    BuiltPackage {
        hash: PackageHash::new(manifest.package_hash).unwrap(),
        segments: manifest.identity.segments,
    }
}

fn request(
    package_dir: &Path,
    built: &BuiltPackage,
    disposition: WriteDisposition,
) -> ParquetCommitRequest {
    ParquetCommitRequest {
        package_dir: package_dir.to_path_buf(),
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

fn commit_with_session(
    dest: &ParquetDestination,
    commit: &ParquetCommitRequest,
) -> (ParquetCommitPlan, Receipt) {
    let plan = dest.plan_package_commit(commit).unwrap();
    let mut session = DestinationProtocol::begin(dest, commit.commit.clone(), plan.kernel.clone())
        .expect("begin Parquet commit session");
    session.apply_migrations().unwrap();
    let segments = PackageReader::open(&commit.package_dir)
        .unwrap()
        .read_commit_segments(&commit.commit.segments)
        .unwrap();
    for segment in segments {
        let ack = session.write_segment(segment).unwrap();
        assert!(commit.commit.segments.iter().any(|state| {
            ack.segment_id == state.segment_id
                && ack.row_count == state.row_count
                && ack.byte_count == state.byte_count
        }));
    }
    let receipt = session.finalize().unwrap();
    (plan, receipt)
}

fn assert_same_receipt_identity(left: &Receipt, right: &Receipt) {
    assert_eq!(left.receipt_id, right.receipt_id);
    assert_eq!(left.destination, right.destination);
    assert_eq!(left.target, right.target);
    assert_eq!(left.package_hash, right.package_hash);
    assert_eq!(left.segment_acks, right.segment_acks);
    assert_eq!(left.disposition, right.disposition);
    assert_eq!(left.idempotency_token, right.idempotency_token);
    assert_eq!(left.counts, right.counts);
    assert_eq!(left.schema_hash, right.schema_hash);
    assert_eq!(left.migrations, right.migrations);
    assert_eq!(left.verify.kind, right.verify.kind);
    assert_eq!(left.verify.statement, right.verify.statement);
    assert_eq!(
        left.transaction
            .as_ref()
            .map(|transaction| transaction.system.as_str()),
        Some("object_store")
    );
    assert_eq!(
        right
            .transaction
            .as_ref()
            .map(|transaction| transaction.system.as_str()),
        Some("object_store")
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
    let dest = test_filesystem(&root).unwrap();

    let error = dest
        .commit_package(request(&package_dir, &built, WriteDisposition::Append))
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
            row_provenance_persistence: CapabilitySupport::Unsupported,
            row_provenance_targetability: CapabilitySupport::Unsupported,
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
    let dest = test_filesystem(temp.path().join("lake")).unwrap();
    let base = dest
        .commit_package(request(&package_dir, &built, WriteDisposition::Append))
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
    let dest = test_filesystem(temp.path().join("lake")).unwrap();
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
    let base = dest
        .commit_package(request(&package_dir, &built, WriteDisposition::Append))
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
    let store = Arc::new(InMemory::default());
    let dest = test_object_store(store, "").unwrap();
    let correction = correction_request(&PackageHash::new("sha256:base-package").unwrap());
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
    let dest = test_object_store(Arc::new(InMemory::default()), "").unwrap();
    let correction = correction_request(&PackageHash::new("sha256:base-package").unwrap());
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
    let dest = test_filesystem(&root).unwrap();

    let outcome = dest
        .commit_package(request(&package_dir, &built, WriteDisposition::Append))
        .unwrap();

    assert!(!outcome.duplicate);
    assert!(outcome.package_receipt_recorded);
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

    let receipts = PackageReader::open(&package_dir)
        .unwrap()
        .receipts()
        .unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].receipt_id, outcome.receipt.receipt_id);
}

#[test]
fn begin_session_flow_materializes_verifiable_manifest_receipt() {
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
    let dest = test_object_store(Arc::new(InMemory::default()), "").unwrap();
    let commit = request(&package_dir, &built, WriteDisposition::Append);

    let (plan, receipt) = commit_with_session(&dest, &commit);

    assert!(!plan.duplicate);
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
    assert_eq!(manifest.objects[0].key, plan.object_keys[0]);
    assert_eq!(manifest.objects[0].row_count, 2);
    assert_eq!(manifest.objects[0].schema_hash, "schema-v1");

    let receipts = PackageReader::open(&package_dir)
        .unwrap()
        .receipts()
        .unwrap();
    assert_eq!(receipts, vec![receipt]);
}

#[test]
fn segment_session_flow_matches_commit_package_receipt_shape() {
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
    let wrapper_dest = test_object_store(Arc::new(InMemory::default()), "").unwrap();
    let wrapper = wrapper_dest.commit_package(commit.clone()).unwrap();
    assert!(!wrapper.duplicate);

    let session_dest = test_object_store(Arc::new(InMemory::default()), "").unwrap();
    let (_session_plan, session_receipt) = commit_with_session(&session_dest, &commit);
    let session_manifest = load_manifest(&session_dest, manifest_key(&session_receipt));

    assert_same_receipt_identity(&session_receipt, &wrapper.receipt);
    assert_eq!(
        session_manifest.manifest_version,
        wrapper.object_manifest.manifest_version
    );
    assert_eq!(
        session_manifest.destination,
        wrapper.object_manifest.destination
    );
    assert_eq!(session_manifest.target, wrapper.object_manifest.target);
    assert_eq!(
        session_manifest.package_hash,
        wrapper.object_manifest.package_hash
    );
    assert_eq!(
        session_manifest.idempotency_token,
        wrapper.object_manifest.idempotency_token
    );
    assert_eq!(
        session_manifest.disposition,
        wrapper.object_manifest.disposition
    );
    assert_eq!(
        session_manifest.schema_hash,
        wrapper.object_manifest.schema_hash
    );
    assert_eq!(
        session_manifest.total_rows,
        wrapper.object_manifest.total_rows
    );
    assert_eq!(
        session_receipt.segment_acks,
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
    assert_eq!(
        session_manifest.objects.len(),
        wrapper.object_manifest.objects.len()
    );
    for (session_object, wrapper_object) in session_manifest
        .objects
        .iter()
        .zip(wrapper.object_manifest.objects.iter())
    {
        assert_eq!(session_object.segment_id, wrapper_object.segment_id);
        assert_eq!(session_object.key, wrapper_object.key);
        assert_eq!(session_object.row_count, wrapper_object.row_count);
        assert_eq!(session_object.byte_count, wrapper_object.byte_count);
        assert_eq!(
            session_object.package_byte_count,
            wrapper_object.package_byte_count
        );
        assert_eq!(
            session_object.parquet_byte_count,
            wrapper_object.parquet_byte_count
        );
        assert_eq!(session_object.sha256, wrapper_object.sha256);
        assert_eq!(session_object.schema_hash, wrapper_object.schema_hash);
        assert_ne!(session_object.byte_count, session_object.package_byte_count);
    }
    assert!(
        session_dest
            .verify_receipt(&session_receipt)
            .unwrap()
            .verified
    );
    let protocol: &dyn DestinationProtocol = &session_dest;
    assert!(protocol.verify(&session_receipt).unwrap().verified);
}

#[test]
fn session_finalize_rejects_missing_segments() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-session-missing-segments");
    let built = build_package(
        &package_dir,
        "pkg-session-missing-segments",
        vec![
            ("seg-000001", vec![sample_batch(vec![1], vec![Some("ada")])]),
            (
                "seg-000002",
                vec![sample_batch(vec![2], vec![Some("grace")])],
            ),
        ],
    );
    let dest = test_object_store(Arc::new(InMemory::default()), "").unwrap();
    let commit = request(&package_dir, &built, WriteDisposition::Append);
    let plan = dest.plan_package_commit(&commit).unwrap();
    let mut session = DestinationProtocol::begin(&dest, commit.commit.clone(), plan.kernel.clone())
        .expect("begin Parquet commit session");
    session.apply_migrations().unwrap();
    let mut segments = PackageReader::open(&commit.package_dir)
        .unwrap()
        .read_commit_segments(&commit.commit.segments)
        .unwrap();
    session.write_segment(segments.remove(0)).unwrap();

    let error = session.finalize().unwrap_err();
    assert!(error.to_string().contains("accepted 1 of 2"), "{error}");
    assert!(
        !dest
            .store()
            .exists(dest.execution(), &plan.manifest_key)
            .unwrap()
    );
}

#[test]
fn begin_session_duplicate_replay_preserves_existing_manifest() {
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
    let dest = test_object_store(Arc::new(InMemory::default()), "").unwrap();
    let commit = request(&package_dir, &built, WriteDisposition::Append);

    let first = dest.commit_package(commit.clone()).unwrap();
    let manifest_before = dest
        .store()
        .get_required(dest.execution(), &first.plan.manifest_key)
        .unwrap();
    let (duplicate_plan, duplicate_receipt) = commit_with_session(&dest, &commit);
    let manifest_after = dest
        .store()
        .get_required(dest.execution(), &first.plan.manifest_key)
        .unwrap();

    assert!(duplicate_plan.duplicate);
    assert_eq!(first.receipt.receipt_id, duplicate_receipt.receipt_id);
    assert_eq!(manifest_before, manifest_after);
    assert!(dest.verify_receipt(&duplicate_receipt).unwrap().verified);

    let receipts = PackageReader::open(&package_dir)
        .unwrap()
        .receipts()
        .unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].receipt_id, first.receipt.receipt_id);
}

#[test]
fn begin_session_abort_before_write_leaves_manifest_unwritten() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-session-abort");
    let built = build_package(
        &package_dir,
        "pkg-session-abort",
        vec![(
            "seg-000001",
            vec![sample_batch(vec![1], vec![Some("abort")])],
        )],
    );
    let dest = test_object_store(Arc::new(InMemory::default()), "").unwrap();
    let commit = request(&package_dir, &built, WriteDisposition::Append);
    let plan = dest.plan_package_commit(&commit).unwrap();

    let session = DestinationProtocol::begin(&dest, commit.commit.clone(), plan.kernel.clone())
        .expect("begin Parquet commit session");
    session.abort().unwrap();

    assert!(
        !dest
            .store()
            .exists(dest.execution(), &plan.manifest_key)
            .unwrap()
    );
    assert!(
        PackageReader::open(&package_dir)
            .unwrap()
            .receipts()
            .unwrap()
            .is_empty()
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
    let dest = test_object_store(store, "lake").unwrap();
    let commit = request(&package_dir, &built, WriteDisposition::Append);

    let first = dest.commit_package(commit.clone()).unwrap();
    assert!(first.object_manifest.committed_at_ms > 1_700_000_000_000);
    let manifest_before = dest
        .store()
        .get_required(dest.execution(), &first.plan.manifest_key)
        .unwrap();
    let duplicate_plan = dest.plan_package_commit(&commit).unwrap();
    assert!(duplicate_plan.duplicate);
    let second = dest.commit_package(commit).unwrap();
    let manifest_after = dest
        .store()
        .get_required(dest.execution(), &first.plan.manifest_key)
        .unwrap();

    assert!(!first.duplicate);
    assert!(second.duplicate);
    assert!(second.plan.duplicate);
    assert!(!second.package_receipt_recorded);
    assert_eq!(first.receipt.receipt_id, second.receipt.receipt_id);
    assert_eq!(manifest_before, manifest_after);
}

#[test]
fn replace_writes_current_pointer_to_latest_manifest() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("lake");
    let dest = test_filesystem(&root).unwrap();

    let first_dir = temp.path().join("pkg-first");
    let first = build_package(
        &first_dir,
        "pkg-first",
        vec![("seg-000001", vec![sample_batch(vec![1], vec![Some("old")])])],
    );
    let first_outcome = dest
        .commit_package(request(&first_dir, &first, WriteDisposition::Replace))
        .unwrap();

    let second_dir = temp.path().join("pkg-second");
    let second = build_package(
        &second_dir,
        "pkg-second",
        vec![(
            "seg-000001",
            vec![sample_batch(vec![9, 10], vec![Some("new"), Some("rows")])],
        )],
    );
    let second_outcome = dest
        .commit_package(request(&second_dir, &second, WriteDisposition::Replace))
        .unwrap();

    let pointer_key = second_outcome.plan.replace_pointer_key.as_ref().unwrap();
    let pointer_bytes = dest
        .store()
        .get_required(dest.execution(), pointer_key)
        .unwrap();
    let pointer: ReplacePointer = serde_json::from_slice(&pointer_bytes).unwrap();

    assert_ne!(
        first_outcome.plan.manifest_key,
        second_outcome.plan.manifest_key
    );
    assert_eq!(pointer.manifest_key, second_outcome.plan.manifest_key);
    assert!(
        dest.verify_receipt(&second_outcome.receipt)
            .unwrap()
            .verified
    );
}

#[test]
fn zero_data_append_and_replace_record_receipts_without_objects_or_pointer_mutation() {
    let temp = tempfile::tempdir().unwrap();
    let dest = test_object_store(Arc::new(InMemory::default()), "").unwrap();

    let data_dir = temp.path().join("pkg-data");
    let data = build_package(
        &data_dir,
        "pkg-data",
        vec![(
            "seg-000001",
            vec![sample_batch(vec![1, 2], vec![Some("old"), Some("rows")])],
        )],
    );
    let seeded = dest
        .commit_package(request(&data_dir, &data, WriteDisposition::Replace))
        .unwrap();
    let pointer_key = seeded.plan.replace_pointer_key.clone().unwrap();
    let pointer_before = dest
        .store()
        .get_required(dest.execution(), &pointer_key)
        .unwrap();

    for (package_id, disposition) in [
        ("pkg-empty-append", WriteDisposition::Append),
        ("pkg-empty-replace", WriteDisposition::Replace),
    ] {
        let package_dir = temp.path().join(package_id);
        let empty = build_package(&package_dir, package_id, Vec::new());
        let commit = request(&package_dir, &empty, disposition.clone());
        let plan = dest.plan_package_commit(&commit).unwrap();
        assert!(plan.object_keys.is_empty());
        assert!(plan.replace_pointer_key.is_none());

        let outcome = dest.commit_package(commit).unwrap();
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
fn dry_run_plan_reports_keys_without_writing() {
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

    let plan = dest
        .plan_package_commit(&request(&package_dir, &built, WriteDisposition::Replace))
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
    assert_eq!(
        plan.object_keys,
        vec![format!(
            "targets/orders/packages/{encoded_token}/data/seg-000001.parquet"
        )]
    );
    assert_eq!(
        plan.replace_pointer_key.as_deref(),
        Some("targets/orders/current.json")
    );
    assert_eq!(plan.object_keys.len(), 1);
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
    let dest = test_object_store(Arc::new(InMemory::default()), "").unwrap();
    let commit = request(&package_dir, &built, WriteDisposition::Append);
    let plan = dest.plan_package_commit(&commit).unwrap();

    let error = dest.commit_package(commit).unwrap_err();

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
        !dest
            .store()
            .exists(dest.execution(), &plan.object_keys[0])
            .unwrap()
    );
}

#[test]
fn canonical_json_keeps_array_separators_in_order() {
    let bytes = canonical_json_bytes(&serde_json::json!([1, 2, 3])).unwrap();
    assert_eq!(bytes, b"[1,2,3]");
}

#[test]
fn replace_duplicate_replay_requires_current_pointer_identity() {
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
    let dest = test_object_store(Arc::new(InMemory::default()), "").unwrap();
    let commit = request(&package_dir, &built, WriteDisposition::Replace);
    let first = dest.commit_package(commit.clone()).unwrap();
    let pointer_key = first.plan.replace_pointer_key.as_ref().unwrap().clone();
    let original_pointer = load_replace_pointer(&dest, &pointer_key);

    let replay = dest.commit_package(commit.clone()).unwrap();
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

        let error = dest.commit_package(commit.clone()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("replace pointer targets/orders/current.json does not point")
        );
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
    let dest = test_object_store(Arc::new(InMemory::default()), "").unwrap();
    let outcome = dest
        .commit_package(request(&package_dir, &built, WriteDisposition::Replace))
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
    let dest = test_object_store(Arc::new(InMemory::default()), "").unwrap();
    let outcome = dest
        .commit_package(request(&package_dir, &built, WriteDisposition::Append))
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
    let dest = test_object_store(store.clone(), "//lake//").unwrap();
    let outcome = dest
        .commit_package(request(&package_dir, &built, WriteDisposition::Append))
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
    let dest = test_object_store(store, "").unwrap();

    let tamper_dir = temp.path().join("pkg-tamper");
    let tamper_pkg = build_package(
        &tamper_dir,
        "pkg-tamper",
        vec![(
            "seg-000001",
            vec![sample_batch(vec![1], vec![Some("tamper")])],
        )],
    );
    let tamper = dest
        .commit_package(request(&tamper_dir, &tamper_pkg, WriteDisposition::Append))
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
    let replay_error = dest
        .commit_package(request(&tamper_dir, &tamper_pkg, WriteDisposition::Append))
        .unwrap_err();
    assert!(replay_error.to_string().contains("refusing to overwrite"));

    let missing_dir = temp.path().join("pkg-missing");
    let missing_pkg = build_package(
        &missing_dir,
        "pkg-missing",
        vec![(
            "seg-000001",
            vec![sample_batch(vec![2], vec![Some("missing")])],
        )],
    );
    let missing = dest
        .commit_package(request(
            &missing_dir,
            &missing_pkg,
            WriteDisposition::Append,
        ))
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

    let error = dest.plan_package_commit(&bad).unwrap_err();
    assert!(error.to_string().contains("requested segment"));
}
