use super::*;

use std::io::Write;

use crate::manifest::{ParquetObjectManifest, ReplacePointer, canonical_json_bytes, sha256_hex};
use arrow_array::{ArrayRef, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use duckdb::Connection;
use firn_conformance::destination::{
    DestinationConformanceCase, assert_destination_conformance, representative_commit_request,
};
use firn_kernel::{
    CursorPosition, CursorValue, IdempotencyToken, PackageHash, PartitionId, ScopeKey, SegmentId,
    SourcePosition,
};
use firn_package::{PackageBuilder, PackageStatus, SegmentEntry};
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

fn sample_batch(ids: Vec<i64>, names: Vec<Option<&str>>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let id: ArrayRef = Arc::new(Int64Array::from(ids));
    let name: ArrayRef = Arc::new(StringArray::from(names));
    RecordBatch::try_new(schema, vec![id, name]).unwrap()
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
    let path = temp.path().to_str().unwrap();
    let conn = Connection::open_in_memory().unwrap();
    conn.query_row(
        &format!("SELECT count(*) FROM read_parquet({})", sql_string(path)),
        [],
        |row| row.get::<_, usize>(0),
    )
    .unwrap()
}

fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
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
    let bytes = dest.store().get_required(dest.runtime(), key).unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn load_replace_pointer(dest: &ParquetDestination, key: &str) -> ReplacePointer {
    let bytes = dest.store().get_required(dest.runtime(), key).unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn store_manifest(
    dest: &ParquetDestination,
    key: &str,
    manifest: &ParquetObjectManifest,
) -> StoredJson {
    let bytes = canonical_json_bytes(manifest).unwrap();
    let sha256 = sha256_hex(&bytes);
    let put = dest.store().put(dest.runtime(), key, bytes).unwrap();
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
    let put = dest.store().put(dest.runtime(), key, bytes).unwrap();
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
    let dest = ParquetDestination::new_filesystem(&root).unwrap();

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
    let dest = ParquetDestination::new_filesystem(temp.path()).unwrap();
    let sheet = dest.sheet();

    assert_eq!(sheet.destination.as_str(), "parquet_object_store");
    assert_eq!(sheet.transactions, TransactionSupport::AtomicTarget);
    assert_eq!(sheet.idempotency, IdempotencySupport::PackageToken);
    assert_eq!(sheet.migration_support, CapabilitySupport::Unsupported);
    assert_eq!(sheet.quarantine_tables, CapabilitySupport::Unsupported);
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
    let dest = ParquetDestination::new_filesystem(temp.path()).unwrap();

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
    let dest = ParquetDestination::new_filesystem(&root).unwrap();

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
        .get_required(dest.runtime(), &outcome.object_manifest.objects[0].key)
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
    let dest = ParquetDestination::new_object_store(store, "lake").unwrap();
    let commit = request(&package_dir, &built, WriteDisposition::Append);

    let first = dest.commit_package(commit.clone()).unwrap();
    assert!(first.object_manifest.committed_at_ms > 1_700_000_000_000);
    let manifest_before = dest
        .store()
        .get_required(dest.runtime(), &first.plan.manifest_key)
        .unwrap();
    let duplicate_plan = dest.plan_package_commit(&commit).unwrap();
    assert!(duplicate_plan.duplicate);
    let second = dest.commit_package(commit).unwrap();
    let manifest_after = dest
        .store()
        .get_required(dest.runtime(), &first.plan.manifest_key)
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
    let dest = ParquetDestination::new_filesystem(&root).unwrap();

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
        .get_required(dest.runtime(), pointer_key)
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
    let dest = ParquetDestination::new_filesystem(&root).unwrap();

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
            .exists(dest.runtime(), &plan.manifest_key)
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
    let dest = ParquetDestination::new_object_store(Arc::new(InMemory::default()), "").unwrap();
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
            .exists(dest.runtime(), &plan.manifest_key)
            .unwrap()
    );
    assert!(
        !dest
            .store()
            .exists(dest.runtime(), &plan.object_keys[0])
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
    let dest = ParquetDestination::new_object_store(Arc::new(InMemory::default()), "").unwrap();
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
    let dest = ParquetDestination::new_object_store(Arc::new(InMemory::default()), "").unwrap();
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
    let dest = ParquetDestination::new_object_store(Arc::new(InMemory::default()), "").unwrap();
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
    assert!(
        ParquetDestination::new_object_store(Arc::new(InMemory::default()), "lake/../bad").is_err()
    );

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
    let dest = ParquetDestination::new_object_store(store.clone(), "//lake//").unwrap();
    let outcome = dest
        .commit_package(request(&package_dir, &built, WriteDisposition::Append))
        .unwrap();
    let object_key = &outcome.object_manifest.objects[0].key;

    assert!(
        dest.runtime()
            .block_on(store.head(&ObjectPath::from(format!("lake/{object_key}"))))
            .is_ok()
    );
    assert!(
        dest.runtime()
            .block_on(store.head(&ObjectPath::from(object_key.as_str())))
            .is_err()
    );
}

#[test]
fn verification_fails_for_tampered_and_missing_objects() {
    let temp = tempfile::tempdir().unwrap();
    let store = Arc::new(InMemory::default());
    let dest = ParquetDestination::new_object_store(store, "").unwrap();

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
            dest.runtime(),
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
        .delete(dest.runtime(), &missing.object_manifest.objects[0].key)
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
    let dest = ParquetDestination::new_filesystem(temp.path().join("lake")).unwrap();
    let mut bad = request(&package_dir, &built, WriteDisposition::Append);
    bad.commit.segments[0].row_count += 1;

    let error = dest.plan_package_commit(&bad).unwrap_err();
    assert!(error.to_string().contains("requested segment"));
}
