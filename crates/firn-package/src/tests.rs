use super::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    sync::Arc,
};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, Time32SecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use duckdb::Connection;
use firn_kernel::{
    CommitCounts, DestinationId, IdempotencyToken, PackageHash, Receipt, ReceiptId, SchemaHash,
    SegmentAck, SegmentId, TargetName, VerifyClause, WriteDisposition,
};

fn sample_batch() -> RecordBatch {
    sample_batch_values(vec![1, 2, 3], vec![Some("ada"), Some("grace"), None])
}

fn sample_batch_values(ids: Vec<i64>, names: Vec<Option<&str>>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let id: ArrayRef = Arc::new(Int64Array::from(ids));
    let name: ArrayRef = Arc::new(StringArray::from(names));
    RecordBatch::try_new(schema, vec![id, name]).unwrap()
}

fn build_fixture(package_dir: &Path) -> PackageManifest {
    let mut builder = PackageBuilder::create(package_dir, "pkg-test-0001").unwrap();
    builder.update_status(PackageStatus::Extracting).unwrap();
    builder
        .write_json_artifact(
            "plan/resource_plan.json",
            &BTreeMap::from([("resource", "orders"), ("partition", "p0")]),
        )
        .unwrap();
    builder
        .write_identity_artifact(
            "plan/execution_plan.txt",
            b"PackageSinkExec: deterministic fixture\n",
        )
        .unwrap();
    builder
        .write_json_artifact(
            "plan/validation_program.json",
            &BTreeMap::from([("program", "accept-all")]),
        )
        .unwrap();
    builder
        .write_json_artifact(
            "schema/observed.arrow.json",
            &BTreeMap::from([("schema_hash", "schema-fixture")]),
        )
        .unwrap();
    builder
        .write_json_artifact(
            "schema/output.arrow.json",
            &BTreeMap::from([("schema_hash", "schema-fixture")]),
        )
        .unwrap();
    builder
        .write_json_artifact("schema/diff.json", &BTreeMap::<String, String>::new())
        .unwrap();
    builder
        .write_stats_artifact("profile.parquet", b"stats-fixture")
        .unwrap();
    builder
        .write_stats_artifact("quality.parquet", b"quality-fixture")
        .unwrap();
    builder
        .write_quarantine_artifact("part-000001.parquet", b"quarantine-fixture")
        .unwrap();
    builder
        .write_lineage_artifact("batches.parquet", b"lineage-fixture")
        .unwrap();
    builder
        .write_json_artifact(
            "state/input_checkpoint.json",
            &BTreeMap::from([("cursor", "before")]),
        )
        .unwrap();
    builder
        .write_json_artifact(
            "state/proposed_delta.json",
            &BTreeMap::from([("cursor", "after")]),
        )
        .unwrap();
    builder
        .write_json_artifact(
            "destination/commit_plan.json",
            &BTreeMap::from([("target", "orders"), ("disposition", "append")]),
        )
        .unwrap();
    builder
        .append_trace_event(&BTreeMap::from([("event", "fixture-start")]))
        .unwrap();
    builder
        .write_segment(SegmentId::new("seg-000001").unwrap(), &[sample_batch()])
        .unwrap();
    builder.finish().unwrap()
}

fn build_archive_fixture(package_dir: &Path) -> PackageManifest {
    let mut builder = PackageBuilder::create(package_dir, "pkg-archive-0001").unwrap();
    builder.update_status(PackageStatus::Extracting).unwrap();
    builder
        .write_json_artifact(
            "plan/resource_plan.json",
            &BTreeMap::from([("resource", "orders"), ("partition", "p0")]),
        )
        .unwrap();
    builder
        .write_segment(
            SegmentId::new("seg-000001").unwrap(),
            &[sample_batch_values(vec![1, 2], vec![Some("ada"), None])],
        )
        .unwrap();
    builder
        .write_segment(
            SegmentId::new("seg-000002").unwrap(),
            &[sample_batch_values(vec![3], vec![Some("grace")])],
        )
        .unwrap();
    builder.finish().unwrap()
}

fn sample_receipt(package_hash: &str) -> Receipt {
    Receipt {
        receipt_id: ReceiptId::new("receipt-1").unwrap(),
        destination: DestinationId::new("local-test").unwrap(),
        target: TargetName::new("orders").unwrap(),
        package_hash: PackageHash::new(package_hash.to_owned()).unwrap(),
        segment_acks: vec![SegmentAck {
            segment_id: SegmentId::new("seg-000001").unwrap(),
            row_count: 3,
            byte_count: 0,
        }],
        disposition: WriteDisposition::Append,
        idempotency_token: IdempotencyToken::new(package_hash.to_owned()).unwrap(),
        transaction: None,
        counts: CommitCounts {
            rows_written: 3,
            rows_inserted: Some(3),
            rows_updated: Some(0),
            rows_deleted: Some(0),
        },
        schema_hash: SchemaHash::new("schema-fixture").unwrap(),
        migrations: Vec::new(),
        committed_at_ms: 1_700_000_000_000,
        verify: VerifyClause {
            kind: "test".to_owned(),
            statement: "fixture receipt".to_owned(),
            parameters: BTreeMap::new(),
        },
    }
}

fn package_files(package_dir: &Path) -> Vec<String> {
    fn collect(base: &Path, directory: &Path, files: &mut Vec<String>) {
        let mut entries = fs::read_dir(directory)
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        entries.sort_by_key(|entry| entry.path());

        for entry in entries {
            let path = entry.path();
            let file_type = entry.file_type().unwrap();
            if file_type.is_dir() {
                collect(base, &path, files);
            } else if file_type.is_file() {
                files.push(
                    path.strip_prefix(base)
                        .unwrap()
                        .to_string_lossy()
                        .replace(std::path::MAIN_SEPARATOR, "/"),
                );
            }
        }
    }

    let mut files = Vec::new();
    collect(package_dir, package_dir, &mut files);
    files.sort();
    files
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

#[test]
fn package_layout_manifest_and_verification_cover_identity_files() {
    let temp = tempfile::tempdir().unwrap();
    let manifest = build_fixture(temp.path());

    assert_eq!(manifest.lifecycle.status, PackageStatus::Packaged);
    assert_eq!(manifest.signature.value, None);
    assert_eq!(manifest.signature.signing_input, manifest.package_hash);
    for directory in REQUIRED_DIRECTORIES {
        assert!(temp.path().join(directory).is_dir(), "{directory}");
    }
    assert!(temp.path().join(MANIFEST_FILE).is_file());
    assert!(temp.path().join(TRACE_FILE).is_file());

    let paths = manifest
        .identity
        .files
        .iter()
        .map(|entry| entry.path.as_str())
        .collect::<BTreeSet<_>>();
    assert!(paths.contains("data/seg-000001.arrow"));
    assert!(paths.contains("trace.jsonl"));
    assert!(
        manifest
            .identity
            .files
            .iter()
            .all(|entry| entry.byte_count > 0 || entry.path == TRACE_FILE)
    );
    assert!(
        manifest
            .identity
            .files
            .iter()
            .all(|entry| entry.sha256.len() == 64)
    );

    let report = verify_package(temp.path()).unwrap();
    assert_eq!(report.package_hash, manifest.package_hash);
    assert_eq!(report.checked_files.len(), manifest.identity.files.len());
}

#[test]
fn fixed_fixture_hash_is_deterministic_across_repeated_runs() {
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();

    let first_manifest = build_fixture(first.path());
    let second_manifest = build_fixture(second.path());

    assert_eq!(first_manifest.package_hash, second_manifest.package_hash);
    assert_eq!(
        first_manifest.package_hash,
        "sha256:87789e563e66acd0cec0f0edcb4b5f54052e7695440cdc66d5512b5007b24adf"
    );
}

#[test]
fn arrow_ipc_segments_round_trip_for_replay() {
    let temp = tempfile::tempdir().unwrap();
    let manifest = build_fixture(temp.path());
    let reader = PackageReader::open(temp.path()).unwrap();

    let segment_id = &manifest.identity.segments[0].segment_id;
    let batches = reader.read_segment(segment_id).unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3);

    let replay = reader.replay_view().unwrap();
    assert_eq!(replay.package_hash.as_str(), manifest.package_hash);
    assert_eq!(replay.segments.len(), 1);
}

#[test]
fn status_updates_are_atomic_and_preserve_identity_hash() {
    let temp = tempfile::tempdir().unwrap();
    let manifest = build_fixture(temp.path());
    let original_hash = manifest.package_hash.clone();

    let updated = update_package_status(temp.path(), PackageStatus::Loading).unwrap();
    assert_eq!(updated.lifecycle.status, PackageStatus::Loading);
    assert_eq!(updated.package_hash, original_hash);
    verify_package(temp.path()).unwrap();

    let updated = update_package_status(temp.path(), PackageStatus::Committed).unwrap();
    assert_eq!(updated.lifecycle.status, PackageStatus::Committed);
    assert_eq!(updated.package_hash, original_hash);
    verify_package(temp.path()).unwrap();
}

#[test]
fn verification_detects_tampered_identity_file() {
    let temp = tempfile::tempdir().unwrap();
    build_fixture(temp.path());

    let segment_path = temp.path().join("data").join("seg-000001.arrow");
    let mut file = OpenOptions::new().append(true).open(&segment_path).unwrap();
    file.write_all(b"tamper").unwrap();
    file.sync_all().unwrap();

    let error = verify_package(temp.path()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("tampered identity file data/seg-000001.arrow"),
        "{error}"
    );
}

#[test]
fn receipt_append_is_stored_outside_identity_and_exposed_to_replay() {
    let temp = tempfile::tempdir().unwrap();
    let manifest = build_fixture(temp.path());
    let reader = PackageReader::open(temp.path()).unwrap();
    let before_receipt_hash = manifest.package_hash.clone();

    let receipts = reader
        .append_receipt(sample_receipt(&manifest.package_hash))
        .unwrap();
    assert_eq!(receipts.len(), 1);
    assert!(
        temp.path()
            .join("destination")
            .join("receipts.json")
            .is_file()
    );

    let reread = PackageReader::open(temp.path()).unwrap();
    assert_eq!(reread.receipts().unwrap().len(), 1);
    assert_eq!(reread.replay_view().unwrap().receipts.len(), 1);
    assert_eq!(reread.manifest().package_hash, before_receipt_hash);
    verify_package(temp.path()).unwrap();
}

#[test]
fn tombstone_removes_identity_files_but_preserves_manifest_hashes() {
    let temp = tempfile::tempdir().unwrap();
    let manifest = build_fixture(temp.path());
    let manifest_path = temp.path().join(MANIFEST_FILE);
    let mut reader = PackageReader::open(temp.path()).unwrap();

    let report = reader.tombstone().unwrap();
    assert_eq!(report.package_hash, manifest.package_hash);
    assert!(manifest_path.is_file());
    assert!(
        report
            .removed_files
            .contains(&"data/seg-000001.arrow".to_owned())
    );

    let tombstoned_manifest = read_manifest(temp.path()).unwrap();
    assert_eq!(tombstoned_manifest.package_hash, manifest.package_hash);
    assert_eq!(
        tombstoned_manifest.lifecycle.status,
        PackageStatus::Archived
    );
    assert!(reader.replay_view().is_err());
    assert!(verify_package(temp.path()).is_err());
}

#[test]
fn archive_report_records_parquet_bytes_and_preserves_canonical_package() {
    let temp = tempfile::tempdir().unwrap();
    let manifest = build_archive_fixture(temp.path());
    let manifest_before = fs::read(temp.path().join(MANIFEST_FILE)).unwrap();
    let files_before = package_files(temp.path());

    let report = archive_package_to_parquet(temp.path()).unwrap();

    assert_eq!(report.package_hash, manifest.package_hash);
    assert!(
        report
            .fidelity_statement
            .contains("Arrow IPC remains the canonical package data")
    );
    assert!(
        report
            .fidelity_statement
            .contains("Parquet bytes are an archive/interchange projection")
    );
    assert!(
        report
            .fidelity_statement
            .contains("Arrow field metadata and other Arrow-only semantics")
    );
    assert_eq!(report.segments.len(), manifest.identity.segments.len());

    for (archived, source) in report.segments.iter().zip(&manifest.identity.segments) {
        assert_eq!(archived.segment_id, source.segment_id.as_str());
        assert_eq!(archived.source_path, source.path);
        assert_eq!(archived.source_byte_count, source.byte_count);
        assert_eq!(archived.source_sha256, source.sha256);
        assert_eq!(archived.source_row_count, source.row_count);
        assert_eq!(archived.parquet_row_count, source.row_count);
        assert_eq!(
            archived.parquet_byte_count,
            archived.parquet_bytes.len() as u64
        );
        assert_eq!(archived.parquet_sha256.len(), 64);
        assert_eq!(
            parquet_rows(&archived.parquet_bytes),
            source.row_count as usize
        );
    }

    assert_eq!(
        fs::read(temp.path().join(MANIFEST_FILE)).unwrap(),
        manifest_before
    );
    assert_eq!(package_files(temp.path()), files_before);
    assert_eq!(
        read_manifest(temp.path()).unwrap().lifecycle.status,
        PackageStatus::Packaged
    );
    verify_package(temp.path()).unwrap();
}

#[test]
fn archive_transcode_is_deterministic_for_unchanged_package() {
    let temp = tempfile::tempdir().unwrap();
    build_archive_fixture(temp.path());

    let first = archive_package_to_parquet(temp.path()).unwrap();
    let second = archive_package_to_parquet(temp.path()).unwrap();

    assert_eq!(first.package_hash, second.package_hash);
    assert_eq!(first.segments.len(), second.segments.len());
    for (first, second) in first.segments.iter().zip(second.segments.iter()) {
        assert_eq!(first.source_sha256, second.source_sha256);
        assert_eq!(first.parquet_sha256, second.parquet_sha256);
        assert_eq!(first.parquet_bytes, second.parquet_bytes);
    }
}

#[test]
fn archive_transcode_keeps_replay_and_read_segment_on_ipc() {
    let temp = tempfile::tempdir().unwrap();
    let manifest = build_archive_fixture(temp.path());

    archive_package_to_parquet(temp.path()).unwrap();

    let reader = PackageReader::open(temp.path()).unwrap();
    let segment_id = &manifest.identity.segments[0].segment_id;
    let batches = reader.read_segment(segment_id).unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 2);

    let replay = reader.replay_view().unwrap();
    assert_eq!(replay.package_hash.as_str(), manifest.package_hash);
    assert_eq!(replay.segments[0].path, "data/seg-000001.arrow");
}

#[test]
fn archive_transcode_refuses_tampered_package_before_report() {
    let temp = tempfile::tempdir().unwrap();
    build_archive_fixture(temp.path());

    let segment_path = temp.path().join("data").join("seg-000001.arrow");
    let mut file = OpenOptions::new().append(true).open(&segment_path).unwrap();
    file.write_all(b"tamper").unwrap();
    file.sync_all().unwrap();
    let files_before = package_files(temp.path());
    let manifest_before = fs::read(temp.path().join(MANIFEST_FILE)).unwrap();

    let error = archive_package_to_parquet(temp.path()).unwrap_err();

    assert!(error.to_string().contains("package verification failed"));
    assert!(error.to_string().contains("tampered identity file"));
    assert_eq!(package_files(temp.path()), files_before);
    assert_eq!(
        fs::read(temp.path().join(MANIFEST_FILE)).unwrap(),
        manifest_before
    );
}

#[test]
fn archive_transcode_reports_unsupported_arrow_types() {
    let temp = tempfile::tempdir().unwrap();
    let mut builder = PackageBuilder::create(temp.path(), "pkg-archive-unsupported").unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "unsupported_time",
        DataType::Time32(TimeUnit::Second),
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Time32SecondArray::from(vec![1]))]).unwrap();
    builder
        .write_segment(SegmentId::new("seg-000001").unwrap(), &[batch])
        .unwrap();
    builder.finish().unwrap();

    let error = archive_package_to_parquet(temp.path()).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("does not support Arrow type Time32")
    );
}

#[test]
fn archive_transcode_rejects_duplicate_column_names_before_duckdb_ddl() {
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

    let error = transcode_record_batches_to_parquet_bytes(&[batch]).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("duplicate Parquet column name duplicate"),
        "{error}"
    );
}
