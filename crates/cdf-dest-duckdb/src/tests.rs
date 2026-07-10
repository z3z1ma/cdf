use super::*;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use cdf_conformance::destination::{
    DestinationConformanceCase, DestinationCorrectionConformanceEvidence,
    assert_destination_conformance, assert_destination_correction_conformance,
    representative_commit_request,
};
use cdf_kernel::{
    CursorPosition, CursorValue, IdempotencyToken, PackageHash, PartitionId, ScopeKey, SegmentAck,
    SegmentId, SourcePosition,
};
use cdf_package::{PackageBuilder, PackageStatus};

fn sample_batch(ids: Vec<i64>, names: Vec<Option<&str>>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let id: ArrayRef = Arc::new(Int64Array::from(ids));
    let name: ArrayRef = Arc::new(StringArray::from(names));
    RecordBatch::try_new(schema, vec![id, name]).unwrap()
}

fn build_package(package_dir: &Path, package_id: &str, batches: &[RecordBatch]) -> PackageHash {
    build_package_segments(
        package_dir,
        package_id,
        &[(SegmentId::new("seg-000001").unwrap(), batches.to_vec())],
    )
}

fn build_package_segments(
    package_dir: &Path,
    package_id: &str,
    segments: &[(SegmentId, Vec<RecordBatch>)],
) -> PackageHash {
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
        builder.write_segment(segment_id.clone(), batches).unwrap();
    }
    let manifest = builder.finish().unwrap();
    PackageHash::new(manifest.package_hash).unwrap()
}

fn state_segment(rows: u64) -> StateSegment {
    state_segment_for("seg-000001", rows, 3)
}

fn state_segment_for(segment_id: &str, rows: u64, cursor: i64) -> StateSegment {
    StateSegment {
        segment_id: SegmentId::new(segment_id).unwrap(),
        scope: ScopeKey::Partition {
            partition_id: PartitionId::new("p0").unwrap(),
        },
        output_position: SourcePosition::Cursor(CursorPosition {
            version: 1,
            field: "id".to_owned(),
            value: CursorValue::I64(cursor),
        }),
        row_count: rows,
        byte_count: rows * 16,
    }
}

fn request(
    package_dir: &Path,
    package_hash: PackageHash,
    disposition: WriteDisposition,
    merge_keys: Vec<String>,
    rows: u64,
) -> DuckDbCommitRequest {
    request_with_segments(
        package_dir,
        package_hash,
        disposition,
        merge_keys,
        vec![state_segment(rows)],
    )
}

fn request_with_segments(
    package_dir: &Path,
    package_hash: PackageHash,
    disposition: WriteDisposition,
    merge_keys: Vec<String>,
    segments: Vec<StateSegment>,
) -> DuckDbCommitRequest {
    DuckDbCommitRequest {
        package_dir: package_dir.to_path_buf(),
        commit: DestinationCommitRequest {
            package_hash: package_hash.clone(),
            target: TargetName::new("orders").unwrap(),
            disposition,
            segments,
            idempotency_token: IdempotencyToken::new(package_hash.as_str()).unwrap(),
        },
        schema_hash: SchemaHash::new("schema-v1").unwrap(),
        merge_keys,
    }
}

fn destination(path: &Path) -> DuckDbDestination {
    DuckDbDestination::new(path).unwrap()
}

fn finalize_session(dest: &DuckDbDestination, request: &DuckDbCommitRequest) -> Receipt {
    let plan = dest.plan_package_commit(request).unwrap();
    let mut session = dest
        .begin(request.commit.clone(), plan.kernel.clone())
        .unwrap();
    session.apply_migrations().unwrap();
    let segments = PackageReader::open(&request.package_dir)
        .unwrap()
        .read_commit_segments(&request.commit.segments)
        .unwrap();
    for segment in segments {
        let ack = session.write_segment(segment).unwrap();
        assert!(request.commit.segments.iter().any(|state| {
            ack.segment_id == state.segment_id
                && ack.row_count == state.row_count
                && ack.byte_count == state.byte_count
        }));
    }
    session.finalize().unwrap()
}

fn finalize_empty_session(dest: &DuckDbDestination, request: &DuckDbCommitRequest) -> Receipt {
    let plan = dest.plan_empty_package_commit(request).unwrap();
    assert_eq!(plan.effect, DuckDbCommitEffect::NoData);
    let mut session = dest
        .begin(request.commit.clone(), plan.kernel.clone())
        .unwrap();
    session.apply_migrations().unwrap();
    session.finalize().unwrap()
}

fn assert_same_receipt_shape(left: &Receipt, right: &Receipt) {
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
    assert_eq!(left.verify, right.verify);
    assert_eq!(
        left.transaction.as_ref().map(|tx| tx.system.as_str()),
        Some("duckdb")
    );
    assert_eq!(
        right.transaction.as_ref().map(|tx| tx.system.as_str()),
        Some("duckdb")
    );
}

#[test]
fn sheet_declares_duckdb_destination_contract() {
    let temp = tempfile::tempdir().unwrap();
    let dest = destination(&temp.path().join("local.duckdb"));
    let sheet = dest.sheet();
    assert_eq!(sheet.destination.as_str(), "duckdb");
    assert_eq!(sheet.transactions, TransactionSupport::AtomicPackage);
    assert_eq!(sheet.idempotency, IdempotencySupport::PackageToken);
    assert_eq!(sheet.concurrency.max_writers, Some(1));
    assert!(
        sheet
            .supported_dispositions
            .contains(&WriteDisposition::Append)
    );
    assert!(
        dest.capabilities()
            .bulk_paths
            .contains(&BulkPath::ArrowIpcPackageRows)
    );
}

#[test]
fn reusable_destination_conformance_suite_accepts_duckdb_sheet_and_plans() {
    let temp = tempfile::tempdir().unwrap();
    let dest = destination(&temp.path().join("local.duckdb"));

    assert_destination_conformance(
        &dest,
        [
            DestinationConformanceCase::new(representative_commit_request(
                WriteDisposition::Append,
            )),
            DestinationConformanceCase::new(representative_commit_request(
                WriteDisposition::Replace,
            )),
            DestinationConformanceCase::new(representative_commit_request(WriteDisposition::Merge)),
        ],
    );
    assert_destination_correction_conformance(
        &dest,
        &DestinationCorrectionConformanceEvidence::unsupported(),
    );
}

#[test]
fn segment_session_flow_returns_wrapper_receipt_shape_and_verifies() {
    let temp = tempfile::tempdir().unwrap();
    let package = temp.path().join("pkg-session");
    let package_hash = build_package_segments(
        &package,
        "pkg-session",
        &[
            (
                SegmentId::new("seg-000001").unwrap(),
                vec![sample_batch(vec![1, 2], vec![Some("ada"), Some("grace")])],
            ),
            (
                SegmentId::new("seg-000002").unwrap(),
                vec![sample_batch(vec![3], vec![None])],
            ),
        ],
    );
    let request = request_with_segments(
        &package,
        package_hash,
        WriteDisposition::Append,
        Vec::new(),
        vec![
            state_segment_for("seg-000001", 2, 2),
            state_segment_for("seg-000002", 1, 3),
        ],
    );
    let wrapper_dest = destination(&temp.path().join("wrapper.duckdb"));
    let wrapper = wrapper_dest.commit_package(request.clone()).unwrap();
    assert!(!wrapper.duplicate);

    let session_dest = destination(&temp.path().join("session.duckdb"));
    let receipt = finalize_session(&session_dest, &request);

    assert_same_receipt_shape(&receipt, &wrapper.receipt);
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
    assert!(session_dest.verify_receipt(&receipt).unwrap().verified);
    let protocol: &dyn DestinationProtocol = &session_dest;
    assert!(protocol.verify(&receipt).unwrap().verified);
}

#[test]
fn session_finalize_rejects_missing_segments() {
    let temp = tempfile::tempdir().unwrap();
    let package = temp.path().join("pkg-session-missing-segments");
    let package_hash = build_package_segments(
        &package,
        "pkg-session-missing-segments",
        &[
            (
                SegmentId::new("seg-000001").unwrap(),
                vec![sample_batch(vec![1], vec![Some("ada")])],
            ),
            (
                SegmentId::new("seg-000002").unwrap(),
                vec![sample_batch(vec![2], vec![Some("grace")])],
            ),
        ],
    );
    let request = request_with_segments(
        &package,
        package_hash,
        WriteDisposition::Append,
        Vec::new(),
        vec![
            state_segment_for("seg-000001", 1, 1),
            state_segment_for("seg-000002", 1, 2),
        ],
    );
    let dest = destination(&temp.path().join("local.duckdb"));
    let plan = dest.plan_package_commit(&request).unwrap();
    let mut session = dest
        .begin(request.commit.clone(), plan.kernel.clone())
        .unwrap();
    session.apply_migrations().unwrap();
    let mut segments = PackageReader::open(&request.package_dir)
        .unwrap()
        .read_commit_segments(&request.commit.segments)
        .unwrap();
    session.write_segment(segments.remove(0)).unwrap();

    let error = session.finalize().unwrap_err();
    assert!(error.to_string().contains("accepted 1 of 2"), "{error}");
}

#[test]
fn begin_session_duplicate_returns_existing_receipt_without_extra_rows() {
    let temp = tempfile::tempdir().unwrap();
    let package = temp.path().join("pkg-session-duplicate");
    let package_hash = build_package(
        &package,
        "pkg-session-duplicate",
        &[sample_batch(
            vec![1, 2, 3],
            vec![Some("ada"), Some("grace"), None],
        )],
    );
    let db_path = temp.path().join("local.duckdb");
    let dest = destination(&db_path);
    let request = request(
        &package,
        package_hash,
        WriteDisposition::Append,
        Vec::new(),
        3,
    );

    let first = finalize_session(&dest, &request);
    let duplicate = finalize_session(&dest, &request);

    assert_same_receipt_shape(&duplicate, &first);
    assert!(dest.verify_receipt(&duplicate).unwrap().verified);

    let conn = Connection::open(db_path).unwrap();
    let target_rows: u64 = conn
        .query_row("SELECT count(*) FROM orders", [], |row| row.get(0))
        .unwrap();
    assert_eq!(target_rows, 3);

    let mirror = dest.read_mirror_snapshot_read_only().unwrap();
    assert_eq!(mirror.loads.len(), 1);
    assert_eq!(mirror.state.len(), 1);
    assert_eq!(
        PackageReader::open(&package)
            .unwrap()
            .receipts()
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn append_commit_is_idempotent_and_verifiable_after_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let package = temp.path().join("pkg");
    let package_hash = build_package(
        &package,
        "pkg-append",
        &[sample_batch(
            vec![1, 2, 3],
            vec![Some("ada"), Some("grace"), None],
        )],
    );
    let db_path = temp.path().join("local.duckdb");
    let dest = destination(&db_path);
    let request = request(
        &package,
        package_hash.clone(),
        WriteDisposition::Append,
        Vec::new(),
        3,
    );

    let outcome = dest.commit_package(request.clone()).unwrap();
    assert!(!outcome.duplicate);
    assert_eq!(outcome.receipt.counts.rows_written, 3);
    assert!(dest.verify_receipt(&outcome.receipt).unwrap().verified);

    let reopened = destination(&db_path);
    assert!(reopened.verify_receipt(&outcome.receipt).unwrap().verified);
    let duplicate = reopened.commit_package(request).unwrap();
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.receipt.receipt_id, outcome.receipt.receipt_id);

    let conn = Connection::open(db_path).unwrap();
    let target_rows: u64 = conn
        .query_row("SELECT count(*) FROM orders", [], |row| row.get(0))
        .unwrap();
    let load_rows: u64 = conn
        .query_row("SELECT count(*) FROM _cdf_loads", [], |row| row.get(0))
        .unwrap();
    let state_rows: u64 = conn
        .query_row("SELECT count(*) FROM _cdf_state", [], |row| row.get(0))
        .unwrap();
    assert_eq!(target_rows, 3);
    assert_eq!(load_rows, 1);
    assert_eq!(state_rows, 1);
}

#[test]
fn zero_data_append_and_replace_record_receipts_without_mutating_target_data() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("local.duckdb");
    let dest = destination(&db_path);

    let empty_append_dir = temp.path().join("pkg-empty-append");
    let empty_append_hash = build_package_segments(&empty_append_dir, "pkg-empty-append", &[]);
    let empty_append = request_with_segments(
        &empty_append_dir,
        empty_append_hash,
        WriteDisposition::Append,
        Vec::new(),
        Vec::new(),
    );
    let append_receipt = finalize_empty_session(&dest, &empty_append);
    assert!(append_receipt.segment_acks.is_empty());
    assert_eq!(append_receipt.counts, CommitCounts::default());
    assert!(dest.verify_receipt(&append_receipt).unwrap().verified);

    let conn = Connection::open(&db_path).unwrap();
    let target_tables: u64 = conn
        .query_row(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'orders'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(target_tables, 0);
    drop(conn);

    let data_dir = temp.path().join("pkg-data");
    let data_hash = build_package(
        &data_dir,
        "pkg-data",
        &[sample_batch(vec![1, 2], vec![Some("old"), Some("rows")])],
    );
    dest.commit_package(request(
        &data_dir,
        data_hash,
        WriteDisposition::Append,
        Vec::new(),
        2,
    ))
    .unwrap();

    let empty_replace_dir = temp.path().join("pkg-empty-replace");
    let empty_replace_hash = build_package_segments(&empty_replace_dir, "pkg-empty-replace", &[]);
    let empty_replace = request_with_segments(
        &empty_replace_dir,
        empty_replace_hash,
        WriteDisposition::Replace,
        Vec::new(),
        Vec::new(),
    );
    let replace_receipt = finalize_empty_session(&dest, &empty_replace);
    assert!(replace_receipt.segment_acks.is_empty());
    assert_eq!(replace_receipt.counts, CommitCounts::default());
    assert!(dest.verify_receipt(&replace_receipt).unwrap().verified);

    let conn = Connection::open(&db_path).unwrap();
    let target_rows: u64 = conn
        .query_row("SELECT count(*) FROM orders", [], |row| row.get(0))
        .unwrap();
    assert_eq!(target_rows, 2);
}

#[test]
fn replace_rebuilds_target_atomically() {
    let temp = tempfile::tempdir().unwrap();
    let first_package = temp.path().join("pkg-first");
    let first_hash = build_package(
        &first_package,
        "pkg-first",
        &[sample_batch(vec![1, 2], vec![Some("old"), Some("rows")])],
    );
    let db_path = temp.path().join("local.duckdb");
    let dest = destination(&db_path);
    dest.commit_package(request(
        &first_package,
        first_hash,
        WriteDisposition::Append,
        Vec::new(),
        2,
    ))
    .unwrap();

    let second_package = temp.path().join("pkg-second");
    let second_hash = build_package(
        &second_package,
        "pkg-second",
        &[sample_batch(vec![9], vec![Some("new")])],
    );
    let outcome = dest
        .commit_package(request(
            &second_package,
            second_hash,
            WriteDisposition::Replace,
            Vec::new(),
            1,
        ))
        .unwrap();
    assert_eq!(outcome.receipt.counts.rows_written, 1);

    let conn = Connection::open(db_path).unwrap();
    let rows: Vec<(i64, String)> = conn
        .prepare("SELECT id, name FROM orders ORDER BY id")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    assert_eq!(rows, vec![(9, "new".to_owned())]);
}

#[test]
fn merge_deduplicates_exact_replayed_rows_and_updates_keys() {
    let temp = tempfile::tempdir().unwrap();
    let initial_package = temp.path().join("pkg-initial");
    let initial_hash = build_package(
        &initial_package,
        "pkg-initial",
        &[sample_batch(vec![1, 2], vec![Some("old"), Some("keep")])],
    );
    let db_path = temp.path().join("local.duckdb");
    let dest = destination(&db_path);
    dest.commit_package(request(
        &initial_package,
        initial_hash,
        WriteDisposition::Append,
        Vec::new(),
        2,
    ))
    .unwrap();

    let merge_package = temp.path().join("pkg-merge");
    let merge_hash = build_package(
        &merge_package,
        "pkg-merge",
        &[sample_batch(
            vec![1, 1, 3],
            vec![Some("new"), Some("new"), Some("insert")],
        )],
    );
    let outcome = dest
        .commit_package(request(
            &merge_package,
            merge_hash,
            WriteDisposition::Merge,
            vec!["id".to_owned()],
            3,
        ))
        .unwrap();
    assert_eq!(outcome.receipt.counts.rows_written, 2);
    assert_eq!(outcome.receipt.counts.rows_updated, Some(1));
    assert_eq!(outcome.receipt.counts.rows_inserted, Some(1));

    let conn = Connection::open(db_path).unwrap();
    let rows: Vec<(i64, String)> = conn
        .prepare("SELECT id, name FROM orders ORDER BY id")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    assert_eq!(
        rows,
        vec![
            (1, "new".to_owned()),
            (2, "keep".to_owned()),
            (3, "insert".to_owned())
        ]
    );
}

#[test]
fn merge_rejects_conflicting_duplicate_keys() {
    let temp = tempfile::tempdir().unwrap();
    let package = temp.path().join("pkg-conflict");
    let package_hash = build_package(
        &package,
        "pkg-conflict",
        &[sample_batch(vec![1, 1], vec![Some("left"), Some("right")])],
    );
    let dest = destination(&temp.path().join("local.duckdb"));
    let error = dest
        .commit_package(request(
            &package,
            package_hash,
            WriteDisposition::Merge,
            vec!["id".to_owned()],
            2,
        ))
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("conflicting duplicate merge keys"),
        "{error}"
    );
}

#[test]
fn dry_run_plan_reports_create_table_ddl_without_writing() {
    let temp = tempfile::tempdir().unwrap();
    let package = temp.path().join("pkg-plan");
    let package_hash = build_package(
        &package,
        "pkg-plan",
        &[sample_batch(vec![1], vec![Some("planned")])],
    );
    let db_path = temp.path().join("local.duckdb");
    let dest = destination(&db_path);
    let plan = dest
        .plan_package_commit(&request(
            &package,
            package_hash,
            WriteDisposition::Append,
            Vec::new(),
            1,
        ))
        .unwrap();
    assert!(plan.ddl.iter().any(|ddl| ddl.contains("CREATE TABLE")));
    assert!(
        !db_path.exists() || {
            let conn = Connection::open(&db_path).unwrap();
            conn.query_row::<u64, _, _>(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = 'orders'",
                [],
                |row| row.get(0),
            )
            .unwrap()
                == 0
        }
    );
}

#[test]
fn single_writer_lock_blocks_second_writer() {
    let temp = tempfile::tempdir().unwrap();
    let dest = destination(&temp.path().join("local.duckdb"));
    let _held = dest.acquire_writer_lock().unwrap();
    let error = dest.acquire_writer_lock().unwrap_err();
    assert!(error.to_string().contains("writer lock is already held"));
}

#[test]
fn icu_probe_reports_availability_or_runtime_error() {
    let temp = tempfile::tempdir().unwrap();
    let dest = destination(&temp.path().join("local.duckdb"));
    let probe = dest.probe_icu().unwrap();
    assert!(probe.statement.contains("icu_sort_key"));
    if !probe.available {
        assert!(probe.error.is_some());
    }
}
