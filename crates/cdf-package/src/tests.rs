use super::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

use ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, Time32SecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use cdf_kernel::{
    CHECKPOINT_STATE_VERSION, CdfError, Checkpoint, CheckpointId, CheckpointStatus, CommitCounts,
    CommitSegment, CursorPosition, CursorValue, DestinationId, FileManifest, FilePosition,
    IdempotencyToken, PackageHash, PartitionId, PipelineId, ProcessedObservationOutcome,
    ProcessedObservationPosition, Receipt, ReceiptId, ResourceId, Result, SchemaHash, ScopeKey,
    SegmentAck, SegmentId, SourcePosition, StateDelta, StateSegment, TableSnapshotPosition,
    TableSnapshotSelector, TargetName, VerifyClause, WriteDisposition,
    aggregate_processed_observation_positions,
};
use cdf_memory::{
    ConsumerKey, DeterministicMemoryCoordinator, MemoryClass, MemoryCoordinator,
    record_batch_retained_bytes,
};
use cdf_package_contract::*;
use sha2::Digest;

fn collect_quarantine_records(reader: &PackageReader) -> Vec<QuarantineRecord> {
    let mut records = Vec::new();
    reader
        .for_each_quarantine_record(&mut |record| {
            records.push(record);
            Ok(())
        })
        .unwrap();
    records
}

fn collect_dedup_dropped_provenance(reader: &PackageReader) -> Vec<(u64, u64)> {
    let mut rows = Vec::new();
    reader
        .for_each_dedup_dropped_provenance(&mut |dropped, kept| {
            rows.push((dropped, kept));
            Ok(())
        })
        .unwrap();
    rows
}

fn table_snapshot_position() -> SourcePosition {
    SourcePosition::TableSnapshot(Box::new(TableSnapshotPosition {
        version: CHECKPOINT_STATE_VERSION,
        protocol: "iceberg".to_owned(),
        catalog: "glue:us-east-1:123456789012".to_owned(),
        namespace: vec!["analytics".to_owned()],
        table: "orders".to_owned(),
        selector: TableSnapshotSelector::Branch {
            name: "main".to_owned(),
        },
        snapshot_id: 42,
        sequence_number: 7,
        parent_snapshot_id: Some(41),
        metadata_location: "s3://warehouse/analytics/orders/metadata/v42.json".to_owned(),
        metadata_generation: "version-id:v42".to_owned(),
    }))
}

#[test]
fn table_snapshot_position_has_stable_canonical_json_and_hash() {
    let bytes = canonical_json_bytes(&table_snapshot_position()).unwrap();
    assert_eq!(
        String::from_utf8(bytes.clone()).unwrap(),
        concat!(
            "{\"catalog\":\"glue:us-east-1:123456789012\",",
            "\"kind\":\"table_snapshot\",",
            "\"metadata_generation\":\"version-id:v42\",",
            "\"metadata_location\":\"s3://warehouse/analytics/orders/metadata/v42.json\",",
            "\"namespace\":[\"analytics\"],",
            "\"parent_snapshot_id\":41,",
            "\"protocol\":\"iceberg\",",
            "\"selector\":{\"kind\":\"branch\",\"name\":\"main\"},",
            "\"sequence_number\":7,",
            "\"snapshot_id\":42,",
            "\"table\":\"orders\",",
            "\"version\":1}"
        )
    );
    assert_eq!(
        hex::encode(sha2::Sha256::digest(&bytes)),
        "0e6d4a51d3cb81ce0ba7ba73b4684c9ac501886fe720ee1ee29087f19175e623"
    );
}

#[test]
fn package_local_authority_is_absolute_for_relative_roots() {
    let relative = PathBuf::from(format!(
        "target/cdf-package-relative-authority-{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&relative);
    let builder = PackageBuilder::create(&relative, "pkg-relative-authority").unwrap();
    assert!(builder.package_dir().is_absolute());
    builder.finish().unwrap();
    PackageReader::open(&relative).unwrap();
    fs::remove_dir_all(&relative).unwrap();
}

#[cfg(unix)]
#[test]
fn finalization_does_not_reopen_registered_artifact_content() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().unwrap();
    let package_dir = directory.path().join("package");
    let builder = PackageBuilder::create(&package_dir, "pkg-no-reread").unwrap();
    builder
        .write_identity_artifact("stats/receipt-backed.bin", b"registered")
        .unwrap();
    let artifact = package_dir.join("stats/receipt-backed.bin");
    fs::set_permissions(&artifact, fs::Permissions::from_mode(0o000)).unwrap();

    let manifest = builder.finish().unwrap();

    fs::set_permissions(&artifact, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(
        manifest
            .identity
            .files
            .iter()
            .any(|entry| entry.path == "stats/receipt-backed.bin")
    );
}

#[cfg(unix)]
#[test]
fn finalization_authority_opens_for_consumption_without_rereading_identity_files() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().unwrap();
    let package_dir = directory.path().join("package");
    let builder = PackageBuilder::create(&package_dir, "pkg-finalization-proof").unwrap();
    builder
        .write_identity_artifact("stats/receipt-backed.bin", b"registered")
        .unwrap();
    let (manifest, verified) = builder.finish_verified().unwrap();
    let artifact = package_dir.join("stats/receipt-backed.bin");
    fs::set_permissions(&artifact, fs::Permissions::from_mode(0o000)).unwrap();

    let package = PackageReader::open(&package_dir)
        .unwrap()
        .with_verification(verified)
        .unwrap();
    assert_eq!(package.verification().package_hash(), manifest.package_hash);

    fs::set_permissions(&artifact, fs::Permissions::from_mode(0o600)).unwrap();
}

#[test]
fn segment_encoding_completion_cannot_override_canonical_registration_order() {
    let directory = tempfile::tempdir().unwrap();
    let builder = PackageBuilder::create(directory.path(), "pkg-encode-frontier").unwrap();
    let encoder = builder.segment_encoder();
    let second = encoder
        .encode(
            SegmentId::new("seg-000002").unwrap(),
            3,
            &[canonical_batch(sample_batch(), 3)],
            true,
        )
        .unwrap();
    let first = encoder
        .encode(
            SegmentId::new("seg-000001").unwrap(),
            0,
            &[canonical_batch(sample_batch(), 0)],
            true,
        )
        .unwrap();

    let first = builder.register_encoded_segment(first).unwrap().segment;
    let second = builder.register_encoded_segment(second).unwrap().segment;
    let mut journal = Vec::new();
    builder
        .visit_segment_entries(&mut |entry| {
            journal.push(entry);
            Ok(())
        })
        .unwrap();
    assert_eq!(journal, [first.clone(), second.clone()]);
    let error = builder
        .visit_segment_entries(&mut |_| Err(CdfError::data("stop visiting")))
        .unwrap_err();
    assert!(error.message.contains("stop visiting"));
    let third = encoder
        .encode(
            SegmentId::new("seg-000003").unwrap(),
            6,
            &[canonical_batch(sample_batch(), 6)],
            true,
        )
        .unwrap();
    let third = builder.register_encoded_segment(third).unwrap().segment;
    journal.clear();
    builder
        .visit_segment_entries(&mut |entry| {
            journal.push(entry);
            Ok(())
        })
        .unwrap();
    assert_eq!(journal, [first, second, third]);
    let manifest = builder.finish().unwrap();
    assert_eq!(
        manifest
            .identity
            .segments
            .iter()
            .map(|segment| segment.segment_id.as_str())
            .collect::<Vec<_>>(),
        ["seg-000001", "seg-000002", "seg-000003"]
    );
}

#[test]
fn imported_canonical_segment_preserves_native_package_identity() {
    let directory = tempfile::tempdir().unwrap();
    let direct_root = directory.path().join("direct");
    let imported_root = directory.path().join("imported");
    let segment_id = SegmentId::new("seg-000001").unwrap();
    let batch = canonical_batch(sample_batch(), 0);

    let direct = PackageBuilder::create(&direct_root, "pkg-import-equivalence").unwrap();
    let direct_segment = direct
        .write_segment(segment_id.clone(), 0, std::slice::from_ref(&batch))
        .unwrap();
    let direct_manifest = direct.finish().unwrap();

    let mut encoded = Vec::new();
    encode_canonical_segment_ipc(
        &mut encoded,
        batch.schema().as_ref(),
        std::slice::from_ref(&batch),
    )
    .unwrap();
    let imported = PackageBuilder::create(&imported_root, "pkg-import-equivalence").unwrap();
    let imported_segment = imported
        .import_canonical_segment(segment_id, 0, 3, &encoded)
        .unwrap()
        .segment;
    let imported_manifest = imported.finish().unwrap();

    assert_eq!(imported_segment, direct_segment);
    assert_eq!(imported_manifest, direct_manifest);

    let invalid = PackageBuilder::create(
        directory.path().join("invalid"),
        "pkg-import-invalid-ordinal",
    )
    .unwrap();
    assert!(
        invalid
            .import_canonical_segment(SegmentId::new("seg-invalid").unwrap(), 1, 3, &encoded)
            .unwrap_err()
            .message
            .contains("ordinal")
    );
    assert!(
        !invalid
            .package_dir()
            .join("data/seg-invalid.arrow")
            .exists()
    );
}

#[test]
fn unpublished_segment_encoding_is_rolled_back_on_drop() {
    let directory = tempfile::tempdir().unwrap();
    let builder = PackageBuilder::create(directory.path(), "pkg-encode-rollback").unwrap();
    let encoded = builder
        .segment_encoder()
        .encode(
            SegmentId::new("seg-unregistered").unwrap(),
            0,
            &[canonical_batch(sample_batch(), 0)],
            false,
        )
        .unwrap();
    let path = directory.path().join("data/seg-unregistered.arrow");
    assert!(path.exists());
    drop(encoded);
    assert!(!path.exists());
}

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

fn canonical_batch(batch: RecordBatch, start: u64) -> RecordBatch {
    append_package_row_ord(vec![batch], start)
        .unwrap()
        .pop()
        .unwrap()
}

#[test]
fn verified_statistics_profile_is_manifest_bound_typed_parquet() {
    let temp = tempfile::tempdir().unwrap();
    let builder = PackageBuilder::create(temp.path(), "pkg-stats-profile").unwrap();
    let segment_id = SegmentId::new("seg-000001").unwrap();
    let batch = sample_batch();
    let stats = cdf_kernel::BatchStats::compute(&batch).unwrap();
    let mut profile = builder.begin_statistics_profile().unwrap();
    profile
        .write_stats(
            StatisticsProfileGrain::Segment,
            0,
            segment_id.as_str(),
            "sha256:schema",
            &stats,
        )
        .unwrap();
    profile
        .write_stats(
            StatisticsProfileGrain::Package,
            0,
            "pkg-stats-profile",
            "sha256:schema",
            &stats,
        )
        .unwrap();
    profile.finish().unwrap();
    builder
        .write_segment(segment_id, 0, &[canonical_batch(batch, 0)])
        .unwrap();
    let (_, verified) = builder.finish_verified().unwrap();
    let reader = PackageReader::open(temp.path()).unwrap();

    let rows = reader.verified_statistics_profile(&verified).unwrap();

    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0].grain, StatisticsProfileGrain::Segment);
    assert_eq!(rows[0].container_id, "seg-000001");
    assert_eq!(rows[0].field_path[0].as_ref(), "id");
    assert_eq!(rows[0].minimum, Some(cdf_kernel::TypedScalar::Signed(1)));
    assert_eq!(rows[1].field_path[0].as_ref(), "name");
    assert_eq!(
        rows[1].maximum,
        Some(cdf_kernel::TypedScalar::Utf8("grace".into()))
    );
    assert_eq!(rows[2].grain, StatisticsProfileGrain::Package);

    fs::write(temp.path().join(STATISTICS_PROFILE_FILE), b"tampered").unwrap();
    let error = reader.verified_statistics_profile(&verified).unwrap_err();
    assert!(
        error
            .message
            .contains("identity artifact stats/profile.parquet changed after package verification"),
        "{error}"
    );
}

#[test]
fn verification_rejects_unknown_contract_evolution_versions() {
    for (name, artifact) in [
        (
            "top-level",
            serde_json::json!({
                "version": 2,
                "residual_capture": null,
                "residual_decisions": []
            }),
        ),
        (
            "capture",
            serde_json::json!({
                "version": 1,
                "residual_capture": {"version": 2},
                "residual_decisions": []
            }),
        ),
        (
            "decision",
            serde_json::json!({
                "version": 1,
                "residual_capture": null,
                "residual_decisions": [{"version": 2}]
            }),
        ),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let builder = PackageBuilder::create(temp.path(), format!("pkg-{name}")).unwrap();
        builder.update_status(PackageStatus::Extracting).unwrap();
        builder
            .write_json_artifact("schema/contract-evolution.json", &artifact)
            .unwrap();
        builder.update_status(PackageStatus::Validated).unwrap();
        builder.finish().unwrap();
        let error = verify_package(temp.path()).unwrap_err();
        assert!(error.to_string().contains("unsupported"));
    }
}

#[test]
fn incomplete_construction_can_be_discarded_only_under_its_exact_identity() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("incomplete");
    let builder = PackageBuilder::create(&package_dir, "pkg-incomplete").unwrap();
    builder.update_status(PackageStatus::Extracting).unwrap();
    builder
        .write_identity_artifact("plan/partial.json", b"{}")
        .unwrap();
    drop(builder);

    let mismatch = PackageReader::open(&package_dir)
        .unwrap()
        .discard_incomplete_construction("pkg-other")
        .unwrap_err();
    assert!(mismatch.to_string().contains("expected"));
    assert!(package_dir.exists());

    PackageReader::open(&package_dir)
        .unwrap()
        .discard_incomplete_construction("pkg-incomplete")
        .unwrap();
    assert!(!package_dir.exists());
}

#[test]
fn replayable_package_cannot_cross_incomplete_construction_deletion_boundary() {
    let temp = tempfile::tempdir().unwrap();
    build_fixture(temp.path());

    let error = PackageReader::open(temp.path())
        .unwrap()
        .discard_incomplete_construction("pkg-test-0001")
        .unwrap_err();

    assert!(error.to_string().contains("verified replay"));
    assert!(temp.path().join(MANIFEST_FILE).exists());
}

fn build_fixture(package_dir: &Path) -> PackageManifest {
    let builder = PackageBuilder::create(package_dir, "pkg-test-0001").unwrap();
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
        .append_trace_event(&BTreeMap::from([("event", "fixture-start")]))
        .unwrap();
    let segment = builder
        .write_segment(
            SegmentId::new("seg-000001").unwrap(),
            0,
            &[canonical_batch(sample_batch(), 0)],
        )
        .unwrap();
    write_state_commit_artifacts(&builder, segment);
    builder.finish().unwrap()
}

fn write_state_commit_artifacts(builder: &PackageBuilder, segment: SegmentEntry) {
    let scope = ScopeKey::Partition {
        partition_id: PartitionId::new("p0").unwrap(),
    };
    let output_position = SourcePosition::Cursor(CursorPosition {
        version: CHECKPOINT_STATE_VERSION,
        field: "id".to_owned(),
        value: CursorValue::I64(3),
    });
    let segments = vec![StateSegment {
        segment_id: segment.segment_id,
        scope: scope.clone(),
        output_position: output_position.clone(),
        row_count: segment.row_count,
        byte_count: segment.byte_count,
    }];
    let state_delta = StateDeltaPreimage {
        checkpoint_id: CheckpointId::new("checkpoint-fixture").unwrap(),
        pipeline_id: PipelineId::new("pipeline-fixture").unwrap(),
        resource_id: ResourceId::new("orders").unwrap(),
        scope,
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: None,
        input_position: None,
        output_position,
        output_watermark: None,
        partition_watermarks: Vec::new(),
        late_data_carryover: Vec::new(),
        source_continuation: None,
        schema_hash: SchemaHash::new("schema-fixture").unwrap(),
        segments: segments.clone(),
    };
    let commit_plan = DestinationCommitPlanPreimage::package_hash_token(
        TargetName::new("orders").unwrap(),
        WriteDisposition::Append,
        Vec::new(),
        SchemaHash::new("schema-fixture").unwrap(),
        segments,
    );
    builder.write_input_checkpoint_artifact(&None).unwrap();
    builder
        .write_state_delta_preimage_artifact(&state_delta)
        .unwrap();
    builder
        .write_commit_plan_preimage_artifact(&commit_plan)
        .unwrap();
}

fn state_segment_for_entry(segment: &SegmentEntry, byte_count: u64) -> StateSegment {
    StateSegment {
        segment_id: segment.segment_id.clone(),
        scope: ScopeKey::Partition {
            partition_id: PartitionId::new("p0").unwrap(),
        },
        output_position: SourcePosition::Cursor(CursorPosition {
            version: CHECKPOINT_STATE_VERSION,
            field: "id".to_owned(),
            value: CursorValue::I64(segment.row_count as i64),
        }),
        row_count: segment.row_count,
        byte_count,
    }
}

fn state_segments_for_manifest(manifest: &PackageManifest) -> Vec<StateSegment> {
    manifest
        .identity
        .segments
        .iter()
        .map(|segment| state_segment_for_entry(segment, segment.byte_count))
        .collect()
}

fn collect_commit_segments_for_test(
    reader: &PackageReader,
    state_segments: &[StateSegment],
) -> Result<Vec<CommitSegment>> {
    let memory: Arc<dyn MemoryCoordinator> =
        Arc::new(DeterministicMemoryCoordinator::new(128 * 1024 * 1024, BTreeMap::new()).unwrap());
    reader
        .verified_commit_segment_stream(state_segments, memory, 64 * 1024 * 1024)?
        .map(|segment| segment.and_then(|segment| segment.into_commit_segment()))
        .collect()
}

fn build_archive_fixture(package_dir: &Path) -> PackageManifest {
    let builder = PackageBuilder::create(package_dir, "pkg-archive-0001").unwrap();
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
            0,
            &[canonical_batch(
                sample_batch_values(vec![1, 2], vec![Some("ada"), None]),
                0,
            )],
        )
        .unwrap();
    builder
        .write_segment(
            SegmentId::new("seg-000002").unwrap(),
            2,
            &[canonical_batch(
                sample_batch_values(vec![3], vec![Some("grace")]),
                2,
            )],
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
    assert_eq!(report.checked_file_count, manifest.identity.files.len());
}

#[test]
fn quarantine_records_round_trip_as_parquet_identity_evidence() {
    let records = vec![
        QuarantineRecord {
            source_row_ordinal: 7,
            rule_id: "row-rule-0000-regex".to_owned(),
            error_code: "regex_violation".to_owned(),
            source_position: Some(SourcePosition::Cursor(CursorPosition {
                version: CHECKPOINT_STATE_VERSION,
                field: "updated_at".to_owned(),
                value: CursorValue::I64(42),
            })),
            observed_value_redacted: QuarantineObservedValue::Hashed {
                algorithm: "sha256".to_owned(),
                value: "sha256:abc123".to_owned(),
            },
        },
        QuarantineRecord {
            source_row_ordinal: 8,
            rule_id: "row-rule-0001-domain".to_owned(),
            error_code: "domain_violation".to_owned(),
            source_position: None,
            observed_value_redacted: QuarantineObservedValue::Preserved {
                value: "inactive".to_owned(),
            },
        },
    ];

    let temp = tempfile::tempdir().unwrap();
    let builder = PackageBuilder::create(temp.path(), "pkg-quarantine-0001").unwrap();
    builder
        .write_quarantine_records("part-000001.parquet", &records)
        .unwrap();
    let manifest = builder.finish().unwrap();
    let reader = PackageReader::open(temp.path()).unwrap();
    assert_eq!(collect_quarantine_records(&reader), records);
    assert_eq!(reader.quarantine_record_count().unwrap(), 2);

    let report = verify_package(temp.path()).unwrap();
    assert_eq!(report.checked_file_count, manifest.identity.files.len());
    assert!(
        manifest
            .identity
            .files
            .iter()
            .any(|file| file.path == "quarantine/part-000001.parquet")
    );

    let mut file = OpenOptions::new()
        .append(true)
        .open(temp.path().join("quarantine/part-000001.parquet"))
        .unwrap();
    file.write_all(b"tamper").unwrap();
    file.sync_all().unwrap();
    let error = verify_package(temp.path()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("tampered identity file quarantine/part-000001.parquet"),
        "{error}"
    );

    let traversal = tempfile::tempdir().unwrap();
    let builder = PackageBuilder::create(traversal.path(), "pkg-quarantine-traversal").unwrap();
    builder
        .write_quarantine_records("part-000001.parquet", &records)
        .unwrap();
    let mut manifest = builder.finish().unwrap();
    manifest.identity.files.push(FileEntry {
        path: "quarantine/../escape.parquet".to_owned(),
        byte_count: 0,
        sha256: String::new(),
    });
    fs::write(
        traversal.path().join(MANIFEST_FILE),
        canonical_json_bytes(&manifest).unwrap(),
    )
    .unwrap();
    let error = PackageReader::open(traversal.path())
        .unwrap()
        .for_each_quarantine_record(&mut |_| Ok(()))
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("package artifact path must be relative and stay inside the package"),
        "{error}"
    );
}

#[test]
fn quarantine_writer_streams_multiple_bounded_record_chunks() {
    let temp = tempfile::tempdir().unwrap();
    let builder = PackageBuilder::create(temp.path(), "pkg-quarantine-stream").unwrap();
    let mut writer = builder
        .begin_quarantine_records("part-000001.parquet")
        .unwrap();
    let expected = (0..20_000_u64)
        .map(|row| QuarantineRecord {
            source_row_ordinal: row,
            rule_id: "range-rule".to_owned(),
            error_code: "range_violation".to_owned(),
            source_position: None,
            observed_value_redacted: QuarantineObservedValue::Omitted,
        })
        .collect::<Vec<_>>();
    for chunk in expected.chunks(4_096) {
        writer.write_records(chunk).unwrap();
    }
    writer.finish().unwrap();
    builder.finish().unwrap();

    let reader = PackageReader::open(temp.path()).unwrap();
    let actual = collect_quarantine_records(&reader);
    assert_eq!(actual, expected);
    assert_eq!(reader.quarantine_record_count().unwrap(), 20_000);

    let mut visited = 0_u64;
    let error = reader
        .for_each_quarantine_record(&mut |_| {
            visited += 1;
            if visited == 10 {
                return Err(CdfError::data("stop quarantine visitor"));
            }
            Ok(())
        })
        .unwrap_err();
    assert_eq!(visited, 10);
    assert_eq!(error.to_string(), "Data: stop quarantine visitor");
}

#[test]
fn dedup_summary_round_trips_as_json_identity_evidence() {
    let temp = tempfile::tempdir().unwrap();
    let builder = PackageBuilder::create(temp.path(), "pkg-dedup-summary-0001").unwrap();
    let summary = serde_json::json!({
        "version": 3,
        "rule_id": "row-rule-0000-dedup",
        "keys": ["id"],
        "keep": "last",
        "input_rows": 6,
        "output_rows": 3,
        "duplicate_key_count": 3,
        "dropped_row_count": 3,
        "provenance_format": "parquet",
        "provenance_version": 1,
        "provenance_path": "stats/dedup-dropped/",
        "provenance_shard_row_target": 65536,
        "shard_count": 2
    });

    builder
        .write_dedup_provenance_shard(1, &[(0, 2), (4, 5)])
        .unwrap();
    builder.write_dedup_provenance_shard(2, &[(7, 9)]).unwrap();
    builder.write_dedup_summary(&summary).unwrap();
    let manifest = builder.finish().unwrap();
    let reader = PackageReader::open(temp.path()).unwrap();

    assert_eq!(reader.read_dedup_summary_json().unwrap(), Some(summary));
    assert_eq!(
        collect_dedup_dropped_provenance(&reader),
        vec![(0, 2), (4, 5), (7, 9)]
    );
    assert!(
        manifest
            .identity
            .files
            .iter()
            .any(|file| file.path == DEDUP_SUMMARY_FILE)
    );
    assert_eq!(
        verify_package(temp.path()).unwrap().checked_file_count,
        manifest.identity.files.len()
    );

    let mut file = OpenOptions::new()
        .append(true)
        .open(temp.path().join(DEDUP_SUMMARY_FILE))
        .unwrap();
    file.write_all(b"tamper").unwrap();
    file.sync_all().unwrap();
    let error = verify_package(temp.path()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("tampered identity file stats/dedup-summary.json"),
        "{error}"
    );
}

#[test]
fn streaming_identity_artifact_is_atomic_hashed_and_manifest_owned() {
    let temp = tempfile::tempdir().unwrap();
    let builder = PackageBuilder::create(temp.path(), "streaming-artifact").unwrap();
    let mut artifact = builder
        .begin_streaming_identity_artifact("stats/large-array.json")
        .unwrap();
    artifact.write_all(b"[").unwrap();
    for (index, value) in ["alpha", "beta", "gamma"].iter().enumerate() {
        if index > 0 {
            artifact.write_all(b",").unwrap();
        }
        artifact.write_json(value).unwrap();
    }
    artifact.write_all(b"]").unwrap();
    let receipt = artifact.finish().unwrap();
    assert_eq!(receipt.path, "stats/large-array.json");

    let manifest = builder.finish().unwrap();
    let bytes = fs::read(temp.path().join(&receipt.path)).unwrap();
    assert_eq!(bytes, br#"["alpha","beta","gamma"]"#);
    assert_eq!(receipt.byte_count, bytes.len() as u64);
    assert_eq!(
        manifest
            .identity
            .files
            .iter()
            .find(|entry| entry.path == receipt.path),
        Some(&receipt)
    );
}

#[test]
fn dropped_streaming_identity_artifact_publishes_nothing() {
    let temp = tempfile::tempdir().unwrap();
    let builder = PackageBuilder::create(temp.path(), "dropped-streaming-artifact").unwrap();
    {
        let mut artifact = builder
            .begin_streaming_identity_artifact("stats/incomplete.json")
            .unwrap();
        artifact.write_all(b"[").unwrap();
    }
    assert!(!temp.path().join("stats/incomplete.json").exists());
}

#[test]
fn finalization_rejects_unregistered_identity_writers() {
    let temp = tempfile::tempdir().unwrap();
    let builder = PackageBuilder::create(temp.path(), "unregistered-writer").unwrap();
    fs::write(temp.path().join("stats/bypass.json"), b"{}").unwrap();

    let error = builder.finish().unwrap_err();
    assert!(error.message.contains("no hash-while-write receipt"));
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
        "sha256:7c4c100198dfcb530f14715902116732a9cbf54e57387a650b291a6c24bf236c"
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
fn replay_inputs_reconstruct_state_delta_and_commit_request_from_verified_preimages() {
    let temp = tempfile::tempdir().unwrap();
    let manifest = build_fixture(temp.path());
    let reader = PackageReader::open(temp.path()).unwrap();

    let inputs = reader.replay_inputs().unwrap();

    assert_eq!(inputs.input_checkpoint, None);
    assert_eq!(
        inputs.state_delta.package_hash.as_str(),
        manifest.package_hash
    );
    assert_eq!(
        inputs.destination_commit.package_hash.as_str(),
        manifest.package_hash
    );
    assert_eq!(
        inputs.destination_commit.idempotency_token.as_str(),
        manifest.package_hash
    );
    assert_eq!(inputs.destination_commit.target.as_str(), "orders");
    assert_eq!(inputs.schema_hash.as_str(), "schema-fixture");
    assert_eq!(
        inputs.state_delta.segments,
        inputs.destination_commit.segments
    );

    let state_json: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(temp.path().join(STATE_PROPOSED_DELTA_FILE)).unwrap(),
    )
    .unwrap();
    assert!(state_json.get("package_hash").is_none());

    let commit_json: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(temp.path().join(DESTINATION_COMMIT_PLAN_FILE)).unwrap(),
    )
    .unwrap();
    assert_eq!(commit_json["idempotency_token_source"], "package_hash");
    assert!(commit_json.get("idempotency_token").is_none());
    assert!(commit_json.get("package_hash").is_none());
}

#[test]
fn verified_commit_stream_preserves_requested_and_package_byte_counts() {
    let temp = tempfile::tempdir().unwrap();
    let manifest = build_archive_fixture(temp.path());
    let reader = PackageReader::open(temp.path()).unwrap();
    let mut state_segments = state_segments_for_manifest(&manifest);
    state_segments[0].byte_count = manifest.identity.segments[0].byte_count + 100;

    let commit_segments = collect_commit_segments_for_test(&reader, &state_segments).unwrap();

    assert_eq!(commit_segments.len(), state_segments.len());
    assert_eq!(commit_segments[0].state, state_segments[0]);
    assert_eq!(
        commit_segments[0].state.byte_count,
        manifest.identity.segments[0].byte_count + 100
    );
    assert_eq!(
        commit_segments[0].package_byte_count,
        manifest.identity.segments[0].byte_count
    );
    assert_eq!(commit_segments[0].batches[0].num_rows(), 2);
    assert_eq!(commit_segments[1].batches[0].num_rows(), 1);
}

#[test]
fn consumption_verification_authority_is_bound_to_one_package_directory() {
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    build_fixture(first.path());
    build_fixture(second.path());
    let first_reader = PackageReader::open(first.path()).unwrap();
    let second_reader = PackageReader::open(second.path()).unwrap();
    let verified = first_reader.verify_for_consumption().unwrap();

    first_reader.replay_inputs_verified(&verified).unwrap();
    let error = second_reader.replay_inputs_verified(&verified).unwrap_err();
    assert!(
        error
            .message
            .contains("does not bind this package identity")
    );
}

#[test]
fn verified_identity_artifact_read_rejects_post_verification_change() {
    let temp = tempfile::tempdir().unwrap();
    let builder = PackageBuilder::create(temp.path(), "pkg-plan-consumption-binding").unwrap();
    builder
        .write_json_artifact(
            "plan/scan.json",
            &BTreeMap::from([("plan_id", "recorded-plan")]),
        )
        .unwrap();
    let (_, verified) = builder.finish_verified().unwrap();
    let reader = PackageReader::open(temp.path()).unwrap();

    fs::write(
        temp.path().join("plan/scan.json"),
        br#"{"plan_id":"changed"}"#,
    )
    .unwrap();

    let error = reader
        .verified_json_artifact::<BTreeMap<String, String>>(&verified, "plan/scan.json")
        .unwrap_err();
    assert!(
        error
            .message
            .contains("identity artifact plan/scan.json changed after package verification"),
        "{error}"
    );
}

#[test]
fn verified_commit_stream_holds_one_accounted_segment_window() {
    let temp = tempfile::tempdir().unwrap();
    let manifest = build_archive_fixture(temp.path());
    let reader = PackageReader::open(temp.path()).unwrap();
    let state_segments = state_segments_for_manifest(&manifest);
    let memory: Arc<dyn MemoryCoordinator> =
        Arc::new(DeterministicMemoryCoordinator::new(64 * 1024, BTreeMap::new()).unwrap());
    let mut stream = reader
        .verified_commit_segment_stream(&state_segments, Arc::clone(&memory), 64 * 1024)
        .unwrap();

    let first = stream.next().unwrap().unwrap();
    assert_eq!(first.entry.segment_id, state_segments[0].segment_id);
    assert_eq!(first.authority, state_segments[0]);
    assert_eq!(first.batches[0].num_rows(), 2);
    assert!(first.accounted_bytes() > 0);
    assert_eq!(memory.snapshot().current_bytes, first.accounted_bytes());
    drop(first);
    assert_eq!(memory.snapshot().current_bytes, 0);

    let second = stream.next().unwrap().unwrap();
    assert_eq!(second.entry.segment_id, state_segments[1].segment_id);
    assert_eq!(second.authority, state_segments[1]);
    assert_eq!(second.batches[0].num_rows(), 1);
    drop(second);
    assert!(stream.next().is_none());
    let snapshot = memory.snapshot();
    assert_eq!(snapshot.current_bytes, 0);
    assert!(snapshot.peak_bytes <= 64 * 1024);
}

#[test]
fn package_reader_clones_share_manifest_cardinality_authority() {
    let temp = tempfile::tempdir().unwrap();
    build_archive_fixture(temp.path());
    let reader = PackageReader::open(temp.path()).unwrap();
    let cloned = reader.clone();

    assert!(std::ptr::eq(reader.manifest(), cloned.manifest()));

    let memory: Arc<dyn MemoryCoordinator> =
        Arc::new(DeterministicMemoryCoordinator::new(64 * 1024, BTreeMap::new()).unwrap());
    let mut stream = cloned.verified_segment_stream(memory, 64 * 1024).unwrap();
    assert_eq!(
        stream.next().unwrap().unwrap().entry.segment_id,
        reader.manifest().identity.segments[0].segment_id
    );
}

#[test]
fn concurrent_commit_segments_retain_independent_accounted_windows() {
    let temp = tempfile::tempdir().unwrap();
    let manifest = build_archive_fixture(temp.path());
    let reader = PackageReader::open(temp.path()).unwrap();
    let state_segments = state_segments_for_manifest(&manifest);
    let memory: Arc<dyn MemoryCoordinator> =
        Arc::new(DeterministicMemoryCoordinator::new(128 * 1024, BTreeMap::new()).unwrap());
    let mut stream = reader
        .verified_commit_segment_stream(&state_segments, Arc::clone(&memory), 64 * 1024)
        .unwrap();

    let first = stream
        .next()
        .unwrap()
        .unwrap()
        .into_commit_segment()
        .unwrap();
    let second = stream
        .next()
        .unwrap()
        .unwrap()
        .into_commit_segment()
        .unwrap();
    assert!(first.retained_bytes() > 0);
    assert!(second.retained_bytes() > 0);
    assert_eq!(
        memory.snapshot().current_bytes,
        first.retained_bytes() + second.retained_bytes()
    );
    assert!(stream.next().is_none());
    drop(first);
    assert_eq!(memory.snapshot().current_bytes, second.retained_bytes());
    drop(second);
    assert_eq!(memory.snapshot().current_bytes, 0);
}

#[test]
fn verified_segment_stream_rejects_tamper_and_undersized_windows() {
    let tampered = tempfile::tempdir().unwrap();
    let manifest = build_fixture(tampered.path());
    let path = tampered.path().join(&manifest.identity.segments[0].path);
    fs::write(&path, b"tampered").unwrap();
    let reader = PackageReader::open(tampered.path()).unwrap();
    let memory: Arc<dyn MemoryCoordinator> =
        Arc::new(DeterministicMemoryCoordinator::new(64 * 1024, BTreeMap::new()).unwrap());
    let error = reader
        .verified_segment_stream(Arc::clone(&memory), 64 * 1024)
        .err()
        .unwrap();
    assert!(error.message.contains("tampered identity file"));
    assert_eq!(memory.snapshot().current_bytes, 0);

    let small = tempfile::tempdir().unwrap();
    build_fixture(small.path());
    let reader = PackageReader::open(small.path()).unwrap();
    let tiny: Arc<dyn MemoryCoordinator> =
        Arc::new(DeterministicMemoryCoordinator::new(64 * 1024, BTreeMap::new()).unwrap());
    let mut stream = reader
        .verified_segment_stream(Arc::clone(&tiny), 1)
        .unwrap();
    let error = stream.next().unwrap().unwrap_err();
    assert!(
        error
            .message
            .contains("above its 1-byte verified stream window")
    );
    assert_eq!(tiny.snapshot().current_bytes, 0);
    assert!(stream.next().is_none());
}

#[test]
fn verified_segment_stream_allows_multiple_windows_within_memory_budget() {
    let temp = tempfile::tempdir().unwrap();
    let manifest = build_archive_fixture(temp.path());
    let reader = PackageReader::open(temp.path()).unwrap();
    let memory: Arc<dyn MemoryCoordinator> =
        Arc::new(DeterministicMemoryCoordinator::new(128 * 1024, BTreeMap::new()).unwrap());
    let mut stream = reader
        .verified_segment_stream(Arc::clone(&memory), 64 * 1024)
        .unwrap();
    let first = stream.next().unwrap().unwrap();
    let second = stream.next().unwrap().unwrap();
    assert_eq!(
        memory.snapshot().current_bytes,
        first.accounted_bytes() + second.accounted_bytes()
    );
    assert!(stream.next().is_none());
    drop(first);
    assert_eq!(memory.snapshot().current_bytes, second.accounted_bytes());
    drop(second);
    assert_eq!(memory.snapshot().current_bytes, 0);
    assert_eq!(manifest.identity.segments.len(), 2);
}

#[test]
fn verified_commit_stream_rejects_bad_segment_requests_and_row_counts() {
    let duplicate = tempfile::tempdir().unwrap();
    let duplicate_manifest = build_archive_fixture(duplicate.path());
    let duplicate_reader = PackageReader::open(duplicate.path()).unwrap();
    let mut duplicate_segments = state_segments_for_manifest(&duplicate_manifest);
    duplicate_segments.push(duplicate_segments[0].clone());
    let error =
        collect_commit_segments_for_test(&duplicate_reader, &duplicate_segments).unwrap_err();
    assert!(
        error.to_string().contains("contains duplicate segment"),
        "{error}"
    );

    let unknown = tempfile::tempdir().unwrap();
    let unknown_manifest = build_archive_fixture(unknown.path());
    let unknown_reader = PackageReader::open(unknown.path()).unwrap();
    let mut unknown_segments = state_segments_for_manifest(&unknown_manifest);
    unknown_segments[0].segment_id = SegmentId::new("seg-unknown").unwrap();
    let error = collect_commit_segments_for_test(&unknown_reader, &unknown_segments).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("is not present in the package manifest"),
        "{error}"
    );

    let missing = tempfile::tempdir().unwrap();
    let missing_manifest = build_archive_fixture(missing.path());
    let missing_reader = PackageReader::open(missing.path()).unwrap();
    let mut missing_segments = state_segments_for_manifest(&missing_manifest);
    missing_segments.pop();
    let error = collect_commit_segments_for_test(&missing_reader, &missing_segments).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("is missing from destination commit request"),
        "{error}"
    );

    let reordered = tempfile::tempdir().unwrap();
    let reordered_manifest = build_archive_fixture(reordered.path());
    let reordered_reader = PackageReader::open(reordered.path()).unwrap();
    let mut reordered_segments = state_segments_for_manifest(&reordered_manifest);
    reordered_segments.swap(0, 1);
    let error =
        collect_commit_segments_for_test(&reordered_reader, &reordered_segments).unwrap_err();
    assert!(
        error.to_string().contains("canonical package order"),
        "{error}"
    );

    let requested_row_mismatch = tempfile::tempdir().unwrap();
    let requested_row_mismatch_manifest = build_archive_fixture(requested_row_mismatch.path());
    let requested_row_mismatch_reader = PackageReader::open(requested_row_mismatch.path()).unwrap();
    let mut requested_row_mismatch_segments =
        state_segments_for_manifest(&requested_row_mismatch_manifest);
    requested_row_mismatch_segments[0].row_count += 1;
    let error = collect_commit_segments_for_test(
        &requested_row_mismatch_reader,
        &requested_row_mismatch_segments,
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("but package manifest has"),
        "{error}"
    );

    let package_row_mismatch = tempfile::tempdir().unwrap();
    let mut package_row_mismatch_manifest = build_archive_fixture(package_row_mismatch.path());
    package_row_mismatch_manifest.identity.segments[1].row_count += 1;
    package_row_mismatch_manifest.package_hash =
        manifest_identity_hash(&package_row_mismatch_manifest.identity).unwrap();
    package_row_mismatch_manifest.signature.signing_input =
        package_row_mismatch_manifest.package_hash.clone();
    fs::write(
        package_row_mismatch.path().join(MANIFEST_FILE),
        canonical_json_bytes(&package_row_mismatch_manifest).unwrap(),
    )
    .unwrap();
    let package_row_mismatch_reader = PackageReader::open(package_row_mismatch.path()).unwrap();
    let package_row_mismatch_segments = state_segments_for_manifest(&package_row_mismatch_manifest);
    let error = collect_commit_segments_for_test(
        &package_row_mismatch_reader,
        &package_row_mismatch_segments,
    )
    .unwrap_err();
    assert!(error.to_string().contains("manifest row count"), "{error}");
}

#[test]
fn replay_inputs_rejects_invalid_state_preimage_semantics() {
    let package_hash =
        PackageHash::new("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .unwrap();
    let segment = SegmentEntry {
        segment_id: SegmentId::new("seg-000001").unwrap(),
        path: "data/seg-000001.arrow".to_owned(),
        package_row_ord_start: 0,
        row_count: 3,
        byte_count: 99,
        sha256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
    };
    let state_delta = StateDeltaPreimage {
        checkpoint_id: CheckpointId::new("checkpoint-next").unwrap(),
        pipeline_id: PipelineId::new("pipeline-1").unwrap(),
        resource_id: ResourceId::new("orders").unwrap(),
        scope: ScopeKey::Partition {
            partition_id: PartitionId::new("p0").unwrap(),
        },
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: Some(CheckpointId::new("checkpoint-prev").unwrap()),
        input_position: Some(SourcePosition::Cursor(CursorPosition {
            version: CHECKPOINT_STATE_VERSION,
            field: "id".to_owned(),
            value: CursorValue::I64(2),
        })),
        output_position: SourcePosition::Cursor(CursorPosition {
            version: CHECKPOINT_STATE_VERSION,
            field: "id".to_owned(),
            value: CursorValue::I64(3),
        }),
        output_watermark: None,
        partition_watermarks: Vec::new(),
        late_data_carryover: Vec::new(),
        source_continuation: None,
        schema_hash: SchemaHash::new("schema-fixture").unwrap(),
        segments: vec![StateSegment {
            segment_id: segment.segment_id.clone(),
            scope: ScopeKey::Partition {
                partition_id: PartitionId::new("p0").unwrap(),
            },
            output_position: SourcePosition::Cursor(CursorPosition {
                version: CHECKPOINT_STATE_VERSION,
                field: "id".to_owned(),
                value: CursorValue::I64(3),
            }),
            row_count: segment.row_count,
            byte_count: segment.byte_count,
        }],
    };
    let commit_plan = DestinationCommitPlanPreimage::package_hash_token(
        TargetName::new("orders").unwrap(),
        WriteDisposition::Append,
        Vec::new(),
        SchemaHash::new("schema-fixture").unwrap(),
        state_delta.segments.clone(),
    );
    let valid_input_checkpoint = Checkpoint {
        delta: StateDelta {
            checkpoint_id: CheckpointId::new("checkpoint-prev").unwrap(),
            pipeline_id: state_delta.pipeline_id.clone(),
            resource_id: ResourceId::new("orders").unwrap(),
            scope: ScopeKey::Partition {
                partition_id: PartitionId::new("p0").unwrap(),
            },
            state_version: CHECKPOINT_STATE_VERSION,
            parent_checkpoint_id: None,
            input_position: None,
            output_position: state_delta.input_position.clone().unwrap(),
            output_watermark: None,
            partition_watermarks: Vec::new(),
            late_data_carryover: Vec::new(),
            source_continuation: None,
            package_hash: package_hash.clone(),
            schema_hash: SchemaHash::new("schema-fixture").unwrap(),
            segments: state_delta.segments.clone(),
        },
        receipt: None,
        status: CheckpointStatus::Committed,
        is_head: true,
        created_at_ms: 1,
        committed_at_ms: Some(2),
        rewind_target_checkpoint_id: None,
    };

    let mut non_committed_checkpoint = valid_input_checkpoint.clone();
    non_committed_checkpoint.status = CheckpointStatus::Proposed;
    let error = PackageReplayInputs::from_preimages(
        package_hash.clone(),
        Some(non_committed_checkpoint),
        state_delta.clone(),
        commit_plan.clone(),
        std::slice::from_ref(&segment),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("state input checkpoint must be the committed head"),
        "{error}"
    );

    let mut non_head_checkpoint = valid_input_checkpoint.clone();
    non_head_checkpoint.is_head = false;
    let error = PackageReplayInputs::from_preimages(
        package_hash.clone(),
        Some(non_head_checkpoint),
        state_delta.clone(),
        commit_plan.clone(),
        std::slice::from_ref(&segment),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("state input checkpoint must be the committed head"),
        "{error}"
    );

    let mut input_checkpoint = valid_input_checkpoint.clone();
    input_checkpoint.delta.pipeline_id = PipelineId::new("different-pipeline").unwrap();
    let error = PackageReplayInputs::from_preimages(
        package_hash.clone(),
        Some(input_checkpoint),
        state_delta.clone(),
        commit_plan.clone(),
        std::slice::from_ref(&segment),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("state input checkpoint tuple does not match state delta tuple"),
        "{error}"
    );

    let mut input_checkpoint = valid_input_checkpoint.clone();
    input_checkpoint.delta.resource_id = ResourceId::new("different-resource").unwrap();
    let error = PackageReplayInputs::from_preimages(
        package_hash.clone(),
        Some(input_checkpoint),
        state_delta.clone(),
        commit_plan.clone(),
        std::slice::from_ref(&segment),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("state input checkpoint tuple does not match state delta tuple"),
        "{error}"
    );

    let mut input_checkpoint = valid_input_checkpoint.clone();
    input_checkpoint.delta.scope = ScopeKey::Resource;
    let error = PackageReplayInputs::from_preimages(
        package_hash.clone(),
        Some(input_checkpoint),
        state_delta.clone(),
        commit_plan.clone(),
        std::slice::from_ref(&segment),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("state input checkpoint tuple does not match state delta tuple"),
        "{error}"
    );

    let mut mismatched_parent = state_delta.clone();
    mismatched_parent.parent_checkpoint_id = Some(CheckpointId::new("different-parent").unwrap());
    let error = PackageReplayInputs::from_preimages(
        package_hash.clone(),
        Some(valid_input_checkpoint.clone()),
        mismatched_parent,
        commit_plan.clone(),
        std::slice::from_ref(&segment),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("state delta parent checkpoint does not match input checkpoint"),
        "{error}"
    );

    let mut mismatched_input_position = state_delta.clone();
    mismatched_input_position.input_position = Some(SourcePosition::Cursor(CursorPosition {
        version: CHECKPOINT_STATE_VERSION,
        field: "id".to_owned(),
        value: CursorValue::I64(1),
    }));
    let error = PackageReplayInputs::from_preimages(
        package_hash.clone(),
        Some(valid_input_checkpoint.clone()),
        mismatched_input_position,
        commit_plan.clone(),
        std::slice::from_ref(&segment),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("state delta input position does not match input checkpoint output position"),
        "{error}"
    );

    let mut parent_without_checkpoint = state_delta.clone();
    parent_without_checkpoint.input_position = None;
    let error = PackageReplayInputs::from_preimages(
        package_hash.clone(),
        None,
        parent_without_checkpoint,
        commit_plan.clone(),
        std::slice::from_ref(&segment),
    )
    .unwrap_err();
    assert!(
        error.to_string().contains(
            "state delta cannot reference an input checkpoint when input checkpoint artifact is null"
        ),
        "{error}"
    );

    let mut input_without_checkpoint = state_delta.clone();
    input_without_checkpoint.parent_checkpoint_id = None;
    let error = PackageReplayInputs::from_preimages(
        package_hash.clone(),
        None,
        input_without_checkpoint,
        commit_plan.clone(),
        std::slice::from_ref(&segment),
    )
    .unwrap_err();
    assert!(
        error.to_string().contains(
            "state delta cannot reference an input checkpoint when input checkpoint artifact is null"
        ),
        "{error}"
    );

    let mut empty_segments = state_delta.clone();
    empty_segments.segments.clear();
    let empty_commit_plan = DestinationCommitPlanPreimage::package_hash_token(
        TargetName::new("orders").unwrap(),
        WriteDisposition::Append,
        Vec::new(),
        SchemaHash::new("schema-fixture").unwrap(),
        empty_segments.segments.clone(),
    );
    let error = PackageReplayInputs::from_preimages(
        package_hash.clone(),
        Some(valid_input_checkpoint.clone()),
        empty_segments,
        empty_commit_plan,
        std::slice::from_ref(&segment),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("zero-segment state advancement requires a zero-segment package and typed processed-observation evidence"),
        "{error}"
    );

    let mut row_mismatch = state_delta.clone();
    row_mismatch.segments[0].row_count += 1;
    let row_mismatch_commit_plan = DestinationCommitPlanPreimage::package_hash_token(
        TargetName::new("orders").unwrap(),
        WriteDisposition::Append,
        Vec::new(),
        SchemaHash::new("schema-fixture").unwrap(),
        row_mismatch.segments.clone(),
    );
    let error = PackageReplayInputs::from_preimages(
        package_hash.clone(),
        Some(valid_input_checkpoint.clone()),
        row_mismatch,
        row_mismatch_commit_plan,
        std::slice::from_ref(&segment),
    )
    .unwrap_err();
    assert!(error.to_string().contains("rows/"), "{error}");

    let mut byte_mismatch = state_delta.clone();
    byte_mismatch.segments[0].byte_count += 1;
    let byte_mismatch_commit_plan = DestinationCommitPlanPreimage::package_hash_token(
        TargetName::new("orders").unwrap(),
        WriteDisposition::Append,
        Vec::new(),
        SchemaHash::new("schema-fixture").unwrap(),
        byte_mismatch.segments.clone(),
    );
    let error = PackageReplayInputs::from_preimages(
        package_hash.clone(),
        Some(valid_input_checkpoint),
        byte_mismatch,
        byte_mismatch_commit_plan,
        std::slice::from_ref(&segment),
    )
    .unwrap_err();
    assert!(error.to_string().contains("rows/"), "{error}");

    let mut unsupported = state_delta;
    unsupported.state_version = CHECKPOINT_STATE_VERSION + 1;
    let error = PackageReplayInputs::from_preimages(
        package_hash,
        None,
        unsupported,
        commit_plan,
        std::slice::from_ref(&segment),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unsupported state delta preimage version"),
        "{error}"
    );
}

#[test]
fn zero_segment_replay_requires_exact_typed_processed_observation_evidence() {
    let package_hash =
        PackageHash::new("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .unwrap();
    let processed_position = SourcePosition::FileManifest(FileManifest {
        version: CHECKPOINT_STATE_VERSION,
        files: vec![FilePosition {
            path: "month-07.parquet".to_owned(),
            size_bytes: 41,
            source_generation: None,
            etag: Some("etag-07".to_owned()),
            object_version: None,
            sha256: None,
        }],
    });
    let observation = ProcessedObservationPosition::new(
        "month-07.parquet",
        ProcessedObservationOutcome::Quarantined,
        processed_position.clone(),
    )
    .unwrap();
    let state_delta = StateDeltaPreimage {
        checkpoint_id: CheckpointId::new("checkpoint-zero").unwrap(),
        pipeline_id: PipelineId::new("pipeline-1").unwrap(),
        resource_id: ResourceId::new("orders").unwrap(),
        scope: ScopeKey::Resource,
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: None,
        input_position: None,
        output_position: processed_position.clone(),
        output_watermark: None,
        partition_watermarks: Vec::new(),
        late_data_carryover: Vec::new(),
        source_continuation: None,
        schema_hash: SchemaHash::new("schema-fixture").unwrap(),
        segments: Vec::new(),
    };
    let commit_plan = DestinationCommitPlanPreimage::package_hash_token(
        TargetName::new("orders").unwrap(),
        WriteDisposition::Append,
        Vec::new(),
        state_delta.schema_hash.clone(),
        Vec::new(),
    );

    let missing = PackageReplayInputs::from_preimages_with_processed(
        package_hash.clone(),
        None,
        state_delta.clone(),
        commit_plan.clone(),
        &[],
        None,
    )
    .unwrap_err();
    assert!(
        missing
            .to_string()
            .contains("typed processed-observation evidence")
    );

    let processed = ProcessedObservationEvidenceArtifact::new(
        None,
        WriteDisposition::Append,
        vec![observation],
        processed_position,
    )
    .unwrap();
    let replay = PackageReplayInputs::from_preimages_with_processed(
        package_hash,
        None,
        state_delta.clone(),
        commit_plan,
        &[],
        Some(processed.clone()),
    )
    .unwrap();
    assert!(replay.state_delta.segments.is_empty());

    let mut mismatched = processed;
    mismatched.output_position = SourcePosition::FileManifest(FileManifest {
        version: CHECKPOINT_STATE_VERSION,
        files: vec![FilePosition {
            path: "month-08.parquet".to_owned(),
            size_bytes: 42,
            source_generation: None,
            etag: Some("etag-08".to_owned()),
            object_version: None,
            sha256: None,
        }],
    });
    let error = PackageReplayInputs::from_preimages_with_processed(
        PackageHash::new("sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
            .unwrap(),
        None,
        state_delta,
        DestinationCommitPlanPreimage::package_hash_token(
            TargetName::new("orders").unwrap(),
            WriteDisposition::Append,
            Vec::new(),
            SchemaHash::new("schema-fixture").unwrap(),
            Vec::new(),
        ),
        &[],
        Some(mismatched),
    )
    .unwrap_err();
    assert!(error.to_string().contains("does not aggregate"), "{error}");
}

#[test]
fn table_snapshot_replay_preserves_exact_processed_authority_and_rejects_tamper() {
    let package_hash =
        PackageHash::new("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .unwrap();
    let position = table_snapshot_position();
    let observation = ProcessedObservationPosition::new(
        "task-000001",
        ProcessedObservationOutcome::Admitted,
        position.clone(),
    )
    .unwrap();
    let processed = ProcessedObservationEvidenceArtifact::new(
        None,
        WriteDisposition::Append,
        vec![observation],
        position.clone(),
    )
    .unwrap();
    let state_delta = StateDeltaPreimage {
        checkpoint_id: CheckpointId::new("checkpoint-table-snapshot").unwrap(),
        pipeline_id: PipelineId::new("pipeline-1").unwrap(),
        resource_id: ResourceId::new("iceberg.orders").unwrap(),
        scope: ScopeKey::Resource,
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: None,
        input_position: None,
        output_position: position,
        output_watermark: None,
        partition_watermarks: Vec::new(),
        late_data_carryover: Vec::new(),
        source_continuation: None,
        schema_hash: SchemaHash::new("schema-fixture").unwrap(),
        segments: Vec::new(),
    };
    let commit_plan = DestinationCommitPlanPreimage::package_hash_token(
        TargetName::new("orders").unwrap(),
        WriteDisposition::Append,
        Vec::new(),
        state_delta.schema_hash.clone(),
        Vec::new(),
    );

    let replay = PackageReplayInputs::from_preimages_with_processed(
        package_hash.clone(),
        None,
        state_delta.clone(),
        commit_plan.clone(),
        &[],
        Some(processed.clone()),
    )
    .unwrap();
    assert_eq!(
        replay.state_delta.output_position,
        processed.output_position
    );

    let mut tampered = processed;
    let SourcePosition::TableSnapshot(position) = &mut tampered.output_position else {
        unreachable!();
    };
    position.metadata_generation = "version-id:tampered".to_owned();
    let error = PackageReplayInputs::from_preimages_with_processed(
        package_hash,
        None,
        state_delta,
        commit_plan,
        &[],
        Some(tampered),
    )
    .unwrap_err();
    assert!(error.to_string().contains("does not aggregate"), "{error}");
}

#[test]
fn processed_observation_aggregation_respects_append_and_replace_dispositions() {
    let old = SourcePosition::FileManifest(FileManifest {
        version: CHECKPOINT_STATE_VERSION,
        files: vec![FilePosition {
            path: "old.parquet".to_owned(),
            size_bytes: 10,
            source_generation: None,
            etag: Some("old".to_owned()),
            object_version: None,
            sha256: None,
        }],
    });
    let new = SourcePosition::FileManifest(FileManifest {
        version: CHECKPOINT_STATE_VERSION,
        files: vec![FilePosition {
            path: "new.parquet".to_owned(),
            size_bytes: 20,
            source_generation: None,
            etag: Some("new".to_owned()),
            object_version: None,
            sha256: None,
        }],
    });
    let observation = ProcessedObservationPosition::new(
        "new.parquet",
        ProcessedObservationOutcome::Admitted,
        new.clone(),
    )
    .unwrap();

    let append_output = aggregate_processed_observation_positions(
        Some(&old),
        std::slice::from_ref(&observation),
        &WriteDisposition::Append,
    )
    .unwrap();
    let append = ProcessedObservationEvidenceArtifact::new(
        Some(old.clone()),
        WriteDisposition::Append,
        vec![observation.clone()],
        append_output,
    )
    .unwrap();
    assert_eq!(
        match append.output_position {
            SourcePosition::FileManifest(manifest) => manifest.files.len(),
            _ => 0,
        },
        2
    );

    let replace = ProcessedObservationEvidenceArtifact::new(
        Some(old),
        WriteDisposition::Replace,
        vec![observation],
        new,
    )
    .unwrap();
    assert_eq!(
        match replace.output_position {
            SourcePosition::FileManifest(manifest) => manifest.files.len(),
            _ => 0,
        },
        1
    );
}

#[test]
fn runtime_arrow_schema_round_trips_as_verified_package_identity() {
    let temp = tempfile::tempdir().unwrap();
    let builder = PackageBuilder::create(temp.path(), "pkg-runtime-schema").unwrap();
    let schema = sample_batch().schema();
    builder.write_runtime_arrow_schema(schema.as_ref()).unwrap();
    builder.finish().unwrap();

    let reader = PackageReader::open(temp.path()).unwrap();
    assert_eq!(reader.runtime_arrow_schema().unwrap(), schema);

    OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(temp.path().join(RUNTIME_ARROW_SCHEMA_FILE))
        .unwrap()
        .write_all(b"tampered")
        .unwrap();
    assert!(
        reader
            .runtime_arrow_schema()
            .unwrap_err()
            .to_string()
            .contains("sha256")
    );
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
fn verification_rejects_nonportable_and_case_alias_manifest_paths_before_open() {
    for invalid_path in [
        "stats/../escape.json",
        "stats/CON.json",
        "stats/alternate:stream.json",
        "stats/trailing-dot.",
        "stats/back\\slash.json",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let mut manifest = build_fixture(temp.path());
        manifest.identity.files.push(FileEntry {
            path: invalid_path.to_owned(),
            byte_count: 0,
            sha256: hex::encode(sha2::Sha256::digest([])),
        });
        manifest
            .identity
            .files
            .sort_by(|left, right| left.path.cmp(&right.path));
        manifest.package_hash = manifest_identity_hash(&manifest.identity).unwrap();
        manifest.signature.signing_input = manifest.package_hash.clone();
        fs::write(
            temp.path().join(MANIFEST_FILE),
            canonical_json_bytes(&manifest).unwrap(),
        )
        .unwrap();

        let error = verify_package(temp.path()).unwrap_err();
        assert!(
            error.message.contains("package artifact path")
                || error.message.contains("package path component"),
            "{invalid_path}: {error}"
        );
    }

    let temp = tempfile::tempdir().unwrap();
    let mut manifest = build_fixture(temp.path());
    for path in ["stats/Alias.json", "stats/alias.json"] {
        manifest.identity.files.push(FileEntry {
            path: path.to_owned(),
            byte_count: 0,
            sha256: hex::encode(sha2::Sha256::digest([])),
        });
    }
    manifest
        .identity
        .files
        .sort_by(|left, right| left.path.cmp(&right.path));
    manifest.package_hash = manifest_identity_hash(&manifest.identity).unwrap();
    manifest.signature.signing_input = manifest.package_hash.clone();
    fs::write(
        temp.path().join(MANIFEST_FILE),
        canonical_json_bytes(&manifest).unwrap(),
    )
    .unwrap();
    let error = verify_package(temp.path()).unwrap_err();
    assert!(error.message.contains("portable case folding"), "{error}");
}

#[cfg(unix)]
#[test]
fn verification_reports_unexpected_symlinks_without_following_them() {
    let temp = tempfile::tempdir().unwrap();
    build_fixture(temp.path());
    let outside = tempfile::NamedTempFile::new().unwrap();
    fs::write(outside.path(), b"outside-secret").unwrap();
    std::os::unix::fs::symlink(outside.path(), temp.path().join("stats/unexpected-link")).unwrap();

    let error = verify_package(temp.path()).unwrap_err();
    assert!(
        error
            .message
            .contains("unexpected non-regular identity entry stats/unexpected-link"),
        "{error}"
    );
    assert!(!error.message.contains("outside-secret"), "{error}");
}

#[test]
fn package_writers_reject_nonportable_identity_names() {
    let temp = tempfile::tempdir().unwrap();
    let builder = PackageBuilder::create(temp.path(), "pkg-portable-paths").unwrap();
    for path in [
        "stats/CON.json",
        "stats/name:stream",
        "stats/trailing-space ",
        "stats/back\\slash",
    ] {
        let error = builder.write_identity_artifact(path, b"value").unwrap_err();
        assert!(error.message.contains("not portable") || error.message.contains("reserved"));
    }
}

#[test]
fn verification_detects_tampered_or_missing_state_and_commit_preimages() {
    for path in [
        STATE_INPUT_CHECKPOINT_FILE,
        STATE_PROPOSED_DELTA_FILE,
        DESTINATION_COMMIT_PLAN_FILE,
    ] {
        let tampered = tempfile::tempdir().unwrap();
        build_fixture(tampered.path());
        let artifact_path = tampered.path().join(path);
        let mut file = OpenOptions::new()
            .append(true)
            .open(&artifact_path)
            .unwrap();
        file.write_all(b"tamper").unwrap();
        file.sync_all().unwrap();
        let error = verify_package(tampered.path()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains(&format!("tampered identity file {path}")),
            "{path}: {error}"
        );

        let missing = tempfile::tempdir().unwrap();
        build_fixture(missing.path());
        fs::remove_file(missing.path().join(path)).unwrap();
        let error = verify_package(missing.path()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains(&format!("missing identity file {path}")),
            "{path}: {error}"
        );
    }
}

#[test]
fn replay_inputs_reject_manifest_package_hash_mismatch_before_reconstruction() {
    let temp = tempfile::tempdir().unwrap();
    build_fixture(temp.path());
    let mut manifest = read_manifest(temp.path()).unwrap();
    manifest.package_hash = "sha256:wrong-package".to_owned();
    manifest.signature.signing_input = manifest.package_hash.clone();
    fs::write(
        temp.path().join(MANIFEST_FILE),
        canonical_json_bytes(&manifest).unwrap(),
    )
    .unwrap();

    let error = PackageReader::open(temp.path())
        .unwrap()
        .replay_inputs()
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("manifest identity hash mismatch")
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
    assert_eq!(
        report.removed_file_count,
        manifest.identity.files.len() as u64
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
fn archive_transcode_reports_unsupported_arrow_types() {
    let temp = tempfile::tempdir().unwrap();
    let builder = PackageBuilder::create(temp.path(), "pkg-archive-unsupported").unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "unsupported_time",
        DataType::Time32(TimeUnit::Second),
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Time32SecondArray::from(vec![1]))]).unwrap();
    builder
        .write_segment(
            SegmentId::new("seg-000001").unwrap(),
            0,
            &[canonical_batch(batch, 0)],
        )
        .unwrap();
    builder.finish().unwrap();

    let error = persist_package_parquet_archive(temp.path(), false).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("does not support Arrow type Time32")
    );
}

#[test]
fn archive_transcode_rejects_duplicate_column_names_before_native_write() {
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

#[test]
fn persisted_archive_writes_sidecars_manifest_metadata_and_fidelity_json() {
    let temp = tempfile::tempdir().unwrap();
    let manifest = build_archive_fixture(temp.path());
    let original_identity = manifest.identity.clone();
    let original_hash = manifest.package_hash.clone();
    let original_signature = manifest.signature.clone();
    let original_status = manifest.lifecycle.status.clone();

    let report = persist_package_parquet_archive(temp.path(), false).unwrap();

    assert_eq!(report.status, PackageArchiveWriteStatus::Written);
    assert_eq!(report.package_hash, original_hash);
    assert_eq!(report.format, "parquet");
    assert_eq!(report.fidelity_report_path, "archive/parquet/fidelity.json");
    assert_eq!(report.segments.len(), manifest.identity.segments.len());

    let archived_manifest = read_manifest(temp.path()).unwrap();
    assert_eq!(archived_manifest.identity, original_identity);
    assert_eq!(archived_manifest.package_hash, original_hash);
    assert_eq!(archived_manifest.signature, original_signature);
    assert_eq!(archived_manifest.lifecycle.status, original_status);
    let metadata = archived_manifest
        .archives
        .as_ref()
        .and_then(|archives| archives.parquet.as_ref())
        .unwrap();
    assert_eq!(metadata.format_version, 1);
    assert_eq!(metadata.segments, report.segments);
    for (index, segment) in metadata.segments.iter().enumerate() {
        assert_eq!(
            segment.segment_id,
            manifest.identity.segments[index].segment_id.as_str()
        );
        assert_eq!(
            segment.archive_path,
            format!("archive/parquet/data/{}.parquet", segment.segment_id)
        );
        assert!(temp.path().join(&segment.archive_path).is_file());
        assert_eq!(
            parquet_rows(&fs::read(temp.path().join(&segment.archive_path)).unwrap()),
            segment.archive_row_count as usize
        );
    }

    let fidelity_path = temp.path().join("archive/parquet/fidelity.json");
    let fidelity_bytes = fs::read(&fidelity_path).unwrap();
    let fidelity: PackageArchiveFidelityReport = serde_json::from_slice(&fidelity_bytes).unwrap();
    assert_eq!(fidelity.package_hash, original_hash);
    assert_eq!(fidelity.source_format, "arrow_ipc_lz4");
    assert_eq!(fidelity.archive_format, "parquet");
    assert_eq!(fidelity.segments, metadata.segments);
    assert_eq!(fidelity_bytes, canonical_json_bytes(&fidelity).unwrap());

    let verification = verify_package(temp.path()).unwrap();
    assert_eq!(verification.checked_archive_count, metadata.segments.len());
    let reader = PackageReader::open(temp.path()).unwrap();
    assert_eq!(
        reader.replay_view().unwrap().segments[0].path,
        "data/seg-000001.arrow"
    );
    assert_eq!(
        reader
            .read_segment(&SegmentId::new("seg-000001").unwrap())
            .unwrap()[0]
            .num_rows(),
        2
    );
}

#[test]
fn persisted_archive_clean_rerun_skips_and_cleans_stale_temp_paths() {
    let temp = tempfile::tempdir().unwrap();
    build_archive_fixture(temp.path());
    let first = persist_package_parquet_archive(temp.path(), false).unwrap();
    let first_manifest = fs::read(temp.path().join(MANIFEST_FILE)).unwrap();
    fs::create_dir_all(temp.path().join("archive/.tmp/stale")).unwrap();
    fs::write(
        temp.path().join("archive/.tmp/stale/partial.parquet"),
        b"stale",
    )
    .unwrap();

    let second = persist_package_parquet_archive(temp.path(), false).unwrap();

    assert_eq!(second.status, PackageArchiveWriteStatus::Skipped);
    assert_eq!(second.segments, first.segments);
    assert_eq!(
        fs::read(temp.path().join(MANIFEST_FILE)).unwrap(),
        first_manifest
    );
    assert!(!temp.path().join("archive/.tmp/stale").exists());
    verify_package(temp.path()).unwrap();
}

#[test]
fn persisted_archive_default_fails_on_tamper_and_force_replaces() {
    let temp = tempfile::tempdir().unwrap();
    build_archive_fixture(temp.path());
    persist_package_parquet_archive(temp.path(), false).unwrap();
    let manifest_before = fs::read(temp.path().join(MANIFEST_FILE)).unwrap();

    let archive_path = temp.path().join("archive/parquet/data/seg-000001.parquet");
    let mut file = OpenOptions::new().append(true).open(&archive_path).unwrap();
    file.write_all(b"tamper").unwrap();
    file.sync_all().unwrap();

    let verify_error = verify_package(temp.path()).unwrap_err();
    assert!(
        verify_error
            .to_string()
            .contains("tampered archive sidecar archive/parquet/data/seg-000001.parquet"),
        "{verify_error}"
    );
    let default_error = persist_package_parquet_archive(temp.path(), false).unwrap_err();
    assert!(
        default_error
            .to_string()
            .contains("tampered archive sidecar"),
        "{default_error}"
    );
    assert_eq!(
        fs::read(temp.path().join(MANIFEST_FILE)).unwrap(),
        manifest_before
    );

    let replaced = persist_package_parquet_archive(temp.path(), true).unwrap();

    assert_eq!(replaced.status, PackageArchiveWriteStatus::Replaced);
    verify_package(temp.path()).unwrap();
}

#[test]
fn persisted_archive_enforces_one_accounted_input_output_window() {
    assert_eq!(ARCHIVE_SEGMENT_WINDOW_BYTES, 64 * 1024 * 1024);
    let package = tempfile::tempdir().unwrap();
    build_archive_fixture(package.path());
    let reader = PackageReader::open(package.path()).unwrap();

    let memory: Arc<dyn MemoryCoordinator> =
        Arc::new(DeterministicMemoryCoordinator::new(64 * 1024, BTreeMap::new()).unwrap());
    let archive = tempfile::tempdir().unwrap();
    write_streamed_archive_temp_tree_with_memory(
        &reader,
        archive.path(),
        Arc::clone(&memory),
        64 * 1024,
    )
    .unwrap();
    let snapshot = memory.snapshot();
    let consumer = ConsumerKey::new(ARCHIVE_SEGMENT_MEMORY_CONSUMER, MemoryClass::Package).unwrap();
    assert_eq!(snapshot.current_bytes, 0);
    assert_eq!(snapshot.peak_bytes, 64 * 1024);
    assert_eq!(snapshot.consumers[&consumer].current_bytes, 0);
    assert_eq!(snapshot.consumers[&consumer].peak_bytes, 64 * 1024);

    let batches = reader
        .read_segment(&reader.manifest().identity.segments[0].segment_id)
        .unwrap();
    let retained_arrow_bytes = batches
        .into_iter()
        .map(|batch| {
            record_batch_retained_bytes(
                &cdf_package_contract::strip_package_row_ord(batch).unwrap(),
            )
            .unwrap()
        })
        .sum::<u64>();
    let combined_window = retained_arrow_bytes + 1;
    let combined_memory: Arc<dyn MemoryCoordinator> =
        Arc::new(DeterministicMemoryCoordinator::new(combined_window, BTreeMap::new()).unwrap());
    let combined_archive = tempfile::tempdir().unwrap();
    let error = write_streamed_archive_temp_tree_with_memory(
        &reader,
        combined_archive.path(),
        Arc::clone(&combined_memory),
        combined_window,
    )
    .unwrap_err();
    assert!(
        error
            .message
            .contains("Parquet output exceeds its 1-byte package archive window"),
        "{error}"
    );
    assert_eq!(combined_memory.snapshot().current_bytes, 0);
    assert!(
        fs::read_dir(combined_archive.path().join("data"))
            .unwrap()
            .next()
            .is_none()
    );

    let oversized_window = retained_arrow_bytes - 1;
    let oversized_memory: Arc<dyn MemoryCoordinator> =
        Arc::new(DeterministicMemoryCoordinator::new(oversized_window, BTreeMap::new()).unwrap());
    let oversized_archive = tempfile::tempdir().unwrap();
    let error = write_streamed_archive_temp_tree_with_memory(
        &reader,
        oversized_archive.path(),
        Arc::clone(&oversized_memory),
        oversized_window,
    )
    .unwrap_err();
    assert!(error.message.contains("Arrow bytes above its"), "{error}");
    assert_eq!(oversized_memory.snapshot().current_bytes, 0);
}

#[test]
fn archive_verification_reports_missing_source_mismatched_orphan_and_bad_fidelity() {
    let missing = tempfile::tempdir().unwrap();
    build_archive_fixture(missing.path());
    persist_package_parquet_archive(missing.path(), false).unwrap();
    fs::remove_file(
        missing
            .path()
            .join("archive/parquet/data/seg-000001.parquet"),
    )
    .unwrap();
    let error = verify_package(missing.path()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("missing archive sidecar archive/parquet/data/seg-000001.parquet"),
        "{error}"
    );

    let missing_fidelity = tempfile::tempdir().unwrap();
    build_archive_fixture(missing_fidelity.path());
    persist_package_parquet_archive(missing_fidelity.path(), false).unwrap();
    fs::remove_file(
        missing_fidelity
            .path()
            .join("archive/parquet/fidelity.json"),
    )
    .unwrap();
    let error = verify_package(missing_fidelity.path()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("missing archive fidelity report archive/parquet/fidelity.json"),
        "{error}"
    );

    let source_mismatch = tempfile::tempdir().unwrap();
    build_archive_fixture(source_mismatch.path());
    persist_package_parquet_archive(source_mismatch.path(), false).unwrap();
    let mut manifest = read_manifest(source_mismatch.path()).unwrap();
    manifest
        .archives
        .as_mut()
        .unwrap()
        .parquet
        .as_mut()
        .unwrap()
        .segments[0]
        .source_sha256 = "not-the-source-hash".to_owned();
    fs::write(
        source_mismatch.path().join(MANIFEST_FILE),
        canonical_json_bytes(&manifest).unwrap(),
    )
    .unwrap();
    let error = verify_package(source_mismatch.path()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("archive source metadata mismatch for segment seg-000001"),
        "{error}"
    );

    let orphan = tempfile::tempdir().unwrap();
    build_archive_fixture(orphan.path());
    persist_package_parquet_archive(orphan.path(), false).unwrap();
    fs::write(
        orphan.path().join("archive/parquet/data/orphan.parquet"),
        b"orphan",
    )
    .unwrap();
    let error = verify_package(orphan.path()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("orphan archive sidecar archive/parquet/data/orphan.parquet"),
        "{error}"
    );

    let bad_fidelity = tempfile::tempdir().unwrap();
    build_archive_fixture(bad_fidelity.path());
    persist_package_parquet_archive(bad_fidelity.path(), false).unwrap();
    fs::write(
        bad_fidelity.path().join("archive/parquet/fidelity.json"),
        b"{\"package_hash\":\"wrong\"}",
    )
    .unwrap();
    let error = verify_package(bad_fidelity.path()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("archive fidelity report mismatch"),
        "{error}"
    );
}

#[test]
fn archive_verification_reports_single_field_archive_metadata_mismatches() {
    let archive_hash = tempfile::tempdir().unwrap();
    build_archive_fixture(archive_hash.path());
    persist_package_parquet_archive(archive_hash.path(), false).unwrap();
    let mut manifest = read_manifest(archive_hash.path()).unwrap();
    manifest
        .archives
        .as_mut()
        .unwrap()
        .parquet
        .as_mut()
        .unwrap()
        .segments[0]
        .archive_sha256 = "not-the-archive-hash".to_owned();
    rewrite_manifest_and_fidelity(archive_hash.path(), &manifest);
    let error = verify_package(archive_hash.path()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("tampered archive sidecar archive/parquet/data/seg-000001.parquet"),
        "{error}"
    );

    let source_byte_count = tempfile::tempdir().unwrap();
    build_archive_fixture(source_byte_count.path());
    persist_package_parquet_archive(source_byte_count.path(), false).unwrap();
    let mut manifest = read_manifest(source_byte_count.path()).unwrap();
    manifest
        .archives
        .as_mut()
        .unwrap()
        .parquet
        .as_mut()
        .unwrap()
        .segments[0]
        .source_byte_count += 1;
    rewrite_manifest_and_fidelity(source_byte_count.path(), &manifest);
    let error = verify_package(source_byte_count.path()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("archive source metadata mismatch for segment seg-000001"),
        "{error}"
    );

    let source_row_count = tempfile::tempdir().unwrap();
    build_archive_fixture(source_row_count.path());
    persist_package_parquet_archive(source_row_count.path(), false).unwrap();
    let mut manifest = read_manifest(source_row_count.path()).unwrap();
    manifest
        .archives
        .as_mut()
        .unwrap()
        .parquet
        .as_mut()
        .unwrap()
        .segments[0]
        .source_row_count += 1;
    rewrite_manifest_and_fidelity(source_row_count.path(), &manifest);
    let error = verify_package(source_row_count.path()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("archive source metadata mismatch for segment seg-000001"),
        "{error}"
    );
}

#[test]
#[cfg(unix)]
fn archive_verification_distinguishes_unreadable_sidecar_and_fidelity_paths() {
    use std::os::unix::fs::PermissionsExt;

    let sidecar = tempfile::tempdir().unwrap();
    build_archive_fixture(sidecar.path());
    persist_package_parquet_archive(sidecar.path(), false).unwrap();
    let sidecar_path = sidecar
        .path()
        .join("archive/parquet/data/seg-000001.parquet");
    fs::set_permissions(&sidecar_path, fs::Permissions::from_mode(0o000)).unwrap();
    let error = verify_package(sidecar.path()).unwrap_err();
    let _ = fs::set_permissions(&sidecar_path, fs::Permissions::from_mode(0o600));
    assert!(
        error
            .to_string()
            .contains("archive sidecar archive/parquet/data/seg-000001.parquet could not be read"),
        "{error}"
    );

    let fidelity = tempfile::tempdir().unwrap();
    build_archive_fixture(fidelity.path());
    persist_package_parquet_archive(fidelity.path(), false).unwrap();
    let fidelity_path = fidelity.path().join("archive/parquet/fidelity.json");
    fs::set_permissions(&fidelity_path, fs::Permissions::from_mode(0o000)).unwrap();
    let error = verify_package(fidelity.path()).unwrap_err();
    let _ = fs::set_permissions(&fidelity_path, fs::Permissions::from_mode(0o600));
    assert!(
        error
            .to_string()
            .contains("archive fidelity report archive/parquet/fidelity.json could not be read"),
        "{error}"
    );
}

#[test]
fn force_archive_reports_replaced_when_manifest_metadata_survives_missing_tree() {
    let temp = tempfile::tempdir().unwrap();
    build_archive_fixture(temp.path());
    persist_package_parquet_archive(temp.path(), false).unwrap();
    fs::remove_dir_all(temp.path().join("archive/parquet")).unwrap();

    let report = persist_package_parquet_archive(temp.path(), true).unwrap();

    assert_eq!(report.status, PackageArchiveWriteStatus::Replaced);
    verify_package(temp.path()).unwrap();
}

#[test]
fn persisted_archive_rejects_unsafe_manifest_segment_ids() {
    for segment_id in ["bad/id", "bad\\id", "bad..id", "."] {
        let temp = tempfile::tempdir().unwrap();
        build_archive_fixture(temp.path());
        let mut manifest = read_manifest(temp.path()).unwrap();
        manifest.identity.segments[0].segment_id = SegmentId::new(segment_id).unwrap();
        manifest.package_hash = manifest_identity_hash(&manifest.identity).unwrap();
        manifest.signature.signing_input = manifest.package_hash.clone();
        fs::write(
            temp.path().join(MANIFEST_FILE),
            canonical_json_bytes(&manifest).unwrap(),
        )
        .unwrap();

        let error = persist_package_parquet_archive(temp.path(), false).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("segment id cannot be used as an archive file name"),
            "{segment_id}: {error}"
        );
    }
}

#[test]
fn persisted_archive_status_gate_allows_only_ratified_statuses() {
    for status in [
        PackageStatus::Packaged,
        PackageStatus::Loaded,
        PackageStatus::Committed,
        PackageStatus::Checkpointed,
    ] {
        let temp = tempfile::tempdir().unwrap();
        build_archive_fixture(temp.path());
        update_package_status(temp.path(), status.clone()).unwrap();
        let report = persist_package_parquet_archive(temp.path(), false).unwrap();
        assert_eq!(report.status, PackageArchiveWriteStatus::Written);
        assert_eq!(read_manifest(temp.path()).unwrap().lifecycle.status, status);
    }

    for status in [
        PackageStatus::Planned,
        PackageStatus::Extracting,
        PackageStatus::Validated,
        PackageStatus::Loading,
        PackageStatus::Archived,
    ] {
        let temp = tempfile::tempdir().unwrap();
        build_archive_fixture(temp.path());
        update_package_status(temp.path(), status.clone()).unwrap();
        let error = persist_package_parquet_archive(temp.path(), false).unwrap_err();
        assert!(
            error
                .to_string()
                .contains(&format!("status {} cannot be archived", status.as_str())),
            "{error}"
        );
    }
}

fn rewrite_manifest_and_fidelity(package_dir: &Path, manifest: &PackageManifest) {
    fs::write(
        package_dir.join(MANIFEST_FILE),
        canonical_json_bytes(manifest).unwrap(),
    )
    .unwrap();
    let metadata = manifest
        .archives
        .as_ref()
        .and_then(|archives| archives.parquet.as_ref())
        .unwrap();
    let fidelity = PackageArchiveFidelityReport {
        package_hash: manifest.package_hash.clone(),
        source_format: "arrow_ipc_lz4".to_owned(),
        archive_format: "parquet".to_owned(),
        fidelity_statement: metadata.fidelity_statement.clone(),
        segments: metadata.segments.clone(),
    };
    fs::write(
        package_dir.join("archive/parquet/fidelity.json"),
        canonical_json_bytes(&fidelity).unwrap(),
    )
    .unwrap();
}

#[test]
fn production_commit_paths_cannot_collect_package_segments() {
    let crates_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cdf-package has a crates parent");
    let archive_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("archive.rs");
    let reader_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("reader.rs");
    let mut files = vec![archive_path.clone(), reader_path.clone()];
    files.push(crates_dir.join("cdf-project/src/promotion.rs"));
    for relative in [
        "cdf-project/src/runtime",
        "cdf-dest-duckdb/src",
        "cdf-dest-postgres/src",
        "cdf-dest-parquet/src",
    ] {
        collect_production_rust_files(&crates_dir.join(relative), &mut files);
    }
    for path in files {
        let source = fs::read_to_string(&path).unwrap();
        let mut forbidden = vec![
            "Vec<CommitSegment>",
            "read_quarantine_records(",
            "read_dedup_dropped_provenance(",
        ];
        if path != archive_path && path != reader_path {
            forbidden.push("read_segment(");
        }
        for forbidden in forbidden {
            assert!(
                !source.contains(forbidden),
                "production package materialization token {forbidden:?} found in {}",
                path.display()
            );
        }
    }
}

fn collect_production_rust_files(directory: &Path, output: &mut Vec<std::path::PathBuf>) {
    for entry in fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            collect_production_rust_files(&path, output);
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs")
            && !matches!(
                path.file_name().and_then(|name| name.to_str()),
                Some("tests.rs" | "live_tests.rs")
            )
        {
            output.push(path);
        }
    }
}
