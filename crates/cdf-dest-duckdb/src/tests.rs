use super::*;
use crate::{
    sql::parse_target,
    table::{existing_columns, require_targetable_provenance},
};
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use cdf_conformance::destination::{
    DestinationConformanceCase, DestinationCorrectionConformanceEvidence,
    assert_destination_conformance, assert_destination_correction_conformance,
    representative_commit_request,
};
use cdf_kernel::{
    CHECKPOINT_STATE_VERSION, CanonicalArrowField, CheckpointId, CorrectionStrategy,
    CursorPosition, CursorValue, DeliveryGuarantee, DestinationCorrectionCommitRequest,
    DestinationCorrectionOperation, DestinationCorrectionPlan,
    DestinationCorrectionReceiptEvidence, DestinationCorrectionRequest, IdempotencyToken,
    PackageHash, PartitionId, PipelineId, PlanId, ProcessedObservationOutcome,
    ProcessedObservationPosition, PromotionId, ResidualCorrectionOperation, ResourceId,
    RowProvenanceAddress, ScanPlan, ScanRequest, ScopeKey, SegmentAck, SegmentId, SourcePosition,
    StateSegment,
};
use cdf_package::{PackageBuilder, PackageReader};
use cdf_package_contract::{
    DestinationCommitPlanPreimage, PROCESSED_OBSERVATIONS_FILE, PackageStatus,
    ProcessedObservationEvidenceArtifact, SegmentEntry, StateDeltaPreimage,
};
use cdf_runtime::{DestinationRuntime, DurableSegmentReader, StagedSegmentIngress};

use crate::sheet::duckdb_correction_capabilities;

fn sample_batch(ids: Vec<i64>, names: Vec<Option<&str>>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let id: ArrayRef = Arc::new(Int64Array::from(ids));
    let name: ArrayRef = Arc::new(StringArray::from(names));
    RecordBatch::try_new(schema, vec![id, name]).unwrap()
}

fn residual_batch() -> RecordBatch {
    let value_42 = Int64Array::from(vec![42_i64]);
    let keep = StringArray::from(vec!["keep"]);
    let first = cdf_contract::encode_residual_json_v1([
        cdf_contract::ResidualFieldRef::new(["age"], &value_42, 0).unwrap(),
        cdf_contract::ResidualFieldRef::new(["keep"], &keep, 0).unwrap(),
    ])
    .unwrap();
    let value_84 = Int64Array::from(vec![84_i64]);
    let second = cdf_contract::encode_residual_json_v1([cdf_contract::ResidualFieldRef::new(
        ["age"],
        &value_84,
        0,
    )
    .unwrap()])
    .unwrap();
    let mut metadata = std::collections::HashMap::new();
    metadata.insert(
        cdf_kernel::SEMANTIC_METADATA_KEY.to_owned(),
        cdf_contract::VARIANT_SEMANTIC_TAG.to_owned(),
    );
    metadata.insert(
        cdf_contract::RESIDUAL_ENCODING_METADATA_KEY.to_owned(),
        cdf_contract::RESIDUAL_ENCODING_NAME.to_owned(),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new(cdf_contract::VARIANT_COLUMN_NAME, DataType::Utf8, true).with_metadata(metadata),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2_i64])),
            Arc::new(StringArray::from(vec![Some("ada"), Some("grace")])),
            Arc::new(StringArray::from(vec![
                Some(String::from_utf8(first).unwrap()),
                Some(String::from_utf8(second).unwrap()),
            ])),
        ],
    )
    .unwrap()
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
                selected_strategy: CorrectionStrategy::InPlaceUpdate,
            },
            transaction_guarantee: TransactionSupport::AtomicPackage,
            idempotency_guarantee: IdempotencySupport::PackageToken,
        },
        output_field: CanonicalArrowField::from_arrow(&Field::new("age", DataType::Int64, true))
            .unwrap(),
        promoted_value_residual_json_v1: authority,
    }
}

fn correction_request(
    operations: Vec<DestinationCorrectionOperation>,
) -> DestinationCorrectionCommitRequest {
    let correction_hash = PackageHash::new("sha256:correction-age").unwrap();
    DestinationCorrectionCommitRequest::new(
        correction_hash.clone(),
        IdempotencyToken::new(correction_hash.as_str()).unwrap(),
        TargetName::new("orders").unwrap(),
        WriteDisposition::Append,
        vec![state_segment_for(
            "seg-correction",
            operations.len() as u64,
            4,
        )],
        operations,
    )
    .unwrap()
}

fn finalize_correction(
    destination: &DuckDbDestination,
    request: &DestinationCorrectionCommitRequest,
) -> Receipt {
    let plan = destination.plan_correction(request).unwrap();
    let mut session = destination.begin_correction(request.clone(), plan).unwrap();
    session.apply_migrations().unwrap();
    assert_eq!(
        session.apply_corrections().unwrap().rows_updated,
        Some(request.addressed_row_count())
    );
    session.finalize().unwrap()
}

fn build_package(package_dir: &Path, package_id: &str, batches: &[RecordBatch]) -> PackageHash {
    build_package_for_commit(
        package_dir,
        package_id,
        batches,
        WriteDisposition::Append,
        Vec::new(),
    )
}

fn build_package_for_commit(
    package_dir: &Path,
    package_id: &str,
    batches: &[RecordBatch],
    disposition: WriteDisposition,
    merge_keys: Vec<String>,
) -> PackageHash {
    build_package_segments_for_commit(
        package_dir,
        package_id,
        &[(SegmentId::new("seg-000001").unwrap(), batches.to_vec())],
        disposition,
        merge_keys,
    )
}

fn build_package_segments(
    package_dir: &Path,
    package_id: &str,
    segments: &[(SegmentId, Vec<RecordBatch>)],
) -> PackageHash {
    build_package_segments_for_commit(
        package_dir,
        package_id,
        segments,
        WriteDisposition::Append,
        Vec::new(),
    )
}

fn build_package_segments_for_commit(
    package_dir: &Path,
    package_id: &str,
    segments: &[(SegmentId, Vec<RecordBatch>)],
    disposition: WriteDisposition,
    merge_keys: Vec<String>,
) -> PackageHash {
    let builder = PackageBuilder::create(package_dir, package_id).unwrap();
    builder.update_status(PackageStatus::Extracting).unwrap();
    let schema = segments
        .iter()
        .flat_map(|(_, batches)| batches)
        .next()
        .map_or_else(|| Arc::new(Schema::empty()), RecordBatch::schema);
    write_current_plan_artifacts(&builder, schema.as_ref());
    builder.write_runtime_arrow_schema(schema.as_ref()).unwrap();
    builder
        .write_json_artifact(
            "schema/output.arrow.json",
            &BTreeMap::from([("schema_hash", "schema-v1")]),
        )
        .unwrap();
    let entries = segments
        .iter()
        .map(|(segment_id, batches)| builder.write_segment(segment_id.clone(), batches).unwrap())
        .collect::<Vec<_>>();
    write_current_state_artifacts(&builder, &entries, disposition, merge_keys);
    let manifest = builder.finish().unwrap();
    PackageHash::new(manifest.package_hash).unwrap()
}

fn write_current_plan_artifacts(builder: &PackageBuilder, schema: &Schema) {
    let mut program = cdf_contract::compile_validation_program(
        &cdf_contract::ContractPolicy::evolve(),
        &cdf_contract::ObservedSchema::from_arrow(schema),
    )
    .unwrap();
    program.row_rules.clear();
    program.transforms.clear();
    program.compiled_expression_plan = Some(
        cdf_contract::CompiledExpressionPlan::current(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .unwrap(),
    );
    builder
        .write_json_artifact("plan/validation-program.json", &program)
        .unwrap();
    builder
        .write_json_artifact(
            "plan/scan.json",
            &ScanPlan {
                plan_id: PlanId::new("duckdb-current-test-plan").unwrap(),
                request: ScanRequest {
                    resource_id: ResourceId::new("orders").unwrap(),
                    projection: None,
                    filters: Vec::new(),
                    limit: None,
                    order_by: Vec::new(),
                    scope: ScopeKey::Resource,
                },
                partitions: Vec::new(),
                pushed_predicates: Vec::new(),
                unsupported_predicates: Vec::new(),
                estimated_rows: None,
                estimated_bytes: None,
                delivery_guarantee: DeliveryGuarantee::AtLeastOnceDuplicateRisk,
            },
        )
        .unwrap();
}

fn write_current_state_artifacts(
    builder: &PackageBuilder,
    entries: &[SegmentEntry],
    disposition: WriteDisposition,
    merge_keys: Vec<String>,
) {
    let scope = ScopeKey::Partition {
        partition_id: PartitionId::new("p0").unwrap(),
    };
    let output_position = SourcePosition::Cursor(CursorPosition {
        version: CHECKPOINT_STATE_VERSION,
        field: "id".to_owned(),
        value: CursorValue::I64(3),
    });
    let segments = entries
        .iter()
        .map(|entry| StateSegment {
            segment_id: entry.segment_id.clone(),
            scope: scope.clone(),
            output_position: output_position.clone(),
            row_count: entry.row_count,
            byte_count: entry.byte_count,
        })
        .collect::<Vec<_>>();
    if entries.is_empty() {
        let observation = ProcessedObservationPosition::new(
            "duckdb-current-test-empty-observation",
            ProcessedObservationOutcome::Quarantined,
            output_position.clone(),
        )
        .unwrap();
        builder
            .write_json_artifact(
                PROCESSED_OBSERVATIONS_FILE,
                &ProcessedObservationEvidenceArtifact::new(
                    None,
                    disposition.clone(),
                    vec![observation],
                    output_position.clone(),
                )
                .unwrap(),
            )
            .unwrap();
    }
    builder.write_input_checkpoint_artifact(&None).unwrap();
    builder
        .write_state_delta_preimage_artifact(&StateDeltaPreimage {
            checkpoint_id: CheckpointId::new("checkpoint-duckdb-current-test").unwrap(),
            pipeline_id: PipelineId::new("pipeline-duckdb-current-test").unwrap(),
            resource_id: ResourceId::new("orders").unwrap(),
            scope,
            state_version: CHECKPOINT_STATE_VERSION,
            parent_checkpoint_id: None,
            input_position: None,
            output_position,
            schema_hash: SchemaHash::new("schema-v1").unwrap(),
            segments: segments.clone(),
        })
        .unwrap();
    builder
        .write_commit_plan_preimage_artifact(&DestinationCommitPlanPreimage::package_hash_token(
            TargetName::new("orders").unwrap(),
            disposition,
            merge_keys,
            SchemaHash::new("schema-v1").unwrap(),
            segments,
        ))
        .unwrap();
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
) -> CurrentCommitRequest {
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
) -> CurrentCommitRequest {
    let package = PackageReader::open(package_dir)
        .unwrap()
        .into_verified()
        .unwrap();
    let inputs = package.replay_inputs().unwrap();
    assert_eq!(inputs.destination_commit.package_hash, package_hash);
    assert_eq!(inputs.destination_commit.disposition, disposition);
    assert_eq!(inputs.merge_keys, merge_keys);
    assert_eq!(inputs.destination_commit.segments.len(), segments.len());
    for (recorded, expected) in inputs.destination_commit.segments.iter().zip(&segments) {
        assert_eq!(recorded.segment_id, expected.segment_id);
        assert_eq!(recorded.row_count, expected.row_count);
    }
    CurrentCommitRequest {
        package_dir: package_dir.to_path_buf(),
        commit: inputs.destination_commit,
        schema_hash: inputs.schema_hash,
        merge_keys: inputs.merge_keys,
    }
}

fn destination(path: &Path) -> DuckDbDestination {
    DuckDbDestination::new(path).unwrap()
}

#[test]
fn connections_enforce_bounded_native_resources() {
    let temp = tempfile::tempdir().unwrap();
    let destination = destination(&temp.path().join("bounded.duckdb"));
    let conn = destination.open_connection().unwrap();
    let settings: (String, i64, String, bool) = conn
        .query_row(
            "SELECT current_setting('memory_limit'), current_setting('threads'), current_setting('max_temp_directory_size'), current_setting('preserve_insertion_order')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        settings,
        ("256.0 MiB".to_owned(), 1, "1.0 GiB".to_owned(), false)
    );
    drop(conn);

    let read_only = destination.open_read_only_connection().unwrap();
    let threads: i64 = read_only
        .query_row("SELECT current_setting('threads')", [], |row| row.get(0))
        .unwrap();
    assert_eq!(threads, 1);
}

#[derive(Debug)]
struct CurrentCommitOutcome {
    receipt: Receipt,
}

#[derive(Clone, Debug)]
struct CurrentCommitRequest {
    package_dir: PathBuf,
    commit: DestinationCommitRequest,
    schema_hash: SchemaHash,
    merge_keys: Vec<String>,
}

struct TestDurableSegmentReader {
    identity: cdf_runtime::StagedSegmentIdentity,
    batches: std::vec::IntoIter<RecordBatch>,
}

struct TestStagedSegmentStream {
    requests: std::vec::IntoIter<cdf_runtime::StagedSegmentRequest>,
    acknowledgements: Vec<cdf_runtime::StagedSegmentAck>,
}

impl cdf_runtime::StagedSegmentStream for TestStagedSegmentStream {
    fn next_segment(&mut self) -> Result<Option<cdf_runtime::StagedSegmentRequest>> {
        Ok(self.requests.next())
    }

    fn acknowledge(&mut self, acknowledgement: cdf_runtime::StagedSegmentAck) -> Result<()> {
        self.acknowledgements.push(acknowledgement);
        Ok(())
    }
}

impl DurableSegmentReader for TestDurableSegmentReader {
    fn identity(&self) -> &cdf_runtime::StagedSegmentIdentity {
        &self.identity
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.batches.next())
    }
}

fn commit_current(
    destination: &DuckDbDestination,
    request: CurrentCommitRequest,
) -> CurrentCommitOutcome {
    try_commit_current(destination, request).unwrap()
}

fn try_commit_current(
    destination: &DuckDbDestination,
    request: CurrentCommitRequest,
) -> Result<CurrentCommitOutcome> {
    static ATTEMPT: AtomicU64 = AtomicU64::new(0);
    let package = PackageReader::open(&request.package_dir)?.into_verified()?;
    let reader = package.reader();
    let verified = package.verification();
    let output_schema = reader.runtime_arrow_schema_verified(verified)?;
    let mut runtime = destination.clone();
    let capabilities = runtime.runtime_capabilities();
    let destination_id = runtime.sheet().destination.clone();
    let bulk_path = runtime.prepare_selected_bulk_path(
        &cdf_runtime::BulkPathPreparationInput::new(output_schema.as_ref())
            .with_commit(&request.commit),
    )?;
    let plan = runtime.plan_commit(&request.commit)?;
    let attempt_id = cdf_runtime::LoadAttemptId::new(format!(
        "duckdb-test-{}",
        ATTEMPT.fetch_add(1, Ordering::Relaxed)
    ))?;
    let staging_identity = cdf_runtime::StagingLeaseIdentity::new(
        destination_id.clone(),
        request.commit.target.clone(),
        attempt_id.clone(),
    );
    let services = cdf_conformance::test_execution_services();
    let managed_lease = services.acquire_staging_lease(staging_identity)?;
    let staging_lease = managed_lease.snapshot()?;
    let mutation_guard = managed_lease.mutation_guard()?;
    let mut session = runtime.begin_staged_ingress(cdf_runtime::StagedIngressRequest::new(
        attempt_id.clone(),
        cdf_runtime::StagingAttemptBinding {
            destination_id,
            target: request.commit.target.clone(),
            disposition: request.commit.disposition.clone(),
            schema_hash: request.schema_hash.clone(),
            output_arrow_schema_hash: cdf_kernel::canonical_arrow_schema_hash(
                output_schema.as_ref(),
            )?,
            merge_keys: request.merge_keys.clone(),
            execution_plan_id: reader.recorded_scan_plan_verified(verified)?.plan_id,
        },
        staging_lease,
        mutation_guard,
        bulk_path,
        cdf_runtime::StagingSchedulingContext::new(
            capabilities
                .max_in_flight_segments
                .ok_or_else(|| CdfError::contract("test destination omitted segment bound"))?,
            capabilities
                .max_in_flight_bytes
                .ok_or_else(|| CdfError::contract("test destination omitted byte bound"))?,
        )?,
        output_schema.as_ref().clone(),
    )?)?;
    let requests = reader
        .manifest()
        .identity
        .segments
        .iter()
        .enumerate()
        .map(|(ordinal, entry)| {
            let identity = cdf_runtime::StagedSegmentIdentity::from_manifest_entry(
                entry,
                request.schema_hash.clone(),
                u32::try_from(ordinal)
                    .map_err(|_| CdfError::data("test package has too many segments"))?,
            )?;
            let batches = reader.read_segment(&entry.segment_id)?.into_iter();
            cdf_runtime::StagedSegmentRequest::new(
                identity.clone(),
                Box::new(TestDurableSegmentReader { identity, batches }),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let mut stream = TestStagedSegmentStream {
        requests: requests.into_iter(),
        acknowledgements: Vec::new(),
    };
    session.stage_stream(&mut stream)?;
    let binding =
        cdf_runtime::VerifiedFinalBinding::from_verified_package(attempt_id, &package, plan)?;
    let receipt = session.bind_final(binding)?.receipt;
    managed_lease.finish()?;
    Ok(CurrentCommitOutcome { receipt })
}

fn plan_current_package_commit(
    destination: &DuckDbDestination,
    request: &CurrentCommitRequest,
) -> Result<DuckDbCommitPlan> {
    let reader = PackageReader::open(&request.package_dir)?;
    let schema =
        if request
            .package_dir
            .join(cdf_package::RUNTIME_ARROW_SCHEMA_FILE)
            .exists()
        {
            reader.runtime_arrow_schema()?
        } else {
            let first =
                reader.manifest().identity.segments.first().ok_or_else(|| {
                    CdfError::data("DuckDB package has no segment schema authority")
                })?;
            reader
                .read_segment(&first.segment_id)?
                .into_iter()
                .next()
                .map(|batch| batch.schema())
                .ok_or_else(|| CdfError::data("DuckDB package first segment has no Arrow batch"))?
        };
    destination.plan_schema_commit(&request.commit, schema.as_ref())
}

#[test]
fn sheet_declares_duckdb_destination_contract() {
    let temp = tempfile::tempdir().unwrap();
    let mut dest = destination(&temp.path().join("local.duckdb"));
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
    let runtime = &mut dest as &mut dyn cdf_runtime::DestinationRuntime;
    assert_eq!(
        runtime.runtime_capabilities().bulk_path.as_deref(),
        Some("arrow_record_batch_appender")
    );
    assert_eq!(
        runtime.runtime_capabilities().ingress_mode,
        cdf_runtime::DestinationIngressMode::StagedDurableSegments
    );
    assert!(matches!(
        runtime.ingress(),
        cdf_runtime::DestinationIngress::StagedSegments(_)
    ));
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
        &DestinationCorrectionConformanceEvidence {
            row_provenance_persistence: CapabilitySupport::Supported,
            row_provenance_targetability: CapabilitySupport::Supported,
            residual_readback: CapabilitySupport::Supported,
            strategies: duckdb_correction_capabilities().strategies,
        },
    );
}

#[test]
fn verified_final_binding_rejects_execution_plan_drift() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-execution-plan-drift");
    let package_hash = build_package(
        &package_dir,
        "pkg-execution-plan-drift",
        &[sample_batch(vec![1], vec![Some("ada")])],
    );
    let request = request(
        &package_dir,
        package_hash,
        WriteDisposition::Append,
        Vec::new(),
        1,
    );
    let destination = destination(&temp.path().join("plan-drift.duckdb"));
    let plan = destination.plan_commit(&request.commit).unwrap();
    let package = PackageReader::open(&package_dir)
        .unwrap()
        .into_verified()
        .unwrap();

    let error = cdf_runtime::VerifiedFinalBinding::from_verified_package_with_execution_authority(
        cdf_runtime::LoadAttemptId::new("duckdb-plan-drift").unwrap(),
        PlanId::new("different-execution-plan").unwrap(),
        &package,
        plan,
    )
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("does not match recorded package execution plan"),
        "{error}"
    );
}

#[test]
fn staged_segment_ingress_returns_verifiable_receipt_and_exact_provenance() {
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
    let session_dest = destination(&temp.path().join("session.duckdb"));
    let receipt = commit_current(&session_dest, request.clone()).receipt;

    assert_eq!(
        receipt.transaction.as_ref().map(|tx| tx.system.as_str()),
        Some("duckdb")
    );
    assert_eq!(
        receipt.segment_acks,
        request
            .commit
            .segments
            .iter()
            .map(|state| SegmentAck {
                segment_id: state.segment_id.clone(),
                row_count: state.row_count,
                byte_count: state.byte_count,
            })
            .collect::<Vec<_>>()
    );
    assert!(session_dest.verify_receipt(&receipt).unwrap().verified);
    let protocol: &dyn DestinationProtocol = &session_dest;
    assert!(protocol.verify(&receipt).unwrap().verified);
    let conn = Connection::open(session_dest.database_path()).unwrap();
    let provenance: Vec<(i64, String, u64)> = conn
        .prepare("SELECT o.id, p.segment_id, o._cdf_row_key - p.row_key_start FROM orders o JOIN _cdf_segments p ON o._cdf_row_key >= p.row_key_start AND o._cdf_row_key < p.row_key_end ORDER BY o.id")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    assert_eq!(
        provenance,
        vec![
            (1, "seg-000001".to_owned(), 0),
            (2, "seg-000001".to_owned(), 1),
            (3, "seg-000002".to_owned(), 0),
        ]
    );
}

#[test]
fn staged_duplicate_returns_existing_receipt_without_extra_rows() {
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

    let first = commit_current(&dest, request.clone()).receipt;
    let duplicate = commit_current(&dest, request.clone()).receipt;

    assert_eq!(duplicate, first);
    assert!(dest.verify_receipt(&duplicate).unwrap().verified);

    let conn = Connection::open(db_path).unwrap();
    let target_rows: u64 = conn
        .query_row("SELECT count(*) FROM orders", [], |row| row.get(0))
        .unwrap();
    assert_eq!(target_rows, 3);
    assert_eq!(crate::mirrors::next_row_key(&conn).unwrap(), 4);

    let mirror = dest.read_mirror_snapshot_read_only().unwrap();
    assert_eq!(mirror.loads.len(), 1);
    assert_eq!(mirror.state.len(), 1);
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

    let outcome = commit_current(&dest, request.clone());
    assert_eq!(outcome.receipt.counts.rows_written, 3);
    assert!(dest.verify_receipt(&outcome.receipt).unwrap().verified);

    let reopened = destination(&db_path);
    assert!(reopened.verify_receipt(&outcome.receipt).unwrap().verified);
    let duplicate = commit_current(&reopened, request);
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
    let provenance: Vec<(i64, String, String, u64)> = conn
        .prepare("SELECT o.id, p.package_hash, p.segment_id, o._cdf_row_key - p.row_key_start FROM orders o JOIN _cdf_segments p ON o._cdf_row_key >= p.row_key_start AND o._cdf_row_key < p.row_key_end ORDER BY o._cdf_row_key")
        .unwrap()
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    assert_eq!(
        provenance,
        vec![
            (1, package_hash.to_string(), "seg-000001".to_owned(), 0),
            (2, package_hash.to_string(), "seg-000001".to_owned(), 1),
            (3, package_hash.to_string(), "seg-000001".to_owned(), 2),
        ]
    );
    let duplicate_address = conn.execute(
        "INSERT INTO orders (id, name, _cdf_row_key) \
         SELECT 99, 'duplicate', _cdf_row_key FROM orders WHERE id = 1",
        [],
    );
    assert_eq!(duplicate_address.unwrap(), 1);
    let target = parse_target(&TargetName::new("orders").unwrap()).unwrap();
    let existing = existing_columns(&conn, &target).unwrap();
    let duplicate_error = require_targetable_provenance(&conn, &target, &existing).unwrap_err();
    assert!(
        duplicate_error
            .to_string()
            .contains("duplicate compact row-provenance"),
        "{duplicate_error}"
    );
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
    let append_receipt = commit_current(&dest, empty_append).receipt;
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
    commit_current(
        &dest,
        request(
            &data_dir,
            data_hash,
            WriteDisposition::Append,
            Vec::new(),
            2,
        ),
    );

    let empty_replace_dir = temp.path().join("pkg-empty-replace");
    let empty_replace_hash = build_package_segments_for_commit(
        &empty_replace_dir,
        "pkg-empty-replace",
        &[],
        WriteDisposition::Replace,
        Vec::new(),
    );
    let empty_replace = request_with_segments(
        &empty_replace_dir,
        empty_replace_hash,
        WriteDisposition::Replace,
        Vec::new(),
        Vec::new(),
    );
    let replace_receipt = commit_current(&dest, empty_replace).receipt;
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
    commit_current(
        &dest,
        request(
            &first_package,
            first_hash,
            WriteDisposition::Append,
            Vec::new(),
            2,
        ),
    );

    let second_package = temp.path().join("pkg-second");
    let second_hash = build_package_for_commit(
        &second_package,
        "pkg-second",
        &[sample_batch(vec![9], vec![Some("new")])],
        WriteDisposition::Replace,
        Vec::new(),
    );
    let outcome = commit_current(
        &dest,
        request(
            &second_package,
            second_hash.clone(),
            WriteDisposition::Replace,
            Vec::new(),
            1,
        ),
    );
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
    let provenance: (String, String, u64) = conn
        .query_row(
            "SELECT p.package_hash, p.segment_id, o._cdf_row_key - p.row_key_start FROM orders o JOIN _cdf_segments p ON o._cdf_row_key >= p.row_key_start AND o._cdf_row_key < p.row_key_end",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        provenance,
        (second_hash.to_string(), "seg-000001".to_owned(), 0)
    );
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
    commit_current(
        &dest,
        request(
            &initial_package,
            initial_hash.clone(),
            WriteDisposition::Append,
            Vec::new(),
            2,
        ),
    );

    let merge_package = temp.path().join("pkg-merge");
    let merge_hash = build_package_for_commit(
        &merge_package,
        "pkg-merge",
        &[sample_batch(
            vec![1, 1, 3],
            vec![Some("new"), Some("new"), Some("insert")],
        )],
        WriteDisposition::Merge,
        vec!["id".to_owned()],
    );
    let outcome = commit_current(
        &dest,
        request(
            &merge_package,
            merge_hash.clone(),
            WriteDisposition::Merge,
            vec!["id".to_owned()],
            3,
        ),
    );
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
    let provenance: Vec<(i64, String, u64)> = conn
        .prepare("SELECT o.id, p.package_hash, o._cdf_row_key - p.row_key_start FROM orders o JOIN _cdf_segments p ON o._cdf_row_key >= p.row_key_start AND o._cdf_row_key < p.row_key_end ORDER BY o.id")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    assert_eq!(
        provenance,
        vec![
            (1, merge_hash.to_string(), 0),
            (2, initial_hash.to_string(), 1),
            (3, merge_hash.to_string(), 2),
        ]
    );
}

#[test]
fn preexisting_targets_without_current_provenance_fail_without_mutation() {
    let temp = tempfile::tempdir().unwrap();
    let package = temp.path().join("pkg-legacy");
    let package_hash = build_package(
        &package,
        "pkg-legacy",
        &[sample_batch(vec![2], vec![Some("new")])],
    );
    let db_path = temp.path().join("legacy.duckdb");
    let conn = Connection::open(&db_path).unwrap();
    conn.execute_batch("CREATE TABLE orders (id BIGINT NOT NULL, name VARCHAR)")
        .unwrap();
    conn.execute("INSERT INTO orders VALUES (1, 'legacy')", [])
        .unwrap();
    drop(conn);

    let dest = destination(&db_path);
    let error = plan_current_package_commit(
        &dest,
        &request(
            &package,
            package_hash,
            WriteDisposition::Append,
            Vec::new(),
            1,
        ),
    )
    .unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("current compact _cdf_row_key"),
        "{message}"
    );
    assert!(message.contains("use replace"), "{message}");

    let conn = Connection::open(db_path).unwrap();
    let rows: Vec<(i64, String)> = conn
        .prepare("SELECT id, name FROM orders")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    assert_eq!(rows, vec![(1, "legacy".to_owned())]);
}

#[test]
fn compact_provenance_requires_exact_non_null_types_and_unique_address() {
    let temp = tempfile::tempdir().unwrap();
    let package = temp.path().join("pkg-legacy-provenance-shape");
    let package_hash = build_package(
        &package,
        "pkg-legacy-provenance-shape",
        &[sample_batch(vec![1], vec![Some("row")])],
    );

    let nullable_path = temp.path().join("nullable.duckdb");
    let conn = Connection::open(&nullable_path).unwrap();
    conn.execute_batch(
        "CREATE TABLE orders (
            id BIGINT NOT NULL,
            name VARCHAR,
            _cdf_row_key UBIGINT
        )",
    )
    .unwrap();
    drop(conn);
    let error = plan_current_package_commit(
        &destination(&nullable_path),
        &request(
            &package,
            package_hash.clone(),
            WriteDisposition::Append,
            Vec::new(),
            1,
        ),
    )
    .unwrap_err();
    assert!(error.to_string().contains("is nullable"), "{error}");

    let wrong_type_path = temp.path().join("wrong-type.duckdb");
    let conn = Connection::open(&wrong_type_path).unwrap();
    conn.execute_batch(
        "CREATE TABLE orders (
            id BIGINT NOT NULL,
            name VARCHAR,
            _cdf_row_key BIGINT NOT NULL
        )",
    )
    .unwrap();
    drop(conn);
    let error = plan_current_package_commit(
        &destination(&wrong_type_path),
        &request(
            &package,
            package_hash,
            WriteDisposition::Append,
            Vec::new(),
            1,
        ),
    )
    .unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("_cdf_row_key has type BIGINT"),
        "{message}"
    );
    assert!(message.contains("expected UBIGINT"), "{message}");
}

#[test]
fn user_columns_cannot_impersonate_reserved_provenance() {
    let temp = tempfile::tempdir().unwrap();
    let package = temp.path().join("pkg-reserved");
    let schema = Arc::new(Schema::new(vec![Field::new(
        CDF_ROW_KEY_COLUMN,
        DataType::UInt64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(UInt64Array::from(vec![1])) as ArrayRef],
    )
    .unwrap();
    let package_hash = build_package(&package, "pkg-reserved", &[batch]);
    let db_path = temp.path().join("reserved.duckdb");
    let dest = destination(&db_path);
    let error = try_commit_current(
        &dest,
        request(
            &package,
            package_hash,
            WriteDisposition::Append,
            Vec::new(),
            1,
        ),
    )
    .unwrap_err();
    let message = error.to_string();
    assert!(message.contains("reserved `_cdf_*` namespace"), "{message}");
    assert!(!db_path.exists());
}

#[test]
fn addressed_correction_adds_nullable_column_preserves_residuals_and_replays_as_noop() {
    let temp = tempfile::tempdir().unwrap();
    let package = temp.path().join("pkg-residual");
    let original_hash = build_package(&package, "pkg-residual", &[residual_batch()]);
    let db_path = temp.path().join("correction.duckdb");
    let dest = destination(&db_path);
    commit_current(
        &dest,
        request(
            &package,
            original_hash.clone(),
            WriteDisposition::Append,
            Vec::new(),
            2,
        ),
    );

    let conn = Connection::open(&db_path).unwrap();
    let readback: (String, String, String, u64) = conn
        .query_row(
            "SELECT o._cdf_variant, p.package_hash, p.segment_id, o._cdf_row_key - p.row_key_start FROM orders o JOIN _cdf_segments p ON o._cdf_row_key >= p.row_key_start AND o._cdf_row_key < p.row_key_end WHERE o.id = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    let decoded = cdf_contract::decode_residual_json_v1(readback.0.as_bytes()).unwrap();
    assert_eq!(
        decoded
            .iter()
            .map(|field| field.path.as_str())
            .collect::<Vec<_>>(),
        vec!["/age", "/keep"]
    );
    assert_eq!(readback.1, original_hash.to_string());
    assert_eq!(readback.2, "seg-000001");
    assert_eq!(readback.3, 0);
    drop(conn);

    let first_address = RowProvenanceAddress::new(
        original_hash.clone(),
        SegmentId::new("seg-000001").unwrap(),
        0,
    );
    let before = dest
        .read_correction_residual(&TargetName::new("orders").unwrap(), &first_address)
        .unwrap()
        .unwrap();
    assert_eq!(before.original_row, first_address);
    let before_bytes = before.residual_json_v1.unwrap();
    assert_eq!(before_bytes, readback.0.as_bytes());
    let before_fields = cdf_contract::decode_residual_json_v1(&before_bytes).unwrap();
    assert_eq!(
        before_fields
            .iter()
            .map(|field| field.path.as_str())
            .collect::<Vec<_>>(),
        vec!["/age", "/keep"]
    );

    let correction = correction_request(vec![
        correction_operation(&original_hash, 0, 42),
        correction_operation(&original_hash, 1, 84),
    ]);
    let receipt = finalize_correction(&dest, &correction);
    assert_eq!(receipt.counts.rows_written, 2);
    assert_eq!(receipt.counts.rows_updated, Some(2));
    assert!(dest.verify_correction(&receipt).unwrap().verified);
    let reopened = destination(&db_path);
    assert!(reopened.verify_correction(&receipt).unwrap().verified);
    let evidence = DestinationCorrectionReceiptEvidence::from_receipt(&receipt).unwrap();
    assert_eq!(evidence.addressed_rows, 2);
    assert_eq!(evidence.residual_paths_removed, 2);

    let conn = Connection::open(&db_path).unwrap();
    let rows: Vec<(i64, i64, Option<String>, String, String, u64)> = conn
        .prepare(
            "SELECT o.id, o.age, o._cdf_variant, p.package_hash, p.segment_id, o._cdf_row_key - p.row_key_start FROM orders o JOIN _cdf_segments p ON o._cdf_row_key >= p.row_key_start AND o._cdf_row_key < p.row_key_end ORDER BY o.id",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ))
        })
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[0].1, 42);
    let retained = rows[0].2.as_ref().unwrap();
    let decoded = cdf_contract::decode_residual_json_v1(retained.as_bytes()).unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].path, "/keep");
    assert_eq!(rows[1].0, 2);
    assert_eq!(rows[1].1, 84);
    assert_eq!(rows[1].2, None);
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row.3, original_hash.to_string());
        assert_eq!(row.4, "seg-000001");
        assert_eq!(row.5, index as u64);
    }
    drop(conn);

    let after = reopened
        .read_correction_residual(&TargetName::new("orders").unwrap(), &first_address)
        .unwrap()
        .unwrap();
    assert_eq!(after.original_row, first_address);
    let after_bytes = after.residual_json_v1.unwrap();
    assert_eq!(after_bytes, retained.as_bytes());
    let after_fields = cdf_contract::decode_residual_json_v1(&after_bytes).unwrap();
    assert_eq!(after_fields.len(), 1);
    assert_eq!(after_fields[0].path, "/keep");
    let second_address = RowProvenanceAddress::new(
        original_hash.clone(),
        SegmentId::new("seg-000001").unwrap(),
        1,
    );
    let second_after = reopened
        .read_correction_residual(&TargetName::new("orders").unwrap(), &second_address)
        .unwrap()
        .unwrap();
    assert_eq!(second_after.original_row, second_address);
    assert_eq!(second_after.residual_json_v1, None);

    let replay = finalize_correction(&reopened, &correction);
    assert_eq!(replay, receipt);
    let conn = Connection::open(&db_path).unwrap();
    let loads: u64 = conn
        .query_row("SELECT count(*) FROM _cdf_loads", [], |row| row.get(0))
        .unwrap();
    assert_eq!(loads, 2);
    let ages: Vec<i64> = conn
        .prepare("SELECT age FROM orders ORDER BY id")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    assert_eq!(ages, vec![42, 84]);
}

#[test]
fn correction_failure_after_planning_rolls_back_nullable_migration_and_all_updates() {
    let temp = tempfile::tempdir().unwrap();
    let package = temp.path().join("pkg-rollback");
    let original_hash = build_package(&package, "pkg-rollback", &[residual_batch()]);
    let db_path = temp.path().join("rollback.duckdb");
    let dest = destination(&db_path);
    commit_current(
        &dest,
        request(
            &package,
            original_hash.clone(),
            WriteDisposition::Append,
            Vec::new(),
            2,
        ),
    );
    let correction = correction_request(vec![
        correction_operation(&original_hash, 0, 42),
        correction_operation(&original_hash, 1, 84),
    ]);
    let plan = dest.plan_correction(&correction).unwrap();

    let conn = Connection::open(&db_path).unwrap();
    let first_residual: String = conn
        .query_row("SELECT _cdf_variant FROM orders WHERE id = 1", [], |row| {
            row.get(0)
        })
        .unwrap();
    conn.execute("UPDATE orders SET _cdf_variant = NULL WHERE id = 2", [])
        .unwrap();
    drop(conn);

    let mut session = dest.begin_correction(correction, plan).unwrap();
    session.apply_migrations().unwrap();
    session.apply_corrections().unwrap();
    let error = session.finalize().unwrap_err();
    assert!(
        error.to_string().contains("missing or has no residual"),
        "{error}"
    );

    let conn = Connection::open(&db_path).unwrap();
    let age_columns: u64 = conn
        .query_row(
            "SELECT count(*) FROM information_schema.columns WHERE table_name = 'orders' AND column_name = 'age'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(age_columns, 0);
    let actual_first: String = conn
        .query_row("SELECT _cdf_variant FROM orders WHERE id = 1", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(actual_first, first_residual);
    let correction_loads: u64 = conn
        .query_row(
            "SELECT count(*) FROM _cdf_loads WHERE package_hash = 'sha256:correction-age'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(correction_loads, 0);
}

#[test]
fn correction_session_abort_before_finalize_is_a_noop() {
    let temp = tempfile::tempdir().unwrap();
    let package = temp.path().join("pkg-abort");
    let original_hash = build_package(&package, "pkg-abort", &[residual_batch()]);
    let db_path = temp.path().join("abort.duckdb");
    let dest = destination(&db_path);
    commit_current(
        &dest,
        request(
            &package,
            original_hash.clone(),
            WriteDisposition::Append,
            Vec::new(),
            2,
        ),
    );
    let correction = correction_request(vec![correction_operation(&original_hash, 0, 42)]);
    let plan = dest.plan_correction(&correction).unwrap();
    let mut session = dest.begin_correction(correction, plan).unwrap();
    session.apply_migrations().unwrap();
    session.apply_corrections().unwrap();
    session.abort().unwrap();

    let conn = Connection::open(db_path).unwrap();
    let age_columns: u64 = conn
        .query_row(
            "SELECT count(*) FROM information_schema.columns WHERE table_name = 'orders' AND column_name = 'age'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(age_columns, 0);
    let residual: String = conn
        .query_row("SELECT _cdf_variant FROM orders WHERE id = 1", [], |row| {
            row.get(0)
        })
        .unwrap();
    let paths = cdf_contract::decode_residual_json_v1(residual.as_bytes())
        .unwrap()
        .into_iter()
        .map(|field| field.path)
        .collect::<Vec<_>>();
    assert_eq!(paths, vec!["/age", "/keep"]);
    let loads: u64 = conn
        .query_row("SELECT count(*) FROM _cdf_loads", [], |row| row.get(0))
        .unwrap();
    assert_eq!(loads, 1);
}

#[test]
fn correction_dry_plan_uses_read_only_database_without_wal_or_byte_changes() {
    let temp = tempfile::tempdir().unwrap();
    let package = temp.path().join("pkg-dry-correction");
    let original_hash = build_package(&package, "pkg-dry-correction", &[residual_batch()]);
    let db_path = temp.path().join("dry-correction.duckdb");
    let dest = destination(&db_path);
    commit_current(
        &dest,
        request(
            &package,
            original_hash.clone(),
            WriteDisposition::Append,
            Vec::new(),
            2,
        ),
    );
    let correction = correction_request(vec![correction_operation(&original_hash, 0, 42)]);
    let bytes_before = fs::read(&db_path).unwrap();
    let files_before = fs::read_dir(temp.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<BTreeSet<_>>();

    let plan = dest.plan_correction(&correction).unwrap();

    assert_eq!(plan.correction_count, 1);
    assert_eq!(fs::read(&db_path).unwrap(), bytes_before);
    let files_after = fs::read_dir(temp.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<BTreeSet<_>>();
    assert_eq!(files_after, files_before);
}

#[test]
fn correction_missing_address_fails_dry_plan_without_migration_or_receipt() {
    let temp = tempfile::tempdir().unwrap();
    let package = temp.path().join("pkg-missing-address");
    let original_hash = build_package(&package, "pkg-missing-address", &[residual_batch()]);
    let db_path = temp.path().join("missing-address.duckdb");
    let dest = destination(&db_path);
    commit_current(
        &dest,
        request(
            &package,
            original_hash.clone(),
            WriteDisposition::Append,
            Vec::new(),
            2,
        ),
    );
    let correction = correction_request(vec![correction_operation(&original_hash, 99, 42)]);

    let error = dest.plan_correction(&correction).unwrap_err();
    assert!(
        error.to_string().contains("outside its segment range"),
        "{error}"
    );
    let conn = Connection::open(db_path).unwrap();
    let age_columns: u64 = conn
        .query_row(
            "SELECT count(*) FROM information_schema.columns WHERE table_name = 'orders' AND column_name = 'age'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(age_columns, 0);
    let loads: u64 = conn
        .query_row("SELECT count(*) FROM _cdf_loads", [], |row| row.get(0))
        .unwrap();
    assert_eq!(loads, 1);
}

#[test]
fn merge_rejects_conflicting_duplicate_keys() {
    let temp = tempfile::tempdir().unwrap();
    let package = temp.path().join("pkg-conflict");
    let package_hash = build_package_for_commit(
        &package,
        "pkg-conflict",
        &[sample_batch(vec![1, 1], vec![Some("left"), Some("right")])],
        WriteDisposition::Merge,
        vec!["id".to_owned()],
    );
    let dest = destination(&temp.path().join("local.duckdb"));
    let error = try_commit_current(
        &dest,
        request(
            &package,
            package_hash,
            WriteDisposition::Merge,
            vec!["id".to_owned()],
            2,
        ),
    )
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
    let plan = plan_current_package_commit(
        &dest,
        &request(
            &package,
            package_hash,
            WriteDisposition::Append,
            Vec::new(),
            1,
        ),
    )
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
