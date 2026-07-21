use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use cdf_contract::{ContractPolicy, ObservedSchema, compile_validation_program};
use cdf_engine::{
    CompiledStreamAdmissionEvidence, EnginePlanInput, LineageInputObservation, LineageSummary,
    PhysicalObservationEvidence, Planner, StreamAdmissionCompletion,
    StreamAdmissionObservationEvidence,
};
use cdf_kernel::{
    CHECKPOINT_STATE_VERSION, CdfError, Checkpoint, CheckpointId, CheckpointStatus,
    CheckpointStore, CompiledScanIntent, CursorPosition, CursorValue, ExecutionExtent,
    PLAN_SCHEMA_OBSERVATION_ID_KEY, PackageHash, PartitionId, PartitionOpenAttempt, PartitionPlan,
    PartitionRetrySafety, PipelineId, ProcessedObservationOutcome, ProcessedObservationPosition,
    Receipt, ResourceDescriptor, ResourceId, ResourceStream, Result, ScanRequest, SchemaHash,
    SchemaSource, ScopeKey, SegmentId, SourcePosition, StateDelta, StateSegment, TargetName,
    TrustLevel, WriteDisposition,
};
use cdf_package_contract::{
    DestinationCommitPlanPreimage, PROCESSED_OBSERVATIONS_FILE, PackageManifest, PackageStatus,
    ProcessedObservationEvidenceArtifact, SegmentEntry, StateDeltaPreimage,
};
use cdf_project::{
    PackageArtifactRecoveryRequest, PackageArtifactReplayRequest, ReceiptVerifiedHook,
    ResolvedProjectDestination, recover_package_from_artifacts, replay_package_from_artifacts,
};

pub use cdf_dest_duckdb::{DuckDbDestination, DuckDbMirrorSnapshot};
pub use cdf_package::{PackageBuilder, PackageReader};
pub use cdf_project::{PackageReplayReport, ProjectReceiptSource};
pub use cdf_state_sqlite::SqliteCheckpointStore;

pub const DEFAULT_PREPARED_SCHEMA_HASH: &str = "schema-v1";
pub const DEFAULT_PREPARED_TARGET: &str = "orders";
pub const DEFAULT_PREPARED_SEGMENT_ID: &str = "seg-000001";
const PREPARED_PARTITION_ID: &str = "p0";
const PREPARED_OBSERVATION_ID: &str = "p0";

#[derive(Clone, Debug)]
pub struct PreparedPackageFixtureSpec {
    pub package_dir: PathBuf,
    pub package_id: String,
    pub target: TargetName,
    pub disposition: WriteDisposition,
    pub schema_hash: SchemaHash,
    pub segment_id: SegmentId,
    pub checkpoint_id: CheckpointId,
    pub status: PackageStatus,
}

impl PreparedPackageFixtureSpec {
    pub fn new(package_dir: impl AsRef<Path>, package_id: impl Into<String>) -> Result<Self> {
        Ok(Self {
            package_dir: package_dir.as_ref().to_path_buf(),
            package_id: package_id.into(),
            target: TargetName::new(DEFAULT_PREPARED_TARGET)?,
            disposition: WriteDisposition::Append,
            schema_hash: SchemaHash::new(DEFAULT_PREPARED_SCHEMA_HASH)?,
            segment_id: SegmentId::new(DEFAULT_PREPARED_SEGMENT_ID)?,
            checkpoint_id: CheckpointId::new("checkpoint-prepared-artifact")?,
            status: PackageStatus::Packaged,
        })
    }
}

#[derive(Clone, Debug)]
pub struct PreparedPackageFixture {
    pub package_dir: PathBuf,
    pub manifest: PackageManifest,
    pub target: TargetName,
    pub disposition: WriteDisposition,
    pub schema_hash: SchemaHash,
}

impl PreparedPackageFixture {
    pub fn package_hash(&self) -> Result<PackageHash> {
        PackageHash::new(self.manifest.package_hash.clone())
    }

    pub fn state_segments(
        &self,
        scope: ScopeKey,
        output_position: SourcePosition,
    ) -> Vec<StateSegment> {
        self.manifest
            .identity
            .segments
            .iter()
            .map(|segment| StateSegment {
                segment_id: segment.segment_id.clone(),
                scope: scope.clone(),
                output_position: output_position.clone(),
                row_count: segment.row_count,
                byte_count: segment.byte_count,
            })
            .collect()
    }

    pub fn replay_case(&self) -> Result<PreparedPackageReplayCase> {
        let inputs = PackageReader::open(&self.package_dir)?.replay_inputs()?;
        if inputs.destination_commit.target != self.target
            || inputs.destination_commit.disposition != self.disposition
            || inputs.schema_hash != self.schema_hash
        {
            return Err(CdfError::contract(
                "package replay fixture metadata does not match its recorded replay authority",
            ));
        }
        Ok(PreparedPackageReplayCase {
            package_dir: self.package_dir.clone(),
            delta: inputs.state_delta,
            target: self.target.clone(),
            disposition: self.disposition.clone(),
            schema_hash: self.schema_hash.clone(),
        })
    }
}

#[derive(Clone, Debug)]
pub struct PreparedPackageReplayCase {
    pub package_dir: PathBuf,
    pub delta: StateDelta,
    pub target: TargetName,
    pub disposition: WriteDisposition,
    pub schema_hash: SchemaHash,
}

impl PreparedPackageReplayCase {
    pub fn replay_request<'a, Store>(
        &'a self,
        destination: &'a DuckDbDestination,
        checkpoint_store: &'a Store,
        after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
    ) -> PackageArtifactReplayRequest<'a, Store>
    where
        Store: CheckpointStore + ?Sized,
    {
        PackageArtifactReplayRequest {
            package_dir: self.package_dir.clone(),
            destination: resolved_duckdb_destination(destination, self.target.clone()),
            checkpoint_store,
            after_receipt_verified,
        }
    }

    pub fn recovery_request<'a, Store>(
        &'a self,
        destination: &'a DuckDbDestination,
        checkpoint_store: &'a Store,
        receipt: Receipt,
        after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
    ) -> PackageArtifactRecoveryRequest<'a, Store>
    where
        Store: CheckpointStore + ?Sized,
    {
        PackageArtifactRecoveryRequest {
            package_dir: self.package_dir.clone(),
            destination: resolved_duckdb_destination(destination, self.target.clone()),
            checkpoint_store,
            receipt,
            after_receipt_verified,
        }
    }
}

pub fn build_prepared_package_fixture(
    spec: PreparedPackageFixtureSpec,
) -> Result<PreparedPackageFixture> {
    let builder = PackageBuilder::create(
        &spec.package_dir,
        spec.package_id.clone(),
        cdf_package::PackageBuilderResources::standalone(8 * 1024 * 1024, 64 * 1024 * 1024)?,
    )?;
    builder.update_status(PackageStatus::Extracting)?;
    let batch = deterministic_orders_batch()?;
    let schema = batch.schema();
    let (admission, partition_binding) =
        write_compiled_plan_artifacts(&builder, Arc::clone(&schema), &spec.schema_hash)?;
    builder.write_runtime_arrow_schema(batch.schema().as_ref())?;
    builder.write_json_artifact(
        "schema/output.arrow.json",
        &BTreeMap::from([("schema_hash", spec.schema_hash.as_str())]),
    )?;
    let batch = cdf_package_contract::append_package_row_ord(vec![batch], 0)?;
    let segment = builder.write_segment(spec.segment_id.clone(), 0, &batch)?;
    write_stream_admission_artifacts(
        &builder,
        &admission,
        &partition_binding,
        segment.row_count,
        schema.as_ref(),
    )?;
    write_prepared_state_commit_artifacts(&builder, &spec, segment)?;
    builder.finish_with_status(spec.status)?;
    let manifest = cdf_package::read_manifest(&spec.package_dir)?;

    Ok(PreparedPackageFixture {
        package_dir: spec.package_dir,
        manifest,
        target: spec.target,
        disposition: spec.disposition,
        schema_hash: spec.schema_hash,
    })
}

fn write_compiled_plan_artifacts(
    builder: &PackageBuilder,
    schema: Arc<Schema>,
    schema_hash: &SchemaHash,
) -> Result<(
    cdf_engine::CompiledSchemaAdmissionPlan,
    cdf_kernel::SchemaObservationBinding,
)> {
    let mut program = compile_validation_program(
        &ContractPolicy::evolve(),
        &ObservedSchema::from_arrow(schema.as_ref()),
    )?;
    program.row_rules.clear();
    program.transforms.clear();

    let resource = PreparedFixtureResource::new(Arc::clone(&schema), schema_hash.clone())?;
    let plan = Planner::new().plan_tier_a(
        &resource,
        EnginePlanInput {
            request: ScanRequest {
                resource_id: ResourceId::new("orders")?,
                projection: None,
                filters: Vec::new(),
                limit: None,
                order_by: Vec::new(),
                scope: ScopeKey::Resource,
            },
            validation_program: program,
            execution_extent: ExecutionExtent::bounded(),
            segmentation: cdf_engine::CanonicalSegmentationPolicy::performance_default(),
            package_id: "conformance-prepared-package".to_owned(),
        },
    )?;
    builder.write_json_artifact("plan/validation-program.json", &plan.validation_program)?;
    builder.write_json_artifact("plan/scan.json", &plan.scan)?;
    builder.write_json_artifact(
        "plan/schema-admission.json",
        &plan.compiled_schema_admission,
    )?;
    let partition_binding = cdf_kernel::partition_schema_observation_binding(
        plan.scan
            .inline_partitions()
            .ok_or_else(|| CdfError::internal("prepared replay plan is not inline"))?
            .first()
            .ok_or_else(|| CdfError::internal("prepared replay plan omitted its partition"))?,
    )?;
    Ok((plan.compiled_schema_admission, partition_binding))
}

fn write_stream_admission_artifacts(
    builder: &PackageBuilder,
    admission: &cdf_engine::CompiledSchemaAdmissionPlan,
    partition_binding: &cdf_kernel::SchemaObservationBinding,
    row_count: u64,
    schema: &Schema,
) -> Result<()> {
    let physical = PhysicalObservationEvidence::arrow_schema(schema)?;
    let physical_hash = physical.identity_hash()?;
    let coercion = admission.instantiate(schema, &physical_hash)?;
    let output_position = prepared_output_position();
    let observation = StreamAdmissionObservationEvidence::new(
        PREPARED_OBSERVATION_ID,
        physical_hash.clone(),
        coercion,
        StreamAdmissionCompletion::Complete {
            source_position: output_position.clone(),
            partition_binding: partition_binding.clone(),
        },
    )?;
    let stream_evidence = CompiledStreamAdmissionEvidence::new(
        admission,
        BTreeMap::from([(physical_hash.to_string(), physical)]),
        vec![observation],
    )?;
    builder.write_json_artifact("schema/stream-admission-evidence.json", &stream_evidence)?;
    let partition_id = prepared_partition_id()?;
    let lineage = LineageSummary {
        input_rows: row_count,
        input_observations: vec![LineageInputObservation {
            observation_id: PREPARED_OBSERVATION_ID.to_owned(),
            partition_id,
            partition_binding: partition_binding.clone(),
            observed_rows: row_count,
            output_position: Some(output_position),
        }],
    };
    builder.write_lineage_artifact(
        "lineage.json",
        &cdf_package::canonical_json_bytes(&lineage)?,
    )?;
    Ok(())
}

struct PreparedFixtureResource {
    descriptor: ResourceDescriptor,
    schema: Arc<Schema>,
}

impl PreparedFixtureResource {
    fn new(schema: Arc<Schema>, schema_hash: SchemaHash) -> Result<Self> {
        Ok(Self {
            descriptor: ResourceDescriptor {
                resource_id: ResourceId::new("orders")?,
                schema_source: SchemaSource::Declared {
                    schema_hash,
                    source: "prepared-package-fixture".to_owned(),
                },
                primary_key: Vec::new(),
                merge_key: Vec::new(),
                cursor: None,
                write_disposition: WriteDisposition::Append,
                deduplication: None,
                contract: None,
                state_scope: ScopeKey::Resource,
                freshness: None,
                trust_level: TrustLevel::Experimental,
            },
            schema,
        })
    }
}

impl ResourceStream for PreparedFixtureResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn plan_partitions(&self, _request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        let partition_id = prepared_partition_id()?;
        Ok(vec![PartitionPlan {
            partition_id: partition_id.clone(),
            scope: ScopeKey::Partition { partition_id },
            planned_position: None,
            start_position: None,
            scan_intent: CompiledScanIntent::full_scan(),
            retry_safety: PartitionRetrySafety::Forbidden,
            metadata: BTreeMap::from([(
                PLAN_SCHEMA_OBSERVATION_ID_KEY.to_owned(),
                PREPARED_OBSERVATION_ID.to_owned(),
            )]),
        }])
    }

    fn open(&self, _partition: PartitionPlan) -> PartitionOpenAttempt<'_> {
        PartitionOpenAttempt::materialized(Box::pin(async {
            Err(CdfError::internal(
                "prepared replay fixture has no source payload",
            ))
        }))
    }
}

pub fn replay_package_case<Store>(
    case: &PreparedPackageReplayCase,
    destination: &DuckDbDestination,
    checkpoint_store: &Store,
) -> Result<PackageReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    replay_package_from_artifacts(case.replay_request(destination, checkpoint_store, None))
}

pub fn replay_package_artifacts<Store>(
    package_dir: impl AsRef<Path>,
    destination: &DuckDbDestination,
    checkpoint_store: &Store,
) -> Result<PackageReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    let reader = PackageReader::open(package_dir.as_ref())?;
    let target = reader.replay_inputs()?.destination_commit.target;
    replay_package_from_artifacts(PackageArtifactReplayRequest {
        package_dir: package_dir.as_ref().to_path_buf(),
        destination: resolved_duckdb_destination(destination, target),
        checkpoint_store,
        after_receipt_verified: None,
    })
}

pub fn recover_package_case<Store>(
    case: &PreparedPackageReplayCase,
    destination: &DuckDbDestination,
    checkpoint_store: &Store,
    receipt: Receipt,
) -> Result<PackageReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    recover_package_from_artifacts(case.recovery_request(
        destination,
        checkpoint_store,
        receipt,
        None,
    ))
}

pub fn recover_package_artifacts<Store>(
    package_dir: impl AsRef<Path>,
    destination: &DuckDbDestination,
    checkpoint_store: &Store,
    receipt: Receipt,
) -> Result<PackageReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    let reader = PackageReader::open(package_dir.as_ref())?;
    let target = reader.replay_inputs()?.destination_commit.target;
    recover_package_from_artifacts(PackageArtifactRecoveryRequest {
        package_dir: package_dir.as_ref().to_path_buf(),
        checkpoint_store,
        destination: resolved_duckdb_destination(destination, target),
        receipt,
        after_receipt_verified: None,
    })
}

fn resolved_duckdb_destination(
    destination: &DuckDbDestination,
    target: TargetName,
) -> ResolvedProjectDestination {
    ResolvedProjectDestination::new(Box::new(destination.clone()), target)
        .with_bound_execution_services(crate::test_execution_services())
        .expect("bind conformance execution services to DuckDB replay destination")
}

pub fn assert_packaged_replay_committed_without_source_contact<Store>(
    case: &PreparedPackageReplayCase,
    destination: &DuckDbDestination,
    checkpoint_store: &Store,
    report: &PackageReplayReport,
) where
    Store: CheckpointStore + ?Sized,
{
    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(
        PackageReader::open(&case.package_dir)
            .unwrap()
            .manifest()
            .lifecycle
            .status,
        PackageStatus::Checkpointed
    );
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(report.checkpoint.delta, case.delta);
    assert_checkpoint_head_matches(checkpoint_store, &case.delta);
    assert_receipt_matches_case(case, &report.receipt);
    assert!(
        destination
            .verify_receipt(&report.receipt)
            .unwrap()
            .verified,
        "destination receipt must verify before checkpoint commit"
    );
    assert_package_receipt_durable(&case.package_dir, &report.receipt);
    assert_duckdb_mirror_matches_receipt(
        &destination.read_mirror_snapshot_read_only().unwrap(),
        case,
        &report.receipt,
    );
}

pub fn assert_duplicate_replay_identity(
    case: &PreparedPackageReplayCase,
    report: &PackageReplayReport,
    original_receipt: &Receipt,
    snapshot: &DuckDbMirrorSnapshot,
) {
    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::DestinationCommit {
            duplicate: true,
            package_receipt_recorded: false
        },
        "duplicate replay must surface duplicate/no-op receipt behavior"
    );
    assert_eq!(
        &report.receipt, original_receipt,
        "duplicate replay must return the original durable receipt"
    );
    assert_eq!(
        snapshot.loads.len(),
        1,
        "duplicate replay must leave exactly one destination load mirror entry"
    );
    assert_duckdb_mirror_matches_receipt(snapshot, case, original_receipt);
}

pub fn assert_recovery_committed_from_durable_receipt<Store>(
    case: &PreparedPackageReplayCase,
    checkpoint_store: &Store,
    report: &PackageReplayReport,
    durable_receipt: &Receipt,
    snapshot_before: &DuckDbMirrorSnapshot,
    snapshot_after: &DuckDbMirrorSnapshot,
) where
    Store: CheckpointStore + ?Sized,
{
    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::SuppliedDurableReceipt
    );
    assert_eq!(&report.receipt, durable_receipt);
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(
        report.checkpoint.delta.output_position,
        case.delta.output_position
    );
    assert_eq!(
        report.checkpoint.delta.package_hash,
        durable_receipt.package_hash
    );
    assert_eq!(
        report.checkpoint.delta.schema_hash,
        durable_receipt.schema_hash
    );
    assert_checkpoint_head_matches(checkpoint_store, &case.delta);
    assert_no_second_destination_write(snapshot_before, snapshot_after);
    assert_duckdb_mirror_matches_receipt(snapshot_after, case, durable_receipt);
}

pub fn assert_no_checkpoint_head<Store>(store: &Store, delta: &StateDelta)
where
    Store: CheckpointStore + ?Sized,
{
    assert!(
        store
            .head(&delta.pipeline_id, &delta.resource_id, &delta.scope)
            .unwrap()
            .is_none(),
        "checkpoint store must not expose an uncommitted checkpoint as head"
    );
}

pub fn assert_checkpoint_head_matches<Store>(store: &Store, delta: &StateDelta) -> Checkpoint
where
    Store: CheckpointStore + ?Sized,
{
    let head = store
        .head(&delta.pipeline_id, &delta.resource_id, &delta.scope)
        .unwrap()
        .expect("checkpoint head must exist");
    assert_eq!(head.status, CheckpointStatus::Committed);
    assert!(head.is_head);
    assert_eq!(head.delta, *delta);
    head
}

pub fn assert_package_receipt_durable(package_dir: impl AsRef<Path>, receipt: &Receipt) {
    let reader = PackageReader::open(package_dir).unwrap();
    let mut receipts = Vec::new();
    reader
        .for_each_receipt(&mut |receipt| {
            receipts.push(receipt);
            Ok(())
        })
        .unwrap();
    assert!(
        receipts.iter().any(|stored| stored == receipt),
        "package receipts must durably contain receipt {}",
        receipt.receipt_id
    );
}

pub fn assert_no_second_destination_write(
    snapshot_before: &DuckDbMirrorSnapshot,
    snapshot_after: &DuckDbMirrorSnapshot,
) {
    assert_eq!(
        snapshot_after, snapshot_before,
        "receipt recovery must not issue a second DuckDB load"
    );
}

pub fn assert_no_duckdb_destination_write(snapshot: &DuckDbMirrorSnapshot) {
    assert!(
        !snapshot.loads_table_present,
        "_cdf_loads must not exist before destination write"
    );
    assert!(
        !snapshot.state_table_present,
        "_cdf_state must not exist before destination write"
    );
    assert!(
        snapshot.loads.is_empty(),
        "_cdf_loads must contain no rows before destination write"
    );
    assert!(
        snapshot.state.is_empty(),
        "_cdf_state must contain no rows before destination write"
    );
}

pub fn assert_duckdb_mirror_matches_receipt(
    snapshot: &DuckDbMirrorSnapshot,
    case: &PreparedPackageReplayCase,
    receipt: &Receipt,
) {
    assert!(snapshot.loads_table_present, "_cdf_loads must exist");
    assert!(snapshot.state_table_present, "_cdf_state must exist");
    assert_receipt_matches_case(case, receipt);

    let load = snapshot
        .loads
        .iter()
        .find(|row| {
            row.target == case.target.as_str()
                && row.idempotency_token == receipt.idempotency_token.as_str()
                && row.package_hash == receipt.package_hash.as_str()
        })
        .expect("DuckDB _cdf_loads must contain the durable receipt mirror");
    assert_eq!(load.receipt_id, receipt.receipt_id.as_str());
    let mirrored_receipt: Receipt = serde_json::from_str(&load.receipt_json).unwrap();
    assert_eq!(&mirrored_receipt, receipt);

    let state_rows = snapshot
        .state
        .iter()
        .filter(|row| {
            row.target == case.target.as_str() && row.package_hash == receipt.package_hash.as_str()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        state_rows.len(),
        case.delta.segments.len(),
        "DuckDB _cdf_state must mirror every replayed segment exactly once"
    );

    let rows_by_segment = state_rows
        .iter()
        .map(|row| (row.segment_id.as_str(), *row))
        .collect::<BTreeMap<_, _>>();
    let receipt_acks = receipt
        .segment_acks
        .iter()
        .map(|ack| (ack.segment_id.as_str(), ack))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        rows_by_segment.keys().copied().collect::<BTreeSet<_>>(),
        case.delta
            .segments
            .iter()
            .map(|segment| segment.segment_id.as_str())
            .collect::<BTreeSet<_>>()
    );
    assert_eq!(
        receipt_acks.keys().copied().collect::<BTreeSet<_>>(),
        rows_by_segment.keys().copied().collect::<BTreeSet<_>>()
    );

    for segment in &case.delta.segments {
        let row = rows_by_segment[segment.segment_id.as_str()];
        let ack = receipt_acks[segment.segment_id.as_str()];
        assert_eq!(ack.row_count, segment.row_count);
        assert_eq!(ack.byte_count, segment.byte_count);
        assert_eq!(row.row_count, segment.row_count);
        assert_eq!(row.byte_count, segment.byte_count);
        let mirrored_scope: ScopeKey =
            serde_json::from_str(row.scope_json.as_deref().expect("scope mirror")).unwrap();
        let mirrored_position: SourcePosition = serde_json::from_str(
            row.output_position_json
                .as_deref()
                .expect("output position mirror"),
        )
        .unwrap();
        assert_eq!(mirrored_scope, segment.scope);
        assert_eq!(mirrored_position, segment.output_position);
        assert_eq!(mirrored_position, case.delta.output_position);
    }
}

fn assert_receipt_matches_case(case: &PreparedPackageReplayCase, receipt: &Receipt) {
    assert_eq!(receipt.package_hash, case.delta.package_hash);
    assert_eq!(receipt.schema_hash, case.delta.schema_hash);
    assert_eq!(receipt.schema_hash, case.schema_hash);
    assert_eq!(receipt.target, case.target);
    assert_eq!(receipt.disposition, case.disposition);
    assert_eq!(
        receipt.idempotency_token.as_str(),
        case.delta.package_hash.as_str()
    );

    let reader = PackageReader::open(&case.package_dir).unwrap();
    let mut package_segments = Vec::new();
    reader
        .for_each_identity_segment(&mut |segment| {
            package_segments.push(segment);
            Ok(())
        })
        .unwrap();
    assert_segments_match(&package_segments, &case.delta.segments);

    let ack_by_segment = receipt
        .segment_acks
        .iter()
        .map(|ack| (ack.segment_id.as_str(), ack))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(ack_by_segment.len(), receipt.segment_acks.len());
    assert_eq!(ack_by_segment.len(), case.delta.segments.len());
    for segment in &case.delta.segments {
        let ack = ack_by_segment
            .get(segment.segment_id.as_str())
            .expect("receipt ack for segment");
        assert_eq!(ack.row_count, segment.row_count);
        assert_eq!(ack.byte_count, segment.byte_count);
    }
}

fn assert_segments_match(package_segments: &[SegmentEntry], state_segments: &[StateSegment]) {
    let package_by_segment = package_segments
        .iter()
        .map(|segment| (segment.segment_id.as_str(), segment))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(package_by_segment.len(), package_segments.len());
    assert_eq!(package_by_segment.len(), state_segments.len());

    for segment in state_segments {
        let package_segment = package_by_segment
            .get(segment.segment_id.as_str())
            .expect("state segment must be present in package manifest");
        assert_eq!(package_segment.row_count, segment.row_count);
        assert_eq!(package_segment.byte_count, segment.byte_count);
    }
}

fn deterministic_orders_batch() -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let id: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let name: ArrayRef = Arc::new(StringArray::from(vec![Some("ada"), Some("grace"), None]));
    RecordBatch::try_new(schema, vec![id, name]).map_err(|error| CdfError::data(error.to_string()))
}

fn write_prepared_state_commit_artifacts(
    builder: &PackageBuilder,
    spec: &PreparedPackageFixtureSpec,
    segment: SegmentEntry,
) -> Result<()> {
    let scope = ScopeKey::Partition {
        partition_id: prepared_partition_id()?,
    };
    let output_position = prepared_output_position();
    let segments = vec![StateSegment {
        segment_id: segment.segment_id,
        scope: scope.clone(),
        output_position: output_position.clone(),
        row_count: segment.row_count,
        byte_count: segment.byte_count,
    }];
    let state_delta = StateDeltaPreimage {
        checkpoint_id: spec.checkpoint_id.clone(),
        pipeline_id: PipelineId::new("pipeline-1")?,
        resource_id: ResourceId::new("orders")?,
        scope,
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: None,
        input_position: None,
        output_position: output_position.clone(),
        output_watermark: None,
        partition_watermarks: Vec::new(),
        late_data_carryover: Vec::new(),
        source_continuation: None,
        schema_hash: spec.schema_hash.clone(),
        segments,
    };
    let processed = ProcessedObservationPosition::new(
        PREPARED_OBSERVATION_ID,
        ProcessedObservationOutcome::Admitted,
        output_position.clone(),
    )?;
    builder.write_json_artifact(
        PROCESSED_OBSERVATIONS_FILE,
        &ProcessedObservationEvidenceArtifact::new(
            None,
            spec.disposition.clone(),
            vec![processed],
            output_position.clone(),
        )?,
    )?;
    let commit_plan = DestinationCommitPlanPreimage::package_hash_token(
        spec.target.clone(),
        spec.disposition.clone(),
        Vec::new(),
        spec.schema_hash.clone(),
    );
    builder.write_input_checkpoint_artifact(&None)?;
    builder.write_state_delta_preimage_artifact(&state_delta)?;
    builder.write_commit_plan_preimage_artifact(&commit_plan)?;
    Ok(())
}

fn prepared_partition_id() -> Result<PartitionId> {
    PartitionId::new(PREPARED_PARTITION_ID)
}

fn prepared_output_position() -> SourcePosition {
    SourcePosition::Cursor(CursorPosition {
        version: CHECKPOINT_STATE_VERSION,
        field: "id".to_owned(),
        value: CursorValue::I64(3),
    })
}

#[cfg(test)]
mod tests;
