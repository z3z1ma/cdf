use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use cdf_kernel::{
    CHECKPOINT_STATE_VERSION, CdfError, Checkpoint, CheckpointId, CheckpointStatus,
    CheckpointStore, CursorPosition, CursorValue, PackageHash, PartitionId, PipelineId, Receipt,
    ResourceId, Result, SchemaHash, ScopeKey, SegmentId, SourcePosition, StateDelta, StateSegment,
    TargetName, WriteDisposition,
};
use cdf_package::{
    DestinationCommitPlanPreimage, PackageManifest, PackageStatus, SegmentEntry, StateDeltaPreimage,
};
use cdf_project::{
    PackageArtifactDuckDbRecoveryRequest, PackageArtifactDuckDbReplayRequest,
    PreparedDuckDbRecoveryRequest, PreparedDuckDbReplayRequest, ReceiptVerifiedHook,
    recover_duckdb_package_from_artifacts, recover_prepared_duckdb_package,
    replay_duckdb_package_from_artifacts, replay_prepared_duckdb_package,
};

pub use cdf_dest_duckdb::{DuckDbDestination, DuckDbMirrorSnapshot};
pub use cdf_package::{PackageBuilder, PackageReader};
pub use cdf_project::{PreparedDuckDbReplayReport, PreparedReceiptSource};
pub use cdf_state_sqlite::SqliteCheckpointStore;

pub const DEFAULT_PREPARED_SCHEMA_HASH: &str = "schema-v1";
pub const DEFAULT_PREPARED_TARGET: &str = "orders";
pub const DEFAULT_PREPARED_SEGMENT_ID: &str = "seg-000001";

#[derive(Clone, Debug)]
pub struct PreparedPackageFixtureSpec {
    pub package_dir: PathBuf,
    pub package_id: String,
    pub target: TargetName,
    pub disposition: WriteDisposition,
    pub schema_hash: SchemaHash,
    pub segment_id: SegmentId,
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

    pub fn replay_case(&self, delta: StateDelta) -> PreparedPackageReplayCase {
        PreparedPackageReplayCase {
            package_dir: self.package_dir.clone(),
            delta,
            target: self.target.clone(),
            disposition: self.disposition.clone(),
            merge_keys: Vec::new(),
            schema_hash: self.schema_hash.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PreparedPackageReplayCase {
    pub package_dir: PathBuf,
    pub delta: StateDelta,
    pub target: TargetName,
    pub disposition: WriteDisposition,
    pub merge_keys: Vec<String>,
    pub schema_hash: SchemaHash,
}

impl PreparedPackageReplayCase {
    pub fn replay_request<'a, Store>(
        &'a self,
        destination: &'a DuckDbDestination,
        checkpoint_store: &'a Store,
        after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
    ) -> PreparedDuckDbReplayRequest<'a, Store>
    where
        Store: CheckpointStore + ?Sized,
    {
        PreparedDuckDbReplayRequest {
            package_dir: self.package_dir.clone(),
            destination,
            checkpoint_store,
            delta: self.delta.clone(),
            target: self.target.clone(),
            disposition: self.disposition.clone(),
            merge_keys: self.merge_keys.clone(),
            schema_hash: self.schema_hash.clone(),
            after_receipt_verified,
        }
    }

    pub fn recovery_request<'a, Store>(
        &'a self,
        destination: &'a DuckDbDestination,
        checkpoint_store: &'a Store,
        receipt: Receipt,
        after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
    ) -> PreparedDuckDbRecoveryRequest<'a, Store>
    where
        Store: CheckpointStore + ?Sized,
    {
        PreparedDuckDbRecoveryRequest {
            package_dir: self.package_dir.clone(),
            destination,
            checkpoint_store,
            delta: self.delta.clone(),
            target: self.target.clone(),
            disposition: self.disposition.clone(),
            schema_hash: self.schema_hash.clone(),
            receipt,
            after_receipt_verified,
        }
    }
}

pub fn build_prepared_package_fixture(
    spec: PreparedPackageFixtureSpec,
) -> Result<PreparedPackageFixture> {
    let mut builder = PackageBuilder::create(&spec.package_dir, spec.package_id.clone())?;
    builder.update_status(PackageStatus::Extracting)?;
    builder.write_json_artifact(
        "schema/output.arrow.json",
        &BTreeMap::from([("schema_hash", spec.schema_hash.as_str())]),
    )?;
    let segment =
        builder.write_segment(spec.segment_id.clone(), &[deterministic_orders_batch()?])?;
    write_prepared_state_commit_artifacts(&builder, &spec, segment)?;
    let manifest = builder.finish_with_status(spec.status)?;

    Ok(PreparedPackageFixture {
        package_dir: spec.package_dir,
        manifest,
        target: spec.target,
        disposition: spec.disposition,
        schema_hash: spec.schema_hash,
    })
}

pub fn replay_prepared_package_case<Store>(
    case: &PreparedPackageReplayCase,
    destination: &DuckDbDestination,
    checkpoint_store: &Store,
) -> Result<PreparedDuckDbReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    replay_prepared_duckdb_package(case.replay_request(destination, checkpoint_store, None))
}

pub fn replay_package_artifacts<Store>(
    package_dir: impl AsRef<Path>,
    destination: &DuckDbDestination,
    checkpoint_store: &Store,
) -> Result<PreparedDuckDbReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    replay_duckdb_package_from_artifacts(PackageArtifactDuckDbReplayRequest {
        package_dir: package_dir.as_ref().to_path_buf(),
        destination,
        checkpoint_store,
        after_receipt_verified: None,
    })
}

pub fn recover_prepared_package_case<Store>(
    case: &PreparedPackageReplayCase,
    destination: &DuckDbDestination,
    checkpoint_store: &Store,
    receipt: Receipt,
) -> Result<PreparedDuckDbReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    recover_prepared_duckdb_package(case.recovery_request(
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
) -> Result<PreparedDuckDbReplayReport>
where
    Store: CheckpointStore + ?Sized,
{
    recover_duckdb_package_from_artifacts(PackageArtifactDuckDbRecoveryRequest {
        package_dir: package_dir.as_ref().to_path_buf(),
        destination,
        checkpoint_store,
        receipt,
        after_receipt_verified: None,
    })
}

pub fn assert_packaged_replay_committed_without_source_contact<Store>(
    case: &PreparedPackageReplayCase,
    destination: &DuckDbDestination,
    checkpoint_store: &Store,
    report: &PreparedDuckDbReplayReport,
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
    report: &PreparedDuckDbReplayReport,
    original_receipt: &Receipt,
    snapshot: &DuckDbMirrorSnapshot,
) {
    assert_eq!(
        report.receipt_source,
        PreparedReceiptSource::DuckDbCommit {
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
    report: &PreparedDuckDbReplayReport,
    durable_receipt: &Receipt,
    snapshot_before: &DuckDbMirrorSnapshot,
    snapshot_after: &DuckDbMirrorSnapshot,
) where
    Store: CheckpointStore + ?Sized,
{
    assert_eq!(
        report.receipt_source,
        PreparedReceiptSource::SuppliedDurableReceipt
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
    let receipts = PackageReader::open(package_dir)
        .unwrap()
        .receipts()
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

    let package_segments = PackageReader::open(&case.package_dir)
        .unwrap()
        .manifest()
        .identity
        .segments
        .clone();
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
        partition_id: PartitionId::new("p0")?,
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
        checkpoint_id: CheckpointId::new("checkpoint-prepared-artifact")?,
        pipeline_id: PipelineId::new("pipeline-1")?,
        resource_id: ResourceId::new("orders")?,
        scope,
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: None,
        input_position: None,
        output_position,
        schema_hash: spec.schema_hash.clone(),
        segments: segments.clone(),
    };
    let commit_plan = DestinationCommitPlanPreimage::package_hash_token(
        spec.target.clone(),
        spec.disposition.clone(),
        Vec::new(),
        spec.schema_hash.clone(),
        segments,
    );
    builder.write_input_checkpoint_artifact(&None)?;
    builder.write_state_delta_preimage_artifact(&state_delta)?;
    builder.write_commit_plan_preimage_artifact(&commit_plan)?;
    Ok(())
}

#[cfg(test)]
mod tests;
