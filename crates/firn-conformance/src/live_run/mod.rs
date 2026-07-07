use std::{
    fs,
    path::{Path, PathBuf},
};

use firn_kernel::{
    CheckpointId, CheckpointStatus, FirnError, PipelineId, Receipt, Result, SourcePosition,
    StateDelta, TargetName, WriteDisposition,
};
use firn_package::{PackageReader, PackageStatus};
use firn_project::{
    InMemoryResourceSourceResolver, LocalFileDuckDbRunReport, LocalFileDuckDbRunRequest,
    compile_project_declarative_resources_with_root, parse_firn_toml,
    run_local_file_to_duckdb_checkpoint,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    golden_package::{
        GoldenPackageEvidence, assert_golden_package_evidence_matches,
        read_verified_golden_package_evidence,
    },
    package_replay::{
        DuckDbDestination, PreparedPackageReplayCase, SqliteCheckpointStore,
        assert_checkpoint_head_matches, assert_duckdb_mirror_matches_receipt,
        assert_package_receipt_durable,
    },
};

pub const LIVE_LOCAL_FILE_V1_EXPECTED_JSON: &str =
    include_str!("../../golden/live-local-file-v1/expected.json");
pub const LIVE_LOCAL_FILE_V1_PACKAGE_ID: &str = "live-local-file-v1";
pub const LIVE_LOCAL_FILE_V1_CHECKPOINT_ID: &str = "checkpoint-live-local-file-v1";
pub const LIVE_LOCAL_FILE_V1_PIPELINE_ID: &str = "pipeline-live-local-file-v1";
pub const LIVE_LOCAL_FILE_V1_RESOURCE_ID: &str = "local.events";
pub const LIVE_LOCAL_FILE_V1_TARGET: &str = "events";
pub const LIVE_LOCAL_FILE_V1_SOURCE_PATH: &str = "data/events.ndjson";
pub const LIVE_LOCAL_FILE_V1_SOURCE_SHA256: &str =
    "b8ecb46f86694505cef18e88722db9f4bc3a7c07cfb62230bf7ad123e61c9cb6";
pub const LIVE_LOCAL_FILE_V1_SOURCE_SIZE_BYTES: u64 = 46;
pub const LIVE_LOCAL_FILE_V1_ROW_COUNT: u64 = 2;
pub const LIVE_LOCAL_FILE_V1_SEGMENT_COUNT: usize = 1;

const FIRN_PROJECT_TOML: &str = r#"
[project]
name = "live_local_file_conformance"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.firn/state.sqlite"
packages = ".firn/packages"
destination = "duckdb://.firn/dev.duckdb"

[resources."local.events"]
source = "resources/live.toml"
"#;

const LIVE_RESOURCE_TOML: &str = r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
id = "local.events"
glob = "events.ndjson"
format = "ndjson"
primary_key = ["id"]
write_disposition = "append"
trust = "governed"
partition = { by = "file" }
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "name", type = "string", nullable = true },
] }
"#;

const LIVE_SOURCE_CONTENTS: &str = "{\"id\":1,\"name\":\"ada\"}\n{\"id\":2,\"name\":\"grace\"}\n";

#[derive(Clone, Debug)]
pub struct LiveLocalFileFixtureSpec {
    pub project_root: PathBuf,
    pub package_root: PathBuf,
    pub destination_path: PathBuf,
    pub state_store_path: PathBuf,
    pub package_id: String,
    pub checkpoint_id: CheckpointId,
    pub pipeline_id: PipelineId,
    pub target: TargetName,
}

impl LiveLocalFileFixtureSpec {
    pub fn live_local_file_v1(project_root: impl AsRef<Path>) -> Result<Self> {
        let project_root = project_root.as_ref().to_path_buf();
        Ok(Self {
            package_root: project_root.join(".firn/packages"),
            destination_path: project_root.join(".firn/dev.duckdb"),
            state_store_path: project_root.join(".firn/state.sqlite"),
            project_root,
            package_id: LIVE_LOCAL_FILE_V1_PACKAGE_ID.to_owned(),
            checkpoint_id: CheckpointId::new(LIVE_LOCAL_FILE_V1_CHECKPOINT_ID)?,
            pipeline_id: PipelineId::new(LIVE_LOCAL_FILE_V1_PIPELINE_ID)?,
            target: TargetName::new(LIVE_LOCAL_FILE_V1_TARGET)?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct LiveLocalFileFixture {
    pub spec: LiveLocalFileFixtureSpec,
    pub report: LocalFileDuckDbRunReport,
    pub package_evidence: GoldenPackageEvidence,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveRunGoldenEvidence {
    pub package_id: String,
    pub checkpoint_id: String,
    pub pipeline_id: String,
    pub resource_id: String,
    pub target: String,
    pub source_path_suffix: String,
    pub source_sha256: String,
    pub source_size_bytes: u64,
    pub destination_rows: u64,
    pub segment_count: usize,
    pub mirror_load_rows: usize,
    pub mirror_state_rows: usize,
    pub package: GoldenPackageEvidence,
}

pub fn live_local_file_v1_expected_evidence() -> Result<LiveRunGoldenEvidence> {
    serde_json::from_str(LIVE_LOCAL_FILE_V1_EXPECTED_JSON).map_err(|error| {
        FirnError::data(format!(
            "read live-local-file-v1 expected evidence: {error}"
        ))
    })
}

pub async fn run_live_local_file_fixture(
    spec: LiveLocalFileFixtureSpec,
) -> Result<LiveLocalFileFixture> {
    let report = run_live_local_file_fixture_with_hook(spec.clone(), None).await?;
    let package_evidence = read_verified_golden_package_evidence(&report.package_dir)?;
    Ok(LiveLocalFileFixture {
        spec,
        report,
        package_evidence,
    })
}

pub async fn run_live_local_file_fixture_with_hook(
    spec: LiveLocalFileFixtureSpec,
    after_receipt_verified: Option<firn_project::ReceiptVerifiedHook<'_>>,
) -> Result<LocalFileDuckDbRunReport> {
    write_live_fixture_files(&spec.project_root)?;
    fs::create_dir_all(&spec.package_root)
        .map_err(|error| FirnError::data(format!("create package root: {error}")))?;
    if let Some(parent) = spec.destination_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| FirnError::data(format!("create destination parent: {error}")))?;
    }
    if let Some(parent) = spec.state_store_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| FirnError::data(format!("create state parent: {error}")))?;
    }

    let config = parse_firn_toml(FIRN_PROJECT_TOML)?;
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/live.toml", LIVE_RESOURCE_TOML);
    let mut resources =
        compile_project_declarative_resources_with_root(&config, &resolver, &spec.project_root)?;
    if resources.len() != 1 {
        return Err(FirnError::contract(format!(
            "live conformance fixture expected one resource, found {}",
            resources.len()
        )));
    }
    let resource = resources.remove(0);
    if resource.descriptor().resource_id.as_str() != LIVE_LOCAL_FILE_V1_RESOURCE_ID {
        return Err(FirnError::contract(format!(
            "live conformance fixture compiled unexpected resource {}",
            resource.descriptor().resource_id
        )));
    }

    let plan =
        serde_json::from_value(live_engine_plan_json(&spec.package_id)).map_err(|error| {
            FirnError::data(format!(
                "build live-local-file-v1 engine plan from fixture json: {error}"
            ))
        })?;

    run_local_file_to_duckdb_checkpoint(LocalFileDuckDbRunRequest {
        resource: &resource,
        plan,
        package_root: spec.package_root,
        destination_path: spec.destination_path,
        state_store_path: spec.state_store_path,
        pipeline_id: spec.pipeline_id,
        target: spec.target,
        package_id: spec.package_id,
        checkpoint_id: spec.checkpoint_id,
        after_receipt_verified,
    })
    .await
}

pub fn assert_live_run_matches_expected(
    fixture: &LiveLocalFileFixture,
    expected: &LiveRunGoldenEvidence,
) {
    assert_eq!(fixture.report.package_id, expected.package_id);
    assert_eq!(
        fixture.report.package_hash.as_str(),
        expected.package.package_hash
    );
    assert_eq!(fixture.report.package_status, PackageStatus::Checkpointed);
    assert_eq!(
        fixture.report.checkpoint.status,
        CheckpointStatus::Committed
    );
    assert_eq!(
        fixture.report.checkpoint.delta.checkpoint_id.as_str(),
        expected.checkpoint_id
    );
    assert_eq!(
        fixture.report.checkpoint.delta.pipeline_id.as_str(),
        expected.pipeline_id
    );
    assert_eq!(
        fixture.report.checkpoint.delta.resource_id.as_str(),
        expected.resource_id
    );
    assert_eq!(fixture.report.receipt.target.as_str(), expected.target);
    assert_eq!(fixture.report.row_count, expected.destination_rows);
    assert_eq!(fixture.report.segment_count, expected.segment_count);
    assert_eq!(
        fixture.report.receipt.counts.rows_written,
        expected.destination_rows
    );
    assert_eq!(
        fixture.report.receipt.counts.rows_inserted,
        Some(expected.destination_rows)
    );
    assert_eq!(
        fixture
            .report
            .receipt
            .segment_acks
            .iter()
            .map(|ack| ack.row_count)
            .sum::<u64>(),
        expected.destination_rows
    );
    assert_source_position_matches_expected(&fixture.report.checkpoint.delta, expected);

    let store = SqliteCheckpointStore::open(&fixture.spec.state_store_path).unwrap();
    assert_checkpoint_head_matches(&store, &fixture.report.checkpoint.delta);
    assert_package_receipt_durable(&fixture.report.package_dir, &fixture.report.receipt);

    let destination = DuckDbDestination::new(&fixture.spec.destination_path).unwrap();
    assert!(
        destination
            .verify_receipt(&fixture.report.receipt)
            .unwrap()
            .verified,
        "live run DuckDB receipt must verify"
    );
    let snapshot = destination.read_mirror_snapshot_read_only().unwrap();
    assert_eq!(snapshot.loads.len(), expected.mirror_load_rows);
    assert_eq!(snapshot.state.len(), expected.mirror_state_rows);
    assert_eq!(
        snapshot.state.iter().map(|row| row.row_count).sum::<u64>(),
        expected.destination_rows
    );
    let case = live_replay_case(
        &fixture.report.package_dir,
        fixture.report.checkpoint.delta.clone(),
        fixture.spec.target.clone(),
    );
    assert_duckdb_mirror_matches_receipt(&snapshot, &case, &fixture.report.receipt);

    let actual = read_verified_golden_package_evidence(&fixture.report.package_dir).unwrap();
    assert_golden_package_evidence_matches(&expected.package, &actual);
}

pub fn live_run_expected_from_fixture(fixture: &LiveLocalFileFixture) -> LiveRunGoldenEvidence {
    LiveRunGoldenEvidence {
        package_id: fixture.report.package_id.clone(),
        checkpoint_id: fixture
            .report
            .checkpoint
            .delta
            .checkpoint_id
            .as_str()
            .to_owned(),
        pipeline_id: fixture
            .report
            .checkpoint
            .delta
            .pipeline_id
            .as_str()
            .to_owned(),
        resource_id: fixture
            .report
            .checkpoint
            .delta
            .resource_id
            .as_str()
            .to_owned(),
        target: fixture.report.receipt.target.as_str().to_owned(),
        source_path_suffix: LIVE_LOCAL_FILE_V1_SOURCE_PATH.to_owned(),
        source_sha256: LIVE_LOCAL_FILE_V1_SOURCE_SHA256.to_owned(),
        source_size_bytes: LIVE_LOCAL_FILE_V1_SOURCE_SIZE_BYTES,
        destination_rows: fixture.report.row_count,
        segment_count: fixture.report.segment_count,
        mirror_load_rows: 1,
        mirror_state_rows: fixture.report.checkpoint.delta.segments.len(),
        package: fixture.package_evidence.clone(),
    }
}

pub fn live_replay_case(
    package_dir: impl AsRef<Path>,
    delta: StateDelta,
    target: TargetName,
) -> PreparedPackageReplayCase {
    PreparedPackageReplayCase {
        package_dir: package_dir.as_ref().to_path_buf(),
        schema_hash: delta.schema_hash.clone(),
        delta,
        target,
        disposition: WriteDisposition::Append,
        merge_keys: Vec::new(),
    }
}

pub fn read_single_live_receipt(package_dir: impl AsRef<Path>) -> Receipt {
    let receipts = PackageReader::open(package_dir)
        .unwrap()
        .receipts()
        .unwrap();
    assert_eq!(receipts.len(), 1, "live package must contain one receipt");
    receipts[0].clone()
}

fn write_live_fixture_files(project_root: &Path) -> Result<()> {
    let data_dir = project_root.join("data");
    fs::create_dir_all(&data_dir)
        .map_err(|error| FirnError::data(format!("create live fixture data dir: {error}")))?;
    fs::write(data_dir.join("events.ndjson"), LIVE_SOURCE_CONTENTS)
        .map_err(|error| FirnError::data(format!("write live fixture source file: {error}")))
}

fn assert_source_position_matches_expected(delta: &StateDelta, expected: &LiveRunGoldenEvidence) {
    let SourcePosition::FileManifest(manifest) = &delta.output_position else {
        panic!("live run checkpoint output position must be a FileManifest");
    };
    assert_eq!(manifest.version, 1);
    assert_eq!(manifest.files.len(), 1);
    let file = &manifest.files[0];
    assert!(
        file.path.ends_with(&expected.source_path_suffix),
        "source path {:?} must end with {:?}",
        file.path,
        expected.source_path_suffix
    );
    assert_eq!(file.size_bytes, expected.source_size_bytes);
    assert_eq!(
        file.sha256.as_deref(),
        Some(expected.source_sha256.as_str())
    );
    assert_eq!(file.etag, None);
    for segment in &delta.segments {
        assert_eq!(
            segment.output_position, delta.output_position,
            "live run segment source position must match checkpoint output position"
        );
    }
}

fn live_engine_plan_json(package_id: &str) -> serde_json::Value {
    let validation_program = validation_program_json();
    let scope = json!({ "kind": "file", "path": "events.ndjson" });
    let scan = json!({
        "plan_id": "plan-local.events",
        "request": {
            "resource_id": LIVE_LOCAL_FILE_V1_RESOURCE_ID,
            "projection": null,
            "filters": [],
            "limit": null,
            "order_by": [],
            "scope": scope,
        },
        "partitions": [{
            "partition_id": "files",
            "scope": scope,
            "start_position": null,
            "metadata": {
                "kind": "files",
                "glob": "events.ndjson",
                "resource_id": LIVE_LOCAL_FILE_V1_RESOURCE_ID,
            },
        }],
        "pushed_predicates": [],
        "unsupported_predicates": [],
        "estimated_rows": null,
        "estimated_bytes": null,
        "delivery_guarantee": "effectively_once_per_package",
    });
    let operator_chain = json!([
        {
            "kind": "data_fusion_table_provider",
            "provider_kind": "datafusion_table_provider",
            "resource_id": LIVE_LOCAL_FILE_V1_RESOURCE_ID,
        },
        {
            "kind": "data_fusion_scan_exec",
            "projection": null,
            "residual_predicates": [],
            "limit": null,
        },
        { "kind": "schema_fingerprint_exec" },
        {
            "kind": "contract_exec",
            "normalizer_version": "namecase-v1",
            "column_program_count": 2,
        },
        {
            "kind": "normalize_exec",
            "normalizer_version": "namecase-v1",
        },
        { "kind": "profile_exec" },
        { "kind": "lineage_exec" },
        {
            "kind": "package_sink",
            "package_id": package_id,
        },
    ]);

    json!({
        "scan": scan,
        "final_projection": null,
        "residual_predicates": [],
        "boundedness": { "kind": "bounded" },
        "validation_program": validation_program,
        "operator_chain": operator_chain,
        "explain": {
            "resource_id": LIVE_LOCAL_FILE_V1_RESOURCE_ID,
            "projected_fields": [],
            "projection_pushed": false,
            "limit": null,
            "limit_pushed": false,
            "pushed_predicates": [],
            "inexact_predicates": [],
            "unsupported_predicates": [],
            "partitions": [{
                "partition_id": "files",
                "scope_kind": "file",
                "metadata": {
                    "kind": "files",
                    "glob": "events.ndjson",
                    "resource_id": LIVE_LOCAL_FILE_V1_RESOURCE_ID,
                },
            }],
            "estimates": {
                "support": "bytes",
                "rows": null,
                "bytes": null,
            },
            "delivery_guarantee": "effectively_once_per_package",
            "boundedness": { "kind": "bounded" },
            "operator_chain": operator_chain,
        },
        "package_id": package_id,
    })
}

fn validation_program_json() -> serde_json::Value {
    json!({
        "normalizer_version": "namecase-v1",
        "schema_verdicts": [],
        "column_programs": [
            {
                "source_name": "id",
                "output_name": "id",
                "arrow_type": { "kind": "int", "signed": true, "bits": 64 },
                "steps": [],
                "nested_action": { "kind": "not_nested" },
                "redaction": { "kind": "preserve" },
            },
            {
                "source_name": "name",
                "output_name": "name",
                "arrow_type": { "kind": "utf8" },
                "steps": [],
                "nested_action": { "kind": "not_nested" },
                "redaction": { "kind": "preserve" },
            },
        ],
        "row_dispositions": [
            { "outcome": "pass", "disposition": "accept" },
            { "outcome": "coerced", "disposition": "accept" },
            { "outcome": "admitted_as_variant", "disposition": "accept" },
            { "outcome": "violation", "disposition": "quarantine" },
            { "outcome": "fatal", "disposition": "reject_run" },
        ],
        "transforms": [],
        "promotion": {
            "clean_runs_required": 3,
            "allow_sampled_fast_path": false,
            "demote_on_drift": true,
            "demote_on_anomaly": true,
            "demote_on_quarantine": true,
        },
        "warnings": [],
    })
}

#[cfg(test)]
mod tests;
