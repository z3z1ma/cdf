use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use cdf_contract::{ContractPolicy, ObservedSchema, compile_validation_program};
use cdf_engine::{EnginePlanInput, Planner};
use cdf_kernel::ExecutionExtent;
use cdf_kernel::{
    CdfError, CheckpointId, CheckpointStatus, PipelineId, Receipt, Result, ScanRequest,
    SourcePosition, StateDelta, TargetName, WriteDisposition,
};
use cdf_package::PackageReader;
use cdf_package_contract::PackageStatus;
use cdf_project::{
    InMemoryResourceSourceResolver, ProjectRunReport, ProjectRunRequest, ProjectRunSource,
    ResolvedProjectDestination, compile_project_declarative_resources_with_root, parse_cdf_toml,
    run_project,
};
use serde::{Deserialize, Serialize};

use crate::{
    golden_package::{
        GoldenPackageEvidence, assert_verified_package_matches_golden,
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
pub const LIVE_LOCAL_FILE_PARQUET_V1_EXPECTED_JSON: &str =
    include_str!("../../golden/live-local-file-parquet-v1/expected.json");
pub const LIVE_LOCAL_FILE_POSTGRES_V1_EXPECTED_JSON: &str =
    include_str!("../../golden/live-local-file-postgres-v1/expected.json");
pub const LIVE_LOCAL_FILE_V1_PACKAGE_ID: &str = "live-local-file-v1";
pub const LIVE_LOCAL_FILE_PARQUET_V1_PACKAGE_ID: &str = "live-local-file-parquet-v1";
pub const LIVE_LOCAL_FILE_POSTGRES_V1_PACKAGE_ID: &str = "live-local-file-postgres-v1";
pub const LIVE_LOCAL_FILE_V1_CHECKPOINT_ID: &str = "checkpoint-live-local-file-v1";
pub const LIVE_LOCAL_FILE_PARQUET_V1_CHECKPOINT_ID: &str = "checkpoint-live-local-file-parquet-v1";
pub const LIVE_LOCAL_FILE_POSTGRES_V1_CHECKPOINT_ID: &str =
    "checkpoint-live-local-file-postgres-v1";
pub const LIVE_LOCAL_FILE_V1_PIPELINE_ID: &str = "pipeline-live-local-file-v1";
pub const LIVE_LOCAL_FILE_PARQUET_V1_PIPELINE_ID: &str = "pipeline-live-local-file-parquet-v1";
pub const LIVE_LOCAL_FILE_POSTGRES_V1_PIPELINE_ID: &str = "pipeline-live-local-file-postgres-v1";
pub const LIVE_LOCAL_FILE_V1_RESOURCE_ID: &str = "local.events";
pub const LIVE_LOCAL_FILE_V1_TARGET: &str = "events";
pub const LIVE_LOCAL_FILE_POSTGRES_SCHEMA: &str = "cdf_live_run_golden";
pub const LIVE_LOCAL_FILE_V1_SOURCE_PATH: &str = "data/events.ndjson";
pub const LIVE_LOCAL_FILE_V1_SOURCE_POSITION_PATH: &str = "events.ndjson";
pub const LIVE_LOCAL_FILE_V1_SOURCE_SHA256: &str =
    "sha256:b8ecb46f86694505cef18e88722db9f4bc3a7c07cfb62230bf7ad123e61c9cb6";
pub const LIVE_LOCAL_FILE_V1_SOURCE_SIZE_BYTES: u64 = 46;
pub const LIVE_LOCAL_FILE_V1_ROW_COUNT: u64 = 2;
pub const LIVE_LOCAL_FILE_V1_SEGMENT_COUNT: usize = 1;

const CDF_PROJECT_TOML: &str = r#"
[project]
name = "live_local_file_conformance"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.sqlite"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[resources."local.events"]
source = "resources/live.toml"
"#;

const LIVE_RESOURCE_TOML: &str = r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
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
const LIVE_SOURCE_MODIFIED_SECS: u64 = 1_700_000_000;

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
    pub destination: LiveRunGoldenDestination,
}

impl LiveLocalFileFixtureSpec {
    pub fn live_local_file_v1(project_root: impl AsRef<Path>) -> Result<Self> {
        let project_root = project_root.as_ref().to_path_buf();
        Ok(Self {
            package_root: project_root.join(".cdf/packages"),
            destination_path: project_root.join(".cdf/dev.duckdb"),
            state_store_path: project_root.join(".cdf/state.sqlite"),
            project_root,
            package_id: LIVE_LOCAL_FILE_V1_PACKAGE_ID.to_owned(),
            checkpoint_id: CheckpointId::new(LIVE_LOCAL_FILE_V1_CHECKPOINT_ID)?,
            pipeline_id: PipelineId::new(LIVE_LOCAL_FILE_V1_PIPELINE_ID)?,
            target: TargetName::new(LIVE_LOCAL_FILE_V1_TARGET)?,
            destination: LiveRunGoldenDestination::DuckDb,
        })
    }

    pub fn live_local_file_parquet_v1(project_root: impl AsRef<Path>) -> Result<Self> {
        let project_root = project_root.as_ref().to_path_buf();
        Ok(Self {
            package_root: project_root.join(".cdf/packages"),
            destination_path: project_root.join(".cdf/lake"),
            state_store_path: project_root.join(".cdf/state.sqlite"),
            project_root,
            package_id: LIVE_LOCAL_FILE_PARQUET_V1_PACKAGE_ID.to_owned(),
            checkpoint_id: CheckpointId::new(LIVE_LOCAL_FILE_PARQUET_V1_CHECKPOINT_ID)?,
            pipeline_id: PipelineId::new(LIVE_LOCAL_FILE_PARQUET_V1_PIPELINE_ID)?,
            target: TargetName::new(LIVE_LOCAL_FILE_V1_TARGET)?,
            destination: LiveRunGoldenDestination::ParquetFilesystem,
        })
    }

    pub fn live_local_file_postgres_v1(
        project_root: impl AsRef<Path>,
        target: TargetName,
    ) -> Result<Self> {
        let project_root = project_root.as_ref().to_path_buf();
        Ok(Self {
            package_root: project_root.join(".cdf/packages"),
            destination_path: project_root.join(".cdf/postgres-unused"),
            state_store_path: project_root.join(".cdf/state.sqlite"),
            project_root,
            package_id: LIVE_LOCAL_FILE_POSTGRES_V1_PACKAGE_ID.to_owned(),
            checkpoint_id: CheckpointId::new(LIVE_LOCAL_FILE_POSTGRES_V1_CHECKPOINT_ID)?,
            pipeline_id: PipelineId::new(LIVE_LOCAL_FILE_POSTGRES_V1_PIPELINE_ID)?,
            target,
            destination: LiveRunGoldenDestination::Postgres,
        })
    }
}

#[derive(Clone, Debug)]
pub struct LiveLocalFileFixture {
    pub spec: LiveLocalFileFixtureSpec,
    pub report: ProjectRunReport,
    pub package_evidence: GoldenPackageEvidence,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveRunGoldenDestination {
    #[serde(rename = "duckdb")]
    DuckDb,
    ParquetFilesystem,
    Postgres,
}

impl LiveRunGoldenDestination {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DuckDb => "duckdb",
            Self::ParquetFilesystem => "parquet_filesystem",
            Self::Postgres => "postgres",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveRunGoldenEvidence {
    pub package_id: String,
    pub package_hash: String,
    pub checkpoint_id: String,
    pub pipeline_id: String,
    pub resource_id: String,
    pub destination: LiveRunGoldenDestination,
    pub destination_target: String,
    pub source_path_suffix: String,
    pub source_sha256: String,
    pub source_size_bytes: u64,
    pub destination_rows: u64,
    pub destination_row_counts: BTreeMap<String, u64>,
    pub segment_count: usize,
    pub mirror_load_rows: usize,
    pub mirror_state_rows: usize,
    pub package: GoldenPackageEvidence,
}

pub fn live_local_file_v1_expected_evidence() -> Result<LiveRunGoldenEvidence> {
    live_run_expected_evidence_from_json("live-local-file-v1", LIVE_LOCAL_FILE_V1_EXPECTED_JSON)
}

pub fn live_local_file_parquet_v1_expected_evidence() -> Result<LiveRunGoldenEvidence> {
    live_run_expected_evidence_from_json(
        "live-local-file-parquet-v1",
        LIVE_LOCAL_FILE_PARQUET_V1_EXPECTED_JSON,
    )
}

pub fn live_local_file_postgres_v1_expected_evidence() -> Result<LiveRunGoldenEvidence> {
    live_run_expected_evidence_from_json(
        "live-local-file-postgres-v1",
        LIVE_LOCAL_FILE_POSTGRES_V1_EXPECTED_JSON,
    )
}

fn live_run_expected_evidence_from_json(
    fixture_name: &str,
    json: &str,
) -> Result<LiveRunGoldenEvidence> {
    serde_json::from_str(json)
        .map_err(|error| CdfError::data(format!("read {fixture_name} expected evidence: {error}")))
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
    after_receipt_verified: Option<cdf_project::ReceiptVerifiedHook<'_>>,
) -> Result<ProjectRunReport> {
    let destination = crate::destination_catalog::resolve(
        &crate::destination_catalog::local_uri("duckdb", &spec.destination_path),
        &spec.project_root,
        spec.target.clone(),
    )?;
    run_live_local_file_fixture_with_destination(spec, destination, after_receipt_verified).await
}

pub async fn run_live_local_file_fixture_with_destination(
    spec: LiveLocalFileFixtureSpec,
    destination: ResolvedProjectDestination,
    after_receipt_verified: Option<cdf_project::ReceiptVerifiedHook<'_>>,
) -> Result<ProjectRunReport> {
    write_live_fixture_files(&spec.project_root)?;
    fs::create_dir_all(&spec.package_root)
        .map_err(|error| CdfError::data(format!("create package root: {error}")))?;
    if let Some(parent) = spec.destination_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| CdfError::data(format!("create destination parent: {error}")))?;
    }
    if let Some(parent) = spec.state_store_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| CdfError::data(format!("create state parent: {error}")))?;
    }

    let config = parse_cdf_toml(CDF_PROJECT_TOML)?;
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/live.toml", LIVE_RESOURCE_TOML);
    let source_registry = crate::source_fixture::local_file_registry()?;
    let mut resources = compile_project_declarative_resources_with_root(
        &source_registry,
        &config,
        &resolver,
        &spec.project_root,
    )?;
    if resources.len() != 1 {
        return Err(CdfError::contract(format!(
            "live conformance fixture expected one resource, found {}",
            resources.len()
        )));
    }
    let resource = resources.remove(0);
    if resource.descriptor().resource_id.as_str() != LIVE_LOCAL_FILE_V1_RESOURCE_ID {
        return Err(CdfError::contract(format!(
            "live conformance fixture compiled unexpected resource {}",
            resource.descriptor().resource_id
        )));
    }
    let runtime_resource =
        crate::source_fixture::resolve_local_file(&resource, &spec.project_root)?;

    let mut policy = ContractPolicy::for_trust(
        runtime_resource
            .queryable()
            .descriptor()
            .trust_level
            .clone(),
    );
    if let Some(identifier_policy) = destination.column_identifier_policy()? {
        policy.normalization.identifier = identifier_policy;
    }
    let validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(runtime_resource.queryable().schema().as_ref()),
    )?;
    let plan = Planner::new().plan_tier_b(
        runtime_resource.queryable(),
        EnginePlanInput {
            request: ScanRequest {
                resource_id: runtime_resource
                    .queryable()
                    .descriptor()
                    .resource_id
                    .clone(),
                projection: None,
                filters: Vec::new(),
                limit: None,
                order_by: Vec::new(),
                scope: runtime_resource
                    .queryable()
                    .descriptor()
                    .state_scope
                    .clone(),
            },
            validation_program,
            execution_extent: ExecutionExtent::bounded(),
            package_id: spec.package_id.clone(),
        },
    )?;
    let plan = runtime_resource.bind_plan(plan)?;

    let services = crate::test_execution_services();
    run_project(
        ProjectRunRequest {
            resource: ProjectRunSource::new(runtime_resource.queryable()),
            plan,
            package_root: spec.package_root,
            state_store_path: spec.state_store_path,
            pipeline_id: spec.pipeline_id,
            destination,
            package_id: spec.package_id,
            checkpoint_id: spec.checkpoint_id,
            run_id: None,
            event_sink: None,
            after_receipt_verified,
        },
        &services,
    )
    .await
}

pub fn assert_live_run_matches_expected(
    fixture: &LiveLocalFileFixture,
    expected: &LiveRunGoldenEvidence,
    destination_row_counts: BTreeMap<String, u64>,
) {
    let actual_package =
        assert_verified_package_matches_golden(&fixture.report.package_dir, &expected.package)
            .unwrap();
    assert_eq!(actual_package.package_hash, expected.package_hash);
    assert_eq!(fixture.report.package_id, expected.package_id);
    assert_eq!(fixture.report.package_hash.as_str(), expected.package_hash);
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
    assert_eq!(fixture.spec.destination, expected.destination);
    assert_eq!(
        fixture.report.receipt.target.as_str(),
        expected.destination_target
    );
    assert_eq!(fixture.report.row_count, expected.destination_rows);
    assert_eq!(destination_row_counts, expected.destination_row_counts);
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
    let replay_inputs = PackageReader::open(&fixture.report.package_dir)
        .unwrap()
        .replay_inputs()
        .unwrap();
    assert_eq!(replay_inputs.state_delta, fixture.report.checkpoint.delta);
    assert_eq!(
        replay_inputs.destination_commit.package_hash,
        fixture.report.package_hash
    );
    assert_eq!(
        replay_inputs.destination_commit.idempotency_token.as_str(),
        fixture.report.package_hash.as_str()
    );

    if expected.destination == LiveRunGoldenDestination::DuckDb {
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
    }
}

pub fn live_run_expected_from_fixture(
    fixture: &LiveLocalFileFixture,
    destination_row_counts: BTreeMap<String, u64>,
) -> LiveRunGoldenEvidence {
    LiveRunGoldenEvidence {
        package_id: fixture.report.package_id.clone(),
        package_hash: fixture.report.package_hash.as_str().to_owned(),
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
        destination: fixture.spec.destination,
        destination_target: fixture.report.receipt.target.as_str().to_owned(),
        source_path_suffix: LIVE_LOCAL_FILE_V1_SOURCE_POSITION_PATH.to_owned(),
        source_sha256: LIVE_LOCAL_FILE_V1_SOURCE_SHA256.to_owned(),
        source_size_bytes: LIVE_LOCAL_FILE_V1_SOURCE_SIZE_BYTES,
        destination_rows: fixture.report.row_count,
        destination_row_counts,
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
        .map_err(|error| CdfError::data(format!("create live fixture data dir: {error}")))?;
    let path = data_dir.join("events.ndjson");
    fs::write(&path, LIVE_SOURCE_CONTENTS)
        .map_err(|error| CdfError::data(format!("write live fixture source file: {error}")))?;
    let file = fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .map_err(|error| CdfError::data(format!("open live fixture source file: {error}")))?;
    let modified = SystemTime::UNIX_EPOCH + Duration::from_secs(LIVE_SOURCE_MODIFIED_SECS);
    file.set_times(fs::FileTimes::new().set_modified(modified))
        .map_err(|error| CdfError::data(format!("stabilize live fixture source time: {error}")))
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

#[cfg(test)]
mod destinations;
#[cfg(test)]
pub(crate) mod drift_quarantine;
#[cfg(test)]
mod evidence;
#[cfg(test)]
mod tests;
