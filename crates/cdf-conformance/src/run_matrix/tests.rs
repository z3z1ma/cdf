use std::{
    cell::Cell,
    env, fs,
    net::TcpListener,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use cdf_declarative::CompiledResource;
use cdf_dest_duckdb::{DuckDbDestination, DuckDbMirrorSnapshot};
use cdf_dest_parquet::ParquetDestination;
use cdf_dest_postgres::{MergeDedupPolicy, PostgresDestination, PostgresTarget};
use cdf_engine::EnginePlan;
use cdf_kernel::{
    CdfError, CheckpointStatus, CheckpointStore, DestinationProtocol, IdempotencySupport,
    PipelineId, Receipt, ResourceId, Result, RunId, ScopeKey, SourcePosition, TargetName,
};
use cdf_package::{PackageReader, PackageStatus};
use cdf_project::{
    InMemoryResourceSourceResolver, PackageArtifactReplayRequest, ProjectReceiptSource,
    ProjectRunReport, ProjectRunRequest, ProjectRunSource, ResolvedProjectDestination,
    compile_project_declarative_resources_with_root, parse_cdf_toml, replay_package_from_artifacts,
    run_project,
};
use cdf_state_sqlite::SqliteCheckpointStore;
use postgres::{Client, NoTls};
use serde_json::json;
use tempfile::TempDir;

use super::{
    ExcludedMatrixCell, ExecutedMatrixCell, MatrixDestination, MatrixDisposition, RunMatrixCell,
    RunMatrixOutput, file_source_matrix_cells,
};

const CDF_PROJECT_TOML: &str = r#"
[project]
name = "run_matrix_file_conformance"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.sqlite"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[resources."local.events"]
source = "resources/live.toml"
"#;

const RESOURCE_ID: &str = "local.events";
const SOURCE_PATH: &str = "data/events.ndjson";
const SOURCE_POSITION_PATH: &str = "events.ndjson";
const SOURCE_CONTENTS: &str = "{\"id\":1,\"name\":\"ada\"}\n{\"id\":2,\"name\":\"grace\"}\n";
const SOURCE_SHA256: &str = "b8ecb46f86694505cef18e88722db9f4bc3a7c07cfb62230bf7ad123e61c9cb6";
const SOURCE_SIZE_BYTES: u64 = 46;
const ROW_COUNT: u64 = 2;
const SEGMENT_COUNT: usize = 1;

static LIVE_POSTGRES_SCHEMA_COUNTER: AtomicU64 = AtomicU64::new(0);
static LOCAL_POSTGRES_START: Mutex<()> = Mutex::new(());

#[test]
fn run_matrix_file_source_cells_persist_output() {
    let postgres = LivePostgres::start().expect(
        "C1 run matrix requires Postgres coverage; set TEST_DATABASE_URL or install initdb/pg_ctl",
    );
    let mut output = RunMatrixOutput::default();

    for cell in file_source_matrix_cells() {
        if let Some(reason) = sheet_exclusion_reason(&cell) {
            output
                .excluded_cells
                .push(ExcludedMatrixCell { cell, reason });
            continue;
        }

        output
            .executed_cells
            .push(execute_cell(cell, &postgres).unwrap());
    }

    assert!(output.executed_cells.iter().any(|executed| executed.cell
        == RunMatrixCell::file(MatrixDestination::DuckDb, MatrixDisposition::Append)));
    assert!(output.executed_cells.iter().any(|executed| executed.cell
        == RunMatrixCell::file(
            MatrixDestination::ParquetFilesystem,
            MatrixDisposition::Replace,
        )));
    assert!(output.excluded_cells.iter().any(|excluded| {
        excluded.cell
            == RunMatrixCell::file(
                MatrixDestination::ParquetFilesystem,
                MatrixDisposition::Merge,
            )
            && excluded
                .reason
                .contains("supported_dispositions=[append, replace]")
    }));
    assert!(output.executed_cells.iter().any(|executed| executed.cell
        == RunMatrixCell::file(MatrixDestination::Postgres, MatrixDisposition::Merge)));

    println!(
        "CDF_RUN_MATRIX_FILE_SOURCE_OUTPUT={}",
        serde_json::to_string_pretty(&output).unwrap()
    );
}

fn sheet_exclusion_reason(cell: &RunMatrixCell) -> Option<String> {
    if cell.destination != MatrixDestination::ParquetFilesystem
        || cell.disposition != MatrixDisposition::Merge
    {
        return None;
    }
    let temp = tempfile::tempdir().unwrap();
    let destination = ParquetDestination::new_filesystem(temp.path()).unwrap();
    assert!(
        !destination
            .sheet()
            .supported_dispositions
            .contains(&cell.disposition.to_write_disposition())
    );
    Some(
        "Parquet destination sheet supported_dispositions=[append, replace]; merge is not listed"
            .to_owned(),
    )
}

fn execute_cell(cell: RunMatrixCell, postgres: &LivePostgres) -> Result<ExecutedMatrixCell> {
    let temp = tempfile::tempdir()
        .map_err(|error| CdfError::data(format!("create run matrix tempdir: {error}")))?;
    let package_id = format!(
        "run-matrix-{}-{}-{}",
        cell.source_archetype.as_str(),
        cell.destination.as_str(),
        cell.disposition.as_str()
    );
    let checkpoint_id = cdf_kernel::CheckpointId::new(format!("checkpoint-{package_id}"))?;
    let pipeline_id = PipelineId::new(format!("pipeline-{package_id}"))?;
    let run_id = RunId::new(format!("run-{package_id}"))?;
    let package_root = temp.path().join(".cdf/packages");
    let state_store_path = temp.path().join(".cdf/state.sqlite");

    let resource = file_resource(temp.path(), cell.disposition)?;
    let target = target_for_cell(cell, postgres)?;
    let destination =
        MatrixDestinationHandle::new(cell.destination, temp.path(), target, postgres)?;
    let plan = serde_json::from_value(file_engine_plan_json(&package_id, cell.disposition))
        .map_err(|error| CdfError::data(format!("build run matrix engine plan: {error}")))?;
    assert_plan_honesty(&plan, &resource, &package_id);

    let gate_observed = Cell::new(false);
    let resource_id = resource.descriptor().resource_id.clone();
    let scope = resource.descriptor().state_scope.clone();
    let gate_pipeline_id = pipeline_id.clone();
    let gate_store_path = state_store_path.clone();
    let hook = |_receipt: &Receipt| {
        assert_no_checkpoint_head_at_receipt_verified(
            &gate_store_path,
            &gate_pipeline_id,
            &resource_id,
            &scope,
        )?;
        gate_observed.set(true);
        Ok(())
    };

    let report = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::local_file(&resource),
        plan,
        package_root,
        state_store_path: state_store_path.clone(),
        pipeline_id: pipeline_id.clone(),
        package_id: package_id.clone(),
        checkpoint_id: checkpoint_id.clone(),
        destination: destination.resolved()?,
        run_id: Some(run_id),
        after_receipt_verified: Some(&hook),
    }))?;

    assert!(
        gate_observed.get(),
        "receipt verification gate hook must run"
    );
    assert_run_report(cell, &report, &resource_id, &scope, &pipeline_id);
    PackageReader::open(&report.package_dir)?.verify()?;
    assert_replay_inputs_match_run(cell, &report);
    destination.verify_trait_receipt(&report.receipt)?;
    assert_committed_checkpoint(&state_store_path, &report);
    assert_file_source_position(&report);

    let duplicate_behavior =
        assert_duplicate_replay_noop(cell, &destination, &report, temp.path())?;
    assert_artifact_replay_identity(cell, &destination, &report, temp.path())?;

    Ok(ExecutedMatrixCell {
        cell,
        package_id,
        checkpoint_id: checkpoint_id.as_str().to_owned(),
        receipt_id: report.receipt.receipt_id.as_str().to_owned(),
        row_count: report.row_count,
        plan_honesty_asserted: true,
        package_verified: true,
        destination_receipt_verified: true,
        checkpoint_gated_after_receipt_verification: true,
        artifact_replay_identity_asserted: true,
        duplicate_behavior,
    })
}

fn file_resource(project_root: &Path, disposition: MatrixDisposition) -> Result<CompiledResource> {
    let data_dir = project_root.join("data");
    fs::create_dir_all(&data_dir)
        .map_err(|error| CdfError::data(format!("create run matrix data dir: {error}")))?;
    fs::write(project_root.join(SOURCE_PATH), SOURCE_CONTENTS)
        .map_err(|error| CdfError::data(format!("write run matrix source file: {error}")))?;

    let config = parse_cdf_toml(CDF_PROJECT_TOML)?;
    let resource_toml = file_resource_toml(disposition);
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/live.toml", resource_toml);
    let mut resources =
        compile_project_declarative_resources_with_root(&config, &resolver, project_root)?;
    if resources.len() != 1 {
        return Err(CdfError::contract(format!(
            "run matrix expected one file resource, found {}",
            resources.len()
        )));
    }
    let resource = resources.remove(0);
    if resource.descriptor().resource_id.as_str() != RESOURCE_ID {
        return Err(CdfError::contract(format!(
            "run matrix compiled unexpected resource {}",
            resource.descriptor().resource_id
        )));
    }
    Ok(resource)
}

fn file_resource_toml(disposition: MatrixDisposition) -> String {
    format!(
        r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
id = "{RESOURCE_ID}"
glob = "events.ndjson"
format = "ndjson"
primary_key = ["id"]
merge_key = ["id"]
write_disposition = "{}"
trust = "governed"
partition = {{ by = "file" }}
schema = {{ fields = [
  {{ name = "id", type = "int64", nullable = false }},
  {{ name = "name", type = "string", nullable = true }},
] }}
"#,
        disposition.as_str()
    )
}

fn target_for_cell(cell: RunMatrixCell, postgres: &LivePostgres) -> Result<MatrixTarget> {
    let table = format!("events_{}", cell.disposition.as_str());
    match cell.destination {
        MatrixDestination::DuckDb | MatrixDestination::ParquetFilesystem => {
            Ok(MatrixTarget::Plain(TargetName::new(table)?))
        }
        MatrixDestination::Postgres => {
            let target = PostgresTarget::new(Some(&postgres.schema), &table)?;
            Ok(MatrixTarget::Postgres {
                target_name: TargetName::new(target.display_name())?,
                target,
                schema: postgres.schema.clone(),
                table,
            })
        }
    }
}

#[derive(Clone, Debug)]
enum MatrixTarget {
    Plain(TargetName),
    Postgres {
        target_name: TargetName,
        target: PostgresTarget,
        schema: String,
        table: String,
    },
}

#[derive(Clone, Debug)]
enum MatrixDestinationHandle {
    DuckDb {
        database_path: PathBuf,
        target: TargetName,
    },
    Parquet {
        root: PathBuf,
        target: TargetName,
    },
    Postgres {
        database_url: String,
        target: PostgresTarget,
        target_name: TargetName,
        schema: String,
        table: String,
    },
}

impl MatrixDestinationHandle {
    fn new(
        destination: MatrixDestination,
        root: &Path,
        target: MatrixTarget,
        postgres: &LivePostgres,
    ) -> Result<Self> {
        match (destination, target) {
            (MatrixDestination::DuckDb, MatrixTarget::Plain(target)) => Ok(Self::DuckDb {
                database_path: root.join(".cdf/run-matrix.duckdb"),
                target,
            }),
            (MatrixDestination::ParquetFilesystem, MatrixTarget::Plain(target)) => {
                Ok(Self::Parquet {
                    root: root.join(".cdf/lake"),
                    target,
                })
            }
            (
                MatrixDestination::Postgres,
                MatrixTarget::Postgres {
                    target_name,
                    target,
                    schema,
                    table,
                },
            ) => Ok(Self::Postgres {
                database_url: postgres.url.clone(),
                target,
                target_name,
                schema,
                table,
            }),
            _ => Err(CdfError::contract(
                "run matrix destination and target kind do not match",
            )),
        }
    }

    fn resolved(&self) -> Result<ResolvedProjectDestination> {
        match self {
            Self::DuckDb {
                database_path,
                target,
            } => ResolvedProjectDestination::duckdb(database_path, target.clone()),
            Self::Parquet { root, target } => {
                ResolvedProjectDestination::parquet_filesystem(root, target.clone())
            }
            Self::Postgres {
                database_url,
                target,
                ..
            } => ResolvedProjectDestination::postgres(
                database_url.clone(),
                target.clone(),
                MergeDedupPolicy::Last,
                None,
            ),
        }
    }

    fn fresh_artifact_replay_destination(&self, root: &Path) -> Result<Self> {
        match self {
            Self::DuckDb { target, .. } => Ok(Self::DuckDb {
                database_path: root.join(".cdf/replay.duckdb"),
                target: target.clone(),
            }),
            Self::Parquet { target, .. } => Ok(Self::Parquet {
                root: root.join(".cdf/replay-lake"),
                target: target.clone(),
            }),
            Self::Postgres {
                database_url,
                target,
                target_name,
                schema,
                table,
            } => {
                reset_postgres_schema(database_url, schema)?;
                Ok(Self::Postgres {
                    database_url: database_url.clone(),
                    target: target.clone(),
                    target_name: target_name.clone(),
                    schema: schema.clone(),
                    table: table.clone(),
                })
            }
        }
    }

    fn verify_trait_receipt(&self, receipt: &Receipt) -> Result<()> {
        let verification = match self {
            Self::DuckDb { database_path, .. } => {
                let destination = DuckDbDestination::new(database_path)?;
                DestinationProtocol::verify(&destination, receipt)?
            }
            Self::Parquet { root, .. } => {
                let destination = ParquetDestination::new_filesystem(root)?;
                DestinationProtocol::verify(&destination, receipt)?
            }
            Self::Postgres { database_url, .. } => {
                let destination = PostgresDestination::connect(database_url.clone())?;
                DestinationProtocol::verify(&destination, receipt)?
            }
        };
        if !verification.verified {
            return Err(CdfError::destination(format!(
                "run matrix receipt {} did not verify through DestinationProtocol::verify: {}",
                verification.receipt_id,
                verification
                    .reason
                    .unwrap_or_else(|| "verification returned false".to_owned())
            )));
        }
        Ok(())
    }

    fn footprint(&self) -> Result<DestinationFootprint> {
        match self {
            Self::DuckDb { database_path, .. } => Ok(DestinationFootprint::DuckDb(
                DuckDbDestination::new(database_path)?.read_mirror_snapshot_read_only()?,
            )),
            Self::Parquet { root, .. } => Ok(DestinationFootprint::Parquet {
                files: list_relative_files(root)?,
            }),
            Self::Postgres {
                database_url,
                schema,
                table,
                ..
            } => postgres_footprint(database_url, schema, table),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum DestinationFootprint {
    DuckDb(DuckDbMirrorSnapshot),
    Parquet {
        files: Vec<FileFootprint>,
    },
    Postgres {
        target_rows: i64,
        loads_rows: i64,
        state_rows: i64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct FileFootprint {
    path: String,
    size_bytes: u64,
}

fn assert_plan_honesty(plan: &EnginePlan, resource: &CompiledResource, package_id: &str) {
    let descriptor = resource.descriptor();
    assert_eq!(plan.scan.request.resource_id, descriptor.resource_id);
    assert_eq!(plan.package_id, package_id);
    assert_eq!(plan.scan.request.scope, descriptor.state_scope);
}

fn assert_no_checkpoint_head_at_receipt_verified(
    state_store_path: &Path,
    pipeline_id: &PipelineId,
    resource_id: &ResourceId,
    scope: &ScopeKey,
) -> Result<()> {
    let store = SqliteCheckpointStore::open(state_store_path)?;
    let history = store.history(pipeline_id, resource_id, scope)?;
    if history.len() != 1 {
        return Err(CdfError::contract(format!(
            "receipt-verified gate expected one proposed checkpoint, found {}",
            history.len()
        )));
    }
    if history[0].status != CheckpointStatus::Proposed || history[0].is_head {
        return Err(CdfError::contract(
            "receipt-verified gate observed a checkpoint that was not proposed-only",
        ));
    }
    if store.head(pipeline_id, resource_id, scope)?.is_some() {
        return Err(CdfError::contract(
            "checkpoint head advanced before receipt verification gate returned",
        ));
    }
    Ok(())
}

fn assert_run_report(
    cell: RunMatrixCell,
    report: &ProjectRunReport,
    resource_id: &ResourceId,
    scope: &ScopeKey,
    pipeline_id: &PipelineId,
) {
    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert!(report.checkpoint.is_head);
    assert_eq!(report.checkpoint.delta.pipeline_id, *pipeline_id);
    assert_eq!(report.checkpoint.delta.resource_id, *resource_id);
    assert_eq!(report.checkpoint.delta.scope, *scope);
    assert_eq!(
        report.checkpoint.delta.package_hash,
        report.receipt.package_hash
    );
    assert_eq!(
        report.receipt.disposition,
        cell.disposition.to_write_disposition()
    );
    assert_eq!(report.row_count, ROW_COUNT);
    assert_eq!(report.segment_count, SEGMENT_COUNT);
    assert_eq!(report.receipt.counts.rows_written, ROW_COUNT);
    assert_eq!(
        report
            .receipt
            .segment_acks
            .iter()
            .map(|ack| ack.row_count)
            .sum::<u64>(),
        ROW_COUNT
    );
}

fn assert_replay_inputs_match_run(cell: RunMatrixCell, report: &ProjectRunReport) {
    let replay_inputs = PackageReader::open(&report.package_dir)
        .unwrap()
        .replay_inputs()
        .unwrap();
    assert_eq!(replay_inputs.state_delta, report.checkpoint.delta);
    assert_eq!(
        replay_inputs.destination_commit.package_hash,
        report.package_hash
    );
    assert_eq!(
        replay_inputs.destination_commit.target,
        report.receipt.target
    );
    assert_eq!(
        replay_inputs.destination_commit.disposition,
        cell.disposition.to_write_disposition()
    );
    assert_eq!(
        replay_inputs.destination_commit.idempotency_token.as_str(),
        report.package_hash.as_str()
    );
    assert_eq!(replay_inputs.schema_hash, report.receipt.schema_hash);
    assert_eq!(replay_inputs.merge_keys, vec!["id".to_owned()]);
}

fn assert_committed_checkpoint(state_store_path: &Path, report: &ProjectRunReport) {
    let store = SqliteCheckpointStore::open(state_store_path).unwrap();
    let head = store
        .head(
            &report.checkpoint.delta.pipeline_id,
            &report.checkpoint.delta.resource_id,
            &report.checkpoint.delta.scope,
        )
        .unwrap()
        .expect("checkpoint head");
    assert_eq!(head.status, CheckpointStatus::Committed);
    assert!(head.is_head);
    assert_eq!(head.delta, report.checkpoint.delta);
    assert_eq!(head.receipt.as_ref(), Some(&report.receipt));
}

fn assert_file_source_position(report: &ProjectRunReport) {
    let SourcePosition::FileManifest(manifest) = &report.checkpoint.delta.output_position else {
        panic!("run matrix file source must checkpoint a FileManifest");
    };
    assert_eq!(manifest.version, 1);
    assert_eq!(manifest.files.len(), 1);
    let file = &manifest.files[0];
    assert!(file.path.ends_with(SOURCE_POSITION_PATH));
    assert_eq!(file.size_bytes, SOURCE_SIZE_BYTES);
    assert_eq!(file.sha256.as_deref(), Some(SOURCE_SHA256));
    for segment in &report.checkpoint.delta.segments {
        assert_eq!(segment.scope, report.checkpoint.delta.scope);
        assert_eq!(
            segment.output_position,
            report.checkpoint.delta.output_position
        );
    }
}

fn assert_duplicate_replay_noop(
    cell: RunMatrixCell,
    destination: &MatrixDestinationHandle,
    report: &ProjectRunReport,
    root: &Path,
) -> Result<String> {
    assert_sheet_idempotency_is_package_token(cell.destination);
    let before = destination.footprint()?;
    let duplicate_store = SqliteCheckpointStore::open(root.join(format!(
        ".cdf/duplicate-{}.sqlite",
        cell.disposition.as_str()
    )))?;
    let duplicate = replay_package_from_artifacts(PackageArtifactReplayRequest {
        package_dir: report.package_dir.clone(),
        destination: destination.resolved()?,
        checkpoint_store: &duplicate_store,
        after_receipt_verified: None,
    })?;
    let after = destination.footprint()?;

    assert_eq!(
        before, after,
        "duplicate replay must not mutate destination"
    );
    assert_eq!(duplicate.checkpoint.delta, report.checkpoint.delta);
    assert_eq!(duplicate.checkpoint.status, CheckpointStatus::Committed);
    assert_receipt_core_identity(&report.receipt, &duplicate.receipt);
    assert_eq!(duplicate.receipt, report.receipt);
    let duplicate_head = duplicate_store
        .head(
            &duplicate.checkpoint.delta.pipeline_id,
            &duplicate.checkpoint.delta.resource_id,
            &duplicate.checkpoint.delta.scope,
        )?
        .expect("duplicate checkpoint head");
    assert_eq!(duplicate_head.status, CheckpointStatus::Committed);
    destination.verify_trait_receipt(&duplicate.receipt)?;

    Ok(match (cell.destination, duplicate.receipt_source) {
        (
            MatrixDestination::DuckDb | MatrixDestination::ParquetFilesystem,
            ProjectReceiptSource::DestinationCommit {
                duplicate: true,
                package_receipt_recorded: false,
            },
        ) => "no-op duplicate: destination sheet idempotency=package_token, runtime reported duplicate=true, destination footprint unchanged".to_owned(),
        (
            MatrixDestination::Postgres,
            ProjectReceiptSource::DestinationCommitReceiptOnly {
                package_receipt_recorded: false,
            },
        ) => "no-op duplicate: Postgres sheet idempotency=package_token, receipt-only runtime returned the stable receipt and destination footprint unchanged".to_owned(),
        (_, source) => panic!("unexpected duplicate receipt source: {source:?}"),
    })
}

fn assert_artifact_replay_identity(
    cell: RunMatrixCell,
    destination: &MatrixDestinationHandle,
    report: &ProjectRunReport,
    root: &Path,
) -> Result<()> {
    let replay_package_dir = root
        .join(".cdf/replay-packages")
        .join(format!("{}-copy", report.package_id));
    copy_dir_all(&report.package_dir, &replay_package_dir)?;
    let replay_destination = destination.fresh_artifact_replay_destination(root)?;
    let replay_store = SqliteCheckpointStore::open(root.join(format!(
        ".cdf/artifact-{}.sqlite",
        cell.disposition.as_str()
    )))?;
    let replay = replay_package_from_artifacts(PackageArtifactReplayRequest {
        package_dir: replay_package_dir.clone(),
        destination: replay_destination.resolved()?,
        checkpoint_store: &replay_store,
        after_receipt_verified: None,
    })?;

    assert_eq!(replay.package_status, PackageStatus::Checkpointed);
    assert_eq!(replay.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(replay.checkpoint.delta, report.checkpoint.delta);
    assert_receipt_core_identity(&report.receipt, &replay.receipt);
    replay_destination.verify_trait_receipt(&replay.receipt)?;
    PackageReader::open(&replay_package_dir)?.verify()?;
    let replay_head = replay_store
        .head(
            &replay.checkpoint.delta.pipeline_id,
            &replay.checkpoint.delta.resource_id,
            &replay.checkpoint.delta.scope,
        )?
        .expect("artifact replay checkpoint head");
    assert_eq!(replay_head.status, CheckpointStatus::Committed);
    Ok(())
}

fn assert_receipt_core_identity(expected: &Receipt, actual: &Receipt) {
    assert_eq!(actual.receipt_id, expected.receipt_id);
    assert_eq!(actual.destination, expected.destination);
    assert_eq!(actual.target, expected.target);
    assert_eq!(actual.package_hash, expected.package_hash);
    assert_eq!(actual.schema_hash, expected.schema_hash);
    assert_eq!(actual.disposition, expected.disposition);
    assert_eq!(actual.idempotency_token, expected.idempotency_token);
    assert_eq!(actual.segment_acks, expected.segment_acks);
    assert_eq!(actual.counts, expected.counts);
}

fn file_engine_plan_json(package_id: &str, disposition: MatrixDisposition) -> serde_json::Value {
    let delivery_guarantee = match disposition {
        MatrixDisposition::Append => "effectively_once_per_package",
        MatrixDisposition::Replace => "effectively_once_per_target",
        MatrixDisposition::Merge => "effectively_once_per_key",
    };
    let validation_program = validation_program_json();
    let scope = json!({ "kind": "file", "path": SOURCE_POSITION_PATH });
    let scan = json!({
        "plan_id": format!("plan-{RESOURCE_ID}"),
        "request": {
            "resource_id": RESOURCE_ID,
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
                "glob": SOURCE_POSITION_PATH,
                "resource_id": RESOURCE_ID,
            },
        }],
        "pushed_predicates": [],
        "unsupported_predicates": [],
        "estimated_rows": null,
        "estimated_bytes": null,
        "delivery_guarantee": delivery_guarantee,
    });
    let operator_chain = json!([
        {
            "kind": "cdf_resource_adapter",
            "adapter_kind": "cdf_native_resource_adapter",
            "resource_id": RESOURCE_ID,
        },
        {
            "kind": "cdf_native_scan",
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
            "resource_id": RESOURCE_ID,
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
                    "glob": SOURCE_POSITION_PATH,
                    "resource_id": RESOURCE_ID,
                },
            }],
            "estimates": {
                "support": "bytes",
                "rows": null,
                "bytes": null,
            },
            "delivery_guarantee": delivery_guarantee,
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

fn list_relative_files(root: &Path) -> Result<Vec<FileFootprint>> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut files = Vec::new();
    collect_relative_files(root, root, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_relative_files(
    root: &Path,
    current: &Path,
    files: &mut Vec<FileFootprint>,
) -> Result<()> {
    for entry in fs::read_dir(current)
        .map_err(|error| CdfError::data(format!("read {}: {error}", current.display())))?
    {
        let entry = entry.map_err(|error| {
            CdfError::data(format!("read entry in {}: {error}", current.display()))
        })?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|error| {
            CdfError::data(format!("read file type for {}: {error}", path.display()))
        })?;
        if file_type.is_dir() {
            collect_relative_files(root, &path, files)?;
        } else if file_type.is_file() {
            let relative = path.strip_prefix(root).map_err(|error| {
                CdfError::data(format!("relativize {}: {error}", path.display()))
            })?;
            let metadata = fs::metadata(&path).map_err(|error| {
                CdfError::data(format!("read metadata for {}: {error}", path.display()))
            })?;
            files.push(FileFootprint {
                path: relative.display().to_string(),
                size_bytes: metadata.len(),
            });
        }
    }
    Ok(())
}

fn postgres_footprint(
    database_url: &str,
    schema: &str,
    table: &str,
) -> Result<DestinationFootprint> {
    let mut client = Client::connect(database_url, NoTls)
        .map_err(|error| CdfError::destination(format!("connect to Postgres: {error}")))?;
    let target_rows = query_count(&mut client, &qualified_name(schema, table))?;
    let loads_rows = query_count(&mut client, &qualified_name(schema, "_cdf_loads"))?;
    let state_rows = query_count(&mut client, &qualified_name(schema, "_cdf_state"))?;
    Ok(DestinationFootprint::Postgres {
        target_rows,
        loads_rows,
        state_rows,
    })
}

fn query_count(client: &mut Client, table: &str) -> Result<i64> {
    client
        .query_one(&format!("SELECT COUNT(*)::bigint FROM {table}"), &[])
        .map(|row| row.get(0))
        .map_err(|error| CdfError::destination(format!("query row count from {table}: {error}")))
}

fn copy_dir_all(source: &Path, destination: &Path) -> Result<()> {
    fs::create_dir_all(destination)
        .map_err(|error| CdfError::data(format!("create {}: {error}", destination.display())))?;
    for entry in fs::read_dir(source)
        .map_err(|error| CdfError::data(format!("read {}: {error}", source.display())))?
    {
        let entry = entry.map_err(|error| {
            CdfError::data(format!("read entry in {}: {error}", source.display()))
        })?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let file_type = entry.file_type().map_err(|error| {
            CdfError::data(format!(
                "read file type for {}: {error}",
                source_path.display()
            ))
        })?;
        if file_type.is_dir() {
            copy_dir_all(&source_path, &destination_path)?;
        } else if file_type.is_file() {
            fs::copy(&source_path, &destination_path).map_err(|error| {
                CdfError::data(format!(
                    "copy {} to {}: {error}",
                    source_path.display(),
                    destination_path.display()
                ))
            })?;
        }
    }
    Ok(())
}

struct LivePostgres {
    url: String,
    schema: String,
    _server: Option<LocalPostgres>,
}

struct LocalPostgres {
    data_dir: TempDir,
    _socket_dir: TempDir,
    pg_ctl: PathBuf,
    port: u16,
}

impl LivePostgres {
    fn start() -> Result<Self> {
        let (url, server) = match env::var("TEST_DATABASE_URL") {
            Ok(url) if !url.trim().is_empty() => (url, None),
            _ => {
                let server = LocalPostgres::start()?;
                (server.url(), Some(server))
            }
        };
        let schema = format!(
            "cdf_conformance_run_matrix_{}_{}",
            std::process::id(),
            LIVE_POSTGRES_SCHEMA_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        create_postgres_schema(&url, &schema)?;
        Ok(Self {
            url,
            schema,
            _server: server,
        })
    }
}

impl Drop for LivePostgres {
    fn drop(&mut self) {
        if let Ok(mut client) = Client::connect(&self.url, NoTls) {
            let _ = client.batch_execute(&format!(
                "DROP SCHEMA IF EXISTS {} CASCADE",
                quote_identifier(&self.schema)
            ));
        }
    }
}

impl LocalPostgres {
    fn start() -> Result<Self> {
        let _guard = LOCAL_POSTGRES_START.lock().unwrap();
        let initdb = find_binary("initdb").ok_or_else(|| {
            CdfError::data("C1 run matrix requires initdb on PATH or TEST_DATABASE_URL")
        })?;
        let pg_ctl = find_binary("pg_ctl").ok_or_else(|| {
            CdfError::data("C1 run matrix requires pg_ctl on PATH or TEST_DATABASE_URL")
        })?;
        let data_dir = tempfile::tempdir()
            .map_err(|error| CdfError::data(format!("create Postgres data dir: {error}")))?;
        let socket_dir = tempfile::tempdir()
            .map_err(|error| CdfError::data(format!("create Postgres socket dir: {error}")))?;
        let port = free_port().ok_or_else(|| CdfError::data("allocate local Postgres port"))?;
        let data_dir_str = data_dir.path().to_str().ok_or_else(|| {
            CdfError::data(format!(
                "local Postgres data dir is not UTF-8: {}",
                data_dir.path().display()
            ))
        })?;

        let init_status = Command::new(&initdb)
            .args(["-D", data_dir_str])
            .args(["-A", "trust"])
            .args(["-U", "cdf"])
            .arg("--no-sync")
            .status()
            .map_err(|error| CdfError::destination(format!("run initdb: {error}")))?;
        if !init_status.success() {
            return Err(CdfError::destination(format!(
                "initdb failed with status {init_status}"
            )));
        }

        let options = format!("-h 127.0.0.1 -p {port} -k {}", socket_dir.path().display());
        let log_path = data_dir.path().join("postgres.log");
        let log_path_str = log_path.to_str().ok_or_else(|| {
            CdfError::data(format!(
                "local Postgres log path is not UTF-8: {}",
                log_path.display()
            ))
        })?;
        let start_status = Command::new(&pg_ctl)
            .args(["-D", data_dir_str])
            .args(["-l", log_path_str])
            .args(["-o", &options])
            .args(["-w", "start"])
            .status()
            .map_err(|error| CdfError::destination(format!("run pg_ctl start: {error}")))?;
        if !start_status.success() {
            return Err(CdfError::destination(format!(
                "pg_ctl start failed with status {start_status}; log: {}",
                log_path.display()
            )));
        }

        Ok(Self {
            data_dir,
            _socket_dir: socket_dir,
            pg_ctl,
            port,
        })
    }

    fn url(&self) -> String {
        format!("postgresql://cdf@127.0.0.1:{}/postgres", self.port)
    }
}

impl Drop for LocalPostgres {
    fn drop(&mut self) {
        let _ = Command::new(&self.pg_ctl)
            .args(["-D", self.data_dir.path().to_str().unwrap()])
            .args(["-m", "fast"])
            .args(["-w", "stop"])
            .status();
    }
}

fn create_postgres_schema(database_url: &str, schema: &str) -> Result<()> {
    Client::connect(database_url, NoTls)
        .map_err(|error| CdfError::destination(format!("connect to Postgres: {error}")))?
        .batch_execute(&format!("CREATE SCHEMA {}", quote_identifier(schema)))
        .map_err(|error| CdfError::destination(format!("create Postgres schema: {error}")))
}

fn reset_postgres_schema(database_url: &str, schema: &str) -> Result<()> {
    let schema = quote_identifier(schema);
    Client::connect(database_url, NoTls)
        .map_err(|error| CdfError::destination(format!("connect to Postgres: {error}")))?
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}"
        ))
        .map_err(|error| CdfError::destination(format!("reset Postgres schema: {error}")))
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn qualified_name(schema: &str, table: &str) -> String {
    format!("{}.{}", quote_identifier(schema), quote_identifier(table))
}

fn find_binary(name: &str) -> Option<PathBuf> {
    env::var_os("PATH").and_then(|paths| {
        env::split_paths(&paths)
            .map(|path| path.join(name))
            .find(|candidate| candidate.is_file())
    })
}

fn free_port() -> Option<u16> {
    TcpListener::bind("127.0.0.1:0")
        .ok()?
        .local_addr()
        .ok()
        .map(|addr| addr.port())
}

fn assert_sheet_idempotency_is_package_token(destination: MatrixDestination) {
    let idempotency = match destination {
        MatrixDestination::DuckDb => {
            let temp = tempfile::tempdir().unwrap();
            DuckDbDestination::new(temp.path().join("sheet.duckdb"))
                .unwrap()
                .sheet()
                .idempotency
                .clone()
        }
        MatrixDestination::ParquetFilesystem => {
            let temp = tempfile::tempdir().unwrap();
            ParquetDestination::new_filesystem(temp.path())
                .unwrap()
                .sheet()
                .idempotency
                .clone()
        }
        MatrixDestination::Postgres => PostgresDestination::new().sheet().idempotency.clone(),
    };
    assert_eq!(idempotency, IdempotencySupport::PackageToken);
}
