use std::{path::Path, sync::Arc};

use cdf_declarative::CompiledResource;
use cdf_kernel::{
    CdfError, CheckpointId, CheckpointStore, CursorValue, PipelineId, Result, RunId, SourcePosition,
};
use cdf_project::{ProjectRunReport, ProjectRunRequest, ProjectRunSource, run_project};
use cdf_runtime::{SourceRegistry, SourceResolutionContext};
use cdf_state_sqlite::SqliteCheckpointStore;

use super::{
    MatrixDestination, MatrixDisposition, RunMatrixCell, SourceArchetype,
    destinations::{ConformanceEnvironment, destination_for_cell},
    plan_json,
    test_support::StaticSecretProvider,
};

const RESOURCE_ID: &str = "warehouse.events";
const ENDPOINT_ENV: &str = "CDF_CLICKHOUSE_ENDPOINT";

pub(crate) fn resource(
    cell: &RunMatrixCell,
) -> Result<crate::source_fixture::ResolvedSourceFixture> {
    let endpoint = std::env::var(ENDPOINT_ENV).map_err(|_| {
        CdfError::environment(format!(
            "ClickHouse run-matrix shard requires {ENDPOINT_ENV}=clickhouse://host:port"
        ))
    })?;
    let table = format!(
        "cdf_source_{}_{}",
        cell.destination.as_str(),
        cell.disposition.as_str()
    );
    seed_table(&endpoint, &table)?;
    resolve_table(cell, &endpoint, &table)
}

fn resolve_table(
    cell: &RunMatrixCell,
    endpoint: &str,
    table: &str,
) -> Result<crate::source_fixture::ResolvedSourceFixture> {
    let mut registry = SourceRegistry::new();
    registry.register(cdf_source_clickhouse::ClickHouseSourceDriver::new()?)?;
    let document = cdf_declarative::parse_toml(&resource_toml(cell.disposition, endpoint, table))?;
    let provisional = one_resource(cdf_declarative::compile_document(&registry, &document)?)?;
    let execution = crate::test_execution_services();
    let context = SourceResolutionContext::new(
        Path::new("."),
        Arc::new(StaticSecretProvider::new(Vec::<(String, String)>::new())),
        &execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    );
    let mut discovery = cdf_project::discover_resource_schema_with_source_registry(
        &provisional,
        &registry,
        provisional.source_plan(),
        &context,
        cdf_project::SchemaDiscoveryExecutionOptions::new(),
    )?;
    let compiled = cdf_project::compile_discovered_schema_artifacts(&provisional, &mut discovery)?;
    crate::source_fixture::ResolvedSourceFixture::resolve(&compiled, &registry, &context)
}

pub(crate) fn assert_source_position(report: &ProjectRunReport) {
    let SourcePosition::Cursor(cursor) = &report.checkpoint.delta.output_position else {
        panic!("run matrix ClickHouse source must checkpoint a cursor position");
    };
    assert_eq!(cursor.version, cdf_kernel::SOURCE_POSITION_VERSION);
    assert_eq!(cursor.field, "updated_at");
    assert_eq!(cursor.value, CursorValue::I64(20));
}

#[test]
#[ignore = "requires digest-pinned ClickHouse and the local destination conformance services"]
fn project_checkpoint_atomicity_laws() {
    let environment = ConformanceEnvironment::start().expect(
        "ClickHouse project laws require Postgres coverage; set TEST_DATABASE_URL or install initdb/pg_ctl",
    );
    assert_project_limit_atomicity(&environment)
        .expect("ClickHouse project/package atomicity laws must pass");
}

fn assert_project_limit_atomicity(environment: &ConformanceEnvironment) -> Result<()> {
    let endpoint = std::env::var(ENDPOINT_ENV).map_err(|_| {
        CdfError::environment(format!(
            "ClickHouse project law requires {ENDPOINT_ENV}=clickhouse://host:port"
        ))
    })?;
    let cell = RunMatrixCell::new(
        SourceArchetype::clickhouse(),
        MatrixDestination::new("duckdb")?,
        MatrixDisposition::Append,
    );
    let temp = tempfile::tempdir()
        .map_err(|error| crate::conformance_host_error("create ClickHouse law tempdir", error))?;
    let table = "cdf_source_duckdb_append";
    let source = resource(&cell)?;
    seed_equal_cursor_group(&endpoint, table)?;
    let destination = destination_for_cell(&cell, temp.path(), environment)?;
    let before = destination.footprint()?;
    let resolved = destination.resolved()?;
    let identifier_policy = resolved.column_identifier_policy()?;
    let package_root = temp.path().join(".cdf/packages");
    let state_store_path = temp.path().join(".cdf/state.sqlite");
    let pipeline_id = PipelineId::new("pipeline-clickhouse-limit-law")?;
    let package_id = "clickhouse-limit-law";
    let limited = plan_json::planned_engine_plan_with_limit(
        source.queryable(),
        package_id,
        identifier_policy.as_ref(),
        Some(1),
    )?;
    let limited = source.bind_plan(limited)?;
    let error = futures_executor::block_on(run_project(
        ProjectRunRequest {
            resource: ProjectRunSource::new(source.queryable()),
            plan: limited,
            package_root: package_root.clone(),
            state_store_path: state_store_path.clone(),
            state_store_path_ownership: cdf_project::StateStorePathOwnership::Configured,
            pipeline_id: pipeline_id.clone(),
            package_id: package_id.to_owned(),
            checkpoint_id: CheckpointId::new("checkpoint-clickhouse-limit-law")?,
            destination: resolved,
            run_id: Some(RunId::new("run-clickhouse-limit-law")?),
            event_sink: None,
            after_receipt_verified: None,
        },
        source.execution(),
    ))
    .expect_err("cursor limit that bisects an equal-cursor group must fail checkpointing closed");
    if !error
        .message
        .contains("partial or limited source execution cannot advance state")
    {
        return Err(CdfError::internal(format!(
            "ClickHouse cursor-limit project law failed for an unexpected reason: {error}"
        )));
    }
    if destination.footprint()? != before {
        return Err(CdfError::internal(
            "ClickHouse limited cursor run changed destination publication",
        ));
    }
    let store = SqliteCheckpointStore::open(&state_store_path)?;
    if store
        .head(
            &pipeline_id,
            &source.queryable().descriptor().resource_id,
            &source.queryable().descriptor().state_scope,
        )?
        .is_some()
    {
        return Err(CdfError::internal(
            "ClickHouse limited cursor run advanced durable checkpoint state",
        ));
    }

    let destination = destination_for_cell(&cell, temp.path(), environment)?;
    let resolved = destination.resolved()?;
    let identifier_policy = resolved.column_identifier_policy()?;
    let resumed_package_id = "clickhouse-limit-law-resumed";
    let resumed = plan_json::planned_engine_plan_with_limit(
        source.queryable(),
        resumed_package_id,
        identifier_policy.as_ref(),
        None,
    )?;
    let resumed = source.bind_plan(resumed)?;
    let report = futures_executor::block_on(run_project(
        ProjectRunRequest {
            resource: ProjectRunSource::new(source.queryable()),
            plan: resumed,
            package_root,
            state_store_path,
            state_store_path_ownership: cdf_project::StateStorePathOwnership::Configured,
            pipeline_id,
            package_id: resumed_package_id.to_owned(),
            checkpoint_id: CheckpointId::new("checkpoint-clickhouse-limit-law-resumed")?,
            destination: resolved,
            run_id: Some(RunId::new("run-clickhouse-limit-law-resumed")?),
            event_sink: None,
            after_receipt_verified: None,
        },
        source.execution(),
    ))?
    .into_committed()?;
    if report.row_count != 4 {
        return Err(CdfError::internal(format!(
            "ClickHouse cursor-limit resume published {} rows instead of the complete equal-cursor groups",
            report.row_count
        )));
    }
    let SourcePosition::Cursor(cursor) = report.checkpoint.delta.output_position else {
        return Err(CdfError::internal(
            "ClickHouse cursor-limit resume omitted its cursor checkpoint",
        ));
    };
    if cursor.value != CursorValue::I64(20) {
        return Err(CdfError::internal(
            "ClickHouse cursor-limit resume checkpointed an unexpected frontier",
        ));
    }

    assert_partial_stream_project_atomicity(environment, &endpoint)?;
    Ok(())
}

fn assert_partial_stream_project_atomicity(
    environment: &ConformanceEnvironment,
    endpoint: &str,
) -> Result<()> {
    let source_cell = RunMatrixCell::new(
        SourceArchetype::clickhouse(),
        MatrixDestination::new("partial")?,
        MatrixDisposition::Append,
    );
    let table = "cdf_source_partial_append";
    seed_partial_table(endpoint, table)?;
    let source = resolve_table(&source_cell, endpoint, table)?;
    let destination_cell = RunMatrixCell::new(
        SourceArchetype::clickhouse(),
        MatrixDestination::new("duckdb")?,
        MatrixDisposition::Append,
    );
    let temp = tempfile::tempdir().map_err(|error| {
        crate::conformance_host_error("create ClickHouse partial-law tempdir", error)
    })?;
    let destination = destination_for_cell(&destination_cell, temp.path(), environment)?;
    let before = destination.footprint()?;
    let resolved = destination.resolved()?;
    let identifier_policy = resolved.column_identifier_policy()?;
    let plan = plan_json::planned_engine_plan(
        source.queryable(),
        "clickhouse-partial-law",
        identifier_policy.as_ref(),
    )?;
    let plan = source.bind_plan(plan)?;
    let pipeline_id = PipelineId::new("pipeline-clickhouse-partial-law")?;
    let state_store_path = temp.path().join(".cdf/state.sqlite");
    let error = futures_executor::block_on(run_project(
        ProjectRunRequest {
            resource: ProjectRunSource::new(source.queryable()),
            plan,
            package_root: temp.path().join(".cdf/packages"),
            state_store_path: state_store_path.clone(),
            state_store_path_ownership: cdf_project::StateStorePathOwnership::Configured,
            pipeline_id: pipeline_id.clone(),
            package_id: "clickhouse-partial-law".to_owned(),
            checkpoint_id: CheckpointId::new("checkpoint-clickhouse-partial-law")?,
            destination: resolved,
            run_id: Some(RunId::new("run-clickhouse-partial-law")?),
            event_sink: None,
            after_receipt_verified: None,
        },
        source.execution(),
    ))
    .expect_err("ClickHouse post-batch failure must fail the project run");
    if error.kind != cdf_kernel::ErrorKind::Data {
        return Err(CdfError::internal(format!(
            "ClickHouse partial project law returned unexpected error ownership: {error}"
        )));
    }
    if destination.footprint()? != before {
        return Err(CdfError::internal(
            "ClickHouse partial project run changed destination publication",
        ));
    }
    let store = SqliteCheckpointStore::open(&state_store_path)?;
    if store
        .head(
            &pipeline_id,
            &source.queryable().descriptor().resource_id,
            &source.queryable().descriptor().state_scope,
        )?
        .is_some()
    {
        return Err(CdfError::internal(
            "ClickHouse partial project run advanced durable checkpoint state",
        ));
    }
    Ok(())
}

fn seed_partial_table(endpoint: &str, table: &str) -> Result<()> {
    let client = clickhouse::Client::default()
        .with_url(operational_endpoint(endpoint)?)
        .with_database("default");
    let table = table.to_owned();
    crate::test_execution_services().run_io(async move {
        client
            .query(&format!("DROP TABLE IF EXISTS `{table}`"))
            .execute()
            .await
            .map_err(|_| CdfError::environment("reset ClickHouse partial project table"))?;
        client
            .query(&format!(
                "CREATE TABLE `{table}` (\
                 id Int64, \
                 name UUID ALIAS toUUID('550e8400-e29b-41d4-a716-446655440000'), \
                 updated_at Int64, payload String, \
                 fault UInt8 ALIAS if(sleepEachRow(0.03) = 0, throwIf(id >= 3, 'cdf project fixture'), 0)) ENGINE = TinyLog"
            ))
            .execute()
            .await
            .map_err(|error| clickhouse_fixture_query_error("create partial project table", error))?;
        for id in 0..3 {
            client
                .query(&format!(
                    "INSERT INTO `{table}` (id, updated_at, payload) SELECT {id}, {id}, randomString(4194304)"
                ))
                .execute()
                .await
                .map_err(|_| CdfError::environment("seed ClickHouse partial project block"))?;
        }
        client
            .query(&format!(
                "INSERT INTO `{table}` (id, updated_at, payload) SELECT 3, 3, randomString(1)"
            ))
            .execute()
            .await
            .map_err(|_| CdfError::environment("seed ClickHouse partial project fault"))
    })
}

fn clickhouse_fixture_query_error(action: &str, error: clickhouse::error::Error) -> CdfError {
    if let clickhouse::error::Error::BadResponse(message) = error {
        let code = message.split_once("Code:").and_then(|(_, remainder)| {
            let digits = remainder
                .trim_start()
                .chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>();
            (!digits.is_empty())
                .then(|| digits.parse::<u32>().ok())
                .flatten()
        });
        return CdfError::data(format!(
            "ClickHouse fixture {action} was rejected{}",
            code.map_or_else(String::new, |code| format!(" with server code {code}"))
        ));
    }
    CdfError::environment(format!(
        "ClickHouse fixture {action} failed before a stable server response"
    ))
}

fn seed_equal_cursor_group(endpoint: &str, table: &str) -> Result<()> {
    let http = operational_endpoint(endpoint)?;
    let client = clickhouse::Client::default()
        .with_url(http)
        .with_database("default");
    let table = table.to_owned();
    crate::test_execution_services().run_io(async move {
        client
            .query(&format!("TRUNCATE TABLE `{table}`"))
            .execute()
            .await
            .map_err(|_| CdfError::environment("truncate ClickHouse cursor-limit fixture"))?;
        client
            .query(&format!(
                "INSERT INTO `{table}` VALUES \
                 (1, '550e8400-e29b-41d4-a716-446655440000', 10), \
                 (2, '123e4567-e89b-12d3-a456-426614174000', 10), \
                 (3, 'f47ac10b-58cc-4372-a567-0e02b2c3d479', 20), \
                 (4, '6ba7b810-9dad-11d1-80b4-00c04fd430c8', 20)"
            ))
            .execute()
            .await
            .map_err(|_| CdfError::environment("seed ClickHouse equal-cursor fixture"))
    })
}

fn operational_endpoint(endpoint: &str) -> Result<String> {
    endpoint
        .strip_prefix("clickhouse://")
        .map(|authority| format!("http://{authority}"))
        .or_else(|| {
            endpoint
                .strip_prefix("clickhouses://")
                .map(|authority| format!("https://{authority}"))
        })
        .ok_or_else(|| CdfError::contract("ClickHouse fixture endpoint must use clickhouse(s)://"))
}

fn seed_table(endpoint: &str, table: &str) -> Result<()> {
    let http = endpoint
        .strip_prefix("clickhouse://")
        .map(|authority| format!("http://{authority}"))
        .or_else(|| {
            endpoint
                .strip_prefix("clickhouses://")
                .map(|authority| format!("https://{authority}"))
        })
        .ok_or_else(|| {
            CdfError::contract("ClickHouse fixture endpoint must use clickhouse(s)://")
        })?;
    let client = clickhouse::Client::default()
        .with_url(http)
        .with_database("default");
    let table = table.to_owned();
    crate::test_execution_services().run_io(async move {
        client
            .query(&format!("DROP TABLE IF EXISTS `{table}`"))
            .execute()
            .await
            .map_err(|_| CdfError::environment("reset ClickHouse run-matrix fixture table"))?;
        client
            .query(&format!(
                "CREATE TABLE `{table}` (id Int64, name UUID, updated_at Int64) ENGINE = Memory"
            ))
            .execute()
            .await
            .map_err(|_| CdfError::environment("create ClickHouse run-matrix fixture table"))?;
        client
            .query(&format!(
                "INSERT INTO `{table}` VALUES \
                 (1, '550e8400-e29b-41d4-a716-446655440000', 10), \
                 (2, '123e4567-e89b-12d3-a456-426614174000', 20)"
            ))
            .execute()
            .await
            .map_err(|_| CdfError::environment("seed ClickHouse run-matrix fixture table"))?;
        Ok(())
    })
}

fn one_resource(mut resources: Vec<CompiledResource>) -> Result<CompiledResource> {
    if resources.len() != 1 {
        return Err(CdfError::contract(format!(
            "run matrix expected one ClickHouse resource, found {}",
            resources.len()
        )));
    }
    let resource = resources.remove(0);
    if resource.descriptor().resource_id.as_str() != RESOURCE_ID {
        return Err(CdfError::contract(format!(
            "run matrix compiled unexpected ClickHouse resource {}",
            resource.descriptor().resource_id
        )));
    }
    Ok(resource)
}

fn resource_toml(disposition: MatrixDisposition, endpoint: &str, table: &str) -> String {
    let keys = merge_keys(disposition);
    format!(
        r#"
[source.warehouse]
kind = "clickhouse"
endpoint = "{endpoint}"
database = "default"
max_threads = 2
max_block_rows = 1024
stream_buffer_batches = 1

[resource.events]
table = "{table}"
stable_key = "id"
{keys}
cursor = {{ field = "updated_at", ordering = "exact", lag = "0ms" }}
write_disposition = "{}"
trust = "governed"
schema_mode = "discover"
"#,
        disposition.as_str()
    )
}

fn merge_keys(disposition: MatrixDisposition) -> &'static str {
    if disposition == MatrixDisposition::Merge {
        "primary_key = [\"id\"]\nmerge_key = [\"id\"]"
    } else {
        ""
    }
}
