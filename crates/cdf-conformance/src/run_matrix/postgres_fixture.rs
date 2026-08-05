use std::{collections::BTreeMap, fs, path::Path, sync::Arc};

use cdf_declarative::CompiledResource;
use cdf_kernel::{CdfError, CursorValue, Result, SourcePosition};
use cdf_project::ProjectRunReport;
use cdf_runtime::{SourceRegistry, SourceResolutionContext};

use super::{
    MatrixDisposition, RunMatrixCell, local_postgres::LivePostgres,
    test_support::StaticSecretProvider,
};

const RESOURCE_ID: &str = "warehouse.events";
const SECRET_REF: &str = "secret://env/POSTGRES_URL";

pub(crate) fn resource(
    cell: RunMatrixCell,
    project_root: &Path,
    postgres: &LivePostgres,
) -> Result<crate::source_fixture::ResolvedSourceFixture> {
    let table = format!(
        "postgres_source_{}_{}",
        cell.destination.as_str(),
        cell.disposition.as_str()
    );
    let source_table = postgres.create_source_events_table(&table)?;
    let mut registry = SourceRegistry::new();
    registry.register(cdf_source_postgres::PostgresSourceDriver::new()?)?;
    let project_toml = project_toml();
    write_query_project(project_root, &project_toml, cell.disposition, &source_table)?;
    let config = cdf_project::parse_cdf_toml(&project_toml)?;
    let destination =
        cdf_dest_duckdb::DuckDbDestination::new(project_root.join(".cdf/compile-only.duckdb"))?;
    let mut entries = cdf_project::compile_query_project_resources(
        &registry,
        &config,
        project_root,
        "dev",
        cdf_kernel::DestinationProtocol::sheet(&destination),
        &cdf_semantic::SemanticCatalog::builtins()?,
        &BTreeMap::new(),
    )?;
    if entries.len() != 1 {
        return Err(CdfError::contract(format!(
            "run matrix expected one Postgres query resource, found {}",
            entries.len()
        )));
    }
    let mut entry = entries.remove(0);
    let provisional = entry.resource.clone();
    let execution = crate::test_execution_services();
    let context = SourceResolutionContext::new(
        project_root,
        Arc::new(StaticSecretProvider::new([(
            SECRET_REF,
            postgres.url().to_owned(),
        )])),
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
    entry.resource =
        cdf_project::compile_discovered_schema_artifacts(&provisional, &mut discovery)?;
    entry = cdf_project::finalize_query_project_resource(
        entry,
        &cdf_semantic::SemanticCatalog::builtins()?,
    )?;
    one_resource(&entry.resource)?;
    crate::source_fixture::ResolvedSourceFixture::resolve(&entry.resource, &registry, &context)
}

pub(crate) fn assert_source_position(report: &ProjectRunReport) {
    let SourcePosition::Cursor(cursor) = &report.checkpoint.delta.output_position else {
        panic!("run matrix Postgres source must checkpoint a cursor position");
    };
    assert_eq!(cursor.version, cdf_kernel::SOURCE_POSITION_VERSION);
    assert_eq!(cursor.field, "updated_at");
    assert_eq!(cursor.value, CursorValue::I64(20));
}

pub(crate) fn make_runtime_id_physically_narrower(
    cell: &RunMatrixCell,
    postgres: &LivePostgres,
) -> Result<()> {
    let table = format!(
        "postgres_source_{}_{}",
        cell.destination.as_str(),
        cell.disposition.as_str()
    );
    postgres.alter_source_events_id_to_integer(&table)
}

fn one_resource(resource: &CompiledResource) -> Result<()> {
    if resource.descriptor().resource_id.as_str() != RESOURCE_ID {
        return Err(CdfError::contract(format!(
            "run matrix compiled unexpected Postgres resource {}",
            resource.descriptor().resource_id
        )));
    }
    Ok(())
}

fn project_toml() -> String {
    format!(
        r#"[project]
name = "postgres_run_matrix"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.sqlite"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[sources.warehouse]
type = "postgres"
connection = "{SECRET_REF}"
dialect = "postgres"
"#
    )
}

fn write_query_project(
    project_root: &Path,
    project_toml: &str,
    disposition: MatrixDisposition,
    table: &str,
) -> Result<()> {
    let directory = project_root.join("cdf/warehouse");
    fs::create_dir_all(&directory).map_err(|error| {
        crate::conformance_private_io_error("create Postgres query resource directory", error)
    })?;
    fs::write(project_root.join("cdf.toml"), project_toml).map_err(|error| {
        crate::conformance_private_io_error("write Postgres query project", error)
    })?;
    let disposition = match disposition {
        MatrixDisposition::Append => "APPEND",
        MatrixDisposition::Replace => "REPLACE",
        MatrixDisposition::Merge => "MERGE(id)",
    };
    let sql = format!(
        r#"RESOURCE
TARGET events
DISPOSITION {disposition}
CURSOR updated_at
TRUST GOVERNED
EXECUTION BOUNDED
AS
SELECT id, name, updated_at
FROM upstream(source => 'warehouse', table => '{table}');
"#
    );
    fs::write(directory.join("events.cdf.sql"), sql).map_err(|error| {
        crate::conformance_private_io_error("write Postgres query resource", error)
    })
}
