use std::{fs, path::Path};

#[cfg(test)]
use std::sync::Arc;

use cdf_declarative::CompiledResource;
use cdf_kernel::{CdfError, CursorValue, Result, SourcePosition};
use cdf_project::ProjectRunReport;
use cdf_runtime::SourceRegistry;
use rusqlite::{Connection, params};

use super::{MatrixDisposition, RunMatrixCell};

const RESOURCE_ID: &str = "warehouse.events";

pub(crate) fn resource(
    cell: &RunMatrixCell,
    project_root: &Path,
) -> Result<crate::source_fixture::ResolvedSourceFixture> {
    let data_dir = project_root.join("data");
    fs::create_dir_all(&data_dir).map_err(|error| {
        crate::conformance_private_io_error("create SQLite run-matrix data directory", error)
    })?;
    let file_name = format!(
        "sqlite-source-{}-{}.sqlite",
        cell.destination.as_str(),
        cell.disposition.as_str()
    );
    seed_database(&data_dir.join(&file_name))?;

    let mut registry = SourceRegistry::new();
    registry.register(cdf_source_sqlite::SqliteSourceDriver::new()?)?;
    let document = cdf_declarative::parse_toml(&resource_toml(
        cell.disposition,
        &format!("data/{file_name}"),
    ))?;
    let compiled = one_resource(cdf_declarative::compile_document(&registry, &document)?)?;
    crate::source_fixture::resolve_with_registry(
        &compiled,
        &registry,
        project_root,
        Default::default(),
    )
}

pub(crate) fn assert_source_position(report: &ProjectRunReport) {
    let SourcePosition::Cursor(cursor) = &report.checkpoint.delta.output_position else {
        panic!("run matrix SQLite source must checkpoint a cursor position");
    };
    assert_eq!(cursor.version, cdf_kernel::SOURCE_POSITION_VERSION);
    assert_eq!(cursor.field, "updated_at");
    assert_eq!(cursor.value, CursorValue::I64(20));
}

fn seed_database(path: &Path) -> Result<()> {
    let mut connection = Connection::open(path).map_err(|error| {
        CdfError::environment(format!("create SQLite run-matrix database: {error}"))
    })?;
    let transaction = connection.transaction().map_err(|error| {
        CdfError::environment(format!(
            "begin SQLite run-matrix fixture transaction: {error}"
        ))
    })?;
    transaction
        .execute_batch(
            "CREATE TABLE events (
                id INTEGER PRIMARY KEY,
                name TEXT,
                updated_at INTEGER NOT NULL
            ) STRICT;",
        )
        .map_err(|error| CdfError::data(format!("create SQLite run-matrix table: {error}")))?;
    for (id, name, updated_at) in [(1_i64, Some("ada"), 10_i64), (2, Some("grace"), 20)] {
        transaction
            .execute(
                "INSERT INTO events (id, name, updated_at) VALUES (?1, ?2, ?3)",
                params![id, name, updated_at],
            )
            .map_err(|error| CdfError::data(format!("seed SQLite run-matrix row: {error}")))?;
    }
    transaction.commit().map_err(|error| {
        CdfError::environment(format!(
            "commit SQLite run-matrix fixture transaction: {error}"
        ))
    })
}

fn one_resource(mut resources: Vec<CompiledResource>) -> Result<CompiledResource> {
    if resources.len() != 1 {
        return Err(CdfError::contract(format!(
            "run matrix expected one SQLite resource, found {}",
            resources.len()
        )));
    }
    let resource = resources.remove(0);
    if resource.descriptor().resource_id.as_str() != RESOURCE_ID {
        return Err(CdfError::contract(format!(
            "run matrix compiled unexpected SQLite resource {}",
            resource.descriptor().resource_id
        )));
    }
    Ok(resource)
}

fn resource_toml(disposition: MatrixDisposition, relative_path: &str) -> String {
    let keys = merge_keys(disposition);
    format!(
        r#"
[source.warehouse]
kind = "sqlite"
location = "sqlite://{relative_path}"

[resource.events]
table = "events"
stable_key = "id"
{keys}
cursor = {{ field = "updated_at", ordering = "exact", lag = "0ms" }}
write_disposition = "{}"
trust = "governed"
schema = {{ fields = [
  {{ name = "id", type = "int64", nullable = false }},
  {{ name = "name", type = "string", nullable = true }},
  {{ name = "updated_at", type = "int64", nullable = false }},
] }}
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

#[cfg(test)]
mod tests {
    use super::*;
    use cdf_http::{SecretProvider, SecretUri, SecretValue};
    use cdf_runtime::SourceResolutionContext;

    struct NoSecrets;

    impl SecretProvider for NoSecrets {
        fn resolve(&self, uri: &SecretUri) -> Result<SecretValue> {
            Err(CdfError::auth(format!(
                "SQLite discovery-preview test has no secret for {uri}"
            )))
        }
    }

    #[test]
    fn discovered_primary_key_proof_survives_compilation_and_preview() {
        let temp = tempfile::tempdir().unwrap();
        let data = temp.path().join("data");
        fs::create_dir_all(&data).unwrap();
        seed_database(&data.join("events.sqlite")).unwrap();

        let mut registry = SourceRegistry::new();
        registry
            .register(cdf_source_sqlite::SqliteSourceDriver::new().unwrap())
            .unwrap();
        let document = cdf_declarative::parse_toml(
            r#"
[source.warehouse]
kind = "sqlite"
location = "sqlite://data/events.sqlite"

[resource.events]
table = "events"
stable_key = "id"
cursor = { field = "updated_at", ordering = "exact", lag = "0ms" }
write_disposition = "append"
trust = "governed"
schema_mode = "discover"
"#,
        )
        .unwrap();
        let provisional = one_resource(
            cdf_declarative::compile_document_with_project_root(&registry, &document, temp.path())
                .unwrap(),
        )
        .unwrap();
        assert!(provisional.schema().fields().is_empty());

        let execution = crate::test_execution_services();
        let context = SourceResolutionContext::new(
            temp.path(),
            Arc::new(NoSecrets),
            &execution,
            Arc::new(cdf_http::EgressAllowlist::allow_any()),
        );
        let mut artifacts = cdf_project::discover_resource_schema_with_source_registry(
            &provisional,
            &registry,
            provisional.source_plan(),
            &context,
            cdf_project::SchemaDiscoveryExecutionOptions::new(),
        )
        .unwrap();
        let prepared =
            cdf_project::compile_discovered_schema_artifacts(&provisional, &mut artifacts).unwrap();
        assert_eq!(
            prepared
                .schema()
                .field_with_name("id")
                .unwrap()
                .metadata()
                .get("cdf:sqlite_unique")
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            prepared
                .source_plan()
                .schema
                .field_with_name("id")
                .unwrap()
                .metadata()
                .get("cdf:sqlite_unique")
                .map(String::as_str),
            Some("true")
        );

        let resolved =
            crate::source_fixture::ResolvedSourceFixture::resolve(&prepared, &registry, &context)
                .unwrap();
        let plan = resolved
            .bind_plan(
                super::super::plan_json::planned_engine_plan(
                    resolved.queryable(),
                    "sqlite-discovered-pk-preview",
                    None,
                )
                .unwrap(),
            )
            .unwrap();
        let preview = futures_executor::block_on(cdf_engine::preview_resource(
            &plan,
            resolved.queryable(),
            cdf_engine::EnginePreviewLimits::default(),
        ))
        .unwrap();
        assert_eq!(preview.row_count, 2);
        assert!(preview.fields.iter().any(|field| field == "id"));
    }

    #[test]
    fn declared_schema_preview_reconciles_live_physical_catalog_through_type_policy() {
        let temp = tempfile::tempdir().unwrap();
        let data = temp.path().join("data");
        fs::create_dir_all(&data).unwrap();
        let database = data.join("events.sqlite");
        let connection = Connection::open(&database).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE events (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    updated_at INTEGER NOT NULL
                ) STRICT;
                INSERT INTO events (id, name, updated_at) VALUES
                    ('1', 'ada', 10),
                    ('2', 'grace', 20);",
            )
            .unwrap();
        drop(connection);

        let mut registry = SourceRegistry::new();
        registry
            .register(cdf_source_sqlite::SqliteSourceDriver::new().unwrap())
            .unwrap();
        let document = cdf_declarative::parse_toml(
            r#"
[source.warehouse]
kind = "sqlite"
location = "sqlite://data/events.sqlite"

[resource.events]
table = "events"
stable_key = "id"
cursor = { field = "updated_at", ordering = "exact", lag = "0ms" }
write_disposition = "append"
trust = "governed"
types = { coerce_types = true, allow_lossy_mapping = false }
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "name", type = "string", nullable = true },
  { name = "updated_at", type = "int64", nullable = false },
] }
"#,
        )
        .unwrap();
        let compiled = one_resource(
            cdf_declarative::compile_document_with_project_root(&registry, &document, temp.path())
                .unwrap(),
        )
        .unwrap();
        assert!(compiled.effective_schema_runtime().is_none());
        assert!(compiled.source_plan().type_policy_allowances.coerce_types);
        assert!(
            !compiled
                .source_plan()
                .type_policy_allowances
                .allow_lossy_mapping
        );

        let execution = crate::test_execution_services();
        let context = SourceResolutionContext::new(
            temp.path(),
            Arc::new(NoSecrets),
            &execution,
            Arc::new(cdf_http::EgressAllowlist::allow_any()),
        );
        let resolved =
            crate::source_fixture::ResolvedSourceFixture::resolve(&compiled, &registry, &context)
                .unwrap();
        let plan = resolved
            .bind_plan(
                super::super::plan_json::planned_engine_plan(
                    resolved.queryable(),
                    "sqlite-declared-live-physical-preview",
                    None,
                )
                .unwrap(),
            )
            .unwrap();
        let preview = futures_executor::block_on(cdf_engine::preview_resource(
            &plan,
            resolved.queryable(),
            cdf_engine::EnginePreviewLimits::default(),
        ))
        .unwrap();

        assert_eq!(preview.row_count, 2);
        assert_eq!(
            preview.fields,
            vec!["id", "name", "updated_at", "_cdf_variant"]
        );
        assert_eq!(preview.quarantined_row_count, 0);
        assert_eq!(preview.residual_row_count, 0);
    }
}
