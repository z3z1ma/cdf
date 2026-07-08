use cdf_declarative::{CompiledResource, SqlResource, SqlRuntimeDependencies};
use cdf_kernel::{CdfError, CursorValue, Result, SourcePosition};
use cdf_project::ProjectRunReport;

use super::{
    MatrixDisposition, RunMatrixCell, local_postgres::LivePostgres,
    test_support::StaticSecretProvider,
};

const RESOURCE_ID: &str = "postgres.events";
const SECRET_REF: &str = "secret://env/POSTGRES_URL";

pub(crate) fn resource(cell: RunMatrixCell, postgres: &LivePostgres) -> Result<SqlResource> {
    let table = format!(
        "sql_source_{}_{}",
        cell.destination.as_str(),
        cell.disposition.as_str()
    );
    let source_table = postgres.create_source_events_table(&table)?;
    let document = cdf_declarative::parse_toml(&resource_toml(cell.disposition, &source_table))?;
    let compiled = one_resource(cdf_declarative::compile_document(&document)?)?;
    compiled.to_sql_resource(SqlRuntimeDependencies::new().with_secret_provider(
        StaticSecretProvider::new([(SECRET_REF, postgres.url().to_owned())]),
    ))
}

pub(crate) fn assert_source_position(report: &ProjectRunReport) {
    let SourcePosition::Cursor(cursor) = &report.checkpoint.delta.output_position else {
        panic!("run matrix SQL source must checkpoint a cursor position");
    };
    assert_eq!(cursor.version, 1);
    assert_eq!(cursor.field, "updated_at");
    assert_eq!(cursor.value, CursorValue::I64(20));
}

fn one_resource(mut resources: Vec<CompiledResource>) -> Result<CompiledResource> {
    if resources.len() != 1 {
        return Err(CdfError::contract(format!(
            "run matrix expected one SQL resource, found {}",
            resources.len()
        )));
    }
    let resource = resources.remove(0);
    if resource.descriptor().resource_id.as_str() != RESOURCE_ID {
        return Err(CdfError::contract(format!(
            "run matrix compiled unexpected SQL resource {}",
            resource.descriptor().resource_id
        )));
    }
    Ok(resource)
}

fn resource_toml(disposition: MatrixDisposition, table: &str) -> String {
    format!(
        r#"
[source.warehouse]
kind = "sql"
connection = "{SECRET_REF}"
dialect = "postgres"

[resource.events]
id = "{RESOURCE_ID}"
table = "{table}"
primary_key = ["id"]
merge_key = ["id"]
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
