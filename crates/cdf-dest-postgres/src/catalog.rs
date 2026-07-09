use std::collections::BTreeMap;

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use cdf_kernel::{CdfError, ResourceId, Result, with_physical_type};
use postgres::{Client, NoTls, Row};

use crate::PostgresTarget;

pub const POSTGRES_CATALOG_DISCOVERY_PROBE: &str = "postgres-catalog";

#[derive(Clone, Debug, PartialEq)]
pub struct PostgresCatalogDiscovery {
    pub schema: Schema,
    pub source_identity: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PostgresCatalogColumn {
    pub name: String,
    pub observed_type: String,
    pub nullable: bool,
}

pub fn discover_postgres_table_catalog_schema(
    database_url: &str,
    resource_id: &ResourceId,
    target: &PostgresTarget,
) -> Result<PostgresCatalogDiscovery> {
    if database_url.trim().is_empty() {
        return Err(CdfError::auth(
            "Postgres source connection string resolved to an empty value",
        ));
    }

    let mut client = Client::connect(database_url, NoTls)
        .map_err(|_| CdfError::transient("connect to Postgres catalog for schema discovery"))?;
    let columns = read_catalog_columns(&mut client, target)?;
    let schema = schema_from_catalog_columns(resource_id, columns)?;
    let source_identity = BTreeMap::from([
        ("source_kind".to_owned(), "sql".to_owned()),
        ("dialect".to_owned(), "postgres".to_owned()),
        ("table".to_owned(), target.display_name()),
    ]);
    Ok(PostgresCatalogDiscovery {
        schema,
        source_identity,
    })
}

pub(crate) fn schema_from_catalog_columns(
    resource_id: &ResourceId,
    columns: Vec<PostgresCatalogColumn>,
) -> Result<Schema> {
    if columns.is_empty() {
        return Err(CdfError::data(format!(
            "Postgres catalog discovery for resource `{resource_id}` found no columns for the configured table"
        )));
    }

    let fields = columns
        .into_iter()
        .map(|column| {
            let data_type =
                arrow_type_for_catalog_type(&column.observed_type).ok_or_else(|| {
                    unsupported_catalog_type(resource_id, &column.name, &column.observed_type)
                })?;
            Ok(with_physical_type(
                Field::new(&column.name, data_type, column.nullable),
                column.observed_type,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Schema::new(fields))
}

pub(crate) fn arrow_type_for_catalog_type(observed_type: &str) -> Option<DataType> {
    let normalized = observed_type.trim().to_ascii_lowercase();
    let base = normalized
        .split_once('(')
        .map(|(base, _)| base.trim())
        .unwrap_or_else(|| normalized.trim());
    match base {
        "boolean" | "bool" => Some(DataType::Boolean),
        "smallint" | "int2" | "integer" | "int" | "int4" | "bigint" | "int8" => {
            Some(DataType::Int64)
        }
        "real" | "float4" | "double precision" | "float8" => Some(DataType::Float64),
        "text" | "character varying" | "varchar" | "character" | "char" | "uuid" => {
            Some(DataType::Utf8)
        }
        "date" => Some(DataType::Date32),
        "timestamp without time zone" | "timestamp" => {
            Some(DataType::Timestamp(TimeUnit::Microsecond, None))
        }
        "timestamp with time zone" | "timestamptz" => Some(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        )),
        _ => None,
    }
}

fn read_catalog_columns(
    client: &mut Client,
    target: &PostgresTarget,
) -> Result<Vec<PostgresCatalogColumn>> {
    let schema = target
        .schema
        .as_ref()
        .map(|schema| schema.as_str().to_owned());
    let table = target.table.as_str().to_owned();
    let rows = client
        .query(
            concat!(
                "SELECT column_name, is_nullable, data_type ",
                "FROM information_schema.columns ",
                "WHERE table_schema = COALESCE($1::text, current_schema()) ",
                "AND table_name = $2::text ",
                "ORDER BY ordinal_position"
            ),
            &[&schema, &table],
        )
        .map_err(|_| CdfError::data("query Postgres catalog columns for schema discovery"))?;
    Ok(rows.iter().map(catalog_column_from_row).collect())
}

fn catalog_column_from_row(row: &Row) -> PostgresCatalogColumn {
    let nullable: String = row.get(1);
    PostgresCatalogColumn {
        name: row.get(0),
        observed_type: row.get(2),
        nullable: nullable.eq_ignore_ascii_case("YES"),
    }
}

fn unsupported_catalog_type(
    resource_id: &ResourceId,
    column_name: &str,
    observed_type: &str,
) -> CdfError {
    CdfError::data(format!(
        "Postgres catalog discovery for resource `{resource_id}` does not support column `{column_name}` with catalog type `{observed_type}`; this source type is not yet supported by the Postgres discovery/execution slice"
    ))
}
