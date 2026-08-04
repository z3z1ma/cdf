use std::collections::BTreeMap;

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use cdf_kernel::{CdfError, ResourceId, Result, with_physical_type};
use cdf_runtime::SourceEgressScope;
use cdf_semantic::{
    POSTGRES_JSON_TEXT_SEMANTIC, POSTGRES_JSONB_TEXT_SEMANTIC, POSTGRES_NUMERIC_TEXT_SEMANTIC,
    SemanticAuthority, builtin_catalog,
};
use postgres::{Client, GenericClient, NoTls, Row};

use cdf_postgres::PostgresTarget;

use crate::error::classify_postgres_error;

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
    pub numeric_precision: Option<i32>,
    pub numeric_scale: Option<i32>,
    pub nullable: bool,
}

pub fn discover_postgres_table_catalog_schema(
    database_url: &str,
    resource_id: &ResourceId,
    target: &PostgresTarget,
    egress: &SourceEgressScope,
) -> Result<PostgresCatalogDiscovery> {
    if database_url.trim().is_empty() {
        return Err(CdfError::auth(
            "Postgres source connection string resolved to an empty value",
        ));
    }

    egress.authorize(database_url)?;
    let mut client = Client::connect(database_url, NoTls).map_err(|error| {
        classify_postgres_error("connect to Postgres catalog for schema discovery", error)
    })?;
    let columns = read_catalog_columns(&mut client, target)?;
    let schema = schema_from_catalog_columns(resource_id, columns)?;
    let source_identity = BTreeMap::from([
        ("source_kind".to_owned(), "postgres".to_owned()),
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

    let semantic_catalog = builtin_catalog()?;
    let fields = columns
        .into_iter()
        .map(|column| {
            let (data_type, semantic) =
                arrow_type_for_catalog_column(&column).ok_or_else(|| {
                    unsupported_catalog_type(resource_id, &column.name, &column.observed_type)
                })?;
            let physical_type = column.physical_type();
            let field = with_physical_type(
                Field::new(&column.name, data_type, column.nullable),
                physical_type,
            );
            match semantic {
                Some(semantic) => {
                    semantic_catalog.apply_reference(field, semantic, SemanticAuthority::Observed)
                }
                None => Ok(field),
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Schema::new(fields))
}

pub(crate) fn arrow_type_for_catalog_type(observed_type: &str) -> Option<DataType> {
    let normalized = observed_type.trim().to_ascii_lowercase();
    if normalized.starts_with("timestamp(") {
        if normalized.ends_with(" without time zone") {
            return Some(DataType::Timestamp(TimeUnit::Microsecond, None));
        }
        if normalized.ends_with(" with time zone") {
            return Some(DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into()),
            ));
        }
    }
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
        "text" | "character varying" | "varchar" | "character" | "char" | "uuid" | "json"
        | "jsonb" => Some(DataType::Utf8),
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

fn arrow_type_for_catalog_column(
    column: &PostgresCatalogColumn,
) -> Option<(DataType, Option<&'static str>)> {
    match catalog_base_type(&column.observed_type) {
        "json" => Some((DataType::Utf8, Some(POSTGRES_JSON_TEXT_SEMANTIC))),
        "jsonb" => Some((DataType::Utf8, Some(POSTGRES_JSONB_TEXT_SEMANTIC))),
        "numeric" | "decimal" => {
            let Some((precision, scale)) = column.arrow_decimal_precision_scale() else {
                return Some((DataType::Utf8, Some(POSTGRES_NUMERIC_TEXT_SEMANTIC)));
            };
            let data_type = if precision <= 38 && (-38..=38).contains(&scale) {
                DataType::Decimal128(precision, scale)
            } else if precision <= 76 && (-76..=76).contains(&scale) {
                DataType::Decimal256(precision, scale)
            } else {
                return Some((DataType::Utf8, Some(POSTGRES_NUMERIC_TEXT_SEMANTIC)));
            };
            Some((data_type, None))
        }
        _ => arrow_type_for_catalog_type(&column.observed_type).map(|data_type| (data_type, None)),
    }
}

impl PostgresCatalogColumn {
    pub(crate) fn is_numeric(&self) -> bool {
        matches!(
            catalog_base_type(&self.observed_type),
            "numeric" | "decimal"
        )
    }

    pub(crate) fn arrow_decimal_precision_scale(&self) -> Option<(u8, i8)> {
        let precision = u8::try_from(self.numeric_precision?).ok()?;
        let scale = i8::try_from(self.numeric_scale?).ok()?;
        if precision == 0 || (scale > 0 && scale.unsigned_abs() > precision) {
            return None;
        }
        Some((precision, scale))
    }

    pub(crate) fn physical_type(&self) -> String {
        match (self.numeric_precision, self.numeric_scale) {
            (Some(precision), Some(scale)) if self.is_numeric() => {
                format!("numeric({precision},{scale})")
            }
            _ => self.observed_type.clone(),
        }
    }
}

fn catalog_base_type(observed_type: &str) -> &str {
    observed_type
        .trim()
        .split_once('(')
        .map(|(base, _)| base.trim())
        .unwrap_or_else(|| observed_type.trim())
}

pub(crate) fn read_catalog_columns<C: GenericClient>(
    client: &mut C,
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
                "SELECT a.attname, NOT a.attnotnull, ",
                "pg_catalog.format_type(a.atttypid, a.atttypmod), ",
                "CASE WHEN t.typname = 'numeric' AND a.atttypmod >= 4 ",
                "THEN (((a.atttypmod - 4) >> 16) & 65535)::integer END, ",
                "CASE WHEN t.typname = 'numeric' AND a.atttypmod >= 4 THEN ",
                "CASE WHEN ((a.atttypmod - 4) & 2047) > 1023 ",
                "THEN (((a.atttypmod - 4) & 2047) - 2048)::integer ",
                "ELSE ((a.atttypmod - 4) & 2047)::integer END END ",
                "FROM pg_catalog.pg_attribute a ",
                "JOIN pg_catalog.pg_class c ON c.oid = a.attrelid ",
                "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace ",
                "JOIN pg_catalog.pg_type t ON t.oid = a.atttypid ",
                "WHERE n.nspname = COALESCE($1::text, current_schema()) ",
                "AND c.relname = $2::text AND a.attnum > 0 AND NOT a.attisdropped ",
                "ORDER BY a.attnum"
            ),
            &[&schema, &table],
        )
        .map_err(|error| {
            classify_postgres_error("query Postgres catalog columns for schema discovery", error)
        })?;
    Ok(rows.iter().map(catalog_column_from_row).collect())
}

fn catalog_column_from_row(row: &Row) -> PostgresCatalogColumn {
    PostgresCatalogColumn {
        name: row.get(0),
        observed_type: row.get(2),
        numeric_precision: row.get(3),
        numeric_scale: row.get(4),
        nullable: row.get(1),
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn maps_supported_catalog_types_to_arrow() {
        let schema = schema_from_catalog_columns(
            &ResourceId::new("warehouse.orders").unwrap(),
            vec![
                PostgresCatalogColumn {
                    name: "VendorID".to_owned(),
                    observed_type: "integer".to_owned(),
                    numeric_precision: None,
                    numeric_scale: None,
                    nullable: false,
                },
                PostgresCatalogColumn {
                    name: "is_active".to_owned(),
                    observed_type: "boolean".to_owned(),
                    numeric_precision: None,
                    numeric_scale: None,
                    nullable: true,
                },
                PostgresCatalogColumn {
                    name: "ratio".to_owned(),
                    observed_type: "double precision".to_owned(),
                    numeric_precision: None,
                    numeric_scale: None,
                    nullable: false,
                },
                PostgresCatalogColumn {
                    name: "customer_uuid".to_owned(),
                    observed_type: "uuid".to_owned(),
                    numeric_precision: None,
                    numeric_scale: None,
                    nullable: true,
                },
                PostgresCatalogColumn {
                    name: "service_date".to_owned(),
                    observed_type: "date".to_owned(),
                    numeric_precision: None,
                    numeric_scale: None,
                    nullable: false,
                },
                PostgresCatalogColumn {
                    name: "created_at".to_owned(),
                    observed_type: "timestamp without time zone".to_owned(),
                    numeric_precision: None,
                    numeric_scale: None,
                    nullable: true,
                },
                PostgresCatalogColumn {
                    name: "updated_at".to_owned(),
                    observed_type: "timestamp(3) with time zone".to_owned(),
                    numeric_precision: None,
                    numeric_scale: None,
                    nullable: false,
                },
            ],
        )
        .unwrap();

        let fields = schema.fields();
        assert_eq!(fields[0].data_type(), &DataType::Int64);
        assert!(!fields[0].is_nullable());
        assert_eq!(fields[0].metadata()["cdf:physical_type"], "integer");
        assert_eq!(fields[1].data_type(), &DataType::Boolean);
        assert!(fields[1].is_nullable());
        assert_eq!(fields[2].data_type(), &DataType::Float64);
        assert_eq!(fields[3].data_type(), &DataType::Utf8);
        assert_eq!(fields[4].data_type(), &DataType::Date32);
        assert_eq!(
            fields[5].data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            fields[6].data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn maps_json_and_numeric_without_losing_exact_values() {
        let schema = schema_from_catalog_columns(
            &ResourceId::new("warehouse.orders").unwrap(),
            vec![
                PostgresCatalogColumn {
                    name: "payload".to_owned(),
                    observed_type: "jsonb".to_owned(),
                    numeric_precision: None,
                    numeric_scale: None,
                    nullable: false,
                },
                PostgresCatalogColumn {
                    name: "amount".to_owned(),
                    observed_type: "numeric".to_owned(),
                    numeric_precision: Some(38),
                    numeric_scale: Some(9),
                    nullable: false,
                },
                PostgresCatalogColumn {
                    name: "wide".to_owned(),
                    observed_type: "numeric".to_owned(),
                    numeric_precision: Some(60),
                    numeric_scale: Some(18),
                    nullable: true,
                },
                PostgresCatalogColumn {
                    name: "unbounded".to_owned(),
                    observed_type: "numeric".to_owned(),
                    numeric_precision: None,
                    numeric_scale: None,
                    nullable: true,
                },
            ],
        )
        .unwrap();
        let fields = schema.fields();
        assert_eq!(fields[0].data_type(), &DataType::Utf8);
        assert_eq!(
            fields[0].metadata()["cdf:semantic"],
            POSTGRES_JSONB_TEXT_SEMANTIC
        );
        assert_eq!(fields[1].data_type(), &DataType::Decimal128(38, 9));
        assert_eq!(fields[2].data_type(), &DataType::Decimal256(60, 18));
        assert_eq!(fields[3].data_type(), &DataType::Utf8);
        assert_eq!(
            fields[3].metadata()["cdf:semantic"],
            POSTGRES_NUMERIC_TEXT_SEMANTIC
        );
        assert_eq!(fields[2].metadata()["cdf:physical_type"], "numeric(60,18)");
    }

    #[test]
    fn rejects_unsupported_type_with_resource_and_column() {
        let error = schema_from_catalog_columns(
            &ResourceId::new("warehouse.orders").unwrap(),
            vec![PostgresCatalogColumn {
                name: "payload".to_owned(),
                observed_type: "bytea".to_owned(),
                numeric_precision: None,
                numeric_scale: None,
                nullable: true,
            }],
        )
        .unwrap_err();

        let message = error.to_string();
        assert!(message.contains("warehouse.orders"));
        assert!(message.contains("payload"));
        assert!(message.contains("bytea"));
        assert!(message.contains("not yet supported by the Postgres discovery/execution slice"));
    }

    #[test]
    fn host_egress_denial_precedes_postgres_connection_attempt() {
        let egress = SourceEgressScope::new(
            cdf_runtime::SourceDriverId::new("postgres").unwrap(),
            Arc::new(cdf_http::EgressAllowlist::from_hosts([
                "host-permitted.example.org",
            ])),
        );

        let error = discover_postgres_table_catalog_schema(
            "postgres://operator:secret@127.0.0.1:1/catalog",
            &ResourceId::new("warehouse.orders").unwrap(),
            &PostgresTarget::parse("raw.orders").unwrap(),
            &egress,
        )
        .unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Auth);
        assert!(!error.to_string().contains("secret"));
    }
}
