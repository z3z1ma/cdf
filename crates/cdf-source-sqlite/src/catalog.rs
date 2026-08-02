use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    time::Duration,
};

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use cdf_kernel::{CdfError, ResourceId, Result, with_physical_type};
use rusqlite::{Connection, OpenFlags, params};

use crate::{
    error::{classify_sqlite_error, classify_sqlite_open_error, validate_source_file},
    identifier::SqliteIdentifier,
};

pub(crate) const SQLITE_STRICT_METADATA_KEY: &str = "cdf:sqlite_strict";
pub(crate) const SQLITE_TEMPORAL_ENCODING_METADATA_KEY: &str = "cdf:sqlite_temporal_encoding";
pub(crate) const SQLITE_UNIQUE_METADATA_KEY: &str = "cdf:sqlite_unique";

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SqliteCatalogDiscovery {
    pub schema: Schema,
    pub source_identity: BTreeMap<String, String>,
    pub strict: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CatalogColumn {
    name: String,
    declared_type: String,
    nullable: bool,
    hidden: i64,
}

pub(crate) fn discover_sqlite_table(
    database_path: &Path,
    resource_id: &ResourceId,
    table: &SqliteIdentifier,
) -> Result<SqliteCatalogDiscovery> {
    validate_source_file(database_path)?;
    let connection = open_read_only(database_path, "open SQLite catalog for schema discovery")?;
    discover_sqlite_table_on_connection(&connection, resource_id, table)
}

pub(crate) fn discover_sqlite_table_on_connection(
    connection: &Connection,
    resource_id: &ResourceId,
    table: &SqliteIdentifier,
) -> Result<SqliteCatalogDiscovery> {
    let strict = connection
        .query_row(
            "SELECT strict FROM pragma_table_list WHERE schema = 'main' AND name = ?1 AND type = 'table'",
            params![table.as_str()],
            |row| row.get::<_, i64>(0),
        )
        .map(|value| value != 0)
        .map_err(|error| {
            if matches!(error, rusqlite::Error::QueryReturnedNoRows) {
                CdfError::data(format!(
                    "SQLite catalog discovery for resource `{resource_id}` found no table named `{table}`"
                ))
            } else {
                classify_sqlite_error("inspect SQLite table catalog", error)
            }
        })?;
    let mut statement = connection
        .prepare(
            "SELECT name, type, \"notnull\", pk, hidden FROM pragma_table_xinfo(?1) ORDER BY cid",
        )
        .map_err(|error| classify_sqlite_error("prepare SQLite catalog query", error))?;
    let columns = statement
        .query_map(params![table.as_str()], |row| {
            let not_null = row.get::<_, i64>(2)? != 0;
            let primary_key_position = row.get::<_, i64>(3)?;
            Ok(CatalogColumn {
                name: row.get(0)?,
                declared_type: row.get(1)?,
                nullable: !not_null && primary_key_position == 0,
                hidden: row.get(4)?,
            })
        })
        .map_err(|error| classify_sqlite_error("query SQLite catalog columns", error))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| classify_sqlite_error("decode SQLite catalog columns", error))?;
    let unique_columns = unique_single_columns(connection, table)?;
    let schema = schema_from_columns(resource_id, columns, strict, &unique_columns)?;
    let source_identity = BTreeMap::from([
        ("source_kind".to_owned(), "sqlite".to_owned()),
        ("dialect".to_owned(), "sqlite".to_owned()),
        ("table".to_owned(), table.as_str().to_owned()),
        ("strict".to_owned(), strict.to_string()),
        (
            "unique_columns".to_owned(),
            unique_columns.iter().cloned().collect::<Vec<_>>().join(","),
        ),
    ]);
    Ok(SqliteCatalogDiscovery {
        schema,
        source_identity,
        strict,
    })
}

pub(crate) fn open_read_only(path: &Path, action: &str) -> Result<Connection> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| classify_sqlite_open_error(action, path, error))?;
    connection
        .busy_timeout(Duration::ZERO)
        .map_err(|error| classify_sqlite_error("configure bounded SQLite busy handling", error))?;
    Ok(connection)
}

fn schema_from_columns(
    resource_id: &ResourceId,
    columns: Vec<CatalogColumn>,
    strict: bool,
    unique_columns: &BTreeSet<String>,
) -> Result<Schema> {
    let visible = columns
        .into_iter()
        .filter(|column| column.hidden != 1)
        .collect::<Vec<_>>();
    if visible.is_empty() {
        return Err(CdfError::data(format!(
            "SQLite catalog discovery for resource `{resource_id}` found no visible columns"
        )));
    }
    let fields = visible
        .into_iter()
        .map(|column| {
            SqliteIdentifier::new(column.name.clone())?;
            let data_type = arrow_type_for_declared_type(&column.declared_type).ok_or_else(|| {
                CdfError::data(format!(
                    "SQLite catalog discovery for resource `{resource_id}` cannot infer column `{}` from declared type `{}`; pin an explicit supported Arrow schema",
                    column.name, column.declared_type
                ))
            })?;
            let is_temporal = matches!(data_type, DataType::Date32 | DataType::Timestamp(..));
            let mut field = with_physical_type(
                Field::new(&column.name, data_type, column.nullable),
                column.declared_type,
            );
            let mut metadata = field.metadata().clone();
            metadata.insert(SQLITE_STRICT_METADATA_KEY.to_owned(), strict.to_string());
            if unique_columns.contains(&column.name) {
                metadata.insert(SQLITE_UNIQUE_METADATA_KEY.to_owned(), "true".to_owned());
            }
            if is_temporal {
                metadata.insert(
                    SQLITE_TEMPORAL_ENCODING_METADATA_KEY.to_owned(),
                    "iso8601_text".to_owned(),
                );
            }
            field = field.with_metadata(metadata);
            Ok(field)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Schema::new(fields))
}

pub(crate) fn unique_single_columns(
    connection: &Connection,
    table: &SqliteIdentifier,
) -> Result<BTreeSet<String>> {
    let mut unique = BTreeSet::new();
    let mut columns_statement = connection
        .prepare("SELECT name, pk FROM pragma_table_xinfo(?1) WHERE hidden != 1 ORDER BY cid")
        .map_err(|error| {
            classify_sqlite_error("prepare SQLite primary-key catalog query", error)
        })?;
    let primary_key = columns_statement
        .query_map(params![table.as_str()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .map_err(|error| classify_sqlite_error("query SQLite primary-key catalog", error))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| classify_sqlite_error("decode SQLite primary-key catalog", error))?
        .into_iter()
        .filter(|(_, position)| *position > 0)
        .collect::<Vec<_>>();
    if primary_key.len() == 1 {
        unique.insert(primary_key[0].0.clone());
    }

    let mut index_statement = connection
        .prepare(
            "SELECT name FROM pragma_index_list(?1) WHERE \"unique\" = 1 AND partial = 0 ORDER BY seq",
        )
        .map_err(|error| classify_sqlite_error("prepare SQLite unique-index catalog query", error))?;
    let indexes = index_statement
        .query_map(params![table.as_str()], |row| row.get::<_, String>(0))
        .map_err(|error| classify_sqlite_error("query SQLite unique-index catalog", error))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| classify_sqlite_error("decode SQLite unique-index catalog", error))?;
    for index in indexes {
        let mut info_statement = connection
            .prepare("SELECT name FROM pragma_index_info(?1) ORDER BY seqno")
            .map_err(|error| {
                classify_sqlite_error("prepare SQLite index-column catalog query", error)
            })?;
        let columns = info_statement
            .query_map(params![index], |row| row.get::<_, Option<String>>(0))
            .map_err(|error| classify_sqlite_error("query SQLite index-column catalog", error))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| classify_sqlite_error("decode SQLite index-column catalog", error))?;
        if let [Some(column)] = columns.as_slice() {
            unique.insert(column.clone());
        }
    }
    Ok(unique)
}

pub(crate) fn validate_live_unique_stable_key(
    connection: &Connection,
    table: &SqliteIdentifier,
    stable_key: &SqliteIdentifier,
) -> Result<()> {
    if unique_single_columns(connection, table)?.contains(stable_key.as_str()) {
        Ok(())
    } else {
        Err(CdfError::data(format!(
            "SQLite stable_key `{stable_key}` is no longer backed by a single-column PRIMARY KEY or UNIQUE constraint"
        )))
    }
}

fn arrow_type_for_declared_type(declared_type: &str) -> Option<DataType> {
    let normalized = declared_type.trim().to_ascii_uppercase();
    if normalized.is_empty() {
        return None;
    }
    if normalized.contains("BOOL") {
        Some(DataType::Boolean)
    } else if normalized == "DATE" {
        Some(DataType::Date32)
    } else if normalized.contains("TIMESTAMP") || normalized.contains("DATETIME") {
        Some(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        ))
    } else if normalized.contains("INT") {
        Some(DataType::Int64)
    } else if normalized.contains("CHAR")
        || normalized.contains("CLOB")
        || normalized.contains("TEXT")
        || normalized.contains("JSON")
    {
        Some(DataType::Utf8)
    } else if normalized.contains("REAL")
        || normalized.contains("FLOA")
        || normalized.contains("DOUB")
        || normalized.contains("NUMERIC")
        || normalized.contains("DECIMAL")
    {
        Some(DataType::Float64)
    } else if normalized.contains("BLOB") {
        Some(DataType::Binary)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovery_retains_names_types_nullability_and_strict_authority() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("catalog.sqlite");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE events (\"Event ID\" INTEGER PRIMARY KEY, label TEXT, score REAL NOT NULL) STRICT;",
            )
            .unwrap();
        drop(connection);

        let discovery = discover_sqlite_table(
            &path,
            &ResourceId::new("local.events").unwrap(),
            &SqliteIdentifier::new("events").unwrap(),
        )
        .unwrap();

        assert!(discovery.strict);
        assert_eq!(discovery.schema.fields()[0].name(), "Event ID");
        assert!(!discovery.schema.fields()[0].is_nullable());
        assert_eq!(discovery.schema.fields()[1].data_type(), &DataType::Utf8);
        assert!(discovery.schema.fields()[1].is_nullable());
        assert_eq!(
            discovery.schema.fields()[2].metadata()[SQLITE_STRICT_METADATA_KEY],
            "true"
        );
        assert_eq!(
            discovery.schema.fields()[0].metadata()[SQLITE_UNIQUE_METADATA_KEY],
            "true"
        );
        assert_eq!(discovery.source_identity["unique_columns"], "Event ID");
    }

    #[test]
    fn discovery_only_attests_single_column_nonpartial_uniqueness() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("unique.sqlite");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE events (
                    id INTEGER NOT NULL,
                    tenant_id INTEGER NOT NULL,
                    external_id TEXT NOT NULL,
                    optional_code TEXT
                 ) STRICT;
                 CREATE UNIQUE INDEX events_external_id_unique ON events(external_id);
                 CREATE UNIQUE INDEX events_tenant_id_unique ON events(tenant_id, id);
                 CREATE UNIQUE INDEX events_optional_code_partial ON events(optional_code)
                    WHERE optional_code IS NOT NULL;",
            )
            .unwrap();
        drop(connection);

        let discovery = discover_sqlite_table(
            &path,
            &ResourceId::new("local.events").unwrap(),
            &SqliteIdentifier::new("events").unwrap(),
        )
        .unwrap();
        let metadata = |name: &str| {
            discovery
                .schema
                .field_with_name(name)
                .unwrap()
                .metadata()
                .get(SQLITE_UNIQUE_METADATA_KEY)
                .cloned()
        };
        assert_eq!(metadata("external_id").as_deref(), Some("true"));
        assert_eq!(metadata("id"), None);
        assert_eq!(metadata("tenant_id"), None);
        assert_eq!(metadata("optional_code"), None);
        assert_eq!(discovery.source_identity["unique_columns"], "external_id");
    }

    #[test]
    fn missing_database_and_table_are_data_owned_without_path_disclosure() {
        let missing = std::path::Path::new("/definitely/not/a/cdf/sqlite/database");
        let error = discover_sqlite_table(
            missing,
            &ResourceId::new("local.events").unwrap(),
            &SqliteIdentifier::new("events").unwrap(),
        )
        .unwrap_err();
        assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
        assert!(!error.message.contains("/definitely/not"));
    }
}
