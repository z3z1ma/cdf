use std::{collections::BTreeSet, path::Path, time::Duration};

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use cdf_kernel::{
    CdfError, ErrorKind, ForeignState, ResourceDescriptor, ResourceId, Result, SourcePosition,
    canonical_arrow_schema_hash, with_physical_type,
};
use cdf_runtime::artifact_hash;
use rusqlite::{
    Connection, OpenFlags, TransactionBehavior,
    hooks::{AuthAction, AuthContext, Authorization},
    types::ValueRef,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

use crate::{
    error::{classify_sqlite_error, classify_sqlite_open_error, validate_source_file},
    identifier::SqliteIdentifier,
};

pub(crate) const SQLITE_DEFAULT_DISCOVERY_RECORDS: u64 = 1_000;
pub(crate) const SQLITE_DEFAULT_DISCOVERY_BYTES: u64 = 16 * 1024 * 1024;
// This is the measured passing table-source default. Overrides are adapter-owned knobs.
pub(crate) const SQLITE_DEFAULT_OUTPUT_BATCH_ROWS: usize = 32 * 1024;
pub(crate) const SQLITE_SOURCE_GENERATION_SCHEMA_KEY: &str = "cdf.source.sqlite.source_generation";
pub(crate) const SQLITE_SOURCE_GENERATION_PROTOCOL: &str = "cdf.sqlite.source-generation.v1";
const MAXIMUM_QUERY_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum SqliteSourceInput {
    Table {
        table: SqliteIdentifier,
    },
    Query {
        #[serde(rename = "sql_base64", with = "query_bytes_base64")]
        sql: String,
        sha256: String,
    },
}

mod query_bytes_base64 {
    use super::*;

    pub(super) fn serialize<S>(sql: &String, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&BASE64_STANDARD.encode(sql.as_bytes()))
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> std::result::Result<String, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        let bytes = BASE64_STANDARD.decode(encoded).map_err(D::Error::custom)?;
        String::from_utf8(bytes).map_err(D::Error::custom)
    }
}

impl SqliteSourceInput {
    pub(crate) fn from_authored(table: Option<String>, query: Option<String>) -> Result<Self> {
        match (table, query) {
            (Some(table), None) => Ok(Self::Table {
                table: SqliteIdentifier::new(table)?,
            }),
            (None, Some(query)) => {
                let sql = validate_authored_query(&query)?;
                Ok(Self::Query {
                    sha256: artifact_hash(&sql)?,
                    sql,
                })
            }
            (Some(_), Some(_)) => Err(CdfError::contract(
                "SQLite resource must set exactly one of `table` or `query`, not both",
            )),
            (None, None) => Err(CdfError::contract(
                "SQLite resource must set exactly one of `table` or `query`",
            )),
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        match self {
            Self::Table { table } => {
                SqliteIdentifier::new(table.as_str())?;
            }
            Self::Query { sql, sha256 } => {
                if validate_authored_query(sql)? != *sql {
                    return Err(CdfError::contract(
                        "compiled SQLite native query is not canonical",
                    ));
                }
                if artifact_hash(sql)? != *sha256 {
                    return Err(CdfError::contract(
                        "compiled SQLite native query hash does not match its exact text",
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn relation_sql(&self) -> String {
        match self {
            Self::Table { table } => table.quoted(),
            Self::Query { sql, .. } => format!("({sql}) AS \"_cdf_native_query\""),
        }
    }

    pub(crate) fn location_summary(&self) -> String {
        match self {
            Self::Table { table } => table.as_str().to_owned(),
            Self::Query { sha256, .. } => format!("query:{sha256}"),
        }
    }

    pub(crate) fn redacted_evidence(&self) -> serde_json::Value {
        match self {
            Self::Table { table } => serde_json::json!({
                "kind": "table",
                "table": table.as_str(),
            }),
            Self::Query { sql, sha256 } => serde_json::json!({
                "kind": "query",
                "query_sha256": sha256,
                "query_bytes": sql.len(),
            }),
        }
    }

    pub(crate) fn table(&self) -> Option<&SqliteIdentifier> {
        match self {
            Self::Table { table } => Some(table),
            Self::Query { .. } => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SqliteNativeOptions {
    pub(crate) output_batch_rows: usize,
    pub(crate) busy_timeout_ms: Option<u64>,
    pub(crate) cache_kib: Option<u64>,
    pub(crate) mmap_bytes: Option<u64>,
}

impl Default for SqliteNativeOptions {
    fn default() -> Self {
        Self {
            output_batch_rows: SQLITE_DEFAULT_OUTPUT_BATCH_ROWS,
            busy_timeout_ms: None,
            cache_kib: None,
            mmap_bytes: None,
        }
    }
}

impl SqliteNativeOptions {
    pub(crate) fn from_authored(
        output_batch_rows: Option<u64>,
        busy_timeout_ms: Option<u64>,
        cache_kib: Option<u64>,
        mmap_bytes: Option<u64>,
    ) -> Result<Self> {
        let output_batch_rows = usize::try_from(
            output_batch_rows.unwrap_or(SQLITE_DEFAULT_OUTPUT_BATCH_ROWS as u64),
        )
        .map_err(|_| CdfError::contract("SQLite output_batch_rows exceeds platform bounds"))?;
        let options = Self {
            output_batch_rows,
            busy_timeout_ms,
            cache_kib,
            mmap_bytes,
        };
        options.validate()?;
        Ok(options)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        let output_batch_rows = u64::try_from(self.output_batch_rows)
            .map_err(|_| CdfError::contract("SQLite output_batch_rows exceeds u64 bounds"))?;
        validate_range("output_batch_rows", output_batch_rows, 1, 100_000)?;
        if let Some(value) = self.busy_timeout_ms {
            validate_range("busy_timeout_ms", value, 1, 3_600_000)?;
        }
        if let Some(value) = self.cache_kib {
            validate_range("cache_kib", value, 64, 1_048_576)?;
        }
        if let Some(value) = self.mmap_bytes {
            validate_range("mmap_bytes", value, 0, 1_073_741_824)?;
        }
        Ok(())
    }

    pub(crate) fn open_read_only(&self, path: &Path, action: &str) -> Result<Connection> {
        self.validate()?;
        validate_source_file(path)?;
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|error| classify_sqlite_open_error(action, path, error))?;
        connection
            .busy_timeout(Duration::from_millis(self.busy_timeout_ms.unwrap_or(0)))
            .map_err(|error| {
                classify_sqlite_error("configure SQLite source busy timeout", error)
            })?;
        connection
            .pragma_update(None, "query_only", true)
            .map_err(|error| classify_sqlite_error("enable SQLite query-only mode", error))?;
        if let Some(cache_kib) = self.cache_kib {
            let cache_kib = i64::try_from(cache_kib)
                .map_err(|_| CdfError::contract("SQLite cache_kib exceeds i64 bounds"))?;
            connection
                .pragma_update(None, "cache_size", -cache_kib)
                .map_err(|error| classify_sqlite_error("configure SQLite source cache", error))?;
        }
        if let Some(mmap_bytes) = self.mmap_bytes {
            let mmap_bytes = i64::try_from(mmap_bytes)
                .map_err(|_| CdfError::contract("SQLite mmap_bytes exceeds i64 bounds"))?;
            connection
                .pragma_update(None, "mmap_size", mmap_bytes)
                .map_err(|error| classify_sqlite_error("configure SQLite source mmap", error))?;
        }
        Ok(connection)
    }
}

fn validate_range(name: &str, value: u64, minimum: u64, maximum: u64) -> Result<()> {
    if !(minimum..=maximum).contains(&value) {
        return Err(CdfError::contract(format!(
            "SQLite {name} must be between {minimum} and {maximum}"
        )));
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) struct SqliteQueryDiscovery {
    pub(crate) schema: Schema,
    pub(crate) records_read: u64,
    pub(crate) bytes_read: u64,
    pub(crate) complete: bool,
}

pub(crate) fn discover_sqlite_query(
    database_path: &Path,
    resource_id: &ResourceId,
    input: &SqliteSourceInput,
    options: &SqliteNativeOptions,
    maximum_records: u64,
    maximum_bytes: u64,
) -> Result<SqliteQueryDiscovery> {
    let SqliteSourceInput::Query { sql, sha256 } = input else {
        return Err(CdfError::internal(
            "SQLite query discovery received a table input",
        ));
    };
    let record_limit = maximum_records;
    let byte_limit = maximum_bytes;
    let mut connection = options.open_read_only(
        database_path,
        "open SQLite native query for schema discovery",
    )?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Deferred)
        .map_err(|error| classify_sqlite_error("begin SQLite query discovery snapshot", error))?;
    install_read_query_authorizer(&transaction)?;
    let mut statement = transaction.prepare(sql).map_err(|error| {
        classify_authored_query_prepare_error("prepare SQLite native query", error)
    })?;
    if !statement.readonly() {
        return Err(CdfError::contract("SQLite native query must be read-only"));
    }
    if statement.parameter_count() != 0 {
        return Err(CdfError::contract(
            "SQLite native query parameters are not supported; author a complete read query",
        ));
    }
    transaction
        .authorizer(None::<fn(AuthContext<'_>) -> Authorization>)
        .map_err(|error| classify_sqlite_error("remove SQLite query authorizer", error))?;

    let columns = statement.columns();
    if columns.is_empty() {
        return Err(CdfError::data(format!(
            "SQLite native query for resource `{resource_id}` produced no columns"
        )));
    }
    let mut names = BTreeSet::new();
    let mut observed = Vec::with_capacity(columns.len());
    for column in columns {
        if !names.insert(column.name().to_owned()) {
            return Err(CdfError::data(format!(
                "SQLite native query for resource `{resource_id}` produced duplicate column `{}`; alias every output column uniquely",
                column.name()
            )));
        }
        SqliteIdentifier::new(column.name())?;
        observed.push(QueryColumnObservation::new(
            column.name(),
            column.decl_type(),
        ));
    }
    let mut rows = statement
        .query([])
        .map_err(|error| classify_sqlite_error("step SQLite query discovery", error))?;
    let mut records_read = 0_u64;
    let mut bytes_read = 0_u64;
    let mut complete = false;
    while records_read < record_limit && bytes_read < byte_limit {
        let Some(row) = rows
            .next()
            .map_err(|error| classify_sqlite_error("read SQLite query discovery row", error))?
        else {
            complete = true;
            break;
        };
        let mut row_bytes = 0_u64;
        for index in 0..observed.len() {
            let value = row.get_ref(index).map_err(|error| {
                classify_sqlite_error("read SQLite query discovery value", error)
            })?;
            row_bytes = row_bytes
                .checked_add(discovery_value_bytes(value))
                .ok_or_else(|| CdfError::data("SQLite query discovery byte count overflowed"))?;
        }
        if row_bytes > byte_limit {
            return Err(CdfError::data(format!(
                "SQLite native query for resource `{resource_id}` produced one discovery row larger than the {byte_limit}-byte discovery bound; project or cast smaller values"
            )));
        }
        if bytes_read.saturating_add(row_bytes) > byte_limit {
            break;
        }
        for (index, column) in observed.iter_mut().enumerate() {
            let value = row.get_ref(index).map_err(|error| {
                classify_sqlite_error("read SQLite query discovery value", error)
            })?;
            column.observe(value)?;
        }
        bytes_read += row_bytes;
        records_read += 1;
    }
    drop(rows);
    drop(statement);
    transaction
        .commit()
        .map_err(|error| classify_sqlite_error("close SQLite query discovery snapshot", error))?;

    let fields = observed
        .into_iter()
        .map(|column| column.into_field(resource_id))
        .collect::<Result<Vec<_>>>()?;
    let mut schema = Schema::new(fields);
    let _ = sha256;
    bind_source_generation(&mut schema, input, options)?;
    Ok(SqliteQueryDiscovery {
        schema,
        records_read,
        bytes_read,
        complete,
    })
}

pub(crate) fn bind_source_generation(
    schema: &mut Schema,
    input: &SqliteSourceInput,
    options: &SqliteNativeOptions,
) -> Result<()> {
    if let Some(existing) = schema.metadata().get(SQLITE_SOURCE_GENERATION_SCHEMA_KEY) {
        return validate_generation(existing);
    }
    let generation = match input {
        SqliteSourceInput::Table { .. } => {
            artifact_hash(&(SQLITE_SOURCE_GENERATION_PROTOCOL, input, options))?
        }
        SqliteSourceInput::Query { .. } => artifact_hash(&(
            SQLITE_SOURCE_GENERATION_PROTOCOL,
            input,
            options,
            canonical_arrow_schema_hash(schema)?.to_string(),
        ))?,
    };
    schema
        .metadata
        .insert(SQLITE_SOURCE_GENERATION_SCHEMA_KEY.to_owned(), generation);
    Ok(())
}

pub(crate) fn source_generation_from_schema(schema: &Schema) -> Result<&str> {
    schema
        .metadata()
        .get(SQLITE_SOURCE_GENERATION_SCHEMA_KEY)
        .map(String::as_str)
        .ok_or_else(|| {
            CdfError::data(
                "SQLite compiled schema omitted source-generation authority; compile the resource again",
            )
        })
}

pub(crate) fn sqlite_source_generation_position(
    descriptor: &ResourceDescriptor,
    generation: &str,
) -> Result<SourcePosition> {
    validate_generation(generation)?;
    let authority = (
        SQLITE_SOURCE_GENERATION_PROTOCOL,
        descriptor.resource_id.as_str(),
        generation,
    );
    let opaque_blob = serde_json::to_vec(&authority).map_err(|error| {
        CdfError::internal(format!(
            "serialize SQLite source-generation authority: {error}"
        ))
    })?;
    let position = SourcePosition::ForeignState(ForeignState {
        version: cdf_kernel::SOURCE_POSITION_VERSION,
        protocol: SQLITE_SOURCE_GENERATION_PROTOCOL.to_owned(),
        blob_sha256: artifact_hash(&authority)?,
        opaque_blob,
    });
    position.validate()?;
    Ok(position)
}

fn validate_generation(generation: &str) -> Result<()> {
    let Some(hex) = generation.strip_prefix("sha256:") else {
        return Err(CdfError::data(
            "SQLite source-generation identity must use sha256:<64 lowercase hex>",
        ));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(CdfError::data(
            "SQLite source-generation identity must use sha256:<64 lowercase hex>",
        ));
    }
    Ok(())
}

fn install_read_query_authorizer(connection: &Connection) -> Result<()> {
    connection
        .authorizer(Some(|context: AuthContext<'_>| match context.action {
            AuthAction::Read { .. } | AuthAction::Select | AuthAction::Recursive => {
                Authorization::Allow
            }
            AuthAction::Function { function_name }
                if !function_name.eq_ignore_ascii_case("load_extension") =>
            {
                Authorization::Allow
            }
            _ => Authorization::Deny,
        }))
        .map_err(|error| classify_sqlite_error("install SQLite query authorizer", error))
}

pub(crate) fn prepare_runtime_query<'connection>(
    connection: &'connection Connection,
    sql: &str,
) -> Result<rusqlite::Statement<'connection>> {
    install_read_query_authorizer(connection)?;
    let statement = connection.prepare(sql).map_err(|error| {
        classify_authored_query_prepare_error("prepare SQLite native query", error)
    })?;
    if !statement.readonly() {
        return Err(CdfError::contract("SQLite native query must be read-only"));
    }
    if statement.parameter_count() != 0 {
        return Err(CdfError::contract(
            "SQLite native query parameters are not supported; author a complete read query",
        ));
    }
    connection
        .authorizer(None::<fn(AuthContext<'_>) -> Authorization>)
        .map_err(|error| classify_sqlite_error("remove SQLite query authorizer", error))?;
    Ok(statement)
}

fn classify_authored_query_prepare_error(action: &str, error: rusqlite::Error) -> CdfError {
    if authored_query_contract_error(&error) {
        return CdfError::contract(format!(
            "{action}: query must be one read-only statement without PRAGMA, writes, attach/detach, or extension loading"
        ));
    }
    let mut classified = classify_sqlite_error(action, error);
    classified.message = match classified.kind {
        ErrorKind::Data => format!(
            "{action}: SQLite native query references missing or incompatible source input; inspect the query and database"
        ),
        ErrorKind::Transient | ErrorKind::RateLimited => format!(
            "{action}: SQLite source is temporarily unavailable while preparing the native query"
        ),
        ErrorKind::Environment => {
            format!("{action}: the host could not prepare the SQLite native query")
        }
        ErrorKind::Internal => {
            format!("{action}: CDF could not prepare the validated SQLite native query")
        }
        _ => format!("{action}: SQLite native query preparation failed"),
    };
    classified
}

fn authored_query_contract_error(error: &rusqlite::Error) -> bool {
    match error {
        rusqlite::Error::MultipleStatement | rusqlite::Error::InvalidQuery => true,
        rusqlite::Error::SqliteFailure(failure, message) => {
            failure.code == rusqlite::ffi::ErrorCode::AuthorizationForStatementDenied
                || message.as_deref().is_some_and(|message| {
                    let message = message.to_ascii_lowercase();
                    message.contains("syntax error")
                        || message.contains("incomplete input")
                        || message.contains("unrecognized token")
                        || message.contains("parse error")
                })
        }
        _ => false,
    }
}

fn validate_authored_query(query: &str) -> Result<String> {
    let mut sql = query.trim();
    if let Some(without_terminator) = sql.strip_suffix(';') {
        sql = without_terminator.trim_end();
    }
    if sql.is_empty() {
        return Err(CdfError::contract("SQLite native query cannot be empty"));
    }
    if sql.len() > MAXIMUM_QUERY_BYTES {
        return Err(CdfError::contract(
            "SQLite native query exceeds the 1 MiB authored-query bound",
        ));
    }
    if sql
        .chars()
        .any(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
    {
        return Err(CdfError::contract(
            "SQLite native query contains unsupported control characters",
        ));
    }
    Ok(sql.to_owned())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ObservedStorage {
    Integer,
    Real,
    Text,
    Blob,
}

struct QueryColumnObservation {
    name: String,
    declared_type: Option<String>,
    observed: Option<ObservedStorage>,
}

impl QueryColumnObservation {
    fn new(name: &str, declared_type: Option<&str>) -> Self {
        Self {
            name: name.to_owned(),
            declared_type: declared_type.map(str::to_owned),
            observed: None,
        }
    }

    fn observe(&mut self, value: ValueRef<'_>) -> Result<()> {
        let next = match value {
            ValueRef::Null => {
                return Ok(());
            }
            ValueRef::Integer(_) => ObservedStorage::Integer,
            ValueRef::Real(_) => ObservedStorage::Real,
            ValueRef::Text(_) => ObservedStorage::Text,
            ValueRef::Blob(_) => ObservedStorage::Blob,
        };
        self.observed = match (self.observed, next) {
            (None, next) => Some(next),
            (Some(value), next) if value == next => Some(next),
            (Some(ObservedStorage::Integer), ObservedStorage::Real)
            | (Some(ObservedStorage::Real), ObservedStorage::Integer) => {
                Some(ObservedStorage::Real)
            }
            (Some(previous), next) => {
                return Err(CdfError::data(format!(
                    "SQLite native query column `{}` mixed dynamic storage classes {previous:?} and {next:?} during discovery; cast the output to one stable type",
                    self.name
                )));
            }
        };
        Ok(())
    }

    fn into_field(self, resource_id: &ResourceId) -> Result<Field> {
        let declared_arrow = self
            .declared_type
            .as_deref()
            .and_then(arrow_type_for_declared_type);
        let data_type = match (declared_arrow, self.observed) {
            (Some(DataType::Boolean), Some(ObservedStorage::Integer)) => DataType::Boolean,
            (Some(DataType::Date32), Some(ObservedStorage::Text)) => DataType::Date32,
            (Some(DataType::Timestamp(unit, timezone)), Some(ObservedStorage::Text)) => {
                DataType::Timestamp(unit, timezone)
            }
            (Some(data_type), None) => data_type,
            (_, Some(ObservedStorage::Integer)) => DataType::Int64,
            (_, Some(ObservedStorage::Real)) => DataType::Float64,
            (_, Some(ObservedStorage::Text)) => DataType::Utf8,
            (_, Some(ObservedStorage::Blob)) => DataType::Binary,
            (None, None) => {
                return Err(CdfError::data(format!(
                    "SQLite native query for resource `{resource_id}` could not infer all-null column `{}`; cast it to a supported SQLite type",
                    self.name
                )));
            }
        };
        let physical = self
            .declared_type
            .unwrap_or_else(|| observed_type_name(self.observed).to_owned());
        let mut field =
            with_physical_type(Field::new(self.name, data_type.clone(), true), physical);
        if matches!(data_type, DataType::Date32 | DataType::Timestamp(..)) {
            let mut metadata = field.metadata().clone();
            metadata.insert(
                crate::catalog::SQLITE_TEMPORAL_ENCODING_METADATA_KEY.to_owned(),
                "iso8601_text".to_owned(),
            );
            field = field.with_metadata(metadata);
        }
        Ok(field)
    }
}

fn observed_type_name(observed: Option<ObservedStorage>) -> &'static str {
    match observed {
        Some(ObservedStorage::Integer) => "INTEGER",
        Some(ObservedStorage::Real) => "REAL",
        Some(ObservedStorage::Text) => "TEXT",
        Some(ObservedStorage::Blob) => "BLOB",
        None => "NULL",
    }
}

fn discovery_value_bytes(value: ValueRef<'_>) -> u64 {
    match value {
        ValueRef::Null => 1,
        ValueRef::Integer(_) | ValueRef::Real(_) => 8,
        ValueRef::Text(value) | ValueRef::Blob(value) => {
            u64::try_from(value.len()).unwrap_or(u64::MAX)
        }
    }
}

pub(crate) fn arrow_type_for_declared_type(declared_type: &str) -> Option<DataType> {
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
    } else if normalized.contains("REAL")
        || normalized.contains("FLOA")
        || normalized.contains("DOUB")
        || normalized.contains("NUM")
        || normalized.contains("DEC")
    {
        Some(DataType::Float64)
    } else if normalized.contains("CHAR")
        || normalized.contains("CLOB")
        || normalized.contains("TEXT")
        || normalized.contains("JSON")
    {
        Some(DataType::Utf8)
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
    fn controls_preserve_measured_defaults_and_enforce_bounds() {
        let defaults = SqliteNativeOptions::from_authored(None, None, None, None).unwrap();
        assert_eq!(defaults.output_batch_rows, 32 * 1024);
        assert!(SqliteNativeOptions::from_authored(Some(0), None, None, None).is_err());
        assert!(SqliteNativeOptions::from_authored(None, None, Some(1_048_577), None).is_err());
    }

    #[test]
    fn input_is_exactly_one_and_query_evidence_is_redacted() {
        let query = "SELECT id FROM private_events WHERE tenant = 'private-value'";
        let input = SqliteSourceInput::from_authored(None, Some(query.to_owned())).unwrap();
        let evidence = input.redacted_evidence().to_string();
        assert!(evidence.contains("query_sha256"));
        assert!(!evidence.contains("private-value"));
        assert!(
            SqliteSourceInput::from_authored(Some("events".to_owned()), Some(query.to_owned()))
                .is_err()
        );
    }

    #[test]
    fn query_discovery_accepts_complex_reads_and_rejects_mutation() {
        let temporary = tempfile::tempdir().unwrap();
        let database = temporary.path().join("native.sqlite");
        let connection = Connection::open(&database).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE events(id INTEGER PRIMARY KEY, category TEXT, payload TEXT);\
                 INSERT INTO events VALUES (1, 'a', '{\"value\":2}'), (2, 'a', '{\"value\":3}');",
            )
            .unwrap();
        drop(connection);
        let input = SqliteSourceInput::from_authored(
            None,
            Some(
                "WITH ranked AS (SELECT id, category, json_extract(payload, '$.value') AS value, row_number() OVER (PARTITION BY category ORDER BY id) AS ordinal FROM events) SELECT category, sum(value) AS total, max(ordinal) AS members FROM ranked GROUP BY category"
                    .to_owned(),
            ),
        )
        .unwrap();
        let discovery = discover_sqlite_query(
            &database,
            &ResourceId::new("local.summary").unwrap(),
            &input,
            &SqliteNativeOptions::default(),
            1_000,
            16 * 1024 * 1024,
        )
        .unwrap();
        assert_eq!(discovery.records_read, 1);
        assert_eq!(discovery.schema.fields().len(), 3);
        assert!(discovery.complete);

        for query in [
            "PRAGMA journal_mode=WAL",
            "DELETE FROM events",
            "ATTACH DATABASE 'other.sqlite' AS other",
            "SELECT load_extension('private-extension')",
            "SELECT 1; SELECT 2",
        ] {
            let input = SqliteSourceInput::from_authored(None, Some(query.to_owned())).unwrap();
            assert!(
                discover_sqlite_query(
                    &database,
                    &ResourceId::new("local.rejected").unwrap(),
                    &input,
                    &SqliteNativeOptions::default(),
                    1_000,
                    16 * 1024 * 1024,
                )
                .is_err(),
                "query unexpectedly accepted: {query}"
            );
        }
    }

    #[test]
    fn query_prepare_errors_preserve_ownership_without_echoing_authored_text() {
        let temporary = tempfile::tempdir().unwrap();
        let database = temporary.path().join("native.sqlite");
        drop(Connection::open(&database).unwrap());

        let syntax =
            SqliteSourceInput::from_authored(None, Some("SELECT 'private-value' AS".to_owned()))
                .unwrap();
        let error = discover_sqlite_query(
            &database,
            &ResourceId::new("local.syntax").unwrap(),
            &syntax,
            &SqliteNativeOptions::default(),
            1_000,
            16 * 1024 * 1024,
        )
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Contract);
        assert!(!error.message.contains("private-value"));

        let missing = SqliteSourceInput::from_authored(
            None,
            Some("SELECT id FROM private_events".to_owned()),
        )
        .unwrap();
        let error = discover_sqlite_query(
            &database,
            &ResourceId::new("local.missing").unwrap(),
            &missing,
            &SqliteNativeOptions::default(),
            1_000,
            16 * 1024 * 1024,
        )
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Data);
        assert!(!error.message.contains("private_events"));
    }
}
