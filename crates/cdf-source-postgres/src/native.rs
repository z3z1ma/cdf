use std::{collections::BTreeSet, ops::ControlFlow};

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use cdf_kernel::{
    CdfError, ForeignState, ResourceDescriptor, ResourceId, Result, SourcePosition,
    canonical_arrow_schema_hash, with_physical_type,
};
use cdf_postgres::{PostgresIdentifier, PostgresTarget};
use cdf_runtime::artifact_hash;
use cdf_semantic::{
    POSTGRES_JSON_TEXT_SEMANTIC, POSTGRES_JSONB_TEXT_SEMANTIC, POSTGRES_NUMERIC_TEXT_SEMANTIC,
    SemanticAuthority, builtin_catalog,
};
use postgres::{Client, IsolationLevel, Statement, Transaction, types::Type};
use serde::{Deserialize, Serialize};
use sqlparser::{
    ast::{Expr, Query, Select, SetExpr, Statement as SqlStatement, Value, Visit, Visitor},
    dialect::PostgreSqlDialect,
    parser::Parser,
};

use crate::error::classify_postgres_error;

pub(crate) const POSTGRES_DEFAULT_OUTPUT_BATCH_ROWS: usize = 65_536;
pub(crate) const POSTGRES_QUERY_GENERATION_SCHEMA_KEY: &str =
    "cdf.source.postgres.query_generation";
const POSTGRES_QUERY_GENERATION_PROTOCOL: &str = "cdf.postgres.query-generation.v1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum PostgresSourceInput {
    Table {
        target: PostgresTarget,
    },
    Query {
        #[serde(rename = "sql_base64", with = "query_bytes_base64")]
        sql: String,
        sha256: String,
    },
}

mod query_bytes_base64 {
    use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
    use serde::{Deserialize, Deserializer, Serializer, de::Error as _};

    pub(super) fn serialize<S>(sql: &String, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&BASE64_STANDARD.encode(sql.as_bytes()))
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<String, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        let bytes = BASE64_STANDARD.decode(encoded).map_err(D::Error::custom)?;
        String::from_utf8(bytes).map_err(D::Error::custom)
    }
}

impl PostgresSourceInput {
    pub(crate) fn from_authored(table: Option<String>, query: Option<String>) -> Result<Self> {
        match (table, query) {
            (Some(table), None) => Ok(Self::Table {
                target: PostgresTarget::parse(&table)?,
            }),
            (None, Some(query)) => {
                let sql = validate_postgres_read_query(&query)?;
                Ok(Self::Query {
                    sha256: artifact_hash(&sql)?,
                    sql,
                })
            }
            (Some(_), Some(_)) => Err(CdfError::contract(
                "Postgres resource must set exactly one of `table` or `query`, not both",
            )),
            (None, None) => Err(CdfError::contract(
                "Postgres resource must set exactly one of `table` or `query`",
            )),
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        match self {
            Self::Table { target } => {
                PostgresTarget::parse(&target.display_name())?;
            }
            Self::Query { sql, sha256 } => {
                let normalized = validate_postgres_read_query(sql)?;
                if &normalized != sql {
                    return Err(CdfError::contract(
                        "compiled Postgres native query is not canonical",
                    ));
                }
                if artifact_hash(sql)? != *sha256 {
                    return Err(CdfError::contract(
                        "compiled Postgres native query hash does not match its exact text",
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn redacted_evidence(&self) -> serde_json::Value {
        match self {
            Self::Table { target } => serde_json::json!({
                "kind": "table",
                "table": target.display_name(),
            }),
            Self::Query { sql, sha256 } => serde_json::json!({
                "kind": "query",
                "query_sha256": sha256,
                "query_bytes": sql.len(),
            }),
        }
    }

    pub(crate) fn relation_sql(&self) -> String {
        match self {
            Self::Table { target } => target.sql(),
            Self::Query { sql, .. } => format!("({sql}) AS \"_cdf_native_query\""),
        }
    }

    pub(crate) fn location_summary(&self) -> String {
        match self {
            Self::Table { target } => target.display_name(),
            Self::Query { sha256, .. } => format!("query:{sha256}"),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PostgresIsolation {
    ReadCommitted,
    #[default]
    RepeatableRead,
    Serializable,
}

impl PostgresIsolation {
    fn driver(self) -> IsolationLevel {
        match self {
            Self::ReadCommitted => IsolationLevel::ReadCommitted,
            Self::RepeatableRead => IsolationLevel::RepeatableRead,
            Self::Serializable => IsolationLevel::Serializable,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PostgresNativeOptions {
    #[serde(default)]
    pub(crate) isolation: PostgresIsolation,
    pub(crate) statement_timeout_ms: Option<u64>,
    pub(crate) lock_timeout_ms: Option<u64>,
    pub(crate) output_batch_rows: usize,
    pub(crate) search_path: Vec<PostgresIdentifier>,
}

impl Default for PostgresNativeOptions {
    fn default() -> Self {
        Self {
            isolation: PostgresIsolation::RepeatableRead,
            statement_timeout_ms: None,
            lock_timeout_ms: None,
            output_batch_rows: POSTGRES_DEFAULT_OUTPUT_BATCH_ROWS,
            search_path: Vec::new(),
        }
    }
}

impl PostgresNativeOptions {
    pub(crate) fn from_authored(
        isolation: PostgresIsolation,
        statement_timeout_ms: Option<u64>,
        lock_timeout_ms: Option<u64>,
        output_batch_rows: Option<u64>,
        search_path: Vec<String>,
    ) -> Result<Self> {
        let output_batch_rows =
            output_batch_rows.unwrap_or(POSTGRES_DEFAULT_OUTPUT_BATCH_ROWS as u64);
        let output_batch_rows = usize::try_from(output_batch_rows).map_err(|_| {
            CdfError::contract("Postgres output_batch_rows exceeds platform bounds")
        })?;
        let options = Self {
            isolation,
            statement_timeout_ms,
            lock_timeout_ms,
            output_batch_rows,
            search_path: search_path
                .into_iter()
                .map(PostgresIdentifier::user)
                .collect::<Result<Vec<_>>>()?,
        };
        options.validate()?;
        Ok(options)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_timeout("statement_timeout_ms", self.statement_timeout_ms)?;
        validate_timeout("lock_timeout_ms", self.lock_timeout_ms)?;
        if !(1..=100_000).contains(&self.output_batch_rows) {
            return Err(CdfError::contract(format!(
                "Postgres output_batch_rows {} must be between 1 and 100000",
                self.output_batch_rows
            )));
        }
        if self.search_path.len() > 64 {
            return Err(CdfError::contract(
                "Postgres search_path cannot contain more than 64 identifiers",
            ));
        }
        for identifier in &self.search_path {
            PostgresIdentifier::user(identifier.as_str())?;
        }
        Ok(())
    }

    pub(crate) fn begin_transaction<'a>(
        &self,
        client: &'a mut Client,
        action: &str,
    ) -> Result<Transaction<'a>> {
        self.validate()?;
        let mut transaction = client
            .build_transaction()
            .isolation_level(self.isolation.driver())
            .read_only(true)
            .start()
            .map_err(|error| classify_postgres_error(action, error))?;
        self.apply_local_settings(&mut transaction)?;
        Ok(transaction)
    }

    fn apply_local_settings(&self, transaction: &mut Transaction<'_>) -> Result<()> {
        if let Some(timeout) = self.statement_timeout_ms {
            transaction
                .batch_execute(&format!("SET LOCAL statement_timeout = {timeout}"))
                .map_err(|error| {
                    classify_postgres_error("set Postgres source statement_timeout", error)
                })?;
        }
        if let Some(timeout) = self.lock_timeout_ms {
            transaction
                .batch_execute(&format!("SET LOCAL lock_timeout = {timeout}"))
                .map_err(|error| {
                    classify_postgres_error("set Postgres source lock_timeout", error)
                })?;
        }
        if !self.search_path.is_empty() {
            let path = self
                .search_path
                .iter()
                .map(PostgresIdentifier::quoted)
                .collect::<Vec<_>>()
                .join(", ");
            transaction
                .batch_execute(&format!("SET LOCAL search_path = {path}"))
                .map_err(|error| {
                    classify_postgres_error("set Postgres source search_path", error)
                })?;
        }
        Ok(())
    }
}

fn validate_timeout(name: &str, value: Option<u64>) -> Result<()> {
    if value.is_some_and(|value| !(1..=3_600_000).contains(&value)) {
        return Err(CdfError::contract(format!(
            "Postgres {name} must be between 1 and 3600000 milliseconds"
        )));
    }
    Ok(())
}

pub(crate) fn describe_postgres_query(
    transaction: &mut Transaction<'_>,
    resource_id: &ResourceId,
    input: &PostgresSourceInput,
    options: &PostgresNativeOptions,
) -> Result<Schema> {
    let (query, query_sha256) = match input {
        PostgresSourceInput::Query { sql, sha256 } => (sql, sha256),
        PostgresSourceInput::Table { .. } => {
            return Err(CdfError::internal(
                "Postgres query description received a table input",
            ));
        }
    };
    let statement = transaction.prepare(query).map_err(|error| {
        classify_postgres_error("prepare Postgres native query for schema discovery", error)
    })?;
    let mut schema = schema_from_query_statement(resource_id, &statement)?;
    let descriptor_hash = canonical_arrow_schema_hash(&schema)?;
    let generation = artifact_hash(&(
        POSTGRES_QUERY_GENERATION_PROTOCOL,
        query_sha256,
        options,
        descriptor_hash.to_string(),
    ))?;
    schema
        .metadata
        .insert(POSTGRES_QUERY_GENERATION_SCHEMA_KEY.to_owned(), generation);
    Ok(schema)
}

fn schema_from_query_statement(resource_id: &ResourceId, statement: &Statement) -> Result<Schema> {
    if statement.columns().is_empty() {
        return Err(CdfError::data(format!(
            "Postgres native query for resource `{resource_id}` produced no columns"
        )));
    }
    let semantic_catalog = builtin_catalog()?;
    let mut names = BTreeSet::new();
    let fields = statement
        .columns()
        .iter()
        .map(|column| {
            if !names.insert(column.name().to_owned()) {
                return Err(CdfError::data(format!(
                    "Postgres native query for resource `{resource_id}` produced duplicate column `{}`; alias every output column uniquely",
                    column.name()
                )));
            }
            PostgresIdentifier::user(column.name())?;
            let (data_type, semantic) = arrow_type_for_query_column(column.type_()).ok_or_else(|| {
                CdfError::data(format!(
                    "Postgres native query for resource `{resource_id}` produced unsupported column `{}` with PostgreSQL type `{}`; cast it to a supported exact type in the query",
                    column.name(),
                    column.type_().name()
                ))
            })?;
            let field = with_physical_type(
                Field::new(column.name(), data_type, true),
                column.type_().name(),
            );
            match semantic {
                Some(semantic) => semantic_catalog.apply_reference(
                    field,
                    semantic,
                    SemanticAuthority::Observed,
                ),
                None => Ok(field),
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Schema::new(fields))
}

fn arrow_type_for_query_column(data_type: &Type) -> Option<(DataType, Option<&'static str>)> {
    let mapping = if data_type == &Type::BOOL {
        (DataType::Boolean, None)
    } else if matches!(data_type, &Type::INT2 | &Type::INT4 | &Type::INT8) {
        (DataType::Int64, None)
    } else if data_type == &Type::OID {
        (DataType::UInt64, None)
    } else if matches!(data_type, &Type::FLOAT4 | &Type::FLOAT8) {
        (DataType::Float64, None)
    } else if matches!(
        data_type,
        &Type::TEXT | &Type::VARCHAR | &Type::BPCHAR | &Type::NAME | &Type::UUID | &Type::UNKNOWN
    ) {
        (DataType::Utf8, None)
    } else if data_type == &Type::JSON {
        (DataType::Utf8, Some(POSTGRES_JSON_TEXT_SEMANTIC))
    } else if data_type == &Type::JSONB {
        (DataType::Utf8, Some(POSTGRES_JSONB_TEXT_SEMANTIC))
    } else if data_type == &Type::NUMERIC {
        (DataType::Utf8, Some(POSTGRES_NUMERIC_TEXT_SEMANTIC))
    } else if data_type == &Type::DATE {
        (DataType::Date32, None)
    } else if data_type == &Type::TIMESTAMP {
        (DataType::Timestamp(TimeUnit::Microsecond, None), None)
    } else if data_type == &Type::TIMESTAMPTZ {
        (
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            None,
        )
    } else {
        return None;
    };
    Some(mapping)
}

pub(crate) fn query_generation_from_schema(schema: &Schema) -> Result<&str> {
    schema
        .metadata()
        .get(POSTGRES_QUERY_GENERATION_SCHEMA_KEY)
        .map(String::as_str)
        .ok_or_else(|| {
            CdfError::data(
                "Postgres compiled query schema omitted source-generation authority; compile the resource again",
            )
        })
}

pub(crate) fn postgres_query_generation_position(
    descriptor: &ResourceDescriptor,
    generation: &str,
) -> Result<SourcePosition> {
    let Some(hex) = generation.strip_prefix("sha256:") else {
        return Err(CdfError::data(
            "Postgres query-generation identity must use sha256:<64 lowercase hex>",
        ));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(CdfError::data(
            "Postgres query-generation identity must use sha256:<64 lowercase hex>",
        ));
    }
    let authority = (
        POSTGRES_QUERY_GENERATION_PROTOCOL,
        descriptor.resource_id.as_str(),
        generation,
    );
    let opaque_blob = serde_json::to_vec(&authority).map_err(|error| {
        CdfError::internal(format!(
            "serialize Postgres query-generation authority: {error}"
        ))
    })?;
    let position = SourcePosition::ForeignState(ForeignState {
        version: cdf_kernel::SOURCE_POSITION_VERSION,
        protocol: POSTGRES_QUERY_GENERATION_PROTOCOL.to_owned(),
        blob_sha256: artifact_hash(&authority)?,
        opaque_blob,
    });
    position.validate()?;
    Ok(position)
}

pub(crate) fn validate_postgres_read_query(query: &str) -> Result<String> {
    let mut sql = query.trim();
    if let Some(without_semicolon) = sql.strip_suffix(';') {
        sql = without_semicolon.trim_end();
    }
    if sql.is_empty() {
        return Err(CdfError::contract("Postgres native query cannot be empty"));
    }
    if sql.len() > 1024 * 1024 {
        return Err(CdfError::contract(
            "Postgres native query exceeds the 1 MiB authored-query bound",
        ));
    }
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, sql).map_err(|_| {
        CdfError::contract(
            "Postgres native query is not valid PostgreSQL SQL; inspect the query syntax and retry",
        )
    })?;
    let [statement] = statements.as_slice() else {
        return Err(CdfError::contract(format!(
            "Postgres native query must contain exactly one statement, found {}",
            statements.len()
        )));
    };
    if !matches!(statement, SqlStatement::Query(_)) {
        return Err(CdfError::contract(
            "Postgres native query must be one SELECT, WITH ... SELECT, VALUES, or set-operation query",
        ));
    }
    let mut visitor = ReadOnlyQueryVisitor;
    if let ControlFlow::Break(reason) = statement.visit(&mut visitor) {
        return Err(CdfError::contract(format!(
            "Postgres native query is not read-only: {reason}"
        )));
    }
    Ok(sql.to_owned())
}

struct ReadOnlyQueryVisitor;

impl Visitor for ReadOnlyQueryVisitor {
    type Break = &'static str;

    fn pre_visit_statement(&mut self, statement: &SqlStatement) -> ControlFlow<Self::Break> {
        if matches!(statement, SqlStatement::Query(_)) {
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break("data-changing statements and commands are not allowed")
        }
    }

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        if !query.locks.is_empty() {
            return ControlFlow::Break("FOR UPDATE/SHARE row locks are not allowed");
        }
        if query.settings.is_some()
            || query.format_clause.is_some()
            || query.for_clause.is_some()
            || !query.pipe_operators.is_empty()
        {
            return ControlFlow::Break("query-local command/format clauses are not allowed");
        }
        validate_set_expression(&query.body)
    }

    fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<Self::Break> {
        if select.into.is_some() {
            ControlFlow::Break("SELECT INTO is not allowed")
        } else {
            ControlFlow::Continue(())
        }
    }

    fn pre_visit_expr(&mut self, expression: &Expr) -> ControlFlow<Self::Break> {
        if matches!(expression, Expr::Value(value) if matches!(value.value, Value::Placeholder(_)))
        {
            ControlFlow::Break("query parameters are not supported; author complete project SQL")
        } else {
            ControlFlow::Continue(())
        }
    }
}

fn validate_set_expression(expression: &SetExpr) -> ControlFlow<&'static str> {
    match expression {
        SetExpr::Select(_) | SetExpr::Values(_) => ControlFlow::Continue(()),
        SetExpr::Query(query) => validate_set_expression(&query.body),
        SetExpr::SetOperation { left, right, .. } => {
            validate_set_expression(left)?;
            validate_set_expression(right)
        }
        SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Delete(_) | SetExpr::Merge(_) => {
            ControlFlow::Break("data-changing common-table expressions are not allowed")
        }
        SetExpr::Table(_) => ControlFlow::Break("TABLE commands are not accepted; use SELECT"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_world_class_read_query_shapes() {
        for query in [
            "SELECT id, sum(amount) OVER (PARTITION BY account_id) AS total FROM ledger",
            "WITH totals AS (SELECT account_id, sum(amount) AS total FROM ledger GROUP BY account_id) SELECT * FROM totals",
            "VALUES (1, 'one'), (2, 'two')",
            "SELECT 1 AS id UNION ALL SELECT 2 AS id",
            "SELECT * FROM LATERAL jsonb_array_elements('[1,2]'::jsonb)",
        ] {
            assert_eq!(validate_postgres_read_query(query).unwrap(), query);
        }
    }

    #[test]
    fn rejects_writes_locks_parameters_and_multiple_statements() {
        for query in [
            "INSERT INTO target VALUES (1)",
            "UPDATE target SET value = 1",
            "DELETE FROM target",
            "COPY target TO STDOUT",
            "CALL mutate()",
            "SELECT * INTO new_table FROM source",
            "SELECT * FROM source FOR UPDATE",
            "WITH changed AS (DELETE FROM source RETURNING *) SELECT * FROM changed",
            "SELECT $1::integer",
            "SELECT 1; SELECT 2",
        ] {
            assert!(
                validate_postgres_read_query(query).is_err(),
                "query unexpectedly accepted: {query}"
            );
        }
    }

    #[test]
    fn validates_native_option_bounds_and_preserves_copy_default() {
        let defaults = PostgresNativeOptions::from_authored(
            PostgresIsolation::default(),
            None,
            None,
            None,
            Vec::new(),
        )
        .unwrap();
        assert_eq!(defaults.output_batch_rows, 65_536);

        assert!(
            PostgresNativeOptions::from_authored(
                PostgresIsolation::ReadCommitted,
                Some(0),
                None,
                Some(1),
                vec!["analytics".to_owned()],
            )
            .is_err()
        );
        assert!(
            PostgresNativeOptions::from_authored(
                PostgresIsolation::Serializable,
                None,
                None,
                Some(100_001),
                Vec::new(),
            )
            .is_err()
        );
    }

    #[test]
    fn enforces_exactly_one_native_input_and_redacts_query_literals() {
        assert!(PostgresSourceInput::from_authored(None, None).is_err());
        assert!(
            PostgresSourceInput::from_authored(
                Some("public.events".to_owned()),
                Some("SELECT 1".to_owned())
            )
            .is_err()
        );
        let input = PostgresSourceInput::from_authored(
            None,
            Some("SELECT * FROM ledger WHERE tenant = 'private-value'".to_owned()),
        )
        .unwrap();
        let evidence = input.redacted_evidence().to_string();
        assert!(evidence.contains("query_sha256"));
        assert!(!evidence.contains("private-value"));

        let serialized = serde_json::to_string(&input).unwrap();
        assert!(serialized.contains("sql_base64"));
        assert!(!serialized.contains("private-value"));
        assert_eq!(
            serde_json::from_str::<PostgresSourceInput>(&serialized).unwrap(),
            input
        );

        let error = validate_postgres_read_query("SELECT 'private-parse-literal").unwrap_err();
        assert!(!error.to_string().contains("private-parse-literal"));
    }
}
