use std::ops::ControlFlow;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use cdf_kernel::{CdfError, Result};
use cdf_runtime::artifact_hash;
use mysql_async::{IsolationLevel, TxOpts};
use serde::{Deserialize, Serialize};
use sqlparser::{
    ast::{Expr, Query, Select, SetExpr, Statement as SqlStatement, Value, Visit, Visitor},
    dialect::MySqlDialect,
    parser::Parser,
};

use crate::identifier::MySqlTarget;

pub(crate) const MYSQL_DEFAULT_FETCH_ROWS: usize = 8_192;
pub(crate) const MYSQL_DEFAULT_OUTPUT_BATCH_ROWS: usize = 65_536;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum MySqlSourceInput {
    Table {
        target: MySqlTarget,
    },
    Query {
        #[serde(rename = "sql_base64", with = "query_bytes_base64")]
        sql: String,
        sha256: String,
    },
}

mod query_bytes_base64 {
    use super::*;
    use serde::{Deserializer, Serializer, de::Error as _};

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

impl MySqlSourceInput {
    pub(crate) fn from_authored(table: Option<String>, query: Option<String>) -> Result<Self> {
        match (table, query) {
            (Some(table), None) => Ok(Self::Table {
                target: MySqlTarget::parse(&table)?,
            }),
            (None, Some(query)) => {
                let sql = validate_mysql_read_query(&query)?;
                Ok(Self::Query {
                    sha256: artifact_hash(&sql)?,
                    sql,
                })
            }
            (Some(_), Some(_)) => Err(CdfError::contract(
                "MySQL resource must set exactly one of `table` or `query`, not both",
            )),
            (None, None) => Err(CdfError::contract(
                "MySQL resource must set exactly one of `table` or `query`",
            )),
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        match self {
            Self::Table { target } => {
                MySqlTarget::parse(&target.display_name())?;
            }
            Self::Query { sql, sha256 } => {
                let canonical = validate_mysql_read_query(sql)?;
                if &canonical != sql || artifact_hash(sql)? != *sha256 {
                    return Err(CdfError::contract(
                        "compiled MySQL native query differs from its exact authored authority",
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn relation_sql(&self) -> String {
        match self {
            Self::Table { target } => target.sql(),
            Self::Query { sql, .. } => format!("({sql}) AS `_cdf_native_query`"),
        }
    }

    pub(crate) fn location_summary(&self) -> String {
        match self {
            Self::Table { target } => target.display_name(),
            Self::Query { sha256, .. } => format!("query:{sha256}"),
        }
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
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MySqlIsolation {
    ReadCommitted,
    #[default]
    RepeatableRead,
    Serializable,
}

impl MySqlIsolation {
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
pub(crate) struct MySqlNativeOptions {
    #[serde(default)]
    pub(crate) isolation: MySqlIsolation,
    pub(crate) fetch_rows: usize,
    pub(crate) output_batch_rows: usize,
    pub(crate) max_execution_time_ms: Option<u64>,
    pub(crate) lock_wait_timeout_ms: Option<u64>,
    #[serde(default)]
    pub(crate) use_invisible_indexes: bool,
}

impl MySqlNativeOptions {
    pub(crate) fn from_authored(
        isolation: MySqlIsolation,
        fetch_rows: Option<u64>,
        output_batch_rows: Option<u64>,
        max_execution_time_ms: Option<u64>,
        lock_wait_timeout_ms: Option<u64>,
        use_invisible_indexes: bool,
    ) -> Result<Self> {
        let options = Self {
            isolation,
            fetch_rows: platform_rows("fetch_rows", fetch_rows, MYSQL_DEFAULT_FETCH_ROWS)?,
            output_batch_rows: platform_rows(
                "output_batch_rows",
                output_batch_rows,
                MYSQL_DEFAULT_OUTPUT_BATCH_ROWS,
            )?,
            max_execution_time_ms,
            lock_wait_timeout_ms,
            use_invisible_indexes,
        };
        options.validate()?;
        Ok(options)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        for (name, rows) in [
            ("fetch_rows", self.fetch_rows),
            ("output_batch_rows", self.output_batch_rows),
        ] {
            if !(1..=100_000).contains(&rows) {
                return Err(CdfError::contract(format!(
                    "MySQL {name} {rows} must be between 1 and 100000"
                )));
            }
        }
        for (name, value) in [
            ("max_execution_time_ms", self.max_execution_time_ms),
            ("lock_wait_timeout_ms", self.lock_wait_timeout_ms),
        ] {
            if value.is_some_and(|value| !(1..=3_600_000).contains(&value)) {
                return Err(CdfError::contract(format!(
                    "MySQL {name} must be between 1 and 3600000 milliseconds"
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn transaction_options(&self) -> TxOpts {
        let mut options = TxOpts::new();
        options
            .with_isolation_level(self.isolation.driver())
            .with_readonly(true)
            .with_consistent_snapshot(true);
        options
    }
}

fn platform_rows(name: &str, value: Option<u64>, default: usize) -> Result<usize> {
    let rows = usize::try_from(value.unwrap_or(default as u64))
        .map_err(|_| CdfError::contract(format!("MySQL {name} exceeds platform bounds")))?;
    Ok(rows)
}

pub(crate) fn validate_mysql_read_query(query: &str) -> Result<String> {
    let mut sql = query.trim();
    if let Some(without_semicolon) = sql.strip_suffix(';') {
        sql = without_semicolon.trim_end();
    }
    if sql.is_empty() {
        return Err(CdfError::contract("MySQL native query cannot be empty"));
    }
    if sql.len() > 1024 * 1024 {
        return Err(CdfError::contract(
            "MySQL native query exceeds the 1 MiB authored-query bound",
        ));
    }
    let statements = Parser::parse_sql(&MySqlDialect {}, sql).map_err(|_| {
        CdfError::contract(
            "MySQL native query is not valid MySQL SQL; inspect the query syntax and retry",
        )
    })?;
    let [statement] = statements.as_slice() else {
        return Err(CdfError::contract(format!(
            "MySQL native query must contain exactly one statement, found {}",
            statements.len()
        )));
    };
    if !matches!(statement, SqlStatement::Query(_)) {
        return Err(CdfError::contract(
            "MySQL native query must be one SELECT, WITH ... SELECT, VALUES, or set-operation query",
        ));
    }
    let mut visitor = ReadOnlyQueryVisitor;
    if let ControlFlow::Break(reason) = statement.visit(&mut visitor) {
        return Err(CdfError::contract(format!(
            "MySQL native query is not read-only: {reason}"
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
        if !query.locks.is_empty() || query.for_clause.is_some() {
            return ControlFlow::Break("FOR UPDATE/SHARE row locks are not allowed");
        }
        if query.settings.is_some()
            || query.format_clause.is_some()
            || !query.pipe_operators.is_empty()
        {
            return ControlFlow::Break("query-local command/format clauses are not allowed");
        }
        validate_set_expression(&query.body)
    }

    fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<Self::Break> {
        if select.into.is_some() {
            ControlFlow::Break("SELECT INTO OUTFILE/DUMPFILE is not allowed")
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
    fn accepts_mysql_read_queries_and_rejects_mutating_shapes() {
        for query in [
            "SELECT id, JSON_EXTRACT(payload, '$.kind') AS kind FROM events",
            "WITH totals AS (SELECT account_id, SUM(amount) total FROM ledger GROUP BY account_id) SELECT * FROM totals",
            "VALUES ROW(1, 'one'), ROW(2, 'two')",
            "SELECT 1 AS id UNION ALL SELECT 2 AS id",
        ] {
            assert!(validate_mysql_read_query(query).is_ok(), "{query}");
        }
        for query in [
            "UPDATE ledger SET amount = 0",
            "DELETE FROM ledger",
            "CALL mutate()",
            "SELECT * FROM ledger FOR UPDATE",
            "SELECT * INTO OUTFILE '/tmp/x' FROM ledger",
            "SELECT 1; SELECT 2",
        ] {
            assert!(validate_mysql_read_query(query).is_err(), "{query}");
        }
    }

    #[test]
    fn native_options_are_bounded_and_cursor_independent() {
        let options = MySqlNativeOptions::from_authored(
            MySqlIsolation::default(),
            None,
            None,
            None,
            None,
            false,
        )
        .unwrap();
        assert_eq!(options.fetch_rows, MYSQL_DEFAULT_FETCH_ROWS);
        assert_eq!(options.output_batch_rows, MYSQL_DEFAULT_OUTPUT_BATCH_ROWS);
        assert!(
            MySqlNativeOptions::from_authored(
                MySqlIsolation::default(),
                Some(0),
                None,
                None,
                None,
                false,
            )
            .is_err()
        );
    }
}
