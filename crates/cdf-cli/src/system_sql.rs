use std::{
    fs,
    path::{Path, PathBuf},
};

use cdf_kernel::CdfError;
use cdf_package::{MANIFEST_FILE, read_manifest, read_receipts};
use rusqlite::{Connection, OpenFlags, OptionalExtension, Row, Statement, params, types::ValueRef};
use serde::Serialize;
use serde_json::{Number, Value};

use crate::{context::ProjectContext, error_catalog, output::CliError};

const TABLES: &[&str] = &[
    "checkpoints",
    "packages",
    "package_files",
    "package_segments",
    "package_receipts",
    "package_receipt_segments",
];

const CHECKPOINT_HISTORY_SELECT: &str = "
    SELECT
        sequence,
        checkpoint_id,
        pipeline_id,
        resource_id,
        scope_json,
        state_version,
        parent_checkpoint_id,
        input_position_json,
        output_position_json,
        package_hash,
        schema_hash,
        receipt_id,
        status,
        is_head,
        created_at_ms,
        committed_at_ms,
        delta_json,
        receipt_json,
        rewind_target_checkpoint_id
    FROM cdf_checkpoints
    ORDER BY sequence
";

const MUTATING_KEYWORDS: &[&str] = &[
    "insert", "update", "delete", "create", "drop", "alter", "pragma", "attach", "detach",
    "vacuum", "reindex", "replace",
];

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct SystemSqlReport {
    pub tables: Vec<&'static str>,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

impl SystemSqlReport {
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
}

pub(crate) fn run(context: &ProjectContext, query: &str) -> Result<SystemSqlReport, CliError> {
    let query = read_only_query(query)?;
    let conn = Connection::open_in_memory().map_err(sqlite_cli_error)?;
    create_schema(&conn)?;
    mount_checkpoints(&conn, context.state_store_path()?)?;
    mount_packages(&conn, context.package_root())?;
    query_rows(&conn, query)
}

fn create_schema(conn: &Connection) -> Result<(), CliError> {
    conn.execute_batch(
        "
        CREATE TABLE checkpoints (
            sequence INTEGER NOT NULL,
            checkpoint_id TEXT NOT NULL,
            pipeline_id TEXT NOT NULL,
            resource_id TEXT NOT NULL,
            scope_json TEXT NOT NULL,
            state_version INTEGER NOT NULL,
            parent_checkpoint_id TEXT,
            input_position_json TEXT,
            output_position_json TEXT NOT NULL,
            package_hash TEXT NOT NULL,
            schema_hash TEXT NOT NULL,
            receipt_id TEXT,
            status TEXT NOT NULL,
            is_head INTEGER NOT NULL,
            created_at_ms INTEGER NOT NULL,
            committed_at_ms INTEGER,
            delta_json TEXT NOT NULL,
            receipt_json TEXT,
            rewind_target_checkpoint_id TEXT
        );

        CREATE TABLE packages (
            package_path TEXT NOT NULL,
            package_id TEXT NOT NULL,
            package_hash TEXT NOT NULL,
            status TEXT NOT NULL,
            signing_input TEXT NOT NULL,
            signature TEXT,
            identity_file_count INTEGER NOT NULL,
            segment_count INTEGER NOT NULL,
            receipt_count INTEGER NOT NULL
        );

        CREATE TABLE package_files (
            package_hash TEXT NOT NULL,
            package_id TEXT NOT NULL,
            path TEXT NOT NULL,
            byte_count INTEGER NOT NULL,
            sha256 TEXT NOT NULL
        );

        CREATE TABLE package_segments (
            package_hash TEXT NOT NULL,
            package_id TEXT NOT NULL,
            segment_id TEXT NOT NULL,
            path TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            byte_count INTEGER NOT NULL,
            sha256 TEXT NOT NULL
        );

        CREATE TABLE package_receipts (
            package_hash TEXT NOT NULL,
            package_id TEXT NOT NULL,
            receipt_id TEXT NOT NULL,
            destination TEXT NOT NULL,
            target TEXT NOT NULL,
            disposition TEXT NOT NULL,
            idempotency_token TEXT NOT NULL,
            rows_written INTEGER NOT NULL,
            rows_inserted INTEGER,
            rows_updated INTEGER,
            rows_deleted INTEGER,
            schema_hash TEXT NOT NULL,
            committed_at_ms INTEGER NOT NULL,
            receipt_json TEXT NOT NULL
        );

        CREATE TABLE package_receipt_segments (
            package_hash TEXT NOT NULL,
            package_id TEXT NOT NULL,
            receipt_id TEXT NOT NULL,
            segment_id TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            byte_count INTEGER NOT NULL
        );
        ",
    )
    .map_err(sqlite_cli_error)
}

fn mount_checkpoints(conn: &Connection, path: PathBuf) -> Result<(), CliError> {
    if !path.exists() {
        return Ok(());
    }
    let ledger = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(sqlite_cli_error)?;
    let has_checkpoints = ledger
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'cdf_checkpoints'",
            [],
            |_| Ok(()),
        )
        .optional()
        .map_err(sqlite_cli_error)?
        .is_some();
    if !has_checkpoints {
        return Ok(());
    }
    let mut insert = conn
        .prepare(
            "
            INSERT INTO checkpoints (
                sequence,
                checkpoint_id,
                pipeline_id,
                resource_id,
                scope_json,
                state_version,
                parent_checkpoint_id,
                input_position_json,
                output_position_json,
                package_hash,
                schema_hash,
                receipt_id,
                status,
                is_head,
                created_at_ms,
                committed_at_ms,
                delta_json,
                receipt_json,
                rewind_target_checkpoint_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .map_err(sqlite_cli_error)?;
    let mut select = ledger
        .prepare(CHECKPOINT_HISTORY_SELECT)
        .map_err(sqlite_cli_error)?;
    let rows = select
        .query_map([], raw_checkpoint_row)
        .map_err(sqlite_cli_error)?;
    for row in rows {
        let row = row.map_err(sqlite_cli_error)?;
        insert
            .execute(params![
                row.sequence,
                row.checkpoint_id,
                row.pipeline_id,
                row.resource_id,
                row.scope_json,
                row.state_version,
                row.parent_checkpoint_id,
                row.input_position_json,
                row.output_position_json,
                row.package_hash,
                row.schema_hash,
                row.receipt_id,
                row.status,
                row.is_head,
                row.created_at_ms,
                row.committed_at_ms,
                row.delta_json,
                row.receipt_json,
                row.rewind_target_checkpoint_id,
            ])
            .map_err(sqlite_cli_error)?;
    }
    Ok(())
}

fn mount_packages(conn: &Connection, root: PathBuf) -> Result<(), CliError> {
    if !root.exists() {
        return Ok(());
    }
    let mut entries = fs::read_dir(&root)
        .map_err(|error| CdfError::data(format!("read {}: {error}", root.display())))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| CdfError::data(format!("read {}: {error}", root.display())))?;
    entries.sort_by_key(|entry| entry.path());

    for entry in entries {
        let path = entry.path();
        if path.join(MANIFEST_FILE).exists() {
            mount_package(conn, &path)?;
        }
    }
    Ok(())
}

fn mount_package(conn: &Connection, path: &Path) -> Result<(), CliError> {
    let manifest = read_manifest(path)?;
    let package_id = manifest.identity.package_id.as_str();
    let package_hash = manifest.package_hash.as_str();
    let receipts = read_receipts(path)?;
    conn.execute(
        "
        INSERT INTO packages (
            package_path,
            package_id,
            package_hash,
            status,
            signing_input,
            signature,
            identity_file_count,
            segment_count,
            receipt_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ",
        params![
            path.display().to_string(),
            package_id,
            package_hash,
            manifest.lifecycle.status.as_str(),
            &manifest.signature.signing_input,
            manifest.signature.value.as_deref(),
            to_i64(manifest.identity.files.len())?,
            to_i64(manifest.identity.segments.len())?,
            to_i64(receipts.len())?,
        ],
    )
    .map_err(sqlite_cli_error)?;

    let mut insert_file = conn
        .prepare(
            "
            INSERT INTO package_files (
                package_hash,
                package_id,
                path,
                byte_count,
                sha256
            ) VALUES (?, ?, ?, ?, ?)
            ",
        )
        .map_err(sqlite_cli_error)?;
    for file in &manifest.identity.files {
        insert_file
            .execute(params![
                package_hash,
                package_id,
                &file.path,
                to_i64(file.byte_count)?,
                &file.sha256,
            ])
            .map_err(sqlite_cli_error)?;
    }

    let mut insert_segment = conn
        .prepare(
            "
            INSERT INTO package_segments (
                package_hash,
                package_id,
                segment_id,
                path,
                row_count,
                byte_count,
                sha256
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .map_err(sqlite_cli_error)?;
    for segment in &manifest.identity.segments {
        insert_segment
            .execute(params![
                package_hash,
                package_id,
                segment.segment_id.as_str(),
                &segment.path,
                to_i64(segment.row_count)?,
                to_i64(segment.byte_count)?,
                &segment.sha256,
            ])
            .map_err(sqlite_cli_error)?;
    }

    let mut insert_receipt = conn
        .prepare(
            "
            INSERT INTO package_receipts (
                package_hash,
                package_id,
                receipt_id,
                destination,
                target,
                disposition,
                idempotency_token,
                rows_written,
                rows_inserted,
                rows_updated,
                rows_deleted,
                schema_hash,
                committed_at_ms,
                receipt_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .map_err(sqlite_cli_error)?;
    let mut insert_ack = conn
        .prepare(
            "
            INSERT INTO package_receipt_segments (
                package_hash,
                package_id,
                receipt_id,
                segment_id,
                row_count,
                byte_count
            ) VALUES (?, ?, ?, ?, ?, ?)
            ",
        )
        .map_err(sqlite_cli_error)?;
    for receipt in &receipts {
        let receipt_json = json_string(receipt)?;
        insert_receipt
            .execute(params![
                package_hash,
                package_id,
                receipt.receipt_id.as_str(),
                receipt.destination.as_str(),
                receipt.target.as_str(),
                json_scalar_string(&receipt.disposition)?,
                receipt.idempotency_token.as_str(),
                to_i64(receipt.counts.rows_written)?,
                optional_to_i64(receipt.counts.rows_inserted)?,
                optional_to_i64(receipt.counts.rows_updated)?,
                optional_to_i64(receipt.counts.rows_deleted)?,
                receipt.schema_hash.as_str(),
                receipt.committed_at_ms,
                receipt_json,
            ])
            .map_err(sqlite_cli_error)?;
        for ack in &receipt.segment_acks {
            insert_ack
                .execute(params![
                    package_hash,
                    package_id,
                    receipt.receipt_id.as_str(),
                    ack.segment_id.as_str(),
                    to_i64(ack.row_count)?,
                    to_i64(ack.byte_count)?,
                ])
                .map_err(sqlite_cli_error)?;
        }
    }
    Ok(())
}

#[derive(Debug)]
struct RawCheckpointRow {
    sequence: i64,
    checkpoint_id: String,
    pipeline_id: String,
    resource_id: String,
    scope_json: String,
    state_version: i64,
    parent_checkpoint_id: Option<String>,
    input_position_json: Option<String>,
    output_position_json: String,
    package_hash: String,
    schema_hash: String,
    receipt_id: Option<String>,
    status: String,
    is_head: i64,
    created_at_ms: i64,
    committed_at_ms: Option<i64>,
    delta_json: String,
    receipt_json: Option<String>,
    rewind_target_checkpoint_id: Option<String>,
}

fn raw_checkpoint_row(row: &Row<'_>) -> rusqlite::Result<RawCheckpointRow> {
    Ok(RawCheckpointRow {
        sequence: row.get("sequence")?,
        checkpoint_id: row.get("checkpoint_id")?,
        pipeline_id: row.get("pipeline_id")?,
        resource_id: row.get("resource_id")?,
        scope_json: row.get("scope_json")?,
        state_version: row.get("state_version")?,
        parent_checkpoint_id: row.get("parent_checkpoint_id")?,
        input_position_json: row.get("input_position_json")?,
        output_position_json: row.get("output_position_json")?,
        package_hash: row.get("package_hash")?,
        schema_hash: row.get("schema_hash")?,
        receipt_id: row.get("receipt_id")?,
        status: row.get("status")?,
        is_head: row.get("is_head")?,
        created_at_ms: row.get("created_at_ms")?,
        committed_at_ms: row.get("committed_at_ms")?,
        delta_json: row.get("delta_json")?,
        receipt_json: row.get("receipt_json")?,
        rewind_target_checkpoint_id: row.get("rewind_target_checkpoint_id")?,
    })
}

fn query_rows(conn: &Connection, query: &str) -> Result<SystemSqlReport, CliError> {
    let mut stmt = conn.prepare(query).map_err(query_cli_error)?;
    reject_non_readonly_statement(&stmt)?;
    let columns = stmt
        .column_names()
        .into_iter()
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if columns.is_empty() {
        return Err(CliError::usage_with(
            "sql requires a read-only query that returns columns",
            error_catalog::SQL_QUERY,
        ));
    }

    let mut rows = stmt.query([]).map_err(query_cli_error)?;
    let mut values = Vec::new();
    while let Some(row) = rows.next().map_err(query_cli_error)? {
        values.push(row_values(row, columns.len())?);
    }
    Ok(SystemSqlReport {
        tables: TABLES.to_vec(),
        columns,
        rows: values,
    })
}

fn reject_non_readonly_statement(stmt: &Statement<'_>) -> Result<(), CliError> {
    if stmt.readonly() {
        Ok(())
    } else {
        Err(CliError::usage_with(
            "sql accepts one read-only SELECT or WITH query",
            error_catalog::SQL_QUERY,
        ))
    }
}

fn row_values(row: &Row<'_>, column_count: usize) -> Result<Vec<Value>, CliError> {
    (0..column_count)
        .map(|index| {
            row.get_ref(index)
                .map(sql_json_value)
                .map_err(query_cli_error)
        })
        .collect()
}

pub(crate) fn read_only_query(query: &str) -> Result<&str, CliError> {
    let query = query.trim();
    if query.is_empty() {
        return Err(CliError::usage_with(
            "sql requires a query string",
            error_catalog::SQL_QUERY,
        ));
    }
    let query = strip_trailing_semicolon(query)?;
    match leading_keyword(query).as_deref() {
        Some("select" | "with") => {
            reject_mutating_keywords(query)?;
            Ok(query)
        }
        _ => Err(CliError::usage_with(
            "sql accepts one read-only SELECT or WITH query",
            error_catalog::SQL_QUERY,
        )),
    }
}

fn strip_trailing_semicolon(query: &str) -> Result<&str, CliError> {
    let mut semicolon = None;
    let mut scanner = Scanner::new(query);
    while let Some((index, ch)) = scanner.next_code_char()? {
        if ch == ';' {
            semicolon = Some(index);
            break;
        }
    }
    let Some(index) = semicolon else {
        return Ok(query);
    };
    let rest = &query[index + 1..];
    if has_code(rest)? {
        return Err(CliError::usage_with(
            "sql accepts one query statement",
            error_catalog::SQL_QUERY,
        ));
    }
    Ok(query[..index].trim_end())
}

fn leading_keyword(query: &str) -> Option<String> {
    let mut scanner = Scanner::new(query);
    while let Some((_, ch)) = scanner.next_leading_char().ok()? {
        if ch.is_whitespace() {
            continue;
        }
        if ch == '_' || ch.is_ascii_alphabetic() {
            let mut keyword = String::new();
            keyword.push(ch.to_ascii_lowercase());
            while let Some(ch) = scanner.peek_char() {
                if ch == '_' || ch.is_ascii_alphanumeric() {
                    keyword.push(ch.to_ascii_lowercase());
                    scanner.next_char();
                } else {
                    break;
                }
            }
            return Some(keyword);
        }
        return None;
    }
    None
}

fn reject_mutating_keywords(query: &str) -> Result<(), CliError> {
    let mut scanner = Scanner::new(query);
    while let Some(keyword) = scanner.next_keyword()? {
        if MUTATING_KEYWORDS.contains(&keyword.as_str()) {
            return Err(CliError::usage_with(
                "sql accepts one read-only SELECT or WITH query",
                error_catalog::SQL_QUERY,
            ));
        }
    }
    Ok(())
}

fn has_code(sql: &str) -> Result<bool, CliError> {
    let mut scanner = Scanner::new(sql);
    while let Some((_, ch)) = scanner.next_code_char()? {
        if !ch.is_whitespace() {
            return Ok(true);
        }
    }
    Ok(false)
}

struct Scanner<'a> {
    sql: &'a str,
    cursor: usize,
}

impl<'a> Scanner<'a> {
    fn new(sql: &'a str) -> Self {
        Self { sql, cursor: 0 }
    }

    fn next_keyword(&mut self) -> Result<Option<String>, CliError> {
        while let Some((_, ch)) = self.next_code_char()? {
            if ch == '_' || ch.is_ascii_alphabetic() {
                let mut keyword = String::new();
                keyword.push(ch.to_ascii_lowercase());
                while let Some(ch) = self.peek_char() {
                    if ch == '_' || ch.is_ascii_alphanumeric() {
                        keyword.push(ch.to_ascii_lowercase());
                        self.next_char();
                    } else {
                        break;
                    }
                }
                return Ok(Some(keyword));
            }
        }
        Ok(None)
    }

    fn next_leading_char(&mut self) -> Result<Option<(usize, char)>, CliError> {
        loop {
            let Some((index, ch)) = self.next_char() else {
                return Ok(None);
            };
            match ch {
                '-' if self.peek_char() == Some('-') => {
                    self.next_char();
                    self.skip_line_comment();
                }
                '/' if self.peek_char() == Some('*') => {
                    self.next_char();
                    self.skip_block_comment()?;
                }
                _ => return Ok(Some((index, ch))),
            }
        }
    }

    fn next_code_char(&mut self) -> Result<Option<(usize, char)>, CliError> {
        loop {
            let Some((index, ch)) = self.next_char() else {
                return Ok(None);
            };
            match ch {
                '\'' => self.skip_quoted('\'')?,
                '"' => self.skip_quoted('"')?,
                '-' if self.peek_char() == Some('-') => {
                    self.next_char();
                    self.skip_line_comment();
                }
                '/' if self.peek_char() == Some('*') => {
                    self.next_char();
                    self.skip_block_comment()?;
                }
                _ => return Ok(Some((index, ch))),
            }
        }
    }

    fn next_char(&mut self) -> Option<(usize, char)> {
        let rest = self.sql.get(self.cursor..)?;
        let (offset, ch) = rest.char_indices().next()?;
        let index = self.cursor + offset;
        self.cursor = index + ch.len_utf8();
        Some((index, ch))
    }

    fn peek_char(&self) -> Option<char> {
        self.sql.get(self.cursor..)?.chars().next()
    }

    fn skip_quoted(&mut self, quote: char) -> Result<(), CliError> {
        while let Some((_, ch)) = self.next_char() {
            if ch == quote {
                if self.peek_char() == Some(quote) {
                    self.next_char();
                } else {
                    return Ok(());
                }
            }
        }
        Err(CliError::usage_with(
            "sql query contains an unterminated string",
            error_catalog::SQL_QUERY,
        ))
    }

    fn skip_line_comment(&mut self) {
        while let Some((_, ch)) = self.next_char() {
            if ch == '\n' {
                return;
            }
        }
    }

    fn skip_block_comment(&mut self) -> Result<(), CliError> {
        while let Some((_, ch)) = self.next_char() {
            if ch == '*' && self.peek_char() == Some('/') {
                self.next_char();
                return Ok(());
            }
        }
        Err(CliError::usage_with(
            "sql query contains an unterminated comment",
            error_catalog::SQL_QUERY,
        ))
    }
}

fn sql_json_value(value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(value) => Value::Number(value.into()),
        ValueRef::Real(value) => Number::from_f64(value).map_or(Value::Null, Value::Number),
        ValueRef::Text(value) => Value::String(String::from_utf8_lossy(value).into_owned()),
        ValueRef::Blob(value) => Value::String(format!("0x{}", hex_bytes(value))),
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn json_string<T: Serialize>(value: &T) -> Result<String, CliError> {
    serde_json::to_string(value).map_err(json_cli_error)
}

fn json_scalar_string<T: Serialize>(value: &T) -> Result<String, CliError> {
    let value = serde_json::to_value(value).map_err(json_cli_error)?;
    value.as_str().map(ToOwned::to_owned).ok_or_else(|| {
        CliError::mapped(
            CdfError::data("expected JSON string scalar"),
            error_catalog::SQL_RESULT,
        )
    })
}

fn to_i64(value: impl TryInto<i64>) -> Result<i64, CliError> {
    value.try_into().map_err(|_| {
        CliError::mapped(
            CdfError::internal("integer does not fit in i64"),
            error_catalog::SQL_INTERNAL,
        )
    })
}

fn optional_to_i64<T>(value: Option<T>) -> Result<Option<i64>, CliError>
where
    T: TryInto<i64>,
{
    value.map(to_i64).transpose()
}

fn sqlite_cli_error(error: rusqlite::Error) -> CliError {
    CliError::mapped(
        CdfError::internal(error.to_string()),
        error_catalog::SQL_INTERNAL,
    )
}

fn query_cli_error(error: rusqlite::Error) -> CliError {
    CliError::usage_with(
        format!("sql query failed: {error}"),
        error_catalog::SQL_QUERY,
    )
}

fn json_cli_error(error: serde_json::Error) -> CliError {
    CliError::mapped(CdfError::data(error.to_string()), error_catalog::SQL_RESULT)
}
