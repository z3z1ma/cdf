//! SQLite configuration, capacity proof, and host-error ownership.

use cdf_kernel::{CdfError, Result};
use rusqlite::{Connection, ErrorCode, params};

pub(crate) const SQLITE_PAGE_BYTES: u64 = 4096;
const SQLITE_INDEX_MAX_LOCAL_PAYLOAD_BYTES: u64 = 1002;
// These mirror bundled SQLite: BTCURSOR_MAX_DEPTH is 20 and balancing allocates at most two net
// pages per level. Eight pages cover the root, schema, and conservative record-header slack.
pub(crate) const SQLITE_BTREE_MAX_DEPTH: u64 = 20;
pub(crate) const SQLITE_BALANCE_NEW_PAGES_PER_LEVEL: u64 = 2;
pub(crate) const SQLITE_INSERT_GUARD_PAGES: u64 = 8;

pub(crate) fn configure_canonical_index(connection: &Connection, cache_bytes: u64) -> Result<()> {
    connection
        .pragma_update(
            None,
            "page_size",
            i64::try_from(SQLITE_PAGE_BYTES)
                .map_err(|_| CdfError::internal("SQLite page size exceeds i64"))?,
        )
        .and_then(|_| connection.pragma_update(None, "journal_mode", "OFF"))
        .and_then(|_| connection.pragma_update(None, "synchronous", "OFF"))
        .and_then(|_| connection.pragma_update(None, "locking_mode", "EXCLUSIVE"))
        .and_then(|_| connection.pragma_update(None, "temp_store", "FILE"))
        .and_then(|_| connection.pragma_update(None, "mmap_size", 0_i64))
        .and_then(|_| connection.pragma_update(None, "cache_spill", true))
        .map_err(|error| sqlite_error("configure canonical task index", error))?;
    let cache_kib = cache_bytes.div_ceil(1024).max(1);
    connection
        .pragma_update(
            None,
            "cache_size",
            -i64::try_from(cache_kib).unwrap_or(i64::MAX),
        )
        .map_err(|error| sqlite_error("configure canonical task index cache", error))
}

pub(crate) fn set_page_ceiling(connection: &Connection, reserved_bytes: u64) -> Result<()> {
    let pages = reserved_bytes / SQLITE_PAGE_BYTES;
    if pages < 2 {
        return Err(CdfError::data(
            "canonical task index spill reservation cannot hold two SQLite pages",
        ));
    }
    connection
        .pragma_update(
            None,
            "max_page_count",
            i64::try_from(pages).unwrap_or(i64::MAX),
        )
        .map_err(|error| sqlite_error("raise canonical task index page ceiling", error))
}

pub(crate) fn is_sqlite_constraint(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(failure, _)
            if failure.code == ErrorCode::ConstraintViolation
    )
}

pub(crate) fn sqlite_page_count(connection: &Connection) -> Result<u64> {
    let page_count: i64 = connection
        .query_row("PRAGMA page_count", [], |row| row.get(0))
        .map_err(|error| sqlite_error("inspect canonical task page count", error))?;
    u64::try_from(page_count)
        .map_err(|_| CdfError::internal("canonical task SQLite page count is negative"))
}

pub(crate) fn sqlite_single_leaf_fits(
    connection: &Connection,
    tree_name: &str,
    required_bytes: u64,
) -> Result<bool> {
    if required_bytes > SQLITE_INDEX_MAX_LOCAL_PAYLOAD_BYTES {
        return Ok(false);
    }
    let mut statement = connection
        .prepare("SELECT pagetype, unused FROM dbstat WHERE name = ?1 LIMIT 2")
        .map_err(|error| sqlite_error("prepare canonical task leaf inspection", error))?;
    let mut rows = statement
        .query(params![tree_name])
        .map_err(|error| sqlite_error("inspect canonical task leaf capacity", error))?;
    let Some(row) = rows
        .next()
        .map_err(|error| sqlite_error("read canonical task leaf capacity", error))?
    else {
        return Err(CdfError::internal(format!(
            "canonical task SQLite tree `{tree_name}` has no root page"
        )));
    };
    let page_type: String = row
        .get(0)
        .map_err(|error| sqlite_error("read canonical task page type", error))?;
    let unused_bytes: i64 = row
        .get(1)
        .map_err(|error| sqlite_error("read canonical task unused bytes", error))?;
    let unused_bytes = u64::try_from(unused_bytes)
        .map_err(|_| CdfError::internal("canonical task SQLite unused bytes are negative"))?;
    let has_second_page = rows
        .next()
        .map_err(|error| sqlite_error("read canonical task leaf count", error))?
        .is_some();
    Ok(page_type == "leaf" && !has_second_page && unused_bytes >= required_bytes)
}

pub(crate) fn sqlite_error(action: &str, error: rusqlite::Error) -> CdfError {
    if sqlite_host_error(&error) {
        CdfError::environment(format!(
            "{action} in CDF-managed task scratch: {error}; check temporary storage, permissions, free space, memory, and process file limits before retrying"
        ))
    } else {
        CdfError::internal(format!("{action} in CDF-managed task scratch: {error}"))
    }
}

pub(crate) fn is_sqlite_full(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(failure, _) if failure.code == ErrorCode::DiskFull
    )
}

fn sqlite_host_error(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(failure, _)
            if matches!(
                failure.code,
                ErrorCode::PermissionDenied
                    | ErrorCode::OutOfMemory
                    | ErrorCode::ReadOnly
                    | ErrorCode::SystemIoFailure
                    | ErrorCode::NotFound
                    | ErrorCode::DiskFull
                    | ErrorCode::CannotOpen
                    | ErrorCode::FileLockingProtocolFailed
                    | ErrorCode::NoLargeFileSupport
                    | ErrorCode::AuthorizationForStatementDenied
            )
    )
}
