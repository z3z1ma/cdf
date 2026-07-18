use crate::*;

use std::{
    ffi::{CStr, CString},
    os::raw::{c_char, c_void},
    path::Path,
    ptr,
};

use arrow_array::ffi_stream::FFI_ArrowArrayStream;

#[derive(Clone, Copy, Debug)]
pub(crate) enum RawDuckDbParam<'a> {
    I64(i64),
    U64(u64),
    Varchar(&'a str),
    Null,
}

pub(crate) struct RawDuckDbConnection {
    database: duckdb::ffi::duckdb_database,
    connection: duckdb::ffi::duckdb_connection,
}

// SAFETY: CDF holds a single writer lane for DuckDB staged ingress, and this wrapper owns
// exactly one DuckDB connection handle that is disconnected on drop. Moving the wrapper to
// the lane's worker thread is sound; concurrent use is not exposed because all methods require
// `&mut self`.
unsafe impl Send for RawDuckDbConnection {}

impl std::fmt::Debug for RawDuckDbConnection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RawDuckDbConnection")
            .field("database", &self.database)
            .field("connection", &self.connection)
            .finish()
    }
}

impl RawDuckDbConnection {
    pub(crate) fn open(path: &Path) -> Result<Self> {
        let path = duckdb_cstring("DuckDB database path", &path.display().to_string())?;
        let mut database = ptr::null_mut();
        let mut connection = ptr::null_mut();
        // SAFETY: `path` is a live NUL-terminated string for this call, and DuckDB
        // initializes the output database handle or reports an error. This wrapper
        // owns a successful handle and closes it in `Drop`.
        let open_state = unsafe { duckdb::ffi::duckdb_open(path.as_ptr(), &mut database) };
        if open_state != duckdb::ffi::DuckDBSuccess {
            return Err(CdfError::destination("DuckDB raw open failed"));
        }
        // SAFETY: `database` is a valid handle returned by `duckdb_open`.
        let connect_state = unsafe { duckdb::ffi::duckdb_connect(database, &mut connection) };
        if connect_state != duckdb::ffi::DuckDBSuccess {
            // SAFETY: `database` was returned by `duckdb_open` and has not been closed yet.
            unsafe {
                duckdb::ffi::duckdb_close(&mut database);
            }
            return Err(CdfError::destination("DuckDB raw connect failed"));
        }
        Ok(Self {
            database,
            connection,
        })
    }

    pub(crate) fn execute(&mut self, sql: impl AsRef<str>) -> Result<()> {
        let mut result = self.query_result(sql.as_ref())?;
        result.destroy();
        Ok(())
    }

    pub(crate) fn execute_prepared(
        &mut self,
        sql: impl AsRef<str>,
        params: &[RawDuckDbParam<'_>],
    ) -> Result<usize> {
        let mut statement = self.prepare(sql.as_ref())?;
        let mut result = statement.execute(params)?;
        let changed = result.rows_changed()?;
        result.destroy();
        Ok(changed)
    }

    pub(crate) fn query_optional_string(
        &mut self,
        sql: impl AsRef<str>,
        params: &[RawDuckDbParam<'_>],
    ) -> Result<Option<String>> {
        let mut statement = self.prepare(sql.as_ref())?;
        let mut result = statement.execute(params)?;
        let output = if result.row_count() == 0 {
            None
        } else {
            Some(result.value_string(0, 0)?)
        };
        result.destroy();
        Ok(output)
    }

    pub(crate) fn query_u64(
        &mut self,
        sql: impl AsRef<str>,
        params: &[RawDuckDbParam<'_>],
    ) -> Result<u64> {
        let mut statement = self.prepare(sql.as_ref())?;
        let mut result = statement.execute(params)?;
        if result.row_count() != 1 || result.column_count() != 1 {
            result.destroy();
            return Err(CdfError::destination(
                "DuckDB raw scalar query did not return exactly one value",
            ));
        }
        let output = result.value_u64(0, 0)?;
        result.destroy();
        Ok(output)
    }

    pub(crate) fn configure_resources(
        &mut self,
        memory_limit_bytes: u64,
        maximum_temp_directory_bytes: u64,
        internal_threads: i64,
    ) -> Result<()> {
        if memory_limit_bytes == 0 || maximum_temp_directory_bytes == 0 || internal_threads <= 0 {
            return Err(CdfError::contract(
                "DuckDB raw resources require positive memory, temp, and thread settings",
            ));
        }
        self.execute(format!("SET memory_limit = '{}B'", memory_limit_bytes))?;
        self.execute(format!(
            "SET max_temp_directory_size = '{}B'",
            maximum_temp_directory_bytes
        ))?;
        self.execute(format!("SET threads = {internal_threads}"))?;
        self.execute("SET preserve_insertion_order = false")
    }

    pub(crate) fn register_arrow_stream_scan(
        &mut self,
        view_name: &str,
        stream: &mut FFI_ArrowArrayStream,
    ) -> Result<()> {
        let view_name = duckdb_cstring("DuckDB Arrow stream view name", view_name)?;
        // SAFETY: DuckDB 1.10504's deprecated `duckdb_arrow_scan` accepts the opaque
        // `duckdb_arrow_stream` type but immediately treats it as an Arrow C Stream
        // Interface pointer. `arrow-rs` owns and releases this stream; DuckDB borrows it
        // for the registered scan. Callers must keep `stream` alive until the SQL statement
        // that consumes the view has completed, and must not call DuckDB's destroy function
        // for this borrowed stream.
        let state = unsafe {
            duckdb::ffi::duckdb_arrow_scan(
                self.connection,
                view_name.as_ptr(),
                (stream as *mut FFI_ArrowArrayStream).cast::<duckdb::ffi::_duckdb_arrow_stream>(),
            )
        };
        if state == duckdb::ffi::DuckDBSuccess {
            Ok(())
        } else {
            Err(CdfError::destination(
                "DuckDB Arrow stream-scan registration failed",
            ))
        }
    }

    fn query_result(&mut self, sql: &str) -> Result<RawDuckDbResult> {
        let sql = duckdb_cstring("DuckDB SQL", sql)?;
        let mut result = unsafe { std::mem::zeroed::<duckdb::ffi::duckdb_result>() };
        // SAFETY: the connection is owned by this wrapper and `sql` is a live
        // NUL-terminated string. DuckDB initializes `result`; the wrapper destroys it
        // on every path as required by the C API.
        let state =
            unsafe { duckdb::ffi::duckdb_query(self.connection, sql.as_ptr(), &mut result) };
        if state == duckdb::ffi::DuckDBSuccess {
            Ok(RawDuckDbResult {
                result,
                destroyed: false,
            })
        } else {
            let message = duckdb_result_error_message(&mut result);
            // SAFETY: `duckdb_query` requires result destruction even when the state is an error.
            unsafe {
                duckdb::ffi::duckdb_destroy_result(&mut result);
            }
            Err(CdfError::destination(format!(
                "DuckDB raw query failed: {message}"
            )))
        }
    }

    fn prepare(&mut self, sql: &str) -> Result<RawDuckDbStatement> {
        let sql = duckdb_cstring("DuckDB prepared SQL", sql)?;
        let mut statement = ptr::null_mut();
        // SAFETY: the connection is owned by this wrapper and `sql` is a live
        // NUL-terminated string. DuckDB initializes `statement`; the wrapper destroys it.
        let state =
            unsafe { duckdb::ffi::duckdb_prepare(self.connection, sql.as_ptr(), &mut statement) };
        if state == duckdb::ffi::DuckDBSuccess && !statement.is_null() {
            Ok(RawDuckDbStatement { statement })
        } else {
            let message = if statement.is_null() {
                "unknown error".to_owned()
            } else {
                duckdb_prepare_error_message(statement)
            };
            // SAFETY: DuckDB permits destroying a prepared statement handle returned from
            // `duckdb_prepare`, including failed prepares.
            unsafe {
                duckdb::ffi::duckdb_destroy_prepare(&mut statement);
            }
            Err(CdfError::destination(format!(
                "DuckDB raw prepare failed: {message}"
            )))
        }
    }
}

impl Drop for RawDuckDbConnection {
    fn drop(&mut self) {
        // SAFETY: both handles are owned by this wrapper and are disconnected/closed at
        // most once because DuckDB nulls the pointers through these APIs.
        unsafe {
            if !self.connection.is_null() {
                duckdb::ffi::duckdb_disconnect(&mut self.connection);
            }
            if !self.database.is_null() {
                duckdb::ffi::duckdb_close(&mut self.database);
            }
        }
    }
}

struct RawDuckDbStatement {
    statement: duckdb::ffi::duckdb_prepared_statement,
}

impl RawDuckDbStatement {
    fn execute(&mut self, params: &[RawDuckDbParam<'_>]) -> Result<RawDuckDbResult> {
        bind_params(self.statement, params)?;
        let mut result = unsafe { std::mem::zeroed::<duckdb::ffi::duckdb_result>() };
        // SAFETY: `statement` is owned by this wrapper, parameters have been bound, and
        // DuckDB initializes `result`; the result wrapper destroys it.
        let state = unsafe { duckdb::ffi::duckdb_execute_prepared(self.statement, &mut result) };
        if state == duckdb::ffi::DuckDBSuccess {
            Ok(RawDuckDbResult {
                result,
                destroyed: false,
            })
        } else {
            let message = duckdb_result_error_message(&mut result);
            // SAFETY: `duckdb_execute_prepared` requires result destruction even on error.
            unsafe {
                duckdb::ffi::duckdb_destroy_result(&mut result);
            }
            Err(CdfError::destination(format!(
                "DuckDB raw prepared execution failed: {message}"
            )))
        }
    }
}

impl Drop for RawDuckDbStatement {
    fn drop(&mut self) {
        // SAFETY: the statement is owned by this wrapper.
        unsafe {
            if !self.statement.is_null() {
                duckdb::ffi::duckdb_destroy_prepare(&mut self.statement);
            }
        }
    }
}

struct RawDuckDbResult {
    result: duckdb::ffi::duckdb_result,
    destroyed: bool,
}

impl RawDuckDbResult {
    fn row_count(&mut self) -> u64 {
        // SAFETY: `result` is initialized and owned by this wrapper.
        unsafe { duckdb::ffi::duckdb_row_count(&mut self.result) }
    }

    fn column_count(&mut self) -> u64 {
        // SAFETY: `result` is initialized and owned by this wrapper.
        unsafe { duckdb::ffi::duckdb_column_count(&mut self.result) }
    }

    fn rows_changed(&mut self) -> Result<usize> {
        // SAFETY: `result` is initialized and owned by this wrapper.
        let changed = unsafe { duckdb::ffi::duckdb_rows_changed(&mut self.result) };
        usize::try_from(changed)
            .map_err(|_| CdfError::destination("DuckDB changed-row count exceeds usize"))
    }

    fn value_u64(&mut self, col: u64, row: u64) -> Result<u64> {
        self.ensure_cell(col, row)?;
        // SAFETY: bounds are checked by `ensure_cell`; DuckDB performs scalar conversion.
        Ok(unsafe { duckdb::ffi::duckdb_value_uint64(&mut self.result, col, row) })
    }

    fn value_string(&mut self, col: u64, row: u64) -> Result<String> {
        self.ensure_cell(col, row)?;
        // SAFETY: bounds are checked by `ensure_cell`; DuckDB returns an owned C string
        // that must be released with `duckdb_free`.
        let pointer = unsafe { duckdb::ffi::duckdb_value_varchar(&mut self.result, col, row) };
        if pointer.is_null() {
            return Err(CdfError::destination(
                "DuckDB raw string value returned null",
            ));
        }
        let output = unsafe { CStr::from_ptr(pointer) }
            .to_string_lossy()
            .into_owned();
        // SAFETY: `duckdb_value_varchar` returns an owned allocation.
        unsafe {
            duckdb::ffi::duckdb_free(pointer.cast::<c_void>());
        }
        Ok(output)
    }

    fn ensure_cell(&mut self, col: u64, row: u64) -> Result<()> {
        if row >= self.row_count() || col >= self.column_count() {
            return Err(CdfError::destination(
                "DuckDB raw result cell is outside query bounds",
            ));
        }
        Ok(())
    }

    fn destroy(&mut self) {
        if !self.destroyed {
            // SAFETY: `result` is initialized and owned by this wrapper.
            unsafe {
                duckdb::ffi::duckdb_destroy_result(&mut self.result);
            }
            self.destroyed = true;
        }
    }
}

impl Drop for RawDuckDbResult {
    fn drop(&mut self) {
        self.destroy();
    }
}

fn bind_params(
    statement: duckdb::ffi::duckdb_prepared_statement,
    params: &[RawDuckDbParam<'_>],
) -> Result<()> {
    let mut cstrings = Vec::new();
    for (index, param) in params.iter().enumerate() {
        let index = u64::try_from(index + 1)
            .map_err(|_| CdfError::destination("DuckDB parameter index exceeds u64"))?;
        let state = match param {
            RawDuckDbParam::I64(value) => {
                // SAFETY: `statement` is live and `index` is one-based by DuckDB contract.
                unsafe { duckdb::ffi::duckdb_bind_int64(statement, index, *value) }
            }
            RawDuckDbParam::U64(value) => {
                // SAFETY: `statement` is live and `index` is one-based by DuckDB contract.
                unsafe { duckdb::ffi::duckdb_bind_uint64(statement, index, *value) }
            }
            RawDuckDbParam::Varchar(value) => {
                cstrings.push(duckdb_cstring("DuckDB varchar parameter", value)?);
                let pointer = cstrings
                    .last()
                    .expect("just pushed DuckDB parameter")
                    .as_ptr();
                // SAFETY: `statement` is live, `index` is one-based, and `pointer` remains
                // valid through prepared execution because `cstrings` lives until all binds
                // finish and DuckDB copies bound varchar data for prepared execution.
                unsafe { duckdb::ffi::duckdb_bind_varchar(statement, index, pointer) }
            }
            RawDuckDbParam::Null => {
                // SAFETY: `statement` is live and `index` is one-based by DuckDB contract.
                unsafe { duckdb::ffi::duckdb_bind_null(statement, index) }
            }
        };
        if state != duckdb::ffi::DuckDBSuccess {
            return Err(CdfError::destination(format!(
                "DuckDB raw bind failed for parameter {index}"
            )));
        }
    }
    Ok(())
}

fn duckdb_cstring(label: &str, value: &str) -> Result<CString> {
    CString::new(value).map_err(|_| CdfError::contract(format!("{label} contains an interior NUL")))
}

fn duckdb_result_error_message(result: *mut duckdb::ffi::duckdb_result) -> String {
    // SAFETY: `result` is an initialized DuckDB result object; DuckDB owns the returned
    // pointer until the result is destroyed.
    let pointer = unsafe { duckdb::ffi::duckdb_result_error(result) };
    cstr_message(pointer)
}

fn duckdb_prepare_error_message(statement: duckdb::ffi::duckdb_prepared_statement) -> String {
    // SAFETY: `statement` is a DuckDB prepared statement handle returned by `duckdb_prepare`.
    let pointer = unsafe { duckdb::ffi::duckdb_prepare_error(statement) };
    cstr_message(pointer)
}

fn cstr_message(pointer: *const c_char) -> String {
    if pointer.is_null() {
        "unknown error".to_owned()
    } else {
        // SAFETY: DuckDB returns a valid NUL-terminated diagnostic pointer for the
        // lifetime documented by the calling function.
        unsafe { CStr::from_ptr(pointer) }
            .to_string_lossy()
            .into_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow_array::{ArrayRef, RecordBatchIterator, StringArray, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};

    #[test]
    fn raw_arrow_stream_scan_materializes_borrowed_arrow_stream() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("stream.duckdb");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let first = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from_iter_values([1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("ada"), Some("grace")])) as ArrayRef,
            ],
        )
        .unwrap();
        let second = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from_iter_values([3])) as ArrayRef,
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(first), Ok(second)].into_iter(), schema);
        let mut stream = FFI_ArrowArrayStream::new(Box::new(reader));
        let mut connection = RawDuckDbConnection::open(&path).unwrap();

        connection
            .register_arrow_stream_scan("cdf_arrow_stream", &mut stream)
            .unwrap();
        connection
            .execute("CREATE TABLE stream_scan AS SELECT * FROM cdf_arrow_stream")
            .unwrap();

        let count = connection
            .query_u64("SELECT count(*) FROM stream_scan", &[])
            .unwrap();
        assert_eq!(count, 3);
        let names = connection
            .query_optional_string(
                "SELECT string_agg(coalesce(name, '<null>'), ',' ORDER BY id) FROM stream_scan",
                &[],
            )
            .unwrap();
        assert_eq!(names.as_deref(), Some("ada,grace,<null>"));
        drop(stream);
    }
}
