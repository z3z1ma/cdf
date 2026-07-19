use std::{
    ffi::{CStr, CString},
    fs::File,
    io::BufReader,
    mem::{ManuallyDrop, align_of, size_of},
    os::raw::{c_char, c_void},
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    ptr,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use arrow_array::{Array, StructArray, ffi::FFI_ArrowArray};
use arrow_ipc::reader::FileReader as IpcFileReader;
use arrow_schema::{DataType, Schema, SchemaRef, TimeUnit, ffi::FFI_ArrowSchema};
use cdf_kernel::{CdfError, Result};

use crate::{
    CDF_ROW_KEY_COLUMN, CDF_STAGE_ORDER_COLUMN, DuckDbCommitWriter, DuckDbNativeResources,
    package::duckdb_type, sql::quote_ident,
};

pub(crate) const SEGMENT_SCAN_FUNCTION: &str = "__cdf_canonical_segments";

pub(crate) fn ingest_canonical_segments(
    writer: &mut DuckDbCommitWriter,
    expected_rows: u64,
    merge: bool,
) -> Result<()> {
    if expected_rows == 0 {
        return Err(CdfError::internal(
            "DuckDB canonical segment scan requires nonempty input",
        ));
    }
    let first_row_key = writer
        .first_row_key
        .ok_or_else(|| CdfError::internal("DuckDB row-key allocator is not initialized"))?;
    let mut insert_columns = writer
        .persisted_fields
        .iter()
        .map(|field| quote_ident(&field.name))
        .collect::<Vec<_>>();
    let mut select_columns = writer.persisted_fields[..writer.user_field_count]
        .iter()
        .map(|field| quote_ident(&field.name))
        .collect::<Vec<_>>();
    select_columns.push(format!(
        "CAST({first_row_key} + {} AS UBIGINT) AS {}",
        quote_ident(cdf_package_contract::CDF_PACKAGE_ROW_ORD_FIELD),
        quote_ident(CDF_ROW_KEY_COLUMN),
    ));
    if merge {
        insert_columns.push(quote_ident(CDF_STAGE_ORDER_COLUMN));
        select_columns.push(quote_ident(cdf_package_contract::CDF_PACKAGE_ROW_ORD_FIELD));
    }
    let sql = format!(
        "INSERT INTO {} ({}) SELECT {} FROM {}()",
        writer.write_target.sql_name(),
        insert_columns.join(", "),
        select_columns.join(", "),
        SEGMENT_SCAN_FUNCTION,
    );
    let rows = writer.conn.execute(&sql, []).map_err(|error| {
        CdfError::destination(format!(
            "ingest canonical Arrow IPC segments into DuckDB: {error}"
        ))
    })?;
    let rows = u64::try_from(rows)
        .map_err(|_| CdfError::data("DuckDB canonical segment row count exceeds u64"))?;
    let scanned = writer.segment_scan.rows_scanned();
    if rows != expected_rows || scanned != expected_rows {
        return Err(CdfError::data(format!(
            "DuckDB canonical scan inserted {rows} and decoded {scanned} rows but segment identities require {expected_rows}"
        )));
    }
    writer.rows_received = rows;
    Ok(())
}

pub(crate) struct DuckDbSegmentScanRuntime {
    database: duckdb::ffi::duckdb_database,
    registration_connection: duckdb::ffi::duckdb_connection,
    telemetry: Arc<SegmentScanTelemetry>,
}

impl std::fmt::Debug for DuckDbSegmentScanRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DuckDbSegmentScanRuntime")
            .field("rows", &self.telemetry.rows.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl DuckDbSegmentScanRuntime {
    pub(crate) fn open(
        path: &Path,
        resources: &DuckDbNativeResources,
        paths: Vec<PathBuf>,
        schema: SchemaRef,
    ) -> Result<Self> {
        if paths.is_empty() {
            return Err(CdfError::internal(
                "DuckDB canonical segment scan requires at least one file",
            ));
        }
        let path = path_to_cstring(path)?;
        let mut config = ptr::null_mut();
        let mut database = ptr::null_mut();
        let mut registration_connection = ptr::null_mut();
        let mut open_error = ptr::null_mut();
        // SAFETY: every handle is initialized by DuckDB and is either moved
        // into the returned owner or released on the error path below.
        unsafe {
            if duckdb::ffi::duckdb_create_config(&mut config) != duckdb::ffi::DuckDBSuccess {
                return Err(CdfError::destination(
                    "create bounded DuckDB segment-scan configuration",
                ));
            }
            for (name, value) in crate::api::duckdb_config_options(resources) {
                let name = cstring(&name)?;
                let value = cstring(&value)?;
                if duckdb::ffi::duckdb_set_config(config, name.as_ptr(), value.as_ptr())
                    != duckdb::ffi::DuckDBSuccess
                {
                    duckdb::ffi::duckdb_destroy_config(&mut config);
                    return Err(CdfError::destination(format!(
                        "configure DuckDB option {name:?} for canonical segment scan"
                    )));
                }
            }
            let state =
                duckdb::ffi::duckdb_open_ext(path.as_ptr(), &mut database, config, &mut open_error);
            duckdb::ffi::duckdb_destroy_config(&mut config);
            if state != duckdb::ffi::DuckDBSuccess {
                let message = cstr_message(open_error);
                if !open_error.is_null() {
                    duckdb::ffi::duckdb_free(open_error.cast::<c_void>());
                }
                return Err(CdfError::destination(format!(
                    "open DuckDB canonical segment-scan runtime: {message}"
                )));
            }
            if duckdb::ffi::duckdb_connect(database, &mut registration_connection)
                != duckdb::ffi::DuckDBSuccess
            {
                duckdb::ffi::duckdb_close(&mut database);
                return Err(CdfError::destination(
                    "connect DuckDB canonical segment-scan registration runtime",
                ));
            }
        }
        let telemetry = Arc::new(SegmentScanTelemetry {
            rows: AtomicU64::new(0),
        });
        let registration = register_segment_scan(
            registration_connection,
            paths,
            schema,
            usize::try_from(resources.internal_threads.max(1)).unwrap_or(usize::MAX),
            telemetry.clone(),
        );
        if let Err(error) = registration {
            // SAFETY: both successful handles are still owned locally.
            unsafe {
                duckdb::ffi::duckdb_disconnect(&mut registration_connection);
                duckdb::ffi::duckdb_close(&mut database);
            }
            return Err(error);
        }
        Ok(Self {
            database,
            registration_connection,
            telemetry,
        })
    }

    pub(crate) fn connection(&self) -> Result<duckdb::Connection> {
        // SAFETY: the runtime owns a live database handle for longer than the
        // returned connection. `open_from_raw` creates a distinct connection
        // and does not take ownership of the database handle.
        unsafe { duckdb::Connection::open_from_raw(self.database) }
            .map_err(|error| CdfError::destination(format!("connect DuckDB writer: {error}")))
    }

    pub(crate) fn rows_scanned(&self) -> u64 {
        self.telemetry.rows.load(Ordering::Relaxed)
    }
}

impl Drop for DuckDbSegmentScanRuntime {
    fn drop(&mut self) {
        // SAFETY: this owner releases the registration connection before the
        // database and sets both handles to null through DuckDB's APIs.
        unsafe {
            if !self.registration_connection.is_null() {
                duckdb::ffi::duckdb_disconnect(&mut self.registration_connection);
            }
            if !self.database.is_null() {
                duckdb::ffi::duckdb_close(&mut self.database);
            }
        }
    }
}

struct SegmentScanTelemetry {
    rows: AtomicU64,
}

struct SegmentScanContext {
    paths: Vec<PathBuf>,
    schema: SchemaRef,
    connection: duckdb::ffi::duckdb_connection,
    converted_schema: ConvertedSchema,
    next_path: AtomicUsize,
    max_threads: usize,
    telemetry: Arc<SegmentScanTelemetry>,
}

struct SegmentScanLocalState {
    reader: Option<IpcFileReader<BufReader<File>>>,
    batch: Option<arrow_array::RecordBatch>,
    batch_offset: usize,
}

impl SegmentScanLocalState {
    fn next_slice(
        &mut self,
        context: &SegmentScanContext,
        vector_rows: usize,
    ) -> Result<Option<arrow_array::RecordBatch>> {
        loop {
            if let Some(batch) = self.batch.as_ref() {
                if self.batch_offset < batch.num_rows() {
                    let rows = vector_rows.min(batch.num_rows() - self.batch_offset);
                    let slice = batch.slice(self.batch_offset, rows);
                    self.batch_offset += rows;
                    return Ok(Some(slice));
                }
                self.batch = None;
                self.batch_offset = 0;
            }
            if let Some(reader) = self.reader.as_mut() {
                match reader.next() {
                    Some(Ok(batch)) => {
                        if batch.schema().as_ref() != context.schema.as_ref() {
                            return Err(CdfError::data(
                                "DuckDB canonical segment batch schema differs from its bound schema",
                            ));
                        }
                        context.telemetry.rows.fetch_add(
                            u64::try_from(batch.num_rows()).map_err(|_| {
                                CdfError::data("DuckDB canonical segment rows exceed u64")
                            })?,
                            Ordering::Relaxed,
                        );
                        self.batch = Some(batch);
                        continue;
                    }
                    Some(Err(error)) => {
                        return Err(CdfError::data(format!(
                            "decode DuckDB canonical Arrow IPC segment: {error}"
                        )));
                    }
                    None => self.reader = None,
                }
            }
            let index = context.next_path.fetch_add(1, Ordering::Relaxed);
            let Some(path) = context.paths.get(index) else {
                return Ok(None);
            };
            let reader = IpcFileReader::try_new_buffered(
                File::open(path).map_err(|error| {
                    CdfError::data(format!(
                        "open canonical segment {} for DuckDB: {error}",
                        path.display()
                    ))
                })?,
                None,
            )
            .map_err(|error| {
                CdfError::data(format!(
                    "open canonical Arrow IPC segment {} for DuckDB: {error}",
                    path.display()
                ))
            })?;
            if reader.schema().as_ref() != context.schema.as_ref() {
                return Err(CdfError::data(format!(
                    "DuckDB canonical segment {} schema differs from its bound schema",
                    path.display()
                )));
            }
            self.reader = Some(reader);
        }
    }
}

struct ConvertedSchema {
    handle: duckdb::ffi::duckdb_arrow_converted_schema,
}

impl ConvertedSchema {
    fn new(connection: duckdb::ffi::duckdb_connection, schema: &Schema) -> Result<Self> {
        assert_c_data_layout();
        let mut arrow_schema = FFI_ArrowSchema::try_from(schema).map_err(|error| {
            CdfError::destination(format!("export canonical Arrow schema to DuckDB: {error}"))
        })?;
        let mut handle = ptr::null_mut();
        // SAFETY: the ABI layout is asserted and DuckDB copies/converts the
        // live Arrow schema into an independently owned handle.
        let error = unsafe {
            duckdb::ffi::duckdb_schema_from_arrow(
                connection,
                (&mut arrow_schema as *mut FFI_ArrowSchema).cast::<duckdb::ffi::ArrowSchema>(),
                &mut handle,
            )
        };
        duckdb_error_data_result(error, "convert canonical Arrow schema")?;
        if handle.is_null() {
            return Err(CdfError::destination(
                "DuckDB canonical schema conversion returned null",
            ));
        }
        Ok(Self { handle })
    }
}

impl Drop for ConvertedSchema {
    fn drop(&mut self) {
        // SAFETY: this wrapper owns the converted schema exactly once.
        unsafe {
            if !self.handle.is_null() {
                duckdb::ffi::duckdb_destroy_arrow_converted_schema(&mut self.handle);
            }
        }
    }
}

struct LogicalType {
    handle: duckdb::ffi::duckdb_logical_type,
}

impl LogicalType {
    fn from_arrow(data_type: &DataType) -> Result<Self> {
        duckdb_type(data_type)?;
        use duckdb::ffi::*;
        let primitive = match data_type {
            DataType::Boolean => Some(DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN),
            DataType::Int8 => Some(DUCKDB_TYPE_DUCKDB_TYPE_TINYINT),
            DataType::Int16 => Some(DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT),
            DataType::Int32 => Some(DUCKDB_TYPE_DUCKDB_TYPE_INTEGER),
            DataType::Int64 => Some(DUCKDB_TYPE_DUCKDB_TYPE_BIGINT),
            DataType::UInt8 => Some(DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT),
            DataType::UInt16 => Some(DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT),
            DataType::UInt32 => Some(DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER),
            DataType::UInt64 => Some(DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT),
            DataType::Float32 => Some(DUCKDB_TYPE_DUCKDB_TYPE_FLOAT),
            DataType::Float64 => Some(DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE),
            DataType::Utf8 | DataType::LargeUtf8 => Some(DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR),
            DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
                Some(DUCKDB_TYPE_DUCKDB_TYPE_BLOB)
            }
            DataType::Date32 => Some(DUCKDB_TYPE_DUCKDB_TYPE_DATE),
            DataType::Time32(_) | DataType::Time64(TimeUnit::Microsecond) => {
                Some(DUCKDB_TYPE_DUCKDB_TYPE_TIME)
            }
            DataType::Timestamp(TimeUnit::Second, None) => {
                Some(DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S)
            }
            DataType::Timestamp(TimeUnit::Millisecond, None) => {
                Some(DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS)
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                Some(DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP)
            }
            DataType::Decimal128(precision, scale) => {
                // SAFETY: `duckdb_type` already validated width and scale.
                let handle = unsafe {
                    duckdb_create_decimal_type(
                        *precision,
                        u8::try_from(*scale).map_err(|_| {
                            CdfError::contract("DuckDB decimal scale must be nonnegative")
                        })?,
                    )
                };
                return Self::owned(handle, data_type);
            }
            DataType::List(field) | DataType::LargeList(field) => {
                let child = Self::from_arrow(field.data_type())?;
                // SAFETY: DuckDB copies the live child logical type.
                return Self::owned(unsafe { duckdb_create_list_type(child.handle) }, data_type);
            }
            DataType::FixedSizeList(field, size) => {
                let child = Self::from_arrow(field.data_type())?;
                // SAFETY: the positive size was validated by `duckdb_type`.
                return Self::owned(
                    unsafe {
                        duckdb_create_array_type(
                            child.handle,
                            u64::try_from(*size).map_err(|_| {
                                CdfError::contract("DuckDB array size must be positive")
                            })?,
                        )
                    },
                    data_type,
                );
            }
            DataType::Struct(fields) => {
                let mut children = fields
                    .iter()
                    .map(|field| Self::from_arrow(field.data_type()))
                    .collect::<Result<Vec<_>>>()?;
                let names = fields
                    .iter()
                    .map(|field| cstring(field.name()))
                    .collect::<Result<Vec<_>>>()?;
                let mut handles = children
                    .iter_mut()
                    .map(|child| child.handle)
                    .collect::<Vec<_>>();
                let mut pointers = names.iter().map(|name| name.as_ptr()).collect::<Vec<_>>();
                // SAFETY: all arrays stay live for this call; DuckDB copies them.
                return Self::owned(
                    unsafe {
                        duckdb_create_struct_type(
                            handles.as_mut_ptr(),
                            pointers.as_mut_ptr(),
                            u64::try_from(handles.len()).map_err(|_| {
                                CdfError::contract("DuckDB struct field count exceeds u64")
                            })?,
                        )
                    },
                    data_type,
                );
            }
            DataType::Map(entries, _) => {
                let DataType::Struct(fields) = entries.data_type() else {
                    unreachable!("validated by duckdb_type")
                };
                let key = Self::from_arrow(fields[0].data_type())?;
                let value = Self::from_arrow(fields[1].data_type())?;
                // SAFETY: DuckDB copies both live child types.
                return Self::owned(
                    unsafe { duckdb_create_map_type(key.handle, value.handle) },
                    data_type,
                );
            }
            _ => None,
        };
        let primitive = primitive.ok_or_else(|| {
            CdfError::contract(format!(
                "DuckDB canonical segment scan cannot bind Arrow type {data_type:?}"
            ))
        })?;
        // SAFETY: all complex types were handled above.
        Self::owned(unsafe { duckdb_create_logical_type(primitive) }, data_type)
    }

    fn owned(handle: duckdb::ffi::duckdb_logical_type, data_type: &DataType) -> Result<Self> {
        if handle.is_null() {
            Err(CdfError::destination(format!(
                "DuckDB logical type creation returned null for {data_type:?}"
            )))
        } else {
            Ok(Self { handle })
        }
    }
}

impl Drop for LogicalType {
    fn drop(&mut self) {
        // SAFETY: this wrapper owns its handle exactly once.
        unsafe {
            if !self.handle.is_null() {
                duckdb::ffi::duckdb_destroy_logical_type(&mut self.handle);
            }
        }
    }
}

fn register_segment_scan(
    connection: duckdb::ffi::duckdb_connection,
    paths: Vec<PathBuf>,
    schema: SchemaRef,
    max_threads: usize,
    telemetry: Arc<SegmentScanTelemetry>,
) -> Result<()> {
    assert_c_data_layout();
    let converted_schema = ConvertedSchema::new(connection, schema.as_ref())?;
    let context = Box::new(SegmentScanContext {
        paths,
        schema,
        connection,
        converted_schema,
        next_path: AtomicUsize::new(0),
        max_threads,
        telemetry,
    });
    let name = cstring(SEGMENT_SCAN_FUNCTION)?;
    // SAFETY: callbacks use the C ABI, contain panics, and DuckDB owns the
    // context through the registered exact-once destructor.
    unsafe {
        let mut function = duckdb::ffi::duckdb_create_table_function();
        if function.is_null() {
            return Err(CdfError::destination(
                "create DuckDB canonical segment table function",
            ));
        }
        duckdb::ffi::duckdb_table_function_set_name(function, name.as_ptr());
        duckdb::ffi::duckdb_table_function_set_extra_info(
            function,
            Box::into_raw(context).cast::<c_void>(),
            Some(drop_context),
        );
        duckdb::ffi::duckdb_table_function_set_bind(function, Some(bind));
        duckdb::ffi::duckdb_table_function_set_init(function, Some(init));
        duckdb::ffi::duckdb_table_function_set_local_init(function, Some(local_init));
        duckdb::ffi::duckdb_table_function_set_function(function, Some(scan));
        let state = duckdb::ffi::duckdb_register_table_function(connection, function);
        duckdb::ffi::duckdb_destroy_table_function(&mut function);
        if state != duckdb::ffi::DuckDBSuccess {
            return Err(CdfError::destination(
                "register DuckDB canonical segment table function",
            ));
        }
    }
    Ok(())
}

unsafe extern "C" fn drop_context(data: *mut c_void) {
    if !data.is_null() {
        // SAFETY: this is the exact pointer transferred at registration.
        unsafe { drop(Box::from_raw(data.cast::<SegmentScanContext>())) };
    }
}

unsafe extern "C" fn drop_local_state(data: *mut c_void) {
    if !data.is_null() {
        // SAFETY: this is the exact pointer transferred during local init.
        unsafe { drop(Box::from_raw(data.cast::<SegmentScanLocalState>())) };
    }
}

unsafe extern "C" fn bind(info: duckdb::ffi::duckdb_bind_info) {
    let result = catch_unwind(AssertUnwindSafe(|| -> Result<()> {
        let context = unsafe { context(duckdb::ffi::duckdb_bind_get_extra_info(info))? };
        for field in context.schema.fields() {
            let name = cstring(field.name())?;
            let logical_type = LogicalType::from_arrow(field.data_type())?;
            // SAFETY: DuckDB copies both values into the live bind result.
            unsafe {
                duckdb::ffi::duckdb_bind_add_result_column(
                    info,
                    name.as_ptr(),
                    logical_type.handle,
                );
            }
        }
        Ok(())
    }));
    if let Err(message) = callback_result(result) {
        set_bind_error(info, &message);
    }
}

unsafe extern "C" fn init(info: duckdb::ffi::duckdb_init_info) {
    let result = catch_unwind(AssertUnwindSafe(|| -> Result<()> {
        let context = unsafe { context(duckdb::ffi::duckdb_init_get_extra_info(info))? };
        let threads = context.max_threads.min(context.paths.len()).max(1);
        // SAFETY: the init object is live and the thread count is positive.
        unsafe { duckdb::ffi::duckdb_init_set_max_threads(info, u64::try_from(threads).unwrap()) };
        Ok(())
    }));
    if let Err(message) = callback_result(result) {
        set_init_error(info, &message);
    }
}

unsafe extern "C" fn local_init(info: duckdb::ffi::duckdb_init_info) {
    let result = catch_unwind(AssertUnwindSafe(|| -> Result<()> {
        let state = Box::new(SegmentScanLocalState {
            reader: None,
            batch: None,
            batch_offset: 0,
        });
        // SAFETY: DuckDB owns this exact pointer through the destructor.
        unsafe {
            duckdb::ffi::duckdb_init_set_init_data(
                info,
                Box::into_raw(state).cast::<c_void>(),
                Some(drop_local_state),
            )
        };
        Ok(())
    }));
    if let Err(message) = callback_result(result) {
        set_init_error(info, &message);
    }
}

unsafe extern "C" fn scan(
    info: duckdb::ffi::duckdb_function_info,
    output: duckdb::ffi::duckdb_data_chunk,
) {
    let result = catch_unwind(AssertUnwindSafe(|| -> Result<()> {
        let context = unsafe { context(duckdb::ffi::duckdb_function_get_extra_info(info))? };
        let state = unsafe { local_state(duckdb::ffi::duckdb_function_get_local_init_data(info))? };
        // SAFETY: linked DuckDB reports its active output vector capacity.
        let vector_rows = usize::try_from(unsafe { duckdb::ffi::duckdb_vector_size() })
            .map_err(|_| CdfError::destination("DuckDB vector size exceeds usize"))?;
        let Some(batch) = state.next_slice(context, vector_rows)? else {
            // SAFETY: this is the live output chunk.
            unsafe { duckdb::ffi::duckdb_data_chunk_set_size(output, 0) };
            return Ok(());
        };
        reference_batch(context, batch, output)
    }));
    if let Err(message) = callback_result(result) {
        set_function_error(info, &message);
    }
}

fn reference_batch(
    context: &SegmentScanContext,
    batch: arrow_array::RecordBatch,
    output: duckdb::ffi::duckdb_data_chunk,
) -> Result<()> {
    assert_c_data_layout();
    let rows = batch.num_rows();
    let struct_array = StructArray::from(batch);
    let mut arrow_array = ManuallyDrop::new(FFI_ArrowArray::new(&struct_array.to_data()));
    let mut converted = ptr::null_mut();
    // SAFETY: ABI layout is asserted; successful conversion transfers Arrow
    // ownership to the returned DuckDB chunk.
    let error = unsafe {
        duckdb::ffi::duckdb_data_chunk_from_arrow(
            context.connection,
            (&mut *arrow_array as *mut FFI_ArrowArray).cast::<duckdb::ffi::ArrowArray>(),
            context.converted_schema.handle,
            &mut converted,
        )
    };
    if let Err(error) = duckdb_error_data_result(error, "convert canonical segment batch") {
        // SAFETY: DuckDB did not accept ownership on conversion failure.
        unsafe { ManuallyDrop::drop(&mut arrow_array) };
        return Err(error);
    }
    if converted.is_null() {
        return Err(CdfError::destination(
            "DuckDB canonical batch conversion returned null",
        ));
    }
    // SAFETY: both chunks are live for all queries and references below.
    let input_columns = unsafe { duckdb::ffi::duckdb_data_chunk_get_column_count(converted) };
    let output_columns = unsafe { duckdb::ffi::duckdb_data_chunk_get_column_count(output) };
    if input_columns != output_columns {
        // SAFETY: this function owns the converted chunk.
        unsafe { duckdb::ffi::duckdb_destroy_data_chunk(&mut converted) };
        return Err(CdfError::destination(format!(
            "DuckDB canonical batch converted to {input_columns} columns for {output_columns}-column output"
        )));
    }
    for column in 0..input_columns {
        // SAFETY: the verified column index is valid in both chunks; DuckDB
        // retains shared auxiliary ownership in the referenced output vector.
        unsafe {
            let source = duckdb::ffi::duckdb_data_chunk_get_vector(converted, column);
            let destination = duckdb::ffi::duckdb_data_chunk_get_vector(output, column);
            duckdb::ffi::duckdb_vector_reference_vector(destination, source);
        }
    }
    // SAFETY: the references remain owned by output after converted is freed.
    unsafe {
        duckdb::ffi::duckdb_data_chunk_set_size(
            output,
            u64::try_from(rows)
                .map_err(|_| CdfError::destination("DuckDB batch rows exceed u64"))?,
        );
        duckdb::ffi::duckdb_destroy_data_chunk(&mut converted);
    }
    Ok(())
}

unsafe fn context<'a>(pointer: *mut c_void) -> Result<&'a SegmentScanContext> {
    // SAFETY: every caller receives the registered pointer from DuckDB.
    unsafe { pointer.cast::<SegmentScanContext>().as_ref() }
        .ok_or_else(|| CdfError::destination("DuckDB segment-scan context is null"))
}

unsafe fn local_state<'a>(pointer: *mut c_void) -> Result<&'a mut SegmentScanLocalState> {
    // SAFETY: DuckDB gives a local state to only its owning worker callback.
    unsafe { pointer.cast::<SegmentScanLocalState>().as_mut() }
        .ok_or_else(|| CdfError::destination("DuckDB segment-scan local state is null"))
}

fn callback_result(result: std::thread::Result<Result<()>>) -> std::result::Result<(), String> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(error.to_string()),
        Err(payload) => Err(if let Some(message) = payload.downcast_ref::<&str>() {
            format!("DuckDB canonical segment callback panicked: {message}")
        } else if let Some(message) = payload.downcast_ref::<String>() {
            format!("DuckDB canonical segment callback panicked: {message}")
        } else {
            "DuckDB canonical segment callback panicked".to_owned()
        }),
    }
}

fn set_bind_error(info: duckdb::ffi::duckdb_bind_info, message: &str) {
    let message = callback_message(message);
    // SAFETY: DuckDB copies this message during the callback.
    unsafe { duckdb::ffi::duckdb_bind_set_error(info, message.as_ptr()) };
}

fn set_init_error(info: duckdb::ffi::duckdb_init_info, message: &str) {
    let message = callback_message(message);
    // SAFETY: DuckDB copies this message during the callback.
    unsafe { duckdb::ffi::duckdb_init_set_error(info, message.as_ptr()) };
}

fn set_function_error(info: duckdb::ffi::duckdb_function_info, message: &str) {
    let message = callback_message(message);
    // SAFETY: DuckDB copies this message during the callback.
    unsafe { duckdb::ffi::duckdb_function_set_error(info, message.as_ptr()) };
}

fn callback_message(message: &str) -> CString {
    CString::new(message.replace('\0', "\\0")).expect("escaped callback message contains no NUL")
}

fn cstring(value: &str) -> Result<CString> {
    CString::new(value).map_err(|error| {
        CdfError::contract(format!("DuckDB C string contains an interior NUL: {error}"))
    })
}

#[cfg(unix)]
fn path_to_cstring(path: &Path) -> Result<CString> {
    use std::os::unix::ffi::OsStrExt;
    CString::new(path.as_os_str().as_bytes()).map_err(|error| {
        CdfError::contract(format!(
            "DuckDB database path contains an interior NUL: {error}"
        ))
    })
}

#[cfg(not(unix))]
fn path_to_cstring(path: &Path) -> Result<CString> {
    cstring(
        path.to_str()
            .ok_or_else(|| CdfError::contract("DuckDB database path is not valid Unicode"))?,
    )
}

fn assert_c_data_layout() {
    assert_eq!(
        size_of::<FFI_ArrowArray>(),
        size_of::<duckdb::ffi::ArrowArray>()
    );
    assert_eq!(
        align_of::<FFI_ArrowArray>(),
        align_of::<duckdb::ffi::ArrowArray>()
    );
    assert_eq!(
        size_of::<FFI_ArrowSchema>(),
        size_of::<duckdb::ffi::ArrowSchema>()
    );
    assert_eq!(
        align_of::<FFI_ArrowSchema>(),
        align_of::<duckdb::ffi::ArrowSchema>()
    );
}

fn duckdb_error_data_result(mut data: duckdb::ffi::duckdb_error_data, context: &str) -> Result<()> {
    if data.is_null() {
        return Ok(());
    }
    // SAFETY: this helper owns error data returned by DuckDB and destroys it.
    let message = unsafe {
        let message = if duckdb::ffi::duckdb_error_data_has_error(data) {
            Some(cstr_message(duckdb::ffi::duckdb_error_data_message(data)))
        } else {
            None
        };
        duckdb::ffi::duckdb_destroy_error_data(&mut data);
        message
    };
    match message {
        Some(message) => Err(CdfError::destination(format!("{context}: {message}"))),
        None => Ok(()),
    }
}

fn cstr_message(pointer: *const c_char) -> String {
    if pointer.is_null() {
        return "unknown error".to_owned();
    }
    // SAFETY: DuckDB diagnostic pointers are NUL-terminated for their lifetime.
    unsafe { CStr::from_ptr(pointer) }
        .to_string_lossy()
        .into_owned()
}
