use crate::package::persistence_batch;
use crate::*;

use std::{
    collections::BTreeSet,
    ffi::CString,
    marker::PhantomData,
    os::raw::{c_char, c_int, c_void},
    ptr,
    sync::Arc,
};

use arrow_array::{StructArray, ffi::FFI_ArrowArray, ffi_stream::FFI_ArrowArrayStream};
use arrow_schema::{DataType, Field, Schema, ffi::FFI_ArrowSchema};

const DUCKDB_ARROW_STREAM_EIO: c_int = 5;

pub(crate) struct StagedArrowStreamOutcome {
    pub(crate) accepted: Vec<cdf_runtime::StagedSegmentIdentity>,
    pub(crate) rows_received: u64,
    pub(crate) next_row_key: u64,
}

pub(crate) struct StagedArrowStream<'a> {
    stream: FFI_ArrowArrayStream,
    _scope: PhantomData<&'a mut dyn cdf_runtime::StagedSegmentStream>,
}

impl<'a> StagedArrowStream<'a> {
    pub(crate) fn new(
        request: &'a cdf_runtime::StagedIngressRequest,
        source: &'a mut dyn cdf_runtime::StagedSegmentStream,
        first_segment: cdf_runtime::StagedSegmentRequest,
        next_row_key: u64,
        accepted_before: &[cdf_runtime::StagedSegmentIdentity],
    ) -> Result<Self> {
        let state = StagedArrowStreamState::new(
            request,
            source,
            first_segment,
            next_row_key,
            accepted_before,
        )?;
        Ok(Self {
            stream: FFI_ArrowArrayStream {
                get_schema: Some(get_schema),
                get_next: Some(get_next),
                get_last_error: Some(get_last_error),
                release: Some(release_stream),
                private_data: Box::into_raw(Box::new(state)).cast::<c_void>(),
            },
            _scope: PhantomData,
        })
    }

    pub(crate) fn stream_mut(&mut self) -> &mut FFI_ArrowArrayStream {
        &mut self.stream
    }

    pub(crate) fn outcome(&self) -> Result<StagedArrowStreamOutcome> {
        let state = self.state()?;
        if let Some(error) = &state.last_error {
            return Err(CdfError::destination(format!(
                "DuckDB staged Arrow stream failed: {}",
                error.to_string_lossy()
            )));
        }
        if !state.eof {
            return Err(CdfError::destination(
                "DuckDB Arrow stream-scan did not exhaust the staged segment stream",
            ));
        }
        Ok(StagedArrowStreamOutcome {
            accepted: state.accepted.clone(),
            rows_received: state.rows_received,
            next_row_key: state.next_row_key,
        })
    }

    fn state(&self) -> Result<&StagedArrowStreamState> {
        if self.stream.private_data.is_null() {
            return Err(CdfError::destination(
                "DuckDB Arrow staged stream state was released unexpectedly",
            ));
        }
        // SAFETY: `private_data` is initialized by `new` and remains owned by this wrapper
        // until `release_stream` runs during `Drop`.
        Ok(unsafe { &*self.stream.private_data.cast::<StagedArrowStreamState>() })
    }
}

struct StagedArrowStreamState {
    request: *const cdf_runtime::StagedIngressRequest,
    source: *mut dyn cdf_runtime::StagedSegmentStream,
    persisted_schema: Schema,
    current: Option<cdf_runtime::StagedSegmentRequest>,
    current_rows: u64,
    accepted: Vec<cdf_runtime::StagedSegmentIdentity>,
    seen_segment_ids: BTreeSet<SegmentId>,
    expected_ordinal: u32,
    next_row_key: u64,
    rows_received: u64,
    eof: bool,
    last_error: Option<CString>,
}

impl StagedArrowStreamState {
    fn new(
        request: &cdf_runtime::StagedIngressRequest,
        source: &mut dyn cdf_runtime::StagedSegmentStream,
        first_segment: cdf_runtime::StagedSegmentRequest,
        next_row_key: u64,
        accepted_before: &[cdf_runtime::StagedSegmentIdentity],
    ) -> Result<Self> {
        let mut fields = request.output_schema().fields().to_vec();
        fields.push(Arc::new(Field::new(
            CDF_ROW_KEY_COLUMN,
            DataType::UInt64,
            false,
        )));
        // SAFETY: the C callback state stores an erased trait-object pointer because the C ABI
        // cannot carry Rust lifetimes. `StagedArrowStream<'a>` retains `PhantomData<&'a mut ...>`
        // so the wrapper cannot outlive the borrowed staged source, and DuckDB consumes the
        // stream synchronously before `stage_stream` releases that borrow.
        let source = unsafe {
            std::mem::transmute::<
                *mut dyn cdf_runtime::StagedSegmentStream,
                *mut (dyn cdf_runtime::StagedSegmentStream + 'static),
            >(source as *mut dyn cdf_runtime::StagedSegmentStream)
        };
        let mut state = Self {
            request: request as *const _,
            source,
            persisted_schema: Schema::new(fields),
            current: None,
            current_rows: 0,
            accepted: Vec::new(),
            seen_segment_ids: accepted_before
                .iter()
                .map(|identity| identity.segment_id.clone())
                .collect(),
            expected_ordinal: u32::try_from(accepted_before.len())
                .map_err(|_| CdfError::data("DuckDB staged segment count exceeds u32"))?,
            next_row_key,
            rows_received: 0,
            eof: false,
            last_error: None,
        };
        state.accept_segment(first_segment)?;
        Ok(state)
    }

    fn request(&self) -> &cdf_runtime::StagedIngressRequest {
        // SAFETY: the request outlives this stream by `StagedArrowStream`'s lifetime.
        unsafe { &*self.request }
    }

    fn source(&mut self) -> &mut dyn cdf_runtime::StagedSegmentStream {
        // SAFETY: the staged segment source outlives this stream by `StagedArrowStream`'s lifetime
        // and DuckDB consumes the C stream synchronously before that mutable borrow is released.
        unsafe { &mut *self.source }
    }

    fn next_array(&mut self, out: *mut FFI_ArrowArray) -> Result<()> {
        loop {
            if self.eof {
                // SAFETY: `out` is a DuckDB-provided ArrowArray pointer for this callback.
                unsafe {
                    ptr::write(out, FFI_ArrowArray::empty());
                }
                return Ok(());
            }
            if self.current.is_none() {
                match self.source().next_segment()? {
                    Some(segment) => self.accept_segment(segment)?,
                    None => {
                        self.eof = true;
                        // SAFETY: `out` is a DuckDB-provided ArrowArray pointer for this callback.
                        unsafe {
                            ptr::write(out, FFI_ArrowArray::empty());
                        }
                        return Ok(());
                    }
                }
            }
            if let Some(batch) = self.next_persisted_batch()? {
                let struct_array = StructArray::from(batch);
                let array = FFI_ArrowArray::new(&struct_array.to_data());
                // SAFETY: `out` is a DuckDB-provided ArrowArray pointer. Ownership of the
                // exported Arrow private data moves to the consumer by the C Data Interface;
                // `forget` prevents Rust from releasing it prematurely.
                unsafe {
                    ptr::write(out, array);
                }
                return Ok(());
            }
        }
    }

    fn next_persisted_batch(&mut self) -> Result<Option<RecordBatch>> {
        let Some(segment) = self.current.as_mut() else {
            return Ok(None);
        };
        let identity = segment.identity.clone();
        match segment.reader_mut().next_batch()? {
            Some(batch) => {
                if batch.schema().as_ref() != self.request().output_schema() {
                    return Err(CdfError::data(format!(
                        "DuckDB staged segment {} schema differs from the planned output schema",
                        identity.segment_id
                    )));
                }
                let batch_rows = u64::try_from(batch.num_rows())
                    .map_err(|_| CdfError::data("DuckDB staged batch rows exceed u64"))?;
                let persisted = persistence_batch(batch, self.next_row_key, None)?;
                self.next_row_key = self
                    .next_row_key
                    .checked_add(batch_rows)
                    .ok_or_else(|| CdfError::data("DuckDB staged row key overflowed"))?;
                self.rows_received = self
                    .rows_received
                    .checked_add(batch_rows)
                    .ok_or_else(|| CdfError::data("DuckDB staged row count overflowed"))?;
                self.current_rows = self
                    .current_rows
                    .checked_add(batch_rows)
                    .ok_or_else(|| CdfError::data("DuckDB staged segment row count overflowed"))?;
                Ok(Some(persisted))
            }
            None => {
                self.finish_current_segment(identity)?;
                Ok(None)
            }
        }
    }

    fn accept_segment(&mut self, segment: cdf_runtime::StagedSegmentRequest) -> Result<()> {
        let identity = &segment.identity;
        if identity.schema_hash != self.request().binding().schema_hash {
            return Err(CdfError::data(
                "DuckDB staged segment schema hash differs from its attempt",
            ));
        }
        if identity.ordinal != self.expected_ordinal
            || !self.seen_segment_ids.insert(identity.segment_id.clone())
        {
            return Err(CdfError::data(
                "DuckDB staged segments must be unique and arrive in canonical order",
            ));
        }
        if identity.row_count == 0 {
            return Err(CdfError::data(
                "DuckDB staged data segment must contain at least one row",
            ));
        }
        self.current = Some(segment);
        self.current_rows = 0;
        Ok(())
    }

    fn finish_current_segment(
        &mut self,
        identity: cdf_runtime::StagedSegmentIdentity,
    ) -> Result<()> {
        if self.current_rows != identity.row_count {
            return Err(CdfError::data(format!(
                "DuckDB staged segment {} row count differs from durable identity",
                identity.segment_id
            )));
        }
        let attempt_id = self.request().attempt_id().clone();
        self.source().acknowledge(cdf_runtime::StagedSegmentAck {
            attempt_id,
            identity: identity.clone(),
            external_durable: false,
        })?;
        self.accepted.push(identity);
        self.expected_ordinal = self
            .expected_ordinal
            .checked_add(1)
            .ok_or_else(|| CdfError::data("DuckDB staged segment ordinal overflowed"))?;
        self.current = None;
        self.current_rows = 0;
        Ok(())
    }

    fn record_error(&mut self, error: CdfError) -> c_int {
        let message = error.to_string().replace('\0', "\\0");
        self.last_error = CString::new(message).ok();
        DUCKDB_ARROW_STREAM_EIO
    }
}

unsafe extern "C" fn get_schema(
    stream: *mut FFI_ArrowArrayStream,
    out: *mut FFI_ArrowSchema,
) -> c_int {
    let Some(state) = state_mut(stream) else {
        return DUCKDB_ARROW_STREAM_EIO;
    };
    match FFI_ArrowSchema::try_from(&state.persisted_schema) {
        Ok(schema) => {
            // SAFETY: `out` is a DuckDB-provided ArrowSchema pointer. Ownership moves to
            // the consumer by the C Data Interface.
            unsafe {
                ptr::write(out, schema);
            }
            0
        }
        Err(error) => state.record_error(CdfError::data(format!(
            "build DuckDB staged Arrow schema: {error}"
        ))),
    }
}

unsafe extern "C" fn get_next(
    stream: *mut FFI_ArrowArrayStream,
    out: *mut FFI_ArrowArray,
) -> c_int {
    let Some(state) = state_mut(stream) else {
        return DUCKDB_ARROW_STREAM_EIO;
    };
    match state.next_array(out) {
        Ok(()) => 0,
        Err(error) => state.record_error(error),
    }
}

unsafe extern "C" fn get_last_error(stream: *mut FFI_ArrowArrayStream) -> *const c_char {
    let Some(state) = state_mut(stream) else {
        return ptr::null();
    };
    state
        .last_error
        .as_ref()
        .map_or(ptr::null(), |error| error.as_ptr())
}

unsafe extern "C" fn release_stream(stream: *mut FFI_ArrowArrayStream) {
    if stream.is_null() {
        return;
    }
    // SAFETY: `stream` is a live ArrowArrayStream owned by `StagedArrowStream`.
    let stream = unsafe { &mut *stream };
    stream.get_schema = None;
    stream.get_next = None;
    stream.get_last_error = None;
    if !stream.private_data.is_null() {
        // SAFETY: `private_data` was allocated with `Box::into_raw` in `StagedArrowStream::new`.
        let state = unsafe { Box::from_raw(stream.private_data.cast::<StagedArrowStreamState>()) };
        drop(state);
        stream.private_data = ptr::null_mut();
    }
    stream.release = None;
}

fn state_mut(stream: *mut FFI_ArrowArrayStream) -> Option<&'static mut StagedArrowStreamState> {
    if stream.is_null() {
        return None;
    }
    // SAFETY: callbacks are invoked only while the stream is live. The returned lifetime is
    // immediately bounded by the callback body; it is never stored.
    let stream = unsafe { &mut *stream };
    if stream.private_data.is_null() {
        return None;
    }
    // SAFETY: `private_data` points to `StagedArrowStreamState` while the stream is live.
    Some(unsafe { &mut *stream.private_data.cast::<StagedArrowStreamState>() })
}
