//! Streaming top-level JSON document framing into NDJSON records.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use cdf_kernel::{CdfError, Result};
use cdf_memory::{ConsumerKey, MemoryClass, MemoryLease, ReservationRequest, reserve};
use cdf_runtime::AccountedByteStream;
use futures_util::{TryStreamExt, stream};

use crate::options::{MAXIMUM_JSON_NESTING_DEPTH, maximum_record_bytes_error};

pub(crate) struct JsonFrameRequest {
    pub(crate) maximum_input_bytes: u64,
    pub(crate) maximum_records: Option<u64>,
    pub(crate) preferred_output_chunk_bytes: u64,
    pub(crate) maximum_record_bytes: u64,
    pub(crate) maximum_nesting_depth: usize,
    pub(crate) require_terminal_document: bool,
    pub(crate) input_counter: Arc<AtomicU64>,
    pub(crate) memory: Arc<dyn cdf_memory::MemoryCoordinator>,
    pub(crate) cancellation: cdf_runtime::RunCancellation,
}

#[derive(Clone, Copy, Debug)]
enum DocumentPhase {
    Start,
    Single,
    Array { expect_value: bool, seen: bool },
    Done,
}

struct JsonFrameState {
    input: AccountedByteStream,
    current: Option<cdf_memory::AccountedBytes>,
    offset: usize,
    request: JsonFrameRequest,
    phase: DocumentPhase,
    close_stack: [u8; 256],
    depth: usize,
    in_string: bool,
    escaped: bool,
    input_bytes: u64,
    records: u64,
    record_bytes: u64,
    sample_complete: bool,
    output: Vec<u8>,
    output_lease: Option<MemoryLease>,
    input_finished: bool,
}

pub(crate) fn frame_json_document(
    input: AccountedByteStream,
    request: JsonFrameRequest,
) -> Result<AccountedByteStream> {
    if request.maximum_input_bytes == 0
        || request.preferred_output_chunk_bytes < 2
        || request.maximum_record_bytes == 0
        || request.maximum_nesting_depth == 0
        || request.maximum_nesting_depth > MAXIMUM_JSON_NESTING_DEPTH
        || request.maximum_records == Some(0)
    {
        return Err(CdfError::contract(
            "JSON framing requires positive input, record, and output chunk bounds",
        ));
    }
    let state = JsonFrameState {
        input,
        current: None,
        offset: 0,
        request,
        phase: DocumentPhase::Start,
        close_stack: [0; 256],
        depth: 0,
        in_string: false,
        escaped: false,
        input_bytes: 0,
        records: 0,
        record_bytes: 0,
        sample_complete: false,
        output: Vec::new(),
        output_lease: None,
        input_finished: false,
    };
    Ok(Box::pin(stream::try_unfold(state, frame_next)))
}

async fn frame_next(
    mut state: JsonFrameState,
) -> Result<Option<(cdf_memory::AccountedBytes, JsonFrameState)>> {
    let output_bound = usize::try_from(state.request.preferred_output_chunk_bytes)
        .map_err(|_| CdfError::contract("JSON output chunk bound exceeds usize"))?;
    ensure_frame_output(&mut state).await?;
    loop {
        state.request.cancellation.check()?;
        if state.output.len() + 2 > output_bound {
            return emit_frame_output(state).map(Some);
        }
        if state.sample_complete {
            if state.output.is_empty() {
                state
                    .request
                    .input_counter
                    .store(state.input_bytes, Ordering::Relaxed);
                return Ok(None);
            }
            return emit_frame_output(state).map(Some);
        }
        if state
            .current
            .as_ref()
            .is_none_or(|chunk| state.offset == chunk.payload().len())
            && !state.input_finished
        {
            state.current = state.input.try_next().await?;
            state.offset = 0;
            state.input_finished = state.current.is_none();
        }
        let Some(chunk) = &state.current else {
            validate_frame_terminal(&state)?;
            state.sample_complete = true;
            continue;
        };
        let byte = chunk.payload()[state.offset];
        state.offset += 1;
        state.input_bytes = state
            .input_bytes
            .checked_add(1)
            .ok_or_else(|| CdfError::data("JSON input byte count overflowed"))?;
        if state.input_bytes > state.request.maximum_input_bytes {
            return Err(CdfError::data(format!(
                "JSON discovery exceeded its {}-byte input bound before completing the requested sample",
                state.request.maximum_input_bytes
            )));
        }
        process_frame_byte(&mut state, byte)?;
    }
}

async fn ensure_frame_output(state: &mut JsonFrameState) -> Result<()> {
    if state.output_lease.is_some() {
        return Ok(());
    }
    let lease = reserve(
        Arc::clone(&state.request.memory),
        ReservationRequest::new(
            ConsumerKey::new("json-document-framing", MemoryClass::Transform)?,
            state.request.preferred_output_chunk_bytes,
        )?,
    )
    .await?;
    state.output = Vec::with_capacity(
        usize::try_from(state.request.preferred_output_chunk_bytes)
            .map_err(|_| CdfError::contract("JSON output chunk bound exceeds usize"))?,
    );
    state.output_lease = Some(lease);
    Ok(())
}

fn emit_frame_output(
    mut state: JsonFrameState,
) -> Result<(cdf_memory::AccountedBytes, JsonFrameState)> {
    state
        .request
        .input_counter
        .store(state.input_bytes, Ordering::Relaxed);
    let lease = state
        .output_lease
        .take()
        .ok_or_else(|| CdfError::internal("JSON framing output lease missing"))?;
    let bytes = cdf_memory::AccountedBytes::new(
        bytes::Bytes::from(std::mem::take(&mut state.output)),
        lease,
    )?;
    Ok((bytes, state))
}

fn process_frame_byte(state: &mut JsonFrameState, byte: u8) -> Result<()> {
    if state.depth != 0 {
        observe_json_document_record_byte(state)?;
        if state.in_string {
            state.output.push(byte);
            if state.escaped {
                state.escaped = false;
            } else if byte == b'\\' {
                state.escaped = true;
            } else if byte == b'"' {
                state.in_string = false;
            }
            return Ok(());
        }
        // The framing output is NDJSON, so source formatting whitespace cannot
        // survive as physical newlines inside one logical record. JSON permits
        // all ASCII whitespace between tokens; remove it while retaining exact
        // source-byte and maximum-record accounting above. String contents are
        // emitted unchanged by the branch above.
        if byte.is_ascii_whitespace() {
            return Ok(());
        }
        state.output.push(byte);
        match byte {
            b'"' => state.in_string = true,
            b'{' => push_close(state, b'}')?,
            b'[' => push_close(state, b']')?,
            b'}' | b']' => {
                if state.close_stack[state.depth - 1] != byte {
                    return Err(CdfError::data("JSON document has mismatched delimiters"));
                }
                state.depth -= 1;
                if state.depth == 0 {
                    state.output.push(b'\n');
                    state.record_bytes = 0;
                    state.records = state
                        .records
                        .checked_add(1)
                        .ok_or_else(|| CdfError::data("JSON record count overflowed"))?;
                    state.phase = match state.phase {
                        DocumentPhase::Single => DocumentPhase::Done,
                        DocumentPhase::Array { .. } => DocumentPhase::Array {
                            expect_value: false,
                            seen: true,
                        },
                        _ => {
                            return Err(CdfError::internal(
                                "JSON framing closed a record outside a document",
                            ));
                        }
                    };
                    state.sample_complete = state
                        .request
                        .maximum_records
                        .is_some_and(|maximum| state.records >= maximum);
                }
            }
            _ => {}
        }
        return Ok(());
    }

    match state.phase {
        DocumentPhase::Start => {
            if byte.is_ascii_whitespace() {
                return Ok(());
            }
            match byte {
                b'{' => {
                    state.phase = DocumentPhase::Single;
                    start_record(state)?;
                }
                b'[' => {
                    state.phase = DocumentPhase::Array {
                        expect_value: true,
                        seen: false,
                    };
                }
                _ => {
                    return Err(CdfError::data(
                        "JSON file source must be an object or an array of objects",
                    ));
                }
            }
        }
        DocumentPhase::Single | DocumentPhase::Done => {
            if !byte.is_ascii_whitespace() {
                return Err(CdfError::data(
                    "JSON document has trailing non-whitespace data",
                ));
            }
        }
        DocumentPhase::Array { expect_value, seen } => {
            if byte.is_ascii_whitespace() {
                return Ok(());
            }
            if expect_value {
                match byte {
                    b'{' => start_record(state)?,
                    b']' if !seen => state.phase = DocumentPhase::Done,
                    b']' => return Err(CdfError::data("JSON array has a trailing comma")),
                    _ => {
                        return Err(CdfError::data(
                            "JSON file source array entries must be objects",
                        ));
                    }
                }
            } else {
                match byte {
                    b',' => {
                        state.phase = DocumentPhase::Array {
                            expect_value: true,
                            seen,
                        };
                    }
                    b']' => state.phase = DocumentPhase::Done,
                    _ => return Err(CdfError::data("JSON array entries require a comma")),
                }
            }
        }
    }
    Ok(())
}

fn start_record(state: &mut JsonFrameState) -> Result<()> {
    state.record_bytes = 1;
    if state.record_bytes > state.request.maximum_record_bytes {
        return Err(maximum_record_bytes_error(
            state.request.maximum_record_bytes,
        ));
    }
    state.output.push(b'{');
    push_close(state, b'}')
}

fn observe_json_document_record_byte(state: &mut JsonFrameState) -> Result<()> {
    state.record_bytes = state
        .record_bytes
        .checked_add(1)
        .ok_or_else(|| CdfError::data("JSON record byte count overflowed"))?;
    if state.record_bytes > state.request.maximum_record_bytes {
        return Err(maximum_record_bytes_error(
            state.request.maximum_record_bytes,
        ));
    }
    Ok(())
}

fn push_close(state: &mut JsonFrameState, close: u8) -> Result<()> {
    if state.depth == state.request.maximum_nesting_depth {
        return Err(CdfError::data(format!(
            "JSON nesting exceeds the configured {}-level limit",
            state.request.maximum_nesting_depth
        )));
    }
    state.close_stack[state.depth] = close;
    state.depth += 1;
    Ok(())
}

fn validate_frame_terminal(state: &JsonFrameState) -> Result<()> {
    if state.sample_complete && !state.request.require_terminal_document {
        return Ok(());
    }
    if state.depth != 0 || state.in_string || state.escaped {
        return Err(CdfError::data("JSON document ended inside a record"));
    }
    match state.phase {
        DocumentPhase::Done => Ok(()),
        DocumentPhase::Array {
            expect_value: true,
            seen: true,
        } => Err(CdfError::data("JSON array ended after a comma")),
        _ => Err(CdfError::data(
            "JSON document ended before its top-level value completed",
        )),
    }
}
