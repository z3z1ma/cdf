use std::{mem::size_of, sync::Arc};

use bytes::{Bytes, BytesMut};
use cdf_foreign_stream::{
    ForeignBatchOutcome, ForeignControlEvent, ForeignControlKind, ForeignCopyClassification,
    ForeignDiagnosticSeverity, ForeignEventStream, ForeignStreamEvent, ForeignTerminalStatus,
    ForeignTransferMode,
};
use cdf_kernel::{CdfError, ErrorKind, Result, SourcePosition};
use cdf_memory::{
    AccountedBytes, ConsumerKey, MemoryClass, MemoryCoordinator, MemoryLease, ReservationRequest,
};
use cdf_runtime::{
    AccountedByteStream, BoundedFormatRequest, ByteSource, DecodeSchemaPlan, FormatBatchStream,
    MemoryByteSource, ReadOptions, RunCancellation, SequentialReadRequest, decode_format_stream,
};
use futures_util::{StreamExt, TryStreamExt, stream};
use serde_json::Value;

use crate::{
    AirbyteMessage, SingerMessage, StreamIdentity, SubprocessProtocol, SupervisionOptions,
    decode_airbyte_message, decode_singer_message,
    protocol::canonical_json_hash,
    runner::{
        SubprocessLifecycle, SubprocessStdoutByteSource, SubprocessTerminal,
        with_terminal_diagnostic,
    },
};

pub(crate) struct ProtocolEventRequest {
    pub(crate) source: Arc<SubprocessStdoutByteSource>,
    pub(crate) protocol: SubprocessProtocol,
    pub(crate) read_options: ReadOptions,
    pub(crate) schema: DecodeSchemaPlan,
    pub(crate) supervision: SupervisionOptions,
    pub(crate) memory: Arc<dyn MemoryCoordinator>,
    pub(crate) lifecycle: SubprocessLifecycle,
}

pub(crate) fn protocol_foreign_events(request: ProtocolEventRequest) -> Result<ForeignEventStream> {
    let selected_stream = selected_stream(&request.protocol)?.clone();
    let read_options = request.read_options.clone().with_batch_id_prefix(format!(
        "{}-{}",
        request.read_options.batch_id_prefix,
        selected_stream.batch_id_part()
    ))?;
    let maximum_line_bytes = usize::try_from(request.supervision.maximum_protocol_line_bytes)
        .map_err(|_| CdfError::contract("subprocess protocol line boundary exceeds usize"))?;
    let row_window_bytes = usize::try_from(request.supervision.protocol_row_window_bytes)
        .map_err(|_| CdfError::contract("subprocess protocol row window exceeds usize"))?;
    let state = ProtocolEventState {
        source: request.source,
        input: None,
        current_chunk: None,
        chunk_offset: 0,
        input_finished: false,
        protocol: request.protocol,
        selected_stream,
        read_options,
        schema: request.schema,
        supervision: request.supervision,
        memory: request.memory,
        lifecycle: request.lifecycle,
        line_buffer: BytesMut::new(),
        maximum_line_bytes,
        row_window_bytes,
        line_number: 0,
        line_lease: None,
        parser_lease: None,
        control_lease: None,
        row_buffer: Vec::new(),
        row_lease: None,
        pending_record: None,
        decoder: None,
        pending_control: None,
        pending_terminal: None,
        last_position: None,
        next_sequence: 1,
        initialized: false,
        finished: false,
    };
    Ok(Box::pin(stream::unfold(state, protocol_event_next)))
}

fn selected_stream(protocol: &SubprocessProtocol) -> Result<&StreamIdentity> {
    match protocol {
        SubprocessProtocol::Singer { stream } | SubprocessProtocol::Airbyte { stream } => {
            Ok(stream)
        }
        _ => Err(CdfError::internal(
            "protocol event stream requires Singer or Airbyte",
        )),
    }
}

struct ProtocolEventState {
    source: Arc<SubprocessStdoutByteSource>,
    input: Option<AccountedByteStream>,
    current_chunk: Option<AccountedBytes>,
    chunk_offset: usize,
    input_finished: bool,
    protocol: SubprocessProtocol,
    selected_stream: StreamIdentity,
    read_options: ReadOptions,
    schema: DecodeSchemaPlan,
    supervision: SupervisionOptions,
    memory: Arc<dyn MemoryCoordinator>,
    lifecycle: SubprocessLifecycle,
    line_buffer: BytesMut,
    maximum_line_bytes: usize,
    row_window_bytes: usize,
    line_number: usize,
    line_lease: Option<MemoryLease>,
    parser_lease: Option<MemoryLease>,
    control_lease: Option<MemoryLease>,
    row_buffer: Vec<u8>,
    row_lease: Option<MemoryLease>,
    pending_record: Option<Vec<u8>>,
    decoder: Option<FormatBatchStream>,
    pending_control: Option<ForeignControlKind>,
    pending_terminal: Option<ForeignTerminalStatus>,
    last_position: Option<SourcePosition>,
    next_sequence: u64,
    initialized: bool,
    finished: bool,
}

impl Drop for ProtocolEventState {
    fn drop(&mut self) {
        if !self.finished {
            self.lifecycle.cancel();
        }
    }
}

async fn protocol_event_next(
    mut state: ProtocolEventState,
) -> Option<(Result<ForeignStreamEvent>, ProtocolEventState)> {
    if state.finished {
        return None;
    }
    if let Some(terminal) = state.pending_terminal.take() {
        state.finished = true;
        return Some((Ok(ForeignStreamEvent::Terminal(terminal)), state));
    }
    loop {
        if let Some(control) = state.pending_control.take() {
            let event = next_control_event(&mut state, control);
            return Some((event, state));
        }
        if let Some(decoder) = state.decoder.as_mut() {
            match decoder.next().await {
                Some(Ok(batch)) => {
                    let event = next_outcome_event(&mut state, batch);
                    return Some((event, state));
                }
                Some(Err(error)) => {
                    let terminal = fail_protocol_stream(&mut state, error).await;
                    return Some((Ok(ForeignStreamEvent::Terminal(terminal)), state));
                }
                None => state.decoder = None,
            }
        }
        if !state.initialized
            && let Err(error) = initialize_protocol_stream(&mut state).await
        {
            let terminal = fail_protocol_stream(&mut state, error).await;
            return Some((Ok(ForeignStreamEvent::Terminal(terminal)), state));
        }
        if let Some(record) = state.pending_record.take() {
            if let Err(error) = append_protocol_record(&mut state, record).await {
                let terminal = fail_protocol_stream(&mut state, error).await;
                return Some((Ok(ForeignStreamEvent::Terminal(terminal)), state));
            }
            if state.row_buffer.len() == state.row_window_bytes {
                if let Err(error) = start_row_window_decode(&mut state).await {
                    let terminal = fail_protocol_stream(&mut state, error).await;
                    return Some((Ok(ForeignStreamEvent::Terminal(terminal)), state));
                }
                continue;
            }
        }
        let has_line = match read_protocol_line(&mut state).await {
            Ok(has_line) => has_line,
            Err(error) => {
                let terminal = fail_protocol_stream(&mut state, error).await;
                return Some((Ok(ForeignStreamEvent::Terminal(terminal)), state));
            }
        };
        if has_line {
            state.line_number = match state.line_number.checked_add(1) {
                Some(line) => line,
                None => {
                    let terminal = fail_protocol_stream(
                        &mut state,
                        CdfError::data("subprocess protocol line count overflowed"),
                    )
                    .await;
                    return Some((Ok(ForeignStreamEvent::Terminal(terminal)), state));
                }
            };
            let item = decode_protocol_line(&state);
            state.line_buffer.clear();
            match item {
                Ok(ProtocolItem::Ignore) => continue,
                Ok(ProtocolItem::Record(record)) => {
                    if let Err(error) = append_protocol_record(&mut state, record).await {
                        let terminal = fail_protocol_stream(&mut state, error).await;
                        return Some((Ok(ForeignStreamEvent::Terminal(terminal)), state));
                    }
                    if state.row_buffer.len() == state.row_window_bytes
                        && let Err(error) = start_row_window_decode(&mut state).await
                    {
                        let terminal = fail_protocol_stream(&mut state, error).await;
                        return Some((Ok(ForeignStreamEvent::Terminal(terminal)), state));
                    }
                }
                Ok(ProtocolItem::Control(control)) => {
                    if let ForeignControlKind::ForeignState { position } = &control {
                        state.last_position = Some(position.clone());
                    }
                    if state.row_buffer.is_empty() {
                        let event = next_control_event(&mut state, control);
                        return Some((event, state));
                    }
                    state.pending_control = Some(control);
                    if let Err(error) = start_row_window_decode(&mut state).await {
                        let terminal = fail_protocol_stream(&mut state, error).await;
                        return Some((Ok(ForeignStreamEvent::Terminal(terminal)), state));
                    }
                }
                Err(error) => {
                    let terminal = fail_protocol_stream(&mut state, error).await;
                    return Some((Ok(ForeignStreamEvent::Terminal(terminal)), state));
                }
            }
            continue;
        }
        if !state.row_buffer.is_empty() {
            if let Err(error) = start_row_window_decode(&mut state).await {
                let terminal = fail_protocol_stream(&mut state, error).await;
                return Some((Ok(ForeignStreamEvent::Terminal(terminal)), state));
            }
            continue;
        }
        let event = terminal_event(&mut state).await;
        return Some((Ok(event), state));
    }
}

async fn initialize_protocol_stream(state: &mut ProtocolEventState) -> Result<()> {
    let line_bytes = state
        .supervision
        .maximum_protocol_line_bytes
        .checked_add(2)
        .ok_or_else(|| CdfError::contract("subprocess protocol line boundary overflowed"))?;
    let cancellation = state.lifecycle.run_cancellation();
    state.line_lease = Some(
        reserve_protocol_memory(
            &state.memory,
            "subprocess-protocol-line",
            line_bytes,
            &cancellation,
        )
        .await?,
    );
    state.parser_lease = Some(
        reserve_protocol_memory(
            &state.memory,
            "subprocess-protocol-parser",
            state.supervision.protocol_parser_scratch_bytes,
            &cancellation,
        )
        .await?,
    );
    state.control_lease = Some(
        reserve_protocol_memory(
            &state.memory,
            "subprocess-protocol-control",
            state.supervision.maximum_protocol_line_bytes,
            &cancellation,
        )
        .await?,
    );
    state.line_buffer = BytesMut::with_capacity(
        state
            .maximum_line_bytes
            .checked_add(2)
            .ok_or_else(|| CdfError::contract("subprocess protocol line capacity overflowed"))?,
    );
    let preferred_chunk_bytes = state
        .supervision
        .maximum_stream_chunk_bytes
        .min(state.supervision.maximum_protocol_line_bytes)
        .max(1);
    state.input = Some(
        state
            .source
            .open_sequential(SequentialReadRequest {
                preferred_chunk_bytes,
                cancellation: state.lifecycle.run_cancellation(),
            })
            .await?,
    );
    state.initialized = true;
    Ok(())
}

async fn reserve_protocol_memory(
    memory: &Arc<dyn MemoryCoordinator>,
    consumer: &str,
    bytes: u64,
    cancellation: &RunCancellation,
) -> Result<MemoryLease> {
    cancellation
        .await_or_cancel(cdf_memory::reserve(
            Arc::clone(memory),
            ReservationRequest::new(
                ConsumerKey::new(consumer, MemoryClass::Source)?,
                bytes.max(1),
            )?
            .as_minimum_working_set(),
        ))
        .await
}

async fn read_protocol_line(state: &mut ProtocolEventState) -> Result<bool> {
    loop {
        if let Some(chunk) = state.current_chunk.as_ref() {
            let available = &chunk.payload()[state.chunk_offset..];
            if available.is_empty() {
                state.current_chunk = None;
                state.chunk_offset = 0;
                continue;
            }
            let newline = available.iter().position(|byte| *byte == b'\n');
            let remaining_frame = state
                .maximum_line_bytes
                .saturating_add(2)
                .saturating_sub(state.line_buffer.len());
            let requested = newline.map_or(available.len(), |index| index + 1);
            let copied = requested.min(remaining_frame);
            state.line_buffer.extend_from_slice(&available[..copied]);
            state.chunk_offset += copied;
            if newline.is_some_and(|index| copied == index + 1) {
                state.line_buffer.truncate(state.line_buffer.len() - 1);
                if state.line_buffer.last() == Some(&b'\r') {
                    state.line_buffer.truncate(state.line_buffer.len() - 1);
                }
                enforce_protocol_payload_boundary(state)?;
                return Ok(true);
            }
            if state.line_buffer.len() > state.maximum_line_bytes
                && !(state.line_buffer.len() == state.maximum_line_bytes.saturating_add(1)
                    && state.line_buffer.last() == Some(&b'\r'))
            {
                return protocol_payload_boundary_error(state);
            }
            if copied < requested {
                return protocol_payload_boundary_error(state);
            }
            continue;
        }
        if state.input_finished {
            enforce_protocol_payload_boundary(state)?;
            return Ok(!state.line_buffer.is_empty());
        }
        let input = state
            .input
            .as_mut()
            .ok_or_else(|| CdfError::internal("subprocess protocol input was not initialized"))?;
        match input.try_next().await? {
            Some(chunk) => state.current_chunk = Some(chunk),
            None => {
                state.input_finished = true;
                state.input = None;
            }
        }
    }
}

fn enforce_protocol_payload_boundary(state: &ProtocolEventState) -> Result<()> {
    if state.line_buffer.len() > state.maximum_line_bytes {
        return protocol_payload_boundary_error(state);
    }
    Ok(())
}

fn protocol_payload_boundary_error<T>(state: &ProtocolEventState) -> Result<T> {
    Err(CdfError::data(format!(
        "subprocess protocol message line {} exceeded the {}-byte payload boundary",
        state.line_number.saturating_add(1),
        state.maximum_line_bytes
    )))
}

enum ProtocolItem {
    Record(Vec<u8>),
    Control(ForeignControlKind),
    Ignore,
}

fn decode_protocol_line(state: &ProtocolEventState) -> Result<ProtocolItem> {
    match &state.protocol {
        SubprocessProtocol::Singer { .. } => {
            let Some(message) = decode_singer_message(state.line_number, &state.line_buffer)?
            else {
                return Ok(ProtocolItem::Ignore);
            };
            enforce_message_scratch(singer_raw(&message), state)?;
            match message {
                SingerMessage::Record(record)
                    if StreamIdentity::singer(&record.stream) == state.selected_stream =>
                {
                    encoded_record(record.record, state)
                }
                SingerMessage::Schema(schema)
                    if StreamIdentity::singer(&schema.stream) == state.selected_stream =>
                {
                    metadata_control("singer", "schema", &schema.raw)
                }
                SingerMessage::State(protocol_state) => {
                    Ok(ProtocolItem::Control(ForeignControlKind::ForeignState {
                        position: protocol_state.source_position()?,
                    }))
                }
                SingerMessage::Other(other) => {
                    metadata_control("singer", &other.message_type, &other.raw)
                }
                SingerMessage::Record(_) | SingerMessage::Schema(_) => Ok(ProtocolItem::Ignore),
            }
        }
        SubprocessProtocol::Airbyte { .. } => {
            let Some(message) = decode_airbyte_message(state.line_number, &state.line_buffer)?
            else {
                return Ok(ProtocolItem::Ignore);
            };
            enforce_message_scratch(airbyte_raw(&message), state)?;
            match message {
                AirbyteMessage::Record(record)
                    if StreamIdentity::airbyte(record.namespace.clone(), &record.stream)
                        == state.selected_stream =>
                {
                    encoded_record(record.data, state)
                }
                AirbyteMessage::State(protocol_state)
                    if protocol_state
                        .stream
                        .as_ref()
                        .is_none_or(|stream| stream == &state.selected_stream) =>
                {
                    Ok(ProtocolItem::Control(ForeignControlKind::ForeignState {
                        position: protocol_state.source_position()?,
                    }))
                }
                AirbyteMessage::Catalog(catalog) => {
                    metadata_control("airbyte", "catalog", &catalog.raw)
                }
                AirbyteMessage::Other(other) => {
                    metadata_control("airbyte", &other.message_type, &other.raw)
                }
                AirbyteMessage::Record(_) | AirbyteMessage::State(_) => Ok(ProtocolItem::Ignore),
            }
        }
        _ => Err(CdfError::internal(
            "protocol line decoder requires Singer or Airbyte",
        )),
    }
}

fn singer_raw(message: &SingerMessage) -> &Value {
    match message {
        SingerMessage::Schema(message) => &message.raw,
        SingerMessage::Record(message) => &message.raw,
        SingerMessage::State(message) => &message.raw,
        SingerMessage::Other(message) => &message.raw,
    }
}

fn airbyte_raw(message: &AirbyteMessage) -> &Value {
    match message {
        AirbyteMessage::Catalog(message) => &message.raw,
        AirbyteMessage::Record(message) => &message.raw,
        AirbyteMessage::State(message) => &message.raw,
        AirbyteMessage::Other(message) => &message.raw,
    }
}

fn enforce_message_scratch(value: &Value, state: &ProtocolEventState) -> Result<()> {
    let estimated = estimated_value_bytes(value)?;
    if estimated > state.supervision.protocol_parser_scratch_bytes {
        return Err(CdfError::data(format!(
            "subprocess protocol message line {} requires an estimated {estimated} parser bytes, above the configured {}-byte parser scratch window",
            state.line_number, state.supervision.protocol_parser_scratch_bytes
        )));
    }
    Ok(())
}

fn estimated_value_bytes(value: &Value) -> Result<u64> {
    let base = u64::try_from(size_of::<Value>())
        .map_err(|_| CdfError::internal("JSON value size exceeds u64"))?;
    let nested = match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => 0,
        Value::String(value) => u64::try_from(value.len())
            .map_err(|_| CdfError::data("JSON string length exceeds u64"))?,
        Value::Array(values) => values.iter().try_fold(0_u64, |total, value| {
            total
                .checked_add(estimated_value_bytes(value)?)
                .ok_or_else(|| CdfError::data("JSON parser scratch estimate overflowed"))
        })?,
        Value::Object(values) => values.iter().try_fold(0_u64, |total, (key, value)| {
            let key = u64::try_from(key.len())
                .map_err(|_| CdfError::data("JSON object key length exceeds u64"))?;
            let value = estimated_value_bytes(value)?;
            total
                .checked_add(key)
                .and_then(|total| total.checked_add(value))
                .ok_or_else(|| CdfError::data("JSON parser scratch estimate overflowed"))
        })?,
    };
    base.checked_add(nested)
        .ok_or_else(|| CdfError::data("JSON parser scratch estimate overflowed"))
}

fn encoded_record(value: Value, state: &ProtocolEventState) -> Result<ProtocolItem> {
    let mut encoded = serde_json::to_vec(&value).map_err(|error| {
        CdfError::data(format!("serialize subprocess protocol record: {error}"))
    })?;
    encoded.push(b'\n');
    if encoded.len() > state.row_window_bytes {
        return Err(CdfError::data(format!(
            "subprocess protocol record at line {} requires {} bytes, above the configured {}-byte row window",
            state.line_number,
            encoded.len(),
            state.row_window_bytes
        )));
    }
    Ok(ProtocolItem::Record(encoded))
}

fn metadata_control(protocol: &str, message_type: &str, raw: &Value) -> Result<ProtocolItem> {
    Ok(ProtocolItem::Control(
        ForeignControlKind::ProtocolMetadata {
            protocol: protocol.to_owned(),
            message_type: message_type.to_ascii_lowercase(),
            payload_sha256: canonical_json_hash(raw)?,
        },
    ))
}

async fn append_protocol_record(state: &mut ProtocolEventState, record: Vec<u8>) -> Result<()> {
    if !state.row_buffer.is_empty()
        && state.row_buffer.len().saturating_add(record.len()) > state.row_window_bytes
    {
        state.pending_record = Some(record);
        start_row_window_decode(state).await?;
        return Ok(());
    }
    if state.row_lease.is_none() {
        let cancellation = state.lifecycle.run_cancellation();
        state.row_lease = Some(
            reserve_protocol_memory(
                &state.memory,
                "subprocess-protocol-row-window",
                state.supervision.protocol_row_window_bytes,
                &cancellation,
            )
            .await?,
        );
        state.row_buffer = Vec::with_capacity(state.row_window_bytes);
    }
    state.row_buffer.extend_from_slice(&record);
    Ok(())
}

async fn start_row_window_decode(state: &mut ProtocolEventState) -> Result<()> {
    if state.row_buffer.is_empty() {
        return Err(CdfError::internal(
            "subprocess protocol attempted to decode an empty row window",
        ));
    }
    let lease = state
        .row_lease
        .take()
        .ok_or_else(|| CdfError::internal("subprocess protocol row window omitted its lease"))?;
    let bytes = std::mem::take(&mut state.row_buffer);
    let accounted = AccountedBytes::new(Bytes::from(bytes), lease)?;
    let source: Arc<dyn ByteSource> = Arc::new(MemoryByteSource::from_accounted_bytes(
        format!("subprocess-protocol:{}", state.selected_stream.scope_name()),
        accounted,
    )?);
    let stream = decode_format_stream(
        Arc::new(cdf_format_json::NdjsonFormatDriver::new()?),
        source,
        BoundedFormatRequest::new(state.read_options.clone(), Arc::clone(&state.memory))
            .with_schema(state.schema.clone())
            .with_cancellation(state.lifecycle.run_cancellation()),
    )
    .await?;
    state.decoder = Some(stream.batches);
    Ok(())
}

fn next_sequence(state: &mut ProtocolEventState) -> Result<u64> {
    let sequence = state.next_sequence;
    state.next_sequence = state
        .next_sequence
        .checked_add(1)
        .ok_or_else(|| CdfError::data("subprocess foreign event sequence overflowed"))?;
    Ok(sequence)
}

fn next_outcome_event(
    state: &mut ProtocolEventState,
    batch: cdf_kernel::Batch,
) -> Result<ForeignStreamEvent> {
    ForeignBatchOutcome::new(
        next_sequence(state)?,
        batch,
        ForeignTransferMode::RowCompat,
        ForeignCopyClassification::CopyUnknown,
    )
    .map(ForeignStreamEvent::Outcome)
}

fn next_control_event(
    state: &mut ProtocolEventState,
    control: ForeignControlKind,
) -> Result<ForeignStreamEvent> {
    ForeignControlEvent::new(next_sequence(state)?, control).map(ForeignStreamEvent::Control)
}

async fn terminal_event(state: &mut ProtocolEventState) -> ForeignStreamEvent {
    match state.lifecycle.terminal().await {
        SubprocessTerminal::Succeeded { diagnostic } => {
            let terminal = ForeignTerminalStatus::Succeeded {
                final_position: state.last_position.clone(),
            };
            if let Some(diagnostic) = diagnostic {
                state.pending_terminal = Some(terminal);
                let suffix = if diagnostic.truncated {
                    format!(
                        " ({} diagnostic bytes discarded)",
                        diagnostic.discarded_bytes
                    )
                } else {
                    String::new()
                };
                match next_control_event(
                    state,
                    ForeignControlKind::Diagnostic {
                        severity: ForeignDiagnosticSeverity::Info,
                        message: format!("{}{}", diagnostic.summary, suffix),
                    },
                ) {
                    Ok(event) => event,
                    Err(error) => ForeignStreamEvent::Terminal(ForeignTerminalStatus::Failed {
                        retryable: false,
                        message: error.message,
                    }),
                }
            } else {
                state.finished = true;
                ForeignStreamEvent::Terminal(terminal)
            }
        }
        SubprocessTerminal::Failed(error) => {
            state.finished = true;
            ForeignStreamEvent::Terminal(ForeignTerminalStatus::Failed {
                retryable: retryable(&error),
                message: error.message,
            })
        }
        SubprocessTerminal::Cancelled { .. } => {
            state.finished = true;
            ForeignStreamEvent::Terminal(ForeignTerminalStatus::Cancelled)
        }
    }
}

async fn fail_protocol_stream(
    state: &mut ProtocolEventState,
    error: CdfError,
) -> ForeignTerminalStatus {
    let externally_cancelled = state.lifecycle.is_cancelled();
    state.input = None;
    state.current_chunk = None;
    state.decoder = None;
    state.lifecycle.cancel();
    let process_terminal = state.lifecycle.terminal().await;
    state.finished = true;
    match process_terminal {
        SubprocessTerminal::Cancelled { .. } if externally_cancelled => {
            ForeignTerminalStatus::Cancelled
        }
        SubprocessTerminal::Cancelled { diagnostic }
        | SubprocessTerminal::Succeeded { diagnostic } => ForeignTerminalStatus::Failed {
            retryable: retryable(&error),
            message: with_terminal_diagnostic(error.message, diagnostic),
        },
        SubprocessTerminal::Failed(process_error) if process_error != error => {
            ForeignTerminalStatus::Failed {
                retryable: retryable(&error) || retryable(&process_error),
                message: format!(
                    "{}; subprocess cleanup/termination also reported: {}",
                    error.message, process_error.message
                ),
            }
        }
        SubprocessTerminal::Failed(_) => ForeignTerminalStatus::Failed {
            retryable: retryable(&error),
            message: error.message,
        },
    }
}

fn retryable(error: &CdfError) -> bool {
    matches!(error.kind, ErrorKind::Transient | ErrorKind::RateLimited)
}
