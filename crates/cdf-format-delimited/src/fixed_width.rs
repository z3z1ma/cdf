use std::{collections::BTreeMap, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_cast::cast;
use arrow_schema::{DataType, Field, Schema};
use cdf_kernel::{
    Batch, BatchId, BoxFuture, CdfError, PushdownFidelity, Result, with_physical_type,
};
use cdf_memory::{ConsumerKey, MemoryClass, MemoryLease, ReservationRequest, reserve};
use cdf_runtime::{
    AccountedByteStream, AccountedPhysicalBatch, ByteExtent, ByteSource, DecodePlanningRequest,
    DecodeUnitPlan, FormatDecodeSession, FormatDetection, FormatDetectionConfidence,
    FormatDetectionProbe, FormatDiscoveryCapabilities, FormatDiscoveryKind, FormatDiscoveryRequest,
    FormatDriver, FormatDriverDescriptor, FormatErrorIsolation, FormatId, FormatProbe,
    FormatSourceAccess, PhysicalDecodeRequest, PhysicalDecodeStream, PhysicalSchemaObservation,
    SequentialReadRequest,
};
use cdf_transform_character::CharacterEncoding;
use futures_util::{TryStreamExt, stream};
use serde::{Deserialize, Serialize};

const INPUT_CHUNK_CEILING_BYTES: u64 = 4 * 1024 * 1024;
const WORKING_SET_CEILING_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug)]
pub struct FixedWidthFormatDriver {
    descriptor: FormatDriverDescriptor,
}

impl FixedWidthFormatDriver {
    pub fn new() -> Result<Self> {
        Ok(Self {
            descriptor: FormatDriverDescriptor {
                format_id: FormatId::new("fixed_width")?,
                semantic_version: "1.0.0".to_owned(),
                aliases: vec!["fixed".to_owned()],
                // Generic text extensions are deliberately omitted: fixed-width cannot be
                // distinguished safely from other text without an explicit format/layout.
                extensions: Vec::new(),
                mime_types: vec!["text/plain".to_owned()],
                magic: Vec::new(),
                detection_probe: FormatDetectionProbe {
                    prefix_bytes: 0,
                    suffix_bytes: 0,
                },
                option_schema: fixed_width_option_schema(),
                projection_pushdown: PushdownFidelity::Unsupported,
                predicate_pushdown: PushdownFidelity::Unsupported,
                predicate_operators: Vec::new(),
                source_access: FormatSourceAccess::Sequential,
                discovery: FormatDiscoveryCapabilities::only(FormatDiscoveryKind::FormatMetadata),
                decode_unit_policy: "fixed_width_stream_v1".to_owned(),
                error_isolation: FormatErrorIsolation::DecodeUnit,
                decode_cpu: cdf_runtime::CpuTaskSpec {
                    task_kind: "format.fixed_width.decode".to_owned(),
                    cpu_slot_cost: 1,
                    native_internal_parallelism: 1,
                },
                minimum_working_set_bytes: 1024 * 1024,
                maximum_working_set_bytes: WORKING_SET_CEILING_BYTES,
            },
        })
    }

    fn options(&self, value: serde_json::Value) -> Result<FixedWidthOptions> {
        let options: FixedWidthOptions = serde_json::from_value(value).map_err(|error| {
            CdfError::contract(format!("invalid fixed-width format options: {error}"))
        })?;
        options.validate()?;
        Ok(options)
    }
}

impl FormatDriver for FixedWidthFormatDriver {
    fn descriptor(&self) -> &FormatDriverDescriptor {
        &self.descriptor
    }

    fn canonical_options(&self, options: serde_json::Value) -> Result<serde_json::Value> {
        serde_json::to_value(self.options(options)?)
            .map_err(|error| CdfError::internal(format!("serialize fixed-width options: {error}")))
    }

    fn detect(&self, _probe: &FormatProbe) -> Result<FormatDetection> {
        Ok(FormatDetection {
            confidence: FormatDetectionConfidence::None,
            reason: "fixed-width text requires an explicit format and pinned layout".to_owned(),
        })
    }

    fn discover(
        &self,
        source: Arc<dyn ByteSource>,
        request: FormatDiscoveryRequest,
    ) -> BoxFuture<'_, Result<PhysicalSchemaObservation>> {
        Box::pin(async move {
            request.cancellation.check()?;
            let options = self.options(request.options)?;
            let fields = options
                .fields
                .iter()
                .map(|layout| {
                    with_physical_type(
                        Field::new(
                            &layout.name,
                            DataType::Utf8,
                            !options.null_tokens.is_empty(),
                        ),
                        format!(
                            "fixed_width/{}/{}[{}..{}]",
                            options.encoding.as_str(),
                            options.unit.as_str(),
                            layout.start,
                            layout.end
                        ),
                    )
                })
                .collect::<Vec<_>>();
            Ok(PhysicalSchemaObservation {
                identity: source.identity().clone(),
                arrow_schema: Arc::new(Schema::new(fields)),
                sampled_bytes: 0,
                sampled_records: 0,
                evidence: BTreeMap::from([
                    (
                        "layout_version".to_owned(),
                        options.layout_version.to_string(),
                    ),
                    ("layout_unit".to_owned(), options.unit.as_str().to_owned()),
                    ("encoding".to_owned(), options.encoding.as_str().to_owned()),
                ]),
            })
        })
    }

    fn prepare_decode(
        &self,
        source: Arc<dyn ByteSource>,
        request: DecodePlanningRequest,
    ) -> BoxFuture<'_, Result<Arc<dyn FormatDecodeSession>>> {
        Box::pin(async move {
            request.cancellation.check()?;
            if request.target_batch_rows == 0 || request.target_batch_bytes == 0 {
                return Err(CdfError::contract(
                    "fixed-width planning requires nonzero row and byte batch targets",
                ));
            }
            if request.projection.is_some() || !request.predicates.is_empty() {
                return Err(CdfError::contract(
                    "fixed-width projection and predicate pushdown are unsupported",
                ));
            }
            let options = self.options(request.options)?;
            let estimated_working_set_bytes = fixed_working_set_bytes(
                request.target_batch_rows,
                request.target_batch_bytes,
                &options,
            )?;
            let units = vec![DecodeUnitPlan {
                unit_id: "fixed-width-stream".to_owned(),
                ordinal: 0,
                extent: source
                    .identity()
                    .size_bytes
                    .map(|size| ByteExtent::new(0, size))
                    .transpose()?,
                estimated_working_set_bytes,
                independently_retryable: true,
            }];
            Ok(Arc::new(FixedWidthDecodeSession {
                source,
                units,
                options,
            }) as Arc<dyn FormatDecodeSession>)
        })
    }
}

fn fixed_width_option_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "layout_version", "unit", "encoding", "line_ending", "trim",
            "null_tokens", "record_width", "fields", "required_gaps", "max_record_bytes"
        ],
        "properties": {
            "layout_version": {"type": "integer", "const": 1},
            "unit": {"type": "string", "enum": ["bytes", "characters"]},
            "encoding": {
                "type": "string",
                "enum": ["utf8", "utf16le", "utf16be", "windows1252", "iso8859_1"]
            },
            "line_ending": {"type": "string", "enum": ["lf", "crlf"]},
            "trim": {"type": "string", "enum": ["none", "ascii", "unicode"]},
            "null_tokens": {"type": "array", "items": {"type": "string"}},
            "record_width": {"type": "integer", "minimum": 1},
            "max_record_bytes": {"type": "integer", "minimum": 1},
            "fields": {
                "type": "array",
                "minItems": 1,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["name", "start", "end"],
                    "properties": {
                        "name": {"type": "string", "minLength": 1},
                        "start": {"type": "integer", "minimum": 0},
                        "end": {"type": "integer", "minimum": 1}
                    }
                }
            },
            "required_gaps": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["start", "end"],
                    "properties": {
                        "start": {"type": "integer", "minimum": 0},
                        "end": {"type": "integer", "minimum": 1}
                    }
                }
            },
            "discriminator": {
                "type": "object",
                "additionalProperties": false,
                "required": ["start", "end", "value"],
                "properties": {
                    "start": {"type": "integer", "minimum": 0},
                    "end": {"type": "integer", "minimum": 1},
                    "value": {"type": "string"}
                }
            }
        }
    })
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FixedWidthOptions {
    layout_version: u16,
    unit: LayoutUnit,
    encoding: CharacterEncoding,
    line_ending: LineEnding,
    trim: TrimPolicy,
    null_tokens: Vec<String>,
    record_width: usize,
    fields: Vec<LayoutField>,
    required_gaps: Vec<LayoutRange>,
    #[serde(skip_serializing_if = "Option::is_none")]
    discriminator: Option<Discriminator>,
    max_record_bytes: u64,
}

impl FixedWidthOptions {
    fn validate(&self) -> Result<()> {
        if self.layout_version != 1 {
            return Err(CdfError::contract("fixed-width layout_version must be 1"));
        }
        if self.record_width == 0 || self.fields.is_empty() || self.max_record_bytes == 0 {
            return Err(CdfError::contract(
                "fixed-width layout requires positive record_width/max_record_bytes and at least one field",
            ));
        }
        let minimum_bytes = match self.unit {
            LayoutUnit::Bytes => u64::try_from(self.record_width),
            LayoutUnit::Characters => u64::try_from(self.record_width)
                .map(|width| width.saturating_mul(self.encoding.maximum_utf8_bytes_per_unit())),
        }
        .map_err(|_| CdfError::contract("fixed-width record width exceeds u64"))?;
        if minimum_bytes > self.max_record_bytes {
            return Err(CdfError::contract(
                "fixed-width max_record_bytes is smaller than the declared record width",
            ));
        }
        if self.max_record_bytes > WORKING_SET_CEILING_BYTES / 2 {
            return Err(CdfError::contract(format!(
                "fixed-width max_record_bytes exceeds the driver working-set ceiling of {0} bytes",
                WORKING_SET_CEILING_BYTES / 2
            )));
        }
        let mut names = std::collections::BTreeSet::new();
        let mut coverage = vec![None::<&'static str>; self.record_width];
        for field in &self.fields {
            validate_range(field.start, field.end, self.record_width, "field")?;
            if !names.insert(field.name.as_str()) {
                return Err(CdfError::contract(format!(
                    "fixed-width layout repeats field name `{}`",
                    field.name
                )));
            }
            mark_coverage(&mut coverage, field.start, field.end, "field")?;
        }
        for gap in &self.required_gaps {
            validate_range(gap.start, gap.end, self.record_width, "required gap")?;
            mark_coverage(&mut coverage, gap.start, gap.end, "required gap")?;
        }
        if let Some(index) = coverage.iter().position(Option::is_none) {
            return Err(CdfError::contract(format!(
                "fixed-width layout leaves position {index} unclassified; add a field or required_gap range"
            )));
        }
        if let Some(discriminator) = &self.discriminator {
            validate_range(
                discriminator.start,
                discriminator.end,
                self.record_width,
                "discriminator",
            )?;
        }
        if self.unit == LayoutUnit::Bytes && self.encoding.is_utf16() {
            for (label, start, end) in self
                .fields
                .iter()
                .map(|field| (field.name.as_str(), field.start, field.end))
                .chain(
                    self.required_gaps
                        .iter()
                        .map(|gap| ("required_gap", gap.start, gap.end)),
                )
                .chain(
                    self.discriminator
                        .iter()
                        .map(|value| ("discriminator", value.start, value.end)),
                )
            {
                if start % 2 != 0 || end % 2 != 0 {
                    return Err(CdfError::contract(format!(
                        "fixed-width UTF-16 byte range `{label}` [{start}..{end}] splits a code unit"
                    )));
                }
            }
        }
        Ok(())
    }

    fn line_ending_bytes(&self) -> Result<Vec<u8>> {
        self.encoding.encode_ascii(match self.line_ending {
            LineEnding::Lf => "\n",
            LineEnding::Crlf => "\r\n",
        })
    }
}

fn validate_range(start: usize, end: usize, width: usize, label: &str) -> Result<()> {
    if start >= end || end > width {
        return Err(CdfError::contract(format!(
            "fixed-width {label} range [{start}..{end}] must be nonempty and within record width {width}"
        )));
    }
    Ok(())
}

fn mark_coverage(
    coverage: &mut [Option<&'static str>],
    start: usize,
    end: usize,
    label: &'static str,
) -> Result<()> {
    for (index, slot) in coverage.iter_mut().enumerate().take(end).skip(start) {
        if let Some(previous) = slot {
            return Err(CdfError::contract(format!(
                "fixed-width {label} overlaps {previous} at position {index}"
            )));
        }
        *slot = Some(label);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LayoutUnit {
    Bytes,
    Characters,
}

impl LayoutUnit {
    fn as_str(self) -> &'static str {
        match self {
            Self::Bytes => "bytes",
            Self::Characters => "characters",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LineEnding {
    Lf,
    Crlf,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TrimPolicy {
    None,
    Ascii,
    Unicode,
}

impl TrimPolicy {
    fn apply(self, value: &str) -> &str {
        match self {
            Self::None => value,
            Self::Ascii => value.trim_matches(|character: char| character.is_ascii_whitespace()),
            Self::Unicode => value.trim(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LayoutField {
    name: String,
    start: usize,
    end: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LayoutRange {
    start: usize,
    end: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Discriminator {
    start: usize,
    end: usize,
    value: String,
}

struct FixedWidthDecodeSession {
    source: Arc<dyn ByteSource>,
    units: Vec<DecodeUnitPlan>,
    options: FixedWidthOptions,
}

impl FormatDecodeSession for FixedWidthDecodeSession {
    fn units(&self) -> &[DecodeUnitPlan] {
        &self.units
    }

    fn decode(
        &self,
        request: PhysicalDecodeRequest,
    ) -> BoxFuture<'_, Result<PhysicalDecodeStream>> {
        Box::pin(async move {
            request.cancellation.check()?;
            self.validate_unit(&request.unit)?;
            validate_decode_schema(&self.options, request.schema.decoder_schema.as_ref())?;
            if request.projection.is_some() || !request.predicates.is_empty() {
                return Err(CdfError::contract(
                    "fixed-width projection and predicate pushdown are unsupported",
                ));
            }
            let preferred_chunk_bytes = request
                .target_batch_bytes
                .clamp(64 * 1024, INPUT_CHUNK_CEILING_BYTES);
            let input = self
                .source
                .open_sequential(SequentialReadRequest {
                    preferred_chunk_bytes,
                    cancellation: request.cancellation.clone(),
                })
                .await?;
            let working_bytes = fixed_working_set_bytes(
                request.target_batch_rows,
                request.target_batch_bytes,
                &self.options,
            )?;
            let working_lease = reserve(
                Arc::clone(&request.memory),
                ReservationRequest::new(
                    ConsumerKey::new("fixed-width-parser", MemoryClass::Decode)?,
                    working_bytes,
                )?
                .as_minimum_working_set(),
            )
            .await?;
            let state = FixedDecodeState {
                input,
                request,
                options: self.options.clone(),
                working_lease,
                buffer: Vec::new(),
                scan_offset: 0,
                input_finished: false,
                prefix_checked: false,
                sequence: 0,
                record_number: 0,
                terminal: false,
            };
            Ok(Box::pin(stream::try_unfold(state, fixed_decode_next)) as PhysicalDecodeStream)
        })
    }
}

fn validate_decode_schema(options: &FixedWidthOptions, schema: &Schema) -> Result<()> {
    if schema.fields().len() != options.fields.len() {
        return Err(CdfError::contract(format!(
            "fixed-width layout declares {} fields but decoder schema has {}",
            options.fields.len(),
            schema.fields().len()
        )));
    }
    for (layout, field) in options.fields.iter().zip(schema.fields()) {
        if layout.name.as_str() != field.name() {
            return Err(CdfError::contract(format!(
                "fixed-width layout field `{}` does not match decoder field `{}`",
                layout.name,
                field.name()
            )));
        }
    }
    Ok(())
}

fn fixed_working_set_bytes(
    target_rows: usize,
    target_bytes: u64,
    options: &FixedWidthOptions,
) -> Result<u64> {
    let offsets = u64::try_from(target_rows)
        .ok()
        .and_then(|rows| rows.checked_mul(options.fields.len() as u64))
        .and_then(|cells| cells.checked_mul(16))
        .ok_or_else(|| CdfError::contract("fixed-width batch offset authority overflowed"))?;
    let bytes = target_bytes
        .checked_add(options.max_record_bytes)
        .and_then(|value| value.checked_add(INPUT_CHUNK_CEILING_BYTES))
        .and_then(|value| value.checked_add(offsets))
        .ok_or_else(|| CdfError::contract("fixed-width working-set authority overflowed"))?;
    if bytes > WORKING_SET_CEILING_BYTES {
        return Err(CdfError::contract(format!(
            "fixed-width batch requires {bytes} working bytes, above the driver ceiling {WORKING_SET_CEILING_BYTES}; lower batch rows/bytes or max_record_bytes"
        )));
    }
    Ok(bytes)
}

struct FixedDecodeState {
    input: AccountedByteStream,
    request: PhysicalDecodeRequest,
    options: FixedWidthOptions,
    #[allow(dead_code)]
    working_lease: MemoryLease,
    buffer: Vec<u8>,
    scan_offset: usize,
    input_finished: bool,
    prefix_checked: bool,
    sequence: u64,
    record_number: u64,
    terminal: bool,
}

async fn fixed_decode_next(
    mut state: FixedDecodeState,
) -> Result<Option<(AccountedPhysicalBatch, FixedDecodeState)>> {
    state.request.cancellation.check()?;
    if state.terminal {
        return Ok(None);
    }
    let mut columns = (0..state.options.fields.len())
        .map(|_| Vec::<Option<String>>::new())
        .collect::<Vec<_>>();
    let mut batch_value_bytes = 0_u64;
    loop {
        state.request.cancellation.check()?;
        if !state.prefix_checked && (state.buffer.len() >= 3 || state.input_finished) {
            let stripped = state.options.encoding.strip_matching_bom(&state.buffer)?;
            let stripped_bytes = state.buffer.len() - stripped.len();
            if stripped_bytes != 0 {
                state.buffer.drain(..stripped_bytes);
            }
            state.prefix_checked = true;
        }
        let terminator = state.options.line_ending_bytes()?;
        let next_end = find_subslice(
            &state.buffer,
            state.scan_offset,
            &terminator,
            if state.options.encoding.is_utf16() {
                2
            } else {
                1
            },
        );
        let record = if let Some(end) = next_end {
            let record = state.buffer[state.scan_offset..end].to_vec();
            state.scan_offset = end + terminator.len();
            Some(record)
        } else if state.input_finished && state.scan_offset < state.buffer.len() {
            let record = state.buffer[state.scan_offset..].to_vec();
            state.scan_offset = state.buffer.len();
            Some(record)
        } else {
            None
        };
        if let Some(record) = record {
            state.record_number = state
                .record_number
                .checked_add(1)
                .ok_or_else(|| CdfError::data("fixed-width record number overflowed"))?;
            if u64::try_from(record.len()).unwrap_or(u64::MAX) > state.options.max_record_bytes {
                return Err(CdfError::data(format!(
                    "fixed-width record {} exceeds max_record_bytes {}",
                    state.record_number, state.options.max_record_bytes
                )));
            }
            let values = parse_record(&state.options, &record, state.record_number)?;
            for (column, value) in columns.iter_mut().zip(values) {
                if let Some(value) = &value {
                    batch_value_bytes = batch_value_bytes.saturating_add(value.len() as u64);
                }
                column.push(value);
            }
            if columns[0].len() >= state.request.target_batch_rows
                || batch_value_bytes >= state.request.target_batch_bytes
            {
                break;
            }
            continue;
        }
        if state.input_finished {
            state.terminal = true;
            break;
        }
        if state.buffer.len().saturating_sub(state.scan_offset)
            > usize::try_from(state.options.max_record_bytes).unwrap_or(usize::MAX)
        {
            return Err(CdfError::data(format!(
                "fixed-width record {} exceeds max_record_bytes {} before a line ending",
                state.record_number.saturating_add(1),
                state.options.max_record_bytes
            )));
        }
        let next = state.input.try_next().await?;
        match next {
            Some(chunk) => state.buffer.extend_from_slice(chunk.payload()),
            None => state.input_finished = true,
        }
    }

    if state.scan_offset != 0 {
        state.buffer.drain(..state.scan_offset);
        state.scan_offset = 0;
    }
    if columns[0].is_empty() {
        if state.sequence != 0 {
            return Ok(None);
        }
        let batch = RecordBatch::new_empty(Arc::clone(&state.request.schema.decoder_schema));
        state.terminal = true;
        return emit_fixed_batch(state, batch).await.map(Some);
    }
    let batch = build_record_batch(&state.options, &state.request, columns)?;
    emit_fixed_batch(state, batch).await.map(Some)
}

async fn emit_fixed_batch(
    mut state: FixedDecodeState,
    record_batch: RecordBatch,
) -> Result<(AccountedPhysicalBatch, FixedDecodeState)> {
    let retained = cdf_memory::record_batch_retained_bytes(&record_batch)?.max(1);
    let lease = reserve(
        Arc::clone(&state.request.memory),
        ReservationRequest::new(
            ConsumerKey::new("fixed-width-output", MemoryClass::Decode)?,
            retained,
        )?
        .as_minimum_working_set(),
    )
    .await?;
    let batch_id = BatchId::new(format!(
        "{}-u{:08}-b{:08}",
        state.request.batch_id_prefix, state.request.unit.ordinal, state.sequence
    ))?;
    state.sequence = state
        .sequence
        .checked_add(1)
        .ok_or_else(|| CdfError::data("fixed-width batch sequence overflowed"))?;
    let mut batch = Batch::from_record_batch(
        batch_id,
        state.request.resource_id.clone(),
        state.request.partition_id.clone(),
        cdf_kernel::canonical_arrow_schema_hash(state.request.schema.decoder_schema.as_ref())?,
        record_batch,
    )?;
    batch.header.source_position = state.request.source_position.clone();
    Ok((AccountedPhysicalBatch::new(batch, lease)?, state))
}

fn build_record_batch(
    _options: &FixedWidthOptions,
    request: &PhysicalDecodeRequest,
    columns: Vec<Vec<Option<String>>>,
) -> Result<RecordBatch> {
    let arrays = columns
        .into_iter()
        .zip(request.schema.decoder_schema.fields())
        .map(|(values, field)| {
            let strings = StringArray::from(values);
            if field.data_type() == &DataType::Utf8 {
                Ok(Arc::new(strings) as ArrayRef)
            } else {
                cast(&strings, field.data_type()).map_err(|error| {
                    CdfError::data(format!(
                        "cast fixed-width field `{}` from utf8 to {}: {error}",
                        field.name(),
                        field.data_type()
                    ))
                })
            }
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(Arc::clone(&request.schema.decoder_schema), arrays)
        .map_err(|error| CdfError::data(format!("build fixed-width Arrow batch: {error}")))
}

fn parse_record(
    options: &FixedWidthOptions,
    record: &[u8],
    record_number: u64,
) -> Result<Vec<Option<String>>> {
    let decoded = if options.unit == LayoutUnit::Characters {
        Some(
            options
                .encoding
                .decode_slice(record, &format!("fixed-width record {record_number}"))?,
        )
    } else {
        None
    };
    let observed_width = match &decoded {
        Some(value) => value.chars().count(),
        None => record.len(),
    };
    if observed_width != options.record_width {
        return Err(CdfError::data(format!(
            "fixed-width record {record_number} has {} {} but layout requires exactly {}",
            observed_width,
            options.unit.as_str(),
            options.record_width
        )));
    }
    if let Some(discriminator) = &options.discriminator {
        let value = extract_range(
            options,
            record,
            decoded.as_deref(),
            discriminator.start,
            discriminator.end,
            record_number,
            "discriminator",
        )?;
        if value != discriminator.value {
            return Err(CdfError::data(format!(
                "fixed-width record {record_number} discriminator is {value:?}, expected {:?}",
                discriminator.value
            )));
        }
    }
    options
        .fields
        .iter()
        .map(|field| {
            let value = extract_range(
                options,
                record,
                decoded.as_deref(),
                field.start,
                field.end,
                record_number,
                &format!("field `{}`", field.name),
            )?;
            let value = options.trim.apply(&value);
            Ok(if options.null_tokens.iter().any(|token| token == value) {
                None
            } else {
                Some(value.to_owned())
            })
        })
        .collect()
}

fn extract_range(
    options: &FixedWidthOptions,
    record: &[u8],
    decoded: Option<&str>,
    start: usize,
    end: usize,
    record_number: u64,
    label: &str,
) -> Result<String> {
    match options.unit {
        LayoutUnit::Bytes => options.encoding.decode_slice(
            &record[start..end],
            &format!("fixed-width record {record_number} {label}"),
        ),
        LayoutUnit::Characters => {
            let value = decoded.ok_or_else(|| {
                CdfError::internal("fixed-width character record was not decoded")
            })?;
            Ok(value.chars().skip(start).take(end - start).collect())
        }
    }
}

fn find_subslice(haystack: &[u8], start: usize, needle: &[u8], alignment: usize) -> Option<usize> {
    if needle.is_empty() || start > haystack.len() || alignment == 0 {
        return None;
    }
    haystack[start..]
        .windows(needle.len())
        .enumerate()
        .find(|(offset, window)| offset.is_multiple_of(alignment) && *window == needle)
        .map(|(offset, _)| offset)
        .map(|offset| start + offset)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bytes::Bytes;
    use cdf_kernel::{PartitionId, ResourceId};
    use cdf_memory::{
        AccountedBytes, DeterministicMemoryCoordinator, MemoryCoordinator, reserve_blocking,
    };
    use cdf_runtime::{AccountedByteStream, DecodeSchemaPlan, RunCancellation};

    use super::*;

    fn options(unit: &str, encoding: &str, width: usize) -> serde_json::Value {
        serde_json::json!({
            "layout_version": 1,
            "unit": unit,
            "encoding": encoding,
            "line_ending": "lf",
            "trim": "ascii",
            "null_tokens": ["NULL"],
            "record_width": width,
            "fields": [
                {"name": "id", "start": 0, "end": 4},
                {"name": "name", "start": 5, "end": width}
            ],
            "required_gaps": [{"start": 4, "end": 5}],
            "max_record_bytes": 1024
        })
    }

    #[test]
    fn layout_requires_explicit_nonoverlapping_coverage() {
        let driver = FixedWidthFormatDriver::new().unwrap();
        let mut missing_gap = options("bytes", "utf8", 9);
        missing_gap["required_gaps"] = serde_json::json!([]);
        assert!(
            driver
                .canonical_options(missing_gap)
                .unwrap_err()
                .message
                .contains("unclassified")
        );

        let mut overlap = options("bytes", "utf8", 9);
        overlap["required_gaps"] = serde_json::json!([{"start": 3, "end": 5}]);
        assert!(
            driver
                .canonical_options(overlap)
                .unwrap_err()
                .message
                .contains("overlaps")
        );
    }

    #[test]
    fn byte_and_character_layouts_preserve_encoding_boundaries() {
        let driver = FixedWidthFormatDriver::new().unwrap();
        let byte_options = driver.options(options("bytes", "utf8", 10)).unwrap();
        let values = parse_record(&byte_options, b"0001 Alice", 1).unwrap();
        assert_eq!(
            values,
            vec![Some("0001".to_owned()), Some("Alice".to_owned())]
        );

        let mut character = options("characters", "utf8", 8);
        character["fields"] = serde_json::json!([
            {"name": "id", "start": 0, "end": 2},
            {"name": "name", "start": 3, "end": 8}
        ]);
        character["required_gaps"] = serde_json::json!([{"start": 2, "end": 3}]);
        let character_options = driver.options(character).unwrap();
        let values = parse_record(&character_options, "01 José!".as_bytes(), 1).unwrap();
        assert_eq!(
            values,
            vec![Some("01".to_owned()), Some("José!".to_owned())]
        );

        let mut split = options("bytes", "utf8", 7);
        split["fields"] = serde_json::json!([
            {"name": "id", "start": 0, "end": 4},
            {"name": "name", "start": 5, "end": 6}
        ]);
        split["required_gaps"] =
            serde_json::json!([{"start": 4, "end": 5}, {"start": 6, "end": 7}]);
        let split_options = driver.options(split).unwrap();
        let error = parse_record(&split_options, "0001 é".as_bytes(), 1).unwrap_err();
        assert!(error.message.contains("invalid UTF-8"));
    }

    #[test]
    fn utf16_byte_layout_and_discriminator_are_exact() {
        let driver = FixedWidthFormatDriver::new().unwrap();
        let mut value = options("bytes", "utf16le", 12);
        value["fields"] = serde_json::json!([
            {"name": "id", "start": 0, "end": 4},
            {"name": "name", "start": 6, "end": 12}
        ]);
        value["required_gaps"] = serde_json::json!([{"start": 4, "end": 6}]);
        value["discriminator"] = serde_json::json!({"start": 0, "end": 4, "value": "01"});
        let options = driver.options(value).unwrap();
        let record = CharacterEncoding::Utf16Le.encode_ascii("01 Bob").unwrap();
        let values = parse_record(&options, &record, 1).unwrap();
        assert_eq!(values, vec![Some("01".to_owned()), Some("Bob".to_owned())]);
    }

    #[test]
    fn chunked_stream_casts_typed_fields_and_preserves_batch_order() {
        let driver = FixedWidthFormatDriver::new().unwrap();
        let options = driver.options(options("bytes", "utf8", 10)).unwrap();
        let memory: Arc<dyn MemoryCoordinator> = Arc::new(
            DeterministicMemoryCoordinator::new(64 * 1024 * 1024, BTreeMap::new()).unwrap(),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let request = PhysicalDecodeRequest {
            unit: DecodeUnitPlan {
                unit_id: "fixed-width-stream".to_owned(),
                ordinal: 0,
                extent: None,
                estimated_working_set_bytes: 1024 * 1024,
                independently_retryable: true,
            },
            resource_id: ResourceId::new("events.fixed").unwrap(),
            partition_id: PartitionId::new("file-fixed").unwrap(),
            batch_id_prefix: "events-fixed".to_owned(),
            schema: DecodeSchemaPlan::fixed_admission(schema),
            source_position: None,
            projection: None,
            predicates: Vec::new(),
            target_batch_rows: 1,
            target_batch_bytes: 1024 * 1024,
            memory: Arc::clone(&memory),
            cancellation: RunCancellation::default(),
        };
        let chunks = [b"0001 Al".as_slice(), b"ice\n0002 Bob  \n".as_slice()]
            .into_iter()
            .map(|payload| {
                let lease = reserve_blocking(
                    Arc::clone(&memory),
                    &ReservationRequest::new(
                        ConsumerKey::new("fixed-width-test-input", MemoryClass::Source).unwrap(),
                        payload.len() as u64,
                    )
                    .unwrap(),
                )
                .unwrap();
                Ok(AccountedBytes::new(Bytes::copy_from_slice(payload), lease).unwrap())
            })
            .collect::<Vec<Result<AccountedBytes>>>();
        let input: AccountedByteStream = Box::pin(stream::iter(chunks));
        let working_lease = reserve_blocking(
            Arc::clone(&memory),
            &ReservationRequest::new(
                ConsumerKey::new("fixed-width-test-parser", MemoryClass::Decode).unwrap(),
                4 * 1024 * 1024,
            )
            .unwrap(),
        )
        .unwrap();
        let mut state = FixedDecodeState {
            input,
            request,
            options,
            working_lease,
            buffer: Vec::new(),
            scan_offset: 0,
            input_finished: false,
            prefix_checked: false,
            sequence: 0,
            record_number: 0,
            terminal: false,
        };
        let batches = futures_executor::block_on(async move {
            let mut batches = Vec::new();
            while let Some((batch, next)) = fixed_decode_next(state).await? {
                batches.push(
                    batch
                        .into_batch()?
                        .record_batch()
                        .ok_or_else(|| {
                            CdfError::internal("fixed-width test batch omitted payload")
                        })?
                        .clone(),
                );
                state = next;
            }
            Result::<Vec<RecordBatch>>::Ok(batches)
        })
        .unwrap();
        assert_eq!(batches.len(), 2);
        let first_id = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        let second_name = batches[1]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(first_id.value(0), 1);
        assert_eq!(second_name.value(0), "Bob");
    }
}
