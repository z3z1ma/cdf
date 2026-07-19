#![doc = "Streaming delimited text format drivers for cdf."]

mod fixed_width;

pub use fixed_width::FixedWidthFormatDriver;

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_csv::reader::{Decoder, Format, ReaderBuilder};
use cdf_kernel::{Batch, BatchId, BoxFuture, CdfError, PushdownFidelity, Result};
use cdf_memory::{ConsumerKey, MemoryClass, MemoryLease, ReservationRequest, reserve};
use cdf_runtime::{
    AccountedByteStream, AccountedChunksReader, AccountedPhysicalBatch, ByteExtent, ByteSource,
    DecodePlanningRequest, DecodeUnitPlan, FormatDecodeSession, FormatDetection,
    FormatDetectionConfidence, FormatDetectionProbe, FormatDiscoveryRequest, FormatDriver,
    FormatDriverDescriptor, FormatId, FormatProbe, PhysicalDecodeRequest, PhysicalDecodeStream,
    PhysicalSchemaObservation, SequentialReadRequest,
};
use futures_util::{TryStreamExt, stream};

const DISCOVERY_CHUNK_BYTES: u64 = 1024 * 1024;
const PARALLEL_PLANNING_CHUNK_BYTES: u64 = 4 * 1024 * 1024;
const DEFAULT_PARALLEL_UNIT_BYTES: u64 = 32 * 1024 * 1024;
const DEFAULT_MAXIMUM_RECORD_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Debug)]
pub struct DelimitedFormatDriver {
    descriptor: FormatDriverDescriptor,
    label: &'static str,
    default_delimiter: Option<u8>,
}

pub type CsvFormatDriver = DelimitedFormatDriver;

impl DelimitedFormatDriver {
    pub fn new() -> Result<Self> {
        Self::csv()
    }

    pub fn csv() -> Result<Self> {
        Ok(Self {
            descriptor: descriptor(
                "csv",
                Vec::new(),
                vec!["csv".to_owned()],
                vec!["text/csv".to_owned()],
                "format.csv.decode",
                "delimited_adaptive_units_v2",
            )?,
            label: "CSV",
            default_delimiter: Some(b','),
        })
    }

    pub fn tsv() -> Result<Self> {
        Ok(Self {
            descriptor: descriptor(
                "tsv",
                vec![
                    "tab_separated".to_owned(),
                    "tab-separated-values".to_owned(),
                ],
                vec!["tsv".to_owned(), "tab".to_owned()],
                vec!["text/tab-separated-values".to_owned()],
                "format.tsv.decode",
                "delimited_adaptive_units_v2",
            )?,
            label: "TSV",
            default_delimiter: Some(b'\t'),
        })
    }

    pub fn psv() -> Result<Self> {
        Ok(Self {
            descriptor: descriptor(
                "psv",
                vec![
                    "pipe_separated".to_owned(),
                    "pipe-separated-values".to_owned(),
                ],
                vec!["psv".to_owned()],
                vec!["text/plain".to_owned()],
                "format.psv.decode",
                "delimited_adaptive_units_v2",
            )?,
            label: "PSV",
            default_delimiter: Some(b'|'),
        })
    }

    pub fn custom() -> Result<Self> {
        Ok(Self {
            descriptor: descriptor(
                "delimited",
                vec!["dsv".to_owned()],
                Vec::new(),
                vec!["text/plain".to_owned()],
                "format.delimited.decode",
                "delimited_adaptive_units_v2",
            )?,
            label: "delimited text",
            default_delimiter: None,
        })
    }
}

fn descriptor(
    format_id: &str,
    aliases: Vec<String>,
    extensions: Vec<String>,
    mime_types: Vec<String>,
    task_kind: &str,
    decode_unit_policy: &str,
) -> Result<FormatDriverDescriptor> {
    Ok(FormatDriverDescriptor {
        format_id: FormatId::new(format_id)?,
        semantic_version: "1.2.0".to_owned(),
        aliases,
        extensions,
        mime_types,
        magic: Vec::new(),
        detection_probe: FormatDetectionProbe {
            prefix_bytes: 4096,
            suffix_bytes: 0,
        },
        option_schema: serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "delimiter": {"type": "string", "minLength": 1, "maxLength": 1},
                "header": {"type": "boolean"},
                "header_validation": {"type": "boolean"},
                "quote": {"type": "string", "minLength": 1, "maxLength": 1},
                "escape": {"type": "string", "minLength": 1, "maxLength": 1},
                "terminator": {"type": "string", "minLength": 1, "maxLength": 1},
                "comment": {"type": "string", "minLength": 1, "maxLength": 1},
                "truncated_rows": {"type": "boolean"},
                "parallel_decode": {"type": "string", "enum": ["auto", "off", "on"]},
                "parallel_unit_bytes": {"type": "integer", "minimum": 1},
                "parallel_max_record_bytes": {"type": "integer", "minimum": 1}
            }
        }),
        projection_pushdown: PushdownFidelity::Unsupported,
        predicate_pushdown: PushdownFidelity::Unsupported,
        predicate_operators: Vec::new(),
        source_access: cdf_runtime::FormatSourceAccess::Sequential,
        discovery: cdf_runtime::FormatDiscoveryCapabilities::only(
            cdf_runtime::FormatDiscoveryKind::BoundedContent,
        ),
        decode_unit_policy: decode_unit_policy.to_owned(),
        error_isolation: cdf_runtime::FormatErrorIsolation::DecodeUnit,
        decode_cpu: cdf_runtime::CpuTaskSpec {
            task_kind: task_kind.to_owned(),
            cpu_slot_cost: 1,
            native_internal_parallelism: 1,
        },
        minimum_working_set_bytes: 1024 * 1024,
        maximum_working_set_bytes: 64 * 1024 * 1024,
    })
}

impl FormatDriver for DelimitedFormatDriver {
    fn descriptor(&self) -> &FormatDriverDescriptor {
        &self.descriptor
    }

    fn canonical_options(&self, options: serde_json::Value) -> Result<serde_json::Value> {
        DelimitedOptions::from_json(self, options)?.to_json(self)
    }

    fn detect(&self, probe: &FormatProbe) -> Result<FormatDetection> {
        let delimiter = self.default_delimiter;
        Ok(FormatDetection {
            confidence: delimiter
                .filter(|delimiter| probe.prefix.contains(delimiter))
                .map(|_| FormatDetectionConfidence::Weak)
                .unwrap_or(FormatDetectionConfidence::None),
            reason: match delimiter {
                Some(delimiter) => format!(
                    "{} has no strong magic; delimiter `{}` was inspected in the prefix",
                    self.label,
                    display_byte(delimiter)
                ),
                None => "custom delimited text has no strong magic or default delimiter".to_owned(),
            },
        })
    }

    fn discover(
        &self,
        source: Arc<dyn ByteSource>,
        request: FormatDiscoveryRequest,
    ) -> BoxFuture<'_, Result<PhysicalSchemaObservation>> {
        Box::pin(async move {
            let format = self.arrow_format(request.options)?;
            request.cancellation.check()?;
            if request.maximum_bytes == 0 || request.maximum_records == 0 {
                return Err(CdfError::contract(
                    "delimited discovery requires nonzero byte and record bounds",
                ));
            }
            let mut input = source
                .open_sequential(SequentialReadRequest {
                    preferred_chunk_bytes: DISCOVERY_CHUNK_BYTES.min(request.maximum_bytes),
                    cancellation: request.cancellation.clone(),
                })
                .await?;
            let mut chunks = Vec::new();
            let mut sampled_bytes = 0_u64;
            while sampled_bytes < request.maximum_bytes {
                let Some(chunk) = input.try_next().await? else {
                    break;
                };
                let chunk_bytes = u64::try_from(chunk.payload().len())
                    .map_err(|_| CdfError::data("delimited discovery chunk length exceeds u64"))?;
                sampled_bytes = sampled_bytes
                    .saturating_add(chunk_bytes)
                    .min(request.maximum_bytes);
                chunks.push(chunk);
            }
            let maximum_records = usize::try_from(request.maximum_records)
                .map_err(|_| CdfError::contract("delimited record bound exceeds usize"))?;
            let (schema, sampled_records) = format
                .infer_schema(
                    AccountedChunksReader::with_byte_limit(chunks, sampled_bytes)?,
                    Some(maximum_records),
                )
                .map_err(|error| CdfError::data(format!("infer delimited schema: {error}")))?;
            let schema = Arc::new(schema);
            Ok(PhysicalSchemaObservation {
                identity: source.identity().clone(),
                arrow_schema: schema,
                sampled_bytes,
                sampled_records: u64::try_from(sampled_records)
                    .map_err(|_| CdfError::data("CSV sampled record count exceeds u64"))?,
                evidence: std::collections::BTreeMap::new(),
            })
        })
    }

    fn prepare_decode(
        &self,
        source: Arc<dyn ByteSource>,
        request: DecodePlanningRequest,
    ) -> BoxFuture<'_, Result<Arc<dyn FormatDecodeSession>>> {
        Box::pin(async move {
            let options = DelimitedOptions::from_json(self, request.options)?;
            request.cancellation.check()?;
            if request.target_batch_rows == 0 || request.target_batch_bytes == 0 {
                return Err(CdfError::contract(
                    "delimited planning requires nonzero row and byte batch targets",
                ));
            }
            if request.projection.is_some() || !request.predicates.is_empty() {
                return Err(CdfError::contract(
                    "delimited projection and predicate pushdown are unsupported",
                ));
            }
            let units = plan_delimited_units(
                Arc::clone(&source),
                &options,
                request.target_batch_bytes,
                request.cancellation.clone(),
            )
            .await?;
            Ok(Arc::new(DelimitedDecodeSession {
                source,
                units,
                options,
            }) as Arc<dyn FormatDecodeSession>)
        })
    }
}

impl DelimitedFormatDriver {
    fn arrow_format(&self, options: serde_json::Value) -> Result<Format> {
        DelimitedOptions::from_json(self, options)?.to_arrow_format()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DelimitedOptions {
    delimiter: u8,
    header: bool,
    header_validation: bool,
    quote: Option<u8>,
    escape: Option<u8>,
    terminator: Option<u8>,
    comment: Option<u8>,
    truncated_rows: bool,
    parallel_decode: ParallelDecode,
    parallel_unit_bytes: u64,
    parallel_max_record_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ParallelDecode {
    Auto,
    Off,
    On,
}

impl DelimitedOptions {
    fn from_json(driver: &DelimitedFormatDriver, options: serde_json::Value) -> Result<Self> {
        let object = options.as_object().ok_or_else(|| {
            CdfError::contract(format!("{} format options must be an object", driver.label))
        })?;
        for key in object.keys() {
            if !matches!(
                key.as_str(),
                "delimiter"
                    | "header"
                    | "header_validation"
                    | "quote"
                    | "escape"
                    | "terminator"
                    | "comment"
                    | "truncated_rows"
                    | "parallel_decode"
                    | "parallel_unit_bytes"
                    | "parallel_max_record_bytes"
            ) {
                return Err(CdfError::contract(format!(
                    "{} format option `{key}` is unsupported",
                    driver.label
                )));
            }
        }
        let delimiter_override = byte_option(object, "delimiter", driver.label)?;
        let delimiter = delimiter_override
            .or(driver.default_delimiter)
            .ok_or_else(|| {
                CdfError::contract("custom delimited format requires `format_options.delimiter`")
            })?;
        if matches!(delimiter, b'\n' | b'\r') {
            return Err(CdfError::contract(
                "delimited delimiter cannot be a line terminator",
            ));
        }
        let header = bool_option(object, "header", true, driver.label)?;
        let header_validation = bool_option(object, "header_validation", false, driver.label)?;
        if header_validation && !header {
            return Err(CdfError::contract(
                "delimited header_validation requires header = true",
            ));
        }
        let quote = byte_option(object, "quote", driver.label)?;
        if quote == Some(delimiter) {
            return Err(CdfError::contract(
                "delimited quote character cannot equal delimiter",
            ));
        }
        let escape = byte_option(object, "escape", driver.label)?;
        let terminator = byte_option(object, "terminator", driver.label)?;
        if terminator == Some(delimiter) {
            return Err(CdfError::contract(
                "delimited terminator cannot equal delimiter",
            ));
        }
        let comment = byte_option(object, "comment", driver.label)?;
        if comment == Some(delimiter) {
            return Err(CdfError::contract(
                "delimited comment character cannot equal delimiter",
            ));
        }
        Ok(Self {
            delimiter,
            header,
            header_validation,
            quote,
            escape,
            terminator,
            comment,
            truncated_rows: bool_option(object, "truncated_rows", false, driver.label)?,
            parallel_decode: parallel_decode_option(object, driver.label)?,
            parallel_unit_bytes: positive_u64_option(
                object,
                "parallel_unit_bytes",
                DEFAULT_PARALLEL_UNIT_BYTES,
                driver.label,
            )?,
            parallel_max_record_bytes: positive_u64_option(
                object,
                "parallel_max_record_bytes",
                DEFAULT_MAXIMUM_RECORD_BYTES,
                driver.label,
            )?,
        })
    }

    fn to_json(&self, driver: &DelimitedFormatDriver) -> Result<serde_json::Value> {
        let mut object = serde_json::Map::new();
        if driver.default_delimiter != Some(self.delimiter) {
            object.insert(
                "delimiter".to_owned(),
                serde_json::Value::String(byte_string(self.delimiter)?),
            );
        }
        if !self.header {
            object.insert("header".to_owned(), serde_json::Value::Bool(false));
        }
        if self.header_validation {
            object.insert(
                "header_validation".to_owned(),
                serde_json::Value::Bool(true),
            );
        }
        if let Some(quote) = self.quote.filter(|quote| *quote != b'"') {
            object.insert(
                "quote".to_owned(),
                serde_json::Value::String(byte_string(quote)?),
            );
        }
        if let Some(escape) = self.escape {
            object.insert(
                "escape".to_owned(),
                serde_json::Value::String(byte_string(escape)?),
            );
        }
        if let Some(terminator) = self.terminator {
            object.insert(
                "terminator".to_owned(),
                serde_json::Value::String(byte_string(terminator)?),
            );
        }
        if let Some(comment) = self.comment {
            object.insert(
                "comment".to_owned(),
                serde_json::Value::String(byte_string(comment)?),
            );
        }
        if self.truncated_rows {
            object.insert("truncated_rows".to_owned(), serde_json::Value::Bool(true));
        }
        match self.parallel_decode {
            ParallelDecode::Auto => {}
            ParallelDecode::Off => {
                object.insert(
                    "parallel_decode".to_owned(),
                    serde_json::Value::String("off".to_owned()),
                );
            }
            ParallelDecode::On => {
                object.insert(
                    "parallel_decode".to_owned(),
                    serde_json::Value::String("on".to_owned()),
                );
            }
        }
        if self.parallel_unit_bytes != DEFAULT_PARALLEL_UNIT_BYTES {
            object.insert(
                "parallel_unit_bytes".to_owned(),
                serde_json::Value::Number(self.parallel_unit_bytes.into()),
            );
        }
        if self.parallel_max_record_bytes != DEFAULT_MAXIMUM_RECORD_BYTES {
            object.insert(
                "parallel_max_record_bytes".to_owned(),
                serde_json::Value::Number(self.parallel_max_record_bytes.into()),
            );
        }
        Ok(serde_json::Value::Object(object))
    }

    fn to_arrow_format(&self) -> Result<Format> {
        self.to_arrow_format_with_header(self.header)
    }

    fn to_arrow_format_with_header(&self, header: bool) -> Result<Format> {
        let mut format = Format::default()
            .with_header(header)
            .with_header_validation(header && self.header_validation)
            .with_delimiter(self.delimiter)
            .with_truncated_rows(self.truncated_rows);
        if let Some(quote) = self.quote {
            format = format.with_quote(quote);
        }
        if let Some(escape) = self.escape {
            format = format.with_escape(escape);
        }
        if let Some(terminator) = self.terminator {
            format = format.with_terminator(terminator);
        }
        if let Some(comment) = self.comment {
            format = format.with_comment(comment);
        }
        Ok(format)
    }
}

fn parallel_decode_option(
    object: &serde_json::Map<String, serde_json::Value>,
    label: &str,
) -> Result<ParallelDecode> {
    match object.get("parallel_decode") {
        None => Ok(ParallelDecode::Auto),
        Some(serde_json::Value::String(value)) if value == "auto" => Ok(ParallelDecode::Auto),
        Some(serde_json::Value::String(value)) if value == "off" => Ok(ParallelDecode::Off),
        Some(serde_json::Value::String(value)) if value == "on" => Ok(ParallelDecode::On),
        Some(_) => Err(CdfError::contract(format!(
            "{label} format option `parallel_decode` must be `auto`, `off`, or `on`"
        ))),
    }
}

fn positive_u64_option(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    default: u64,
    label: &str,
) -> Result<u64> {
    match object.get(key) {
        None => Ok(default),
        Some(value) => value.as_u64().filter(|value| *value != 0).ok_or_else(|| {
            CdfError::contract(format!(
                "{label} format option `{key}` must be a positive integer"
            ))
        }),
    }
}

fn bool_option(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    default: bool,
    label: &str,
) -> Result<bool> {
    match object.get(key) {
        Some(serde_json::Value::Bool(value)) => Ok(*value),
        Some(_) => Err(CdfError::contract(format!(
            "{label} format option `{key}` must be a boolean"
        ))),
        None => Ok(default),
    }
}

fn byte_option(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    label: &str,
) -> Result<Option<u8>> {
    let Some(value) = object.get(key) else {
        return Ok(None);
    };
    let string = value.as_str().ok_or_else(|| {
        CdfError::contract(format!(
            "{label} format option `{key}` must be a one-byte string"
        ))
    })?;
    let bytes = string.as_bytes();
    if bytes.len() == 1 {
        Ok(Some(bytes[0]))
    } else {
        Err(CdfError::contract(format!(
            "{label} format option `{key}` must be exactly one UTF-8 byte"
        )))
    }
}

fn byte_string(byte: u8) -> Result<String> {
    String::from_utf8(vec![byte])
        .map_err(|_| CdfError::contract("delimited option byte must be valid UTF-8"))
}

fn display_byte(byte: u8) -> String {
    match byte {
        b'\t' => "\\t".to_owned(),
        b'\n' => "\\n".to_owned(),
        b'\r' => "\\r".to_owned(),
        byte if byte.is_ascii_graphic() || byte == b' ' => (byte as char).to_string(),
        _ => format!("0x{byte:02x}"),
    }
}

async fn plan_delimited_units(
    source: Arc<dyn ByteSource>,
    options: &DelimitedOptions,
    target_batch_bytes: u64,
    cancellation: cdf_runtime::RunCancellation,
) -> Result<Vec<DecodeUnitPlan>> {
    let size_bytes = source.identity().size_bytes;
    let parallel_source = source.capabilities().known_length
        && source.capabilities().exact_ranges
        && source.supports_local_range_replay();
    let parallel = match options.parallel_decode {
        ParallelDecode::Off => false,
        ParallelDecode::Auto => parallel_source,
        ParallelDecode::On if parallel_source => true,
        ParallelDecode::On => {
            return Err(CdfError::contract(
                "delimited parallel decode requires a known-length exact-range source whose planning pass is replayed from local materialization; use `parallel_decode = \"auto\"` or `parallel_decode = \"off\"` for streaming/remote input",
            ));
        }
    };
    let Some(size_bytes) = size_bytes else {
        return Ok(vec![whole_delimited_unit(None, target_batch_bytes)?]);
    };
    if !parallel || size_bytes <= options.parallel_unit_bytes {
        let extent = (size_bytes != 0)
            .then(|| ByteExtent::new(0, size_bytes))
            .transpose()?;
        return Ok(vec![whole_delimited_unit(extent, target_batch_bytes)?]);
    }

    let preferred_chunk_bytes = PARALLEL_PLANNING_CHUNK_BYTES.clamp(
        source.capabilities().minimum_chunk_bytes,
        source.capabilities().maximum_chunk_bytes,
    );
    let mut input = source
        .open_sequential(SequentialReadRequest {
            preferred_chunk_bytes,
            cancellation: cancellation.clone(),
        })
        .await?;
    let mut planner = DelimitedBoundaryPlanner::new(options, size_bytes)?;
    while let Some(chunk) = input.try_next().await? {
        cancellation.check()?;
        planner.push(chunk.payload())?;
    }
    let boundaries = planner.finish()?;
    let mut starts = Vec::with_capacity(boundaries.len() + 1);
    starts.push(0_u64);
    starts.extend(boundaries);
    let mut units = Vec::with_capacity(starts.len());
    for (ordinal, start) in starts.iter().copied().enumerate() {
        let end = starts.get(ordinal + 1).copied().unwrap_or(size_bytes);
        if end <= start {
            return Err(CdfError::internal(
                "delimited boundary planner produced a non-increasing extent",
            ));
        }
        let extent = ByteExtent::new(start, end - start)?;
        let estimated_working_set_bytes = extent
            .length
            .checked_add(target_batch_bytes.max(1024 * 1024))
            .ok_or_else(|| CdfError::data("delimited unit working set overflowed"))?;
        let unit = DecodeUnitPlan {
            unit_id: format!("delimited-range-{ordinal:08}"),
            ordinal: u32::try_from(ordinal)
                .map_err(|_| CdfError::data("delimited unit ordinal exceeds u32"))?,
            extent: Some(extent),
            estimated_working_set_bytes,
            independently_retryable: true,
        };
        unit.validate()?;
        units.push(unit);
    }
    Ok(units)
}

fn whole_delimited_unit(
    extent: Option<ByteExtent>,
    target_batch_bytes: u64,
) -> Result<DecodeUnitPlan> {
    let unit = DecodeUnitPlan {
        unit_id: "delimited-stream".to_owned(),
        ordinal: 0,
        extent,
        estimated_working_set_bytes: target_batch_bytes.max(1024 * 1024),
        independently_retryable: true,
    };
    unit.validate()?;
    Ok(unit)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BoundaryState {
    StartRecord,
    StartField,
    InField,
    InQuotedField,
    AfterQuote,
    AfterEscape,
    InComment,
    PendingCr,
}

struct DelimitedBoundaryPlanner {
    delimiter: u8,
    quote: u8,
    escape: Option<u8>,
    terminator: Option<u8>,
    comment: Option<u8>,
    unit_bytes: u64,
    maximum_record_bytes: u64,
    size_bytes: u64,
    state: BoundaryState,
    offset: u64,
    record_start: u64,
    next_unit_target: u64,
    boundaries: Vec<u64>,
}

impl DelimitedBoundaryPlanner {
    fn new(options: &DelimitedOptions, size_bytes: u64) -> Result<Self> {
        if options.parallel_unit_bytes == 0 || options.parallel_max_record_bytes == 0 {
            return Err(CdfError::contract(
                "delimited boundary planning requires positive unit and record byte bounds",
            ));
        }
        Ok(Self {
            delimiter: options.delimiter,
            quote: options.quote.unwrap_or(b'"'),
            escape: options.escape,
            terminator: options.terminator,
            comment: options.comment,
            unit_bytes: options.parallel_unit_bytes,
            maximum_record_bytes: options.parallel_max_record_bytes,
            size_bytes,
            state: BoundaryState::StartRecord,
            offset: 0,
            record_start: 0,
            next_unit_target: options.parallel_unit_bytes,
            boundaries: Vec::new(),
        })
    }

    fn push(&mut self, bytes: &[u8]) -> Result<()> {
        for &byte in bytes {
            self.consume(byte)?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<u64>> {
        if self.offset != self.size_bytes {
            return Err(CdfError::data(format!(
                "delimited boundary planning observed {} of {} source bytes",
                self.offset, self.size_bytes
            )));
        }
        if self.state == BoundaryState::PendingCr {
            self.record_boundary(self.offset)?;
        } else if self.record_start != self.offset {
            self.validate_record_length(self.offset)?;
        }
        Ok(self.boundaries)
    }

    fn consume(&mut self, byte: u8) -> Result<()> {
        loop {
            match self.state {
                BoundaryState::PendingCr => {
                    if byte == b'\n' {
                        self.offset = self.offset.checked_add(1).ok_or_else(|| {
                            CdfError::data("delimited boundary offset overflowed")
                        })?;
                        self.record_boundary(self.offset)?;
                        self.state = BoundaryState::StartRecord;
                        return Ok(());
                    }
                    self.record_boundary(self.offset)?;
                    self.state = BoundaryState::StartRecord;
                }
                BoundaryState::StartRecord => {
                    if self.is_terminator(byte) {
                        return self.consume_terminator(byte);
                    }
                    if self.comment == Some(byte) {
                        self.advance()?;
                        self.state = BoundaryState::InComment;
                        return Ok(());
                    }
                    self.state = BoundaryState::StartField;
                }
                BoundaryState::StartField => {
                    if byte == self.quote {
                        self.advance()?;
                        self.state = BoundaryState::InQuotedField;
                        return Ok(());
                    }
                    if byte == self.delimiter {
                        self.advance()?;
                        return Ok(());
                    }
                    if self.is_terminator(byte) {
                        return self.consume_terminator(byte);
                    }
                    self.advance()?;
                    self.state = BoundaryState::InField;
                    return Ok(());
                }
                BoundaryState::InField => {
                    if byte == self.delimiter {
                        self.advance()?;
                        self.state = BoundaryState::StartField;
                    } else if self.is_terminator(byte) {
                        return self.consume_terminator(byte);
                    } else {
                        self.advance()?;
                    }
                    return Ok(());
                }
                BoundaryState::InQuotedField => {
                    if byte == self.quote {
                        self.advance()?;
                        self.state = BoundaryState::AfterQuote;
                    } else if self.escape == Some(byte) {
                        self.advance()?;
                        self.state = BoundaryState::AfterEscape;
                    } else {
                        self.advance()?;
                    }
                    return Ok(());
                }
                BoundaryState::AfterQuote => {
                    if byte == self.quote {
                        self.advance()?;
                        self.state = BoundaryState::InQuotedField;
                    } else if byte == self.delimiter {
                        self.advance()?;
                        self.state = BoundaryState::StartField;
                    } else if self.is_terminator(byte) {
                        return self.consume_terminator(byte);
                    } else {
                        self.advance()?;
                        self.state = BoundaryState::InField;
                    }
                    return Ok(());
                }
                BoundaryState::AfterEscape => {
                    self.advance()?;
                    self.state = BoundaryState::InQuotedField;
                    return Ok(());
                }
                BoundaryState::InComment => {
                    self.advance()?;
                    if byte == b'\n' {
                        self.record_boundary(self.offset)?;
                        self.state = BoundaryState::StartRecord;
                    }
                    return Ok(());
                }
            }
        }
    }

    fn is_terminator(&self, byte: u8) -> bool {
        self.terminator
            .map_or(matches!(byte, b'\r' | b'\n'), |value| byte == value)
    }

    fn consume_terminator(&mut self, byte: u8) -> Result<()> {
        self.advance()?;
        if self.terminator.is_none() && byte == b'\r' {
            self.state = BoundaryState::PendingCr;
        } else {
            self.record_boundary(self.offset)?;
            self.state = BoundaryState::StartRecord;
        }
        Ok(())
    }

    fn advance(&mut self) -> Result<()> {
        self.offset = self
            .offset
            .checked_add(1)
            .ok_or_else(|| CdfError::data("delimited boundary offset overflowed"))?;
        if self.offset > self.size_bytes {
            return Err(CdfError::data(
                "delimited boundary planning exceeded the source generation",
            ));
        }
        Ok(())
    }

    fn record_boundary(&mut self, position: u64) -> Result<()> {
        self.validate_record_length(position)?;
        self.record_start = position;
        if position >= self.next_unit_target && position < self.size_bytes {
            self.boundaries.push(position);
            self.next_unit_target = position
                .checked_add(self.unit_bytes)
                .ok_or_else(|| CdfError::data("delimited unit target overflowed"))?;
        }
        Ok(())
    }

    fn validate_record_length(&self, end: u64) -> Result<()> {
        let length = end.checked_sub(self.record_start).ok_or_else(|| {
            CdfError::internal("delimited record boundary moved before its start")
        })?;
        if length > self.maximum_record_bytes {
            return Err(CdfError::data(format!(
                "delimited record starting at byte {} is at least {length} bytes, exceeding format_options.parallel_max_record_bytes = {}; increase the explicit parallel-planning bound or repair the malformed/unclosed record",
                self.record_start, self.maximum_record_bytes
            )));
        }
        Ok(())
    }
}

struct DelimitedDecodeSession {
    source: Arc<dyn ByteSource>,
    units: Vec<DecodeUnitPlan>,
    options: DelimitedOptions,
}

impl FormatDecodeSession for DelimitedDecodeSession {
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
            if request.projection.is_some() || !request.predicates.is_empty() {
                return Err(CdfError::contract(
                    "delimited projection and predicate pushdown are unsupported",
                ));
            }
            let parallel_units = self.units.len() > 1;
            let input = if parallel_units {
                let extent = request.unit.extent.ok_or_else(|| {
                    CdfError::internal("parallel delimited unit omitted its exact byte extent")
                })?;
                let bytes = self
                    .source
                    .read_exact_range(extent, request.cancellation.clone())
                    .await?;
                Box::pin(stream::once(async move { Ok(bytes) })) as AccountedByteStream
            } else {
                self.source
                    .open_sequential(SequentialReadRequest {
                        preferred_chunk_bytes: request
                            .target_batch_bytes
                            .clamp(64 * 1024, 4 * 1024 * 1024),
                        cancellation: request.cancellation.clone(),
                    })
                    .await?
            };
            let format = self
                .options
                .to_arrow_format_with_header(self.options.header && request.unit.ordinal == 0)?;
            let decoder = ReaderBuilder::new(Arc::clone(&request.schema.decoder_schema))
                .with_format(format)
                .with_batch_size(request.target_batch_rows)
                .build_decoder();
            let output_lease = reserve_output(&request).await?;
            let state = DecodeState {
                input,
                current: None,
                offset: 0,
                decoder,
                request,
                output_lease: Some(output_lease),
                sequence: 0,
                input_finished: false,
                terminal: false,
            };
            Ok(Box::pin(stream::try_unfold(state, decode_next)) as PhysicalDecodeStream)
        })
    }
}

struct DecodeState {
    input: AccountedByteStream,
    current: Option<cdf_memory::AccountedBytes>,
    offset: usize,
    decoder: Decoder,
    request: PhysicalDecodeRequest,
    output_lease: Option<MemoryLease>,
    sequence: u64,
    input_finished: bool,
    terminal: bool,
}

async fn decode_next(
    mut state: DecodeState,
) -> Result<Option<(AccountedPhysicalBatch, DecodeState)>> {
    loop {
        state.request.cancellation.check()?;
        if state.terminal {
            return Ok(None);
        }
        if !state.input_finished
            && state
                .current
                .as_ref()
                .is_none_or(|chunk| state.offset == chunk.payload().len())
        {
            state.current = state.input.try_next().await?;
            state.offset = 0;
            state.input_finished = state.current.is_none();
        }
        let consumed = if let Some(chunk) = &state.current {
            state
                .decoder
                .decode(&chunk.payload()[state.offset..])
                .map_err(|error| CdfError::data(format!("decode CSV: {error}")))?
        } else {
            state
                .decoder
                .decode(&[])
                .map_err(|error| CdfError::data(format!("finish CSV decode: {error}")))?
        };
        state.offset += consumed;
        if consumed != 0 {
            continue;
        }
        let record_batch = state
            .decoder
            .flush()
            .map_err(|error| CdfError::data(format!("flush CSV batch: {error}")))?;
        let record_batch = match record_batch {
            Some(batch) => batch,
            None => {
                state.terminal = state.input_finished;
                if state.terminal && state.sequence == 0 {
                    RecordBatch::new_empty(Arc::clone(&state.request.schema.decoder_schema))
                } else if state.terminal {
                    return Ok(None);
                } else {
                    continue;
                }
            }
        };
        let lease = state
            .output_lease
            .take()
            .ok_or_else(|| CdfError::internal("CSV output lease missing"))?;
        let batch_id = BatchId::new(format!(
            "{}-u{:08}-b{:08}",
            state.request.batch_id_prefix, state.request.unit.ordinal, state.sequence
        ))?;
        state.sequence = state
            .sequence
            .checked_add(1)
            .ok_or_else(|| CdfError::data("CSV batch sequence overflowed"))?;
        let mut batch = Batch::from_record_batch(
            batch_id,
            state.request.resource_id.clone(),
            state.request.partition_id.clone(),
            cdf_kernel::canonical_arrow_schema_hash(state.request.schema.decoder_schema.as_ref())?,
            record_batch,
        )?;
        batch.header.source_position = state.request.source_position.clone();
        let physical = AccountedPhysicalBatch::new(batch, lease)?;
        if !state.terminal {
            state.output_lease = Some(reserve_output(&state.request).await?);
        }
        return Ok(Some((physical, state)));
    }
}

async fn reserve_output(request: &PhysicalDecodeRequest) -> Result<MemoryLease> {
    reserve(
        Arc::clone(&request.memory),
        ReservationRequest::new(
            ConsumerKey::new("csv-output", MemoryClass::Decode)?,
            request.target_batch_bytes.max(1024 * 1024),
        )?
        .as_minimum_working_set(),
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use bytes::Bytes;
    use cdf_kernel::{PartitionId, PhysicalObservationRepresentation, ResourceId};
    use cdf_memory::{
        AccountedBytes, ConsumerKey, DeterministicMemoryCoordinator, MemoryClass,
        MemoryCoordinator, ReservationRequest, reserve_blocking,
    };
    use cdf_runtime::{
        AccountedByteStream, DecodePlanningRequest, DecodeSchemaPlan, MemoryByteSource,
        RunCancellation,
    };
    use futures_util::{TryStreamExt, stream};

    use super::*;

    fn accounted(memory: Arc<dyn MemoryCoordinator>, payload: &'static [u8]) -> AccountedBytes {
        let lease = reserve_blocking(
            memory,
            &ReservationRequest::new(
                ConsumerKey::new("delimited-test-input", MemoryClass::Source).unwrap(),
                payload.len() as u64,
            )
            .unwrap(),
        )
        .unwrap();
        AccountedBytes::new(Bytes::from_static(payload), lease).unwrap()
    }

    #[test]
    fn empty_csv_emits_schema_bearing_physical_batch() {
        let memory: Arc<dyn MemoryCoordinator> = Arc::new(
            DeterministicMemoryCoordinator::new(64 * 1024 * 1024, BTreeMap::new()).unwrap(),
        );
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let request = PhysicalDecodeRequest {
            unit: DecodeUnitPlan {
                unit_id: "empty-csv".to_owned(),
                ordinal: 0,
                extent: None,
                estimated_working_set_bytes: 1024 * 1024,
                independently_retryable: true,
            },
            resource_id: ResourceId::new("events.empty").unwrap(),
            partition_id: PartitionId::new("file-empty").unwrap(),
            batch_id_prefix: "events-empty".to_owned(),
            schema: DecodeSchemaPlan::fixed_admission(schema),
            source_position: None,
            projection: None,
            predicates: Vec::new(),
            target_batch_rows: 64,
            target_batch_bytes: 1024 * 1024,
            memory,
            cancellation: RunCancellation::default(),
        };
        let batch = futures_executor::block_on(async move {
            let input: AccountedByteStream = Box::pin(stream::empty());
            let decoder = ReaderBuilder::new(Arc::clone(&request.schema.decoder_schema))
                .with_format(CsvFormatDriver::new()?.arrow_format(serde_json::json!({}))?)
                .with_batch_size(request.target_batch_rows)
                .build_decoder();
            let output_lease = reserve_output(&request).await?;
            let state = DecodeState {
                input,
                current: None,
                offset: 0,
                decoder,
                request,
                output_lease: Some(output_lease),
                sequence: 0,
                input_finished: false,
                terminal: false,
            };
            let (batch, state) = decode_next(state)
                .await?
                .ok_or_else(|| CdfError::internal("empty CSV omitted schema-bearing batch"))?;
            if decode_next(state).await?.is_some() {
                return Err(CdfError::internal("empty CSV emitted multiple batches"));
            }
            Result::<AccountedPhysicalBatch>::Ok(batch)
        })
        .unwrap();

        assert_eq!(batch.batch().record_batch().unwrap().num_rows(), 0);
        assert_eq!(
            batch.batch().header.observation_representation,
            PhysicalObservationRepresentation::ArrowSchema
        );
    }

    #[test]
    fn tsv_decode_streams_chunked_delimiter_without_runtime_guessing() {
        let memory: Arc<dyn MemoryCoordinator> = Arc::new(
            DeterministicMemoryCoordinator::new(64 * 1024 * 1024, BTreeMap::new()).unwrap(),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let request = PhysicalDecodeRequest {
            unit: DecodeUnitPlan {
                unit_id: "tsv-stream".to_owned(),
                ordinal: 0,
                extent: None,
                estimated_working_set_bytes: 1024 * 1024,
                independently_retryable: true,
            },
            resource_id: ResourceId::new("events.tsv").unwrap(),
            partition_id: PartitionId::new("file-tsv").unwrap(),
            batch_id_prefix: "events-tsv".to_owned(),
            schema: DecodeSchemaPlan::fixed_admission(schema),
            source_position: None,
            projection: None,
            predicates: Vec::new(),
            target_batch_rows: 64,
            target_batch_bytes: 1024 * 1024,
            memory: Arc::clone(&memory),
            cancellation: RunCancellation::default(),
        };

        let batches = futures_executor::block_on(async move {
            let input: AccountedByteStream = Box::pin(stream::iter([
                Ok(accounted(Arc::clone(&memory), b"id\tna")),
                Ok(accounted(Arc::clone(&memory), b"me\n1\talpha\n2\tbeta\n")),
            ]));
            let decoder = ReaderBuilder::new(Arc::clone(&request.schema.decoder_schema))
                .with_format(DelimitedFormatDriver::tsv()?.arrow_format(serde_json::json!({}))?)
                .with_batch_size(request.target_batch_rows)
                .build_decoder();
            let output_lease = reserve_output(&request).await?;
            let mut state = DecodeState {
                input,
                current: None,
                offset: 0,
                decoder,
                request,
                output_lease: Some(output_lease),
                sequence: 0,
                input_finished: false,
                terminal: false,
            };
            let mut batches = Vec::new();
            while let Some((batch, next)) = decode_next(state).await? {
                batches.push(
                    batch
                        .into_batch()?
                        .record_batch()
                        .ok_or_else(|| CdfError::internal("delimited decode omitted payload"))?
                        .clone(),
                );
                state = next;
            }
            Result::<Vec<RecordBatch>>::Ok(batches)
        })
        .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[test]
    fn delimited_options_are_canonical_and_custom_requires_delimiter() {
        let csv = CsvFormatDriver::new().unwrap();
        assert_eq!(
            csv.canonical_options(serde_json::json!({"delimiter": ",", "header": true}))
                .unwrap(),
            serde_json::json!({})
        );
        let custom = DelimitedFormatDriver::custom().unwrap();
        let error = custom.canonical_options(serde_json::json!({})).unwrap_err();
        assert!(
            error
                .message
                .contains("requires `format_options.delimiter`")
        );
        assert_eq!(
            custom
                .canonical_options(serde_json::json!({
                    "delimiter": "|",
                    "header": false,
                    "truncated_rows": true
                }))
                .unwrap(),
            serde_json::json!({
                "delimiter": "|",
                "header": false,
                "truncated_rows": true
            })
        );
    }

    #[test]
    fn quote_aware_boundaries_never_split_embedded_newlines_or_crlf() {
        let driver = CsvFormatDriver::new().unwrap();
        let options = DelimitedOptions::from_json(
            &driver,
            serde_json::json!({
                "parallel_unit_bytes": 10,
                "parallel_max_record_bytes": 100
            }),
        )
        .unwrap();
        let payload = b"id,text\r\n1,\"alpha\nbeta\"\r\n2,gamma\r\n";
        let mut planner = DelimitedBoundaryPlanner::new(&options, payload.len() as u64).unwrap();
        for chunk in payload.chunks(3) {
            planner.push(chunk).unwrap();
        }
        assert_eq!(planner.finish().unwrap(), vec![25]);
    }

    #[test]
    fn local_parallel_units_match_sequential_rows_with_quoted_newline() {
        let memory: Arc<dyn MemoryCoordinator> = Arc::new(
            DeterministicMemoryCoordinator::new(64 * 1024 * 1024, BTreeMap::new()).unwrap(),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("text", DataType::Utf8, false),
        ]));
        let payload = b"id,text\r\n1,\"alpha\nbeta\"\r\n2,gamma\r\n".to_vec();

        let decode = |parallel_decode: &str| {
            let memory = Arc::clone(&memory);
            let schema = Arc::clone(&schema);
            let payload = payload.clone();
            let parallel_decode = parallel_decode.to_owned();
            futures_executor::block_on(async move {
                let source: Arc<dyn ByteSource> = Arc::new(
                    MemoryByteSource::from_bytes("quoted-csv", payload, Arc::clone(&memory))
                        .await?,
                );
                let driver = CsvFormatDriver::new()?;
                let session = driver
                    .prepare_decode(
                        source,
                        DecodePlanningRequest {
                            options: serde_json::json!({
                                "parallel_decode": parallel_decode,
                                "parallel_unit_bytes": 10,
                                "parallel_max_record_bytes": 100
                            }),
                            projection: None,
                            predicates: Vec::new(),
                            target_batch_rows: 64,
                            target_batch_bytes: 1024 * 1024,
                            cancellation: RunCancellation::default(),
                        },
                    )
                    .await?;
                let mut ids = Vec::new();
                for unit in session.units() {
                    let batches = session
                        .decode(PhysicalDecodeRequest {
                            unit: unit.clone(),
                            resource_id: ResourceId::new("events.quoted")?,
                            partition_id: PartitionId::new("file-quoted")?,
                            batch_id_prefix: "events-quoted".to_owned(),
                            schema: DecodeSchemaPlan::fixed_admission(Arc::clone(&schema)),
                            source_position: None,
                            projection: None,
                            predicates: Vec::new(),
                            target_batch_rows: 64,
                            target_batch_bytes: 1024 * 1024,
                            memory: Arc::clone(&memory),
                            cancellation: RunCancellation::default(),
                        })
                        .await?
                        .try_collect::<Vec<_>>()
                        .await?;
                    for batch in batches {
                        let record_batch = batch.batch().record_batch().ok_or_else(|| {
                            CdfError::internal("CSV physical batch omitted its Arrow payload")
                        })?;
                        let array = record_batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .ok_or_else(|| CdfError::internal("CSV id column is not int64"))?;
                        ids.extend((0..array.len()).map(|index| array.value(index)));
                    }
                }
                Result::<(usize, Vec<i64>)>::Ok((session.units().len(), ids))
            })
            .unwrap()
        };

        let sequential = decode("off");
        let parallel = decode("on");
        assert_eq!(sequential, (1, vec![1, 2]));
        assert_eq!(parallel, (2, vec![1, 2]));
        assert_eq!(memory.snapshot().current_bytes, 0);
    }

    #[test]
    fn parallel_planner_rejects_records_above_the_explicit_bound() {
        let driver = CsvFormatDriver::new().unwrap();
        let options = DelimitedOptions::from_json(
            &driver,
            serde_json::json!({
                "parallel_unit_bytes": 2,
                "parallel_max_record_bytes": 4
            }),
        )
        .unwrap();
        let mut planner = DelimitedBoundaryPlanner::new(&options, 6).unwrap();
        planner.push(b"abcdef").unwrap();
        let error = planner.finish().unwrap_err();
        assert!(
            error.message.contains("parallel_max_record_bytes"),
            "{error}"
        );
    }
}
