#![doc = "Descriptor-bound, length-delimited Protobuf format driver for cdf."]

mod schema;
mod wire;

use std::{collections::BTreeMap, mem::size_of, sync::Arc};

use arrow_array::{
    RecordBatch,
    builder::{
        ArrayBuilder, BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder,
        Int64Builder, ListBuilder, MapBuilder, StringBuilder, StructBuilder, UInt32Builder,
        UInt64Builder, make_builder,
    },
};
use arrow_schema::{DataType, Field};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use cdf_kernel::{
    Batch, BatchId, BoxFuture, CdfError, PreContractResidualCandidate, PushdownFidelity, Result,
};
use cdf_memory::{
    ConsumerKey, MemoryClass, MemoryLease, ReservationRequest, record_batch_retained_bytes, reserve,
};
use cdf_runtime::{
    AccountedByteCursor, AccountedPhysicalBatch, ByteExtent, ByteSource, DecodePlanningRequest,
    DecodeSchemaAuthority, DecodeUnitPlan, FormatDecodeSession, FormatDetection,
    FormatDetectionConfidence, FormatDetectionProbe, FormatDiscoveryCapabilities,
    FormatDiscoveryKind, FormatDiscoveryRequest, FormatDriver, FormatDriverDescriptor,
    FormatErrorIsolation, FormatId, FormatProbe, FormatSourceAccess, PhysicalDecodeRequest,
    PhysicalDecodeStream, PhysicalSchemaObservation, SequentialReadRequest,
};
use futures_util::stream;
use prost_reflect::{Cardinality, DescriptorPool, FieldDescriptor, Kind, Value};
use serde::{Deserialize, Serialize};

use crate::{
    schema::{FieldPlan, MessagePlan, ScalarPlan, ValuePlan},
    wire::{
        MessageView, WIRE_FIXED32, WIRE_FIXED64, WIRE_LENGTH_DELIMITED, WIRE_START_GROUP,
        WIRE_VARINT, WireOccurrence, decode_varint,
    },
};

const DEFAULT_MAXIMUM_DESCRIPTOR_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_MAXIMUM_MESSAGE_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_MAXIMUM_OUTPUT_BATCH_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_MAXIMUM_NESTING_DEPTH: u32 = 100;
const MAXIMUM_DESCRIPTOR_BYTES: u64 = 1024 * 1024 * 1024;
const MAXIMUM_MESSAGE_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const MAXIMUM_OUTPUT_BATCH_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const MAXIMUM_NESTING_DEPTH: u32 = 4096;
const MAXIMUM_LENGTH_PREFIX_BYTES: usize = 10;
// Wire values contribute at most one Arrow value plus offsets/validity; per-cell overhead below
// separately accounts sparse/default-heavy messages. Four keeps the lease conservative without
// rejecting the runtime's legal 32 MiB adaptive batches under the default output authority.
const OUTPUT_ESTIMATE_MULTIPLIER: u64 = 4;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Framing {
    LengthDelimited,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProtobufOptions {
    descriptor_set_base64: String,
    message: String,
    framing: Framing,
    #[serde(default = "default_maximum_descriptor_bytes")]
    maximum_descriptor_bytes: u64,
    #[serde(default = "default_maximum_message_bytes")]
    maximum_message_bytes: u64,
    #[serde(default = "default_maximum_output_batch_bytes")]
    maximum_output_batch_bytes: u64,
    #[serde(default = "default_maximum_nesting_depth")]
    maximum_nesting_depth: u32,
}

const fn default_maximum_descriptor_bytes() -> u64 {
    DEFAULT_MAXIMUM_DESCRIPTOR_BYTES
}

const fn default_maximum_message_bytes() -> u64 {
    DEFAULT_MAXIMUM_MESSAGE_BYTES
}

const fn default_maximum_output_batch_bytes() -> u64 {
    DEFAULT_MAXIMUM_OUTPUT_BATCH_BYTES
}

const fn default_maximum_nesting_depth() -> u32 {
    DEFAULT_MAXIMUM_NESTING_DEPTH
}

impl ProtobufOptions {
    fn parse(value: serde_json::Value) -> Result<(Self, MessagePlan)> {
        let options: Self = serde_json::from_value(value)
            .map_err(|error| CdfError::contract(format!("invalid Protobuf options: {error}")))?;
        if options.descriptor_set_base64.is_empty() {
            return Err(CdfError::contract(
                "Protobuf format_options.descriptor_set_base64 is required",
            ));
        }
        if options.message.trim().is_empty() || options.message.starts_with('.') {
            return Err(CdfError::contract(
                "Protobuf format_options.message requires a fully qualified message name without a leading dot",
            ));
        }
        if !(1..=MAXIMUM_DESCRIPTOR_BYTES).contains(&options.maximum_descriptor_bytes)
            || !(1..=MAXIMUM_MESSAGE_BYTES).contains(&options.maximum_message_bytes)
            || !(1..=MAXIMUM_OUTPUT_BATCH_BYTES).contains(&options.maximum_output_batch_bytes)
            || !(1..=MAXIMUM_NESTING_DEPTH).contains(&options.maximum_nesting_depth)
        {
            return Err(CdfError::contract(format!(
                "Protobuf maximum_descriptor_bytes, maximum_message_bytes, maximum_output_batch_bytes, and maximum_nesting_depth must be nonzero and no greater than their physical limits ({MAXIMUM_DESCRIPTOR_BYTES}, {MAXIMUM_MESSAGE_BYTES}, {MAXIMUM_OUTPUT_BATCH_BYTES}, {MAXIMUM_NESTING_DEPTH})"
            )));
        }
        if options.maximum_output_batch_bytes < options.maximum_message_bytes {
            return Err(CdfError::contract(
                "Protobuf maximum_output_batch_bytes must be at least maximum_message_bytes so one admitted message can be materialized atomically",
            ));
        }
        let plan = options.message_plan()?;
        Ok((options, plan))
    }

    fn canonical(self) -> Result<serde_json::Value> {
        serde_json::to_value(self)
            .map_err(|error| CdfError::internal(format!("encode Protobuf options: {error}")))
    }

    fn descriptor_bytes(&self) -> Result<Vec<u8>> {
        let maximum_encoded = self
            .maximum_descriptor_bytes
            .checked_mul(4)
            .and_then(|bytes| bytes.checked_div(3))
            .and_then(|bytes| bytes.checked_add(8))
            .ok_or_else(|| CdfError::contract("Protobuf descriptor authority overflowed"))?;
        let encoded_length = u64::try_from(self.descriptor_set_base64.len())
            .map_err(|_| CdfError::contract("Protobuf descriptor text length exceeds u64"))?;
        if encoded_length > maximum_encoded {
            return Err(CdfError::contract(format!(
                "Protobuf descriptor_set_base64 exceeds the configured {}-byte decoded descriptor authority",
                self.maximum_descriptor_bytes
            )));
        }
        let bytes = BASE64_STANDARD
            .decode(&self.descriptor_set_base64)
            .map_err(|error| {
                CdfError::contract(format!("decode Protobuf descriptor set: {error}"))
            })?;
        let length = u64::try_from(bytes.len())
            .map_err(|_| CdfError::contract("Protobuf descriptor set length exceeds u64"))?;
        if length == 0 || length > self.maximum_descriptor_bytes {
            return Err(CdfError::contract(format!(
                "Protobuf descriptor set contains {length} bytes outside the configured 1..={} byte authority",
                self.maximum_descriptor_bytes
            )));
        }
        Ok(bytes)
    }

    fn message_plan(&self) -> Result<MessagePlan> {
        let bytes = self.descriptor_bytes()?;
        let pool = DescriptorPool::decode(bytes.as_slice()).map_err(|error| {
            CdfError::contract(format!("decode Protobuf FileDescriptorSet: {error}"))
        })?;
        let descriptor = pool.get_message_by_name(&self.message).ok_or_else(|| {
            CdfError::contract(format!(
                "Protobuf descriptor set does not define message `{}`",
                self.message
            ))
        })?;
        MessagePlan::compile(descriptor)
    }
}

#[derive(Clone, Debug)]
pub struct ProtobufFormatDriver {
    descriptor: FormatDriverDescriptor,
}

impl ProtobufFormatDriver {
    pub fn new() -> Result<Self> {
        Ok(Self {
            descriptor: FormatDriverDescriptor {
                format_id: FormatId::new("protobuf")?,
                semantic_version: "1.0.0".to_owned(),
                aliases: vec!["proto".to_owned()],
                extensions: vec!["pb".to_owned(), "protobuf".to_owned()],
                mime_types: vec![
                    "application/x-protobuf".to_owned(),
                    "application/protobuf".to_owned(),
                ],
                magic: Vec::new(),
                detection_probe: FormatDetectionProbe {
                    prefix_bytes: 0,
                    suffix_bytes: 0,
                },
                option_schema: serde_json::json!({
                    "type": "object",
                    "required": ["descriptor_set_base64", "message", "framing"],
                    "properties": {
                        "descriptor_set_base64": { "type": "string", "minLength": 1 },
                        "message": { "type": "string", "minLength": 1 },
                        "framing": { "const": "length_delimited" },
                        "maximum_descriptor_bytes": { "type": "integer", "minimum": 1, "maximum": MAXIMUM_DESCRIPTOR_BYTES, "default": DEFAULT_MAXIMUM_DESCRIPTOR_BYTES },
                        "maximum_message_bytes": { "type": "integer", "minimum": 1, "maximum": MAXIMUM_MESSAGE_BYTES, "default": DEFAULT_MAXIMUM_MESSAGE_BYTES },
                        "maximum_output_batch_bytes": { "type": "integer", "minimum": 1, "maximum": MAXIMUM_OUTPUT_BATCH_BYTES, "default": DEFAULT_MAXIMUM_OUTPUT_BATCH_BYTES },
                        "maximum_nesting_depth": { "type": "integer", "minimum": 1, "maximum": MAXIMUM_NESTING_DEPTH, "default": DEFAULT_MAXIMUM_NESTING_DEPTH }
                    },
                    "additionalProperties": false
                }),
                projection_pushdown: PushdownFidelity::Exact,
                predicate_pushdown: PushdownFidelity::Unsupported,
                predicate_operators: Vec::new(),
                source_access: FormatSourceAccess::Sequential,
                discovery: FormatDiscoveryCapabilities::only(FormatDiscoveryKind::FormatMetadata),
                decode_unit_policy: "length_delimited_stream_v1".to_owned(),
                error_isolation: FormatErrorIsolation::DecodeUnit,
                decode_cpu: cdf_runtime::CpuTaskSpec {
                    task_kind: "format.protobuf.decode".to_owned(),
                    cpu_slot_cost: 1,
                    native_internal_parallelism: 1,
                },
                minimum_working_set_bytes: 64 * 1024,
                maximum_working_set_bytes: MAXIMUM_OUTPUT_BATCH_BYTES,
            },
        })
    }
}

impl FormatDriver for ProtobufFormatDriver {
    fn descriptor(&self) -> &FormatDriverDescriptor {
        &self.descriptor
    }

    fn canonical_options(&self, options: serde_json::Value) -> Result<serde_json::Value> {
        ProtobufOptions::parse(options)?.0.canonical()
    }

    fn detect(&self, probe: &FormatProbe) -> Result<FormatDetection> {
        let matched = probe
            .extension
            .as_deref()
            .is_some_and(|extension| matches!(extension, "pb" | "protobuf"))
            || probe.mime_type.as_deref().is_some_and(|mime| {
                matches!(mime, "application/x-protobuf" | "application/protobuf")
            });
        Ok(FormatDetection {
            confidence: if matched {
                FormatDetectionConfidence::Weak
            } else {
                FormatDetectionConfidence::None
            },
            reason: if matched {
                "Protobuf extension or MIME matched; explicit descriptor and framing remain required"
            } else {
                "Protobuf has no self-identifying magic"
            }
            .to_owned(),
        })
    }

    fn discover(
        &self,
        source: Arc<dyn ByteSource>,
        request: FormatDiscoveryRequest,
    ) -> BoxFuture<'_, Result<PhysicalSchemaObservation>> {
        Box::pin(async move {
            request.cancellation.check()?;
            if request.discovery_kind != FormatDiscoveryKind::FormatMetadata {
                return Err(CdfError::contract(
                    "Protobuf discovery uses its explicit descriptor metadata and does not sample payload bytes",
                ));
            }
            let (options, plan) = ProtobufOptions::parse(request.options)?;
            let mut evidence = BTreeMap::new();
            evidence.insert("protobuf.message".to_owned(), options.message);
            evidence.insert("protobuf.framing".to_owned(), "length_delimited".to_owned());
            Ok(PhysicalSchemaObservation {
                identity: source.identity().clone(),
                arrow_schema: plan.arrow_schema,
                sampled_bytes: 0,
                sampled_records: 0,
                evidence,
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
            if !request.predicates.is_empty() {
                return Err(CdfError::contract(
                    "Protobuf predicate pushdown is unsupported",
                ));
            }
            if request.target_batch_rows == 0 || request.target_batch_bytes == 0 {
                return Err(CdfError::contract(
                    "Protobuf decode requires nonzero target batch rows and bytes",
                ));
            }
            let (options, complete_plan) = ProtobufOptions::parse(request.options)?;
            let projected_plan = complete_plan.projected(request.projection.as_deref())?;
            let extent = source
                .identity()
                .size_bytes
                .map(|size| ByteExtent::new(0, size))
                .transpose()?;
            let unit = DecodeUnitPlan {
                unit_id: "length-delimited-stream".to_owned(),
                ordinal: 0,
                extent,
                estimated_working_set_bytes: request
                    .target_batch_bytes
                    .max(options.maximum_message_bytes)
                    .min(options.maximum_output_batch_bytes),
                independently_retryable: source.capabilities().reopenable,
            };
            unit.validate()?;
            Ok(Arc::new(ProtobufDecodeSession {
                source,
                options,
                complete_plan,
                projected_plan,
                units: vec![unit],
            }) as Arc<dyn FormatDecodeSession>)
        })
    }
}

struct ProtobufDecodeSession {
    source: Arc<dyn ByteSource>,
    options: ProtobufOptions,
    complete_plan: MessagePlan,
    projected_plan: MessagePlan,
    units: Vec<DecodeUnitPlan>,
}

impl FormatDecodeSession for ProtobufDecodeSession {
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
            if !request.predicates.is_empty() {
                return Err(CdfError::contract(
                    "Protobuf predicate pushdown is unsupported",
                ));
            }
            let requested_projection = self
                .complete_plan
                .projected(request.projection.as_deref())?;
            if requested_projection.arrow_schema != self.projected_plan.arrow_schema {
                return Err(CdfError::contract(
                    "Protobuf decode projection differs from its prepared session",
                ));
            }
            let observed_hash =
                cdf_kernel::canonical_arrow_schema_hash(self.projected_plan.arrow_schema.as_ref())?;
            if request.schema.authority == DecodeSchemaAuthority::VerifiedPhysicalObservation {
                let expected = cdf_kernel::canonical_arrow_schema_hash(
                    request.schema.authority_schema.as_ref(),
                )?;
                if expected != observed_hash {
                    return Err(CdfError::data(format!(
                        "Protobuf descriptor schema changed before decode: planned {expected}, observed {observed_hash}"
                    )));
                }
            }
            let input = self
                .source
                .open_sequential(SequentialReadRequest {
                    preferred_chunk_bytes: request
                        .target_batch_bytes
                        .min(self.options.maximum_message_bytes)
                        .max(1),
                    cancellation: request.cancellation.clone(),
                })
                .await?;
            let state = DecodeState {
                cursor: AccountedByteCursor::new(input),
                request,
                options: self.options.clone(),
                complete_plan: self.complete_plan.clone(),
                projected_plan: self.projected_plan.clone(),
                observed_hash,
                sequence: 0,
                source_row_ordinal: 0,
                finished: false,
            };
            Ok(Box::pin(stream::try_unfold(state, decode_next)) as PhysicalDecodeStream)
        })
    }
}

struct BufferedMessage {
    bytes: Vec<u8>,
    _lease: MemoryLease,
}

struct DecodeState {
    cursor: AccountedByteCursor,
    request: PhysicalDecodeRequest,
    options: ProtobufOptions,
    complete_plan: MessagePlan,
    projected_plan: MessagePlan,
    observed_hash: cdf_kernel::SchemaHash,
    sequence: u64,
    source_row_ordinal: u64,
    finished: bool,
}

async fn decode_next(
    mut state: DecodeState,
) -> Result<Option<(AccountedPhysicalBatch, DecodeState)>> {
    if state.finished {
        return Ok(None);
    }
    let mut messages = Vec::new();
    let mut encoded_bytes = 0_u64;
    while messages.len() < state.request.target_batch_rows
        && (messages.is_empty() || encoded_bytes < state.request.target_batch_bytes)
    {
        state.request.cancellation.check()?;
        let Some(length) = read_length_prefix(&mut state.cursor).await? else {
            state.finished = true;
            break;
        };
        if length > state.options.maximum_message_bytes {
            return Err(CdfError::data(format!(
                "Protobuf message {} declares {length} bytes above the configured {}-byte maximum; increase format_options.maximum_message_bytes only for a trusted producer",
                state.source_row_ordinal, state.options.maximum_message_bytes
            )));
        }
        let length_usize = usize::try_from(length)
            .map_err(|_| CdfError::data("Protobuf message length exceeds usize"))?;
        let accounted = length
            .saturating_add(u64::try_from(size_of::<BufferedMessage>()).unwrap_or(64))
            .max(1);
        let lease = reserve(
            Arc::clone(&state.request.memory),
            ReservationRequest::new(
                ConsumerKey::new("protobuf-framed-message", MemoryClass::Decode)?,
                accounted,
            )?,
        )
        .await?;
        let bytes = if length_usize == 0 {
            Vec::new()
        } else {
            state
                .cursor
                .read_exact(length_usize, "Protobuf framed message")
                .await?
        };
        encoded_bytes = encoded_bytes
            .checked_add(length)
            .ok_or_else(|| CdfError::data("Protobuf batch encoded-byte count overflowed"))?;
        messages.push(BufferedMessage {
            bytes,
            _lease: lease,
        });
        state.source_row_ordinal = state
            .source_row_ordinal
            .checked_add(1)
            .ok_or_else(|| CdfError::data("Protobuf source row ordinal overflowed"))?;
    }
    if messages.is_empty() {
        return Ok(None);
    }
    let batch_start_ordinal = state
        .source_row_ordinal
        .checked_sub(
            u64::try_from(messages.len())
                .map_err(|_| CdfError::data("Protobuf batch row count exceeds u64"))?,
        )
        .ok_or_else(|| CdfError::internal("Protobuf batch ordinal underflowed"))?;
    let output_authority = output_authority_bytes(
        encoded_bytes,
        messages.len(),
        state.projected_plan.fields.len(),
        state.request.target_batch_bytes,
        state.options.maximum_output_batch_bytes,
    )?;
    let output_lease = reserve(
        Arc::clone(&state.request.memory),
        ReservationRequest::new(
            ConsumerKey::new("protobuf-arrow-output", MemoryClass::Decode)?,
            output_authority,
        )?,
    )
    .await?;
    let (record_batch, unknowns) = build_record_batch(
        &state.complete_plan,
        &state.projected_plan,
        &messages,
        batch_start_ordinal,
        state.options.maximum_nesting_depth,
    )?;
    let batch_id = BatchId::new(format!(
        "{}-u{:08}-b{:08}",
        state.request.batch_id_prefix, state.request.unit.ordinal, state.sequence
    ))?;
    let mut batch = Batch::from_record_batch(
        batch_id,
        state.request.resource_id.clone(),
        state.request.partition_id.clone(),
        state.observed_hash.clone(),
        record_batch,
    )?;
    batch.header.source_position = state.request.source_position.clone();
    unknowns.attach(&mut batch)?;
    let actual_bytes = record_batch_retained_bytes(
        batch
            .record_batch()
            .ok_or_else(|| CdfError::internal("Protobuf batch lost its Arrow payload"))?,
    )?
    .checked_add(batch.header.pre_contract_evidence_retained_bytes()?)
    .ok_or_else(|| CdfError::data("Protobuf Arrow output memory overflowed"))?;
    if actual_bytes > state.options.maximum_output_batch_bytes {
        return Err(CdfError::data(format!(
            "Protobuf batch materialized {actual_bytes} bytes above the configured {}-byte output maximum; lower batch sizing or increase format_options.maximum_output_batch_bytes",
            state.options.maximum_output_batch_bytes
        )));
    }
    if actual_bytes > output_lease.bytes() {
        return Err(CdfError::data(format!(
            "Protobuf output estimate reserved {} bytes but the Arrow batch requires {actual_bytes}; lower batch sizing or increase format_options.maximum_output_batch_bytes",
            output_lease.bytes()
        )));
    }
    let physical = AccountedPhysicalBatch::new(batch, output_lease)?;
    state.sequence = state
        .sequence
        .checked_add(1)
        .ok_or_else(|| CdfError::data("Protobuf batch sequence overflowed"))?;
    Ok(Some((physical, state)))
}

async fn read_length_prefix(cursor: &mut AccountedByteCursor) -> Result<Option<u64>> {
    let Some(first) = cursor.next_byte().await? else {
        return Ok(None);
    };
    let mut bytes = [0_u8; MAXIMUM_LENGTH_PREFIX_BYTES];
    bytes[0] = first;
    if first & 0x80 == 0 {
        return Ok(Some(u64::from(first)));
    }
    for index in 1..MAXIMUM_LENGTH_PREFIX_BYTES {
        bytes[index] = cursor.next_byte().await?.ok_or_else(|| {
            CdfError::data("Protobuf stream ended inside a message length prefix")
        })?;
        if bytes[index] & 0x80 == 0 {
            return decode_varint(&bytes[..=index], "message length").map(|(value, _)| Some(value));
        }
    }
    Err(CdfError::data(
        "Protobuf message length prefix exceeds ten bytes",
    ))
}

fn output_authority_bytes(
    encoded_bytes: u64,
    rows: usize,
    fields: usize,
    target_batch_bytes: u64,
    maximum_output_batch_bytes: u64,
) -> Result<u64> {
    let row_field_overhead = u64::try_from(rows)
        .ok()
        .and_then(|rows| rows.checked_mul(u64::try_from(fields).ok()?))
        .and_then(|cells| cells.checked_mul(32))
        .ok_or_else(|| CdfError::data("Protobuf output estimate overflowed"))?;
    let estimate = encoded_bytes
        .checked_mul(OUTPUT_ESTIMATE_MULTIPLIER)
        .and_then(|bytes| bytes.checked_add(row_field_overhead))
        .and_then(|bytes| bytes.checked_add(64 * 1024))
        .ok_or_else(|| CdfError::data("Protobuf output estimate overflowed"))?
        .max(target_batch_bytes)
        .max(1);
    if estimate > maximum_output_batch_bytes {
        return Err(CdfError::data(format!(
            "Protobuf batch requires a conservative {estimate}-byte output authority above the configured {maximum_output_batch_bytes}-byte maximum; lower batch sizing or increase format_options.maximum_output_batch_bytes"
        )));
    }
    Ok(estimate)
}

struct UnknownValue {
    source_row_ordinal: u64,
    batch_row_ordinal: usize,
    path: Vec<String>,
    number: u32,
    wire_type: u8,
    raw: Vec<u8>,
}

#[derive(Default)]
struct UnknownValues(Vec<UnknownValue>);

impl UnknownValues {
    fn push(
        &mut self,
        source_row_ordinal: u64,
        batch_row_ordinal: usize,
        path: &[&str],
        occurrence: &WireOccurrence<'_>,
    ) {
        let mut source_path = path
            .iter()
            .map(|segment| (*segment).to_owned())
            .collect::<Vec<_>>();
        source_path.push("$protobuf_unknown".to_owned());
        source_path.push(occurrence.number.to_string());
        self.0.push(UnknownValue {
            source_row_ordinal,
            batch_row_ordinal,
            path: source_path,
            number: occurrence.number,
            wire_type: occurrence.wire_type,
            raw: occurrence.raw.to_vec(),
        });
    }

    fn attach(self, batch: &mut Batch) -> Result<()> {
        if self.0.is_empty() {
            return Ok(());
        }
        let total = self.0.iter().try_fold(0_usize, |total, value| {
            total
                .checked_add(value.raw.len())
                .ok_or_else(|| CdfError::data("Protobuf unknown-field evidence size overflowed"))
        })?;
        let mut builder = BinaryBuilder::with_capacity(self.0.len(), total);
        for value in &self.0 {
            builder.append_value(&value.raw);
        }
        let values = Arc::new(builder.finish());
        for (index, value) in self.0.into_iter().enumerate() {
            let mut metadata = std::collections::HashMap::new();
            metadata.insert(
                "cdf:protobuf_field_number".to_owned(),
                value.number.to_string(),
            );
            metadata.insert(
                "cdf:protobuf_wire_type".to_owned(),
                value.wire_type.to_string(),
            );
            batch
                .header
                .push_residual_candidate(PreContractResidualCandidate::new(
                    value.source_row_ordinal,
                    value.batch_row_ordinal,
                    value.path,
                    Field::new("protobuf_unknown", DataType::Binary, false).with_metadata(metadata),
                    None,
                    values.clone(),
                    index,
                )?);
        }
        Ok(())
    }
}

fn build_record_batch(
    complete_plan: &MessagePlan,
    projected_plan: &MessagePlan,
    messages: &[BufferedMessage],
    source_row_start: u64,
    maximum_depth: u32,
) -> Result<(RecordBatch, UnknownValues)> {
    let mut builders = projected_plan
        .fields
        .iter()
        .map(|field| make_builder(field.arrow_field.data_type(), messages.len()))
        .collect::<Vec<_>>();
    let mut unknowns = UnknownValues::default();
    for (row, message) in messages.iter().enumerate() {
        let view = MessageView::parse(&message.bytes, maximum_depth)?;
        let source_row = source_row_start
            .checked_add(
                u64::try_from(row).map_err(|_| CdfError::data("Protobuf batch row exceeds u64"))?,
            )
            .ok_or_else(|| CdfError::data("Protobuf source row ordinal overflowed"))?;
        let mut path = Vec::new();
        inspect_message(
            complete_plan,
            &view,
            &mut path,
            source_row,
            row,
            maximum_depth,
            &mut unknowns,
        )?;
        for (builder, field) in builders.iter_mut().zip(&projected_plan.fields) {
            append_field(builder.as_mut(), field, &view, maximum_depth)?;
        }
    }
    let arrays = builders
        .iter_mut()
        .map(|builder| builder.finish())
        .collect();
    let batch = RecordBatch::try_new(Arc::clone(&projected_plan.arrow_schema), arrays)
        .map_err(|error| CdfError::data(format!("build Protobuf Arrow batch: {error}")))?;
    Ok((batch, unknowns))
}

fn inspect_message<'a>(
    plan: &'a MessagePlan,
    view: &MessageView<'_>,
    path: &mut Vec<&'a str>,
    source_row: u64,
    batch_row: usize,
    maximum_depth: u32,
    output: &mut UnknownValues,
) -> Result<()> {
    for occurrence in view.occurrences() {
        match plan.descriptor_fields.get(&occurrence.number) {
            Some(field) if wire_compatible(field, occurrence.wire_type) => {}
            _ => output.push(source_row, batch_row, path, occurrence),
        }
    }
    for field in &plan.fields {
        let nested = needs_inspection(&field.value);
        if field.descriptor.cardinality() != Cardinality::Required && !nested {
            continue;
        }
        let occurrences = selected_occurrences(&field.descriptor, view);
        if field.descriptor.cardinality() == Cardinality::Required && occurrences.is_empty() {
            return Err(CdfError::data(format!(
                "Protobuf required field `{}` ({}) is absent",
                field.descriptor.full_name(),
                field.descriptor.number()
            )));
        }
        path.push(field.descriptor.name());
        inspect_nested(
            &field.value,
            &occurrences,
            path,
            source_row,
            batch_row,
            maximum_depth,
            output,
        )?;
        path.pop();
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn inspect_nested<'a>(
    value: &'a ValuePlan,
    occurrences: &SelectedOccurrences<'_>,
    path: &mut Vec<&'a str>,
    source_row: u64,
    batch_row: usize,
    maximum_depth: u32,
    output: &mut UnknownValues,
) -> Result<()> {
    let nested = needs_inspection(value);
    if !nested {
        return Ok(());
    }
    match value {
        ValuePlan::Message(message) => {
            if !occurrences.is_empty() {
                let view = MessageView::parse_many(
                    occurrences
                        .as_slice()
                        .iter()
                        .map(|occurrence| occurrence.value),
                    maximum_depth,
                )?;
                inspect_message(
                    message,
                    &view,
                    path,
                    source_row,
                    batch_row,
                    maximum_depth.saturating_sub(1),
                    output,
                )?;
            }
        }
        ValuePlan::List(inner) => {
            for occurrence in occurrences.as_slice() {
                let occurrence = SelectedOccurrences::One(occurrence);
                inspect_nested(
                    inner,
                    &occurrence,
                    path,
                    source_row,
                    batch_row,
                    maximum_depth.saturating_sub(1),
                    output,
                )?;
            }
        }
        ValuePlan::Map { key, value } => {
            for occurrence in occurrences.as_slice() {
                let entry = MessageView::parse(occurrence.value, maximum_depth)?;
                path.push("$map_entry");
                for entry_occurrence in entry.occurrences() {
                    let compatible = match entry_occurrence.number {
                        1 => value_wire_compatible(key, entry_occurrence.wire_type),
                        2 => value_wire_compatible(value, entry_occurrence.wire_type),
                        _ => false,
                    };
                    if !compatible {
                        output.push(source_row, batch_row, path, entry_occurrence);
                    }
                }
                let values = SelectedOccurrences::from_iter(
                    entry
                        .field(2)
                        .filter(|occurrence| value_wire_compatible(value, occurrence.wire_type)),
                );
                if !values.is_empty() {
                    path.push("value");
                    inspect_nested(
                        value,
                        &values,
                        path,
                        source_row,
                        batch_row,
                        maximum_depth.saturating_sub(1),
                        output,
                    )?;
                    path.pop();
                }
                path.pop();
            }
        }
        ValuePlan::Timestamp | ValuePlan::Duration => collect_well_known_unknowns(
            occurrences,
            &[(1, WIRE_VARINT), (2, WIRE_VARINT)],
            path,
            source_row,
            batch_row,
            maximum_depth,
            output,
        )?,
        ValuePlan::Wrapper(scalar) => collect_well_known_unknowns(
            occurrences,
            &[(1, scalar_wire_type(scalar))],
            path,
            source_row,
            batch_row,
            maximum_depth,
            output,
        )?,
        ValuePlan::Any => collect_well_known_unknowns(
            occurrences,
            &[(1, WIRE_LENGTH_DELIMITED), (2, WIRE_LENGTH_DELIMITED)],
            path,
            source_row,
            batch_row,
            maximum_depth,
            output,
        )?,
        ValuePlan::FieldMask => collect_well_known_unknowns(
            occurrences,
            &[(1, WIRE_LENGTH_DELIMITED)],
            path,
            source_row,
            batch_row,
            maximum_depth,
            output,
        )?,
        ValuePlan::Empty => collect_well_known_unknowns(
            occurrences,
            &[],
            path,
            source_row,
            batch_row,
            maximum_depth,
            output,
        )?,
        _ => {}
    }
    Ok(())
}

fn needs_inspection(value: &ValuePlan) -> bool {
    match value {
        ValuePlan::Scalar(_) | ValuePlan::OpaqueMessage { .. } => false,
        ValuePlan::List(inner) => needs_inspection(inner),
        _ => true,
    }
}

#[allow(clippy::too_many_arguments)]
fn collect_well_known_unknowns(
    occurrences: &SelectedOccurrences<'_>,
    known: &[(u32, u8)],
    path: &mut Vec<&str>,
    source_row: u64,
    batch_row: usize,
    maximum_depth: u32,
    output: &mut UnknownValues,
) -> Result<()> {
    if occurrences.is_empty() {
        return Ok(());
    }
    let view = MessageView::parse_many(
        occurrences
            .as_slice()
            .iter()
            .map(|occurrence| occurrence.value),
        maximum_depth,
    )?;
    for occurrence in view.occurrences() {
        if !known.iter().any(|(number, wire_type)| {
            *number == occurrence.number && *wire_type == occurrence.wire_type
        }) {
            output.push(source_row, batch_row, path, occurrence);
        }
    }
    Ok(())
}

fn append_field(
    builder: &mut dyn ArrayBuilder,
    field: &FieldPlan,
    view: &MessageView<'_>,
    maximum_depth: u32,
) -> Result<()> {
    if let ValuePlan::Scalar(scalar) = &field.value {
        let occurrence = selected_scalar_occurrence(&field.descriptor, view);
        return append_scalar_field(builder, scalar, &field.descriptor, occurrence);
    }
    let occurrences = selected_occurrences(&field.descriptor, view);
    match &field.value {
        ValuePlan::List(value) => {
            append_list(builder, value, occurrences.as_slice(), maximum_depth)
        }
        ValuePlan::Map { key, value } => {
            append_map(builder, key, value, occurrences.as_slice(), maximum_depth)
        }
        value => append_singular(
            builder,
            value,
            &field.descriptor,
            occurrences.as_slice(),
            maximum_depth,
        ),
    }
}

enum SelectedOccurrences<'a> {
    None,
    One(&'a WireOccurrence<'a>),
    Many(Vec<&'a WireOccurrence<'a>>),
}

impl<'a> SelectedOccurrences<'a> {
    fn from_iter(iter: impl IntoIterator<Item = &'a WireOccurrence<'a>>) -> Self {
        let mut iter = iter.into_iter();
        let Some(first) = iter.next() else {
            return Self::None;
        };
        let Some(second) = iter.next() else {
            return Self::One(first);
        };
        let mut values = vec![first, second];
        values.extend(iter);
        Self::Many(values)
    }

    fn is_empty(&self) -> bool {
        matches!(self, Self::None)
    }

    fn as_slice(&self) -> &[&'a WireOccurrence<'a>] {
        match self {
            Self::None => &[],
            Self::One(value) => std::slice::from_ref(value),
            Self::Many(values) => values,
        }
    }
}

fn append_scalar_field(
    builder: &mut dyn ArrayBuilder,
    scalar: &ScalarPlan,
    field: &FieldDescriptor,
    occurrence: Option<&WireOccurrence<'_>>,
) -> Result<()> {
    if let Some(occurrence) = occurrence {
        return append_scalar(builder, scalar, occurrence);
    }
    if field.cardinality() == Cardinality::Required {
        return Err(CdfError::data(format!(
            "Protobuf required field `{}` ({}) is absent",
            field.full_name(),
            field.number()
        )));
    }
    if field.supports_presence() {
        return append_null(builder, &ValuePlan::Scalar(scalar.clone()), 1);
    }
    append_scalar_default(builder, scalar, Some(field.default_value()))
}

fn selected_scalar_occurrence<'a>(
    field: &FieldDescriptor,
    view: &'a MessageView<'a>,
) -> Option<&'a WireOccurrence<'a>> {
    let candidate = view
        .field(field.number())
        .filter(|occurrence| wire_compatible(field, occurrence.wire_type))
        .last()?;
    let Some(oneof) = field.containing_oneof() else {
        return Some(candidate);
    };
    oneof
        .fields()
        .filter_map(|member| {
            view.field(member.number())
                .filter(|occurrence| wire_compatible(&member, occurrence.wire_type))
                .last()
        })
        .max_by_key(|occurrence| occurrence.order)
        .filter(|active| active.number == field.number())
}

fn selected_occurrences<'a>(
    field: &FieldDescriptor,
    view: &'a MessageView<'a>,
) -> SelectedOccurrences<'a> {
    let compatible = || {
        view.field(field.number())
            .filter(|occurrence| wire_compatible(field, occurrence.wire_type))
    };
    let Some(oneof) = field.containing_oneof() else {
        return if field.is_list() || field.is_map() || matches!(field.kind(), Kind::Message(_)) {
            SelectedOccurrences::from_iter(compatible())
        } else {
            compatible()
                .last()
                .map_or(SelectedOccurrences::None, SelectedOccurrences::One)
        };
    };
    let members = oneof.fields().collect::<Vec<_>>();
    let active = members
        .iter()
        .flat_map(|member| {
            view.field(member.number())
                .filter(|occurrence| wire_compatible(member, occurrence.wire_type))
        })
        .max_by_key(|occurrence| occurrence.order);
    if active.is_none_or(|active| active.number != field.number()) {
        return SelectedOccurrences::None;
    }
    if !matches!(field.kind(), Kind::Message(_)) {
        return compatible()
            .last()
            .map_or(SelectedOccurrences::None, SelectedOccurrences::One);
    }
    let cutoff = view
        .occurrences()
        .iter()
        .filter(|occurrence| {
            members.iter().any(|member| {
                member.number() != field.number()
                    && member.number() == occurrence.number
                    && wire_compatible(member, occurrence.wire_type)
            })
        })
        .map(|occurrence| occurrence.order)
        .max()
        .map_or(0, |order| order.saturating_add(1));
    SelectedOccurrences::from_iter(compatible().filter(|occurrence| occurrence.order >= cutoff))
}

fn wire_compatible(field: &FieldDescriptor, wire_type: u8) -> bool {
    if field.is_group() {
        return wire_type == WIRE_START_GROUP;
    }
    if field.is_list() && wire_type == WIRE_LENGTH_DELIMITED {
        return true;
    }
    match field.kind() {
        Kind::Double | Kind::Fixed64 | Kind::Sfixed64 => wire_type == WIRE_FIXED64,
        Kind::Float | Kind::Fixed32 | Kind::Sfixed32 => wire_type == WIRE_FIXED32,
        Kind::Int32
        | Kind::Int64
        | Kind::Uint32
        | Kind::Uint64
        | Kind::Sint32
        | Kind::Sint64
        | Kind::Bool
        | Kind::Enum(_) => wire_type == WIRE_VARINT,
        Kind::String | Kind::Bytes | Kind::Message(_) => wire_type == WIRE_LENGTH_DELIMITED,
    }
}

fn value_wire_compatible(value: &ValuePlan, wire_type: u8) -> bool {
    match value {
        ValuePlan::Scalar(scalar) | ValuePlan::Wrapper(scalar) => {
            scalar_wire_type(scalar) == wire_type
        }
        ValuePlan::Message(_)
        | ValuePlan::OpaqueMessage { .. }
        | ValuePlan::Timestamp
        | ValuePlan::Duration
        | ValuePlan::Any
        | ValuePlan::FieldMask
        | ValuePlan::Empty
        | ValuePlan::Map { .. } => wire_type == WIRE_LENGTH_DELIMITED,
        ValuePlan::List(inner) => {
            wire_type == WIRE_LENGTH_DELIMITED || value_wire_compatible(inner, wire_type)
        }
    }
}

fn scalar_wire_type(scalar: &ScalarPlan) -> u8 {
    match scalar {
        ScalarPlan::Double | ScalarPlan::Fixed64 | ScalarPlan::Sfixed64 => WIRE_FIXED64,
        ScalarPlan::Float | ScalarPlan::Fixed32 | ScalarPlan::Sfixed32 => WIRE_FIXED32,
        ScalarPlan::String | ScalarPlan::Bytes => WIRE_LENGTH_DELIMITED,
        _ => WIRE_VARINT,
    }
}

fn append_singular(
    builder: &mut dyn ArrayBuilder,
    value: &ValuePlan,
    field: &FieldDescriptor,
    occurrences: &[&WireOccurrence<'_>],
    maximum_depth: u32,
) -> Result<()> {
    if occurrences.is_empty() {
        if field.cardinality() == Cardinality::Required {
            return Err(CdfError::data(format!(
                "Protobuf required field `{}` ({}) is absent",
                field.full_name(),
                field.number()
            )));
        }
        if field.supports_presence() {
            return append_null(builder, value, maximum_depth);
        }
        return append_default(builder, value, field.default_value(), maximum_depth);
    }
    append_value(builder, value, occurrences, maximum_depth)
}

fn append_value(
    builder: &mut dyn ArrayBuilder,
    value: &ValuePlan,
    occurrences: &[&WireOccurrence<'_>],
    maximum_depth: u32,
) -> Result<()> {
    match value {
        ValuePlan::Scalar(scalar) => append_scalar(
            builder,
            scalar,
            occurrences.last().copied().ok_or_else(|| {
                CdfError::internal("append Protobuf scalar without an occurrence")
            })?,
        ),
        ValuePlan::Message(message) => {
            let view = MessageView::parse_many(
                occurrences.iter().map(|occurrence| occurrence.value),
                maximum_depth,
            )?;
            let builder = downcast_builder::<StructBuilder>(builder, "struct")?;
            for (child_builder, field) in
                builder.field_builders_mut().iter_mut().zip(&message.fields)
            {
                append_field(
                    child_builder.as_mut(),
                    field,
                    &view,
                    maximum_depth.saturating_sub(1),
                )?;
            }
            builder.append(true);
            Ok(())
        }
        ValuePlan::OpaqueMessage { .. } => {
            let builder = downcast_builder::<BinaryBuilder>(builder, "binary")?;
            let total = occurrences.iter().try_fold(0_usize, |total, value| {
                total.checked_add(value.value.len()).ok_or_else(|| {
                    CdfError::data("Protobuf recursive-message merge size overflowed")
                })
            })?;
            let mut merged = Vec::with_capacity(total);
            for occurrence in occurrences {
                merged.extend_from_slice(occurrence.value);
            }
            builder.append_value(merged);
            Ok(())
        }
        ValuePlan::Timestamp => append_timestamp(builder, occurrences, maximum_depth),
        ValuePlan::Duration => append_duration(builder, occurrences, maximum_depth),
        ValuePlan::Wrapper(scalar) => {
            let view = MessageView::parse_many(
                occurrences.iter().map(|occurrence| occurrence.value),
                maximum_depth,
            )?;
            if let Some(value) = view
                .field(1)
                .filter(|occurrence| occurrence.wire_type == scalar_wire_type(scalar))
                .last()
            {
                append_scalar(builder, scalar, value)
            } else {
                append_scalar_default(builder, scalar, None)
            }
        }
        ValuePlan::Any => append_any(builder, occurrences, maximum_depth),
        ValuePlan::FieldMask => append_field_mask(builder, occurrences, maximum_depth),
        ValuePlan::Empty => {
            for occurrence in occurrences {
                MessageView::parse(occurrence.value, maximum_depth)?;
            }
            downcast_builder::<StructBuilder>(builder, "empty struct")?.append(true);
            Ok(())
        }
        ValuePlan::List(_) | ValuePlan::Map { .. } => Err(CdfError::internal(
            "nested Protobuf collection reached singular append path",
        )),
    }
}

fn append_list(
    builder: &mut dyn ArrayBuilder,
    value: &ValuePlan,
    occurrences: &[&WireOccurrence<'_>],
    maximum_depth: u32,
) -> Result<()> {
    let builder = downcast_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(builder, "list")?;
    for occurrence in occurrences {
        if occurrence.wire_type == WIRE_LENGTH_DELIMITED && scalar_is_packable(value) {
            for packed in packed_occurrences(value, occurrence.value)? {
                append_value(builder.values().as_mut(), value, &[&packed], maximum_depth)?;
            }
        } else {
            append_value(
                builder.values().as_mut(),
                value,
                &[*occurrence],
                maximum_depth,
            )?;
        }
    }
    builder.append(true);
    Ok(())
}

fn append_map(
    builder: &mut dyn ArrayBuilder,
    key_plan: &ValuePlan,
    value_plan: &ValuePlan,
    occurrences: &[&WireOccurrence<'_>],
    maximum_depth: u32,
) -> Result<()> {
    let builder = downcast_builder::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>(
        builder, "map",
    )?;
    let mut entries = Vec::with_capacity(occurrences.len());
    for occurrence in occurrences {
        let entry = MessageView::parse(occurrence.value, maximum_depth)?;
        let key = map_key(
            key_plan,
            entry
                .field(1)
                .filter(|occurrence| value_wire_compatible(key_plan, occurrence.wire_type))
                .last(),
        )?;
        entries.push((key, entry));
    }
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    let mut deduplicated = Vec::with_capacity(entries.len());
    for entry in entries {
        if deduplicated
            .last()
            .is_some_and(|previous: &(MapKey, MessageView<'_>)| previous.0 == entry.0)
        {
            deduplicated.pop();
        }
        deduplicated.push(entry);
    }
    for (key, entry) in deduplicated {
        append_map_key(builder.keys().as_mut(), key_plan, &key)?;
        let values = entry
            .field(2)
            .filter(|occurrence| value_wire_compatible(value_plan, occurrence.wire_type))
            .collect::<Vec<_>>();
        if values.is_empty() {
            append_default_for_plan(builder.values().as_mut(), value_plan, maximum_depth)?;
        } else {
            append_value(
                builder.values().as_mut(),
                value_plan,
                &values,
                maximum_depth,
            )?;
        }
    }
    builder
        .append(true)
        .map_err(|error| CdfError::data(format!("append Protobuf map: {error}")))
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum MapKey {
    Bool(bool),
    Signed(i64),
    Unsigned(u64),
    String(String),
}

fn map_key(plan: &ValuePlan, occurrence: Option<&WireOccurrence<'_>>) -> Result<MapKey> {
    let ValuePlan::Scalar(scalar) = plan else {
        return Err(CdfError::contract("Protobuf map key must be scalar"));
    };
    let Some(occurrence) = occurrence else {
        return Ok(match scalar {
            ScalarPlan::Bool => MapKey::Bool(false),
            ScalarPlan::String => MapKey::String(String::new()),
            ScalarPlan::Int32
            | ScalarPlan::Int64
            | ScalarPlan::Sint32
            | ScalarPlan::Sint64
            | ScalarPlan::Sfixed32
            | ScalarPlan::Sfixed64 => MapKey::Signed(0),
            ScalarPlan::Uint32 | ScalarPlan::Uint64 | ScalarPlan::Fixed32 | ScalarPlan::Fixed64 => {
                MapKey::Unsigned(0)
            }
            _ => return Err(CdfError::contract("invalid Protobuf map key kind")),
        });
    };
    Ok(match scalar {
        ScalarPlan::Bool => MapKey::Bool(read_varint(occurrence)? != 0),
        ScalarPlan::String => MapKey::String(read_string(occurrence)?.to_owned()),
        ScalarPlan::Int32 => MapKey::Signed(i64::from(read_varint(occurrence)? as i32)),
        ScalarPlan::Int64 => MapKey::Signed(read_varint(occurrence)? as i64),
        ScalarPlan::Sint32 => MapKey::Signed(i64::from(zigzag32(read_varint(occurrence)?))),
        ScalarPlan::Sint64 => MapKey::Signed(zigzag64(read_varint(occurrence)?)),
        ScalarPlan::Sfixed32 => MapKey::Signed(i64::from(read_fixed32(occurrence)? as i32)),
        ScalarPlan::Sfixed64 => MapKey::Signed(read_fixed64(occurrence)? as i64),
        ScalarPlan::Uint32 => MapKey::Unsigned(u64::from(read_varint(occurrence)? as u32)),
        ScalarPlan::Uint64 => MapKey::Unsigned(read_varint(occurrence)?),
        ScalarPlan::Fixed32 => MapKey::Unsigned(u64::from(read_fixed32(occurrence)?)),
        ScalarPlan::Fixed64 => MapKey::Unsigned(read_fixed64(occurrence)?),
        _ => return Err(CdfError::contract("invalid Protobuf map key kind")),
    })
}

fn append_map_key(builder: &mut dyn ArrayBuilder, plan: &ValuePlan, key: &MapKey) -> Result<()> {
    let ValuePlan::Scalar(scalar) = plan else {
        return Err(CdfError::internal("append non-scalar Protobuf map key"));
    };
    match (scalar, key) {
        (ScalarPlan::Bool, MapKey::Bool(value)) => {
            downcast_builder::<BooleanBuilder>(builder, "bool")?.append_value(*value)
        }
        (ScalarPlan::String, MapKey::String(value)) => {
            downcast_builder::<StringBuilder>(builder, "string")?.append_value(value)
        }
        (ScalarPlan::Int32 | ScalarPlan::Sint32 | ScalarPlan::Sfixed32, MapKey::Signed(value)) => {
            downcast_builder::<Int32Builder>(builder, "int32")?.append_value(*value as i32)
        }
        (ScalarPlan::Int64 | ScalarPlan::Sint64 | ScalarPlan::Sfixed64, MapKey::Signed(value)) => {
            downcast_builder::<Int64Builder>(builder, "int64")?.append_value(*value)
        }
        (ScalarPlan::Uint32 | ScalarPlan::Fixed32, MapKey::Unsigned(value)) => {
            downcast_builder::<UInt32Builder>(builder, "uint32")?.append_value(*value as u32)
        }
        (ScalarPlan::Uint64 | ScalarPlan::Fixed64, MapKey::Unsigned(value)) => {
            downcast_builder::<UInt64Builder>(builder, "uint64")?.append_value(*value)
        }
        _ => return Err(CdfError::internal("Protobuf map key plan/value mismatch")),
    }
    Ok(())
}

fn append_scalar(
    builder: &mut dyn ArrayBuilder,
    scalar: &ScalarPlan,
    occurrence: &WireOccurrence<'_>,
) -> Result<()> {
    match scalar {
        ScalarPlan::Double => downcast_builder::<Float64Builder>(builder, "double")?
            .append_value(f64::from_bits(read_fixed64(occurrence)?)),
        ScalarPlan::Float => downcast_builder::<Float32Builder>(builder, "float")?
            .append_value(f32::from_bits(read_fixed32(occurrence)?)),
        ScalarPlan::Int32 => downcast_builder::<Int32Builder>(builder, "int32")?
            .append_value(read_varint(occurrence)? as i32),
        ScalarPlan::Int64 => downcast_builder::<Int64Builder>(builder, "int64")?
            .append_value(read_varint(occurrence)? as i64),
        ScalarPlan::Uint32 => downcast_builder::<UInt32Builder>(builder, "uint32")?
            .append_value(read_varint(occurrence)? as u32),
        ScalarPlan::Uint64 => downcast_builder::<UInt64Builder>(builder, "uint64")?
            .append_value(read_varint(occurrence)?),
        ScalarPlan::Sint32 => downcast_builder::<Int32Builder>(builder, "sint32")?
            .append_value(zigzag32(read_varint(occurrence)?)),
        ScalarPlan::Sint64 => downcast_builder::<Int64Builder>(builder, "sint64")?
            .append_value(zigzag64(read_varint(occurrence)?)),
        ScalarPlan::Fixed32 => downcast_builder::<UInt32Builder>(builder, "fixed32")?
            .append_value(read_fixed32(occurrence)?),
        ScalarPlan::Fixed64 => downcast_builder::<UInt64Builder>(builder, "fixed64")?
            .append_value(read_fixed64(occurrence)?),
        ScalarPlan::Sfixed32 => downcast_builder::<Int32Builder>(builder, "sfixed32")?
            .append_value(read_fixed32(occurrence)? as i32),
        ScalarPlan::Sfixed64 => downcast_builder::<Int64Builder>(builder, "sfixed64")?
            .append_value(read_fixed64(occurrence)? as i64),
        ScalarPlan::Bool => downcast_builder::<BooleanBuilder>(builder, "bool")?
            .append_value(read_varint(occurrence)? != 0),
        ScalarPlan::String => downcast_builder::<StringBuilder>(builder, "string")?
            .append_value(read_string(occurrence)?),
        ScalarPlan::Bytes => {
            downcast_builder::<BinaryBuilder>(builder, "bytes")?.append_value(occurrence.value)
        }
        ScalarPlan::Enum { .. } => downcast_builder::<Int32Builder>(builder, "enum")?
            .append_value(read_varint(occurrence)? as i32),
    }
    Ok(())
}

fn append_default(
    builder: &mut dyn ArrayBuilder,
    value: &ValuePlan,
    default: Value,
    maximum_depth: u32,
) -> Result<()> {
    match value {
        ValuePlan::Scalar(scalar) => append_scalar_default(builder, scalar, Some(default)),
        _ => append_default_for_plan(builder, value, maximum_depth),
    }
}

fn append_scalar_default(
    builder: &mut dyn ArrayBuilder,
    scalar: &ScalarPlan,
    default: Option<Value>,
) -> Result<()> {
    match scalar {
        ScalarPlan::Double => {
            downcast_builder::<Float64Builder>(builder, "double")?.append_value(match default {
                Some(Value::F64(value)) => value,
                _ => 0.0,
            })
        }
        ScalarPlan::Float => {
            downcast_builder::<Float32Builder>(builder, "float")?.append_value(match default {
                Some(Value::F32(value)) => value,
                _ => 0.0,
            })
        }
        ScalarPlan::Int32 | ScalarPlan::Sint32 | ScalarPlan::Sfixed32 => {
            downcast_builder::<Int32Builder>(builder, "int32")?.append_value(match default {
                Some(Value::I32(value)) => value,
                _ => 0,
            })
        }
        ScalarPlan::Int64 | ScalarPlan::Sint64 | ScalarPlan::Sfixed64 => {
            downcast_builder::<Int64Builder>(builder, "int64")?.append_value(match default {
                Some(Value::I64(value)) => value,
                _ => 0,
            })
        }
        ScalarPlan::Uint32 | ScalarPlan::Fixed32 => {
            downcast_builder::<UInt32Builder>(builder, "uint32")?.append_value(match default {
                Some(Value::U32(value)) => value,
                _ => 0,
            })
        }
        ScalarPlan::Uint64 | ScalarPlan::Fixed64 => {
            downcast_builder::<UInt64Builder>(builder, "uint64")?.append_value(match default {
                Some(Value::U64(value)) => value,
                _ => 0,
            })
        }
        ScalarPlan::Bool => {
            downcast_builder::<BooleanBuilder>(builder, "bool")?.append_value(match default {
                Some(Value::Bool(value)) => value,
                _ => false,
            })
        }
        ScalarPlan::String => {
            downcast_builder::<StringBuilder>(builder, "string")?.append_value(match default {
                Some(Value::String(value)) => value,
                _ => String::new(),
            })
        }
        ScalarPlan::Bytes => {
            downcast_builder::<BinaryBuilder>(builder, "bytes")?.append_value(match default {
                Some(Value::Bytes(value)) => value,
                _ => Vec::new().into(),
            })
        }
        ScalarPlan::Enum { .. } => {
            downcast_builder::<Int32Builder>(builder, "enum")?.append_value(match default {
                Some(Value::EnumNumber(value)) => value,
                _ => 0,
            })
        }
    }
    Ok(())
}

fn append_default_for_plan(
    builder: &mut dyn ArrayBuilder,
    value: &ValuePlan,
    maximum_depth: u32,
) -> Result<()> {
    match value {
        ValuePlan::Scalar(scalar) | ValuePlan::Wrapper(scalar) => {
            append_scalar_default(builder, scalar, None)
        }
        ValuePlan::Message(message) => {
            let view = MessageView::parse(&[], maximum_depth)?;
            let builder = downcast_builder::<StructBuilder>(builder, "struct")?;
            for (child, field) in builder.field_builders_mut().iter_mut().zip(&message.fields) {
                append_field(
                    child.as_mut(),
                    field,
                    &view,
                    maximum_depth.saturating_sub(1),
                )?;
            }
            builder.append(true);
            Ok(())
        }
        ValuePlan::OpaqueMessage { .. } => {
            downcast_builder::<BinaryBuilder>(builder, "binary")?.append_value([]);
            Ok(())
        }
        ValuePlan::Timestamp | ValuePlan::Duration => append_time_parts(builder, 0, 0, true),
        ValuePlan::Any => append_any(builder, &[], maximum_depth),
        ValuePlan::FieldMask => {
            downcast_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(builder, "field mask")?
                .append(true);
            Ok(())
        }
        ValuePlan::Empty => {
            downcast_builder::<StructBuilder>(builder, "empty")?.append(true);
            Ok(())
        }
        ValuePlan::List(_) => {
            downcast_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(builder, "list")?.append(true);
            Ok(())
        }
        ValuePlan::Map { .. } => downcast_builder::<
            MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>,
        >(builder, "map")?
        .append(true)
        .map_err(|error| CdfError::data(format!("append Protobuf map default: {error}"))),
    }
}

fn append_null(
    builder: &mut dyn ArrayBuilder,
    value: &ValuePlan,
    maximum_depth: u32,
) -> Result<()> {
    match value {
        ValuePlan::Scalar(ScalarPlan::Double) => {
            downcast_builder::<Float64Builder>(builder, "double")?.append_null()
        }
        ValuePlan::Scalar(ScalarPlan::Float) => {
            downcast_builder::<Float32Builder>(builder, "float")?.append_null()
        }
        ValuePlan::Scalar(
            ScalarPlan::Int32 | ScalarPlan::Sint32 | ScalarPlan::Sfixed32 | ScalarPlan::Enum { .. },
        ) => downcast_builder::<Int32Builder>(builder, "int32")?.append_null(),
        ValuePlan::Scalar(ScalarPlan::Int64 | ScalarPlan::Sint64 | ScalarPlan::Sfixed64) => {
            downcast_builder::<Int64Builder>(builder, "int64")?.append_null()
        }
        ValuePlan::Scalar(ScalarPlan::Uint32 | ScalarPlan::Fixed32) => {
            downcast_builder::<UInt32Builder>(builder, "uint32")?.append_null()
        }
        ValuePlan::Scalar(ScalarPlan::Uint64 | ScalarPlan::Fixed64) => {
            downcast_builder::<UInt64Builder>(builder, "uint64")?.append_null()
        }
        ValuePlan::Scalar(ScalarPlan::Bool) => {
            downcast_builder::<BooleanBuilder>(builder, "bool")?.append_null()
        }
        ValuePlan::Scalar(ScalarPlan::String) => {
            downcast_builder::<StringBuilder>(builder, "string")?.append_null()
        }
        ValuePlan::Scalar(ScalarPlan::Bytes) | ValuePlan::OpaqueMessage { .. } => {
            downcast_builder::<BinaryBuilder>(builder, "binary")?.append_null()
        }
        ValuePlan::Message(message) => {
            let builder = downcast_builder::<StructBuilder>(builder, "struct")?;
            for (child, field) in builder.field_builders_mut().iter_mut().zip(&message.fields) {
                append_default_for_plan(
                    child.as_mut(),
                    &field.value,
                    maximum_depth.saturating_sub(1),
                )?;
            }
            builder.append(false);
        }
        ValuePlan::Timestamp | ValuePlan::Duration => append_time_parts(builder, 0, 0, false)?,
        ValuePlan::Wrapper(scalar) => {
            return append_null(builder, &ValuePlan::Scalar(scalar.clone()), maximum_depth);
        }
        ValuePlan::Any => {
            let builder = downcast_builder::<StructBuilder>(builder, "any")?;
            builder
                .field_builder::<StringBuilder>(0)
                .ok_or_else(|| CdfError::internal("Any type_url builder mismatch"))?
                .append_value("");
            builder
                .field_builder::<BinaryBuilder>(1)
                .ok_or_else(|| CdfError::internal("Any value builder mismatch"))?
                .append_value([]);
            builder.append(false);
        }
        ValuePlan::FieldMask | ValuePlan::List(_) => {
            downcast_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(builder, "list")?.append(false)
        }
        ValuePlan::Empty => downcast_builder::<StructBuilder>(builder, "empty")?.append(false),
        ValuePlan::Map { .. } => downcast_builder::<
            MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>,
        >(builder, "map")?
        .append(false)
        .map_err(|error| CdfError::data(format!("append null Protobuf map: {error}")))?,
    }
    Ok(())
}

fn append_timestamp(
    builder: &mut dyn ArrayBuilder,
    occurrences: &[&WireOccurrence<'_>],
    maximum_depth: u32,
) -> Result<()> {
    let view = MessageView::parse_many(
        occurrences.iter().map(|occurrence| occurrence.value),
        maximum_depth,
    )?;
    let seconds = last_wire_field(&view, 1, WIRE_VARINT)
        .map(read_varint)
        .transpose()?
        .unwrap_or(0) as i64;
    let nanos = last_wire_field(&view, 2, WIRE_VARINT)
        .map(read_varint)
        .transpose()?
        .unwrap_or(0) as i32;
    if !(-62_135_596_800..=253_402_300_799).contains(&seconds)
        || !(0..=999_999_999).contains(&nanos)
    {
        return Err(CdfError::data(format!(
            "Protobuf Timestamp is outside its specified range: seconds={seconds}, nanos={nanos}"
        )));
    }
    append_time_parts(builder, seconds, nanos, true)
}

fn append_duration(
    builder: &mut dyn ArrayBuilder,
    occurrences: &[&WireOccurrence<'_>],
    maximum_depth: u32,
) -> Result<()> {
    let view = MessageView::parse_many(
        occurrences.iter().map(|occurrence| occurrence.value),
        maximum_depth,
    )?;
    let seconds = last_wire_field(&view, 1, WIRE_VARINT)
        .map(read_varint)
        .transpose()?
        .unwrap_or(0) as i64;
    let nanos = last_wire_field(&view, 2, WIRE_VARINT)
        .map(read_varint)
        .transpose()?
        .unwrap_or(0) as i32;
    if !(-315_576_000_000..=315_576_000_000).contains(&seconds)
        || !(-999_999_999..=999_999_999).contains(&nanos)
        || (seconds > 0 && nanos < 0)
        || (seconds < 0 && nanos > 0)
    {
        return Err(CdfError::data(format!(
            "Protobuf Duration is outside its specified range: seconds={seconds}, nanos={nanos}"
        )));
    }
    append_time_parts(builder, seconds, nanos, true)
}

fn append_time_parts(
    builder: &mut dyn ArrayBuilder,
    seconds: i64,
    nanos: i32,
    valid: bool,
) -> Result<()> {
    let builder = downcast_builder::<StructBuilder>(builder, "timestamp/duration")?;
    builder
        .field_builder::<Int64Builder>(0)
        .ok_or_else(|| CdfError::internal("Protobuf time seconds builder mismatch"))?
        .append_value(seconds);
    builder
        .field_builder::<Int32Builder>(1)
        .ok_or_else(|| CdfError::internal("Protobuf time nanos builder mismatch"))?
        .append_value(nanos);
    builder.append(valid);
    Ok(())
}

fn append_any(
    builder: &mut dyn ArrayBuilder,
    occurrences: &[&WireOccurrence<'_>],
    maximum_depth: u32,
) -> Result<()> {
    let view = MessageView::parse_many(
        occurrences.iter().map(|occurrence| occurrence.value),
        maximum_depth,
    )?;
    let type_url = last_wire_field(&view, 1, WIRE_LENGTH_DELIMITED)
        .map(read_string)
        .transpose()?
        .unwrap_or("");
    let value =
        last_wire_field(&view, 2, WIRE_LENGTH_DELIMITED).map_or(&[][..], |value| value.value);
    let builder = downcast_builder::<StructBuilder>(builder, "Any")?;
    builder
        .field_builder::<StringBuilder>(0)
        .ok_or_else(|| CdfError::internal("Any type_url builder mismatch"))?
        .append_value(type_url);
    builder
        .field_builder::<BinaryBuilder>(1)
        .ok_or_else(|| CdfError::internal("Any value builder mismatch"))?
        .append_value(value);
    builder.append(true);
    Ok(())
}

fn append_field_mask(
    builder: &mut dyn ArrayBuilder,
    occurrences: &[&WireOccurrence<'_>],
    maximum_depth: u32,
) -> Result<()> {
    let view = MessageView::parse_many(
        occurrences.iter().map(|occurrence| occurrence.value),
        maximum_depth,
    )?;
    let builder = downcast_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(builder, "FieldMask")?;
    let values = downcast_builder::<StringBuilder>(builder.values().as_mut(), "FieldMask path")?;
    for path in view
        .field(1)
        .filter(|occurrence| occurrence.wire_type == WIRE_LENGTH_DELIMITED)
    {
        values.append_value(read_string(path)?);
    }
    builder.append(true);
    Ok(())
}

fn scalar_is_packable(value: &ValuePlan) -> bool {
    matches!(
        value,
        ValuePlan::Scalar(
            ScalarPlan::Double
                | ScalarPlan::Float
                | ScalarPlan::Int32
                | ScalarPlan::Int64
                | ScalarPlan::Uint32
                | ScalarPlan::Uint64
                | ScalarPlan::Sint32
                | ScalarPlan::Sint64
                | ScalarPlan::Fixed32
                | ScalarPlan::Fixed64
                | ScalarPlan::Sfixed32
                | ScalarPlan::Sfixed64
                | ScalarPlan::Bool
                | ScalarPlan::Enum { .. }
        )
    )
}

fn packed_occurrences<'a>(value: &ValuePlan, bytes: &'a [u8]) -> Result<Vec<WireOccurrence<'a>>> {
    let ValuePlan::Scalar(scalar) = value else {
        return Err(CdfError::internal("packed non-scalar Protobuf field"));
    };
    let wire_type = match scalar {
        ScalarPlan::Double | ScalarPlan::Fixed64 | ScalarPlan::Sfixed64 => WIRE_FIXED64,
        ScalarPlan::Float | ScalarPlan::Fixed32 | ScalarPlan::Sfixed32 => WIRE_FIXED32,
        _ => WIRE_VARINT,
    };
    let mut output = Vec::new();
    let mut offset = 0;
    while offset < bytes.len() {
        let length = match wire_type {
            WIRE_FIXED64 => 8,
            WIRE_FIXED32 => 4,
            WIRE_VARINT => decode_varint(&bytes[offset..], "packed value")?.1,
            _ => {
                return Err(CdfError::internal(
                    "non-packable Protobuf wire type reached packed decoder",
                ));
            }
        };
        let end = offset
            .checked_add(length)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| CdfError::data("Protobuf packed field ended inside a value"))?;
        output.push(WireOccurrence {
            number: 0,
            wire_type,
            value: &bytes[offset..end],
            raw: &bytes[offset..end],
            order: output.len(),
        });
        offset = end;
    }
    Ok(output)
}

fn read_varint(occurrence: &WireOccurrence<'_>) -> Result<u64> {
    if occurrence.wire_type != WIRE_VARINT {
        return Err(CdfError::data("Protobuf scalar has the wrong wire type"));
    }
    let (value, consumed) = decode_varint(occurrence.value, "scalar value")?;
    if consumed != occurrence.value.len() {
        return Err(CdfError::data("Protobuf scalar varint has trailing bytes"));
    }
    Ok(value)
}

fn last_wire_field<'a>(
    view: &'a MessageView<'a>,
    number: u32,
    wire_type: u8,
) -> Option<&'a WireOccurrence<'a>> {
    view.last_field_with_wire(number, wire_type)
}

fn read_fixed32(occurrence: &WireOccurrence<'_>) -> Result<u32> {
    let bytes: [u8; 4] = occurrence
        .value
        .try_into()
        .map_err(|_| CdfError::data("Protobuf fixed32 field does not contain four bytes"))?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_fixed64(occurrence: &WireOccurrence<'_>) -> Result<u64> {
    let bytes: [u8; 8] = occurrence
        .value
        .try_into()
        .map_err(|_| CdfError::data("Protobuf fixed64 field does not contain eight bytes"))?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_string<'a>(occurrence: &'a WireOccurrence<'a>) -> Result<&'a str> {
    std::str::from_utf8(occurrence.value)
        .map_err(|error| CdfError::data(format!("Protobuf string is not UTF-8: {error}")))
}

fn zigzag32(value: u64) -> i32 {
    let value = value as u32;
    ((value >> 1) as i32) ^ -((value & 1) as i32)
}

fn zigzag64(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

fn downcast_builder<'a, T: ArrayBuilder + 'static>(
    builder: &'a mut dyn ArrayBuilder,
    label: &str,
) -> Result<&'a mut T> {
    builder
        .as_any_mut()
        .downcast_mut::<T>()
        .ok_or_else(|| CdfError::internal(format!("Protobuf {label} Arrow builder type mismatch")))
}

#[cfg(test)]
mod tests;
