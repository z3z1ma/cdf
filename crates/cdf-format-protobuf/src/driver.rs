//! Public Protobuf format-driver composition and discovery.

use std::{collections::BTreeMap, sync::Arc};

use cdf_kernel::{BoxFuture, CdfError, PushdownFidelity, Result};
use cdf_runtime::{
    ByteExtent, ByteSource, DecodePlanningRequest, DecodeUnitPlan, FormatDecodeSession,
    FormatDetection, FormatDetectionConfidence, FormatDetectionProbe, FormatDiscoveryCapabilities,
    FormatDiscoveryKind, FormatDiscoveryRequest, FormatDriver, FormatDriverDescriptor,
    FormatErrorIsolation, FormatId, FormatProbe, FormatSourceAccess, PhysicalSchemaObservation,
};

use crate::decode::ProtobufDecodeSession;
use crate::options::{
    DEFAULT_MAXIMUM_DESCRIPTOR_BYTES, DEFAULT_MAXIMUM_MESSAGE_BYTES, DEFAULT_MAXIMUM_NESTING_DEPTH,
    DEFAULT_MAXIMUM_OUTPUT_BATCH_BYTES, MAXIMUM_DESCRIPTOR_BYTES, MAXIMUM_MESSAGE_BYTES,
    MAXIMUM_NESTING_DEPTH, MAXIMUM_OUTPUT_BATCH_BYTES, ProtobufOptions,
};

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
