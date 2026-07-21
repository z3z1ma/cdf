use cdf_kernel::{
    CdfError, CompiledSourcePlanHash, DrainTermination, ExecutionExtent, LateDataAction,
    OperatorWatermarkBehavior, PartitionWatermarkAggregation, ResourceId, Result,
    WatermarkAuthority, WatermarkPolicy,
};
use serde::{Deserialize, Serialize};

use crate::{
    CompiledSourceExecutionPlan, CompiledSourcePlan, SourceExecutionCapabilities,
    SourceStreamCapabilities, artifact_hash,
};

pub const COMPILED_STREAM_POLICY_VERSION: u16 = 1;

/// Canonical evidence that a kernel execution extent was joined with one exact source capability
/// artifact. It records policy compilation; A8 owns executing finite epochs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledStreamPolicy {
    pub version: u16,
    pub resource_id: ResourceId,
    pub execution_extent: ExecutionExtent,
    pub compiled_source_plan_hash: CompiledSourcePlanHash,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_stream_capabilities: Option<SourceStreamCapabilities>,
    pub semantic_hash: String,
}

impl CompiledStreamPolicy {
    pub fn compile(extent: &ExecutionExtent, source: &CompiledSourcePlan) -> Result<Self> {
        source.validate()?;
        extent.validate_for_plan()?;
        validate_extent_source_join(extent, source)?;
        let mut compiled = Self {
            version: COMPILED_STREAM_POLICY_VERSION,
            resource_id: source.descriptor.resource_id.clone(),
            execution_extent: extent.clone(),
            compiled_source_plan_hash: source.compiled_source_plan_hash()?,
            source_stream_capabilities: source.stream_capabilities.clone(),
            semantic_hash: String::new(),
        };
        compiled.semantic_hash = compiled.canonical_hash()?;
        compiled.validate_intrinsic()?;
        compiled.validate_against_source(source)?;
        Ok(compiled)
    }

    pub fn validate_intrinsic(&self) -> Result<()> {
        if self.version != COMPILED_STREAM_POLICY_VERSION {
            return Err(CdfError::contract(format!(
                "unsupported compiled stream policy version {}; expected {}",
                self.version, COMPILED_STREAM_POLICY_VERSION
            )));
        }
        ResourceId::new(self.resource_id.as_str())?;
        self.execution_extent.validate_for_plan()?;
        if let Some(capabilities) = &self.source_stream_capabilities {
            capabilities.validate()?;
        }
        if self.semantic_hash != self.canonical_hash()? {
            return Err(CdfError::contract(
                "compiled stream policy semantic hash does not match its canonical evidence",
            ));
        }
        Ok(())
    }

    pub fn validate_against_source(&self, source: &CompiledSourcePlan) -> Result<()> {
        source.validate()?;
        self.validate_intrinsic()?;
        if self.resource_id != source.descriptor.resource_id
            || self.compiled_source_plan_hash != source.compiled_source_plan_hash()?
            || self.source_stream_capabilities != source.stream_capabilities
        {
            return Err(CdfError::contract(
                "compiled stream policy no longer matches its exact source capability artifact",
            ));
        }
        self.execution_extent.validate_for_plan()?;
        validate_extent_source_join(&self.execution_extent, source)
    }

    pub fn validate_against_execution_plan(
        &self,
        source: &CompiledSourceExecutionPlan,
    ) -> Result<()> {
        source.validate()?;
        self.validate_intrinsic()?;
        if self.resource_id != source.resource_id
            || &self.compiled_source_plan_hash != source.compiled_source_plan_hash()
            || self.source_stream_capabilities.as_ref() != source.stream_capabilities()
        {
            return Err(CdfError::contract(
                "compiled stream policy no longer matches its exact source execution artifact",
            ));
        }
        validate_extent_capabilities(
            &self.execution_extent,
            &source.resource_id,
            &source.execution_capabilities,
            source.stream_capabilities.as_ref(),
        )
    }

    fn canonical_hash(&self) -> Result<String> {
        artifact_hash(&(
            self.version,
            &self.resource_id,
            &self.execution_extent,
            &self.compiled_source_plan_hash,
            &self.source_stream_capabilities,
        ))
    }
}

fn validate_extent_source_join(
    extent: &ExecutionExtent,
    source: &CompiledSourcePlan,
) -> Result<()> {
    validate_extent_capabilities(
        extent,
        &source.descriptor.resource_id,
        &source.execution_capabilities,
        source.stream_capabilities.as_ref(),
    )
}

fn validate_extent_capabilities(
    extent: &ExecutionExtent,
    resource_id: &ResourceId,
    execution: &SourceExecutionCapabilities,
    stream: Option<&SourceStreamCapabilities>,
) -> Result<()> {
    match extent {
        ExecutionExtent::Bounded { .. } if execution.bounded => Ok(()),
        ExecutionExtent::Bounded { .. } => Err(CdfError::contract(format!(
            "resource `{}` is unbounded but execution defaults to bounded; declare a complete drain policy with finite termination",
            resource_id
        ))),
        ExecutionExtent::Resident { .. } => Err(CdfError::contract(
            "resident execution is not enabled; use a finite drain termination or wait for the resident supervisor",
        )),
        ExecutionExtent::Drain { .. } if execution.bounded => Err(CdfError::contract(format!(
            "resource `{}` is bounded and cannot use drain execution; remove the drain policy",
            resource_id
        ))),
        ExecutionExtent::Drain {
            policy,
            termination,
            ..
        } => {
            let capabilities = stream.ok_or_else(|| {
                CdfError::contract(format!(
                    "resource `{}` is unbounded but its source driver omitted stream capabilities",
                    resource_id
                ))
            })?;
            if !capabilities.supports_frontier(policy.safe_frontier) {
                return Err(CdfError::contract(format!(
                    "resource `{}` source does not support the declared safe_frontier; choose a capability reported by `cdf inspect resource {}`",
                    resource_id, resource_id
                )));
            }
            validate_termination(termination, resource_id, execution, capabilities)?;
            validate_watermark(&policy.watermark, capabilities, resource_id)?;
            if policy.late_data == LateDataAction::RecaptureNextEpoch
                && (!execution.resumable || !execution.reopenable)
            {
                return Err(CdfError::contract(format!(
                    "resource `{}` uses recapture_next_epoch but its source is not resumable and reopenable; use quarantine/admit_with_annotation or a resumable source",
                    resource_id
                )));
            }
            Ok(())
        }
    }
}

fn validate_termination(
    termination: &DrainTermination,
    resource_id: &ResourceId,
    execution: &SourceExecutionCapabilities,
    capabilities: &SourceStreamCapabilities,
) -> Result<()> {
    match termination {
        DrainTermination::Quiescent if !capabilities.quiescence => {
            Err(CdfError::contract(format!(
                "resource `{}` requests quiescent drain termination but its source cannot prove quiescence; choose duration, records, bytes, or source_frontier",
                resource_id
            )))
        }
        DrainTermination::SourceFrontier { .. }
            if !execution.resumable || !execution.reopenable =>
        {
            Err(CdfError::contract(format!(
                "resource `{}` requests source_frontier termination but its source is not resumable and reopenable",
                resource_id
            )))
        }
        DrainTermination::SourceFrontier { position } => {
            position.validate()?;
            if !capabilities.supports_source_frontier(position) {
                return Err(CdfError::contract(format!(
                    "resource `{resource_id}` cannot compare the declared {:?} source_frontier dimensions; choose duration, records, or bytes termination, or use the exact field/log/protocol reported by `cdf inspect resource {resource_id}`",
                    position.kind(),
                )));
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_watermark(
    watermark: &WatermarkPolicy,
    capabilities: &SourceStreamCapabilities,
    resource_id: &ResourceId,
) -> Result<()> {
    let WatermarkPolicy::Enabled {
        partition_aggregation,
        ..
    } = watermark
    else {
        return Ok(());
    };
    let WatermarkPolicy::Enabled { authority, .. } = watermark else {
        unreachable!("enabled watermark established above")
    };
    let WatermarkPolicy::Enabled {
        event_time_field,
        domain,
        ..
    } = watermark
    else {
        unreachable!("enabled watermark established above")
    };
    if capabilities.watermark_behavior == OperatorWatermarkBehavior::Drop {
        return Err(CdfError::contract(format!(
            "resource `{resource_id}` enables watermarks but its source declares watermark behavior drop; disable watermarks or use a source that emits compatible claims"
        )));
    }
    let source_watermark = capabilities.watermark.as_ref().ok_or_else(|| {
        CdfError::contract(format!(
            "resource `{resource_id}` enables watermark field `{event_time_field}` but its source declares no watermark authority; disable watermarks or use the exact field/domain reported by `cdf inspect resource {resource_id}`"
        ))
    })?;
    if source_watermark.event_time_field.as_ref() != event_time_field.as_ref()
        || &source_watermark.domain != domain
        || &source_watermark.authority != authority
    {
        return Err(CdfError::contract(format!(
            "resource `{resource_id}` watermark field/domain/authority does not match the source capability; use the exact capability reported by `cdf inspect resource {resource_id}` or disable watermarks"
        )));
    }
    match (&capabilities.watermark_behavior, authority) {
        (OperatorWatermarkBehavior::Preserve, WatermarkAuthority::Source) => {}
        (
            OperatorWatermarkBehavior::Transform { mapping_id },
            WatermarkAuthority::Derived {
                mapping_id: authority_mapping,
            },
        ) if mapping_id == authority_mapping => {}
        (OperatorWatermarkBehavior::Drop, _) => {
            return Err(CdfError::contract(format!(
                "resource `{resource_id}` enables watermarks but its source declares watermark behavior drop; disable watermarks or use a source that emits compatible claims"
            )));
        }
        (OperatorWatermarkBehavior::Preserve, WatermarkAuthority::Derived { mapping_id }) => {
            return Err(CdfError::contract(format!(
                "resource `{resource_id}` declares derived watermark mapping `{mapping_id}` but no source transform provides it; use source authority or a source-declared transform with the same mapping id"
            )));
        }
        (OperatorWatermarkBehavior::Transform { mapping_id }, _) => {
            return Err(CdfError::contract(format!(
                "resource `{resource_id}` source transforms watermarks with mapping `{mapping_id}` but the policy does not record that exact derived authority"
            )));
        }
    }
    if let PartitionWatermarkAggregation::MinimumEligible { capability_id, .. } =
        partition_aggregation
        && !capabilities.supports_idleness(capability_id)
    {
        return Err(CdfError::contract(format!(
            "resource `{resource_id}` requests idleness capability `{capability_id}` that its source does not declare"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use cdf_kernel::{
        CanonicalArrowTimeUnit, CursorPosition, CursorValue, EpochClosureTrigger, EventTimeDomain,
        ResourceCapabilities, ResourceDescriptor, STREAM_EPOCH_POLICY_VERSION, SafeFrontierPolicy,
        SchemaHash, SchemaSource, ScopeKey, SourcePosition, StreamEpochPolicy, TrustLevel,
        TypePolicyAllowances, WatermarkAuthority, WriteDisposition,
    };

    use super::*;
    use crate::{
        CompiledSourcePlanInput, SourceAttestationStrength, SourceBatchMemoryContract,
        SourceDriverDescriptor, SourceDriverId, SourceExecutionCapabilities, SourceExecutorClass,
        SourceFrontierCapability, SourceRetryGranularity, SourceWatermarkCapability,
    };

    fn source(stream: SourceStreamCapabilities, resumable: bool) -> CompiledSourcePlan {
        let schema_hash = SchemaHash::new(format!("sha256:{}", "11".repeat(32))).unwrap();
        CompiledSourcePlan::new_with_stream_capabilities(
            SourceDriverDescriptor {
                driver_id: SourceDriverId::new("mock_stream").unwrap(),
                driver_version: "1.0.0".to_owned(),
                option_schema_hash: format!("sha256:{}", "22".repeat(32)),
                kinds: vec!["mock_stream".to_owned()],
                schemes: Vec::new(),
            },
            ResourceCapabilities::default(),
            SourceExecutionCapabilities {
                minimum_poll_bytes: 1,
                maximum_poll_bytes: 1024,
                minimum_decode_bytes: 1,
                maximum_decode_bytes: 4096,
                maximum_concurrency: 4,
                useful_concurrency: 4,
                executor_class: SourceExecutorClass::Io,
                blocking_lane: None,
                pausable: true,
                spillable: false,
                idempotent_reads: resumable,
                reopenable: true,
                resumable,
                speculative_safe: false,
                retry_granularity: SourceRetryGranularity::None,
                retryable_errors: Vec::new(),
                retry_policy: None,
                attestation: SourceAttestationStrength::Metadata,
                rate_limit: None,
                quota_authority: None,
                canonical_order: true,
                bounded: false,
                batch_memory: SourceBatchMemoryContract::Preaccounted,
                telemetry_version: "stream-test-v1".to_owned(),
            },
            Some(stream),
            CompiledSourcePlanInput {
                descriptor: ResourceDescriptor {
                    resource_id: ResourceId::new("mock.events").unwrap(),
                    schema_source: SchemaSource::Declared {
                        schema_hash,
                        source: "mock".to_owned(),
                    },
                    primary_key: Vec::new(),
                    merge_key: Vec::new(),
                    cursor: None,
                    write_disposition: WriteDisposition::Append,
                    deduplication: None,
                    contract: None,
                    state_scope: ScopeKey::Resource,
                    freshness: None,
                    trust_level: TrustLevel::Governed,
                },
                schema: Schema::new(vec![Field::new(
                    "event_time",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    false,
                )]),
                type_policy_allowances: TypePolicyAllowances::default(),
                effective_schema_runtime: None,
                baseline_observation_schema_catalog: Vec::new(),
                redacted_options: serde_json::json!({}),
                physical_plan: serde_json::json!({}),
            },
        )
        .unwrap()
    }

    fn capabilities(watermark_behavior: OperatorWatermarkBehavior) -> SourceStreamCapabilities {
        let watermark = match &watermark_behavior {
            OperatorWatermarkBehavior::Preserve => Some(SourceWatermarkCapability {
                event_time_field: "event_time".into(),
                domain: EventTimeDomain::Timestamp {
                    unit: CanonicalArrowTimeUnit::Microsecond,
                    timezone: Some("UTC".into()),
                },
                authority: WatermarkAuthority::Source,
            }),
            OperatorWatermarkBehavior::Transform { mapping_id } => {
                Some(SourceWatermarkCapability {
                    event_time_field: "event_time".into(),
                    domain: EventTimeDomain::Timestamp {
                        unit: CanonicalArrowTimeUnit::Microsecond,
                        timezone: Some("UTC".into()),
                    },
                    authority: WatermarkAuthority::Derived {
                        mapping_id: mapping_id.clone(),
                    },
                })
            }
            OperatorWatermarkBehavior::Drop => None,
        };
        SourceStreamCapabilities {
            quiescence: false,
            watermark_behavior,
            watermark,
            safe_frontiers: vec![SafeFrontierPolicy::CanonicalAdmittedSourcePosition],
            source_frontiers: vec![SourceFrontierCapability::Cursor {
                fields: vec!["offset".to_owned()],
            }],
            idleness_capabilities: vec!["idle-v1".to_owned()],
        }
    }

    fn drain(watermark: WatermarkPolicy, termination: DrainTermination) -> ExecutionExtent {
        ExecutionExtent::Drain {
            version: cdf_kernel::EXECUTION_EXTENT_VERSION,
            policy: StreamEpochPolicy {
                version: STREAM_EPOCH_POLICY_VERSION,
                checkpoint_cadence: EpochClosureTrigger::Rows { count: 10_000 },
                package_rotation: EpochClosureTrigger::Bytes {
                    count: 64 * 1024 * 1024,
                },
                watermark,
                late_data: LateDataAction::Quarantine,
                safe_frontier: SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
            },
            termination,
        }
    }

    fn watermark(aggregation: PartitionWatermarkAggregation) -> WatermarkPolicy {
        WatermarkPolicy::Enabled {
            event_time_field: "event_time".into(),
            domain: EventTimeDomain::Timestamp {
                unit: CanonicalArrowTimeUnit::Microsecond,
                timezone: Some("UTC".into()),
            },
            authority: WatermarkAuthority::Source,
            partition_aggregation: aggregation,
        }
    }

    #[test]
    fn unbounded_policy_compiles_to_canonical_source_bound_evidence() {
        let source = source(capabilities(OperatorWatermarkBehavior::Preserve), false);
        let extent = drain(
            WatermarkPolicy::Disabled,
            DrainTermination::Records { count: 100_000 },
        );
        let compiled = CompiledStreamPolicy::compile(&extent, &source).unwrap();
        assert_eq!(compiled.execution_extent, extent);
        assert_eq!(
            compiled,
            CompiledStreamPolicy::compile(&extent, &source).unwrap()
        );
        compiled.validate_against_source(&source).unwrap();
        compiled
            .validate_against_execution_plan(
                &CompiledSourceExecutionPlan::compile(&source).unwrap(),
            )
            .unwrap();

        let mut tampered = compiled;
        tampered.semantic_hash = format!("sha256:{}", "00".repeat(32));
        assert!(
            tampered
                .validate_intrinsic()
                .unwrap_err()
                .message
                .contains("semantic hash")
        );
    }

    #[test]
    fn capability_matrix_fails_before_extraction_with_specific_remediation() {
        let dropped = source(capabilities(OperatorWatermarkBehavior::Drop), false);
        let error = CompiledStreamPolicy::compile(
            &drain(
                watermark(PartitionWatermarkAggregation::MinimumAll),
                DrainTermination::Duration {
                    milliseconds: 1_000,
                },
            ),
            &dropped,
        )
        .unwrap_err();
        assert!(
            error
                .message
                .contains("source declares watermark behavior drop")
        );

        let source = source(capabilities(OperatorWatermarkBehavior::Preserve), false);
        let error =
            CompiledStreamPolicy::compile(&ExecutionExtent::bounded(), &source).unwrap_err();
        assert!(error.message.contains("declare a complete drain policy"));
        let error = CompiledStreamPolicy::compile(
            &drain(WatermarkPolicy::Disabled, DrainTermination::Quiescent),
            &source,
        )
        .unwrap_err();
        assert!(error.message.contains("cannot prove quiescence"));
        let error = CompiledStreamPolicy::compile(
            &drain(
                watermark(PartitionWatermarkAggregation::MinimumEligible {
                    idle_after_milliseconds: 10_000,
                    capability_id: "missing-idle".into(),
                }),
                DrainTermination::Bytes { count: 1024 },
            ),
            &source,
        )
        .unwrap_err();
        assert!(error.message.contains("missing-idle"));
    }

    #[test]
    fn recapture_requires_resumable_source_capability() {
        let source = source(capabilities(OperatorWatermarkBehavior::Preserve), false);
        let mut extent = drain(
            WatermarkPolicy::Disabled,
            DrainTermination::Records { count: 100 },
        );
        let ExecutionExtent::Drain { policy, .. } = &mut extent else {
            unreachable!();
        };
        policy.late_data = LateDataAction::RecaptureNextEpoch;
        let error = CompiledStreamPolicy::compile(&extent, &source).unwrap_err();
        assert!(error.message.contains("not resumable and reopenable"));
    }

    #[test]
    fn every_finite_termination_and_enabled_watermark_compile_from_capabilities() {
        let mut stream = capabilities(OperatorWatermarkBehavior::Preserve);
        stream.quiescence = true;
        let source = source(stream, true);
        let terminations = [
            DrainTermination::Quiescent,
            DrainTermination::Duration {
                milliseconds: 1_000,
            },
            DrainTermination::Records { count: 10_000 },
            DrainTermination::Bytes { count: 1_048_576 },
            DrainTermination::SourceFrontier {
                position: SourcePosition::Cursor(CursorPosition {
                    version: 1,
                    field: "offset".to_owned(),
                    value: CursorValue::U64(42),
                }),
            },
        ];
        for termination in terminations {
            CompiledStreamPolicy::compile(
                &drain(
                    watermark(PartitionWatermarkAggregation::MinimumEligible {
                        idle_after_milliseconds: 30_000,
                        capability_id: "idle-v1".into(),
                    }),
                    termination,
                ),
                &source,
            )
            .unwrap();
        }
    }

    #[test]
    fn source_frontier_requires_declared_comparison_kind_and_valid_position() {
        let source = source(capabilities(OperatorWatermarkBehavior::Preserve), true);
        let incompatible = drain(
            WatermarkPolicy::Disabled,
            DrainTermination::SourceFrontier {
                position: SourcePosition::Log(cdf_kernel::LogPosition {
                    version: cdf_kernel::SOURCE_POSITION_VERSION,
                    log: "events".to_owned(),
                    offset: 42,
                    sequence: None,
                }),
            },
        );
        let error = CompiledStreamPolicy::compile(&incompatible, &source).unwrap_err();
        assert!(
            error
                .message
                .contains("cannot compare the declared Log source_frontier")
        );

        let wrong_field = drain(
            WatermarkPolicy::Disabled,
            DrainTermination::SourceFrontier {
                position: SourcePosition::Cursor(CursorPosition {
                    version: cdf_kernel::SOURCE_POSITION_VERSION,
                    field: "other".to_owned(),
                    value: CursorValue::U64(42),
                }),
            },
        );
        let error = CompiledStreamPolicy::compile(&wrong_field, &source).unwrap_err();
        assert!(error.message.contains("exact field/log/protocol"));

        let malformed = drain(
            WatermarkPolicy::Disabled,
            DrainTermination::SourceFrontier {
                position: SourcePosition::Cursor(CursorPosition {
                    version: cdf_kernel::SOURCE_POSITION_VERSION,
                    field: String::new(),
                    value: CursorValue::U64(42),
                }),
            },
        );
        let error = CompiledStreamPolicy::compile(&malformed, &source).unwrap_err();
        assert!(error.message.contains("cursor field"));

        let malformed_decimal = drain(
            WatermarkPolicy::Disabled,
            DrainTermination::SourceFrontier {
                position: SourcePosition::Cursor(CursorPosition {
                    version: cdf_kernel::SOURCE_POSITION_VERSION,
                    field: "offset".to_owned(),
                    value: CursorValue::DecimalString("not-a-decimal".to_owned()),
                }),
            },
        );
        let error = CompiledStreamPolicy::compile(&malformed_decimal, &source).unwrap_err();
        assert!(error.message.contains("cursor decimal string"));
    }

    #[test]
    fn source_watermark_capability_is_bound_to_the_compiled_arrow_schema() {
        let mut invalid_field_source =
            source(capabilities(OperatorWatermarkBehavior::Preserve), true);
        invalid_field_source
            .stream_capabilities
            .as_mut()
            .unwrap()
            .watermark
            .as_mut()
            .unwrap()
            .event_time_field = "missing".into();
        let error = invalid_field_source.validate().unwrap_err();
        assert!(
            error
                .message
                .contains("absent from the compiled source schema")
        );

        let mut invalid_domain_source =
            source(capabilities(OperatorWatermarkBehavior::Preserve), true);
        invalid_domain_source
            .stream_capabilities
            .as_mut()
            .unwrap()
            .watermark
            .as_mut()
            .unwrap()
            .domain = EventTimeDomain::Date32;
        let error = invalid_domain_source.validate().unwrap_err();
        assert!(error.message.contains("compiled Arrow type is"));
    }

    #[test]
    fn intrinsic_policy_rejects_coherently_rehashed_malformed_source_hash() {
        let source = source(capabilities(OperatorWatermarkBehavior::Preserve), true);
        let policy = CompiledStreamPolicy::compile(
            &drain(
                WatermarkPolicy::Disabled,
                DrainTermination::Records { count: 10 },
            ),
            &source,
        )
        .unwrap();
        let mut encoded = serde_json::to_value(&policy).unwrap();
        encoded["compiled_source_plan_hash"] = serde_json::json!("not-a-hash");
        let error = serde_json::from_value::<CompiledStreamPolicy>(encoded).unwrap_err();
        assert!(error.to_string().contains("sha256:<64 lowercase hex>"));
    }
}
