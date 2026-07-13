use std::cmp::Reverse;
use std::collections::BTreeSet;

use arrow_schema::{DataType, Field, TimeUnit};
use cdf_kernel::{
    CdfError, DeduplicationSpec, ResourceDescriptor, Result, TypeMapping, TypeMappingFidelity,
    semantic,
};
use serde::{Deserialize, Serialize};

use crate::{
    normalization::{
        NormalizedSchema, normalize_identifier, normalize_schema, validate_normalizer,
    },
    policy::*,
    program::*,
    schema::*,
    transforms::TransformColumn,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TypeMappingDecision {
    AllowedLossless,
    AllowedLossyByContract,
}
pub fn compile_validation_program(
    policy: &ContractPolicy,
    observed_schema: &ObservedSchema,
) -> Result<ValidationProgram> {
    validate_normalizer(&policy.normalization.identifier)?;
    let normalized_schema = normalize_schema(observed_schema, &policy.normalization.identifier)?;

    let mut column_programs = Vec::with_capacity(observed_schema.fields.len());
    for (field, normalized) in observed_schema
        .fields
        .iter()
        .zip(normalized_schema.fields.iter())
    {
        validate_type_fidelity(policy, field)?;

        let mut steps = Vec::new();
        if policy.types.preserve_decimal_exactness {
            steps.push(ColumnProgramStep::PreserveDecimalExactness);
        }
        if policy.types.preserve_timestamp_timezone {
            steps.push(ColumnProgramStep::PreserveTimestampTimezone);
        }
        steps.extend(
            policy
                .transforms
                .iter()
                .filter(|transform| transform.column_name() == Some(field.source_name.as_str()))
                .cloned()
                .map(ColumnProgramStep::ApplyTransform),
        );

        column_programs.push(ColumnProgram {
            source_name: field.source_name.clone(),
            output_name: normalized.output_name.clone(),
            arrow_type: field.arrow_type.clone(),
            steps,
            nested_action: nested_action_for_field(
                &field.source_name,
                &field.arrow_type,
                &policy.normalization,
            )?,
            redaction: redaction_decision_for_semantic(
                field
                    .metadata
                    .get(cdf_kernel::SEMANTIC_METADATA_KEY)
                    .map(String::as_str),
                &policy.quarantine.pii_redaction,
            ),
        });
    }

    for transform in &policy.transforms {
        let TransformDescription::Derive { column, .. } = transform else {
            continue;
        };
        let output_name = normalize_identifier(column, &policy.normalization.identifier)?;
        if column_programs.iter().any(|program| {
            program.source_name == *column
                || program.output_name == *column
                || program.output_name == output_name
        }) {
            return Err(CdfError::contract(format!(
                "derived field {column:?} collides with an existing source or normalized output field; rename the derived field"
            )));
        }
        column_programs.push(ColumnProgram {
            source_name: column.clone(),
            output_name,
            arrow_type: ArrowType::Boolean,
            steps: vec![ColumnProgramStep::ApplyTransform(transform.clone())],
            nested_action: NestedAction::NotNested,
            redaction: RedactionDecision::Preserve,
        });
    }

    let row_rules = row_rule_programs(policy, observed_schema);
    Ok(ValidationProgram {
        compiled_expression_plan: None,
        normalizer_version: policy.normalization.identifier.version.clone(),
        identifier_policy: policy.normalization.identifier.clone(),
        schema_coercion: None,
        residual: Some(residual_program(
            policy,
            observed_schema,
            &normalized_schema,
            &row_rules,
            None,
        )),
        schema_verdicts: schema_verdicts(&policy.schema, &policy.normalization.nested),
        column_programs,
        row_rules,
        explicit_anomalies: Vec::new(),
        row_dispositions: row_dispositions(policy),
        transforms: policy.transforms.clone(),
        promotion: policy.promotion.clone(),
        warnings: Vec::new(),
    })
}

pub fn compile_resource_validation_program(
    policy: &ContractPolicy,
    observed_schema: &ObservedSchema,
    descriptor: &ResourceDescriptor,
) -> Result<ValidationProgram> {
    let program = compile_validation_program(policy, observed_schema)?;
    bind_validation_program_to_resource(program, descriptor)
}

pub fn bind_validation_program_to_resource(
    mut program: ValidationProgram,
    descriptor: &ResourceDescriptor,
) -> Result<ValidationProgram> {
    if matches!(descriptor.deduplication, Some(DeduplicationSpec::ExactRow)) {
        if program.has_keyed_dedup_rule() {
            return Err(CdfError::contract(
                "resource exact-row deduplication conflicts with a contract dedup rule",
            ));
        }
        if !program.has_exact_row_dedup_rule() {
            let keys = program
                .column_programs
                .iter()
                .map(|column| column.output_name.clone())
                .chain(
                    program
                        .residual
                        .as_ref()
                        .and_then(|residual| residual.capture.as_ref())
                        .map(|capture| capture.variant_column.clone()),
                )
                .collect::<Vec<_>>();
            if keys.is_empty() {
                return Err(CdfError::contract(
                    "resource exact-row deduplication requires at least one schema field",
                ));
            }
            program.row_rules.push(RowRuleProgram {
                rule_id: format!("row-rule-{:04}-dedup", program.row_rules.len()),
                expression: dedup_expression("exact_row_dedup", keys, DedupKeepProgram::First),
                missing_column: MissingColumnBehavior::Error,
            });
            if let Some(residual) = &mut program.residual {
                for field in &mut residual.fields {
                    field.control_critical = true;
                }
            }
        }
    }
    let controls = descriptor
        .primary_key
        .iter()
        .chain(&descriptor.merge_key)
        .chain(descriptor.cursor.iter().map(|cursor| &cursor.field))
        .collect::<BTreeSet<_>>();
    if let Some(residual) = &mut program.residual {
        for control in controls {
            let field = residual
                .fields
                .iter_mut()
                .find(|field| field.source_name == *control || field.output_name == *control)
                .ok_or_else(|| {
                    CdfError::contract(format!(
                        "resource control field {control:?} is not covered by the validation program"
                    ))
                })?;
            field.control_critical = true;
        }
    } else if !controls.is_empty() {
        return Err(CdfError::contract(
            "resource control fields require a compiled residual verdict program",
        ));
    }
    Ok(program)
}

fn residual_program(
    policy: &ContractPolicy,
    observed_schema: &ObservedSchema,
    normalized_schema: &NormalizedSchema,
    row_rules: &[RowRuleProgram],
    descriptor: Option<&ResourceDescriptor>,
) -> ResidualProgram {
    let explicit_capture = matches!(
        policy.normalization.nested,
        NestedDataPolicy::VariantCapture(_)
    );
    let capture_allowed = policy.schema.mode == SchemaEvolutionMode::Evolve || explicit_capture;
    let default_verdict = if capture_allowed {
        ResidualCandidateVerdict::Capture
    } else {
        ResidualCandidateVerdict::Quarantine
    };
    let capture = if capture_allowed {
        let (variant_column, semantic) = match &policy.normalization.nested {
            NestedDataPolicy::VariantCapture(spec) => {
                (spec.column_name.clone(), spec.semantic.clone())
            }
            _ => (
                VARIANT_COLUMN_NAME.to_owned(),
                VARIANT_SEMANTIC_TAG.to_owned(),
            ),
        };
        Some(ResidualCaptureOutput {
            variant_column,
            semantic,
            encoding: crate::RESIDUAL_ENCODING_NAME.to_owned(),
        })
    } else {
        None
    };
    let controls = descriptor
        .map(|descriptor| {
            descriptor
                .primary_key
                .iter()
                .chain(&descriptor.merge_key)
                .chain(descriptor.cursor.iter().map(|cursor| &cursor.field))
                .cloned()
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    let required = row_rules
        .iter()
        .filter(|rule| rule.expression_function() == Some("is_not_null"))
        .flat_map(RowRuleProgram::referenced_columns)
        .collect::<BTreeSet<_>>();
    let rule_controls = row_rules
        .iter()
        .flat_map(RowRuleProgram::referenced_columns)
        .collect::<BTreeSet<_>>();
    ResidualProgram {
        default_verdict,
        pii_redaction: policy.quarantine.pii_redaction.clone(),
        capture,
        fields: observed_schema
            .fields
            .iter()
            .zip(&normalized_schema.fields)
            .map(|(field, normalized)| {
                let required = required.contains(field.source_name.as_str())
                    || required.contains(normalized.output_name.as_str());
                let control_critical = controls.contains(&field.source_name)
                    || controls.contains(&normalized.output_name)
                    || rule_controls.contains(field.source_name.as_str())
                    || rule_controls.contains(normalized.output_name.as_str());
                ResidualFieldProgram {
                    source_name: field.source_name.clone(),
                    output_name: normalized.output_name.clone(),
                    required,
                    control_critical,
                    redaction: redaction_decision_for_semantic(
                        field
                            .metadata
                            .get(cdf_kernel::SEMANTIC_METADATA_KEY)
                            .map(String::as_str),
                        &policy.quarantine.pii_redaction,
                    ),
                }
            })
            .collect(),
    }
}
pub fn redaction_decision_for_field(
    field: &Field,
    policy: &PiiRedactionPolicy,
) -> RedactionDecision {
    redaction_decision_for_semantic(semantic(field), policy)
}

pub fn redaction_decision_for_semantic(
    semantic: Option<&str>,
    policy: &PiiRedactionPolicy,
) -> RedactionDecision {
    match semantic {
        Some(tag) if tag.starts_with("pii:") => policy.pii_action.clone(),
        _ => policy.default_action.clone(),
    }
}

pub fn validate_type_mapping(
    policy: &ContractPolicy,
    mapping: &TypeMapping,
) -> Result<TypeMappingDecision> {
    match mapping.fidelity {
        TypeMappingFidelity::Lossless => Ok(TypeMappingDecision::AllowedLossless),
        TypeMappingFidelity::LossyRequiresContractAllowance if policy.types.allow_lossy_mapping => {
            Ok(TypeMappingDecision::AllowedLossyByContract)
        }
        TypeMappingFidelity::LossyRequiresContractAllowance => Err(CdfError::contract(format!(
            "lossy destination mapping from {} to {} requires allow_lossy_mapping",
            mapping.arrow_type, mapping.destination_type
        ))),
        TypeMappingFidelity::Unsupported => Err(CdfError::contract(format!(
            "unsupported destination mapping from {} to {}",
            mapping.arrow_type, mapping.destination_type
        ))),
    }
}

/// Resolves a canonical Arrow type against the destination-sheet pattern vocabulary.
/// Destination adapters declare data; shared compiler semantics interpret that data.
pub fn resolve_destination_type_mapping<'a>(
    mappings: &'a [TypeMapping],
    data_type: &DataType,
) -> Result<Option<&'a TypeMapping>> {
    let mut matches = mappings
        .iter()
        .filter_map(|mapping| {
            destination_type_pattern_specificity(&mapping.arrow_type, data_type)
                .map(|specificity| (specificity, mapping))
        })
        .collect::<Vec<_>>();
    matches.sort_by_key(|item| Reverse(item.0));
    let Some((best_specificity, best)) = matches.first().copied() else {
        return Ok(None);
    };
    let ambiguous = matches
        .iter()
        .skip(1)
        .take_while(|(specificity, _)| *specificity == best_specificity)
        .map(|(_, mapping)| mapping.arrow_type.as_str())
        .collect::<Vec<_>>();
    if !ambiguous.is_empty() {
        return Err(CdfError::contract(format!(
            "destination type mappings are ambiguous for Arrow type {data_type}: {:?} and {:?} have equal specificity {best_specificity}",
            best.arrow_type, ambiguous
        )));
    }
    Ok(Some(best))
}

fn destination_type_pattern_specificity(pattern: &str, data_type: &DataType) -> Option<u8> {
    let pattern = compact_ascii_lower(pattern);
    let display = compact_ascii_lower(&data_type.to_string());
    if pattern == display {
        return Some(100);
    }
    match data_type {
        DataType::Decimal128(_, _) if pattern == "decimal128(p,s)" => Some(90),
        DataType::Decimal256(_, _) if pattern == "decimal256(p,s)" => Some(90),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) if pattern == "decimal*" => {
            Some(50)
        }
        DataType::Time32(TimeUnit::Second | TimeUnit::Millisecond)
            if pattern == "time32(second|millisecond)" =>
        {
            Some(70)
        }
        DataType::Time64(TimeUnit::Microsecond) if pattern == "time64(microsecond)" => Some(90),
        DataType::Time64(TimeUnit::Nanosecond) if pattern == "time64(nanosecond)" => Some(90),
        DataType::Timestamp(unit, timezone) => {
            let unit = compact_ascii_lower(&format!("{unit:?}"));
            match timezone {
                None if pattern == format!("timestamp({unit},none)") => Some(90),
                Some(_) if pattern == format!("timestamp({unit},some(_))") => Some(90),
                _ if pattern == format!("timestamp({unit},*)") => Some(75),
                None if pattern == "timestamp(second|millisecond|microsecond,none)"
                    && matches!(unit.as_str(), "second" | "millisecond" | "microsecond") =>
                {
                    Some(70)
                }
                Some(_) if pattern == "timestamp(*,timezone)" => Some(60),
                _ => None,
            }
        }
        DataType::Struct(_) if pattern == "struct" => Some(85),
        DataType::Struct(_) if pattern == "struct/list/map" => Some(60),
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _)
            if pattern == "list" =>
        {
            Some(85)
        }
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _)
            if pattern == "struct/list/map" =>
        {
            Some(60)
        }
        DataType::Map(_, _) if pattern == "map" => Some(85),
        DataType::Map(_, _) if pattern == "struct/list/map" => Some(60),
        DataType::Union(_, _) if pattern == "union" => Some(85),
        DataType::Dictionary(_, _) if pattern == "dictionary" => Some(85),
        DataType::Duration(_) if pattern == "duration" => Some(85),
        DataType::Interval(_) if pattern == "interval" => Some(85),
        _ => None,
    }
}

fn compact_ascii_lower(value: &str) -> String {
    value
        .chars()
        .filter(|character| !character.is_ascii_whitespace())
        .flat_map(char::to_lowercase)
        .collect()
}

fn validate_type_fidelity(policy: &ContractPolicy, field: &ObservedField) -> Result<()> {
    if policy.types.preserve_decimal_exactness
        && let Some(SourceTypeClaim::Decimal { precision, scale }) = field.source_type
        && field.arrow_type.is_float()
    {
        return Err(CdfError::contract(format!(
            "decimal source field {:?} ({precision},{scale}) cannot compile as floating point",
            field.source_name
        )));
    }

    if policy.types.preserve_timestamp_timezone
        && let Some(SourceTypeClaim::Timestamp { timezone }) = &field.source_type
    {
        validate_timestamp_timezone(field, timezone)?;
    }

    Ok(())
}

fn validate_timestamp_timezone(field: &ObservedField, claim: &TimestampZoneClaim) -> Result<()> {
    let ArrowType::Timestamp { timezone, .. } = &field.arrow_type else {
        return Ok(());
    };

    match (claim, timezone) {
        (TimestampZoneClaim::Zoned { zone }, None) => Err(CdfError::contract(format!(
            "zoned timestamp field {:?} from zone {zone:?} lost its timezone",
            field.source_name
        ))),
        (TimestampZoneClaim::Naive, Some(observed_zone)) => Err(CdfError::contract(format!(
            "naive timestamp field {:?} cannot be silently assumed as timezone {observed_zone:?}",
            field.source_name
        ))),
        _ => Ok(()),
    }
}

fn schema_verdicts(
    schema: &SchemaPolicy,
    nested_policy: &NestedDataPolicy,
) -> Vec<SchemaVerdictRule> {
    vec![
        SchemaVerdictRule {
            change: SchemaChangeKind::NewTable,
            verdict: if schema.allow_new_table {
                VerdictAction::Admit
            } else {
                VerdictAction::RejectRun
            },
        },
        SchemaVerdictRule {
            change: SchemaChangeKind::NewColumn,
            verdict: if schema.allow_new_column {
                VerdictAction::Admit
            } else {
                VerdictAction::RejectRun
            },
        },
        SchemaVerdictRule {
            change: SchemaChangeKind::TypeWidening,
            verdict: if schema.allow_type_widening {
                VerdictAction::Admit
            } else {
                VerdictAction::RejectBatch
            },
        },
        SchemaVerdictRule {
            change: SchemaChangeKind::TypeNarrowing,
            verdict: if schema.quarantine_type_narrowing {
                VerdictAction::Quarantine
            } else {
                VerdictAction::RejectRun
            },
        },
        SchemaVerdictRule {
            change: SchemaChangeKind::UnknownField,
            verdict: if matches!(nested_policy, NestedDataPolicy::VariantCapture(_)) {
                VerdictAction::AdmitAsVariant
            } else if schema.allow_unknown_fields {
                VerdictAction::Admit
            } else {
                VerdictAction::RejectRun
            },
        },
    ]
}

fn row_dispositions(policy: &ContractPolicy) -> Vec<RowDispositionRule> {
    let violation = if policy.quarantine.enabled
        && matches!(policy.verdicts.violation, VerdictAction::Quarantine)
    {
        RowDispositionKind::Quarantine
    } else {
        action_to_row_disposition(&policy.verdicts.violation)
    };

    vec![
        RowDispositionRule {
            outcome: RuleOutcome::Pass,
            disposition: RowDispositionKind::Accept,
        },
        RowDispositionRule {
            outcome: RuleOutcome::Coerced,
            disposition: RowDispositionKind::Accept,
        },
        RowDispositionRule {
            outcome: RuleOutcome::AdmittedAsVariant,
            disposition: RowDispositionKind::Accept,
        },
        RowDispositionRule {
            outcome: RuleOutcome::Violation,
            disposition: violation,
        },
        RowDispositionRule {
            outcome: RuleOutcome::Fatal,
            disposition: action_to_row_disposition(&policy.verdicts.fatal),
        },
    ]
}

fn row_rule_programs(
    policy: &ContractPolicy,
    observed_schema: &ObservedSchema,
) -> Vec<RowRuleProgram> {
    let explicit_nullability_columns = policy
        .rows
        .rules
        .iter()
        .filter_map(|rule| match rule {
            RowRule::Nullability { column } => Some(column.as_str()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    let mut programs = Vec::new();

    for field in &observed_schema.fields {
        if field.nullable || explicit_nullability_columns.contains(field.source_name.as_str()) {
            continue;
        }
        programs.push(RowRuleProgram {
            rule_id: format!("nullability:{}", field.source_name),
            expression: unary_column_expression("is_not_null", &field.source_name),
            missing_column: MissingColumnBehavior::Skip,
        });
    }

    programs.extend(
        policy
            .rows
            .rules
            .iter()
            .enumerate()
            .map(|(index, rule)| row_rule_program_from_policy(index, rule)),
    );
    programs
}

fn row_rule_program_from_policy(index: usize, rule: &RowRule) -> RowRuleProgram {
    let (kind, expression) = match rule {
        RowRule::Nullability { column } => (
            "nullability",
            unary_column_expression("is_not_null", column),
        ),
        RowRule::Domain { column, allowed } => (
            "domain",
            crate::Expression::call(
                "in_domain",
                vec![
                    crate::ExpressionNode::Column {
                        name: column.clone(),
                    },
                    crate::ExpressionNode::Literal {
                        value: crate::ExpressionLiteral::StringList(allowed.clone()),
                    },
                ],
            ),
        ),
        RowRule::Range { column, min, max } => (
            "range",
            crate::Expression::call(
                "in_range",
                vec![
                    crate::ExpressionNode::Column {
                        name: column.clone(),
                    },
                    optional_bound(min),
                    optional_bound(max),
                ],
            ),
        ),
        RowRule::Regex { column, pattern } => (
            "regex",
            crate::Expression::call(
                "matches_regex",
                vec![
                    crate::ExpressionNode::Column {
                        name: column.clone(),
                    },
                    crate::ExpressionNode::Literal {
                        value: crate::ExpressionLiteral::String(pattern.clone()),
                    },
                ],
            ),
        ),
        RowRule::Freshness { column, max_age_ms } => (
            "freshness",
            crate::Expression::call(
                "fresh_within",
                vec![
                    crate::ExpressionNode::Column {
                        name: column.clone(),
                    },
                    crate::ExpressionNode::Literal {
                        value: crate::ExpressionLiteral::Unsigned(*max_age_ms),
                    },
                ],
            ),
        ),
        RowRule::Dedup { keys, keep } => (
            "dedup",
            dedup_expression(
                "dedup",
                keys.clone(),
                match keep {
                    DedupKeep::First => DedupKeepProgram::First,
                    DedupKeep::Last => DedupKeepProgram::Last,
                    DedupKeep::Fail => DedupKeepProgram::Fail,
                },
            ),
        ),
    };
    RowRuleProgram {
        rule_id: format!("row-rule-{index:04}-{kind}"),
        expression,
        missing_column: MissingColumnBehavior::Error,
    }
}

fn unary_column_expression(function: &str, column: &str) -> crate::Expression {
    crate::Expression::call(
        function,
        vec![crate::ExpressionNode::Column {
            name: column.to_owned(),
        }],
    )
}

fn optional_bound(value: &Option<String>) -> crate::ExpressionNode {
    crate::ExpressionNode::Literal {
        value: value
            .clone()
            .map(crate::ExpressionLiteral::String)
            .unwrap_or(crate::ExpressionLiteral::Null),
    }
}

fn dedup_expression(
    function: &str,
    keys: Vec<String>,
    keep: DedupKeepProgram,
) -> crate::Expression {
    let keep = match keep {
        DedupKeepProgram::First => "first",
        DedupKeepProgram::Last => "last",
        DedupKeepProgram::Fail => "fail",
    };
    crate::Expression::call(
        function,
        vec![
            crate::ExpressionNode::Literal {
                value: crate::ExpressionLiteral::StringList(keys),
            },
            crate::ExpressionNode::Literal {
                value: crate::ExpressionLiteral::String(keep.to_owned()),
            },
        ],
    )
}

fn action_to_row_disposition(action: &VerdictAction) -> RowDispositionKind {
    match action {
        VerdictAction::Admit | VerdictAction::AdmitAsVariant => RowDispositionKind::Accept,
        VerdictAction::Quarantine => RowDispositionKind::Quarantine,
        VerdictAction::RejectBatch => RowDispositionKind::RejectBatch,
        VerdictAction::RejectRun => RowDispositionKind::RejectRun,
    }
}

fn nested_action_for_field(
    source_name: &str,
    arrow_type: &ArrowType,
    policy: &NormalizationPolicy,
) -> Result<NestedAction> {
    if !arrow_type.is_nested() {
        return Ok(NestedAction::NotNested);
    }

    match &policy.nested {
        NestedDataPolicy::KeepNested => Ok(NestedAction::KeepNested),
        NestedDataPolicy::ChildTableExpansion { .. } => Ok(NestedAction::ExpandToChildTable {
            child_table: normalize_identifier(source_name, &policy.identifier)?,
        }),
        NestedDataPolicy::VariantCapture(spec) => Ok(NestedAction::CaptureVariant {
            column_name: spec.column_name.clone(),
            semantic: spec.semantic.clone(),
        }),
    }
}

#[cfg(test)]
mod destination_mapping_tests {
    use super::*;
    use std::sync::Arc;

    fn mapping(pattern: &str) -> TypeMapping {
        TypeMapping {
            arrow_type: pattern.to_owned(),
            destination_type: pattern.to_owned(),
            fidelity: TypeMappingFidelity::Lossless,
        }
    }

    #[test]
    fn exact_mapping_outranks_family_and_sheet_order() {
        let mappings = vec![mapping("Decimal*"), mapping("Decimal128(p,s)")];
        let selected =
            resolve_destination_type_mapping(&mappings, &DataType::Decimal128(38, 9)).unwrap();
        assert_eq!(selected.unwrap().arrow_type, "Decimal128(p,s)");
        let reversed = mappings.into_iter().rev().collect::<Vec<_>>();
        let selected =
            resolve_destination_type_mapping(&reversed, &DataType::Decimal128(38, 9)).unwrap();
        assert_eq!(selected.unwrap().arrow_type, "Decimal128(p,s)");
    }

    #[test]
    fn current_temporal_and_nested_patterns_resolve_case_insensitively() {
        let mappings = vec![
            mapping("Time32(second|millisecond)"),
            mapping("Time64(microsecond)"),
            mapping("Timestamp(second|millisecond|microsecond, none)"),
            mapping("Timestamp(*, timezone)"),
            mapping("Timestamp(Nanosecond,*)"),
            mapping("Struct/List/Map"),
        ];
        for data_type in [
            DataType::Time32(TimeUnit::Second),
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Struct(vec![Field::new("x", DataType::Int64, true)].into()),
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Int64, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
        ] {
            assert!(
                resolve_destination_type_mapping(&mappings, &data_type)
                    .unwrap()
                    .is_some(),
                "missing mapping for {data_type}"
            );
        }
    }

    #[test]
    fn equal_specificity_is_rejected_as_ambiguous() {
        let mappings = vec![mapping("Int64"), mapping(" int64 ")];
        let error = resolve_destination_type_mapping(&mappings, &DataType::Int64).unwrap_err();
        assert!(error.to_string().contains("ambiguous"));
    }

    #[test]
    fn unsupported_mapping_remains_explicit_sheet_authority() {
        let mappings = vec![TypeMapping {
            arrow_type: "Decimal*".to_owned(),
            destination_type: "DECIMAL".to_owned(),
            fidelity: TypeMappingFidelity::Unsupported,
        }];
        let selected = resolve_destination_type_mapping(&mappings, &DataType::Decimal128(38, 9))
            .unwrap()
            .unwrap();
        assert_eq!(selected.fidelity, TypeMappingFidelity::Unsupported);
    }
}
