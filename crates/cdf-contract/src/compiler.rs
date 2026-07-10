use std::collections::BTreeSet;

use arrow_schema::Field;
use cdf_kernel::{
    CdfError, ResourceDescriptor, Result, TypeMapping, TypeMappingFidelity, semantic,
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

    let row_rules = row_rule_programs(policy, observed_schema);
    Ok(ValidationProgram {
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
        .filter_map(|rule| match &rule.predicate {
            RowRulePredicate::Nullability { column } => Some(column.as_str()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    let rule_controls = row_rules
        .iter()
        .flat_map(|rule| match &rule.predicate {
            RowRulePredicate::Nullability { column }
            | RowRulePredicate::Domain { column, .. }
            | RowRulePredicate::Range { column, .. }
            | RowRulePredicate::Regex { column, .. }
            | RowRulePredicate::Freshness { column, .. } => vec![column.as_str()],
            RowRulePredicate::Dedup { keys, .. } => keys.iter().map(String::as_str).collect(),
        })
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
            predicate: RowRulePredicate::Nullability {
                column: field.source_name.clone(),
            },
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
    let predicate = match rule {
        RowRule::Nullability { column } => RowRulePredicate::Nullability {
            column: column.clone(),
        },
        RowRule::Domain { column, allowed } => RowRulePredicate::Domain {
            column: column.clone(),
            allowed: allowed.clone(),
        },
        RowRule::Range { column, min, max } => RowRulePredicate::Range {
            column: column.clone(),
            min: min.clone(),
            max: max.clone(),
        },
        RowRule::Regex { column, pattern } => RowRulePredicate::Regex {
            column: column.clone(),
            pattern: pattern.clone(),
        },
        RowRule::Freshness { column, max_age_ms } => RowRulePredicate::Freshness {
            column: column.clone(),
            max_age_ms: *max_age_ms,
        },
        RowRule::Dedup { keys, keep } => RowRulePredicate::Dedup {
            keys: keys.clone(),
            keep: match keep {
                DedupKeep::First => DedupKeepProgram::First,
                DedupKeep::Last => DedupKeepProgram::Last,
                DedupKeep::Fail => DedupKeepProgram::Fail,
            },
        },
    };
    RowRuleProgram {
        rule_id: format!("row-rule-{index:04}-{}", row_rule_kind(&predicate)),
        predicate,
        missing_column: MissingColumnBehavior::Error,
    }
}

fn row_rule_kind(predicate: &RowRulePredicate) -> &'static str {
    match predicate {
        RowRulePredicate::Nullability { .. } => "nullability",
        RowRulePredicate::Domain { .. } => "domain",
        RowRulePredicate::Range { .. } => "range",
        RowRulePredicate::Regex { .. } => "regex",
        RowRulePredicate::Freshness { .. } => "freshness",
        RowRulePredicate::Dedup { .. } => "dedup",
    }
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
