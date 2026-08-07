use std::cmp::Reverse;
use std::collections::BTreeSet;

use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit, UnionMode};
use cdf_kernel::{
    CdfError, DeduplicationSpec, DestinationSheet, ResourceDescriptor, Result, TypeMapping,
    TypeMappingFidelity,
};
use cdf_semantic::{ResolvedSemantic, SemanticAuthority, SemanticCatalog, builtin_catalog};
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
    compile_validation_program_with_semantic_catalog(policy, observed_schema, builtin_catalog()?)
}

pub fn compile_validation_program_with_semantic_catalog(
    policy: &ContractPolicy,
    observed_schema: &ObservedSchema,
    semantic_catalog: &SemanticCatalog,
) -> Result<ValidationProgram> {
    validate_normalizer(&policy.normalization.identifier)?;
    let normalized_schema = normalize_schema(observed_schema, &policy.normalization.identifier)?;
    let resolved_semantics = observed_schema
        .fields
        .iter()
        .map(|field| resolve_observed_semantic(semantic_catalog, field))
        .collect::<Result<Vec<_>>>()?;

    let mut column_programs = Vec::with_capacity(observed_schema.fields.len());
    for ((field, normalized), resolved_semantic) in observed_schema
        .fields
        .iter()
        .zip(normalized_schema.fields.iter())
        .zip(&resolved_semantics)
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
            redaction: redaction_decision_for_resolved_semantic(
                resolved_semantic.as_ref(),
                &policy.evidence.pii_redaction,
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
        admission: policy.admission.clone(),
        schema_coercion: None,
        residual: Some(residual_program(
            policy,
            observed_schema,
            &normalized_schema,
            &row_rules,
            &resolved_semantics,
        )),
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
    compile_resource_validation_program_with_semantic_catalog(
        policy,
        observed_schema,
        descriptor,
        builtin_catalog()?,
    )
}

pub fn compile_resource_validation_program_with_semantic_catalog(
    policy: &ContractPolicy,
    observed_schema: &ObservedSchema,
    descriptor: &ResourceDescriptor,
    semantic_catalog: &SemanticCatalog,
) -> Result<ValidationProgram> {
    let program = compile_validation_program_with_semantic_catalog(
        policy,
        observed_schema,
        semantic_catalog,
    )?;
    bind_validation_program_to_resource(program, descriptor)
}

pub fn bind_validation_program_to_resource(
    mut program: ValidationProgram,
    descriptor: &ResourceDescriptor,
) -> Result<ValidationProgram> {
    let admission = program.admission.clone();
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
                    assign_field_role(field, FieldRole::DestinationIdentity, &admission);
                }
            }
        }
    }
    if descriptor.write_disposition == cdf_kernel::WriteDisposition::Merge
        && !program.has_keyed_dedup_rule()
    {
        if descriptor.merge_key.is_empty() {
            return Err(CdfError::contract(
                "merge package deduplication requires at least one merge key",
            ));
        }
        program.row_rules.push(RowRuleProgram {
            rule_id: format!("row-rule-{:04}-merge-key-unique", program.row_rules.len()),
            expression: dedup_expression(
                "dedup",
                descriptor.merge_key.clone(),
                DedupKeepProgram::Fail,
            ),
            missing_column: MissingColumnBehavior::Error,
        });
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
            let role = if descriptor
                .cursor
                .as_ref()
                .is_some_and(|cursor| cursor.field == *control)
            {
                FieldRole::SourceProgress
            } else {
                FieldRole::DestinationIdentity
            };
            assign_field_role(field, role, &admission);
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
    resolved_semantics: &[Option<ResolvedSemantic>],
) -> ResidualProgram {
    let capture_allowed = policy.admission.field == FieldDisposition::CaptureVariant;
    let capture = if capture_allowed {
        let (variant_column, semantic) = match &policy.normalization.nested {
            NestedDataPolicy::VariantCapture(spec) => {
                (spec.column_name.clone(), CDF_VARIANT_SEMANTIC.to_owned())
            }
            _ => (
                VARIANT_COLUMN_NAME.to_owned(),
                CDF_VARIANT_SEMANTIC.to_owned(),
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
        default_disposition: policy.admission.field,
        pii_redaction: policy.evidence.pii_redaction.clone(),
        capture,
        fields: observed_schema
            .fields
            .iter()
            .zip(&normalized_schema.fields)
            .zip(resolved_semantics)
            .map(|((field, normalized), resolved_semantic)| {
                let required = required.contains(field.source_name.as_str())
                    || required.contains(normalized.output_name.as_str())
                    || rule_controls.contains(field.source_name.as_str())
                    || rule_controls.contains(normalized.output_name.as_str());
                let mut roles = vec![FieldRole::OrdinaryData];
                if required {
                    roles.push(FieldRole::RequiredOutput);
                }
                roles.sort();
                roles.dedup();
                let allowed_dispositions = allowed_field_dispositions(&roles);
                let disposition = admitted_field_disposition(
                    policy.admission.field,
                    policy.admission.row,
                    &allowed_dispositions,
                );
                ResidualFieldProgram {
                    source_name: field.source_name.clone(),
                    output_name: normalized.output_name.clone(),
                    roles,
                    disposition,
                    allowed_dispositions,
                    redaction: redaction_decision_for_resolved_semantic(
                        resolved_semantic.as_ref(),
                        &policy.evidence.pii_redaction,
                    ),
                }
            })
            .collect(),
    }
}
pub fn redaction_decision_for_field(
    field: &Field,
    policy: &PiiRedactionPolicy,
    authority: SemanticAuthority,
) -> Result<RedactionDecision> {
    redaction_decision_for_field_with_semantic_catalog(field, policy, authority, builtin_catalog()?)
}

pub fn redaction_decision_for_field_with_semantic_catalog(
    field: &Field,
    policy: &PiiRedactionPolicy,
    authority: SemanticAuthority,
    semantic_catalog: &SemanticCatalog,
) -> Result<RedactionDecision> {
    let resolved = semantic_catalog.resolve_field(field, authority)?;
    Ok(redaction_decision_for_resolved_semantic(
        resolved.as_ref(),
        policy,
    ))
}

pub fn redaction_decision_for_resolved_semantic(
    semantic: Option<&ResolvedSemantic>,
    policy: &PiiRedactionPolicy,
) -> RedactionDecision {
    match semantic {
        Some(resolved) if resolved.pii_class().is_some() => policy.pii_action.clone(),
        _ => policy.default_action.clone(),
    }
}

fn resolve_observed_semantic(
    catalog: &SemanticCatalog,
    field: &ObservedField,
) -> Result<Option<ResolvedSemantic>> {
    let arrow_type = field
        .canonical_arrow_type
        .as_ref()
        .ok_or_else(|| {
            CdfError::data(format!(
                "observed field {:?} is missing exact canonical Arrow type authority",
                field.name
            ))
        })?
        .to_arrow()
        .map_err(|error| {
            CdfError::data(format!(
                "observed field {:?} has invalid exact canonical Arrow type authority: {}",
                field.name, error.message
            ))
        })?;
    if ArrowType::from(&arrow_type) != field.arrow_type {
        return Err(CdfError::data(format!(
            "observed field {:?} has contradictory Arrow type authorities",
            field.name
        )));
    }
    let arrow_field = Field::new(&field.name, arrow_type, field.nullable).with_metadata(
        field
            .metadata
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    );
    catalog.resolve_field(&arrow_field, SemanticAuthority::Observed)
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

/// Validates the complete canonical output schema against one destination's declared mapping
/// authority before adapter planning or payload mutation.
///
/// Container mappings authorize the container representation; child fields are resolved
/// independently so a broad `Struct`/`List`/`Map` claim cannot hide an unsupported leaf.
pub fn validate_destination_schema_mappings(
    policy: &TypePolicy,
    sheet: &DestinationSheet,
    schema: &Schema,
) -> Result<()> {
    validate_destination_schema_mappings_with_semantic_catalog(
        policy,
        sheet,
        schema,
        builtin_catalog()?,
    )
}

pub fn validate_destination_schema_mappings_with_semantic_catalog(
    policy: &TypePolicy,
    sheet: &DestinationSheet,
    schema: &Schema,
    semantic_catalog: &SemanticCatalog,
) -> Result<()> {
    if sheet.type_mappings.is_empty() {
        return Err(CdfError::contract(format!(
            "destination {} declares no Arrow type mappings",
            sheet.destination
        )));
    }
    for field in schema.fields() {
        validate_destination_field_semantics(
            policy,
            sheet,
            field.name(),
            field.as_ref(),
            semantic_catalog,
        )?;
        validate_destination_field_mapping(policy, sheet, field.name(), field.data_type())?;
    }
    Ok(())
}

fn validate_destination_field_semantics(
    policy: &TypePolicy,
    sheet: &DestinationSheet,
    path: &str,
    field: &Field,
    catalog: &SemanticCatalog,
) -> Result<()> {
    if let Some(resolved) = catalog.resolve_field(field, SemanticAuthority::Authored)?
        && let Some(mapping) =
            catalog.resolve_destination_mapping(&resolved, field, sheet.destination.as_str())?
    {
        match mapping.fidelity {
            TypeMappingFidelity::Lossless => {}
            TypeMappingFidelity::LossyRequiresContractAllowance if policy.allow_lossy_mapping => {}
            TypeMappingFidelity::LossyRequiresContractAllowance => {
                return Err(CdfError::contract(format!(
                    "destination {} maps semantic {} on field `{path}` to {} lossily; enable `allow_lossy_mapping` only if that loss is intended",
                    sheet.destination,
                    resolved.reference(),
                    mapping.destination_type
                )));
            }
            TypeMappingFidelity::Unsupported => {
                return Err(CdfError::contract(format!(
                    "destination {} does not support semantic {} on field `{path}`",
                    sheet.destination,
                    resolved.reference()
                )));
            }
        }
    }

    match field.data_type() {
        DataType::Struct(fields) => {
            for child in fields {
                validate_destination_field_semantics(
                    policy,
                    sheet,
                    &format!("{path}.{}", child.name()),
                    child.as_ref(),
                    catalog,
                )?;
            }
        }
        DataType::List(child)
        | DataType::LargeList(child)
        | DataType::ListView(child)
        | DataType::LargeListView(child)
        | DataType::FixedSizeList(child, _) => validate_destination_field_semantics(
            policy,
            sheet,
            &format!("{path}[]"),
            child.as_ref(),
            catalog,
        )?,
        DataType::Map(entries, _) => {
            if let DataType::Struct(fields) = entries.data_type() {
                for child in fields {
                    validate_destination_field_semantics(
                        policy,
                        sheet,
                        &format!("{path}.{}", child.name()),
                        child.as_ref(),
                        catalog,
                    )?;
                }
            }
        }
        DataType::Union(fields, _) => {
            for (_, child) in fields.iter() {
                validate_destination_field_semantics(
                    policy,
                    sheet,
                    &format!("{path}.{}", child.name()),
                    child.as_ref(),
                    catalog,
                )?;
            }
        }
        DataType::RunEndEncoded(run_ends, values) => {
            for (suffix, child) in [("run_ends", run_ends), ("values", values)] {
                validate_destination_field_semantics(
                    policy,
                    sheet,
                    &format!("{path}.{suffix}"),
                    child.as_ref(),
                    catalog,
                )?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_destination_field_mapping(
    policy: &TypePolicy,
    sheet: &DestinationSheet,
    path: &str,
    data_type: &DataType,
) -> Result<()> {
    let mapping = resolve_destination_type_mapping(&sheet.type_mappings, data_type)?.ok_or_else(|| {
        CdfError::contract(format!(
            "destination {} has no declared mapping for field `{path}` with Arrow type {data_type}; add a truthful mapping to the destination sheet or cast the field to a supported type before planning",
            sheet.destination
        ))
    })?;
    match mapping.fidelity {
        TypeMappingFidelity::Lossless => {}
        TypeMappingFidelity::LossyRequiresContractAllowance if policy.allow_lossy_mapping => {}
        TypeMappingFidelity::LossyRequiresContractAllowance => {
            return Err(CdfError::contract(format!(
                "destination {} maps field `{path}` from Arrow type {data_type} to {} lossily; enable `allow_lossy_mapping` only if that loss is intended, or cast the field to a lossless supported type",
                sheet.destination, mapping.destination_type
            )));
        }
        TypeMappingFidelity::Unsupported => {
            return Err(CdfError::contract(format!(
                "destination {} does not support field `{path}` with Arrow type {data_type}; its sheet maps that type to {} as unsupported; cast the field to a supported type or choose a destination that preserves it",
                sheet.destination, mapping.destination_type
            )));
        }
    }

    match data_type {
        DataType::Struct(fields) => {
            if fields.is_empty() {
                return Err(CdfError::contract(format!(
                    "destination {} cannot map field `{path}` because an Arrow struct must contain at least one child",
                    sheet.destination
                )));
            }
            for child in fields {
                validate_destination_field_mapping(
                    policy,
                    sheet,
                    &format!("{path}.{}", child.name()),
                    child.data_type(),
                )?;
            }
        }
        DataType::FixedSizeList(_, size) if *size <= 0 => {
            return Err(CdfError::contract(format!(
                "destination {} cannot map field `{path}` because an Arrow fixed-size list must have a positive element count, found {size}",
                sheet.destination
            )));
        }
        DataType::List(child)
        | DataType::LargeList(child)
        | DataType::ListView(child)
        | DataType::LargeListView(child)
        | DataType::FixedSizeList(child, _) => validate_destination_field_mapping(
            policy,
            sheet,
            &format!("{path}[]"),
            child.data_type(),
        )?,
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(CdfError::contract(format!(
                    "destination {} cannot map field `{path}` because its Arrow map entries are not a struct<key,value>",
                    sheet.destination
                )));
            };
            if fields.len() != 2 {
                return Err(CdfError::contract(format!(
                    "destination {} cannot map field `{path}` because its Arrow map entries must contain exactly key and value fields, found {}",
                    sheet.destination,
                    fields.len()
                )));
            }
            if fields[0].is_nullable() {
                return Err(CdfError::contract(format!(
                    "destination {} cannot map field `{path}` because Arrow map keys must be non-nullable",
                    sheet.destination
                )));
            }
            for child in fields {
                validate_destination_field_mapping(
                    policy,
                    sheet,
                    &format!("{path}.{}", child.name()),
                    child.data_type(),
                )?;
            }
        }
        DataType::Dictionary(key, value) => {
            validate_destination_field_mapping(
                policy,
                sheet,
                &format!("{path}.dictionary_key"),
                key,
            )?;
            validate_destination_field_mapping(
                policy,
                sheet,
                &format!("{path}.dictionary_value"),
                value,
            )?;
        }
        DataType::Union(fields, _) => {
            if fields.is_empty() {
                return Err(CdfError::contract(format!(
                    "destination {} cannot map field `{path}` because an Arrow union must contain at least one child",
                    sheet.destination
                )));
            }
            for (_, child) in fields.iter() {
                validate_destination_field_mapping(
                    policy,
                    sheet,
                    &format!("{path}.{}", child.name()),
                    child.data_type(),
                )?;
            }
        }
        DataType::RunEndEncoded(run_ends, values) => {
            validate_destination_field_mapping(
                policy,
                sheet,
                &format!("{path}.run_ends"),
                run_ends.data_type(),
            )?;
            validate_destination_field_mapping(
                policy,
                sheet,
                &format!("{path}.values"),
                values.data_type(),
            )?;
        }
        _ => {}
    }
    Ok(())
}

fn destination_type_pattern_specificity(pattern: &str, data_type: &DataType) -> Option<u8> {
    let pattern = compact_ascii_lower(pattern);
    let display = compact_ascii_lower(&data_type.to_string());
    if pattern == display {
        return Some(100);
    }
    match data_type {
        DataType::Decimal128(precision, scale)
            if pattern == "decimal128(precision<=38,scale>=0)"
                && *precision <= 38
                && *scale >= 0 =>
        {
            Some(95)
        }
        DataType::Decimal32(precision, scale)
            if pattern == "decimal32(precision<=9,scale>=0)" && *precision <= 9 && *scale >= 0 =>
        {
            Some(95)
        }
        DataType::Decimal64(precision, scale)
            if pattern == "decimal64(precision<=18,scale>=0)"
                && *precision <= 18
                && *scale >= 0 =>
        {
            Some(95)
        }
        DataType::Decimal32(_, _) if pattern == "decimal32(p,s)" => Some(90),
        DataType::Decimal64(_, _) if pattern == "decimal64(p,s)" => Some(90),
        DataType::Decimal128(_, _) if pattern == "decimal128(p,s)" => Some(90),
        DataType::Decimal256(_, _) if pattern == "decimal256(p,s)" => Some(90),
        DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _)
            if pattern == "decimal*" =>
        {
            Some(50)
        }
        DataType::FixedSizeBinary(_) if pattern == "fixedsizebinary(*)" => Some(90),
        DataType::Time32(TimeUnit::Second | TimeUnit::Millisecond)
            if pattern == "time32(second|millisecond)" =>
        {
            Some(70)
        }
        DataType::Time32(_) if pattern == "time32" => Some(50),
        DataType::Time64(TimeUnit::Microsecond) if pattern == "time64(microsecond)" => Some(90),
        DataType::Time64(TimeUnit::Nanosecond) if pattern == "time64(nanosecond)" => Some(90),
        DataType::Time64(_) if pattern == "time64" => Some(50),
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
                _ if pattern == "timestamp(*,*)" => Some(40),
                _ => None,
            }
        }
        DataType::Struct(_) if pattern == "struct" => Some(85),
        DataType::List(_) if pattern == "list" => Some(85),
        DataType::LargeList(_) if pattern == "largelist" => Some(85),
        DataType::FixedSizeList(_, _) if pattern == "fixedsizelist" => Some(85),
        DataType::ListView(_) if pattern == "listview" => Some(85),
        DataType::LargeListView(_) if pattern == "largelistview" => Some(85),
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _)
            if pattern == "list*" =>
        {
            Some(60)
        }
        DataType::Map(_, _) if pattern == "map" => Some(85),
        DataType::Union(_, UnionMode::Sparse) if pattern == "union(sparse)" => Some(90),
        DataType::Union(_, UnionMode::Dense) if pattern == "union(dense)" => Some(90),
        DataType::Union(_, _) if pattern == "union" => Some(85),
        DataType::Dictionary(_, _) if pattern == "dictionary" => Some(85),
        DataType::Duration(TimeUnit::Second | TimeUnit::Millisecond | TimeUnit::Microsecond)
            if pattern == "duration(second|millisecond|microsecond)" =>
        {
            Some(90)
        }
        DataType::Duration(TimeUnit::Nanosecond) if pattern == "duration(nanosecond)" => Some(90),
        DataType::Duration(_) if pattern == "duration" => Some(85),
        DataType::Interval(IntervalUnit::YearMonth | IntervalUnit::DayTime)
            if pattern == "interval(yearmonth|daytime)" =>
        {
            Some(90)
        }
        DataType::Interval(IntervalUnit::MonthDayNano) if pattern == "interval(monthdaynano)" => {
            Some(90)
        }
        DataType::Interval(_) if pattern == "interval" => Some(85),
        DataType::RunEndEncoded(_, _) if pattern == "runendencoded" => Some(85),
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

fn row_dispositions(policy: &ContractPolicy) -> Vec<RowDispositionRule> {
    let violation = match policy.admission.row {
        RowViolationDisposition::QuarantineRow => RowDispositionKind::Quarantine,
        RowViolationDisposition::FailRun => RowDispositionKind::RejectRun,
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
            disposition: RowDispositionKind::RejectRun,
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
            crate::DeclarativeExpression::call(
                "in_domain",
                vec![
                    crate::DeclarativeExpressionNode::Column {
                        name: column.clone(),
                    },
                    crate::DeclarativeExpressionNode::Literal {
                        value: crate::DeclarativeExpressionLiteral::StringList(allowed.clone()),
                    },
                ],
            ),
        ),
        RowRule::Range { column, min, max } => (
            "range",
            crate::DeclarativeExpression::call(
                "in_range",
                vec![
                    crate::DeclarativeExpressionNode::Column {
                        name: column.clone(),
                    },
                    optional_bound(min),
                    optional_bound(max),
                ],
            ),
        ),
        RowRule::Regex { column, pattern } => (
            "regex",
            crate::DeclarativeExpression::call(
                "matches_regex",
                vec![
                    crate::DeclarativeExpressionNode::Column {
                        name: column.clone(),
                    },
                    crate::DeclarativeExpressionNode::Literal {
                        value: crate::DeclarativeExpressionLiteral::String(pattern.clone()),
                    },
                ],
            ),
        ),
        RowRule::Freshness { column, max_age_ms } => (
            "freshness",
            crate::DeclarativeExpression::call(
                "fresh_within",
                vec![
                    crate::DeclarativeExpressionNode::Column {
                        name: column.clone(),
                    },
                    crate::DeclarativeExpressionNode::Literal {
                        value: crate::DeclarativeExpressionLiteral::Unsigned(*max_age_ms),
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

fn unary_column_expression(function: &str, column: &str) -> crate::DeclarativeExpression {
    crate::DeclarativeExpression::call(
        function,
        vec![crate::DeclarativeExpressionNode::Column {
            name: column.to_owned(),
        }],
    )
}

fn optional_bound(value: &Option<String>) -> crate::DeclarativeExpressionNode {
    crate::DeclarativeExpressionNode::Literal {
        value: value
            .clone()
            .map(crate::DeclarativeExpressionLiteral::String)
            .unwrap_or(crate::DeclarativeExpressionLiteral::Null),
    }
}

fn dedup_expression(
    function: &str,
    keys: Vec<String>,
    keep: DedupKeepProgram,
) -> crate::DeclarativeExpression {
    let keep = match keep {
        DedupKeepProgram::First => "first",
        DedupKeepProgram::Last => "last",
        DedupKeepProgram::Fail => "fail",
    };
    crate::DeclarativeExpression::call(
        function,
        vec![
            crate::DeclarativeExpressionNode::Literal {
                value: crate::DeclarativeExpressionLiteral::StringList(keys),
            },
            crate::DeclarativeExpressionNode::Literal {
                value: crate::DeclarativeExpressionLiteral::String(keep.to_owned()),
            },
        ],
    )
}

fn allowed_field_dispositions(roles: &[FieldRole]) -> Vec<FieldDisposition> {
    if roles.iter().any(|role| {
        matches!(
            role,
            FieldRole::DestinationIdentity
                | FieldRole::SourceProgress
                | FieldRole::CdcOperation
                | FieldRole::TransactionBoundary
        )
    }) {
        return vec![FieldDisposition::FailRun];
    }
    if roles.contains(&FieldRole::RequiredOutput) {
        return vec![FieldDisposition::QuarantineRow, FieldDisposition::FailRun];
    }
    vec![
        FieldDisposition::CaptureVariant,
        FieldDisposition::QuarantineRow,
        FieldDisposition::FailRun,
    ]
}

fn admitted_field_disposition(
    configured: FieldDisposition,
    row: RowViolationDisposition,
    allowed: &[FieldDisposition],
) -> FieldDisposition {
    let requested = if allowed.contains(&FieldDisposition::CaptureVariant) {
        configured
    } else {
        match row {
            RowViolationDisposition::QuarantineRow => FieldDisposition::QuarantineRow,
            RowViolationDisposition::FailRun => FieldDisposition::FailRun,
        }
    };
    if allowed.contains(&requested) {
        requested
    } else {
        FieldDisposition::FailRun
    }
}

fn assign_field_role(
    field: &mut ResidualFieldProgram,
    role: FieldRole,
    admission: &AdmissionPolicy,
) {
    if !field.roles.contains(&role) {
        field.roles.push(role);
        field.roles.sort();
    }
    field.allowed_dispositions = allowed_field_dispositions(&field.roles);
    field.disposition =
        admitted_field_disposition(admission.field, admission.row, &field.allowed_dispositions);
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
            semantic: CDF_VARIANT_SEMANTIC.to_owned(),
        }),
    }
}

#[cfg(test)]
mod destination_mapping_tests {
    use super::*;
    use std::sync::Arc;

    use cdf_kernel::{
        CapabilitySupport, ConcurrencyLimit, DestinationId, IdempotencySupport, IdentifierRules,
        TransactionSupport, WriteDisposition,
    };

    fn mapping(pattern: &str) -> TypeMapping {
        TypeMapping {
            arrow_type: pattern.to_owned(),
            destination_type: pattern.to_owned(),
            fidelity: TypeMappingFidelity::Lossless,
        }
    }

    fn sheet(mappings: Vec<TypeMapping>) -> DestinationSheet {
        DestinationSheet {
            destination: DestinationId::new("plausible").unwrap(),
            supported_dispositions: vec![WriteDisposition::Append],
            transactions: TransactionSupport::AtomicPackage,
            idempotency: IdempotencySupport::PackageToken,
            type_mappings: mappings,
            identifier_rules: IdentifierRules {
                normalizer: "namecase-v1".to_owned(),
                max_length: None,
                allowed_pattern: None,
            },
            migration_support: CapabilitySupport::Supported,
            quarantine_tables: CapabilitySupport::Unsupported,
            concurrency: ConcurrencyLimit {
                max_writers: Some(1),
            },
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
            mapping("Struct"),
            mapping("List"),
            mapping("Map"),
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

    #[test]
    fn recursive_schema_mapping_names_the_unsupported_nested_leaf() {
        let mappings = vec![
            mapping("Struct"),
            mapping("List"),
            mapping("Int64"),
            TypeMapping {
                arrow_type: "Timestamp(*, timezone)".to_owned(),
                destination_type: "TIMESTAMPTZ".to_owned(),
                fidelity: TypeMappingFidelity::Unsupported,
            },
        ];
        let schema = Schema::new(vec![Field::new(
            "event",
            DataType::Struct(
                vec![Field::new(
                    "history",
                    DataType::List(Arc::new(Field::new(
                        "item",
                        DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                        true,
                    ))),
                    true,
                )]
                .into(),
            ),
            true,
        )]);
        let error = validate_destination_schema_mappings(
            &TypePolicy::strict_fidelity(),
            &sheet(mappings),
            &schema,
        )
        .unwrap_err();
        let message = error.to_string();
        assert!(message.contains("event.history[]"), "{message}");
        assert!(message.contains("Timestamp(µs, \"+00:00\")"), "{message}");
        assert!(message.contains("TIMESTAMPTZ"), "{message}");
        assert!(message.contains("unsupported"), "{message}");
    }

    #[test]
    fn lossy_mapping_requires_the_recorded_allowance() {
        let sheet = sheet(vec![TypeMapping {
            arrow_type: "Timestamp(Nanosecond,*)".to_owned(),
            destination_type: "TIMESTAMPTZ".to_owned(),
            fidelity: TypeMappingFidelity::LossyRequiresContractAllowance,
        }]);
        let schema = Schema::new(vec![Field::new(
            "observed_at",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        )]);
        let error =
            validate_destination_schema_mappings(&TypePolicy::strict_fidelity(), &sheet, &schema)
                .unwrap_err();
        assert!(error.to_string().contains("allow_lossy_mapping"));

        let mut allowed = TypePolicy::strict_fidelity();
        allowed.allow_lossy_mapping = true;
        validate_destination_schema_mappings(&allowed, &sheet, &schema).unwrap();
    }

    #[test]
    fn constrained_decimal_mapping_outranks_generic_rejection() {
        let sheet = sheet(vec![
            TypeMapping {
                arrow_type: "Decimal128(precision<=38, scale>=0)".to_owned(),
                destination_type: "DECIMAL".to_owned(),
                fidelity: TypeMappingFidelity::Lossless,
            },
            TypeMapping {
                arrow_type: "Decimal128(p,s)".to_owned(),
                destination_type: "DECIMAL".to_owned(),
                fidelity: TypeMappingFidelity::Unsupported,
            },
        ]);
        validate_destination_schema_mappings(
            &TypePolicy::strict_fidelity(),
            &sheet,
            &Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal128(38, 9),
                false,
            )]),
        )
        .unwrap();
        let error = validate_destination_schema_mappings(
            &TypePolicy::strict_fidelity(),
            &sheet,
            &Schema::new(vec![Field::new(
                "amount",
                DataType::Decimal128(39, 9),
                false,
            )]),
        )
        .unwrap_err();
        assert!(error.to_string().contains("unsupported"));
    }
}
