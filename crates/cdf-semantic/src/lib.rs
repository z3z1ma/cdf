#![cfg_attr(
    not(test),
    deny(clippy::expect_used, clippy::panic, clippy::unwrap_used)
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::OnceLock,
};

use arrow_schema::{DataType, Field};
pub use cdf_kernel::CDF_PACKAGE_ROW_ORDINAL_SEMANTIC;
use cdf_kernel::{
    CdfError, Result, SemanticParameterValue, SemanticReference, TypeMappingFidelity,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const DEFINITION_SCHEMA_VERSION: u32 = 1;
pub const CDF_VARIANT_SEMANTIC: &str = "cdf.variant@1";
pub const POSTGRES_JSON_TEXT_SEMANTIC: &str = "postgres.json_text@1";
pub const POSTGRES_JSONB_TEXT_SEMANTIC: &str = "postgres.jsonb_text@1";
pub const POSTGRES_NUMERIC_TEXT_SEMANTIC: &str = "postgres.numeric_text@1";

pub const POSTGRES_JSON_TEXT_MAPPING_PROFILE: &str = "postgres_exact_json_text_v1";
pub const POSTGRES_JSONB_TEXT_MAPPING_PROFILE: &str = "postgres_exact_jsonb_text_v1";
pub const POSTGRES_NUMERIC_TEXT_MAPPING_PROFILE: &str = "postgres_exact_numeric_text_v1";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SemanticAuthority {
    Authored,
    Observed,
    Compiled,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticDefinition {
    pub definition_schema_version: u32,
    pub namespace: String,
    pub name: String,
    pub version: u32,
    pub description: String,
    pub owning_namespace: String,
    pub supersedes: Option<String>,
    pub deprecated: bool,
    pub arrow_patterns: Vec<ArrowPattern>,
    pub nullability: SemanticNullability,
    pub parameters: BTreeMap<String, ParameterDefinition>,
    pub required_metadata: Vec<MetadataRequirement>,
    pub validation: Vec<ValidationPredicate>,
    pub privacy: PrivacyClassification,
    pub equivalence: Vec<EquivalenceRule>,
    pub casts: Vec<CastRule>,
    pub destination_mappings: Vec<DestinationMapping>,
    pub base_arrow_fallback: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArrowPattern {
    Any,
    Family { family: ArrowTypeFamily },
    Exact { arrow_type: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArrowTypeFamily {
    Boolean,
    SignedInteger,
    UnsignedInteger,
    Float,
    Decimal,
    Utf8,
    Binary,
    Date,
    Time,
    Timestamp,
    Duration,
    Interval,
    Struct,
    List,
    Map,
    Dictionary,
    Union,
}

impl ArrowPattern {
    fn matches(&self, data_type: &DataType) -> bool {
        match self {
            Self::Any => true,
            Self::Family { family } => family.matches(data_type),
            Self::Exact { arrow_type } => cdf_kernel::parse_arrow_field_type(arrow_type)
                .is_ok_and(|expected| &expected == data_type),
        }
    }

    fn specificity(&self) -> u8 {
        match self {
            Self::Any => 0,
            Self::Family { .. } => 1,
            Self::Exact { .. } => 2,
        }
    }
}

impl ArrowTypeFamily {
    fn matches(&self, data_type: &DataType) -> bool {
        match self {
            Self::Boolean => matches!(data_type, DataType::Boolean),
            Self::SignedInteger => matches!(
                data_type,
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            ),
            Self::UnsignedInteger => matches!(
                data_type,
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
            ),
            Self::Float => matches!(
                data_type,
                DataType::Float16 | DataType::Float32 | DataType::Float64
            ),
            Self::Decimal => matches!(
                data_type,
                DataType::Decimal32(_, _)
                    | DataType::Decimal64(_, _)
                    | DataType::Decimal128(_, _)
                    | DataType::Decimal256(_, _)
            ),
            Self::Utf8 => matches!(
                data_type,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ),
            Self::Binary => matches!(
                data_type,
                DataType::Binary
                    | DataType::LargeBinary
                    | DataType::BinaryView
                    | DataType::FixedSizeBinary(_)
            ),
            Self::Date => matches!(data_type, DataType::Date32 | DataType::Date64),
            Self::Time => matches!(data_type, DataType::Time32(_) | DataType::Time64(_)),
            Self::Timestamp => matches!(data_type, DataType::Timestamp(_, _)),
            Self::Duration => matches!(data_type, DataType::Duration(_)),
            Self::Interval => matches!(data_type, DataType::Interval(_)),
            Self::Struct => matches!(data_type, DataType::Struct(_)),
            Self::List => matches!(
                data_type,
                DataType::List(_)
                    | DataType::LargeList(_)
                    | DataType::FixedSizeList(_, _)
                    | DataType::ListView(_)
                    | DataType::LargeListView(_)
            ),
            Self::Map => matches!(data_type, DataType::Map(_, _)),
            Self::Dictionary => matches!(data_type, DataType::Dictionary(_, _)),
            Self::Union => matches!(data_type, DataType::Union(_, _)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticNullability {
    Any,
    Nullable,
    NonNullable,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParameterDefinition {
    pub kind: ParameterKind,
    pub required: bool,
    pub format: ParameterFormat,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_values: Vec<SemanticParameterValue>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParameterKind {
    String,
    Number,
    Boolean,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParameterFormat {
    Any,
    LowerSnakeIdentifier,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataRequirement {
    pub key: String,
    pub predicate: MetadataPredicate,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MetadataPredicate {
    Exact { value: String },
    AsciiCaseInsensitiveExact { value: String },
    SqlTypeFamily { base_names: Vec<String> },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ValidationPredicate {
    NonEmptyStringParameter { parameter: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PrivacyClassification {
    Ordinary,
    Pii { class_parameter: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EquivalenceRule {
    pub semantic: String,
    pub parameter_identity: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CastRule {
    pub destination_semantic: String,
    pub fidelity: SemanticCastFidelity,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticCastFidelity {
    Lossless,
    LossyRequiresContractAllowance,
    Unsupported,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationMapping {
    pub destination: String,
    pub arrow_pattern: ArrowPattern,
    pub parameter_equals: BTreeMap<String, SemanticParameterValue>,
    pub required_metadata: Vec<MetadataRequirement>,
    pub mapping_profile: String,
    pub destination_type: String,
    pub fidelity: TypeMappingFidelity,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisteredSemanticDefinition {
    pub definition: SemanticDefinition,
    pub definition_hash: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedSemantic {
    reference: SemanticReference,
    registered: RegisteredSemanticDefinition,
}

impl ResolvedSemantic {
    pub fn reference(&self) -> &SemanticReference {
        &self.reference
    }

    pub fn definition(&self) -> &SemanticDefinition {
        &self.registered.definition
    }

    pub fn definition_hash(&self) -> &str {
        &self.registered.definition_hash
    }

    pub fn pii_class(&self) -> Option<&str> {
        match &self.registered.definition.privacy {
            PrivacyClassification::Ordinary => None,
            PrivacyClassification::Pii { class_parameter } => self
                .reference
                .parameter(class_parameter)
                .and_then(SemanticParameterValue::as_str),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SemanticCatalog {
    definitions: BTreeMap<(String, String, u32), RegisteredSemanticDefinition>,
}

impl SemanticCatalog {
    pub fn new(definitions: Vec<SemanticDefinition>) -> Result<Self> {
        let mut catalog = Self {
            definitions: BTreeMap::new(),
        };
        for definition in definitions {
            validate_definition(&definition)?;
            let key = (
                definition.namespace.clone(),
                definition.name.clone(),
                definition.version,
            );
            let definition_hash = definition_hash(&definition)?;
            if catalog
                .definitions
                .insert(
                    key.clone(),
                    RegisteredSemanticDefinition {
                        definition,
                        definition_hash,
                    },
                )
                .is_some()
            {
                return Err(CdfError::contract(format!(
                    "duplicate semantic definition {}.{}@{}",
                    key.0, key.1, key.2
                )));
            }
        }
        Ok(catalog)
    }

    pub fn builtins() -> Result<Self> {
        builtin_catalog().cloned()
    }

    pub fn with_builtins(additional: Vec<SemanticDefinition>) -> Result<Self> {
        let mut definitions = builtin_catalog()?
            .definitions()
            .map(|registered| registered.definition.clone())
            .collect::<Vec<_>>();
        definitions.extend(additional);
        Self::new(definitions)
    }

    fn build_builtins() -> Result<Self> {
        Self::new(builtin_definitions()).map_err(|error| {
            CdfError::internal(format!(
                "CDF built-in semantic registry is invalid: {}",
                error.message
            ))
        })
    }

    pub fn definitions(&self) -> impl Iterator<Item = &RegisteredSemanticDefinition> {
        self.definitions.values()
    }

    pub fn parse_reference(
        &self,
        value: &str,
        authority: SemanticAuthority,
    ) -> Result<SemanticReference> {
        value.parse().map_err(|error| {
            authority_error(
                authority,
                format!("invalid semantic reference {value:?}: {error}"),
            )
        })
    }

    pub fn resolve_reference(
        &self,
        reference: &SemanticReference,
        authority: SemanticAuthority,
    ) -> Result<ResolvedSemantic> {
        let key = (
            reference.namespace().to_owned(),
            reference.name().to_owned(),
            reference.version(),
        );
        let registered = self.definitions.get(&key).ok_or_else(|| {
            authority_error(
                authority,
                format!(
                    "semantic definition {}.{}@{} is not present in the compiled registry",
                    key.0, key.1, key.2
                ),
            )
        })?;
        validate_parameters(reference, &registered.definition, authority)?;
        Ok(ResolvedSemantic {
            reference: reference.clone(),
            registered: registered.clone(),
        })
    }

    pub fn resolve_field(
        &self,
        field: &Field,
        authority: SemanticAuthority,
    ) -> Result<Option<ResolvedSemantic>> {
        let Some(raw) = cdf_kernel::semantic(field) else {
            return Ok(None);
        };
        let reference = self.parse_reference(raw, authority)?;
        let resolved = self.resolve_reference(&reference, authority)?;
        validate_field(&resolved, field, authority)?;
        Ok(Some(resolved))
    }

    pub fn apply_reference(
        &self,
        field: Field,
        value: &str,
        authority: SemanticAuthority,
    ) -> Result<Field> {
        let reference = self.parse_reference(value, authority)?;
        let resolved = self.resolve_reference(&reference, authority)?;
        validate_field(&resolved, &field, authority)?;
        Ok(cdf_kernel::with_semantic(field, &reference))
    }

    pub fn resolve_destination_mapping(
        &self,
        resolved: &ResolvedSemantic,
        field: &Field,
        destination: &str,
    ) -> Result<Option<DestinationMapping>> {
        let mut candidates = resolved
            .definition()
            .destination_mappings
            .iter()
            .filter(|mapping| mapping.destination == destination)
            .filter(|mapping| mapping.arrow_pattern.matches(field.data_type()))
            .filter(|mapping| parameters_match(&mapping.parameter_equals, resolved.reference()))
            .filter(|mapping| metadata_matches(&mapping.required_metadata, field))
            .map(|mapping| (mapping_specificity(mapping), mapping))
            .collect::<Vec<_>>();
        candidates.sort_by_key(|(specificity, _)| *specificity);
        let Some((specificity, selected)) = candidates.pop() else {
            if resolved.definition().base_arrow_fallback {
                return Ok(None);
            }
            return Err(CdfError::contract(format!(
                "semantic {} has no valid mapping for destination {destination:?}",
                resolved.reference()
            )));
        };
        if candidates
            .last()
            .is_some_and(|(candidate_specificity, _)| candidate_specificity == &specificity)
        {
            return Err(CdfError::contract(format!(
                "semantic {} has ambiguous equally specific mappings for destination {destination:?}",
                resolved.reference()
            )));
        }
        Ok(Some(selected.clone()))
    }
}

pub fn builtin_reference(value: &str) -> Result<SemanticReference> {
    let catalog = builtin_catalog()?;
    let reference = catalog.parse_reference(value, SemanticAuthority::Compiled)?;
    catalog.resolve_reference(&reference, SemanticAuthority::Compiled)?;
    Ok(reference)
}

pub fn builtin_catalog() -> Result<&'static SemanticCatalog> {
    static CATALOG: OnceLock<Result<SemanticCatalog>> = OnceLock::new();
    match CATALOG.get_or_init(SemanticCatalog::build_builtins) {
        Ok(catalog) => Ok(catalog),
        Err(error) => Err(error.clone()),
    }
}

fn validate_definition(definition: &SemanticDefinition) -> Result<()> {
    if definition.definition_schema_version != DEFINITION_SCHEMA_VERSION {
        return Err(CdfError::contract(format!(
            "semantic definition {}.{}@{} has unsupported definition schema version {}",
            definition.namespace,
            definition.name,
            definition.version,
            definition.definition_schema_version
        )));
    }
    let identity = SemanticReference::new(
        definition.namespace.clone(),
        definition.name.clone(),
        definition.version,
        BTreeMap::new(),
    )
    .map_err(|error| {
        CdfError::contract(format!("invalid semantic definition identity: {error}"))
    })?;
    if definition.description.trim().is_empty() || definition.owning_namespace.trim().is_empty() {
        return Err(CdfError::contract(format!(
            "semantic definition {identity} requires description and owning namespace"
        )));
    }
    if definition.arrow_patterns.is_empty() {
        return Err(CdfError::contract(format!(
            "semantic definition {identity} requires at least one Arrow pattern"
        )));
    }
    for pattern in &definition.arrow_patterns {
        validate_arrow_pattern(&identity, pattern)?;
    }
    for (parameter, schema) in &definition.parameters {
        validate_parameter_identifier(parameter)?;
        let mut allowed = BTreeSet::new();
        for value in &schema.allowed_values {
            validate_parameter_value(&identity, parameter, schema, value)?;
            if !allowed.insert(value) {
                return Err(CdfError::contract(format!(
                    "semantic definition {identity} parameter {parameter:?} repeats an allowed value"
                )));
            }
        }
    }
    validate_metadata_requirements(&identity, &definition.required_metadata)?;
    for predicate in &definition.validation {
        match predicate {
            ValidationPredicate::NonEmptyStringParameter { parameter } => {
                require_string_parameter(&identity, definition, parameter, false)?;
            }
        }
    }
    if let PrivacyClassification::Pii { class_parameter } = &definition.privacy {
        require_string_parameter(&identity, definition, class_parameter, true)?;
    }
    if let Some(supersedes) = &definition.supersedes {
        validate_related_semantic_reference(&identity, "supersedes", supersedes)?;
    }
    for equivalence in &definition.equivalence {
        validate_related_semantic_reference(&identity, "equivalence", &equivalence.semantic)?;
        for parameter in &equivalence.parameter_identity {
            if !definition.parameters.contains_key(parameter) {
                return Err(CdfError::contract(format!(
                    "semantic definition {identity} equivalence references undeclared parameter {parameter:?}"
                )));
            }
        }
    }
    for cast in &definition.casts {
        validate_related_semantic_reference(&identity, "cast", &cast.destination_semantic)?;
    }
    let mut mapping_identities = BTreeSet::new();
    for mapping in &definition.destination_mappings {
        if mapping.destination.trim().is_empty()
            || mapping.mapping_profile.trim().is_empty()
            || mapping.destination_type.trim().is_empty()
        {
            return Err(CdfError::contract(format!(
                "semantic definition {identity} has an incomplete destination mapping"
            )));
        }
        validate_arrow_pattern(&identity, &mapping.arrow_pattern)?;
        validate_metadata_requirements(&identity, &mapping.required_metadata)?;
        for (parameter, value) in &mapping.parameter_equals {
            let schema = definition.parameters.get(parameter).ok_or_else(|| {
                CdfError::contract(format!(
                    "semantic definition {identity} mapping references undeclared parameter {parameter:?}"
                ))
            })?;
            validate_parameter_value(&identity, parameter, schema, value)?;
        }
        let mapping_identity = serde_json::to_string(mapping).map_err(|error| {
            CdfError::internal(format!("semantic mapping serialization failed: {error}"))
        })?;
        if !mapping_identities.insert(mapping_identity) {
            return Err(CdfError::contract(format!(
                "semantic definition {identity} contains a duplicate destination mapping"
            )));
        }
    }
    Ok(())
}

fn validate_arrow_pattern(identity: &SemanticReference, pattern: &ArrowPattern) -> Result<()> {
    if let ArrowPattern::Exact { arrow_type } = pattern {
        cdf_kernel::parse_arrow_field_type(arrow_type).map_err(|error| {
            CdfError::contract(format!(
                "semantic definition {identity} has invalid exact Arrow pattern {arrow_type:?}: {}",
                error.message
            ))
        })?;
    }
    Ok(())
}

fn validate_parameter_value(
    identity: &SemanticReference,
    parameter: &str,
    schema: &ParameterDefinition,
    value: &SemanticParameterValue,
) -> Result<()> {
    value.validate().map_err(|error| {
        CdfError::contract(format!(
            "semantic definition {identity} parameter {parameter:?} contains a noncanonical value: {error}"
        ))
    })?;
    if parameter_value_kind(value) != schema.kind {
        return Err(CdfError::contract(format!(
            "semantic definition {identity} parameter {parameter:?} contains a value of the wrong kind"
        )));
    }
    if matches!(schema.format, ParameterFormat::LowerSnakeIdentifier)
        && value.as_str().is_some_and(|value| !is_lower_snake(value))
    {
        return Err(CdfError::contract(format!(
            "semantic definition {identity} parameter {parameter:?} contains a value that is not a lowercase snake identifier"
        )));
    }
    if !schema.allowed_values.is_empty() && !schema.allowed_values.contains(value) {
        return Err(CdfError::contract(format!(
            "semantic definition {identity} mapping contains a disallowed value for parameter {parameter:?}"
        )));
    }
    Ok(())
}

fn require_string_parameter(
    identity: &SemanticReference,
    definition: &SemanticDefinition,
    parameter: &str,
    required: bool,
) -> Result<()> {
    let schema = definition.parameters.get(parameter).ok_or_else(|| {
        CdfError::contract(format!(
            "semantic definition {identity} references undeclared string parameter {parameter:?}"
        ))
    })?;
    if schema.kind != ParameterKind::String || (required && !schema.required) {
        return Err(CdfError::contract(format!(
            "semantic definition {identity} requires {parameter:?} to be a{} string parameter",
            if required { " required" } else { "" }
        )));
    }
    Ok(())
}

fn validate_related_semantic_reference(
    identity: &SemanticReference,
    relationship: &str,
    value: &str,
) -> Result<()> {
    value.parse::<SemanticReference>().map_err(|error| {
        CdfError::contract(format!(
            "semantic definition {identity} has invalid {relationship} reference {value:?}: {error}"
        ))
    })?;
    Ok(())
}

fn validate_metadata_requirements(
    identity: &SemanticReference,
    requirements: &[MetadataRequirement],
) -> Result<()> {
    let mut keys = BTreeSet::new();
    for requirement in requirements {
        if requirement.key.trim().is_empty() {
            return Err(CdfError::contract(format!(
                "semantic definition {identity} has a metadata requirement with an empty key"
            )));
        }
        if !keys.insert(&requirement.key) {
            return Err(CdfError::contract(format!(
                "semantic definition {identity} repeats metadata requirement {:?}",
                requirement.key
            )));
        }
        if let MetadataPredicate::SqlTypeFamily { base_names } = &requirement.predicate {
            let mut names = BTreeSet::new();
            if base_names.is_empty()
                || base_names
                    .iter()
                    .any(|name| name.trim().is_empty() || !names.insert(name.to_ascii_lowercase()))
            {
                return Err(CdfError::contract(format!(
                    "semantic definition {identity} has an empty or duplicate SQL type-family metadata predicate"
                )));
            }
        }
    }
    Ok(())
}

fn validate_parameters(
    reference: &SemanticReference,
    definition: &SemanticDefinition,
    authority: SemanticAuthority,
) -> Result<()> {
    for parameter in reference.parameters().keys() {
        if !definition.parameters.contains_key(parameter) {
            return Err(authority_error(
                authority,
                format!("semantic {reference} has unknown parameter {parameter:?}"),
            ));
        }
    }
    for (name, expected) in &definition.parameters {
        let actual = reference.parameter(name);
        if expected.required && actual.is_none() {
            return Err(authority_error(
                authority,
                format!("semantic {reference} requires parameter {name:?}"),
            ));
        }
        let Some(actual) = actual else {
            continue;
        };
        let kind_matches = matches!(
            (&expected.kind, actual),
            (ParameterKind::String, SemanticParameterValue::String(_))
                | (ParameterKind::Number, SemanticParameterValue::Number(_))
                | (ParameterKind::Boolean, SemanticParameterValue::Boolean(_))
        );
        if !kind_matches {
            return Err(authority_error(
                authority,
                format!(
                    "semantic {reference} parameter {name:?} must be {}, not {}",
                    parameter_kind_name(&expected.kind),
                    actual.kind_name()
                ),
            ));
        }
        if matches!(expected.format, ParameterFormat::LowerSnakeIdentifier)
            && actual.as_str().is_some_and(|value| !is_lower_snake(value))
        {
            return Err(authority_error(
                authority,
                format!(
                    "semantic {reference} parameter {name:?} must be a lowercase snake identifier"
                ),
            ));
        }
        if !expected.allowed_values.is_empty() && !expected.allowed_values.contains(actual) {
            return Err(authority_error(
                authority,
                format!(
                    "semantic {reference} parameter {name:?} is not one of the definition's allowed values"
                ),
            ));
        }
    }
    for predicate in &definition.validation {
        match predicate {
            ValidationPredicate::NonEmptyStringParameter { parameter }
                if reference
                    .parameter(parameter)
                    .and_then(SemanticParameterValue::as_str)
                    .is_none_or(str::is_empty) =>
            {
                return Err(authority_error(
                    authority,
                    format!(
                        "semantic {reference} parameter {parameter:?} must be a non-empty string"
                    ),
                ));
            }
            ValidationPredicate::NonEmptyStringParameter { .. } => {}
        }
    }
    Ok(())
}

fn validate_field(
    resolved: &ResolvedSemantic,
    field: &Field,
    authority: SemanticAuthority,
) -> Result<()> {
    let definition = resolved.definition();
    if !definition
        .arrow_patterns
        .iter()
        .any(|pattern| pattern.matches(field.data_type()))
    {
        return Err(authority_error(
            authority,
            format!(
                "semantic {} is incompatible with Arrow field {:?} type {:?}",
                resolved.reference(),
                field.name(),
                field.data_type()
            ),
        ));
    }
    let nullable = match definition.nullability {
        SemanticNullability::Any => true,
        SemanticNullability::Nullable => field.is_nullable(),
        SemanticNullability::NonNullable => !field.is_nullable(),
    };
    if !nullable {
        return Err(authority_error(
            authority,
            format!(
                "semantic {} has incompatible nullability for Arrow field {:?}",
                resolved.reference(),
                field.name()
            ),
        ));
    }
    if !metadata_matches(&definition.required_metadata, field) {
        return Err(authority_error(
            authority,
            format!(
                "semantic {} metadata prerequisites are not satisfied for Arrow field {:?}",
                resolved.reference(),
                field.name()
            ),
        ));
    }
    Ok(())
}

fn metadata_matches(requirements: &[MetadataRequirement], field: &Field) -> bool {
    requirements.iter().all(|requirement| {
        field
            .metadata()
            .get(&requirement.key)
            .is_some_and(|actual| match &requirement.predicate {
                MetadataPredicate::Exact { value } => actual == value,
                MetadataPredicate::AsciiCaseInsensitiveExact { value } => {
                    actual.eq_ignore_ascii_case(value)
                }
                MetadataPredicate::SqlTypeFamily { base_names } => {
                    let trimmed = actual.trim();
                    let base = trimmed
                        .split_once('(')
                        .map_or(trimmed, |(base, _)| base)
                        .trim();
                    base_names
                        .iter()
                        .any(|candidate| base.eq_ignore_ascii_case(candidate))
                }
            })
    })
}

fn parameters_match(
    expected: &BTreeMap<String, SemanticParameterValue>,
    reference: &SemanticReference,
) -> bool {
    expected
        .iter()
        .all(|(key, value)| reference.parameter(key) == Some(value))
}

fn mapping_specificity(mapping: &DestinationMapping) -> usize {
    usize::from(mapping.arrow_pattern.specificity())
        + mapping.parameter_equals.len()
        + mapping.required_metadata.len()
}

fn definition_hash(definition: &SemanticDefinition) -> Result<String> {
    let bytes = serde_json::to_vec(definition).map_err(|error| {
        CdfError::internal(format!("semantic definition serialization failed: {error}"))
    })?;
    Ok(format!("sha256:{}", hex::encode(Sha256::digest(bytes))))
}

fn authority_error(authority: SemanticAuthority, message: impl Into<String>) -> CdfError {
    match authority {
        SemanticAuthority::Authored => CdfError::contract(message),
        SemanticAuthority::Observed => CdfError::data(message),
        SemanticAuthority::Compiled => CdfError::internal(message),
    }
}

fn validate_parameter_identifier(value: &str) -> Result<()> {
    if is_lower_snake(value) {
        Ok(())
    } else {
        Err(CdfError::contract(format!(
            "semantic parameter name {value:?} must be a lowercase snake identifier"
        )))
    }
}

fn is_lower_snake(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_lowercase())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn parameter_kind_name(kind: &ParameterKind) -> &'static str {
    match kind {
        ParameterKind::String => "string",
        ParameterKind::Number => "number",
        ParameterKind::Boolean => "boolean",
    }
}

fn parameter_value_kind(value: &SemanticParameterValue) -> ParameterKind {
    match value {
        SemanticParameterValue::String(_) => ParameterKind::String,
        SemanticParameterValue::Number(_) => ParameterKind::Number,
        SemanticParameterValue::Boolean(_) => ParameterKind::Boolean,
    }
}

fn builtin_definitions() -> Vec<SemanticDefinition> {
    vec![
        definition(
            "cdf",
            "variant",
            "CDF framework residual capture column",
            vec![ArrowPattern::Exact {
                arrow_type: "utf8".to_owned(),
            }],
            SemanticNullability::Nullable,
        ),
        definition(
            "cdf",
            "package_row_ordinal",
            "CDF internal package row ordinal",
            vec![ArrowPattern::Exact {
                arrow_type: "uint64".to_owned(),
            }],
            SemanticNullability::NonNullable,
        ),
        SemanticDefinition {
            parameters: BTreeMap::from([(
                "class".to_owned(),
                ParameterDefinition {
                    kind: ParameterKind::String,
                    required: true,
                    format: ParameterFormat::LowerSnakeIdentifier,
                    allowed_values: Vec::new(),
                },
            )]),
            validation: vec![ValidationPredicate::NonEmptyStringParameter {
                parameter: "class".to_owned(),
            }],
            privacy: PrivacyClassification::Pii {
                class_parameter: "class".to_owned(),
            },
            ..definition(
                "cdf",
                "pii",
                "Personally identifiable information classified by parameter",
                vec![ArrowPattern::Any],
                SemanticNullability::Any,
            )
        },
        postgres_exact_definition(
            "json_text",
            "Exact PostgreSQL JSON value represented as UTF-8 text",
            "json",
            "JSON",
            POSTGRES_JSON_TEXT_MAPPING_PROFILE,
        ),
        postgres_exact_definition(
            "jsonb_text",
            "Exact PostgreSQL JSONB value represented as UTF-8 text",
            "jsonb",
            "JSONB",
            POSTGRES_JSONB_TEXT_MAPPING_PROFILE,
        ),
        postgres_numeric_definition(),
    ]
}

fn definition(
    namespace: &str,
    name: &str,
    description: &str,
    arrow_patterns: Vec<ArrowPattern>,
    nullability: SemanticNullability,
) -> SemanticDefinition {
    SemanticDefinition {
        definition_schema_version: DEFINITION_SCHEMA_VERSION,
        namespace: namespace.to_owned(),
        name: name.to_owned(),
        version: 1,
        description: description.to_owned(),
        owning_namespace: namespace.to_owned(),
        supersedes: None,
        deprecated: false,
        arrow_patterns,
        nullability,
        parameters: BTreeMap::new(),
        required_metadata: Vec::new(),
        validation: Vec::new(),
        privacy: PrivacyClassification::Ordinary,
        equivalence: Vec::new(),
        casts: Vec::new(),
        destination_mappings: Vec::new(),
        base_arrow_fallback: true,
    }
}

fn postgres_exact_definition(
    name: &str,
    description: &str,
    physical_type: &str,
    destination_type: &str,
    mapping_profile: &str,
) -> SemanticDefinition {
    let requirement = MetadataRequirement {
        key: cdf_kernel::PHYSICAL_TYPE_METADATA_KEY.to_owned(),
        predicate: MetadataPredicate::AsciiCaseInsensitiveExact {
            value: physical_type.to_owned(),
        },
    };
    SemanticDefinition {
        required_metadata: vec![requirement.clone()],
        destination_mappings: vec![DestinationMapping {
            destination: "postgres".to_owned(),
            arrow_pattern: ArrowPattern::Exact {
                arrow_type: "utf8".to_owned(),
            },
            parameter_equals: BTreeMap::new(),
            required_metadata: vec![requirement],
            mapping_profile: mapping_profile.to_owned(),
            destination_type: destination_type.to_owned(),
            fidelity: TypeMappingFidelity::Lossless,
        }],
        ..definition(
            "postgres",
            name,
            description,
            vec![ArrowPattern::Exact {
                arrow_type: "utf8".to_owned(),
            }],
            SemanticNullability::Any,
        )
    }
}

fn postgres_numeric_definition() -> SemanticDefinition {
    let requirement = MetadataRequirement {
        key: cdf_kernel::PHYSICAL_TYPE_METADATA_KEY.to_owned(),
        predicate: MetadataPredicate::SqlTypeFamily {
            base_names: vec!["numeric".to_owned(), "decimal".to_owned()],
        },
    };
    SemanticDefinition {
        required_metadata: vec![requirement.clone()],
        destination_mappings: vec![DestinationMapping {
            destination: "postgres".to_owned(),
            arrow_pattern: ArrowPattern::Exact {
                arrow_type: "utf8".to_owned(),
            },
            parameter_equals: BTreeMap::new(),
            required_metadata: vec![requirement],
            mapping_profile: POSTGRES_NUMERIC_TEXT_MAPPING_PROFILE.to_owned(),
            destination_type: "NUMERIC".to_owned(),
            fidelity: TypeMappingFidelity::Lossless,
        }],
        ..definition(
            "postgres",
            "numeric_text",
            "Exact PostgreSQL NUMERIC value represented as UTF-8 text",
            vec![ArrowPattern::Exact {
                arrow_type: "utf8".to_owned(),
            }],
            SemanticNullability::Any,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cdf_kernel::{ErrorKind, with_physical_type};

    fn pii(class: &str) -> String {
        format!("cdf.pii@1(class={})", serde_json::to_string(class).unwrap())
    }

    #[test]
    fn builtins_are_unique_and_hash_deterministic() {
        let first = builtin_catalog().unwrap();
        let second = builtin_catalog().unwrap();
        let first = first
            .definitions()
            .map(|entry| entry.definition_hash.clone())
            .collect::<Vec<_>>();
        let second = second
            .definitions()
            .map(|entry| entry.definition_hash.clone())
            .collect::<Vec<_>>();
        assert_eq!(first, second);
        assert_eq!(first.len(), 6);
        assert!(first.iter().all(|hash| hash.starts_with("sha256:")));
    }

    #[test]
    fn parameter_schema_and_unknown_ownership_are_exact() {
        let catalog = builtin_catalog().unwrap();
        let field = Field::new("email", DataType::Utf8, false);
        assert!(
            catalog
                .apply_reference(field.clone(), &pii("email"), SemanticAuthority::Authored)
                .is_ok()
        );
        for (value, authority, kind) in [
            (
                "pii:email",
                SemanticAuthority::Authored,
                ErrorKind::Contract,
            ),
            (
                "other.email@1",
                SemanticAuthority::Observed,
                ErrorKind::Data,
            ),
            (
                "other.email@1",
                SemanticAuthority::Compiled,
                ErrorKind::Internal,
            ),
            (
                "cdf.pii@1",
                SemanticAuthority::Authored,
                ErrorKind::Contract,
            ),
            (
                "cdf.pii@1(class=7)",
                SemanticAuthority::Authored,
                ErrorKind::Contract,
            ),
        ] {
            let error = catalog
                .apply_reference(field.clone(), value, authority)
                .unwrap_err();
            assert_eq!(error.kind, kind, "{value}");
        }
    }

    #[test]
    fn arrow_nullability_and_metadata_prerequisites_fail_closed() {
        let catalog = builtin_catalog().unwrap();
        assert!(
            catalog
                .apply_reference(
                    Field::new("variant", DataType::Utf8, true),
                    CDF_VARIANT_SEMANTIC,
                    SemanticAuthority::Authored,
                )
                .is_ok()
        );
        assert!(
            catalog
                .apply_reference(
                    Field::new("variant", DataType::Utf8, false),
                    CDF_VARIANT_SEMANTIC,
                    SemanticAuthority::Authored,
                )
                .is_err()
        );
        assert!(
            catalog
                .apply_reference(
                    Field::new("payload", DataType::Utf8, true),
                    POSTGRES_JSONB_TEXT_SEMANTIC,
                    SemanticAuthority::Observed,
                )
                .is_err()
        );
        let field = with_physical_type(Field::new("payload", DataType::Utf8, true), "JSONB");
        assert!(
            catalog
                .apply_reference(
                    field,
                    POSTGRES_JSONB_TEXT_SEMANTIC,
                    SemanticAuthority::Observed,
                )
                .is_ok()
        );
    }

    #[test]
    fn destination_resolution_is_specific_and_ambiguity_rejecting() {
        let mut definition = definition(
            "test",
            "value",
            "test semantic",
            vec![ArrowPattern::Exact {
                arrow_type: "utf8".to_owned(),
            }],
            SemanticNullability::Any,
        );
        definition.base_arrow_fallback = false;
        let mapping = DestinationMapping {
            destination: "test".to_owned(),
            arrow_pattern: ArrowPattern::Exact {
                arrow_type: "utf8".to_owned(),
            },
            parameter_equals: BTreeMap::new(),
            required_metadata: Vec::new(),
            mapping_profile: "exact_v1".to_owned(),
            destination_type: "TEXT".to_owned(),
            fidelity: TypeMappingFidelity::Lossless,
        };
        definition.destination_mappings = vec![mapping.clone()];
        let catalog = SemanticCatalog::new(vec![definition.clone()]).unwrap();
        let field = catalog
            .apply_reference(
                Field::new("value", DataType::Utf8, false),
                "test.value@1",
                SemanticAuthority::Authored,
            )
            .unwrap();
        let resolved = catalog
            .resolve_field(&field, SemanticAuthority::Authored)
            .unwrap()
            .unwrap();
        assert_eq!(
            catalog
                .resolve_destination_mapping(&resolved, &field, "test")
                .unwrap()
                .unwrap()
                .mapping_profile,
            "exact_v1"
        );

        let mut competing = mapping;
        competing.mapping_profile = "competing_v1".to_owned();
        definition.destination_mappings.push(competing);
        let catalog = SemanticCatalog::new(vec![definition]).unwrap();
        let reference = catalog
            .parse_reference("test.value@1", SemanticAuthority::Authored)
            .unwrap();
        let resolved = catalog
            .resolve_reference(&reference, SemanticAuthority::Authored)
            .unwrap();
        assert!(
            catalog
                .resolve_destination_mapping(
                    &resolved,
                    &Field::new("value", DataType::Utf8, false),
                    "test"
                )
                .is_err()
        );
    }

    #[test]
    fn project_definitions_compose_with_builtins_and_constrain_parameters() {
        let mut currency = definition(
            "finance",
            "currency",
            "ISO currency identity carried over a decimal Arrow value",
            vec![ArrowPattern::Family {
                family: ArrowTypeFamily::Decimal,
            }],
            SemanticNullability::Any,
        );
        currency.parameters.insert(
            "code".to_owned(),
            ParameterDefinition {
                kind: ParameterKind::String,
                required: true,
                format: ParameterFormat::Any,
                allowed_values: vec![
                    SemanticParameterValue::String("EUR".to_owned()),
                    SemanticParameterValue::String("USD".to_owned()),
                ],
            },
        );
        currency.destination_mappings.push(DestinationMapping {
            destination: "warehouse".to_owned(),
            arrow_pattern: ArrowPattern::Family {
                family: ArrowTypeFamily::Decimal,
            },
            parameter_equals: BTreeMap::from([(
                "code".to_owned(),
                SemanticParameterValue::String("USD".to_owned()),
            )]),
            required_metadata: Vec::new(),
            mapping_profile: "currency_usd_v1".to_owned(),
            destination_type: "DECIMAL(38,9)".to_owned(),
            fidelity: TypeMappingFidelity::Lossless,
        });
        let catalog = SemanticCatalog::with_builtins(vec![currency]).unwrap();
        let usd = catalog
            .apply_reference(
                Field::new("amount", DataType::Decimal128(38, 9), false),
                r#"finance.currency@1(code="USD")"#,
                SemanticAuthority::Authored,
            )
            .unwrap();
        let resolved = catalog
            .resolve_field(&usd, SemanticAuthority::Authored)
            .unwrap()
            .unwrap();
        assert_eq!(
            catalog
                .resolve_destination_mapping(&resolved, &usd, "warehouse")
                .unwrap()
                .unwrap()
                .mapping_profile,
            "currency_usd_v1"
        );
        let error = catalog
            .apply_reference(
                Field::new("amount", DataType::Decimal128(38, 9), false),
                r#"finance.currency@1(code="GBP")"#,
                SemanticAuthority::Authored,
            )
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Contract);
    }

    #[test]
    fn project_definition_descriptors_fail_before_registration() {
        let mut invalid_mapping = definition(
            "finance",
            "currency",
            "project currency semantic",
            vec![ArrowPattern::Family {
                family: ArrowTypeFamily::Decimal,
            }],
            SemanticNullability::Any,
        );
        invalid_mapping.parameters.insert(
            "code".to_owned(),
            ParameterDefinition {
                kind: ParameterKind::String,
                required: true,
                format: ParameterFormat::Any,
                allowed_values: vec![SemanticParameterValue::String("USD".to_owned())],
            },
        );
        invalid_mapping
            .destination_mappings
            .push(DestinationMapping {
                destination: "warehouse".to_owned(),
                arrow_pattern: ArrowPattern::Exact {
                    arrow_type: "not_an_arrow_type".to_owned(),
                },
                parameter_equals: BTreeMap::from([(
                    "code".to_owned(),
                    SemanticParameterValue::String("EUR".to_owned()),
                )]),
                required_metadata: Vec::new(),
                mapping_profile: "currency_eur_v1".to_owned(),
                destination_type: "DECIMAL(38,9)".to_owned(),
                fidelity: TypeMappingFidelity::Lossless,
            });
        assert_eq!(
            SemanticCatalog::new(vec![invalid_mapping])
                .unwrap_err()
                .kind,
            ErrorKind::Contract
        );

        let mut invalid_privacy = definition(
            "project",
            "customer_value",
            "project privacy semantic",
            vec![ArrowPattern::Any],
            SemanticNullability::Any,
        );
        invalid_privacy.privacy = PrivacyClassification::Pii {
            class_parameter: "class".to_owned(),
        };
        assert_eq!(
            SemanticCatalog::new(vec![invalid_privacy])
                .unwrap_err()
                .kind,
            ErrorKind::Contract
        );
    }
}
