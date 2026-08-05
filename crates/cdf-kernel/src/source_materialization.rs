use std::collections::BTreeMap;

use arrow_schema::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

use crate::{CanonicalArrowType, CdfError, Result, source_name};

/// One source-owned, exact physical-to-Arrow materialization relation.
///
/// The source that owns the decoder compiles this rule. Generic schema admission consumes only
/// the typed relation and never branches on a driver, wire type, or semantic identifier.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceMaterializationRule {
    pub materializer_id: String,
    pub field_path: Vec<String>,
    pub observed_type: CanonicalArrowType,
    pub required_observed_metadata: BTreeMap<String, String>,
    pub output_type: CanonicalArrowType,
}

impl SourceMaterializationRule {
    pub fn new(
        materializer_id: impl Into<String>,
        field_path: Vec<String>,
        observed_type: CanonicalArrowType,
        required_observed_metadata: BTreeMap<String, String>,
        output_type: CanonicalArrowType,
    ) -> Result<Self> {
        let rule = Self {
            materializer_id: materializer_id.into(),
            field_path,
            observed_type,
            required_observed_metadata,
            output_type,
        };
        rule.validate()?;
        Ok(rule)
    }

    pub fn validate(&self) -> Result<()> {
        if self.materializer_id.is_empty()
            || self.materializer_id.len() > 128
            || !self.materializer_id.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(&byte)
            })
            || !self
                .materializer_id
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_lowercase)
        {
            return Err(CdfError::contract(
                "source materializer id must be 1..=128 lowercase ASCII identifier characters beginning with a letter",
            ));
        }
        if self.field_path.is_empty()
            || self.field_path.len() > 64
            || self.field_path.iter().any(|segment| {
                segment.is_empty() || segment.len() > 1_024 || segment.chars().any(char::is_control)
            })
        {
            return Err(CdfError::contract(
                "source materialization field path must contain 1..=64 nonempty control-free segments",
            ));
        }
        if self.required_observed_metadata.is_empty()
            || self.required_observed_metadata.len() > 64
            || self
                .required_observed_metadata
                .iter()
                .fold(0_usize, |total, (key, value)| {
                    total.saturating_add(key.len()).saturating_add(value.len())
                })
                > 64 * 1_024
            || self.required_observed_metadata.iter().any(|(key, value)| {
                key.is_empty()
                    || value.is_empty()
                    || key.chars().any(char::is_control)
                    || value.chars().any(char::is_control)
            })
        {
            return Err(CdfError::contract(
                "source materialization requires nonempty control-free observed metadata predicates",
            ));
        }
        let observed = self.observed_type.to_arrow()?;
        let output = self.output_type.to_arrow()?;
        if observed == output {
            return Err(CdfError::contract(
                "source materialization must change the observed Arrow type",
            ));
        }
        Ok(())
    }

    pub fn matches(&self, field_path: &[String], observed: &Field, output: &Field) -> Result<bool> {
        self.validate()?;
        Ok(self.field_path == field_path
            && self.observed_type == CanonicalArrowType::from_arrow(observed.data_type())?
            && self.output_type == CanonicalArrowType::from_arrow(output.data_type())?
            && self
                .required_observed_metadata
                .iter()
                .all(|(key, value)| observed.metadata().get(key) == Some(value)))
    }
}

pub fn validate_source_materializations(
    rules: &[SourceMaterializationRule],
    schema: &Schema,
) -> Result<()> {
    if rules.len() > 65_536 {
        return Err(CdfError::contract(
            "compiled source materialization count exceeds 65536",
        ));
    }
    if rules
        .windows(2)
        .any(|pair| pair[0].field_path >= pair[1].field_path)
    {
        return Err(CdfError::contract(
            "compiled source materializations must be sorted by unique field path",
        ));
    }
    for rule in rules {
        rule.validate()?;
        let field = field_at_path(schema, &rule.field_path).ok_or_else(|| {
            CdfError::contract(format!(
                "compiled source materializer {} references absent field path {:?}",
                rule.materializer_id, rule.field_path
            ))
        })?;
        if rule.output_type != CanonicalArrowType::from_arrow(field.data_type())? {
            return Err(CdfError::contract(format!(
                "compiled source materializer {} output does not match field path {:?}",
                rule.materializer_id, rule.field_path
            )));
        }
    }
    Ok(())
}

fn field_at_path<'a>(schema: &'a Schema, path: &[String]) -> Option<&'a Field> {
    let (first, rest) = path.split_first()?;
    let field = schema
        .fields()
        .iter()
        .map(AsRef::as_ref)
        .find(|field| source_name(field).unwrap_or_else(|| field.name()) == first)?;
    field_descendant(field, rest)
}

fn field_descendant<'a>(field: &'a Field, path: &[String]) -> Option<&'a Field> {
    let Some((next, rest)) = path.split_first() else {
        return Some(field);
    };
    let child = match field.data_type() {
        DataType::Struct(fields) => fields
            .iter()
            .map(AsRef::as_ref)
            .find(|field| source_name(field).unwrap_or_else(|| field.name()) == next)?,
        DataType::List(child)
        | DataType::LargeList(child)
        | DataType::ListView(child)
        | DataType::LargeListView(child)
            if source_name(child).unwrap_or_else(|| child.name()) == next =>
        {
            child.as_ref()
        }
        DataType::FixedSizeList(child, _)
            if source_name(child).unwrap_or_else(|| child.name()) == next =>
        {
            child.as_ref()
        }
        _ => return None,
    };
    field_descendant(child, rest)
}
