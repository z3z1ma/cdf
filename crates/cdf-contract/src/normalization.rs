use std::collections::BTreeMap;

use arrow_schema::Schema;
use cdf_kernel::{CdfError, Result, with_source_name};
use serde::{Deserialize, Serialize};
use unicode_normalization::UnicodeNormalization;

use crate::{
    policy::{IdentifierCharset, IdentifierPolicy, NORMALIZER_NAMECASE_V1},
    schema::ObservedSchema,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizedSchema {
    pub fields: Vec<NormalizedField>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizedField {
    pub source_name: String,
    pub output_name: String,
    pub metadata: BTreeMap<String, String>,
}

pub fn normalize_schema(
    observed_schema: &ObservedSchema,
    policy: &IdentifierPolicy,
) -> Result<NormalizedSchema> {
    validate_normalizer(policy)?;
    let mut outputs = BTreeMap::<String, String>::new();
    let mut fields = Vec::with_capacity(observed_schema.fields.len());

    for field in &observed_schema.fields {
        let output_name = normalize_identifier(&field.source_name, policy)?;
        if let Some(previous_source) =
            outputs.insert(output_name.clone(), field.source_name.clone())
        {
            return Err(CdfError::contract(format!(
                "identifier collision after {NORMALIZER_NAMECASE_V1}: {previous_source:?} and {:?} both normalize to {output_name:?}; add an explicit rename",
                field.source_name
            )));
        }

        let mut metadata = field.metadata.clone();
        metadata.insert(
            cdf_kernel::SOURCE_NAME_METADATA_KEY.to_owned(),
            field.source_name.clone(),
        );
        fields.push(NormalizedField {
            source_name: field.source_name.clone(),
            output_name,
            metadata,
        });
    }

    Ok(NormalizedSchema { fields })
}

pub fn normalize_arrow_schema(schema: &Schema, policy: &IdentifierPolicy) -> Result<Schema> {
    let observed = ObservedSchema::from_arrow(schema);
    let normalized = normalize_schema(&observed, policy)?;
    let fields = schema
        .fields()
        .iter()
        .zip(normalized.fields)
        .map(|(field_ref, normalized)| {
            with_source_name(field_ref.as_ref().clone(), normalized.source_name)
                .with_name(normalized.output_name)
        })
        .collect::<Vec<_>>();

    Ok(Schema::new(fields))
}

pub fn normalize_identifier(source_name: &str, policy: &IdentifierPolicy) -> Result<String> {
    validate_normalizer(policy)?;
    let nfc = source_name.nfc().collect::<String>();
    let snake = lower_snake_case(&nfc);
    let filtered = filter_identifier_charset(&snake, &policy.charset);
    truncate_identifier(&filtered, source_name, policy.max_length)
}
pub(crate) fn validate_normalizer(policy: &IdentifierPolicy) -> Result<()> {
    if policy.version != NORMALIZER_NAMECASE_V1 {
        return Err(CdfError::contract(format!(
            "unsupported identifier normalizer {:?}; expected {NORMALIZER_NAMECASE_V1:?}",
            policy.version
        )));
    }
    if policy.max_length < 10 {
        return Err(CdfError::contract(
            "identifier max_length must leave room for hash suffix",
        ));
    }
    Ok(())
}

fn lower_snake_case(input: &str) -> String {
    let chars = input.chars().collect::<Vec<_>>();
    let mut out = String::new();
    let mut previous_was_word = false;
    let mut previous_was_lower_or_digit = false;

    for (index, ch) in chars.iter().copied().enumerate() {
        if !ch.is_alphanumeric() {
            push_separator(&mut out);
            previous_was_word = false;
            previous_was_lower_or_digit = false;
            continue;
        }

        let next_is_lower = chars.get(index + 1).is_some_and(|next| next.is_lowercase());
        if ch.is_uppercase() && previous_was_word && (previous_was_lower_or_digit || next_is_lower)
        {
            push_separator(&mut out);
        }

        for lower in ch.to_lowercase() {
            out.push(lower);
        }
        previous_was_word = true;
        previous_was_lower_or_digit = ch.is_lowercase() || ch.is_numeric();
    }

    trim_identifier_separators(out)
}

fn filter_identifier_charset(input: &str, charset: &IdentifierCharset) -> String {
    match charset {
        IdentifierCharset::AsciiLowerSnake => {
            let mut out = String::new();
            for ch in input.chars() {
                if ch.is_ascii_lowercase() || ch.is_ascii_digit() {
                    out.push(ch);
                } else {
                    push_separator(&mut out);
                }
            }
            let filtered = trim_identifier_separators(out);
            if filtered.is_empty() {
                "field".to_owned()
            } else {
                filtered
            }
        }
    }
}

fn truncate_identifier(normalized: &str, source_name: &str, max_length: u16) -> Result<String> {
    let max_length = usize::from(max_length);
    if normalized.len() <= max_length {
        return Ok(normalized.to_owned());
    }

    if max_length < 10 {
        return Err(CdfError::contract(
            "identifier max_length must leave room for hash suffix",
        ));
    }

    let prefix_len = max_length - 9;
    let prefix = normalized.chars().take(prefix_len).collect::<String>();
    Ok(format!(
        "{}_{}",
        prefix.trim_end_matches('_'),
        hash8(source_name)
    ))
}

fn push_separator(out: &mut String) {
    if !out.is_empty() && !out.ends_with('_') {
        out.push('_');
    }
}

fn trim_identifier_separators(mut value: String) -> String {
    while value.ends_with('_') {
        value.pop();
    }
    value
}

fn hash8(value: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{:08x}", hash as u32)
}
