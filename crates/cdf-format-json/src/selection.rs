//! Zero-copy bounded JSON response-envelope selection.

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Range,
};

use cdf_kernel::{CdfError, Result};
use serde_json::value::RawValue;

use crate::raw::{BorrowedJsonObject, trim_ascii_whitespace};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundedJsonSelection {
    pub byte_range: Range<usize>,
    pub records_present: bool,
    pub top_level_scalar_fields: BTreeMap<String, String>,
}

/// Resolves the bounded streaming-selector grammar without constructing a JSON DOM.
///
/// `$` selects a top-level array. `$.field` selects one top-level object field whose value is an
/// array. The returned range borrows the caller's original accounted body and can therefore be
/// passed to the ordinary JSON document driver as a zero-copy slice.
pub fn select_bounded_json_records(bytes: &[u8], selector: &str) -> Result<BoundedJsonSelection> {
    if selector == "$" {
        let byte_range = trim_ascii_whitespace_range(bytes);
        let records_present =
            json_array_has_records(bytes.get(byte_range.clone()).ok_or_else(|| {
                CdfError::data("JSON record selector `$` requires a top-level array")
            })?)?;
        return Ok(BoundedJsonSelection {
            byte_range,
            records_present,
            top_level_scalar_fields: BTreeMap::new(),
        });
    }
    let Some(field) = selector.strip_prefix("$.") else {
        return Err(CdfError::contract(
            "JSON record selector must be `$` or `$.<field>`",
        ));
    };
    if field.is_empty() || field.contains('.') {
        return Err(CdfError::contract(
            "JSON record selector supports exactly one object field after `$.`",
        ));
    }
    let object: BorrowedJsonObject<'_> = serde_json::from_slice(bytes)
        .map_err(|error| CdfError::data(format!("decode JSON response envelope: {error}")))?;
    let mut selected = None;
    let mut scalars = BTreeMap::new();
    let mut seen = BTreeSet::new();
    for (name, value) in object.0 {
        if !seen.insert(name.clone()) {
            return Err(CdfError::data(format!(
                "JSON response envelope repeats field {name:?}"
            )));
        }
        if name == field {
            if !trim_ascii_whitespace(value.get().as_bytes()).starts_with(b"[") {
                return Err(CdfError::data(format!(
                    "JSON record selector target `{field}` is not an array"
                )));
            }
            selected = Some(raw_value_range(bytes, value)?);
        } else if let Some(marker) = raw_scalar_marker(value)? {
            scalars.insert(name, marker);
        }
    }
    let byte_range = selected.ok_or_else(|| {
        CdfError::data(format!(
            "JSON record selector target `{field}` is missing from response"
        ))
    })?;
    let records_present = json_array_has_records(
        bytes
            .get(byte_range.clone())
            .ok_or_else(|| CdfError::internal("selected JSON range escaped its source body"))?,
    )?;
    Ok(BoundedJsonSelection {
        byte_range,
        records_present,
        top_level_scalar_fields: scalars,
    })
}

fn json_array_has_records(bytes: &[u8]) -> Result<bool> {
    let bytes = trim_ascii_whitespace(bytes);
    if bytes.first() != Some(&b'[') || bytes.last() != Some(&b']') {
        return Err(CdfError::data(
            "JSON record selector target must be a complete array",
        ));
    }
    Ok(!trim_ascii_whitespace(&bytes[1..bytes.len() - 1]).is_empty())
}

fn raw_value_range(bytes: &[u8], value: &RawValue) -> Result<Range<usize>> {
    let start = (value.get().as_ptr() as usize)
        .checked_sub(bytes.as_ptr() as usize)
        .ok_or_else(|| CdfError::internal("borrowed JSON value precedes its source body"))?;
    let end = start
        .checked_add(value.get().len())
        .ok_or_else(|| CdfError::data("borrowed JSON value range overflowed"))?;
    if end > bytes.len() || bytes.get(start..end) != Some(value.get().as_bytes()) {
        return Err(CdfError::internal(
            "borrowed JSON value range escaped its source body",
        ));
    }
    Ok(start..end)
}

fn raw_scalar_marker(value: &RawValue) -> Result<Option<String>> {
    let raw = value.get();
    Ok(match raw.as_bytes().first().copied() {
        Some(b'"') => Some(serde_json::from_str(raw).map_err(|error| {
            CdfError::data(format!("decode JSON response scalar string: {error}"))
        })?),
        Some(b't' | b'f' | b'-' | b'0'..=b'9') => Some(raw.to_owned()),
        Some(b'n' | b'{' | b'[') => None,
        Some(_) => {
            return Err(CdfError::data(
                "JSON response scalar contains an unsupported token",
            ));
        }
        None => return Err(CdfError::data("JSON response scalar is empty")),
    })
}

fn trim_ascii_whitespace_range(bytes: &[u8]) -> Range<usize> {
    let start = bytes
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())
        .map_or(start, |index| index + 1);
    start..end
}
