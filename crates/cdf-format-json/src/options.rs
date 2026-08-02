//! JSON format options and bounded physical authorities.

use cdf_kernel::{CdfError, Result};
use serde::{Deserialize, Serialize};

pub(crate) const DISCOVERY_CHUNK_BYTES: u64 = 1024 * 1024;
pub(crate) const FULL_CONTENT_INFERENCE_WINDOW_BYTES: u64 = 8 * 1024 * 1024;
pub(crate) const MAXIMUM_DECODE_WORKING_SET_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const MAXIMUM_CONFIGURED_RECORD_BYTES: u64 = 32 * 1024 * 1024;
pub(crate) const DEFAULT_MAXIMUM_RECORD_BYTES: u64 = 16 * 1024 * 1024;
pub(crate) const MAXIMUM_JSON_NESTING_DEPTH: usize = 256;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct NdjsonOptions {
    pub(crate) maximum_record_bytes: u64,
}

impl Default for NdjsonOptions {
    fn default() -> Self {
        Self {
            maximum_record_bytes: DEFAULT_MAXIMUM_RECORD_BYTES,
        }
    }
}

impl NdjsonOptions {
    pub(crate) fn parse(value: serde_json::Value) -> Result<Self> {
        let options: Self = serde_json::from_value(value)
            .map_err(|error| CdfError::contract(format!("invalid NDJSON options: {error}")))?;
        validate_maximum_record_bytes(options.maximum_record_bytes)?;
        Ok(options)
    }

    pub(crate) fn canonical(self) -> Result<serde_json::Value> {
        serde_json::to_value(self)
            .map_err(|error| CdfError::internal(format!("encode NDJSON options: {error}")))
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct JsonDocumentOptions {
    pub(crate) maximum_record_bytes: u64,
    pub(crate) maximum_nesting_depth: usize,
}

impl Default for JsonDocumentOptions {
    fn default() -> Self {
        Self {
            maximum_record_bytes: DEFAULT_MAXIMUM_RECORD_BYTES,
            maximum_nesting_depth: MAXIMUM_JSON_NESTING_DEPTH,
        }
    }
}

impl JsonDocumentOptions {
    pub(crate) fn parse(value: serde_json::Value) -> Result<Self> {
        let options: Self = serde_json::from_value(value)
            .map_err(|error| CdfError::contract(format!("invalid JSON options: {error}")))?;
        validate_maximum_record_bytes(options.maximum_record_bytes)?;
        if options.maximum_nesting_depth == 0
            || options.maximum_nesting_depth > MAXIMUM_JSON_NESTING_DEPTH
        {
            return Err(CdfError::contract(format!(
                "JSON maximum_nesting_depth must be in 1..={MAXIMUM_JSON_NESTING_DEPTH}"
            )));
        }
        Ok(options)
    }

    pub(crate) fn canonical(self) -> Result<serde_json::Value> {
        serde_json::to_value(self)
            .map_err(|error| CdfError::internal(format!("encode JSON options: {error}")))
    }
}

pub(crate) fn validate_maximum_record_bytes(value: u64) -> Result<()> {
    // Every token and string byte consumes at least one record byte. This limit is therefore also
    // a hard token-count and string-size ceiling without adding counters to the decode hot loop.
    if value == 0 || value > MAXIMUM_CONFIGURED_RECORD_BYTES {
        return Err(CdfError::contract(format!(
            "JSON maximum_record_bytes must be in 1..={MAXIMUM_CONFIGURED_RECORD_BYTES}"
        )));
    }
    Ok(())
}

pub(crate) fn maximum_record_bytes_error(maximum_record_bytes: u64) -> CdfError {
    CdfError::data(format!(
        "JSON record exceeds the planned {maximum_record_bytes}-byte maximum_record_bytes limit; increase format_options.maximum_record_bytes before planning or split the source record"
    ))
}
