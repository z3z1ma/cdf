//! Descriptor-backed Protobuf options and their physical authorities.

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use cdf_kernel::{CdfError, Result};
use prost_reflect::DescriptorPool;
use serde::{Deserialize, Serialize};

use crate::schema::MessagePlan;

pub(crate) const DEFAULT_MAXIMUM_DESCRIPTOR_BYTES: u64 = 16 * 1024 * 1024;
pub(crate) const DEFAULT_MAXIMUM_MESSAGE_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const DEFAULT_MAXIMUM_OUTPUT_BATCH_BYTES: u64 = 256 * 1024 * 1024;
pub(crate) const DEFAULT_MAXIMUM_NESTING_DEPTH: u32 = 100;
pub(crate) const MAXIMUM_DESCRIPTOR_BYTES: u64 = 1024 * 1024 * 1024;
pub(crate) const MAXIMUM_MESSAGE_BYTES: u64 = 4 * 1024 * 1024 * 1024;
pub(crate) const MAXIMUM_OUTPUT_BATCH_BYTES: u64 = 16 * 1024 * 1024 * 1024;
pub(crate) const MAXIMUM_NESTING_DEPTH: u32 = 4096;
pub(crate) const MAXIMUM_LENGTH_PREFIX_BYTES: usize = 10;
// Wire values contribute at most one Arrow value plus offsets/validity; per-cell overhead below
// separately accounts sparse/default-heavy messages. Four keeps the lease conservative without
// rejecting the runtime's legal 32 MiB adaptive batches under the default output authority.
pub(crate) const OUTPUT_ESTIMATE_MULTIPLIER: u64 = 4;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Framing {
    LengthDelimited,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProtobufOptions {
    pub(crate) descriptor_set_base64: String,
    pub(crate) message: String,
    pub(crate) framing: Framing,
    #[serde(default = "default_maximum_descriptor_bytes")]
    pub(crate) maximum_descriptor_bytes: u64,
    #[serde(default = "default_maximum_message_bytes")]
    pub(crate) maximum_message_bytes: u64,
    #[serde(default = "default_maximum_output_batch_bytes")]
    pub(crate) maximum_output_batch_bytes: u64,
    #[serde(default = "default_maximum_nesting_depth")]
    pub(crate) maximum_nesting_depth: u32,
}

const fn default_maximum_descriptor_bytes() -> u64 {
    DEFAULT_MAXIMUM_DESCRIPTOR_BYTES
}

const fn default_maximum_message_bytes() -> u64 {
    DEFAULT_MAXIMUM_MESSAGE_BYTES
}

const fn default_maximum_output_batch_bytes() -> u64 {
    DEFAULT_MAXIMUM_OUTPUT_BATCH_BYTES
}

const fn default_maximum_nesting_depth() -> u32 {
    DEFAULT_MAXIMUM_NESTING_DEPTH
}

impl ProtobufOptions {
    pub(crate) fn parse(value: serde_json::Value) -> Result<(Self, MessagePlan)> {
        let options: Self = serde_json::from_value(value)
            .map_err(|error| CdfError::contract(format!("invalid Protobuf options: {error}")))?;
        if options.descriptor_set_base64.is_empty() {
            return Err(CdfError::contract(
                "Protobuf format_options.descriptor_set_base64 is required",
            ));
        }
        if options.message.trim().is_empty() || options.message.starts_with('.') {
            return Err(CdfError::contract(
                "Protobuf format_options.message requires a fully qualified message name without a leading dot",
            ));
        }
        if !(1..=MAXIMUM_DESCRIPTOR_BYTES).contains(&options.maximum_descriptor_bytes)
            || !(1..=MAXIMUM_MESSAGE_BYTES).contains(&options.maximum_message_bytes)
            || !(1..=MAXIMUM_OUTPUT_BATCH_BYTES).contains(&options.maximum_output_batch_bytes)
            || !(1..=MAXIMUM_NESTING_DEPTH).contains(&options.maximum_nesting_depth)
        {
            return Err(CdfError::contract(format!(
                "Protobuf maximum_descriptor_bytes, maximum_message_bytes, maximum_output_batch_bytes, and maximum_nesting_depth must be nonzero and no greater than their physical limits ({MAXIMUM_DESCRIPTOR_BYTES}, {MAXIMUM_MESSAGE_BYTES}, {MAXIMUM_OUTPUT_BATCH_BYTES}, {MAXIMUM_NESTING_DEPTH})"
            )));
        }
        if options.maximum_output_batch_bytes < options.maximum_message_bytes {
            return Err(CdfError::contract(
                "Protobuf maximum_output_batch_bytes must be at least maximum_message_bytes so one admitted message can be materialized atomically",
            ));
        }
        let plan = options.message_plan()?;
        Ok((options, plan))
    }

    pub(crate) fn canonical(self) -> Result<serde_json::Value> {
        serde_json::to_value(self)
            .map_err(|error| CdfError::internal(format!("encode Protobuf options: {error}")))
    }

    pub(crate) fn descriptor_bytes(&self) -> Result<Vec<u8>> {
        let maximum_encoded = self
            .maximum_descriptor_bytes
            .checked_mul(4)
            .and_then(|bytes| bytes.checked_div(3))
            .and_then(|bytes| bytes.checked_add(8))
            .ok_or_else(|| CdfError::contract("Protobuf descriptor authority overflowed"))?;
        let encoded_length = u64::try_from(self.descriptor_set_base64.len())
            .map_err(|_| CdfError::contract("Protobuf descriptor text length exceeds u64"))?;
        if encoded_length > maximum_encoded {
            return Err(CdfError::contract(format!(
                "Protobuf descriptor_set_base64 exceeds the configured {}-byte decoded descriptor authority",
                self.maximum_descriptor_bytes
            )));
        }
        let bytes = BASE64_STANDARD
            .decode(&self.descriptor_set_base64)
            .map_err(|error| {
                CdfError::contract(format!("decode Protobuf descriptor set: {error}"))
            })?;
        let length = u64::try_from(bytes.len())
            .map_err(|_| CdfError::contract("Protobuf descriptor set length exceeds u64"))?;
        if length == 0 || length > self.maximum_descriptor_bytes {
            return Err(CdfError::contract(format!(
                "Protobuf descriptor set contains {length} bytes outside the configured 1..={} byte authority",
                self.maximum_descriptor_bytes
            )));
        }
        Ok(bytes)
    }

    pub(crate) fn message_plan(&self) -> Result<MessagePlan> {
        let bytes = self.descriptor_bytes()?;
        let pool = DescriptorPool::decode(bytes.as_slice()).map_err(|error| {
            CdfError::contract(format!("decode Protobuf FileDescriptorSet: {error}"))
        })?;
        let descriptor = pool.get_message_by_name(&self.message).ok_or_else(|| {
            CdfError::contract(format!(
                "Protobuf descriptor set does not define message `{}`",
                self.message
            ))
        })?;
        MessagePlan::compile(descriptor)
    }
}
