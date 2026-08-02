//! Canonical Avro format options and physical limits.

use arrow_avro::schema::{AvroSchema, FingerprintAlgorithm, SchemaStore};
use cdf_kernel::{CdfError, Result};
use serde::{Deserialize, Serialize};

use crate::errors::avro_arrow_error;

pub(crate) const OCF_MAGIC: &[u8; 4] = b"Obj\x01";
pub(crate) const SOE_MAGIC: &[u8; 2] = &[0xc3, 0x01];
pub(crate) const DEFAULT_MAXIMUM_BLOCK_BYTES: u64 = 256 * 1024 * 1024;
pub(crate) const DEFAULT_MAXIMUM_DECODED_BLOCK_BYTES: u64 = 512 * 1024 * 1024;
pub(crate) const DEFAULT_MAXIMUM_BLOCK_RECORDS: u64 = 16 * 1024 * 1024;
pub(crate) const DEFAULT_MAXIMUM_BLOCKS: u32 = 1_000_000;
pub(crate) const DEFAULT_MAXIMUM_HEADER_BYTES: u64 = 16 * 1024 * 1024;
pub(crate) const OCF_HEADER_READ_BYTES: u64 = 16 * 1024;
pub(crate) const DEFAULT_MAXIMUM_RECORD_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const MAXIMUM_INDIVIDUAL_VALUE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
pub(crate) const MAXIMUM_WORKING_SET_BYTES: u64 = 4 * 1024 * 1024 * 1024;
pub(crate) const MAXIMUM_VLQ_HEADER_BYTES: u64 = 20;
pub(crate) const OCF_SYNC_MARKER_BYTES: u64 = 16;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct OcfOptions {
    pub(crate) maximum_header_bytes: u64,
    pub(crate) maximum_block_bytes: u64,
    pub(crate) maximum_decoded_block_bytes: u64,
    pub(crate) maximum_block_records: u64,
    pub(crate) maximum_blocks: u32,
}

impl Default for OcfOptions {
    fn default() -> Self {
        Self {
            maximum_header_bytes: DEFAULT_MAXIMUM_HEADER_BYTES,
            maximum_block_bytes: DEFAULT_MAXIMUM_BLOCK_BYTES,
            maximum_decoded_block_bytes: DEFAULT_MAXIMUM_DECODED_BLOCK_BYTES,
            maximum_block_records: DEFAULT_MAXIMUM_BLOCK_RECORDS,
            maximum_blocks: DEFAULT_MAXIMUM_BLOCKS,
        }
    }
}

impl OcfOptions {
    pub(crate) fn parse(value: serde_json::Value) -> Result<Self> {
        let options: Self = serde_json::from_value(value)
            .map_err(|error| CdfError::contract(format!("invalid Avro OCF options: {error}")))?;
        if options.maximum_header_bytes == 0
            || options.maximum_block_bytes == 0
            || options.maximum_decoded_block_bytes == 0
            || options.maximum_block_records == 0
            || options.maximum_blocks == 0
            || options.maximum_header_bytes > MAXIMUM_INDIVIDUAL_VALUE_BYTES
            || options.maximum_block_bytes > MAXIMUM_INDIVIDUAL_VALUE_BYTES
            || options.maximum_decoded_block_bytes > MAXIMUM_WORKING_SET_BYTES
        {
            return Err(CdfError::contract(
                "Avro OCF maximum_header_bytes, maximum_block_bytes, maximum_decoded_block_bytes, maximum_block_records, and maximum_blocks must be nonzero; byte authorities may not exceed their documented physical maximum",
            ));
        }
        Ok(options)
    }

    pub(crate) fn canonical(self) -> Result<serde_json::Value> {
        serde_json::to_value(self)
            .map_err(|error| CdfError::internal(format!("encode Avro OCF options: {error}")))
    }

    pub(crate) fn maximum_request_bytes(self) -> Result<u64> {
        let block_request = self
            .maximum_block_bytes
            .checked_add(MAXIMUM_VLQ_HEADER_BYTES + OCF_SYNC_MARKER_BYTES)
            .ok_or_else(|| CdfError::contract("Avro block request authority overflowed"))?
            .checked_add(MAXIMUM_VLQ_HEADER_BYTES)
            .ok_or_else(|| CdfError::contract("Avro range request authority overflowed"))?;
        Ok(block_request.max(OCF_HEADER_READ_BYTES))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SingleObjectOptions {
    pub(crate) writer_schema: serde_json::Value,
    #[serde(default = "default_maximum_record_bytes")]
    pub(crate) maximum_record_bytes: u64,
}

const fn default_maximum_record_bytes() -> u64 {
    DEFAULT_MAXIMUM_RECORD_BYTES
}

impl SingleObjectOptions {
    pub(crate) fn parse(value: serde_json::Value) -> Result<Self> {
        let options: Self = serde_json::from_value(value).map_err(|error| {
            CdfError::contract(format!("invalid Avro single-object options: {error}"))
        })?;
        if options.maximum_record_bytes == 0
            || options.maximum_record_bytes > MAXIMUM_INDIVIDUAL_VALUE_BYTES
        {
            return Err(CdfError::contract(format!(
                "Avro single-object maximum_record_bytes must be in 1..={MAXIMUM_INDIVIDUAL_VALUE_BYTES}"
            )));
        }
        options.writer_schema()?;
        Ok(options)
    }

    pub(crate) fn canonical(self) -> Result<serde_json::Value> {
        serde_json::to_value(self).map_err(|error| {
            CdfError::internal(format!("encode Avro single-object options: {error}"))
        })
    }

    pub(crate) fn writer_schema(&self) -> Result<AvroSchema> {
        if self.writer_schema.is_null() {
            return Err(CdfError::contract(
                "Avro single-object writer_schema cannot be null",
            ));
        }
        let schema = AvroSchema::new(self.writer_schema.to_string());
        schema
            .fingerprint(FingerprintAlgorithm::Rabin)
            .map_err(avro_arrow_error)?;
        Ok(schema)
    }

    pub(crate) fn schema_store(&self) -> Result<SchemaStore> {
        let mut store = SchemaStore::new();
        store
            .register(self.writer_schema()?)
            .map_err(avro_arrow_error)?;
        Ok(store)
    }
}
