//! Arrow-Avro range adapter and OCF block-bound validation.

use std::ops::Range;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use arrow_avro::{errors::AvroError, reader::AsyncFileReader};
use bytes::Bytes;
use cdf_kernel::{CdfError, Result};
use cdf_runtime::{ByteExtent, ByteSource};
use futures_util::{FutureExt, future::BoxFuture as FuturesBoxFuture};

use crate::errors::cdf_to_avro;

pub(crate) struct AvroByteSource {
    source: Arc<dyn ByteSource>,
    cancellation: cdf_runtime::RunCancellation,
    maximum_request_bytes: u64,
    bytes_read: Arc<AtomicU64>,
    maximum_total_bytes: Option<u64>,
    ocf_validator: Option<OcfBlockValidator>,
}

impl AvroByteSource {
    pub(crate) fn new(
        source: Arc<dyn ByteSource>,
        cancellation: cdf_runtime::RunCancellation,
        maximum_request_bytes: u64,
        bytes_read: Arc<AtomicU64>,
    ) -> Self {
        Self {
            source,
            cancellation,
            maximum_request_bytes,
            bytes_read,
            maximum_total_bytes: None,
            ocf_validator: None,
        }
    }

    pub(crate) fn with_total_budget(mut self, maximum_total_bytes: u64) -> Self {
        self.maximum_total_bytes = Some(maximum_total_bytes);
        self
    }

    pub(crate) fn with_ocf_validation(
        mut self,
        sync: [u8; 16],
        maximum_block_bytes: u64,
        maximum_block_records: u64,
    ) -> Self {
        self.ocf_validator = Some(OcfBlockValidator::new(
            sync,
            maximum_block_bytes,
            maximum_block_records,
        ));
        self
    }
}

impl AsyncFileReader for AvroByteSource {
    fn get_bytes(
        &mut self,
        range: Range<u64>,
    ) -> FuturesBoxFuture<'_, std::result::Result<Bytes, AvroError>> {
        async move {
            self.cancellation.check().map_err(cdf_to_avro)?;
            let size = self
                .source
                .identity()
                .size_bytes
                .ok_or_else(|| AvroError::General("Avro range source length is unknown".to_owned()))?;
            if range.start >= range.end || range.end > size {
                return Err(AvroError::General(format!(
                    "Avro requested invalid byte range {}..{} for {size}-byte source",
                    range.start, range.end
                )));
            }
            let length = range.end - range.start;
            if length > self.maximum_request_bytes {
                return Err(AvroError::General(format!(
                    "Avro block requires a {length}-byte range above the configured {}-byte authority",
                    self.maximum_request_bytes
                )));
            }
            if let Some(maximum_total_bytes) = self.maximum_total_bytes {
                let already_read = self.bytes_read.load(Ordering::Relaxed);
                let requested_total = already_read.checked_add(length).ok_or_else(|| {
                    AvroError::General("Avro byte observation count overflowed".to_owned())
                })?;
                if requested_total > maximum_total_bytes {
                    return Err(AvroError::General(format!(
                        "Avro discovery requires more than its configured {maximum_total_bytes}-byte observation budget"
                    )));
                }
            }
            let bytes = self
                .source
                .read_exact_range(
                    ByteExtent::new(range.start, length).map_err(cdf_to_avro)?,
                    self.cancellation.clone(),
                )
                .await
                .map_err(cdf_to_avro)?;
            if let Some(validator) = &mut self.ocf_validator {
                validator
                    .validate(range.start, bytes.payload())
                    .map_err(cdf_to_avro)?;
            }
            self.bytes_read.fetch_add(length, Ordering::Relaxed);
            Ok(bytes.into_retained_bytes())
        }
        .boxed()
    }
}

#[derive(Clone, Debug)]
struct OcfBlockValidator {
    sync: [u8; 16],
    maximum_block_bytes: u64,
    maximum_block_records: u64,
    next_offset: Option<u64>,
    state: OcfBlockValidationState,
}

#[derive(Clone, Debug)]
enum OcfBlockValidationState {
    SeekingFirstSync,
    Count(AvroLongDecoder),
    Size(AvroLongDecoder),
    Data(u64),
    Sync(usize),
}

#[derive(Clone, Debug, Default)]
pub(crate) struct AvroLongDecoder {
    raw: u64,
    shift: u32,
    bytes: u8,
}

impl AvroLongDecoder {
    pub(crate) fn push(&mut self, byte: u8) -> Result<Option<i64>> {
        if self.bytes == 10 || (self.bytes == 9 && byte > 1) {
            return Err(CdfError::data("Avro block header contains an invalid long"));
        }
        self.raw |= u64::from(byte & 0x7f) << self.shift;
        self.bytes += 1;
        if byte & 0x80 != 0 {
            self.shift += 7;
            return Ok(None);
        }
        let value = ((self.raw >> 1) as i64) ^ -((self.raw & 1) as i64);
        Ok(Some(value))
    }
}

impl OcfBlockValidator {
    pub(crate) fn new(
        sync: [u8; 16],
        maximum_block_bytes: u64,
        maximum_block_records: u64,
    ) -> Self {
        Self {
            sync,
            maximum_block_bytes,
            maximum_block_records,
            next_offset: None,
            state: OcfBlockValidationState::SeekingFirstSync,
        }
    }

    fn validate(&mut self, offset: u64, mut bytes: &[u8]) -> Result<()> {
        let input_length = u64::try_from(bytes.len())
            .map_err(|_| CdfError::data("Avro OCF validation length exceeds u64"))?;
        if let Some(expected) = self.next_offset
            && expected != offset
        {
            return Err(CdfError::data(format!(
                "Avro OCF reader requested non-contiguous validation ranges: expected offset {expected}, observed {offset}"
            )));
        }

        if matches!(self.state, OcfBlockValidationState::SeekingFirstSync) {
            let Some(position) = bytes
                .windows(self.sync.len())
                .position(|candidate| candidate == self.sync)
            else {
                self.next_offset = None;
                return Ok(());
            };
            bytes = &bytes[position + self.sync.len()..];
            self.state = OcfBlockValidationState::Count(AvroLongDecoder::default());
        }

        while !bytes.is_empty() {
            match &mut self.state {
                OcfBlockValidationState::SeekingFirstSync => unreachable!("handled above"),
                OcfBlockValidationState::Count(decoder) => {
                    let byte = bytes[0];
                    bytes = &bytes[1..];
                    if let Some(count) = decoder.push(byte)? {
                        if count < 0 {
                            return Err(CdfError::data(format!(
                                "Avro OCF block count cannot be negative: {count}"
                            )));
                        }
                        let count = u64::try_from(count)
                            .map_err(|_| CdfError::data("Avro OCF block count exceeds u64"))?;
                        if count == 0 || count > self.maximum_block_records {
                            return Err(CdfError::data(format!(
                                "Avro OCF block declares {count} records outside the configured 1..={} record authority; increase format_options.maximum_block_records only for a trusted producer",
                                self.maximum_block_records
                            )));
                        }
                        self.state = OcfBlockValidationState::Size(AvroLongDecoder::default());
                    }
                }
                OcfBlockValidationState::Size(decoder) => {
                    let byte = bytes[0];
                    bytes = &bytes[1..];
                    if let Some(size) = decoder.push(byte)? {
                        let size = u64::try_from(size).map_err(|_| {
                            CdfError::data(format!(
                                "Avro OCF block size cannot be negative: {size}"
                            ))
                        })?;
                        if size > self.maximum_block_bytes {
                            return Err(CdfError::data(format!(
                                "Avro OCF block declares {size} encoded bytes above the configured {}-byte maximum; increase format_options.maximum_block_bytes only for a trusted producer",
                                self.maximum_block_bytes
                            )));
                        }
                        self.state = OcfBlockValidationState::Data(size);
                    }
                }
                OcfBlockValidationState::Data(remaining) => {
                    let available = u64::try_from(bytes.len())
                        .map_err(|_| CdfError::data("Avro OCF range length exceeds u64"))?;
                    let consumed = available.min(*remaining);
                    let consumed_usize = usize::try_from(consumed)
                        .map_err(|_| CdfError::data("Avro OCF block length exceeds usize"))?;
                    bytes = &bytes[consumed_usize..];
                    *remaining -= consumed;
                    if *remaining == 0 {
                        self.state = OcfBlockValidationState::Sync(0);
                    }
                }
                OcfBlockValidationState::Sync(matched) => {
                    let remaining = self.sync.len() - *matched;
                    let compared = remaining.min(bytes.len());
                    if bytes[..compared] != self.sync[*matched..*matched + compared] {
                        return Err(CdfError::data(
                            "Avro OCF block sync marker does not match the file header",
                        ));
                    }
                    *matched += compared;
                    bytes = &bytes[compared..];
                    if *matched == self.sync.len() {
                        self.state = OcfBlockValidationState::Count(AvroLongDecoder::default());
                    }
                }
            }
        }
        self.next_offset = Some(
            offset
                .checked_add(input_length)
                .ok_or_else(|| CdfError::data("Avro OCF validation offset overflowed"))?,
        );
        Ok(())
    }
}
