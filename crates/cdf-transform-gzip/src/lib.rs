use std::sync::Arc;

use bytes::Bytes;
use cdf_kernel::{CdfError, Result};
use cdf_memory::{AccountedBytes, MemoryLease, ReservationRequest, reserve};
use cdf_runtime::{
    AccountedByteStream, ByteTransformDescriptor, ByteTransformDriver, ByteTransformId,
    ByteTransformRequest, MagicSignature, TransformChecksumBehavior,
};
use crc32fast::Hasher;
use flate2::{Decompress, FlushDecompress, Status};
use futures_util::{StreamExt, stream};

const GZIP_MAGIC: &[u8; 2] = b"\x1f\x8b";
const MAXIMUM_HEADER_BYTES: usize = 64 * 1024;
const INTERNAL_WORKING_SET_BYTES: u64 = 128 * 1024;
const DEFAULT_MAXIMUM_WORKING_SET_BYTES: u64 = 32 * 1024 * 1024;
const DEFAULT_MAXIMUM_EXPANDED_BYTES: u64 = 4 * 1024 * 1024 * 1024 * 1024;
const DEFAULT_MAXIMUM_EXPANSION_RATIO: u32 = 1_000;

#[derive(Debug)]
pub struct GzipTransformDriver {
    descriptor: ByteTransformDescriptor,
}

impl GzipTransformDriver {
    pub fn new() -> Result<Self> {
        Ok(Self {
            descriptor: ByteTransformDescriptor {
                transform_id: ByteTransformId::new("gzip")?,
                semantic_version: "1.0.0".to_owned(),
                extensions: vec!["gz".to_owned(), "gzip".to_owned()],
                magic: vec![MagicSignature {
                    offset: 0,
                    bytes: GZIP_MAGIC.to_vec(),
                    strong: true,
                }],
                preserves_random_access: false,
                splittable: false,
                supports_concatenated_members: true,
                maximum_working_set_bytes: DEFAULT_MAXIMUM_WORKING_SET_BYTES,
                maximum_expanded_bytes: DEFAULT_MAXIMUM_EXPANDED_BYTES,
                maximum_expansion_ratio: DEFAULT_MAXIMUM_EXPANSION_RATIO,
                checksum: TransformChecksumBehavior::Required,
            },
        })
    }
}

impl ByteTransformDriver for GzipTransformDriver {
    fn descriptor(&self) -> &ByteTransformDescriptor {
        &self.descriptor
    }

    fn transform(
        &self,
        input: AccountedByteStream,
        request: ByteTransformRequest,
    ) -> Result<AccountedByteStream> {
        request.validate_for(&self.descriptor)?;
        if request
            .preferred_output_chunk_bytes
            .checked_add(INTERNAL_WORKING_SET_BYTES)
            .is_none_or(|bytes| bytes > self.descriptor.maximum_working_set_bytes)
        {
            return Err(CdfError::contract(
                "gzip output chunk plus native decoder working set exceeds driver authority",
            ));
        }
        let output_chunk_bytes = usize::try_from(request.preferred_output_chunk_bytes)
            .map_err(|_| CdfError::contract("gzip output chunk exceeds usize"))?;
        let state = GzipState {
            input: InputCursor::new(input),
            request,
            output_chunk_bytes,
            decoder: None,
            member_crc: Hasher::new(),
            member_expanded_bytes: 0,
            total_expanded_bytes: 0,
            members: 0,
            working_set_lease: None,
        };
        Ok(Box::pin(stream::try_unfold(
            state,
            |mut state| async move {
                let output = state.next_output().await?;
                Ok(output.map(|bytes| (bytes, state)))
            },
        )))
    }
}

struct GzipState {
    input: InputCursor,
    request: ByteTransformRequest,
    output_chunk_bytes: usize,
    decoder: Option<Decompress>,
    member_crc: Hasher,
    member_expanded_bytes: u64,
    total_expanded_bytes: u64,
    members: u64,
    working_set_lease: Option<MemoryLease>,
}

impl GzipState {
    async fn next_output(&mut self) -> Result<Option<AccountedBytes>> {
        loop {
            self.request.cancellation.check()?;
            if self.working_set_lease.is_none() {
                let reservation = ReservationRequest::new(
                    self.request.consumer.clone(),
                    INTERNAL_WORKING_SET_BYTES,
                )?
                .as_minimum_working_set();
                self.working_set_lease =
                    Some(reserve(Arc::clone(&self.request.memory), reservation).await?);
            }
            if self.decoder.is_none() {
                if !self.read_header().await? {
                    if self.members == 0 {
                        return Err(CdfError::data("gzip input is empty"));
                    }
                    self.enforce_terminal_ratio()?;
                    return Ok(None);
                }
                self.decoder = Some(Decompress::new(false));
                self.member_crc = Hasher::new();
                self.member_expanded_bytes = 0;
            }

            if !self.input.ensure_current().await? {
                return Err(CdfError::data(
                    "gzip input ended before the deflate stream and trailer completed",
                ));
            }

            let reservation = ReservationRequest::new(
                self.request.consumer.clone(),
                u64::try_from(self.output_chunk_bytes)
                    .map_err(|_| CdfError::data("gzip output chunk exceeds u64"))?,
            )?;
            let lease = reserve(Arc::clone(&self.request.memory), reservation).await?;
            let mut output = vec![0_u8; self.output_chunk_bytes];
            let input = self.input.current_slice();
            let decoder = self
                .decoder
                .as_mut()
                .ok_or_else(|| CdfError::internal("gzip decoder was not initialized"))?;
            let input_before = decoder.total_in();
            let output_before = decoder.total_out();
            let status = decoder
                .decompress(input, &mut output, FlushDecompress::None)
                .map_err(|error| {
                    CdfError::data(format!("gzip deflate stream is corrupt: {error}"))
                })?;
            let consumed = usize::try_from(decoder.total_in() - input_before)
                .map_err(|_| CdfError::data("gzip consumed-byte count exceeds usize"))?;
            let produced = usize::try_from(decoder.total_out() - output_before)
                .map_err(|_| CdfError::data("gzip expanded-byte count exceeds usize"))?;
            self.input.consume(consumed)?;
            output.truncate(produced);

            if produced > 0 {
                self.member_crc.update(&output);
                self.member_expanded_bytes = checked_add_bytes(
                    self.member_expanded_bytes,
                    produced,
                    "gzip member expanded-byte count overflowed",
                )?;
                self.total_expanded_bytes = checked_add_bytes(
                    self.total_expanded_bytes,
                    produced,
                    "gzip total expanded-byte count overflowed",
                )?;
                self.enforce_expansion_limits(false)?;
            }

            if status == Status::StreamEnd {
                self.decoder = None;
                self.verify_trailer().await?;
                self.members = self
                    .members
                    .checked_add(1)
                    .ok_or_else(|| CdfError::data("gzip member count overflowed"))?;
            } else if consumed == 0 && produced == 0 {
                return Err(CdfError::data(
                    "gzip decoder made no progress on a nonempty compressed chunk",
                ));
            }

            if produced > 0 {
                return Ok(Some(AccountedBytes::new(Bytes::from(output), lease)?));
            }
        }
    }

    async fn read_header(&mut self) -> Result<bool> {
        let Some(first) = self.input.next_byte().await? else {
            return Ok(false);
        };
        let mut header = Vec::with_capacity(10);
        header.push(first);
        for _ in 1..10 {
            header.push(self.required_header_byte().await?);
        }
        if &header[..2] != GZIP_MAGIC {
            return Err(CdfError::data(format!(
                "gzip member {} has invalid magic",
                self.members
            )));
        }
        if header[2] != 8 {
            return Err(CdfError::data(format!(
                "gzip member {} uses unsupported compression method {}",
                self.members, header[2]
            )));
        }
        let flags = header[3];
        if flags & 0b1110_0000 != 0 {
            return Err(CdfError::data(format!(
                "gzip member {} sets reserved header flags",
                self.members
            )));
        }
        if flags & 0x04 != 0 {
            let low = self.required_header_byte().await?;
            let high = self.required_header_byte().await?;
            header.extend_from_slice(&[low, high]);
            let extra_len = usize::from(u16::from_le_bytes([low, high]));
            self.extend_header_exact(&mut header, extra_len).await?;
        }
        if flags & 0x08 != 0 {
            self.extend_header_c_string(&mut header).await?;
        }
        if flags & 0x10 != 0 {
            self.extend_header_c_string(&mut header).await?;
        }
        if flags & 0x02 != 0 {
            let expected = (crc32fast::hash(&header) & 0xffff) as u16;
            let actual = u16::from_le_bytes([
                self.required_header_byte().await?,
                self.required_header_byte().await?,
            ]);
            if actual != expected {
                return Err(CdfError::data(format!(
                    "gzip member {} header checksum mismatch",
                    self.members
                )));
            }
        }
        Ok(true)
    }

    async fn required_header_byte(&mut self) -> Result<u8> {
        self.input.next_byte().await?.ok_or_else(|| {
            CdfError::data(format!(
                "gzip member {} ended inside its header",
                self.members
            ))
        })
    }

    async fn extend_header_exact(&mut self, header: &mut Vec<u8>, count: usize) -> Result<()> {
        let final_len = header
            .len()
            .checked_add(count)
            .ok_or_else(|| CdfError::data("gzip header length overflowed"))?;
        if final_len > MAXIMUM_HEADER_BYTES {
            return Err(CdfError::data(format!(
                "gzip member {} header exceeds the {}-byte safety limit",
                self.members, MAXIMUM_HEADER_BYTES
            )));
        }
        for _ in 0..count {
            header.push(self.required_header_byte().await?);
        }
        Ok(())
    }

    async fn extend_header_c_string(&mut self, header: &mut Vec<u8>) -> Result<()> {
        loop {
            if header.len() == MAXIMUM_HEADER_BYTES {
                return Err(CdfError::data(format!(
                    "gzip member {} header exceeds the {}-byte safety limit",
                    self.members, MAXIMUM_HEADER_BYTES
                )));
            }
            let byte = self.required_header_byte().await?;
            header.push(byte);
            if byte == 0 {
                return Ok(());
            }
        }
    }

    async fn verify_trailer(&mut self) -> Result<()> {
        let mut trailer = [0_u8; 8];
        for byte in &mut trailer {
            *byte = self.input.next_byte().await?.ok_or_else(|| {
                CdfError::data(format!(
                    "gzip member {} ended inside its checksum trailer",
                    self.members
                ))
            })?;
        }
        let expected_crc = u32::from_le_bytes(trailer[..4].try_into().unwrap());
        let expected_size = u32::from_le_bytes(trailer[4..].try_into().unwrap());
        let actual_crc = self.member_crc.clone().finalize();
        let actual_size = self.member_expanded_bytes as u32;
        if actual_crc != expected_crc {
            return Err(CdfError::data(format!(
                "gzip member {} payload checksum mismatch",
                self.members
            )));
        }
        if actual_size != expected_size {
            return Err(CdfError::data(format!(
                "gzip member {} expanded-size trailer mismatch: expected {expected_size}, observed modulo-2^32 {actual_size}",
                self.members
            )));
        }
        self.enforce_expansion_limits(true)
    }

    fn enforce_expansion_limits(&self, member_complete: bool) -> Result<()> {
        if self.total_expanded_bytes > self.request.maximum_expanded_bytes {
            return Err(CdfError::data(format!(
                "gzip expansion produced {} bytes, exceeding the configured {}-byte ceiling",
                self.total_expanded_bytes, self.request.maximum_expanded_bytes
            )));
        }
        let compressed = self.input.consumed_bytes;
        let ratio_ceiling = compressed
            .checked_mul(u64::from(self.request.maximum_expansion_ratio))
            .ok_or_else(|| CdfError::data("gzip expansion-ratio calculation overflowed"))?;
        let streaming_grace = if member_complete {
            0
        } else {
            self.request.preferred_output_chunk_bytes
        };
        if self.total_expanded_bytes > ratio_ceiling.saturating_add(streaming_grace) {
            return Err(CdfError::data(format!(
                "gzip expansion ratio exceeds the configured {}:1 ceiling after {} compressed bytes",
                self.request.maximum_expansion_ratio, compressed
            )));
        }
        Ok(())
    }

    fn enforce_terminal_ratio(&self) -> Result<()> {
        self.enforce_expansion_limits(true)
    }
}

struct InputCursor {
    stream: AccountedByteStream,
    current: Option<AccountedBytes>,
    offset: usize,
    consumed_bytes: u64,
}

impl InputCursor {
    fn new(stream: AccountedByteStream) -> Self {
        Self {
            stream,
            current: None,
            offset: 0,
            consumed_bytes: 0,
        }
    }

    async fn ensure_current(&mut self) -> Result<bool> {
        while self
            .current
            .as_ref()
            .is_none_or(|chunk| self.offset == chunk.payload().len())
        {
            self.current = None;
            self.current = self.stream.next().await.transpose()?;
            self.offset = 0;
            if self.current.is_none() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn current_slice(&self) -> &[u8] {
        self.current
            .as_ref()
            .map(|chunk| &chunk.payload()[self.offset..])
            .unwrap_or_default()
    }

    fn consume(&mut self, bytes: usize) -> Result<()> {
        let available = self.current_slice().len();
        if bytes > available {
            return Err(CdfError::internal(
                "gzip decoder consumed beyond its current input chunk",
            ));
        }
        self.offset += bytes;
        self.consumed_bytes = checked_add_bytes(
            self.consumed_bytes,
            bytes,
            "gzip compressed-byte count overflowed",
        )?;
        Ok(())
    }

    async fn next_byte(&mut self) -> Result<Option<u8>> {
        if !self.ensure_current().await? {
            return Ok(None);
        }
        let byte = self.current_slice()[0];
        self.consume(1)?;
        Ok(Some(byte))
    }
}

fn checked_add_bytes(current: u64, additional: usize, message: &str) -> Result<u64> {
    current
        .checked_add(
            u64::try_from(additional).map_err(|_| CdfError::data("byte count exceeds u64"))?,
        )
        .ok_or_else(|| CdfError::data(message))
}

#[cfg(test)]
mod tests {
    use std::{io::Write, sync::Arc};

    use cdf_memory::{
        ConsumerKey, DeterministicMemoryCoordinator, MemoryClass, MemoryCoordinator, MemorySnapshot,
    };
    use flate2::{Compression, write::GzEncoder};
    use futures_executor::block_on;

    use super::*;

    fn encode_member(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    }

    fn input_stream(
        bytes: Vec<u8>,
        chunk_bytes: usize,
        memory: Arc<dyn MemoryCoordinator>,
    ) -> AccountedByteStream {
        let bytes: Arc<[u8]> = bytes.into();
        let consumer = ConsumerKey::new("gzip-test-input", MemoryClass::Source).unwrap();
        Box::pin(stream::try_unfold(
            (bytes, 0_usize, memory, consumer),
            move |(bytes, offset, memory, consumer)| async move {
                if offset == bytes.len() {
                    return Ok(None);
                }
                let end = offset.saturating_add(chunk_bytes).min(bytes.len());
                let len = end - offset;
                let reservation =
                    ReservationRequest::new(consumer.clone(), u64::try_from(len).unwrap())?;
                let lease = reserve(Arc::clone(&memory), reservation).await?;
                let chunk =
                    AccountedBytes::new(Bytes::copy_from_slice(&bytes[offset..end]), lease)?;
                Ok(Some((chunk, (bytes, end, memory, consumer))))
            },
        ))
    }

    fn decode(
        compressed: Vec<u8>,
        input_chunk_bytes: usize,
        output_chunk_bytes: u64,
        maximum_expanded_bytes: u64,
        maximum_expansion_ratio: u32,
        cancellation: cdf_runtime::RunCancellation,
    ) -> (Result<Vec<u8>>, MemorySnapshot) {
        let input_size_bytes = u64::try_from(compressed.len()).unwrap();
        let memory: Arc<dyn MemoryCoordinator> = Arc::new(
            DeterministicMemoryCoordinator::new(4 * 1024 * 1024, Default::default()).unwrap(),
        );
        let input = input_stream(compressed, input_chunk_bytes, Arc::clone(&memory));
        let request = ByteTransformRequest {
            preferred_output_chunk_bytes: output_chunk_bytes,
            maximum_expanded_bytes,
            maximum_expansion_ratio,
            input_size_bytes: Some(input_size_bytes),
            memory: Arc::clone(&memory),
            consumer: ConsumerKey::new("gzip-test", MemoryClass::Transform).unwrap(),
            cancellation,
        };
        let driver = GzipTransformDriver::new().unwrap();
        let result = driver.transform(input, request).and_then(|mut output| {
            block_on(async move {
                let mut decoded = Vec::new();
                while let Some(chunk) = output.next().await {
                    decoded.extend_from_slice(chunk?.payload());
                }
                Ok(decoded)
            })
        });
        let snapshot = memory.snapshot();
        (result, snapshot)
    }

    #[test]
    fn streams_concatenated_members_across_single_byte_input_chunks() {
        let first = b"first member\n".repeat(31);
        let second = b"second member\n".repeat(37);
        let mut compressed = encode_member(&first);
        compressed.extend_from_slice(&encode_member(&second));

        let (decoded, snapshot) = decode(compressed, 1, 29, 1024 * 1024, 100, Default::default());
        let mut expected = first;
        expected.extend_from_slice(&second);
        assert_eq!(decoded.unwrap(), expected);
        assert_eq!(snapshot.current_bytes, 0);
        assert!(
            snapshot.peak_bytes <= INTERNAL_WORKING_SET_BYTES + 30,
            "unexpected peak: {snapshot:?}"
        );
    }

    #[test]
    fn rejects_payload_checksum_corruption_and_truncation() {
        let mut corrupted = encode_member(b"checksum authority");
        let crc_offset = corrupted.len() - 8;
        corrupted[crc_offset] ^= 0xff;
        let (corrupt_result, _) = decode(corrupted, 7, 32, 1024, 100, Default::default());
        assert!(corrupt_result.unwrap_err().to_string().contains("checksum"));

        let mut truncated = encode_member(b"trailer authority");
        truncated.truncate(truncated.len() - 3);
        let (truncated_result, _) = decode(truncated, 5, 32, 1024, 100, Default::default());
        assert!(
            truncated_result
                .unwrap_err()
                .to_string()
                .contains("trailer")
        );
    }

    #[test]
    fn enforces_expanded_byte_ratio_and_cancellation_authority() {
        let compressed = encode_member(&vec![0_u8; 4096]);
        let (bytes_result, _) = decode(compressed.clone(), 11, 64, 1024, 1000, Default::default());
        assert!(bytes_result.unwrap_err().to_string().contains("ceiling"));

        let (ratio_result, _) = decode(compressed.clone(), 11, 64, 8192, 2, Default::default());
        assert!(ratio_result.unwrap_err().to_string().contains("ratio"));

        let cancellation = cdf_runtime::RunCancellation::default();
        cancellation.cancel();
        let (cancelled_result, _) = decode(compressed, 11, 64, 8192, 1000, cancellation);
        assert!(
            cancelled_result
                .unwrap_err()
                .to_string()
                .contains("cancelled")
        );
    }
}
