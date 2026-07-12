use std::sync::Arc;

use bytes::Bytes;
use cdf_kernel::{CdfError, Result};
use cdf_memory::{AccountedBytes, MemoryLease, ReservationRequest, reserve};
use cdf_runtime::{
    AccountedByteCursor, AccountedByteStream, ByteTransformDescriptor, ByteTransformDriver,
    ByteTransformId, ByteTransformRequest, MagicSignature, TransformChecksumBehavior,
    TransformExpansionGuard,
};
use futures_util::stream;

const STREAM_IDENTIFIER: &[u8; 10] = b"\xff\x06\x00\x00sNaPpY";
const MAXIMUM_CHUNK_PAYLOAD_BYTES: usize = 65_540;
const MAXIMUM_DECODED_CHUNK_BYTES: usize = 65_536;
const INTERNAL_WORKING_SET_BYTES: u64 = 160 * 1024;
const DEFAULT_MAXIMUM_WORKING_SET_BYTES: u64 = 16 * 1024 * 1024 + INTERNAL_WORKING_SET_BYTES;
const DEFAULT_MAXIMUM_EXPANDED_BYTES: u64 = 4 * 1024 * 1024 * 1024 * 1024;
const DEFAULT_MAXIMUM_EXPANSION_RATIO: u32 = 1_000;

#[derive(Debug)]
pub struct SnappyFramedTransformDriver {
    descriptor: ByteTransformDescriptor,
}

impl SnappyFramedTransformDriver {
    pub fn new() -> Result<Self> {
        Ok(Self {
            descriptor: ByteTransformDescriptor {
                transform_id: ByteTransformId::new("snappy_framed")?,
                semantic_version: "1.0.0".to_owned(),
                extensions: vec!["sz".to_owned(), "snappy".to_owned()],
                magic: vec![MagicSignature {
                    offset: 0,
                    bytes: STREAM_IDENTIFIER.to_vec(),
                    strong: true,
                }],
                preserves_random_access: false,
                splittable: false,
                supports_concatenated_members: true,
                maximum_output_chunk_bytes: 16 * 1024 * 1024,
                maximum_working_set_bytes: DEFAULT_MAXIMUM_WORKING_SET_BYTES,
                maximum_expanded_bytes: DEFAULT_MAXIMUM_EXPANDED_BYTES,
                maximum_expansion_ratio: DEFAULT_MAXIMUM_EXPANSION_RATIO,
                checksum: TransformChecksumBehavior::Required,
            },
        })
    }
}

impl ByteTransformDriver for SnappyFramedTransformDriver {
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
                "Snappy output chunk plus bounded frame working set exceeds driver authority",
            ));
        }
        let output_chunk_bytes = usize::try_from(request.preferred_output_chunk_bytes)
            .map_err(|_| CdfError::contract("Snappy output chunk exceeds usize"))?;
        let expansion = TransformExpansionGuard::new(&request)?;
        let state = SnappyState {
            input: AccountedByteCursor::new(input),
            request,
            output_chunk_bytes,
            expansion,
            working_set_lease: None,
            stream_started: false,
            decoded_chunk: Vec::new(),
            decoded_offset: 0,
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

struct SnappyState {
    input: AccountedByteCursor,
    request: ByteTransformRequest,
    output_chunk_bytes: usize,
    expansion: TransformExpansionGuard,
    working_set_lease: Option<MemoryLease>,
    stream_started: bool,
    decoded_chunk: Vec<u8>,
    decoded_offset: usize,
}

impl SnappyState {
    async fn next_output(&mut self) -> Result<Option<AccountedBytes>> {
        loop {
            self.request.cancellation.check()?;
            self.ensure_working_set().await?;
            if self.decoded_offset < self.decoded_chunk.len() {
                return self.emit_decoded_slice().await.map(Some);
            }
            self.decoded_chunk.clear();
            self.decoded_offset = 0;

            let Some(chunk_type) = self.input.next_byte().await? else {
                if !self.stream_started {
                    return Err(CdfError::data(
                        "Snappy framed input is empty or missing its stream identifier",
                    ));
                }
                self.expansion
                    .enforce_exact_ratio(self.input.consumed_bytes())?;
                return Ok(None);
            };
            let length = read_u24(&mut self.input).await?;
            if !self.stream_started && chunk_type != 0xff {
                return Err(CdfError::data(
                    "Snappy framed input is missing its required stream identifier; raw unframed Snappy requires explicit framing metadata and is not accepted here",
                ));
            }
            if length > MAXIMUM_CHUNK_PAYLOAD_BYTES {
                return Err(CdfError::data(format!(
                    "Snappy framed chunk length {length} exceeds the {MAXIMUM_CHUNK_PAYLOAD_BYTES}-byte safety limit"
                )));
            }
            match chunk_type {
                0xff => self.read_stream_identifier(length).await?,
                0x00 | 0x01 => self.read_data_chunk(chunk_type, length).await?,
                0x80..=0xfe => self.skip_chunk(length).await?,
                0x02..=0x7f => {
                    return Err(CdfError::data(format!(
                        "Snappy framed input contains unsupported reserved chunk type 0x{chunk_type:02x}"
                    )));
                }
            }
        }
    }

    async fn ensure_working_set(&mut self) -> Result<()> {
        if self.working_set_lease.is_none() {
            let reservation =
                ReservationRequest::new(self.request.consumer.clone(), INTERNAL_WORKING_SET_BYTES)?
                    .as_minimum_working_set();
            self.working_set_lease =
                Some(reserve(Arc::clone(&self.request.memory), reservation).await?);
        }
        Ok(())
    }

    async fn read_stream_identifier(&mut self, length: usize) -> Result<()> {
        if length != 6 {
            return Err(CdfError::data(format!(
                "Snappy stream identifier has length {length}; expected 6"
            )));
        }
        let body = self
            .input
            .read_exact(length, "Snappy stream identifier")
            .await?;
        if body.as_slice() != &STREAM_IDENTIFIER[4..] {
            return Err(CdfError::data("Snappy stream identifier body is invalid"));
        }
        self.stream_started = true;
        Ok(())
    }

    async fn read_data_chunk(&mut self, chunk_type: u8, length: usize) -> Result<()> {
        if !self.stream_started {
            return Err(CdfError::data(
                "Snappy data chunk appeared before the required stream identifier",
            ));
        }
        if length < 4 {
            return Err(CdfError::data(format!(
                "Snappy data chunk length {length} is smaller than its checksum"
            )));
        }
        let payload = self.input.read_exact(length, "Snappy data chunk").await?;
        let expected_checksum =
            u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
        let encoded = &payload[4..];
        self.decoded_chunk.clear();
        if chunk_type == 0x01 {
            self.decoded_chunk.extend_from_slice(encoded);
        } else {
            let decoded_len = snap::raw::decompress_len(encoded).map_err(snappy_error)?;
            if decoded_len > MAXIMUM_DECODED_CHUNK_BYTES {
                return Err(CdfError::data(format!(
                    "Snappy framed chunk declares {decoded_len} decoded bytes, exceeding the {MAXIMUM_DECODED_CHUNK_BYTES}-byte format limit"
                )));
            }
            self.decoded_chunk.resize(decoded_len, 0);
            let observed = snap::raw::Decoder::new()
                .decompress(encoded, &mut self.decoded_chunk)
                .map_err(snappy_error)?;
            if observed != decoded_len {
                return Err(CdfError::data(format!(
                    "Snappy raw decoder produced {observed} bytes; frame declared {decoded_len}"
                )));
            }
        }
        if self.decoded_chunk.len() > MAXIMUM_DECODED_CHUNK_BYTES {
            return Err(CdfError::data(format!(
                "Snappy framed chunk expanded to {} bytes, exceeding the {MAXIMUM_DECODED_CHUNK_BYTES}-byte format limit",
                self.decoded_chunk.len()
            )));
        }
        let checksum = crc32c::crc32c(&self.decoded_chunk)
            .rotate_right(15)
            .wrapping_add(0xa282_ead8);
        if checksum != expected_checksum {
            return Err(CdfError::data(format!(
                "Snappy framed chunk checksum mismatch: expected {expected_checksum:#010x}, observed {checksum:#010x}"
            )));
        }
        self.expansion
            .record(self.decoded_chunk.len(), self.input.consumed_bytes(), true)
    }

    async fn skip_chunk(&mut self, length: usize) -> Result<()> {
        self.input
            .skip_exact(length, "Snappy skippable chunk")
            .await
    }

    async fn emit_decoded_slice(&mut self) -> Result<AccountedBytes> {
        let end = self
            .decoded_offset
            .saturating_add(self.output_chunk_bytes)
            .min(self.decoded_chunk.len());
        let bytes = end - self.decoded_offset;
        let reservation = ReservationRequest::new(
            self.request.consumer.clone(),
            u64::try_from(bytes).map_err(|_| CdfError::data("Snappy output slice exceeds u64"))?,
        )?;
        let lease = reserve(Arc::clone(&self.request.memory), reservation).await?;
        let output = if self.decoded_offset == 0 && end == self.decoded_chunk.len() {
            self.decoded_offset = 0;
            Bytes::from(std::mem::take(&mut self.decoded_chunk))
        } else {
            let output = Bytes::copy_from_slice(&self.decoded_chunk[self.decoded_offset..end]);
            self.decoded_offset = end;
            output
        };
        AccountedBytes::new(output, lease)
    }
}

async fn read_u24(input: &mut AccountedByteCursor) -> Result<usize> {
    let low = required_byte(input, "Snappy chunk length").await?;
    let middle = required_byte(input, "Snappy chunk length").await?;
    let high = required_byte(input, "Snappy chunk length").await?;
    Ok(usize::from(low) | (usize::from(middle) << 8) | (usize::from(high) << 16))
}

async fn required_byte(input: &mut AccountedByteCursor, label: &str) -> Result<u8> {
    input
        .next_byte()
        .await?
        .ok_or_else(|| CdfError::data(format!("{label} ended before its declared length")))
}

fn snappy_error(error: snap::Error) -> CdfError {
    CdfError::data(format!("Snappy framed chunk is corrupt: {error}"))
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        sync::Arc,
        time::Instant,
    };

    use cdf_memory::{
        ConsumerKey, DeterministicMemoryCoordinator, MemoryClass, MemoryCoordinator, MemorySnapshot,
    };
    use futures_executor::block_on;
    use futures_util::StreamExt;

    use super::*;

    fn encode_stream(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = snap::write::FrameEncoder::new(Vec::new());
        encoder.write_all(bytes).unwrap();
        encoder.into_inner().unwrap()
    }

    fn input_stream(
        bytes: Vec<u8>,
        chunk_bytes: usize,
        memory: Arc<dyn MemoryCoordinator>,
    ) -> AccountedByteStream {
        let bytes: Arc<[u8]> = bytes.into();
        let consumer = ConsumerKey::new("snappy-test-input", MemoryClass::Source).unwrap();
        Box::pin(stream::try_unfold(
            (bytes, 0_usize, memory, consumer),
            move |(bytes, offset, memory, consumer)| async move {
                if offset == bytes.len() {
                    return Ok(None);
                }
                let end = offset.saturating_add(chunk_bytes).min(bytes.len());
                let reservation = ReservationRequest::new(
                    consumer.clone(),
                    u64::try_from(end - offset).unwrap(),
                )?;
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
            maximum_expansion_ratio: 1000,
            input_size_bytes: Some(input_size_bytes),
            memory: Arc::clone(&memory),
            consumer: ConsumerKey::new("snappy-test", MemoryClass::Transform).unwrap(),
            cancellation,
        };
        let driver = SnappyFramedTransformDriver::new().unwrap();
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
    fn streams_concatenated_identifiers_and_verified_chunks_at_arbitrary_boundaries() {
        let first = b"first snappy stream\n".repeat(4000);
        let second = b"second snappy stream\n".repeat(2000);
        let mut compressed = encode_stream(&first);
        compressed.extend_from_slice(&[0xfe, 3, 0, 0, 1, 2, 3]);
        compressed.extend_from_slice(&encode_stream(&second));
        let (decoded, snapshot) = decode(compressed, 1, 4096, 1024 * 1024, Default::default());
        let mut expected = first;
        expected.extend_from_slice(&second);
        assert_eq!(decoded.unwrap(), expected);
        assert_eq!(snapshot.current_bytes, 0);
        assert!(snapshot.peak_bytes <= INTERNAL_WORKING_SET_BYTES + 4097);
    }

    #[test]
    fn rejects_checksum_corruption_truncation_and_raw_snappy() {
        let mut corrupt = encode_stream(b"checksum authority");
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0xff;
        assert!(
            decode(corrupt, 7, 1024, 1024, Default::default())
                .0
                .is_err()
        );

        let mut truncated = encode_stream(b"truncation authority");
        truncated.pop();
        assert!(
            decode(truncated, 5, 1024, 1024, Default::default())
                .0
                .unwrap_err()
                .to_string()
                .contains("declared length")
        );

        let raw = snap::raw::Encoder::new()
            .compress_vec(b"raw is not framed")
            .unwrap();
        assert!(
            decode(raw, 4, 1024, 1024, Default::default())
                .0
                .unwrap_err()
                .to_string()
                .contains("stream identifier")
        );
    }

    #[test]
    fn enforces_expansion_and_cancellation_authority() {
        let compressed = encode_stream(&vec![0_u8; 4096]);
        assert!(
            decode(compressed.clone(), 11, 1024, 1024, Default::default())
                .0
                .unwrap_err()
                .to_string()
                .contains("ceiling")
        );
        let cancellation = cdf_runtime::RunCancellation::default();
        cancellation.cancel();
        assert!(
            decode(compressed, 11, 1024, 8192, cancellation)
                .0
                .unwrap_err()
                .to_string()
                .contains("cancelled")
        );
    }

    #[test]
    #[ignore = "performance evidence; run in release mode"]
    fn snappy_driver_reference_rate() {
        const BYTES: usize = 64 * 1024 * 1024;
        let input = (0..BYTES)
            .map(|index| ((index.wrapping_mul(31) ^ (index >> 7)) & 0xff) as u8)
            .collect::<Vec<_>>();
        let compressed = encode_stream(&input);

        let reference_start = Instant::now();
        let mut reference = Vec::with_capacity(input.len());
        snap::read::FrameDecoder::new(compressed.as_slice())
            .read_to_end(&mut reference)
            .unwrap();
        let reference_elapsed = reference_start.elapsed();
        assert_eq!(reference, input);

        let driver_start = Instant::now();
        let (decoded, _) = decode(
            compressed,
            1024 * 1024,
            1024 * 1024,
            128 * 1024 * 1024,
            Default::default(),
        );
        let driver_elapsed = driver_start.elapsed();
        assert_eq!(decoded.unwrap(), input);
        let reference_ratio = reference_elapsed.as_secs_f64() / driver_elapsed.as_secs_f64();
        eprintln!(
            "snappy_reference_ms={:.3} snappy_driver_ms={:.3} reference_ratio={reference_ratio:.3}",
            reference_elapsed.as_secs_f64() * 1000.0,
            driver_elapsed.as_secs_f64() * 1000.0,
        );
        assert!(
            reference_ratio >= 0.6,
            "Snappy driver achieved {reference_ratio:.3}x reference throughput"
        );
    }
}
