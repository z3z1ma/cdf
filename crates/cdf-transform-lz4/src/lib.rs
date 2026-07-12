use std::{hash::Hasher, sync::Arc};

use bytes::Bytes;
use cdf_kernel::{CdfError, Result};
use cdf_memory::{AccountedBytes, MemoryLease, ReservationRequest, reserve};
use cdf_runtime::{
    AccountedByteCursor, AccountedByteStream, ByteTransformDescriptor, ByteTransformDriver,
    ByteTransformId, ByteTransformRequest, MagicSignature, TransformChecksumBehavior,
    TransformExpansionGuard,
};
use futures_util::stream;
use twox_hash::XxHash32;

const LZ4_MAGIC: [u8; 4] = 0x184d_2204_u32.to_le_bytes();
const LZ4_LEGACY_MAGIC: u32 = 0x184c_2102;
const SKIPPABLE_MAGIC_START: u32 = 0x184d_2a50;
const SKIPPABLE_MAGIC_END: u32 = 0x184d_2a5f;
const MAX_BLOCK_BYTES: usize = 4 * 1024 * 1024;
const HISTORY_BYTES: usize = 64 * 1024;
const MAX_SKIPPABLE_BYTES: usize = 16 * 1024 * 1024;
const MAX_INTERNAL_WORKING_SET_BYTES: u64 = (MAX_BLOCK_BYTES * 2 + HISTORY_BYTES) as u64;
const DEFAULT_MAXIMUM_WORKING_SET_BYTES: u64 = 48 * 1024 * 1024;
const DEFAULT_MAXIMUM_EXPANDED_BYTES: u64 = 4 * 1024 * 1024 * 1024 * 1024;
const DEFAULT_MAXIMUM_EXPANSION_RATIO: u32 = 1_000;

#[derive(Debug)]
pub struct Lz4FrameTransformDriver {
    descriptor: ByteTransformDescriptor,
}

impl Lz4FrameTransformDriver {
    pub fn new() -> Result<Self> {
        Ok(Self {
            descriptor: ByteTransformDescriptor {
                transform_id: ByteTransformId::new("lz4_frame")?,
                semantic_version: "1.0.0".to_owned(),
                extensions: vec!["lz4".to_owned()],
                magic: vec![MagicSignature {
                    offset: 0,
                    bytes: LZ4_MAGIC.to_vec(),
                    strong: true,
                }],
                preserves_random_access: false,
                splittable: false,
                supports_concatenated_members: true,
                maximum_output_chunk_bytes: 32 * 1024 * 1024,
                maximum_working_set_bytes: DEFAULT_MAXIMUM_WORKING_SET_BYTES,
                maximum_expanded_bytes: DEFAULT_MAXIMUM_EXPANDED_BYTES,
                maximum_expansion_ratio: DEFAULT_MAXIMUM_EXPANSION_RATIO,
                checksum: TransformChecksumBehavior::Optional,
            },
        })
    }
}

impl ByteTransformDriver for Lz4FrameTransformDriver {
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
            .checked_add(MAX_INTERNAL_WORKING_SET_BYTES)
            .is_none_or(|bytes| bytes > self.descriptor.maximum_working_set_bytes)
        {
            return Err(CdfError::contract(
                "LZ4 output chunk plus maximum frame working set exceeds driver authority",
            ));
        }
        let output_chunk_bytes = usize::try_from(request.preferred_output_chunk_bytes)
            .map_err(|_| CdfError::contract("LZ4 output chunk exceeds usize"))?;
        let expansion = TransformExpansionGuard::new(&request)?;
        let state = Lz4State {
            input: AccountedByteCursor::new(input),
            request,
            output_chunk_bytes,
            expansion,
            frame: None,
            frames: 0,
            decoded: Vec::new(),
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

struct Lz4Frame {
    independent_blocks: bool,
    block_checksums: bool,
    content_checksum: bool,
    content_size: Option<u64>,
    max_block_bytes: usize,
    decoded_bytes: u64,
    content_hasher: XxHash32,
    history: Vec<u8>,
    _working_set: MemoryLease,
}

struct Lz4State {
    input: AccountedByteCursor,
    request: ByteTransformRequest,
    output_chunk_bytes: usize,
    expansion: TransformExpansionGuard,
    frame: Option<Lz4Frame>,
    frames: u64,
    decoded: Vec<u8>,
    decoded_offset: usize,
}

impl Lz4State {
    async fn next_output(&mut self) -> Result<Option<AccountedBytes>> {
        loop {
            self.request.cancellation.check()?;
            if self.decoded_offset < self.decoded.len() {
                return self.emit_decoded_slice().await.map(Some);
            }
            self.decoded.clear();
            self.decoded_offset = 0;

            if self.frame.is_none() && !self.begin_frame().await? {
                if self.frames == 0 {
                    return Err(CdfError::data("LZ4 frame input is empty"));
                }
                self.expansion
                    .enforce_exact_ratio(self.input.consumed_bytes())?;
                return Ok(None);
            }
            if self.read_block().await? {
                continue;
            }
        }
    }

    async fn begin_frame(&mut self) -> Result<bool> {
        loop {
            let Some(first) = self.input.next_byte().await? else {
                return Ok(false);
            };
            let tail = self.input.read_exact(3, "LZ4 frame magic").await?;
            let magic = u32::from_le_bytes([first, tail[0], tail[1], tail[2]]);
            if (SKIPPABLE_MAGIC_START..=SKIPPABLE_MAGIC_END).contains(&magic) {
                let length = read_u32(&mut self.input, "LZ4 skippable frame length").await?;
                let length = usize::try_from(length)
                    .map_err(|_| CdfError::data("LZ4 skippable frame length exceeds usize"))?;
                if length > MAX_SKIPPABLE_BYTES {
                    return Err(CdfError::data(format!(
                        "LZ4 skippable frame length {length} exceeds the {MAX_SKIPPABLE_BYTES}-byte safety limit"
                    )));
                }
                self.input
                    .skip_exact(length, "LZ4 skippable frame payload")
                    .await?;
                continue;
            }
            if magic == LZ4_LEGACY_MAGIC {
                return Err(CdfError::data(
                    "legacy LZ4 framing is not accepted; select an explicit legacy framing transform",
                ));
            }
            if magic.to_le_bytes() != LZ4_MAGIC {
                return Err(CdfError::data(
                    "LZ4 frame magic is invalid; raw unframed LZ4 requires explicit framing metadata",
                ));
            }
            break;
        }

        let flg = self
            .input
            .next_byte()
            .await?
            .ok_or_else(|| CdfError::data("LZ4 frame ended before FLG"))?;
        let bd = self
            .input
            .next_byte()
            .await?
            .ok_or_else(|| CdfError::data("LZ4 frame ended before BD"))?;
        if flg & 0xc0 != 0x40 || flg & 0x02 != 0 {
            return Err(CdfError::data(
                "LZ4 frame FLG version/reserved bits are invalid",
            ));
        }
        if bd & 0x8f != 0 {
            return Err(CdfError::data("LZ4 frame BD reserved bits are invalid"));
        }
        let max_block_bytes = match (bd >> 4) & 0x07 {
            4 => 64 * 1024,
            5 => 256 * 1024,
            6 => 1024 * 1024,
            7 => MAX_BLOCK_BYTES,
            value => {
                return Err(CdfError::data(format!(
                    "LZ4 frame block-size code {value} is unsupported"
                )));
            }
        };
        let mut descriptor = vec![flg, bd];
        let content_size = if flg & 0x08 != 0 {
            let bytes = self.input.read_exact(8, "LZ4 content size").await?;
            descriptor.extend_from_slice(&bytes);
            Some(u64::from_le_bytes(bytes.try_into().map_err(|_| {
                CdfError::internal("LZ4 content size length was not eight")
            })?))
        } else {
            None
        };
        if flg & 0x01 != 0 {
            let dictionary = self.input.read_exact(4, "LZ4 dictionary id").await?;
            descriptor.extend_from_slice(&dictionary);
            return Err(CdfError::data(
                "LZ4 frame dictionaries are unsupported without explicit dictionary authority",
            ));
        }
        let expected_header_checksum = self
            .input
            .next_byte()
            .await?
            .ok_or_else(|| CdfError::data("LZ4 frame ended before header checksum"))?;
        let observed_header_checksum = ((xxh32(&descriptor) >> 8) & 0xff) as u8;
        if observed_header_checksum != expected_header_checksum {
            return Err(CdfError::data(format!(
                "LZ4 frame header checksum mismatch: expected {expected_header_checksum:#04x}, observed {observed_header_checksum:#04x}"
            )));
        }

        let working_bytes = u64::try_from(max_block_bytes * 2 + HISTORY_BYTES)
            .map_err(|_| CdfError::data("LZ4 working set exceeds u64"))?;
        let reservation = ReservationRequest::new(self.request.consumer.clone(), working_bytes)?
            .as_minimum_working_set();
        let lease = reserve(Arc::clone(&self.request.memory), reservation).await?;
        self.frame = Some(Lz4Frame {
            independent_blocks: flg & 0x20 != 0,
            block_checksums: flg & 0x10 != 0,
            content_checksum: flg & 0x04 != 0,
            content_size,
            max_block_bytes,
            decoded_bytes: 0,
            content_hasher: XxHash32::with_seed(0),
            history: Vec::new(),
            _working_set: lease,
        });
        Ok(true)
    }

    async fn read_block(&mut self) -> Result<bool> {
        let block_word = read_u32(&mut self.input, "LZ4 block header").await?;
        if block_word == 0 {
            self.end_frame().await?;
            return Ok(true);
        }
        let uncompressed = block_word & 0x8000_0000 != 0;
        let encoded_len = usize::try_from(block_word & 0x7fff_ffff)
            .map_err(|_| CdfError::data("LZ4 block length exceeds usize"))?;
        let (max_block_bytes, block_checksums, independent_blocks) = {
            let frame = self
                .frame
                .as_ref()
                .ok_or_else(|| CdfError::internal("LZ4 block has no active frame"))?;
            (
                frame.max_block_bytes,
                frame.block_checksums,
                frame.independent_blocks,
            )
        };
        if encoded_len == 0 || encoded_len > max_block_bytes {
            return Err(CdfError::data(format!(
                "LZ4 block length {encoded_len} exceeds frame maximum {max_block_bytes}"
            )));
        }
        let encoded = self
            .input
            .read_exact(encoded_len, "LZ4 block payload")
            .await?;
        if block_checksums {
            let expected = read_u32(&mut self.input, "LZ4 block checksum").await?;
            let observed = xxh32(&encoded);
            if observed != expected {
                return Err(CdfError::data(format!(
                    "LZ4 block checksum mismatch: expected {expected:#010x}, observed {observed:#010x}"
                )));
            }
        }

        self.decoded.clear();
        if uncompressed {
            self.decoded.extend_from_slice(&encoded);
        } else {
            self.decoded.resize(max_block_bytes, 0);
            let written = if independent_blocks {
                lz4_flex::block::decompress_into(&encoded, &mut self.decoded)
            } else {
                let history = &self
                    .frame
                    .as_ref()
                    .ok_or_else(|| CdfError::internal("LZ4 linked block lost its frame"))?
                    .history;
                lz4_flex::block::decompress_into_with_dict(&encoded, &mut self.decoded, history)
            }
            .map_err(|error| CdfError::data(format!("decode LZ4 block: {error}")))?;
            self.decoded.truncate(written);
        }
        let frame = self
            .frame
            .as_mut()
            .ok_or_else(|| CdfError::internal("LZ4 decoded block lost its frame"))?;
        frame.decoded_bytes = frame
            .decoded_bytes
            .checked_add(
                u64::try_from(self.decoded.len())
                    .map_err(|_| CdfError::data("LZ4 decoded block length exceeds u64"))?,
            )
            .ok_or_else(|| CdfError::data("LZ4 decoded content length overflowed"))?;
        if frame.content_checksum {
            frame.content_hasher.write(&self.decoded);
        }
        if !frame.independent_blocks {
            update_history(&mut frame.history, &self.decoded);
        }
        self.expansion
            .record(self.decoded.len(), self.input.consumed_bytes(), true)?;
        Ok(false)
    }

    async fn end_frame(&mut self) -> Result<()> {
        let mut frame = self
            .frame
            .take()
            .ok_or_else(|| CdfError::internal("LZ4 end marker has no active frame"))?;
        if let Some(expected) = frame.content_size
            && frame.decoded_bytes != expected
        {
            return Err(CdfError::data(format!(
                "LZ4 content length mismatch: expected {expected}, observed {}",
                frame.decoded_bytes
            )));
        }
        if frame.content_checksum {
            let expected = read_u32(&mut self.input, "LZ4 content checksum").await?;
            let observed = frame.content_hasher.finish() as u32;
            if observed != expected {
                return Err(CdfError::data(format!(
                    "LZ4 content checksum mismatch: expected {expected:#010x}, observed {observed:#010x}"
                )));
            }
        }
        self.frames = self
            .frames
            .checked_add(1)
            .ok_or_else(|| CdfError::data("LZ4 frame count overflowed"))?;
        self.expansion
            .enforce_exact_ratio(self.input.consumed_bytes())?;
        frame.history.clear();
        Ok(())
    }

    async fn emit_decoded_slice(&mut self) -> Result<AccountedBytes> {
        let end = self
            .decoded_offset
            .saturating_add(self.output_chunk_bytes)
            .min(self.decoded.len());
        let length = end - self.decoded_offset;
        let reservation = ReservationRequest::new(
            self.request.consumer.clone(),
            u64::try_from(length).map_err(|_| CdfError::data("LZ4 output exceeds u64"))?,
        )?;
        let lease = reserve(Arc::clone(&self.request.memory), reservation).await?;
        let output = if self.decoded_offset == 0 && end == self.decoded.len() {
            Bytes::from(std::mem::take(&mut self.decoded))
        } else {
            let output = Bytes::copy_from_slice(&self.decoded[self.decoded_offset..end]);
            self.decoded_offset = end;
            output
        };
        AccountedBytes::new(output, lease)
    }
}

fn update_history(history: &mut Vec<u8>, decoded: &[u8]) {
    if decoded.len() >= HISTORY_BYTES {
        history.clear();
        history.extend_from_slice(&decoded[decoded.len() - HISTORY_BYTES..]);
        return;
    }
    let retained = HISTORY_BYTES
        .saturating_sub(decoded.len())
        .min(history.len());
    if retained < history.len() {
        let start = history.len() - retained;
        history.copy_within(start.., 0);
        history.truncate(retained);
    }
    history.extend_from_slice(decoded);
}

async fn read_u32(input: &mut AccountedByteCursor, context: &'static str) -> Result<u32> {
    let bytes = input.read_exact(4, context).await?;
    Ok(u32::from_le_bytes(bytes.try_into().map_err(|_| {
        CdfError::internal("four-byte LZ4 field had the wrong length")
    })?))
}

fn xxh32(bytes: &[u8]) -> u32 {
    let mut hasher = XxHash32::with_seed(0);
    hasher.write(bytes);
    hasher.finish() as u32
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
    use lz4_flex::frame::{BlockMode, BlockSize, FrameEncoder, FrameInfo};

    use super::*;

    fn encode(payload: &[u8], linked: bool) -> Vec<u8> {
        let info = FrameInfo::new()
            .block_size(BlockSize::Max64KB)
            .block_mode(if linked {
                BlockMode::Linked
            } else {
                BlockMode::Independent
            })
            .block_checksums(true)
            .content_checksum(true)
            .content_size(Some(payload.len() as u64));
        let mut encoder = FrameEncoder::with_frame_info(info, Vec::new());
        encoder.write_all(payload).unwrap();
        encoder.finish().unwrap()
    }

    fn input_stream(
        bytes: Vec<u8>,
        chunk_bytes: usize,
        memory: Arc<dyn MemoryCoordinator>,
    ) -> AccountedByteStream {
        let bytes: Arc<[u8]> = bytes.into();
        let consumer = ConsumerKey::new("lz4-test-input", MemoryClass::Source).unwrap();
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

    fn decode_with(
        compressed: Vec<u8>,
        input_chunk_bytes: usize,
        output_chunk_bytes: u64,
        maximum_expanded_bytes: u64,
        cancellation: cdf_runtime::RunCancellation,
    ) -> (Result<Vec<u8>>, MemorySnapshot) {
        let input_size_bytes = u64::try_from(compressed.len()).unwrap();
        let memory: Arc<dyn MemoryCoordinator> = Arc::new(
            DeterministicMemoryCoordinator::new(64 * 1024 * 1024, Default::default()).unwrap(),
        );
        let input = input_stream(compressed, input_chunk_bytes, Arc::clone(&memory));
        let request = ByteTransformRequest {
            preferred_output_chunk_bytes: output_chunk_bytes,
            maximum_expanded_bytes,
            maximum_expansion_ratio: 1_000,
            input_size_bytes: Some(input_size_bytes),
            memory: Arc::clone(&memory),
            consumer: ConsumerKey::new("lz4-test", MemoryClass::Transform).unwrap(),
            cancellation,
        };
        let driver = Lz4FrameTransformDriver::new().unwrap();
        let result = driver.transform(input, request).and_then(|mut output| {
            block_on(async move {
                let mut decoded = Vec::new();
                while let Some(chunk) = output.next().await {
                    decoded.extend_from_slice(chunk?.payload());
                }
                Ok(decoded)
            })
        });
        (result, memory.snapshot())
    }

    fn decode(compressed: Vec<u8>) -> (Result<Vec<u8>>, MemorySnapshot) {
        decode_with(
            compressed,
            1,
            17 * 1024,
            128 * 1024 * 1024,
            Default::default(),
        )
    }

    #[test]
    fn decodes_linked_concatenated_frames_across_one_byte_chunks() {
        let first = vec![b'a'; 200_000];
        let second = (0..180_000)
            .map(|value| (value % 251) as u8)
            .collect::<Vec<_>>();
        let mut encoded = encode(&first, true);
        encoded.extend_from_slice(&encode(&second, false));
        let mut expected = first;
        expected.extend_from_slice(&second);
        let (decoded, snapshot) = decode(encoded);
        assert_eq!(decoded.unwrap(), expected);
        assert_eq!(snapshot.current_bytes, 0);
        assert!(snapshot.peak_bytes <= MAX_INTERNAL_WORKING_SET_BYTES + 17 * 1024 + 1);
    }

    #[test]
    fn rejects_raw_and_checksum_corruption() {
        let error = decode(b"not a frame".to_vec()).0.unwrap_err();
        assert!(error.message.contains("raw unframed LZ4"));

        let mut encoded = encode(b"checksum me", false);
        let end = encoded.len() - 1;
        encoded[end] ^= 0x80;
        let error = decode(encoded).0.unwrap_err();
        assert!(error.message.contains("checksum mismatch"));
    }

    #[test]
    fn rejects_truncation_and_enforces_expansion_and_cancellation() {
        let mut truncated = encode(b"truncated frame", false);
        truncated.pop();
        assert!(decode(truncated).0.is_err());

        let compressed = encode(&vec![0_u8; 4096], false);
        assert!(
            decode_with(compressed.clone(), 7, 1024, 1024, Default::default())
                .0
                .unwrap_err()
                .to_string()
                .contains("ceiling")
        );
        let cancellation = cdf_runtime::RunCancellation::default();
        cancellation.cancel();
        assert!(
            decode_with(compressed, 7, 1024, 8192, cancellation)
                .0
                .unwrap_err()
                .to_string()
                .contains("cancelled")
        );
    }

    #[test]
    #[ignore = "performance evidence; run in release mode"]
    fn lz4_driver_reference_rate() {
        const BYTES: usize = 64 * 1024 * 1024;
        let input = (0..BYTES)
            .map(|index| ((index.wrapping_mul(31) ^ (index >> 7)) & 0xff) as u8)
            .collect::<Vec<_>>();
        let compressed = encode(&input, true);

        let reference_start = Instant::now();
        let mut reference = Vec::with_capacity(input.len());
        lz4_flex::frame::FrameDecoder::new(compressed.as_slice())
            .read_to_end(&mut reference)
            .unwrap();
        let reference_elapsed = reference_start.elapsed();
        assert_eq!(reference, input);

        let driver_start = Instant::now();
        let (decoded, _) = decode_with(
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
            "lz4_reference_ms={:.3} lz4_driver_ms={:.3} reference_ratio={reference_ratio:.3}",
            reference_elapsed.as_secs_f64() * 1000.0,
            driver_elapsed.as_secs_f64() * 1000.0,
        );
        assert!(
            reference_ratio >= 0.6,
            "LZ4 driver achieved {reference_ratio:.3}x reference throughput"
        );
    }
}
