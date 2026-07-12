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
use zstd::stream::raw::{DParameter, Decoder, Operation};

const ZSTD_MAGIC: &[u8; 4] = b"\x28\xb5\x2f\xfd";
const MIB: u64 = 1024 * 1024;
const MAXIMUM_WINDOW_LOG: u32 = 26;
const INTERNAL_WORKING_SET_BYTES: u64 = 68 * MIB;
const DEFAULT_MAXIMUM_WORKING_SET_BYTES: u64 = 100 * MIB;
const DEFAULT_MAXIMUM_EXPANDED_BYTES: u64 = 4 * 1024 * 1024 * 1024 * 1024;
const DEFAULT_MAXIMUM_EXPANSION_RATIO: u32 = 1_000;

#[derive(Debug)]
pub struct ZstdTransformDriver {
    descriptor: ByteTransformDescriptor,
}

impl ZstdTransformDriver {
    pub fn new() -> Result<Self> {
        Ok(Self {
            descriptor: ByteTransformDescriptor {
                transform_id: ByteTransformId::new("zstd")?,
                semantic_version: "1.0.0".to_owned(),
                extensions: vec!["zst".to_owned(), "zstd".to_owned()],
                magic: vec![MagicSignature {
                    offset: 0,
                    bytes: ZSTD_MAGIC.to_vec(),
                    strong: true,
                }],
                preserves_random_access: false,
                splittable: false,
                supports_concatenated_members: true,
                maximum_working_set_bytes: DEFAULT_MAXIMUM_WORKING_SET_BYTES,
                maximum_expanded_bytes: DEFAULT_MAXIMUM_EXPANDED_BYTES,
                maximum_expansion_ratio: DEFAULT_MAXIMUM_EXPANSION_RATIO,
                checksum: TransformChecksumBehavior::Optional,
            },
        })
    }
}

impl ByteTransformDriver for ZstdTransformDriver {
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
                "zstd output chunk plus maximum native decoder window exceeds driver authority",
            ));
        }
        let output_chunk_bytes = usize::try_from(request.preferred_output_chunk_bytes)
            .map_err(|_| CdfError::contract("zstd output chunk exceeds usize"))?;
        let expansion = TransformExpansionGuard::new(&request)?;
        let state = ZstdState {
            input: AccountedByteCursor::new(input),
            request,
            output_chunk_bytes,
            decoder: None,
            expansion,
            frames: 0,
            frame_finished: false,
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

struct ZstdState {
    input: AccountedByteCursor,
    request: ByteTransformRequest,
    output_chunk_bytes: usize,
    decoder: Option<Decoder<'static>>,
    expansion: TransformExpansionGuard,
    frames: u64,
    frame_finished: bool,
    working_set_lease: Option<MemoryLease>,
}

impl ZstdState {
    async fn next_output(&mut self) -> Result<Option<AccountedBytes>> {
        loop {
            self.request.cancellation.check()?;
            self.ensure_decoder().await?;

            if self.frame_finished {
                if !self.input.ensure_current().await? {
                    self.expansion
                        .enforce_exact_ratio(self.input.consumed_bytes())?;
                    return Ok(None);
                }
                self.decoder_mut()?.reinit().map_err(zstd_error)?;
                self.frame_finished = false;
            } else if !self.input.ensure_current().await? {
                let message = if self.input.consumed_bytes() == 0 {
                    "zstd input is empty"
                } else {
                    "zstd input ended before the current frame completed"
                };
                return Err(CdfError::data(message));
            }

            let reservation = ReservationRequest::new(
                self.request.consumer.clone(),
                u64::try_from(self.output_chunk_bytes)
                    .map_err(|_| CdfError::data("zstd output chunk exceeds u64"))?,
            )?;
            let lease = reserve(Arc::clone(&self.request.memory), reservation).await?;
            let mut output = vec![0_u8; self.output_chunk_bytes];
            let input = self.input.current_slice();
            let decoder = self
                .decoder
                .as_mut()
                .ok_or_else(|| CdfError::internal("zstd decoder was not initialized"))?;
            let status = decoder
                .run_on_buffers(input, &mut output)
                .map_err(zstd_error)?;
            self.input.consume(status.bytes_read)?;
            output.truncate(status.bytes_written);

            let frame_complete = status.remaining == 0;
            if status.bytes_written > 0 {
                self.expansion.record(
                    status.bytes_written,
                    self.input.consumed_bytes(),
                    frame_complete,
                )?;
            } else if frame_complete {
                self.expansion
                    .enforce_exact_ratio(self.input.consumed_bytes())?;
            }
            if frame_complete {
                self.frame_finished = true;
                self.frames = self
                    .frames
                    .checked_add(1)
                    .ok_or_else(|| CdfError::data("zstd frame count overflowed"))?;
            } else if status.bytes_read == 0 && status.bytes_written == 0 {
                return Err(CdfError::data(
                    "zstd decoder made no progress on a nonempty compressed chunk",
                ));
            }

            if status.bytes_written > 0 {
                return Ok(Some(AccountedBytes::new(Bytes::from(output), lease)?));
            }
        }
    }

    async fn ensure_decoder(&mut self) -> Result<()> {
        if self.working_set_lease.is_none() {
            let reservation =
                ReservationRequest::new(self.request.consumer.clone(), INTERNAL_WORKING_SET_BYTES)?
                    .as_minimum_working_set();
            self.working_set_lease =
                Some(reserve(Arc::clone(&self.request.memory), reservation).await?);
        }
        if self.decoder.is_none() {
            let mut decoder = Decoder::new().map_err(zstd_error)?;
            decoder
                .set_parameter(DParameter::WindowLogMax(MAXIMUM_WINDOW_LOG))
                .map_err(zstd_error)?;
            self.decoder = Some(decoder);
        }
        Ok(())
    }

    fn decoder_mut(&mut self) -> Result<&mut Decoder<'static>> {
        self.decoder
            .as_mut()
            .ok_or_else(|| CdfError::internal("zstd decoder was not initialized"))
    }
}

fn zstd_error(error: std::io::Error) -> CdfError {
    CdfError::data(format!("zstd frame is corrupt or unsupported: {error}"))
}

#[cfg(test)]
mod tests {
    use std::{io::Write, sync::Arc};

    use cdf_memory::{
        ConsumerKey, DeterministicMemoryCoordinator, MemoryClass, MemoryCoordinator, MemorySnapshot,
    };
    use futures_executor::block_on;
    use futures_util::StreamExt;

    use super::*;

    fn encode_frame(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = zstd::stream::Encoder::new(Vec::new(), 1).unwrap();
        encoder.include_checksum(true).unwrap();
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    }

    fn input_stream(
        bytes: Vec<u8>,
        chunk_bytes: usize,
        memory: Arc<dyn MemoryCoordinator>,
    ) -> AccountedByteStream {
        let bytes: Arc<[u8]> = bytes.into();
        let consumer = ConsumerKey::new("zstd-test-input", MemoryClass::Source).unwrap();
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
        maximum_expansion_ratio: u32,
        cancellation: cdf_runtime::RunCancellation,
    ) -> (Result<Vec<u8>>, MemorySnapshot) {
        let input_size_bytes = u64::try_from(compressed.len()).unwrap();
        let memory: Arc<dyn MemoryCoordinator> =
            Arc::new(DeterministicMemoryCoordinator::new(128 * MIB, Default::default()).unwrap());
        let input = input_stream(compressed, input_chunk_bytes, Arc::clone(&memory));
        let request = ByteTransformRequest {
            preferred_output_chunk_bytes: output_chunk_bytes,
            maximum_expanded_bytes,
            maximum_expansion_ratio,
            input_size_bytes: Some(input_size_bytes),
            memory: Arc::clone(&memory),
            consumer: ConsumerKey::new("zstd-test", MemoryClass::Transform).unwrap(),
            cancellation,
        };
        let driver = ZstdTransformDriver::new().unwrap();
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
    fn streams_concatenated_frames_across_single_byte_input_chunks() {
        let first = b"first zstd frame\n".repeat(41);
        let second = b"second zstd frame\n".repeat(43);
        let mut compressed = encode_frame(&first);
        compressed.extend_from_slice(&encode_frame(&second));
        let (decoded, snapshot) = decode(compressed, 1, 31, 1024 * 1024, 100, Default::default());
        let mut expected = first;
        expected.extend_from_slice(&second);
        assert_eq!(decoded.unwrap(), expected);
        assert_eq!(snapshot.current_bytes, 0);
        assert!(snapshot.peak_bytes <= INTERNAL_WORKING_SET_BYTES + 32);
    }

    #[test]
    fn rejects_checksum_corruption_and_truncation() {
        let mut corrupted = encode_frame(b"zstd checksum authority");
        let last = corrupted.len() - 1;
        corrupted[last] ^= 0xff;
        let (corrupt_result, _) = decode(corrupted, 7, 32, 1024, 100, Default::default());
        assert!(corrupt_result.is_err());

        let mut truncated = encode_frame(b"zstd truncation authority");
        truncated.truncate(truncated.len() - 3);
        let (truncated_result, _) = decode(truncated, 5, 32, 1024, 100, Default::default());
        let error = truncated_result.unwrap_err().to_string();
        assert!(
            error.contains("ended") || error.contains("corrupt"),
            "unexpected truncation error: {error}"
        );
    }

    #[test]
    fn enforces_expansion_and_cancellation_authority() {
        let compressed = encode_frame(&vec![0_u8; 4096]);
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
