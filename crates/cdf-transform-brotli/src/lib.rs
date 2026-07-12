use std::sync::Arc;

use brotli::{BrotliDecompressStream, BrotliResult, BrotliState, HeapAlloc, HuffmanCode};
use bytes::Bytes;
use cdf_kernel::{CdfError, Result};
use cdf_memory::{AccountedBytes, MemoryLease, ReservationRequest, reserve};
use cdf_runtime::{
    AccountedByteCursor, AccountedByteStream, ByteTransformDescriptor, ByteTransformDriver,
    ByteTransformId, ByteTransformRequest, TransformChecksumBehavior, TransformExpansionGuard,
};
use futures_util::stream;

const STANDARD_WINDOW_AND_TABLES_BYTES: u64 = 32 * 1024 * 1024;
const DEFAULT_MAXIMUM_WORKING_SET_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_MAXIMUM_EXPANDED_BYTES: u64 = 4 * 1024 * 1024 * 1024 * 1024;
const DEFAULT_MAXIMUM_EXPANSION_RATIO: u32 = 10_000;

type Decoder = BrotliState<HeapAlloc<u8>, HeapAlloc<u32>, HeapAlloc<HuffmanCode>>;

#[derive(Debug)]
pub struct BrotliTransformDriver {
    descriptor: ByteTransformDescriptor,
}

impl BrotliTransformDriver {
    pub fn new() -> Result<Self> {
        Ok(Self {
            descriptor: ByteTransformDescriptor {
                transform_id: ByteTransformId::new("brotli")?,
                semantic_version: "1.0.0".to_owned(),
                extensions: vec!["br".to_owned()],
                magic: Vec::new(),
                preserves_random_access: false,
                splittable: false,
                supports_concatenated_members: true,
                maximum_output_chunk_bytes: 16 * 1024 * 1024,
                maximum_working_set_bytes: DEFAULT_MAXIMUM_WORKING_SET_BYTES,
                maximum_expanded_bytes: DEFAULT_MAXIMUM_EXPANDED_BYTES,
                maximum_expansion_ratio: DEFAULT_MAXIMUM_EXPANSION_RATIO,
                checksum: TransformChecksumBehavior::None,
            },
        })
    }
}

impl ByteTransformDriver for BrotliTransformDriver {
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
            .checked_add(STANDARD_WINDOW_AND_TABLES_BYTES)
            .is_none_or(|bytes| bytes > self.descriptor.maximum_working_set_bytes)
        {
            return Err(CdfError::contract(
                "Brotli output chunk plus strict standard-window working set exceeds driver authority",
            ));
        }
        let output_chunk_bytes = usize::try_from(request.preferred_output_chunk_bytes)
            .map_err(|_| CdfError::contract("Brotli output chunk exceeds usize"))?;
        let expansion = TransformExpansionGuard::new(&request)?;
        let state = BrotliStreamState {
            input: AccountedByteCursor::new(input),
            request,
            output_chunk_bytes,
            expansion,
            decoder: None,
            working_set: None,
            streams: 0,
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

struct BrotliStreamState {
    input: AccountedByteCursor,
    request: ByteTransformRequest,
    output_chunk_bytes: usize,
    expansion: TransformExpansionGuard,
    decoder: Option<Decoder>,
    working_set: Option<MemoryLease>,
    streams: u64,
}

impl BrotliStreamState {
    async fn next_output(&mut self) -> Result<Option<AccountedBytes>> {
        loop {
            self.request.cancellation.check()?;
            if self.decoder.is_none() {
                if !self.input.ensure_current().await? {
                    if self.streams == 0 {
                        return Err(CdfError::data("Brotli input is empty"));
                    }
                    self.expansion
                        .enforce_exact_ratio(self.input.consumed_bytes())?;
                    return Ok(None);
                }
                self.begin_stream().await?;
            }

            let reservation = ReservationRequest::new(
                self.request.consumer.clone(),
                u64::try_from(self.output_chunk_bytes)
                    .map_err(|_| CdfError::data("Brotli output chunk exceeds u64"))?,
            )?;
            let lease = reserve(Arc::clone(&self.request.memory), reservation).await?;
            let mut output = vec![0_u8; self.output_chunk_bytes];
            let input_available = self.input.ensure_current().await?;
            let input = if input_available {
                self.input.current_slice()
            } else {
                &[]
            };
            let mut available_in = input.len();
            let mut input_offset = 0;
            let mut available_out = output.len();
            let mut output_offset = 0;
            let mut total_out = 0;
            let result = BrotliDecompressStream(
                &mut available_in,
                &mut input_offset,
                input,
                &mut available_out,
                &mut output_offset,
                &mut output,
                &mut total_out,
                self.decoder
                    .as_mut()
                    .ok_or_else(|| CdfError::internal("Brotli decoder was not initialized"))?,
            );
            self.input.consume(input_offset)?;
            output.truncate(output_offset);
            let stream_complete = matches!(result, BrotliResult::ResultSuccess);
            if output_offset > 0 {
                self.expansion.record(
                    output_offset,
                    self.input.consumed_bytes(),
                    stream_complete,
                )?;
            }
            match result {
                BrotliResult::ResultFailure => {
                    return Err(CdfError::data("Brotli stream is corrupt or unsupported"));
                }
                BrotliResult::ResultSuccess => {
                    self.streams = self
                        .streams
                        .checked_add(1)
                        .ok_or_else(|| CdfError::data("Brotli stream count overflowed"))?;
                    self.decoder = None;
                    self.working_set = None;
                    self.expansion
                        .enforce_exact_ratio(self.input.consumed_bytes())?;
                }
                BrotliResult::NeedsMoreInput if !input_available => {
                    return Err(CdfError::data(
                        "Brotli input ended before the current stream completed",
                    ));
                }
                BrotliResult::NeedsMoreInput if input_offset == 0 && output_offset == 0 => {
                    return Err(CdfError::data(
                        "Brotli decoder made no progress while input remained",
                    ));
                }
                BrotliResult::NeedsMoreOutput if output_offset == 0 => {
                    return Err(CdfError::data(
                        "Brotli decoder requested more output without producing bytes",
                    ));
                }
                BrotliResult::NeedsMoreInput | BrotliResult::NeedsMoreOutput => {}
            }
            if !output.is_empty() {
                return AccountedBytes::new(Bytes::from(output), lease).map(Some);
            }
        }
    }

    async fn begin_stream(&mut self) -> Result<()> {
        let reservation = ReservationRequest::new(
            self.request.consumer.clone(),
            STANDARD_WINDOW_AND_TABLES_BYTES,
        )?
        .as_minimum_working_set();
        self.working_set = Some(reserve(Arc::clone(&self.request.memory), reservation).await?);
        self.decoder = Some(BrotliState::new_strict(
            HeapAlloc::default(),
            HeapAlloc::default(),
            HeapAlloc::default(),
        ));
        Ok(())
    }
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

    fn encode(payload: &[u8]) -> Vec<u8> {
        let mut encoded = Vec::new();
        let mut compressor = brotli::CompressorWriter::new(&mut encoded, 64 * 1024, 6, 22);
        compressor.write_all(payload).unwrap();
        drop(compressor);
        encoded
    }

    fn input_stream(
        bytes: Vec<u8>,
        chunk_bytes: usize,
        memory: Arc<dyn MemoryCoordinator>,
    ) -> AccountedByteStream {
        let bytes: Arc<[u8]> = bytes.into();
        let consumer = ConsumerKey::new("brotli-test-input", MemoryClass::Source).unwrap();
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
            DeterministicMemoryCoordinator::new(64 * 1024 * 1024, Default::default()).unwrap(),
        );
        let request = ByteTransformRequest {
            preferred_output_chunk_bytes: output_chunk_bytes,
            maximum_expanded_bytes,
            maximum_expansion_ratio: 10_000,
            input_size_bytes: Some(input_size_bytes),
            memory: Arc::clone(&memory),
            consumer: ConsumerKey::new("brotli-test", MemoryClass::Transform).unwrap(),
            cancellation,
        };
        let driver = BrotliTransformDriver::new().unwrap();
        let result = driver
            .transform(
                input_stream(compressed, input_chunk_bytes, Arc::clone(&memory)),
                request,
            )
            .and_then(|mut output| {
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

    #[test]
    fn streams_concatenated_brotli_at_arbitrary_boundaries() {
        let first = b"first brotli stream\n".repeat(4000);
        let second = b"second brotli stream\n".repeat(3000);
        let mut compressed = encode(&first);
        compressed.extend_from_slice(&encode(&second));
        let (decoded, snapshot) = decode(compressed, 1, 4096, 1024 * 1024, Default::default());
        let mut expected = first;
        expected.extend_from_slice(&second);
        assert_eq!(decoded.unwrap(), expected);
        assert_eq!(snapshot.current_bytes, 0);
        assert!(snapshot.peak_bytes <= STANDARD_WINDOW_AND_TABLES_BYTES + 4097);
    }

    #[test]
    fn rejects_corruption_truncation_expansion_and_cancellation() {
        let mut corrupt = encode(b"corrupt brotli");
        corrupt[0] ^= 0xff;
        assert!(
            decode(corrupt, 3, 1024, 4096, Default::default())
                .0
                .is_err()
        );

        let mut truncated = encode(b"truncated brotli");
        truncated.pop();
        assert!(
            decode(truncated, 3, 1024, 4096, Default::default())
                .0
                .is_err()
        );

        let compressed = encode(&vec![0_u8; 4096]);
        assert!(
            decode(compressed.clone(), 7, 1024, 1024, Default::default())
                .0
                .unwrap_err()
                .to_string()
                .contains("ceiling")
        );
        let cancellation = cdf_runtime::RunCancellation::default();
        cancellation.cancel();
        assert!(
            decode(compressed, 7, 1024, 8192, cancellation)
                .0
                .unwrap_err()
                .to_string()
                .contains("cancelled")
        );
    }

    #[test]
    #[ignore = "performance evidence; run in release mode"]
    fn brotli_driver_reference_rate() {
        const BYTES: usize = 32 * 1024 * 1024;
        let input = (0..BYTES)
            .map(|index| ((index.wrapping_mul(31) ^ (index >> 7)) & 0xff) as u8)
            .collect::<Vec<_>>();
        let compressed = encode(&input);

        let reference_start = Instant::now();
        let mut reference = Vec::with_capacity(input.len());
        brotli::Decompressor::new(compressed.as_slice(), 1024 * 1024)
            .read_to_end(&mut reference)
            .unwrap();
        let reference_elapsed = reference_start.elapsed();
        assert_eq!(reference, input);

        let driver_start = Instant::now();
        let (decoded, _) = decode(
            compressed,
            1024 * 1024,
            1024 * 1024,
            64 * 1024 * 1024,
            Default::default(),
        );
        let driver_elapsed = driver_start.elapsed();
        assert_eq!(decoded.unwrap(), input);
        let reference_ratio = reference_elapsed.as_secs_f64() / driver_elapsed.as_secs_f64();
        eprintln!(
            "brotli_reference_ms={:.3} brotli_driver_ms={:.3} reference_ratio={reference_ratio:.3}",
            reference_elapsed.as_secs_f64() * 1000.0,
            driver_elapsed.as_secs_f64() * 1000.0,
        );
        assert!(
            reference_ratio >= 0.6,
            "Brotli driver achieved {reference_ratio:.3}x reference throughput"
        );
    }
}
