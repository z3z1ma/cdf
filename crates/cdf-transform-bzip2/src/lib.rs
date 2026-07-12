use std::sync::Arc;

use bytes::Bytes;
use bzip2::{Decompress, Status};
use cdf_kernel::{CdfError, Result};
use cdf_memory::{AccountedBytes, MemoryLease, ReservationRequest, reserve};
use cdf_runtime::{
    AccountedByteCursor, AccountedByteStream, ByteTransformDescriptor, ByteTransformDriver,
    ByteTransformId, ByteTransformRequest, MagicSignature, TransformChecksumBehavior,
    TransformExpansionGuard,
};
use futures_util::stream;

const BZIP2_MAGIC: &[u8; 3] = b"BZh";
const DECODER_WORKING_SET_BYTES: u64 = 8 * 1024 * 1024;
const DEFAULT_MAXIMUM_WORKING_SET_BYTES: u64 = 40 * 1024 * 1024;
const DEFAULT_MAXIMUM_EXPANDED_BYTES: u64 = 4 * 1024 * 1024 * 1024 * 1024;
const DEFAULT_MAXIMUM_EXPANSION_RATIO: u32 = 10_000;

#[derive(Debug)]
pub struct Bzip2TransformDriver {
    descriptor: ByteTransformDescriptor,
}

impl Bzip2TransformDriver {
    pub fn new() -> Result<Self> {
        Ok(Self {
            descriptor: ByteTransformDescriptor {
                transform_id: ByteTransformId::new("bzip2")?,
                semantic_version: "1.0.0".to_owned(),
                extensions: vec!["bz2".to_owned(), "bz".to_owned()],
                magic: vec![MagicSignature {
                    offset: 0,
                    bytes: BZIP2_MAGIC.to_vec(),
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

impl ByteTransformDriver for Bzip2TransformDriver {
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
            .checked_add(DECODER_WORKING_SET_BYTES)
            .is_none_or(|bytes| bytes > self.descriptor.maximum_working_set_bytes)
        {
            return Err(CdfError::contract(
                "bzip2 output chunk plus maximum decoder working set exceeds driver authority",
            ));
        }
        let output_chunk_bytes = usize::try_from(request.preferred_output_chunk_bytes)
            .map_err(|_| CdfError::contract("bzip2 output chunk exceeds usize"))?;
        let expansion = TransformExpansionGuard::new(&request)?;
        let state = Bzip2State {
            input: AccountedByteCursor::new(input),
            request,
            output_chunk_bytes,
            expansion,
            decoder: None,
            working_set: None,
            members: 0,
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

struct Bzip2State {
    input: AccountedByteCursor,
    request: ByteTransformRequest,
    output_chunk_bytes: usize,
    expansion: TransformExpansionGuard,
    decoder: Option<Decompress>,
    working_set: Option<MemoryLease>,
    members: u64,
}

impl Bzip2State {
    async fn next_output(&mut self) -> Result<Option<AccountedBytes>> {
        loop {
            self.request.cancellation.check()?;
            if self.decoder.is_none() {
                if !self.input.ensure_current().await? {
                    if self.members == 0 {
                        return Err(CdfError::data("bzip2 input is empty"));
                    }
                    self.expansion
                        .enforce_exact_ratio(self.input.consumed_bytes())?;
                    return Ok(None);
                }
                self.begin_member().await?;
            }

            let reservation = ReservationRequest::new(
                self.request.consumer.clone(),
                u64::try_from(self.output_chunk_bytes)
                    .map_err(|_| CdfError::data("bzip2 output chunk exceeds u64"))?,
            )?;
            let lease = reserve(Arc::clone(&self.request.memory), reservation).await?;
            let mut output = vec![0_u8; self.output_chunk_bytes];
            let input_available = self.input.ensure_current().await?;
            let input = if input_available {
                self.input.current_slice()
            } else {
                &[]
            };
            let decoder = self
                .decoder
                .as_mut()
                .ok_or_else(|| CdfError::internal("bzip2 decoder was not initialized"))?;
            let before_in = decoder.total_in();
            let before_out = decoder.total_out();
            let status = decoder
                .decompress(input, &mut output)
                .map_err(|error| CdfError::data(format!("decode bzip2 member: {error}")))?;
            let consumed = usize::try_from(decoder.total_in() - before_in)
                .map_err(|_| CdfError::data("bzip2 consumed-byte count exceeds usize"))?;
            let written = usize::try_from(decoder.total_out() - before_out)
                .map_err(|_| CdfError::data("bzip2 output-byte count exceeds usize"))?;
            self.input.consume(consumed)?;
            output.truncate(written);
            let member_complete = status == Status::StreamEnd;
            if written > 0 {
                self.expansion
                    .record(written, self.input.consumed_bytes(), member_complete)?;
            }
            match status {
                Status::StreamEnd => {
                    self.members = self
                        .members
                        .checked_add(1)
                        .ok_or_else(|| CdfError::data("bzip2 member count overflowed"))?;
                    self.decoder = None;
                    self.working_set = None;
                    self.expansion
                        .enforce_exact_ratio(self.input.consumed_bytes())?;
                }
                Status::MemNeeded => {
                    return Err(CdfError::data(
                        "bzip2 decoder exceeded its admitted native working set",
                    ));
                }
                Status::Ok if !input_available && written == 0 => {
                    return Err(CdfError::data(
                        "bzip2 input ended before the current member completed",
                    ));
                }
                Status::Ok if consumed == 0 && written == 0 => {
                    return Err(CdfError::data(
                        "bzip2 decoder made no progress while input remained",
                    ));
                }
                Status::Ok => {}
                Status::FlushOk | Status::RunOk | Status::FinishOk => {
                    return Err(CdfError::internal(
                        "bzip2 decompressor returned a compression-only status",
                    ));
                }
            }
            if !output.is_empty() {
                return AccountedBytes::new(Bytes::from(output), lease).map(Some);
            }
        }
    }

    async fn begin_member(&mut self) -> Result<()> {
        let reservation =
            ReservationRequest::new(self.request.consumer.clone(), DECODER_WORKING_SET_BYTES)?
                .as_minimum_working_set();
        self.working_set = Some(reserve(Arc::clone(&self.request.memory), reservation).await?);
        self.decoder = Some(Decompress::new(false));
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

    use bzip2::{Compression, read::BzDecoder, write::BzEncoder};
    use cdf_memory::{
        ConsumerKey, DeterministicMemoryCoordinator, MemoryClass, MemoryCoordinator, MemorySnapshot,
    };
    use futures_executor::block_on;
    use futures_util::StreamExt;

    use super::*;

    fn encode(payload: &[u8]) -> Vec<u8> {
        let mut encoder = BzEncoder::new(Vec::new(), Compression::best());
        encoder.write_all(payload).unwrap();
        encoder.finish().unwrap()
    }

    fn input_stream(
        bytes: Vec<u8>,
        chunk_bytes: usize,
        memory: Arc<dyn MemoryCoordinator>,
    ) -> AccountedByteStream {
        let bytes: Arc<[u8]> = bytes.into();
        let consumer = ConsumerKey::new("bzip2-test-input", MemoryClass::Source).unwrap();
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
            DeterministicMemoryCoordinator::new(48 * 1024 * 1024, Default::default()).unwrap(),
        );
        let request = ByteTransformRequest {
            preferred_output_chunk_bytes: output_chunk_bytes,
            maximum_expanded_bytes,
            maximum_expansion_ratio: 10_000,
            input_size_bytes: Some(input_size_bytes),
            memory: Arc::clone(&memory),
            consumer: ConsumerKey::new("bzip2-test", MemoryClass::Transform).unwrap(),
            cancellation,
        };
        let driver = Bzip2TransformDriver::new().unwrap();
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
    fn streams_concatenated_members_across_one_byte_chunks() {
        let first = b"first bzip2 member\n".repeat(4000);
        let second = b"second bzip2 member\n".repeat(3000);
        let mut compressed = encode(&first);
        compressed.extend_from_slice(&encode(&second));
        let (decoded, snapshot) = decode(compressed, 1, 4096, 1024 * 1024, Default::default());
        let mut expected = first;
        expected.extend_from_slice(&second);
        assert_eq!(decoded.unwrap(), expected);
        assert_eq!(snapshot.current_bytes, 0);
        assert!(snapshot.peak_bytes <= DECODER_WORKING_SET_BYTES + 4097);
    }

    #[test]
    fn rejects_corruption_truncation_expansion_and_cancellation() {
        let mut corrupt = encode(b"corrupt bzip2");
        let middle = corrupt.len() / 2;
        corrupt[middle] ^= 0xff;
        assert!(
            decode(corrupt, 3, 1024, 4096, Default::default())
                .0
                .is_err()
        );

        let mut truncated = encode(b"truncated bzip2");
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
    fn bzip2_driver_reference_rate() {
        const BYTES: usize = 32 * 1024 * 1024;
        let input = (0..BYTES)
            .map(|index| ((index.wrapping_mul(31) ^ (index >> 7)) & 0xff) as u8)
            .collect::<Vec<_>>();
        let compressed = encode(&input);

        let reference_start = Instant::now();
        let mut reference = Vec::with_capacity(input.len());
        BzDecoder::new(compressed.as_slice())
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
            "bzip2_reference_ms={:.3} bzip2_driver_ms={:.3} reference_ratio={reference_ratio:.3}",
            reference_elapsed.as_secs_f64() * 1000.0,
            driver_elapsed.as_secs_f64() * 1000.0,
        );
        assert!(
            reference_ratio >= 0.6,
            "bzip2 driver achieved {reference_ratio:.3}x reference throughput"
        );
    }
}
