use std::pin::Pin;
use std::task::{Context, Poll, ready};

use bytes::{BufMut, Bytes, BytesMut};
use cityhash_rs::cityhash_102_128;
use futures_util::stream::Stream;
use zstd::stream::raw::Operation;

use crate::{
    ResponseLimits,
    error::{Error, Result},
    response::Chunk,
};

const OUTPUT_BUFFER_SIZE: usize = 64 * 1024;

// ClickHouse native compression framing.
const CHECKSUM_SIZE: usize = 16;
const HEADER_SIZE: usize = 9;
const META_SIZE: usize = CHECKSUM_SIZE + HEADER_SIZE;
const ZSTD_MAGIC: u8 = 0x90;

fn calc_checksum(buffer: &[u8]) -> u128 {
    let hash = cityhash_102_128(buffer);
    hash.rotate_right(64)
}

pub(crate) fn compress(uncompressed: &[u8], level: Option<i32>) -> Result<Bytes> {
    let level = level.unwrap_or(zstd::DEFAULT_COMPRESSION_LEVEL);
    let max_compressed_size = zstd::zstd_safe::compress_bound(uncompressed.len());

    let mut buffer = BytesMut::new();
    buffer.resize(META_SIZE + max_compressed_size, 0);

    let compressed_data_size =
        zstd::zstd_safe::compress(&mut buffer[META_SIZE..], uncompressed, level)
            .map_err(|code| Error::Compression(zstd::zstd_safe::get_error_name(code).into()))?;

    buffer.truncate(META_SIZE + compressed_data_size);

    // Write header: [magic] [compressed_size] [uncompressed_size]
    let mut header = &mut buffer[CHECKSUM_SIZE..META_SIZE];
    header.put_u8(ZSTD_MAGIC);
    header.put_u32_le((HEADER_SIZE + compressed_data_size) as u32);
    header.put_u32_le(uncompressed.len() as u32);

    // Write checksum over header + data.
    let checksum = calc_checksum(&buffer[CHECKSUM_SIZE..]);
    (&mut buffer[..CHECKSUM_SIZE]).put_u128_le(checksum);

    Ok(buffer.freeze())
}

/// Streaming decoder for HTTP-level `Content-Encoding: zstd` responses.
/// Does not expect ClickHouse's native compression framing.
pub(crate) struct ZstdHttpDecoder<S> {
    stream: S,
    decoder: zstd::stream::raw::Decoder<'static>,
    input: BytesMut,
    output: BytesMut,
    stream_ended: bool,
    maximum_input_bytes: usize,
    maximum_output_bytes: usize,
}

impl<S> ZstdHttpDecoder<S> {
    pub(crate) fn new(stream: S, limits: Option<ResponseLimits>) -> Self {
        let maximum_output_bytes = limits
            .map(ResponseLimits::maximum_decoded_chunk_bytes)
            .unwrap_or(OUTPUT_BUFFER_SIZE);
        Self {
            stream,
            decoder: zstd::stream::raw::Decoder::new().expect("failed to create ZSTD decoder"),
            input: BytesMut::new(),
            output: BytesMut::zeroed(OUTPUT_BUFFER_SIZE.min(maximum_output_bytes)),
            stream_ended: false,
            maximum_input_bytes: limits
                .map(ResponseLimits::maximum_input_chunk_bytes)
                .unwrap_or(usize::MAX),
            maximum_output_bytes,
        }
    }
}

impl<S> Stream for ZstdHttpDecoder<S>
where
    S: Stream<Item = Result<Bytes>> + Unpin,
{
    type Item = Result<Chunk>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            // Try to decompress any buffered input.
            if !this.input.is_empty() {
                // Ensure the output buffer has enough initialized space.
                let spare = this.output.capacity() - this.output.len();
                let output_chunk_bytes = OUTPUT_BUFFER_SIZE.min(this.maximum_output_bytes);
                if spare < output_chunk_bytes {
                    this.output.reserve(output_chunk_bytes - spare);
                }
                this.output.resize(output_chunk_bytes, 0);

                let status = this
                    .decoder
                    .run_on_buffers(&this.input, &mut this.output)
                    .map_err(|err| Error::Decompression(err.into()))?;

                let net_size = status.bytes_read;
                let bytes_written = status.bytes_written;

                let _ = this.input.split_to(net_size);

                if bytes_written > 0 {
                    let data = this.output.split_to(bytes_written).freeze();
                    return Poll::Ready(Some(Ok(Chunk { data, net_size })));
                }

                // ZSTD consumed input but produced no output yet; need more data.
            }

            if this.stream_ended {
                return Poll::Ready(None);
            }

            // Pull more data from the inner stream.
            match ready!(Pin::new(&mut this.stream).poll_next(cx)) {
                Some(Ok(chunk)) => {
                    if this
                        .input
                        .len()
                        .checked_add(chunk.len())
                        .is_none_or(|length| length > this.maximum_input_bytes)
                    {
                        return Poll::Ready(Some(Err(Error::ResponseTooLarge {
                            stage: "ZSTD input buffer",
                            limit: this.maximum_input_bytes,
                        })));
                    }
                    this.input.extend_from_slice(&chunk);
                }
                Some(Err(err)) => return Poll::Ready(Some(Err(err))),
                None => {
                    this.stream_ended = true;

                    // Flush any remaining buffered output from the decoder.
                    if !this.input.is_empty() {
                        return Poll::Ready(Some(Err(Error::Decompression(
                            "unexpected end of ZSTD stream".into(),
                        ))));
                    }

                    return Poll::Ready(None);
                }
            }
        }
    }
}

#[tokio::test]
async fn it_decompresses_http_zstd() {
    use futures_util::stream::{self, TryStreamExt};

    let original = vec![
        1u8, 0, 2, 255, 255, 255, 255, 0, 1, 1, 1, 115, 6, 83, 116, 114, 105, 110, 103, 3, 97, 98,
        99,
    ];

    // Compress with raw ZSTD (no ClickHouse framing).
    let compressed = zstd::bulk::compress(&original, zstd::DEFAULT_COMPRESSION_LEVEL)
        .expect("failed to compress");

    async fn test(chunks: &[&[u8]], expected: &[u8]) {
        let stream = stream::iter(
            chunks
                .iter()
                .map(|s| Bytes::copy_from_slice(s))
                .map(Ok::<_, crate::error::Error>)
                .collect::<Vec<_>>(),
        );
        let mut decoder = ZstdHttpDecoder::new(stream, None);
        let mut result = Vec::new();
        while let Some(chunk) = decoder.try_next().await.unwrap() {
            result.extend_from_slice(&chunk.data);
        }
        assert_eq!(result, expected);
    }

    // 1 chunk.
    test(&[&compressed], &original).await;

    // 2 chunks.
    for i in 0..compressed.len() {
        let (left, right) = compressed.split_at(i);
        test(&[left, right], &original).await;
    }
}

#[tokio::test]
async fn input_limit_rejects_before_zstd_buffer_growth() {
    use std::num::NonZeroUsize;

    use futures_util::stream::{self, TryStreamExt};

    let limits = ResponseLimits::new(
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        NonZeroUsize::new(4).unwrap(),
    );
    let stream = stream::iter([Ok::<_, Error>(Bytes::from_static(b"abcde"))]);
    let error = match ZstdHttpDecoder::new(stream, Some(limits)).try_next().await {
        Err(error) => error,
        Ok(_) => panic!("oversized ZSTD input was admitted"),
    };
    assert!(matches!(
        error,
        Error::ResponseTooLarge {
            stage: "ZSTD input buffer",
            limit: 4
        }
    ));
}

#[test]
fn it_compresses_and_decompresses() {
    let source = vec![
        1u8, 0, 2, 255, 255, 255, 255, 0, 1, 1, 1, 115, 6, 83, 116, 114, 105, 110, 103, 3, 97, 98,
        99,
    ];

    let compressed = compress(&source, None).unwrap();

    // Verify the magic byte.
    assert_eq!(compressed[CHECKSUM_SIZE], ZSTD_MAGIC);

    // Verify decompression of the payload.
    let decompressed = zstd::bulk::decompress(&compressed[META_SIZE..], source.len()).unwrap();
    assert_eq!(decompressed, source);
}
