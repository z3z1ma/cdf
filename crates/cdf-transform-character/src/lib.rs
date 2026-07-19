use std::sync::Arc;

use bytes::Bytes;
use cdf_kernel::{CdfError, Result};
use cdf_memory::{AccountedBytes, ReservationRequest, reserve};
use cdf_runtime::{
    AccountedByteCursor, AccountedByteStream, ByteTransformDescriptor, ByteTransformDriver,
    ByteTransformId, ByteTransformRequest, MagicSignature, TransformChecksumBehavior,
    TransformExpansionGuard,
};
use futures_util::stream;
use serde::{Deserialize, Serialize};

const UTF8_BOM: &[u8; 3] = b"\xef\xbb\xbf";
const UTF16_LE_BOM: &[u8; 2] = b"\xff\xfe";
const UTF16_BE_BOM: &[u8; 2] = b"\xfe\xff";
const INTERNAL_WORKING_SET_BYTES: u64 = 64;
const DEFAULT_MAXIMUM_WORKING_SET_BYTES: u64 = 16 * 1024 * 1024 + INTERNAL_WORKING_SET_BYTES;
const DEFAULT_MAXIMUM_EXPANDED_BYTES: u64 = 4 * 1024 * 1024 * 1024 * 1024;
const DEFAULT_MAXIMUM_EXPANSION_RATIO: u32 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CharacterEncoding {
    #[serde(rename = "auto")]
    Auto,
    #[serde(rename = "utf8")]
    Utf8,
    #[serde(rename = "utf16le")]
    Utf16Le,
    #[serde(rename = "utf16be")]
    Utf16Be,
    #[serde(rename = "windows1252")]
    Windows1252,
    #[serde(rename = "iso8859_1")]
    Iso8859_1,
}

impl CharacterEncoding {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "text_auto",
            Self::Utf8 => "utf8",
            Self::Utf16Le => "utf16le",
            Self::Utf16Be => "utf16be",
            Self::Windows1252 => "windows1252",
            Self::Iso8859_1 => "iso8859_1",
        }
    }

    pub fn is_utf16(self) -> bool {
        matches!(self, Self::Utf16Le | Self::Utf16Be)
    }

    pub fn maximum_utf8_bytes_per_unit(self) -> u64 {
        match self {
            Self::Auto | Self::Utf8 | Self::Utf16Le | Self::Utf16Be => 4,
            Self::Windows1252 | Self::Iso8859_1 => 3,
        }
    }

    pub fn encode_ascii(self, value: &str) -> Result<Vec<u8>> {
        if self == Self::Auto {
            return Err(CdfError::contract(
                "auto character encoding cannot encode a pinned delimiter",
            ));
        }
        if !value.is_ascii() {
            return Err(CdfError::contract(
                "character delimiter encoding accepts ASCII control text only",
            ));
        }
        if !self.is_utf16() {
            return Ok(value.as_bytes().to_vec());
        }
        Ok(value
            .encode_utf16()
            .flat_map(|unit| match self {
                Self::Utf16Le => unit.to_le_bytes(),
                Self::Utf16Be => unit.to_be_bytes(),
                _ => unreachable!("UTF-16 branch checked above"),
            })
            .collect())
    }

    pub fn strip_matching_bom(self, input: &[u8]) -> Result<&[u8]> {
        let observed = if input.starts_with(UTF8_BOM) {
            Some((Self::Utf8, UTF8_BOM.len()))
        } else if input.starts_with(UTF16_LE_BOM) {
            Some((Self::Utf16Le, UTF16_LE_BOM.len()))
        } else if input.starts_with(UTF16_BE_BOM) {
            Some((Self::Utf16Be, UTF16_BE_BOM.len()))
        } else {
            None
        };
        match (self, observed) {
            (Self::Auto, Some((_, bytes))) => Ok(&input[bytes..]),
            (Self::Auto, None) => Ok(input),
            (configured, Some((observed, bytes))) if configured == observed => Ok(&input[bytes..]),
            (configured, Some((observed, _))) => Err(CdfError::data(format!(
                "character encoding conflict: configured {} but input BOM declares {}",
                configured.as_str(),
                observed.as_str()
            ))),
            (_, None) => Ok(input),
        }
    }

    pub fn decode_slice(self, input: &[u8], context: &str) -> Result<String> {
        match self {
            Self::Auto => Err(CdfError::contract(
                "auto character encoding must be resolved before slice decoding",
            )),
            Self::Utf8 => std::str::from_utf8(input)
                .map(str::to_owned)
                .map_err(|error| {
                    CdfError::data(format!(
                        "{context} is invalid UTF-8 at byte {}",
                        error.valid_up_to()
                    ))
                }),
            Self::Utf16Le | Self::Utf16Be => decode_utf16_slice(input, self, context),
            Self::Windows1252 => decode_single_byte_slice(input, true, context),
            Self::Iso8859_1 => decode_single_byte_slice(input, false, context),
        }
    }
}

#[derive(Debug)]
pub struct CharacterTransformDriver {
    configured: CharacterEncoding,
    descriptor: ByteTransformDescriptor,
}

impl CharacterTransformDriver {
    pub fn new(configured: CharacterEncoding) -> Result<Self> {
        let magic = if configured == CharacterEncoding::Auto {
            vec![
                MagicSignature {
                    offset: 0,
                    bytes: UTF8_BOM.to_vec(),
                    strong: true,
                },
                MagicSignature {
                    offset: 0,
                    bytes: UTF16_LE_BOM.to_vec(),
                    strong: true,
                },
                MagicSignature {
                    offset: 0,
                    bytes: UTF16_BE_BOM.to_vec(),
                    strong: true,
                },
            ]
        } else {
            Vec::new()
        };
        Ok(Self {
            configured,
            descriptor: ByteTransformDescriptor {
                transform_id: ByteTransformId::new(configured.as_str())?,
                semantic_version: "1.0.0".to_owned(),
                extensions: Vec::new(),
                magic,
                preserves_random_access: false,
                splittable: false,
                supports_concatenated_members: false,
                maximum_output_chunk_bytes: 16 * 1024 * 1024,
                maximum_working_set_bytes: DEFAULT_MAXIMUM_WORKING_SET_BYTES,
                maximum_expanded_bytes: DEFAULT_MAXIMUM_EXPANDED_BYTES,
                maximum_expansion_ratio: DEFAULT_MAXIMUM_EXPANSION_RATIO,
                checksum: TransformChecksumBehavior::None,
            },
        })
    }
}

impl ByteTransformDriver for CharacterTransformDriver {
    fn descriptor(&self) -> &ByteTransformDescriptor {
        &self.descriptor
    }

    fn transform(
        &self,
        input: AccountedByteStream,
        request: ByteTransformRequest,
    ) -> Result<AccountedByteStream> {
        request.validate_for(&self.descriptor)?;
        if request.preferred_output_chunk_bytes < 4 {
            return Err(CdfError::contract(
                "character transforms require output chunks of at least four bytes",
            ));
        }
        if request
            .preferred_output_chunk_bytes
            .checked_add(INTERNAL_WORKING_SET_BYTES)
            .is_none_or(|bytes| bytes > self.descriptor.maximum_working_set_bytes)
        {
            return Err(CdfError::contract(
                "character output chunk plus decoder state exceeds driver authority",
            ));
        }
        let output_chunk_bytes = usize::try_from(request.preferred_output_chunk_bytes)
            .map_err(|_| CdfError::contract("character output chunk exceeds usize"))?;
        let expansion = TransformExpansionGuard::new(&request)?;
        let state = CharacterState {
            input: AccountedByteCursor::new(input),
            request,
            configured: self.configured,
            selected: None,
            output_chunk_bytes,
            expansion,
            pending: Vec::with_capacity(4),
            terminal: false,
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

struct CharacterState {
    input: AccountedByteCursor,
    request: ByteTransformRequest,
    configured: CharacterEncoding,
    selected: Option<CharacterEncoding>,
    output_chunk_bytes: usize,
    expansion: TransformExpansionGuard,
    pending: Vec<u8>,
    terminal: bool,
}

impl CharacterState {
    async fn next_output(&mut self) -> Result<Option<AccountedBytes>> {
        self.request.cancellation.check()?;
        if self.terminal {
            return Ok(None);
        }
        if self.selected.is_none() {
            self.select_encoding().await?;
        }
        let reservation = ReservationRequest::new(
            self.request.consumer.clone(),
            u64::try_from(self.output_chunk_bytes)
                .map_err(|_| CdfError::data("character output chunk exceeds u64"))?,
        )?;
        let lease = reserve(Arc::clone(&self.request.memory), reservation).await?;
        let mut output = Vec::with_capacity(self.output_chunk_bytes);
        loop {
            self.request.cancellation.check()?;
            let selected = self
                .selected
                .ok_or_else(|| CdfError::internal("character encoding was not selected"))?;
            let eof = match selected {
                CharacterEncoding::Auto => {
                    return Err(CdfError::internal(
                        "auto character encoding remained unresolved",
                    ));
                }
                CharacterEncoding::Utf8 => self.decode_utf8(&mut output).await?,
                CharacterEncoding::Utf16Le => self.decode_utf16(&mut output, true).await?,
                CharacterEncoding::Utf16Be => self.decode_utf16(&mut output, false).await?,
                CharacterEncoding::Windows1252 => {
                    self.decode_single_byte(&mut output, true).await?
                }
                CharacterEncoding::Iso8859_1 => self.decode_single_byte(&mut output, false).await?,
            };
            if eof || output.len() >= self.output_chunk_bytes.saturating_sub(3) {
                self.expansion
                    .record(output.len(), self.input.consumed_bytes(), eof)?;
                if eof {
                    self.terminal = true;
                }
                if output.is_empty() {
                    return Ok(None);
                }
                return AccountedBytes::new(Bytes::from(output), lease).map(Some);
            }
        }
    }

    async fn select_encoding(&mut self) -> Result<()> {
        while self.pending.len() < 3 {
            let Some(byte) = self.input.next_byte().await? else {
                break;
            };
            self.pending.push(byte);
        }
        let bom = if self.pending.starts_with(UTF8_BOM) {
            Some((CharacterEncoding::Utf8, UTF8_BOM.len()))
        } else if self.pending.starts_with(UTF16_LE_BOM) {
            Some((CharacterEncoding::Utf16Le, UTF16_LE_BOM.len()))
        } else if self.pending.starts_with(UTF16_BE_BOM) {
            Some((CharacterEncoding::Utf16Be, UTF16_BE_BOM.len()))
        } else {
            None
        };
        let selected = match (self.configured, bom) {
            (CharacterEncoding::Auto, Some((encoding, bytes))) => {
                self.pending.drain(..bytes);
                encoding
            }
            (CharacterEncoding::Auto, None) => CharacterEncoding::Utf8,
            (configured, Some((observed, bytes))) if configured == observed => {
                self.pending.drain(..bytes);
                configured
            }
            (configured, Some((observed, _))) => {
                return Err(CdfError::data(format!(
                    "character encoding conflict: configured {} but input BOM declares {}",
                    configured.as_str(),
                    observed.as_str()
                )));
            }
            (configured, None) => configured,
        };
        self.selected = Some(selected);
        Ok(())
    }

    async fn decode_utf8(&mut self, output: &mut Vec<u8>) -> Result<bool> {
        if !self.pending.is_empty() {
            loop {
                match std::str::from_utf8(&self.pending) {
                    Ok(_) => {
                        if output.len() + self.pending.len() > self.output_chunk_bytes {
                            return Ok(false);
                        }
                        output.extend_from_slice(&self.pending);
                        self.pending.clear();
                        break;
                    }
                    Err(error) if error.error_len().is_some() => {
                        return Err(self.invalid_encoding("UTF-8", error.valid_up_to()));
                    }
                    Err(_) if self.pending.len() == 4 => {
                        return Err(self.invalid_encoding("UTF-8", 0));
                    }
                    Err(_) => match self.input.next_byte().await? {
                        Some(byte) => self.pending.push(byte),
                        None => {
                            return Err(CdfError::data(format!(
                                "UTF-8 input ended with an incomplete code point at byte {}",
                                self.input
                                    .consumed_bytes()
                                    .saturating_sub(self.pending.len() as u64)
                            )));
                        }
                    },
                }
            }
        }
        if !self.input.ensure_current().await? {
            return Ok(true);
        }
        let remaining = self.output_chunk_bytes - output.len();
        let slice = self.input.current_slice();
        let candidate_len = slice.len().min(remaining);
        let candidate = &slice[..candidate_len];
        match std::str::from_utf8(candidate) {
            Ok(_) => {
                output.extend_from_slice(candidate);
                self.input.consume(candidate_len)?;
            }
            Err(error) if error.error_len().is_some() => {
                if error.valid_up_to() > 0 {
                    output.extend_from_slice(&candidate[..error.valid_up_to()]);
                    self.input.consume(error.valid_up_to())?;
                }
                return Err(self.invalid_encoding("UTF-8", 0));
            }
            Err(error) => {
                let valid = error.valid_up_to();
                output.extend_from_slice(&candidate[..valid]);
                if candidate_len == slice.len() {
                    self.pending.extend_from_slice(&candidate[valid..]);
                    self.input.consume(candidate_len)?;
                } else {
                    self.input.consume(valid)?;
                }
            }
        }
        Ok(false)
    }

    async fn decode_utf16(&mut self, output: &mut Vec<u8>, little_endian: bool) -> Result<bool> {
        while output.len() < self.output_chunk_bytes.saturating_sub(3) {
            while self.pending.len() < 2 {
                let Some(byte) = self.input.next_byte().await? else {
                    if self.pending.is_empty() {
                        return Ok(true);
                    }
                    return Err(CdfError::data(format!(
                        "UTF-16 input ended with an incomplete code unit at byte {}",
                        self.input
                            .consumed_bytes()
                            .saturating_sub(self.pending.len() as u64)
                    )));
                };
                self.pending.push(byte);
            }
            let first = read_u16(&self.pending[..2], little_endian);
            let (codepoint, consumed) = if (0xd800..=0xdbff).contains(&first) {
                while self.pending.len() < 4 {
                    let Some(byte) = self.input.next_byte().await? else {
                        return Err(CdfError::data(format!(
                            "UTF-16 input ended after a high surrogate at byte {}",
                            self.input
                                .consumed_bytes()
                                .saturating_sub(self.pending.len() as u64)
                        )));
                    };
                    self.pending.push(byte);
                }
                let second = read_u16(&self.pending[2..4], little_endian);
                if !(0xdc00..=0xdfff).contains(&second) {
                    return Err(self.invalid_encoding("UTF-16", 2));
                }
                (
                    0x1_0000 + ((u32::from(first) - 0xd800) << 10) + (u32::from(second) - 0xdc00),
                    4,
                )
            } else if (0xdc00..=0xdfff).contains(&first) {
                return Err(self.invalid_encoding("UTF-16", 0));
            } else {
                (u32::from(first), 2)
            };
            let character =
                char::from_u32(codepoint).ok_or_else(|| self.invalid_encoding("UTF-16", 0))?;
            let mut encoded = [0_u8; 4];
            let bytes = character.encode_utf8(&mut encoded).as_bytes();
            output.extend_from_slice(bytes);
            self.pending.drain(..consumed);
        }
        Ok(false)
    }

    async fn decode_single_byte(
        &mut self,
        output: &mut Vec<u8>,
        windows1252: bool,
    ) -> Result<bool> {
        while output.len() < self.output_chunk_bytes.saturating_sub(3) {
            let byte = if let Some(byte) = self.pending.first().copied() {
                self.pending.remove(0);
                byte
            } else {
                let Some(byte) = self.input.next_byte().await? else {
                    return Ok(true);
                };
                byte
            };
            let character = if windows1252 {
                windows_1252(byte).ok_or_else(|| {
                    CdfError::data(format!(
                        "Windows-1252 input contains undefined byte 0x{byte:02x} at byte {}",
                        self.input.consumed_bytes().saturating_sub(1)
                    ))
                })?
            } else {
                char::from_u32(u32::from(byte))
                    .ok_or_else(|| CdfError::internal("ISO-8859-1 byte was not a scalar"))?
            };
            let mut encoded = [0_u8; 4];
            output.extend_from_slice(character.encode_utf8(&mut encoded).as_bytes());
        }
        Ok(false)
    }

    fn invalid_encoding(&self, encoding: &str, relative: usize) -> CdfError {
        let pending = u64::try_from(self.pending.len()).unwrap_or(u64::MAX);
        CdfError::data(format!(
            "{encoding} input is invalid at byte {}; declare the correct character encoding or quarantine the containing record/file",
            self.input
                .consumed_bytes()
                .saturating_sub(pending)
                .saturating_add(relative as u64)
        ))
    }
}

fn decode_utf16_slice(input: &[u8], encoding: CharacterEncoding, context: &str) -> Result<String> {
    if !input.len().is_multiple_of(2) {
        return Err(CdfError::data(format!(
            "{context} ends with an incomplete UTF-16 code unit"
        )));
    }
    let units = input.chunks_exact(2).map(|bytes| match encoding {
        CharacterEncoding::Utf16Le => u16::from_le_bytes([bytes[0], bytes[1]]),
        CharacterEncoding::Utf16Be => u16::from_be_bytes([bytes[0], bytes[1]]),
        _ => unreachable!("UTF-16 slice decoder called only for UTF-16 encoding"),
    });
    char::decode_utf16(units)
        .enumerate()
        .map(|(index, value)| {
            value.map_err(|_| {
                CdfError::data(format!(
                    "{context} has an invalid UTF-16 surrogate at code unit {index}"
                ))
            })
        })
        .collect()
}

fn decode_single_byte_slice(input: &[u8], windows1252: bool, context: &str) -> Result<String> {
    input
        .iter()
        .enumerate()
        .map(|(index, byte)| {
            if windows1252 {
                windows_1252(*byte).ok_or_else(|| {
                    CdfError::data(format!(
                        "{context} has undefined Windows-1252 byte 0x{byte:02x} at offset {index}"
                    ))
                })
            } else {
                char::from_u32(u32::from(*byte))
                    .ok_or_else(|| CdfError::internal("ISO-8859-1 byte was not a scalar"))
            }
        })
        .collect()
}

fn read_u16(bytes: &[u8], little_endian: bool) -> u16 {
    if little_endian {
        u16::from_le_bytes([bytes[0], bytes[1]])
    } else {
        u16::from_be_bytes([bytes[0], bytes[1]])
    }
}

fn windows_1252(byte: u8) -> Option<char> {
    let codepoint = match byte {
        0x80 => 0x20ac,
        0x81 | 0x8d | 0x8f | 0x90 | 0x9d => return None,
        0x82 => 0x201a,
        0x83 => 0x0192,
        0x84 => 0x201e,
        0x85 => 0x2026,
        0x86 => 0x2020,
        0x87 => 0x2021,
        0x88 => 0x02c6,
        0x89 => 0x2030,
        0x8a => 0x0160,
        0x8b => 0x2039,
        0x8c => 0x0152,
        0x8e => 0x017d,
        0x91 => 0x2018,
        0x92 => 0x2019,
        0x93 => 0x201c,
        0x94 => 0x201d,
        0x95 => 0x2022,
        0x96 => 0x2013,
        0x97 => 0x2014,
        0x98 => 0x02dc,
        0x99 => 0x2122,
        0x9a => 0x0161,
        0x9b => 0x203a,
        0x9c => 0x0153,
        0x9e => 0x017e,
        0x9f => 0x0178,
        value => u32::from(value),
    };
    char::from_u32(codepoint)
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Instant};

    use cdf_memory::{
        ConsumerKey, DeterministicMemoryCoordinator, MemoryClass, MemoryCoordinator, MemorySnapshot,
    };
    use futures_executor::block_on;
    use futures_util::StreamExt;

    use super::*;

    fn input_stream(
        bytes: Vec<u8>,
        chunk_bytes: usize,
        memory: Arc<dyn MemoryCoordinator>,
    ) -> AccountedByteStream {
        let bytes = Bytes::from(bytes);
        let consumer = ConsumerKey::new("character-test-input", MemoryClass::Source).unwrap();
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
                let chunk = AccountedBytes::new(bytes.slice(offset..end), lease)?;
                Ok(Some((chunk, (bytes, end, memory, consumer))))
            },
        ))
    }

    fn decode(
        encoding: CharacterEncoding,
        input: Vec<u8>,
        input_chunk_bytes: usize,
        output_chunk_bytes: u64,
    ) -> (Result<Vec<u8>>, MemorySnapshot) {
        let input_size_bytes = u64::try_from(input.len()).unwrap();
        let memory: Arc<dyn MemoryCoordinator> = Arc::new(
            DeterministicMemoryCoordinator::new(32 * 1024 * 1024, Default::default()).unwrap(),
        );
        let request = ByteTransformRequest {
            preferred_output_chunk_bytes: output_chunk_bytes,
            maximum_expanded_bytes: 128 * 1024 * 1024,
            maximum_expansion_ratio: 4,
            input_size_bytes: Some(input_size_bytes),
            memory: Arc::clone(&memory),
            consumer: ConsumerKey::new("character-test", MemoryClass::Transform).unwrap(),
            cancellation: Default::default(),
        };
        let driver = CharacterTransformDriver::new(encoding).unwrap();
        let result = driver
            .transform(
                input_stream(input, input_chunk_bytes, Arc::clone(&memory)),
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
    fn auto_bom_and_explicit_encodings_stream_without_replacement() {
        let text = "hello — 世界 😀";
        let mut utf16_le = UTF16_LE_BOM.to_vec();
        utf16_le.extend(text.encode_utf16().flat_map(u16::to_le_bytes));
        assert_eq!(
            decode(CharacterEncoding::Auto, utf16_le, 1, 7).0.unwrap(),
            text.as_bytes()
        );

        let mut utf16_be = UTF16_BE_BOM.to_vec();
        utf16_be.extend(text.encode_utf16().flat_map(u16::to_be_bytes));
        assert_eq!(
            decode(CharacterEncoding::Utf16Be, utf16_be, 1, 7)
                .0
                .unwrap(),
            text.as_bytes()
        );

        assert_eq!(
            decode(
                CharacterEncoding::Windows1252,
                vec![b'c', b'a', b'f', 0xe9, b' ', 0x80],
                1,
                7,
            )
            .0
            .unwrap(),
            "café €".as_bytes()
        );
        assert_eq!(
            decode(CharacterEncoding::Iso8859_1, vec![0x41, 0x80, 0xff], 1, 7)
                .0
                .unwrap(),
            "A\u{80}ÿ".as_bytes()
        );
    }

    #[test]
    fn rejects_bom_conflicts_invalid_utf_and_undefined_windows_bytes() {
        let error = decode(
            CharacterEncoding::Utf8,
            [UTF16_LE_BOM.as_slice(), b"x\0"].concat(),
            1,
            8,
        )
        .0
        .unwrap_err();
        assert!(error.message.contains("encoding conflict"));

        assert!(
            decode(CharacterEncoding::Utf8, vec![0xf0, 0x9f, 0x98], 1, 8)
                .0
                .unwrap_err()
                .message
                .contains("incomplete code point")
        );
        assert!(
            decode(
                CharacterEncoding::Utf16Le,
                vec![0x00, 0xd8, 0x41, 0x00],
                1,
                8,
            )
            .0
            .is_err()
        );
        assert!(
            decode(CharacterEncoding::Windows1252, vec![0x81], 1, 8)
                .0
                .unwrap_err()
                .message
                .contains("undefined byte")
        );
    }

    #[test]
    #[ignore = "performance evidence; run in release mode"]
    fn utf8_driver_reference_rate() {
        const BYTES: usize = 64 * 1024 * 1024;
        let input = b"ascii fast path with utf8 validation\n"
            .iter()
            .copied()
            .cycle()
            .take(BYTES)
            .collect::<Vec<_>>();

        let reference_start = Instant::now();
        let reference = std::str::from_utf8(&input).unwrap().as_bytes().to_vec();
        let reference_sum = reference
            .iter()
            .fold(0_u64, |sum, byte| sum.wrapping_add(u64::from(*byte)));
        let reference_elapsed = reference_start.elapsed();

        let driver_start = Instant::now();
        let input_size_bytes = u64::try_from(input.len()).unwrap();
        let memory: Arc<dyn MemoryCoordinator> = Arc::new(
            DeterministicMemoryCoordinator::new(32 * 1024 * 1024, Default::default()).unwrap(),
        );
        let request = ByteTransformRequest {
            preferred_output_chunk_bytes: 1024 * 1024,
            maximum_expanded_bytes: 128 * 1024 * 1024,
            maximum_expansion_ratio: 4,
            input_size_bytes: Some(input_size_bytes),
            memory: Arc::clone(&memory),
            consumer: ConsumerKey::new("character-benchmark", MemoryClass::Transform).unwrap(),
            cancellation: Default::default(),
        };
        let mut output = CharacterTransformDriver::new(CharacterEncoding::Utf8)
            .unwrap()
            .transform(
                input_stream(input, 1024 * 1024, Arc::clone(&memory)),
                request,
            )
            .unwrap();
        let (driver_bytes, driver_sum) = block_on(async move {
            let mut bytes = 0_usize;
            let mut sum = 0_u64;
            while let Some(chunk) = output.next().await {
                let chunk = chunk.unwrap();
                bytes += chunk.payload().len();
                sum = chunk
                    .payload()
                    .iter()
                    .fold(sum, |sum, byte| sum.wrapping_add(u64::from(*byte)));
            }
            (bytes, sum)
        });
        let driver_elapsed = driver_start.elapsed();
        assert_eq!(driver_bytes, reference.len());
        assert_eq!(driver_sum, reference_sum);
        assert_eq!(memory.snapshot().current_bytes, 0);
        let reference_ratio = reference_elapsed.as_secs_f64() / driver_elapsed.as_secs_f64();
        eprintln!(
            "utf8_reference_ms={:.3} utf8_driver_ms={:.3} reference_ratio={reference_ratio:.3}",
            reference_elapsed.as_secs_f64() * 1000.0,
            driver_elapsed.as_secs_f64() * 1000.0,
        );
        assert!(
            reference_ratio >= 0.6,
            "UTF-8 driver achieved {reference_ratio:.3}x reference throughput"
        );
    }
}
