// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_buffer::{Buffer, MutableBuffer};
use arrow_data::UnsafeFlag;
use arrow_schema::{ArrowError, SchemaRef};

use crate::convert::{IpcSchemaLimits, MessageBuffer, try_fb_to_schema_with_limits};
use crate::reader::{RecordBatchDecoder, read_dictionary_impl};
use crate::{CONTINUATION_MARKER, MessageHeader};

/// A low-level interface for reading [`RecordBatch`] data from a stream of bytes
///
/// See [StreamReader](crate::reader::StreamReader) for a higher-level interface
#[derive(Debug)]
pub struct StreamDecoder {
    /// The schema of this decoder, if read
    schema: Option<SchemaRef>,
    /// Lookup table for dictionaries by ID
    dictionaries: HashMap<i64, ArrayRef>,
    /// The decoder state
    state: DecoderState,
    /// A scratch buffer when a read is split across multiple `Buffer`
    buf: MutableBuffer,
    /// Whether or not array data in input buffers are required to be aligned
    require_alignment: bool,
    /// Should validation be skipped when reading data? Defaults to false.
    ///
    /// See [`StreamDecoder::with_skip_validation`] for details.
    ///
    skip_validation: UnsafeFlag,
    /// Maximum flatbuffer metadata bytes admitted before scratch growth.
    maximum_message_bytes: usize,
    /// Maximum IPC body bytes admitted before scratch growth or batch decoding.
    maximum_body_bytes: usize,
    /// Maximum rows admitted from one record-batch header.
    maximum_record_batch_rows: usize,
    /// Structural and owned-allocation ceilings applied before schema conversion.
    schema_limits: IpcSchemaLimits,
    /// Whether dictionary messages are rejected before their values can accumulate across batches.
    reject_dictionary_batches: bool,
    /// Whether compressed batches are rejected before decoded buffers can overlap the encoded
    /// body and any alignment copies.
    reject_compressed_record_batches: bool,
}

impl Default for StreamDecoder {
    fn default() -> Self {
        Self {
            schema: None,
            dictionaries: HashMap::new(),
            state: DecoderState::default(),
            buf: MutableBuffer::new(0),
            require_alignment: false,
            skip_validation: UnsafeFlag::default(),
            maximum_message_bytes: usize::MAX,
            maximum_body_bytes: usize::MAX,
            maximum_record_batch_rows: usize::MAX,
            schema_limits: IpcSchemaLimits::unlimited(),
            reject_dictionary_batches: false,
            reject_compressed_record_batches: false,
        }
    }
}

#[derive(Debug)]
enum DecoderState {
    /// Decoding the message header
    Header {
        /// Temporary buffer
        buf: [u8; 4],
        /// Number of bytes read into buf
        read: u8,
        /// If we have read a continuation token
        continuation: bool,
    },
    /// Decoding the message flatbuffer
    Message {
        /// The size of the message flatbuffer
        size: u32,
    },
    /// Decoding the message body
    Body {
        /// The message flatbuffer
        message: MessageBuffer,
    },
    /// Reached the end of the stream
    Finished,
}

fn validate_body_length(
    message: &MessageBuffer,
    maximum_body_bytes: usize,
) -> Result<(), ArrowError> {
    let body_length = usize::try_from(message.as_ref().bodyLength()).map_err(|_| {
        ArrowError::IpcError(format!(
            "IPC message body length {} is negative or not representable",
            message.as_ref().bodyLength()
        ))
    })?;
    if body_length > maximum_body_bytes {
        return Err(ArrowError::IpcError(format!(
            "IPC message body length {body_length} exceeds configured {maximum_body_bytes}-byte limit"
        )));
    }
    Ok(())
}

impl Default for DecoderState {
    fn default() -> Self {
        Self::Header {
            buf: [0; 4],
            read: 0,
            continuation: false,
        }
    }
}

impl StreamDecoder {
    /// Create a new [`StreamDecoder`]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum IPC flatbuffer metadata length accepted by this decoder.
    pub fn with_max_message_size(mut self, maximum_bytes: usize) -> Self {
        self.maximum_message_bytes = maximum_bytes;
        self
    }

    /// Sets the maximum IPC message body length accepted by this decoder.
    pub fn with_max_body_size(mut self, maximum_bytes: usize) -> Self {
        self.maximum_body_bytes = maximum_bytes;
        self
    }

    /// Sets the maximum row count accepted from one record-batch header.
    pub fn with_max_record_batch_rows(mut self, maximum_rows: usize) -> Self {
        self.maximum_record_batch_rows = maximum_rows;
        self
    }

    /// Sets the structural and owned-allocation ceilings used during schema conversion.
    pub fn with_schema_limits(mut self, limits: IpcSchemaLimits) -> Self {
        self.schema_limits = limits;
        self
    }

    /// Rejects dictionary messages before decoding their retained values.
    ///
    /// A stream can introduce arbitrarily many dictionary IDs before yielding a record batch.
    /// Callers without a cumulative dictionary-memory authority can use this to keep a per-message
    /// body ceiling from becoming an unbounded cross-message allocation.
    pub fn with_reject_dictionary_batches(mut self, reject: bool) -> Self {
        self.reject_dictionary_batches = reject;
        self
    }

    /// Rejects compressed record batches before allocating their decoded buffers.
    pub fn with_reject_compressed_record_batches(mut self, reject: bool) -> Self {
        self.reject_compressed_record_batches = reject;
        self
    }

    /// Specifies whether or not array data in input buffers is required to be properly aligned.
    ///
    /// If `require_alignment` is true, this decoder will return an error if any array data in the
    /// input `buf` is not properly aligned.
    /// Under the hood it will use [`arrow_data::ArrayDataBuilder::build`] to construct
    /// [`arrow_data::ArrayData`].
    ///
    /// If `require_alignment` is false (the default), this decoder will automatically allocate a
    /// new aligned buffer and copy over the data if any array data in the input `buf` is not
    /// properly aligned. (Properly aligned array data will remain zero-copy.)
    /// Under the hood it will use [`arrow_data::ArrayDataBuilder::build_aligned`] to construct
    /// [`arrow_data::ArrayData`].
    pub fn with_require_alignment(mut self, require_alignment: bool) -> Self {
        self.require_alignment = require_alignment;
        self
    }

    /// Return the schema if decoded, else None.
    pub fn schema(&self) -> Option<SchemaRef> {
        self.schema.as_ref().map(|schema| schema.clone())
    }

    /// Specifies if validation should be skipped when reading data (defaults to `false`)
    ///
    /// # Safety
    ///
    /// This flag must only be set to `true` when you trust the input data and are
    /// sure the data you are reading is valid Arrow IPC stream data, otherwise
    /// undefined behavior may result.
    ///
    /// For example, DataFusion uses this when reading spill files it wrote itself.
    pub unsafe fn with_skip_validation(mut self, skip_validation: bool) -> Self {
        unsafe { self.skip_validation.set(skip_validation) };
        self
    }

    /// Try to read the next [`RecordBatch`] from the provided [`Buffer`]
    ///
    /// [`Buffer::advance`] will be called on `buffer` for any consumed bytes.
    ///
    /// The push-based interface facilitates integration with sources that yield arbitrarily
    /// delimited bytes ranges, such as a chunked byte stream received from object storage
    ///
    /// ```
    /// # use arrow_array::RecordBatch;
    /// # use arrow_buffer::Buffer;
    /// # use arrow_ipc::reader::StreamDecoder;
    /// # use arrow_schema::ArrowError;
    /// #
    /// fn print_stream<I>(src: impl Iterator<Item = Buffer>) -> Result<(), ArrowError> {
    ///     let mut decoder = StreamDecoder::new();
    ///     for mut x in src {
    ///         while !x.is_empty() {
    ///             if let Some(x) = decoder.decode(&mut x)? {
    ///                 println!("{x:?}");
    ///             }
    ///             if let Some(schema) = decoder.schema() {
    ///                 println!("Schema: {schema:?}");
    ///             }
    ///         }
    ///     }
    ///     decoder.finish().unwrap();
    ///     Ok(())
    /// }
    /// ```
    pub fn decode(&mut self, buffer: &mut Buffer) -> Result<Option<RecordBatch>, ArrowError> {
        while !buffer.is_empty() {
            match &mut self.state {
                DecoderState::Header {
                    buf,
                    read,
                    continuation,
                } => {
                    let offset_buf = &mut buf[*read as usize..];
                    let to_read = buffer.len().min(offset_buf.len());
                    offset_buf[..to_read].copy_from_slice(&buffer[..to_read]);
                    *read += to_read as u8;
                    buffer.advance(to_read);
                    if *read == 4 {
                        if !*continuation && buf == &CONTINUATION_MARKER {
                            *continuation = true;
                            *read = 0;
                            continue;
                        }
                        let size = u32::from_le_bytes(*buf);

                        if size == 0 {
                            self.state = DecoderState::Finished;
                            continue;
                        }
                        if size as usize > self.maximum_message_bytes {
                            return Err(ArrowError::IpcError(format!(
                                "IPC message metadata length {size} exceeds configured {}-byte limit",
                                self.maximum_message_bytes
                            )));
                        }
                        self.state = DecoderState::Message { size };
                    }
                }
                DecoderState::Message { size } => {
                    let len = *size as usize;
                    if self.buf.is_empty() && buffer.len() > len {
                        let message = MessageBuffer::try_new(buffer.slice_with_length(0, len))?;
                        validate_body_length(&message, self.maximum_body_bytes)?;
                        self.state = DecoderState::Body { message };
                        buffer.advance(len);
                        continue;
                    }

                    let to_read = buffer.len().min(len - self.buf.len());
                    self.buf.extend_from_slice(&buffer[..to_read]);
                    buffer.advance(to_read);
                    if self.buf.len() == len {
                        let message = MessageBuffer::try_new(std::mem::take(&mut self.buf).into())?;
                        validate_body_length(&message, self.maximum_body_bytes)?;
                        self.state = DecoderState::Body { message };
                    }
                }
                DecoderState::Body { message } => {
                    let message = message.as_ref();
                    let body_length = usize::try_from(message.bodyLength()).map_err(|_| {
                        ArrowError::IpcError(format!(
                            "IPC message body length {} is negative or not representable",
                            message.bodyLength()
                        ))
                    })?;

                    let body = if self.buf.is_empty() && buffer.len() >= body_length {
                        let body = buffer.slice_with_length(0, body_length);
                        buffer.advance(body_length);
                        body
                    } else {
                        let to_read = buffer.len().min(body_length - self.buf.len());
                        self.buf.extend_from_slice(&buffer[..to_read]);
                        buffer.advance(to_read);

                        if self.buf.len() != body_length {
                            continue;
                        }
                        std::mem::take(&mut self.buf).into()
                    };

                    let version = message.version();
                    match message.header_type() {
                        MessageHeader::Schema => {
                            if self.schema.is_some() {
                                return Err(ArrowError::IpcError(
                                    "Not expecting a schema when messages are read".to_string(),
                                ));
                            }

                            let ipc_schema = message.header_as_schema().ok_or_else(|| {
                                ArrowError::IpcError(
                                    "schema message omitted its schema header".to_string(),
                                )
                            })?;
                            let schema =
                                try_fb_to_schema_with_limits(ipc_schema, self.schema_limits)?;
                            self.state = DecoderState::default();
                            self.schema = Some(Arc::new(schema));
                        }
                        MessageHeader::RecordBatch => {
                            let batch = message.header_as_record_batch().ok_or_else(|| {
                                ArrowError::IpcError(
                                    "record-batch message omitted its batch header".to_string(),
                                )
                            })?;
                            if self.reject_compressed_record_batches
                                && batch.compression().is_some()
                            {
                                return Err(ArrowError::IpcError(
                                    "compressed IPC record batches are disabled by the configured stream limits"
                                        .to_string(),
                                ));
                            }
                            let schema = self.schema.clone().ok_or_else(|| {
                                ArrowError::IpcError("Missing schema".to_string())
                            })?;
                            let batch = RecordBatchDecoder::try_new(
                                &body,
                                batch,
                                schema,
                                &self.dictionaries,
                                &version,
                            )?
                            .with_require_alignment(self.require_alignment)
                            .with_max_decoded_body_size(self.maximum_body_bytes)
                            .with_max_record_batch_rows(self.maximum_record_batch_rows)
                            .read_record_batch()?;
                            self.state = DecoderState::default();
                            return Ok(Some(batch));
                        }
                        MessageHeader::DictionaryBatch => {
                            if self.reject_dictionary_batches {
                                return Err(ArrowError::IpcError(
                                    "IPC dictionary batches are disabled by the configured stream limits"
                                        .to_string(),
                                ));
                            }
                            let dictionary =
                                message.header_as_dictionary_batch().ok_or_else(|| {
                                    ArrowError::IpcError(
                                        "dictionary message omitted its dictionary header"
                                            .to_string(),
                                    )
                                })?;
                            let schema = self.schema.as_deref().ok_or_else(|| {
                                ArrowError::IpcError("Missing schema".to_string())
                            })?;
                            read_dictionary_impl(
                                &body,
                                dictionary,
                                schema,
                                &mut self.dictionaries,
                                &version,
                                self.require_alignment,
                                self.skip_validation.clone(),
                            )?;
                            self.state = DecoderState::default();
                        }
                        MessageHeader::NONE => {
                            self.state = DecoderState::default();
                        }
                        t => {
                            return Err(ArrowError::IpcError(format!(
                                "Message type unsupported by StreamDecoder: {t:?}"
                            )));
                        }
                    }
                }
                DecoderState::Finished => {
                    return Err(ArrowError::IpcError("Unexpected EOS".to_string()));
                }
            }
        }
        Ok(None)
    }

    /// Signal the end of stream
    ///
    /// Returns an error if any partial data remains in the stream
    pub fn finish(&mut self) -> Result<(), ArrowError> {
        match self.state {
            DecoderState::Finished
            | DecoderState::Header {
                read: 0,
                continuation: false,
                ..
            } => Ok(()),
            _ => Err(ArrowError::IpcError("Unexpected End of Stream".to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::{IpcWriteOptions, StreamWriter};
    use arrow_array::{
        DictionaryArray, Int32Array, Int64Array, RecordBatch, RunArray, types::Int32Type,
    };
    use arrow_schema::{DataType, Field, Schema};
    use flatbuffers::FlatBufferBuilder;

    // Further tests in arrow-integration-testing/tests/ipc_reader.rs

    #[test]
    fn test_eos() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("int32", DataType::Int32, false),
            Field::new("int64", DataType::Int64, false),
        ]));

        let input = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as _,
                Arc::new(Int64Array::from(vec![1, 2, 3])) as _,
            ],
        )
        .unwrap();

        let mut buf = Vec::with_capacity(1024);
        let mut s = StreamWriter::try_new(&mut buf, &schema).unwrap();
        s.write(&input).unwrap();
        s.finish().unwrap();
        drop(s);

        let buffer = Buffer::from_vec(buf);

        let mut b = buffer.slice_with_length(0, buffer.len() - 1);
        let mut decoder = StreamDecoder::new();
        let output = decoder.decode(&mut b).unwrap().unwrap();
        assert_eq!(output, input);
        assert_eq!(b.len(), 7); // 8 byte EOS truncated by 1 byte
        assert!(decoder.decode(&mut b).unwrap().is_none());

        let err = decoder.finish().unwrap_err().to_string();
        assert_eq!(err, "Ipc error: Unexpected End of Stream");
    }

    #[test]
    fn test_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("int32", DataType::Int32, false),
            Field::new("int64", DataType::Int64, false),
        ]));

        let mut buf = Vec::with_capacity(1024);
        let mut s = StreamWriter::try_new(&mut buf, &schema).unwrap();
        s.finish().unwrap();
        drop(s);

        let buffer = Buffer::from_vec(buf);

        let mut b = buffer.slice_with_length(0, buffer.len() - 1);
        let mut decoder = StreamDecoder::new();
        let output = decoder.decode(&mut b).unwrap();
        assert!(output.is_none());
        let decoded_schema = decoder.schema().unwrap();
        assert_eq!(schema, decoded_schema);

        let err = decoder.finish().unwrap_err().to_string();
        assert_eq!(err, "Ipc error: Unexpected End of Stream");
    }

    #[test]
    fn schema_and_record_batch_limits_reject_before_owned_decode() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )
        .unwrap();
        let mut bytes = Vec::new();
        let mut writer = StreamWriter::try_new(&mut bytes, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);

        let mut schema_limited = StreamDecoder::new().with_schema_limits(
            crate::convert::IpcSchemaLimits::new(0, usize::MAX, usize::MAX, usize::MAX),
        );
        let error = schema_limited
            .decode(&mut Buffer::from_vec(bytes.clone()))
            .unwrap_err();
        assert!(error.to_string().contains("configured 0-node limit"));
        assert!(schema_limited.schema().is_none());

        let mut row_limited = StreamDecoder::new().with_max_record_batch_rows(1);
        let error = row_limited
            .decode(&mut Buffer::from_vec(bytes))
            .unwrap_err();
        assert!(error.to_string().contains("2 rows beyond configured 1-row limit"));
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn bounded_stream_can_reject_compressed_batches_before_decode() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )
        .unwrap();
        let options = IpcWriteOptions::default()
            .try_with_compression(Some(crate::CompressionType::LZ4_FRAME))
            .unwrap();
        let mut bytes = Vec::new();
        let mut writer = StreamWriter::try_new_with_options(&mut bytes, &schema, options).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);

        let mut decoder = StreamDecoder::new().with_reject_compressed_record_batches(true);
        let error = decoder
            .decode(&mut Buffer::from_vec(bytes))
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("compressed IPC record batches are disabled"));
    }

    fn message_with_body_length(body_length: i64) -> Buffer {
        let mut builder = FlatBufferBuilder::new();
        let mut message = crate::MessageBuilder::new(&mut builder);
        message.add_version(crate::MetadataVersion::V5);
        message.add_bodyLength(body_length);
        let message = message.finish();
        builder.finish(message, None);
        let metadata = builder.finished_data();
        let mut bytes = Vec::with_capacity(4 + metadata.len());
        bytes.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
        bytes.extend_from_slice(metadata);
        Buffer::from_vec(bytes)
    }

    fn message_without_declared_header(header_type: MessageHeader) -> Buffer {
        let mut builder = FlatBufferBuilder::new();
        let mut message = crate::MessageBuilder::new(&mut builder);
        message.add_version(crate::MetadataVersion::V5);
        message.add_header_type(header_type);
        let message = message.finish();
        builder.finish(message, None);
        let metadata = builder.finished_data();
        let mut bytes = Vec::with_capacity(4 + metadata.len());
        bytes.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
        bytes.extend_from_slice(metadata);
        Buffer::from_vec(bytes)
    }

    #[test]
    fn missing_schema_union_header_returns_error_instead_of_panicking() {
        let mut malformed = message_without_declared_header(MessageHeader::Schema);
        let _error = StreamDecoder::new().decode(&mut malformed).unwrap_err();
    }

    #[test]
    fn message_limit_rejects_before_scratch_growth_for_contiguous_and_split_headers() {
        const LIMIT: usize = 16;
        let declared = (LIMIT as u32 + 1).to_le_bytes();

        let mut decoder = StreamDecoder::new().with_max_message_size(LIMIT);
        let error = decoder
            .decode(&mut Buffer::from_vec(declared.to_vec()))
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("17 exceeds configured 16-byte limit")
        );
        assert!(decoder.buf.is_empty());

        let mut decoder = StreamDecoder::new().with_max_message_size(LIMIT);
        for byte in declared.iter().take(3) {
            assert!(
                decoder
                    .decode(&mut Buffer::from_vec(vec![*byte]))
                    .unwrap()
                    .is_none()
            );
        }
        let error = decoder
            .decode(&mut Buffer::from_vec(vec![declared[3]]))
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("17 exceeds configured 16-byte limit")
        );
        assert!(decoder.buf.is_empty());

        let mut decoder = StreamDecoder::new().with_max_message_size(LIMIT);
        assert!(
            decoder
                .decode(&mut Buffer::from_vec((LIMIT as u32).to_le_bytes().to_vec()))
                .unwrap()
                .is_none()
        );
        assert!(decoder.buf.is_empty());
    }

    #[test]
    fn body_limit_rejects_declared_oversize_and_negative_lengths_before_body_buffering() {
        let mut oversized = message_with_body_length(17);
        let mut decoder = StreamDecoder::new().with_max_body_size(16);
        let error = decoder.decode(&mut oversized).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("17 exceeds configured 16-byte limit")
        );
        assert!(decoder.buf.is_empty());

        let mut negative = message_with_body_length(-1);
        let mut decoder = StreamDecoder::new().with_max_body_size(16);
        let error = decoder.decode(&mut negative).unwrap_err();
        assert!(error.to_string().contains("body length -1 is negative"));
        assert!(decoder.buf.is_empty());

        let mut exact = message_with_body_length(0);
        let mut decoder = StreamDecoder::new().with_max_body_size(1);
        assert!(decoder.decode(&mut exact).unwrap().is_none());
        assert!(decoder.buf.is_empty());
    }

    #[test]
    fn maximum_split_body_has_the_budgeted_allocator_capacity() {
        const BODY_BYTES: usize = 25 * 1024 * 1024;
        const FIRST_SPLIT_BYTES: usize = 16 * 1024 * 1024;
        const EXPECTED_CAPACITY_BYTES: usize = 32 * 1024 * 1024;

        // StreamDecoder uses this exact MutableBuffer growth path when one logical body spans
        // response chunks. The separate body-limit test proves the declared logical ceiling is
        // checked before growth; this pressure test freezes the allocator-capacity term callers
        // must budget for a maximum admitted split body.
        let mut scratch = MutableBuffer::new(0);
        scratch.resize(FIRST_SPLIT_BYTES, 0);
        scratch.resize(BODY_BYTES - 1, 0);
        assert_eq!(scratch.len(), BODY_BYTES - 1);
        assert_eq!(scratch.capacity(), EXPECTED_CAPACITY_BYTES);
    }

    #[test]
    fn test_read_ree_dict_record_batches_from_buffer() {
        let schema = Schema::new(vec![Field::new(
            "test1",
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends".to_string(), DataType::Int32, false)),
                #[allow(deprecated)]
                Arc::new(Field::new_dict(
                    "values".to_string(),
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    true,
                    0,
                    false,
                )),
            ),
            true,
        )]);
        let batch = RecordBatch::try_new(
            schema.clone().into(),
            vec![Arc::new(
                RunArray::try_new(
                    &Int32Array::from(vec![1, 2, 3]),
                    &vec![Some("a"), None, Some("a")]
                        .into_iter()
                        .collect::<DictionaryArray<Int32Type>>(),
                )
                .expect("Failed to create RunArray"),
            )],
        )
        .expect("Failed to create RecordBatch");

        let mut buffer = vec![];
        {
            let mut writer = StreamWriter::try_new_with_options(
                &mut buffer,
                &schema,
                IpcWriteOptions::default(),
            )
            .expect("Failed to create StreamWriter");
            writer.write(&batch).expect("Failed to write RecordBatch");
            writer.finish().expect("Failed to finish StreamWriter");
        }

        let mut decoder = StreamDecoder::new();
        let buf = &mut Buffer::from(buffer.as_slice());
        while let Some(batch) = decoder
            .decode(buf)
            .map_err(|e| {
                ArrowError::ExternalError(format!("Failed to decode record batch: {e}").into())
            })
            .expect("Failed to decode record batch")
        {
            assert_eq!(batch, batch);
        }

        decoder.finish().expect("Failed to finish decoder");

        let mut decoder = StreamDecoder::new().with_reject_dictionary_batches(true);
        let error = decoder
            .decode(&mut Buffer::from(buffer.as_slice()))
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("dictionary batches are disabled")
        );
        assert!(decoder.dictionaries.is_empty());
    }
}
