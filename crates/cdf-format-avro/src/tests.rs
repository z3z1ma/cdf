use std::{collections::BTreeMap, io::Cursor, sync::Arc};

use apache_avro::{Schema as ApacheAvroSchema, Writer as ApacheAvroWriter, types::Record};
use arrow_array::{
    ArrayRef, Date32Array, Decimal128Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray, UnionArray,
};
use arrow_avro::{
    compression::CompressionCodec,
    reader::read_header_info,
    schema::{AvroSchema, FingerprintStrategy, SCHEMA_METADATA_KEY},
    writer::{
        AvroWriter, WriterBuilder,
        format::{AvroOcfFormat, AvroSoeFormat},
    },
};
use arrow_buffer::Buffer;
use arrow_schema::{DataType, Field, Schema, TimeUnit, UnionFields, UnionMode};
use cdf_kernel::{PartitionId, ResourceId};
use cdf_memory::{DeterministicMemoryCoordinator, MemoryCoordinator};
use cdf_runtime::{
    ByteSource, DecodePlanningRequest, DecodeSchemaPlan, FormatDiscoveryKind,
    FormatDiscoveryRequest, FormatDriver, FormatProbe, MemoryByteSource, PhysicalDecodeRequest,
    RunCancellation,
};
use futures_util::TryStreamExt;

use super::*;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn batch(schema: Arc<Schema>, start: i64, rows: usize) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(
                start..start + i64::try_from(rows).unwrap(),
            )) as ArrayRef,
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(|row| format!("name-{start}-{row}")),
            )) as ArrayRef,
        ],
    )
    .unwrap()
}

fn ocf_fixture() -> (Arc<Schema>, Vec<u8>) {
    let schema = schema();
    let mut writer = AvroWriter::new(Vec::new(), schema.as_ref().clone()).unwrap();
    for index in 0..16 {
        writer
            .write(&batch(Arc::clone(&schema), index * 128, 128))
            .unwrap();
    }
    writer.finish().unwrap();
    let bytes = writer.into_inner();
    (schema, bytes)
}

fn ocf_fixture_with_codec(codec: Option<CompressionCodec>, rows: usize) -> (Arc<Schema>, Vec<u8>) {
    let schema = schema();
    let mut writer = WriterBuilder::new(schema.as_ref().clone())
        .with_compression(codec)
        .build::<_, AvroOcfFormat>(Vec::new())
        .unwrap();
    writer.write(&batch(Arc::clone(&schema), 0, rows)).unwrap();
    writer.finish().unwrap();
    (schema, writer.into_inner())
}

fn decode_ocf(
    bytes: Vec<u8>,
    projection: Option<Vec<String>>,
    options: serde_json::Value,
) -> Result<Vec<AccountedPhysicalBatch>> {
    decode_ocf_in_order(bytes, projection, options, false)
}

fn decode_ocf_in_order(
    bytes: Vec<u8>,
    projection: Option<Vec<String>>,
    options: serde_json::Value,
    reverse_units: bool,
) -> Result<Vec<AccountedPhysicalBatch>> {
    let memory = memory(1024 * 1024 * 1024);
    let source: Arc<dyn ByteSource> = Arc::new(futures_executor::block_on(
        MemoryByteSource::from_bytes("memory:fixture.avro", bytes, Arc::clone(&memory)),
    )?);
    let driver = AvroOcfFormatDriver::new()?;
    let observation = futures_executor::block_on(driver.discover(
        Arc::clone(&source),
        FormatDiscoveryRequest {
            options: options.clone(),
            discovery_kind: FormatDiscoveryKind::FormatMetadata,
            maximum_bytes: 1024 * 1024,
            maximum_records: 0,
            memory: Arc::clone(&memory),
            cancellation: RunCancellation::default(),
        },
    ))?;
    let session = futures_executor::block_on(driver.prepare_decode(
        source,
        DecodePlanningRequest {
            options,
            projection: projection.clone(),
            predicates: Vec::new(),
            target_batch_rows: 1024,
            target_batch_bytes: 8 * 1024 * 1024,
            cancellation: RunCancellation::default(),
        },
    ))?;
    let mut output = Vec::new();
    let mut units = session.units().to_vec();
    if reverse_units {
        units.reverse();
    }
    for unit in &units {
        let stream = futures_executor::block_on(session.decode(PhysicalDecodeRequest {
            unit: unit.clone(),
            resource_id: ResourceId::new("fixture.avro")?,
            partition_id: PartitionId::new("file-000001")?,
            batch_id_prefix: "fixture".to_owned(),
            schema: DecodeSchemaPlan::verified_physical(Arc::clone(&observation.arrow_schema)),
            source_position: None,
            projection: projection.clone(),
            predicates: Vec::new(),
            target_batch_rows: 1024,
            target_batch_bytes: 8 * 1024 * 1024,
            memory: Arc::clone(&memory),
            cancellation: RunCancellation::default(),
        }))?;
        output.extend(futures_executor::block_on(stream.try_collect::<Vec<_>>())?);
    }
    Ok(output)
}

fn decode_single_object(
    bytes: Vec<u8>,
    writer_schema: serde_json::Value,
    maximum_record_bytes: u64,
    projection: Option<Vec<String>>,
) -> Result<Vec<AccountedPhysicalBatch>> {
    let memory = memory(256 * 1024 * 1024);
    let source: Arc<dyn ByteSource> = Arc::new(futures_executor::block_on(
        MemoryByteSource::from_bytes("memory:fixture.avrosoe", bytes, Arc::clone(&memory)),
    )?);
    let driver = AvroSingleObjectFormatDriver::new()?;
    let options = serde_json::json!({
        "writer_schema": writer_schema,
        "maximum_record_bytes": maximum_record_bytes
    });
    let observation = futures_executor::block_on(driver.discover(
        Arc::clone(&source),
        FormatDiscoveryRequest {
            options: options.clone(),
            discovery_kind: FormatDiscoveryKind::FormatMetadata,
            maximum_bytes: 1,
            maximum_records: 0,
            memory: Arc::clone(&memory),
            cancellation: RunCancellation::default(),
        },
    ))?;
    let session = futures_executor::block_on(driver.prepare_decode(
        source,
        DecodePlanningRequest {
            options,
            projection: projection.clone(),
            predicates: Vec::new(),
            target_batch_rows: 1024,
            target_batch_bytes: 1024,
            cancellation: RunCancellation::default(),
        },
    ))?;
    let stream = futures_executor::block_on(session.decode(PhysicalDecodeRequest {
        unit: session.units()[0].clone(),
        resource_id: ResourceId::new("fixture.avrosoe")?,
        partition_id: PartitionId::new("file-000001")?,
        batch_id_prefix: "fixture".to_owned(),
        schema: DecodeSchemaPlan::verified_physical(observation.arrow_schema),
        source_position: None,
        projection,
        predicates: Vec::new(),
        target_batch_rows: 1024,
        target_batch_bytes: 1024,
        memory,
        cancellation: RunCancellation::default(),
    }))?;
    futures_executor::block_on(stream.try_collect::<Vec<_>>())
}

fn single_object_fixture(rows: usize) -> (serde_json::Value, Arc<Schema>, Vec<u8>) {
    let avro_schema = serde_json::json!({
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"}
        ]
    });
    let mut metadata = std::collections::HashMap::new();
    metadata.insert(SCHEMA_METADATA_KEY.to_owned(), avro_schema.to_string());
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ],
        metadata,
    ));
    let avro = AvroSchema::new(avro_schema.to_string());
    let fingerprint = avro.fingerprint(FingerprintAlgorithm::Rabin).unwrap();
    let mut writer = WriterBuilder::new(schema.as_ref().clone())
        .with_fingerprint_strategy(FingerprintStrategy::from(fingerprint))
        .build::<_, AvroSoeFormat>(Vec::new())
        .unwrap();
    writer.write(&batch(Arc::clone(&schema), 0, rows)).unwrap();
    writer.finish().unwrap();
    (avro_schema, schema, writer.into_inner())
}

fn memory(bytes: u64) -> Arc<dyn MemoryCoordinator> {
    Arc::new(DeterministicMemoryCoordinator::new(bytes, BTreeMap::new()).unwrap())
}

#[test]
fn transport_errors_preserve_classification_through_avro_adapters() {
    let expected = CdfError::rate_limited("provider throttle", Some(750));
    assert_eq!(avro_error(cdf_to_avro(expected.clone())), expected);

    let arrow_error: arrow_schema::ArrowError = cdf_to_avro(expected.clone()).into();
    assert_eq!(avro_arrow_error(arrow_error), expected);
}

#[test]
fn drivers_publish_disjoint_strong_signatures_and_canonical_knobs() {
    let ocf = AvroOcfFormatDriver::new().unwrap();
    let soe = AvroSingleObjectFormatDriver::new().unwrap();

    assert_eq!(
        ocf.detect(&FormatProbe {
            extension: Some("avro".to_owned()),
            mime_type: None,
            prefix: OCF_MAGIC.to_vec(),
            suffix: Vec::new(),
        })
        .unwrap()
        .confidence,
        FormatDetectionConfidence::Strong
    );
    assert_eq!(
        soe.detect(&FormatProbe {
            extension: None,
            mime_type: Some("avro/binary".to_owned()),
            prefix: SOE_MAGIC.to_vec(),
            suffix: Vec::new(),
        })
        .unwrap()
        .confidence,
        FormatDetectionConfidence::Strong
    );

    let options = ocf.canonical_options(serde_json::json!({})).unwrap();
    assert_eq!(
        options["maximum_header_bytes"],
        DEFAULT_MAXIMUM_HEADER_BYTES
    );
    assert_eq!(options["maximum_block_bytes"], DEFAULT_MAXIMUM_BLOCK_BYTES);
    assert_eq!(
        options["maximum_decoded_block_bytes"],
        DEFAULT_MAXIMUM_DECODED_BLOCK_BYTES
    );
    assert_eq!(
        options["maximum_block_records"],
        DEFAULT_MAXIMUM_BLOCK_RECORDS
    );
    assert_eq!(options["maximum_blocks"], DEFAULT_MAXIMUM_BLOCKS);
    assert!(
        soe.canonical_options(serde_json::json!({
            "maximum_record_bytes": 1024
        }))
        .is_err()
    );
}

#[test]
fn ocf_discovers_embedded_schema_and_decodes_exact_block_units() {
    let (expected_schema, bytes) = ocf_fixture();
    let memory = memory(128 * 1024 * 1024);
    let source: Arc<dyn ByteSource> = Arc::new(
        futures_executor::block_on(MemoryByteSource::from_bytes(
            "memory:fixture.avro",
            bytes,
            Arc::clone(&memory),
        ))
        .unwrap(),
    );
    let driver = AvroOcfFormatDriver::new().unwrap();
    let options = serde_json::json!({
        "maximum_block_bytes": 4 * 1024 * 1024,
        "maximum_decoded_block_bytes": 64 * 1024 * 1024
    });
    let observation = futures_executor::block_on(driver.discover(
        Arc::clone(&source),
        FormatDiscoveryRequest {
            options: options.clone(),
            discovery_kind: FormatDiscoveryKind::FormatMetadata,
            maximum_bytes: 1024 * 1024,
            maximum_records: 0,
            memory: Arc::clone(&memory),
            cancellation: RunCancellation::default(),
        },
    ))
    .unwrap();
    assert_eq!(observation.arrow_schema.fields(), expected_schema.fields());
    assert!(observation.sampled_bytes <= source.identity().size_bytes.unwrap().min(16 * 1024));
    assert!(observation.evidence.contains_key("avro.writer_fingerprint"));

    let session = futures_executor::block_on(driver.prepare_decode(
        Arc::clone(&source),
        DecodePlanningRequest {
            options,
            projection: Some(vec!["id".to_owned()]),
            predicates: Vec::new(),
            target_batch_rows: 257,
            target_batch_bytes: 1024 * 1024,
            cancellation: RunCancellation::default(),
        },
    ))
    .unwrap();
    assert_eq!(session.units().len(), 16);
    assert!(session.units().iter().all(|unit| unit.extent.is_some()));
    for pair in session.units().windows(2) {
        let previous = pair[0].extent.unwrap();
        let next = pair[1].extent.unwrap();
        assert_eq!(
            previous.start + previous.length,
            next.start + OCF_SYNC_MARKER_BYTES
        );
    }

    let mut rows = 0_u64;
    let mut ids = Vec::new();
    for unit in session.units() {
        let stream = futures_executor::block_on(session.decode(PhysicalDecodeRequest {
            unit: unit.clone(),
            resource_id: ResourceId::new("fixture.avro").unwrap(),
            partition_id: PartitionId::new("file-000001").unwrap(),
            batch_id_prefix: "fixture".to_owned(),
            schema: DecodeSchemaPlan::verified_physical(Arc::clone(&observation.arrow_schema)),
            source_position: None,
            projection: Some(vec!["id".to_owned()]),
            predicates: Vec::new(),
            target_batch_rows: 257,
            target_batch_bytes: 1024 * 1024,
            memory: Arc::clone(&memory),
            cancellation: RunCancellation::default(),
        }))
        .unwrap();
        let batches = futures_executor::block_on(stream.try_collect::<Vec<_>>())
            .unwrap_or_else(|error| panic!("{}: {error:?}", unit.unit_id));
        for batch in batches {
            let record = batch.batch().record_batch().unwrap();
            rows += u64::try_from(record.num_rows()).unwrap();
            ids.extend(
                record
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied(),
            );
        }
    }
    assert_eq!(rows, 2048);
    assert_eq!(ids, (0_i64..2048).collect::<Vec<_>>());
}

#[test]
fn ocf_discovery_never_crosses_its_observation_budget() {
    let (_, bytes) = ocf_fixture();
    let memory = memory(128 * 1024 * 1024);
    let source: Arc<dyn ByteSource> = Arc::new(
        futures_executor::block_on(MemoryByteSource::from_bytes(
            "memory:budget.avro",
            bytes,
            Arc::clone(&memory),
        ))
        .unwrap(),
    );
    let error = futures_executor::block_on(AvroOcfFormatDriver::new().unwrap().discover(
        source,
        FormatDiscoveryRequest {
            options: serde_json::json!({}),
            discovery_kind: FormatDiscoveryKind::FormatMetadata,
            maximum_bytes: 4,
            maximum_records: 0,
            memory,
            cancellation: RunCancellation::default(),
        },
    ))
    .unwrap_err();
    assert!(
        error.message.contains("4-byte observation budget"),
        "{error}"
    );
}

#[test]
fn ocf_batch_identity_and_contents_are_scheduler_order_invariant() {
    let (_, bytes) = ocf_fixture();
    let options = serde_json::json!({
        "maximum_block_bytes": 4 * 1024 * 1024
    });
    let canonical = decode_ocf_in_order(bytes.clone(), None, options.clone(), false).unwrap();
    let reverse = decode_ocf_in_order(bytes, None, options, true).unwrap();

    let signature = |batches: Vec<AccountedPhysicalBatch>| {
        let mut batches = batches
            .into_iter()
            .map(|batch| {
                let id = batch.batch().header.batch_id.as_str().to_owned();
                let values = batch
                    .batch()
                    .record_batch()
                    .unwrap()
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec();
                (id, values)
            })
            .collect::<Vec<_>>();
        batches.sort_by(|left, right| left.0.cmp(&right.0));
        batches
    };
    assert_eq!(signature(canonical), signature(reverse));
}

#[test]
fn single_object_requires_explicit_matching_fingerprint_and_one_datum() {
    let (writer_schema, expected_schema, bytes) = single_object_fixture(1);
    let memory = memory(64 * 1024 * 1024);
    let source: Arc<dyn ByteSource> = Arc::new(
        futures_executor::block_on(MemoryByteSource::from_bytes(
            "memory:fixture.avrosoe",
            bytes.clone(),
            Arc::clone(&memory),
        ))
        .unwrap(),
    );
    let driver = AvroSingleObjectFormatDriver::new().unwrap();
    let options = serde_json::json!({
        "writer_schema": writer_schema,
        "maximum_record_bytes": 1024 * 1024
    });
    let observation = futures_executor::block_on(driver.discover(
        Arc::clone(&source),
        FormatDiscoveryRequest {
            options: options.clone(),
            discovery_kind: FormatDiscoveryKind::FormatMetadata,
            maximum_bytes: 1,
            maximum_records: 0,
            memory: Arc::clone(&memory),
            cancellation: RunCancellation::default(),
        },
    ))
    .unwrap();
    assert_eq!(observation.sampled_bytes, 0);
    assert_eq!(observation.arrow_schema.fields(), expected_schema.fields());

    let session = futures_executor::block_on(driver.prepare_decode(
        source,
        DecodePlanningRequest {
            options: options.clone(),
            projection: None,
            predicates: Vec::new(),
            target_batch_rows: 5,
            target_batch_bytes: 1024 * 1024,
            cancellation: RunCancellation::default(),
        },
    ))
    .unwrap();
    let stream = futures_executor::block_on(session.decode(PhysicalDecodeRequest {
        unit: session.units()[0].clone(),
        resource_id: ResourceId::new("fixture.avrosoe").unwrap(),
        partition_id: PartitionId::new("file-000001").unwrap(),
        batch_id_prefix: "fixture".to_owned(),
        schema: DecodeSchemaPlan::verified_physical(Arc::clone(&observation.arrow_schema)),
        source_position: None,
        projection: None,
        predicates: Vec::new(),
        target_batch_rows: 5,
        target_batch_bytes: 1024 * 1024,
        memory: Arc::clone(&memory),
        cancellation: RunCancellation::default(),
    }))
    .unwrap();
    let batches = futures_executor::block_on(stream.try_collect::<Vec<_>>()).unwrap();
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.batch().header.row_count)
            .sum::<u64>(),
        1
    );
    assert_eq!(batches.len(), 1);

    let mut corrupted = bytes;
    corrupted[2] ^= 0xff;
    let corrupt_source: Arc<dyn ByteSource> = Arc::new(
        futures_executor::block_on(MemoryByteSource::from_bytes(
            "memory:corrupt.avrosoe",
            corrupted,
            Arc::clone(&memory),
        ))
        .unwrap(),
    );
    let corrupt = futures_executor::block_on(driver.prepare_decode(
        corrupt_source,
        DecodePlanningRequest {
            options,
            projection: None,
            predicates: Vec::new(),
            target_batch_rows: 5,
            target_batch_bytes: 1024 * 1024,
            cancellation: RunCancellation::default(),
        },
    ))
    .unwrap();
    let stream = futures_executor::block_on(corrupt.decode(PhysicalDecodeRequest {
        unit: corrupt.units()[0].clone(),
        resource_id: ResourceId::new("corrupt.avrosoe").unwrap(),
        partition_id: PartitionId::new("file-000001").unwrap(),
        batch_id_prefix: "corrupt".to_owned(),
        schema: DecodeSchemaPlan::verified_physical(observation.arrow_schema),
        source_position: None,
        projection: None,
        predicates: Vec::new(),
        target_batch_rows: 5,
        target_batch_bytes: 1024 * 1024,
        memory,
        cancellation: RunCancellation::default(),
    }))
    .unwrap();
    let error = futures_executor::block_on(stream.try_collect::<Vec<_>>()).unwrap_err();
    assert!(error.message.contains("fingerprint"));
}

#[test]
fn ocf_decodes_every_native_block_codec() {
    for codec in [
        None,
        Some(CompressionCodec::Deflate),
        Some(CompressionCodec::Snappy),
        Some(CompressionCodec::ZStandard),
        Some(CompressionCodec::Bzip2),
        Some(CompressionCodec::Xz),
    ] {
        let (_, bytes) = ocf_fixture_with_codec(codec, 4096);
        let batches = decode_ocf(
            bytes,
            None,
            serde_json::json!({
                "maximum_block_bytes": 8 * 1024 * 1024
            }),
        )
        .unwrap_or_else(|error| panic!("{codec:?}: {error}"));
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.batch().header.row_count)
                .sum::<u64>(),
            4096,
            "{codec:?}"
        );
    }
}

#[test]
fn ocf_decodes_apache_avro_reference_output() {
    let apache_schema = ApacheAvroSchema::parse_str(
        r#"{
            "type":"record",
            "name":"ReferenceRow",
            "fields":[
                {"name":"id","type":"long"},
                {"name":"name","type":"string"}
            ]
        }"#,
    )
    .unwrap();
    let mut writer = ApacheAvroWriter::new(&apache_schema, Vec::new());
    for id in 0_i64..1024 {
        let mut record = Record::new(&apache_schema).unwrap();
        record.put("id", id);
        record.put("name", format!("reference-{id}"));
        writer.append(record).unwrap();
    }
    writer.flush().unwrap();
    let bytes = writer.into_inner().unwrap();

    let batches = decode_ocf(
        bytes,
        None,
        serde_json::json!({
            "maximum_block_bytes": 8 * 1024 * 1024
        }),
    )
    .unwrap();
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.batch().header.row_count)
            .sum::<u64>(),
        1024
    );
    assert_eq!(
        batches[0].batch().record_batch().unwrap().schema().fields(),
        schema().fields()
    );
}

#[test]
fn ocf_preserves_logical_types_and_dense_union_branches() {
    let union_fields = UnionFields::try_new(
        vec![2, 5],
        vec![
            Field::new("text", DataType::Utf8, false),
            Field::new("number", DataType::Int32, false),
        ],
    )
    .unwrap();
    let union = UnionArray::try_new(
        union_fields.clone(),
        Buffer::from_slice_ref([2_i8, 5, 2, 5]).into(),
        Some(Buffer::from_slice_ref([0_i32, 0, 1, 1]).into()),
        vec![
            Arc::new(StringArray::from(vec!["one", "three"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![2, 4])) as ArrayRef,
        ],
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("event_date", DataType::Date32, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("amount", DataType::Decimal128(18, 4), false),
        Field::new(
            "variant",
            DataType::Union(union_fields, UnionMode::Dense),
            false,
        ),
    ]));
    let input = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Date32Array::from(vec![0, 1, 365, -1])) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(vec![1, 2, 3, 4])) as ArrayRef,
            Arc::new(
                Decimal128Array::from(vec![10000, -25000, 0, 99999])
                    .with_precision_and_scale(18, 4)
                    .unwrap(),
            ) as ArrayRef,
            Arc::new(union) as ArrayRef,
        ],
    )
    .unwrap();
    let mut writer = AvroWriter::new(Vec::new(), schema.as_ref().clone()).unwrap();
    writer.write(&input).unwrap();
    writer.finish().unwrap();
    let batches = decode_ocf(
        writer.into_inner(),
        None,
        serde_json::json!({
            "maximum_block_bytes": 8 * 1024 * 1024
        }),
    )
    .unwrap();
    let output = batches[0].batch().record_batch().unwrap();
    assert_eq!(output.schema().field(0).data_type(), &DataType::Date32);
    assert_eq!(
        output.schema().field(1).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(
        output.schema().field(2).data_type(),
        &DataType::Decimal128(18, 4)
    );
    let decoded_union = output
        .column(3)
        .as_any()
        .downcast_ref::<UnionArray>()
        .unwrap();
    assert_eq!(
        decoded_union.type_ids().iter().copied().collect::<Vec<_>>(),
        vec![0, 1, 0, 1]
    );
    let output_schema = output.schema();
    let DataType::Union(decoded_fields, UnionMode::Dense) = output_schema.field(3).data_type()
    else {
        panic!("general Avro union must remain an Arrow dense union")
    };
    assert_eq!(
        decoded_fields
            .iter()
            .map(|(type_id, field)| (type_id, field.name().as_str()))
            .collect::<Vec<_>>(),
        vec![(0, "string"), (1, "int")]
    );
}

#[test]
fn ocf_rejects_sync_corruption_and_oversized_blocks_before_emitting_rows() {
    let (_, bytes) = ocf_fixture_with_codec(Some(CompressionCodec::Snappy), 4096);
    let header = read_header_info(Cursor::new(bytes.as_slice())).unwrap();
    assert!(bytes.len() > usize::try_from(header.header_len()).unwrap() + 16);

    let mut corrupt = bytes.clone();
    *corrupt.last_mut().unwrap() ^= 0xff;
    let sync_error = decode_ocf(
        corrupt,
        None,
        serde_json::json!({
            "maximum_block_bytes": 8 * 1024 * 1024
        }),
    )
    .unwrap_err();
    assert!(sync_error.message.contains("sync marker"), "{sync_error}");

    let size_error = decode_ocf(
        bytes,
        None,
        serde_json::json!({
            "maximum_block_bytes": 64
        }),
    )
    .unwrap_err();
    assert!(
        size_error.message.contains("configured 64-byte maximum"),
        "{size_error}"
    );

    let (_, mut trailing_header) = ocf_fixture();
    trailing_header.push(0x80);
    let trailing_error = decode_ocf(
        trailing_header,
        None,
        serde_json::json!({
            "maximum_block_bytes": 8 * 1024 * 1024
        }),
    )
    .unwrap_err();
    assert!(
        trailing_error
            .message
            .contains("ended inside a block-header long"),
        "{trailing_error}"
    );

    let (_, many_blocks) = ocf_fixture();
    let block_count_error = decode_ocf(
        many_blocks,
        None,
        serde_json::json!({
            "maximum_block_bytes": 8 * 1024 * 1024,
            "maximum_blocks": 15
        }),
    )
    .unwrap_err();
    assert!(
        block_count_error
            .message
            .contains("configured 15 block maximum"),
        "{block_count_error}"
    );

    let (_, output_too_large) = ocf_fixture_with_codec(None, 128);
    let output_error = decode_ocf(
        output_too_large,
        None,
        serde_json::json!({
            "maximum_block_bytes": 8 * 1024 * 1024,
            "maximum_decoded_block_bytes": 64
        }),
    )
    .unwrap_err();
    assert!(
        output_error
            .message
            .contains("decoded Arrow bytes above the configured 64-byte maximum"),
        "{output_error}"
    );
}

#[test]
fn ocf_late_record_failure_never_publishes_an_earlier_batch() {
    let apache_schema = ApacheAvroSchema::parse_str(
        r#"{
            "type":"record",
            "name":"LateFailure",
            "fields":[{"name":"value","type":"long"}]
        }"#,
    )
    .unwrap();
    let mut writer = ApacheAvroWriter::new(&apache_schema, Vec::new());
    for value in 0_i64..2048 {
        let mut record = Record::new(&apache_schema).unwrap();
        record.put("value", value);
        writer.append(record).unwrap();
    }
    writer.flush().unwrap();
    let mut bytes = writer.into_inner().unwrap();
    let header = read_header_info(Cursor::new(bytes.as_slice())).unwrap();
    let header_len = usize::try_from(header.header_len()).unwrap();
    let (_, count_bytes) = decode_avro_long(&bytes[header_len..]).unwrap();
    let (encoded_bytes, size_bytes) = decode_avro_long(&bytes[header_len + count_bytes..]).unwrap();
    let data_start = header_len + count_bytes + size_bytes;
    let data_end = data_start + usize::try_from(encoded_bytes).unwrap();
    bytes[data_end - 1] = 0x80;

    let error = decode_ocf(
        bytes,
        None,
        serde_json::json!({
            "maximum_block_bytes": 8 * 1024 * 1024
        }),
    )
    .unwrap_err();
    assert!(error.message.contains("decode Avro"), "{error}");
}

#[test]
fn single_object_rejects_truncated_frames() {
    let (writer_schema, _, bytes) = single_object_fixture(1);
    let mut truncated = bytes;
    truncated.pop();
    let truncated = decode_single_object(truncated, writer_schema, 1024 * 1024, None).unwrap_err();
    assert!(
        truncated.message.contains("decode Avro")
            || truncated.message.contains("ended inside its encoded datum"),
        "{truncated}"
    );
}

#[test]
fn single_object_projection_keeps_full_schema_authority_and_hashes_decoded_schema() {
    let (writer_schema, _, bytes) = single_object_fixture(1);
    let batches = decode_single_object(
        bytes,
        writer_schema,
        1024 * 1024,
        Some(vec!["id".to_owned()]),
    )
    .unwrap();
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.batch().header.row_count)
            .sum::<u64>(),
        1
    );
    for batch in batches {
        let record = batch.batch().record_batch().unwrap();
        assert_eq!(record.num_columns(), 1);
        assert_eq!(record.schema().field(0).name(), "id");
        assert_eq!(
            batch.batch().header.observed_schema_hash,
            cdf_kernel::canonical_arrow_schema_hash(record.schema().as_ref()).unwrap()
        );
    }
}

#[test]
fn single_object_rejects_concatenated_datums_without_message_boundaries() {
    let (writer_schema, _, bytes) = single_object_fixture(2);
    let error = decode_single_object(bytes, writer_schema, 1024 * 1024, None).unwrap_err();
    assert!(error.message.contains("multiple encoded datums"), "{error}");
}

#[test]
fn single_object_decodes_a_valid_record_larger_than_one_input_chunk() {
    let avro_schema = serde_json::json!({
        "type": "record",
        "name": "LargeUser",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"}
        ]
    });
    let mut metadata = std::collections::HashMap::new();
    metadata.insert(SCHEMA_METADATA_KEY.to_owned(), avro_schema.to_string());
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ],
        metadata,
    ));
    let payload = "x".repeat(16 * 1024 * 1024 + 1);
    let input = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![7])) as ArrayRef,
            Arc::new(StringArray::from(vec![payload.as_str()])) as ArrayRef,
        ],
    )
    .unwrap();
    let avro = AvroSchema::new(avro_schema.to_string());
    let fingerprint = avro.fingerprint(FingerprintAlgorithm::Rabin).unwrap();
    let mut writer = WriterBuilder::new(schema.as_ref().clone())
        .with_fingerprint_strategy(FingerprintStrategy::from(fingerprint))
        .build::<_, AvroSoeFormat>(Vec::new())
        .unwrap();
    writer.write(&input).unwrap();
    writer.finish().unwrap();
    let batches =
        decode_single_object(writer.into_inner(), avro_schema, 32 * 1024 * 1024, None).unwrap();
    let output = batches[0].batch().record_batch().unwrap();
    assert_eq!(output.num_rows(), 1);
    assert_eq!(
        output
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .len(),
        payload.len()
    );
}
