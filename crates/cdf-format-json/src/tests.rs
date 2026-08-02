use std::{
    collections::BTreeMap,
    fmt::Write as _,
    io::Cursor,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use arrow_array::{Array, BinaryArray, Int64Array, StringArray};
use arrow_json::reader::infer_json_schema;
use arrow_schema::{DataType, Field, Schema};
use cdf_kernel::{CdfError, PartitionId, ResourceId, Result, physical_type};
use cdf_memory::{
    AccountedBytes, ConsumerKey, DeterministicMemoryCoordinator, MemoryClass, MemoryCoordinator,
    ReservationRequest, reserve_blocking,
};
use cdf_runtime::{
    AccountedByteStream, AccountedPhysicalBatch, BoundedFormatRequest, ByteSource,
    DecodeSchemaPlan, DecodeUnitPlan, FormatDiscoveryKind, FormatDiscoveryRequest, FormatDriver,
    MemoryByteSource, PhysicalDecodeRequest, ReadOptions, SequentialReadRequest,
    decode_bounded_format,
};
use futures_util::{FutureExt, StreamExt, TryStreamExt, stream};

use crate::decode::{decode_ndjson_stream, next_decode_window_target};
use crate::discovery::infer_full_content_json_schema;
use crate::framing::{JsonFrameRequest, frame_json_document};
use crate::options::{
    DEFAULT_MAXIMUM_RECORD_BYTES, MAXIMUM_CONFIGURED_RECORD_BYTES, MAXIMUM_JSON_NESTING_DEPTH,
};
use crate::{JsonDocumentFormatDriver, NdjsonFormatDriver, select_bounded_json_records};

fn frame_with_depth(
    input: &[u8],
    maximum_records: Option<u64>,
    maximum_nesting_depth: usize,
) -> Result<(Vec<u8>, u64, u64)> {
    let coordinator =
        Arc::new(DeterministicMemoryCoordinator::new(1024 * 1024, BTreeMap::new()).unwrap());
    let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
    let chunks = input
        .iter()
        .enumerate()
        .map(|(index, byte)| {
            let lease = reserve_blocking(
                Arc::clone(&memory),
                &ReservationRequest::new(
                    ConsumerKey::new(format!("json-test-input-{index}"), MemoryClass::Source)
                        .unwrap(),
                    1,
                )
                .unwrap(),
            )
            .unwrap();
            Ok(AccountedBytes::new(bytes::Bytes::copy_from_slice(&[*byte]), lease).unwrap())
        })
        .collect::<Vec<Result<_>>>();
    let counter = Arc::new(AtomicU64::new(0));
    let mut framed = frame_json_document(
        Box::pin(stream::iter(chunks)),
        JsonFrameRequest {
            maximum_input_bytes: u64::try_from(input.len()).unwrap(),
            maximum_records,
            preferred_output_chunk_bytes: 7,
            maximum_record_bytes: DEFAULT_MAXIMUM_RECORD_BYTES,
            maximum_nesting_depth,
            require_terminal_document: maximum_records.is_none(),
            input_counter: Arc::clone(&counter),
            memory,
            cancellation: cdf_runtime::RunCancellation::default(),
        },
    )?;
    let output = futures_executor::block_on(async move {
        let mut output = Vec::new();
        while let Some(chunk) = framed.try_next().await? {
            output.extend_from_slice(chunk.payload());
        }
        Result::<Vec<u8>>::Ok(output)
    })?;
    let sampled = counter.load(Ordering::Relaxed);
    let retained = coordinator.snapshot().current_bytes;
    Ok((output, sampled, retained))
}

fn frame(input: &[u8], maximum_records: Option<u64>) -> Result<(Vec<u8>, u64, u64)> {
    frame_with_depth(input, maximum_records, MAXIMUM_JSON_NESTING_DEPTH)
}

#[test]
fn json_document_framing_is_invariant_to_one_byte_chunks() {
    let input = br#" [ {"a":"},["}, {"b":{"c":[1,2]}} ] "#;
    let (output, sampled, retained) = frame(input, None).unwrap();

    assert_eq!(
        output,
        br#"{"a":"},["}
{"b":{"c":[1,2]}}
"#
    );
    assert_eq!(sampled, u64::try_from(input.len()).unwrap());
    assert_eq!(retained, 0);
}

#[test]
fn json_document_sampling_stops_after_complete_records() {
    let input = br#"[{"a":1},{"b":2},this-rest-is-not-json"#;
    let (output, sampled, retained) = frame(input, Some(2)).unwrap();

    assert_eq!(output, b"{\"a\":1}\n{\"b\":2}\n");
    assert_eq!(sampled, 16);
    assert_eq!(retained, 0);
}

#[test]
fn json_document_framing_rejects_trailing_commas() {
    let error = frame(br#"[{"a":1},]"#, None).unwrap_err();

    assert!(error.message.contains("trailing comma"), "{error}");
}

#[test]
fn json_document_framing_enforces_the_compiled_depth_limit() {
    let error = frame_with_depth(br#"[{"a":{"b":{"c":1}}}]"#, None, 2).unwrap_err();

    assert!(error.message.contains("2-level limit"), "{error}");
}

#[test]
fn malformed_json_document_corpus_fails_closed_without_retained_memory() {
    for (input, expected) in [
        (br#"[{"a":[1}}]"#.as_slice(), "mismatched delimiters"),
        (
            br#"[{"a":"unterminated}]"#.as_slice(),
            "ended inside a record",
        ),
        (br#"[1]"#.as_slice(), "array entries must be objects"),
        (br#"{"a":1} trailing"#.as_slice(), "trailing non-whitespace"),
        (br#"[{"a":1},"#.as_slice(), "ended after a comma"),
    ] {
        let error = frame(input, None).unwrap_err();
        assert!(error.message.contains(expected), "{input:?}: {error}");
    }
}

#[test]
fn codec_limits_are_explicit_canonical_plan_evidence() {
    let ndjson = NdjsonFormatDriver::new()
        .unwrap()
        .canonical_options(serde_json::json!({}))
        .unwrap();
    assert_eq!(
        ndjson,
        serde_json::json!({"maximum_record_bytes": DEFAULT_MAXIMUM_RECORD_BYTES})
    );
    let json = JsonDocumentFormatDriver::new()
        .unwrap()
        .canonical_options(serde_json::json!({"maximum_nesting_depth": 32}))
        .unwrap();
    assert_eq!(
        json,
        serde_json::json!({
            "maximum_nesting_depth": 32,
            "maximum_record_bytes": DEFAULT_MAXIMUM_RECORD_BYTES
        })
    );
    let error = NdjsonFormatDriver::new()
        .unwrap()
        .canonical_options(serde_json::json!({
            "maximum_record_bytes": MAXIMUM_CONFIGURED_RECORD_BYTES + 1
        }))
        .unwrap_err();
    assert!(error.message.contains("maximum_record_bytes"), "{error}");
}

#[test]
fn full_content_discovery_observes_late_fields_beyond_bounded_limits() {
    let coordinator =
        Arc::new(DeterministicMemoryCoordinator::new(256 * 1024 * 1024, BTreeMap::new()).unwrap());
    let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
    let ndjson = br#"{"id":1}
{"id":2,"late":"observed"}
"#
    .to_vec();
    let source: Arc<dyn ByteSource> = Arc::new(
        futures_executor::block_on(MemoryByteSource::from_bytes(
            "full-content-ndjson",
            ndjson.clone(),
            Arc::clone(&memory),
        ))
        .unwrap(),
    );
    let observation = futures_executor::block_on(NdjsonFormatDriver::new().unwrap().discover(
        Arc::clone(&source),
        FormatDiscoveryRequest {
            options: serde_json::json!({}),
            discovery_kind: FormatDiscoveryKind::FullContent,
            maximum_bytes: 8,
            maximum_records: 1,
            memory: Arc::clone(&memory),
            cancellation: cdf_runtime::RunCancellation::default(),
        },
    ))
    .unwrap();
    assert_eq!(observation.sampled_bytes, ndjson.len() as u64);
    assert_eq!(observation.sampled_records, 2);
    assert_eq!(observation.arrow_schema.field(1).name(), "late");
    assert_eq!(observation.evidence["content_coverage"], "full_content");
    drop(observation);
    drop(source);

    let document = br#"[{"id":1},{"id":2,"late":"observed"}]"#.to_vec();
    let source: Arc<dyn ByteSource> = Arc::new(
        futures_executor::block_on(MemoryByteSource::from_bytes(
            "full-content-json",
            document.clone(),
            Arc::clone(&memory),
        ))
        .unwrap(),
    );
    let observation =
        futures_executor::block_on(JsonDocumentFormatDriver::new().unwrap().discover(
            Arc::clone(&source),
            FormatDiscoveryRequest {
                options: serde_json::json!({}),
                discovery_kind: FormatDiscoveryKind::FullContent,
                maximum_bytes: 8,
                maximum_records: 1,
                memory: Arc::clone(&memory),
                cancellation: cdf_runtime::RunCancellation::default(),
            },
        ))
        .unwrap();
    assert_eq!(observation.sampled_bytes, document.len() as u64);
    assert_eq!(observation.sampled_records, 2);
    assert_eq!(observation.arrow_schema.field(1).name(), "late");
    assert_eq!(observation.evidence["content_coverage"], "full_content");
    drop(observation);
    drop(source);
    assert_eq!(coordinator.snapshot().current_bytes, 0);
}

#[test]
fn full_content_json_discovery_normalizes_pretty_printed_records_to_ndjson() {
    let coordinator =
        Arc::new(DeterministicMemoryCoordinator::new(256 * 1024 * 1024, BTreeMap::new()).unwrap());
    let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
    let document = br#"[
  {
    "id": 1,
    "label": "space preserved"
  },
  {
    "id": 2,
    "late": true
  }
]"#
    .to_vec();
    let source: Arc<dyn ByteSource> = Arc::new(
        futures_executor::block_on(MemoryByteSource::from_bytes(
            "pretty-full-content-json",
            document.clone(),
            Arc::clone(&memory),
        ))
        .unwrap(),
    );

    let observation =
        futures_executor::block_on(JsonDocumentFormatDriver::new().unwrap().discover(
            Arc::clone(&source),
            FormatDiscoveryRequest {
                options: serde_json::json!({}),
                discovery_kind: FormatDiscoveryKind::FullContent,
                maximum_bytes: document.len() as u64,
                maximum_records: u64::MAX,
                memory: Arc::clone(&memory),
                cancellation: cdf_runtime::RunCancellation::default(),
            },
        ))
        .unwrap();

    assert_eq!(observation.sampled_bytes, document.len() as u64);
    assert_eq!(observation.sampled_records, 2);
    assert_eq!(observation.arrow_schema.field(0).name(), "id");
    assert_eq!(observation.arrow_schema.field(1).name(), "label");
    assert_eq!(observation.arrow_schema.field(2).name(), "late");
    drop(observation);
    drop(source);
    assert_eq!(coordinator.snapshot().current_bytes, 0);
}

#[test]
fn full_content_schema_is_invariant_to_transport_rechunking_and_inference_windows() {
    let input = br#"{"id":1,"metric":1,"values":[1],"nested":{"active":true}}
{"id":2,"metric":1.5,"values":2,"nested":{"label":"x"}}
{"id":3,"metric":null,"values":[3.5],"late":"yes"}
"#;
    let (expected, expected_records) =
        infer_json_schema(Cursor::new(input.as_slice()), None).unwrap();
    assert_eq!(expected_records, 3);

    for chunk_bytes in [1_u64, 2, 7, 31, 1024] {
        let coordinator = Arc::new(
            DeterministicMemoryCoordinator::new(256 * 1024 * 1024, BTreeMap::new()).unwrap(),
        );
        let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
        let source: Arc<dyn ByteSource> = Arc::new(
            futures_executor::block_on(MemoryByteSource::from_bytes(
                format!("rechunk-{chunk_bytes}"),
                input.to_vec(),
                Arc::clone(&memory),
            ))
            .unwrap(),
        );
        let stream = futures_executor::block_on(source.open_sequential(SequentialReadRequest {
            preferred_chunk_bytes: chunk_bytes,
            cancellation: cdf_runtime::RunCancellation::default(),
        }))
        .unwrap();
        let (observed, sampled_bytes, sampled_records) =
            futures_executor::block_on(infer_full_content_json_schema(
                stream,
                Arc::clone(&memory),
                cdf_runtime::RunCancellation::default(),
                DEFAULT_MAXIMUM_RECORD_BYTES,
                32,
            ))
            .unwrap();
        assert_eq!(observed, expected, "chunk size {chunk_bytes}");
        assert_eq!(sampled_bytes, input.len() as u64);
        assert_eq!(sampled_records, 3);
        drop(source);
        assert_eq!(coordinator.snapshot().current_bytes, 0);
    }

    for seed in 1_u64..=32 {
        let coordinator = Arc::new(
            DeterministicMemoryCoordinator::new(256 * 1024 * 1024, BTreeMap::new()).unwrap(),
        );
        let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
        let mut state = seed;
        let mut offset = 0_usize;
        let mut chunks = Vec::new();
        while offset < input.len() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let length = (state as usize % 37 + 1).min(input.len() - offset);
            let lease = reserve_blocking(
                Arc::clone(&memory),
                &ReservationRequest::new(
                    ConsumerKey::new(
                        format!("json-random-rechunk-{seed}-{offset}"),
                        MemoryClass::Source,
                    )
                    .unwrap(),
                    length as u64,
                )
                .unwrap(),
            )
            .unwrap();
            chunks.push(Ok(AccountedBytes::new(
                bytes::Bytes::copy_from_slice(&input[offset..offset + length]),
                lease,
            )
            .unwrap()));
            offset += length;
        }
        let stream: AccountedByteStream = Box::pin(stream::iter(chunks));
        let (observed, sampled_bytes, sampled_records) =
            futures_executor::block_on(infer_full_content_json_schema(
                stream,
                Arc::clone(&memory),
                cdf_runtime::RunCancellation::default(),
                DEFAULT_MAXIMUM_RECORD_BYTES,
                32,
            ))
            .unwrap();
        assert_eq!(observed, expected, "random rechunk seed {seed}");
        assert_eq!(sampled_bytes, input.len() as u64);
        assert_eq!(sampled_records, 3);
        assert_eq!(coordinator.snapshot().current_bytes, 0);
    }
}

#[test]
fn bounded_selector_returns_zero_copy_array_range_and_scalar_pagination() {
    let body = br#" {"count":2,"next":"page-2","ignored":null,"items" : [ {"id":1}, {"id":2} ]} "#;
    let selected = select_bounded_json_records(body, "$.items").unwrap();

    assert_eq!(&body[selected.byte_range], br#"[ {"id":1}, {"id":2} ]"#);
    assert!(selected.records_present);
    assert_eq!(
        selected.top_level_scalar_fields,
        BTreeMap::from([
            ("count".to_owned(), "2".to_owned()),
            ("next".to_owned(), "page-2".to_owned())
        ])
    );
}

#[test]
fn bounded_selector_rejects_duplicate_and_non_array_targets() {
    let duplicate =
        select_bounded_json_records(br#"{"items":[],"items":[]}"#, "$.items").unwrap_err();
    assert!(duplicate.message.contains("repeats field"), "{duplicate}");
    let scalar = select_bounded_json_records(br#"{"items":1}"#, "$.items").unwrap_err();
    assert!(scalar.message.contains("not an array"), "{scalar}");
    let empty = select_bounded_json_records(br#"{"items": [ ]}"#, "$.items").unwrap();
    assert!(!empty.records_present);
}

#[test]
#[ignore = "release performance envelope"]
fn rest_selector_tape_decode_release_envelope() {
    const RECORDS: u64 = 262_144;
    const ITERATIONS: usize = 5;
    const PARALLELISM: usize = 2;
    let mut document = String::with_capacity(RECORDS as usize * 52);
    document.push_str(r#"{"next":"done","items":["#);
    for id in 0..RECORDS {
        if id != 0 {
            document.push(',');
        }
        write!(
            document,
            r#"{{"id":{id},"active":true,"category":"benchmark"}}"#
        )
        .unwrap();
    }
    document.push_str("]}");

    let coordinator =
        Arc::new(DeterministicMemoryCoordinator::new(512 * 1024 * 1024, BTreeMap::new()).unwrap());
    let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
    let body_bytes = u64::try_from(document.len()).unwrap();
    let lease = reserve_blocking(
        Arc::clone(&memory),
        &ReservationRequest::new(
            ConsumerKey::new("rest-release-envelope-input", MemoryClass::Source).unwrap(),
            body_bytes,
        )
        .unwrap(),
    )
    .unwrap();
    let body = AccountedBytes::new(bytes::Bytes::from(document), lease).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("active", DataType::Boolean, false),
        Field::new("category", DataType::Utf8, false),
    ]));
    let mut observations = Vec::with_capacity(ITERATIONS);
    for iteration in 0..=ITERATIONS {
        let started = Instant::now();
        let decoded_rows = std::thread::scope(|scope| {
            (0..PARALLELISM)
                .map(|worker| {
                    let body = body.clone();
                    let schema = Arc::clone(&schema);
                    let memory = Arc::clone(&memory);
                    scope.spawn(move || {
                        let selection =
                            select_bounded_json_records(body.payload(), "$.items").unwrap();
                        let selected = body.slice(selection.byte_range).unwrap();
                        let source = Arc::new(
                            MemoryByteSource::from_ephemeral_accounted_bytes(
                                format!("rest-release-envelope-{iteration}-{worker}"),
                                selected,
                            )
                            .unwrap(),
                        );
                        let decoded = futures_executor::block_on(decode_bounded_format(
                            Arc::new(JsonDocumentFormatDriver::new().unwrap()),
                            source,
                            BoundedFormatRequest::new(
                                ReadOptions::new(
                                    ResourceId::new("benchmark.rest").unwrap(),
                                    PartitionId::new(format!("rest-{worker}")).unwrap(),
                                ),
                                memory,
                            )
                            .with_schema(DecodeSchemaPlan::fixed_admission(schema)),
                        ))
                        .unwrap();
                        decoded
                            .batches
                            .iter()
                            .map(|batch| batch.header.row_count)
                            .sum::<u64>()
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .sum::<u64>()
        });
        assert_eq!(decoded_rows, RECORDS * PARALLELISM as u64);
        let elapsed = started.elapsed();
        if iteration != 0 {
            observations.push((
                elapsed,
                body_bytes as f64 * PARALLELISM as f64 / elapsed.as_secs_f64(),
            ));
        }
    }
    observations.sort_by_key(|(elapsed, _)| *elapsed);
    let (median_elapsed, median_bytes_per_second) = observations[ITERATIONS / 2];
    eprintln!(
        "rest selector+tape decode: {} rows, {} bytes in {median_elapsed:?}: {:.1} MiB/s, {:.1} M rows/s",
        RECORDS * PARALLELISM as u64,
        body_bytes * PARALLELISM as u64,
        median_bytes_per_second / (1024.0 * 1024.0),
        RECORDS as f64 * PARALLELISM as f64 / median_elapsed.as_secs_f64() / 1_000_000.0,
    );
    assert!(
        median_bytes_per_second >= 300.0 * 1024.0 * 1024.0,
        "REST aggregate selector+tape decode fell below 300 MiB/s: {median_bytes_per_second} B/s"
    );
    drop(body);
    assert_eq!(coordinator.snapshot().current_bytes, 0);
}

#[test]
#[ignore = "release performance envelope"]
fn rest_selector_tape_decode_exceeds_superseded_dom_shape_by_three_times() {
    const RECORDS: u64 = 262_144;
    const ITERATIONS: usize = 5;
    let mut document = String::with_capacity(RECORDS as usize * 52);
    document.push_str(r#"{"next":"done","items":["#);
    for id in 0..RECORDS {
        if id != 0 {
            document.push(',');
        }
        write!(
            document,
            r#"{{"id":{id},"active":true,"category":"benchmark"}}"#
        )
        .unwrap();
    }
    document.push_str("]}");

    let coordinator =
        Arc::new(DeterministicMemoryCoordinator::new(512 * 1024 * 1024, BTreeMap::new()).unwrap());
    let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
    let body_bytes = u64::try_from(document.len()).unwrap();
    let lease = reserve_blocking(
        Arc::clone(&memory),
        &ReservationRequest::new(
            ConsumerKey::new("rest-dom-comparison-input", MemoryClass::Source).unwrap(),
            body_bytes,
        )
        .unwrap(),
    )
    .unwrap();
    let body = AccountedBytes::new(bytes::Bytes::from(document), lease).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("active", DataType::Boolean, false),
        Field::new("category", DataType::Utf8, false),
    ]));
    let mut tape_observations = Vec::with_capacity(ITERATIONS);
    let mut dom_observations = Vec::with_capacity(ITERATIONS);
    for iteration in 0..=ITERATIONS {
        let started = Instant::now();
        let selection = select_bounded_json_records(body.payload(), "$.items").unwrap();
        let selected = body.slice(selection.byte_range).unwrap();
        let source = Arc::new(
            MemoryByteSource::from_ephemeral_accounted_bytes(
                format!("rest-dom-comparison-{iteration}"),
                selected,
            )
            .unwrap(),
        );
        let decoded = futures_executor::block_on(decode_bounded_format(
            Arc::new(JsonDocumentFormatDriver::new().unwrap()),
            source,
            BoundedFormatRequest::new(
                ReadOptions::new(
                    ResourceId::new("benchmark.rest").unwrap(),
                    PartitionId::new("rest-dom-comparison").unwrap(),
                ),
                Arc::clone(&memory),
            )
            .with_schema(DecodeSchemaPlan::fixed_admission(Arc::clone(&schema))),
        ))
        .unwrap();
        assert_eq!(
            decoded
                .batches
                .iter()
                .map(|batch| batch.header.row_count)
                .sum::<u64>(),
            RECORDS
        );
        let tape_elapsed = started.elapsed();
        drop(decoded);

        // This benchmark-only reference intentionally performs less work than the deleted
        // REST implementation: it includes its full DOM, object materialization,
        // reserialization, and Arrow decode, but omits the old per-page schema inference and
        // reconciliation. It is therefore a conservative lower bound for the superseded
        // production shape, not production compatibility code.
        let started = Instant::now();
        let mut root: serde_json::Value = serde_json::from_slice(body.payload()).unwrap();
        let pagination = root
            .as_object()
            .unwrap()
            .iter()
            .filter_map(|(name, value)| {
                value.as_str().map(|value| (name.clone(), value.to_owned()))
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(pagination.get("next").map(String::as_str), Some("done"));
        let records = root
            .get_mut("items")
            .and_then(serde_json::Value::as_array_mut)
            .map(std::mem::take)
            .unwrap();
        let records = records
            .into_iter()
            .map(|record| record.as_object().unwrap().clone())
            .collect::<Vec<_>>();
        let declared = BTreeMap::from([("active", 1_u8), ("category", 4), ("id", 2)]);
        let mut admitted = Vec::with_capacity(records.len());
        for record in &records {
            let mut row = record.clone();
            row.retain(|name, _| declared.contains_key(name.as_str()));
            for (name, kind) in &declared {
                let value = row.get(*name).unwrap();
                assert!(match kind {
                    1 => value.is_boolean(),
                    2 => value.is_i64() || value.is_u64(),
                    4 => value.is_string(),
                    _ => false,
                });
            }
            admitted.push(row);
        }
        let mut inferred = BTreeMap::<String, (u8, bool, bool)>::new();
        for (record_index, record) in admitted.iter().enumerate() {
            for (_, _, seen) in inferred.values_mut() {
                *seen = false;
            }
            for (name, value) in record {
                let kind = if value.is_boolean() {
                    1
                } else if value.is_i64() || value.is_u64() {
                    2
                } else if value.is_f64() {
                    3
                } else if value.is_string() {
                    4
                } else {
                    5
                };
                let entry = inferred
                    .entry(name.clone())
                    .or_insert((kind, record_index != 0, true));
                entry.0 = entry.0.max(kind);
                entry.2 = true;
            }
            for (_, nullable, seen) in inferred.values_mut() {
                if !*seen {
                    *nullable = true;
                }
            }
        }
        assert_eq!(
            inferred
                .iter()
                .map(|(name, (kind, nullable, _))| (name.as_str(), *kind, *nullable))
                .collect::<Vec<_>>(),
            vec![
                ("active", 1, false),
                ("category", 4, false),
                ("id", 2, false)
            ]
        );
        let physical_schema = Schema::new(vec![
            Field::new("active", DataType::Boolean, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("id", DataType::Int64, false),
        ]);
        let physical_schema_hash =
            cdf_kernel::canonical_arrow_schema_hash(&physical_schema).unwrap();
        std::hint::black_box(physical_schema_hash.to_string());
        let mut ndjson = Vec::with_capacity(body.payload().len());
        for record in &records {
            serde_json::to_writer(&mut ndjson, record).unwrap();
            ndjson.push(b'\n');
        }
        let source = Arc::new(
            futures_executor::block_on(MemoryByteSource::from_bytes(
                format!("rest-dom-reference-{iteration}"),
                ndjson,
                Arc::clone(&memory),
            ))
            .unwrap(),
        );
        let decoded = futures_executor::block_on(decode_bounded_format(
            Arc::new(NdjsonFormatDriver::new().unwrap()),
            source,
            BoundedFormatRequest::new(
                ReadOptions::new(
                    ResourceId::new("benchmark.rest-dom").unwrap(),
                    PartitionId::new("rest-dom-reference").unwrap(),
                )
                .with_batch_size(records.len())
                .unwrap(),
                Arc::clone(&memory),
            )
            .with_schema(DecodeSchemaPlan::fixed_admission(Arc::clone(&schema))),
        ))
        .unwrap();
        assert_eq!(
            decoded
                .batches
                .iter()
                .map(|batch| batch.header.row_count)
                .sum::<u64>(),
            RECORDS
        );
        let dom_elapsed = started.elapsed();
        drop(decoded);

        if iteration != 0 {
            tape_observations.push(tape_elapsed);
            dom_observations.push(dom_elapsed);
        }
    }
    tape_observations.sort_unstable();
    dom_observations.sort_unstable();
    let tape = tape_observations[ITERATIONS / 2];
    let dom = dom_observations[ITERATIONS / 2];
    let speedup = dom.as_secs_f64() / tape.as_secs_f64();
    eprintln!(
        "REST selector+tape versus superseded DOM lower bound: {:.1} MiB, tape {tape:?}, DOM {dom:?}, {speedup:.2}x",
        body_bytes as f64 / (1024.0 * 1024.0),
    );
    assert!(
        speedup >= 3.0,
        "REST selector+tape decode did not reach 3x the superseded DOM lower bound: {speedup:.3}x"
    );
    drop(body);
    assert_eq!(coordinator.snapshot().current_bytes, 0);
}

#[test]
#[ignore = "release performance envelope"]
fn full_content_discovery_tracks_arrow_json_roofline() {
    const RECORDS: u64 = 524_288;
    const ITERATIONS: usize = 3;
    let mut input = String::with_capacity(RECORDS as usize * 72);
    for id in 0..RECORDS {
        writeln!(
            input,
            r#"{{"id":{id},"active":true,"metric":12.5,"category":"benchmark"}}"#
        )
        .unwrap();
    }
    let bytes = input.into_bytes();
    let byte_count = bytes.len() as f64;
    let (expected, expected_records) =
        infer_json_schema(Cursor::new(bytes.as_slice()), None).unwrap();
    assert_eq!(expected_records as u64, RECORDS);
    let coordinator =
        Arc::new(DeterministicMemoryCoordinator::new(512 * 1024 * 1024, BTreeMap::new()).unwrap());
    let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
    let source: Arc<dyn ByteSource> = Arc::new(
        futures_executor::block_on(MemoryByteSource::from_bytes(
            "full-content-discovery-envelope",
            bytes.clone(),
            Arc::clone(&memory),
        ))
        .unwrap(),
    );

    let mut reference = Vec::with_capacity(ITERATIONS);
    let mut cdf = Vec::with_capacity(ITERATIONS);
    for _ in 0..ITERATIONS {
        let started = Instant::now();
        let (schema, records) = infer_json_schema(Cursor::new(bytes.as_slice()), None).unwrap();
        reference.push(started.elapsed());
        assert_eq!(schema, expected);
        assert_eq!(records as u64, RECORDS);

        let started = Instant::now();
        let observation = futures_executor::block_on(NdjsonFormatDriver::new().unwrap().discover(
            Arc::clone(&source),
            FormatDiscoveryRequest {
                options: serde_json::json!({}),
                discovery_kind: FormatDiscoveryKind::FullContent,
                maximum_bytes: 1,
                maximum_records: 1,
                memory: Arc::clone(&memory),
                cancellation: cdf_runtime::RunCancellation::default(),
            },
        ))
        .unwrap();
        cdf.push(started.elapsed());
        assert_eq!(observation.arrow_schema.as_ref(), &expected);
        assert_eq!(observation.sampled_records, RECORDS);
    }
    reference.sort_unstable();
    cdf.sort_unstable();
    let reference = reference[ITERATIONS / 2];
    let cdf = cdf[ITERATIONS / 2];
    let reference_rate = byte_count / reference.as_secs_f64();
    let cdf_rate = byte_count / cdf.as_secs_f64();
    let roofline_ratio = cdf_rate / reference_rate;
    eprintln!(
        "full-content discovery: {:.1} MiB, Arrow reference {reference:?} ({:.1} MiB/s), CDF {cdf:?} ({:.1} MiB/s), {:.2}x roofline",
        byte_count / (1024.0 * 1024.0),
        reference_rate / (1024.0 * 1024.0),
        cdf_rate / (1024.0 * 1024.0),
        roofline_ratio,
    );
    assert!(
        roofline_ratio >= 0.6,
        "full-content discovery fell below 0.6x raw arrow-json inference: {roofline_ratio:.3}"
    );
    drop(source);
    assert_eq!(coordinator.snapshot().current_bytes, 0);
}

#[test]
fn ndjson_oversized_record_fails_before_publishing_a_batch() {
    let input = br#"{"id":1,"value":"this-record-is-too-large"}
{"id":2,"value":"would-otherwise-be-valid"}
"#;
    let coordinator =
        Arc::new(DeterministicMemoryCoordinator::new(128 * 1024 * 1024, BTreeMap::new()).unwrap());
    let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
    let input_lease = reserve_blocking(
        Arc::clone(&memory),
        &ReservationRequest::new(
            ConsumerKey::new("json-oversized-test-input", MemoryClass::Source).unwrap(),
            u64::try_from(input.len()).unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    let accounted = AccountedBytes::new(bytes::Bytes::copy_from_slice(input), input_lease).unwrap();
    let request = PhysicalDecodeRequest {
        unit: DecodeUnitPlan {
            unit_id: "ndjson-oversized".to_owned(),
            ordinal: 0,
            extent: None,
            estimated_working_set_bytes: 1024 * 1024,
            independently_retryable: true,
        },
        resource_id: ResourceId::new("events.oversized").unwrap(),
        partition_id: PartitionId::new("file-0001").unwrap(),
        batch_id_prefix: "events-oversized".to_owned(),
        schema: cdf_runtime::DecodeSchemaPlan::verified_physical(Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("value", DataType::Utf8, true),
        ]))),
        source_position: None,
        projection: None,
        predicates: Vec::new(),
        target_batch_rows: 64,
        target_batch_bytes: 16,
        memory,
        cancellation: cdf_runtime::RunCancellation::default(),
    };
    let error = futures_executor::block_on(async move {
        let input: AccountedByteStream = Box::pin(stream::iter([Ok(accounted)]));
        let mut decoded = decode_ndjson_stream(input, request, 8).await?;
        match decoded.try_next().await {
            Err(error) => Result::<()>::Err(error),
            Ok(_) => Result::<()>::Err(CdfError::internal("oversized NDJSON emitted a batch")),
        }
    })
    .unwrap_err();

    assert!(error.message.contains("planned 8-byte"), "{error}");
    assert!(error.message.contains("maximum_record_bytes"), "{error}");
    assert_eq!(coordinator.snapshot().current_bytes, 0);
}

#[test]
fn byte_feedback_is_deterministic_and_never_exceeds_the_plan_target() {
    const MIB: u64 = 1024 * 1024;
    assert_eq!(
        next_decode_window_target(16 * MIB, 8 * MIB, 16 * MIB),
        16 * MIB
    );
    assert_eq!(
        next_decode_window_target(16 * MIB, 32 * MIB, 16 * MIB),
        8 * MIB
    );
    assert_eq!(
        next_decode_window_target(8 * MIB, 4 * MIB, 16 * MIB),
        16 * MIB
    );
    assert_eq!(
        next_decode_window_target(16 * MIB, 64 * MIB, 16 * MIB),
        4 * MIB
    );
}

#[test]
fn ndjson_tape_decode_flushes_at_the_byte_target_before_the_row_target() {
    let coordinator =
        Arc::new(DeterministicMemoryCoordinator::new(128 * 1024 * 1024, BTreeMap::new()).unwrap());
    let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
    let chunks = [
        br#"{"id":1,"value":"aaaa"#.as_slice(),
        br#"bbbbbbbb"}
{"id":2,"value":"cccccccc"}
"#
        .as_slice(),
        br#"{"id":3,"value":"dddddddd"}
"#
        .as_slice(),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, input)| {
        let lease = reserve_blocking(
            Arc::clone(&memory),
            &ReservationRequest::new(
                ConsumerKey::new(
                    format!("json-byte-target-input-{index}"),
                    MemoryClass::Source,
                )
                .unwrap(),
                u64::try_from(input.len()).unwrap(),
            )
            .unwrap(),
        )
        .unwrap();
        Ok(AccountedBytes::new(bytes::Bytes::copy_from_slice(input), lease).unwrap())
    })
    .collect::<Vec<Result<_>>>();
    let request = PhysicalDecodeRequest {
        unit: DecodeUnitPlan {
            unit_id: "ndjson-byte-target".to_owned(),
            ordinal: 0,
            extent: None,
            estimated_working_set_bytes: 1024 * 1024,
            independently_retryable: true,
        },
        resource_id: ResourceId::new("events.byte_target").unwrap(),
        partition_id: PartitionId::new("file-0001").unwrap(),
        batch_id_prefix: "events-byte-target".to_owned(),
        schema: cdf_runtime::DecodeSchemaPlan::verified_physical(Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("value", DataType::Utf8, true),
        ]))),
        source_position: None,
        projection: None,
        predicates: Vec::new(),
        target_batch_rows: 64,
        target_batch_bytes: 16,
        memory,
        cancellation: cdf_runtime::RunCancellation::default(),
    };
    let batches = futures_executor::block_on(async move {
        let input: AccountedByteStream = Box::pin(stream::iter(chunks));
        let mut decoded =
            decode_ndjson_stream(input, request, DEFAULT_MAXIMUM_RECORD_BYTES).await?;
        let mut batches = Vec::new();
        while let Some(batch) = decoded.try_next().await? {
            batches.push(batch);
        }
        Result::<Vec<AccountedPhysicalBatch>>::Ok(batches)
    })
    .unwrap();

    assert_eq!(batches.len(), 3);
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.batch().header.row_count)
            .collect::<Vec<_>>(),
        vec![1, 1, 1]
    );
    drop(batches);
    assert_eq!(coordinator.snapshot().current_bytes, 0);
}

#[test]
fn ndjson_tape_decode_flushes_at_the_row_target_before_source_eof() {
    let input = br#"{"id":1}
"#;
    let coordinator =
        Arc::new(DeterministicMemoryCoordinator::new(128 * 1024 * 1024, BTreeMap::new()).unwrap());
    let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
    let lease = reserve_blocking(
        Arc::clone(&memory),
        &ReservationRequest::new(
            ConsumerKey::new("json-row-target-input", MemoryClass::Source).unwrap(),
            u64::try_from(input.len()).unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    let accounted = AccountedBytes::new(bytes::Bytes::copy_from_slice(input), lease).unwrap();
    let request = PhysicalDecodeRequest {
        unit: DecodeUnitPlan {
            unit_id: "ndjson-row-target".to_owned(),
            ordinal: 0,
            extent: None,
            estimated_working_set_bytes: 1024 * 1024,
            independently_retryable: true,
        },
        resource_id: ResourceId::new("events.row_target").unwrap(),
        partition_id: PartitionId::new("stream-0001").unwrap(),
        batch_id_prefix: "events-row-target".to_owned(),
        schema: cdf_runtime::DecodeSchemaPlan::verified_physical(Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
        ]))),
        source_position: None,
        projection: None,
        predicates: Vec::new(),
        target_batch_rows: 1,
        target_batch_bytes: 16 * 1024 * 1024,
        memory,
        cancellation: cdf_runtime::RunCancellation::default(),
    };
    let (batch, decoded) = futures_executor::block_on(async move {
        let input: AccountedByteStream =
            Box::pin(stream::once(async { Ok(accounted) }).chain(stream::pending()));
        let mut decoded = decode_ndjson_stream(input, request, DEFAULT_MAXIMUM_RECORD_BYTES)
            .await
            .unwrap();
        let batch = decoded
            .try_next()
            .now_or_never()
            .expect("row target did not flush before the unbounded source requested more data")
            .unwrap()
            .unwrap();
        (batch, decoded)
    });
    assert_eq!(batch.batch().header.row_count, 1);
    drop(batch);
    drop(decoded);
    assert_eq!(coordinator.snapshot().current_bytes, 0);
}

#[test]
fn ndjson_tape_decode_recovers_drift_with_exact_residual_evidence() {
    let input = br#"{"id":1,"event_type":"order.created","extra":{"source":"mobile"}}
{"id":2,"event_type":"order.updated"}
{"id":3,"event_type":42}
"#;
    let coordinator =
        Arc::new(DeterministicMemoryCoordinator::new(64 * 1024 * 1024, BTreeMap::new()).unwrap());
    let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
    let input_lease = reserve_blocking(
        Arc::clone(&memory),
        &ReservationRequest::new(
            ConsumerKey::new("json-drift-test-input", MemoryClass::Source).unwrap(),
            u64::try_from(input.len()).unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    let accounted = AccountedBytes::new(bytes::Bytes::copy_from_slice(input), input_lease).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("event_type", DataType::Utf8, false),
    ]));
    let request = PhysicalDecodeRequest {
        unit: DecodeUnitPlan {
            unit_id: "ndjson-stream".to_owned(),
            ordinal: 0,
            extent: None,
            estimated_working_set_bytes: 1024 * 1024,
            independently_retryable: true,
        },
        resource_id: ResourceId::new("events.raw").unwrap(),
        partition_id: PartitionId::new("file-0001").unwrap(),
        batch_id_prefix: "events-raw".to_owned(),
        schema: cdf_runtime::DecodeSchemaPlan::verified_physical(schema),
        source_position: None,
        projection: None,
        predicates: Vec::new(),
        target_batch_rows: 64,
        target_batch_bytes: 1024 * 1024,
        memory,
        cancellation: cdf_runtime::RunCancellation::default(),
    };
    let batches = futures_executor::block_on(async move {
        let input: AccountedByteStream = Box::pin(stream::iter([Ok(accounted)]));
        let mut decoded =
            decode_ndjson_stream(input, request, DEFAULT_MAXIMUM_RECORD_BYTES).await?;
        let mut batches = Vec::new();
        while let Some(batch) = decoded.try_next().await? {
            batches.push(batch);
        }
        Result::<Vec<AccountedPhysicalBatch>>::Ok(batches)
    })
    .unwrap();

    assert_eq!(batches.len(), 1);
    let batch = batches[0].batch();
    let record_batch = batch.record_batch().unwrap();
    assert_eq!(record_batch.num_rows(), 3);
    assert_eq!(
        batch.header.observed_schema_hash,
        cdf_kernel::canonical_arrow_schema_hash(record_batch.schema().as_ref()).unwrap()
    );
    assert_eq!(
        batch.header.observation_representation,
        cdf_kernel::PhysicalObservationRepresentation::MaterializedOutput
    );
    assert!(
        record_batch
            .schema()
            .field_with_name("event_type")
            .unwrap()
            .is_nullable()
    );
    let event_types = record_batch
        .column_by_name("event_type")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(event_types.value(0), "order.created");
    assert_eq!(event_types.value(1), "order.updated");
    assert!(event_types.is_null(2));

    let candidates = batch.header.residual_candidates();
    assert_eq!(candidates.len(), 2);
    let extra = candidates
        .iter()
        .find(|candidate| candidate.source_path() == ["extra"])
        .unwrap();
    assert_eq!(physical_type(extra.observed_field()), Some("json:object"));
    assert_eq!(
        extra
            .value()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        br#"{"source":"mobile"}"#
    );
    let drift = candidates
        .iter()
        .find(|candidate| candidate.source_path() == ["event_type"])
        .unwrap();
    assert_eq!(drift.source_row_ordinal(), 2);
    assert_eq!(drift.batch_row_ordinal(), 2);
    assert_eq!(drift.observed_field().data_type(), &DataType::Int64);
    assert_eq!(drift.expected_field().unwrap().data_type(), &DataType::Utf8);
    assert_eq!(
        drift
            .value()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        42
    );

    drop(batches);
    assert_eq!(coordinator.snapshot().current_bytes, 0);
}

#[test]
fn empty_ndjson_emits_schema_bearing_physical_batch() {
    let memory: Arc<dyn MemoryCoordinator> =
        Arc::new(DeterministicMemoryCoordinator::new(64 * 1024 * 1024, BTreeMap::new()).unwrap());
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let request = PhysicalDecodeRequest {
        unit: DecodeUnitPlan {
            unit_id: "empty-ndjson".to_owned(),
            ordinal: 0,
            extent: None,
            estimated_working_set_bytes: 1024 * 1024,
            independently_retryable: true,
        },
        resource_id: ResourceId::new("events.empty").unwrap(),
        partition_id: PartitionId::new("file-empty").unwrap(),
        batch_id_prefix: "events-empty".to_owned(),
        schema: cdf_runtime::DecodeSchemaPlan::fixed_admission(schema),
        source_position: None,
        projection: None,
        predicates: Vec::new(),
        target_batch_rows: 64,
        target_batch_bytes: 1024 * 1024,
        memory,
        cancellation: cdf_runtime::RunCancellation::default(),
    };
    let batches = futures_executor::block_on(async move {
        let input: AccountedByteStream = Box::pin(stream::empty());
        let mut decoded =
            decode_ndjson_stream(input, request, DEFAULT_MAXIMUM_RECORD_BYTES).await?;
        let mut batches = Vec::new();
        while let Some(batch) = decoded.try_next().await? {
            batches.push(batch);
        }
        Result::<Vec<AccountedPhysicalBatch>>::Ok(batches)
    })
    .unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].batch().record_batch().unwrap().num_rows(), 0);
    assert_eq!(
        batches[0].batch().header.observation_representation,
        cdf_kernel::PhysicalObservationRepresentation::ArrowSchema
    );
}
