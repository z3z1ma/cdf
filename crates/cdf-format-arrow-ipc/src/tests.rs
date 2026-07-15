use std::{
    collections::{BTreeMap, HashMap},
    io::Cursor,
    sync::Arc,
    time::Instant,
};

use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_ipc::{reader::FileReader, writer::FileWriter};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use cdf_kernel::{PartitionId, ResourceId};
use cdf_memory::{
    AccountedBytes, ConsumerKey, DeterministicMemoryCoordinator, MemoryClass, MemoryCoordinator,
    ReservationRequest, reserve_blocking,
};
use cdf_runtime::{
    ByteSourceCapabilities, ContentIdentity, FormatDriver, GenerationStrength,
    PhysicalDecodeRequest, RunCancellation,
};
use futures_util::TryStreamExt;

use super::*;

struct MemoryByteSource {
    identity: ContentIdentity,
    capabilities: ByteSourceCapabilities,
    bytes: Bytes,
    memory: Arc<dyn MemoryCoordinator>,
}

impl MemoryByteSource {
    fn new(bytes: Vec<u8>, memory: Arc<dyn MemoryCoordinator>) -> Arc<Self> {
        Arc::new(Self {
            identity: ContentIdentity {
                stable_id: "memory://fixture.arrow".to_owned(),
                size_bytes: Some(bytes.len() as u64),
                generation: None,
                checksum: Some("sha256:test-fixture".to_owned()),
                strength: GenerationStrength::ContentAddressed,
            },
            capabilities: ByteSourceCapabilities {
                known_length: true,
                reopenable: true,
                seekable: true,
                exact_ranges: true,
                useful_range_concurrency: 4,
                minimum_chunk_bytes: 1,
                maximum_chunk_bytes: 1024 * 1024,
            },
            bytes: Bytes::from(bytes),
            memory,
        })
    }

    fn accounted(&self, bytes: Bytes) -> Result<AccountedBytes> {
        let request = ReservationRequest::new(
            ConsumerKey::new("arrow-ipc-test-source", MemoryClass::Source)?,
            bytes.len() as u64,
        )?;
        let lease = reserve_blocking(Arc::clone(&self.memory), &request)?;
        AccountedBytes::new(bytes, lease)
    }
}

impl ByteSource for MemoryByteSource {
    fn identity(&self) -> &ContentIdentity {
        &self.identity
    }

    fn capabilities(&self) -> &ByteSourceCapabilities {
        &self.capabilities
    }

    fn open_sequential(
        &self,
        _request: cdf_runtime::SequentialReadRequest,
    ) -> BoxFuture<'_, Result<cdf_runtime::AccountedByteStream>> {
        Box::pin(async move {
            let bytes = self.accounted(self.bytes.clone())?;
            Ok(
                Box::pin(futures_util::stream::once(async move { Ok(bytes) }))
                    as cdf_runtime::AccountedByteStream,
            )
        })
    }

    fn read_exact_range(
        &self,
        extent: ByteExtent,
        cancellation: RunCancellation,
    ) -> BoxFuture<'_, Result<AccountedBytes>> {
        Box::pin(async move {
            cancellation.check()?;
            let start = usize::try_from(extent.start)
                .map_err(|_| CdfError::data("test range start exceeds usize"))?;
            let end = usize::try_from(
                extent
                    .start
                    .checked_add(extent.length)
                    .ok_or_else(|| CdfError::data("test range overflow"))?,
            )
            .map_err(|_| CdfError::data("test range end exceeds usize"))?;
            if end > self.bytes.len() {
                return Err(CdfError::data("test range exceeds fixture"));
            }
            self.accounted(self.bytes.slice(start..end))
        })
    }
}

fn fixture() -> (Arc<Schema>, Vec<u8>) {
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ],
        HashMap::from([("owner".to_owned(), "cdf-test".to_owned())]),
    ));
    let mut bytes = Vec::new();
    let mut writer = FileWriter::try_new(&mut bytes, schema.as_ref()).unwrap();
    for offset in [0, 2] {
        writer
            .write(
                &RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int64Array::from(vec![offset + 1, offset + 2])),
                        Arc::new(StringArray::from(vec!["a", "b"])),
                    ],
                )
                .unwrap(),
            )
            .unwrap();
    }
    writer.finish().unwrap();
    drop(writer);
    (schema, bytes)
}

#[test]
fn arrow_ipc_file_driver_discovers_projects_and_streams_blocks() {
    let (schema, bytes) = fixture();
    let memory: Arc<dyn MemoryCoordinator> =
        Arc::new(DeterministicMemoryCoordinator::new(64 * 1024 * 1024, BTreeMap::new()).unwrap());
    let source: Arc<dyn ByteSource> = MemoryByteSource::new(bytes, Arc::clone(&memory));
    let driver = ArrowIpcFileFormatDriver::new().unwrap();
    let detection = driver
        .detect(&FormatProbe {
            extension: Some("arrow".to_owned()),
            mime_type: None,
            prefix: MAGIC.to_vec(),
            suffix: MAGIC.to_vec(),
        })
        .unwrap();
    assert_eq!(detection.confidence, FormatDetectionConfidence::Strong);

    let error = futures_executor::block_on(driver.discover(
        Arc::clone(&source),
        FormatDiscoveryRequest {
            options: serde_json::json!({}),
            maximum_bytes: 9,
            maximum_records: 0,
            memory: Arc::clone(&memory),
            cancellation: RunCancellation::default(),
        },
    ))
    .unwrap_err();
    assert!(error.message.contains("discovery budget 9"));
    assert_eq!(memory.snapshot().current_bytes, 0);

    let observation = futures_executor::block_on(driver.discover(
        Arc::clone(&source),
        FormatDiscoveryRequest {
            options: serde_json::json!({}),
            maximum_bytes: 1024 * 1024,
            maximum_records: 0,
            memory: Arc::clone(&memory),
            cancellation: RunCancellation::default(),
        },
    ))
    .unwrap();
    assert_eq!(observation.arrow_schema.as_ref(), schema.as_ref());

    let session = futures_executor::block_on(driver.prepare_decode(
        source,
        DecodePlanningRequest {
            options: serde_json::json!({}),
            projection: Some(vec!["id".to_owned()]),
            predicates: Vec::new(),
            target_batch_rows: 64 * 1024,
            target_batch_bytes: 1024 * 1024,
            cancellation: RunCancellation::default(),
        },
    ))
    .unwrap();
    assert_eq!(session.units().len(), 1);

    let stream = futures_executor::block_on(session.decode(PhysicalDecodeRequest {
        unit: session.units()[0].clone(),
        resource_id: ResourceId::new("fixture.arrow").unwrap(),
        partition_id: PartitionId::new("file-000001").unwrap(),
        batch_id_prefix: "fixture".to_owned(),
        schema: cdf_runtime::DecodeSchemaPlan::verified_physical(schema),
        source_position: None,
        projection: Some(vec!["id".to_owned()]),
        predicates: Vec::new(),
        target_batch_rows: 64 * 1024,
        target_batch_bytes: 1024 * 1024,
        memory: Arc::clone(&memory),
        cancellation: RunCancellation::default(),
    }))
    .unwrap();
    let batches = futures_executor::block_on(stream.try_collect::<Vec<_>>()).unwrap();
    assert_eq!(batches.len(), 2);
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.batch().header.row_count)
            .sum::<u64>(),
        4
    );
    assert!(batches.iter().all(|batch| {
        let record = batch.batch().record_batch().unwrap();
        record.num_columns() == 1 && record.column(0).as_any().is::<Int64Array>()
    }));
    drop(batches);
    drop(session);
    assert_eq!(memory.snapshot().current_bytes, 0);
}

#[test]
#[ignore = "performance evidence; run in release mode"]
fn arrow_ipc_driver_reference_rate() {
    const BATCHES: usize = 64;
    const ROWS: usize = 65_536;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from_iter_values(0..ROWS as i64)),
            Arc::new(Float64Array::from_iter_values(
                (0..ROWS).map(|value| value as f64),
            )),
        ],
    )
    .unwrap();
    let mut bytes = Vec::new();
    let mut writer = FileWriter::try_new(&mut bytes, schema.as_ref()).unwrap();
    for _ in 0..BATCHES {
        writer.write(&batch).unwrap();
    }
    writer.finish().unwrap();
    drop(writer);

    let reference_start = Instant::now();
    let reference_rows = FileReader::try_new(Cursor::new(bytes.clone()), None)
        .unwrap()
        .map(|batch| batch.unwrap().num_rows())
        .sum::<usize>();
    let reference_elapsed = reference_start.elapsed();

    let memory: Arc<dyn MemoryCoordinator> =
        Arc::new(DeterministicMemoryCoordinator::new(512 * 1024 * 1024, BTreeMap::new()).unwrap());
    let source: Arc<dyn ByteSource> = MemoryByteSource::new(bytes.clone(), Arc::clone(&memory));
    let driver = ArrowIpcFileFormatDriver::new().unwrap();
    let session = futures_executor::block_on(driver.prepare_decode(
        source,
        DecodePlanningRequest {
            options: serde_json::json!({}),
            projection: None,
            predicates: Vec::new(),
            target_batch_rows: ROWS,
            target_batch_bytes: 16 * 1024 * 1024,
            cancellation: RunCancellation::default(),
        },
    ))
    .unwrap();
    let driver_start = Instant::now();
    let stream = futures_executor::block_on(session.decode(PhysicalDecodeRequest {
        unit: session.units()[0].clone(),
        resource_id: ResourceId::new("bench.arrow").unwrap(),
        partition_id: PartitionId::new("file-000001").unwrap(),
        batch_id_prefix: "bench".to_owned(),
        schema: cdf_runtime::DecodeSchemaPlan::verified_physical(schema),
        source_position: None,
        projection: None,
        predicates: Vec::new(),
        target_batch_rows: ROWS,
        target_batch_bytes: 16 * 1024 * 1024,
        memory: Arc::clone(&memory),
        cancellation: RunCancellation::default(),
    }))
    .unwrap();
    let decoded = futures_executor::block_on(stream.try_collect::<Vec<_>>()).unwrap();
    let driver_elapsed = driver_start.elapsed();
    let driver_rows = decoded
        .iter()
        .map(|batch| batch.batch().header.row_count as usize)
        .sum::<usize>();
    assert_eq!(driver_rows, reference_rows);
    let mib = bytes.len() as f64 / (1024.0 * 1024.0);
    eprintln!(
        "arrow-ipc reference={:.2} MiB/s driver={:.2} MiB/s ratio={:.3}",
        mib / reference_elapsed.as_secs_f64(),
        mib / driver_elapsed.as_secs_f64(),
        reference_elapsed.as_secs_f64() / driver_elapsed.as_secs_f64()
    );
    drop(decoded);
    drop(session);
    assert_eq!(memory.snapshot().current_bytes, 0);
}
