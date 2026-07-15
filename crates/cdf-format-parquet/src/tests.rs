use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
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
use parquet::arrow::ArrowWriter;

use super::*;

struct MemoryByteSource {
    identity: ContentIdentity,
    capabilities: ByteSourceCapabilities,
    bytes: Bytes,
    memory: Arc<dyn MemoryCoordinator>,
    suffix_reads: AtomicU64,
    range_reads: Mutex<Vec<ByteExtent>>,
}

impl MemoryByteSource {
    fn new(bytes: Vec<u8>, memory: Arc<dyn MemoryCoordinator>) -> Arc<Self> {
        Arc::new(Self {
            identity: ContentIdentity {
                stable_id: "memory://fixture.parquet".to_owned(),
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
            suffix_reads: AtomicU64::new(0),
            range_reads: Mutex::new(Vec::new()),
        })
    }

    fn accounted(&self, bytes: Bytes) -> Result<AccountedBytes> {
        let request = ReservationRequest::new(
            ConsumerKey::new("parquet-test-source", MemoryClass::Source)?,
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
            self.range_reads.lock().unwrap().push(extent);
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
            if end == self.bytes.len() {
                self.suffix_reads.fetch_add(1, Ordering::Relaxed);
            }
            self.accounted(self.bytes.slice(start..end))
        })
    }
}

fn fixture() -> (Arc<Schema>, Vec<u8>) {
    let schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("id", DataType::Int64, false)],
        HashMap::from([(
            "org.apache.spark.sql.parquet.row.metadata".to_owned(),
            r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}"#
                .to_owned(),
        )]),
    ));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
    )
    .unwrap();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, Arc::clone(&schema), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    (schema, bytes)
}

fn empty_fixture() -> (Arc<Schema>, Vec<u8>) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut bytes = Vec::new();
    let writer = ArrowWriter::try_new(&mut bytes, Arc::clone(&schema), None).unwrap();
    writer.close().unwrap();
    (schema, bytes)
}

fn multi_row_group_fixture() -> (Arc<Schema>, Vec<u8>) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, Arc::clone(&schema), None).unwrap();
    for value in 0..8_i64 {
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![value]))],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.flush().unwrap();
    }
    writer.close().unwrap();
    (schema, bytes)
}

#[test]
fn prepared_session_loads_footer_once_across_many_row_groups() {
    let (schema, bytes) = multi_row_group_fixture();
    let memory: Arc<dyn MemoryCoordinator> =
        Arc::new(DeterministicMemoryCoordinator::new(64 * 1024 * 1024, BTreeMap::new()).unwrap());
    let source = MemoryByteSource::new(bytes, Arc::clone(&memory));
    let driver = ParquetFormatDriver::new().unwrap();
    let session = futures_executor::block_on(driver.prepare_decode(
        source.clone(),
        DecodePlanningRequest {
            options: serde_json::json!({}),
            projection: None,
            predicates: Vec::new(),
            target_batch_rows: 64 * 1024,
            target_batch_bytes: 1024 * 1024,
            cancellation: RunCancellation::default(),
        },
    ))
    .unwrap();
    assert_eq!(session.units().len(), 8);
    let frontiers = cdf_runtime::decode_unit_no_lookback_frontiers(session.units())
        .unwrap()
        .expect("Parquet row groups publish complete byte envelopes");
    assert!(frontiers.windows(2).all(|pair| pair[0] <= pair[1]));
    let suffix_reads = source.suffix_reads.load(Ordering::Relaxed);
    assert!(suffix_reads > 0);
    source.range_reads.lock().unwrap().clear();

    for unit in session.units().iter().cloned() {
        let planned_extent = unit.extent.expect("Parquet row group byte envelope");
        let first_request = source.range_reads.lock().unwrap().len();
        let stream = futures_executor::block_on(session.decode(PhysicalDecodeRequest {
            unit,
            resource_id: ResourceId::new("fixture.parquet").unwrap(),
            partition_id: PartitionId::new("file-000001").unwrap(),
            batch_id_prefix: "fixture".to_owned(),
            schema: cdf_runtime::DecodeSchemaPlan::verified_physical(Arc::clone(&schema)),
            source_position: None,
            projection: None,
            predicates: Vec::new(),
            target_batch_rows: 64 * 1024,
            target_batch_bytes: 1024 * 1024,
            memory: Arc::clone(&memory),
            cancellation: RunCancellation::default(),
        }))
        .unwrap();
        let batches = futures_executor::block_on(stream.try_collect::<Vec<_>>()).unwrap();
        assert_eq!(batches.len(), 1);
        drop(batches);
        let requests = source.range_reads.lock().unwrap();
        assert!(requests.len() > first_request);
        let planned_end = planned_extent.start + planned_extent.length;
        assert!(requests[first_request..].iter().all(|request| {
            request.start >= planned_extent.start && request.start + request.length <= planned_end
        }));
    }

    assert_eq!(source.suffix_reads.load(Ordering::Relaxed), suffix_reads);
    drop(session);
    assert_eq!(memory.snapshot().current_bytes, 0);
}

#[test]
fn parquet_driver_discovers_plans_and_decodes_through_neutral_byte_source() {
    let (schema, bytes) = fixture();
    let memory: Arc<dyn MemoryCoordinator> =
        Arc::new(DeterministicMemoryCoordinator::new(64 * 1024 * 1024, BTreeMap::new()).unwrap());
    let source: Arc<dyn ByteSource> = MemoryByteSource::new(bytes, Arc::clone(&memory));
    let driver = ParquetFormatDriver::new().unwrap();
    let detection = driver
        .detect(&FormatProbe {
            extension: Some("parquet".to_owned()),
            mime_type: None,
            prefix: b"PAR1".to_vec(),
            suffix: b"PAR1".to_vec(),
        })
        .unwrap();
    assert_eq!(detection.confidence, FormatDetectionConfidence::Strong);

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
    assert!(observation.sampled_bytes > 0);
    assert_eq!(observation.evidence["row_count"], "4");
    assert_eq!(observation.evidence["row_group_count"], "1");
    assert!(observation.evidence["footer_sha256"].starts_with("sha256:"));

    let session = futures_executor::block_on(driver.prepare_decode(
        source,
        DecodePlanningRequest {
            options: serde_json::json!({}),
            projection: None,
            predicates: Vec::new(),
            target_batch_rows: 64 * 1024,
            target_batch_bytes: 8 * 1024 * 1024,
            cancellation: RunCancellation::default(),
        },
    ))
    .unwrap();
    assert_eq!(session.units().len(), 1);

    let stream = futures_executor::block_on(session.decode(PhysicalDecodeRequest {
        unit: session.units()[0].clone(),
        resource_id: ResourceId::new("fixture.parquet").unwrap(),
        partition_id: PartitionId::new("file-000001").unwrap(),
        batch_id_prefix: "fixture".to_owned(),
        schema: cdf_runtime::DecodeSchemaPlan::verified_physical(schema),
        source_position: None,
        projection: None,
        predicates: Vec::new(),
        target_batch_rows: 64 * 1024,
        target_batch_bytes: 8 * 1024 * 1024,
        memory: Arc::clone(&memory),
        cancellation: RunCancellation::default(),
    }))
    .unwrap();
    let batches = futures_executor::block_on(stream.try_collect::<Vec<_>>()).unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].batch().header.row_count, 4);
    assert_eq!(
        batches[0].batch().record_batch().unwrap().schema().as_ref(),
        observation.arrow_schema.as_ref()
    );
    assert_eq!(
        batches[0].batch().header.observed_schema_hash,
        cdf_kernel::canonical_arrow_schema_hash(observation.arrow_schema.as_ref()).unwrap()
    );
    drop(batches);
    assert_eq!(memory.snapshot().current_bytes, 0);
}

#[test]
fn empty_parquet_file_emits_one_schema_bearing_batch() {
    let (schema, bytes) = empty_fixture();
    let memory: Arc<dyn MemoryCoordinator> =
        Arc::new(DeterministicMemoryCoordinator::new(64 * 1024 * 1024, BTreeMap::new()).unwrap());
    let source: Arc<dyn ByteSource> = MemoryByteSource::new(bytes, Arc::clone(&memory));
    let driver = ParquetFormatDriver::new().unwrap();
    let session = futures_executor::block_on(driver.prepare_decode(
        source,
        DecodePlanningRequest {
            options: serde_json::json!({}),
            projection: None,
            predicates: Vec::new(),
            target_batch_rows: 64 * 1024,
            target_batch_bytes: 8 * 1024 * 1024,
            cancellation: RunCancellation::default(),
        },
    ))
    .unwrap();
    assert_eq!(session.units().len(), 1);
    assert_eq!(session.units()[0].unit_id, "parquet-schema-only");

    let stream = futures_executor::block_on(session.decode(PhysicalDecodeRequest {
        unit: session.units()[0].clone(),
        resource_id: ResourceId::new("fixture.parquet").unwrap(),
        partition_id: PartitionId::new("file-000001").unwrap(),
        batch_id_prefix: "fixture".to_owned(),
        schema: cdf_runtime::DecodeSchemaPlan::fixed_admission(Arc::clone(&schema)),
        source_position: None,
        projection: None,
        predicates: Vec::new(),
        target_batch_rows: 64 * 1024,
        target_batch_bytes: 8 * 1024 * 1024,
        memory: Arc::clone(&memory),
        cancellation: RunCancellation::default(),
    }))
    .unwrap();
    let batches = futures_executor::block_on(stream.try_collect::<Vec<_>>()).unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].batch().header.row_count, 0);
    assert_eq!(
        batches[0].batch().record_batch().unwrap().schema().as_ref(),
        schema.as_ref()
    );
    drop(batches);
    assert_eq!(memory.snapshot().current_bytes, 0);
}
