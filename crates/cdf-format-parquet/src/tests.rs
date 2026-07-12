use std::{collections::BTreeMap, sync::Arc};

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
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
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

    let units = futures_executor::block_on(driver.plan_decode_units(
        Arc::clone(&source),
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
    assert_eq!(units.len(), 1);

    let stream = futures_executor::block_on(driver.decode(
        source,
        PhysicalDecodeRequest {
            options: serde_json::json!({}),
            unit: units[0].clone(),
            resource_id: ResourceId::new("fixture.parquet").unwrap(),
            partition_id: PartitionId::new("file-000001").unwrap(),
            batch_id_prefix: "fixture".to_owned(),
            physical_schema: schema,
            source_position: None,
            projection: None,
            predicates: Vec::new(),
            target_batch_rows: 64 * 1024,
            target_batch_bytes: 8 * 1024 * 1024,
            memory: Arc::clone(&memory),
            cancellation: RunCancellation::default(),
        },
    ))
    .unwrap();
    let batches = futures_executor::block_on(stream.try_collect::<Vec<_>>()).unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].batch().header.row_count, 4);
    drop(batches);
    assert_eq!(memory.snapshot().current_bytes, 0);
}
