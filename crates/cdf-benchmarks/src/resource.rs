use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use cdf_formats::schema_hash;
use cdf_http::{HttpRequest, HttpResponse, HttpTransport};
use cdf_kernel::{
    Batch, BatchId, BatchStream, CdfError, PartitionId, PartitionPlan, ResourceDescriptor,
    ResourceId, ResourceStream, Result as CdfResult, ScanRequest, SchemaSnapshotReference,
    SchemaSource, ScopeKey, TrustLevel, WriteDisposition,
};
use futures_util::stream;

use crate::{BenchResult, bench_error};

#[derive(Clone)]
pub(crate) struct MemoryResource {
    descriptor: ResourceDescriptor,
    schema: SchemaRef,
    partition: PartitionPlan,
    batches: Vec<Batch>,
}

impl MemoryResource {
    pub(crate) fn from_record_batches(
        resource_id: &str,
        partition_id: &str,
        batches: Vec<RecordBatch>,
    ) -> BenchResult<Self> {
        let schema = batches
            .first()
            .map(RecordBatch::schema)
            .ok_or_else(|| bench_error("memory resource requires at least one batch"))?;
        let schema_hash = schema_hash(schema.as_ref())?;
        let resource_id = ResourceId::new(resource_id)?;
        let partition_id = PartitionId::new(partition_id)?;
        let descriptor = ResourceDescriptor {
            resource_id: resource_id.clone(),
            schema_source: SchemaSource::Discovered {
                snapshot: SchemaSnapshotReference {
                    schema_hash: schema_hash.clone(),
                    path: format!(".cdf/schemas/{resource_id}@{schema_hash}.json"),
                    metadata: BTreeMap::from([("probe".to_owned(), "benchmark".to_owned())]),
                },
            },
            primary_key: vec!["id".to_owned()],
            merge_key: vec!["id".to_owned()],
            cursor: None,
            write_disposition: WriteDisposition::Append,
            deduplication: None,
            contract: None,
            state_scope: ScopeKey::Resource,
            freshness: None,
            trust_level: TrustLevel::Governed,
        };
        let mut cdf_batches = Vec::with_capacity(batches.len());
        for (index, batch) in batches.into_iter().enumerate() {
            cdf_batches.push(Batch::from_record_batch(
                BatchId::new(format!("bench-batch-{:06}", index + 1))?,
                resource_id.clone(),
                partition_id.clone(),
                schema_hash.clone(),
                batch,
            )?);
        }
        let partition = PartitionPlan {
            partition_id,
            scope: ScopeKey::Resource,
            start_position: None,
            metadata: BTreeMap::from([("kind".to_owned(), "memory".to_owned())]),
        };
        Ok(Self {
            descriptor,
            schema,
            partition,
            batches: cdf_batches,
        })
    }

    pub(crate) fn from_batches(
        descriptor: ResourceDescriptor,
        partition_id: PartitionId,
        scope: ScopeKey,
        batches: Vec<Batch>,
    ) -> BenchResult<Self> {
        let schema = batches
            .first()
            .and_then(|batch| batch.record_batch())
            .map(RecordBatch::schema)
            .ok_or_else(|| bench_error("format read did not produce an Arrow batch"))?;
        let partition = PartitionPlan {
            partition_id,
            scope,
            start_position: None,
            metadata: BTreeMap::from([("kind".to_owned(), "format_read".to_owned())]),
        };
        Ok(Self {
            descriptor,
            schema,
            partition,
            batches,
        })
    }
}

impl ResourceStream for MemoryResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn plan_partitions(&self, request: &ScanRequest) -> CdfResult<Vec<PartitionPlan>> {
        if request.resource_id != self.descriptor.resource_id {
            return Err(CdfError::contract("benchmark memory resource id mismatch"));
        }
        Ok(vec![self.partition.clone()])
    }

    fn open(&self, partition: PartitionPlan) -> cdf_kernel::BoxFuture<'_, CdfResult<BatchStream>> {
        let expected = self.partition.clone();
        let batches = self.batches.clone();
        Box::pin(async move {
            if partition.partition_id != expected.partition_id || partition.scope != expected.scope
            {
                return Err(CdfError::contract(
                    "benchmark memory resource partition mismatch",
                ));
            }
            Ok(Box::pin(stream::iter(batches.into_iter().map(Ok))) as BatchStream)
        })
    }
}

#[derive(Clone)]
pub(crate) struct FixtureTransport {
    state: Arc<Mutex<VecDeque<HttpResponse>>>,
}

impl FixtureTransport {
    pub(crate) fn new(response_body: Vec<u8>) -> Self {
        Self {
            state: Arc::new(Mutex::new(VecDeque::from([
                HttpResponse::new(200).with_body(response_body)
            ]))),
        }
    }
}

impl HttpTransport for FixtureTransport {
    fn send(&self, _request: HttpRequest) -> CdfResult<HttpResponse> {
        self.state
            .lock()
            .map_err(|_| CdfError::internal("fixture transport mutex poisoned"))?
            .pop_front()
            .ok_or_else(|| CdfError::internal("fixture transport exhausted responses"))
    }
}
