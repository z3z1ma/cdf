use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
};

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use cdf_http::{HttpRequest, HttpResponse, HttpTransport};
use cdf_kernel::{
    BackpressureSupport, Batch, BatchId, BatchStream, CdfError, PartitionId, PartitionPlan,
    ResourceCapabilities, ResourceDescriptor, ResourceId, ResourceStream, Result as CdfResult,
    ScanRequest, SchemaSnapshotReference, SchemaSource, ScopeKey, TrustLevel, WriteDisposition,
    canonical_arrow_schema_hash,
};
use cdf_runtime::{
    CompiledSourcePlan, CompiledSourcePlanInput, SourceAttestationStrength,
    SourceBatchMemoryContract, SourceDriverDescriptor, SourceDriverId, SourceExecutionCapabilities,
    SourceExecutorClass, SourceRetryGranularity, artifact_hash,
};
use futures_util::stream;

use crate::{BenchResult, bench_error};

#[derive(Clone)]
pub(crate) struct MemoryResource {
    descriptor: ResourceDescriptor,
    schema: SchemaRef,
    partition: PartitionPlan,
    batches: Vec<Batch>,
    compiled_source_plan: CompiledSourcePlan,
    compiled_source_plan_hash: cdf_kernel::CompiledSourcePlanHash,
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
        let schema_hash = canonical_arrow_schema_hash(schema.as_ref())?;
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
            planned_position: None,
            start_position: None,
            scan_intent: cdf_kernel::CompiledScanIntent::full_scan(),
            retry_safety: cdf_kernel::PartitionRetrySafety::Forbidden,
            metadata: BTreeMap::from([("kind".to_owned(), "memory".to_owned())]),
        };
        let compiled_source_plan =
            Self::compile_source_plan(&descriptor, schema.as_ref(), &partition, &cdf_batches)?;
        let compiled_source_plan_hash = compiled_source_plan.compiled_source_plan_hash()?;
        Ok(Self {
            descriptor,
            schema,
            partition,
            batches: cdf_batches,
            compiled_source_plan,
            compiled_source_plan_hash,
        })
    }

    pub(crate) fn compiled_source_plan(&self) -> &CompiledSourcePlan {
        &self.compiled_source_plan
    }

    fn compile_source_plan(
        descriptor: &ResourceDescriptor,
        schema: &Schema,
        partition: &PartitionPlan,
        batches: &[Batch],
    ) -> BenchResult<CompiledSourcePlan> {
        let maximum_batch_bytes = batches
            .iter()
            .map(|batch| {
                let record_batch = batch.record_batch().ok_or_else(|| {
                    bench_error("benchmark memory source requires materialized Arrow batches")
                })?;
                cdf_memory::record_batch_retained_bytes(record_batch)?
                    .checked_add(batch.header.pre_contract_evidence_retained_bytes()?)
                    .ok_or_else(|| bench_error("benchmark memory batch retained bytes overflow"))
            })
            .collect::<BenchResult<Vec<_>>>()?
            .into_iter()
            .max()
            .unwrap_or(1)
            .max(1);
        let resource_capabilities = ResourceCapabilities {
            idempotent_reads: true,
            backpressure: BackpressureSupport::Pausable,
            ..ResourceCapabilities::default()
        };
        CompiledSourcePlan::new(
            SourceDriverDescriptor {
                driver_id: SourceDriverId::new("benchmark_memory")?,
                driver_version: "benchmark-memory-v1".to_owned(),
                option_schema_hash: artifact_hash(&serde_json::json!({
                    "driver": "benchmark_memory",
                    "version": 1,
                }))?,
                kinds: vec!["benchmark_memory".to_owned()],
                schemes: vec!["memory".to_owned()],
            },
            resource_capabilities,
            SourceExecutionCapabilities {
                minimum_poll_bytes: 1,
                maximum_poll_bytes: maximum_batch_bytes,
                minimum_decode_bytes: 1,
                maximum_decode_bytes: maximum_batch_bytes,
                maximum_emitted_batch_bytes: maximum_batch_bytes,
                maximum_concurrency: 1,
                useful_concurrency: 1,
                executor_class: SourceExecutorClass::Cpu,
                blocking_lane: None,
                pausable: true,
                spillable: false,
                idempotent_reads: true,
                reopenable: true,
                resumable: false,
                speculative_safe: false,
                retry_granularity: SourceRetryGranularity::None,
                retryable_errors: Vec::new(),
                retry_policy: None,
                attestation: SourceAttestationStrength::None,
                rate_limit: None,
                quota_authority: None,
                canonical_order: true,
                bounded: true,
                batch_memory: SourceBatchMemoryContract::FrontierReserved,
                telemetry_version: "benchmark-memory-v1".to_owned(),
            },
            CompiledSourcePlanInput {
                descriptor: descriptor.clone(),
                schema: schema.clone(),
                type_policy_allowances: Default::default(),
                effective_schema_runtime: None,
                baseline_observation_schema_catalog: Vec::new(),
                redacted_options: serde_json::json!({"kind": "benchmark_memory"}),
                physical_plan: serde_json::json!({
                    "kind": "benchmark_memory",
                    "partition_id": partition.partition_id.as_str(),
                    "batch_count": batches.len(),
                    "maximum_batch_bytes": maximum_batch_bytes,
                }),
            },
        )
        .map_err(Into::into)
    }
}

impl ResourceStream for MemoryResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn compiled_source_plan_hash(&self) -> Option<&cdf_kernel::CompiledSourcePlanHash> {
        Some(&self.compiled_source_plan_hash)
    }

    fn plan_partitions(&self, request: &ScanRequest) -> CdfResult<Vec<PartitionPlan>> {
        if request.resource_id != self.descriptor.resource_id {
            return Err(CdfError::contract("benchmark memory resource id mismatch"));
        }
        Ok(vec![self.partition.clone()])
    }

    fn open(&self, partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        let expected = self.partition.clone();
        let batches = self.batches.clone();
        cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async move {
            if partition.partition_id != expected.partition_id || partition.scope != expected.scope
            {
                return Err(CdfError::contract(
                    "benchmark memory resource partition mismatch",
                ));
            }
            let stream = Box::pin(stream::iter(batches.into_iter().map(Ok))) as BatchStream;
            Ok(cdf_kernel::PartitionStreamPayload::batches(stream))
        }))
    }
}

#[derive(Clone)]
pub(crate) struct FixtureTransport {
    state: Arc<Mutex<VecDeque<Vec<u8>>>>,
}

impl FixtureTransport {
    pub(crate) fn new(response_body: Vec<u8>) -> Self {
        Self {
            state: Arc::new(Mutex::new(VecDeque::from([response_body]))),
        }
    }
}

impl HttpTransport for FixtureTransport {
    fn send(
        &self,
        _request: HttpRequest,
        budget: cdf_http::HttpResponseBudget,
    ) -> cdf_kernel::BoxFuture<'_, CdfResult<HttpResponse>> {
        Box::pin(async move {
            let body = self
                .state
                .lock()
                .map_err(|_| CdfError::internal("fixture transport mutex poisoned"))?
                .pop_front()
                .ok_or_else(|| CdfError::internal("fixture transport exhausted responses"))?;
            Ok(HttpResponse::new(200).with_body(budget.account_body(body).await?))
        })
    }
}
