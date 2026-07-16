use std::{collections::BTreeMap, path::Path, sync::Arc};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::Schema;
use cdf_kernel::{
    BackpressureSupport, Batch, BatchId, BatchStream, CapabilitySupport, CursorPosition,
    CursorValue, DeliveryGuarantee, EstimateSupport, FilterCapabilities, IncrementalShape,
    PartitionId, PartitionPlan, PartitionRetrySafety, PartitioningCapabilities, PlanId,
    QueryableResource, ReplaySupport, ResourceCapabilities, ResourceDescriptor, ResourceStream,
    Result, ScanPlan, ScanRequest, SourcePosition, TypePolicyAllowances,
};
use cdf_project::ProjectRunReport;
use cdf_runtime::{
    CompiledSourcePlan, CompiledSourcePlanInput, SourceAddPlanner, SourceAddProposal,
    SourceAddRequest, SourceAttestationStrength, SourceBatchMemoryContract, SourceCompileRequest,
    SourceDiscoveryCandidate, SourceDiscoveryKind, SourceDiscoveryRequest, SourceDiscoverySession,
    SourceDriver, SourceDriverDescriptor, SourceDriverId, SourceEvidenceLocation,
    SourceExecutionCapabilities, SourceExecutorClass, SourceHealthRequest, SourceHealthResult,
    SourceHealthStatus, SourceResolutionContext, SourceRetryGranularity, SourceSchemaObservation,
    artifact_hash,
};
use futures_util::stream;

use super::MatrixDisposition;

const DRIVER_ID: &str = "external_mock";
const UPDATED_AT: i64 = 20;

pub(crate) fn resource(
    project_root: &Path,
    disposition: MatrixDisposition,
) -> Result<crate::source_fixture::ResolvedSourceFixture> {
    let mut registry = cdf_runtime::SourceRegistry::new();
    registry.register(ExternalMockSourceDriver::new()?)?;
    let document = cdf_declarative::parse_toml(&resource_toml(disposition))?;
    let mut resources = cdf_declarative::compile_document(&registry, &document)?;
    if resources.len() != 1 {
        return Err(cdf_kernel::CdfError::contract(format!(
            "external source fixture expected one resource, found {}",
            resources.len()
        )));
    }
    let compiled = resources.remove(0);
    let execution = crate::test_execution_services();
    let context = SourceResolutionContext::new(
        project_root,
        Arc::new(NoSecrets),
        &execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    );
    crate::source_fixture::ResolvedSourceFixture::resolve(&compiled, &registry, &context)
}

pub(crate) fn assert_source_position(report: &ProjectRunReport) {
    let SourcePosition::Cursor(cursor) = &report.checkpoint.delta.output_position else {
        panic!("external source must checkpoint a cursor position");
    };
    assert_eq!(cursor.version, 1);
    assert_eq!(cursor.field, "updated_at");
    assert_eq!(cursor.value, CursorValue::I64(UPDATED_AT));
}

fn resource_toml(disposition: MatrixDisposition) -> String {
    let keys = merge_keys(disposition);
    format!(
        r#"
[source.external]
kind = "external_mock"
seed = 7

[resource.events]
rows = 2
{keys}
cursor = {{ field = "updated_at", ordering = "exact", lag = "0ms" }}
write_disposition = "{}"
trust = "governed"
schema = {{ fields = [
  {{ name = "id", type = "int64", nullable = false }},
  {{ name = "name", type = "string", nullable = false }},
  {{ name = "updated_at", type = "int64", nullable = false }},
] }}
"#,
        disposition.as_str()
    )
}

fn merge_keys(disposition: MatrixDisposition) -> &'static str {
    if disposition == MatrixDisposition::Merge {
        "primary_key = [\"id\"]\nmerge_key = [\"id\"]"
    } else {
        ""
    }
}

struct ExternalMockSourceDriver {
    descriptor: SourceDriverDescriptor,
    option_schema: serde_json::Value,
}

impl ExternalMockSourceDriver {
    fn new() -> Result<Self> {
        let option_schema = serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "source": {
                "type": "object",
                "additionalProperties": false,
                "properties": {"seed": {"type": "integer", "minimum": 0}},
                "required": ["seed"]
            },
            "resource": {
                "type": "object",
                "additionalProperties": false,
                "properties": {"rows": {"type": "integer", "const": 2}},
                "required": ["rows"]
            }
        });
        Ok(Self {
            descriptor: SourceDriverDescriptor {
                driver_id: SourceDriverId::new(DRIVER_ID)?,
                driver_version: "1.0.0".to_owned(),
                option_schema_hash: artifact_hash(&option_schema)?,
                kinds: vec![DRIVER_ID.to_owned()],
                schemes: vec!["external-mock".to_owned()],
            },
            option_schema,
        })
    }
}

impl SourceDriver for ExternalMockSourceDriver {
    fn descriptor(&self) -> &SourceDriverDescriptor {
        &self.descriptor
    }

    fn option_schema(&self) -> &serde_json::Value {
        &self.option_schema
    }

    fn add_planner(&self) -> Option<&dyn SourceAddPlanner> {
        Some(self)
    }

    fn health(
        &self,
        request: SourceHealthRequest,
        _context: &SourceResolutionContext<'_>,
    ) -> Result<Vec<SourceHealthResult>> {
        Ok(vec![SourceHealthResult {
            probe_id: "health".to_owned(),
            status: SourceHealthStatus::Passed,
            message: "external source conformance probe passed".to_owned(),
            details: serde_json::json!({
                "compiled_resources": request.compiled_plans.len(),
            }),
        }])
    }

    fn compile(&self, request: SourceCompileRequest) -> Result<CompiledSourcePlan> {
        request.context.validate()?;
        CompiledSourcePlan::new(
            self.descriptor.clone(),
            resource_capabilities(),
            execution_capabilities(),
            CompiledSourcePlanInput {
                descriptor: request.descriptor,
                schema: request.schema,
                type_policy_allowances: request.type_policy_allowances,
                effective_schema_runtime: request.effective_schema_runtime,
                baseline_observation_schema_catalog: request.baseline_observation_schema_catalog,
                redacted_options: serde_json::json!({
                    "source": request.source_options,
                    "resource": request.resource_options,
                }),
                physical_plan: serde_json::json!({
                    "seed": 7,
                    "rows": 2,
                }),
            },
        )
    }

    fn discovery_session(
        &self,
        plan: &CompiledSourcePlan,
        _context: &SourceResolutionContext<'_>,
    ) -> Result<Box<dyn SourceDiscoverySession>> {
        Ok(Box::new(ExternalDiscoverySession {
            schema: plan.schema.clone(),
        }))
    }

    fn resolve(
        &self,
        plan: &CompiledSourcePlan,
        _context: &SourceResolutionContext<'_>,
    ) -> Result<Arc<dyn QueryableResource>> {
        Ok(Arc::new(ExternalMockResource {
            descriptor: plan.descriptor.clone(),
            schema: Arc::new(plan.schema.clone()),
            capabilities: plan.resource_capabilities.clone(),
            type_policy_allowances: plan.type_policy_allowances,
            effective_schema_runtime: plan.effective_schema_runtime.clone(),
            compiled_source_plan_hash: artifact_hash(plan)?,
        }))
    }
}

impl SourceAddPlanner for ExternalMockSourceDriver {
    fn propose_add(&self, request: &SourceAddRequest) -> Result<Option<SourceAddProposal>> {
        if !request.location.starts_with("external-mock://") {
            return Ok(None);
        }
        Ok(Some(SourceAddProposal {
            source_kind: DRIVER_ID.to_owned(),
            source_options: BTreeMap::from([("seed".to_owned(), serde_json::json!(7))]),
            resource_options: BTreeMap::from([("rows".to_owned(), serde_json::json!(2))]),
            cursor: None,
            display_location: SourceEvidenceLocation::from_operational(&request.location)?,
            display_selection: request.resource_name.clone(),
            private_files: Vec::new(),
        }))
    }
}

struct ExternalDiscoverySession {
    schema: Schema,
}

impl SourceDiscoverySession for ExternalDiscoverySession {
    fn kind(&self) -> SourceDiscoveryKind {
        SourceDiscoveryKind::SchemaMetadata
    }

    fn candidates(&self) -> Result<Vec<SourceDiscoveryCandidate>> {
        Ok(vec![SourceDiscoveryCandidate::new(
            "external-mock://events",
            Some(2),
            None,
            BTreeMap::from([("snapshot".to_owned(), "fixture-v1".to_owned())]),
        )?])
    }

    fn observe(
        &self,
        candidate: &SourceDiscoveryCandidate,
        request: &SourceDiscoveryRequest,
    ) -> Result<SourceSchemaObservation> {
        request.validate()?;
        SourceSchemaObservation::new(
            candidate,
            self.schema.clone(),
            BTreeMap::from([("snapshot".to_owned(), "fixture-v1".to_owned())]),
            0,
            0,
        )
    }
}

struct ExternalMockResource {
    descriptor: ResourceDescriptor,
    schema: Arc<Schema>,
    capabilities: ResourceCapabilities,
    type_policy_allowances: TypePolicyAllowances,
    effective_schema_runtime: Option<cdf_kernel::EffectiveSchemaRuntime>,
    compiled_source_plan_hash: String,
}

impl ResourceStream for ExternalMockResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn compiled_source_plan_hash(&self) -> Option<&str> {
        Some(&self.compiled_source_plan_hash)
    }

    fn effective_schema_runtime(&self) -> Option<&cdf_kernel::EffectiveSchemaRuntime> {
        self.effective_schema_runtime.as_ref()
    }

    fn type_policy_allowances(&self) -> TypePolicyAllowances {
        self.type_policy_allowances
    }

    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        Ok(vec![PartitionPlan {
            partition_id: PartitionId::new("external-mock-000000")?,
            scope: request.scope.clone(),
            planned_position: None,
            start_position: None,
            scan_intent: cdf_kernel::CompiledScanIntent::full_scan(),
            retry_safety: PartitionRetrySafety::Forbidden,
            metadata: BTreeMap::new(),
        }])
    }

    fn open(&self, partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        let resource_id = self.descriptor.resource_id.clone();
        let schema = Arc::clone(&self.schema);
        cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async move {
            let record_batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![1, 2])),
                    Arc::new(StringArray::from(vec!["ada", "grace"])),
                    Arc::new(Int64Array::from(vec![10, UPDATED_AT])),
                ],
            )
            .map_err(|error| {
                cdf_kernel::CdfError::data(format!("build external source batch: {error}"))
            })?;
            let mut batch = Batch::from_record_batch(
                BatchId::new("external-mock-batch-000000")?,
                resource_id,
                partition.partition_id,
                cdf_kernel::canonical_arrow_schema_hash(schema.as_ref())?,
                record_batch,
            )?;
            batch.header.source_position = Some(SourcePosition::Cursor(CursorPosition {
                version: 1,
                field: "updated_at".to_owned(),
                value: CursorValue::I64(UPDATED_AT),
            }));
            let stream = Box::pin(stream::iter([Ok(batch)])) as BatchStream;
            Ok(cdf_kernel::PartitionStreamPayload::batches(stream))
        }))
    }
}

impl QueryableResource for ExternalMockResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        &self.capabilities
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        Ok(ScanPlan {
            plan_id: PlanId::new("external-mock-plan")?,
            request: request.clone(),
            partitions: self.plan_partitions(request)?,
            pushed_predicates: Vec::new(),
            unsupported_predicates: request.filters.clone(),
            estimated_rows: Some(2),
            estimated_bytes: None,
            delivery_guarantee: DeliveryGuarantee::AtLeastOnceDuplicateRisk,
        })
    }
}

fn resource_capabilities() -> ResourceCapabilities {
    ResourceCapabilities {
        projection: CapabilitySupport::Unsupported,
        filters: FilterCapabilities::default(),
        limits: CapabilitySupport::Unsupported,
        ordering: CapabilitySupport::Unsupported,
        partitioning: PartitioningCapabilities::default(),
        incremental: IncrementalShape::Cursor,
        replay: ReplaySupport::None,
        idempotent_reads: true,
        backpressure: BackpressureSupport::Pausable,
        estimates: EstimateSupport::Rows,
    }
}

fn execution_capabilities() -> SourceExecutionCapabilities {
    SourceExecutionCapabilities {
        minimum_poll_bytes: 1,
        maximum_poll_bytes: 1024,
        minimum_decode_bytes: 1,
        maximum_decode_bytes: 4096,
        maximum_concurrency: 1,
        useful_concurrency: 1,
        executor_class: SourceExecutorClass::Io,
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
        attestation: SourceAttestationStrength::ImmutableContent,
        rate_limit: Some(cdf_runtime::SourceRateLimit {
            operations: 100,
            interval_ms: 1_000,
        }),
        quota_authority: Some("external-mock-fixture".to_owned()),
        canonical_order: true,
        bounded: true,
        batch_memory: SourceBatchMemoryContract::FrontierReserved,
        telemetry_version: "v1".to_owned(),
    }
}

struct NoSecrets;

impl cdf_http::SecretProvider for NoSecrets {
    fn resolve(&self, uri: &cdf_http::SecretUri) -> Result<cdf_http::SecretValue> {
        Err(cdf_kernel::CdfError::auth(format!(
            "external source fixture has no secret for {uri}"
        )))
    }
}

#[test]
fn external_source_inherits_registry_schema_add_discovery_and_doctor_laws() {
    let driver = ExternalMockSourceDriver::new().unwrap();
    let mut registry = cdf_runtime::SourceRegistry::new();
    registry.register(driver).unwrap();
    assert!(registry.option_schemas().contains_key(DRIVER_ID));

    let add = registry
        .plan_add(
            SourceAddRequest {
                source_name: "external".to_owned(),
                resource_name: "events".to_owned(),
                location: "external-mock://events".to_owned(),
                project_root: Path::new(".").to_path_buf(),
                current_dir: Path::new(".").to_path_buf(),
                options: BTreeMap::new(),
                project_options: None,
            },
            &BTreeMap::new(),
        )
        .unwrap();
    assert_eq!(add.driver.driver_id.as_str(), DRIVER_ID);

    let compiled_document =
        cdf_declarative::parse_toml(&resource_toml(MatrixDisposition::Append)).unwrap();
    let compiled = cdf_declarative::compile_document(&registry, &compiled_document)
        .unwrap()
        .remove(0);
    let execution = crate::test_execution_services();
    let context = SourceResolutionContext::new(
        Path::new("."),
        Arc::new(NoSecrets),
        &execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    );
    let discovery = registry
        .discovery_session(compiled.source_plan(), &context)
        .unwrap();
    let candidates = discovery.candidates().unwrap();
    let observation = discovery
        .observe(&candidates[0], &SourceDiscoveryRequest::new(1, 1).unwrap())
        .unwrap();
    assert_eq!(observation.schema, *compiled.schema().as_ref());

    let health = registry
        .health_checks(&context, &[compiled.source_plan().clone()])
        .unwrap();
    assert_eq!(health.len(), 1);
    assert_eq!(health[0].status, SourceHealthStatus::Passed);
}

#[test]
fn external_source_inherits_generic_plan_run_receipt_checkpoint_and_replay_laws() {
    let executed = super::core::execute_cell(
        super::RunMatrixCell::new(
            super::SourceArchetype::external_mock(),
            super::MatrixDestination::DuckDb,
            MatrixDisposition::Append,
        ),
        None,
    )
    .unwrap();
    assert_eq!(executed.row_count, 2);
    assert!(executed.plan_honesty_asserted);
    assert!(executed.package_verified);
    assert!(executed.destination_receipt_verified);
    assert!(executed.checkpoint_gated_after_receipt_verification);
    assert!(executed.artifact_replay_identity_asserted);
}
