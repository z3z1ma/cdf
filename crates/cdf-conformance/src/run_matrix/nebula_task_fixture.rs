use std::{collections::BTreeMap, path::Path, sync::Arc};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::Schema;
use cdf_kernel::{
    BackpressureSupport, Batch, BatchId, BatchStream, CapabilitySupport, CompiledScanIntent,
    CursorPosition, CursorValue, DeliveryGuarantee, ErrorKind, EstimateSupport,
    ExecutablePartition, FilterCapabilities, IncrementalShape, PartitionAttestation,
    PartitionAttestationAttempt, PartitionId, PartitionPlan, PartitionRetrySafety,
    PartitioningCapabilities, PayloadRetention, PlanId, PlannedPartitionReader,
    PlannedTaskSetReference, QueryableResource, ReplaySupport, ResourceCapabilities,
    ResourceDescriptor, ResourceStream, Result, ScanPlan, ScanRequest, ScopeKey, SourcePosition,
    TypePolicyAllowances,
};
use cdf_memory::{ConsumerKey, MemoryClass, ReservationRequest};
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
use cdf_task_store::{
    CanonicalTaskSetLimits, ExternalTaskParseMemory, ExternalTaskPlanningCodec,
    ExternalTaskSetCodec, ExternalTaskStore, RetainedExternalTask, TaskSetLimits,
    TypedCanonicalTaskSetBuilder, TypedExternalTaskSetReader, TypedExternalTaskSetReaderConfig,
};
use futures_util::stream;
use serde::{Deserialize, Serialize};

use super::MatrixDisposition;

const DRIVER_ID: &str = "nebula";
const TASK_SET_TYPE: &str = "nebula-catalog-task-v1";
const TASK_AUTHORITY_VERSION: u16 = 1;
const TASK_VERSION: u16 = 1;
const UPDATED_AT: i64 = 20;

pub(crate) fn resource(
    project_root: &Path,
    disposition: MatrixDisposition,
) -> Result<crate::source_fixture::ResolvedSourceFixture> {
    let mut registry = cdf_runtime::SourceRegistry::new();
    registry.register(NebulaSourceDriver::new()?)?;
    let document = cdf_declarative::parse_toml(&resource_toml(disposition))?;
    let mut resources = cdf_declarative::compile_document(&registry, &document)?;
    if resources.len() != 1 {
        return Err(cdf_kernel::CdfError::contract(format!(
            "Nebula source fixture expected one resource, found {}",
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
        panic!("Nebula source must checkpoint a cursor position");
    };
    assert_eq!(cursor.version, 1);
    assert_eq!(cursor.field, "updated_at");
    assert_eq!(cursor.value, CursorValue::I64(UPDATED_AT));
}

fn resource_toml(disposition: MatrixDisposition) -> String {
    let keys = merge_keys(disposition);
    format!(
        r#"
[source.nebula]
kind = "nebula"
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

struct NebulaSourceDriver {
    descriptor: SourceDriverDescriptor,
    option_schema: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ExternalSourceOptions {
    seed: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ExternalResourceOptions {
    rows: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ExternalPhysicalPlan {
    seed: u64,
    rows: u64,
}

impl NebulaSourceDriver {
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
                schemes: vec!["nebula".to_owned()],
            },
            option_schema,
        })
    }
}

impl SourceDriver for NebulaSourceDriver {
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
        output: &mut dyn cdf_runtime::SourceHealthSink,
    ) -> Result<()> {
        request.budget.consume_work(1)?;
        output.emit(SourceHealthResult {
            probe_id: "health".to_owned(),
            status: SourceHealthStatus::Passed,
            message: "Nebula source conformance probe passed".to_owned(),
            details: serde_json::json!({
                "compiled_resources": request.compiled_plans.len(),
            }),
        })
    }

    fn compile(&self, request: SourceCompileRequest) -> Result<CompiledSourcePlan> {
        request.context.validate()?;
        let source: ExternalSourceOptions = serde_json::from_value(serde_json::Value::Object(
            request.source_options.clone().into_iter().collect(),
        ))
        .map_err(|error| {
            cdf_kernel::CdfError::contract(format!(
                "decode Nebula source conformance options: {error}"
            ))
        })?;
        let resource: ExternalResourceOptions = serde_json::from_value(serde_json::Value::Object(
            request.resource_options.clone().into_iter().collect(),
        ))
        .map_err(|error| {
            cdf_kernel::CdfError::contract(format!(
                "decode Nebula resource conformance options: {error}"
            ))
        })?;
        let physical_plan = ExternalPhysicalPlan {
            seed: source.seed,
            rows: resource.rows,
        };
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
                physical_plan: serde_json::to_value(physical_plan).map_err(|error| {
                    cdf_kernel::CdfError::internal(format!(
                        "encode Nebula source conformance physical plan: {error}"
                    ))
                })?,
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
        context: &SourceResolutionContext<'_>,
    ) -> Result<Arc<dyn QueryableResource>> {
        let physical_plan: ExternalPhysicalPlan =
            serde_json::from_value(plan.physical_plan.clone()).map_err(|error| {
                cdf_kernel::CdfError::contract(format!(
                    "decode Nebula source conformance physical plan: {error}"
                ))
            })?;
        let task_store = ExternalTaskStore::new(
            context.artifact_root().join(".cdf"),
            cdf_kernel::ContentStoreNamespace::new("nebula-conformance")?,
        )?;
        Ok(Arc::new(NebulaResource {
            descriptor: plan.descriptor.clone(),
            schema: Arc::new(plan.schema.clone()),
            capabilities: plan.resource_capabilities.clone(),
            type_policy_allowances: plan.type_policy_allowances,
            effective_schema_runtime: plan.effective_schema_runtime.clone(),
            baseline_observation_schema_catalog: plan.baseline_observation_schema_catalog.clone(),
            compiled_source_plan_hash: plan.compiled_source_plan_hash()?,
            execution: context.execution().clone(),
            execution_capabilities: plan.execution_capabilities.clone(),
            physical_plan,
            task_store,
            cancellation: context.cancellation(),
        }))
    }
}

impl SourceAddPlanner for NebulaSourceDriver {
    fn propose_add(&self, request: &SourceAddRequest) -> Result<Option<SourceAddProposal>> {
        request.validate()?;
        if !request.location.starts_with("nebula://") {
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
            "nebula://events",
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

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct NebulaTaskAuthority {
    version: u16,
    snapshot: String,
    scope: ScopeKey,
    scan_intent: CompiledScanIntent,
}

impl NebulaTaskAuthority {
    fn validate(&self) -> Result<()> {
        if self.version != TASK_AUTHORITY_VERSION {
            return Err(cdf_kernel::CdfError::contract(format!(
                "Nebula task authority version {} is unsupported; expected {TASK_AUTHORITY_VERSION}",
                self.version
            )));
        }
        if self.snapshot.is_empty() || self.snapshot.chars().any(char::is_control) {
            return Err(cdf_kernel::CdfError::contract(
                "Nebula task authority snapshot must be nonempty and control-free",
            ));
        }
        self.scan_intent.validate()
    }

    fn content_sha256(&self) -> Result<String> {
        self.validate()?;
        artifact_hash(self)
    }

    fn encode_to(&self, output: &mut dyn std::io::Write) -> Result<()> {
        self.validate()?;
        serde_json::to_writer(output, self).map_err(|error| {
            cdf_kernel::CdfError::data(format!("encode Nebula task authority: {error}"))
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct NebulaRow {
    id: i64,
    name: String,
    updated_at: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct NebulaCatalogTask {
    version: u16,
    canonical_ordinal: u64,
    sort_key: String,
    rows: Vec<NebulaRow>,
}

impl NebulaCatalogTask {
    fn events() -> Self {
        Self {
            version: TASK_VERSION,
            canonical_ordinal: u64::MAX,
            sort_key: "events".to_owned(),
            rows: vec![
                NebulaRow {
                    id: 1,
                    name: "ada".to_owned(),
                    updated_at: 10,
                },
                NebulaRow {
                    id: 2,
                    name: "grace".to_owned(),
                    updated_at: UPDATED_AT,
                },
            ],
        }
    }

    fn validate_against(&self, authority: &NebulaTaskAuthority) -> Result<()> {
        authority.validate()?;
        if self.version != TASK_VERSION {
            return Err(cdf_kernel::CdfError::contract(format!(
                "Nebula task version {} is unsupported; expected {TASK_VERSION}",
                self.version
            )));
        }
        if self.sort_key != "events" {
            return Err(cdf_kernel::CdfError::data(
                "Nebula task sort key does not match its catalog selection",
            ));
        }
        if self.rows.is_empty()
            || self
                .rows
                .iter()
                .any(|row| row.name.is_empty() || row.name.chars().any(char::is_control))
        {
            return Err(cdf_kernel::CdfError::data(
                "Nebula task rows and names must be nonempty and control-free",
            ));
        }
        Ok(())
    }

    fn encode_to(&self, output: &mut dyn std::io::Write) -> Result<()> {
        serde_json::to_writer(output, self)
            .map_err(|error| cdf_kernel::CdfError::data(format!("encode Nebula task: {error}")))
    }
}

struct NebulaTaskCodec;

impl ExternalTaskSetCodec for NebulaTaskCodec {
    type Authority = NebulaTaskAuthority;
    type Task = NebulaCatalogTask;

    fn decode_authority(&self, payload: &[u8]) -> Result<Self::Authority> {
        let authority: NebulaTaskAuthority = serde_json::from_slice(payload).map_err(|error| {
            cdf_kernel::CdfError::data(format!("decode Nebula task authority: {error}"))
        })?;
        authority.validate()?;
        Ok(authority)
    }

    fn authority_content_sha256(&self, authority: &Self::Authority) -> Result<String> {
        authority.content_sha256()
    }

    fn decode_task(&self, payload: &[u8], authority: &Self::Authority) -> Result<Self::Task> {
        let task: NebulaCatalogTask = serde_json::from_slice(payload).map_err(|error| {
            cdf_kernel::CdfError::data(format!("decode Nebula catalog task: {error}"))
        })?;
        task.validate_against(authority)?;
        Ok(task)
    }

    fn task_canonical_ordinal(&self, task: &Self::Task) -> u64 {
        task.canonical_ordinal
    }

    fn encode_task(&self, task: &Self::Task, output: &mut dyn std::io::Write) -> Result<()> {
        task.encode_to(output)
    }
}

impl ExternalTaskPlanningCodec for NebulaTaskCodec {
    fn set_task_canonical_ordinal(&self, task: &mut Self::Task, ordinal: u64) {
        task.canonical_ordinal = ordinal;
    }

    fn encode_authority(
        &self,
        authority: &Self::Authority,
        output: &mut dyn std::io::Write,
    ) -> Result<()> {
        authority.encode_to(output)
    }
}

#[derive(Clone)]
struct NebulaExecutableTask {
    retained: RetainedExternalTask<NebulaTaskAuthority, NebulaCatalogTask>,
}

struct NebulaPlannedPartitionReader {
    reader: TypedExternalTaskSetReader<NebulaTaskCodec>,
    descriptor_resource_id: String,
    effective_schema_runtime: Option<cdf_kernel::EffectiveSchemaRuntime>,
}

impl NebulaPlannedPartitionReader {
    fn open(
        store: &ExternalTaskStore,
        reference: PlannedTaskSetReference,
        memory: Arc<dyn cdf_memory::MemoryCoordinator>,
        cancellation: cdf_runtime::RunCancellation,
        descriptor_resource_id: String,
        effective_schema_runtime: Option<cdf_kernel::EffectiveSchemaRuntime>,
    ) -> Result<Self> {
        let authority_parse = ExternalTaskParseMemory::blocking(
            "nebula-task-authority-parse",
            MemoryClass::Control,
            40_000,
            4096,
        )?;
        let task_parse = ExternalTaskParseMemory::blocking(
            "nebula-task-record-parse",
            MemoryClass::Control,
            40_000,
            4096,
        )?;
        let config = TypedExternalTaskSetReaderConfig::new(
            TASK_SET_TYPE,
            4096,
            4096,
            authority_parse,
            task_parse,
        )?;
        Ok(Self {
            reader: TypedExternalTaskSetReader::open(
                store,
                reference,
                memory,
                cancellation,
                config,
                NebulaTaskCodec,
            )?,
            descriptor_resource_id,
            effective_schema_runtime,
        })
    }
}

impl PlannedPartitionReader for NebulaPlannedPartitionReader {
    fn next_partition(&mut self, expected_ordinal: u64) -> Result<Option<ExecutablePartition>> {
        let Some(retained) = self.reader.next_task(expected_ordinal)? else {
            return Ok(None);
        };
        let task = retained.task();
        let authority = retained.authority();
        let updated_at = task
            .rows
            .iter()
            .map(|row| row.updated_at)
            .max()
            .ok_or_else(|| cdf_kernel::CdfError::data("Nebula task omitted catalog rows"))?;
        let position = SourcePosition::Cursor(CursorPosition {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            field: "updated_at".to_owned(),
            value: CursorValue::I64(updated_at),
        });
        let mut plan = PartitionPlan {
            partition_id: PartitionId::new(format!(
                "nebula-task-{:020}",
                retained.canonical_ordinal()
            ))?,
            scope: authority.scope.clone(),
            planned_position: Some(position),
            start_position: None,
            scan_intent: authority.scan_intent.clone(),
            retry_safety: PartitionRetrySafety::ImmutableContent,
            metadata: BTreeMap::from([(
                "cdf:external_task_sha256".to_owned(),
                retained.content_sha256().to_owned(),
            )]),
        };
        if let Some(runtime) = &self.effective_schema_runtime {
            cdf_kernel::bind_partition_schema_observation(
                &mut plan,
                runtime,
                &self.descriptor_resource_id,
            )?;
        }
        let retained_bytes = retained.retained_bytes();
        Ok(Some(ExecutablePartition::retained(
            plan,
            PayloadRetention::new(Arc::new(NebulaExecutableTask { retained }), retained_bytes)?,
        )))
    }
}

struct NebulaResource {
    descriptor: ResourceDescriptor,
    schema: Arc<Schema>,
    capabilities: ResourceCapabilities,
    type_policy_allowances: TypePolicyAllowances,
    effective_schema_runtime: Option<cdf_kernel::EffectiveSchemaRuntime>,
    baseline_observation_schema_catalog: Vec<cdf_kernel::EffectiveSchemaCatalogEntry>,
    compiled_source_plan_hash: cdf_kernel::CompiledSourcePlanHash,
    execution: cdf_runtime::ExecutionServices,
    execution_capabilities: SourceExecutionCapabilities,
    physical_plan: ExternalPhysicalPlan,
    task_store: ExternalTaskStore,
    cancellation: cdf_runtime::RunCancellation,
}

impl ResourceStream for NebulaResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn compiled_source_plan_hash(&self) -> Option<&cdf_kernel::CompiledSourcePlanHash> {
        Some(&self.compiled_source_plan_hash)
    }

    fn effective_schema_runtime(&self) -> Option<&cdf_kernel::EffectiveSchemaRuntime> {
        self.effective_schema_runtime.as_ref()
    }

    fn baseline_observation_schema_catalog(&self) -> &[cdf_kernel::EffectiveSchemaCatalogEntry] {
        &self.baseline_observation_schema_catalog
    }

    fn type_policy_allowances(&self) -> TypePolicyAllowances {
        self.type_policy_allowances
    }

    fn plan_partitions(&self, _request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        Err(cdf_kernel::CdfError::contract(
            "Nebula uses external canonical catalog tasks and must be planned through negotiate",
        ))
    }

    fn planned_partition_reader(
        &self,
        reference: &PlannedTaskSetReference,
    ) -> Result<Box<dyn PlannedPartitionReader>> {
        Ok(Box::new(NebulaPlannedPartitionReader::open(
            &self.task_store,
            reference.clone(),
            self.execution.memory(),
            self.cancellation.clone(),
            self.descriptor.resource_id.as_str().to_owned(),
            self.effective_schema_runtime.clone(),
        )?))
    }

    fn open(&self, _partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async {
            Err(cdf_kernel::CdfError::contract(
                "Nebula executes retained external catalog tasks; open an executable partition",
            ))
        }))
    }

    fn open_executable(
        &self,
        partition: ExecutablePartition,
    ) -> cdf_kernel::PartitionOpenAttempt<'_> {
        let retained = partition
            .retention()
            .and_then(PayloadRetention::downcast_ref::<NebulaExecutableTask>)
            .cloned();
        let Some(executable) = retained else {
            return cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async {
                Err(cdf_kernel::CdfError::contract(
                    "Nebula executable partition omitted its retained canonical task",
                ))
            }));
        };
        let partition = partition.into_plan();
        let resource_id = self.descriptor.resource_id.clone();
        let schema = Arc::clone(&self.schema);
        let execution = self.execution.clone();
        let execution_capabilities = self.execution_capabilities.clone();
        let physical_plan = self.physical_plan.clone();
        let cancellation = self.cancellation.clone();
        cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async move {
            execution.admit_source_operation(
                execution_capabilities
                    .quota_authority
                    .as_deref()
                    .ok_or_else(|| {
                        cdf_kernel::CdfError::internal(
                            "Nebula source conformance omitted its quota authority",
                        )
                    })?,
                execution_capabilities.rate_limit,
                cancellation,
            )?;
            let task = executable.retained.task();
            if physical_plan.rows != 2 {
                return Err(cdf_kernel::CdfError::internal(format!(
                    "Nebula source conformance physical plan requires 2 rows, received {}",
                    physical_plan.rows
                )));
            }
            let task_updated_at = task
                .rows
                .iter()
                .map(|row| row.updated_at)
                .max()
                .ok_or_else(|| cdf_kernel::CdfError::data("Nebula task omitted catalog rows"))?;
            let record_batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        task.rows.iter().map(|row| row.id).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        task.rows
                            .iter()
                            .map(|row| row.name.as_str())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        task.rows
                            .iter()
                            .map(|row| row.updated_at)
                            .collect::<Vec<_>>(),
                    )),
                ],
            )
            .map_err(|error| {
                cdf_kernel::CdfError::data(format!("build Nebula source batch: {error}"))
            })?;
            let retained_bytes = cdf_memory::record_batch_retained_bytes(&record_batch)?;
            let lease = cdf_memory::reserve(
                execution.memory(),
                ReservationRequest::new(
                    ConsumerKey::new("nebula-task-batch", MemoryClass::Source)?,
                    retained_bytes,
                )?,
            )
            .await?;
            let mut batch = Batch::from_record_batch(
                BatchId::new(format!(
                    "nebula-task-batch-{:06}-{:020}",
                    physical_plan.seed, task.canonical_ordinal
                ))?,
                resource_id,
                partition.partition_id,
                cdf_kernel::canonical_arrow_schema_hash(schema.as_ref())?,
                record_batch,
            )?
            .with_retention(PayloadRetention::new(Arc::new(lease), retained_bytes)?)?;
            batch.header.source_position = Some(SourcePosition::Cursor(CursorPosition {
                version: cdf_kernel::SOURCE_POSITION_VERSION,
                field: "updated_at".to_owned(),
                value: CursorValue::I64(task_updated_at),
            }));
            let stream = Box::pin(stream::iter([Ok(batch)])) as BatchStream;
            Ok(cdf_kernel::PartitionStreamPayload::batches(stream))
        }))
    }

    fn attest_partition(&self, partition: PartitionPlan) -> PartitionAttestationAttempt<'_> {
        let position = partition.planned_position;
        let schema = Arc::clone(&self.schema);
        PartitionAttestationAttempt::materialized(Box::pin(async move {
            let position = position.ok_or_else(|| {
                cdf_kernel::CdfError::internal(
                    "Nebula source conformance partition omitted its planned position",
                )
            })?;
            Ok(Some(PartitionAttestation::new(
                position,
                Some(cdf_kernel::canonical_arrow_schema_hash(schema.as_ref())?),
            )))
        }))
    }
}

impl QueryableResource for NebulaResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        &self.capabilities
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        if self.physical_plan.rows != 2 {
            return Err(cdf_kernel::CdfError::internal(format!(
                "Nebula source conformance physical plan requires 2 rows, received {}",
                self.physical_plan.rows
            )));
        }
        let mut builder = TypedCanonicalTaskSetBuilder::new(
            &self.task_store,
            TASK_SET_TYPE,
            CanonicalTaskSetLimits {
                tasks: TaskSetLimits {
                    maximum_task_bytes: 4096,
                    maximum_authority_bytes: 4096,
                    writer_buffer_bytes: 8192,
                },
                maximum_sort_key_bytes: 64,
                index_cache_bytes: 64 * 1024,
                spill_growth_bytes: 1024 * 1024,
                minimum_initial_spill_bytes: 1024 * 1024,
            },
            self.execution.memory(),
            self.execution.spill(),
            self.cancellation.clone(),
            NebulaTaskCodec,
        )?;
        // The shared canonical planner owns spill admission, duplicate handling, ordering, and
        // ordinal assignment; the source owns only its typed task and catalog selection key.
        let task = NebulaCatalogTask::events();
        if !builder.push_idempotent_by(task.clone(), |task| task.sort_key.as_bytes())?
            || builder.push_idempotent_by(task, |task| task.sort_key.as_bytes())?
        {
            return Err(cdf_kernel::CdfError::internal(
                "Nebula canonical planner did not suppress an identical catalog task",
            ));
        }
        let authority = NebulaTaskAuthority {
            version: TASK_AUTHORITY_VERSION,
            snapshot: "fixture-v1".to_owned(),
            scope: request.scope.clone(),
            scan_intent: CompiledScanIntent::full_scan(),
        };
        let artifact = builder.finalize(&authority)?;
        Ok(ScanPlan::from_partition_authority(
            PlanId::new("nebula-catalog-task-plan")?,
            request.clone(),
            cdf_kernel::PartitionAuthority::External(artifact.reference),
            Vec::new(),
            request.filters.clone(),
            Some(2),
            None,
            DeliveryGuarantee::AtLeastOnceDuplicateRisk,
        ))
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
        maximum_emitted_batch_bytes: 4096,
        maximum_concurrency: 1,
        useful_concurrency: 1,
        executor_class: SourceExecutorClass::Io,
        blocking_lane: None,
        pausable: true,
        spillable: false,
        idempotent_reads: true,
        reopenable: true,
        resumable: true,
        speculative_safe: true,
        retry_granularity: SourceRetryGranularity::Partition,
        retryable_errors: vec![ErrorKind::Transient],
        retry_policy: Some(cdf_runtime::SourceRetryPolicy::default()),
        attestation: SourceAttestationStrength::ImmutableContent,
        rate_limit: Some(cdf_runtime::SourceRateLimit {
            operations: 100,
            interval_ms: 1_000,
        }),
        quota_authority: Some("nebula-conformance-fixture".to_owned()),
        canonical_order: true,
        bounded: true,
        batch_memory: SourceBatchMemoryContract::Preaccounted,
        telemetry_version: "v1".to_owned(),
    }
}

struct NoSecrets;

impl cdf_http::SecretProvider for NoSecrets {
    fn resolve(&self, uri: &cdf_http::SecretUri) -> Result<cdf_http::SecretValue> {
        Err(cdf_kernel::CdfError::auth(format!(
            "Nebula source fixture has no secret for {uri}"
        )))
    }
}

#[test]
fn nebula_source_inherits_registry_schema_add_discovery_and_doctor_laws() {
    let driver = NebulaSourceDriver::new().unwrap();
    let mut registry = cdf_runtime::SourceRegistry::new();
    registry.register(driver).unwrap();
    assert!(registry.option_schemas().contains_key(DRIVER_ID));

    let add = registry
        .plan_add(
            SourceAddRequest {
                source_name: "nebula".to_owned(),
                resource_name: "events".to_owned(),
                location: "nebula://events".to_owned(),
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
    let changed_document = cdf_declarative::parse_toml(
        &resource_toml(MatrixDisposition::Append).replace("seed = 7", "seed = 8"),
    )
    .unwrap();
    let changed = cdf_declarative::compile_document(&registry, &changed_document)
        .unwrap()
        .remove(0);
    assert_ne!(
        compiled.source_plan().physical_plan_hash,
        changed.source_plan().physical_plan_hash
    );

    let invalid_document = cdf_declarative::parse_toml(
        &resource_toml(MatrixDisposition::Append).replace("rows = 2", "rows = 3"),
    )
    .unwrap();
    let invalid_error = cdf_declarative::compile_document(&registry, &invalid_document)
        .expect_err("registry option schema must reject invalid Nebula resource options");
    assert!(invalid_error.to_string().contains("rows"));
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
        .health_checks(
            &context,
            &[compiled.source_plan().clone()],
            &[],
            cdf_runtime::SourceHealthLimits::default(),
            cdf_runtime::RunCancellation::default(),
        )
        .unwrap();
    assert_eq!(health.len(), 1);
    assert_eq!(health[0].status, SourceHealthStatus::Passed);
}

#[test]
fn nebula_source_inherits_generic_plan_run_receipt_checkpoint_and_replay_laws() {
    let admitted_before = crate::test_execution_services()
        .scheduler_report()
        .unwrap()
        .source_rate_admission
        .admitted_operations;
    let environment = crate::destination_catalog::ConformanceEnvironment::local_only();
    let executed = super::core::execute_cell(
        super::RunMatrixCell::new(
            super::SourceArchetype::nebula(),
            super::MatrixDestination::new("duckdb").unwrap(),
            MatrixDisposition::Append,
        ),
        &environment,
    )
    .unwrap();
    assert_eq!(executed.row_count, 2);
    assert!(executed.plan_honesty_asserted);
    assert!(executed.package_verified);
    assert!(executed.destination_receipt_verified);
    assert!(executed.checkpoint_gated_after_receipt_verification);
    assert!(executed.artifact_replay_identity_asserted);
    assert!(
        executed.runtime_scheduler.source_rate_admission.authorities >= 1,
        "the process-shared conformance host must retain the Nebula source authority"
    );
    assert!(
        executed
            .runtime_scheduler
            .source_rate_admission
            .admitted_operations
            > admitted_before,
        "the Nebula source run must add an admitted operation to the process-shared report"
    );
}
