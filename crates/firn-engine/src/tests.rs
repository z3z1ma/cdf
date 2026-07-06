use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use arrow_array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use firn_contract::{ContractPolicy, ObservedSchema, compile_validation_program};
use firn_kernel::{
    BackpressureSupport, Batch, BatchHeader, BatchId, BatchStats, BatchStream, CapabilitySupport,
    ContractRef, DeliveryGuarantee, EstimateSupport, FilterCapabilities, FreshnessSpec,
    IncrementalShape, PartitionId, PartitionPlan, PartitioningCapabilities, PredicateId,
    PushdownFidelity, QueryableResource, ResourceCapabilities, ResourceDescriptor, ResourceId,
    ResourceStream, Result, RunId, ScanPlan, ScanPredicate, ScanRequest, SchemaHash, SchemaSource,
    ScopeKey, TrustLevel, WriteDisposition, source_name,
};
use firn_package::PackageStatus;
use futures_executor::block_on;
use futures_util::stream;
use tempfile::TempDir;
use tracing::{
    Event, Id, Metadata, Subscriber,
    field::{Field as TracingField, Visit},
    span::{Attributes, Record},
};

use super::*;

#[test]
fn tier_a_resource_runs_engine_projection_filter_limit_into_package() {
    let resource = MockResource::tier_a(sample_batches());
    let input = plan_input(
        vec!["id > 1", "active = true"],
        Some(vec!["name".to_owned()]),
        Some(1),
        PlanBoundedness::Bounded,
    );
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();

    assert_eq!(plan.explain.pushed_predicates, Vec::new());
    assert_eq!(plan.explain.unsupported_predicates.len(), 2);

    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.manifest.lifecycle.status, PackageStatus::Packaged);
    assert_eq!(output.profile.output_rows, 1);
    assert!(output.profile.output_bytes > 0);
    assert_eq!(output.segments.len(), 1);

    let reader = firn_package::PackageReader::open(temp.path()).unwrap();
    let batches = reader.read_segment(&output.segments[0].segment_id).unwrap();
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.schema().field(0).name(), "name");
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "two");
}

#[test]
fn residual_limit_is_consumed_across_partitions() {
    let resource = MockResource::tier_b(sample_batches());
    let input = plan_input(
        vec!["active = true"],
        Some(vec!["name".to_owned()]),
        Some(1),
        PlanBoundedness::Bounded,
    );
    let plan = Planner::new().plan_tier_b(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 1);
    assert_eq!(output.profile.output_batches, 1);
    assert_eq!(output.segments.len(), 1);
}

#[test]
fn tier_b_negotiates_pushdown_fidelity_without_io() {
    let resource = MockResource::tier_b(sample_batches());
    let input = plan_input(
        vec!["id > 1", "active = true", "name != 'missing'"],
        Some(vec!["name".to_owned()]),
        Some(10),
        PlanBoundedness::Bounded,
    );
    let plan = Planner::new().plan_tier_b(&resource, input).unwrap();

    assert_eq!(resource.negotiate_count.load(Ordering::SeqCst), 1);
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
    assert_eq!(plan.scan.pushed_predicates.len(), 2);
    assert_eq!(
        plan.scan.pushed_predicates[0].fidelity,
        PushdownFidelity::Exact
    );
    assert_eq!(
        datafusion_filter_pushdown(&plan.scan.pushed_predicates[0].fidelity),
        datafusion::logical_expr::TableProviderFilterPushDown::Exact
    );
    assert_eq!(
        plan.scan.pushed_predicates[1].fidelity,
        PushdownFidelity::Inexact
    );
    assert_eq!(plan.scan.unsupported_predicates.len(), 1);
    assert_eq!(plan.residual_predicates.len(), 2);
    assert!(plan.explain.projection_pushed);
    assert!(plan.explain.limit_pushed);
    assert_eq!(plan.explain.inexact_predicates.len(), 1);
    assert_eq!(plan.explain.unsupported_predicates.len(), 1);
    assert_eq!(plan.explain.partitions.len(), 2);
    assert_eq!(plan.explain.estimates.rows, Some(3));
    assert_eq!(
        plan.explain.delivery_guarantee,
        DeliveryGuarantee::EffectivelyOncePerKey
    );
}

#[test]
fn inexact_and_unsupported_predicates_are_reapplied_during_execution() {
    let resource = MockResource::tier_b(sample_batches());
    let input = plan_input(
        vec!["id > 1", "active = true", "name != 'three'"],
        Some(vec!["name".to_owned()]),
        None,
        PlanBoundedness::Bounded,
    );
    let plan = Planner::new().plan_tier_b(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 2);
    let reader = firn_package::PackageReader::open(temp.path()).unwrap();
    for segment in output.segments {
        let batches = reader.read_segment(&segment.segment_id).unwrap();
        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "two");
    }
}

#[test]
fn illegal_unbounded_live_plan_is_rejected() {
    let resource = MockResource::tier_a(sample_batches());
    let input = plan_input(
        vec![],
        None,
        None,
        PlanBoundedness::UnboundedLive {
            checkpoint_cadence_ms: None,
            package_rotation_rows: None,
            watermark: None,
        },
    );
    let error = Planner::new().plan_tier_a(&resource, input).unwrap_err();

    assert_eq!(error.kind, firn_kernel::ErrorKind::Contract);
    assert!(error.message.contains("unbounded live plans are illegal"));
}

#[test]
fn explain_and_operator_chain_carry_contract_package_details() {
    let resource = MockResource::tier_a(sample_batches());
    let input = plan_input(
        vec!["active = true"],
        Some(vec!["id".to_owned(), "name".to_owned()]),
        Some(2),
        PlanBoundedness::UnboundedDrain,
    );
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let explain_json = serde_json::to_value(&plan.explain).unwrap();

    assert!(explain_json.get("pushed_predicates").is_some());
    assert!(explain_json.get("inexact_predicates").is_some());
    assert!(explain_json.get("unsupported_predicates").is_some());
    assert!(explain_json.get("partitions").is_some());
    assert!(explain_json.get("estimates").is_some());
    assert!(explain_json.get("delivery_guarantee").is_some());
    assert!(plan.operator_chain.iter().any(|operator| {
        matches!(
            operator,
            OperatorNode::ContractExec {
                normalizer_version,
                ..
            } if normalizer_version == firn_contract::NORMALIZER_NAMECASE_V1
        )
    }));
    assert!(plan.operator_chain.iter().any(|operator| {
        matches!(
            operator,
            OperatorNode::PackageSink { package_id } if package_id == "pkg-engine-test"
        )
    }));
}

#[test]
fn validation_program_source_name_can_cover_and_rename_batch_field() {
    let resource = MockResource::tier_a(sample_batches());
    let input = plan_input(
        vec![],
        Some(vec!["name".to_owned()]),
        None,
        PlanBoundedness::Bounded,
    );
    let mut plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    rename_column_program_output(&mut plan.validation_program, "name", "customer_name");
    retain_column_program_by_source(&mut plan.validation_program, "name");
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 3);
    let reader = firn_package::PackageReader::open(temp.path()).unwrap();
    let batches = reader.read_segment(&output.segments[0].segment_id).unwrap();
    let schema = batches[0].schema();
    let field = schema.field(0);
    assert_eq!(field.name(), "customer_name");
    assert_eq!(source_name(field), Some("name"));
}

#[test]
fn validation_program_output_name_can_cover_already_normalized_batch_field() {
    let resource = MockResource::tier_a(output_name_batches());
    let input = plan_input_for_schema(
        output_name_schema(),
        vec![],
        Some(vec!["customer_name".to_owned()]),
        None,
        PlanBoundedness::Bounded,
    );
    let mut plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    rename_column_program_source(&mut plan.validation_program, "customer_name", "name");
    retain_column_program_by_output(&mut plan.validation_program, "customer_name");
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 3);
    let reader = firn_package::PackageReader::open(temp.path()).unwrap();
    let batches = reader.read_segment(&output.segments[0].segment_id).unwrap();
    let schema = batches[0].schema();
    let field = schema.field(0);
    assert_eq!(field.name(), "customer_name");
    assert_eq!(source_name(field), Some("name"));
}

#[test]
fn traced_execution_emits_run_resource_package_and_partition_spans() {
    let resource = MockResource::tier_a(sample_batches());
    let input = plan_input(vec![], None, None, PlanBoundedness::Bounded);
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let run_id = RunId::new("run-engine-trace-test").unwrap();
    let temp = TempDir::new().unwrap();
    let subscriber = CapturingSubscriber::default();

    let output = tracing::subscriber::with_default(subscriber.clone(), || {
        block_on(execute_to_package_with_run_id(
            &run_id,
            &plan,
            &resource,
            temp.path(),
        ))
    })
    .unwrap();

    assert_eq!(output.profile.output_batches, 1);
    let spans = subscriber.captured_spans();
    let package_span = spans
        .iter()
        .find(|span| span.name == "firn_engine.package_execution")
        .expect("package execution span is emitted");
    assert_span_fields(
        package_span,
        &[
            ("run_id", "run-engine-trace-test"),
            ("resource_id", "orders"),
            ("package_id", "pkg-engine-test"),
        ],
    );

    let partition_span = spans
        .iter()
        .find(|span| span.name == "firn_engine.partition_execution")
        .expect("partition execution span is emitted");
    assert_span_fields(
        partition_span,
        &[
            ("run_id", "run-engine-trace-test"),
            ("resource_id", "orders"),
            ("package_id", "pkg-engine-test"),
            ("partition_id", "part-0"),
        ],
    );
}

#[test]
fn traced_execution_preserves_manifest_identity_hash() {
    let resource = MockResource::tier_a(sample_batches());
    let input = plan_input(vec![], None, None, PlanBoundedness::Bounded);
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let untraced_temp = TempDir::new().unwrap();
    let traced_temp = TempDir::new().unwrap();

    let untraced = block_on(execute_to_package(&plan, &resource, untraced_temp.path())).unwrap();
    let traced = block_on(execute_to_package_with_run_id(
        &RunId::new("run-engine-hash-test").unwrap(),
        &plan,
        &resource,
        traced_temp.path(),
    ))
    .unwrap();

    assert_eq!(traced.manifest.identity, untraced.manifest.identity);
    assert_eq!(traced.manifest.package_hash, untraced.manifest.package_hash);
    assert_eq!(traced.manifest.signature, untraced.manifest.signature);
}

#[derive(Clone)]
struct MockResource {
    descriptor: ResourceDescriptor,
    schema: SchemaRef,
    batches: Vec<Batch>,
    tier_b: bool,
    negotiate_count: Arc<AtomicUsize>,
    open_count: Arc<AtomicUsize>,
}

impl MockResource {
    fn tier_a(batches: Vec<Batch>) -> Self {
        Self::new(batches, false)
    }

    fn tier_b(batches: Vec<Batch>) -> Self {
        Self::new(batches, true)
    }

    fn new(batches: Vec<Batch>, tier_b: bool) -> Self {
        let schema = batches
            .first()
            .and_then(Batch::record_batch)
            .map(RecordBatch::schema)
            .unwrap_or_else(sample_schema);
        Self {
            descriptor: descriptor(),
            schema,
            batches,
            tier_b,
            negotiate_count: Arc::new(AtomicUsize::new(0)),
            open_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl ResourceStream for MockResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn plan_partitions(&self, _request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        let count = if self.tier_b { 2 } else { 1 };
        (0..count)
            .map(|index| {
                Ok(PartitionPlan {
                    partition_id: PartitionId::new(format!("part-{index}"))?,
                    scope: ScopeKey::Partition {
                        partition_id: PartitionId::new(format!("part-{index}"))?,
                    },
                    start_position: None,
                    metadata: BTreeMap::from([("ordinal".to_owned(), index.to_string())]),
                })
            })
            .collect()
    }

    fn open(&self, partition: PartitionPlan) -> firn_kernel::BoxFuture<'_, Result<BatchStream>> {
        self.open_count.fetch_add(1, Ordering::SeqCst);
        let batches = self
            .batches
            .iter()
            .filter(|batch| batch.header.partition_id == partition.partition_id)
            .cloned()
            .collect::<Vec<_>>();
        Box::pin(
            async move { Ok(Box::pin(stream::iter(batches.into_iter().map(Ok))) as BatchStream) },
        )
    }
}

impl QueryableResource for MockResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        static CAPABILITIES: std::sync::OnceLock<ResourceCapabilities> = std::sync::OnceLock::new();
        CAPABILITIES.get_or_init(|| ResourceCapabilities {
            projection: CapabilitySupport::Supported,
            filters: FilterCapabilities {
                default_fidelity: PushdownFidelity::Inexact,
                supported_operators: vec![">".to_owned(), "=".to_owned()],
            },
            limits: CapabilitySupport::Supported,
            ordering: CapabilitySupport::Unsupported,
            partitioning: PartitioningCapabilities {
                parallel_partitions: true,
                supported_scopes: vec![firn_kernel::ScopeKind::Partition],
            },
            incremental: IncrementalShape::Cursor,
            replay: firn_kernel::ReplaySupport::ExactRecordedBatches,
            idempotent_reads: true,
            backpressure: BackpressureSupport::Pausable,
            estimates: EstimateSupport::RowsAndBytes,
        })
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        self.negotiate_count.fetch_add(1, Ordering::SeqCst);
        let mut plan = negotiate_scan_plan(
            request.resource_id.clone(),
            request.clone(),
            self.capabilities(),
            self.plan_partitions(request)?,
            Some(3),
            Some(256),
            DeliveryGuarantee::EffectivelyOncePerKey,
        )?;
        for pushed in &mut plan.pushed_predicates {
            if pushed.predicate.expression == "id > 1" {
                pushed.fidelity = PushdownFidelity::Exact;
            }
        }
        Ok(plan)
    }
}

#[derive(Clone, Default)]
struct CapturingSubscriber {
    next_id: Arc<AtomicU64>,
    spans: Arc<Mutex<Vec<CapturedSpan>>>,
}

impl CapturingSubscriber {
    fn captured_spans(&self) -> Vec<CapturedSpan> {
        self.spans.lock().unwrap().clone()
    }
}

impl Subscriber for CapturingSubscriber {
    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, attrs: &Attributes<'_>) -> Id {
        let mut visitor = FieldVisitor::default();
        attrs.record(&mut visitor);
        self.spans.lock().unwrap().push(CapturedSpan {
            name: attrs.metadata().name().to_owned(),
            fields: visitor.fields,
        });
        Id::from_u64(self.next_id.fetch_add(1, Ordering::SeqCst) + 1)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, _event: &Event<'_>) {}

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}

#[derive(Clone, Debug)]
struct CapturedSpan {
    name: String,
    fields: BTreeMap<String, String>,
}

#[derive(Default)]
struct FieldVisitor {
    fields: BTreeMap<String, String>,
}

impl Visit for FieldVisitor {
    fn record_str(&mut self, field: &TracingField, value: &str) {
        self.fields
            .insert(field.name().to_owned(), value.to_owned());
    }

    fn record_bool(&mut self, field: &TracingField, value: bool) {
        self.fields
            .insert(field.name().to_owned(), value.to_string());
    }

    fn record_i64(&mut self, field: &TracingField, value: i64) {
        self.fields
            .insert(field.name().to_owned(), value.to_string());
    }

    fn record_u64(&mut self, field: &TracingField, value: u64) {
        self.fields
            .insert(field.name().to_owned(), value.to_string());
    }

    fn record_debug(&mut self, field: &TracingField, value: &dyn fmt::Debug) {
        self.fields
            .insert(field.name().to_owned(), format!("{value:?}"));
    }
}

fn assert_span_fields(span: &CapturedSpan, expected: &[(&str, &str)]) {
    let expected = expected
        .iter()
        .map(|(field, value)| ((*field).to_owned(), (*value).to_owned()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        span.fields, expected,
        "span {} should record the exact field set",
        span.name
    );
}

fn plan_input(
    filters: Vec<&str>,
    projection: Option<Vec<String>>,
    limit: Option<u64>,
    boundedness: PlanBoundedness,
) -> EnginePlanInput {
    plan_input_for_schema(sample_schema(), filters, projection, limit, boundedness)
}

fn plan_input_for_schema(
    schema: SchemaRef,
    filters: Vec<&str>,
    projection: Option<Vec<String>>,
    limit: Option<u64>,
    boundedness: PlanBoundedness,
) -> EnginePlanInput {
    let observed = ObservedSchema::from_arrow(schema.as_ref());
    let validation_program =
        compile_validation_program(&ContractPolicy::for_trust(TrustLevel::Governed), &observed)
            .unwrap();
    EnginePlanInput {
        request: ScanRequest {
            resource_id: ResourceId::new("orders").unwrap(),
            projection,
            filters: filters
                .into_iter()
                .enumerate()
                .map(|(index, expression)| ScanPredicate {
                    predicate_id: PredicateId::new(format!("p{index}")).unwrap(),
                    expression: expression.to_owned(),
                })
                .collect(),
            limit,
            order_by: Vec::new(),
            scope: ScopeKey::Resource,
        },
        validation_program,
        boundedness,
        package_id: "pkg-engine-test".to_owned(),
    }
}

fn rename_column_program_output(
    program: &mut firn_contract::ValidationProgram,
    source_name: &str,
    output_name: &str,
) {
    let column = program
        .column_programs
        .iter_mut()
        .find(|column| column.source_name == source_name)
        .unwrap();
    column.output_name = output_name.to_owned();
}

fn rename_column_program_source(
    program: &mut firn_contract::ValidationProgram,
    output_name: &str,
    source_name: &str,
) {
    let column = program
        .column_programs
        .iter_mut()
        .find(|column| column.output_name == output_name)
        .unwrap();
    column.source_name = source_name.to_owned();
}

fn retain_column_program_by_source(
    program: &mut firn_contract::ValidationProgram,
    source_name: &str,
) {
    program
        .column_programs
        .retain(|column| column.source_name == source_name);
}

fn retain_column_program_by_output(
    program: &mut firn_contract::ValidationProgram,
    output_name: &str,
) {
    program
        .column_programs
        .retain(|column| column.output_name == output_name);
}

fn descriptor() -> ResourceDescriptor {
    ResourceDescriptor {
        resource_id: ResourceId::new("orders").unwrap(),
        schema_source: SchemaSource::Discovered {
            schema_hash: Some(SchemaHash::new("schema-v1").unwrap()),
        },
        primary_key: vec!["id".to_owned()],
        merge_key: vec!["id".to_owned()],
        cursor: None,
        write_disposition: WriteDisposition::Merge,
        contract: Some(ContractRef::new("contract-orders").unwrap()),
        state_scope: ScopeKey::Resource,
        freshness: Some(FreshnessSpec { max_age_ms: 60_000 }),
        trust_level: TrustLevel::Governed,
    }
}

fn sample_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
    ]))
}

fn output_name_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("customer_name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
    ]))
}

fn sample_batches() -> Vec<Batch> {
    vec![
        batch_for_partition(
            "batch-0",
            "part-0",
            vec![1, 2, 3],
            vec!["one", "two", "three"],
            vec![false, true, true],
        ),
        batch_for_partition(
            "batch-1",
            "part-1",
            vec![1, 2, 3],
            vec!["one", "two", "three"],
            vec![false, true, true],
        ),
    ]
}

fn output_name_batches() -> Vec<Batch> {
    vec![batch_for_partition_with_schema(
        "batch-0",
        "part-0",
        output_name_schema(),
        vec![1, 2, 3],
        vec!["one", "two", "three"],
        vec![false, true, true],
    )]
}

fn batch_for_partition(
    batch_id: &str,
    partition_id: &str,
    ids: Vec<i32>,
    names: Vec<&str>,
    active: Vec<bool>,
) -> Batch {
    batch_for_partition_with_schema(batch_id, partition_id, sample_schema(), ids, names, active)
}

fn batch_for_partition_with_schema(
    batch_id: &str,
    partition_id: &str,
    schema: SchemaRef,
    ids: Vec<i32>,
    names: Vec<&str>,
    active: Vec<bool>,
) -> Batch {
    let record_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(BooleanArray::from(active)) as ArrayRef,
        ],
    )
    .unwrap();

    Batch {
        header: BatchHeader {
            batch_id: BatchId::new(batch_id).unwrap(),
            resource_id: ResourceId::new("orders").unwrap(),
            partition_id: PartitionId::new(partition_id).unwrap(),
            observed_schema_hash: SchemaHash::new("schema-v1").unwrap(),
            row_count: record_batch.num_rows() as u64,
            byte_count: record_batch.get_array_memory_size() as u64,
            source_position: None,
            watermarks: Vec::new(),
            stats: BatchStats::default(),
            cdc: None,
        },
        payload: firn_kernel::BatchPayload::RecordBatch(record_batch),
    }
}
