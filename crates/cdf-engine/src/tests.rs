use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use cdf_contract::{
    ContractPolicy, ObservedSchema, RowRule, VerdictAction, compile_validation_program,
};
use cdf_kernel::{
    BackpressureSupport, Batch, BatchHeader, BatchId, BatchStats, BatchStream, CapabilitySupport,
    ContractRef, DeliveryGuarantee, EstimateSupport, FileManifest, FilePosition,
    FilterCapabilities, FreshnessSpec, IncrementalShape, PartitionId, PartitionPlan,
    PartitioningCapabilities, PredicateId, PushdownFidelity, QueryableResource,
    ResourceCapabilities, ResourceDescriptor, ResourceId, ResourceStream, Result, RunId, ScanPlan,
    ScanPredicate, ScanRequest, SchemaHash, SchemaSource, ScopeKey, SourcePosition, TrustLevel,
    WriteDisposition, source_name,
};
use cdf_package::PackageStatus;
use datafusion::{
    catalog::TableProvider, physical_plan::common::collect as collect_stream, prelude::*,
};
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

    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
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
fn execution_returns_segment_source_position_evidence() {
    let resource = MockResource::tier_a(vec![batch_with_file_position()]);
    let input = plan_input(vec![], None, None, PlanBoundedness::Bounded);
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package_with_segment_positions(
        &plan,
        &resource,
        temp.path(),
    ))
    .unwrap();

    assert_eq!(output.output.segments.len(), 1);
    assert_eq!(output.segment_positions.len(), 1);
    assert_eq!(
        output.segment_positions[0].segment_id,
        output.output.segments[0].segment_id
    );
    let Some(SourcePosition::FileManifest(manifest)) = &output.segment_positions[0].output_position
    else {
        panic!("expected file manifest position evidence");
    };
    assert_eq!(manifest.files[0].path, "/tmp/cdf/events.ndjson");
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
fn tier_b_explain_serializes_honest_cdf_native_operator_metadata() {
    let resource = MockResource::tier_b(sample_batches());
    let input = plan_input(
        vec!["id > 1", "active = true", "name != 'missing'"],
        Some(vec!["name".to_owned()]),
        Some(10),
        PlanBoundedness::Bounded,
    );
    let plan = Planner::new().plan_tier_b(&resource, input).unwrap();
    let explain_json = serde_json::to_value(&plan.explain).unwrap();

    assert_honest_cdf_native_operator_metadata(&plan);
    assert_explain_carries_required_fields(&explain_json);
    assert_eq!(plan.explain.pushed_predicates.len(), 2);
    assert_eq!(plan.explain.inexact_predicates.len(), 1);
    assert_eq!(plan.explain.unsupported_predicates.len(), 1);
    assert!(plan.explain.projection_pushed);
    assert!(plan.explain.limit_pushed);
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
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
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

    assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
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

    assert_honest_cdf_native_operator_metadata(&plan);
    assert_explain_carries_required_fields(&explain_json);
    assert!(plan.operator_chain.iter().any(|operator| {
        matches!(
            operator,
            OperatorNode::ContractExec {
                normalizer_version,
                ..
            } if normalizer_version == cdf_contract::NORMALIZER_NAMECASE_V1
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
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
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
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = reader.read_segment(&output.segments[0].segment_id).unwrap();
    let schema = batches[0].schema();
    let field = schema.field(0);
    assert_eq!(field.name(), "customer_name");
    assert_eq!(source_name(field), Some("name"));
}

#[test]
fn contract_exec_filters_quarantined_rows_before_normalize() {
    let resource = MockResource::tier_a(sample_batches());
    let mut input = plan_input(vec![], None, None, PlanBoundedness::Bounded);
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Domain {
        column: "name".to_owned(),
        allowed: vec!["two".to_owned(), "three".to_owned()],
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(sample_schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 2);
    assert_eq!(output.segments.len(), 1);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = reader.read_segment(&output.segments[0].segment_id).unwrap();
    let names = batches[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "two");
    assert_eq!(names.value(1), "three");
}

#[test]
fn reject_batch_contract_abort_prevents_packaged_manifest() {
    let resource = MockResource::tier_a(sample_batches());
    let mut input = plan_input(vec![], None, None, PlanBoundedness::Bounded);
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.verdicts.violation = VerdictAction::RejectBatch;
    policy.rows.rules = vec![RowRule::Domain {
        column: "name".to_owned(),
        allowed: vec!["missing".to_owned()],
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(sample_schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();

    let error = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap_err();

    assert!(error.to_string().contains("reject_batch"));
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    assert_ne!(reader.manifest().lifecycle.status, PackageStatus::Packaged);
}

#[test]
fn freshness_contract_writes_observed_at_context_when_rule_requires_it() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "updated_at",
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        false,
    )]));
    let batch = Batch::from_record_batch(
        BatchId::new("freshness-batch").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        SchemaHash::new("schema-v1").unwrap(),
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![0]).with_timezone("UTC")) as ArrayRef,
            ],
        )
        .unwrap(),
    )
    .unwrap();
    let resource = MockResource::tier_a(vec![batch]);
    let mut input = plan_input_for_schema(schema, vec![], None, None, PlanBoundedness::Bounded);
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Freshness {
        column: "updated_at".to_owned(),
        max_age_ms: 1,
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 0);
    assert!(output.segments.is_empty());
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    assert!(
        reader
            .manifest()
            .identity
            .files
            .iter()
            .any(|file| { file.path == "plan/contract-evaluation-context.json" })
    );
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
        .find(|span| span.name == "cdf_engine.package_execution")
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
        .find(|span| span.name == "cdf_engine.partition_execution")
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

#[test]
fn datafusion_table_provider_pushdown_classification_delegates_to_resource() {
    let resource = Arc::new(DataFusionMockResource::new());
    let provider = QueryableResourceTableProvider::new(resource.clone(), ScopeKey::Resource);
    let filters = [
        col("id").gt(lit(1_i32)),
        col("active").eq(lit(true)),
        col("name").not_eq(lit("three")),
        col("id").add(lit(1_i32)).gt(lit(2_i32)),
    ];
    let filter_refs = filters.iter().collect::<Vec<_>>();

    let pushdown = provider.supports_filters_pushdown(&filter_refs).unwrap();

    assert_eq!(resource.negotiate_count.load(Ordering::SeqCst), 1);
    assert_eq!(
        pushdown,
        vec![
            datafusion::logical_expr::TableProviderFilterPushDown::Exact,
            datafusion::logical_expr::TableProviderFilterPushDown::Inexact,
            datafusion::logical_expr::TableProviderFilterPushDown::Unsupported,
            datafusion::logical_expr::TableProviderFilterPushDown::Unsupported,
        ]
    );
    let requests = resource.requests.lock().unwrap();
    assert_eq!(requests[0].filters.len(), 3);
    assert_eq!(requests[0].filters[0].expression, "id > 1");
    assert_eq!(requests[0].filters[1].expression, "active = true");
    assert_eq!(requests[0].filters[2].expression, "name != 'three'");
}

#[test]
fn datafusion_registered_table_executes_with_residuals_and_projection() {
    let resource = Arc::new(DataFusionMockResource::new());
    let provider = queryable_resource_table_provider(resource.clone(), ScopeKey::Resource);
    let ctx = SessionContext::new();
    ctx.register_table("orders", provider).unwrap();

    let batches = block_on(async {
        let provider = ctx.table_provider("orders").await.unwrap();
        let projection = vec![1];
        let filters = vec![col("id").gt(lit(1_i32))];
        let plan = provider
            .scan(&ctx.state(), Some(&projection), &filters, None)
            .await
            .unwrap();
        collect_execution_plan_partitions(plan, ctx.task_ctx()).await
    });

    assert_eq!(resource.open_count.load(Ordering::SeqCst), 2);
    assert_eq!(
        batch_strings(&batches, "name"),
        vec!["two", "three", "two", "three"]
    );
    assert_eq!(batches[0].schema().fields().len(), 1);
    assert_eq!(batches[0].schema().field(0).name(), "name");
}

#[test]
fn datafusion_unsupported_expression_stays_residual() {
    let resource = Arc::new(DataFusionMockResource::new());
    let provider = QueryableResourceTableProvider::new(resource.clone(), ScopeKey::Resource);
    let unsupported = col("id").add(lit(1_i32)).gt(lit(2_i32));
    let filter_refs = vec![&unsupported];
    let pushdown = provider.supports_filters_pushdown(&filter_refs).unwrap();

    assert_eq!(
        pushdown,
        vec![datafusion::logical_expr::TableProviderFilterPushDown::Unsupported]
    );
    let requests = resource.requests.lock().unwrap();
    assert!(requests.iter().all(|request| request.filters.is_empty()));
}

#[test]
fn datafusion_limit_pushdown_is_disabled_for_inexact_filters() {
    let resource = Arc::new(DataFusionMockResource::new());
    let provider = QueryableResourceTableProvider::new(resource.clone(), ScopeKey::Resource);
    let ctx = SessionContext::new();
    let filters = vec![col("active").eq(lit(true))];

    let _plan = block_on(provider.scan(&ctx.state(), None, &filters, Some(1))).unwrap();

    let requests = resource.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].limit, None);
    assert_eq!(requests[1].limit, None);
}

#[test]
fn datafusion_limit_pushdown_remains_enabled_for_exact_filters() {
    let resource = Arc::new(DataFusionMockResource::new());
    let provider = QueryableResourceTableProvider::new(resource.clone(), ScopeKey::Resource);
    let ctx = SessionContext::new();
    let filters = vec![col("id").gt(lit(1_i32))];

    let _plan = block_on(provider.scan(&ctx.state(), None, &filters, Some(1))).unwrap();

    let requests = resource.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].limit, None);
    assert_eq!(requests[1].limit, Some(1));
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

#[derive(Clone)]
struct DataFusionMockResource {
    descriptor: ResourceDescriptor,
    schema: SchemaRef,
    batches: Vec<Batch>,
    negotiate_count: Arc<AtomicUsize>,
    open_count: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<ScanRequest>>>,
}

impl DataFusionMockResource {
    fn new() -> Self {
        Self {
            descriptor: descriptor(),
            schema: sample_schema(),
            batches: sample_batches(),
            negotiate_count: Arc::new(AtomicUsize::new(0)),
            open_count: Arc::new(AtomicUsize::new(0)),
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl ResourceStream for DataFusionMockResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn plan_partitions(&self, _request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        unreachable!("DataFusion adapter must use QueryableResource::negotiate")
    }

    fn open(&self, partition: PartitionPlan) -> cdf_kernel::BoxFuture<'_, Result<BatchStream>> {
        self.open_count.fetch_add(1, Ordering::SeqCst);
        let exact_filters = partition
            .metadata
            .get("exact_filters")
            .map(|filters| filters.split('\n').map(str::to_owned).collect::<Vec<_>>())
            .unwrap_or_default();
        let batches = self
            .batches
            .iter()
            .filter(|batch| batch.header.partition_id == partition.partition_id)
            .map(|batch| apply_mock_exact_filters(batch.clone(), &exact_filters))
            .collect::<Result<Vec<_>>>();
        Box::pin(
            async move { Ok(Box::pin(stream::iter(batches?.into_iter().map(Ok))) as BatchStream) },
        )
    }
}

impl QueryableResource for DataFusionMockResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        static CAPABILITIES: std::sync::OnceLock<ResourceCapabilities> = std::sync::OnceLock::new();
        CAPABILITIES.get_or_init(|| ResourceCapabilities {
            projection: CapabilitySupport::Supported,
            filters: FilterCapabilities {
                default_fidelity: PushdownFidelity::Unsupported,
                supported_operators: vec![">".to_owned(), "=".to_owned(), "!=".to_owned()],
            },
            limits: CapabilitySupport::Supported,
            ordering: CapabilitySupport::Unsupported,
            partitioning: PartitioningCapabilities {
                parallel_partitions: true,
                supported_scopes: vec![cdf_kernel::ScopeKind::Partition],
            },
            incremental: IncrementalShape::Full,
            replay: cdf_kernel::ReplaySupport::ExactRecordedBatches,
            idempotent_reads: true,
            backpressure: BackpressureSupport::Pausable,
            estimates: EstimateSupport::Rows,
        })
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        self.negotiate_count.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request.clone());

        let mut pushed_predicates = Vec::new();
        let mut unsupported_predicates = Vec::new();
        for predicate in &request.filters {
            match predicate.expression.as_str() {
                "id > 1" => pushed_predicates.push(cdf_kernel::PushedPredicate {
                    predicate: predicate.clone(),
                    fidelity: PushdownFidelity::Exact,
                }),
                "active = true" => pushed_predicates.push(cdf_kernel::PushedPredicate {
                    predicate: predicate.clone(),
                    fidelity: PushdownFidelity::Inexact,
                }),
                _ => unsupported_predicates.push(predicate.clone()),
            }
        }

        let exact_filters = pushed_predicates
            .iter()
            .filter(|pushed| pushed.fidelity == PushdownFidelity::Exact)
            .map(|pushed| pushed.predicate.expression.clone())
            .collect::<Vec<_>>()
            .join("\n");
        let partitions = ["part-0", "part-1"]
            .into_iter()
            .map(|partition| {
                let partition_id = PartitionId::new(partition)?;
                Ok(PartitionPlan {
                    partition_id: partition_id.clone(),
                    scope: ScopeKey::Partition { partition_id },
                    start_position: None,
                    metadata: BTreeMap::from([("exact_filters".to_owned(), exact_filters.clone())]),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(ScanPlan {
            plan_id: cdf_kernel::PlanId::new(format!(
                "df-plan-{}-{}",
                request.resource_id.as_str(),
                self.negotiate_count.load(Ordering::SeqCst)
            ))?,
            request: request.clone(),
            partitions,
            pushed_predicates,
            unsupported_predicates,
            estimated_rows: Some(6),
            estimated_bytes: None,
            delivery_guarantee: DeliveryGuarantee::EffectivelyOncePerKey,
        })
    }
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

    fn open(&self, partition: PartitionPlan) -> cdf_kernel::BoxFuture<'_, Result<BatchStream>> {
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
                supported_scopes: vec![cdf_kernel::ScopeKind::Partition],
            },
            incremental: IncrementalShape::Cursor,
            replay: cdf_kernel::ReplaySupport::ExactRecordedBatches,
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

fn assert_honest_cdf_native_operator_metadata(plan: &EnginePlan) {
    let plan_json = serde_json::to_value(plan).unwrap();
    let plan_text = serde_json::to_string(&plan_json).unwrap();
    assert!(!plan_text.contains("data_fusion_table_provider"));
    assert!(!plan_text.contains("data_fusion_scan_exec"));
    assert!(!plan_text.contains("datafusion_table_provider"));

    assert_cdf_native_operator_kinds(&plan_json["operator_chain"]);
    assert_cdf_native_operator_kinds(&plan_json["explain"]["operator_chain"]);
    assert_eq!(
        plan_json["operator_chain"][0]["adapter_kind"],
        "cdf_native_resource_adapter"
    );
    assert_eq!(
        plan_json["explain"]["operator_chain"][0]["adapter_kind"],
        "cdf_native_resource_adapter"
    );
}

fn assert_cdf_native_operator_kinds(operator_chain: &serde_json::Value) {
    let actual = operator_chain
        .as_array()
        .unwrap()
        .iter()
        .map(|operator| operator["kind"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![
            "cdf_resource_adapter",
            "cdf_native_scan",
            "schema_fingerprint_exec",
            "contract_exec",
            "normalize_exec",
            "profile_exec",
            "lineage_exec",
            "package_sink",
        ]
    );
}

fn assert_explain_carries_required_fields(explain_json: &serde_json::Value) {
    for field in [
        "pushed_predicates",
        "inexact_predicates",
        "unsupported_predicates",
        "partitions",
        "estimates",
        "delivery_guarantee",
        "boundedness",
    ] {
        assert!(explain_json.get(field).is_some(), "missing {field}");
    }
}

fn batch_strings(batches: &[RecordBatch], column: &str) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            let index = batch.schema().index_of(column).unwrap();
            let array = batch
                .column(index)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..array.len())
                .map(|row| array.value(row).to_owned())
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn collect_execution_plan_partitions(
    plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    task_ctx: Arc<datafusion::execution::TaskContext>,
) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    for partition in 0..plan.properties().partitioning.partition_count() {
        let stream = plan.execute(partition, Arc::clone(&task_ctx)).unwrap();
        batches.extend(collect_stream(stream).await.unwrap());
    }
    batches
}

fn apply_mock_exact_filters(batch: Batch, filters: &[String]) -> Result<Batch> {
    if filters.is_empty() {
        return Ok(batch);
    }
    let Some(record_batch) = batch.record_batch() else {
        return Ok(batch);
    };
    let mut keep = vec![true; record_batch.num_rows()];
    for filter in filters {
        if filter == "id > 1" {
            let id_index = record_batch.schema().index_of("id").unwrap();
            let ids = record_batch
                .column(id_index)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for (row, keep_row) in keep.iter_mut().enumerate().take(ids.len()) {
                *keep_row &= ids.value(row) > 1;
            }
        }
    }
    let filtered =
        arrow_select::filter::filter_record_batch(record_batch, &BooleanArray::from(keep))
            .map_err(cdf_kernel::CdfError::from)?;
    Ok(Batch {
        header: BatchHeader {
            row_count: filtered.num_rows() as u64,
            byte_count: filtered.get_array_memory_size() as u64,
            ..batch.header
        },
        payload: cdf_kernel::BatchPayload::RecordBatch(filtered),
    })
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
    program: &mut cdf_contract::ValidationProgram,
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
    program: &mut cdf_contract::ValidationProgram,
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
    program: &mut cdf_contract::ValidationProgram,
    source_name: &str,
) {
    program
        .column_programs
        .retain(|column| column.source_name == source_name);
}

fn retain_column_program_by_output(
    program: &mut cdf_contract::ValidationProgram,
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

fn batch_with_file_position() -> Batch {
    let mut batch = batch_for_partition(
        "batch-file",
        "part-0",
        vec![1, 2],
        vec!["one", "two"],
        vec![true, true],
    );
    batch.header.source_position = Some(SourcePosition::FileManifest(FileManifest {
        version: 1,
        files: vec![FilePosition {
            path: "/tmp/cdf/events.ndjson".to_owned(),
            size_bytes: 42,
            etag: None,
            sha256: Some("sha256-file".to_owned()),
        }],
    }));
    batch
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
        payload: cdf_kernel::BatchPayload::RecordBatch(record_batch),
    }
}
