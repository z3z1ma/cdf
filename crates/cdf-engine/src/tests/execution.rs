use super::support::Result;
use super::support::{
    Arc, BTreeMap, DEFAULT_PREVIEW_MAX_BYTES, DataType, DrainTermination, EXECUTION_EXTENT_VERSION,
    EffectiveSchemaCatalogEntry, EffectiveSchemaObservationEvidence, EffectiveSchemaRuntime,
    EngineExecutionConfig, EnginePackageDraft, EnginePreviewLimits, ExecutionExtent, Field,
    FieldCoercionDecision, MockResource, Ordering, PREVIEW_POLICY_BALANCED_STRATIFIED_V1, Planner,
    STRATIFIED_HASH_SELECTOR_V1, Schema, SchemaChangeKind, SchemaHash, StandaloneExecutionHost,
    StringArray, TempDir, VerdictAction, batch_for_partition_with_schema, block_on,
    bound_effective_schema_evidence, execute_to_package,
    execute_to_package_with_segment_positions_and_pre_finalize, fast_test_retry_policy,
    mock_compiled_source_plan, plan_input, plan_input_for_schema, preview_resource, sample_batches,
    sample_schema, sample_stream_epoch_policy, schema_observation_binding, terminal_file_position,
};

#[test]
fn reusable_engine_execution_config_creates_isolated_invocation_state() {
    let config = EngineExecutionConfig::default();
    let first = config.new_invocation();
    let second = config.new_invocation();

    first.cancellation.cancel();

    assert!(first.cancellation.is_cancelled());
    assert!(!second.cancellation.is_cancelled());
    assert!(first.source_retry_evidence().snapshot().unwrap().is_empty());
    assert!(
        second
            .source_retry_evidence()
            .snapshot()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn residual_limit_is_consumed_across_partitions() {
    let mut batches = sample_batches();
    for batch in &mut batches {
        batch.header.source_position = Some(terminal_file_position());
    }
    let resource = MockResource::tier_b(batches).without_control_keys();
    let input = plan_input(
        vec!["active = true"],
        Some(vec!["name".to_owned()]),
        Some(1),
        ExecutionExtent::bounded(),
    );
    let plan = Planner::new().plan_tier_b(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 1);
    assert_eq!(output.profile.output_batches, 1);
    assert_eq!(output.identity_segments().len(), 1);
}

#[test]
fn preobserved_baseline_widening_survives_the_drift_reject_verdict() {
    let physical_schema = sample_schema();
    let effective_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
    ]));
    let physical_hash = cdf_kernel::canonical_arrow_schema_hash(physical_schema.as_ref()).unwrap();
    let evidence = bound_effective_schema_evidence(
        SchemaHash::new("effective-preobserved-v1").unwrap(),
        "manifest-preobserved-v1",
        ".cdf/schemas/orders@manifest-preobserved-v1.discovery.json",
        vec![EffectiveSchemaObservationEvidence::new(
            "input-0",
            physical_hash.clone(),
            schema_observation_binding("input-0"),
        )],
    );
    let runtime = EffectiveSchemaRuntime::new(
        evidence,
        vec![EffectiveSchemaCatalogEntry::new(
            physical_hash,
            physical_schema,
        )],
    )
    .unwrap();
    let resource = MockResource::tier_b(sample_batches())
        .with_effective_schema_runtime(effective_schema.clone(), runtime);
    let mut input = plan_input_for_schema(
        effective_schema,
        vec![],
        None,
        None,
        ExecutionExtent::bounded(),
    );
    input
        .validation_program
        .schema_verdicts
        .iter_mut()
        .find(|rule| rule.change == SchemaChangeKind::TypeWidening)
        .unwrap()
        .verdict = VerdictAction::RejectBatch;

    let plan = Planner::new().plan_tier_b(&resource, input).unwrap();
    let widening = plan
        .effective_schema_evidence
        .as_ref()
        .unwrap()
        .observations[0]
        .coercion_plan
        .fields
        .iter()
        .find(|field| field.source_name == "id")
        .unwrap();
    assert_eq!(widening.decision, FieldCoercionDecision::Widened);
}

#[test]
fn execution_rejects_batch_labeled_for_another_partition_before_admission() {
    let resource = MockResource::tier_a(vec![batch_for_partition_with_schema(
        "misrouted-batch",
        "part-1",
        sample_schema(),
        vec![1],
        vec!["one"],
        vec![true],
    )])
    .with_misrouted_batches();
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let package_dir = TempDir::new().unwrap();

    let error = block_on(execute_to_package(&plan, &resource, package_dir.path())).unwrap_err();

    assert!(
        error.to_string().contains("planned partition")
            && error.to_string().contains("received batch")
            && error.to_string().contains("part-1"),
        "{error}"
    );
}

#[test]
fn preview_traverses_every_planned_partition_through_the_engine_front_end() {
    let resource = MockResource::tier_b(sample_batches());
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let limits = EnginePreviewLimits::default();

    let preview = block_on(preview_resource(&plan, &resource, limits.clone())).unwrap();

    assert_eq!(resource.open_count.load(Ordering::SeqCst), 2);
    assert_eq!(preview.planned_partition_count, 2);
    assert_eq!(preview.payload_opened_partition_count, 2);
    assert_eq!(preview.attested_partition_count, 0);
    assert_eq!(preview.inspected_partition_count, 2);
    assert_eq!(preview.inspected_batch_count, 2);
    assert_eq!(preview.selected_partition_count, 2);
    assert_eq!(
        preview.selection.policy,
        PREVIEW_POLICY_BALANCED_STRATIFIED_V1
    );
    assert_eq!(preview.selection.selector, STRATIFIED_HASH_SELECTOR_V1);
    assert_eq!(preview.selection.selected.len(), 2);
    assert_eq!(preview.selection.selected[0].batch_quota, 32);
    assert_eq!(preview.selection.selected[1].batch_quota, 32);
    assert_eq!(preview.row_count, 6);
    assert_eq!(preview.fields, vec!["id", "name", "active", "_cdf_variant"]);
    assert_eq!(preview.limits, limits);
    assert!(!preview.truncated);
}

#[test]
fn preview_rejects_stale_compiled_expression_plan_before_source_contact() {
    let resource = MockResource::tier_b(sample_batches());
    let mut plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(vec!["id >= 1"], None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    plan.compiled_expression_plan.native_filter_lowering_version = "stale".to_owned();

    let error = block_on(preview_resource(
        &plan,
        &resource,
        EnginePreviewLimits::default(),
    ))
    .unwrap_err();

    assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
}

#[test]
fn preview_applies_explicit_row_limit_globally_without_opening_later_payloads() {
    let resource = MockResource::tier_b(sample_batches());
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, Some(2), ExecutionExtent::bounded()),
        )
        .unwrap();
    let limits = EnginePreviewLimits::default().with_max_rows(2).unwrap();

    let preview = block_on(preview_resource(&plan, &resource, limits)).unwrap();

    assert_eq!(resource.open_count.load(Ordering::SeqCst), 1);
    assert_eq!(preview.payload_opened_partition_count, 1);
    assert_eq!(preview.attested_partition_count, 0);
    assert_eq!(preview.inspected_partition_count, 1);
    assert_eq!(preview.inspected_batch_count, 1);
    assert_eq!(preview.row_count, 2);
    assert_eq!(
        preview
            .selection
            .selected_but_uninspected_partition_ids
            .len(),
        1
    );
    assert_eq!(preview.payload_uninspected_partition_count, 1);
    assert!(preview.truncated);
}

#[test]
fn preview_configured_byte_limit_accounts_decoded_input_separately_from_output() {
    let baseline_resource = MockResource::tier_b(sample_batches());
    let baseline_plan = Planner::new()
        .plan_tier_b(
            &baseline_resource,
            plan_input(Vec::new(), None, Some(1), ExecutionExtent::bounded()),
        )
        .unwrap();
    let one_row = block_on(preview_resource(
        &baseline_plan,
        &baseline_resource,
        EnginePreviewLimits::default().with_max_rows(1).unwrap(),
    ))
    .unwrap();

    let resource = MockResource::tier_b(sample_batches());
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let preview = block_on(preview_resource(
        &plan,
        &resource,
        EnginePreviewLimits::new(500, one_row.byte_count, 64).unwrap(),
    ))
    .unwrap();

    assert_eq!(preview.byte_count, one_row.byte_count);
    assert!(preview.output_byte_count > 0);
    assert_eq!(preview.inspected_batch_count, 1);
    assert_eq!(preview.payload_opened_partition_count, 1);
    assert_eq!(preview.payload_uninspected_partition_count, 1);
    assert!(preview.truncated);
}

#[test]
fn preview_rejects_an_oversized_batch_atomically() {
    let baseline_resource = MockResource::tier_b(sample_batches());
    let baseline_plan = Planner::new()
        .plan_tier_b(
            &baseline_resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let baseline = block_on(preview_resource(
        &baseline_plan,
        &baseline_resource,
        EnginePreviewLimits::default().with_max_rows(1).unwrap(),
    ))
    .unwrap();
    let resource = MockResource::tier_b(sample_batches());
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();

    let preview = block_on(preview_resource(
        &plan,
        &resource,
        EnginePreviewLimits::new(500, baseline.byte_count - 1, 64).unwrap(),
    ))
    .unwrap();

    assert_eq!(preview.payload_opened_partition_count, 2);
    assert_eq!(preview.inspected_partition_count, 0);
    assert_eq!(preview.inspected_batch_count, 0);
    assert_eq!(preview.row_count, 0);
    assert_eq!(preview.byte_count, 0);
    assert_eq!(preview.output_byte_count, 0);
    assert_eq!(preview.payload_uninspected_partition_count, 2);
    assert!(preview.truncated);
}

#[test]
fn preview_fair_batch_quotas_are_fixed_before_payload_io() {
    let resource = MockResource::tier_b(sample_batches()).with_partition_count(3);
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();

    let preview = block_on(preview_resource(
        &plan,
        &resource,
        EnginePreviewLimits::new(500, DEFAULT_PREVIEW_MAX_BYTES, 8).unwrap(),
    ))
    .unwrap();

    assert_eq!(preview.selected_partition_count, 3);
    assert_eq!(
        preview
            .selection
            .selected
            .iter()
            .map(|partition| partition.batch_quota)
            .collect::<Vec<_>>(),
        vec![3, 3, 2]
    );
}

#[test]
fn preview_large_plan_selects_and_opens_at_most_the_global_batch_budget() {
    let resource = MockResource::tier_b(Vec::new()).with_partition_count(10_000);
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();

    let preview = block_on(preview_resource(
        &plan,
        &resource,
        EnginePreviewLimits::default(),
    ))
    .unwrap();

    assert_eq!(preview.planned_partition_count, 10_000);
    assert_eq!(preview.payload_eligible_partition_count, 10_000);
    assert_eq!(preview.selected_partition_count, 64);
    assert_eq!(preview.payload_opened_partition_count, 64);
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 64);
    assert_eq!(preview.selection.selected.len(), 64);
    assert!(
        preview
            .selection
            .selected
            .iter()
            .all(|partition| partition.batch_quota == 1)
    );
    assert_eq!(preview.inspected_partition_count, 64);
    assert_eq!(preview.payload_uninspected_partition_count, 9_936);
    assert!(preview.truncated);
}

#[test]
fn inexact_and_unsupported_predicates_are_reapplied_during_execution() {
    let resource = MockResource::tier_b(sample_batches()).without_control_keys();
    let input = plan_input(
        vec!["id > 1", "active = true", "name != 'three'"],
        Some(vec!["name".to_owned()]),
        None,
        ExecutionExtent::bounded(),
    );
    let plan = Planner::new().plan_tier_b(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 2);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let memory: Arc<dyn cdf_memory::MemoryCoordinator> = Arc::new(
        cdf_memory::DeterministicMemoryCoordinator::new(128 * 1024 * 1024, BTreeMap::new())
            .unwrap(),
    );
    for segment in reader
        .verified_canonical_segment_stream(memory, 128 * 1024 * 1024)
        .unwrap()
    {
        let batches = segment.unwrap().batches;
        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "two");
    }
}

#[test]
fn execution_rejects_resident_extent_before_source_contact() {
    let resource = MockResource::tier_a(sample_batches());
    let mut plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    plan.execution_extent = ExecutionExtent::Resident {
        version: EXECUTION_EXTENT_VERSION,
        policy: sample_stream_epoch_policy(),
    };
    plan.explain.execution_extent = plan.execution_extent.clone();

    let temp = TempDir::new().unwrap();
    let error = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap_err();
    assert!(error.message.contains("resident execution is not enabled"));
    assert!(std::fs::read_dir(temp.path()).unwrap().next().is_none());
}

#[test]
fn execution_rejects_divergent_recorded_extent_before_source_contact() {
    let resource = MockResource::tier_a(sample_batches());
    let mut plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    plan.explain.execution_extent = ExecutionExtent::Drain {
        version: EXECUTION_EXTENT_VERSION,
        policy: sample_stream_epoch_policy(),
        termination: DrainTermination::Records { count: 10 },
    };

    let temp = TempDir::new().unwrap();
    let error = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap_err();
    assert!(
        error
            .message
            .contains("does not match its recorded explain extent")
    );
    assert!(std::fs::read_dir(temp.path()).unwrap().next().is_none());
}

#[test]
fn execution_rejects_coherently_widened_source_ceiling_and_schedule() {
    let resource = MockResource::tier_b(sample_batches())
        .with_partition_count(1)
        .with_transient_open_failures(0);
    let mut plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let source = mock_compiled_source_plan(&resource, Some(fast_test_retry_policy()));
    resource.bind_compiled_source(&source);
    plan = plan.bind_compiled_source(&source).unwrap();
    let mut forged_compiler_source = source.clone();
    forged_compiler_source
        .execution_capabilities
        .retry_policy
        .as_mut()
        .unwrap()
        .max_total_attempts = 4;
    forged_compiler_source.validate().unwrap();
    let forged_source =
        cdf_runtime::CompiledSourceExecutionPlan::compile(&forged_compiler_source).unwrap();
    let forged_schedule =
        cdf_runtime::CanonicalPartitionSchedule::compile(&forged_source, &plan.scan).unwrap();
    plan.compiled_schema_admission.source =
        Some(cdf_runtime::CompiledSourceCompilerBinding::compile(&forged_compiler_source).unwrap());
    plan.compiled_source_execution = Some(forged_source);
    plan.partition_schedule = Some(forged_schedule.clone());
    plan.explain.partition_schedule = Some(forged_schedule);
    plan.compiled_stream_policy = None;
    plan.explain.compiled_stream_policy = None;
    let package = TempDir::new().unwrap();

    let error = block_on(execute_to_package(&plan, &resource, package.path())).unwrap_err();

    assert!(
        error
            .message
            .contains("resolved source does not match the compiler source artifact"),
        "{error}"
    );
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
}

#[test]
fn fused_transform_reserves_before_allocation_and_releases_after_persist() {
    let resource = MockResource::tier_a(sample_batches());
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(vec![], None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let pre_finalize =
        |_: &cdf_package::PackageBuilder, _: EnginePackageDraft<'_>| -> Result<()> { Ok(()) };
    let (_, services) =
        StandaloneExecutionHost::default_services_with_spill(64 * 1024 * 1024, 1024 * 1024)
            .unwrap();
    let output_dir = TempDir::new().unwrap();
    block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        output_dir.path(),
        &pre_finalize,
        EngineExecutionConfig::default().with_execution_services(services.clone()),
    ))
    .unwrap();
    let memory = services.memory().snapshot();
    assert!(memory.consumers.iter().any(|(consumer, usage)| {
        consumer.class == cdf_memory::MemoryClass::Transform && usage.peak_bytes > 0
    }));
    assert_eq!(memory.current_bytes, 0);

    let (_, tiny_services) =
        StandaloneExecutionHost::default_services_with_spill(64, 1024).unwrap();
    let failed_dir = TempDir::new().unwrap();
    let error = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        failed_dir.path(),
        &pre_finalize,
        EngineExecutionConfig::default().with_execution_services(tiny_services.clone()),
    ))
    .unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(error.message.contains("exceeds managed budget"));
    assert_eq!(tiny_services.memory().snapshot().current_bytes, 0);
}
