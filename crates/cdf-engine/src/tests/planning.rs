use super::support::{
    Arc, AtomicUsize, BTreeMap, BackpressureSupport, Batch, BatchStream, BooleanArray,
    CapabilitySupport, ContentObjectKey, ContentProviderGeneration, ContentStoreNamespace,
    CursorPosition, CursorValue, DataType, DeliveryGuarantee, EXECUTION_EXTENT_VERSION,
    EngineExecutionConfig, EnginePackageDraft, EnginePlan, EstimateSupport, ExecutionExtent, Field,
    FilterCapabilities, IncrementalShape, Int32Array, MockResource, Mutex, OperatorNode, Ordering,
    PLANNED_TASK_SET_REFERENCE_VERSION, PackageStatus, PartitionAuthority, PartitionId,
    PartitionPlan, PartitioningCapabilities, PlannedTaskSetReference, Planner, Poll,
    PushdownFidelity, QueryableResource, QueryableResourceTableProvider, RecordBatch,
    ResourceCapabilities, ResourceDescriptor, ResourceStream, SafeFrontierPolicy, ScanPlan,
    ScanRequest, Schema, SchemaRef, ScopeKey, SessionContext, SourcePosition,
    StandaloneExecutionHost, StringArray, TempDir, TimeUnit, WriteDisposition,
    assert_explain_carries_required_fields, assert_honest_cdf_native_operator_metadata,
    batch_strings, block_on, col, collect_stream, compile_operator_graph,
    datafusion_filter_pushdown, descriptor, execute_to_package,
    execute_to_package_with_segment_positions_and_pre_finalize, lit, mock_compiled_source_plan,
    plan_input, plan_input_for_schema, queryable_resource_table_provider, read_package_segment,
    sample_batches, sample_schema, sample_stream_epoch_policy, stream, terminal_file_position,
};
use super::support::{Result, TableProvider};
use std::ops::Add;

#[test]
fn merge_planning_synthesizes_fail_closed_package_key_authority() {
    let resource = MockResource::tier_a(sample_batches());
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let rule = cdf_contract::package_dedup_rule(&plan.validation_program)
        .unwrap()
        .unwrap();
    assert_eq!(rule.keys, ["id"]);
    assert_eq!(rule.keep, cdf_contract::DedupKeepProgram::Fail);
}

#[test]
fn tier_a_resource_runs_engine_projection_filter_limit_into_package() {
    let mut batches = sample_batches();
    for batch in &mut batches {
        batch.header.source_position = Some(terminal_file_position());
    }
    let resource = MockResource::tier_a(batches).without_control_keys();
    let input = plan_input(
        vec!["id > 1", "active = true"],
        Some(vec!["name".to_owned()]),
        Some(1),
        ExecutionExtent::bounded(),
    );
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();

    assert_eq!(plan.explain.pushed_predicates, Vec::new());
    assert_eq!(plan.explain.unsupported_predicates.len(), 2);

    let temp = TempDir::new().unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let output = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        temp.path(),
        &pre_finalize,
        EngineExecutionConfig::default().with_statistics_profile(true),
    ))
    .unwrap()
    .output;

    assert_eq!(output.manifest.lifecycle.status, PackageStatus::Packaged);
    assert_eq!(output.profile.output_rows, 1);
    assert!(output.profile.output_bytes > 0);
    assert_eq!(output.identity_segments().len(), 1);
    assert_eq!(output.profile.statistics.columns[0].field_path.len(), 1);
    assert_eq!(
        output.profile.statistics.columns[0].field_path[0].as_ref(),
        "name"
    );
    assert_eq!(
        output.profile.statistics.columns[0].minimum,
        Some(cdf_kernel::TypedScalar::Utf8("two".into()))
    );
    assert!(!temp.path().join("stats/profile.json").exists());
    assert!(
        temp.path()
            .join(cdf_package::STATISTICS_PROFILE_FILE)
            .exists()
    );
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let verified = reader.verify_for_consumption().unwrap();
    let mut profile_rows = Vec::new();
    reader
        .for_each_verified_statistics_profile(&verified, &mut |row| {
            profile_rows.push(row);
            Ok(())
        })
        .unwrap();
    assert!(profile_rows.iter().any(|row| {
        row.grain == cdf_package::StatisticsProfileGrain::Package
            && row.field_path[0].as_ref() == "name"
            && row.minimum == Some(cdf_kernel::TypedScalar::Utf8("two".into()))
    }));

    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = read_package_segment(&reader, &output.identity_segments()[0].segment_id);
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
fn tier_a_rejects_partition_intent_that_claims_source_pushdown() {
    let intent = cdf_kernel::CompiledScanIntent {
        version: cdf_kernel::COMPILED_SCAN_INTENT_VERSION,
        projection: Some(vec!["name".to_owned()]),
        predicates: Vec::new(),
        limit: Some(1),
        order_by: Vec::new(),
    };
    let resource = MockResource::tier_a(sample_batches()).with_tier_a_intent(intent);
    let error = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(
                Vec::new(),
                Some(vec!["name".to_owned()]),
                Some(1),
                ExecutionExtent::bounded(),
            ),
        )
        .unwrap_err();
    assert!(error.message.contains("Tier-A partition"));
    assert!(error.message.contains("full-scan intent"));
}

#[test]
fn tier_b_planning_binds_the_committed_frontier_during_source_negotiation() {
    let resource = MockResource::tier_b(sample_batches()).with_partition_count(1);
    let frontier = SourcePosition::Cursor(CursorPosition {
        version: cdf_kernel::SOURCE_POSITION_VERSION,
        field: "id".to_owned(),
        value: CursorValue::I64(7),
    });
    let mut input = plan_input(Vec::new(), None, None, ExecutionExtent::bounded());
    input.committed_frontier = Some(frontier.clone());

    let plan = Planner::new().plan_tier_b(&resource, input).unwrap();

    assert_eq!(
        resource.negotiated_frontier.lock().unwrap().as_ref(),
        Some(&frontier)
    );
    assert_eq!(plan.initial_committed_frontier, Some(frontier.clone()));
    assert_eq!(
        plan.scan.inline_partitions().unwrap()[0].start_position,
        Some(frontier)
    );
}

#[test]
fn engine_plan_requires_recorded_schema_authorities() {
    let resource = MockResource::tier_a(Vec::new());
    let mut plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(vec![], None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    assert!(
        plan.validate_partition_schedule()
            .unwrap_err()
            .message
            .contains("requires compiled source")
    );
    let source = mock_compiled_source_plan(&resource, None);
    let mut unbounded = source.clone();
    unbounded.execution_capabilities.bounded = false;
    unbounded.stream_capabilities = Some(cdf_runtime::SourceStreamCapabilities {
        quiescence: false,
        watermark_behavior: cdf_kernel::OperatorWatermarkBehavior::Drop,
        watermark: None,
        safe_frontiers: vec![SafeFrontierPolicy::CanonicalAdmittedSourcePosition],
        source_frontiers: vec![cdf_runtime::SourceFrontierCapability::Cursor {
            fields: vec!["id".to_owned()],
        }],
        idleness_capabilities: Vec::new(),
    });
    assert!(
        plan.clone()
            .bind_compiled_source(&unbounded)
            .unwrap_err()
            .message
            .contains("declare a complete drain policy")
    );
    resource.bind_compiled_source(&source);
    plan = plan.bind_compiled_source(&source).unwrap();
    assert_eq!(
        plan.compiled_stream_policy,
        plan.explain.compiled_stream_policy
    );
    assert!(plan.compiled_stream_policy.is_none());
    let serialized = serde_json::to_value(&plan).unwrap();
    assert!(serialized.get("compiled_stream_policy").is_none());
    assert!(
        serialized["explain"]
            .get("compiled_stream_policy")
            .is_none()
    );
    for required in [
        "schema_authority",
        "output_schema",
        "compiled_schema_admission",
    ] {
        let mut incomplete = serde_json::to_value(&plan).unwrap();
        incomplete.as_object_mut().unwrap().remove(required);
        let error = serde_json::from_value::<EnginePlan>(incomplete).unwrap_err();
        assert!(error.to_string().contains(required));
    }
    for required in ["compiled_source_execution", "partition_schedule"] {
        let mut incomplete = serde_json::to_value(&plan).unwrap();
        incomplete.as_object_mut().unwrap().remove(required);
        let incomplete: EnginePlan = serde_json::from_value(incomplete).unwrap();
        let error = incomplete.validate_partition_schedule().unwrap_err();
        assert!(
            error.message.contains("must be present together")
                || error
                    .message
                    .contains("does not match its recorded explain")
        );
    }
}

#[test]
fn tier_b_exact_temporal_pushdown_selects_recorded_source_lowering_without_residual() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "updated_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ]));
    let resource = MockResource::tier_b(Vec::new()).with_schema(schema.clone());
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input_for_schema(
                schema,
                vec!["updated_at >= '2026-07-12T00:00:00Z'"],
                None,
                None,
                ExecutionExtent::bounded(),
            ),
        )
        .unwrap();

    assert!(plan.residual_predicates.is_empty());
    assert!(plan.compiled_expression_plan.residuals.is_empty());
    assert_eq!(
        plan.compiled_expression_plan.predicates[0].optimizer.name,
        cdf_contract::SOURCE_EXACT_PUSHDOWN_OPTIMIZER
    );
    plan.validate_compiled_expression_plan().unwrap();
}

#[test]
fn tier_b_negotiates_pushdown_fidelity_without_io() {
    let resource = MockResource::tier_b(sample_batches());
    let input = plan_input(
        vec!["id > 1", "active = true", "name != 'missing'"],
        Some(vec!["name".to_owned()]),
        Some(10),
        ExecutionExtent::bounded(),
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
        ExecutionExtent::bounded(),
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
fn engine_plan_deserialization_rejects_missing_required_execution_policy() {
    let resource =
        MockResource::tier_a(sample_batches()).with_write_disposition(WriteDisposition::Append);
    let input = plan_input(Vec::new(), None, None, ExecutionExtent::bounded());
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let mut plan_json = serde_json::to_value(&plan).unwrap();
    plan_json
        .as_object_mut()
        .unwrap()
        .remove("execution_extent");
    let error = serde_json::from_value::<EnginePlan>(plan_json).unwrap_err();
    assert!(error.to_string().contains("execution_extent"));

    let mut plan_json = serde_json::to_value(&plan).unwrap();
    plan_json
        .as_object_mut()
        .unwrap()
        .remove("write_disposition");
    let error = serde_json::from_value::<EnginePlan>(plan_json).unwrap_err();
    assert!(error.to_string().contains("write_disposition"));

    let mut plan_json = serde_json::to_value(&plan).unwrap();
    for operator in plan_json["operator_chain"].as_array_mut().unwrap() {
        if operator["kind"] == "package_sink" {
            operator.as_object_mut().unwrap().remove("segmentation");
        }
    }
    let error = serde_json::from_value::<EnginePlan>(plan_json).unwrap_err();
    assert!(error.to_string().contains("segmentation"));
}

#[test]
fn resident_execution_plan_is_rejected_until_supervisor_exists() {
    let resource = MockResource::tier_a(sample_batches());
    let input = plan_input(
        vec![],
        None,
        None,
        ExecutionExtent::Resident {
            version: EXECUTION_EXTENT_VERSION,
            policy: sample_stream_epoch_policy(),
        },
    );
    let error = Planner::new().plan_tier_a(&resource, input).unwrap_err();

    assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
    assert!(error.message.contains("resident execution is not enabled"));
}

#[test]
fn execution_extent_is_not_redefined_by_the_engine() {
    let source_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for entry in std::fs::read_dir(source_dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(std::ffi::OsStr::to_str) != Some("rs")
            || path.file_name().and_then(std::ffi::OsStr::to_str) == Some("tests.rs")
        {
            continue;
        }
        let source = std::fs::read_to_string(&path).unwrap();
        for forbidden in ["PlanBoundedness", "UnboundedLive", "UnboundedDrain"] {
            assert!(
                !source.contains(forbidden),
                "{} contains obsolete execution-extent authority {forbidden}",
                path.display()
            );
        }
        for line in source.lines() {
            let tokens = line.split_whitespace().collect::<Vec<_>>();
            for declaration in tokens.windows(2) {
                assert!(
                    !matches!(declaration[0], "enum" | "struct")
                        || !declaration[1].trim_end_matches('{').contains("Extent"),
                    "{} defines engine-owned extent type on line `{}`",
                    path.display(),
                    line.trim()
                );
            }
        }
    }
}

#[test]
fn external_partition_schedule_preserves_ordinals_above_u32_without_enumeration() {
    let resource = MockResource::tier_b(sample_batches());
    let input = plan_input(Vec::new(), None, None, ExecutionExtent::bounded());
    let mut scan = resource.negotiate(&input.request).unwrap();
    let representative_partition = scan.inline_partitions().unwrap()[0].clone();
    let high_ordinal = u64::from(u32::MAX) + 17;
    let task_count = high_ordinal + 1;
    scan = scan
        .try_map_partition_authority(|authority| match authority {
            PartitionAuthority::Inline(_) => {
                Ok(PartitionAuthority::External(PlannedTaskSetReference {
                    version: PLANNED_TASK_SET_REFERENCE_VERSION,
                    task_type: "portable-partition-v1".to_owned(),
                    task_count,
                    store_namespace: ContentStoreNamespace::new("portable-partition-test").unwrap(),
                    object_key: ContentObjectKey::new("tasks/partitions.jsonl").unwrap(),
                    byte_count: 1,
                    content_sha256:
                        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                            .to_owned(),
                    provider_generation: ContentProviderGeneration::new("generation-1").unwrap(),
                }))
            }
            PartitionAuthority::External(_) => unreachable!(),
        })
        .unwrap();
    let compiled_source = mock_compiled_source_plan(&resource, None);
    let execution_source =
        cdf_runtime::CompiledSourceExecutionPlan::compile(&compiled_source).unwrap();

    let schedule =
        cdf_runtime::CanonicalPartitionSchedule::compile(&execution_source, &scan).unwrap();
    let scheduled = schedule
        .scheduled_partition(&execution_source, high_ordinal, &representative_partition)
        .unwrap();

    assert_eq!(schedule.partition_count(), task_count);
    assert!(schedule.inline_partitions().is_none());
    assert_eq!(scheduled.ordinal.get(), high_ordinal);
}

#[test]
fn operator_graph_compiles_from_capabilities_without_driver_name_dispatch() {
    let resource = MockResource::tier_b(sample_batches());
    let mut plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    for operator in &mut plan.operator_chain {
        if let OperatorNode::PackageSink { segmentation, .. } = operator {
            segmentation.target_rows = 1;
            segmentation.maximum_rows = 1;
            segmentation.microbatch_minimum_rows = 1;
            segmentation.microbatch_maximum_rows = 1;
        }
    }
    let source = mock_compiled_source_plan(&resource, None);
    resource.bind_compiled_source(&source);

    let graph = compile_operator_graph(
        &plan,
        &source,
        &cdf_runtime::DestinationRuntimeCapabilities::default(),
    )
    .unwrap();

    graph.validate().unwrap();
    graph
        .validate_destination_join(&cdf_runtime::DestinationRuntimeCapabilities::default())
        .unwrap();
    let stale_staged = cdf_runtime::DestinationRuntimeCapabilities {
        ingress_mode: cdf_runtime::DestinationIngressMode::StagedDurableSegments,
        staged_ingress: Some(cdf_runtime::StagedIngressCapabilities {
            recovery: cdf_runtime::StagingRecoveryMode::RollbackRedrive,
            visibility: cdf_runtime::StagingVisibility::IsolatedUntilFinalBinding,
            abort_idempotent: true,
            lifecycle_cleanup: true,
            final_binding_requires_exclusive_writer: false,
        }),
        max_in_flight_bytes: Some(64 * 1024 * 1024),
        ..Default::default()
    };
    assert!(graph.validate_destination_join(&stale_staged).is_err());
    assert_eq!(graph.nodes[0].implementation_version, "mock-v1");
    assert_eq!(graph.execution_extent, plan.execution_extent);
    assert!(
        graph
            .nodes
            .iter()
            .all(|node| node.execution_extent_hash.is_none())
    );
    assert!(
        graph
            .nodes
            .iter()
            .all(|node| node.node_id != "external_mock")
    );
    assert!(graph.edges.iter().any(|edge| {
        edge.transfer == cdf_runtime::GraphEdgeTransfer::Fused
            && edge.producer == "reconcile"
            && edge.consumer == "transform"
    }));
    assert!(graph.edges.iter().any(|edge| {
        edge.transfer == cdf_runtime::GraphEdgeTransfer::Durable
            && edge.producer == "segment_persist"
    }));
    plan = plan.bind_compiled_source(&source).unwrap();
    plan.operator_graph = Some(graph.clone());
    let temp = TempDir::new().unwrap();
    let serial = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();
    let packaged: cdf_runtime::CompiledOperatorGraph = serde_json::from_slice(
        &std::fs::read(temp.path().join("plan/operator-graph.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(packaged, graph);

    let parallel_temp = TempDir::new().unwrap();
    // The encoder conservatively charges 3x the 64 MiB segment ceiling. A 512 MiB logical
    // coordinator budget admits two workers, so jobs=4 can complete segments out of order while
    // the canonical registration frontier remains deterministic.
    let memory: Arc<dyn cdf_memory::MemoryCoordinator> = Arc::new(
        cdf_memory::DeterministicMemoryCoordinator::new(512 * 1024 * 1024, BTreeMap::new())
            .unwrap(),
    );
    let host = Arc::new(
        StandaloneExecutionHost::new(
            cdf_runtime::ExecutionHostCapabilities {
                logical_cpu_slots: 4,
                io_workers: 2,
                blocking_lanes: Vec::new(),
            },
            memory,
        )
        .unwrap(),
    );
    let services = cdf_runtime::ExecutionServices::new(host).unwrap();
    let scheduler = cdf_runtime::resolve_runtime_scheduler(
        plan.scan.partition_count().unwrap(),
        &source.execution_capabilities,
        &cdf_runtime::DestinationRuntimeCapabilities::default(),
        &services,
        Some(4),
    )
    .unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let parallel = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        parallel_temp.path(),
        &pre_finalize,
        EngineExecutionConfig::default()
            .with_execution_services(services.clone())
            .with_scheduler_resolution(scheduler),
    ))
    .unwrap();
    assert!(serial.identity_segments().len() > 1);
    assert_eq!(
        parallel.output.identity_segments().len(),
        serial.identity_segments().len()
    );
    assert_eq!(parallel.output.manifest.identity, serial.manifest.identity);
    assert_eq!(parallel.output.lineage, serial.lineage);
    assert_eq!(
        parallel.output.profile.statistics,
        serial.profile.statistics
    );
    assert_eq!(services.memory().snapshot().current_bytes, 0);

    let mut stale_scheduler = cdf_runtime::resolve_runtime_scheduler(
        plan.scan.partition_count().unwrap(),
        &source.execution_capabilities,
        &cdf_runtime::DestinationRuntimeCapabilities::default(),
        &services,
        Some(4),
    )
    .unwrap();
    stale_scheduler.source_bounded = !source.execution_capabilities.bounded;
    let stale_temp = TempDir::new().unwrap();
    let error = block_on(
        super::execute_to_package_with_segment_positions_and_pre_finalize(
            &plan,
            &resource,
            stale_temp.path(),
            &pre_finalize,
            EngineExecutionConfig::default()
                .with_execution_services(services.clone())
                .with_scheduler_resolution(stale_scheduler)
                .new_invocation(),
        ),
    )
    .unwrap_err();
    assert!(error.message.contains("scheduler source authority"));

    let destination = cdf_runtime::DestinationRuntimeCapabilities {
        blocking_lanes: vec![
            cdf_runtime::BlockingLaneSpec {
                lane_id: "mock.maintenance".to_owned(),
                binding: cdf_runtime::BlockingLaneBinding::Static,
                maximum_concurrency: 1,
                cpu_slot_cost: 1,
                native_internal_parallelism: 1,
                affinity: cdf_runtime::LaneAffinity::Shared,
                interruption: cdf_runtime::InterruptionSafety::CooperativeOnly,
            },
            cdf_runtime::BlockingLaneSpec {
                lane_id: "mock.commit".to_owned(),
                binding: cdf_runtime::BlockingLaneBinding::Static,
                maximum_concurrency: 1,
                cpu_slot_cost: 1,
                native_internal_parallelism: 1,
                affinity: cdf_runtime::LaneAffinity::Pinned,
                interruption: cdf_runtime::InterruptionSafety::CooperativeOnly,
            },
        ],
        final_binding_lane: Some("mock.commit".to_owned()),
        ..cdf_runtime::DestinationRuntimeCapabilities::default()
    };
    let graph = compile_operator_graph(&plan, &source, &destination).unwrap();
    graph.validate_destination_join(&destination).unwrap();
    let binding = graph
        .nodes
        .iter()
        .find(|node| node.node_id == "destination_bind")
        .unwrap();
    assert_eq!(binding.blocking_lane.as_deref(), Some("mock.commit"));
}

#[test]
fn datafusion_table_provider_pushdown_classification_delegates_to_resource() {
    let resource = Arc::new(DataFusionMockResource::new());
    let provider = QueryableResourceTableProvider::new(
        resource.clone(),
        ScopeKey::Resource,
        datafusion_test_services(),
    );
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
    let provider = queryable_resource_table_provider(
        resource.clone(),
        ScopeKey::Resource,
        datafusion_test_services(),
    );
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
    let poll_threads = resource.poll_threads.lock().unwrap();
    assert!(!poll_threads.is_empty());
    assert!(
        poll_threads
            .iter()
            .all(|thread| thread.starts_with("cdf-cpu-")),
        "CDF source polling and adaptation bypassed CPU admission: {poll_threads:?}"
    );
}

#[test]
fn datafusion_unsupported_expression_stays_residual() {
    let resource = Arc::new(DataFusionMockResource::new());
    let provider = QueryableResourceTableProvider::new(
        resource.clone(),
        ScopeKey::Resource,
        datafusion_test_services(),
    );
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
    let provider = QueryableResourceTableProvider::new(
        resource.clone(),
        ScopeKey::Resource,
        datafusion_test_services(),
    );
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
    let provider = QueryableResourceTableProvider::new(
        resource.clone(),
        ScopeKey::Resource,
        datafusion_test_services(),
    );
    let ctx = SessionContext::new();
    let filters = vec![col("id").gt(lit(1_i32))];

    let _plan = block_on(provider.scan(&ctx.state(), None, &filters, Some(1))).unwrap();

    let requests = resource.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].limit, None);
    assert_eq!(requests[1].limit, Some(1));
}

#[test]
fn datafusion_zero_fetch_never_opens_a_source_partition() {
    let resource = Arc::new(DataFusionMockResource::new());
    let provider = QueryableResourceTableProvider::new(
        resource.clone(),
        ScopeKey::Resource,
        datafusion_test_services(),
    );
    let ctx = SessionContext::new();

    let batches = block_on(async {
        let plan = provider
            .scan(&ctx.state(), None, &[], Some(0))
            .await
            .unwrap();
        collect_execution_plan_partitions(plan, ctx.task_ctx()).await
    });

    assert!(batches.is_empty());
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
}

fn datafusion_test_services() -> cdf_runtime::ExecutionServices {
    StandaloneExecutionHost::default_services(64 * 1024 * 1024)
        .unwrap()
        .1
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
    let mut header = batch.header;
    header.set_payload_counts(
        filtered.num_rows() as u64,
        filtered.get_array_memory_size() as u64,
    );
    Ok(Batch {
        header,
        payload: cdf_kernel::BatchPayload::in_memory(filtered),
    })
}

#[derive(Clone)]
struct DataFusionMockResource {
    descriptor: ResourceDescriptor,
    schema: SchemaRef,
    batches: Vec<Batch>,
    negotiate_count: Arc<AtomicUsize>,
    open_count: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<ScanRequest>>>,
    poll_threads: Arc<Mutex<Vec<String>>>,
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
            poll_threads: Arc::new(Mutex::new(Vec::new())),
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

    fn open(&self, partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
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
        let poll_threads = Arc::clone(&self.poll_threads);
        cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async move {
            let mut batches = batches?.into_iter();
            let stream = Box::pin(stream::poll_fn(move |_| {
                poll_threads
                    .lock()
                    .unwrap()
                    .push(std::thread::current().name().unwrap_or_default().to_owned());
                Poll::Ready(batches.next().map(Ok))
            })) as BatchStream;
            Ok(cdf_kernel::PartitionStreamPayload::batches(stream))
        }))
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
        let scan_intent = cdf_kernel::CompiledScanIntent {
            version: cdf_kernel::COMPILED_SCAN_INTENT_VERSION,
            projection: request.projection.clone(),
            predicates: pushed_predicates.clone(),
            limit: request.limit,
            order_by: Vec::new(),
        };
        scan_intent.validate()?;
        let partitions = ["part-0", "part-1"]
            .into_iter()
            .map(|partition| {
                let partition_id = PartitionId::new(partition)?;
                Ok(PartitionPlan {
                    partition_id: partition_id.clone(),
                    scope: ScopeKey::Partition { partition_id },
                    planned_position: None,
                    start_position: None,
                    scan_intent: scan_intent.clone(),
                    retry_safety: cdf_kernel::PartitionRetrySafety::Forbidden,
                    metadata: BTreeMap::from([("exact_filters".to_owned(), exact_filters.clone())]),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(ScanPlan::from_partition_authority(
            cdf_kernel::PlanId::new(format!(
                "df-plan-{}-{}",
                request.resource_id.as_str(),
                self.negotiate_count.load(Ordering::SeqCst)
            ))?,
            request.clone(),
            PartitionAuthority::Inline(partitions),
            pushed_predicates,
            unsupported_predicates,
            Some(6),
            None,
            DeliveryGuarantee::EffectivelyOncePerKey,
        ))
    }
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
