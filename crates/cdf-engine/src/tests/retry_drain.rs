use super::support::{
    Arc, AtomicUsize, BTreeMap, BTreeSet, BackpressureSupport, Batch, CursorPosition, CursorValue,
    DrainTermination, EXECUTION_EXTENT_VERSION, EffectiveSchemaRuntime, EngineDrainEpochOutcome,
    EngineExecutionConfig, EnginePackageDraft, EpochClosureTrigger, EventTimeDomain,
    ExecutionExtent, FileManifest, FilePosition, Int32Array, LATE_DATA_PAYLOAD_CATALOG_FILE,
    LateDataAction, LateDataPayloadCatalog, LateDataPayloadLocation, MockResource, Mutex, Ordering,
    PartitionAttestation, PartitionId, PartitionPlan, Planner, QueryableResource,
    RedactionDecision, ResourceCapabilities, ResourceDescriptor, ResourceStream,
    STREAM_EPOCH_POLICY_VERSION, SafeFrontierPolicy, ScanPlan, ScanRequest, SchemaRef,
    SourcePosition, StandaloneExecutionHost, StreamEpochPolicy, TempDir, WATERMARK_CLAIM_VERSION,
    WatermarkAuthority, WatermarkClaim, WatermarkObservationContext, WatermarkPolicy,
    WatermarkValue, batch_for_partition, block_on, collect_quarantine_records,
    compile_operator_graph, executable_mock_options, execute_to_package,
    execute_to_package_with_segment_positions,
    execute_to_package_with_segment_positions_and_pre_finalize, fast_test_retry_policy,
    missing_control_field_batch, mock_compiled_source_plan,
    mock_compiled_source_plan_with_speculation, mock_unbounded_source_plan, plan_input,
    sample_batches, sample_schema, sample_stream_epoch_policy, stream, terminal_file_position,
};
use super::support::{Result, StreamExt};

#[test]
fn execution_rejects_drain_extent_before_source_contact() {
    let resource = MockResource::tier_a(sample_batches());
    let mut plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let drain = ExecutionExtent::Drain {
        version: EXECUTION_EXTENT_VERSION,
        policy: sample_stream_epoch_policy(),
        termination: DrainTermination::Records { count: 10 },
    };
    plan.execution_extent = drain.clone();
    plan.explain.execution_extent = drain;

    let temp = TempDir::new().unwrap();
    let error = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap_err();
    assert!(
        error
            .message
            .contains("bounded and cannot use drain execution")
    );
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
    assert!(std::fs::read_dir(temp.path()).unwrap().next().is_none());
}

#[test]
fn drain_epochs_stop_at_canonical_partition_frontiers_and_require_settlement() {
    let mut batches = sample_batches();
    for (ordinal, batch) in batches.iter_mut().enumerate() {
        batch.header.source_position = Some(SourcePosition::FileManifest(FileManifest {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            files: vec![FilePosition {
                path: format!("input-{ordinal}.arrow"),
                size_bytes: batch.header.byte_count,
                source_generation: Some(format!("generation-{ordinal}")),
                etag: None,
                object_version: None,
                sha256: None,
            }],
        }));
    }
    let resource = MockResource::tier_b(batches).without_control_keys();
    let extent = ExecutionExtent::Drain {
        version: EXECUTION_EXTENT_VERSION,
        policy: StreamEpochPolicy {
            version: STREAM_EPOCH_POLICY_VERSION,
            checkpoint_cadence: EpochClosureTrigger::Rows { count: 3 },
            package_rotation: EpochClosureTrigger::Bytes { count: 1 << 20 },
            watermark: WatermarkPolicy::Disabled,
            late_data: LateDataAction::Quarantine,
            safe_frontier: SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
        },
        termination: DrainTermination::Records { count: 6 },
    };
    let source = mock_unbounded_source_plan(&resource);
    resource.bind_compiled_source(&source);
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, extent.clone()),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap();
    let mut controller = cdf_runtime::DrainEpochController::new(&extent).unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let root = TempDir::new().unwrap();
    let first_dir = root.path().join("epoch-0");
    let first = block_on(super::execute_drain_epoch_with_hooks(
        &plan,
        &resource,
        &first_dir,
        &pre_finalize,
        super::DrainEpochExecution::new(&mut controller),
        executable_mock_options(EngineExecutionConfig::default()).unwrap(),
    ))
    .unwrap()
    .into_package()
    .unwrap();
    let first_epoch = first.drain_epoch.as_ref().unwrap();
    assert_eq!(first_epoch.consumed_partition_count, 1);
    assert_eq!(first.output.profile.output_rows, 3);
    assert!(matches!(
        first_epoch.closure.evidence.cause,
        cdf_kernel::EpochClosureCause::CheckpointCadence { .. }
    ));
    assert!(first_dir.join("plan/epoch-frontier.json").is_file());
    let opened_after_first = resource.open_count.load(Ordering::SeqCst);

    let selected = BTreeSet::from([PartitionId::new("part-1").unwrap()]);
    let second_plan = plan
        .clone()
        .select_partitions(&selected)
        .unwrap()
        .rebind_package_id("pkg-engine-test-e000001")
        .unwrap();
    let blocked_dir = root.path().join("blocked");
    let blocked = block_on(super::execute_drain_epoch_with_hooks(
        &second_plan,
        &resource,
        &blocked_dir,
        &pre_finalize,
        super::DrainEpochExecution::new(&mut controller),
        executable_mock_options(EngineExecutionConfig::default()).unwrap(),
    ))
    .unwrap_err();
    assert!(blocked.message.contains("before frontier settlement"));
    assert_eq!(
        resource.open_count.load(Ordering::SeqCst),
        opened_after_first
    );
    controller
        .acknowledge_settlement(&first_epoch.closure.frontier.frontier)
        .unwrap();

    let second_dir = root.path().join("epoch-1");
    let second = block_on(super::execute_drain_epoch_with_hooks(
        &second_plan,
        &resource,
        &second_dir,
        &pre_finalize,
        super::DrainEpochExecution::new(&mut controller),
        executable_mock_options(EngineExecutionConfig::default()).unwrap(),
    ))
    .unwrap()
    .into_package()
    .unwrap();
    let second_epoch = second.drain_epoch.as_ref().unwrap();
    assert_eq!(second_epoch.consumed_partition_count, 1);
    assert!(second_epoch.closure.terminate_after_settlement);
    assert!(matches!(
        second_epoch.closure.evidence.cause,
        cdf_kernel::EpochClosureCause::DrainTermination {
            termination: DrainTermination::Records { count: 6 }
        }
    ));
    controller
        .acknowledge_settlement(&second_epoch.closure.frontier.frontier)
        .unwrap();
    assert!(controller.is_finished());
}

#[test]
fn drain_epochs_resume_one_unbounded_partition_from_each_settled_batch_frontier() {
    let mut batches = [1_i64, 2, 3]
        .into_iter()
        .map(|position| {
            let mut batch = batch_for_partition(
                &format!("batch-{position}"),
                "part-0",
                vec![i32::try_from(position).unwrap()],
                vec!["event"],
                vec![true],
            );
            batch.header.source_position = Some(SourcePosition::Cursor(CursorPosition {
                version: cdf_kernel::SOURCE_POSITION_VERSION,
                field: "id".to_owned(),
                value: CursorValue::I64(position),
            }));
            batch
        })
        .collect::<Vec<_>>();
    let resource = MockResource::tier_a(std::mem::take(&mut batches)).without_control_keys();
    let extent = ExecutionExtent::Drain {
        version: EXECUTION_EXTENT_VERSION,
        policy: StreamEpochPolicy {
            version: STREAM_EPOCH_POLICY_VERSION,
            checkpoint_cadence: EpochClosureTrigger::Rows { count: 1 },
            package_rotation: EpochClosureTrigger::Bytes { count: 1 << 20 },
            watermark: WatermarkPolicy::Disabled,
            late_data: LateDataAction::Quarantine,
            safe_frontier: SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
        },
        termination: DrainTermination::Records { count: 3 },
    };
    let source = mock_unbounded_cursor_source_plan(&resource);
    resource.bind_compiled_source(&source);
    let mut plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, extent.clone()),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap();
    let mut controller = cdf_runtime::DrainEpochController::new(&extent).unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let root = TempDir::new().unwrap();

    let fresh_process_plan = plan
        .clone()
        .rebind_initial_committed_frontier(
            &resource,
            &SourcePosition::Cursor(CursorPosition {
                version: cdf_kernel::SOURCE_POSITION_VERSION,
                field: "id".to_owned(),
                value: CursorValue::I64(1),
            }),
        )
        .unwrap();
    assert_eq!(
        fresh_process_plan.scan.inline_partitions().unwrap()[0].start_position,
        Some(SourcePosition::Cursor(CursorPosition {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            field: "id".to_owned(),
            value: CursorValue::I64(1),
        }))
    );

    for epoch in 0..3_u64 {
        let package_id = format!("pkg-cursor-epoch-{epoch}");
        plan = plan.rebind_package_id(package_id).unwrap();
        let output = block_on(super::execute_drain_epoch_with_hooks(
            &plan,
            &resource,
            root.path().join(format!("epoch-{epoch}")),
            &pre_finalize,
            super::DrainEpochExecution::new(&mut controller),
            executable_mock_options(EngineExecutionConfig::default()).unwrap(),
        ))
        .unwrap()
        .into_package()
        .unwrap();
        let drain = output.drain_epoch.as_ref().unwrap();
        assert_eq!(output.output.profile.output_rows, 1);
        assert_eq!(drain.consumed_partition_count, 0);
        assert_eq!(
            drain
                .resume_partition
                .as_deref()
                .map(|resume| &resume.start_position),
            Some(&SourcePosition::Cursor(CursorPosition {
                version: cdf_kernel::SOURCE_POSITION_VERSION,
                field: "id".to_owned(),
                value: CursorValue::I64(i64::try_from(epoch + 1).unwrap()),
            }))
        );
        assert_eq!(
            output.execution_evidence().processed_observations().len(),
            1
        );
        assert!(output.execution_evidence().checkpoint_eligible());
        controller
            .acknowledge_settlement(&drain.closure.frontier.frontier)
            .unwrap();
        plan.advance_committed_drain_frontier(
            drain.consumed_partition_count,
            drain.resume_partition.as_deref(),
        )
        .unwrap();
    }

    assert!(controller.is_finished());
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 3);
    assert_eq!(resource.batch_poll_count.load(Ordering::SeqCst), 3);
}

#[test]
fn duration_drain_closes_while_the_next_batch_poll_is_silent() {
    let mut batch = batch_for_partition("batch-1", "part-0", vec![1], vec!["event"], vec![true]);
    batch.header.source_position = Some(SourcePosition::Cursor(CursorPosition {
        version: cdf_kernel::SOURCE_POSITION_VERSION,
        field: "id".to_owned(),
        value: CursorValue::I64(1),
    }));
    let resource = MockResource::tier_a(vec![batch])
        .without_control_keys()
        .with_stall_after_batches();
    let extent = ExecutionExtent::Drain {
        version: EXECUTION_EXTENT_VERSION,
        policy: StreamEpochPolicy {
            version: STREAM_EPOCH_POLICY_VERSION,
            checkpoint_cadence: EpochClosureTrigger::Elapsed {
                milliseconds: 60_000,
            },
            package_rotation: EpochClosureTrigger::Bytes { count: 1 << 20 },
            watermark: WatermarkPolicy::Disabled,
            late_data: LateDataAction::Quarantine,
            safe_frontier: SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
        },
        termination: DrainTermination::Duration { milliseconds: 25 },
    };
    let source = mock_unbounded_cursor_source_plan(&resource);
    resource.bind_compiled_source(&source);
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, extent.clone()),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap();
    let mut controller = cdf_runtime::DrainEpochController::new(&extent).unwrap();
    let root = TempDir::new().unwrap();
    let package_dir = root.path().join("duration-epoch");

    let output = block_on(super::execute_drain_epoch_with_hooks(
        &plan,
        &resource,
        &package_dir,
        &|_, _| Ok(()),
        super::DrainEpochExecution::new(&mut controller),
        executable_mock_options(EngineExecutionConfig::default()).unwrap(),
    ))
    .unwrap()
    .into_package()
    .unwrap();

    assert_eq!(output.output.profile.output_rows, 1);
    let drain = output.drain_epoch.unwrap();
    assert!(matches!(
        drain.closure.evidence.cause,
        cdf_kernel::EpochClosureCause::DrainTermination {
            termination: DrainTermination::Duration { milliseconds: 25 }
        }
    ));
    assert_eq!(
        drain
            .resume_partition
            .as_deref()
            .map(|resume| &resume.start_position),
        Some(&SourcePosition::Cursor(CursorPosition {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            field: "id".to_owned(),
            value: CursorValue::I64(1),
        }))
    );
}

#[test]
fn duration_drain_discards_an_empty_package_while_source_open_is_silent() {
    let resource = MockResource::tier_a(Vec::new())
        .without_control_keys()
        .with_stall_after_batches();
    let extent = ExecutionExtent::Drain {
        version: EXECUTION_EXTENT_VERSION,
        policy: StreamEpochPolicy {
            version: STREAM_EPOCH_POLICY_VERSION,
            checkpoint_cadence: EpochClosureTrigger::Elapsed {
                milliseconds: 60_000,
            },
            package_rotation: EpochClosureTrigger::Bytes { count: 1 << 20 },
            watermark: WatermarkPolicy::Disabled,
            late_data: LateDataAction::Quarantine,
            safe_frontier: SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
        },
        termination: DrainTermination::Duration { milliseconds: 20 },
    };
    let source = mock_unbounded_cursor_source_plan(&resource);
    resource.bind_compiled_source(&source);
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, extent.clone()),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap();
    let mut controller = cdf_runtime::DrainEpochController::new(&extent).unwrap();
    let root = TempDir::new().unwrap();
    let package_dir = root.path().join("empty-duration-epoch");

    let outcome = block_on(super::execute_drain_epoch_with_hooks(
        &plan,
        &resource,
        &package_dir,
        &|_, _| Ok(()),
        super::DrainEpochExecution::new(&mut controller),
        executable_mock_options(EngineExecutionConfig::default()).unwrap(),
    ))
    .unwrap();

    assert!(matches!(
        outcome,
        EngineDrainEpochOutcome::FinishedNoOp { .. }
    ));
    assert!(!package_dir.exists());
    assert!(controller.is_finished());
}

#[test]
fn immediately_exhausted_drain_is_a_no_op_without_a_package() {
    let resource = MockResource::tier_a(Vec::new()).without_control_keys();
    let extent = ExecutionExtent::Drain {
        version: EXECUTION_EXTENT_VERSION,
        policy: StreamEpochPolicy {
            version: STREAM_EPOCH_POLICY_VERSION,
            checkpoint_cadence: EpochClosureTrigger::Rows { count: 1 },
            package_rotation: EpochClosureTrigger::Bytes { count: 1 << 20 },
            watermark: WatermarkPolicy::Disabled,
            late_data: LateDataAction::Quarantine,
            safe_frontier: SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
        },
        termination: DrainTermination::Duration {
            milliseconds: 60_000,
        },
    };
    let source = mock_unbounded_cursor_source_plan(&resource);
    resource.bind_compiled_source(&source);
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, extent.clone()),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap();
    let mut controller = cdf_runtime::DrainEpochController::new(&extent).unwrap();
    let root = TempDir::new().unwrap();
    let package_dir = root.path().join("empty");

    let outcome = block_on(super::execute_drain_epoch_with_hooks(
        &plan,
        &resource,
        &package_dir,
        &|_, _| Ok(()),
        super::DrainEpochExecution::new(&mut controller),
        executable_mock_options(EngineExecutionConfig::default()).unwrap(),
    ))
    .unwrap();

    assert!(matches!(
        outcome,
        EngineDrainEpochOutcome::FinishedNoOp { .. }
    ));
    assert!(controller.is_finished());
    assert!(!package_dir.exists());
}

#[test]
fn drain_partition_resume_stays_local_when_resource_frontier_is_a_larger_cursor() {
    let batches = [("part-0", 100_i64), ("part-1", 1), ("part-1", 2)]
        .into_iter()
        .map(|(partition, position)| {
            let mut batch = batch_for_partition(
                &format!("{partition}-{position}"),
                partition,
                vec![i32::try_from(position).unwrap()],
                vec!["event"],
                vec![true],
            );
            batch.header.source_position = Some(SourcePosition::Cursor(CursorPosition {
                version: cdf_kernel::SOURCE_POSITION_VERSION,
                field: "id".to_owned(),
                value: CursorValue::I64(position),
            }));
            batch
        })
        .collect::<Vec<_>>();
    let resource = MockResource::tier_b(batches).without_control_keys();
    let extent = ExecutionExtent::Drain {
        version: EXECUTION_EXTENT_VERSION,
        policy: StreamEpochPolicy {
            version: STREAM_EPOCH_POLICY_VERSION,
            checkpoint_cadence: EpochClosureTrigger::Rows { count: 2 },
            package_rotation: EpochClosureTrigger::Bytes { count: 1 << 20 },
            watermark: WatermarkPolicy::Disabled,
            late_data: LateDataAction::Quarantine,
            safe_frontier: SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
        },
        termination: DrainTermination::Records { count: 3 },
    };
    let source = mock_unbounded_cursor_source_plan(&resource);
    resource.bind_compiled_source(&source);
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, extent.clone()),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap();
    let mut controller = cdf_runtime::DrainEpochController::new(&extent).unwrap();
    let root = TempDir::new().unwrap();
    let output = block_on(super::execute_drain_epoch_with_hooks(
        &plan,
        &resource,
        root.path().join("epoch-0"),
        &|_, _| Ok(()),
        super::DrainEpochExecution::new(&mut controller),
        executable_mock_options(EngineExecutionConfig::default()).unwrap(),
    ))
    .unwrap()
    .into_package()
    .unwrap();
    let drain = output.drain_epoch.unwrap();
    assert_eq!(drain.consumed_partition_count, 1);
    assert_eq!(
        drain.closure.frontier.frontier,
        SourcePosition::Cursor(CursorPosition {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            field: "id".to_owned(),
            value: CursorValue::I64(100),
        })
    );
    assert_eq!(
        drain
            .resume_partition
            .as_deref()
            .map(|resume| &resume.start_position),
        Some(&SourcePosition::Cursor(CursorPosition {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            field: "id".to_owned(),
            value: CursorValue::I64(1),
        }))
    );
    let SourcePosition::Composite(continuation) = drain
        .closure
        .frontier
        .carryover
        .as_ref()
        .expect("durable partition continuation")
    else {
        panic!("multi-partition drain continuation must be partition-keyed");
    };
    assert_eq!(
        continuation.positions.get("part-0"),
        Some(&SourcePosition::Cursor(CursorPosition {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            field: "id".to_owned(),
            value: CursorValue::I64(100),
        }))
    );
    assert_eq!(
        continuation.positions.get("part-1"),
        Some(&SourcePosition::Cursor(CursorPosition {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            field: "id".to_owned(),
            value: CursorValue::I64(1),
        }))
    );

    let restarted = plan
        .clone()
        .rebind_initial_committed_frontier(
            &resource,
            &SourcePosition::Composite(continuation.clone()),
        )
        .unwrap();
    assert_eq!(
        restarted.scan.inline_partitions().unwrap()[1].start_position,
        continuation.positions.get("part-1").cloned()
    );
}

#[test]
fn drain_epoch_records_the_minimum_partition_watermark_not_the_latest_claim() {
    fn claim(partition: &str, value: i64) -> WatermarkClaim {
        WatermarkClaim {
            version: WATERMARK_CLAIM_VERSION,
            policy_version: STREAM_EPOCH_POLICY_VERSION,
            event_time_field: "id".into(),
            domain: EventTimeDomain::SignedInteger,
            value: WatermarkValue::Signed(value),
            partition_id: PartitionId::new(partition).unwrap(),
            source_position: SourcePosition::Cursor(CursorPosition {
                version: cdf_kernel::SOURCE_POSITION_VERSION,
                field: "id".to_owned(),
                value: CursorValue::I64(value),
            }),
            authority: WatermarkAuthority::Source,
            observation_context: WatermarkObservationContext::SourcePoll,
        }
    }

    let mut batches = [("part-0", 100_i64), ("part-1", 5_i64)]
        .into_iter()
        .map(|(partition, value)| {
            let mut batch = batch_for_partition(
                &format!("{partition}-{value}"),
                partition,
                vec![i32::try_from(value).unwrap()],
                vec!["event"],
                vec![true],
            );
            batch.header.source_position = Some(SourcePosition::Cursor(CursorPosition {
                version: cdf_kernel::SOURCE_POSITION_VERSION,
                field: "id".to_owned(),
                value: CursorValue::I64(value),
            }));
            batch.header.watermarks.push(claim(partition, value));
            batch
        })
        .collect::<Vec<_>>();
    let resource = MockResource::tier_b(std::mem::take(&mut batches)).without_control_keys();
    let extent = ExecutionExtent::Drain {
        version: EXECUTION_EXTENT_VERSION,
        policy: StreamEpochPolicy {
            version: STREAM_EPOCH_POLICY_VERSION,
            checkpoint_cadence: EpochClosureTrigger::Rows { count: 2 },
            package_rotation: EpochClosureTrigger::Bytes { count: 1 << 20 },
            watermark: WatermarkPolicy::Enabled {
                event_time_field: "id".into(),
                domain: EventTimeDomain::SignedInteger,
                authority: WatermarkAuthority::Source,
                partition_aggregation: cdf_kernel::PartitionWatermarkAggregation::MinimumAll,
            },
            late_data: LateDataAction::Quarantine,
            safe_frontier: SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
        },
        termination: DrainTermination::Records { count: 2 },
    };
    let mut source = mock_unbounded_cursor_source_plan(&resource);
    source
        .stream_capabilities
        .as_mut()
        .unwrap()
        .watermark_behavior = cdf_kernel::OperatorWatermarkBehavior::Preserve;
    source.stream_capabilities.as_mut().unwrap().watermark =
        Some(cdf_runtime::SourceWatermarkCapability {
            event_time_field: "id".into(),
            domain: EventTimeDomain::SignedInteger,
            authority: WatermarkAuthority::Source,
        });
    source.validate().unwrap();
    resource.bind_compiled_source(&source);
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, extent.clone()),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap();
    let mut controller = cdf_runtime::DrainEpochController::new(&extent).unwrap();
    let root = TempDir::new().unwrap();
    let output = block_on(super::execute_drain_epoch_with_hooks(
        &plan,
        &resource,
        root.path().join("epoch-0"),
        &|_, _| Ok(()),
        super::DrainEpochExecution::new(&mut controller),
        executable_mock_options(EngineExecutionConfig::default()).unwrap(),
    ))
    .unwrap()
    .into_package()
    .unwrap();

    assert_eq!(
        output
            .drain_epoch
            .unwrap()
            .closure
            .frontier
            .watermark
            .unwrap()
            .value,
        WatermarkValue::Signed(5)
    );
}

#[test]
fn late_rows_are_quarantined_or_admitted_with_identity_evidence() {
    for action in [
        LateDataAction::Quarantine,
        LateDataAction::RecaptureNextEpoch,
        LateDataAction::AdmitWithAnnotation,
    ] {
        let claim = |value: i64, offset: i64| WatermarkClaim {
            version: WATERMARK_CLAIM_VERSION,
            policy_version: STREAM_EPOCH_POLICY_VERSION,
            event_time_field: "id".into(),
            domain: EventTimeDomain::SignedInteger,
            value: WatermarkValue::Signed(value),
            partition_id: PartitionId::new("part-0").unwrap(),
            source_position: SourcePosition::Cursor(CursorPosition {
                version: cdf_kernel::SOURCE_POSITION_VERSION,
                field: "id".to_owned(),
                value: CursorValue::I64(offset),
            }),
            authority: WatermarkAuthority::Source,
            observation_context: WatermarkObservationContext::SourcePoll,
        };
        let mut batches = [20_i64, 10]
            .into_iter()
            .enumerate()
            .map(|(ordinal, value)| {
                let offset = i64::try_from(ordinal + 1).unwrap();
                let mut batch = batch_for_partition(
                    &format!("batch-{value}"),
                    "part-0",
                    vec![i32::try_from(value).unwrap()],
                    vec!["event"],
                    vec![true],
                );
                batch.header.source_position = Some(SourcePosition::Cursor(CursorPosition {
                    version: cdf_kernel::SOURCE_POSITION_VERSION,
                    field: "id".to_owned(),
                    value: CursorValue::I64(offset),
                }));
                batch.header.watermarks.push(claim(20, offset));
                batch
            })
            .collect::<Vec<_>>();
        let resource = MockResource::tier_a(std::mem::take(&mut batches)).without_control_keys();
        let extent = ExecutionExtent::Drain {
            version: EXECUTION_EXTENT_VERSION,
            policy: StreamEpochPolicy {
                version: STREAM_EPOCH_POLICY_VERSION,
                checkpoint_cadence: EpochClosureTrigger::Rows { count: 1 },
                package_rotation: EpochClosureTrigger::Bytes { count: 1 << 20 },
                watermark: WatermarkPolicy::Enabled {
                    event_time_field: "id".into(),
                    domain: EventTimeDomain::SignedInteger,
                    authority: WatermarkAuthority::Source,
                    partition_aggregation: cdf_kernel::PartitionWatermarkAggregation::MinimumAll,
                },
                late_data: action,
                safe_frontier: SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
            },
            termination: DrainTermination::Records { count: 3 },
        };
        let mut source = mock_unbounded_cursor_source_plan(&resource);
        source
            .stream_capabilities
            .as_mut()
            .unwrap()
            .watermark_behavior = cdf_kernel::OperatorWatermarkBehavior::Preserve;
        source.stream_capabilities.as_mut().unwrap().watermark =
            Some(cdf_runtime::SourceWatermarkCapability {
                event_time_field: "id".into(),
                domain: EventTimeDomain::SignedInteger,
                authority: WatermarkAuthority::Source,
            });
        source.validate().unwrap();
        resource.bind_compiled_source(&source);
        let mut plan = Planner::new()
            .plan_tier_a(
                &resource,
                plan_input(Vec::new(), None, None, extent.clone()),
            )
            .unwrap()
            .bind_compiled_source(&source)
            .unwrap()
            .bind_operator_graph(
                &source,
                &cdf_runtime::DestinationRuntimeCapabilities::default(),
            )
            .unwrap();
        let mut controller = cdf_runtime::DrainEpochController::new(&extent).unwrap();
        let root = TempDir::new().unwrap();

        let first = block_on(super::execute_drain_epoch_with_hooks(
            &plan,
            &resource,
            root.path().join(format!("{action:?}-epoch-0")),
            &|_, _| Ok(()),
            super::DrainEpochExecution::new(&mut controller),
            executable_mock_options(EngineExecutionConfig::default()).unwrap(),
        ))
        .unwrap()
        .into_package()
        .unwrap();
        let first_drain = first.drain_epoch.as_ref().unwrap();
        assert_eq!(first.output.profile.output_rows, 1);
        assert_eq!(
            first_drain
                .closure
                .frontier
                .watermark
                .as_ref()
                .unwrap()
                .value,
            WatermarkValue::Signed(20)
        );
        controller
            .acknowledge_settlement(&first_drain.closure.frontier.frontier)
            .unwrap();
        plan.advance_committed_drain_frontier(
            first_drain.consumed_partition_count,
            first_drain.resume_partition.as_deref(),
        )
        .unwrap();
        plan = plan
            .rebind_package_id(format!("pkg-late-{action:?}"))
            .unwrap();
        let second_dir = root.path().join(format!("{action:?}-epoch-1"));
        let second = block_on(super::execute_drain_epoch_with_hooks(
            &plan,
            &resource,
            &second_dir,
            &|_, _| Ok(()),
            super::DrainEpochExecution::new(&mut controller),
            executable_mock_options(EngineExecutionConfig::default()).unwrap(),
        ))
        .unwrap()
        .into_package()
        .unwrap();
        let evidence: cdf_package_contract::LateDataEvidence = serde_json::from_slice(
            &std::fs::read(second_dir.join(cdf_package_contract::LATE_DATA_EVIDENCE_FILE)).unwrap(),
        )
        .unwrap();
        assert_eq!(evidence.batches.len(), 1);
        let evidence_batch = &evidence.batches[0];
        assert_eq!(evidence_batch.rows.len(), 1);
        assert_eq!(
            evidence_batch.rows[0].event_time,
            WatermarkValue::Signed(10)
        );
        assert_eq!(
            evidence_batch.effective_watermark.value,
            WatermarkValue::Signed(20)
        );
        let package = cdf_package::PackageReader::open(&second_dir).unwrap();
        let verified = package.verify_for_consumption().unwrap();
        let (joined_evidence, _) = package
            .late_data_evidence_verified(&verified)
            .unwrap()
            .expect("late-data evidence");
        assert_eq!(joined_evidence, evidence);
        let quarantine = collect_quarantine_records(&package);
        assert_eq!(
            second.output.verdict_summary.accepted_rows, second.output.profile.output_rows,
            "accepted-row telemetry must describe rows that remain in the main package"
        );
        match action {
            LateDataAction::Quarantine => {
                assert_eq!(second.output.profile.output_rows, 0);
                assert_eq!(quarantine.len(), 1);
                assert_eq!(quarantine[0].rule_id, "cdf.late_data");
                let catalog: LateDataPayloadCatalog = serde_json::from_slice(
                    &std::fs::read(second_dir.join(LATE_DATA_PAYLOAD_CATALOG_FILE)).unwrap(),
                )
                .unwrap();
                catalog.validate().unwrap();
                assert_eq!(catalog.artifacts.len(), 1);
                assert_eq!(catalog.artifacts[0].action, action);
                assert_eq!(catalog.artifacts[0].row_count, 1);
                assert!(matches!(
                    evidence_batch.rows[0].payload,
                    LateDataPayloadLocation::ArtifactRow {
                        artifact_ordinal: 0,
                        row_ordinal: 0,
                    }
                ));
                let file =
                    std::fs::File::open(second_dir.join(&catalog.artifacts[0].path)).unwrap();
                let mut payload = arrow_ipc::reader::FileReader::try_new(file, None).unwrap();
                let batch = payload.next().unwrap().unwrap();
                assert_eq!(batch.num_rows(), 1);
                assert_eq!(
                    batch
                        .column_by_name("id")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .value(0),
                    10
                );
                assert!(payload.next().is_none());
                let summary: cdf_contract::VerdictSummary = serde_json::from_slice(
                    &std::fs::read(second_dir.join("stats/verdict-summary.json")).unwrap(),
                )
                .unwrap();
                assert_eq!(summary.input_rows, 1);
                assert_eq!(summary.accepted_rows, 0);
                assert_eq!(summary.quarantined_rows, 1);
            }
            LateDataAction::AdmitWithAnnotation => {
                assert_eq!(second.output.profile.output_rows, 1);
                assert!(quarantine.is_empty());
                assert!(matches!(
                    evidence_batch.rows[0].payload,
                    LateDataPayloadLocation::AdmittedOutput {
                        package_row_ordinal: 0
                    }
                ));
                assert!(!second_dir.join(LATE_DATA_PAYLOAD_CATALOG_FILE).exists());
            }
            LateDataAction::RecaptureNextEpoch => {
                assert_eq!(second.output.profile.output_rows, 0);
                assert!(quarantine.is_empty());
                let catalog: LateDataPayloadCatalog = serde_json::from_slice(
                    &std::fs::read(second_dir.join(LATE_DATA_PAYLOAD_CATALOG_FILE)).unwrap(),
                )
                .unwrap();
                catalog.validate().unwrap();
                assert_eq!(catalog.artifacts.len(), 1);
                assert_eq!(catalog.artifacts[0].action, action);
                assert!(matches!(
                    evidence_batch.rows[0].payload,
                    LateDataPayloadLocation::ArtifactRow {
                        artifact_ordinal: 0,
                        row_ordinal: 0,
                    }
                ));
                let second_drain = second.drain_epoch.as_ref().unwrap();
                assert_eq!(second_drain.late_data_carryover.len(), 1);
                controller
                    .acknowledge_settlement(&second_drain.closure.frontier.frontier)
                    .unwrap();
                plan.advance_committed_drain_frontier(
                    second_drain.consumed_partition_count,
                    second_drain.resume_partition.as_deref(),
                )
                .unwrap();
                plan = plan
                    .rebind_package_id("pkg-late-recapture-epoch-2")
                    .unwrap();
                let reader = cdf_package::PackageReader::open(&second_dir).unwrap();
                let verified = Arc::new(reader.verify_for_consumption().unwrap());
                let carryover = second_drain
                    .late_data_carryover
                    .iter()
                    .map(|reference| {
                        let object = reader
                            .verified_identity_object(
                                Arc::clone(&verified),
                                &reference.relative_path,
                            )
                            .unwrap();
                        super::LateDataCarryoverInput::new(reference.clone(), object).unwrap()
                    })
                    .collect();
                let third_dir = root.path().join("RecaptureNextEpoch-epoch-2");
                let third = block_on(super::execute_drain_epoch_with_hooks(
                    &plan,
                    &resource,
                    &third_dir,
                    &|_, _| Ok(()),
                    super::DrainEpochExecution::new(&mut controller)
                        .with_late_data_carryover(carryover),
                    executable_mock_options(EngineExecutionConfig::default()).unwrap(),
                ))
                .unwrap()
                .into_package()
                .unwrap();
                let third_drain = third.drain_epoch.as_ref().unwrap();
                assert_eq!(third.output.profile.output_rows, 1);
                assert_eq!(third_drain.consumed_late_data_carryover.len(), 1);
                assert!(third_drain.late_data_carryover.is_empty());
                assert!(third_drain.closure.terminate_after_settlement);
                assert!(
                    third_dir
                        .join("plan/late-data-carryover-input.json")
                        .is_file()
                );
            }
        }
    }
}

#[test]
fn drain_rejects_an_earlier_regressing_claim_even_when_the_batch_tail_recovers() {
    let claim = |value: i64| WatermarkClaim {
        version: WATERMARK_CLAIM_VERSION,
        policy_version: STREAM_EPOCH_POLICY_VERSION,
        event_time_field: "id".into(),
        domain: EventTimeDomain::SignedInteger,
        value: WatermarkValue::Signed(value),
        partition_id: PartitionId::new("part-0").unwrap(),
        source_position: SourcePosition::Cursor(CursorPosition {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            field: "id".to_owned(),
            value: CursorValue::I64(1),
        }),
        authority: WatermarkAuthority::Source,
        observation_context: WatermarkObservationContext::SourcePoll,
    };
    let mut batch = batch_for_partition("batch-1", "part-0", vec![100], vec!["event"], vec![true]);
    batch.header.source_position = Some(SourcePosition::Cursor(CursorPosition {
        version: cdf_kernel::SOURCE_POSITION_VERSION,
        field: "id".to_owned(),
        value: CursorValue::I64(1),
    }));
    batch.header.watermarks = vec![claim(100), claim(90), claim(110)];
    let resource = MockResource::tier_a(vec![batch]).without_control_keys();
    let extent = ExecutionExtent::Drain {
        version: EXECUTION_EXTENT_VERSION,
        policy: StreamEpochPolicy {
            version: STREAM_EPOCH_POLICY_VERSION,
            checkpoint_cadence: EpochClosureTrigger::Rows { count: 1 },
            package_rotation: EpochClosureTrigger::Bytes { count: 1 << 20 },
            watermark: WatermarkPolicy::Enabled {
                event_time_field: "id".into(),
                domain: EventTimeDomain::SignedInteger,
                authority: WatermarkAuthority::Source,
                partition_aggregation: cdf_kernel::PartitionWatermarkAggregation::MinimumAll,
            },
            late_data: LateDataAction::Quarantine,
            safe_frontier: SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
        },
        termination: DrainTermination::Records { count: 1 },
    };
    let mut source = mock_unbounded_cursor_source_plan(&resource);
    source
        .stream_capabilities
        .as_mut()
        .unwrap()
        .watermark_behavior = cdf_kernel::OperatorWatermarkBehavior::Preserve;
    source.stream_capabilities.as_mut().unwrap().watermark =
        Some(cdf_runtime::SourceWatermarkCapability {
            event_time_field: "id".into(),
            domain: EventTimeDomain::SignedInteger,
            authority: WatermarkAuthority::Source,
        });
    source.validate().unwrap();
    resource.bind_compiled_source(&source);
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, extent.clone()),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap();
    let mut controller = cdf_runtime::DrainEpochController::new(&extent).unwrap();
    let root = TempDir::new().unwrap();

    let error = block_on(super::execute_drain_epoch_with_hooks(
        &plan,
        &resource,
        root.path().join("regressing-watermark"),
        &|_, _| Ok(()),
        super::DrainEpochExecution::new(&mut controller),
        executable_mock_options(EngineExecutionConfig::default()).unwrap(),
    ))
    .unwrap_err();
    assert!(error.message.contains("watermark regressed"));
}

#[test]
fn watermark_projection_fails_at_graph_compilation_before_source_contact() {
    let resource = MockResource::tier_b(sample_batches());
    let mut policy = sample_stream_epoch_policy();
    policy.watermark = WatermarkPolicy::Enabled {
        event_time_field: "id".into(),
        domain: cdf_kernel::EventTimeDomain::SignedInteger,
        authority: cdf_kernel::WatermarkAuthority::Source,
        partition_aggregation: cdf_kernel::PartitionWatermarkAggregation::MinimumAll,
    };
    let extent = ExecutionExtent::Drain {
        version: EXECUTION_EXTENT_VERSION,
        policy,
        termination: DrainTermination::Records { count: 100 },
    };
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), Some(vec!["name".to_owned()]), None, extent),
        )
        .unwrap();
    let mut source = mock_compiled_source_plan(&resource, None);
    source.execution_capabilities.bounded = false;
    source.stream_capabilities = Some(cdf_runtime::SourceStreamCapabilities {
        quiescence: false,
        watermark_behavior: cdf_kernel::OperatorWatermarkBehavior::Preserve,
        watermark: Some(cdf_runtime::SourceWatermarkCapability {
            event_time_field: "id".into(),
            domain: cdf_kernel::EventTimeDomain::SignedInteger,
            authority: WatermarkAuthority::Source,
        }),
        safe_frontiers: vec![SafeFrontierPolicy::CanonicalAdmittedSourcePosition],
        source_frontiers: vec![cdf_runtime::SourceFrontierCapability::Cursor {
            fields: vec!["id".to_owned()],
        }],
        idleness_capabilities: Vec::new(),
    });
    source.validate().unwrap();
    let plan = plan.bind_compiled_source(&source).unwrap();

    let error = plan
        .bind_operator_graph(
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap_err();

    assert!(error.message.contains("event-time field `id` is removed"));
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
}

#[test]
fn watermark_compilation_proves_field_domain_authority_and_column_preservation() {
    fn extent(field: &str, domain: cdf_kernel::EventTimeDomain) -> ExecutionExtent {
        let mut policy = sample_stream_epoch_policy();
        policy.watermark = WatermarkPolicy::Enabled {
            event_time_field: field.into(),
            domain,
            authority: WatermarkAuthority::Source,
            partition_aggregation: cdf_kernel::PartitionWatermarkAggregation::MinimumAll,
        };
        ExecutionExtent::Drain {
            version: EXECUTION_EXTENT_VERSION,
            policy,
            termination: DrainTermination::Records { count: 100 },
        }
    }

    fn unbounded_source(resource: &MockResource) -> cdf_runtime::CompiledSourcePlan {
        let mut source = mock_compiled_source_plan(resource, None);
        source.execution_capabilities.bounded = false;
        source.stream_capabilities = Some(cdf_runtime::SourceStreamCapabilities {
            quiescence: false,
            watermark_behavior: cdf_kernel::OperatorWatermarkBehavior::Preserve,
            watermark: Some(cdf_runtime::SourceWatermarkCapability {
                event_time_field: "id".into(),
                domain: cdf_kernel::EventTimeDomain::SignedInteger,
                authority: WatermarkAuthority::Source,
            }),
            safe_frontiers: vec![SafeFrontierPolicy::CanonicalAdmittedSourcePosition],
            source_frontiers: vec![cdf_runtime::SourceFrontierCapability::Cursor {
                fields: vec!["id".to_owned()],
            }],
            idleness_capabilities: Vec::new(),
        });
        source.validate().unwrap();
        source
    }

    let destination = cdf_runtime::DestinationRuntimeCapabilities::default();

    let resource = MockResource::tier_b(sample_batches());
    let source = unbounded_source(&resource);
    let error = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(
                Vec::new(),
                None,
                None,
                extent("missing", cdf_kernel::EventTimeDomain::SignedInteger),
            ),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap_err();
    assert!(
        error
            .message
            .contains("watermark field/domain/authority does not match")
    );

    let error = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(
                Vec::new(),
                None,
                None,
                extent("id", cdf_kernel::EventTimeDomain::Date32),
            ),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap_err();
    assert!(
        error
            .message
            .contains("watermark field/domain/authority does not match"),
        "{}",
        error.message
    );

    let mut redacted = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(
                Vec::new(),
                None,
                None,
                extent("id", cdf_kernel::EventTimeDomain::SignedInteger),
            ),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap();
    redacted
        .validation_program
        .column_programs
        .iter_mut()
        .find(|column| column.output_name == "id")
        .unwrap()
        .redaction = RedactionDecision::Omit;
    let error = redacted
        .bind_operator_graph(&source, &destination)
        .unwrap_err();
    assert!(error.message.contains("is redacted"));

    let mut derived_extent = extent("id", cdf_kernel::EventTimeDomain::SignedInteger);
    let ExecutionExtent::Drain { policy, .. } = &mut derived_extent else {
        unreachable!()
    };
    let WatermarkPolicy::Enabled { authority, .. } = &mut policy.watermark else {
        unreachable!()
    };
    *authority = WatermarkAuthority::Derived {
        mapping_id: "missing-mapping".into(),
    };
    let derived = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, derived_extent),
        )
        .unwrap();
    let error = derived.bind_compiled_source(&source).unwrap_err();
    assert!(
        error
            .message
            .contains("watermark field/domain/authority does not match")
    );
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
}

#[test]
fn operator_graph_binds_the_plan_source_and_drain_policy_exactly() {
    let resource = MockResource::tier_b(sample_batches());
    let source = mock_compiled_source_plan(&resource, None);
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap();
    let mut other_source = source.clone();
    other_source.physical_plan = serde_json::json!({"partitioning": "other"});
    other_source.physical_plan_hash = cdf_kernel::PhysicalSourcePlanHash::new(
        cdf_runtime::artifact_hash(&other_source.physical_plan).unwrap(),
    )
    .unwrap();
    other_source.validate().unwrap();
    let error = plan
        .bind_operator_graph(
            &other_source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap_err();
    assert!(
        error
            .message
            .contains("differs from the source already bound")
    );

    let mut drain_source = source.clone();
    drain_source.execution_capabilities.bounded = false;
    drain_source.stream_capabilities = Some(cdf_runtime::SourceStreamCapabilities {
        quiescence: false,
        watermark_behavior: cdf_kernel::OperatorWatermarkBehavior::Drop,
        watermark: None,
        safe_frontiers: vec![SafeFrontierPolicy::CanonicalAdmittedSourcePosition],
        source_frontiers: vec![cdf_runtime::SourceFrontierCapability::Cursor {
            fields: vec!["id".to_owned()],
        }],
        idleness_capabilities: Vec::new(),
    });
    drain_source.validate().unwrap();
    resource.bind_compiled_source(&drain_source);
    let drain = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(
                Vec::new(),
                None,
                None,
                ExecutionExtent::Drain {
                    version: EXECUTION_EXTENT_VERSION,
                    policy: sample_stream_epoch_policy(),
                    termination: DrainTermination::Records { count: 100 },
                },
            ),
        )
        .unwrap()
        .bind_compiled_source(&drain_source)
        .unwrap();
    let error = drain
        .validate_compiled_source_resource(&resource)
        .unwrap_err();
    assert!(error.message.contains("requires a compiled operator graph"));
}

#[test]
fn non_pausable_unbounded_execution_requires_runtime_replay_retention() {
    let resource = MockResource::tier_a(sample_batches()).without_control_keys();
    let mut source = mock_unbounded_cursor_source_plan(&resource);
    source.resource_capabilities.backpressure = BackpressureSupport::SpillRequired;
    source.execution_capabilities.pausable = false;
    source.execution_capabilities.spillable = true;
    source.validate().unwrap();
    resource.bind_compiled_source(&source);
    let extent = ExecutionExtent::Drain {
        version: EXECUTION_EXTENT_VERSION,
        policy: sample_stream_epoch_policy(),
        termination: DrainTermination::Records { count: 3 },
    };
    let plan = Planner::new()
        .plan_tier_a(&resource, plan_input(Vec::new(), None, None, extent))
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap();

    let error = plan
        .validate_compiled_source_resource(&resource)
        .unwrap_err();
    assert!(error.message.contains("replay-retention authority"));
    assert!(error.message.contains("byte, age, and unit-count knobs"));
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
}

#[test]
fn engine_parallel_frontier_polls_later_partition_while_head_is_stalled() {
    let (head_sender, head_receiver) = tokio::sync::oneshot::channel::<()>();
    let later_polls = Arc::new(AtomicUsize::new(0));
    let resource = StalledHeadResource {
        inner: MockResource::tier_b(sample_batches()).without_control_keys(),
        head_gate: Arc::new(Mutex::new(Some(head_receiver))),
        later_polls: Arc::clone(&later_polls),
    };
    let mut plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let source = mock_compiled_source_plan(&resource.inner, None);
    resource.inner.bind_compiled_source(&source);
    plan = plan.bind_compiled_source(&source).unwrap();
    plan.operator_graph = Some(
        compile_operator_graph(
            &plan,
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap(),
    );

    let memory: Arc<dyn cdf_memory::MemoryCoordinator> = Arc::new(
        cdf_memory::DeterministicMemoryCoordinator::new(512 * 1024 * 1024, BTreeMap::new())
            .unwrap(),
    );
    let host = Arc::new(
        StandaloneExecutionHost::new(
            cdf_runtime::ExecutionHostCapabilities {
                logical_cpu_slots: 2,
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
        Some(2),
    )
    .unwrap();
    assert_eq!(scheduler.effective_jobs.jobs, 2);

    let parallel_plan = plan.clone();
    let parallel_resource = resource.clone();
    let parallel_services = services.clone();
    let run = std::thread::spawn(move || {
        let package = TempDir::new().unwrap();
        let pre_finalize =
            |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
        block_on(
            super::execute_to_package_with_segment_positions_and_pre_finalize(
                &parallel_plan,
                &parallel_resource,
                package.path(),
                &pre_finalize,
                EngineExecutionConfig::default()
                    .with_execution_services(parallel_services)
                    .with_scheduler_resolution(scheduler)
                    .new_invocation(),
            ),
        )
    });

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while later_polls.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    let later_polled_while_head_stalled = later_polls.load(Ordering::SeqCst) == 1;
    head_sender.send(()).unwrap();
    let parallel = run.join().unwrap().unwrap();

    assert!(
        later_polled_while_head_stalled,
        "jobs=2 did not poll the later partition before the canonical head was released"
    );
    assert_eq!(later_polls.load(Ordering::SeqCst), 1);
    assert_eq!(resource.inner.open_count.load(Ordering::SeqCst), 2);
    assert_eq!(services.memory().snapshot().current_bytes, 0);

    let serial_resource = MockResource::tier_b(sample_batches());
    serial_resource.bind_compiled_source(&source);
    let serial_package = TempDir::new().unwrap();
    let serial = block_on(execute_to_package(
        &plan,
        &serial_resource,
        serial_package.path(),
    ))
    .unwrap();
    assert_eq!(parallel.output.manifest.identity, serial.manifest.identity);
    assert_eq!(parallel.output.lineage, serial.lineage);
    assert_eq!(
        parallel.output.profile.statistics,
        serial.profile.statistics
    );
}

#[test]
fn engine_keeps_non_speculative_source_frontier_serial() {
    let resource = MockResource::tier_b(sample_batches());
    let mut plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let source = mock_compiled_source_plan_with_speculation(&resource, None, false);
    resource.bind_compiled_source(&source);
    plan = plan.bind_compiled_source(&source).unwrap();
    plan.operator_graph = Some(
        compile_operator_graph(
            &plan,
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap(),
    );

    let (_, services) = StandaloneExecutionHost::default_services(512 * 1024 * 1024).unwrap();
    let scheduler = cdf_runtime::resolve_runtime_scheduler(
        plan.scan.partition_count().unwrap(),
        &source.execution_capabilities,
        &cdf_runtime::DestinationRuntimeCapabilities::default(),
        &services,
        Some(4),
    )
    .unwrap();
    assert!(scheduler.effective_jobs.jobs > 1);
    let options = EngineExecutionConfig::default()
        .with_execution_services(services)
        .with_scheduler_resolution(scheduler)
        .new_invocation();

    assert_eq!(crate::execution::partition_open_jobs(&plan, &options), 1);
}

#[test]
fn scheduler_retries_atomic_open_and_records_one_canonical_success() {
    let mut position = terminal_file_position();
    let SourcePosition::FileManifest(planned_manifest) = &mut position else {
        unreachable!("fixture is a file manifest")
    };
    planned_manifest.files[0].sha256 = None;
    let attestation = PartitionAttestation::new(position.clone(), None);
    let mut completed_position = position.clone();
    let SourcePosition::FileManifest(completed_manifest) = &mut completed_position else {
        unreachable!("fixture is a file manifest")
    };
    completed_manifest.files[0].sha256 = Some(format!("sha256:{}", "a".repeat(64)));
    let resource = MockResource::tier_b(retry_positioned_batches(&position))
        .with_partition_count(1)
        .with_transient_open_failures(1)
        .with_attestation(attestation)
        .with_completion_attestation(PartitionAttestation::new(completed_position, None));
    let mut plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let source = mock_compiled_source_plan(&resource, Some(fast_test_retry_policy()));
    resource.bind_compiled_source(&source);
    plan = plan.bind_compiled_source(&source).unwrap();
    let (_, services) = StandaloneExecutionHost::default_services(256 * 1024 * 1024).unwrap();
    let package = TempDir::new().unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let observed_retry = Arc::new(Mutex::new(Vec::new()));
    let retry_progress = Arc::clone(&observed_retry);
    let options = EngineExecutionConfig::default()
        .with_execution_services(services)
        .new_invocation()
        .with_source_retry_progress(Arc::new(move |_partition, entry| {
            retry_progress.lock().unwrap().push(entry.clone());
        }));

    let output = block_on(
        super::execute_to_package_with_segment_positions_and_pre_finalize(
            &plan,
            &resource,
            package.path(),
            &pre_finalize,
            options,
        ),
    )
    .unwrap();

    assert_eq!(resource.open_count.load(Ordering::SeqCst), 2);
    assert_eq!(resource.attest_count.load(Ordering::SeqCst), 1);
    assert_eq!(output.output.profile.output_rows, 3);
    let retries = output.execution_evidence().source_retries();
    assert_eq!(retries.len(), 1);
    assert_eq!(retries[0].partition_ordinal(), 0);
    assert_eq!(retries[0].history().len(), 1);
    assert_eq!(retries[0].history()[0].failed_attempt, 1);
    assert_eq!(
        retries[0].history()[0].cause,
        cdf_kernel::ErrorKind::Transient
    );
    let observed_retry = observed_retry.lock().unwrap();
    assert_eq!(observed_retry.len(), 1);
    assert_eq!(observed_retry[0], retries[0].history()[0]);
    assert!(observed_retry[0].selected_delay_ms.is_some());
}

#[test]
fn scheduler_retries_lazy_stream_failure_before_first_batch() {
    let position = terminal_file_position();
    let attestation = PartitionAttestation::new(position.clone(), None);
    let resource = MockResource::tier_b(retry_positioned_batches(&position))
        .with_partition_count(1)
        .with_transient_stream_failures(1)
        .with_attestation(attestation.clone())
        .with_completion_attestation(attestation);
    let mut plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let source = mock_compiled_source_plan(&resource, Some(fast_test_retry_policy()));
    resource.bind_compiled_source(&source);
    plan = plan.bind_compiled_source(&source).unwrap();
    let (_, services) = StandaloneExecutionHost::default_services(256 * 1024 * 1024).unwrap();
    let package = TempDir::new().unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());

    let output = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        package.path(),
        &pre_finalize,
        EngineExecutionConfig::default().with_execution_services(services),
    ))
    .unwrap();

    assert_eq!(resource.open_count.load(Ordering::SeqCst), 2);
    assert_eq!(resource.attest_count.load(Ordering::SeqCst), 1);
    assert_eq!(output.output.profile.output_rows, 3);
    assert_eq!(output.execution_evidence().source_retries().len(), 1);
}

#[test]
fn scheduler_reattests_a_retried_partial_limit_without_requiring_eof() {
    let position = terminal_file_position();
    let attestation = PartitionAttestation::new(position.clone(), None);
    let mut batches = retry_positioned_batches(&position);
    batches[1].header.partition_id = PartitionId::new("part-0").unwrap();
    let resource = MockResource::tier_b(batches)
        .with_partition_count(1)
        .with_transient_open_failures(1)
        .with_attestation(attestation.clone())
        .with_completion_attestation(attestation);
    let mut plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, Some(1), ExecutionExtent::bounded()),
        )
        .unwrap();
    let source = mock_compiled_source_plan(&resource, Some(fast_test_retry_policy()));
    resource.bind_compiled_source(&source);
    plan = plan.bind_compiled_source(&source).unwrap();
    let (_, services) = StandaloneExecutionHost::default_services(256 * 1024 * 1024).unwrap();
    let package = TempDir::new().unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());

    let output = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        package.path(),
        &pre_finalize,
        EngineExecutionConfig::default().with_execution_services(services),
    ))
    .unwrap();

    assert_eq!(resource.open_count.load(Ordering::SeqCst), 2);
    assert_eq!(resource.attest_count.load(Ordering::SeqCst), 2);
    assert_eq!(output.output.profile.output_rows, 1);
    assert_eq!(output.execution_evidence().source_retries().len(), 1);
}

#[test]
fn exhausted_retry_history_survives_a_failed_engine_return() {
    let position = terminal_file_position();
    let attestation = PartitionAttestation::new(position.clone(), None);
    let resource = MockResource::tier_b(retry_positioned_batches(&position))
        .with_partition_count(1)
        .with_transient_open_failures(3)
        .with_attestation(attestation.clone())
        .with_completion_attestation(attestation);
    let mut plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let source = mock_compiled_source_plan(&resource, Some(fast_test_retry_policy()));
    resource.bind_compiled_source(&source);
    plan = plan.bind_compiled_source(&source).unwrap();
    let (_, services) = StandaloneExecutionHost::default_services(256 * 1024 * 1024).unwrap();
    let package = TempDir::new().unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let options = EngineExecutionConfig::default()
        .with_execution_services(services)
        .new_invocation();
    let retry_evidence = options.source_retry_evidence();

    let error = block_on(
        super::execute_to_package_with_segment_positions_and_pre_finalize(
            &plan,
            &resource,
            package.path(),
            &pre_finalize,
            options,
        ),
    )
    .unwrap_err();

    assert!(error.message.contains("attempt limit exhausted"), "{error}");
    let history = retry_evidence.snapshot().unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].history().len(), 3);
    assert_eq!(
        history[0].history()[2].exhaustion,
        Some(cdf_runtime::SourceRetryExhaustion::AttemptLimit)
    );
}

#[test]
fn nonretryable_reattest_failure_preserves_primary_error_and_history() {
    let position = terminal_file_position();
    let resource = MockResource::tier_b(retry_positioned_batches(&position))
        .with_partition_count(1)
        .with_transient_open_failures(1)
        .with_attestation_error("planned identity cannot be reattested");
    let mut plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let source = mock_compiled_source_plan(&resource, Some(fast_test_retry_policy()));
    resource.bind_compiled_source(&source);
    plan = plan.bind_compiled_source(&source).unwrap();
    let (_, services) = StandaloneExecutionHost::default_services(256 * 1024 * 1024).unwrap();
    let package = TempDir::new().unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let options = EngineExecutionConfig::default()
        .with_execution_services(services)
        .new_invocation();
    let retry_evidence = options.source_retry_evidence();

    let error = block_on(
        super::execute_to_package_with_segment_positions_and_pre_finalize(
            &plan,
            &resource,
            package.path(),
            &pre_finalize,
            options,
        ),
    )
    .unwrap_err();

    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data, "{error}");
    assert!(error.message.contains("cannot be reattested"), "{error}");
    let history = retry_evidence.snapshot().unwrap();
    assert_eq!(history[0].history().len(), 2);
    assert_eq!(
        history[0].history()[1].exhaustion,
        Some(cdf_runtime::SourceRetryExhaustion::Ineligible)
    );
    assert_eq!(history[0].history()[1].cause, cdf_kernel::ErrorKind::Data);
}

#[test]
fn execution_rejects_tampered_retry_schedule_before_source_contact() {
    let resource = MockResource::tier_b(sample_batches()).with_partition_count(1);
    let mut plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let source = mock_compiled_source_plan(&resource, Some(fast_test_retry_policy()));
    resource.bind_compiled_source(&source);
    plan = plan.bind_compiled_source(&source).unwrap();
    let mut forged_retry =
        cdf_runtime::CompiledSourceRetry::from_capabilities(&source.execution_capabilities)
            .unwrap();
    forged_retry.as_mut().unwrap().policy.max_total_attempts += 1;
    plan.partition_schedule.as_mut().unwrap().admission.retry = forged_retry;
    plan.explain.partition_schedule = plan.partition_schedule.clone();
    let package = TempDir::new().unwrap();

    let error = block_on(execute_to_package(&plan, &resource, package.path())).unwrap_err();

    assert!(
        error
            .message
            .contains("partition schedule differs from its scan or compiled source execution plan"),
        "{error}"
    );
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
}

#[test]
fn scheduler_rejects_generation_change_after_retried_open() {
    let initial_position = terminal_file_position();
    let pre_open = PartitionAttestation::new(initial_position.clone(), None);
    let mut changed_position = initial_position.clone();
    let SourcePosition::FileManifest(changed_manifest) = &mut changed_position else {
        unreachable!("fixture is a file manifest")
    };
    changed_manifest.files[0].etag = Some("etag-changed".to_owned());
    let resource = MockResource::tier_b(retry_positioned_batches(&initial_position))
        .with_partition_count(1)
        .with_transient_open_failures(1)
        .with_attestation(pre_open)
        .with_completion_attestation(PartitionAttestation::new(changed_position, None));
    let mut plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let source = mock_compiled_source_plan(&resource, Some(fast_test_retry_policy()));
    resource.bind_compiled_source(&source);
    plan = plan.bind_compiled_source(&source).unwrap();
    let (_, services) = StandaloneExecutionHost::default_services(256 * 1024 * 1024).unwrap();
    let package = TempDir::new().unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());

    let error = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        package.path(),
        &pre_finalize,
        EngineExecutionConfig::default().with_execution_services(services),
    ))
    .unwrap_err();

    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data, "{error:?}");
    assert!(
        error
            .message
            .contains("changed source generation or schema")
    );
    assert!(error.message.contains("re-plan"));
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 2);
}

#[test]
fn dynamic_schema_quarantine_drains_to_eof_and_commits_terminal_content_identity() {
    let initial_file = FilePosition {
        path: "https://data.example.test/events.ndjson".to_owned(),
        size_bytes: 42,
        source_generation: Some("weak:last-modified".to_owned()),
        etag: None,
        object_version: None,
        sha256: None,
    };
    let initial_position = SourcePosition::FileManifest(FileManifest {
        version: cdf_kernel::SOURCE_POSITION_VERSION,
        files: vec![initial_file.clone()],
    });
    let mut terminal_file = initial_file;
    terminal_file.sha256 = Some("sha256:terminal-content".to_owned());
    let terminal_position = SourcePosition::FileManifest(FileManifest {
        version: cdf_kernel::SOURCE_POSITION_VERSION,
        files: vec![terminal_file],
    });
    let mut batches = vec![
        missing_control_field_batch("schema-quarantine-0", "part-0", vec!["one"]),
        missing_control_field_batch("schema-quarantine-1", "part-0", vec!["two", "three"]),
    ];
    for batch in &mut batches {
        batch.header.source_position = Some(initial_position.clone());
    }
    let resource = MockResource::tier_a(batches)
        .with_schema(sample_schema())
        .with_completion_attestation(PartitionAttestation::new(terminal_position.clone(), None));
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let temp = TempDir::new().unwrap();

    let output = block_on(execute_to_package_with_segment_positions(
        &plan,
        &resource,
        temp.path(),
    ))
    .unwrap();

    assert!(output.output.identity_segments().is_empty());
    assert_eq!(output.output.lineage.input_rows, 3);
    assert_eq!(output.output.terminal_schema_quarantines.len(), 1);
    let processed = output.execution_evidence().processed_observations();
    assert_eq!(processed.len(), 1);
    assert_eq!(
        processed[0].outcome,
        cdf_kernel::ProcessedObservationOutcome::Quarantined
    );
    assert_eq!(processed[0].source_position, terminal_position);
}

fn mock_unbounded_cursor_source_plan(resource: &MockResource) -> cdf_runtime::CompiledSourcePlan {
    let mut source = mock_unbounded_source_plan(resource);
    source.stream_capabilities = Some(cdf_runtime::SourceStreamCapabilities {
        quiescence: true,
        watermark_behavior: cdf_kernel::OperatorWatermarkBehavior::Drop,
        watermark: None,
        safe_frontiers: vec![SafeFrontierPolicy::CanonicalAdmittedSourcePosition],
        source_frontiers: vec![cdf_runtime::SourceFrontierCapability::Cursor {
            fields: vec!["id".to_owned()],
        }],
        idleness_capabilities: Vec::new(),
    });
    source.validate().unwrap();
    source
}

fn retry_positioned_batches(position: &SourcePosition) -> Vec<Batch> {
    sample_batches()
        .into_iter()
        .map(|mut batch| {
            batch.header.source_position = Some(position.clone());
            let retained_bytes = batch
                .record_batch()
                .map(cdf_memory::record_batch_retained_bytes)
                .transpose()
                .unwrap()
                .unwrap_or(0)
                + batch.header.pre_contract_evidence_retained_bytes().unwrap();
            batch
                .with_retention(
                    cdf_kernel::PayloadRetention::new(Arc::new(()), retained_bytes).unwrap(),
                )
                .unwrap()
        })
        .collect()
}

#[derive(Clone)]
struct StalledHeadResource {
    inner: MockResource,
    head_gate: Arc<Mutex<Option<tokio::sync::oneshot::Receiver<()>>>>,
    later_polls: Arc<AtomicUsize>,
}

impl ResourceStream for StalledHeadResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        self.inner.descriptor()
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn compiled_source_plan_hash(&self) -> Option<&cdf_kernel::CompiledSourcePlanHash> {
        self.inner.compiled_source_plan_hash()
    }

    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        self.inner.plan_partitions(request)
    }

    fn open(&self, partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        self.inner.open_count.fetch_add(1, Ordering::SeqCst);
        let batch = self
            .inner
            .batches
            .iter()
            .find(|batch| batch.header.partition_id == partition.partition_id)
            .cloned()
            .expect("stalled-head fixture covers every partition");
        if partition.partition_id.as_str() == "part-0" {
            let receiver = self
                .head_gate
                .lock()
                .unwrap()
                .take()
                .expect("head gate is single-use");
            return cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async move {
                let stream = stream::once(async move {
                    receiver
                        .await
                        .map_err(|_| cdf_kernel::CdfError::internal("head gate dropped"))?;
                    Ok(batch)
                });
                Ok(cdf_kernel::PartitionStreamPayload::batches(Box::pin(
                    stream,
                )))
            }));
        }
        let later_polls = Arc::clone(&self.later_polls);
        cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async move {
            let stream = stream::iter([Ok(batch)]).inspect(move |_| {
                later_polls.fetch_add(1, Ordering::SeqCst);
            });
            Ok(cdf_kernel::PartitionStreamPayload::batches(Box::pin(
                stream,
            )))
        }))
    }

    fn attest_partition(
        &self,
        partition: PartitionPlan,
    ) -> cdf_kernel::PartitionAttestationAttempt<'_> {
        self.inner.attest_partition(partition)
    }

    fn effective_schema_runtime(&self) -> Option<&EffectiveSchemaRuntime> {
        self.inner.effective_schema_runtime()
    }

    fn type_policy_allowances(&self) -> cdf_kernel::TypePolicyAllowances {
        self.inner.type_policy_allowances()
    }
}

impl QueryableResource for StalledHeadResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        self.inner.capabilities()
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        self.inner.negotiate(request)
    }
}
