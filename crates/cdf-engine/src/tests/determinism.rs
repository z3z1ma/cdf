use super::support::{
    Arc, BTreeSet, Batch, ContractPolicy, DrainTermination, EXECUTION_EXTENT_VERSION,
    EffectiveSchemaRuntime, EngineExecutionConfig, EnginePackageDraft, EnginePlan, EnginePlanInput,
    EngineSegmentPosition, EpochClosureTrigger, EventTimeDomain, ExecutionExtent, ExecutionProfile,
    FileManifest, FilePosition, LateDataAction, LineageSummary, MockResource, ObservedSchema,
    OperatorNode, Ordering, PartitionId, PartitionPlan, Planner, Poll, ProptestConfig,
    QueryableResource, ResourceCapabilities, ResourceDescriptor, ResourceStream, RetainedEngineRun,
    RngAlgorithm, RngSeed, RowRule, RunId, STREAM_EPOCH_POLICY_VERSION, SafeFrontierPolicy,
    ScanPlan, ScanRequest, SchemaRef, SegmentEntry, SourcePosition, StandaloneExecutionHost,
    StreamEpochPolicy, TempDir, TerminalSchemaObservationQuarantine, TestCaseError, TestRunner,
    TrustLevel, WATERMARK_CLAIM_VERSION, WatermarkAuthority, WatermarkClaim,
    WatermarkObservationContext, WatermarkPolicy, WatermarkValue, batch_for_partition, block_on,
    compile_validation_program, execute_to_package, execute_to_package_with_run_id,
    execute_to_package_with_segment_positions_and_pre_finalize, mock_compiled_source_plan,
    mock_unbounded_source_plan, plan_input, prop_assert_eq, sample_batches, sample_schema, stream,
    terminal_file_position,
};
use super::support::{Result, read_package_segment};

#[test]
fn fixed_drain_epoch_packages_are_jobs_invariant() {
    let jobs_one = run_fixed_drain_epochs_with_jobs(1);
    let jobs_many = run_fixed_drain_epochs_with_jobs(8);
    assert_eq!(jobs_one.0, jobs_many.0);
    assert_eq!(jobs_one.0.len(), 2);
    assert_eq!(jobs_one.1, 1);
    assert!(jobs_many.1 > jobs_one.1);
}

#[test]
fn randomized_skew_limit_projection_and_filter_matrix_is_jobs_invariant() {
    for seed in 0..12 {
        let resource = skewed_resource(seed, []);
        let (plan, source) = skewed_plan(&resource, seed);
        let runs = [1, 2, 4, 8]
            .into_iter()
            .map(|jobs| run_skewed_jobs(&resource, &plan, &source, jobs).unwrap())
            .collect::<Vec<_>>();
        for run in &runs[1..] {
            assert_eq!(
                run.output.manifest.package_hash,
                runs[0].output.manifest.package_hash
            );
            assert_eq!(
                run.output.identity_segments(),
                runs[0].output.identity_segments()
            );
            assert_eq!(run.output.lineage, runs[0].output.lineage);
            assert_eq!(run.output.profile, runs[0].output.profile);
            assert_eq!(run.segment_positions, runs[0].segment_positions);
            assert_eq!(
                run.output.terminal_schema_quarantines,
                runs[0].output.terminal_schema_quarantines
            );
        }
    }
}

#[test]
fn model_based_execution_shapes_preserve_canonical_identity() {
    let mut runner = TestRunner::new(ProptestConfig {
        cases: 12,
        max_shrink_iters: 128,
        failure_persistence: None,
        rng_algorithm: RngAlgorithm::ChaCha,
        rng_seed: RngSeed::Fixed(0xcdf2_0260_7310_0001),
        ..ProptestConfig::default()
    });
    let shapes = (0_usize..96, 0_usize..512, 1_usize..=7, 1_u16..=8);

    runner
        .run(
            &shapes,
            |(logical_seed, schedule_seed, rows_per_batch, jobs)| {
                let baseline_resource =
                    skewed_resource_with_shape(logical_seed, 0, std::iter::empty(), usize::MAX);
                let varied_resource = skewed_resource_with_shape(
                    logical_seed,
                    schedule_seed,
                    std::iter::empty(),
                    rows_per_batch,
                );
                let (baseline_plan, baseline_source) = skewed_identity_plan(&baseline_resource);
                let (varied_plan, varied_source) = skewed_identity_plan(&varied_resource);
                prop_assert_eq!(&baseline_plan, &varied_plan);

                let baseline =
                    run_skewed_jobs(&baseline_resource, &baseline_plan, &baseline_source, 1)
                        .map_err(|error| TestCaseError::fail(error.to_string()))?;
                let varied = run_skewed_jobs(&varied_resource, &varied_plan, &varied_source, jobs)
                    .map_err(|error| TestCaseError::fail(error.to_string()))?;

                let baseline_reader = cdf_package::PackageReader::open(baseline._package.path())
                    .map_err(|error| TestCaseError::fail(error.to_string()))?;
                let varied_reader = cdf_package::PackageReader::open(varied._package.path())
                    .map_err(|error| TestCaseError::fail(error.to_string()))?;
                let baseline_segments = baseline.output.identity_segments();
                let varied_segments = varied.output.identity_segments();
                prop_assert_eq!(baseline_segments.len(), varied_segments.len());
                for (baseline_segment, varied_segment) in
                    baseline_segments.iter().zip(&varied_segments)
                {
                    prop_assert_eq!(&baseline_segment.segment_id, &varied_segment.segment_id);
                    prop_assert_eq!(
                        read_package_segment(&baseline_reader, &baseline_segment.segment_id),
                        read_package_segment(&varied_reader, &varied_segment.segment_id)
                    );
                }

                prop_assert_eq!(
                    core_identity_snapshot(&baseline),
                    core_identity_snapshot(&varied)
                );
                Ok(())
            },
        )
        .unwrap();
}

#[test]
fn core_identity_snapshot_detects_a_faulty_package_identity() {
    let resource = skewed_resource_with_shape(3, 0, std::iter::empty(), usize::MAX);
    let (plan, source) = skewed_plan(&resource, 3);
    let run = run_skewed_jobs(&resource, &plan, &source, 1).unwrap();
    let expected = core_identity_snapshot(&run);
    let mut faulty = expected.clone();
    faulty.package_hash.push_str("-fault-injected");

    assert_ne!(expected, faulty);
}

#[test]
fn randomized_skew_terminal_failure_is_canonical_across_jobs() {
    let mut successful_cases = 0;
    let mut canonical_failures = BTreeSet::new();
    for seed in 12..36 {
        let first_failure = (seed * 5 + 1) % 8;
        let mut second_failure = (seed * 3 + 4) % 8;
        if second_failure == first_failure {
            second_failure = (second_failure + 1) % 8;
        }
        let resource = skewed_resource(seed, [first_failure, second_failure]);
        let (plan, source) = skewed_plan(&resource, seed);
        let outcomes = [1, 2, 4, 8]
            .into_iter()
            .map(|jobs| run_skewed_jobs(&resource, &plan, &source, jobs))
            .collect::<Vec<_>>();
        match &outcomes[0] {
            Ok(expected) => {
                successful_cases += 1;
                for outcome in &outcomes[1..] {
                    let actual = outcome.as_ref().unwrap();
                    assert_eq!(
                        actual.output.manifest.package_hash,
                        expected.output.manifest.package_hash
                    );
                    assert_eq!(
                        actual.output.identity_segments(),
                        expected.output.identity_segments()
                    );
                    assert_eq!(actual.output.lineage, expected.output.lineage);
                    assert_eq!(actual.segment_positions, expected.segment_positions);
                }
            }
            Err(expected) => {
                canonical_failures.insert(expected.message.clone());
                for outcome in &outcomes[1..] {
                    let actual = outcome.as_ref().unwrap_err();
                    assert_eq!(actual.kind, expected.kind);
                    assert_eq!(actual.message, expected.message);
                }
                assert!(
                    expected
                        .message
                        .contains(&format!("partition {first_failure}"))
                        || expected
                            .message
                            .contains(&format!("partition {second_failure}")),
                    "{expected}"
                );
            }
        }
    }
    assert!(
        successful_cases > 0,
        "limits never stopped before source failure"
    );
    assert!(
        canonical_failures.len() >= 4,
        "failure matrix did not vary canonical error authority: {canonical_failures:?}"
    );
}

#[test]
fn fused_and_unfused_transform_modes_produce_identical_packages() {
    let resource = MockResource::tier_a(sample_batches());
    let mut input = plan_input(vec![], None, None, ExecutionExtent::bounded());
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
    let fused_dir = TempDir::new().unwrap();
    let unfused_dir = TempDir::new().unwrap();
    let pre_finalize =
        |_: &cdf_package::PackageBuilder, _: EnginePackageDraft<'_>| -> Result<()> { Ok(()) };

    let fused = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        fused_dir.path(),
        &pre_finalize,
        EngineExecutionConfig::default(),
    ))
    .unwrap();
    let unfused = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        unfused_dir.path(),
        &pre_finalize,
        EngineExecutionConfig::default().with_unfused_transform_for_conformance(true),
    ))
    .unwrap();

    assert_eq!(fused.source_frontier.wait_ns, 0);
    assert_eq!(unfused.source_frontier.wait_ns, 0);
    assert_eq!(fused, unfused);
    assert_eq!(
        std::fs::read(fused_dir.path().join("quarantine/part-000001.parquet")).unwrap(),
        std::fs::read(unfused_dir.path().join("quarantine/part-000001.parquet")).unwrap()
    );
    cdf_package::PackageReader::open(fused_dir.path())
        .unwrap()
        .verify()
        .unwrap();
    cdf_package::PackageReader::open(unfused_dir.path())
        .unwrap()
        .verify()
        .unwrap();
}

#[test]
fn package_identity_is_invariant_to_source_batch_rechunking() {
    let one = MockResource::tier_a(vec![batch_for_partition(
        "source-page-one",
        "part-0",
        vec![1, 2, 3, 4],
        vec!["one", "two", "three", "four"],
        vec![true; 4],
    )]);
    let many = MockResource::tier_a(vec![
        batch_for_partition("source-page-a", "part-0", vec![1], vec!["one"], vec![true]),
        batch_for_partition(
            "source-page-b",
            "part-0",
            vec![2, 3],
            vec!["two", "three"],
            vec![true; 2],
        ),
        batch_for_partition("source-page-c", "part-0", vec![4], vec!["four"], vec![true]),
    ]);
    let input = plan_input(Vec::new(), None, None, ExecutionExtent::bounded());
    let one_plan = Planner::new().plan_tier_a(&one, input.clone()).unwrap();
    let many_plan = Planner::new().plan_tier_a(&many, input).unwrap();
    assert_eq!(one_plan, many_plan);
    let one_dir = TempDir::new().unwrap();
    let many_dir = TempDir::new().unwrap();
    let one_output = block_on(execute_to_package(&one_plan, &one, one_dir.path())).unwrap();
    let many_output = block_on(execute_to_package(&many_plan, &many, many_dir.path())).unwrap();
    assert_eq!(
        one_output.identity_segments(),
        many_output.identity_segments()
    );
    assert_eq!(one_output.lineage, many_output.lineage);
    assert_eq!(one_output.manifest.identity, many_output.manifest.identity);
    assert_eq!(
        one_output.manifest.package_hash,
        many_output.manifest.package_hash
    );
    assert_eq!(
        one_output.manifest.package_hash,
        "sha256:ce88efb01f31da2d13ce3760c524ae80039db9a8d81b1e540668c8c10789b904"
    );
}

#[test]
fn traced_execution_preserves_manifest_identity_hash() {
    let resource = MockResource::tier_a(sample_batches());
    let input = plan_input(vec![], None, None, ExecutionExtent::bounded());
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
fn parallel_segment_encoding_is_identical_to_inline_canonical_registration() {
    let resource = MockResource::tier_a(sample_batches());
    let mut plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(vec![], None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    for operator in &mut plan.operator_chain {
        if let OperatorNode::PackageSink { segmentation, .. } = operator {
            segmentation.target_rows = 2;
            segmentation.maximum_rows = 2;
            segmentation.microbatch_minimum_rows = 1;
            segmentation.microbatch_maximum_rows = 2;
        }
    }
    let inline_dir = TempDir::new().unwrap();
    let parallel_dir = TempDir::new().unwrap();
    let inline = block_on(execute_to_package(&plan, &resource, inline_dir.path())).unwrap();
    let (_, services) = StandaloneExecutionHost::default_services(64 * 1024 * 1024).unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let parallel = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        parallel_dir.path(),
        &pre_finalize,
        EngineExecutionConfig::default().with_execution_services(services.clone()),
    ))
    .unwrap();

    assert_eq!(parallel.output.manifest.identity, inline.manifest.identity);
    let parallel_segments = parallel.output.identity_segments();
    assert_eq!(
        parallel_segments
            .iter()
            .map(|segment| segment.package_row_ord_start)
            .collect::<Vec<_>>(),
        vec![0, 2]
    );
    let parallel_reader = cdf_package::PackageReader::open(parallel_dir.path()).unwrap();
    let persisted_ordinals = parallel_reader
        .verified_canonical_segment_stream(services.memory(), 64 * 1024 * 1024)
        .unwrap()
        .flat_map(|segment| {
            segment.unwrap().batches.into_iter().flat_map(|batch| {
                cdf_package_contract::package_row_ord_array(&batch)
                    .unwrap()
                    .values()
                    .to_vec()
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(persisted_ordinals, vec![0, 1, 2]);
    assert_eq!(
        parallel.output.identity_segments(),
        inline.identity_segments()
    );
    assert_eq!(parallel.output.lineage, inline.lineage);
    assert_eq!(
        parallel.segment_positions,
        inline
            .identity_segments()
            .iter()
            .map(|segment| {
                EngineSegmentPosition {
                    segment_id: segment.segment_id.clone(),
                    partition_ordinal: 0,
                    output_position: None,
                }
            })
            .collect::<Vec<_>>()
    );
    assert_eq!(services.memory().snapshot().current_bytes, 0);
}

type FixedDrainEpochEvidence = (
    String,
    Vec<cdf_package_contract::SegmentEntry>,
    cdf_kernel::EpochClosureEvidence,
    bool,
);

fn run_fixed_drain_epochs_with_jobs(jobs: u16) -> (Vec<FixedDrainEpochEvidence>, u64) {
    let mut batches = sample_batches();
    for (ordinal, batch) in batches.iter_mut().enumerate() {
        batch.header.partition_id = PartitionId::new(format!("part-{}", ordinal + 1)).unwrap();
    }
    batches.insert(
        0,
        batch_for_partition("batch-idle", "part-0", Vec::new(), Vec::new(), Vec::new()),
    );
    for (ordinal, batch) in batches.iter_mut().enumerate() {
        let source_position = SourcePosition::FileManifest(FileManifest {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            files: vec![FilePosition {
                path: format!("input-{ordinal}.arrow"),
                size_bytes: batch.header.byte_count,
                source_generation: Some(format!("generation-{ordinal}")),
                etag: None,
                object_version: None,
                sha256: None,
            }],
        });
        batch.header.source_position = Some(source_position.clone());
        if batch.header.partition_id.as_str() == "part-0" {
            batch.header.partition_idleness = Some(cdf_kernel::PartitionIdlenessClaim {
                version: cdf_kernel::PARTITION_IDLENESS_CLAIM_VERSION,
                partition_id: batch.header.partition_id.clone(),
                source_position,
                capability_id: "source-idleness-v1".into(),
                idle_for_milliseconds: 10,
            });
        } else {
            batch.header.watermarks.push(WatermarkClaim {
                version: WATERMARK_CLAIM_VERSION,
                policy_version: STREAM_EPOCH_POLICY_VERSION,
                event_time_field: "id".into(),
                domain: EventTimeDomain::SignedInteger,
                value: WatermarkValue::Signed(i64::try_from((ordinal + 1) * 10).unwrap()),
                partition_id: batch.header.partition_id.clone(),
                source_position,
                authority: WatermarkAuthority::Source,
                observation_context: WatermarkObservationContext::SourcePoll,
            });
        }
    }
    let resource = MockResource::tier_b(batches)
        .with_partition_count(3)
        .without_control_keys()
        .with_dynamic_attestation();
    let extent = ExecutionExtent::Drain {
        version: EXECUTION_EXTENT_VERSION,
        policy: StreamEpochPolicy {
            version: STREAM_EPOCH_POLICY_VERSION,
            checkpoint_cadence: EpochClosureTrigger::Rows { count: 3 },
            package_rotation: EpochClosureTrigger::Bytes { count: 1 << 20 },
            watermark: WatermarkPolicy::Enabled {
                event_time_field: "id".into(),
                domain: EventTimeDomain::SignedInteger,
                authority: WatermarkAuthority::Source,
                partition_aggregation: cdf_kernel::PartitionWatermarkAggregation::MinimumEligible {
                    idle_after_milliseconds: 10,
                    capability_id: "source-idleness-v1".into(),
                },
            },
            late_data: LateDataAction::Quarantine,
            safe_frontier: SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
        },
        termination: DrainTermination::Records { count: 6 },
    };
    let mut source = mock_unbounded_source_plan(&resource);
    source.execution_capabilities.speculative_safe = true;
    source.execution_capabilities.attestation =
        cdf_runtime::SourceAttestationStrength::ImmutableContent;
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
    source
        .stream_capabilities
        .as_mut()
        .unwrap()
        .idleness_capabilities = vec!["source-idleness-v1".to_owned()];
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
    let (_, services) = StandaloneExecutionHost::default_services(512 * 1024 * 1024).unwrap();
    let services = services.with_run_job_ceiling(jobs).unwrap();
    let scheduler = cdf_runtime::resolve_runtime_scheduler(
        plan.scan.partition_count().unwrap(),
        &source.execution_capabilities,
        &cdf_runtime::DestinationRuntimeCapabilities::default(),
        &services,
        Some(jobs),
    )
    .unwrap();
    let mut controller = cdf_runtime::DrainEpochController::new(&extent).unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let root = TempDir::new().unwrap();
    let mut outputs = Vec::new();
    let mut maximum_active = 0_u64;
    let mut observed_global_watermark = false;
    let mut observed_source_idleness = false;
    let mut epoch = 0_u64;
    while !controller.is_finished() {
        plan = plan
            .rebind_package_id(format!("pkg-drain-jobs-{epoch}"))
            .unwrap();
        let output = block_on(super::execute_drain_epoch_with_hooks(
            &plan,
            &resource,
            root.path().join(format!("epoch-{epoch}")),
            &pre_finalize,
            super::DrainEpochExecution::new(&mut controller),
            EngineExecutionConfig::default()
                .with_execution_services(services.clone())
                .with_scheduler_resolution(
                    scheduler.narrow_to_partition_count(plan.scan.partition_count().unwrap()),
                )
                .new_invocation(),
        ))
        .unwrap()
        .into_package()
        .unwrap();
        let drain = output.drain_epoch.as_ref().unwrap();
        observed_global_watermark |= drain.closure.frontier.watermark.is_some();
        observed_source_idleness |= drain
            .partition_watermarks
            .iter()
            .any(|state| state.partition_id.as_str() == "part-0" && state.idleness.is_some());
        maximum_active = maximum_active.max(output.source_frontier.maximum_active);
        outputs.push((
            output.output.manifest.package_hash.clone(),
            output.output.identity_segments().to_vec(),
            drain.closure.evidence.clone(),
            drain.closure.terminate_after_settlement,
        ));
        controller
            .acknowledge_settlement(&drain.closure.frontier.frontier)
            .unwrap();
        plan.advance_committed_drain_frontier(
            drain.consumed_partition_count,
            drain.resume_partition.as_deref(),
        )
        .unwrap();
        epoch += 1;
    }
    assert!(observed_global_watermark);
    assert!(observed_source_idleness);
    assert_eq!(services.memory().snapshot().current_bytes, 0);
    (outputs, maximum_active)
}

fn skewed_resource(
    seed: usize,
    terminal_failure_partitions: impl IntoIterator<Item = usize>,
) -> SkewedMockResource {
    skewed_resource_with_shape(seed, seed, terminal_failure_partitions, usize::MAX)
}

fn skewed_resource_with_shape(
    logical_seed: usize,
    schedule_seed: usize,
    terminal_failure_partitions: impl IntoIterator<Item = usize>,
    rows_per_batch: usize,
) -> SkewedMockResource {
    let partition_count = 8;
    let batches = (0..partition_count)
        .flat_map(|ordinal| {
            let row_count = 1 + ((logical_seed * 17 + ordinal * 5) % 7);
            let first_id = i32::try_from(logical_seed * 10_000 + ordinal * 100).unwrap();
            let ids = (0..row_count)
                .map(|row| first_id + i32::try_from(row).unwrap())
                .collect::<Vec<_>>();
            let active = (0..row_count)
                .map(|row| !(logical_seed + ordinal + row).is_multiple_of(3))
                .collect::<Vec<_>>();
            let rows_per_batch = rows_per_batch.max(1);
            (0..row_count)
                .step_by(rows_per_batch)
                .enumerate()
                .map(move |(batch_ordinal, start)| {
                    let end = start.saturating_add(rows_per_batch).min(row_count);
                    let mut batch = batch_for_partition(
                        &format!("skew-{logical_seed}-{ordinal}-{batch_ordinal}"),
                        &format!("part-{ordinal}"),
                        ids[start..end].to_vec(),
                        vec!["skew-value"; end - start],
                        active[start..end].to_vec(),
                    );
                    batch.header.source_position = Some(terminal_file_position());
                    batch
                })
                .collect::<Vec<_>>()
        })
        .collect();
    let inner = MockResource::tier_b(batches).with_partition_count(partition_count);
    SkewedMockResource {
        inner,
        poll_delays: Arc::new(
            (0..partition_count)
                .map(|ordinal| (schedule_seed * 29 + ordinal * 11) % 9)
                .collect(),
        ),
        terminal_failure_partitions: terminal_failure_partitions.into_iter().collect(),
    }
}

fn skewed_plan(
    resource: &SkewedMockResource,
    seed: usize,
) -> (EnginePlan, cdf_runtime::CompiledSourcePlan) {
    let limits = [None, Some(0), Some(1), Some(3), Some(7), Some(19)];
    let filters = if seed.is_multiple_of(2) {
        vec!["active = true"]
    } else {
        Vec::new()
    };
    let projection = seed
        .is_multiple_of(3)
        .then(|| vec!["id".to_owned(), "name".to_owned()]);
    bind_skewed_plan(
        resource,
        plan_input(
            filters,
            projection,
            limits[seed % limits.len()],
            ExecutionExtent::bounded(),
        ),
    )
}

fn skewed_identity_plan(
    resource: &SkewedMockResource,
) -> (EnginePlan, cdf_runtime::CompiledSourcePlan) {
    bind_skewed_plan(
        resource,
        plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
    )
}

fn bind_skewed_plan(
    resource: &SkewedMockResource,
    input: EnginePlanInput,
) -> (EnginePlan, cdf_runtime::CompiledSourcePlan) {
    let mut plan = Planner::new().plan_tier_b(resource, input).unwrap();
    for operator in &mut plan.operator_chain {
        if let OperatorNode::PackageSink { segmentation, .. } = operator {
            segmentation.target_rows = 1;
            segmentation.maximum_rows = 1;
            segmentation.microbatch_minimum_rows = 1;
            segmentation.microbatch_maximum_rows = 1;
        }
    }
    let source = mock_compiled_source_plan(&resource.inner, None);
    resource.inner.bind_compiled_source(&source);
    plan = plan.bind_compiled_source(&source).unwrap();
    plan = plan
        .bind_operator_graph(
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap();
    (plan, source)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CoreIdentitySnapshot {
    package_hash: String,
    segments: Vec<SegmentEntry>,
    lineage: LineageSummary,
    profile: ExecutionProfile,
    segment_positions: Vec<EngineSegmentPosition>,
    terminal_schema_quarantines: Vec<TerminalSchemaObservationQuarantine>,
}

fn core_identity_snapshot(run: &RetainedEngineRun) -> CoreIdentitySnapshot {
    CoreIdentitySnapshot {
        package_hash: run.output.manifest.package_hash.clone(),
        segments: run.output.identity_segments(),
        lineage: run.output.lineage.clone(),
        profile: run.output.profile.clone(),
        segment_positions: run.segment_positions.clone(),
        terminal_schema_quarantines: run.output.terminal_schema_quarantines.clone(),
    }
}

fn run_skewed_jobs(
    resource: &SkewedMockResource,
    plan: &EnginePlan,
    source: &cdf_runtime::CompiledSourcePlan,
    jobs: u16,
) -> Result<RetainedEngineRun> {
    let (_, services) = StandaloneExecutionHost::default_services(4 * 1024 * 1024 * 1024)?;
    let services = services.with_run_job_ceiling(jobs)?;
    let scheduler = cdf_runtime::resolve_runtime_scheduler(
        plan.scan.partition_count().unwrap(),
        &source.execution_capabilities,
        &cdf_runtime::DestinationRuntimeCapabilities::default(),
        &services,
        Some(jobs),
    )?;
    services.tighten_run_job_ceiling(scheduler.effective_jobs.jobs)?;
    let package = TempDir::new().unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let run = block_on(
        super::execute_to_package_with_segment_positions_and_pre_finalize(
            plan,
            resource,
            package.path(),
            &pre_finalize,
            EngineExecutionConfig::default()
                .with_execution_services(services)
                .with_scheduler_resolution(scheduler)
                .new_invocation(),
        ),
    )?;
    Ok(RetainedEngineRun {
        run,
        _package: package,
    })
}

#[derive(Clone)]
struct SkewedMockResource {
    inner: MockResource,
    poll_delays: Arc<Vec<usize>>,
    terminal_failure_partitions: BTreeSet<usize>,
}

impl ResourceStream for SkewedMockResource {
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
        let ordinal = partition
            .metadata
            .get("ordinal")
            .and_then(|value| value.parse::<usize>().ok())
            .expect("skew fixture partition has an ordinal");
        let mut delay = self.poll_delays[ordinal];
        let mut batches = self
            .inner
            .batches
            .iter()
            .filter(|batch| batch.header.partition_id == partition.partition_id)
            .cloned()
            .map(Ok)
            .collect::<Vec<Result<Batch>>>()
            .into_iter();
        if self.terminal_failure_partitions.contains(&ordinal) {
            batches = vec![Err(cdf_kernel::CdfError::data(format!(
                "skew fixture terminal failure at partition {ordinal}"
            )))]
            .into_iter();
        }
        cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async move {
            let stream = stream::poll_fn(move |context| {
                if delay > 0 {
                    delay -= 1;
                    context.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Poll::Ready(batches.next())
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

impl QueryableResource for SkewedMockResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        self.inner.capabilities()
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        self.inner.negotiate(request)
    }
}
