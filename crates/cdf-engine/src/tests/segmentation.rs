use super::support::Result;
use super::support::{
    Arc, ArrayRef, BTreeMap, Batch, BatchHeader, BatchId, CompiledStreamAdmissionEvidence,
    ContractPolicy, CursorPosition, CursorValue, DataType, DedupKeep, DurableSegmentPayload,
    EngineExecutionConfig, EnginePackageDraft, ExecutionExtent, Field, FileManifest, FilePosition,
    Int64Array, MockResource, Mutex, ObservedSchema, OperatorNode, Ordering, PackageStatus,
    PartitionAttestation, PartitionId, Planner, RecordBatch, ResourceId, RowRule, Schema,
    SegmentEntry, SourcePosition, StandaloneExecutionHost, TempDir, TrustLevel,
    batch_for_partition, block_on, compile_validation_program,
    execute_to_package_with_segment_positions,
    execute_to_package_with_segment_positions_and_pre_finalize,
    execute_to_package_with_streaming_hooks, mock_compiled_source_plan, plan_input,
    plan_input_for_schema, sample_batches, sample_schema, terminal_file_position,
};

#[test]
fn execution_returns_segment_source_position_evidence() {
    let resource = MockResource::tier_a(vec![batch_with_file_position()]);
    let input = plan_input(vec![], None, None, ExecutionExtent::bounded());
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package_with_segment_positions(
        &plan,
        &resource,
        temp.path(),
    ))
    .unwrap();

    assert_eq!(output.output.identity_segments().len(), 1);
    assert_eq!(output.segment_positions.len(), 1);
    assert_eq!(
        output.segment_positions[0].segment_id,
        output.output.identity_segments()[0].segment_id
    );
    let Some(SourcePosition::FileManifest(manifest)) = &output.segment_positions[0].output_position
    else {
        panic!("expected file manifest position evidence");
    };
    assert_eq!(manifest.files[0].path, "/tmp/cdf/events.ndjson");
}

#[test]
fn limited_multi_batch_partition_records_exact_non_checkpointing_partial_attempt() {
    let mut batches = sample_batches();
    for batch in &mut batches {
        batch.header.partition_id = PartitionId::new("part-0").unwrap();
        batch.header.source_position = Some(terminal_file_position());
    }
    let resource = MockResource::tier_a(batches);
    let mut plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, Some(1), ExecutionExtent::bounded()),
        )
        .unwrap();
    let source = mock_compiled_source_plan(&resource, None);
    resource.bind_compiled_source(&source);
    plan = plan.bind_compiled_source(&source).unwrap();
    let temp = TempDir::new().unwrap();

    let output = block_on(execute_to_package_with_segment_positions(
        &plan,
        &resource,
        temp.path(),
    ))
    .unwrap();

    assert_eq!(resource.batch_poll_count.load(Ordering::SeqCst), 1);
    assert!(!output.execution_evidence().checkpoint_eligible());

    let evidence: CompiledStreamAdmissionEvidence = serde_json::from_slice(
        &std::fs::read(temp.path().join("schema/stream-admission-evidence.json")).unwrap(),
    )
    .unwrap();
    let [observation] = evidence.observations.as_slice() else {
        panic!("expected exactly one partial schema observation");
    };
    match &observation.completion {
        crate::StreamAdmissionCompletion::Partial {
            attempted_position: Some(position),
            observed_rows,
            partition_binding,
        } => {
            assert_eq!(position, &terminal_file_position());
            assert_eq!(*observed_rows, 3);
            assert!(partition_binding.as_str().starts_with("sha256:"));
        }
        other => panic!("expected exact partial attempt, got {other:?}"),
    }
    assert!(
        !temp
            .path()
            .join(cdf_package_contract::PROCESSED_OBSERVATIONS_FILE)
            .exists()
    );
}

#[test]
fn limited_cursor_batch_never_assigns_unsliced_position_to_output_segments() {
    let attempted = SourcePosition::Cursor(CursorPosition {
        version: cdf_kernel::SOURCE_POSITION_VERSION,
        field: "id".to_owned(),
        value: CursorValue::I64(3),
    });
    let mut batches = sample_batches();
    for batch in &mut batches {
        batch.header.partition_id = PartitionId::new("part-0").unwrap();
        batch.header.source_position = Some(attempted.clone());
    }
    let resource = MockResource::tier_a(batches);
    let mut plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, Some(1), ExecutionExtent::bounded()),
        )
        .unwrap();
    let source = mock_compiled_source_plan(&resource, None);
    resource.bind_compiled_source(&source);
    plan = plan.bind_compiled_source(&source).unwrap();
    let temp = TempDir::new().unwrap();

    let output = block_on(execute_to_package_with_segment_positions(
        &plan,
        &resource,
        temp.path(),
    ))
    .unwrap();

    assert!(!output.execution_evidence().checkpoint_eligible());
    assert_eq!(output.segment_positions.len(), 1);
    assert_eq!(output.segment_positions[0].output_position, None);

    let evidence: CompiledStreamAdmissionEvidence = serde_json::from_slice(
        &std::fs::read(temp.path().join("schema/stream-admission-evidence.json")).unwrap(),
    )
    .unwrap();
    let [observation] = evidence.observations.as_slice() else {
        panic!("expected exactly one partial schema observation");
    };
    match &observation.completion {
        crate::StreamAdmissionCompletion::Partial {
            attempted_position: Some(position),
            ..
        } => assert_eq!(position, &attempted),
        other => panic!("expected exact partial attempt, got {other:?}"),
    }
}

#[test]
fn durable_segment_hook_runs_after_publish_with_exact_entry_and_batch() {
    let resource = MockResource::tier_b(sample_batches()).without_control_keys();
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(vec![], None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let package_dir = TempDir::new().unwrap();
    let durable_root = package_dir.path().to_path_buf();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let hook_observed = Arc::clone(&observed);
    let retained_payloads = Arc::new(Mutex::new(Vec::new()));
    let hook_payloads = Arc::clone(&retained_payloads);
    let mut durable_segment = move |entry: &SegmentEntry, payload: DurableSegmentPayload| {
        assert!(durable_root.join(&entry.path).is_file());
        hook_observed.lock().unwrap().push((
            entry.segment_id.clone(),
            entry.sha256.clone(),
            entry.row_count,
            payload
                .batches()
                .iter()
                .map(|batch| batch.num_rows() as u64)
                .sum::<u64>(),
        ));
        hook_payloads.lock().unwrap().push(payload);
        Ok(())
    };
    fn pre_finalize(
        _builder: &cdf_package::PackageBuilder,
        _draft: EnginePackageDraft<'_>,
    ) -> Result<()> {
        Ok(())
    }
    let mut stream_finalize = || Ok(());

    let (_, services) = StandaloneExecutionHost::default_services(512 * 1024 * 1024).unwrap();
    let output = block_on(execute_to_package_with_streaming_hooks(
        &plan,
        &resource,
        package_dir.path(),
        &pre_finalize,
        &mut durable_segment,
        &mut stream_finalize,
        EngineExecutionConfig::default().with_execution_services(services.clone()),
    ))
    .unwrap();

    let observed = observed.lock().unwrap();
    assert_eq!(observed.len(), output.output.identity_segments().len());
    for (actual, expected) in observed.iter().zip(output.output.identity_segments()) {
        assert_eq!(&actual.0, &expected.segment_id);
        assert_eq!(&actual.1, &expected.sha256);
        assert_eq!(actual.2, actual.3);
        assert_eq!(actual.2, expected.row_count);
    }
    assert!(services.memory().snapshot().current_bytes > 0);
    retained_payloads.lock().unwrap().clear();
    assert_eq!(services.memory().snapshot().current_bytes, 0);
}

#[test]
fn canonical_segment_releases_construction_peak_before_durable_ingress() {
    let resource = MockResource::tier_a(sample_batches());
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(vec![], None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let package_dir = TempDir::new().unwrap();
    let (_, services) = StandaloneExecutionHost::default_services(64 * 1024 * 1024).unwrap();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let hook_observed = Arc::clone(&observed);
    let mut durable_segment = move |_entry: &SegmentEntry, payload: DurableSegmentPayload| {
        let (_durable_local_file, batches, memory_leases) = payload.into_parts();
        let output_bytes =
            batches.iter().try_fold(0_u64, |total, batch| {
                total
                    .checked_add(u64::try_from(batch.get_array_memory_size()).map_err(|_| {
                        cdf_kernel::CdfError::data("durable payload bytes exceed u64")
                    })?)
                    .ok_or_else(|| cdf_kernel::CdfError::data("durable payload bytes overflow"))
            })?;
        let scratch_bytes = memory_leases
            .last()
            .ok_or_else(|| cdf_kernel::CdfError::internal("canonical scratch lease is absent"))?
            .bytes();
        hook_observed
            .lock()
            .unwrap()
            .push((scratch_bytes, output_bytes.max(1)));
        Ok(())
    };
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let mut stream_finalize = || Ok(());

    block_on(execute_to_package_with_streaming_hooks(
        &plan,
        &resource,
        package_dir.path(),
        &pre_finalize,
        &mut durable_segment,
        &mut stream_finalize,
        EngineExecutionConfig::default().with_execution_services(services.clone()),
    ))
    .unwrap();

    let observed = observed.lock().unwrap();
    assert!(!observed.is_empty());
    assert!(
        observed
            .iter()
            .all(|(scratch, output)| *scratch > 0 && scratch < output),
        "canonical scratch must own new allocations without duplicating traveling input leases: {observed:?}"
    );
    assert_eq!(services.memory().snapshot().current_bytes, 0);
}

#[test]
fn pipeline_concurrency_joins_source_segment_and_encode_bounds() {
    const MIB: u64 = 1024 * 1024;

    let constrained = crate::execution::resolve_pipeline_concurrency_from_bounds(
        16,
        15,
        1_504 * MIB,
        64 * MIB,
        64 * MIB,
        256 * MIB,
        0,
    )
    .unwrap();
    assert_eq!(constrained.source_jobs, 7);
    assert_eq!(constrained.segment_encode_jobs, 1);

    let roomy = crate::execution::resolve_pipeline_concurrency_from_bounds(
        16,
        15,
        16 * 1024 * MIB,
        64 * MIB,
        64 * MIB,
        256 * MIB,
        0,
    )
    .unwrap();
    assert_eq!(roomy.source_jobs, 16);
    assert_eq!(roomy.segment_encode_jobs, 15);
}

#[test]
fn pipeline_concurrency_falls_back_to_safe_inline_encoding() {
    const MIB: u64 = 1024 * 1024;

    let inline = crate::execution::resolve_pipeline_concurrency_from_bounds(
        16,
        15,
        900 * MIB,
        64 * MIB,
        64 * MIB,
        256 * MIB,
        0,
    )
    .unwrap();
    assert_eq!(inline.source_jobs, 3);
    assert_eq!(inline.segment_encode_jobs, 0);

    let serial = crate::execution::resolve_pipeline_concurrency_from_bounds(
        1,
        15,
        700 * MIB,
        64 * MIB,
        64 * MIB,
        256 * MIB,
        0,
    )
    .unwrap();
    assert_eq!(serial.source_jobs, 1);
    assert_eq!(serial.segment_encode_jobs, 0);
}

#[test]
fn pipeline_concurrency_reserves_the_staged_handoff_window() {
    const MIB: u64 = 1024 * 1024;

    // Staged ingress admits two maximum-sized segment requests globally. The engine must reserve
    // both live payloads before resolving source and canonical-encode fan-out.
    let constrained = crate::execution::resolve_pipeline_concurrency_from_bounds(
        16,
        15,
        1_504 * MIB,
        64 * MIB,
        64 * MIB,
        256 * MIB,
        512 * MIB,
    )
    .unwrap();
    assert_eq!(constrained.source_jobs, 4);
    assert_eq!(constrained.segment_encode_jobs, 0);

    let roomy = crate::execution::resolve_pipeline_concurrency_from_bounds(
        16,
        15,
        16 * 1024 * MIB,
        64 * MIB,
        64 * MIB,
        256 * MIB,
        512 * MIB,
    )
    .unwrap();
    assert_eq!(roomy.source_jobs, 16);
    assert_eq!(roomy.segment_encode_jobs, 15);
}

#[test]
fn pipeline_concurrency_rejects_an_unavailable_source_minimum() {
    const MIB: u64 = 1024 * 1024;

    let error = crate::execution::resolve_pipeline_concurrency_from_bounds(
        1,
        0,
        96 * MIB,
        96 * MIB + 8 * 1024,
        160 * MIB,
        256 * MIB,
        0,
    )
    .unwrap_err();

    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(error.message.contains("resident destination working sets"));
    assert!(error.message.contains("only 100663296 bytes are free"));
}

#[test]
fn terminal_attestation_enriches_segments_after_package_dedup() {
    let initial_file = FilePosition {
        path: "/tmp/cdf/events.ndjson".to_owned(),
        size_bytes: 42,
        source_generation: Some("local-v1:generation".to_owned()),
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
        batch_for_partition(
            "batch-terminal-0",
            "part-0",
            vec![1, 2],
            vec!["one-first", "two"],
            vec![true, true],
        ),
        batch_for_partition(
            "batch-terminal-1",
            "part-0",
            vec![1, 3],
            vec!["one-last", "three"],
            vec![true, true],
        ),
    ];
    for batch in &mut batches {
        batch.header.source_position = Some(initial_position.clone());
    }
    let resource = MockResource::tier_a(batches)
        .with_completion_attestation(PartitionAttestation::new(terminal_position.clone(), None));
    let mut input = plan_input(vec![], None, None, ExecutionExtent::bounded());
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Dedup {
        keys: vec!["id".to_owned()],
        keep: DedupKeep::Last,
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(sample_schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();

    let output = block_on(execute_to_package_with_segment_positions(
        &plan,
        &resource,
        temp.path(),
    ))
    .unwrap();

    assert_eq!(output.output.profile.output_rows, 3);
    assert_eq!(output.segment_positions.len(), 1);
    assert_eq!(output.segment_positions[0].partition_ordinal, 0);
    assert_eq!(
        output.segment_positions[0].output_position.as_ref(),
        Some(&terminal_position)
    );
}

#[test]
fn accounted_canonical_input_is_not_reserved_again_during_construction() {
    const ROWS: usize = 700_000;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let record_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from_iter_values(0..ROWS as i64)) as ArrayRef],
    )
    .unwrap();
    let retained_bytes = u64::try_from(record_batch.get_array_memory_size()).unwrap();
    let source_bytes = cdf_memory::record_batch_retained_bytes(&record_batch).unwrap();
    assert!(retained_bytes < 8 * 1024 * 1024);
    let row_count = u64::try_from(ROWS).unwrap();
    let accounted_construction =
        crate::execution::canonical_construction_reservation_bytes(retained_bytes, row_count, 0)
            .unwrap();
    let former_duplicate_charge = crate::execution::canonical_construction_reservation_bytes(
        retained_bytes,
        row_count,
        retained_bytes,
    )
    .unwrap();
    assert_eq!(
        former_duplicate_charge - accounted_construction,
        retained_bytes
    );

    let batch = Batch {
        header: BatchHeader::new(
            BatchId::new("accounted-canonical-construction").unwrap(),
            ResourceId::new("orders").unwrap(),
            PartitionId::new("part-0").unwrap(),
            cdf_kernel::canonical_arrow_schema_hash(schema.as_ref()).unwrap(),
            row_count,
            retained_bytes,
        ),
        payload: cdf_kernel::BatchPayload::in_memory(record_batch),
    };
    let resource = MockResource::tier_a(vec![batch]).without_control_keys();
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input_for_schema(schema, Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();

    // At construction the source payload and normalized output are already owned. Exactly one new
    // concat output plus the ordinal vector fits; the former duplicate input charge does not.
    let admission_budget = source_bytes
        .checked_add(retained_bytes)
        .and_then(|bytes| bytes.checked_add(accounted_construction))
        .unwrap();
    assert!(
        source_bytes + retained_bytes + former_duplicate_charge > admission_budget,
        "fixture must reject the superseded duplicate input charge"
    );
    let admission: Arc<dyn cdf_memory::MemoryCoordinator> = Arc::new(
        cdf_memory::DeterministicMemoryCoordinator::new(admission_budget, BTreeMap::new()).unwrap(),
    );
    let source_lease = admission
        .try_reserve(
            &cdf_memory::ReservationRequest::new(
                cdf_memory::ConsumerKey::new(
                    "canonical-admission-source",
                    cdf_memory::MemoryClass::Source,
                )
                .unwrap(),
                source_bytes,
            )
            .unwrap(),
        )
        .unwrap()
        .unwrap();
    let output_lease = admission
        .try_reserve(
            &cdf_memory::ReservationRequest::new(
                cdf_memory::ConsumerKey::new(
                    "canonical-admission-output",
                    cdf_memory::MemoryClass::Transform,
                )
                .unwrap(),
                retained_bytes,
            )
            .unwrap(),
        )
        .unwrap()
        .unwrap();
    let former_request = cdf_memory::ReservationRequest::new(
        cdf_memory::ConsumerKey::new(
            "canonical-admission-former",
            cdf_memory::MemoryClass::Package,
        )
        .unwrap(),
        former_duplicate_charge,
    )
    .unwrap();
    assert!(admission.try_reserve(&former_request).unwrap().is_none());
    let current_request = cdf_memory::ReservationRequest::new(
        cdf_memory::ConsumerKey::new(
            "canonical-admission-current",
            cdf_memory::MemoryClass::Package,
        )
        .unwrap(),
        accounted_construction,
    )
    .unwrap();
    let construction_lease = admission
        .try_reserve(&current_request)
        .unwrap()
        .expect("actual construction working set must fit");
    drop((construction_lease, output_lease, source_lease));
    assert_eq!(admission.snapshot().current_bytes, 0);

    // The governed default residual-capture policy reserves its own worst-case transform
    // expansion. Keep that independent policy out of this boundary-specific lifecycle proof.
    let budget = 192 * 1024 * 1024;
    let memory: Arc<dyn cdf_memory::MemoryCoordinator> =
        Arc::new(cdf_memory::DeterministicMemoryCoordinator::new(budget, BTreeMap::new()).unwrap());
    let host = Arc::new(
        StandaloneExecutionHost::new(
            cdf_runtime::ExecutionHostCapabilities {
                logical_cpu_slots: 1,
                io_workers: 1,
                blocking_lanes: Vec::new(),
            },
            Arc::clone(&memory),
        )
        .unwrap(),
    );
    let services = cdf_runtime::ExecutionServices::new(host).unwrap();
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

    assert_eq!(output.output.profile.output_rows, row_count);
    assert_eq!(output.output.identity_segments().len(), 1);
    cdf_package::PackageReader::open(package.path())
        .unwrap()
        .verify()
        .unwrap();
    let snapshot = memory.snapshot();
    assert!(snapshot.peak_bytes <= budget);
    assert_eq!(snapshot.current_bytes, 0);
}

#[test]
fn sequential_statistics_scratch_reserves_only_the_largest_batch() {
    let first = batch_for_partition(
        "stats-first",
        "part-0",
        vec![1, 2, 3],
        vec!["one", "two", "three"],
        vec![true, false, true],
    );
    let second = batch_for_partition(
        "stats-second",
        "part-0",
        vec![4, 5],
        vec!["four", "five"],
        vec![false, true],
    );
    let first = first.record_batch().unwrap().clone();
    let second = second.record_batch().unwrap().clone();
    let expected = cdf_kernel::BatchStats::computation_reservation_bytes(&first)
        .unwrap()
        .max(cdf_kernel::BatchStats::computation_reservation_bytes(&second).unwrap());
    assert_eq!(
        crate::execution::statistics_computation_reservation_bytes(&[first, second]).unwrap(),
        expected
    );
}

#[test]
fn parallel_segment_frontier_failure_joins_workers_and_prevents_finalization() {
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
    let package_dir = TempDir::new().unwrap();
    let (_, services) = StandaloneExecutionHost::default_services(64 * 1024 * 1024).unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let mut durable_segment =
        |_entry: &SegmentEntry, _payload: DurableSegmentPayload| -> Result<()> {
            Err(cdf_kernel::CdfError::internal(
                "stop at canonical segment frontier",
            ))
        };
    let mut stream_finalize =
        || -> Result<()> { panic!("failed segment frontier must not reach stream finalization") };

    let error = block_on(execute_to_package_with_streaming_hooks(
        &plan,
        &resource,
        package_dir.path(),
        &pre_finalize,
        &mut durable_segment,
        &mut stream_finalize,
        EngineExecutionConfig::default().with_execution_services(services.clone()),
    ))
    .unwrap_err();

    assert!(error.message.contains("canonical segment frontier"));
    assert_eq!(
        cdf_package::PackageReader::open(package_dir.path())
            .unwrap()
            .manifest()
            .lifecycle
            .status,
        PackageStatus::Extracting
    );
    assert_eq!(services.memory().snapshot().current_bytes, 0);
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
        version: cdf_kernel::SOURCE_POSITION_VERSION,
        files: vec![FilePosition {
            path: "/tmp/cdf/events.ndjson".to_owned(),
            size_bytes: 42,
            source_generation: None,
            etag: None,
            object_version: None,
            sha256: Some(
                "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                    .to_owned(),
            ),
        }],
    }));
    batch
}
