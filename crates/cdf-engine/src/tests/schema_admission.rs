use super::support::{
    Arc, ArrayRef, AtomicUsize, BTreeMap, Batch, BatchId, CompiledStreamAdmissionEvidence,
    ContractPolicy, DataType, DiscoveryExecutorBudgetEvidence, DurableSegmentPayload,
    EffectiveSchemaCatalogEntry, EffectiveSchemaObservationEvidence, EffectiveSchemaRuntime,
    EngineExecutionConfig, EngineExecutionEvidence, EnginePackageDraft, ExecutionExtent, Field,
    FieldCoercionDecision, FieldDisposition, Int32Array, Int64Array, MockResource, Mutex,
    ObservedSchema, Ordering, PLAN_PHYSICAL_SCHEMA_HASH_KEY, PLAN_SCHEMA_OBSERVATION_BINDING_KEY,
    PLAN_SCHEMA_OBSERVATION_ID_KEY, PartitionAttestation, PartitionId, PhysicalObservationEvidence,
    Planner, PreContractResidualCandidate, RecordBatch, ResourceId, RowRule, Schema, SchemaHash,
    SchemaObservationFieldQuarantine, SchemaRef, SegmentEntry, StandaloneExecutionHost,
    StreamAdmissionObservationEvidence, StringArray, TempDir, TerminalSchemaObservationQuarantine,
    TrustLevel, WriteDisposition, batch_for_partition_with_schema, block_on,
    bound_effective_schema_evidence, coercion_decision, compile_validation_program,
    execute_to_package, execute_to_package_with_segment_positions,
    execute_to_package_with_streaming_hooks, incompatible_sample_schema, plan_input,
    plan_input_for_schema, read_package_segment, reconcile_schema, rename_column_program_output,
    sample_batches, sample_schema, schema_observation_binding, semantic_field, source_name,
    stream_admission_coercion, terminal_effective_schema_runtime, terminal_file_position,
};
use super::support::{ResourceStream, Result};
use arrow_array::{BooleanArray, Decimal128Array};

#[test]
fn validation_program_rebind_atomically_rebuilds_compiled_output_schema() {
    let resource = MockResource::tier_a(sample_batches());
    let mut plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let before = plan.output_arrow_schema().unwrap();
    assert!(before.field_with_name("name").is_ok());
    let mut rebound = plan.validation_program.clone();
    rename_column_program_output(&mut rebound, "name", "customer_name");

    plan.rebind_validation_program(rebound, resource.schema().as_ref())
        .unwrap();

    let after = plan.output_arrow_schema().unwrap();
    assert!(after.field_with_name("name").is_err());
    assert!(after.field_with_name("customer_name").is_ok());
    crate::planning::validate_plan_schema_authority(&resource, &plan).unwrap();
}

#[test]
fn compiled_stream_admission_is_replay_verifiable_and_rejects_mismatched_evidence() {
    let resource = MockResource::tier_a(sample_batches());
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let physical_schema_hash =
        cdf_kernel::canonical_arrow_schema_hash(resource.schema().as_ref()).unwrap();
    let coercion_plan = plan
        .compiled_schema_admission
        .instantiate(resource.schema().as_ref(), &physical_schema_hash)
        .unwrap();
    let physical_observation =
        crate::PhysicalObservationEvidence::arrow_schema(resource.schema().as_ref()).unwrap();
    let physical_observation_hash = physical_observation.identity_hash().unwrap();
    let evidence = CompiledStreamAdmissionEvidence {
        compiled_admission_hash: cdf_runtime::artifact_hash(&plan.compiled_schema_admission)
            .unwrap(),
        baseline_schema_hash: plan.schema_authority.baseline_schema_hash.to_string(),
        effective_schema_hash: plan.schema_authority.effective_schema_hash.to_string(),
        physical_observation_catalog: BTreeMap::from([(
            physical_observation_hash.to_string(),
            physical_observation,
        )]),
        routed_admission_catalog: BTreeMap::new(),
        observations: vec![
            StreamAdmissionObservationEvidence::new(
                "partition-1",
                physical_observation_hash,
                coercion_plan,
                crate::StreamAdmissionCompletion::Complete {
                    source_position: terminal_file_position(),
                    partition_binding: cdf_kernel::SchemaObservationBinding::new(format!(
                        "sha256:{:064x}",
                        1
                    ))
                    .unwrap(),
                },
            )
            .unwrap(),
        ],
    };

    evidence.validate(&plan.compiled_schema_admission).unwrap();
    let mut unbound = evidence.clone();
    unbound.observations[0].physical_observation_hash = "sha256:unbound".to_owned();
    let error = unbound
        .validate(&plan.compiled_schema_admission)
        .unwrap_err();
    assert!(
        error.to_string().contains("absent physical observation"),
        "{error}"
    );
    let mut unauthorized = evidence.clone();
    unauthorized.observations[0].coercion_plan.fields[0].decision =
        FieldCoercionDecision::LossyAllowed;
    let error = unauthorized
        .validate(&plan.compiled_schema_admission)
        .unwrap_err();
    assert!(
        error.to_string().contains("compiled coercion verdict"),
        "{error}"
    );

    let mut forged = evidence.clone();
    forged.observations[0].coercion_plan.fields[0].decision = FieldCoercionDecision::Missing;
    let error = forged
        .validate(&plan.compiled_schema_admission)
        .unwrap_err();
    assert!(
        error.to_string().contains("compiled coercion verdict"),
        "{error}"
    );

    let mut with_unused_catalog_entry = evidence.clone();
    let unused = crate::PhysicalObservationEvidence::materialized_output(
        resource.schema().as_ref(),
        resource.schema().as_ref(),
        Vec::<String>::new(),
    )
    .unwrap();
    with_unused_catalog_entry
        .physical_observation_catalog
        .insert(unused.identity_hash().unwrap().to_string(), unused);
    let error = with_unused_catalog_entry
        .validate(&plan.compiled_schema_admission)
        .unwrap_err();
    assert!(
        error.to_string().contains("exact referenced set"),
        "{error}"
    );

    let mut mismatched = evidence;
    mismatched.compiled_admission_hash = "sha256:mismatched".to_owned();
    let error = mismatched
        .validate(&plan.compiled_schema_admission)
        .unwrap_err();
    assert!(error.to_string().contains("does not match"), "{error}");
}

#[test]
fn compiled_stream_admission_fails_unknown_fields_and_admits_lossless_widening() {
    let resource = MockResource::tier_a(sample_batches());
    let mut plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    assert!(
        plan.compiled_schema_admission
            .captures_unknown_fields()
            .unwrap(),
        "capture_variant must preserve admitted unknown fields in its residual capture"
    );

    plan.compiled_schema_admission.admission.field = FieldDisposition::FailRun;
    let unknown = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
        Field::new("unexpected", DataType::Int64, true),
    ]);
    let unknown_hash = cdf_kernel::canonical_arrow_schema_hash(&unknown).unwrap();
    let error = plan
        .compiled_schema_admission
        .instantiate(&unknown, &unknown_hash)
        .unwrap_err();
    assert!(error.to_string().contains("unknown field"), "{error}");

    plan.compiled_schema_admission.admission.field = FieldDisposition::CaptureVariant;
    let narrow = Schema::new(vec![
        Field::new("id", DataType::Int16, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
    ]);
    let narrow_hash = cdf_kernel::canonical_arrow_schema_hash(&narrow).unwrap();
    let widening = plan
        .compiled_schema_admission
        .instantiate(&narrow, &narrow_hash)
        .unwrap();
    assert_eq!(widening.fields[0].decision, FieldCoercionDecision::Widened);
}

#[test]
fn pinned_baseline_admission_projects_full_physical_catalog_before_hashing() {
    let baseline_physical = sample_schema();
    let baseline_hash =
        cdf_kernel::canonical_arrow_schema_hash(baseline_physical.as_ref()).unwrap();
    let effective = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let resource = MockResource::tier_b(sample_batches())
        .with_schema(effective.clone())
        .with_baseline_observation_schema_catalog(vec![EffectiveSchemaCatalogEntry::new(
            baseline_hash,
            baseline_physical,
        )]);
    let mut input = plan_input_for_schema(
        effective,
        Vec::new(),
        Some(vec!["id".to_owned()]),
        None,
        ExecutionExtent::bounded(),
    );
    input.validation_program = compile_validation_program(
        &ContractPolicy::for_trust(TrustLevel::Financial),
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();
    input.validation_program.row_rules.clear();

    let plan = Planner::new().plan_tier_b(&resource, input).unwrap();
    assert_eq!(
        plan.compiled_schema_admission.baseline_projection,
        Some(vec!["id".to_owned()])
    );
    let projected_baseline = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
    let projected_hash = cdf_kernel::canonical_arrow_schema_hash(&projected_baseline).unwrap();
    plan.compiled_schema_admission
        .instantiate(&projected_baseline, &projected_hash)
        .unwrap();

    let new_drift = Schema::new(vec![Field::new("id", DataType::Int16, false)]);
    let drift_hash = cdf_kernel::canonical_arrow_schema_hash(&new_drift).unwrap();
    let drift = plan
        .compiled_schema_admission
        .instantiate(&new_drift, &drift_hash)
        .unwrap();
    assert_eq!(drift.fields[0].decision, FieldCoercionDecision::Widened);
}

#[test]
fn materialized_stream_admission_rejects_noncanonical_provenance_and_nullable_claims() {
    let resource = MockResource::tier_a(sample_batches());
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let output_schema = plan
        .compiled_schema_admission
        .constraint_schema
        .to_arrow()
        .unwrap();
    let output_hash = cdf_kernel::canonical_arrow_schema_hash(output_schema.as_ref()).unwrap();
    let coercion = plan
        .compiled_schema_admission
        .instantiate(output_schema.as_ref(), &output_hash)
        .unwrap();
    let physical = crate::PhysicalObservationEvidence::materialized_output(
        output_schema.as_ref(),
        output_schema.as_ref(),
        Vec::<String>::new(),
    )
    .unwrap();
    let physical_hash = physical.identity_hash().unwrap();
    let evidence = CompiledStreamAdmissionEvidence::new(
        &plan.compiled_schema_admission,
        BTreeMap::from([(physical_hash.to_string(), physical)]),
        vec![
            StreamAdmissionObservationEvidence::new(
                "part-0",
                physical_hash,
                coercion,
                crate::StreamAdmissionCompletion::CompleteUnpositioned {
                    partition_binding: cdf_kernel::SchemaObservationBinding::new(format!(
                        "sha256:{:064x}",
                        2
                    ))
                    .unwrap(),
                },
            )
            .unwrap(),
        ],
    )
    .unwrap();

    let mut forged_reason = evidence.clone();
    forged_reason.observations[0].coercion_plan.fields[0].reason = "forged".to_owned();
    let error = forged_reason
        .validate(&plan.compiled_schema_admission)
        .unwrap_err();
    assert!(
        error.to_string().contains("typed physical observation"),
        "{error}"
    );

    let mut forged_relation = evidence.clone();
    let forged_relation_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
    ]);
    let forged_physical = crate::PhysicalObservationEvidence::materialized_output(
        &forged_relation_schema,
        output_schema.as_ref(),
        Vec::<String>::new(),
    )
    .unwrap();
    let forged_hash = forged_physical.identity_hash().unwrap();
    forged_relation.physical_observation_catalog =
        BTreeMap::from([(forged_hash.to_string(), forged_physical)]);
    forged_relation.observations[0].physical_observation_hash = forged_hash.to_string();
    let error = forged_relation
        .validate(&plan.compiled_schema_admission)
        .unwrap_err();
    assert!(
        error.to_string().contains("typed physical observation"),
        "{error}"
    );

    let mut forged_nullable = evidence;
    let forged_physical = crate::PhysicalObservationEvidence::materialized_output(
        output_schema.as_ref(),
        output_schema.as_ref(),
        ["id".to_owned()],
    )
    .unwrap();
    let forged_hash = forged_physical.identity_hash().unwrap();
    forged_nullable.physical_observation_catalog =
        BTreeMap::from([(forged_hash.to_string(), forged_physical)]);
    forged_nullable.observations[0].physical_observation_hash = forged_hash.to_string();
    let error = forged_nullable
        .validate(&plan.compiled_schema_admission)
        .unwrap_err();
    assert!(
        error.to_string().contains("nullable residual identities"),
        "{error}"
    );
}

#[test]
fn planning_rejects_one_schema_observation_identity_across_partitions() {
    let schema = sample_schema();
    let physical_hash = cdf_kernel::canonical_arrow_schema_hash(schema.as_ref()).unwrap();
    let evidence = bound_effective_schema_evidence(
        SchemaHash::new("effective-unique-observations-v1").unwrap(),
        "manifest-unique-observations-v1",
        ".cdf/schemas/orders@manifest-unique-observations-v1.discovery.json",
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
            schema.clone(),
        )],
    )
    .unwrap();
    let resource = MockResource::tier_b(sample_batches())
        .with_effective_schema_runtime(schema, runtime)
        .with_duplicate_observation_identity();

    let error = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap_err();

    assert!(
        error.to_string().contains("assigned to planned partitions"),
        "{error}"
    );
}

#[test]
fn dynamic_planning_rejects_duplicate_observation_identity_without_runtime_evidence() {
    let resource = MockResource::tier_b(sample_batches()).with_duplicate_observation_identity();

    let error = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap_err();

    assert!(
        error.to_string().contains("assigned to planned partitions"),
        "{error}"
    );
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
}

#[test]
fn execution_rejects_duplicate_planned_observations_before_staged_ingress() {
    let resource = MockResource::tier_b(sample_batches());
    let mut plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    for partition in plan.scan.inline_partitions_mut().unwrap() {
        partition.metadata.insert(
            PLAN_SCHEMA_OBSERVATION_ID_KEY.to_owned(),
            "forged-shared-observation".to_owned(),
        );
    }
    let package_dir = TempDir::new().unwrap();
    let durable_calls = Arc::new(AtomicUsize::new(0));
    let hook_calls = Arc::clone(&durable_calls);
    let mut durable_segment = move |_entry: &SegmentEntry, _payload: DurableSegmentPayload| {
        hook_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    };
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let mut stream_finalize = || Ok(());

    let error = block_on(execute_to_package_with_streaming_hooks(
        &plan,
        &resource,
        package_dir.path(),
        &pre_finalize,
        &mut durable_segment,
        &mut stream_finalize,
        EngineExecutionConfig::default(),
    ))
    .unwrap_err();

    assert!(
        error.to_string().contains("assigned to planned partitions"),
        "{error}"
    );
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
    assert_eq!(durable_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn execution_evidence_rejects_repeated_observation_identity_even_when_identical() {
    let observation = cdf_kernel::ProcessedObservationPosition::new(
        "input-0",
        cdf_kernel::ProcessedObservationOutcome::Admitted,
        terminal_file_position(),
    )
    .unwrap();

    let error = EngineExecutionEvidence::new(
        vec![observation.clone(), observation],
        Vec::new(),
        None,
        true,
    )
    .unwrap_err();

    assert!(
        error.to_string().contains("more than one partition"),
        "{error}"
    );
}

#[test]
fn missing_identity_field_uses_the_compiled_fail_run_disposition() {
    let resource = MockResource::tier_a(sample_batches());
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let physical = Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
    ]);
    let physical_hash = cdf_kernel::canonical_arrow_schema_hash(&physical).unwrap();

    let error = plan
        .compiled_schema_admission
        .instantiate_or_quarantine("missing-id", &physical, &physical_hash)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("compiled disposition is fail_run")
    );
}

#[test]
fn recorded_schema_quarantine_must_match_the_compiled_admission_action() {
    let resource = MockResource::tier_b(Vec::new()).without_control_keys();
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let physical = incompatible_sample_schema();
    let physical_hash = cdf_kernel::canonical_arrow_schema_hash(physical.as_ref()).unwrap();
    let quarantine = TerminalSchemaObservationQuarantine::new(
        "input-0",
        physical_hash,
        "schema-observation:incompatible-partition",
        "schema_observation_quarantined",
        "publish compatible input",
        vec![
            SchemaObservationFieldQuarantine::whole_schema("incompatible physical schema").unwrap(),
        ],
    )
    .unwrap();
    let physical_evidence = PhysicalObservationEvidence::arrow_schema(physical.as_ref()).unwrap();

    let error = plan
        .compiled_schema_admission
        .validate_quarantined_observation(&quarantine, &physical_evidence)
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("does not match the compiled admission action"),
        "{error}"
    );
}

#[test]
fn validation_program_rebind_rejects_new_physical_dependencies_without_mutating_plan() {
    let resource = MockResource::tier_b(sample_batches());
    let mut input = plan_input(
        Vec::new(),
        Some(vec!["name".to_owned()]),
        None,
        ExecutionExtent::bounded(),
    );
    input.validation_program = compile_validation_program(
        &ContractPolicy::for_trust(TrustLevel::Financial),
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();
    input.validation_program.row_rules.clear();
    let mut plan = Planner::new().plan_tier_b(&resource, input).unwrap();
    assert_eq!(
        plan.scan.request.projection,
        Some(vec!["id".to_owned(), "name".to_owned()])
    );
    let original = plan.clone();
    let mut policy = ContractPolicy::for_trust(TrustLevel::Financial);
    policy.rows.rules = vec![RowRule::Nullability {
        column: "active".to_owned(),
    }];
    let replacement = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();

    let error = plan
        .rebind_validation_program(replacement, resource.schema().as_ref())
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("outside the compiled physical projection")
    );
    assert_eq!(plan, original);
}

#[test]
fn effective_schema_binds_only_the_attempted_partition_observation_under_limit() {
    let effective_schema = sample_schema();
    let physical_schema = sample_schema();
    let physical_hash = cdf_kernel::canonical_arrow_schema_hash(physical_schema.as_ref()).unwrap();
    let mut batches = vec![
        batch_for_partition_with_schema(
            "batch-limit-0",
            "part-0",
            physical_schema.clone(),
            vec![1, 2, 3],
            vec!["one", "two", "three"],
            vec![true, true, true],
        ),
        batch_for_partition_with_schema(
            "batch-limit-1",
            "part-1",
            physical_schema.clone(),
            vec![4, 5, 6],
            vec!["four", "five", "six"],
            vec![true, true, true],
        ),
    ];
    for batch in &mut batches {
        batch.header.observed_schema_hash = physical_hash.clone();
        batch.header.source_position = Some(terminal_file_position());
    }
    let evidence = bound_effective_schema_evidence(
        SchemaHash::new("effective-snapshot-v1").unwrap(),
        "manifest-v1",
        ".cdf/schemas/orders@manifest-v1.discovery.json",
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
    .unwrap()
    .with_discovery_executor_budget(
        DiscoveryExecutorBudgetEvidence::new(64, 1_000, 128, 2).unwrap(),
    )
    .unwrap();
    let resource =
        MockResource::tier_b(batches).with_effective_schema_runtime(effective_schema, runtime);
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, Some(1), ExecutionExtent::bounded()),
        )
        .unwrap();
    assert_eq!(
        plan.effective_schema_evidence().unwrap().observations.len(),
        1
    );

    let temp = TempDir::new().unwrap();
    block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(resource.open_count.load(Ordering::SeqCst), 1);
    assert_eq!(resource.attest_count.load(Ordering::SeqCst), 0);
    let witnessed: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temp.path().join("schema/stream-admission-evidence.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(witnessed["observations"].as_array().unwrap().len(), 1);
    assert_eq!(
        witnessed["observations"][0]["completion"]["kind"],
        "partial"
    );

    let mut tampered = plan.clone();
    tampered
        .effective_schema_evidence
        .as_mut()
        .unwrap()
        .discovery_executor_budget =
        Some(DiscoveryExecutorBudgetEvidence::new(32, 1_000, 128, 2).unwrap());
    let tampered_package = TempDir::new().unwrap();
    let error = block_on(execute_to_package(
        &tampered,
        &resource,
        tampered_package.path(),
    ))
    .unwrap_err();
    assert!(
        error.to_string().contains("discovery executor budget"),
        "{error}"
    );
}

#[test]
fn pushed_projection_rebinds_preobserved_physical_evidence_before_execution() {
    let effective_schema = sample_schema();
    let physical_schema = sample_schema();
    let physical_hash = cdf_kernel::canonical_arrow_schema_hash(physical_schema.as_ref()).unwrap();
    let projected_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let projected_hash =
        cdf_kernel::canonical_arrow_schema_hash(projected_schema.as_ref()).unwrap();
    let record_batch = RecordBatch::try_new(
        projected_schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
    )
    .unwrap();
    let mut batch = Batch::from_record_batch(
        BatchId::new("batch-projected-preobserved").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        projected_hash.clone(),
        record_batch,
    )
    .unwrap();
    batch.header.source_position = Some(terminal_file_position());

    let evidence = bound_effective_schema_evidence(
        SchemaHash::new("effective-projected-v1").unwrap(),
        "manifest-projected-v1",
        ".cdf/schemas/orders@manifest-projected-v1.discovery.json",
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
    let resource = MockResource::tier_b(vec![batch])
        .with_partition_count(1)
        .with_effective_schema_runtime(effective_schema, runtime);
    let mut input = plan_input(
        Vec::new(),
        Some(vec!["id".to_owned()]),
        None,
        ExecutionExtent::bounded(),
    );
    input.validation_program = compile_validation_program(
        &ContractPolicy::for_trust(TrustLevel::Financial),
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();
    input.validation_program.row_rules.clear();
    let plan = Planner::new().plan_tier_b(&resource, input).unwrap();
    let planned = &plan.effective_schema_evidence().unwrap().observations[0];
    assert_eq!(
        planned.physical_schema_hash,
        projected_hash,
        "compiled projection: {:?}",
        plan.scan.inline_partitions().unwrap()[0]
            .scan_intent
            .projection
    );
    assert_eq!(
        plan.scan.inline_partitions().unwrap()[0]
            .metadata
            .get(PLAN_PHYSICAL_SCHEMA_HASH_KEY),
        Some(&projected_hash.to_string())
    );

    let temp = TempDir::new().unwrap();
    block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();
}

#[test]
fn tagged_mongodb_decimal_catalog_plans_source_materialized_exact_output() {
    let pii_amount = semantic_field(
        Field::new("amount", DataType::Decimal128(18, 2), false),
        "cdf.pii@1(class=\"financial\")",
    );
    let nested_amount = Field::new("nested_amount", DataType::Decimal128(18, 2), true);
    let list_item = Field::new("item", DataType::Decimal128(18, 2), true);
    let effective_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        pii_amount,
        Field::new_struct("profile", vec![nested_amount], true),
        Field::new_list("amounts", list_item, true),
        Field::new("active", DataType::Boolean, false),
    ]));
    let physical_decimal = |name: &str, nullable: bool| {
        semantic_field(
            cdf_kernel::with_physical_type(
                Field::new(name, DataType::Utf8, nullable),
                "bson:decimal128",
            ),
            cdf_semantic::MONGODB_DECIMAL128_TEXT_SEMANTIC,
        )
    };
    let physical_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        physical_decimal("amount", false),
        Field::new_struct(
            "profile",
            vec![physical_decimal("nested_amount", true)],
            true,
        ),
        Field::new_list("amounts", physical_decimal("item", true), true),
        Field::new("active", DataType::Boolean, false),
    ]));
    let exact_materializer = |field_path: Vec<&str>, output: &DataType| {
        cdf_kernel::SourceMaterializationRule::new(
            "mongodb.bson_decimal128_to_arrow_decimal128.v1",
            field_path.into_iter().map(str::to_owned).collect(),
            cdf_kernel::CanonicalArrowType::from_arrow(&DataType::Utf8).unwrap(),
            BTreeMap::from([
                (
                    cdf_kernel::PHYSICAL_TYPE_METADATA_KEY.to_owned(),
                    "bson:decimal128".to_owned(),
                ),
                (
                    cdf_kernel::SEMANTIC_METADATA_KEY.to_owned(),
                    cdf_semantic::MONGODB_DECIMAL128_TEXT_SEMANTIC.to_owned(),
                ),
            ]),
            cdf_kernel::CanonicalArrowType::from_arrow(output).unwrap(),
        )
        .unwrap()
    };
    let mut source_materializations = vec![
        exact_materializer(vec!["amount"], effective_schema.field(1).data_type()),
        exact_materializer(
            vec!["profile", "nested_amount"],
            &DataType::Decimal128(18, 2),
        ),
        exact_materializer(vec!["amounts", "item"], &DataType::Decimal128(18, 2)),
    ];
    source_materializations.sort_by(|left, right| left.field_path.cmp(&right.field_path));
    let physical_hash = cdf_kernel::canonical_arrow_schema_hash(physical_schema.as_ref()).unwrap();
    let decimals = Decimal128Array::from(vec![1_234_i128])
        .with_precision_and_scale(18, 2)
        .unwrap();
    let record_batch = RecordBatch::try_new(
        Arc::clone(&effective_schema),
        vec![
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            Arc::new(decimals) as ArrayRef,
            arrow_array::new_null_array(effective_schema.field(2).data_type(), 1),
            arrow_array::new_null_array(effective_schema.field(3).data_type(), 1),
            Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
        ],
    )
    .unwrap();
    let mut batch = Batch::from_record_batch(
        BatchId::new("batch-mongodb-decimal").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        physical_hash.clone(),
        record_batch,
    )
    .unwrap();
    batch
        .header
        .mark_materialized_output(physical_schema.as_ref())
        .unwrap();
    batch.header.source_position = Some(terminal_file_position());

    let evidence = bound_effective_schema_evidence(
        cdf_kernel::canonical_arrow_schema_hash(effective_schema.as_ref()).unwrap(),
        "manifest-mongodb-decimal-v1",
        ".cdf/schemas/orders@manifest-mongodb-decimal-v1.discovery.json",
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
    let resource = MockResource::tier_b(vec![batch])
        .with_partition_count(1)
        .with_effective_schema_runtime(Arc::clone(&effective_schema), runtime)
        .with_source_materializations(source_materializations);
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input_for_schema(
                effective_schema,
                Vec::new(),
                None,
                None,
                ExecutionExtent::bounded(),
            ),
        )
        .unwrap();
    let fields = &plan.effective_schema_evidence().unwrap().observations[0]
        .coercion_plan
        .fields;
    for index in [1, 2, 3] {
        assert_eq!(
            fields[index].decision,
            FieldCoercionDecision::SourceMaterializedExact
        );
    }
    plan.validate_compiled_schema_admission(&resource).unwrap();
    let mut tampered = plan.clone();
    tampered.compiled_schema_admission.source_materializations[0].materializer_id =
        "other.exact_decimal.v1".to_owned();
    let error = tampered
        .validate_compiled_schema_admission(&resource)
        .unwrap_err();
    assert!(error.message.contains("source materializations"), "{error}");

    let temp = TempDir::new().unwrap();
    block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();
}

#[test]
fn terminal_schema_observation_quarantine_processes_distinct_partitions_without_opening_data() {
    let effective_schema = sample_schema();
    let physical_schema = incompatible_sample_schema();
    let physical_hash = cdf_kernel::canonical_arrow_schema_hash(physical_schema.as_ref()).unwrap();
    let runtime = terminal_effective_schema_runtime(physical_schema, physical_hash.clone());
    let processed_position = terminal_file_position();
    let secret_batches = vec![batch_for_partition_with_schema(
        "secret-batch",
        "part-0",
        effective_schema.clone(),
        vec![1],
        vec!["super-secret-row-value"],
        vec![true],
    )];
    let resource = MockResource::tier_b(secret_batches)
        .with_effective_schema_runtime(effective_schema, runtime)
        .without_control_keys()
        .with_attestation(PartitionAttestation::new(
            processed_position.clone(),
            Some(physical_hash),
        ));
    let plan = Planner::new()
        .plan_tier_b(
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
    assert!(output.segment_positions.is_empty());
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
    assert_eq!(resource.attest_count.load(Ordering::SeqCst), 2);
    let processed = output.execution_evidence().processed_observations();
    assert_eq!(processed.len(), 2);
    assert!(
        processed
            .iter()
            .all(|observation| observation.source_position == processed_position)
    );
    assert!(
        temp.path()
            .join("quarantine/schema-observations.json")
            .is_file()
    );
    assert!(
        temp.path()
            .join("quarantine/schema-admission-evidence.json")
            .is_file()
    );
    assert!(!temp.path().join("quarantine/records.parquet").is_file());
    let terminal_json =
        std::fs::read_to_string(temp.path().join("quarantine/schema-observations.json")).unwrap();
    assert!(!terminal_json.contains("super-secret-row-value"));

    let mut conflicting = plan.clone();
    conflicting.scan.inline_partitions_mut().unwrap()[1]
        .metadata
        .insert(
            PLAN_SCHEMA_OBSERVATION_BINDING_KEY.to_owned(),
            "conflicting-binding".to_owned(),
        );
    let conflicting_package = TempDir::new().unwrap();
    let error = block_on(execute_to_package(
        &conflicting,
        &resource,
        conflicting_package.path(),
    ))
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("missing or spoofed cdf:schema_observation_binding"),
        "{error}"
    );
}

#[test]
fn terminal_schema_observation_attestation_change_aborts_before_processed_evidence() {
    let effective_schema = sample_schema();
    let physical_schema = incompatible_sample_schema();
    let physical_hash = cdf_kernel::canonical_arrow_schema_hash(physical_schema.as_ref()).unwrap();
    let runtime = terminal_effective_schema_runtime(physical_schema, physical_hash);
    let resource = MockResource::tier_b(Vec::new())
        .with_effective_schema_runtime(effective_schema, runtime)
        .without_control_keys()
        .with_attestation(PartitionAttestation::new(
            terminal_file_position(),
            Some(SchemaHash::new("changed-physical-schema").unwrap()),
        ));
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let temp = TempDir::new().unwrap();

    let error = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap_err();

    assert!(
        error.to_string().contains("changed physical schema"),
        "{error}"
    );
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
    assert_eq!(resource.attest_count.load(Ordering::SeqCst), 1);
    assert!(
        !temp
            .path()
            .join("state/processed-observations.json")
            .exists()
    );
    assert!(
        !temp
            .path()
            .join("quarantine/schema-observations.json")
            .exists()
    );
}

#[test]
fn terminal_schema_observation_identity_attestation_failure_aborts_before_processed_evidence() {
    let effective_schema = sample_schema();
    let physical_schema = incompatible_sample_schema();
    let physical_hash = cdf_kernel::canonical_arrow_schema_hash(physical_schema.as_ref()).unwrap();
    let runtime = terminal_effective_schema_runtime(physical_schema, physical_hash);
    let resource = MockResource::tier_b(Vec::new())
        .with_effective_schema_runtime(effective_schema, runtime)
        .without_control_keys()
        .with_attestation_error("file identity changed between planning and execution");
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let temp = TempDir::new().unwrap();

    let error = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap_err();

    assert!(
        error.to_string().contains("file identity changed"),
        "{error}"
    );
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
    assert_eq!(resource.attest_count.load(Ordering::SeqCst), 1);
    assert!(
        !temp
            .path()
            .join("state/processed-observations.json")
            .exists()
    );
}

#[test]
fn validation_program_source_name_can_cover_and_rename_batch_field() {
    let resource = MockResource::tier_a(sample_batches()).without_control_keys();
    let mut input = plan_input(
        vec![],
        Some(vec!["name".to_owned()]),
        None,
        ExecutionExtent::bounded(),
    );
    rename_column_program_output(&mut input.validation_program, "name", "customer_name");
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 3);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = read_package_segment(&reader, &output.identity_segments()[0].segment_id);
    let schema = batches[0].schema();
    let field = schema.field(0);
    assert_eq!(field.name(), "customer_name");
    assert_eq!(source_name(field), Some("name"));
}

#[test]
fn validation_program_output_name_can_cover_already_normalized_batch_field() {
    let resource = MockResource::tier_a(output_name_batches()).without_control_keys();
    let mut input = plan_input_for_schema(
        output_name_schema(),
        vec![],
        Some(vec!["customer_name".to_owned()]),
        None,
        ExecutionExtent::bounded(),
    );
    rename_column_program_source(&mut input.validation_program, "customer_name", "name");
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 3);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = read_package_segment(&reader, &output.identity_segments()[0].segment_id);
    let schema = batches[0].schema();
    let field = schema.field(0);
    assert_eq!(field.name(), "customer_name");
    assert_eq!(source_name(field), Some("name"));
}

#[test]
fn compiled_output_schema_strips_runtime_provenance_only_after_serializing_evidence() {
    let observed = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
    let constraint = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let reconciliation = reconcile_schema(
        &observed,
        constraint.as_ref(),
        &ContractPolicy::default().types,
    )
    .unwrap();
    let serialized_plan = serde_json::to_string(&reconciliation.plan).unwrap();
    let runtime_schema = Arc::new(reconciliation.schema);
    assert_eq!(
        runtime_schema
            .field(0)
            .metadata()
            .get("cdf:physical_type")
            .map(String::as_str),
        Some("Int32")
    );
    let record_batch = RecordBatch::try_new(
        runtime_schema,
        vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
    )
    .unwrap();
    let mut batch = Batch::from_record_batch(
        BatchId::new("batch-runtime-provenance").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        SchemaHash::new("schema-runtime-provenance").unwrap(),
        record_batch,
    )
    .unwrap();
    batch.header.schema_coercion_plan = Some(serialized_plan);
    batch.header.mark_materialized_output(&observed).unwrap();
    let resource = MockResource::tier_a(vec![batch]).with_schema(constraint.clone());
    let input = plan_input_for_schema(constraint, vec![], None, None, ExecutionExtent::bounded());
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();

    block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    reader.verify().unwrap();
    let runtime_output = reader.runtime_arrow_schema().unwrap();
    assert_eq!(runtime_output, plan.output_arrow_schema().unwrap());
    assert!(
        !runtime_output
            .field(0)
            .metadata()
            .contains_key("cdf:physical_type")
    );
    assert_eq!(
        runtime_output
            .field(0)
            .metadata()
            .get("cdf:source_name")
            .map(String::as_str),
        Some("id")
    );
    let evidence = stream_admission_coercion(temp.path());
    let widened = coercion_decision(&evidence, "id");
    assert_eq!(widened.observed_type.as_deref(), Some("Int32"));
    assert_eq!(widened.constraint_type.as_deref(), Some("Int64"));
    assert_eq!(widened.decision, FieldCoercionDecision::Widened);
}

#[test]
fn residual_multi_partition_decisions_share_verified_effective_schema_and_keep_identity() {
    const CAPTURE_SENTINEL: &str = "rp2-captured-pii-sentinel";
    const QUARANTINE_SENTINEL: &str = "rp2-quarantined-pii-sentinel";

    let physical_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        semantic_field(
            Field::new("note", DataType::Int32, true),
            r#"cdf.pii@1(class="note")"#,
        ),
    ]));
    let physical_hash = cdf_kernel::canonical_arrow_schema_hash(physical_schema.as_ref()).unwrap();
    let reconciliation = reconcile_schema(
        physical_schema.as_ref(),
        physical_schema.as_ref(),
        &ContractPolicy::default().types,
    )
    .unwrap();
    let serialized_coercion = serde_json::to_string(&reconciliation.plan).unwrap();
    let schema = Arc::new(reconciliation.schema);
    let id_field = schema.field(0).as_ref().clone();
    let note_field = schema.field(1).as_ref().clone();

    let captured_record = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef,
            Arc::new(Int32Array::from(vec![None])) as ArrayRef,
        ],
    )
    .unwrap();
    let mut captured_batch = Batch::from_record_batch(
        BatchId::new("batch-residual-captured").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        cdf_kernel::canonical_arrow_schema_hash(captured_record.schema().as_ref()).unwrap(),
        captured_record,
    )
    .unwrap();
    captured_batch.header.observed_schema_hash = physical_hash.clone();
    captured_batch.header.schema_coercion_plan = Some(serialized_coercion.clone());
    captured_batch
        .header
        .mark_materialized_output(physical_schema.as_ref())
        .unwrap();
    captured_batch.header.source_position = Some(terminal_file_position());
    captured_batch.header.push_residual_candidate(
        PreContractResidualCandidate::new(
            10,
            0,
            vec!["note".to_owned()],
            semantic_field(
                Field::new("note", DataType::Utf8, true),
                r#"cdf.pii@1(class="note")"#,
            ),
            Some(note_field),
            Arc::new(StringArray::from(vec![CAPTURE_SENTINEL])) as ArrayRef,
            0,
        )
        .unwrap(),
    );
    captured_batch.header.extend_physical_reconciliations([
        cdf_kernel::PreContractPhysicalReconciliation::new(
            vec!["id".to_owned()],
            cdf_kernel::with_physical_type(Field::new("id", DataType::Int32, true), "bson:int32"),
            id_field.clone(),
            Arc::new(Int32Array::from(vec![1_i32])) as ArrayRef,
            vec![0],
        )
        .unwrap(),
    ]);

    let quarantined_record = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![None])) as ArrayRef,
            Arc::new(Int32Array::from(vec![Some(30)])) as ArrayRef,
        ],
    )
    .unwrap();
    let mut quarantined_batch = Batch::from_record_batch(
        BatchId::new("batch-residual-quarantined").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-1").unwrap(),
        cdf_kernel::canonical_arrow_schema_hash(quarantined_record.schema().as_ref()).unwrap(),
        quarantined_record,
    )
    .unwrap();
    quarantined_batch.header.observed_schema_hash = physical_hash.clone();
    quarantined_batch.header.schema_coercion_plan = Some(serialized_coercion);
    quarantined_batch
        .header
        .mark_materialized_output(physical_schema.as_ref())
        .unwrap();
    quarantined_batch.header.source_position = Some(terminal_file_position());
    quarantined_batch.header.push_residual_candidate(
        PreContractResidualCandidate::new(
            20,
            0,
            vec!["id".to_owned()],
            Field::new("id", DataType::Utf8, true),
            Some(id_field),
            Arc::new(StringArray::from(vec!["bad-control-id"])) as ArrayRef,
            0,
        )
        .unwrap(),
    );
    quarantined_batch.header.push_residual_candidate(
        PreContractResidualCandidate::new(
            20,
            0,
            vec!["new_secret".to_owned()],
            semantic_field(
                Field::new("new_secret", DataType::Utf8, true),
                r#"cdf.pii@1(class="secret")"#,
            ),
            None,
            Arc::new(StringArray::from(vec![QUARANTINE_SENTINEL])) as ArrayRef,
            0,
        )
        .unwrap(),
    );

    let effective_schema_hash = SchemaHash::new("effective-snapshot-v1").unwrap();
    let evidence = bound_effective_schema_evidence(
        effective_schema_hash.clone(),
        "manifest-residual-mixed",
        ".cdf/schemas/orders@manifest-residual-mixed.discovery.json",
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
    .unwrap()
    .with_discovery_executor_budget(
        DiscoveryExecutorBudgetEvidence::new(64, 1_000, 128, 2).unwrap(),
    )
    .unwrap();
    let resource = MockResource::tier_b(vec![captured_batch, quarantined_batch])
        .with_effective_schema_runtime(schema.clone(), runtime)
        .without_control_keys();
    let mut input = plan_input_for_schema(
        schema,
        vec![],
        Some(vec!["id".to_owned(), "note".to_owned()]),
        None,
        ExecutionExtent::bounded(),
    );
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Range {
        column: "id".to_owned(),
        min: Some("0".to_owned()),
        max: None,
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_b(&resource, input).unwrap();
    let planned_schema = plan.output_arrow_schema().unwrap();
    assert_eq!(
        plan.schema_authority().effective_schema_hash,
        effective_schema_hash
    );

    let temp = TempDir::new().unwrap();
    let plain = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();
    let managed_temp = TempDir::new().unwrap();
    let (_, services) =
        StandaloneExecutionHost::default_services_with_spill(64 * 1024 * 1024, 64 * 1024 * 1024)
            .unwrap();
    let residual_scratch = managed_temp.path().join(".residual-decisions-spill");
    let hook_order = Arc::new(Mutex::new(Vec::new()));
    let pre_finalize_services = services.clone();
    let pre_finalize_scratch = residual_scratch.clone();
    let pre_finalize_order = Arc::clone(&hook_order);
    let pre_finalize = move |_builder: &cdf_package::PackageBuilder,
                             _draft: EnginePackageDraft<'_>| {
        assert!(pre_finalize_services.memory().snapshot().current_bytes >= 8 * 1024 * 1024);
        assert!(pre_finalize_services.spill().snapshot().current_bytes > 0);
        assert!(pre_finalize_scratch.is_dir());
        pre_finalize_order.lock().unwrap().push("pre_finalize");
        Ok(())
    };
    let mut durable_segment =
        |_entry: &SegmentEntry, _payload: DurableSegmentPayload| -> Result<()> { Ok(()) };
    let stream_finalize_services = services.clone();
    let stream_finalize_scratch = residual_scratch.clone();
    let stream_finalize_order = Arc::clone(&hook_order);
    let mut stream_finalize = move || {
        assert!(stream_finalize_services.memory().snapshot().current_bytes >= 8 * 1024 * 1024);
        assert!(stream_finalize_services.spill().snapshot().current_bytes > 0);
        assert!(stream_finalize_scratch.is_dir());
        stream_finalize_order
            .lock()
            .unwrap()
            .push("stream_finalize");
        Ok(())
    };
    let managed = block_on(execute_to_package_with_streaming_hooks(
        &plan,
        &resource,
        managed_temp.path(),
        &pre_finalize,
        &mut durable_segment,
        &mut stream_finalize,
        EngineExecutionConfig::default().with_execution_services(services.clone()),
    ))
    .unwrap();
    assert_eq!(managed.output.manifest.identity, plain.manifest.identity);
    assert_eq!(
        managed.output.manifest.package_hash,
        plain.manifest.package_hash
    );
    assert!(services.spill().snapshot().peak_bytes > 0);
    assert_eq!(services.spill().snapshot().current_bytes, 0);
    assert_eq!(services.memory().snapshot().current_bytes, 0);
    assert!(!residual_scratch.exists());
    assert_eq!(
        *hook_order.lock().unwrap(),
        ["stream_finalize", "pre_finalize"]
    );
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    reader.verify().unwrap();
    assert_eq!(reader.runtime_arrow_schema().unwrap(), planned_schema);

    let admission: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temp.path().join("schema/admission-evidence.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        admission["baseline_schema_hash"],
        plan.schema_authority().baseline_schema_hash.as_str()
    );
    assert_eq!(
        admission["effective_schema_hash"],
        effective_schema_hash.as_str()
    );
    let decisions = admission["residual_decisions"].as_array().unwrap();
    assert_eq!(decisions.len(), 3);
    assert!(decisions.iter().all(|decision| decision["version"] == 1));
    let captured = decisions
        .iter()
        .filter(|decision| decision["batch_id"] == "batch-residual-captured")
        .collect::<Vec<_>>();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0]["observation_id"], "input-0");
    assert_eq!(captured[0]["verdict"], "captured");
    assert_eq!(captured[0]["source_path"], serde_json::json!(["note"]));
    let quarantined = decisions
        .iter()
        .filter(|decision| decision["batch_id"] == "batch-residual-quarantined")
        .collect::<Vec<_>>();
    assert_eq!(quarantined.len(), 2);
    assert!(
        quarantined
            .iter()
            .all(|decision| decision["observation_id"] == "unobserved-part-1")
    );
    assert!(
        quarantined
            .iter()
            .all(|decision| decision["verdict"] == "quarantined")
    );
    let physical: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temp.path().join("schema/physical-reconciliations.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(physical["version"], 1);
    assert_eq!(physical["reconciliations"].as_array().unwrap().len(), 1);
    assert_eq!(
        physical["reconciliations"][0]["observed_field"]["metadata"]
            [cdf_kernel::PHYSICAL_TYPE_METADATA_KEY],
        "bson:int32"
    );
    assert_package_tree_excludes(temp.path(), &[CAPTURE_SENTINEL, QUARANTINE_SENTINEL]);
}

#[test]
fn execution_rejects_schema_authority_and_zero_row_output_schema_tampering() {
    let resource =
        MockResource::tier_a(Vec::new()).with_write_disposition(WriteDisposition::Append);
    let input = plan_input(vec![], None, None, ExecutionExtent::bounded());
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();

    let mut authority_tamper = plan.clone();
    authority_tamper.schema_authority.effective_schema_hash =
        SchemaHash::new("sha256:forged-authority").unwrap();
    let temp = TempDir::new().unwrap();
    let error = block_on(execute_to_package(
        &authority_tamper,
        &resource,
        temp.path(),
    ))
    .unwrap_err();
    assert!(error.to_string().contains("schema authority"));

    let mut output_tamper = plan;
    let output = &mut output_tamper.output_schema;
    output.fields.pop();
    let forged_schema = Schema::new(
        output
            .fields
            .iter()
            .map(|field| field.to_arrow().unwrap())
            .collect::<Vec<_>>(),
    );
    output.arrow_schema_hash = cdf_kernel::canonical_arrow_schema_hash(&forged_schema).unwrap();
    let temp = TempDir::new().unwrap();
    let error = block_on(execute_to_package(&output_tamper, &resource, temp.path())).unwrap_err();
    assert!(error.to_string().contains("compiled output schema"));
}

fn assert_package_tree_excludes(root: &std::path::Path, sentinels: &[&str]) {
    for entry in std::fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            assert_package_tree_excludes(&path, sentinels);
            continue;
        }
        let bytes = std::fs::read(&path).unwrap();
        for sentinel in sentinels {
            assert!(
                !bytes
                    .windows(sentinel.len())
                    .any(|window| window == sentinel.as_bytes()),
                "package artifact {} contains raw sentinel {sentinel:?}",
                path.display()
            );
        }
    }
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

fn output_name_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("customer_name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
    ]))
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
