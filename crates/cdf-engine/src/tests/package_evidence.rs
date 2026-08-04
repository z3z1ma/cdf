use super::support::{
    Arc, ArrayRef, AtomicU64, Attributes, BTreeMap, BTreeSet, Batch, BatchHeader, BatchId,
    BooleanArray, CDF_VARIANT_SEMANTIC, CanonicalSegmentationPolicy, ContractPolicy,
    DEDUP_SUMMARY_FILE, DataType, DedupKeep, DeduplicationSpec, DrainTermination,
    EXECUTION_EXTENT_VERSION, EngineExecutionConfig, EnginePackageDraft, EnginePreviewLimits,
    Event, ExecutionExtent, Expression, Field, FieldCoercionDecision, FileManifest, FilePosition,
    HashMap, Id, Int32Array, Int32Builder, Int32Type, Int64Array, ListArray, MapBuilder, Metadata,
    MockResource, Mutex, NestedDataPolicy, ObservedSchema, OperatorNode, Ordering, PackageStatus,
    PartitionAttestation, PartitionId, Planner, PreContractObservedValue,
    PreContractQuarantineFact, PreContractResidualCandidate, QuarantineObservedValue,
    RESIDUAL_ENCODING_METADATA_KEY, RESIDUAL_ENCODING_NAME, Record, RecordBatch, ResourceId,
    RowRule, RunId, RunPhase, RunPhaseStatus, Schema, SchemaEvolutionMode, SchemaHash, SchemaRef,
    SegmentEntry, SourcePosition, StandaloneExecutionHost, StringArray, StringBuilder,
    StringDictionaryBuilder, StructArray, Subscriber, TempDir, TimeUnit, TimestampMillisecondArray,
    TracingField, TrustLevel, VARIANT_COLUMN_NAME, VerdictAction, Visit, WriteDisposition,
    assert_explain_carries_required_fields, assert_honest_cdf_native_operator_metadata,
    batch_for_partition, batch_for_partition_with_schema, batch_strings, block_on,
    coercion_decision, collect_quarantine_records, compile_resource_validation_program,
    compile_validation_program, execute_to_package, execute_to_package_with_run_id,
    execute_to_package_with_segment_positions_and_pre_finalize, fmt, incompatible_sample_schema,
    plan_input, plan_input_for_schema, preview_resource, read_package_segment, reconcile_schema,
    rename_column_program_output, sample_batches, sample_schema, sample_stream_epoch_policy,
    semantic_field, stream_admission_coercion, terminal_effective_schema_runtime,
    terminal_file_position,
};
use super::support::{Array, ResourceStream};

#[test]
fn zero_limit_finalizes_an_empty_package_without_source_contact() {
    let resource = MockResource::tier_b(sample_batches());
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, Some(0), ExecutionExtent::bounded()),
        )
        .unwrap();
    let temp = TempDir::new().unwrap();

    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
    assert_eq!(output.profile.output_rows, 0);
    assert!(output.identity_segments().is_empty());
}

#[test]
fn preview_terminal_quarantine_uses_run_attestation_without_opening_payloads() {
    let effective_schema = sample_schema();
    let physical_schema = incompatible_sample_schema();
    let physical_hash = cdf_kernel::canonical_arrow_schema_hash(physical_schema.as_ref()).unwrap();
    let runtime = terminal_effective_schema_runtime(physical_schema, physical_hash.clone());
    let resource = MockResource::tier_b(sample_batches())
        .with_effective_schema_runtime(effective_schema, runtime)
        .with_attestation(PartitionAttestation::new(
            terminal_file_position(),
            Some(physical_hash),
        ));
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

    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
    assert_eq!(resource.attest_count.load(Ordering::SeqCst), 2);
    assert_eq!(preview.planned_partition_count, 2);
    assert_eq!(preview.payload_opened_partition_count, 0);
    assert_eq!(preview.attested_partition_count, 2);
    assert_eq!(preview.terminal_quarantine_count, 2);
    assert_eq!(preview.row_count, 0);
}

#[test]
fn explain_and_operator_chain_carry_contract_package_details() {
    let resource = MockResource::tier_a(sample_batches());
    let input = plan_input(
        vec!["active = true"],
        Some(vec!["id".to_owned(), "name".to_owned()]),
        Some(2),
        ExecutionExtent::Drain {
            version: EXECUTION_EXTENT_VERSION,
            policy: sample_stream_epoch_policy(),
            termination: DrainTermination::Records { count: 10 },
        },
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
            OperatorNode::PackageSink { package_id, segmentation }
                if package_id == "pkg-engine-test"
                    && segmentation == &CanonicalSegmentationPolicy::performance_default()
        )
    }));
}

#[test]
fn package_artifacts_record_schema_coercion_evidence_and_physical_type_metadata() {
    let resource = MockResource::tier_a(vec![parquet_reconciled_batch()]);
    let input = plan_input_for_schema(
        parquet_reconciled_schema(),
        vec![],
        None,
        None,
        ExecutionExtent::bounded(),
    );
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 2);

    let plan_evidence = stream_admission_coercion(temp.path());
    let widened = coercion_decision(&plan_evidence, "id");
    assert_eq!(widened.decision, FieldCoercionDecision::Widened);
    assert_eq!(widened.observed_type.as_deref(), Some("Int32"));
    assert_eq!(widened.constraint_type.as_deref(), Some("Int64"));

    let preserved = coercion_decision(&plan_evidence, "name");
    assert_eq!(preserved.decision, FieldCoercionDecision::Preserved);
    assert_eq!(preserved.observed_type.as_deref(), Some("Utf8"));
    assert_eq!(preserved.constraint_type.as_deref(), Some("Utf8"));

    assert!(!temp.path().join("schema/coercion-plan.json").exists());

    let output_schema: serde_json::Value =
        serde_json::from_slice(&std::fs::read(temp.path().join("schema/output.json")).unwrap())
            .unwrap();
    assert_eq!(
        output_schema["fields"][0]["metadata"]["cdf:physical_type"],
        "Int32"
    );
    assert_eq!(
        output_schema["fields"][0]["metadata"]["cdf:source_name"],
        "id"
    );
    assert_eq!(
        output_schema["fields"][1]["metadata"]["cdf:source_name"],
        "name"
    );

    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = read_package_segment(&reader, &output.identity_segments()[0].segment_id);
    assert_eq!(
        batches[0]
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[1, 2]
    );
}

#[test]
fn package_artifacts_preserve_exact_embedded_lossy_and_extra_reconciliation_decisions() {
    let observed = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("source_only", DataType::Utf8, true),
    ]);
    let constraint = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
    let mut type_policy = ContractPolicy::default().types;
    type_policy.allow_lossy_mapping = true;
    let reconciliation = reconcile_schema(&observed, &constraint, &type_policy).unwrap();
    let serialized_plan = serde_json::to_string(&reconciliation.plan).unwrap();
    let schema = Arc::new(reconciliation.schema);
    let record_batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();
    let mut batch = Batch::from_record_batch(
        BatchId::new("batch-json-reconciled").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        SchemaHash::new("schema-json-reconciled").unwrap(),
        record_batch,
    )
    .unwrap();
    batch.header.schema_coercion_plan = Some(serialized_plan);
    batch.header.mark_materialized_output(&observed).unwrap();
    batch.header.push_residual_candidate(
        PreContractResidualCandidate::new(
            1,
            1,
            vec!["source_only".to_owned()],
            Field::new("source_only", DataType::Utf8, true),
            None,
            Arc::new(StringArray::from(vec!["present-on-one-row"])) as ArrayRef,
            0,
        )
        .unwrap(),
    );
    let incomplete_residual_evidence = batch.clone();
    batch.header.mark_materialized_residuals_complete();
    let resource = MockResource::tier_a(vec![batch]).with_type_policy_allowances(
        cdf_kernel::TypePolicyAllowances {
            coerce_types: false,
            allow_lossy_mapping: true,
        },
    );
    let input = plan_input_for_schema(
        Arc::new(constraint),
        vec![],
        None,
        None,
        ExecutionExtent::bounded(),
    );
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();

    block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    let evidence = stream_admission_coercion(temp.path());
    assert_eq!(
        coercion_decision(&evidence, "id").decision,
        FieldCoercionDecision::LossyAllowed
    );
    assert_eq!(
        coercion_decision(&evidence, "source_only").decision,
        FieldCoercionDecision::Extra
    );

    let incomplete_resource = MockResource::tier_a(vec![incomplete_residual_evidence])
        .with_type_policy_allowances(cdf_kernel::TypePolicyAllowances {
            coerce_types: false,
            allow_lossy_mapping: true,
        });
    let incomplete_plan = Planner::new()
        .plan_tier_a(
            &incomplete_resource,
            plan_input_for_schema(
                Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
                vec![],
                None,
                None,
                ExecutionExtent::bounded(),
            ),
        )
        .unwrap();
    let incomplete_package = TempDir::new().unwrap();
    let error = block_on(execute_to_package(
        &incomplete_plan,
        &incomplete_resource,
        incomplete_package.path(),
    ))
    .unwrap_err();
    assert!(
        error.to_string().contains("absent from its physical batch"),
        "{error}"
    );
}

#[test]
fn package_execution_rejects_source_carried_coercion_metadata_without_trusted_header() {
    let injected_plan = serde_json::json!({
        "fields": [{
            "source_name": "id",
            "observed_name": "id",
            "output_name": "id",
            "observed_type": "Int64",
            "constraint_type": "Int64",
            "decision": "preserved",
            "outcome": "pass",
            "reason": "observed type already satisfies the constraint"
        }]
    })
    .to_string();
    let injected_schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("id", DataType::Int64, false)],
        HashMap::from([("cdf:schema_coercion_plan".to_owned(), injected_plan)]),
    ));
    let record_batch =
        RecordBatch::try_new(injected_schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    let batch = Batch::from_record_batch(
        BatchId::new("batch-injected-coercion").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        SchemaHash::new("schema-injected-coercion").unwrap(),
        record_batch,
    )
    .unwrap();
    let resource = MockResource::tier_a(vec![batch]);
    let input = plan_input_for_schema(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![],
        None,
        None,
        ExecutionExtent::bounded(),
    );
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();

    let error = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap_err();
    assert!(error.to_string().contains("without trusted batch evidence"));
}

#[test]
fn package_execution_rejects_malformed_trusted_coercion_header() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let record_batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    let mut batch = Batch::from_record_batch(
        BatchId::new("batch-malformed-coercion").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        SchemaHash::new("schema-malformed-coercion").unwrap(),
        record_batch,
    )
    .unwrap();
    batch.header.schema_coercion_plan = Some("{not-json".to_owned());
    batch
        .header
        .mark_materialized_output(schema.as_ref())
        .unwrap();
    let resource = MockResource::tier_a(vec![batch]);
    let input = plan_input_for_schema(schema, vec![], None, None, ExecutionExtent::bounded());
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();

    let error = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap_err();
    assert!(error.to_string().contains("not a valid coercion plan"));
}

#[test]
fn package_execution_rejects_valid_header_only_coercion_injection() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let record_batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    let mut batch = Batch::from_record_batch(
        BatchId::new("batch-header-only-coercion").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        SchemaHash::new("schema-header-only-coercion").unwrap(),
        record_batch,
    )
    .unwrap();
    batch.header.schema_coercion_plan = Some(
        serde_json::json!({
            "fields": [{
                "source_name": "fabricated_extra",
                "observed_name": "fabricated_extra",
                "observed_type": "Utf8",
                "decision": "extra",
                "outcome": "admitted_as_variant",
                "reason": "observed field is outside the constraint projection"
            }]
        })
        .to_string(),
    );
    batch
        .header
        .mark_materialized_output(schema.as_ref())
        .unwrap();
    let resource = MockResource::tier_a(vec![batch]);
    let input = plan_input_for_schema(schema, vec![], None, None, ExecutionExtent::bounded());
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();

    let error = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("has no matching reserved Arrow schema metadata")
    );
}

#[test]
fn contract_exec_filters_quarantined_rows_before_normalize() {
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
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 2);
    assert_eq!(output.identity_segments().len(), 1);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = read_package_segment(&reader, &output.identity_segments()[0].segment_id);
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
fn contract_exec_writes_redacted_quarantine_artifact_and_keeps_accepted_rows() {
    let raw_pii = "pii-fixture-sensitive";
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        semantic_field(
            Field::new("name", DataType::Utf8, false),
            r#"cdf.pii@1(class="email")"#,
        ),
        Field::new("active", DataType::Boolean, false),
    ]));
    let mut batch = batch_for_partition_with_schema(
        "batch-pii",
        "part-0",
        schema.clone(),
        vec![1, 2],
        vec!["ok@example.test", raw_pii],
        vec![true, true],
    );
    batch.header.source_position = Some(SourcePosition::FileManifest(FileManifest {
        version: 1,
        files: vec![FilePosition {
            path: "/tmp/cdf/pii.ndjson".to_owned(),
            size_bytes: 64,
            source_generation: None,
            etag: None,
            object_version: None,
            sha256: Some(
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
            ),
        }],
    }));
    let resource = MockResource::tier_a(vec![batch]);
    let mut input = plan_input_for_schema(schema, vec![], None, None, ExecutionExtent::bounded());
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Regex {
        column: "name".to_owned(),
        pattern: r"^[^@]+@example\.test$".to_owned(),
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 1);
    assert_eq!(output.identity_segments().len(), 1);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = read_package_segment(&reader, &output.identity_segments()[0].segment_id);
    let accepted = batches[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(accepted.len(), 1);
    assert_eq!(accepted.value(0), "ok@example.test");

    let quarantine = collect_quarantine_records(&reader);
    assert_eq!(quarantine.len(), 1);
    assert_eq!(quarantine[0].source_row_ordinal, 1);
    assert_eq!(quarantine[0].error_code, "regex_violation");
    assert!(matches!(
        quarantine[0].source_position,
        Some(SourcePosition::FileManifest(_))
    ));
    let QuarantineObservedValue::Hashed { algorithm, value } =
        &quarantine[0].observed_value_redacted
    else {
        panic!("pii semantic field must be hash-redacted");
    };
    assert_eq!(algorithm, "sha256");
    assert_eq!(
        value,
        "sha256:0a08d503e0f6794940fd8e6a1f547999622742616551894946ba6dc0489cf184"
    );

    let files = package_identity_file_paths(&reader);
    assert!(files.contains("stats/verdict-summary.json"));
    assert!(files.contains("stats/quarantine-summary.json"));

    let verdict_summary: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temp.path().join("stats/verdict-summary.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(verdict_summary["input_rows"], 2);
    assert_eq!(verdict_summary["accepted_rows"], 1);
    assert_eq!(verdict_summary["quarantined_rows"], 1);
    assert_eq!(verdict_summary["violation_count"], 1);
    assert_eq!(verdict_summary["quarantine_candidate_count"], 1);
    assert!(
        verdict_summary["rule_summaries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|summary| summary
                == &serde_json::json!({
                    "rule_id": "row-rule-0000-regex",
                    "error_code": "regex_violation",
                    "checked_rows": 2,
                    "violation_count": 1
                }))
    );

    let quarantine_summary: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temp.path().join("stats/quarantine-summary.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(quarantine_summary["quarantined_rows"], 1);
    assert_eq!(quarantine_summary["quarantine_candidate_count"], 1);
    assert_eq!(quarantine_summary["artifact_count"], 1);
    assert_eq!(
        quarantine_summary["artifacts"],
        serde_json::json!(["quarantine/part-000001.parquet"])
    );

    let quarantine_path = temp.path().join("quarantine/part-000001.parquet");
    let artifact = std::fs::read(quarantine_path).unwrap();
    assert!(!String::from_utf8_lossy(&artifact).contains(raw_pii));
    assert!(package_identity_file_paths(&reader).contains("quarantine/part-000001.parquet"));
}

#[test]
fn contract_quarantine_preserves_source_ordinal_after_transform_filter() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
    ]));
    let batch = batch_for_partition_with_schema(
        "batch-transform-quarantine",
        "part-0",
        schema.clone(),
        vec![1, 2],
        vec!["ignored", "bad"],
        vec![true, true],
    );
    let resource = MockResource::tier_a(vec![batch]);
    let mut input = plan_input_for_schema(schema, vec![], None, None, ExecutionExtent::bounded());
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.transforms = vec![cdf_contract::TransformDescription::Filter {
        expression: Expression::parse_comparison("id >= 2").unwrap(),
    }];
    policy.rows.rules = vec![RowRule::Regex {
        column: "name".to_owned(),
        pattern: "^ok$".to_owned(),
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();
    let quarantine =
        collect_quarantine_records(&cdf_package::PackageReader::open(temp.path()).unwrap());
    assert_eq!(quarantine.len(), 1);
    assert_eq!(quarantine[0].error_code, "regex_violation");
    assert_eq!(quarantine[0].source_row_ordinal, 1);
}

#[test]
fn contract_quarantine_preserves_source_ordinal_after_residual_quarantine() {
    let id_field = Field::new("id", DataType::Int32, true);
    let schema = Arc::new(Schema::new(vec![
        id_field.clone(),
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
    ]));
    let record_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![None, Some(2)])) as ArrayRef,
            Arc::new(StringArray::from(vec!["ignored", "bad"])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![true, true])) as ArrayRef,
        ],
    )
    .unwrap();
    let mut batch = Batch::from_record_batch(
        BatchId::new("batch-residual-contract-quarantine").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        cdf_kernel::canonical_arrow_schema_hash(record_batch.schema().as_ref()).unwrap(),
        record_batch,
    )
    .unwrap();
    batch.header.push_residual_candidate(
        PreContractResidualCandidate::new(
            0,
            0,
            vec!["id".to_owned()],
            Field::new("id", DataType::Utf8, true),
            Some(id_field),
            Arc::new(StringArray::from(vec!["bad-id"])) as ArrayRef,
            0,
        )
        .unwrap(),
    );
    let resource = MockResource::tier_a(vec![batch]);
    let mut input = plan_input_for_schema(schema, vec![], None, None, ExecutionExtent::bounded());
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.schema.mode = SchemaEvolutionMode::Evolve;
    policy.rows.rules = vec![RowRule::Regex {
        column: "name".to_owned(),
        pattern: "^ok$".to_owned(),
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();
    let quarantine =
        collect_quarantine_records(&cdf_package::PackageReader::open(temp.path()).unwrap());
    let contract = quarantine
        .iter()
        .find(|record| record.error_code == "regex_violation")
        .unwrap();
    assert_eq!(contract.source_row_ordinal, 1);
}

#[test]
fn source_decode_quarantine_facts_fold_into_package_artifacts() {
    let mut batch = batch_for_partition(
        "batch-source-drift",
        "part-0",
        vec![3],
        vec!["three"],
        vec![true],
    );
    batch.header.source_position = Some(SourcePosition::FileManifest(FileManifest {
        version: 1,
        files: vec![FilePosition {
            path: "/tmp/cdf/source-drift.ndjson".to_owned(),
            size_bytes: 96,
            source_generation: None,
            etag: None,
            object_version: None,
            sha256: Some(
                "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    .to_owned(),
            ),
        }],
    }));
    batch.header.pre_contract_quarantine = vec![PreContractQuarantineFact {
        source_row_ordinal: 1,
        rule_id: "source-decode:event_type:type-mismatch".to_owned(),
        error_code: "source_type_mismatch".to_owned(),
        source_position: batch.header.source_position.clone(),
        observed_value_redacted: PreContractObservedValue::Preserved {
            value: "42".to_owned(),
        },
    }];
    let resource = MockResource::tier_a(vec![batch]);
    let input = plan_input(vec![], None, None, ExecutionExtent::bounded());
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 1);
    assert_eq!(output.identity_segments().len(), 1);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let accepted = read_package_segment(&reader, &output.identity_segments()[0].segment_id);
    assert_eq!(batch_i32s(&accepted[0], "id"), vec![3]);
    assert_eq!(batch_strings(&accepted, "name"), vec!["three"]);
    let quarantine = collect_quarantine_records(&reader);
    assert_eq!(quarantine.len(), 1);
    assert_eq!(quarantine[0].source_row_ordinal, 1);
    assert_eq!(
        quarantine[0].rule_id,
        "source-decode:event_type:type-mismatch"
    );
    assert_eq!(quarantine[0].error_code, "source_type_mismatch");
    assert!(matches!(
        quarantine[0].source_position,
        Some(SourcePosition::FileManifest(_))
    ));
    assert_eq!(
        quarantine[0].observed_value_redacted,
        QuarantineObservedValue::Preserved {
            value: "42".to_owned()
        }
    );

    let verdict_summary: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temp.path().join("stats/verdict-summary.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(verdict_summary["input_rows"], 2);
    assert_eq!(verdict_summary["accepted_rows"], 1);
    assert_eq!(verdict_summary["quarantined_rows"], 1);
    assert_eq!(verdict_summary["violation_count"], 1);
    assert_eq!(verdict_summary["quarantine_candidate_count"], 1);
    assert!(
        verdict_summary["rule_summaries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|summary| summary
                == &serde_json::json!({
                    "rule_id": "source-decode:event_type:type-mismatch",
                    "error_code": "source_type_mismatch",
                    "checked_rows": 1,
                    "violation_count": 1
                }))
    );

    let quarantine_summary: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temp.path().join("stats/quarantine-summary.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(quarantine_summary["quarantined_rows"], 1);
    assert_eq!(quarantine_summary["quarantine_candidate_count"], 1);
    assert_eq!(
        quarantine_summary["artifacts"],
        serde_json::json!(["quarantine/part-000001.parquet"])
    );
    reader.verify().unwrap();
}

#[test]
fn variant_capture_materializes_nested_values_and_contract_evolution_evidence() {
    let resource = MockResource::tier_a(vec![nested_variant_batch()]);
    let mut input = plan_input_for_schema(
        resource.schema(),
        vec![],
        None,
        None,
        ExecutionExtent::bounded(),
    );
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.normalization.nested = NestedDataPolicy::VariantCapture(Default::default());
    policy.rows.rules = vec![RowRule::Regex {
        column: "email".to_owned(),
        pattern: r"^[^@]+@example\.test$".to_owned(),
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 1);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = read_package_segment(&reader, &output.identity_segments()[0].segment_id);
    let batch = &batches[0];
    assert_eq!(batch.schema().fields().len(), 4);
    assert!(batch.schema().field_with_name("payload").is_err());
    assert!(batch.schema().field_with_name("tags").is_err());
    assert!(batch.schema().field_with_name("attributes").is_err());
    let batch_schema = batch.schema();
    let variant_field = batch_schema.field_with_name(VARIANT_COLUMN_NAME).unwrap();
    assert_eq!(
        cdf_kernel::semantic(variant_field),
        Some(CDF_VARIANT_SEMANTIC)
    );
    assert_eq!(
        variant_field
            .metadata()
            .get(RESIDUAL_ENCODING_METADATA_KEY)
            .map(String::as_str),
        Some(RESIDUAL_ENCODING_NAME)
    );
    let variants = batch
        .column_by_name(VARIANT_COLUMN_NAME)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        variants.value(0),
        r#"{"v":1,"fields":{"/attributes":{"arrow_type":{"kind":"map","field":{"name":"entries","data_type":{"kind":"struct","fields":[{"name":"keys","data_type":{"kind":"utf8","offset_width":32},"nullable":false,"metadata":{}},{"name":"values","data_type":{"kind":"int","signed":true,"bits":32},"nullable":true,"metadata":{}}]},"nullable":false,"metadata":{}},"sorted":false},"encoding":"nested","value":[{"key":"tier","value":"1"}]},"/payload":{"arrow_type":{"kind":"struct","fields":[{"name":"kind","data_type":{"kind":"utf8","offset_width":32},"nullable":false,"metadata":{}},{"name":"count","data_type":{"kind":"int","signed":true,"bits":32},"nullable":false,"metadata":{}}]},"encoding":"nested","value":{"count":"7","kind":"alpha"}},"/tags":{"arrow_type":{"kind":"list","field":{"name":"item","data_type":{"kind":"int","signed":true,"bits":32},"nullable":true,"metadata":{}},"offset_width":32,"view":false},"encoding":"nested","value":["1","2"]}}}"#
    );
    let decoded = cdf_contract::decode_residual_json_v1(variants.value(0).as_bytes()).unwrap();
    assert_eq!(
        decoded
            .iter()
            .map(|field| field.path.as_str())
            .collect::<Vec<_>>(),
        vec!["/attributes", "/payload", "/tags"]
    );
    let source_schema = resource.schema();
    assert_eq!(
        decoded[0].array.data_type(),
        source_schema
            .field_with_name("attributes")
            .unwrap()
            .data_type()
    );
    assert_eq!(
        decoded[1].array.data_type(),
        source_schema
            .field_with_name("payload")
            .unwrap()
            .data_type()
    );
    assert_eq!(
        decoded[2].array.data_type(),
        source_schema.field_with_name("tags").unwrap().data_type()
    );

    let output_schema: serde_json::Value =
        serde_json::from_slice(&std::fs::read(temp.path().join("schema/output.json")).unwrap())
            .unwrap();
    assert_eq!(
        output_schema["fields"][2],
        serde_json::json!({
            "name": VARIANT_COLUMN_NAME,
            "data_type": "Utf8",
            "nullable": true,
            "semantic": CDF_VARIANT_SEMANTIC,
            "metadata": {
                (RESIDUAL_ENCODING_METADATA_KEY): RESIDUAL_ENCODING_NAME
            }
        })
    );
    let evolution_path = temp.path().join("schema/contract-evolution.json");
    let evolution_bytes = std::fs::read(&evolution_path).unwrap();
    let evolution: serde_json::Value = serde_json::from_slice(&evolution_bytes).unwrap();
    assert_eq!(evolution["implicit_promotion_count"], 0);
    assert_eq!(evolution["promotion_events"], serde_json::json!([]));
    assert_eq!(
        evolution["variant_capture"],
        serde_json::json!([
            {
                "source_field": "attributes",
                "variant_column": VARIANT_COLUMN_NAME,
                "semantic": CDF_VARIANT_SEMANTIC
            },
            {
                "source_field": "payload",
                "variant_column": VARIANT_COLUMN_NAME,
                "semantic": CDF_VARIANT_SEMANTIC
            },
            {
                "source_field": "tags",
                "variant_column": VARIANT_COLUMN_NAME,
                "semantic": CDF_VARIANT_SEMANTIC
            }
        ])
    );
    assert_eq!(
        evolution_bytes,
        cdf_package::canonical_json_bytes(&evolution).unwrap()
    );
    assert!(package_identity_file_paths(&reader).contains("schema/contract-evolution.json"));
    assert_eq!(reader.manifest().identity.segment_count, 1);

    let quarantine = collect_quarantine_records(&reader);
    assert_eq!(quarantine.len(), 1);
    let QuarantineObservedValue::Hashed { value, .. } = &quarantine[0].observed_value_redacted
    else {
        panic!("pii variant interaction must keep quarantine observed value hashed");
    };
    assert!(value.starts_with("sha256:"));
    let quarantine_artifact =
        std::fs::read(temp.path().join("quarantine/part-000001.parquet")).unwrap();
    assert!(!String::from_utf8_lossy(&quarantine_artifact).contains("raw-secret"));
}

#[test]
fn residual_contract_exec_captures_safe_values_redacts_pii_and_quarantines_controls() {
    let id_field = Field::new("id", DataType::Int32, true);
    let note_field = semantic_field(
        Field::new("note", DataType::Int32, true),
        r#"cdf.pii@1(class="note")"#,
    );
    let schema = Arc::new(Schema::new(vec![id_field.clone(), note_field.clone()]));
    let record_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![Some(1), Some(2), None])) as ArrayRef,
            Arc::new(Int32Array::from(vec![Some(10), None, Some(30)])) as ArrayRef,
        ],
    )
    .unwrap();
    let mut batch = Batch::from_record_batch(
        BatchId::new("batch-residual").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        cdf_kernel::canonical_arrow_schema_hash(record_batch.schema().as_ref()).unwrap(),
        record_batch,
    )
    .unwrap();
    let note_values = Arc::new(StringArray::from(vec!["alice@example.test"])) as ArrayRef;
    batch.header.push_residual_candidate(
        PreContractResidualCandidate::new(
            1,
            1,
            vec!["note".to_owned()],
            semantic_field(
                Field::new("note", DataType::Utf8, true),
                r#"cdf.pii@1(class="note")"#,
            ),
            Some(note_field),
            note_values,
            0,
        )
        .unwrap(),
    );
    let unknown_values = Arc::new(StringArray::from(vec!["top-secret"])) as ArrayRef;
    batch.header.push_residual_candidate(
        PreContractResidualCandidate::new(
            1,
            1,
            vec!["new_secret".to_owned()],
            semantic_field(
                Field::new("new_secret", DataType::Utf8, true),
                r#"cdf.pii@1(class="secret")"#,
            ),
            None,
            unknown_values,
            0,
        )
        .unwrap(),
    );
    let id_values = Arc::new(StringArray::from(vec!["bad-id"])) as ArrayRef;
    batch.header.push_residual_candidate(
        PreContractResidualCandidate::new(
            2,
            2,
            vec!["id".to_owned()],
            Field::new("id", DataType::Utf8, true),
            Some(id_field),
            id_values,
            0,
        )
        .unwrap(),
    );

    let resource =
        MockResource::tier_a(vec![batch]).with_write_disposition(WriteDisposition::Append);
    let mut input = plan_input_for_schema(
        schema,
        vec![],
        Some(vec!["id".to_owned(), "note".to_owned()]),
        None,
        ExecutionExtent::bounded(),
    );
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.schema.mode = SchemaEvolutionMode::Evolve;
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let planned_schema = plan.output_arrow_schema().unwrap();
    assert_eq!(planned_schema.fields().len(), 3);
    assert_eq!(planned_schema.field(2).name(), VARIANT_COLUMN_NAME);
    assert_ne!(
        plan.schema_authority().effective_schema_hash,
        plan.output_schema.arrow_schema_hash
    );

    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = read_package_segment(&reader, &output.identity_segments()[0].segment_id);
    let output = &batches[0];
    assert_eq!(output.num_rows(), 2);
    let variants = output
        .column_by_name(VARIANT_COLUMN_NAME)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(variants.is_null(0));
    assert!(variants.value(1).contains("sha256:"));
    assert!(!variants.value(1).contains("alice@example.test"));
    assert!(!variants.value(1).contains("top-secret"));

    let quarantine = collect_quarantine_records(&reader);
    assert_eq!(quarantine.len(), 1);
    assert_eq!(quarantine[0].error_code, "cdf.residual_control_critical");
    let evolution_bytes =
        std::fs::read(temp.path().join("schema/contract-evolution.json")).unwrap();
    let evolution_text = String::from_utf8(evolution_bytes.clone()).unwrap();
    assert!(!evolution_text.contains("alice@example.test"));
    assert!(!evolution_text.contains("top-secret"));
    let evolution: serde_json::Value = serde_json::from_slice(&evolution_bytes).unwrap();
    assert_eq!(evolution["version"], 1);
    assert_eq!(evolution["residual_decisions"].as_array().unwrap().len(), 3);
    reader.verify().unwrap();
    assert_eq!(reader.runtime_arrow_schema().unwrap(), planned_schema);
}

#[test]
fn residual_unsupported_encoding_becomes_named_quarantine() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
    let record_batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef],
    )
    .unwrap();
    let mut batch = Batch::from_record_batch(
        BatchId::new("batch-unsupported-residual").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        cdf_kernel::canonical_arrow_schema_hash(record_batch.schema().as_ref()).unwrap(),
        record_batch,
    )
    .unwrap();
    let mut dictionary = StringDictionaryBuilder::<Int32Type>::new();
    dictionary.append("value").unwrap();
    let dictionary = Arc::new(dictionary.finish()) as ArrayRef;
    batch.header.push_residual_candidate(
        PreContractResidualCandidate::new(
            0,
            0,
            vec!["unsupported".to_owned()],
            Field::new("unsupported", dictionary.data_type().clone(), true),
            None,
            dictionary,
            0,
        )
        .unwrap(),
    );
    let resource =
        MockResource::tier_a(vec![batch]).with_write_disposition(WriteDisposition::Append);
    let mut input = plan_input_for_schema(schema, vec![], None, None, ExecutionExtent::bounded());
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.schema.mode = SchemaEvolutionMode::Evolve;
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let quarantine = collect_quarantine_records(&reader);
    assert_eq!(quarantine.len(), 1);
    assert_eq!(
        quarantine[0].error_code,
        cdf_contract::RESIDUAL_ENCODE_UNSUPPORTED_CODE
    );
    let evolution: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temp.path().join("schema/contract-evolution.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        evolution["residual_decisions"][0]["observed_physical_type"]["kind"],
        "dictionary"
    );
}

#[test]
fn reject_batch_contract_abort_prevents_packaged_manifest() {
    let resource = MockResource::tier_a(sample_batches());
    let mut input = plan_input(vec![], None, None, ExecutionExtent::bounded());
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
fn merge_dedup_keep_last_runs_after_contract_filtering_and_before_normalize() {
    let batches = vec![
        batch_for_partition(
            "batch-dedup-0",
            "part-0",
            vec![1, 2],
            vec!["one-first", "two"],
            vec![true, true],
        ),
        batch_for_partition(
            "batch-dedup-1",
            "part-0",
            vec![1, 3, 1],
            vec!["one-last", "three", "one-invalid"],
            vec![true, true, true],
        ),
    ];
    let resource = MockResource::tier_a(batches);
    let mut input = plan_input(vec![], None, None, ExecutionExtent::bounded());
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![
        RowRule::Domain {
            column: "name".to_owned(),
            allowed: vec![
                "one-first".to_owned(),
                "one-last".to_owned(),
                "two".to_owned(),
                "three".to_owned(),
            ],
        },
        RowRule::Dedup {
            keys: vec!["id".to_owned()],
            keep: DedupKeep::Last,
        },
    ];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(sample_schema().as_ref()),
    )
    .unwrap();
    rename_column_program_output(&mut input.validation_program, "name", "customer_name");
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 3);
    assert_eq!(output.identity_segments().len(), 1);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let segment = read_package_segment(&reader, &output.identity_segments()[0].segment_id);
    assert_eq!(batch_i32s(&segment[0], "id"), vec![2, 1, 3]);
    assert_eq!(
        batch_strings(&segment, "customer_name"),
        vec!["two", "one-last", "three"]
    );

    let summary = reader.read_dedup_summary_json().unwrap().unwrap();
    assert_eq!(summary["rule_id"], "row-rule-0001-dedup");
    assert_eq!(summary["keep"], "last");
    assert_eq!(summary["input_rows"], 4);
    assert_eq!(summary["output_rows"], 3);
    assert_eq!(summary["duplicate_key_count"], 1);
    assert_eq!(summary["dropped_row_count"], 1);
    assert_eq!(collect_dedup_dropped_provenance(&reader), vec![(0, 2)]);
    assert!(package_identity_file_paths(&reader).contains(DEDUP_SUMMARY_FILE));
}

#[test]
fn merge_dedup_keep_first_uses_package_order() {
    let batches = vec![
        batch_for_partition(
            "batch-dedup-first-0",
            "part-0",
            vec![1, 2],
            vec!["one-first", "two"],
            vec![true, true],
        ),
        batch_for_partition(
            "batch-dedup-first-1",
            "part-0",
            vec![1, 3],
            vec!["one-last", "three"],
            vec![true, true],
        ),
    ];
    let resource = MockResource::tier_a(batches);
    let mut input = plan_input(vec![], None, None, ExecutionExtent::bounded());
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Dedup {
        keys: vec!["id".to_owned()],
        keep: DedupKeep::First,
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(sample_schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 3);
    assert_eq!(output.identity_segments().len(), 1);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let segment = read_package_segment(&reader, &output.identity_segments()[0].segment_id);
    assert_eq!(batch_i32s(&segment[0], "id"), vec![1, 2, 3]);
    assert_eq!(
        batch_strings(&segment, "name"),
        vec!["one-first", "two", "three"]
    );

    let summary = reader.read_dedup_summary_json().unwrap().unwrap();
    assert_eq!(summary["keep"], "first");
    assert_eq!(summary["input_rows"], 4);
    assert_eq!(summary["output_rows"], 3);
    assert_eq!(summary["duplicate_key_count"], 1);
    assert_eq!(summary["dropped_row_count"], 1);
    assert_eq!(collect_dedup_dropped_provenance(&reader), vec![(2, 0)]);
}

#[test]
fn append_plan_with_compiled_dedup_rule_does_not_change_rows_or_write_summary() {
    let resource = MockResource::tier_a(vec![batch_for_partition(
        "batch-append-dedup",
        "part-0",
        vec![1, 1],
        vec!["one-first", "one-last"],
        vec![true, true],
    )])
    .with_write_disposition(WriteDisposition::Append);
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
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 2);
    assert_eq!(output.identity_segments().len(), 1);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = read_package_segment(&reader, &output.identity_segments()[0].segment_id);
    assert_eq!(batch_i32s(&batches[0], "id"), vec![1, 1]);
    assert_eq!(
        batch_strings(&batches, "name"),
        vec!["one-first", "one-last"]
    );
    assert!(reader.read_dedup_summary_json().unwrap().is_none());
}

#[test]
fn append_exact_row_dedup_compiles_and_drops_only_complete_duplicates() {
    let mut resource = MockResource::tier_a(vec![batch_for_partition(
        "batch-append-exact-row-dedup",
        "part-0",
        vec![1, 1, 1],
        vec!["same", "same", "different"],
        vec![true, true, true],
    )])
    .with_write_disposition(WriteDisposition::Append);
    resource.descriptor.deduplication = Some(DeduplicationSpec::ExactRow);
    let mut input = plan_input(vec![], None, None, ExecutionExtent::bounded());
    input.validation_program = compile_resource_validation_program(
        &ContractPolicy::for_trust(TrustLevel::Governed),
        &ObservedSchema::from_arrow(sample_schema().as_ref()),
        resource.descriptor(),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 2);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = read_package_segment(&reader, &output.identity_segments()[0].segment_id);
    assert_eq!(batch_i32s(&batches[0], "id"), vec![1, 1]);
    assert_eq!(batch_strings(&batches, "name"), vec!["same", "different"]);
    let summary = reader.read_dedup_summary_json().unwrap().unwrap();
    assert_eq!(summary["keep"], "first");
    assert_eq!(summary["input_rows"], 3);
    assert_eq!(summary["output_rows"], 2);
    assert_eq!(summary["dropped_row_count"], 1);
    assert_eq!(summary["version"], 3);
    assert_eq!(summary["provenance_format"], "parquet");
    assert_eq!(summary["provenance_path"], "stats/dedup-dropped/");
    assert_eq!(summary["provenance_shard_row_target"], 65_536);
    assert_eq!(summary["shard_count"], 1);
    assert!(summary.get("dropped_rows").is_none());
    assert!(
        temp.path()
            .join("stats/dedup-dropped/part-00000000000000000001.parquet")
            .is_file()
    );

    let spill_temp = TempDir::new().unwrap();
    let (_, services) =
        StandaloneExecutionHost::default_services_with_spill(64 * 1024 * 1024, 64 * 1024 * 1024)
            .unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let spilled = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        spill_temp.path(),
        &pre_finalize,
        EngineExecutionConfig::default().with_execution_services(services.clone()),
    ))
    .unwrap();

    assert_eq!(spilled.output.manifest.identity, output.manifest.identity);
    assert_eq!(
        spilled.output.manifest.package_hash,
        output.manifest.package_hash
    );
    let spill = services.spill().snapshot();
    assert!(spill.peak_bytes > 0);
    assert_eq!(spill.current_bytes, 0);
    let memory = services.memory().snapshot();
    assert!(memory.peak_bytes > 0);
    assert_eq!(memory.current_bytes, 0);
}

#[test]
fn replace_plan_with_compiled_dedup_rule_does_not_change_rows_or_write_summary() {
    let resource = MockResource::tier_a(vec![batch_for_partition(
        "batch-replace-dedup",
        "part-0",
        vec![1, 1],
        vec!["one-first", "one-last"],
        vec![true, true],
    )])
    .with_write_disposition(WriteDisposition::Replace);
    let mut input = plan_input(vec![], None, None, ExecutionExtent::bounded());
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Dedup {
        keys: vec!["id".to_owned()],
        keep: DedupKeep::First,
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
    assert_eq!(output.identity_segments().len(), 1);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = read_package_segment(&reader, &output.identity_segments()[0].segment_id);
    assert_eq!(batch_i32s(&batches[0], "id"), vec![1, 1]);
    assert_eq!(
        batch_strings(&batches, "name"),
        vec!["one-first", "one-last"]
    );
    assert!(reader.read_dedup_summary_json().unwrap().is_none());
}

#[test]
fn merge_dedup_fail_aborts_before_package_finalization() {
    let resource = MockResource::tier_a(vec![batch_for_partition(
        "batch-dedup-fail",
        "part-0",
        vec![1, 1],
        vec!["one-first", "one-last"],
        vec![true, true],
    )]);
    let mut input = plan_input(vec![], None, None, ExecutionExtent::bounded());
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Dedup {
        keys: vec!["id".to_owned()],
        keep: DedupKeep::Fail,
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(sample_schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();

    let error = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap_err();

    assert!(error.to_string().contains("keep=fail aborts"));
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    assert_ne!(reader.manifest().lifecycle.status, PackageStatus::Packaged);
    assert!(package_identity_segments(&reader).is_empty());
    assert!(reader.read_dedup_summary_json().unwrap().is_none());
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
        cdf_kernel::canonical_arrow_schema_hash(schema.as_ref()).unwrap(),
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![0]).with_timezone("UTC")) as ArrayRef,
            ],
        )
        .unwrap(),
    )
    .unwrap();
    let mut resource = MockResource::tier_a(vec![batch]);
    resource.descriptor.primary_key.clear();
    resource.descriptor.merge_key.clear();
    resource.descriptor.write_disposition = WriteDisposition::Append;
    let mut input = plan_input_for_schema(schema, vec![], None, None, ExecutionExtent::bounded());
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
    assert!(output.identity_segments().is_empty());
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    assert!(package_identity_file_paths(&reader).contains("plan/contract-evaluation-context.json"));
}

#[test]
fn traced_execution_emits_run_resource_package_and_partition_spans() {
    let resource = MockResource::tier_a(sample_batches());
    let input = plan_input(vec![], None, None, ExecutionExtent::bounded());
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
fn phase_telemetry_is_additive_and_preserves_manifest_identity() {
    let resource = MockResource::tier_a(sample_batches());
    let input = plan_input(vec![], None, None, ExecutionExtent::bounded());
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let plain_temp = TempDir::new().unwrap();
    let measured_temp = TempDir::new().unwrap();
    let plain = block_on(execute_to_package(&plan, &resource, plain_temp.path())).unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());

    let measured = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        measured_temp.path(),
        &pre_finalize,
        EngineExecutionConfig::default().with_phase_metrics(true),
    ))
    .unwrap();

    assert_eq!(measured.output.manifest.identity, plain.manifest.identity);
    assert_eq!(
        measured.output.manifest.package_hash,
        plain.manifest.package_hash
    );
    assert_eq!(measured.output.manifest.signature, plain.manifest.signature);
    assert!(!measured.phase_metrics.is_empty());
    assert!(measured.phase_metrics.iter().all(|metric| {
        metric.status == RunPhaseStatus::Completed
            && metric.duration_ns > 0
            && metric.operations > 0
    }));
    let phases = measured
        .phase_metrics
        .iter()
        .map(|metric| metric.phase)
        .collect::<std::collections::BTreeSet<_>>();
    for phase in [
        RunPhase::Decode,
        RunPhase::ValidationNormalization,
        RunPhase::SegmentEncode,
        RunPhase::PersistHash,
        RunPhase::PackageFinalize,
    ] {
        assert!(phases.contains(&phase), "missing {phase:?}");
    }
}

fn collect_dedup_dropped_provenance(reader: &cdf_package::PackageReader) -> Vec<(u64, u64)> {
    let mut rows = Vec::new();
    reader
        .for_each_dedup_dropped_provenance(&mut |dropped, kept| {
            rows.push((dropped, kept));
            Ok(())
        })
        .unwrap();
    rows
}

fn package_identity_file_paths(reader: &cdf_package::PackageReader) -> BTreeSet<String> {
    let mut paths = BTreeSet::new();
    reader
        .for_each_identity_file(&mut |entry| {
            paths.insert(entry.path);
            Ok(())
        })
        .unwrap();
    paths
}

fn package_identity_segments(reader: &cdf_package::PackageReader) -> Vec<SegmentEntry> {
    let mut segments = Vec::new();
    reader
        .for_each_identity_segment(&mut |entry| {
            segments.push(entry);
            Ok(())
        })
        .unwrap();
    segments
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

fn batch_i32s(batch: &RecordBatch, column: &str) -> Vec<i32> {
    let index = batch.schema().index_of(column).unwrap();
    let array = batch
        .column(index)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    (0..array.len()).map(|row| array.value(row)).collect()
}

fn parquet_reconciled_schema() -> SchemaRef {
    Arc::new(parquet_reconciliation().schema)
}

fn parquet_reconciliation() -> cdf_contract::SchemaReconciliation {
    reconcile_schema(
        &Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]),
        &Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]),
        &ContractPolicy::default().types,
    )
    .unwrap()
}

fn parquet_reconciled_batch() -> Batch {
    let reconciliation = parquet_reconciliation();
    let serialized_plan = serde_json::to_string(&reconciliation.plan).unwrap();
    let schema = Arc::new(reconciliation.schema);
    let record_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["one", "two"])) as ArrayRef,
        ],
    )
    .unwrap();

    Batch {
        header: {
            let mut header = BatchHeader::new(
                BatchId::new("batch-parquet-reconciled").unwrap(),
                ResourceId::new("orders").unwrap(),
                PartitionId::new("part-0").unwrap(),
                cdf_kernel::canonical_arrow_schema_hash(record_batch.schema().as_ref()).unwrap(),
                record_batch.num_rows() as u64,
                record_batch.get_array_memory_size() as u64,
            );
            header.schema_coercion_plan = Some(serialized_plan);
            header
                .mark_materialized_output(&Schema::new(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("name", DataType::Utf8, true),
                ]))
                .unwrap();
            header
        },
        payload: cdf_kernel::BatchPayload::in_memory(record_batch),
    }
}

fn nested_variant_batch() -> Batch {
    let payload = StructArray::from(vec![
        (
            Arc::new(Field::new("kind", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["alpha", "beta"])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("count", DataType::Int32, false)),
            Arc::new(Int32Array::from(vec![7, 9])) as ArrayRef,
        ),
    ]);
    let tags = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1), Some(2)]),
        Some(vec![Some(3), None]),
    ]);
    let mut attributes = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
    attributes.keys().append_value("tier");
    attributes.values().append_value(1);
    attributes.append(true).unwrap();
    attributes.keys().append_value("score");
    attributes.values().append_value(5);
    attributes.append(true).unwrap();
    let attributes = attributes.finish();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        semantic_field(
            Field::new("email", DataType::Utf8, false),
            r#"cdf.pii@1(class="email")"#,
        ),
        Field::new("payload", payload.data_type().clone(), true),
        Field::new("tags", tags.data_type().clone(), true),
        Field::new("attributes", attributes.data_type().clone(), true),
    ]));
    let record_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["ok@example.test", "raw-secret"])) as ArrayRef,
            Arc::new(payload) as ArrayRef,
            Arc::new(tags) as ArrayRef,
            Arc::new(attributes) as ArrayRef,
        ],
    )
    .unwrap();

    Batch {
        header: BatchHeader::new(
            BatchId::new("batch-variant").unwrap(),
            ResourceId::new("orders").unwrap(),
            PartitionId::new("part-0").unwrap(),
            cdf_kernel::canonical_arrow_schema_hash(record_batch.schema().as_ref()).unwrap(),
            record_batch.num_rows() as u64,
            record_batch.get_array_memory_size() as u64,
        ),
        payload: cdf_kernel::BatchPayload::in_memory(record_batch),
    }
}
