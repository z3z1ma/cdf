use super::*;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Instant,
};

use arrow_array::{
    ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use cdf_kernel::{
    IdentifierRules, TrustLevel, TypeMapping, TypeMappingFidelity, physical_type, source_name,
    with_semantic, with_source_name,
};

#[test]
fn validation_program_serializes_and_has_total_lattice() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let observed = ObservedSchema::from_arrow(&schema);
    let program =
        compile_validation_program(&ContractPolicy::for_trust(TrustLevel::Governed), &observed)
            .unwrap();

    assert!(program.row_rules.iter().any(|rule| {
        rule.rule_id == "nullability:id" && rule.expression_function() == Some("is_not_null")
    }));
    assert_verdict_lattice_total(&program).unwrap();
    for outcome in RuleOutcome::ALL {
        assert_ne!(
            program.disposition_for(outcome, "rule-1"),
            RuleDisposition::RejectRun {
                rule_id: "missing".to_owned()
            }
        );
    }

    let json = serde_json::to_string(&program).unwrap();
    assert!(!json.contains("schema_coercion"));
    assert_eq!(
        program,
        serde_json::from_str::<ValidationProgram>(&json).unwrap()
    );
}

#[test]
fn validation_program_coercion_evidence_is_optional_and_round_trips() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let observed = ObservedSchema::from_arrow(&schema);
    let mut program =
        compile_validation_program(&ContractPolicy::for_trust(TrustLevel::Governed), &observed)
            .unwrap();

    let mut legacy_json = serde_json::to_value(&program).unwrap();
    legacy_json
        .as_object_mut()
        .unwrap()
        .remove("schema_coercion");
    let legacy = serde_json::from_value::<ValidationProgram>(legacy_json).unwrap();
    assert!(legacy.schema_coercion.is_none());

    let reconciliation = reconcile_schema(
        &Schema::new(vec![Field::new("id", DataType::Int32, false)]),
        &schema,
        &ContractPolicy::default().types,
    )
    .unwrap();
    program.schema_coercion = Some(reconciliation.plan.clone());

    let value = serde_json::to_value(&program).unwrap();
    assert_eq!(value["schema_coercion"]["fields"][0]["decision"], "widened");
    assert_eq!(
        program,
        serde_json::from_value::<ValidationProgram>(value).unwrap()
    );
}

#[test]
fn legacy_validation_program_without_identifier_policy_uses_versioned_default() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let observed = ObservedSchema::from_arrow(&schema);
    let program =
        compile_validation_program(&ContractPolicy::for_trust(TrustLevel::Governed), &observed)
            .unwrap();
    let mut legacy_json = serde_json::to_value(&program).unwrap();
    legacy_json
        .as_object_mut()
        .unwrap()
        .remove("identifier_policy");

    let legacy = serde_json::from_value::<ValidationProgram>(legacy_json).unwrap();
    assert_eq!(legacy.identifier_policy, IdentifierPolicy::default());
    assert_eq!(legacy.normalizer_version, legacy.identifier_policy.version);
}

#[test]
fn row_evaluator_returns_accept_mask_quarantine_candidates_and_summary() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("code", DataType::Utf8, true),
        Field::new("score", DataType::Int32, true),
        Field::new("required_note", DataType::Utf8, true),
    ]);
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![
        RowRule::Domain {
            column: "status".to_owned(),
            allowed: vec!["open".to_owned()],
        },
        RowRule::Regex {
            column: "code".to_owned(),
            pattern: "^A-[0-9]+$".to_owned(),
        },
        RowRule::Range {
            column: "score".to_owned(),
            min: Some("0".to_owned()),
            max: Some("10".to_owned()),
        },
        RowRule::Nullability {
            column: "required_note".to_owned(),
        },
    ];
    let program =
        compile_validation_program(&policy, &ObservedSchema::from_arrow(&schema)).unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec!["open", "bad", "open"])),
            Arc::new(StringArray::from(vec![
                Some("A-1"),
                Some("no"),
                Some("A-2"),
            ])),
            Arc::new(Int32Array::from(vec![Some(5), Some(7), Some(11)])),
            Arc::new(StringArray::from(vec![Some("kept"), None, Some("kept")])),
        ],
    )
    .unwrap();

    let evaluation =
        evaluate_record_batch(&program, &ContractEvaluationContext::default(), &batch).unwrap();

    assert!(evaluation.accepted_rows.value(0));
    assert!(!evaluation.accepted_rows.value(1));
    assert!(!evaluation.accepted_rows.value(2));
    assert_eq!(evaluation.summary.input_rows, 3);
    assert_eq!(evaluation.summary.accepted_rows, 1);
    assert_eq!(evaluation.summary.quarantined_rows, 2);
    assert_eq!(evaluation.summary.quarantine_candidate_count, 4);
    assert!(evaluation.quarantine_candidates.iter().any(|candidate| {
        candidate.source_row_ordinal == 1 && candidate.error_code == "domain_violation"
    }));
    assert!(evaluation.quarantine_candidates.iter().any(|candidate| {
        candidate.source_row_ordinal == 1 && candidate.error_code == "regex_violation"
    }));
    assert!(evaluation.quarantine_candidates.iter().any(|candidate| {
        candidate.source_row_ordinal == 2 && candidate.error_code == "range_violation"
    }));
}

#[test]
fn package_order_dedup_keeps_last_across_accepted_batches_and_summarizes_drops() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]);
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Dedup {
        keys: vec!["id".to_owned()],
        keep: DedupKeep::Last,
    }];
    let program =
        compile_validation_program(&policy, &ObservedSchema::from_arrow(&schema)).unwrap();
    let first = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["one-first", "two"])),
        ],
    )
    .unwrap();
    let second = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(vec![3, 1])) as ArrayRef,
            Arc::new(StringArray::from(vec!["three", "one-last"])),
        ],
    )
    .unwrap();

    let evaluation = evaluate_package_order_dedup(&program, &[first, second])
        .unwrap()
        .unwrap();

    assert!(!evaluation.retained_rows[0].value(0));
    assert!(evaluation.retained_rows[0].value(1));
    assert!(evaluation.retained_rows[1].value(0));
    assert!(evaluation.retained_rows[1].value(1));
    assert_eq!(evaluation.summary.rule_id, "row-rule-0000-dedup");
    assert_eq!(evaluation.summary.keep, DedupKeepProgram::Last);
    assert_eq!(evaluation.summary.input_rows, 4);
    assert_eq!(evaluation.summary.output_rows, 3);
    assert_eq!(evaluation.summary.duplicate_key_count, 1);
    assert_eq!(evaluation.summary.dropped_row_count, 1);
    assert_eq!(
        evaluation.summary.dropped_rows,
        vec![DedupDroppedRow {
            package_row_ordinal: 0,
            kept_package_row_ordinal: 3,
        }]
    );
}

#[test]
fn package_order_dedup_fail_aborts_on_duplicate_key() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Dedup {
        keys: vec!["id".to_owned()],
        keep: DedupKeep::Fail,
    }];
    let program =
        compile_validation_program(&policy, &ObservedSchema::from_arrow(&schema)).unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(Int64Array::from(vec![1, 1])) as ArrayRef],
    )
    .unwrap();

    let error = evaluate_package_order_dedup(&program, &[batch]).unwrap_err();

    assert!(error.to_string().contains("keep=fail aborts"));
}

#[test]
fn package_order_dedup_treats_null_as_a_typed_identity_value() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, true)]);
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Dedup {
        keys: vec!["id".to_owned()],
        keep: DedupKeep::Last,
    }];
    let program =
        compile_validation_program(&policy, &ObservedSchema::from_arrow(&schema)).unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(Int64Array::from(vec![None, Some(1), None])) as ArrayRef],
    )
    .unwrap();

    let evaluation = evaluate_package_order_dedup(&program, &[batch])
        .unwrap()
        .unwrap();

    assert_eq!(evaluation.summary.input_rows, 3);
    assert_eq!(evaluation.summary.output_rows, 2);
    assert_eq!(evaluation.summary.dropped_row_count, 1);
    assert_eq!(evaluation.summary.dropped_rows[0].package_row_ordinal, 0);
    assert_eq!(
        evaluation.summary.dropped_rows[0].kept_package_row_ordinal,
        2
    );
}

#[test]
fn exact_row_dedup_compares_the_final_residual_variant_field() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.schema.mode = SchemaEvolutionMode::Evolve;
    let mut program =
        compile_validation_program(&policy, &ObservedSchema::from_arrow(&schema)).unwrap();
    program.row_rules.push(RowRuleProgram {
        rule_id: "exact-final-output".to_owned(),
        expression: Expression::call(
            "exact_row_dedup",
            vec![
                ExpressionNode::Literal {
                    value: ExpressionLiteral::StringList(vec![
                        "id".to_owned(),
                        VARIANT_COLUMN_NAME.to_owned(),
                    ]),
                },
                ExpressionNode::Literal {
                    value: ExpressionLiteral::String("first".to_owned()),
                },
            ],
        ),
        missing_column: MissingColumnBehavior::Error,
    });
    let final_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(VARIANT_COLUMN_NAME, DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(final_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1])) as ArrayRef,
            Arc::new(StringArray::from(vec!["different-a", "different-b"])) as ArrayRef,
        ],
    )
    .unwrap();

    let evaluation = evaluate_package_order_dedup(&program, &[batch])
        .unwrap()
        .unwrap();

    assert_eq!(evaluation.summary.input_rows, 2);
    assert_eq!(evaluation.summary.output_rows, 2);
    assert_eq!(evaluation.summary.dropped_row_count, 0);
}

#[test]
fn freshness_uses_observed_at_context_and_fails_closed_without_it() {
    let schema = Schema::new(vec![Field::new(
        "updated_at",
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        false,
    )]);
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Freshness {
        column: "updated_at".to_owned(),
        max_age_ms: 1_000,
    }];
    let program =
        compile_validation_program(&policy, &ObservedSchema::from_arrow(&schema)).unwrap();
    assert!(program.requires_observed_at_ms());
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![9_500, 8_000]).with_timezone("UTC"))
                as ArrayRef,
        ],
    )
    .unwrap();

    let missing_context =
        evaluate_record_batch(&program, &ContractEvaluationContext::default(), &batch).unwrap_err();
    assert!(missing_context.to_string().contains("observed_at_ms"));

    let evaluation = evaluate_record_batch(
        &program,
        &ContractEvaluationContext::observed_at(10_000),
        &batch,
    )
    .unwrap();
    assert!(evaluation.accepted_rows.value(0));
    assert!(!evaluation.accepted_rows.value(1));
}

#[test]
fn row_evaluator_fails_closed_on_missing_coverage_type_mismatch_and_bad_timestamp_rule() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let program = compile_validation_program(
        &ContractPolicy::for_trust(TrustLevel::Governed),
        &ObservedSchema::from_arrow(&schema),
    )
    .unwrap();
    let uncovered = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
    )
    .unwrap();
    let error = evaluate_record_batch(&program, &ContractEvaluationContext::default(), &uncovered)
        .unwrap_err();
    assert!(error.to_string().contains("does not cover field"));

    let wrong_type = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
    )
    .unwrap();
    let error = evaluate_record_batch(&program, &ContractEvaluationContext::default(), &wrong_type)
        .unwrap_err();
    assert!(error.to_string().contains("expects"));

    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Freshness {
        column: "id".to_owned(),
        max_age_ms: 1_000,
    }];
    let bad_freshness_program =
        compile_validation_program(&policy, &ObservedSchema::from_arrow(&schema)).unwrap();
    let error = evaluate_record_batch(
        &bad_freshness_program,
        &ContractEvaluationContext::observed_at(10_000),
        &RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
        )
        .unwrap(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("requires a timestamp column"));
}

#[test]
fn reject_batch_disposition_aborts_evaluation() {
    let schema = Schema::new(vec![Field::new("status", DataType::Utf8, false)]);
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.verdicts.violation = VerdictAction::RejectBatch;
    policy.rows.rules = vec![RowRule::Domain {
        column: "status".to_owned(),
        allowed: vec!["open".to_owned()],
    }];
    let program =
        compile_validation_program(&policy, &ObservedSchema::from_arrow(&schema)).unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(StringArray::from(vec!["closed"])) as ArrayRef],
    )
    .unwrap();

    let error =
        evaluate_record_batch(&program, &ContractEvaluationContext::default(), &batch).unwrap_err();

    assert!(error.to_string().contains("reject_batch"));
}

#[test]
fn local_non_public_type_null_domain_100k_rows_benchmarkable_path() {
    let row_count = 100_000;
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, false),
    ]);
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Domain {
        column: "status".to_owned(),
        allowed: vec!["ok".to_owned()],
    }];
    let program =
        compile_validation_program(&policy, &ObservedSchema::from_arrow(&schema)).unwrap();
    let ids = (0..row_count as i64).collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(vec!["ok"; row_count])),
        ],
    )
    .unwrap();

    let started = Instant::now();
    let evaluation =
        evaluate_record_batch(&program, &ContractEvaluationContext::default(), &batch).unwrap();
    let elapsed = started.elapsed();

    assert_eq!(evaluation.summary.input_rows, row_count as u64);
    assert_eq!(evaluation.summary.accepted_rows, row_count as u64);
    assert_eq!(evaluation.summary.quarantined_rows, 0);
    println!(
        "local_non_public_contract_eval_type_null_domain rows={} elapsed_ms={:.3}",
        row_count,
        elapsed.as_secs_f64() * 1000.0
    );
}

#[test]
fn decimal_fidelity_rejects_silent_float_conversion() {
    let schema = Schema::new(vec![Field::new("amount", DataType::Float64, false)]);
    let mut claims = BTreeMap::new();
    claims.insert(
        "amount".to_owned(),
        SourceTypeClaim::Decimal {
            precision: 18,
            scale: 2,
        },
    );
    let observed = ObservedSchema::from_arrow_with_claims(&schema, claims);

    let error =
        compile_validation_program(&ContractPolicy::for_trust(TrustLevel::Financial), &observed)
            .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("cannot compile as floating point")
    );
}

#[test]
fn timestamp_fidelity_rejects_naive_utc_assumption() {
    let schema = Schema::new(vec![Field::new(
        "created_at",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        false,
    )]);
    let mut claims = BTreeMap::new();
    claims.insert(
        "created_at".to_owned(),
        SourceTypeClaim::Timestamp {
            timezone: TimestampZoneClaim::Naive,
        },
    );
    let observed = ObservedSchema::from_arrow_with_claims(&schema, claims);

    let error =
        compile_validation_program(&ContractPolicy::for_trust(TrustLevel::Financial), &observed)
            .unwrap_err();

    assert!(error.to_string().contains("cannot be silently assumed"));
}

#[test]
fn timestamp_fidelity_rejects_lost_zoned_story() {
    let schema = Schema::new(vec![Field::new(
        "created_at",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        false,
    )]);
    let mut claims = BTreeMap::new();
    claims.insert(
        "created_at".to_owned(),
        SourceTypeClaim::Timestamp {
            timezone: TimestampZoneClaim::Zoned {
                zone: "America/Phoenix".to_owned(),
            },
        },
    );
    let observed = ObservedSchema::from_arrow_with_claims(&schema, claims);

    let error =
        compile_validation_program(&ContractPolicy::for_trust(TrustLevel::Financial), &observed)
            .unwrap_err();

    assert!(error.to_string().contains("lost its timezone"));
}

#[test]
fn normalizer_preserves_source_names_and_rejects_collisions() {
    let schema = Schema::new(vec![
        Field::new("userName", DataType::Utf8, true),
        Field::new("user_name", DataType::Utf8, true),
    ]);
    let observed = ObservedSchema::from_arrow(&schema);
    let error = normalize_schema(&observed, &IdentifierPolicy::default()).unwrap_err();
    assert!(error.to_string().contains("identifier collision"));

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("id", DataType::Utf8, true),
    ]);
    let error = normalize_schema(
        &ObservedSchema::from_arrow(&schema),
        &IdentifierPolicy::default(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("identifier collision"));

    let source_field = with_source_name(
        Field::new("already_normalized", DataType::Utf8, true),
        "Original Name",
    );
    let schema = Schema::new(vec![source_field]);
    let normalized = normalize_arrow_schema(&schema, &IdentifierPolicy::default()).unwrap();
    let field = normalized.field(0);
    assert_eq!(field.name(), "original_name");
    assert_eq!(source_name(field), Some("Original Name"));
}

#[test]
fn namecase_v1_truncates_with_stable_hash_suffix() {
    let policy = IdentifierPolicy {
        max_length: Some(20),
        ..IdentifierPolicy::default()
    };
    let normalized = normalize_identifier("Very Long Source Identifier Name", &policy).unwrap();

    assert_eq!(normalized.len(), 20);
    assert!(normalized.contains('_'));
    assert_eq!(
        normalized,
        normalize_identifier("Very Long Source Identifier Name", &policy).unwrap()
    );
}

#[test]
fn identifier_policy_serde_missing_max_length_keeps_default_cap() {
    let policy = serde_json::from_value::<IdentifierPolicy>(serde_json::json!({
        "version": "namecase-v1",
        "charset": "ascii_lower_snake"
    }))
    .unwrap();

    assert_eq!(policy.max_length, Some(63));
    assert_eq!(
        normalize_identifier(
            "Very Long Source Identifier Name Repeated Until It Exceeds The Default Cap",
            &policy,
        )
        .unwrap()
        .len(),
        63
    );
}

#[test]
fn destination_identifier_policy_preserves_postgres_max_length() {
    let policy = IdentifierPolicy::from_destination_rules(&IdentifierRules {
        normalizer: "namecase-v1/postgres-quoted-v1".to_owned(),
        max_length: Some(63),
        allowed_pattern: Some(
            "quoted UTF-8 identifier without NUL; cdf reserves _cdf_*".to_owned(),
        ),
    })
    .unwrap();

    let normalized = normalize_identifier(
        "Very Long Source Identifier Name Repeated Until It Exceeds Postgres Limit",
        &policy,
    )
    .unwrap();

    assert_eq!(policy.version, NORMALIZER_NAMECASE_V1);
    assert_eq!(policy.max_length, Some(63));
    assert_eq!(normalized.len(), 63);
    assert_eq!(
        normalized,
        normalize_identifier(
            "Very Long Source Identifier Name Repeated Until It Exceeds Postgres Limit",
            &policy,
        )
        .unwrap()
    );
}

#[test]
fn destination_identifier_policy_rejects_duckdb_pattern_miss() {
    let policy = identifier_policy_from_destination_rules(&IdentifierRules {
        normalizer: "namecase-v1".to_owned(),
        max_length: None,
        allowed_pattern: Some("^[a-z_][a-z0-9_]*$".to_owned()),
    })
    .unwrap();

    let long_duckdb_name = normalize_identifier(&format!("a{}", "b".repeat(80)), &policy).unwrap();
    assert_eq!(policy.max_length, None);
    assert_eq!(long_duckdb_name.len(), 81);

    let error = normalize_identifier("123 Source Name", &policy).unwrap_err();

    assert!(error.to_string().contains("allowed_pattern"));
    assert!(error.to_string().contains("123_source_name"));
}

#[test]
fn destination_identifier_policy_rejects_unsupported_rules() {
    let error = IdentifierPolicy::from_destination_rules(&IdentifierRules {
        normalizer: "object-key-component-v1".to_owned(),
        max_length: None,
        allowed_pattern: None,
    })
    .unwrap_err();
    let message = error.to_string();

    assert!(message.contains("object-key-component-v1"));
    assert!(
        message
            .contains("live column normalization for that rule is not implemented by this adapter")
    );
}

#[test]
fn destination_identifier_policy_keeps_collision_behavior_stable() {
    let policy = IdentifierPolicy::from_destination_rules(&IdentifierRules {
        normalizer: "namecase-v1/postgres-quoted-v1".to_owned(),
        max_length: Some(63),
        allowed_pattern: Some(
            "quoted UTF-8 identifier without NUL; cdf reserves _cdf_*".to_owned(),
        ),
    })
    .unwrap();
    let schema = Schema::new(vec![
        Field::new("userName", DataType::Utf8, true),
        Field::new("user_name", DataType::Utf8, true),
    ]);
    let error = normalize_schema(&ObservedSchema::from_arrow(&schema), &policy).unwrap_err();
    let message = error.to_string();

    assert!(message.contains("identifier collision after namecase-v1"));
    assert!(message.contains("userName"));
    assert!(message.contains("user_name"));
    assert!(message.contains("user_name"));
}

#[test]
fn trust_presets_expand_to_specified_policy_shapes() {
    let experimental = ContractPolicy::for_trust(TrustLevel::Experimental);
    assert_eq!(experimental.schema.mode, SchemaEvolutionMode::Evolve);
    assert!(!experimental.quarantine.enabled);
    assert!(matches!(
        experimental.normalization.nested,
        NestedDataPolicy::VariantCapture(_)
    ));

    let governed = ContractPolicy::for_trust(TrustLevel::Governed);
    assert_eq!(governed.schema.mode, SchemaEvolutionMode::Evolve);
    assert!(governed.schema.review_artifact_required);
    assert_eq!(governed.rows.validation_depth, ValidationDepth::Full);
    assert!(governed.quarantine.enabled);

    let financial = ContractPolicy::for_trust(TrustLevel::Financial);
    assert_eq!(financial.schema.mode, SchemaEvolutionMode::Freeze);
    assert!(financial.types.preserve_decimal_exactness);
    assert!(financial.types.preserve_timestamp_timezone);
    assert_eq!(financial.lineage, LineagePolicy::Full);
    assert!(financial.receipts_required);
    assert!(financial.reconciliation_counts);
    assert_eq!(financial.retention, RetentionClass::Long);

    let serving = ContractPolicy::for_trust(TrustLevel::Serving);
    assert_eq!(serving.schema.mode, SchemaEvolutionMode::Freeze);
    assert!(serving.rows.freshness_slo);
    assert!(matches!(
        serving.rows.validation_depth,
        ValidationDepth::SampledFastPath { .. }
    ));
    assert!(serving.promotion.demote_on_anomaly);
}

#[test]
fn nested_variant_policy_compiles_variant_capture_action() {
    let schema = Schema::new(vec![Field::new_struct(
        "payload",
        vec![Field::new("id", DataType::Int64, false)],
        true,
    )]);
    let observed = ObservedSchema::from_arrow(&schema);
    let program = compile_validation_program(
        &ContractPolicy::for_trust(TrustLevel::Experimental),
        &observed,
    )
    .unwrap();

    assert_eq!(
        program.column_programs[0].nested_action,
        NestedAction::CaptureVariant {
            column_name: VARIANT_COLUMN_NAME.to_owned(),
            semantic: VARIANT_SEMANTIC_TAG.to_owned(),
        }
    );
    assert!(program.schema_verdicts.iter().any(|rule| {
        rule.change == SchemaChangeKind::UnknownField
            && rule.verdict == VerdictAction::AdmitAsVariant
    }));
}

#[test]
fn framework_variant_field_classifier_requires_the_exact_contract() {
    let exact = with_semantic(
        Field::new(VARIANT_COLUMN_NAME, DataType::Utf8, true),
        VARIANT_SEMANTIC_TAG,
    );
    let mut exact_metadata = exact.metadata().clone();
    exact_metadata.insert(
        RESIDUAL_ENCODING_METADATA_KEY.to_owned(),
        RESIDUAL_ENCODING_NAME.to_owned(),
    );
    let exact = exact.with_metadata(exact_metadata.clone());
    assert!(is_framework_variant_field(&exact));

    let impostors = [
        Field::new(VARIANT_COLUMN_NAME, DataType::Utf8, true),
        with_semantic(
            Field::new(VARIANT_COLUMN_NAME, DataType::Utf8, true),
            "wrong",
        ),
        with_semantic(
            Field::new(VARIANT_COLUMN_NAME, DataType::Int64, true),
            VARIANT_SEMANTIC_TAG,
        )
        .with_metadata(exact_metadata.clone()),
        with_semantic(
            Field::new(VARIANT_COLUMN_NAME, DataType::Utf8, false),
            VARIANT_SEMANTIC_TAG,
        )
        .with_metadata(exact_metadata.clone()),
        with_semantic(
            Field::new("variant", DataType::Utf8, true),
            VARIANT_SEMANTIC_TAG,
        )
        .with_metadata(exact_metadata.clone()),
        with_semantic(
            Field::new(VARIANT_COLUMN_NAME, DataType::Utf8, true),
            VARIANT_SEMANTIC_TAG,
        )
        .with_metadata(std::collections::HashMap::from([
            (
                cdf_kernel::SEMANTIC_METADATA_KEY.to_owned(),
                VARIANT_SEMANTIC_TAG.to_owned(),
            ),
            (
                RESIDUAL_ENCODING_METADATA_KEY.to_owned(),
                "wrong".to_owned(),
            ),
        ])),
    ];
    for impostor in impostors {
        assert!(!is_framework_variant_field(&impostor));
    }
}

#[test]
fn pii_redaction_decision_is_available_from_semantic_metadata() {
    let field = with_semantic(Field::new("email", DataType::Utf8, false), "pii:email");
    let decision = redaction_decision_for_field(&field, &PiiRedactionPolicy::default());

    assert_eq!(
        decision,
        RedactionDecision::Hash {
            algorithm: "sha256".to_owned(),
        }
    );
}

#[test]
fn lossy_mapping_requires_explicit_policy_allowance() {
    let mapping = TypeMapping {
        arrow_type: "Decimal128(18, 2)".to_owned(),
        destination_type: "DOUBLE".to_owned(),
        fidelity: TypeMappingFidelity::LossyRequiresContractAllowance,
    };

    let denied = validate_type_mapping(&ContractPolicy::default(), &mapping).unwrap_err();
    assert!(denied.to_string().contains("requires allow_lossy_mapping"));

    let mut allowed_policy = ContractPolicy::default();
    allowed_policy.types.allow_lossy_mapping = true;
    assert_eq!(
        validate_type_mapping(&allowed_policy, &mapping).unwrap(),
        TypeMappingDecision::AllowedLossyByContract
    );
}

#[test]
fn schema_reconciliation_preserves_constraint_names_and_classifies_extra_fields() {
    let observed = Schema::new(vec![
        Field::new("VendorID", DataType::Int64, false),
        Field::new("ignored_physical_column", DataType::Utf8, true),
    ]);
    let constraint = Schema::new(vec![with_source_name(
        with_semantic(Field::new("vendor_id", DataType::Int64, false), "id"),
        "VendorID",
    )]);

    let reconciliation =
        reconcile_schema(&observed, &constraint, &ContractPolicy::default().types).unwrap();
    let field = reconciliation.schema.field(0);

    assert_eq!(field.name(), "vendor_id");
    assert_eq!(source_name(field), Some("VendorID"));
    assert_eq!(physical_type(field), Some("Int64"));
    assert_eq!(field.metadata().get("cdf:semantic"), Some(&"id".to_owned()));
    assert_eq!(
        decision_for(&reconciliation.plan, "VendorID").decision,
        FieldCoercionDecision::Preserved
    );
    assert_eq!(
        decision_for(&reconciliation.plan, "ignored_physical_column").decision,
        FieldCoercionDecision::Extra
    );

    let json = serde_json::to_string(&reconciliation.plan).unwrap();
    assert_eq!(
        reconciliation.plan,
        serde_json::from_str::<SchemaCoercionPlan>(&json).unwrap()
    );
}

#[test]
fn schema_reconciliation_records_lossless_widenings_and_physical_type() {
    let observed = Schema::new(vec![
        Field::new("signed", DataType::Int32, false),
        Field::new("unsigned", DataType::UInt8, false),
        Field::new("float", DataType::Float32, false),
        Field::new("decimal_ready", DataType::Int32, false),
        Field::new("service_day", DataType::Date32, false),
    ]);
    let constraint = Schema::new(vec![
        Field::new("signed", DataType::Int64, false),
        Field::new("unsigned", DataType::UInt64, false),
        Field::new("float", DataType::Float64, false),
        Field::new("decimal_ready", DataType::Decimal128(12, 2), false),
        Field::new(
            "service_day",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ]);

    let reconciliation =
        reconcile_schema(&observed, &constraint, &ContractPolicy::default().types).unwrap();

    for source in [
        "signed",
        "unsigned",
        "float",
        "decimal_ready",
        "service_day",
    ] {
        assert_eq!(
            decision_for(&reconciliation.plan, source).decision,
            FieldCoercionDecision::Widened
        );
        assert!(physical_type(reconciliation.schema.field_with_name(source).unwrap()).is_some());
    }
    assert_eq!(
        physical_type(reconciliation.schema.field_with_name("signed").unwrap()),
        Some("Int32")
    );
    assert_eq!(
        physical_type(
            reconciliation
                .schema
                .field_with_name("service_day")
                .unwrap()
        ),
        Some("Date32")
    );
}

#[test]
fn schema_coercion_plan_from_reconciled_schema_records_widened_and_preserved_fields() {
    let observed = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]);
    let constraint = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]);
    let schema = reconcile_schema(&observed, &constraint, &ContractPolicy::default().types)
        .unwrap()
        .schema;

    let plan = schema_coercion_plan_from_reconciled_schema(&schema)
        .unwrap()
        .unwrap();

    let widened = decision_for(&plan, "id");
    assert_eq!(widened.decision, FieldCoercionDecision::Widened);
    assert_eq!(widened.observed_type.as_deref(), Some("Int32"));
    assert_eq!(widened.constraint_type.as_deref(), Some("Int64"));
    assert_eq!(widened.observed_name.as_deref(), Some("id"));
    assert_eq!(widened.output_name.as_deref(), Some("id"));

    let preserved = decision_for(&plan, "name");
    assert_eq!(preserved.decision, FieldCoercionDecision::Preserved);
    assert_eq!(preserved.observed_type.as_deref(), Some("Utf8"));
    assert_eq!(preserved.constraint_type.as_deref(), Some("Utf8"));
}

#[test]
fn schema_reconciliation_missing_fields_fail_with_operator_fixes() {
    let observed = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let constraint = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Decimal128(10, 2), false),
    ]);

    let report =
        plan_schema_reconciliation(&observed, &constraint, &ContractPolicy::default().types)
            .unwrap();
    assert!(report.schema.is_none());
    assert_eq!(report.errors.len(), 1);
    assert_eq!(
        decision_for(&report.plan, "amount").decision,
        FieldCoercionDecision::Missing
    );

    let error = report.into_result().unwrap_err().to_string();
    assert!(error.contains("amount"));
    assert!(error.contains("Decimal128(10, 2)"));
    assert!(error.contains("fix the source or discovery probe"));
}

#[test]
fn schema_reconciliation_rejects_lossy_casts_until_policy_allows_them() {
    let observed = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let constraint = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

    let denied =
        plan_schema_reconciliation(&observed, &constraint, &ContractPolicy::default().types)
            .unwrap();
    assert_eq!(
        decision_for(&denied.plan, "id").decision,
        FieldCoercionDecision::LossyRejected
    );
    let error = denied.into_result().unwrap_err().to_string();
    assert!(error.contains("observed type Int64"));
    assert!(error.contains("declared type Int32"));
    assert!(error.contains("enable allow_lossy_mapping"));

    let mut type_policy = ContractPolicy::default().types;
    type_policy.allow_lossy_mapping = true;
    let allowed = reconcile_schema(&observed, &constraint, &type_policy).unwrap();

    assert_eq!(
        decision_for(&allowed.plan, "id").decision,
        FieldCoercionDecision::LossyAllowed
    );
    assert_eq!(
        physical_type(allowed.schema.field_with_name("id").unwrap()),
        Some("Int64")
    );
    assert_eq!(
        schema_coercion_plan_from_reconciled_schema(&allowed.schema),
        Ok(Some(allowed.plan))
    );
}

#[test]
fn reconciled_schema_metadata_preserves_extra_field_decisions_for_package_evidence() {
    let observed = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("source_only", DataType::Utf8, true),
    ]);
    let constraint = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

    let reconciliation =
        reconcile_schema(&observed, &constraint, &ContractPolicy::default().types).unwrap();

    assert_eq!(
        decision_for(&reconciliation.plan, "source_only").decision,
        FieldCoercionDecision::Extra
    );
    assert_eq!(
        schema_coercion_plan_from_reconciled_schema(&reconciliation.schema),
        Ok(Some(reconciliation.plan))
    );
}

#[test]
fn schema_coercion_evidence_rejects_malformed_and_false_metadata() {
    let malformed = Schema::new_with_metadata(
        vec![Field::new("id", DataType::Int64, false)],
        HashMap::from([(
            "cdf:schema_coercion_plan".to_owned(),
            "{not-json".to_owned(),
        )]),
    );
    let error = schema_coercion_plan_from_reconciled_schema(&malformed).unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(error.to_string().contains("not a valid coercion plan"));

    let observed = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let constraint = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
    let mut type_policy = ContractPolicy::default().types;
    type_policy.allow_lossy_mapping = true;
    let reconciliation = reconcile_schema(&observed, &constraint, &type_policy).unwrap();
    let mut false_plan = reconciliation.plan;
    false_plan.fields[0].decision = FieldCoercionDecision::Widened;
    false_plan.fields[0].reason = "lossless widening from Int64 to Int32".to_owned();
    let false_schema = Schema::new_with_metadata(
        reconciliation.schema.fields().clone(),
        HashMap::from([(
            "cdf:schema_coercion_plan".to_owned(),
            serde_json::to_string(&false_plan).unwrap(),
        )]),
    );

    let error = schema_coercion_plan_from_reconciled_schema(&false_schema).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("inconsistent with Int64 -> Int32")
    );
}

#[test]
fn source_schema_coercion_metadata_requires_trusted_batch_evidence() {
    let source_carried = Schema::new_with_metadata(
        vec![Field::new("id", DataType::Int64, false)],
        HashMap::from([(
            "cdf:schema_coercion_plan".to_owned(),
            serde_json::json!({
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
            .to_string(),
        )]),
    );

    let error = reject_untrusted_schema_coercion_metadata(&source_carried).unwrap_err();
    assert!(error.to_string().contains("without trusted batch evidence"));
}

#[test]
fn trusted_batch_coercion_evidence_requires_matching_reserved_schema_metadata() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let header_only = serde_json::json!({
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

    let error = schema_coercion_plan_from_trusted_json(&schema, &header_only).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("has no matching reserved Arrow schema metadata")
    );
}

#[test]
fn schema_reconciliation_keeps_string_parse_coercions_opt_in() {
    let observed = Schema::new(vec![Field::new("created_at", DataType::Utf8, false)]);
    let constraint = Schema::new(vec![Field::new(
        "created_at",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        false,
    )]);
    let mut type_policy = ContractPolicy::default().types;
    type_policy.coerce_types = false;

    let denied = plan_schema_reconciliation(&observed, &constraint, &type_policy).unwrap();
    assert_eq!(
        decision_for(&denied.plan, "created_at").decision,
        FieldCoercionDecision::LossyRejected
    );
    let error = denied.into_result().unwrap_err().to_string();
    assert!(error.contains("enable coerce_types"));

    type_policy.coerce_types = true;
    let allowed = reconcile_schema(&observed, &constraint, &type_policy).unwrap();
    assert_eq!(
        decision_for(&allowed.plan, "created_at").decision,
        FieldCoercionDecision::CoercedByPolicy
    );
    assert_eq!(
        physical_type(allowed.schema.field_with_name("created_at").unwrap()),
        Some("Utf8")
    );
}

#[test]
fn schema_reconciliation_reports_unsupported_mappings() {
    let observed = Schema::new(vec![Field::new_struct(
        "payload",
        vec![Field::new("id", DataType::Int64, false)],
        false,
    )]);
    let constraint = Schema::new(vec![Field::new("payload", DataType::Int64, false)]);

    let report =
        plan_schema_reconciliation(&observed, &constraint, &ContractPolicy::default().types)
            .unwrap();

    assert_eq!(
        decision_for(&report.plan, "payload").decision,
        FieldCoercionDecision::Unsupported
    );
    let error = report.into_result().unwrap_err().to_string();
    assert!(error.contains("unsupported schema reconciliation"));
    assert!(error.contains("declared type Int64"));
}

fn decision_for<'a>(plan: &'a SchemaCoercionPlan, source_name: &str) -> &'a FieldCoercion {
    plan.fields
        .iter()
        .find(|field| field.source_name == source_name)
        .unwrap()
}

#[test]
fn shared_coercion_materializer_widens_projects_and_materializes_missing_nulls() {
    let observed_schema = Arc::new(Schema::new(vec![
        Field::new("source_id", DataType::Int32, false),
        Field::new("ignored", DataType::Utf8, true),
    ]));
    let observed = RecordBatch::try_new(
        Arc::clone(&observed_schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
        ],
    )
    .unwrap();
    let constraint = Schema::new(vec![
        Field::new("source_id", DataType::Int64, false),
        Field::new("optional", DataType::Utf8, true),
    ]);
    let reconciliation = reconcile_schema(
        &observed_schema,
        &constraint,
        &TypePolicy::strict_fidelity(),
    )
    .unwrap();
    let materialized =
        materialize_schema_coercion(&observed, &constraint, &reconciliation.plan).unwrap();
    assert_eq!(materialized.schema().as_ref(), &reconciliation.schema);
    assert_eq!(materialized.num_columns(), 2);
    assert_eq!(
        materialized
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[1, 2]
    );
    assert_eq!(materialized.column(1).null_count(), 2);
}
