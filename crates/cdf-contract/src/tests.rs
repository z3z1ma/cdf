use super::*;
use std::collections::BTreeMap;

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use cdf_kernel::{
    TrustLevel, TypeMapping, TypeMappingFidelity, source_name, with_semantic, with_source_name,
};

#[test]
fn validation_program_serializes_and_has_total_lattice() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let observed = ObservedSchema::from_arrow(&schema);
    let program =
        compile_validation_program(&ContractPolicy::for_trust(TrustLevel::Governed), &observed)
            .unwrap();

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
    assert_eq!(
        program,
        serde_json::from_str::<ValidationProgram>(&json).unwrap()
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
        max_length: 20,
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
