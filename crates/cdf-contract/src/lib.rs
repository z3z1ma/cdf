#![doc = "Schema contract compilation boundary for cdf."]

mod aggregation;
mod compiler;
mod correction;
mod dedup_key;
mod evaluator;
mod expression;
mod lattice;
mod normalization;
mod policy;
mod program;
mod reconciliation;
mod residual;
mod schema;
mod transforms;
mod vector;

pub use aggregation::{
    AggregateFieldDecision, AggregateFieldSchemaVerdict, AggregateFileSchemaVerdict,
    AggregateMetadataVariance, AggregateSchemaCandidate, AggregateSchemaIncompatibility,
    AggregateSchemaJoin, AggregateSchemaJoinReport, aggregate_arrow_schemas,
    plan_aggregate_arrow_schema_join,
};
pub use compiler::{
    TypeMappingDecision, bind_validation_program_to_resource, compile_resource_validation_program,
    compile_validation_program, redaction_decision_for_field, redaction_decision_for_semantic,
    resolve_destination_type_mapping, validate_destination_schema_mappings, validate_type_mapping,
};
pub use correction::{
    correction_operations_digest, decode_destination_correction_value,
    validate_destination_correction_commit_request,
};
pub use evaluator::{
    ContractBatchEvaluation, ContractEvaluationContext, DedupDroppedRow, DedupSummary,
    PackageDedupEvaluation, PackageDedupRuleSpec, QuarantineCandidate, RedactedObservedValue,
    RuleVerdictSummary, VerdictSummary, encode_package_dedup_keys, evaluate_package_order_dedup,
    evaluate_record_batch, package_dedup_rule,
};
pub use expression::{
    CDF_FUNCTION_NAMESPACE, CDF_FUNCTION_VERSION, COMPILED_EXPRESSION_PLAN_VERSION,
    CompiledExpressionPlan, DATAFUSION_EXPRESSION_OPTIMIZER, DATAFUSION_EXPRESSION_PIN,
    EXPRESSION_IR_VERSION, Expression, ExpressionFidelity, ExpressionLint, ExpressionLintCode,
    ExpressionLiteral, ExpressionNode, ExpressionUse, FunctionReference, NATIVE_CONTRACT_OPTIMIZER,
    NATIVE_FILTER_LOWERING_VERSION, OptimizerIdentity, PlannedExpression,
    SOURCE_EXACT_PUSHDOWN_OPTIMIZER,
};
pub use lattice::assert_verdict_lattice_total;
pub use normalization::{
    NormalizedField, NormalizedSchema, normalize_arrow_schema, normalize_identifier,
    normalize_schema,
};
pub use policy::{
    ContractPolicy, DedupKeep, IdentifierCharset, IdentifierPolicy, LineagePolicy,
    NORMALIZER_NAMECASE_V1, NestedDataPolicy, NormalizationPolicy, PiiRedactionPolicy,
    ProfilingPolicy, PromotionPolicy, QuarantinePolicy, RedactionDecision, RetentionClass,
    RowPolicy, RowRule, SchemaEvolutionMode, SchemaPolicy, TransformDescription, TypePolicy,
    VARIANT_COLUMN_NAME, VARIANT_SEMANTIC_TAG, ValidationDepth, VariantColumnSpec, VerdictAction,
    VerdictPolicy, identifier_policy_from_destination_rules, is_framework_variant_field,
};
pub(crate) use program::NativeRowRule;
pub use program::{
    AnomalyFact, ColumnProgram, ColumnProgramStep, CompileWarning, DedupKeepProgram,
    MissingColumnBehavior, NestedAction, ResidualCandidateVerdict, ResidualCaptureOutput,
    ResidualFieldProgram, ResidualProgram, RowDispositionKind, RowDispositionRule, RowRuleProgram,
    RuleDisposition, RuleOutcome, SchemaChangeKind, SchemaVerdictRule,
    ValidationDepthTransitionEvent, ValidationProgram, ValidationTransitionTrigger,
};
pub use reconciliation::{
    FieldCoercion, FieldCoercionDecision, SCHEMA_COERCION_PLAN_METADATA_KEY, SchemaCoercionPlan,
    SchemaReconciliation, SchemaReconciliationError, SchemaReconciliationReport,
    is_lossless_type_widening, materialize_schema_coercion, plan_schema_reconciliation,
    reconcile_schema, reject_untrusted_schema_coercion_metadata,
    schema_coercion_plan_from_reconciled_schema, schema_coercion_plan_from_trusted_json,
};
pub use residual::{
    CanonicalArrowDateUnit, CanonicalArrowField, CanonicalArrowIntervalUnit,
    CanonicalArrowTimeUnit, CanonicalArrowType, CanonicalArrowUnionField, CanonicalArrowUnionMode,
    DecodedResidualField, RESIDUAL_ENCODE_UNSUPPORTED_CODE, RESIDUAL_ENCODING_METADATA_KEY,
    RESIDUAL_ENCODING_NAME, RESIDUAL_JSON_V1, ResidualArrowField, ResidualArrowType,
    ResidualCodecError, ResidualDateUnit, ResidualFieldRef, ResidualFieldWithRedaction,
    ResidualIntervalUnit, ResidualTimeUnit, ResidualUnionField, ResidualUnionMode,
    arrow_value_to_canonical_json, decode_residual_json_v1, encode_residual_json_v1,
    encode_residual_json_v1_redacted, remove_residual_json_v1_path, residual_json_pointer,
};
pub use schema::{
    ArrowType, ObservedField, ObservedSchema, SourceTypeClaim, TimeUnitName, TimestampZoneClaim,
};
pub use vector::{
    VectorMaskEvaluation, VectorRuleMask, VectorSelectionEvaluation, VectorValidationEvaluator,
    VectorValidationPlan, bind_vector_validation_plan, range_bounds_are_unsatisfiable,
};

#[cfg(test)]
mod tests;
