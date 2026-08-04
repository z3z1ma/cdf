use std::{
    hint::black_box,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Int8Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray,
};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::filter::filter_record_batch;
use cdf_contract::{
    DATAFUSION_EXPRESSION_PIN, DATAFUSION_SCALAR_CONFIG_IDENTITY, DATAFUSION_SCALAR_FEATURE_SET,
    ScalarCastMode, ScalarExpression, ScalarExpressionKind,
};
use cdf_memory::{
    ConsumerKey, DeterministicMemoryCoordinator, MemoryClass, MemoryCoordinator, MemoryLease,
    ReservationRequest,
};
use cdf_runtime::RunCancellation;
use datafusion::{
    common::DFSchema,
    execution::SessionStateDefaults,
    logical_expr::{
        Cast, ColumnarValue, Expr, TryCast, Volatility, create_udf,
        execution_props::ExecutionProps, simplify::SimplifyContext,
    },
    optimizer::simplify_expressions::ExprSimplifier,
    physical_expr::{PhysicalExpr, create_physical_expr},
};
use proptest::prelude::*;

use crate::{
    AnalyzedProjectionExpression, AnalyzedScalarExpression, ExpressionSourceLocation,
    bind_relational_expression_plan, compile_relational_expression_plan,
    expression_execution::{bind_scalar_expression, evaluate_bound_scalar},
    lower_analyzed_scalar_expression,
};

fn expression_memory(_batch: &RecordBatch) -> MemoryLease {
    // The coordinator accounts authority, not resident allocation. Generic variable-width
    // functions reserve their canonical Arrow maximum before execution, so this focused harness
    // supplies enough virtual authority while the actual arrays remain deliberately tiny.
    let bytes = 1_u64 << 50;
    let coordinator = DeterministicMemoryCoordinator::new(bytes, Default::default()).unwrap();
    coordinator
        .try_reserve(
            &ReservationRequest::new(
                ConsumerKey::new("expression-test", MemoryClass::Transform).unwrap(),
                bytes,
            )
            .unwrap(),
        )
        .unwrap()
        .unwrap()
}

fn execute_scalar_expression(
    expression: &ScalarExpression,
    batch: &RecordBatch,
    cancellation: &RunCancellation,
) -> cdf_kernel::Result<ArrayRef> {
    let memory = expression_memory(batch);
    crate::expression_execution::execute_scalar_expression(expression, batch, &memory, cancellation)
}

fn execute_relational_expression_plan(
    plan: &cdf_contract::RelationalExpressionPlan,
    batch: &RecordBatch,
    cancellation: &RunCancellation,
) -> cdf_kernel::Result<RecordBatch> {
    let memory = expression_memory(batch);
    crate::expression_execution::execute_relational_expression_plan(
        plan,
        batch,
        &memory,
        cancellation,
    )
}

fn execute_bound_relational_expression_plan(
    plan: &crate::BoundRelationalExpressionPlan,
    batch: &RecordBatch,
    cancellation: &RunCancellation,
) -> cdf_kernel::Result<RecordBatch> {
    let memory = expression_memory(batch);
    crate::expression_execution::execute_bound_relational_expression_plan(
        plan,
        batch,
        &memory,
        cancellation,
    )
}

fn builtin(name: &str) -> Arc<datafusion::logical_expr::ScalarUDF> {
    SessionStateDefaults::default_scalar_functions()
        .into_iter()
        .find(|function| {
            function.name() == name || function.aliases().iter().any(|alias| alias == name)
        })
        .unwrap_or_else(|| panic!("missing pinned DataFusion function {name:?}"))
}

fn analyzed(expression: Expr, schema: &Schema) -> Expr {
    let schema = Arc::new(DFSchema::try_from(schema.clone()).unwrap());
    let context = SimplifyContext::builder()
        .with_schema(Arc::clone(&schema))
        .build();
    ExprSimplifier::new(context)
        .coerce(expression, schema.as_ref())
        .unwrap()
}

fn direct_physical(expression: &Expr, schema: &Schema) -> Arc<dyn PhysicalExpr> {
    let schema = DFSchema::try_from(schema.clone()).unwrap();
    create_physical_expr(expression, &schema, &ExecutionProps::new()).unwrap()
}

fn assert_datafusion_equivalent(
    expression: Expr,
    schema: &Schema,
    batch: &RecordBatch,
) -> ScalarExpression {
    let expression = analyzed(expression, schema);
    let expected = direct_physical(&expression, schema)
        .evaluate(batch)
        .unwrap()
        .into_array(batch.num_rows())
        .unwrap();
    let compiled =
        lower_analyzed_scalar_expression(&AnalyzedScalarExpression::new(expression), schema)
            .unwrap();
    let actual = execute_scalar_expression(&compiled, batch, &RunCancellation::default()).unwrap();
    assert_eq!(actual.data_type(), expected.data_type());
    assert_eq!(actual.to_data(), expected.to_data());
    compiled
}

#[test]
fn scalar_function_families_match_datafusion_exactly() {
    let numeric_schema = Schema::new(vec![Field::new("number", DataType::Int64, true)]);
    let numeric_batch = RecordBatch::try_new(
        Arc::new(numeric_schema.clone()),
        vec![Arc::new(Int64Array::from(vec![Some(-9), None, Some(4)]))],
    )
    .unwrap();
    assert_datafusion_equivalent(
        builtin("abs").call(vec![datafusion::logical_expr::col("number")]),
        &numeric_schema,
        &numeric_batch,
    );

    let text_schema = Schema::new(vec![Field::new("text", DataType::Utf8, true)]);
    let text_batch = RecordBatch::try_new(
        Arc::new(text_schema.clone()),
        vec![Arc::new(StringArray::from(vec![
            Some("Alpha,BETA"),
            None,
            Some("gamma"),
        ]))],
    )
    .unwrap();
    assert_datafusion_equivalent(
        builtin("lower").call(vec![datafusion::logical_expr::col("text")]),
        &text_schema,
        &text_batch,
    );
    assert_datafusion_equivalent(
        builtin("string_to_array").call(vec![
            datafusion::logical_expr::col("text"),
            datafusion::logical_expr::lit(","),
        ]),
        &text_schema,
        &text_batch,
    );
    assert_datafusion_equivalent(
        builtin("nullif").call(vec![
            datafusion::logical_expr::col("text"),
            datafusion::logical_expr::lit("gamma"),
        ]),
        &text_schema,
        &text_batch,
    );

    let binary_schema = Schema::new(vec![Field::new("bytes", DataType::Binary, true)]);
    let binary_batch = RecordBatch::try_new(
        Arc::new(binary_schema.clone()),
        vec![Arc::new(BinaryArray::from(vec![
            Some(b"cdf".as_slice()),
            None,
            Some(b"fast".as_slice()),
        ]))],
    )
    .unwrap();
    assert_datafusion_equivalent(
        builtin("encode").call(vec![
            datafusion::logical_expr::col("bytes"),
            datafusion::logical_expr::lit("hex"),
        ]),
        &binary_schema,
        &binary_batch,
    );

    let temporal_schema = Schema::new(vec![Field::new(
        "observed_at",
        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
        true,
    )]);
    let temporal_batch = RecordBatch::try_new(
        Arc::new(temporal_schema.clone()),
        vec![Arc::new(TimestampMillisecondArray::from(vec![
            Some(0),
            None,
            Some(1_735_689_600_000),
        ]))],
    )
    .unwrap();
    assert_datafusion_equivalent(
        builtin("date_part").call(vec![
            datafusion::logical_expr::lit("year"),
            datafusion::logical_expr::col("observed_at"),
        ]),
        &temporal_schema,
        &temporal_batch,
    );
}

#[test]
fn admission_uses_registry_identity_and_immutable_volatility() {
    let empty = Schema::empty();
    for name in ["random", "current_date"] {
        let error = lower_analyzed_scalar_expression(
            &AnalyzedScalarExpression::new(builtin(name).call(Vec::new())),
            &empty,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("volatility"),
            "unexpected {name} rejection: {error}"
        );
    }

    let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
    let custom = create_udf(
        "custom_identity",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|arguments: &[ColumnarValue]| Ok(arguments[0].clone())),
    );
    let error = lower_analyzed_scalar_expression(
        &AnalyzedScalarExpression::new(custom.call(vec![datafusion::logical_expr::col("value")])),
        &schema,
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("not the pinned DataFusion built-in")
    );
}

#[test]
fn pre_simplification_admission_cannot_launder_stable_functions() {
    let source = builtin("current_date").call(Vec::new());
    let optimized = datafusion::logical_expr::lit(20_000_i32);
    let error = lower_analyzed_scalar_expression(
        &AnalyzedScalarExpression::new(optimized)
            .with_admission_expression(source)
            .with_source_location(
                Vec::new(),
                ExpressionSourceLocation::new("sources/postgres/orders.cdf.sql", 7, 19).unwrap(),
            ),
        &Schema::empty(),
    )
    .unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
    assert!(error.message.contains("volatility is Stable"));
    assert!(
        error
            .message
            .contains("sources/postgres/orders.cdf.sql:7:19")
    );
}

#[test]
fn nested_admission_failure_retains_exact_node_location() {
    let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
    let custom = create_udf(
        "custom_identity",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|arguments: &[ColumnarValue]| Ok(arguments[0].clone())),
    );
    let expression = datafusion::logical_expr::col("value")
        + custom.call(vec![datafusion::logical_expr::col("value")]);
    let error = lower_analyzed_scalar_expression(
        &AnalyzedScalarExpression::new(expression).with_source_location(
            vec![1],
            ExpressionSourceLocation::new("sources/postgres/orders.cdf.sql", 11, 9).unwrap(),
        ),
        &schema,
    )
    .unwrap_err();
    assert!(error.message.contains("not the pinned DataFusion built-in"));
    assert!(error.message.contains("orders.cdf.sql:11:9"));
    assert!(error.message.contains("expression path 1"));
}

#[test]
fn aliases_lower_to_canonical_function_identity_and_stable_bytes() {
    let function = builtin("array_length");
    let alias = builtin("list_length");
    assert_eq!(function.name(), alias.name());
    let schema = Schema::new(vec![Field::new("text", DataType::Utf8, true)]);
    let source = builtin("string_to_array").call(vec![
        datafusion::logical_expr::col("text"),
        datafusion::logical_expr::lit(","),
    ]);
    let array = analyzed(source, &schema);
    let canonical_expression = analyzed(function.call(vec![array.clone()]), &schema);
    let alias_expression = analyzed(alias.call(vec![array]), &schema);
    let first = lower_analyzed_scalar_expression(
        &AnalyzedScalarExpression::new(canonical_expression),
        &schema,
    )
    .unwrap();
    let second =
        lower_analyzed_scalar_expression(&AnalyzedScalarExpression::new(alias_expression), &schema)
            .unwrap();
    assert_eq!(first, second);
    assert_eq!(
        serde_json::to_vec(&first).unwrap(),
        serde_json::to_vec(&second).unwrap()
    );
    assert_eq!(first.content_sha256, second.content_sha256);
    assert!(first.function_dependencies().iter().any(|dependency| {
        dependency.canonical_name == "array_length"
            && dependency.implementation_version == DATAFUSION_EXPRESSION_PIN
            && dependency.feature_set == DATAFUSION_SCALAR_FEATURE_SET
            && dependency.config_identity == DATAFUSION_SCALAR_CONFIG_IDENTITY
    }));
    let mut tampered = first;
    tampered.datafusion_feature_set.push_str(";tampered");
    assert!(
        tampered
            .validate()
            .unwrap_err()
            .to_string()
            .contains("stale")
    );
}

#[test]
fn cast_modes_preserve_success_null_and_error_behavior() {
    let schema = Schema::new(vec![Field::new("raw", DataType::Utf8, true)]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(StringArray::from(vec![
            Some("42"),
            Some("bad"),
            None,
        ]))],
    )
    .unwrap();
    let cast = Expr::Cast(Cast::new(
        Box::new(datafusion::logical_expr::col("raw")),
        DataType::Int64,
    ));
    let explicit = lower_analyzed_scalar_expression(
        &AnalyzedScalarExpression::new(cast.clone()).with_explicit_cast(Vec::new()),
        &schema,
    )
    .unwrap();
    let implicit =
        lower_analyzed_scalar_expression(&AnalyzedScalarExpression::new(cast.clone()), &schema)
            .unwrap();
    assert!(matches!(
        explicit.root.expression,
        ScalarExpressionKind::Cast {
            mode: ScalarCastMode::Explicit,
            ..
        }
    ));
    assert!(matches!(
        implicit.root.expression,
        ScalarExpressionKind::Cast {
            mode: ScalarCastMode::Implicit,
            ..
        }
    ));
    let direct_error = direct_physical(&cast, &schema)
        .evaluate(&batch)
        .unwrap_err();
    let cdf_error =
        execute_scalar_expression(&explicit, &batch, &RunCancellation::default()).unwrap_err();
    assert!(matches!(
        direct_error.find_root(),
        datafusion::common::DataFusionError::ArrowError(_, _)
            | datafusion::common::DataFusionError::Execution(_)
    ));
    assert_eq!(cdf_error.kind, cdf_kernel::ErrorKind::Data);
    assert!(cdf_error.message.contains("DataFusion scalar execution"));
    assert!(cdf_error.message.contains("(arrow)") || cdf_error.message.contains("(execution)"));

    let try_cast = Expr::TryCast(TryCast::new(
        Box::new(datafusion::logical_expr::col("raw")),
        DataType::Int64,
    ));
    let compiled = assert_datafusion_equivalent(try_cast, &schema, &batch);
    assert!(matches!(
        compiled.root.expression,
        ScalarExpressionKind::Cast {
            mode: ScalarCastMode::Try,
            ..
        }
    ));
    let output = execute_scalar_expression(&compiled, &batch, &RunCancellation::default()).unwrap();
    assert_eq!(
        output.as_any().downcast_ref::<Int64Array>().unwrap(),
        &Int64Array::from(vec![Some(42), None, None])
    );

    let narrow_schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
    let narrow_batch = RecordBatch::try_new(
        Arc::new(narrow_schema.clone()),
        vec![Arc::new(Int64Array::from(vec![127, 128, -129]))],
    )
    .unwrap();
    let narrow = Expr::Cast(Cast::new(
        Box::new(datafusion::logical_expr::col("value")),
        DataType::Int8,
    ));
    let explicit_narrow = lower_analyzed_scalar_expression(
        &AnalyzedScalarExpression::new(narrow).with_explicit_cast(Vec::new()),
        &narrow_schema,
    )
    .unwrap();
    assert_eq!(
        execute_scalar_expression(
            &explicit_narrow,
            &narrow_batch.slice(0, 1),
            &RunCancellation::default(),
        )
        .unwrap()
        .as_any()
        .downcast_ref::<Int8Array>()
        .unwrap(),
        &Int8Array::from(vec![127])
    );
    let overflow =
        execute_scalar_expression(&explicit_narrow, &narrow_batch, &RunCancellation::default())
            .unwrap_err();
    assert_eq!(overflow.kind, cdf_kernel::ErrorKind::Data);

    let try_narrow = Expr::TryCast(TryCast::new(
        Box::new(datafusion::logical_expr::col("value")),
        DataType::Int8,
    ));
    let try_narrow = assert_datafusion_equivalent(try_narrow, &narrow_schema, &narrow_batch);
    let values =
        execute_scalar_expression(&try_narrow, &narrow_batch, &RunCancellation::default()).unwrap();
    assert_eq!(
        values.as_any().downcast_ref::<Int8Array>().unwrap(),
        &Int8Array::from(vec![Some(127), None, None])
    );
}

#[test]
fn explicit_cast_provenance_must_name_an_actual_cast_node() {
    let error = lower_analyzed_scalar_expression(
        &AnalyzedScalarExpression::new(datafusion::logical_expr::lit(1_i64))
            .with_explicit_cast(Vec::new())
            .with_source_location(
                Vec::new(),
                ExpressionSourceLocation::new("sources/postgres/orders.cdf.sql", 5, 12).unwrap(),
            ),
        &Schema::empty(),
    )
    .unwrap_err();
    assert!(
        error
            .message
            .contains("does not identify a resolved CAST node")
    );
    assert!(error.message.contains("orders.cdf.sql:5:12"));
}

#[test]
fn datafusion_error_ownership_preserves_typed_sources_and_phase() {
    let embedded = cdf_kernel::CdfError::rate_limited("provider throttle", Some(275));
    let mapped = crate::expression::classify_datafusion_error(
        datafusion::common::DataFusionError::External(Box::new(embedded)),
        crate::expression::DataFusionErrorPhase::Execution,
    );
    assert_eq!(mapped.kind, cdf_kernel::ErrorKind::RateLimited);
    assert_eq!(mapped.retry_after_ms, Some(275));
    assert!(mapped.message.contains("provider throttle"));

    let nested = std::io::Error::other(std::io::Error::other(cdf_kernel::CdfError::auth(
        "nested credential owner",
    )));
    let nested = crate::expression::classify_datafusion_error(
        datafusion::common::DataFusionError::External(Box::new(nested)),
        crate::expression::DataFusionErrorPhase::Execution,
    );
    assert_eq!(nested.kind, cdf_kernel::ErrorKind::Auth);
    assert!(nested.message.contains("nested credential owner"));

    let exhausted = crate::expression::classify_datafusion_error(
        datafusion::common::DataFusionError::ResourcesExhausted("allocator".to_owned()),
        crate::expression::DataFusionErrorPhase::Execution,
    );
    assert_eq!(exhausted.kind, cdf_kernel::ErrorKind::Environment);
    assert!(exhausted.message.contains("resources_exhausted"));

    let malformed = crate::expression::classify_datafusion_error(
        datafusion::common::DataFusionError::Execution("invalid cast value".to_owned()),
        crate::expression::DataFusionErrorPhase::Execution,
    );
    assert_eq!(malformed.kind, cdf_kernel::ErrorKind::Data);
    assert!(malformed.message.contains("(execution)"));

    let invariant = crate::expression::classify_datafusion_error(
        datafusion::common::DataFusionError::Internal("broken invariant".to_owned()),
        crate::expression::DataFusionErrorPhase::Binding,
    );
    assert_eq!(invariant.kind, cdf_kernel::ErrorKind::Internal);
    assert!(invariant.message.contains("(internal)"));
}

#[test]
fn public_expression_execution_requires_preacquired_cdf_memory() {
    let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let expression = lower_analyzed_scalar_expression(
        &AnalyzedScalarExpression::new(datafusion::logical_expr::col("value")),
        &schema,
    )
    .unwrap();
    let coordinator = DeterministicMemoryCoordinator::new(1, Default::default()).unwrap();
    let lease = coordinator
        .try_reserve(
            &ReservationRequest::new(
                ConsumerKey::new("expression-too-small", MemoryClass::Transform).unwrap(),
                1,
            )
            .unwrap(),
        )
        .unwrap()
        .unwrap();
    let error =
        crate::execute_scalar_expression(&expression, &batch, &lease, &RunCancellation::default())
            .unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Environment);
    assert!(error.message.contains("pre-acquired CDF memory lease"));
}

#[test]
fn expanding_scalar_and_prebound_plan_fail_before_an_undersized_lease() {
    let schema = Schema::new(vec![Field::new("text", DataType::Utf8, false)]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(StringArray::from(vec!["cdf"]))],
    )
    .unwrap();
    let repeat = analyzed(
        builtin("repeat").call(vec![
            datafusion::logical_expr::col("text"),
            datafusion::logical_expr::lit(1_000_000_i64),
        ]),
        &schema,
    );
    let expression =
        lower_analyzed_scalar_expression(&AnalyzedScalarExpression::new(repeat.clone()), &schema)
            .unwrap();
    let coordinator = DeterministicMemoryCoordinator::new(1_048_576, Default::default()).unwrap();
    let lease = coordinator
        .try_reserve(
            &ReservationRequest::new(
                ConsumerKey::new("expanding-expression", MemoryClass::Transform).unwrap(),
                1_048_576,
            )
            .unwrap(),
        )
        .unwrap()
        .unwrap();

    let scalar_error =
        crate::execute_scalar_expression(&expression, &batch, &lease, &RunCancellation::default())
            .unwrap_err();
    assert_eq!(scalar_error.kind, cdf_kernel::ErrorKind::Environment);
    assert!(
        scalar_error
            .message
            .contains("pre-acquired CDF memory lease")
    );

    let plan = compile_relational_expression_plan(
        &schema,
        None,
        vec![AnalyzedProjectionExpression {
            name: "expanded".to_owned(),
            scalar: AnalyzedScalarExpression::new(repeat),
        }],
        Vec::new(),
    )
    .unwrap();
    let bound = bind_relational_expression_plan(&plan).unwrap();
    let plan_error = crate::expression_execution::execute_bound_relational_expression_plan(
        &bound,
        &batch,
        &lease,
        &RunCancellation::default(),
    )
    .unwrap_err();
    assert_eq!(plan_error.kind, cdf_kernel::ErrorKind::Environment);
    assert!(plan_error.message.contains("pre-acquired CDF memory lease"));
}

#[test]
fn relational_plan_filters_before_projection_preserves_control_and_reloads_exactly() {
    let semantic_metadata = std::collections::HashMap::from([(
        "cdf:semantic".to_owned(),
        "urn:cdf:semantic:currency@1?unit=USD".to_owned(),
    )]);
    let schema = Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::Int64, false).with_metadata(semantic_metadata.clone()),
            Field::new("keep", DataType::Boolean, true),
            Field::new("_cdf_op", DataType::Utf8, false).with_metadata(semantic_metadata.clone()),
        ],
        std::collections::HashMap::from([("schema-authority".to_owned(), "v1".to_owned())]),
    );
    let plan = compile_relational_expression_plan(
        &schema,
        Some(AnalyzedScalarExpression::new(
            datafusion::logical_expr::col("keep"),
        )),
        vec![
            AnalyzedProjectionExpression {
                name: "id".to_owned(),
                scalar: AnalyzedScalarExpression::new(datafusion::logical_expr::col("id")),
            },
            AnalyzedProjectionExpression {
                name: "_cdf_op".to_owned(),
                scalar: AnalyzedScalarExpression::new(datafusion::logical_expr::col("_cdf_op")),
            },
        ],
        vec!["_cdf_op".to_owned()],
    )
    .unwrap();
    let encoded = serde_json::to_vec(&plan).unwrap();
    let loaded = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(plan, loaded);
    assert_eq!(encoded, serde_json::to_vec(&loaded).unwrap());
    let loaded_schema = loaded.output_schema.to_arrow().unwrap();
    assert_eq!(loaded_schema.metadata(), schema.metadata());
    assert_eq!(loaded_schema.field(0).metadata(), &semantic_metadata);
    assert_eq!(loaded_schema.field(1).metadata(), &semantic_metadata);
    let mut metadata_stripped = loaded.output_schema.clone();
    metadata_stripped.fields[1].metadata.clear();
    let metadata_error = cdf_contract::RelationalExpressionPlan::current(
        loaded.input_schema.clone(),
        loaded.filter.clone(),
        loaded.projection.clone(),
        metadata_stripped,
        loaded.control_fields.clone(),
    )
    .unwrap_err();
    assert!(metadata_error.message.contains("metadata"));

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
            Arc::new(StringArray::from(vec!["upsert", "delete", "upsert"])),
        ],
    )
    .unwrap();
    let bound = bind_relational_expression_plan(&loaded).unwrap();
    let output =
        execute_bound_relational_expression_plan(&bound, &batch, &RunCancellation::default())
            .unwrap();
    assert_eq!(output.num_rows(), 1);
    assert_eq!(
        output
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap(),
        &Int64Array::from(vec![10])
    );
    assert_eq!(
        output
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap(),
        &StringArray::from(vec!["upsert"])
    );
}

#[test]
fn relational_plan_rejects_collisions_empty_projection_and_control_rewrites() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("_cdf_op", DataType::Utf8, false),
    ]);
    let column = |name: &str| AnalyzedProjectionExpression {
        name: name.to_owned(),
        scalar: AnalyzedScalarExpression::new(datafusion::logical_expr::col(name)),
    };
    assert!(
        compile_relational_expression_plan(&schema, None, Vec::new(), Vec::new())
            .unwrap_err()
            .to_string()
            .contains("cannot be empty")
    );
    assert!(
        compile_relational_expression_plan(
            &schema,
            None,
            vec![column("id"), column("id")],
            Vec::new(),
        )
        .unwrap_err()
        .to_string()
        .contains("inconsistent name")
    );
    let rewritten_control = AnalyzedProjectionExpression {
        name: "_cdf_op".to_owned(),
        scalar: AnalyzedScalarExpression::new(datafusion::logical_expr::col("id")),
    };
    assert!(
        compile_relational_expression_plan(
            &schema,
            None,
            vec![column("id"), rewritten_control],
            vec!["_cdf_op".to_owned()],
        )
        .unwrap_err()
        .to_string()
        .contains("changed identity, type, or metadata")
    );
}

#[test]
fn pass_through_projection_is_zero_copy_and_stale_identity_fails_closed() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let mut plan = compile_relational_expression_plan(
        &schema,
        None,
        vec![AnalyzedProjectionExpression {
            name: "id".to_owned(),
            scalar: AnalyzedScalarExpression::new(datafusion::logical_expr::col("id")),
        }],
        Vec::new(),
    )
    .unwrap();
    let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::clone(&values)]).unwrap();
    let output =
        execute_relational_expression_plan(&plan, &batch, &RunCancellation::default()).unwrap();
    assert!(Arc::ptr_eq(output.column(0), &values));

    plan.datafusion_version = "53.0.0".to_owned();
    let error = match bind_relational_expression_plan(&plan) {
        Ok(_) => panic!("stale relational plan unexpectedly bound"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("cdf compile"));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn arithmetic_graph_matches_datafusion_for_generated_batches(
        values in proptest::collection::vec(-1_000_000_i64..1_000_000, 1..128),
        delta in -10_000_i64..10_000,
    ) {
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int64Array::from(values))],
        ).unwrap();
        let expression = datafusion::logical_expr::col("value")
            + datafusion::logical_expr::lit(delta);
        assert_datafusion_equivalent(expression, &schema, &batch);
    }

    #[test]
    fn filter_then_projection_matches_generated_sql_null_semantics(
        rows in proptest::collection::vec(
            (-1_000_000_i64..1_000_000, proptest::option::of(any::<bool>())),
            1..128,
        ),
    ) {
        let schema = Schema::new(vec![
            Field::new("value", DataType::Int64, false),
            Field::new("keep", DataType::Boolean, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
                Arc::new(BooleanArray::from(rows.iter().map(|row| row.1).collect::<Vec<_>>())),
            ],
        ).unwrap();
        let plan = compile_relational_expression_plan(
            &schema,
            Some(AnalyzedScalarExpression::new(datafusion::logical_expr::col("keep"))),
            vec![AnalyzedProjectionExpression {
                name: "adjusted".to_owned(),
                scalar: AnalyzedScalarExpression::new(
                    datafusion::logical_expr::col("value") + datafusion::logical_expr::lit(1_i64),
                ),
            }],
            Vec::new(),
        ).unwrap();
        let output = execute_relational_expression_plan(
            &plan,
            &batch,
            &RunCancellation::default(),
        ).unwrap();
        let expected = rows
            .iter()
            .filter(|row| row.1 == Some(true))
            .map(|row| row.0 + 1)
            .collect::<Vec<_>>();
        prop_assert_eq!(
            output.column(0).as_any().downcast_ref::<Int64Array>().unwrap(),
            &Int64Array::from(expected),
        );
    }
}

#[test]
fn bound_scalar_wrapper_stays_within_execution_roofline() {
    const ROWS: usize = 1_000_000;
    const ITERATIONS: usize = 8;
    let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int64Array::from_iter_values(
            (0..ROWS).map(|value| -(value as i64)),
        ))],
    )
    .unwrap();
    let logical = analyzed(
        builtin("abs").call(vec![datafusion::logical_expr::col("value")]),
        &schema,
    );
    let direct = direct_physical(&logical, &schema);
    let compiled =
        lower_analyzed_scalar_expression(&AnalyzedScalarExpression::new(logical), &schema).unwrap();
    let bound = bind_scalar_expression(&compiled, &schema).unwrap();
    let cancellation = RunCancellation::default();

    for _ in 0..2 {
        black_box(
            direct
                .evaluate(&batch)
                .unwrap()
                .into_array(batch.num_rows())
                .unwrap(),
        );
        black_box(evaluate_bound_scalar(&batch, &bound, None, &cancellation).unwrap());
    }
    let measure = |mut operation: Box<dyn FnMut()>| {
        let started = Instant::now();
        for _ in 0..ITERATIONS {
            operation();
        }
        started.elapsed()
    };
    let direct_time = measure(Box::new(|| {
        black_box(
            direct
                .evaluate(&batch)
                .unwrap()
                .into_array(batch.num_rows())
                .unwrap(),
        );
    }));
    let cdf_time = measure(Box::new(|| {
        black_box(evaluate_bound_scalar(&batch, &bound, None, &cancellation).unwrap());
    }));
    let allowance = direct_time.mul_f64(1.15) + Duration::from_micros(50);
    eprintln!(
        "D2 scalar roofline: direct={direct_time:?} cdf={cdf_time:?} ratio={:.4}",
        cdf_time.as_secs_f64() / direct_time.as_secs_f64()
    );
    assert!(
        cdf_time <= allowance,
        "CDF bound scalar execution {cdf_time:?} exceeded direct DataFusion {direct_time:?} by more than 15%"
    );
}

#[test]
fn bound_filter_projection_stays_within_execution_roofline() {
    const ROWS: usize = 1_000_000;
    const ITERATIONS: usize = 6;
    let schema = Schema::new(vec![
        Field::new("value", DataType::Int64, false),
        Field::new("keep", DataType::Boolean, false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from_iter_values(
                (0..ROWS).map(|value| -(value as i64)),
            )),
            Arc::new(BooleanArray::from(
                (0..ROWS).map(|value| value % 2 == 0).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let projection = builtin("abs").call(vec![datafusion::logical_expr::col("value")]);
    let plan = compile_relational_expression_plan(
        &schema,
        Some(AnalyzedScalarExpression::new(
            datafusion::logical_expr::col("keep"),
        )),
        vec![AnalyzedProjectionExpression {
            name: "magnitude".to_owned(),
            scalar: AnalyzedScalarExpression::new(projection.clone()),
        }],
        Vec::new(),
    )
    .unwrap();
    let output_schema = Arc::new(plan.output_schema.to_arrow().unwrap());
    let bound = bind_relational_expression_plan(&plan).unwrap();
    let direct_filter = direct_physical(&datafusion::logical_expr::col("keep"), &schema);
    let direct_projection = direct_physical(&projection, &schema);
    let cancellation = RunCancellation::default();

    let direct_once = || {
        let predicate = direct_filter
            .evaluate(&batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        let filtered = filter_record_batch(
            &batch,
            predicate.as_any().downcast_ref::<BooleanArray>().unwrap(),
        )
        .unwrap();
        let projected = direct_projection
            .evaluate(&filtered)
            .unwrap()
            .into_array(filtered.num_rows())
            .unwrap();
        RecordBatch::try_new(Arc::clone(&output_schema), vec![projected]).unwrap()
    };
    for _ in 0..2 {
        black_box(direct_once());
        black_box(execute_bound_relational_expression_plan(&bound, &batch, &cancellation).unwrap());
    }
    let direct_started = Instant::now();
    for _ in 0..ITERATIONS {
        black_box(direct_once());
    }
    let direct_time = direct_started.elapsed();
    let cdf_started = Instant::now();
    for _ in 0..ITERATIONS {
        black_box(execute_bound_relational_expression_plan(&bound, &batch, &cancellation).unwrap());
    }
    let cdf_time = cdf_started.elapsed();
    let allowance = direct_time.mul_f64(1.15) + Duration::from_micros(50);
    eprintln!(
        "D2 filter/projection roofline: direct={direct_time:?} cdf={cdf_time:?} ratio={:.4}",
        cdf_time.as_secs_f64() / direct_time.as_secs_f64()
    );
    assert!(
        cdf_time <= allowance,
        "CDF filter/projection execution {cdf_time:?} exceeded direct DataFusion/Arrow {direct_time:?} by more than 15%"
    );
}
