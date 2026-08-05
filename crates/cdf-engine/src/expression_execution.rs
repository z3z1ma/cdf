use std::{io::Cursor, sync::Arc};

use arrow_array::{ArrayRef, BooleanArray, RecordBatch};
use arrow_ipc::reader::StreamReader;
use arrow_schema::{Field, Schema};
use arrow_select::filter::filter_record_batch;
use cdf_contract::{
    DATAFUSION_EXPRESSION_PIN, DATAFUSION_SCALAR_CONFIG_IDENTITY, DATAFUSION_SCALAR_FEATURE_SET,
    DATAFUSION_SCALAR_NAMESPACE, ExpressionUse, PlannedExpression, RelationalExpressionPlan,
    ScalarBinaryOperator, ScalarCastMode, ScalarExpression, ScalarExpressionKind,
    ScalarExpressionNode, ScalarFunctionReference, ScalarFunctionVolatility, ScalarType,
    ScalarUnaryOperator, TransformDescription,
};
use cdf_kernel::{CdfError, Result};
use cdf_memory::MemoryLease;
use cdf_runtime::RunCancellation;
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::{
    common::{DFSchema, ScalarValue},
    execution::SessionStateDefaults,
    logical_expr::{BinaryExpr, Cast, Expr, Operator, TryCast, Volatility},
    physical_expr::{PhysicalExpr, create_physical_expr},
};

use crate::{
    expression::{DataFusionErrorPhase, classify_datafusion_error},
    expression_memory::{expression_nodes_working_set_bytes, expression_working_set_bytes},
};

pub(crate) const SOURCE_ROW_TRACKING_FIELD: &str = "_cdf_internal_source_row";

#[derive(Clone)]
pub(crate) struct BoundScalarExpression {
    physical: Arc<dyn PhysicalExpr>,
    input_schema: Schema,
    output_type: ScalarType,
    allocation_root: ScalarExpressionNode,
    dependency_indexes: Vec<usize>,
}

#[derive(Clone)]
pub(crate) enum BoundExpressionTransform {
    Derive {
        column: String,
        expression: BoundScalarExpression,
    },
    Filter(BoundScalarExpression),
}

#[derive(Clone)]
pub struct BoundRelationalExpressionPlan {
    input_schema: Schema,
    output_schema: Schema,
    filter: Option<BoundScalarExpression>,
    projection: Vec<BoundScalarExpression>,
}

pub(crate) fn bind_scalar_expression(
    expression: &ScalarExpression,
    schema: &Schema,
) -> Result<BoundScalarExpression> {
    expression.validate()?;
    validate_column_bindings(expression, schema)?;
    let logical = reconstruct_node(&expression.root)?;
    let df_schema = DFSchema::try_from(schema.clone()).map_err(binding_error)?;
    let physical = create_physical_expr(&logical, &df_schema, &ExecutionProps::new())
        .map_err(binding_error)?;
    let physical_type = physical.data_type(schema).map_err(binding_error)?;
    let physical_nullable = physical.nullable(schema).map_err(binding_error)?;
    if physical_type != expression.root.scalar_type.to_arrow()?
        || physical_nullable != expression.root.scalar_type.nullable
    {
        return Err(CdfError::contract(format!(
            "scalar runtime binding produced {physical_type}/{physical_nullable} but the recorded graph requires {}/{:?}; run `cdf compile`",
            expression.root.scalar_type.to_arrow()?,
            expression.root.scalar_type.nullable
        )));
    }
    Ok(BoundScalarExpression {
        physical,
        input_schema: schema.clone(),
        output_type: expression.root.scalar_type.clone(),
        allocation_root: expression.root.clone(),
        dependency_indexes: expression
            .column_dependencies()
            .iter()
            .map(|dependency| dependency.index)
            .collect(),
    })
}

pub(crate) fn bind_filter_expressions(
    expressions: &[PlannedExpression],
    schema: &Schema,
) -> Result<Vec<BoundScalarExpression>> {
    expressions
        .iter()
        .map(|planned| {
            planned.validate_recorded()?;
            if planned.use_kind != ExpressionUse::Filter {
                return Err(CdfError::contract(
                    "non-filter scalar expression entered residual filter binding",
                ));
            }
            bind_scalar_expression(&planned.expression, schema)
        })
        .collect()
}

pub(crate) fn bind_expression_transforms(
    transforms: &[TransformDescription],
    planned: &[PlannedExpression],
    schema: &Schema,
) -> Result<Vec<BoundExpressionTransform>> {
    let mut schema = schema.clone();
    let mut planned = planned.iter();
    let mut bound = Vec::new();
    for transform in transforms {
        match transform {
            TransformDescription::Derive { column, .. } => {
                let expression = planned.next().ok_or_else(|| {
                    CdfError::contract("derive transform has no compiled scalar expression")
                })?;
                if expression.use_kind != ExpressionUse::Derive {
                    return Err(CdfError::contract(
                        "derive transform is bound to a non-derive scalar expression",
                    ));
                }
                let expression = bind_scalar_expression(&expression.expression, &schema)?;
                schema = schema_with_derived(&schema, column, &expression.output_type)?;
                bound.push(BoundExpressionTransform::Derive {
                    column: column.clone(),
                    expression,
                });
            }
            TransformDescription::Filter { .. } => {
                let expression = planned.next().ok_or_else(|| {
                    CdfError::contract("filter transform has no compiled scalar expression")
                })?;
                if expression.use_kind != ExpressionUse::Filter {
                    return Err(CdfError::contract(
                        "filter transform is bound to a non-filter scalar expression",
                    ));
                }
                bound.push(BoundExpressionTransform::Filter(bind_scalar_expression(
                    &expression.expression,
                    &schema,
                )?));
            }
            _ => {}
        }
    }
    if planned.next().is_some() {
        return Err(CdfError::contract(
            "compiled scalar plan has extra transform expressions",
        ));
    }
    Ok(bound)
}

pub(crate) fn expression_transform_output_schema(
    transforms: &[TransformDescription],
    planned: &[PlannedExpression],
    schema: &Schema,
) -> Result<Schema> {
    let mut output = schema.clone();
    let mut planned = planned.iter();
    for transform in transforms {
        match transform {
            TransformDescription::Derive { column, .. } => {
                let expression = planned.next().ok_or_else(|| {
                    CdfError::contract("derive transform has no compiled scalar expression")
                })?;
                output =
                    schema_with_derived(&output, column, &expression.expression.root.scalar_type)?;
            }
            TransformDescription::Filter { .. } => {
                planned.next().ok_or_else(|| {
                    CdfError::contract("filter transform has no compiled scalar expression")
                })?;
            }
            _ => {}
        }
    }
    if planned.next().is_some() {
        return Err(CdfError::contract(
            "compiled scalar plan has extra transform expressions",
        ));
    }
    Ok(output)
}

pub(crate) fn apply_bound_filters(
    batch: &RecordBatch,
    expressions: &[BoundScalarExpression],
    memory: Option<&MemoryLease>,
    cancellation: &RunCancellation,
) -> Result<RecordBatch> {
    let mut output = batch.clone();
    for expression in expressions {
        cancellation.check()?;
        let values = evaluate_bound_scalar(&output, expression, memory, cancellation)?;
        let predicate = values
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| CdfError::contract("bound filter did not produce Boolean"))?;
        output = filter_record_batch(&output, predicate).map_err(CdfError::from)?;
    }
    Ok(output)
}

pub(crate) fn apply_bound_expression_transforms(
    mut batch: RecordBatch,
    transforms: &[BoundExpressionTransform],
    memory: Option<&MemoryLease>,
    cancellation: &RunCancellation,
) -> Result<RecordBatch> {
    for transform in transforms {
        cancellation.check()?;
        match transform {
            BoundExpressionTransform::Derive { column, expression } => {
                let values = evaluate_bound_scalar(&batch, expression, memory, cancellation)?;
                let schema =
                    schema_with_derived(batch.schema().as_ref(), column, &expression.output_type)?;
                let mut columns = batch.columns().to_vec();
                if let Ok(index) = batch.schema().index_of(column) {
                    columns[index] = values;
                } else if let Ok(index) = batch.schema().index_of(SOURCE_ROW_TRACKING_FIELD) {
                    columns.insert(index, values);
                } else {
                    columns.push(values);
                }
                batch = RecordBatch::try_new(Arc::new(schema), columns).map_err(CdfError::from)?;
            }
            BoundExpressionTransform::Filter(expression) => {
                batch = apply_bound_filters(
                    &batch,
                    std::slice::from_ref(expression),
                    memory,
                    cancellation,
                )?;
            }
        }
    }
    Ok(batch)
}

pub(crate) fn apply_expression_transforms(
    batch: RecordBatch,
    transforms: &[TransformDescription],
    planned: &[PlannedExpression],
    memory: Option<&MemoryLease>,
    cancellation: &RunCancellation,
) -> Result<RecordBatch> {
    let bound = bind_expression_transforms(transforms, planned, batch.schema().as_ref())?;
    apply_bound_expression_transforms(batch, &bound, memory, cancellation)
}

pub fn execute_scalar_expression(
    expression: &ScalarExpression,
    batch: &RecordBatch,
    memory: &MemoryLease,
    cancellation: &RunCancellation,
) -> Result<ArrayRef> {
    let expression_bytes =
        expression_working_set_bytes(std::iter::once(expression), batch.num_rows())?;
    validate_expression_memory(batch, expression_bytes, memory)?;
    let bound = bind_scalar_expression(expression, batch.schema().as_ref())?;
    evaluate_bound_scalar(batch, &bound, Some(memory), cancellation)
}

pub fn execute_relational_expression_plan(
    plan: &RelationalExpressionPlan,
    batch: &RecordBatch,
    memory: &MemoryLease,
    cancellation: &RunCancellation,
) -> Result<RecordBatch> {
    let expression_bytes = expression_working_set_bytes(
        plan.filter.iter().chain(
            plan.projection
                .iter()
                .map(|projection| &projection.expression),
        ),
        batch.num_rows(),
    )?;
    validate_expression_memory(batch, expression_bytes, memory)?;
    let bound = bind_relational_expression_plan(plan)?;
    execute_bound_relational_expression_plan_inner(&bound, batch, memory, cancellation)
}

pub fn bind_relational_expression_plan(
    plan: &RelationalExpressionPlan,
) -> Result<BoundRelationalExpressionPlan> {
    plan.validate_recorded()?;
    let input_schema =
        crate::output_schema::canonicalize_expression_input_schema(&plan.input_schema.to_arrow()?);
    let output_schema = plan.output_schema.to_arrow()?;
    let filter = plan
        .filter
        .as_ref()
        .map(|filter| bind_scalar_expression(filter, &input_schema))
        .transpose()?;
    let projection = plan
        .projection
        .iter()
        .map(|projection| bind_scalar_expression(&projection.expression, &input_schema))
        .collect::<Result<Vec<_>>>()?;
    Ok(BoundRelationalExpressionPlan {
        input_schema,
        output_schema,
        filter,
        projection,
    })
}

pub fn execute_bound_relational_expression_plan(
    plan: &BoundRelationalExpressionPlan,
    batch: &RecordBatch,
    memory: &MemoryLease,
    cancellation: &RunCancellation,
) -> Result<RecordBatch> {
    let expression_bytes = bound_expression_working_set_bytes(
        plan.filter.iter().chain(plan.projection.iter()),
        batch.num_rows(),
    )?;
    validate_expression_memory(batch, expression_bytes, memory)?;
    execute_bound_relational_expression_plan_inner(plan, batch, memory, cancellation)
}

pub(crate) fn execute_bound_relational_expression_plan_tracked(
    plan: &BoundRelationalExpressionPlan,
    batch: &RecordBatch,
    memory: &MemoryLease,
    cancellation: &RunCancellation,
) -> Result<(RecordBatch, Vec<usize>)> {
    let batch = crate::output_schema::canonicalize_expression_input_batch(batch.clone())?;
    let expression_bytes = bound_expression_working_set_bytes(
        plan.filter.iter().chain(plan.projection.iter()),
        batch.num_rows(),
    )?;
    validate_expression_memory(&batch, expression_bytes, memory)?;
    cancellation.check()?;
    validate_bound_relational_input(batch.schema().as_ref(), &plan.input_schema)?;
    let (filtered, source_rows) = match &plan.filter {
        Some(filter) => {
            let values = evaluate_bound_scalar(&batch, filter, Some(memory), cancellation)?;
            let predicate = values
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    CdfError::contract("bound relational filter did not produce Boolean")
                })?;
            let source_rows = predicate
                .iter()
                .enumerate()
                .filter_map(|(index, keep)| keep.unwrap_or(false).then_some(index))
                .collect::<Vec<_>>();
            (
                filter_record_batch(&batch, predicate).map_err(CdfError::from)?,
                source_rows,
            )
        }
        None => (batch.clone(), (0..batch.num_rows()).collect()),
    };
    let mut columns = Vec::with_capacity(plan.projection.len());
    for projection in &plan.projection {
        cancellation.check()?;
        columns.push(evaluate_bound_scalar(
            &filtered,
            projection,
            Some(memory),
            cancellation,
        )?);
    }
    let output_schema = relational_output_schema(&plan.output_schema, &columns);
    let output = RecordBatch::try_new(Arc::new(output_schema), columns).map_err(CdfError::from)?;
    validate_expression_output_memory(&batch, output.get_array_memory_size(), memory)?;
    Ok((output, source_rows))
}

fn execute_bound_relational_expression_plan_inner(
    plan: &BoundRelationalExpressionPlan,
    batch: &RecordBatch,
    memory: &MemoryLease,
    cancellation: &RunCancellation,
) -> Result<RecordBatch> {
    let batch = crate::output_schema::canonicalize_expression_input_batch(batch.clone())?;
    cancellation.check()?;
    validate_bound_relational_input(batch.schema().as_ref(), &plan.input_schema)?;
    let filtered = match &plan.filter {
        Some(filter) => apply_bound_filters(
            &batch,
            std::slice::from_ref(filter),
            Some(memory),
            cancellation,
        )?,
        None => batch.clone(),
    };
    let mut columns = Vec::with_capacity(plan.projection.len());
    for projection in &plan.projection {
        cancellation.check()?;
        columns.push(evaluate_bound_scalar(
            &filtered,
            projection,
            Some(memory),
            cancellation,
        )?);
    }
    let output_schema = relational_output_schema(&plan.output_schema, &columns);
    let output = RecordBatch::try_new(Arc::new(output_schema), columns).map_err(CdfError::from)?;
    validate_expression_output_memory(&batch, output.get_array_memory_size(), memory)?;
    Ok(output)
}

fn validate_bound_relational_input(actual: &Schema, expected: &Schema) -> Result<()> {
    let compatible = actual.metadata() == expected.metadata()
        && actual.fields().len() == expected.fields().len()
        && actual
            .fields()
            .iter()
            .zip(expected.fields())
            .all(|(actual, expected)| {
                actual.name() == expected.name()
                    && actual.data_type() == expected.data_type()
                    && actual.metadata() == expected.metadata()
                    && (actual.is_nullable() == expected.is_nullable()
                        || (actual.is_nullable() && !expected.is_nullable()))
            });
    if compatible {
        Ok(())
    } else {
        Err(CdfError::contract(
            "relational expression input schema differs from its compiled authority; run `cdf compile`",
        ))
    }
}

fn relational_output_schema(expected: &Schema, columns: &[ArrayRef]) -> Schema {
    let fields = expected
        .fields()
        .iter()
        .zip(columns)
        .map(|(field, column)| {
            if column.null_count() == 0 || field.is_nullable() {
                Arc::clone(field)
            } else {
                Arc::new(field.as_ref().clone().with_nullable(true))
            }
        })
        .collect::<Vec<_>>();
    Schema::new_with_metadata(fields, expected.metadata().clone())
}

fn validate_expression_memory(
    batch: &RecordBatch,
    expression_bytes: u64,
    memory: &MemoryLease,
) -> Result<()> {
    let input_bytes = u64::try_from(batch.get_array_memory_size())
        .map_err(|_| CdfError::data("scalar input memory exceeds u64"))?;
    let minimum = input_bytes
        .max(1)
        .checked_mul(4)
        .and_then(|bytes| bytes.checked_add(expression_bytes))
        .ok_or_else(|| CdfError::data("scalar working-set memory overflow"))?;
    if memory.bytes() < minimum {
        return Err(CdfError::environment(format!(
            "scalar execution requires a pre-acquired CDF memory lease of at least {minimum} bytes; lease owns {}",
            memory.bytes()
        )));
    }
    Ok(())
}

fn bound_expression_working_set_bytes<'a>(
    expressions: impl IntoIterator<Item = &'a BoundScalarExpression>,
    rows: usize,
) -> Result<u64> {
    expression_nodes_working_set_bytes(
        expressions
            .into_iter()
            .map(|expression| &expression.allocation_root),
        rows,
    )
}

fn validate_expression_output_memory(
    batch: &RecordBatch,
    output_bytes: usize,
    memory: &MemoryLease,
) -> Result<()> {
    let observed = batch
        .get_array_memory_size()
        .checked_add(output_bytes)
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| CdfError::data("scalar output working-set memory overflow"))?;
    if observed > memory.bytes() {
        return Err(CdfError::environment(format!(
            "scalar execution produced a {observed}-byte working set above its {}-byte CDF memory lease",
            memory.bytes()
        )));
    }
    Ok(())
}

pub(crate) fn evaluate_bound_scalar(
    batch: &RecordBatch,
    expression: &BoundScalarExpression,
    memory: Option<&MemoryLease>,
    cancellation: &RunCancellation,
) -> Result<ArrayRef> {
    cancellation.check()?;
    validate_bound_relational_input(batch.schema().as_ref(), &expression.input_schema)?;
    let actual_schema = batch.schema();
    let nullable_dependency_widened = expression.dependency_indexes.iter().any(|index| {
        let actual = &actual_schema.fields()[*index];
        let expected = &expression.input_schema.fields()[*index];
        actual.is_nullable() && !expected.is_nullable()
    });
    let value = expression
        .physical
        .evaluate(batch)
        .map_err(execution_error)?;
    cancellation.check()?;
    if value.data_type() != expression.output_type.to_arrow()? {
        return Err(CdfError::internal(
            "pinned scalar kernel returned a type different from its verified binding",
        ));
    }
    let output = value
        .into_array(batch.num_rows())
        .map_err(execution_error)?;
    if output.len() != batch.num_rows() {
        return Err(CdfError::internal(
            "pinned scalar kernel returned an invalid batch length",
        ));
    }
    if !expression.output_type.nullable && output.null_count() != 0 && !nullable_dependency_widened
    {
        return Err(CdfError::internal(
            "pinned scalar kernel returned nulls for a verified non-nullable binding",
        ));
    }
    if let Some(memory) = memory {
        validate_expression_output_memory(batch, output.get_array_memory_size(), memory)?;
    }
    Ok(output)
}

pub(crate) fn reconstruct_node(node: &ScalarExpressionNode) -> Result<Expr> {
    match &node.expression {
        ScalarExpressionKind::Column { name, .. } => Ok(datafusion::logical_expr::col(name)),
        ScalarExpressionKind::Literal { arrow_ipc } => {
            Ok(Expr::Literal(decode_scalar_literal(arrow_ipc)?, None))
        }
        ScalarExpressionKind::Unary { operator, argument } => {
            let argument = Box::new(reconstruct_node(argument)?);
            Ok(match operator {
                ScalarUnaryOperator::Not => Expr::Not(argument),
                ScalarUnaryOperator::Negative => Expr::Negative(argument),
                ScalarUnaryOperator::IsNull => Expr::IsNull(argument),
                ScalarUnaryOperator::IsNotNull => Expr::IsNotNull(argument),
            })
        }
        ScalarExpressionKind::Binary {
            operator,
            left,
            right,
        } => Ok(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(reconstruct_node(left)?),
            datafusion_operator(*operator),
            Box::new(reconstruct_node(right)?),
        ))),
        ScalarExpressionKind::Call {
            function,
            arguments,
        } => {
            let function = resolve_function(function)?;
            let arguments = arguments
                .iter()
                .map(reconstruct_node)
                .collect::<Result<Vec<_>>>()?;
            Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
                function, arguments,
            )))
        }
        ScalarExpressionKind::Cast {
            mode,
            target_type,
            argument,
            ..
        } => {
            let argument = Box::new(reconstruct_node(argument)?);
            let target = target_type.to_arrow()?;
            Ok(match mode {
                ScalarCastMode::Implicit | ScalarCastMode::Explicit => {
                    Expr::Cast(Cast::new(argument, target))
                }
                ScalarCastMode::Try => Expr::TryCast(TryCast::new(argument, target)),
            })
        }
    }
}

fn resolve_function(
    recorded: &ScalarFunctionReference,
) -> Result<Arc<datafusion::logical_expr::ScalarUDF>> {
    if recorded.namespace != DATAFUSION_SCALAR_NAMESPACE
        || recorded.implementation_version != DATAFUSION_EXPRESSION_PIN
        || recorded.feature_set != DATAFUSION_SCALAR_FEATURE_SET
        || recorded.config_identity != DATAFUSION_SCALAR_CONFIG_IDENTITY
        || recorded.volatility != ScalarFunctionVolatility::Immutable
    {
        return Err(CdfError::contract(
            "scalar function dependency tuple is stale; run `cdf compile`",
        ));
    }
    let function = SessionStateDefaults::default_scalar_functions()
        .into_iter()
        .find(|function| function.name() == recorded.canonical_name)
        .ok_or_else(|| {
            CdfError::contract(format!(
                "pinned scalar function {:?} is unavailable; run `cdf compile`",
                recorded.canonical_name
            ))
        })?;
    if function.signature().volatility != Volatility::Immutable {
        return Err(CdfError::contract(format!(
            "pinned scalar function {:?} no longer has Immutable volatility; run `cdf compile`",
            recorded.canonical_name
        )));
    }
    Ok(function)
}

pub(crate) fn decode_scalar_literal(bytes: &[u8]) -> Result<ScalarValue> {
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None)
        .map_err(|error| CdfError::contract(format!("decode scalar literal IPC: {error}")))?;
    let batch = reader
        .next()
        .transpose()
        .map_err(|error| CdfError::contract(format!("decode scalar literal batch: {error}")))?
        .ok_or_else(|| CdfError::contract("scalar literal IPC contains no batch"))?;
    if batch.num_rows() != 1 || batch.num_columns() != 1 || reader.next().is_some() {
        return Err(CdfError::contract(
            "scalar literal IPC must contain exactly one cell",
        ));
    }
    ScalarValue::try_from_array(batch.column(0), 0)
        .map_err(|error| CdfError::contract(format!("decode scalar literal value: {error}")))
}

fn validate_column_bindings(expression: &ScalarExpression, schema: &Schema) -> Result<()> {
    for dependency in expression.column_dependencies() {
        let field = schema.fields().get(dependency.index).ok_or_else(|| {
            CdfError::contract(format!(
                "scalar column {:?} has stale ordinal {}; run `cdf compile`",
                dependency.name, dependency.index
            ))
        })?;
        if field.name() != &dependency.name
            || dependency.scalar_type
                != ScalarType::from_arrow(field.data_type(), field.is_nullable())?
        {
            return Err(CdfError::contract(format!(
                "scalar column {:?} changed name, type, or nullability; run `cdf compile`",
                dependency.name
            )));
        }
    }
    Ok(())
}

fn schema_with_derived(schema: &Schema, column: &str, scalar_type: &ScalarType) -> Result<Schema> {
    let field = Arc::new(Field::new(
        column,
        scalar_type.to_arrow()?,
        scalar_type.nullable,
    ));
    let mut fields = schema.fields().iter().cloned().collect::<Vec<_>>();
    if let Ok(index) = schema.index_of(column) {
        fields[index] = field;
    } else if let Ok(index) = schema.index_of(SOURCE_ROW_TRACKING_FIELD) {
        fields.insert(index, field);
    } else {
        fields.push(field);
    }
    Ok(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

fn datafusion_operator(operator: ScalarBinaryOperator) -> Operator {
    match operator {
        ScalarBinaryOperator::Equal => Operator::Eq,
        ScalarBinaryOperator::NotEqual => Operator::NotEq,
        ScalarBinaryOperator::Less => Operator::Lt,
        ScalarBinaryOperator::LessOrEqual => Operator::LtEq,
        ScalarBinaryOperator::Greater => Operator::Gt,
        ScalarBinaryOperator::GreaterOrEqual => Operator::GtEq,
        ScalarBinaryOperator::Add => Operator::Plus,
        ScalarBinaryOperator::Subtract => Operator::Minus,
        ScalarBinaryOperator::Multiply => Operator::Multiply,
        ScalarBinaryOperator::Divide => Operator::Divide,
        ScalarBinaryOperator::Modulo => Operator::Modulo,
        ScalarBinaryOperator::And => Operator::And,
        ScalarBinaryOperator::Or => Operator::Or,
        ScalarBinaryOperator::IsDistinctFrom => Operator::IsDistinctFrom,
        ScalarBinaryOperator::IsNotDistinctFrom => Operator::IsNotDistinctFrom,
        ScalarBinaryOperator::RegexMatch => Operator::RegexMatch,
        ScalarBinaryOperator::RegexInsensitiveMatch => Operator::RegexIMatch,
        ScalarBinaryOperator::RegexNotMatch => Operator::RegexNotMatch,
        ScalarBinaryOperator::RegexNotInsensitiveMatch => Operator::RegexNotIMatch,
        ScalarBinaryOperator::Like => Operator::LikeMatch,
        ScalarBinaryOperator::InsensitiveLike => Operator::ILikeMatch,
        ScalarBinaryOperator::NotLike => Operator::NotLikeMatch,
        ScalarBinaryOperator::NotInsensitiveLike => Operator::NotILikeMatch,
        ScalarBinaryOperator::BitwiseAnd => Operator::BitwiseAnd,
        ScalarBinaryOperator::BitwiseOr => Operator::BitwiseOr,
        ScalarBinaryOperator::BitwiseXor => Operator::BitwiseXor,
        ScalarBinaryOperator::BitwiseShiftRight => Operator::BitwiseShiftRight,
        ScalarBinaryOperator::BitwiseShiftLeft => Operator::BitwiseShiftLeft,
        ScalarBinaryOperator::StringConcat => Operator::StringConcat,
        ScalarBinaryOperator::ListContains => Operator::AtArrow,
        ScalarBinaryOperator::ListContainedBy => Operator::ArrowAt,
    }
}

fn binding_error(error: datafusion::common::DataFusionError) -> CdfError {
    let mut error = classify_datafusion_error(error, DataFusionErrorPhase::Binding);
    if error.kind == cdf_kernel::ErrorKind::Contract {
        error.message.push_str("; run `cdf compile`");
    }
    error
}

fn execution_error(error: datafusion::common::DataFusionError) -> CdfError {
    classify_datafusion_error(error, DataFusionErrorPhase::Execution)
}
