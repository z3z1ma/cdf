use std::sync::Arc;

use arrow_schema::{DataType, Schema};
use cdf_contract::{
    DATAFUSION_EXPRESSION_OPTIMIZER, DATAFUSION_EXPRESSION_PIN, Expression, ExpressionFidelity,
    ExpressionLint, ExpressionLintCode, ExpressionLiteral, ExpressionNode, ExpressionUse,
    FunctionReference, NATIVE_CONTRACT_OPTIMIZER, OptimizerIdentity, PlannedExpression,
    SOURCE_EXACT_PUSHDOWN_OPTIMIZER,
};
use cdf_kernel::{CdfError, Result};
use datafusion::{
    common::{DFSchema, ScalarValue},
    logical_expr::{BinaryExpr, Expr, Operator, simplify::SimplifyContext},
    optimizer::simplify_expressions::ExprSimplifier,
};

pub(crate) fn plan_expression(
    expression: Expression,
    use_kind: ExpressionUse,
    schema: &Schema,
) -> Result<PlannedExpression> {
    expression.validate()?;
    let logical = to_datafusion(&expression.root)?;
    let df_schema = Arc::new(DFSchema::try_from(schema.clone()).map_err(datafusion_error)?);
    let context = SimplifyContext::builder()
        .with_schema(Arc::clone(&df_schema))
        .build();
    let simplifier = ExprSimplifier::new(context);
    let coerced = simplifier
        .coerce(logical, df_schema.as_ref())
        .map_err(datafusion_error)?;
    let optimized = simplifier.simplify(coerced).map_err(datafusion_error)?;
    let optimized = Expression::new(from_datafusion(&optimized)?);
    validate_native_filter_expression(&optimized.root, schema)?;
    let mut lints = lint_expression(&expression);
    for lint in lint_expression(&optimized) {
        if !lints.iter().any(|existing| existing.code == lint.code) {
            lints.push(lint);
        }
    }
    let functions = optimized.function_dependencies();
    let fidelity = ExpressionFidelity::Exact;
    let residuals = Vec::new();
    if optimized.root
        == (ExpressionNode::Literal {
            value: ExpressionLiteral::Boolean(true),
        })
        && !lints
            .iter()
            .any(|lint| lint.code == ExpressionLintCode::AlwaysTrue)
    {
        lints.push(ExpressionLint {
            code: ExpressionLintCode::AlwaysTrue,
            message: "expression is provably always true".to_owned(),
        });
    }
    Ok(PlannedExpression {
        use_kind,
        source_text: None,
        original: expression,
        optimized: optimized.clone(),
        optimizer: OptimizerIdentity {
            name: DATAFUSION_EXPRESSION_OPTIMIZER.to_owned(),
            version: DATAFUSION_EXPRESSION_PIN.to_owned(),
        },
        functions,
        fidelity,
        residuals: residuals.clone(),
        lints,
        substrait_version: None,
    })
}

pub(crate) fn record_native_contract_expression(
    expression: Expression,
    schema: &Schema,
) -> Result<PlannedExpression> {
    expression.validate()?;
    let function = match &expression.root {
        ExpressionNode::Call { function, .. } => function.name.as_str(),
        _ => "",
    };
    if !matches!(
        function,
        "is_not_null"
            | "in_domain"
            | "in_range"
            | "matches_regex"
            | "fresh_within"
            | "dedup"
            | "exact_row_dedup"
    ) {
        return Err(CdfError::contract(format!(
            "contract expression function {function:?} has no admitted native fused lowering"
        )));
    }
    let functions = expression.function_dependencies();
    let mut lints = lint_expression(&expression);
    lints.extend(lint_contract_expression(&expression.root, schema));
    let fidelity = ExpressionFidelity::Exact;
    Ok(PlannedExpression {
        use_kind: ExpressionUse::Contract,
        source_text: None,
        original: expression.clone(),
        optimized: expression.clone(),
        optimizer: OptimizerIdentity {
            name: NATIVE_CONTRACT_OPTIMIZER.to_owned(),
            version: cdf_contract::CDF_FUNCTION_VERSION.to_owned(),
        },
        functions,
        fidelity,
        residuals: Vec::new(),
        lints,
        substrait_version: None,
    })
}

pub(crate) fn record_exact_source_expression(expression: Expression) -> Result<PlannedExpression> {
    expression.validate()?;
    Ok(PlannedExpression {
        use_kind: ExpressionUse::Filter,
        source_text: None,
        original: expression.clone(),
        optimized: expression.clone(),
        optimizer: OptimizerIdentity {
            name: SOURCE_EXACT_PUSHDOWN_OPTIMIZER.to_owned(),
            version: cdf_contract::CDF_FUNCTION_VERSION.to_owned(),
        },
        functions: expression.function_dependencies(),
        fidelity: ExpressionFidelity::Exact,
        residuals: Vec::new(),
        lints: lint_expression(&expression),
        substrait_version: None,
    })
}

fn validate_native_filter_expression(node: &ExpressionNode, schema: &Schema) -> Result<()> {
    match node {
        ExpressionNode::Literal {
            value: ExpressionLiteral::Boolean(_) | ExpressionLiteral::Null,
        } => Ok(()),
        ExpressionNode::Column { name } => {
            let field = schema.field_with_name(name).map_err(|_| {
                CdfError::contract(format!(
                    "predicate field {name:?} is absent from the planned schema"
                ))
            })?;
            if field.data_type() == &DataType::Boolean {
                Ok(())
            } else {
                Err(CdfError::contract(format!(
                    "predicate field {name:?} has non-boolean type {}",
                    field.data_type()
                )))
            }
        }
        ExpressionNode::Call {
            function,
            arguments,
        } => match (function.name.as_str(), arguments.as_slice()) {
            ("not", [value]) => validate_native_filter_expression(value, schema),
            ("and" | "or", [left, right]) => {
                validate_native_filter_expression(left, schema)?;
                validate_native_filter_expression(right, schema)
            }
            ("is_null" | "is_not_null", [ExpressionNode::Column { name }]) => {
                schema.field_with_name(name).map(|_| ()).map_err(|_| {
                    CdfError::contract(format!(
                        "predicate field {name:?} is absent from the planned schema"
                    ))
                })
            }
            (
                "eq" | "neq" | "gt" | "gte" | "lt" | "lte",
                [
                    ExpressionNode::Column { name },
                    ExpressionNode::Literal { value },
                ],
            ) => validate_native_comparison(name, function.name.as_str(), value, schema),
            (name, _) => Err(CdfError::contract(format!(
                "CDF expression function {name:?} has no native filter capability"
            ))),
        },
        other => Err(CdfError::contract(format!(
            "recorded expression {other:?} has no native boolean lowering"
        ))),
    }
}

fn validate_native_comparison(
    name: &str,
    operator: &str,
    literal: &ExpressionLiteral,
    schema: &Schema,
) -> Result<()> {
    let field = schema.field_with_name(name).map_err(|_| {
        CdfError::contract(format!(
            "predicate field {name:?} is absent from the planned schema"
        ))
    })?;
    let supported = match (field.data_type(), literal) {
        (DataType::Int8, ExpressionLiteral::Signed(value)) => i8::try_from(*value).is_ok(),
        (DataType::Int16, ExpressionLiteral::Signed(value)) => i16::try_from(*value).is_ok(),
        (DataType::Int32, ExpressionLiteral::Signed(value)) => i32::try_from(*value).is_ok(),
        (DataType::Int64, ExpressionLiteral::Signed(_)) => true,
        (DataType::UInt8, ExpressionLiteral::Unsigned(value)) => u8::try_from(*value).is_ok(),
        (DataType::UInt16, ExpressionLiteral::Unsigned(value)) => u16::try_from(*value).is_ok(),
        (DataType::UInt32, ExpressionLiteral::Unsigned(value)) => u32::try_from(*value).is_ok(),
        (DataType::UInt64, ExpressionLiteral::Unsigned(_)) => true,
        (DataType::Float64, ExpressionLiteral::Float64Bits(bits)) => {
            f64::from_bits(*bits).is_finite()
        }
        (DataType::Utf8 | DataType::LargeUtf8, ExpressionLiteral::String(_)) => true,
        (DataType::Boolean, ExpressionLiteral::Boolean(_)) => matches!(operator, "eq" | "neq"),
        (_, ExpressionLiteral::Null) => true,
        _ => false,
    };
    if !supported {
        return Err(CdfError::contract(format!(
            "predicate field {name:?} with type {} and operator {operator:?} has no exact native filter lowering for {literal:?}",
            field.data_type()
        )));
    }
    Ok(())
}

fn lint_contract_expression(node: &ExpressionNode, schema: &Schema) -> Vec<ExpressionLint> {
    let ExpressionNode::Call {
        function,
        arguments,
    } = node
    else {
        return Vec::new();
    };
    match (function.name.as_str(), arguments.as_slice()) {
        ("is_not_null", [ExpressionNode::Column { name }]) => schema
            .field_with_name(name)
            .ok()
            .filter(|field| !field.is_nullable())
            .map(|_| {
                vec![ExpressionLint {
                    code: ExpressionLintCode::AlwaysTrue,
                    message: format!(
                        "nullability rule for non-nullable field {name:?} is provably always true"
                    ),
                }]
            })
            .unwrap_or_default(),
        (
            "in_range",
            [
                ExpressionNode::Column { name },
                ExpressionNode::Literal { value: min },
                ExpressionNode::Literal { value: max },
            ],
        ) => {
            if matches!(min, ExpressionLiteral::Null) && matches!(max, ExpressionLiteral::Null) {
                return vec![ExpressionLint {
                    code: ExpressionLintCode::AlwaysTrue,
                    message: format!("unbounded range rule for {name:?} is always true"),
                }];
            }
            let Some(field) = schema.field_with_name(name).ok() else {
                return Vec::new();
            };
            let (ExpressionLiteral::String(min), ExpressionLiteral::String(max)) = (min, max)
            else {
                return Vec::new();
            };
            let impossible = cdf_contract::range_bounds_are_unsatisfiable(
                field.data_type(),
                Some(min),
                Some(max),
            )
            .unwrap_or(false);
            impossible
                .then(|| ExpressionLint {
                    code: ExpressionLintCode::UnsatisfiableRange,
                    message: format!("range rule for {name:?} is provably empty"),
                })
                .into_iter()
                .collect()
        }
        _ => Vec::new(),
    }
}

pub(crate) fn validate_recorded_expressions(expressions: &[PlannedExpression]) -> Result<()> {
    expressions
        .iter()
        .try_for_each(PlannedExpression::validate_recorded)
}

pub(crate) fn mark_cursor_subsumed(expressions: &mut [PlannedExpression], cursor_field: &str) {
    for candidate in 0..expressions.len() {
        let Some((field, operator, value)) = comparison(&expressions[candidate].optimized.root)
        else {
            continue;
        };
        if field != cursor_field {
            continue;
        }
        let subsumed = expressions.iter().enumerate().any(|(other_index, other)| {
            if other_index == candidate {
                return false;
            }
            let Some((other_field, other_operator, other_value)) =
                comparison(&other.optimized.root)
            else {
                return false;
            };
            other_field == field && bound_subsumes(other_operator, other_value, operator, value)
        });
        if subsumed {
            expressions[candidate].lints.push(ExpressionLint {
                code: ExpressionLintCode::CursorSubsumed,
                message: format!(
                    "cursor filter on {cursor_field:?} is provably subsumed by a stronger recorded filter"
                ),
            });
        }
    }
}

fn bound_subsumes(
    stronger_operator: &str,
    stronger: &ExpressionLiteral,
    candidate_operator: &str,
    candidate: &ExpressionLiteral,
) -> bool {
    match (stronger, candidate) {
        (ExpressionLiteral::Signed(stronger), ExpressionLiteral::Signed(candidate)) => {
            numeric_bound_subsumes(stronger_operator, *stronger, candidate_operator, *candidate)
        }
        (ExpressionLiteral::Unsigned(stronger), ExpressionLiteral::Unsigned(candidate)) => {
            numeric_bound_subsumes(stronger_operator, *stronger, candidate_operator, *candidate)
        }
        _ => false,
    }
}

fn numeric_bound_subsumes<T: Ord>(
    stronger_operator: &str,
    stronger: T,
    candidate_operator: &str,
    candidate: T,
) -> bool {
    match (stronger_operator, candidate_operator) {
        ("gt" | "gte", "gt" | "gte") => {
            stronger > candidate
                || stronger == candidate
                    && (stronger_operator == "gt" || candidate_operator == "gte")
        }
        ("lt" | "lte", "lt" | "lte") => {
            stronger < candidate
                || stronger == candidate
                    && (stronger_operator == "lt" || candidate_operator == "lte")
        }
        _ => false,
    }
}

fn to_datafusion(node: &ExpressionNode) -> Result<Expr> {
    match node {
        ExpressionNode::Column { name } => Ok(datafusion::logical_expr::col(name)),
        ExpressionNode::Literal { value } => Ok(Expr::Literal(to_scalar(value)?, None)),
        ExpressionNode::Call {
            function,
            arguments,
        } => {
            require_cdf_v1(function)?;
            let mut arguments = arguments
                .iter()
                .map(to_datafusion)
                .collect::<Result<Vec<_>>>()?;
            match (function.name.as_str(), arguments.len()) {
                ("not", 1) => Ok(Expr::Not(Box::new(arguments.remove(0)))),
                ("is_null", 1) => Ok(Expr::IsNull(Box::new(arguments.remove(0)))),
                ("is_not_null", 1) => Ok(Expr::IsNotNull(Box::new(arguments.remove(0)))),
                (name, 2) => {
                    let right = arguments.pop().expect("binary right argument");
                    let left = arguments.pop().expect("binary left argument");
                    Ok(Expr::BinaryExpr(BinaryExpr::new(
                        Box::new(left),
                        operator(name)?,
                        Box::new(right),
                    )))
                }
                (name, arity) => Err(CdfError::contract(format!(
                    "CDF expression function {name:?} has unsupported arity {arity}; identity-bearing planning admits unary null/not and binary comparison/boolean functions"
                ))),
            }
        }
        other => Err(CdfError::contract(format!(
            "expression node {other:?} is not admitted by this engine version"
        ))),
    }
}

/// Reconstructs the recorded filter with literal types resolved from its recorded Arrow schema.
/// Unlike planning, this performs no simplification or optimization. `None` means the recorded
/// predicate is valid but this engine version cannot use it as pruning proof, so callers retain.
pub(crate) fn lower_recorded_filter_for_pruning(
    node: &ExpressionNode,
    schema: &Schema,
) -> Result<Option<Expr>> {
    match node {
        ExpressionNode::Column { name } => {
            schema.field_with_name(name).map_err(|_| {
                CdfError::contract(format!(
                    "recorded filter field {name:?} is absent from the recorded schema"
                ))
            })?;
            Ok(Some(datafusion::logical_expr::col(name)))
        }
        ExpressionNode::Literal { value } => Ok(root_filter_literal(value)),
        ExpressionNode::Call {
            function,
            arguments,
        } => lower_recorded_filter_call(function, arguments, schema),
        other => Err(CdfError::contract(format!(
            "recorded filter expression node {other:?} is unsupported"
        ))),
    }
}

fn lower_recorded_filter_call(
    function: &FunctionReference,
    arguments: &[ExpressionNode],
    schema: &Schema,
) -> Result<Option<Expr>> {
    require_cdf_v1(function)?;
    match (function.name.as_str(), arguments) {
        ("not", [value]) => Ok(lower_recorded_filter_for_pruning(value, schema)?
            .map(|value| Expr::Not(Box::new(value)))),
        ("is_null", [value]) => Ok(lower_recorded_filter_for_pruning(value, schema)?
            .map(|value| Expr::IsNull(Box::new(value)))),
        ("is_not_null", [value]) => Ok(lower_recorded_filter_for_pruning(value, schema)?
            .map(|value| Expr::IsNotNull(Box::new(value)))),
        ("and" | "or", [left, right]) => {
            let Some(left) = lower_recorded_filter_for_pruning(left, schema)? else {
                return Ok(None);
            };
            let Some(right) = lower_recorded_filter_for_pruning(right, schema)? else {
                return Ok(None);
            };
            Ok(Some(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(left),
                operator(function.name.as_str())?,
                Box::new(right),
            ))))
        }
        (
            "eq" | "neq" | "gt" | "gte" | "lt" | "lte",
            [
                ExpressionNode::Column { name },
                ExpressionNode::Literal { value },
            ],
        ) => {
            let field = schema.field_with_name(name).map_err(|_| {
                CdfError::contract(format!(
                    "recorded filter field {name:?} is absent from the recorded schema"
                ))
            })?;
            let Some(value) = typed_filter_literal(field.data_type(), value)? else {
                return Ok(None);
            };
            Ok(Some(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(datafusion::logical_expr::col(name)),
                operator(function.name.as_str())?,
                Box::new(Expr::Literal(value, None)),
            ))))
        }
        _ => Ok(None),
    }
}

fn root_filter_literal(value: &ExpressionLiteral) -> Option<Expr> {
    let scalar = match value {
        ExpressionLiteral::Null => ScalarValue::Null,
        ExpressionLiteral::Boolean(value) => ScalarValue::Boolean(Some(*value)),
        _ => return None,
    };
    Some(Expr::Literal(scalar, None))
}

fn typed_filter_literal(
    data_type: &DataType,
    value: &ExpressionLiteral,
) -> Result<Option<ScalarValue>> {
    if matches!(value, ExpressionLiteral::Null) {
        return ScalarValue::try_new_null(data_type)
            .map(Some)
            .map_err(datafusion_error);
    }
    Ok(match (data_type, value) {
        (DataType::Boolean, ExpressionLiteral::Boolean(value)) => {
            Some(ScalarValue::Boolean(Some(*value)))
        }
        (DataType::Int8, ExpressionLiteral::Signed(value)) => i8::try_from(*value)
            .ok()
            .map(|value| ScalarValue::Int8(Some(value))),
        (DataType::Int16, ExpressionLiteral::Signed(value)) => i16::try_from(*value)
            .ok()
            .map(|value| ScalarValue::Int16(Some(value))),
        (DataType::Int32, ExpressionLiteral::Signed(value)) => i32::try_from(*value)
            .ok()
            .map(|value| ScalarValue::Int32(Some(value))),
        (DataType::Int64, ExpressionLiteral::Signed(value)) => {
            Some(ScalarValue::Int64(Some(*value)))
        }
        (DataType::UInt8, ExpressionLiteral::Unsigned(value)) => u8::try_from(*value)
            .ok()
            .map(|value| ScalarValue::UInt8(Some(value))),
        (DataType::UInt16, ExpressionLiteral::Unsigned(value)) => u16::try_from(*value)
            .ok()
            .map(|value| ScalarValue::UInt16(Some(value))),
        (DataType::UInt32, ExpressionLiteral::Unsigned(value)) => u32::try_from(*value)
            .ok()
            .map(|value| ScalarValue::UInt32(Some(value))),
        (DataType::UInt64, ExpressionLiteral::Unsigned(value)) => {
            Some(ScalarValue::UInt64(Some(*value)))
        }
        (DataType::Float64, ExpressionLiteral::Float64Bits(bits)) => {
            let value = f64::from_bits(*bits);
            value
                .is_finite()
                .then_some(ScalarValue::Float64(Some(value)))
        }
        (DataType::Utf8, ExpressionLiteral::String(value)) => {
            Some(ScalarValue::Utf8(Some(value.clone())))
        }
        (DataType::LargeUtf8, ExpressionLiteral::String(value)) => {
            Some(ScalarValue::LargeUtf8(Some(value.clone())))
        }
        _ => None,
    })
}

fn from_datafusion(expression: &Expr) -> Result<ExpressionNode> {
    match expression {
        Expr::Column(column) => Ok(ExpressionNode::Column {
            name: column.name.clone(),
        }),
        Expr::Literal(value, _) => Ok(ExpressionNode::Literal {
            value: from_scalar(value)?,
        }),
        Expr::BinaryExpr(binary) => Ok(ExpressionNode::Call {
            function: FunctionReference::cdf(operator_name(binary.op)?),
            arguments: vec![
                from_datafusion(&binary.left)?,
                from_datafusion(&binary.right)?,
            ],
        }),
        Expr::Not(value) => unary("not", from_datafusion(value)?),
        Expr::IsNull(value) => unary("is_null", from_datafusion(value)?),
        Expr::IsNotNull(value) => unary("is_not_null", from_datafusion(value)?),
        other => Err(CdfError::contract(format!(
            "DataFusion simplified expression {other:?} has no exact CDF native lowering"
        ))),
    }
}

fn unary(name: &str, argument: ExpressionNode) -> Result<ExpressionNode> {
    Ok(ExpressionNode::Call {
        function: FunctionReference::cdf(name),
        arguments: vec![argument],
    })
}

fn require_cdf_v1(function: &FunctionReference) -> Result<()> {
    if function.namespace != cdf_contract::CDF_FUNCTION_NAMESPACE
        || function.version != cdf_contract::CDF_FUNCTION_VERSION
    {
        return Err(CdfError::contract(format!(
            "unsupported expression function {}.{}@{}; identity-bearing planning requires cdf functions at version 1",
            function.namespace, function.name, function.version
        )));
    }
    Ok(())
}

fn operator(name: &str) -> Result<Operator> {
    match name {
        "eq" => Ok(Operator::Eq),
        "neq" => Ok(Operator::NotEq),
        "gt" => Ok(Operator::Gt),
        "gte" => Ok(Operator::GtEq),
        "lt" => Ok(Operator::Lt),
        "lte" => Ok(Operator::LtEq),
        "and" => Ok(Operator::And),
        "or" => Ok(Operator::Or),
        other => Err(CdfError::contract(format!(
            "CDF expression function {other:?} has no native/DataFusion identity lowering"
        ))),
    }
}

fn operator_name(operator: Operator) -> Result<&'static str> {
    match operator {
        Operator::Eq => Ok("eq"),
        Operator::NotEq => Ok("neq"),
        Operator::Gt => Ok("gt"),
        Operator::GtEq => Ok("gte"),
        Operator::Lt => Ok("lt"),
        Operator::LtEq => Ok("lte"),
        Operator::And => Ok("and"),
        Operator::Or => Ok("or"),
        other => Err(CdfError::contract(format!(
            "DataFusion operator {other:?} has no admitted CDF native lowering"
        ))),
    }
}

fn to_scalar(value: &ExpressionLiteral) -> Result<ScalarValue> {
    match value {
        ExpressionLiteral::Null => Ok(ScalarValue::Null),
        ExpressionLiteral::Boolean(value) => Ok(ScalarValue::Boolean(Some(*value))),
        ExpressionLiteral::Signed(value) => Ok(ScalarValue::Int64(Some(*value))),
        ExpressionLiteral::Unsigned(value) => Ok(ScalarValue::UInt64(Some(*value))),
        ExpressionLiteral::Float64Bits(bits) => {
            Ok(ScalarValue::Float64(Some(f64::from_bits(*bits))))
        }
        ExpressionLiteral::String(value) => Ok(ScalarValue::Utf8(Some(value.clone()))),
        ExpressionLiteral::StringList(_) => Err(CdfError::contract(
            "list literals require a named native function lowering",
        )),
        other => Err(CdfError::contract(format!(
            "expression literal {other:?} is not admitted by this engine version"
        ))),
    }
}

fn from_scalar(value: &ScalarValue) -> Result<ExpressionLiteral> {
    match value {
        ScalarValue::Null => Ok(ExpressionLiteral::Null),
        ScalarValue::Boolean(Some(value)) => Ok(ExpressionLiteral::Boolean(*value)),
        ScalarValue::Int8(Some(value)) => Ok(ExpressionLiteral::Signed(i64::from(*value))),
        ScalarValue::Int16(Some(value)) => Ok(ExpressionLiteral::Signed(i64::from(*value))),
        ScalarValue::Int32(Some(value)) => Ok(ExpressionLiteral::Signed(i64::from(*value))),
        ScalarValue::Int64(Some(value)) => Ok(ExpressionLiteral::Signed(*value)),
        ScalarValue::UInt8(Some(value)) => Ok(ExpressionLiteral::Unsigned(u64::from(*value))),
        ScalarValue::UInt16(Some(value)) => Ok(ExpressionLiteral::Unsigned(u64::from(*value))),
        ScalarValue::UInt32(Some(value)) => Ok(ExpressionLiteral::Unsigned(u64::from(*value))),
        ScalarValue::UInt64(Some(value)) => Ok(ExpressionLiteral::Unsigned(*value)),
        ScalarValue::Float64(Some(value)) => ExpressionLiteral::finite_float64(*value),
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
            Ok(ExpressionLiteral::String(value.clone()))
        }
        ScalarValue::Boolean(None)
        | ScalarValue::Int8(None)
        | ScalarValue::Int16(None)
        | ScalarValue::Int32(None)
        | ScalarValue::Int64(None)
        | ScalarValue::UInt8(None)
        | ScalarValue::UInt16(None)
        | ScalarValue::UInt32(None)
        | ScalarValue::UInt64(None)
        | ScalarValue::Float64(None)
        | ScalarValue::Utf8(None)
        | ScalarValue::LargeUtf8(None) => Ok(ExpressionLiteral::Null),
        other => Err(CdfError::contract(format!(
            "DataFusion literal {other:?} has no exact CDF serialized literal"
        ))),
    }
}

fn lint_expression(expression: &Expression) -> Vec<ExpressionLint> {
    let mut lints = Vec::new();
    if is_unsatisfiable_range(&expression.root) {
        lints.push(ExpressionLint {
            code: ExpressionLintCode::UnsatisfiableRange,
            message: "expression contains a provably empty range".to_owned(),
        });
    }
    if matches!(
        expression.root,
        ExpressionNode::Literal {
            value: ExpressionLiteral::Boolean(true)
        }
    ) {
        lints.push(ExpressionLint {
            code: ExpressionLintCode::AlwaysTrue,
            message: "expression is provably always true".to_owned(),
        });
    }
    lints
}

fn is_unsatisfiable_range(node: &ExpressionNode) -> bool {
    let ExpressionNode::Call {
        function,
        arguments,
    } = node
    else {
        return false;
    };
    if function.name != "and" || arguments.len() != 2 {
        return arguments.iter().any(is_unsatisfiable_range);
    }
    let Some((left_column, left_op, left_value)) = comparison(&arguments[0]) else {
        return false;
    };
    let Some((right_column, right_op, right_value)) = comparison(&arguments[1]) else {
        return false;
    };
    if left_column != right_column {
        return false;
    }
    let (lower_op, lower, upper_op, upper) =
        if matches!(left_op, "gt" | "gte") && matches!(right_op, "lt" | "lte") {
            (left_op, left_value, right_op, right_value)
        } else if matches!(right_op, "gt" | "gte") && matches!(left_op, "lt" | "lte") {
            (right_op, right_value, left_op, left_value)
        } else {
            return false;
        };
    match (lower, upper) {
        (ExpressionLiteral::Signed(lower), ExpressionLiteral::Signed(upper)) => {
            lower > upper || (lower == upper && (lower_op == "gt" || upper_op == "lt"))
        }
        (ExpressionLiteral::Unsigned(lower), ExpressionLiteral::Unsigned(upper)) => {
            lower > upper || (lower == upper && (lower_op == "gt" || upper_op == "lt"))
        }
        _ => false,
    }
}

fn comparison(node: &ExpressionNode) -> Option<(&str, &str, &ExpressionLiteral)> {
    let ExpressionNode::Call {
        function,
        arguments,
    } = node
    else {
        return None;
    };
    if arguments.len() != 2 {
        return None;
    }
    let ExpressionNode::Column { name } = &arguments[0] else {
        return None;
    };
    let ExpressionNode::Literal { value } = &arguments[1] else {
        return None;
    };
    Some((name, function.name.as_str(), value))
}

fn datafusion_error(error: impl std::fmt::Display) -> CdfError {
    CdfError::contract(format!("DataFusion expression planning failed: {error}"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, BooleanArray, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        LargeStringArray, RecordBatch, StringArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use arrow_select::filter::filter_record_batch;
    use cdf_contract::{
        Expression, ExpressionLintCode, ExpressionLiteral, ExpressionNode, ExpressionUse,
        FunctionReference,
    };
    use datafusion::{common::DFSchema, execution::context::SessionContext};

    use super::{
        mark_cursor_subsumed, plan_expression, record_exact_source_expression,
        record_native_contract_expression, to_datafusion,
    };
    use cdf_expression::{apply_bound_filters, bind_filter_expressions};

    fn schema() -> Schema {
        Schema::new(vec![Field::new("id", DataType::Int64, true)])
    }

    #[test]
    fn expression_serialization_and_datafusion_round_trip_are_stable() {
        let expression = Expression::parse_comparison("id >= 7").unwrap();
        let encoded = serde_json::to_string(&expression).unwrap();
        assert_eq!(
            encoded,
            r#"{"version":1,"root":{"kind":"call","function":{"namespace":"cdf","name":"gte","version":"1"},"arguments":[{"kind":"column","name":"id"},{"kind":"literal","value":{"kind":"signed","value":7}}]}}"#
        );
        let planned =
            plan_expression(expression.clone(), ExpressionUse::Filter, &schema()).unwrap();
        assert_eq!(planned.original, expression);
        assert_eq!(planned.optimized, planned.original);
        assert_eq!(planned.functions, vec![FunctionReference::cdf("gte")]);
        assert_eq!(
            planned.optimizer.version,
            cdf_contract::DATAFUSION_EXPRESSION_PIN
        );
        assert_eq!(planned.substrait_version, None);
    }

    #[test]
    fn exact_source_temporal_pushdown_records_compiled_plan_without_native_reparse() {
        let expression =
            Expression::parse_comparison("updated_at >= '2026-07-12T00:00:00Z'").unwrap();
        let mut planned = record_exact_source_expression(expression.clone()).unwrap();
        planned.source_text = Some("updated_at >= '2026-07-12T00:00:00Z'".to_owned());

        planned.validate_recorded().unwrap();
        assert_eq!(planned.original, expression);
        assert_eq!(planned.optimized, expression);
        assert_eq!(
            planned.optimizer.name,
            cdf_contract::SOURCE_EXACT_PUSHDOWN_OPTIMIZER
        );
    }

    #[test]
    fn expression_linter_is_conservative_for_ranges_and_constants() {
        let empty_range = Expression::call(
            "and",
            vec![
                Expression::call(
                    "gte",
                    vec![
                        ExpressionNode::Column {
                            name: "id".to_owned(),
                        },
                        ExpressionNode::Literal {
                            value: ExpressionLiteral::Signed(10),
                        },
                    ],
                )
                .root,
                Expression::call(
                    "lt",
                    vec![
                        ExpressionNode::Column {
                            name: "id".to_owned(),
                        },
                        ExpressionNode::Literal {
                            value: ExpressionLiteral::Signed(10),
                        },
                    ],
                )
                .root,
            ],
        );
        let planned = plan_expression(empty_range, ExpressionUse::Filter, &schema()).unwrap();
        assert!(
            planned
                .lints
                .iter()
                .any(|lint| lint.code == ExpressionLintCode::UnsatisfiableRange)
        );

        let unknown = Expression::call(
            "gte",
            vec![
                ExpressionNode::Column {
                    name: "id".to_owned(),
                },
                ExpressionNode::Literal {
                    value: ExpressionLiteral::Signed(10),
                },
            ],
        );
        let planned = plan_expression(unknown, ExpressionUse::Filter, &schema()).unwrap();
        assert!(planned.lints.is_empty());

        let temporal_range = Expression::call(
            "and",
            vec![
                Expression::parse_comparison("updated_at >= '2026-07-12T01:00:00+01:00'")
                    .unwrap()
                    .root,
                Expression::parse_comparison("updated_at < '2026-07-12T00:30:00Z'")
                    .unwrap()
                    .root,
            ],
        );
        let planned = record_exact_source_expression(temporal_range).unwrap();
        assert!(
            !planned
                .lints
                .iter()
                .any(|lint| lint.code == ExpressionLintCode::UnsatisfiableRange)
        );
    }

    #[test]
    fn unsupported_identity_function_fails_at_plan_time() {
        let expression = Expression::call(
            "mystery",
            vec![ExpressionNode::Column {
                name: "id".to_owned(),
            }],
        );
        let error = plan_expression(expression, ExpressionUse::Filter, &schema()).unwrap_err();
        assert!(error.to_string().contains("unsupported arity"));
    }

    #[test]
    fn expression_linter_marks_only_provably_subsumed_cursor_bounds() {
        let mut planned = ["id >= 10", "id >= 5", "id <= 20"]
            .into_iter()
            .map(|value| {
                plan_expression(
                    Expression::parse_comparison(value).unwrap(),
                    ExpressionUse::Filter,
                    &schema(),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        mark_cursor_subsumed(&mut planned, "id");
        assert!(
            !planned[0]
                .lints
                .iter()
                .any(|lint| lint.code == ExpressionLintCode::CursorSubsumed)
        );
        assert!(
            planned[1]
                .lints
                .iter()
                .any(|lint| lint.code == ExpressionLintCode::CursorSubsumed)
        );
        assert!(
            !planned[2]
                .lints
                .iter()
                .any(|lint| lint.code == ExpressionLintCode::CursorSubsumed)
        );

        let mut temporal = [
            "updated_at >= '2026-07-12T01:00:00+01:00'",
            "updated_at >= '2026-07-12T00:30:00Z'",
        ]
        .into_iter()
        .map(|value| {
            let mut planned =
                record_exact_source_expression(Expression::parse_comparison(value).unwrap())
                    .unwrap();
            planned.source_text = Some(value.to_owned());
            planned
        })
        .collect::<Vec<_>>();
        mark_cursor_subsumed(&mut temporal, "updated_at");
        assert!(temporal.iter().all(|planned| {
            !planned
                .lints
                .iter()
                .any(|lint| lint.code == ExpressionLintCode::CursorSubsumed)
        }));
    }

    #[test]
    fn expression_linter_proves_contract_range_only_with_schema_type() {
        let expression = Expression::call(
            "in_range",
            vec![
                ExpressionNode::Column {
                    name: "id".to_owned(),
                },
                ExpressionNode::Literal {
                    value: ExpressionLiteral::String("10".to_owned()),
                },
                ExpressionNode::Literal {
                    value: ExpressionLiteral::String("2".to_owned()),
                },
            ],
        );
        let planned = record_native_contract_expression(expression, &schema()).unwrap();
        assert!(
            planned
                .lints
                .iter()
                .any(|lint| lint.code == ExpressionLintCode::UnsatisfiableRange)
        );
    }

    #[test]
    fn native_identity_filter_matches_datafusion_for_every_admitted_arrow_type_and_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("i8", DataType::Int8, true),
            Field::new("i16", DataType::Int16, true),
            Field::new("i32", DataType::Int32, true),
            Field::new("i64", DataType::Int64, true),
            Field::new("u8", DataType::UInt8, true),
            Field::new("u16", DataType::UInt16, true),
            Field::new("u32", DataType::UInt32, true),
            Field::new("u64", DataType::UInt64, true),
            Field::new("f64", DataType::Float64, true),
            Field::new("text", DataType::Utf8, true),
            Field::new("large_text", DataType::LargeUtf8, true),
            Field::new("flag", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int8Array::from(vec![Some(1), None, Some(3)])) as ArrayRef,
                Arc::new(Int16Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(UInt8Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(UInt16Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(UInt32Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(UInt64Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(Float64Array::from(vec![Some(1.0), None, Some(3.0)])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
                Arc::new(LargeStringArray::from(vec![Some("a"), None, Some("c")])),
                Arc::new(BooleanArray::from(vec![Some(false), None, Some(true)])),
            ],
        )
        .unwrap();
        let df_schema = DFSchema::try_from(schema.as_ref().clone()).unwrap();
        let session = SessionContext::new();
        let assert_parity = |label: &str, expression: Expression| {
            let planned =
                plan_expression(expression, ExpressionUse::Filter, schema.as_ref()).unwrap();
            let bound =
                bind_filter_expressions(std::slice::from_ref(&planned), schema.as_ref()).unwrap();
            let native = apply_bound_filters(&batch, &bound).unwrap();
            let physical = session
                .create_physical_expr(to_datafusion(&planned.optimized.root).unwrap(), &df_schema)
                .unwrap();
            let mask = physical
                .evaluate(&batch)
                .unwrap()
                .into_array(batch.num_rows())
                .unwrap();
            let mask = mask.as_any().downcast_ref::<BooleanArray>().unwrap();
            let reference = filter_record_batch(&batch, mask).unwrap();
            assert_eq!(native, reference, "native/DataFusion drift for {label}");
        };

        for field in ["i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64"] {
            for operator in ["=", "!=", ">", ">=", "<", "<="] {
                let source = format!("{field} {operator} 2");
                assert_parity(&source, Expression::parse_comparison(&source).unwrap());
            }
        }
        for operator in ["=", "!=", ">", ">=", "<", "<="] {
            let source = format!("f64 {operator} 2.5");
            assert_parity(&source, Expression::parse_comparison(&source).unwrap());
        }
        for field in ["text", "large_text"] {
            for operator in ["=", "!=", ">", ">=", "<", "<="] {
                let source = format!("{field} {operator} 'b'");
                assert_parity(&source, Expression::parse_comparison(&source).unwrap());
            }
        }
        for operator in ["=", "!="] {
            let source = format!("flag {operator} true");
            assert_parity(&source, Expression::parse_comparison(&source).unwrap());
        }
        let unary = |name: &str, column: &str| {
            Expression::call(
                name,
                vec![ExpressionNode::Column {
                    name: column.to_owned(),
                }],
            )
        };
        assert_parity("is_null(i8)", unary("is_null", "i8"));
        assert_parity("is_not_null(text)", unary("is_not_null", "text"));
        assert_parity("not(flag)", unary("not", "flag"));
        assert_parity(
            "flag OR is_null(i8)",
            Expression::call(
                "or",
                vec![
                    ExpressionNode::Column {
                        name: "flag".to_owned(),
                    },
                    unary("is_null", "i8").root,
                ],
            ),
        );
        assert_parity(
            "flag AND is_not_null(text)",
            Expression::call(
                "and",
                vec![
                    ExpressionNode::Column {
                        name: "flag".to_owned(),
                    },
                    unary("is_not_null", "text").root,
                ],
            ),
        );
    }

    #[test]
    fn identity_planning_rejects_types_without_native_lowering() {
        let schema = Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(38, 9),
            true,
        )]);
        let error = plan_expression(
            Expression::parse_comparison("amount >= 2").unwrap(),
            ExpressionUse::Filter,
            &schema,
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("has no exact CDF serialized literal"),
            "{error}"
        );
    }

    #[test]
    #[ignore = "release-only expression throughput evidence"]
    fn native_vector_filter_stays_within_datafusion_arrow_kernel_roofline() {
        let rows = 64 * 1024;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(0..rows as i64))],
        )
        .unwrap();
        let planned = plan_expression(
            Expression::parse_comparison("id >= 32768").unwrap(),
            ExpressionUse::Filter,
            schema.as_ref(),
        )
        .unwrap();
        let df_schema = DFSchema::try_from(schema.as_ref().clone()).unwrap();
        let physical = SessionContext::new()
            .create_physical_expr(to_datafusion(&planned.optimized.root).unwrap(), &df_schema)
            .unwrap();
        let bound =
            bind_filter_expressions(std::slice::from_ref(&planned), schema.as_ref()).unwrap();
        let measure = |iterations: usize, native: bool| {
            let started = std::time::Instant::now();
            for _ in 0..iterations {
                if native {
                    std::hint::black_box(apply_bound_filters(&batch, &bound).unwrap());
                } else {
                    let mask = physical
                        .evaluate(&batch)
                        .unwrap()
                        .into_array(batch.num_rows())
                        .unwrap();
                    let mask = mask.as_any().downcast_ref::<BooleanArray>().unwrap();
                    std::hint::black_box(filter_record_batch(&batch, mask).unwrap());
                }
            }
            started.elapsed()
        };
        // Warm both implementations after linking, then alternate sample order to avoid
        // attributing CPU frequency and cache transients to either implementation.
        measure(50, true);
        measure(50, false);
        let iterations = 200;
        let mut native_samples = Vec::with_capacity(7);
        let mut reference_samples = Vec::with_capacity(7);
        for sample in 0..7 {
            if sample % 2 == 0 {
                native_samples.push(measure(iterations, true));
                reference_samples.push(measure(iterations, false));
            } else {
                reference_samples.push(measure(iterations, false));
                native_samples.push(measure(iterations, true));
            }
        }
        native_samples.sort_unstable();
        reference_samples.sort_unstable();
        let native = native_samples[native_samples.len() / 2];
        let reference = reference_samples[reference_samples.len() / 2];
        eprintln!(
            "native_vector_filter_median={native:?} datafusion_physical_filter_median={reference:?} rows_per_sample={} iterations={iterations} samples={}",
            rows * iterations,
            native_samples.len()
        );
        assert!(
            native.as_nanos() * 100 <= reference.as_nanos() * 115,
            "native identity lowering exceeded the 15% DataFusion/Arrow roofline allowance: native={native:?} reference={reference:?}"
        );
    }
}
