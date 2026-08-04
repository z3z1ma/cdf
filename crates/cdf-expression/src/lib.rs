#![doc = "Arrow-native evaluation for bounded declarative adapter predicates."]

use arrow_arith::boolean::{and_kleene, is_not_null, is_null, not, or_kleene};
use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeStringArray, RecordBatch, Scalar, StringArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow_ord::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow_schema::{DataType, Schema};
use arrow_select::filter::filter_record_batch;
use cdf_contract::{
    CDF_FUNCTION_NAMESPACE, CDF_FUNCTION_VERSION, DeclarativeExpression,
    DeclarativeExpressionLiteral, DeclarativeExpressionNode,
};
use cdf_kernel::{CdfError, Result};

#[derive(Clone)]
pub enum BoundBooleanExpression {
    Column(BoundColumn),
    Literal(Option<bool>),
    Not(Box<Self>),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    IsNull(BoundColumn),
    IsNotNull(BoundColumn),
    Comparison {
        column: BoundColumn,
        operator: BoundComparisonOperator,
        scalar: Scalar<ArrayRef>,
    },
}

#[derive(Clone)]
pub struct BoundColumn {
    index: usize,
    name: String,
    data_type: DataType,
}

#[derive(Clone, Copy)]
pub enum BoundComparisonOperator {
    Equal,
    NotEqual,
    Greater,
    GreaterOrEqual,
    Less,
    LessOrEqual,
}

pub fn apply_bound_filters(
    batch: &RecordBatch,
    expressions: &[BoundBooleanExpression],
) -> Result<RecordBatch> {
    if expressions.is_empty() || batch.num_rows() == 0 {
        return Ok(batch.clone());
    }
    let mut expressions = expressions.iter();
    let mut keep = evaluate_bound_expression(
        batch,
        expressions
            .next()
            .expect("non-empty expression slice checked above"),
    )?;
    for expression in expressions {
        keep = and_kleene(&keep, &evaluate_bound_expression(batch, expression)?)?;
    }

    filter_record_batch(batch, &keep).map_err(CdfError::from)
}

pub fn bind_boolean_expression(
    expression: &DeclarativeExpression,
    schema: &Schema,
) -> Result<BoundBooleanExpression> {
    expression.validate()?;
    bind_boolean_node(&expression.root, schema)
}

fn bind_boolean_node(
    node: &DeclarativeExpressionNode,
    schema: &Schema,
) -> Result<BoundBooleanExpression> {
    match node {
        DeclarativeExpressionNode::Column { name } => {
            let column = bind_column(name, schema)?;
            if column.data_type != DataType::Boolean {
                return Err(CdfError::contract(format!(
                    "predicate field {name:?} does not have its planned boolean type"
                )));
            }
            Ok(BoundBooleanExpression::Column(column))
        }
        DeclarativeExpressionNode::Literal {
            value: DeclarativeExpressionLiteral::Boolean(value),
        } => Ok(BoundBooleanExpression::Literal(Some(*value))),
        DeclarativeExpressionNode::Literal {
            value: DeclarativeExpressionLiteral::Null,
        } => Ok(BoundBooleanExpression::Literal(None)),
        DeclarativeExpressionNode::Call {
            function,
            arguments,
        } => {
            if function.namespace != CDF_FUNCTION_NAMESPACE
                || function.version != CDF_FUNCTION_VERSION
            {
                return Err(CdfError::contract(format!(
                    "unsupported expression function {}.{}@{}; native execution requires the recorded CDF function version",
                    function.namespace, function.name, function.version
                )));
            }
            match (function.name.as_str(), arguments.as_slice()) {
                ("not", [value]) => Ok(BoundBooleanExpression::Not(Box::new(bind_boolean_node(
                    value, schema,
                )?))),
                ("and", [left, right]) => Ok(BoundBooleanExpression::And(
                    Box::new(bind_boolean_node(left, schema)?),
                    Box::new(bind_boolean_node(right, schema)?),
                )),
                ("or", [left, right]) => Ok(BoundBooleanExpression::Or(
                    Box::new(bind_boolean_node(left, schema)?),
                    Box::new(bind_boolean_node(right, schema)?),
                )),
                ("is_null", [DeclarativeExpressionNode::Column { name }]) => {
                    Ok(BoundBooleanExpression::IsNull(bind_column(name, schema)?))
                }
                ("is_not_null", [DeclarativeExpressionNode::Column { name }]) => Ok(
                    BoundBooleanExpression::IsNotNull(bind_column(name, schema)?),
                ),
                (
                    operator @ ("eq" | "neq" | "gt" | "gte" | "lt" | "lte"),
                    [
                        DeclarativeExpressionNode::Column { name },
                        DeclarativeExpressionNode::Literal { value },
                    ],
                ) => {
                    let column = bind_column(name, schema)?;
                    let scalar = Scalar::new(scalar_for_array(name, &column.data_type, value)?);
                    Ok(BoundBooleanExpression::Comparison {
                        column,
                        operator: match operator {
                            "eq" => BoundComparisonOperator::Equal,
                            "neq" => BoundComparisonOperator::NotEqual,
                            "gt" => BoundComparisonOperator::Greater,
                            "gte" => BoundComparisonOperator::GreaterOrEqual,
                            "lt" => BoundComparisonOperator::Less,
                            "lte" => BoundComparisonOperator::LessOrEqual,
                            _ => unreachable!("operator admitted by pattern"),
                        },
                        scalar,
                    })
                }
                (name, _) => Err(CdfError::contract(format!(
                    "recorded expression function {name:?} has no native fused filter lowering"
                ))),
            }
        }
        other => Err(CdfError::contract(format!(
            "recorded expression {other:?} does not produce a boolean filter"
        ))),
    }
}

pub fn evaluate_bound_expression(
    batch: &RecordBatch,
    expression: &BoundBooleanExpression,
) -> Result<BooleanArray> {
    match expression {
        BoundBooleanExpression::Column(column) => bound_column_array(batch, column)?
            .as_any()
            .downcast_ref::<BooleanArray>()
            .cloned()
            .ok_or_else(|| CdfError::contract("bound boolean column changed physical type")),
        BoundBooleanExpression::Literal(Some(value)) => {
            Ok(BooleanArray::from(vec![*value; batch.num_rows()]))
        }
        BoundBooleanExpression::Literal(None) => Ok(BooleanArray::new_null(batch.num_rows())),
        BoundBooleanExpression::Not(value) => Ok(not(&evaluate_bound_expression(batch, value)?)?),
        BoundBooleanExpression::And(left, right) => Ok(and_kleene(
            &evaluate_bound_expression(batch, left)?,
            &evaluate_bound_expression(batch, right)?,
        )?),
        BoundBooleanExpression::Or(left, right) => Ok(or_kleene(
            &evaluate_bound_expression(batch, left)?,
            &evaluate_bound_expression(batch, right)?,
        )?),
        BoundBooleanExpression::IsNull(column) => {
            Ok(is_null(bound_column_array(batch, column)?.as_ref())?)
        }
        BoundBooleanExpression::IsNotNull(column) => {
            Ok(is_not_null(bound_column_array(batch, column)?.as_ref())?)
        }
        BoundBooleanExpression::Comparison {
            column,
            operator,
            scalar,
        } => {
            let array = bound_column_array(batch, column)?;
            let result = match operator {
                BoundComparisonOperator::Equal => eq(array, scalar),
                BoundComparisonOperator::NotEqual => neq(array, scalar),
                BoundComparisonOperator::Greater => gt(array, scalar),
                BoundComparisonOperator::GreaterOrEqual => gt_eq(array, scalar),
                BoundComparisonOperator::Less => lt(array, scalar),
                BoundComparisonOperator::LessOrEqual => lt_eq(array, scalar),
            };
            result.map_err(CdfError::from)
        }
    }
}

fn bind_column(name: &str, schema: &Schema) -> Result<BoundColumn> {
    let index = schema.index_of(name).map_err(|_| missing_field(name))?;
    Ok(BoundColumn {
        index,
        name: name.to_owned(),
        data_type: schema.field(index).data_type().clone(),
    })
}

fn bound_column_array<'a>(batch: &'a RecordBatch, column: &BoundColumn) -> Result<&'a ArrayRef> {
    let batch_schema = batch.schema();
    let field = batch_schema.field(column.index);
    if field.name() != &column.name || field.data_type() != &column.data_type {
        return Err(CdfError::data(format!(
            "bound expression field {:?} at ordinal {} changed to {:?} with type {}; replan against the physical scan schema",
            column.name,
            column.index,
            field.name(),
            field.data_type()
        )));
    }
    Ok(batch.column(column.index))
}

fn scalar_for_array(
    name: &str,
    data_type: &arrow_schema::DataType,
    value: &DeclarativeExpressionLiteral,
) -> Result<ArrayRef> {
    macro_rules! signed {
        ($array:ty, $native:ty) => {{
            let value = match value {
                DeclarativeExpressionLiteral::Signed(value) => Some(
                    <$native>::try_from(*value)
                        .map_err(|_| literal_type(name, stringify!($native)))?,
                ),
                DeclarativeExpressionLiteral::Null => None,
                _ => return Err(literal_type(name, "signed integer")),
            };
            std::sync::Arc::new(<$array>::from(vec![value])) as ArrayRef
        }};
    }
    macro_rules! unsigned {
        ($array:ty, $native:ty) => {{
            let value = match value {
                DeclarativeExpressionLiteral::Unsigned(value) => Some(
                    <$native>::try_from(*value)
                        .map_err(|_| literal_type(name, stringify!($native)))?,
                ),
                DeclarativeExpressionLiteral::Signed(value) if *value >= 0 => Some(
                    <$native>::try_from(*value as u64)
                        .map_err(|_| literal_type(name, stringify!($native)))?,
                ),
                DeclarativeExpressionLiteral::Null => None,
                _ => return Err(literal_type(name, "unsigned integer")),
            };
            std::sync::Arc::new(<$array>::from(vec![value])) as ArrayRef
        }};
    }
    Ok(match data_type {
        arrow_schema::DataType::Int8 => signed!(Int8Array, i8),
        arrow_schema::DataType::Int16 => signed!(Int16Array, i16),
        arrow_schema::DataType::Int32 => signed!(Int32Array, i32),
        arrow_schema::DataType::Int64 => signed!(Int64Array, i64),
        arrow_schema::DataType::UInt8 => unsigned!(UInt8Array, u8),
        arrow_schema::DataType::UInt16 => unsigned!(UInt16Array, u16),
        arrow_schema::DataType::UInt32 => unsigned!(UInt32Array, u32),
        arrow_schema::DataType::UInt64 => unsigned!(UInt64Array, u64),
        arrow_schema::DataType::Float64 => {
            let value = match value {
                DeclarativeExpressionLiteral::Float64Bits(bits) => Some(f64::from_bits(*bits)),
                DeclarativeExpressionLiteral::Null => None,
                _ => return Err(literal_type(name, "float64")),
            };
            std::sync::Arc::new(Float64Array::from(vec![value]))
        }
        arrow_schema::DataType::Utf8 => {
            let value = match value {
                DeclarativeExpressionLiteral::String(value) => Some(value.as_str()),
                DeclarativeExpressionLiteral::Null => None,
                _ => return Err(literal_type(name, "string")),
            };
            std::sync::Arc::new(StringArray::from(vec![value]))
        }
        arrow_schema::DataType::LargeUtf8 => {
            let value = match value {
                DeclarativeExpressionLiteral::String(value) => Some(value.as_str()),
                DeclarativeExpressionLiteral::Null => None,
                _ => return Err(literal_type(name, "string")),
            };
            std::sync::Arc::new(LargeStringArray::from(vec![value]))
        }
        arrow_schema::DataType::Boolean => {
            let value = match value {
                DeclarativeExpressionLiteral::Boolean(value) => Some(*value),
                DeclarativeExpressionLiteral::Null => None,
                _ => return Err(literal_type(name, "boolean")),
            };
            std::sync::Arc::new(BooleanArray::from(vec![value]))
        }
        other => {
            return Err(CdfError::contract(format!(
                "predicate field {name:?} has unsupported native filter type {other}"
            )));
        }
    })
}

fn missing_field(name: &str) -> CdfError {
    CdfError::data(format!(
        "predicate field {name:?} is not present in resource batch"
    ))
}

fn literal_type(name: &str, expected: &str) -> CdfError {
    CdfError::contract(format!(
        "predicate field {name:?} requires a {expected} literal"
    ))
}
