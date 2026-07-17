#![doc = "Shared Arrow-native execution for CDF's compiled expression IR."]

use arrow_arith::boolean::{and_kleene, is_not_null, is_null, not, or_kleene};
use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeStringArray, RecordBatch, Scalar, StringArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow_ord::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::filter::filter_record_batch;
use cdf_contract::{
    CDF_FUNCTION_NAMESPACE, CDF_FUNCTION_VERSION, ExpressionLiteral, ExpressionNode,
    PlannedExpression, TransformDescription,
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

pub enum BoundExpressionTransform {
    Derive {
        column: String,
        expression: BoundBooleanExpression,
    },
    Filter(BoundBooleanExpression),
}

pub fn bind_filter_expressions(
    expressions: &[PlannedExpression],
    schema: &Schema,
) -> Result<Vec<BoundBooleanExpression>> {
    expressions
        .iter()
        .map(|expression| bind_boolean_expression(&expression.optimized.root, schema))
        .collect()
}

pub fn bind_expression_transforms(
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
                    CdfError::contract("derive transform has no recorded compiled expression plan")
                })?;
                bound.push(BoundExpressionTransform::Derive {
                    column: column.clone(),
                    expression: bind_boolean_expression(&expression.optimized.root, &schema)?,
                });
                schema = schema_with_derived_boolean(&schema, column);
            }
            TransformDescription::Filter { .. } => {
                let expression = planned.next().ok_or_else(|| {
                    CdfError::contract("filter transform has no recorded compiled expression plan")
                })?;
                bound.push(BoundExpressionTransform::Filter(bind_boolean_expression(
                    &expression.optimized.root,
                    &schema,
                )?));
            }
            _ => {}
        }
    }
    if planned.next().is_some() {
        return Err(CdfError::contract(
            "recorded compiled expression plan has extra transform expressions",
        ));
    }
    Ok(bound)
}

pub fn expression_transform_output_schema(
    transforms: &[TransformDescription],
    schema: &Schema,
) -> Schema {
    transforms.iter().fold(schema.clone(), |schema, transform| {
        if let TransformDescription::Derive { column, .. } = transform {
            schema_with_derived_boolean(&schema, column)
        } else {
            schema
        }
    })
}

fn schema_with_derived_boolean(schema: &Schema, column: &str) -> Schema {
    let field = std::sync::Arc::new(Field::new(column, DataType::Boolean, true));
    let mut fields = schema.fields().iter().cloned().collect::<Vec<_>>();
    if let Ok(index) = schema.index_of(column) {
        fields[index] = field;
    } else {
        fields.push(field);
    }
    Schema::new_with_metadata(fields, schema.metadata().clone())
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

pub fn apply_expression_transforms(
    batch: RecordBatch,
    transforms: &[TransformDescription],
    planned: &[PlannedExpression],
) -> Result<RecordBatch> {
    let bound = bind_expression_transforms(transforms, planned, batch.schema().as_ref())?;
    apply_bound_expression_transforms(batch, &bound)
}

pub fn apply_bound_expression_transforms(
    mut batch: RecordBatch,
    transforms: &[BoundExpressionTransform],
) -> Result<RecordBatch> {
    for transform in transforms {
        match transform {
            BoundExpressionTransform::Derive { column, expression } => {
                let values = evaluate_bound_expression(&batch, expression)?;
                let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
                let mut columns = batch.columns().to_vec();
                let field = std::sync::Arc::new(arrow_schema::Field::new(
                    column,
                    arrow_schema::DataType::Boolean,
                    true,
                ));
                if let Ok(index) = batch.schema().index_of(column) {
                    fields[index] = field;
                    columns[index] = std::sync::Arc::new(values);
                } else {
                    fields.push(field);
                    columns.push(std::sync::Arc::new(values));
                }
                batch = RecordBatch::try_new(
                    std::sync::Arc::new(arrow_schema::Schema::new_with_metadata(
                        fields,
                        batch.schema().metadata().clone(),
                    )),
                    columns,
                )?;
            }
            BoundExpressionTransform::Filter(expression) => {
                batch = apply_bound_filters(&batch, std::slice::from_ref(expression))?;
            }
        }
    }
    Ok(batch)
}

pub fn bind_boolean_expression(
    node: &ExpressionNode,
    schema: &Schema,
) -> Result<BoundBooleanExpression> {
    match node {
        ExpressionNode::Column { name } => {
            let column = bind_column(name, schema)?;
            if column.data_type != DataType::Boolean {
                return Err(CdfError::contract(format!(
                    "predicate field {name:?} does not have its planned boolean type"
                )));
            }
            Ok(BoundBooleanExpression::Column(column))
        }
        ExpressionNode::Literal {
            value: ExpressionLiteral::Boolean(value),
        } => Ok(BoundBooleanExpression::Literal(Some(*value))),
        ExpressionNode::Literal {
            value: ExpressionLiteral::Null,
        } => Ok(BoundBooleanExpression::Literal(None)),
        ExpressionNode::Call {
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
                ("not", [value]) => Ok(BoundBooleanExpression::Not(Box::new(
                    bind_boolean_expression(value, schema)?,
                ))),
                ("and", [left, right]) => Ok(BoundBooleanExpression::And(
                    Box::new(bind_boolean_expression(left, schema)?),
                    Box::new(bind_boolean_expression(right, schema)?),
                )),
                ("or", [left, right]) => Ok(BoundBooleanExpression::Or(
                    Box::new(bind_boolean_expression(left, schema)?),
                    Box::new(bind_boolean_expression(right, schema)?),
                )),
                ("is_null", [ExpressionNode::Column { name }]) => {
                    Ok(BoundBooleanExpression::IsNull(bind_column(name, schema)?))
                }
                ("is_not_null", [ExpressionNode::Column { name }]) => Ok(
                    BoundBooleanExpression::IsNotNull(bind_column(name, schema)?),
                ),
                (
                    operator @ ("eq" | "neq" | "gt" | "gte" | "lt" | "lte"),
                    [
                        ExpressionNode::Column { name },
                        ExpressionNode::Literal { value },
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
    value: &ExpressionLiteral,
) -> Result<ArrayRef> {
    macro_rules! signed {
        ($array:ty, $native:ty) => {{
            let value = match value {
                ExpressionLiteral::Signed(value) => Some(
                    <$native>::try_from(*value)
                        .map_err(|_| literal_type(name, stringify!($native)))?,
                ),
                ExpressionLiteral::Null => None,
                _ => return Err(literal_type(name, "signed integer")),
            };
            std::sync::Arc::new(<$array>::from(vec![value])) as ArrayRef
        }};
    }
    macro_rules! unsigned {
        ($array:ty, $native:ty) => {{
            let value = match value {
                ExpressionLiteral::Unsigned(value) => Some(
                    <$native>::try_from(*value)
                        .map_err(|_| literal_type(name, stringify!($native)))?,
                ),
                ExpressionLiteral::Signed(value) if *value >= 0 => Some(
                    <$native>::try_from(*value as u64)
                        .map_err(|_| literal_type(name, stringify!($native)))?,
                ),
                ExpressionLiteral::Null => None,
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
                ExpressionLiteral::Float64Bits(bits) => Some(f64::from_bits(*bits)),
                ExpressionLiteral::Null => None,
                _ => return Err(literal_type(name, "float64")),
            };
            std::sync::Arc::new(Float64Array::from(vec![value]))
        }
        arrow_schema::DataType::Utf8 => {
            let value = match value {
                ExpressionLiteral::String(value) => Some(value.as_str()),
                ExpressionLiteral::Null => None,
                _ => return Err(literal_type(name, "string")),
            };
            std::sync::Arc::new(StringArray::from(vec![value]))
        }
        arrow_schema::DataType::LargeUtf8 => {
            let value = match value {
                ExpressionLiteral::String(value) => Some(value.as_str()),
                ExpressionLiteral::Null => None,
                _ => return Err(literal_type(name, "string")),
            };
            std::sync::Arc::new(LargeStringArray::from(vec![value]))
        }
        arrow_schema::DataType::Boolean => {
            let value = match value {
                ExpressionLiteral::Boolean(value) => Some(*value),
                ExpressionLiteral::Null => None,
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
