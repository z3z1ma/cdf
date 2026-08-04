use cdf_contract::{ScalarExpression, ScalarExpressionKind, ScalarExpressionNode};
use cdf_kernel::{CanonicalArrowType, CanonicalArrowUnionMode, CdfError, Result};

pub(crate) fn expression_working_set_bytes<'a>(
    expressions: impl IntoIterator<Item = &'a ScalarExpression>,
    rows: usize,
) -> Result<u64> {
    expression_nodes_working_set_bytes(
        expressions.into_iter().map(|expression| &expression.root),
        rows,
    )
}

pub(crate) fn expression_nodes_working_set_bytes<'a>(
    nodes: impl IntoIterator<Item = &'a ScalarExpressionNode>,
    rows: usize,
) -> Result<u64> {
    let outputs = nodes.into_iter().try_fold(0_u64, |total, node| {
        total
            .checked_add(node_working_set_bytes(node, rows, true)?)
            .ok_or_else(allocation_bound_error)
    })?;
    // DataFusion/Arrow builders may hold a completed child and a same-sized construction buffer
    // concurrently. Reserving twice the complete expression-output tree bounds that overlap
    // without depending on function names or implementation-specific allocation order.
    outputs.checked_mul(2).ok_or_else(allocation_bound_error)
}

fn allocation_bound_error() -> CdfError {
    CdfError::environment("scalar allocation bound exceeds addressable CDF memory")
}

fn node_working_set_bytes(
    node: &ScalarExpressionNode,
    rows: usize,
    top_level: bool,
) -> Result<u64> {
    let children = match &node.expression {
        ScalarExpressionKind::Column { .. } => return Ok(0),
        // DataFusion evaluates a durable literal as a scalar `ColumnarValue`; broadcasting is
        // owned by the consuming kernel and covered by that node's output bound. A top-level
        // literal is broadcast by CDF itself and therefore materializes an output array.
        ScalarExpressionKind::Literal { .. } if !top_level => return Ok(0),
        ScalarExpressionKind::Literal { .. } => 0,
        ScalarExpressionKind::Unary { argument, .. }
        | ScalarExpressionKind::Cast { argument, .. } => {
            node_working_set_bytes(argument, rows, false)?
        }
        ScalarExpressionKind::Binary { left, right, .. } => {
            node_working_set_bytes(left, rows, false)?
                .checked_add(node_working_set_bytes(right, rows, false)?)
                .ok_or_else(allocation_bound_error)?
        }
        ScalarExpressionKind::Call { arguments, .. } => {
            arguments.iter().try_fold(0_u64, |total, argument| {
                total
                    .checked_add(node_working_set_bytes(argument, rows, false)?)
                    .ok_or_else(allocation_bound_error)
            })?
        }
    };
    children
        .checked_add(maximum_array_bytes(
            &node.scalar_type.data_type,
            u64::try_from(rows).map_err(|_| CdfError::contract("scalar row count exceeds u64"))?,
        )?)
        .ok_or_else(allocation_bound_error)
}

fn maximum_array_bytes(data_type: &CanonicalArrowType, rows: u64) -> Result<u64> {
    let validity = rows
        .checked_div(8)
        .and_then(|bytes| bytes.checked_add(u64::from(!rows.is_multiple_of(8))))
        .ok_or_else(allocation_bound_error)?;
    let checked = |value: Option<u64>| value.ok_or_else(allocation_bound_error);
    let fixed = |width: u64| {
        checked(
            rows.checked_mul(width)
                .and_then(|bytes| bytes.checked_add(validity)),
        )
    };
    let bytes = match data_type {
        CanonicalArrowType::Null => 0,
        CanonicalArrowType::Boolean => checked(
            rows.checked_div(8)
                .and_then(|bytes| bytes.checked_add(u64::from(!rows.is_multiple_of(8))))
                .and_then(|bytes| bytes.checked_add(validity)),
        )?,
        CanonicalArrowType::Int { bits, .. }
        | CanonicalArrowType::Float { bits }
        | CanonicalArrowType::Time { bits, .. } => fixed(u64::from(*bits) / 8)?,
        CanonicalArrowType::Decimal { bits, .. } => fixed(u64::from(*bits) / 8)?,
        CanonicalArrowType::Timestamp { .. } | CanonicalArrowType::Duration { .. } => fixed(8)?,
        CanonicalArrowType::Date { unit } => fixed(match unit {
            cdf_kernel::CanonicalArrowDateUnit::Day => 4,
            cdf_kernel::CanonicalArrowDateUnit::Millisecond => 8,
            _ => {
                return Err(CdfError::contract(
                    "canonical date unit has no scalar allocation bound",
                ));
            }
        })?,
        CanonicalArrowType::Interval { unit } => fixed(match unit {
            cdf_kernel::CanonicalArrowIntervalUnit::YearMonth => 4,
            cdf_kernel::CanonicalArrowIntervalUnit::DayTime => 8,
            cdf_kernel::CanonicalArrowIntervalUnit::MonthDayNano => 16,
            _ => {
                return Err(CdfError::contract(
                    "canonical interval unit has no scalar allocation bound",
                ));
            }
        })?,
        CanonicalArrowType::FixedSizeBinary { byte_width } => fixed(
            u64::try_from(*byte_width)
                .map_err(|_| CdfError::contract("fixed-size binary width is negative"))?,
        )?,
        CanonicalArrowType::Binary { offset_width } | CanonicalArrowType::Utf8 { offset_width } => {
            let offset_bytes = u64::from(*offset_width) / 8;
            let values = if *offset_width == 32 {
                i32::MAX as u64
            } else {
                i64::MAX as u64
            };
            checked(
                rows.checked_add(1)
                    .and_then(|count| count.checked_mul(offset_bytes))
                    .and_then(|offsets| offsets.checked_add(values))
                    .and_then(|bytes| bytes.checked_add(validity)),
            )?
        }
        CanonicalArrowType::BinaryView | CanonicalArrowType::Utf8View => checked(
            rows.checked_mul(16)
                .and_then(|views| {
                    rows.checked_mul(u32::MAX as u64)
                        .and_then(|values| views.checked_add(values))
                })
                .and_then(|bytes| bytes.checked_add(validity)),
        )?,
        CanonicalArrowType::List {
            field,
            offset_width,
            view,
        } => {
            let offset_bytes = u64::from(*offset_width) / 8;
            let child_rows = if *offset_width == 32 {
                i32::MAX as u64
            } else {
                i64::MAX as u64
            };
            let offsets = checked(
                rows.checked_add(1)
                    .and_then(|count| count.checked_mul(offset_bytes)),
            )?;
            let sizes = if *view {
                checked(rows.checked_mul(offset_bytes))?
            } else {
                0
            };
            let child = maximum_array_bytes(&field.data_type, child_rows)?;
            checked(
                offsets
                    .checked_add(sizes)
                    .and_then(|bytes| bytes.checked_add(validity))
                    .and_then(|bytes| bytes.checked_add(child)),
            )?
        }
        CanonicalArrowType::FixedSizeList { field, length } => {
            let length = u64::try_from(*length)
                .map_err(|_| CdfError::contract("fixed-size list length is negative"))?;
            let child_rows = checked(rows.checked_mul(length))?;
            checked(maximum_array_bytes(&field.data_type, child_rows)?.checked_add(validity))?
        }
        CanonicalArrowType::Struct { fields } => {
            fields.iter().try_fold(validity, |total, field| {
                total
                    .checked_add(maximum_array_bytes(&field.data_type, rows)?)
                    .ok_or_else(allocation_bound_error)
            })?
        }
        CanonicalArrowType::Map { field, .. } => {
            let child = maximum_array_bytes(&field.data_type, i32::MAX as u64)?;
            checked(
                rows.checked_add(1)
                    .and_then(|count| count.checked_mul(4))
                    .and_then(|offsets| offsets.checked_add(validity))
                    .and_then(|bytes| bytes.checked_add(child)),
            )?
        }
        CanonicalArrowType::Union { fields, mode } => {
            let base = match mode {
                CanonicalArrowUnionMode::Sparse => rows,
                CanonicalArrowUnionMode::Dense => checked(rows.checked_mul(5))?,
                _ => {
                    return Err(CdfError::contract(
                        "canonical union mode has no scalar allocation bound",
                    ));
                }
            };
            fields.iter().try_fold(base, |total, field| {
                total
                    .checked_add(maximum_array_bytes(&field.field.data_type, rows)?)
                    .ok_or_else(allocation_bound_error)
            })?
        }
        CanonicalArrowType::Dictionary { key, value } => {
            checked(maximum_array_bytes(key, rows)?.checked_add(maximum_array_bytes(value, rows)?))?
        }
        CanonicalArrowType::RunEndEncoded { run_ends, values } => checked(
            maximum_array_bytes(&run_ends.data_type, rows)?
                .checked_add(maximum_array_bytes(&values.data_type, rows)?),
        )?,
        _ => {
            return Err(CdfError::contract(
                "canonical scalar type has no allocation bound",
            ));
        }
    };
    bytes.checked_add(256).ok_or_else(allocation_bound_error)
}
