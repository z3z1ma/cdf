use arrow_array::{
    Array, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray, UInt32Array, UInt64Array,
};
use arrow_schema::DataType;
use arrow_select::filter::filter_record_batch;
use cdf_kernel::{CdfError, Result, ScanPredicate};

pub(crate) fn apply_residual_filters(
    batch: &RecordBatch,
    predicates: &[ScanPredicate],
) -> Result<RecordBatch> {
    if predicates.is_empty() || batch.num_rows() == 0 {
        return Ok(batch.clone());
    }

    let mut keep = vec![true; batch.num_rows()];
    for predicate in predicates {
        let parsed = ParsedPredicate::parse(&predicate.expression)?;
        for (row, keep_row) in keep.iter_mut().enumerate() {
            *keep_row &= evaluate_predicate(batch, &parsed, row)?;
        }
    }

    let mask = BooleanArray::from(keep);
    filter_record_batch(batch, &mask).map_err(CdfError::from)
}

pub(crate) fn predicate_operator(expression: &str) -> Option<String> {
    ParsedPredicate::split(expression).map(|(_, operator, _)| operator.to_owned())
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ParsedPredicate {
    column: String,
    operator: ComparisonOperator,
    literal: Literal,
}

impl ParsedPredicate {
    fn parse(expression: &str) -> Result<Self> {
        let Some((column, operator, literal)) = Self::split(expression) else {
            return Err(CdfError::contract(format!(
                "unsupported predicate expression {expression:?}; MVP predicates use '<column> <op> <literal>'"
            )));
        };
        Ok(Self {
            column: column.to_owned(),
            operator: ComparisonOperator::parse(operator)?,
            literal: Literal::parse(literal),
        })
    }

    fn split(expression: &str) -> Option<(&str, &str, &str)> {
        for operator in [">=", "<=", "!=", "=", ">", "<"] {
            if let Some(index) = expression.find(operator) {
                let (column, rest) = expression.split_at(index);
                let literal = &rest[operator.len()..];
                let column = column.trim();
                let literal = literal.trim();
                if !column.is_empty() && !literal.is_empty() {
                    return Some((column, operator, literal));
                }
            }
        }
        None
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ComparisonOperator {
    Eq,
    NotEq,
    Gt,
    GtEq,
    Lt,
    LtEq,
}

impl ComparisonOperator {
    fn parse(operator: &str) -> Result<Self> {
        match operator {
            "=" => Ok(Self::Eq),
            "!=" => Ok(Self::NotEq),
            ">" => Ok(Self::Gt),
            ">=" => Ok(Self::GtEq),
            "<" => Ok(Self::Lt),
            "<=" => Ok(Self::LtEq),
            other => Err(CdfError::contract(format!(
                "unsupported predicate operator {other:?}"
            ))),
        }
    }

    fn compare_ord<T: Ord>(&self, left: T, right: T) -> bool {
        match self {
            Self::Eq => left == right,
            Self::NotEq => left != right,
            Self::Gt => left > right,
            Self::GtEq => left >= right,
            Self::Lt => left < right,
            Self::LtEq => left <= right,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Literal {
    Bool(bool),
    I64(i64),
    U64(u64),
    String(String),
}

impl Literal {
    fn parse(raw: &str) -> Self {
        let trimmed = raw.trim();
        if let Some(unquoted) = trimmed
            .strip_prefix('\'')
            .and_then(|value| value.strip_suffix('\''))
        {
            return Self::String(unquoted.to_owned());
        }
        if let Some(unquoted) = trimmed
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
        {
            return Self::String(unquoted.to_owned());
        }
        if trimmed.eq_ignore_ascii_case("true") {
            return Self::Bool(true);
        }
        if trimmed.eq_ignore_ascii_case("false") {
            return Self::Bool(false);
        }
        if let Ok(value) = trimmed.parse::<i64>() {
            return Self::I64(value);
        }
        if let Ok(value) = trimmed.parse::<u64>() {
            return Self::U64(value);
        }
        Self::String(trimmed.to_owned())
    }
}

fn evaluate_predicate(
    batch: &RecordBatch,
    predicate: &ParsedPredicate,
    row: usize,
) -> Result<bool> {
    let index = batch.schema().index_of(&predicate.column).map_err(|_| {
        CdfError::data(format!(
            "predicate field {:?} is not present in resource batch",
            predicate.column
        ))
    })?;
    let array = batch.column(index);
    if array.is_null(row) {
        return Ok(false);
    }

    match array.data_type() {
        DataType::Int32 => compare_i64(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Arrow Int32 array downcast"),
            row,
            predicate,
        ),
        DataType::Int64 => compare_i64(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Arrow Int64 array downcast"),
            row,
            predicate,
        ),
        DataType::UInt32 => compare_u64(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .expect("Arrow UInt32 array downcast"),
            row,
            predicate,
        ),
        DataType::UInt64 => compare_u64(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("Arrow UInt64 array downcast"),
            row,
            predicate,
        ),
        DataType::Utf8 => compare_string(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Arrow Utf8 array downcast"),
            row,
            predicate,
        ),
        DataType::Boolean => compare_bool(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("Arrow Boolean array downcast"),
            row,
            predicate,
        ),
        other => Err(CdfError::contract(format!(
            "predicate field {:?} has unsupported MVP filter type {other}",
            predicate.column
        ))),
    }
}

trait IntValueArray {
    fn value_i64(&self, row: usize) -> i64;
}

impl IntValueArray for Int32Array {
    fn value_i64(&self, row: usize) -> i64 {
        i64::from(self.value(row))
    }
}

impl IntValueArray for Int64Array {
    fn value_i64(&self, row: usize) -> i64 {
        self.value(row)
    }
}

fn compare_i64<T>(array: &T, row: usize, predicate: &ParsedPredicate) -> Result<bool>
where
    T: IntValueArray,
{
    let Literal::I64(right) = predicate.literal else {
        return Err(CdfError::contract(format!(
            "predicate {:?} requires a signed integer literal",
            predicate.column
        )));
    };
    Ok(predicate.operator.compare_ord(array.value_i64(row), right))
}

trait UIntValueArray {
    fn value_u64(&self, row: usize) -> u64;
}

impl UIntValueArray for UInt32Array {
    fn value_u64(&self, row: usize) -> u64 {
        u64::from(self.value(row))
    }
}

impl UIntValueArray for UInt64Array {
    fn value_u64(&self, row: usize) -> u64 {
        self.value(row)
    }
}

fn compare_u64<T>(array: &T, row: usize, predicate: &ParsedPredicate) -> Result<bool>
where
    T: UIntValueArray,
{
    let right = match predicate.literal {
        Literal::U64(value) => value,
        Literal::I64(value) if value >= 0 => value as u64,
        _ => {
            return Err(CdfError::contract(format!(
                "predicate {:?} requires an unsigned integer literal",
                predicate.column
            )));
        }
    };
    Ok(predicate.operator.compare_ord(array.value_u64(row), right))
}

fn compare_string(array: &StringArray, row: usize, predicate: &ParsedPredicate) -> Result<bool> {
    let Literal::String(ref right) = predicate.literal else {
        return Err(CdfError::contract(format!(
            "predicate {:?} requires a string literal",
            predicate.column
        )));
    };
    Ok(predicate
        .operator
        .compare_ord(array.value(row), right.as_str()))
}

fn compare_bool(array: &BooleanArray, row: usize, predicate: &ParsedPredicate) -> Result<bool> {
    let Literal::Bool(right) = predicate.literal else {
        return Err(CdfError::contract(format!(
            "predicate {:?} requires a boolean literal",
            predicate.column
        )));
    };
    match predicate.operator {
        ComparisonOperator::Eq | ComparisonOperator::NotEq => {
            Ok(predicate.operator.compare_ord(array.value(row), right))
        }
        _ => Err(CdfError::contract(format!(
            "predicate {:?} uses an unsupported boolean operator",
            predicate.column
        ))),
    }
}
