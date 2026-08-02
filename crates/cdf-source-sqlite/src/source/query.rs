use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow_schema::{DataType, Field, SchemaRef};
use cdf_kernel::{
    CdfError, CompiledScanIntent, Expression, ExpressionLiteral, PartitionPlan, PushdownFidelity,
    ResourceDescriptor, Result, SortDirection, SourcePosition,
};
use rusqlite::types::Value;

use crate::{catalog::SQLITE_STRICT_METADATA_KEY, identifier::SqliteIdentifier};

use super::{
    schema::{
        SqliteTemporalEncoding, field_by_name, field_by_source_or_output_name,
        source_column_identifier, temporal_encoding, validate_sqlite_table_resource_shape,
    },
    temporal::bind_cursor_value,
};

pub(super) const SQLITE_SOURCE_KIND: &str = "sqlite";
pub(super) const SQLITE_SQL_DIALECT: &str = "sqlite";

pub(super) fn scan_from_partition(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    table: &SqliteIdentifier,
    stable_key: Option<&SqliteIdentifier>,
    temporal_encodings: &BTreeMap<String, SqliteTemporalEncoding>,
    partition: &PartitionPlan,
) -> Result<SqliteTableScan> {
    if partition.partition_id.as_str() != "sqlite"
        || partition.metadata.get("kind").map(String::as_str) != Some(SQLITE_SOURCE_KIND)
        || partition.metadata.get("dialect").map(String::as_str) != Some(SQLITE_SQL_DIALECT)
    {
        return Err(CdfError::contract(
            "SQLite table source requires its canonical sqlite SQL partition",
        ));
    }
    if partition.metadata.get("resource_id").map(String::as_str)
        != Some(descriptor.resource_id.as_str())
        || partition.metadata.get("table").map(String::as_str) != Some(table.as_str())
        || partition.metadata.get("stable_key").map(String::as_str)
            != stable_key.map(SqliteIdentifier::as_str)
        || partition.scope != descriptor.state_scope
    {
        return Err(CdfError::contract(
            "SQLite source partition authority does not match the compiled resource",
        ));
    }
    validate_sqlite_table_resource_shape(
        descriptor,
        schema,
        table,
        stable_key,
        temporal_encodings,
    )?;
    let scan =
        SqliteTableScan::from_intent(descriptor, schema, stable_key, &partition.scan_intent)?;
    if let Some(cursor) = &descriptor.cursor
        && !scan.projection.iter().any(|field| field == &cursor.field)
    {
        return Err(CdfError::contract(format!(
            "SQLite cursor field `{}` must be projected so emitted rows carry cursor position",
            cursor.field
        )));
    }
    Ok(scan)
}

#[derive(Clone, Debug)]
pub(super) struct SqliteTableScan {
    pub(super) projection: Vec<String>,
    pub(super) predicates: Vec<SqliteStoredPredicate>,
    pub(super) order_by: Vec<SqliteStoredOrder>,
}

impl SqliteTableScan {
    pub(super) fn from_intent(
        descriptor: &ResourceDescriptor,
        schema: &SchemaRef,
        stable_key: Option<&SqliteIdentifier>,
        intent: &CompiledScanIntent,
    ) -> Result<Self> {
        intent.validate()?;
        let projection = intent.projection.clone().unwrap_or_else(|| {
            schema
                .fields()
                .iter()
                .map(|field| field.name().to_owned())
                .collect()
        });
        validate_projection(descriptor, schema, stable_key, &projection)?;
        let predicates = intent
            .predicates
            .iter()
            .map(|pushed| {
                let parsed =
                    parse_supported_predicate(schema, &pushed.predicate.canonical_expression)
                        .ok_or_else(|| {
                            CdfError::contract("compiled SQLite predicate is not executable")
                        })?;
                if parsed.fidelity != pushed.fidelity {
                    return Err(CdfError::contract(
                        "compiled SQLite predicate fidelity changed",
                    ));
                }
                Ok(parsed)
            })
            .collect::<Result<Vec<_>>>()?;
        validate_requested_order(descriptor, stable_key, &intent.order_by)?;
        let order_by = if let Some(cursor) = &descriptor.cursor {
            vec![
                SqliteStoredOrder {
                    field: cursor.field.clone(),
                    direction: SortDirection::Asc,
                },
                SqliteStoredOrder {
                    field: stable_key
                        .expect("cursor shape validated")
                        .as_str()
                        .to_owned(),
                    direction: SortDirection::Asc,
                },
            ]
        } else {
            intent
                .order_by
                .iter()
                .map(|order| SqliteStoredOrder {
                    field: order.field.clone(),
                    direction: order.direction.clone(),
                })
                .collect()
        };
        Ok(Self {
            projection,
            predicates,
            order_by,
        })
    }
}

#[derive(Clone, Debug)]
pub(super) struct SqliteStoredOrder {
    pub(super) field: String,
    direction: SortDirection,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PredicateOperator {
    Eq,
    Gt,
    Gte,
    Lt,
    Lte,
}
impl PredicateOperator {
    fn sql(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::Gt => ">",
            Self::Gte => ">=",
            Self::Lt => "<",
            Self::Lte => "<=",
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct SqliteStoredPredicate {
    field: String,
    operator: PredicateOperator,
    value: Value,
    pub(super) fidelity: PushdownFidelity,
}

pub(super) struct SqliteQuery {
    pub(super) sql: String,
    pub(super) params: Vec<Value>,
}

pub(super) fn build_query(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    table: &SqliteIdentifier,
    stable_key: Option<&SqliteIdentifier>,
    temporal_encodings: &BTreeMap<String, SqliteTemporalEncoding>,
    partition: &PartitionPlan,
    scan: &SqliteTableScan,
) -> Result<SqliteQuery> {
    let projection = projected_fields(schema, &scan.projection)?
        .iter()
        .map(|field| {
            Ok(format!(
                "{} AS {}",
                source_column_identifier(field)?.quoted(),
                SqliteIdentifier::new(field.name())?.quoted()
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut sql = format!("SELECT {} FROM {}", projection.join(", "), table.quoted());
    let mut clauses = Vec::new();
    let mut params = Vec::new();
    if let Some(position) = &partition.start_position {
        let cursor = descriptor.cursor.as_ref().ok_or_else(|| {
            CdfError::contract("SQLite snapshot resource cannot resume from a cursor position")
        })?;
        let SourcePosition::Cursor(position) = position else {
            return Err(CdfError::contract(
                "SQLite source start position must be a cursor",
            ));
        };
        if position.field != cursor.field {
            return Err(CdfError::contract(
                "SQLite source start cursor field changed",
            ));
        }
        let field = field_by_name(schema, &cursor.field).expect("cursor validated");
        params.push(bind_cursor_value(
            field,
            &position.value,
            temporal_encoding(field, temporal_encodings),
        )?);
        clauses.push(format!(
            "{} > ?{}",
            source_column_identifier(field)?.quoted(),
            params.len()
        ));
    }
    for predicate in &scan.predicates {
        if predicate.fidelity != PushdownFidelity::Exact {
            continue;
        }
        let field = field_by_name(schema, &predicate.field)
            .ok_or_else(|| CdfError::contract("SQLite predicate field disappeared"))?;
        params.push(predicate.value.clone());
        clauses.push(format!(
            "{} {} ?{}",
            source_column_identifier(field)?.quoted(),
            predicate.operator.sql(),
            params.len()
        ));
    }
    if !clauses.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&clauses.join(" AND "));
    }
    let order_by = if descriptor.cursor.is_some() {
        let _ = stable_key.expect("cursor validated");
        &scan.order_by
    } else {
        &scan.order_by
    };
    if !order_by.is_empty() {
        let ordering = order_by
            .iter()
            .map(|order| {
                let field =
                    field_by_source_or_output_name(schema, &order.field).ok_or_else(|| {
                        CdfError::contract(format!(
                            "SQLite order field `{}` is not in the schema",
                            order.field
                        ))
                    })?;
                Ok(format!(
                    "{} {}",
                    source_column_identifier(field)?.quoted(),
                    match order.direction {
                        SortDirection::Asc => "ASC",
                        SortDirection::Desc => "DESC",
                    }
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        sql.push_str(" ORDER BY ");
        sql.push_str(&ordering.join(", "));
    }
    Ok(SqliteQuery { sql, params })
}

pub(super) fn parse_supported_predicate(
    schema: &SchemaRef,
    expression: &Expression,
) -> Option<SqliteStoredPredicate> {
    let (field_name, operator, literal) = expression.comparison()?;
    let operator = match operator {
        "eq" => PredicateOperator::Eq,
        "gt" => PredicateOperator::Gt,
        "gte" => PredicateOperator::Gte,
        "lt" => PredicateOperator::Lt,
        "lte" => PredicateOperator::Lte,
        _ => return None,
    };
    let field = field_by_name(schema, field_name)?;
    source_column_identifier(field).ok()?;
    let value = match (field.data_type(), literal) {
        (DataType::Boolean, ExpressionLiteral::Boolean(value))
            if operator == PredicateOperator::Eq =>
        {
            Value::Integer(i64::from(*value))
        }
        (DataType::Int64, ExpressionLiteral::Signed(value)) => Value::Integer(*value),
        (DataType::UInt64, ExpressionLiteral::Unsigned(value)) => {
            Value::Integer(i64::try_from(*value).ok()?)
        }
        (DataType::UInt64, ExpressionLiteral::Signed(value)) if *value >= 0 => {
            Value::Integer(*value)
        }
        (DataType::Float64, ExpressionLiteral::Float64Bits(bits)) => {
            let value = f64::from_bits(*bits);
            if !value.is_finite() {
                return None;
            }
            Value::Real(value)
        }
        (DataType::Utf8, ExpressionLiteral::String(value)) => Value::Text(value.clone()),
        _ => return None,
    };
    let strict = field
        .metadata()
        .get(SQLITE_STRICT_METADATA_KEY)
        .is_some_and(|value| value == "true");
    let fidelity = if strict
        && matches!(
            field.data_type(),
            DataType::Boolean | DataType::Int64 | DataType::UInt64 | DataType::Float64
        ) {
        PushdownFidelity::Exact
    } else {
        PushdownFidelity::Inexact
    };
    Some(SqliteStoredPredicate {
        field: field_name.to_owned(),
        operator,
        value,
        fidelity,
    })
}

pub(super) fn validate_requested_order(
    descriptor: &ResourceDescriptor,
    stable_key: Option<&SqliteIdentifier>,
    order_by: &[cdf_kernel::OrderBy],
) -> Result<()> {
    let Some(cursor) = &descriptor.cursor else {
        return Ok(());
    };
    if order_by.is_empty() {
        return Ok(());
    }
    let stable_key = stable_key.expect("cursor shape validated");
    if order_by.len() == 2
        && order_by[0].field == cursor.field
        && order_by[0].direction == SortDirection::Asc
        && order_by[1].field == stable_key.as_str()
        && order_by[1].direction == SortDirection::Asc
    {
        return Ok(());
    }
    Err(CdfError::contract(format!(
        "SQLite cursor scans require ascending `{}` followed by ascending `{stable_key}`",
        cursor.field
    )))
}

fn validate_projection(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    stable_key: Option<&SqliteIdentifier>,
    projection: &[String],
) -> Result<()> {
    if projection.is_empty() {
        return Err(CdfError::contract(
            "SQLite projection must include at least one field",
        ));
    }
    let mut names = BTreeSet::new();
    for name in projection {
        if !names.insert(name) || field_by_name(schema, name).is_none() {
            return Err(CdfError::contract(format!(
                "SQLite projection contains invalid or repeated field `{name}`"
            )));
        }
    }
    if let Some(cursor) = &descriptor.cursor {
        for required in [
            cursor.field.as_str(),
            stable_key.expect("cursor shape validated").as_str(),
        ] {
            let output_name = field_by_source_or_output_name(schema, required)
                .map(Field::name)
                .ok_or_else(|| {
                    CdfError::contract("SQLite cursor projection authority is invalid")
                })?;
            if !projection.iter().any(|name| name == output_name) {
                return Err(CdfError::contract(format!(
                    "SQLite cursor scan must project `{output_name}`"
                )));
            }
        }
    }
    Ok(())
}

pub(super) fn projected_fields(
    schema: &SchemaRef,
    projection: &[String],
) -> Result<Vec<Arc<Field>>> {
    projection
        .iter()
        .map(|name| {
            schema
                .field_with_name(name)
                .cloned()
                .map(Arc::new)
                .map_err(|_| {
                    CdfError::contract(format!(
                        "SQLite projection field `{name}` is not in the schema"
                    ))
                })
        })
        .collect()
}
