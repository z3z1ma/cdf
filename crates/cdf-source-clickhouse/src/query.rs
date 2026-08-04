use std::collections::BTreeSet;

use arrow_schema::{DataType, Field, SchemaRef, TimeUnit};
use cdf_kernel::{
    CdfError, CompiledScanIntent, CursorValue, DeclarativeExpression, DeclarativeExpressionLiteral,
    PartitionPlan, PushdownFidelity, ResourceDescriptor, Result, SortDirection, SourcePosition,
    physical_type, source_name,
};

use crate::{
    identifier::ClickHouseIdentifier,
    types::{
        CLICKHOUSE_MAXIMUM_VARIABLE_ROW_BYTES, ClickHouseCursorCast, cursor_cast,
        datetime_timezone, field_by_name, projection_has_variable_width, validate_clickhouse_type,
    },
};

pub(crate) const CLICKHOUSE_SOURCE_KIND: &str = "clickhouse";
pub(crate) const CLICKHOUSE_SQL_DIALECT: &str = "clickhouse";

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ClickHouseTableScan {
    pub(crate) projection: Vec<String>,
    predicates: Vec<StoredPredicate>,
    order_by: Vec<StoredOrder>,
    limit: Option<u64>,
}

impl ClickHouseTableScan {
    fn from_intent(
        descriptor: &ResourceDescriptor,
        schema: &SchemaRef,
        stable_key: Option<&ClickHouseIdentifier>,
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
        validate_projection(schema, &projection)?;
        if let Some(cursor) = &descriptor.cursor
            && !projection.contains(&cursor.field)
        {
            return Err(CdfError::contract(format!(
                "ClickHouse cursor field `{}` must be projected so emitted rows carry cursor position",
                cursor.field
            )));
        }
        let predicates = intent
            .predicates
            .iter()
            .map(|pushed| {
                if pushed.fidelity != PushdownFidelity::Exact {
                    return Err(CdfError::contract(
                        "compiled ClickHouse predicates must retain exact fidelity",
                    ));
                }
                parse_supported_predicate(schema, &pushed.predicate.canonical_expression)
                    .ok_or_else(|| {
                        CdfError::contract(
                            "compiled ClickHouse predicate is not type-safe and executable",
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let order_by = canonical_order(descriptor, schema, stable_key, &intent.order_by)?;
        if descriptor.cursor.is_some() && intent.limit.is_some() {
            return Err(CdfError::contract(
                "ClickHouse cursor partitions must retain limits for generic engine evaluation; SQL LIMIT cannot safely cross a cursor frontier",
            ));
        }
        Ok(Self {
            projection,
            predicates,
            order_by,
            limit: intent.limit,
        })
    }
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

#[derive(Clone, Debug, PartialEq)]
struct StoredPredicate {
    field: String,
    operator: PredicateOperator,
    value: QueryParameter,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct StoredOrder {
    field: String,
    direction: SortDirection,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum QueryParameter {
    Boolean(bool),
    Signed(i64),
    Unsigned(u64),
    Float(f64),
    String(String),
}

pub(crate) struct ClickHouseQuery {
    pub(crate) sql: String,
    pub(crate) parameters: Vec<QueryParameter>,
}

pub(crate) fn scan_from_partition(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    table: &ClickHouseIdentifier,
    stable_key: Option<&ClickHouseIdentifier>,
    partition: &PartitionPlan,
) -> Result<ClickHouseTableScan> {
    if partition.partition_id.as_str() != CLICKHOUSE_SOURCE_KIND
        || partition.metadata.get("kind").map(String::as_str) != Some(CLICKHOUSE_SOURCE_KIND)
        || partition.metadata.get("dialect").map(String::as_str) != Some(CLICKHOUSE_SQL_DIALECT)
    {
        return Err(CdfError::contract(
            "ClickHouse table source requires its canonical ClickHouse SQL partition",
        ));
    }
    if partition.metadata.get("resource_id").map(String::as_str)
        != Some(descriptor.resource_id.as_str())
        || partition.metadata.get("table").map(String::as_str) != Some(table.as_str())
        || partition.metadata.get("stable_key").map(String::as_str)
            != stable_key.map(ClickHouseIdentifier::as_str)
        || partition.scope != descriptor.state_scope
    {
        return Err(CdfError::contract(
            "ClickHouse partition authority does not match its compiled resource",
        ));
    }
    if partition.start_position.is_some() && descriptor.cursor.is_none() {
        return Err(CdfError::contract(
            "ClickHouse snapshot resource cannot resume from a cursor position",
        ));
    }
    ClickHouseTableScan::from_intent(descriptor, schema, stable_key, &partition.scan_intent)
}

pub(crate) fn build_query(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    database: &ClickHouseIdentifier,
    table: &ClickHouseIdentifier,
    partition: &PartitionPlan,
    scan: &ClickHouseTableScan,
) -> Result<ClickHouseQuery> {
    let projection = scan
        .projection
        .iter()
        .map(|name| {
            let field = field_by_name(schema, name).ok_or_else(|| {
                CdfError::contract(format!(
                    "ClickHouse projection field `{name}` is absent from the pinned schema"
                ))
            })?;
            let source = source_identifier(field)?;
            Ok(format!(
                "{} AS {}",
                field_source_expression(field, &source)?,
                ClickHouseIdentifier::new(field.name().to_owned())?.quoted()
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut sql = format!(
        "SELECT {} FROM {}.{}",
        projection.join(", "),
        database.quoted(),
        table.quoted()
    );
    let mut clauses = Vec::new();
    let mut parameters = Vec::new();
    if projection_has_variable_width(schema, &scan.projection) {
        let row = scan
            .projection
            .iter()
            .map(|name| {
                let field = field_by_name(schema, name).ok_or_else(|| {
                    CdfError::contract("ClickHouse row-bound projection field disappeared")
                })?;
                let source = source_identifier(field)?;
                field_source_expression(field, &source)
            })
            .collect::<Result<Vec<_>>>()?;
        clauses.push(format!(
            "throwIf(byteSize(tuple({})) > {CLICKHOUSE_MAXIMUM_VARIABLE_ROW_BYTES}, 'CDF ClickHouse row exceeds bounded decode envelope') = 0",
            row.join(", ")
        ));
    }
    for predicate in &scan.predicates {
        let field = field_by_name(schema, &predicate.field)
            .ok_or_else(|| CdfError::contract("compiled ClickHouse predicate field disappeared"))?;
        parameters.push(predicate.value.clone());
        let source = source_identifier(field)?;
        clauses.push(format!(
            "{} {} ?",
            field_source_expression(field, &source)?,
            predicate.operator.sql()
        ));
    }
    if let Some(start) = &partition.start_position {
        let cursor = descriptor.cursor.as_ref().ok_or_else(|| {
            CdfError::contract("ClickHouse snapshot resource cannot carry a start position")
        })?;
        let SourcePosition::Cursor(start) = start else {
            return Err(CdfError::contract(
                "ClickHouse start position must be a cursor",
            ));
        };
        if start.field != cursor.field {
            return Err(CdfError::contract(
                "ClickHouse start cursor field changed after compilation",
            ));
        }
        let field = field_by_name(schema, &cursor.field)
            .ok_or_else(|| CdfError::contract("ClickHouse cursor field disappeared"))?;
        let (parameter, expression) = cursor_parameter(field, &start.value)?;
        parameters.push(parameter);
        let source = source_identifier(field)?;
        clauses.push(format!(
            "{} > {expression}",
            field_source_expression(field, &source)?
        ));
    }
    if !clauses.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&clauses.join(" AND "));
    }
    if !scan.order_by.is_empty() {
        let ordering = scan
            .order_by
            .iter()
            .map(|order| {
                let field = field_by_name(schema, &order.field).or_else(|| {
                    schema
                        .fields()
                        .iter()
                        .map(AsRef::as_ref)
                        .find(|field| source_name(field) == Some(order.field.as_str()))
                })?;
                let source = source_identifier(field).ok()?;
                Some(format!(
                    "{} {}",
                    field_source_expression(field, &source).ok()?,
                    match order.direction {
                        SortDirection::Asc => "ASC",
                        SortDirection::Desc => "DESC",
                    }
                ))
            })
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| CdfError::contract("ClickHouse order field disappeared"))?;
        sql.push_str(" ORDER BY ");
        sql.push_str(&ordering.join(", "));
    }
    if let Some(limit) = scan.limit {
        parameters.push(QueryParameter::Unsigned(limit));
        sql.push_str(" LIMIT ?");
    }
    Ok(ClickHouseQuery { sql, parameters })
}

pub(crate) fn predicate_fidelity(
    schema: &SchemaRef,
    expression: &DeclarativeExpression,
) -> PushdownFidelity {
    parse_supported_predicate(schema, expression)
        .map_or(PushdownFidelity::Unsupported, |_| PushdownFidelity::Exact)
}

fn parse_supported_predicate(
    schema: &SchemaRef,
    expression: &DeclarativeExpression,
) -> Option<StoredPredicate> {
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
    source_identifier(field).ok()?;
    let value = match (field.data_type(), literal) {
        (DataType::Boolean, DeclarativeExpressionLiteral::Boolean(value))
            if operator == PredicateOperator::Eq =>
        {
            QueryParameter::Boolean(*value)
        }
        (
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64,
            DeclarativeExpressionLiteral::Signed(value),
        ) => QueryParameter::Signed(*value),
        (
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64,
            DeclarativeExpressionLiteral::Unsigned(value),
        ) => QueryParameter::Unsigned(*value),
        (
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64,
            DeclarativeExpressionLiteral::Signed(value),
        ) if *value >= 0 => QueryParameter::Unsigned((*value).try_into().ok()?),
        (
            DataType::Float32 | DataType::Float64,
            DeclarativeExpressionLiteral::Float64Bits(bits),
        ) => {
            let value = f64::from_bits(*bits);
            if !value.is_finite() {
                return None;
            }
            QueryParameter::Float(value)
        }
        (DataType::Utf8 | DataType::LargeUtf8, DeclarativeExpressionLiteral::String(value)) => {
            QueryParameter::String(value.clone())
        }
        _ => return None,
    };
    Some(StoredPredicate {
        field: field_name.to_owned(),
        operator,
        value,
    })
}

fn canonical_order(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    stable_key: Option<&ClickHouseIdentifier>,
    requested: &[cdf_kernel::OrderBy],
) -> Result<Vec<StoredOrder>> {
    if let Some(cursor) = &descriptor.cursor {
        let stable_key = stable_key.ok_or_else(|| {
            CdfError::internal("validated ClickHouse cursor resource lost its stable-key authority")
        })?;
        let canonical = vec![
            StoredOrder {
                field: cursor.field.clone(),
                direction: SortDirection::Asc,
            },
            StoredOrder {
                field: stable_key.as_str().to_owned(),
                direction: SortDirection::Asc,
            },
        ];
        if !requested.is_empty()
            && (requested.len() != canonical.len()
                || requested.iter().zip(&canonical).any(|(left, right)| {
                    left.field != right.field || left.direction != right.direction
                }))
        {
            return Err(CdfError::contract(
                "ClickHouse cursor scans require exact cursor ASC, stable_key ASC ordering",
            ));
        }
        return Ok(canonical);
    }
    requested
        .iter()
        .map(|order| {
            let field = field_by_name(schema, &order.field).ok_or_else(|| {
                CdfError::contract(format!(
                    "ClickHouse order field `{}` is absent from the pinned schema",
                    order.field
                ))
            })?;
            source_identifier(field)?;
            Ok(StoredOrder {
                field: order.field.clone(),
                direction: order.direction.clone(),
            })
        })
        .collect()
}

fn validate_projection(schema: &SchemaRef, projection: &[String]) -> Result<()> {
    if projection.is_empty() {
        return Err(CdfError::contract(
            "ClickHouse projection must contain at least one field",
        ));
    }
    let mut unique = BTreeSet::new();
    for name in projection {
        if !unique.insert(name) {
            return Err(CdfError::contract(format!(
                "ClickHouse projection repeats field `{name}`"
            )));
        }
        let field = field_by_name(schema, name).ok_or_else(|| {
            CdfError::contract(format!(
                "ClickHouse projection field `{name}` is absent from the pinned schema"
            ))
        })?;
        source_identifier(field)?;
    }
    Ok(())
}

fn source_identifier(field: &Field) -> Result<ClickHouseIdentifier> {
    ClickHouseIdentifier::new(
        source_name(field)
            .unwrap_or_else(|| field.name())
            .to_owned(),
    )
}

pub(crate) fn source_expression(
    identifier: &ClickHouseIdentifier,
    source_physical_type: Option<&str>,
) -> Result<String> {
    if let Some(source_physical_type) = source_physical_type {
        validate_clickhouse_type(identifier.as_str(), source_physical_type)?;
    }
    match source_physical_type {
        Some("UUID") => Ok(format!("toString({})", identifier.quoted())),
        // ClickHouse 25.8 exposes its narrow Date and DateTime storage as UInt16/UInt32 in
        // ArrowStream. Promote only those two source types to the corresponding Arrow logical
        // families; Date32 and DateTime64 already carry truthful Arrow logical types.
        Some("Date") => Ok(format!("toDate32({})", identifier.quoted())),
        Some(value) if datetime_timezone(value) == Some(None) => {
            Ok(format!("toDateTime64({}, 0)", identifier.quoted()))
        }
        Some(value) if datetime_timezone(value).is_some() => {
            let timezone = datetime_timezone(value)
                .flatten()
                .ok_or_else(|| CdfError::internal("validated ClickHouse timezone disappeared"))?;
            Ok(format!(
                "toDateTime64({}, 0, '{timezone}')",
                identifier.quoted()
            ))
        }
        _ => Ok(identifier.quoted()),
    }
}

pub(crate) fn source_expression_with_cursor_cast(
    identifier: &ClickHouseIdentifier,
    source_physical_type: Option<&str>,
    cast: Option<ClickHouseCursorCast>,
) -> Result<String> {
    if let Some(source_physical_type) = source_physical_type {
        validate_clickhouse_type(identifier.as_str(), source_physical_type)?;
    }
    match cast {
        Some(ClickHouseCursorCast::Signed64) => Ok(format!("toInt64({})", identifier.quoted())),
        Some(ClickHouseCursorCast::Unsigned64) => Ok(format!("toUInt64({})", identifier.quoted())),
        None => source_expression(identifier, source_physical_type),
    }
}

fn field_source_expression(field: &Field, identifier: &ClickHouseIdentifier) -> Result<String> {
    source_expression_with_cursor_cast(identifier, physical_type(field), cursor_cast(field)?)
}

fn cursor_parameter(field: &Field, value: &CursorValue) -> Result<(QueryParameter, &'static str)> {
    let mismatch = || {
        CdfError::contract(format!(
            "ClickHouse cursor value does not match field `{}` type {:?}",
            field.name(),
            field.data_type()
        ))
    };
    match (field.data_type(), value) {
        (DataType::Int64, CursorValue::I64(value)) => Ok((QueryParameter::Signed(*value), "?")),
        (DataType::UInt64, CursorValue::U64(value)) => Ok((QueryParameter::Unsigned(*value), "?")),
        (DataType::Date32, CursorValue::I64(days)) => Ok((
            QueryParameter::Signed(*days),
            "addDays(toDate32('1970-01-01'), ?)",
        )),
        (DataType::Date64, CursorValue::TimestampMicros { micros, .. }) => Ok((
            QueryParameter::Signed(*micros),
            "fromUnixTimestamp64Micro(?)",
        )),
        (
            DataType::Timestamp(
                TimeUnit::Second
                | TimeUnit::Millisecond
                | TimeUnit::Microsecond
                | TimeUnit::Nanosecond,
                _,
            ),
            CursorValue::TimestampMicros { micros, .. },
        ) => Ok((
            QueryParameter::Signed(*micros),
            "fromUnixTimestamp64Micro(?)",
        )),
        _ => Err(mismatch()),
    }
}
