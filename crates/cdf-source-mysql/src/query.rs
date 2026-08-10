use std::collections::BTreeMap;

use arrow_schema::{DataType, Field, SchemaRef};
use cdf_kernel::{
    CdfError, CompiledScanIntent, CursorValue, DeclarativeExpression, DeclarativeExpressionLiteral,
    DeliveryGuarantee, PartitionAuthority, PartitionId, PartitionPlan, PlanId, PushdownFidelity,
    PushedPredicate, ResourceDescriptor, Result, ScanPlan, ScanPredicate, ScanRequest,
    SortDirection, SourcePosition, source_name,
};
use mysql_async::{Params, Value};

use crate::{
    identifier::MySqlIdentifier,
    native::MySqlSourceInput,
    schema::{generation_from_schema, generation_position},
};

const MYSQL_PARTITION_KIND: &str = "mysql";

#[derive(Clone, Debug)]
pub(crate) struct MySqlQuery {
    pub(crate) sql: String,
    pub(crate) params: Params,
    pub(crate) projection: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Operator {
    Eq,
    Gt,
    Gte,
    Lt,
    Lte,
}

impl Operator {
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
struct StoredPredicate {
    field: String,
    operator: Operator,
    value: Value,
}

pub(crate) fn negotiate_scan(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    input: &MySqlSourceInput,
    request: &ScanRequest,
) -> Result<ScanPlan> {
    validate_request(descriptor, schema, input, request)?;
    let (pushed, unsupported) = classify_predicates(schema, &request.filters);
    Ok(ScanPlan::from_partition_authority(
        PlanId::new(format!("mysql-scan-{}", descriptor.resource_id))?,
        request.clone(),
        PartitionAuthority::Inline(vec![plan_partition(descriptor, schema, input, request)?]),
        pushed,
        unsupported,
        None,
        None,
        delivery_guarantee(descriptor),
    ))
}

pub(crate) fn plan_partition(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    input: &MySqlSourceInput,
    request: &ScanRequest,
) -> Result<PartitionPlan> {
    validate_request(descriptor, schema, input, request)?;
    let (predicates, _) = classify_predicates(schema, &request.filters);
    let scan_intent = CompiledScanIntent {
        version: cdf_kernel::COMPILED_SCAN_INTENT_VERSION,
        projection: request.projection.clone(),
        predicates,
        limit: request.limit,
        order_by: request.order_by.clone(),
    };
    scan_intent.validate()?;
    let mut metadata = BTreeMap::from([
        ("kind".to_owned(), MYSQL_PARTITION_KIND.to_owned()),
        ("resource_id".to_owned(), descriptor.resource_id.to_string()),
        ("input".to_owned(), input.location_summary()),
        (
            "schema_generation".to_owned(),
            generation_from_schema(schema)?.to_owned(),
        ),
    ]);
    if let Some(cursor) = &descriptor.cursor {
        metadata.insert("cursor_field".to_owned(), cursor.field.clone());
    }
    Ok(PartitionPlan {
        partition_id: PartitionId::new(MYSQL_PARTITION_KIND)?,
        scope: descriptor.state_scope.clone(),
        planned_position: Some(generation_position(
            descriptor,
            &input.location_summary(),
            schema,
        )?),
        start_position: None,
        scan_intent,
        retry_safety: cdf_kernel::PartitionRetrySafety::Forbidden,
        metadata,
    })
}

fn validate_request(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    input: &MySqlSourceInput,
    request: &ScanRequest,
) -> Result<()> {
    if request.resource_id != descriptor.resource_id {
        return Err(CdfError::contract(format!(
            "scan resource `{}` does not match MySQL resource `{}`",
            request.resource_id, descriptor.resource_id
        )));
    }
    validate_resource_shape(descriptor, schema, input)
}

pub(crate) fn scan_query(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    input: &MySqlSourceInput,
    partition: &PartitionPlan,
) -> Result<MySqlQuery> {
    if partition.partition_id.as_str() != MYSQL_PARTITION_KIND
        || partition.metadata.get("kind").map(String::as_str) != Some(MYSQL_PARTITION_KIND)
        || partition.metadata.get("resource_id").map(String::as_str)
            != Some(descriptor.resource_id.as_str())
        || partition.metadata.get("input").map(String::as_str)
            != Some(input.location_summary().as_str())
        || partition
            .metadata
            .get("schema_generation")
            .map(String::as_str)
            != Some(generation_from_schema(schema)?)
        || partition.scope != descriptor.state_scope
        || partition.planned_position.as_ref()
            != Some(&generation_position(
                descriptor,
                &input.location_summary(),
                schema,
            )?)
    {
        return Err(CdfError::contract(
            "MySQL partition authority differs from its compiled resource",
        ));
    }
    if let Some(position) = &partition.start_position {
        position.validate()?;
    }
    match (&partition.start_position, &descriptor.cursor) {
        (None, _) => {}
        (Some(SourcePosition::Cursor(position)), Some(cursor))
            if position.field == cursor.field => {}
        (Some(_), Some(cursor)) => {
            return Err(CdfError::contract(format!(
                "MySQL start position must be a cursor for field `{}`",
                cursor.field
            )));
        }
        (Some(_), None) => {
            return Err(CdfError::contract(
                "MySQL bounded resource cannot carry a start position",
            ));
        }
    }
    partition.scan_intent.validate()?;
    let projection = partition.scan_intent.projection.clone().unwrap_or_else(|| {
        schema
            .fields()
            .iter()
            .map(|field| field.name().to_owned())
            .collect()
    });
    validate_projection(schema, &projection)?;
    if let Some(cursor) = &descriptor.cursor
        && !projection.iter().any(|name| name == &cursor.field)
    {
        return Err(CdfError::contract(format!(
            "MySQL cursor field `{}` must be projected",
            cursor.field
        )));
    }

    let select = projection
        .iter()
        .map(|name| {
            let field = field_by_name(schema, name).ok_or_else(|| {
                CdfError::contract(format!("MySQL projection field `{name}` disappeared"))
            })?;
            Ok(format!(
                "{} AS {}",
                source_identifier(field)?.quoted(),
                MySqlIdentifier::user(field.name())?.quoted()
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let hint = partition
        .metadata
        .get("max_execution_time_ms")
        .map(|value| format!(" /*+ MAX_EXECUTION_TIME({value}) */"))
        .unwrap_or_default();
    let mut sql = format!(
        "SELECT{hint} {} FROM {}",
        select.join(", "),
        input.relation_sql()
    );
    let mut params = Vec::new();
    let mut where_parts = Vec::new();
    if let Some(position) = &partition.start_position {
        let SourcePosition::Cursor(position) = position else {
            unreachable!("validated cursor position")
        };
        let field = field_by_name(schema, &position.field).ok_or_else(|| {
            CdfError::contract(format!(
                "MySQL cursor field `{}` disappeared",
                position.field
            ))
        })?;
        let value = cursor_parameter(field, &position.value)?;
        where_parts.push(format!("{} > ?", source_identifier(field)?.quoted()));
        params.push(value);
    }
    for predicate in &partition.scan_intent.predicates {
        let stored = parse_predicate(schema, &predicate.predicate.canonical_expression)
            .ok_or_else(|| CdfError::contract("compiled MySQL predicate is not exact"))?;
        let field = field_by_name(schema, &stored.field)
            .ok_or_else(|| CdfError::contract("MySQL predicate field disappeared"))?;
        where_parts.push(format!(
            "{} {} ?",
            source_identifier(field)?.quoted(),
            stored.operator.sql()
        ));
        params.push(stored.value);
    }
    if !where_parts.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_parts.join(" AND "));
    }
    if !partition.scan_intent.order_by.is_empty() {
        let ordering = partition
            .scan_intent
            .order_by
            .iter()
            .map(|order| {
                let field = field_by_name(schema, &order.field).ok_or_else(|| {
                    CdfError::contract(format!("MySQL order field `{}` disappeared", order.field))
                })?;
                if !orderable(field) {
                    return Err(CdfError::contract(format!(
                        "MySQL field `{}` cannot preserve logical ordering for its physical domain",
                        field.name()
                    )));
                }
                let direction = match order.direction {
                    SortDirection::Asc => "ASC",
                    SortDirection::Desc => "DESC",
                };
                Ok(format!(
                    "{} {direction}",
                    source_identifier(field)?.quoted()
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        sql.push_str(" ORDER BY ");
        sql.push_str(&ordering.join(", "));
    }
    if let Some(limit) = partition.scan_intent.limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }
    Ok(MySqlQuery {
        sql,
        params: Params::Positional(params),
        projection,
    })
}

pub(crate) fn classify_predicates(
    schema: &SchemaRef,
    predicates: &[ScanPredicate],
) -> (Vec<PushedPredicate>, Vec<ScanPredicate>) {
    let mut pushed = Vec::new();
    let mut unsupported = Vec::new();
    for predicate in predicates {
        if parse_predicate(schema, &predicate.canonical_expression).is_some() {
            pushed.push(PushedPredicate {
                predicate: predicate.clone(),
                fidelity: PushdownFidelity::Exact,
            });
        } else {
            unsupported.push(predicate.clone());
        }
    }
    (pushed, unsupported)
}

fn parse_predicate(
    schema: &SchemaRef,
    expression: &DeclarativeExpression,
) -> Option<StoredPredicate> {
    let (field_name, operator, literal) = expression.comparison()?;
    let operator = match operator {
        "eq" => Operator::Eq,
        "gt" => Operator::Gt,
        "gte" => Operator::Gte,
        "lt" => Operator::Lt,
        "lte" => Operator::Lte,
        _ => return None,
    };
    let field = field_by_name(schema, field_name)?;
    let value = literal_parameter(field, literal, operator)?;
    Some(StoredPredicate {
        field: field_name.to_owned(),
        operator,
        value,
    })
}

fn literal_parameter(
    field: &Field,
    literal: &DeclarativeExpressionLiteral,
    operator: Operator,
) -> Option<Value> {
    match (field.data_type(), literal) {
        (DataType::Int64, DeclarativeExpressionLiteral::Signed(value)) => Some(Value::Int(*value)),
        (DataType::UInt64, DeclarativeExpressionLiteral::Unsigned(value)) => {
            Some(Value::UInt(*value))
        }
        (DataType::UInt64, DeclarativeExpressionLiteral::Signed(value)) if *value >= 0 => {
            Some(Value::UInt(*value as u64))
        }
        (DataType::Float32, DeclarativeExpressionLiteral::Float64Bits(bits)) => {
            let value = f64::from_bits(*bits);
            value.is_finite().then_some(Value::Float(value as f32))
        }
        (DataType::Float64, DeclarativeExpressionLiteral::Float64Bits(bits)) => {
            let value = f64::from_bits(*bits);
            value.is_finite().then_some(Value::Double(value))
        }
        (DataType::Utf8, DeclarativeExpressionLiteral::String(value)) if text_domain(field) => {
            Some(Value::Bytes(value.as_bytes().to_vec()))
        }
        (DataType::Binary, DeclarativeExpressionLiteral::String(value))
            if operator == Operator::Eq =>
        {
            Some(Value::Bytes(value.as_bytes().to_vec()))
        }
        _ => None,
    }
}

fn cursor_parameter(field: &Field, value: &CursorValue) -> Result<Value> {
    let value = match (field.data_type(), value) {
        (DataType::Int64, CursorValue::I64(value)) => Value::Int(*value),
        (DataType::UInt64, CursorValue::U64(value)) => Value::UInt(*value),
        (DataType::Float64, CursorValue::DecimalString(value)) => {
            let value = value
                .parse::<f64>()
                .map_err(|_| CdfError::contract("MySQL floating cursor is not a finite number"))?;
            if !value.is_finite() {
                return Err(CdfError::contract(
                    "MySQL floating cursor is not a finite number",
                ));
            }
            Value::Double(value)
        }
        (DataType::Utf8, CursorValue::String(value)) if text_domain(field) => {
            Value::Bytes(value.as_bytes().to_vec())
        }
        _ => {
            return Err(CdfError::contract(format!(
                "MySQL cursor checkpoint does not match field `{}` type {:?}",
                field.name(),
                field.data_type()
            )));
        }
    };
    Ok(value)
}

pub(crate) fn validate_resource_shape(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    input: &MySqlSourceInput,
) -> Result<()> {
    input.validate()?;
    generation_from_schema(schema)?;
    if schema.fields().is_empty() {
        return Err(CdfError::data(
            "MySQL execution requires at least one discovered field",
        ));
    }
    for field in schema.fields() {
        MySqlIdentifier::user(field.name())?;
        source_identifier(field)?;
        if !matches!(
            field.data_type(),
            DataType::Int64
                | DataType::UInt64
                | DataType::Float32
                | DataType::Float64
                | DataType::Utf8
                | DataType::Binary
        ) {
            return Err(CdfError::data(format!(
                "MySQL field `{}` has unsupported Arrow type {:?}",
                field.name(),
                field.data_type()
            )));
        }
    }
    if let Some(cursor) = &descriptor.cursor {
        let field = field_by_name(schema, &cursor.field).ok_or_else(|| {
            CdfError::data(format!(
                "MySQL cursor field `{}` is absent from the schema",
                cursor.field
            ))
        })?;
        if !matches!(
            field.data_type(),
            DataType::Int64 | DataType::UInt64 | DataType::Float64 | DataType::Utf8
        ) || (field.data_type() == &DataType::Utf8 && !text_domain(field))
        {
            return Err(CdfError::contract(format!(
                "MySQL cursor field `{}` does not have an exactly ordered source domain",
                cursor.field
            )));
        }
    }
    Ok(())
}

fn validate_projection(schema: &SchemaRef, projection: &[String]) -> Result<()> {
    if projection.is_empty() {
        return Err(CdfError::contract("MySQL projection cannot be empty"));
    }
    for field in projection {
        if field_by_name(schema, field).is_none() {
            return Err(CdfError::contract(format!(
                "MySQL projection field `{field}` is absent from the schema"
            )));
        }
    }
    Ok(())
}

pub(crate) fn field_by_name<'a>(schema: &'a SchemaRef, name: &str) -> Option<&'a Field> {
    schema
        .fields()
        .iter()
        .find(|field| field.name() == name)
        .map(AsRef::as_ref)
}

fn source_identifier(field: &Field) -> Result<MySqlIdentifier> {
    MySqlIdentifier::user(source_name(field).unwrap_or(field.name()))
}

fn text_domain(field: &Field) -> bool {
    field
        .metadata()
        .get("cdf:physical_type")
        .is_some_and(|physical| {
            physical.starts_with("MYSQL_TYPE_VARCHAR")
                || physical.starts_with("MYSQL_TYPE_VAR_STRING")
                || physical.starts_with("MYSQL_TYPE_STRING")
                || physical.starts_with("MYSQL_TYPE_ENUM")
                || physical.starts_with("MYSQL_TYPE_SET")
                || (physical.contains("BLOB") && !physical.contains("charset=63"))
        })
}

fn orderable(field: &Field) -> bool {
    matches!(
        field.data_type(),
        DataType::Int64 | DataType::UInt64 | DataType::Float32 | DataType::Float64
    ) || (field.data_type() == &DataType::Utf8 && text_domain(field))
}

fn delivery_guarantee(descriptor: &ResourceDescriptor) -> DeliveryGuarantee {
    match descriptor.write_disposition {
        cdf_kernel::WriteDisposition::Merge if !descriptor.primary_key.is_empty() => {
            DeliveryGuarantee::EffectivelyOncePerKey
        }
        cdf_kernel::WriteDisposition::Replace => DeliveryGuarantee::EffectivelyOncePerTarget,
        cdf_kernel::WriteDisposition::CdcApply => DeliveryGuarantee::EffectivelyOncePerPosition,
        cdf_kernel::WriteDisposition::Append | cdf_kernel::WriteDisposition::Merge => {
            DeliveryGuarantee::AtLeastOnceDuplicateRisk
        }
    }
}
