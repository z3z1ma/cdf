use std::collections::BTreeSet;

use arrow_schema::{DataType, Field, SchemaRef, TimeUnit};
use cdf_kernel::{
    CdfError, CompiledScanIntent, CursorValue, DeclarativeExpression, DeclarativeExpressionLiteral,
    PartitionPlan, PushdownFidelity, ResourceDescriptor, Result, SortDirection, SourcePosition,
    source_name,
};
use mongodb::bson::{Bson, DateTime, Document};

use crate::identifier::{MongoDbIdentifier, validate_field_path};

pub(crate) const MONGODB_SOURCE_KIND: &str = "mongodb";

#[derive(Clone, Debug)]
pub(crate) struct MongoDbScan {
    pub(crate) projection: Vec<String>,
    predicates: Vec<StoredPredicate>,
    order_by: Vec<StoredOrder>,
    limit: Option<u64>,
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
    fn bson(self) -> &'static str {
        match self {
            Self::Eq => "$eq",
            Self::Gt => "$gt",
            Self::Gte => "$gte",
            Self::Lt => "$lt",
            Self::Lte => "$lte",
        }
    }
}

#[derive(Clone, Debug)]
struct StoredPredicate {
    source_field: String,
    operator: PredicateOperator,
    value: Bson,
}

#[derive(Clone, Debug)]
struct StoredOrder {
    source_field: String,
    direction: SortDirection,
}

pub(crate) struct MongoDbQuery {
    pub(crate) filter: Document,
    pub(crate) sort: Document,
    pub(crate) limit: Option<i64>,
}

pub(crate) fn scan_from_partition(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    collection: &MongoDbIdentifier,
    partition: &PartitionPlan,
) -> Result<MongoDbScan> {
    if partition.partition_id.as_str() != MONGODB_SOURCE_KIND
        || partition.metadata.get("kind").map(String::as_str) != Some(MONGODB_SOURCE_KIND)
        || partition.metadata.get("resource_id").map(String::as_str)
            != Some(descriptor.resource_id.as_str())
        || partition.metadata.get("collection").map(String::as_str) != Some(collection.as_str())
        || partition.scope != descriptor.state_scope
    {
        return Err(CdfError::contract(
            "MongoDB partition authority does not match its compiled collection resource",
        ));
    }
    if partition.start_position.is_some() && descriptor.cursor.is_none() {
        return Err(CdfError::contract(
            "MongoDB snapshot resource cannot resume from a cursor position",
        ));
    }
    MongoDbScan::from_intent(descriptor, schema, &partition.scan_intent)
}

impl MongoDbScan {
    fn from_intent(
        descriptor: &ResourceDescriptor,
        schema: &SchemaRef,
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
        validate_projection(descriptor, schema, &projection)?;
        let predicates = intent
            .predicates
            .iter()
            .map(|pushed| {
                if pushed.fidelity != PushdownFidelity::Exact {
                    return Err(CdfError::contract(
                        "compiled MongoDB predicate must retain exact fidelity",
                    ));
                }
                parse_supported_predicate(schema, &pushed.predicate.canonical_expression)
                    .ok_or_else(|| {
                        CdfError::contract(
                            "compiled MongoDB predicate is not type-safe and executable",
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let order_by = canonical_order(descriptor, schema, &intent.order_by)?;
        if descriptor.cursor.is_some() && intent.limit.is_some() {
            return Err(CdfError::contract(
                "MongoDB cursor partitions retain limits for generic engine evaluation; a server limit cannot cross a cursor frontier",
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

pub(crate) fn build_query(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    partition: &PartitionPlan,
    scan: &MongoDbScan,
) -> Result<MongoDbQuery> {
    let mut clauses = Vec::new();
    if let Some(position) = &partition.start_position {
        let cursor = descriptor
            .cursor
            .as_ref()
            .ok_or_else(|| CdfError::contract("MongoDB snapshot cannot carry a start position"))?;
        let SourcePosition::Cursor(position) = position else {
            return Err(CdfError::contract(
                "MongoDB start position must be an ordered cursor",
            ));
        };
        if position.field != cursor.field {
            return Err(CdfError::contract(
                "MongoDB start cursor field changed after compilation",
            ));
        }
        let field = field_by_name(schema, &cursor.field)
            .ok_or_else(|| CdfError::contract("MongoDB cursor field disappeared"))?;
        clauses.push(single_field_clause(
            source_field(field)?,
            "$gt",
            cursor_bson(field, &position.value)?,
        ));
    }
    clauses.extend(scan.predicates.iter().map(|predicate| {
        single_field_clause(
            &predicate.source_field,
            predicate.operator.bson(),
            predicate.value.clone(),
        )
    }));
    let filter = match clauses.len() {
        0 => Document::new(),
        1 => clauses
            .pop()
            .ok_or_else(|| CdfError::internal("MongoDB single-clause filter disappeared"))?,
        _ => {
            let mut filter = Document::new();
            filter.insert(
                "$and",
                Bson::Array(clauses.into_iter().map(Bson::Document).collect()),
            );
            filter
        }
    };
    let mut sort = Document::new();
    for order in &scan.order_by {
        sort.insert(
            &order.source_field,
            match order.direction {
                SortDirection::Asc => 1_i32,
                SortDirection::Desc => -1_i32,
            },
        );
    }
    let limit = scan
        .limit
        .map(|value| {
            i64::try_from(value).map_err(|_| CdfError::contract("MongoDB limit exceeds i64"))
        })
        .transpose()?;
    Ok(MongoDbQuery {
        filter,
        sort,
        limit,
    })
}

fn single_field_clause(field: &str, operator: &str, value: Bson) -> Document {
    let mut comparison = Document::new();
    comparison.insert(operator, value);
    let mut clause = Document::new();
    clause.insert(field, comparison);
    clause
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
    if field.is_nullable() || matches!(field.data_type(), DataType::List(_) | DataType::Struct(_)) {
        return None;
    }
    let value = match (field.data_type(), literal) {
        (DataType::Boolean, DeclarativeExpressionLiteral::Boolean(value))
            if operator == PredicateOperator::Eq =>
        {
            Bson::Boolean(*value)
        }
        (DataType::Int32, DeclarativeExpressionLiteral::Signed(value)) => {
            Bson::Int32((*value).try_into().ok()?)
        }
        (DataType::Int64, DeclarativeExpressionLiteral::Signed(value)) => Bson::Int64(*value),
        (DataType::Float64, DeclarativeExpressionLiteral::Float64Bits(bits)) => {
            let value = f64::from_bits(*bits);
            if !value.is_finite() {
                return None;
            }
            Bson::Double(value)
        }
        _ => return None,
    };
    Some(StoredPredicate {
        source_field: source_field(field).ok()?.to_owned(),
        operator,
        value,
    })
}

fn canonical_order(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    requested: &[cdf_kernel::OrderBy],
) -> Result<Vec<StoredOrder>> {
    if let Some(cursor) = &descriptor.cursor {
        let cursor_field = field_by_name(schema, &cursor.field)
            .ok_or_else(|| CdfError::contract("MongoDB cursor field is absent from schema"))?;
        let id_field = field_by_source_name(schema, "_id").ok_or_else(|| {
            CdfError::contract("MongoDB cursor resources require a pinned `_id` stable key")
        })?;
        let canonical = vec![
            StoredOrder {
                source_field: source_field(cursor_field)?.to_owned(),
                direction: SortDirection::Asc,
            },
            StoredOrder {
                source_field: source_field(id_field)?.to_owned(),
                direction: SortDirection::Asc,
            },
        ];
        if !requested.is_empty()
            && (requested.len() != 2
                || requested[0].field != cursor.field
                || requested[0].direction != SortDirection::Asc
                || requested[1].field.as_str() != id_field.name()
                || requested[1].direction != SortDirection::Asc)
        {
            return Err(CdfError::contract(
                "MongoDB cursor scans require exact cursor ASC, `_id` ASC ordering",
            ));
        }
        return Ok(canonical);
    }
    requested
        .iter()
        .map(|order| {
            let field = field_by_name(schema, &order.field).ok_or_else(|| {
                CdfError::contract(format!(
                    "MongoDB order field `{}` is absent from schema",
                    order.field
                ))
            })?;
            if field.data_type() == &DataType::Utf8 {
                return Err(CdfError::contract(format!(
                    "MongoDB string ordering for `{}` is not exact without compiled simple-collation authority",
                    order.field
                )));
            }
            Ok(StoredOrder {
                source_field: source_field(field)?.to_owned(),
                direction: order.direction.clone(),
            })
        })
        .collect()
}

fn validate_projection(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    projection: &[String],
) -> Result<()> {
    if projection.is_empty() {
        return Err(CdfError::contract(
            "MongoDB projection must contain at least one field",
        ));
    }
    let mut unique = BTreeSet::new();
    for name in projection {
        if !unique.insert(name) {
            return Err(CdfError::contract(format!(
                "MongoDB projection repeats field `{name}`"
            )));
        }
        let field = field_by_name(schema, name).ok_or_else(|| {
            CdfError::contract(format!(
                "MongoDB projection field `{name}` is absent from schema"
            ))
        })?;
        source_field(field)?;
    }
    if let Some(cursor) = &descriptor.cursor
        && (!projection.contains(&cursor.field)
            || !projection.iter().any(|name| {
                field_by_name(schema, name).and_then(|field| source_name(field)) == Some("_id")
                    || name == "_id"
            }))
    {
        return Err(CdfError::contract(
            "MongoDB cursor projections must include both the cursor field and `_id` stable key",
        ));
    }
    Ok(())
}

fn cursor_bson(field: &Field, value: &CursorValue) -> Result<Bson> {
    match (field.data_type(), value) {
        (DataType::Int32, CursorValue::I64(value)) => {
            Ok(Bson::Int32((*value).try_into().map_err(|_| {
                CdfError::contract("MongoDB Int32 cursor value exceeds its field domain")
            })?))
        }
        (DataType::Int64, CursorValue::I64(value)) => Ok(Bson::Int64(*value)),
        (DataType::Date32, CursorValue::I64(days)) => Ok(Bson::DateTime(DateTime::from_millis(
            days.checked_mul(86_400_000).ok_or_else(|| {
                CdfError::contract("MongoDB Date32 cursor overflows milliseconds")
            })?,
        ))),
        (
            DataType::Timestamp(TimeUnit::Millisecond, _),
            CursorValue::TimestampMicros { micros, .. },
        ) if micros.rem_euclid(1_000) == 0 => {
            Ok(Bson::DateTime(DateTime::from_millis(micros / 1_000)))
        }
        _ => Err(CdfError::contract(format!(
            "MongoDB cursor value does not match field `{}` type {:?}",
            field.name(),
            field.data_type()
        ))),
    }
}

pub(crate) fn field_by_name<'a>(schema: &'a SchemaRef, name: &str) -> Option<&'a Field> {
    schema
        .fields()
        .iter()
        .map(AsRef::as_ref)
        .find(|field| field.name() == name)
}

fn field_by_source_name<'a>(schema: &'a SchemaRef, name: &str) -> Option<&'a Field> {
    schema
        .fields()
        .iter()
        .map(AsRef::as_ref)
        .find(|field| source_name(field).unwrap_or_else(|| field.name()) == name)
}

pub(crate) fn source_field(field: &Field) -> Result<&str> {
    let source = source_name(field).unwrap_or_else(|| field.name());
    validate_field_path(source)?;
    Ok(source)
}
