use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};

use arrow_array::{
    ArrayRef, BooleanArray, Date32Array, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use cdf_kernel::{
    BackpressureSupport, Batch, BatchId, BatchStream, BoxFuture, CapabilitySupport, CdfError,
    CursorPosition, CursorValue, DeliveryGuarantee, EstimateSupport, FilterCapabilities,
    IncrementalShape, PartitionId, PartitionPlan, PartitioningCapabilities, PlanId,
    PushdownFidelity, PushedPredicate, QueryableResource, ReplaySupport, ResourceCapabilities,
    ResourceDescriptor, ResourceStream, Result, ScanPlan, ScanPredicate, ScanRequest, SchemaHash,
    SchemaSource, ScopeKind, SortDirection, SourcePosition, source_name,
};
use futures_util::stream;
use postgres::{Client, NoTls, Row, types::ToSql};
use serde::{Deserialize, Serialize};

use cdf_postgres::{PostgresIdentifier, PostgresTarget};
use cdf_runtime::ExecutionServices;

pub const POSTGRES_SQL_SCAN_METADATA: &str = "postgres_sql_scan";

const POSTGRES_SQL_KIND: &str = "sql";
const POSTGRES_SQL_DIALECT: &str = "postgres";

#[derive(Clone)]
pub struct PostgresTableResource {
    descriptor: ResourceDescriptor,
    schema: SchemaRef,
    target: PostgresTarget,
    database_url: String,
    capabilities: ResourceCapabilities,
    execution: Option<ExecutionServices>,
    type_policy_allowances: cdf_kernel::TypePolicyAllowances,
}

impl PostgresTableResource {
    pub fn new(
        database_url: impl Into<String>,
        descriptor: ResourceDescriptor,
        schema: SchemaRef,
        target: PostgresTarget,
    ) -> Result<Self> {
        let database_url = database_url.into();
        if database_url.trim().is_empty() {
            return Err(CdfError::auth(
                "Postgres source connection string resolved to an empty value",
            ));
        }
        validate_postgres_table_resource_shape(&descriptor, &schema, &target)?;
        let capabilities = postgres_table_capabilities(&descriptor);
        Ok(Self {
            descriptor,
            schema,
            target,
            database_url,
            capabilities,
            execution: None,
            type_policy_allowances: Default::default(),
        })
    }

    pub fn with_execution(mut self, execution: ExecutionServices) -> Self {
        self.execution = Some(execution);
        self
    }

    pub fn with_type_policy(mut self, allowances: cdf_kernel::TypePolicyAllowances) -> Self {
        self.type_policy_allowances = allowances;
        self
    }
}

impl fmt::Debug for PostgresTableResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresTableResource")
            .field("descriptor", &self.descriptor)
            .field("schema", &self.schema)
            .field("target", &self.target)
            .field("database_url", &"<redacted>")
            .field("capabilities", &self.capabilities)
            .field("managed_execution", &self.execution.is_some())
            .finish()
    }
}

impl ResourceStream for PostgresTableResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        Ok(vec![plan_postgres_table_partition(
            &self.descriptor,
            &self.schema,
            &self.target,
            request,
        )?])
    }

    fn type_policy_allowances(&self) -> cdf_kernel::TypePolicyAllowances {
        self.type_policy_allowances
    }

    fn open(&self, partition: PartitionPlan) -> BoxFuture<'_, Result<BatchStream>> {
        let descriptor = self.descriptor.clone();
        let schema = Arc::clone(&self.schema);
        let target = self.target.clone();
        let database_url = self.database_url.clone();
        let execution = self.execution.clone();

        Box::pin(async move {
            let batches = match execution {
                Some(execution) => execution.run_blocking("postgres-source.sync", move || {
                    execute_postgres_table(&database_url, &descriptor, schema, &target, partition)
                })?,
                None => {
                    execute_postgres_table(&database_url, &descriptor, schema, &target, partition)?
                }
            };
            Ok(Box::pin(stream::iter(batches.into_iter().map(Ok))) as BatchStream)
        })
    }
}

impl QueryableResource for PostgresTableResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        &self.capabilities
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        negotiate_postgres_table_scan(&self.descriptor, &self.schema, &self.target, request)
    }
}

pub fn postgres_table_capabilities(descriptor: &ResourceDescriptor) -> ResourceCapabilities {
    ResourceCapabilities {
        projection: CapabilitySupport::Supported,
        filters: FilterCapabilities {
            default_fidelity: PushdownFidelity::Exact,
            supported_operators: vec![
                "=".to_owned(),
                ">".to_owned(),
                ">=".to_owned(),
                "<".to_owned(),
                "<=".to_owned(),
            ],
        },
        limits: CapabilitySupport::Supported,
        ordering: CapabilitySupport::Supported,
        partitioning: match descriptor.state_scope.kind() {
            ScopeKind::Resource => PartitioningCapabilities::default(),
            kind => PartitioningCapabilities {
                parallel_partitions: true,
                supported_scopes: vec![kind],
            },
        },
        incremental: if descriptor.cursor.is_some() {
            IncrementalShape::Cursor
        } else {
            IncrementalShape::Full
        },
        replay: if descriptor.cursor.is_some() {
            ReplaySupport::FromPosition
        } else {
            ReplaySupport::None
        },
        idempotent_reads: true,
        backpressure: BackpressureSupport::Pausable,
        estimates: EstimateSupport::Rows,
    }
}

pub fn validate_postgres_table_resource_shape(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    _target: &PostgresTarget,
) -> Result<()> {
    execution_schema_hash(descriptor)?;
    if schema.fields().is_empty() {
        return Err(CdfError::data(
            "Postgres table source execution requires a declared schema with at least one field",
        ));
    }

    let mut names = BTreeSet::new();
    for field in schema.fields() {
        if !names.insert(field.name().to_owned()) {
            return Err(CdfError::contract(format!(
                "Postgres table source schema declares duplicate field `{}`",
                field.name()
            )));
        }
        validate_supported_field(field.as_ref())?;
        PostgresIdentifier::user(field.name().as_str())?;
        source_column_identifier(field.as_ref())?;
    }
    if let Some(cursor) = &descriptor.cursor
        && field_by_name(schema, &cursor.field).is_none()
    {
        return Err(CdfError::data(format!(
            "Postgres cursor field `{}` is missing from the declared schema",
            cursor.field
        )));
    }
    Ok(())
}

pub fn postgres_table_predicate_fidelity(schema: &SchemaRef, expression: &str) -> PushdownFidelity {
    match parse_supported_predicate(schema, expression) {
        Some(_) => PushdownFidelity::Exact,
        None => PushdownFidelity::Unsupported,
    }
}

pub fn negotiate_postgres_table_scan(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    target: &PostgresTarget,
    request: &ScanRequest,
) -> Result<ScanPlan> {
    if request.resource_id != descriptor.resource_id {
        return Err(CdfError::contract(format!(
            "scan request resource `{}` does not match Postgres resource `{}`",
            request.resource_id, descriptor.resource_id
        )));
    }
    validate_postgres_table_resource_shape(descriptor, schema, target)?;

    let (pushed_predicates, unsupported_predicates) =
        classify_postgres_table_predicates(schema, &request.filters);

    Ok(ScanPlan {
        plan_id: PlanId::new(format!("postgres-scan-{}", descriptor.resource_id))?,
        request: request.clone(),
        partitions: vec![plan_postgres_table_partition(
            descriptor, schema, target, request,
        )?],
        pushed_predicates,
        unsupported_predicates,
        estimated_rows: None,
        estimated_bytes: None,
        delivery_guarantee: delivery_guarantee(descriptor),
    })
}

pub fn classify_postgres_table_predicates(
    schema: &SchemaRef,
    predicates: &[ScanPredicate],
) -> (Vec<PushedPredicate>, Vec<ScanPredicate>) {
    let mut pushed = Vec::new();
    let mut unsupported = Vec::new();
    for predicate in predicates {
        match parse_supported_predicate(schema, &predicate.expression) {
            Some(_) => pushed.push(PushedPredicate {
                predicate: predicate.clone(),
                fidelity: PushdownFidelity::Exact,
            }),
            None => unsupported.push(predicate.clone()),
        }
    }
    (pushed, unsupported)
}

pub fn plan_postgres_table_partition(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    target: &PostgresTarget,
    request: &ScanRequest,
) -> Result<PartitionPlan> {
    if request.resource_id != descriptor.resource_id {
        return Err(CdfError::contract(format!(
            "scan request resource `{}` does not match Postgres resource `{}`",
            request.resource_id, descriptor.resource_id
        )));
    }
    validate_postgres_table_resource_shape(descriptor, schema, target)?;
    let scan = PostgresTableScan::from_request(schema, target, request)?;
    let mut metadata = BTreeMap::new();
    metadata.insert("kind".to_owned(), POSTGRES_SQL_KIND.to_owned());
    metadata.insert("dialect".to_owned(), POSTGRES_SQL_DIALECT.to_owned());
    metadata.insert("table".to_owned(), target.display_name());
    metadata.insert("resource_id".to_owned(), descriptor.resource_id.to_string());
    metadata.insert(
        POSTGRES_SQL_SCAN_METADATA.to_owned(),
        serde_json::to_string(&scan).map_err(|error| {
            CdfError::internal(format!("serialize Postgres scan plan: {error}"))
        })?,
    );

    Ok(PartitionPlan {
        partition_id: PartitionId::new("sql")?,
        scope: descriptor.state_scope.clone(),
        start_position: None,
        metadata,
    })
}

fn execute_postgres_table(
    database_url: &str,
    descriptor: &ResourceDescriptor,
    schema: SchemaRef,
    target: &PostgresTarget,
    partition: PartitionPlan,
) -> Result<Vec<Batch>> {
    validate_postgres_table_resource_shape(descriptor, &schema, target)?;
    let scan = scan_from_partition(descriptor, &schema, target, &partition)?;
    let query = build_query(&schema, target, &scan)?;

    let mut client = Client::connect(database_url, NoTls)
        .map_err(|error| CdfError::transient(format!("connect to Postgres source: {error}")))?;
    let params = query
        .params
        .iter()
        .map(SqlParam::as_to_sql)
        .collect::<Vec<_>>();
    let rows = client
        .query(&query.sql, &params)
        .map_err(|error| CdfError::data(format!("query Postgres source table: {error}")))?;
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let (record_batch, source_position) = rows_to_record_batch(&schema, descriptor, &scan, &rows)?;
    let mut batch = Batch::from_record_batch(
        BatchId::new(format!(
            "{}-{}-000001",
            sanitize_id_part(descriptor.resource_id.as_str()),
            sanitize_id_part(partition.partition_id.as_str())
        ))?,
        descriptor.resource_id.clone(),
        partition.partition_id,
        execution_schema_hash(descriptor)?,
        record_batch,
    )?;
    batch.header.source_position = source_position;
    Ok(vec![batch])
}

fn scan_from_partition(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    target: &PostgresTarget,
    partition: &PartitionPlan,
) -> Result<PostgresTableScan> {
    if partition.partition_id.as_str() != "sql" {
        return Err(CdfError::contract(format!(
            "Postgres table resource `{}` expected partition `sql`, got `{}`",
            descriptor.resource_id, partition.partition_id
        )));
    }
    if partition.metadata.get("kind").map(String::as_str) != Some(POSTGRES_SQL_KIND) {
        return Err(CdfError::contract(format!(
            "Postgres table resource `{}` expected a SQL partition plan",
            descriptor.resource_id
        )));
    }
    if partition.metadata.get("dialect").map(String::as_str) != Some(POSTGRES_SQL_DIALECT) {
        return Err(CdfError::contract(
            "Postgres table source partition must declare dialect `postgres`",
        ));
    }
    if partition.metadata.get("resource_id").map(String::as_str)
        != Some(descriptor.resource_id.as_str())
    {
        return Err(CdfError::contract(format!(
            "Postgres source partition resource id does not match `{}`",
            descriptor.resource_id
        )));
    }
    if partition.metadata.get("table").map(String::as_str) != Some(target.display_name().as_str()) {
        return Err(CdfError::contract(format!(
            "Postgres source partition table does not match `{}`",
            target.display_name()
        )));
    }
    if partition.scope != descriptor.state_scope {
        return Err(CdfError::contract(format!(
            "Postgres source partition scope does not match resource `{}`",
            descriptor.resource_id
        )));
    }

    let metadata = partition
        .metadata
        .get(POSTGRES_SQL_SCAN_METADATA)
        .ok_or_else(|| CdfError::contract("Postgres SQL partition is missing scan metadata"))?;
    let scan = serde_json::from_str::<PostgresTableScan>(metadata)
        .map_err(|error| CdfError::contract(format!("decode Postgres scan metadata: {error}")))?;
    scan.validate(schema, target)?;
    if let Some(cursor) = &descriptor.cursor
        && !scan.projection.iter().any(|field| field == &cursor.field)
    {
        return Err(CdfError::contract(format!(
            "Postgres cursor field `{}` must be projected so emitted rows can carry cursor position",
            cursor.field
        )));
    }
    Ok(scan)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PostgresTableScan {
    version: u16,
    target: String,
    projection: Vec<String>,
    filters: Vec<PostgresStoredPredicate>,
    order_by: Vec<PostgresStoredOrder>,
    limit: Option<u64>,
}

impl PostgresTableScan {
    fn from_request(
        schema: &SchemaRef,
        target: &PostgresTarget,
        request: &ScanRequest,
    ) -> Result<Self> {
        let projection = match &request.projection {
            Some(fields) => fields.clone(),
            None => schema
                .fields()
                .iter()
                .map(|field| field.name().to_owned())
                .collect(),
        };
        validate_projection(schema, &projection)?;

        let filters = request
            .filters
            .iter()
            .filter_map(|predicate| parse_supported_predicate(schema, &predicate.expression))
            .collect();
        let order_by = request
            .order_by
            .iter()
            .map(|order| {
                if field_by_name(schema, &order.field).is_none() {
                    return Err(CdfError::contract(format!(
                        "Postgres order field `{}` is not in the declared schema",
                        order.field
                    )));
                }
                PostgresIdentifier::user(order.field.as_str())?;
                Ok(PostgresStoredOrder {
                    field: order.field.clone(),
                    direction: PostgresStoredDirection::from(&order.direction),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            version: 1,
            target: target.display_name(),
            projection,
            filters,
            order_by,
            limit: request.limit,
        })
    }

    fn validate(&self, schema: &SchemaRef, target: &PostgresTarget) -> Result<()> {
        if self.version != 1 {
            return Err(CdfError::contract(format!(
                "unsupported Postgres scan metadata version {}",
                self.version
            )));
        }
        if self.target != target.display_name() {
            return Err(CdfError::contract(format!(
                "Postgres scan target `{}` does not match `{}`",
                self.target,
                target.display_name()
            )));
        }
        validate_projection(schema, &self.projection)?;
        for predicate in &self.filters {
            predicate.validate(schema)?;
        }
        for order in &self.order_by {
            if field_by_name(schema, &order.field).is_none() {
                return Err(CdfError::contract(format!(
                    "Postgres order field `{}` is not in the declared schema",
                    order.field
                )));
            }
            PostgresIdentifier::user(order.field.as_str())?;
        }
        if let Some(limit) = self.limit {
            i64::try_from(limit).map_err(|_| {
                CdfError::contract(format!("Postgres scan limit {limit} exceeds i64::MAX"))
            })?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PostgresStoredPredicate {
    field: String,
    operator: PostgresPredicateOperator,
    literal: String,
}

impl PostgresStoredPredicate {
    fn validate(&self, schema: &SchemaRef) -> Result<()> {
        let field = field_by_name(schema, &self.field).ok_or_else(|| {
            CdfError::contract(format!(
                "Postgres predicate field `{}` is not in the declared schema",
                self.field
            ))
        })?;
        source_column_identifier(field)?;
        parse_literal_for_field(field, self.operator, &self.literal)
            .ok_or_else(|| CdfError::contract("Postgres predicate metadata is not type-safe"))?;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PostgresStoredOrder {
    field: String,
    direction: PostgresStoredDirection,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PostgresStoredDirection {
    Asc,
    Desc,
}

impl PostgresStoredDirection {
    fn from(direction: &SortDirection) -> Self {
        match direction {
            SortDirection::Asc => Self::Asc,
            SortDirection::Desc => Self::Desc,
        }
    }

    fn sql(self) -> &'static str {
        match self {
            Self::Asc => "ASC",
            Self::Desc => "DESC",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PostgresPredicateOperator {
    Eq,
    Gt,
    Gte,
    Lt,
    Lte,
}

impl PostgresPredicateOperator {
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

struct PostgresQuery {
    sql: String,
    params: Vec<SqlParam>,
}

#[derive(Clone, Debug)]
enum SqlParam {
    String(String),
    I64(i64),
    F64(f64),
    Bool(bool),
}

impl SqlParam {
    fn as_to_sql(&self) -> &(dyn ToSql + Sync) {
        match self {
            Self::String(value) => value,
            Self::I64(value) => value,
            Self::F64(value) => value,
            Self::Bool(value) => value,
        }
    }
}

fn build_query(
    schema: &SchemaRef,
    target: &PostgresTarget,
    scan: &PostgresTableScan,
) -> Result<PostgresQuery> {
    let projection = scan
        .projection
        .iter()
        .map(|name| {
            let field = field_by_name(schema, name).ok_or_else(|| {
                CdfError::contract(format!(
                    "Postgres projection field `{name}` is not in the declared schema"
                ))
            })?;
            select_expression(field)
        })
        .collect::<Result<Vec<_>>>()?;
    let mut sql = format!("SELECT {} FROM {}", projection.join(", "), target.sql());
    let mut params = Vec::new();

    if !scan.filters.is_empty() {
        let predicates = scan
            .filters
            .iter()
            .map(|predicate| {
                let field = field_by_name(schema, &predicate.field).ok_or_else(|| {
                    CdfError::contract(format!(
                        "Postgres predicate field `{}` is not in the declared schema",
                        predicate.field
                    ))
                })?;
                let value = parse_literal_for_field(field, predicate.operator, &predicate.literal)
                    .ok_or_else(|| {
                        CdfError::contract("Postgres predicate metadata is not type-safe")
                    })?;
                params.push(value.param);
                Ok(format!(
                    "{} {} ${}::{}",
                    source_column_identifier(field)?.quoted(),
                    predicate.operator.sql(),
                    params.len(),
                    value.postgres_type
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        sql.push_str(" WHERE ");
        sql.push_str(&predicates.join(" AND "));
    }

    if !scan.order_by.is_empty() {
        let ordering = scan
            .order_by
            .iter()
            .map(|order| {
                let field = field_by_name(schema, &order.field).ok_or_else(|| {
                    CdfError::contract(format!(
                        "Postgres order field `{}` is not in the declared schema",
                        order.field
                    ))
                })?;
                Ok(format!(
                    "{} {}",
                    source_column_identifier(field)?.quoted(),
                    order.direction.sql()
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        sql.push_str(" ORDER BY ");
        sql.push_str(&ordering.join(", "));
    }

    if let Some(limit) = scan.limit {
        let limit = i64::try_from(limit).map_err(|_| {
            CdfError::contract(format!("Postgres scan limit {limit} exceeds i64::MAX"))
        })?;
        params.push(SqlParam::I64(limit));
        sql.push_str(&format!(" LIMIT ${}", params.len()));
    }

    Ok(PostgresQuery { sql, params })
}

fn select_expression(field: &Field) -> Result<String> {
    let source = source_column_identifier(field)?.quoted();
    let output = PostgresIdentifier::user(field.name().as_str())?.quoted();
    let expression = match field.data_type() {
        DataType::Boolean => format!("{source}::boolean AS {output}"),
        DataType::Int64 => format!("{source}::bigint AS {output}"),
        DataType::UInt64 => format!("{source}::text AS {output}"),
        DataType::Float64 => format!("{source}::double precision AS {output}"),
        DataType::Utf8 => format!("{source}::text AS {output}"),
        DataType::Date32 => format!("({source} - DATE '1970-01-01')::integer AS {output}"),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            format!("floor(extract(epoch from {source}) * 1000)::bigint AS {output}")
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            format!("floor(extract(epoch from {source}) * 1000000)::bigint AS {output}")
        }
        other => {
            return Err(CdfError::data(format!(
                "Postgres table source does not support Arrow type {other:?}"
            )));
        }
    };
    Ok(expression)
}

struct TypedLiteral {
    param: SqlParam,
    postgres_type: &'static str,
}

fn parse_supported_predicate(
    schema: &SchemaRef,
    expression: &str,
) -> Option<PostgresStoredPredicate> {
    let parsed = parse_simple_predicate(expression)?;
    let field_name = parse_predicate_field(parsed.field)?;
    let field = field_by_name(schema, &field_name)?;
    source_column_identifier(field).ok()?;
    let literal = parse_literal_token(parsed.literal)?;
    if !literal_quoting_is_exact_for_field(field, literal.quoted) {
        return None;
    }
    parse_literal_for_field(field, parsed.operator, &literal.value)?;
    Some(PostgresStoredPredicate {
        field: field_name,
        operator: parsed.operator,
        literal: literal.value,
    })
}

struct ParsedPredicate<'a> {
    field: &'a str,
    operator: PostgresPredicateOperator,
    literal: &'a str,
}

fn parse_simple_predicate(expression: &str) -> Option<ParsedPredicate<'_>> {
    for (token, operator) in [
        (">=", PostgresPredicateOperator::Gte),
        ("<=", PostgresPredicateOperator::Lte),
        ("=", PostgresPredicateOperator::Eq),
        (">", PostgresPredicateOperator::Gt),
        ("<", PostgresPredicateOperator::Lt),
    ] {
        let Some((left, right)) = expression.split_once(token) else {
            continue;
        };
        if right.contains(token) {
            return None;
        }
        return Some(ParsedPredicate {
            field: left.trim(),
            operator,
            literal: right.trim(),
        });
    }
    None
}

fn parse_predicate_field(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if let Some(unquoted) = parse_quoted_identifier(value) {
        return Some(unquoted);
    }
    if value
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return Some(value.to_owned());
    }
    None
}

fn parse_quoted_identifier(value: &str) -> Option<String> {
    let inner = value.strip_prefix('"')?.strip_suffix('"')?;
    if inner.is_empty() {
        return None;
    }
    let mut output = String::new();
    let mut chars = inner.chars().peekable();
    while let Some(character) = chars.next() {
        if character == '"' {
            if chars.peek() == Some(&'"') {
                chars.next();
                output.push('"');
            } else {
                return None;
            }
        } else {
            output.push(character);
        }
    }
    Some(output)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ParsedLiteral {
    value: String,
    quoted: bool,
}

fn parse_literal_token(value: &str) -> Option<ParsedLiteral> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if value.starts_with('\'') {
        return parse_single_quoted_literal(value);
    }
    if value.split_whitespace().count() == 1
        && !value.contains(';')
        && !value.contains("--")
        && !value.contains("/*")
    {
        return Some(ParsedLiteral {
            value: value.to_owned(),
            quoted: false,
        });
    }
    None
}

fn parse_single_quoted_literal(value: &str) -> Option<ParsedLiteral> {
    let mut chars = value.char_indices();
    if chars.next()? != (0, '\'') {
        return None;
    }

    let mut output = String::new();
    while let Some((index, character)) = chars.next() {
        if character == '\'' {
            if value[index + character.len_utf8()..].starts_with('\'') {
                chars.next();
                output.push('\'');
                continue;
            }
            if value[index + character.len_utf8()..].trim().is_empty() {
                return Some(ParsedLiteral {
                    value: output,
                    quoted: true,
                });
            }
            return None;
        }
        output.push(character);
    }
    None
}

fn literal_quoting_is_exact_for_field(field: &Field, quoted: bool) -> bool {
    match field.data_type() {
        DataType::Utf8
        | DataType::Date32
        | DataType::Timestamp(TimeUnit::Millisecond | TimeUnit::Microsecond, _) => quoted,
        DataType::Boolean | DataType::Int64 | DataType::UInt64 | DataType::Float64 => !quoted,
        _ => false,
    }
}

fn parse_literal_for_field(
    field: &Field,
    operator: PostgresPredicateOperator,
    literal: &str,
) -> Option<TypedLiteral> {
    match field.data_type() {
        DataType::Utf8 => Some(TypedLiteral {
            param: SqlParam::String(literal.to_owned()),
            postgres_type: "text",
        }),
        DataType::Int64 => Some(TypedLiteral {
            param: SqlParam::I64(literal.parse::<i64>().ok()?),
            postgres_type: "bigint",
        }),
        DataType::UInt64 => Some(TypedLiteral {
            param: SqlParam::String(literal.parse::<u64>().ok()?.to_string()),
            postgres_type: "numeric",
        }),
        DataType::Float64 => {
            let value = literal.parse::<f64>().ok()?;
            if !value.is_finite() {
                return None;
            }
            Some(TypedLiteral {
                param: SqlParam::F64(value),
                postgres_type: "double precision",
            })
        }
        DataType::Boolean if operator == PostgresPredicateOperator::Eq => {
            let value = match literal {
                value if value.eq_ignore_ascii_case("true") => true,
                value if value.eq_ignore_ascii_case("false") => false,
                _ => return None,
            };
            Some(TypedLiteral {
                param: SqlParam::Bool(value),
                postgres_type: "boolean",
            })
        }
        DataType::Boolean => None,
        DataType::Date32 => {
            parse_date32(literal)?;
            Some(TypedLiteral {
                param: SqlParam::String(literal.to_owned()),
                postgres_type: "date",
            })
        }
        DataType::Timestamp(TimeUnit::Millisecond | TimeUnit::Microsecond, timezone) => {
            parse_rfc3339_micros(literal)?;
            Some(TypedLiteral {
                param: SqlParam::String(literal.to_owned()),
                postgres_type: if timezone.is_some() {
                    "timestamptz"
                } else {
                    "timestamp"
                },
            })
        }
        _ => None,
    }
}

fn rows_to_record_batch(
    schema: &SchemaRef,
    descriptor: &ResourceDescriptor,
    scan: &PostgresTableScan,
    rows: &[Row],
) -> Result<(RecordBatch, Option<SourcePosition>)> {
    let projected_fields = scan
        .projection
        .iter()
        .map(|name| {
            field_by_name(schema, name)
                .ok_or_else(|| {
                    CdfError::contract(format!(
                        "Postgres projection field `{name}` is not in the declared schema"
                    ))
                })
                .cloned()
        })
        .collect::<Result<Vec<_>>>()?;
    let output_schema = Arc::new(Schema::new(projected_fields.clone()));
    let mut arrays = Vec::with_capacity(projected_fields.len());
    let mut cursor = None;

    for (index, field) in projected_fields.iter().enumerate() {
        arrays.push(array_for_field(field, rows, index)?);
        if descriptor
            .cursor
            .as_ref()
            .is_some_and(|cursor| cursor.field == field.name().as_str())
        {
            cursor = Some(max_cursor_for_field(field, rows, index)?);
        }
    }

    let record_batch = RecordBatch::try_new(output_schema, arrays).map_err(CdfError::from)?;
    let source_position = match (&descriptor.cursor, cursor) {
        (Some(cursor_spec), Some(value)) => Some(SourcePosition::Cursor(CursorPosition {
            version: 1,
            field: cursor_spec.field.clone(),
            value: value.into_cursor_value(),
        })),
        (Some(cursor_spec), None) => {
            return Err(CdfError::data(format!(
                "Postgres cursor field `{}` is missing from emitted rows",
                cursor_spec.field
            )));
        }
        (None, _) => None,
    };
    Ok((record_batch, source_position))
}

fn array_for_field(field: &Field, rows: &[Row], index: usize) -> Result<ArrayRef> {
    match field.data_type() {
        DataType::Boolean => Ok(Arc::new(BooleanArray::from(
            rows.iter()
                .map(|row| checked_value(field, row, index, row_bool))
                .collect::<Result<Vec<_>>>()?,
        ))),
        DataType::Int64 => Ok(Arc::new(Int64Array::from(
            rows.iter()
                .map(|row| checked_value(field, row, index, row_i64))
                .collect::<Result<Vec<_>>>()?,
        ))),
        DataType::UInt64 => Ok(Arc::new(UInt64Array::from(
            rows.iter()
                .map(|row| checked_value(field, row, index, row_u64))
                .collect::<Result<Vec<_>>>()?,
        ))),
        DataType::Float64 => Ok(Arc::new(Float64Array::from(
            rows.iter()
                .map(|row| checked_value(field, row, index, row_f64))
                .collect::<Result<Vec<_>>>()?,
        ))),
        DataType::Utf8 => Ok(Arc::new(StringArray::from(
            rows.iter()
                .map(|row| checked_value(field, row, index, row_string))
                .collect::<Result<Vec<_>>>()?,
        ))),
        DataType::Date32 => Ok(Arc::new(Date32Array::from(
            rows.iter()
                .map(|row| checked_value(field, row, index, row_date32))
                .collect::<Result<Vec<_>>>()?,
        ))),
        DataType::Timestamp(TimeUnit::Millisecond, timezone) => {
            let array = TimestampMillisecondArray::from(
                rows.iter()
                    .map(|row| checked_value(field, row, index, row_i64))
                    .collect::<Result<Vec<_>>>()?,
            )
            .with_timezone_opt(timezone.clone());
            Ok(Arc::new(array))
        }
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
            let array = TimestampMicrosecondArray::from(
                rows.iter()
                    .map(|row| checked_value(field, row, index, row_i64))
                    .collect::<Result<Vec<_>>>()?,
            )
            .with_timezone_opt(timezone.clone());
            Ok(Arc::new(array))
        }
        other => Err(CdfError::data(format!(
            "Postgres table source does not support Arrow type {other:?}"
        ))),
    }
}

fn checked_value<T>(
    field: &Field,
    row: &Row,
    index: usize,
    read: fn(&Row, usize, &Field) -> Result<Option<T>>,
) -> Result<Option<T>> {
    let value = read(row, index, field)?;
    if value.is_none() && !field.is_nullable() {
        return Err(CdfError::data(format!(
            "Postgres row has NULL for non-nullable field `{}`",
            field.name()
        )));
    }
    Ok(value)
}

fn row_bool(row: &Row, index: usize, field: &Field) -> Result<Option<bool>> {
    row.try_get::<usize, Option<bool>>(index)
        .map_err(|error| row_error(field, error))
}

fn row_i64(row: &Row, index: usize, field: &Field) -> Result<Option<i64>> {
    row.try_get::<usize, Option<i64>>(index)
        .map_err(|error| row_error(field, error))
}

fn row_f64(row: &Row, index: usize, field: &Field) -> Result<Option<f64>> {
    let value = row
        .try_get::<usize, Option<f64>>(index)
        .map_err(|error| row_error(field, error))?;
    if value.is_some_and(|value| !value.is_finite()) {
        return Err(CdfError::data(format!(
            "Postgres row field `{}` contains a non-finite float64",
            field.name()
        )));
    }
    Ok(value)
}

fn row_string(row: &Row, index: usize, field: &Field) -> Result<Option<String>> {
    row.try_get::<usize, Option<String>>(index)
        .map_err(|error| row_error(field, error))
}

fn row_u64(row: &Row, index: usize, field: &Field) -> Result<Option<u64>> {
    row_string(row, index, field)?
        .map(|value| {
            value.parse::<u64>().map_err(|error| {
                CdfError::data(format!(
                    "Postgres row field `{}` cannot be parsed as uint64: {error}",
                    field.name()
                ))
            })
        })
        .transpose()
}

fn row_date32(row: &Row, index: usize, field: &Field) -> Result<Option<i32>> {
    row.try_get::<usize, Option<i32>>(index)
        .map_err(|error| row_error(field, error))
}

fn row_error(field: &Field, error: postgres::Error) -> CdfError {
    CdfError::data(format!(
        "Postgres row field `{}` does not match declared Arrow type {:?}: {error}",
        field.name(),
        field.data_type()
    ))
}

fn max_cursor_for_field(field: &Field, rows: &[Row], index: usize) -> Result<ObservedCursor> {
    let mut max_value = None;
    for row in rows {
        let value = cursor_value_for_field(field, row, index)?;
        if max_value
            .as_ref()
            .is_none_or(|current| value.greater_than(current))
        {
            max_value = Some(value);
        }
    }
    max_value.ok_or_else(|| {
        CdfError::data(format!(
            "Postgres cursor field `{}` has no observed values",
            field.name()
        ))
    })
}

fn cursor_value_for_field(field: &Field, row: &Row, index: usize) -> Result<ObservedCursor> {
    Ok(match field.data_type() {
        DataType::Utf8 => {
            ObservedCursor::String(required_cursor(field, row_string(row, index, field)?)?)
        }
        DataType::Int64 => {
            ObservedCursor::I64(required_cursor(field, row_i64(row, index, field)?)?)
        }
        DataType::UInt64 => {
            ObservedCursor::U64(required_cursor(field, row_u64(row, index, field)?)?)
        }
        DataType::Float64 => {
            ObservedCursor::F64(required_cursor(field, row_f64(row, index, field)?)?)
        }
        DataType::Date32 => ObservedCursor::I64(i64::from(required_cursor(
            field,
            row_date32(row, index, field)?,
        )?)),
        DataType::Timestamp(TimeUnit::Millisecond, timezone) => ObservedCursor::TimestampMicros {
            micros: required_cursor(field, row_i64(row, index, field)?)?
                .checked_mul(1_000)
                .ok_or_else(|| {
                    CdfError::data(format!(
                        "Postgres cursor field `{}` overflows timestamp microseconds",
                        field.name()
                    ))
                })?,
            timezone: timezone.as_ref().map(ToString::to_string),
        },
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => ObservedCursor::TimestampMicros {
            micros: required_cursor(field, row_i64(row, index, field)?)?,
            timezone: timezone.as_ref().map(ToString::to_string),
        },
        other => {
            return Err(CdfError::data(format!(
                "Postgres cursor field `{}` has unsupported Arrow type {other:?}",
                field.name()
            )));
        }
    })
}

fn required_cursor<T>(field: &Field, value: Option<T>) -> Result<T> {
    value.ok_or_else(|| {
        CdfError::data(format!(
            "Postgres cursor field `{}` is NULL in an accepted row",
            field.name()
        ))
    })
}

#[derive(Clone, Debug, PartialEq)]
enum ObservedCursor {
    String(String),
    I64(i64),
    U64(u64),
    F64(f64),
    TimestampMicros {
        micros: i64,
        timezone: Option<String>,
    },
}

impl ObservedCursor {
    fn greater_than(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(left), Self::String(right)) => left > right,
            (Self::I64(left), Self::I64(right)) => left > right,
            (Self::U64(left), Self::U64(right)) => left > right,
            (Self::F64(left), Self::F64(right)) => left > right,
            (
                Self::TimestampMicros { micros: left, .. },
                Self::TimestampMicros { micros: right, .. },
            ) => left > right,
            _ => false,
        }
    }

    fn into_cursor_value(self) -> CursorValue {
        match self {
            Self::String(value) => CursorValue::String(value),
            Self::I64(value) => CursorValue::I64(value),
            Self::U64(value) => CursorValue::U64(value),
            Self::F64(value) => CursorValue::DecimalString(value.to_string()),
            Self::TimestampMicros { micros, timezone } => {
                CursorValue::TimestampMicros { micros, timezone }
            }
        }
    }
}

fn validate_projection(schema: &SchemaRef, projection: &[String]) -> Result<()> {
    if projection.is_empty() {
        return Err(CdfError::contract(
            "Postgres table source projection must include at least one field",
        ));
    }
    let mut names = BTreeSet::new();
    for name in projection {
        if !names.insert(name) {
            return Err(CdfError::contract(format!(
                "Postgres table source projection repeats field `{name}`"
            )));
        }
        if field_by_name(schema, name).is_none() {
            return Err(CdfError::contract(format!(
                "Postgres projection field `{name}` is not in the declared schema"
            )));
        }
        PostgresIdentifier::user(name.as_str())?;
    }
    Ok(())
}

fn validate_supported_field(field: &Field) -> Result<()> {
    match field.data_type() {
        DataType::Boolean
        | DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::Utf8
        | DataType::Date32
        | DataType::Timestamp(TimeUnit::Millisecond | TimeUnit::Microsecond, _) => Ok(()),
        other => Err(CdfError::data(format!(
            "Postgres table source does not support Arrow type {other:?} for field `{}`",
            field.name()
        ))),
    }
}

fn source_column_identifier(field: &Field) -> Result<PostgresIdentifier> {
    PostgresIdentifier::user(source_name(field).unwrap_or_else(|| field.name().as_str()))
}

fn field_by_name<'a>(schema: &'a Schema, name: &str) -> Option<&'a Field> {
    schema
        .fields()
        .iter()
        .find(|field| field.name() == name)
        .map(|field| field.as_ref())
}

fn execution_schema_hash(descriptor: &ResourceDescriptor) -> Result<SchemaHash> {
    match &descriptor.schema_source {
        SchemaSource::Declared { schema_hash, .. } => Ok(schema_hash.clone()),
        SchemaSource::Discovered { snapshot } => Ok(snapshot.schema_hash.clone()),
        SchemaSource::Discover | SchemaSource::Hints { .. } | SchemaSource::Contract { .. } => {
            Err(CdfError::data(
                "Postgres table source execution requires a declared schema hash or pinned discovered schema snapshot",
            ))
        }
    }
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

fn sanitize_id_part(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '-'
            }
        })
        .collect()
}

fn parse_date32(value: &str) -> Option<i32> {
    let (year, month, day) = parse_date(value)?;
    i32::try_from(days_from_civil(year, month, day)).ok()
}

fn parse_rfc3339_micros(value: &str) -> Option<i64> {
    let (date, rest) = value.split_once('T')?;
    let (year, month, day) = parse_date(date)?;
    let timezone_start = rest
        .rfind(['Z', '+', '-'])
        .filter(|index| *index >= "00:00:00".len())?;
    let (time, timezone) = rest.split_at(timezone_start);
    let offset_seconds = parse_timezone_offset(timezone)?;
    let (clock, fraction) = match time.split_once('.') {
        Some((clock, fraction)) => (clock, Some(fraction)),
        None => (time, None),
    };
    let mut parts = clock.split(':');
    let hour = parts.next()?.parse::<i64>().ok()?;
    let minute = parts.next()?.parse::<i64>().ok()?;
    let second = parts.next()?.parse::<i64>().ok()?;
    if parts.next().is_some()
        || !(0..=23).contains(&hour)
        || !(0..=59).contains(&minute)
        || !(0..=60).contains(&second)
    {
        return None;
    }
    let micros = parse_fraction_micros(fraction.unwrap_or(""))?;
    let days = days_from_civil(year, month, day);
    Some(
        days.saturating_mul(86_400_000_000)
            .saturating_add(hour.saturating_mul(3_600_000_000))
            .saturating_add(minute.saturating_mul(60_000_000))
            .saturating_add(second.saturating_mul(1_000_000))
            .saturating_add(micros)
            .saturating_sub(offset_seconds.saturating_mul(1_000_000)),
    )
}

fn parse_date(value: &str) -> Option<(i64, u32, u32)> {
    if value.len() < 10 {
        return None;
    }
    let year = value.get(..4)?.parse::<i64>().ok()?;
    if value.get(4..5)? != "-" || value.get(7..8)? != "-" || value.len() != 10 {
        return None;
    }
    let month = value.get(5..7)?.parse::<u32>().ok()?;
    let day = value.get(8..10)?.parse::<u32>().ok()?;
    if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
        return None;
    }
    Some((year, month, day))
}

fn parse_timezone_offset(value: &str) -> Option<i64> {
    if value == "Z" {
        return Some(0);
    }
    let sign = match value.get(..1)? {
        "+" => 1,
        "-" => -1,
        _ => return None,
    };
    if value.len() != 6 || value.get(3..4)? != ":" {
        return None;
    }
    let hours = value.get(1..3)?.parse::<i64>().ok()?;
    let minutes = value.get(4..6)?.parse::<i64>().ok()?;
    if !(0..=23).contains(&hours) || !(0..=59).contains(&minutes) {
        return None;
    }
    Some(sign * (hours * 3_600 + minutes * 60))
}

fn parse_fraction_micros(value: &str) -> Option<i64> {
    if value.is_empty() {
        return Some(0);
    }
    if value.len() > 6 || !value.chars().all(|character| character.is_ascii_digit()) {
        return None;
    }
    let padded = format!("{value:0<6}");
    padded.parse::<i64>().ok()
}

fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = i64::from(month);
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;
    use cdf_kernel::{
        ContractRef, CursorOrderingClaim, CursorSpec, ResourceId, ScopeKey, TrustLevel,
        WriteDisposition, with_source_name,
    };

    #[test]
    fn predicate_parser_accepts_only_structured_literals() {
        let schema = schema();
        assert_eq!(
            postgres_table_predicate_fidelity(&schema, "id = 1"),
            PushdownFidelity::Exact
        );
        assert_eq!(
            postgres_table_predicate_fidelity(&schema, "name = 'ada'"),
            PushdownFidelity::Exact
        );
        assert_eq!(
            postgres_table_predicate_fidelity(&schema, "name = ada"),
            PushdownFidelity::Unsupported
        );
        assert_eq!(
            postgres_table_predicate_fidelity(&schema, "id = '1'"),
            PushdownFidelity::Unsupported
        );
        assert_eq!(
            postgres_table_predicate_fidelity(&schema, "id = 1 OR 1 = 1"),
            PushdownFidelity::Unsupported
        );
        assert_eq!(
            postgres_table_predicate_fidelity(&schema, "missing = 1"),
            PushdownFidelity::Unsupported
        );
        assert_eq!(
            postgres_table_predicate_fidelity(&schema, "active > true"),
            PushdownFidelity::Unsupported
        );
    }

    #[test]
    fn partition_metadata_carries_only_safe_scan_shape() {
        let descriptor = descriptor(None);
        let schema = schema();
        let target = PostgresTarget::parse("raw.orders").unwrap();
        let request = ScanRequest {
            resource_id: descriptor.resource_id.clone(),
            projection: Some(vec!["id".to_owned(), "name".to_owned()]),
            filters: vec![
                ScanPredicate {
                    predicate_id: cdf_kernel::PredicateId::new("safe").unwrap(),
                    expression: "id >= 2".to_owned(),
                },
                ScanPredicate {
                    predicate_id: cdf_kernel::PredicateId::new("unsafe").unwrap(),
                    expression: "name = 'ada' OR true".to_owned(),
                },
            ],
            limit: Some(10),
            order_by: vec![cdf_kernel::OrderBy {
                field: "id".to_owned(),
                direction: SortDirection::Desc,
            }],
            scope: ScopeKey::Resource,
        };
        let partition =
            plan_postgres_table_partition(&descriptor, &schema, &target, &request).unwrap();
        assert_eq!(partition.partition_id.as_str(), "sql");
        let scan = serde_json::from_str::<PostgresTableScan>(
            partition.metadata.get(POSTGRES_SQL_SCAN_METADATA).unwrap(),
        )
        .unwrap();
        assert_eq!(scan.projection, vec!["id", "name"]);
        assert_eq!(scan.filters.len(), 1);
        assert_eq!(scan.filters[0].field, "id");
        assert_eq!(scan.limit, Some(10));
    }

    #[test]
    fn source_shape_fails_closed_for_empty_and_unsupported_schemas() {
        let target = PostgresTarget::parse("raw.orders").unwrap();
        let empty_schema = Arc::new(Schema::empty());
        assert!(
            PostgresTableResource::new(
                "postgresql://localhost/db",
                descriptor(None),
                empty_schema,
                target.clone(),
            )
            .is_err()
        );

        let unsupported_schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        assert!(
            PostgresTableResource::new(
                "postgresql://localhost/db",
                descriptor(None),
                unsupported_schema,
                target,
            )
            .is_err()
        );
    }

    #[test]
    fn source_shape_accepts_discovered_snapshot_and_rejects_unpinned_schema_modes() {
        let target = PostgresTarget::parse("raw.orders").unwrap();
        let mut discovered = descriptor(None);
        discovered.schema_source = SchemaSource::Discovered {
            snapshot: cdf_kernel::SchemaSnapshotReference {
                schema_hash: SchemaHash::new("sha256:postgres-discovered-test").unwrap(),
                path: ".cdf/schemas/warehouse.orders@sha256:postgres-discovered-test.json"
                    .to_owned(),
                metadata: BTreeMap::new(),
            },
        };
        PostgresTableResource::new(
            "postgresql://localhost/db",
            discovered,
            schema(),
            target.clone(),
        )
        .unwrap();

        for schema_source in [
            SchemaSource::Discover,
            SchemaSource::Hints {
                source: "test:hints".to_owned(),
                hints_hash: None,
                snapshot: None,
            },
            SchemaSource::Contract {
                contract: ContractRef::new("orders").unwrap(),
                schema_hash: None,
            },
        ] {
            let mut descriptor = descriptor(None);
            descriptor.schema_source = schema_source;
            let error = PostgresTableResource::new(
                "postgresql://localhost/db",
                descriptor,
                schema(),
                target.clone(),
            )
            .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("declared schema hash or pinned discovered schema snapshot")
            );
        }
    }

    #[test]
    fn query_builder_uses_source_name_metadata_for_physical_columns() {
        let schema = Arc::new(Schema::new(vec![with_source_name(
            Field::new("vendor_id", DataType::Int64, false),
            "VendorID",
        )]));
        let target = PostgresTarget::parse("raw.orders").unwrap();
        let scan = PostgresTableScan {
            version: 1,
            target: target.display_name(),
            projection: vec!["vendor_id".to_owned()],
            filters: vec![PostgresStoredPredicate {
                field: "vendor_id".to_owned(),
                operator: PostgresPredicateOperator::Gt,
                literal: "1".to_owned(),
            }],
            order_by: vec![PostgresStoredOrder {
                field: "vendor_id".to_owned(),
                direction: PostgresStoredDirection::Desc,
            }],
            limit: None,
        };

        let query = build_query(&schema, &target, &scan).unwrap();

        assert_eq!(
            query.sql,
            "SELECT \"VendorID\"::bigint AS \"vendor_id\" FROM \"raw\".\"orders\" WHERE \"VendorID\" > $1::bigint ORDER BY \"VendorID\" DESC"
        );
    }

    #[test]
    fn debug_redacts_connection_string() {
        let target = PostgresTarget::parse("raw.orders").unwrap();
        let resource = PostgresTableResource::new(
            "postgresql://user:super-secret@example.com/db",
            descriptor(None),
            schema(),
            target,
        )
        .unwrap();
        let debug = format!("{resource:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("super-secret"));
    }

    #[test]
    fn tampered_partition_metadata_fails_before_connecting() {
        let descriptor = descriptor(None);
        let schema = schema();
        let target = PostgresTarget::parse("raw.orders").unwrap();
        let resource = PostgresTableResource::new(
            "postgresql://127.0.0.1:1/not-used",
            descriptor.clone(),
            Arc::clone(&schema),
            target,
        )
        .unwrap();
        let request = ScanRequest {
            resource_id: descriptor.resource_id.clone(),
            projection: None,
            filters: Vec::new(),
            limit: None,
            order_by: Vec::new(),
            scope: ScopeKey::Resource,
        };
        let mut partition = resource.plan_partitions(&request).unwrap().remove(0);
        partition
            .metadata
            .insert("table".to_owned(), "raw.other".to_owned());
        let error = match futures_executor::block_on(resource.open(partition)) {
            Ok(_) => panic!("tampered partition unexpectedly opened"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("partition table"));
    }

    fn descriptor(cursor: Option<CursorSpec>) -> ResourceDescriptor {
        ResourceDescriptor {
            resource_id: ResourceId::new("warehouse.orders").unwrap(),
            schema_source: SchemaSource::Declared {
                schema_hash: SchemaHash::new("sha256:postgres-source-test").unwrap(),
                source: "test:postgres-source".to_owned(),
            },
            primary_key: vec!["id".to_owned()],
            merge_key: vec!["id".to_owned()],
            cursor,
            write_disposition: WriteDisposition::Merge,
            deduplication: None,
            contract: Some(ContractRef::new("orders").unwrap()),
            state_scope: ScopeKey::Resource,
            freshness: None,
            trust_level: TrustLevel::Governed,
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, false),
        ]))
    }

    #[test]
    fn cursor_projection_is_required_at_open_time() {
        let descriptor = descriptor(Some(CursorSpec {
            field: "id".to_owned(),
            ordering: CursorOrderingClaim::Exact,
            lag_tolerance_ms: 0,
        }));
        let schema = schema();
        let target = PostgresTarget::parse("raw.orders").unwrap();
        let resource = PostgresTableResource::new(
            "postgresql://127.0.0.1:1/not-used",
            descriptor.clone(),
            Arc::clone(&schema),
            target,
        )
        .unwrap();
        let request = ScanRequest {
            resource_id: descriptor.resource_id.clone(),
            projection: Some(vec!["name".to_owned()]),
            filters: Vec::new(),
            limit: None,
            order_by: Vec::new(),
            scope: ScopeKey::Resource,
        };
        let partition = resource.plan_partitions(&request).unwrap().remove(0);
        let error = match futures_executor::block_on(resource.open(partition)) {
            Ok(_) => panic!("cursorless projection unexpectedly opened"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("must be projected"));
    }
}
