use std::{fmt, future::Future, pin::Pin, sync::Arc};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use cdf_kernel::{
    Batch, CdfError, OpenedPartitionStream, PartitionPlan, PredicateId, PushdownFidelity,
    QueryableResource, ScanPlan, ScanPredicate, ScanRequest, ScopeKey,
};
use cdf_runtime::{CpuTaskSpec, ExecutionServices};
use datafusion::{
    catalog::{Session, TableProvider},
    common::{DataFusionError, Result as DataFusionResult, internal_err},
    datasource::provider::TableType,
    logical_expr::{
        Expr, Operator, TableProviderFilterPushDown, TableType as LogicalTableType,
        expr::BinaryExpr,
    },
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
};
use futures_util::{StreamExt, stream};

use crate::planning::datafusion_filter_pushdown;

type SharedQueryableResource = Arc<dyn QueryableResource + Send + Sync>;

#[derive(Clone)]
pub struct QueryableResourceTableProvider {
    resource: SharedQueryableResource,
    scope: ScopeKey,
    execution: ExecutionServices,
}

impl QueryableResourceTableProvider {
    pub fn new(
        resource: SharedQueryableResource,
        scope: ScopeKey,
        execution: ExecutionServices,
    ) -> Self {
        Self {
            resource,
            scope,
            execution,
        }
    }

    fn request(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<ScanRequest> {
        let projection = projection
            .map(|projection| projection_fields(&self.schema(), projection))
            .transpose()?;
        let filters = filters
            .iter()
            .enumerate()
            .filter_map(|(index, filter)| scan_predicate(index, filter).transpose())
            .collect::<DataFusionResult<Vec<_>>>()?;
        Ok(ScanRequest {
            resource_id: self.resource.descriptor().resource_id.clone(),
            projection,
            filters,
            limit: limit.map(|limit| limit as u64),
            order_by: Vec::new(),
            scope: self.scope.clone(),
        })
    }

    fn negotiate(&self, request: &ScanRequest) -> DataFusionResult<ScanPlan> {
        self.resource.negotiate(request).map_err(cdf_to_datafusion)
    }
}

impl fmt::Debug for QueryableResourceTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryableResourceTableProvider")
            .field("resource_id", &self.resource.descriptor().resource_id)
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

pub fn queryable_resource_table_provider(
    resource: SharedQueryableResource,
    scope: ScopeKey,
    execution: ExecutionServices,
) -> Arc<dyn TableProvider> {
    Arc::new(QueryableResourceTableProvider::new(
        resource, scope, execution,
    ))
}

impl TableProvider for QueryableResourceTableProvider {
    fn schema(&self) -> SchemaRef {
        self.resource.schema()
    }

    fn table_type(&self) -> TableType {
        LogicalTableType::Base
    }

    fn scan<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        _state: &'life1 dyn Session,
        projection: Option<&'life2 Vec<usize>>,
        filters: &'life3 [Expr],
        limit: Option<usize>,
    ) -> Pin<Box<dyn Future<Output = DataFusionResult<Arc<dyn ExecutionPlan>>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let classification_request = self.request(projection, filters, None)?;
            let classification = self.negotiate(&classification_request)?;
            let effective_limit = if classification
                .pushed_predicates
                .iter()
                .any(|pushed| pushed.fidelity == PushdownFidelity::Inexact)
            {
                None
            } else {
                limit
            };

            let request = self.request(projection, filters, effective_limit)?;
            let scan = self.negotiate(&request)?;
            let plan: Arc<dyn ExecutionPlan> = Arc::new(QueryableResourceExec::new(
                Arc::clone(&self.resource),
                scan,
                self.schema(),
                projection.cloned(),
                effective_limit,
                self.execution.clone(),
            ));
            Ok(plan)
        })
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        let mut responses = vec![TableProviderFilterPushDown::Unsupported; filters.len()];
        let predicates = filters
            .iter()
            .enumerate()
            .filter_map(|(index, filter)| scan_predicate(index, filter).transpose())
            .collect::<DataFusionResult<Vec<_>>>()?;
        if predicates.is_empty() {
            return Ok(responses);
        }

        let request = ScanRequest {
            resource_id: self.resource.descriptor().resource_id.clone(),
            projection: None,
            filters: predicates,
            limit: None,
            order_by: Vec::new(),
            scope: self.scope.clone(),
        };
        let scan = self.negotiate(&request)?;
        for pushed in &scan.pushed_predicates {
            if let Some(index) = predicate_index(&pushed.predicate.predicate_id)? {
                responses[index] = datafusion_filter_pushdown(&pushed.fidelity);
            }
        }
        Ok(responses)
    }
}

struct QueryableResourceExec {
    resource: SharedQueryableResource,
    scan: ScanPlan,
    projection: Option<Vec<usize>>,
    fetch: Option<usize>,
    execution: ExecutionServices,
    properties: Arc<PlanProperties>,
}

impl QueryableResourceExec {
    fn new(
        resource: SharedQueryableResource,
        scan: ScanPlan,
        input_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        fetch: Option<usize>,
        execution: ExecutionServices,
    ) -> Self {
        let output_schema = projected_schema(&input_schema, projection.as_ref())
            .expect("projection indexes are built from the provider schema");
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            Partitioning::UnknownPartitioning(scan.partitions.len().max(1)),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            resource,
            scan,
            projection,
            fetch,
            execution,
            properties,
        }
    }
}

impl fmt::Debug for QueryableResourceExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryableResourceExec")
            .field("resource_id", &self.scan.request.resource_id)
            .field("partition_count", &self.scan.partitions.len())
            .field("projection", &self.projection)
            .field("fetch", &self.fetch)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for QueryableResourceExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "QueryableResourceExec: resource_id={}, partitions={}, fetch={:?}",
                self.scan.request.resource_id.as_str(),
                self.scan.partitions.len(),
                self.fetch
            ),
            DisplayFormatType::TreeRender => Ok(()),
        }
    }
}

impl ExecutionPlan for QueryableResourceExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            internal_err!("QueryableResourceExec is a leaf plan")
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let Some(partition_plan) = self.scan.partitions.get(partition).cloned() else {
            return internal_err!(
                "partition {partition} is out of range for {} CDF partitions",
                self.scan.partitions.len()
            );
        };
        let schema = self.schema();
        if self.fetch == Some(0) {
            let stream = stream::empty::<DataFusionResult<RecordBatch>>();
            return Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)));
        }
        let resource_stream = stream::try_unfold(
            ResourceExecutionState::Opening {
                resource: Arc::clone(&self.resource),
                partition: Box::new(partition_plan),
                projection: self.projection.clone(),
                remaining: self.fetch,
            },
            |mut state| async move {
                loop {
                    state = match state {
                        ResourceExecutionState::Opening {
                            resource,
                            partition,
                            projection,
                            remaining,
                        } => {
                            let mut opening = resource.open(*partition);
                            let opened = match (&mut opening).await {
                                Ok(opened) => opened,
                                Err(error) => {
                                    return match opening.terminate_and_join().await {
                                        Ok(()) => Err(cdf_to_datafusion(error)),
                                        Err(cleanup) => Err(cdf_to_datafusion(append_cleanup(
                                            error,
                                            "source opening",
                                            cleanup,
                                        ))),
                                    };
                                }
                            };
                            ResourceExecutionState::Opened {
                                stream: opened,
                                projection,
                                remaining,
                            }
                        }
                        ResourceExecutionState::Opened {
                            mut stream,
                            projection,
                            mut remaining,
                        } => {
                            if remaining == Some(0) {
                                stream
                                    .terminate_and_join()
                                    .await
                                    .map_err(cdf_to_datafusion)?;
                                return Ok(None);
                            }
                            let batch = match stream.next().await {
                                Some(Ok(batch)) => batch,
                                Some(Err(error)) => {
                                    return match stream.join_failed_attempt().await {
                                        Ok(()) => Err(cdf_to_datafusion(error)),
                                        Err(cleanup) => Err(cdf_to_datafusion(append_cleanup(
                                            error,
                                            "source stream",
                                            cleanup,
                                        ))),
                                    };
                                }
                                None => {
                                    stream.completion().await.map_err(cdf_to_datafusion)?;
                                    return Ok(None);
                                }
                            };
                            let batch = match cdf_batch_to_record_batch(batch) {
                                Ok(batch) => batch,
                                Err(error) => {
                                    return match stream.terminate_and_join().await {
                                        Ok(()) => Err(error),
                                        Err(cleanup) => Err(append_datafusion_cleanup(
                                            error,
                                            "source batch conversion",
                                            cleanup,
                                        )),
                                    };
                                }
                            };
                            let projected = match project_batch(batch, projection.as_ref()) {
                                Ok(projected) => projected,
                                Err(error) => {
                                    return match stream.terminate_and_join().await {
                                        Ok(()) => Err(error),
                                        Err(cleanup) => Err(append_datafusion_cleanup(
                                            error,
                                            "source projection",
                                            cleanup,
                                        )),
                                    };
                                }
                            };
                            let output = match remaining {
                                Some(left) if projected.num_rows() > left => {
                                    remaining = Some(0);
                                    projected.slice(0, left)
                                }
                                Some(left) => {
                                    remaining = Some(left - projected.num_rows());
                                    projected
                                }
                                None => projected,
                            };
                            return Ok(Some((
                                output,
                                ResourceExecutionState::Opened {
                                    stream,
                                    projection,
                                    remaining,
                                },
                            )));
                        }
                    };
                }
            },
        );
        let session_id = context.session_id();
        let task_id = context.task_id().unwrap_or_else(|| "anonymous".to_owned());
        let run_id = format!("datafusion/{session_id}/{task_id}/{partition}");
        let stream = self
            .execution
            .spawn_cpu_stream(
                &run_id,
                CpuTaskSpec {
                    task_kind: "datafusion.resource_adapter".to_owned(),
                    cpu_slot_cost: 1,
                    native_internal_parallelism: 1,
                },
                2,
                move |mut sender, cancellation| async move {
                    futures_util::pin_mut!(resource_stream);
                    while let Some(result) = resource_stream.next().await {
                        cancellation.check()?;
                        match result {
                            Ok(batch) => sender.send(Ok(batch)).await?,
                            Err(error) => {
                                sender.send(Err(error)).await?;
                                return Ok(());
                            }
                        }
                    }
                    Ok(())
                },
            )
            .map_err(cdf_to_datafusion)?
            .map(|result| result.map_err(cdf_to_datafusion).and_then(|result| result));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn supports_limit_pushdown(&self) -> bool {
        self.fetch.is_some()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        Some(Arc::new(Self::new(
            Arc::clone(&self.resource),
            self.scan.clone(),
            self.resource.schema(),
            self.projection.clone(),
            limit,
            self.execution.clone(),
        )))
    }

    fn fetch(&self) -> Option<usize> {
        self.fetch
    }
}

enum ResourceExecutionState {
    Opening {
        resource: SharedQueryableResource,
        partition: Box<PartitionPlan>,
        projection: Option<Vec<usize>>,
        remaining: Option<usize>,
    },
    Opened {
        stream: OpenedPartitionStream,
        projection: Option<Vec<usize>>,
        remaining: Option<usize>,
    },
}

fn append_cleanup(mut primary: CdfError, context: &str, cleanup: CdfError) -> CdfError {
    primary.message = format!(
        "{}; {context} termination also failed: {}",
        primary.message, cleanup.message
    );
    primary
}

fn append_datafusion_cleanup(
    primary: DataFusionError,
    context: &str,
    cleanup: CdfError,
) -> DataFusionError {
    DataFusionError::Execution(format!(
        "{primary}; {context} termination also failed: {}",
        cleanup.message
    ))
}

fn projection_fields(schema: &SchemaRef, projection: &[usize]) -> DataFusionResult<Vec<String>> {
    projection
        .iter()
        .map(|index| {
            schema
                .fields()
                .get(*index)
                .map(|field| field.name().clone())
                .ok_or_else(|| {
                    DataFusionError::Plan(format!("projection index {index} is outside schema"))
                })
        })
        .collect()
}

fn projected_schema(
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> DataFusionResult<SchemaRef> {
    let Some(projection) = projection else {
        return Ok(Arc::clone(schema));
    };
    Ok(Arc::new(schema.project(projection).map_err(|error| {
        DataFusionError::ArrowError(Box::new(error), None)
    })?))
}

fn project_batch(
    batch: RecordBatch,
    projection: Option<&Vec<usize>>,
) -> DataFusionResult<RecordBatch> {
    let Some(projection) = projection else {
        return Ok(batch);
    };
    batch
        .project(projection)
        .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

fn scan_predicate(index: usize, expr: &Expr) -> DataFusionResult<Option<ScanPredicate>> {
    let Some(expression) = simple_predicate_expression(expr)? else {
        return Ok(None);
    };
    Ok(Some(
        ScanPredicate::new(
            PredicateId::new(format!("df-filter-{index}")).map_err(cdf_to_datafusion)?,
            expression,
        )
        .map_err(cdf_to_datafusion)?,
    ))
}

fn simple_predicate_expression(expr: &Expr) -> DataFusionResult<Option<String>> {
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr else {
        return Ok(None);
    };
    let Some(operator) = comparison_operator(*op) else {
        return Ok(None);
    };
    if let (Some(column), Some(literal)) = (column_name(left), literal_value(right)?) {
        return Ok(Some(format!("{column} {operator} {literal}")));
    }
    if let (Some(literal), Some(column)) = (literal_value(left)?, column_name(right)) {
        return Ok(Some(format!(
            "{column} {} {literal}",
            reverse_operator(operator)
        )));
    }
    Ok(None)
}

fn comparison_operator(operator: Operator) -> Option<&'static str> {
    match operator {
        Operator::Eq => Some("="),
        Operator::NotEq => Some("!="),
        Operator::Gt => Some(">"),
        Operator::GtEq => Some(">="),
        Operator::Lt => Some("<"),
        Operator::LtEq => Some("<="),
        _ => None,
    }
}

fn reverse_operator(operator: &str) -> &'static str {
    match operator {
        ">" => "<",
        ">=" => "<=",
        "<" => ">",
        "<=" => ">=",
        "=" => "=",
        "!=" => "!=",
        _ => unreachable!("comparison_operator returns only known comparison operators"),
    }
}

fn column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(column) => Some(column.name.clone()),
        _ => None,
    }
}

fn literal_value(expr: &Expr) -> DataFusionResult<Option<String>> {
    let Expr::Literal(value, _) = expr else {
        return Ok(None);
    };
    match value {
        datafusion::scalar::ScalarValue::Boolean(Some(value)) => Ok(Some(value.to_string())),
        datafusion::scalar::ScalarValue::Int8(Some(value)) => Ok(Some(value.to_string())),
        datafusion::scalar::ScalarValue::Int16(Some(value)) => Ok(Some(value.to_string())),
        datafusion::scalar::ScalarValue::Int32(Some(value)) => Ok(Some(value.to_string())),
        datafusion::scalar::ScalarValue::Int64(Some(value)) => Ok(Some(value.to_string())),
        datafusion::scalar::ScalarValue::UInt8(Some(value)) => Ok(Some(value.to_string())),
        datafusion::scalar::ScalarValue::UInt16(Some(value)) => Ok(Some(value.to_string())),
        datafusion::scalar::ScalarValue::UInt32(Some(value)) => Ok(Some(value.to_string())),
        datafusion::scalar::ScalarValue::UInt64(Some(value)) => Ok(Some(value.to_string())),
        datafusion::scalar::ScalarValue::Utf8(Some(value))
        | datafusion::scalar::ScalarValue::LargeUtf8(Some(value))
        | datafusion::scalar::ScalarValue::Utf8View(Some(value)) => {
            Ok(Some(format!("'{}'", value.replace('\'', "''"))))
        }
        datafusion::scalar::ScalarValue::Null
        | datafusion::scalar::ScalarValue::Boolean(None)
        | datafusion::scalar::ScalarValue::Int8(None)
        | datafusion::scalar::ScalarValue::Int16(None)
        | datafusion::scalar::ScalarValue::Int32(None)
        | datafusion::scalar::ScalarValue::Int64(None)
        | datafusion::scalar::ScalarValue::UInt8(None)
        | datafusion::scalar::ScalarValue::UInt16(None)
        | datafusion::scalar::ScalarValue::UInt32(None)
        | datafusion::scalar::ScalarValue::UInt64(None)
        | datafusion::scalar::ScalarValue::Utf8(None)
        | datafusion::scalar::ScalarValue::LargeUtf8(None)
        | datafusion::scalar::ScalarValue::Utf8View(None) => Ok(None),
        other => Err(DataFusionError::Plan(format!(
            "unsupported CDF pushdown literal {other:?}"
        ))),
    }
}

fn predicate_index(predicate_id: &PredicateId) -> DataFusionResult<Option<usize>> {
    let Some(raw) = predicate_id.as_str().strip_prefix("df-filter-") else {
        return Ok(None);
    };
    raw.parse::<usize>().map(Some).map_err(|error| {
        DataFusionError::Plan(format!(
            "CDF pushdown predicate id {:?} did not preserve DataFusion filter index: {error}",
            predicate_id.as_str()
        ))
    })
}

fn cdf_batch_to_record_batch(batch: Batch) -> DataFusionResult<RecordBatch> {
    batch.record_batch().cloned().ok_or_else(|| {
        DataFusionError::Execution(
            "DataFusion execution requires in-memory Arrow record batches at MVP".to_owned(),
        )
    })
}

fn cdf_to_datafusion(error: CdfError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
