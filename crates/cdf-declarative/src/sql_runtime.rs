use std::{fmt, sync::Arc};

use arrow_schema::SchemaRef;
use cdf_dest_postgres::{
    PostgresTableResource, PostgresTarget, plan_postgres_table_partition,
    postgres_table_capabilities, postgres_table_predicate_fidelity,
    validate_postgres_table_resource_shape,
};
use cdf_http::SecretProvider;
use cdf_kernel::{
    BackpressureSupport, BatchStream, BoxFuture, CapabilitySupport, CdfError, EstimateSupport,
    FilterCapabilities, IncrementalShape, PartitionId, PartitionPlan, PartitioningCapabilities,
    PushdownFidelity, QueryableResource, ReplaySupport, ResourceCapabilities, ResourceDescriptor,
    ResourceStream, Result, ScanPlan, ScanRequest,
};

use crate::{CompiledResource, CompiledResourcePlan, SqlResourcePlan};

#[derive(Clone, Default)]
pub struct SqlRuntimeDependencies {
    secret_provider: Option<Arc<dyn SecretProvider + Send + Sync>>,
}

impl SqlRuntimeDependencies {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_secret_provider(
        mut self,
        provider: impl SecretProvider + Send + Sync + 'static,
    ) -> Self {
        self.secret_provider = Some(Arc::new(provider));
        self
    }
}

impl fmt::Debug for SqlRuntimeDependencies {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqlRuntimeDependencies")
            .field("secret_provider", &self.secret_provider.is_some())
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct SqlResource {
    compiled: CompiledResource,
    dependencies: SqlRuntimeDependencies,
}

impl SqlResource {
    pub fn new(compiled: CompiledResource, dependencies: SqlRuntimeDependencies) -> Result<Self> {
        let CompiledResourcePlan::Sql(plan) = compiled.plan() else {
            return Err(CdfError::contract(
                "only compiled SQL resources can be opened with SQL runtime dependencies",
            ));
        };
        let target = postgres_table_target_for_runtime(plan)?;
        validate_postgres_table_resource_shape(compiled.descriptor(), &compiled.schema(), &target)?;
        Ok(Self {
            compiled,
            dependencies,
        })
    }

    pub fn compiled(&self) -> &CompiledResource {
        &self.compiled
    }

    pub fn validate_runtime_dependencies(&self) -> Result<()> {
        let CompiledResourcePlan::Sql(plan) = self.compiled.plan() else {
            return Err(CdfError::contract(
                "only compiled SQL resources can be opened by SqlResource",
            ));
        };
        let provider = self.dependencies.secret_provider.as_deref().ok_or_else(|| {
            CdfError::auth(
                "Postgres SQL resource connection requires an explicit SecretProvider runtime dependency",
            )
        })?;
        let secret = provider.resolve(&plan.connection)?;
        if secret.as_str()?.trim().is_empty() {
            return Err(CdfError::auth(
                "Postgres source connection string resolved to an empty value",
            ));
        }
        Ok(())
    }
}

impl CompiledResource {
    pub fn into_sql_resource(self, dependencies: SqlRuntimeDependencies) -> Result<SqlResource> {
        SqlResource::new(self, dependencies)
    }

    pub fn to_sql_resource(&self, dependencies: SqlRuntimeDependencies) -> Result<SqlResource> {
        SqlResource::new(self.clone(), dependencies)
    }
}

impl ResourceStream for SqlResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        self.compiled.descriptor()
    }

    fn schema(&self) -> SchemaRef {
        self.compiled.schema()
    }

    fn type_policy_allowances(&self) -> cdf_kernel::TypePolicyAllowances {
        self.compiled.type_policy_allowances()
    }

    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        self.compiled.plan_partitions(request)
    }

    fn open(&self, partition: PartitionPlan) -> BoxFuture<'_, Result<BatchStream>> {
        let descriptor = self.compiled.descriptor().clone();
        let schema = self.compiled.schema();
        let plan = match self.compiled.plan() {
            CompiledResourcePlan::Sql(plan) => plan.clone(),
            CompiledResourcePlan::Rest(_) | CompiledResourcePlan::Files(_) => {
                return Box::pin(async {
                    Err(CdfError::contract(
                        "only compiled SQL resources can be opened by SqlResource",
                    ))
                });
            }
        };
        let dependencies = self.dependencies.clone();

        Box::pin(async move {
            let target = postgres_table_target_for_runtime(&plan)?;
            let provider = dependencies.secret_provider.as_deref().ok_or_else(|| {
                CdfError::auth(
                    "Postgres SQL resource connection requires an explicit SecretProvider runtime dependency",
                )
            })?;
            let secret = provider.resolve(&plan.connection)?;
            let database_url = secret.as_str()?.to_owned();
            let resource = PostgresTableResource::new(database_url, descriptor, schema, target)?;
            resource.open(partition).await
        })
    }
}

impl QueryableResource for SqlResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        self.compiled.capabilities()
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        self.compiled.negotiate(request)
    }
}

pub(crate) fn sql_capabilities_for(
    descriptor: &ResourceDescriptor,
    plan: &SqlResourcePlan,
) -> ResourceCapabilities {
    if postgres_table_target_for_planning(plan).is_ok() {
        return postgres_table_capabilities(descriptor);
    }

    ResourceCapabilities {
        projection: CapabilitySupport::Unsupported,
        filters: FilterCapabilities::default(),
        limits: CapabilitySupport::Unsupported,
        ordering: CapabilitySupport::Unsupported,
        partitioning: PartitioningCapabilities::default(),
        incremental: IncrementalShape::Full,
        replay: ReplaySupport::None,
        idempotent_reads: false,
        backpressure: BackpressureSupport::CannotPause,
        estimates: EstimateSupport::None,
    }
}

pub(crate) fn sql_predicate_fidelity_for(
    schema: &SchemaRef,
    plan: &SqlResourcePlan,
    expression: &str,
) -> PushdownFidelity {
    if postgres_table_target_for_planning(plan).is_err() {
        return PushdownFidelity::Unsupported;
    }
    postgres_table_predicate_fidelity(schema, expression)
}

pub(crate) fn sql_partition_for_plan(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    plan: &SqlResourcePlan,
    request: Option<&ScanRequest>,
) -> Result<PartitionPlan> {
    if let (Ok(target), Some(request)) = (postgres_table_target_for_planning(plan), request) {
        return plan_postgres_table_partition(descriptor, schema, &target, request);
    }

    let mut metadata = std::collections::BTreeMap::new();
    metadata.insert("kind".to_owned(), "sql".to_owned());
    if let Some(dialect) = &plan.dialect {
        metadata.insert("dialect".to_owned(), dialect.clone());
    }
    if let Some(table) = &plan.table {
        metadata.insert("table".to_owned(), table.clone());
    }
    metadata.insert("resource_id".to_owned(), descriptor.resource_id.to_string());

    Ok(PartitionPlan {
        partition_id: PartitionId::new("sql")?,
        scope: descriptor.state_scope.clone(),
        start_position: None,
        metadata,
    })
}

pub fn postgres_table_target_for_sql_plan(plan: &SqlResourcePlan) -> Result<PostgresTarget> {
    postgres_table_target_for_runtime(plan)
}

fn postgres_table_target_for_runtime(plan: &SqlResourcePlan) -> Result<PostgresTarget> {
    if plan.query.is_some() {
        return Err(CdfError::contract(
            "arbitrary declarative SQL query resources are not supported by the Postgres table runtime; declare `table` instead",
        ));
    }
    postgres_table_target_for_planning(plan)
}

fn postgres_table_target_for_planning(plan: &SqlResourcePlan) -> Result<PostgresTarget> {
    if !is_postgres_dialect(plan) {
        return Err(CdfError::contract(
            "declarative SQL runtime supports only dialect `postgres`",
        ));
    }
    let table = plan.table.as_deref().ok_or_else(|| {
        CdfError::contract("declarative Postgres SQL resources must declare `table`")
    })?;
    PostgresTarget::parse(table)
}

fn is_postgres_dialect(plan: &SqlResourcePlan) -> bool {
    plan.dialect
        .as_deref()
        .is_none_or(|dialect| dialect.eq_ignore_ascii_case("postgres"))
}
