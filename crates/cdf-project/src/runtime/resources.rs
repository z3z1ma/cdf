use super::prelude::*;

#[derive(Clone, Copy)]
pub struct ProjectRunSource<'a> {
    resource: &'a dyn QueryableResource,
}

impl std::fmt::Debug for ProjectRunSource<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectRunSource")
            .field("resource_id", &self.descriptor().resource_id)
            .field("state_scope", &self.descriptor().state_scope)
            .field("incremental", &self.capabilities().incremental)
            .finish_non_exhaustive()
    }
}

impl<'a> ProjectRunSource<'a> {
    pub fn new(resource: &'a dyn QueryableResource) -> Self {
        Self { resource }
    }

    pub fn stream(self) -> &'a dyn ResourceStream {
        self.resource
    }

    pub fn queryable(self) -> &'a dyn QueryableResource {
        self.resource
    }

    pub fn capabilities(self) -> &'a ResourceCapabilities {
        self.resource.capabilities()
    }

    pub fn descriptor(self) -> &'a ResourceDescriptor {
        self.resource.descriptor()
    }

    pub fn validate_supported(self) -> Result<()> {
        self.resource.validate_runtime_dependencies()
    }
}

pub struct WindowScopedResource<'a> {
    inner: &'a dyn QueryableResource,
    descriptor: ResourceDescriptor,
    inner_scope: ScopeKey,
}

impl<'a> WindowScopedResource<'a> {
    pub fn new(inner: &'a dyn QueryableResource, scope: ScopeKey) -> Self {
        let mut descriptor = inner.descriptor().clone();
        let inner_scope = descriptor.state_scope.clone();
        descriptor.state_scope = scope;
        Self {
            inner,
            descriptor,
            inner_scope,
        }
    }

    fn inner_request(&self, request: &ScanRequest) -> ScanRequest {
        let mut request = request.clone();
        request.scope = self.inner_scope.clone();
        request
    }

    fn outer_partition(&self, mut partition: PartitionPlan) -> PartitionPlan {
        partition.scope = self.descriptor.state_scope.clone();
        partition
    }

    fn inner_partition(&self, mut partition: PartitionPlan) -> PartitionPlan {
        partition.scope = self.inner_scope.clone();
        partition
    }
}

impl ResourceStream for WindowScopedResource<'_> {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        self.inner.schema()
    }

    fn compiled_source_plan_hash(&self) -> Option<&str> {
        self.inner.compiled_source_plan_hash()
    }

    fn validate_runtime_dependencies(&self) -> Result<()> {
        self.inner.validate_runtime_dependencies()
    }

    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        self.inner
            .plan_partitions(&self.inner_request(request))
            .map(|partitions| {
                partitions
                    .into_iter()
                    .map(|partition| self.outer_partition(partition))
                    .collect()
            })
    }

    fn open(&self, partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        self.inner.open(self.inner_partition(partition))
    }

    fn attest_partition(
        &self,
        partition: PartitionPlan,
    ) -> cdf_kernel::PartitionAttestationAttempt<'_> {
        self.inner.attest_partition(self.inner_partition(partition))
    }

    fn effective_schema_runtime(&self) -> Option<&EffectiveSchemaRuntime> {
        self.inner.effective_schema_runtime()
    }

    fn type_policy_allowances(&self) -> cdf_kernel::TypePolicyAllowances {
        self.inner.type_policy_allowances()
    }
}

impl QueryableResource for WindowScopedResource<'_> {
    fn capabilities(&self) -> &ResourceCapabilities {
        self.inner.capabilities()
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        let mut plan = self.inner.negotiate(&self.inner_request(request))?;
        plan.request = request.clone();
        plan.partitions = plan
            .partitions
            .into_iter()
            .map(|partition| self.outer_partition(partition))
            .collect();
        Ok(plan)
    }
}
