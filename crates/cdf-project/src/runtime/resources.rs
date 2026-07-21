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

struct WindowScopedPartitionReader {
    inner: Box<dyn cdf_kernel::PlannedPartitionReader>,
    outer_scope: ScopeKey,
}

impl cdf_kernel::PlannedPartitionReader for WindowScopedPartitionReader {
    fn next_partition(
        &mut self,
        expected_ordinal: u64,
    ) -> Result<Option<cdf_kernel::ExecutablePartition>> {
        self.inner
            .next_partition(expected_ordinal)
            .map(|partition| {
                partition.map(|partition| {
                    let outer_scope = self.outer_scope.clone();
                    partition.map_plan(|mut plan| {
                        plan.scope = outer_scope;
                        plan
                    })
                })
            })
    }
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

    fn map_scan_scope(
        &self,
        mut scan: ScanPlan,
        request: ScanRequest,
        scope: ScopeKey,
    ) -> Result<ScanPlan> {
        scan.request = request;
        scan.try_map_partition_authority(|authority| match authority {
            cdf_kernel::PartitionAuthority::Inline(partitions) => {
                Ok(cdf_kernel::PartitionAuthority::Inline(
                    partitions
                        .into_iter()
                        .map(|mut partition| {
                            partition.scope = scope.clone();
                            partition
                        })
                        .collect(),
                ))
            }
            cdf_kernel::PartitionAuthority::External(reference) => {
                Ok(cdf_kernel::PartitionAuthority::External(reference))
            }
        })
    }
}

impl ResourceStream for WindowScopedResource<'_> {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        self.inner.schema()
    }

    fn compiled_source_plan_hash(&self) -> Option<&cdf_kernel::CompiledSourcePlanHash> {
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

    fn planned_partition_reader(
        &self,
        reference: &cdf_kernel::PlannedTaskSetReference,
    ) -> Result<Box<dyn cdf_kernel::PlannedPartitionReader>> {
        Ok(Box::new(WindowScopedPartitionReader {
            inner: self.inner.planned_partition_reader(reference)?,
            outer_scope: self.descriptor.state_scope.clone(),
        }))
    }

    fn rebind_scan_for_resume(
        &self,
        scan: cdf_kernel::ScanPlan,
        committed_frontier: &SourcePosition,
    ) -> Result<cdf_kernel::ScanPlan> {
        let outer_request = scan.request.clone();
        let inner = self.map_scan_scope(
            scan,
            self.inner_request(&outer_request),
            self.inner_scope.clone(),
        )?;
        let rebound = self
            .inner
            .rebind_scan_for_resume(inner, committed_frontier)?;
        self.map_scan_scope(rebound, outer_request, self.descriptor.state_scope.clone())
    }

    fn open(&self, partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        self.inner.open(self.inner_partition(partition))
    }

    fn open_executable(
        &self,
        partition: cdf_kernel::ExecutablePartition,
    ) -> cdf_kernel::PartitionOpenAttempt<'_> {
        let inner_scope = self.inner_scope.clone();
        self.inner.open_executable(partition.map_plan(|mut plan| {
            plan.scope = inner_scope;
            plan
        }))
    }

    fn attest_partition(
        &self,
        partition: PartitionPlan,
    ) -> cdf_kernel::PartitionAttestationAttempt<'_> {
        self.inner.attest_partition(self.inner_partition(partition))
    }

    fn attest_executable(
        &self,
        partition: cdf_kernel::ExecutablePartition,
    ) -> cdf_kernel::PartitionAttestationAttempt<'_> {
        let inner_scope = self.inner_scope.clone();
        self.inner.attest_executable(partition.map_plan(|mut plan| {
            plan.scope = inner_scope;
            plan
        }))
    }

    fn effective_schema_runtime(&self) -> Option<&EffectiveSchemaRuntime> {
        self.inner.effective_schema_runtime()
    }

    fn type_policy_allowances(&self) -> cdf_kernel::TypePolicyAllowances {
        self.inner.type_policy_allowances()
    }

    fn replay_retention(&self) -> Option<&dyn cdf_kernel::SourceReplayRetention> {
        self.inner.replay_retention()
    }
}

impl QueryableResource for WindowScopedResource<'_> {
    fn capabilities(&self) -> &ResourceCapabilities {
        self.inner.capabilities()
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        let plan = self.inner.negotiate(&self.inner_request(request))?;
        self.map_scan_scope(plan, request.clone(), self.descriptor.state_scope.clone())
    }
}
