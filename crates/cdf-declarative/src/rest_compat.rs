use cdf_kernel::{CdfError, ResourceStream, Result, ScanRequest};
use cdf_source_rest::{
    RestDiscoveryDependencies, RestResource, RestRuntimeDependencies, RestSampleSchemaDiscovery,
    discover_rest_sample_schema as discover_source_rest_sample_schema,
};

use crate::{CompiledResource, CompiledResourcePlan};

impl CompiledResource {
    pub fn into_rest_resource(self, dependencies: RestRuntimeDependencies) -> Result<RestResource> {
        build_rest_resource(&self, dependencies)
    }

    pub fn to_rest_resource(&self, dependencies: RestRuntimeDependencies) -> Result<RestResource> {
        build_rest_resource(self, dependencies)
    }
}

fn build_rest_resource(
    resource: &CompiledResource,
    dependencies: RestRuntimeDependencies,
) -> Result<RestResource> {
    let CompiledResourcePlan::Rest(plan) = resource.plan() else {
        return Err(CdfError::contract(
            "only compiled REST resources can be opened with REST runtime dependencies",
        ));
    };
    RestResource::new(
        resource.descriptor().clone(),
        resource.schema(),
        resource.capabilities().clone(),
        (**plan).clone(),
        resource.type_policy_allowances(),
        dependencies,
    )
}

pub fn discover_rest_sample_schema(
    resource: &CompiledResource,
    dependencies: &RestDiscoveryDependencies<'_>,
) -> Result<RestSampleSchemaDiscovery> {
    let CompiledResourcePlan::Rest(plan) = resource.plan() else {
        return Err(CdfError::contract(
            "only compiled REST resources can be sampled for REST schema discovery",
        ));
    };
    let partition = resource
        .plan_partitions(&ScanRequest {
            resource_id: resource.descriptor().resource_id.clone(),
            projection: None,
            filters: Vec::new(),
            limit: None,
            order_by: Vec::new(),
            scope: resource.descriptor().state_scope.clone(),
        })?
        .into_iter()
        .next()
        .ok_or_else(|| {
            CdfError::contract(format!(
                "REST discovery for resource `{}` expected one REST partition",
                resource.descriptor().resource_id
            ))
        })?;
    discover_source_rest_sample_schema(resource.descriptor(), plan, &partition, dependencies)
}
