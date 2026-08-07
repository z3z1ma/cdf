use cdf_declarative::CompiledResource;
use cdf_kernel::QueryableResource;
use cdf_project::{DefaultSecretProvider, EnvSecretProvider, FileSecretProvider, ProjectRunSource};
use std::{path::Path, sync::Arc};

use crate::{context::ProjectContext, output::CliError};

pub(crate) struct PreparedRuntimeResourceForCli {
    pub(crate) resource: CliProjectRunSource,
    pub(crate) compiled_resource: CompiledResource,
    pub(crate) schema_snapshot: Option<crate::reports::SchemaSnapshotActionReport>,
    pub(crate) schema_artifact_files: Vec<(String, Vec<u8>)>,
}

pub(crate) struct CliProjectRunSource {
    resource: Arc<dyn QueryableResource>,
    source_plan: cdf_runtime::CompiledSourcePlan,
    execution_extent: cdf_kernel::ExecutionExtent,
    relational_expression_plan: Option<cdf_contract::RelationalExpressionPlan>,
}

impl CliProjectRunSource {
    pub(crate) fn from_shared(
        resource: Arc<dyn QueryableResource>,
        source_plan: cdf_runtime::CompiledSourcePlan,
        execution_extent: cdf_kernel::ExecutionExtent,
        relational_expression_plan: Option<cdf_contract::RelationalExpressionPlan>,
    ) -> Self {
        Self {
            resource,
            source_plan,
            execution_extent,
            relational_expression_plan,
        }
    }

    pub(crate) fn as_project_resource(&self) -> ProjectRunSource<'_> {
        ProjectRunSource::new(self.resource.as_ref())
    }

    pub(crate) fn as_queryable(&self) -> &dyn QueryableResource {
        self.resource.as_ref()
    }

    pub(crate) fn source_plan(&self) -> &cdf_runtime::CompiledSourcePlan {
        &self.source_plan
    }

    pub(crate) fn execution_extent(&self) -> &cdf_kernel::ExecutionExtent {
        &self.execution_extent
    }

    pub(crate) fn relational_expression_plan(
        &self,
    ) -> Option<&cdf_contract::RelationalExpressionPlan> {
        self.relational_expression_plan.as_ref()
    }
}

pub(crate) fn build_project_run_resource_from_compilation(
    context: &crate::context::ProjectCompilationContext,
    resource: &CompiledResource,
    artifact_root: &Path,
    execution: &cdf_runtime::ExecutionServices,
) -> Result<CliProjectRunSource, CliError> {
    let source_plan = resource.source_plan().clone();
    let registry = crate::source_registry::builtin_source_registry()?;
    let source_schema = resource
        .relational_expression_plan()
        .map(|plan| plan.input_schema.to_arrow())
        .transpose()?
        .unwrap_or_else(|| resource.schema().as_ref().clone());
    source_plan.validate_schema_authority(
        &source_plan.descriptor,
        &source_schema,
        resource.effective_schema_runtime(),
        resource.baseline_observation_schema_catalog(),
    )?;
    let secrets = DefaultSecretProvider::new(
        EnvSecretProvider::process(),
        FileSecretProvider::new(context.root.clone()),
    );
    let resolution = cdf_runtime::SourceResolutionContext::new(
        &context.root,
        Arc::new(secrets),
        execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    )
    .with_artifact_root(artifact_root)
    .with_driver_options(context.compilation.config.driver_options.clone());
    Ok(CliProjectRunSource::from_shared(
        registry.resolve(&source_plan, &resolution)?,
        source_plan,
        resource.execution_extent().clone(),
        resource.relational_expression_plan().cloned(),
    ))
}

pub(crate) fn prepare_runtime_resource_for_cli(
    context: &ProjectContext,
    resource_id: &str,
    commit_schema: bool,
    execution: Option<&cdf_runtime::ExecutionServices>,
) -> Result<PreparedRuntimeResourceForCli, CliError> {
    let artifact_root = context.root.clone();
    prepare_runtime_resource_for_cli_with_artifact_root(
        context,
        resource_id,
        commit_schema,
        execution,
        &artifact_root,
    )
}

pub(crate) fn prepare_runtime_resource_for_cli_with_artifact_root(
    context: &ProjectContext,
    resource_id: &str,
    commit_schema: bool,
    execution: Option<&cdf_runtime::ExecutionServices>,
    artifact_root: &Path,
) -> Result<PreparedRuntimeResourceForCli, CliError> {
    let compiled = context.resource(resource_id)?;
    let prepared = crate::scan_command::prepare_resource_schema_for_cli(
        context,
        compiled,
        commit_schema,
        execution,
        artifact_root,
    )?;
    let runtime = build_project_run_resource_with_artifact_root(
        context,
        &prepared.resource,
        prepared.source_plan.clone(),
        execution,
        prepared.prepared_payloads.clone(),
        artifact_root,
    )?;
    Ok(PreparedRuntimeResourceForCli {
        resource: runtime,
        compiled_resource: prepared.resource,
        schema_snapshot: prepared.schema_snapshot,
        schema_artifact_files: prepared.schema_artifact_files,
    })
}

fn build_project_run_resource_with_artifact_root(
    context: &ProjectContext,
    resource: &CompiledResource,
    source_plan: cdf_runtime::CompiledSourcePlan,
    execution: Option<&cdf_runtime::ExecutionServices>,
    prepared_payloads: cdf_runtime::PreparedSourcePayloads,
    artifact_root: &Path,
) -> Result<CliProjectRunSource, CliError> {
    let execution = execution.ok_or_else(|| {
        cdf_kernel::CdfError::internal("runtime source resolution requires execution services")
    })?;
    let registry = crate::source_registry::builtin_source_registry()?;
    let source_schema = resource
        .relational_expression_plan()
        .map(|plan| plan.input_schema.to_arrow())
        .transpose()?
        .unwrap_or_else(|| resource.schema().as_ref().clone());
    source_plan.validate_schema_authority(
        &source_plan.descriptor,
        &source_schema,
        resource.effective_schema_runtime(),
        resource.baseline_observation_schema_catalog(),
    )?;
    let secrets = context.secret_provider();
    let resolution = cdf_runtime::SourceResolutionContext::new(
        &context.root,
        Arc::new(secrets),
        execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    )
    .with_artifact_root(artifact_root)
    .with_prepared_payloads(prepared_payloads)
    .with_driver_options(context.config.driver_options.clone());
    Ok(CliProjectRunSource::from_shared(
        registry.resolve(&source_plan, &resolution)?,
        source_plan,
        resource.execution_extent().clone(),
        resource.relational_expression_plan().cloned(),
    ))
}

pub(crate) fn compile_source_plan_for_cli(
    resource: &CompiledResource,
) -> cdf_kernel::Result<cdf_runtime::CompiledSourcePlan> {
    resource.source_plan().validate()?;
    Ok(resource.source_plan().clone())
}

pub(crate) fn discover_source_schema_with_plan_for_cli_at(
    context: &ProjectContext,
    resource: &CompiledResource,
    source_plan: &cdf_runtime::CompiledSourcePlan,
    execution: &cdf_runtime::ExecutionServices,
    prepared_payloads: cdf_runtime::PreparedSourcePayloads,
    options: cdf_project::SchemaDiscoveryExecutionOptions,
    artifact_root: &Path,
) -> cdf_kernel::Result<cdf_project::ResourceSchemaDiscoveryArtifacts> {
    let registry = crate::source_registry::builtin_source_registry()?;
    let cancellation = options.cancellation();
    let resolution = cdf_runtime::SourceResolutionContext::new(
        &context.root,
        Arc::new(context.secret_provider()),
        execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    )
    .with_artifact_root(artifact_root)
    .with_cancellation(cancellation)
    .with_prepared_payloads(prepared_payloads)
    .with_driver_options(context.config.driver_options.clone());
    cdf_project::discover_resource_schema_with_source_registry(
        resource,
        registry,
        source_plan,
        &resolution,
        options,
    )
}
