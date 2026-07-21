use super::{prelude::*, types::ProjectReceiptSource};
use cdf_contract::{IdentifierPolicy, identifier_policy_from_destination_rules};

pub use cdf_runtime::{
    DestinationCommitPlanningInputs, DestinationCommitPlanningOutcome,
    DestinationDescription as ProjectDestinationDescription,
    DestinationDriver as ProjectDestinationDriver, DestinationPlanningContext,
    DestinationReceiptReportingPolicy, DestinationRegistry as ProjectDestinationRegistry,
    DestinationResolutionContext as ProjectResolutionContext,
    DestinationRuntime as ProjectDestinationRuntime, PreparedDestinationCommit,
    absolute_under_root, commit_request, local_uri_path,
};

pub(super) struct DestinationOutputSchema {
    pub(super) schema: arrow_schema::SchemaRef,
    pub(super) schema_hash: SchemaHash,
}

pub(super) fn project_receipt_source(
    policy: DestinationReceiptReportingPolicy,
    package_receipt_recorded: bool,
) -> ProjectReceiptSource {
    match policy {
        DestinationReceiptReportingPolicy::DestinationCommit { duplicate } => {
            ProjectReceiptSource::DestinationCommit {
                duplicate,
                package_receipt_recorded,
            }
        }
        DestinationReceiptReportingPolicy::DestinationCommitReceiptOnly => {
            ProjectReceiptSource::DestinationCommitReceiptOnly {
                package_receipt_recorded,
            }
        }
    }
}

pub struct ResolvedProjectDestination {
    target: TargetName,
    runtime: Box<dyn ProjectDestinationRuntime>,
    execution: Option<cdf_runtime::ExecutionServices>,
}

impl std::fmt::Debug for ResolvedProjectDestination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedProjectDestination")
            .field("target", &self.target)
            .field("description", &self.describe())
            .finish_non_exhaustive()
    }
}

impl ResolvedProjectDestination {
    pub fn new(runtime: Box<dyn ProjectDestinationRuntime>, target: TargetName) -> Self {
        Self {
            target,
            runtime,
            execution: None,
        }
    }

    pub fn with_execution_services(mut self, execution: cdf_runtime::ExecutionServices) -> Self {
        self.execution = Some(execution);
        self
    }

    pub fn bind_execution_services(
        &mut self,
        execution: cdf_runtime::ExecutionServices,
    ) -> Result<()> {
        self.runtime.bind_execution_services(&execution)?;
        self.execution = Some(execution);
        Ok(())
    }

    pub fn with_bound_execution_services(
        mut self,
        execution: cdf_runtime::ExecutionServices,
    ) -> Result<Self> {
        self.bind_execution_services(execution)?;
        Ok(self)
    }

    pub fn target(&self) -> &TargetName {
        &self.target
    }

    pub fn column_identifier_policy(&self) -> Result<Option<IdentifierPolicy>> {
        let sheet = self.runtime.destination_sheet()?;
        identifier_policy_from_destination_rules(&sheet.identifier_rules).map(Some)
    }

    pub(super) fn output_schema(&self, plan: &EnginePlan) -> Result<DestinationOutputSchema> {
        let identifier_policy = self.column_identifier_policy()?;
        let schema = plan.output_arrow_schema()?;
        if let Some(identifier_policy) = &identifier_policy
            && plan.validation_program.identifier_policy != *identifier_policy
        {
            return Err(CdfError::contract(format!(
                "run plan identifier policy does not match resolved destination sheet: planned {:?}, destination {:?}; rebuild the plan for the selected destination",
                plan.validation_program.identifier_policy, identifier_policy
            )));
        }
        let schema_hash = plan.effective_schema_hash().clone();
        Ok(DestinationOutputSchema {
            schema,
            schema_hash,
        })
    }

    pub fn describe(&self) -> ProjectDestinationDescription {
        self.runtime.describe()
    }

    pub fn secret_redaction(&self) -> Option<&str> {
        self.runtime.secret_redaction()
    }

    pub fn runtime_capabilities(&self) -> cdf_runtime::DestinationRuntimeCapabilities {
        self.runtime.runtime_capabilities()
    }

    pub(crate) fn runtime_mut(&mut self) -> &mut dyn ProjectDestinationRuntime {
        self.runtime.as_mut()
    }

    pub(super) fn execution_services(&self) -> Option<&cdf_runtime::ExecutionServices> {
        self.execution.as_ref()
    }
}

pub fn resolve_project_run_destination(
    registry: &ProjectDestinationRegistry,
    uri: &str,
    context: &ProjectResolutionContext<'_>,
) -> Result<ResolvedProjectDestination> {
    let runtime = registry.resolve(uri, context).map_err(|mut error| {
        if error
            .message
            .starts_with("no destination driver registered for URI scheme")
        {
            error.message = error.message.replacen(
                "no destination driver registered",
                "no project destination driver registered",
                1,
            );
        }
        error
    })?;
    let mut destination = ResolvedProjectDestination::new(runtime, context.target()?.clone());
    if let Some(execution) = context.execution_services() {
        destination.bind_execution_services(execution.clone())?;
    }
    Ok(destination)
}
