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

    #[cfg(test)]
    pub fn duckdb(database_path: impl AsRef<Path>, target: TargetName) -> Result<Self> {
        let (_, base_services) =
            cdf_engine::StandaloneExecutionHost::default_services(64 * 1024 * 1024)?;
        let scopes: std::sync::Arc<dyn cdf_kernel::ScopeLeaseStore> =
            std::sync::Arc::new(cdf_state_sqlite::InMemoryScopeLeaseStore::new());
        let services = base_services.with_staging_lease_authority(std::sync::Arc::new(
            cdf_runtime::ScopeStagingLeaseAuthority::new(scopes),
        ))?;
        Ok(Self::new(
            Box::new(cdf_dest_duckdb::DuckDbDestination::new(database_path)?),
            target,
        )
        .with_execution_services(services))
    }

    #[cfg(test)]
    pub fn parquet_filesystem(root: impl AsRef<Path>, target: TargetName) -> Result<Self> {
        let (_, base_services) =
            cdf_engine::StandaloneExecutionHost::default_services(64 * 1024 * 1024)?;
        let scopes: std::sync::Arc<dyn cdf_kernel::ScopeLeaseStore> =
            std::sync::Arc::new(cdf_state_sqlite::InMemoryScopeLeaseStore::new());
        let services = base_services.with_staging_lease_authority(std::sync::Arc::new(
            cdf_runtime::ScopeStagingLeaseAuthority::new(scopes),
        ))?;
        Ok(Self::new(
            Box::new(
                cdf_dest_parquet::FilesystemParquetRuntime::with_execution_services(
                    root.as_ref().to_path_buf(),
                    services.clone(),
                ),
            ),
            target,
        )
        .with_execution_services(services))
    }

    #[cfg(test)]
    pub fn postgres(
        database_url: impl Into<String>,
        target: cdf_dest_postgres::PostgresTarget,
        dedup: cdf_dest_postgres::MergeDedupPolicy,
        existing_table: Option<cdf_dest_postgres::PostgresExistingTable>,
    ) -> Result<Self> {
        let target_name = TargetName::new(target.display_name())?;
        let destination = cdf_dest_postgres::PostgresDestination::connect(database_url)?;
        Ok(Self::new(
            Box::new(cdf_dest_postgres::PostgresRuntime::for_replay(
                &destination,
                target,
                dedup,
                existing_table,
            )),
            target_name,
        ))
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

    pub(super) fn runtime_mut(&mut self) -> &mut dyn ProjectDestinationRuntime {
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
    let destination = ResolvedProjectDestination::new(runtime, context.target()?.clone());
    Ok(match context.execution_services() {
        Some(execution) => destination.with_execution_services(execution.clone()),
        None => destination,
    })
}
