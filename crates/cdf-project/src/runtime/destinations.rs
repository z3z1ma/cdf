use super::{prelude::*, types::ProjectReceiptSource};
use cdf_contract::{IdentifierPolicy, identifier_policy_from_destination_rules};
use cdf_kernel::{CapabilitySupport, DestinationSheet};

mod duckdb;
mod parquet;
mod postgres;

pub use duckdb::DuckDbProjectDestinationDriver;
pub(crate) use duckdb::DuckDbProjectDestinationRuntime;
pub(super) use parquet::FilesystemParquetProjectDestinationRuntime;
pub use parquet::ParquetProjectDestinationDriver;
pub use postgres::PostgresProjectDestinationDriver;
pub(super) use postgres::PostgresProjectDestinationRuntime;

pub use cdf_runtime::{
    DestinationCommitPlanningInputs, DestinationCommitPlanningOutcome,
    DestinationDescription as ProjectDestinationDescription,
    DestinationDriver as ProjectDestinationDriver, DestinationOutputSchema,
    DestinationPlanningContext, DestinationReceiptReportingPolicy,
    DestinationRegistry as ProjectDestinationRegistry,
    DestinationResolutionContext as ProjectResolutionContext,
    DestinationRuntime as ProjectDestinationRuntime, PreparedDestinationCommit,
    absolute_under_root, commit_request, local_uri_path, reject_unexpected_pending_context,
};

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
        Self { target, runtime }
    }

    pub fn duckdb(database_path: impl AsRef<Path>, target: TargetName) -> Result<Self> {
        Ok(Self::new(
            Box::new(DuckDbProjectDestinationRuntime::new(database_path)?),
            target,
        ))
    }

    pub fn parquet_filesystem(root: impl AsRef<Path>, target: TargetName) -> Result<Self> {
        Ok(Self::new(
            Box::new(FilesystemParquetProjectDestinationRuntime::new(
                root.as_ref().to_path_buf(),
            )),
            target,
        ))
    }

    pub fn postgres(
        database_url: impl Into<String>,
        target: PostgresTarget,
        dedup: MergeDedupPolicy,
        existing_table: Option<PostgresExistingTable>,
    ) -> Result<Self> {
        let target_name = TargetName::new(target.display_name())?;
        let destination = PostgresDestination::connect(database_url)?;
        Ok(Self::new(
            Box::new(PostgresProjectDestinationRuntime::for_replay(
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

    pub fn output_schema(&self, plan: &EnginePlan) -> Result<DestinationOutputSchema> {
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
        let schema_hash = plan.effective_schema_hash()?.clone();
        Ok(DestinationOutputSchema {
            schema,
            schema_hash,
            identifier_policy,
        })
    }

    pub fn describe(&self) -> ProjectDestinationDescription {
        self.runtime.describe()
    }

    pub fn secret_redaction(&self) -> Option<&str> {
        self.runtime.secret_redaction()
    }

    pub(super) fn runtime_mut(&mut self) -> &mut dyn ProjectDestinationRuntime {
        self.runtime.as_mut()
    }
}

pub fn resolve_project_run_destination(
    uri: &str,
    context: &ProjectResolutionContext<'_>,
) -> Result<ResolvedProjectDestination> {
    let mut registry = ProjectDestinationRegistry::new();
    registry.register(DuckDbProjectDestinationDriver)?;
    registry.register(ParquetProjectDestinationDriver)?;
    registry.register(PostgresProjectDestinationDriver)?;
    let runtime = registry.resolve(uri, context)?;
    Ok(ResolvedProjectDestination::new(
        runtime,
        context.target()?.clone(),
    ))
}
