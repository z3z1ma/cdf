use std::{path::Path, sync::Arc};

use cdf_dest_postgres::{MergeDedupPolicy, PostgresExistingTable, PostgresTarget};
use cdf_kernel::{Result, ScopeLeaseStore, TargetName};

use crate::ResolvedProjectDestination;

pub(crate) fn duckdb(
    database_path: impl AsRef<Path>,
    target: TargetName,
) -> Result<ResolvedProjectDestination> {
    let services = execution_services()?;
    ResolvedProjectDestination::new(
        Box::new(cdf_dest_duckdb::DuckDbDestination::new(database_path)?),
        target,
    )
    .with_bound_execution_services(services)
}

pub(crate) fn parquet_filesystem(
    root: impl AsRef<Path>,
    target: TargetName,
) -> Result<ResolvedProjectDestination> {
    let services = execution_services()?.with_content_reachability_store(Arc::new(
        cdf_state_sqlite::SqliteContentReachabilityStore::open_in_memory()?,
    ));
    ResolvedProjectDestination::new(
        Box::new(
            cdf_dest_parquet::FilesystemParquetRuntime::with_execution_services(
                root.as_ref().to_path_buf(),
                services.clone(),
            ),
        ),
        target,
    )
    .with_bound_execution_services(services)
}

pub(crate) fn postgres(
    database_url: impl Into<String>,
    target: PostgresTarget,
    dedup: MergeDedupPolicy,
    existing_table: Option<PostgresExistingTable>,
) -> Result<ResolvedProjectDestination> {
    let target_name = TargetName::new(target.display_name())?;
    let destination = cdf_dest_postgres::PostgresDestination::connect(database_url)?;
    Ok(ResolvedProjectDestination::new(
        Box::new(cdf_dest_postgres::PostgresRuntime::for_replay(
            &destination,
            target,
            dedup,
            existing_table,
        )),
        target_name,
    ))
}

fn execution_services() -> Result<cdf_runtime::ExecutionServices> {
    let (_, base_services) =
        cdf_engine::StandaloneExecutionHost::default_services(64 * 1024 * 1024)?;
    let scopes: Arc<dyn ScopeLeaseStore> =
        Arc::new(cdf_state_sqlite::InMemoryScopeLeaseStore::new());
    base_services.with_staging_lease_authority(Arc::new(
        cdf_runtime::ScopeStagingLeaseAuthority::new(scopes),
    ))
}
