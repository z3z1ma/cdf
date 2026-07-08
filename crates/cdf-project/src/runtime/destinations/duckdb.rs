use super::*;

pub struct DuckDbProjectDestinationDriver;

impl ProjectDestinationDriver for DuckDbProjectDestinationDriver {
    fn schemes(&self) -> &'static [&'static str] {
        &["duckdb"]
    }

    fn resolve(
        &self,
        uri: &str,
        context: &ProjectResolutionContext<'_>,
    ) -> Result<Box<dyn ProjectDestinationRuntime>> {
        let database_path =
            absolute_under_root(context.project_root()?, local_uri_path(uri, "duckdb")?);
        Ok(Box::new(DuckDbDestination::new(database_path)?))
    }
}

impl ProjectDestinationRuntime for DuckDbDestination {
    fn protocol(&self) -> &dyn DestinationProtocol {
        self
    }

    fn describe(&self) -> ProjectDestinationDescription {
        duckdb_project_description(self)
    }

    fn prepare_package_commit(
        &mut self,
        package_dir: &Path,
        _reader: &PackageReader,
        inputs: &PackageReplayInputs,
        _context: &DestinationPlanningContext<'_>,
    ) -> Result<PreparedDestinationCommit> {
        prepare_duckdb_package_commit(self, package_dir, inputs)
    }

    fn bind_prepared_commit(&mut self, prepared: &mut PreparedDestinationCommit) -> Result<()> {
        reject_unexpected_pending_context(prepared, "DuckDB")
    }
}

fn duckdb_project_description(destination: &DuckDbDestination) -> ProjectDestinationDescription {
    ProjectDestinationDescription {
        destination_id: destination.sheet().destination.clone(),
        schemes: &["duckdb"],
        label: destination.database_path().display().to_string(),
    }
}

fn prepare_duckdb_package_commit(
    destination: &DuckDbDestination,
    package_dir: &Path,
    inputs: &PackageReplayInputs,
) -> Result<PreparedDestinationCommit> {
    let request = DuckDbCommitRequest {
        package_dir: package_dir.to_path_buf(),
        commit: inputs.destination_commit.clone(),
        schema_hash: inputs.schema_hash.clone(),
        merge_keys: inputs.merge_keys.clone(),
    };
    let duplicate = duckdb_has_duplicate_receipt(destination, &request.commit)?;
    let plan = destination.plan_package_commit(&request)?;
    Ok(PreparedDestinationCommit::new(
        request.commit,
        plan.kernel,
        DestinationReceiptReportingPolicy::DestinationCommit { duplicate },
    ))
}

fn duckdb_has_duplicate_receipt(
    destination: &DuckDbDestination,
    request: &DestinationCommitRequest,
) -> Result<bool> {
    if !destination.database_path().exists() {
        return Ok(false);
    }
    let snapshot = destination.read_mirror_snapshot_read_only()?;
    for load in snapshot.loads {
        if load.target == request.target.as_str()
            && load.idempotency_token == request.idempotency_token.as_str()
            && load.package_hash == request.package_hash.as_str()
        {
            return Ok(true);
        }
    }
    Ok(false)
}
