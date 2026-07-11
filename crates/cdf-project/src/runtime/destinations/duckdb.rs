use super::*;

pub struct DuckDbProjectDestinationDriver;

pub(crate) struct DuckDbProjectDestinationRuntime {
    destination: DuckDbDestination,
}

impl DuckDbProjectDestinationRuntime {
    pub(super) fn new(database_path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            destination: DuckDbDestination::new(database_path)?,
        })
    }

    #[cfg(test)]
    pub(crate) fn from_destination(destination: DuckDbDestination) -> Self {
        Self { destination }
    }
}

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
        Ok(Box::new(DuckDbProjectDestinationRuntime::new(
            database_path,
        )?))
    }
}

impl ProjectDestinationRuntime for DuckDbProjectDestinationRuntime {
    fn protocol(&self) -> &dyn DestinationProtocol {
        &self.destination
    }

    fn describe(&self) -> ProjectDestinationDescription {
        duckdb_project_description(&self.destination)
    }

    fn plan_resource_commit(
        &mut self,
        _resource: &dyn ResourceStream,
        output_schema: &Schema,
        inputs: &DestinationCommitPlanningInputs,
    ) -> Result<DestinationCommitPlanningOutcome> {
        let plan = self
            .destination
            .plan_schema_commit(&inputs.destination_commit, output_schema)?;
        Ok(DestinationCommitPlanningOutcome::new(
            self.destination.sheet().clone(),
            plan.kernel,
        ))
    }

    fn prepare_package_commit(
        &mut self,
        package_dir: &Path,
        _reader: &PackageReader,
        inputs: &PackageReplayInputs,
        _context: &DestinationPlanningContext<'_>,
    ) -> Result<PreparedDestinationCommit> {
        prepare_duckdb_package_commit(&self.destination, package_dir, inputs)
    }

    fn bind_prepared_commit(&mut self, prepared: &mut PreparedDestinationCommit) -> Result<()> {
        reject_unexpected_pending_context(prepared, "DuckDB")
    }
}

fn duckdb_project_description(destination: &DuckDbDestination) -> ProjectDestinationDescription {
    ProjectDestinationDescription::new(
        destination.sheet().destination.clone(),
        &["duckdb"],
        destination.database_path().display().to_string(),
    )
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
    let plan = if request.commit.segments.is_empty() {
        destination.plan_empty_package_commit(&request)?
    } else {
        destination.plan_package_commit(&request)?
    };
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
