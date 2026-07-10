use super::*;

pub struct ParquetProjectDestinationDriver;

impl ProjectDestinationDriver for ParquetProjectDestinationDriver {
    fn schemes(&self) -> &'static [&'static str] {
        &["parquet"]
    }

    fn resolve(
        &self,
        uri: &str,
        context: &ProjectResolutionContext<'_>,
    ) -> Result<Box<dyn ProjectDestinationRuntime>> {
        let root = absolute_under_root(context.project_root()?, local_uri_path(uri, "parquet")?);
        Ok(Box::new(FilesystemParquetProjectDestinationRuntime::new(
            root,
        )))
    }
}

impl ProjectDestinationRuntime for ParquetDestination {
    fn protocol(&self) -> &dyn DestinationProtocol {
        self
    }

    fn describe(&self) -> ProjectDestinationDescription {
        parquet_project_description(self)
    }

    fn prepare_package_commit(
        &mut self,
        package_dir: &Path,
        _reader: &PackageReader,
        inputs: &PackageReplayInputs,
        _context: &DestinationPlanningContext<'_>,
    ) -> Result<PreparedDestinationCommit> {
        prepare_parquet_package_commit(self, package_dir, inputs)
    }

    fn bind_prepared_commit(&mut self, prepared: &mut PreparedDestinationCommit) -> Result<()> {
        reject_unexpected_pending_context(prepared, "Parquet")
    }
}

fn parquet_project_description(destination: &ParquetDestination) -> ProjectDestinationDescription {
    ProjectDestinationDescription {
        destination_id: destination.sheet().destination.clone(),
        schemes: &["parquet"],
        label: "parquet filesystem".to_owned(),
    }
}

fn prepare_parquet_package_commit(
    destination: &ParquetDestination,
    package_dir: &Path,
    inputs: &PackageReplayInputs,
) -> Result<PreparedDestinationCommit> {
    let request = ParquetCommitRequest {
        package_dir: package_dir.to_path_buf(),
        commit: inputs.destination_commit.clone(),
        schema_hash: inputs.schema_hash.clone(),
    };
    let plan = destination.plan_package_commit(&request)?;
    Ok(PreparedDestinationCommit::new(
        request.commit,
        plan.kernel,
        DestinationReceiptReportingPolicy::DestinationCommit {
            duplicate: plan.duplicate,
        },
    ))
}

pub(crate) struct FilesystemParquetProjectDestinationRuntime {
    destination: Option<ParquetDestination>,
    root: PathBuf,
}

impl FilesystemParquetProjectDestinationRuntime {
    pub(crate) fn new(root: PathBuf) -> Self {
        Self {
            destination: None,
            root,
        }
    }

    fn destination(&mut self) -> Result<&ParquetDestination> {
        if self.destination.is_none() {
            self.destination = Some(ParquetDestination::new_filesystem(&self.root)?);
        }
        Ok(self.destination.as_ref().expect("destination was just set"))
    }
}

impl ProjectDestinationRuntime for FilesystemParquetProjectDestinationRuntime {
    fn protocol(&self) -> &dyn DestinationProtocol {
        self.destination
            .as_ref()
            .expect("filesystem Parquet destination must be materialized before protocol use")
    }

    fn describe(&self) -> ProjectDestinationDescription {
        ProjectDestinationDescription {
            destination_id: DestinationId::new("parquet_object_store")
                .expect("static destination id"),
            schemes: &["parquet"],
            label: self.root.display().to_string(),
        }
    }

    fn destination_sheet(&self) -> Result<DestinationSheet> {
        ParquetDestination::destination_sheet()
    }

    fn supported_dispositions(&self) -> &[WriteDisposition] {
        static SUPPORTED: [WriteDisposition; 2] =
            [WriteDisposition::Append, WriteDisposition::Replace];
        &SUPPORTED
    }

    fn quarantine_table_support(&self) -> CapabilitySupport {
        CapabilitySupport::Unsupported
    }

    fn plan_resource_commit(
        &mut self,
        _resource: &dyn ResourceStream,
        _output_schema: &Schema,
        inputs: &DestinationCommitPlanningInputs,
    ) -> Result<DestinationCommitPlanningOutcome> {
        let (sheet, plan) = ParquetDestination::dry_plan_commit(&inputs.destination_commit)?;
        Ok(DestinationCommitPlanningOutcome::new(sheet, plan))
    }

    fn prepare_package_commit(
        &mut self,
        package_dir: &Path,
        _reader: &PackageReader,
        inputs: &PackageReplayInputs,
        _context: &DestinationPlanningContext<'_>,
    ) -> Result<PreparedDestinationCommit> {
        let destination = self.destination()?;
        prepare_parquet_package_commit(destination, package_dir, inputs)
    }

    fn bind_prepared_commit(&mut self, prepared: &mut PreparedDestinationCommit) -> Result<()> {
        reject_unexpected_pending_context(prepared, "Parquet")
    }

    fn ensure_protocol_ready(&mut self) -> Result<()> {
        self.destination().map(|_| ())
    }
}
