use super::{hooks::ReceiptVerifiedHook, prelude::*, types::ProjectReceiptSource};

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ProjectDestinationDescription {
    pub destination_id: DestinationId,
    pub schemes: &'static [&'static str],
    pub label: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DestinationReceiptReportingPolicy {
    DestinationCommit { duplicate: bool },
    DestinationCommitReceiptOnly,
}

impl DestinationReceiptReportingPolicy {
    pub fn into_project_receipt_source(
        self,
        package_receipt_recorded: bool,
    ) -> ProjectReceiptSource {
        match self {
            Self::DestinationCommit { duplicate } => ProjectReceiptSource::DestinationCommit {
                duplicate,
                package_receipt_recorded,
            },
            Self::DestinationCommitReceiptOnly => {
                ProjectReceiptSource::DestinationCommitReceiptOnly {
                    package_receipt_recorded,
                }
            }
        }
    }
}

pub struct PreparedDestinationCommit {
    pub commit: DestinationCommitRequest,
    pub plan: cdf_kernel::CommitPlan,
    pub reporting_policy: DestinationReceiptReportingPolicy,
    pub pending_context: Option<Box<dyn Any + Send + Sync>>,
}

impl PreparedDestinationCommit {
    pub fn new(
        commit: DestinationCommitRequest,
        plan: cdf_kernel::CommitPlan,
        reporting_policy: DestinationReceiptReportingPolicy,
    ) -> Self {
        Self {
            commit,
            plan,
            reporting_policy,
            pending_context: None,
        }
    }

    pub fn with_pending_context(
        mut self,
        pending_context: impl Any + Send + Sync + 'static,
    ) -> Self {
        self.pending_context = Some(Box::new(pending_context));
        self
    }

    pub fn take_pending_context<T>(&mut self, label: &str) -> Result<T>
    where
        T: Any + Send + Sync + 'static,
    {
        let pending = self.pending_context.take().ok_or_else(|| {
            CdfError::internal(format!(
                "prepared destination commit is missing {label} pending context"
            ))
        })?;
        pending.downcast::<T>().map(|boxed| *boxed).map_err(|_| {
            CdfError::internal(format!(
                "prepared destination commit pending context did not match {label}"
            ))
        })
    }

    pub fn has_pending_context(&self) -> bool {
        self.pending_context.is_some()
    }
}

#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct ProjectResolutionContext<'a> {
    marker: PhantomData<&'a ()>,
}

impl<'a> ProjectResolutionContext<'a> {
    pub fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl Default for ProjectResolutionContext<'_> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy)]
#[non_exhaustive]
pub struct DestinationPlanningContext<'a> {
    pub after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
}

impl<'a> DestinationPlanningContext<'a> {
    pub fn new() -> Self {
        Self {
            after_receipt_verified: None,
        }
    }
}

impl Default for DestinationPlanningContext<'_> {
    fn default() -> Self {
        Self::new()
    }
}

pub trait ProjectDestinationDriver {
    fn schemes(&self) -> &'static [&'static str];

    fn resolve(
        &self,
        uri: &str,
        context: &ProjectResolutionContext<'_>,
    ) -> Result<Box<dyn ProjectDestinationRuntime>>;
}

pub trait ProjectDestinationRuntime {
    fn protocol(&self) -> &dyn DestinationProtocol;

    fn describe(&self) -> ProjectDestinationDescription;

    fn prepare_package_commit(
        &mut self,
        package_dir: &Path,
        reader: &PackageReader,
        inputs: &PackageReplayInputs,
        context: &DestinationPlanningContext<'_>,
    ) -> Result<PreparedDestinationCommit>;

    fn bind_prepared_commit(&mut self, prepared: &mut PreparedDestinationCommit) -> Result<()>;
}

pub(super) fn postgres_load_plan_input(
    request: &ProjectRunRequest<'_>,
    inputs: &PackageReplayInputs,
    columns: Vec<PostgresColumn>,
) -> Result<PostgresLoadPlanInput> {
    let ProjectRunDestination::Postgres {
        target,
        dedup,
        existing_table,
        ..
    } = &request.destination
    else {
        return Err(CdfError::internal(
            "postgres load plan requested for non-Postgres project destination",
        ));
    };
    let descriptor = request.resource.descriptor();
    Ok(PostgresLoadPlanInput {
        package_hash: inputs.state_delta.package_hash.clone(),
        idempotency_token: inputs.destination_commit.idempotency_token.clone(),
        target: target.clone(),
        disposition: descriptor.write_disposition.clone(),
        schema_hash: inputs.schema_hash.clone(),
        segments: inputs.state_delta.segments.clone(),
        columns,
        merge_keys: postgres_merge_keys(descriptor)?,
        dedup: dedup.clone(),
        existing_table: existing_table.clone(),
        resource_id: Some(descriptor.resource_id.clone()),
        state_delta: Some(inputs.state_delta.clone()),
    })
}

pub(super) fn postgres_load_plan_input_from_artifacts(
    inputs: &PackageReplayInputs,
    target: PostgresTarget,
    dedup: MergeDedupPolicy,
    existing_table: Option<PostgresExistingTable>,
    columns: Vec<PostgresColumn>,
) -> Result<PostgresLoadPlanInput> {
    validate_postgres_replay_target(&target, &inputs.destination_commit.target)?;
    Ok(PostgresLoadPlanInput {
        package_hash: inputs.state_delta.package_hash.clone(),
        idempotency_token: inputs.destination_commit.idempotency_token.clone(),
        target,
        disposition: inputs.destination_commit.disposition.clone(),
        schema_hash: inputs.schema_hash.clone(),
        segments: inputs.state_delta.segments.clone(),
        columns,
        merge_keys: postgres_merge_keys_from_artifacts(&inputs.merge_keys)?,
        dedup,
        existing_table,
        resource_id: Some(inputs.state_delta.resource_id.clone()),
        state_delta: Some(inputs.state_delta.clone()),
    })
}

pub(super) fn validate_postgres_replay_target(
    target: &PostgresTarget,
    package_target: &TargetName,
) -> Result<()> {
    let explicit = target.display_name();
    if explicit != package_target.as_str() {
        return Err(CdfError::contract(format!(
            "explicit Postgres replay target {explicit} does not match package destination commit target {package_target}"
        )));
    }
    Ok(())
}

pub(super) fn postgres_merge_keys(
    descriptor: &ResourceDescriptor,
) -> Result<Vec<PostgresIdentifier>> {
    if descriptor.write_disposition != WriteDisposition::Merge {
        return Ok(Vec::new());
    }
    descriptor
        .merge_key
        .iter()
        .map(PostgresIdentifier::user)
        .collect()
}

pub(super) fn postgres_merge_keys_from_artifacts(
    keys: &[String],
) -> Result<Vec<PostgresIdentifier>> {
    keys.iter().map(PostgresIdentifier::user).collect()
}

pub(super) fn postgres_columns_from_schema(
    resource: &dyn ResourceStream,
) -> Result<Vec<PostgresColumn>> {
    postgres_columns_for_schema(resource.schema().as_ref())
}

pub(super) fn postgres_columns_from_package(reader: &PackageReader) -> Result<Vec<PostgresColumn>> {
    let segments = reader.read_all_segments()?;
    let schema = segments
        .iter()
        .flat_map(|(_, batches)| batches.iter())
        .next()
        .map(|batch| batch.schema())
        .ok_or_else(|| {
            CdfError::data("Postgres destination requires at least one package batch")
        })?;
    postgres_columns_for_schema(schema.as_ref())
}

pub(super) fn postgres_target(request: &ProjectRunRequest<'_>) -> Result<TargetName> {
    match &request.destination {
        ProjectRunDestination::Postgres { target, .. } => TargetName::new(target.display_name()),
        _ => Err(CdfError::internal(
            "postgres target requested for non-Postgres project destination",
        )),
    }
}

pub(super) fn commit_request(
    delta: &StateDelta,
    target: TargetName,
    disposition: WriteDisposition,
) -> Result<DestinationCommitRequest> {
    Ok(DestinationCommitRequest {
        package_hash: delta.package_hash.clone(),
        target,
        disposition,
        segments: delta.segments.clone(),
        idempotency_token: IdempotencyToken::new(delta.package_hash.as_str())?,
    })
}

use super::types::{ProjectRunDestination, ProjectRunRequest};
