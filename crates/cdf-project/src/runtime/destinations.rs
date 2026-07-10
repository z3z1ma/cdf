use super::{hooks::ReceiptVerifiedHook, prelude::*, types::ProjectReceiptSource};
use crate::DestinationPolicy;
use cdf_contract::{IdentifierPolicy, identifier_policy_from_destination_rules};
use cdf_kernel::{CapabilitySupport, CommitPlan, DestinationSheet};

mod duckdb;
mod parquet;
mod postgres;

pub use duckdb::DuckDbProjectDestinationDriver;
pub(super) use parquet::FilesystemParquetProjectDestinationRuntime;
pub use parquet::ParquetProjectDestinationDriver;
pub use postgres::PostgresProjectDestinationDriver;
pub(super) use postgres::PostgresProjectDestinationRuntime;

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DestinationCommitPlanningInputs {
    pub state_delta: StateDelta,
    pub destination_commit: DestinationCommitRequest,
    pub schema_hash: SchemaHash,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DestinationCommitPlanningOutcome {
    pub sheet: DestinationSheet,
    pub plan: CommitPlan,
}

#[derive(Clone, Debug)]
pub struct DestinationOutputSchema {
    pub schema: arrow_schema::SchemaRef,
    pub schema_hash: SchemaHash,
    pub identifier_policy: Option<IdentifierPolicy>,
}

impl DestinationCommitPlanningOutcome {
    pub fn new(sheet: DestinationSheet, plan: CommitPlan) -> Self {
        Self { sheet, plan }
    }
}

type ProjectSecretProvider = dyn SecretProvider + Send + Sync + std::panic::RefUnwindSafe;

#[derive(Clone, Copy)]
#[non_exhaustive]
pub struct ProjectResolutionContext<'a> {
    project_root: Option<&'a Path>,
    target: Option<&'a TargetName>,
    environment_name: Option<&'a str>,
    destination_policy: Option<&'a DestinationPolicy>,
    secret_provider: Option<&'a ProjectSecretProvider>,
}

impl<'a> ProjectResolutionContext<'a> {
    pub fn new() -> Self {
        Self {
            project_root: None,
            target: None,
            environment_name: None,
            destination_policy: None,
            secret_provider: None,
        }
    }

    pub fn for_project_run(project_root: &'a Path, target: &'a TargetName) -> Self {
        Self {
            project_root: Some(project_root),
            target: Some(target),
            environment_name: None,
            destination_policy: None,
            secret_provider: None,
        }
    }

    pub fn with_environment_name(mut self, environment_name: &'a str) -> Self {
        self.environment_name = Some(environment_name);
        self
    }

    pub fn with_destination_policy(mut self, policy: &'a DestinationPolicy) -> Self {
        self.destination_policy = Some(policy);
        self
    }

    pub fn with_secret_provider(mut self, provider: &'a ProjectSecretProvider) -> Self {
        self.secret_provider = Some(provider);
        self
    }

    fn project_root(&self) -> Result<&'a Path> {
        self.project_root.ok_or_else(|| {
            CdfError::contract("project destination resolution requires a project root")
        })
    }

    fn target(&self) -> Result<&'a TargetName> {
        self.target.ok_or_else(|| {
            CdfError::contract("project destination resolution requires a run target")
        })
    }

    fn destination_policy(&self) -> Result<&'a DestinationPolicy> {
        self.destination_policy.ok_or_else(|| {
            CdfError::contract("project destination resolution requires destination policy")
        })
    }

    fn secret_provider(&self) -> Result<&'a ProjectSecretProvider> {
        self.secret_provider.ok_or_else(|| {
            CdfError::auth("secret-backed destination URI requires a SecretProvider")
        })
    }

    fn environment_name(&self) -> &str {
        self.environment_name.unwrap_or("<selected>")
    }
}

impl std::fmt::Debug for ProjectResolutionContext<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectResolutionContext")
            .field("project_root", &self.project_root)
            .field("target", &self.target)
            .field("environment_name", &self.environment_name)
            .field("destination_policy", &self.destination_policy.is_some())
            .field("secret_provider", &self.secret_provider.is_some())
            .finish_non_exhaustive()
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

#[derive(Default)]
pub struct ProjectDestinationRegistry {
    drivers: Vec<Box<dyn ProjectDestinationDriver>>,
}

impl ProjectDestinationRegistry {
    pub fn new() -> Self {
        Self {
            drivers: Vec::new(),
        }
    }

    pub fn with_builtin_drivers() -> Result<Self> {
        let mut registry = Self::new();
        registry.register(DuckDbProjectDestinationDriver)?;
        registry.register(ParquetProjectDestinationDriver)?;
        registry.register(PostgresProjectDestinationDriver)?;
        Ok(registry)
    }

    pub fn register<D>(&mut self, driver: D) -> Result<()>
    where
        D: ProjectDestinationDriver + 'static,
    {
        self.register_boxed(Box::new(driver))
    }

    pub fn register_boxed(&mut self, driver: Box<dyn ProjectDestinationDriver>) -> Result<()> {
        let schemes = driver.schemes();
        if schemes.is_empty() {
            return Err(CdfError::contract(
                "project destination driver must register at least one URI scheme",
            ));
        }
        for scheme in schemes {
            validate_project_destination_scheme(scheme)?;
            if self.driver_for_scheme(scheme).is_some() {
                return Err(CdfError::contract(format!(
                    "project destination driver scheme `{scheme}` is already registered"
                )));
            }
        }
        self.drivers.push(driver);
        Ok(())
    }

    pub fn resolve(
        &self,
        uri: &str,
        context: &ProjectResolutionContext<'_>,
    ) -> Result<Box<dyn ProjectDestinationRuntime>> {
        let scheme = project_destination_uri_scheme(uri)?;
        self.driver_for_scheme(scheme)
            .ok_or_else(|| {
                CdfError::contract(format!(
                    "no project destination driver registered for URI scheme `{scheme}`"
                ))
            })?
            .resolve(uri, context)
    }

    fn driver_for_scheme(&self, scheme: &str) -> Option<&dyn ProjectDestinationDriver> {
        self.drivers
            .iter()
            .map(|driver| driver.as_ref())
            .find(|driver| {
                driver
                    .schemes()
                    .iter()
                    .any(|registered| registered.eq_ignore_ascii_case(scheme))
            })
    }
}

pub trait ProjectDestinationRuntime {
    fn protocol(&self) -> &dyn DestinationProtocol;

    fn destination_sheet(&self) -> Result<DestinationSheet> {
        Ok(self.protocol().sheet().clone())
    }

    fn describe(&self) -> ProjectDestinationDescription;

    fn supported_dispositions(&self) -> &[WriteDisposition] {
        &self.protocol().sheet().supported_dispositions
    }

    fn quarantine_table_support(&self) -> CapabilitySupport {
        self.protocol().sheet().quarantine_tables.clone()
    }

    fn validate_run_preflight(
        &mut self,
        _resource: &dyn ResourceStream,
        _output_schema: &Schema,
        _schema_hash: &SchemaHash,
    ) -> Result<()> {
        Ok(())
    }

    fn plan_resource_commit(
        &mut self,
        _resource: &dyn ResourceStream,
        _output_schema: &Schema,
        inputs: &DestinationCommitPlanningInputs,
    ) -> Result<DestinationCommitPlanningOutcome> {
        let plan = self.protocol().plan_commit(&inputs.destination_commit)?;
        Ok(DestinationCommitPlanningOutcome::new(
            self.protocol().sheet().clone(),
            plan,
        ))
    }

    fn prepare_package_commit(
        &mut self,
        package_dir: &Path,
        reader: &PackageReader,
        inputs: &PackageReplayInputs,
        context: &DestinationPlanningContext<'_>,
    ) -> Result<PreparedDestinationCommit>;

    fn bind_prepared_commit(&mut self, prepared: &mut PreparedDestinationCommit) -> Result<()>;

    fn ensure_protocol_ready(&mut self) -> Result<()> {
        Ok(())
    }

    fn verify_receipt(&mut self, receipt: &Receipt) -> Result<cdf_kernel::ReceiptVerification> {
        self.ensure_protocol_ready()?;
        self.protocol().verify(receipt)
    }

    fn secret_redaction(&self) -> Option<&str> {
        None
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
            Box::new(DuckDbDestination::new(database_path)?),
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
    let registry = ProjectDestinationRegistry::with_builtin_drivers()?;
    let runtime = registry.resolve(uri, context)?;
    Ok(ResolvedProjectDestination::new(
        runtime,
        context.target()?.clone(),
    ))
}

fn project_destination_uri_scheme(uri: &str) -> Result<&str> {
    let (scheme, _) = uri.split_once(':').ok_or_else(|| {
        CdfError::contract(format!(
            "project destination URI `{uri}` is missing a scheme"
        ))
    })?;
    validate_project_destination_scheme(scheme)?;
    Ok(scheme)
}

fn validate_project_destination_scheme(scheme: &str) -> Result<()> {
    if scheme.is_empty() {
        return Err(CdfError::contract(
            "project destination URI scheme cannot be empty",
        ));
    }
    if scheme
        .bytes()
        .any(|byte| !(byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'.' | b'-')))
    {
        return Err(CdfError::contract(format!(
            "project destination URI scheme `{scheme}` contains invalid characters"
        )));
    }
    Ok(())
}

fn local_uri_path<'a>(uri: &'a str, scheme: &str) -> Result<&'a str> {
    let prefix = format!("{scheme}://");
    let raw = uri.strip_prefix(&prefix).ok_or_else(|| {
        CdfError::contract(format!(
            "destination URI `{uri}` is unsupported; expected {scheme}://path"
        ))
    })?;
    if raw.trim().is_empty() || raw.contains("://") {
        return Err(CdfError::contract(format!(
            "destination URI `{uri}` is malformed or non-local; expected {scheme}://path"
        )));
    }
    Ok(raw)
}

fn absolute_under_root(root: &Path, value: &str) -> PathBuf {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        path
    } else {
        root.join(path)
    }
}

pub(super) fn reject_unexpected_pending_context(
    prepared: &PreparedDestinationCommit,
    destination: &str,
) -> Result<()> {
    if prepared.has_pending_context() {
        return Err(CdfError::internal(format!(
            "{destination} prepared destination commit carried unexpected pending context"
        )));
    }
    Ok(())
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
