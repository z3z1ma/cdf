use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};

use cdf_kernel::{
    CdfError, QueryableResource, ResourceDescriptor, ResourceStream, Result, ScopeKey,
    TypePolicyAllowances,
};
use cdf_runtime::{
    BlockingLaneSpec, CompiledSourcePlan, CompiledSourcePlanInput, LaneAffinity,
    SourceAttestationStrength, SourceBatchMemoryContract, SourceCompileRequest,
    SourceDiscoveryCandidate, SourceDiscoveryKind, SourceDiscoveryRequest, SourceDiscoverySession,
    SourceDriver, SourceDriverDescriptor, SourceDriverId, SourceExecutionCapabilities,
    SourceExecutorClass, SourceHealthRequest, SourceHealthResult, SourceHealthStatus,
    SourceReferenceCompileRequest, SourceReferenceCompiler, SourceResolutionContext,
    SourceRetryGranularity, SourceSchemaObservation, artifact_hash,
};
use serde::Deserialize;

use crate::{PythonPhysicalPlan, PythonResource, validate_attached_interpreter};

const DRIVER_ID: &str = "python";
const MIN_PYTHON_MAJOR: u16 = 3;
const MIN_PYTHON_MINOR: u16 = 12;
const INTERPRETER_PROBE: &str = r#"
import json
import platform
import sys
import sysconfig

gil_enabled = True
is_gil_enabled = getattr(sys, "_is_gil_enabled", None)
if is_gil_enabled is not None:
    gil_enabled = bool(is_gil_enabled())

free_threaded_build = sysconfig.get_config_var("Py_GIL_DISABLED") == 1
version = sys.version_info
sys.stdout.write(json.dumps({
    "executable": sys.executable,
    "version": "{}.{}.{}".format(version.major, version.minor, version.micro),
    "major": version.major,
    "minor": version.minor,
    "micro": version.micro,
    "implementation": platform.python_implementation(),
    "gil_enabled": gil_enabled,
    "free_threaded_build": free_threaded_build,
    "can_parallelize_python": free_threaded_build and not gil_enabled,
}, sort_keys=True))
"#;

#[derive(Clone, Debug)]
pub struct PythonSourceDriver {
    descriptor: SourceDriverDescriptor,
    option_schema: serde_json::Value,
}

impl PythonSourceDriver {
    pub fn new() -> Result<Self> {
        let option_schema = serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "source": {
                "type": "object",
                "additionalProperties": false,
                "required": ["uri"],
                "properties": {
                    "uri": {"type": "string", "pattern": "^python://"}
                }
            },
            "resource": {
                "type": "object",
                "additionalProperties": false,
                "properties": {}
            }
        });
        Ok(Self {
            descriptor: SourceDriverDescriptor {
                driver_id: SourceDriverId::new(DRIVER_ID)?,
                driver_version: "1.0.0".to_owned(),
                option_schema_hash: artifact_hash(&option_schema)?,
                kinds: vec![DRIVER_ID.to_owned()],
                schemes: vec![DRIVER_ID.to_owned()],
            },
            option_schema,
        })
    }
}

impl SourceDriver for PythonSourceDriver {
    fn descriptor(&self) -> &SourceDriverDescriptor {
        &self.descriptor
    }

    fn option_schema(&self) -> &serde_json::Value {
        &self.option_schema
    }

    fn validate_project_options(&self, options: &serde_json::Value) -> Result<()> {
        decode_project_options(options).map(drop)
    }

    fn compile(&self, request: SourceCompileRequest) -> Result<CompiledSourcePlan> {
        request.context.validate()?;
        let options: PythonSourceOptions = decode_options(request.source_options.clone())?;
        let _: EmptyOptions = decode_options(request.resource_options.clone())?;
        let project_root = request.context.project_root.as_deref().ok_or_else(|| {
            CdfError::contract("Python source compilation requires a project root")
        })?;
        let resource = PythonResource::load(
            project_root,
            &options.uri,
            request.descriptor.resource_id.clone(),
            request.descriptor.trust_level.clone(),
        )?;
        validate_declarative_metadata(&request, &resource)?;
        compile_resource_plan(
            self.descriptor.clone(),
            request.descriptor,
            request.schema,
            request.type_policy_allowances,
            resource,
            options.uri,
        )
    }

    fn reference_compiler(&self) -> Option<&dyn SourceReferenceCompiler> {
        Some(self)
    }

    fn health(
        &self,
        request: SourceHealthRequest,
        context: &SourceResolutionContext<'_>,
    ) -> Result<Vec<SourceHealthResult>> {
        self.doctor_health(request, context)
    }

    fn discovery_session(
        &self,
        plan: &CompiledSourcePlan,
        _context: &SourceResolutionContext<'_>,
    ) -> Result<Box<dyn SourceDiscoverySession>> {
        let physical = physical_plan(plan)?;
        Ok(Box::new(PythonDiscoverySession {
            location: format!(
                "python://{}#{}",
                physical.module_relative, physical.callable
            ),
            content_hash: physical.content_hash,
            schema: plan.schema.clone(),
        }))
    }

    fn resolve(
        &self,
        plan: &CompiledSourcePlan,
        context: &SourceResolutionContext<'_>,
    ) -> Result<Arc<dyn QueryableResource>> {
        validate_project_options(
            context.project_root(),
            context.driver_options(&self.descriptor.driver_id),
        )?;
        let lane = plan
            .execution_capabilities
            .blocking_lane
            .as_ref()
            .ok_or_else(|| {
                CdfError::contract("compiled Python source omitted its blocking lane")
            })?;
        let resource = PythonResource::from_compiled(
            context.project_root(),
            plan.descriptor.clone(),
            Arc::new(plan.schema.clone()),
            plan.resource_capabilities.clone(),
            physical_plan(plan)?,
            artifact_hash(plan)?,
        )?
        .with_execution_services_and_lane(context.execution().clone(), lane.lane_id.clone())?;
        Ok(Arc::new(resource))
    }
}

impl SourceReferenceCompiler for PythonSourceDriver {
    fn compile_reference(
        &self,
        request: SourceReferenceCompileRequest,
    ) -> Result<CompiledSourcePlan> {
        request.validate()?;
        validate_project_options(&request.project_root, Some(&request.project_options))?;
        let resource = PythonResource::load(
            &request.project_root,
            &request.uri,
            request.resource_id.clone(),
            request.trust_level,
        )?;
        let mut descriptor = resource.descriptor().clone();
        descriptor.freshness = request.freshness;
        let schema = resource.schema().as_ref().clone();
        compile_resource_plan(
            self.descriptor.clone(),
            descriptor,
            schema,
            TypePolicyAllowances::default(),
            resource,
            request.uri,
        )
    }
}

impl PythonSourceDriver {
    fn doctor_health(
        &self,
        request: SourceHealthRequest,
        context: &SourceResolutionContext<'_>,
    ) -> Result<Vec<SourceHealthResult>> {
        let resource_count = request.compiled_plans.len();
        let Some(options) = context.driver_options(&self.descriptor.driver_id) else {
            let (status, message) = if resource_count == 0 {
                (
                    SourceHealthStatus::Skipped,
                    "no python.interpreter configured".to_owned(),
                )
            } else {
                (
                    SourceHealthStatus::Failed,
                    "python.interpreter is required because at least one Python resource is configured"
                        .to_owned(),
                )
            };
            return Ok(vec![SourceHealthResult {
                probe_id: "interpreter".to_owned(),
                status,
                message,
                details: serde_json::json!({
                    "python_resources": resource_count,
                    "require_free_threaded": false,
                }),
            }]);
        };
        let options = decode_project_options(options)?;
        let path = configured_interpreter_path(context.project_root(), &options.interpreter);
        let result = match probe_interpreter(&path) {
            Err(message) => SourceHealthResult {
                probe_id: "interpreter".to_owned(),
                status: SourceHealthStatus::Failed,
                message,
                details: serde_json::json!({
                    "executable": path.display().to_string(),
                    "require_free_threaded": options.require_free_threaded,
                    "python_resources": resource_count,
                }),
            },
            Ok((executable, report)) => {
                let details = serde_json::json!({
                    "executable": executable.display().to_string(),
                    "reported_executable": report.executable,
                    "version": report.version,
                    "implementation": report.implementation,
                    "gil_enabled": report.gil_enabled,
                    "free_threaded_build": report.free_threaded_build,
                    "can_parallelize_python": report.can_parallelize_python,
                    "require_free_threaded": options.require_free_threaded,
                    "python_resources": resource_count,
                });
                if (report.major, report.minor) < (MIN_PYTHON_MAJOR, MIN_PYTHON_MINOR) {
                    SourceHealthResult {
                        probe_id: "interpreter".to_owned(),
                        status: SourceHealthStatus::Failed,
                        message: format!(
                            "Python interpreter {} is older than required {MIN_PYTHON_MAJOR}.{MIN_PYTHON_MINOR}",
                            report.version
                        ),
                        details,
                    }
                } else if options.require_free_threaded && !report.can_parallelize_python {
                    SourceHealthResult {
                        probe_id: "interpreter".to_owned(),
                        status: SourceHealthStatus::Failed,
                        message: "configured Python resources require a free-threaded interpreter with the GIL disabled".to_owned(),
                        details,
                    }
                } else {
                    SourceHealthResult {
                        probe_id: "interpreter".to_owned(),
                        status: SourceHealthStatus::Passed,
                        message: format!(
                            "configured interpreter {} passed Python doctor probe",
                            report.version
                        ),
                        details,
                    }
                }
            }
        };
        Ok(vec![result])
    }
}

fn compile_resource_plan(
    driver: SourceDriverDescriptor,
    descriptor: ResourceDescriptor,
    schema: arrow_schema::Schema,
    type_policy_allowances: TypePolicyAllowances,
    resource: PythonResource,
    uri: String,
) -> Result<CompiledSourcePlan> {
    let capabilities = resource.capabilities().clone();
    let physical = resource.physical_plan();
    CompiledSourcePlan::new(
        driver,
        capabilities.clone(),
        execution_capabilities(
            capabilities.partitioning.parallel_partitions,
            physical.bounded,
        ),
        CompiledSourcePlanInput {
            descriptor,
            schema,
            type_policy_allowances,
            effective_schema_runtime: None,
            baseline_observation_schema_catalog: Vec::new(),
            redacted_options: serde_json::json!({"uri": uri}),
            physical_plan: serde_json::to_value(physical).map_err(|error| {
                CdfError::internal(format!("serialize Python source plan: {error}"))
            })?,
        },
    )
}

fn validate_declarative_metadata(
    request: &SourceCompileRequest,
    resource: &PythonResource,
) -> Result<()> {
    let observed = resource.descriptor();
    if resource.schema().as_ref() != &request.schema
        || observed.primary_key != request.descriptor.primary_key
        || observed.merge_key != request.descriptor.merge_key
        || observed.cursor != request.descriptor.cursor
        || observed.write_disposition != request.descriptor.write_disposition
        || observed.state_scope != ScopeKey::Resource
    {
        return Err(CdfError::contract(
            "declarative Python schema, keys, cursor, disposition, or scope does not match the callable metadata",
        ));
    }
    Ok(())
}

fn physical_plan(plan: &CompiledSourcePlan) -> Result<PythonPhysicalPlan> {
    serde_json::from_value(plan.physical_plan.clone())
        .map_err(|error| CdfError::contract(format!("invalid Python source plan: {error}")))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PythonSourceOptions {
    uri: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EmptyOptions {}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PythonProjectOptions {
    interpreter: String,
    #[serde(default)]
    require_free_threaded: bool,
}

fn decode_options<T: for<'de> Deserialize<'de>>(
    options: BTreeMap<String, serde_json::Value>,
) -> Result<T> {
    serde_json::from_value(serde_json::Value::Object(options.into_iter().collect()))
        .map_err(|error| CdfError::contract(format!("Python source options are invalid: {error}")))
}

fn validate_project_options(
    project_root: &std::path::Path,
    options: Option<&serde_json::Value>,
) -> Result<()> {
    let options = options.ok_or_else(|| {
        CdfError::contract("python.interpreter is required for Python plan, preview, and run")
    })?;
    let options = decode_project_options(options)?;
    let configured = configured_interpreter_path(project_root, &options.interpreter);
    let configured = configured.canonicalize().map_err(|error| {
        CdfError::contract(format!(
            "configured Python interpreter is missing or inaccessible at {}: {error}",
            configured.display()
        ))
    })?;
    validate_attached_interpreter(configured, options.require_free_threaded)?;
    Ok(())
}

fn decode_project_options(options: &serde_json::Value) -> Result<PythonProjectOptions> {
    serde_json::from_value(options.clone())
        .map_err(|error| CdfError::contract(format!("Python project options are invalid: {error}")))
}

fn configured_interpreter_path(project_root: &Path, interpreter: &str) -> PathBuf {
    let configured = PathBuf::from(interpreter);
    if configured.is_absolute() {
        configured
    } else {
        project_root.join(configured)
    }
}

#[derive(Debug, Deserialize)]
struct PythonProbeReport {
    executable: String,
    version: String,
    major: u16,
    minor: u16,
    micro: u16,
    implementation: String,
    gil_enabled: bool,
    free_threaded_build: bool,
    can_parallelize_python: bool,
}

fn probe_interpreter(path: &Path) -> std::result::Result<(PathBuf, PythonProbeReport), String> {
    let metadata = fs::metadata(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            format!("configured interpreter is missing at {}", path.display())
        } else {
            format!(
                "configured interpreter metadata could not be read at {}: {error}",
                path.display()
            )
        }
    })?;
    if !metadata.is_file() {
        return Err(format!(
            "configured interpreter is not a file at {}",
            path.display()
        ));
    }
    if !is_executable(&metadata) {
        return Err(format!(
            "configured interpreter is not executable at {}",
            path.display()
        ));
    }
    let executable = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let output = Command::new(&executable)
        .arg("-I")
        .arg("-c")
        .arg(INTERPRETER_PROBE)
        .output()
        .map_err(|error| format!("configured interpreter could not be executed: {error}"))?;
    if !output.status.success() {
        return Err(match output.status.code() {
            Some(code) => {
                format!("configured interpreter inspection exited unsuccessfully with code {code}")
            }
            None => "configured interpreter inspection exited unsuccessfully".to_owned(),
        });
    }
    let report: PythonProbeReport = serde_json::from_slice(&output.stdout).map_err(|error| {
        format!("configured interpreter did not emit valid inspection JSON: {error}")
    })?;
    if report.version != format!("{}.{}.{}", report.major, report.minor, report.micro)
        || report.can_parallelize_python != (report.free_threaded_build && !report.gil_enabled)
    {
        return Err(
            "configured interpreter emitted inconsistent version or GIL metadata".to_owned(),
        );
    }
    Ok((executable, report))
}

#[cfg(unix)]
fn is_executable(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;

    metadata.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn is_executable(_metadata: &fs::Metadata) -> bool {
    true
}

fn execution_capabilities(parallel: bool, bounded: bool) -> SourceExecutionCapabilities {
    let concurrency = if parallel { 64 } else { 1 };
    SourceExecutionCapabilities {
        minimum_poll_bytes: 8 * 1024,
        maximum_poll_bytes: crate::DEFAULT_BOUNDARY_CHANNEL_BYTES,
        minimum_decode_bytes: 8 * 1024,
        maximum_decode_bytes: crate::DEFAULT_BOUNDARY_CHANNEL_BYTES,
        maximum_concurrency: concurrency,
        useful_concurrency: concurrency,
        executor_class: SourceExecutorClass::BlockingLane,
        blocking_lane: Some(BlockingLaneSpec {
            lane_id: "python.source".to_owned(),
            maximum_concurrency: concurrency,
            cpu_slot_cost: 1,
            native_internal_parallelism: 1,
            affinity: LaneAffinity::Shared,
            interruption: cdf_runtime::InterruptionSafety::CooperativeOnly,
        }),
        pausable: true,
        spillable: false,
        idempotent_reads: false,
        reopenable: false,
        resumable: false,
        speculative_safe: false,
        retry_granularity: SourceRetryGranularity::None,
        retryable_errors: Vec::new(),
        retry_policy: None,
        attestation: SourceAttestationStrength::ImmutableContent,
        rate_limit: None,
        quota_authority: None,
        canonical_order: true,
        bounded,
        batch_memory: SourceBatchMemoryContract::Preaccounted,
        telemetry_version: "v1".to_owned(),
    }
}

struct PythonDiscoverySession {
    location: String,
    content_hash: String,
    schema: arrow_schema::Schema,
}

impl SourceDiscoverySession for PythonDiscoverySession {
    fn kind(&self) -> SourceDiscoveryKind {
        SourceDiscoveryKind::SchemaMetadata
    }

    fn candidates(&self) -> Result<Vec<SourceDiscoveryCandidate>> {
        Ok(vec![SourceDiscoveryCandidate::new(
            self.location.clone(),
            None,
            None,
            BTreeMap::from([
                ("source_kind".to_owned(), DRIVER_ID.to_owned()),
                ("content_hash".to_owned(), self.content_hash.clone()),
            ]),
        )?])
    }

    fn observe(
        &self,
        candidate: &SourceDiscoveryCandidate,
        request: &SourceDiscoveryRequest,
    ) -> Result<SourceSchemaObservation> {
        request.validate()?;
        SourceSchemaObservation::new(
            candidate,
            self.schema.clone(),
            BTreeMap::from([("content_hash".to_owned(), self.content_hash.clone())]),
            0,
            0,
        )
    }
}
