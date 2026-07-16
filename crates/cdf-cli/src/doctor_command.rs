use std::{
    fs,
    path::{Path, PathBuf},
    process::Command as ProcessCommand,
};

use cdf_kernel::ScanRequest;
use cdf_project::{FileResourceSourceResolver, ResourceSourceKind, validate_project};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    context::{DestinationRuntime, ProjectContext},
    doctor_drift::{self, DriftStatus},
    output::{CliError, CommandOutput},
    render::{
        RenderDocument,
        primitives::{KeyValuePanel, NextCommand, SectionRule, StatusKind, StatusLine, Table},
        redaction::redact_uri_userinfo,
    },
};

const MIN_PYTHON_MAJOR: u16 = 3;
const MIN_PYTHON_MINOR: u16 = 12;
const PYTHON_INTERPRETER_PROBE: &str = r#"
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

pub(crate) fn doctor(
    cli: &crate::args::Cli,
    execution: &cdf_runtime::ExecutionServices,
    destinations: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let mut checks = vec![
        DoctorCheck::passed("project_file", "cdf.toml parsed and environment resolved")
            .with_details(project_health_details(&context)),
        DoctorCheck::passed(
            "declarative_resources",
            format!("{} resource(s) compiled", context.resources.len()),
        ),
    ];

    let resolver = FileResourceSourceResolver::new(&context.root);
    let provider = context.secret_provider();
    let source_registry = crate::source_registry::builtin_source_registry()?;
    match validate_project(
        &source_registry,
        &context.config,
        Some(&context.environment.name),
        &resolver,
        &provider,
    ) {
        Ok(report) => checks.push(
            DoctorCheck::passed(
                "secrets",
                format!(
                    "{} secret reference(s) resolved",
                    report.checked_secrets.len()
                ),
            )
            .with_details(secret_check_details(&report)),
        ),
        Err(error) => checks.push(DoctorCheck::failed("secrets", error.to_string())),
    }

    checks.push(python_check(&context));
    checks.extend(source_runtime_checks(&context, execution));
    checks.extend(destination_checks(
        context.destination_runtime(destinations),
    ));
    checks.push(ledger_destination_drift_check(&context));

    let failed = checks
        .iter()
        .filter(|check| matches!(check.status, CheckStatus::Failed))
        .count();
    let unsupported = checks
        .iter()
        .filter(|check| matches!(check.status, CheckStatus::Unsupported))
        .count();
    let report = DoctorReport {
        checks,
        failed,
        unsupported,
    };
    let exit_code = if failed == 0 { 0 } else { 1 };
    CommandOutput::rendered_with_exit_code("doctor", report.render_document(), report, exit_code)
}

fn source_runtime_checks(
    context: &ProjectContext,
    execution: &cdf_runtime::ExecutionServices,
) -> Vec<DoctorCheck> {
    let registry = match crate::source_registry::builtin_source_registry() {
        Ok(registry) => registry,
        Err(error) => {
            return vec![DoctorCheck::failed(
                "source_registry",
                format!("source registry initialization failed: {}", error.message),
            )];
        }
    };
    let resolution = cdf_runtime::SourceResolutionContext::new(
        &context.root,
        std::sync::Arc::new(context.secret_provider()),
        execution,
    );
    context
        .resources
        .iter()
        .map(|resource| {
            let resource_id = resource.descriptor().resource_id.to_string();
            let driver = resource.source_plan().driver.driver_id.as_str();
            let probe = registry
                .resolve(resource.source_plan(), &resolution)
                .and_then(|runtime| {
                    runtime.plan_partitions(&ScanRequest {
                        resource_id: resource.descriptor().resource_id.clone(),
                        projection: None,
                        filters: Vec::new(),
                        limit: None,
                        order_by: Vec::new(),
                        scope: resource.descriptor().state_scope.clone(),
                    })
                });
            match probe {
                Ok(partitions) => DoctorCheck::passed(
                    format!("source:{resource_id}"),
                    format!("{driver} source resolved {} partition(s)", partitions.len()),
                )
                .with_details(json!({
                    "resource_id": resource_id,
                    "driver": driver,
                    "partitions": partitions.len(),
                })),
                Err(error) => DoctorCheck::failed(
                    format!("source:{resource_id}"),
                    format!("{driver} source probe failed: {}", error.message),
                )
                .with_details(json!({
                    "resource_id": resource_id,
                    "driver": driver,
                    "partitions": 0,
                })),
            }
        })
        .collect()
}

fn project_health_details(context: &ProjectContext) -> serde_json::Value {
    json!({
        "project_root": context.root,
        "selected_environment": context.environment.name,
        "compiled_resources": context.resources.len(),
        "lockfile_present": context.lock.is_some(),
    })
}

fn secret_check_details(report: &cdf_project::ProjectValidationReport) -> serde_json::Value {
    json!({
        "count": report.checked_secrets.len(),
        "references": report
            .checked_secrets
            .iter()
            .map(|check| check.uri.as_str())
            .collect::<Vec<_>>(),
    })
}

fn python_check(context: &ProjectContext) -> DoctorCheck {
    let require_free_threaded = context.config.python.require_free_threaded.unwrap_or(false);
    let Some(interpreter) = &context.config.python.interpreter else {
        return if has_python_resource(context) {
            DoctorCheck::failed(
                "python",
                "python.interpreter is required because at least one Python resource is configured",
            )
            .with_details(json!({
                "python_resources": python_resource_count(context),
                "require_free_threaded": require_free_threaded,
            }))
        } else {
            DoctorCheck::skipped("python", "no python.interpreter configured")
        };
    };

    let path = configured_interpreter_path(&context.root, interpreter);
    let (executable, report) = match probe_python_interpreter(&path) {
        Ok(report) => report,
        Err(message) => {
            return DoctorCheck::failed("python", message)
                .with_details(python_config_details(&path, require_free_threaded));
        }
    };
    let details = python_probe_details(&executable, &report, require_free_threaded);

    if (report.major, report.minor) < (MIN_PYTHON_MAJOR, MIN_PYTHON_MINOR) {
        return DoctorCheck::failed(
            "python",
            format!(
                "Python interpreter {} is older than required {MIN_PYTHON_MAJOR}.{MIN_PYTHON_MINOR}",
                python_version(&report)
            ),
        )
        .with_details(details);
    }

    if require_free_threaded && !python_can_parallelize(&report) {
        return DoctorCheck::failed(
            "python",
            "configured Python resources require a free-threaded interpreter with the GIL disabled",
        )
        .with_details(details);
    }

    DoctorCheck::passed(
        "python",
        format!(
            "configured interpreter {} passed Python doctor probe",
            python_version(&report)
        ),
    )
    .with_details(details)
}

fn configured_interpreter_path(root: &Path, interpreter: &str) -> PathBuf {
    let path = PathBuf::from(interpreter);
    if path.is_absolute() {
        path
    } else {
        root.join(path)
    }
}

fn has_python_resource(context: &ProjectContext) -> bool {
    python_resource_count(context) > 0
}

fn python_resource_count(context: &ProjectContext) -> usize {
    context
        .config
        .resources
        .values()
        .filter(|resource| matches!(resource.source_kind(), ResourceSourceKind::Python { .. }))
        .count()
}

fn probe_python_interpreter(path: &Path) -> Result<(PathBuf, PythonProbeReport), String> {
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
    let output = ProcessCommand::new(&executable)
        .arg("-I")
        .arg("-c")
        .arg(PYTHON_INTERPRETER_PROBE)
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
    validate_python_probe_report(&report)?;
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

fn validate_python_probe_report(report: &PythonProbeReport) -> Result<(), String> {
    if report.version != python_version(report) {
        return Err("configured interpreter emitted inconsistent version metadata".to_owned());
    }
    if report.can_parallelize_python != python_can_parallelize(report) {
        return Err("configured interpreter emitted inconsistent GIL metadata".to_owned());
    }
    Ok(())
}

fn python_config_details(path: &Path, require_free_threaded: bool) -> serde_json::Value {
    json!({
        "executable": path.display().to_string(),
        "require_free_threaded": require_free_threaded,
    })
}

fn python_probe_details(
    executable: &Path,
    report: &PythonProbeReport,
    require_free_threaded: bool,
) -> serde_json::Value {
    json!({
        "executable": executable.display().to_string(),
        "reported_executable": report.executable,
        "version": python_version(report),
        "implementation": report.implementation,
        "gil_enabled": report.gil_enabled,
        "free_threaded_build": report.free_threaded_build,
        "can_parallelize_python": python_can_parallelize(report),
        "require_free_threaded": require_free_threaded,
    })
}

fn python_version(report: &PythonProbeReport) -> String {
    format!("{}.{}.{}", report.major, report.minor, report.micro)
}

fn python_can_parallelize(report: &PythonProbeReport) -> bool {
    report.free_threaded_build && !report.gil_enabled
}

fn destination_checks(runtime: DestinationRuntime) -> Vec<DoctorCheck> {
    if let Some(error) = runtime.error {
        return vec![DoctorCheck::unsupported("destination", error)];
    }
    let mut checks = runtime
        .health
        .into_iter()
        .map(|result| {
            let message = redact_uri_userinfo(&result.message);
            let check = match result.status {
                cdf_runtime::DestinationHealthStatus::Passed => {
                    DoctorCheck::passed(result.probe_id, message)
                }
                cdf_runtime::DestinationHealthStatus::Failed => {
                    DoctorCheck::failed(result.probe_id, message)
                }
                cdf_runtime::DestinationHealthStatus::Skipped => {
                    DoctorCheck::skipped(result.probe_id, message)
                }
                cdf_runtime::DestinationHealthStatus::Unsupported => {
                    DoctorCheck::unsupported(result.probe_id, message)
                }
            };
            check.with_details(redact_json_uri_userinfo(json!(result.details)))
        })
        .collect::<Vec<_>>();
    checks.push(destination_bulk_path_check(runtime.capabilities));
    checks
}

fn destination_bulk_path_check(
    capabilities: Option<cdf_runtime::DestinationRuntimeCapabilities>,
) -> DoctorCheck {
    let Some(capabilities) = capabilities else {
        return DoctorCheck::unsupported(
            "destination_bulk_paths",
            "destination does not publish runtime bulk-path capabilities",
        );
    };
    if capabilities.bulk_paths.is_empty() {
        return DoctorCheck::unsupported(
            "destination_bulk_paths",
            "destination publishes no bulk path descriptors",
        );
    }
    if let Err(error) = capabilities.validate() {
        return DoctorCheck::failed(
            "destination_bulk_paths",
            format!(
                "destination bulk-path declaration is invalid: {}",
                error.message
            ),
        )
        .with_details(json!({
            "selected_path": &capabilities.bulk_path,
            "evidence_version": &capabilities.bulk_evidence_version,
            "paths": &capabilities.bulk_paths,
        }));
    }
    let selected = capabilities.bulk_path.as_deref();
    let details = json!({
        "selected_path": selected,
        "evidence_version": &capabilities.bulk_evidence_version,
        "paths": &capabilities.bulk_paths,
    });
    DoctorCheck::passed(
        "destination_bulk_paths",
        format!(
            "selected measured bulk path {}",
            selected.unwrap_or("<unavailable>")
        ),
    )
    .with_details(details)
}

fn redact_json_uri_userinfo(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::String(value) => serde_json::Value::String(redact_uri_userinfo(&value)),
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(redact_json_uri_userinfo).collect())
        }
        serde_json::Value::Object(values) => serde_json::Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, redact_json_uri_userinfo(value)))
                .collect(),
        ),
        value => value,
    }
}

fn ledger_destination_drift_check(context: &ProjectContext) -> DoctorCheck {
    match doctor_drift::probe(context) {
        Ok(probe) => {
            let message = redact_uri_userinfo(&probe.message);
            let details = redact_json_uri_userinfo(probe.details);
            match probe.status {
                DriftStatus::Passed => {
                    DoctorCheck::passed("ledger_destination_drift", message).with_details(details)
                }
                DriftStatus::Failed => {
                    DoctorCheck::failed("ledger_destination_drift", message).with_details(details)
                }
                DriftStatus::Skipped => {
                    DoctorCheck::skipped("ledger_destination_drift", message).with_details(details)
                }
                DriftStatus::Unsupported => {
                    DoctorCheck::unsupported("ledger_destination_drift", message)
                        .with_details(details)
                }
            }
        }
        Err(error) => DoctorCheck::failed(
            "ledger_destination_drift",
            redact_uri_userinfo(&error.message),
        ),
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct DoctorReport {
    checks: Vec<DoctorCheck>,
    failed: usize,
    unsupported: usize,
}

impl DoctorReport {
    fn render_document(&self) -> RenderDocument {
        let table = self.checks.iter().fold(
            Table::new(["check", "status", "message"]),
            |table, check| {
                table.row([
                    check.name.clone(),
                    check.status.name().to_owned(),
                    redact_uri_userinfo(&check.message),
                ])
            },
        );

        RenderDocument::new()
            .push(SectionRule::new())
            .push(StatusLine::new(
                if self.failed > 0 {
                    StatusKind::Error
                } else if self.unsupported > 0 {
                    StatusKind::Warning
                } else {
                    StatusKind::Success
                },
                if self.failed == 0 {
                    format!(
                        "doctor completed with {} unsupported check(s)",
                        self.unsupported
                    )
                } else {
                    format!("doctor found {} failed check(s)", self.failed)
                },
            ))
            .blank_line()
            .push(
                KeyValuePanel::new("Doctor")
                    .row("checks", self.checks.len().to_string())
                    .row("failed", self.failed.to_string())
                    .row("unsupported", self.unsupported.to_string())
                    .row("passed", self.passed_count().to_string())
                    .row("skipped", self.skipped_count().to_string()),
            )
            .blank_line()
            .push(table)
            .blank_line()
            .push(NextCommand::new("cdf status"))
    }

    fn passed_count(&self) -> usize {
        self.checks
            .iter()
            .filter(|check| matches!(check.status, CheckStatus::Passed))
            .count()
    }

    fn skipped_count(&self) -> usize {
        self.checks
            .iter()
            .filter(|check| matches!(check.status, CheckStatus::Skipped))
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn destination_doctor_rendering_redacts_driver_health_in_json_and_human_output() {
        let runtime = DestinationRuntime {
            kind: "fourth".to_owned(),
            destination_id: Some("fourth".to_owned()),
            label: Some("fourth destination".to_owned()),
            schemes: vec!["fourth".to_owned()],
            sheet: None,
            capabilities: None,
            health: vec![cdf_runtime::DestinationHealthResult {
                probe_id: "fourth_ready".to_owned(),
                status: cdf_runtime::DestinationHealthStatus::Passed,
                message: "connected to fourth://user:doctor-secret@example.invalid/db".to_owned(),
                details: BTreeMap::from([(
                    "endpoint".to_owned(),
                    json!("fourth://user:doctor-secret@example.invalid/db"),
                )]),
            }],
            error: None,
        };
        let checks = destination_checks(runtime);
        let report = DoctorReport {
            checks,
            failed: 0,
            unsupported: 0,
        };

        let json = serde_json::to_string(&report).unwrap();
        let human = report
            .render_document()
            .render(&crate::render::RenderConfig::headless_for_width(96));
        assert!(!json.contains("doctor-secret"));
        assert!(!human.contains("doctor-secret"));
        assert!(json.contains("fourth://[redacted]@example.invalid/db"));
        assert!(human.contains("fourth://[redacted]@example.invalid/db"));
    }

    #[test]
    fn destination_doctor_reports_registry_bulk_path_degradation_without_driver_branches() {
        let descriptor = cdf_runtime::BulkPathDescriptor {
            path_id: "fourth_native".to_owned(),
            version: 1,
            ingress_mode: cdf_runtime::DestinationIngressMode::FinalizedPackageOnly,
            writer_model: cdf_runtime::DestinationWriterModel::SingleWriter,
            ordering: cdf_runtime::BulkOrdering::ManifestOrder,
            rows: cdf_runtime::BulkSizeRange {
                minimum: 1,
                preferred: 64,
                maximum: 128,
            },
            bytes: cdf_runtime::BulkSizeRange {
                minimum: 1,
                preferred: 1024,
                maximum: 4096,
            },
            max_useful_writers: 1,
            blocking_lane: None,
            native_internal_parallelism: 1,
            external_staging: false,
            fallback: cdf_runtime::BulkFallbackMode::PreflightOnly,
            schema_preflight_version: "fourth-schema@1".to_owned(),
            measured_evidence_version: None,
        };
        let check =
            destination_bulk_path_check(Some(cdf_runtime::DestinationRuntimeCapabilities {
                bulk_paths: vec![descriptor],
                bulk_path: Some("fourth_native".to_owned()),
                ..Default::default()
            }));

        assert_eq!(check.status, CheckStatus::Failed);
        assert!(check.message.contains("measured evidence version"));
        assert_eq!(check.details.unwrap()["selected_path"], "fourth_native");

        let unavailable = destination_bulk_path_check(None);
        assert_eq!(unavailable.status, CheckStatus::Unsupported);
        assert!(unavailable.message.contains("does not publish"));
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct DoctorCheck {
    name: String,
    status: CheckStatus,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<serde_json::Value>,
}

impl DoctorCheck {
    fn passed(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: CheckStatus::Passed,
            message: message.into(),
            details: None,
        }
    }

    fn failed(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: CheckStatus::Failed,
            message: message.into(),
            details: None,
        }
    }

    fn skipped(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: CheckStatus::Skipped,
            message: message.into(),
            details: None,
        }
    }

    fn unsupported(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: CheckStatus::Unsupported,
            message: message.into(),
            details: None,
        }
    }

    fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum CheckStatus {
    Passed,
    Failed,
    Skipped,
    Unsupported,
}

impl CheckStatus {
    fn name(&self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
            Self::Unsupported => "unsupported",
        }
    }
}
