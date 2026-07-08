use std::{
    fs,
    path::{Path, PathBuf},
    process::Command as ProcessCommand,
};

use cdf_project::{FileResourceSourceResolver, ResourceSourceKind, validate_project};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    context::{DestinationRuntime, DoctorProbe, ProjectContext},
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

pub(crate) fn doctor(cli: &crate::args::Cli) -> Result<CommandOutput, CliError> {
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
    match validate_project(
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
    checks.extend(destination_checks(context.destination_runtime()));
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
    match runtime {
        DestinationRuntime::DuckDb {
            database_path,
            icu_probe,
            ..
        } => {
            let mut checks = vec![
                DoctorCheck::passed("destination", "DuckDB destination capabilities loaded")
                    .with_details(json!({
                        "kind": "duck_db",
                        "database_path": database_path,
                    })),
            ];
            checks.push(match icu_probe {
                DoctorProbe::Passed => DoctorCheck::passed("duckdb_icu", "ICU probe passed")
                    .with_details(duckdb_icu_details(&database_path, true, None)),
                DoctorProbe::Failed { message } => {
                    DoctorCheck::failed("duckdb_icu", message.clone())
                        .with_details(duckdb_icu_details(&database_path, false, Some(message)))
                }
                DoctorProbe::Skipped { reason } => {
                    DoctorCheck::skipped("duckdb_icu", reason.clone()).with_details(json!({
                        "database_path": database_path,
                        "database_exists": false,
                        "probe": "icu_sort_key",
                        "reason": reason,
                    }))
                }
            });
            checks
        }
        DestinationRuntime::Postgres { .. } => vec![DoctorCheck::passed(
            "destination",
            "Postgres destination capabilities loaded",
        )],
        DestinationRuntime::Unsupported { reason, .. } => {
            vec![DoctorCheck::unsupported("destination", reason)]
        }
    }
}

fn duckdb_icu_details(
    database_path: &str,
    available: bool,
    diagnostic: Option<String>,
) -> serde_json::Value {
    json!({
        "database_path": database_path,
        "database_exists": true,
        "probe": "icu_sort_key",
        "available": available,
        "diagnostic": diagnostic,
    })
}

fn ledger_destination_drift_check(context: &ProjectContext) -> DoctorCheck {
    match doctor_drift::probe(context) {
        Ok(probe) => match probe.status {
            DriftStatus::Passed => DoctorCheck::passed("ledger_destination_drift", probe.message)
                .with_details(probe.details),
            DriftStatus::Failed => DoctorCheck::failed("ledger_destination_drift", probe.message)
                .with_details(probe.details),
            DriftStatus::Skipped => DoctorCheck::skipped("ledger_destination_drift", probe.message)
                .with_details(probe.details),
            DriftStatus::Unsupported => {
                DoctorCheck::unsupported("ledger_destination_drift", probe.message)
                    .with_details(probe.details)
            }
        },
        Err(error) => DoctorCheck::failed("ledger_destination_drift", error.message),
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
