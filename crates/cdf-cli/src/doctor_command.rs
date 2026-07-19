use cdf_project::{FileResourceSourceResolver, ResourceSourceKind, validate_project};
use serde::Serialize;
use serde_json::json;

use crate::{
    context::{DestinationRuntime, ProjectContext},
    doctor_drift::{self, DriftStatus},
    output::{CliError, CommandOutput},
    render::{
        RenderDocument,
        humanize::humanize_bytes,
        primitives::{KeyValuePanel, NextCommand, SectionRule, StatusKind, StatusLine, Table},
        redaction::redact_uri_userinfo,
    },
};

pub(crate) fn doctor(
    cli: &cdf_cli_core::args::Cli,
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
        source_registry,
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

    checks.extend(source_driver_health_checks(
        &context,
        source_registry,
        execution,
    ));
    checks.extend(destination_checks(
        context.destination_runtime(destinations),
    ));
    checks.push(runtime_memory_budget_check(cli, execution));
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

fn runtime_memory_budget_check(
    cli: &cdf_cli_core::args::Cli,
    execution: &cdf_runtime::ExecutionServices,
) -> DoctorCheck {
    let report = match crate::runtime_budget::resolve(cli) {
        Ok(report) => report,
        Err(error) => {
            return DoctorCheck::failed(
                "runtime_memory_budget",
                format!(
                    "runtime memory budget could not be resolved: {}",
                    error.message
                ),
            );
        }
    };
    let managed_snapshot = execution.memory().snapshot();
    let resolution = &report.resolution;
    let enforcement = if report.has_enforced_memory_authority() {
        "cgroup-enforced"
    } else {
        "not cgroup-enforced"
    };
    let message = format!(
        "process budget {}; managed pool {}; spill budget {}; {enforcement}",
        humanize_bytes(resolution.process_budget_bytes),
        humanize_bytes(resolution.managed_pool_bytes),
        humanize_bytes(resolution.spill_budget_bytes),
    );
    let details = json!({
        "budget": report,
        "managed_memory_snapshot": managed_snapshot,
    });
    if report.has_enforced_memory_authority() {
        DoctorCheck::passed("runtime_memory_budget", message).with_details(details)
    } else {
        DoctorCheck::unsupported("runtime_memory_budget", message).with_details(details)
    }
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

fn source_driver_health_checks(
    context: &ProjectContext,
    registry: &cdf_runtime::SourceRegistry,
    execution: &cdf_runtime::ExecutionServices,
) -> Vec<DoctorCheck> {
    let plans = context
        .resources
        .iter()
        .map(|resource| resource.source_plan().clone())
        .collect::<Vec<_>>();
    let configured_resources = context
        .config
        .resources
        .iter()
        .filter_map(|(resource_id, mapping)| match mapping.source_kind() {
            ResourceSourceKind::Reference { uri } => Some(
                cdf_kernel::ResourceId::new(resource_id.clone()).and_then(|resource_id| {
                    let driver = registry.driver_for_uri(&uri)?;
                    Ok(cdf_runtime::SourceHealthTarget::new(
                        resource_id,
                        driver.descriptor().driver_id.clone(),
                    ))
                }),
            ),
            ResourceSourceKind::DeclarativeFile { .. } => None,
        })
        .collect::<Result<Vec<_>, _>>();
    let configured_resources = match configured_resources {
        Ok(resources) => resources,
        Err(error) => {
            return vec![DoctorCheck::failed(
                "source_health",
                format!("source health inventory is invalid: {}", error.message),
            )];
        }
    };
    let resolution = cdf_runtime::SourceResolutionContext::new(
        &context.root,
        std::sync::Arc::new(context.secret_provider()),
        execution,
        std::sync::Arc::new(cdf_http::EgressAllowlist::allow_any()),
    )
    .with_driver_options(context.config.driver_options.clone());
    match registry.health_checks(
        &resolution,
        &plans,
        &configured_resources,
        cdf_runtime::SourceHealthLimits::default(),
        cdf_runtime::RunCancellation::default(),
    ) {
        Ok(results) => results
            .into_iter()
            .map(|result| {
                let check = match result.status {
                    cdf_runtime::SourceHealthStatus::Passed => {
                        DoctorCheck::passed(result.probe_id, result.message)
                    }
                    cdf_runtime::SourceHealthStatus::Failed => {
                        DoctorCheck::failed(result.probe_id, result.message)
                    }
                    cdf_runtime::SourceHealthStatus::Skipped => {
                        DoctorCheck::skipped(result.probe_id, result.message)
                    }
                    cdf_runtime::SourceHealthStatus::Unsupported => {
                        DoctorCheck::unsupported(result.probe_id, result.message)
                    }
                };
                check.with_details(redact_json_uri_userinfo(result.details))
            })
            .collect(),
        Err(error) => vec![DoctorCheck::failed(
            "source_health",
            format!("source health probes failed: {}", error.message),
        )],
    }
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
            .render(&cdf_cli_core::render::RenderConfig::headless_for_width(96));
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
