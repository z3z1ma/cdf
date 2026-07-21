use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::{
    BenchResult, BenchmarkObservation, BenchmarkReport, Capability, DestinationPathEligibility,
    ObservationStatus, bench_error, canonical_sha256, host_class, validate_report,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvelopeSpec {
    pub schema_version: u16,
    pub title: String,
    pub evidence_status: String,
    pub evidence_record: Option<String>,
    pub targets: Vec<EnvelopeTarget>,
    #[serde(default)]
    pub destination_paths: Vec<DestinationEnvelopeTarget>,
    pub profile_links: BTreeMap<String, Vec<String>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvelopeTarget {
    pub workload_id: String,
    pub label: String,
    pub target: String,
    pub reference_workload_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationEnvelopeTarget {
    pub destination_id: String,
    pub target: String,
    pub eligible_schema_fixture: String,
    pub ineligible_schema_fixture: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationBulkCatalogEntry {
    pub destination_id: String,
    pub runtime: cdf_runtime::DestinationRuntimeCapabilities,
}

pub fn destination_execution_descriptor_sha256(
    descriptor: &cdf_runtime::BulkPathDescriptor,
) -> BenchResult<String> {
    let mut execution = serde_json::to_value(descriptor)?;
    let object = execution
        .as_object_mut()
        .ok_or_else(|| bench_error("bulk path descriptor must serialize as an object"))?;
    object.remove("schema_preflight_version");
    object.remove("measured_evidence_version");
    canonical_sha256(&execution)
}

impl From<&cdf_runtime::DestinationInspection> for DestinationBulkCatalogEntry {
    fn from(inspection: &cdf_runtime::DestinationInspection) -> Self {
        Self {
            destination_id: inspection.description.destination_id.as_str().to_owned(),
            runtime: inspection.runtime.clone(),
        }
    }
}

pub fn generate_envelope(
    report: &BenchmarkReport,
    spec: &EnvelopeSpec,
    destination_catalog: &[DestinationBulkCatalogEntry],
    destination_report: &BenchmarkReport,
    evidence_root: &Path,
) -> BenchResult<String> {
    validate_report(report)?;
    validate_report(destination_report)?;
    validate_envelope_spec(spec)?;
    let report_digest = canonical_sha256(report)?;
    let mut output = String::new();
    output.push_str(&format!("# {}\n\n", spec.title));
    output.push_str(&format!("> **{}**\n\n", escape(&spec.evidence_status)));
    output.push_str(
        "This document is generated from the machine report; edit its inputs, not this file.\n\n",
    );
    output.push_str("## Evidence authority\n\n");
    output.push_str(&format!("- Report: `{report_digest}`\n"));
    output.push_str(&format!("- Host class: `{}`\n", host_class(&report.host)?));
    output.push_str(&format!(
        "- Host: {} / {} logical cores / {} {} / Rust {}\n",
        escape(&report.host.architecture),
        report.host.advertised_logical_cores,
        escape(&report.host.os.family),
        escape(&report.host.os.version),
        escape(&report.host.rust_version)
    ));
    output.push_str(&format!(
        "- Effective CPU: {}\n",
        cpu_capability_summary(&report.host.effective_cpu)
    ));
    output.push_str(&format!(
        "- Effective memory: {}\n",
        memory_capability_summary(&report.host.effective_memory_bytes)
    ));
    output.push_str(&format!(
        "- Storage: {}\n",
        storage_capability_summary(&report.host.storage)
    ));
    if let Some(evidence) = &spec.evidence_record {
        output.push_str(&format!("- Evidence record: `{}`\n", escape(evidence)));
    }

    output.push_str("\n## Performance envelope\n\n");
    output.push_str("| Workload | Target | Observation | Roofline ratio | Evidence overhead | Peak RSS | Status |\n");
    output.push_str("|---|---:|---:|---:|---:|---:|---|\n");
    for target in &spec.targets {
        let observations = report
            .observations
            .iter()
            .filter(|observation| observation.comparability.workload_id == target.workload_id)
            .collect::<Vec<_>>();
        if observations.is_empty() {
            envelope_row(&mut output, target, None, None);
        } else {
            for observation in observations {
                let reference = target.reference_workload_id.as_ref().and_then(|id| {
                    report
                        .observations
                        .iter()
                        .find(|candidate| candidate.comparability.workload_id == *id)
                });
                envelope_row(&mut output, target, Some(observation), reference);
            }
        }
    }

    if !spec.destination_paths.is_empty() {
        output.push_str("\n## Destination bulk-path matrix\n\n");
        output.push_str("| Destination | Path | Cell | Evidence version | Host class | Target | Observation | Status | Evidence |\n");
        output.push_str("|---|---|---|---|---|---:|---:|---|---|\n");
        if spec.destination_paths.len() != destination_catalog.len()
            || spec.destination_paths.iter().any(|target| {
                !destination_catalog
                    .iter()
                    .any(|entry| entry.destination_id == target.destination_id)
            })
        {
            return Err(bench_error(
                "destination envelope targets must exactly cover the registered destination catalog",
            ));
        }
        validate_destination_observation_joins(
            destination_report,
            destination_catalog,
            &spec.destination_paths,
            evidence_root,
        )?;
        for entry in destination_catalog {
            let target = spec
                .destination_paths
                .iter()
                .find(|target| target.destination_id == entry.destination_id)
                .ok_or_else(|| {
                    bench_error(format!(
                        "destination envelope has no target for registered destination {}",
                        entry.destination_id
                    ))
                })?;
            for path in &entry.runtime.bulk_paths {
                for eligibility in [
                    DestinationPathEligibility::Eligible,
                    DestinationPathEligibility::Ineligible,
                ] {
                    destination_path_row(
                        &mut output,
                        destination_report,
                        entry,
                        path,
                        eligibility,
                        target,
                    )?;
                }
            }
        }
    }

    output.push_str("\n## Bias and unavailable evidence\n\n");
    for observation in &report.observations {
        let status = status_text(observation);
        let bias = if observation.bias.is_empty() {
            "none recorded".to_owned()
        } else {
            observation
                .bias
                .iter()
                .map(|bias| format!("{}: {}", escape(&bias.code), escape(&bias.description)))
                .collect::<Vec<_>>()
                .join("; ")
        };
        output.push_str(&format!(
            "- `{}` ({}): {}; bias: {}\n",
            escape(&observation.comparability.workload_id),
            io_mode(observation),
            status,
            bias
        ));
    }

    output.push_str("\n## Profiles\n\n");
    if spec.profile_links.is_empty() {
        output.push_str("No profile artifacts are attached to this report.\n");
    } else {
        for (workload, links) in &spec.profile_links {
            output.push_str(&format!(
                "- `{}`: {}\n",
                escape(workload),
                links
                    .iter()
                    .map(|link| format!("[artifact]({})", escape(link)))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
    }
    Ok(output)
}

fn envelope_row(
    output: &mut String,
    target: &EnvelopeTarget,
    observation: Option<&BenchmarkObservation>,
    reference: Option<&BenchmarkObservation>,
) {
    let (observed, rss, status) = observation.map_or_else(
        || {
            (
                "—".to_owned(),
                "—".to_owned(),
                "unavailable: no report cell".to_owned(),
            )
        },
        |observation| {
            let summary = observation.summary.as_ref();
            (
                summary.map_or_else(|| "—".to_owned(), format_rate),
                summary
                    .and_then(|summary| summary.peak_rss_bytes)
                    .map_or_else(|| "—".to_owned(), format_bytes),
                status_text(observation),
            )
        },
    );
    let (ratio, overhead) = observation
        .zip(reference)
        .and_then(|(observation, reference)| {
            let observed_summary = observation.summary.as_ref()?;
            let reference_summary = reference.summary.as_ref()?;
            let observed_work = observation.samples.first()?;
            let reference_work = reference.samples.first()?;
            if observed_work.rows != reference_work.rows
                || observed_work.physical_bytes != reference_work.physical_bytes
            {
                return Some((
                    "inconclusive: work mismatch".to_owned(),
                    "inconclusive: work mismatch".to_owned(),
                ));
            }
            Some((
                format_ratio(
                    observed_summary.median_wall_time_ns,
                    reference_summary.median_wall_time_ns,
                ),
                format_overhead(
                    observed_summary.median_wall_time_ns,
                    reference_summary.median_wall_time_ns,
                ),
            ))
        })
        .unwrap_or_else(|| ("—".to_owned(), "—".to_owned()));
    output.push_str(&format!(
        "| {} | {} | {} | {} | {} | {} | {} |\n",
        escape(&target.label),
        escape(&target.target),
        observed,
        ratio,
        overhead,
        rss,
        escape(&status)
    ));
}

fn validate_envelope_spec(spec: &EnvelopeSpec) -> BenchResult<()> {
    if spec.schema_version != 1 || spec.title.trim().is_empty() || spec.targets.is_empty() {
        return Err(bench_error(
            "invalid envelope spec version title or targets",
        ));
    }
    let mut ids = std::collections::BTreeSet::new();
    for target in &spec.targets {
        if target.workload_id.trim().is_empty()
            || target.label.trim().is_empty()
            || target.target.trim().is_empty()
            || !ids.insert(target.workload_id.as_str())
        {
            return Err(bench_error(
                "envelope targets require unique non-empty workload ids labels and targets",
            ));
        }
    }
    let mut destination_ids = std::collections::BTreeSet::new();
    for path in &spec.destination_paths {
        if path.destination_id.trim().is_empty()
            || path.target.trim().is_empty()
            || path.eligible_schema_fixture.trim().is_empty()
            || path.ineligible_schema_fixture.trim().is_empty()
            || !destination_ids.insert(path.destination_id.as_str())
        {
            return Err(bench_error(
                "destination envelope targets require unique destination ids and non-empty targets",
            ));
        }
    }
    Ok(())
}

fn validate_destination_observation_joins(
    report: &BenchmarkReport,
    catalog: &[DestinationBulkCatalogEntry],
    targets: &[DestinationEnvelopeTarget],
    evidence_root: &Path,
) -> BenchResult<()> {
    let report_host_class = host_class(&report.host)?;
    let mut cells = std::collections::BTreeSet::new();
    for observation in &report.observations {
        let identity = observation.destination_path.as_ref().ok_or_else(|| {
            bench_error("destination matrix report contains a non-destination observation")
        })?;
        if observation.comparability.host_class != report_host_class {
            return Err(bench_error(format!(
                "destination observation {} host class {} differs from report host class {}",
                identity.path_id, observation.comparability.host_class, report_host_class
            )));
        }
        let matching_path = catalog
            .iter()
            .find(|entry| entry.destination_id == identity.destination_id)
            .and_then(|entry| {
                entry
                    .runtime
                    .bulk_paths
                    .iter()
                    .find(|path| path.path_id == identity.path_id)
            });
        let exact = if let Some(path) = matching_path {
            path.measured_evidence_version.as_deref() == Some(identity.evidence_version.as_str())
                && path.schema_preflight_version == identity.schema_preflight_version
                && destination_execution_descriptor_sha256(path)?
                    == identity.execution_descriptor_sha256
        } else {
            false
        };
        if !exact {
            return Err(bench_error(format!(
                "destination observation {}/{} evidence {} does not exactly match a registry descriptor",
                identity.destination_id, identity.path_id, identity.evidence_version
            )));
        }
        let target = targets
            .iter()
            .find(|target| target.destination_id == identity.destination_id)
            .ok_or_else(|| {
                bench_error(format!(
                    "destination observation {} has no envelope target",
                    identity.destination_id
                ))
            })?;
        let expected_fixture = match identity.eligibility {
            DestinationPathEligibility::Eligible => target.eligible_schema_fixture.as_str(),
            DestinationPathEligibility::Ineligible => target.ineligible_schema_fixture.as_str(),
        };
        match (identity.eligibility, &observation.status) {
            (DestinationPathEligibility::Eligible, ObservationStatus::Observed)
            | (DestinationPathEligibility::Ineligible, ObservationStatus::Ineligible { .. }) => {}
            (DestinationPathEligibility::Eligible, _) => {
                return Err(bench_error(format!(
                    "destination observation {}/{} eligible cell must be observed",
                    identity.destination_id, identity.path_id
                )));
            }
            (DestinationPathEligibility::Ineligible, _) => {
                return Err(bench_error(format!(
                    "destination observation {}/{} ineligible cell must record a schema-preflight rejection",
                    identity.destination_id, identity.path_id
                )));
            }
        }
        if identity.schema_fixture != expected_fixture {
            return Err(bench_error(format!(
                "destination observation {}/{} schema fixture {} does not match target cell {}",
                identity.destination_id,
                identity.path_id,
                identity.schema_fixture,
                expected_fixture
            )));
        }
        let eligibility = match identity.eligibility {
            DestinationPathEligibility::Eligible => "eligible",
            DestinationPathEligibility::Ineligible => "ineligible",
        };
        if !cells.insert((
            identity.destination_id.as_str(),
            identity.path_id.as_str(),
            identity.evidence_version.as_str(),
            identity.execution_descriptor_sha256.as_str(),
            identity.schema_preflight_version.as_str(),
            eligibility,
            identity.schema_fixture.as_str(),
        )) {
            return Err(bench_error(format!(
                "destination observation cell {}/{} ({eligibility}, {}) is duplicated",
                identity.destination_id, identity.path_id, identity.schema_fixture
            )));
        }
        if !evidence_root.join(&identity.evidence_record).is_file() {
            return Err(bench_error(format!(
                "destination observation evidence record {} does not exist",
                identity.evidence_record
            )));
        }
    }
    for entry in catalog {
        let target = targets
            .iter()
            .find(|target| target.destination_id == entry.destination_id)
            .expect("catalog/spec coverage validated before destination observation joins");
        for path in &entry.runtime.bulk_paths {
            let evidence = path.measured_evidence_version.as_deref().ok_or_else(|| {
                bench_error(format!(
                    "destination path {}/{} has no measured evidence version",
                    entry.destination_id, path.path_id
                ))
            })?;
            let execution_descriptor_sha256 = destination_execution_descriptor_sha256(path)?;
            for (eligibility, fixture) in [
                ("eligible", target.eligible_schema_fixture.as_str()),
                ("ineligible", target.ineligible_schema_fixture.as_str()),
            ] {
                if !cells.contains(&(
                    entry.destination_id.as_str(),
                    path.path_id.as_str(),
                    evidence,
                    execution_descriptor_sha256.as_str(),
                    path.schema_preflight_version.as_str(),
                    eligibility,
                    fixture,
                )) {
                    return Err(bench_error(format!(
                        "destination matrix is missing {eligibility} cell for {}/{} fixture {}",
                        entry.destination_id, path.path_id, fixture
                    )));
                }
            }
        }
    }
    Ok(())
}

fn destination_path_row(
    output: &mut String,
    report: &BenchmarkReport,
    entry: &DestinationBulkCatalogEntry,
    path: &cdf_runtime::BulkPathDescriptor,
    eligibility: DestinationPathEligibility,
    target: &DestinationEnvelopeTarget,
) -> BenchResult<()> {
    let execution_descriptor_sha256 = destination_execution_descriptor_sha256(path)?;
    let observation = report.observations.iter().find(|observation| {
        observation
            .destination_path
            .as_ref()
            .is_some_and(|identity| {
                identity.destination_id == entry.destination_id
                    && identity.path_id == path.path_id
                    && path.measured_evidence_version.as_deref()
                        == Some(identity.evidence_version.as_str())
                    && execution_descriptor_sha256 == identity.execution_descriptor_sha256
                    && path.schema_preflight_version == identity.schema_preflight_version
                    && identity.eligibility == eligibility
                    && identity.schema_fixture
                        == match eligibility {
                            DestinationPathEligibility::Eligible => {
                                target.eligible_schema_fixture.as_str()
                            }
                            DestinationPathEligibility::Ineligible => {
                                target.ineligible_schema_fixture.as_str()
                            }
                        }
            })
    });
    let (host, observed, status, evidence) = observation.map_or_else(
        || {
            (
                "—".to_owned(),
                "—".to_owned(),
                "unavailable: no exact registry-bound machine observation".to_owned(),
                "—".to_owned(),
            )
        },
        |observation| {
            let identity = observation.destination_path.as_ref().unwrap();
            (
                observation.comparability.host_class.clone(),
                observation
                    .summary
                    .as_ref()
                    .map_or_else(|| "—".to_owned(), format_rate),
                status_text(observation),
                format!("[record](../{})", escape(&identity.evidence_record)),
            )
        },
    );
    let cell = match eligibility {
        DestinationPathEligibility::Eligible => {
            format!("eligible ({})", target.eligible_schema_fixture)
        }
        DestinationPathEligibility::Ineligible => {
            format!("schema-ineligible ({})", target.ineligible_schema_fixture)
        }
    };
    output.push_str(&format!(
        "| {} | `{}` | {} | `{}` | `{}` | {} | {} | {} | {} |\n",
        escape(&entry.destination_id),
        escape(&path.path_id),
        escape(&cell),
        escape(
            path.measured_evidence_version
                .as_deref()
                .unwrap_or("unmeasured")
        ),
        escape(&host),
        escape(&target.target),
        escape(&observed),
        escape(&status),
        evidence,
    ));
    Ok(())
}

fn format_rate(summary: &crate::MeasurementSummary) -> String {
    if summary.median_physical_bytes_per_second > 0 {
        format!(
            "{:.2} MiB/s",
            summary.median_physical_bytes_per_second as f64 / (1024.0 * 1024.0)
        )
    } else {
        format!("{} rows/s", summary.median_rows_per_second)
    }
}

fn format_ratio(observed_wall: u64, reference_wall: u64) -> String {
    if observed_wall == 0 {
        return "—".to_owned();
    }
    format!("{:.3}×", reference_wall as f64 / observed_wall as f64)
}

fn format_overhead(observed_wall: u64, reference_wall: u64) -> String {
    if reference_wall == 0 {
        return "—".to_owned();
    }
    let percent = (observed_wall as f64 - reference_wall as f64) * 100.0 / reference_wall as f64;
    format!("{percent:+.1}%")
}

fn format_bytes(bytes: u64) -> String {
    format!("{:.2} MiB", bytes as f64 / (1024.0 * 1024.0))
}

fn status_text(observation: &BenchmarkObservation) -> String {
    match &observation.status {
        ObservationStatus::Observed => "observed".to_owned(),
        ObservationStatus::Ineligible { reason } => format!("ineligible: {}", escape(reason)),
        ObservationStatus::Failed { error } => format!("failed: {}", escape(error)),
        ObservationStatus::TimedOut { timeout_ms } => format!("timed out: {timeout_ms} ms"),
        ObservationStatus::Unavailable { reason } => {
            format!("unavailable: {}", escape(reason))
        }
        ObservationStatus::Inconclusive { reason } => {
            format!("inconclusive: {}", escape(reason))
        }
    }
}

fn cpu_capability_summary(capability: &Capability<crate::EffectiveCpu>) -> String {
    match capability {
        Capability::Supported { value, method, .. } => {
            format!(
                "supported via {}: {} logical, quota {}, affinity {}",
                escape(method),
                value.logical_cores,
                value
                    .quota_millicores
                    .map_or_else(|| "unbounded".to_owned(), |value| format!("{value}m")),
                value
                    .affinity_cores
                    .map_or_else(|| "unknown".to_owned(), |value| value.to_string())
            )
        }
        Capability::Unavailable { reason, method, .. } => {
            format!("unavailable via {}: {}", escape(method), escape(reason))
        }
        Capability::Failed { error, method, .. } => {
            format!("failed via {}: {}", escape(method), escape(error))
        }
    }
}

fn memory_capability_summary(capability: &Capability<u64>) -> String {
    match capability {
        Capability::Supported { value, method, .. } => {
            format!("supported via {}: {}", escape(method), format_bytes(*value))
        }
        Capability::Unavailable { reason, method, .. } => {
            format!("unavailable via {}: {}", escape(method), escape(reason))
        }
        Capability::Failed { error, method, .. } => {
            format!("failed via {}: {}", escape(method), escape(error))
        }
    }
}

fn storage_capability_summary(capability: &Capability<crate::StorageClass>) -> String {
    match capability {
        Capability::Supported { value, method, .. } => format!(
            "supported via {}: {} / {} / {}",
            escape(method),
            escape(&value.medium),
            escape(&value.filesystem),
            escape(&value.label)
        ),
        Capability::Unavailable { reason, method, .. } => {
            format!("unavailable via {}: {}", escape(method), escape(reason))
        }
        Capability::Failed { error, method, .. } => {
            format!("failed via {}: {}", escape(method), escape(error))
        }
    }
}

fn io_mode(observation: &BenchmarkObservation) -> &'static str {
    match observation.comparability.io_mode {
        crate::IoMode::Warm => "warm",
        crate::IoMode::Cold => "cold",
        crate::IoMode::Uncontrolled => "uncontrolled",
    }
}

fn escape(value: &str) -> String {
    value.replace('|', "\\|").replace('\n', " ")
}
