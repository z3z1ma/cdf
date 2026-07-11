use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    BenchResult, BenchmarkObservation, BenchmarkReport, Capability, ObservationStatus, bench_error,
    canonical_sha256, host_class, validate_report,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvelopeSpec {
    pub schema_version: u16,
    pub title: String,
    pub evidence_status: String,
    pub evidence_record: Option<String>,
    pub targets: Vec<EnvelopeTarget>,
    pub profile_links: BTreeMap<String, Vec<String>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvelopeTarget {
    pub workload_id: String,
    pub label: String,
    pub target: String,
    pub reference_workload_id: Option<String>,
}

pub fn generate_envelope(report: &BenchmarkReport, spec: &EnvelopeSpec) -> BenchResult<String> {
    validate_report(report)?;
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
