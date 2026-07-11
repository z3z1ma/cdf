use std::{collections::BTreeSet, fs, io::Write, path::Path};

use serde::{Deserialize, Serialize};

use crate::{
    BenchResult, BenchmarkObservation, BenchmarkReport, ComparabilityKey, ObservationStatus,
    ReferenceIdentity, bench_error, canonical_sha256, validate_report,
};

pub const REGRESSION_THRESHOLD_PERCENT: u64 = 10;
pub const HIGH_VARIANCE_MAD_PERCENT: u64 = 10;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComparisonReport {
    pub schema_version: u16,
    pub baseline_report_sha256: String,
    pub current_report_sha256: String,
    pub cells: Vec<ComparisonCell>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComparisonCell {
    pub dataset_id: String,
    pub workload_id: String,
    pub baseline_revision: Option<String>,
    pub current_revision: Option<String>,
    pub verdict: ComparisonVerdict,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ComparisonVerdict {
    Pass {
        baseline_wall_time_ns: u64,
        current_wall_time_ns: u64,
        change_basis_points: i64,
    },
    Regression {
        baseline_wall_time_ns: u64,
        current_wall_time_ns: u64,
        change_basis_points: i64,
        threshold_percent: u64,
    },
    Inconclusive {
        reason: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaselineIndex {
    pub schema_version: u16,
    pub current_report_sha256: String,
    pub entries: Vec<BaselineEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaselineEntry {
    pub report_sha256: String,
    pub report_file: String,
    pub evidence_record: String,
}

pub fn compare_reports(
    baseline: &BenchmarkReport,
    current: &BenchmarkReport,
) -> BenchResult<ComparisonReport> {
    validate_report(baseline)?;
    validate_report(current)?;
    if baseline.schema_version != current.schema_version {
        return Err(bench_error("benchmark report schema versions differ"));
    }
    let mut cells = Vec::new();
    let mut matched_baseline = BTreeSet::new();
    for current_cell in &current.observations {
        let matching = baseline
            .observations
            .iter()
            .enumerate()
            .filter(|(_, baseline_cell)| same_comparison_authority(baseline_cell, current_cell))
            .collect::<Vec<_>>();
        let verdict = match matching.as_slice() {
            [] => ComparisonVerdict::Inconclusive {
                reason: "no baseline cell has the same dataset workload host mode toolchain dependency and reference authority"
                    .to_owned(),
            },
            [(index, baseline_cell)] => {
                matched_baseline.insert(*index);
                compare_cell(baseline_cell, current_cell)
            }
            _ => {
                return Err(bench_error(
                    "baseline contains duplicate normalized comparison authority",
                ));
            }
        };
        cells.push(ComparisonCell {
            dataset_id: current_cell.comparability.dataset_id.clone(),
            workload_id: current_cell.comparability.workload_id.clone(),
            baseline_revision: matching
                .first()
                .map(|(_, cell)| cell.comparability.cdf_revision.clone()),
            current_revision: Some(current_cell.comparability.cdf_revision.clone()),
            verdict,
        });
    }
    for (index, baseline_cell) in baseline.observations.iter().enumerate() {
        if !matched_baseline.contains(&index) {
            cells.push(ComparisonCell {
                dataset_id: baseline_cell.comparability.dataset_id.clone(),
                workload_id: baseline_cell.comparability.workload_id.clone(),
                baseline_revision: Some(baseline_cell.comparability.cdf_revision.clone()),
                current_revision: None,
                verdict: ComparisonVerdict::Inconclusive {
                    reason: "current report is missing the baseline cell".to_owned(),
                },
            });
        }
    }
    cells.sort_by(|left, right| {
        (&left.dataset_id, &left.workload_id, &left.current_revision).cmp(&(
            &right.dataset_id,
            &right.workload_id,
            &right.current_revision,
        ))
    });
    Ok(ComparisonReport {
        schema_version: 1,
        baseline_report_sha256: canonical_sha256(baseline)?,
        current_report_sha256: canonical_sha256(current)?,
        cells,
    })
}

pub fn comparison_fails(report: &ComparisonReport) -> bool {
    report
        .cells
        .iter()
        .any(|cell| matches!(cell.verdict, ComparisonVerdict::Regression { .. }))
}

pub fn install_baseline(
    baseline_root: &Path,
    repository_root: &Path,
    report: &BenchmarkReport,
    evidence_record: &str,
) -> BenchResult<BaselineIndex> {
    validate_report(report)?;
    validate_evidence_reference(repository_root, evidence_record)?;
    fs::create_dir_all(baseline_root)?;
    let digest = canonical_sha256(report)?;
    let report_file = format!("report-{}.json", &digest[7..]);
    let report_bytes = cdf_package::canonical_json_bytes(report)?;
    write_create_or_verify(&baseline_root.join(&report_file), &report_bytes)?;

    let index_path = baseline_root.join("baseline-index.json");
    let mut index = if index_path.exists() {
        let index = serde_json::from_slice::<BaselineIndex>(&fs::read(&index_path)?)?;
        validate_baseline_index(baseline_root, repository_root, &index)?;
        index
    } else {
        BaselineIndex {
            schema_version: 1,
            current_report_sha256: digest.clone(),
            entries: Vec::new(),
        }
    };
    if index.schema_version != 1 {
        return Err(bench_error("unsupported baseline index schema version"));
    }
    if !index
        .entries
        .iter()
        .any(|entry| entry.report_sha256 == digest)
    {
        index.entries.push(BaselineEntry {
            report_sha256: digest.clone(),
            report_file,
            evidence_record: evidence_record.to_owned(),
        });
    }
    index.current_report_sha256 = digest;
    validate_baseline_index(baseline_root, repository_root, &index)?;
    write_atomic_replace(&index_path, &cdf_package::canonical_json_bytes(&index)?)?;
    Ok(index)
}

fn compare_cell(
    baseline: &BenchmarkObservation,
    current: &BenchmarkObservation,
) -> ComparisonVerdict {
    if !matches!(baseline.status, ObservationStatus::Observed)
        || !matches!(current.status, ObservationStatus::Observed)
    {
        return ComparisonVerdict::Inconclusive {
            reason: "both baseline and current cells must be observed".to_owned(),
        };
    }
    let same_work = baseline
        .samples
        .first()
        .zip(current.samples.first())
        .is_some_and(|(baseline, current)| {
            baseline.rows == current.rows
                && baseline.logical_bytes == current.logical_bytes
                && baseline.physical_bytes == current.physical_bytes
        });
    if !same_work {
        return ComparisonVerdict::Inconclusive {
            reason: "baseline and current cells measured different row or byte authorities"
                .to_owned(),
        };
    }
    let Some(baseline_summary) = &baseline.summary else {
        return ComparisonVerdict::Inconclusive {
            reason: "baseline summary is missing".to_owned(),
        };
    };
    let Some(current_summary) = &current.summary else {
        return ComparisonVerdict::Inconclusive {
            reason: "current summary is missing".to_owned(),
        };
    };
    if high_variance(
        baseline_summary.median_wall_time_ns,
        baseline_summary.median_absolute_deviation_ns,
    ) || high_variance(
        current_summary.median_wall_time_ns,
        current_summary.median_absolute_deviation_ns,
    ) {
        return ComparisonVerdict::Inconclusive {
            reason: "median absolute deviation exceeds 10 percent of median wall time".to_owned(),
        };
    }
    let baseline_wall = baseline_summary.median_wall_time_ns;
    let current_wall = current_summary.median_wall_time_ns;
    let change_basis_points = change_basis_points(baseline_wall, current_wall);
    if u128::from(current_wall).saturating_mul(100)
        > u128::from(baseline_wall).saturating_mul(u128::from(100 + REGRESSION_THRESHOLD_PERCENT))
    {
        ComparisonVerdict::Regression {
            baseline_wall_time_ns: baseline_wall,
            current_wall_time_ns: current_wall,
            change_basis_points,
            threshold_percent: REGRESSION_THRESHOLD_PERCENT,
        }
    } else {
        ComparisonVerdict::Pass {
            baseline_wall_time_ns: baseline_wall,
            current_wall_time_ns: current_wall,
            change_basis_points,
        }
    }
}

fn same_comparison_authority(
    baseline: &BenchmarkObservation,
    current: &BenchmarkObservation,
) -> bool {
    let ComparabilityKey {
        dataset_id: baseline_dataset,
        workload_id: baseline_workload,
        timed_region_version: baseline_timed_region,
        cdf_revision: _,
        dependency_tuple: baseline_dependencies,
        host_class: baseline_host,
        os_toolchain: baseline_toolchain,
        io_mode: baseline_mode,
    } = &baseline.comparability;
    let key = &current.comparability;
    baseline_dataset == &key.dataset_id
        && baseline_workload == &key.workload_id
        && baseline_timed_region == &key.timed_region_version
        && baseline_dependencies == &key.dependency_tuple
        && baseline_host == &key.host_class
        && baseline_toolchain == &key.os_toolchain
        && baseline_mode == &key.io_mode
        && same_reference(&baseline.reference, &current.reference)
}

fn same_reference(
    baseline: &Option<ReferenceIdentity>,
    current: &Option<ReferenceIdentity>,
) -> bool {
    baseline == current
}

fn high_variance(median: u64, mad: u64) -> bool {
    u128::from(mad).saturating_mul(100)
        > u128::from(median).saturating_mul(u128::from(HIGH_VARIANCE_MAD_PERCENT))
}

fn change_basis_points(baseline: u64, current: u64) -> i64 {
    let delta = i128::from(current) - i128::from(baseline);
    let basis_points = delta.saturating_mul(10_000) / i128::from(baseline);
    i64::try_from(basis_points).unwrap_or_else(|_| {
        if basis_points.is_negative() {
            i64::MIN
        } else {
            i64::MAX
        }
    })
}

fn validate_evidence_reference(repository_root: &Path, reference: &str) -> BenchResult<()> {
    let path = Path::new(reference);
    if !reference.starts_with(".10x/evidence/")
        || path.extension().and_then(|value| value.to_str()) != Some("md")
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir | std::path::Component::RootDir
            )
        })
        || !repository_root.join(path).is_file()
    {
        return Err(bench_error(
            "baseline replacement requires an existing .10x evidence record",
        ));
    }
    let evidence_root = repository_root.join(".10x/evidence").canonicalize()?;
    let evidence = repository_root.join(path).canonicalize()?;
    if !evidence.starts_with(evidence_root) {
        return Err(bench_error(
            "baseline evidence record resolves outside the evidence directory",
        ));
    }
    Ok(())
}

fn validate_baseline_index(
    baseline_root: &Path,
    repository_root: &Path,
    index: &BaselineIndex,
) -> BenchResult<()> {
    if index.schema_version != 1 || index.entries.is_empty() {
        return Err(bench_error("invalid or empty baseline index"));
    }
    let mut digests = BTreeSet::new();
    for entry in &index.entries {
        if !digests.insert(entry.report_sha256.as_str())
            || Path::new(&entry.report_file).components().count() != 1
        {
            return Err(bench_error(
                "baseline index contains duplicate digests or unsafe report paths",
            ));
        }
        validate_evidence_reference(repository_root, &entry.evidence_record)?;
        let report: BenchmarkReport =
            serde_json::from_slice(&fs::read(baseline_root.join(&entry.report_file))?)?;
        validate_report(&report)?;
        if canonical_sha256(&report)? != entry.report_sha256 {
            return Err(bench_error(
                "baseline report bytes do not match indexed content identity",
            ));
        }
    }
    if !digests.contains(index.current_report_sha256.as_str()) {
        return Err(bench_error(
            "baseline index current report is not present in preserved history",
        ));
    }
    Ok(())
}

fn write_create_or_verify(path: &Path, bytes: &[u8]) -> BenchResult<()> {
    match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(mut file) => {
            file.write_all(bytes)?;
            file.sync_all()?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if fs::read(path)? == bytes {
                Ok(())
            } else {
                Err(bench_error(
                    "content-addressed baseline report conflicts with existing bytes",
                ))
            }
        }
        Err(error) => Err(error.into()),
    }
}

fn write_atomic_replace(path: &Path, bytes: &[u8]) -> BenchResult<()> {
    let temp = path.with_extension(format!("tmp-{}", std::process::id()));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&temp, path)?;
    if let Some(parent) = path.parent() {
        fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}
