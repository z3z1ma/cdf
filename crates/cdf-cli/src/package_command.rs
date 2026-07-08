use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use cdf_kernel::{CdfError, PackageHash};
use cdf_package::{MANIFEST_FILE, PackageReader, PackageStatus};
use cdf_state_sqlite::SqliteCheckpointStore;
use serde::Serialize;
use serde_json::json;

use crate::{
    args::{Cli, PackageArchiveArgs, PackageCommand},
    commands::output,
    context::ProjectContext,
    output::{CliError, CommandOutput},
};

pub(crate) fn package(cli: &Cli, command: PackageCommand) -> Result<CommandOutput, CliError> {
    match command {
        PackageCommand::Ls { packages_dir } => {
            let root = match packages_dir {
                Some(path) => path,
                None => {
                    ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?.package_root()
                }
            };
            let packages = list_packages(root)?;
            output(
                "package ls",
                format!("{} package(s)", packages.len()),
                json!({ "packages": packages }),
            )
        }
        PackageCommand::Gc { packages_dir } => {
            let report = package_gc_plan(cli, packages_dir)?;
            output(
                "package gc",
                format!(
                    "planned package gc for {}: dry-run, {} artifact(s), {} collectible, {} protected, {} corrupt, {} missing",
                    report.package_root,
                    report.artifacts.len(),
                    report.counts.collectible,
                    report.counts.protected,
                    report.counts.corrupt,
                    report.counts.missing
                ),
                report,
            )
        }
        PackageCommand::Verify { package_dir } => {
            let reader = PackageReader::open(&package_dir)?;
            let report = reader.verify()?;
            output(
                "package verify",
                format!(
                    "verified package {}: {} file(s), {} archive segment(s)",
                    report.package_hash,
                    report.checked_files.len(),
                    report.checked_archives.len()
                ),
                PackageVerifyReport {
                    package_hash: report.package_hash,
                    checked_files: report.checked_files,
                    checked_archives: report.checked_archives,
                },
            )
        }
        PackageCommand::Archive(args) => package_archive(args),
    }
}

fn package_gc_plan(
    cli: &Cli,
    packages_dir: Option<PathBuf>,
) -> Result<PackageGcPlanReport, CliError> {
    let context = if packages_dir.is_none() || cli.project.is_some() {
        Some(ProjectContext::load(
            cli.project.as_ref(),
            cli.env.as_deref(),
        )?)
    } else {
        None
    };
    let root = packages_dir.unwrap_or_else(|| {
        context
            .as_ref()
            .expect("context loaded when package gc has no explicit directory")
            .package_root()
    });
    let protected_hashes = match context.as_ref() {
        Some(context) => committed_package_hashes(context)?,
        None => BTreeSet::new(),
    };
    let artifacts = plan_package_gc_artifacts(&root, &protected_hashes)?;
    let counts = PackageGcCounts::from_artifacts(&artifacts);
    Ok(PackageGcPlanReport {
        command: "package gc",
        package_root: root.display().to_string(),
        mode: "dry_run",
        artifacts,
        counts,
    })
}

fn committed_package_hashes(context: &ProjectContext) -> Result<BTreeSet<PackageHash>, CliError> {
    let path = context.state_store_path()?;
    if !path.exists() {
        return Ok(BTreeSet::new());
    }
    SqliteCheckpointStore::open_read_only(path)?
        .committed_package_hashes()
        .map_err(CliError::from)
}

fn plan_package_gc_artifacts(
    root: &Path,
    protected_hashes: &BTreeSet<PackageHash>,
) -> Result<Vec<PackageGcArtifact>, CliError> {
    let mut artifacts = Vec::new();
    let mut readable_hashes = BTreeSet::new();
    if root.exists() {
        for entry in sorted_child_entries(root)? {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let artifact = classify_package_artifact(&path, protected_hashes);
            if let Some(hash) = artifact.package_hash.as_deref() {
                readable_hashes.insert(hash.to_owned());
            }
            artifacts.push(artifact);
        }
    }

    for protected_hash in protected_hashes {
        if !readable_hashes.contains(protected_hash.as_str()) {
            artifacts.push(PackageGcArtifact {
                package_path: None,
                package_hash: Some(protected_hash.as_str().to_owned()),
                classification: PackageGcClassification::Missing,
                retention_reason: "committed_checkpoint_missing_artifact",
                planned_action: PackageGcPlannedAction::RestoreRequired,
            });
        }
    }

    artifacts.sort_by(|left, right| {
        (
            left.package_path.as_deref().unwrap_or(""),
            left.package_hash.as_deref().unwrap_or(""),
            left.retention_reason,
        )
            .cmp(&(
                right.package_path.as_deref().unwrap_or(""),
                right.package_hash.as_deref().unwrap_or(""),
                right.retention_reason,
            ))
    });
    Ok(artifacts)
}

fn classify_package_artifact(
    package_dir: &Path,
    protected_hashes: &BTreeSet<PackageHash>,
) -> PackageGcArtifact {
    let package_path = Some(package_dir.display().to_string());
    if !package_dir.join(MANIFEST_FILE).exists() {
        return PackageGcArtifact {
            package_path,
            package_hash: None,
            classification: PackageGcClassification::Corrupt,
            retention_reason: "manifest_missing",
            planned_action: PackageGcPlannedAction::Retain,
        };
    }

    let manifest = match cdf_package::read_manifest(package_dir) {
        Ok(manifest) => manifest,
        Err(_) => {
            return PackageGcArtifact {
                package_path,
                package_hash: None,
                classification: PackageGcClassification::Corrupt,
                retention_reason: "manifest_unreadable",
                planned_action: PackageGcPlannedAction::Retain,
            };
        }
    };
    let package_hash = Some(manifest.package_hash.clone());

    if manifest.lifecycle.status == PackageStatus::Archived {
        return PackageGcArtifact {
            package_path,
            package_hash,
            classification: PackageGcClassification::Protected,
            retention_reason: "retention_tombstone",
            planned_action: PackageGcPlannedAction::Retain,
        };
    }

    let protected_by_checkpoint = PackageHash::new(manifest.package_hash.clone())
        .is_ok_and(|hash| protected_hashes.contains(&hash));
    if cdf_package::verify_package(package_dir).is_err() {
        return PackageGcArtifact {
            package_path,
            package_hash,
            classification: PackageGcClassification::Corrupt,
            retention_reason: if protected_by_checkpoint {
                "committed_checkpoint_verification_failed"
            } else {
                "verification_failed"
            },
            planned_action: PackageGcPlannedAction::Retain,
        };
    }

    if protected_by_checkpoint {
        return PackageGcArtifact {
            package_path,
            package_hash,
            classification: PackageGcClassification::Protected,
            retention_reason: "committed_checkpoint",
            planned_action: PackageGcPlannedAction::Retain,
        };
    }
    match cdf_package::read_receipts(package_dir) {
        Ok(receipts) if !receipts.is_empty() => {
            return PackageGcArtifact {
                package_path,
                package_hash,
                classification: PackageGcClassification::Protected,
                retention_reason: "package_receipt",
                planned_action: PackageGcPlannedAction::Retain,
            };
        }
        Ok(_) => {}
        Err(_) => {
            return PackageGcArtifact {
                package_path,
                package_hash,
                classification: PackageGcClassification::Corrupt,
                retention_reason: "receipt_unreadable",
                planned_action: PackageGcPlannedAction::Retain,
            };
        }
    }

    if matches!(
        manifest.lifecycle.status,
        PackageStatus::Planned | PackageStatus::Extracting | PackageStatus::Validated
    ) {
        PackageGcArtifact {
            package_path,
            package_hash,
            classification: PackageGcClassification::Collectible,
            retention_reason: "pre_packaged_artifact",
            planned_action: PackageGcPlannedAction::WouldCollect,
        }
    } else {
        PackageGcArtifact {
            package_path,
            package_hash,
            classification: PackageGcClassification::Retained,
            retention_reason: "replay_or_recovery_artifact",
            planned_action: PackageGcPlannedAction::Retain,
        }
    }
}

fn package_archive(args: PackageArchiveArgs) -> Result<CommandOutput, CliError> {
    if args.format != "parquet" {
        return Err(CliError::usage(format!(
            "unsupported package archive format `{}`",
            args.format
        )));
    }

    let report = cdf_package::persist_package_parquet_archive(&args.package_dir, args.force)?;
    let archive_byte_count = report
        .segments
        .iter()
        .map(|segment| segment.archive_byte_count)
        .sum::<u64>();
    output(
        "package archive",
        format!(
            "archived package {} as parquet: status {}, {} segment(s), {} byte(s), fidelity {}",
            report.package_hash,
            package_archive_status(&report.status),
            report.segments.len(),
            archive_byte_count,
            report.fidelity_report_path
        ),
        PackageArchiveCliReport {
            command: "package archive",
            package_hash: report.package_hash,
            format: report.format,
            status: report.status,
            fidelity_report_path: report.fidelity_report_path,
            fidelity_statement: report.fidelity_statement,
            segments: report.segments,
        },
    )
}

fn list_packages(root: PathBuf) -> Result<Vec<PackageListEntry>, CliError> {
    if !root.exists() {
        return Ok(Vec::new());
    }

    let mut packages = Vec::new();
    for entry in sorted_child_entries(&root)? {
        let path = entry.path();
        if path.join(MANIFEST_FILE).exists() {
            let manifest = cdf_package::read_manifest(&path)?;
            packages.push(PackageListEntry {
                path: path.display().to_string(),
                package_hash: manifest.package_hash,
                status: manifest.lifecycle.status.as_str().to_owned(),
                segments: manifest.identity.segments.len(),
            });
        }
    }
    Ok(packages)
}

fn sorted_child_entries(root: &Path) -> Result<Vec<fs::DirEntry>, CliError> {
    let mut entries = fs::read_dir(root)
        .map_err(|error| CdfError::data(format!("read {}: {error}", root.display())))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| CdfError::data(format!("read {}: {error}", root.display())))?;
    entries.sort_by_key(|entry| entry.path());
    Ok(entries)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PackageListEntry {
    path: String,
    package_hash: String,
    status: String,
    segments: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PackageGcPlanReport {
    command: &'static str,
    package_root: String,
    mode: &'static str,
    artifacts: Vec<PackageGcArtifact>,
    counts: PackageGcCounts,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PackageGcArtifact {
    package_path: Option<String>,
    package_hash: Option<String>,
    classification: PackageGcClassification,
    retention_reason: &'static str,
    planned_action: PackageGcPlannedAction,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum PackageGcClassification {
    Retained,
    Collectible,
    Missing,
    Corrupt,
    Protected,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum PackageGcPlannedAction {
    Retain,
    WouldCollect,
    RestoreRequired,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
struct PackageGcCounts {
    retained: usize,
    collectible: usize,
    missing: usize,
    corrupt: usize,
    protected: usize,
}

impl PackageGcCounts {
    fn from_artifacts(artifacts: &[PackageGcArtifact]) -> Self {
        let mut counts = BTreeMap::from([
            ("retained", 0),
            ("collectible", 0),
            ("missing", 0),
            ("corrupt", 0),
            ("protected", 0),
        ]);
        for artifact in artifacts {
            let key = match artifact.classification {
                PackageGcClassification::Retained => "retained",
                PackageGcClassification::Collectible => "collectible",
                PackageGcClassification::Missing => "missing",
                PackageGcClassification::Corrupt => "corrupt",
                PackageGcClassification::Protected => "protected",
            };
            *counts.get_mut(key).expect("known package gc count key") += 1;
        }
        Self {
            retained: counts["retained"],
            collectible: counts["collectible"],
            missing: counts["missing"],
            corrupt: counts["corrupt"],
            protected: counts["protected"],
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PackageVerifyReport {
    package_hash: String,
    checked_files: Vec<cdf_package::FileEntry>,
    checked_archives: Vec<cdf_package::ArchiveSegmentMetadata>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PackageArchiveCliReport {
    command: &'static str,
    package_hash: String,
    format: String,
    status: cdf_package::PackageArchiveWriteStatus,
    fidelity_report_path: String,
    fidelity_statement: String,
    segments: Vec<cdf_package::ArchiveSegmentMetadata>,
}

fn package_archive_status(status: &cdf_package::PackageArchiveWriteStatus) -> &'static str {
    match status {
        cdf_package::PackageArchiveWriteStatus::Written => "written",
        cdf_package::PackageArchiveWriteStatus::Skipped => "skipped",
        cdf_package::PackageArchiveWriteStatus::Replaced => "replaced",
    }
}
