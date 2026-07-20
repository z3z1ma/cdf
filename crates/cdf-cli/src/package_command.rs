use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use cdf_kernel::{CdfError, PackageHash};
use cdf_package::PackageReader;
use cdf_package_contract::{ArchiveSegmentMetadata, MANIFEST_FILE, PackageStatus};
use cdf_project::{
    LocalPromotionCollectionAction, LocalPromotionCollectionAssessment,
    assess_local_promotion_collection, inspect_local_package_promotion_availability,
};
use cdf_state_sqlite::SqliteCheckpointStore;
use serde::Serialize;

use crate::{
    args::{Cli, PackageArchiveArgs, PackageCommand},
    context::ProjectContext,
    error_catalog,
    output::{CliError, CommandOutput},
    render::{
        RenderDocument,
        humanize::humanize_bytes,
        primitives::{KeyValuePanel, NextCommand, SectionRule, StatusKind, StatusLine, Table},
        redaction::redact_uri_userinfo,
    },
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
            let report = PackageListReport {
                packages: packages.clone(),
            };
            CommandOutput::rendered("package ls", report.render_document(), packages)
        }
        PackageCommand::Gc { packages_dir } => {
            let report = package_gc_plan(cli, packages_dir)?;
            CommandOutput::rendered("package gc", report.render_document(), report)
        }
        PackageCommand::Verify { package_dir } => {
            let reader = PackageReader::open(&package_dir)?;
            let report = reader.verify()?;
            let cli_report = PackageVerifyReport {
                package_hash: report.package_hash,
                checked_file_count: report.checked_file_count,
                checked_archive_count: report.checked_archive_count,
            };
            CommandOutput::rendered("package verify", cli_report.render_document(), cli_report)
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
    let promotion_availability = promotion_gc_availability(&root, &artifacts)?;
    let counts = PackageGcCounts::from_artifacts(&artifacts);
    Ok(PackageGcPlanReport {
        command: "package gc",
        package_root: root.display().to_string(),
        mode: "dry_run",
        artifacts,
        promotion_availability,
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

    let manifest = match cdf_package::read_manifest_header(package_dir) {
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
        return Err(CliError::usage_with(
            format!("unsupported package archive format `{}`", args.format),
            error_catalog::PACKAGE_ARGUMENT,
        ));
    }

    let report = cdf_package::persist_package_parquet_archive(&args.package_dir, args.force)?;
    let archive_byte_count = report
        .segments
        .iter()
        .map(|segment| segment.archive_byte_count)
        .sum::<u64>();
    let cli_report = PackageArchiveCliReport {
        command: "package archive",
        package_hash: report.package_hash,
        format: report.format,
        status: report.status,
        fidelity_report_path: report.fidelity_report_path,
        fidelity_statement: report.fidelity_statement,
        segments: report.segments,
    };
    CommandOutput::rendered(
        "package archive",
        cli_report.render_document(archive_byte_count),
        cli_report,
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
            let mut segments = 0_u64;
            let manifest =
                cdf_package::visit_manifest_entries(&path, &mut |_| Ok(()), &mut |_| {
                    segments = segments
                        .checked_add(1)
                        .ok_or_else(|| CdfError::data("package segment count overflowed u64"))?;
                    Ok(())
                })?;
            packages.push(PackageListEntry {
                path: path.display().to_string(),
                package_hash: manifest.package_hash,
                status: manifest.lifecycle.status.as_str().to_owned(),
                segments,
            });
        }
    }
    Ok(packages)
}

fn sorted_child_entries(root: &Path) -> Result<Vec<fs::DirEntry>, CliError> {
    let mut entries = fs::read_dir(root)
        .map_err(|error| {
            CliError::mapped(
                CdfError::data(format!("read {}: {error}", root.display())),
                error_catalog::PACKAGE_ARTIFACT,
            )
        })?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| {
            CliError::mapped(
                CdfError::data(format!("read {}: {error}", root.display())),
                error_catalog::PACKAGE_ARTIFACT,
            )
        })?;
    entries.sort_by_key(|entry| entry.path());
    Ok(entries)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PackageListReport {
    packages: Vec<PackageListEntry>,
}

impl PackageListReport {
    fn render_document(&self) -> RenderDocument {
        let table = self.packages.iter().fold(
            Table::new(["path", "hash", "status", "segments"]),
            |table, package| {
                table.row([
                    redact_uri_userinfo(&package.path),
                    package.package_hash.clone(),
                    package.status.clone(),
                    package.segments.to_string(),
                ])
            },
        );

        let mut document = RenderDocument::new()
            .push(SectionRule::new())
            .push(StatusLine::new(
                StatusKind::Success,
                format!("{} package(s)", self.packages.len()),
            ))
            .blank_line()
            .push(
                KeyValuePanel::new("Packages")
                    .row("count", self.packages.len().to_string())
                    .row("source", "package root"),
            );

        if !self.packages.is_empty() {
            document = document.blank_line().push(table);
        }

        document
            .blank_line()
            .push(NextCommand::new("cdf package verify <package>"))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PackageListEntry {
    path: String,
    package_hash: String,
    status: String,
    segments: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PackageGcPlanReport {
    command: &'static str,
    package_root: String,
    mode: &'static str,
    artifacts: Vec<PackageGcArtifact>,
    promotion_availability: Vec<LocalPromotionCollectionAssessment>,
    counts: PackageGcCounts,
}

impl PackageGcPlanReport {
    fn render_document(&self) -> RenderDocument {
        let table = self.artifacts.iter().fold(
            Table::new(["artifact", "classification", "action", "reason"]),
            |table, artifact| {
                table.row([
                    artifact_display(artifact),
                    classification_name(&artifact.classification).to_owned(),
                    planned_action_name(&artifact.planned_action).to_owned(),
                    artifact.retention_reason.to_owned(),
                ])
            },
        );

        let mut document = RenderDocument::new()
            .push(SectionRule::new())
            .push(StatusLine::new(
                StatusKind::Success,
                format!("planned package gc for {}", self.package_root),
            ))
            .blank_line()
            .push(
                KeyValuePanel::new("Package GC")
                    .row("root", redact_uri_userinfo(&self.package_root))
                    .row("mode", self.mode)
                    .row("artifacts", self.artifacts.len().to_string())
                    .row("collectible", self.counts.collectible.to_string())
                    .row("protected", self.counts.protected.to_string())
                    .row("corrupt", self.counts.corrupt.to_string())
                    .row("missing", self.counts.missing.to_string()),
            );

        if !self.artifacts.is_empty() {
            document = document.blank_line().push(table);
        }

        if !self.promotion_availability.is_empty() {
            document = document
                .blank_line()
                .push(self.promotion_availability.iter().fold(
                    Table::new([
                        "resource",
                        "package",
                        "local bytes",
                        "promotable",
                        "action",
                        "removes last local authority",
                    ]),
                    |table, item| {
                        table.row([
                            item.resource_id.clone(),
                            item.package_hash.clone(),
                            humanize_bytes(item.local_residual_bytes),
                            yes_no(item.locally_promotable).to_owned(),
                            item.planned_action.as_str().to_owned(),
                            yes_no(item.collection_removes_last_local_promotable_copy).to_owned(),
                        ])
                    },
                ))
                .blank_line()
                .push(
                    KeyValuePanel::new("Promotion availability")
                        .row(
                            "remediation",
                            self.promotion_availability[0].remediation.clone(),
                        )
                        .row("destination readback inferred", "no"),
                );
        }

        document
            .blank_line()
            .push(NextCommand::new("cdf package verify <package>"))
    }
}

fn promotion_gc_availability(
    package_root: &Path,
    artifacts: &[PackageGcArtifact],
) -> Result<Vec<LocalPromotionCollectionAssessment>, CliError> {
    let local = inspect_local_package_promotion_availability(package_root)?;
    let actions = artifacts
        .iter()
        .filter_map(|artifact| {
            artifact.package_path.as_ref().map(|path| {
                let action = match artifact.planned_action {
                    PackageGcPlannedAction::Retain => LocalPromotionCollectionAction::Retain,
                    PackageGcPlannedAction::WouldCollect => {
                        LocalPromotionCollectionAction::WouldCollect
                    }
                    PackageGcPlannedAction::RestoreRequired => {
                        LocalPromotionCollectionAction::RestoreRequired
                    }
                };
                (path.clone(), action)
            })
        })
        .collect::<BTreeMap<_, _>>();
    Ok(assess_local_promotion_collection(local, &actions))
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
    checked_file_count: usize,
    checked_archive_count: usize,
}

impl PackageVerifyReport {
    fn render_document(&self) -> RenderDocument {
        RenderDocument::new()
            .push(SectionRule::new())
            .push(StatusLine::new(
                StatusKind::Success,
                format!("verified package {}", self.package_hash),
            ))
            .blank_line()
            .push(
                KeyValuePanel::new("Integrity")
                    .row("package", self.package_hash.clone())
                    .row("files", self.checked_file_count.to_string())
                    .row("archive segments", self.checked_archive_count.to_string()),
            )
            .blank_line()
            .push(NextCommand::new("cdf inspect package <package>"))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PackageArchiveCliReport {
    command: &'static str,
    package_hash: String,
    format: String,
    status: cdf_package::PackageArchiveWriteStatus,
    fidelity_report_path: String,
    fidelity_statement: String,
    segments: Vec<ArchiveSegmentMetadata>,
}

impl PackageArchiveCliReport {
    fn render_document(&self, archive_byte_count: u64) -> RenderDocument {
        let table = self.segments.iter().fold(
            Table::new(["segment", "source", "archive", "rows", "bytes"]),
            |table, segment| {
                table.row([
                    segment.segment_id.clone(),
                    redact_uri_userinfo(&segment.source_path),
                    redact_uri_userinfo(&segment.archive_path),
                    segment.archive_row_count.to_string(),
                    humanize_bytes(segment.archive_byte_count),
                ])
            },
        );

        let mut document = RenderDocument::new()
            .push(SectionRule::new())
            .push(StatusLine::new(
                StatusKind::Success,
                format!("archived package {}", self.package_hash),
            ))
            .blank_line()
            .push(
                KeyValuePanel::new("Archive")
                    .row("package", self.package_hash.clone())
                    .row("format", self.format.clone())
                    .row("status", package_archive_status(&self.status))
                    .row("segments", self.segments.len().to_string())
                    .row("bytes", humanize_bytes(archive_byte_count))
                    .row("fidelity", redact_uri_userinfo(&self.fidelity_report_path))
                    .row("statement", self.fidelity_statement.clone()),
            );

        if !self.segments.is_empty() {
            document = document.blank_line().push(table);
        }

        document
            .blank_line()
            .push(NextCommand::new("cdf package verify <package>"))
    }
}

fn package_archive_status(status: &cdf_package::PackageArchiveWriteStatus) -> &'static str {
    match status {
        cdf_package::PackageArchiveWriteStatus::Written => "written",
        cdf_package::PackageArchiveWriteStatus::Skipped => "skipped",
        cdf_package::PackageArchiveWriteStatus::Replaced => "replaced",
    }
}

fn artifact_display(artifact: &PackageGcArtifact) -> String {
    artifact
        .package_path
        .as_deref()
        .or(artifact.package_hash.as_deref())
        .map(redact_uri_userinfo)
        .unwrap_or_else(|| "unknown".to_owned())
}

fn classification_name(classification: &PackageGcClassification) -> &'static str {
    match classification {
        PackageGcClassification::Retained => "retained",
        PackageGcClassification::Collectible => "collectible",
        PackageGcClassification::Missing => "missing",
        PackageGcClassification::Corrupt => "corrupt",
        PackageGcClassification::Protected => "protected",
    }
}

fn planned_action_name(action: &PackageGcPlannedAction) -> &'static str {
    match action {
        PackageGcPlannedAction::Retain => "retain",
        PackageGcPlannedAction::WouldCollect => "would_collect",
        PackageGcPlannedAction::RestoreRequired => "restore_required",
    }
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}
