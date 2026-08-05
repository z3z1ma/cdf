use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use cdf_kernel::{CdfError, PackageHash};
use cdf_package::PackageReader;
use cdf_package_contract::{MANIFEST_FILE, PackageStatus};
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
};

pub(crate) fn package(
    cli: &Cli,
    command: PackageCommand,
    destinations: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    match command {
        PackageCommand::Ls { packages_dir } => {
            let root = match packages_dir {
                Some(path) => path,
                None => ProjectContext::load_with_destination_registry(
                    cli.project.as_ref(),
                    cli.env.as_deref(),
                    destinations,
                )?
                .package_root(),
            };
            let packages = list_packages(root)?;
            let report = PackageListReport { packages };
            CommandOutput::rendered("package ls", report.render_document(), report)
        }
        PackageCommand::Gc { packages_dir } => {
            let report = package_gc_plan(cli, packages_dir, destinations)?;
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
    destinations: &cdf_runtime::DestinationRegistry,
) -> Result<PackageGcPlanReport, CliError> {
    let context = if packages_dir.is_none() || cli.project.is_some() {
        Some(ProjectContext::load_with_destination_registry(
            cli.project.as_ref(),
            cli.env.as_deref(),
            destinations,
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
    let ownership = context.state_store_path_ownership();
    if !cdf_state_sqlite::database_path_exists(&path, ownership)? {
        return Ok(BTreeSet::new());
    }
    SqliteCheckpointStore::open_read_only_with_path_ownership(path, ownership)?
        .committed_package_hashes()
        .map_err(CliError::from)
}

fn plan_package_gc_artifacts(
    root: &Path,
    protected_hashes: &BTreeSet<PackageHash>,
) -> Result<Vec<PackageGcArtifact>, CliError> {
    let mut artifacts = Vec::new();
    let mut readable_hashes = BTreeSet::new();
    if package_root_is_directory(root)? {
        for entry in sorted_child_entries(root)? {
            let path = entry.path();
            if !package_entry_is_directory(&entry)? {
                continue;
            }
            let artifact = classify_package_artifact(&path, protected_hashes)?;
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
) -> Result<PackageGcArtifact, CliError> {
    let package_path = Some(package_dir.display().to_string());
    let manifest_path = package_dir.join(MANIFEST_FILE);
    match fs::symlink_metadata(&manifest_path) {
        Ok(metadata) if metadata.is_file() => {}
        Ok(_) => {
            return Ok(PackageGcArtifact {
                package_path,
                package_hash: None,
                classification: PackageGcClassification::Corrupt,
                retention_reason: "manifest_unreadable",
                planned_action: PackageGcPlannedAction::Retain,
            });
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(PackageGcArtifact {
                package_path,
                package_hash: None,
                classification: PackageGcClassification::Corrupt,
                retention_reason: "manifest_missing",
                planned_action: PackageGcPlannedAction::Retain,
            });
        }
        Err(error) if package_artifact_shape_error(&error) => {
            return Ok(PackageGcArtifact {
                package_path,
                package_hash: None,
                classification: PackageGcClassification::Corrupt,
                retention_reason: "manifest_unreadable",
                planned_action: PackageGcPlannedAction::Retain,
            });
        }
        Err(error) => {
            return Err(package_artifact_host_error(
                "inspect package manifest",
                &manifest_path,
                error,
            ));
        }
    }

    let manifest = match cdf_package::read_manifest_header(package_dir) {
        Ok(manifest) => manifest,
        Err(error) if error.kind == cdf_kernel::ErrorKind::Data => {
            return Ok(PackageGcArtifact {
                package_path,
                package_hash: None,
                classification: PackageGcClassification::Corrupt,
                retention_reason: "manifest_unreadable",
                planned_action: PackageGcPlannedAction::Retain,
            });
        }
        Err(error) => return Err(error.into()),
    };
    let package_hash = Some(manifest.package_hash.clone());

    if manifest.lifecycle.status == PackageStatus::Archived {
        return Ok(PackageGcArtifact {
            package_path,
            package_hash,
            classification: PackageGcClassification::Protected,
            retention_reason: "retention_tombstone",
            planned_action: PackageGcPlannedAction::Retain,
        });
    }

    let protected_by_checkpoint = PackageHash::new(manifest.package_hash.clone())
        .is_ok_and(|hash| protected_hashes.contains(&hash));
    if let Err(error) = cdf_package::verify_package(package_dir) {
        if error.kind != cdf_kernel::ErrorKind::Data {
            return Err(error.into());
        }
        return Ok(PackageGcArtifact {
            package_path,
            package_hash,
            classification: PackageGcClassification::Corrupt,
            retention_reason: if protected_by_checkpoint {
                "committed_checkpoint_verification_failed"
            } else {
                "verification_failed"
            },
            planned_action: PackageGcPlannedAction::Retain,
        });
    }

    if protected_by_checkpoint {
        return Ok(PackageGcArtifact {
            package_path,
            package_hash,
            classification: PackageGcClassification::Protected,
            retention_reason: "committed_checkpoint",
            planned_action: PackageGcPlannedAction::Retain,
        });
    }
    match cdf_package::PackageReader::open(package_dir).and_then(|reader| reader.receipt_count()) {
        Ok(count) if count != 0 => {
            return Ok(PackageGcArtifact {
                package_path,
                package_hash,
                classification: PackageGcClassification::Protected,
                retention_reason: "package_receipt",
                planned_action: PackageGcPlannedAction::Retain,
            });
        }
        Ok(_) => {}
        Err(error) if error.kind == cdf_kernel::ErrorKind::Data => {
            return Ok(PackageGcArtifact {
                package_path,
                package_hash,
                classification: PackageGcClassification::Corrupt,
                retention_reason: "receipt_unreadable",
                planned_action: PackageGcPlannedAction::Retain,
            });
        }
        Err(error) => return Err(error.into()),
    }

    if matches!(
        manifest.lifecycle.status,
        PackageStatus::Planned | PackageStatus::Extracting | PackageStatus::Validated
    ) {
        Ok(PackageGcArtifact {
            package_path,
            package_hash,
            classification: PackageGcClassification::Collectible,
            retention_reason: "pre_packaged_artifact",
            planned_action: PackageGcPlannedAction::WouldCollect,
        })
    } else {
        Ok(PackageGcArtifact {
            package_path,
            package_hash,
            classification: PackageGcClassification::Retained,
            retention_reason: "replay_or_recovery_artifact",
            planned_action: PackageGcPlannedAction::Retain,
        })
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
    let cli_report = PackageArchiveCliReport {
        command: "package archive",
        package_hash: report.package_hash,
        format: report.format,
        status: report.status,
        fidelity_report_path: report.fidelity_report_path,
        fidelity_statement: report.fidelity_statement,
        segment_index_path: report.segment_index_path,
        segment_count: report.segment_count,
        row_count: report.row_count,
        archive_byte_count: report.archive_byte_count,
    };
    CommandOutput::rendered("package archive", cli_report.render_document(), cli_report)
}

fn list_packages(root: PathBuf) -> Result<Vec<PackageListEntry>, CliError> {
    if !package_root_is_directory(&root)? {
        return Ok(Vec::new());
    }

    let mut packages = Vec::new();
    for entry in sorted_child_entries(&root)? {
        let path = entry.path();
        if !package_entry_is_directory(&entry)? {
            continue;
        }
        let manifest_path = path.join(MANIFEST_FILE);
        match fs::symlink_metadata(&manifest_path) {
            Ok(metadata) if metadata.is_file() => {
                let mut segments = 0_u64;
                let manifest =
                    cdf_package::visit_manifest_entries(&path, &mut |_| Ok(()), &mut |_| {
                        segments = segments.checked_add(1).ok_or_else(|| {
                            CdfError::data("package segment count overflowed u64")
                        })?;
                        Ok(())
                    })?;
                packages.push(PackageListEntry {
                    path: path.display().to_string(),
                    package_hash: manifest.package_hash,
                    status: manifest.lifecycle.status.as_str().to_owned(),
                    segments,
                });
            }
            Ok(_) => {
                return Err(CliError::mapped(
                    CdfError::data(format!(
                        "package manifest {} is not a real regular file",
                        manifest_path.display()
                    )),
                    error_catalog::PACKAGE_ARTIFACT,
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) if package_artifact_shape_error(&error) => {
                return Err(CliError::mapped(
                    CdfError::data(format!(
                        "package manifest {} has an invalid filesystem shape: {error}",
                        manifest_path.display()
                    )),
                    error_catalog::PACKAGE_ARTIFACT,
                ));
            }
            Err(error) => {
                return Err(package_artifact_host_error(
                    "inspect package manifest",
                    &manifest_path,
                    error,
                ));
            }
        }
    }
    Ok(packages)
}

fn package_root_is_directory(root: &Path) -> Result<bool, CliError> {
    match fs::symlink_metadata(root) {
        Ok(metadata) if metadata.is_dir() => Ok(true),
        Ok(_) => Err(CliError::mapped(
            CdfError::data(format!(
                "package root {} is not a real directory",
                root.display()
            )),
            error_catalog::PACKAGE_ARTIFACT,
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            validate_missing_package_root_ancestors(root)?;
            Ok(false)
        }
        Err(error) if package_artifact_shape_error(&error) => Err(CliError::mapped(
            CdfError::data(format!(
                "package root {} has an invalid filesystem shape: {error}",
                root.display()
            )),
            error_catalog::PACKAGE_ARTIFACT,
        )),
        Err(error) => Err(package_artifact_host_error(
            "inspect package root",
            root,
            error,
        )),
    }
}

fn validate_missing_package_root_ancestors(root: &Path) -> Result<(), CliError> {
    let mut cursor = root.parent();
    while let Some(parent) = cursor {
        if parent.as_os_str().is_empty() {
            return Ok(());
        }
        match fs::metadata(parent) {
            Ok(metadata) if !metadata.is_dir() => {
                return Err(CliError::mapped(
                    CdfError::data(format!(
                        "package root ancestor {} is not a real directory",
                        parent.display()
                    )),
                    error_catalog::PACKAGE_ARTIFACT,
                ));
            }
            Ok(_) => return Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match fs::symlink_metadata(parent) {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        return Err(CliError::mapped(
                            CdfError::data(format!(
                                "package root ancestor {} is a dangling symlink",
                                parent.display()
                            )),
                            error_catalog::PACKAGE_ARTIFACT,
                        ));
                    }
                    Ok(_) => {
                        return Err(CliError::mapped(
                            CdfError::data(format!(
                                "package root ancestor {} changed filesystem shape during inspection",
                                parent.display()
                            )),
                            error_catalog::PACKAGE_ARTIFACT,
                        ));
                    }
                    Err(link_error) if link_error.kind() == std::io::ErrorKind::NotFound => {
                        cursor = parent.parent();
                    }
                    Err(link_error) => {
                        return Err(package_artifact_host_error(
                            "inspect package root ancestor",
                            parent,
                            link_error,
                        ));
                    }
                }
            }
            Err(error) if package_artifact_shape_error(&error) => {
                return Err(CliError::mapped(
                    CdfError::data(format!(
                        "package root ancestor {} has an invalid filesystem shape: {error}",
                        parent.display()
                    )),
                    error_catalog::PACKAGE_ARTIFACT,
                ));
            }
            Err(error) => {
                return Err(package_artifact_host_error(
                    "inspect package root ancestor",
                    parent,
                    error,
                ));
            }
        }
    }
    Ok(())
}

fn package_entry_is_directory(entry: &fs::DirEntry) -> Result<bool, CliError> {
    let kind = entry.file_type().map_err(|error| {
        package_artifact_io_error("inspect package-root entry", &entry.path(), error)
    })?;
    if kind.is_symlink() {
        return Err(CliError::mapped(
            CdfError::data(format!(
                "package-root entry {} is a symlink; package artifacts must be real directories beneath the configured package root",
                entry.path().display()
            )),
            error_catalog::PACKAGE_ARTIFACT,
        ));
    }
    Ok(kind.is_dir())
}

fn sorted_child_entries(root: &Path) -> Result<Vec<fs::DirEntry>, CliError> {
    let mut entries = fs::read_dir(root)
        .map_err(|error| package_artifact_io_error("read package root", root, error))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| package_artifact_io_error("read package-root entry", root, error))?;
    entries.sort_by_key(|entry| entry.path());
    Ok(entries)
}

fn package_artifact_shape_error(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::NotFound
            | std::io::ErrorKind::NotADirectory
            | std::io::ErrorKind::IsADirectory
            | std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::InvalidInput
            | std::io::ErrorKind::InvalidData
    ) || cdf_kernel::is_filesystem_loop(error)
}

fn package_artifact_io_error(action: &str, path: &Path, error: std::io::Error) -> CliError {
    if package_artifact_shape_error(&error) {
        CliError::mapped(
            CdfError::data(format!(
                "{action} {} with invalid filesystem shape: {error}",
                path.display()
            )),
            error_catalog::PACKAGE_ARTIFACT,
        )
    } else {
        package_artifact_host_error(action, path, error)
    }
}

fn package_artifact_host_error(action: &str, path: &Path, error: std::io::Error) -> CliError {
    CdfError::environment(format!(
        "{action} {}: {error}; check package-path permissions, device availability, free space, and process file limits before retrying",
        path.display()
    ))
    .into()
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
struct PackageListReport {
    packages: Vec<PackageListEntry>,
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
    checked_file_count: u64,
    checked_archive_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PackageArchiveCliReport {
    command: &'static str,
    package_hash: String,
    format: String,
    status: cdf_package::PackageArchiveWriteStatus,
    fidelity_report_path: String,
    fidelity_statement: String,
    segment_index_path: String,
    segment_count: u64,
    row_count: u64,
    archive_byte_count: u64,
}

mod render;
