use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use cdf_kernel::{
    CdfError, EnvironmentName, ResourceId, SchemaAuthorityKey, SchemaAuthorityStore,
    SchemaHeadStatus,
};
use cdf_package::PackageReader;
use cdf_package_contract::MANIFEST_FILE;
use cdf_project::{
    LocalPromotionCollectionAssessment, PackageCollectionAction, PackageCollectionArtifact,
    PackageCollectionClassification, PackageCollectionRequest, RetentionRule,
    execute_package_collection, plan_package_collection, retention_rule_for_trust,
};
use cdf_state_sqlite::{
    SqliteCheckpointStore, SqliteSchemaAuthorityState, SqliteSchemaAuthorityStore,
};
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
        PackageCommand::Gc {
            packages_dir,
            execute,
        } => {
            let report = package_gc(cli, packages_dir, execute, destinations)?;
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

fn package_gc(
    cli: &Cli,
    packages_dir: Option<PathBuf>,
    execute: bool,
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
    let checkpoints = committed_checkpoints(context.as_ref())?;
    let retention_by_resource = retention_by_resource(context.as_ref());
    let protected_resources = active_promotion_resources(context.as_ref())?;
    let evaluated_at_ms = system_time_ms()?;
    let request = PackageCollectionRequest {
        package_root: &root,
        committed_checkpoints: &checkpoints,
        retention_by_resource: &retention_by_resource,
        protected_resources: &protected_resources,
        evaluated_at_ms,
    };
    let planned = plan_package_collection(&request)?;
    let plan = if execute {
        execute_package_collection(&request, &planned)?
    } else {
        planned
    };
    let counts = PackageGcCounts::from_artifacts(&plan.artifacts);
    Ok(PackageGcPlanReport {
        command: "package gc",
        package_root: plan.package_root,
        mode: if execute { "execute" } else { "dry_run" },
        artifacts: plan.artifacts,
        promotion_availability: plan.promotion_availability,
        counts,
    })
}

fn committed_checkpoints(
    context: Option<&ProjectContext>,
) -> Result<Vec<cdf_kernel::Checkpoint>, CliError> {
    let Some(context) = context else {
        return Ok(Vec::new());
    };
    let path = context.state_store_path()?;
    let ownership = context.state_store_path_ownership();
    if !cdf_state_sqlite::database_path_exists(&path, ownership)? {
        return Ok(Vec::new());
    }
    SqliteCheckpointStore::open_read_only_with_path_ownership(path, ownership)?
        .committed_checkpoints()
        .map_err(Into::into)
}

fn retention_by_resource(
    context: Option<&ProjectContext>,
) -> BTreeMap<ResourceId, Option<RetentionRule>> {
    let Some(context) = context else {
        return BTreeMap::new();
    };
    context
        .resources
        .iter()
        .map(|resource| {
            (
                resource.descriptor().resource_id.clone(),
                retention_rule_for_trust(
                    context.environment.retention.as_ref(),
                    &resource.descriptor().trust_level,
                ),
            )
        })
        .collect()
}

fn active_promotion_resources(
    context: Option<&ProjectContext>,
) -> Result<BTreeSet<ResourceId>, CliError> {
    let Some(context) = context else {
        return Ok(BTreeSet::new());
    };
    let state_path = context.state_store_path()?;
    let ownership = context.state_store_path_ownership();
    let SqliteSchemaAuthorityState::Ready {
        authority_domain_id,
    } = SqliteSchemaAuthorityStore::inspect_state(&state_path, ownership)?
    else {
        return Ok(BTreeSet::new());
    };
    let store =
        SqliteSchemaAuthorityStore::open_read_only_with_path_ownership(&state_path, ownership)?;
    let mut protected = BTreeSet::new();
    for resource in &context.resources {
        let resource_id = resource.descriptor().resource_id.clone();
        let key = SchemaAuthorityKey::new(
            authority_domain_id.clone(),
            context.config.project.id.clone(),
            EnvironmentName::new(context.environment.name.clone())?,
            resource_id.clone(),
        )?;
        if store
            .head(&key)?
            .is_some_and(|head| matches!(head.status, SchemaHeadStatus::Promoting { .. }))
        {
            protected.insert(resource_id);
        }
    }
    Ok(protected)
}

fn system_time_ms() -> Result<i64, CliError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            CdfError::environment(format!("system clock precedes Unix epoch: {error}"))
        })?;
    i64::try_from(elapsed.as_millis())
        .map_err(|_| CdfError::environment("system epoch milliseconds exceed i64").into())
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
    artifacts: Vec<PackageCollectionArtifact>,
    promotion_availability: Vec<LocalPromotionCollectionAssessment>,
    counts: PackageGcCounts,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
struct PackageGcCounts {
    retained: usize,
    collectible: usize,
    collected: usize,
    missing: usize,
    corrupt: usize,
    protected: usize,
    tombstoned: usize,
}

impl PackageGcCounts {
    fn from_artifacts(artifacts: &[PackageCollectionArtifact]) -> Self {
        let mut counts = BTreeMap::from([
            ("retained", 0),
            ("collectible", 0),
            ("collected", 0),
            ("missing", 0),
            ("corrupt", 0),
            ("protected", 0),
            ("tombstoned", 0),
        ]);
        for artifact in artifacts {
            let key = match artifact.classification {
                PackageCollectionClassification::Retained => "retained",
                PackageCollectionClassification::Collectible => "collectible",
                PackageCollectionClassification::Collected => "collected",
                PackageCollectionClassification::Missing => "missing",
                PackageCollectionClassification::Corrupt => "corrupt",
                PackageCollectionClassification::Protected => "protected",
                PackageCollectionClassification::Tombstoned => "tombstoned",
            };
            *counts.get_mut(key).expect("known package gc count key") += 1;
        }
        Self {
            retained: counts["retained"],
            collectible: counts["collectible"],
            collected: counts["collected"],
            missing: counts["missing"],
            corrupt: counts["corrupt"],
            protected: counts["protected"],
            tombstoned: counts["tombstoned"],
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
