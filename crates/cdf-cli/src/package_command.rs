use std::{fs, path::PathBuf};

use cdf_kernel::CdfError;
use cdf_package::{MANIFEST_FILE, PackageReader};
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
            let root = match packages_dir {
                Some(path) => path.display().to_string(),
                None => ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?
                    .package_root()
                    .display()
                    .to_string(),
            };
            Err(CliError::not_supported(
                "package gc",
                format!(
                    "retention-safe GC for package root `{root}` requires checkpoint proof checks"
                ),
                "package retention planner tied to CheckpointStore history",
            ))
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
    let mut entries = fs::read_dir(&root)
        .map_err(|error| CdfError::data(format!("read {}: {error}", root.display())))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| CdfError::data(format!("read {}: {error}", root.display())))?;
    entries.sort_by_key(|entry| entry.path());

    let mut packages = Vec::new();
    for entry in entries {
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PackageListEntry {
    path: String,
    package_hash: String,
    status: String,
    segments: usize,
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
