use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use cdf_kernel::{CdfError, Result};
use cdf_memory::{DEFAULT_PROCESS_BUDGET_BYTES, DeterministicMemoryCoordinator, MemoryCoordinator};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    json::canonical_json_bytes,
    model::{
        ArchiveSegmentMetadata, ManifestArchives, PackageManifest, ParquetArchiveMetadata,
        SegmentEntry,
    },
    ops::{read_manifest, verify_package, verify_package_identity},
    parquet::transcode_record_batches_to_parquet_bytes,
    reader::PackageReader,
    storage::{io_error, package_path, sync_directory, write_manifest_atomic},
};

pub const ARCHIVE_FIDELITY_STATEMENT: &str = "Arrow IPC remains the canonical package data. Parquet bytes are an archive/interchange projection; Arrow field metadata and other Arrow-only semantics are not promoted to canonical Parquet truth.";
const PARQUET_ARCHIVE_FORMAT_VERSION: u16 = 1;
const ARCHIVE_ROOT: &str = "archive";
const PARQUET_ARCHIVE_DIR: &str = "archive/parquet";
const PARQUET_DATA_DIR: &str = "archive/parquet/data";
const FIDELITY_REPORT_PATH: &str = "archive/parquet/fidelity.json";
const SOURCE_FORMAT: &str = "arrow_ipc_lz4";
const ARCHIVE_FORMAT: &str = "parquet";

static ARCHIVE_TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageArchiveReport {
    pub package_hash: String,
    pub fidelity_statement: String,
    pub segments: Vec<ArchivedSegmentReport>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchivedSegmentReport {
    pub segment_id: String,
    pub source_path: String,
    pub source_byte_count: u64,
    pub source_sha256: String,
    pub source_row_count: u64,
    pub parquet_bytes: Vec<u8>,
    pub parquet_byte_count: u64,
    pub parquet_sha256: String,
    pub parquet_row_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedPackageArchiveReport {
    pub package_hash: String,
    pub format: String,
    pub status: PackageArchiveWriteStatus,
    pub fidelity_report_path: String,
    pub fidelity_statement: String,
    pub segments: Vec<ArchiveSegmentMetadata>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageArchiveWriteStatus {
    Written,
    Skipped,
    Replaced,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageArchiveFidelityReport {
    pub package_hash: String,
    pub source_format: String,
    pub archive_format: String,
    pub fidelity_statement: String,
    pub segments: Vec<ArchiveSegmentMetadata>,
}

pub fn archive_package_to_parquet(package_dir: impl AsRef<Path>) -> Result<PackageArchiveReport> {
    let package_dir = package_dir.as_ref();
    verify_package_identity(package_dir)?;
    let reader = PackageReader::open(package_dir)?;

    let mut segments = Vec::new();
    for entry in &reader.manifest().identity.segments {
        let batches = reader.read_segment(&entry.segment_id)?;
        if batches.is_empty() {
            return Err(CdfError::data(format!(
                "package segment {} contains no batches",
                entry.segment_id.as_str()
            )));
        }

        let mut row_count = 0_u64;
        for batch in &batches {
            row_count += batch.num_rows() as u64;
        }
        if row_count != entry.row_count {
            return Err(CdfError::data(format!(
                "segment {} manifest row count {} differs from package data {}",
                entry.segment_id.as_str(),
                entry.row_count,
                row_count
            )));
        }

        let parquet_bytes = transcode_record_batches_to_parquet_bytes(&batches)?;
        segments.push(archive_segment_report(entry, parquet_bytes, row_count));
    }

    Ok(PackageArchiveReport {
        package_hash: reader.manifest().package_hash.clone(),
        fidelity_statement: ARCHIVE_FIDELITY_STATEMENT.to_owned(),
        segments,
    })
}

pub fn persist_package_parquet_archive(
    package_dir: impl AsRef<Path>,
    force: bool,
) -> Result<PersistedPackageArchiveReport> {
    let package_dir = package_dir.as_ref();
    cleanup_stale_archive_temps(package_dir)?;
    verify_package_identity(package_dir)?;

    let manifest = read_manifest(package_dir)?;
    if !manifest.lifecycle.status.is_archivable() {
        return Err(CdfError::data(format!(
            "package {} at status {} cannot be archived as Parquet",
            manifest.package_hash,
            manifest.lifecycle.status.as_str()
        )));
    }

    let replacing = has_parquet_archive_state(package_dir, &manifest)?;
    if !force {
        match verify_parquet_archive_metadata(package_dir, &manifest) {
            Ok(_) if manifest_parquet_archive(&manifest).is_some() => {
                let metadata = manifest_parquet_archive(&manifest)
                    .expect("checked archive metadata exists")
                    .clone();
                return Ok(persisted_archive_report(
                    manifest.package_hash,
                    PackageArchiveWriteStatus::Skipped,
                    metadata,
                ));
            }
            Ok(_) => {}
            Err(error) => return Err(error),
        }
    }

    let temp_dir = create_archive_temp_dir(package_dir)?;
    let reader = PackageReader::open(package_dir)?;
    let write_result = write_streamed_archive_temp_tree(&reader, &temp_dir).and_then(|metadata| {
        let fidelity = fidelity_report(&manifest.package_hash, &metadata);
        let fidelity_path = temp_dir.join("fidelity.json");
        write_new_file(&fidelity_path, &canonical_json_bytes(&fidelity)?)?;
        sync_directory(&temp_dir)?;
        install_archive_tree(package_dir, &temp_dir)?;
        let mut updated_manifest = manifest.clone();
        updated_manifest.archives = Some(ManifestArchives {
            parquet: Some(metadata.clone()),
        });
        write_manifest_atomic(package_dir, &updated_manifest)?;
        verify_package(package_dir)?;
        Ok(metadata)
    });

    let metadata = match write_result {
        Ok(metadata) => metadata,
        Err(error) => {
            let _ = fs::remove_dir_all(&temp_dir);
            return Err(error);
        }
    };

    Ok(persisted_archive_report(
        manifest.package_hash,
        if replacing {
            PackageArchiveWriteStatus::Replaced
        } else {
            PackageArchiveWriteStatus::Written
        },
        metadata,
    ))
}

fn write_streamed_archive_temp_tree(
    reader: &PackageReader,
    temp_dir: &Path,
) -> Result<ParquetArchiveMetadata> {
    let data_dir = temp_dir.join("data");
    fs::create_dir_all(&data_dir)
        .map_err(|error| io_error(format!("create {}", data_dir.display()), error))?;
    let memory: std::sync::Arc<dyn MemoryCoordinator> = std::sync::Arc::new(
        DeterministicMemoryCoordinator::new(DEFAULT_PROCESS_BUDGET_BYTES, Default::default())?,
    );
    let stream = reader.verified_canonical_segment_stream(memory, 64 * 1024 * 1024)?;
    let mut segments = Vec::with_capacity(reader.manifest().identity.segments.len());
    for segment in stream {
        let segment = segment?;
        let parquet_bytes = transcode_record_batches_to_parquet_bytes(&segment.batches)?;
        let archive_path = archive_segment_path(segment.entry.segment_id.as_str())?;
        let file_name = Path::new(&archive_path).file_name().ok_or_else(|| {
            CdfError::internal(format!("archive path {archive_path} has no file name"))
        })?;
        let path = data_dir.join(file_name);
        write_new_file(&path, &parquet_bytes)?;
        segments.push(ArchiveSegmentMetadata {
            segment_id: segment.entry.segment_id.as_str().to_owned(),
            source_path: segment.entry.path,
            source_byte_count: segment.entry.byte_count,
            source_sha256: segment.entry.sha256,
            source_row_count: segment.entry.row_count,
            archive_path,
            archive_byte_count: parquet_bytes.len() as u64,
            archive_sha256: sha256_hex(&parquet_bytes),
            archive_row_count: segment
                .batches
                .iter()
                .map(|batch| batch.num_rows() as u64)
                .sum(),
        });
    }
    sync_directory(&data_dir)?;
    Ok(ParquetArchiveMetadata {
        format_version: PARQUET_ARCHIVE_FORMAT_VERSION,
        fidelity_report_path: FIDELITY_REPORT_PATH.to_owned(),
        fidelity_statement: ARCHIVE_FIDELITY_STATEMENT.to_owned(),
        segments,
    })
}

pub(crate) fn verify_parquet_archive_metadata(
    package_dir: &Path,
    manifest: &PackageManifest,
) -> Result<Vec<ArchiveSegmentMetadata>> {
    let mut failures = Vec::new();
    let Some(metadata) = manifest_parquet_archive(manifest) else {
        for orphan in collect_parquet_archive_files(package_dir)? {
            failures.push(format!("orphan archive sidecar {orphan}"));
        }
        return archive_verification_result(failures, Vec::new());
    };

    if metadata.format_version != PARQUET_ARCHIVE_FORMAT_VERSION {
        failures.push(format!(
            "archive metadata format_version {} is unsupported",
            metadata.format_version
        ));
    }
    if metadata.fidelity_report_path != FIDELITY_REPORT_PATH {
        failures.push(format!(
            "archive fidelity report path {} does not match {FIDELITY_REPORT_PATH}",
            metadata.fidelity_report_path
        ));
    }
    if metadata.fidelity_statement != ARCHIVE_FIDELITY_STATEMENT {
        failures
            .push("archive fidelity statement does not match the canonical statement".to_owned());
    }

    let mut expected_paths = BTreeSet::from([FIDELITY_REPORT_PATH.to_owned()]);
    let checked_segments =
        verify_archive_segments(manifest, metadata, &mut expected_paths, &mut failures);

    for path in &expected_paths {
        if path == FIDELITY_REPORT_PATH {
            continue;
        }
        let actual = archive_file_entry(package_dir, path);
        match (
            metadata
                .segments
                .iter()
                .find(|segment| segment.archive_path == *path),
            actual,
        ) {
            (Some(expected), Ok(actual)) => {
                if actual.byte_count != expected.archive_byte_count
                    || actual.sha256 != expected.archive_sha256
                {
                    failures.push(format!(
                        "tampered archive sidecar {}: expected {} bytes sha256 {}, got {} bytes sha256 {}",
                        expected.archive_path,
                        expected.archive_byte_count,
                        expected.archive_sha256,
                        actual.byte_count,
                        actual.sha256
                    ));
                }
            }
            (Some(expected), Err(error)) if missing_file_error(&error) => {
                failures.push(format!("missing archive sidecar {}", expected.archive_path));
            }
            (Some(expected), Err(error)) => failures.push(format!(
                "archive sidecar {} could not be read: {}",
                expected.archive_path, error.message
            )),
            (None, _) => {}
        }
    }

    verify_fidelity_report(package_dir, manifest, metadata, &mut failures)?;

    let actual_paths = collect_parquet_archive_files(package_dir)?;
    for orphan in actual_paths.difference(&expected_paths) {
        failures.push(format!("orphan archive sidecar {orphan}"));
    }

    archive_verification_result(failures, checked_segments)
}

fn archive_segment_report(
    entry: &SegmentEntry,
    parquet_bytes: Vec<u8>,
    row_count: u64,
) -> ArchivedSegmentReport {
    ArchivedSegmentReport {
        segment_id: entry.segment_id.as_str().to_owned(),
        source_path: entry.path.clone(),
        source_byte_count: entry.byte_count,
        source_sha256: entry.sha256.clone(),
        source_row_count: entry.row_count,
        parquet_byte_count: parquet_bytes.len() as u64,
        parquet_sha256: sha256_hex(&parquet_bytes),
        parquet_row_count: row_count,
        parquet_bytes,
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn persisted_archive_report(
    package_hash: String,
    status: PackageArchiveWriteStatus,
    metadata: ParquetArchiveMetadata,
) -> PersistedPackageArchiveReport {
    PersistedPackageArchiveReport {
        package_hash,
        format: ARCHIVE_FORMAT.to_owned(),
        status,
        fidelity_report_path: metadata.fidelity_report_path,
        fidelity_statement: metadata.fidelity_statement,
        segments: metadata.segments,
    }
}

fn fidelity_report(
    package_hash: &str,
    metadata: &ParquetArchiveMetadata,
) -> PackageArchiveFidelityReport {
    PackageArchiveFidelityReport {
        package_hash: package_hash.to_owned(),
        source_format: SOURCE_FORMAT.to_owned(),
        archive_format: ARCHIVE_FORMAT.to_owned(),
        fidelity_statement: metadata.fidelity_statement.clone(),
        segments: metadata.segments.clone(),
    }
}

fn write_new_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| io_error(format!("create {}", path.display()), error))?;
    file.write_all(bytes)
        .map_err(|error| io_error(format!("write {}", path.display()), error))?;
    file.sync_all()
        .map_err(|error| io_error(format!("sync {}", path.display()), error))
}

fn install_archive_tree(package_dir: &Path, temp_dir: &Path) -> Result<()> {
    let final_dir = package_path(package_dir, PARQUET_ARCHIVE_DIR);
    let archive_root = package_path(package_dir, ARCHIVE_ROOT);
    fs::create_dir_all(&archive_root)
        .map_err(|error| io_error(format!("create {}", archive_root.display()), error))?;

    if final_dir.exists() {
        let backup_dir = create_backup_path(package_dir);
        fs::rename(&final_dir, &backup_dir).map_err(|error| {
            io_error(
                format!("rename {} to {}", final_dir.display(), backup_dir.display()),
                error,
            )
        })?;
        if let Err(error) = fs::rename(temp_dir, &final_dir).map_err(|error| {
            io_error(
                format!("rename {} to {}", temp_dir.display(), final_dir.display()),
                error,
            )
        }) {
            let _ = fs::rename(&backup_dir, &final_dir);
            return Err(error);
        }
        let _ = fs::remove_dir_all(&backup_dir);
    } else {
        fs::rename(temp_dir, &final_dir).map_err(|error| {
            io_error(
                format!("rename {} to {}", temp_dir.display(), final_dir.display()),
                error,
            )
        })?;
    }
    sync_directory(&archive_root)
}

fn verify_archive_segments(
    manifest: &PackageManifest,
    metadata: &ParquetArchiveMetadata,
    expected_paths: &mut BTreeSet<String>,
    failures: &mut Vec<String>,
) -> Vec<ArchiveSegmentMetadata> {
    let mut checked_segments = Vec::new();
    for (index, source) in manifest.identity.segments.iter().enumerate() {
        let Some(segment) = metadata.segments.get(index) else {
            failures.push(format!(
                "archive metadata missing segment {}",
                source.segment_id.as_str()
            ));
            continue;
        };
        if segment.segment_id != source.segment_id.as_str() {
            failures.push(format!(
                "archive metadata segment {} at index {} does not match manifest segment {}",
                segment.segment_id,
                index,
                source.segment_id.as_str()
            ));
        }
        let expected_path = match archive_segment_path(source.segment_id.as_str()) {
            Ok(path) => path,
            Err(error) => {
                failures.push(error.message);
                continue;
            }
        };
        expected_paths.insert(expected_path.clone());
        if segment.archive_path != expected_path {
            failures.push(format!(
                "archive path for segment {} is {}, expected {}",
                source.segment_id.as_str(),
                segment.archive_path,
                expected_path
            ));
        }
        if segment.source_path != source.path
            || segment.source_byte_count != source.byte_count
            || segment.source_sha256 != source.sha256
            || segment.source_row_count != source.row_count
        {
            failures.push(format!(
                "archive source metadata mismatch for segment {}",
                source.segment_id.as_str()
            ));
        }
        checked_segments.push(segment.clone());
    }

    for segment in metadata
        .segments
        .iter()
        .skip(manifest.identity.segments.len())
    {
        failures.push(format!(
            "archive metadata has extra segment {}",
            segment.segment_id
        ));
    }

    checked_segments
}

fn verify_fidelity_report(
    package_dir: &Path,
    manifest: &PackageManifest,
    metadata: &ParquetArchiveMetadata,
    failures: &mut Vec<String>,
) -> Result<()> {
    let path = package_path(package_dir, FIDELITY_REPORT_PATH);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            failures.push(format!(
                "missing archive fidelity report {FIDELITY_REPORT_PATH}"
            ));
            return Ok(());
        }
        Err(error) => {
            failures.push(format!(
                "archive fidelity report {FIDELITY_REPORT_PATH} could not be read: {}",
                io_error(format!("read {}", path.display()), error).message
            ));
            return Ok(());
        }
    };
    let actual: PackageArchiveFidelityReport = match serde_json::from_slice(&bytes) {
        Ok(actual) => actual,
        Err(error) => {
            failures.push(format!("archive fidelity report mismatch: {error}"));
            return Ok(());
        }
    };
    let expected = fidelity_report(&manifest.package_hash, metadata);
    if actual != expected {
        failures.push("archive fidelity report mismatch".to_owned());
        return Ok(());
    }
    let canonical = canonical_json_bytes(&expected)?;
    if bytes != canonical {
        failures.push("archive fidelity report is not canonical JSON".to_owned());
    }
    Ok(())
}

fn archive_verification_result(
    failures: Vec<String>,
    checked_segments: Vec<ArchiveSegmentMetadata>,
) -> Result<Vec<ArchiveSegmentMetadata>> {
    if failures.is_empty() {
        Ok(checked_segments)
    } else {
        Err(CdfError::data(format!(
            "package archive verification failed: {}",
            failures.join("; ")
        )))
    }
}

fn has_parquet_archive_state(package_dir: &Path, manifest: &PackageManifest) -> Result<bool> {
    Ok(manifest_parquet_archive(manifest).is_some()
        || package_path(package_dir, PARQUET_ARCHIVE_DIR).exists())
}

fn manifest_parquet_archive(manifest: &PackageManifest) -> Option<&ParquetArchiveMetadata> {
    manifest
        .archives
        .as_ref()
        .and_then(|archives| archives.parquet.as_ref())
}

fn archive_segment_path(segment_id: &str) -> Result<String> {
    if segment_id.contains('/')
        || segment_id.contains('\\')
        || segment_id.contains("..")
        || segment_id.trim().is_empty()
        || segment_id == "."
    {
        return Err(CdfError::data(format!(
            "segment id cannot be used as an archive file name: {segment_id:?}"
        )));
    }
    Ok(format!("{PARQUET_DATA_DIR}/{segment_id}.parquet"))
}

fn collect_parquet_archive_files(package_dir: &Path) -> Result<BTreeSet<String>> {
    let archive_dir = package_path(package_dir, PARQUET_ARCHIVE_DIR);
    if !archive_dir.exists() {
        return Ok(BTreeSet::new());
    }
    let mut files = BTreeSet::new();
    collect_archive_files(package_dir, &archive_dir, &mut files)?;
    Ok(files)
}

fn collect_archive_files(
    package_dir: &Path,
    directory: &Path,
    files: &mut BTreeSet<String>,
) -> Result<()> {
    let mut entries = fs::read_dir(directory)
        .map_err(|error| io_error(format!("read directory {}", directory.display()), error))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| io_error(format!("read directory {}", directory.display()), error))?;
    entries.sort_by_key(|entry| entry.path());

    for entry in entries {
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| io_error(format!("stat {}", path.display()), error))?;
        if file_type.is_dir() {
            collect_archive_files(package_dir, &path, files)?;
        } else if file_type.is_file() {
            files.insert(relative_archive_path(package_dir, &path)?);
        }
    }
    Ok(())
}

fn relative_archive_path(package_dir: &Path, path: &Path) -> Result<String> {
    let relative = path.strip_prefix(package_dir).map_err(|error| {
        CdfError::internal(format!(
            "path {} is not under {}: {error}",
            path.display(),
            package_dir.display()
        ))
    })?;
    let mut parts = Vec::new();
    for component in relative.components() {
        match component {
            std::path::Component::Normal(part) => {
                let part = part.to_str().ok_or_else(|| {
                    CdfError::data(format!("path is not valid UTF-8: {}", path.display()))
                })?;
                parts.push(part.to_owned());
            }
            _ => {
                return Err(CdfError::data(format!(
                    "archive path must stay inside the package: {}",
                    path.display()
                )));
            }
        }
    }
    Ok(parts.join("/"))
}

fn archive_file_entry(package_dir: &Path, relative_path: &str) -> Result<FileDigest> {
    file_entry(&package_path(package_dir, relative_path))
}

fn file_entry(path: &Path) -> Result<FileDigest> {
    let mut file =
        File::open(path).map_err(|error| io_error(format!("open {}", path.display()), error))?;
    let mut hasher = Sha256::new();
    let byte_count = std::io::copy(&mut file, &mut hasher)
        .map_err(|error| io_error(format!("hash {}", path.display()), error))?;
    Ok(FileDigest {
        byte_count,
        sha256: hex::encode(hasher.finalize()),
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FileDigest {
    byte_count: u64,
    sha256: String,
}

fn create_archive_temp_dir(package_dir: &Path) -> Result<PathBuf> {
    let tmp_root = package_dir.join(ARCHIVE_ROOT).join(".tmp");
    fs::create_dir_all(&tmp_root)
        .map_err(|error| io_error(format!("create {}", tmp_root.display()), error))?;
    for _ in 0..100 {
        let counter = ARCHIVE_TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let temp_dir = tmp_root.join(format!("parquet-{}-{counter}", std::process::id()));
        match fs::create_dir(&temp_dir) {
            Ok(()) => return Ok(temp_dir),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(io_error(format!("create {}", temp_dir.display()), error)),
        }
    }
    Err(CdfError::internal(format!(
        "could not create archive temporary directory under {}",
        tmp_root.display()
    )))
}

fn create_backup_path(package_dir: &Path) -> PathBuf {
    let counter = ARCHIVE_TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    package_dir
        .join(ARCHIVE_ROOT)
        .join(".tmp")
        .join(format!("previous-parquet-{}-{counter}", std::process::id()))
}

fn cleanup_stale_archive_temps(package_dir: &Path) -> Result<()> {
    let tmp_root = package_dir.join(ARCHIVE_ROOT).join(".tmp");
    match fs::remove_dir_all(&tmp_root) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(format!("remove {}", tmp_root.display()), error)),
    }
}

fn missing_file_error(error: &CdfError) -> bool {
    error.message.contains(": No such file or directory")
        || error.message.contains("os error 2")
        || error
            .message
            .contains("The system cannot find the file specified")
}
