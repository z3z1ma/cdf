use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use cdf_kernel::{CdfError, Result};
use cdf_memory::{
    ConsumerKey, DeterministicMemoryCoordinator, MemoryClass, MemoryCoordinator,
    ReservationRequest, record_batch_retained_bytes, reserve_blocking,
};
use cdf_package_contract::{
    ArchiveSegmentMetadata, ManifestArchives, PackageManifest, ParquetArchiveMetadata,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    json::canonical_json_bytes,
    ops::{read_manifest, verify_package, verify_package_identity},
    package_fs::{PackageEntryKind, PackageRoot},
    parquet::transcode_record_batches_to_bounded_parquet_bytes,
    reader::PackageReader,
    storage::{
        io_error, package_path, portable_path_cmp, sync_directory,
        validate_canonical_relative_path, write_manifest_atomic,
    },
};

pub const ARCHIVE_FIDELITY_STATEMENT: &str = "Arrow IPC remains the canonical package data. Parquet bytes are an archive/interchange projection; Arrow field metadata and other Arrow-only semantics are not promoted to canonical Parquet truth.";
const PARQUET_ARCHIVE_FORMAT_VERSION: u16 = 1;
const ARCHIVE_ROOT: &str = "archive";
const PARQUET_ARCHIVE_DIR: &str = "archive/parquet";
const PARQUET_DATA_DIR: &str = "archive/parquet/data";
const FIDELITY_REPORT_PATH: &str = "archive/parquet/fidelity.json";
const SOURCE_FORMAT: &str = "arrow_ipc_lz4";
const ARCHIVE_FORMAT: &str = "parquet";
pub(crate) const ARCHIVE_SEGMENT_WINDOW_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const ARCHIVE_SEGMENT_MEMORY_CONSUMER: &str = "package-parquet-archive-segment";

static ARCHIVE_TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

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

pub fn persist_package_parquet_archive(
    package_dir: impl AsRef<Path>,
    force: bool,
) -> Result<PersistedPackageArchiveReport> {
    let package_dir = package_dir.as_ref();
    cleanup_stale_archive_temps(package_dir)?;
    verify_package_identity(package_dir)?;

    let manifest = read_manifest(package_dir)?;
    let package_root = PackageRoot::open(package_dir)?;
    if !manifest.lifecycle.status.is_archivable() {
        return Err(CdfError::data(format!(
            "package {} at status {} cannot be archived as Parquet",
            manifest.package_hash,
            manifest.lifecycle.status.as_str()
        )));
    }

    let replacing = has_parquet_archive_state(&package_root, &manifest)?;
    if !force {
        match verify_parquet_archive_metadata(&package_root, &manifest) {
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
    let memory: std::sync::Arc<dyn MemoryCoordinator> = std::sync::Arc::new(
        DeterministicMemoryCoordinator::new(ARCHIVE_SEGMENT_WINDOW_BYTES, Default::default())?,
    );
    write_streamed_archive_temp_tree_with_memory(
        reader,
        temp_dir,
        memory,
        ARCHIVE_SEGMENT_WINDOW_BYTES,
    )
}

pub(crate) fn write_streamed_archive_temp_tree_with_memory(
    reader: &PackageReader,
    temp_dir: &Path,
    memory: std::sync::Arc<dyn MemoryCoordinator>,
    maximum_window_bytes: u64,
) -> Result<ParquetArchiveMetadata> {
    if maximum_window_bytes == 0 {
        return Err(CdfError::contract(
            "package Parquet archive segment window must be nonzero",
        ));
    }
    if maximum_window_bytes > memory.snapshot().budget_bytes {
        return Err(CdfError::data(format!(
            "package Parquet archive segment window {maximum_window_bytes} exceeds managed budget {}",
            memory.snapshot().budget_bytes
        )));
    }
    let data_dir = temp_dir.join("data");
    fs::create_dir_all(&data_dir)
        .map_err(|error| io_error(format!("create {}", data_dir.display()), error))?;
    let mut segments = Vec::new();
    reader.for_each_identity_segment(&mut |entry| {
        let request = ReservationRequest::new(
            ConsumerKey::new(ARCHIVE_SEGMENT_MEMORY_CONSUMER, MemoryClass::Package)?,
            maximum_window_bytes,
        )?
        .as_minimum_working_set();
        let _window = reserve_blocking(std::sync::Arc::clone(&memory), &request)?;
        let batches = reader
            .read_segment(&entry.segment_id)?
            .into_iter()
            .map(cdf_package_contract::strip_package_row_ord)
            .collect::<Result<Vec<_>>>()?;
        let retained_arrow_bytes = batches.iter().try_fold(0_u64, |total, batch| {
            total
                .checked_add(record_batch_retained_bytes(batch)?)
                .ok_or_else(|| CdfError::data("archive segment retained Arrow memory overflow"))
        })?;
        if retained_arrow_bytes > maximum_window_bytes {
            return Err(CdfError::data(format!(
                "segment {} retains {retained_arrow_bytes} Arrow bytes above its {maximum_window_bytes}-byte package Parquet archive window",
                entry.segment_id
            )));
        }
        let row_count = batches.iter().try_fold(0_u64, |total, batch| {
            total
                .checked_add(
                    u64::try_from(batch.num_rows())
                        .map_err(|_| CdfError::data("archive segment row count exceeds u64"))?,
                )
                .ok_or_else(|| CdfError::data("archive segment row count overflow"))
        })?;
        if row_count != entry.row_count {
            return Err(CdfError::data(format!(
                "segment {} manifest row count {} differs from package data {row_count}",
                entry.segment_id, entry.row_count
            )));
        }
        let maximum_parquet_bytes = maximum_window_bytes - retained_arrow_bytes;
        let parquet_bytes =
            transcode_record_batches_to_bounded_parquet_bytes(&batches, maximum_parquet_bytes)?;
        let retained_parquet_bytes = u64::try_from(parquet_bytes.capacity())
            .map_err(|_| CdfError::data("archive Parquet output allocation exceeds u64"))?;
        let retained_window_bytes = retained_arrow_bytes
            .checked_add(retained_parquet_bytes)
            .ok_or_else(|| CdfError::data("archive segment retained memory overflow"))?;
        if retained_window_bytes > maximum_window_bytes {
            return Err(CdfError::data(format!(
                "segment {} retains {retained_arrow_bytes} Arrow bytes plus {retained_parquet_bytes} Parquet output bytes, above its {maximum_window_bytes}-byte package Parquet archive window",
                entry.segment_id
            )));
        }
        let archive_path = archive_segment_path(entry.segment_id.as_str())?;
        let file_name = Path::new(&archive_path).file_name().ok_or_else(|| {
            CdfError::internal(format!("archive path {archive_path} has no file name"))
        })?;
        let path = data_dir.join(file_name);
        write_new_file(&path, &parquet_bytes)?;
        segments.push(ArchiveSegmentMetadata {
            segment_id: entry.segment_id.as_str().to_owned(),
            source_path: entry.path.clone(),
            source_byte_count: entry.byte_count,
            source_sha256: entry.sha256.clone(),
            source_row_count: entry.row_count,
            archive_path,
            archive_byte_count: parquet_bytes.len() as u64,
            archive_sha256: sha256_hex(&parquet_bytes),
            archive_row_count: row_count,
        });
        Ok(())
    })?;
    sync_directory(&data_dir)?;
    Ok(ParquetArchiveMetadata {
        format_version: PARQUET_ARCHIVE_FORMAT_VERSION,
        fidelity_report_path: FIDELITY_REPORT_PATH.to_owned(),
        fidelity_statement: ARCHIVE_FIDELITY_STATEMENT.to_owned(),
        segments,
    })
}

pub(crate) fn verify_parquet_archive_metadata(
    package_root: &PackageRoot,
    manifest: &PackageManifest,
) -> Result<usize> {
    let Some(metadata) = manifest_parquet_archive(manifest) else {
        if let Some((path, _)) = first_archive_entry(package_root, |_| false)? {
            return Err(archive_verification_failure(format!(
                "orphan archive sidecar {path}"
            )));
        }
        return Ok(0);
    };

    if metadata.format_version != PARQUET_ARCHIVE_FORMAT_VERSION {
        return Err(archive_verification_failure(format!(
            "archive metadata format_version {} is unsupported",
            metadata.format_version
        )));
    }
    if metadata.fidelity_report_path != FIDELITY_REPORT_PATH {
        return Err(archive_verification_failure(format!(
            "archive fidelity report path {} does not match {FIDELITY_REPORT_PATH}",
            metadata.fidelity_report_path
        )));
    }
    if metadata.fidelity_statement != ARCHIVE_FIDELITY_STATEMENT {
        return Err(archive_verification_failure(
            "archive fidelity statement does not match the canonical statement",
        ));
    }

    verify_archive_segments(manifest, metadata)?;

    for expected in &metadata.segments {
        match package_root.file_entry(&expected.archive_path) {
            Ok(Some(actual)) => {
                if actual.byte_count != expected.archive_byte_count
                    || actual.sha256 != expected.archive_sha256
                {
                    return Err(archive_verification_failure(format!(
                        "tampered archive sidecar {}: expected {} bytes sha256 {}, got {} bytes sha256 {}",
                        expected.archive_path,
                        expected.archive_byte_count,
                        expected.archive_sha256,
                        actual.byte_count,
                        actual.sha256
                    )));
                }
            }
            Ok(None) => {
                return Err(archive_verification_failure(format!(
                    "missing archive sidecar {}",
                    expected.archive_path
                )));
            }
            Err(error) => {
                return Err(archive_verification_failure(format!(
                    "archive sidecar {} could not be read: {}",
                    expected.archive_path, error.message
                )));
            }
        }
    }

    verify_fidelity_report(package_root, manifest, metadata)?;

    let expected_entry_count = metadata
        .segments
        .len()
        .checked_add(1)
        .ok_or_else(|| CdfError::data("archive expected-entry count overflow"))?;
    if archive_entry_count(package_root)? != expected_entry_count {
        if let Some((path, kind)) = first_archive_entry(package_root, |path| {
            path == FIDELITY_REPORT_PATH
                || metadata
                    .segments
                    .iter()
                    .any(|segment| segment.archive_path == path)
        })? {
            let label = match kind {
                PackageEntryKind::RegularFile => "orphan archive sidecar",
                PackageEntryKind::NonRegular => "unexpected non-regular archive entry",
            };
            return Err(archive_verification_failure(format!("{label} {path}")));
        }
        return Err(archive_verification_failure(format!(
            "archive contains an unexpected entry count: expected {expected_entry_count}"
        )));
    }

    Ok(metadata.segments.len())
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
) -> Result<()> {
    if metadata.segments.len() != manifest.identity.segments.len() {
        return Err(archive_verification_failure(format!(
            "archive metadata has {} segments, expected {}",
            metadata.segments.len(),
            manifest.identity.segments.len()
        )));
    }
    for (index, source) in manifest.identity.segments.iter().enumerate() {
        let segment = &metadata.segments[index];
        if segment.segment_id != source.segment_id.as_str() {
            return Err(archive_verification_failure(format!(
                "archive metadata segment {} at index {} does not match manifest segment {}",
                segment.segment_id,
                index,
                source.segment_id.as_str()
            )));
        }
        let expected_path = archive_segment_path(source.segment_id.as_str())?;
        validate_canonical_relative_path(&segment.archive_path)?;
        if segment.archive_path != expected_path {
            return Err(archive_verification_failure(format!(
                "archive path for segment {} is {}, expected {}",
                source.segment_id.as_str(),
                segment.archive_path,
                expected_path
            )));
        }
        if segment.source_path != source.path
            || segment.source_byte_count != source.byte_count
            || segment.source_sha256 != source.sha256
            || segment.source_row_count != source.row_count
        {
            return Err(archive_verification_failure(format!(
                "archive source metadata mismatch for segment {}",
                source.segment_id.as_str()
            )));
        }
    }
    Ok(())
}

fn verify_fidelity_report(
    package_root: &PackageRoot,
    manifest: &PackageManifest,
    metadata: &ParquetArchiveMetadata,
) -> Result<()> {
    let bytes = match package_root.read_optional(FIDELITY_REPORT_PATH) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => {
            return Err(archive_verification_failure(format!(
                "missing archive fidelity report {FIDELITY_REPORT_PATH}"
            )));
        }
        Err(error) => {
            return Err(archive_verification_failure(format!(
                "archive fidelity report {FIDELITY_REPORT_PATH} could not be read: {}",
                error.message
            )));
        }
    };
    let actual: PackageArchiveFidelityReport = match serde_json::from_slice(&bytes) {
        Ok(actual) => actual,
        Err(error) => {
            return Err(archive_verification_failure(format!(
                "archive fidelity report mismatch: {error}"
            )));
        }
    };
    let expected = fidelity_report(&manifest.package_hash, metadata);
    if actual != expected {
        return Err(archive_verification_failure(
            "archive fidelity report mismatch",
        ));
    }
    let canonical = canonical_json_bytes(&expected)?;
    if bytes != canonical {
        return Err(archive_verification_failure(
            "archive fidelity report is not canonical JSON",
        ));
    }
    Ok(())
}

fn archive_verification_failure(message: impl std::fmt::Display) -> CdfError {
    CdfError::data(format!("package archive verification failed: {message}"))
}

fn has_parquet_archive_state(
    package_root: &PackageRoot,
    manifest: &PackageManifest,
) -> Result<bool> {
    Ok(manifest_parquet_archive(manifest).is_some()
        || first_archive_entry(package_root, |_| false)?.is_some())
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

fn first_archive_entry(
    package_root: &PackageRoot,
    mut is_expected: impl FnMut(&str) -> bool,
) -> Result<Option<(String, PackageEntryKind)>> {
    let mut first: Option<(String, PackageEntryKind)> = None;
    package_root.visit_tree_entries(PARQUET_ARCHIVE_DIR, |path, kind| {
        if !is_expected(&path)
            && first
                .as_ref()
                .is_none_or(|(candidate, _)| portable_path_cmp(&path, candidate).is_lt())
        {
            first = Some((path, kind));
        }
        Ok(())
    })?;
    Ok(first)
}

fn archive_entry_count(package_root: &PackageRoot) -> Result<usize> {
    let mut count = 0_usize;
    package_root.visit_tree_entries(PARQUET_ARCHIVE_DIR, |_, _| {
        count = count
            .checked_add(1)
            .ok_or_else(|| CdfError::data("archive entry count overflow"))?;
        Ok(())
    })?;
    Ok(count)
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
