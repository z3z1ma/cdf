use std::{fs, io::BufReader, path::Path};

use arrow_array::RecordBatch;
use arrow_ipc::reader::FileReader;
use cdf_kernel::{CdfError, Receipt, Result};
use cdf_package_contract::{
    FileEntry, MANIFEST_FILE, MANIFEST_VERSION, PackageManifest, PackageStatus, RECEIPTS_FILE,
    TombstoneReport, VerificationReport,
};

use crate::{
    archive::verify_parquet_archive_metadata,
    json::{canonical_json_bytes, json_error, manifest_identity_hash},
    package_fs::{PackageEntryKind, PackageRoot},
    storage::{
        atomic_write, io_error, package_path, validate_manifest_identity_paths,
        write_manifest_atomic,
    },
};

pub fn read_manifest(package_dir: impl AsRef<Path>) -> Result<PackageManifest> {
    let root = PackageRoot::open(package_dir.as_ref())?;
    read_manifest_from_root(&root)
}

pub(crate) fn read_manifest_from_root(root: &PackageRoot) -> Result<PackageManifest> {
    let file = root.open_regular_file(MANIFEST_FILE)?.into_std();
    let manifest: PackageManifest =
        serde_json::from_reader(BufReader::new(file)).map_err(json_error)?;
    if manifest.manifest_version != MANIFEST_VERSION
        || manifest.identity.manifest_version != MANIFEST_VERSION
    {
        return Err(CdfError::data(format!(
            "package manifest/storage version must be {MANIFEST_VERSION}; observed manifest {} identity {}",
            manifest.manifest_version, manifest.identity.manifest_version
        )));
    }
    cdf_package_contract::validate_segment_ordinal_manifest(&manifest.identity.segments)?;
    Ok(manifest)
}

pub fn update_package_status(
    package_dir: impl AsRef<Path>,
    status: PackageStatus,
) -> Result<PackageManifest> {
    let package_dir = package_dir.as_ref();
    let mut manifest = read_manifest(package_dir)?;
    manifest.lifecycle.status = status;
    write_manifest_atomic(package_dir, &manifest)?;
    Ok(manifest)
}

pub fn append_receipt(package_dir: impl AsRef<Path>, receipt: Receipt) -> Result<Vec<Receipt>> {
    let package_dir = package_dir.as_ref();
    let manifest = read_manifest(package_dir)?;
    if receipt.package_hash.as_str() != manifest.package_hash {
        return Err(CdfError::data(format!(
            "receipt package hash {} does not match manifest package hash {}",
            receipt.package_hash.as_str(),
            manifest.package_hash
        )));
    }

    let mut receipts = read_receipts(package_dir)?;
    receipts.push(receipt);
    let path = package_path(package_dir, RECEIPTS_FILE);
    let bytes = canonical_json_bytes(&receipts)?;
    atomic_write(&path, &bytes)?;
    Ok(receipts)
}

pub fn read_receipts(package_dir: impl AsRef<Path>) -> Result<Vec<Receipt>> {
    let path = package_path(package_dir.as_ref(), RECEIPTS_FILE);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let bytes =
        fs::read(&path).map_err(|error| io_error(format!("read {}", path.display()), error))?;
    serde_json::from_slice(&bytes).map_err(json_error)
}

pub fn verify_package(package_dir: impl AsRef<Path>) -> Result<VerificationReport> {
    let root = PackageRoot::open(package_dir.as_ref())?;
    let manifest = read_manifest_from_root(&root)?;
    verify_package_from_root(&root, &manifest)
}

pub(crate) fn verify_package_from_root(
    root: &PackageRoot,
    manifest: &PackageManifest,
) -> Result<VerificationReport> {
    let mut report = verify_package_identity_with(root, manifest)?;
    verify_contract_evolution_versions(root, manifest)?;
    report.checked_archive_count = verify_parquet_archive_metadata(root, manifest)?;
    Ok(report)
}

fn verify_contract_evolution_versions(
    root: &PackageRoot,
    manifest: &PackageManifest,
) -> Result<()> {
    const PATH: &str = "schema/contract-evolution.json";
    if !manifest.identity.files.iter().any(|file| file.path == PATH) {
        return Ok(());
    }
    let bytes = root.read(PATH)?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(json_error)?;
    if value.get("version").and_then(serde_json::Value::as_u64) != Some(1) {
        return Err(CdfError::data(
            "schema/contract-evolution.json has an unsupported or missing version",
        ));
    }
    if value.get("residual_capture").is_some_and(|capture| {
        !capture.is_null() && capture.get("version").and_then(serde_json::Value::as_u64) != Some(1)
    }) {
        return Err(CdfError::data(
            "schema/contract-evolution.json has an unsupported residual-capture version",
        ));
    }
    if value
        .get("residual_decisions")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|decisions| {
            decisions.iter().any(|decision| {
                decision.get("version").and_then(serde_json::Value::as_u64) != Some(1)
            })
        })
    {
        return Err(CdfError::data(
            "schema/contract-evolution.json has an unsupported residual-decision version",
        ));
    }
    Ok(())
}

pub fn verify_package_identity(package_dir: impl AsRef<Path>) -> Result<VerificationReport> {
    let root = PackageRoot::open(package_dir.as_ref())?;
    let manifest = read_manifest_from_root(&root)?;
    verify_package_identity_with(&root, &manifest)
}

fn verify_package_identity_with(
    root: &PackageRoot,
    manifest: &PackageManifest,
) -> Result<VerificationReport> {
    validate_manifest_identity_paths(&manifest.identity.files)?;

    let actual_hash = manifest_identity_hash(&manifest.identity)?;
    if actual_hash != manifest.package_hash {
        return Err(verification_failure(format!(
            "manifest identity hash mismatch: expected {}, got {}",
            manifest.package_hash, actual_hash
        )));
    }
    if manifest.signature.signing_input != manifest.package_hash {
        return Err(verification_failure(format!(
            "signature signing input {} does not match package hash {}",
            manifest.signature.signing_input, manifest.package_hash
        )));
    }

    if let Some(failure) = first_unexpected_identity_failure(root, &manifest.identity.files)? {
        return Err(verification_failure(failure));
    }

    for expected in &manifest.identity.files {
        match root.file_entry(&expected.path)? {
            Some(actual)
                if actual.byte_count == expected.byte_count && actual.sha256 == expected.sha256 => {
            }
            Some(actual) => {
                return Err(verification_failure(format!(
                    "tampered identity file {}: expected {} bytes sha256 {}, got {} bytes sha256 {}",
                    expected.path,
                    expected.byte_count,
                    expected.sha256,
                    actual.byte_count,
                    actual.sha256
                )));
            }
            None => {
                return Err(verification_failure(format!(
                    "missing identity file {}",
                    expected.path
                )));
            }
        }
    }

    for segment in &manifest.identity.segments {
        match manifest
            .identity
            .files
            .binary_search_by(|entry| crate::storage::portable_path_cmp(&entry.path, &segment.path))
        {
            Ok(index) => match &manifest.identity.files[index] {
                file if file.byte_count == segment.byte_count && file.sha256 == segment.sha256 => {}
                file => {
                    return Err(verification_failure(format!(
                        "segment {} does not match file entry {}: segment {} bytes sha256 {}, file {} bytes sha256 {}",
                        segment.segment_id.as_str(),
                        segment.path,
                        segment.byte_count,
                        segment.sha256,
                        file.byte_count,
                        file.sha256
                    )));
                }
            },
            Err(_) => {
                return Err(verification_failure(format!(
                    "segment {} references missing file entry {}",
                    segment.segment_id.as_str(),
                    segment.path
                )));
            }
        }
    }

    Ok(VerificationReport {
        package_hash: manifest.package_hash.clone(),
        checked_file_count: manifest.identity.files.len(),
        checked_archive_count: 0,
    })
}

fn first_unexpected_identity_failure(
    root: &PackageRoot,
    expected: &[FileEntry],
) -> Result<Option<String>> {
    let mut first: Option<(String, PackageEntryKind)> = None;
    root.visit_identity_entries(|path, kind| {
        let is_expected = expected
            .binary_search_by(|entry| crate::storage::portable_path_cmp(&entry.path, &path))
            .is_ok();
        let is_before_first = first.as_ref().is_none_or(|(candidate, _)| {
            crate::storage::portable_path_cmp(&path, candidate).is_lt()
        });
        if !is_expected && is_before_first {
            first = Some((path, kind));
        }
        Ok(())
    })?;
    Ok(first.map(|(path, kind)| {
        let label = match kind {
            PackageEntryKind::RegularFile => "unexpected identity file",
            PackageEntryKind::NonRegular => "unexpected non-regular identity entry",
        };
        format!("{label} {path}")
    }))
}

fn verification_failure(message: impl std::fmt::Display) -> CdfError {
    CdfError::data(format!("package verification failed: {message}"))
}

pub fn tombstone_package(package_dir: impl AsRef<Path>) -> Result<TombstoneReport> {
    let package_dir = package_dir.as_ref();
    let manifest = read_manifest(package_dir)?;
    let mut removed_files = Vec::new();

    for entry in &manifest.identity.files {
        let path = package_path(package_dir, &entry.path);
        if path.exists() {
            fs::remove_file(&path)
                .map_err(|error| io_error(format!("remove {}", path.display()), error))?;
            removed_files.push(entry.path.clone());
        }
    }
    removed_files.sort();

    update_package_status(package_dir, PackageStatus::Archived)?;
    Ok(TombstoneReport {
        package_hash: manifest.package_hash,
        removed_files,
    })
}

pub(crate) fn read_segment_file_from_root(
    root: &PackageRoot,
    relative_path: &str,
) -> Result<Vec<RecordBatch>> {
    let file = root.open_regular_file(relative_path)?.into_std();
    let reader = FileReader::try_new(file, None).map_err(CdfError::from)?;
    reader
        .map(|batch| batch.map_err(CdfError::from))
        .collect::<Result<Vec<_>>>()
}
