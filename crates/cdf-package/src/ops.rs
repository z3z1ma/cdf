use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    path::Path,
};

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
    storage::{
        atomic_write, collect_identity_file_entries, io_error, normalize_artifact_path,
        package_path, write_manifest_atomic,
    },
};

pub fn read_manifest(package_dir: impl AsRef<Path>) -> Result<PackageManifest> {
    let path = package_dir.as_ref().join(MANIFEST_FILE);
    let bytes =
        fs::read(&path).map_err(|error| io_error(format!("read {}", path.display()), error))?;
    let manifest: PackageManifest = serde_json::from_slice(&bytes).map_err(json_error)?;
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
    let package_dir = package_dir.as_ref();
    let mut report = verify_package_identity(package_dir)?;
    let manifest = read_manifest(package_dir)?;
    verify_contract_evolution_versions(package_dir, &manifest)?;
    report.checked_archives = verify_parquet_archive_metadata(package_dir, &manifest)?;
    Ok(report)
}

fn verify_contract_evolution_versions(
    package_dir: &Path,
    manifest: &PackageManifest,
) -> Result<()> {
    const PATH: &str = "schema/contract-evolution.json";
    if !manifest.identity.files.iter().any(|file| file.path == PATH) {
        return Ok(());
    }
    let bytes = fs::read(package_dir.join(PATH))
        .map_err(|error| io_error(format!("read {PATH}"), error))?;
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
    let package_dir = package_dir.as_ref();
    let manifest = read_manifest(package_dir)?;
    let mut failures = Vec::new();

    let actual_hash = manifest_identity_hash(&manifest.identity)?;
    if actual_hash != manifest.package_hash {
        failures.push(format!(
            "manifest identity hash mismatch: expected {}, got {}",
            manifest.package_hash, actual_hash
        ));
    }
    if manifest.signature.signing_input != manifest.package_hash {
        failures.push(format!(
            "signature signing input {} does not match package hash {}",
            manifest.signature.signing_input, manifest.package_hash
        ));
    }

    let expected_paths: BTreeSet<&str> = manifest
        .identity
        .files
        .iter()
        .map(|entry| entry.path.as_str())
        .collect();
    let actual_files = collect_identity_file_entries(package_dir)?;
    let actual_paths: BTreeSet<&str> = actual_files
        .iter()
        .map(|entry| entry.path.as_str())
        .collect();

    for unexpected in actual_paths.difference(&expected_paths) {
        failures.push(format!("unexpected identity file {unexpected}"));
    }

    let actual_by_path: BTreeMap<&str, &FileEntry> = actual_files
        .iter()
        .map(|entry| (entry.path.as_str(), entry))
        .collect();
    let mut checked_files = Vec::with_capacity(manifest.identity.files.len());
    for expected in &manifest.identity.files {
        match actual_by_path.get(expected.path.as_str()) {
            Some(actual)
                if actual.byte_count == expected.byte_count && actual.sha256 == expected.sha256 =>
            {
                checked_files.push((*actual).clone());
            }
            Some(actual) => failures.push(format!(
                "tampered identity file {}: expected {} bytes sha256 {}, got {} bytes sha256 {}",
                expected.path,
                expected.byte_count,
                expected.sha256,
                actual.byte_count,
                actual.sha256
            )),
            None => failures.push(format!("missing identity file {}", expected.path)),
        }
    }

    for segment in &manifest.identity.segments {
        match manifest
            .identity
            .files
            .iter()
            .find(|entry| entry.path == segment.path)
        {
            Some(file)
                if file.byte_count == segment.byte_count && file.sha256 == segment.sha256 => {}
            Some(file) => failures.push(format!(
                "segment {} does not match file entry {}: segment {} bytes sha256 {}, file {} bytes sha256 {}",
                segment.segment_id.as_str(),
                segment.path,
                segment.byte_count,
                segment.sha256,
                file.byte_count,
                file.sha256
            )),
            None => failures.push(format!(
                "segment {} references missing file entry {}",
                segment.segment_id.as_str(),
                segment.path
            )),
        }
    }

    if !failures.is_empty() {
        return Err(CdfError::data(format!(
            "package verification failed: {}",
            failures.join("; ")
        )));
    }

    Ok(VerificationReport {
        package_hash: manifest.package_hash,
        checked_files,
        checked_archives: Vec::new(),
    })
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

pub fn read_segment_file(
    package_dir: impl AsRef<Path>,
    relative_path: impl AsRef<Path>,
) -> Result<Vec<RecordBatch>> {
    let relative_path = normalize_artifact_path(relative_path.as_ref())?;
    if !relative_path.starts_with("data/") {
        return Err(CdfError::data(format!(
            "segment path must live under data/: {relative_path}"
        )));
    }
    let path = package_path(package_dir.as_ref(), &relative_path);
    let file =
        File::open(&path).map_err(|error| io_error(format!("open {}", path.display()), error))?;
    let reader = FileReader::try_new(file, None).map_err(CdfError::from)?;
    reader
        .map(|batch| batch.map_err(CdfError::from))
        .collect::<Result<Vec<_>>>()
}
