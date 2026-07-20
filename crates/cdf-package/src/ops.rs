use std::{fs, io::BufReader, path::Path};

use arrow_array::RecordBatch;
use arrow_ipc::reader::FileReader;
use cdf_kernel::{CdfError, Receipt, Result};
use cdf_package_contract::{
    FileEntry, MANIFEST_FILE, MANIFEST_VERSION, PackageManifest, PackageStatus, RECEIPTS_FILE,
    TombstoneReport, VerificationReport,
};

use crate::{
    archive::{verify_parquet_archive_absence, verify_parquet_archive_metadata},
    json::{canonical_json_bytes, json_error},
    manifest_stream::{
        ManifestFileStream, ManifestSegmentStream, PackageManifestHeader,
        rewrite_manifest_lifecycle, stored_manifest_identity_hash, visit_package_manifest,
    },
    package_fs::{PackageEntryKind, PackageRoot},
    storage::{
        ArtifactDurability, AtomicArtifactSink, atomic_write, io_error, package_path,
        portable_path_cmp, validate_manifest_identity_path,
    },
};

pub fn read_manifest_header(package_dir: impl AsRef<Path>) -> Result<PackageManifestHeader> {
    let root = PackageRoot::open(package_dir.as_ref())?;
    read_manifest_header_from_root(&root)
}

pub fn visit_manifest_entries(
    package_dir: impl AsRef<Path>,
    file_visitor: &mut dyn FnMut(FileEntry) -> Result<()>,
    segment_visitor: &mut dyn FnMut(cdf_package_contract::SegmentEntry) -> Result<()>,
) -> Result<PackageManifestHeader> {
    let root = PackageRoot::open(package_dir.as_ref())?;
    visit_manifest_entries_from_root(&root, file_visitor, segment_visitor)
}

pub(crate) fn read_manifest_header_from_root(root: &PackageRoot) -> Result<PackageManifestHeader> {
    visit_manifest_entries_from_root(root, &mut |_| Ok(()), &mut |_| Ok(()))
}

pub(crate) fn visit_manifest_entries_from_root(
    root: &PackageRoot,
    file_visitor: &mut dyn FnMut(FileEntry) -> Result<()>,
    segment_visitor: &mut dyn FnMut(cdf_package_contract::SegmentEntry) -> Result<()>,
) -> Result<PackageManifestHeader> {
    let file = root.open_regular_file(MANIFEST_FILE)?.into_std();
    visit_package_manifest(BufReader::new(file), file_visitor, segment_visitor)
}

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
) -> Result<PackageManifestHeader> {
    let package_dir = package_dir.as_ref();
    let root = PackageRoot::open(package_dir)?;
    let mut manifest = read_manifest_header_from_root(&root)?;
    let input = root.open_regular_file(MANIFEST_FILE)?.into_std();
    let mut sink = AtomicArtifactSink::create(
        &package_path(package_dir, MANIFEST_FILE),
        ArtifactDurability::PhaseMetadata,
    )?;
    rewrite_manifest_lifecycle(input, sink.writer_mut()?, status.clone())?;
    sink.finish()?;
    manifest.lifecycle.status = status;
    Ok(manifest)
}

pub fn append_receipt(package_dir: impl AsRef<Path>, receipt: Receipt) -> Result<Vec<Receipt>> {
    let package_dir = package_dir.as_ref();
    let manifest = read_manifest_header(package_dir)?;
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
    let manifest = read_manifest_header_from_root(&root)?;
    verify_package_from_root(&root, &manifest)
}

pub(crate) fn verify_package_from_root(
    root: &PackageRoot,
    manifest: &PackageManifestHeader,
) -> Result<VerificationReport> {
    let mut report = verify_package_identity_with(root, manifest)?;
    verify_contract_evolution_versions(root, manifest)?;
    report.checked_archive_count = if manifest.archives.is_some() {
        verify_parquet_archive_metadata(root, manifest)?
    } else {
        verify_parquet_archive_absence(root)?
    };
    Ok(report)
}

fn verify_contract_evolution_versions(
    root: &PackageRoot,
    _manifest: &PackageManifestHeader,
) -> Result<()> {
    const PATH: &str = "schema/contract-evolution.json";
    if !manifest_contains_file(root, PATH)? {
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
    let manifest = read_manifest_header_from_root(&root)?;
    verify_package_identity_with(&root, &manifest)
}

fn verify_package_identity_with(
    root: &PackageRoot,
    manifest: &PackageManifestHeader,
) -> Result<VerificationReport> {
    let actual_hash =
        stored_manifest_identity_hash(root.open_regular_file(MANIFEST_FILE)?.into_std())?;
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

    validate_manifest_file_paths(root)?;
    let mut checked_file_count = 0_u64;
    for expected in manifest_file_stream(root)? {
        let expected = expected?;
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
        checked_file_count = checked_file_count
            .checked_add(1)
            .ok_or_else(|| CdfError::data("verified package file count overflowed u64"))?;
    }

    let actual_entry_count = identity_entry_count(root)?;
    if actual_entry_count != checked_file_count {
        if let Some(failure) = first_unexpected_identity_failure(root)? {
            return Err(verification_failure(failure));
        }
        return Err(verification_failure(format!(
            "identity entry count mismatch: manifest has {checked_file_count}, package has {actual_entry_count}"
        )));
    }

    verify_segment_authority(root)?;

    Ok(VerificationReport {
        package_hash: manifest.package_hash.clone(),
        checked_file_count,
        checked_archive_count: 0,
    })
}

fn validate_manifest_file_paths(root: &PackageRoot) -> Result<()> {
    let mut previous_path = None;
    for entry in manifest_file_stream(root)? {
        let entry = entry?;
        validate_manifest_identity_path(previous_path.as_deref(), &entry.path)?;
        previous_path = Some(entry.path);
    }
    Ok(())
}

fn first_unexpected_identity_failure(root: &PackageRoot) -> Result<Option<String>> {
    let mut first: Option<(String, PackageEntryKind)> = None;
    root.visit_identity_entries(|path, kind| {
        let is_expected = manifest_contains_file(root, &path)?;
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

fn identity_entry_count(root: &PackageRoot) -> Result<u64> {
    let mut count = 0_u64;
    root.visit_identity_entries(|_, _| {
        count = count
            .checked_add(1)
            .ok_or_else(|| CdfError::data("package identity entry count overflowed u64"))?;
        Ok(())
    })?;
    Ok(count)
}

fn manifest_contains_file(root: &PackageRoot, path: &str) -> Result<bool> {
    for entry in manifest_file_stream(root)? {
        let entry = entry?;
        match portable_path_cmp(&entry.path, path) {
            std::cmp::Ordering::Less => {}
            std::cmp::Ordering::Equal => return Ok(true),
            std::cmp::Ordering::Greater => return Ok(false),
        }
    }
    Ok(false)
}

fn manifest_file_stream(root: &PackageRoot) -> Result<ManifestFileStream<std::fs::File>> {
    Ok(ManifestFileStream::new(
        root.open_regular_file(MANIFEST_FILE)?.into_std(),
    ))
}

fn manifest_segment_stream(root: &PackageRoot) -> Result<ManifestSegmentStream<std::fs::File>> {
    Ok(ManifestSegmentStream::new(
        root.open_regular_file(MANIFEST_FILE)?.into_std(),
    ))
}

fn verify_segment_authority(root: &PackageRoot) -> Result<()> {
    let mut files = manifest_file_stream(root)?;
    let mut current_file = files.next().transpose()?;
    let mut previous_segment_path: Option<String> = None;
    let mut next_package_row_ord = 0_u64;
    for segment in manifest_segment_stream(root)? {
        let segment = segment?;
        if segment.row_count == 0 {
            return Err(CdfError::data(format!(
                "canonical segment {} must contain at least one row",
                segment.segment_id
            )));
        }
        if segment.package_row_ord_start != next_package_row_ord {
            return Err(CdfError::data(format!(
                "canonical segment {} package row ordinal starts at {} but manifest order requires {next_package_row_ord}",
                segment.segment_id, segment.package_row_ord_start
            )));
        }
        next_package_row_ord = next_package_row_ord
            .checked_add(segment.row_count)
            .ok_or_else(|| CdfError::data("package row ordinal range overflow"))?;
        if previous_segment_path
            .as_deref()
            .is_some_and(|previous| portable_path_cmp(previous, &segment.path).is_ge())
        {
            return Err(CdfError::data(
                "package manifest segment paths must be strictly portable-path-sorted",
            ));
        }
        previous_segment_path = Some(segment.path.clone());

        while current_file
            .as_ref()
            .is_some_and(|file| portable_path_cmp(&file.path, &segment.path).is_lt())
        {
            current_file = files.next().transpose()?;
        }
        let Some(file) = current_file
            .as_ref()
            .filter(|file| file.path == segment.path)
        else {
            return Err(verification_failure(format!(
                "segment {} references missing file entry {}",
                segment.segment_id.as_str(),
                segment.path
            )));
        };
        if file.byte_count != segment.byte_count || file.sha256 != segment.sha256 {
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
    }
    Ok(())
}

fn verification_failure(message: impl std::fmt::Display) -> CdfError {
    CdfError::data(format!("package verification failed: {message}"))
}

pub fn tombstone_package(package_dir: impl AsRef<Path>) -> Result<TombstoneReport> {
    let package_dir = package_dir.as_ref();
    let root = PackageRoot::open(package_dir)?;
    let manifest = read_manifest_header_from_root(&root)?;
    let mut removed_file_count = 0_u64;

    for entry in manifest_file_stream(&root)? {
        let entry = entry?;
        validate_manifest_identity_path(None, &entry.path)?;
        let path = package_path(package_dir, &entry.path);
        if path.exists() {
            fs::remove_file(&path)
                .map_err(|error| io_error(format!("remove {}", path.display()), error))?;
            removed_file_count = removed_file_count
                .checked_add(1)
                .ok_or_else(|| CdfError::data("tombstoned file count overflowed u64"))?;
        }
    }

    update_package_status(package_dir, PackageStatus::Archived)?;
    Ok(TombstoneReport {
        package_hash: manifest.package_hash,
        removed_file_count,
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
