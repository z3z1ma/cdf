#![doc = "Package builder and reader boundary for firn."]

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Component, Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use arrow_array::RecordBatch;
use arrow_ipc::{
    CompressionType,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use firn_kernel::{FirnError, PackageHash, Receipt, Result, SegmentId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub const MANIFEST_VERSION: u16 = 1;
pub const MANIFEST_FILE: &str = "manifest.json";
pub const TRACE_FILE: &str = "trace.jsonl";
pub const RECEIPTS_FILE: &str = "destination/receipts.json";
pub const REQUIRED_DIRECTORIES: &[&str] = &[
    "plan",
    "schema",
    "data",
    "quarantine",
    "stats",
    "lineage",
    "state",
    "destination",
];

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageManifest {
    pub manifest_version: u16,
    pub package_hash: String,
    pub identity: ManifestIdentity,
    pub lifecycle: LifecycleState,
    pub signature: SignatureSlot,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestIdentity {
    pub manifest_version: u16,
    pub package_id: String,
    pub layout: Vec<String>,
    pub files: Vec<FileEntry>,
    pub segments: Vec<SegmentEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileEntry {
    pub path: String,
    pub byte_count: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentEntry {
    pub segment_id: SegmentId,
    pub path: String,
    pub row_count: u64,
    pub byte_count: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleState {
    pub status: PackageStatus,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignatureSlot {
    pub signing_input: String,
    pub value: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageStatus {
    Planned,
    Extracting,
    Validated,
    Packaged,
    Loading,
    Loaded,
    Committed,
    Checkpointed,
    Archived,
}

impl PackageStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Extracting => "extracting",
            Self::Validated => "validated",
            Self::Packaged => "packaged",
            Self::Loading => "loading",
            Self::Loaded => "loaded",
            Self::Committed => "committed",
            Self::Checkpointed => "checkpointed",
            Self::Archived => "archived",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "planned" => Ok(Self::Planned),
            "extracting" => Ok(Self::Extracting),
            "validated" => Ok(Self::Validated),
            "packaged" => Ok(Self::Packaged),
            "loading" => Ok(Self::Loading),
            "loaded" => Ok(Self::Loaded),
            "committed" => Ok(Self::Committed),
            "checkpointed" => Ok(Self::Checkpointed),
            "archived" => Ok(Self::Archived),
            other => Err(FirnError::data(format!("unknown package status {other:?}"))),
        }
    }

    pub fn is_replayable(&self) -> bool {
        self.rank() >= Self::Packaged.rank() && self != &Self::Archived
    }

    fn rank(&self) -> u8 {
        match self {
            Self::Planned => 0,
            Self::Extracting => 1,
            Self::Validated => 2,
            Self::Packaged => 3,
            Self::Loading => 4,
            Self::Loaded => 5,
            Self::Committed => 6,
            Self::Checkpointed => 7,
            Self::Archived => 8,
        }
    }
}

impl TryFrom<&str> for PackageStatus {
    type Error = FirnError;

    fn try_from(value: &str) -> Result<Self> {
        Self::parse(value)
    }
}

#[derive(Debug)]
pub struct PackageBuilder {
    package_dir: PathBuf,
    package_id: String,
    segments: Vec<SegmentDraft>,
}

#[derive(Clone, Debug)]
struct SegmentDraft {
    segment_id: SegmentId,
    path: String,
    row_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerificationReport {
    pub package_hash: String,
    pub checked_files: Vec<FileEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TombstoneReport {
    pub package_hash: String,
    pub removed_files: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayView {
    pub package_hash: PackageHash,
    pub status: PackageStatus,
    pub segments: Vec<SegmentEntry>,
    pub receipts: Vec<Receipt>,
}

#[derive(Clone, Debug)]
pub struct PackageReader {
    package_dir: PathBuf,
    manifest: PackageManifest,
}

impl PackageBuilder {
    pub fn create(package_dir: impl AsRef<Path>, package_id: impl Into<String>) -> Result<Self> {
        let package_dir = package_dir.as_ref().to_path_buf();
        let package_id = package_id.into();
        if package_id.trim().is_empty() {
            return Err(FirnError::contract("package id cannot be empty"));
        }
        if package_dir.join(MANIFEST_FILE).exists() {
            return Err(FirnError::data(format!(
                "package manifest already exists at {}",
                package_dir.join(MANIFEST_FILE).display()
            )));
        }

        create_layout(&package_dir)?;
        let manifest = build_manifest(
            package_id.clone(),
            collect_identity_file_entries(&package_dir)?,
            Vec::new(),
            PackageStatus::Planned,
        )?;
        write_manifest_atomic(&package_dir, &manifest)?;

        Ok(Self {
            package_dir,
            package_id,
            segments: Vec::new(),
        })
    }

    pub fn package_dir(&self) -> &Path {
        &self.package_dir
    }

    pub fn update_status(&self, status: PackageStatus) -> Result<PackageManifest> {
        update_package_status(&self.package_dir, status)
    }

    pub fn write_json_artifact<T: Serialize>(
        &self,
        relative_path: impl AsRef<Path>,
        value: &T,
    ) -> Result<FileEntry> {
        let bytes = canonical_json_bytes(value)?;
        self.write_identity_artifact(relative_path, &bytes)
    }

    pub fn write_identity_artifact(
        &self,
        relative_path: impl AsRef<Path>,
        bytes: &[u8],
    ) -> Result<FileEntry> {
        let relative_path = normalize_artifact_path(relative_path.as_ref())?;
        let path = package_path(&self.package_dir, &relative_path);
        atomic_write(&path, bytes)?;
        file_entry_for_path(&self.package_dir, &relative_path)
    }

    pub fn write_stats_artifact(
        &self,
        file_name: impl AsRef<Path>,
        bytes: &[u8],
    ) -> Result<FileEntry> {
        self.write_identity_artifact(nested_artifact_path("stats", file_name.as_ref())?, bytes)
    }

    pub fn write_quarantine_artifact(
        &self,
        file_name: impl AsRef<Path>,
        bytes: &[u8],
    ) -> Result<FileEntry> {
        self.write_identity_artifact(
            nested_artifact_path("quarantine", file_name.as_ref())?,
            bytes,
        )
    }

    pub fn write_lineage_artifact(
        &self,
        file_name: impl AsRef<Path>,
        bytes: &[u8],
    ) -> Result<FileEntry> {
        self.write_identity_artifact(nested_artifact_path("lineage", file_name.as_ref())?, bytes)
    }

    pub fn append_trace_event<T: Serialize>(&self, event: &T) -> Result<()> {
        let mut bytes = canonical_json_bytes(event)?;
        bytes.push(b'\n');
        let path = self.package_dir.join(TRACE_FILE);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|error| io_error(format!("open {}", path.display()), error))?;
        file.write_all(&bytes)
            .map_err(|error| io_error(format!("write {}", path.display()), error))?;
        file.sync_all()
            .map_err(|error| io_error(format!("sync {}", path.display()), error))?;
        sync_directory(&self.package_dir)
    }

    pub fn write_segment(
        &mut self,
        segment_id: SegmentId,
        batches: &[RecordBatch],
    ) -> Result<SegmentEntry> {
        if batches.is_empty() {
            return Err(FirnError::data("segment must contain at least one batch"));
        }

        let schema = batches[0].schema();
        let mut row_count = 0_u64;
        for batch in batches {
            if batch.schema().as_ref() != schema.as_ref() {
                return Err(FirnError::data(
                    "all record batches in a package segment must share one schema",
                ));
            }
            row_count += batch.num_rows() as u64;
        }

        let relative_path = segment_relative_path(&segment_id)?;
        let path = package_path(&self.package_dir, &relative_path);
        write_arrow_ipc_file(&path, schema.as_ref(), batches)?;

        let file_entry = file_entry_for_path(&self.package_dir, &relative_path)?;
        let segment = SegmentEntry {
            segment_id: segment_id.clone(),
            path: relative_path.clone(),
            row_count,
            byte_count: file_entry.byte_count,
            sha256: file_entry.sha256,
        };
        self.segments.push(SegmentDraft {
            segment_id,
            path: relative_path,
            row_count,
        });
        Ok(segment)
    }

    pub fn finish(&self) -> Result<PackageManifest> {
        self.finish_with_status(PackageStatus::Packaged)
    }

    pub fn finish_with_status(&self, status: PackageStatus) -> Result<PackageManifest> {
        let files = collect_identity_file_entries(&self.package_dir)?;
        let entries_by_path: BTreeMap<&str, &FileEntry> = files
            .iter()
            .map(|entry| (entry.path.as_str(), entry))
            .collect();
        let mut segments = Vec::with_capacity(self.segments.len());

        for draft in &self.segments {
            let entry = entries_by_path.get(draft.path.as_str()).ok_or_else(|| {
                FirnError::data(format!(
                    "segment file {} missing before package finalization",
                    draft.path
                ))
            })?;
            segments.push(SegmentEntry {
                segment_id: draft.segment_id.clone(),
                path: draft.path.clone(),
                row_count: draft.row_count,
                byte_count: entry.byte_count,
                sha256: entry.sha256.clone(),
            });
        }

        let manifest = build_manifest(self.package_id.clone(), files, segments, status)?;
        write_manifest_atomic(&self.package_dir, &manifest)?;
        Ok(manifest)
    }
}

impl PackageReader {
    pub fn open(package_dir: impl AsRef<Path>) -> Result<Self> {
        let package_dir = package_dir.as_ref().to_path_buf();
        let manifest = read_manifest(&package_dir)?;
        Ok(Self {
            package_dir,
            manifest,
        })
    }

    pub fn manifest(&self) -> &PackageManifest {
        &self.manifest
    }

    pub fn verify(&self) -> Result<VerificationReport> {
        verify_package(&self.package_dir)
    }

    pub fn update_status(&mut self, status: PackageStatus) -> Result<&PackageManifest> {
        self.manifest = update_package_status(&self.package_dir, status)?;
        Ok(&self.manifest)
    }

    pub fn append_receipt(&self, receipt: Receipt) -> Result<Vec<Receipt>> {
        append_receipt(&self.package_dir, receipt)
    }

    pub fn receipts(&self) -> Result<Vec<Receipt>> {
        read_receipts(&self.package_dir)
    }

    pub fn replay_view(&self) -> Result<ReplayView> {
        if !self.manifest.lifecycle.status.is_replayable() {
            return Err(FirnError::data(format!(
                "package {} is not replayable at status {}",
                self.manifest.package_hash,
                self.manifest.lifecycle.status.as_str()
            )));
        }
        Ok(ReplayView {
            package_hash: PackageHash::new(self.manifest.package_hash.clone())?,
            status: self.manifest.lifecycle.status.clone(),
            segments: self.manifest.identity.segments.clone(),
            receipts: self.receipts()?,
        })
    }

    pub fn read_segment(&self, segment_id: &SegmentId) -> Result<Vec<RecordBatch>> {
        let segment = self
            .manifest
            .identity
            .segments
            .iter()
            .find(|segment| &segment.segment_id == segment_id)
            .ok_or_else(|| {
                FirnError::data(format!(
                    "segment {} is not in manifest",
                    segment_id.as_str()
                ))
            })?;
        read_segment_file(&self.package_dir, &segment.path)
    }

    pub fn read_all_segments(&self) -> Result<Vec<(SegmentEntry, Vec<RecordBatch>)>> {
        self.manifest
            .identity
            .segments
            .iter()
            .map(|segment| {
                Ok((
                    segment.clone(),
                    read_segment_file(&self.package_dir, &segment.path)?,
                ))
            })
            .collect()
    }

    pub fn tombstone(&mut self) -> Result<TombstoneReport> {
        let report = tombstone_package(&self.package_dir)?;
        self.manifest = read_manifest(&self.package_dir)?;
        Ok(report)
    }
}

pub fn canonical_json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let value = serde_json::to_value(value).map_err(json_error)?;
    let mut output = Vec::new();
    write_canonical_value(&value, &mut output)?;
    Ok(output)
}

pub fn manifest_identity_hash(identity: &ManifestIdentity) -> Result<String> {
    Ok(format!(
        "sha256:{}",
        sha256_hex(&canonical_json_bytes(identity)?)
    ))
}

pub fn read_manifest(package_dir: impl AsRef<Path>) -> Result<PackageManifest> {
    let path = package_dir.as_ref().join(MANIFEST_FILE);
    let bytes =
        fs::read(&path).map_err(|error| io_error(format!("read {}", path.display()), error))?;
    serde_json::from_slice(&bytes).map_err(json_error)
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
        return Err(FirnError::data(format!(
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
        return Err(FirnError::data(format!(
            "package verification failed: {}",
            failures.join("; ")
        )));
    }

    Ok(VerificationReport {
        package_hash: manifest.package_hash,
        checked_files,
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
        return Err(FirnError::data(format!(
            "segment path must live under data/: {relative_path}"
        )));
    }
    let path = package_path(package_dir.as_ref(), &relative_path);
    let file =
        File::open(&path).map_err(|error| io_error(format!("open {}", path.display()), error))?;
    let reader = FileReader::try_new(file, None).map_err(FirnError::from)?;
    reader
        .map(|batch| batch.map_err(FirnError::from))
        .collect::<Result<Vec<_>>>()
}

fn create_layout(package_dir: &Path) -> Result<()> {
    fs::create_dir_all(package_dir)
        .map_err(|error| io_error(format!("create {}", package_dir.display()), error))?;
    for directory in REQUIRED_DIRECTORIES {
        let path = package_dir.join(directory);
        fs::create_dir_all(&path)
            .map_err(|error| io_error(format!("create {}", path.display()), error))?;
    }
    let trace_path = package_dir.join(TRACE_FILE);
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&trace_path)
        .map_err(|error| io_error(format!("create {}", trace_path.display()), error))?
        .sync_all()
        .map_err(|error| io_error(format!("sync {}", trace_path.display()), error))?;
    sync_directory(package_dir)
}

fn build_manifest(
    package_id: String,
    files: Vec<FileEntry>,
    segments: Vec<SegmentEntry>,
    status: PackageStatus,
) -> Result<PackageManifest> {
    let identity = ManifestIdentity {
        manifest_version: MANIFEST_VERSION,
        package_id,
        layout: package_layout(),
        files,
        segments,
    };
    let package_hash = manifest_identity_hash(&identity)?;
    Ok(PackageManifest {
        manifest_version: MANIFEST_VERSION,
        package_hash: package_hash.clone(),
        identity,
        lifecycle: LifecycleState { status },
        signature: SignatureSlot {
            signing_input: package_hash,
            value: None,
        },
    })
}

fn package_layout() -> Vec<String> {
    let mut layout = REQUIRED_DIRECTORIES
        .iter()
        .map(|directory| format!("{directory}/"))
        .collect::<Vec<_>>();
    layout.push(TRACE_FILE.to_owned());
    layout
}

fn write_manifest_atomic(package_dir: &Path, manifest: &PackageManifest) -> Result<()> {
    let path = package_dir.join(MANIFEST_FILE);
    let bytes = canonical_json_bytes(manifest)?;
    atomic_write(&path, &bytes)
}

fn collect_identity_file_entries(package_dir: &Path) -> Result<Vec<FileEntry>> {
    let mut relative_paths = Vec::new();
    for directory in REQUIRED_DIRECTORIES {
        let directory_path = package_dir.join(directory);
        if directory_path.exists() {
            collect_files(package_dir, &directory_path, &mut relative_paths)?;
        }
    }
    let trace_path = package_dir.join(TRACE_FILE);
    if trace_path.exists() {
        relative_paths.push(TRACE_FILE.to_owned());
    }

    relative_paths.retain(|path| is_identity_file(path));
    relative_paths.sort();
    relative_paths
        .iter()
        .map(|path| file_entry_for_path(package_dir, path))
        .collect()
}

fn collect_files(package_dir: &Path, directory: &Path, files: &mut Vec<String>) -> Result<()> {
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
            collect_files(package_dir, &path, files)?;
        } else if file_type.is_file() {
            files.push(relative_path_string(package_dir, &path)?);
        }
    }
    Ok(())
}

fn is_identity_file(relative_path: &str) -> bool {
    relative_path != MANIFEST_FILE && relative_path != RECEIPTS_FILE
}

fn file_entry_for_path(package_dir: &Path, relative_path: &str) -> Result<FileEntry> {
    let path = package_path(package_dir, relative_path);
    let mut file =
        File::open(&path).map_err(|error| io_error(format!("open {}", path.display()), error))?;
    let mut hasher = Sha256::new();
    let byte_count = std::io::copy(&mut file, &mut hasher)
        .map_err(|error| io_error(format!("hash {}", path.display()), error))?;
    Ok(FileEntry {
        path: relative_path.to_owned(),
        byte_count,
        sha256: hex::encode(hasher.finalize()),
    })
}

fn write_arrow_ipc_file(
    path: &Path,
    schema: &arrow_schema::Schema,
    batches: &[RecordBatch],
) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        FirnError::internal(format!("path {} has no parent directory", path.display()))
    })?;
    fs::create_dir_all(parent)
        .map_err(|error| io_error(format!("create {}", parent.display()), error))?;
    let (tmp_path, mut file) = create_temp_sibling(path)?;
    let write_result = (|| {
        let options = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::LZ4_FRAME))
            .map_err(FirnError::from)?;
        {
            let mut writer = FileWriter::try_new_with_options(&mut file, schema, options)
                .map_err(FirnError::from)?;
            for batch in batches {
                writer.write(batch).map_err(FirnError::from)?;
            }
            writer.finish().map_err(FirnError::from)?;
        }
        file.sync_all()
            .map_err(|error| io_error(format!("sync {}", tmp_path.display()), error))
    })();

    if let Err(error) = write_result {
        let _ = fs::remove_file(&tmp_path);
        return Err(error);
    }

    fs::rename(&tmp_path, path).map_err(|error| {
        let _ = fs::remove_file(&tmp_path);
        io_error(
            format!("rename {} to {}", tmp_path.display(), path.display()),
            error,
        )
    })?;
    sync_directory(parent)
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        FirnError::internal(format!("path {} has no parent directory", path.display()))
    })?;
    fs::create_dir_all(parent)
        .map_err(|error| io_error(format!("create {}", parent.display()), error))?;
    let (tmp_path, mut file) = create_temp_sibling(path)?;
    let write_result = (|| {
        file.write_all(bytes)
            .map_err(|error| io_error(format!("write {}", tmp_path.display()), error))?;
        file.sync_all()
            .map_err(|error| io_error(format!("sync {}", tmp_path.display()), error))
    })();

    if let Err(error) = write_result {
        let _ = fs::remove_file(&tmp_path);
        return Err(error);
    }

    fs::rename(&tmp_path, path).map_err(|error| {
        let _ = fs::remove_file(&tmp_path);
        io_error(
            format!("rename {} to {}", tmp_path.display(), path.display()),
            error,
        )
    })?;
    sync_directory(parent)
}

fn create_temp_sibling(path: &Path) -> Result<(PathBuf, File)> {
    let parent = path.parent().ok_or_else(|| {
        FirnError::internal(format!("path {} has no parent directory", path.display()))
    })?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| FirnError::internal(format!("path {} has no file name", path.display())))?;

    for _ in 0..100 {
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp_path = parent.join(format!(
            ".{file_name}.tmp.{}.{}",
            std::process::id(),
            counter
        ));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_path)
        {
            Ok(file) => return Ok((tmp_path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(io_error(format!("create {}", tmp_path.display()), error));
            }
        }
    }

    Err(FirnError::internal(format!(
        "could not create temporary sibling for {}",
        path.display()
    )))
}

fn sync_directory(directory: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        File::open(directory)
            .map_err(|error| io_error(format!("open directory {}", directory.display()), error))?
            .sync_all()
            .map_err(|error| io_error(format!("sync directory {}", directory.display()), error))?;
    }
    #[cfg(not(unix))]
    {
        let _ = directory;
    }
    Ok(())
}

fn normalize_artifact_path(relative_path: &Path) -> Result<String> {
    let relative_path = normalize_relative_path(relative_path)?;
    if relative_path == MANIFEST_FILE {
        return Err(FirnError::data(
            "manifest.json is managed by the package writer",
        ));
    }
    if relative_path == RECEIPTS_FILE {
        return Err(FirnError::data(
            "destination/receipts.json is managed by receipt append hooks",
        ));
    }
    if relative_path == TRACE_FILE
        || REQUIRED_DIRECTORIES
            .iter()
            .any(|directory| relative_path.starts_with(&format!("{directory}/")))
    {
        return Ok(relative_path);
    }
    Err(FirnError::data(format!(
        "package artifact path must be under required layout directories: {relative_path}"
    )))
}

fn nested_artifact_path(directory: &str, file_name: &Path) -> Result<String> {
    let file_name = normalize_relative_path(file_name)?;
    if file_name.contains('/') {
        return Err(FirnError::data(format!(
            "artifact file name must not contain directories: {file_name}"
        )));
    }
    Ok(format!("{directory}/{file_name}"))
}

fn normalize_relative_path(path: &Path) -> Result<String> {
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => {
                let part = part.to_str().ok_or_else(|| {
                    FirnError::data(format!("path is not valid UTF-8: {}", path.display()))
                })?;
                if part.is_empty() {
                    return Err(FirnError::data("path component cannot be empty"));
                }
                parts.push(part.to_owned());
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(FirnError::data(format!(
                    "package artifact path must be relative and stay inside the package: {}",
                    path.display()
                )));
            }
        }
    }
    if parts.is_empty() {
        return Err(FirnError::data("package artifact path cannot be empty"));
    }
    Ok(parts.join("/"))
}

fn relative_path_string(base: &Path, path: &Path) -> Result<String> {
    let relative = path.strip_prefix(base).map_err(|error| {
        FirnError::internal(format!(
            "path {} is not under {}: {error}",
            path.display(),
            base.display()
        ))
    })?;
    normalize_relative_path(relative)
}

fn package_path(package_dir: &Path, relative_path: impl AsRef<str>) -> PathBuf {
    relative_path
        .as_ref()
        .split('/')
        .fold(package_dir.to_path_buf(), |path, part| path.join(part))
}

fn segment_relative_path(segment_id: &SegmentId) -> Result<String> {
    let id = segment_id.as_str();
    if id.contains('/')
        || id.contains('\\')
        || id.contains("..")
        || id.trim().is_empty()
        || id == "."
    {
        return Err(FirnError::data(format!(
            "segment id cannot be used as a package file name: {id:?}"
        )));
    }
    Ok(format!("data/{id}.arrow"))
}

fn write_canonical_value(value: &Value, output: &mut Vec<u8>) -> Result<()> {
    match value {
        Value::Null => output.extend_from_slice(b"null"),
        Value::Bool(value) => output.extend_from_slice(if *value { b"true" } else { b"false" }),
        Value::Number(number) => output.extend_from_slice(number.to_string().as_bytes()),
        Value::String(value) => write_canonical_string(value, output)?,
        Value::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_canonical_value(value, output)?;
            }
            output.push(b']');
        }
        Value::Object(map) => {
            output.push(b'{');
            let mut entries = map.iter().collect::<Vec<_>>();
            entries.sort_by_key(|(key, _)| *key);
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_canonical_string(key, output)?;
                output.push(b':');
                write_canonical_value(value, output)?;
            }
            output.push(b'}');
        }
    }
    Ok(())
}

fn write_canonical_string(value: &str, output: &mut Vec<u8>) -> Result<()> {
    let escaped = serde_json::to_string(value).map_err(json_error)?;
    output.extend_from_slice(escaped.as_bytes());
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn io_error(context: impl Into<String>, error: std::io::Error) -> FirnError {
    FirnError::internal(format!("{}: {error}", context.into()))
}

fn json_error(error: serde_json::Error) -> FirnError {
    FirnError::data(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::BTreeMap, sync::Arc};

    use arrow_array::{ArrayRef, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use firn_kernel::{
        CommitCounts, DestinationId, IdempotencyToken, ReceiptId, SchemaHash, SegmentAck,
        TargetName, VerifyClause, WriteDisposition,
    };

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let id: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let name: ArrayRef = Arc::new(StringArray::from(vec![Some("ada"), Some("grace"), None]));
        RecordBatch::try_new(schema, vec![id, name]).unwrap()
    }

    fn build_fixture(package_dir: &Path) -> PackageManifest {
        let mut builder = PackageBuilder::create(package_dir, "pkg-test-0001").unwrap();
        builder.update_status(PackageStatus::Extracting).unwrap();
        builder
            .write_json_artifact(
                "plan/resource_plan.json",
                &BTreeMap::from([("resource", "orders"), ("partition", "p0")]),
            )
            .unwrap();
        builder
            .write_identity_artifact(
                "plan/execution_plan.txt",
                b"PackageSinkExec: deterministic fixture\n",
            )
            .unwrap();
        builder
            .write_json_artifact(
                "plan/validation_program.json",
                &BTreeMap::from([("program", "accept-all")]),
            )
            .unwrap();
        builder
            .write_json_artifact(
                "schema/observed.arrow.json",
                &BTreeMap::from([("schema_hash", "schema-fixture")]),
            )
            .unwrap();
        builder
            .write_json_artifact(
                "schema/output.arrow.json",
                &BTreeMap::from([("schema_hash", "schema-fixture")]),
            )
            .unwrap();
        builder
            .write_json_artifact("schema/diff.json", &BTreeMap::<String, String>::new())
            .unwrap();
        builder
            .write_stats_artifact("profile.parquet", b"stats-fixture")
            .unwrap();
        builder
            .write_stats_artifact("quality.parquet", b"quality-fixture")
            .unwrap();
        builder
            .write_quarantine_artifact("part-000001.parquet", b"quarantine-fixture")
            .unwrap();
        builder
            .write_lineage_artifact("batches.parquet", b"lineage-fixture")
            .unwrap();
        builder
            .write_json_artifact(
                "state/input_checkpoint.json",
                &BTreeMap::from([("cursor", "before")]),
            )
            .unwrap();
        builder
            .write_json_artifact(
                "state/proposed_delta.json",
                &BTreeMap::from([("cursor", "after")]),
            )
            .unwrap();
        builder
            .write_json_artifact(
                "destination/commit_plan.json",
                &BTreeMap::from([("target", "orders"), ("disposition", "append")]),
            )
            .unwrap();
        builder
            .append_trace_event(&BTreeMap::from([("event", "fixture-start")]))
            .unwrap();
        builder
            .write_segment(SegmentId::new("seg-000001").unwrap(), &[sample_batch()])
            .unwrap();
        builder.finish().unwrap()
    }

    fn sample_receipt(package_hash: &str) -> Receipt {
        Receipt {
            receipt_id: ReceiptId::new("receipt-1").unwrap(),
            destination: DestinationId::new("local-test").unwrap(),
            target: TargetName::new("orders").unwrap(),
            package_hash: PackageHash::new(package_hash.to_owned()).unwrap(),
            segment_acks: vec![SegmentAck {
                segment_id: SegmentId::new("seg-000001").unwrap(),
                row_count: 3,
                byte_count: 0,
            }],
            disposition: WriteDisposition::Append,
            idempotency_token: IdempotencyToken::new(package_hash.to_owned()).unwrap(),
            transaction: None,
            counts: CommitCounts {
                rows_written: 3,
                rows_inserted: Some(3),
                rows_updated: Some(0),
                rows_deleted: Some(0),
            },
            schema_hash: SchemaHash::new("schema-fixture").unwrap(),
            migrations: Vec::new(),
            committed_at_ms: 1_700_000_000_000,
            verify: VerifyClause {
                kind: "test".to_owned(),
                statement: "fixture receipt".to_owned(),
                parameters: BTreeMap::new(),
            },
        }
    }

    #[test]
    fn package_layout_manifest_and_verification_cover_identity_files() {
        let temp = tempfile::tempdir().unwrap();
        let manifest = build_fixture(temp.path());

        assert_eq!(manifest.lifecycle.status, PackageStatus::Packaged);
        assert_eq!(manifest.signature.value, None);
        assert_eq!(manifest.signature.signing_input, manifest.package_hash);
        for directory in REQUIRED_DIRECTORIES {
            assert!(temp.path().join(directory).is_dir(), "{directory}");
        }
        assert!(temp.path().join(MANIFEST_FILE).is_file());
        assert!(temp.path().join(TRACE_FILE).is_file());

        let paths = manifest
            .identity
            .files
            .iter()
            .map(|entry| entry.path.as_str())
            .collect::<BTreeSet<_>>();
        assert!(paths.contains("data/seg-000001.arrow"));
        assert!(paths.contains("trace.jsonl"));
        assert!(
            manifest
                .identity
                .files
                .iter()
                .all(|entry| entry.byte_count > 0 || entry.path == TRACE_FILE)
        );
        assert!(
            manifest
                .identity
                .files
                .iter()
                .all(|entry| entry.sha256.len() == 64)
        );

        let report = verify_package(temp.path()).unwrap();
        assert_eq!(report.package_hash, manifest.package_hash);
        assert_eq!(report.checked_files.len(), manifest.identity.files.len());
    }

    #[test]
    fn fixed_fixture_hash_is_deterministic_across_repeated_runs() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();

        let first_manifest = build_fixture(first.path());
        let second_manifest = build_fixture(second.path());

        assert_eq!(first_manifest.package_hash, second_manifest.package_hash);
        assert_eq!(
            first_manifest.package_hash,
            "sha256:87789e563e66acd0cec0f0edcb4b5f54052e7695440cdc66d5512b5007b24adf"
        );
    }

    #[test]
    fn arrow_ipc_segments_round_trip_for_replay() {
        let temp = tempfile::tempdir().unwrap();
        let manifest = build_fixture(temp.path());
        let reader = PackageReader::open(temp.path()).unwrap();

        let segment_id = &manifest.identity.segments[0].segment_id;
        let batches = reader.read_segment(segment_id).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);

        let replay = reader.replay_view().unwrap();
        assert_eq!(replay.package_hash.as_str(), manifest.package_hash);
        assert_eq!(replay.segments.len(), 1);
    }

    #[test]
    fn status_updates_are_atomic_and_preserve_identity_hash() {
        let temp = tempfile::tempdir().unwrap();
        let manifest = build_fixture(temp.path());
        let original_hash = manifest.package_hash.clone();

        let updated = update_package_status(temp.path(), PackageStatus::Loading).unwrap();
        assert_eq!(updated.lifecycle.status, PackageStatus::Loading);
        assert_eq!(updated.package_hash, original_hash);
        verify_package(temp.path()).unwrap();

        let updated = update_package_status(temp.path(), PackageStatus::Committed).unwrap();
        assert_eq!(updated.lifecycle.status, PackageStatus::Committed);
        assert_eq!(updated.package_hash, original_hash);
        verify_package(temp.path()).unwrap();
    }

    #[test]
    fn verification_detects_tampered_identity_file() {
        let temp = tempfile::tempdir().unwrap();
        build_fixture(temp.path());

        let segment_path = temp.path().join("data").join("seg-000001.arrow");
        let mut file = OpenOptions::new().append(true).open(&segment_path).unwrap();
        file.write_all(b"tamper").unwrap();
        file.sync_all().unwrap();

        let error = verify_package(temp.path()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("tampered identity file data/seg-000001.arrow"),
            "{error}"
        );
    }

    #[test]
    fn receipt_append_is_stored_outside_identity_and_exposed_to_replay() {
        let temp = tempfile::tempdir().unwrap();
        let manifest = build_fixture(temp.path());
        let reader = PackageReader::open(temp.path()).unwrap();
        let before_receipt_hash = manifest.package_hash.clone();

        let receipts = reader
            .append_receipt(sample_receipt(&manifest.package_hash))
            .unwrap();
        assert_eq!(receipts.len(), 1);
        assert!(
            temp.path()
                .join("destination")
                .join("receipts.json")
                .is_file()
        );

        let reread = PackageReader::open(temp.path()).unwrap();
        assert_eq!(reread.receipts().unwrap().len(), 1);
        assert_eq!(reread.replay_view().unwrap().receipts.len(), 1);
        assert_eq!(reread.manifest().package_hash, before_receipt_hash);
        verify_package(temp.path()).unwrap();
    }

    #[test]
    fn tombstone_removes_identity_files_but_preserves_manifest_hashes() {
        let temp = tempfile::tempdir().unwrap();
        let manifest = build_fixture(temp.path());
        let manifest_path = temp.path().join(MANIFEST_FILE);
        let mut reader = PackageReader::open(temp.path()).unwrap();

        let report = reader.tombstone().unwrap();
        assert_eq!(report.package_hash, manifest.package_hash);
        assert!(manifest_path.is_file());
        assert!(
            report
                .removed_files
                .contains(&"data/seg-000001.arrow".to_owned())
        );

        let tombstoned_manifest = read_manifest(temp.path()).unwrap();
        assert_eq!(tombstoned_manifest.package_hash, manifest.package_hash);
        assert_eq!(
            tombstoned_manifest.lifecycle.status,
            PackageStatus::Archived
        );
        assert!(reader.replay_view().is_err());
        assert!(verify_package(temp.path()).is_err());
    }
}
