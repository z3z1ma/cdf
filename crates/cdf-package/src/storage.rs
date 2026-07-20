use std::{
    cmp::Ordering as CmpOrdering,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Component, Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use arrow_array::RecordBatch;
use arrow_ipc::{
    CompressionType,
    writer::{FileWriter, IpcWriteOptions},
};
use cdf_kernel::{CdfError, Result, SegmentId};
use cdf_package_contract::{
    FileEntry, LifecycleState, MANIFEST_FILE, MANIFEST_VERSION, ManifestIdentity, PackageManifest,
    PackageStatus, RECEIPTS_FILE, REQUIRED_DIRECTORIES, SegmentEntry, SignatureSlot, TRACE_FILE,
};
use sha2::{Digest, Sha256};

use crate::json::{manifest_identity_hash, write_package_manifest_canonical};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ArtifactDurability {
    SegmentPublish,
    PhaseMetadata,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WrittenArtifact {
    pub path: PathBuf,
    pub byte_count: u64,
    pub sha256: String,
    pub durability: ArtifactDurability,
}

pub(crate) struct IpcWriteReceipt {
    pub artifact: WrittenArtifact,
    pub encode_hash_duration_ns: u64,
    pub publish_duration_ns: u64,
}

#[derive(Debug)]
pub(crate) struct HashingWriter<W> {
    inner: W,
    hasher: Sha256,
    byte_count: u64,
}

impl<W> HashingWriter<W> {
    pub(crate) fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
            byte_count: 0,
        }
    }
}

impl HashingWriter<File> {
    pub(crate) fn sync_all(&mut self) -> Result<()> {
        self.flush()
            .map_err(|error| io_error("flush hashing writer", error))?;
        self.inner
            .sync_all()
            .map_err(|error| io_error("sync hashing writer", error))
    }

    pub(crate) fn file_entry(&self, path: impl Into<String>) -> FileEntry {
        FileEntry {
            path: path.into(),
            byte_count: self.byte_count,
            sha256: hex::encode(self.hasher.clone().finalize()),
        }
    }
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(bytes)?;
        self.hasher.update(&bytes[..written]);
        self.byte_count = self
            .byte_count
            .checked_add(written as u64)
            .ok_or_else(|| std::io::Error::other("artifact byte count overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

pub(crate) struct AtomicArtifactSink {
    final_path: PathBuf,
    temp_path: PathBuf,
    parent: PathBuf,
    writer: Option<HashingWriter<File>>,
    durability: ArtifactDurability,
    #[cfg(test)]
    fail_at: Option<PublishBoundary>,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PublishBoundary {
    EncoderFinish,
    FileSync,
    Rename,
    DirectorySync,
}

impl AtomicArtifactSink {
    pub(crate) fn create(path: &Path, durability: ArtifactDurability) -> Result<Self> {
        let parent = path.parent().ok_or_else(|| {
            CdfError::internal(format!("path {} has no parent directory", path.display()))
        })?;
        fs::create_dir_all(parent)
            .map_err(|error| io_error(format!("create {}", parent.display()), error))?;
        let (temp_path, file) = create_temp_sibling(path)?;
        Ok(Self {
            final_path: path.to_path_buf(),
            temp_path,
            parent: parent.to_path_buf(),
            writer: Some(HashingWriter::new(file)),
            durability,
            #[cfg(test)]
            fail_at: None,
        })
    }

    #[cfg(test)]
    fn inject_failure(&mut self, boundary: PublishBoundary) {
        self.fail_at = Some(boundary);
    }

    #[cfg(test)]
    fn check_failure(&self, boundary: PublishBoundary) -> Result<()> {
        if self.fail_at == Some(boundary) {
            return Err(CdfError::internal(format!(
                "injected artifact publish failure at {boundary:?}"
            )));
        }
        Ok(())
    }

    pub(crate) fn writer_mut(&mut self) -> Result<&mut HashingWriter<File>> {
        self.writer
            .as_mut()
            .ok_or_else(|| CdfError::internal("artifact sink is already finished"))
    }

    pub(crate) fn finish(mut self) -> Result<WrittenArtifact> {
        let mut writer = self
            .writer
            .take()
            .ok_or_else(|| CdfError::internal("artifact sink is already finished"))?;
        let mut renamed = false;
        let publish = (|| {
            writer
                .flush()
                .map_err(|error| io_error(format!("flush {}", self.temp_path.display()), error))?;
            #[cfg(test)]
            self.check_failure(PublishBoundary::FileSync)?;
            writer
                .inner
                .sync_all()
                .map_err(|error| io_error(format!("sync {}", self.temp_path.display()), error))?;
            #[cfg(test)]
            self.check_failure(PublishBoundary::Rename)?;
            fs::rename(&self.temp_path, &self.final_path).map_err(|error| {
                io_error(
                    format!(
                        "rename {} to {}",
                        self.temp_path.display(),
                        self.final_path.display()
                    ),
                    error,
                )
            })?;
            renamed = true;
            #[cfg(test)]
            self.check_failure(PublishBoundary::DirectorySync)?;
            sync_directory(&self.parent)
        })();
        if let Err(error) = publish {
            let cleanup_path = if renamed {
                &self.final_path
            } else {
                &self.temp_path
            };
            if let Err(cleanup_error) = remove_artifact_and_sync(cleanup_path) {
                return Err(CdfError::internal(format!(
                    "{error}; failed to clean unpublished artifact {}: {cleanup_error}",
                    cleanup_path.display()
                )));
            }
            return Err(error);
        }
        Ok(WrittenArtifact {
            path: self.final_path.clone(),
            byte_count: writer.byte_count,
            sha256: hex::encode(writer.hasher.finalize()),
            durability: self.durability,
        })
    }
}

pub(crate) fn remove_artifact_and_sync(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(io_error(format!("remove {}", path.display()), error));
        }
    }
    let parent = path.parent().ok_or_else(|| {
        CdfError::internal(format!("artifact path {} has no parent", path.display()))
    })?;
    sync_directory(parent)
}

impl Drop for AtomicArtifactSink {
    fn drop(&mut self) {
        if self.writer.is_some() {
            let _ = fs::remove_file(&self.temp_path);
        }
    }
}
pub(crate) fn create_layout(package_dir: &Path) -> Result<()> {
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

pub(crate) fn build_manifest(
    package_id: String,
    files: Vec<FileEntry>,
    segments: Vec<SegmentEntry>,
    status: PackageStatus,
) -> Result<PackageManifest> {
    validate_manifest_identity_paths(&files)?;
    cdf_package_contract::validate_segment_ordinal_manifest(&segments)?;
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
        archives: None,
    })
}

pub(crate) fn package_layout() -> Vec<String> {
    let mut layout = REQUIRED_DIRECTORIES
        .iter()
        .map(|directory| format!("{directory}/"))
        .collect::<Vec<_>>();
    layout.push(TRACE_FILE.to_owned());
    layout
}

pub(crate) fn write_manifest_atomic(package_dir: &Path, manifest: &PackageManifest) -> Result<()> {
    let path = package_dir.join(MANIFEST_FILE);
    let mut sink = AtomicArtifactSink::create(&path, ArtifactDurability::PhaseMetadata)?;
    write_package_manifest_canonical(manifest, sink.writer_mut()?)?;
    sink.finish().map(|_| ())
}

pub(crate) fn collect_identity_file_entries(package_dir: &Path) -> Result<Vec<FileEntry>> {
    collect_identity_file_paths(package_dir)?
        .iter()
        .map(|path| file_entry_for_path(package_dir, path))
        .collect()
}

pub(crate) fn collect_identity_file_paths(package_dir: &Path) -> Result<Vec<String>> {
    let mut relative_paths = Vec::new();
    visit_identity_file_paths(package_dir, |path| {
        relative_paths.push(path);
        Ok(())
    })?;
    relative_paths.sort_by(|left, right| portable_path_cmp(left, right));
    Ok(relative_paths)
}

pub(crate) fn visit_identity_file_paths(
    package_dir: &Path,
    mut visit: impl FnMut(String) -> Result<()>,
) -> Result<()> {
    for directory in REQUIRED_DIRECTORIES {
        let directory_path = package_dir.join(directory);
        if directory_path.exists() {
            visit_files(package_dir, &directory_path, &mut visit)?;
        }
    }
    let trace_path = package_dir.join(TRACE_FILE);
    if trace_path.exists() {
        visit(TRACE_FILE.to_owned())?;
    }
    Ok(())
}

fn visit_files(
    package_dir: &Path,
    directory: &Path,
    visit: &mut impl FnMut(String) -> Result<()>,
) -> Result<()> {
    for entry in fs::read_dir(directory)
        .map_err(|error| io_error(format!("read directory {}", directory.display()), error))?
    {
        let entry = entry
            .map_err(|error| io_error(format!("read directory {}", directory.display()), error))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| io_error(format!("stat {}", path.display()), error))?;
        if file_type.is_dir() {
            visit_files(package_dir, &path, visit)?;
        } else if file_type.is_file() {
            let relative = relative_path_string(package_dir, &path)?;
            if is_identity_file(&relative) {
                visit(relative)?;
            }
        }
    }
    Ok(())
}

fn is_identity_file(relative_path: &str) -> bool {
    relative_path != MANIFEST_FILE && relative_path != RECEIPTS_FILE
}

pub(crate) fn file_entry_for_path(package_dir: &Path, relative_path: &str) -> Result<FileEntry> {
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

pub(crate) fn write_arrow_ipc_file(
    path: &Path,
    schema: &arrow_schema::Schema,
    batches: &[RecordBatch],
) -> Result<IpcWriteReceipt> {
    let mut sink = AtomicArtifactSink::create(path, ArtifactDurability::SegmentPublish)?;
    let encode_started = std::time::Instant::now();
    encode_canonical_segment_ipc(sink.writer_mut()?, schema, batches)?;
    let encode_hash_duration_ns = duration_ns(encode_started, "IPC encode/hash")?;
    let publish_started = std::time::Instant::now();
    let artifact = sink.finish()?;
    Ok(IpcWriteReceipt {
        artifact,
        encode_hash_duration_ns,
        publish_duration_ns: duration_ns(publish_started, "IPC publish")?,
    })
}

pub(crate) fn write_canonical_segment_ipc_bytes(
    path: &Path,
    bytes: &[u8],
) -> Result<IpcWriteReceipt> {
    let mut sink = AtomicArtifactSink::create(path, ArtifactDurability::SegmentPublish)?;
    let persist_started = std::time::Instant::now();
    sink.writer_mut()?
        .write_all(bytes)
        .map_err(|error| io_error(format!("write {}", path.display()), error))?;
    let persist_hash_duration_ns = duration_ns(persist_started, "IPC import/hash")?;
    let publish_started = std::time::Instant::now();
    let artifact = sink.finish()?;
    Ok(IpcWriteReceipt {
        artifact,
        encode_hash_duration_ns: 0,
        publish_duration_ns: persist_hash_duration_ns
            .checked_add(duration_ns(publish_started, "IPC import publish")?)
            .ok_or_else(|| CdfError::internal("IPC import duration overflow"))?,
    })
}

/// Encodes the canonical LZ4 Arrow IPC file representation used by package segments.
///
/// Isolated workers use the same byte authority for durable prepared/finalized handoffs; keeping
/// the writer here prevents a second identity-bearing IPC implementation from drifting.
pub fn encode_canonical_segment_ipc(
    sink: &mut dyn Write,
    schema: &arrow_schema::Schema,
    batches: &[RecordBatch],
) -> Result<()> {
    let options = IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::LZ4_FRAME))
        .map_err(CdfError::from)?;
    let mut writer =
        FileWriter::try_new_with_options(sink, schema, options).map_err(CdfError::from)?;
    for batch in batches {
        writer.write(batch).map_err(CdfError::from)?;
    }
    writer.finish().map_err(CdfError::from)
}

pub(crate) fn atomic_write(path: &Path, bytes: &[u8]) -> Result<WrittenArtifact> {
    let mut sink = AtomicArtifactSink::create(path, ArtifactDurability::PhaseMetadata)?;
    sink.writer_mut()?
        .write_all(bytes)
        .map_err(|error| io_error(format!("write {}", path.display()), error))?;
    sink.finish()
}

fn duration_ns(started: std::time::Instant, label: &str) -> Result<u64> {
    u64::try_from(started.elapsed().as_nanos())
        .map_err(|_| CdfError::internal(format!("{label} duration exceeds u64")))
}

fn create_temp_sibling(path: &Path) -> Result<(PathBuf, File)> {
    let parent = path.parent().ok_or_else(|| {
        CdfError::internal(format!("path {} has no parent directory", path.display()))
    })?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| CdfError::internal(format!("path {} has no file name", path.display())))?;

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

    Err(CdfError::internal(format!(
        "could not create temporary sibling for {}",
        path.display()
    )))
}

pub(crate) fn sync_directory(directory: &Path) -> Result<()> {
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

pub(crate) fn normalize_artifact_path(relative_path: &Path) -> Result<String> {
    let relative_path = normalize_relative_path(relative_path)?;
    if relative_path == MANIFEST_FILE {
        return Err(CdfError::data(
            "manifest.json is managed by the package writer",
        ));
    }
    if relative_path == RECEIPTS_FILE {
        return Err(CdfError::data(
            "destination/receipts.json is managed by receipt append hooks",
        ));
    }
    if relative_path == TRACE_FILE
        || relative_path
            .split_once('/')
            .is_some_and(|(directory, _)| REQUIRED_DIRECTORIES.contains(&directory))
    {
        return Ok(relative_path);
    }
    Err(CdfError::data(format!(
        "package artifact path must be under required layout directories: {relative_path}"
    )))
}

pub(crate) fn nested_artifact_path(directory: &str, file_name: &Path) -> Result<String> {
    let file_name = normalize_relative_path(file_name)?;
    if file_name.contains('/') {
        return Err(CdfError::data(format!(
            "artifact file name must not contain directories: {file_name}"
        )));
    }
    Ok(format!("{directory}/{file_name}"))
}

pub(crate) fn normalize_relative_path(path: &Path) -> Result<String> {
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => {
                let part = part.to_str().ok_or_else(|| {
                    CdfError::data(format!("path is not valid UTF-8: {}", path.display()))
                })?;
                if part.is_empty() {
                    return Err(CdfError::data("path component cannot be empty"));
                }
                validate_portable_path_component(part)?;
                parts.push(part.to_owned());
            }
            Component::CurDir
            | Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => {
                return Err(CdfError::data(format!(
                    "package artifact path must be relative and stay inside the package: {}",
                    path.display()
                )));
            }
        }
    }
    if parts.is_empty() {
        return Err(CdfError::data("package artifact path cannot be empty"));
    }
    Ok(parts.join("/"))
}

pub(crate) fn validate_manifest_identity_paths(files: &[FileEntry]) -> Result<()> {
    let mut previous: Option<&str> = None;
    for entry in files {
        validate_manifest_identity_path(previous, &entry.path)?;
        previous = Some(&entry.path);
    }
    Ok(())
}

pub(crate) fn validate_manifest_identity_path(previous: Option<&str>, path: &str) -> Result<()> {
    validate_canonical_artifact_path(path)?;
    if let Some(prior) = previous {
        if portable_casefold_cmp(prior, path) == CmpOrdering::Equal {
            return Err(CdfError::data(format!(
                "package manifest identity paths collide after portable case folding: {path}"
            )));
        }
        if portable_path_cmp(prior, path) != CmpOrdering::Less {
            return Err(CdfError::data(
                "package manifest identity files must be strictly portable-path-sorted",
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_canonical_relative_path(path: &str) -> Result<()> {
    if path.is_empty() {
        return Err(CdfError::data("package artifact path cannot be empty"));
    }
    for component in path.split('/') {
        if component.is_empty() || matches!(component, "." | "..") {
            return Err(CdfError::data(format!(
                "package artifact path must use canonical portable spelling: {path}"
            )));
        }
        validate_portable_path_component(component)?;
    }
    Ok(())
}

fn validate_canonical_artifact_path(path: &str) -> Result<()> {
    validate_canonical_relative_path(path)?;
    if path == TRACE_FILE
        || path
            .split_once('/')
            .is_some_and(|(directory, _)| REQUIRED_DIRECTORIES.contains(&directory))
    {
        Ok(())
    } else {
        Err(CdfError::data(format!(
            "package artifact path must be under required layout directories: {path}"
        )))
    }
}

pub(crate) fn validate_portable_path_component(component: &str) -> Result<()> {
    if component.contains(['\\', ':'])
        || component.chars().any(char::is_control)
        || component.ends_with(['.', ' '])
    {
        return Err(CdfError::data(format!(
            "package path component is not portable: {component:?}"
        )));
    }
    let basename = component.split('.').next().unwrap_or(component);
    let reserved = ["CON", "PRN", "AUX", "NUL"]
        .iter()
        .any(|name| basename.eq_ignore_ascii_case(name))
        || is_numbered_portable_device(basename, b"COM")
        || is_numbered_portable_device(basename, b"LPT");
    if reserved {
        return Err(CdfError::data(format!(
            "package path component uses a reserved portable device name: {component:?}"
        )));
    }
    Ok(())
}

fn is_numbered_portable_device(basename: &str, prefix: &[u8; 3]) -> bool {
    let bytes = basename.as_bytes();
    bytes.len() == 4 && bytes[..3].eq_ignore_ascii_case(prefix) && matches!(bytes[3], b'1'..=b'9')
}

fn portable_casefold_cmp(left: &str, right: &str) -> CmpOrdering {
    left.chars()
        .flat_map(char::to_lowercase)
        .cmp(right.chars().flat_map(char::to_lowercase))
}

pub(crate) fn portable_path_cmp(left: &str, right: &str) -> CmpOrdering {
    portable_casefold_cmp(left, right).then_with(|| left.cmp(right))
}

fn relative_path_string(base: &Path, path: &Path) -> Result<String> {
    let relative = path.strip_prefix(base).map_err(|error| {
        CdfError::internal(format!(
            "path {} is not under {}: {error}",
            path.display(),
            base.display()
        ))
    })?;
    normalize_relative_path(relative)
}

pub(crate) fn package_path(package_dir: &Path, relative_path: impl AsRef<str>) -> PathBuf {
    relative_path
        .as_ref()
        .split('/')
        .fold(package_dir.to_path_buf(), |path, part| path.join(part))
}

pub(crate) fn segment_relative_path(segment_id: &SegmentId) -> Result<String> {
    let id = segment_id.as_str();
    if id.contains('/')
        || id.contains('\\')
        || id.contains("..")
        || id.trim().is_empty()
        || id == "."
    {
        return Err(CdfError::data(format!(
            "segment id cannot be used as a package file name: {id:?}"
        )));
    }
    Ok(format!("data/{id}.arrow"))
}

pub(crate) fn io_error(context: impl Into<String>, error: std::io::Error) -> CdfError {
    CdfError::internal(format!("{}: {error}", context.into()))
}

#[cfg(test)]
mod tests {
    use std::{fs, io::Write, sync::Arc, time::Instant};

    use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
    use arrow_ipc::{
        CompressionType,
        writer::{FileWriter, IpcWriteOptions},
    };
    use arrow_schema::{DataType, Field, Schema};
    use sha2::{Digest, Sha256};

    use super::{
        ArtifactDurability, AtomicArtifactSink, HashingWriter, PublishBoundary, atomic_write,
        create_temp_sibling, encode_canonical_segment_ipc, sync_directory, write_arrow_ipc_file,
    };

    #[test]
    fn atomic_write_receipt_matches_published_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("artifact.bin");
        let bytes = b"receipt hashes exactly the installed bytes";

        let receipt = atomic_write(&path, bytes).unwrap();

        assert_eq!(receipt.path, path);
        assert_eq!(receipt.byte_count, bytes.len() as u64);
        assert_eq!(receipt.sha256, hex::encode(Sha256::digest(bytes)));
        assert_eq!(receipt.durability, ArtifactDurability::PhaseMetadata);
        assert_eq!(fs::read(receipt.path).unwrap(), bytes);
    }

    #[test]
    fn failed_publish_removes_exclusive_temporary_file() {
        let directory = tempfile::tempdir().unwrap();
        let final_path = directory.path().join("occupied");
        fs::create_dir(&final_path).unwrap();
        let mut sink =
            AtomicArtifactSink::create(&final_path, ArtifactDurability::SegmentPublish).unwrap();
        sink.writer_mut().unwrap().write_all(b"partial").unwrap();

        assert!(sink.finish().is_err());
        let names = fs::read_dir(directory.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["occupied"]);
    }

    #[test]
    fn dropped_unfinished_sink_leaves_no_partial_artifact() {
        let directory = tempfile::tempdir().unwrap();
        let final_path = directory.path().join("cancelled.bin");
        {
            let mut sink =
                AtomicArtifactSink::create(&final_path, ArtifactDurability::SegmentPublish)
                    .unwrap();
            sink.writer_mut().unwrap().write_all(b"partial").unwrap();
        }

        assert!(!final_path.exists());
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    #[test]
    fn panic_while_writing_leaves_no_partial_artifact() {
        let directory = tempfile::tempdir().unwrap();
        let final_path = directory.path().join("panicked.bin");

        let panic = std::panic::catch_unwind(|| {
            let mut sink =
                AtomicArtifactSink::create(&final_path, ArtifactDurability::SegmentPublish)
                    .unwrap();
            sink.writer_mut().unwrap().write_all(b"partial").unwrap();
            panic!("injected writer panic");
        });

        assert!(panic.is_err());
        assert!(!final_path.exists());
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    #[test]
    fn encoder_failure_leaves_no_partial_artifact() {
        use std::sync::Arc;

        use arrow_array::{Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};

        let directory = tempfile::tempdir().unwrap();
        let final_path = directory.path().join("bad.arrow");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let mut sink =
            AtomicArtifactSink::create(&final_path, ArtifactDurability::SegmentPublish).unwrap();
        sink.inject_failure(PublishBoundary::EncoderFinish);
        encode_canonical_segment_ipc(sink.writer_mut().unwrap(), schema.as_ref(), &[batch])
            .unwrap();
        assert!(sink.check_failure(PublishBoundary::EncoderFinish).is_err());
        drop(sink);

        assert!(!final_path.exists());
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    #[test]
    fn injected_publish_boundaries_return_no_receipt() {
        for boundary in [
            PublishBoundary::FileSync,
            PublishBoundary::Rename,
            PublishBoundary::DirectorySync,
        ] {
            let directory = tempfile::tempdir().unwrap();
            let final_path = directory.path().join("artifact.bin");
            let mut sink =
                AtomicArtifactSink::create(&final_path, ArtifactDurability::SegmentPublish)
                    .unwrap();
            sink.writer_mut().unwrap().write_all(b"complete").unwrap();
            sink.inject_failure(boundary);

            assert!(sink.finish().is_err(), "boundary {boundary:?}");
            let temp_count = fs::read_dir(directory.path())
                .unwrap()
                .filter_map(Result::ok)
                .filter(|entry| entry.file_name().to_string_lossy().contains(".tmp."))
                .count();
            assert_eq!(temp_count, 0, "boundary {boundary:?}");
            assert!(!final_path.exists(), "boundary {boundary:?}");
        }
    }

    #[test]
    #[ignore = "performance evidence; run in release mode"]
    fn hashing_writer_sha256_rate() {
        const CHUNK_BYTES: usize = 1024 * 1024;
        const CHUNKS: usize = 512;
        let chunk = vec![0x5a; CHUNK_BYTES];
        let mut writer = HashingWriter::new(std::io::sink());
        let started = Instant::now();
        for _ in 0..CHUNKS {
            writer.write_all(&chunk).unwrap();
        }
        let elapsed = started.elapsed();
        let gib_per_second =
            (CHUNK_BYTES * CHUNKS) as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0 * 1024.0);
        assert_eq!(writer.byte_count, (CHUNK_BYTES * CHUNKS) as u64);
        assert!(
            writer
                .hasher
                .finalize()
                .as_slice()
                .iter()
                .any(|byte| *byte != 0)
        );
        eprintln!(
            "sha256_rate_gib_s={gib_per_second:.3} bytes={} elapsed_ns={}",
            CHUNK_BYTES * CHUNKS,
            elapsed.as_nanos()
        );
    }

    #[test]
    #[ignore = "performance evidence; run in release mode"]
    fn hashing_writer_disk_overhead() {
        const CHUNK_BYTES: usize = 1024 * 1024;
        const CHUNKS: usize = 256;
        const SAMPLES: usize = 5;
        let directory = tempfile::tempdir().unwrap();
        let chunk = vec![0xa5; CHUNK_BYTES];
        let plain_path = directory.path().join("plain.bin");
        let hashed_path = directory.path().join("hashed.bin");
        let mut plain_samples = Vec::with_capacity(SAMPLES);
        let mut hashed_samples = Vec::with_capacity(SAMPLES);
        for sample in 0..SAMPLES {
            let run_plain = || {
                let started = Instant::now();
                let (temp_path, mut file) = create_temp_sibling(&plain_path).unwrap();
                for _ in 0..CHUNKS {
                    file.write_all(&chunk).unwrap();
                }
                file.flush().unwrap();
                file.sync_all().unwrap();
                fs::rename(temp_path, &plain_path).unwrap();
                sync_directory(directory.path()).unwrap();
                started.elapsed().as_nanos()
            };
            let run_hashed = || {
                let started = Instant::now();
                let mut sink =
                    AtomicArtifactSink::create(&hashed_path, ArtifactDurability::SegmentPublish)
                        .unwrap();
                for _ in 0..CHUNKS {
                    sink.writer_mut().unwrap().write_all(&chunk).unwrap();
                }
                let receipt = sink.finish().unwrap();
                assert_eq!(receipt.byte_count, (CHUNK_BYTES * CHUNKS) as u64);
                started.elapsed().as_nanos()
            };
            if sample % 2 == 0 {
                plain_samples.push(run_plain());
                hashed_samples.push(run_hashed());
            } else {
                hashed_samples.push(run_hashed());
                plain_samples.push(run_plain());
            }
        }
        plain_samples.sort_unstable();
        hashed_samples.sort_unstable();
        let plain_ns = plain_samples[SAMPLES / 2];
        let hashed_ns = hashed_samples[SAMPLES / 2];
        let overhead_percent = (hashed_ns as f64 - plain_ns as f64) * 100.0 / plain_ns as f64;
        eprintln!(
            "plain_atomic_median_ns={plain_ns} hash_atomic_median_ns={hashed_ns} hashing_overhead_percent={overhead_percent:.2} bytes_per_sample={} samples={SAMPLES}",
            CHUNK_BYTES * CHUNKS,
        );
    }

    #[test]
    #[ignore = "performance evidence; run in release mode"]
    fn arrow_ipc_package_writer_roofline() {
        const ROWS_PER_BATCH: usize = 64 * 1024;
        const BATCHES: usize = 32;
        const PARALLEL_BATCHES: usize = 8;
        const COLUMNS: usize = 8;
        const SAMPLES: usize = 5;
        const RAW_CHUNK_BYTES: usize = 1024 * 1024;

        let directory = std::env::var_os("CDF_E4_BENCH_DIR").map_or_else(
            || tempfile::tempdir().unwrap(),
            |root| tempfile::tempdir_in(root).unwrap(),
        );
        let fields = (0..COLUMNS)
            .map(|column| Field::new(format!("value_{column}"), DataType::UInt64, false))
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));
        let columns = (0..COLUMNS)
            .map(|column| {
                Arc::new(UInt64Array::from_iter_values(
                    (0..ROWS_PER_BATCH).map(|row| splitmix64(((column as u64) << 48) | row as u64)),
                )) as ArrayRef
            })
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        let batches = vec![batch; BATCHES];

        let warm_path = directory.path().join("warm.arrow");
        let warm_receipt = write_arrow_ipc_file(&warm_path, schema.as_ref(), &batches).unwrap();
        let encoded_bytes = warm_receipt.artifact.byte_count;
        fs::remove_file(&warm_path).unwrap();
        sync_directory(directory.path()).unwrap();

        let raw_chunk = vec![0xa5; RAW_CHUNK_BYTES];
        let mut plain_samples = Vec::with_capacity(SAMPLES);
        let mut hashed_samples = Vec::with_capacity(SAMPLES);
        let mut raw_samples = Vec::with_capacity(SAMPLES);
        for sample in 0..SAMPLES {
            let plain_path = directory.path().join(format!("plain-{sample}.arrow"));
            let hashed_path = directory.path().join(format!("hashed-{sample}.arrow"));
            let raw_path = directory.path().join(format!("raw-{sample}.bin"));

            let run_plain =
                || write_plain_arrow_ipc(&plain_path, schema.as_ref(), &batches, encoded_bytes);
            let run_hashed = || {
                let started = Instant::now();
                let receipt =
                    write_arrow_ipc_file(&hashed_path, schema.as_ref(), &batches).unwrap();
                assert_eq!(receipt.artifact.byte_count, encoded_bytes);
                started.elapsed().as_nanos()
            };
            if sample % 2 == 0 {
                plain_samples.push(run_plain());
                hashed_samples.push(run_hashed());
            } else {
                hashed_samples.push(run_hashed());
                plain_samples.push(run_plain());
            }
            raw_samples.push(write_raw_durable(&raw_path, encoded_bytes, &raw_chunk));

            for path in [&plain_path, &hashed_path, &raw_path] {
                fs::remove_file(path).unwrap();
            }
            sync_directory(directory.path()).unwrap();
        }

        let plain_observations = plain_samples.clone();
        let hashed_observations = hashed_samples.clone();
        let raw_observations = raw_samples.clone();
        plain_samples.sort_unstable();
        hashed_samples.sort_unstable();
        raw_samples.sort_unstable();
        let plain_ns = plain_samples[SAMPLES / 2];
        let hashed_ns = hashed_samples[SAMPLES / 2];
        let raw_ns = raw_samples[SAMPLES / 2];
        let hash_share_percent =
            (hashed_ns as f64 - plain_ns as f64).max(0.0) * 100.0 / hashed_ns as f64;
        let writer_roofline_ratio = raw_ns as f64 / hashed_ns as f64;
        let physical_mib = encoded_bytes as f64 / (1024.0 * 1024.0);
        let hashed_mib_per_second = physical_mib / (hashed_ns as f64 / 1_000_000_000.0);
        eprintln!(
            "encoded_bytes={encoded_bytes} rows={} plain_samples_ns={plain_observations:?} hashed_samples_ns={hashed_observations:?} raw_samples_ns={raw_observations:?} plain_ipc_median_ns={plain_ns} hashed_ipc_median_ns={hashed_ns} raw_durable_median_ns={raw_ns} hash_share_percent={hash_share_percent:.2} writer_roofline_ratio={writer_roofline_ratio:.3} hashed_mib_per_second={hashed_mib_per_second:.1}",
            ROWS_PER_BATCH * BATCHES,
        );

        let jobs = std::thread::available_parallelism().unwrap().get();
        let parallel_batches = (0..jobs)
            .map(|job| {
                (0..PARALLEL_BATCHES)
                    .map(|batch_index| {
                        let columns = (0..COLUMNS)
                            .map(|column| {
                                let stream = ((job as u64) << 56)
                                    ^ ((batch_index as u64) << 48)
                                    ^ ((column as u64) << 40);
                                Arc::new(UInt64Array::from_iter_values(
                                    (0..ROWS_PER_BATCH).map(|row| splitmix64(stream ^ row as u64)),
                                )) as ArrayRef
                            })
                            .collect::<Vec<_>>();
                        RecordBatch::try_new(Arc::clone(&schema), columns).unwrap()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let mut parallel_encoded_bytes = Vec::with_capacity(jobs);
        let mut raw_payloads = Vec::with_capacity(jobs);
        for (job, job_batches) in parallel_batches.iter().enumerate() {
            let path = directory.path().join(format!("parallel-warm-{job}.arrow"));
            let receipt = write_arrow_ipc_file(&path, schema.as_ref(), job_batches).unwrap();
            let bytes = fs::read(&path).unwrap();
            assert_eq!(receipt.artifact.byte_count, bytes.len() as u64);
            parallel_encoded_bytes.push(receipt.artifact.byte_count);
            raw_payloads.push(bytes);
            fs::remove_file(path).unwrap();
        }
        sync_directory(directory.path()).unwrap();
        let mut parallel_plain_samples = Vec::with_capacity(SAMPLES);
        let mut parallel_hashed_samples = Vec::with_capacity(SAMPLES);
        let mut parallel_raw_samples = Vec::with_capacity(SAMPLES);
        for sample in 0..SAMPLES {
            let run_plain = || {
                write_plain_arrow_ipc_parallel(
                    directory.path(),
                    sample,
                    jobs,
                    schema.as_ref(),
                    &parallel_batches,
                    &parallel_encoded_bytes,
                )
            };
            let run_hashed = || {
                write_hashed_arrow_ipc_parallel(
                    directory.path(),
                    sample,
                    jobs,
                    schema.as_ref(),
                    &parallel_batches,
                    &parallel_encoded_bytes,
                )
            };
            if sample % 2 == 0 {
                parallel_plain_samples.push(run_plain());
                parallel_hashed_samples.push(run_hashed());
            } else {
                parallel_hashed_samples.push(run_hashed());
                parallel_plain_samples.push(run_plain());
            }
            parallel_raw_samples.push(write_raw_durable_parallel(
                directory.path(),
                sample,
                jobs,
                &raw_payloads,
            ));
            remove_parallel_outputs(directory.path(), sample, jobs);
        }

        let parallel_plain_observations = parallel_plain_samples.clone();
        let parallel_hashed_observations = parallel_hashed_samples.clone();
        let parallel_raw_observations = parallel_raw_samples.clone();
        parallel_plain_samples.sort_unstable();
        parallel_hashed_samples.sort_unstable();
        parallel_raw_samples.sort_unstable();
        let parallel_plain_ns = parallel_plain_samples[SAMPLES / 2];
        let parallel_hashed_ns = parallel_hashed_samples[SAMPLES / 2];
        let parallel_raw_ns = parallel_raw_samples[SAMPLES / 2];
        let parallel_hash_share_percent =
            (parallel_hashed_ns as f64 - parallel_plain_ns as f64).max(0.0) * 100.0
                / parallel_hashed_ns as f64;
        let parallel_writer_roofline_ratio = parallel_raw_ns as f64 / parallel_hashed_ns as f64;
        let total_parallel_encoded_bytes = parallel_encoded_bytes.iter().sum::<u64>();
        let parallel_physical_mib = total_parallel_encoded_bytes as f64 / (1024.0 * 1024.0);
        let parallel_hashed_mib_per_second =
            parallel_physical_mib / (parallel_hashed_ns as f64 / 1_000_000_000.0);
        eprintln!(
            "jobs={jobs} total_encoded_bytes={} parallel_plain_samples_ns={parallel_plain_observations:?} parallel_hashed_samples_ns={parallel_hashed_observations:?} parallel_raw_samples_ns={parallel_raw_observations:?} parallel_plain_ipc_median_ns={parallel_plain_ns} parallel_hashed_ipc_median_ns={parallel_hashed_ns} parallel_raw_durable_median_ns={parallel_raw_ns} parallel_hash_share_percent={parallel_hash_share_percent:.2} parallel_writer_roofline_ratio={parallel_writer_roofline_ratio:.3} parallel_hashed_mib_per_second={parallel_hashed_mib_per_second:.1}",
            total_parallel_encoded_bytes,
        );

        if let Ok(target_gib) = std::env::var("CDF_E4_SUSTAINED_GIB") {
            let target_gib = target_gib.parse::<u64>().unwrap();
            assert!(target_gib > 0);
            let sustained_samples = std::env::var("CDF_E4_SUSTAINED_SAMPLES")
                .map(|value| value.parse::<usize>().unwrap())
                .unwrap_or(3);
            assert!(sustained_samples > 0);
            let target_bytes = target_gib * 1024 * 1024 * 1024;
            let waves = target_bytes.div_ceil(total_parallel_encoded_bytes) as usize;
            let bytes_per_sample = total_parallel_encoded_bytes * waves as u64;
            let mut sustained_plain_samples = Vec::with_capacity(sustained_samples);
            let mut sustained_hashed_samples = Vec::with_capacity(sustained_samples);
            for sample in 0..sustained_samples {
                let run_plain = || {
                    write_plain_arrow_ipc_sustained(
                        directory.path(),
                        sample,
                        waves,
                        jobs,
                        schema.as_ref(),
                        &parallel_batches,
                        &parallel_encoded_bytes,
                    )
                };
                let run_hashed = || {
                    write_hashed_arrow_ipc_sustained(
                        directory.path(),
                        sample,
                        waves,
                        jobs,
                        schema.as_ref(),
                        &parallel_batches,
                        &parallel_encoded_bytes,
                    )
                };
                if sample % 2 == 0 {
                    sustained_plain_samples.push(run_plain());
                    remove_sustained_outputs(directory.path(), "plain", sample, waves, jobs);
                    sustained_hashed_samples.push(run_hashed());
                    remove_sustained_outputs(directory.path(), "hashed", sample, waves, jobs);
                } else {
                    sustained_hashed_samples.push(run_hashed());
                    remove_sustained_outputs(directory.path(), "hashed", sample, waves, jobs);
                    sustained_plain_samples.push(run_plain());
                    remove_sustained_outputs(directory.path(), "plain", sample, waves, jobs);
                }
            }
            let sustained_plain_observations = sustained_plain_samples.clone();
            let sustained_hashed_observations = sustained_hashed_samples.clone();
            sustained_plain_samples.sort_unstable();
            sustained_hashed_samples.sort_unstable();
            let sustained_plain_ns = sustained_plain_samples[sustained_samples / 2];
            let sustained_hashed_ns = sustained_hashed_samples[sustained_samples / 2];
            let sustained_hash_share_percent =
                (sustained_hashed_ns as f64 - sustained_plain_ns as f64).max(0.0) * 100.0
                    / sustained_hashed_ns as f64;
            let sustained_mib = bytes_per_sample as f64 / (1024.0 * 1024.0);
            let sustained_hashed_mib_per_second =
                sustained_mib / (sustained_hashed_ns as f64 / 1_000_000_000.0);
            eprintln!(
                "sustained_jobs={jobs} sustained_waves={waves} sustained_bytes_per_sample={bytes_per_sample} sustained_plain_samples_ns={sustained_plain_observations:?} sustained_hashed_samples_ns={sustained_hashed_observations:?} sustained_plain_median_ns={sustained_plain_ns} sustained_hashed_median_ns={sustained_hashed_ns} sustained_hash_share_percent={sustained_hash_share_percent:.2} sustained_hashed_mib_per_second={sustained_hashed_mib_per_second:.1}"
            );
        }
    }

    fn write_plain_arrow_ipc_sustained(
        directory: &std::path::Path,
        sample: usize,
        waves: usize,
        jobs: usize,
        schema: &Schema,
        batches: &[Vec<RecordBatch>],
        expected_bytes: &[u64],
    ) -> u128 {
        let started = Instant::now();
        for wave in 0..waves {
            write_plain_arrow_ipc_parallel(
                directory,
                sample * waves + wave,
                jobs,
                schema,
                batches,
                expected_bytes,
            );
        }
        started.elapsed().as_nanos()
    }

    fn write_hashed_arrow_ipc_sustained(
        directory: &std::path::Path,
        sample: usize,
        waves: usize,
        jobs: usize,
        schema: &Schema,
        batches: &[Vec<RecordBatch>],
        expected_bytes: &[u64],
    ) -> u128 {
        let started = Instant::now();
        for wave in 0..waves {
            write_hashed_arrow_ipc_parallel(
                directory,
                sample * waves + wave,
                jobs,
                schema,
                batches,
                expected_bytes,
            );
        }
        started.elapsed().as_nanos()
    }

    fn remove_sustained_outputs(
        directory: &std::path::Path,
        kind: &str,
        sample: usize,
        waves: usize,
        jobs: usize,
    ) {
        for wave in 0..waves {
            let output_sample = sample * waves + wave;
            for job in 0..jobs {
                fs::remove_file(
                    directory.join(format!("parallel-{kind}-{output_sample}-{job}.arrow")),
                )
                .unwrap();
            }
        }
        sync_directory(directory).unwrap();
    }

    fn write_plain_arrow_ipc_parallel(
        directory: &std::path::Path,
        sample: usize,
        jobs: usize,
        schema: &Schema,
        batches: &[Vec<RecordBatch>],
        expected_bytes: &[u64],
    ) -> u128 {
        let started = Instant::now();
        std::thread::scope(|scope| {
            let handles = (0..jobs)
                .map(|job| {
                    let path = directory.join(format!("parallel-plain-{sample}-{job}.arrow"));
                    scope.spawn(move || {
                        write_plain_arrow_ipc(&path, schema, &batches[job], expected_bytes[job])
                    })
                })
                .collect::<Vec<_>>();
            for handle in handles {
                handle.join().unwrap();
            }
        });
        started.elapsed().as_nanos()
    }

    fn write_hashed_arrow_ipc_parallel(
        directory: &std::path::Path,
        sample: usize,
        jobs: usize,
        schema: &Schema,
        batches: &[Vec<RecordBatch>],
        expected_bytes: &[u64],
    ) -> u128 {
        let started = Instant::now();
        std::thread::scope(|scope| {
            let handles = (0..jobs)
                .map(|job| {
                    let path = directory.join(format!("parallel-hashed-{sample}-{job}.arrow"));
                    scope.spawn(move || {
                        let receipt = write_arrow_ipc_file(&path, schema, &batches[job]).unwrap();
                        assert_eq!(receipt.artifact.byte_count, expected_bytes[job]);
                    })
                })
                .collect::<Vec<_>>();
            for handle in handles {
                handle.join().unwrap();
            }
        });
        started.elapsed().as_nanos()
    }

    fn write_raw_durable_parallel(
        directory: &std::path::Path,
        sample: usize,
        jobs: usize,
        payloads: &[Vec<u8>],
    ) -> u128 {
        let started = Instant::now();
        std::thread::scope(|scope| {
            let handles = (0..jobs)
                .map(|job| {
                    let path = directory.join(format!("parallel-raw-{sample}-{job}.bin"));
                    scope.spawn(move || write_raw_durable_bytes(&path, &payloads[job]))
                })
                .collect::<Vec<_>>();
            for handle in handles {
                handle.join().unwrap();
            }
        });
        started.elapsed().as_nanos()
    }

    fn remove_parallel_outputs(directory: &std::path::Path, sample: usize, jobs: usize) {
        for job in 0..jobs {
            for path in [
                directory.join(format!("parallel-plain-{sample}-{job}.arrow")),
                directory.join(format!("parallel-hashed-{sample}-{job}.arrow")),
                directory.join(format!("parallel-raw-{sample}-{job}.bin")),
            ] {
                fs::remove_file(path).unwrap();
            }
        }
        sync_directory(directory).unwrap();
    }

    fn write_plain_arrow_ipc(
        path: &std::path::Path,
        schema: &Schema,
        batches: &[RecordBatch],
        expected_bytes: u64,
    ) -> u128 {
        let started = Instant::now();
        let (temp_path, mut file) = create_temp_sibling(path).unwrap();
        let options = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::LZ4_FRAME))
            .unwrap();
        {
            let mut writer = FileWriter::try_new_with_options(&mut file, schema, options).unwrap();
            for batch in batches {
                writer.write(batch).unwrap();
            }
            writer.finish().unwrap();
        }
        file.flush().unwrap();
        file.sync_all().unwrap();
        assert_eq!(file.metadata().unwrap().len(), expected_bytes);
        fs::rename(temp_path, path).unwrap();
        sync_directory(path.parent().unwrap()).unwrap();
        started.elapsed().as_nanos()
    }

    fn write_raw_durable(path: &std::path::Path, bytes: u64, chunk: &[u8]) -> u128 {
        let started = Instant::now();
        let (temp_path, mut file) = create_temp_sibling(path).unwrap();
        let mut remaining = bytes;
        while remaining > 0 {
            let write_len = usize::try_from(remaining.min(chunk.len() as u64)).unwrap();
            file.write_all(&chunk[..write_len]).unwrap();
            remaining -= write_len as u64;
        }
        file.flush().unwrap();
        file.sync_all().unwrap();
        fs::rename(temp_path, path).unwrap();
        sync_directory(path.parent().unwrap()).unwrap();
        started.elapsed().as_nanos()
    }

    fn write_raw_durable_bytes(path: &std::path::Path, bytes: &[u8]) -> u128 {
        let started = Instant::now();
        let (temp_path, mut file) = create_temp_sibling(path).unwrap();
        file.write_all(bytes).unwrap();
        file.flush().unwrap();
        file.sync_all().unwrap();
        fs::rename(temp_path, path).unwrap();
        sync_directory(path.parent().unwrap()).unwrap();
        started.elapsed().as_nanos()
    }

    fn splitmix64(value: u64) -> u64 {
        let mut value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }
}
