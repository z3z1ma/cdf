use std::{
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
use sha2::{Digest, Sha256};

use crate::{
    json::{canonical_json_bytes, manifest_identity_hash},
    model::{
        FileEntry, LifecycleState, MANIFEST_FILE, MANIFEST_VERSION, ManifestIdentity,
        PackageManifest, PackageStatus, RECEIPTS_FILE, REQUIRED_DIRECTORIES, SegmentEntry,
        SignatureSlot, TRACE_FILE,
    },
};

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

struct HashingWriter<W> {
    inner: W,
    hasher: Sha256,
    byte_count: u64,
}

impl<W> HashingWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
            byte_count: 0,
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

struct AtomicArtifactSink {
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
    fn create(path: &Path, durability: ArtifactDurability) -> Result<Self> {
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

    fn writer_mut(&mut self) -> Result<&mut HashingWriter<File>> {
        self.writer
            .as_mut()
            .ok_or_else(|| CdfError::internal("artifact sink is already finished"))
    }

    fn finish(mut self) -> Result<WrittenArtifact> {
        let mut writer = self
            .writer
            .take()
            .ok_or_else(|| CdfError::internal("artifact sink is already finished"))?;
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
            #[cfg(test)]
            self.check_failure(PublishBoundary::DirectorySync)?;
            sync_directory(&self.parent)
        })();
        if let Err(error) = publish {
            let _ = fs::remove_file(&self.temp_path);
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

fn package_layout() -> Vec<String> {
    let mut layout = REQUIRED_DIRECTORIES
        .iter()
        .map(|directory| format!("{directory}/"))
        .collect::<Vec<_>>();
    layout.push(TRACE_FILE.to_owned());
    layout
}

pub(crate) fn write_manifest_atomic(package_dir: &Path, manifest: &PackageManifest) -> Result<()> {
    let path = package_dir.join(MANIFEST_FILE);
    let bytes = canonical_json_bytes(manifest)?;
    atomic_write(&path, &bytes).map(|_| ())
}

pub(crate) fn collect_identity_file_entries(package_dir: &Path) -> Result<Vec<FileEntry>> {
    collect_identity_file_paths(package_dir)?
        .iter()
        .map(|path| file_entry_for_path(package_dir, path))
        .collect()
}

pub(crate) fn collect_identity_file_paths(package_dir: &Path) -> Result<Vec<String>> {
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
    Ok(relative_paths)
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
    encode_arrow_ipc(&mut sink, schema, batches)?;
    let encode_hash_duration_ns = duration_ns(encode_started, "IPC encode/hash")?;
    let publish_started = std::time::Instant::now();
    let artifact = sink.finish()?;
    Ok(IpcWriteReceipt {
        artifact,
        encode_hash_duration_ns,
        publish_duration_ns: duration_ns(publish_started, "IPC publish")?,
    })
}

fn encode_arrow_ipc(
    sink: &mut AtomicArtifactSink,
    schema: &arrow_schema::Schema,
    batches: &[RecordBatch],
) -> Result<()> {
    let options = IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::LZ4_FRAME))
        .map_err(CdfError::from)?;
    let mut writer = FileWriter::try_new_with_options(sink.writer_mut()?, schema, options)
        .map_err(CdfError::from)?;
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
        || REQUIRED_DIRECTORIES
            .iter()
            .any(|directory| relative_path.starts_with(&format!("{directory}/")))
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

fn normalize_relative_path(path: &Path) -> Result<String> {
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
                parts.push(part.to_owned());
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
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
    use std::{fs, io::Write, time::Instant};

    use sha2::{Digest, Sha256};

    use super::{
        ArtifactDurability, AtomicArtifactSink, HashingWriter, PublishBoundary, atomic_write,
        create_temp_sibling, encode_arrow_ipc, sync_directory,
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
        encode_arrow_ipc(&mut sink, schema.as_ref(), &[batch]).unwrap();
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
            if boundary != PublishBoundary::DirectorySync {
                assert!(!final_path.exists(), "boundary {boundary:?}");
            } else {
                assert_eq!(fs::read(final_path).unwrap(), b"complete");
            }
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
}
