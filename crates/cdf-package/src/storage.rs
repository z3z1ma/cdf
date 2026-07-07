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
    atomic_write(&path, &bytes)
}

pub(crate) fn collect_identity_file_entries(package_dir: &Path) -> Result<Vec<FileEntry>> {
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
) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        CdfError::internal(format!("path {} has no parent directory", path.display()))
    })?;
    fs::create_dir_all(parent)
        .map_err(|error| io_error(format!("create {}", parent.display()), error))?;
    let (tmp_path, mut file) = create_temp_sibling(path)?;
    let write_result = (|| {
        let options = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::LZ4_FRAME))
            .map_err(CdfError::from)?;
        {
            let mut writer = FileWriter::try_new_with_options(&mut file, schema, options)
                .map_err(CdfError::from)?;
            for batch in batches {
                writer.write(batch).map_err(CdfError::from)?;
            }
            writer.finish().map_err(CdfError::from)?;
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

pub(crate) fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        CdfError::internal(format!("path {} has no parent directory", path.display()))
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
