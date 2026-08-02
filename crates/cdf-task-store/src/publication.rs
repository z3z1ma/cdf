//! Atomic filesystem publication and artifact I/O error classification.

use std::fs::{self, File};
use std::io::{self};
use std::path::Path;

use cdf_kernel::{CdfError, Result};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;

pub(crate) fn install_content_addressed(
    temporary: NamedTempFile,
    final_path: &Path,
    expected_bytes: u64,
    expected_sha256: &str,
) -> Result<()> {
    if let Some(parent) = final_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| io_error("create task-set content directory", parent, error))?;
    }
    match temporary.persist_noclobber(final_path) {
        Ok(_) => {
            sync_parent(final_path)?;
            Ok(())
        }
        Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {
            verify_file(final_path, expected_bytes, expected_sha256)
        }
        Err(error) => Err(io_error(
            "install task-set content address",
            final_path,
            error.error,
        )),
    }
}

fn verify_file(path: &Path, expected_bytes: u64, expected_sha256: &str) -> Result<()> {
    let mut file = File::open(path)
        .map_err(|error| artifact_io_error("verify task-set artifact", path, error))?;
    let mut hasher = Sha256::new();
    let bytes = io::copy(&mut file, &mut hasher)
        .map_err(|error| artifact_io_error("hash task-set artifact", path, error))?;
    let digest = format!("sha256:{}", hex::encode(hasher.finalize()));
    if bytes != expected_bytes || digest != expected_sha256 {
        return Err(CdfError::contract(format!(
            "content-addressed task-set path {} contains different bytes",
            path.display()
        )));
    }
    Ok(())
}

fn sync_parent(path: &Path) -> Result<()> {
    #[cfg(unix)]
    if let Some(parent) = path.parent() {
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| io_error("sync task-set directory", parent, error))?;
    }
    Ok(())
}

pub(crate) fn io_error(action: &str, path: &Path, error: io::Error) -> CdfError {
    if let Some(mut classified) = cdf_kernel::embedded_cdf_error(&error) {
        classified.message = format!("{action} {}: {}", path.display(), classified.message);
        return classified;
    }
    CdfError::environment(format!(
        "{action} {}: {error}; check the local path, permissions, temporary storage, and process file limits before retrying",
        path.display()
    ))
}

pub(crate) fn artifact_io_error(action: &str, path: &Path, error: io::Error) -> CdfError {
    if let Some(mut classified) = cdf_kernel::embedded_cdf_error(&error) {
        classified.message = format!("{action} {}: {}", path.display(), classified.message);
        return classified;
    }
    if matches!(
        error.kind(),
        io::ErrorKind::NotFound
            | io::ErrorKind::UnexpectedEof
            | io::ErrorKind::InvalidData
            | io::ErrorKind::NotADirectory
            | io::ErrorKind::IsADirectory
    ) || cdf_kernel::is_filesystem_loop(&error)
    {
        CdfError::data(format!("{action} {}: {error}", path.display()))
    } else {
        io_error(action, path, error)
    }
}
