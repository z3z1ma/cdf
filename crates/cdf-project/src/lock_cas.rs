use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    process,
    sync::atomic::{AtomicU64, Ordering},
};

use cdf_kernel::{CdfError, Result, ScopeLease, ScopeLeaseStore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LockFileAuthority {
    pub bytes: Vec<u8>,
    pub sha256: String,
}

impl LockFileAuthority {
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let sha256 = lock_bytes_hash(&bytes);
        Self { bytes, sha256 }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LockFileCasFailpoint {
    BeforeTempSync,
    BeforeRename,
    AfterRename,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct LockFileCasReport {
    pub installed: LockFileAuthority,
    pub parent_directory_synced: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct LockFileAtomicityCapabilities {
    pub atomic_rename_over_existing: bool,
    pub parent_directory_fsync: bool,
    pub cooperating_cdf_writers_serialized: bool,
    pub limitation: &'static str,
}

pub fn lock_file_atomicity_capabilities() -> LockFileAtomicityCapabilities {
    #[cfg(unix)]
    {
        LockFileAtomicityCapabilities {
            atomic_rename_over_existing: true,
            parent_directory_fsync: true,
            cooperating_cdf_writers_serialized: true,
            limitation: "CDF writers serialize through an advisory project lock, but non-cooperating filesystem actors remain outside that protocol; atomicity and durability require the temporary file and cdf.lock to remain on the same Unix filesystem with POSIX rename and fsync semantics",
        }
    }
    #[cfg(not(unix))]
    {
        LockFileAtomicityCapabilities {
            atomic_rename_over_existing: false,
            parent_directory_fsync: false,
            cooperating_cdf_writers_serialized: true,
            limitation: "CDF writers serialize through an advisory project lock, but non-cooperating filesystem actors remain outside that protocol; Rust std does not provide portable atomic rename-over-existing and directory fsync, so replacement is refused on this platform",
        }
    }
}

pub struct LockFileMutationGuard {
    _file: File,
}

pub fn acquire_lock_file_mutation_guard(
    lock_path: impl AsRef<Path>,
) -> Result<LockFileMutationGuard> {
    let lock_path = lock_path.as_ref();
    let guard_path = mutation_guard_path(lock_path)?;
    let guard_parent = guard_path.parent().ok_or_else(|| {
        CdfError::internal(format!(
            "mutation guard {} has no parent",
            guard_path.display()
        ))
    })?;
    fs::create_dir_all(guard_parent)
        .map_err(|error| CdfError::data(format!("create {}: {error}", guard_parent.display())))?;
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&guard_path)
        .map_err(|error| CdfError::data(format!("open {}: {error}", guard_path.display())))?;
    file.lock().map_err(|error| {
        CdfError::data(format!(
            "lock CDF mutation guard {}: {error}",
            guard_path.display()
        ))
    })?;
    Ok(LockFileMutationGuard { _file: file })
}

pub fn write_lock_file_guarded(
    path: impl AsRef<Path>,
    expected: Option<&LockFileAuthority>,
    bytes: impl AsRef<[u8]>,
) -> Result<()> {
    // `expected` must be the exact authority observed before deriving `bytes`;
    // `None` asserts that the lock file did not exist at that point.
    let path = path.as_ref();
    let _guard = acquire_lock_file_mutation_guard(path)?;
    match expected {
        Some(expected) => {
            validate_expected_authority(expected)?;
            assert_prior_bytes(path, expected)?;
        }
        None if path.exists() => {
            return Err(CdfError::contract(format!(
                "cdf.lock guarded creation refused because {} now exists",
                path.display()
            )));
        }
        None => {}
    }
    let temporary = write_synced_temporary(path, bytes.as_ref())?;
    let install = match expected {
        Some(expected) => {
            assert_prior_bytes(path, expected).and_then(|()| rename_over(&temporary, path))
        }
        None => fs::hard_link(&temporary, path).map_err(|error| {
            CdfError::data(format!(
                "atomically create {} without clobbering: {error}",
                path.display()
            ))
        }),
    };
    if let Err(error) = install {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    if expected.is_none() {
        fs::remove_file(&temporary)
            .map_err(|error| CdfError::data(format!("remove {}: {error}", temporary.display())))?;
    }
    sync_parent_directory(path)?;
    Ok(())
}

pub fn read_lock_file_authority(path: impl AsRef<Path>) -> Result<LockFileAuthority> {
    let path = path.as_ref();
    let bytes = fs::read(path)
        .map_err(|error| CdfError::data(format!("read {}: {error}", path.display())))?;
    Ok(LockFileAuthority::from_bytes(bytes))
}

pub fn compare_and_swap_lock_file<S: ScopeLeaseStore>(
    path: impl AsRef<Path>,
    expected: &LockFileAuthority,
    replacement: &[u8],
    lease_store: &S,
    lease: &ScopeLease,
) -> Result<LockFileCasReport> {
    compare_and_swap_lock_file_with_failpoint(path, expected, replacement, lease_store, lease, None)
}

pub fn compare_and_swap_lock_file_with_failpoint<S: ScopeLeaseStore>(
    path: impl AsRef<Path>,
    expected: &LockFileAuthority,
    replacement: &[u8],
    lease_store: &S,
    lease: &ScopeLease,
    failpoint: Option<LockFileCasFailpoint>,
) -> Result<LockFileCasReport> {
    compare_and_swap_lock_file_inner(
        path,
        expected,
        replacement,
        lease_store,
        lease,
        failpoint,
        || Ok(()),
    )
}

#[cfg(test)]
pub(crate) fn compare_and_swap_lock_file_with_publication_hook<S, H>(
    path: impl AsRef<Path>,
    expected: &LockFileAuthority,
    replacement: &[u8],
    lease_store: &S,
    lease: &ScopeLease,
    publication_hook: H,
) -> Result<LockFileCasReport>
where
    S: ScopeLeaseStore,
    H: FnOnce() -> Result<()>,
{
    compare_and_swap_lock_file_inner(
        path,
        expected,
        replacement,
        lease_store,
        lease,
        None,
        publication_hook,
    )
}

fn compare_and_swap_lock_file_inner<S, H>(
    path: impl AsRef<Path>,
    expected: &LockFileAuthority,
    replacement: &[u8],
    lease_store: &S,
    lease: &ScopeLease,
    failpoint: Option<LockFileCasFailpoint>,
    publication_hook: H,
) -> Result<LockFileCasReport>
where
    S: ScopeLeaseStore,
    H: FnOnce() -> Result<()>,
{
    let path = path.as_ref();
    validate_expected_authority(expected)?;
    let _mutation_guard = acquire_lock_file_mutation_guard(path)?;
    lease_store.assert_current(lease)?;
    assert_prior_bytes(path, expected)?;

    let temporary = temporary_path(path)?;
    let installed = LockFileAuthority::from_bytes(replacement.to_vec());
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| CdfError::data(format!("create {}: {error}", temporary.display())))?;
    if let Err(error) = file.write_all(replacement) {
        let _ = fs::remove_file(&temporary);
        return Err(CdfError::data(format!(
            "write {}: {error}",
            temporary.display()
        )));
    }
    if failpoint == Some(LockFileCasFailpoint::BeforeTempSync) {
        return Err(failpoint_error("before temporary-file sync"));
    }
    if let Err(error) = file.sync_all() {
        let _ = fs::remove_file(&temporary);
        return Err(CdfError::data(format!(
            "sync {}: {error}",
            temporary.display()
        )));
    }
    drop(file);

    // Recheck both authorities at the publication boundary. The lease serializes
    // cooperating writers; exact bytes detect edits made outside that protocol.
    if let Err(error) = lease_store.assert_current(lease) {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    if let Err(error) = assert_prior_bytes(path, expected) {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    if let Err(error) = publication_hook() {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    if failpoint == Some(LockFileCasFailpoint::BeforeRename) {
        return Err(failpoint_error("before rename"));
    }

    if let Err(error) = rename_over(&temporary, path) {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    if failpoint == Some(LockFileCasFailpoint::AfterRename) {
        return Err(failpoint_error(
            "after rename and before parent-directory sync",
        ));
    }
    let parent_directory_synced = sync_parent_directory(path)?;
    Ok(LockFileCasReport {
        installed,
        parent_directory_synced,
    })
}

fn validate_expected_authority(expected: &LockFileAuthority) -> Result<()> {
    let actual = lock_bytes_hash(&expected.bytes);
    if actual == expected.sha256 {
        Ok(())
    } else {
        Err(CdfError::contract(format!(
            "expected cdf.lock bytes hash to {}, but the supplied bytes hash to {actual}",
            expected.sha256
        )))
    }
}

fn assert_prior_bytes(path: &Path, expected: &LockFileAuthority) -> Result<()> {
    let current = read_lock_file_authority(path)?;
    if current.bytes == expected.bytes && current.sha256 == expected.sha256 {
        Ok(())
    } else {
        Err(CdfError::contract(format!(
            "cdf.lock compare-and-swap refused because prior authority changed: expected {}, found {}",
            expected.sha256, current.sha256
        )))
    }
}

fn temporary_path(path: &Path) -> Result<PathBuf> {
    let parent = path.parent().ok_or_else(|| {
        CdfError::contract(format!("cdf.lock path {} has no parent", path.display()))
    })?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            CdfError::contract(format!(
                "cdf.lock path {} has no UTF-8 filename",
                path.display()
            ))
        })?;
    let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    Ok(parent.join(format!(".{name}.{}.{}.cas.tmp", process::id(), sequence)))
}

fn write_synced_temporary(path: &Path, bytes: &[u8]) -> Result<PathBuf> {
    let temporary = temporary_path(path)?;
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| CdfError::data(format!("create {}: {error}", temporary.display())))?;
        file.write_all(bytes)
            .map_err(|error| CdfError::data(format!("write {}: {error}", temporary.display())))?;
        file.sync_all()
            .map_err(|error| CdfError::data(format!("sync {}: {error}", temporary.display())))?;
        Ok(())
    })();
    if let Err(error) = result {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    Ok(temporary)
}

fn mutation_guard_path(path: &Path) -> Result<PathBuf> {
    let parent = path.parent().ok_or_else(|| {
        CdfError::contract(format!("cdf.lock path {} has no parent", path.display()))
    })?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            CdfError::contract(format!(
                "cdf.lock path {} has no UTF-8 filename",
                path.display()
            ))
        })?;
    Ok(parent
        .join(".cdf")
        .join("locks")
        .join(format!("{name}.mutation.lock")))
}

fn lock_bytes_hash(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

#[cfg(unix)]
fn rename_over(temporary: &Path, path: &Path) -> Result<()> {
    fs::rename(temporary, path).map_err(|error| {
        CdfError::data(format!(
            "atomically rename {} over {}: {error}",
            temporary.display(),
            path.display()
        ))
    })
}

#[cfg(not(unix))]
fn rename_over(_temporary: &Path, path: &Path) -> Result<()> {
    Err(CdfError::contract(format!(
        "atomic rename-over-existing for {} is unsupported on this platform; cdf.lock was not changed",
        path.display()
    )))
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> Result<bool> {
    let parent = path.parent().ok_or_else(|| {
        CdfError::contract(format!("cdf.lock path {} has no parent", path.display()))
    })?;
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| CdfError::data(format!("sync {}: {error}", parent.display())))?;
    Ok(true)
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &Path) -> Result<bool> {
    Ok(false)
}

fn failpoint_error(stage: &str) -> CdfError {
    CdfError::internal(format!("injected cdf.lock publication crash {stage}"))
}
