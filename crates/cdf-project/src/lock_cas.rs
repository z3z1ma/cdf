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

#[derive(Debug)]
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
    ensure_lock_guard_parent(lock_path, guard_parent)?;
    validate_lock_guard_leaf(&guard_path)?;
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true).truncate(false);
    configure_lock_guard_no_follow(&mut options);
    let file = options
        .open(&guard_path)
        .map_err(|error| lock_private_path_error("open", &guard_path, error))?;
    file.lock()
        .map_err(|error| lock_host_error("lock CDF mutation guard", &guard_path, error))?;
    Ok(LockFileMutationGuard { _file: file })
}

fn ensure_lock_guard_parent(lock_path: &Path, guard_parent: &Path) -> Result<()> {
    let project_root = lock_path.parent().ok_or_else(|| {
        CdfError::internal(format!(
            "cdf.lock path {} has no project parent",
            lock_path.display()
        ))
    })?;
    let relative = guard_parent.strip_prefix(project_root).map_err(|_| {
        CdfError::internal(format!(
            "CDF-managed lock guard parent {} escapes project root {}",
            guard_parent.display(),
            project_root.display()
        ))
    })?;
    let mut current = project_root.to_path_buf();
    for component in relative.components() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Ok(_) => {
                return Err(CdfError::internal(format!(
                    "CDF-managed lock guard ancestor {} is not a real directory",
                    current.display()
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match fs::create_dir(&current) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        let metadata = fs::symlink_metadata(&current).map_err(|error| {
                            lock_private_path_error("revalidate", &current, error)
                        })?;
                        if metadata.file_type().is_symlink() || !metadata.is_dir() {
                            return Err(CdfError::internal(format!(
                                "CDF-managed lock guard ancestor {} changed to a non-directory or symlink during creation",
                                current.display()
                            )));
                        }
                    }
                    Err(error) => {
                        return Err(lock_private_path_error("create", &current, error));
                    }
                }
            }
            Err(error) => return Err(lock_private_path_error("inspect", &current, error)),
        }
    }
    Ok(())
}

fn validate_lock_guard_leaf(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            Err(CdfError::internal(format!(
                "CDF-managed lock guard {} is not a real regular file",
                path.display()
            )))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(lock_private_path_error("inspect", path, error)),
    }
}

#[cfg(unix)]
fn configure_lock_guard_no_follow(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;

    options.custom_flags(libc::O_NOFOLLOW);
}

#[cfg(not(unix))]
fn configure_lock_guard_no_follow(_options: &mut OpenOptions) {}

pub fn write_lock_file_guarded(
    path: impl AsRef<Path>,
    expected: Option<&LockFileAuthority>,
    bytes: impl AsRef<[u8]>,
) -> Result<()> {
    write_lock_file_guarded_inner(path.as_ref(), expected, bytes.as_ref(), || Ok(()))
}

fn write_lock_file_guarded_inner<H>(
    path: &Path,
    expected: Option<&LockFileAuthority>,
    bytes: &[u8],
    publication_hook: H,
) -> Result<()>
where
    H: FnOnce() -> Result<()>,
{
    // `expected` must be the exact authority observed before deriving `bytes`;
    // `None` asserts that the lock file did not exist at that point.
    let _guard = acquire_lock_file_mutation_guard(path)?;
    match expected {
        Some(expected) => {
            validate_expected_authority(expected)?;
            assert_prior_bytes(path, expected)?;
        }
        None => match path.try_exists() {
            Ok(true) => {
                return Err(CdfError::contract(format!(
                    "cdf.lock guarded creation refused because {} now exists",
                    path.display()
                )));
            }
            Ok(false) => {}
            Err(error) => return Err(lock_cas_revalidation_error(path, error)),
        },
    }
    let temporary = write_synced_temporary(path, bytes)?;
    if let Err(error) = publication_hook() {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    let install = match expected {
        Some(expected) => {
            assert_prior_bytes(path, expected).and_then(|()| rename_over(&temporary, path))
        }
        None => fs::hard_link(&temporary, path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                CdfError::contract(format!(
                    "cdf.lock guarded creation refused a concurrent creation of {}",
                    path.display()
                ))
            } else {
                lock_host_error("atomically create without clobbering", path, error)
            }
        }),
    };
    if let Err(error) = install {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    if expected.is_none() {
        // The no-clobber hard link is the authority commit point. Once it
        // succeeds, scratch cleanup cannot coherently turn the installed lock
        // into a reported failure; a later transaction can remove stale temps.
        let _ = fs::remove_file(&temporary);
    }
    sync_parent_directory(path)?;
    Ok(())
}

pub fn read_lock_file_authority(path: impl AsRef<Path>) -> Result<LockFileAuthority> {
    let path = path.as_ref();
    let bytes = fs::read(path).map_err(|error| lock_artifact_read_error(path, error))?;
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
        .map_err(|error| lock_private_path_error("create", &temporary, error))?;
    if let Err(error) = file.write_all(replacement) {
        let _ = fs::remove_file(&temporary);
        return Err(lock_host_error("write", &temporary, error));
    }
    if failpoint == Some(LockFileCasFailpoint::BeforeTempSync) {
        return Err(failpoint_error("before temporary-file sync"));
    }
    if let Err(error) = file.sync_all() {
        let _ = fs::remove_file(&temporary);
        return Err(lock_host_error("sync", &temporary, error));
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
    if let Err(error) = assert_prior_bytes(path, expected) {
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
    let bytes = fs::read(path).map_err(|error| lock_cas_revalidation_error(path, error))?;
    let current = LockFileAuthority::from_bytes(bytes);
    if current.bytes == expected.bytes && current.sha256 == expected.sha256 {
        Ok(())
    } else {
        Err(CdfError::contract(format!(
            "cdf.lock compare-and-swap refused because prior authority changed: expected {}, found {}",
            expected.sha256, current.sha256
        )))
    }
}

fn lock_cas_revalidation_error(path: &Path, error: std::io::Error) -> CdfError {
    if matches!(
        error.kind(),
        std::io::ErrorKind::NotFound
            | std::io::ErrorKind::NotADirectory
            | std::io::ErrorKind::IsADirectory
    ) || cdf_kernel::is_filesystem_loop(&error)
    {
        CdfError::contract(format!(
            "cdf.lock compare-and-swap refused because prior authority changed at {}: {error}",
            path.display()
        ))
    } else {
        lock_host_error("re-read cdf.lock before publication", path, error)
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
            .map_err(|error| lock_private_path_error("create", &temporary, error))?;
        file.write_all(bytes)
            .map_err(|error| lock_host_error("write", &temporary, error))?;
        file.sync_all()
            .map_err(|error| lock_host_error("sync", &temporary, error))?;
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
    fs::rename(temporary, path).map_err(|error| lock_publication_error(temporary, path, error))
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
        .map_err(|error| lock_host_error("sync", parent, error))?;
    Ok(true)
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &Path) -> Result<bool> {
    Ok(false)
}

fn failpoint_error(stage: &str) -> CdfError {
    CdfError::internal(format!("injected cdf.lock publication crash {stage}"))
}

fn lock_artifact_read_error(path: &Path, error: std::io::Error) -> CdfError {
    if matches!(
        error.kind(),
        std::io::ErrorKind::NotFound
            | std::io::ErrorKind::NotADirectory
            | std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::InvalidData
            | std::io::ErrorKind::IsADirectory
    ) || cdf_kernel::is_filesystem_loop(&error)
    {
        CdfError::data(format!(
            "read cdf.lock artifact {}: {error}",
            path.display()
        ))
    } else {
        lock_host_error("read cdf.lock artifact", path, error)
    }
}

fn lock_host_error(action: &str, path: &Path, error: std::io::Error) -> CdfError {
    CdfError::environment(format!(
        "{action} {}: {error}; check project-path permissions, free space, device availability, and process file limits before retrying",
        path.display()
    ))
}

fn lock_private_path_error(action: &str, path: &Path, error: std::io::Error) -> CdfError {
    if matches!(
        error.kind(),
        std::io::ErrorKind::NotFound
            | std::io::ErrorKind::NotADirectory
            | std::io::ErrorKind::IsADirectory
            | std::io::ErrorKind::AlreadyExists
            | std::io::ErrorKind::InvalidInput
            | std::io::ErrorKind::InvalidData
    ) || cdf_kernel::is_filesystem_loop(&error)
    {
        CdfError::internal(format!(
            "{action} CDF-managed lock guard path {} with invalid filesystem shape: {error}",
            path.display()
        ))
    } else {
        lock_host_error(action, path, error)
    }
}

fn lock_publication_error(temporary: &Path, path: &Path, error: std::io::Error) -> CdfError {
    if matches!(
        error.kind(),
        std::io::ErrorKind::NotFound
            | std::io::ErrorKind::NotADirectory
            | std::io::ErrorKind::IsADirectory
            | std::io::ErrorKind::AlreadyExists
            | std::io::ErrorKind::InvalidInput
            | std::io::ErrorKind::InvalidData
    ) || cdf_kernel::is_filesystem_loop(&error)
    {
        CdfError::contract(format!(
            "cdf.lock compare-and-swap publication refused because authority changed while renaming {} over {}: {error}",
            temporary.display(),
            path.display()
        ))
    } else {
        CdfError::environment(format!(
            "atomically rename {} over {}: {error}; check project-path permissions, free space, device availability, and process file limits before retrying",
            temporary.display(),
            path.display()
        ))
    }
}

#[cfg(test)]
mod tests {
    use cdf_kernel::{ErrorKind, LeaseOwnerId, ScopeKey};
    use cdf_state_sqlite::InMemoryScopeLeaseStore;

    use super::*;

    #[test]
    fn lock_artifact_read_separates_shape_from_host_failures() {
        let path = Path::new("cdf.lock");
        let missing =
            lock_artifact_read_error(path, std::io::Error::from(std::io::ErrorKind::NotFound));
        assert_eq!(missing.kind, ErrorKind::Data);

        let denied = lock_artifact_read_error(
            path,
            std::io::Error::from(std::io::ErrorKind::PermissionDenied),
        );
        assert_eq!(denied.kind, ErrorKind::Environment);
        assert!(denied.message.contains("project-path permissions"));
    }

    #[test]
    fn standalone_lock_read_with_regular_file_parent_is_data_owned() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("project");
        fs::write(&parent, b"not a directory").unwrap();

        let error = read_lock_file_authority(parent.join("cdf.lock")).unwrap_err();

        assert_eq!(error.kind, ErrorKind::Data);
    }

    #[test]
    fn mutation_guard_wrong_shape_is_internal() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join(".cdf"), b"not a directory").unwrap();

        let error = acquire_lock_file_mutation_guard(root.path().join("cdf.lock")).unwrap_err();

        assert_eq!(error.kind, ErrorKind::Internal);
        assert!(error.message.contains("lock guard"));
    }

    #[cfg(unix)]
    #[test]
    fn mutation_guard_rejects_live_and_dangling_symlink_authority() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        symlink(outside.path(), root.path().join(".cdf")).unwrap();
        let live_error =
            acquire_lock_file_mutation_guard(root.path().join("cdf.lock")).unwrap_err();
        assert_eq!(live_error.kind, ErrorKind::Internal);
        assert!(!outside.path().join("locks").exists());

        fs::remove_file(root.path().join(".cdf")).unwrap();
        symlink(root.path().join("missing"), root.path().join(".cdf")).unwrap();
        let dangling_error =
            acquire_lock_file_mutation_guard(root.path().join("cdf.lock")).unwrap_err();
        assert_eq!(dangling_error.kind, ErrorKind::Internal);

        fs::remove_file(root.path().join(".cdf")).unwrap();
        let locks = root.path().join(".cdf/locks");
        fs::create_dir_all(&locks).unwrap();
        let outside_lock = outside.path().join("guard");
        fs::write(&outside_lock, b"outside").unwrap();
        symlink(&outside_lock, locks.join("cdf.lock.mutation.lock")).unwrap();
        let leaf_error =
            acquire_lock_file_mutation_guard(root.path().join("cdf.lock")).unwrap_err();
        assert_eq!(leaf_error.kind, ErrorKind::Internal);
        assert_eq!(fs::read(outside_lock).unwrap(), b"outside");
    }

    #[test]
    fn guarded_creation_race_is_contract_and_preserves_the_racer() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("cdf.lock");

        let error = write_lock_file_guarded_inner(&path, None, b"ours", || {
            fs::write(&path, b"racer").unwrap();
            Ok(())
        })
        .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Contract);
        assert!(error.message.contains("concurrent creation"));
        assert_eq!(fs::read(path).unwrap(), b"racer");
    }

    #[test]
    fn guarded_replacement_race_is_contract_and_preserves_the_racer() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("cdf.lock");
        fs::write(&path, b"observed").unwrap();
        let expected = read_lock_file_authority(&path).unwrap();

        let error = write_lock_file_guarded_inner(&path, Some(&expected), b"ours", || {
            fs::write(&path, b"racer").unwrap();
            Ok(())
        })
        .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Contract);
        assert_eq!(fs::read(path).unwrap(), b"racer");
    }

    #[cfg(unix)]
    #[test]
    fn leased_cas_rechecks_after_publication_hook_and_preserves_the_racer() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("cdf.lock");
        fs::write(&path, b"observed").unwrap();
        let expected = read_lock_file_authority(&path).unwrap();
        let store = InMemoryScopeLeaseStore::new();
        let lease = store
            .acquire(
                ScopeKey::Resource,
                LeaseOwnerId::new("lock-cas-test").unwrap(),
                1_000,
            )
            .unwrap();

        let error = compare_and_swap_lock_file_with_publication_hook(
            &path,
            &expected,
            b"ours",
            &store,
            &lease,
            || {
                fs::write(&path, b"racer").unwrap();
                Ok(())
            },
        )
        .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Contract);
        assert_eq!(fs::read(path).unwrap(), b"racer");
    }

    #[test]
    fn cas_revalidation_shape_changes_are_contract_owned() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("cdf.lock");
        fs::write(&path, b"observed").unwrap();
        let expected = read_lock_file_authority(&path).unwrap();

        fs::remove_file(&path).unwrap();
        let deletion = assert_prior_bytes(&path, &expected).unwrap_err();
        assert_eq!(deletion.kind, ErrorKind::Contract);

        fs::create_dir(&path).unwrap();
        let directory = assert_prior_bytes(&path, &expected).unwrap_err();
        assert_eq!(directory.kind, ErrorKind::Contract);
    }
}
