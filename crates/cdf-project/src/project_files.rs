use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions, Permissions},
    io::Write,
    path::{Component, Path, PathBuf},
    process,
    sync::atomic::{AtomicU64, Ordering},
};

use cdf_kernel::{CdfError, Result};

use crate::acquire_lock_file_mutation_guard;

static TRANSACTION_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub enum ProjectFileExpectation {
    Absent,
    Exact(Vec<u8>),
    AbsentOrExact(Vec<u8>),
}

pub struct ProjectFileWrite {
    relative_path: PathBuf,
    bytes: Vec<u8>,
    expectation: ProjectFileExpectation,
    owner_only: bool,
}

impl ProjectFileWrite {
    pub fn new(
        relative_path: impl Into<PathBuf>,
        bytes: impl Into<Vec<u8>>,
        expectation: ProjectFileExpectation,
    ) -> Self {
        Self {
            relative_path: relative_path.into(),
            bytes: bytes.into(),
            expectation,
            owner_only: false,
        }
    }

    pub fn owner_only(mut self) -> Self {
        self.owner_only = true;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectFileTransactionReport {
    pub installed_paths: Vec<PathBuf>,
    pub unchanged_paths: Vec<PathBuf>,
}

pub fn publish_project_files_transactionally(
    project_root: impl AsRef<Path>,
    commit_relative_path: impl AsRef<Path>,
    writes: Vec<ProjectFileWrite>,
) -> Result<ProjectFileTransactionReport> {
    publish_project_files_inner(
        project_root.as_ref(),
        commit_relative_path.as_ref(),
        writes,
        None,
    )
}

fn publish_project_files_inner(
    project_root: &Path,
    commit_relative_path: &Path,
    writes: Vec<ProjectFileWrite>,
    fail_after_install_count: Option<usize>,
) -> Result<ProjectFileTransactionReport> {
    publish_project_files_inner_with_hook(
        project_root,
        commit_relative_path,
        writes,
        fail_after_install_count,
        &mut |_| Ok(()),
        &mut |_, _| Ok(()),
    )
}

fn publish_project_files_inner_with_hook(
    project_root: &Path,
    commit_relative_path: &Path,
    writes: Vec<ProjectFileWrite>,
    fail_after_install_count: Option<usize>,
    before_install: &mut dyn FnMut(&Path) -> Result<()>,
    after_absent_install: &mut dyn FnMut(&Path, &Path) -> Result<()>,
) -> Result<ProjectFileTransactionReport> {
    validate_relative_path(commit_relative_path)?;
    let Some(last) = writes.last() else {
        return Err(CdfError::contract(
            "project file transaction requires at least one write",
        ));
    };
    if last.relative_path != commit_relative_path {
        return Err(CdfError::contract(format!(
            "project file transaction commit point {} must be the final write",
            commit_relative_path.display()
        )));
    }

    let mut unique = BTreeSet::new();
    for write in &writes {
        validate_relative_path(&write.relative_path)?;
        if !unique.insert(write.relative_path.clone()) {
            return Err(CdfError::contract(format!(
                "project file transaction repeats path {}",
                write.relative_path.display()
            )));
        }
    }

    let _guard = acquire_lock_file_mutation_guard(project_root.join("cdf.lock"))?;
    let mut created_directories = Vec::new();
    let result = publish_under_guard(
        project_root,
        writes,
        fail_after_install_count,
        &mut created_directories,
        before_install,
        after_absent_install,
    );
    if result.is_err() {
        remove_empty_directories(&created_directories);
    }
    result
}

fn publish_under_guard(
    project_root: &Path,
    writes: Vec<ProjectFileWrite>,
    fail_after_install_count: Option<usize>,
    created_directories: &mut Vec<PathBuf>,
    before_install: &mut dyn FnMut(&Path) -> Result<()>,
    after_absent_install: &mut dyn FnMut(&Path, &Path) -> Result<()>,
) -> Result<ProjectFileTransactionReport> {
    let states = writes
        .iter()
        .map(|write| {
            let target = project_root.join(&write.relative_path);
            let parent = target.parent().ok_or_else(|| {
                CdfError::contract(format!(
                    "project transaction target {} has no parent",
                    target.display()
                ))
            })?;
            ensure_safe_parent(project_root, parent, created_directories)?;
            read_and_validate_prior(project_root, write)
        })
        .collect::<Result<Vec<_>>>()?;
    let mut prepared = Vec::with_capacity(writes.len());
    let mut unchanged_paths = Vec::new();
    let preparation = (|| {
        for (write, state) in writes.into_iter().zip(states) {
            let target = project_root.join(&write.relative_path);
            if state.matches_bytes(&write.bytes) {
                let parent = target.parent().ok_or_else(|| {
                    CdfError::contract(format!(
                        "project transaction target {} has no parent",
                        target.display()
                    ))
                })?;
                ensure_safe_parent(project_root, parent, created_directories)?;
                revalidate_prior(&target, &state)?;
                unchanged_paths.push(write.relative_path);
                prepared.push(PreparedWrite::Unchanged);
                continue;
            }
            let parent = target.parent().ok_or_else(|| {
                CdfError::contract(format!(
                    "project transaction target {} has no parent",
                    target.display()
                ))
            })?;
            ensure_safe_parent(project_root, parent, created_directories)?;
            let temporary = temporary_path(&target)?;
            write_synced_file(
                &temporary,
                &write.bytes,
                write.owner_only,
                state.permissions(),
            )?;
            prepared.push(PreparedWrite::Install {
                relative_path: write.relative_path,
                target,
                temporary,
                prior: state,
            });
        }
        Ok::<(), CdfError>(())
    })();
    if let Err(error) = preparation {
        cleanup_temporaries(&prepared);
        return Err(error);
    }

    let mut installed = Vec::new();
    let install_result = (|| {
        let mut install_count = 0_usize;
        for entry in &mut prepared {
            let PreparedWrite::Install {
                relative_path,
                target,
                temporary,
                prior,
            } = entry
            else {
                continue;
            };
            revalidate_prior(target, prior)?;
            before_install(target)?;
            revalidate_prior(target, prior)?;
            match prior {
                PriorFile::Absent => {
                    fs::hard_link(&*temporary, &*target).map_err(|error| {
                        if concurrent_project_file_error(&error) {
                            concurrent_project_file_change(target, error)
                        } else {
                            project_file_host_error("atomically create project file", target, error)
                        }
                    })?;
                    installed.push(relative_path.clone());
                    after_absent_install(temporary, target)?;
                    fs::remove_file(&*temporary).map_err(|error| {
                        project_file_private_path_error(
                            "remove project transaction temporary after atomic create",
                            temporary,
                            error,
                        )
                    })?;
                }
                PriorFile::Existing { .. } => {
                    fs::rename(&*temporary, &*target).map_err(|error| {
                        if concurrent_project_file_error(&error) {
                            concurrent_project_file_change(target, error)
                        } else {
                            project_file_host_error(
                                "atomically replace project file",
                                target,
                                error,
                            )
                        }
                    })?;
                    installed.push(relative_path.clone());
                }
            }
            install_count = install_count.saturating_add(1);
            if fail_after_install_count == Some(install_count) {
                return Err(CdfError::internal(format!(
                    "injected project file transaction failure after {install_count} install(s)"
                )));
            }
        }
        sync_installed_parent_directories(project_root, &installed)?;
        sync_created_directory_parents(created_directories, |parent, error| {
            project_file_host_error(
                "sync parent of newly created project transaction directory",
                parent,
                error,
            )
        })
    })();

    if let Err(error) = install_result {
        let rollback = rollback_installed(project_root, &prepared, &installed);
        cleanup_temporaries(&prepared);
        if let Err(rollback_error) = rollback {
            return Err(with_rollback_failure(error, rollback_error));
        }
        return Err(error);
    }
    cleanup_temporaries(&prepared);
    Ok(ProjectFileTransactionReport {
        installed_paths: installed,
        unchanged_paths,
    })
}

enum PreparedWrite {
    Unchanged,
    Install {
        relative_path: PathBuf,
        target: PathBuf,
        temporary: PathBuf,
        prior: PriorFile,
    },
}

enum PriorFile {
    Absent,
    Existing {
        bytes: Vec<u8>,
        permissions: Permissions,
    },
}

impl PriorFile {
    fn matches_bytes(&self, bytes: &[u8]) -> bool {
        matches!(self, Self::Existing { bytes: prior, .. } if prior == bytes)
    }

    fn permissions(&self) -> Option<&Permissions> {
        match self {
            Self::Absent => None,
            Self::Existing { permissions, .. } => Some(permissions),
        }
    }
}

fn read_and_validate_prior(project_root: &Path, write: &ProjectFileWrite) -> Result<PriorFile> {
    let path = project_root.join(&write.relative_path);
    let prior = match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(CdfError::contract(format!(
                "project file transaction refuses symlink target {}",
                path.display()
            )));
        }
        Ok(metadata) if metadata.is_file() => PriorFile::Existing {
            bytes: fs::read(&path).map_err(|error| project_prior_read_error(&path, error))?,
            permissions: metadata.permissions(),
        },
        Ok(_) => {
            return Err(CdfError::contract(format!(
                "project file transaction target {} is not a regular file",
                path.display()
            )));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => PriorFile::Absent,
        Err(error) if concurrent_project_file_error(&error) => {
            return Err(concurrent_project_file_change(&path, error));
        }
        Err(error) => {
            return Err(project_file_host_error(
                "inspect project file",
                &path,
                error,
            ));
        }
    };
    match (&write.expectation, &prior) {
        (ProjectFileExpectation::Absent, PriorFile::Absent)
        | (ProjectFileExpectation::AbsentOrExact(_), PriorFile::Absent) => Ok(prior),
        (ProjectFileExpectation::Exact(expected), PriorFile::Existing { bytes, .. })
        | (ProjectFileExpectation::AbsentOrExact(expected), PriorFile::Existing { bytes, .. })
            if expected == bytes =>
        {
            Ok(prior)
        }
        _ => Err(CdfError::contract(format!(
            "project file transaction refused because prior authority changed for {}",
            write.relative_path.display()
        ))),
    }
}

fn revalidate_prior(path: &Path, prior: &PriorFile) -> Result<()> {
    match (prior, fs::read(path)) {
        (PriorFile::Absent, Err(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        (PriorFile::Existing { bytes, .. }, Ok(current)) if *bytes == current => Ok(()),
        (_, Ok(_)) => Err(CdfError::contract(format!(
            "project file transaction refused a concurrent change to {}",
            path.display()
        ))),
        (_, Err(error)) if concurrent_project_file_error(&error) => {
            Err(concurrent_project_file_change(path, error))
        }
        (PriorFile::Absent, Err(error)) => Err(project_file_host_error(
            "re-inspect absent project file before publication",
            path,
            error,
        )),
        (PriorFile::Existing { .. }, Err(error)) => Err(project_file_host_error(
            "re-read project file before publication",
            path,
            error,
        )),
    }
}

fn concurrent_project_file_error(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::NotFound
            | std::io::ErrorKind::NotADirectory
            | std::io::ErrorKind::IsADirectory
            | std::io::ErrorKind::AlreadyExists
    ) || cdf_kernel::is_filesystem_loop(error)
}

fn concurrent_project_file_change(path: &Path, error: std::io::Error) -> CdfError {
    CdfError::contract(format!(
        "project file transaction refused a concurrent change to {}: {error}",
        path.display()
    ))
}

fn rollback_installed(
    project_root: &Path,
    prepared: &[PreparedWrite],
    installed: &[PathBuf],
) -> Result<()> {
    for relative_path in installed.iter().rev() {
        let entry = prepared.iter().find(|entry| {
            matches!(entry, PreparedWrite::Install { relative_path: candidate, .. } if candidate == relative_path)
        });
        let Some(PreparedWrite::Install { target, prior, .. }) = entry else {
            return Err(CdfError::internal(
                "project transaction rollback lost an installed path",
            ));
        };
        match prior {
            PriorFile::Absent => fs::remove_file(target).map_err(|error| {
                project_file_host_error("rollback newly created project file", target, error)
            })?,
            PriorFile::Existing { bytes, permissions } => {
                let temporary = temporary_path(target)?;
                write_synced_file(&temporary, bytes, false, Some(permissions))?;
                fs::rename(&temporary, target).map_err(|error| {
                    project_file_host_error("rollback replaced project file", target, error)
                })?;
            }
        }
    }
    sync_installed_parent_directories(project_root, installed)
}

fn validate_relative_path(path: &Path) -> Result<()> {
    if path.as_os_str().is_empty() || path.is_absolute() {
        return Err(CdfError::contract(
            "project file transaction paths must be nonempty and relative",
        ));
    }
    if path.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return Err(CdfError::contract(format!(
            "project file transaction path {} escapes the project root",
            path.display()
        )));
    }
    Ok(())
}

fn ensure_safe_parent(
    project_root: &Path,
    parent: &Path,
    created_directories: &mut Vec<PathBuf>,
) -> Result<()> {
    let relative = parent.strip_prefix(project_root).map_err(|_| {
        CdfError::contract(format!(
            "project transaction parent {} escapes project root {}",
            parent.display(),
            project_root.display()
        ))
    })?;
    let mut current = project_root.to_path_buf();
    for component in relative.components() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(CdfError::contract(format!(
                    "project transaction parent {} is not a real directory",
                    current.display()
                )));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match fs::create_dir(&current) {
                    Ok(()) => created_directories.push(current.clone()),
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        revalidate_concurrent_parent_directory(&current)?;
                    }
                    Err(error) => {
                        return Err(if concurrent_project_file_error(&error) {
                            concurrent_project_file_change(&current, error)
                        } else {
                            project_file_host_error(
                                "create project transaction directory",
                                &current,
                                error,
                            )
                        });
                    }
                }
            }
            Err(error) => {
                return Err(if concurrent_project_file_error(&error) {
                    concurrent_project_file_change(&current, error)
                } else {
                    project_file_host_error(
                        "inspect project transaction directory",
                        &current,
                        error,
                    )
                });
            }
        }
    }
    Ok(())
}

fn revalidate_concurrent_parent_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => Ok(()),
        Ok(_) => Err(CdfError::contract(format!(
            "project transaction parent {} changed to a non-directory or symlink during creation",
            path.display()
        ))),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound
                    | std::io::ErrorKind::NotADirectory
                    | std::io::ErrorKind::IsADirectory
                    | std::io::ErrorKind::AlreadyExists
            ) || cdf_kernel::is_filesystem_loop(&error) =>
        {
            Err(CdfError::contract(format!(
                "project transaction parent {} changed filesystem shape during creation: {error}",
                path.display()
            )))
        }
        Err(error) => Err(project_file_host_error(
            "revalidate concurrently created project transaction directory",
            path,
            error,
        )),
    }
}

fn temporary_path(target: &Path) -> Result<PathBuf> {
    let parent = target.parent().ok_or_else(|| {
        CdfError::contract(format!(
            "project transaction target {} has no parent",
            target.display()
        ))
    })?;
    let name = target
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            CdfError::contract(format!(
                "project transaction target {} has no UTF-8 filename",
                target.display()
            ))
        })?;
    let sequence = TRANSACTION_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    Ok(parent.join(format!(
        ".{name}.{}.{}.project-txn.tmp",
        process::id(),
        sequence
    )))
}

fn write_synced_file(
    path: &Path,
    bytes: &[u8],
    owner_only: bool,
    permissions: Option<&Permissions>,
) -> Result<()> {
    let result = (|| {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        configure_create_permissions(&mut options, owner_only, permissions);
        let mut file = options.open(path).map_err(|error| {
            project_file_private_path_error("create project transaction temporary", path, error)
        })?;
        file.write_all(bytes).map_err(|error| {
            project_file_host_error("write project transaction temporary", path, error)
        })?;
        let desired_permissions = permissions
            .cloned()
            .or_else(|| owner_permissions(owner_only));
        if let Some(permissions) = desired_permissions {
            file.set_permissions(permissions).map_err(|error| {
                project_file_host_error("set project transaction permissions on", path, error)
            })?;
        }
        file.sync_all().map_err(|error| {
            project_file_host_error("sync project transaction temporary", path, error)
        })
    })();
    if result.is_err() {
        let _ = fs::remove_file(path);
    }
    result
}

#[cfg(unix)]
fn configure_create_permissions(
    options: &mut OpenOptions,
    owner_only: bool,
    permissions: Option<&Permissions>,
) {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    let mode = permissions
        .map(PermissionsExt::mode)
        .unwrap_or(if owner_only { 0o600 } else { 0o666 });
    options.mode(mode);
}

#[cfg(not(unix))]
fn configure_create_permissions(
    _options: &mut OpenOptions,
    _owner_only: bool,
    _permissions: Option<&Permissions>,
) {
}

#[cfg(unix)]
fn owner_permissions(owner_only: bool) -> Option<Permissions> {
    use std::os::unix::fs::PermissionsExt;

    owner_only.then(|| Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn owner_permissions(owner_only: bool) -> Option<Permissions> {
    let _ = owner_only;
    None
}

fn cleanup_temporaries(prepared: &[PreparedWrite]) {
    for entry in prepared {
        if let PreparedWrite::Install { temporary, .. } = entry {
            let _ = fs::remove_file(temporary);
        }
    }
}

fn remove_empty_directories(created_directories: &[PathBuf]) {
    for directory in created_directories.iter().rev() {
        let _ = fs::remove_dir(directory);
    }
}

#[cfg(unix)]
fn sync_installed_parent_directories(project_root: &Path, installed: &[PathBuf]) -> Result<()> {
    let parents = installed
        .iter()
        .filter_map(|path| project_root.join(path).parent().map(Path::to_path_buf))
        .collect::<BTreeSet<_>>();
    for parent in parents {
        File::open(&parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| {
                project_file_host_error("sync project transaction directory", &parent, error)
            })?;
    }
    Ok(())
}

#[cfg(unix)]
pub(crate) fn sync_created_directory_parents(
    created_directories: &[PathBuf],
    mut classify: impl FnMut(&Path, std::io::Error) -> CdfError,
) -> Result<()> {
    for parent in created_directory_parent_sync_order(created_directories) {
        File::open(&parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| classify(&parent, error))?;
    }
    Ok(())
}

#[cfg(not(unix))]
pub(crate) fn sync_created_directory_parents(
    _created_directories: &[PathBuf],
    _classify: impl FnMut(&Path, std::io::Error) -> CdfError,
) -> Result<()> {
    Ok(())
}

fn created_directory_parent_sync_order(created_directories: &[PathBuf]) -> Vec<PathBuf> {
    let mut parents = Vec::new();
    for directory in created_directories.iter().rev() {
        let Some(parent) = directory.parent().map(Path::to_path_buf) else {
            continue;
        };
        if !parents.contains(&parent) {
            parents.push(parent);
        }
    }
    parents
}

#[cfg(unix)]
pub(crate) fn sync_directory_ancestry_through_root(
    leaf_directory: &Path,
    root: &Path,
    mut classify: impl FnMut(&Path, std::io::Error) -> CdfError,
) -> Result<()> {
    for directory in directory_ancestry_sync_order(leaf_directory, root)? {
        File::open(&directory)
            .and_then(|handle| handle.sync_all())
            .map_err(|error| classify(&directory, error))?;
    }
    Ok(())
}

#[cfg(not(unix))]
pub(crate) fn sync_directory_ancestry_through_root(
    _leaf_directory: &Path,
    _root: &Path,
    _classify: impl FnMut(&Path, std::io::Error) -> CdfError,
) -> Result<()> {
    Ok(())
}

fn directory_ancestry_sync_order(leaf_directory: &Path, root: &Path) -> Result<Vec<PathBuf>> {
    let mut directories = Vec::new();
    let mut current = leaf_directory;
    loop {
        directories.push(current.to_path_buf());
        if current == root {
            return Ok(directories);
        }
        current = current.parent().ok_or_else(|| {
            CdfError::internal(format!(
                "directory {} is not beneath durability root {}",
                leaf_directory.display(),
                root.display()
            ))
        })?;
    }
}

fn project_prior_read_error(path: &Path, error: std::io::Error) -> CdfError {
    if concurrent_project_file_error(&error) {
        concurrent_project_file_change(path, error)
    } else {
        project_file_host_error("read project file", path, error)
    }
}

fn project_file_host_error(action: &str, path: &Path, error: std::io::Error) -> CdfError {
    CdfError::environment(format!(
        "{action} {}: {error}; check project-path permissions, free space, device availability, and process file limits before retrying",
        path.display()
    ))
}

fn project_file_private_path_error(action: &str, path: &Path, error: std::io::Error) -> CdfError {
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
            "{action} at CDF-managed scratch path {} with invalid filesystem shape: {error}",
            path.display()
        ))
    } else {
        project_file_host_error(action, path, error)
    }
}

fn with_rollback_failure(primary: CdfError, rollback: CdfError) -> CdfError {
    CdfError::internal(format!(
        "project file transaction entered an unexpected partial-mutation state: primary failure kind {:?}, retry_after_ms {:?} ({}) and rollback also failed ({rollback})",
        primary.kind, primary.retry_after_ms, primary.message
    ))
}

#[cfg(not(unix))]
fn sync_installed_parent_directories(_project_root: &Path, _installed: &[PathBuf]) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_rolls_back_every_prior_install_on_failure() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("cdf.toml"), b"before-project").unwrap();
        let writes = vec![
            ProjectFileWrite::new(
                "resources/events.toml",
                b"resource".to_vec(),
                ProjectFileExpectation::Absent,
            ),
            ProjectFileWrite::new(
                "cdf.toml",
                b"after-project".to_vec(),
                ProjectFileExpectation::Exact(b"before-project".to_vec()),
            ),
            ProjectFileWrite::new(
                "cdf.lock",
                b"commit".to_vec(),
                ProjectFileExpectation::Absent,
            ),
        ];

        let error =
            publish_project_files_inner(root.path(), Path::new("cdf.lock"), writes, Some(2))
                .unwrap_err();

        assert!(error.message.contains("injected"));
        assert_eq!(
            fs::read(root.path().join("cdf.toml")).unwrap(),
            b"before-project"
        );
        assert!(!root.path().join("resources/events.toml").exists());
        assert!(!root.path().join("resources").exists());
        assert!(!root.path().join("cdf.lock").exists());
    }

    #[test]
    fn transaction_installs_content_before_the_final_commit_point() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("cdf.toml"), b"before-project").unwrap();
        let writes = vec![
            ProjectFileWrite::new(
                ".cdf/schemas/events.json",
                b"schema".to_vec(),
                ProjectFileExpectation::AbsentOrExact(b"schema".to_vec()),
            ),
            ProjectFileWrite::new(
                "cdf.toml",
                b"after-project".to_vec(),
                ProjectFileExpectation::Exact(b"before-project".to_vec()),
            ),
            ProjectFileWrite::new(
                "cdf.lock",
                b"commit".to_vec(),
                ProjectFileExpectation::Absent,
            ),
        ];

        let report =
            publish_project_files_transactionally(root.path(), "cdf.lock", writes).unwrap();

        assert_eq!(
            report.installed_paths.last().unwrap(),
            Path::new("cdf.lock")
        );
        assert_eq!(
            fs::read(root.path().join("cdf.toml")).unwrap(),
            b"after-project"
        );
        assert_eq!(fs::read(root.path().join("cdf.lock")).unwrap(), b"commit");
        assert_eq!(
            fs::read(root.path().join(".cdf/schemas/events.json")).unwrap(),
            b"schema"
        );
    }

    #[cfg(unix)]
    #[test]
    fn owner_only_write_is_private_from_initial_creation() {
        use std::os::unix::fs::PermissionsExt;

        let root = tempfile::tempdir().unwrap();
        let writes = vec![
            ProjectFileWrite::new(
                ".cdf/secrets/sources/events.token",
                b"secret".to_vec(),
                ProjectFileExpectation::Absent,
            )
            .owner_only(),
            ProjectFileWrite::new(
                "cdf.lock",
                b"commit".to_vec(),
                ProjectFileExpectation::Absent,
            ),
        ];

        publish_project_files_transactionally(root.path(), "cdf.lock", writes).unwrap();

        let mode = fs::metadata(root.path().join(".cdf/secrets/sources/events.token"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn rollback_failure_is_an_internal_partial_mutation_invariant() {
        let primary = CdfError::rate_limited("primary destination failure", Some(250));
        let rollback = CdfError::environment("rollback host failure");

        let combined = with_rollback_failure(primary, rollback);

        assert_eq!(combined.kind, cdf_kernel::ErrorKind::Internal);
        assert_eq!(combined.retry_after_ms, None);
        assert!(combined.message.contains("RateLimited"));
        assert!(combined.message.contains("Some(250)"));
        assert!(combined.message.contains("primary destination failure"));
        assert!(combined.message.contains("rollback host failure"));
    }

    #[test]
    fn created_directory_parent_sync_order_is_child_to_project_root() {
        let root = Path::new("/project");
        let created = vec![
            root.join(".cdf"),
            root.join(".cdf/secrets"),
            root.join(".cdf/secrets/sources"),
        ];

        assert_eq!(
            created_directory_parent_sync_order(&created),
            vec![
                root.join(".cdf/secrets"),
                root.join(".cdf"),
                root.to_path_buf(),
            ]
        );
    }

    #[test]
    fn full_directory_ancestry_sync_order_retries_leaf_through_root() {
        let root = Path::new("/project");
        let leaf = root.join(".cdf/promotions/promotion/targets");

        assert_eq!(
            directory_ancestry_sync_order(&leaf, root).unwrap(),
            vec![
                root.join(".cdf/promotions/promotion/targets"),
                root.join(".cdf/promotions/promotion"),
                root.join(".cdf/promotions"),
                root.join(".cdf"),
                root.to_path_buf(),
            ]
        );
    }

    #[test]
    fn project_transaction_host_failure_has_environment_remediation() {
        let error = project_file_host_error(
            "write project file",
            Path::new("cdf.toml"),
            std::io::Error::from(std::io::ErrorKind::PermissionDenied),
        );

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Environment);
        assert!(error.message.contains("project-path permissions"));
    }

    #[test]
    fn project_transaction_creation_race_is_contract_and_preserves_the_racer() {
        let root = tempfile::tempdir().unwrap();
        let writes = vec![ProjectFileWrite::new(
            "cdf.lock",
            b"ours".to_vec(),
            ProjectFileExpectation::Absent,
        )];
        let target = root.path().join("cdf.lock");
        let mut raced = false;

        let error = publish_project_files_inner_with_hook(
            root.path(),
            Path::new("cdf.lock"),
            writes,
            None,
            &mut |path| {
                if !raced {
                    fs::write(path, b"racer").unwrap();
                    raced = true;
                }
                Ok(())
            },
            &mut |_, _| Ok(()),
        )
        .unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
        assert_eq!(fs::read(target).unwrap(), b"racer");
    }

    #[test]
    fn project_transaction_replacement_race_is_contract_and_preserves_the_racer() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("cdf.lock");
        fs::write(&target, b"observed").unwrap();
        let writes = vec![ProjectFileWrite::new(
            "cdf.lock",
            b"ours".to_vec(),
            ProjectFileExpectation::Exact(b"observed".to_vec()),
        )];

        let error = publish_project_files_inner_with_hook(
            root.path(),
            Path::new("cdf.lock"),
            writes,
            None,
            &mut |path| {
                fs::write(path, b"racer").unwrap();
                Ok(())
            },
            &mut |_, _| Ok(()),
        )
        .unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
        assert_eq!(fs::read(target).unwrap(), b"racer");
    }

    #[test]
    fn absent_install_cleanup_failure_rolls_back_the_published_target() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("cdf.lock");
        let writes = vec![ProjectFileWrite::new(
            "cdf.lock",
            b"commit".to_vec(),
            ProjectFileExpectation::Absent,
        )];

        let error = publish_project_files_inner_with_hook(
            root.path(),
            Path::new("cdf.lock"),
            writes,
            None,
            &mut |_| Ok(()),
            &mut |temporary, _target| {
                fs::remove_file(temporary).unwrap();
                Ok(())
            },
        )
        .unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Internal);
        assert!(error.message.contains("CDF-managed scratch path"));
        assert!(
            !target.exists(),
            "failed transaction must roll back the already-published commit point"
        );
    }

    #[test]
    fn project_transaction_revalidation_shape_change_is_contract_owned() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("cdf.toml");
        fs::write(&path, b"observed").unwrap();
        let prior = PriorFile::Existing {
            bytes: b"observed".to_vec(),
            permissions: fs::metadata(&path).unwrap().permissions(),
        };
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();

        let error = revalidate_prior(&path, &prior).unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
        assert!(error.message.contains("concurrent change"));
    }

    #[test]
    fn concurrent_parent_directory_revalidation_accepts_only_real_directories() {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("directory");
        fs::create_dir(&directory).unwrap();
        revalidate_concurrent_parent_directory(&directory).unwrap();

        let file = root.path().join("file");
        fs::write(&file, b"not a directory").unwrap();
        let error = revalidate_concurrent_parent_directory(&file).unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
        assert!(error.message.contains("non-directory"));
    }

    #[cfg(unix)]
    #[test]
    fn unchanged_project_file_rejects_symlinked_parent() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        fs::write(outside.path().join("config.toml"), b"same").unwrap();
        symlink(outside.path(), root.path().join("linked")).unwrap();
        let writes = vec![ProjectFileWrite::new(
            "linked/config.toml",
            b"same".to_vec(),
            ProjectFileExpectation::Exact(b"same".to_vec()),
        )];

        let error =
            publish_project_files_transactionally(root.path(), "linked/config.toml", writes)
                .unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
        assert_eq!(
            fs::read(outside.path().join("config.toml")).unwrap(),
            b"same"
        );
    }
}
