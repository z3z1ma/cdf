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

    let commit_path = project_root.join(commit_relative_path);
    let _guard = acquire_lock_file_mutation_guard(&commit_path)?;
    let mut created_directories = Vec::new();
    let result = publish_under_guard(
        project_root,
        writes,
        fail_after_install_count,
        &mut created_directories,
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
) -> Result<ProjectFileTransactionReport> {
    let states = writes
        .iter()
        .map(|write| read_and_validate_prior(project_root, write))
        .collect::<Result<Vec<_>>>()?;
    let mut prepared = Vec::with_capacity(writes.len());
    let mut unchanged_paths = Vec::new();
    let preparation = (|| {
        for (write, state) in writes.into_iter().zip(states) {
            let target = project_root.join(&write.relative_path);
            if state.matches_bytes(&write.bytes) {
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
            match prior {
                PriorFile::Absent => {
                    fs::hard_link(&*temporary, &*target).map_err(|error| {
                        CdfError::data(format!(
                            "atomically create project file {}: {error}",
                            target.display()
                        ))
                    })?;
                    if let Err(error) = fs::remove_file(&*temporary) {
                        let cleanup = fs::remove_file(&*target);
                        return Err(CdfError::data(format!(
                            "remove project transaction temporary {} after atomic create: {error}; target cleanup status: {}",
                            temporary.display(),
                            cleanup
                                .map(|()| "removed".to_owned())
                                .unwrap_or_else(|cleanup_error| cleanup_error.to_string())
                        )));
                    }
                }
                PriorFile::Existing { .. } => {
                    fs::rename(&*temporary, &*target).map_err(|error| {
                        CdfError::data(format!(
                            "atomically replace project file {}: {error}",
                            target.display()
                        ))
                    })?;
                }
            }
            installed.push(relative_path.clone());
            install_count = install_count.saturating_add(1);
            if fail_after_install_count == Some(install_count) {
                return Err(CdfError::internal(format!(
                    "injected project file transaction failure after {install_count} install(s)"
                )));
            }
        }
        sync_installed_parent_directories(project_root, &installed)
    })();

    if let Err(error) = install_result {
        let rollback = rollback_installed(project_root, &prepared, &installed);
        cleanup_temporaries(&prepared);
        if let Err(rollback_error) = rollback {
            return Err(CdfError::internal(format!(
                "project file transaction failed ({error}) and rollback also failed ({rollback_error})"
            )));
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
            bytes: fs::read(&path).map_err(|error| {
                CdfError::data(format!("read project file {}: {error}", path.display()))
            })?,
            permissions: metadata.permissions(),
        },
        Ok(_) => {
            return Err(CdfError::contract(format!(
                "project file transaction target {} is not a regular file",
                path.display()
            )));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => PriorFile::Absent,
        Err(error) => {
            return Err(CdfError::data(format!(
                "inspect project file {}: {error}",
                path.display()
            )));
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
        (_, Ok(_)) | (PriorFile::Absent, Err(_)) => Err(CdfError::contract(format!(
            "project file transaction refused a concurrent change to {}",
            path.display()
        ))),
        (PriorFile::Existing { .. }, Err(error)) => Err(CdfError::data(format!(
            "re-read project file {} before publication: {error}",
            path.display()
        ))),
    }
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
                CdfError::data(format!(
                    "rollback newly created project file {}: {error}",
                    target.display()
                ))
            })?,
            PriorFile::Existing { bytes, permissions } => {
                let temporary = temporary_path(target)?;
                write_synced_file(&temporary, bytes, false, Some(permissions))?;
                fs::rename(&temporary, target).map_err(|error| {
                    CdfError::data(format!(
                        "rollback replaced project file {}: {error}",
                        target.display()
                    ))
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
                fs::create_dir(&current).map_err(|error| {
                    CdfError::data(format!(
                        "create project transaction directory {}: {error}",
                        current.display()
                    ))
                })?;
                created_directories.push(current.clone());
            }
            Err(error) => {
                return Err(CdfError::data(format!(
                    "inspect project transaction directory {}: {error}",
                    current.display()
                )));
            }
        }
    }
    Ok(())
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
            CdfError::data(format!(
                "create project transaction temporary {}: {error}",
                path.display()
            ))
        })?;
        file.write_all(bytes).map_err(|error| {
            CdfError::data(format!(
                "write project transaction temporary {}: {error}",
                path.display()
            ))
        })?;
        let desired_permissions = permissions
            .cloned()
            .or_else(|| owner_permissions(owner_only));
        if let Some(permissions) = desired_permissions {
            file.set_permissions(permissions).map_err(|error| {
                CdfError::data(format!(
                    "set project transaction permissions on {}: {error}",
                    path.display()
                ))
            })?;
        }
        file.sync_all().map_err(|error| {
            CdfError::data(format!(
                "sync project transaction temporary {}: {error}",
                path.display()
            ))
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
                CdfError::data(format!(
                    "sync project transaction directory {}: {error}",
                    parent.display()
                ))
            })?;
    }
    Ok(())
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
}
