use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions, Permissions},
    io::Write,
    path::{Component, Path, PathBuf},
    process,
    sync::atomic::{AtomicU64, Ordering},
};

use cdf_kernel::{CdfError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::acquire_lock_file_mutation_guard;

static TRANSACTION_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const PROJECT_FILE_TRANSACTION_MARKER: &str = ".cdf/project-files.transaction.json";
const PROJECT_FILE_TRANSACTION_MARKER_VERSION: u16 = 2;
const MAX_PROJECT_FILE_TRANSACTION_MARKER_BYTES: u64 = 16 * 1024 * 1024;

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

pub struct ProjectFileGuard {
    relative_path: PathBuf,
    bytes: Vec<u8>,
}

impl ProjectFileGuard {
    pub fn exact(relative_path: impl Into<PathBuf>, bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            relative_path: relative_path.into(),
            bytes: bytes.into(),
        }
    }
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ProjectFileTransactionMarker {
    version: u16,
    generation: u64,
    #[serde(flatten)]
    state: ProjectFileTransactionState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum ProjectFileTransactionState {
    Committed,
    Pending {
        commit_relative_path: PathBuf,
        guards: Vec<ProjectFileTransactionGuard>,
        entries: Vec<ProjectFileTransactionEntry>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ProjectFileTransactionGuard {
    relative_path: PathBuf,
    len: u64,
    sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ProjectFileTransactionEntry {
    relative_path: PathBuf,
    temporary_relative_path: PathBuf,
    prior: ProjectFileTransactionPrior,
    new_len: u64,
    new_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ProjectFileTransactionPrior {
    Absent,
    Existing { len: u64, sha256: String },
}

#[derive(Clone, Debug)]
struct ProjectFileTransactionMarkerAuthority {
    bytes: Option<Vec<u8>>,
    marker: Option<ProjectFileTransactionMarker>,
}

struct ProjectFileTransactionHooks<'a> {
    before_install: &'a mut dyn FnMut(&Path) -> Result<()>,
    after_absent_install: &'a mut dyn FnMut(&Path, &Path) -> Result<()>,
    after_install: &'a mut dyn FnMut(&Path) -> Result<()>,
    fail_after_install_count: Option<usize>,
}

pub fn recover_project_file_transaction(project_root: impl AsRef<Path>) -> Result<u64> {
    let project_root = project_root.as_ref();
    let observed = read_project_file_transaction_marker(project_root)?;
    if !observed.is_pending() {
        return Ok(observed.generation());
    }

    let _guard = acquire_lock_file_mutation_guard(project_root.join("cdf.lock"))?;
    Ok(recover_project_file_transaction_under_guard(
        project_root,
        read_project_file_transaction_marker(project_root)?,
    )?
    .generation())
}

pub fn project_file_transaction_generation(project_root: impl AsRef<Path>) -> Result<u64> {
    let project_root = project_root.as_ref();
    let observed = read_project_file_transaction_marker(project_root)?;
    if observed.is_pending() {
        return Err(CdfError::contract(format!(
            "project publication is incomplete at {}; retry the interrupted cdf add without --dry-run before using this project",
            project_root.join(PROJECT_FILE_TRANSACTION_MARKER).display()
        )));
    }
    Ok(observed.generation())
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

pub fn publish_project_files_transactionally_guarded(
    project_root: impl AsRef<Path>,
    commit_relative_path: impl AsRef<Path>,
    guards: Vec<ProjectFileGuard>,
    writes: Vec<ProjectFileWrite>,
) -> Result<ProjectFileTransactionReport> {
    publish_project_files_inner_with_guards(
        project_root.as_ref(),
        commit_relative_path.as_ref(),
        guards,
        writes,
        None,
    )
}

pub fn publish_project_files_transactionally_without_recovery(
    project_root: impl AsRef<Path>,
    commit_relative_path: impl AsRef<Path>,
    writes: Vec<ProjectFileWrite>,
) -> Result<ProjectFileTransactionReport> {
    publish_project_files_inner_with_policy(
        project_root.as_ref(),
        commit_relative_path.as_ref(),
        Vec::new(),
        writes,
        None,
        PendingTransactionPolicy::FailClosed,
    )
}

pub fn publish_project_files_transactionally_guarded_without_recovery(
    project_root: impl AsRef<Path>,
    commit_relative_path: impl AsRef<Path>,
    guards: Vec<ProjectFileGuard>,
    writes: Vec<ProjectFileWrite>,
) -> Result<ProjectFileTransactionReport> {
    publish_project_files_inner_with_policy(
        project_root.as_ref(),
        commit_relative_path.as_ref(),
        guards,
        writes,
        None,
        PendingTransactionPolicy::FailClosed,
    )
}

#[derive(Clone, Copy)]
enum PendingTransactionPolicy {
    Recover,
    FailClosed,
}

fn publish_project_files_inner(
    project_root: &Path,
    commit_relative_path: &Path,
    writes: Vec<ProjectFileWrite>,
    fail_after_install_count: Option<usize>,
) -> Result<ProjectFileTransactionReport> {
    publish_project_files_inner_with_guards(
        project_root,
        commit_relative_path,
        Vec::new(),
        writes,
        fail_after_install_count,
    )
}

fn publish_project_files_inner_with_guards(
    project_root: &Path,
    commit_relative_path: &Path,
    guards: Vec<ProjectFileGuard>,
    writes: Vec<ProjectFileWrite>,
    fail_after_install_count: Option<usize>,
) -> Result<ProjectFileTransactionReport> {
    publish_project_files_inner_with_policy(
        project_root,
        commit_relative_path,
        guards,
        writes,
        fail_after_install_count,
        PendingTransactionPolicy::Recover,
    )
}

fn publish_project_files_inner_with_policy(
    project_root: &Path,
    commit_relative_path: &Path,
    guards: Vec<ProjectFileGuard>,
    writes: Vec<ProjectFileWrite>,
    fail_after_install_count: Option<usize>,
    pending_policy: PendingTransactionPolicy,
) -> Result<ProjectFileTransactionReport> {
    let mut hooks = ProjectFileTransactionHooks {
        before_install: &mut |_| Ok(()),
        after_absent_install: &mut |_, _| Ok(()),
        after_install: &mut |_| Ok(()),
        fail_after_install_count,
    };
    publish_project_files_inner_with_hook_and_policy(
        project_root,
        commit_relative_path,
        guards,
        writes,
        pending_policy,
        &mut hooks,
    )
}

#[cfg(test)]
fn publish_project_files_inner_with_hook(
    project_root: &Path,
    commit_relative_path: &Path,
    writes: Vec<ProjectFileWrite>,
    fail_after_install_count: Option<usize>,
    before_install: &mut dyn FnMut(&Path) -> Result<()>,
    after_absent_install: &mut dyn FnMut(&Path, &Path) -> Result<()>,
    after_install: &mut dyn FnMut(&Path) -> Result<()>,
) -> Result<ProjectFileTransactionReport> {
    let mut hooks = ProjectFileTransactionHooks {
        before_install,
        after_absent_install,
        after_install,
        fail_after_install_count,
    };
    publish_project_files_inner_with_hook_and_policy(
        project_root,
        commit_relative_path,
        Vec::new(),
        writes,
        PendingTransactionPolicy::Recover,
        &mut hooks,
    )
}

#[cfg(test)]
fn publish_project_files_inner_with_guards_and_hook(
    project_root: &Path,
    commit_relative_path: &Path,
    guards: Vec<ProjectFileGuard>,
    writes: Vec<ProjectFileWrite>,
    after_install: &mut dyn FnMut(&Path) -> Result<()>,
) -> Result<ProjectFileTransactionReport> {
    let mut hooks = ProjectFileTransactionHooks {
        before_install: &mut |_| Ok(()),
        after_absent_install: &mut |_, _| Ok(()),
        after_install,
        fail_after_install_count: None,
    };
    publish_project_files_inner_with_hook_and_policy(
        project_root,
        commit_relative_path,
        guards,
        writes,
        PendingTransactionPolicy::Recover,
        &mut hooks,
    )
}

fn publish_project_files_inner_with_hook_and_policy(
    project_root: &Path,
    commit_relative_path: &Path,
    guards: Vec<ProjectFileGuard>,
    writes: Vec<ProjectFileWrite>,
    pending_policy: PendingTransactionPolicy,
    hooks: &mut ProjectFileTransactionHooks<'_>,
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
    for guard in &guards {
        validate_relative_path(&guard.relative_path)?;
        if !unique.insert(guard.relative_path.clone()) {
            return Err(CdfError::contract(format!(
                "project file transaction repeats path {}",
                guard.relative_path.display()
            )));
        }
    }
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
    let observed_marker = read_project_file_transaction_marker(project_root)?;
    let marker_authority = match pending_policy {
        PendingTransactionPolicy::Recover => {
            recover_project_file_transaction_under_guard(project_root, observed_marker)?
        }
        PendingTransactionPolicy::FailClosed if observed_marker.is_pending() => {
            return Err(CdfError::contract(format!(
                "project publication is incomplete at {}; run `cdf compile --refresh` to recover it before offline compilation",
                project_root.join(PROJECT_FILE_TRANSACTION_MARKER).display()
            )));
        }
        PendingTransactionPolicy::FailClosed => observed_marker,
    };
    let mut created_directories = Vec::new();
    let result = publish_under_guard(
        project_root,
        commit_relative_path,
        guards,
        writes,
        &marker_authority,
        &mut created_directories,
        hooks,
    );
    if result.is_err() {
        remove_empty_directories(&created_directories);
    }
    result
}

fn publish_under_guard(
    project_root: &Path,
    commit_relative_path: &Path,
    guards: Vec<ProjectFileGuard>,
    writes: Vec<ProjectFileWrite>,
    marker_authority: &ProjectFileTransactionMarkerAuthority,
    created_directories: &mut Vec<PathBuf>,
    hooks: &mut ProjectFileTransactionHooks<'_>,
) -> Result<ProjectFileTransactionReport> {
    let guards = guards
        .iter()
        .map(|guard| read_and_validate_guard(project_root, guard))
        .collect::<Result<Vec<_>>>()?;
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
                prepared.push(PreparedWrite::Unchanged {
                    target,
                    prior: state,
                });
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
                new_len: u64::try_from(write.bytes.len()).map_err(|_| {
                    CdfError::internal("project transaction write length does not fit u64")
                })?,
                new_sha256: project_file_bytes_hash(&write.bytes),
            });
        }
        Ok::<(), CdfError>(())
    })();
    if let Err(error) = preparation {
        cleanup_temporaries(&prepared);
        return Err(error);
    }
    let prepared_paths = prepared
        .iter()
        .filter_map(|entry| match entry {
            PreparedWrite::Unchanged { .. } => None,
            PreparedWrite::Install { relative_path, .. } => Some(relative_path.clone()),
        })
        .collect::<Vec<_>>();
    let preparation_sync = sync_installed_parent_directories(project_root, &prepared_paths)
        .and_then(|()| {
            sync_created_directory_parents(created_directories, |parent, error| {
                project_file_host_error(
                    "sync parent of prepared project transaction directory",
                    parent,
                    error,
                )
            })
        });
    if let Err(error) = preparation_sync {
        cleanup_temporaries(&prepared);
        return Err(error);
    }

    validate_transaction_guards(project_root, &guards)?;
    for entry in &prepared {
        match entry {
            PreparedWrite::Unchanged { target, prior }
            | PreparedWrite::Install { target, prior, .. } => revalidate_prior(target, prior)?,
        }
    }
    let pending_marker = match begin_project_file_transaction(
        project_root,
        commit_relative_path,
        &guards,
        &prepared,
        marker_authority,
    ) {
        Ok(marker) => marker,
        Err(error) => {
            cleanup_temporaries(&prepared);
            return Err(error);
        }
    };
    let mut installed = Vec::new();
    let install_result = (|| {
        let mut install_count = 0_usize;
        for entry in &mut prepared {
            let PreparedWrite::Install {
                relative_path,
                target,
                temporary,
                prior,
                ..
            } = entry
            else {
                continue;
            };
            validate_transaction_guards(project_root, &guards)?;
            revalidate_prior(target, prior)?;
            (hooks.before_install)(target)?;
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
                    (hooks.after_absent_install)(temporary, target)?;
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
            (hooks.after_install)(target)?;
            validate_transaction_guards(project_root, &guards)?;
            install_count = install_count.saturating_add(1);
            if hooks.fail_after_install_count == Some(install_count) {
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

    // Once the pending marker is durable, forward recovery is the only safe terminal decision.
    // Destructive rollback could consume the prepared new bytes or overwrite a non-cooperating
    // editor, leaving the durable journal unable to converge after another process loss.
    install_result?;
    if let Some(pending_marker) = pending_marker.as_ref() {
        validate_transaction_guards(project_root, &guards)?;
        commit_project_file_transaction_marker(project_root, pending_marker)?;
    }
    cleanup_temporaries(&prepared);
    Ok(ProjectFileTransactionReport {
        installed_paths: installed,
        unchanged_paths,
    })
}

enum PreparedWrite {
    Unchanged {
        target: PathBuf,
        prior: PriorFile,
    },
    Install {
        relative_path: PathBuf,
        target: PathBuf,
        temporary: PathBuf,
        prior: PriorFile,
        new_len: u64,
        new_sha256: String,
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

impl ProjectFileTransactionMarkerAuthority {
    fn generation(&self) -> u64 {
        self.marker.as_ref().map_or(0, |marker| marker.generation)
    }

    fn is_pending(&self) -> bool {
        self.marker.as_ref().is_some_and(|marker| {
            matches!(marker.state, ProjectFileTransactionState::Pending { .. })
        })
    }
}

fn begin_project_file_transaction(
    project_root: &Path,
    commit_relative_path: &Path,
    guards: &[ProjectFileTransactionGuard],
    prepared: &[PreparedWrite],
    previous: &ProjectFileTransactionMarkerAuthority,
) -> Result<Option<ProjectFileTransactionMarkerAuthority>> {
    let entries = prepared
        .iter()
        .filter_map(|entry| match entry {
            PreparedWrite::Unchanged { .. } => None,
            PreparedWrite::Install {
                relative_path,
                temporary,
                prior,
                new_len,
                new_sha256,
                ..
            } => Some((relative_path, temporary, prior, new_len, new_sha256)),
        })
        .map(|(relative_path, temporary, prior, new_len, new_sha256)| {
            let temporary_relative_path = temporary
                .strip_prefix(project_root)
                .map_err(|_| {
                    CdfError::internal(format!(
                        "project transaction temporary {} escapes project root {}",
                        temporary.display(),
                        project_root.display()
                    ))
                })?
                .to_path_buf();
            Ok(ProjectFileTransactionEntry {
                relative_path: relative_path.clone(),
                temporary_relative_path,
                prior: match prior {
                    PriorFile::Absent => ProjectFileTransactionPrior::Absent,
                    PriorFile::Existing { bytes, .. } => ProjectFileTransactionPrior::Existing {
                        len: u64::try_from(bytes.len()).map_err(|_| {
                            CdfError::internal("project transaction prior length does not fit u64")
                        })?,
                        sha256: project_file_bytes_hash(bytes),
                    },
                },
                new_len: *new_len,
                new_sha256: new_sha256.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if entries.is_empty() {
        return Ok(None);
    }
    let generation = previous
        .generation()
        .checked_add(1)
        .ok_or_else(|| CdfError::internal("project transaction generation overflow"))?;
    let marker = ProjectFileTransactionMarker {
        version: PROJECT_FILE_TRANSACTION_MARKER_VERSION,
        generation,
        state: ProjectFileTransactionState::Pending {
            commit_relative_path: commit_relative_path.to_path_buf(),
            guards: guards.to_vec(),
            entries,
        },
    };
    validate_project_file_transaction_marker(&marker)?;
    Ok(Some(replace_project_file_transaction_marker(
        project_root,
        previous.bytes.as_deref(),
        Some(&marker),
    )?))
}

fn commit_project_file_transaction_marker(
    project_root: &Path,
    pending: &ProjectFileTransactionMarkerAuthority,
) -> Result<()> {
    let generation = pending.generation();
    let committed = ProjectFileTransactionMarker {
        version: PROJECT_FILE_TRANSACTION_MARKER_VERSION,
        generation,
        state: ProjectFileTransactionState::Committed,
    };
    replace_project_file_transaction_marker(
        project_root,
        pending.bytes.as_deref(),
        Some(&committed),
    )?;
    Ok(())
}

fn recover_project_file_transaction_under_guard(
    project_root: &Path,
    authority: ProjectFileTransactionMarkerAuthority,
) -> Result<ProjectFileTransactionMarkerAuthority> {
    let Some(marker) = authority.marker.as_ref() else {
        return Ok(authority);
    };
    let ProjectFileTransactionState::Pending {
        guards, entries, ..
    } = &marker.state
    else {
        return Ok(authority);
    };

    validate_transaction_guards(project_root, guards)?;
    for entry in entries {
        validate_transaction_guards(project_root, guards)?;
        recover_project_file_transaction_entry(project_root, entry)?;
        validate_transaction_guards(project_root, guards)?;
    }
    let paths = entries
        .iter()
        .map(|entry| entry.relative_path.clone())
        .collect::<Vec<_>>();
    sync_installed_parent_directories(project_root, &paths)?;
    validate_transaction_guards(project_root, guards)?;
    let committed = ProjectFileTransactionMarker {
        version: PROJECT_FILE_TRANSACTION_MARKER_VERSION,
        generation: marker.generation,
        state: ProjectFileTransactionState::Committed,
    };
    replace_project_file_transaction_marker(
        project_root,
        authority.bytes.as_deref(),
        Some(&committed),
    )
}

fn recover_project_file_transaction_entry(
    project_root: &Path,
    entry: &ProjectFileTransactionEntry,
) -> Result<()> {
    let target = project_root.join(&entry.relative_path);
    let temporary = project_root.join(&entry.temporary_relative_path);
    verify_existing_safe_parent(
        project_root,
        target.parent().ok_or_else(|| {
            CdfError::contract(format!(
                "project transaction recovery target {} has no parent",
                target.display()
            ))
        })?,
        false,
    )?;
    verify_existing_safe_parent(
        project_root,
        temporary.parent().ok_or_else(|| {
            CdfError::internal(format!(
                "project transaction recovery temporary {} has no parent",
                temporary.display()
            ))
        })?,
        true,
    )?;

    let current = read_recovery_file(&target, false)?;
    if file_bytes_match(current.as_deref(), entry.new_len, entry.new_sha256.as_str()) {
        cleanup_recovery_temporary(&temporary, entry)?;
        return Ok(());
    }
    if !prior_file_matches(current.as_deref(), &entry.prior) {
        return Err(CdfError::contract(format!(
            "project transaction recovery refused unrelated authority at {}; leave {} in place and restore or reconcile the project files before retrying",
            target.display(),
            project_root.join(PROJECT_FILE_TRANSACTION_MARKER).display()
        )));
    }
    let temporary_bytes = read_recovery_file(&temporary, true)?.ok_or_else(|| {
        CdfError::internal(format!(
            "project transaction recovery is missing prepared temporary {}",
            temporary.display()
        ))
    })?;
    if !file_bytes_match(
        Some(&temporary_bytes),
        entry.new_len,
        entry.new_sha256.as_str(),
    ) {
        return Err(CdfError::internal(format!(
            "project transaction recovery temporary {} does not match its journaled content",
            temporary.display()
        )));
    }

    let current = read_recovery_file(&target, false)?;
    if !prior_file_matches(current.as_deref(), &entry.prior) {
        return Err(CdfError::contract(format!(
            "project transaction recovery refused a concurrent change to {}",
            target.display()
        )));
    }
    match entry.prior {
        ProjectFileTransactionPrior::Absent => {
            fs::hard_link(&temporary, &target).map_err(|error| {
                if concurrent_project_file_error(&error) {
                    concurrent_project_file_change(&target, error)
                } else {
                    project_file_host_error(
                        "recover project transaction file creation",
                        &target,
                        error,
                    )
                }
            })?;
            fs::remove_file(&temporary).map_err(|error| {
                project_file_private_path_error(
                    "remove recovered project transaction temporary",
                    &temporary,
                    error,
                )
            })?;
        }
        ProjectFileTransactionPrior::Existing { .. } => {
            fs::rename(&temporary, &target).map_err(|error| {
                project_file_host_error(
                    "recover project transaction file replacement",
                    &target,
                    error,
                )
            })?;
        }
    }
    Ok(())
}

fn cleanup_recovery_temporary(temporary: &Path, entry: &ProjectFileTransactionEntry) -> Result<()> {
    let Some(bytes) = read_recovery_file(temporary, true)? else {
        return Ok(());
    };
    if !file_bytes_match(Some(&bytes), entry.new_len, entry.new_sha256.as_str()) {
        return Err(CdfError::internal(format!(
            "project transaction recovery found unrelated content at managed temporary {}",
            temporary.display()
        )));
    }
    fs::remove_file(temporary).map_err(|error| {
        project_file_private_path_error(
            "remove completed project transaction temporary",
            temporary,
            error,
        )
    })
}

fn read_recovery_file(path: &Path, private: bool) -> Result<Option<Vec<u8>>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            let message = format!(
                "project transaction recovery path {} is not a real regular file",
                path.display()
            );
            Err(if private {
                CdfError::internal(message)
            } else {
                CdfError::contract(message)
            })
        }
        Ok(_) => fs::read(path).map(Some).map_err(|error| {
            if private {
                project_file_private_path_error(
                    "read project transaction recovery temporary",
                    path,
                    error,
                )
            } else {
                project_prior_read_error(path, error)
            }
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(project_file_host_error(
            "inspect project transaction recovery path",
            path,
            error,
        )),
    }
}

fn prior_file_matches(bytes: Option<&[u8]>, prior: &ProjectFileTransactionPrior) -> bool {
    match prior {
        ProjectFileTransactionPrior::Absent => bytes.is_none(),
        ProjectFileTransactionPrior::Existing { len, sha256 } => {
            file_bytes_match(bytes, *len, sha256)
        }
    }
}

fn file_bytes_match(bytes: Option<&[u8]>, len: u64, sha256: &str) -> bool {
    bytes.is_some_and(|bytes| {
        u64::try_from(bytes.len()) == Ok(len) && project_file_bytes_hash(bytes) == sha256
    })
}

fn read_project_file_transaction_marker(
    project_root: &Path,
) -> Result<ProjectFileTransactionMarkerAuthority> {
    let bytes = read_project_file_transaction_marker_bytes(project_root)?;
    let Some(bytes) = bytes else {
        return Ok(ProjectFileTransactionMarkerAuthority {
            bytes: None,
            marker: None,
        });
    };
    let marker =
        serde_json::from_slice::<ProjectFileTransactionMarker>(&bytes).map_err(|error| {
            CdfError::internal(format!(
                "parse project transaction marker {}: {error}",
                project_root.join(PROJECT_FILE_TRANSACTION_MARKER).display()
            ))
        })?;
    validate_project_file_transaction_marker(&marker)?;
    Ok(ProjectFileTransactionMarkerAuthority {
        bytes: Some(bytes),
        marker: Some(marker),
    })
}

fn read_project_file_transaction_marker_bytes(project_root: &Path) -> Result<Option<Vec<u8>>> {
    let path = project_root.join(PROJECT_FILE_TRANSACTION_MARKER);
    match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            Err(CdfError::internal(format!(
                "project transaction marker {} is not a real regular file",
                path.display()
            )))
        }
        Ok(metadata) if metadata.len() > MAX_PROJECT_FILE_TRANSACTION_MARKER_BYTES => {
            Err(CdfError::internal(format!(
                "project transaction marker {} exceeds the {}-byte limit",
                path.display(),
                MAX_PROJECT_FILE_TRANSACTION_MARKER_BYTES
            )))
        }
        Ok(_) => fs::read(&path).map(Some).map_err(|error| {
            project_file_host_error("read project transaction marker", &path, error)
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(project_file_host_error(
            "inspect project transaction marker",
            &path,
            error,
        )),
    }
}

fn validate_project_file_transaction_marker(marker: &ProjectFileTransactionMarker) -> Result<()> {
    validate_project_file_transaction_marker_inner(marker).map_err(|error| {
        CdfError::internal(format!(
            "invalid CDF-managed project transaction marker: {}",
            error.message
        ))
    })
}

fn validate_project_file_transaction_marker_inner(
    marker: &ProjectFileTransactionMarker,
) -> Result<()> {
    if marker.version != PROJECT_FILE_TRANSACTION_MARKER_VERSION || marker.generation == 0 {
        return Err(CdfError::data(format!(
            "project transaction marker has unsupported version {} or invalid generation {}",
            marker.version, marker.generation
        )));
    }
    let ProjectFileTransactionState::Pending {
        commit_relative_path,
        guards,
        entries,
    } = &marker.state
    else {
        return Ok(());
    };
    validate_relative_path(commit_relative_path)?;
    if entries.is_empty() {
        return Err(CdfError::data(
            "pending project transaction marker has no file entries",
        ));
    }
    let mut targets = BTreeSet::new();
    let mut temporaries = BTreeSet::new();
    for guard in guards {
        validate_relative_path(&guard.relative_path)?;
        validate_project_file_hash(&guard.sha256)?;
        if !targets.insert(guard.relative_path.clone()) {
            return Err(CdfError::data(
                "project transaction marker repeats a guarded or target path",
            ));
        }
    }
    for entry in entries {
        validate_relative_path(&entry.relative_path)?;
        validate_relative_path(&entry.temporary_relative_path)?;
        if !targets.insert(entry.relative_path.clone())
            || !temporaries.insert(entry.temporary_relative_path.clone())
        {
            return Err(CdfError::data(
                "project transaction marker repeats a guarded, target, or temporary path",
            ));
        }
        validate_project_transaction_temporary_path(entry)?;
        validate_project_file_hash(entry.new_sha256.as_str())?;
        if let ProjectFileTransactionPrior::Existing { sha256, .. } = &entry.prior {
            validate_project_file_hash(sha256)?;
        }
    }
    if let Some(index) = entries
        .iter()
        .position(|entry| entry.relative_path == *commit_relative_path)
        && index + 1 != entries.len()
    {
        return Err(CdfError::data(
            "project transaction marker commit point is not the final installed entry",
        ));
    }
    Ok(())
}

fn validate_project_transaction_temporary_path(entry: &ProjectFileTransactionEntry) -> Result<()> {
    if entry.relative_path.parent() != entry.temporary_relative_path.parent() {
        return Err(CdfError::data(
            "project transaction marker temporary is not beside its target",
        ));
    }
    let target_name = entry
        .relative_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| CdfError::data("project transaction target has no UTF-8 filename"))?;
    let temporary_name = entry
        .temporary_relative_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| CdfError::data("project transaction temporary has no UTF-8 filename"))?;
    if !temporary_name.starts_with(format!(".{target_name}.").as_str())
        || !temporary_name.ends_with(".project-txn.tmp")
    {
        return Err(CdfError::data(
            "project transaction marker names an invalid managed temporary",
        ));
    }
    Ok(())
}

fn validate_project_file_hash(hash: &str) -> Result<()> {
    let Some(hex) = hash.strip_prefix("sha256:") else {
        return Err(CdfError::data(
            "project transaction marker has a non-SHA-256 content hash",
        ));
    };
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(CdfError::data(
            "project transaction marker has an invalid SHA-256 content hash",
        ));
    }
    Ok(())
}

fn replace_project_file_transaction_marker(
    project_root: &Path,
    expected: Option<&[u8]>,
    marker: Option<&ProjectFileTransactionMarker>,
) -> Result<ProjectFileTransactionMarkerAuthority> {
    let replacement = marker
        .map(serialize_project_file_transaction_marker)
        .transpose()?;
    replace_project_file_transaction_marker_bytes(project_root, expected, replacement.as_deref())?;
    Ok(ProjectFileTransactionMarkerAuthority {
        bytes: replacement,
        marker: marker.cloned(),
    })
}

fn serialize_project_file_transaction_marker(
    marker: &ProjectFileTransactionMarker,
) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec_pretty(marker).map_err(|error| {
        CdfError::internal(format!("serialize project transaction marker: {error}"))
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn replace_project_file_transaction_marker_bytes(
    project_root: &Path,
    expected: Option<&[u8]>,
    replacement: Option<&[u8]>,
) -> Result<()> {
    let path = project_root.join(PROJECT_FILE_TRANSACTION_MARKER);
    let observed = read_project_file_transaction_marker_bytes(project_root)?;
    if observed.as_deref() != expected {
        return Err(CdfError::contract(format!(
            "project transaction marker {} changed outside the CDF mutation guard",
            path.display()
        )));
    }
    let Some(replacement) = replacement else {
        if observed.is_some() {
            fs::remove_file(&path).map_err(|error| {
                project_file_private_path_error("remove project transaction marker", &path, error)
            })?;
            sync_project_file_transaction_marker_parent(project_root, &path)?;
        }
        return Ok(());
    };

    let temporary = temporary_path(&path)?;
    write_synced_file(&temporary, replacement, true, None)?;
    let current = read_project_file_transaction_marker_bytes(project_root)?;
    if current.as_deref() != expected {
        let _ = fs::remove_file(&temporary);
        return Err(CdfError::contract(format!(
            "project transaction marker {} changed before publication",
            path.display()
        )));
    }
    let install = if expected.is_some() {
        fs::rename(&temporary, &path).map_err(|error| {
            project_file_private_path_error("replace project transaction marker", &path, error)
        })
    } else {
        fs::hard_link(&temporary, &path)
            .map_err(|error| {
                if concurrent_project_file_error(&error) {
                    concurrent_project_file_change(&path, error)
                } else {
                    project_file_private_path_error(
                        "create project transaction marker",
                        &path,
                        error,
                    )
                }
            })
            .and_then(|()| {
                fs::remove_file(&temporary).map_err(|error| {
                    project_file_private_path_error(
                        "remove project transaction marker temporary",
                        &temporary,
                        error,
                    )
                })
            })
    };
    if let Err(error) = install {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    sync_project_file_transaction_marker_parent(project_root, &path)
}

#[cfg(unix)]
fn sync_project_file_transaction_marker_parent(project_root: &Path, path: &Path) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        CdfError::internal(format!(
            "project transaction marker {} has no parent",
            path.display()
        ))
    })?;
    sync_directory_ancestry_through_root(parent, project_root, |directory, error| {
        project_file_host_error(
            "sync project transaction marker directory ancestry",
            directory,
            error,
        )
    })
}

#[cfg(not(unix))]
fn sync_project_file_transaction_marker_parent(_project_root: &Path, _path: &Path) -> Result<()> {
    Ok(())
}

fn verify_existing_safe_parent(project_root: &Path, parent: &Path, private: bool) -> Result<()> {
    let invalid_shape = |message: String| {
        if private {
            CdfError::internal(message)
        } else {
            CdfError::contract(message)
        }
    };
    let relative = parent.strip_prefix(project_root).map_err(|_| {
        invalid_shape(format!(
            "project transaction recovery parent {} escapes project root {}",
            parent.display(),
            project_root.display()
        ))
    })?;
    let mut current = project_root.to_path_buf();
    for component in relative.components() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Ok(_) => {
                return Err(invalid_shape(format!(
                    "project transaction recovery parent {} is not a real directory",
                    current.display()
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(invalid_shape(format!(
                    "project transaction recovery parent {} is missing",
                    current.display()
                )));
            }
            Err(error) => {
                return Err(project_file_host_error(
                    "inspect project transaction recovery parent",
                    &current,
                    error,
                ));
            }
        }
    }
    Ok(())
}

fn project_file_bytes_hash(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
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

fn read_and_validate_guard(
    project_root: &Path,
    guard: &ProjectFileGuard,
) -> Result<ProjectFileTransactionGuard> {
    let path = project_root.join(&guard.relative_path);
    verify_existing_safe_parent(
        project_root,
        path.parent().ok_or_else(|| {
            CdfError::contract(format!(
                "project transaction guard {} has no parent",
                path.display()
            ))
        })?,
        false,
    )?;
    let observed = read_recovery_file(&path, false)?.ok_or_else(|| {
        CdfError::contract(format!(
            "project file transaction guard is missing {}",
            guard.relative_path.display()
        ))
    })?;
    if observed != guard.bytes {
        return Err(CdfError::contract(format!(
            "project file transaction refused because guarded authority changed for {}",
            guard.relative_path.display()
        )));
    }
    Ok(ProjectFileTransactionGuard {
        relative_path: guard.relative_path.clone(),
        len: u64::try_from(observed.len())
            .map_err(|_| CdfError::internal("project transaction guard length exceeds u64"))?,
        sha256: project_file_bytes_hash(&observed),
    })
}

fn validate_transaction_guards(
    project_root: &Path,
    guards: &[ProjectFileTransactionGuard],
) -> Result<()> {
    for guard in guards {
        let path = project_root.join(&guard.relative_path);
        verify_existing_safe_parent(
            project_root,
            path.parent().ok_or_else(|| {
                CdfError::contract(format!(
                    "project transaction guard {} has no parent",
                    path.display()
                ))
            })?,
            false,
        )?;
        let observed = read_recovery_file(&path, false)?;
        if !file_bytes_match(observed.as_deref(), guard.len, &guard.sha256) {
            return Err(CdfError::contract(format!(
                "project file transaction refused a concurrent change to guarded authority {}",
                guard.relative_path.display()
            )));
        }
    }
    Ok(())
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

#[cfg(not(unix))]
fn sync_installed_parent_directories(_project_root: &Path, _installed: &[PathBuf]) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROCESS_CRASH_HELPER_ROOT: &str = "CDF_PROJECT_FILE_TRANSACTION_CRASH_ROOT";
    const PROCESS_CRASH_EXIT_CODE: i32 = 86;

    #[test]
    fn transaction_failure_after_pending_marker_recovers_forward() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("cdf.toml"), b"before-project").unwrap();
        let writes = vec![
            ProjectFileWrite::new(
                "cdf/local/events.cdf.sql",
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
            b"after-project"
        );
        assert_eq!(
            fs::read(root.path().join("cdf/local/events.cdf.sql")).unwrap(),
            b"resource"
        );
        assert!(!root.path().join("cdf.lock").exists());
        assert!(
            read_project_file_transaction_marker(root.path())
                .unwrap()
                .is_pending()
        );
        let marker_before = fs::read(root.path().join(PROJECT_FILE_TRANSACTION_MARKER)).unwrap();
        let error = project_file_transaction_generation(root.path()).unwrap_err();
        assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
        assert_eq!(
            fs::read(root.path().join(PROJECT_FILE_TRANSACTION_MARKER),).unwrap(),
            marker_before
        );
        assert!(!root.path().join("cdf.lock").exists());
        assert_eq!(recover_project_file_transaction(root.path()).unwrap(), 1);
        assert_eq!(fs::read(root.path().join("cdf.lock")).unwrap(), b"commit");
    }

    #[test]
    fn fail_closed_publication_never_recovers_a_pending_transaction() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("cdf.toml"), b"before-project").unwrap();
        let pending_writes = vec![
            ProjectFileWrite::new(
                "cdf.toml",
                b"after-project".to_vec(),
                ProjectFileExpectation::Exact(b"before-project".to_vec()),
            ),
            ProjectFileWrite::new(
                "cdf.lock",
                b"pending-lock".to_vec(),
                ProjectFileExpectation::Absent,
            ),
        ];
        publish_project_files_inner(root.path(), Path::new("cdf.lock"), pending_writes, Some(1))
            .unwrap_err();
        let marker_before = fs::read(root.path().join(PROJECT_FILE_TRANSACTION_MARKER)).unwrap();
        assert!(!root.path().join("cdf.lock").exists());

        let error = publish_project_files_transactionally_without_recovery(
            root.path(),
            "manifest.json",
            vec![ProjectFileWrite::new(
                "manifest.json",
                b"manifest".to_vec(),
                ProjectFileExpectation::Absent,
            )],
        )
        .unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
        assert!(error.message.contains("cdf compile --refresh"));
        assert_eq!(
            fs::read(root.path().join(PROJECT_FILE_TRANSACTION_MARKER)).unwrap(),
            marker_before
        );
        assert!(!root.path().join("cdf.lock").exists());
        assert!(!root.path().join("manifest.json").exists());
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
            &mut |_| Ok(()),
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
            &mut |_| Ok(()),
        )
        .unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
        assert_eq!(fs::read(target).unwrap(), b"racer");
    }

    #[test]
    fn post_install_racer_is_preserved_by_forward_recovery() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("cdf.toml");
        fs::write(&target, b"observed").unwrap();
        let writes = vec![
            ProjectFileWrite::new(
                "cdf.toml",
                b"ours".to_vec(),
                ProjectFileExpectation::Exact(b"observed".to_vec()),
            ),
            ProjectFileWrite::new(
                "cdf.lock",
                b"commit".to_vec(),
                ProjectFileExpectation::Absent,
            ),
        ];
        let installed_first = std::cell::Cell::new(false);

        let error = publish_project_files_inner_with_hook(
            root.path(),
            Path::new("cdf.lock"),
            writes,
            None,
            &mut |path| {
                if installed_first.get() && path.ends_with("cdf.lock") {
                    return Err(CdfError::environment("injected later install failure"));
                }
                Ok(())
            },
            &mut |_, _| Ok(()),
            &mut |path| {
                if path.ends_with("cdf.toml") {
                    fs::write(path, b"racer").unwrap();
                    installed_first.set(true);
                }
                Ok(())
            },
        )
        .unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Environment);
        assert_eq!(fs::read(&target).unwrap(), b"racer");
        let recovery_error = recover_project_file_transaction(root.path()).unwrap_err();
        assert_eq!(recovery_error.kind, cdf_kernel::ErrorKind::Contract);
        assert_eq!(fs::read(&target).unwrap(), b"racer");
    }

    #[test]
    fn guarded_input_change_after_an_output_install_blocks_commit_and_recovery() {
        let root = tempfile::tempdir().unwrap();
        let authored = root.path().join("cdf.toml");
        let manifest = root.path().join("manifest.json");
        let lock = root.path().join("cdf.lock");
        fs::write(&authored, b"authored\n").unwrap();
        let guards = vec![ProjectFileGuard::exact("cdf.toml", b"authored\n".to_vec())];
        let writes = vec![
            ProjectFileWrite::new(
                "manifest.json",
                b"manifest\n".to_vec(),
                ProjectFileExpectation::Absent,
            ),
            ProjectFileWrite::new(
                "cdf.lock",
                b"lock\n".to_vec(),
                ProjectFileExpectation::Absent,
            ),
        ];
        let mut changed = false;

        let error = publish_project_files_inner_with_guards_and_hook(
            root.path(),
            Path::new("cdf.lock"),
            guards,
            writes,
            &mut |path| {
                if !changed && path == manifest {
                    fs::write(&authored, b"edited\n").unwrap();
                    changed = true;
                }
                Ok(())
            },
        )
        .unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
        assert_eq!(fs::read(&authored).unwrap(), b"edited\n");
        assert_eq!(fs::read(&manifest).unwrap(), b"manifest\n");
        assert!(!lock.exists());
        assert!(
            read_project_file_transaction_marker(root.path())
                .unwrap()
                .is_pending()
        );

        let recovery_error = recover_project_file_transaction(root.path()).unwrap_err();

        assert_eq!(recovery_error.kind, cdf_kernel::ErrorKind::Contract);
        assert_eq!(fs::read(&authored).unwrap(), b"edited\n");
        assert!(!lock.exists());
        assert!(
            read_project_file_transaction_marker(root.path())
                .unwrap()
                .is_pending()
        );
    }

    #[test]
    fn absent_install_cleanup_failure_remains_forward_recoverable() {
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
            &mut |_| Ok(()),
        )
        .unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Internal);
        assert!(error.message.contains("CDF-managed scratch path"));
        assert_eq!(fs::read(&target).unwrap(), b"commit");
        assert!(
            read_project_file_transaction_marker(root.path())
                .unwrap()
                .is_pending()
        );
        assert_eq!(recover_project_file_transaction(root.path()).unwrap(), 1);
        assert_eq!(fs::read(&target).unwrap(), b"commit");
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

    #[test]
    fn project_transaction_process_crash_helper() {
        let Some(root) = std::env::var_os(PROCESS_CRASH_HELPER_ROOT) else {
            return;
        };
        let root = PathBuf::from(root);
        let mut after_install = |path: &Path| -> Result<()> {
            if path == root.join("cdf.toml") {
                process::exit(PROCESS_CRASH_EXIT_CODE);
            }
            Ok(())
        };

        publish_project_files_inner_with_hook(
            &root,
            Path::new("cdf.lock"),
            crash_transaction_writes(),
            None,
            &mut |_| Ok(()),
            &mut |_, _| Ok(()),
            &mut after_install,
        )
        .expect("crash helper must exit before publication returns");
    }

    #[test]
    fn process_loss_after_project_install_recovers_before_commit_use_and_retry_converges() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("cdf.toml"), b"project-before\n").unwrap();

        let output = spawn_project_transaction_crash(root.path());

        assert_eq!(output.status.code(), Some(PROCESS_CRASH_EXIT_CODE));
        assert_eq!(
            fs::read(root.path().join("cdf.toml")).unwrap(),
            b"project-after\n"
        );
        assert!(!root.path().join("cdf.lock").exists());
        let pending = read_project_file_transaction_marker(root.path()).unwrap();
        assert!(pending.is_pending());

        let generation = recover_project_file_transaction(root.path()).unwrap();

        assert_eq!(generation, 1);
        assert_eq!(
            fs::read(root.path().join("cdf/local/events.cdf.sql")).unwrap(),
            b"resource\n"
        );
        assert_eq!(fs::read(root.path().join("cdf.lock")).unwrap(), b"commit\n");
        assert!(
            !read_project_file_transaction_marker(root.path())
                .unwrap()
                .is_pending()
        );

        let retry = publish_project_files_transactionally(
            root.path(),
            "cdf.lock",
            vec![
                ProjectFileWrite::new(
                    "cdf/local/events.cdf.sql",
                    b"resource\n".to_vec(),
                    ProjectFileExpectation::AbsentOrExact(b"resource\n".to_vec()),
                ),
                ProjectFileWrite::new(
                    "cdf.toml",
                    b"project-after\n".to_vec(),
                    ProjectFileExpectation::Exact(b"project-after\n".to_vec()),
                ),
                ProjectFileWrite::new(
                    "cdf.lock",
                    b"commit\n".to_vec(),
                    ProjectFileExpectation::Exact(b"commit\n".to_vec()),
                ),
            ],
        )
        .unwrap();
        assert!(retry.installed_paths.is_empty());
        assert_eq!(retry.unchanged_paths.len(), 3);
        assert_eq!(recover_project_file_transaction(root.path()).unwrap(), 1);
    }

    #[test]
    fn crash_recovery_refuses_to_overwrite_unrelated_authority() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("cdf.toml"), b"project-before\n").unwrap();
        let output = spawn_project_transaction_crash(root.path());
        assert_eq!(output.status.code(), Some(PROCESS_CRASH_EXIT_CODE));
        fs::write(root.path().join("cdf.toml"), b"unrelated-racer\n").unwrap();

        let error = recover_project_file_transaction(root.path()).unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
        assert!(error.message.contains("unrelated authority"));
        assert_eq!(
            fs::read(root.path().join("cdf.toml")).unwrap(),
            b"unrelated-racer\n"
        );
        assert!(!root.path().join("cdf.lock").exists());
        assert!(
            read_project_file_transaction_marker(root.path())
                .unwrap()
                .is_pending()
        );
    }

    #[test]
    fn malformed_private_transaction_marker_is_internal() {
        let root = tempfile::tempdir().unwrap();
        fs::create_dir(root.path().join(".cdf")).unwrap();
        fs::write(
            root.path().join(PROJECT_FILE_TRANSACTION_MARKER),
            b"{not-json",
        )
        .unwrap();

        let error = recover_project_file_transaction(root.path()).unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Internal);
        assert!(error.message.contains("parse project transaction marker"));
    }

    #[test]
    fn missing_private_recovery_temporary_is_internal() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("cdf.toml"), b"project-before\n").unwrap();
        let output = spawn_project_transaction_crash(root.path());
        assert_eq!(output.status.code(), Some(PROCESS_CRASH_EXIT_CODE));
        let authority = read_project_file_transaction_marker(root.path()).unwrap();
        let marker = authority.marker.unwrap();
        let ProjectFileTransactionState::Pending { entries, .. } = marker.state else {
            panic!("crash helper must leave a pending marker");
        };
        let lock_entry = entries
            .into_iter()
            .find(|entry| entry.relative_path == Path::new("cdf.lock"))
            .unwrap();
        fs::remove_file(root.path().join(lock_entry.temporary_relative_path)).unwrap();

        let error = recover_project_file_transaction(root.path()).unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Internal);
        assert!(error.message.contains("missing prepared temporary"));
    }

    fn crash_transaction_writes() -> Vec<ProjectFileWrite> {
        vec![
            ProjectFileWrite::new(
                "cdf/local/events.cdf.sql",
                b"resource\n".to_vec(),
                ProjectFileExpectation::Absent,
            ),
            ProjectFileWrite::new(
                "cdf.toml",
                b"project-after\n".to_vec(),
                ProjectFileExpectation::Exact(b"project-before\n".to_vec()),
            ),
            ProjectFileWrite::new(
                "cdf.lock",
                b"commit\n".to_vec(),
                ProjectFileExpectation::Absent,
            ),
        ]
    }

    fn spawn_project_transaction_crash(root: &Path) -> std::process::Output {
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "project_files::tests::project_transaction_process_crash_helper",
                "--nocapture",
            ])
            .env(PROCESS_CRASH_HELPER_ROOT, root)
            .output()
            .unwrap()
    }
}
