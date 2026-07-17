use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions, TryLockError},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use cdf_kernel::{CdfError, PayloadRetention, Result};
use cdf_memory::MemoryCoordinator;
use cdf_runtime::{ByteSource, ContentIdentity, GenerationStrength, RunCancellation};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use sha2::{Digest, Sha256};

const CACHE_VERSION: u16 = 1;
const MAX_MANIFEST_BYTES: u64 = 64 * 1024;
#[cfg(test)]
const HASH_BUFFER_BYTES: usize = 1024 * 1024;
const ROOT_LOCK_RETRY: Duration = Duration::from_millis(5);
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static CACHE_ROOTS: OnceLock<Mutex<BTreeMap<PathBuf, Weak<CacheRoot>>>> = OnceLock::new();

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FilePayloadCachePolicy {
    pub maximum_entries: usize,
    pub maximum_bytes: u64,
}

impl FilePayloadCachePolicy {
    pub fn new(maximum_entries: usize, maximum_bytes: u64) -> Result<Self> {
        if maximum_entries == 0 || maximum_bytes == 0 {
            return Err(CdfError::contract(
                "file payload cache requires positive entry and byte bounds",
            ));
        }
        Ok(Self {
            maximum_entries,
            maximum_bytes,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct FilePayloadCacheKey(String);

impl FilePayloadCacheKey {
    pub(crate) fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if !valid_sha256(&value) {
            return Err(CdfError::contract(
                "file payload cache key must be a canonical sha256 digest",
            ));
        }
        Ok(Self(value))
    }

    fn digest(&self) -> &str {
        self.0
            .strip_prefix("sha256:")
            .expect("validated payload cache key")
    }
}

#[derive(Clone)]
pub struct FilePayloadCache {
    shared: Arc<CacheRoot>,
}

impl std::fmt::Debug for FilePayloadCache {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FilePayloadCache")
            .field("root", &self.shared.root)
            .field("policy", &self.shared.policy)
            .finish_non_exhaustive()
    }
}

struct CacheRoot {
    root: PathBuf,
    policy: FilePayloadCachePolicy,
    owner_uid: u32,
    state: Mutex<CacheState>,
}

#[derive(Default)]
struct CacheState {
    revision: u64,
    entries: BTreeMap<FilePayloadCacheKey, CacheStateEntry>,
    bytes: u64,
}

#[derive(Clone)]
struct CacheStateEntry {
    size_bytes: u64,
    created_ms: u64,
}

pub(crate) struct FilePayloadCacheHit {
    pub(crate) source: Arc<dyn ByteSource>,
    pub(crate) retention: PayloadRetention,
}

impl std::fmt::Debug for FilePayloadCacheHit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FilePayloadCacheHit")
            .field("identity", self.source.identity())
            .field("retained_bytes", &self.retention.bytes())
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub(crate) enum FilePayloadCacheLookup {
    Hit(FilePayloadCacheHit),
    Miss,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FilePayloadCachePromotion {
    Stored,
    AlreadyPresent,
    SkippedCapacity,
    Unavailable,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FilePayloadCacheManifest {
    version: u16,
    key: String,
    sha256: String,
    size_bytes: u64,
    created_ms: u64,
    storage_attestation: String,
    identity: ContentIdentity,
}

impl FilePayloadCacheManifest {
    fn validate(&self, key: &FilePayloadCacheKey) -> Result<()> {
        self.identity.validate()?;
        if self.version != CACHE_VERSION
            || self.key != key.0
            || !valid_sha256(&self.sha256)
            || self.size_bytes == 0
            || self.storage_attestation.is_empty()
            || self.identity.size_bytes != Some(self.size_bytes)
            || self.identity.strength == GenerationStrength::Weak
        {
            return Err(CdfError::data(
                "file payload cache manifest is unsupported or lacks strong complete identity",
            ));
        }
        Ok(())
    }
}

struct ActiveCacheLease {
    marker: File,
    marker_path: PathBuf,
}

impl Drop for ActiveCacheLease {
    fn drop(&mut self) {
        let _ = self.marker.unlock();
        let _ = fs::remove_file(&self.marker_path);
    }
}

struct RemoveFileOnDrop(PathBuf);

impl Drop for RemoveFileOnDrop {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

impl FilePayloadCache {
    pub fn new(
        root: impl Into<PathBuf>,
        maximum_entries: usize,
        maximum_bytes: u64,
    ) -> Result<Self> {
        #[cfg(not(unix))]
        {
            let _ = (root, maximum_entries, maximum_bytes);
            return Err(CdfError::contract(
                "file payload cache requires Unix owner and mode enforcement on this build",
            ));
        }
        #[cfg(unix)]
        {
            let policy = FilePayloadCachePolicy::new(maximum_entries, maximum_bytes)?;
            let root = prepare_cache_root(root.into())?;
            let registry = CACHE_ROOTS.get_or_init(|| Mutex::new(BTreeMap::new()));
            let mut registry = registry
                .lock()
                .map_err(|_| CdfError::internal("file payload cache registry was poisoned"))?;
            if let Some(shared) = registry.get(&root).and_then(Weak::upgrade) {
                if shared.policy != policy {
                    return Err(CdfError::contract(format!(
                        "file payload cache root `{}` is configured with conflicting capacity policies",
                        root.display()
                    )));
                }
                return Ok(Self { shared });
            }
            let owner_uid = private_directory_owner(&root)?;
            let shared = Arc::new(CacheRoot {
                root: root.clone(),
                policy,
                owner_uid,
                state: Mutex::new(CacheState::default()),
            });
            shared.initialize_state()?;
            registry.insert(root, Arc::downgrade(&shared));
            Ok(Self { shared })
        }
    }

    pub fn root(&self) -> &Path {
        &self.shared.root
    }

    pub fn policy(&self) -> &FilePayloadCachePolicy {
        &self.shared.policy
    }

    pub(crate) fn staging_root(&self) -> PathBuf {
        self.shared.staging_root()
    }

    pub(crate) fn lookup(
        &self,
        key: &FilePayloadCacheKey,
        expected_stable_id: &str,
        expected_size_bytes: u64,
        cancellation: &RunCancellation,
        memory: Arc<dyn MemoryCoordinator>,
    ) -> Result<FilePayloadCacheLookup> {
        cancellation.check()?;
        let root_lock = self.shared.lock_root(cancellation)?;
        let manifest_path = self.shared.manifest_path(key);
        let manifest = match read_manifest(&manifest_path) {
            Ok(Some(manifest))
                if manifest.validate(key).is_ok()
                    && manifest.identity.stable_id == expected_stable_id
                    && manifest.size_bytes == expected_size_bytes =>
            {
                manifest
            }
            Ok(None) => return Ok(FilePayloadCacheLookup::Miss),
            Ok(Some(_)) | Err(_) => {
                self.shared.invalidate_key_locked(key)?;
                return Ok(FilePayloadCacheLookup::Miss);
            }
        };
        let object_path = self.shared.object_path(key);
        if !private_immutable_file(&object_path, manifest.size_bytes, self.shared.owner_uid) {
            self.shared.invalidate_key_locked(key)?;
            return Ok(FilePayloadCacheLookup::Miss);
        }
        if crate::local_byte_source::local_storage_attestation(&object_path).as_deref()
            != Ok(manifest.storage_attestation.as_str())
        {
            self.shared.invalidate_key_locked(key)?;
            return Ok(FilePayloadCacheLookup::Miss);
        }
        let lease = self.shared.create_active_lease_locked(key)?;
        drop(root_lock);
        let source = crate::local_byte_source::open_identity_preserving_local_source(
            &object_path,
            manifest.identity,
            manifest.size_bytes,
            memory,
        )?;
        let retention = PayloadRetention::new(Arc::new(lease), manifest.size_bytes)?;
        Ok(FilePayloadCacheLookup::Hit(FilePayloadCacheHit {
            source,
            retention,
        }))
    }

    pub(crate) fn promote(
        &self,
        key: &FilePayloadCacheKey,
        source: &Path,
        identity: ContentIdentity,
        size_bytes: u64,
        sha256: &str,
        cancellation: &RunCancellation,
    ) -> Result<FilePayloadCachePromotion> {
        cancellation.check()?;
        let mut manifest = FilePayloadCacheManifest {
            version: CACHE_VERSION,
            key: key.0.clone(),
            sha256: sha256.to_owned(),
            size_bytes,
            created_ms: now_ms(),
            storage_attestation: String::new(),
            identity,
        };
        if size_bytes > self.shared.policy.maximum_bytes
            || !valid_sha256(sha256)
            || manifest.identity.validate().is_err()
            || manifest.identity.size_bytes != Some(size_bytes)
            || manifest.identity.strength == GenerationStrength::Weak
        {
            return Ok(FilePayloadCachePromotion::SkippedCapacity);
        }
        let source = match fs::canonicalize(source) {
            Ok(source) if source.starts_with(self.shared.staging_root()) => source,
            _ => return Ok(FilePayloadCachePromotion::Unavailable),
        };
        cancellation.check()?;
        File::open(&source)
            .and_then(|file| file.sync_all())
            .map_err(|error| CdfError::data(format!("sync payload cache staging file: {error}")))?;
        let _root_lock = self.shared.lock_root(cancellation)?;
        let mut state = self.shared.lock_state()?;
        self.shared.synchronize_state_locked(&mut state)?;
        self.shared.prune_unreferenced_objects_locked(&mut state)?;
        let pinned = self.shared.pinned_keys_locked()?;
        let existing = state.entries.get(key).cloned();
        if existing
            .as_ref()
            .is_some_and(|entry| entry.size_bytes == size_bytes)
            && private_immutable_file(
                &self.shared.object_path(key),
                size_bytes,
                self.shared.owner_uid,
            )
            && read_manifest(&self.shared.manifest_path(key))
                .ok()
                .flatten()
                .is_some_and(|current| {
                    current.sha256 == sha256
                        && crate::local_byte_source::local_storage_attestation(
                            &self.shared.object_path(key),
                        )
                        .as_deref()
                            == Ok(current.storage_attestation.as_str())
                })
        {
            return Ok(FilePayloadCachePromotion::AlreadyPresent);
        }

        let mut projected_entries = state.entries.len() + usize::from(existing.is_none());
        let mut projected_bytes = state
            .bytes
            .saturating_sub(existing.as_ref().map_or(0, |entry| entry.size_bytes))
            .saturating_add(size_bytes);
        let mut evictions = state
            .entries
            .iter()
            .filter(|(candidate, _)| *candidate != key && !pinned.contains(*candidate))
            .map(|(candidate, entry)| (entry.created_ms, candidate.clone(), entry.size_bytes))
            .collect::<Vec<_>>();
        evictions.sort();
        let mut selected = Vec::new();
        for (_, candidate, bytes) in evictions {
            if projected_entries <= self.shared.policy.maximum_entries
                && projected_bytes <= self.shared.policy.maximum_bytes
            {
                break;
            }
            projected_entries = projected_entries.saturating_sub(1);
            projected_bytes = projected_bytes.saturating_sub(bytes);
            selected.push(candidate);
        }
        if projected_entries > self.shared.policy.maximum_entries
            || projected_bytes > self.shared.policy.maximum_bytes
            || pinned.contains(key)
        {
            return Ok(FilePayloadCachePromotion::SkippedCapacity);
        }

        self.shared.begin_mutation_locked(&state)?;
        for candidate in selected {
            self.shared.remove_entry_files_locked(&candidate);
            if let Some(removed) = state.entries.remove(&candidate) {
                state.bytes = state.bytes.saturating_sub(removed.size_bytes);
            }
        }
        if let Some(replaced) = state.entries.remove(key) {
            self.shared.remove_entry_files_locked(key);
            state.bytes = state.bytes.saturating_sub(replaced.size_bytes);
        }
        set_private_file_permissions(&source)?;
        install_hard_link(&source, &self.shared.object_path(key))?;
        fs::remove_file(&source).map_err(|error| {
            CdfError::data(format!("retire payload cache staging link: {error}"))
        })?;
        manifest.storage_attestation =
            crate::local_byte_source::local_storage_attestation(&self.shared.object_path(key))?;
        manifest.validate(key)?;
        let encoded = serde_json::to_vec(&manifest).map_err(|error| {
            CdfError::internal(format!("serialize payload cache manifest: {error}"))
        })?;
        if u64::try_from(encoded.len()).unwrap_or(u64::MAX) > MAX_MANIFEST_BYTES {
            let _ = fs::remove_file(self.shared.object_path(key));
            return Ok(FilePayloadCachePromotion::Unavailable);
        }
        install_bytes(&self.shared.manifest_path(key), &encoded)?;
        state.bytes = state.bytes.saturating_add(size_bytes);
        state.entries.insert(
            key.clone(),
            CacheStateEntry {
                size_bytes,
                created_ms: manifest.created_ms,
            },
        );
        self.shared.finish_mutation_locked(&mut state)?;
        Ok(FilePayloadCachePromotion::Stored)
    }
}

impl CacheRoot {
    fn initialize_state(&self) -> Result<()> {
        let cancellation = RunCancellation::default();
        let _root_lock = self.lock_root(&cancellation)?;
        let mut state = self.lock_state()?;
        state.revision = read_revision(&self.revision_path()).unwrap_or(0);
        self.rebuild_state_locked(&mut state)
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, CacheState>> {
        self.state
            .lock()
            .map_err(|_| CdfError::internal("file payload cache state was poisoned"))
    }

    fn manifest_root(&self) -> PathBuf {
        self.root.join("keys")
    }

    fn object_root(&self) -> PathBuf {
        self.root.join("objects")
    }

    fn staging_root(&self) -> PathBuf {
        self.root.join("staging")
    }

    fn active_root(&self) -> PathBuf {
        self.root.join("active")
    }

    fn lock_path(&self) -> PathBuf {
        self.root.join("root.lock")
    }

    fn revision_path(&self) -> PathBuf {
        self.root.join("revision")
    }

    fn manifest_path(&self, key: &FilePayloadCacheKey) -> PathBuf {
        self.manifest_root().join(format!("{}.json", key.digest()))
    }

    fn object_path(&self, key: &FilePayloadCacheKey) -> PathBuf {
        self.object_root().join(format!("{}.bin", key.digest()))
    }

    fn lock_root(&self, cancellation: &RunCancellation) -> Result<File> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.lock_path())
            .map_err(|error| CdfError::data(format!("open payload cache root lock: {error}")))?;
        loop {
            cancellation.check()?;
            match file.try_lock() {
                Ok(()) => return Ok(file),
                Err(TryLockError::WouldBlock) => std::thread::sleep(ROOT_LOCK_RETRY),
                Err(TryLockError::Error(error)) => {
                    return Err(CdfError::data(format!("lock payload cache root: {error}")));
                }
            }
        }
    }

    fn synchronize_state_locked(&self, state: &mut CacheState) -> Result<()> {
        let revision = read_revision(&self.revision_path()).unwrap_or(0);
        if revision != state.revision {
            state.revision = revision;
            self.rebuild_state_locked(state)?;
        }
        Ok(())
    }

    fn rebuild_state_locked(&self, state: &mut CacheState) -> Result<()> {
        state.entries.clear();
        state.bytes = 0;
        let pinned = self.pinned_keys_locked()?;
        let entries = fs::read_dir(self.manifest_root())
            .map_err(|error| CdfError::data(format!("read payload cache manifests: {error}")))?;
        for entry in entries.filter_map(|entry| entry.ok()) {
            let path = entry.path();
            let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
                let _ = fs::remove_file(path);
                continue;
            };
            let Ok(key) = FilePayloadCacheKey::new(format!("sha256:{stem}")) else {
                let _ = fs::remove_file(path);
                continue;
            };
            let manifest = match read_manifest(&path) {
                Ok(Some(manifest)) if manifest.validate(&key).is_ok() => manifest,
                _ => {
                    let _ = fs::remove_file(self.manifest_path(&key));
                    if !pinned.contains(&key) {
                        let _ = fs::remove_file(self.object_path(&key));
                    }
                    continue;
                }
            };
            if !private_immutable_file(&self.object_path(&key), manifest.size_bytes, self.owner_uid)
            {
                let _ = fs::remove_file(self.manifest_path(&key));
                if !pinned.contains(&key) {
                    let _ = fs::remove_file(self.object_path(&key));
                }
                continue;
            }
            state.bytes = state
                .bytes
                .checked_add(manifest.size_bytes)
                .ok_or_else(|| CdfError::data("file payload cache byte accounting overflowed"))?;
            state.entries.insert(
                key,
                CacheStateEntry {
                    size_bytes: manifest.size_bytes,
                    created_ms: manifest.created_ms,
                },
            );
        }
        state.bytes = state
            .bytes
            .checked_add(self.remove_unreferenced_objects_locked(&state.entries)?)
            .ok_or_else(|| CdfError::data("file payload cache byte accounting overflowed"))?;
        Ok(())
    }

    fn begin_mutation_locked(&self, state: &CacheState) -> Result<()> {
        write_revision(&self.revision_path(), state.revision.saturating_add(1))
    }

    fn finish_mutation_locked(&self, state: &mut CacheState) -> Result<()> {
        state.revision = state.revision.saturating_add(1);
        Ok(())
    }

    fn invalidate_key_locked(&self, key: &FilePayloadCacheKey) -> Result<()> {
        let mut state = self.lock_state()?;
        self.synchronize_state_locked(&mut state)?;
        if let Some(removed) = state.entries.remove(key) {
            self.begin_mutation_locked(&state)?;
            let pinned = self.pinned_keys_locked()?;
            let _ = fs::remove_file(self.manifest_path(key));
            if !pinned.contains(key) {
                let _ = fs::remove_file(self.object_path(key));
                state.bytes = state.bytes.saturating_sub(removed.size_bytes);
            }
            self.finish_mutation_locked(&mut state)?;
        } else {
            let _ = fs::remove_file(self.manifest_path(key));
        }
        Ok(())
    }

    fn remove_entry_files_locked(&self, key: &FilePayloadCacheKey) {
        let _ = fs::remove_file(self.manifest_path(key));
        let _ = fs::remove_file(self.object_path(key));
    }

    fn remove_unreferenced_objects_locked(
        &self,
        entries: &BTreeMap<FilePayloadCacheKey, CacheStateEntry>,
    ) -> Result<u64> {
        let pinned = self.pinned_keys_locked()?;
        let mut pinned_orphan_bytes = 0_u64;
        if let Ok(objects) = fs::read_dir(self.object_root()) {
            for entry in objects.filter_map(|entry| entry.ok()) {
                let path = entry.path();
                let key = path
                    .file_stem()
                    .and_then(|value| value.to_str())
                    .and_then(|stem| FilePayloadCacheKey::new(format!("sha256:{stem}")).ok());
                match key.as_ref() {
                    Some(key) if entries.contains_key(key) => {}
                    Some(key) if pinned.contains(key) => {
                        pinned_orphan_bytes = pinned_orphan_bytes
                            .checked_add(path.metadata().map_or(0, |metadata| metadata.len()))
                            .ok_or_else(|| {
                                CdfError::data("file payload cache byte accounting overflowed")
                            })?;
                    }
                    _ => {
                        let _ = fs::remove_file(path);
                    }
                }
            }
        }
        Ok(pinned_orphan_bytes)
    }

    fn prune_unreferenced_objects_locked(&self, state: &mut CacheState) -> Result<()> {
        let pinned = self.pinned_keys_locked()?;
        let objects = fs::read_dir(self.object_root())
            .map_err(|error| CdfError::data(format!("read payload cache objects: {error}")))?;
        for entry in objects.filter_map(|entry| entry.ok()) {
            let path = entry.path();
            let key = path
                .file_stem()
                .and_then(|value| value.to_str())
                .and_then(|stem| FilePayloadCacheKey::new(format!("sha256:{stem}")).ok());
            if key
                .as_ref()
                .is_some_and(|key| state.entries.contains_key(key) || pinned.contains(key))
            {
                continue;
            }
            let bytes = path.metadata().map_or(0, |metadata| metadata.len());
            if fs::remove_file(&path).is_ok() {
                state.bytes = state.bytes.saturating_sub(bytes);
            }
        }
        Ok(())
    }

    fn create_active_lease_locked(&self, key: &FilePayloadCacheKey) -> Result<ActiveCacheLease> {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let marker_path = self.active_root().join(format!(
            "{}-{}-{sequence}.lease",
            key.digest(),
            std::process::id()
        ));
        let marker = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&marker_path)
            .map_err(|error| CdfError::data(format!("create payload cache read lease: {error}")))?;
        set_private_mutable_file_permissions(&marker_path)?;
        marker
            .lock()
            .map_err(|error| CdfError::data(format!("lock payload cache read lease: {error}")))?;
        Ok(ActiveCacheLease {
            marker,
            marker_path,
        })
    }

    fn pinned_keys_locked(&self) -> Result<BTreeSet<FilePayloadCacheKey>> {
        let mut pinned = BTreeSet::new();
        let entries = fs::read_dir(self.active_root())
            .map_err(|error| CdfError::data(format!("read payload cache leases: {error}")))?;
        for entry in entries.filter_map(|entry| entry.ok()) {
            let path = entry.path();
            let key = path
                .file_name()
                .and_then(|value| value.to_str())
                .and_then(|name| name.split('-').next())
                .and_then(|digest| FilePayloadCacheKey::new(format!("sha256:{digest}")).ok());
            let Some(key) = key else {
                let _ = fs::remove_file(path);
                continue;
            };
            let file = match OpenOptions::new().read(true).write(true).open(&path) {
                Ok(file) => file,
                Err(_) => {
                    pinned.insert(key);
                    continue;
                }
            };
            match file.try_lock() {
                Ok(()) => {
                    let _ = file.unlock();
                    let _ = fs::remove_file(path);
                }
                Err(TryLockError::WouldBlock) | Err(TryLockError::Error(_)) => {
                    pinned.insert(key);
                }
            }
        }
        Ok(pinned)
    }
}

fn prepare_cache_root(root: PathBuf) -> Result<PathBuf> {
    let absolute = if root.is_absolute() {
        root
    } else {
        std::env::current_dir()
            .map_err(|error| CdfError::data(format!("resolve payload cache root: {error}")))?
            .join(root)
    };
    let root = canonicalize_future_path(&absolute)?;
    fs::create_dir_all(&root)
        .map_err(|error| CdfError::data(format!("create payload cache root: {error}")))?;
    reject_symlink_components(&root)?;
    set_private_directory_permissions(&root)?;
    let owner_uid = private_directory_owner(&root)?;
    for child in ["keys", "objects", "staging", "active"] {
        let path = root.join(child);
        fs::create_dir(&path)
            .or_else(|error| {
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    Ok(())
                } else {
                    Err(error)
                }
            })
            .map_err(|error| CdfError::data(format!("create payload cache directory: {error}")))?;
        reject_symlink_components(&path)?;
        set_private_directory_permissions(&path)?;
        if private_directory_owner(&path)? != owner_uid {
            return Err(CdfError::auth(
                "payload cache directories must share one owner",
            ));
        }
    }
    for file_name in ["root.lock", "revision"] {
        let path = root.join(file_name);
        if !path.exists() {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .map_err(|error| {
                    CdfError::data(format!("create payload cache control: {error}"))
                })?;
            if file_name == "revision" {
                file.write_all(b"0").map_err(|error| {
                    CdfError::data(format!("write payload cache revision: {error}"))
                })?;
                file.sync_all().ok();
            }
            set_private_mutable_file_permissions(&path)?;
        }
        if !private_mutable_file(&path, owner_uid) {
            return Err(CdfError::auth(
                "payload cache control files must be owner-only regular files",
            ));
        }
    }
    Ok(root)
}

pub(crate) fn resolve_project_cache_root(
    project_root: &Path,
    configured: &Path,
) -> Result<PathBuf> {
    if configured.is_absolute() {
        return Ok(configured.to_path_buf());
    }
    let project_root = fs::canonicalize(project_root)
        .map_err(|error| CdfError::data(format!("canonicalize project root: {error}")))?;
    let resolved = canonicalize_future_path(&project_root.join(configured))?;
    if !resolved.starts_with(&project_root) {
        return Err(CdfError::contract(
            "relative file payload cache location must stay under the project root",
        ));
    }
    Ok(resolved)
}

fn canonicalize_future_path(path: &Path) -> Result<PathBuf> {
    let mut existing = path.to_path_buf();
    let mut missing = Vec::new();
    while !existing.exists() {
        let component = existing
            .file_name()
            .ok_or_else(|| CdfError::contract("payload cache path has no existing ancestor"))?
            .to_owned();
        missing.push(component);
        existing = existing
            .parent()
            .ok_or_else(|| CdfError::contract("payload cache path has no existing ancestor"))?
            .to_path_buf();
    }
    if existing
        .symlink_metadata()
        .is_ok_and(|metadata| metadata.file_type().is_symlink())
    {
        return Err(CdfError::contract(
            "payload cache root cannot be a symbolic link",
        ));
    }
    let mut resolved = fs::canonicalize(existing)
        .map_err(|error| CdfError::data(format!("canonicalize payload cache root: {error}")))?;
    for component in missing.into_iter().rev() {
        resolved.push(component);
    }
    Ok(resolved)
}

fn reject_symlink_components(path: &Path) -> Result<()> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        if current.as_os_str().is_empty() || current == Path::new("/") {
            continue;
        }
        let metadata = fs::symlink_metadata(&current).map_err(|error| {
            CdfError::data(format!(
                "inspect payload cache path {}: {error}",
                current.display()
            ))
        })?;
        if metadata.file_type().is_symlink() {
            return Err(CdfError::contract(format!(
                "payload cache path cannot traverse symbolic link `{}`",
                current.display()
            )));
        }
    }
    Ok(())
}

#[cfg(unix)]
fn private_directory_owner(path: &Path) -> Result<u32> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| CdfError::data(format!("inspect payload cache directory: {error}")))?;
    if !metadata.file_type().is_dir() || metadata.permissions().mode() & 0o077 != 0 {
        return Err(CdfError::auth(
            "payload cache directories must be owner-only and non-symlinked",
        ));
    }
    Ok(metadata.uid())
}

fn set_private_directory_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|error| CdfError::auth(format!("secure payload cache directory: {error}")))?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Err(CdfError::contract(
            "payload cache owner-only storage is unsupported on this build",
        ))
    }
}

fn set_private_file_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o400))
            .map_err(|error| CdfError::auth(format!("secure payload cache object: {error}")))?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Err(CdfError::contract(
            "payload cache owner-only storage is unsupported on this build",
        ))
    }
}

fn set_private_mutable_file_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .map_err(|error| CdfError::auth(format!("secure payload cache control: {error}")))?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Err(CdfError::contract(
            "payload cache owner-only storage is unsupported on this build",
        ))
    }
}

#[cfg(unix)]
fn private_immutable_file(path: &Path, size_bytes: u64, owner_uid: u32) -> bool {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    path.symlink_metadata().is_ok_and(|metadata| {
        metadata.file_type().is_file()
            && metadata.len() == size_bytes
            && metadata.uid() == owner_uid
            && metadata.permissions().mode() & 0o777 == 0o400
    })
}

#[cfg(unix)]
fn private_mutable_file(path: &Path, owner_uid: u32) -> bool {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    path.symlink_metadata().is_ok_and(|metadata| {
        metadata.file_type().is_file()
            && metadata.uid() == owner_uid
            && metadata.permissions().mode() & 0o077 == 0
    })
}

fn valid_sha256(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

fn read_manifest(path: &Path) -> std::io::Result<Option<FilePayloadCacheManifest>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if file.metadata()?.len() > MAX_MANIFEST_BYTES {
        return Err(std::io::Error::other("payload cache manifest is oversized"));
    }
    let mut bytes = Vec::new();
    file.take(MAX_MANIFEST_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_MANIFEST_BYTES {
        return Err(std::io::Error::other("payload cache manifest is oversized"));
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(std::io::Error::other)
}

#[cfg(test)]
fn hash_file_cancellable(path: &Path, cancellation: &RunCancellation) -> Result<String> {
    let mut file = File::open(path)
        .map_err(|error| CdfError::data(format!("open payload cache object: {error}")))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; HASH_BUFFER_BYTES];
    loop {
        cancellation.check()?;
        let read = file
            .read(&mut buffer)
            .map_err(|error| CdfError::data(format!("read payload cache object: {error}")))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    cancellation.check()?;
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

fn install_hard_link(source: &Path, destination: &Path) -> Result<()> {
    fs::hard_link(source, destination).map_err(|error| {
        CdfError::data(format!("atomically install payload cache object: {error}"))
    })
}

fn install_bytes(destination: &Path, bytes: &[u8]) -> Result<()> {
    let temporary = temporary_path(destination);
    let _guard = RemoveFileOnDrop(temporary.clone());
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(|error| CdfError::data(format!("create payload cache manifest: {error}")))?;
    output
        .write_all(bytes)
        .map_err(|error| CdfError::data(format!("write payload cache manifest: {error}")))?;
    output
        .sync_all()
        .map_err(|error| CdfError::data(format!("sync payload cache manifest: {error}")))?;
    set_private_file_permissions(&temporary)?;
    fs::rename(&temporary, destination)
        .map_err(|error| CdfError::data(format!("publish payload cache manifest: {error}")))?;
    Ok(())
}

fn temporary_path(destination: &Path) -> PathBuf {
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    destination.with_extension(format!("tmp-{}-{sequence}", std::process::id()))
}

fn read_revision(path: &Path) -> std::io::Result<u64> {
    fs::read_to_string(path)?
        .parse::<u64>()
        .map_err(std::io::Error::other)
}

fn write_revision(path: &Path, revision: u64) -> Result<()> {
    install_bytes(path, revision.to_string().as_bytes())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(bytes: u64) -> ContentIdentity {
        ContentIdentity {
            stable_id: "https://data.example/events.parquet".to_owned(),
            size_bytes: Some(bytes),
            generation: Some("etag-1".to_owned()),
            checksum: None,
            strength: GenerationStrength::Strong,
        }
    }

    fn stage(cache: &FilePayloadCache, bytes: &[u8]) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new_in(cache.staging_root()).unwrap();
        file.write_all(bytes).unwrap();
        file.flush().unwrap();
        file
    }

    #[test]
    fn cache_verifies_same_size_corruption_and_retains_active_source() {
        let temp = tempfile::tempdir().unwrap();
        let cache = FilePayloadCache::new(temp.path().join("cache"), 1, 64).unwrap();
        let key = FilePayloadCacheKey::new(format!("sha256:{:064x}", 1)).unwrap();
        let staged = stage(&cache, b"payload-one");
        let sha256 = hash_file_cancellable(staged.path(), &RunCancellation::default()).unwrap();
        assert_eq!(
            cache
                .promote(
                    &key,
                    staged.path(),
                    identity(11),
                    11,
                    &sha256,
                    &RunCancellation::default(),
                )
                .unwrap(),
            FilePayloadCachePromotion::Stored
        );
        let hit = match cache
            .lookup(
                &key,
                "https://data.example/events.parquet",
                11,
                &RunCancellation::default(),
                crate::test_execution_services().memory(),
            )
            .unwrap()
        {
            FilePayloadCacheLookup::Hit(hit) => hit,
            other => panic!("expected payload cache hit, got {other:?}"),
        };
        let other_key = FilePayloadCacheKey::new(format!("sha256:{:064x}", 2)).unwrap();
        let other = stage(&cache, b"payload-two");
        let other_sha = hash_file_cancellable(other.path(), &RunCancellation::default()).unwrap();
        assert_eq!(
            cache
                .promote(
                    &other_key,
                    other.path(),
                    ContentIdentity {
                        generation: Some("etag-2".to_owned()),
                        ..identity(11)
                    },
                    11,
                    &other_sha,
                    &RunCancellation::default(),
                )
                .unwrap(),
            FilePayloadCachePromotion::SkippedCapacity
        );
        assert_eq!(hit.source.identity(), &identity(11));
        drop(hit);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let object = cache.shared.object_path(&key);
            fs::set_permissions(&object, fs::Permissions::from_mode(0o600)).unwrap();
            fs::write(&object, b"payload-two").unwrap();
            fs::set_permissions(&object, fs::Permissions::from_mode(0o400)).unwrap();
        }
        assert!(matches!(
            cache
                .lookup(
                    &key,
                    "https://data.example/events.parquet",
                    11,
                    &RunCancellation::default(),
                    crate::test_execution_services().memory(),
                )
                .unwrap(),
            FilePayloadCacheLookup::Miss
        ));
        assert_eq!(
            cache
                .promote(
                    &other_key,
                    other.path(),
                    ContentIdentity {
                        generation: Some("etag-2".to_owned()),
                        ..identity(11)
                    },
                    11,
                    &other_sha,
                    &RunCancellation::default(),
                )
                .unwrap(),
            FilePayloadCachePromotion::Stored
        );
    }

    #[test]
    fn cache_registry_reuses_one_root_and_rejects_conflicting_policy() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("cache");
        let first = FilePayloadCache::new(&root, 2, 64).unwrap();
        let second = FilePayloadCache::new(&root, 2, 64).unwrap();
        assert!(Arc::ptr_eq(&first.shared, &second.shared));
        assert!(FilePayloadCache::new(root, 3, 64).is_err());
    }

    #[test]
    fn cache_rebuilds_persistent_state_after_process_local_authority_drops() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("cache");
        let key = FilePayloadCacheKey::new(format!("sha256:{:064x}", 3)).unwrap();
        {
            let cache = FilePayloadCache::new(&root, 2, 64).unwrap();
            let staged = stage(&cache, b"persistent");
            let sha256 = hash_file_cancellable(staged.path(), &RunCancellation::default()).unwrap();
            assert_eq!(
                cache
                    .promote(
                        &key,
                        staged.path(),
                        identity(10),
                        10,
                        &sha256,
                        &RunCancellation::default(),
                    )
                    .unwrap(),
                FilePayloadCachePromotion::Stored
            );
        }

        let reopened = FilePayloadCache::new(root, 2, 64).unwrap();
        assert!(matches!(
            reopened
                .lookup(
                    &key,
                    "https://data.example/events.parquet",
                    10,
                    &RunCancellation::default(),
                    crate::test_execution_services().memory(),
                )
                .unwrap(),
            FilePayloadCacheLookup::Hit(_)
        ));
    }
}
