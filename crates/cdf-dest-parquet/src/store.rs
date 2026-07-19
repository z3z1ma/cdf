use std::{
    fs::OpenOptions,
    future::Future,
    io::{Read, Write},
};

use crate::*;
use futures_util::TryStreamExt;

const VERIFY_RANGE_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredObject {
    pub(crate) byte_count: u64,
    pub(crate) e_tag: Option<String>,
    pub(crate) provider_generation: Option<ContentProviderGeneration>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ListedObject {
    pub(crate) key: String,
    pub(crate) byte_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ObjectDigest {
    pub(crate) byte_count: u64,
    pub(crate) sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CreateObjectOutcome {
    Created(StoredObject),
    AlreadyExists,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VersionedObject {
    pub(crate) bytes: Vec<u8>,
    version: UpdateVersion,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CompareAndSwapOutcome {
    Written(StoredObject),
    Conflict,
}

#[derive(Clone)]
pub(crate) struct StoreClient {
    store: Arc<dyn ObjectStore>,
    namespace: ContentStoreNamespace,
    root_prefix: String,
    local_root: Option<PathBuf>,
}

pub(crate) struct StoreContentDeleter<'a> {
    store: &'a StoreClient,
}

impl cdf_runtime::ConditionalContentDeleter for StoreContentDeleter<'_> {
    fn store_namespace(&self) -> &cdf_kernel::ContentStoreNamespace {
        self.store.namespace()
    }

    fn delete_if_generation(
        &self,
        content: &cdf_kernel::ImmutableContentIdentity,
    ) -> Result<cdf_runtime::ConditionalContentDeleteOutcome> {
        if &content.store_namespace != self.store.namespace() {
            return Err(CdfError::contract(
                "conditional content delete crossed object-store namespaces",
            ));
        }
        let Some(root) = &self.store.local_root else {
            // object_store 0.12 has no conditional-delete contract. Remote providers must inject
            // an exact-generation capability rather than emulating it with HEAD then DELETE.
            return Ok(cdf_runtime::ConditionalContentDeleteOutcome::Unsupported);
        };
        if content.digest.algorithm.as_str() != "sha256" {
            return Ok(cdf_runtime::ConditionalContentDeleteOutcome::Unsupported);
        }
        let path = root.join(self.store.path(content.object_key.as_str())?.as_ref());
        let _lock = lock_content_path(root, &path)?;
        if !path.exists() {
            return Ok(cdf_runtime::ConditionalContentDeleteOutcome::AlreadyAbsent);
        }
        let actual = sha256_file(&path)?;
        let expected_generation = format!("sha256:{actual}");
        if content
            .provider_generation
            .as_ref()
            .map(|value| value.as_str())
            != Some(expected_generation.as_str())
            || content.digest.value.as_str() != actual
        {
            return Ok(cdf_runtime::ConditionalContentDeleteOutcome::GenerationMismatch);
        }
        fs::remove_file(&path).map_err(|error| {
            CdfError::destination(format!("delete {}: {error}", path.display()))
        })?;
        Ok(cdf_runtime::ConditionalContentDeleteOutcome::Deleted)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ObjectKeyEncoder {
    policy: ObjectKeyPolicy,
}

impl ObjectKeyEncoder {
    pub(crate) fn from_capabilities(
        capabilities: &cdf_kernel::DestinationProtocolCapabilities,
    ) -> Result<Self> {
        let rules = capabilities.object_key_rules().ok_or_else(|| {
            CdfError::contract("Parquet object-key construction requires typed object-key rules")
        })?;
        rules.validate()?;
        let policy = match rules.policy {
            ObjectKeyPolicy::ComponentV1 => ObjectKeyPolicy::ComponentV1,
        };
        Ok(Self { policy })
    }

    pub(crate) fn encode(self, value: &str) -> String {
        match self.policy {
            ObjectKeyPolicy::ComponentV1 => encode_component_v1(value),
        }
    }
}

impl StoreClient {
    pub(crate) fn new_filesystem(root: &Path) -> Result<Self> {
        fs::create_dir_all(root).map_err(|error| {
            CdfError::destination(format!("create {}: {error}", root.display()))
        })?;
        let canonical_root = fs::canonicalize(root).map_err(|error| {
            CdfError::destination(format!("canonicalize {}: {error}", root.display()))
        })?;
        let store = LocalFileSystem::new_with_prefix(root)
            .map(|store| store.with_fsync(true))
            .map_err(|error| {
                CdfError::destination(format!("open object store filesystem: {error}"))
            })?;
        Ok(Self {
            store: Arc::new(store),
            namespace: ContentStoreNamespace::new(format!(
                "parquet-filesystem-sha256:{}",
                hex::encode(Sha256::digest(canonical_root.to_string_lossy().as_bytes()))
            ))?,
            root_prefix: String::new(),
            local_root: Some(canonical_root.clone()),
        })
    }

    pub(crate) fn new_object_store(
        namespace: ContentStoreNamespace,
        store: Arc<dyn ObjectStore>,
        root_prefix: impl Into<String>,
    ) -> Result<Self> {
        let root_prefix = normalize_prefix(root_prefix.into())?;
        Ok(Self {
            store,
            namespace,
            root_prefix,
            local_root: None,
        })
    }

    pub(crate) fn namespace(&self) -> &ContentStoreNamespace {
        &self.namespace
    }

    pub(crate) fn content_deleter(&self) -> StoreContentDeleter<'_> {
        StoreContentDeleter { store: self }
    }

    pub(crate) fn staging_file(&self) -> Result<tempfile::NamedTempFile> {
        match &self.local_root {
            Some(root) => {
                let staging = root.join(".cdf-staging");
                fs::create_dir_all(&staging).map_err(|error| {
                    CdfError::destination(format!("create {}: {error}", staging.display()))
                })?;
                tempfile::NamedTempFile::new_in(&staging).map_err(|error| {
                    CdfError::destination(format!(
                        "create Parquet staging file under {}: {error}",
                        staging.display()
                    ))
                })
            }
            None => tempfile::NamedTempFile::new().map_err(|error| {
                CdfError::destination(format!("create Parquet staging file: {error}"))
            }),
        }
    }

    pub(crate) fn put(
        &self,
        execution: &cdf_runtime::ExecutionServices,
        key: &str,
        bytes: Vec<u8>,
    ) -> Result<StoredObject> {
        let byte_count = bytes.len() as u64;
        let path = self.path(key)?;
        let store = Arc::clone(&self.store);
        let operation = format!("put {key}");
        let put: PutResult = execution.run_io(async move {
            store
                .put(&path, PutPayload::from(bytes))
                .await
                .map_err(|error| store_error(operation, error))
        })?;
        let provider_generation = provider_generation(&put.e_tag, &put.version)?;
        Ok(StoredObject {
            byte_count,
            e_tag: put.e_tag,
            provider_generation,
        })
    }

    pub(crate) fn put_encoded_file(
        &self,
        execution: &cdf_runtime::ExecutionServices,
        key: &str,
        encoded: crate::package::EncodedParquetObject,
        mutation_guard: &cdf_runtime::StagingMutationGuard,
        cancellation: &cdf_runtime::RunCancellation,
    ) -> Result<StoredObject> {
        cancellation.check()?;
        mutation_guard.assert_current()?;
        if let Some(root) = &self.local_root {
            let stored = self.install_local_file(
                execution,
                root,
                key,
                encoded,
                mutation_guard,
                cancellation,
            )?;
            cancellation.check()?;
            mutation_guard.assert_current()?;
            return Ok(stored);
        }
        let byte_count = encoded.byte_count;
        let expected_hash = encoded.sha256.clone();
        let object_path = self.path(key)?;
        let store = Arc::clone(&self.store);
        let file_path = encoded.file.path().to_path_buf();
        let operation = format!("atomic put {key}");
        let request = cdf_memory::ReservationRequest::new(
            cdf_memory::ConsumerKey::new(
                "parquet-atomic-object-put",
                cdf_memory::MemoryClass::Destination,
            )?,
            byte_count.max(1),
        )?
        .as_minimum_working_set();
        let memory = execution.memory();
        let async_mutation_guard = mutation_guard.clone();
        let async_cancellation = cancellation.clone();
        let put: object_store::Result<PutResult> = execution.run_io(async move {
            let _encoded = encoded;
            let _lease = await_fenced(
                &async_cancellation,
                &async_mutation_guard,
                cdf_memory::reserve(memory, request),
            )
            .await?;
            let bytes = await_fenced(&async_cancellation, &async_mutation_guard, async {
                tokio::fs::read(&file_path).await.map_err(|error| {
                    CdfError::destination(format!("read {}: {error}", file_path.display()))
                })
            })
            .await?;
            if bytes.len() as u64 != byte_count {
                return Err(CdfError::destination(format!(
                    "encoded Parquet object changed size from {byte_count} to {} before publication",
                    bytes.len()
                )));
            }
            let options = PutOptions {
                mode: PutMode::Create,
                ..PutOptions::default()
            };
            await_fenced(&async_cancellation, &async_mutation_guard, async {
                Ok(store
                    .put_opts(&object_path, PutPayload::from(bytes), options)
                    .await)
            })
            .await
        })?;
        match put {
            Ok(put) => {
                let provider_generation = provider_generation(&put.e_tag, &put.version)?;
                Ok(StoredObject {
                    byte_count,
                    e_tag: put.e_tag,
                    provider_generation,
                })
            }
            Err(object_store::Error::AlreadyExists { .. })
            | Err(object_store::Error::Precondition { .. }) => {
                mutation_guard.assert_current()?;
                let digest = self.digest(execution, key)?;
                mutation_guard.assert_current()?;
                if digest.byte_count != byte_count || digest.sha256 != expected_hash {
                    return Err(CdfError::destination(format!(
                        "immutable Parquet object {key} already exists with different bytes"
                    )));
                }
                let metadata = self.object_metadata(execution, key)?;
                Ok(StoredObject {
                    byte_count,
                    e_tag: metadata.e_tag.clone(),
                    provider_generation: provider_generation(&metadata.e_tag, &metadata.version)?,
                })
            }
            Err(error) => Err(store_error(operation, error)),
        }
    }

    fn install_local_file(
        &self,
        execution: &cdf_runtime::ExecutionServices,
        root: &Path,
        key: &str,
        encoded: crate::package::EncodedParquetObject,
        mutation_guard: &cdf_runtime::StagingMutationGuard,
        cancellation: &cdf_runtime::RunCancellation,
    ) -> Result<StoredObject> {
        cancellation.check()?;
        mutation_guard.assert_current()?;
        let object_path = self.path(key)?;
        let destination = root.join(object_path.as_ref());
        let parent = destination.parent().ok_or_else(|| {
            CdfError::destination(format!(
                "Parquet object {} has no parent directory",
                destination.display()
            ))
        })?;
        fs::create_dir_all(parent).map_err(|error| {
            CdfError::destination(format!("create {}: {error}", parent.display()))
        })?;
        let _content_lock = lock_content_path(root, &destination)?;
        mutation_guard.assert_current()?;
        let byte_count = encoded.byte_count;
        let expected_hash = encoded.sha256.clone();
        match encoded.file.persist_noclobber(&destination) {
            Ok(file) => {
                mutation_guard.assert_current()?;
                file.sync_all().map_err(|error| {
                    CdfError::destination(format!("sync {}: {error}", destination.display()))
                })?;
            }
            Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                mutation_guard.assert_current()?;
                let actual_hash = sha256_file(&destination)?;
                mutation_guard.assert_current()?;
                if actual_hash != expected_hash {
                    return Err(CdfError::destination(format!(
                        "immutable Parquet object {} already exists with hash {} instead of {}",
                        destination.display(),
                        actual_hash,
                        expected_hash
                    )));
                }
            }
            Err(error) => {
                return Err(CdfError::destination(format!(
                    "atomically install {}: {}",
                    destination.display(),
                    error.error
                )));
            }
        }
        mutation_guard.assert_current()?;
        fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| {
                CdfError::destination(format!("sync {}: {error}", parent.display()))
            })?;
        cancellation.check()?;
        mutation_guard.assert_current()?;
        Ok(StoredObject {
            byte_count,
            e_tag: self.etag(execution, key)?,
            provider_generation: Some(ContentProviderGeneration::new(format!(
                "sha256:{expected_hash}"
            ))?),
        })
    }

    pub(crate) fn put_create(
        &self,
        execution: &cdf_runtime::ExecutionServices,
        key: &str,
        bytes: Vec<u8>,
    ) -> Result<CreateObjectOutcome> {
        let byte_count = bytes.len() as u64;
        let path = self.path(key)?;
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        let store = Arc::clone(&self.store);
        let key = key.to_owned();
        match execution.run_io(async move {
            Ok(store
                .put_opts(&path, PutPayload::from(bytes), options)
                .await)
        })? {
            Ok(put) => {
                let provider_generation = provider_generation(&put.e_tag, &put.version)?;
                Ok(CreateObjectOutcome::Created(StoredObject {
                    byte_count,
                    e_tag: put.e_tag,
                    provider_generation,
                }))
            }
            Err(object_store::Error::AlreadyExists { .. })
            | Err(object_store::Error::Precondition { .. }) => {
                Ok(CreateObjectOutcome::AlreadyExists)
            }
            Err(error) => Err(store_error(format!("create {key}"), error)),
        }
    }

    pub(crate) fn put_create_or_verify(
        &self,
        execution: &cdf_runtime::ExecutionServices,
        key: &str,
        bytes: Vec<u8>,
    ) -> Result<StoredObject> {
        let byte_count = bytes.len() as u64;
        match self.put_create(execution, key, bytes.clone())? {
            CreateObjectOutcome::Created(stored) => Ok(stored),
            CreateObjectOutcome::AlreadyExists => {
                let existing = self.get_required(execution, key)?;
                if existing != bytes {
                    return Err(CdfError::destination(format!(
                        "immutable object {key} already exists with different bytes"
                    )));
                }
                Ok(StoredObject {
                    byte_count,
                    e_tag: self.etag(execution, key)?,
                    provider_generation: None,
                })
            }
        }
    }

    pub(crate) fn get_optional(
        &self,
        execution: &cdf_runtime::ExecutionServices,
        key: &str,
    ) -> Result<Option<Vec<u8>>> {
        let path = self.path(key)?;
        let store = Arc::clone(&self.store);
        let key = key.to_owned();
        execution.run_io(async move {
            match store.get(&path).await {
                Ok(result) => result
                    .bytes()
                    .await
                    .map(|bytes| Some(bytes.to_vec()))
                    .map_err(|error| store_error(format!("read {key}"), error)),
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(error) => Err(store_error(format!("get {key}"), error)),
            }
        })
    }

    pub(crate) fn get_required(
        &self,
        execution: &cdf_runtime::ExecutionServices,
        key: &str,
    ) -> Result<Vec<u8>> {
        self.get_optional(execution, key)?
            .ok_or_else(|| CdfError::data(format!("object {key} is missing")))
    }

    pub(crate) fn get_optional_versioned(
        &self,
        execution: &cdf_runtime::ExecutionServices,
        key: &str,
    ) -> Result<Option<VersionedObject>> {
        let path = self.path(key)?;
        let store = Arc::clone(&self.store);
        let key = key.to_owned();
        execution.run_io(async move {
            match store.get(&path).await {
                Ok(result) => {
                    let version = UpdateVersion {
                        e_tag: result.meta.e_tag.clone(),
                        version: result.meta.version.clone(),
                    };
                    result
                        .bytes()
                        .await
                        .map(|bytes| {
                            Some(VersionedObject {
                                bytes: bytes.to_vec(),
                                version,
                            })
                        })
                        .map_err(|error| store_error(format!("read {key}"), error))
                }
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(error) => Err(store_error(format!("get {key}"), error)),
            }
        })
    }

    /// Atomically replaces one small control object iff `expected` is still current.
    ///
    /// Local filesystems serialize cooperating processes through a persistent advisory lock and
    /// publish with a same-directory durable rename. Remote stores use the provider's generation
    /// precondition. A provider that cannot honor conditional update fails closed.
    pub(crate) fn compare_and_swap(
        &self,
        execution: &cdf_runtime::ExecutionServices,
        key: &str,
        expected: Option<&VersionedObject>,
        replacement: Vec<u8>,
    ) -> Result<CompareAndSwapOutcome> {
        if let Some(root) = &self.local_root {
            return self.compare_and_swap_local(root, key, expected, replacement);
        }
        let byte_count = replacement.len() as u64;
        let path = self.path(key)?;
        let mode = match expected {
            Some(expected) => PutMode::Update(expected.version.clone()),
            None => PutMode::Create,
        };
        let options = PutOptions {
            mode,
            ..PutOptions::default()
        };
        let store = Arc::clone(&self.store);
        let operation = format!("compare-and-swap {key}");
        match execution.run_io(async move {
            Ok(store
                .put_opts(&path, PutPayload::from(replacement), options)
                .await)
        })? {
            Ok(put) => {
                let provider_generation = provider_generation(&put.e_tag, &put.version)?;
                Ok(CompareAndSwapOutcome::Written(StoredObject {
                    byte_count,
                    e_tag: put.e_tag,
                    provider_generation,
                }))
            }
            Err(object_store::Error::AlreadyExists { .. })
            | Err(object_store::Error::Precondition { .. })
            | Err(object_store::Error::NotFound { .. }) => Ok(CompareAndSwapOutcome::Conflict),
            Err(error) => Err(store_error(operation, error)),
        }
    }

    fn compare_and_swap_local(
        &self,
        root: &Path,
        key: &str,
        expected: Option<&VersionedObject>,
        replacement: Vec<u8>,
    ) -> Result<CompareAndSwapOutcome> {
        let destination = root.join(self.path(key)?.as_ref());
        let parent = destination.parent().ok_or_else(|| {
            CdfError::destination(format!(
                "Parquet control object {} has no parent directory",
                destination.display()
            ))
        })?;
        fs::create_dir_all(parent).map_err(|error| {
            CdfError::destination(format!("create {}: {error}", parent.display()))
        })?;
        let filename = destination
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .ok_or_else(|| CdfError::destination("Parquet control object filename is invalid"))?;
        let lock_path = parent.join(format!(".{filename}.cdf-cas.lock"));
        let lock = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|error| {
                CdfError::destination(format!("open {}: {error}", lock_path.display()))
            })?;
        lock.lock().map_err(|error| {
            CdfError::destination(format!("lock {}: {error}", lock_path.display()))
        })?;
        let observed = match fs::read(&destination) {
            Ok(bytes) => Some(bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => {
                return Err(CdfError::destination(format!(
                    "read {}: {error}",
                    destination.display()
                )));
            }
        };
        if observed.as_deref() != expected.map(|expected| expected.bytes.as_slice()) {
            return Ok(CompareAndSwapOutcome::Conflict);
        }
        let byte_count = replacement.len() as u64;
        let mut temporary = tempfile::NamedTempFile::new_in(parent).map_err(|error| {
            CdfError::destination(format!(
                "create control object under {}: {error}",
                parent.display()
            ))
        })?;
        temporary.write_all(&replacement).map_err(|error| {
            CdfError::destination(format!(
                "write control object {}: {error}",
                destination.display()
            ))
        })?;
        temporary.as_file().sync_all().map_err(|error| {
            CdfError::destination(format!(
                "sync control object {}: {error}",
                destination.display()
            ))
        })?;
        match expected {
            Some(_) => temporary.persist(&destination).map_err(|error| {
                CdfError::destination(format!(
                    "atomically replace {}: {}",
                    destination.display(),
                    error.error
                ))
            })?,
            None => match temporary.persist_noclobber(&destination) {
                Ok(file) => file,
                Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                    return Ok(CompareAndSwapOutcome::Conflict);
                }
                Err(error) => {
                    return Err(CdfError::destination(format!(
                        "atomically create {}: {}",
                        destination.display(),
                        error.error
                    )));
                }
            },
        };
        fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| {
                CdfError::destination(format!("sync {}: {error}", parent.display()))
            })?;
        Ok(CompareAndSwapOutcome::Written(StoredObject {
            byte_count,
            e_tag: None,
            provider_generation: None,
        }))
    }

    pub(crate) fn exists(
        &self,
        execution: &cdf_runtime::ExecutionServices,
        key: &str,
    ) -> Result<bool> {
        let path = self.path(key)?;
        let store = Arc::clone(&self.store);
        match execution.run_io(async move { Ok(store.head(&path).await) })? {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(error) => Err(store_error(format!("head {key}"), error)),
        }
    }

    pub(crate) fn digest(
        &self,
        execution: &cdf_runtime::ExecutionServices,
        key: &str,
    ) -> Result<ObjectDigest> {
        if let Some(root) = &self.local_root {
            let path = root.join(self.path(key)?.as_ref());
            let byte_count = match fs::metadata(&path) {
                Ok(metadata) => metadata.len(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Err(CdfError::data(format!("object {key} is missing")));
                }
                Err(error) => {
                    return Err(CdfError::destination(format!(
                        "inspect {}: {error}",
                        path.display()
                    )));
                }
            };
            return Ok(ObjectDigest {
                byte_count,
                sha256: sha256_file(&path)?,
            });
        }
        let path = self.path(key)?;
        let store = Arc::clone(&self.store);
        let memory = execution.memory();
        let key = key.to_owned();
        execution.run_io(async move {
            let metadata = match store.head(&path).await {
                Ok(metadata) => metadata,
                Err(object_store::Error::NotFound { .. }) => {
                    return Err(CdfError::data(format!("object {key} is missing")));
                }
                Err(error) => return Err(store_error(format!("head {key}"), error)),
            };
            let reserved_bytes = metadata.size.clamp(1, VERIFY_RANGE_BYTES);
            let request = cdf_memory::ReservationRequest::new(
                cdf_memory::ConsumerKey::new(
                    "parquet-object-verification",
                    cdf_memory::MemoryClass::Destination,
                )?,
                reserved_bytes,
            )?
            .as_minimum_working_set();
            let _lease = cdf_memory::reserve(memory, request).await?;
            let mut hash = Sha256::new();
            let mut offset = 0_u64;
            while offset < metadata.size {
                let end = offset.saturating_add(VERIFY_RANGE_BYTES).min(metadata.size);
                let bytes = store
                    .get_range(&path, offset..end)
                    .await
                    .map_err(|error| store_error(format!("read {key} at {offset}..{end}"), error))?;
                let observed = u64::try_from(bytes.len())
                    .map_err(|_| CdfError::destination("verification range exceeds u64"))?;
                if observed != end.saturating_sub(offset) {
                    return Err(CdfError::destination(format!(
                        "object {key} returned {observed} bytes for verification range {offset}..{end}"
                    )));
                }
                hash.update(&bytes);
                offset = end;
            }
            Ok(ObjectDigest {
                byte_count: metadata.size,
                sha256: hex::encode(hash.finalize()),
            })
        })
    }

    pub(crate) fn etag(
        &self,
        execution: &cdf_runtime::ExecutionServices,
        key: &str,
    ) -> Result<Option<String>> {
        let path = self.path(key)?;
        let store = Arc::clone(&self.store);
        let operation = format!("head {key}");
        execution
            .run_io(async move {
                store
                    .head(&path)
                    .await
                    .map_err(|error| store_error(operation, error))
            })
            .map(|meta| meta.e_tag)
    }

    fn object_metadata(
        &self,
        execution: &cdf_runtime::ExecutionServices,
        key: &str,
    ) -> Result<object_store::ObjectMeta> {
        let path = self.path(key)?;
        let store = Arc::clone(&self.store);
        let operation = format!("head {key}");
        execution.run_io(async move {
            store
                .head(&path)
                .await
                .map_err(|error| store_error(operation, error))
        })
    }

    pub(crate) fn delete(
        &self,
        execution: &cdf_runtime::ExecutionServices,
        key: &str,
    ) -> Result<()> {
        if let Some(root) = &self.local_root {
            let path = root.join(self.path(key)?.as_ref());
            return match fs::remove_file(&path) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(CdfError::destination(format!(
                    "delete {}: {error}",
                    path.display()
                ))),
            };
        }
        let path = self.path(key)?;
        let store = Arc::clone(&self.store);
        let operation = format!("delete {key}");
        execution.run_io(async move {
            match store.delete(&path).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                Err(error) => Err(store_error(operation, error)),
            }
        })
    }

    pub(crate) fn delete_prefix(
        &self,
        execution: &cdf_runtime::ExecutionServices,
        prefix: &str,
        mutation_guard: &cdf_runtime::StagingMutationGuard,
    ) -> Result<u64> {
        let prefix = self.path(prefix)?;
        let store = Arc::clone(&self.store);
        let mutation_guard = mutation_guard.clone();
        let operation = format!("delete prefix {prefix}");
        execution.run_io(async move {
            let objects = store
                .list(Some(&prefix))
                .try_collect::<Vec<_>>()
                .await
                .map_err(|error| store_error(&operation, error))?;
            let mut removed = 0_u64;
            for object in objects {
                mutation_guard.assert_current()?;
                match store.delete(&object.location).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                        removed = removed.saturating_add(1);
                    }
                    Err(error) => return Err(store_error(&operation, error)),
                }
            }
            mutation_guard.assert_current()?;
            Ok(removed)
        })
    }

    pub(crate) fn delete_prefix_marker_last(
        &self,
        execution: &cdf_runtime::ExecutionServices,
        prefix: &str,
        marker: &str,
        mutation_guard: &cdf_runtime::StagingMutationGuard,
    ) -> Result<u64> {
        let prefix = self.path(prefix)?;
        let marker = self.path(marker)?;
        if !marker.as_ref().starts_with(prefix.as_ref()) {
            return Err(CdfError::contract(
                "staging cleanup marker must be inside its exact prefix",
            ));
        }
        let store = Arc::clone(&self.store);
        let mutation_guard = mutation_guard.clone();
        let operation = format!("delete prefix {prefix} with marker last");
        execution.run_io(async move {
            let mut objects = store
                .list(Some(&prefix))
                .try_collect::<Vec<_>>()
                .await
                .map_err(|error| store_error(&operation, error))?;
            objects.sort_by(|left, right| left.location.cmp(&right.location));
            let marker_present = objects.iter().any(|object| object.location == marker);
            if !marker_present {
                return Err(CdfError::data(format!(
                    "staging cleanup marker {marker} disappeared before payload deletion"
                )));
            }
            let mut removed = 0_u64;
            for object in objects
                .into_iter()
                .filter(|object| object.location != marker)
            {
                mutation_guard.assert_current()?;
                match store.delete(&object.location).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                        removed = removed.saturating_add(1);
                    }
                    Err(error) => return Err(store_error(&operation, error)),
                }
            }
            mutation_guard.assert_current()?;
            match store.delete(&marker).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                    removed = removed.saturating_add(1);
                }
                Err(error) => return Err(store_error(&operation, error)),
            }
            mutation_guard.assert_current()?;
            Ok(removed)
        })
    }

    pub(crate) fn list_prefix(
        &self,
        execution: &cdf_runtime::ExecutionServices,
        prefix: &str,
    ) -> Result<Vec<ListedObject>> {
        let prefix_path = self.path(prefix)?;
        let store = Arc::clone(&self.store);
        let root_prefix = self.root_prefix.clone();
        let operation = format!("list prefix {prefix}");
        execution.run_io(async move {
            let objects = store
                .list(Some(&prefix_path))
                .try_collect::<Vec<_>>()
                .await
                .map_err(|error| store_error(&operation, error))?;
            objects
                .into_iter()
                .map(|object| {
                    let key = object.location.as_ref();
                    let key = if root_prefix.is_empty() {
                        key.to_owned()
                    } else {
                        key.strip_prefix(&format!("{root_prefix}/"))
                            .ok_or_else(|| {
                                CdfError::destination(format!(
                                    "listed object {key} is outside configured root {root_prefix}"
                                ))
                            })?
                            .to_owned()
                    };
                    Ok(ListedObject {
                        key,
                        byte_count: object.size,
                    })
                })
                .collect()
        })
    }

    fn path(&self, key: &str) -> Result<ObjectPath> {
        if key.trim().is_empty() {
            return Err(CdfError::contract("object key cannot be empty"));
        }
        if self.root_prefix.is_empty() {
            Ok(ObjectPath::from(key))
        } else {
            Ok(ObjectPath::from(format!("{}/{}", self.root_prefix, key)))
        }
    }
}

pub(crate) fn package_manifest_key(
    encoder: ObjectKeyEncoder,
    target: &TargetName,
    token: &cdf_kernel::IdempotencyToken,
) -> String {
    format!(
        "targets/{}/packages/{}/manifest.json",
        encoder.encode(target.as_str()),
        encoder.encode(token.as_str())
    )
}

pub(crate) fn data_object_key(
    encoder: ObjectKeyEncoder,
    target: &TargetName,
    sha256: &str,
) -> String {
    format!(
        "targets/{}/objects/sha256/{}.parquet",
        encoder.encode(target.as_str()),
        encoder.encode(sha256)
    )
}

#[cfg(test)]
pub(crate) fn staged_data_object_key(
    encoder: ObjectKeyEncoder,
    target: &TargetName,
    authority_domain_id: &cdf_kernel::LeaseAuthorityDomainId,
    attempt_id: &cdf_runtime::LoadAttemptId,
    fencing_token: u64,
    object_ordinal: u32,
) -> String {
    format!(
        "targets/{}/staging/{}/{}/{}/part-{object_ordinal:08}.parquet",
        encoder.encode(target.as_str()),
        encoder.encode(authority_domain_id.as_str()),
        encoder.encode(attempt_id.as_str()),
        fencing_token
    )
}

pub(crate) fn staged_attempt_prefix(
    encoder: ObjectKeyEncoder,
    target: &TargetName,
    authority_domain_id: &cdf_kernel::LeaseAuthorityDomainId,
    attempt_id: &cdf_runtime::LoadAttemptId,
    fencing_token: u64,
) -> String {
    format!(
        "targets/{}/staging/{}/{}/{}/",
        encoder.encode(target.as_str()),
        encoder.encode(authority_domain_id.as_str()),
        encoder.encode(attempt_id.as_str()),
        fencing_token
    )
}

pub(crate) fn staged_target_prefix(encoder: ObjectKeyEncoder, target: &TargetName) -> String {
    format!("targets/{}/staging/", encoder.encode(target.as_str()))
}

pub(crate) fn staged_attempt_metadata_key(
    encoder: ObjectKeyEncoder,
    target: &TargetName,
    authority_domain_id: &cdf_kernel::LeaseAuthorityDomainId,
    attempt_id: &cdf_runtime::LoadAttemptId,
    fencing_token: u64,
) -> String {
    format!(
        "{}attempt.json",
        staged_attempt_prefix(
            encoder,
            target,
            authority_domain_id,
            attempt_id,
            fencing_token,
        )
    )
}

pub(crate) fn provenance_manifest_key(
    encoder: ObjectKeyEncoder,
    target: &TargetName,
    package_hash: &PackageHash,
) -> String {
    format!(
        "targets/{}/provenance/{}/manifest.json",
        encoder.encode(target.as_str()),
        encoder.encode(package_hash.as_str())
    )
}

pub(crate) fn current_pointer_key(encoder: ObjectKeyEncoder, target: &TargetName) -> String {
    format!("targets/{}/current.json", encoder.encode(target.as_str()))
}

pub(crate) fn replace_settlement_key(
    encoder: ObjectKeyEncoder,
    target: &TargetName,
    token: &cdf_kernel::IdempotencyToken,
) -> String {
    format!(
        "targets/{}/packages/{}/replace.json",
        encoder.encode(target.as_str()),
        encoder.encode(token.as_str())
    )
}

pub(crate) fn package_publication_metadata_key(
    encoder: ObjectKeyEncoder,
    target: &TargetName,
    authority_domain_id: &cdf_kernel::LeaseAuthorityDomainId,
    attempt_id: &cdf_runtime::LoadAttemptId,
    fencing_token: u64,
    token: &cdf_kernel::IdempotencyToken,
) -> String {
    format!(
        "targets/{}/publication-attempts/{}/{}/{}/{}.json",
        encoder.encode(target.as_str()),
        encoder.encode(authority_domain_id.as_str()),
        encoder.encode(attempt_id.as_str()),
        fencing_token,
        encoder.encode(token.as_str())
    )
}

pub(crate) fn publication_attempt_target_prefix(
    encoder: ObjectKeyEncoder,
    target: &TargetName,
) -> String {
    format!(
        "targets/{}/publication-attempts/",
        encoder.encode(target.as_str())
    )
}

pub(crate) fn correction_sidecar_object_key(
    encoder: ObjectKeyEncoder,
    target: &TargetName,
    sha256: &str,
) -> String {
    format!(
        "targets/{}/corrections/objects/{}.json",
        encoder.encode(target.as_str()),
        encoder.encode(sha256)
    )
}

pub(crate) fn correction_sidecar_manifest_key(
    encoder: ObjectKeyEncoder,
    target: &TargetName,
    sha256: &str,
) -> String {
    format!(
        "targets/{}/corrections/manifests/{}.json",
        encoder.encode(target.as_str()),
        encoder.encode(sha256)
    )
}

pub(crate) fn correction_receipt_key(
    encoder: ObjectKeyEncoder,
    target: &TargetName,
    token: &cdf_kernel::IdempotencyToken,
) -> String {
    format!(
        "targets/{}/corrections/receipts/{}.json",
        encoder.encode(target.as_str()),
        encoder.encode(token.as_str())
    )
}

pub(crate) fn version_manifest_key(
    encoder: ObjectKeyEncoder,
    target: &TargetName,
    target_version: &str,
) -> String {
    format!(
        "targets/{}/versions/{}/manifest.json",
        encoder.encode(target.as_str()),
        encoder.encode(target_version)
    )
}

fn normalize_prefix(prefix: String) -> Result<String> {
    let trimmed = prefix.trim_matches('/').to_owned();
    if trimmed.contains("..") {
        return Err(CdfError::contract(
            "object store root prefix cannot contain parent traversal",
        ));
    }
    Ok(trimmed)
}

fn encode_component_v1(value: &str) -> String {
    let mut output = String::new();
    for byte in value.bytes() {
        match byte {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'.' | b'-' | b'_' | b'=' => {
                output.push(byte as char);
            }
            other => {
                output.push('~');
                output.push_str(&format!("{other:02x}"));
            }
        }
    }
    output
}

fn store_error(action: impl Into<String>, error: object_store::Error) -> CdfError {
    CdfError::destination(format!("{}: {error}", action.into()))
}

fn provider_generation(
    e_tag: &Option<String>,
    version: &Option<String>,
) -> Result<Option<ContentProviderGeneration>> {
    version
        .as_ref()
        .map(|value| format!("version:{value}"))
        .or_else(|| e_tag.as_ref().map(|value| format!("etag:{value}")))
        .map(ContentProviderGeneration::new)
        .transpose()
}

async fn await_fenced<T, F>(
    cancellation: &cdf_runtime::RunCancellation,
    mutation_guard: &cdf_runtime::StagingMutationGuard,
    future: F,
) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    cancellation.check()?;
    mutation_guard.assert_current()?;
    let lease_cancellation = mutation_guard.cancellation();
    let result = cancellation
        .await_or_cancel(lease_cancellation.await_or_cancel(future))
        .await?;
    mutation_guard.assert_current()?;
    Ok(result)
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path)
        .map_err(|error| CdfError::destination(format!("open {}: {error}", path.display())))?;
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| CdfError::destination(format!("read {}: {error}", path.display())))?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
    }
    Ok(hex::encode(hash.finalize()))
}

fn lock_content_path(root: &Path, path: &Path) -> Result<fs::File> {
    let lock_dir = root.join(".cdf-locks/content");
    fs::create_dir_all(&lock_dir).map_err(|error| {
        CdfError::destination(format!("create {}: {error}", lock_dir.display()))
    })?;
    let lock_name = hex::encode(Sha256::digest(path.to_string_lossy().as_bytes()));
    let lock_path = lock_dir.join(format!("{lock_name}.lock"));
    let lock = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|error| CdfError::destination(format!("open {}: {error}", lock_path.display())))?;
    lock.lock()
        .map_err(|error| CdfError::destination(format!("lock {}: {error}", lock_path.display())))?;
    Ok(lock)
}

pub(crate) fn now_ms() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| CdfError::internal(format!("system clock before epoch: {error}")))?;
    i64::try_from(duration.as_millis())
        .map_err(|_| CdfError::internal("system time does not fit i64 milliseconds"))
}
