use std::io::Read;

use crate::*;

const MULTIPART_CHUNK_BYTES: usize = 8 * 1024 * 1024;
const MULTIPART_CONCURRENCY: usize = 4;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredObject {
    pub(crate) byte_count: u64,
    pub(crate) e_tag: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CreateObjectOutcome {
    Created(StoredObject),
    AlreadyExists,
}

pub(crate) struct StoreClient {
    store: Arc<dyn ObjectStore>,
    root_prefix: String,
    local_root: Option<PathBuf>,
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

    fn encode(self, value: &str) -> String {
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
        let store = LocalFileSystem::new_with_prefix(root).map_err(|error| {
            CdfError::destination(format!("open object store filesystem: {error}"))
        })?;
        Ok(Self {
            store: Arc::new(store),
            root_prefix: String::new(),
            local_root: Some(root.to_path_buf()),
        })
    }

    pub(crate) fn new_object_store(
        store: Arc<dyn ObjectStore>,
        root_prefix: impl Into<String>,
    ) -> Result<Self> {
        let root_prefix = normalize_prefix(root_prefix.into())?;
        Ok(Self {
            store,
            root_prefix,
            local_root: None,
        })
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
        Ok(StoredObject {
            byte_count,
            e_tag: put.e_tag,
        })
    }

    pub(crate) fn put_encoded_file(
        &self,
        execution: &cdf_runtime::ExecutionServices,
        key: &str,
        encoded: crate::package::EncodedParquetObject,
    ) -> Result<StoredObject> {
        if let Some(root) = &self.local_root {
            return self.install_local_file(execution, root, key, encoded);
        }
        let byte_count = encoded.byte_count;
        let object_path = self.path(key)?;
        let store = Arc::clone(&self.store);
        let file_path = encoded.file.path().to_path_buf();
        let operation = format!("multipart put {key}");
        let reserved_bytes = byte_count
            .min((MULTIPART_CHUNK_BYTES * (MULTIPART_CONCURRENCY + 1)) as u64)
            .max(1);
        let request = cdf_memory::ReservationRequest::new(
            cdf_memory::ConsumerKey::new(
                "parquet-multipart-upload",
                cdf_memory::MemoryClass::Destination,
            )?,
            reserved_bytes,
        )?
        .as_minimum_working_set();
        let memory = execution.memory();
        let put: PutResult = execution.run_io(async move {
            use tokio::io::AsyncReadExt;

            let _encoded = encoded;
            let _lease = cdf_memory::reserve(memory, request).await?;
            let mut file = tokio::fs::File::open(&file_path).await.map_err(|error| {
                CdfError::destination(format!("open {}: {error}", file_path.display()))
            })?;
            let upload = store
                .put_multipart(&object_path)
                .await
                .map_err(|error| store_error(&operation, error))?;
            let mut writer =
                object_store::WriteMultipart::new_with_chunk_size(upload, MULTIPART_CHUNK_BYTES);
            loop {
                if let Err(error) = writer.wait_for_capacity(MULTIPART_CONCURRENCY).await {
                    let _ = writer.abort().await;
                    return Err(store_error(&operation, error));
                }
                let mut chunk = vec![0; MULTIPART_CHUNK_BYTES];
                let read = match file.read(&mut chunk).await {
                    Ok(read) => read,
                    Err(error) => {
                        let _ = writer.abort().await;
                        return Err(CdfError::destination(format!(
                            "read {}: {error}",
                            file_path.display()
                        )));
                    }
                };
                if read == 0 {
                    break;
                }
                chunk.truncate(read);
                writer.put(bytes::Bytes::from(chunk));
            }
            writer
                .finish()
                .await
                .map_err(|error| store_error(operation, error))
        })?;
        Ok(StoredObject {
            byte_count,
            e_tag: put.e_tag,
        })
    }

    fn install_local_file(
        &self,
        execution: &cdf_runtime::ExecutionServices,
        root: &Path,
        key: &str,
        encoded: crate::package::EncodedParquetObject,
    ) -> Result<StoredObject> {
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
        let byte_count = encoded.byte_count;
        let expected_hash = encoded.sha256.clone();
        match encoded.file.persist_noclobber(&destination) {
            Ok(file) => file.sync_all().map_err(|error| {
                CdfError::destination(format!("sync {}: {error}", destination.display()))
            })?,
            Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                let actual_hash = sha256_file(&destination)?;
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
        fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| {
                CdfError::destination(format!("sync {}: {error}", parent.display()))
            })?;
        Ok(StoredObject {
            byte_count,
            e_tag: self.etag(execution, key)?,
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
            Ok(put) => Ok(CreateObjectOutcome::Created(StoredObject {
                byte_count,
                e_tag: put.e_tag,
            })),
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

    #[cfg(test)]
    pub(crate) fn delete(
        &self,
        execution: &cdf_runtime::ExecutionServices,
        key: &str,
    ) -> Result<()> {
        let path = self.path(key)?;
        let store = Arc::clone(&self.store);
        let operation = format!("delete {key}");
        execution.run_io(async move {
            store
                .delete(&path)
                .await
                .map_err(|error| store_error(operation, error))
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

pub(crate) fn segment_object_key(
    encoder: ObjectKeyEncoder,
    target: &TargetName,
    token: &cdf_kernel::IdempotencyToken,
    segment_id: &cdf_kernel::SegmentId,
) -> String {
    format!(
        "targets/{}/packages/{}/data/{}.parquet",
        encoder.encode(target.as_str()),
        encoder.encode(token.as_str()),
        encoder.encode(segment_id.as_str())
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

pub(crate) fn replace_pointer_key(encoder: ObjectKeyEncoder, target: &TargetName) -> String {
    format!("targets/{}/current.json", encoder.encode(target.as_str()))
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

pub(crate) fn now_ms() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| CdfError::internal(format!("system clock before epoch: {error}")))?;
    i64::try_from(duration.as_millis())
        .map_err(|_| CdfError::internal("system time does not fit i64 milliseconds"))
}
