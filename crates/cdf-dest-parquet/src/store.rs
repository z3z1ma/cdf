use crate::*;

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
        Self::new_object_store(Arc::new(store), "")
    }

    pub(crate) fn new_object_store(
        store: Arc<dyn ObjectStore>,
        root_prefix: impl Into<String>,
    ) -> Result<Self> {
        let root_prefix = normalize_prefix(root_prefix.into())?;
        Ok(Self { store, root_prefix })
    }

    pub(crate) fn put(&self, runtime: &Runtime, key: &str, bytes: Vec<u8>) -> Result<StoredObject> {
        let byte_count = bytes.len() as u64;
        let path = self.path(key)?;
        let put: PutResult = runtime
            .block_on(self.store.put(&path, PutPayload::from(bytes)))
            .map_err(|error| store_error(format!("put {key}"), error))?;
        Ok(StoredObject {
            byte_count,
            e_tag: put.e_tag,
        })
    }

    pub(crate) fn put_create(
        &self,
        runtime: &Runtime,
        key: &str,
        bytes: Vec<u8>,
    ) -> Result<CreateObjectOutcome> {
        let byte_count = bytes.len() as u64;
        let path = self.path(key)?;
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match runtime.block_on(self.store.put_opts(&path, PutPayload::from(bytes), options)) {
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
        runtime: &Runtime,
        key: &str,
        bytes: Vec<u8>,
    ) -> Result<StoredObject> {
        let byte_count = bytes.len() as u64;
        match self.put_create(runtime, key, bytes.clone())? {
            CreateObjectOutcome::Created(stored) => Ok(stored),
            CreateObjectOutcome::AlreadyExists => {
                let existing = self.get_required(runtime, key)?;
                if existing != bytes {
                    return Err(CdfError::destination(format!(
                        "immutable object {key} already exists with different bytes"
                    )));
                }
                Ok(StoredObject {
                    byte_count,
                    e_tag: self.etag(runtime, key)?,
                })
            }
        }
    }

    pub(crate) fn get_optional(&self, runtime: &Runtime, key: &str) -> Result<Option<Vec<u8>>> {
        let path = self.path(key)?;
        match runtime.block_on(self.store.get(&path)) {
            Ok(result) => runtime
                .block_on(result.bytes())
                .map(|bytes| Some(bytes.to_vec()))
                .map_err(|error| store_error(format!("read {key}"), error)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(store_error(format!("get {key}"), error)),
        }
    }

    pub(crate) fn get_required(&self, runtime: &Runtime, key: &str) -> Result<Vec<u8>> {
        self.get_optional(runtime, key)?
            .ok_or_else(|| CdfError::data(format!("object {key} is missing")))
    }

    pub(crate) fn exists(&self, runtime: &Runtime, key: &str) -> Result<bool> {
        let path = self.path(key)?;
        match runtime.block_on(self.store.head(&path)) {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(error) => Err(store_error(format!("head {key}"), error)),
        }
    }

    pub(crate) fn etag(&self, runtime: &Runtime, key: &str) -> Result<Option<String>> {
        let path = self.path(key)?;
        runtime
            .block_on(self.store.head(&path))
            .map(|meta| meta.e_tag)
            .map_err(|error| store_error(format!("head {key}"), error))
    }

    #[cfg(test)]
    pub(crate) fn delete(&self, runtime: &Runtime, key: &str) -> Result<()> {
        let path = self.path(key)?;
        runtime
            .block_on(self.store.delete(&path))
            .map_err(|error| store_error(format!("delete {key}"), error))
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

pub(crate) fn now_ms() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| CdfError::internal(format!("system clock before epoch: {error}")))?;
    i64::try_from(duration.as_millis())
        .map_err(|_| CdfError::internal("system time does not fit i64 milliseconds"))
}
