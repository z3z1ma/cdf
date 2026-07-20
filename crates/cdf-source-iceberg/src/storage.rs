use std::{collections::BTreeMap, fmt, ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use cdf_kernel::{CdfError, ErrorKind as CdfErrorKind, Result};
use cdf_object_access::{FileIdentityMetadata, FileTransportControl};
use cdf_runtime::{ByteExtent, ByteSource, RunCancellation, artifact_hash};
use futures_util::stream::BoxStream;
use iceberg::{
    Error as IcebergError, ErrorKind as IcebergErrorKind,
    io::{
        FileIO, FileIOBuilder, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage,
        StorageConfig, StorageFactory,
    },
};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _, ser::Error as _};
use sha2::{Digest, Sha256};

use crate::{
    IcebergCatalogContext, IcebergScanTask, IcebergSourceOptions, catalog::transport_resource,
};

#[derive(Clone)]
struct PreparedIcebergObject {
    identity: FileIdentityMetadata,
    source: Arc<dyn ByteSource>,
}

impl fmt::Debug for PreparedIcebergObject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedIcebergObject")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub(crate) struct CdfIcebergStorage {
    objects: Arc<BTreeMap<String, PreparedIcebergObject>>,
    cancellation: RunCancellation,
}

impl fmt::Debug for CdfIcebergStorage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CdfIcebergStorage")
            .field("object_count", &self.objects.len())
            .finish_non_exhaustive()
    }
}

impl Serialize for CdfIcebergStorage {
    fn serialize<S>(&self, _serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Err(S::Error::custom(
            "CDF Iceberg storage contains invocation-local capability authority and cannot be serialized",
        ))
    }
}

impl<'de> Deserialize<'de> for CdfIcebergStorage {
    fn deserialize<D>(_deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Err(D::Error::custom(
            "CDF Iceberg storage must be injected by the execution host",
        ))
    }
}

#[derive(Clone)]
struct CdfIcebergStorageFactory {
    storage: CdfIcebergStorage,
}

impl fmt::Debug for CdfIcebergStorageFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CdfIcebergStorageFactory")
            .field("storage", &self.storage)
            .finish()
    }
}

impl Serialize for CdfIcebergStorageFactory {
    fn serialize<S>(&self, _serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Err(S::Error::custom(
            "CDF Iceberg storage factory is invocation-local and cannot be serialized",
        ))
    }
}

impl<'de> Deserialize<'de> for CdfIcebergStorageFactory {
    fn deserialize<D>(_deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Err(D::Error::custom(
            "CDF Iceberg storage factory must be injected by the execution host",
        ))
    }
}

#[derive(Clone, Debug)]
struct PlannedObject {
    size_bytes: u64,
    task_generation: String,
}

pub(crate) fn prepare_task_file_io(
    context: &IcebergCatalogContext,
    source: &IcebergSourceOptions,
    task: &IcebergScanTask,
    cancellation: RunCancellation,
) -> Result<(FileIO, String)> {
    cancellation.check()?;
    let mut planned = BTreeMap::<String, PlannedObject>::new();
    insert_planned_object(
        &mut planned,
        &task.data_file.path,
        task.data_file.file_size_bytes,
        &task.data_file.object_generation,
    )?;
    for delete in &task.deletes {
        insert_planned_object(
            &mut planned,
            &delete.path,
            delete.file_size_bytes,
            &delete.object_generation,
        )?;
    }

    let control = FileTransportControl::new(cancellation.clone(), None);
    let memory = context.execution.memory();
    let mut objects = BTreeMap::new();
    let mut generation_hasher = Sha256::new();
    for (path, planned) in planned {
        cancellation.check()?;
        let logical = transport_resource(&path, source, None)?;
        let observation = context
            .object_access
            .metadata(&context.egress, &logical, &control)?;
        let access = observation.access_resource(&logical);
        let identity = observation.into_identity();
        identity.validate()?;
        if identity.size_bytes != Some(planned.size_bytes) {
            return Err(CdfError::data(format!(
                "Iceberg object `{path}` has {} bytes but its immutable manifest entry requires {}",
                identity
                    .size_bytes
                    .map_or_else(|| "unknown".to_owned(), |value| value.to_string()),
                planned.size_bytes
            )));
        }
        let byte_source = context.object_access.open_byte_source(
            &context.egress,
            &access,
            &identity,
            Arc::clone(&memory),
        )?;
        if !byte_source.capabilities().exact_ranges {
            return Err(CdfError::contract(format!(
                "Iceberg Parquet object `{path}` requires a generation-bound exact-range transport; use a seekable local/object-store source or enable the bounded HTTP spool controller"
            )));
        }
        if byte_source.identity().size_bytes != Some(planned.size_bytes) {
            return Err(CdfError::data(format!(
                "Iceberg object `{path}` byte source does not preserve its manifest size authority"
            )));
        }
        generation_hasher.update(artifact_hash(&identity)?.as_bytes());
        objects.insert(
            path,
            PreparedIcebergObject {
                identity,
                source: byte_source,
            },
        );
    }
    let storage = CdfIcebergStorage {
        objects: Arc::new(objects),
        cancellation,
    };
    let generation_hash = format!("sha256:{}", hex::encode(generation_hasher.finalize()));
    Ok((
        FileIOBuilder::new(Arc::new(CdfIcebergStorageFactory { storage })).build(),
        generation_hash,
    ))
}

fn insert_planned_object(
    objects: &mut BTreeMap<String, PlannedObject>,
    path: &str,
    size_bytes: u64,
    task_generation: &str,
) -> Result<()> {
    let candidate = PlannedObject {
        size_bytes,
        task_generation: task_generation.to_owned(),
    };
    if let Some(existing) = objects.get(path) {
        if existing.size_bytes != candidate.size_bytes
            || existing.task_generation != candidate.task_generation
        {
            return Err(CdfError::data(format!(
                "Iceberg task assigns conflicting immutable identities to object `{path}`"
            )));
        }
        return Ok(());
    }
    objects.insert(path.to_owned(), candidate);
    Ok(())
}

#[typetag::serde(name = "cdf-iceberg-readonly-v1")]
#[async_trait]
impl Storage for CdfIcebergStorage {
    async fn exists(&self, path: &str) -> iceberg::Result<bool> {
        self.cancellation.check().map_err(to_iceberg_error)?;
        Ok(self.objects.contains_key(path))
    }

    async fn metadata(&self, path: &str) -> iceberg::Result<FileMetadata> {
        let object = self.object(path)?;
        Ok(FileMetadata {
            size: object
                .identity
                .size_bytes
                .expect("prepared Iceberg object has exact size"),
        })
    }

    async fn read(&self, path: &str) -> iceberg::Result<Bytes> {
        Err(IcebergError::new(
            IcebergErrorKind::FeatureUnsupported,
            format!(
                "whole-object buffering is forbidden for CDF Iceberg payload `{path}`; use ranged FileRead"
            ),
        ))
    }

    async fn reader(&self, path: &str) -> iceberg::Result<Box<dyn FileRead>> {
        let object = self.object(path)?;
        Ok(Box::new(CdfIcebergFileRead {
            source: Arc::clone(&object.source),
            size_bytes: object
                .identity
                .size_bytes
                .expect("prepared Iceberg object has exact size"),
            cancellation: self.cancellation.clone(),
        }))
    }

    async fn write(&self, _path: &str, _bs: Bytes) -> iceberg::Result<()> {
        Err(read_only_error())
    }

    async fn writer(&self, _path: &str) -> iceberg::Result<Box<dyn FileWrite>> {
        Err(read_only_error())
    }

    async fn delete(&self, _path: &str) -> iceberg::Result<()> {
        Err(read_only_error())
    }

    async fn delete_prefix(&self, _path: &str) -> iceberg::Result<()> {
        Err(read_only_error())
    }

    async fn delete_stream(&self, _paths: BoxStream<'static, String>) -> iceberg::Result<()> {
        Err(read_only_error())
    }

    fn new_input(&self, path: &str) -> iceberg::Result<InputFile> {
        self.object(path)?;
        Ok(InputFile::new(Arc::new(self.clone()), path.to_owned()))
    }

    fn new_output(&self, _path: &str) -> iceberg::Result<OutputFile> {
        Err(read_only_error())
    }
}

impl CdfIcebergStorage {
    fn object(&self, path: &str) -> iceberg::Result<&PreparedIcebergObject> {
        self.cancellation.check().map_err(to_iceberg_error)?;
        self.objects.get(path).ok_or_else(|| {
            IcebergError::new(
                IcebergErrorKind::PreconditionFailed,
                "Iceberg reader requested an object absent from the immutable scan task",
            )
            .with_context("path", path)
        })
    }
}

#[typetag::serde(name = "cdf-iceberg-readonly-factory-v1")]
impl StorageFactory for CdfIcebergStorageFactory {
    fn build(&self, _config: &StorageConfig) -> iceberg::Result<Arc<dyn Storage>> {
        Ok(Arc::new(self.storage.clone()))
    }
}

struct CdfIcebergFileRead {
    source: Arc<dyn ByteSource>,
    size_bytes: u64,
    cancellation: RunCancellation,
}

#[async_trait]
impl FileRead for CdfIcebergFileRead {
    async fn read(&self, range: Range<u64>) -> iceberg::Result<Bytes> {
        self.cancellation.check().map_err(to_iceberg_error)?;
        if range.start > range.end || range.end > self.size_bytes {
            return Err(IcebergError::new(
                IcebergErrorKind::DataInvalid,
                "Iceberg reader requested an invalid object range",
            )
            .with_context("range", format!("{}..{}", range.start, range.end))
            .with_context("size_bytes", self.size_bytes.to_string()));
        }
        if range.is_empty() {
            return Ok(Bytes::new());
        }
        let extent =
            ByteExtent::new(range.start, range.end - range.start).map_err(to_iceberg_error)?;
        let payload = self
            .source
            .read_exact_range(extent, self.cancellation.clone())
            .await
            .map_err(to_iceberg_error)?;
        Ok(payload.into_retained_bytes())
    }
}

fn read_only_error() -> IcebergError {
    IcebergError::new(
        IcebergErrorKind::FeatureUnsupported,
        "CDF Iceberg source storage is read-only",
    )
}

pub(crate) fn to_iceberg_error(error: CdfError) -> IcebergError {
    let retryable = matches!(
        error.kind,
        CdfErrorKind::Transient | CdfErrorKind::RateLimited
    );
    let kind = match error.kind {
        CdfErrorKind::Transient | CdfErrorKind::RateLimited | CdfErrorKind::Internal => {
            IcebergErrorKind::Unexpected
        }
        CdfErrorKind::Auth | CdfErrorKind::Contract => IcebergErrorKind::PreconditionFailed,
        CdfErrorKind::Data | CdfErrorKind::Destination => IcebergErrorKind::DataInvalid,
    };
    IcebergError::new(kind, error.message.clone())
        .with_retryable(retryable)
        .with_source(error)
}
