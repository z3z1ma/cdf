use std::{collections::BTreeMap, io::Read, sync::Arc};

use arrow_schema::{DataType, FieldRef, Schema};
use cdf_http::{
    EgressAllowlist, HttpMethod, HttpRequest, HttpResponseBudget, HttpTransport, SecretProvider,
    SecretUri,
};
use cdf_kernel::{BoxFuture, CdfError, Result, SOURCE_POSITION_VERSION, TableSnapshotPosition};
use cdf_memory::{
    AccountedBytes, ConsumerKey, MemoryClass, MemoryCoordinator, MemoryLease, ReservationRequest,
};
use cdf_object_access::{
    FileIdentityMetadata, FileTransport, FileTransportControl, FileTransportLocation,
    FileTransportResource,
};
use cdf_runtime::{ExecutionServices, RunCancellation, SequentialReadRequest, SourceEgressScope};
use flate2::read::GzDecoder;
use futures_util::TryStreamExt;
use iceberg::spec::{FormatVersion, NestedField, Snapshot, TableMetadata};
use serde::Deserialize;
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};

use crate::{
    IcebergCatalogOptions, IcebergResourceOptions, IcebergSnapshotSelector, IcebergTableIdentity,
};

const METADATA_READ_CHUNK_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Clone)]
pub struct IcebergCatalogContext {
    pub object_access: Arc<dyn FileTransport>,
    pub rest_http: Arc<dyn HttpTransport>,
    pub glue: Arc<dyn GlueCatalogClient>,
    pub secrets: Arc<dyn SecretProvider + Send + Sync>,
    pub execution: ExecutionServices,
    pub blocking_lane: cdf_runtime::BlockingLaneSpec,
    pub egress: SourceEgressScope,
    pub project_root: std::path::PathBuf,
}

impl std::fmt::Debug for IcebergCatalogContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergCatalogContext")
            .field("project_root", &self.project_root)
            .field("blocking_lane", &self.blocking_lane)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub struct IcebergCatalogLoadRequest {
    pub source: crate::IcebergSourceOptions,
    pub resource: IcebergResourceOptions,
    pub cancellation: RunCancellation,
}

#[derive(Clone)]
pub struct LoadedIcebergTable {
    pub catalog_identity: String,
    pub resource: IcebergResourceOptions,
    pub metadata_location: String,
    pub metadata_generation: String,
    pub catalog_generation: Option<String>,
    pub metadata: Arc<TableMetadata>,
    pub selected: Option<SelectedIcebergSnapshot>,
    pub arrow_schema: Arc<Schema>,
    pub bytes_read: u64,
    pub objects_read: u64,
    retained: Arc<RetainedMetadata>,
}

impl std::fmt::Debug for LoadedIcebergTable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LoadedIcebergTable")
            .field("catalog_identity", &self.catalog_identity)
            .field("resource", &self.resource.display_name())
            .field("metadata_location", &self.metadata_location)
            .field("metadata_generation", &self.metadata_generation)
            .field("catalog_generation", &self.catalog_generation)
            .field("selected", &self.selected)
            .field("bytes_read", &self.bytes_read)
            .field("objects_read", &self.objects_read)
            .finish_non_exhaustive()
    }
}

impl LoadedIcebergTable {
    pub fn table_identity(&self) -> IcebergTableIdentity {
        IcebergTableIdentity {
            catalog: self.catalog_identity.clone(),
            namespace: self.resource.namespace.clone(),
            table: self.resource.table.clone(),
            table_uuid: self.metadata.uuid().to_string(),
            metadata_location: self.metadata_location.clone(),
            metadata_generation: self.metadata_generation.clone(),
        }
    }

    pub fn retained_bytes(&self) -> u64 {
        self.retained.retained_bytes()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectedIcebergSnapshot {
    pub position: TableSnapshotPosition,
    pub schema_id: i32,
    pub manifest_list: String,
    pub timestamp_ms: i64,
}

struct RetainedMetadata {
    parse_lease: MemoryLease,
}

struct CatalogObservation {
    metadata_location: String,
    catalog_generation: Option<String>,
    metadata_payload: AccountedBytes,
    embedded_metadata: Option<Box<RawValue>>,
    bytes_read: u64,
    objects_read: u64,
}

pub(crate) struct LoadedCatalogObject {
    pub payload: AccountedBytes,
}

pub(crate) fn load_catalog_object(
    context: &IcebergCatalogContext,
    source: &crate::IcebergSourceOptions,
    location: &str,
    expected_size: Option<u64>,
    cancellation: RunCancellation,
) -> Result<LoadedCatalogObject> {
    let resource = transport_resource(location, source, None)?;
    let control = FileTransportControl::new(cancellation.clone(), None);
    let metadata = context
        .object_access
        .metadata(&context.egress, &resource, &control)?;
    let access = metadata.access_resource(&resource);
    let identity = metadata.into_identity();
    if let Some(expected_size) = expected_size
        && identity.size_bytes != Some(expected_size)
    {
        return Err(CdfError::data(format!(
            "Iceberg metadata object `{}` has {} bytes but its parent metadata requires {expected_size}",
            identity.location,
            identity
                .size_bytes
                .map_or_else(|| "unknown".to_owned(), |value| value.to_string())
        )));
    }
    let payload = read_metadata_object(
        context,
        &access,
        &identity,
        source.maximum_metadata_bytes,
        cancellation,
    )?;
    Ok(LoadedCatalogObject { payload })
}

impl RetainedMetadata {
    fn retained_bytes(&self) -> u64 {
        self.parse_lease.bytes()
    }
}

pub trait GlueCatalogClient: Send + Sync {
    fn get_table(&self, request: GlueGetTableRequest) -> BoxFuture<'_, Result<GlueTablePointer>>;
}

#[derive(Clone, Debug)]
pub struct GlueGetTableRequest {
    pub region: String,
    pub catalog_id: Option<String>,
    pub database: String,
    pub table: String,
    pub endpoint: Option<String>,
    pub credentials: Option<SecretUri>,
    pub maximum_response_bytes: u64,
    pub cancellation: RunCancellation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GlueTablePointer {
    pub metadata_location: String,
    pub catalog_generation: Option<String>,
    /// Actual response-body bytes transferred from the Glue metadata plane.
    pub bytes_read: u64,
    /// Retained response bytes reported by the host adapter. The catalog layer charges these
    /// against the shared discovery ledger before retaining the pointer.
    pub retained_bytes: u64,
}

#[derive(Debug, Default)]
pub struct UnsupportedGlueCatalogClient;

impl GlueCatalogClient for UnsupportedGlueCatalogClient {
    fn get_table(&self, _request: GlueGetTableRequest) -> BoxFuture<'_, Result<GlueTablePointer>> {
        Box::pin(async {
            Err(CdfError::contract(
                "AWS Glue catalog support is not installed in this host registry",
            ))
        })
    }
}

#[derive(Default)]
pub struct IcebergCatalogRegistry {
    bindings: BTreeMap<&'static str, Arc<dyn IcebergCatalogBinding>>,
}

impl IcebergCatalogRegistry {
    pub fn standard() -> Result<Self> {
        let mut registry = Self::default();
        registry.register(Arc::new(FilesystemCatalogBinding))?;
        registry.register(Arc::new(RestCatalogBinding))?;
        registry.register(Arc::new(GlueCatalogBinding))?;
        Ok(registry)
    }

    pub fn register(&mut self, binding: Arc<dyn IcebergCatalogBinding>) -> Result<()> {
        if self.bindings.insert(binding.kind(), binding).is_some() {
            return Err(CdfError::contract(
                "Iceberg catalog binding kind is already registered",
            ));
        }
        Ok(())
    }

    pub fn load_table(
        &self,
        request: &IcebergCatalogLoadRequest,
        context: &IcebergCatalogContext,
    ) -> Result<LoadedIcebergTable> {
        request.source.validate()?;
        request.resource.validate()?;
        request.cancellation.check()?;
        let kind = match &request.source.catalog {
            IcebergCatalogOptions::Filesystem { .. } => "filesystem",
            IcebergCatalogOptions::Rest { .. } => "rest",
            IcebergCatalogOptions::Glue { .. } => "glue",
        };
        self.bindings
            .get(kind)
            .ok_or_else(|| {
                CdfError::contract(format!(
                    "Iceberg catalog binding `{kind}` is not registered"
                ))
            })?
            .load_table(request, context)
    }
}

pub trait IcebergCatalogBinding: Send + Sync {
    fn kind(&self) -> &'static str;
    fn load_table(
        &self,
        request: &IcebergCatalogLoadRequest,
        context: &IcebergCatalogContext,
    ) -> Result<LoadedIcebergTable>;
}

struct FilesystemCatalogBinding;

const VERSION_HINT_FILE: &str = "version-hint.text";

impl IcebergCatalogBinding for FilesystemCatalogBinding {
    fn kind(&self) -> &'static str {
        "filesystem"
    }

    fn load_table(
        &self,
        request: &IcebergCatalogLoadRequest,
        context: &IcebergCatalogContext,
    ) -> Result<LoadedIcebergTable> {
        let IcebergCatalogOptions::Filesystem { warehouse } = &request.source.catalog else {
            return Err(CdfError::internal(
                "filesystem binding received another catalog kind",
            ));
        };
        let warehouse = resolve_warehouse(warehouse, &context.project_root)?;
        let metadata_root = join_location(
            &warehouse,
            request
                .resource
                .namespace
                .iter()
                .map(String::as_str)
                .chain([request.resource.table.as_str(), "metadata"]),
        )?;
        let root_resource = transport_resource(&metadata_root, &request.source, None)?;
        let control = FileTransportControl::new(request.cancellation.clone(), None);
        let mut stream = context.object_access.list(
            &context.egress,
            &root_resource,
            request.source.maximum_metadata_files,
            &control,
        )?;
        let identities = context.execution.run_io(async move {
            let mut identities = Vec::new();
            while let Some(identity) = stream.try_next().await? {
                identities.push(identity.into_identity());
            }
            Ok::<_, CdfError>(identities)
        })?;
        let version_hint = identities
            .iter()
            .find(|identity| location_name(&identity.location) == VERSION_HINT_FILE)
            .cloned();
        let (selected, hint_bytes, hint_objects) = match version_hint {
            Some(hint) => {
                let hint_resource = transport_resource(&hint.location, &request.source, None)?;
                let payload = read_metadata_object(
                    context,
                    &hint_resource,
                    &hint,
                    request.source.maximum_metadata_bytes,
                    request.cancellation.clone(),
                )?;
                let bytes = u64::try_from(payload.payload().len()).unwrap_or(u64::MAX);
                let selected = match parse_version_hint(payload.payload())
                    .and_then(|version| select_metadata_file_from_hint(&identities, version))
                {
                    Ok(selected) => selected,
                    Err(_) => select_latest_metadata_file(identities.clone())?,
                };
                (selected, bytes, 1)
            }
            None => (select_latest_metadata_file(identities)?, 0, 0),
        };
        let access = transport_resource(&selected.location, &request.source, None)?;
        let payload = read_metadata_object(
            context,
            &access,
            &selected,
            request.source.maximum_metadata_bytes,
            request.cancellation.clone(),
        )?;
        build_loaded_table(
            request,
            context,
            CatalogObservation {
                metadata_location: selected.location.clone(),
                catalog_generation: metadata_file_version(&selected.location)
                    .map(|version| format!("hadoop-version:{version}")),
                metadata_payload: payload,
                embedded_metadata: None,
                bytes_read: selected.size_bytes.unwrap_or(0).saturating_add(hint_bytes),
                objects_read: 1_u64.saturating_add(hint_objects),
            },
        )
    }
}

struct RestCatalogBinding;

impl IcebergCatalogBinding for RestCatalogBinding {
    fn kind(&self) -> &'static str {
        "rest"
    }

    fn load_table(
        &self,
        request: &IcebergCatalogLoadRequest,
        context: &IcebergCatalogContext,
    ) -> Result<LoadedIcebergTable> {
        let IcebergCatalogOptions::Rest {
            uri,
            warehouse,
            credentials,
        } = &request.source.catalog
        else {
            return Err(CdfError::internal(
                "REST binding received another catalog kind",
            ));
        };
        let authorization = credentials
            .as_ref()
            .map(|reference| {
                context
                    .secrets
                    .resolve(&SecretUri::new(reference.clone())?)?
                    .as_str()
                    .map(|token| format!("Bearer {token}"))
            })
            .transpose()?;
        let allowlist = allowlist(&request.source);
        let config_endpoint = rest_config_endpoint(uri, warehouse.as_deref())?;
        context.egress.authorize(&config_endpoint)?;
        let config_payload = send_rest_request(
            context,
            &allowlist,
            config_endpoint,
            authorization.as_deref(),
            request.source.maximum_metadata_bytes,
            request.cancellation.clone(),
        )?;
        let config_bytes = u64::try_from(config_payload.payload().len())
            .map_err(|_| CdfError::data("Iceberg REST config length exceeds u64"))?;
        let config_parse_lease = reserve_parse_memory(
            context.execution.memory(),
            config_bytes,
            request.source.metadata_parse_amplification_bps,
            "iceberg-rest-config-parse",
        )?;
        let catalog_config: RestCatalogConfigResponse =
            serde_json::from_slice(config_payload.payload()).map_err(|error| {
                CdfError::data(format!("decode Iceberg REST catalog config: {error}"))
            })?;
        let routing = RestCatalogRouting::negotiate(uri, catalog_config)?;
        drop(config_parse_lease);
        drop(config_payload);
        let endpoint =
            rest_table_endpoint(&routing.uri, routing.prefix.as_deref(), &request.resource)?;
        context.egress.authorize(&endpoint)?;
        let payload = send_rest_request(
            context,
            &allowlist,
            endpoint,
            authorization.as_deref(),
            request.source.maximum_metadata_bytes,
            request.cancellation.clone(),
        )?;
        let response_bytes = u64::try_from(payload.payload().len())
            .map_err(|_| CdfError::data("Iceberg REST response length exceeds u64"))?;
        let envelope: RestLoadTableResponse =
            serde_json::from_slice(payload.payload()).map_err(|error| {
                CdfError::data(format!("decode Iceberg REST table response: {error}"))
            })?;
        let metadata_location = envelope.metadata_location.ok_or_else(|| {
            CdfError::data("Iceberg REST table response omitted metadata-location")
        })?;
        build_loaded_table(
            request,
            context,
            CatalogObservation {
                metadata_location,
                catalog_generation: None,
                metadata_payload: payload,
                embedded_metadata: Some(envelope.metadata),
                bytes_read: config_bytes.saturating_add(response_bytes),
                objects_read: 2,
            },
        )
    }
}

fn send_rest_request(
    context: &IcebergCatalogContext,
    allowlist: &EgressAllowlist,
    endpoint: String,
    authorization: Option<&str>,
    maximum_bytes: u64,
    cancellation: RunCancellation,
) -> Result<AccountedBytes> {
    let mut request = HttpRequest::new(HttpMethod::Get, endpoint);
    if let Some(authorization) = authorization {
        request = request.with_header("authorization", authorization);
    }
    let budget = HttpResponseBudget::new(
        maximum_bytes,
        context.execution.memory(),
        Arc::new(move || cancellation.check()),
    )?;
    let rest_http = Arc::clone(&context.rest_http);
    let allowlist = allowlist.clone();
    let response = context.execution.run_io(async move {
        cdf_http::send_with_policy(rest_http.as_ref(), &allowlist, request, budget).await
    })?;
    if response.status != 200 {
        return Err(http_catalog_error(response.status));
    }
    response
        .accounted_body()
        .cloned()
        .ok_or_else(|| CdfError::data("Iceberg REST response omitted its JSON body"))
}

struct GlueCatalogBinding;

impl IcebergCatalogBinding for GlueCatalogBinding {
    fn kind(&self) -> &'static str {
        "glue"
    }

    fn load_table(
        &self,
        request: &IcebergCatalogLoadRequest,
        context: &IcebergCatalogContext,
    ) -> Result<LoadedIcebergTable> {
        let IcebergCatalogOptions::Glue {
            region,
            catalog_id,
            endpoint,
            credentials,
            ..
        } = &request.source.catalog
        else {
            return Err(CdfError::internal(
                "Glue binding received another catalog kind",
            ));
        };
        if request.resource.namespace.len() != 1 {
            return Err(CdfError::contract(
                "AWS Glue maps an Iceberg table to exactly one database namespace component",
            ));
        }
        let credentials = credentials
            .as_ref()
            .map(|value| SecretUri::new(value.clone()))
            .transpose()?;
        if let Some(endpoint) = endpoint {
            context.egress.authorize(endpoint)?;
        }
        let glue = Arc::clone(&context.glue);
        let glue_request = GlueGetTableRequest {
            region: region.clone(),
            catalog_id: catalog_id.clone(),
            database: request.resource.namespace[0].clone(),
            table: request.resource.table.clone(),
            endpoint: endpoint.clone(),
            credentials,
            maximum_response_bytes: request.source.maximum_metadata_bytes,
            cancellation: request.cancellation.clone(),
        };
        let pointer = context
            .execution
            .run_io(async move { glue.get_table(glue_request).await })?;
        request.cancellation.check()?;
        let _pointer_lease = reserve_discovery_memory(
            context.execution.memory(),
            pointer.retained_bytes.max(1),
            "iceberg-glue-pointer",
        )?;
        let access = transport_resource(&pointer.metadata_location, &request.source, None)?;
        let control = FileTransportControl::new(request.cancellation.clone(), None);
        let metadata = context
            .object_access
            .metadata(&context.egress, &access, &control)?;
        let access = metadata.access_resource(&access);
        let identity = metadata.into_identity();
        let payload = read_metadata_object(
            context,
            &access,
            &identity,
            request.source.maximum_metadata_bytes,
            request.cancellation.clone(),
        )?;
        build_loaded_table(
            request,
            context,
            CatalogObservation {
                metadata_location: pointer.metadata_location.clone(),
                catalog_generation: pointer.catalog_generation,
                metadata_payload: payload,
                embedded_metadata: None,
                bytes_read: pointer
                    .bytes_read
                    .saturating_add(identity.size_bytes.unwrap_or(0)),
                objects_read: 2,
            },
        )
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct RestLoadTableResponse {
    metadata_location: Option<String>,
    metadata: Box<RawValue>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RestCatalogConfigResponse {
    #[serde(default)]
    defaults: BTreeMap<String, String>,
    #[serde(default)]
    overrides: BTreeMap<String, String>,
    #[serde(default)]
    endpoints: Option<Vec<String>>,
}

struct RestCatalogRouting {
    uri: String,
    prefix: Option<String>,
}

impl RestCatalogRouting {
    fn negotiate(configured_uri: &str, response: RestCatalogConfigResponse) -> Result<Self> {
        let _advertised_endpoints = response.endpoints;
        let mut properties = response.defaults;
        properties.extend(response.overrides);
        let uri = properties
            .remove("uri")
            .unwrap_or_else(|| configured_uri.to_owned());
        validate_rest_uri("negotiated Iceberg REST catalog URI", &uri)?;
        let prefix = properties.remove("prefix");
        if let Some(prefix) = &prefix {
            validate_rest_prefix(prefix)?;
        }
        Ok(Self { uri, prefix })
    }
}

fn build_loaded_table(
    request: &IcebergCatalogLoadRequest,
    context: &IcebergCatalogContext,
    observation: CatalogObservation,
) -> Result<LoadedIcebergTable> {
    let CatalogObservation {
        metadata_location,
        catalog_generation,
        metadata_payload,
        embedded_metadata,
        bytes_read,
        objects_read,
    } = observation;
    validate_metadata_location(&metadata_location)?;
    let parse_input_bytes = embedded_metadata.as_ref().map_or_else(
        || {
            metadata_json_size(
                &metadata_location,
                metadata_payload.payload(),
                request.source.maximum_metadata_bytes,
            )
        },
        |metadata| {
            u64::try_from(metadata.get().len())
                .map_err(|_| CdfError::data("Iceberg metadata JSON length exceeds u64"))
        },
    )?;
    let parse_lease = reserve_parse_memory(
        context.execution.memory(),
        parse_input_bytes,
        request.source.metadata_parse_amplification_bps,
        "iceberg-metadata-parse",
    )?;
    let (metadata, metadata_generation) = decode_table_metadata(
        &metadata_location,
        metadata_payload.payload(),
        embedded_metadata.as_deref(),
    )?;
    if !matches!(
        metadata.format_version(),
        FormatVersion::V1 | FormatVersion::V2
    ) {
        return Err(CdfError::contract(
            "Iceberg source currently supports table format version 1 or 2; use a v1/v2 snapshot or wait for the v3 capability",
        ));
    }
    let reference_kind = selected_reference_kind(
        &request.resource.selector,
        &metadata_location,
        metadata_payload.payload(),
        embedded_metadata.as_deref(),
    )?;
    drop(embedded_metadata);
    drop(metadata_payload);
    let selected = select_snapshot(
        &request.source.catalog_identity(),
        &request.resource,
        &metadata_location,
        &metadata_generation,
        &metadata,
        reference_kind,
    )?;
    let schema = selected.as_ref().map_or_else(
        || Ok(metadata.current_schema().clone()),
        |selected| {
            metadata
                .schema_by_id(selected.schema_id)
                .cloned()
                .ok_or_else(|| {
                    CdfError::data(format!(
                        "Iceberg selected schema id {} is absent from table metadata",
                        selected.schema_id
                    ))
                })
        },
    )?;
    let arrow_schema = Arc::new(annotated_arrow_schema(schema.as_ref())?);
    let retained = Arc::new(RetainedMetadata { parse_lease });
    Ok(LoadedIcebergTable {
        catalog_identity: request.source.catalog_identity(),
        resource: request.resource.clone(),
        metadata_location,
        metadata_generation,
        catalog_generation,
        metadata: Arc::new(metadata),
        selected,
        arrow_schema,
        bytes_read,
        objects_read,
        retained,
    })
}

fn validate_metadata_location(location: &str) -> Result<()> {
    if location.trim().is_empty()
        || location.chars().any(char::is_control)
        || location.contains(['?', '#'])
    {
        return Err(CdfError::contract(
            "Iceberg metadata location must be nonempty, control-free, and contain no query or fragment; signed URLs belong in runtime credentials, not plan authority",
        ));
    }
    if let Some((_, remainder)) = location.split_once("://")
        && remainder
            .split('/')
            .next()
            .is_some_and(|value| value.contains('@'))
    {
        return Err(CdfError::contract(
            "Iceberg metadata location cannot contain URI user information",
        ));
    }
    Ok(())
}

fn metadata_json_size(location: &str, payload: &[u8], maximum_bytes: u64) -> Result<u64> {
    if !is_gzip_metadata_location(location) {
        return u64::try_from(payload.len())
            .map_err(|_| CdfError::data("Iceberg metadata JSON length exceeds u64"));
    }
    let maximum_plus_one = maximum_bytes
        .checked_add(1)
        .ok_or_else(|| CdfError::contract("maximum_metadata_bytes cannot equal u64::MAX"))?;
    let mut decoder = GzDecoder::new(payload);
    let mut bounded = decoder.by_ref().take(maximum_plus_one);
    let expanded = std::io::copy(&mut bounded, &mut std::io::sink())
        .map_err(|error| CdfError::data(format!("decode gzip Iceberg table metadata: {error}")))?;
    if expanded == 0 || expanded > maximum_bytes {
        return Err(CdfError::data(format!(
            "gzip Iceberg table metadata expands to {expanded} bytes outside the configured 1..={maximum_bytes} byte budget"
        )));
    }
    Ok(expanded)
}

fn decode_table_metadata(
    location: &str,
    payload: &[u8],
    embedded: Option<&RawValue>,
) -> Result<(TableMetadata, String)> {
    let (metadata, digest) = if let Some(raw) = embedded {
        (
            serde_json::from_str(raw.get()).map_err(|error| {
                CdfError::data(format!("validate Iceberg table metadata: {error}"))
            })?,
            Sha256::digest(raw.get().as_bytes()),
        )
    } else if is_gzip_metadata_location(location) {
        let mut reader = HashingReader::new(GzDecoder::new(payload));
        let metadata = serde_json::from_reader(&mut reader)
            .map_err(|error| CdfError::data(format!("validate Iceberg table metadata: {error}")))?;
        // Consume the logical stream through EOF so the generation includes insignificant
        // trailing whitespace and gzip validates its checksum before authority is frozen.
        std::io::copy(&mut reader, &mut std::io::sink()).map_err(|error| {
            CdfError::data(format!("finish gzip Iceberg table metadata: {error}"))
        })?;
        (metadata, reader.finalize())
    } else {
        (
            serde_json::from_slice(payload).map_err(|error| {
                CdfError::data(format!("validate Iceberg table metadata: {error}"))
            })?,
            Sha256::digest(payload),
        )
    };
    Ok((metadata, format!("sha256:{}", hex::encode(digest))))
}

struct HashingReader<R> {
    inner: R,
    hasher: Sha256,
}

impl<R> HashingReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
        }
    }

    fn finalize(self) -> sha2::digest::Output<Sha256> {
        self.hasher.finalize()
    }
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let bytes = self.inner.read(buffer)?;
        self.hasher.update(&buffer[..bytes]);
        Ok(bytes)
    }
}

fn is_gzip_metadata_location(location: &str) -> bool {
    location_name(location).ends_with(".gz.metadata.json")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum IcebergReferenceKind {
    Branch,
    Tag,
}

impl IcebergReferenceKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Branch => "branch",
            Self::Tag => "tag",
        }
    }
}

#[derive(Deserialize)]
struct MetadataReferenceProjection {
    #[serde(default)]
    refs: BTreeMap<String, MetadataReferenceEntry>,
}

#[derive(Deserialize)]
struct MetadataReferenceEntry {
    #[serde(rename = "type")]
    kind: IcebergReferenceKind,
}

fn selected_reference_kind(
    selector: &IcebergSnapshotSelector,
    location: &str,
    payload: &[u8],
    embedded: Option<&RawValue>,
) -> Result<Option<IcebergReferenceKind>> {
    let name = match selector {
        IcebergSnapshotSelector::Branch { name } | IcebergSnapshotSelector::Tag { name } => name,
        _ => return Ok(None),
    };
    let projection: MetadataReferenceProjection = match embedded {
        Some(metadata) => serde_json::from_str(metadata.get()),
        None if is_gzip_metadata_location(location) => {
            serde_json::from_reader(GzDecoder::new(payload))
        }
        None => serde_json::from_slice(payload),
    }
    .map_err(|error| {
        CdfError::data(format!("decode Iceberg table reference authority: {error}"))
    })?;
    Ok(projection.refs.get(name).map(|reference| reference.kind))
}

fn select_snapshot(
    catalog: &str,
    resource: &IcebergResourceOptions,
    metadata_location: &str,
    metadata_generation: &str,
    metadata: &TableMetadata,
    reference_kind: Option<IcebergReferenceKind>,
) -> Result<Option<SelectedIcebergSnapshot>> {
    let snapshot =
        match &resource.selector {
            IcebergSnapshotSelector::Current => metadata.current_snapshot().map(Arc::clone),
            IcebergSnapshotSelector::Branch { name } => {
                validate_reference_kind(reference_kind, name, true)?;
                Some(metadata.snapshot_for_ref(name).cloned().ok_or_else(|| {
                    CdfError::data(format!("Iceberg branch `{name}` does not exist"))
                })?)
            }
            IcebergSnapshotSelector::Tag { name } => {
                validate_reference_kind(reference_kind, name, false)?;
                Some(metadata.snapshot_for_ref(name).cloned().ok_or_else(|| {
                    CdfError::data(format!("Iceberg tag `{name}` does not exist"))
                })?)
            }
            IcebergSnapshotSelector::Snapshot { snapshot_id } => Some(
                metadata
                    .snapshot_by_id(*snapshot_id)
                    .cloned()
                    .ok_or_else(|| {
                        CdfError::data(format!("Iceberg snapshot id {snapshot_id} does not exist"))
                    })?,
            ),
            IcebergSnapshotSelector::Timestamp { timestamp_ms } => metadata
                .history()
                .iter()
                .filter(|entry| entry.timestamp_ms() <= *timestamp_ms)
                .max_by_key(|entry| (entry.timestamp_ms(), entry.snapshot_id))
                .map(|entry| {
                    metadata
                        .snapshot_by_id(entry.snapshot_id)
                        .cloned()
                        .ok_or_else(|| {
                            CdfError::data(format!(
                                "Iceberg history references missing snapshot id {}",
                                entry.snapshot_id
                            ))
                        })
                })
                .transpose()?,
        };
    let Some(snapshot) = snapshot else {
        if matches!(resource.selector, IcebergSnapshotSelector::Current) {
            return Ok(None);
        }
        return Err(CdfError::data(
            "Iceberg selector resolved no snapshot at or before the requested point",
        ));
    };
    if snapshot.snapshot_id() <= 0 || snapshot.sequence_number() < 0 {
        return Err(CdfError::data(
            "Iceberg selected snapshot has invalid id or sequence authority",
        ));
    }
    let schema_id = snapshot
        .schema_id()
        .unwrap_or_else(|| metadata.current_schema_id());
    Ok(Some(selected_snapshot(
        catalog,
        resource,
        metadata_location,
        metadata_generation,
        &snapshot,
        schema_id,
    )))
}

fn selected_snapshot(
    catalog: &str,
    resource: &IcebergResourceOptions,
    metadata_location: &str,
    metadata_generation: &str,
    snapshot: &Snapshot,
    schema_id: i32,
) -> SelectedIcebergSnapshot {
    SelectedIcebergSnapshot {
        position: TableSnapshotPosition {
            version: SOURCE_POSITION_VERSION,
            protocol: "iceberg".to_owned(),
            catalog: catalog.to_owned(),
            namespace: resource.namespace.clone(),
            table: resource.table.clone(),
            selector: resource.selector.position_selector(),
            snapshot_id: snapshot.snapshot_id(),
            sequence_number: snapshot.sequence_number(),
            parent_snapshot_id: snapshot.parent_snapshot_id(),
            metadata_location: metadata_location.to_owned(),
            metadata_generation: metadata_generation.to_owned(),
        },
        schema_id,
        manifest_list: snapshot.manifest_list().to_owned(),
        timestamp_ms: snapshot.timestamp_ms(),
    }
}

fn validate_reference_kind(
    kind: Option<IcebergReferenceKind>,
    name: &str,
    expected_branch: bool,
) -> Result<()> {
    let kind =
        kind.ok_or_else(|| CdfError::data(format!("Iceberg ref `{name}` does not exist")))?;
    let expected = if expected_branch {
        IcebergReferenceKind::Branch
    } else {
        IcebergReferenceKind::Tag
    };
    if kind != expected {
        return Err(CdfError::contract(format!(
            "Iceberg ref `{name}` is `{}`, not the requested `{}`",
            kind.as_str(),
            expected.as_str()
        )));
    }
    Ok(())
}

pub fn annotated_arrow_schema(schema: &iceberg::spec::Schema) -> Result<Schema> {
    let arrow = iceberg::arrow::schema_to_arrow_schema(schema)
        .map_err(|error| CdfError::data(format!("convert Iceberg schema to Arrow: {error}")))?;
    let fields = arrow
        .fields()
        .iter()
        .map(|field| annotate_arrow_field(field, schema))
        .collect::<Result<Vec<_>>>()?;
    let mut metadata = arrow.metadata().clone();
    metadata.insert(
        "cdf:iceberg_schema_id".to_owned(),
        schema.schema_id().to_string(),
    );
    Ok(Schema::new_with_metadata(fields, metadata))
}

fn annotate_arrow_field(field: &FieldRef, schema: &iceberg::spec::Schema) -> Result<FieldRef> {
    let data_type = annotate_data_type(field.data_type(), schema)?;
    let annotated = field.as_ref().clone().with_data_type(data_type);
    let field_id = annotated
        .metadata()
        .get("PARQUET:field_id")
        .and_then(|value| value.parse::<i32>().ok())
        .ok_or_else(|| {
            CdfError::data(format!(
                "Iceberg Arrow field `{}` is missing its field id",
                field.name()
            ))
        })?;
    let source = schema.field_by_id(field_id).ok_or_else(|| {
        CdfError::data(format!(
            "Iceberg Arrow field id {field_id} is absent from selected schema"
        ))
    })?;
    let mut metadata = annotated.metadata().clone();
    metadata.insert("cdf:source_name".to_owned(), source.name.clone());
    metadata.insert("cdf:iceberg_field_id".to_owned(), field_id.to_string());
    metadata.insert(
        "cdf:iceberg_required".to_owned(),
        source.required.to_string(),
    );
    metadata.insert(
        "cdf:physical_type".to_owned(),
        source.field_type.to_string(),
    );
    if let Some(doc) = &source.doc {
        metadata.insert("cdf:iceberg_doc".to_owned(), doc.clone());
    }
    insert_default(&mut metadata, "cdf:iceberg_initial_default", source, true)?;
    insert_default(&mut metadata, "cdf:iceberg_write_default", source, false)?;
    Ok(Arc::new(annotated.with_metadata(metadata)))
}

fn annotate_data_type(data_type: &DataType, schema: &iceberg::spec::Schema) -> Result<DataType> {
    Ok(match data_type {
        DataType::List(field) => DataType::List(annotate_arrow_field(field, schema)?),
        DataType::LargeList(field) => DataType::LargeList(annotate_arrow_field(field, schema)?),
        DataType::FixedSizeList(field, size) => {
            DataType::FixedSizeList(annotate_arrow_field(field, schema)?, *size)
        }
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| annotate_arrow_field(field, schema))
                .collect::<Result<Vec<_>>>()?
                .into(),
        ),
        DataType::Map(field, sorted) => {
            DataType::Map(annotate_arrow_field(field, schema)?, *sorted)
        }
        other => other.clone(),
    })
}

fn insert_default(
    metadata: &mut std::collections::HashMap<String, String>,
    key: &str,
    field: &NestedField,
    initial: bool,
) -> Result<()> {
    let value = if initial {
        field.initial_default.clone()
    } else {
        field.write_default.clone()
    };
    if let Some(value) = value {
        let json = value
            .try_into_json(field.field_type.as_ref())
            .map_err(|error| {
                CdfError::data(format!(
                    "encode Iceberg field default for {}: {error}",
                    field.id
                ))
            })?;
        metadata.insert(
            key.to_owned(),
            serde_json::to_string(&json)
                .map_err(|error| CdfError::internal(format!("serialize field default: {error}")))?,
        );
    }
    Ok(())
}

fn read_metadata_object(
    context: &IcebergCatalogContext,
    resource: &FileTransportResource,
    identity: &FileIdentityMetadata,
    maximum_bytes: u64,
    cancellation: RunCancellation,
) -> Result<AccountedBytes> {
    let size = identity
        .size_bytes
        .ok_or_else(|| CdfError::data("Iceberg metadata object has no byte-size authority"))?;
    if size == 0 || size > maximum_bytes {
        return Err(CdfError::data(format!(
            "Iceberg metadata object contains {size} bytes outside the configured 1..={maximum_bytes} byte budget"
        )));
    }
    let source = context.object_access.open_byte_source(
        &context.egress,
        resource,
        identity,
        context.execution.memory(),
    )?;
    if source.capabilities().exact_ranges {
        let extent = cdf_runtime::ByteExtent::new(0, size)?;
        return context
            .execution
            .run_io(async move { source.read_exact_range(extent, cancellation).await });
    }
    let memory = context.execution.memory();
    let lease = reserve_discovery_memory(Arc::clone(&memory), size, "iceberg-metadata-body")?;
    context.execution.run_io(async move {
        let preferred_chunk_bytes = METADATA_READ_CHUNK_BYTES.clamp(
            source.capabilities().minimum_chunk_bytes,
            source.capabilities().maximum_chunk_bytes,
        );
        let mut stream = source
            .open_sequential(SequentialReadRequest {
                preferred_chunk_bytes,
                cancellation: cancellation.clone(),
            })
            .await?;
        let capacity = usize::try_from(size)
            .map_err(|_| CdfError::data("Iceberg metadata size exceeds usize"))?;
        let mut output = Vec::with_capacity(capacity);
        while let Some(chunk) = stream.try_next().await? {
            cancellation.check()?;
            if output.len().saturating_add(chunk.payload().len()) > capacity {
                return Err(CdfError::data(
                    "Iceberg metadata stream exceeded its immutable size authority",
                ));
            }
            output.extend_from_slice(chunk.payload());
        }
        if output.len() != capacity {
            return Err(CdfError::data(format!(
                "Iceberg metadata stream expected {size} bytes but read {}",
                output.len()
            )));
        }
        AccountedBytes::new_conservative(bytes::Bytes::from(output), lease)
    })
}

pub(crate) fn reserve_discovery_memory(
    memory: Arc<dyn MemoryCoordinator>,
    bytes: u64,
    consumer: &str,
) -> Result<MemoryLease> {
    memory
        .try_reserve(&ReservationRequest::new(
            ConsumerKey::new(consumer, MemoryClass::Discovery)?,
            bytes,
        )?)?
        .ok_or_else(|| {
            CdfError::data(format!(
                "Iceberg discovery requires {bytes} bytes for {consumer}, but the memory ledger cannot admit it; increase the memory budget or lower the metadata knobs"
            ))
        })
}

pub(crate) fn reserve_parse_memory(
    memory: Arc<dyn MemoryCoordinator>,
    input_bytes: u64,
    amplification_bps: u32,
    consumer: &str,
) -> Result<MemoryLease> {
    let parse_bytes = u64::try_from(
        (u128::from(input_bytes)
            .saturating_mul(u128::from(amplification_bps))
            .saturating_add(9_999)
            / 10_000)
            .max(1),
    )
    .map_err(|_| CdfError::data("Iceberg metadata parse reservation exceeds u64"))?;
    reserve_discovery_memory(memory, parse_bytes, consumer)
}

fn select_latest_metadata_file(
    mut identities: Vec<FileIdentityMetadata>,
) -> Result<FileIdentityMetadata> {
    identities.retain(|identity| metadata_file_version(&identity.location).is_some());
    identities.sort_by(|left, right| {
        metadata_file_version(&left.location)
            .cmp(&metadata_file_version(&right.location))
            .then_with(|| left.location.cmp(&right.location))
    });
    identities.pop().ok_or_else(|| {
        CdfError::data("Iceberg filesystem table metadata directory contains no metadata JSON")
    })
}

fn select_metadata_file_from_hint(
    identities: &[FileIdentityMetadata],
    hinted_version: u64,
) -> Result<FileIdentityMetadata> {
    let mut by_version = BTreeMap::<u64, FileIdentityMetadata>::new();
    for identity in identities {
        let Some(version) = metadata_file_version(&identity.location) else {
            continue;
        };
        by_version
            .entry(version)
            .and_modify(|selected| {
                if identity.location < selected.location {
                    *selected = identity.clone();
                }
            })
            .or_insert_with(|| identity.clone());
    }
    let mut selected = by_version.get(&hinted_version).cloned().ok_or_else(|| {
        CdfError::data(format!(
            "Iceberg version hint references missing metadata version {hinted_version}"
        ))
    })?;
    let mut version = hinted_version;
    while let Some(next_version) = version.checked_add(1)
        && let Some(next) = by_version.get(&next_version)
    {
        selected = next.clone();
        version = next_version;
    }
    Ok(selected)
}

fn parse_version_hint(payload: &[u8]) -> Result<u64> {
    let value = std::str::from_utf8(payload)
        .map_err(|_| CdfError::data("Iceberg version hint is not UTF-8"))?
        .trim();
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(CdfError::data(
            "Iceberg version hint must contain one nonnegative decimal version",
        ));
    }
    value
        .parse()
        .map_err(|_| CdfError::data("Iceberg version hint exceeds u64"))
}

fn location_name(location: &str) -> &str {
    location.rsplit('/').next().unwrap_or(location)
}

fn metadata_file_version(location: &str) -> Option<u64> {
    let name = location_name(location);
    let prefix = name.strip_suffix(".metadata.json")?;
    let prefix = prefix.strip_suffix(".gz").unwrap_or(prefix);
    let digits = prefix
        .strip_prefix('v')
        .and_then(|value| value.split('-').next())
        .or_else(|| prefix.split('-').next())?;
    digits.parse().ok()
}

pub(crate) fn transport_resource(
    location: &str,
    source: &crate::IcebergSourceOptions,
    auth: Option<cdf_http::AuthScheme>,
) -> Result<FileTransportResource> {
    let location_kind = if location.starts_with("http://") || location.starts_with("https://") {
        FileTransportLocation::HttpUrl {
            url: location.to_owned(),
        }
    } else if location.starts_with("file://") {
        FileTransportLocation::FileUrl {
            url: location.to_owned(),
        }
    } else if location.contains("://") {
        FileTransportLocation::RemoteUrl {
            url: location.to_owned(),
        }
    } else {
        FileTransportLocation::LocalPath {
            path: location.to_owned(),
        }
    };
    let mut resource = FileTransportResource {
        location: location_kind,
        egress_allowlist: allowlist(source),
        auth,
        credentials: None,
    };
    if let Some(reference) = &source.object_credentials {
        resource = resource.with_credentials(SecretUri::new(reference.clone())?);
    }
    Ok(resource)
}

fn allowlist(source: &crate::IcebergSourceOptions) -> EgressAllowlist {
    if source.egress_allowlist.is_empty() {
        EgressAllowlist::allow_any()
    } else {
        EgressAllowlist::from_hosts(source.egress_allowlist.clone())
    }
}

fn resolve_warehouse(warehouse: &str, project_root: &std::path::Path) -> Result<String> {
    if warehouse.contains("://") || std::path::Path::new(warehouse).is_absolute() {
        return Ok(warehouse.trim_end_matches('/').to_owned());
    }
    if std::path::Path::new(warehouse)
        .components()
        .any(|component| component == std::path::Component::ParentDir)
    {
        return Err(CdfError::contract(
            "relative Iceberg warehouse must stay under the project root",
        ));
    }
    Ok(project_root
        .join(warehouse)
        .to_string_lossy()
        .trim_end_matches('/')
        .to_owned())
}

fn join_location<'a>(root: &str, parts: impl IntoIterator<Item = &'a str>) -> Result<String> {
    let mut joined = root.trim_end_matches('/').to_owned();
    for part in parts {
        if part.is_empty() || part.contains(['/', '\\']) || part == "." || part == ".." {
            return Err(CdfError::contract(
                "Iceberg namespace and table path components cannot contain path separators",
            ));
        }
        joined.push('/');
        joined.push_str(part);
    }
    Ok(joined)
}

fn rest_config_endpoint(root: &str, warehouse: Option<&str>) -> Result<String> {
    let mut url = url::Url::parse(root)
        .map_err(|error| CdfError::contract(format!("invalid Iceberg REST URI: {error}")))?;
    {
        let mut path = url.path_segments_mut().map_err(|_| {
            CdfError::contract("Iceberg REST URI cannot be used as a hierarchical URL")
        })?;
        path.pop_if_empty().push("v1").push("config");
    }
    if let Some(warehouse) = warehouse {
        url.query_pairs_mut().append_pair("warehouse", warehouse);
    }
    Ok(url.to_string())
}

fn rest_table_endpoint(
    root: &str,
    prefix: Option<&str>,
    resource: &IcebergResourceOptions,
) -> Result<String> {
    let mut url = url::Url::parse(root)
        .map_err(|error| CdfError::contract(format!("invalid Iceberg REST URI: {error}")))?;
    {
        let mut path = url.path_segments_mut().map_err(|_| {
            CdfError::contract("Iceberg REST URI cannot be used as a hierarchical URL")
        })?;
        path.pop_if_empty().push("v1");
        if let Some(prefix) = prefix {
            for component in prefix.split('/') {
                path.push(component);
            }
        }
        path.push("namespaces")
            .push(&resource.namespace.join("\u{001f}"))
            .push("tables")
            .push(&resource.table);
    }
    Ok(url.to_string())
}

fn validate_rest_uri(label: &str, value: &str) -> Result<()> {
    let parsed = url::Url::parse(value)
        .map_err(|error| CdfError::data(format!("{label} is invalid: {error}")))?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(CdfError::data(format!(
            "{label} requires an HTTP(S) URL without userinfo, query, or fragment"
        )));
    }
    Ok(())
}

fn validate_rest_prefix(prefix: &str) -> Result<()> {
    if prefix.is_empty()
        || prefix.starts_with('/')
        || prefix.ends_with('/')
        || prefix
            .split('/')
            .any(|component| component.is_empty() || matches!(component, "." | ".."))
        || prefix.chars().any(char::is_control)
    {
        return Err(CdfError::data(
            "Iceberg REST catalog returned an invalid routing prefix",
        ));
    }
    Ok(())
}

fn http_catalog_error(status: u16) -> CdfError {
    match status {
        401 | 403 => CdfError::auth(format!(
            "Iceberg REST catalog rejected table access with HTTP {status}"
        )),
        404 => CdfError::data("Iceberg REST catalog table was not found"),
        408 | 425 | 429 | 500..=599 => CdfError::transient(format!(
            "Iceberg REST catalog returned retryable HTTP {status}"
        )),
        _ => CdfError::data(format!(
            "Iceberg REST catalog returned unsupported HTTP {status}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flate2::{Compression, write::GzEncoder};

    use super::*;

    fn gzip(payload: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(payload).unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn rest_endpoint_uses_iceberg_namespace_encoding() {
        let endpoint = rest_table_endpoint(
            "https://catalog.example.test",
            None,
            &IcebergResourceOptions {
                namespace: vec!["org".to_owned(), "analytics".to_owned()],
                table: "events".to_owned(),
                selector: IcebergSnapshotSelector::Current,
                mode: crate::IcebergScanMode::Snapshot,
            },
        )
        .unwrap();
        assert_eq!(
            endpoint,
            "https://catalog.example.test/v1/namespaces/org%1Fanalytics/tables/events"
        );
    }

    #[test]
    fn rest_negotiation_keeps_warehouse_in_config_query_and_prefixes_table_route() {
        let config =
            rest_config_endpoint("https://catalog.example.test/api", Some("prod/main")).unwrap();
        assert_eq!(
            config,
            "https://catalog.example.test/api/v1/config?warehouse=prod%2Fmain"
        );
        let routing = RestCatalogRouting::negotiate(
            "https://catalog.example.test/api",
            RestCatalogConfigResponse {
                defaults: BTreeMap::from([("prefix".to_owned(), "ice/prod".to_owned())]),
                overrides: BTreeMap::from([(
                    "uri".to_owned(),
                    "https://routed.example.test/catalog".to_owned(),
                )]),
                endpoints: None,
            },
        )
        .unwrap();
        let endpoint = rest_table_endpoint(
            &routing.uri,
            routing.prefix.as_deref(),
            &IcebergResourceOptions {
                namespace: vec!["analytics".to_owned()],
                table: "events".to_owned(),
                selector: IcebergSnapshotSelector::Current,
                mode: crate::IcebergScanMode::Snapshot,
            },
        )
        .unwrap();
        assert_eq!(
            endpoint,
            "https://routed.example.test/catalog/v1/ice/prod/namespaces/analytics/tables/events"
        );
    }

    #[test]
    fn metadata_file_selection_is_version_then_canonical_name() {
        let identity = |location: &str| FileIdentityMetadata {
            location: location.to_owned(),
            size_bytes: Some(1),
            checksum: None,
            etag: None,
            version: None,
            modified: Some("1".to_owned()),
            exact_ranges: true,
        };
        let selected = select_latest_metadata_file(vec![
            identity("/table/metadata/v1.metadata.json"),
            identity("/table/metadata/00002-a.metadata.json"),
            identity("/table/metadata/v2.gz.metadata.json"),
        ])
        .unwrap();
        assert_eq!(selected.location, "/table/metadata/v2.gz.metadata.json");

        let identities = vec![
            identity("/table/metadata/v1.metadata.json"),
            identity("/table/metadata/v2.metadata.json"),
            identity("/table/metadata/v4.metadata.json"),
        ];
        let selected = select_metadata_file_from_hint(&identities, 1).unwrap();
        assert_eq!(selected.location, "/table/metadata/v2.metadata.json");
        assert_eq!(parse_version_hint(b" 2\n").unwrap(), 2);
        assert!(parse_version_hint(b"v2").is_err());
    }

    #[test]
    fn gzip_metadata_is_bounded_decoded_and_crc_checked() {
        let json = br#"{"format-version":2,"table-uuid":"abc"}"#;
        let payload = gzip(json);
        let location = "/table/metadata/v7.gz.metadata.json";
        assert_eq!(metadata_json_size(location, &payload, 1024).unwrap(), 39);
        let mut decoded = Vec::new();
        GzDecoder::new(payload.as_slice())
            .read_to_end(&mut decoded)
            .unwrap();
        assert_eq!(decoded, json);
        assert!(metadata_json_size(location, &payload, 38).is_err());

        let mut corrupt = payload;
        *corrupt.last_mut().unwrap() ^= 0xff;
        assert!(metadata_json_size(location, &corrupt, 1024).is_err());
    }

    #[test]
    fn hashing_reader_covers_the_complete_logical_gzip_stream() {
        let json = b"{\"value\":7}\n   ";
        let payload = gzip(json);
        let mut reader = HashingReader::new(GzDecoder::new(payload.as_slice()));
        let value: serde_json::Value = serde_json::from_reader(&mut reader).unwrap();
        std::io::copy(&mut reader, &mut std::io::sink()).unwrap();
        let digest = reader.finalize();

        assert_eq!(value["value"], 7);
        assert_eq!(digest.as_slice(), Sha256::digest(json).as_slice());
    }

    #[test]
    fn reference_kind_projection_is_bounded_to_the_selected_ref() {
        let json = br#"{
            "format-version": 2,
            "schemas": [{"schema-id": 0, "fields": []}],
            "refs": {
                "audit": {"snapshot-id": 7, "type": "tag"},
                "main": {"snapshot-id": 9, "type": "branch"}
            }
        }"#;
        let branch = IcebergSnapshotSelector::Branch {
            name: "main".to_owned(),
        };
        let tag = IcebergSnapshotSelector::Tag {
            name: "audit".to_owned(),
        };
        assert_eq!(
            selected_reference_kind(&branch, "metadata.json", json, None).unwrap(),
            Some(IcebergReferenceKind::Branch)
        );
        let compressed = gzip(json);
        assert_eq!(
            selected_reference_kind(&tag, "v1.gz.metadata.json", &compressed, None).unwrap(),
            Some(IcebergReferenceKind::Tag)
        );
        let embedded = RawValue::from_string(String::from_utf8(json.to_vec()).unwrap()).unwrap();
        assert_eq!(
            selected_reference_kind(&tag, "rest", b"ignored", Some(&embedded)).unwrap(),
            Some(IcebergReferenceKind::Tag)
        );
    }
}
