use std::{collections::BTreeMap, sync::Arc};

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
use cdf_runtime::{
    ExecutionServices, RunCancellation, SequentialReadRequest, SourceEgressScope, artifact_hash,
};
use futures_util::TryStreamExt;
use iceberg::spec::{FormatVersion, NestedField, Snapshot, TableMetadata};
use serde::Deserialize;

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
    pub egress: SourceEgressScope,
    pub project_root: std::path::PathBuf,
}

impl std::fmt::Debug for IcebergCatalogContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergCatalogContext")
            .field("project_root", &self.project_root)
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
    pub metadata: Arc<TableMetadata>,
    pub metadata_json: Arc<serde_json::Value>,
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
    payloads: Vec<AccountedBytes>,
    parse_lease: MemoryLease,
    control_leases: Vec<MemoryLease>,
}

struct CatalogObservation {
    metadata_location: String,
    payloads: Vec<AccountedBytes>,
    metadata_json: Option<serde_json::Value>,
    bytes_read: u64,
    objects_read: u64,
    control_leases: Vec<MemoryLease>,
}

impl RetainedMetadata {
    fn retained_bytes(&self) -> u64 {
        self.payloads
            .iter()
            .map(|payload| payload.lease().bytes())
            .sum::<u64>()
            .saturating_add(self.parse_lease.bytes())
            .saturating_add(
                self.control_leases
                    .iter()
                    .map(MemoryLease::bytes)
                    .sum::<u64>(),
            )
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
        let selected = select_latest_metadata_file(identities)?;
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
                payloads: vec![payload],
                metadata_json: None,
                bytes_read: selected.size_bytes.unwrap_or(0),
                objects_read: 1,
                control_leases: Vec::new(),
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
                payloads: vec![config_payload, payload],
                metadata_json: Some(envelope.metadata),
                bytes_read: config_bytes.saturating_add(response_bytes),
                objects_read: 2,
                control_leases: vec![config_parse_lease],
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
            cancellation: request.cancellation.clone(),
        };
        let pointer = context
            .execution
            .run_io(async move { glue.get_table(glue_request).await })?;
        request.cancellation.check()?;
        let pointer_lease = reserve_discovery_memory(
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
                payloads: vec![payload],
                metadata_json: None,
                bytes_read: pointer
                    .bytes_read
                    .saturating_add(identity.size_bytes.unwrap_or(0)),
                objects_read: 2,
                control_leases: vec![pointer_lease],
            },
        )
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct RestLoadTableResponse {
    metadata_location: Option<String>,
    metadata: serde_json::Value,
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
    let metadata_payload = observation.payloads.last();
    let raw_bytes = observation
        .payloads
        .iter()
        .try_fold(0_u64, |total, payload| {
            total
                .checked_add(u64::try_from(payload.payload().len()).unwrap_or(u64::MAX))
                .ok_or_else(|| CdfError::data("Iceberg metadata byte count overflowed"))
        })?;
    let parse_input_bytes = if observation.metadata_json.is_some() {
        metadata_payload.map_or(0, |payload| {
            u64::try_from(payload.payload().len()).unwrap_or(u64::MAX)
        })
    } else {
        raw_bytes
    };
    let parse_lease = reserve_parse_memory(
        context.execution.memory(),
        parse_input_bytes,
        request.source.metadata_parse_amplification_bps,
        "iceberg-metadata-parse",
    )?;
    let metadata_json = observation.metadata_json.map_or_else(
        || {
            let metadata_payload = metadata_payload
                .ok_or_else(|| CdfError::internal("Iceberg metadata payload is absent"))?;
            serde_json::from_slice(metadata_payload.payload()).map_err(|error| {
                CdfError::data(format!("decode Iceberg table metadata JSON: {error}"))
            })
        },
        Ok,
    )?;
    let metadata: TableMetadata = serde_json::from_value(metadata_json.clone())
        .map_err(|error| CdfError::data(format!("validate Iceberg table metadata: {error}")))?;
    if !matches!(
        metadata.format_version(),
        FormatVersion::V1 | FormatVersion::V2
    ) {
        return Err(CdfError::contract(
            "Iceberg source currently supports table format version 1 or 2; use a v1/v2 snapshot or wait for the v3 capability",
        ));
    }
    let metadata_generation = artifact_hash(&metadata_json)?;
    let selected = select_snapshot(
        &request.source.catalog_identity(),
        &request.resource,
        &observation.metadata_location,
        &metadata_generation,
        &metadata,
        &metadata_json,
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
    let retained = Arc::new(RetainedMetadata {
        payloads: observation.payloads,
        parse_lease,
        control_leases: observation.control_leases,
    });
    Ok(LoadedIcebergTable {
        catalog_identity: request.source.catalog_identity(),
        resource: request.resource.clone(),
        metadata_location: observation.metadata_location,
        metadata_generation,
        metadata: Arc::new(metadata),
        metadata_json: Arc::new(metadata_json),
        selected,
        arrow_schema,
        bytes_read: observation.bytes_read,
        objects_read: observation.objects_read,
        retained,
    })
}

fn select_snapshot(
    catalog: &str,
    resource: &IcebergResourceOptions,
    metadata_location: &str,
    metadata_generation: &str,
    metadata: &TableMetadata,
    metadata_json: &serde_json::Value,
) -> Result<Option<SelectedIcebergSnapshot>> {
    let snapshot =
        match &resource.selector {
            IcebergSnapshotSelector::Current => metadata.current_snapshot().map(Arc::clone),
            IcebergSnapshotSelector::Branch { name } => {
                validate_reference_kind(metadata_json, name, true)?;
                Some(metadata.snapshot_for_ref(name).cloned().ok_or_else(|| {
                    CdfError::data(format!("Iceberg branch `{name}` does not exist"))
                })?)
            }
            IcebergSnapshotSelector::Tag { name } => {
                validate_reference_kind(metadata_json, name, false)?;
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
    metadata: &serde_json::Value,
    name: &str,
    expected_branch: bool,
) -> Result<()> {
    let reference = metadata
        .get("refs")
        .and_then(serde_json::Value::as_object)
        .and_then(|refs| refs.get(name))
        .ok_or_else(|| CdfError::data(format!("Iceberg ref `{name}` does not exist")))?;
    let kind = reference
        .get("type")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| CdfError::data(format!("Iceberg ref `{name}` has no type")))?;
    let expected = if expected_branch { "branch" } else { "tag" };
    if kind != expected {
        return Err(CdfError::contract(format!(
            "Iceberg ref `{name}` is `{kind}`, not the requested `{expected}`"
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

fn reserve_discovery_memory(
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

fn reserve_parse_memory(
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

fn metadata_file_version(location: &str) -> Option<u64> {
    let name = location.rsplit('/').next().unwrap_or(location);
    if !name.ends_with(".metadata.json") {
        return None;
    }
    let prefix = name.trim_end_matches(".metadata.json");
    let digits = prefix
        .strip_prefix('v')
        .and_then(|value| value.split('-').next())
        .or_else(|| prefix.split('-').next())?;
    digits.parse().ok()
}

fn transport_resource(
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
    use super::*;

    #[test]
    fn rest_endpoint_uses_iceberg_namespace_encoding() {
        let endpoint = rest_table_endpoint(
            "https://catalog.example.test",
            None,
            &IcebergResourceOptions {
                namespace: vec!["org".to_owned(), "analytics".to_owned()],
                table: "events".to_owned(),
                selector: IcebergSnapshotSelector::Current,
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
            identity("/table/metadata/v2.metadata.json"),
        ])
        .unwrap();
        assert_eq!(selected.location, "/table/metadata/v2.metadata.json");
    }
}
