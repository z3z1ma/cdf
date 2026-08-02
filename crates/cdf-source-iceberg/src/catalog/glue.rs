use std::sync::Arc;

use bytes::Bytes;
use cdf_aws::{AwsControlClient, AwsControlRequest, AwsControlTarget};
use cdf_http::{HttpTransport, SecretProvider, SecretUri};
use cdf_kernel::{BoxFuture, CdfError, Result};
use cdf_object_access::FileTransportControl;
use cdf_runtime::{ExecutionServices, SourceEgressScope};
use serde::{Deserialize, Serialize};

use super::{
    CatalogObservation, IcebergCatalogBinding, IcebergCatalogContext, IcebergCatalogLoadRequest,
    LoadedIcebergTable, build_loaded_table, reserve_discovery_memory, transport_resource,
};
use crate::IcebergCatalogOptions;

const GLUE_TARGET: &str = "AWSGlue.GetTable";

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
    pub cancellation: cdf_runtime::RunCancellation,
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

pub(super) struct GlueCatalogBinding;

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
        let payload = super::read_metadata_object(
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

/// AWS Glue's read-only Iceberg pointer binding over the shared injected AWS JSON authority.
///
/// Iceberg owns only `GetTable` request/response semantics. SigV4, credentials, egress, bounded
/// response memory, HTTP pooling, and cancellation are neutral AWS infrastructure.
#[derive(Clone, Debug)]
pub struct AwsIcebergGlueCatalogClient {
    aws: Arc<AwsControlClient>,
}

impl AwsIcebergGlueCatalogClient {
    pub fn new(
        http: Arc<dyn HttpTransport>,
        secrets: Arc<dyn SecretProvider + Send + Sync>,
        execution: ExecutionServices,
        egress: SourceEgressScope,
    ) -> Self {
        Self {
            aws: Arc::new(AwsControlClient::new(http, secrets, execution, egress)),
        }
    }

    async fn request_table(&self, request: GlueGetTableRequest) -> Result<GlueTablePointer> {
        request.cancellation.check()?;
        let body = serde_json::to_vec(&GlueGetTableBody {
            catalog_id: request.catalog_id.as_deref(),
            database_name: &request.database,
            name: &request.table,
        })
        .map_err(|error| {
            CdfError::internal(format!("encode AWS Glue GetTable request: {error}"))
        })?;
        let response = self
            .aws
            .send(AwsControlRequest {
                service: "glue".to_owned(),
                target: AwsControlTarget::JsonTarget {
                    target: GLUE_TARGET.to_owned(),
                },
                region: request.region,
                endpoint: request.endpoint,
                credentials: request.credentials,
                body: Bytes::from(body),
                maximum_response_bytes: request.maximum_response_bytes,
                cancellation: request.cancellation,
            })
            .await?;
        if response.status() != 200 {
            return Err(glue_error(response.status(), response.body()));
        }
        let payload = response
            .body()
            .ok_or_else(|| CdfError::data("AWS Glue GetTable response omitted its JSON body"))?;
        decode_pointer(payload)
    }
}

impl GlueCatalogClient for AwsIcebergGlueCatalogClient {
    fn get_table(&self, request: GlueGetTableRequest) -> BoxFuture<'_, Result<GlueTablePointer>> {
        Box::pin(async move { self.request_table(request).await })
    }
}

fn decode_pointer(payload: &[u8]) -> Result<GlueTablePointer> {
    let decoded: GlueGetTableResponse = serde_json::from_slice(payload)
        .map_err(|error| CdfError::data(format!("decode AWS Glue GetTable response: {error}")))?;
    let table = decoded
        .table
        .ok_or_else(|| CdfError::data("AWS Glue GetTable response omitted Table"))?;
    let table_type = table.parameters.get("table_type").ok_or_else(|| {
        CdfError::data("AWS Glue table is not an Iceberg table: parameter `table_type` is absent")
    })?;
    if !table_type.eq_ignore_ascii_case("ICEBERG") {
        return Err(CdfError::data(format!(
            "AWS Glue table is not an Iceberg table: parameter `table_type` is `{table_type}`"
        )));
    }
    let metadata_location = table
        .parameters
        .get("metadata_location")
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .ok_or_else(|| {
            CdfError::data("AWS Glue Iceberg table omitted required parameter `metadata_location`")
        })?;
    let bytes_read = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    let retained_bytes =
        bytes_read.saturating_add(u64::try_from(metadata_location.len()).unwrap_or(u64::MAX));
    Ok(GlueTablePointer {
        metadata_location,
        catalog_generation: table.version_id,
        bytes_read,
        retained_bytes,
    })
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct GlueGetTableBody<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    catalog_id: Option<&'a str>,
    database_name: &'a str,
    name: &'a str,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GlueGetTableResponse {
    table: Option<GlueTable>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GlueTable {
    #[serde(default)]
    version_id: Option<String>,
    #[serde(default)]
    parameters: std::collections::BTreeMap<String, String>,
}

fn glue_error(status: u16, payload: Option<&[u8]>) -> CdfError {
    let kind = payload
        .and_then(|payload| serde_json::from_slice::<GlueErrorEnvelope>(payload).ok())
        .and_then(|value| value.kind)
        .unwrap_or_else(|| "unknown".to_owned());
    let kind = kind.rsplit(['#', ':']).next().unwrap_or(&kind);
    match (status, kind) {
        (401 | 403, _) | (_, "AccessDeniedException" | "UnrecognizedClientException") => {
            CdfError::auth(format!("AWS Glue GetTable authorization failed ({kind})"))
        }
        (_, "EntityNotFoundException") | (404, _) => {
            CdfError::data("AWS Glue Iceberg table was not found")
        }
        (429, _) | (_, "ThrottlingException") => CdfError::rate_limited(
            format!("AWS Glue GetTable is rate limited ({kind}, HTTP {status})"),
            None,
        ),
        (408 | 425 | 500..=599, _)
        | (_, "InternalServiceException" | "OperationTimeoutException") => CdfError::transient(
            format!("AWS Glue GetTable is retryable ({kind}, HTTP {status})"),
        ),
        _ => CdfError::data(format!("AWS Glue GetTable failed ({kind}, HTTP {status})")),
    }
}

#[derive(Deserialize)]
struct GlueErrorEnvelope {
    #[serde(rename = "__type")]
    kind: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_classification_and_pointer_mapping_are_exact() {
        assert_eq!(
            glue_error(
                400,
                Some(br#"{"__type":"com.amazonaws.glue#EntityNotFoundException"}"#),
            )
            .kind,
            cdf_kernel::ErrorKind::Data
        );
        assert_eq!(
            glue_error(429, Some(br#"{"__type":"ThrottlingException"}"#)).kind,
            cdf_kernel::ErrorKind::RateLimited
        );
        let pointer = decode_pointer(
            br#"{
                "Table": {
                    "VersionId": "catalog-version-7",
                    "Parameters": {
                        "table_type": "ICEBERG",
                        "metadata_location": "s3://lake/db/events/metadata/v7.metadata.json"
                    }
                }
            }"#,
        )
        .unwrap();
        assert_eq!(
            pointer.catalog_generation.as_deref(),
            Some("catalog-version-7")
        );
        assert_eq!(
            pointer.metadata_location,
            "s3://lake/db/events/metadata/v7.metadata.json"
        );
        assert!(decode_pointer(br#"{"Table":{"Parameters":{}}}"#).is_err());
        assert!(
            decode_pointer(
                br#"{"Table":{"Parameters":{"table_type":"DELTA","metadata_location":"s3://lake/metadata.json"}}}"#
            )
            .is_err()
        );
    }
}
