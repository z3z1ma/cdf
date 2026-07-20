use std::sync::Arc;

use bytes::Bytes;
use cdf_aws::{AwsJsonClient, AwsJsonRequest};
use cdf_http::{HttpTransport, SecretProvider};
use cdf_kernel::{BoxFuture, CdfError, Result};
use cdf_runtime::{ExecutionServices, SourceEgressScope};
use serde::{Deserialize, Serialize};

use crate::{GlueCatalogClient, GlueGetTableRequest, GlueTablePointer};

const GLUE_TARGET: &str = "AWSGlue.GetTable";

/// AWS Glue's read-only Iceberg pointer binding over the shared injected AWS JSON authority.
///
/// Iceberg owns only `GetTable` request/response semantics. SigV4, credentials, egress, bounded
/// response memory, HTTP pooling, and cancellation are neutral AWS infrastructure.
#[derive(Clone, Debug)]
pub struct AwsGlueCatalogClient {
    aws: Arc<AwsJsonClient>,
}

impl AwsGlueCatalogClient {
    pub fn new(
        http: Arc<dyn HttpTransport>,
        secrets: Arc<dyn SecretProvider + Send + Sync>,
        execution: ExecutionServices,
        egress: SourceEgressScope,
    ) -> Self {
        Self {
            aws: Arc::new(AwsJsonClient::new(http, secrets, execution, egress)),
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
            .send(AwsJsonRequest {
                service: "glue".to_owned(),
                target: GLUE_TARGET.to_owned(),
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

impl GlueCatalogClient for AwsGlueCatalogClient {
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
