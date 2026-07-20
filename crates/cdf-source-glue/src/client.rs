use std::sync::Arc;

use bytes::Bytes;
use cdf_aws::{AwsJsonClient, AwsJsonRequest};
use cdf_http::SecretUri;
use cdf_kernel::{BoxFuture, CdfError, Result};
use cdf_runtime::RunCancellation;
use serde::{Deserialize, Serialize};

use crate::model::{GluePartition, GlueTable};

const GLUE_SERVICE: &str = "glue";
const GET_TABLE_TARGET: &str = "AWSGlue.GetTable";
const GET_PARTITIONS_TARGET: &str = "AWSGlue.GetPartitions";

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

impl GlueGetTableRequest {
    fn validate(&self) -> Result<()> {
        validate_catalog_request(
            &self.region,
            self.catalog_id.as_deref(),
            &self.database,
            &self.table,
            self.maximum_response_bytes,
        )
    }
}

#[derive(Clone, Debug)]
pub struct GlueGetPartitionsRequest {
    pub region: String,
    pub catalog_id: Option<String>,
    pub database: String,
    pub table: String,
    pub expression: Option<String>,
    pub next_token: Option<String>,
    pub page_size: u16,
    pub endpoint: Option<String>,
    pub credentials: Option<SecretUri>,
    pub maximum_response_bytes: u64,
    pub cancellation: RunCancellation,
}

impl GlueGetPartitionsRequest {
    fn validate(&self) -> Result<()> {
        validate_catalog_request(
            &self.region,
            self.catalog_id.as_deref(),
            &self.database,
            &self.table,
            self.maximum_response_bytes,
        )?;
        if !(1..=1000).contains(&self.page_size) {
            return Err(CdfError::contract(
                "Glue GetPartitions page_size must be between 1 and 1000",
            ));
        }
        if self
            .expression
            .as_ref()
            .is_some_and(|expression| expression.len() > 2048)
        {
            return Err(CdfError::contract(
                "Glue GetPartitions expression exceeds the 2048-byte service limit",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GlueTableResponse {
    pub table: GlueTable,
    pub bytes_read: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GluePartitionPage {
    pub partitions: Vec<GluePartition>,
    pub next_token: Option<String>,
    pub bytes_read: u64,
}

pub trait GlueCatalogClient: Send + Sync {
    fn get_table(&self, request: GlueGetTableRequest) -> BoxFuture<'_, Result<GlueTableResponse>>;
    fn get_partitions(
        &self,
        request: GlueGetPartitionsRequest,
    ) -> BoxFuture<'_, Result<GluePartitionPage>>;
}

#[derive(Clone, Debug)]
pub struct AwsGlueCatalogClient {
    aws: Arc<AwsJsonClient>,
}

impl AwsGlueCatalogClient {
    pub fn new(aws: Arc<AwsJsonClient>) -> Self {
        Self { aws }
    }

    async fn request_table(&self, request: GlueGetTableRequest) -> Result<GlueTableResponse> {
        request.validate()?;
        request.cancellation.check()?;
        let body = serde_json::to_vec(&GetTableBody {
            catalog_id: request.catalog_id.as_deref(),
            database_name: &request.database,
            name: &request.table,
        })
        .map_err(|error| CdfError::internal(format!("encode Glue GetTable: {error}")))?;
        let response = self
            .aws
            .send(AwsJsonRequest {
                service: GLUE_SERVICE.to_owned(),
                target: GET_TABLE_TARGET.to_owned(),
                region: request.region,
                endpoint: request.endpoint,
                credentials: request.credentials,
                body: Bytes::from(body),
                maximum_response_bytes: request.maximum_response_bytes,
                cancellation: request.cancellation,
            })
            .await?;
        if response.status() != 200 {
            return Err(glue_error("GetTable", response.status(), response.body()));
        }
        let payload = response
            .body()
            .ok_or_else(|| CdfError::data("Glue GetTable response omitted its JSON body"))?;
        let decoded: GetTableResponse = serde_json::from_slice(payload)
            .map_err(|error| CdfError::data(format!("decode Glue GetTable: {error}")))?;
        Ok(GlueTableResponse {
            table: decoded
                .table
                .ok_or_else(|| CdfError::data("Glue GetTable response omitted Table"))?,
            bytes_read: u64::try_from(payload.len()).unwrap_or(u64::MAX),
        })
    }

    async fn request_partitions(
        &self,
        request: GlueGetPartitionsRequest,
    ) -> Result<GluePartitionPage> {
        request.validate()?;
        request.cancellation.check()?;
        let body = serde_json::to_vec(&GetPartitionsBody {
            catalog_id: request.catalog_id.as_deref(),
            database_name: &request.database,
            table_name: &request.table,
            expression: request.expression.as_deref(),
            next_token: request.next_token.as_deref(),
            max_results: request.page_size,
        })
        .map_err(|error| CdfError::internal(format!("encode Glue GetPartitions: {error}")))?;
        let response = self
            .aws
            .send(AwsJsonRequest {
                service: GLUE_SERVICE.to_owned(),
                target: GET_PARTITIONS_TARGET.to_owned(),
                region: request.region,
                endpoint: request.endpoint,
                credentials: request.credentials,
                body: Bytes::from(body),
                maximum_response_bytes: request.maximum_response_bytes,
                cancellation: request.cancellation,
            })
            .await?;
        if response.status() != 200 {
            return Err(glue_error(
                "GetPartitions",
                response.status(),
                response.body(),
            ));
        }
        let payload = response
            .body()
            .ok_or_else(|| CdfError::data("Glue GetPartitions response omitted its JSON body"))?;
        let decoded: GetPartitionsResponse = serde_json::from_slice(payload)
            .map_err(|error| CdfError::data(format!("decode Glue GetPartitions: {error}")))?;
        if decoded.next_token.as_ref().is_some_and(String::is_empty) {
            return Err(CdfError::data(
                "Glue GetPartitions returned an empty continuation token",
            ));
        }
        Ok(GluePartitionPage {
            partitions: decoded.partitions,
            next_token: decoded.next_token,
            bytes_read: u64::try_from(payload.len()).unwrap_or(u64::MAX),
        })
    }
}

fn validate_catalog_request(
    region: &str,
    catalog_id: Option<&str>,
    database: &str,
    table: &str,
    maximum_response_bytes: u64,
) -> Result<()> {
    for (label, value) in [("region", region), ("database", database), ("table", table)] {
        if value.is_empty() || value.chars().any(char::is_control) {
            return Err(CdfError::contract(format!(
                "Glue {label} must be nonempty and control-free"
            )));
        }
    }
    if catalog_id.is_some_and(|value| value.is_empty() || value.chars().any(char::is_control)) {
        return Err(CdfError::contract(
            "Glue catalog id must be nonempty and control-free",
        ));
    }
    if maximum_response_bytes == 0 {
        return Err(CdfError::contract(
            "Glue response byte budget must be nonzero",
        ));
    }
    Ok(())
}

impl GlueCatalogClient for AwsGlueCatalogClient {
    fn get_table(&self, request: GlueGetTableRequest) -> BoxFuture<'_, Result<GlueTableResponse>> {
        Box::pin(async move { self.request_table(request).await })
    }

    fn get_partitions(
        &self,
        request: GlueGetPartitionsRequest,
    ) -> BoxFuture<'_, Result<GluePartitionPage>> {
        Box::pin(async move { self.request_partitions(request).await })
    }
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct GetTableBody<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    catalog_id: Option<&'a str>,
    database_name: &'a str,
    name: &'a str,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GetTableResponse {
    table: Option<GlueTable>,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct GetPartitionsBody<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    catalog_id: Option<&'a str>,
    database_name: &'a str,
    table_name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    expression: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_token: Option<&'a str>,
    max_results: u16,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GetPartitionsResponse {
    #[serde(default)]
    partitions: Vec<GluePartition>,
    #[serde(default)]
    next_token: Option<String>,
}

fn glue_error(operation: &str, status: u16, payload: Option<&[u8]>) -> CdfError {
    let kind = payload
        .and_then(|payload| serde_json::from_slice::<GlueErrorEnvelope>(payload).ok())
        .and_then(|value| value.kind)
        .unwrap_or_else(|| "unknown".to_owned());
    let kind = kind.rsplit(['#', ':']).next().unwrap_or(&kind);
    let message = format!("AWS Glue {operation} failed ({kind}, HTTP {status})");
    match (status, kind) {
        (401 | 403, _) | (_, "AccessDeniedException" | "UnrecognizedClientException") => {
            CdfError::auth(message)
        }
        (_, "EntityNotFoundException") | (404, _) => CdfError::data(message),
        (429, _) | (_, "ThrottlingException") => CdfError::rate_limited(message, None),
        (408 | 425 | 500..=599, _)
        | (_, "InternalServiceException" | "OperationTimeoutException") => {
            CdfError::transient(message)
        }
        _ => CdfError::data(message),
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
    fn glue_errors_preserve_retry_and_auth_classes_without_payload_details() {
        assert_eq!(
            glue_error(
                "GetTable",
                429,
                Some(br#"{"__type":"ThrottlingException"}"#),
            )
            .kind,
            cdf_kernel::ErrorKind::RateLimited
        );
        assert_eq!(
            glue_error(
                "GetTable",
                403,
                Some(br#"{"__type":"AccessDeniedException"}"#),
            )
            .kind,
            cdf_kernel::ErrorKind::Auth
        );
        assert_eq!(
            glue_error(
                "GetTable",
                500,
                Some(br#"{"__type":"InternalServiceException"}"#),
            )
            .kind,
            cdf_kernel::ErrorKind::Transient
        );
    }

    #[test]
    fn partition_request_enforces_service_bounds_before_transport() {
        let request = GlueGetPartitionsRequest {
            region: "us-west-2".to_owned(),
            catalog_id: None,
            database: "analytics".to_owned(),
            table: "events".to_owned(),
            expression: Some("x".repeat(2049)),
            next_token: None,
            page_size: 1001,
            endpoint: None,
            credentials: None,
            maximum_response_bytes: 1024,
            cancellation: RunCancellation::default(),
        };
        assert!(
            request
                .validate()
                .unwrap_err()
                .message
                .contains("page_size")
        );

        let mut request = request;
        request.page_size = 1000;
        assert!(
            request
                .validate()
                .unwrap_err()
                .message
                .contains("2048-byte")
        );
    }
}
