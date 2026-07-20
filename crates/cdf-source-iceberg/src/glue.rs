use std::{sync::Arc, time::UNIX_EPOCH};

use aws_credential_types::Credentials;
use aws_sigv4::{
    http_request::{SignableBody, SignableRequest, SigningParams, SigningSettings, sign},
    sign::v4,
};
use cdf_http::{
    EgressAllowlist, HttpMethod, HttpRequest, HttpResponseBudget, HttpTransport, SecretProvider,
};
use cdf_kernel::{BoxFuture, CdfError, Result};
use cdf_runtime::{ExecutionServices, SourceEgressScope};
use serde::{Deserialize, Serialize};
use url::Url;
use zeroize::ZeroizeOnDrop;

use crate::{GlueCatalogClient, GlueGetTableRequest, GlueTablePointer};

const GLUE_SERVICE: &str = "glue";
const GLUE_TARGET: &str = "AWSGlue.GetTable";
const GLUE_CONTENT_TYPE: &str = "application/x-amz-json-1.1";

/// AWS Glue's read-only Iceberg catalog binding over CDF's injected HTTP and secret authorities.
///
/// This deliberately does not construct an AWS SDK client, HTTP pool, async runtime, retry
/// executor, or credential chain. The only AWS-specific work here is SigV4 signing and decoding
/// the `GetTable` control response.
#[derive(Clone)]
pub struct AwsGlueCatalogClient {
    http: Arc<dyn HttpTransport>,
    secrets: Arc<dyn SecretProvider + Send + Sync>,
    execution: ExecutionServices,
    egress: SourceEgressScope,
}

impl std::fmt::Debug for AwsGlueCatalogClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AwsGlueCatalogClient")
            .finish_non_exhaustive()
    }
}

impl AwsGlueCatalogClient {
    pub fn new(
        http: Arc<dyn HttpTransport>,
        secrets: Arc<dyn SecretProvider + Send + Sync>,
        execution: ExecutionServices,
        egress: SourceEgressScope,
    ) -> Self {
        Self {
            http,
            secrets,
            execution,
            egress,
        }
    }

    async fn request_table(&self, request: GlueGetTableRequest) -> Result<GlueTablePointer> {
        request.cancellation.check()?;
        let endpoint = glue_endpoint(&request.region, request.endpoint.as_deref())?;
        self.egress.authorize(&endpoint)?;
        let credentials = resolve_credentials(self.secrets.as_ref(), request.credentials.as_ref())?;
        let body = serde_json::to_vec(&GlueGetTableBody {
            catalog_id: request.catalog_id.as_deref(),
            database_name: &request.database,
            name: &request.table,
        })
        .map_err(|error| {
            CdfError::internal(format!("encode AWS Glue GetTable request: {error}"))
        })?;
        let body_bytes = u64::try_from(body.len())
            .map_err(|_| CdfError::data("AWS Glue request body length exceeds u64"))?;
        let _body_lease = crate::catalog::reserve_discovery_memory(
            self.execution.memory(),
            body_bytes.max(1),
            "iceberg-glue-request",
        )?;
        let mut http_request = HttpRequest::new(HttpMethod::Post, endpoint.clone())
            .with_header("content-type", GLUE_CONTENT_TYPE)
            .with_header("x-amz-target", GLUE_TARGET)
            .with_body(bytes::Bytes::from(body));
        sign_request(
            &mut http_request,
            &credentials,
            &request.region,
            UNIX_EPOCH + self.execution.unix_now(),
        )?;
        let host = Url::parse(&endpoint)
            .ok()
            .and_then(|url| url.host_str().map(str::to_owned))
            .ok_or_else(|| CdfError::contract("AWS Glue endpoint has no host"))?;
        let budget = HttpResponseBudget::new(
            request.maximum_response_bytes,
            self.execution.memory(),
            Arc::new({
                let cancellation = request.cancellation.clone();
                move || cancellation.check()
            }),
        )?;
        let http = Arc::clone(&self.http);
        let response = cdf_http::send_with_policy(
            http.as_ref(),
            &EgressAllowlist::from_hosts([host]),
            http_request,
            budget,
        )
        .await?;
        request.cancellation.check()?;
        if response.status != 200 {
            return Err(glue_error(response.status, response.body()));
        }
        let payload = response
            .body()
            .ok_or_else(|| CdfError::data("AWS Glue GetTable response omitted its JSON body"))?;
        decode_pointer(payload)
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

impl GlueCatalogClient for AwsGlueCatalogClient {
    fn get_table(&self, request: GlueGetTableRequest) -> BoxFuture<'_, Result<GlueTablePointer>> {
        Box::pin(async move { self.request_table(request).await })
    }
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

#[derive(Deserialize, ZeroizeOnDrop)]
#[serde(deny_unknown_fields)]
struct GlueCredentialDocument {
    access_key_id: String,
    secret_access_key: String,
    #[serde(default)]
    session_token: Option<String>,
}

fn resolve_credentials(
    secrets: &dyn SecretProvider,
    reference: Option<&cdf_http::SecretUri>,
) -> Result<Credentials> {
    let document = match reference {
        Some(reference) => {
            let value = secrets.resolve(reference)?;
            serde_json::from_str::<GlueCredentialDocument>(value.as_str()?).map_err(|error| {
                CdfError::auth(format!(
                    "AWS Glue credential secret must be a JSON object containing access_key_id, secret_access_key, and optional session_token: {error}"
                ))
            })?
        }
        None => GlueCredentialDocument {
            access_key_id: required_env("AWS_ACCESS_KEY_ID")?,
            secret_access_key: required_env("AWS_SECRET_ACCESS_KEY")?,
            session_token: std::env::var("AWS_SESSION_TOKEN").ok(),
        },
    };
    Ok(Credentials::new(
        document.access_key_id.clone(),
        document.secret_access_key.clone(),
        document.session_token.clone(),
        None,
        if reference.is_some() {
            "cdf-secret-provider"
        } else {
            "cdf-process-environment"
        },
    ))
}

fn required_env(name: &str) -> Result<String> {
    std::env::var(name).map_err(|_| {
        CdfError::auth(format!(
            "AWS Glue credentials are absent; configure a secret:// JSON credential reference or export {name}, AWS_SECRET_ACCESS_KEY, and optional AWS_SESSION_TOKEN"
        ))
    })
}

fn glue_endpoint(region: &str, override_endpoint: Option<&str>) -> Result<String> {
    if let Some(endpoint) = override_endpoint {
        return Ok(endpoint.trim_end_matches('/').to_owned());
    }
    let suffix = if region.starts_with("cn-") {
        "amazonaws.com.cn"
    } else {
        "amazonaws.com"
    };
    Ok(format!("https://glue.{region}.{suffix}"))
}

fn sign_request(
    request: &mut HttpRequest,
    credentials: &Credentials,
    region: &str,
    time: std::time::SystemTime,
) -> Result<()> {
    let identity = credentials.clone().into();
    let params: SigningParams<'_> = v4::SigningParams::builder()
        .identity(&identity)
        .region(region)
        .name(GLUE_SERVICE)
        .time(time)
        .settings(SigningSettings::default())
        .build()
        .map_err(|error| CdfError::auth(format!("build AWS Glue signing parameters: {error}")))?
        .into();
    let method = request.method.to_string();
    let signable = SignableRequest::new(
        &method,
        &request.url,
        request
            .headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str())),
        SignableBody::Bytes(request.body().map_or(&[], bytes::Bytes::as_ref)),
    )
    .map_err(|error| CdfError::auth(format!("build AWS Glue signable request: {error}")))?;
    let (instructions, _) = sign(signable, &params)
        .map_err(|error| CdfError::auth(format!("sign AWS Glue request: {error}")))?
        .into_parts();
    for (name, value) in instructions.headers() {
        request.headers.insert(name.to_owned(), value.to_owned());
    }
    Ok(())
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
        (408 | 425 | 429 | 500..=599, _)
        | (_, "ThrottlingException" | "InternalServiceException" | "OperationTimeoutException") => {
            CdfError::transient(format!(
                "AWS Glue GetTable is retryable ({kind}, HTTP {status})"
            ))
        }
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
    fn endpoint_partition_and_error_classification_are_exact() {
        assert_eq!(
            glue_endpoint("us-east-1", None).unwrap(),
            "https://glue.us-east-1.amazonaws.com"
        );
        assert_eq!(
            glue_endpoint("cn-north-1", None).unwrap(),
            "https://glue.cn-north-1.amazonaws.com.cn"
        );
        assert_eq!(
            glue_error(
                400,
                Some(br#"{"__type":"com.amazonaws.glue#EntityNotFoundException"}"#)
            )
            .kind,
            cdf_kernel::ErrorKind::Data
        );
        assert_eq!(
            glue_error(429, Some(br#"{"__type":"ThrottlingException"}"#)).kind,
            cdf_kernel::ErrorKind::Transient
        );
    }

    #[test]
    fn signature_never_enters_debug_output() {
        let credentials = Credentials::new(
            "AKIDEXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            Some("session-token".to_owned()),
            None,
            "test",
        );
        let mut request =
            HttpRequest::new(HttpMethod::Post, "https://glue.us-east-1.amazonaws.com")
                .with_header("content-type", GLUE_CONTENT_TYPE)
                .with_header("x-amz-target", GLUE_TARGET)
                .with_body("{}");
        sign_request(&mut request, &credentials, "us-east-1", UNIX_EPOCH).unwrap();
        let debug = format!("{request:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("AKIDEXAMPLE"));
        assert!(!debug.contains("session-token"));
    }

    #[test]
    fn glue_pointer_requires_iceberg_type_and_exact_metadata_location() {
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
