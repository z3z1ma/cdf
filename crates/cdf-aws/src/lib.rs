#![doc = "Injected AWS control-plane protocol primitives for cdf."]

use std::{sync::Arc, time::UNIX_EPOCH};

use aws_credential_types::Credentials;
use aws_sigv4::{
    http_request::{SignableBody, SignableRequest, SigningParams, SigningSettings, sign},
    sign::v4,
};
use bytes::Bytes;
use cdf_http::{
    EgressAllowlist, HttpMethod, HttpRequest, HttpResponseBudget, HttpTransport, SecretProvider,
    SecretUri,
};
use cdf_kernel::{BoxFuture, CdfError, Result};
use cdf_runtime::{ExecutionServices, RunCancellation, SourceEgressScope};
use serde::{Deserialize, Serialize};
use url::Url;
use zeroize::ZeroizeOnDrop;

const AWS_JSON_CONTENT_TYPE: &str = "application/x-amz-json-1.1";
const AWS_REST_JSON_CONTENT_TYPE: &str = "application/json";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AwsControlTarget {
    JsonTarget { target: String },
    RestJson { path: String },
}

/// One bounded AWS control-plane request.
///
/// Service adapters own request/response models and AWS error semantics. This neutral layer owns
/// only credential resolution, SigV4, injected transport/egress authority, bounded responses, and
/// cancellation. It never creates an SDK client, HTTP pool, runtime, or retry executor.
#[derive(Clone)]
pub struct AwsControlRequest {
    pub service: String,
    pub target: AwsControlTarget,
    pub region: String,
    pub endpoint: Option<String>,
    pub credentials: Option<SecretUri>,
    pub body: Bytes,
    pub maximum_response_bytes: u64,
    pub cancellation: RunCancellation,
}

impl std::fmt::Debug for AwsControlRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AwsControlRequest")
            .field("service", &self.service)
            .field("target", &self.target)
            .field("region", &self.region)
            .field("endpoint_override", &self.endpoint.is_some())
            .field(
                "credentials",
                &self.credentials.as_ref().map(|_| "[REFERENCE]"),
            )
            .field("body_bytes", &self.body.len())
            .field("maximum_response_bytes", &self.maximum_response_bytes)
            .finish_non_exhaustive()
    }
}

impl AwsControlRequest {
    pub fn validate(&self) -> Result<()> {
        for (label, value) in [
            ("AWS service", self.service.as_str()),
            ("AWS region", self.region.as_str()),
        ] {
            if value.is_empty() || value.chars().any(char::is_control) {
                return Err(CdfError::contract(format!(
                    "{label} must be nonempty and control-free"
                )));
            }
        }
        if self.maximum_response_bytes == 0 {
            return Err(CdfError::contract(
                "AWS control response byte budget must be nonzero",
            ));
        }
        match &self.target {
            AwsControlTarget::JsonTarget { target } => {
                if target.is_empty() || target.chars().any(char::is_control) {
                    return Err(CdfError::contract(
                        "AWS JSON target must be nonempty and control-free",
                    ));
                }
            }
            AwsControlTarget::RestJson { path } => {
                if !path.starts_with('/')
                    || path.contains(['?', '#'])
                    || path.chars().any(char::is_control)
                {
                    return Err(CdfError::contract(
                        "AWS REST-JSON path must be an absolute control-free path without query or fragment",
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct AwsControlResponse {
    response: cdf_http::HttpResponse,
}

impl AwsControlResponse {
    pub fn status(&self) -> u16 {
        self.response.status
    }

    pub fn body(&self) -> Option<&[u8]> {
        self.response.body()
    }
}

impl std::fmt::Debug for AwsControlResponse {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AwsControlResponse")
            .field("status", &self.response.status)
            .field("body_bytes", &self.response.body().map_or(0, <[u8]>::len))
            .finish()
    }
}

#[derive(Clone)]
pub struct AwsControlClient {
    http: Arc<dyn HttpTransport>,
    secrets: Arc<dyn SecretProvider + Send + Sync>,
    execution: ExecutionServices,
    egress: SourceEgressScope,
}

impl std::fmt::Debug for AwsControlClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AwsControlClient")
            .finish_non_exhaustive()
    }
}

impl AwsControlClient {
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

    pub async fn send(&self, request: AwsControlRequest) -> Result<AwsControlResponse> {
        request.validate()?;
        request.cancellation.check()?;
        let endpoint = aws_service_endpoint(
            &request.service,
            &request.region,
            request.endpoint.as_deref(),
        )?;
        let (url, content_type, json_target) = match &request.target {
            AwsControlTarget::JsonTarget { target } => (
                endpoint.clone(),
                AWS_JSON_CONTENT_TYPE,
                Some(target.clone()),
            ),
            AwsControlTarget::RestJson { path } => (
                format!("{}{}", endpoint.trim_end_matches('/'), path),
                AWS_REST_JSON_CONTENT_TYPE,
                None,
            ),
        };
        self.egress.authorize(&url)?;
        let credentials = resolve_credentials(self.secrets.as_ref(), request.credentials.as_ref())?;
        let request_body_bytes = u64::try_from(request.body.len())
            .map_err(|_| CdfError::data("AWS control request body exceeds u64"))?;
        let _request_body_lease = if request_body_bytes == 0 {
            None
        } else {
            Some(
                cdf_memory::reserve(
                    self.execution.memory(),
                    cdf_memory::ReservationRequest::new(
                        cdf_memory::ConsumerKey::new(
                            "aws-control-request-body",
                            cdf_memory::MemoryClass::Control,
                        )?,
                        request_body_bytes,
                    )?,
                )
                .await?,
            )
        };
        let mut http_request = HttpRequest::new(HttpMethod::Post, url.clone())
            .with_header("content-type", content_type)
            .with_body(request.body);
        if let Some(target) = json_target {
            http_request = http_request.with_header("x-amz-target", target);
        }
        sign_request(
            &mut http_request,
            &credentials,
            &request.service,
            &request.region,
            UNIX_EPOCH + self.execution.unix_now(),
        )?;
        let host = Url::parse(&url)
            .ok()
            .and_then(|url| url.host_str().map(str::to_owned))
            .ok_or_else(|| CdfError::contract("AWS service endpoint has no host"))?;
        let budget = HttpResponseBudget::new(
            request.maximum_response_bytes,
            self.execution.memory(),
            Arc::new({
                let cancellation = request.cancellation.clone();
                move || cancellation.check()
            }),
        )?;
        let response = cdf_http::send_with_policy(
            self.http.as_ref(),
            &EgressAllowlist::from_hosts([host]),
            http_request,
            budget,
        )
        .await?;
        request.cancellation.check()?;
        Ok(AwsControlResponse { response })
    }
}

#[derive(Clone, Serialize, Deserialize, ZeroizeOnDrop)]
#[serde(deny_unknown_fields)]
pub struct AwsCredentials {
    access_key_id: String,
    secret_access_key: String,
    #[serde(default)]
    session_token: Option<String>,
}

impl AwsCredentials {
    pub fn new(
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        session_token: Option<String>,
    ) -> Result<Self> {
        let credentials = Self {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            session_token,
        };
        if credentials.access_key_id.is_empty() || credentials.secret_access_key.is_empty() {
            return Err(CdfError::auth(
                "AWS access key id and secret access key must be nonempty",
            ));
        }
        Ok(credentials)
    }

    pub fn access_key_id(&self) -> &str {
        &self.access_key_id
    }

    pub fn secret_access_key(&self) -> &str {
        &self.secret_access_key
    }

    pub fn session_token(&self) -> Option<&str> {
        self.session_token.as_deref()
    }
}

impl std::fmt::Debug for AwsCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AwsCredentials([REDACTED])")
    }
}

pub trait AwsCredentialProvider: std::fmt::Debug + Send + Sync {
    fn credentials(&self) -> BoxFuture<'_, Result<std::sync::Arc<AwsCredentials>>>;
}

fn resolve_credentials(
    secrets: &dyn SecretProvider,
    reference: Option<&SecretUri>,
) -> Result<Credentials> {
    let document = match reference {
        Some(reference) => {
            let value = secrets.resolve(reference)?;
            serde_json::from_str::<AwsCredentials>(value.as_str()?).map_err(|error| {
                CdfError::auth(format!(
                    "AWS credential secret must be a JSON object containing access_key_id, secret_access_key, and optional session_token: {error}"
                ))
            })?
        }
        None => AwsCredentials::new(
            required_env("AWS_ACCESS_KEY_ID")?,
            required_env("AWS_SECRET_ACCESS_KEY")?,
            std::env::var("AWS_SESSION_TOKEN").ok(),
        )?,
    };
    Ok(Credentials::new(
        document.access_key_id().to_owned(),
        document.secret_access_key().to_owned(),
        document.session_token().map(str::to_owned),
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
            "AWS credentials are absent; configure a secret:// JSON credential reference or export {name}, AWS_SECRET_ACCESS_KEY, and optional AWS_SESSION_TOKEN"
        ))
    })
}

pub fn aws_service_endpoint(
    service: &str,
    region: &str,
    override_endpoint: Option<&str>,
) -> Result<String> {
    if let Some(endpoint) = override_endpoint {
        let endpoint = endpoint.trim_end_matches('/');
        if endpoint.is_empty() {
            return Err(CdfError::contract("AWS endpoint override cannot be empty"));
        }
        return Ok(endpoint.to_owned());
    }
    let suffix = if region.starts_with("cn-") {
        "amazonaws.com.cn"
    } else {
        "amazonaws.com"
    };
    Ok(format!("https://{service}.{region}.{suffix}"))
}

fn sign_request(
    request: &mut HttpRequest,
    credentials: &Credentials,
    service: &str,
    region: &str,
    time: std::time::SystemTime,
) -> Result<()> {
    let identity = credentials.clone().into();
    let params: SigningParams<'_> = v4::SigningParams::builder()
        .identity(&identity)
        .region(region)
        .name(service)
        .time(time)
        .settings(SigningSettings::default())
        .build()
        .map_err(|error| CdfError::auth(format!("build AWS signing parameters: {error}")))?
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
    .map_err(|error| CdfError::auth(format!("build AWS signable request: {error}")))?;
    let (instructions, _) = sign(signable, &params)
        .map_err(|error| CdfError::auth(format!("sign AWS request: {error}")))?
        .into_parts();
    for (name, value) in instructions.headers() {
        request.headers.insert(name.to_owned(), value.to_owned());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_respects_aws_partitions_and_exact_override() {
        assert_eq!(
            aws_service_endpoint("glue", "us-east-1", None).unwrap(),
            "https://glue.us-east-1.amazonaws.com"
        );
        assert_eq!(
            aws_service_endpoint("glue", "cn-north-1", None).unwrap(),
            "https://glue.cn-north-1.amazonaws.com.cn"
        );
        assert_eq!(
            aws_service_endpoint("glue", "local", Some("http://127.0.0.1:9000/")).unwrap(),
            "http://127.0.0.1:9000"
        );
    }

    #[test]
    fn signed_request_debug_redacts_credential_material() {
        let credentials = Credentials::new(
            "AKIAEXAMPLE",
            "secret-example-key",
            Some("session-example-token".to_owned()),
            None,
            "test",
        );
        let mut request =
            HttpRequest::new(HttpMethod::Post, "https://glue.us-east-1.amazonaws.com")
                .with_body(Bytes::from_static(br#"{}"#));
        sign_request(
            &mut request,
            &credentials,
            "glue",
            "us-east-1",
            UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000),
        )
        .unwrap();
        let rendered = format!("{request:?}");
        assert!(rendered.contains("[REDACTED]"));
        assert!(!rendered.contains("AKIAEXAMPLE"));
        assert!(!rendered.contains("secret-example-key"));
        assert!(!rendered.contains("session-example-token"));
    }
}
