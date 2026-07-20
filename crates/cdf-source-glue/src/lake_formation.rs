use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use cdf_aws::{AwsControlClient, AwsControlRequest, AwsControlTarget, AwsCredentials};
use cdf_http::SecretUri;
use cdf_kernel::{BoxFuture, CdfError, Result};
use cdf_object_access::RuntimeAwsCredentials;
use cdf_runtime::{ExecutionServices, RunCancellation, artifact_hash};
use serde::{Deserialize, Serialize};

use crate::{GlueLakeFormationAuthorization, GlueSourceOptions};

const LAKE_FORMATION_SERVICE: &str = "lakeformation";
const TABLE_CREDENTIALS_PATH: &str = "/GetTemporaryGlueTableCredentials";
const PARTITION_CREDENTIALS_PATH: &str = "/GetTemporaryGluePartitionCredentials";

#[derive(Clone, Debug)]
pub struct LakeFormationCredentialRequest {
    pub region: String,
    pub endpoint: Option<String>,
    pub credentials: Option<SecretUri>,
    pub table_arn: String,
    pub partition_values: Option<Vec<String>>,
    pub s3_path: Option<String>,
    pub query_id: String,
    pub query_start_unix_seconds: u64,
    pub query_authorization_id: String,
    pub audit_context: String,
    pub duration_seconds: Option<u32>,
    pub maximum_response_bytes: u64,
    pub cancellation: RunCancellation,
}

impl LakeFormationCredentialRequest {
    fn validate(&self) -> Result<()> {
        for (label, value) in [
            ("region", self.region.as_str()),
            ("table ARN", self.table_arn.as_str()),
            ("query id", self.query_id.as_str()),
            (
                "query authorization id",
                self.query_authorization_id.as_str(),
            ),
            ("audit context", self.audit_context.as_str()),
        ] {
            if value.is_empty() || value.chars().any(char::is_control) {
                return Err(CdfError::contract(format!(
                    "Lake Formation {label} must be nonempty and control-free"
                )));
            }
        }
        if self.maximum_response_bytes == 0 {
            return Err(CdfError::contract(
                "Lake Formation response byte budget must be nonzero",
            ));
        }
        if self.query_start_unix_seconds == 0 {
            return Err(CdfError::contract(
                "Lake Formation query start time must be a positive Unix timestamp",
            ));
        }
        if self
            .duration_seconds
            .is_some_and(|seconds| !(900..=43_200).contains(&seconds))
        {
            return Err(CdfError::contract(
                "Lake Formation credential duration must be between 900 and 43200 seconds",
            ));
        }
        match &self.partition_values {
            Some(values) => {
                if values.is_empty()
                    || values
                        .iter()
                        .any(|value| value.chars().any(char::is_control))
                {
                    return Err(CdfError::contract(
                        "Lake Formation partition credential request requires nonempty control-free partition values",
                    ));
                }
                if self.s3_path.is_some() {
                    return Err(CdfError::internal(
                        "Lake Formation partition credentials cannot carry a table S3 path",
                    ));
                }
            }
            None => {
                let path = self.s3_path.as_deref().ok_or_else(|| {
                    CdfError::contract(
                        "Lake Formation table credential request requires the selected S3 path",
                    )
                })?;
                if !path.starts_with("s3://") {
                    return Err(CdfError::contract(
                        "Lake Formation table credential scope must be an s3:// path",
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct LakeFormationCredentialResponse {
    pub credentials: Arc<AwsCredentials>,
    pub expiration_unix_seconds: u64,
    pub vended_s3_paths: Vec<String>,
    pub bytes_read: u64,
}

impl std::fmt::Debug for LakeFormationCredentialResponse {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LakeFormationCredentialResponse")
            .field("credentials", &"[REDACTED]")
            .field("expiration_unix_seconds", &self.expiration_unix_seconds)
            .field("vended_s3_paths", &self.vended_s3_paths)
            .field("bytes_read", &self.bytes_read)
            .finish()
    }
}

pub trait LakeFormationClient: Send + Sync {
    fn vend_credentials(
        &self,
        request: LakeFormationCredentialRequest,
    ) -> BoxFuture<'_, Result<LakeFormationCredentialResponse>>;
}

#[derive(Clone)]
pub(crate) struct LakeFormationRuntime {
    context: Arc<LakeFormationVendingContext>,
    partitioned: bool,
    maximum_bindings: usize,
    bindings: Arc<Mutex<BindingCache>>,
}

impl std::fmt::Debug for LakeFormationRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LakeFormationRuntime")
            .field("partitioned", &self.partitioned)
            .field("maximum_bindings", &self.maximum_bindings)
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
struct BindingCache {
    order: VecDeque<String>,
    values: BTreeMap<String, CachedBinding>,
}

struct CachedBinding {
    scope: String,
    credentials: RuntimeAwsCredentials,
}

struct LakeFormationVendingContext {
    client: Arc<dyn LakeFormationClient>,
    region: String,
    object_region: String,
    endpoint: Option<String>,
    credentials: Option<SecretUri>,
    table_arn: String,
    table_s3_path: String,
    authorization: GlueLakeFormationAuthorization,
    duration_seconds: Option<u32>,
    refresh_margin_seconds: u64,
    maximum_response_bytes: u64,
    execution: ExecutionServices,
    cancellation: RunCancellation,
}

impl std::fmt::Debug for LakeFormationVendingContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LakeFormationVendingContext")
            .field("region", &self.region)
            .field("object_region", &self.object_region)
            .field("endpoint_override", &self.endpoint.is_some())
            .field(
                "credentials",
                &self.credentials.as_ref().map(|_| "[REFERENCE]"),
            )
            .field("table_arn", &self.table_arn)
            .field("table_s3_path", &self.table_s3_path)
            .finish_non_exhaustive()
    }
}

impl LakeFormationRuntime {
    pub(crate) fn new(
        client: Arc<dyn LakeFormationClient>,
        source: &GlueSourceOptions,
        authorization: GlueLakeFormationAuthorization,
        table_s3_path: String,
        partitioned: bool,
        execution: ExecutionServices,
        cancellation: RunCancellation,
    ) -> Result<Self> {
        if !table_s3_path.starts_with("s3://") {
            return Err(CdfError::contract(
                "Lake Formation governed Glue table must use an s3:// storage location",
            ));
        }
        let credentials = source
            .credentials
            .as_ref()
            .map(|value| SecretUri::new(value.clone()))
            .transpose()?;
        Ok(Self {
            context: Arc::new(LakeFormationVendingContext {
                client,
                region: source.region.clone(),
                object_region: source
                    .object_region
                    .clone()
                    .unwrap_or_else(|| source.region.clone()),
                endpoint: source.lake_formation_endpoint.clone(),
                credentials,
                table_arn: authorization.resource_arn.clone(),
                table_s3_path,
                authorization,
                duration_seconds: source.lake_formation_session_duration_seconds,
                refresh_margin_seconds: source.lake_formation_refresh_margin_seconds,
                maximum_response_bytes: source.maximum_response_bytes,
                execution,
                cancellation,
            }),
            partitioned,
            maximum_bindings: source.lake_formation_binding_cache_entries,
            bindings: Arc::new(Mutex::new(BindingCache::default())),
        })
    }

    pub(crate) fn authorization(&self) -> &GlueLakeFormationAuthorization {
        &self.context.authorization
    }

    pub(crate) fn binding(
        &self,
        location: &str,
        partition_values: &[Option<String>],
    ) -> Result<RuntimeAwsCredentials> {
        if !location.starts_with("s3://") {
            return Err(CdfError::contract(
                "Lake Formation governed Glue objects must use s3:// locations",
            ));
        }
        if !s3_scope_contains(&self.context.table_s3_path, location) {
            return Err(CdfError::auth(format!(
                "Lake Formation selected object `{location}` is outside governed table path `{}`",
                self.context.table_s3_path
            )));
        }
        let partition_values = if self.partitioned {
            if partition_values.is_empty() {
                return Err(CdfError::data(
                    "Lake Formation governed partition omitted its partition values",
                ));
            }
            Some(
                partition_values
                    .iter()
                    .map(|value| {
                        value.clone().ok_or_else(|| {
                            CdfError::data("Lake Formation governed partition value cannot be null")
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            )
        } else {
            if !partition_values.is_empty() {
                return Err(CdfError::data(
                    "Lake Formation unpartitioned table received partition values",
                ));
            }
            None
        };
        let key = artifact_hash(&serde_json::json!({
            "table": self.context.table_arn,
            "partition": partition_values,
        }))?;
        let mut bindings = self
            .bindings
            .lock()
            .map_err(|_| CdfError::internal("Lake Formation binding cache lock is poisoned"))?;
        if let Some(binding) = bindings.values.get(&key) {
            if !s3_scope_contains(&binding.scope, location) {
                return Err(CdfError::auth(format!(
                    "Lake Formation object `{location}` is outside the selected partition path `{}`",
                    binding.scope
                )));
            }
            return Ok(binding.credentials.clone());
        }
        let provider = Arc::new(LakeFormationAwsCredentialProvider {
            context: Arc::clone(&self.context),
            partition_values,
            cached: futures_util::lock::Mutex::new(None),
        });
        let binding = RuntimeAwsCredentials::new(
            key.clone(),
            BTreeMap::from([("aws_region".to_owned(), self.context.object_region.clone())]),
            provider,
        )?;
        while bindings.values.len() >= self.maximum_bindings {
            let Some(evicted) = bindings.order.pop_front() else {
                break;
            };
            bindings.values.remove(&evicted);
        }
        bindings.order.push_back(key.clone());
        bindings.values.insert(
            key,
            CachedBinding {
                scope: location.to_owned(),
                credentials: binding.clone(),
            },
        );
        Ok(binding)
    }
}

struct LakeFormationAwsCredentialProvider {
    context: Arc<LakeFormationVendingContext>,
    partition_values: Option<Vec<String>>,
    cached: futures_util::lock::Mutex<Option<CachedCredentials>>,
}

impl std::fmt::Debug for LakeFormationAwsCredentialProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LakeFormationAwsCredentialProvider")
            .field("partition_scoped", &self.partition_values.is_some())
            .finish_non_exhaustive()
    }
}

struct CachedCredentials {
    credentials: Arc<AwsCredentials>,
    expiration_unix_seconds: u64,
}

impl cdf_aws::AwsCredentialProvider for LakeFormationAwsCredentialProvider {
    fn credentials(&self) -> BoxFuture<'_, Result<Arc<AwsCredentials>>> {
        Box::pin(async move {
            self.context.cancellation.check()?;
            let mut cached = self.cached.lock().await;
            let now = self.context.execution.unix_now().as_secs();
            if let Some(credentials) = cached.as_ref()
                && now.saturating_add(self.context.refresh_margin_seconds)
                    < credentials.expiration_unix_seconds
            {
                return Ok(Arc::clone(&credentials.credentials));
            }
            let response = self
                .context
                .client
                .vend_credentials(LakeFormationCredentialRequest {
                    region: self.context.region.clone(),
                    endpoint: self.context.endpoint.clone(),
                    credentials: self.context.credentials.clone(),
                    table_arn: self.context.table_arn.clone(),
                    partition_values: self.partition_values.clone(),
                    s3_path: self
                        .partition_values
                        .is_none()
                        .then(|| self.context.table_s3_path.clone()),
                    query_id: self.context.authorization.query_id.clone(),
                    query_start_unix_seconds: self.context.authorization.query_start_unix_seconds,
                    query_authorization_id: self
                        .context
                        .authorization
                        .query_authorization_id
                        .clone(),
                    audit_context: format!("cdf:{}", self.context.authorization.query_id),
                    duration_seconds: self.context.duration_seconds,
                    maximum_response_bytes: self.context.maximum_response_bytes,
                    cancellation: self.context.cancellation.clone(),
                })
                .await?;
            if response.expiration_unix_seconds
                <= now.saturating_add(self.context.refresh_margin_seconds)
            {
                return Err(CdfError::auth(
                    "Lake Formation returned credentials that expire before the configured refresh margin",
                ));
            }
            let credentials = Arc::clone(&response.credentials);
            *cached = Some(CachedCredentials {
                credentials: response.credentials,
                expiration_unix_seconds: response.expiration_unix_seconds,
            });
            Ok(credentials)
        })
    }
}

#[derive(Clone, Debug)]
pub struct AwsLakeFormationClient {
    aws: Arc<AwsControlClient>,
}

impl AwsLakeFormationClient {
    pub fn new(aws: Arc<AwsControlClient>) -> Self {
        Self { aws }
    }

    async fn request_credentials(
        &self,
        request: LakeFormationCredentialRequest,
    ) -> Result<LakeFormationCredentialResponse> {
        request.validate()?;
        request.cancellation.check()?;
        let (path, body) = match &request.partition_values {
            Some(values) => (
                PARTITION_CREDENTIALS_PATH,
                serde_json::to_vec(&PartitionCredentialsBody {
                    table_arn: &request.table_arn,
                    partition: PartitionValues { values },
                    permissions: ["SELECT"],
                    duration_seconds: request.duration_seconds,
                    audit_context: AuditContext {
                        additional_audit_context: &request.audit_context,
                    },
                    supported_permission_types: ["COLUMN_PERMISSION"],
                }),
            ),
            None => (
                TABLE_CREDENTIALS_PATH,
                serde_json::to_vec(&TableCredentialsBody {
                    table_arn: &request.table_arn,
                    permissions: ["SELECT"],
                    duration_seconds: request.duration_seconds,
                    audit_context: AuditContext {
                        additional_audit_context: &request.audit_context,
                    },
                    supported_permission_types: ["COLUMN_PERMISSION"],
                    s3_path: request.s3_path.as_deref().ok_or_else(|| {
                        CdfError::internal("validated Lake Formation table request omitted S3Path")
                    })?,
                    query_session_context: QuerySessionContext {
                        query_id: &request.query_id,
                        query_start_time: request.query_start_unix_seconds,
                        query_authorization_id: &request.query_authorization_id,
                    },
                }),
            ),
        };
        let body = body.map_err(|error| {
            CdfError::internal(format!("encode Lake Formation credential request: {error}"))
        })?;
        let response = self
            .aws
            .send(AwsControlRequest {
                service: LAKE_FORMATION_SERVICE.to_owned(),
                target: AwsControlTarget::RestJson {
                    path: path.to_owned(),
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
            return Err(lake_formation_error(
                path.trim_start_matches('/'),
                response.status(),
                response.body(),
            ));
        }
        let payload = response.body().ok_or_else(|| {
            CdfError::data("Lake Formation credential response omitted its JSON body")
        })?;
        let decoded: CredentialResponse = serde_json::from_slice(payload).map_err(|error| {
            CdfError::data(format!(
                "decode Lake Formation credential response: {error}"
            ))
        })?;
        let expiration = decoded.expiration.ok_or_else(|| {
            CdfError::auth("Lake Formation credential response omitted Expiration")
        })?;
        if !expiration.is_finite()
            || expiration <= 0.0
            || expiration.fract() != 0.0
            || expiration > u64::MAX as f64
        {
            return Err(CdfError::data(
                "Lake Formation credential expiration is not a whole Unix timestamp",
            ));
        }
        let expiration_unix_seconds = expiration as u64;
        let credentials = Arc::new(AwsCredentials::new(
            decoded.access_key_id.ok_or_else(|| {
                CdfError::auth("Lake Formation credential response omitted AccessKeyId")
            })?,
            decoded.secret_access_key.ok_or_else(|| {
                CdfError::auth("Lake Formation credential response omitted SecretAccessKey")
            })?,
            Some(decoded.session_token.ok_or_else(|| {
                CdfError::auth("Lake Formation credential response omitted SessionToken")
            })?),
        )?);
        if request.partition_values.is_none() {
            let requested = request.s3_path.as_deref().ok_or_else(|| {
                CdfError::internal("validated Lake Formation table request omitted S3Path")
            })?;
            if decoded.vended_s3_path.is_empty()
                || !decoded
                    .vended_s3_path
                    .iter()
                    .any(|scope| s3_scope_contains(scope, requested))
            {
                return Err(CdfError::auth(format!(
                    "Lake Formation vended credentials do not cover selected path `{requested}`"
                )));
            }
        }
        Ok(LakeFormationCredentialResponse {
            credentials,
            expiration_unix_seconds,
            vended_s3_paths: decoded.vended_s3_path,
            bytes_read: u64::try_from(payload.len()).unwrap_or(u64::MAX),
        })
    }
}

impl LakeFormationClient for AwsLakeFormationClient {
    fn vend_credentials(
        &self,
        request: LakeFormationCredentialRequest,
    ) -> BoxFuture<'_, Result<LakeFormationCredentialResponse>> {
        Box::pin(async move { self.request_credentials(request).await })
    }
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct AuditContext<'a> {
    additional_audit_context: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct QuerySessionContext<'a> {
    query_id: &'a str,
    query_start_time: u64,
    query_authorization_id: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct TableCredentialsBody<'a> {
    table_arn: &'a str,
    permissions: [&'static str; 1],
    #[serde(skip_serializing_if = "Option::is_none")]
    duration_seconds: Option<u32>,
    audit_context: AuditContext<'a>,
    supported_permission_types: [&'static str; 1],
    s3_path: &'a str,
    query_session_context: QuerySessionContext<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct PartitionValues<'a> {
    values: &'a [String],
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct PartitionCredentialsBody<'a> {
    table_arn: &'a str,
    partition: PartitionValues<'a>,
    permissions: [&'static str; 1],
    #[serde(skip_serializing_if = "Option::is_none")]
    duration_seconds: Option<u32>,
    audit_context: AuditContext<'a>,
    supported_permission_types: [&'static str; 1],
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CredentialResponse {
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
    expiration: Option<f64>,
    #[serde(default)]
    vended_s3_path: Vec<String>,
}

fn s3_scope_contains(scope: &str, requested: &str) -> bool {
    let scope = scope.trim_end_matches('/');
    requested == scope
        || requested
            .strip_prefix(scope)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn lake_formation_error(operation: &str, status: u16, payload: Option<&[u8]>) -> CdfError {
    let kind = payload
        .and_then(|payload| serde_json::from_slice::<LakeFormationErrorEnvelope>(payload).ok())
        .and_then(|value| value.kind.or(value.code))
        .unwrap_or_else(|| "unknown".to_owned());
    let kind = kind.rsplit(['#', ':']).next().unwrap_or(&kind);
    let message = match kind {
        "PermissionTypeMismatchException" => format!(
            "Lake Formation {operation} requires cell/nested filtering that CDF cannot enforce exactly; query this table through Athena or Trino"
        ),
        _ => format!("Lake Formation {operation} failed ({kind}, HTTP {status})"),
    };
    match (status, kind) {
        (_, "PermissionTypeMismatchException") => CdfError::contract(message),
        (401 | 403, _) | (_, "AccessDeniedException") => CdfError::auth(message),
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
struct LakeFormationErrorEnvelope {
    #[serde(rename = "__type")]
    kind: Option<String>,
    #[serde(rename = "Code")]
    code: Option<String>,
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use cdf_engine::StandaloneExecutionHost;

    use super::*;

    #[derive(Debug)]
    struct RecordingLakeFormationClient {
        calls: AtomicUsize,
        expiration_unix_seconds: u64,
    }

    impl LakeFormationClient for RecordingLakeFormationClient {
        fn vend_credentials(
            &self,
            _request: LakeFormationCredentialRequest,
        ) -> BoxFuture<'_, Result<LakeFormationCredentialResponse>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let expiration = self.expiration_unix_seconds;
            Box::pin(async move {
                Ok(LakeFormationCredentialResponse {
                    credentials: Arc::new(AwsCredentials::new(
                        "fresh-key",
                        "fresh-secret",
                        Some("fresh-token".to_owned()),
                    )?),
                    expiration_unix_seconds: expiration,
                    vended_s3_paths: vec!["s3://bucket/table".to_owned()],
                    bytes_read: 128,
                })
            })
        }
    }

    #[test]
    fn vended_scope_check_respects_path_boundaries() {
        assert!(s3_scope_contains(
            "s3://bucket/table",
            "s3://bucket/table/day=1"
        ));
        assert!(!s3_scope_contains(
            "s3://bucket/table",
            "s3://bucket/table2/day=1"
        ));
    }

    #[test]
    fn unsupported_permission_mode_names_the_exact_query_engine_remediation() {
        let error = lake_formation_error(
            "GetTemporaryGlueTableCredentials",
            400,
            Some(br#"{"__type":"PermissionTypeMismatchException"}"#),
        );
        assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
        assert!(error.message.contains("Athena or Trino"));
    }

    #[test]
    fn credential_response_debug_never_exposes_secret_material() {
        let response = LakeFormationCredentialResponse {
            credentials: Arc::new(
                AwsCredentials::new("AKIA", "secret", Some("token".into())).unwrap(),
            ),
            expiration_unix_seconds: 1,
            vended_s3_paths: vec!["s3://bucket/table".to_owned()],
            bytes_read: 10,
        };
        let rendered = format!("{response:?}");
        assert!(!rendered.contains("secret"));
        assert!(!rendered.contains("token"));
        assert!(!rendered.contains("AKIA"));
    }

    #[test]
    fn expired_credentials_refresh_once_and_remain_cached() {
        let (_host, execution) =
            StandaloneExecutionHost::default_services_with_spill(64 << 20, 64 << 20).unwrap();
        let now = execution.unix_now().as_secs();
        let client = Arc::new(RecordingLakeFormationClient {
            calls: AtomicUsize::new(0),
            expiration_unix_seconds: now + 3600,
        });
        let context = Arc::new(LakeFormationVendingContext {
            client: Arc::clone(&client) as Arc<dyn LakeFormationClient>,
            region: "us-west-2".to_owned(),
            object_region: "us-west-2".to_owned(),
            endpoint: None,
            credentials: None,
            table_arn: "arn:aws:glue:us-west-2:123456789012:table/db/events".to_owned(),
            table_s3_path: "s3://bucket/table".to_owned(),
            authorization: GlueLakeFormationAuthorization {
                query_id: "query-1".to_owned(),
                query_start_unix_seconds: now,
                query_authorization_id: "authorization-1".to_owned(),
                resource_arn: "arn:aws:glue:us-west-2:123456789012:table/db/events".to_owned(),
                authorized_columns: vec!["id".to_owned()],
            },
            duration_seconds: None,
            refresh_margin_seconds: 60,
            maximum_response_bytes: 4096,
            execution,
            cancellation: RunCancellation::default(),
        });
        let provider = LakeFormationAwsCredentialProvider {
            context,
            partition_values: None,
            cached: futures_util::lock::Mutex::new(Some(CachedCredentials {
                credentials: Arc::new(
                    AwsCredentials::new("old-key", "old-secret", Some("old-token".to_owned()))
                        .unwrap(),
                ),
                expiration_unix_seconds: now,
            })),
        };
        let first =
            futures_executor::block_on(cdf_aws::AwsCredentialProvider::credentials(&provider))
                .unwrap();
        let second =
            futures_executor::block_on(cdf_aws::AwsCredentialProvider::credentials(&provider))
                .unwrap();
        assert_eq!(first.access_key_id(), "fresh-key");
        assert_eq!(second.access_key_id(), "fresh-key");
        assert_eq!(client.calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn governed_bindings_are_partition_scoped_runtime_authority_not_secrets() {
        let (_host, execution) =
            StandaloneExecutionHost::default_services_with_spill(64 << 20, 64 << 20).unwrap();
        let source: GlueSourceOptions = serde_json::from_value(serde_json::json!({
            "region": "us-west-2",
            "lake_formation_binding_cache_entries": 2
        }))
        .unwrap();
        source.validate().unwrap();
        let client = Arc::new(RecordingLakeFormationClient {
            calls: AtomicUsize::new(0),
            expiration_unix_seconds: execution.unix_now().as_secs() + 3600,
        });
        let runtime = LakeFormationRuntime::new(
            client,
            &source,
            GlueLakeFormationAuthorization {
                query_id: "query-1".to_owned(),
                query_start_unix_seconds: execution.unix_now().as_secs(),
                query_authorization_id: "authorization-1".to_owned(),
                resource_arn: "arn:aws:glue:us-west-2:123456789012:table/db/events".to_owned(),
                authorized_columns: vec!["id".to_owned()],
            },
            "s3://bucket/table".to_owned(),
            true,
            execution,
            RunCancellation::default(),
        )
        .unwrap();
        let first = runtime
            .binding("s3://bucket/table/day=1/", &[Some("1".to_owned())])
            .unwrap();
        let same = runtime
            .binding(
                "s3://bucket/table/day=1/other.parquet",
                &[Some("1".to_owned())],
            )
            .unwrap();
        let other = runtime
            .binding(
                "s3://bucket/table/day=2/part.parquet",
                &[Some("2".to_owned())],
            )
            .unwrap();
        assert_eq!(first, same);
        assert_ne!(first, other);
        let wrong_partition_path = runtime
            .binding(
                "s3://bucket/table/day=other/part.parquet",
                &[Some("1".to_owned())],
            )
            .unwrap_err();
        assert_eq!(wrong_partition_path.kind, cdf_kernel::ErrorKind::Auth);
        let outside_table = runtime
            .binding(
                "s3://bucket/table2/day=1/part.parquet",
                &[Some("1".to_owned())],
            )
            .unwrap_err();
        assert_eq!(outside_table.kind, cdf_kernel::ErrorKind::Auth);
        let resource = cdf_object_access::FileTransportResource::remote_url(
            "s3://bucket/table/day=1/part.parquet",
        )
        .with_runtime_aws_credentials(first)
        .unwrap();
        assert!(resource.uses_runtime_aws_credentials());
        let rendered = format!("{resource:?}");
        assert!(!rendered.contains("fresh-secret"));
        assert!(!rendered.contains("fresh-token"));
    }
}
