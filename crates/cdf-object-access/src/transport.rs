use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, fs,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, UNIX_EPOCH},
};

use cdf_http::{
    AuthScheme, EgressAllowlist, HeaderMap, HttpMethod, HttpRequest, Redactor, SecretProvider,
    SecretUri, SecretValue,
};
use cdf_kernel::{BoxFuture, CdfError, ErrorKind, FilePosition, InvocationTermination, Result};
use cdf_memory::{
    ConsumerKey, MemoryClass, MemoryCoordinator, MemoryLease, ReservationRequest, reserve,
};
use cdf_runtime::{
    BlockingLaneSpec, ByteSource, ExecutionServices, GenerationStrength, RunCancellation,
    SourceEgressScope,
};
use futures_util::{Stream, TryStreamExt};
use object_store::{ObjectStore, ObjectStoreExt, path::Path as ObjectPath};
use serde::{Deserialize, Serialize};
use url::Url;

const FILE_LIST_CHANNEL_ENTRIES: usize = 32;
const MAX_FILE_LOCATION_BYTES: usize = 64 * 1024;
const MAX_FILE_IDENTITY_FIELD_BYTES: usize = 16 * 1024;
pub const FILE_IDENTITY_MEMORY_ENVELOPE_BYTES: u64 = 144 * 1024;

#[derive(Clone, PartialEq, Eq)]
pub struct FileTransportResource {
    pub location: FileTransportLocation,
    pub egress_allowlist: EgressAllowlist,
    pub auth: Option<AuthScheme>,
    pub credentials: Option<SecretUri>,
    runtime_aws_credentials: Option<RuntimeAwsCredentials>,
}

impl FileTransportResource {
    pub fn new(location: FileTransportLocation) -> Self {
        Self {
            location,
            egress_allowlist: EgressAllowlist::allow_any(),
            auth: None,
            credentials: None,
            runtime_aws_credentials: None,
        }
    }

    pub fn local_path(path: impl AsRef<Path>) -> Self {
        Self::new(FileTransportLocation::LocalPath {
            path: path_to_lossless_string(path.as_ref()),
        })
    }

    pub fn file_url(url: impl Into<String>) -> Self {
        Self::new(FileTransportLocation::FileUrl { url: url.into() })
    }

    pub fn http_url(url: impl Into<String>) -> Self {
        Self::new(FileTransportLocation::HttpUrl { url: url.into() })
    }

    pub fn remote_url(url: impl Into<String>) -> Self {
        Self::new(FileTransportLocation::RemoteUrl { url: url.into() })
    }

    pub fn with_egress_allowlist(mut self, allowlist: EgressAllowlist) -> Self {
        self.egress_allowlist = allowlist;
        self
    }

    pub fn with_auth(mut self, auth: AuthScheme) -> Self {
        self.auth = Some(auth);
        self
    }

    pub fn with_credentials(mut self, credentials: SecretUri) -> Self {
        self.credentials = Some(credentials);
        self
    }

    pub fn with_runtime_aws_credentials(
        mut self,
        credentials: RuntimeAwsCredentials,
    ) -> Result<Self> {
        if self.credentials.is_some() {
            return Err(CdfError::contract(
                "file transport cannot combine static and runtime AWS credentials",
            ));
        }
        self.runtime_aws_credentials = Some(credentials);
        Ok(self)
    }

    pub fn secret_references(&self) -> Vec<&cdf_http::SecretUri> {
        match &self.auth {
            Some(AuthScheme::Bearer { token_uri }) => vec![token_uri],
            Some(AuthScheme::Header { value_uri, .. }) => vec![value_uri],
            None => Vec::new(),
        }
        .into_iter()
        .chain(self.credentials.iter())
        .collect()
    }

    pub fn uses_runtime_aws_credentials(&self) -> bool {
        self.runtime_aws_credentials.is_some()
    }
}

#[derive(Clone)]
pub struct RuntimeAwsCredentials(Arc<RuntimeAwsCredentialsInner>);

struct RuntimeAwsCredentialsInner {
    identity: String,
    options: BTreeMap<String, String>,
    provider: Arc<dyn cdf_aws::AwsCredentialProvider>,
    stores: Mutex<BTreeMap<String, Arc<dyn ObjectStore>>>,
}

impl RuntimeAwsCredentials {
    pub fn new(
        identity: impl Into<String>,
        options: BTreeMap<String, String>,
        provider: Arc<dyn cdf_aws::AwsCredentialProvider>,
    ) -> Result<Self> {
        let identity = identity.into();
        if identity.is_empty() || identity.chars().any(char::is_control) {
            return Err(CdfError::contract(
                "runtime AWS credential identity must be nonempty and control-free",
            ));
        }
        for key in options.keys() {
            let normalized = key.to_ascii_lowercase();
            if matches!(
                normalized.as_str(),
                "aws_access_key_id"
                    | "aws_secret_access_key"
                    | "aws_session_token"
                    | "access_key_id"
                    | "secret_access_key"
                    | "token"
                    | "session_token"
                    | "aws_token"
            ) {
                return Err(CdfError::contract(
                    "runtime AWS credential options cannot contain credential values",
                ));
            }
        }
        Ok(Self(Arc::new(RuntimeAwsCredentialsInner {
            identity,
            options,
            provider,
            stores: Mutex::new(BTreeMap::new()),
        })))
    }

    fn identity(&self) -> &str {
        &self.0.identity
    }

    fn options(&self) -> &BTreeMap<String, String> {
        &self.0.options
    }

    fn provider(&self) -> Arc<dyn cdf_aws::AwsCredentialProvider> {
        Arc::clone(&self.0.provider)
    }

    fn store(&self, origin: &str) -> Result<Option<Arc<dyn ObjectStore>>> {
        self.0
            .stores
            .lock()
            .map(|stores| stores.get(origin).cloned())
            .map_err(|_| CdfError::internal("runtime AWS object-store cache lock is poisoned"))
    }

    fn insert_or_get_store(
        &self,
        origin: String,
        store: Arc<dyn ObjectStore>,
    ) -> Result<Arc<dyn ObjectStore>> {
        let mut stores =
            self.0.stores.lock().map_err(|_| {
                CdfError::internal("runtime AWS object-store cache lock is poisoned")
            })?;
        Ok(Arc::clone(stores.entry(origin).or_insert(store)))
    }
}

impl PartialEq for RuntimeAwsCredentials {
    fn eq(&self, other: &Self) -> bool {
        self.identity() == other.identity() && self.options() == other.options()
    }
}

impl Eq for RuntimeAwsCredentials {}

impl fmt::Debug for RuntimeAwsCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeAwsCredentials")
            .field("identity", &self.identity())
            .field("option_keys", &self.options().keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for FileTransportResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FileTransportResource")
            .field("location", &self.location)
            .field("egress_allowlist", &self.egress_allowlist)
            .field(
                "auth",
                &self.auth.as_ref().map(|auth| match auth {
                    AuthScheme::Bearer { .. } => "bearer",
                    AuthScheme::Header { .. } => "header",
                }),
            )
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FileTransportLocation {
    LocalPath { path: String },
    FileUrl { url: String },
    HttpUrl { url: String },
    RemoteUrl { url: String },
}

impl fmt::Debug for FileTransportLocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LocalPath { path } => formatter
                .debug_struct("LocalPath")
                .field("path", &redacted_location_for_debug(path))
                .finish(),
            Self::FileUrl { url } => formatter
                .debug_struct("FileUrl")
                .field("url", &redacted_location_for_debug(url))
                .finish(),
            Self::HttpUrl { .. } => formatter
                .debug_struct("HttpUrl")
                .field("url", &"<opaque HTTP URL>")
                .finish(),
            Self::RemoteUrl { url } => formatter
                .debug_struct("RemoteUrl")
                .field("url", &redacted_location_for_debug(url))
                .finish(),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileIdentityMetadata {
    pub location: String,
    pub size_bytes: Option<u64>,
    pub checksum: Option<FileChecksum>,
    pub etag: Option<String>,
    pub version: Option<String>,
    pub modified: Option<String>,
    /// True only when the provider has positively attested independent byte-range support.
    pub exact_ranges: bool,
}

impl FileIdentityMetadata {
    pub fn validate(&self) -> Result<()> {
        validate_identity_text(
            "file location",
            &self.location,
            MAX_FILE_LOCATION_BYTES,
            false,
        )?;
        for (label, value) in [
            ("file ETag", self.etag.as_deref()),
            ("file object version", self.version.as_deref()),
            ("file modification identity", self.modified.as_deref()),
        ] {
            if let Some(value) = value {
                validate_identity_text(label, value, MAX_FILE_IDENTITY_FIELD_BYTES, false)?;
            }
        }
        if let Some(checksum) = &self.checksum {
            validate_identity_text("file checksum algorithm", &checksum.algorithm, 64, true)?;
            validate_identity_text(
                "file checksum value",
                &checksum.value,
                MAX_FILE_IDENTITY_FIELD_BYTES,
                true,
            )?;
        }
        Ok(())
    }

    pub fn file_position_evidence(&self) -> Result<FilePosition> {
        let size_bytes = self.size_bytes.ok_or_else(|| {
            CdfError::data(format!(
                "file metadata for `{}` is missing byte size evidence",
                self.location
            ))
        })?;
        Ok(FilePosition {
            path: self.location.clone(),
            size_bytes,
            source_generation: (self.generation_strength() == GenerationStrength::Weak)
                .then(|| self.modified.clone())
                .flatten(),
            etag: self.etag.clone(),
            object_version: self.version.clone(),
            sha256: self.sha256().map(str::to_owned),
        })
    }

    pub fn sha256(&self) -> Option<&str> {
        self.checksum
            .as_ref()
            .filter(|checksum| checksum.algorithm == "sha256")
            .map(|checksum| checksum.value.as_str())
    }

    pub fn generation_strength(&self) -> GenerationStrength {
        if self.sha256().is_some() {
            GenerationStrength::ContentAddressed
        } else if self.etag.is_some() || self.version.is_some() {
            GenerationStrength::Strong
        } else {
            GenerationStrength::Weak
        }
    }
}

impl fmt::Debug for FileIdentityMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FileIdentityMetadata")
            .field("location", &redacted_location_for_debug(&self.location))
            .field("size_bytes", &self.size_bytes)
            .field("checksum", &self.checksum)
            .field("etag", &self.etag)
            .field("version", &self.version)
            .field("modified", &self.modified)
            .finish()
    }
}

/// Metadata observed for a logical file together with the concrete location that may be used to
/// read that observation. The access location is deliberately separate from identity: HTTP
/// redirects commonly contain short-lived signed URLs which must never become package evidence.
#[derive(Clone, PartialEq, Eq)]
pub struct FileMetadataObservation {
    identity: FileIdentityMetadata,
    access_location: OpaqueAccessLocation,
    forward_auth: bool,
}

#[derive(Clone, PartialEq, Eq)]
struct OpaqueAccessLocation(FileTransportLocation);

impl fmt::Debug for OpaqueAccessLocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<opaque transport location>")
    }
}

impl fmt::Debug for FileMetadataObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FileMetadataObservation")
            .field("identity", &self.identity)
            .field("access_location", &self.access_location)
            .field("forward_auth", &self.forward_auth)
            .finish()
    }
}

impl FileMetadataObservation {
    pub fn direct(resource: &FileTransportResource, identity: FileIdentityMetadata) -> Self {
        Self {
            identity,
            access_location: OpaqueAccessLocation(resource.location.clone()),
            forward_auth: true,
        }
    }

    pub fn access_resource(&self, logical: &FileTransportResource) -> FileTransportResource {
        let mut resource = logical.clone();
        resource.location = self.access_location.0.clone();
        if !self.forward_auth {
            resource.auth = None;
        }
        resource
    }

    pub fn identity(&self) -> &FileIdentityMetadata {
        &self.identity
    }

    pub fn into_identity(self) -> FileIdentityMetadata {
        self.identity
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileChecksum {
    pub algorithm: String,
    pub value: String,
}

pub struct FileIdentityStream {
    inner: Pin<Box<dyn Stream<Item = Result<AccountedFileIdentity>> + Send + 'static>>,
    termination: InvocationTermination,
    terminal: bool,
}

impl FileIdentityStream {
    pub fn scoped(stream: cdf_runtime::ScopedTaskStream<AccountedFileIdentity>) -> Self {
        let termination = stream.termination();
        Self {
            inner: Box::pin(stream),
            termination,
            terminal: false,
        }
    }

    pub fn materialized(
        stream: impl Stream<Item = Result<AccountedFileIdentity>> + Send + 'static,
    ) -> Self {
        Self {
            inner: Box::pin(stream),
            termination: InvocationTermination::completed(),
            terminal: false,
        }
    }

    pub fn termination(&self) -> InvocationTermination {
        self.termination.clone()
    }
}

impl Stream for FileIdentityStream {
    type Item = Result<AccountedFileIdentity>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let next = self.inner.as_mut().poll_next(context);
        if matches!(next, Poll::Ready(None)) {
            self.terminal = true;
        }
        next
    }
}

#[derive(Debug)]
pub struct AccountedFileIdentity {
    identity: FileIdentityMetadata,
    _lease: MemoryLease,
}

impl AccountedFileIdentity {
    pub fn new(identity: FileIdentityMetadata, lease: MemoryLease) -> Result<Self> {
        identity.validate()?;
        if lease.bytes() < FILE_IDENTITY_MEMORY_ENVELOPE_BYTES {
            return Err(CdfError::internal(
                "file identity metadata lease is smaller than its fixed envelope",
            ));
        }
        Ok(Self {
            identity,
            _lease: lease,
        })
    }

    pub fn identity(&self) -> &FileIdentityMetadata {
        &self.identity
    }

    pub fn into_identity(self) -> FileIdentityMetadata {
        self.identity
    }
}

impl std::ops::Deref for AccountedFileIdentity {
    type Target = FileIdentityMetadata;

    fn deref(&self) -> &Self::Target {
        &self.identity
    }
}

impl Drop for FileIdentityStream {
    fn drop(&mut self) {
        if !self.terminal {
            self.termination.cancel();
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct FileTransportControl {
    cancellation: RunCancellation,
    deadline: Option<Duration>,
}

impl FileTransportControl {
    pub fn new(cancellation: RunCancellation, deadline: Option<Duration>) -> Self {
        Self {
            cancellation,
            deadline,
        }
    }

    pub fn cancellation(&self) -> RunCancellation {
        self.cancellation.clone()
    }

    pub fn check(&self, execution: Option<&ExecutionServices>) -> Result<()> {
        self.cancellation.check()?;
        if let Some(deadline) = self.deadline {
            let execution = execution.ok_or_else(|| {
                CdfError::contract("deadline-bound file transport requires ExecutionServices")
            })?;
            if execution.monotonic_now() >= deadline {
                return Err(CdfError::data(
                    "file transport operation exceeded its deadline",
                ));
            }
        }
        Ok(())
    }

    fn remaining(&self, execution: &ExecutionServices) -> Result<Option<Duration>> {
        self.check(Some(execution))?;
        Ok(self
            .deadline
            .map(|deadline| deadline.saturating_sub(execution.monotonic_now())))
    }
}

pub trait FileTransport: Send + Sync {
    fn metadata(
        &self,
        egress: &SourceEgressScope,
        resource: &FileTransportResource,
        control: &FileTransportControl,
    ) -> Result<FileMetadataObservation>;
    fn metadata_if_exists(
        &self,
        egress: &SourceEgressScope,
        resource: &FileTransportResource,
        control: &FileTransportControl,
    ) -> Result<Option<FileMetadataObservation>> {
        self.metadata(egress, resource, control).map(Some)
    }
    fn list(
        &self,
        egress: &SourceEgressScope,
        resource: &FileTransportResource,
        maximum_results: usize,
        control: &FileTransportControl,
    ) -> Result<FileIdentityStream>;
    fn open_byte_source(
        &self,
        egress: &SourceEgressScope,
        resource: &FileTransportResource,
        expected: &FileIdentityMetadata,
        memory: Arc<dyn MemoryCoordinator>,
    ) -> Result<Arc<dyn ByteSource>>;
}

pub trait HttpFileTransport: Send + Sync {
    /// Sends a request and returns only its status and headers. Implementations MUST NOT buffer or
    /// drain the response body; metadata fallback requests may be answered with the full object.
    fn send_headers(
        &self,
        request: HttpFileRequest,
    ) -> BoxFuture<'static, Result<HttpFileResponse>>;
    fn open_byte_source(
        &self,
        resource: &FileTransportResource,
        expected: &FileIdentityMetadata,
        auth: Option<ResolvedHttpAuth>,
        memory: Arc<dyn MemoryCoordinator>,
    ) -> Result<Arc<dyn ByteSource>>;
}

#[derive(Clone)]
pub struct ResolvedHttpAuth {
    scheme: AuthScheme,
    value: Arc<SecretValue>,
}

impl ResolvedHttpAuth {
    fn new(scheme: AuthScheme, value: SecretValue) -> Self {
        Self {
            scheme,
            value: Arc::new(value),
        }
    }

    pub fn apply(&self, request: &mut HttpFileRequest) -> Result<()> {
        let (name, value) = match &self.scheme {
            AuthScheme::Bearer { .. } => (
                "authorization".to_owned(),
                format!("Bearer {}", self.value.as_str()?),
            ),
            AuthScheme::Header { name, .. } => {
                (name.to_ascii_lowercase(), self.value.as_str()?.to_owned())
            }
        };
        if name.is_empty()
            || name
                .bytes()
                .any(|byte| !byte.is_ascii_alphanumeric() && !matches!(byte, b'-' | b'_'))
        {
            return Err(CdfError::contract(
                "HTTP auth header name must contain only ASCII letters, digits, `-`, or `_`",
            ));
        }
        request.headers.insert(name.clone(), value);
        request.sensitive_headers.insert(name);
        Ok(())
    }
}

impl fmt::Debug for ResolvedHttpAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ResolvedHttpAuth([REDACTED])")
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct HttpFileRequest {
    pub method: HttpMethod,
    pub url: String,
    pub headers: HeaderMap,
    sensitive_headers: BTreeSet<String>,
}

impl HttpFileRequest {
    pub fn new(method: HttpMethod, url: impl Into<String>) -> Self {
        Self {
            method,
            url: url.into(),
            headers: HeaderMap::new(),
            sensitive_headers: BTreeSet::new(),
        }
    }

    fn strip_sensitive_headers(&mut self) {
        for name in std::mem::take(&mut self.sensitive_headers) {
            self.headers.remove(&name);
        }
    }
}

impl fmt::Debug for HttpFileRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let redactor = Redactor::default();
        let mut headers = redactor.redact_headers(&self.headers);
        for name in &self.sensitive_headers {
            if let Some(value) = headers.get_mut(name) {
                *value = "[REDACTED]".to_owned();
            }
        }
        formatter
            .debug_struct("HttpFileRequest")
            .field("method", &self.method)
            .field("url", &"<opaque HTTP URL>")
            .field("headers", &headers)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct HttpFileResponse {
    pub status: u16,
    pub headers: HeaderMap,
}

impl HttpFileResponse {
    pub fn new(status: u16) -> Self {
        Self {
            status,
            headers: HeaderMap::new(),
        }
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        set_header(&mut self.headers, name, value);
        self
    }
}

impl fmt::Debug for HttpFileResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut headers = Redactor::default().redact_headers(&self.headers);
        if let Some((_, location)) = headers
            .iter_mut()
            .find(|(name, _)| name.eq_ignore_ascii_case("location"))
        {
            *location = "[REDACTED REDIRECT LOCATION]".to_owned();
        }
        formatter
            .debug_struct("HttpFileResponse")
            .field("status", &self.status)
            .field("headers", &headers)
            .finish()
    }
}

#[derive(Clone, Default)]
pub struct ObjectStoreClientPool {
    clients: Arc<Mutex<BTreeMap<String, Arc<dyn ObjectStore>>>>,
}

impl ObjectStoreClientPool {
    fn get(&self, key: &str) -> Result<Option<Arc<dyn ObjectStore>>> {
        self.clients
            .lock()
            .map(|clients| clients.get(key).cloned())
            .map_err(|_| CdfError::internal("object-store client pool lock is poisoned"))
    }

    fn insert_or_get(
        &self,
        key: String,
        client: Arc<dyn ObjectStore>,
    ) -> Result<Arc<dyn ObjectStore>> {
        let mut clients = self
            .clients
            .lock()
            .map_err(|_| CdfError::internal("object-store client pool lock is poisoned"))?;
        Ok(Arc::clone(clients.entry(key).or_insert(client)))
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.clients.lock().unwrap().len()
    }
}

#[derive(Default)]
pub struct FileTransportFacade {
    http: Option<Box<dyn HttpFileTransport>>,
    secret_provider: Option<Arc<dyn SecretProvider + Send + Sync>>,
    object_stores: BTreeMap<String, Arc<dyn ObjectStore>>,
    object_store_clients: ObjectStoreClientPool,
    execution: Option<cdf_runtime::ExecutionServices>,
    local_listing_lane: Option<BlockingLaneSpec>,
}

impl FileTransportFacade {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_http_transport(mut self, transport: impl HttpFileTransport + 'static) -> Self {
        self.http = Some(Box::new(transport));
        self
    }

    pub fn with_secret_provider(
        mut self,
        provider: impl SecretProvider + Send + Sync + 'static,
    ) -> Self {
        self.secret_provider = Some(Arc::new(provider));
        self
    }

    pub fn with_shared_secret_provider(
        mut self,
        provider: Arc<dyn SecretProvider + Send + Sync>,
    ) -> Self {
        self.secret_provider = Some(provider);
        self
    }

    pub fn with_object_store(
        mut self,
        origin: impl Into<String>,
        store: Arc<dyn ObjectStore>,
    ) -> Self {
        self.object_stores.insert(origin.into(), store);
        self
    }

    pub fn with_shared_object_store_clients(mut self, pool: ObjectStoreClientPool) -> Self {
        self.object_store_clients = pool;
        self
    }

    pub fn with_execution_services(mut self, execution: cdf_runtime::ExecutionServices) -> Self {
        self.execution = Some(execution);
        self
    }

    /// Selects the caller-owned blocking lane used for local directory traversal.
    ///
    /// Object access owns the blocking operation, while the source adapter owns its scheduling
    /// policy. Requiring an injected lane prevents a neutral transport from inventing a hidden
    /// executor or source-specific concurrency default.
    pub fn with_local_listing_lane(mut self, lane: BlockingLaneSpec) -> Result<Self> {
        lane.validate()?;
        self.local_listing_lane = Some(lane);
        Ok(self)
    }
}

impl fmt::Debug for FileTransportFacade {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FileTransportFacade")
            .field("http", &self.http.is_some())
            .field("secret_provider", &self.secret_provider.is_some())
            .field("object_store_count", &self.object_stores.len())
            .field(
                "pooled_object_store_count",
                &self
                    .object_store_clients
                    .clients
                    .lock()
                    .map(|clients| clients.len())
                    .ok(),
            )
            .field("execution_services", &self.execution.is_some())
            .field(
                "local_listing_lane",
                &self.local_listing_lane.as_ref().map(|lane| &lane.lane_id),
            )
            .finish()
    }
}

impl FileTransport for FileTransportFacade {
    fn metadata(
        &self,
        egress: &SourceEgressScope,
        resource: &FileTransportResource,
        control: &FileTransportControl,
    ) -> Result<FileMetadataObservation> {
        control.check(self.execution.as_ref())?;
        match &resource.location {
            FileTransportLocation::LocalPath { path } => local_metadata(Path::new(path))
                .map(|identity| FileMetadataObservation::direct(resource, identity)),
            FileTransportLocation::FileUrl { url } => local_metadata(&file_url_path(url)?)
                .map(|identity| FileMetadataObservation::direct(resource, identity)),
            FileTransportLocation::HttpUrl { url } => {
                self.http_metadata(egress, resource, url, control)
            }
            FileTransportLocation::RemoteUrl { url } => self
                .object_store_metadata(egress, resource, url, control)
                .map(|identity| FileMetadataObservation::direct(resource, identity)),
        }
    }

    fn metadata_if_exists(
        &self,
        egress: &SourceEgressScope,
        resource: &FileTransportResource,
        control: &FileTransportControl,
    ) -> Result<Option<FileMetadataObservation>> {
        match &resource.location {
            FileTransportLocation::HttpUrl { url } => {
                self.http_metadata_if_exists(egress, resource, url, control)
            }
            _ => self.metadata(egress, resource, control).map(Some),
        }
    }

    fn list(
        &self,
        egress: &SourceEgressScope,
        resource: &FileTransportResource,
        maximum_results: usize,
        control: &FileTransportControl,
    ) -> Result<FileIdentityStream> {
        control.check(self.execution.as_ref())?;
        match &resource.location {
            FileTransportLocation::LocalPath { path } => {
                self.list_local(PathBuf::from(path), maximum_results, control.clone())
            }
            FileTransportLocation::FileUrl { url } => {
                self.list_local(file_url_path(url)?, maximum_results, control.clone())
            }
            FileTransportLocation::HttpUrl { .. } => Err(CdfError::contract(
                "HTTP(S) file transport does not support arbitrary directory listing; use an explicit URL or a ratified template/range enumerator",
            )),
            FileTransportLocation::RemoteUrl { url } => {
                self.list_object_store(egress, resource, url, maximum_results, control.clone())
            }
        }
    }

    fn open_byte_source(
        &self,
        egress: &SourceEgressScope,
        resource: &FileTransportResource,
        expected: &FileIdentityMetadata,
        memory: Arc<dyn MemoryCoordinator>,
    ) -> Result<Arc<dyn ByteSource>> {
        let (source, origin): (Arc<dyn ByteSource>, Option<String>) = match &resource.location {
            FileTransportLocation::LocalPath { path } => {
                (Arc::new(crate::LocalByteSource::open(path, memory)?), None)
            }
            FileTransportLocation::FileUrl { url } => (
                Arc::new(crate::LocalByteSource::open(file_url_path(url)?, memory)?),
                None,
            ),
            FileTransportLocation::RemoteUrl { url } => {
                let (store, path, origin) = self.resolve_object_store(egress, resource, url)?;
                (
                    Arc::new(crate::ObjectStoreByteSource::new(
                        store,
                        path,
                        expected.clone(),
                        memory,
                    )?),
                    Some(origin),
                )
            }
            FileTransportLocation::HttpUrl { url } => {
                egress.authorize(url)?;
                let auth = self.resolve_http_auth(resource)?;
                (
                    self.http_transport()?
                        .open_byte_source(resource, expected, auth, memory)?,
                    Some(http_origin(url)?),
                )
            }
        };
        let Some(origin) = origin else {
            return Ok(source);
        };
        if !source.capabilities().exact_ranges
            || expected.generation_strength() == GenerationStrength::Weak
        {
            return Ok(source);
        }
        let limits = cdf_runtime::SourceIoControllerLimits::automatic(
            source.capabilities().useful_range_concurrency,
        )?;
        Ok(Arc::new(cdf_runtime::ControlledByteSource::new(
            source,
            origin,
            self.execution()?.clone(),
            limits,
            cdf_runtime::SourceRetryPolicy::default(),
        )?))
    }
}

fn http_origin(url: &str) -> Result<String> {
    let parsed = Url::parse(url)
        .map_err(|error| CdfError::contract(format!("invalid HTTP file URL: {error}")))?;
    let host = parsed
        .host_str()
        .ok_or_else(|| CdfError::contract("HTTP file URL must name a host"))?;
    let port = parsed
        .port()
        .map_or_else(String::new, |port| format!(":{port}"));
    Ok(format!("{}://{host}{port}", parsed.scheme()))
}

impl FileTransportFacade {
    fn http_metadata_if_exists(
        &self,
        egress: &SourceEgressScope,
        resource: &FileTransportResource,
        url: &str,
        control: &FileTransportControl,
    ) -> Result<Option<FileMetadataObservation>> {
        self.probe_http_metadata(egress, resource, url, true, control)
    }

    fn object_store_metadata(
        &self,
        egress: &SourceEgressScope,
        resource: &FileTransportResource,
        url: &str,
        control: &FileTransportControl,
    ) -> Result<FileIdentityMetadata> {
        let (store, path, _) = self.resolve_object_store(egress, resource, url)?;
        let metadata = self
            .controlled_io(control, async move { Ok(store.head(&path).await) })?
            .map_err(|error| object_store_error("read object metadata", error))?;
        Ok(object_identity(url.to_owned(), metadata))
    }

    fn list_object_store(
        &self,
        egress: &SourceEgressScope,
        resource: &FileTransportResource,
        url: &str,
        maximum_results: usize,
        control: FileTransportControl,
    ) -> Result<FileIdentityStream> {
        let (store, prefix, origin) = self.resolve_object_store(egress, resource, url)?;
        let execution = self.execution()?.clone();
        let stream = execution.clone().spawn_io_stream(
            "file-object-store-list",
            FILE_LIST_CHANNEL_ENTRIES,
            move |output, cancellation| {
                publish_object_store_listing(
                    store.list(Some(&prefix)),
                    origin,
                    maximum_results,
                    output,
                    cancellation,
                    control,
                    execution.clone(),
                )
            },
        )?;
        Ok(FileIdentityStream::scoped(stream))
    }

    fn list_local(
        &self,
        path: PathBuf,
        maximum_results: usize,
        control: FileTransportControl,
    ) -> Result<FileIdentityStream> {
        let execution = self.execution()?.clone();
        let lane = self.local_listing_lane.as_ref().ok_or_else(|| {
            CdfError::contract(
                "local object listing requires an injected blocking-lane specification",
            )
        })?;
        execution.ensure_blocking_lanes(std::slice::from_ref(lane))?;
        let stream = execution.clone().spawn_blocking_stream(
            "file-local-list",
            &lane.lane_id,
            FILE_LIST_CHANNEL_ENTRIES,
            move |output, cancellation| {
                control.check(Some(&execution))?;
                cancellation.check()?;
                let metadata = fs::metadata(&path).map_err(|error| {
                    CdfError::data(format!(
                        "stat local file source {}: {error}",
                        path.display()
                    ))
                })?;
                if metadata.is_file() {
                    if maximum_results == 0 {
                        return Err(CdfError::data(
                            "file inventory exceeds the 0-entry boundary",
                        ));
                    }
                    let lease = reserve_file_identity_envelope(&execution, &control)?;
                    let identity = local_metadata(&path)?;
                    send_blocking_file_identity(
                        &output,
                        AccountedFileIdentity::new(identity, lease)?,
                        &execution,
                        &control,
                    )?;
                    return Ok(());
                }
                if !metadata.is_dir() {
                    return Err(CdfError::data(format!(
                        "local file transport path {} is neither a file nor a directory",
                        path.display()
                    )));
                }
                let mut emitted = 0_usize;
                for entry in fs::read_dir(&path).map_err(|error| {
                    CdfError::data(format!(
                        "read local file source directory {}: {error}",
                        path.display()
                    ))
                })? {
                    control.check(Some(&execution))?;
                    cancellation.check()?;
                    let entry = entry.map_err(|error| {
                        CdfError::data(format!(
                            "read local file source directory {}: {error}",
                            path.display()
                        ))
                    })?;
                    if !entry.path().is_file() {
                        continue;
                    }
                    if emitted == maximum_results {
                        return Err(CdfError::data(format!(
                            "file inventory exceeds the {maximum_results}-entry boundary"
                        )));
                    }
                    let lease = reserve_file_identity_envelope(&execution, &control)?;
                    let identity = local_metadata(&entry.path())?;
                    send_blocking_file_identity(
                        &output,
                        AccountedFileIdentity::new(identity, lease)?,
                        &execution,
                        &control,
                    )?;
                    emitted += 1;
                }
                Ok(())
            },
        )?;
        Ok(FileIdentityStream::scoped(stream))
    }

    fn resolve_object_store(
        &self,
        egress: &SourceEgressScope,
        resource: &FileTransportResource,
        url: &str,
    ) -> Result<(Arc<dyn ObjectStore>, ObjectPath, String)> {
        let parsed = Url::parse(url)
            .map_err(|error| CdfError::contract(format!("invalid object-store URL: {error}")))?;
        if !matches!(parsed.scheme(), "s3" | "gs" | "az") {
            return Err(CdfError::contract(
                "object-store file URLs must use s3://, gs://, or az://",
            ));
        }
        let host = parsed
            .host_str()
            .ok_or_else(|| CdfError::contract("object-store URL must name a bucket/account"))?;
        let origin = format!("{}://{}", parsed.scheme(), host);
        egress.authorize(url)?;
        let policy = HttpRequest::new(HttpMethod::Get, format!("https://{host}/"));
        resource.egress_allowlist.check(&policy)?;
        if resource.credentials.is_some() && resource.runtime_aws_credentials.is_some() {
            return Err(CdfError::contract(
                "file transport cannot combine static and runtime AWS credentials",
            ));
        }
        if let Some(credentials) = &resource.runtime_aws_credentials {
            if parsed.scheme() != "s3" {
                return Err(CdfError::contract(
                    "runtime AWS credentials can only authorize s3:// resources",
                ));
            }
            let store = match credentials.store(&origin)? {
                Some(store) => store,
                None => {
                    let mut builder = object_store::aws::AmazonS3Builder::new()
                        .with_url(url)
                        .with_credentials(Arc::new(ObjectStoreAwsCredentialProvider {
                            provider: credentials.provider(),
                        }));
                    for (key, value) in credentials.options() {
                        let key = key
                            .parse::<object_store::aws::AmazonS3ConfigKey>()
                            .map_err(|error| {
                                CdfError::contract(format!(
                                    "invalid runtime S3 option `{key}`: {error}"
                                ))
                            })?;
                        builder = builder.with_config(key, value);
                    }
                    let store: Arc<dyn ObjectStore> =
                        Arc::new(builder.build().map_err(|error| {
                            CdfError::auth(format!(
                                "configure S3 transport with runtime credentials: {error}"
                            ))
                        })?);
                    credentials.insert_or_get_store(origin.clone(), store)?
                }
            };
            return Ok((
                store,
                ObjectPath::parse(parsed.path().trim_start_matches('/'))
                    .map_err(|error| CdfError::contract(format!("parse object path: {error}")))?,
                origin,
            ));
        }
        if let Some(store) = self.object_stores.get(&origin) {
            return Ok((
                Arc::clone(store),
                ObjectPath::parse(parsed.path().trim_start_matches('/'))
                    .map_err(|error| CdfError::contract(format!("parse object path: {error}")))?,
                origin,
            ));
        }
        let pool_key = format!(
            "{origin}\0{}",
            resource
                .credentials
                .as_ref()
                .map_or("anonymous", SecretUri::as_str)
        );
        if let Some(store) = self.object_store_clients.get(&pool_key)? {
            return Ok((
                store,
                ObjectPath::parse(parsed.path().trim_start_matches('/'))
                    .map_err(|error| CdfError::contract(format!("parse object path: {error}")))?,
                origin,
            ));
        }
        let options = match &resource.credentials {
            Some(reference) => {
                let provider = self.secret_provider.as_ref().ok_or_else(|| {
                    CdfError::auth("object-store credentials require a secret provider")
                })?;
                let value = provider.resolve(reference)?.as_str()?.to_owned();
                serde_json::from_str::<BTreeMap<String, String>>(&value).map_err(|_| {
                    CdfError::auth(
                        "object-store credential secret must be a JSON object of provider options",
                    )
                })?
            }
            None => BTreeMap::new(),
        };
        let (store, path) = object_store::parse_url_opts(&parsed, options)
            .map_err(|error| object_store_error("configure object store", error))?;
        let store = self
            .object_store_clients
            .insert_or_get(pool_key, Arc::from(store))?;
        Ok((store, path, origin))
    }

    fn execution(&self) -> Result<&cdf_runtime::ExecutionServices> {
        self.execution
            .as_ref()
            .ok_or_else(|| CdfError::contract("file transport requires injected ExecutionServices"))
    }

    fn controlled_io<T, F>(&self, control: &FileTransportControl, future: F) -> Result<T>
    where
        T: Send + 'static,
        F: std::future::Future<Output = Result<T>> + Send + 'static,
    {
        let execution = self.execution()?.clone();
        let control = control.clone();
        execution
            .clone()
            .run_io(async move { await_controlled(&control, &execution, future).await })
    }

    fn http_metadata(
        &self,
        egress: &SourceEgressScope,
        resource: &FileTransportResource,
        url: &str,
        control: &FileTransportControl,
    ) -> Result<FileMetadataObservation> {
        self.probe_http_metadata(egress, resource, url, false, control)?
            .ok_or_else(|| CdfError::data("HTTP file transport resource does not exist"))
    }

    fn probe_http_metadata(
        &self,
        egress: &SourceEgressScope,
        resource: &FileTransportResource,
        logical_url: &str,
        missing_is_none: bool,
        control: &FileTransportControl,
    ) -> Result<Option<FileMetadataObservation>> {
        const MAX_REDIRECTS: usize = 10;

        validate_http_file_url(logical_url)?;
        let auth = self.resolve_http_auth(resource)?;
        let mut head = HttpFileRequest::new(HttpMethod::Head, logical_url.to_owned());
        if let Some(auth) = &auth {
            auth.apply(&mut head)?;
        }
        let (mut access_url, mut response) =
            self.send_headers_following_redirects(egress, resource, head, MAX_REDIRECTS, control)?;
        if response.status == 404 && missing_is_none {
            return Ok(None);
        }

        // Some public object frontends reject HEAD or omit useful length metadata. Probe one byte
        // without reading the body so even a server that ignores Range cannot make discovery
        // download or buffer the object.
        if matches!(response.status, 400 | 403 | 405 | 501)
            || ((200..=299).contains(&response.status)
                && optional_u64_header(&response.headers, "content-length")?.is_none())
        {
            let mut get = HttpFileRequest::new(HttpMethod::Get, logical_url.to_owned());
            if let Some(auth) = &auth {
                auth.apply(&mut get)?;
            }
            set_header(&mut get.headers, "range", "bytes=0-0");
            set_header(&mut get.headers, "accept-encoding", "identity");
            (access_url, response) = self.send_headers_following_redirects(
                egress,
                resource,
                get,
                MAX_REDIRECTS,
                control,
            )?;
            if response.status == 404 && missing_is_none {
                return Ok(None);
            }
            ensure_http_success(HttpMethod::Get, &response)?;
        } else {
            ensure_http_success(HttpMethod::Head, &response)?;
        }

        let identity = http_identity(logical_url, &response)?;
        Ok(Some(FileMetadataObservation {
            identity,
            access_location: OpaqueAccessLocation(FileTransportLocation::HttpUrl {
                url: access_url.clone(),
            }),
            forward_auth: same_http_origin(logical_url, &access_url)?,
        }))
    }

    fn send_headers_following_redirects(
        &self,
        egress: &SourceEgressScope,
        resource: &FileTransportResource,
        mut request: HttpFileRequest,
        maximum_redirects: usize,
        control: &FileTransportControl,
    ) -> Result<(String, HttpFileResponse)> {
        for redirect_count in 0..=maximum_redirects {
            validate_http_file_url(&request.url)?;
            egress.authorize(&request.url)?;
            resource.egress_allowlist.check(&policy_request(&request))?;
            let response = self.controlled_io(
                control,
                self.http_transport()?.send_headers(request.clone()),
            )?;
            if !matches!(response.status, 301 | 302 | 303 | 307 | 308) {
                return Ok((request.url, response));
            }
            if redirect_count == maximum_redirects {
                return Err(CdfError::data(format!(
                    "HTTP file transport exceeded {maximum_redirects} redirects"
                )));
            }
            let location = header_value(&response.headers, "location").ok_or_else(|| {
                CdfError::data("HTTP file transport redirect omitted the Location header")
            })?;
            let base = Url::parse(&request.url)
                .map_err(|error| CdfError::contract(format!("invalid HTTP file URL: {error}")))?;
            let target = base.join(location).map_err(|error| {
                CdfError::data(format!("HTTP file transport redirect is invalid: {error}"))
            })?;
            if !same_http_origin(&request.url, target.as_str())? {
                request.strip_sensitive_headers();
            }
            request.url = target.into();
        }
        Err(CdfError::internal(
            "HTTP file transport redirect loop exhausted unexpectedly",
        ))
    }

    fn http_transport(&self) -> Result<&dyn HttpFileTransport> {
        self.http
            .as_deref()
            .map(|transport| transport as &dyn HttpFileTransport)
            .ok_or_else(|| {
                CdfError::contract(
                    "HTTP(S) file resources require an explicit HttpFileTransport dependency",
                )
            })
    }

    fn resolve_http_auth(
        &self,
        resource: &FileTransportResource,
    ) -> Result<Option<ResolvedHttpAuth>> {
        let Some(scheme) = &resource.auth else {
            return Ok(None);
        };
        let provider = self
            .secret_provider
            .as_ref()
            .ok_or_else(|| CdfError::auth("HTTP file auth requires an injected secret provider"))?;
        let reference = match scheme {
            AuthScheme::Bearer { token_uri } => token_uri,
            AuthScheme::Header { value_uri, .. } => value_uri,
        };
        Ok(Some(ResolvedHttpAuth::new(
            scheme.clone(),
            provider.resolve(reference)?,
        )))
    }
}

#[derive(Clone)]
struct ObjectStoreAwsCredentialProvider {
    provider: Arc<dyn cdf_aws::AwsCredentialProvider>,
}

impl fmt::Debug for ObjectStoreAwsCredentialProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ObjectStoreAwsCredentialProvider([RUNTIME AUTHORITY])")
    }
}

#[async_trait::async_trait]
impl object_store::CredentialProvider for ObjectStoreAwsCredentialProvider {
    type Credential = object_store::aws::AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let credentials =
            self.provider
                .credentials()
                .await
                .map_err(|error| object_store::Error::Generic {
                    store: "CDF runtime AWS credential provider",
                    source: Box::new(error),
                })?;
        Ok(Arc::new(object_store::aws::AwsCredential {
            key_id: credentials.access_key_id().to_owned(),
            secret_key: credentials.secret_access_key().to_owned(),
            token: credentials.session_token().map(str::to_owned),
        }))
    }
}

async fn publish_object_store_listing(
    mut objects: futures_util::stream::BoxStream<
        'static,
        object_store::Result<object_store::ObjectMeta>,
    >,
    origin: String,
    maximum_results: usize,
    mut output: cdf_runtime::TaskStreamSender<AccountedFileIdentity>,
    cancellation: cdf_runtime::RunCancellation,
    control: FileTransportControl,
    execution: ExecutionServices,
) -> Result<()> {
    let mut emitted = 0_usize;
    loop {
        let lease = await_controlled(
            &control,
            &execution,
            cancellation.await_or_cancel(reserve(
                execution.memory(),
                ReservationRequest::new(
                    ConsumerKey::new("file-identity-metadata", MemoryClass::Discovery)?,
                    FILE_IDENTITY_MEMORY_ENVELOPE_BYTES,
                )?,
            )),
        )
        .await?;
        let operation = cancellation.await_or_cancel(async {
            objects
                .try_next()
                .await
                .map_err(|error| object_store_error("list object prefix", error))
        });
        let next = await_controlled(&control, &execution, operation).await?;
        let Some(metadata) = next else {
            drop(lease);
            return Ok(());
        };
        if emitted == maximum_results {
            return Err(CdfError::data(format!(
                "file inventory exceeds the {maximum_results}-entry boundary"
            )));
        }
        let location = format!(
            "{}/{}",
            origin.trim_end_matches('/'),
            metadata.location.as_ref()
        );
        let identity = AccountedFileIdentity::new(object_identity(location, metadata), lease)?;
        await_controlled(
            &control,
            &execution,
            cancellation.await_or_cancel(output.send(identity)),
        )
        .await?;
        emitted += 1;
    }
}

fn reserve_file_identity_envelope(
    execution: &ExecutionServices,
    control: &FileTransportControl,
) -> Result<MemoryLease> {
    let execution_for_future = execution.clone();
    let control = control.clone();
    execution.run_io(async move {
        await_controlled(
            &control,
            &execution_for_future,
            control.cancellation.await_or_cancel(reserve(
                execution_for_future.memory(),
                ReservationRequest::new(
                    ConsumerKey::new("file-identity-metadata", MemoryClass::Discovery)?,
                    FILE_IDENTITY_MEMORY_ENVELOPE_BYTES,
                )?,
            )),
        )
        .await
    })
}

fn send_blocking_file_identity(
    output: &cdf_runtime::BlockingTaskStreamSender<AccountedFileIdentity>,
    identity: AccountedFileIdentity,
    execution: &ExecutionServices,
    control: &FileTransportControl,
) -> Result<()> {
    let execution_for_future = execution.clone();
    let control = control.clone();
    let send = output.send_future(identity);
    execution.run_io(async move { await_controlled(&control, &execution_for_future, send).await })
}

async fn await_controlled<T>(
    control: &FileTransportControl,
    execution: &ExecutionServices,
    operation: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    let operation = control.cancellation.await_or_cancel(operation);
    let Some(remaining) = control.remaining(execution)? else {
        return operation.await;
    };
    let timer_cancellation = RunCancellation::default();
    let timer = execution.delay(remaining, timer_cancellation.clone());
    futures_util::pin_mut!(operation, timer);
    match futures_util::future::select(operation, timer).await {
        futures_util::future::Either::Left((result, _)) => {
            timer_cancellation.cancel();
            result
        }
        futures_util::future::Either::Right((timer_result, _)) => {
            timer_result?;
            Err(CdfError::data(
                "file transport operation exceeded its deadline",
            ))
        }
    }
}

fn same_http_origin(left: &str, right: &str) -> Result<bool> {
    let left = Url::parse(left)
        .map_err(|error| CdfError::contract(format!("invalid HTTP file URL: {error}")))?;
    let right = Url::parse(right)
        .map_err(|error| CdfError::contract(format!("invalid HTTP file URL: {error}")))?;
    Ok(left.scheme() == right.scheme()
        && left.host_str() == right.host_str()
        && left.port_or_known_default() == right.port_or_known_default())
}

fn validate_identity_text(
    label: &str,
    value: &str,
    maximum_bytes: usize,
    ascii_only: bool,
) -> Result<()> {
    if value.is_empty()
        || value.len() > maximum_bytes
        || value.chars().any(char::is_control)
        || (ascii_only && !value.is_ascii())
    {
        return Err(CdfError::data(format!(
            "{label} must be nonempty, control-free, and at most {maximum_bytes} bytes{}",
            if ascii_only { " of ASCII" } else { "" }
        )));
    }
    Ok(())
}

fn local_metadata(path: &Path) -> Result<FileIdentityMetadata> {
    let metadata = fs::metadata(path).map_err(|error| {
        CdfError::data(format!(
            "stat local file source {}: {error}",
            path.display()
        ))
    })?;
    if !metadata.is_file() {
        return Err(CdfError::data(format!(
            "local file transport path {} is not a regular file",
            path.display()
        )));
    }
    let canonical = fs::canonicalize(path).map_err(|error| {
        CdfError::data(format!(
            "canonicalize local file source {}: {error}",
            path.display()
        ))
    })?;
    Ok(FileIdentityMetadata {
        location: path_to_lossless_string(&canonical),
        size_bytes: Some(metadata.len()),
        checksum: None,
        etag: None,
        version: None,
        modified: metadata
            .modified()
            .ok()
            .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
            .map(|duration| format!("unix_ms:{}", duration.as_millis())),
        exact_ranges: true,
    })
}

pub(crate) fn object_identity(
    location: String,
    metadata: object_store::ObjectMeta,
) -> FileIdentityMetadata {
    FileIdentityMetadata {
        location,
        size_bytes: Some(metadata.size),
        checksum: None,
        etag: metadata.e_tag,
        version: metadata.version,
        modified: Some(format!(
            "unix_ms:{}",
            metadata.last_modified.timestamp_millis()
        )),
        exact_ranges: true,
    }
}

pub(crate) fn object_store_error(action: &str, error: object_store::Error) -> CdfError {
    // Provider errors may embed signed URLs, credential-bearing configuration, or response
    // bodies. Preserve the scheduler-relevant class without copying opaque provider text across
    // the transport boundary.
    let message = format!("{action}: object-store provider request failed");
    match error {
        object_store::Error::PermissionDenied { .. }
        | object_store::Error::Unauthenticated { .. } => CdfError::auth(message),
        object_store::Error::Generic { .. } | object_store::Error::JoinError { .. } => {
            CdfError::transient(message)
        }
        object_store::Error::InvalidPath { .. }
        | object_store::Error::NotSupported { .. }
        | object_store::Error::NotImplemented { .. }
        | object_store::Error::UnknownConfigurationKey { .. } => CdfError::contract(message),
        object_store::Error::NotFound { .. }
        | object_store::Error::AlreadyExists { .. }
        | object_store::Error::Precondition { .. }
        | object_store::Error::NotModified { .. } => CdfError::data(message),
        _ => CdfError::transient(message),
    }
}

pub(crate) fn verify_generation_identity(
    expected: &FileIdentityMetadata,
    observed: &FileIdentityMetadata,
    observed_size_bytes: u64,
) -> Result<()> {
    if observed.size_bytes != Some(observed_size_bytes) {
        return Err(CdfError::data(
            "observed file metadata size does not match the transferred generation size",
        ));
    }
    if expected.size_bytes != Some(observed_size_bytes) {
        return Err(CdfError::data(format!(
            "observed file generation has {observed_size_bytes} bytes but the planned generation has {} bytes",
            expected
                .size_bytes
                .map_or_else(|| "unknown".to_owned(), |size| size.to_string())
        )));
    }
    if expected.etag != observed.etag {
        return Err(CdfError::data(
            "file generation changed during the generation-bound operation (ETag mismatch)",
        ));
    }
    if expected.version != observed.version {
        return Err(CdfError::data(
            "file generation changed during the generation-bound operation (version mismatch)",
        ));
    }
    if expected.etag.is_none()
        && expected.version.is_none()
        && expected.modified != observed.modified
    {
        return Err(CdfError::data(
            "file generation changed during the generation-bound operation (modification identity mismatch)",
        ));
    }
    Ok(())
}

pub fn file_url_path(url: &str) -> Result<PathBuf> {
    let rest = url
        .strip_prefix("file://")
        .ok_or_else(|| CdfError::contract("file URL must use the file:// scheme"))?;
    if !rest.starts_with('/') {
        return Err(CdfError::contract(
            "file URL must be an absolute local file URL without an authority",
        ));
    }
    if rest.contains('%') {
        return Err(CdfError::contract(
            "percent-encoded file URLs are not implemented in this facade slice",
        ));
    }
    Ok(PathBuf::from(rest))
}

fn validate_http_file_url(url: &str) -> Result<()> {
    if url.contains(char::is_whitespace) {
        return Err(CdfError::contract(
            "HTTP(S) file URL must not contain whitespace",
        ));
    }
    if url.contains('#') {
        return Err(CdfError::contract(
            "HTTP(S) file URL must not include a fragment",
        ));
    }
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| CdfError::contract("HTTP(S) file URL must include a scheme"))?;
    if !matches!(scheme, "http" | "https") {
        return Err(CdfError::contract(
            "HTTP(S) file URL must use the http or https scheme",
        ));
    }
    let authority = rest
        .split(['/', '?', '#'])
        .next()
        .ok_or_else(|| CdfError::contract("HTTP(S) file URL must include a host"))?;
    if authority.trim().is_empty() || authority.contains(char::is_whitespace) {
        return Err(CdfError::contract("HTTP(S) file URL must include a host"));
    }
    if authority.contains('@') {
        return Err(CdfError::contract(
            "HTTP(S) file URL must not include userinfo; use secret:// auth references",
        ));
    }
    Ok(())
}

fn ensure_http_success(method: HttpMethod, response: &HttpFileResponse) -> Result<()> {
    match response.status {
        200..=399 => Ok(()),
        401 | 403 => Err(CdfError::auth(format!(
            "HTTP file transport {method} {} requires credential review",
            response.status
        ))),
        408 | 500..=599 => Err(CdfError::transient(format!(
            "HTTP file transport {method} {} from upstream",
            response.status
        ))),
        429 => Err(CdfError::rate_limited(
            format!("HTTP file transport {method} 429 rate limit"),
            http_retry_after_ms(response),
        )),
        400..=499 => Err(CdfError::data(format!(
            "HTTP file transport {method} {} response is not retryable as a request",
            response.status
        ))),
        _ => Err(CdfError::new(
            ErrorKind::Internal,
            format!("unexpected HTTP file transport status {}", response.status),
        )),
    }
}

fn http_retry_after_ms(response: &HttpFileResponse) -> Option<u64> {
    header_value(&response.headers, "retry-after")
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(|seconds| seconds.saturating_mul(1_000))
}

fn http_identity(url: &str, response: &HttpFileResponse) -> Result<FileIdentityMetadata> {
    let etag = header_value(&response.headers, "etag")
        .filter(|etag| !is_weak_http_etag(etag))
        .map(str::to_owned);
    Ok(FileIdentityMetadata {
        location: url.to_owned(),
        size_bytes: http_response_object_size(response)?,
        checksum: None,
        etag,
        version: None,
        modified: header_value(&response.headers, "last-modified").map(str::to_owned),
        exact_ranges: response.status == 206
            || header_value(&response.headers, "accept-ranges")
                .is_some_and(|value| value.eq_ignore_ascii_case("bytes")),
    })
}

fn http_response_object_size(response: &HttpFileResponse) -> Result<Option<u64>> {
    if response.status == 206 {
        let content_range = header_value(&response.headers, "content-range")
            .ok_or_else(|| CdfError::data("HTTP metadata range response omitted Content-Range"))?;
        let (start, end, total) = parse_http_content_range(content_range)?;
        if start != 0 || end != 0 {
            return Err(CdfError::data(format!(
                "HTTP metadata range response `{content_range}` does not attest requested bytes 0-0"
            )));
        }
        if optional_u64_header(&response.headers, "content-length")? != Some(1) {
            return Err(CdfError::data(
                "HTTP metadata range response must declare Content-Length: 1",
            ));
        }
        if total == 0 {
            return Err(CdfError::data(
                "HTTP metadata range response cannot attest bytes 0-0 for an empty object",
            ));
        }
        return Ok(Some(total));
    }
    optional_u64_header(&response.headers, "content-length")
}

fn parse_http_content_range(value: &str) -> Result<(u64, u64, u64)> {
    let raw = value.trim().strip_prefix("bytes ").ok_or_else(|| {
        CdfError::data(format!("HTTP Content-Range `{value}` is not a byte range"))
    })?;
    let (extent, total) = raw
        .split_once('/')
        .ok_or_else(|| CdfError::data(format!("HTTP Content-Range `{value}` omitted its total")))?;
    if total.contains('/') {
        return Err(CdfError::data(format!(
            "HTTP Content-Range `{value}` has multiple totals"
        )));
    }
    let (start, end) = extent.split_once('-').ok_or_else(|| {
        CdfError::data(format!("HTTP Content-Range `{value}` omitted its extent"))
    })?;
    let parse = |part: &str, label: &str| {
        part.parse::<u64>().map_err(|error| {
            CdfError::data(format!("HTTP Content-Range {label} is not u64: {error}"))
        })
    };
    let start = parse(start, "start")?;
    let end = parse(end, "end")?;
    let total = parse(total, "total")?;
    if start > end || end >= total {
        return Err(CdfError::data(format!(
            "HTTP Content-Range `{value}` is outside its declared total"
        )));
    }
    Ok((start, end, total))
}

fn is_weak_http_etag(etag: &str) -> bool {
    etag.trim_start()
        .get(..2)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("w/"))
}

fn optional_u64_header(headers: &HeaderMap, name: &str) -> Result<Option<u64>> {
    header_value(headers, name)
        .map(|value| {
            value.trim().parse::<u64>().map_err(|error| {
                CdfError::data(format!(
                    "HTTP file transport header `{name}` is not u64: {error}"
                ))
            })
        })
        .transpose()
}

fn policy_request(request: &HttpFileRequest) -> HttpRequest {
    let mut policy = HttpRequest::new(request.method.clone(), request.url.clone());
    policy.headers = request.headers.clone();
    policy
}

fn header_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn set_header(headers: &mut HeaderMap, name: impl Into<String>, value: impl Into<String>) {
    headers.insert(name.into().to_ascii_lowercase(), value.into());
}

fn redacted_location_for_debug(location: &str) -> String {
    if Url::parse(location)
        .ok()
        .is_some_and(|url| matches!(url.scheme(), "http" | "https"))
    {
        return "<opaque HTTP location>".to_owned();
    }
    Redactor::default().redact_url(location)
}

fn path_to_lossless_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, VecDeque},
        fs,
        sync::{Arc, Mutex},
        thread,
        time::Duration,
    };

    use cdf_http::{SecretUri, SecretValue};
    use futures_util::StreamExt;
    use object_store::{ObjectStoreExt, PutPayload, memory::InMemory};
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn http_rate_limit_preserves_retry_after_for_scheduler_policy() {
        let response = HttpFileResponse::new(429).with_header("Retry-After", "7");
        let error = ensure_http_success(HttpMethod::Get, &response).unwrap_err();

        assert_eq!(error.kind, ErrorKind::RateLimited);
        assert_eq!(error.retry_after_ms, Some(7_000));
    }

    #[test]
    fn object_store_errors_preserve_retry_auth_and_contract_taxonomy() {
        let source = || -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::other("fixture"))
        };
        let transient = object_store_error(
            "list",
            object_store::Error::Generic {
                store: "fixture",
                source: source(),
            },
        );
        let auth = object_store_error(
            "head",
            object_store::Error::Unauthenticated {
                path: "opaque".to_owned(),
                source: source(),
            },
        );
        let contract = object_store_error(
            "list",
            object_store::Error::NotImplemented {
                operation: "list".to_owned(),
                implementer: "fixture".to_owned(),
            },
        );
        assert_eq!(transient.kind, ErrorKind::Transient);
        assert_eq!(auth.kind, ErrorKind::Auth);
        assert_eq!(contract.kind, ErrorKind::Contract);
    }

    #[test]
    fn streamed_file_identity_metadata_has_a_fixed_per_entry_envelope() {
        let mut identity = FileIdentityMetadata {
            location: "s3://bucket/object".to_owned(),
            size_bytes: Some(1),
            checksum: None,
            etag: Some("\"generation\"".to_owned()),
            version: None,
            modified: None,
            exact_ranges: true,
        };
        identity.validate().unwrap();
        identity.location = "x".repeat(MAX_FILE_LOCATION_BYTES + 1);
        let error = identity.validate().unwrap_err();
        assert_eq!(error.kind, ErrorKind::Data);
        assert!(error.message.contains("at most 65536 bytes"));
    }

    #[test]
    #[ignore = "million-entry bounded listing evidence; run in the G1 slow gate"]
    fn million_entry_object_listing_uses_the_bounded_transport_stream() {
        const ENTRIES: u64 = 1_000_000;
        let objects = futures_util::stream::iter((0..ENTRIES).map(|ordinal| {
            Ok(object_store::ObjectMeta {
                location: ObjectPath::from(format!("prod/{ordinal:010}.parquet")),
                last_modified: Default::default(),
                size: 1,
                e_tag: Some(format!("\"{ordinal}\"")),
                version: None,
            })
        }))
        .boxed();
        let execution = crate::test_execution_services();
        let memory = execution.memory();
        let listing_execution = execution.clone();
        let mut listing = execution
            .spawn_io_stream(
                "million-object-list",
                FILE_LIST_CHANNEL_ENTRIES,
                move |output, cancellation| {
                    publish_object_store_listing(
                        objects,
                        "s3://bounded".to_owned(),
                        usize::MAX,
                        output,
                        cancellation,
                        FileTransportControl::default(),
                        listing_execution.clone(),
                    )
                },
            )
            .unwrap();
        let mut count = 0_u64;
        futures_executor::block_on(async {
            while let Some(identity) = listing.try_next().await.unwrap() {
                assert_eq!(
                    identity.location,
                    format!("s3://bounded/prod/{count:010}.parquet")
                );
                count += 1;
            }
        });
        assert_eq!(count, ENTRIES);
        let snapshot = memory.snapshot();
        assert_eq!(snapshot.current_bytes, 0);
        assert!(
            snapshot.peak_bytes
                <= FILE_IDENTITY_MEMORY_ENVELOPE_BYTES
                    .saturating_mul(FILE_LIST_CHANNEL_ENTRIES as u64 + 2)
        );
    }

    #[test]
    fn object_store_transport_lists_and_heads_through_one_facade() {
        let store = Arc::new(InMemory::new());
        futures_executor::block_on(store.put(
            &ObjectPath::from("prod/2026/events.parquet"),
            PutPayload::from_static(b"PAR1payloadPAR1"),
        ))
        .unwrap();
        let transport = FileTransportFacade::new()
            .with_object_store("s3://acme-events", store)
            .with_execution_services(crate::test_execution_services());
        let root = FileTransportResource::remote_url("s3://acme-events/prod/");
        let listed = transport
            .list(
                &crate::test_egress_scope(),
                &root,
                usize::MAX,
                &FileTransportControl::default(),
            )
            .and_then(|stream| futures_executor::block_on(stream.try_collect::<Vec<_>>()))
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(
            listed[0].location,
            "s3://acme-events/prod/2026/events.parquet"
        );
        assert_eq!(listed[0].size_bytes, Some(15));
        let object = FileTransportResource::remote_url(&listed[0].location);
        let head = transport
            .metadata(
                &crate::test_egress_scope(),
                &object,
                &FileTransportControl::default(),
            )
            .unwrap();
        assert_eq!(head.identity.size_bytes, Some(15));
    }

    #[test]
    fn file_transport_inventory_stops_at_the_caller_boundary() {
        let store = Arc::new(InMemory::new());
        for path in ["prod/a.parquet", "prod/b.parquet"] {
            futures_executor::block_on(
                store.put(&ObjectPath::from(path), PutPayload::from_static(b"PAR1")),
            )
            .unwrap();
        }
        let transport = FileTransportFacade::new()
            .with_object_store("s3://bounded", store)
            .with_execution_services(crate::test_execution_services());
        let root = FileTransportResource::remote_url("s3://bounded/prod/");
        let error = futures_executor::block_on(
            transport
                .list(
                    &crate::test_egress_scope(),
                    &root,
                    1,
                    &FileTransportControl::default(),
                )
                .unwrap()
                .try_collect::<Vec<_>>(),
        )
        .unwrap_err();
        assert!(error.message.contains("1-entry boundary"));
        assert_eq!(
            transport
                .list(
                    &crate::test_egress_scope(),
                    &root,
                    2,
                    &FileTransportControl::default(),
                )
                .and_then(|stream| futures_executor::block_on(stream.try_collect::<Vec<_>>()))
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn object_store_provider_urls_build_through_the_shared_parser() {
        for (location, options, expected_path) in [
            (
                "s3://cdf-conformance/data/file.parquet",
                Vec::new(),
                "data/file.parquet",
            ),
            (
                "gs://cdf-conformance/data/file.parquet",
                Vec::new(),
                "data/file.parquet",
            ),
            (
                "az://cdf-conformance/data/file.parquet",
                vec![("azure_storage_account_name", "cdf-conformance")],
                "data/file.parquet",
            ),
        ] {
            let url = Url::parse(location).unwrap();
            let (_, path) = object_store::parse_url_opts(&url, options)
                .unwrap_or_else(|error| panic!("build provider for {location}: {error}"));
            assert_eq!(path.as_ref(), expected_path);
        }
    }

    #[test]
    fn object_store_clients_are_shared_across_resource_runtime_facades() {
        let pool = ObjectStoreClientPool::default();
        let first = FileTransportFacade::new()
            .with_shared_object_store_clients(pool.clone())
            .with_execution_services(crate::test_execution_services());
        let second = FileTransportFacade::new()
            .with_shared_object_store_clients(pool.clone())
            .with_execution_services(crate::test_execution_services());
        let resource = FileTransportResource::remote_url("s3://cdf-pool-law/data/a.parquet");
        let (first_client, _, _) = first
            .resolve_object_store(
                &crate::test_egress_scope(),
                &resource,
                "s3://cdf-pool-law/data/a.parquet",
            )
            .unwrap();
        let (second_client, _, _) = second
            .resolve_object_store(
                &crate::test_egress_scope(),
                &resource,
                "s3://cdf-pool-law/data/b.parquet",
            )
            .unwrap();

        assert!(Arc::ptr_eq(&first_client, &second_client));
        assert_eq!(pool.len(), 1);
    }

    #[derive(Debug)]
    struct StaticRuntimeAwsCredentialProvider;

    impl cdf_aws::AwsCredentialProvider for StaticRuntimeAwsCredentialProvider {
        fn credentials(&self) -> cdf_kernel::BoxFuture<'_, Result<Arc<cdf_aws::AwsCredentials>>> {
            Box::pin(async {
                Ok(Arc::new(cdf_aws::AwsCredentials::new(
                    "runtime-key",
                    "runtime-secret",
                    Some("runtime-token".to_owned()),
                )?))
            })
        }
    }

    #[test]
    fn runtime_aws_authority_precedes_process_global_object_store_clients() {
        let global: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let transport = FileTransportFacade::new()
            .with_object_store("s3://governed-bucket", Arc::clone(&global))
            .with_execution_services(crate::test_execution_services());
        let binding = RuntimeAwsCredentials::new(
            "governed-binding",
            BTreeMap::from([("aws_region".to_owned(), "us-west-2".to_owned())]),
            Arc::new(StaticRuntimeAwsCredentialProvider),
        )
        .unwrap();
        let resource =
            FileTransportResource::remote_url("s3://governed-bucket/table/part-000.parquet")
                .with_runtime_aws_credentials(binding)
                .unwrap();
        let (resolved, _, _) = transport
            .resolve_object_store(
                &crate::test_egress_scope(),
                &resource,
                "s3://governed-bucket/table/part-000.parquet",
            )
            .unwrap();

        assert!(!Arc::ptr_eq(&resolved, &global));
    }

    #[test]
    fn object_store_credentials_and_egress_fail_before_network_without_leaks() {
        let credential = SecretUri::new("secret://file/cloud-options").unwrap();
        let resource = FileTransportResource::remote_url("s3://private-bucket/data.parquet")
            .with_credentials(credential)
            .with_egress_allowlist(EgressAllowlist::from_hosts(["allowed-bucket"]));
        let transport = FileTransportFacade::new();
        let error = transport
            .metadata(&crate::test_egress_scope(), &resource, &test_control())
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Auth);
        assert!(!error.message.contains("cloud-options"));
        assert!(!format!("{resource:?}").contains("cloud-options"));
    }

    #[test]
    fn file_transport_local_inventory_is_metadata_only() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("sample.bin");
        fs::write(&path, b"\x00abcdef\xff").unwrap();
        let transport = FileTransportFacade::new();

        let metadata = transport
            .metadata(
                &crate::test_egress_scope(),
                &FileTransportResource::local_path(&path),
                &test_control(),
            )
            .unwrap()
            .identity;
        assert!(metadata.location.ends_with("sample.bin"));
        assert_eq!(metadata.size_bytes, Some(8));
        assert_eq!(metadata.etag, None);
        assert!(metadata.modified.is_some());
        assert_eq!(metadata.checksum, None);
        assert_eq!(metadata.sha256(), None);
        assert_eq!(metadata.generation_strength(), GenerationStrength::Weak);

        let position = metadata.file_position_evidence().unwrap();
        assert_eq!(position.size_bytes, 8);
        assert_eq!(position.etag, None);
        assert_eq!(position.sha256, None);
        assert_eq!(position.source_generation, metadata.modified);
    }

    #[test]
    fn local_listing_uses_caller_owned_blocking_lane() {
        let temp = TempDir::new().unwrap();
        fs::write(temp.path().join("first.bin"), b"first").unwrap();
        fs::write(temp.path().join("second.bin"), b"second").unwrap();
        let resource = FileTransportResource::local_path(temp.path());
        let control = FileTransportControl::default();
        let without_lane =
            FileTransportFacade::new().with_execution_services(crate::test_execution_services());
        let error =
            match without_lane.list(&crate::test_egress_scope(), &resource, usize::MAX, &control) {
                Ok(_) => panic!("local listing must require injected blocking policy"),
                Err(error) => error,
            };
        assert!(error.message.contains("injected blocking-lane"));

        let with_lane = FileTransportFacade::new()
            .with_execution_services(crate::test_execution_services())
            .with_local_listing_lane(crate::test_local_listing_lane())
            .unwrap();
        let listed = with_lane
            .list(&crate::test_egress_scope(), &resource, usize::MAX, &control)
            .and_then(|stream| futures_executor::block_on(stream.try_collect::<Vec<_>>()))
            .unwrap();
        assert_eq!(listed.len(), 2);
    }

    #[test]
    fn file_transport_http_metadata_uses_headers_only_client() {
        let client = RecordingHttpFileTransport::new([HttpFileResponse::new(200)
            .with_header("Content-Length", "12345")
            .with_header("ETag", "\"etag-1\"")
            .with_header("Last-Modified", "Wed, 08 Jul 2026 12:00:00 GMT")]);
        let recorder = client.clone();
        let resource = FileTransportResource::http_url("https://data.example.org/events.parquet")
            .with_egress_allowlist(EgressAllowlist::from_hosts(["data.example.org"]));
        let transport = http_facade(client);

        let metadata = transport
            .metadata(&crate::test_egress_scope(), &resource, &test_control())
            .unwrap()
            .identity;
        assert_eq!(metadata.location, "https://data.example.org/events.parquet");
        assert_eq!(metadata.size_bytes, Some(12345));
        assert_eq!(metadata.etag.as_deref(), Some("\"etag-1\""));
        assert_eq!(
            metadata.modified.as_deref(),
            Some("Wed, 08 Jul 2026 12:00:00 GMT")
        );
        assert_eq!(metadata.checksum, None);
        assert_eq!(
            metadata.file_position_evidence().unwrap(),
            FilePosition {
                path: "https://data.example.org/events.parquet".to_owned(),
                size_bytes: 12345,
                source_generation: None,
                etag: Some("\"etag-1\"".to_owned()),
                object_version: None,
                sha256: None,
            }
        );

        let requests = recorder.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, HttpMethod::Head);
        assert!(!requests[0].headers.contains_key("range"));
    }

    #[test]
    fn file_transport_http_metadata_falls_back_from_head_errors_and_keeps_access_ephemeral() {
        for head_status in [400, 403] {
            let client = RecordingHttpFileTransport::new([
                HttpFileResponse::new(head_status),
                HttpFileResponse::new(302).with_header(
                    "Location",
                    "https://objects.example.org/file.parquet?token=sensitive",
                ),
                HttpFileResponse::new(206)
                    .with_header("Content-Length", "1")
                    .with_header("Content-Range", "bytes 0-0/987654")
                    .with_header("ETag", "\"generation-2\""),
            ]);
            let recorder = client.clone();
            let logical = "https://catalog.example.org/file.parquet";
            let resource = FileTransportResource::http_url(logical).with_egress_allowlist(
                EgressAllowlist::from_hosts(["catalog.example.org", "objects.example.org"]),
            );
            let transport = http_facade(client);

            let observation = transport
                .metadata(&crate::test_egress_scope(), &resource, &test_control())
                .unwrap();

            assert_eq!(observation.identity.location, logical);
            assert_eq!(observation.identity.size_bytes, Some(987654));
            assert_eq!(
                observation.identity.etag.as_deref(),
                Some("\"generation-2\"")
            );
            let access = observation.access_resource(&resource);
            assert!(matches!(
                access.location,
                FileTransportLocation::HttpUrl { ref url }
                    if url.starts_with("https://objects.example.org/file.parquet?")
            ));
            assert!(!format!("{observation:?}").contains("sensitive"));
            assert!(!format!("{observation:?}").contains("objects.example.org"));

            let requests = recorder.requests();
            assert_eq!(requests.len(), 3);
            assert_eq!(requests[0].method, HttpMethod::Head);
            assert_eq!(requests[1].method, HttpMethod::Get);
            assert_eq!(requests[2].method, HttpMethod::Get);
            assert_eq!(
                requests[1].headers.get("range").map(String::as_str),
                Some("bytes=0-0")
            );
        }
    }

    #[test]
    fn weak_http_etag_never_becomes_strong_generation_authority() {
        let client = RecordingHttpFileTransport::new([HttpFileResponse::new(200)
            .with_header("Content-Length", "12345")
            .with_header("ETag", "W/\"cache-validator\"")]);
        let resource = FileTransportResource::http_url("https://data.example.org/events.parquet")
            .with_egress_allowlist(EgressAllowlist::from_hosts(["data.example.org"]));
        let transport = http_facade(client);

        let identity = transport
            .metadata(&crate::test_egress_scope(), &resource, &test_control())
            .unwrap()
            .identity;

        assert_eq!(identity.etag, None);
        assert_eq!(identity.generation_strength(), GenerationStrength::Weak);
        assert_eq!(identity.file_position_evidence().unwrap().etag, None);
    }

    #[test]
    fn file_transport_http_redirect_rechecks_egress_before_next_hop() {
        let client = RecordingHttpFileTransport::new([HttpFileResponse::new(302)
            .with_header("Location", "https://blocked.example.org/file.parquet")]);
        let recorder = client.clone();
        let resource = FileTransportResource::http_url("https://catalog.example.org/file.parquet")
            .with_egress_allowlist(EgressAllowlist::from_hosts(["catalog.example.org"]));
        let transport = http_facade(client);

        let error = transport
            .metadata(&crate::test_egress_scope(), &resource, &test_control())
            .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Auth);
        assert!(error.message.contains("blocked.example.org"));
        assert_eq!(recorder.requests().len(), 1);
    }

    #[test]
    fn file_transport_http_optional_metadata_treats_only_404_as_absent() {
        let client = RecordingHttpFileTransport::new([
            HttpFileResponse::new(404),
            HttpFileResponse::new(403),
            HttpFileResponse::new(403),
        ]);
        let resource = FileTransportResource::http_url("https://data.example.org/missing.parquet")
            .with_egress_allowlist(EgressAllowlist::from_hosts(["data.example.org"]));
        let transport = http_facade(client);

        assert_eq!(
            transport
                .metadata_if_exists(&crate::test_egress_scope(), &resource, &test_control())
                .unwrap(),
            None
        );
        let forbidden = transport
            .metadata_if_exists(&crate::test_egress_scope(), &resource, &test_control())
            .unwrap_err();
        assert_eq!(forbidden.kind, ErrorKind::Auth);
    }

    #[test]
    fn file_transport_http_listing_is_explicitly_unsupported() {
        let client = RecordingHttpFileTransport::new([]);
        let recorder = client.clone();
        let resource = FileTransportResource::http_url("https://data.example.org/");
        let transport = http_facade(client);

        let error = match transport.list(
            &crate::test_egress_scope(),
            &resource,
            usize::MAX,
            &test_control(),
        ) {
            Ok(_) => panic!("HTTP listing must remain unsupported"),
            Err(error) => error,
        };
        assert_eq!(error.kind, ErrorKind::Contract);
        assert!(
            error
                .to_string()
                .contains("does not support arbitrary directory listing")
        );
        assert_eq!(recorder.requests().len(), 0);
    }

    #[test]
    fn file_transport_http_allowlist_and_secret_auth_are_enforced_before_client_use() {
        let blocked_client = RecordingHttpFileTransport::new([]);
        let blocked_recorder = blocked_client.clone();
        let blocked_resource =
            FileTransportResource::http_url("https://blocked.example.org/events.parquet")
                .with_egress_allowlist(EgressAllowlist::from_hosts(["data.example.org"]));
        let blocked_transport = http_facade(blocked_client);

        let error = blocked_transport
            .metadata(
                &crate::test_egress_scope(),
                &blocked_resource,
                &test_control(),
            )
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Auth);
        assert_eq!(blocked_recorder.requests().len(), 0);

        let auth_client = RecordingHttpFileTransport::new([HttpFileResponse::new(200)
            .with_header("Content-Length", "16")
            .with_header("ETag", "\"auth-generation\"")]);
        let auth_recorder = auth_client.clone();
        let auth_resource =
            FileTransportResource::http_url("https://data.example.org/events.parquet")
                .with_egress_allowlist(EgressAllowlist::from_hosts(["data.example.org"]))
                .with_auth(AuthScheme::Bearer {
                    token_uri: SecretUri::new("secret://env/FILE_TOKEN").unwrap(),
                });
        assert_eq!(
            auth_resource.secret_references()[0].as_str(),
            "secret://env/FILE_TOKEN"
        );
        let auth_transport = http_facade(auth_client).with_secret_provider(
            StaticSecretProvider::new([("secret://env/FILE_TOKEN", "secret-value")]),
        );

        let metadata = auth_transport
            .metadata(&crate::test_egress_scope(), &auth_resource, &test_control())
            .unwrap();
        assert_eq!(metadata.identity().size_bytes, Some(16));
        let requests = auth_recorder.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].headers.get("authorization").map(String::as_str),
            Some("Bearer secret-value")
        );
        assert!(!format!("{:?}", requests[0]).contains("secret-value"));
        assert!(!format!("{auth_transport:?}").contains("secret-value"));
    }

    #[test]
    fn authenticated_http_redirect_never_forwards_credentials_across_origins() {
        let client = RecordingHttpFileTransport::new([
            HttpFileResponse::new(302).with_header(
                "Location",
                "https://objects.example.org/file.parquet?sig=signed",
            ),
            HttpFileResponse::new(200)
                .with_header("Content-Length", "16")
                .with_header("ETag", "\"redirect-generation\""),
        ]);
        let recorder = client.clone();
        let resource = FileTransportResource::http_url("https://catalog.example.org/file.parquet")
            .with_egress_allowlist(EgressAllowlist::from_hosts([
                "catalog.example.org",
                "objects.example.org",
            ]))
            .with_auth(AuthScheme::Header {
                name: "x-api-key".to_owned(),
                value_uri: SecretUri::new("secret://env/FILE_KEY").unwrap(),
            });
        let transport = http_facade(client).with_secret_provider(StaticSecretProvider::new([(
            "secret://env/FILE_KEY",
            "do-not-forward",
        )]));

        let observation = transport
            .metadata(&crate::test_egress_scope(), &resource, &test_control())
            .unwrap();
        let requests = recorder.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0].headers.get("x-api-key").map(String::as_str),
            Some("do-not-forward")
        );
        assert!(!requests[1].headers.contains_key("x-api-key"));
        assert!(observation.access_resource(&resource).auth.is_none());
        assert!(!format!("{:?}", requests[0]).contains("do-not-forward"));
        assert!(!format!("{observation:?}").contains("signed"));
    }

    #[test]
    fn host_egress_denial_precedes_resource_policy_and_transport_contact() {
        let client = RecordingHttpFileTransport::new([]);
        let recorder = client.clone();
        let resource =
            FileTransportResource::http_url("https://adapter-permitted.example.org/events.parquet")
                .with_egress_allowlist(EgressAllowlist::allow_any());
        let host_scope = cdf_runtime::SourceEgressScope::new(
            cdf_runtime::SourceDriverId::new("files").unwrap(),
            Arc::new(EgressAllowlist::from_hosts(["host-permitted.example.org"])),
        );
        let transport = http_facade(client);

        let error = transport
            .metadata(&host_scope, &resource, &test_control())
            .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Auth);
        assert!(error.message.contains("adapter-permitted.example.org"));
        assert_eq!(recorder.requests().len(), 0);
    }

    #[test]
    fn file_transport_http_request_debug_redacts_sensitive_values() {
        let mut request = HttpFileRequest::new(
            HttpMethod::Get,
            "https://data.example.org/events.parquet?token=secret-value&plain=ok",
        );
        set_header(&mut request.headers, "authorization", "Bearer secret-value");
        set_header(&mut request.headers, "range", "bytes=0-3");

        let debug = format!("{request:?}");

        assert!(!debug.contains("secret-value"));
        assert!(!debug.contains("data.example.org"));
        assert!(debug.contains("<opaque HTTP URL>"));
        assert!(debug.contains("authorization"));
        assert!(debug.contains("[REDACTED]"));
        assert!(debug.contains("bytes=0-3"));
    }

    #[test]
    fn http_debug_surfaces_never_expose_signed_redirect_material() {
        let secrets = [
            ("X-Amz-Credential", "aws-credential"),
            ("X-Amz-Signature", "aws-signature"),
            ("X-Goog-Signature", "gcs-signature"),
            ("Policy", "cloudfront-policy"),
            ("Key-Pair-Id", "cloudfront-key"),
            ("sig", "azure-signature"),
        ];
        let query = secrets
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join("&");
        let location = format!("https://object.example.test/file.parquet?{query}");
        let request = HttpFileRequest::new(HttpMethod::Get, location.clone());
        let response = HttpFileResponse::new(302).with_header("Location", location.clone());
        let observation = FileMetadataObservation {
            identity: FileIdentityMetadata {
                location: "https://catalog.example.test/file.parquet".to_owned(),
                size_bytes: Some(1),
                checksum: None,
                etag: None,
                version: None,
                modified: None,
                exact_ranges: false,
            },
            access_location: OpaqueAccessLocation(FileTransportLocation::HttpUrl { url: location }),
            forward_auth: false,
        };

        for debug in [
            format!("{request:?}"),
            format!("{response:?}"),
            format!("{observation:?}"),
        ] {
            for (_, secret) in secrets {
                assert!(!debug.contains(secret), "debug leaked `{secret}`: {debug}");
            }
        }
        assert!(format!("{response:?}").contains("[REDACTED REDIRECT LOCATION]"));
    }

    #[test]
    fn file_transport_public_debug_redacts_signed_locations() {
        let resource = FileTransportResource::http_url(
            "https://data.example.org/events.parquet?token=sensitive&plain=ok",
        );
        let metadata = FileIdentityMetadata {
            location: "https://data.example.org/events.parquet?token=sensitive&plain=ok".to_owned(),
            size_bytes: Some(4),
            checksum: None,
            etag: None,
            version: None,
            modified: None,
            exact_ranges: false,
        };

        let resource_debug = format!("{resource:?}");
        let metadata_debug = format!("{metadata:?}");

        assert!(!resource_debug.contains("sensitive"));
        assert!(!metadata_debug.contains("sensitive"));
        assert!(!resource_debug.contains("data.example.org"));
        assert!(!metadata_debug.contains("data.example.org"));
        assert!(resource_debug.contains("<opaque HTTP URL>"));
        assert!(metadata_debug.contains("<opaque HTTP location>"));
    }

    #[derive(Clone)]
    struct RecordingHttpFileTransport {
        state: Arc<Mutex<RecordingHttpFileTransportState>>,
    }

    #[derive(Default)]
    struct RecordingHttpFileTransportState {
        requests: Vec<HttpFileRequest>,
        responses: VecDeque<HttpFileResponse>,
    }

    impl RecordingHttpFileTransport {
        fn new<I>(responses: I) -> Self
        where
            I: IntoIterator<Item = HttpFileResponse>,
        {
            Self {
                state: Arc::new(Mutex::new(RecordingHttpFileTransportState {
                    requests: Vec::new(),
                    responses: responses.into_iter().collect(),
                })),
            }
        }

        fn requests(&self) -> Vec<HttpFileRequest> {
            self.state.lock().unwrap().requests.clone()
        }
    }

    fn http_facade(transport: impl HttpFileTransport + 'static) -> FileTransportFacade {
        FileTransportFacade::new()
            .with_http_transport(transport)
            .with_execution_services(crate::test_execution_services())
    }

    fn test_control() -> FileTransportControl {
        FileTransportControl::default()
    }

    impl HttpFileTransport for RecordingHttpFileTransport {
        fn send_headers(
            &self,
            request: HttpFileRequest,
        ) -> BoxFuture<'static, Result<HttpFileResponse>> {
            let state = Arc::clone(&self.state);
            Box::pin(async move {
                let mut state = state.lock().unwrap();
                state.requests.push(request);
                state.responses.pop_front().ok_or_else(|| {
                    CdfError::internal("test HTTP file transport exhausted responses")
                })
            })
        }

        fn open_byte_source(
            &self,
            _resource: &FileTransportResource,
            _expected: &FileIdentityMetadata,
            _auth: Option<ResolvedHttpAuth>,
            _memory: Arc<dyn MemoryCoordinator>,
        ) -> Result<Arc<dyn ByteSource>> {
            Err(CdfError::internal(
                "control-plane HTTP test double cannot be installed as a file runtime",
            ))
        }
    }

    #[derive(Clone)]
    struct PendingHttpFileTransport;

    impl HttpFileTransport for PendingHttpFileTransport {
        fn send_headers(
            &self,
            _request: HttpFileRequest,
        ) -> BoxFuture<'static, Result<HttpFileResponse>> {
            Box::pin(futures_util::future::pending())
        }

        fn open_byte_source(
            &self,
            _resource: &FileTransportResource,
            _expected: &FileIdentityMetadata,
            _auth: Option<ResolvedHttpAuth>,
            _memory: Arc<dyn MemoryCoordinator>,
        ) -> Result<Arc<dyn ByteSource>> {
            Err(CdfError::internal(
                "pending control-plane transport has no payload source",
            ))
        }
    }

    #[test]
    fn pending_http_metadata_obeys_cancellation_and_absolute_deadline() {
        let resource = FileTransportResource::http_url("https://pending.example.test/file")
            .with_egress_allowlist(EgressAllowlist::from_hosts(["pending.example.test"]));

        let execution = crate::test_execution_services();
        let transport = FileTransportFacade::new()
            .with_http_transport(PendingHttpFileTransport)
            .with_execution_services(execution.clone());
        let cancellation = RunCancellation::default();
        let cancel = cancellation.clone();
        let cancel_thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            cancel.cancel();
        });
        let cancelled = transport
            .metadata(
                &crate::test_egress_scope(),
                &resource,
                &FileTransportControl::new(cancellation, None),
            )
            .unwrap_err();
        cancel_thread.join().unwrap();
        assert!(cancelled.message.contains("cancelled"));

        let deadline = execution
            .monotonic_now()
            .saturating_add(Duration::from_millis(10));
        let expired = transport
            .metadata(
                &crate::test_egress_scope(),
                &resource,
                &FileTransportControl::new(RunCancellation::default(), Some(deadline)),
            )
            .unwrap_err();
        assert_eq!(expired.kind, ErrorKind::Data);
        assert!(expired.message.contains("deadline"));
    }

    struct StaticSecretProvider {
        values: BTreeMap<String, String>,
    }

    impl StaticSecretProvider {
        fn new<I, K, V>(values: I) -> Self
        where
            I: IntoIterator<Item = (K, V)>,
            K: Into<String>,
            V: Into<String>,
        {
            Self {
                values: values
                    .into_iter()
                    .map(|(key, value)| (key.into(), value.into()))
                    .collect(),
            }
        }
    }

    impl SecretProvider for StaticSecretProvider {
        fn resolve(&self, uri: &SecretUri) -> Result<SecretValue> {
            self.values
                .get(uri.as_str())
                .map(|value| SecretValue::new(value.clone()))
                .ok_or_else(|| CdfError::auth(format!("missing secret {uri}")))
        }
    }
}
