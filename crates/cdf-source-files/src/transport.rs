use std::{
    collections::BTreeMap,
    fmt,
    fs::{self, File},
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::Arc,
    time::UNIX_EPOCH,
};

use cdf_http::{
    AuthScheme, EgressAllowlist, HeaderMap, HttpMethod, HttpRequest, Redactor, SecretProvider,
    SecretUri,
};
use cdf_kernel::{CdfError, ErrorKind, FilePosition, Result};
use futures_util::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, path::Path as ObjectPath};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use url::Url;

#[derive(Clone, PartialEq, Eq)]
pub struct FileTransportResource {
    pub location: FileTransportLocation,
    pub egress_allowlist: EgressAllowlist,
    pub auth: Option<AuthScheme>,
    pub credentials: Option<SecretUri>,
}

impl FileTransportResource {
    pub fn local_path(path: impl AsRef<Path>) -> Self {
        Self {
            location: FileTransportLocation::LocalPath {
                path: path_to_lossless_string(path.as_ref()),
            },
            egress_allowlist: EgressAllowlist::allow_any(),
            auth: None,
            credentials: None,
        }
    }

    pub fn file_url(url: impl Into<String>) -> Self {
        Self {
            location: FileTransportLocation::FileUrl { url: url.into() },
            egress_allowlist: EgressAllowlist::allow_any(),
            auth: None,
            credentials: None,
        }
    }

    pub fn http_url(url: impl Into<String>) -> Self {
        Self {
            location: FileTransportLocation::HttpUrl { url: url.into() },
            egress_allowlist: EgressAllowlist::allow_any(),
            auth: None,
            credentials: None,
        }
    }

    pub fn object_store_url(url: impl Into<String>) -> Self {
        Self {
            location: FileTransportLocation::ObjectStoreUrl { url: url.into() },
            egress_allowlist: EgressAllowlist::allow_any(),
            auth: None,
            credentials: None,
        }
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
}

impl fmt::Debug for FileTransportResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FileTransportResource")
            .field("location", &self.location)
            .field("egress_allowlist", &self.egress_allowlist)
            .field("auth", &self.auth)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FileTransportLocation {
    LocalPath { path: String },
    FileUrl { url: String },
    HttpUrl { url: String },
    ObjectStoreUrl { url: String },
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
            Self::HttpUrl { url } => formatter
                .debug_struct("HttpUrl")
                .field("url", &redacted_location_for_debug(url))
                .finish(),
            Self::ObjectStoreUrl { url } => formatter
                .debug_struct("ObjectStoreUrl")
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
    pub modified: Option<String>,
}

impl FileIdentityMetadata {
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
            etag: self.etag.clone(),
            sha256: self.sha256().map(str::to_owned),
        })
    }

    pub fn sha256(&self) -> Option<&str> {
        self.checksum
            .as_ref()
            .filter(|checksum| checksum.algorithm == "sha256")
            .map(|checksum| checksum.value.as_str())
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
            .field("modified", &self.modified)
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileChecksum {
    pub algorithm: String,
    pub value: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ByteRange {
    pub start: u64,
    pub length: u64,
}

impl ByteRange {
    pub fn new(start: u64, length: u64) -> Result<Self> {
        let range = Self { start, length };
        range.validate()?;
        Ok(range)
    }

    fn validate(&self) -> Result<()> {
        if self.length == 0 {
            return Err(CdfError::contract(
                "file transport byte range length must be greater than zero",
            ));
        }
        self.end_inclusive()?;
        Ok(())
    }

    fn end_inclusive(&self) -> Result<u64> {
        self.start
            .checked_add(self.length - 1)
            .ok_or_else(|| CdfError::contract("file transport byte range overflows u64"))
    }
}

pub trait FileTransport: Send + Sync {
    fn metadata(&self, resource: &FileTransportResource) -> Result<FileIdentityMetadata>;
    fn metadata_if_exists(
        &self,
        resource: &FileTransportResource,
    ) -> Result<Option<FileIdentityMetadata>> {
        self.metadata(resource).map(Some)
    }
    fn read_range(&self, resource: &FileTransportResource, range: ByteRange) -> Result<Vec<u8>>;
    fn download_to_path(
        &self,
        resource: &FileTransportResource,
        expected: &FileIdentityMetadata,
        destination: &Path,
    ) -> Result<FileIdentityMetadata>;
    fn list(&self, resource: &FileTransportResource) -> Result<Vec<FileIdentityMetadata>>;
}

pub trait HttpFileTransport: Send + Sync {
    fn send(&self, request: HttpFileRequest) -> Result<HttpFileResponse>;
    fn download(
        &self,
        request: HttpFileRequest,
        destination: &Path,
    ) -> Result<(HttpFileResponse, u64)>;
}

#[derive(Clone, PartialEq, Eq)]
pub struct HttpFileRequest {
    pub method: HttpMethod,
    pub url: String,
    pub headers: HeaderMap,
}

impl HttpFileRequest {
    pub fn new(method: HttpMethod, url: impl Into<String>) -> Self {
        Self {
            method,
            url: url.into(),
            headers: HeaderMap::new(),
        }
    }
}

impl fmt::Debug for HttpFileRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let redactor = Redactor::default();
        formatter
            .debug_struct("HttpFileRequest")
            .field("method", &self.method)
            .field("url", &redactor.redact_url(&self.url))
            .field("headers", &redactor.redact_headers(&self.headers))
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct HttpFileResponse {
    pub status: u16,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
}

impl HttpFileResponse {
    pub fn new(status: u16) -> Self {
        Self {
            status,
            headers: HeaderMap::new(),
            body: Vec::new(),
        }
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        set_header(&mut self.headers, name, value);
        self
    }

    pub fn with_body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.body = body.into();
        self
    }
}

impl fmt::Debug for HttpFileResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpFileResponse")
            .field("status", &self.status)
            .field("headers", &self.headers)
            .field("body_len", &self.body.len())
            .finish()
    }
}

#[derive(Default)]
pub struct FileTransportFacade {
    http: Option<Box<dyn HttpFileTransport>>,
    secret_provider: Option<Arc<dyn SecretProvider + Send + Sync>>,
    object_stores: BTreeMap<String, Arc<dyn ObjectStore>>,
    execution: Option<cdf_runtime::ExecutionServices>,
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

    pub fn with_execution_services(mut self, execution: cdf_runtime::ExecutionServices) -> Self {
        self.execution = Some(execution);
        self
    }
}

impl fmt::Debug for FileTransportFacade {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FileTransportFacade")
            .field("http", &self.http.is_some())
            .field("secret_provider", &self.secret_provider.is_some())
            .field("object_store_count", &self.object_stores.len())
            .field("execution_services", &self.execution.is_some())
            .finish()
    }
}

impl FileTransport for FileTransportFacade {
    fn metadata(&self, resource: &FileTransportResource) -> Result<FileIdentityMetadata> {
        match &resource.location {
            FileTransportLocation::LocalPath { path } => local_metadata(Path::new(path)),
            FileTransportLocation::FileUrl { url } => local_metadata(&file_url_path(url)?),
            FileTransportLocation::HttpUrl { url } => self.http_metadata(resource, url),
            FileTransportLocation::ObjectStoreUrl { url } => {
                self.object_store_metadata(resource, url)
            }
        }
    }

    fn metadata_if_exists(
        &self,
        resource: &FileTransportResource,
    ) -> Result<Option<FileIdentityMetadata>> {
        match &resource.location {
            FileTransportLocation::HttpUrl { url } => self.http_metadata_if_exists(resource, url),
            _ => self.metadata(resource).map(Some),
        }
    }

    fn read_range(&self, resource: &FileTransportResource, range: ByteRange) -> Result<Vec<u8>> {
        range.validate()?;
        match &resource.location {
            FileTransportLocation::LocalPath { path } => read_local_range(Path::new(path), range),
            FileTransportLocation::FileUrl { url } => read_local_range(&file_url_path(url)?, range),
            FileTransportLocation::HttpUrl { url } => self.read_http_range(resource, url, range),
            FileTransportLocation::ObjectStoreUrl { url } => {
                self.read_object_store_range(resource, url, range)
            }
        }
    }

    fn download_to_path(
        &self,
        resource: &FileTransportResource,
        expected: &FileIdentityMetadata,
        destination: &Path,
    ) -> Result<FileIdentityMetadata> {
        match &resource.location {
            FileTransportLocation::LocalPath { path } => {
                copy_local_file(Path::new(path), destination)?;
                local_metadata(Path::new(path))
            }
            FileTransportLocation::FileUrl { url } => {
                let path = file_url_path(url)?;
                copy_local_file(&path, destination)?;
                local_metadata(&path)
            }
            FileTransportLocation::HttpUrl { url } => {
                self.download_http(resource, url, expected, destination)
            }
            FileTransportLocation::ObjectStoreUrl { url } => {
                self.download_object_store(resource, url, expected, destination)
            }
        }
    }

    fn list(&self, resource: &FileTransportResource) -> Result<Vec<FileIdentityMetadata>> {
        match &resource.location {
            FileTransportLocation::LocalPath { path } => list_local(Path::new(path)),
            FileTransportLocation::FileUrl { url } => list_local(&file_url_path(url)?),
            FileTransportLocation::HttpUrl { .. } => Err(CdfError::contract(
                "HTTP(S) file transport does not support arbitrary directory listing; use an explicit URL or a ratified template/range enumerator",
            )),
            FileTransportLocation::ObjectStoreUrl { url } => self.list_object_store(resource, url),
        }
    }
}

impl FileTransportFacade {
    fn download_http(
        &self,
        resource: &FileTransportResource,
        url: &str,
        expected: &FileIdentityMetadata,
        destination: &Path,
    ) -> Result<FileIdentityMetadata> {
        validate_http_file_url(url)?;
        self.reject_unimplemented_auth(resource)?;
        let mut request = HttpFileRequest::new(HttpMethod::Get, url.to_owned());
        if let Some(etag) = &expected.etag {
            set_header(&mut request.headers, "if-match", etag.clone());
        }
        resource.egress_allowlist.check(&policy_request(&request))?;
        let (response, bytes_written) = self.http_transport()?.download(request, destination)?;
        ensure_http_success(HttpMethod::Get, &response)?;
        let observed = http_identity(url, &response)?;
        verify_download_identity(expected, &observed, bytes_written)?;
        if expected.etag.is_none() {
            let reattested = self.http_metadata(resource, url)?;
            verify_download_identity(expected, &reattested, bytes_written)?;
            if expected.size_bytes != reattested.size_bytes
                || expected.modified != reattested.modified
            {
                return Err(CdfError::data(
                    "weakly versioned HTTP object changed during sequential transfer",
                ));
            }
        }
        Ok(observed)
    }

    fn download_object_store(
        &self,
        resource: &FileTransportResource,
        url: &str,
        expected: &FileIdentityMetadata,
        destination: &Path,
    ) -> Result<FileIdentityMetadata> {
        use object_store::GetOptions;

        let (store, path, _) = self.resolve_object_store(resource, url)?;
        let destination = destination.to_owned();
        let expected_etag = expected.etag.clone();
        let (metadata, bytes_written) = self.object_store_io(async move {
            let result = store
                .get_opts(
                    &path,
                    GetOptions {
                        if_match: expected_etag,
                        ..GetOptions::default()
                    },
                )
                .await
                .map_err(|error| object_store_error("open object download", error))?;
            let metadata = result.meta.clone();
            let mut stream = result.into_stream();
            let mut file = tokio::fs::File::create(destination)
                .await
                .map_err(|error| CdfError::data(format!("create object spool: {error}")))?;
            let mut bytes_written = 0_u64;
            while let Some(bytes) = stream
                .try_next()
                .await
                .map_err(|error| object_store_error("stream object download", error))?
            {
                tokio::io::AsyncWriteExt::write_all(&mut file, &bytes)
                    .await
                    .map_err(|error| CdfError::data(format!("write object spool: {error}")))?;
                bytes_written = bytes_written
                    .checked_add(bytes.len() as u64)
                    .ok_or_else(|| CdfError::data("object download byte count overflowed u64"))?;
            }
            tokio::io::AsyncWriteExt::flush(&mut file)
                .await
                .map_err(|error| CdfError::data(format!("flush object spool: {error}")))?;
            Ok((metadata, bytes_written))
        })?;
        let observed = object_identity(url.to_owned(), metadata);
        verify_download_identity(expected, &observed, bytes_written)?;
        Ok(observed)
    }

    fn http_metadata_if_exists(
        &self,
        resource: &FileTransportResource,
        url: &str,
    ) -> Result<Option<FileIdentityMetadata>> {
        validate_http_file_url(url)?;
        self.reject_unimplemented_auth(resource)?;
        let request = HttpFileRequest::new(HttpMethod::Head, url.to_owned());
        resource.egress_allowlist.check(&policy_request(&request))?;
        let response = self.http_transport()?.send(request)?;
        if response.status == 404 {
            return Ok(None);
        }
        ensure_http_success(HttpMethod::Head, &response)?;
        Ok(Some(http_identity(url, &response)?))
    }

    fn object_store_metadata(
        &self,
        resource: &FileTransportResource,
        url: &str,
    ) -> Result<FileIdentityMetadata> {
        let (store, path, _) = self.resolve_object_store(resource, url)?;
        let metadata = self
            .object_store_io(async move { Ok(store.head(&path).await) })?
            .map_err(|error| object_store_error("read object metadata", error))?;
        Ok(object_identity(url.to_owned(), metadata))
    }

    fn read_object_store_range(
        &self,
        resource: &FileTransportResource,
        url: &str,
        range: ByteRange,
    ) -> Result<Vec<u8>> {
        let (store, path, _) = self.resolve_object_store(resource, url)?;
        let end = range
            .start
            .checked_add(range.length)
            .ok_or_else(|| CdfError::contract("object-store byte range overflows u64"))?;
        let bytes = self
            .object_store_io(async move { Ok(store.get_range(&path, range.start..end).await) })?
            .map_err(|error| object_store_error("read object byte range", error))?;
        if bytes.len() as u64 != range.length {
            return Err(CdfError::data(format!(
                "object-store ranged read returned {} bytes for an exact {} byte request",
                bytes.len(),
                range.length
            )));
        }
        Ok(bytes.to_vec())
    }

    fn list_object_store(
        &self,
        resource: &FileTransportResource,
        url: &str,
    ) -> Result<Vec<FileIdentityMetadata>> {
        let (store, prefix, origin) = self.resolve_object_store(resource, url)?;
        let objects = self
            .object_store_io(
                async move { Ok(store.list(Some(&prefix)).try_collect::<Vec<_>>().await) },
            )?
            .map_err(|error| object_store_error("list object prefix", error))?;
        let mut identities = objects
            .into_iter()
            .map(|metadata| {
                let location = format!(
                    "{}/{}",
                    origin.trim_end_matches('/'),
                    metadata.location.as_ref()
                );
                object_identity(location, metadata)
            })
            .collect::<Vec<_>>();
        identities.sort_by(|left, right| left.location.cmp(&right.location));
        Ok(identities)
    }

    fn resolve_object_store(
        &self,
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
        let policy = HttpRequest::new(HttpMethod::Get, format!("https://{host}/"));
        resource.egress_allowlist.check(&policy)?;
        if let Some(store) = self.object_stores.get(&origin) {
            return Ok((
                Arc::clone(store),
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
        Ok((Arc::from(store), path, origin))
    }

    fn object_store_io<T, F>(&self, future: F) -> Result<T>
    where
        T: Send + 'static,
        F: std::future::Future<Output = Result<T>> + Send + 'static,
    {
        self.execution
            .as_ref()
            .ok_or_else(|| {
                CdfError::contract(
                    "object-store file transport requires injected ExecutionServices",
                )
            })?
            .run_io(future)
    }

    fn http_metadata(
        &self,
        resource: &FileTransportResource,
        url: &str,
    ) -> Result<FileIdentityMetadata> {
        validate_http_file_url(url)?;
        self.reject_unimplemented_auth(resource)?;
        let request = HttpFileRequest::new(HttpMethod::Head, url.to_owned());
        resource.egress_allowlist.check(&policy_request(&request))?;
        let response = self.http_transport()?.send(request)?;
        ensure_http_success(HttpMethod::Head, &response)?;
        http_identity(url, &response)
    }

    fn read_http_range(
        &self,
        resource: &FileTransportResource,
        url: &str,
        range: ByteRange,
    ) -> Result<Vec<u8>> {
        validate_http_file_url(url)?;
        self.reject_unimplemented_auth(resource)?;
        let mut request = HttpFileRequest::new(HttpMethod::Get, url.to_owned());
        set_header(
            &mut request.headers,
            "range",
            format!("bytes={}-{}", range.start, range.end_inclusive()?),
        );
        resource.egress_allowlist.check(&policy_request(&request))?;
        let response = self.http_transport()?.send(request)?;
        if response.status == 200 {
            return Err(CdfError::data(
                "HTTP file transport refused a bounded ranged read because the server ignored the Range header",
            ));
        }
        ensure_http_success(HttpMethod::Get, &response)?;
        if response.status != 206 {
            return Err(CdfError::data(format!(
                "HTTP file transport expected 206 Partial Content for ranged read, got {}",
                response.status
            )));
        }
        if response.body.len() as u64 > range.length {
            return Err(CdfError::data(format!(
                "HTTP ranged read returned {} bytes for a {} byte bound",
                response.body.len(),
                range.length
            )));
        }
        Ok(response.body)
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

    fn reject_unimplemented_auth(&self, resource: &FileTransportResource) -> Result<()> {
        if resource.auth.is_none() {
            return Ok(());
        }
        let provider_state = if self.secret_provider.is_some() {
            "configured"
        } else {
            "missing"
        };
        Err(CdfError::auth(format!(
            "HTTP(S) file transport secret auth hooks are represented, but credential resolution is not implemented in this facade slice ({provider_state} SecretProvider)"
        )))
    }
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
        checksum: Some(FileChecksum {
            algorithm: "sha256".to_owned(),
            value: file_sha256(&canonical)?,
        }),
        etag: None,
        modified: metadata
            .modified()
            .ok()
            .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
            .map(|duration| format!("unix_ms:{}", duration.as_millis())),
    })
}

fn object_identity(location: String, metadata: object_store::ObjectMeta) -> FileIdentityMetadata {
    FileIdentityMetadata {
        location,
        size_bytes: Some(metadata.size),
        checksum: None,
        etag: metadata.e_tag.or(metadata.version),
        modified: Some(format!(
            "unix_ms:{}",
            metadata.last_modified.timestamp_millis()
        )),
    }
}

fn object_store_error(action: &str, error: object_store::Error) -> CdfError {
    CdfError::data(format!("{action}: {error}"))
}

fn read_local_range(path: &Path, range: ByteRange) -> Result<Vec<u8>> {
    let mut file = File::open(path).map_err(|error| {
        CdfError::data(format!(
            "open local file source {}: {error}",
            path.display()
        ))
    })?;
    file.seek(SeekFrom::Start(range.start)).map_err(|error| {
        CdfError::data(format!(
            "seek local file source {}: {error}",
            path.display()
        ))
    })?;
    let mut buffer = Vec::new();
    file.take(range.length)
        .read_to_end(&mut buffer)
        .map_err(|error| {
            CdfError::data(format!(
                "read local file source {}: {error}",
                path.display()
            ))
        })?;
    Ok(buffer)
}

fn copy_local_file(source: &Path, destination: &Path) -> Result<()> {
    fs::copy(source, destination).map_err(|error| {
        CdfError::data(format!(
            "copy local file source {} into spool: {error}",
            source.display()
        ))
    })?;
    Ok(())
}

fn verify_download_identity(
    expected: &FileIdentityMetadata,
    observed: &FileIdentityMetadata,
    bytes_written: u64,
) -> Result<()> {
    if expected.size_bytes != Some(bytes_written) {
        return Err(CdfError::data(format!(
            "sequential file transfer wrote {bytes_written} bytes but the planned generation has {} bytes",
            expected
                .size_bytes
                .map_or_else(|| "unknown".to_owned(), |size| size.to_string())
        )));
    }
    if let (Some(expected_etag), Some(observed_etag)) = (&expected.etag, &observed.etag)
        && observed_etag != expected_etag
    {
        return Err(CdfError::data(
            "file generation changed during sequential transfer (ETag mismatch)",
        ));
    }
    if expected.etag.is_none()
        && let (Some(expected_modified), Some(observed_modified)) =
            (&expected.modified, &observed.modified)
        && observed_modified != expected_modified
    {
        return Err(CdfError::data(
            "file generation changed during sequential transfer (modification identity mismatch)",
        ));
    }
    Ok(())
}

fn list_local(path: &Path) -> Result<Vec<FileIdentityMetadata>> {
    let metadata = fs::metadata(path).map_err(|error| {
        CdfError::data(format!(
            "stat local file source {}: {error}",
            path.display()
        ))
    })?;
    if metadata.is_file() {
        return Ok(vec![local_metadata(path)?]);
    }
    if !metadata.is_dir() {
        return Err(CdfError::data(format!(
            "local file transport path {} is neither a file nor a directory",
            path.display()
        )));
    }
    let mut entries = fs::read_dir(path)
        .map_err(|error| {
            CdfError::data(format!(
                "read local file source directory {}: {error}",
                path.display()
            ))
        })?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|error| {
            CdfError::data(format!(
                "read local file source directory {}: {error}",
                path.display()
            ))
        })?;
    entries.sort();
    entries
        .into_iter()
        .filter(|entry| entry.is_file())
        .map(|entry| local_metadata(&entry))
        .collect()
}

fn file_url_path(url: &str) -> Result<PathBuf> {
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
            None,
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

fn http_identity(url: &str, response: &HttpFileResponse) -> Result<FileIdentityMetadata> {
    Ok(FileIdentityMetadata {
        location: url.to_owned(),
        size_bytes: optional_u64_header(&response.headers, "content-length")?,
        checksum: None,
        etag: header_value(&response.headers, "etag").map(str::to_owned),
        modified: header_value(&response.headers, "last-modified").map(str::to_owned),
    })
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

fn file_sha256(path: &Path) -> Result<String> {
    let mut file = File::open(path).map_err(|error| {
        CdfError::data(format!(
            "open local file source {}: {error}",
            path.display()
        ))
    })?;
    let mut hasher = Sha256::new();
    std::io::copy(&mut file, &mut hasher).map_err(|error| {
        CdfError::data(format!(
            "hash local file source {}: {error}",
            path.display()
        ))
    })?;
    Ok(hex::encode(hasher.finalize()))
}

fn redacted_location_for_debug(location: &str) -> String {
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
    };

    use cdf_http::{SecretUri, SecretValue};
    use object_store::{ObjectStoreExt, PutPayload, memory::InMemory};
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn object_store_transport_lists_heads_and_ranges_through_one_facade() {
        let store = Arc::new(InMemory::new());
        futures_executor::block_on(store.put(
            &ObjectPath::from("prod/2026/events.parquet"),
            PutPayload::from_static(b"PAR1payloadPAR1"),
        ))
        .unwrap();
        let transport = FileTransportFacade::new()
            .with_object_store("s3://acme-events", store)
            .with_execution_services(crate::test_execution_services());
        let root = FileTransportResource::object_store_url("s3://acme-events/prod/");
        let listed = transport.list(&root).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(
            listed[0].location,
            "s3://acme-events/prod/2026/events.parquet"
        );
        assert_eq!(listed[0].size_bytes, Some(15));
        let object = FileTransportResource::object_store_url(&listed[0].location);
        let head = transport.metadata(&object).unwrap();
        assert_eq!(head.size_bytes, Some(15));
        assert_eq!(
            transport
                .read_range(&object, ByteRange::new(4, 7).unwrap())
                .unwrap(),
            b"payload"
        );
    }

    #[test]
    fn object_store_credentials_and_egress_fail_before_network_without_leaks() {
        let credential = SecretUri::new("secret://file/cloud-options").unwrap();
        let resource = FileTransportResource::object_store_url("s3://private-bucket/data.parquet")
            .with_credentials(credential)
            .with_egress_allowlist(EgressAllowlist::from_hosts(["allowed-bucket"]));
        let transport = FileTransportFacade::new();
        let error = transport.metadata(&resource).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Auth);
        assert!(!error.message.contains("cloud-options"));
        assert!(!format!("{resource:?}").contains("cloud-options"));
    }

    #[test]
    fn file_transport_local_metadata_and_range_share_identity_model() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("sample.bin");
        fs::write(&path, b"\x00abcdef\xff").unwrap();
        let transport = FileTransportFacade::new();

        let metadata = transport
            .metadata(&FileTransportResource::local_path(&path))
            .unwrap();
        assert!(metadata.location.ends_with("sample.bin"));
        assert_eq!(metadata.size_bytes, Some(8));
        assert_eq!(metadata.etag, None);
        assert!(metadata.modified.is_some());
        assert_eq!(metadata.checksum.as_ref().unwrap().algorithm, "sha256");
        assert_eq!(
            metadata.sha256(),
            Some("c6e5e5fc9d44950227b9ccef6374a99443228ca0b80d9a7c416d8d4d61c92379")
        );

        let position = metadata.file_position_evidence().unwrap();
        assert_eq!(position.size_bytes, 8);
        assert_eq!(position.etag, None);
        assert_eq!(position.sha256.as_deref(), metadata.sha256());

        let bytes = transport
            .read_range(
                &FileTransportResource::local_path(&path),
                ByteRange::new(2, 4).unwrap(),
            )
            .unwrap();
        assert_eq!(bytes, b"bcde");
    }

    #[test]
    fn file_transport_http_metadata_and_bounded_range_use_http_client() {
        let client = RecordingHttpFileTransport::new([
            HttpFileResponse::new(200)
                .with_header("Content-Length", "12345")
                .with_header("ETag", "\"etag-1\"")
                .with_header("Last-Modified", "Wed, 08 Jul 2026 12:00:00 GMT"),
            HttpFileResponse::new(206)
                .with_header("Content-Range", "bytes 100-103/12345")
                .with_body(vec![0, 159, 146, 150]),
        ]);
        let recorder = client.clone();
        let resource = FileTransportResource::http_url("https://data.example.org/events.parquet")
            .with_egress_allowlist(EgressAllowlist::from_hosts(["data.example.org"]));
        let transport = FileTransportFacade::new().with_http_transport(client);

        let metadata = transport.metadata(&resource).unwrap();
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
                etag: Some("\"etag-1\"".to_owned()),
                sha256: None,
            }
        );

        let bytes = transport
            .read_range(&resource, ByteRange::new(100, 4).unwrap())
            .unwrap();
        assert_eq!(bytes, vec![0, 159, 146, 150]);

        let requests = recorder.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].method, HttpMethod::Head);
        assert!(!requests[0].headers.contains_key("range"));
        assert_eq!(requests[1].method, HttpMethod::Get);
        assert_eq!(
            requests[1].headers.get("range").map(String::as_str),
            Some("bytes=100-103")
        );
    }

    #[test]
    fn file_transport_http_optional_metadata_treats_only_404_as_absent() {
        let client = RecordingHttpFileTransport::new([
            HttpFileResponse::new(404),
            HttpFileResponse::new(403),
        ]);
        let resource = FileTransportResource::http_url("https://data.example.org/missing.parquet")
            .with_egress_allowlist(EgressAllowlist::from_hosts(["data.example.org"]));
        let transport = FileTransportFacade::new().with_http_transport(client);

        assert_eq!(transport.metadata_if_exists(&resource).unwrap(), None);
        let forbidden = transport.metadata_if_exists(&resource).unwrap_err();
        assert_eq!(forbidden.kind, ErrorKind::Auth);
    }

    #[test]
    fn file_transport_http_range_rejects_unbounded_or_ignored_range() {
        let client =
            RecordingHttpFileTransport::new([HttpFileResponse::new(200)
                .with_body(b"this would be a full file download".to_vec())]);
        let resource = FileTransportResource::http_url("https://data.example.org/events.parquet");
        let transport = FileTransportFacade::new().with_http_transport(client);

        let zero = transport
            .read_range(
                &resource,
                ByteRange {
                    start: 0,
                    length: 0,
                },
            )
            .unwrap_err();
        assert_eq!(zero.kind, ErrorKind::Contract);

        let ignored = transport
            .read_range(&resource, ByteRange::new(0, 4).unwrap())
            .unwrap_err();
        assert_eq!(ignored.kind, ErrorKind::Data);
        assert!(ignored.to_string().contains("ignored the Range header"));
    }

    #[test]
    fn file_transport_http_listing_is_explicitly_unsupported() {
        let client = RecordingHttpFileTransport::new([]);
        let recorder = client.clone();
        let resource = FileTransportResource::http_url("https://data.example.org/");
        let transport = FileTransportFacade::new().with_http_transport(client);

        let error = transport.list(&resource).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Contract);
        assert!(
            error
                .to_string()
                .contains("does not support arbitrary directory listing")
        );
        assert_eq!(recorder.requests().len(), 0);
    }

    #[test]
    fn file_transport_http_allowlist_and_auth_hooks_fail_before_client_use() {
        let blocked_client = RecordingHttpFileTransport::new([]);
        let blocked_recorder = blocked_client.clone();
        let blocked_resource =
            FileTransportResource::http_url("https://blocked.example.org/events.parquet")
                .with_egress_allowlist(EgressAllowlist::from_hosts(["data.example.org"]));
        let blocked_transport = FileTransportFacade::new().with_http_transport(blocked_client);

        let error = blocked_transport
            .read_range(&blocked_resource, ByteRange::new(0, 1).unwrap())
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Auth);
        assert_eq!(blocked_recorder.requests().len(), 0);

        let auth_client = RecordingHttpFileTransport::new([]);
        let auth_recorder = auth_client.clone();
        let auth_resource = FileTransportResource::http_url(
            "https://data.example.org/events.parquet",
        )
        .with_auth(AuthScheme::Bearer {
            token_uri: SecretUri::new("secret://env/FILE_TOKEN").unwrap(),
        });
        assert_eq!(
            auth_resource.secret_references()[0].as_str(),
            "secret://env/FILE_TOKEN"
        );
        let auth_transport = FileTransportFacade::new()
            .with_http_transport(auth_client)
            .with_secret_provider(StaticSecretProvider::new([(
                "secret://env/FILE_TOKEN",
                "secret-value",
            )]));

        let error = auth_transport
            .read_range(&auth_resource, ByteRange::new(0, 1).unwrap())
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Auth);
        assert!(
            error
                .to_string()
                .contains("credential resolution is not implemented")
        );
        assert_eq!(auth_recorder.requests().len(), 0);
        assert!(!format!("{auth_transport:?}").contains("secret-value"));
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
        assert!(debug.contains("token=[REDACTED]"));
        assert!(debug.contains("authorization"));
        assert!(debug.contains("[REDACTED]"));
        assert!(debug.contains("bytes=0-3"));
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
            modified: None,
        };

        let resource_debug = format!("{resource:?}");
        let metadata_debug = format!("{metadata:?}");

        assert!(!resource_debug.contains("sensitive"));
        assert!(!metadata_debug.contains("sensitive"));
        assert!(resource_debug.contains("token=[REDACTED]"));
        assert!(metadata_debug.contains("token=[REDACTED]"));
        assert!(resource_debug.contains("plain=ok"));
        assert!(metadata_debug.contains("plain=ok"));
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

    impl HttpFileTransport for RecordingHttpFileTransport {
        fn send(&self, request: HttpFileRequest) -> Result<HttpFileResponse> {
            let mut state = self.state.lock().unwrap();
            state.requests.push(request);
            state
                .responses
                .pop_front()
                .ok_or_else(|| CdfError::internal("test HTTP file transport exhausted responses"))
        }

        fn download(
            &self,
            request: HttpFileRequest,
            destination: &Path,
        ) -> Result<(HttpFileResponse, u64)> {
            let response = self.send(request)?;
            fs::write(destination, &response.body)
                .map_err(|error| CdfError::data(format!("write test download: {error}")))?;
            let bytes_written = response.body.len() as u64;
            Ok((response, bytes_written))
        }
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
