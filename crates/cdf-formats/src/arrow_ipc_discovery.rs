use std::{
    collections::BTreeMap,
    fs,
    io::{Read, Seek},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::UNIX_EPOCH,
};

use arrow_ipc::reader::FileReader;
use arrow_schema::SchemaRef;
use cdf_kernel::{CdfError, Result};
use serde::{Deserialize, Serialize};

use crate::schema::schema_hash;

#[derive(Clone, Debug)]
pub struct LocalArrowIpcSchemaDiscovery {
    pub schema: SchemaRef,
    pub source_identity: LocalArrowIpcSourceIdentity,
    pub probe_bytes_read: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalArrowIpcSourceIdentity {
    pub size_bytes: u64,
    pub modified_unix_millis: Option<u64>,
    pub schema_hash: String,
}

impl LocalArrowIpcSourceIdentity {
    pub fn cache_evidence(&self) -> BTreeMap<String, String> {
        let mut evidence = BTreeMap::from([
            ("schema_hash".to_owned(), self.schema_hash.clone()),
            ("size_bytes".to_owned(), self.size_bytes.to_string()),
        ]);
        if let Some(modified) = self.modified_unix_millis {
            evidence.insert("modified_unix_millis".to_owned(), modified.to_string());
        }
        evidence
    }
}

pub fn discover_arrow_ipc_file_schema<R: Read + Seek>(reader: R) -> Result<SchemaRef> {
    let reader = FileReader::try_new(reader, None).map_err(arrow_ipc_discovery_error)?;
    Ok(reader.schema())
}

pub fn discover_local_arrow_ipc_schema(
    path: impl AsRef<Path>,
) -> Result<LocalArrowIpcSchemaDiscovery> {
    discover_local_arrow_ipc_schema_bounded(path, 0, u64::MAX)
}

pub fn discover_local_arrow_ipc_schema_bounded(
    path: impl AsRef<Path>,
    initial_bytes_read: u64,
    max_metadata_bytes: u64,
) -> Result<LocalArrowIpcSchemaDiscovery> {
    let path = path.as_ref();
    if initial_bytes_read > max_metadata_bytes {
        return Err(CdfError::data(format!(
            "discovery metadata budget exceeded for `{}`: measured {initial_bytes_read} bytes, allowed {max_metadata_bytes}; increase the per-file discovery metadata budget and retry",
            path.display()
        )));
    }
    let metadata = fs::metadata(path).map_err(|error| {
        CdfError::data(format!(
            "inspect Arrow IPC file {} for schema discovery: {error}",
            path.display()
        ))
    })?;
    let file = fs::File::open(path).map_err(|error| {
        CdfError::data(format!(
            "open Arrow IPC file {} for schema discovery: {error}",
            path.display()
        ))
    })?;
    let bytes_read = Arc::new(AtomicU64::new(initial_bytes_read));
    let schema = discover_arrow_ipc_file_schema(CountingReader {
        inner: file,
        bytes_read: Arc::clone(&bytes_read),
        max_bytes: max_metadata_bytes,
        path: path.display().to_string(),
    })?;
    let identity = LocalArrowIpcSourceIdentity {
        size_bytes: metadata.len(),
        modified_unix_millis: metadata
            .modified()
            .ok()
            .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
            .and_then(|duration| u64::try_from(duration.as_millis()).ok()),
        schema_hash: schema_hash(schema.as_ref())?.as_str().to_owned(),
    };
    Ok(LocalArrowIpcSchemaDiscovery {
        schema,
        source_identity: identity,
        probe_bytes_read: bytes_read.load(Ordering::Relaxed),
    })
}

struct CountingReader<R> {
    inner: R,
    bytes_read: Arc<AtomicU64>,
    max_bytes: u64,
    path: String,
}

impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let measured = self.bytes_read.load(Ordering::Relaxed);
        let remaining = self.max_bytes.saturating_sub(measured);
        if remaining == 0 && !buffer.is_empty() {
            return Err(std::io::Error::other(format!(
                "discovery metadata budget exceeded for `{}`: measured at least {} bytes, allowed {}; increase the per-file discovery metadata budget and retry",
                self.path,
                measured.saturating_add(1),
                self.max_bytes
            )));
        }
        let allowed = buffer
            .len()
            .min(usize::try_from(remaining).unwrap_or(usize::MAX));
        let count = self.inner.read(&mut buffer[..allowed])?;
        self.bytes_read.fetch_add(count as u64, Ordering::Relaxed);
        Ok(count)
    }
}

impl<R: Seek> Seek for CountingReader<R> {
    fn seek(&mut self, position: std::io::SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(position)
    }
}

fn arrow_ipc_discovery_error(error: impl std::fmt::Display) -> CdfError {
    CdfError::data(format!(
        "Arrow IPC file schema discovery failed; expected Arrow IPC file framing (stream framing is unsupported): {error}"
    ))
}
