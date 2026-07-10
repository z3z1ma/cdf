use std::{
    collections::BTreeMap,
    fs,
    io::{self, Read, Seek, SeekFrom},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::UNIX_EPOCH,
};

use arrow_schema::SchemaRef;
use bytes::Bytes;
use cdf_kernel::{CdfError, Result};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::{
    errors::{ParquetError, Result as ParquetResult},
    file::reader::{ChunkReader, Length},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::schema::schema_hash;

#[derive(Clone, Debug)]
pub struct LocalParquetSchemaDiscovery {
    pub schema: SchemaRef,
    pub source_identity: LocalParquetSourceIdentity,
}

#[derive(Clone, Debug)]
pub struct BoundedLocalParquetSchemaDiscovery {
    pub schema: SchemaRef,
    pub source_identity: LocalParquetSourceIdentity,
    pub probe_bytes_read: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalParquetSourceIdentity {
    pub size_bytes: u64,
    pub modified_unix_millis: Option<u64>,
    pub row_count: u64,
    pub row_group_count: usize,
    pub footer_sha256: String,
}

impl LocalParquetSourceIdentity {
    pub fn cache_evidence(&self) -> BTreeMap<String, String> {
        let mut evidence = BTreeMap::from([
            ("footer_sha256".to_owned(), self.footer_sha256.clone()),
            ("row_count".to_owned(), self.row_count.to_string()),
            (
                "row_group_count".to_owned(),
                self.row_group_count.to_string(),
            ),
            ("size_bytes".to_owned(), self.size_bytes.to_string()),
        ]);
        if let Some(modified) = self.modified_unix_millis {
            evidence.insert("modified_unix_millis".to_owned(), modified.to_string());
        }
        evidence
    }
}

pub fn discover_local_parquet_schema(
    path: impl AsRef<Path>,
) -> Result<LocalParquetSchemaDiscovery> {
    let path = path.as_ref();
    let metadata = fs::metadata(path)
        .map_err(|error| io_data_error(format!("inspect {}", path.display()), error))?;
    let file = fs::File::open(path)
        .map_err(|error| io_data_error(format!("open {}", path.display()), error))?;
    let reader_metadata = ArrowReaderMetadata::load(&file, ArrowReaderOptions::new())
        .map_err(parquet_discovery_error)?;
    let parquet_metadata = reader_metadata.metadata();
    let file_metadata = parquet_metadata.file_metadata();
    let row_count = u64::try_from(file_metadata.num_rows()).map_err(|_| {
        CdfError::data("Parquet metadata discovery found a negative file row count")
    })?;
    let row_group_count = parquet_metadata.num_row_groups();
    let identity = LocalParquetSourceIdentity {
        size_bytes: metadata.len(),
        modified_unix_millis: metadata
            .modified()
            .ok()
            .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
            .and_then(|duration| u64::try_from(duration.as_millis()).ok()),
        row_count,
        row_group_count,
        footer_sha256: footer_sha256(&reader_metadata)?,
    };

    Ok(LocalParquetSchemaDiscovery {
        schema: reader_metadata.schema().clone(),
        source_identity: identity,
    })
}

pub fn discover_local_parquet_schema_bounded(
    path: impl AsRef<Path>,
    initial_bytes_read: u64,
    max_metadata_bytes: u64,
) -> Result<BoundedLocalParquetSchemaDiscovery> {
    let path = path.as_ref();
    if initial_bytes_read > max_metadata_bytes {
        return Err(metadata_budget_error(
            path,
            initial_bytes_read,
            max_metadata_bytes,
        ));
    }
    let metadata = fs::metadata(path)
        .map_err(|error| io_data_error(format!("inspect {}", path.display()), error))?;
    let modified_unix_millis = metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .and_then(|duration| u64::try_from(duration.as_millis()).ok());
    let file = fs::File::open(path)
        .map_err(|error| io_data_error(format!("open {}", path.display()), error))?;
    let file = Arc::new(Mutex::new(file));
    let bytes_read = Arc::new(AtomicU64::new(initial_bytes_read));
    let path_display = path.display().to_string();
    let range_reader = RangeChunkReader::new(metadata.len(), {
        let file = Arc::clone(&file);
        let bytes_read = Arc::clone(&bytes_read);
        move |start, length| {
            reserve_metadata_bytes(
                &bytes_read,
                u64::try_from(length).map_err(|error| CdfError::data(error.to_string()))?,
                max_metadata_bytes,
                &path_display,
            )?;
            let mut bytes = vec![0; length];
            let mut file = file
                .lock()
                .map_err(|_| CdfError::internal("Parquet discovery file mutex was poisoned"))?;
            file.seek(SeekFrom::Start(start)).map_err(|error| {
                CdfError::data(format!("seek {path_display} to {start}: {error}"))
            })?;
            file.read_exact(&mut bytes).map_err(|error| {
                CdfError::data(format!(
                    "read {} bytes from {path_display} at {start}: {error}",
                    length
                ))
            })?;
            Ok(bytes)
        }
    });
    let discovery = discover_parquet_schema_from_chunk_reader(
        &range_reader,
        metadata.len(),
        modified_unix_millis,
    )?;
    Ok(BoundedLocalParquetSchemaDiscovery {
        schema: discovery.schema,
        source_identity: discovery.source_identity,
        probe_bytes_read: bytes_read.load(Ordering::Relaxed),
    })
}

pub fn discover_parquet_schema_from_chunk_reader<T: ChunkReader>(
    reader: &T,
    size_bytes: u64,
    modified_unix_millis: Option<u64>,
) -> Result<LocalParquetSchemaDiscovery> {
    let reader_metadata = ArrowReaderMetadata::load(reader, ArrowReaderOptions::new())
        .map_err(parquet_discovery_error)?;
    let parquet_metadata = reader_metadata.metadata();
    let file_metadata = parquet_metadata.file_metadata();
    let row_count = u64::try_from(file_metadata.num_rows()).map_err(|_| {
        CdfError::data("Parquet metadata discovery found a negative file row count")
    })?;
    let row_group_count = parquet_metadata.num_row_groups();
    let identity = LocalParquetSourceIdentity {
        size_bytes,
        modified_unix_millis,
        row_count,
        row_group_count,
        footer_sha256: footer_sha256(&reader_metadata)?,
    };

    Ok(LocalParquetSchemaDiscovery {
        schema: reader_metadata.schema().clone(),
        source_identity: identity,
    })
}

pub struct RangeChunkReader {
    size_bytes: u64,
    read_range: Arc<dyn Fn(u64, usize) -> Result<Vec<u8>> + Send + Sync>,
}

impl RangeChunkReader {
    pub fn new(
        size_bytes: u64,
        read_range: impl Fn(u64, usize) -> Result<Vec<u8>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            size_bytes,
            read_range: Arc::new(read_range),
        }
    }
}

impl Length for RangeChunkReader {
    fn len(&self) -> u64 {
        self.size_bytes
    }
}

impl ChunkReader for RangeChunkReader {
    type T = RangeRead;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        if start > self.size_bytes {
            return Err(ParquetError::EOF(format!(
                "expected to read at offset {start}, while file has length {}",
                self.size_bytes
            )));
        }
        Ok(RangeRead {
            size_bytes: self.size_bytes,
            offset: start,
            read_range: Arc::clone(&self.read_range),
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        let end = start
            .checked_add(u64::try_from(length).map_err(|error| {
                ParquetError::General(format!("range length conversion failed: {error}"))
            })?)
            .ok_or_else(|| ParquetError::General("range end overflow".to_owned()))?;
        if end > self.size_bytes {
            return Err(ParquetError::EOF(format!(
                "expected to read {length} bytes at offset {start}, while file has length {}",
                self.size_bytes
            )));
        }
        let bytes = (self.read_range)(start, length).map_err(cdf_to_parquet_error)?;
        if bytes.len() != length {
            return Err(ParquetError::EOF(format!(
                "expected to read {length} bytes at offset {start}, read {}",
                bytes.len()
            )));
        }
        Ok(Bytes::from(bytes))
    }
}

pub struct RangeRead {
    size_bytes: u64,
    offset: u64,
    read_range: Arc<dyn Fn(u64, usize) -> Result<Vec<u8>> + Send + Sync>,
}

impl Read for RangeRead {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() || self.offset >= self.size_bytes {
            return Ok(0);
        }
        let remaining = self.size_bytes - self.offset;
        let length = buffer
            .len()
            .min(usize::try_from(remaining).unwrap_or(usize::MAX));
        let bytes = (self.read_range)(self.offset, length)
            .map_err(|error| io::Error::other(error.to_string()))?;
        if bytes.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "range reader returned no bytes before EOF",
            ));
        }
        let count = bytes.len().min(buffer.len());
        buffer[..count].copy_from_slice(&bytes[..count]);
        self.offset = self
            .offset
            .checked_add(u64::try_from(count).map_err(io::Error::other)?)
            .ok_or_else(|| io::Error::other("range reader offset overflow"))?;
        Ok(count)
    }
}

#[derive(Serialize)]
struct FooterFingerprint {
    arrow_schema_hash: String,
    created_by: Option<String>,
    key_value_metadata: BTreeMap<String, Option<String>>,
    parquet_schema: String,
    row_count: u64,
    row_group_count: usize,
    row_groups: Vec<RowGroupFingerprint>,
}

#[derive(Serialize)]
struct RowGroupFingerprint {
    compressed_size: i64,
    num_rows: u64,
    total_byte_size: i64,
}

fn footer_sha256(metadata: &ArrowReaderMetadata) -> Result<String> {
    let parquet_metadata = metadata.metadata();
    let file_metadata = parquet_metadata.file_metadata();
    let row_count = u64::try_from(file_metadata.num_rows()).map_err(|_| {
        CdfError::data("Parquet metadata discovery found a negative file row count")
    })?;
    let key_value_metadata = file_metadata
        .key_value_metadata()
        .into_iter()
        .flatten()
        .map(|entry| (entry.key.clone(), entry.value.clone()))
        .collect();
    let fingerprint = FooterFingerprint {
        arrow_schema_hash: schema_hash(metadata.schema().as_ref())?.as_str().to_owned(),
        created_by: file_metadata.created_by().map(ToOwned::to_owned),
        key_value_metadata,
        parquet_schema: format!("{:?}", file_metadata.schema_descr()),
        row_count,
        row_group_count: parquet_metadata.num_row_groups(),
        row_groups: parquet_metadata
            .row_groups()
            .iter()
            .map(|row_group| {
                let num_rows = u64::try_from(row_group.num_rows()).map_err(|_| {
                    CdfError::data(
                        "Parquet metadata discovery found a negative row-group row count",
                    )
                })?;
                Ok(RowGroupFingerprint {
                    compressed_size: row_group.compressed_size(),
                    num_rows,
                    total_byte_size: row_group.total_byte_size(),
                })
            })
            .collect::<Result<Vec<_>>>()?,
    };
    let mut value =
        serde_json::to_value(fingerprint).map_err(|error| CdfError::internal(error.to_string()))?;
    value.sort_all_objects();
    let bytes =
        serde_json::to_vec(&value).map_err(|error| CdfError::internal(error.to_string()))?;
    Ok(format!("sha256:{}", hex::encode(Sha256::digest(bytes))))
}

fn io_data_error(context: impl Into<String>, error: impl std::fmt::Display) -> CdfError {
    CdfError::data(format!("{}: {error}", context.into()))
}

fn parquet_discovery_error(error: impl std::fmt::Display) -> CdfError {
    CdfError::data(format!("Parquet metadata discovery failed: {error}"))
}

fn cdf_to_parquet_error(error: CdfError) -> ParquetError {
    ParquetError::General(error.to_string())
}

fn reserve_metadata_bytes(
    bytes_read: &AtomicU64,
    requested: u64,
    allowed: u64,
    path: &str,
) -> Result<()> {
    bytes_read
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |measured| {
            measured
                .checked_add(requested)
                .filter(|next| *next <= allowed)
        })
        .map(|_| ())
        .map_err(|measured| {
            CdfError::data(format!(
                "discovery metadata budget exceeded for `{path}`: measured at least {} bytes, allowed {allowed}; increase the per-file discovery metadata budget and retry",
                measured.saturating_add(requested)
            ))
        })
}

fn metadata_budget_error(path: &Path, measured: u64, allowed: u64) -> CdfError {
    CdfError::data(format!(
        "discovery metadata budget exceeded for `{}`: measured {measured} bytes, allowed {allowed}; increase the per-file discovery metadata budget and retry",
        path.display()
    ))
}
