use std::{collections::BTreeMap, fs, path::Path, time::UNIX_EPOCH};

use arrow_schema::SchemaRef;
use cdf_kernel::{CdfError, Result};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::schema::schema_hash;

#[derive(Clone, Debug)]
pub struct LocalParquetSchemaDiscovery {
    pub schema: SchemaRef,
    pub source_identity: LocalParquetSourceIdentity,
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
