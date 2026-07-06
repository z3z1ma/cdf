use std::path::Path;

use firn_kernel::{FirnError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    model::SegmentEntry, parquet::transcode_record_batches_to_parquet_bytes, reader::PackageReader,
};

pub const ARCHIVE_FIDELITY_STATEMENT: &str = "Arrow IPC remains the canonical package data. Parquet bytes are an archive/interchange projection; Arrow field metadata and other Arrow-only semantics are not promoted to canonical Parquet truth.";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageArchiveReport {
    pub package_hash: String,
    pub fidelity_statement: String,
    pub segments: Vec<ArchivedSegmentReport>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchivedSegmentReport {
    pub segment_id: String,
    pub source_path: String,
    pub source_byte_count: u64,
    pub source_sha256: String,
    pub source_row_count: u64,
    pub parquet_bytes: Vec<u8>,
    pub parquet_byte_count: u64,
    pub parquet_sha256: String,
    pub parquet_row_count: u64,
}

pub fn archive_package_to_parquet(package_dir: impl AsRef<Path>) -> Result<PackageArchiveReport> {
    let reader = PackageReader::open(package_dir)?;
    reader.verify()?;

    let mut segments = Vec::new();
    for (entry, batches) in reader.read_all_segments()? {
        if batches.is_empty() {
            return Err(FirnError::data(format!(
                "package segment {} contains no batches",
                entry.segment_id.as_str()
            )));
        }

        let mut row_count = 0_u64;
        for batch in &batches {
            row_count += batch.num_rows() as u64;
        }
        if row_count != entry.row_count {
            return Err(FirnError::data(format!(
                "segment {} manifest row count {} differs from package data {}",
                entry.segment_id.as_str(),
                entry.row_count,
                row_count
            )));
        }

        let parquet_bytes = transcode_record_batches_to_parquet_bytes(&batches)?;
        segments.push(archive_segment_report(&entry, parquet_bytes, row_count));
    }

    Ok(PackageArchiveReport {
        package_hash: reader.manifest().package_hash.clone(),
        fidelity_statement: ARCHIVE_FIDELITY_STATEMENT.to_owned(),
        segments,
    })
}

fn archive_segment_report(
    entry: &SegmentEntry,
    parquet_bytes: Vec<u8>,
    row_count: u64,
) -> ArchivedSegmentReport {
    ArchivedSegmentReport {
        segment_id: entry.segment_id.as_str().to_owned(),
        source_path: entry.path.clone(),
        source_byte_count: entry.byte_count,
        source_sha256: entry.sha256.clone(),
        source_row_count: entry.row_count,
        parquet_byte_count: parquet_bytes.len() as u64,
        parquet_sha256: sha256_hex(&parquet_bytes),
        parquet_row_count: row_count,
        parquet_bytes,
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}
