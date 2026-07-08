use cdf_kernel::WriteDisposition;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceArchetype {
    File,
}

impl SourceArchetype {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MatrixDestination {
    #[serde(rename = "duckdb")]
    DuckDb,
    ParquetFilesystem,
    Postgres,
}

impl MatrixDestination {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DuckDb => "duckdb",
            Self::ParquetFilesystem => "parquet_filesystem",
            Self::Postgres => "postgres",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MatrixDisposition {
    Append,
    Replace,
    Merge,
}

impl MatrixDisposition {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Replace => "replace",
            Self::Merge => "merge",
        }
    }

    pub fn to_write_disposition(self) -> WriteDisposition {
        match self {
            Self::Append => WriteDisposition::Append,
            Self::Replace => WriteDisposition::Replace,
            Self::Merge => WriteDisposition::Merge,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RunMatrixCell {
    pub source_archetype: SourceArchetype,
    pub destination: MatrixDestination,
    pub disposition: MatrixDisposition,
}

impl RunMatrixCell {
    pub const fn file(destination: MatrixDestination, disposition: MatrixDisposition) -> Self {
        Self {
            source_archetype: SourceArchetype::File,
            destination,
            disposition,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutedMatrixCell {
    pub cell: RunMatrixCell,
    pub package_id: String,
    pub checkpoint_id: String,
    pub receipt_id: String,
    pub row_count: u64,
    pub plan_honesty_asserted: bool,
    pub package_verified: bool,
    pub destination_receipt_verified: bool,
    pub checkpoint_gated_after_receipt_verification: bool,
    pub artifact_replay_identity_asserted: bool,
    pub duplicate_behavior: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExcludedMatrixCell {
    pub cell: RunMatrixCell,
    pub reason: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunMatrixOutput {
    pub executed_cells: Vec<ExecutedMatrixCell>,
    pub excluded_cells: Vec<ExcludedMatrixCell>,
}

pub fn file_source_matrix_cells() -> Vec<RunMatrixCell> {
    let mut cells = Vec::new();
    for destination in [
        MatrixDestination::DuckDb,
        MatrixDestination::ParquetFilesystem,
        MatrixDestination::Postgres,
    ] {
        for disposition in [
            MatrixDisposition::Append,
            MatrixDisposition::Replace,
            MatrixDisposition::Merge,
        ] {
            cells.push(RunMatrixCell::file(destination, disposition));
        }
    }
    cells
}

#[cfg(test)]
mod tests;
