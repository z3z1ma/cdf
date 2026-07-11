use cdf_kernel::WriteDisposition;
use serde::{Deserialize, Serialize};

#[cfg(test)]
mod examples;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceArchetype {
    File,
    Python,
    Rest,
    Sql,
}

impl SourceArchetype {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Python => "python",
            Self::Rest => "rest",
            Self::Sql => "sql",
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
    pub const fn new(
        source_archetype: SourceArchetype,
        destination: MatrixDestination,
        disposition: MatrixDisposition,
    ) -> Self {
        Self {
            source_archetype,
            destination,
            disposition,
        }
    }

    pub const fn file(destination: MatrixDestination, disposition: MatrixDisposition) -> Self {
        Self::new(SourceArchetype::File, destination, disposition)
    }

    pub const fn rest(destination: MatrixDestination, disposition: MatrixDisposition) -> Self {
        Self::new(SourceArchetype::Rest, destination, disposition)
    }

    pub const fn python(destination: MatrixDestination, disposition: MatrixDisposition) -> Self {
        Self::new(SourceArchetype::Python, destination, disposition)
    }

    pub const fn sql(destination: MatrixDestination, disposition: MatrixDisposition) -> Self {
        Self::new(SourceArchetype::Sql, destination, disposition)
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
    source_matrix_cells(SourceArchetype::File)
}

pub fn run_spine_matrix_cells() -> Vec<RunMatrixCell> {
    [
        SourceArchetype::File,
        SourceArchetype::Python,
        SourceArchetype::Rest,
        SourceArchetype::Sql,
    ]
    .into_iter()
    .flat_map(source_matrix_cells)
    .collect()
}

pub fn source_matrix_cells(source_archetype: SourceArchetype) -> Vec<RunMatrixCell> {
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
            cells.push(RunMatrixCell::new(
                source_archetype,
                destination,
                disposition,
            ));
        }
    }
    cells
}

#[cfg(test)]
mod assertions;
#[cfg(test)]
mod core;
#[cfg(test)]
mod data_onramp;
#[cfg(test)]
mod destinations;
#[cfg(test)]
mod file_fixture;
#[cfg(test)]
pub(crate) mod local_postgres;
#[cfg(test)]
mod plan_json;
#[cfg(test)]
mod python_fixture;
#[cfg(test)]
mod rest_fixture;
#[cfg(test)]
mod sql_fixture;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;
