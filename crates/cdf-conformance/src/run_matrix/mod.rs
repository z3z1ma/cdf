use cdf_kernel::{CdfError, Result, WriteDisposition};
use serde::{Deserialize, Serialize};

#[cfg(test)]
mod examples;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SourceArchetype(String);

impl SourceArchetype {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty()
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(CdfError::contract(
                "conformance source archetype must be a nonempty lowercase ASCII identifier",
            ));
        }
        Ok(Self(value))
    }

    fn fixture(value: &'static str) -> Self {
        Self::new(value).expect("static conformance source archetype is valid")
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn file() -> Self {
        Self::fixture("file")
    }

    pub fn python() -> Self {
        Self::fixture("python")
    }

    pub fn rest() -> Self {
        Self::fixture("rest")
    }

    pub fn sql() -> Self {
        Self::fixture("sql")
    }

    pub fn external_mock() -> Self {
        Self::fixture("external_mock")
    }
}

impl std::fmt::Display for SourceArchetype {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
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

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RunMatrixCell {
    pub source_archetype: SourceArchetype,
    pub destination: MatrixDestination,
    pub disposition: MatrixDisposition,
}

impl RunMatrixCell {
    pub fn new(
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

    pub fn file(destination: MatrixDestination, disposition: MatrixDisposition) -> Self {
        Self::new(SourceArchetype::file(), destination, disposition)
    }

    pub fn rest(destination: MatrixDestination, disposition: MatrixDisposition) -> Self {
        Self::new(SourceArchetype::rest(), destination, disposition)
    }

    pub fn python(destination: MatrixDestination, disposition: MatrixDisposition) -> Self {
        Self::new(SourceArchetype::python(), destination, disposition)
    }

    pub fn sql(destination: MatrixDestination, disposition: MatrixDisposition) -> Self {
        Self::new(SourceArchetype::sql(), destination, disposition)
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
    source_matrix_cells(SourceArchetype::file())
}

#[cfg(test)]
pub fn run_spine_matrix_cells() -> Vec<RunMatrixCell> {
    source_catalog::archetypes()
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
                source_archetype.clone(),
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
mod external_mock_fixture;
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
mod source_catalog;
#[cfg(test)]
mod sql_fixture;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;
