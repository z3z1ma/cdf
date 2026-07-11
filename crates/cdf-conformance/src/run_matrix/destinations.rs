use std::{
    fs,
    path::{Path, PathBuf},
};

use cdf_dest_duckdb::{DuckDbDestination, DuckDbMirrorSnapshot};
use cdf_dest_parquet::ParquetDestination;
use cdf_dest_postgres::{MergeDedupPolicy, PostgresDestination, PostgresTarget};
use cdf_kernel::{CdfError, DestinationProtocol, Receipt, Result, TargetName};
use cdf_project::ResolvedProjectDestination;
use postgres::{Client, NoTls};

use super::{
    MatrixDestination, RunMatrixCell,
    local_postgres::{LivePostgres, qualified_name, reset_postgres_schema},
};

#[derive(Clone, Debug)]
pub(crate) enum MatrixTarget {
    Plain(TargetName),
    Postgres {
        target_name: TargetName,
        target: PostgresTarget,
        schema: String,
        table: String,
    },
}

#[derive(Clone, Debug)]
pub(crate) enum MatrixDestinationHandle {
    DuckDb {
        database_path: PathBuf,
        target: TargetName,
    },
    Parquet {
        root: PathBuf,
        target: TargetName,
    },
    Postgres {
        database_url: String,
        target: PostgresTarget,
        target_name: TargetName,
        schema: String,
        table: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DestinationFootprint {
    DuckDb(DuckDbMirrorSnapshot),
    Parquet {
        files: Vec<FileFootprint>,
    },
    Postgres {
        target_rows: i64,
        loads_rows: i64,
        state_rows: i64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct FileFootprint {
    path: String,
    size_bytes: u64,
}

impl MatrixDestinationHandle {
    pub(crate) fn new(
        destination: MatrixDestination,
        root: &Path,
        target: MatrixTarget,
        postgres: &LivePostgres,
    ) -> Result<Self> {
        match (destination, target) {
            (MatrixDestination::DuckDb, MatrixTarget::Plain(target)) => Ok(Self::DuckDb {
                database_path: root.join(".cdf/run-matrix.duckdb"),
                target,
            }),
            (MatrixDestination::ParquetFilesystem, MatrixTarget::Plain(target)) => {
                Ok(Self::Parquet {
                    root: root.join(".cdf/lake"),
                    target,
                })
            }
            (
                MatrixDestination::Postgres,
                MatrixTarget::Postgres {
                    target_name,
                    target,
                    schema,
                    table,
                },
            ) => Ok(Self::Postgres {
                database_url: postgres.url().to_owned(),
                target,
                target_name,
                schema,
                table,
            }),
            _ => Err(CdfError::contract(
                "run matrix destination and target kind do not match",
            )),
        }
    }

    pub(crate) fn resolved(&self) -> Result<ResolvedProjectDestination> {
        match self {
            Self::DuckDb {
                database_path,
                target,
            } => ResolvedProjectDestination::duckdb(database_path, target.clone()),
            Self::Parquet { root, target } => {
                ResolvedProjectDestination::parquet_filesystem(root, target.clone())
            }
            Self::Postgres {
                database_url,
                target,
                ..
            } => ResolvedProjectDestination::postgres(
                database_url.clone(),
                target.clone(),
                MergeDedupPolicy::Last,
                None,
            ),
        }
    }

    pub(crate) fn fresh_artifact_replay_destination(&self, root: &Path) -> Result<Self> {
        match self {
            Self::DuckDb { target, .. } => Ok(Self::DuckDb {
                database_path: root.join(".cdf/replay.duckdb"),
                target: target.clone(),
            }),
            Self::Parquet { target, .. } => Ok(Self::Parquet {
                root: root.join(".cdf/replay-lake"),
                target: target.clone(),
            }),
            Self::Postgres {
                database_url,
                target,
                target_name,
                schema,
                table,
            } => {
                reset_postgres_schema(database_url, schema)?;
                Ok(Self::Postgres {
                    database_url: database_url.clone(),
                    target: target.clone(),
                    target_name: target_name.clone(),
                    schema: schema.clone(),
                    table: table.clone(),
                })
            }
        }
    }

    pub(crate) fn verify_trait_receipt(&self, receipt: &Receipt) -> Result<()> {
        let verification = match self {
            Self::DuckDb { database_path, .. } => {
                let destination = DuckDbDestination::new(database_path)?;
                DestinationProtocol::verify(&destination, receipt)?
            }
            Self::Parquet { root, .. } => {
                let destination =
                    ParquetDestination::new_filesystem(root, crate::test_execution_services())?;
                DestinationProtocol::verify(&destination, receipt)?
            }
            Self::Postgres { database_url, .. } => {
                let destination = PostgresDestination::connect(database_url.clone())?;
                DestinationProtocol::verify(&destination, receipt)?
            }
        };
        if !verification.verified {
            return Err(CdfError::destination(format!(
                "run matrix receipt {} did not verify through DestinationProtocol::verify: {}",
                verification.receipt_id,
                verification
                    .reason
                    .unwrap_or_else(|| "verification returned false".to_owned())
            )));
        }
        Ok(())
    }

    pub(crate) fn footprint(&self) -> Result<DestinationFootprint> {
        match self {
            Self::DuckDb { database_path, .. } => Ok(DestinationFootprint::DuckDb(
                DuckDbDestination::new(database_path)?.read_mirror_snapshot_read_only()?,
            )),
            Self::Parquet { root, .. } => Ok(DestinationFootprint::Parquet {
                files: list_relative_files(root)?,
            }),
            Self::Postgres {
                database_url,
                schema,
                table,
                ..
            } => postgres_footprint(database_url, schema, table),
        }
    }
}

pub(crate) fn target_for_cell(
    cell: RunMatrixCell,
    postgres: &LivePostgres,
) -> Result<MatrixTarget> {
    let table = target_table_for_cell(cell);
    match cell.destination {
        MatrixDestination::DuckDb | MatrixDestination::ParquetFilesystem => {
            Ok(MatrixTarget::Plain(TargetName::new(table)?))
        }
        MatrixDestination::Postgres => {
            let target = PostgresTarget::new(Some(postgres.schema()), &table)?;
            Ok(MatrixTarget::Postgres {
                target_name: TargetName::new(target.display_name())?,
                target,
                schema: postgres.schema().to_owned(),
                table,
            })
        }
    }
}

fn target_table_for_cell(cell: RunMatrixCell) -> String {
    let prefix = match cell.source_archetype {
        super::SourceArchetype::File => "events",
        super::SourceArchetype::Python => "python_events",
        super::SourceArchetype::Rest => "rest_events",
        super::SourceArchetype::Sql => "sql_events",
    };
    format!("{prefix}_{}", cell.disposition.as_str())
}

fn list_relative_files(root: &Path) -> Result<Vec<FileFootprint>> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut files = Vec::new();
    collect_relative_files(root, root, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_relative_files(
    root: &Path,
    current: &Path,
    files: &mut Vec<FileFootprint>,
) -> Result<()> {
    for entry in fs::read_dir(current)
        .map_err(|error| CdfError::data(format!("read {}: {error}", current.display())))?
    {
        let entry = entry.map_err(|error| {
            CdfError::data(format!("read entry in {}: {error}", current.display()))
        })?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|error| {
            CdfError::data(format!("read file type for {}: {error}", path.display()))
        })?;
        if file_type.is_dir() {
            collect_relative_files(root, &path, files)?;
        } else if file_type.is_file() {
            let relative = path.strip_prefix(root).map_err(|error| {
                CdfError::data(format!("relativize {}: {error}", path.display()))
            })?;
            let metadata = fs::metadata(&path).map_err(|error| {
                CdfError::data(format!("read metadata for {}: {error}", path.display()))
            })?;
            files.push(FileFootprint {
                path: relative.display().to_string(),
                size_bytes: metadata.len(),
            });
        }
    }
    Ok(())
}

fn postgres_footprint(
    database_url: &str,
    schema: &str,
    table: &str,
) -> Result<DestinationFootprint> {
    let mut client = Client::connect(database_url, NoTls)
        .map_err(|error| CdfError::destination(format!("connect to Postgres: {error}")))?;
    let target_rows = query_count(&mut client, &qualified_name(schema, table))?;
    let loads_rows = query_count(&mut client, &qualified_name(schema, "_cdf_loads"))?;
    let state_rows = query_count(&mut client, &qualified_name(schema, "_cdf_state"))?;
    Ok(DestinationFootprint::Postgres {
        target_rows,
        loads_rows,
        state_rows,
    })
}

fn query_count(client: &mut Client, table: &str) -> Result<i64> {
    client
        .query_one(&format!("SELECT COUNT(*)::bigint FROM {table}"), &[])
        .map(|row| row.get(0))
        .map_err(|error| CdfError::destination(format!("query row count from {table}: {error}")))
}
