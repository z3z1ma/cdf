use std::{
    fs,
    path::{Path, PathBuf},
};

use cdf_dest_duckdb::{DuckDbDestination, DuckDbMirrorSnapshot};
use cdf_dest_parquet::ParquetDestination;
use cdf_dest_postgres::{MergeDedupPolicy, PostgresDestination, PostgresTarget};
use cdf_kernel::{CdfError, DestinationProtocol, Receipt, Result, TargetName};
use cdf_project::{ProjectReceiptSource, ResolvedProjectDestination};
use postgres::{Client, NoTls};

use crate::run_matrix::local_postgres::{LivePostgres, qualified_name};

use super::{ChaosCrashWindow, ChaosDestination};

#[derive(Clone, Debug)]
pub(crate) enum ChaosDestinationHandle {
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
        schema: String,
        table: String,
        target: PostgresTarget,
        target_name: TargetName,
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

impl ChaosDestinationHandle {
    pub(crate) fn new(
        destination: ChaosDestination,
        window: ChaosCrashWindow,
        root: &Path,
        postgres: &LivePostgres,
    ) -> Result<Self> {
        let table = target_table(destination, window);
        match destination {
            ChaosDestination::DuckDb => Ok(Self::DuckDb {
                database_path: root.join(".cdf/runtime-chaos.duckdb"),
                target: TargetName::new(table)?,
            }),
            ChaosDestination::ParquetFilesystem => Ok(Self::Parquet {
                root: root.join(".cdf/runtime-chaos-lake"),
                target: TargetName::new(table)?,
            }),
            ChaosDestination::Postgres => {
                let target = PostgresTarget::new(Some(postgres.schema()), &table)?;
                Ok(Self::Postgres {
                    database_url: postgres.url().to_owned(),
                    schema: postgres.schema().to_owned(),
                    table,
                    target_name: TargetName::new(target.display_name())?,
                    target,
                })
            }
        }
    }

    pub(crate) fn duckdb(database_path: PathBuf, target: TargetName) -> Self {
        Self::DuckDb {
            database_path,
            target,
        }
    }

    pub(crate) fn parquet(root: PathBuf, target: TargetName) -> Self {
        Self::Parquet { root, target }
    }

    pub(crate) fn postgres(database_url: String, schema: String, table: String) -> Result<Self> {
        let target = PostgresTarget::new(Some(&schema), &table)?;
        Ok(Self::Postgres {
            database_url,
            schema,
            table,
            target_name: TargetName::new(target.display_name())?,
            target,
        })
    }

    pub(crate) fn kind(&self) -> ChaosDestination {
        match self {
            Self::DuckDb { .. } => ChaosDestination::DuckDb,
            Self::Parquet { .. } => ChaosDestination::ParquetFilesystem,
            Self::Postgres { .. } => ChaosDestination::Postgres,
        }
    }

    pub(crate) fn target_name(&self) -> TargetName {
        match self {
            Self::DuckDb { target, .. } | Self::Parquet { target, .. } => target.clone(),
            Self::Postgres { target_name, .. } => target_name.clone(),
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

    pub(crate) fn footprint(&self) -> Result<DestinationFootprint> {
        match self {
            Self::DuckDb { database_path, .. } => {
                if !database_path.exists() {
                    return Ok(DestinationFootprint::DuckDb(DuckDbMirrorSnapshot::default()));
                }
                Ok(DestinationFootprint::DuckDb(
                    DuckDbDestination::new(database_path)?.read_mirror_snapshot_read_only()?,
                ))
            }
            Self::Parquet { root, .. } => Ok(DestinationFootprint::Parquet {
                files: list_relative_files(root)?,
            }),
            Self::Postgres {
                database_url,
                schema,
                table,
                target_name,
                ..
            } => postgres_footprint(database_url, schema, table, target_name.as_str()),
        }
    }

    pub(crate) fn verify_trait_receipt(&self, receipt: &Receipt) -> Result<()> {
        let verification = match self {
            Self::DuckDb { database_path, .. } => {
                DestinationProtocol::verify(&DuckDbDestination::new(database_path)?, receipt)?
            }
            Self::Parquet { root, .. } => {
                DestinationProtocol::verify(&ParquetDestination::new_filesystem(root)?, receipt)?
            }
            Self::Postgres { database_url, .. } => DestinationProtocol::verify(
                &PostgresDestination::connect(database_url.clone())?,
                receipt,
            )?,
        };
        if !verification.verified {
            return Err(CdfError::destination(format!(
                "runtime chaos receipt {} did not verify through DestinationProtocol::verify: {}",
                verification.receipt_id,
                verification
                    .reason
                    .unwrap_or_else(|| "verification returned false".to_owned())
            )));
        }
        Ok(())
    }
}

impl DestinationFootprint {
    pub(crate) fn has_destination_write(&self) -> bool {
        match self {
            Self::DuckDb(snapshot) => {
                snapshot.loads_table_present
                    || snapshot.state_table_present
                    || !snapshot.loads.is_empty()
                    || !snapshot.state.is_empty()
            }
            Self::Parquet { files } => !files.is_empty(),
            Self::Postgres {
                target_rows,
                loads_rows,
                ..
            } => *target_rows > 0 || *loads_rows > 0,
        }
    }
}

pub(crate) fn duplicate_retry_behavior(
    destination: ChaosDestination,
    source: ProjectReceiptSource,
) -> String {
    match (destination, source) {
        (
            ChaosDestination::DuckDb | ChaosDestination::ParquetFilesystem,
            ProjectReceiptSource::DestinationCommit {
                duplicate: true,
                package_receipt_recorded: false,
            },
        ) => "no-op duplicate: package-token destination reported duplicate=true and destination footprint was unchanged".to_owned(),
        (
            ChaosDestination::Postgres,
            ProjectReceiptSource::DestinationCommitReceiptOnly {
                package_receipt_recorded: false,
            },
        ) => "no-op duplicate: Postgres returned the stable receipt and destination footprint was unchanged".to_owned(),
        (_, other) => panic!("unexpected duplicate retry receipt source: {other:?}"),
    }
}

fn target_table(destination: ChaosDestination, window: ChaosCrashWindow) -> String {
    let destination = match destination {
        ChaosDestination::DuckDb => "ddb",
        ChaosDestination::ParquetFilesystem => "pq",
        ChaosDestination::Postgres => "pg",
    };
    let window = match window {
        ChaosCrashWindow::PackageReplayVerifiedBeforeDestinationWrite => "pkg",
        ChaosCrashWindow::CheckpointProposedBeforeDestinationWrite => "prop",
        ChaosCrashWindow::DestinationReceiptRecordedVerifiedBeforeCheckpointCommit => "rcpt",
        ChaosCrashWindow::CheckpointCommittedBeforePackageStatusCheckpointed => "ckpt",
    };
    format!("chaos_{destination}_{window}")
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
    target_name: &str,
) -> Result<DestinationFootprint> {
    let mut client = Client::connect(database_url, NoTls)
        .map_err(|error| CdfError::destination(format!("connect to Postgres: {error}")))?;
    Ok(DestinationFootprint::Postgres {
        target_rows: query_count_if_exists(&mut client, schema, table)?,
        loads_rows: query_load_count_if_exists(&mut client, schema, target_name)?,
        state_rows: query_count_if_exists(&mut client, schema, "_cdf_state")?,
    })
}

fn query_count_if_exists(client: &mut Client, schema: &str, table: &str) -> Result<i64> {
    if !table_exists(client, schema, table)? {
        return Ok(0);
    }
    client
        .query_one(
            &format!(
                "SELECT COUNT(*)::bigint FROM {}",
                qualified_name(schema, table)
            ),
            &[],
        )
        .map(|row| row.get(0))
        .map_err(|error| {
            CdfError::destination(format!(
                "query Postgres row count for {schema}.{table}: {error}"
            ))
        })
}

fn query_load_count_if_exists(client: &mut Client, schema: &str, target_name: &str) -> Result<i64> {
    if !table_exists(client, schema, "_cdf_loads")? {
        return Ok(0);
    }
    client
        .query_one(
            &format!(
                "SELECT COUNT(*)::bigint FROM {} WHERE \"target\" = $1",
                qualified_name(schema, "_cdf_loads")
            ),
            &[&target_name],
        )
        .map(|row| row.get(0))
        .map_err(|error| {
            CdfError::destination(format!(
                "query Postgres _cdf_loads row count for target {target_name}: {error}"
            ))
        })
}

fn table_exists(client: &mut Client, schema: &str, table: &str) -> Result<bool> {
    client
        .query_one(
            "SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = $2
            )",
            &[&schema, &table],
        )
        .map(|row| row.get(0))
        .map_err(|error| {
            CdfError::destination(format!(
                "query Postgres table existence for {schema}.{table}: {error}"
            ))
        })
}
