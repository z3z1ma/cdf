use std::{collections::BTreeMap, path::PathBuf};

use cdf_dest_duckdb::DuckDbDestination;
use cdf_dest_parquet::ParquetDestination;
use cdf_dest_postgres::{MergeDedupPolicy, PostgresDestination, PostgresTarget};
use cdf_kernel::{CdfError, DestinationProtocol, Receipt, Result};
use cdf_project::ResolvedProjectDestination;
use postgres::{Client, NoTls};

use super::{LIVE_LOCAL_FILE_POSTGRES_SCHEMA, LiveLocalFileFixtureSpec};
use crate::run_matrix::local_postgres::{qualified_name, reset_postgres_schema};

#[derive(Clone, Debug)]
pub(super) enum LiveRunDestinationHandle {
    DuckDb {
        database_path: PathBuf,
    },
    Parquet {
        root: PathBuf,
    },
    Postgres {
        database_url: String,
        target: PostgresTarget,
        schema: String,
        table: String,
    },
}

impl LiveRunDestinationHandle {
    pub(super) fn duckdb(spec: &LiveLocalFileFixtureSpec) -> Self {
        Self::DuckDb {
            database_path: spec.destination_path.clone(),
        }
    }

    pub(super) fn parquet(spec: &LiveLocalFileFixtureSpec) -> Self {
        Self::Parquet {
            root: spec.destination_path.clone(),
        }
    }

    pub(super) fn postgres(database_url: String, table: &str) -> Result<Self> {
        let schema = LIVE_LOCAL_FILE_POSTGRES_SCHEMA.to_owned();
        reset_postgres_schema(&database_url, &schema)?;
        Ok(Self::Postgres {
            database_url,
            target: PostgresTarget::new(Some(&schema), table)?,
            schema,
            table: table.to_owned(),
        })
    }

    pub(super) fn resolved(
        &self,
        spec: &LiveLocalFileFixtureSpec,
    ) -> Result<ResolvedProjectDestination> {
        match self {
            Self::DuckDb { database_path } => {
                ResolvedProjectDestination::duckdb(database_path, spec.target.clone())
            }
            Self::Parquet { root } => {
                ResolvedProjectDestination::parquet_filesystem(root, spec.target.clone())
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

    pub(super) fn verify_receipt(&self, receipt: &Receipt) -> Result<()> {
        let verification = match self {
            Self::DuckDb { database_path } => {
                let destination = DuckDbDestination::new(database_path)?;
                DestinationProtocol::verify(&destination, receipt)?
            }
            Self::Parquet { root } => {
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
                "live-run golden receipt {} did not verify: {}",
                verification.receipt_id,
                verification
                    .reason
                    .unwrap_or_else(|| "verification returned false".to_owned())
            )));
        }
        Ok(())
    }

    pub(super) fn destination_row_counts(
        &self,
        receipt: &Receipt,
    ) -> Result<BTreeMap<String, u64>> {
        match self {
            Self::DuckDb { database_path } => duckdb_row_counts(database_path, receipt),
            Self::Parquet { root } => parquet_row_counts(root, receipt),
            Self::Postgres {
                database_url,
                schema,
                table,
                ..
            } => postgres_row_counts(database_url, schema, table, receipt),
        }
    }
}

pub(super) fn postgres_target_name(table: &str) -> Result<cdf_kernel::TargetName> {
    cdf_kernel::TargetName::new(format!("{LIVE_LOCAL_FILE_POSTGRES_SCHEMA}.{table}"))
}

fn duckdb_row_counts(database_path: &PathBuf, receipt: &Receipt) -> Result<BTreeMap<String, u64>> {
    let destination = DuckDbDestination::new(database_path)?;
    let snapshot = destination.read_mirror_snapshot_read_only()?;
    Ok(BTreeMap::from([
        (
            "mirror_load_rows".to_owned(),
            u64::try_from(snapshot.loads.len()).unwrap(),
        ),
        (
            "mirror_state_rows".to_owned(),
            u64::try_from(snapshot.state.len()).unwrap(),
        ),
        (
            "mirror_state_row_count".to_owned(),
            snapshot.state.iter().map(|row| row.row_count).sum(),
        ),
        (
            "receipt_rows_written".to_owned(),
            receipt.counts.rows_written,
        ),
    ]))
}

fn parquet_row_counts(_root: &PathBuf, receipt: &Receipt) -> Result<BTreeMap<String, u64>> {
    let transaction = receipt
        .transaction
        .as_ref()
        .ok_or_else(|| CdfError::data("Parquet receipt missing transaction metadata"))?;
    let row_count = transaction_u64(transaction, "row_count")?;
    let object_count = transaction_u64(transaction, "object_count")?;
    Ok(BTreeMap::from([
        ("transaction_row_count".to_owned(), row_count),
        ("transaction_object_count".to_owned(), object_count),
        (
            "receipt_rows_written".to_owned(),
            receipt.counts.rows_written,
        ),
    ]))
}

fn postgres_row_counts(
    database_url: &str,
    schema: &str,
    table: &str,
    receipt: &Receipt,
) -> Result<BTreeMap<String, u64>> {
    let mut client = Client::connect(database_url, NoTls)
        .map_err(|error| CdfError::destination(format!("connect to Postgres: {error}")))?;
    Ok(BTreeMap::from([
        (
            "target_rows".to_owned(),
            query_count(&mut client, &qualified_name(schema, table))?,
        ),
        (
            "load_rows".to_owned(),
            query_count(&mut client, &qualified_name(schema, "_cdf_loads"))?,
        ),
        (
            "state_rows".to_owned(),
            query_count(&mut client, &qualified_name(schema, "_cdf_state"))?,
        ),
        (
            "receipt_rows_written".to_owned(),
            receipt.counts.rows_written,
        ),
    ]))
}

fn query_count(client: &mut Client, table: &str) -> Result<u64> {
    let count: i64 = client
        .query_one(&format!("SELECT COUNT(*)::bigint FROM {table}"), &[])
        .map(|row| row.get(0))
        .map_err(|error| CdfError::destination(format!("query row count from {table}: {error}")))?;
    u64::try_from(count)
        .map_err(|error| CdfError::destination(format!("negative row count from {table}: {error}")))
}

fn transaction_u64(transaction: &cdf_kernel::TransactionMetadata, field: &str) -> Result<u64> {
    transaction
        .values
        .get(field)
        .ok_or_else(|| CdfError::data(format!("Parquet receipt missing {field}")))?
        .parse::<u64>()
        .map_err(|error| CdfError::data(format!("parse Parquet receipt {field}: {error}")))
}
