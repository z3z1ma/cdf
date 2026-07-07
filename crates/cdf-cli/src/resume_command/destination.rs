use std::path::PathBuf;

use cdf_kernel::CdfError;

use crate::{
    context::ProjectContext,
    destination_uri::{parquet_filesystem_root, postgres_database_url, redact_error_value},
    output::CliError,
};

pub(super) enum SelectedDestination {
    DuckDb {
        path: PathBuf,
    },
    Parquet {
        root: PathBuf,
    },
    Postgres {
        destination: Box<cdf_dest_postgres::PostgresDestination>,
        secret_backed: bool,
    },
    Unsupported {
        guidance: String,
    },
}

impl SelectedDestination {
    pub(super) fn from_context(
        context: &ProjectContext,
        command: &'static str,
    ) -> Result<Self, CliError> {
        let uri = &context.environment.destination;
        if uri.starts_with("duckdb://") {
            let Some(path) = context.duckdb_destination_path() else {
                return Ok(Self::Unsupported {
                    guidance: format!("destination URI `{uri}` is not a local DuckDB path"),
                });
            };
            return Ok(Self::DuckDb { path });
        }
        if uri.starts_with("parquet://") {
            return Ok(Self::Parquet {
                root: parquet_filesystem_root(context, uri, command)?,
            });
        }
        if uri.starts_with("postgres://") {
            let (database_url, secret_backed) = postgres_database_url(context, uri, command)?;
            let destination = cdf_dest_postgres::PostgresDestination::connect(database_url.clone())
                .map_err(|error| {
                    redact_error_value(error, secret_backed.then_some(database_url.as_str()))
                })?;
            return Ok(Self::Postgres {
                destination: Box::new(destination),
                secret_backed,
            });
        }
        Ok(Self::Unsupported {
            guidance: format!(
                "selected environment destination URI `{uri}` is unsupported for resume recovery"
            ),
        })
    }
}

pub(super) fn redact_postgres_resume_error(
    error: CdfError,
    destination: &cdf_dest_postgres::PostgresDestination,
    secret_backed: bool,
) -> CdfError {
    let secret = secret_backed.then(|| destination.database_url()).flatten();
    redact_error_value(error, secret)
}

pub(super) fn postgres_resume_replay_dedup(
    context: &ProjectContext,
) -> Result<cdf_dest_postgres::MergeDedupPolicy, CdfError> {
    let policy = context
        .environment
        .destination_policy
        .postgres
        .as_ref()
        .ok_or_else(|| {
            CdfError::contract(format!(
                "Postgres cdf resume requires [environments.{}.destination_policy.postgres] merge_dedup = \"fail\"",
                context.environment.name
            ))
        })?;
    match policy.merge_dedup {
        cdf_project::PostgresMergeDedupPolicy::Fail => {
            Ok(cdf_dest_postgres::MergeDedupPolicy::Fail)
        }
    }
}
