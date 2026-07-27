use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use cdf_dest_duckdb::{DuckDbDestination, DuckDbMirrorLoadRow, DuckDbMirrorStateRow};
use cdf_kernel::{CdfError, Receipt, StateDelta};
use cdf_state_sqlite::SqliteCheckpointStore;
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde::Serialize;
use serde_json::{Value, json};

use crate::{context::ProjectContext, error_catalog, output::CliError};

const EXAMPLE_LIMIT: usize = 5;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DriftProbe {
    pub status: DriftStatus,
    pub message: String,
    pub details: Value,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DriftStatus {
    Passed,
    Failed,
    Skipped,
    Unsupported,
}

#[derive(Clone, Debug)]
struct LedgerHead {
    checkpoint_id: String,
    delta: StateDelta,
    receipt: Receipt,
    receipt_json: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct LoadKey {
    target: String,
    idempotency_token: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct StateKey {
    target: String,
    package_hash: String,
    segment_id: String,
}

#[derive(Clone, Debug)]
struct ExpectedLoad {
    checkpoint_id: String,
    package_hash: String,
    receipt_id: String,
    receipt_json: String,
}

#[derive(Clone, Debug)]
struct ExpectedState {
    checkpoint_id: String,
    scope_json: String,
    output_position_json: String,
    row_count: u64,
    byte_count: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
struct DriftCounts {
    ledger_heads: usize,
    expected_loads: usize,
    expected_state_rows: usize,
    mirror_loads: usize,
    mirror_state_rows: usize,
    missing_loads: usize,
    mismatched_loads: usize,
    extra_loads: usize,
    missing_state_rows: usize,
    mismatched_state_rows: usize,
    extra_state_rows: usize,
}

#[derive(Clone, Debug, Serialize)]
struct DriftExample {
    kind: &'static str,
    reason: String,
    checkpoint_id: Option<String>,
    target: Option<String>,
    idempotency_token: Option<String>,
    package_hash: Option<String>,
    segment_id: Option<String>,
    field: Option<&'static str>,
}

pub(crate) fn probe(context: &ProjectContext) -> Result<DriftProbe, CliError> {
    let Some(duckdb_path) = context.duckdb_destination_path() else {
        return Ok(DriftProbe {
            status: DriftStatus::Unsupported,
            message: "ledger/destination drift is implemented for duckdb:// destinations only"
                .to_owned(),
            details: json!({ "destination": context.environment.destination }),
        });
    };

    let state_path = context.state_store_path()?;
    let state_path_ownership = context.state_store_path_ownership();
    if !cdf_state_sqlite::database_path_exists(&state_path, state_path_ownership)? {
        return Ok(skipped(
            "SQLite state database is absent; drift probe would create it",
            state_path,
            duckdb_path,
        ));
    }
    if !destination_database_path_exists(&duckdb_path)? {
        return Ok(skipped(
            "DuckDB destination database is absent; drift probe would create it",
            state_path,
            duckdb_path,
        ));
    }

    let ledger_heads = read_committed_heads(&state_path, state_path_ownership)?;
    let destination = DuckDbDestination::new(&duckdb_path)?;
    let mirror = destination.read_mirror_snapshot_read_only()?;

    let mut expected_loads = BTreeMap::new();
    let mut expected_states = BTreeMap::new();
    for head in &ledger_heads {
        let target = head.receipt.target.as_str().to_owned();
        let load_key = LoadKey {
            target: target.clone(),
            idempotency_token: head.receipt.idempotency_token.as_str().to_owned(),
        };
        expected_loads.insert(
            load_key,
            ExpectedLoad {
                checkpoint_id: head.checkpoint_id.clone(),
                package_hash: head.receipt.package_hash.as_str().to_owned(),
                receipt_id: head.receipt.receipt_id.as_str().to_owned(),
                receipt_json: head.receipt_json.clone(),
            },
        );

        for segment in &head.delta.segments {
            let key = StateKey {
                target: target.clone(),
                package_hash: head.delta.package_hash.as_str().to_owned(),
                segment_id: segment.segment_id.as_str().to_owned(),
            };
            expected_states.insert(
                key,
                ExpectedState {
                    checkpoint_id: head.checkpoint_id.clone(),
                    scope_json: serde_json::to_string(&segment.scope)
                        .map_err(private_json_error)?,
                    output_position_json: serde_json::to_string(&segment.output_position)
                        .map_err(private_json_error)?,
                    row_count: segment.row_count,
                    byte_count: segment.byte_count,
                },
            );
        }
    }

    let mirror_loads = mirror
        .loads
        .iter()
        .map(|row| {
            (
                LoadKey {
                    target: row.target.clone(),
                    idempotency_token: row.idempotency_token.clone(),
                },
                row,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mirror_states = mirror
        .state
        .iter()
        .map(|row| {
            (
                StateKey {
                    target: row.target.clone(),
                    package_hash: row.package_hash.clone(),
                    segment_id: row.segment_id.clone(),
                },
                row,
            )
        })
        .collect::<BTreeMap<_, _>>();

    let mut counts = DriftCounts {
        ledger_heads: ledger_heads.len(),
        expected_loads: expected_loads.len(),
        expected_state_rows: expected_states.len(),
        mirror_loads: mirror.loads.len(),
        mirror_state_rows: mirror.state.len(),
        ..DriftCounts::default()
    };
    let mut examples = Vec::new();

    compare_loads(&expected_loads, &mirror_loads, &mut counts, &mut examples)?;
    compare_states(&expected_states, &mirror_states, &mut counts, &mut examples)?;
    record_extra_loads(
        expected_loads.keys().collect(),
        mirror_loads.keys().collect(),
        &mut counts,
        &mut examples,
    );
    record_extra_states(
        expected_states.keys().collect(),
        mirror_states.keys().collect(),
        &mut counts,
        &mut examples,
    );

    let issue_count = counts.missing_loads
        + counts.mismatched_loads
        + counts.extra_loads
        + counts.missing_state_rows
        + counts.mismatched_state_rows
        + counts.extra_state_rows;
    let details = json!({
        "state_database": state_path,
        "duckdb_database": duckdb_path,
        "mirror_tables": {
            "loads": mirror.loads_table_present,
            "state": mirror.state_table_present,
        },
        "counts": counts,
        "examples": examples,
    });

    if issue_count == 0 {
        Ok(DriftProbe {
            status: DriftStatus::Passed,
            message: format!(
                "ledger/destination mirrors match: {} committed head(s), {} state segment row(s)",
                ledger_heads.len(),
                mirror.state.len()
            ),
            details,
        })
    } else {
        Ok(DriftProbe {
            status: DriftStatus::Failed,
            message: format!("ledger/destination drift found: {issue_count} issue(s)"),
            details,
        })
    }
}

fn skipped(message: &str, state_path: PathBuf, duckdb_path: PathBuf) -> DriftProbe {
    DriftProbe {
        status: DriftStatus::Skipped,
        message: message.to_owned(),
        details: json!({
            "state_database": state_path,
            "duckdb_database": duckdb_path,
        }),
    }
}

fn read_committed_heads(
    path: &PathBuf,
    ownership: cdf_state_sqlite::StateStorePathOwnership,
) -> Result<Vec<LedgerHead>, CliError> {
    let open_path = cdf_state_sqlite::database_open_path(path, ownership)?;
    let conn = Connection::open_with_flags(
        open_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )
    .map_err(sqlite_error)?;
    let has_checkpoints = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'cdf_checkpoints'",
            [],
            |_| Ok(()),
        )
        .optional()
        .map_err(sqlite_error)?
        .is_some();
    let checkpoint_footprint =
        has_checkpoints || doctor_component_marker_exists(&conn, "checkpoint_store")?;
    if !checkpoint_footprint {
        return Ok(Vec::new());
    }
    SqliteCheckpointStore::open_read_only_with_path_ownership(path, ownership)
        .and_then(|store| store.validate_integrity())
        .map_err(doctor_store_error)?;
    if !has_checkpoints {
        return Ok(Vec::new());
    }

    let mut stmt = conn
        .prepare(
            "SELECT checkpoint_id, delta_json, receipt_json \
             FROM cdf_checkpoints \
             WHERE status = 'committed' AND is_head = 1 AND receipt_json IS NOT NULL \
             ORDER BY sequence",
        )
        .map_err(sqlite_error)?;
    let rows = stmt
        .query_map([], |row| {
            let checkpoint_id: String = row.get(0)?;
            let delta_json: String = row.get(1)?;
            let receipt_json: String = row.get(2)?;
            let delta: StateDelta = serde_json::from_str(&delta_json)
                .map_err(|error| private_json_from_sql("checkpoint delta", error))?;
            let receipt: Receipt = serde_json::from_str(&receipt_json)
                .map_err(|error| private_json_from_sql("checkpoint receipt", error))?;
            validate_private_ledger_head(&checkpoint_id, &delta, &receipt)
                .map_err(private_state_from_sql)?;
            Ok(LedgerHead {
                checkpoint_id,
                delta,
                receipt,
                receipt_json,
            })
        })
        .map_err(sqlite_error)?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(sqlite_error)
}

fn validate_private_ledger_head(
    checkpoint_id: &str,
    delta: &StateDelta,
    receipt: &Receipt,
) -> Result<(), CdfError> {
    cdf_kernel::CheckpointId::new(checkpoint_id.to_owned()).map_err(|error| {
        CdfError::internal(format!(
            "decode CDF-managed doctor drift checkpoint id: {}",
            error.message
        ))
    })?;
    for (field, value) in [
        ("delta checkpoint id", delta.checkpoint_id.as_str()),
        ("delta pipeline id", delta.pipeline_id.as_str()),
        ("delta resource id", delta.resource_id.as_str()),
        ("delta package hash", delta.package_hash.as_str()),
        ("delta schema hash", delta.schema_hash.as_str()),
        ("receipt id", receipt.receipt_id.as_str()),
        ("receipt destination id", receipt.destination.as_str()),
        ("receipt target", receipt.target.as_str()),
        ("receipt package hash", receipt.package_hash.as_str()),
        ("receipt schema hash", receipt.schema_hash.as_str()),
        (
            "receipt idempotency token",
            receipt.idempotency_token.as_str(),
        ),
    ] {
        if value.trim().is_empty() {
            return Err(CdfError::internal(format!(
                "decode CDF-managed doctor drift {field}: value cannot be empty"
            )));
        }
    }
    delta.validate().map_err(|error| {
        CdfError::internal(format!(
            "validate CDF-managed doctor drift checkpoint delta: {}",
            error.message
        ))
    })?;
    if delta.checkpoint_id.as_str() != checkpoint_id {
        return Err(CdfError::internal(
            "CDF-managed doctor drift checkpoint id column does not match delta JSON",
        ));
    }
    if !receipt.covers_state_delta(delta) {
        return Err(CdfError::internal(
            "CDF-managed doctor drift receipt does not cover its checkpoint delta",
        ));
    }
    let acknowledgements = receipt
        .segment_acks
        .iter()
        .map(|ack| (&ack.segment_id, ack))
        .collect::<BTreeMap<_, _>>();
    if !delta.segments.iter().all(|segment| {
        acknowledgements
            .get(&segment.segment_id)
            .is_some_and(|ack| {
                ack.row_count == segment.row_count && ack.byte_count == segment.byte_count
            })
    }) {
        return Err(CdfError::internal(
            "CDF-managed doctor drift receipt segment counts do not match checkpoint delta",
        ));
    }
    Ok(())
}

fn compare_loads(
    expected: &BTreeMap<LoadKey, ExpectedLoad>,
    actual: &BTreeMap<LoadKey, &DuckDbMirrorLoadRow>,
    counts: &mut DriftCounts,
    examples: &mut Vec<DriftExample>,
) -> Result<(), CliError> {
    for (key, expected) in expected {
        let Some(actual) = actual.get(key) else {
            counts.missing_loads += 1;
            push_example(
                examples,
                DriftExample {
                    kind: "missing_load",
                    reason: "no _cdf_loads row for committed ledger receipt".to_owned(),
                    checkpoint_id: Some(expected.checkpoint_id.clone()),
                    target: Some(key.target.clone()),
                    idempotency_token: Some(key.idempotency_token.clone()),
                    package_hash: Some(expected.package_hash.clone()),
                    segment_id: None,
                    field: None,
                },
            );
            continue;
        };

        for (field, matches) in [
            ("package_hash", actual.package_hash == expected.package_hash),
            ("receipt_id", actual.receipt_id == expected.receipt_id),
            (
                "receipt_json",
                destination_private_json_equal(&actual.receipt_json, &expected.receipt_json)?,
            ),
        ] {
            if !matches {
                counts.mismatched_loads += 1;
                push_example(
                    examples,
                    DriftExample {
                        kind: "mismatched_load",
                        reason: "_cdf_loads field differs from committed ledger receipt".to_owned(),
                        checkpoint_id: Some(expected.checkpoint_id.clone()),
                        target: Some(key.target.clone()),
                        idempotency_token: Some(key.idempotency_token.clone()),
                        package_hash: Some(expected.package_hash.clone()),
                        segment_id: None,
                        field: Some(field),
                    },
                );
            }
        }
    }
    Ok(())
}

fn compare_states(
    expected: &BTreeMap<StateKey, ExpectedState>,
    actual: &BTreeMap<StateKey, &DuckDbMirrorStateRow>,
    counts: &mut DriftCounts,
    examples: &mut Vec<DriftExample>,
) -> Result<(), CliError> {
    for (key, expected) in expected {
        let Some(actual) = actual.get(key) else {
            counts.missing_state_rows += 1;
            push_example(
                examples,
                DriftExample {
                    kind: "missing_state",
                    reason: "no _cdf_state row for committed ledger segment".to_owned(),
                    checkpoint_id: Some(expected.checkpoint_id.clone()),
                    target: Some(key.target.clone()),
                    idempotency_token: None,
                    package_hash: Some(key.package_hash.clone()),
                    segment_id: Some(key.segment_id.clone()),
                    field: None,
                },
            );
            continue;
        };

        for (field, matches) in [
            (
                "scope_json",
                optional_json_equal(actual.scope_json.as_deref(), Some(&expected.scope_json))?,
            ),
            (
                "output_position_json",
                optional_json_equal(
                    actual.output_position_json.as_deref(),
                    Some(&expected.output_position_json),
                )?,
            ),
            ("row_count", actual.row_count == expected.row_count),
            ("byte_count", actual.byte_count == expected.byte_count),
        ] {
            if !matches {
                counts.mismatched_state_rows += 1;
                push_example(
                    examples,
                    DriftExample {
                        kind: "mismatched_state",
                        reason: "_cdf_state field differs from committed ledger segment".to_owned(),
                        checkpoint_id: Some(expected.checkpoint_id.clone()),
                        target: Some(key.target.clone()),
                        idempotency_token: None,
                        package_hash: Some(key.package_hash.clone()),
                        segment_id: Some(key.segment_id.clone()),
                        field: Some(field),
                    },
                );
            }
        }
    }
    Ok(())
}

fn record_extra_loads(
    expected: BTreeSet<&LoadKey>,
    actual: BTreeSet<&LoadKey>,
    counts: &mut DriftCounts,
    examples: &mut Vec<DriftExample>,
) {
    for key in actual.difference(&expected) {
        counts.extra_loads += 1;
        push_example(
            examples,
            DriftExample {
                kind: "extra_load",
                reason: "_cdf_loads row has no committed local ledger head".to_owned(),
                checkpoint_id: None,
                target: Some(key.target.clone()),
                idempotency_token: Some(key.idempotency_token.clone()),
                package_hash: None,
                segment_id: None,
                field: None,
            },
        );
    }
}

fn record_extra_states(
    expected: BTreeSet<&StateKey>,
    actual: BTreeSet<&StateKey>,
    counts: &mut DriftCounts,
    examples: &mut Vec<DriftExample>,
) {
    for key in actual.difference(&expected) {
        counts.extra_state_rows += 1;
        push_example(
            examples,
            DriftExample {
                kind: "extra_state",
                reason: "_cdf_state row has no committed local ledger segment".to_owned(),
                checkpoint_id: None,
                target: Some(key.target.clone()),
                idempotency_token: None,
                package_hash: Some(key.package_hash.clone()),
                segment_id: Some(key.segment_id.clone()),
                field: None,
            },
        );
    }
}

fn push_example(examples: &mut Vec<DriftExample>, example: DriftExample) {
    if examples.len() < EXAMPLE_LIMIT {
        examples.push(example);
    }
}

fn destination_private_json_equal(actual: &str, expected: &str) -> Result<bool, CliError> {
    let left = serde_json::from_str::<Value>(actual).map_err(destination_json_error)?;
    let right = serde_json::from_str::<Value>(expected).map_err(private_json_error)?;
    Ok(left == right)
}

fn optional_json_equal(actual: Option<&str>, expected: Option<&str>) -> Result<bool, CliError> {
    match (actual, expected) {
        (Some(actual), Some(expected)) => destination_private_json_equal(actual, expected),
        (None, None) => Ok(true),
        _ => Ok(false),
    }
}

fn private_json_from_sql(field: &str, error: serde_json::Error) -> rusqlite::Error {
    private_state_from_sql(CdfError::internal(format!(
        "decode CDF-managed doctor drift {field}: {error}"
    )))
}

fn private_state_from_sql(error: CdfError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
}

fn sqlite_error(error: rusqlite::Error) -> CliError {
    let error = cdf_state_sqlite::classify_sqlite_error(
        cdf_state_sqlite::SqliteErrorContext::ManagedState,
        "query CDF-managed SQLite checkpoint ledger for doctor drift check",
        error,
    );
    if error.kind == cdf_kernel::ErrorKind::Internal {
        CliError::mapped(error, error_catalog::DOCTOR_DRIFT)
    } else {
        error.into()
    }
}

fn doctor_store_error(error: CdfError) -> CliError {
    if error.kind == cdf_kernel::ErrorKind::Internal {
        CliError::mapped(error, error_catalog::DOCTOR_DRIFT)
    } else {
        error.into()
    }
}

fn doctor_component_marker_exists(conn: &Connection, component: &str) -> Result<bool, CliError> {
    let version_table = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'cdf_sqlite_schema_versions'",
            [],
            |_| Ok(()),
        )
        .optional()
        .map_err(sqlite_error)?
        .is_some();
    if !version_table {
        return Ok(false);
    }
    conn.query_row(
        "SELECT 1 FROM cdf_sqlite_schema_versions WHERE component = ?",
        params![component],
        |_| Ok(()),
    )
    .optional()
    .map(|value| value.is_some())
    .map_err(sqlite_error)
}

fn destination_json_error(error: serde_json::Error) -> CliError {
    CdfError::destination(format!("parse DuckDB doctor drift mirror JSON: {error}")).into()
}

fn private_json_error(error: serde_json::Error) -> CliError {
    CliError::mapped(
        CdfError::internal(format!(
            "parse CDF-managed doctor drift expected JSON: {error}"
        )),
        error_catalog::DOCTOR_DRIFT,
    )
}

fn destination_database_path_exists(path: &Path) -> Result<bool, CliError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(CdfError::destination(format!(
            "DuckDB destination database {} is a symlink; configure the real destination file path",
            path.display()
        ))
        .into()),
        Ok(metadata) if metadata.is_file() => Ok(true),
        Ok(_) => Err(CdfError::destination(format!(
            "DuckDB destination database {} is not a regular file",
            path.display()
        ))
        .into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            validate_missing_destination_ancestors(path)?;
            Ok(false)
        }
        Err(error)
            if error.kind() == std::io::ErrorKind::NotADirectory
                || cdf_kernel::is_filesystem_loop(&error) =>
        {
            Err(CdfError::destination(format!(
                "DuckDB destination database {} has an invalid filesystem shape: {error}",
                path.display()
            ))
            .into())
        }
        Err(error) => Err(CdfError::environment(format!(
            "inspect DuckDB destination database {}: {error}; check destination-path permissions, device availability, and process file limits before retrying",
            path.display()
        ))
        .into()),
    }
}

fn validate_missing_destination_ancestors(path: &Path) -> Result<(), CliError> {
    let mut cursor = path.parent();
    while let Some(parent) = cursor {
        if parent.as_os_str().is_empty() {
            return Ok(());
        }
        match std::fs::metadata(parent) {
            Ok(metadata) if !metadata.is_dir() => {
                return Err(CdfError::destination(format!(
                    "DuckDB destination database ancestor {} is not a real directory",
                    parent.display()
                ))
                .into());
            }
            Ok(_) => return Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match std::fs::symlink_metadata(parent) {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        return Err(CdfError::destination(format!(
                            "DuckDB destination database ancestor {} is a dangling symlink",
                            parent.display()
                        ))
                        .into());
                    }
                    Ok(_) => {
                        return Err(CdfError::destination(format!(
                            "DuckDB destination database ancestor {} changed filesystem shape during inspection",
                            parent.display()
                        ))
                        .into());
                    }
                    Err(link_error) if link_error.kind() == std::io::ErrorKind::NotFound => {
                        cursor = parent.parent();
                    }
                    Err(link_error) => {
                        return Err(CdfError::environment(format!(
                            "inspect DuckDB destination database ancestor {}: {link_error}; check destination-path permissions, device availability, and process file limits before retrying",
                            parent.display()
                        ))
                        .into());
                    }
                }
            }
            Err(error)
                if error.kind() == std::io::ErrorKind::NotADirectory
                    || cdf_kernel::is_filesystem_loop(&error) =>
            {
                return Err(CdfError::destination(format!(
                    "DuckDB destination database ancestor {} has an invalid filesystem shape: {error}",
                    parent.display()
                ))
                .into());
            }
            Err(error) => {
                return Err(CdfError::environment(format!(
                    "inspect DuckDB destination database ancestor {}: {error}; check destination-path permissions, device availability, and process file limits before retrying",
                    parent.display()
                ))
                .into());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use cdf_kernel::ErrorKind;

    use super::*;

    #[test]
    fn doctor_rejects_checkpoint_marker_without_owned_table() {
        let root = tempfile::tempdir().unwrap();
        let state_path = root.path().join(".cdf").join("state.db");
        std::fs::create_dir_all(state_path.parent().unwrap()).unwrap();
        let conn = Connection::open(&state_path).unwrap();
        conn.execute_batch(
            "
            CREATE TABLE cdf_sqlite_schema_versions (
                component TEXT PRIMARY KEY,
                version INTEGER NOT NULL,
                recorded_at_ms INTEGER NOT NULL
            );
            INSERT INTO cdf_sqlite_schema_versions (component, version, recorded_at_ms)
            VALUES ('checkpoint_store', 1, 1);
            ",
        )
        .unwrap();
        drop(conn);

        let error = read_committed_heads(
            &state_path,
            cdf_state_sqlite::StateStorePathOwnership::CdfManaged,
        )
        .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Internal);
        assert_eq!(error.code, error_catalog::DOCTOR_DRIFT.code);
    }

    #[cfg(unix)]
    #[test]
    fn doctor_rejects_live_destination_database_symlink() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let real = root.path().join("real.duckdb");
        let link = root.path().join("linked.duckdb");
        std::fs::write(&real, b"duckdb").unwrap();
        symlink(&real, &link).unwrap();

        let error = destination_database_path_exists(&link).unwrap_err();

        assert_eq!(error.kind, ErrorKind::Destination);
        assert!(error.message.contains("is a symlink"));
    }
}
