use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use cdf_dest_duckdb::{DuckDbDestination, DuckDbMirrorLoadRow, DuckDbMirrorStateRow};
use cdf_kernel::{CdfError, Receipt, StateDelta};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
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
    if !state_path.exists() {
        return Ok(skipped(
            "SQLite state database is absent; drift probe would create it",
            state_path,
            duckdb_path,
        ));
    }
    if !duckdb_path.exists() {
        return Ok(skipped(
            "DuckDB destination database is absent; drift probe would create it",
            state_path,
            duckdb_path,
        ));
    }

    let ledger_heads = read_committed_heads(&state_path)?;
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
                    scope_json: serde_json::to_string(&segment.scope).map_err(json_error)?,
                    output_position_json: serde_json::to_string(&segment.output_position)
                        .map_err(json_error)?,
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

fn read_committed_heads(path: &PathBuf) -> Result<Vec<LedgerHead>, CliError> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
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
            let delta = serde_json::from_str(&delta_json).map_err(json_from_sql)?;
            let receipt = serde_json::from_str(&receipt_json).map_err(json_from_sql)?;
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
                json_equal(&actual.receipt_json, &expected.receipt_json)?,
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

fn json_equal(left: &str, right: &str) -> Result<bool, CliError> {
    let left = serde_json::from_str::<Value>(left).map_err(json_error)?;
    let right = serde_json::from_str::<Value>(right).map_err(json_error)?;
    Ok(left == right)
}

fn optional_json_equal(left: Option<&str>, right: Option<&str>) -> Result<bool, CliError> {
    match (left, right) {
        (Some(left), Some(right)) => json_equal(left, right),
        (None, None) => Ok(true),
        _ => Ok(false),
    }
}

fn json_from_sql(error: serde_json::Error) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
}

fn sqlite_error(error: rusqlite::Error) -> CliError {
    CliError::mapped(
        CdfError::data(format!(
            "query SQLite checkpoint ledger for doctor drift check: {error}"
        )),
        error_catalog::DOCTOR_DRIFT,
    )
}

fn json_error(error: serde_json::Error) -> CliError {
    CliError::mapped(
        CdfError::data(format!("parse doctor drift JSON value: {error}")),
        error_catalog::DOCTOR_DRIFT,
    )
}
