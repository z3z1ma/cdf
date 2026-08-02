use std::path::Path;

use cdf_kernel::{CdfError, Receipt, Result};
use rusqlite::{Connection, OpenFlags, params};

use crate::{
    error::{classify_sqlite_error, classify_sqlite_open_error},
    mirrors::{SqliteCommitEvidence, StoredRowRange, StoredSegment, stored_quarantine_evidence},
};

use super::writer::{ROW_KEY_COLUMN, install_progress_handler, table_columns};

#[cfg(test)]
pub(crate) fn verify_receipt(path: &Path, receipt: &Receipt) -> Result<()> {
    verify_receipt_with_cancellation(path, receipt, &cdf_runtime::RunCancellation::default())
}

pub(crate) fn verify_receipt_with_cancellation(
    path: &Path,
    receipt: &Receipt,
    cancellation: &cdf_runtime::RunCancellation,
) -> Result<()> {
    cancellation.check()?;
    if !path.try_exists().map_err(|error| {
        crate::error::classify_destination_io("inspect SQLite receipt database", &error)
    })? {
        return Err(CdfError::destination(
            "SQLite receipt database does not exist",
        ));
    }
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| classify_sqlite_open_error("open SQLite receipt verifier", path, error))?;
    install_progress_handler(&connection, cancellation)?;
    verify_receipt_on_connection(&connection, receipt, cancellation)
}

pub(super) fn verify_receipt_on_connection(
    connection: &Connection,
    receipt: &Receipt,
    cancellation: &cdf_runtime::RunCancellation,
) -> Result<()> {
    cancellation.check()?;
    let stored_json: String = connection
        .query_row(
            "SELECT receipt_json FROM _cdf_loads
             WHERE target = ?1 AND package_hash = ?2 AND idempotency_token = ?3",
            params![
                receipt.target.as_str(),
                receipt.package_hash.as_str(),
                receipt.idempotency_token.as_str()
            ],
            |row| row.get(0),
        )
        .map_err(|error| classify_sqlite_error("verify SQLite load receipt", error))?;
    let stored: Receipt = serde_json::from_str(&stored_json).map_err(|error| {
        CdfError::destination(format!("decode SQLite verification receipt: {error}"))
    })?;
    if &stored != receipt {
        return Err(CdfError::destination(
            "SQLite load mirror differs from the expected receipt",
        ));
    }
    let transaction = receipt
        .transaction
        .as_ref()
        .ok_or_else(|| CdfError::destination("SQLite receipt is missing transaction metadata"))?;
    let evidence = crate::mirrors::read_commit_evidence(connection, &receipt.receipt_id)?
        .ok_or_else(|| CdfError::destination("SQLite receipt has no immutable commit evidence"))?;
    if evidence.version != 1 {
        return Err(CdfError::destination(format!(
            "SQLite receipt has unsupported commit evidence version {}",
            evidence.version
        )));
    }
    let expected_evidence_hash = transaction
        .values
        .get("commit_evidence_sha256_v1")
        .ok_or_else(|| CdfError::destination("SQLite receipt is missing commit evidence hash"))?;
    if evidence.sha256()? != *expected_evidence_hash {
        return Err(CdfError::destination(
            "SQLite immutable commit evidence differs from the receipt hash",
        ));
    }
    let expected_quarantine_count = transaction
        .values
        .get("quarantine_count")
        .ok_or_else(|| CdfError::destination("SQLite receipt is missing quarantine count"))?
        .parse::<u64>()
        .map_err(|error| {
            CdfError::destination(format!(
                "SQLite receipt quarantine count is invalid: {error}"
            ))
        })?;
    let expected_quarantine_hash = transaction
        .values
        .get("quarantine_multiset_sha256_v1")
        .ok_or_else(|| CdfError::destination("SQLite receipt is missing quarantine evidence"))?;
    if evidence.quarantine.count != expected_quarantine_count
        || evidence.quarantine.hex_commitment() != *expected_quarantine_hash
    {
        return Err(CdfError::destination(
            "SQLite immutable quarantine evidence differs from receipt metadata",
        ));
    }
    let quarantine =
        stored_quarantine_evidence(connection, &receipt.target, &receipt.package_hash)?;
    cancellation.check()?;
    if quarantine != evidence.quarantine {
        return Err(CdfError::destination(
            "SQLite quarantine mirror differs from immutable commit evidence",
        ));
    }
    verify_target_authority(connection, receipt, &evidence)?;
    verify_state_history(connection, receipt, &evidence)?;
    verify_segment_history(connection, receipt, &evidence)?;
    cancellation.check()?;
    Ok(())
}

fn verify_target_authority(
    connection: &Connection,
    receipt: &Receipt,
    evidence: &SqliteCommitEvidence,
) -> Result<()> {
    let Some(expected) = &evidence.target_schema else {
        if evidence.provenance.is_some() || !evidence.segments.is_empty() {
            return Err(CdfError::destination(
                "SQLite commit evidence has inconsistent target authority",
            ));
        }
        return Ok(());
    };
    let actual = table_columns(connection, receipt.target.as_str())?;
    for column in expected {
        let Some((stored_type, not_null)) = actual.get(column.name.as_str()) else {
            return Err(CdfError::destination(format!(
                "SQLite target is missing receipt column {}",
                column.name
            )));
        };
        if !stored_type.eq_ignore_ascii_case(&column.sqlite_type) || column.nullable == *not_null {
            return Err(CdfError::destination(format!(
                "SQLite target column {} differs from receipt schema evidence",
                column.name
            )));
        }
    }
    if !actual.contains_key(ROW_KEY_COLUMN) {
        return Err(CdfError::destination(
            "SQLite target is missing row provenance required by the receipt",
        ));
    }
    let provenance = evidence.provenance.as_ref().ok_or_else(|| {
        CdfError::destination("SQLite commit evidence is missing provenance-index authority")
    })?;
    if provenance.target != receipt.target.as_str()
        || provenance.row_key_column != ROW_KEY_COLUMN
        || !provenance.unique
        || !provenance.partial
    {
        return Err(CdfError::destination(
            "SQLite commit evidence has invalid provenance-index authority",
        ));
    }
    let mut statement = connection
        .prepare(&format!(
            "PRAGMA index_list({})",
            crate::identifier::quote_identifier(receipt.target.as_str())
        ))
        .map_err(|error| classify_sqlite_error("inspect SQLite provenance indexes", error))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)? != 0,
                row.get::<_, i64>(4)? != 0,
            ))
        })
        .map_err(|error| classify_sqlite_error("query SQLite provenance indexes", error))?;
    let indexes = rows
        .map(|row| {
            row.map_err(|error| classify_sqlite_error("decode SQLite provenance index", error))
        })
        .collect::<Result<Vec<_>>>()?;
    if !indexes.iter().any(|(name, unique, partial)| {
        name == &provenance.index_name
            && *unique == provenance.unique
            && *partial == provenance.partial
    }) {
        return Err(CdfError::destination(
            "SQLite target is missing the exact immutable provenance index",
        ));
    }
    let mut columns = connection
        .prepare(&format!(
            "PRAGMA index_info({})",
            crate::identifier::quote_identifier(&provenance.index_name)
        ))
        .map_err(|error| classify_sqlite_error("inspect SQLite provenance index columns", error))?;
    let names = columns
        .query_map([], |row| row.get::<_, String>(2))
        .map_err(|error| classify_sqlite_error("query SQLite provenance index columns", error))?
        .map(|row| {
            row.map_err(|error| {
                classify_sqlite_error("decode SQLite provenance index column", error)
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if names != [ROW_KEY_COLUMN] {
        return Err(CdfError::destination(
            "SQLite provenance index covers unexpected columns",
        ));
    }
    Ok(())
}

fn verify_state_history(
    connection: &Connection,
    receipt: &Receipt,
    evidence: &SqliteCommitEvidence,
) -> Result<()> {
    let stored = crate::mirrors::read_state_history(connection, &receipt.receipt_id)?;
    if stored != evidence.state {
        return Err(CdfError::destination(
            "SQLite immutable state history differs from commit evidence",
        ));
    }
    if let Some(state) = &stored
        && (state.receipt_id != receipt.receipt_id
            || state.package_hash != receipt.package_hash
            || state.schema_hash != receipt.schema_hash
            || state.committed_at_ms != receipt.committed_at_ms)
    {
        return Err(CdfError::destination(
            "SQLite typed state identity or position lineage differs from receipt authority",
        ));
    }
    Ok(())
}

fn verify_segment_history(
    connection: &Connection,
    receipt: &Receipt,
    evidence: &SqliteCommitEvidence,
) -> Result<()> {
    let mut statement = connection
        .prepare(
            "SELECT row_key_start, row_key_end, segment_json FROM _cdf_segments
             WHERE target = ?1 AND package_hash = ?2 AND idempotency_token = ?3",
        )
        .map_err(|error| classify_sqlite_error("prepare SQLite segment verification", error))?;
    let rows = statement
        .query_map(
            params![
                receipt.target.as_str(),
                receipt.package_hash.as_str(),
                receipt.idempotency_token.as_str()
            ],
            |row| {
                Ok((
                    row.get::<_, Option<i64>>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .map_err(|error| classify_sqlite_error("query SQLite segment verification", error))?;
    let mut stored = Vec::new();
    for row in rows {
        let (start, end, json) = row.map_err(|error| {
            classify_sqlite_error("decode SQLite segment verification row", error)
        })?;
        let segment: StoredSegment = serde_json::from_str(&json).map_err(|error| {
            CdfError::destination(format!("decode full SQLite segment evidence: {error}"))
        })?;
        let scalar_range = match (start, end) {
            (Some(start), Some(end)) => Some(StoredRowRange {
                segment_id: segment.segment_id.clone(),
                row_key_start: u64::try_from(start)
                    .map_err(|_| CdfError::destination("SQLite segment start is negative"))?,
                row_key_end: u64::try_from(end)
                    .map_err(|_| CdfError::destination("SQLite segment end is negative"))?,
            }),
            (None, None) => None,
            _ => {
                return Err(CdfError::destination(
                    "SQLite segment scalar range is incomplete",
                ));
            }
        };
        if scalar_range != segment.row_range {
            return Err(CdfError::destination(
                "SQLite segment scalar provenance differs from full segment JSON",
            ));
        }
        stored.push(segment);
    }
    let mut expected = evidence.segments.clone();
    stored.sort_by(|left, right| left.segment_id.as_str().cmp(right.segment_id.as_str()));
    expected.sort_by(|left, right| left.segment_id.as_str().cmp(right.segment_id.as_str()));
    if stored != expected {
        return Err(CdfError::destination(
            "SQLite full segment history differs from immutable commit evidence",
        ));
    }
    let mut ranges = Vec::new();
    for (ack, segment) in receipt.segment_acks.iter().zip(&evidence.segments) {
        if ack.segment_id != segment.segment_id
            || ack.row_count != segment.row_count
            || ack.byte_count != segment.byte_count
            || segment.target != receipt.target
            || segment.package_hash != receipt.package_hash
            || segment.idempotency_token != receipt.idempotency_token
            || segment.committed_at_ms != receipt.committed_at_ms
        {
            return Err(CdfError::destination(
                "SQLite segment identity, position, or count differs from receipt authority",
            ));
        }
        let range = segment.row_range.as_ref().ok_or_else(|| {
            CdfError::destination("SQLite segment evidence is missing immutable row provenance")
        })?;
        if range.segment_id != segment.segment_id
            || range.row_key_end.checked_sub(range.row_key_start) != Some(segment.row_count)
        {
            return Err(CdfError::destination(
                "SQLite segment row provenance is inconsistent with its typed evidence",
            ));
        }
        ranges.push((range.row_key_start, range.row_key_end));
    }
    if receipt.segment_acks.len() != evidence.segments.len() {
        return Err(CdfError::destination(
            "SQLite segment evidence cardinality differs from receipt acknowledgements",
        ));
    }
    ranges.sort_unstable();
    if ranges.windows(2).any(|pair| pair[0].1 > pair[1].0) {
        return Err(CdfError::destination(
            "SQLite immutable segment provenance ranges overlap",
        ));
    }
    Ok(())
}
