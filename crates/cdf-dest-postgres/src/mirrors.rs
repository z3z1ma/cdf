use std::collections::BTreeMap;

use cdf_dest_sql::LoadMirrorRow;
use cdf_kernel::{
    CdfError, CommitCounts, IdempotencyToken, MigrationRecord, PackageHash, Receipt, Result,
    SchemaHash, StateSegment, TargetName, VerifyClause,
};
use cdf_postgres::PostgresIdentifier;
use postgres::Row;

use crate::{
    CDF_LOADS_TABLE, CDF_QUARANTINE_TABLE, CDF_STATE_TABLE, POSTGRES_DESTINATION_ID,
    identifiers::quote_identifier_unchecked,
    plan::{PostgresDriftHooks, PostgresLoadPlanInput, PostgresStatement, StatementExpectation},
    validate::disposition_name,
};

pub(crate) fn decode_postgres_load_row(row: Row) -> Result<LoadMirrorRow> {
    let receipt_json: String = row.get(0);
    let receipt: Receipt =
        serde_json::from_str(&receipt_json).map_err(|error| CdfError::data(error.to_string()))?;
    let rows_written = load_count(row.get(8), "load rows_written")?;
    let rows_inserted = row
        .get::<_, Option<i64>>(9)
        .map(|value| load_count(value, "load rows_inserted"))
        .transpose()?;
    let rows_updated = row
        .get::<_, Option<i64>>(10)
        .map(|value| load_count(value, "load rows_updated"))
        .transpose()?;
    let rows_deleted = row
        .get::<_, Option<i64>>(11)
        .map(|value| load_count(value, "load rows_deleted"))
        .transpose()?;
    let segment_count = load_count(row.get(12), "load segment_count")?;
    let migrations_json: String = row.get(13);
    let migrations: Vec<MigrationRecord> = serde_json::from_str(&migrations_json)
        .map_err(|error| CdfError::data(error.to_string()))?;
    if receipt.receipt_id.as_str() != row.get::<_, String>(1)
        || receipt.destination.as_str() != row.get::<_, String>(2)
        || receipt.target.as_str() != row.get::<_, String>(3)
        || receipt.package_hash.as_str() != row.get::<_, String>(4)
        || receipt.idempotency_token.as_str() != row.get::<_, String>(5)
        || disposition_name(&receipt.disposition) != row.get::<_, String>(6)
        || receipt.schema_hash.as_str() != row.get::<_, String>(7)
        || indexed_counts(&receipt.counts)
            != (rows_written, rows_inserted, rows_updated, rows_deleted)
        || receipt.segment_acks.len() as u64 != segment_count
        || receipt.migrations != migrations
        || receipt.committed_at_ms != row.get::<_, i64>(14)
    {
        return Err(CdfError::data(
            "Postgres receipt JSON differs from independently stored load evidence",
        ));
    }
    Ok(LoadMirrorRow { receipt })
}

pub(crate) fn indexed_counts(
    counts: &CommitCounts,
) -> (u64, Option<u64>, Option<u64>, Option<u64>) {
    match counts {
        CommitCounts::Rows {
            rows_written,
            rows_inserted,
            rows_updated,
            rows_deleted,
        } => (*rows_written, *rows_inserted, *rows_updated, *rows_deleted),
        CommitCounts::KeyedChanges {
            intent,
            rows_inserted,
            rows_updated,
            hard_deletes,
            soft_deletes,
            ..
        } => (
            intent.upserts,
            *rows_inserted,
            *rows_updated,
            (*hard_deletes).or(*soft_deletes),
        ),
    }
}

fn load_count(value: i64, name: &str) -> Result<u64> {
    u64::try_from(value).map_err(|_| CdfError::data(format!("{name} is negative")))
}

pub(crate) fn mirror_statements(
    input: &PostgresLoadPlanInput,
    verify: &VerifyClause,
) -> Vec<PostgresStatement> {
    let mut statements = vec![PostgresStatement::execute(
        "record_cdf_load",
        record_load_sql(),
    )];
    if input.state_delta.is_some() {
        statements.push(PostgresStatement::execute(
            "upsert_cdf_state",
            state_mirror_sql(),
        ));
    }
    statements.push(PostgresStatement::execute(
        "record_cdf_quarantine",
        record_quarantine_sql(),
    ));
    statements.push(PostgresStatement::query(
        "verify_receipt",
        verify.statement.clone(),
        StatementExpectation::ReturnsVerifyRow,
    ));
    statements
}

pub(crate) fn record_load_sql() -> String {
    format!(
        "INSERT INTO {} (\"receipt_id\", \"destination\", \"target\", \"resource_id\", \"package_hash\", \"idempotency_token\", \"disposition\", \"schema_hash\", \"rows_written\", \"rows_inserted\", \"rows_updated\", \"rows_deleted\", \"segment_count\", \"migrations_json\", \"receipt_json\", \"xid\", \"duplicate\", \"committed_at_ms\")\nVALUES ($1, 'postgres', $2, $4, $3, $5, $6, $7, $8, $9, $10, $11, $12, $13::text::jsonb, $14::text::jsonb, $15, $16, $17)\nON CONFLICT (\"target\", \"package_hash\") DO NOTHING\nRETURNING \"receipt_json\"::text",
        quote_identifier_unchecked(CDF_LOADS_TABLE)
    )
}

pub(crate) fn record_quarantine_sql() -> String {
    format!(
        "INSERT INTO {} (\"target\", \"package_hash\", \"receipt_id\", \"source_row_ordinal\", \"rule_id\", \"error_code\", \"source_position_json\", \"observed_value_json\", \"committed_at_ms\")\nVALUES ($1, $2, $3, $4, $5, $6, $7::text::jsonb, $8::text::jsonb, $9)\nON CONFLICT (\"target\", \"package_hash\", \"source_row_ordinal\", \"rule_id\", \"error_code\") DO NOTHING\nRETURNING \"receipt_id\", \"source_position_json\"::text, \"observed_value_json\"::text, \"committed_at_ms\"",
        quote_identifier_unchecked(CDF_QUARANTINE_TABLE)
    )
}

pub(crate) fn state_mirror_sql() -> String {
    format!(
        "INSERT INTO {} AS \"current\" (\"pipeline_id\", \"resource_id\", \"scope\", \"state_version\", \"checkpoint_id\", \"parent_checkpoint_id\", \"package_hash\", \"schema_hash\", \"output_position_json\", \"receipt_id\", \"committed_at_ms\")\nVALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::text::jsonb, $10, $11)\nON CONFLICT (\"pipeline_id\", \"resource_id\", \"scope\") DO UPDATE SET\n  \"state_version\" = EXCLUDED.\"state_version\",\n  \"checkpoint_id\" = EXCLUDED.\"checkpoint_id\",\n  \"parent_checkpoint_id\" = EXCLUDED.\"parent_checkpoint_id\",\n  \"package_hash\" = EXCLUDED.\"package_hash\",\n  \"schema_hash\" = EXCLUDED.\"schema_hash\",\n  \"output_position_json\" = EXCLUDED.\"output_position_json\",\n  \"receipt_id\" = EXCLUDED.\"receipt_id\",\n  \"committed_at_ms\" = EXCLUDED.\"committed_at_ms\"\nWHERE (\n  \"current\".\"checkpoint_id\" = EXCLUDED.\"checkpoint_id\"\n  AND \"current\".\"parent_checkpoint_id\" IS NOT DISTINCT FROM EXCLUDED.\"parent_checkpoint_id\"\n  AND \"current\".\"state_version\" = EXCLUDED.\"state_version\"\n  AND \"current\".\"package_hash\" = EXCLUDED.\"package_hash\"\n  AND \"current\".\"schema_hash\" = EXCLUDED.\"schema_hash\"\n  AND \"current\".\"output_position_json\" = EXCLUDED.\"output_position_json\"\n  AND \"current\".\"receipt_id\" = EXCLUDED.\"receipt_id\"\n  AND \"current\".\"committed_at_ms\" = EXCLUDED.\"committed_at_ms\"\n) OR \"current\".\"checkpoint_id\" = EXCLUDED.\"parent_checkpoint_id\"\nRETURNING \"state_version\", \"checkpoint_id\", \"parent_checkpoint_id\", \"package_hash\", \"schema_hash\", \"output_position_json\"::text, \"receipt_id\", \"committed_at_ms\"",
        quote_identifier_unchecked(CDF_STATE_TABLE)
    )
}

pub(crate) fn verify_clause(
    target: &TargetName,
    target_schema: Option<&PostgresIdentifier>,
    package_hash: &PackageHash,
    idempotency_token: &IdempotencyToken,
    schema_hash: &SchemaHash,
) -> VerifyClause {
    let mut parameters = BTreeMap::from([
        ("target".to_owned(), target.as_str().to_owned()),
        ("package_hash".to_owned(), package_hash.as_str().to_owned()),
        (
            "idempotency_token".to_owned(),
            idempotency_token.as_str().to_owned(),
        ),
        ("schema_hash".to_owned(), schema_hash.as_str().to_owned()),
    ]);
    parameters.insert("destination".to_owned(), POSTGRES_DESTINATION_ID.to_owned());
    if let Some(schema) = target_schema {
        parameters.insert("target_schema".to_owned(), schema.as_str().to_owned());
    }

    VerifyClause {
        kind: "postgres_sql".to_owned(),
        statement: format!(
            "SELECT \"receipt_id\", \"xid\", \"rows_written\", \"schema_hash\", \"receipt_json\"::text AS \"receipt_json\" FROM {} WHERE \"destination\" = 'postgres' AND \"target\" = $1 AND \"package_hash\" = $2 AND \"idempotency_token\" = $3 AND \"schema_hash\" = $4",
            quote_identifier_unchecked(CDF_LOADS_TABLE)
        ),
        parameters,
    }
}

pub(crate) fn drift_hooks() -> PostgresDriftHooks {
    PostgresDriftHooks {
        intents: vec![
            cdf_dest_sql::MirrorReadIntent::LoadForPackage,
            cdf_dest_sql::MirrorReadIntent::StateForScope,
            cdf_dest_sql::MirrorReadIntent::LoadsForTarget,
            cdf_dest_sql::MirrorReadIntent::StateHeads,
        ],
        load_for_package: PostgresStatement::query(
            "doctor_load_for_package",
            format!(
                "SELECT \"receipt_id\", \"schema_hash\", \"rows_written\", \"xid\", \"committed_at_ms\" FROM {} WHERE \"target\" = $1 AND \"package_hash\" = $2",
                quote_identifier_unchecked(CDF_LOADS_TABLE)
            ),
            StatementExpectation::ReturnsMirrorRows,
        ),
        state_for_scope: PostgresStatement::query(
            "doctor_state_for_scope",
            format!(
                "SELECT \"checkpoint_id\", \"package_hash\", \"schema_hash\", \"receipt_id\", \"committed_at_ms\" FROM {} WHERE \"pipeline_id\" = $1 AND \"resource_id\" = $2 AND \"scope\" = $3",
                quote_identifier_unchecked(CDF_STATE_TABLE)
            ),
            StatementExpectation::ReturnsMirrorRows,
        ),
        loads_for_target: PostgresStatement::query(
            "doctor_loads_for_target",
            format!(
                "SELECT \"target\", \"package_hash\", \"schema_hash\", \"receipt_id\", \"committed_at_ms\" FROM {} WHERE \"target\" = $1 ORDER BY \"committed_at_ms\"",
                quote_identifier_unchecked(CDF_LOADS_TABLE)
            ),
            StatementExpectation::ReturnsMirrorRows,
        ),
        state_heads: PostgresStatement::query(
            "doctor_state_heads",
            format!(
                "SELECT \"pipeline_id\", \"resource_id\", \"scope\", \"checkpoint_id\", \"package_hash\", \"schema_hash\", \"receipt_id\" FROM {} ORDER BY \"pipeline_id\", \"resource_id\", \"scope\"",
                quote_identifier_unchecked(CDF_STATE_TABLE)
            ),
            StatementExpectation::ReturnsMirrorRows,
        ),
    }
}

pub(crate) fn add_segments_to_verify_parameters(
    verify: &mut VerifyClause,
    segments: &[StateSegment],
) {
    for segment in segments {
        verify.parameters.insert(
            format!("segment.{}", segment.segment_id.as_str()),
            format!("{}:{}", segment.row_count, segment.byte_count),
        );
    }
}
