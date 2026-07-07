use crate::*;

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
    statements.push(PostgresStatement::query(
        "verify_receipt",
        verify.statement.clone(),
        StatementExpectation::ReturnsVerifyRow,
    ));
    statements
}

pub(crate) fn record_load_sql() -> String {
    format!(
        "INSERT INTO {} (\"receipt_id\", \"destination\", \"target\", \"resource_id\", \"package_hash\", \"idempotency_token\", \"disposition\", \"schema_hash\", \"rows_written\", \"rows_inserted\", \"rows_updated\", \"rows_deleted\", \"segment_count\", \"migrations_json\", \"receipt_json\", \"xid\", \"duplicate\", \"committed_at_ms\")\nVALUES ($1, 'postgres', $2, $4, $3, $5, $6, $7, $8, $9, $10, $11, $12, $13::text::jsonb, $14::text::jsonb, $15, $16, $17)\nON CONFLICT (\"target\", \"package_hash\") DO NOTHING",
        quote_identifier_unchecked(CDF_LOADS_TABLE)
    )
}

pub(crate) fn state_mirror_sql() -> String {
    format!(
        "INSERT INTO {} (\"pipeline_id\", \"resource_id\", \"scope\", \"state_version\", \"checkpoint_id\", \"package_hash\", \"schema_hash\", \"output_position_json\", \"receipt_id\", \"committed_at_ms\")\nVALUES ($1, $2, $3, $4, $5, $6, $7, $8::text::jsonb, $9, $10)\nON CONFLICT (\"pipeline_id\", \"resource_id\", \"scope\") DO UPDATE SET\n  \"state_version\" = EXCLUDED.\"state_version\",\n  \"checkpoint_id\" = EXCLUDED.\"checkpoint_id\",\n  \"package_hash\" = EXCLUDED.\"package_hash\",\n  \"schema_hash\" = EXCLUDED.\"schema_hash\",\n  \"output_position_json\" = EXCLUDED.\"output_position_json\",\n  \"receipt_id\" = EXCLUDED.\"receipt_id\",\n  \"committed_at_ms\" = EXCLUDED.\"committed_at_ms\"\nWHERE {}.\"committed_at_ms\" <= EXCLUDED.\"committed_at_ms\"",
        quote_identifier_unchecked(CDF_STATE_TABLE),
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
