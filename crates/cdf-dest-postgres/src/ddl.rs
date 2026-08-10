use cdf_kernel::{CdfError, MigrationRecord, Result, WriteDisposition};
use cdf_postgres::{PostgresIdentifier, PostgresTarget};
use sha2::{Digest, Sha256};

use crate::{
    CDF_LOADED_AT_COLUMN, CDF_LOADS_TABLE, CDF_QUARANTINE_TABLE, CDF_ROW_KEY_ALLOCATOR_TABLE,
    CDF_ROW_KEY_COLUMN, CDF_SEGMENTS_TABLE, CDF_STATE_TABLE,
    identifiers::{
        PostgresColumn, quote_identifier_unchecked, quote_system_identifier, quote_user_identifier,
        validated_column_definition, validated_system_column_definition, validated_target_sql,
        validated_user_column_definition,
    },
    plan::{PostgresLoadPlanInput, PostgresStatement, StatementExpectation},
};

pub(crate) fn system_table_migrations() -> Vec<MigrationRecord> {
    system_table_ddl()
        .into_iter()
        .map(|statement| MigrationRecord {
            migration_id: format!("postgres.{}", statement.name),
            description: statement.sql,
        })
        .collect()
}

pub(crate) fn system_table_ddl() -> Vec<PostgresStatement> {
    vec![
        PostgresStatement::execute(
            "create_cdf_loads",
            format!(
                "CREATE TABLE IF NOT EXISTS {} (\n  \"receipt_id\" TEXT PRIMARY KEY,\n  \"destination\" TEXT NOT NULL,\n  \"target\" TEXT NOT NULL,\n  \"resource_id\" TEXT,\n  \"package_hash\" TEXT NOT NULL,\n  \"idempotency_token\" TEXT NOT NULL,\n  \"disposition\" TEXT NOT NULL,\n  \"schema_hash\" TEXT NOT NULL,\n  \"rows_written\" BIGINT NOT NULL,\n  \"rows_inserted\" BIGINT,\n  \"rows_updated\" BIGINT,\n  \"rows_deleted\" BIGINT,\n  \"segment_count\" BIGINT NOT NULL,\n  \"migrations_json\" JSONB NOT NULL,\n  \"receipt_json\" JSONB NOT NULL,\n  \"xid\" TEXT NOT NULL,\n  \"duplicate\" BOOLEAN NOT NULL DEFAULT FALSE,\n  \"committed_at_ms\" BIGINT NOT NULL,\n  UNIQUE (\"target\", \"package_hash\")\n)",
                quote_identifier_unchecked(CDF_LOADS_TABLE)
            ),
        ),
        PostgresStatement::execute(
            "create_cdf_state",
            format!(
                "CREATE TABLE IF NOT EXISTS {} (\n  \"pipeline_id\" TEXT NOT NULL,\n  \"resource_id\" TEXT NOT NULL,\n  \"scope\" TEXT NOT NULL,\n  \"state_version\" INTEGER NOT NULL,\n  \"checkpoint_id\" TEXT NOT NULL,\n  \"parent_checkpoint_id\" TEXT,\n  \"package_hash\" TEXT NOT NULL,\n  \"schema_hash\" TEXT NOT NULL,\n  \"output_position_json\" JSONB NOT NULL,\n  \"receipt_id\" TEXT NOT NULL,\n  \"committed_at_ms\" BIGINT NOT NULL,\n  PRIMARY KEY (\"pipeline_id\", \"resource_id\", \"scope\")\n)",
                quote_identifier_unchecked(CDF_STATE_TABLE)
            ),
        ),
        PostgresStatement::execute(
            "upgrade_cdf_state_lineage",
            format!(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS \"parent_checkpoint_id\" TEXT",
                quote_identifier_unchecked(CDF_STATE_TABLE)
            ),
        ),
        PostgresStatement::execute(
            "create_cdf_quarantine",
            format!(
                "CREATE TABLE IF NOT EXISTS {} (\n  \"target\" TEXT NOT NULL,\n  \"package_hash\" TEXT NOT NULL,\n  \"receipt_id\" TEXT NOT NULL,\n  \"source_row_ordinal\" BIGINT NOT NULL,\n  \"rule_id\" TEXT NOT NULL,\n  \"error_code\" TEXT NOT NULL,\n  \"source_position_json\" JSONB,\n  \"observed_value_json\" JSONB NOT NULL,\n  \"committed_at_ms\" BIGINT NOT NULL,\n  PRIMARY KEY (\"target\", \"package_hash\", \"source_row_ordinal\", \"rule_id\", \"error_code\")\n)",
                quote_identifier_unchecked(CDF_QUARANTINE_TABLE)
            ),
        ),
        PostgresStatement::execute(
            "create_cdf_row_key_allocator",
            format!(
                "CREATE TABLE IF NOT EXISTS {} (\n  \"singleton\" BOOLEAN PRIMARY KEY CHECK (\"singleton\"),\n  \"next_key\" BIGINT NOT NULL CHECK (\"next_key\" > 0)\n);\nINSERT INTO {} (\"singleton\", \"next_key\") VALUES (TRUE, 1) ON CONFLICT (\"singleton\") DO NOTHING",
                quote_identifier_unchecked(CDF_ROW_KEY_ALLOCATOR_TABLE),
                quote_identifier_unchecked(CDF_ROW_KEY_ALLOCATOR_TABLE)
            ),
        ),
        PostgresStatement::execute(
            "create_cdf_segments",
            format!(
                "CREATE TABLE IF NOT EXISTS {} (\n  \"row_key_start\" BIGINT PRIMARY KEY,\n  \"row_key_end\" BIGINT NOT NULL,\n  \"target\" TEXT NOT NULL,\n  \"package_hash\" TEXT NOT NULL,\n  \"idempotency_token\" TEXT NOT NULL,\n  \"segment_id\" TEXT NOT NULL,\n  \"scope_json\" JSONB,\n  \"output_position_json\" JSONB,\n  \"row_count\" BIGINT NOT NULL,\n  \"byte_count\" BIGINT NOT NULL,\n  \"committed_at_ms\" BIGINT NOT NULL,\n  CHECK (\"row_key_start\" < \"row_key_end\"),\n  UNIQUE (\"target\", \"package_hash\", \"segment_id\")\n)",
                quote_identifier_unchecked(CDF_SEGMENTS_TABLE)
            ),
        ),
        PostgresStatement::execute(
            "upgrade_cdf_segments_readback",
            format!(
                "ALTER TABLE {} \
                 ADD COLUMN IF NOT EXISTS \"idempotency_token\" TEXT, \
                 ADD COLUMN IF NOT EXISTS \"scope_json\" JSONB, \
                 ADD COLUMN IF NOT EXISTS \"output_position_json\" JSONB, \
                 ADD COLUMN IF NOT EXISTS \"row_count\" BIGINT, \
                 ADD COLUMN IF NOT EXISTS \"byte_count\" BIGINT, \
                 ADD COLUMN IF NOT EXISTS \"committed_at_ms\" BIGINT",
                quote_identifier_unchecked(CDF_SEGMENTS_TABLE)
            ),
        ),
    ]
}

pub(crate) fn target_migrations(input: &PostgresLoadPlanInput) -> Result<Vec<PostgresStatement>> {
    let mut migrations = match &input.existing_table {
        None => vec![PostgresStatement::execute(
            "create_target",
            create_target_table_sql(&input.target, &input.columns, primary_key_for_create(input))?,
        )],
        Some(existing) => {
            let mut migrations = Vec::new();
            for column in &input.columns {
                match existing.columns.get(column.name.as_str()) {
                    Some(existing_column)
                        if existing_column
                            .data_type
                            .eq_ignore_ascii_case(&column.data_type) => {}
                    Some(existing_column) => {
                        return Err(CdfError::destination(format!(
                            "Postgres column {} exists as {} but plan requires {}",
                            column.name.as_str(),
                            existing_column.data_type,
                            column.data_type
                        )));
                    }
                    None if column.nullable => {
                        migrations.push(PostgresStatement::execute(
                            format!("add_column_{}", column.name.as_str()),
                            format!(
                                "ALTER TABLE {} ADD COLUMN {}",
                                validated_target_sql(&input.target)?,
                                validated_user_column_definition(column)?
                            ),
                        ));
                    }
                    None => {
                        return Err(CdfError::destination(format!(
                            "Postgres cannot dry-plan ADD COLUMN {} NOT NULL without a default",
                            column.name.as_str()
                        )));
                    }
                }
            }
            for system_column in system_target_columns() {
                if !existing.columns.contains_key(system_column.name.as_str()) {
                    migrations.push(PostgresStatement::execute(
                        format!("add_column_{}", system_column.name.as_str()),
                        format!(
                            "ALTER TABLE {} ADD COLUMN {}",
                            validated_target_sql(&input.target)?,
                            validated_column_definition(&system_column, true, true)?
                        ),
                    ));
                }
            }
            migrations
        }
    };
    if let cdf_kernel::PackageContentAuthority::KeyedChanges {
        delete_application:
            cdf_kernel::DeleteApplicationAuthority::Apply {
                policy: cdf_kernel::DeleteApplicationPolicy::Soft { marker_field },
            },
        ..
    } = &input.content
        && input.disposition == WriteDisposition::CdcApply
    {
        let existing_marker = input
            .existing_table
            .as_ref()
            .and_then(|table| table.columns.get(marker_field));
        match existing_marker {
            Some(marker)
                if marker.data_type.eq_ignore_ascii_case("BOOLEAN") && !marker.nullable => {}
            Some(marker) => {
                return Err(CdfError::destination(format!(
                    "existing Postgres soft-delete marker {marker_field} is {}{}; it must be BOOLEAN NOT NULL",
                    marker.data_type,
                    if marker.nullable {
                        " NULL"
                    } else {
                        " NOT NULL"
                    }
                )));
            }
            None => migrations.push(soft_marker_migration(&input.target, marker_field)?),
        }
    }
    Ok(migrations)
}

pub(crate) fn soft_marker_migration(
    target: &PostgresTarget,
    marker: &str,
) -> Result<PostgresStatement> {
    let marker = quote_user_identifier(&PostgresIdentifier::user(marker)?)?;
    Ok(PostgresStatement::execute(
        "ensure_soft_delete_marker",
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {marker} BOOLEAN NOT NULL DEFAULT FALSE",
            validated_target_sql(target)?
        ),
    ))
}

pub(crate) fn primary_key_for_create(input: &PostgresLoadPlanInput) -> &[PostgresIdentifier] {
    if matches!(
        input.disposition,
        WriteDisposition::Merge | WriteDisposition::CdcApply
    ) {
        &input.merge_keys
    } else {
        &[]
    }
}

pub(crate) fn create_target_table_sql(
    target: &PostgresTarget,
    columns: &[PostgresColumn],
    primary_key: &[PostgresIdentifier],
) -> Result<String> {
    let mut definitions = columns
        .iter()
        .map(validated_user_column_definition)
        .collect::<Result<Vec<_>>>()?;
    definitions.extend(
        system_target_columns()
            .into_iter()
            .map(|column| validated_system_column_definition(&column))
            .collect::<Result<Vec<_>>>()?,
    );
    if !primary_key.is_empty() {
        definitions.push(format!(
            "PRIMARY KEY ({})",
            primary_key
                .iter()
                .map(quote_user_identifier)
                .collect::<Result<Vec<_>>>()?
                .join(", ")
        ));
    }
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {} (\n  {}\n)",
        validated_target_sql(target)?,
        definitions.join(",\n  ")
    ))
}

pub(crate) fn provenance_unique_index_statement(
    target: &PostgresTarget,
) -> Result<PostgresStatement> {
    let digest = hex::encode(Sha256::digest(target.display_name().as_bytes()));
    let name = PostgresIdentifier::system(format!("_cdf_provenance_{}_uniq", &digest[..24]))?;
    Ok(PostgresStatement::execute(
        "ensure_unique_cdf_provenance",
        format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({})",
            quote_system_identifier(&name)?,
            validated_target_sql(target)?,
            quote_identifier_unchecked(CDF_ROW_KEY_COLUMN)
        ),
    ))
}

pub(crate) fn system_target_columns() -> Vec<PostgresColumn> {
    vec![
        PostgresColumn {
            name: PostgresIdentifier::system(CDF_ROW_KEY_COLUMN).expect("static identifier"),
            data_type: "BIGINT".to_owned(),
            nullable: false,
            semantic: None,
        },
        PostgresColumn {
            name: PostgresIdentifier::system(CDF_LOADED_AT_COLUMN).expect("static identifier"),
            data_type: "BIGINT".to_owned(),
            nullable: false,
            semantic: None,
        },
    ]
}

pub(crate) fn idempotency_lock_statement() -> PostgresStatement {
    PostgresStatement::query(
        "lock_idempotency_key",
        "SELECT pg_advisory_xact_lock(hashtextextended(jsonb_build_array($1::text, $2::text)::text, 0))",
        StatementExpectation::ReturnsIdempotencyLock,
    )
}

pub(crate) fn idempotency_check_statement() -> PostgresStatement {
    PostgresStatement::query(
        "check_duplicate_package",
        format!(
            "SELECT \"receipt_json\"::text, \"receipt_id\", \"destination\", \"target\", \"package_hash\", \"idempotency_token\", \"disposition\", \"schema_hash\", \"rows_written\", \"rows_inserted\", \"rows_updated\", \"rows_deleted\", \"segment_count\", \"migrations_json\"::text, \"committed_at_ms\" FROM {} WHERE \"target\" = $1 AND \"package_hash\" = $2 AND \"idempotency_token\" = $3",
            quote_identifier_unchecked(CDF_LOADS_TABLE)
        ),
        StatementExpectation::ReturnsDuplicateReceiptIfPresent,
    )
}
