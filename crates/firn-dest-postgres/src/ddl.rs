use crate::*;

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
            "create_firn_loads",
            format!(
                "CREATE TABLE IF NOT EXISTS {} (\n  \"receipt_id\" TEXT PRIMARY KEY,\n  \"destination\" TEXT NOT NULL,\n  \"target\" TEXT NOT NULL,\n  \"resource_id\" TEXT,\n  \"package_hash\" TEXT NOT NULL,\n  \"idempotency_token\" TEXT NOT NULL,\n  \"disposition\" TEXT NOT NULL,\n  \"schema_hash\" TEXT NOT NULL,\n  \"rows_written\" BIGINT NOT NULL,\n  \"rows_inserted\" BIGINT,\n  \"rows_updated\" BIGINT,\n  \"rows_deleted\" BIGINT,\n  \"segment_count\" BIGINT NOT NULL,\n  \"migrations_json\" JSONB NOT NULL,\n  \"receipt_json\" JSONB NOT NULL,\n  \"xid\" TEXT NOT NULL,\n  \"duplicate\" BOOLEAN NOT NULL DEFAULT FALSE,\n  \"committed_at_ms\" BIGINT NOT NULL,\n  UNIQUE (\"target\", \"package_hash\")\n)",
                quote_identifier_unchecked(FIRN_LOADS_TABLE)
            ),
        ),
        PostgresStatement::execute(
            "create_firn_state",
            format!(
                "CREATE TABLE IF NOT EXISTS {} (\n  \"pipeline_id\" TEXT NOT NULL,\n  \"resource_id\" TEXT NOT NULL,\n  \"scope\" TEXT NOT NULL,\n  \"state_version\" INTEGER NOT NULL,\n  \"checkpoint_id\" TEXT NOT NULL,\n  \"package_hash\" TEXT NOT NULL,\n  \"schema_hash\" TEXT NOT NULL,\n  \"output_position_json\" JSONB NOT NULL,\n  \"receipt_id\" TEXT NOT NULL,\n  \"committed_at_ms\" BIGINT NOT NULL,\n  PRIMARY KEY (\"pipeline_id\", \"resource_id\", \"scope\")\n)",
                quote_identifier_unchecked(FIRN_STATE_TABLE)
            ),
        ),
    ]
}

pub(crate) fn target_migrations(input: &PostgresLoadPlanInput) -> Result<Vec<PostgresStatement>> {
    match &input.existing_table {
        None => Ok(vec![PostgresStatement::execute(
            "create_target",
            create_target_table_sql(&input.target, &input.columns, primary_key_for_create(input)),
        )]),
        Some(existing) => {
            let mut migrations = Vec::new();
            for column in &input.columns {
                match existing.columns.get(column.name.as_str()) {
                    Some(existing_column)
                        if existing_column
                            .data_type
                            .eq_ignore_ascii_case(&column.data_type) => {}
                    Some(existing_column) => {
                        return Err(FirnError::destination(format!(
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
                                input.target.sql(),
                                column.definition_sql()
                            ),
                        ));
                    }
                    None => {
                        return Err(FirnError::destination(format!(
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
                            input.target.sql(),
                            system_column.definition_sql_with_nullability(true)
                        ),
                    ));
                }
            }
            Ok(migrations)
        }
    }
}

pub(crate) fn primary_key_for_create(input: &PostgresLoadPlanInput) -> &[PostgresIdentifier] {
    if input.disposition == WriteDisposition::Merge {
        &input.merge_keys
    } else {
        &[]
    }
}

pub(crate) fn create_target_table_sql(
    target: &PostgresTarget,
    columns: &[PostgresColumn],
    primary_key: &[PostgresIdentifier],
) -> String {
    let mut definitions = columns
        .iter()
        .map(PostgresColumn::definition_sql)
        .collect::<Vec<_>>();
    definitions.extend(
        system_target_columns()
            .into_iter()
            .map(|column| column.definition_sql()),
    );
    if !primary_key.is_empty() {
        definitions.push(format!(
            "PRIMARY KEY ({})",
            primary_key
                .iter()
                .map(PostgresIdentifier::quoted)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    format!(
        "CREATE TABLE IF NOT EXISTS {} (\n  {}\n)",
        target.sql(),
        definitions.join(",\n  ")
    )
}

pub(crate) fn system_target_columns() -> Vec<PostgresColumn> {
    vec![
        PostgresColumn {
            name: PostgresIdentifier::system(FIRN_LOAD_COLUMN).expect("static identifier"),
            data_type: "TEXT".to_owned(),
            nullable: false,
        },
        PostgresColumn {
            name: PostgresIdentifier::system(FIRN_SEGMENT_COLUMN).expect("static identifier"),
            data_type: "TEXT".to_owned(),
            nullable: false,
        },
        PostgresColumn {
            name: PostgresIdentifier::system(FIRN_ROW_COLUMN).expect("static identifier"),
            data_type: "BIGINT".to_owned(),
            nullable: false,
        },
        PostgresColumn {
            name: PostgresIdentifier::system(FIRN_LOADED_AT_COLUMN).expect("static identifier"),
            data_type: "BIGINT".to_owned(),
            nullable: false,
        },
    ]
}

pub(crate) fn idempotency_check_statement() -> PostgresStatement {
    PostgresStatement::query(
        "check_duplicate_package",
        format!(
            "SELECT \"receipt_json\"::text AS \"receipt_json\" FROM {} WHERE \"target\" = $1 AND \"package_hash\" = $2",
            quote_identifier_unchecked(FIRN_LOADS_TABLE)
        ),
        StatementExpectation::ReturnsDuplicateReceiptIfPresent,
    )
}
