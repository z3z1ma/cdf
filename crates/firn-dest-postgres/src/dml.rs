use crate::{ddl::system_target_columns, *};

pub(crate) fn write_statements(
    input: &PostgresLoadPlanInput,
    stage_table: &PostgresIdentifier,
) -> Result<Vec<PostgresStatement>> {
    let mut statements = vec![PostgresStatement::execute(
        "create_stage",
        create_stage_sql(stage_table, &input.columns),
    )];

    match input.disposition {
        WriteDisposition::Append => statements.push(PostgresStatement::execute(
            "append_from_stage",
            append_insert_sql(&input.target, &input.columns, stage_table),
        )),
        WriteDisposition::Replace => {
            statements.push(PostgresStatement::execute(
                "truncate_target_for_replace",
                format!("TRUNCATE TABLE {}", input.target.sql()),
            ));
            statements.push(PostgresStatement::execute(
                "replace_from_stage",
                append_insert_sql(&input.target, &input.columns, stage_table),
            ));
        }
        WriteDisposition::Merge => {
            if input.dedup == MergeDedupPolicy::Fail {
                statements.push(PostgresStatement::query(
                    "merge_duplicate_key_guard",
                    duplicate_key_guard_sql(stage_table, &input.merge_keys),
                    StatementExpectation::ReturnsZeroRows,
                ));
            }
            statements.push(PostgresStatement::execute(
                "merge_from_stage",
                merge_sql(input, stage_table)?,
            ));
        }
        WriteDisposition::CdcApply => unreachable!("validated before write planning"),
    }

    Ok(statements)
}

pub(crate) fn create_stage_sql(
    stage_table: &PostgresIdentifier,
    columns: &[PostgresColumn],
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

    format!(
        "CREATE TEMP TABLE {} (\n  {}\n) ON COMMIT DROP",
        stage_table.quoted(),
        definitions.join(",\n  ")
    )
}

pub(crate) fn append_insert_sql(
    target: &PostgresTarget,
    columns: &[PostgresColumn],
    stage_table: &PostgresIdentifier,
) -> String {
    let mut target_columns = quoted_column_names(columns);
    target_columns.extend(quoted_system_target_column_names());

    let mut selected_columns = quoted_column_names(columns);
    selected_columns.extend(quoted_system_target_column_names());

    format!(
        "INSERT INTO {} ({})\nSELECT {} FROM {}",
        target.sql(),
        target_columns.join(", "),
        selected_columns.join(", "),
        stage_table.quoted()
    )
}

pub(crate) fn merge_sql(
    input: &PostgresLoadPlanInput,
    stage_table: &PostgresIdentifier,
) -> Result<String> {
    let mut target_columns = quoted_column_names(&input.columns);
    target_columns.extend(quoted_system_target_column_names());

    let mut selected_columns = quoted_column_names(&input.columns);
    selected_columns.extend(quoted_system_target_column_names());

    let conflict_columns = input
        .merge_keys
        .iter()
        .map(PostgresIdentifier::quoted)
        .collect::<Vec<_>>()
        .join(", ");
    let assignments = merge_assignments(&input.columns, &input.merge_keys).join(", ");

    let source = match input.dedup {
        MergeDedupPolicy::First | MergeDedupPolicy::Last => format!(
            "WITH \"_firn_ranked\" AS (\n  SELECT {}, ROW_NUMBER() OVER (PARTITION BY {} ORDER BY {}, {}) AS \"_firn_rank\"\n  FROM {}\n), \"_firn_dedup\" AS (\n  SELECT * FROM \"_firn_ranked\" WHERE \"_firn_rank\" = 1\n)\n",
            stage_select_list(&input.columns),
            conflict_columns,
            order_expression(FIRN_SEGMENT_COLUMN, &input.dedup),
            order_expression(FIRN_ROW_COLUMN, &input.dedup),
            stage_table.quoted()
        ),
        MergeDedupPolicy::Fail => String::new(),
    };

    let source_table = match input.dedup {
        MergeDedupPolicy::First | MergeDedupPolicy::Last => "\"_firn_dedup\"".to_owned(),
        MergeDedupPolicy::Fail => stage_table.quoted(),
    };

    Ok(format!(
        "{source}INSERT INTO {} ({})\nSELECT {} FROM {}\nON CONFLICT ({}) DO UPDATE SET {}",
        input.target.sql(),
        target_columns.join(", "),
        selected_columns.join(", "),
        source_table,
        conflict_columns,
        assignments
    ))
}

pub(crate) fn stage_select_list(columns: &[PostgresColumn]) -> String {
    let mut selected = quoted_column_names(columns);
    selected.extend(quoted_system_target_column_names());
    selected.join(", ")
}

pub(crate) fn order_expression(column: &str, policy: &MergeDedupPolicy) -> String {
    let direction = match policy {
        MergeDedupPolicy::First => "ASC",
        MergeDedupPolicy::Last => "DESC",
        MergeDedupPolicy::Fail => "ASC",
    };
    format!("{} {}", quote_identifier_unchecked(column), direction)
}

pub(crate) fn duplicate_key_guard_sql(
    stage_table: &PostgresIdentifier,
    merge_keys: &[PostgresIdentifier],
) -> String {
    let keys = merge_keys
        .iter()
        .map(PostgresIdentifier::quoted)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT {keys}, COUNT(*) AS \"firn_duplicate_count\" FROM {} GROUP BY {keys} HAVING COUNT(*) > 1",
        stage_table.quoted()
    )
}

pub(crate) fn merge_assignments(
    columns: &[PostgresColumn],
    merge_keys: &[PostgresIdentifier],
) -> Vec<String> {
    let key_names = merge_keys
        .iter()
        .map(PostgresIdentifier::as_str)
        .collect::<BTreeSet<_>>();
    let mut assignments = columns
        .iter()
        .filter(|column| !key_names.contains(column.name.as_str()))
        .map(|column| {
            format!(
                "{} = EXCLUDED.{}",
                column.name.quoted(),
                column.name.quoted()
            )
        })
        .collect::<Vec<_>>();
    assignments.push(format!(
        "{} = EXCLUDED.{}",
        quote_identifier_unchecked(FIRN_LOAD_COLUMN),
        quote_identifier_unchecked(FIRN_LOAD_COLUMN)
    ));
    assignments.push(format!(
        "{} = EXCLUDED.{}",
        quote_identifier_unchecked(FIRN_SEGMENT_COLUMN),
        quote_identifier_unchecked(FIRN_SEGMENT_COLUMN)
    ));
    assignments.push(format!(
        "{} = EXCLUDED.{}",
        quote_identifier_unchecked(FIRN_ROW_COLUMN),
        quote_identifier_unchecked(FIRN_ROW_COLUMN)
    ));
    assignments.push(format!(
        "{} = EXCLUDED.{}",
        quote_identifier_unchecked(FIRN_LOADED_AT_COLUMN),
        quote_identifier_unchecked(FIRN_LOADED_AT_COLUMN)
    ));
    assignments
}

pub(crate) fn quoted_column_names(columns: &[PostgresColumn]) -> Vec<String> {
    columns.iter().map(|column| column.name.quoted()).collect()
}

pub(crate) fn quoted_system_target_column_names() -> Vec<String> {
    [
        FIRN_LOAD_COLUMN,
        FIRN_SEGMENT_COLUMN,
        FIRN_ROW_COLUMN,
        FIRN_LOADED_AT_COLUMN,
    ]
    .into_iter()
    .map(quote_identifier_unchecked)
    .collect()
}
