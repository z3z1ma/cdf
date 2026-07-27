use crate::{ddl::system_target_columns, *};

pub(crate) fn write_statements(
    input: &PostgresLoadPlanInput,
    stage_table: Option<&PostgresIdentifier>,
) -> Result<Vec<PostgresStatement>> {
    match input.disposition {
        WriteDisposition::Append => Ok(vec![PostgresStatement::copy_binary(
            "copy_target_binary",
            binary_copy_sql(&validated_target_sql(&input.target)?, &input.columns)?,
        )]),
        WriteDisposition::Replace => Ok(vec![
            PostgresStatement::execute(
                "truncate_target_for_replace",
                format!("TRUNCATE TABLE {}", validated_target_sql(&input.target)?),
            ),
            PostgresStatement::copy_binary(
                "copy_target_binary",
                binary_copy_sql(&validated_target_sql(&input.target)?, &input.columns)?,
            ),
        ]),
        WriteDisposition::Merge => {
            let stage_table = stage_table
                .ok_or_else(|| CdfError::internal("Postgres merge plan omits its stage table"))?;
            let mut statements = vec![
                PostgresStatement::execute(
                    "create_stage",
                    create_stage_sql(stage_table, &input.columns)?,
                ),
                PostgresStatement::copy_binary(
                    "copy_stage_binary",
                    binary_copy_sql(&quote_system_identifier(stage_table)?, &input.columns)?,
                ),
            ];
            if input.dedup == MergeDedupPolicy::Fail {
                statements.push(PostgresStatement::query(
                    "merge_duplicate_key_guard",
                    duplicate_key_guard_sql(stage_table, &input.merge_keys)?,
                    StatementExpectation::ReturnsZeroRows,
                ));
            }
            statements.push(PostgresStatement::execute(
                "merge_from_stage",
                merge_sql(input, stage_table)?,
            ));
            Ok(statements)
        }
        WriteDisposition::CdcApply => unreachable!("validated before write planning"),
    }
}

pub(crate) fn binary_copy_sql(destination: &str, columns: &[PostgresColumn]) -> Result<String> {
    let mut names = quoted_column_names(columns)?;
    names.extend(quoted_system_target_column_names());
    Ok(format!(
        "COPY {destination} ({}) FROM STDIN WITH (FORMAT binary)",
        names.join(", ")
    ))
}

pub(crate) fn create_stage_sql(
    stage_table: &PostgresIdentifier,
    columns: &[PostgresColumn],
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

    Ok(format!(
        "CREATE TEMP TABLE {} (\n  {}\n) ON COMMIT DROP",
        quote_system_identifier(stage_table)?,
        definitions.join(",\n  ")
    ))
}

pub(crate) fn merge_sql(
    input: &PostgresLoadPlanInput,
    stage_table: &PostgresIdentifier,
) -> Result<String> {
    let mut target_columns = quoted_column_names(&input.columns)?;
    target_columns.extend(quoted_system_target_column_names());

    let mut selected_columns = quoted_column_names(&input.columns)?;
    selected_columns.extend(quoted_system_target_column_names());

    let conflict_columns = input
        .merge_keys
        .iter()
        .map(quote_user_identifier)
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    let assignments = merge_assignments(&input.columns, &input.merge_keys)?.join(", ");

    let source = match input.dedup {
        MergeDedupPolicy::First | MergeDedupPolicy::Last => format!(
            "WITH \"_cdf_ranked\" AS (\n  SELECT {}, ROW_NUMBER() OVER (PARTITION BY {} ORDER BY {}, {}) AS \"_cdf_rank\"\n  FROM {}\n), \"_cdf_dedup\" AS (\n  SELECT * FROM \"_cdf_ranked\" WHERE \"_cdf_rank\" = 1\n)\n",
            stage_select_list(&input.columns)?,
            conflict_columns,
            order_expression(CDF_ROW_KEY_COLUMN, &input.dedup),
            order_expression(CDF_LOADED_AT_COLUMN, &input.dedup),
            quote_system_identifier(stage_table)?
        ),
        MergeDedupPolicy::Fail => String::new(),
    };

    let source_table = match input.dedup {
        MergeDedupPolicy::First | MergeDedupPolicy::Last => "\"_cdf_dedup\"".to_owned(),
        MergeDedupPolicy::Fail => quote_system_identifier(stage_table)?,
    };

    Ok(format!(
        "{source}INSERT INTO {} ({})\nSELECT {} FROM {}\nON CONFLICT ({}) DO UPDATE SET {}",
        validated_target_sql(&input.target)?,
        target_columns.join(", "),
        selected_columns.join(", "),
        source_table,
        conflict_columns,
        assignments
    ))
}

pub(crate) fn stage_select_list(columns: &[PostgresColumn]) -> Result<String> {
    let mut selected = quoted_column_names(columns)?;
    selected.extend(quoted_system_target_column_names());
    Ok(selected.join(", "))
}

pub(crate) fn order_expression(column: &'static str, policy: &MergeDedupPolicy) -> String {
    let direction = match policy {
        MergeDedupPolicy::First => "ASC",
        MergeDedupPolicy::Last => "DESC",
        MergeDedupPolicy::Fail => "ASC",
    };
    format!(
        "{} {}",
        quote_validated_identifier(
            &cdf_dest_sql::ValidatedSqlIdentifier::system(&postgres_identifier_rules(), column)
                .expect("framework column must satisfy Postgres sheet rules"),
        ),
        direction
    )
}

pub(crate) fn duplicate_key_guard_sql(
    stage_table: &PostgresIdentifier,
    merge_keys: &[PostgresIdentifier],
) -> Result<String> {
    let keys = merge_keys
        .iter()
        .map(quote_user_identifier)
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    Ok(format!(
        "SELECT {keys}, COUNT(*) AS \"cdf_duplicate_count\" FROM {} GROUP BY {keys} HAVING COUNT(*) > 1",
        quote_system_identifier(stage_table)?
    ))
}

pub(crate) fn merge_assignments(
    columns: &[PostgresColumn],
    merge_keys: &[PostgresIdentifier],
) -> Result<Vec<String>> {
    let key_names = merge_keys
        .iter()
        .map(PostgresIdentifier::as_str)
        .collect::<BTreeSet<_>>();
    let mut assignments = columns
        .iter()
        .filter(|column| !key_names.contains(column.name.as_str()))
        .map(|column| {
            let name = quote_column_identifier(&column.name)?;
            Ok(format!("{name} = EXCLUDED.{name}"))
        })
        .collect::<Result<Vec<_>>>()?;
    assignments.push(format!(
        "{} = EXCLUDED.{}",
        quote_identifier_unchecked(CDF_ROW_KEY_COLUMN),
        quote_identifier_unchecked(CDF_ROW_KEY_COLUMN)
    ));
    assignments.push(format!(
        "{} = EXCLUDED.{}",
        quote_identifier_unchecked(CDF_LOADED_AT_COLUMN),
        quote_identifier_unchecked(CDF_LOADED_AT_COLUMN)
    ));
    Ok(assignments)
}

pub(crate) fn quoted_column_names(columns: &[PostgresColumn]) -> Result<Vec<String>> {
    columns
        .iter()
        .map(|column| quote_column_identifier(&column.name))
        .collect()
}

pub(crate) fn quoted_system_target_column_names() -> Vec<String> {
    [CDF_ROW_KEY_COLUMN, CDF_LOADED_AT_COLUMN]
        .into_iter()
        .map(|name| {
            quote_validated_identifier(
                &cdf_dest_sql::ValidatedSqlIdentifier::system(&postgres_identifier_rules(), name)
                    .expect("framework column must satisfy Postgres sheet rules"),
            )
        })
        .collect()
}
