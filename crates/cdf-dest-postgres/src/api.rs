use std::path::PathBuf;

use crate::*;
use crate::{ddl::*, dml::*, mirrors::*, validate::*};

pub fn plan_postgres_load(
    input: PostgresLoadPlanInput,
    sheet: &PostgresDestinationSheet,
) -> Result<PostgresLoadPlan> {
    ensure_supported_disposition(&input.disposition)?;
    validate_columns(&input.columns)?;
    validate_merge_shape(&input)?;

    let stage_table = stage_table_name(&input.package_hash)?;
    let target_name = input.target.target_name()?;
    let no_data = input.segments.is_empty();
    let migrations = if no_data {
        Vec::new()
    } else {
        target_migrations(&input)?
    };
    let mut kernel_migrations = system_table_migrations();
    kernel_migrations.extend(migrations.iter().map(|statement| MigrationRecord {
        migration_id: format!("postgres.{}", statement.name),
        description: statement.sql.clone(),
    }));

    let kernel = CommitPlan {
        plan_id: plan_id(
            &target_name,
            &input.disposition,
            input.package_hash.as_str(),
        )?,
        target: target_name.clone(),
        disposition: input.disposition.clone(),
        idempotency: sheet.kernel.idempotency.clone(),
        migrations: kernel_migrations,
        delivery_guarantee: delivery_guarantee(&input.disposition),
    };

    let mut verify = verify_clause(
        &target_name,
        input.target.schema.as_ref(),
        &input.package_hash,
        &input.idempotency_token,
        &input.schema_hash,
    );
    add_segments_to_verify_parameters(&mut verify, &input.segments);

    let drift = drift_hooks();
    let write_sql = if no_data {
        Vec::new()
    } else {
        write_statements(&input, &stage_table)?
    };
    let mirror_sql = mirror_statements(&input, &verify);

    Ok(PostgresLoadPlan {
        kernel,
        target: input.target,
        stage_table,
        columns: input.columns,
        merge_keys: input.merge_keys,
        dedup: input.dedup,
        resource_id: input.resource_id,
        state_delta: input.state_delta,
        system_ddl: system_table_ddl(),
        target_ddl: migrations,
        idempotency_check: idempotency_check_statement(),
        xid_probe: PostgresStatement::query(
            "capture_xid",
            POSTGRES_XID_SQL,
            StatementExpectation::ReturnsXid,
        ),
        write_sql,
        mirror_sql,
        verify,
        drift,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostgresCommitRequest {
    pub package_dir: PathBuf,
    pub plan: PostgresLoadPlan,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostgresCommitOutcome {
    pub receipt: Receipt,
    pub duplicate: bool,
    pub plan: PostgresLoadPlan,
    pub package_receipt_recorded: bool,
    pub package_receipt_error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostgresReceiptVerification {
    pub verified: bool,
    pub receipt_id: ReceiptId,
    pub reason: Option<String>,
}

pub fn build_receipt(plan: &PostgresLoadPlan, input: PostgresReceiptInput) -> Result<Receipt> {
    let mut transaction_values = BTreeMap::new();
    transaction_values.insert("xid".to_owned(), input.xid);
    transaction_values.insert("duplicate".to_owned(), input.duplicate.to_string());
    transaction_values.insert("loads_table".to_owned(), CDF_LOADS_TABLE.to_owned());
    transaction_values.insert("state_table".to_owned(), CDF_STATE_TABLE.to_owned());
    transaction_values.insert(
        "quarantine_table".to_owned(),
        CDF_QUARANTINE_TABLE.to_owned(),
    );

    Ok(Receipt {
        receipt_id: input.receipt_id,
        destination: DestinationId::new(POSTGRES_DESTINATION_ID)?,
        target: plan.kernel.target.clone(),
        package_hash: PackageHash::new(
            plan.verify
                .parameters
                .get("package_hash")
                .ok_or_else(|| CdfError::internal("verify clause missing package_hash"))?
                .clone(),
        )?,
        segment_acks: plan_segment_acks(plan),
        disposition: plan.kernel.disposition.clone(),
        idempotency_token: IdempotencyToken::new(
            plan.verify
                .parameters
                .get("idempotency_token")
                .ok_or_else(|| CdfError::internal("verify clause missing idempotency_token"))?
                .clone(),
        )?,
        transaction: Some(TransactionMetadata {
            system: POSTGRES_DESTINATION_ID.to_owned(),
            values: transaction_values,
        }),
        counts: input.counts,
        schema_hash: SchemaHash::new(
            plan.verify
                .parameters
                .get("schema_hash")
                .ok_or_else(|| CdfError::internal("verify clause missing schema_hash"))?
                .clone(),
        )?,
        migrations: plan.kernel.migrations.clone(),
        committed_at_ms: input.committed_at_ms,
        verify: plan.verify.clone(),
    })
}

pub fn source_exercise_hooks(
    target: &PostgresTarget,
    columns: &[PostgresIdentifier],
    order_by: &[PostgresIdentifier],
    cursor: Option<&PostgresIdentifier>,
) -> Result<PostgresSourceExerciseHooks> {
    if columns.is_empty() {
        return Err(CdfError::contract(
            "Postgres source exercise hooks require at least one projected column",
        ));
    }
    if order_by.is_empty() {
        return Err(CdfError::contract(
            "Postgres source exercise hooks require deterministic order_by columns",
        ));
    }

    let projection = columns
        .iter()
        .map(PostgresIdentifier::quoted)
        .collect::<Vec<_>>()
        .join(", ");
    let ordering = order_by
        .iter()
        .map(PostgresIdentifier::quoted)
        .collect::<Vec<_>>()
        .join(", ");
    let snapshot_page = format!(
        "SELECT {projection} FROM {} ORDER BY {ordering} LIMIT $1 OFFSET $2",
        target.sql()
    );
    let incremental_page = cursor.map(|cursor| {
        PostgresStatement::query(
            "source_incremental_page",
            format!(
                "SELECT {projection} FROM {} WHERE {} > $1 ORDER BY {}, {ordering} LIMIT $2",
                target.sql(),
                cursor.quoted(),
                cursor.quoted()
            ),
            StatementExpectation::ReturnsMirrorRows,
        )
    });

    Ok(PostgresSourceExerciseHooks {
        snapshot_count: PostgresStatement::query(
            "source_snapshot_count",
            format!(
                "SELECT COUNT(*) AS \"cdf_source_rows\" FROM {}",
                target.sql()
            ),
            StatementExpectation::ReturnsMirrorRows,
        ),
        snapshot_page: PostgresStatement::query(
            "source_snapshot_page",
            snapshot_page,
            StatementExpectation::ReturnsMirrorRows,
        ),
        incremental_page,
    })
}
