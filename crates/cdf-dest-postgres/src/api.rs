use std::collections::BTreeMap;

use cdf_kernel::{
    CdfError, CommitPlan, DestinationCommitRequest, DestinationId, MigrationRecord, Receipt,
    ReceiptId, Result, TransactionMetadata, WriteDisposition,
};
use cdf_package_contract::{ReceiptDraft, ReceiptEvidence};

use crate::{
    CDF_LOADS_TABLE, CDF_QUARANTINE_TABLE, CDF_STATE_TABLE, POSTGRES_DESTINATION_ID,
    POSTGRES_XID_SQL,
    ddl::{
        idempotency_check_statement, idempotency_lock_statement, provenance_unique_index_statement,
        system_table_ddl, system_table_migrations, target_migrations,
    },
    dml::write_statements,
    identifiers::postgres_identifier_rules,
    mirrors::{add_segments_to_verify_parameters, drift_hooks, mirror_statements, verify_clause},
    models::PostgresDestinationSheet,
    plan::{
        PostgresLoadPlan, PostgresLoadPlanInput, PostgresReceiptInput, PostgresStatement,
        StatementExpectation,
    },
    validate::{
        delivery_guarantee, ensure_supported_disposition, plan_id, plan_segments_in_receipt_order,
        stage_table_name, validate_columns, validate_merge_shape,
    },
};

pub fn plan_postgres_load(
    input: PostgresLoadPlanInput,
    sheet: &PostgresDestinationSheet,
) -> Result<PostgresLoadPlan> {
    ensure_supported_disposition(&input.disposition)?;
    if sheet.kernel.identifier_rules != postgres_identifier_rules() {
        return Err(CdfError::contract(
            "Postgres destination sheet identifier rules differ from the SQL adapter authority",
        ));
    }
    validate_columns(&input.columns)?;
    validate_merge_shape(&input)?;

    let stage_table = matches!(
        input.disposition,
        WriteDisposition::Merge | WriteDisposition::CdcApply
    )
    .then(|| stage_table_name(&input.package_hash))
    .transpose()?;
    let target_name = input.target.target_name()?;
    let no_data = input.segments.is_empty();
    let migrations = if no_data {
        Vec::new()
    } else {
        target_migrations(&input)?
    };
    let post_write_ddl = if no_data {
        Vec::new()
    } else {
        vec![provenance_unique_index_statement(&input.target)?]
    };
    let mut kernel_migrations = system_table_migrations();
    kernel_migrations.extend(migrations.iter().map(|statement| MigrationRecord {
        migration_id: format!("postgres.{}", statement.name),
        description: statement.sql.clone(),
    }));
    kernel_migrations.extend(post_write_ddl.iter().map(|statement| MigrationRecord {
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
        write_statements(&input, stage_table.as_ref())?
    };
    let mirror_sql = mirror_statements(&input, &verify);

    Ok(PostgresLoadPlan {
        kernel,
        package_hash: input.package_hash,
        content: input.content,
        idempotency_token: input.idempotency_token,
        schema_hash: input.schema_hash,
        segments: input.segments,
        target: input.target,
        stage_table,
        columns: input.columns,
        merge_keys: input.merge_keys,
        resource_id: input.resource_id,
        state_delta: input.state_delta,
        system_ddl: system_table_ddl(),
        target_ddl: migrations,
        post_write_ddl,
        idempotency_lock: idempotency_lock_statement(),
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

pub(crate) struct PostgresCommitRequest {
    pub(crate) package: cdf_package_contract::SharedVerifiedPackageAccess,
    pub(crate) plan: PostgresLoadPlan,
    pub(crate) segments: crate::package::PostgresSessionSegments,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostgresReceiptVerification {
    pub verified: bool,
    pub receipt_id: ReceiptId,
    pub reason: Option<String>,
}

pub fn build_receipt(plan: &PostgresLoadPlan, input: PostgresReceiptInput) -> Result<Receipt> {
    let request = DestinationCommitRequest {
        package_hash: plan.package_hash.clone(),
        content: plan.content.clone(),
        target: plan.kernel.target.clone(),
        disposition: plan.kernel.disposition.clone(),
        segments: plan_segments_in_receipt_order(plan),
        idempotency_token: plan.idempotency_token.clone(),
    };
    build_receipt_for_request(
        &request,
        &plan.kernel,
        &plan.schema_hash,
        &plan.verify,
        input,
    )
}

pub(crate) fn build_receipt_for_request(
    request: &DestinationCommitRequest,
    plan: &CommitPlan,
    schema_hash: &cdf_kernel::SchemaHash,
    verify: &cdf_kernel::VerifyClause,
    input: PostgresReceiptInput,
) -> Result<Receipt> {
    let mut transaction_values = BTreeMap::new();
    transaction_values.insert("xid".to_owned(), input.xid);
    transaction_values.insert("duplicate".to_owned(), input.duplicate.to_string());
    transaction_values.insert("loads_table".to_owned(), CDF_LOADS_TABLE.to_owned());
    transaction_values.insert("state_table".to_owned(), CDF_STATE_TABLE.to_owned());
    transaction_values.insert(
        "quarantine_table".to_owned(),
        CDF_QUARANTINE_TABLE.to_owned(),
    );

    ReceiptDraft::ordinary(
        input.receipt_id,
        DestinationId::new(POSTGRES_DESTINATION_ID)?,
        request,
        plan,
        request
            .segments
            .iter()
            .map(|segment| cdf_kernel::SegmentAck {
                kind: segment.kind,
                segment_id: segment.segment_id.clone(),
                row_count: segment.row_count,
                byte_count: segment.byte_count,
            })
            .collect(),
        schema_hash.clone(),
        ReceiptEvidence {
            transaction: Some(TransactionMetadata {
                system: POSTGRES_DESTINATION_ID.to_owned(),
                values: transaction_values,
            }),
            counts: input.counts,
            committed_at_ms: input.committed_at_ms,
            verify: verify.clone(),
        },
    )?
    .finalize()
}
