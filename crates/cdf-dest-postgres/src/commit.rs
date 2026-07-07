use std::{
    io::Write,
    time::{SystemTime, UNIX_EPOCH},
};

use postgres::{Client, NoTls, Row, Transaction};

use crate::{dml::*, package::*, validate::*, *};

impl PostgresDestination {
    pub fn connect(database_url: impl Into<String>) -> Result<Self> {
        let database_url = database_url.into();
        if database_url.trim().is_empty() {
            return Err(CdfError::contract("Postgres database URL cannot be empty"));
        }
        Ok(Self {
            sheet: postgres_destination_sheet(),
            database_url: Some(database_url),
        })
    }

    pub fn database_url(&self) -> Option<&str> {
        self.database_url.as_deref()
    }

    pub fn commit_package(&self, request: PostgresCommitRequest) -> Result<PostgresCommitOutcome> {
        let database_url = self.database_url.as_deref().ok_or_else(|| {
            CdfError::contract(
                "PostgresDestination::commit_package requires PostgresDestination::connect",
            )
        })?;
        let package = load_package_for_plan(&request.package_dir, &request.plan)?;
        let mut client = Client::connect(database_url, NoTls)
            .map_err(|error| postgres_error("connect to Postgres", error))?;
        let mut tx = client
            .transaction()
            .map_err(|error| postgres_error("begin Postgres transaction", error))?;
        set_target_schema_search_path(&mut tx, &request.plan.target)?;

        execute_statements(&mut tx, &request.plan.system_ddl)?;
        if let Some(receipt) = find_duplicate_receipt(&mut tx, &request.plan)? {
            tx.commit()
                .map_err(|error| postgres_error("commit duplicate Postgres transaction", error))?;
            let (recorded, record_error) =
                record_package_receipt_best_effort(&request.package_dir, &receipt);
            return Ok(PostgresCommitOutcome {
                receipt,
                duplicate: true,
                plan: request.plan,
                package_receipt_recorded: recorded,
                package_receipt_error: record_error,
            });
        }

        execute_statements(&mut tx, &request.plan.target_ddl)?;
        let xid = query_xid(&mut tx, &request.plan)?;
        let committed_at_ms = now_ms()?;
        let counts = apply_write_plan(&mut tx, &request.plan, &package, committed_at_ms)?;
        let receipt = build_receipt(
            &request.plan,
            PostgresReceiptInput {
                receipt_id: receipt_id(&request.plan)?,
                xid,
                committed_at_ms,
                counts,
                duplicate: false,
            },
        )?;
        insert_load_mirror(&mut tx, &request.plan, &receipt)?;
        if let Some(delta) = &request.plan.state_delta {
            upsert_state_mirror(&mut tx, &request.plan, &receipt, delta)?;
        }
        verify_receipt_in_transaction(&mut tx, &receipt)?;
        tx.commit()
            .map_err(|error| postgres_error("commit Postgres transaction", error))?;

        let (recorded, record_error) =
            record_package_receipt_best_effort(&request.package_dir, &receipt);
        Ok(PostgresCommitOutcome {
            receipt,
            duplicate: false,
            plan: request.plan,
            package_receipt_recorded: recorded,
            package_receipt_error: record_error,
        })
    }

    pub fn verify_receipt(&self, receipt: &Receipt) -> Result<PostgresReceiptVerification> {
        let database_url = self.database_url.as_deref().ok_or_else(|| {
            CdfError::contract(
                "PostgresDestination::verify_receipt requires PostgresDestination::connect",
            )
        })?;
        let mut client = Client::connect(database_url, NoTls)
            .map_err(|error| postgres_error("connect to Postgres", error))?;
        match verify_receipt_with_client(&mut client, receipt) {
            Ok(()) => Ok(PostgresReceiptVerification {
                verified: true,
                receipt_id: receipt.receipt_id.clone(),
                reason: None,
            }),
            Err(error) => Ok(PostgresReceiptVerification {
                verified: false,
                receipt_id: receipt.receipt_id.clone(),
                reason: Some(error.to_string()),
            }),
        }
    }
}

fn execute_statements(tx: &mut Transaction<'_>, statements: &[PostgresStatement]) -> Result<()> {
    for statement in statements {
        tx.batch_execute(&statement.sql)
            .map_err(|error| postgres_error(format!("execute {}", statement.name), error))?;
    }
    Ok(())
}

fn find_duplicate_receipt(
    tx: &mut Transaction<'_>,
    plan: &PostgresLoadPlan,
) -> Result<Option<Receipt>> {
    let target = plan.kernel.target.as_str();
    let package_hash = verify_parameter(plan, "package_hash")?;
    let row = tx
        .query_opt(&plan.idempotency_check.sql, &[&target, &package_hash])
        .map_err(|error| postgres_error("query Postgres _cdf_loads idempotency", error))?;
    row.map(|row| {
        let json: String = row.get(0);
        serde_json::from_str(&json).map_err(json_error)
    })
    .transpose()
}

fn record_package_receipt_best_effort(
    package_dir: &std::path::Path,
    receipt: &Receipt,
) -> (bool, Option<String>) {
    match record_package_receipt_once(package_dir, receipt) {
        Ok(recorded) => (recorded, None),
        Err(error) => (false, Some(error.to_string())),
    }
}

fn query_xid(tx: &mut Transaction<'_>, plan: &PostgresLoadPlan) -> Result<String> {
    tx.query_one(&plan.xid_probe.sql, &[])
        .map(|row| row.get(0))
        .map_err(|error| postgres_error("query Postgres xid", error))
}

fn apply_write_plan(
    tx: &mut Transaction<'_>,
    plan: &PostgresLoadPlan,
    package: &PostgresPackageData,
    loaded_at_ms: i64,
) -> Result<CommitCounts> {
    let mut rows_deleted = Some(0_u64);
    let mut rows_inserted = None;
    let mut rows_updated = Some(0_u64);
    let mut rows_written = 0_u64;

    for statement in &plan.write_sql {
        match statement.name.as_str() {
            "create_stage" => {
                tx.batch_execute(&statement.sql)
                    .map_err(|error| postgres_error("create Postgres stage table", error))?;
                copy_stage_rows(tx, plan, package, loaded_at_ms)?;
            }
            "truncate_target_for_replace" => {
                rows_deleted = Some(count_target_rows(tx, &plan.target)?);
                tx.batch_execute(&statement.sql)
                    .map_err(|error| postgres_error("truncate Postgres target", error))?;
            }
            "append_from_stage" | "replace_from_stage" => {
                let inserted = execute_count(tx, statement)?;
                rows_inserted = Some(inserted);
                rows_written = inserted;
            }
            "merge_duplicate_key_guard" => {
                let duplicates = tx.query(&statement.sql, &[]).map_err(|error| {
                    postgres_error("query Postgres merge duplicate guard", error)
                })?;
                if !duplicates.is_empty() {
                    return Err(CdfError::data(
                        "Postgres merge package contains duplicate merge keys and dedup policy is fail",
                    ));
                }
            }
            "merge_from_stage" => {
                let source_rows = count_merge_source_rows(tx, plan)?;
                let updated = count_merge_updates(tx, plan)?;
                execute_count(tx, statement)?;
                rows_written = source_rows;
                rows_inserted = Some(source_rows.saturating_sub(updated));
                rows_updated = Some(updated);
            }
            other => {
                return Err(CdfError::internal(format!(
                    "unsupported Postgres write statement {other}"
                )));
            }
        }
    }

    Ok(CommitCounts {
        rows_written,
        rows_inserted,
        rows_updated,
        rows_deleted,
    })
}

fn copy_stage_rows(
    tx: &mut Transaction<'_>,
    plan: &PostgresLoadPlan,
    package: &PostgresPackageData,
    loaded_at_ms: i64,
) -> Result<u64> {
    let mut columns = quoted_column_names(&plan.columns);
    columns.extend(quoted_system_target_column_names());
    let copy_sql = format!(
        "COPY {} ({}) FROM STDIN WITH (FORMAT csv, NULL '\\N')",
        plan.stage_table.quoted(),
        columns.join(", ")
    );
    let mut writer = tx
        .copy_in(&copy_sql)
        .map_err(|error| postgres_error("open Postgres COPY into stage", error))?;
    let load = verify_parameter(plan, "idempotency_token")?;
    for row in &package.rows {
        writer
            .write_all(row.csv_line(&load, loaded_at_ms).as_bytes())
            .map_err(|error| io_error("write Postgres COPY row", error))?;
    }
    writer
        .finish()
        .map_err(|error| postgres_error("finish Postgres COPY into stage", error))
}

fn execute_count(tx: &mut Transaction<'_>, statement: &PostgresStatement) -> Result<u64> {
    tx.execute(&statement.sql, &[])
        .map_err(|error| postgres_error(format!("execute {}", statement.name), error))
}

fn count_target_rows(tx: &mut Transaction<'_>, target: &PostgresTarget) -> Result<u64> {
    let sql = format!("SELECT COUNT(*)::bigint FROM {}", target.sql());
    let count: i64 = tx
        .query_one(&sql, &[])
        .map(|row| row.get(0))
        .map_err(|error| postgres_error("count Postgres target rows", error))?;
    u64::try_from(count).map_err(|_| CdfError::internal("Postgres count was negative"))
}

fn count_merge_source_rows(tx: &mut Transaction<'_>, plan: &PostgresLoadPlan) -> Result<u64> {
    let sql = match plan.dedup {
        MergeDedupPolicy::First | MergeDedupPolicy::Last => format!(
            "{}SELECT COUNT(*)::bigint FROM \"_cdf_dedup\"",
            merge_dedup_cte(plan)
        ),
        MergeDedupPolicy::Fail => {
            format!("SELECT COUNT(*)::bigint FROM {}", plan.stage_table.quoted())
        }
    };
    query_count(tx, &sql, "count Postgres merge source rows")
}

fn count_merge_updates(tx: &mut Transaction<'_>, plan: &PostgresLoadPlan) -> Result<u64> {
    let (cte, source) = match plan.dedup {
        MergeDedupPolicy::First | MergeDedupPolicy::Last => {
            (merge_dedup_cte(plan), "\"_cdf_dedup\"".to_owned())
        }
        MergeDedupPolicy::Fail => (String::new(), plan.stage_table.quoted()),
    };
    let sql = format!(
        "{cte}SELECT COUNT(*)::bigint FROM {} AS \"target\" WHERE EXISTS (SELECT 1 FROM {source} AS \"stage\" WHERE {})",
        plan.target.sql(),
        merge_match_predicate(&plan.merge_keys)
    );
    query_count(tx, &sql, "count Postgres merge updates")
}

fn query_count(tx: &mut Transaction<'_>, sql: &str, context: &str) -> Result<u64> {
    let count: i64 = tx
        .query_one(sql, &[])
        .map(|row| row.get(0))
        .map_err(|error| postgres_error(context, error))?;
    u64::try_from(count).map_err(|_| CdfError::internal("Postgres count was negative"))
}

fn merge_dedup_cte(plan: &PostgresLoadPlan) -> String {
    let conflict_columns = plan
        .merge_keys
        .iter()
        .map(PostgresIdentifier::quoted)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "WITH \"_cdf_ranked\" AS (\n  SELECT {}, ROW_NUMBER() OVER (PARTITION BY {} ORDER BY {}, {}) AS \"_cdf_rank\"\n  FROM {}\n), \"_cdf_dedup\" AS (\n  SELECT * FROM \"_cdf_ranked\" WHERE \"_cdf_rank\" = 1\n)\n",
        stage_select_list(&plan.columns),
        conflict_columns,
        order_expression(CDF_SEGMENT_COLUMN, &plan.dedup),
        order_expression(CDF_ROW_COLUMN, &plan.dedup),
        plan.stage_table.quoted()
    )
}

fn merge_match_predicate(keys: &[PostgresIdentifier]) -> String {
    keys.iter()
        .map(|key| {
            format!(
                "\"target\".{} IS NOT DISTINCT FROM \"stage\".{}",
                key.quoted(),
                key.quoted()
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn insert_load_mirror(
    tx: &mut Transaction<'_>,
    plan: &PostgresLoadPlan,
    receipt: &Receipt,
) -> Result<()> {
    let statement = plan
        .mirror_sql
        .iter()
        .find(|statement| statement.name == "record_cdf_load")
        .ok_or_else(|| CdfError::internal("Postgres plan missing record_cdf_load statement"))?;
    let migrations_json = serde_json::to_string(&receipt.migrations).map_err(json_error)?;
    let receipt_json = serde_json::to_string(receipt).map_err(json_error)?;
    let xid = receipt
        .transaction
        .as_ref()
        .and_then(|metadata| metadata.values.get("xid"))
        .ok_or_else(|| CdfError::internal("Postgres receipt missing xid"))?;
    let duplicate = false;
    let resource_id = plan_resource_id(plan);
    let target = receipt.target.as_str();
    let package_hash = receipt.package_hash.as_str();
    let idempotency_token = receipt.idempotency_token.as_str();
    let disposition = disposition_name(&receipt.disposition);
    let schema_hash = receipt.schema_hash.as_str();
    let rows_written = to_i64(receipt.counts.rows_written, "rows_written")?;
    let rows_inserted = optional_to_i64(receipt.counts.rows_inserted, "rows_inserted")?;
    let rows_updated = optional_to_i64(receipt.counts.rows_updated, "rows_updated")?;
    let rows_deleted = optional_to_i64(receipt.counts.rows_deleted, "rows_deleted")?;
    let segment_count = to_i64(receipt.segment_acks.len() as u64, "segment_count")?;
    tx.execute(
        &statement.sql,
        &[
            &receipt.receipt_id.as_str(),
            &target,
            &package_hash,
            &resource_id,
            &idempotency_token,
            &disposition,
            &schema_hash,
            &rows_written,
            &rows_inserted,
            &rows_updated,
            &rows_deleted,
            &segment_count,
            &migrations_json,
            &receipt_json,
            &xid,
            &duplicate,
            &receipt.committed_at_ms,
        ],
    )
    .map_err(|error| postgres_error("insert Postgres _cdf_loads mirror", error))?;
    Ok(())
}

fn upsert_state_mirror(
    tx: &mut Transaction<'_>,
    plan: &PostgresLoadPlan,
    receipt: &Receipt,
    delta: &StateDelta,
) -> Result<()> {
    let statement = plan
        .mirror_sql
        .iter()
        .find(|statement| statement.name == "upsert_cdf_state")
        .ok_or_else(|| CdfError::internal("Postgres plan missing upsert_cdf_state statement"))?;
    let scope_json = serde_json::to_string(&delta.scope).map_err(json_error)?;
    let output_position_json = serde_json::to_string(&delta.output_position).map_err(json_error)?;
    let state_version = i32::from(delta.state_version);
    tx.execute(
        &statement.sql,
        &[
            &delta.pipeline_id.as_str(),
            &delta.resource_id.as_str(),
            &scope_json,
            &state_version,
            &delta.checkpoint_id.as_str(),
            &receipt.package_hash.as_str(),
            &receipt.schema_hash.as_str(),
            &output_position_json,
            &receipt.receipt_id.as_str(),
            &receipt.committed_at_ms,
        ],
    )
    .map_err(|error| postgres_error("upsert Postgres _cdf_state mirror", error))?;
    Ok(())
}

fn verify_receipt_in_transaction(tx: &mut Transaction<'_>, receipt: &Receipt) -> Result<()> {
    let row = query_verify_row(tx, receipt)?;
    let stored = receipt_from_verify_row(row)?;
    if &stored == receipt {
        Ok(())
    } else {
        Err(CdfError::destination(
            "Postgres receipt verification read different receipt JSON",
        ))
    }
}

fn verify_receipt_with_client(client: &mut Client, receipt: &Receipt) -> Result<()> {
    set_receipt_schema_search_path(client, receipt)?;
    let rows = client
        .query(
            &receipt.verify.statement,
            &[
                &verify_receipt_parameter(receipt, "target")?,
                &verify_receipt_parameter(receipt, "package_hash")?,
                &verify_receipt_parameter(receipt, "idempotency_token")?,
                &verify_receipt_parameter(receipt, "schema_hash")?,
            ],
        )
        .map_err(|error| postgres_error("query Postgres receipt verification", error))?;
    let Some(row) = rows.into_iter().next() else {
        return Err(CdfError::destination(
            "receipt is absent from Postgres _cdf_loads",
        ));
    };
    let stored = receipt_from_verify_row(row)?;
    if &stored == receipt {
        Ok(())
    } else {
        Err(CdfError::destination(
            "stored Postgres receipt JSON differs from supplied receipt",
        ))
    }
}

fn set_target_schema_search_path(tx: &mut Transaction<'_>, target: &PostgresTarget) -> Result<()> {
    let Some(schema) = &target.schema else {
        return Ok(());
    };
    tx.batch_execute(&format!(
        "SET LOCAL search_path = {}, public",
        schema.quoted()
    ))
    .map_err(|error| postgres_error("set Postgres transaction search_path", error))?;
    Ok(())
}

fn set_receipt_schema_search_path(client: &mut Client, receipt: &Receipt) -> Result<()> {
    let Some(schema) = receipt.verify.parameters.get("target_schema") else {
        return Ok(());
    };
    let quoted = quote_identifier(schema)?;
    client
        .batch_execute(&format!("SET search_path = {quoted}, public"))
        .map_err(|error| postgres_error("set Postgres receipt search_path", error))?;
    Ok(())
}

fn query_verify_row(tx: &mut Transaction<'_>, receipt: &Receipt) -> Result<Row> {
    tx.query_opt(
        &receipt.verify.statement,
        &[
            &verify_receipt_parameter(receipt, "target")?,
            &verify_receipt_parameter(receipt, "package_hash")?,
            &verify_receipt_parameter(receipt, "idempotency_token")?,
            &verify_receipt_parameter(receipt, "schema_hash")?,
        ],
    )
    .map_err(|error| postgres_error("query Postgres receipt verification", error))?
    .ok_or_else(|| CdfError::destination("receipt is absent from Postgres _cdf_loads"))
}

fn receipt_from_verify_row(row: Row) -> Result<Receipt> {
    let json: String = row.get("receipt_json");
    serde_json::from_str(&json).map_err(json_error)
}

fn receipt_id(plan: &PostgresLoadPlan) -> Result<ReceiptId> {
    ReceiptId::new(format!(
        "postgres:{}:{}",
        plan.kernel.target.as_str(),
        token_suffix(verify_parameter(plan, "idempotency_token")?.as_str())
    ))
}

fn plan_resource_id(plan: &PostgresLoadPlan) -> Option<&str> {
    plan.resource_id
        .as_ref()
        .map(ResourceId::as_str)
        .or_else(|| {
            plan.state_delta
                .as_ref()
                .map(|delta| delta.resource_id.as_str())
        })
}

fn verify_parameter(plan: &PostgresLoadPlan, name: &str) -> Result<String> {
    plan.verify
        .parameters
        .get(name)
        .cloned()
        .ok_or_else(|| CdfError::internal(format!("verify clause missing {name}")))
}

fn verify_receipt_parameter(receipt: &Receipt, name: &str) -> Result<String> {
    receipt
        .verify
        .parameters
        .get(name)
        .cloned()
        .ok_or_else(|| CdfError::internal(format!("verify clause missing {name}")))
}

fn to_i64(value: u64, name: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| CdfError::internal(format!("{name} exceeds i64")))
}

fn optional_to_i64(value: Option<u64>, name: &str) -> Result<Option<i64>> {
    value.map(|value| to_i64(value, name)).transpose()
}

fn now_ms() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            CdfError::internal(format!("system clock is before UNIX_EPOCH: {error}"))
        })?;
    i64::try_from(duration.as_millis())
        .map_err(|_| CdfError::internal("system time milliseconds exceed i64"))
}

fn postgres_error(context: impl Into<String>, error: postgres::Error) -> CdfError {
    CdfError::destination(format!("{}: {}", context.into(), error))
}

fn io_error(context: impl Into<String>, error: std::io::Error) -> CdfError {
    CdfError::destination(format!("{}: {}", context.into(), error))
}

fn json_error(error: serde_json::Error) -> CdfError {
    CdfError::data(error.to_string())
}
