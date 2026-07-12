use crate::*;
use crate::{api::*, mirrors::*, package::*, rows::*, sheet::*, sql::*, table::*};

#[derive(Clone, Debug)]
pub(crate) struct DuckDbCorrectionContext {
    pub(crate) request: DestinationCorrectionCommitRequest,
    pub(crate) plan: DestinationCorrectionCommitPlan,
    pub(crate) ddl: Vec<String>,
}

#[derive(Clone, Debug)]
struct PreparedCorrectionRow {
    address: RowProvenanceAddress,
    assignments: Vec<(String, CellValue)>,
    residual: Option<String>,
}

fn resolve_row_key(
    conn: &Connection,
    target: &TargetRef,
    address: &RowProvenanceAddress,
) -> Result<Option<u64>> {
    let range: Option<(u64, u64)> = conn.query_row(
        "SELECT row_key_start, row_key_end FROM _cdf_segments WHERE target = ? AND package_hash = ? AND segment_id = ?",
        params![
            target.table.as_str(),
            address.original_package_hash.as_str(),
            address.original_segment_id.as_str(),
        ],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .optional()
    .map_err(|error| duckdb_error("resolve DuckDB logical row provenance", error))?;
    range
        .map(|(start, end)| {
            let row_key = start
                .checked_add(address.original_row_ordinal)
                .ok_or_else(|| CdfError::data("DuckDB logical row address overflowed"))?;
            if row_key >= end {
                return Err(CdfError::destination(
                    "DuckDB logical row ordinal is outside its segment range",
                ));
            }
            Ok(row_key)
        })
        .transpose()
}

pub(crate) fn plan_correction_request(
    destination: &DuckDbDestination,
    request: &DestinationCorrectionCommitRequest,
) -> Result<DestinationCorrectionCommitPlan> {
    cdf_contract::validate_destination_correction_commit_request(request)?;
    request.validate_for(
        &duckdb_correction_capabilities(),
        &destination.sheet().transactions,
        &destination.sheet().idempotency,
    )?;
    if request.strategy() != CorrectionStrategy::InPlaceUpdate {
        return Err(CdfError::contract(
            "DuckDB correction execution requires in_place_update",
        ));
    }
    if !destination
        .sheet()
        .supported_dispositions
        .contains(&request.resource_disposition)
    {
        return Err(CdfError::contract(format!(
            "DuckDB destination does not support {:?} resource disposition",
            request.resource_disposition
        )));
    }
    if !destination.database_path().exists() {
        return Err(CdfError::contract(
            "DuckDB addressed correction requires an existing target database",
        ));
    }
    let conn = destination.open_read_only_connection()?;
    let mirror_request = mirror_request(request);
    let context = match find_duplicate_receipt(&conn, &mirror_request.commit)? {
        Some(receipt) => {
            let ddl = receipt
                .migrations
                .iter()
                .map(|migration| migration.description.clone())
                .collect::<Vec<_>>();
            let context = correction_context_from_ddl(request, ddl)?;
            context.plan.validate_receipt(request, &receipt)?;
            context
        }
        None => build_correction_context(&conn, request)?,
    };
    let plan = context.plan.clone();
    let mut pending = destination
        .pending_corrections
        .lock()
        .map_err(|_| CdfError::internal("DuckDB correction context cache is poisoned"))?;
    pending.insert(plan.kernel.plan_id.clone(), context);
    Ok(plan)
}

pub(crate) fn begin_correction_request<'a>(
    destination: &'a DuckDbDestination,
    request: DestinationCorrectionCommitRequest,
    plan: DestinationCorrectionCommitPlan,
) -> Result<Box<dyn CorrectionCommitSession + 'a>> {
    cdf_contract::validate_destination_correction_commit_request(&request)?;
    plan.validate_for(
        &request,
        &duckdb_correction_capabilities(),
        &destination.sheet().transactions,
        &destination.sheet().idempotency,
    )?;
    let mut pending = destination
        .pending_corrections
        .lock()
        .map_err(|_| CdfError::internal("DuckDB correction context cache is poisoned"))?;
    let context = pending.remove(&plan.kernel.plan_id).ok_or_else(|| {
        CdfError::contract(
            "DuckDB begin_correction requires a prior plan_correction for the same correction package",
        )
    })?;
    if context.request != request || context.plan != plan {
        return Err(CdfError::contract(
            "DuckDB correction begin request or plan does not match planned correction authority",
        ));
    }
    drop(pending);
    Ok(Box::new(DuckDbCorrectionSession {
        destination,
        context,
        migrations_applied: false,
        corrections_applied: false,
    }))
}

pub(crate) fn verify_correction_receipt(
    destination: &DuckDbDestination,
    receipt: &Receipt,
) -> Result<ReceiptVerification> {
    if let Err(error) = DestinationCorrectionReceiptEvidence::from_receipt(receipt) {
        return Ok(ReceiptVerification {
            verified: false,
            receipt_id: receipt.receipt_id.clone(),
            reason: Some(error.to_string()),
        });
    }
    destination.verify_receipt(receipt)
}

pub(crate) fn read_addressed_residual(
    destination: &DuckDbDestination,
    target: &TargetName,
    original_row: &RowProvenanceAddress,
) -> Result<Option<DestinationResidualReadback>> {
    if !destination.database_path().exists() {
        return Ok(None);
    }
    let target = parse_target(target)?;
    let conn = destination.open_read_only_connection()?;
    let existing = existing_columns(&conn, &target)?;
    if existing.is_empty() {
        return Ok(None);
    }
    require_targetable_provenance(&conn, &target, &existing)?;
    match existing.get(cdf_contract::VARIANT_COLUMN_NAME) {
        Some(column) if same_type(&column.data_type, "VARCHAR") => {}
        Some(column) => {
            return Err(CdfError::contract(format!(
                "DuckDB correction target {} has _cdf_variant type {}; expected VARCHAR residual-json-v1 readback",
                target.sql_name(),
                column.data_type
            )));
        }
        None => {
            return Err(CdfError::contract(format!(
                "DuckDB correction target {} has no _cdf_variant residual readback column",
                target.sql_name()
            )));
        }
    }

    let Some(row_key) = resolve_row_key(&conn, &target, original_row)? else {
        return Ok(None);
    };
    let residual: Option<Option<String>> = conn
        .query_row(
            &format!(
                "SELECT {} FROM {} WHERE {} = ?",
                quote_ident(cdf_contract::VARIANT_COLUMN_NAME),
                target.sql_name(),
                quote_ident(CDF_ROW_KEY_COLUMN),
            ),
            params![row_key],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| duckdb_error("read exact DuckDB residual address", error))?;
    residual
        .map(|residual| {
            let residual_json_v1 = residual.map(String::into_bytes);
            if let Some(bytes) = &residual_json_v1 {
                cdf_contract::decode_residual_json_v1(bytes).map_err(|error| {
                    CdfError::destination(format!(
                        "DuckDB residual readback at ({}, {}, {}) is not canonical residual-json-v1: {error}",
                        original_row.original_package_hash,
                        original_row.original_segment_id,
                        original_row.original_row_ordinal
                    ))
                })?;
            }
            Ok(DestinationResidualReadback {
                original_row: original_row.clone(),
                residual_json_v1,
            })
        })
        .transpose()
}

impl CorrectionCommitSession for DuckDbCorrectionSession<'_> {
    fn apply_migrations(&mut self) -> Result<()> {
        self.migrations_applied = true;
        Ok(())
    }

    fn apply_corrections(&mut self) -> Result<CommitCounts> {
        if !self.migrations_applied {
            return Err(CdfError::destination(
                "DuckDB correction migrations must be applied before corrections",
            ));
        }
        self.corrections_applied = true;
        Ok(correction_counts(&self.context.request))
    }

    fn finalize(self: Box<Self>) -> Result<Receipt> {
        if !self.migrations_applied || !self.corrections_applied {
            return Err(CdfError::destination(
                "DuckDB correction session requires migrations and corrections before finalize",
            ));
        }
        commit_corrections(self.destination, self.context)
    }

    fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

fn build_correction_context(
    conn: &Connection,
    request: &DestinationCorrectionCommitRequest,
) -> Result<DuckDbCorrectionContext> {
    let target = parse_target(&request.target)?;
    let existing = existing_columns(conn, &target)?;
    if existing.is_empty() {
        return Err(CdfError::contract(format!(
            "DuckDB correction target {} does not exist",
            target.sql_name()
        )));
    }
    require_targetable_provenance(conn, &target, &existing)?;
    match existing.get(cdf_contract::VARIANT_COLUMN_NAME) {
        Some(column) if same_type(&column.data_type, "VARCHAR") => {}
        Some(column) => {
            return Err(CdfError::contract(format!(
                "DuckDB correction target {} has _cdf_variant type {}; expected VARCHAR residual-json-v1 readback",
                target.sql_name(),
                column.data_type
            )));
        }
        None => {
            return Err(CdfError::contract(format!(
                "DuckDB correction target {} has no _cdf_variant residual readback column",
                target.sql_name()
            )));
        }
    }

    let mut output_fields = BTreeMap::<String, FieldPlan>::new();
    for operation in &request.corrections {
        let arrow = operation.output_field.to_arrow()?;
        if arrow.name().starts_with("_cdf_") {
            return Err(CdfError::contract(format!(
                "DuckDB promoted output field {:?} uses the reserved `_cdf_*` namespace",
                arrow.name()
            )));
        }
        let field = field_plan(&arrow)?;
        if !field.nullable {
            return Err(CdfError::contract(format!(
                "DuckDB promoted output field {:?} must be nullable",
                field.name
            )));
        }
        if let Some(previous) = output_fields.insert(field.name.clone(), field.clone())
            && previous != field
        {
            return Err(CdfError::contract(format!(
                "DuckDB promoted output field {:?} has conflicting types",
                field.name
            )));
        }
    }

    let mut ddl = Vec::new();
    for field in output_fields.values() {
        match existing.get(&field.name) {
            Some(column) if same_type(&column.data_type, &field.sql_type) => {}
            Some(column) => {
                return Err(CdfError::contract(format!(
                    "DuckDB promoted column {}.{} has type {}, correction requires {}",
                    target.table, field.name, column.data_type, field.sql_type
                )));
            }
            None => ddl.push(format!(
                "ALTER TABLE {} ADD COLUMN {} {}",
                target.sql_name(),
                quote_ident(&field.name),
                field.sql_type
            )),
        }
    }
    prepare_correction_rows(conn, &target, request)?;

    correction_context_from_ddl(request, ddl)
}

fn correction_context_from_ddl(
    request: &DestinationCorrectionCommitRequest,
    ddl: Vec<String>,
) -> Result<DuckDbCorrectionContext> {
    let migrations = ddl
        .iter()
        .enumerate()
        .map(|(index, statement)| MigrationRecord {
            migration_id: format!("duckdb-correction-ddl-{:03}", index + 1),
            description: statement.clone(),
        })
        .collect::<Vec<_>>();
    let plan_id = PlanId::new(format!(
        "duckdb-correction:{}:{}",
        request.target, request.idempotency_token
    ))?;
    Ok(DuckDbCorrectionContext {
        request: request.clone(),
        plan: DestinationCorrectionCommitPlan {
            kernel: CommitPlan {
                plan_id,
                target: request.target.clone(),
                disposition: request.resource_disposition.clone(),
                idempotency: IdempotencySupport::PackageToken,
                migrations,
                delivery_guarantee: DeliveryGuarantee::EffectivelyOncePerPackage,
            },
            correction_package_hash: request.correction_package_hash.clone(),
            promotion_id: request.promotion_id().clone(),
            old_schema_hash: request.old_schema_hash().clone(),
            new_schema_hash: request.new_schema_hash().clone(),
            strategy: request.strategy(),
            operations_digest: request.operations_digest.clone(),
            correction_count: request.corrections.len() as u64,
        },
        ddl,
    })
}

fn prepare_correction_rows(
    conn: &Connection,
    target: &TargetRef,
    request: &DestinationCorrectionCommitRequest,
) -> Result<Vec<PreparedCorrectionRow>> {
    let mut by_address =
        BTreeMap::<RowProvenanceAddress, Vec<&DestinationCorrectionOperation>>::new();
    for operation in &request.corrections {
        by_address
            .entry(operation.correction.request.original_row.clone())
            .or_default()
            .push(operation);
    }

    let mut prepared = Vec::with_capacity(by_address.len());
    for (address, operations) in by_address {
        let row_key = resolve_row_key(conn, target, &address)?.ok_or_else(|| {
            CdfError::destination(format!(
                "DuckDB correction address ({}, {}, {}) has no compact provenance mapping",
                address.original_package_hash,
                address.original_segment_id,
                address.original_row_ordinal
            ))
        })?;
        let residual: Option<Option<String>> = conn
            .query_row(
                &format!(
                    "SELECT {} FROM {} WHERE {} = ?",
                    quote_ident(cdf_contract::VARIANT_COLUMN_NAME),
                    target.sql_name(),
                    quote_ident(CDF_ROW_KEY_COLUMN),
                ),
                params![row_key],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| duckdb_error("read addressed DuckDB residual", error))?;
        let Some(Some(mut residual)) = residual else {
            return Err(CdfError::destination(format!(
                "DuckDB correction address ({}, {}, {}) is missing or has no residual value",
                address.original_package_hash,
                address.original_segment_id,
                address.original_row_ordinal
            )));
        };

        let mut assignments = Vec::with_capacity(operations.len());
        let mut assigned_fields = BTreeSet::new();
        for operation in operations {
            if !assigned_fields.insert(operation.output_field.name.clone()) {
                return Err(CdfError::contract(format!(
                    "DuckDB correction assigns output field {:?} more than once for one row",
                    operation.output_field.name
                )));
            }
            let array = cdf_contract::decode_destination_correction_value(operation)?;
            let arrow = operation.output_field.to_arrow()?;
            let value = cell_value(array.as_ref(), arrow.data_type(), 0)?;
            let remaining = cdf_contract::remove_residual_json_v1_path(
                residual.as_bytes(),
                &operation.correction.request.promoted_path,
            )
            .map_err(|error| {
                CdfError::destination(format!(
                    "DuckDB correction residual removal failed for {:?}: {error}",
                    operation.correction.request.promoted_path
                ))
            })?;
            residual = remaining
                .as_ref()
                .map(|bytes| String::from_utf8(bytes.clone()))
                .transpose()
                .map_err(|error| CdfError::internal(error.to_string()))?
                .unwrap_or_default();
            assignments.push((operation.output_field.name.clone(), value));
        }
        prepared.push(PreparedCorrectionRow {
            address,
            assignments,
            residual: (!residual.is_empty()).then_some(residual),
        });
    }
    Ok(prepared)
}

fn commit_corrections(
    destination: &DuckDbDestination,
    context: DuckDbCorrectionContext,
) -> Result<Receipt> {
    let _lock = destination.acquire_writer_lock()?;
    let mut conn = destination.open_connection()?;
    ensure_mirror_tables(&conn)?;
    let mirror_request = mirror_request(&context.request);
    if let Some(receipt) = find_duplicate_receipt(&conn, &mirror_request.commit)? {
        context.plan.validate_receipt(&context.request, &receipt)?;
        return Ok(receipt);
    }

    let target = parse_target(&context.request.target)?;
    let committed_at_ms = now_ms()?;
    let duckdb_version = duckdb_version(&conn).unwrap_or_else(|_| "unknown".to_owned());
    let receipt = {
        let tx = conn
            .transaction()
            .map_err(|error| duckdb_error("begin DuckDB correction transaction", error))?;
        for ddl in &context.ddl {
            tx.execute_batch(ddl).map_err(|error| {
                duckdb_error(format!("apply DuckDB correction DDL {ddl}"), error)
            })?;
        }
        let rows = prepare_correction_rows(&tx, &target, &context.request)?;
        for row in &rows {
            update_correction_row(&tx, &target, row)?;
        }
        let counts = correction_counts(&context.request);
        let receipt = build_correction_receipt(
            destination,
            &context,
            counts,
            committed_at_ms,
            &duckdb_version,
        )?;
        insert_mirrors(&tx, &mirror_request, &receipt.segment_acks, &receipt, None)?;
        tx.commit()
            .map_err(|error| duckdb_error("commit DuckDB correction transaction", error))?;
        receipt
    };
    context.plan.validate_receipt(&context.request, &receipt)?;
    Ok(receipt)
}

fn update_correction_row(
    conn: &Connection,
    target: &TargetRef,
    row: &PreparedCorrectionRow,
) -> Result<()> {
    let row_key = resolve_row_key(conn, target, &row.address)?
        .ok_or_else(|| CdfError::destination("DuckDB correction provenance mapping disappeared"))?;
    let mut assignments = row
        .assignments
        .iter()
        .map(|(name, _)| format!("{} = ?", quote_ident(name)))
        .collect::<Vec<_>>();
    assignments.push(format!(
        "{} = ?",
        quote_ident(cdf_contract::VARIANT_COLUMN_NAME)
    ));
    let mut values = row
        .assignments
        .iter()
        .map(|(_, value)| value.value.clone())
        .collect::<Vec<_>>();
    values.push(
        row.residual
            .as_ref()
            .map_or(Value::Null, |value| Value::Text(value.clone())),
    );
    values.extend([Value::UBigInt(row_key)]);
    let updated = conn
        .execute(
            &format!(
                "UPDATE {} SET {} WHERE {} = ?",
                target.sql_name(),
                assignments.join(", "),
                quote_ident(CDF_ROW_KEY_COLUMN),
            ),
            params_from_iter(values),
        )
        .map_err(|error| duckdb_error("update addressed DuckDB correction row", error))?;
    if updated != 1 {
        return Err(CdfError::destination(format!(
            "DuckDB correction address ({}, {}, {}) updated {updated} rows; expected exactly one",
            row.address.original_package_hash,
            row.address.original_segment_id,
            row.address.original_row_ordinal
        )));
    }
    Ok(())
}

fn correction_counts(request: &DestinationCorrectionCommitRequest) -> CommitCounts {
    let addressed = request.addressed_row_count();
    CommitCounts {
        rows_written: addressed,
        rows_inserted: Some(0),
        rows_updated: Some(addressed),
        rows_deleted: Some(0),
    }
}

fn mirror_request(request: &DestinationCorrectionCommitRequest) -> DuckDbCommitRequest {
    DuckDbCommitRequest {
        package_dir: PathBuf::new(),
        commit: DestinationCommitRequest {
            package_hash: request.correction_package_hash.clone(),
            target: request.target.clone(),
            disposition: request.resource_disposition.clone(),
            segments: request.segments.clone(),
            idempotency_token: request.idempotency_token.clone(),
        },
        schema_hash: request.new_schema_hash().clone(),
        merge_keys: Vec::new(),
    }
}

fn build_correction_receipt(
    destination: &DuckDbDestination,
    context: &DuckDbCorrectionContext,
    counts: CommitCounts,
    committed_at_ms: i64,
    duckdb_version: &str,
) -> Result<Receipt> {
    let request = &context.request;
    let mut transaction_values = BTreeMap::from([
        (
            "database_path".to_owned(),
            destination.database_path().display().to_string(),
        ),
        ("duckdb_version".to_owned(), duckdb_version.to_owned()),
        (
            "writer_lock".to_owned(),
            destination.lock_path().display().to_string(),
        ),
    ]);
    transaction_values.insert(
        DESTINATION_CORRECTION_RECEIPT_EVIDENCE_KEY.to_owned(),
        DestinationCorrectionReceiptEvidence::for_request(request).to_json()?,
    );
    let parameters = BTreeMap::from([
        ("target".to_owned(), request.target.to_string()),
        (
            "idempotency_token".to_owned(),
            request.idempotency_token.to_string(),
        ),
        (
            "package_hash".to_owned(),
            request.correction_package_hash.to_string(),
        ),
    ]);
    Ok(Receipt {
        receipt_id: ReceiptId::new(format!(
            "duckdb:{}:{}",
            request.target, request.idempotency_token
        ))?,
        destination: DestinationId::new(DESTINATION_ID)?,
        target: request.target.clone(),
        package_hash: request.correction_package_hash.clone(),
        segment_acks: request.segment_acks(),
        disposition: request.resource_disposition.clone(),
        idempotency_token: request.idempotency_token.clone(),
        transaction: Some(TransactionMetadata {
            system: "duckdb".to_owned(),
            values: transaction_values,
        }),
        counts,
        schema_hash: request.new_schema_hash().clone(),
        migrations: context.plan.kernel.migrations.clone(),
        committed_at_ms,
        verify: VerifyClause {
            kind: "duckdb_load_receipt_v1".to_owned(),
            statement: "SELECT receipt_json FROM _cdf_loads WHERE target = ? AND idempotency_token = ? AND package_hash = ?".to_owned(),
            parameters,
        },
    })
}
