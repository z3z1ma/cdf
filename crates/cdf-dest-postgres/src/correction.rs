use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use postgres::{Client, NoTls};

use crate::{
    ddl::{provenance_unique_index_statement, system_table_ddl, system_table_migrations},
    identifiers::quote_identifier_unchecked,
    mirrors::{record_load_sql, verify_clause},
    rows::{correction_cell_text, postgres_type_for_arrow},
    validate::{disposition_name, token_suffix},
    *,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostgresCorrectionPlanInput {
    pub request: DestinationCorrectionCommitRequest,
    pub existing_table: PostgresExistingTable,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresCorrectionFieldPlan {
    pub promoted_path: String,
    pub column: PostgresColumn,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresCorrectionPlan {
    pub kernel: DestinationCorrectionCommitPlan,
    pub target: PostgresTarget,
    pub stage_table: PostgresIdentifier,
    pub fields: Vec<PostgresCorrectionFieldPlan>,
    pub system_ddl: Vec<PostgresStatement>,
    pub target_ddl: Vec<PostgresStatement>,
    pub create_stage: PostgresStatement,
    pub update_sql: Vec<PostgresStatement>,
    pub verify: VerifyClause,
}

impl PostgresCorrectionPlan {
    pub fn transactional_statements(&self) -> Vec<PostgresStatement> {
        let mut statements = vec![PostgresStatement::execute("begin", "BEGIN")];
        statements.extend(self.system_ddl.clone());
        statements.extend(self.target_ddl.clone());
        statements.push(self.create_stage.clone());
        statements.extend(self.update_sql.clone());
        statements.push(PostgresStatement::execute(
            "record_cdf_correction",
            record_load_sql(),
        ));
        statements.push(PostgresStatement::query(
            "verify_correction_receipt",
            self.verify.statement.clone(),
            StatementExpectation::ReturnsVerifyRow,
        ));
        statements.push(PostgresStatement::execute("commit", "COMMIT"));
        statements
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PostgresCorrectionCommitRequest {
    pub(crate) package_dir: PathBuf,
    pub(crate) plan: PostgresCorrectionPlan,
}

pub fn plan_postgres_correction(
    input: PostgresCorrectionPlanInput,
    sheet: &PostgresDestinationSheet,
) -> Result<PostgresCorrectionPlan> {
    cdf_contract::validate_destination_correction_commit_request(&input.request)?;
    let capabilities = postgres_correction_capabilities();
    input.request.validate_for(
        &capabilities,
        &sheet.kernel.transactions,
        &sheet.kernel.idempotency,
    )?;
    if input.request.strategy() != CorrectionStrategy::InPlaceUpdate {
        return Err(CdfError::contract(
            "Postgres addressed correction requires in_place_update strategy",
        ));
    }

    validate_existing_provenance(&input.existing_table)?;
    let target = PostgresTarget::parse(input.request.target.as_str())?;
    let fields = correction_field_plans(&input.request)?;
    let mut target_ddl = correction_column_migrations(&target, &fields, &input.existing_table)?;
    target_ddl.push(provenance_unique_index_statement(&target)?);

    let mut migrations = system_table_migrations();
    migrations.extend(target_ddl.iter().map(|statement| MigrationRecord {
        migration_id: format!("postgres.{}", statement.name),
        description: statement.sql.clone(),
    }));

    let kernel_commit = CommitPlan {
        plan_id: PlanId::new(format!(
            "postgres:correction:{}:{}",
            target.display_name().replace('.', "_"),
            token_suffix(input.request.correction_package_hash.as_str())
        ))?,
        target: input.request.target.clone(),
        disposition: input.request.resource_disposition.clone(),
        idempotency: IdempotencySupport::PackageToken,
        migrations,
        delivery_guarantee: DeliveryGuarantee::EffectivelyOncePerPackage,
    };
    let kernel = DestinationCorrectionCommitPlan {
        kernel: kernel_commit,
        correction_package_hash: input.request.correction_package_hash.clone(),
        promotion_id: input.request.promotion_id().clone(),
        old_schema_hash: input.request.old_schema_hash().clone(),
        new_schema_hash: input.request.new_schema_hash().clone(),
        strategy: input.request.strategy(),
        operations_digest: input.request.operations_digest.clone(),
        correction_count: input.request.corrections.len() as u64,
    };
    kernel.validate_for(
        &input.request,
        &capabilities,
        &sheet.kernel.transactions,
        &sheet.kernel.idempotency,
    )?;

    let stage_table = correction_stage_table_name(&input.request.correction_package_hash)?;
    let create_stage = correction_stage_statement(&stage_table);
    let update_sql = fields
        .iter()
        .map(|field| correction_update_statement(&target, &stage_table, field))
        .collect();
    let verify = verify_clause(
        &input.request.target,
        target.schema.as_ref(),
        &input.request.correction_package_hash,
        &input.request.idempotency_token,
        input.request.new_schema_hash(),
    );

    Ok(PostgresCorrectionPlan {
        kernel,
        target,
        stage_table,
        fields,
        system_ddl: system_table_ddl(),
        target_ddl,
        create_stage,
        update_sql,
        verify,
    })
}

pub(crate) fn validate_postgres_correction_begin(
    request: &DestinationCorrectionCommitRequest,
    plan: &DestinationCorrectionCommitPlan,
    postgres_plan: &PostgresCorrectionPlan,
) -> Result<()> {
    cdf_contract::validate_destination_correction_commit_request(request)?;
    if plan != &postgres_plan.kernel {
        return Err(CdfError::destination(
            "Postgres correction session plan does not match prepared adapter plan",
        ));
    }
    plan.validate_for(
        request,
        &postgres_correction_capabilities(),
        &TransactionSupport::AtomicPackage,
        &IdempotencySupport::PackageToken,
    )
}

impl PostgresDestination {
    pub fn inspect_correction_target(
        &self,
        target: &PostgresTarget,
    ) -> Result<PostgresExistingTable> {
        let database_url = self.database_url.as_deref().ok_or_else(|| {
            CdfError::contract(
                "PostgresDestination::inspect_correction_target requires PostgresDestination::connect",
            )
        })?;
        let mut client = Client::connect(database_url, NoTls).map_err(|error| {
            correction_postgres_error("connect for Postgres correction catalog", error)
        })?;
        let schema = target.schema.as_ref().map(PostgresIdentifier::as_str);
        let rows = client
            .query(
                "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = COALESCE($1, current_schema()) AND table_name = $2 ORDER BY ordinal_position",
                &[&schema, &target.table.as_str()],
            )
            .map_err(|error| {
                correction_postgres_error("read Postgres correction target catalog", error)
            })?;
        if rows.is_empty() {
            return Err(CdfError::destination(format!(
                "Postgres correction target {} does not exist",
                target.display_name()
            )));
        }
        let mut columns = BTreeMap::new();
        for row in rows {
            let name: String = row.get(0);
            let identifier = if name.starts_with("_cdf_") {
                PostgresIdentifier::system(&name)?
            } else {
                PostgresIdentifier::user(&name)?
            };
            columns.insert(
                name,
                PostgresExistingColumn {
                    name: identifier,
                    data_type: row.get(1),
                    nullable: row.get::<_, String>(2) == "YES",
                },
            );
        }
        Ok(PostgresExistingTable {
            columns,
            primary_key: Vec::new(),
        })
    }

    pub fn plan_addressed_correction(
        &self,
        input: PostgresCorrectionPlanInput,
    ) -> Result<PostgresCorrectionPlan> {
        plan_postgres_correction(input, &self.sheet)
    }

    pub(crate) fn begin_correction_session(
        &self,
        request: DestinationCorrectionCommitRequest,
        plan: PostgresCorrectionPlan,
        package_dir: PathBuf,
    ) -> Result<PostgresCorrectionSession> {
        let database_url = self.database_url.as_deref().ok_or_else(|| {
            CdfError::contract(
                "PostgresDestination::begin_correction requires PostgresDestination::connect",
            )
        })?;
        validate_postgres_correction_begin(&request, &plan.kernel, &plan)?;
        validate_correction_package(&package_dir, &request)?;
        Ok(PostgresCorrectionSession {
            database_url: database_url.to_owned(),
            request,
            plan,
            client: None,
            phase: PostgresCorrectionSessionPhase::Begun,
            duplicate_receipt: None,
            receipt: None,
            counts: None,
            prepared_rows: Vec::new(),
        })
    }

    pub fn read_addressed_residual(
        &self,
        target: &TargetName,
        original_row: &RowProvenanceAddress,
    ) -> Result<Option<DestinationResidualReadback>> {
        let database_url = self.database_url.as_deref().ok_or_else(|| {
            CdfError::contract(
                "PostgresDestination::read_addressed_residual requires PostgresDestination::connect",
            )
        })?;
        let target = PostgresTarget::parse(target.as_str())?;
        let mut client = Client::connect(database_url, NoTls).map_err(|error| {
            correction_postgres_error("connect for Postgres residual readback", error)
        })?;
        let row_ordinal = i64::try_from(original_row.original_row_ordinal)
            .map_err(|_| CdfError::contract("correction row ordinal exceeds Postgres BIGINT"))?;
        let sql = format!(
            "SELECT \"target\".{} FROM {} AS \"target\" JOIN {} AS \"segment\" ON \"target\".{} >= \"segment\".\"row_key_start\" AND \"target\".{} < \"segment\".\"row_key_end\" WHERE \"segment\".\"target\" = $1 AND \"segment\".\"package_hash\" = $2 AND \"segment\".\"segment_id\" = $3 AND \"target\".{} = \"segment\".\"row_key_start\" + $4",
            quote_identifier_unchecked("_cdf_variant"),
            target.sql(),
            target_system_table(&target, CDF_SEGMENTS_TABLE),
            quote_identifier_unchecked(CDF_ROW_KEY_COLUMN),
            quote_identifier_unchecked(CDF_ROW_KEY_COLUMN),
            quote_identifier_unchecked(CDF_ROW_KEY_COLUMN)
        );
        let rows = client
            .query(
                &sql,
                &[
                    &target.display_name(),
                    &original_row.original_package_hash.as_str(),
                    &original_row.original_segment_id.as_str(),
                    &row_ordinal,
                ],
            )
            .map_err(|error| {
                correction_postgres_error("read exact Postgres residual address", error)
            })?;
        match rows.as_slice() {
            [] => Ok(None),
            [row] => {
                let residual: Option<String> = row.get(0);
                Ok(Some(DestinationResidualReadback {
                    original_row: original_row.clone(),
                    residual_json_v1: residual.map(String::into_bytes),
                }))
            }
            _ => Err(CdfError::destination(format!(
                "Postgres residual address ({}, {}, {}) matched {} rows; expected at most one",
                original_row.original_package_hash,
                original_row.original_segment_id,
                original_row.original_row_ordinal,
                rows.len()
            ))),
        }
    }
}

pub(crate) struct PostgresCorrectionSession {
    database_url: String,
    request: DestinationCorrectionCommitRequest,
    plan: PostgresCorrectionPlan,
    client: Option<Client>,
    phase: PostgresCorrectionSessionPhase,
    duplicate_receipt: Option<Receipt>,
    receipt: Option<Receipt>,
    counts: Option<CommitCounts>,
    prepared_rows: Vec<PreparedCorrectionRow>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PostgresCorrectionSessionPhase {
    Begun,
    MigrationsApplied,
    Corrected,
}

impl PostgresCorrectionSession {
    fn finalize_receipt(mut self) -> Result<Receipt> {
        if self.phase != PostgresCorrectionSessionPhase::Corrected {
            return Err(CdfError::destination(
                "cannot finalize Postgres correction before addressed updates complete",
            ));
        }
        let receipt = self
            .duplicate_receipt
            .take()
            .or_else(|| self.receipt.take())
            .ok_or_else(|| CdfError::internal("Postgres correction session has no receipt"))?;
        self.plan.kernel.validate_receipt(&self.request, &receipt)?;
        let mut client = self
            .client
            .take()
            .ok_or_else(|| CdfError::internal("Postgres correction session has no transaction"))?;
        client
            .batch_execute("COMMIT")
            .map_err(|error| correction_postgres_error("commit Postgres correction", error))?;
        Ok(receipt)
    }

    fn rollback_open_transaction(&mut self) -> Result<()> {
        let Some(mut client) = self.client.take() else {
            return Ok(());
        };
        client
            .batch_execute("ROLLBACK")
            .map_err(|error| correction_postgres_error("abort Postgres correction", error))
    }
}

impl CorrectionCommitSession for PostgresCorrectionSession {
    fn apply_migrations(&mut self) -> Result<()> {
        if self.phase != PostgresCorrectionSessionPhase::Begun {
            return Err(CdfError::destination(
                "Postgres correction migrations have already been applied",
            ));
        }
        let mut client = Client::connect(&self.database_url, NoTls)
            .map_err(|error| correction_postgres_error("connect for Postgres correction", error))?;
        client
            .batch_execute("BEGIN")
            .map_err(|error| correction_postgres_error("begin Postgres correction", error))?;
        set_correction_search_path(&mut client, &self.plan.target)?;
        execute_correction_statements(&mut client, &self.plan.system_ddl)?;
        self.duplicate_receipt = find_correction_duplicate(&mut client, &self.request, &self.plan)?;
        if self.duplicate_receipt.is_none() {
            self.prepared_rows = prepare_correction_rows(&mut client, &self.request, &self.plan)?;
            execute_correction_statements(&mut client, &self.plan.target_ddl)?;
            client
                .batch_execute(&self.plan.create_stage.sql)
                .map_err(|error| {
                    correction_postgres_error("create Postgres correction stage", error)
                })?;
            insert_correction_stage_rows(&mut client, &self.plan, &self.prepared_rows)?;
        }
        self.client = Some(client);
        self.phase = PostgresCorrectionSessionPhase::MigrationsApplied;
        Ok(())
    }

    fn apply_corrections(&mut self) -> Result<CommitCounts> {
        if self.phase != PostgresCorrectionSessionPhase::MigrationsApplied {
            return Err(CdfError::destination(
                "Postgres correction session must apply migrations before updates",
            ));
        }
        if let Some(receipt) = &self.duplicate_receipt {
            self.plan.kernel.validate_receipt(&self.request, receipt)?;
            self.counts = Some(receipt.counts.clone());
            self.phase = PostgresCorrectionSessionPhase::Corrected;
            return Ok(receipt.counts.clone());
        }

        let mut client = self
            .client
            .take()
            .ok_or_else(|| CdfError::internal("Postgres correction session has no transaction"))?;
        for (field, statement) in self.plan.fields.iter().zip(&self.plan.update_sql) {
            let expected = self
                .request
                .corrections
                .iter()
                .filter(|operation| {
                    operation.correction.request.promoted_path == field.promoted_path
                })
                .count() as u64;
            let updated = client
                .execute(&statement.sql, &[&field.promoted_path])
                .map_err(|error| {
                    correction_postgres_error(
                        format!("update promoted Postgres field {}", field.column.name),
                        error,
                    )
                })?;
            if updated != expected {
                return Err(CdfError::destination(format!(
                    "Postgres correction path {:?} updated {updated} row(s), expected {expected}",
                    field.promoted_path
                )));
            }
        }

        let addressed = self.request.addressed_row_count();
        let counts = CommitCounts {
            rows_written: addressed,
            rows_inserted: Some(0),
            rows_updated: Some(addressed),
            rows_deleted: Some(0),
        };
        let xid: String = client
            .query_one(POSTGRES_XID_SQL, &[])
            .map(|row| row.get(0))
            .map_err(|error| correction_postgres_error("query Postgres correction xid", error))?;
        let receipt = build_correction_receipt(&self.request, &self.plan, counts.clone(), xid)?;
        insert_correction_load_mirror(&mut client, &receipt)?;
        verify_correction_receipt_in_transaction(&mut client, &receipt)?;
        self.receipt = Some(receipt);
        self.counts = Some(counts.clone());
        self.client = Some(client);
        self.phase = PostgresCorrectionSessionPhase::Corrected;
        Ok(counts)
    }

    fn finalize(self: Box<Self>) -> Result<Receipt> {
        self.counts.as_ref().ok_or_else(|| {
            CdfError::destination("Postgres correction session has not applied corrections")
        })?;
        (*self).finalize_receipt()
    }

    fn abort(mut self: Box<Self>) -> Result<()> {
        self.rollback_open_transaction()
    }
}

#[derive(Clone, Debug)]
struct PreparedCorrectionRow {
    row_key: i64,
    promoted_path: String,
    promoted_value: Option<String>,
    residual_after: Option<String>,
}

fn correction_field_plans(
    request: &DestinationCorrectionCommitRequest,
) -> Result<Vec<PostgresCorrectionFieldPlan>> {
    let mut by_path = BTreeMap::new();
    for operation in &request.corrections {
        let field = operation.output_field.to_arrow()?;
        let column = PostgresColumn::new(
            field.name(),
            &postgres_type_for_arrow(field.data_type())?,
            field.is_nullable(),
        )?;
        by_path
            .entry(operation.correction.request.promoted_path.clone())
            .or_insert(PostgresCorrectionFieldPlan {
                promoted_path: operation.correction.request.promoted_path.clone(),
                column,
            });
    }
    Ok(by_path.into_values().collect())
}

fn validate_existing_provenance(existing: &PostgresExistingTable) -> Result<()> {
    let required = [(CDF_ROW_KEY_COLUMN, "BIGINT"), ("_cdf_variant", "TEXT")];
    for (name, data_type) in required {
        let column = existing.columns.get(name).ok_or_else(|| {
            CdfError::destination(format!(
                "Postgres in-place correction requires existing {name} provenance/residual column"
            ))
        })?;
        if !column.data_type.eq_ignore_ascii_case(data_type) {
            return Err(CdfError::destination(format!(
                "Postgres in-place correction requires {name} as {data_type}, found {}",
                column.data_type
            )));
        }
        if name == CDF_ROW_KEY_COLUMN && column.nullable {
            return Err(CdfError::destination(format!(
                "Postgres in-place correction requires {name} to be NOT NULL; reload or migrate CDF provenance before enabling addressed correction"
            )));
        }
    }
    Ok(())
}

fn correction_column_migrations(
    target: &PostgresTarget,
    fields: &[PostgresCorrectionFieldPlan],
    existing: &PostgresExistingTable,
) -> Result<Vec<PostgresStatement>> {
    let mut migrations = Vec::new();
    for field in fields {
        match existing.columns.get(field.column.name.as_str()) {
            Some(column)
                if column
                    .data_type
                    .eq_ignore_ascii_case(&field.column.data_type) => {}
            Some(column) => {
                return Err(CdfError::destination(format!(
                    "Postgres promoted column {} exists as {} but correction requires {}",
                    field.column.name, column.data_type, field.column.data_type
                )));
            }
            None => migrations.push(PostgresStatement::execute(
                format!("add_promoted_column_{}", field.column.name),
                format!(
                    "ALTER TABLE {} ADD COLUMN {}",
                    target.sql(),
                    field.column.definition_sql()
                ),
            )),
        }
    }
    Ok(migrations)
}

fn correction_stage_table_name(package_hash: &PackageHash) -> Result<PostgresIdentifier> {
    PostgresIdentifier::system(format!(
        "_cdf_correction_{}",
        token_suffix(package_hash.as_str())
    ))
}

fn correction_stage_statement(stage: &PostgresIdentifier) -> PostgresStatement {
    PostgresStatement::execute(
        "create_correction_stage",
        format!(
            "CREATE TEMP TABLE {} (\n  {} BIGINT NOT NULL,\n  \"promoted_path\" TEXT NOT NULL,\n  \"promoted_value\" TEXT,\n  \"residual_after\" TEXT,\n  PRIMARY KEY ({}, \"promoted_path\")\n) ON COMMIT DROP",
            stage.quoted(),
            quote_identifier_unchecked(CDF_ROW_KEY_COLUMN),
            quote_identifier_unchecked(CDF_ROW_KEY_COLUMN)
        ),
    )
}

fn correction_update_statement(
    target: &PostgresTarget,
    stage: &PostgresIdentifier,
    field: &PostgresCorrectionFieldPlan,
) -> PostgresStatement {
    PostgresStatement::execute(
        format!("update_promoted_column_{}", field.column.name),
        format!(
            "UPDATE {} AS \"target\" SET {} = \"stage\".\"promoted_value\"::{}, {} = \"stage\".\"residual_after\" FROM {} AS \"stage\" WHERE \"stage\".\"promoted_path\" = $1 AND \"target\".{} = \"stage\".{}",
            target.sql(),
            field.column.name.quoted(),
            field.column.data_type,
            quote_identifier_unchecked("_cdf_variant"),
            stage.quoted(),
            quote_identifier_unchecked(CDF_ROW_KEY_COLUMN),
            quote_identifier_unchecked(CDF_ROW_KEY_COLUMN)
        ),
    )
}

fn validate_correction_package(
    package_dir: &Path,
    request: &DestinationCorrectionCommitRequest,
) -> Result<()> {
    let reader = cdf_package::PackageReader::open(package_dir)?;
    reader.verify()?;
    let replay = reader.replay_view()?;
    if replay.package_hash != request.correction_package_hash {
        return Err(CdfError::data(format!(
            "Postgres correction package hash {} does not match request {}",
            replay.package_hash, request.correction_package_hash
        )));
    }
    let expected = request
        .segments
        .iter()
        .map(|segment| {
            (
                segment.segment_id.clone(),
                (segment.row_count, segment.byte_count),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let actual = reader
        .manifest()
        .identity
        .segments
        .iter()
        .map(|segment| {
            (
                segment.segment_id.clone(),
                (segment.row_count, segment.byte_count),
            )
        })
        .collect::<BTreeMap<_, _>>();
    if expected != actual {
        return Err(CdfError::data(
            "Postgres correction request segments do not match verified package manifest",
        ));
    }
    Ok(())
}

fn target_system_table(target: &PostgresTarget, table: &str) -> String {
    match &target.schema {
        Some(schema) => format!("{}.{}", schema.quoted(), quote_identifier_unchecked(table)),
        None => quote_identifier_unchecked(table),
    }
}

fn prepare_correction_rows(
    client: &mut Client,
    request: &DestinationCorrectionCommitRequest,
    plan: &PostgresCorrectionPlan,
) -> Result<Vec<PreparedCorrectionRow>> {
    let mut operations_by_address = BTreeMap::new();
    for operation in &request.corrections {
        operations_by_address
            .entry(operation.correction.request.original_row.clone())
            .or_insert_with(Vec::new)
            .push(operation);
    }

    let address_sql = format!(
        "SELECT \"target\".{}, \"target\".{} FROM {} AS \"target\" JOIN {} AS \"segment\" ON \"target\".{} >= \"segment\".\"row_key_start\" AND \"target\".{} < \"segment\".\"row_key_end\" WHERE \"segment\".\"target\" = $1 AND \"segment\".\"package_hash\" = $2 AND \"segment\".\"segment_id\" = $3 AND \"target\".{} = \"segment\".\"row_key_start\" + $4 FOR UPDATE OF \"target\"",
        quote_identifier_unchecked(CDF_ROW_KEY_COLUMN),
        quote_identifier_unchecked("_cdf_variant"),
        plan.target.sql(),
        target_system_table(&plan.target, CDF_SEGMENTS_TABLE),
        quote_identifier_unchecked(CDF_ROW_KEY_COLUMN),
        quote_identifier_unchecked(CDF_ROW_KEY_COLUMN),
        quote_identifier_unchecked(CDF_ROW_KEY_COLUMN)
    );
    let mut prepared = Vec::with_capacity(request.corrections.len());
    for (address, mut operations) in operations_by_address {
        let row_ordinal = i64::try_from(address.original_row_ordinal)
            .map_err(|_| CdfError::contract("correction row ordinal exceeds Postgres BIGINT"))?;
        let rows = client
            .query(
                &address_sql,
                &[
                    &plan.target.display_name(),
                    &address.original_package_hash.as_str(),
                    &address.original_segment_id.as_str(),
                    &row_ordinal,
                ],
            )
            .map_err(|error| {
                correction_postgres_error("lock exact Postgres correction address", error)
            })?;
        if rows.len() != 1 {
            return Err(CdfError::destination(format!(
                "Postgres correction address ({}, {}, {}) matched {} row(s); expected exactly one",
                address.original_package_hash,
                address.original_segment_id,
                address.original_row_ordinal,
                rows.len()
            )));
        }
        let row_key: i64 = rows[0].get(0);
        let residual: Option<String> = rows[0].get(1);
        let mut residual = residual.ok_or_else(|| {
            CdfError::destination(format!(
                "Postgres correction address ({}, {}, {}) has no _cdf_variant residual",
                address.original_package_hash,
                address.original_segment_id,
                address.original_row_ordinal
            ))
        })?;
        operations.sort_by_key(|operation| operation.correction.request.promoted_path.as_str());
        for operation in &operations {
            residual = cdf_contract::remove_residual_json_v1_path(
                residual.as_bytes(),
                &operation.correction.request.promoted_path,
            )
            .map_err(|error| {
                CdfError::destination(format!(
                    "remove promoted residual path {:?}: {error}",
                    operation.correction.request.promoted_path
                ))
            })?
            .map(|bytes| {
                String::from_utf8(bytes).map_err(|error| {
                    CdfError::internal(format!("canonical residual-json-v1 was not UTF-8: {error}"))
                })
            })
            .transpose()?
            .unwrap_or_default();
        }
        let residual_after = (!residual.is_empty()).then_some(residual);
        for operation in operations {
            let value = cdf_contract::decode_destination_correction_value(operation)?;
            let promoted_value = correction_cell_text(value.as_ref(), value.data_type(), 0)?;
            prepared.push(PreparedCorrectionRow {
                row_key,
                promoted_path: operation.correction.request.promoted_path.clone(),
                promoted_value,
                residual_after: residual_after.clone(),
            });
        }
    }
    Ok(prepared)
}

fn insert_correction_stage_rows(
    client: &mut Client,
    plan: &PostgresCorrectionPlan,
    rows: &[PreparedCorrectionRow],
) -> Result<()> {
    let sql = format!(
        "INSERT INTO {} ({}, \"promoted_path\", \"promoted_value\", \"residual_after\") VALUES ($1, $2, $3, $4)",
        plan.stage_table.quoted(),
        quote_identifier_unchecked(CDF_ROW_KEY_COLUMN)
    );
    for row in rows {
        client
            .execute(
                &sql,
                &[
                    &row.row_key,
                    &row.promoted_path,
                    &row.promoted_value,
                    &row.residual_after,
                ],
            )
            .map_err(|error| {
                correction_postgres_error("insert Postgres correction stage row", error)
            })?;
    }
    Ok(())
}

fn find_correction_duplicate(
    client: &mut Client,
    request: &DestinationCorrectionCommitRequest,
    plan: &PostgresCorrectionPlan,
) -> Result<Option<Receipt>> {
    let sql = format!(
        "SELECT \"receipt_json\"::text FROM {} WHERE \"target\" = $1 AND \"package_hash\" = $2",
        quote_identifier_unchecked(CDF_LOADS_TABLE)
    );
    let row = client
        .query_opt(
            &sql,
            &[
                &request.target.as_str(),
                &request.correction_package_hash.as_str(),
            ],
        )
        .map_err(|error| {
            correction_postgres_error("query Postgres correction idempotency", error)
        })?;
    row.map(|row| {
        let json: String = row.get(0);
        let receipt: Receipt = serde_json::from_str(&json)
            .map_err(|error| CdfError::data(format!("decode correction receipt: {error}")))?;
        plan.kernel.validate_receipt(request, &receipt)?;
        Ok(receipt)
    })
    .transpose()
}

fn build_correction_receipt(
    request: &DestinationCorrectionCommitRequest,
    plan: &PostgresCorrectionPlan,
    counts: CommitCounts,
    xid: String,
) -> Result<Receipt> {
    let evidence = DestinationCorrectionReceiptEvidence::for_request(request);
    let transaction = TransactionMetadata {
        system: POSTGRES_DESTINATION_ID.to_owned(),
        values: BTreeMap::from([
            ("xid".to_owned(), xid),
            ("duplicate".to_owned(), "false".to_owned()),
            ("loads_table".to_owned(), CDF_LOADS_TABLE.to_owned()),
            (
                DESTINATION_CORRECTION_RECEIPT_EVIDENCE_KEY.to_owned(),
                evidence.to_json()?,
            ),
        ]),
    };
    Ok(Receipt {
        receipt_id: ReceiptId::new(format!(
            "postgres:{}:{}",
            request.target,
            token_suffix(request.idempotency_token.as_str())
        ))?,
        destination: DestinationId::new(POSTGRES_DESTINATION_ID)?,
        target: request.target.clone(),
        package_hash: request.correction_package_hash.clone(),
        segment_acks: request.segment_acks(),
        disposition: request.resource_disposition.clone(),
        idempotency_token: request.idempotency_token.clone(),
        transaction: Some(transaction),
        counts,
        schema_hash: request.new_schema_hash().clone(),
        migrations: plan.kernel.kernel.migrations.clone(),
        committed_at_ms: correction_now_ms()?,
        verify: plan.verify.clone(),
    })
}

fn insert_correction_load_mirror(client: &mut Client, receipt: &Receipt) -> Result<()> {
    let migrations_json = serde_json::to_string(&receipt.migrations)
        .map_err(|error| CdfError::data(error.to_string()))?;
    let receipt_json =
        serde_json::to_string(receipt).map_err(|error| CdfError::data(error.to_string()))?;
    let xid = receipt
        .transaction
        .as_ref()
        .and_then(|metadata| metadata.values.get("xid"))
        .ok_or_else(|| CdfError::internal("Postgres correction receipt missing xid"))?;
    let rows_written = i64::try_from(receipt.counts.rows_written)
        .map_err(|_| CdfError::internal("correction rows_written exceeds BIGINT"))?;
    let rows_inserted = Some(0_i64);
    let rows_updated = receipt
        .counts
        .rows_updated
        .map(i64::try_from)
        .transpose()
        .map_err(|_| CdfError::internal("correction rows_updated exceeds BIGINT"))?;
    let rows_deleted = Some(0_i64);
    let segment_count = i64::try_from(receipt.segment_acks.len())
        .map_err(|_| CdfError::internal("correction segment count exceeds BIGINT"))?;
    let duplicate = false;
    client
        .execute(
            &record_load_sql(),
            &[
                &receipt.receipt_id.as_str(),
                &receipt.target.as_str(),
                &receipt.package_hash.as_str(),
                &Option::<String>::None,
                &receipt.idempotency_token.as_str(),
                &disposition_name(&receipt.disposition),
                &receipt.schema_hash.as_str(),
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
        .map_err(|error| {
            correction_postgres_error("insert Postgres correction receipt mirror", error)
        })?;
    Ok(())
}

fn verify_correction_receipt_in_transaction(client: &mut Client, receipt: &Receipt) -> Result<()> {
    let row = client
        .query_opt(
            &receipt.verify.statement,
            &[
                &receipt.target.as_str(),
                &receipt.package_hash.as_str(),
                &receipt.idempotency_token.as_str(),
                &receipt.schema_hash.as_str(),
            ],
        )
        .map_err(|error| correction_postgres_error("verify Postgres correction receipt", error))?
        .ok_or_else(|| {
            CdfError::destination("correction receipt is absent from Postgres _cdf_loads")
        })?;
    let json: String = row.get("receipt_json");
    let stored: Receipt = serde_json::from_str(&json)
        .map_err(|error| CdfError::data(format!("decode stored correction receipt: {error}")))?;
    if &stored != receipt {
        return Err(CdfError::destination(
            "stored Postgres correction receipt differs from committed receipt",
        ));
    }
    DestinationCorrectionReceiptEvidence::from_receipt(&stored)?;
    Ok(())
}

fn execute_correction_statements(
    client: &mut Client,
    statements: &[PostgresStatement],
) -> Result<()> {
    for statement in statements {
        client.batch_execute(&statement.sql).map_err(|error| {
            correction_postgres_error(format!("execute {}", statement.name), error)
        })?;
    }
    Ok(())
}

fn set_correction_search_path(client: &mut Client, target: &PostgresTarget) -> Result<()> {
    let Some(schema) = &target.schema else {
        return Ok(());
    };
    client
        .batch_execute(&format!(
            "SET LOCAL search_path = {}, public",
            schema.quoted()
        ))
        .map_err(|error| correction_postgres_error("set Postgres correction search_path", error))
}

fn correction_now_ms() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| CdfError::internal(format!("system clock before UNIX_EPOCH: {error}")))?;
    i64::try_from(duration.as_millis())
        .map_err(|_| CdfError::internal("system time milliseconds exceed i64"))
}

fn correction_postgres_error(context: impl Into<String>, error: postgres::Error) -> CdfError {
    CdfError::destination(format!("{}: {}", context.into(), error))
}
