use crate::*;
use crate::{
    commit::*, corrections::*, mirrors::*, package::*, planning::*, receipts::*, sheet::*, sql::*,
    table::*,
};

#[derive(Clone, Debug)]
pub struct DuckDbDestination {
    database_path: PathBuf,
    sheet: DestinationSheet,
    native_resources: DuckDbNativeResources,
    pub(crate) pending_corrections: Arc<Mutex<BTreeMap<PlanId, DuckDbCorrectionContext>>>,
}

#[derive(Clone)]
struct DuckDbNativeResources {
    memory_limit_bytes: u64,
    maximum_temp_directory_bytes: u64,
    internal_threads: i64,
    scratch_reservation: Option<Arc<cdf_runtime::SpillReservation>>,
}

impl std::fmt::Debug for DuckDbNativeResources {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DuckDbNativeResources")
            .field("memory_limit_bytes", &self.memory_limit_bytes)
            .field(
                "maximum_temp_directory_bytes",
                &self.maximum_temp_directory_bytes,
            )
            .field("internal_threads", &self.internal_threads)
            .field(
                "scratch_reserved_bytes",
                &self
                    .scratch_reservation
                    .as_ref()
                    .map(|reservation| reservation.bytes()),
            )
            .finish()
    }
}

#[derive(Debug)]
struct DuckDbArrowWriter {
    conn: Connection,
    _lock: WriterLock,
    target: TargetRef,
    write_target: TargetRef,
    first_row_key: Option<u64>,
    next_row_key: Option<u64>,
    persisted_fields: Vec<FieldPlan>,
    user_field_count: usize,
    rows_received: u64,
    duckdb_version: String,
}

#[derive(Debug)]
struct DuckDbStagedIngressSession {
    destination: DuckDbDestination,
    request: cdf_runtime::StagedIngressRequest,
    writer: Option<DuckDbArrowWriter>,
    migrations: Vec<MigrationRecord>,
    accepted: Vec<cdf_runtime::StagedSegmentIdentity>,
}

#[derive(Debug)]
pub(crate) struct DuckDbCorrectionSession<'a> {
    pub(crate) destination: &'a DuckDbDestination,
    pub(crate) context: DuckDbCorrectionContext,
    pub(crate) migrations_applied: bool,
    pub(crate) corrections_applied: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) struct DuckDbCommitPlan {
    pub(crate) kernel: CommitPlan,
    pub(crate) ddl: Vec<String>,
}

pub type ReceiptVerification = cdf_kernel::ReceiptVerification;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcuProbe {
    pub available: bool,
    pub statement: String,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DuckDbMirrorSnapshot {
    pub loads_table_present: bool,
    pub state_table_present: bool,
    pub loads: Vec<DuckDbMirrorLoadRow>,
    pub state: Vec<DuckDbMirrorStateRow>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DuckDbMirrorLoadRow {
    pub target: String,
    pub idempotency_token: String,
    pub package_hash: String,
    pub receipt_id: String,
    pub receipt_json: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DuckDbMirrorStateRow {
    pub target: String,
    pub package_hash: String,
    pub segment_id: String,
    pub scope_json: Option<String>,
    pub output_position_json: Option<String>,
    pub row_count: u64,
    pub byte_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FieldPlan {
    pub(crate) name: String,
    pub(crate) sql_type: String,
    pub(crate) nullable: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct TablePlan {
    pub(crate) target: TargetRef,
    pub(crate) ddl: Vec<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct ExistingColumn {
    pub(crate) data_type: String,
    pub(crate) nullable: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct TargetRef {
    pub(crate) schema: String,
    pub(crate) table: String,
}

pub(crate) struct ReceiptBuildContext<'a> {
    pub(crate) migrations: &'a [MigrationRecord],
    pub(crate) committed_at_ms: i64,
    pub(crate) duckdb_version: &'a str,
    pub(crate) database_path: &'a Path,
    pub(crate) lock_path: &'a Path,
}

impl TargetRef {
    pub(crate) fn sql_name(&self) -> String {
        if self.schema == MAIN_SCHEMA {
            quote_ident(&self.table)
        } else {
            format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.table))
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CellValue {
    pub(crate) value: Value,
    pub(crate) key: CellKey,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum CellKey {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F32(u32),
    F64(u64),
    Text(String),
    Blob(Vec<u8>),
    Date32(i32),
    TimeMicros(i64),
    TimestampMicros(i64),
}

impl DuckDbDestination {
    pub fn new(database_path: impl AsRef<Path>) -> Result<Self> {
        let database_path = database_path.as_ref().to_path_buf();
        if database_path.as_os_str().is_empty() {
            return Err(CdfError::contract("DuckDB database path cannot be empty"));
        }

        Ok(Self {
            database_path,
            sheet: duckdb_sheet()?,
            native_resources: DuckDbNativeResources::conservative(),
            pending_corrections: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    pub fn with_execution_services(
        mut self,
        execution: &cdf_runtime::ExecutionServices,
    ) -> Result<Self> {
        self.native_resources = DuckDbNativeResources::for_execution(execution)?;
        Ok(self)
    }

    pub fn database_path(&self) -> &Path {
        &self.database_path
    }

    pub(crate) fn plan_schema_commit(
        &self,
        request: &DestinationCommitRequest,
        schema: &Schema,
    ) -> Result<DuckDbCommitPlan> {
        validate_user_schema_fields(schema)?;
        let fields = schema
            .fields()
            .iter()
            .map(|field| field_plan(field.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        validate_field_names(&fields)?;
        let fields = persistence_fields(&fields);
        let target = parse_target(&request.target)?;
        let table_plan = if self.database_path.exists() {
            let conn = self.open_read_only_connection()?;
            plan_table(&conn, target, &fields, request.disposition.clone())?
        } else {
            plan_absent_table(target, &fields, request.disposition.clone())?
        };
        let mut kernel = self.plan_commit(request)?;
        kernel.migrations = table_plan
            .ddl
            .iter()
            .enumerate()
            .map(|(index, ddl)| MigrationRecord {
                migration_id: format!("duckdb-ddl-{:03}", index + 1),
                description: ddl.clone(),
            })
            .collect();
        Ok(DuckDbCommitPlan {
            kernel,
            ddl: table_plan.ddl,
        })
    }

    fn start_staged_writer(
        &self,
        request: &cdf_runtime::StagedIngressRequest,
    ) -> Result<(DuckDbArrowWriter, Vec<MigrationRecord>)> {
        validate_user_schema_fields(request.output_schema())?;
        let user_fields = request
            .output_schema()
            .fields()
            .iter()
            .map(|field| field_plan(field.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        validate_field_names(&user_fields)?;
        let persisted_fields = persistence_fields(&user_fields);
        let target = parse_target(&request.binding().target)?;
        let lock = self.acquire_writer_lock()?;
        let conn = self.open_connection()?;
        ensure_mirror_tables(&conn)?;
        conn.execute_batch("BEGIN TRANSACTION")
            .map_err(|error| duckdb_error("begin staged Arrow transaction", error))?;
        let table_plan = plan_table(
            &conn,
            target,
            &persisted_fields,
            request.binding().disposition.clone(),
        )?;
        let migrations = table_plan
            .ddl
            .iter()
            .enumerate()
            .map(|(index, ddl)| MigrationRecord {
                migration_id: format!("duckdb-ddl-{:03}", index + 1),
                description: ddl.clone(),
            })
            .collect::<Vec<_>>();
        apply_table_plan(&conn, &table_plan, request.binding().disposition.clone())?;
        let write_target = if request.binding().disposition == WriteDisposition::Merge {
            let staging = TargetRef {
                schema: MAIN_SCHEMA.to_owned(),
                table: staging_table_name(),
            };
            let mut staging_fields = persisted_fields.clone();
            staging_fields.push(FieldPlan {
                name: CDF_STAGE_ORDER_COLUMN.to_owned(),
                sql_type: "UBIGINT".to_owned(),
                nullable: false,
            });
            conn.execute_batch(&format!(
                "CREATE TEMP TABLE {} ({})",
                quote_ident(&staging.table),
                create_columns_sql(&staging_fields)
            ))
            .map_err(|error| duckdb_error("create staged DuckDB merge table", error))?;
            staging
        } else {
            table_plan.target.clone()
        };
        let first_row_key = next_row_key(&conn)?;
        let duckdb_version = duckdb_version(&conn).unwrap_or_else(|_| "unknown".to_owned());
        Ok((
            DuckDbArrowWriter {
                conn,
                _lock: lock,
                target: table_plan.target,
                write_target,
                first_row_key: Some(first_row_key),
                next_row_key: Some(first_row_key),
                persisted_fields,
                user_field_count: user_fields.len(),
                rows_received: 0,
                duckdb_version,
            },
            migrations,
        ))
    }

    pub(crate) fn begin_staged_ingress_session(
        &self,
        request: cdf_runtime::StagedIngressRequest,
    ) -> Result<Box<dyn cdf_runtime::StagedIngressSession>> {
        if request.binding().destination_id.as_str() != DESTINATION_ID {
            return Err(CdfError::contract(
                "DuckDB staged ingress destination authority mismatch",
            ));
        }
        if request.binding().disposition == WriteDisposition::CdcApply {
            return Err(CdfError::contract(
                "DuckDB destination does not support cdc_apply",
            ));
        }
        Ok(Box::new(DuckDbStagedIngressSession {
            destination: self.clone(),
            request,
            writer: None,
            migrations: Vec::new(),
            accepted: Vec::new(),
        }))
    }

    pub fn verify_receipt(&self, receipt: &Receipt) -> Result<ReceiptVerification> {
        let conn = self.open_connection()?;
        ensure_mirror_tables(&conn)?;

        let stored_json: Option<String> = conn
            .query_row(
                "SELECT receipt_json FROM _cdf_loads WHERE target = ? AND idempotency_token = ? AND package_hash = ?",
                params![
                    receipt.target.as_str(),
                    receipt.idempotency_token.as_str(),
                    receipt.package_hash.as_str()
                ],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| duckdb_error("query receipt verification mirror", error))?;

        let Some(stored_json) = stored_json else {
            return Ok(ReceiptVerification {
                verified: false,
                receipt_id: receipt.receipt_id.clone(),
                reason: Some("receipt is absent from _cdf_loads".to_owned()),
            });
        };

        let stored: Receipt = serde_json::from_str(&stored_json).map_err(json_error)?;
        if &stored == receipt {
            Ok(ReceiptVerification {
                verified: true,
                receipt_id: receipt.receipt_id.clone(),
                reason: None,
            })
        } else {
            Ok(ReceiptVerification {
                verified: false,
                receipt_id: receipt.receipt_id.clone(),
                reason: Some("stored receipt JSON differs from supplied receipt".to_owned()),
            })
        }
    }

    pub fn probe_icu(&self) -> Result<IcuProbe> {
        let statement = "SELECT length(icu_sort_key('CDF', 'en_US')) > 0".to_owned();
        let conn = self.open_connection()?;
        match conn.query_row::<bool, _, _>(&statement, [], |row| row.get(0)) {
            Ok(true) => Ok(IcuProbe {
                available: true,
                statement,
                error: None,
            }),
            Ok(false) => Ok(IcuProbe {
                available: false,
                statement,
                error: Some("ICU probe returned false".to_owned()),
            }),
            Err(error) => Ok(IcuProbe {
                available: false,
                statement,
                error: Some(error.to_string()),
            }),
        }
    }

    pub fn read_mirror_snapshot_read_only(&self) -> Result<DuckDbMirrorSnapshot> {
        let conn = self.open_read_only_connection()?;
        read_mirror_snapshot(&conn)
    }

    pub(crate) fn open_connection(&self) -> Result<Connection> {
        Connection::open_with_flags(
            &self.database_path,
            bounded_connection_config(&self.native_resources, false)?,
        )
        .map_err(|error| duckdb_error(format!("open {}", self.database_path.display()), error))
    }

    pub(crate) fn open_read_only_connection(&self) -> Result<Connection> {
        let config = bounded_connection_config(&self.native_resources, true)?;
        Connection::open_with_flags(&self.database_path, config).map_err(|error| {
            duckdb_error(
                format!("open {} read-only", self.database_path.display()),
                error,
            )
        })
    }

    pub(crate) fn acquire_writer_lock(&self) -> Result<WriterLock> {
        WriterLock::acquire(self.lock_path())
    }

    pub(crate) fn lock_path(&self) -> PathBuf {
        let file_name = self
            .database_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("duckdb");
        self.database_path
            .with_file_name(format!("{file_name}.{LOCK_SUFFIX}"))
    }
}

impl DuckDbNativeResources {
    fn conservative() -> Self {
        Self {
            memory_limit_bytes: DUCKDB_MINIMUM_NATIVE_MEMORY_BYTES,
            maximum_temp_directory_bytes: DUCKDB_MAXIMUM_TEMP_DIRECTORY_BYTES,
            internal_threads: DUCKDB_INTERNAL_THREADS,
            scratch_reservation: None,
        }
    }

    fn for_execution(execution: &cdf_runtime::ExecutionServices) -> Result<Self> {
        Self::for_budgets(
            execution.memory().snapshot().budget_bytes,
            execution.spill(),
        )
    }

    fn for_budgets(
        managed_budget: u64,
        spill: Arc<dyn cdf_runtime::SpillBudgetCoordinator>,
    ) -> Result<Self> {
        let memory_limit_bytes = (managed_budget / 4).clamp(
            DUCKDB_MINIMUM_NATIVE_MEMORY_BYTES,
            DUCKDB_MAXIMUM_NATIVE_MEMORY_BYTES,
        );
        let maximum_temp_directory_bytes = spill
            .snapshot()
            .budget_bytes
            .min(DUCKDB_MAXIMUM_TEMP_DIRECTORY_BYTES);
        let scratch_reservation = spill
            .try_reserve(maximum_temp_directory_bytes)?
            .ok_or_else(|| {
                CdfError::data(format!(
                    "DuckDB destination requires {maximum_temp_directory_bytes} bytes of reserved scratch disk but the shared spill budget is already committed; increase the spill budget or reduce concurrent spool/sort work"
                ))
            })?;
        Ok(Self {
            memory_limit_bytes,
            maximum_temp_directory_bytes,
            internal_threads: DUCKDB_INTERNAL_THREADS,
            scratch_reservation: Some(Arc::new(scratch_reservation)),
        })
    }
}

fn bounded_connection_config(resources: &DuckDbNativeResources, read_only: bool) -> Result<Config> {
    let memory_limit = format!("{}B", resources.memory_limit_bytes);
    let maximum_temp_directory = format!("{}B", resources.maximum_temp_directory_bytes);
    let mut config = Config::default()
        .max_memory(&memory_limit)
        .and_then(|config| config.threads(resources.internal_threads))
        .and_then(|config| config.with("max_temp_directory_size", &maximum_temp_directory))
        .and_then(|config| config.with("preserve_insertion_order", "false"))
        .map_err(|error| duckdb_error("configure bounded DuckDB runtime", error))?;
    if read_only {
        config = config
            .access_mode(AccessMode::ReadOnly)
            .map_err(|error| duckdb_error("configure read-only DuckDB open", error))?;
    }
    Ok(config)
}

impl DuckDbStagedIngressSession {
    fn validate_final_binding(&self, binding: &cdf_runtime::VerifiedFinalBinding) -> Result<()> {
        if binding.attempt_id() != self.request.attempt_id()
            || binding.execution_plan_id() != &self.request.binding().execution_plan_id
            || binding.commit().target != self.request.binding().target
            || binding.commit().disposition != self.request.binding().disposition
            || binding.schema_hash() != &self.request.binding().schema_hash
            || binding.output_arrow_schema_hash()
                != &self.request.binding().output_arrow_schema_hash
            || binding.merge_keys() != self.request.binding().merge_keys
        {
            return Err(CdfError::destination(
                "DuckDB staged ingress final binding differs from its attempt authority",
            ));
        }
        binding.validate_staged_identities(&self.accepted)
    }

    fn bind_empty(
        self,
        binding: cdf_runtime::VerifiedFinalBinding,
    ) -> Result<cdf_runtime::DestinationCommitOutcome> {
        if !binding.ordered_segments().is_empty() {
            return Err(CdfError::internal(
                "DuckDB staged empty binding received data segments",
            ));
        }
        self.request.mutation_guard().assert_current()?;
        let lock = self.destination.acquire_writer_lock()?;
        let conn = self.destination.open_connection()?;
        ensure_mirror_tables(&conn)?;
        if let Some(receipt) = find_duplicate_receipt(&conn, binding.commit())? {
            return Ok(cdf_runtime::DestinationCommitOutcome::new(
                receipt,
                cdf_runtime::DestinationReceiptReportingPolicy::DestinationCommit {
                    duplicate: true,
                },
            ));
        }
        let duckdb_version = duckdb_version(&conn).unwrap_or_else(|_| "unknown".to_owned());
        let committed_at_ms = now_ms()?;
        let receipt = build_receipt(
            binding.commit(),
            binding.schema_hash(),
            &[],
            CommitCounts::default(),
            &ReceiptBuildContext {
                migrations: &[],
                committed_at_ms,
                duckdb_version: &duckdb_version,
                database_path: &self.destination.database_path,
                lock_path: &self.destination.lock_path(),
            },
        )?;
        conn.execute_batch("BEGIN TRANSACTION")
            .map_err(|error| duckdb_error("begin empty staged transaction", error))?;
        insert_mirrors(&conn, binding.commit(), &[], &receipt, None)?;
        conn.execute_batch("COMMIT")
            .map_err(|error| duckdb_error("commit empty staged transaction", error))?;
        drop(lock);
        Ok(cdf_runtime::DestinationCommitOutcome::new(
            receipt,
            cdf_runtime::DestinationReceiptReportingPolicy::DestinationCommit { duplicate: false },
        ))
    }
}

impl cdf_runtime::StagedIngressSession for DuckDbStagedIngressSession {
    fn stage_stream(&mut self, stream: &mut dyn cdf_runtime::StagedSegmentStream) -> Result<()> {
        let Some(first_segment) = stream.next_segment()? else {
            return Ok(());
        };
        if self.writer.is_none() {
            let (writer, migrations) = self.destination.start_staged_writer(&self.request)?;
            self.writer = Some(writer);
            self.migrations = migrations;
        }
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| CdfError::internal("DuckDB staged writer is not initialized"))?;
        let merge = self.request.binding().disposition == WriteDisposition::Merge;
        let mut column_names = writer
            .persisted_fields
            .iter()
            .map(|field| field.name.clone())
            .collect::<Vec<_>>();
        if merge {
            column_names.push(CDF_STAGE_ORDER_COLUMN.to_owned());
        }
        let write_target = writer.write_target.clone();
        let mut appender = open_arrow_appender(&writer.conn, &write_target, &column_names)?;
        let mut current = Some(first_segment);
        while let Some(mut segment) = current {
            self.request.mutation_guard().assert_current()?;
            let identity = segment.identity.clone();
            if identity.schema_hash != self.request.binding().schema_hash {
                return Err(CdfError::data(
                    "DuckDB staged segment schema hash differs from its attempt",
                ));
            }
            let expected_ordinal = u32::try_from(self.accepted.len())
                .map_err(|_| CdfError::data("DuckDB staged segment count exceeds u32"))?;
            if identity.ordinal != expected_ordinal
                || self
                    .accepted
                    .iter()
                    .any(|accepted| accepted.segment_id == identity.segment_id)
            {
                return Err(CdfError::data(
                    "DuckDB staged segments must be unique and arrive in canonical order",
                ));
            }
            if identity.row_count == 0 {
                return Err(CdfError::data(
                    "DuckDB staged data segment must contain at least one row",
                ));
            }
            let mut next_row_key = writer.next_row_key.ok_or_else(|| {
                CdfError::internal("DuckDB staged row-key allocator is not initialized")
            })?;
            let segment_rows_before = writer.rows_received;
            while let Some(batch) = segment.reader_mut().next_batch()? {
                if batch.schema().as_ref() != self.request.output_schema() {
                    return Err(CdfError::data(format!(
                        "DuckDB staged segment {} schema differs from the planned output schema",
                        identity.segment_id
                    )));
                }
                let batch_rows = u64::try_from(batch.num_rows())
                    .map_err(|_| CdfError::data("DuckDB staged batch rows exceed u64"))?;
                let persisted =
                    persistence_batch(batch, next_row_key, merge.then_some(writer.rows_received))?;
                self.request.mutation_guard().assert_current()?;
                append_arrow_batch(&mut appender, &write_target, persisted)?;
                next_row_key = next_row_key
                    .checked_add(batch_rows)
                    .ok_or_else(|| CdfError::data("DuckDB staged row key overflowed"))?;
                writer.rows_received = writer
                    .rows_received
                    .checked_add(batch_rows)
                    .ok_or_else(|| CdfError::data("DuckDB staged row count overflowed"))?;
            }
            if writer.rows_received.saturating_sub(segment_rows_before) != identity.row_count {
                return Err(CdfError::data(format!(
                    "DuckDB staged segment {} row count differs from durable identity",
                    identity.segment_id
                )));
            }
            writer.next_row_key = Some(next_row_key);
            stream.acknowledge(cdf_runtime::StagedSegmentAck {
                attempt_id: self.request.attempt_id().clone(),
                identity: identity.clone(),
                external_durable: false,
            })?;
            self.accepted.push(identity);
            drop(segment);
            current = stream.next_segment()?;
        }
        flush_arrow_appender(&mut appender, &write_target)?;
        Ok(())
    }

    fn snapshot(&self) -> Result<cdf_runtime::StagingSnapshot> {
        Ok(cdf_runtime::StagingSnapshot {
            attempt_id: self.request.attempt_id().clone(),
            binding: self.request.binding().clone(),
            recovery: cdf_runtime::StagingRecoveryMode::RollbackRedrive,
            accepted_segments: self.accepted.clone(),
        })
    }

    fn bind_final(
        mut self: Box<Self>,
        binding: cdf_runtime::VerifiedFinalBinding,
    ) -> Result<cdf_runtime::DestinationCommitOutcome> {
        self.validate_final_binding(&binding)?;
        self.request.mutation_guard().assert_current()?;
        let Some(writer) = self.writer.take() else {
            return (*self).bind_empty(binding);
        };
        if let Some(receipt) = find_duplicate_receipt(&writer.conn, binding.commit())? {
            writer
                .conn
                .execute_batch("ROLLBACK")
                .map_err(|error| duckdb_error("rollback duplicate staged transaction", error))?;
            return Ok(cdf_runtime::DestinationCommitOutcome::new(
                receipt,
                cdf_runtime::DestinationReceiptReportingPolicy::DestinationCommit {
                    duplicate: true,
                },
            ));
        }
        let counts = match binding.commit().disposition {
            WriteDisposition::Append | WriteDisposition::Replace => CommitCounts {
                rows_written: writer.rows_received,
                rows_inserted: Some(writer.rows_received),
                rows_updated: Some(0),
                rows_deleted: Some(0),
            },
            WriteDisposition::Merge => finalize_arrow_merge(
                &writer.conn,
                &writer.target,
                &writer.write_target,
                &writer.persisted_fields,
                writer.user_field_count,
                &self.request.binding().merge_keys,
            )?,
            WriteDisposition::CdcApply => {
                return Err(CdfError::contract(
                    "DuckDB destination does not support cdc_apply",
                ));
            }
        };
        let segment_acks = self
            .accepted
            .iter()
            .map(|identity| SegmentAck {
                segment_id: identity.segment_id.clone(),
                row_count: identity.row_count,
                byte_count: identity.byte_count,
            })
            .collect::<Vec<_>>();
        let committed_at_ms = now_ms()?;
        let receipt = build_receipt(
            binding.commit(),
            binding.schema_hash(),
            &segment_acks,
            counts,
            &ReceiptBuildContext {
                migrations: &self.migrations,
                committed_at_ms,
                duckdb_version: &writer.duckdb_version,
                database_path: &self.destination.database_path,
                lock_path: &self.destination.lock_path(),
            },
        )?;
        advance_row_key_allocator(
            &writer.conn,
            writer
                .first_row_key
                .ok_or_else(|| CdfError::internal("DuckDB staged first row key is absent"))?,
            writer
                .next_row_key
                .ok_or_else(|| CdfError::internal("DuckDB staged next row key is absent"))?,
        )?;
        insert_mirrors(
            &writer.conn,
            binding.commit(),
            &segment_acks,
            &receipt,
            writer.first_row_key,
        )?;
        self.request.mutation_guard().assert_current()?;
        writer
            .conn
            .execute_batch("COMMIT")
            .map_err(|error| duckdb_error("commit staged Arrow transaction", error))?;
        Ok(cdf_runtime::DestinationCommitOutcome::new(
            receipt,
            cdf_runtime::DestinationReceiptReportingPolicy::DestinationCommit { duplicate: false },
        ))
    }

    fn abort(mut self: Box<Self>) -> Result<()> {
        if let Some(writer) = self.writer.take() {
            writer
                .conn
                .execute_batch("ROLLBACK")
                .map_err(|error| duckdb_error("rollback staged Arrow transaction", error))?;
        }
        Ok(())
    }
}

impl DestinationProtocol for DuckDbDestination {
    fn sheet(&self) -> &DestinationSheet {
        &self.sheet
    }

    fn protocol_capabilities(&self) -> cdf_kernel::DestinationProtocolCapabilities {
        cdf_kernel::DestinationProtocolCapabilities::default()
            .with_corrections(duckdb_correction_capabilities())
    }

    fn plan_commit(&self, request: &DestinationCommitRequest) -> Result<CommitPlan> {
        if !self
            .sheet
            .supported_dispositions
            .contains(&request.disposition)
        {
            let disposition = match request.disposition {
                WriteDisposition::Append => "append",
                WriteDisposition::Merge => "merge",
                WriteDisposition::Replace => "replace",
                WriteDisposition::CdcApply => "cdc_apply",
            };
            return Err(CdfError::contract(format!(
                "DuckDB destination does not support {disposition}"
            )));
        }

        Ok(CommitPlan {
            plan_id: PlanId::new(format!(
                "duckdb-plan:{}:{}",
                request.target.as_str(),
                request.idempotency_token.as_str()
            ))?,
            target: request.target.clone(),
            disposition: request.disposition.clone(),
            idempotency: IdempotencySupport::PackageToken,
            migrations: Vec::new(),
            delivery_guarantee: match request.disposition {
                WriteDisposition::Append => DeliveryGuarantee::EffectivelyOncePerPackage,
                WriteDisposition::Replace => DeliveryGuarantee::EffectivelyOncePerTarget,
                WriteDisposition::Merge => DeliveryGuarantee::EffectivelyOncePerKey,
                WriteDisposition::CdcApply => DeliveryGuarantee::EffectivelyOncePerPosition,
            },
        })
    }

    fn verify(&self, receipt: &Receipt) -> Result<ReceiptVerification> {
        self.verify_receipt(receipt)
    }

    fn plan_correction(
        &self,
        request: &DestinationCorrectionCommitRequest,
    ) -> Result<DestinationCorrectionCommitPlan> {
        plan_correction_request(self, request)
    }

    fn begin_correction(
        &self,
        request: DestinationCorrectionCommitRequest,
        plan: DestinationCorrectionCommitPlan,
    ) -> Result<Box<dyn CorrectionCommitSession + '_>> {
        begin_correction_request(self, request, plan)
    }

    fn verify_correction(&self, receipt: &Receipt) -> Result<ReceiptVerification> {
        verify_correction_receipt(self, receipt)
    }

    fn read_correction_residual(
        &self,
        target: &TargetName,
        original_row: &RowProvenanceAddress,
    ) -> Result<Option<DestinationResidualReadback>> {
        read_addressed_residual(self, target, original_row)
    }
}

#[cfg(test)]
mod native_resource_tests {
    use super::*;
    use cdf_runtime::SpillBudgetCoordinator as _;

    #[test]
    fn execution_resources_reserve_and_release_bounded_scratch_capacity() {
        let spill = Arc::new(cdf_runtime::FixedSpillBudget::new(2 * 1024 * 1024 * 1024).unwrap());
        let coordinator: Arc<dyn cdf_runtime::SpillBudgetCoordinator> = spill.clone();
        let resources =
            DuckDbNativeResources::for_budgets(4 * 1024 * 1024 * 1024, coordinator).unwrap();
        assert_eq!(resources.memory_limit_bytes, 1024 * 1024 * 1024);
        assert_eq!(resources.maximum_temp_directory_bytes, 1024 * 1024 * 1024);
        assert_eq!(spill.snapshot().current_bytes, 1024 * 1024 * 1024);

        let clone = resources.clone();
        drop(resources);
        assert_eq!(spill.snapshot().current_bytes, 1024 * 1024 * 1024);
        drop(clone);
        assert_eq!(spill.snapshot().current_bytes, 0);
    }

    #[test]
    fn execution_resources_fail_before_use_when_scratch_is_unavailable() {
        let spill = Arc::new(cdf_runtime::FixedSpillBudget::new(1024).unwrap());
        let held = spill.try_reserve(1024).unwrap().unwrap();
        let coordinator: Arc<dyn cdf_runtime::SpillBudgetCoordinator> = spill.clone();
        let error =
            DuckDbNativeResources::for_budgets(DUCKDB_MINIMUM_NATIVE_MEMORY_BYTES, coordinator)
                .unwrap_err();
        assert!(
            error
                .message
                .contains("shared spill budget is already committed")
        );
        drop(held);
        assert_eq!(spill.snapshot().current_bytes, 0);
    }
}
