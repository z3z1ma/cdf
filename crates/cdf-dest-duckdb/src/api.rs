use crate::*;
use crate::{
    commit::*, corrections::*, mirrors::*, package::*, planning::*, receipts::*, sheet::*, sql::*,
    table::*,
};

#[derive(Clone, Debug)]
pub struct DuckDbDestination {
    database_path: PathBuf,
    sheet: DestinationSheet,
    execution: Option<cdf_runtime::ExecutionServices>,
    // 10x: kernel begin lacks DuckDB package inputs; remove this handoff once begin carries package replay inputs.
    pending_sessions: Arc<Mutex<BTreeMap<PlanId, DuckDbSessionContext>>>,
    pub(crate) pending_corrections: Arc<Mutex<BTreeMap<PlanId, DuckDbCorrectionContext>>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DuckDbCapabilities {
    pub sheet: DestinationSheet,
    pub bulk_paths: Vec<BulkPath>,
    pub single_writer_lock: String,
    pub parquet_replay: CapabilitySupport,
    pub timezone_support: TimezoneSupport,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BulkPath {
    ArrowRecordBatchAppender,
    ParquetScan,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimezoneSupport {
    pub requires_icu_probe: bool,
    pub timezone_aware_timestamps: CapabilitySupport,
}

#[derive(Clone, Debug)]
pub struct DuckDbCommitRequest {
    pub package_dir: PathBuf,
    pub commit: DestinationCommitRequest,
    pub schema_hash: SchemaHash,
    pub merge_keys: Vec<String>,
}

#[derive(Debug)]
struct DuckDbCommitSession<'a> {
    destination: &'a DuckDbDestination,
    request: DuckDbCommitRequest,
    schema: Option<SchemaRef>,
    plan: DuckDbCommitPlan,
    migrations_applied: bool,
    expected_segments: BTreeMap<cdf_kernel::SegmentId, ExpectedSegment>,
    expected_order: Vec<cdf_kernel::SegmentId>,
    accepted_segments: BTreeSet<cdf_kernel::SegmentId>,
    next_expected: usize,
    duplicate_receipt: Option<Receipt>,
    writer: Option<DuckDbArrowWriter>,
}

#[derive(Clone, Debug)]
struct DuckDbSessionContext {
    request: DuckDbCommitRequest,
    schema: Option<SchemaRef>,
    plan: DuckDbCommitPlan,
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

#[derive(Clone, Debug)]
struct ExpectedSegment {
    state: StateSegment,
    package_byte_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct DuckDbCommitPlan {
    pub kernel: CommitPlan,
    pub ddl: Vec<String>,
    pub effect: DuckDbCommitEffect,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DuckDbCommitEffect {
    Data {
        bulk_path: BulkPath,
        target_exists: bool,
    },
    NoData,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DuckDbCommitOutcome {
    pub receipt: Receipt,
    pub duplicate: bool,
    pub plan: DuckDbCommitPlan,
    pub package_receipt_recorded: bool,
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
    pub(crate) target_exists: bool,
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
        Self::new_with_execution(database_path, None)
    }

    pub(crate) fn new_with_execution(
        database_path: impl AsRef<Path>,
        execution: Option<cdf_runtime::ExecutionServices>,
    ) -> Result<Self> {
        let database_path = database_path.as_ref().to_path_buf();
        if database_path.as_os_str().is_empty() {
            return Err(CdfError::contract("DuckDB database path cannot be empty"));
        }

        Ok(Self {
            database_path,
            sheet: duckdb_sheet()?,
            execution,
            pending_sessions: Arc::new(Mutex::new(BTreeMap::new())),
            pending_corrections: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    pub fn database_path(&self) -> &Path {
        &self.database_path
    }

    pub fn capabilities(&self) -> DuckDbCapabilities {
        DuckDbCapabilities {
            sheet: self.sheet.clone(),
            bulk_paths: vec![BulkPath::ArrowRecordBatchAppender],
            single_writer_lock: self.lock_path().display().to_string(),
            parquet_replay: CapabilitySupport::Unsupported,
            timezone_support: TimezoneSupport {
                requires_icu_probe: true,
                timezone_aware_timestamps: CapabilitySupport::Unsupported,
            },
        }
    }

    pub fn plan_package_commit(&self, request: &DuckDbCommitRequest) -> Result<DuckDbCommitPlan> {
        let reader = PackageReader::open(&request.package_dir)?;
        let schema = if request
            .package_dir
            .join(cdf_package::RUNTIME_ARROW_SCHEMA_FILE)
            .exists()
        {
            reader.runtime_arrow_schema()?
        } else {
            let first =
                reader.manifest().identity.segments.first().ok_or_else(|| {
                    CdfError::data("DuckDB package has no segment schema authority")
                })?;
            reader
                .read_segment(&first.segment_id)?
                .into_iter()
                .next()
                .map(|batch| batch.schema())
                .ok_or_else(|| CdfError::data("DuckDB package first segment has no Arrow batch"))?
        };
        let plan = self.plan_schema_commit(&request.commit, schema.as_ref())?;
        self.remember_session_context(request, Some(schema), &plan)?;
        Ok(plan)
    }

    pub fn plan_empty_package_commit(
        &self,
        request: &DuckDbCommitRequest,
    ) -> Result<DuckDbCommitPlan> {
        if !request.commit.segments.is_empty() {
            return Err(CdfError::contract(
                "empty DuckDB package planning requires zero data segments",
            ));
        }
        let plan = DuckDbCommitPlan {
            kernel: self.plan_commit(&request.commit)?,
            ddl: Vec::new(),
            effect: DuckDbCommitEffect::NoData,
        };
        self.remember_session_context(request, None, &plan)?;
        Ok(plan)
    }

    pub fn plan_schema_commit(
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
            effect: DuckDbCommitEffect::Data {
                bulk_path: BulkPath::ArrowRecordBatchAppender,
                target_exists: table_plan.target_exists,
            },
        })
    }

    fn start_staged_writer(
        &self,
        request: &cdf_runtime::StagedIngressRequest,
    ) -> Result<(DuckDbArrowWriter, Vec<MigrationRecord>)> {
        validate_user_schema_fields(&request.output_schema)?;
        let user_fields = request
            .output_schema
            .fields()
            .iter()
            .map(|field| field_plan(field.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        validate_field_names(&user_fields)?;
        let persisted_fields = persistence_fields(&user_fields);
        let target = parse_target(&request.binding.target)?;
        let lock = self.acquire_writer_lock()?;
        let conn = self.open_connection()?;
        ensure_mirror_tables(&conn)?;
        conn.execute_batch("BEGIN TRANSACTION")
            .map_err(|error| duckdb_error("begin staged Arrow transaction", error))?;
        let table_plan = plan_table(
            &conn,
            target,
            &persisted_fields,
            request.binding.disposition.clone(),
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
        apply_table_plan(&conn, &table_plan, request.binding.disposition.clone())?;
        let write_target = if request.binding.disposition == WriteDisposition::Merge {
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
        let duckdb_version = duckdb_version(&conn).unwrap_or_else(|_| "unknown".to_owned());
        Ok((
            DuckDbArrowWriter {
                conn,
                _lock: lock,
                target: table_plan.target,
                write_target,
                first_row_key: None,
                next_row_key: None,
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
        if request.binding.destination_id.as_str() != DESTINATION_ID {
            return Err(CdfError::contract(
                "DuckDB staged ingress destination authority mismatch",
            ));
        }
        if request.binding.disposition == WriteDisposition::CdcApply {
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

    pub fn commit_package(&self, request: DuckDbCommitRequest) -> Result<DuckDbCommitOutcome> {
        let reader = PackageReader::open(&request.package_dir)?;
        reader.verify()?;
        let plan = self.plan_package_commit(&request)?;
        let context = self.take_session_context(&plan.kernel.plan_id, &request.commit)?;
        let mut session = DuckDbCommitSession::new(self, context)?;
        session.apply_migrations()?;
        for state in &request.commit.segments {
            let entry = reader
                .manifest()
                .identity
                .segments
                .iter()
                .find(|entry| entry.segment_id == state.segment_id)
                .ok_or_else(|| {
                    CdfError::data(format!(
                        "DuckDB commit segment {} is absent from the package manifest",
                        state.segment_id
                    ))
                })?;
            let batches = reader.read_segment(&state.segment_id)?;
            session.write_segment(CommitSegment::new(state.clone(), entry.byte_count, batches))?;
        }
        session.finalize_outcome()
    }

    fn commit_empty_package(&self, request: DuckDbCommitRequest) -> Result<DuckDbCommitOutcome> {
        let Some(execution) = self.execution.clone() else {
            return self.commit_empty_package_inline(request);
        };
        let destination = self.clone();
        execution.run_blocking("duckdb.connection", move || {
            destination.commit_empty_package_inline(request)
        })
    }

    fn commit_empty_package_inline(
        &self,
        request: DuckDbCommitRequest,
    ) -> Result<DuckDbCommitOutcome> {
        if !request.commit.segments.is_empty() {
            return Err(CdfError::internal(
                "empty DuckDB commit path received data segments",
            ));
        }
        let lock = self.acquire_writer_lock()?;
        let mut conn = self.open_connection()?;
        ensure_mirror_tables(&conn)?;
        let plan = DuckDbCommitPlan {
            kernel: self.plan_commit(&request.commit)?,
            ddl: Vec::new(),
            effect: DuckDbCommitEffect::NoData,
        };
        if let Some(receipt) = find_duplicate_receipt(&conn, &request.commit)? {
            let recorded = record_package_receipt_once(&request.package_dir, &receipt)?;
            drop(lock);
            return Ok(DuckDbCommitOutcome {
                receipt,
                duplicate: true,
                plan,
                package_receipt_recorded: recorded,
            });
        }
        let duckdb_version = duckdb_version(&conn).unwrap_or_else(|_| "unknown".to_owned());
        let committed_at_ms = now_ms()?;
        let receipt = build_receipt(
            &request.commit,
            &request.schema_hash,
            &[],
            CommitCounts::default(),
            &ReceiptBuildContext {
                migrations: &[],
                committed_at_ms,
                duckdb_version: &duckdb_version,
                database_path: &self.database_path,
                lock_path: &self.lock_path(),
            },
        )?;
        {
            let tx = conn
                .transaction()
                .map_err(|error| duckdb_error("begin empty package transaction", error))?;
            insert_mirrors(&tx, &request.commit, &[], &receipt, None)?;
            tx.commit()
                .map_err(|error| duckdb_error("commit empty package transaction", error))?;
        }
        let recorded = record_package_receipt_once(&request.package_dir, &receipt)?;
        drop(lock);
        Ok(DuckDbCommitOutcome {
            receipt,
            duplicate: false,
            plan,
            package_receipt_recorded: recorded,
        })
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
        Connection::open(&self.database_path)
            .map_err(|error| duckdb_error(format!("open {}", self.database_path.display()), error))
    }

    pub(crate) fn open_read_only_connection(&self) -> Result<Connection> {
        let config = Config::default()
            .access_mode(AccessMode::ReadOnly)
            .map_err(|error| duckdb_error("configure read-only DuckDB open", error))?;
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

    fn remember_session_context(
        &self,
        request: &DuckDbCommitRequest,
        schema: Option<SchemaRef>,
        plan: &DuckDbCommitPlan,
    ) -> Result<()> {
        let mut pending = self
            .pending_sessions
            .lock()
            .map_err(|_| CdfError::internal("DuckDB commit-session context cache is poisoned"))?;
        pending.insert(
            plan.kernel.plan_id.clone(),
            DuckDbSessionContext {
                request: request.clone(),
                schema,
                plan: plan.clone(),
            },
        );
        Ok(())
    }

    fn take_session_context(
        &self,
        plan_id: &PlanId,
        request: &DestinationCommitRequest,
    ) -> Result<DuckDbSessionContext> {
        let mut pending = self
            .pending_sessions
            .lock()
            .map_err(|_| CdfError::internal("DuckDB commit-session context cache is poisoned"))?;
        let Some(duckdb_request) = pending.get(plan_id).cloned() else {
            return Err(CdfError::contract(
                "DuckDB DestinationProtocol::begin requires a prior plan_package_commit for the same package plan",
            ));
        };
        if duckdb_request.request.commit != *request {
            return Err(CdfError::contract(
                "DuckDB DestinationProtocol::begin request does not match the planned package commit",
            ));
        }
        pending.remove(plan_id);
        Ok(duckdb_request)
    }
}

impl DuckDbCommitSession<'_> {
    fn new(
        destination: &DuckDbDestination,
        context: DuckDbSessionContext,
    ) -> Result<DuckDbCommitSession<'_>> {
        let (expected_segments, expected_order) = expected_segments_for_request(&context.request)?;
        Ok(DuckDbCommitSession {
            destination,
            request: context.request,
            schema: context.schema,
            plan: context.plan,
            migrations_applied: false,
            expected_segments,
            expected_order,
            accepted_segments: BTreeSet::new(),
            next_expected: 0,
            duplicate_receipt: None,
            writer: None,
        })
    }

    fn start_writer(&mut self) -> Result<()> {
        if self.expected_segments.is_empty() {
            return Ok(());
        }
        let lock = self.destination.acquire_writer_lock()?;
        let conn = self.destination.open_connection()?;
        ensure_mirror_tables(&conn)?;
        if let Some(receipt) = find_duplicate_receipt(&conn, &self.request.commit)? {
            self.duplicate_receipt = Some(receipt);
            return Ok(());
        }
        let schema = self.schema.as_ref().ok_or_else(|| {
            CdfError::internal("DuckDB data commit session is missing its runtime Arrow schema")
        })?;
        validate_user_schema_fields(schema)?;
        let user_fields = schema
            .fields()
            .iter()
            .map(|field| field_plan(field.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        validate_field_names(&user_fields)?;
        let persisted_fields = persistence_fields(&user_fields);
        let table_plan = plan_table_from_commit_plan(&self.plan)?;
        let duckdb_version = duckdb_version(&conn).unwrap_or_else(|_| "unknown".to_owned());
        conn.execute_batch("BEGIN TRANSACTION")
            .map_err(|error| duckdb_error("begin Arrow commit transaction", error))?;
        let total_rows = self
            .expected_segments
            .values()
            .try_fold(0_u64, |total, segment| {
                total
                    .checked_add(segment.state.row_count)
                    .ok_or_else(|| CdfError::data("DuckDB package row count overflowed u64"))
            })?;
        let first_row_key = allocate_row_keys(&conn, total_rows)?.ok_or_else(|| {
            CdfError::internal("DuckDB non-empty package did not allocate row keys")
        })?;
        apply_table_plan(&conn, &table_plan, self.request.commit.disposition.clone())?;
        let write_target = if self.request.commit.disposition == WriteDisposition::Merge {
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
            .map_err(|error| duckdb_error("create DuckDB Arrow merge staging table", error))?;
            staging
        } else {
            table_plan.target.clone()
        };
        self.writer = Some(DuckDbArrowWriter {
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
        });
        Ok(())
    }

    fn finalize_outcome(mut self) -> Result<DuckDbCommitOutcome> {
        if !self.migrations_applied {
            return Err(CdfError::destination(
                "DuckDB migrations must be applied before finalize",
            ));
        }
        if self.accepted_segments.len() != self.expected_segments.len() {
            return Err(CdfError::destination(format!(
                "cannot finalize DuckDB commit session before all segments are written: accepted {} of {}",
                self.accepted_segments.len(),
                self.expected_segments.len()
            )));
        }

        if self.expected_order.is_empty() {
            return self.destination.commit_empty_package(self.request);
        }
        if let Some(receipt) = self.duplicate_receipt {
            let recorded = record_package_receipt_once(&self.request.package_dir, &receipt)?;
            return Ok(DuckDbCommitOutcome {
                receipt,
                duplicate: true,
                plan: self.plan,
                package_receipt_recorded: recorded,
            });
        }
        let writer = self.writer.take().ok_or_else(|| {
            CdfError::internal("DuckDB Arrow commit session has no active writer")
        })?;
        let counts = match self.request.commit.disposition {
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
                &self.request.merge_keys,
            )?,
            WriteDisposition::CdcApply => {
                return Err(CdfError::contract(
                    "DuckDB destination does not support cdc_apply in the MVP sheet",
                ));
            }
        };
        let segment_acks = self
            .expected_order
            .iter()
            .map(|segment_id| {
                let expected = &self.expected_segments[segment_id];
                SegmentAck {
                    segment_id: segment_id.clone(),
                    row_count: expected.state.row_count,
                    byte_count: expected.state.byte_count,
                }
            })
            .collect::<Vec<_>>();
        let committed_at_ms = now_ms()?;
        let receipt = build_receipt(
            &self.request.commit,
            &self.request.schema_hash,
            &segment_acks,
            counts,
            &ReceiptBuildContext {
                migrations: &self.plan.kernel.migrations,
                committed_at_ms,
                duckdb_version: &writer.duckdb_version,
                database_path: &self.destination.database_path,
                lock_path: &self.destination.lock_path(),
            },
        )?;
        insert_mirrors(
            &writer.conn,
            &self.request.commit,
            &segment_acks,
            &receipt,
            writer.first_row_key,
        )?;
        writer
            .conn
            .execute_batch("COMMIT")
            .map_err(|error| duckdb_error("commit Arrow transaction", error))?;
        let recorded = record_package_receipt_once(&self.request.package_dir, &receipt)?;
        Ok(DuckDbCommitOutcome {
            receipt,
            duplicate: false,
            plan: self.plan,
            package_receipt_recorded: recorded,
        })
    }
}

impl DuckDbStagedIngressSession {
    fn validate_final_binding(&self, binding: &cdf_runtime::VerifiedFinalBinding) -> Result<()> {
        if binding.attempt_id() != &self.request.attempt_id
            || binding.staging_plan_id() != &self.request.binding.plan_id
            || binding.commit().target != self.request.binding.target
            || binding.commit().disposition != self.request.binding.disposition
            || binding.schema_hash() != &self.request.binding.schema_hash
        {
            return Err(CdfError::destination(
                "DuckDB staged ingress final binding differs from its attempt authority",
            ));
        }
        binding.validate_staged_identities(&self.accepted)
    }

    fn bind_empty(self, binding: cdf_runtime::VerifiedFinalBinding) -> Result<Receipt> {
        if !binding.ordered_segments().is_empty() {
            return Err(CdfError::internal(
                "DuckDB staged empty binding received data segments",
            ));
        }
        let lock = self.destination.acquire_writer_lock()?;
        let conn = self.destination.open_connection()?;
        ensure_mirror_tables(&conn)?;
        if let Some(receipt) = find_duplicate_receipt(&conn, binding.commit())? {
            return Ok(receipt);
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
        Ok(receipt)
    }
}

impl cdf_runtime::StagedIngressSession for DuckDbStagedIngressSession {
    fn stage_segment(
        &mut self,
        mut segment: cdf_runtime::StagedSegmentRequest,
    ) -> Result<cdf_runtime::StagedSegmentAck> {
        let identity = segment.identity.clone();
        if identity.schema_hash != self.request.binding.schema_hash {
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
        if self.writer.is_none() {
            let (writer, migrations) = self.destination.start_staged_writer(&self.request)?;
            self.writer = Some(writer);
            self.migrations = migrations;
        }
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| CdfError::internal("DuckDB staged writer is not initialized"))?;
        let segment_start =
            allocate_row_keys(&writer.conn, identity.row_count)?.ok_or_else(|| {
                CdfError::data("DuckDB staged data segment must contain at least one row")
            })?;
        if let Some(next) = writer.next_row_key
            && next != segment_start
        {
            return Err(CdfError::internal(
                "DuckDB staged row-key allocation is not contiguous",
            ));
        }
        writer.first_row_key.get_or_insert(segment_start);
        let mut next_row_key = segment_start;
        let segment_rows_before = writer.rows_received;
        while let Some(batch) = segment.reader_mut().next_batch()? {
            if batch.schema().as_ref() != &self.request.output_schema {
                return Err(CdfError::data(format!(
                    "DuckDB staged segment {} schema differs from the planned output schema",
                    identity.segment_id
                )));
            }
            let batch_rows = u64::try_from(batch.num_rows())
                .map_err(|_| CdfError::data("DuckDB staged batch rows exceed u64"))?;
            let merge = self.request.binding.disposition == WriteDisposition::Merge;
            let persisted =
                persistence_batch(batch, next_row_key, merge.then_some(writer.rows_received))?;
            append_arrow_batch_to_table(&writer.conn, &writer.write_target, persisted)?;
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
        self.accepted.push(identity.clone());
        Ok(cdf_runtime::StagedSegmentAck {
            attempt_id: self.request.attempt_id.clone(),
            identity,
            external_durable: false,
        })
    }

    fn snapshot(&self) -> Result<cdf_runtime::StagingSnapshot> {
        Ok(cdf_runtime::StagingSnapshot {
            attempt_id: self.request.attempt_id.clone(),
            binding: self.request.binding.clone(),
            recovery: cdf_runtime::StagingRecoveryMode::RollbackRedrive,
            accepted_segments: self.accepted.clone(),
        })
    }

    fn bind_final(
        mut self: Box<Self>,
        binding: cdf_runtime::VerifiedFinalBinding,
    ) -> Result<Receipt> {
        self.validate_final_binding(&binding)?;
        let Some(writer) = self.writer.take() else {
            return (*self).bind_empty(binding);
        };
        if let Some(receipt) = find_duplicate_receipt(&writer.conn, binding.commit())? {
            writer
                .conn
                .execute_batch("ROLLBACK")
                .map_err(|error| duckdb_error("rollback duplicate staged transaction", error))?;
            return Ok(receipt);
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
                &self.request.merge_keys,
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
        insert_mirrors(
            &writer.conn,
            binding.commit(),
            &segment_acks,
            &receipt,
            writer.first_row_key,
        )?;
        writer
            .conn
            .execute_batch("COMMIT")
            .map_err(|error| duckdb_error("commit staged Arrow transaction", error))?;
        Ok(receipt)
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

impl CommitSession for DuckDbCommitSession<'_> {
    fn apply_migrations(&mut self) -> Result<()> {
        if self.migrations_applied {
            return Err(CdfError::destination(
                "DuckDB migrations were already applied for this commit session",
            ));
        }
        self.start_writer()?;
        self.migrations_applied = true;
        Ok(())
    }

    fn write_segment(&mut self, segment: CommitSegment) -> Result<SegmentAck> {
        if !self.migrations_applied {
            return Err(CdfError::destination(
                "DuckDB migrations must be applied before writing",
            ));
        }
        let segment_id = segment.state.segment_id.clone();
        let expected = self.expected_segments.get(&segment_id).ok_or_else(|| {
            CdfError::data(format!(
                "DuckDB commit segment {} is not in the planned package request",
                segment_id.as_str()
            ))
        })?;
        if self.accepted_segments.contains(&segment_id) {
            return Err(CdfError::data(format!(
                "DuckDB commit session received duplicate segment {}",
                segment_id.as_str()
            )));
        }
        let ordered = self.expected_order.get(self.next_expected).ok_or_else(|| {
            CdfError::data("DuckDB commit session received more segments than planned")
        })?;
        if ordered != &segment_id {
            return Err(CdfError::data(format!(
                "DuckDB Arrow writer requires manifest-order segments: expected {}, received {}",
                ordered.as_str(),
                segment_id.as_str()
            )));
        }
        validate_commit_segment(&segment, expected)?;

        let ack = SegmentAck {
            segment_id: expected.state.segment_id.clone(),
            row_count: expected.state.row_count,
            byte_count: expected.state.byte_count,
        };
        if self.duplicate_receipt.is_none() {
            let schema = self.schema.as_ref().ok_or_else(|| {
                CdfError::internal("DuckDB Arrow writer is missing its planned schema")
            })?;
            let writer = self
                .writer
                .as_mut()
                .ok_or_else(|| CdfError::internal("DuckDB Arrow writer is not initialized"))?;
            let merge = self.request.commit.disposition == WriteDisposition::Merge;
            for commit_batch in segment.into_batches()? {
                if commit_batch.batch.schema().as_ref() != schema.as_ref() {
                    return Err(CdfError::data(format!(
                        "DuckDB segment {} schema differs from the planned runtime schema",
                        segment_id.as_str()
                    )));
                }
                let batch_rows = u64::try_from(commit_batch.batch.num_rows())
                    .map_err(|_| CdfError::data("DuckDB batch row count exceeds u64"))?;
                let batch = persistence_batch(
                    commit_batch.batch,
                    writer.next_row_key.ok_or_else(|| {
                        CdfError::internal("DuckDB Arrow writer has no next row key")
                    })?,
                    merge.then_some(writer.rows_received),
                )?;
                append_arrow_batch_to_table(&writer.conn, &writer.write_target, batch)?;
                writer.next_row_key = Some(
                    writer
                        .next_row_key
                        .expect("DuckDB initialized writer has a row key")
                        .checked_add(batch_rows)
                        .ok_or_else(|| CdfError::data("DuckDB row key overflowed"))?,
                );
                writer.rows_received = writer
                    .rows_received
                    .checked_add(batch_rows)
                    .ok_or_else(|| CdfError::data("DuckDB package row count overflowed"))?;
            }
        }
        self.accepted_segments.insert(segment_id);
        self.next_expected += 1;
        Ok(ack)
    }

    fn finalize(self: Box<Self>) -> Result<Receipt> {
        Ok(self.finalize_outcome()?.receipt)
    }

    fn abort(mut self: Box<Self>) -> Result<()> {
        if let Some(writer) = self.writer.take() {
            writer
                .conn
                .execute_batch("ROLLBACK")
                .map_err(|error| duckdb_error("rollback DuckDB Arrow transaction", error))?;
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

    fn begin(
        &self,
        request: DestinationCommitRequest,
        plan: CommitPlan,
    ) -> Result<Box<dyn CommitSession + '_>> {
        validate_session_plan(&request, &plan)?;
        let context = self.take_session_context(&plan.plan_id, &request)?;
        if context.plan.kernel != plan {
            return Err(CdfError::contract(
                "DuckDB session plan differs from its prepared package plan",
            ));
        }
        Ok(Box::new(DuckDbCommitSession::new(self, context)?))
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

fn expected_segments_for_request(
    request: &DuckDbCommitRequest,
) -> Result<(
    BTreeMap<cdf_kernel::SegmentId, ExpectedSegment>,
    Vec<cdf_kernel::SegmentId>,
)> {
    let reader = PackageReader::open(&request.package_dir)?;
    let mut manifest_by_id = BTreeMap::new();
    let mut expected_order = Vec::new();
    for segment in &reader.manifest().identity.segments {
        if manifest_by_id
            .insert(segment.segment_id.clone(), segment)
            .is_some()
        {
            return Err(CdfError::data(format!(
                "package manifest contains duplicate segment {}",
                segment.segment_id.as_str()
            )));
        }
        expected_order.push(segment.segment_id.clone());
    }

    let mut request_by_id = BTreeMap::new();
    for state in &request.commit.segments {
        if request_by_id
            .insert(state.segment_id.clone(), state)
            .is_some()
        {
            return Err(CdfError::data(format!(
                "destination commit request contains duplicate segment {}",
                state.segment_id.as_str()
            )));
        }
    }

    let mut expected_segments = BTreeMap::new();
    for (segment_id, manifest_segment) in &manifest_by_id {
        let state = request_by_id.get(segment_id).ok_or_else(|| {
            CdfError::data(format!(
                "package manifest segment {} is missing from destination commit request",
                segment_id.as_str()
            ))
        })?;
        if state.row_count != manifest_segment.row_count {
            return Err(CdfError::data(format!(
                "destination commit request segment {} has {} rows but package manifest has {} rows",
                segment_id.as_str(),
                state.row_count,
                manifest_segment.row_count
            )));
        }
        expected_segments.insert(
            segment_id.clone(),
            ExpectedSegment {
                state: (*state).clone(),
                package_byte_count: manifest_segment.byte_count,
            },
        );
    }

    for segment_id in request_by_id.keys() {
        if !manifest_by_id.contains_key(segment_id) {
            return Err(CdfError::data(format!(
                "destination commit request segment {} is not present in the package manifest",
                segment_id.as_str()
            )));
        }
    }

    Ok((expected_segments, expected_order))
}

fn validate_commit_segment(segment: &CommitSegment, expected: &ExpectedSegment) -> Result<()> {
    if segment.state != expected.state {
        return Err(CdfError::data(format!(
            "DuckDB commit segment {} state does not match destination commit request",
            segment.state.segment_id.as_str()
        )));
    }
    if segment.package_byte_count != expected.package_byte_count {
        return Err(CdfError::data(format!(
            "DuckDB commit segment {} package byte count {} differs from manifest {}",
            segment.state.segment_id.as_str(),
            segment.package_byte_count,
            expected.package_byte_count
        )));
    }
    if segment.batches.is_empty() {
        return Err(CdfError::data(format!(
            "DuckDB commit segment {} contains no record batches",
            segment.state.segment_id.as_str()
        )));
    }
    let schema = segment.batches[0].schema();
    let mut row_count = 0_u64;
    for batch in &segment.batches {
        if batch.schema().as_ref() != schema.as_ref() {
            return Err(CdfError::data(format!(
                "DuckDB commit segment {} contains mixed schemas",
                segment.state.segment_id.as_str()
            )));
        }
        row_count += batch.num_rows() as u64;
    }
    if row_count != expected.state.row_count {
        return Err(CdfError::data(format!(
            "DuckDB commit segment {} has {} payload rows but request expects {}",
            segment.state.segment_id.as_str(),
            row_count,
            expected.state.row_count
        )));
    }
    Ok(())
}

fn validate_session_plan(request: &DestinationCommitRequest, plan: &CommitPlan) -> Result<()> {
    if plan.target != request.target || plan.disposition != request.disposition {
        return Err(CdfError::destination(
            "DuckDB commit plan does not match destination request",
        ));
    }
    if plan.idempotency != IdempotencySupport::PackageToken {
        return Err(CdfError::destination(
            "DuckDB commit plan must use package-token idempotency",
        ));
    }
    Ok(())
}
