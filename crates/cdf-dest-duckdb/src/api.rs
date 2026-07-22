use crate::*;
use crate::{
    commit::*, corrections::*, ingest_envelope::DuckDbIngestEnvelope, mirrors::*, package::*,
    planning::*, receipts::*, segment_scan::*, sheet::*, sql::*, table::*,
};

#[derive(Clone, Debug)]
pub struct DuckDbDestination {
    database_path: PathBuf,
    sheet: DestinationSheet,
    pub(crate) native_resources: DuckDbNativeResources,
    pub(crate) pending_corrections: Arc<Mutex<BTreeMap<PlanId, DuckDbCorrectionContext>>>,
}

#[derive(Clone)]
pub(crate) struct DuckDbNativeResources {
    pub(crate) memory_limit_bytes: u64,
    pub(crate) maximum_temp_directory_bytes: u64,
    pub(crate) internal_threads: i64,
    pub(crate) scan_threads_override: Option<usize>,
    pub(crate) max_in_flight_bytes: u64,
    pub(crate) profiling_directory: Option<PathBuf>,
    scratch_reservation: Option<Arc<cdf_runtime::SpillReservation>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct DuckDbNativeResourceOverrides {
    pub(crate) memory_limit_bytes: Option<u64>,
    pub(crate) maximum_temp_directory_bytes: Option<u64>,
    pub(crate) internal_threads: Option<i64>,
    pub(crate) scan_threads: Option<usize>,
    pub(crate) max_in_flight_bytes: Option<u64>,
    pub(crate) profiling_directory: Option<PathBuf>,
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
            .field("scan_threads_override", &self.scan_threads_override)
            .field("max_in_flight_bytes", &self.max_in_flight_bytes)
            .field("profiling_directory", &self.profiling_directory)
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
pub(crate) struct DuckDbCommitWriter {
    pub(crate) conn: Connection,
    pub(crate) segment_scan: DuckDbSegmentScanRuntime,
    target: TargetRef,
    pub(crate) write_target: TargetRef,
    pub(crate) first_row_key: Option<u64>,
    pub(crate) persisted_fields: Vec<FieldPlan>,
    pub(crate) user_field_count: usize,
    pub(crate) omitted_user_fields: Vec<bool>,
    pub(crate) rows_received: u64,
    duckdb_version: String,
}

#[derive(Debug)]
struct DuckDbStagedIngressSession {
    destination: DuckDbDestination,
    request: cdf_runtime::StagedIngressRequest,
    files: Vec<cdf_runtime::DurableLocalFileAccess>,
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
        files: Vec<cdf_runtime::DurableLocalFileAccess>,
        scan_threads: usize,
        projection: DuckDbSegmentProjection,
    ) -> Result<(DuckDbCommitWriter, Vec<MigrationRecord>)> {
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
        let segment_scan = DuckDbSegmentScanRuntime::open(
            &self.database_path,
            &self.native_resources,
            files,
            projection.clone(),
            scan_threads,
        )?;
        let conn = segment_scan.connection()?;
        ensure_mirror_tables(&conn)?;
        conn.execute_batch("BEGIN TRANSACTION")
            .map_err(|error| duckdb_error("begin DuckDB commit transaction", error))?;
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
            DuckDbCommitWriter {
                conn,
                segment_scan,
                target: table_plan.target,
                write_target,
                first_row_key: Some(first_row_key),
                persisted_fields,
                user_field_count: user_fields.len(),
                omitted_user_fields: projection.omitted_user_fields().to_vec(),
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
        match request.bulk_path().descriptor.path_id.as_str() {
            DUCKDB_BULK_PATH_SEGMENT_SCAN => {}
            path => {
                return Err(CdfError::contract(format!(
                    "unsupported prepared DuckDB bulk path {path}"
                )));
            }
        };
        Ok(Box::new(DuckDbStagedIngressSession {
            destination: self.clone(),
            request,
            files: Vec::new(),
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
            memory_limit_bytes: DUCKDB_CONSERVATIVE_MEMORY_BYTES,
            maximum_temp_directory_bytes: DUCKDB_DEFAULT_TEMP_DIRECTORY_BUDGET_CEILING_BYTES,
            internal_threads: DUCKDB_DEFAULT_INTERNAL_THREADS,
            scan_threads_override: None,
            max_in_flight_bytes: DUCKDB_DEFAULT_MAX_IN_FLIGHT_BYTES,
            profiling_directory: None,
            scratch_reservation: None,
        }
    }

    fn for_execution(execution: &cdf_runtime::ExecutionServices) -> Result<Self> {
        let managed_budget = execution.memory().snapshot().budget_bytes;
        let mut overrides = DuckDbNativeResourceOverrides::from_env()?;
        if overrides.internal_threads.is_none() {
            overrides.internal_threads =
                Some(i64::from(execution.capabilities().logical_cpu_slots.max(1)));
        }
        if overrides.memory_limit_bytes.is_none() {
            overrides.memory_limit_bytes = Some(managed_budget);
        }
        Self::for_budgets_with_overrides(managed_budget, execution.spill(), overrides)
    }

    pub(crate) fn for_budgets_with_overrides(
        managed_budget: u64,
        spill: Arc<dyn cdf_runtime::SpillBudgetCoordinator>,
        overrides: DuckDbNativeResourceOverrides,
    ) -> Result<Self> {
        let memory_limit_bytes = match overrides.memory_limit_bytes {
            Some(0) => {
                return Err(CdfError::contract(format!(
                    "{DUCKDB_MEMORY_LIMIT_ENV} must be greater than zero"
                )));
            }
            Some(bytes) => bytes,
            None => (managed_budget / 4).clamp(1, DUCKDB_DEFAULT_NATIVE_MEMORY_LIMIT_CEILING_BYTES),
        };
        let maximum_temp_directory_bytes =
            overrides.maximum_temp_directory_bytes.unwrap_or_else(|| {
                spill
                    .snapshot()
                    .budget_bytes
                    .min(DUCKDB_DEFAULT_TEMP_DIRECTORY_BUDGET_CEILING_BYTES)
            });
        if maximum_temp_directory_bytes == 0 {
            return Err(CdfError::contract(format!(
                "{DUCKDB_TEMP_BUDGET_ENV} must be greater than zero"
            )));
        }
        let internal_threads = overrides
            .internal_threads
            .unwrap_or(DUCKDB_DEFAULT_INTERNAL_THREADS);
        let scan_threads_override = overrides.scan_threads;
        let max_in_flight_bytes = overrides
            .max_in_flight_bytes
            .unwrap_or(DUCKDB_DEFAULT_MAX_IN_FLIGHT_BYTES);
        if max_in_flight_bytes == 0 {
            return Err(CdfError::contract(format!(
                "{DUCKDB_MAX_IN_FLIGHT_BYTES_ENV} must be greater than zero"
            )));
        }
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
            internal_threads,
            scan_threads_override,
            max_in_flight_bytes,
            profiling_directory: overrides.profiling_directory,
            scratch_reservation: Some(Arc::new(scratch_reservation)),
        })
    }
}

impl DuckDbNativeResourceOverrides {
    fn from_env() -> Result<Self> {
        Ok(Self {
            memory_limit_bytes: optional_env_byte_size(DUCKDB_MEMORY_LIMIT_ENV)?,
            maximum_temp_directory_bytes: optional_env_byte_size(DUCKDB_TEMP_BUDGET_ENV)?,
            internal_threads: optional_env_threads(DUCKDB_THREADS_ENV)?,
            scan_threads: optional_env_threads(DUCKDB_SCAN_THREADS_ENV)?
                .map(|threads| {
                    usize::try_from(threads).map_err(|_| {
                        CdfError::contract(format!(
                            "{DUCKDB_SCAN_THREADS_ENV} exceeds the platform thread limit"
                        ))
                    })
                })
                .transpose()?,
            max_in_flight_bytes: optional_env_byte_size(DUCKDB_MAX_IN_FLIGHT_BYTES_ENV)?,
            profiling_directory: optional_env_directory(DUCKDB_PROFILE_DIRECTORY_ENV)?,
        })
    }
}

fn optional_env_directory(name: &str) -> Result<Option<PathBuf>> {
    match std::env::var(name) {
        Ok(value) => parse_profile_directory(name, &value).map(Some),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(CdfError::contract(format!(
            "{name} must be valid UTF-8 when set"
        ))),
    }
}

fn parse_profile_directory(name: &str, value: &str) -> Result<PathBuf> {
    let value = value.trim();
    if value.is_empty() {
        return Err(CdfError::contract(format!(
            "{name} must be a nonempty absolute directory path"
        )));
    }
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        return Err(CdfError::contract(format!(
            "{name} must be an absolute directory path"
        )));
    }
    Ok(path)
}

fn optional_env_byte_size(name: &str) -> Result<Option<u64>> {
    match std::env::var(name) {
        Ok(value) => cdf_kernel::parse_human_byte_size(name, &value).map(Some),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(CdfError::contract(format!(
            "{name} must be valid UTF-8 when set"
        ))),
    }
}

fn optional_env_threads(name: &str) -> Result<Option<i64>> {
    match std::env::var(name) {
        Ok(value) => parse_threads(name, &value).map(Some),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(CdfError::contract(format!(
            "{name} must be valid UTF-8 when set"
        ))),
    }
}

fn parse_threads(label: &str, value: &str) -> Result<i64> {
    let value = value.trim();
    let threads = value.parse::<i64>().map_err(|error| {
        CdfError::contract(format!("{label} must be a positive integer: {error}"))
    })?;
    if threads <= 0 {
        return Err(CdfError::contract(format!(
            "{label} must be a positive integer"
        )));
    }
    Ok(threads)
}

fn bounded_connection_config(resources: &DuckDbNativeResources, read_only: bool) -> Result<Config> {
    let mut config = Config::default();
    for (name, value) in duckdb_config_options(resources) {
        config = config
            .with(&name, &value)
            .map_err(|error| duckdb_error("configure bounded DuckDB runtime", error))?;
    }
    if read_only {
        config = config
            .access_mode(AccessMode::ReadOnly)
            .map_err(|error| duckdb_error("configure read-only DuckDB open", error))?;
    }
    Ok(config)
}

pub(crate) fn duckdb_config_options(resources: &DuckDbNativeResources) -> Vec<(String, String)> {
    vec![
        (
            "memory_limit".to_owned(),
            format!("{}B", resources.memory_limit_bytes),
        ),
        ("threads".to_owned(), resources.internal_threads.to_string()),
        (
            "max_temp_directory_size".to_owned(),
            format!("{}B", resources.maximum_temp_directory_bytes),
        ),
        ("preserve_insertion_order".to_owned(), "false".to_owned()),
        ("errors_as_json".to_owned(), "true".to_owned()),
        ("duckdb_api".to_owned(), "rust".to_owned()),
    ]
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
        insert_mirrors(&conn, binding.commit(), &[], &receipt, None, None)?;
        conn.execute_batch("COMMIT")
            .map_err(|error| duckdb_error("commit empty staged transaction", error))?;
        drop(lock);
        Ok(cdf_runtime::DestinationCommitOutcome::new(
            receipt,
            cdf_runtime::DestinationReceiptReportingPolicy::DestinationCommit { duplicate: false },
        ))
    }

    fn validate_next_segment(
        request: &cdf_runtime::StagedIngressRequest,
        accepted: &[cdf_runtime::StagedSegmentIdentity],
        identity: &cdf_runtime::StagedSegmentIdentity,
    ) -> Result<()> {
        if identity.schema_hash != request.binding().schema_hash {
            return Err(CdfError::data(
                "DuckDB staged segment schema hash differs from its attempt",
            ));
        }
        let expected_ordinal = u32::try_from(accepted.len())
            .map_err(|_| CdfError::data("DuckDB staged segment count exceeds u32"))?;
        if identity.ordinal != expected_ordinal
            || accepted
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
        let expected_start = accepted.last().map_or(Ok(0), |previous| {
            previous
                .package_row_ord_start
                .checked_add(previous.row_count)
                .ok_or_else(|| CdfError::data("DuckDB staged package ordinal overflowed"))
        })?;
        if identity.package_row_ord_start != expected_start {
            return Err(CdfError::data(format!(
                "DuckDB staged segment {} package ordinal starts at {} but canonical ingress requires {expected_start}",
                identity.segment_id, identity.package_row_ord_start
            )));
        }
        Ok(())
    }

    fn stage_canonical_segments(
        &mut self,
        first_segment: cdf_runtime::StagedSegmentRequest,
        stream: &mut dyn cdf_runtime::StagedSegmentStream,
    ) -> Result<()> {
        let mut current = Some(first_segment);
        while let Some(mut segment) = current {
            self.request.mutation_guard().assert_current()?;
            let identity = segment.identity.clone();
            Self::validate_next_segment(&self.request, &self.accepted, &identity)?;
            let local_file = segment.take_durable_local_file_access().ok_or_else(|| {
                CdfError::data(format!(
                    "DuckDB canonical segment scan requires durable file access for segment {}",
                    identity.segment_id
                ))
            })?;
            if !local_file.path().is_absolute() {
                return Err(CdfError::data(format!(
                    "DuckDB canonical segment path must be absolute: {}",
                    local_file.path().display()
                )));
            }
            stream.acknowledge(cdf_runtime::StagedSegmentAck {
                attempt_id: self.request.attempt_id().clone(),
                identity: identity.clone(),
                external_durable: false,
            })?;
            self.accepted.push(identity);
            self.files.push(local_file);
            drop(segment);
            current = stream.next_segment()?;
        }
        Ok(())
    }
}

impl cdf_runtime::StagedIngressSession for DuckDbStagedIngressSession {
    fn stage_stream(&mut self, stream: &mut dyn cdf_runtime::StagedSegmentStream) -> Result<()> {
        let Some(first_segment) = stream.next_segment()? else {
            return Ok(());
        };
        self.stage_canonical_segments(first_segment, stream)
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
        if self.accepted.is_empty() {
            return (*self).bind_empty(binding);
        }
        let files = std::mem::take(&mut self.files);
        let expected_rows = self.accepted.iter().try_fold(0_u64, |total, identity| {
            total
                .checked_add(identity.row_count)
                .ok_or_else(|| CdfError::data("DuckDB canonical segment row count overflowed"))
        })?;
        let projection = DuckDbSegmentProjection::from_verified_package_statistics(
            self.request.output_schema(),
            self.request.segment_schema(),
            binding.package_statistics(),
            expected_rows,
            &self.request.binding().merge_keys,
        )?;
        let envelope = DuckDbIngestEnvelope::resolve(
            &self.destination.native_resources,
            self.request.segment_schema(),
            self.request.bulk_path().rows_per_batch,
            self.request.bulk_path().bytes_per_batch,
        )?;
        let _lock = self.destination.acquire_writer_lock()?;
        let mut scan_threads = envelope.initial_scan_threads();
        let mut attempt = 1_usize;
        let (writer, migrations) = loop {
            let (mut writer, migrations) = self.destination.start_staged_writer(
                &self.request,
                files.clone(),
                scan_threads,
                projection.clone(),
            )?;
            if let Some(receipt) = find_duplicate_receipt(&writer.conn, binding.commit())? {
                rollback_staged_writer(&mut writer, "rollback duplicate staged transaction")?;
                return Ok(cdf_runtime::DestinationCommitOutcome::new(
                    receipt,
                    cdf_runtime::DestinationReceiptReportingPolicy::DestinationCommit {
                        duplicate: true,
                    },
                ));
            }
            let profile = crate::profiling::DuckDbProfileCapture::start(
                &writer.conn,
                self.destination
                    .native_resources
                    .profiling_directory
                    .as_deref(),
                attempt,
                scan_threads,
            )?;
            let ingest = ingest_canonical_segments(
                &mut writer,
                expected_rows,
                self.request.binding().disposition == WriteDisposition::Merge,
            );
            let profile = profile
                .map(|profile| profile.finish(&writer.conn))
                .transpose();
            let ingest = match (ingest, profile) {
                (Ok(()), Ok(_)) => Ok(()),
                (Ok(()), Err(error)) => Err(DuckDbFailure::other(error)),
                (Err(failure), Ok(_)) => Err(failure),
                (Err(mut failure), Err(profile_error)) => {
                    failure.error.message = format!(
                        "{}; DuckDB materialization profile capture also failed: {}",
                        failure.error.message, profile_error.message
                    );
                    Err(failure)
                }
            };
            match ingest {
                Ok(()) => break (writer, migrations),
                Err(failure) => match resolve_failed_ingest_attempt(
                    &writer.conn,
                    failure,
                    envelope,
                    scan_threads,
                    &self.destination.native_resources,
                )? {
                    FailedIngestDisposition::Retry(next) => {
                        scan_threads = next;
                        attempt = attempt.saturating_add(1);
                        continue;
                    }
                    FailedIngestDisposition::Fail(error) => return Err(error),
                },
            }
        };
        let counts = match binding.commit().disposition.clone() {
            WriteDisposition::Append | WriteDisposition::Replace => CommitCounts {
                rows_written: writer.rows_received,
                rows_inserted: Some(writer.rows_received),
                rows_updated: Some(0),
                rows_deleted: Some(0),
            },
            WriteDisposition::Merge => finalize_merge(
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
                migrations: &migrations,
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
                .first_row_key
                .ok_or_else(|| CdfError::internal("DuckDB staged first row key is absent"))?
                .checked_add(writer.rows_received)
                .ok_or_else(|| CdfError::data("DuckDB staged row key overflowed"))?,
        )?;
        insert_mirrors(
            &writer.conn,
            binding.commit(),
            &segment_acks,
            &receipt,
            writer.first_row_key,
            Some(&self.accepted),
        )?;
        self.request.mutation_guard().assert_current()?;
        writer
            .conn
            .execute_batch("COMMIT")
            .map_err(|error| duckdb_error("commit DuckDB transaction", error))?;
        Ok(cdf_runtime::DestinationCommitOutcome::new(
            receipt,
            cdf_runtime::DestinationReceiptReportingPolicy::DestinationCommit { duplicate: false },
        ))
    }

    fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

fn rollback_staged_writer(writer: &mut DuckDbCommitWriter, context: &str) -> Result<()> {
    rollback_connection(&writer.conn, context)
}

fn rollback_connection(conn: &Connection, context: &str) -> Result<()> {
    conn.execute_batch("ROLLBACK")
        .map_err(|error| duckdb_error(context, error))
}

enum FailedIngestDisposition {
    Retry(usize),
    Fail(CdfError),
}

fn resolve_failed_ingest_attempt(
    conn: &Connection,
    failure: DuckDbFailure,
    envelope: DuckDbIngestEnvelope,
    scan_threads: usize,
    resources: &DuckDbNativeResources,
) -> Result<FailedIngestDisposition> {
    if let Err(rollback) =
        rollback_connection(conn, "rollback failed DuckDB finalized-package attempt")
    {
        return Err(CdfError::destination(format!(
            "{}; the failed finalized-package attempt could not be rolled back: {}",
            failure.error.message, rollback.message
        )));
    }
    if failure.exception_type == DuckDbExceptionType::OutOfMemory
        && let Some(next) = envelope.next_retry_threads(scan_threads)
    {
        return Ok(FailedIngestDisposition::Retry(next));
    }
    if failure.exception_type == DuckDbExceptionType::OutOfMemory && envelope.is_automatic() {
        return Ok(FailedIngestDisposition::Fail(CdfError::destination(
            format!(
                "{}; DuckDB remained out of memory after CDF reduced finalized-package scan concurrency to one worker (estimated worker footprint {} bytes within the admitted {}-byte DuckDB memory budget)",
                failure.error.message,
                envelope.estimated_worker_bytes(),
                resources.memory_limit_bytes,
            ),
        )));
    }
    Ok(FailedIngestDisposition::Fail(failure.error))
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
    fn execution_defaults_use_available_host_parallelism() {
        let (_, services) =
            cdf_engine::StandaloneExecutionHost::default_services(512 * 1024 * 1024).unwrap();
        let resources = DuckDbNativeResources::for_execution(&services).unwrap();
        assert_eq!(
            resources.internal_threads,
            i64::from(services.capabilities().logical_cpu_slots)
        );
    }

    #[test]
    fn execution_resources_reserve_and_release_bounded_scratch_capacity() {
        let spill = Arc::new(cdf_runtime::FixedSpillBudget::new(2 * 1024 * 1024 * 1024).unwrap());
        let coordinator: Arc<dyn cdf_runtime::SpillBudgetCoordinator> = spill.clone();
        let resources = DuckDbNativeResources::for_budgets_with_overrides(
            4 * 1024 * 1024 * 1024,
            coordinator,
            DuckDbNativeResourceOverrides::default(),
        )
        .unwrap();
        assert_eq!(resources.memory_limit_bytes, 1024 * 1024 * 1024);
        assert_eq!(resources.maximum_temp_directory_bytes, 1024 * 1024 * 1024);
        assert_eq!(
            resources.max_in_flight_bytes,
            DUCKDB_DEFAULT_MAX_IN_FLIGHT_BYTES
        );
        assert_eq!(spill.snapshot().current_bytes, 1024 * 1024 * 1024);

        let clone = resources.clone();
        drop(resources);
        assert_eq!(spill.snapshot().current_bytes, 1024 * 1024 * 1024);
        drop(clone);
        assert_eq!(spill.snapshot().current_bytes, 0);
    }

    #[test]
    fn execution_resource_overrides_remove_default_ceilings_when_explicit() {
        let spill = Arc::new(cdf_runtime::FixedSpillBudget::new(8 * 1024 * 1024 * 1024).unwrap());
        let coordinator: Arc<dyn cdf_runtime::SpillBudgetCoordinator> = spill.clone();
        let resources = DuckDbNativeResources::for_budgets_with_overrides(
            4 * 1024 * 1024 * 1024,
            coordinator,
            DuckDbNativeResourceOverrides {
                memory_limit_bytes: Some(3 * 1024 * 1024 * 1024),
                maximum_temp_directory_bytes: Some(6 * 1024 * 1024 * 1024),
                internal_threads: Some(4),
                scan_threads: Some(3),
                max_in_flight_bytes: Some(512 * 1024 * 1024),
                profiling_directory: None,
            },
        )
        .unwrap();

        assert_eq!(resources.memory_limit_bytes, 3 * 1024 * 1024 * 1024);
        assert_eq!(
            resources.maximum_temp_directory_bytes,
            6 * 1024 * 1024 * 1024
        );
        assert_eq!(resources.internal_threads, 4);
        assert_eq!(resources.scan_threads_override, Some(3));
        assert_eq!(resources.max_in_flight_bytes, 512 * 1024 * 1024);
        assert_eq!(spill.snapshot().current_bytes, 6 * 1024 * 1024 * 1024);

        let config = duckdb_config_options(&resources)
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        assert_eq!(config.get("threads").map(String::as_str), Some("4"));
        assert_eq!(
            config.get("errors_as_json").map(String::as_str),
            Some("true")
        );
        assert!(!config.contains_key("scan_threads"));
    }

    #[test]
    fn resource_override_parsers_reject_invalid_values() {
        assert_eq!(
            cdf_kernel::parse_human_byte_size(DUCKDB_TEMP_BUDGET_ENV, "6GiB").unwrap(),
            6 * 1024 * 1024 * 1024
        );
        assert_eq!(parse_threads(DUCKDB_THREADS_ENV, "12").unwrap(), 12);
        assert_eq!(parse_threads(DUCKDB_SCAN_THREADS_ENV, "7").unwrap(), 7);

        let memory_error =
            cdf_kernel::parse_human_byte_size(DUCKDB_MEMORY_LIMIT_ENV, "0").unwrap_err();
        assert!(memory_error.message.contains("must be greater than zero"));

        let in_flight_error =
            cdf_kernel::parse_human_byte_size(DUCKDB_MAX_IN_FLIGHT_BYTES_ENV, "0").unwrap_err();
        assert!(
            in_flight_error
                .message
                .contains("must be greater than zero")
        );

        let thread_error = parse_threads(DUCKDB_THREADS_ENV, "0").unwrap_err();
        assert!(thread_error.message.contains("positive integer"));

        let profile_path =
            parse_profile_directory(DUCKDB_PROFILE_DIRECTORY_ENV, " /tmp/profiles ").unwrap();
        assert_eq!(profile_path, PathBuf::from("/tmp/profiles"));
        for invalid in ["", "relative/profiles"] {
            let error = parse_profile_directory(DUCKDB_PROFILE_DIRECTORY_ENV, invalid).unwrap_err();
            assert!(error.message.contains("absolute directory path"));
        }
    }

    #[test]
    fn execution_resources_fail_before_use_when_scratch_is_unavailable() {
        let spill = Arc::new(cdf_runtime::FixedSpillBudget::new(1024).unwrap());
        let held = spill.try_reserve(1024).unwrap().unwrap();
        let coordinator: Arc<dyn cdf_runtime::SpillBudgetCoordinator> = spill.clone();
        let error = DuckDbNativeResources::for_budgets_with_overrides(
            DUCKDB_CONSERVATIVE_MEMORY_BYTES,
            coordinator,
            DuckDbNativeResourceOverrides::default(),
        )
        .unwrap_err();
        assert!(
            error
                .message
                .contains("shared spill budget is already committed")
        );
        drop(held);
        assert_eq!(spill.snapshot().current_bytes, 0);
    }

    #[test]
    fn typed_oom_rolls_back_before_retrying_with_lower_scan_concurrency() {
        let spill = Arc::new(cdf_runtime::FixedSpillBudget::new(2 * 1024 * 1024).unwrap());
        let resources = DuckDbNativeResources::for_budgets_with_overrides(
            4 * 1024 * 1024 * 1024,
            spill,
            DuckDbNativeResourceOverrides {
                memory_limit_bytes: Some(4 * 1024 * 1024 * 1024),
                maximum_temp_directory_bytes: Some(1024 * 1024),
                internal_threads: Some(16),
                scan_threads: None,
                max_in_flight_bytes: None,
                profiling_directory: None,
            },
        )
        .unwrap();
        let schema = Schema::new(
            (0..2_052)
                .map(|index| Field::new(format!("field_{index}"), DataType::Utf8, true))
                .collect::<Vec<_>>(),
        );
        let envelope =
            DuckDbIngestEnvelope::resolve(&resources, &schema, 64 * 1024, 16 * 1024 * 1024)
                .unwrap();
        assert_eq!(envelope.initial_scan_threads(), 2);

        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "BEGIN TRANSACTION; CREATE TABLE attempt_rows(value BIGINT); INSERT INTO attempt_rows VALUES (1)",
        )
        .unwrap();
        let disposition = resolve_failed_ingest_attempt(
            &conn,
            DuckDbFailure {
                exception_type: DuckDbExceptionType::OutOfMemory,
                error: CdfError::destination("synthetic typed DuckDB OOM"),
            },
            envelope,
            2,
            &resources,
        )
        .unwrap();
        assert!(matches!(disposition, FailedIngestDisposition::Retry(1)));
        let table_count = conn
            .query_row(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = 'attempt_rows'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap();
        assert_eq!(table_count, 0);

        conn.execute_batch(
            "BEGIN TRANSACTION; CREATE TABLE attempt_rows(value BIGINT); INSERT INTO attempt_rows VALUES (2); COMMIT",
        )
        .unwrap();
        let value = conn
            .query_row("SELECT value FROM attempt_rows", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap();
        assert_eq!(value, 2);
    }
}
