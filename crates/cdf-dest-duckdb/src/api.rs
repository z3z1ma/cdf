use crate::*;
use crate::{
    commit::*, mirrors::*, package::*, planning::*, receipts::*, sheet::*, sql::*, table::*,
};

#[derive(Clone, Debug)]
pub struct DuckDbDestination {
    database_path: PathBuf,
    sheet: DestinationSheet,
    // 10x: kernel begin lacks DuckDB package inputs; remove this handoff once begin carries package replay inputs.
    pending_sessions: Arc<Mutex<BTreeMap<PlanId, DuckDbCommitRequest>>>,
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
    ArrowIpcPackageRows,
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
    migrations_applied: bool,
    expected_segments: BTreeMap<cdf_kernel::SegmentId, ExpectedSegment>,
    expected_order: Vec<cdf_kernel::SegmentId>,
    accepted_segments: BTreeSet<cdf_kernel::SegmentId>,
    staged_segments: Vec<CommitSegment>,
}

#[derive(Clone, Debug)]
struct ExpectedSegment {
    state: StateSegment,
    package_byte_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DuckDbCommitPlan {
    pub kernel: CommitPlan,
    pub ddl: Vec<String>,
    pub bulk_path: BulkPath,
    pub target_exists: bool,
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

#[derive(Clone, Debug)]
pub(crate) struct PackageData {
    pub(crate) fields: Vec<FieldPlan>,
    pub(crate) segments: Vec<LoadedSegment>,
    pub(crate) rows: Vec<RowValues>,
}

#[derive(Clone, Debug)]
pub(crate) struct LoadedSegment {
    pub(crate) entry: SegmentEntry,
    pub(crate) row_count: u64,
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

pub(crate) type RowValues = Vec<CellValue>;

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
            pending_sessions: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    pub fn database_path(&self) -> &Path {
        &self.database_path
    }

    pub fn capabilities(&self) -> DuckDbCapabilities {
        DuckDbCapabilities {
            sheet: self.sheet.clone(),
            bulk_paths: vec![BulkPath::ArrowIpcPackageRows],
            single_writer_lock: self.lock_path().display().to_string(),
            parquet_replay: CapabilitySupport::Unsupported,
            timezone_support: TimezoneSupport {
                requires_icu_probe: true,
                timezone_aware_timestamps: CapabilitySupport::Unsupported,
            },
        }
    }

    pub fn plan_package_commit(&self, request: &DuckDbCommitRequest) -> Result<DuckDbCommitPlan> {
        let package = load_package_data(&request.package_dir)?;
        let plan = if self.database_path.exists() {
            let conn = self.open_connection()?;
            self.plan_loaded_package(Some(&conn), request, &package)
        } else {
            self.plan_loaded_package(None, request, &package)
        }?;
        self.remember_session_context(&plan.kernel.plan_id, request)?;
        Ok(plan)
    }

    pub fn plan_schema_commit(
        &self,
        request: &DestinationCommitRequest,
        schema: &Schema,
    ) -> Result<DuckDbCommitPlan> {
        let fields = schema
            .fields()
            .iter()
            .map(|field| field_plan(field.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        validate_field_names(&fields)?;
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
            bulk_path: BulkPath::ArrowIpcPackageRows,
            target_exists: table_plan.target_exists,
        })
    }

    pub fn commit_package(&self, request: DuckDbCommitRequest) -> Result<DuckDbCommitOutcome> {
        let reader = PackageReader::open(&request.package_dir)?;
        reader.verify()?;
        let commit_segments = reader.read_commit_segments(&request.commit.segments)?;
        let mut session = DuckDbCommitSession::new(self, request)?;
        session.apply_migrations()?;
        for segment in commit_segments {
            session.write_segment(segment)?;
        }
        session.finalize_outcome()
    }

    fn commit_package_immediate(
        &self,
        request: DuckDbCommitRequest,
        package: PackageData,
    ) -> Result<DuckDbCommitOutcome> {
        let lock = self.acquire_writer_lock()?;
        let mut conn = self.open_connection()?;
        ensure_mirror_tables(&conn)?;

        if let Some(receipt) = find_duplicate_receipt(&conn, &request.commit)? {
            let recorded = record_package_receipt_once(&request.package_dir, &receipt)?;
            let plan = self.plan_loaded_package(Some(&conn), &request, &package)?;
            drop(lock);
            return Ok(DuckDbCommitOutcome {
                receipt,
                duplicate: true,
                plan,
                package_receipt_recorded: recorded,
            });
        }

        validate_requested_segments(&request.commit.segments, &package)?;
        let plan = self.plan_loaded_package(Some(&conn), &request, &package)?;
        let segment_acks = segment_acks(&request.commit.segments, &package);
        let duckdb_version = duckdb_version(&conn).unwrap_or_else(|_| "unknown".to_owned());
        let committed_at_ms = now_ms()?;
        let table_plan = plan_table_from_commit_plan(&plan)?;

        let counts = {
            let tx = conn
                .transaction()
                .map_err(|error| duckdb_error("begin transaction", error))?;
            apply_table_plan(&tx, &table_plan, request.commit.disposition.clone())?;
            let counts = match request.commit.disposition {
                WriteDisposition::Append => {
                    append_rows(&tx, &table_plan.target, &package.fields, &package.rows)?
                }
                WriteDisposition::Replace => {
                    append_rows(&tx, &table_plan.target, &package.fields, &package.rows)?
                }
                WriteDisposition::Merge => {
                    let key_indexes = merge_key_indexes(&package.fields, &request.merge_keys)?;
                    let deduped = dedup_merge_rows(&package.rows, &key_indexes)?;
                    merge_rows(
                        &tx,
                        &table_plan.target,
                        &package.fields,
                        &request.merge_keys,
                        &deduped,
                    )?
                }
                WriteDisposition::CdcApply => {
                    return Err(CdfError::contract(
                        "DuckDB destination does not support cdc_apply in the MVP sheet",
                    ));
                }
            };

            let receipt = build_receipt(
                &request,
                &segment_acks,
                counts.clone(),
                &ReceiptBuildContext {
                    migrations: &plan.kernel.migrations,
                    committed_at_ms,
                    duckdb_version: &duckdb_version,
                    database_path: &self.database_path,
                    lock_path: &self.lock_path(),
                },
            )?;
            insert_mirrors(&tx, &request, &segment_acks, &receipt)?;
            tx.commit()
                .map_err(|error| duckdb_error("commit transaction", error))?;
            counts
        };

        let receipt = build_receipt(
            &request,
            &segment_acks,
            counts,
            &ReceiptBuildContext {
                migrations: &plan.kernel.migrations,
                committed_at_ms,
                duckdb_version: &duckdb_version,
                database_path: &self.database_path,
                lock_path: &self.lock_path(),
            },
        )?;
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

    fn open_connection(&self) -> Result<Connection> {
        Connection::open(&self.database_path)
            .map_err(|error| duckdb_error(format!("open {}", self.database_path.display()), error))
    }

    fn open_read_only_connection(&self) -> Result<Connection> {
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

    fn lock_path(&self) -> PathBuf {
        let file_name = self
            .database_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("duckdb");
        self.database_path
            .with_file_name(format!("{file_name}.{LOCK_SUFFIX}"))
    }

    fn plan_loaded_package(
        &self,
        conn: Option<&Connection>,
        request: &DuckDbCommitRequest,
        package: &PackageData,
    ) -> Result<DuckDbCommitPlan> {
        let target = parse_target(&request.commit.target)?;
        let table_plan = match conn {
            Some(conn) => plan_table(
                conn,
                target,
                &package.fields,
                request.commit.disposition.clone(),
            )?,
            None => plan_absent_table(target, &package.fields, request.commit.disposition.clone())?,
        };
        let mut kernel = self.plan_commit(&request.commit)?;
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
            bulk_path: BulkPath::ArrowIpcPackageRows,
            target_exists: table_plan.target_exists,
        })
    }

    fn remember_session_context(
        &self,
        plan_id: &PlanId,
        request: &DuckDbCommitRequest,
    ) -> Result<()> {
        let mut pending = self
            .pending_sessions
            .lock()
            .map_err(|_| CdfError::internal("DuckDB commit-session context cache is poisoned"))?;
        pending.insert(plan_id.clone(), request.clone());
        Ok(())
    }

    fn take_session_context(
        &self,
        plan_id: &PlanId,
        request: &DestinationCommitRequest,
    ) -> Result<DuckDbCommitRequest> {
        let mut pending = self
            .pending_sessions
            .lock()
            .map_err(|_| CdfError::internal("DuckDB commit-session context cache is poisoned"))?;
        let Some(duckdb_request) = pending.get(plan_id).cloned() else {
            return Err(CdfError::contract(
                "DuckDB DestinationProtocol::begin requires a prior plan_package_commit for the same package plan",
            ));
        };
        if duckdb_request.commit != *request {
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
        request: DuckDbCommitRequest,
    ) -> Result<DuckDbCommitSession<'_>> {
        let (expected_segments, expected_order) = expected_segments_for_request(&request)?;
        Ok(DuckDbCommitSession {
            destination,
            request,
            migrations_applied: false,
            expected_segments,
            expected_order,
            accepted_segments: BTreeSet::new(),
            staged_segments: Vec::new(),
        })
    }

    fn finalize_outcome(self) -> Result<DuckDbCommitOutcome> {
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

        let mut staged_by_id = BTreeMap::new();
        for segment in self.staged_segments {
            staged_by_id.insert(segment.state.segment_id.clone(), segment);
        }
        let mut ordered_segments = Vec::with_capacity(self.expected_order.len());
        for segment_id in &self.expected_order {
            let segment = staged_by_id.remove(segment_id).ok_or_else(|| {
                CdfError::internal(format!(
                    "accepted DuckDB segment {} is missing from staged payloads",
                    segment_id.as_str()
                ))
            })?;
            ordered_segments.push(segment);
        }

        let package = package_data_from_commit_segments(ordered_segments)?;
        self.destination
            .commit_package_immediate(self.request, package)
    }
}

impl CommitSession for DuckDbCommitSession<'_> {
    fn apply_migrations(&mut self) -> Result<()> {
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
        validate_commit_segment(&segment, expected)?;

        let ack = SegmentAck {
            segment_id: expected.state.segment_id.clone(),
            row_count: expected.state.row_count,
            byte_count: expected.state.byte_count,
        };
        self.accepted_segments.insert(segment_id);
        self.staged_segments.push(segment);
        Ok(ack)
    }

    fn finalize(self: Box<Self>) -> Result<Receipt> {
        Ok(self.finalize_outcome()?.receipt)
    }

    fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

impl DestinationProtocol for DuckDbDestination {
    fn sheet(&self) -> &DestinationSheet {
        &self.sheet
    }

    fn plan_commit(&self, request: &DestinationCommitRequest) -> Result<CommitPlan> {
        if !self
            .sheet
            .supported_dispositions
            .contains(&request.disposition)
        {
            return Err(CdfError::contract(format!(
                "DuckDB destination does not support {:?}",
                request.disposition
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
        let request = self.take_session_context(&plan.plan_id, &request)?;
        Ok(Box::new(DuckDbCommitSession::new(self, request)?))
    }

    fn verify(&self, receipt: &Receipt) -> Result<ReceiptVerification> {
        self.verify_receipt(receipt)
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
