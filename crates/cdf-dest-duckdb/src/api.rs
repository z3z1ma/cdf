use crate::*;
use crate::{
    commit::*, mirrors::*, package::*, planning::*, receipts::*, sheet::*, sql::*, table::*,
};

#[derive(Clone, Debug)]
pub struct DuckDbDestination {
    database_path: PathBuf,
    sheet: DestinationSheet,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceiptVerification {
    pub verified: bool,
    pub receipt_id: ReceiptId,
    pub reason: Option<String>,
}

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
        if self.database_path.exists() {
            let conn = self.open_connection()?;
            self.plan_loaded_package(Some(&conn), request, &package)
        } else {
            self.plan_loaded_package(None, request, &package)
        }
    }

    pub fn commit_package(&self, request: DuckDbCommitRequest) -> Result<DuckDbCommitOutcome> {
        let lock = self.acquire_writer_lock()?;
        let mut conn = self.open_connection()?;
        ensure_mirror_tables(&conn)?;

        if let Some(receipt) = find_duplicate_receipt(&conn, &request.commit)? {
            let recorded = record_package_receipt_once(&request.package_dir, &receipt)?;
            let package = load_package_data(&request.package_dir)?;
            let plan = self.plan_loaded_package(Some(&conn), &request, &package)?;
            drop(lock);
            return Ok(DuckDbCommitOutcome {
                receipt,
                duplicate: true,
                plan,
                package_receipt_recorded: recorded,
            });
        }

        let package = load_package_data(&request.package_dir)?;
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
}
