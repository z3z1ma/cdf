use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use cdf_kernel::{
    DestinationCorrectionCommitPlan, DestinationCorrectionCommitRequest, DestinationSheet, PlanId,
};
use duckdb::types::Value;

#[derive(Clone, Debug)]
pub struct DuckDbDestination {
    pub(crate) database_path: PathBuf,
    pub(crate) sheet: DestinationSheet,
    pub(crate) execution: Option<cdf_runtime::ExecutionServices>,
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
    pub(crate) scratch_reservation: Option<Arc<cdf_runtime::SpillReservation>>,
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
    pub(crate) name: cdf_dest_sql::ValidatedSqlIdentifier,
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
    pub(crate) default_expression: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct TargetRef {
    pub(crate) schema: cdf_dest_sql::ValidatedSqlIdentifier,
    pub(crate) table: cdf_dest_sql::ValidatedSqlIdentifier,
}

pub(crate) struct ReceiptBuildContext<'a> {
    pub(crate) committed_at_ms: i64,
    pub(crate) duckdb_version: &'a str,
    pub(crate) database_path: &'a Path,
    pub(crate) lock_path: &'a Path,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CellValue {
    pub(crate) value: Value,
}

#[derive(Clone, Debug)]
pub(crate) struct DuckDbCorrectionContext {
    pub(crate) request: DestinationCorrectionCommitRequest,
    pub(crate) plan: DestinationCorrectionCommitPlan,
    pub(crate) ddl: Vec<String>,
}
