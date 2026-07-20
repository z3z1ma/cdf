use std::sync::Arc;

use cdf_kernel::{CdfError, Result};
use cdf_memory::{
    ConsumerKey, MemoryClass, MemoryCoordinator, MemoryLease, ReservationRequest, reserve_blocking,
};
use cdf_runtime::{SpillBudgetCoordinator, SpillReservation};
use cdf_task_store::{ExternalTaskStore, ExternalTaskWorkspace};
use rusqlite::{Connection, ErrorCode, OptionalExtension, params};

use crate::{IcebergDeleteContent, IcebergDeleteFile, IcebergSourceOptions};

const SQLITE_PAGE_BYTES: u64 = 4096;

/// Spill-backed exact applicability index for one snapshot's delete files.
///
/// SQLite is an implementation detail of planning, not task authority. Its B-tree keeps lookup
/// logarithmic at high cardinality, while the cache lease and database page ceiling keep memory
/// and disk subordinate to CDF's injected authorities.
pub(crate) struct IcebergDeleteIndex {
    connection: Connection,
    spill: Arc<dyn SpillBudgetCoordinator>,
    spill_reservation: SpillReservation,
    spill_growth_bytes: u64,
    maximum_task_bytes: u64,
    _memory_lease: MemoryLease,
    _workspace: ExternalTaskWorkspace,
}

impl IcebergDeleteIndex {
    pub(crate) fn create(
        task_store: &ExternalTaskStore,
        source: &IcebergSourceOptions,
        memory: Arc<dyn MemoryCoordinator>,
        spill: Arc<dyn SpillBudgetCoordinator>,
    ) -> Result<Self> {
        let memory_bytes = source
            .delete_index_cache_bytes
            .checked_add(source.maximum_task_bytes)
            .ok_or_else(|| CdfError::contract("Iceberg delete-index memory budget overflowed"))?;
        let memory_lease = reserve_blocking(
            memory,
            &ReservationRequest::new(
                ConsumerKey::new("iceberg-delete-index", MemoryClass::Control)?,
                memory_bytes,
            )?,
        )?;
        let available = available_spill_bytes(spill.as_ref());
        let initial = source.delete_index_spill_growth_bytes.min(available).max(1);
        if initial < SQLITE_PAGE_BYTES * 2 {
            return Err(CdfError::data(format!(
                "Iceberg delete planning requires at least {} free spill bytes but only {available} are available; raise the run spill budget or reduce concurrent spill operators",
                SQLITE_PAGE_BYTES * 2
            )));
        }
        let spill_reservation = spill.try_reserve(initial)?.ok_or_else(|| {
            CdfError::data(
                "Iceberg delete planning could not acquire its initial shared spill reservation",
            )
        })?;
        let workspace = task_store.temporary_workspace("iceberg-delete-index")?;
        let connection = Connection::open(workspace.path().join("delete-index.sqlite"))
            .map_err(|error| sqlite_error("open Iceberg delete index", error))?;
        connection
            .pragma_update(
                None,
                "page_size",
                i64::try_from(SQLITE_PAGE_BYTES).expect("SQLite page size fits i64"),
            )
            .and_then(|_| connection.pragma_update(None, "journal_mode", "DELETE"))
            .and_then(|_| connection.pragma_update(None, "synchronous", "OFF"))
            .and_then(|_| connection.pragma_update(None, "locking_mode", "EXCLUSIVE"))
            .and_then(|_| connection.pragma_update(None, "mmap_size", 0_i64))
            .and_then(|_| connection.pragma_update(None, "cache_spill", false))
            .map_err(|error| sqlite_error("configure Iceberg delete index", error))?;
        let cache_kib = source.delete_index_cache_bytes.div_ceil(1024).max(1);
        connection
            .pragma_update(
                None,
                "cache_size",
                -i64::try_from(cache_kib).unwrap_or(i64::MAX),
            )
            .map_err(|error| sqlite_error("configure Iceberg delete-index cache", error))?;
        set_page_ceiling(&connection, spill_reservation.bytes())?;
        connection
            .execute_batch(
                "CREATE TABLE deletes (
                    scope INTEGER NOT NULL,
                    partition_spec_id INTEGER NOT NULL,
                    partition_key BLOB NOT NULL,
                    path TEXT NOT NULL,
                    content INTEGER NOT NULL,
                    file_size_bytes INTEGER NOT NULL,
                    sequence_number INTEGER,
                    referenced_data_file TEXT,
                    descriptor BLOB NOT NULL,
                    PRIMARY KEY (
                        scope, partition_spec_id, partition_key,
                        path, content, file_size_bytes
                    ),
                    UNIQUE (path)
                ) WITHOUT ROWID;",
            )
            .map_err(|error| sqlite_error("create Iceberg delete index", error))?;
        Ok(Self {
            connection,
            spill,
            spill_reservation,
            spill_growth_bytes: source.delete_index_spill_growth_bytes,
            maximum_task_bytes: source.maximum_task_bytes,
            _memory_lease: memory_lease,
            _workspace: workspace,
        })
    }

    pub(crate) fn insert(
        &mut self,
        delete: &IcebergDeleteFile,
        partition_key: &[u8],
        global_equality: bool,
    ) -> Result<()> {
        let descriptor = serde_json::to_vec(delete)
            .map_err(|error| CdfError::internal(format!("encode Iceberg delete: {error}")))?;
        let scope = i64::from(!global_equality);
        let spec_id = if global_equality {
            -1_i64
        } else {
            i64::from(delete.partition_spec_id)
        };
        let content = match delete.content {
            IcebergDeleteContent::Position => 0_i64,
            IcebergDeleteContent::Equality => 1_i64,
        };
        loop {
            let result = self.connection.execute(
                "INSERT INTO deletes (
                    scope, partition_spec_id, partition_key, path, content,
                    file_size_bytes, sequence_number, referenced_data_file, descriptor
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    scope,
                    spec_id,
                    partition_key,
                    delete.path,
                    content,
                    i64::try_from(delete.file_size_bytes).map_err(|_| {
                        CdfError::data("Iceberg delete file size exceeds SQLite i64")
                    })?,
                    delete.sequence_number,
                    delete.referenced_data_file,
                    descriptor,
                ],
            );
            match result {
                Ok(_) => return Ok(()),
                Err(error) if is_disk_full(&error) => self.grow()?,
                Err(error) if is_constraint(&error) => {
                    let existing: Option<Vec<u8>> = self
                        .connection
                        .query_row(
                            "SELECT descriptor FROM deletes WHERE path = ?1",
                            [&delete.path],
                            |row| row.get(0),
                        )
                        .optional()
                        .map_err(|query| sqlite_error("inspect duplicate Iceberg delete", query))?;
                    if existing.as_deref() == Some(descriptor.as_slice()) {
                        return Err(CdfError::data(format!(
                            "Iceberg snapshot repeats live delete file `{}`",
                            delete.path
                        )));
                    }
                    return Err(CdfError::data(format!(
                        "Iceberg snapshot carries conflicting metadata for delete file `{}`",
                        delete.path
                    )));
                }
                Err(error) => return Err(sqlite_error("index Iceberg delete", error)),
            }
        }
    }

    pub(crate) fn applicable(
        &self,
        partition_spec_id: i32,
        partition_key: &[u8],
        data_path: &str,
        data_sequence_number: Option<i64>,
        maximum_task_bytes: u64,
    ) -> Result<Vec<IcebergDeleteFile>> {
        if maximum_task_bytes != self.maximum_task_bytes {
            return Err(CdfError::internal(
                "Iceberg delete lookup task budget differs from its indexed authority",
            ));
        }
        let mut deletes = Vec::new();
        let mut encoded_bytes = 0_u64;
        self.query(
            "SELECT descriptor FROM deletes
             WHERE scope = 0
               AND (?1 IS NULL OR sequence_number > ?1)
             ORDER BY path, content, file_size_bytes",
            rusqlite::params![data_sequence_number],
            &mut deletes,
            &mut encoded_bytes,
        )?;
        self.query(
            "SELECT descriptor FROM deletes
             WHERE scope = 1 AND partition_spec_id = ?1 AND partition_key = ?2
               AND (referenced_data_file IS NULL OR referenced_data_file = ?3)
               AND (
                    ?4 IS NULL OR
                    (content = 0 AND sequence_number >= ?4) OR
                    (content = 1 AND sequence_number > ?4)
               )
             ORDER BY path, content, file_size_bytes",
            rusqlite::params![
                partition_spec_id,
                partition_key,
                data_path,
                data_sequence_number
            ],
            &mut deletes,
            &mut encoded_bytes,
        )?;
        deletes.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.content.cmp(&right.content))
                .then_with(|| left.file_size_bytes.cmp(&right.file_size_bytes))
        });
        if deletes.windows(2).any(|pair| pair[0].path == pair[1].path) {
            return Err(CdfError::data(
                "Iceberg delete index produced a duplicate applicable delete",
            ));
        }
        Ok(deletes)
    }

    fn query<P: rusqlite::Params>(
        &self,
        sql: &str,
        params: P,
        output: &mut Vec<IcebergDeleteFile>,
        encoded_bytes: &mut u64,
    ) -> Result<()> {
        let mut statement = self
            .connection
            .prepare(sql)
            .map_err(|error| sqlite_error("prepare Iceberg delete lookup", error))?;
        let rows = statement
            .query_map(params, |row| row.get::<_, Vec<u8>>(0))
            .map_err(|error| sqlite_error("query Iceberg delete index", error))?;
        for row in rows {
            let encoded = row.map_err(|error| sqlite_error("read Iceberg delete index", error))?;
            *encoded_bytes = encoded_bytes
                .checked_add(u64::try_from(encoded.len()).unwrap_or(u64::MAX))
                .ok_or_else(|| CdfError::data("Iceberg applicable delete bytes overflowed u64"))?;
            if *encoded_bytes > self.maximum_task_bytes {
                return Err(CdfError::data(format!(
                    "Iceberg data file requires more than maximum_task_bytes={} of delete descriptors; raise the source task budget",
                    self.maximum_task_bytes
                )));
            }
            output.push(serde_json::from_slice(&encoded).map_err(|error| {
                CdfError::data(format!("decode indexed Iceberg delete: {error}"))
            })?);
        }
        Ok(())
    }

    fn grow(&mut self) -> Result<()> {
        let available = available_spill_bytes(self.spill.as_ref());
        let additional = self.spill_growth_bytes.min(available);
        if additional < SQLITE_PAGE_BYTES * 2 || !self.spill_reservation.try_grow(additional)? {
            return Err(CdfError::data(format!(
                "Iceberg delete index exhausted its shared spill budget after {} bytes; raise the run spill budget or reduce concurrent spill operators",
                self.spill_reservation.bytes()
            )));
        }
        set_page_ceiling(&self.connection, self.spill_reservation.bytes())
    }
}

fn set_page_ceiling(connection: &Connection, reserved_bytes: u64) -> Result<()> {
    // The rollback journal can transiently approach the database size. Restrict the database to
    // half the reservation so database + journal always remain inside CDF's spill authority.
    let pages = (reserved_bytes / 2 / SQLITE_PAGE_BYTES).max(1);
    connection
        .pragma_update(
            None,
            "max_page_count",
            i64::try_from(pages).unwrap_or(i64::MAX),
        )
        .map_err(|error| sqlite_error("raise Iceberg delete-index page ceiling", error))
}

fn available_spill_bytes(spill: &dyn SpillBudgetCoordinator) -> u64 {
    let snapshot = spill.snapshot();
    snapshot.budget_bytes.saturating_sub(snapshot.current_bytes)
}

fn is_disk_full(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(failure, _)
            if failure.code == ErrorCode::DiskFull
    )
}

fn is_constraint(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(failure, _)
            if failure.code == ErrorCode::ConstraintViolation
    )
}

fn sqlite_error(action: &str, error: rusqlite::Error) -> CdfError {
    CdfError::data(format!("{action}: {error}"))
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use cdf_kernel::ContentStoreNamespace;
    use cdf_memory::DeterministicMemoryCoordinator;
    use cdf_runtime::{FixedSpillBudget, SpillBudgetCoordinator};

    use super::*;
    use crate::IcebergFileFormat;

    fn options() -> IcebergSourceOptions {
        let mut source: IcebergSourceOptions = serde_json::from_value(serde_json::json!({
            "catalog": {"kind": "filesystem", "warehouse": ".warehouse"}
        }))
        .unwrap();
        source.delete_index_cache_bytes = 1024 * 1024;
        source.delete_index_spill_growth_bytes = 4 * 1024 * 1024;
        source
    }

    fn deletion(
        path: &str,
        content: IcebergDeleteContent,
        sequence_number: i64,
        referenced_data_file: Option<&str>,
    ) -> IcebergDeleteFile {
        IcebergDeleteFile {
            path: path.to_owned(),
            format: IcebergFileFormat::Parquet,
            content,
            file_size_bytes: 1024,
            object_generation: format!("sha256:{}", "0".repeat(64)),
            content_sha256: None,
            partition_spec_id: 7,
            record_count: Some(1),
            sequence_number: Some(sequence_number),
            file_sequence_number: Some(sequence_number),
            equality_field_ids: match content {
                IcebergDeleteContent::Equality => vec![1],
                IcebergDeleteContent::Position => Vec::new(),
            },
            referenced_data_file: referenced_data_file.map(str::to_owned),
        }
    }

    #[test]
    fn applicability_is_exact_spill_backed_and_sequence_aware() {
        let root = tempfile::tempdir().unwrap();
        let store = ExternalTaskStore::new(
            root.path(),
            ContentStoreNamespace::new("iceberg-test").unwrap(),
        )
        .unwrap();
        let memory = Arc::new(
            DeterministicMemoryCoordinator::new(8 * 1024 * 1024, BTreeMap::new()).unwrap(),
        );
        let spill: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(FixedSpillBudget::new(16 * 1024 * 1024).unwrap());
        let mut index =
            IcebergDeleteIndex::create(&store, &options(), memory, Arc::clone(&spill)).unwrap();
        index
            .insert(
                &deletion(
                    "global-equality.parquet",
                    IcebergDeleteContent::Equality,
                    5,
                    None,
                ),
                &[],
                true,
            )
            .unwrap();
        index
            .insert(
                &deletion(
                    "partition-equality.parquet",
                    IcebergDeleteContent::Equality,
                    6,
                    None,
                ),
                br#"["west"]"#,
                false,
            )
            .unwrap();
        index
            .insert(
                &deletion(
                    "position.parquet",
                    IcebergDeleteContent::Position,
                    4,
                    Some("data.parquet"),
                ),
                br#"["west"]"#,
                false,
            )
            .unwrap();

        let deletes = index
            .applicable(
                7,
                br#"["west"]"#,
                "data.parquet",
                Some(4),
                options().maximum_task_bytes,
            )
            .unwrap();
        assert_eq!(
            deletes
                .iter()
                .map(|delete| delete.path.as_str())
                .collect::<Vec<_>>(),
            [
                "global-equality.parquet",
                "partition-equality.parquet",
                "position.parquet"
            ]
        );
        assert_eq!(
            index
                .applicable(
                    7,
                    br#"["west"]"#,
                    "other.parquet",
                    Some(5),
                    options().maximum_task_bytes,
                )
                .unwrap()
                .iter()
                .map(|delete| delete.path.as_str())
                .collect::<Vec<_>>(),
            ["partition-equality.parquet"]
        );
        assert!(spill.snapshot().current_bytes > 0);
        drop(index);
        assert_eq!(spill.snapshot().current_bytes, 0);
    }
}
