use std::{path::PathBuf, sync::Arc};

use cdf_kernel::{CdfError, Result};
use cdf_memory::{
    ConsumerKey, MemoryClass, MemoryCoordinator, MemoryLease, ReservationRequest, reserve_blocking,
};
use cdf_runtime::{SpillBudgetCoordinator, SpillReservation};
use cdf_task_store::{ExternalTaskStore, ExternalTaskWorkspace};
use rusqlite::{Connection, ErrorCode, OptionalExtension, params};

use iceberg::spec::ManifestContentType;

use crate::{IcebergDeleteContent, IcebergDeleteFile, IcebergSourceOptions};

const SQLITE_PAGE_BYTES: u64 = 4096;
const MANIFEST_READER_CACHE_BYTES: u64 = 64 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IcebergPlanningManifest {
    pub manifest_path: String,
    pub manifest_length: i64,
    pub partition_spec_id: i32,
    pub content: ManifestContentType,
    pub sequence_number: i64,
    pub added_snapshot_id: i64,
}

/// Spill-backed exact planning index for one snapshot's manifests and delete files.
///
/// SQLite is an implementation detail of planning, not task authority. Its B-tree keeps lookup
/// logarithmic at high cardinality, while the cache lease and database page ceiling keep memory
/// and disk subordinate to CDF's injected authorities.
pub(crate) struct IcebergPlanningIndex {
    connection: Connection,
    database_path: PathBuf,
    spill: Arc<dyn SpillBudgetCoordinator>,
    spill_reservation: SpillReservation,
    spill_growth_bytes: u64,
    maximum_task_bytes: u64,
    manifest_counts: [u64; 2],
    _memory_lease: MemoryLease,
    _workspace: ExternalTaskWorkspace,
}

impl IcebergPlanningIndex {
    pub(crate) fn create(
        task_store: &ExternalTaskStore,
        source: &IcebergSourceOptions,
        memory: Arc<dyn MemoryCoordinator>,
        spill: Arc<dyn SpillBudgetCoordinator>,
    ) -> Result<Self> {
        let memory_bytes = source
            .planning_index_cache_bytes
            .checked_add(source.maximum_task_bytes)
            .and_then(|bytes| bytes.checked_add(MANIFEST_READER_CACHE_BYTES))
            .ok_or_else(|| CdfError::contract("Iceberg planning-index memory budget overflowed"))?;
        let memory_lease = reserve_blocking(
            memory,
            &ReservationRequest::new(
                ConsumerKey::new("iceberg-planning-index", MemoryClass::Control)?,
                memory_bytes,
            )?,
        )?;
        let available = available_spill_bytes(spill.as_ref());
        let initial = source
            .planning_index_spill_growth_bytes
            .min(available)
            .max(1);
        if initial < SQLITE_PAGE_BYTES * 2 {
            return Err(CdfError::data(format!(
                "Iceberg snapshot planning requires at least {} free spill bytes but only {available} are available; raise the run spill budget or reduce concurrent spill operators",
                SQLITE_PAGE_BYTES * 2
            )));
        }
        let spill_reservation = spill.try_reserve(initial)?.ok_or_else(|| {
            CdfError::data(
                "Iceberg snapshot planning could not acquire its initial shared spill reservation",
            )
        })?;
        let workspace = task_store.temporary_workspace("iceberg-planning-index")?;
        let database_path = workspace.path().join("planning-index.sqlite");
        let connection = Connection::open(&database_path)
            .map_err(|error| sqlite_error("open Iceberg planning index", error))?;
        connection
            .pragma_update(
                None,
                "page_size",
                i64::try_from(SQLITE_PAGE_BYTES).expect("SQLite page size fits i64"),
            )
            .and_then(|_| connection.pragma_update(None, "journal_mode", "DELETE"))
            .and_then(|_| connection.pragma_update(None, "synchronous", "OFF"))
            .and_then(|_| connection.pragma_update(None, "locking_mode", "NORMAL"))
            .and_then(|_| connection.pragma_update(None, "mmap_size", 0_i64))
            // Manifest ingestion is one transaction. Dirty pages must spill inside the explicit
            // cache lease instead of making transaction cardinality resident in SQLite's cache.
            .and_then(|_| connection.pragma_update(None, "cache_spill", true))
            .map_err(|error| sqlite_error("configure Iceberg planning index", error))?;
        let cache_kib = source.planning_index_cache_bytes.div_ceil(1024).max(1);
        connection
            .pragma_update(
                None,
                "cache_size",
                -i64::try_from(cache_kib).unwrap_or(i64::MAX),
            )
            .map_err(|error| sqlite_error("configure Iceberg planning-index cache", error))?;
        set_page_ceiling(&connection, spill_reservation.bytes())?;
        connection
            .execute_batch(
                "CREATE TABLE manifests (
                    path TEXT PRIMARY KEY,
                    content INTEGER NOT NULL,
                    manifest_length INTEGER NOT NULL,
                    partition_spec_id INTEGER NOT NULL,
                    sequence_number INTEGER NOT NULL,
                    added_snapshot_id INTEGER NOT NULL
                ) WITHOUT ROWID;
                 CREATE INDEX manifests_by_content_path ON manifests (content, path);
                 CREATE TABLE deletes (
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
            .map_err(|error| sqlite_error("create Iceberg planning index", error))?;
        Ok(Self {
            connection,
            database_path,
            spill,
            spill_reservation,
            spill_growth_bytes: source.planning_index_spill_growth_bytes,
            maximum_task_bytes: source.maximum_task_bytes,
            manifest_counts: [0; 2],
            _memory_lease: memory_lease,
            _workspace: workspace,
        })
    }

    pub(crate) fn begin_manifest_ingest(&self) -> Result<()> {
        if self.manifest_counts != [0; 2] || !self.connection.is_autocommit() {
            return Err(CdfError::internal(
                "Iceberg manifest ingestion began with nonempty transaction state",
            ));
        }
        self.connection
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|error| sqlite_error("begin Iceberg manifest ingestion", error))
    }

    pub(crate) fn finish_manifest_ingest(&self) -> Result<bool> {
        match self.connection.execute_batch("COMMIT") {
            Ok(()) => Ok(true),
            Err(error) if is_disk_full(&error) => Ok(false),
            Err(error) => Err(sqlite_error("commit Iceberg manifest ingestion", error)),
        }
    }

    pub(crate) fn insert_manifest(&mut self, manifest: &IcebergPlanningManifest) -> Result<bool> {
        if self.connection.is_autocommit() {
            return Err(CdfError::internal(
                "Iceberg manifest insert requires an active ingestion transaction",
            ));
        }
        let content = manifest_content_value(manifest.content);
        let count_index = usize::try_from(content).expect("manifest content index is 0 or 1");
        let next_count = self.manifest_counts[count_index]
            .checked_add(1)
            .ok_or_else(|| CdfError::data("indexed Iceberg manifest count exceeds u64"))?;
        let result = self.connection.execute(
            "INSERT INTO manifests (
                path, content, manifest_length, partition_spec_id,
                sequence_number, added_snapshot_id
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                manifest.manifest_path,
                content,
                manifest.manifest_length,
                manifest.partition_spec_id,
                manifest.sequence_number,
                manifest.added_snapshot_id,
            ],
        );
        match result {
            Ok(_) => {
                self.manifest_counts[count_index] = next_count;
                Ok(true)
            }
            Err(error) if is_disk_full(&error) => Ok(false),
            Err(error) if is_constraint(&error) => Err(CdfError::data(format!(
                "Iceberg manifest list repeats manifest path `{}`",
                manifest.manifest_path
            ))),
            Err(error) => Err(sqlite_error("index Iceberg manifest", error)),
        }
    }

    pub(crate) fn restart_manifest_ingest_after_spill_full(&mut self) -> Result<()> {
        if !self.connection.is_autocommit() {
            self.connection
                .execute_batch("ROLLBACK")
                .map_err(|error| sqlite_error("roll back full Iceberg manifest index", error))?;
        }
        let retained_rows: i64 = self
            .connection
            .query_row("SELECT COUNT(*) FROM manifests", [], |row| row.get(0))
            .map_err(|error| sqlite_error("verify rolled-back Iceberg manifest index", error))?;
        if retained_rows != 0 {
            return Err(CdfError::internal(
                "full Iceberg manifest transaction retained partially indexed rows",
            ));
        }
        self.manifest_counts = [0; 2];
        self.grow()
    }

    pub(crate) fn abort_manifest_ingest(&mut self) -> Result<()> {
        if !self.connection.is_autocommit() {
            self.connection
                .execute_batch("ROLLBACK")
                .map_err(|error| sqlite_error("roll back Iceberg manifest ingestion", error))?;
        }
        self.manifest_counts = [0; 2];
        Ok(())
    }

    pub(crate) fn manifest_count(&self, content: ManifestContentType) -> u64 {
        self.manifest_counts
            [usize::try_from(manifest_content_value(content)).expect("manifest content index")]
    }

    pub(crate) fn manifest_reader(
        &self,
        content: ManifestContentType,
    ) -> Result<IcebergPlanningManifestReader> {
        let connection = Connection::open(&self.database_path)
            .map_err(|error| sqlite_error("open Iceberg manifest reader", error))?;
        configure_manifest_reader(&connection)?;
        Ok(IcebergPlanningManifestReader {
            connection,
            content,
            count: self.manifest_count(content),
        })
    }

    pub(crate) fn manifest_cursor(
        &self,
        content: ManifestContentType,
    ) -> Result<IcebergPlanningManifestCursor> {
        let connection = Connection::open(&self.database_path)
            .map_err(|error| sqlite_error("open Iceberg manifest cursor", error))?;
        configure_manifest_reader(&connection)?;
        Ok(IcebergPlanningManifestCursor {
            connection,
            content,
            count: self.manifest_count(content),
            after_path: None,
            exhausted: false,
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
                "Iceberg planning index exhausted its shared spill budget after {} bytes; raise the run spill budget or reduce concurrent spill operators",
                self.spill_reservation.bytes()
            )));
        }
        set_page_ceiling(&self.connection, self.spill_reservation.bytes())
    }
}

pub(crate) struct IcebergPlanningManifestReader {
    connection: Connection,
    content: ManifestContentType,
    count: u64,
}

pub(crate) struct IcebergPlanningManifestCursor {
    connection: Connection,
    content: ManifestContentType,
    count: u64,
    after_path: Option<String>,
    exhausted: bool,
}

impl IcebergPlanningManifestReader {
    pub(crate) fn consume<T>(
        self,
        consume: impl FnOnce(
            u64,
            &mut dyn Iterator<Item = Result<IcebergPlanningManifest>>,
        ) -> Result<T>,
    ) -> Result<T> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT path, manifest_length, partition_spec_id,
                        sequence_number, added_snapshot_id
                 FROM manifests
                 WHERE content = ?1
                 ORDER BY path",
            )
            .map_err(|error| sqlite_error("prepare Iceberg manifest traversal", error))?;
        let rows = statement
            .query_map([manifest_content_value(self.content)], |row| {
                Ok(IcebergPlanningManifest {
                    manifest_path: row.get(0)?,
                    manifest_length: row.get(1)?,
                    partition_spec_id: row.get(2)?,
                    content: self.content,
                    sequence_number: row.get(3)?,
                    added_snapshot_id: row.get(4)?,
                })
            })
            .map_err(|error| sqlite_error("query Iceberg manifests", error))?;
        let mut manifests = rows
            .map(|row| row.map_err(|error| sqlite_error("read indexed Iceberg manifest", error)));
        consume(self.count, &mut manifests)
    }
}

impl IcebergPlanningManifestCursor {
    pub(crate) fn manifest_count(&self) -> u64 {
        self.count
    }
}

impl Iterator for IcebergPlanningManifestCursor {
    type Item = Result<IcebergPlanningManifest>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }
        let map_row = |row: &rusqlite::Row<'_>| {
            Ok(IcebergPlanningManifest {
                manifest_path: row.get(0)?,
                manifest_length: row.get(1)?,
                partition_spec_id: row.get(2)?,
                content: self.content,
                sequence_number: row.get(3)?,
                added_snapshot_id: row.get(4)?,
            })
        };
        let content = manifest_content_value(self.content);
        let result = match self.after_path.as_deref() {
            Some(after_path) => self.connection.query_row(
                "SELECT path, manifest_length, partition_spec_id,
                        sequence_number, added_snapshot_id
                 FROM manifests
                 WHERE content = ?1 AND path > ?2
                 ORDER BY path
                 LIMIT 1",
                params![content, after_path],
                map_row,
            ),
            None => self.connection.query_row(
                "SELECT path, manifest_length, partition_spec_id,
                        sequence_number, added_snapshot_id
                 FROM manifests
                 WHERE content = ?1
                 ORDER BY path
                 LIMIT 1",
                [content],
                map_row,
            ),
        }
        .optional();
        match result {
            Ok(Some(manifest)) => {
                self.after_path = Some(manifest.manifest_path.clone());
                Some(Ok(manifest))
            }
            Ok(None) => {
                self.exhausted = true;
                None
            }
            Err(error) => {
                self.exhausted = true;
                Some(Err(sqlite_error("advance Iceberg manifest cursor", error)))
            }
        }
    }
}

fn configure_manifest_reader(connection: &Connection) -> Result<()> {
    connection
        .pragma_update(None, "query_only", true)
        .and_then(|_| connection.pragma_update(None, "mmap_size", 0_i64))
        .and_then(|_| {
            connection.pragma_update(
                None,
                "cache_size",
                -i64::try_from(MANIFEST_READER_CACHE_BYTES / 1024)
                    .expect("manifest reader cache fits i64"),
            )
        })
        .map_err(|error| sqlite_error("configure Iceberg manifest reader", error))
}

const fn manifest_content_value(content: ManifestContentType) -> i64 {
    match content {
        ManifestContentType::Data => 0,
        ManifestContentType::Deletes => 1,
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
        .map_err(|error| sqlite_error("raise Iceberg planning-index page ceiling", error))
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
        source.planning_index_cache_bytes = 1024 * 1024;
        source.planning_index_spill_growth_bytes = 4 * 1024 * 1024;
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
            IcebergPlanningIndex::create(&store, &options(), memory, Arc::clone(&spill)).unwrap();
        index.begin_manifest_ingest().unwrap();
        for manifest in [
            IcebergPlanningManifest {
                manifest_path: "z-data.avro".to_owned(),
                manifest_length: 100,
                partition_spec_id: 1,
                content: ManifestContentType::Data,
                sequence_number: 4,
                added_snapshot_id: 9,
            },
            IcebergPlanningManifest {
                manifest_path: "a-data.avro".to_owned(),
                manifest_length: 200,
                partition_spec_id: 0,
                content: ManifestContentType::Data,
                sequence_number: 3,
                added_snapshot_id: 9,
            },
            IcebergPlanningManifest {
                manifest_path: "m-delete.avro".to_owned(),
                manifest_length: 300,
                partition_spec_id: 1,
                content: ManifestContentType::Deletes,
                sequence_number: 5,
                added_snapshot_id: 9,
            },
        ] {
            assert!(index.insert_manifest(&manifest).unwrap());
        }
        let duplicate = index
            .insert_manifest(&IcebergPlanningManifest {
                manifest_path: "a-data.avro".to_owned(),
                manifest_length: 201,
                partition_spec_id: 0,
                content: ManifestContentType::Data,
                sequence_number: 3,
                added_snapshot_id: 9,
            })
            .unwrap_err();
        assert!(duplicate.message.contains("repeats manifest path"));
        assert!(index.finish_manifest_ingest().unwrap());
        assert_eq!(index.manifest_count(ManifestContentType::Data), 2);
        let reader = index.manifest_reader(ManifestContentType::Data).unwrap();
        let paths = reader
            .consume(|count, manifests| {
                assert_eq!(count, 2);
                manifests
                    .map(|manifest| manifest.map(|manifest| manifest.manifest_path))
                    .collect::<Result<Vec<_>>>()
            })
            .unwrap();
        assert_eq!(paths, ["a-data.avro", "z-data.avro"]);
        let mut cursor = index.manifest_cursor(ManifestContentType::Deletes).unwrap();
        assert_eq!(cursor.manifest_count(), 1);
        assert_eq!(
            cursor.next().unwrap().unwrap().manifest_path,
            "m-delete.avro"
        );
        assert!(cursor.next().is_none());
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

    #[test]
    fn manifest_cardinality_does_not_change_ledger_residency() {
        const MANIFEST_COUNT: u64 = 10_000;

        let root = tempfile::tempdir().unwrap();
        let store = ExternalTaskStore::new(
            root.path(),
            ContentStoreNamespace::new("iceberg-cardinality-test").unwrap(),
        )
        .unwrap();
        let memory = Arc::new(
            DeterministicMemoryCoordinator::new(8 * 1024 * 1024, BTreeMap::new()).unwrap(),
        );
        let spill: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(FixedSpillBudget::new(16 * 1024 * 1024).unwrap());
        let mut source = options();
        source.planning_index_spill_growth_bytes = 64 * 1024;
        let mut index =
            IcebergPlanningIndex::create(&store, &source, memory.clone(), Arc::clone(&spill))
                .unwrap();
        let retained_bytes = memory.snapshot().current_bytes;

        loop {
            index.begin_manifest_ingest().unwrap();
            let mut complete = true;
            for ordinal in (0..MANIFEST_COUNT).rev() {
                if !index
                    .insert_manifest(&IcebergPlanningManifest {
                        manifest_path: format!("manifest-{ordinal:05}.avro"),
                        manifest_length: 512,
                        partition_spec_id: 0,
                        content: ManifestContentType::Data,
                        sequence_number: 7,
                        added_snapshot_id: 11,
                    })
                    .unwrap()
                {
                    complete = false;
                    break;
                }
            }
            if complete && index.finish_manifest_ingest().unwrap() {
                break;
            }
            index.restart_manifest_ingest_after_spill_full().unwrap();
        }
        assert_eq!(memory.snapshot().current_bytes, retained_bytes);

        index
            .manifest_reader(ManifestContentType::Data)
            .unwrap()
            .consume(|count, manifests| {
                assert_eq!(count, MANIFEST_COUNT);
                for (ordinal, manifest) in manifests.enumerate() {
                    assert_eq!(
                        manifest?.manifest_path,
                        format!("manifest-{ordinal:05}.avro")
                    );
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(memory.snapshot().current_bytes, retained_bytes);
        assert!(spill.snapshot().current_bytes > 0);

        drop(index);
        assert_eq!(memory.snapshot().current_bytes, 0);
        assert_eq!(spill.snapshot().current_bytes, 0);
    }
}
