use std::{io::Write, path::Path, sync::Arc};

use cdf_kernel::{CdfError, Result};
use cdf_memory::{
    ConsumerKey, DeterministicMemoryCoordinator, FixedSpillBudget, MemoryClass, MemoryCoordinator,
    MemoryLease, ReservationRequest, SpillBudgetCoordinator, SpillReservation, reserve_blocking,
};
use cdf_package_contract::{FileEntry, SegmentEntry};
use rusqlite::{Connection, ErrorCode, OptionalExtension, params, types::ValueRef};

use crate::json::canonical_json_bytes;

const SQLITE_PAGE_BYTES: u64 = 4096;
const FILE_RECORD_KIND: i64 = 1;
const SEGMENT_RECORD_KIND: i64 = 2;

/// Explicit resource knobs for the package draft index.
///
/// These limits affect only construction working sets and spill admission. They never enter
/// package identity, so operators can tune them to the host without changing canonical bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackageDraftIndexLimits {
    pub maximum_path_bytes: u64,
    pub maximum_record_bytes: u64,
    pub index_cache_bytes: u64,
    pub spill_growth_bytes: u64,
}

#[derive(Clone)]
pub struct PackageBuilderResources {
    pub limits: PackageDraftIndexLimits,
    pub memory: Arc<dyn MemoryCoordinator>,
    pub spill: Arc<dyn SpillBudgetCoordinator>,
}

impl PackageBuilderResources {
    pub fn new(
        limits: PackageDraftIndexLimits,
        memory: Arc<dyn MemoryCoordinator>,
        spill: Arc<dyn SpillBudgetCoordinator>,
    ) -> Result<Self> {
        limits.validate()?;
        Ok(Self {
            limits,
            memory,
            spill,
        })
    }

    pub fn shared(
        memory: Arc<dyn MemoryCoordinator>,
        spill: Arc<dyn SpillBudgetCoordinator>,
    ) -> Result<Self> {
        let limits = PackageDraftIndexLimits::proportional(
            memory.snapshot().budget_bytes,
            spill.snapshot().budget_bytes,
        )?;
        Self::new(limits, memory, spill)
    }

    /// Creates an isolated authority for package-only tools, fixtures, and embedders that do not
    /// already own a process-wide runtime authority. Full executions must use `shared`.
    pub fn standalone(managed_memory_bytes: u64, spill_budget_bytes: u64) -> Result<Self> {
        let limits =
            PackageDraftIndexLimits::proportional(managed_memory_bytes, spill_budget_bytes)?;
        Self::new(
            limits,
            Arc::new(DeterministicMemoryCoordinator::new(
                managed_memory_bytes,
                Default::default(),
            )?),
            Arc::new(FixedSpillBudget::new(spill_budget_bytes)?),
        )
    }
}

impl PackageDraftIndexLimits {
    /// Derives a host-proportionate default without placing a fixed ceiling on any package
    /// construction resource. Callers with known metadata shapes can supply explicit limits.
    pub fn proportional(managed_memory_bytes: u64, spill_budget_bytes: u64) -> Result<Self> {
        if managed_memory_bytes < 256 * 1024 {
            return Err(CdfError::data(format!(
                "package draft minimum working set {} bytes exceeds managed budget {managed_memory_bytes} bytes; raise the run memory budget or supply smaller explicit package draft limits",
                256 * 1024
            )));
        }
        if spill_budget_bytes < 64 * 1024 {
            return Err(CdfError::data(format!(
                "package draft minimum spill quantum {} bytes exceeds spill budget {spill_budget_bytes} bytes; raise the run spill budget or supply smaller explicit package draft limits",
                64 * 1024
            )));
        }
        let limits = Self {
            maximum_path_bytes: (managed_memory_bytes / 4096).max(4096),
            maximum_record_bytes: (managed_memory_bytes / 2048).max(16 * 1024),
            index_cache_bytes: (managed_memory_bytes / 1024).max(64 * 1024),
            spill_growth_bytes: (spill_budget_bytes / 128).max(64 * 1024),
        };
        limits.validate()?;
        let working_set = limits
            .index_cache_bytes
            .checked_add(limits.maximum_path_bytes.saturating_mul(2))
            .and_then(|bytes| bytes.checked_add(limits.maximum_record_bytes.saturating_mul(2)))
            .ok_or_else(|| CdfError::contract("package draft default working set overflowed"))?;
        if working_set > managed_memory_bytes {
            return Err(CdfError::contract(format!(
                "package draft default working set {working_set} exceeds managed memory budget {managed_memory_bytes}; supply smaller explicit package draft limits"
            )));
        }
        Ok(limits)
    }

    pub fn validate(&self) -> Result<()> {
        if self.maximum_path_bytes == 0
            || self.maximum_record_bytes == 0
            || self.index_cache_bytes == 0
            || self.spill_growth_bytes < SQLITE_PAGE_BYTES * 2
        {
            return Err(CdfError::contract(
                "package draft path, record, index-cache, and spill-growth budgets must be nonzero, and spill growth must cover at least two SQLite pages",
            ));
        }
        usize::try_from(self.maximum_path_bytes).map_err(|_| {
            CdfError::contract("package draft maximum path bytes exceeds addressable memory")
        })?;
        usize::try_from(self.maximum_record_bytes).map_err(|_| {
            CdfError::contract("package draft maximum record bytes exceeds addressable memory")
        })?;
        Ok(())
    }
}

/// Spill-backed, canonically ordered package-construction metadata.
///
/// SQLite is an invocation-local index, never serialized package authority. Its cache and
/// per-record scratch are admitted by the shared memory coordinator, and its page ceiling grows
/// only after the shared spill coordinator admits another caller-selected quantum.
pub(crate) struct PackageDraftIndex {
    limits: PackageDraftIndexLimits,
    spill: Arc<dyn SpillBudgetCoordinator>,
    spill_reservation: SpillReservation,
    connection: Connection,
    file_count: u64,
    segment_count: u64,
    _memory_lease: MemoryLease,
    _workspace: tempfile::TempDir,
}

impl PackageDraftIndex {
    pub(crate) fn create(
        package_dir: &Path,
        limits: PackageDraftIndexLimits,
        memory: Arc<dyn MemoryCoordinator>,
        spill: Arc<dyn SpillBudgetCoordinator>,
    ) -> Result<Self> {
        limits.validate()?;
        let scratch_memory = limits
            .index_cache_bytes
            .checked_add(limits.maximum_path_bytes.saturating_mul(2))
            .and_then(|bytes| bytes.checked_add(limits.maximum_record_bytes.saturating_mul(2)))
            .ok_or_else(|| CdfError::contract("package draft index memory budget overflowed"))?;
        let memory_lease = reserve_blocking(
            memory,
            &ReservationRequest::new(
                ConsumerKey::new("package-draft-index", MemoryClass::Control)?,
                scratch_memory,
            )?,
        )?;
        let available = available_spill_bytes(spill.as_ref());
        let initial = limits.spill_growth_bytes.min(available);
        if initial < SQLITE_PAGE_BYTES * 2 {
            return Err(CdfError::data(format!(
                "package finalization requires at least {} free spill bytes but only {available} are available; raise the run spill budget or reduce concurrent package construction",
                SQLITE_PAGE_BYTES * 2
            )));
        }
        let spill_reservation = spill.try_reserve(initial)?.ok_or_else(|| {
            CdfError::data("package finalization could not acquire its initial spill reservation")
        })?;
        let workspace = tempfile::Builder::new()
            .prefix(".package-draft-")
            .tempdir_in(package_dir)
            .map_err(|error| CdfError::data(format!("create package draft workspace: {error}")))?;
        let connection = Connection::open(workspace.path().join("index.sqlite"))
            .map_err(|error| sqlite_error("open package draft index", error))?;
        configure_index(&connection, limits.index_cache_bytes)?;
        set_page_ceiling(&connection, spill_reservation.bytes())?;
        connection
            .execute_batch(
                "CREATE TABLE records (
                    kind INTEGER NOT NULL,
                    sort_key BLOB NOT NULL,
                    identity_key BLOB NOT NULL,
                    payload BLOB NOT NULL,
                    PRIMARY KEY (kind, sort_key),
                    UNIQUE (kind, identity_key)
                ) WITHOUT ROWID;",
            )
            .map_err(|error| sqlite_error("create package draft index", error))?;
        Ok(Self {
            limits,
            spill,
            spill_reservation,
            connection,
            file_count: 0,
            segment_count: 0,
            _memory_lease: memory_lease,
            _workspace: workspace,
        })
    }

    pub(crate) fn insert_file(&mut self, entry: &FileEntry) -> Result<()> {
        let key = portable_path_key(&entry.path, self.limits.maximum_path_bytes)?;
        let payload = bounded_record(entry, self.limits.maximum_record_bytes, "file entry")?;
        self.insert(
            FILE_RECORD_KIND,
            &key,
            entry.path.as_bytes(),
            &payload,
            &format!("identity artifact path `{}`", entry.path),
        )?;
        self.file_count = self
            .file_count
            .checked_add(1)
            .ok_or_else(|| CdfError::data("package identity file count exceeds u64"))?;
        Ok(())
    }

    pub(crate) fn insert_segment(&mut self, entry: &SegmentEntry) -> Result<()> {
        let key = segment_order_key(entry);
        let payload = bounded_record(entry, self.limits.maximum_record_bytes, "segment entry")?;
        self.insert(
            SEGMENT_RECORD_KIND,
            &key,
            entry.segment_id.as_str().as_bytes(),
            &payload,
            &format!(
                "segment id `{}` or package-row range starting at {}",
                entry.segment_id, entry.package_row_ord_start
            ),
        )?;
        self.segment_count = self
            .segment_count
            .checked_add(1)
            .ok_or_else(|| CdfError::data("package segment count exceeds u64"))?;
        Ok(())
    }

    pub(crate) fn file(&self, path: &str) -> Result<Option<FileEntry>> {
        let key = portable_path_key(path, self.limits.maximum_path_bytes)?;
        self.connection
            .query_row(
                "SELECT payload FROM records WHERE kind = ?1 AND sort_key = ?2",
                params![FILE_RECORD_KIND, key],
                |row| decode_row(row.get_ref(0)?, "file entry"),
            )
            .optional()
            .map_err(|error| sqlite_error("lookup package artifact receipt", error))
    }

    pub(crate) const fn file_count(&self) -> u64 {
        self.file_count
    }

    pub(crate) const fn segment_count(&self) -> u64 {
        self.segment_count
    }

    pub(crate) fn visit_files(
        &self,
        visitor: &mut dyn FnMut(FileEntry) -> Result<()>,
    ) -> Result<()> {
        visit_kind(
            &self.connection,
            FILE_RECORD_KIND,
            self.file_count,
            "file entry",
            visitor,
        )
    }

    pub(crate) fn visit_segments(
        &self,
        visitor: &mut dyn FnMut(SegmentEntry) -> Result<()>,
    ) -> Result<()> {
        visit_kind(
            &self.connection,
            SEGMENT_RECORD_KIND,
            self.segment_count,
            "segment entry",
            visitor,
        )
    }

    fn insert(
        &mut self,
        kind: i64,
        key: &[u8],
        identity_key: &[u8],
        payload: &[u8],
        label: &str,
    ) -> Result<()> {
        loop {
            match self.connection.execute(
                "INSERT INTO records (kind, sort_key, identity_key, payload) VALUES (?1, ?2, ?3, ?4)",
                params![kind, key, identity_key, payload],
            ) {
                Ok(_) => return Ok(()),
                Err(error) if is_sqlite_full(&error) => self.grow_spill()?,
                Err(error) if is_sqlite_constraint(&error) => {
                    return Err(CdfError::data(format!("package draft repeats one {label}")));
                }
                Err(error) => return Err(sqlite_error("insert package draft record", error)),
            }
        }
    }

    fn grow_spill(&mut self) -> Result<()> {
        let available = available_spill_bytes(self.spill.as_ref());
        let growth = self.limits.spill_growth_bytes.min(available);
        if growth == 0 || !self.spill_reservation.try_grow(growth)? {
            return Err(CdfError::data(
                "package draft index exhausted the configured shared spill budget; raise the spill budget or reduce concurrent package construction",
            ));
        }
        set_page_ceiling(&self.connection, self.spill_reservation.bytes())
    }
}

fn visit_kind<T: serde::de::DeserializeOwned>(
    connection: &Connection,
    kind: i64,
    expected_count: u64,
    label: &str,
    visitor: &mut dyn FnMut(T) -> Result<()>,
) -> Result<()> {
    let mut statement = connection
        .prepare("SELECT payload FROM records WHERE kind = ?1 ORDER BY sort_key")
        .map_err(|error| sqlite_error("prepare package draft traversal", error))?;
    let mut rows = statement
        .query(params![kind])
        .map_err(|error| sqlite_error("query package draft records", error))?;
    let mut observed = 0_u64;
    while let Some(row) = rows
        .next()
        .map_err(|error| sqlite_error("read package draft record", error))?
    {
        visitor(
            decode_row(
                row.get_ref(0)
                    .map_err(|error| sqlite_error("read package draft payload", error))?,
                label,
            )
            .map_err(|error| sqlite_error("decode package draft payload", error))?,
        )?;
        observed = observed
            .checked_add(1)
            .ok_or_else(|| CdfError::data("package draft traversal count exceeds u64"))?;
    }
    if observed != expected_count {
        return Err(CdfError::internal(format!(
            "package draft {label} count changed during traversal: expected {expected_count}, observed {observed}"
        )));
    }
    Ok(())
}

fn decode_row<T: serde::de::DeserializeOwned>(
    value: ValueRef<'_>,
    label: &str,
) -> rusqlite::Result<T> {
    let ValueRef::Blob(payload) = value else {
        return Err(rusqlite::Error::InvalidColumnType(
            0,
            label.to_owned(),
            value.data_type(),
        ));
    };
    serde_json::from_slice(payload).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            payload.len(),
            rusqlite::types::Type::Blob,
            Box::new(error),
        )
    })
}

fn bounded_record<T: serde::Serialize>(value: &T, maximum: u64, label: &str) -> Result<Vec<u8>> {
    let bytes = canonical_json_bytes(value)?;
    let observed = u64::try_from(bytes.len())
        .map_err(|_| CdfError::data(format!("package draft {label} exceeds u64")))?;
    if observed > maximum {
        return Err(CdfError::data(format!(
            "package draft {label} requires {observed} bytes but its configured maximum is {maximum}"
        )));
    }
    Ok(bytes)
}

fn portable_path_key(path: &str, maximum: u64) -> Result<Vec<u8>> {
    let path_bytes = u64::try_from(path.len())
        .map_err(|_| CdfError::data("package artifact path length exceeds u64"))?;
    if path_bytes > maximum {
        return Err(CdfError::data(format!(
            "package artifact path requires {path_bytes} bytes but its configured maximum is {maximum}"
        )));
    }
    let folded = path
        .chars()
        .flat_map(char::to_lowercase)
        .collect::<String>();
    let mut key = Vec::with_capacity(folded.len().saturating_add(path.len()).saturating_add(1));
    key.write_all(folded.as_bytes())
        .map_err(|error| CdfError::internal(format!("write package path key: {error}")))?;
    key.push(0);
    key.write_all(path.as_bytes())
        .map_err(|error| CdfError::internal(format!("write package path key: {error}")))?;
    Ok(key)
}

fn segment_order_key(entry: &SegmentEntry) -> Vec<u8> {
    entry.package_row_ord_start.to_be_bytes().to_vec()
}

fn configure_index(connection: &Connection, cache_bytes: u64) -> Result<()> {
    connection
        .pragma_update(
            None,
            "page_size",
            i64::try_from(SQLITE_PAGE_BYTES).expect("SQLite page size fits i64"),
        )
        .and_then(|_| connection.pragma_update(None, "journal_mode", "OFF"))
        .and_then(|_| connection.pragma_update(None, "synchronous", "OFF"))
        .and_then(|_| connection.pragma_update(None, "locking_mode", "EXCLUSIVE"))
        .and_then(|_| connection.pragma_update(None, "temp_store", "FILE"))
        .and_then(|_| connection.pragma_update(None, "mmap_size", 0_i64))
        .and_then(|_| connection.pragma_update(None, "cache_spill", true))
        .map_err(|error| sqlite_error("configure package draft index", error))?;
    let cache_kib = cache_bytes.div_ceil(1024).max(1);
    connection
        .pragma_update(
            None,
            "cache_size",
            -i64::try_from(cache_kib).unwrap_or(i64::MAX),
        )
        .map_err(|error| sqlite_error("configure package draft index cache", error))
}

fn set_page_ceiling(connection: &Connection, reserved_bytes: u64) -> Result<()> {
    let pages = reserved_bytes / SQLITE_PAGE_BYTES;
    if pages < 2 {
        return Err(CdfError::data(
            "package draft spill reservation cannot hold two SQLite pages",
        ));
    }
    connection
        .pragma_update(
            None,
            "max_page_count",
            i64::try_from(pages).unwrap_or(i64::MAX),
        )
        .map_err(|error| sqlite_error("raise package draft page ceiling", error))
}

fn available_spill_bytes(spill: &dyn SpillBudgetCoordinator) -> u64 {
    let snapshot = spill.snapshot();
    snapshot.budget_bytes.saturating_sub(snapshot.current_bytes)
}

fn is_sqlite_full(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(failure, _)
            if failure.code == ErrorCode::DiskFull
    )
}

fn is_sqlite_constraint(error: &rusqlite::Error) -> bool {
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
    use std::collections::BTreeMap;

    use cdf_kernel::SegmentId;
    use cdf_memory::{DeterministicMemoryCoordinator, FixedSpillBudget};

    use super::*;

    fn index(root: &Path) -> PackageDraftIndex {
        PackageDraftIndex::create(
            root,
            PackageDraftIndexLimits {
                maximum_path_bytes: 1024,
                maximum_record_bytes: 4096,
                index_cache_bytes: 16 * 1024,
                spill_growth_bytes: 16 * 1024,
            },
            Arc::new(DeterministicMemoryCoordinator::new(64 * 1024, BTreeMap::new()).unwrap()),
            Arc::new(FixedSpillBudget::new(1024 * 1024).unwrap()),
        )
        .unwrap()
    }

    #[test]
    fn draft_index_streams_portable_path_and_ordinal_order() {
        let root = tempfile::tempdir().unwrap();
        let mut index = index(root.path());
        for path in ["data/z.arrow", "data/B.arrow", "data/a.arrow"] {
            index
                .insert_file(&FileEntry {
                    path: path.to_owned(),
                    byte_count: 1,
                    sha256: "00".repeat(32),
                })
                .unwrap();
        }
        for ordinal in [8, 0, 4] {
            index
                .insert_segment(&SegmentEntry {
                    segment_id: SegmentId::new(format!("segment-{ordinal}")).unwrap(),
                    path: format!("data/segment-{ordinal}.arrow"),
                    package_row_ord_start: ordinal,
                    row_count: 4,
                    byte_count: 1,
                    sha256: "00".repeat(32),
                })
                .unwrap();
        }

        let mut files = Vec::new();
        index
            .visit_files(&mut |entry| {
                files.push(entry.path);
                Ok(())
            })
            .unwrap();
        assert_eq!(files, ["data/a.arrow", "data/B.arrow", "data/z.arrow"]);
        let mut ordinals = Vec::new();
        index
            .visit_segments(&mut |entry| {
                ordinals.push(entry.package_row_ord_start);
                Ok(())
            })
            .unwrap();
        assert_eq!(ordinals, [0, 4, 8]);
    }

    #[test]
    fn draft_index_rejects_duplicate_identity_authority() {
        let root = tempfile::tempdir().unwrap();
        let mut index = index(root.path());
        let entry = FileEntry {
            path: "data/segment.arrow".to_owned(),
            byte_count: 1,
            sha256: "00".repeat(32),
        };
        index.insert_file(&entry).unwrap();
        assert!(
            index
                .insert_file(&entry)
                .unwrap_err()
                .message
                .contains("repeats one identity artifact path `data/segment.arrow`")
        );
    }
}
