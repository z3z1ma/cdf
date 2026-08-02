//! Spill-backed canonical ordering and SQLite index traversal.

use std::io::Write;
use std::sync::Arc;

use cdf_kernel::{CdfError, Result};
use cdf_memory::{
    ConsumerKey, MemoryClass, MemoryCoordinator, MemoryLease, ReservationRequest, reserve_blocking,
};
use cdf_runtime::{RunCancellation, SpillBudgetCoordinator, SpillReservation};
use rusqlite::{Connection, OptionalExtension, params, types::ValueRef};

use crate::encoded::{BoundedVec, ExternalTaskSetArtifact};
use crate::limits::{CanonicalTaskSetLimits, require_token, task_writer_memory_requirements};
use crate::sqlite_capacity::{
    SQLITE_BALANCE_NEW_PAGES_PER_LEVEL, SQLITE_BTREE_MAX_DEPTH, SQLITE_INSERT_GUARD_PAGES,
    SQLITE_PAGE_BYTES, configure_canonical_index, is_sqlite_constraint, is_sqlite_full,
    set_page_ceiling, sqlite_error, sqlite_page_count, sqlite_single_leaf_fits,
};
use crate::store::{ExternalTaskStore, ExternalTaskWorkspace, available_spill_bytes};

impl ExternalTaskStore {
    /// Creates a bounded, spill-backed canonical builder for provider-order task records.
    ///
    /// SQLite is an invocation-local sorting implementation, never serialized authority. Its page
    /// ceiling is raised only after the shared spill coordinator admits another configured growth
    /// quantum. Finalization streams the ordered payloads into the ordinary task-set writer.
    pub fn canonical_builder(
        &self,
        task_type: &str,
        limits: CanonicalTaskSetLimits,
        memory: Arc<dyn MemoryCoordinator>,
        spill: Arc<dyn SpillBudgetCoordinator>,
    ) -> Result<CanonicalTaskSetBuilder> {
        require_token("task-set type", task_type)?;
        limits.validate()?;
        let scratch_memory = limits
            .index_cache_bytes
            .checked_add(limits.tasks.maximum_task_bytes.saturating_mul(2))
            .and_then(|bytes| bytes.checked_add(limits.maximum_sort_key_bytes.saturating_mul(2)))
            .ok_or_else(|| CdfError::contract("canonical task-set memory budget overflowed"))?;
        let (maximum_payload_bytes, writer_memory_bytes) =
            task_writer_memory_requirements(task_type, &limits.tasks)?;
        let combined_memory = scratch_memory
            .checked_add(writer_memory_bytes)
            .ok_or_else(|| CdfError::contract("canonical task-set combined memory overflowed"))?;
        let combined_lease = reserve_blocking(
            Arc::clone(&memory),
            &ReservationRequest::new(
                ConsumerKey::new("canonical-task-set-index", MemoryClass::Control)?,
                combined_memory,
            )?,
        )?;
        let mut memory_leases =
            combined_lease.into_partitions(vec![scratch_memory, writer_memory_bytes])?;
        let memory_lease = memory_leases.remove(0);
        let writer_memory_lease = memory_leases.remove(0);
        let available = available_spill_bytes(spill.as_ref());
        let initial = limits.spill_growth_bytes.min(available).max(1);
        if initial < limits.minimum_initial_spill_bytes {
            return Err(CdfError::data(format!(
                "canonical task planning requires at least {} free spill bytes but only {available} are available; raise the run spill budget or reduce concurrent planning",
                limits.minimum_initial_spill_bytes
            )));
        }
        let spill_reservation = spill.try_reserve(initial)?.ok_or_else(|| {
            CdfError::data(
                "canonical task planning could not acquire its initial shared spill reservation",
            )
        })?;
        let workspace = self.temporary_workspace("canonical-task-index")?;
        let database_path = workspace.path().join("tasks.sqlite");
        let connection = Connection::open(&database_path)
            .map_err(|error| sqlite_error("open canonical task index", error))?;
        configure_canonical_index(&connection, limits.index_cache_bytes)?;
        set_page_ceiling(&connection, spill_reservation.bytes())?;
        connection
            .execute_batch(
                "CREATE TABLE tasks (
                    sort_key BLOB PRIMARY KEY,
                    payload BLOB NOT NULL
                ) WITHOUT ROWID;",
            )
            .map_err(|error| sqlite_error("create canonical task index", error))?;
        Ok(CanonicalTaskSetBuilder {
            store: self.clone(),
            task_type: task_type.to_owned(),
            limits,
            spill,
            spill_reservation,
            connection,
            payload: Vec::new(),
            task_count: 0,
            poisoned: false,
            _memory_lease: memory_lease,
            writer_memory_lease: Some(writer_memory_lease),
            writer_maximum_payload_bytes: maximum_payload_bytes,
            _workspace: workspace,
        })
    }
}

/// Spill-backed provider-order input for one canonical task-set artifact.
pub struct CanonicalTaskSetBuilder {
    store: ExternalTaskStore,
    task_type: String,
    limits: CanonicalTaskSetLimits,
    spill: Arc<dyn SpillBudgetCoordinator>,
    spill_reservation: SpillReservation,
    connection: Connection,
    payload: Vec<u8>,
    task_count: u64,
    poisoned: bool,
    _memory_lease: cdf_memory::MemoryLease,
    writer_memory_lease: Option<MemoryLease>,
    writer_maximum_payload_bytes: usize,
    _workspace: ExternalTaskWorkspace,
}

impl CanonicalTaskSetBuilder {
    /// Inserts one record under its complete canonical ordering key.
    ///
    /// Duplicate keys fail instead of silently choosing one provider observation. Encoding occurs
    /// inside the pre-admitted maximum record working set.
    pub fn push_with(
        &mut self,
        sort_key: &[u8],
        encode: impl FnOnce(&mut dyn Write) -> Result<()>,
    ) -> Result<()> {
        self.push_encoded(sort_key, encode, false).map(|_| ())
    }

    /// Inserts one record or collapses an exact duplicate provider observation.
    ///
    /// The same ordering key with different canonical bytes remains an error. This is useful for
    /// listing providers and recursive globs that can legitimately report the same object through
    /// multiple traversal paths without allowing contradictory metadata to become schedule input.
    pub fn push_idempotent_with(
        &mut self,
        sort_key: &[u8],
        encode: impl FnOnce(&mut dyn Write) -> Result<()>,
    ) -> Result<bool> {
        self.push_encoded(sort_key, encode, true)
    }

    fn push_encoded(
        &mut self,
        sort_key: &[u8],
        encode: impl FnOnce(&mut dyn Write) -> Result<()>,
        accept_identical_duplicate: bool,
    ) -> Result<bool> {
        if self.poisoned {
            return Err(CdfError::contract(
                "canonical task index cannot continue after an unexpected partial insertion",
            ));
        }
        if sort_key.is_empty()
            || u64::try_from(sort_key.len()).unwrap_or(u64::MAX)
                > self.limits.maximum_sort_key_bytes
        {
            return Err(CdfError::data(format!(
                "canonical task sort key requires {} bytes but its configured maximum is {}",
                sort_key.len(),
                self.limits.maximum_sort_key_bytes
            )));
        }
        self.payload.clear();
        let maximum = usize::try_from(self.limits.tasks.maximum_task_bytes)
            .map_err(|_| CdfError::contract("task-set task budget exceeds usize"))?;
        encode(&mut BoundedVec::new(&mut self.payload, maximum))?;
        if self.payload.is_empty() {
            return Err(CdfError::data("canonical task payload cannot be empty"));
        }
        if accept_identical_duplicate {
            let existing = self
                .connection
                .query_row(
                    "SELECT payload FROM tasks WHERE sort_key = ?1",
                    params![sort_key],
                    |row| match row.get_ref(0)? {
                        ValueRef::Blob(payload) => Ok(Some(payload == self.payload.as_slice())),
                        _ => Ok(None),
                    },
                )
                .optional()
                .map_err(|error| sqlite_error("inspect duplicate canonical task", error))?;
            if let Some(identical) = existing.flatten() {
                if identical {
                    return Ok(false);
                }
                return Err(CdfError::data(
                    "canonical task input repeats one ordering key with conflicting payloads",
                ));
            }
        }
        self.ensure_insert_capacity(sort_key.len(), self.payload.len())?;
        match self.connection.execute(
            "INSERT INTO tasks (sort_key, payload) VALUES (?1, ?2)",
            params![sort_key, self.payload],
        ) {
            Ok(_) => {}
            Err(error) if is_sqlite_constraint(&error) => {
                return Err(CdfError::data(
                    "canonical task input repeats one ordering key with conflicting payloads",
                ));
            }
            Err(error) if is_sqlite_full(&error) => {
                self.poisoned = true;
                let admitted_pages = self.spill_reservation.bytes() / SQLITE_PAGE_BYTES;
                if sqlite_page_count(&self.connection)
                    .is_ok_and(|page_count| page_count >= admitted_pages)
                {
                    return Err(CdfError::internal(format!(
                        "canonical task insertion reached its pre-admitted SQLite page ceiling: {error}"
                    )));
                }
                return Err(sqlite_error("insert canonical task", error));
            }
            Err(error) => {
                self.poisoned = true;
                return Err(sqlite_error("insert canonical task", error));
            }
        }
        self.task_count = self
            .task_count
            .checked_add(1)
            .ok_or_else(|| CdfError::data("canonical task count exceeds u64"))?;
        Ok(true)
    }

    pub fn task_count(&self) -> u64 {
        self.task_count
    }

    #[cfg(test)]
    pub(crate) fn reserved_spill_bytes(&self) -> u64 {
        self.spill_reservation.bytes()
    }

    pub fn finalize(
        self,
        encode_authority: impl FnOnce(&mut dyn Write) -> Result<()>,
    ) -> Result<ExternalTaskSetArtifact> {
        self.finalize_transformed(
            None,
            None,
            false,
            |_, payload, output| {
                output.write_all(payload).map_err(|error| {
                    CdfError::data(format!("write sorted canonical task: {error}"))
                })?;
                Ok(())
            },
            encode_authority,
        )
    }

    pub fn finalize_transformed_with_authority_hash(
        self,
        expected_authority_sha256: &str,
        cancellation: &RunCancellation,
        transform: impl FnMut(u64, &[u8], &mut dyn Write) -> Result<()>,
        encode_authority: impl FnOnce(&mut dyn Write) -> Result<()>,
    ) -> Result<ExternalTaskSetArtifact> {
        cdf_runtime::validate_artifact_hash(
            "expected canonical task-set authority",
            expected_authority_sha256,
        )?;
        cancellation.check()?;
        self.finalize_transformed(
            Some(expected_authority_sha256),
            Some(cancellation),
            true,
            transform,
            encode_authority,
        )
    }

    fn finalize_transformed(
        mut self,
        expected_authority_sha256: Option<&str>,
        cancellation: Option<&RunCancellation>,
        allow_empty: bool,
        mut transform: impl FnMut(u64, &[u8], &mut dyn Write) -> Result<()>,
        encode_authority: impl FnOnce(&mut dyn Write) -> Result<()>,
    ) -> Result<ExternalTaskSetArtifact> {
        if self.poisoned {
            return Err(CdfError::contract(
                "canonical task index cannot finalize after a partial insertion",
            ));
        }
        if self.task_count == 0 && !allow_empty {
            return Err(CdfError::data(
                "canonical task-set cannot finalize an empty provider inventory",
            ));
        }
        let writer_memory_lease = self.writer_memory_lease.take().ok_or_else(|| {
            CdfError::contract("canonical task-set writer memory authority is missing")
        })?;
        let mut writer = self.store.writer_with_memory_lease(
            &self.task_type,
            self.limits.tasks.clone(),
            self.spill.as_ref(),
            self.writer_maximum_payload_bytes,
            writer_memory_lease,
        )?;
        {
            let mut statement = self
                .connection
                .prepare("SELECT payload FROM tasks ORDER BY sort_key")
                .map_err(|error| sqlite_error("prepare canonical task traversal", error))?;
            let mut rows = statement
                .query([])
                .map_err(|error| sqlite_error("query canonical tasks", error))?;
            let mut ordinal = 0_u64;
            while let Some(row) = rows
                .next()
                .map_err(|error| sqlite_error("read canonical task", error))?
            {
                let payload = match row
                    .get_ref(0)
                    .map_err(|error| sqlite_error("read canonical task payload", error))?
                {
                    ValueRef::Blob(payload) => payload,
                    _ => {
                        return Err(CdfError::internal(
                            "canonical task index returned a non-blob payload",
                        ));
                    }
                };
                writer.push_with(ordinal, |output| transform(ordinal, payload, output))?;
                ordinal = ordinal
                    .checked_add(1)
                    .ok_or_else(|| CdfError::data("canonical task ordinal exceeds u64"))?;
            }
            if ordinal != self.task_count {
                return Err(CdfError::internal(format!(
                    "canonical task traversal produced {ordinal} records after accepting {}",
                    self.task_count
                )));
            }
        }
        if let Some(cancellation) = cancellation {
            cancellation.check()?;
        }
        match expected_authority_sha256 {
            Some(expected) => writer.finalize_with_authority_hash_and_cancellation(
                expected,
                cancellation.ok_or_else(|| {
                    CdfError::internal(
                        "typed canonical finalization omitted its cancellation authority",
                    )
                })?,
                encode_authority,
            ),
            None => writer.finalize(encode_authority),
        }
    }

    fn grow_spill(&mut self) -> Result<()> {
        let available = available_spill_bytes(self.spill.as_ref());
        let growth = self.limits.spill_growth_bytes.min(available);
        if growth == 0 || !self.spill_reservation.try_grow(growth)? {
            return Err(CdfError::data(
                "canonical task index exhausted the configured spill budget; raise the run spill budget or reduce concurrent planning",
            ));
        }
        set_page_ceiling(&self.connection, self.spill_reservation.bytes())
    }

    fn ensure_insert_capacity(
        &mut self,
        sort_key_bytes: usize,
        payload_bytes: usize,
    ) -> Result<()> {
        let sort_key_bytes = u64::try_from(sort_key_bytes)
            .map_err(|_| CdfError::data("canonical task sort-key size exceeds u64"))?;
        let payload_bytes = u64::try_from(payload_bytes)
            .map_err(|_| CdfError::data("canonical task payload size exceeds u64"))?;
        let record_bytes = sort_key_bytes
            .checked_add(payload_bytes)
            .and_then(|bytes| bytes.checked_add(128))
            .ok_or_else(|| CdfError::data("canonical task index record size overflowed u64"))?;
        if sqlite_single_leaf_fits(&self.connection, "tasks", record_bytes)? {
            return Ok(());
        }
        let record_pages = record_bytes
            .div_ceil(SQLITE_PAGE_BYTES.saturating_sub(4))
            .saturating_add(1);
        let current_pages = sqlite_page_count(&self.connection)?;
        let structural_pages = SQLITE_BTREE_MAX_DEPTH
            .saturating_mul(SQLITE_BALANCE_NEW_PAGES_PER_LEVEL)
            .saturating_add(SQLITE_INSERT_GUARD_PAGES);
        let required_pages = current_pages
            .checked_add(record_pages)
            .and_then(|pages| pages.checked_add(structural_pages))
            .ok_or_else(|| CdfError::data("canonical task index capacity overflowed u64"))?;
        let required_bytes = required_pages
            .checked_mul(SQLITE_PAGE_BYTES)
            .ok_or_else(|| CdfError::data("canonical task index capacity overflowed u64"))?;
        if self.spill_reservation.bytes() >= required_bytes {
            return Ok(());
        }
        while self.spill_reservation.bytes() < required_bytes {
            self.grow_spill()?;
        }
        Ok(())
    }
}
