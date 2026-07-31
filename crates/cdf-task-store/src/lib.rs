#![doc = "Bounded content-addressed task-set artifacts for cdf planners."]
#![cfg_attr(
    not(test),
    deny(
        clippy::expect_used,
        clippy::unwrap_used,
        reason = "foundational production code must propagate recoverable failures"
    )
)]

use std::fs::{self, File};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use cdf_kernel::{
    CdfError, ContentObjectKey, ContentProviderGeneration, ContentStoreNamespace,
    PLANNED_TASK_SET_REFERENCE_VERSION, PlannedTaskSetReference, Result,
};
use cdf_memory::{
    AccountedBytes, ConsumerKey, MemoryClass, MemoryCoordinator, MemoryLease, ReservationRequest,
    reserve_blocking,
};
use cdf_runtime::{RunCancellation, SpillBudgetCoordinator, SpillReservation};
use rusqlite::{Connection, ErrorCode, OptionalExtension, params, types::ValueRef};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;

const MAGIC: &[u8; 8] = b"CDFTASK1";
const FORMAT_VERSION: u16 = 2;
const TASK_TAG: u8 = 1;
const AUTHORITY_TAG: u8 = 2;
const FOOTER_TAG: u8 = u8::MAX;
const FOOTER_BYTES: u64 = 1 + 8 + 8;
const SQLITE_PAGE_BYTES: u64 = 4096;
const SQLITE_INDEX_MAX_LOCAL_PAYLOAD_BYTES: u64 = 1002;
// These mirror bundled SQLite: BTCURSOR_MAX_DEPTH is 20 and balancing allocates at most two net
// pages per level. Eight pages cover the root, schema, and conservative record-header slack.
const SQLITE_BTREE_MAX_DEPTH: u64 = 20;
const SQLITE_BALANCE_NEW_PAGES_PER_LEVEL: u64 = 2;
const SQLITE_INSERT_GUARD_PAGES: u64 = 8;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskSetLimits {
    pub maximum_task_bytes: u64,
    pub maximum_authority_bytes: u64,
    pub writer_buffer_bytes: usize,
}

/// Resource policy for a source-owned spill-backed planning index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalTaskWorkspaceLimits {
    consumer: String,
    memory_class: MemoryClass,
    resident_bytes: u64,
    spill_growth_bytes: u64,
    minimum_spill_bytes: u64,
}

impl ExternalTaskWorkspaceLimits {
    pub fn new(
        consumer: impl Into<String>,
        memory_class: MemoryClass,
        resident_bytes: u64,
        spill_growth_bytes: u64,
        minimum_spill_bytes: u64,
    ) -> Result<Self> {
        let consumer = consumer.into();
        require_token("external task workspace consumer", &consumer)?;
        if resident_bytes == 0
            || spill_growth_bytes == 0
            || minimum_spill_bytes == 0
            || minimum_spill_bytes > spill_growth_bytes
        {
            return Err(CdfError::contract(
                "external task workspace memory, spill growth, and minimum spill budgets must be nonzero, and the minimum cannot exceed one growth quantum",
            ));
        }
        Ok(Self {
            consumer,
            memory_class,
            resident_bytes,
            spill_growth_bytes,
            minimum_spill_bytes,
        })
    }
}

/// Resource authority for accepting task records in arbitrary provider order before emitting the
/// canonical task-set artifact.
///
/// Every value is a knob because provider metadata shapes, host memory, and spill devices vary.
/// The builder's output identity is independent of these values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CanonicalTaskSetLimits {
    pub tasks: TaskSetLimits,
    pub maximum_sort_key_bytes: u64,
    pub index_cache_bytes: u64,
    pub spill_growth_bytes: u64,
    pub minimum_initial_spill_bytes: u64,
}

impl CanonicalTaskSetLimits {
    pub fn validate(&self) -> Result<()> {
        self.tasks.validate()?;
        if self.maximum_sort_key_bytes == 0
            || self.index_cache_bytes == 0
            || self.spill_growth_bytes < SQLITE_PAGE_BYTES * 2
            || self.minimum_initial_spill_bytes < SQLITE_PAGE_BYTES * 2
            || self.minimum_initial_spill_bytes > self.spill_growth_bytes
        {
            return Err(CdfError::contract(
                "canonical task-set sort-key, index-cache, and spill-growth budgets must be nonzero, and initial/growth spill budgets must cover at least two SQLite pages with the initial minimum no larger than one growth quantum",
            ));
        }
        usize::try_from(self.maximum_sort_key_bytes).map_err(|_| {
            CdfError::contract(
                "canonical task-set maximum sort-key bytes exceeds addressable memory",
            )
        })?;
        Ok(())
    }
}

impl TaskSetLimits {
    pub fn validate(&self) -> Result<()> {
        if self.maximum_task_bytes == 0
            || self.maximum_authority_bytes == 0
            || self.writer_buffer_bytes == 0
        {
            return Err(CdfError::contract(
                "task-set record, shared-authority, and writer-buffer budgets must be nonzero",
            ));
        }
        usize::try_from(self.maximum_task_bytes).map_err(|_| {
            CdfError::contract("task-set maximum task bytes exceeds addressable memory")
        })?;
        usize::try_from(self.maximum_authority_bytes).map_err(|_| {
            CdfError::contract("task-set maximum authority bytes exceeds addressable memory")
        })?;
        Ok(())
    }
}

fn task_writer_memory_requirements(
    task_type: &str,
    limits: &TaskSetLimits,
) -> Result<(usize, u64)> {
    let maximum_payload_bytes = usize::try_from(
        limits
            .maximum_task_bytes
            .max(limits.maximum_authority_bytes),
    )
    .map_err(|_| CdfError::contract("task-set payload budget exceeds usize"))?;
    let task_type_bytes = u64::try_from(task_type.len())
        .map_err(|_| CdfError::contract("task-set type length exceeds u64"))?;
    let reserved_memory = u64::try_from(maximum_payload_bytes)
        .map_err(|_| CdfError::contract("task-set payload budget exceeds u64"))?
        .checked_add(
            u64::try_from(limits.writer_buffer_bytes)
                .map_err(|_| CdfError::contract("task-set writer-buffer budget exceeds u64"))?,
        )
        .and_then(|bytes| bytes.checked_add(task_type_bytes))
        .ok_or_else(|| CdfError::contract("task-set memory budget overflowed u64"))?;
    Ok((maximum_payload_bytes, reserved_memory))
}

/// A local content-addressed store for canonical planned task sets.
///
/// The root is an injected planning-artifact store, not a coordinator path embedded in task
/// bytes. Temporary construction is spill-accounted. Once atomically installed, persistent
/// retention belongs to the content store and its ordinary reachability/GC authority.
#[derive(Clone, Debug)]
pub struct ExternalTaskStore {
    root: PathBuf,
    namespace: ContentStoreNamespace,
}

impl ExternalTaskStore {
    pub fn new(root: impl Into<PathBuf>, namespace: ContentStoreNamespace) -> Result<Self> {
        let root = root.into();
        if root.as_os_str().is_empty() {
            return Err(CdfError::contract("task-store root cannot be empty"));
        }
        validate_relative_component(namespace.as_str(), "task-store namespace")?;
        Ok(Self { root, namespace })
    }

    pub fn writer(
        &self,
        task_type: &str,
        limits: TaskSetLimits,
        memory: Arc<dyn MemoryCoordinator>,
        spill: &dyn SpillBudgetCoordinator,
    ) -> Result<ExternalTaskSetWriter> {
        require_token("task-set type", task_type)?;
        limits.validate()?;
        let (maximum_payload_bytes, reserved_memory) =
            task_writer_memory_requirements(task_type, &limits)?;
        let memory_lease = reserve_blocking(
            memory,
            &ReservationRequest::new(
                ConsumerKey::new("external-task-set-writer", MemoryClass::Control)?,
                reserved_memory,
            )?,
        )?;
        self.writer_with_memory_lease(
            task_type,
            limits,
            spill,
            maximum_payload_bytes,
            memory_lease,
        )
    }

    fn writer_with_memory_lease(
        &self,
        task_type: &str,
        limits: TaskSetLimits,
        spill: &dyn SpillBudgetCoordinator,
        maximum_payload_bytes: usize,
        memory_lease: MemoryLease,
    ) -> Result<ExternalTaskSetWriter> {
        let (_, required_memory) = task_writer_memory_requirements(task_type, &limits)?;
        if memory_lease.bytes() != required_memory {
            return Err(CdfError::contract(format!(
                "task-set writer lease owns {} bytes but its working set requires {required_memory}",
                memory_lease.bytes()
            )));
        }
        let directory = self.root.join(self.namespace.as_str()).join("task-sets");
        fs::create_dir_all(&directory)
            .map_err(|error| io_error("create task-set directory", &directory, error))?;
        let mut spill_reservation = spill.try_reserve(1)?.ok_or_else(|| {
            CdfError::data(
                "task-set planning requires spill space but the configured disk budget is exhausted",
            )
        })?;
        spill_reservation.shrink(1);

        let temporary = NamedTempFile::new_in(&directory)
            .map_err(|error| io_error("create task-set temporary file", &directory, error))?;
        let file = temporary
            .as_file()
            .try_clone()
            .map_err(|error| io_error("clone task-set temporary file", temporary.path(), error))?;
        let hashing = HashingWriter::new(file);
        let writer = BufWriter::with_capacity(limits.writer_buffer_bytes, hashing);
        let mut task_writer = ExternalTaskSetWriter {
            store: self.clone(),
            task_type: task_type.to_owned(),
            limits,
            temporary: Some(temporary),
            writer: Some(writer),
            payload: Vec::with_capacity(maximum_payload_bytes),
            next_ordinal: 0,
            spill_reservation: Some(spill_reservation),
            _memory_lease: memory_lease,
            poisoned: false,
        };
        task_writer.write_reserved(MAGIC)?;
        task_writer.write_reserved(&FORMAT_VERSION.to_be_bytes())?;
        let task_type_length = u16::try_from(task_type.len())
            .map_err(|_| CdfError::contract("task-set type is too long"))?;
        task_writer.write_reserved(&task_type_length.to_be_bytes())?;
        task_writer.write_reserved(task_type.as_bytes())?;
        Ok(task_writer)
    }

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

    pub fn reader(
        &self,
        reference: PlannedTaskSetReference,
        expected_task_type: &str,
        maximum_task_bytes: u64,
        maximum_authority_bytes: u64,
        memory: Arc<dyn MemoryCoordinator>,
    ) -> Result<ExternalTaskSetReader> {
        reference.validate()?;
        if reference.store_namespace != self.namespace {
            return Err(CdfError::contract(
                "task-set artifact namespace does not match the selected store",
            ));
        }
        require_token("task-set type", expected_task_type)?;
        if reference.task_type != expected_task_type {
            return Err(CdfError::contract(
                "task-set reference type does not match the expected task decoder",
            ));
        }
        if maximum_task_bytes == 0 || maximum_authority_bytes == 0 {
            return Err(CdfError::contract(
                "task-set reader task and shared-authority budgets must be nonzero",
            ));
        }
        let path = self.path_for_reference(&reference)?;
        let file = File::open(&path)
            .map_err(|error| artifact_io_error("open task-set artifact", &path, error))?;
        let mut cursor = ExternalTaskSetReadCursor {
            file,
            path,
            hasher: Sha256::new(),
            observed_bytes: 0,
        };
        let magic = cursor.read_array::<8>()?;
        if &magic != MAGIC {
            return Err(CdfError::data(
                "task-set artifact has invalid framing magic",
            ));
        }
        let version = u16::from_be_bytes(cursor.read_array::<2>()?);
        if version != FORMAT_VERSION {
            return Err(CdfError::contract(format!(
                "task-set format version {version} is unsupported; expected {FORMAT_VERSION}"
            )));
        }
        let task_type_length = usize::from(u16::from_be_bytes(cursor.read_array::<2>()?));
        let task_type_request = ReservationRequest::new(
            ConsumerKey::new("external-task-set-header", MemoryClass::Control)?,
            u64::try_from(task_type_length)
                .map_err(|_| CdfError::data("task-set type length exceeds u64"))?,
        )?;
        let task_type_lease = reserve_blocking(Arc::clone(&memory), &task_type_request)?;
        let task_type = cursor.read_vec(task_type_length)?;
        if task_type != expected_task_type.as_bytes() {
            return Err(CdfError::contract(format!(
                "task-set type does not match expected `{expected_task_type}`"
            )));
        }
        drop(task_type_lease);

        let task_start = cursor.observed_bytes;
        let footer_offset = reference
            .byte_count
            .checked_sub(FOOTER_BYTES)
            .ok_or_else(|| CdfError::data("task-set artifact is shorter than its footer"))?;
        let mut tail = File::open(&cursor.path)
            .map_err(|error| artifact_io_error("open task-set trailer", &cursor.path, error))?;
        tail.seek(SeekFrom::Start(footer_offset))
            .map_err(|error| artifact_io_error("seek task-set footer", &cursor.path, error))?;
        let mut footer = [0_u8; FOOTER_BYTES as usize];
        tail.read_exact(&mut footer)
            .map_err(|error| artifact_io_error("read task-set footer", &cursor.path, error))?;
        if footer[0] != FOOTER_TAG {
            return Err(CdfError::data("task-set artifact has invalid footer tag"));
        }
        let footer_task_count = u64::from_be_bytes(
            footer[1..9]
                .try_into()
                .map_err(|_| CdfError::internal("task-set footer count slice is invalid"))?,
        );
        if footer_task_count != reference.task_count {
            return Err(CdfError::data(format!(
                "task-set footer count {footer_task_count} does not match referenced count {}",
                reference.task_count
            )));
        }
        let authority_offset = u64::from_be_bytes(
            footer[9..17]
                .try_into()
                .map_err(|_| CdfError::internal("task-set authority offset slice is invalid"))?,
        );
        if authority_offset < task_start || authority_offset >= footer_offset {
            return Err(CdfError::data(
                "task-set authority offset is outside the canonical task body",
            ));
        }
        tail.seek(SeekFrom::Start(authority_offset))
            .map_err(|error| artifact_io_error("seek task-set authority", &cursor.path, error))?;
        let mut authority_tag = [0_u8; 1];
        tail.read_exact(&mut authority_tag).map_err(|error| {
            artifact_io_error("read task-set authority tag", &cursor.path, error)
        })?;
        if authority_tag[0] != AUTHORITY_TAG {
            return Err(CdfError::data(
                "task-set artifact has invalid authority tag",
            ));
        }
        let mut authority_length_bytes = [0_u8; 8];
        tail.read_exact(&mut authority_length_bytes)
            .map_err(|error| {
                artifact_io_error("read task-set authority length", &cursor.path, error)
            })?;
        let authority_length = u64::from_be_bytes(authority_length_bytes);
        if authority_length == 0 || authority_length > maximum_authority_bytes {
            return Err(CdfError::data(format!(
                "task-set authority length {authority_length} exceeds the configured budget {maximum_authority_bytes}"
            )));
        }
        let expected_authority_end = authority_offset
            .checked_add(1 + 8 + 32)
            .and_then(|offset| offset.checked_add(authority_length))
            .ok_or_else(|| CdfError::data("task-set authority bounds overflowed u64"))?;
        if expected_authority_end != footer_offset {
            return Err(CdfError::data(
                "task-set authority frame does not end at the canonical footer",
            ));
        }
        let mut expected_authority_digest = [0_u8; 32];
        tail.read_exact(&mut expected_authority_digest)
            .map_err(|error| {
                artifact_io_error("read task-set authority digest", &cursor.path, error)
            })?;
        let authority_request = ReservationRequest::new(
            ConsumerKey::new("external-task-set-authority", MemoryClass::Control)?,
            authority_length,
        )?;
        let authority_lease = reserve_blocking(Arc::clone(&memory), &authority_request)?;
        let mut authority = vec![
            0_u8;
            usize::try_from(authority_length).map_err(|_| {
                CdfError::data("task-set authority exceeds addressable memory")
            })?
        ];
        tail.read_exact(&mut authority)
            .map_err(|error| artifact_io_error("read task-set authority", &cursor.path, error))?;
        let observed_authority_digest: [u8; 32] = Sha256::digest(&authority).into();
        if observed_authority_digest != expected_authority_digest {
            return Err(CdfError::data(
                "task-set shared authority does not match its content identity",
            ));
        }
        Ok(ExternalTaskSetReader {
            reference,
            cursor,
            expected_ordinal: 0,
            maximum_task_bytes,
            memory,
            authority: Arc::new(AccountedBytes::new(
                Bytes::from(authority),
                authority_lease,
            )?),
            authority_sha256: format!("sha256:{}", hex::encode(observed_authority_digest)),
            task_end: authority_offset,
            footer_task_count,
            finished: false,
        })
    }

    /// Creates an invocation-local workspace beside task artifacts.
    ///
    /// The workspace is never serialized into a task reference and is removed on drop. Callers
    /// remain responsible for accounting every byte written through the shared spill authority.
    pub fn temporary_workspace(&self, label: &str) -> Result<ExternalTaskWorkspace> {
        require_token("task-store workspace label", label)?;
        let directory = self.root.join(self.namespace.as_str()).join("scratch");
        fs::create_dir_all(&directory)
            .map_err(|error| io_error("create task-store scratch directory", &directory, error))?;
        let directory = tempfile::Builder::new()
            .prefix(&format!("{label}-"))
            .tempdir_in(&directory)
            .map_err(|error| {
                io_error("create task-store temporary workspace", &directory, error)
            })?;
        Ok(ExternalTaskWorkspace { directory })
    }

    /// Creates one fully accounted scratch workspace for a source-owned planning index.
    ///
    /// The source still owns the index schema and algorithms. This envelope owns its resident
    /// memory, shared spill reservation, growth policy, temporary directory, and exact cleanup.
    pub fn accounted_workspace(
        &self,
        label: &str,
        limits: ExternalTaskWorkspaceLimits,
        memory: Arc<dyn MemoryCoordinator>,
        spill: Arc<dyn SpillBudgetCoordinator>,
    ) -> Result<AccountedExternalTaskWorkspace> {
        let consumer = ConsumerKey::new(&limits.consumer, limits.memory_class)?;
        let memory_lease = reserve_blocking(
            memory,
            &ReservationRequest::new(consumer, limits.resident_bytes)?,
        )?;
        let available = available_spill_bytes(spill.as_ref());
        let initial = limits.spill_growth_bytes.min(available);
        if initial < limits.minimum_spill_bytes {
            return Err(CdfError::data(format!(
                "{} requires at least {} free spill bytes but only {available} are available",
                limits.consumer, limits.minimum_spill_bytes
            )));
        }
        let spill_reservation = spill.try_reserve(initial)?.ok_or_else(|| {
            CdfError::data(format!(
                "{} could not acquire its initial shared spill reservation",
                limits.consumer
            ))
        })?;
        let workspace = self.temporary_workspace(label)?;
        Ok(AccountedExternalTaskWorkspace {
            workspace,
            spill,
            spill_reservation,
            spill_growth_bytes: limits.spill_growth_bytes,
            minimum_spill_bytes: limits.minimum_spill_bytes,
            consumer: limits.consumer,
            _memory_lease: memory_lease,
        })
    }

    fn path_for_reference(&self, reference: &PlannedTaskSetReference) -> Result<PathBuf> {
        let key = Path::new(reference.object_key.as_str());
        if key.is_absolute()
            || key
                .components()
                .any(|component| !matches!(component, Component::Normal(_) | Component::CurDir))
        {
            return Err(CdfError::contract(
                "task-set object key must be a safe relative path",
            ));
        }
        Ok(self.root.join(self.namespace.as_str()).join(key))
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

/// RAII ownership for invocation-local planner scratch.
pub struct ExternalTaskWorkspace {
    directory: tempfile::TempDir,
}

impl ExternalTaskWorkspace {
    pub fn path(&self) -> &Path {
        self.directory.path()
    }
}

/// RAII envelope for a source-owned task-planning index.
pub struct AccountedExternalTaskWorkspace {
    workspace: ExternalTaskWorkspace,
    spill: Arc<dyn SpillBudgetCoordinator>,
    spill_reservation: SpillReservation,
    spill_growth_bytes: u64,
    minimum_spill_bytes: u64,
    consumer: String,
    _memory_lease: MemoryLease,
}

impl AccountedExternalTaskWorkspace {
    pub fn path(&self) -> &Path {
        self.workspace.path()
    }

    pub fn reserved_spill_bytes(&self) -> u64 {
        self.spill_reservation.bytes()
    }

    /// Grows by one admitted quantum, accepting a smaller final quantum only when it still
    /// satisfies the index's declared minimum.
    pub fn grow_spill(&mut self) -> Result<()> {
        let available = available_spill_bytes(self.spill.as_ref());
        let additional = self.spill_growth_bytes.min(available);
        if additional < self.minimum_spill_bytes || !self.spill_reservation.try_grow(additional)? {
            return Err(CdfError::data(format!(
                "{} exhausted its shared spill budget after {} bytes",
                self.consumer,
                self.spill_reservation.bytes()
            )));
        }
        Ok(())
    }

    /// Ensures an observed file footprint remains subordinate to the shared spill authority.
    pub fn ensure_spill_bytes(&mut self, required: u64) -> Result<()> {
        if required <= self.spill_reservation.bytes() {
            return Ok(());
        }
        let additional = required
            .saturating_sub(self.spill_reservation.bytes())
            .div_ceil(self.spill_growth_bytes)
            .saturating_mul(self.spill_growth_bytes);
        if !self.spill_reservation.try_grow(additional)? {
            return Err(CdfError::data(format!(
                "{} requires {} spill bytes but the shared disk budget is exhausted",
                self.consumer,
                self.spill_reservation.bytes().saturating_add(additional)
            )));
        }
        Ok(())
    }
}

pub struct ExternalTaskSetWriter {
    store: ExternalTaskStore,
    task_type: String,
    limits: TaskSetLimits,
    temporary: Option<NamedTempFile>,
    writer: Option<BufWriter<HashingWriter>>,
    payload: Vec<u8>,
    next_ordinal: u64,
    spill_reservation: Option<SpillReservation>,
    _memory_lease: cdf_memory::MemoryLease,
    poisoned: bool,
}

impl ExternalTaskSetWriter {
    /// Appends one payload whose encoder is responsible for canonical semantic bytes.
    ///
    /// The store deliberately accepts a writer callback rather than arbitrary `Serialize`:
    /// unordered user maps cannot accidentally masquerade as canonical task identity, and the
    /// encoder cannot allocate an unbounded intermediate payload inside this authority.
    pub fn push_with(
        &mut self,
        canonical_ordinal: u64,
        encode: impl FnOnce(&mut dyn Write) -> Result<()>,
    ) -> Result<()> {
        self.push_checked(canonical_ordinal, encode)
    }

    fn push_checked(
        &mut self,
        canonical_ordinal: u64,
        encode: impl FnOnce(&mut dyn Write) -> Result<()>,
    ) -> Result<()> {
        if self.poisoned {
            return Err(CdfError::contract(
                "task-set writer cannot continue after a partial write failure",
            ));
        }
        if canonical_ordinal != self.next_ordinal {
            return Err(CdfError::contract(format!(
                "task-set canonical ordinal {canonical_ordinal} is out of order; expected {}",
                self.next_ordinal
            )));
        }
        self.payload.clear();
        let maximum = usize::try_from(self.limits.maximum_task_bytes)
            .map_err(|_| CdfError::contract("task-set task budget exceeds usize"))?;
        let mut bounded = BoundedVec::new(&mut self.payload, maximum);
        let mut hashing = DigestingWriter::new(&mut bounded);
        encode(&mut hashing)?;
        let payload_digest = hashing.finalize();
        if self.payload.is_empty() {
            return Err(CdfError::data("canonical task payload cannot be empty"));
        }
        let payload_length = u64::try_from(self.payload.len())
            .map_err(|_| CdfError::data("canonical task payload exceeds u64"))?;
        let frame_bytes = 1_u64
            .checked_add(8)
            .and_then(|value| value.checked_add(8))
            .and_then(|value| value.checked_add(32))
            .and_then(|value| value.checked_add(payload_length))
            .ok_or_else(|| CdfError::data("task-set frame length overflowed u64"))?;
        self.reserve_spill(frame_bytes)?;
        self.write_unreserved(&[TASK_TAG], "write task-set record tag")?;
        self.write_unreserved(
            &canonical_ordinal.to_be_bytes(),
            "write task-set record ordinal",
        )?;
        self.write_unreserved(
            &payload_length.to_be_bytes(),
            "write task-set record length",
        )?;
        self.write_unreserved(&payload_digest, "write task-set record digest")?;
        let payload = std::mem::take(&mut self.payload);
        let result = self.write_unreserved(&payload, "write task-set record payload");
        self.payload = payload;
        result?;
        self.next_ordinal = self
            .next_ordinal
            .checked_add(1)
            .ok_or_else(|| CdfError::data("task-set ordinal overflowed u64"))?;
        Ok(())
    }

    pub fn finalize(
        self,
        encode_authority: impl FnOnce(&mut dyn Write) -> Result<()>,
    ) -> Result<ExternalTaskSetArtifact> {
        self.finalize_checked(None, None, encode_authority)
    }

    pub fn finalize_with_authority_hash(
        self,
        expected_authority_sha256: &str,
        encode_authority: impl FnOnce(&mut dyn Write) -> Result<()>,
    ) -> Result<ExternalTaskSetArtifact> {
        cdf_runtime::validate_artifact_hash(
            "expected task-set authority",
            expected_authority_sha256,
        )?;
        self.finalize_checked(None, Some(expected_authority_sha256), encode_authority)
    }

    pub fn finalize_with_authority_hash_and_cancellation(
        self,
        expected_authority_sha256: &str,
        cancellation: &RunCancellation,
        encode_authority: impl FnOnce(&mut dyn Write) -> Result<()>,
    ) -> Result<ExternalTaskSetArtifact> {
        cdf_runtime::validate_artifact_hash(
            "expected task-set authority",
            expected_authority_sha256,
        )?;
        cancellation.check()?;
        self.finalize_checked(
            Some(cancellation),
            Some(expected_authority_sha256),
            encode_authority,
        )
    }

    fn finalize_checked(
        mut self,
        cancellation: Option<&RunCancellation>,
        expected_authority_sha256: Option<&str>,
        encode_authority: impl FnOnce(&mut dyn Write) -> Result<()>,
    ) -> Result<ExternalTaskSetArtifact> {
        if self.poisoned {
            return Err(CdfError::contract(
                "task-set writer cannot finalize after a partial write failure",
            ));
        }
        self.payload.clear();
        let maximum_authority_bytes = usize::try_from(self.limits.maximum_authority_bytes)
            .map_err(|_| CdfError::contract("task-set authority budget exceeds usize"))?;
        encode_authority(&mut BoundedVec::new(
            &mut self.payload,
            maximum_authority_bytes,
        ))?;
        if self.payload.is_empty() {
            return Err(CdfError::data(
                "task-set shared authority payload cannot be empty",
            ));
        }
        let authority_length = u64::try_from(self.payload.len())
            .map_err(|_| CdfError::data("task-set authority payload exceeds u64"))?;
        let authority_digest: [u8; 32] = Sha256::digest(&self.payload).into();
        let authority_sha256 = format!("sha256:{}", hex::encode(authority_digest));
        if expected_authority_sha256.is_some_and(|expected| expected != authority_sha256) {
            return Err(CdfError::data(
                "encoded task-set authority does not match its typed content identity",
            ));
        }

        self.writer_mut()?
            .flush()
            .map_err(|error| io_error("flush task-set body", self.temporary_path(), error))?;
        let authority_offset = self.writer_mut()?.get_ref().bytes;
        let tail_bytes = 1_u64
            .checked_add(8 + 32)
            .and_then(|bytes| bytes.checked_add(authority_length))
            .and_then(|bytes| bytes.checked_add(FOOTER_BYTES))
            .ok_or_else(|| CdfError::data("task-set trailer length overflowed u64"))?;
        self.reserve_spill(tail_bytes)?;
        self.write_unreserved(&[AUTHORITY_TAG], "write task-set authority tag")?;
        self.write_unreserved(
            &authority_length.to_be_bytes(),
            "write task-set authority length",
        )?;
        self.write_unreserved(&authority_digest, "write task-set authority digest")?;
        let payload = std::mem::take(&mut self.payload);
        let result = self.write_unreserved(&payload, "write task-set authority payload");
        self.payload = payload;
        result?;
        self.write_unreserved(&[FOOTER_TAG], "write task-set footer tag")?;
        self.write_unreserved(
            &self.next_ordinal.to_be_bytes(),
            "write task-set footer count",
        )?;
        self.write_unreserved(
            &authority_offset.to_be_bytes(),
            "write task-set authority offset",
        )?;
        let writer = self
            .writer
            .take()
            .ok_or_else(|| CdfError::contract("task-set writer was already finalized"))?;
        let mut hashing = writer.into_inner().map_err(|error| {
            io_error(
                "flush task-set writer",
                self.temporary_path(),
                error.into_error(),
            )
        })?;
        hashing
            .flush()
            .map_err(|error| io_error("flush task-set artifact", self.temporary_path(), error))?;
        hashing
            .file
            .sync_all()
            .map_err(|error| io_error("sync task-set artifact", self.temporary_path(), error))?;
        if let Some(cancellation) = cancellation {
            cancellation.check()?;
        }
        let byte_count = hashing.bytes;
        let digest = format!("sha256:{}", hex::encode(hashing.hasher.finalize()));
        drop(hashing.file);

        let hex_digest = digest.trim_start_matches("sha256:");
        let object_key_text = format!("task-sets/sha256/{hex_digest}.cdftasks");
        let object_key = ContentObjectKey::new(object_key_text.clone())?;
        let final_path = self
            .store
            .root
            .join(self.store.namespace.as_str())
            .join(&object_key_text);
        let reference = PlannedTaskSetReference {
            version: PLANNED_TASK_SET_REFERENCE_VERSION,
            task_type: self.task_type.clone(),
            task_count: self.next_ordinal,
            store_namespace: self.store.namespace.clone(),
            object_key,
            byte_count,
            content_sha256: digest.clone(),
            provider_generation: ContentProviderGeneration::new(digest.clone())?,
        };
        reference.validate()?;
        let temporary = self
            .temporary
            .take()
            .ok_or_else(|| CdfError::contract("task-set temporary file is missing"))?;
        install_content_addressed(temporary, &final_path, byte_count, &digest)?;

        if let Some(mut reservation) = self.spill_reservation.take() {
            reservation.shrink(reservation.bytes());
        }
        Ok(ExternalTaskSetArtifact {
            task_type: self.task_type,
            task_count: self.next_ordinal,
            authority_sha256,
            reference,
            path: final_path,
        })
    }

    fn writer_mut(&mut self) -> Result<&mut BufWriter<HashingWriter>> {
        self.writer
            .as_mut()
            .ok_or_else(|| CdfError::contract("task-set writer was already finalized"))
    }

    fn temporary_path(&self) -> &Path {
        self.temporary
            .as_ref()
            .map_or_else(|| Path::new("<finalized-task-set>"), NamedTempFile::path)
    }

    fn reserve_spill(&mut self, additional: u64) -> Result<()> {
        let reservation = self
            .spill_reservation
            .as_mut()
            .ok_or_else(|| CdfError::contract("task-set spill reservation is missing"))?;
        if !reservation.try_grow(additional)? {
            return Err(CdfError::data(
                "task-set artifact exceeded the configured disk budget; increase the spill budget or narrow the planned table extent",
            ));
        }
        Ok(())
    }

    fn write_unreserved(&mut self, bytes: &[u8], action: &str) -> Result<()> {
        let path = self.temporary_path().to_path_buf();
        if let Err(error) = self.writer_mut()?.write_all(bytes) {
            self.poisoned = true;
            return Err(io_error(action, &path, error));
        }
        Ok(())
    }

    fn write_reserved(&mut self, bytes: &[u8]) -> Result<()> {
        let length = u64::try_from(bytes.len())
            .map_err(|_| CdfError::data("task-set write length exceeds u64"))?;
        self.reserve_spill(length)?;
        self.write_unreserved(bytes, "write task-set artifact")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalTaskSetArtifact {
    pub task_type: String,
    pub task_count: u64,
    pub authority_sha256: String,
    pub reference: PlannedTaskSetReference,
    pub path: PathBuf,
}

struct ExternalTaskSetReadCursor {
    file: File,
    path: PathBuf,
    hasher: Sha256,
    observed_bytes: u64,
}

pub struct ExternalTaskSetReader {
    reference: PlannedTaskSetReference,
    cursor: ExternalTaskSetReadCursor,
    expected_ordinal: u64,
    maximum_task_bytes: u64,
    memory: Arc<dyn MemoryCoordinator>,
    authority: Arc<AccountedBytes>,
    authority_sha256: String,
    task_end: u64,
    footer_task_count: u64,
    finished: bool,
}

impl ExternalTaskSetReader {
    pub fn authority(&self) -> &AccountedBytes {
        self.authority.as_ref()
    }

    pub fn retained_authority(&self) -> Arc<AccountedBytes> {
        Arc::clone(&self.authority)
    }

    pub fn authority_sha256(&self) -> &str {
        &self.authority_sha256
    }
    /// Returns the next task. `None` is returned only after the footer and whole-artifact
    /// identity have been verified, so a successful drain is the caller's side-effect barrier.
    pub fn next_record(&mut self) -> Result<Option<ExternalTaskRecord>> {
        if self.finished {
            return Ok(None);
        }
        if self.cursor.observed_bytes == self.task_end {
            return self.finish_tail();
        }
        if self.cursor.observed_bytes > self.task_end {
            return Err(CdfError::data(
                "task-set task body crossed the authority boundary",
            ));
        }
        let tag = self.read_array::<1>()?[0];
        match tag {
            TASK_TAG => {
                let ordinal = u64::from_be_bytes(self.read_array::<8>()?);
                if ordinal != self.expected_ordinal {
                    return Err(CdfError::data(format!(
                        "task-set ordinal {ordinal} is noncanonical; expected {}",
                        self.expected_ordinal
                    )));
                }
                let payload_length = u64::from_be_bytes(self.read_array::<8>()?);
                if payload_length == 0 || payload_length > self.maximum_task_bytes {
                    return Err(CdfError::data(format!(
                        "task-set payload length {payload_length} exceeds the configured per-task budget {}",
                        self.maximum_task_bytes
                    )));
                }
                let expected_digest = self.read_array::<32>()?;
                let remaining = self
                    .task_end
                    .checked_sub(self.cursor.observed_bytes)
                    .ok_or_else(|| {
                        CdfError::data("task-set task frame crossed the authority boundary")
                    })?;
                if payload_length > remaining {
                    return Err(CdfError::data(
                        "task-set task payload crosses the authority boundary",
                    ));
                }
                let request = ReservationRequest::new(
                    ConsumerKey::new("external-task-set-record", MemoryClass::Control)?,
                    payload_length,
                )?;
                let lease = reserve_blocking(Arc::clone(&self.memory), &request)?;
                let payload_length_usize = usize::try_from(payload_length)
                    .map_err(|_| CdfError::data("task-set payload exceeds addressable memory"))?;
                let payload = self.read_vec(payload_length_usize)?;
                let observed_digest: [u8; 32] = Sha256::digest(&payload).into();
                if observed_digest != expected_digest {
                    return Err(CdfError::data(format!(
                        "task-set payload {ordinal} does not match its content identity"
                    )));
                }
                self.expected_ordinal = self
                    .expected_ordinal
                    .checked_add(1)
                    .ok_or_else(|| CdfError::data("task-set ordinal overflowed u64"))?;
                Ok(Some(ExternalTaskRecord {
                    canonical_ordinal: ordinal,
                    content_sha256: format!("sha256:{}", hex::encode(expected_digest)),
                    payload: AccountedBytes::new(Bytes::from(payload), lease)?,
                }))
            }
            other => Err(CdfError::data(format!(
                "task-set task body contains unknown frame tag {other}"
            ))),
        }
    }

    pub fn observed_task_count(&self) -> u64 {
        self.expected_ordinal
    }

    fn finish_tail(&mut self) -> Result<Option<ExternalTaskRecord>> {
        if self.expected_ordinal != self.footer_task_count {
            return Err(CdfError::data(format!(
                "task-set footer count {} does not match {} observed records",
                self.footer_task_count, self.expected_ordinal
            )));
        }
        if self.read_array::<1>()?[0] != AUTHORITY_TAG {
            return Err(CdfError::data("task-set authority tag changed"));
        }
        let authority_length = u64::from_be_bytes(self.read_array::<8>()?);
        let retained_authority_length = self.authority.payload().len();
        if authority_length
            != u64::try_from(retained_authority_length)
                .map_err(|_| CdfError::data("task-set authority exceeds u64"))?
        {
            return Err(CdfError::data("task-set authority length changed"));
        }
        let authority_digest = self.read_array::<32>()?;
        if format!("sha256:{}", hex::encode(authority_digest)) != self.authority_sha256 {
            return Err(CdfError::data("task-set authority identity changed"));
        }
        let authority = self.read_vec(
            usize::try_from(authority_length)
                .map_err(|_| CdfError::data("task-set authority exceeds addressable memory"))?,
        )?;
        if authority.as_slice() != self.authority.payload() {
            return Err(CdfError::data("task-set authority payload changed"));
        }
        if self.read_array::<1>()?[0] != FOOTER_TAG {
            return Err(CdfError::data("task-set footer tag changed"));
        }
        let record_count = u64::from_be_bytes(self.read_array::<8>()?);
        let authority_offset = u64::from_be_bytes(self.read_array::<8>()?);
        if record_count != self.footer_task_count || authority_offset != self.task_end {
            return Err(CdfError::data("task-set footer authority changed"));
        }
        let mut trailing = [0_u8; 1];
        match self.cursor.file.read(&mut trailing) {
            Ok(0) => {}
            Ok(_) => return Err(CdfError::data("task-set artifact has trailing bytes")),
            Err(error) => {
                return Err(artifact_io_error(
                    "read task-set trailing byte",
                    &self.cursor.path,
                    error,
                ));
            }
        }
        self.verify_complete()?;
        self.finished = true;
        Ok(None)
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.cursor.read_array()
    }

    fn read_vec(&mut self, length: usize) -> Result<Vec<u8>> {
        self.cursor.read_vec(length)
    }

    fn verify_complete(&self) -> Result<()> {
        let observed_digest = format!(
            "sha256:{}",
            hex::encode(self.cursor.hasher.clone().finalize())
        );
        if self.cursor.observed_bytes != self.reference.byte_count
            || observed_digest != self.reference.content_sha256
            || self.reference.provider_generation.as_str() != self.reference.content_sha256
        {
            return Err(CdfError::data(
                "task-set artifact bytes, content identity, or provider generation changed",
            ));
        }
        Ok(())
    }
}

/// Source-owned typed decoding and validation at the external task-set boundary.
///
/// The codec owns no catalog lifecycle. It only translates already-accounted canonical bytes
/// into source types and exposes their independent authority, ordinal, and content identities for
/// the shared reader to verify.
pub trait ExternalTaskSetCodec: Send {
    type Authority: Send + Sync + 'static;
    type Task: Send + Sync + 'static;

    fn decode_authority(&self, payload: &[u8]) -> Result<Self::Authority>;
    fn authority_content_sha256(&self, authority: &Self::Authority) -> Result<String>;
    fn decode_task(&self, payload: &[u8], authority: &Self::Authority) -> Result<Self::Task>;
    fn task_canonical_ordinal(&self, task: &Self::Task) -> u64;
    fn encode_task(&self, task: &Self::Task, output: &mut dyn Write) -> Result<()>;
}

fn encoded_task_content_sha256<C>(codec: &C, task: &C::Task) -> Result<String>
where
    C: ExternalTaskSetCodec,
{
    let mut sink = io::sink();
    let mut hashing = DigestingWriter::new(&mut sink);
    codec.encode_task(task, &mut hashing)?;
    Ok(format!("sha256:{}", hex::encode(hashing.finalize())))
}

/// The source-owned typed encoding half of an external task-set boundary.
pub trait ExternalTaskPlanningCodec: ExternalTaskSetCodec {
    fn set_task_canonical_ordinal(&self, task: &mut Self::Task, ordinal: u64);
    fn encode_authority(&self, authority: &Self::Authority, output: &mut dyn Write) -> Result<()>;
}

/// Typed builder for tasks whose source planner already emits canonical order.
pub struct TypedExternalTaskSetBuilder<C>
where
    C: ExternalTaskPlanningCodec,
{
    writer: ExternalTaskSetWriter,
    codec: C,
    cancellation: RunCancellation,
    next_ordinal: u64,
}

impl<C> TypedExternalTaskSetBuilder<C>
where
    C: ExternalTaskPlanningCodec,
{
    pub fn new(
        store: &ExternalTaskStore,
        task_type: &str,
        limits: TaskSetLimits,
        memory: Arc<dyn MemoryCoordinator>,
        spill: &dyn SpillBudgetCoordinator,
        cancellation: RunCancellation,
        codec: C,
    ) -> Result<Self> {
        cancellation.check()?;
        Ok(Self {
            writer: store.writer(task_type, limits, memory, spill)?,
            codec,
            cancellation,
            next_ordinal: 0,
        })
    }

    pub fn push(&mut self, task: &mut C::Task) -> Result<u64> {
        self.cancellation.check()?;
        let ordinal = self.next_ordinal;
        self.codec.set_task_canonical_ordinal(task, ordinal);
        if self.codec.task_canonical_ordinal(task) != ordinal {
            return Err(CdfError::internal(
                "typed task codec did not install the requested canonical ordinal",
            ));
        }
        self.writer
            .push_with(ordinal, |output| self.codec.encode_task(task, output))?;
        self.next_ordinal = self
            .next_ordinal
            .checked_add(1)
            .ok_or_else(|| CdfError::data("typed task-set ordinal exceeds u64"))?;
        self.cancellation.check()?;
        Ok(ordinal)
    }

    pub fn task_count(&self) -> u64 {
        self.next_ordinal
    }

    pub fn finalize(self, authority: &C::Authority) -> Result<ExternalTaskSetArtifact> {
        self.cancellation.check()?;
        let expected_authority_sha256 = self.codec.authority_content_sha256(authority)?;
        self.writer.finalize_with_authority_hash_and_cancellation(
            &expected_authority_sha256,
            &self.cancellation,
            |output| self.codec.encode_authority(authority, output),
        )
    }
}

/// Typed builder for provider-order tasks that need spill-backed canonical sorting.
pub struct TypedCanonicalTaskSetBuilder<C>
where
    C: ExternalTaskPlanningCodec,
{
    builder: CanonicalTaskSetBuilder,
    codec: C,
    cancellation: RunCancellation,
}

impl<C> TypedCanonicalTaskSetBuilder<C>
where
    C: ExternalTaskPlanningCodec,
{
    pub fn new(
        store: &ExternalTaskStore,
        task_type: &str,
        limits: CanonicalTaskSetLimits,
        memory: Arc<dyn MemoryCoordinator>,
        spill: Arc<dyn SpillBudgetCoordinator>,
        cancellation: RunCancellation,
        codec: C,
    ) -> Result<Self> {
        cancellation.check()?;
        Ok(Self {
            builder: store.canonical_builder(task_type, limits, memory, spill)?,
            codec,
            cancellation,
        })
    }

    pub fn push_idempotent_by<F>(&mut self, mut task: C::Task, sort_key: F) -> Result<bool>
    where
        F: for<'a> FnOnce(&'a C::Task) -> &'a [u8],
    {
        self.cancellation.check()?;
        self.codec.set_task_canonical_ordinal(&mut task, 0);
        let inserted = self
            .builder
            .push_idempotent_with(sort_key(&task), |output| {
                self.codec.encode_task(&task, output)
            })?;
        self.cancellation.check()?;
        Ok(inserted)
    }

    pub fn task_count(&self) -> u64 {
        self.builder.task_count()
    }

    pub fn finalize(self, authority: &C::Authority) -> Result<ExternalTaskSetArtifact> {
        self.cancellation.check()?;
        let expected_authority_sha256 = self.codec.authority_content_sha256(authority)?;
        let codec = &self.codec;
        let cancellation = &self.cancellation;
        self.builder.finalize_transformed_with_authority_hash(
            &expected_authority_sha256,
            &self.cancellation,
            |ordinal, payload, output| {
                cancellation.check()?;
                let mut task = codec.decode_task(payload, authority)?;
                codec.set_task_canonical_ordinal(&mut task, ordinal);
                if codec.task_canonical_ordinal(&task) != ordinal {
                    return Err(CdfError::internal(
                        "typed canonical task codec did not install the requested ordinal",
                    ));
                }
                codec.encode_task(&task, output)
            },
            |output| codec.encode_authority(authority, output),
        )
    }
}

/// Accounted parse-memory policy for one authority or task record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalTaskParseMemory {
    consumer: String,
    class: MemoryClass,
    admission: ExternalTaskParseAdmission,
    amplification_bps: u32,
    fixed_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExternalTaskParseAdmission {
    FailFast,
    Blocking,
}

impl ExternalTaskParseMemory {
    pub fn fail_fast(
        consumer: impl Into<String>,
        class: MemoryClass,
        amplification_bps: u32,
        fixed_bytes: u64,
    ) -> Result<Self> {
        Self::new(
            consumer,
            class,
            ExternalTaskParseAdmission::FailFast,
            amplification_bps,
            fixed_bytes,
        )
    }

    pub fn blocking(
        consumer: impl Into<String>,
        class: MemoryClass,
        amplification_bps: u32,
        fixed_bytes: u64,
    ) -> Result<Self> {
        Self::new(
            consumer,
            class,
            ExternalTaskParseAdmission::Blocking,
            amplification_bps,
            fixed_bytes,
        )
    }

    fn new(
        consumer: impl Into<String>,
        class: MemoryClass,
        admission: ExternalTaskParseAdmission,
        amplification_bps: u32,
        fixed_bytes: u64,
    ) -> Result<Self> {
        let consumer = consumer.into();
        require_token("external task parse-memory consumer", &consumer)?;
        if amplification_bps == 0 {
            return Err(CdfError::contract(
                "external task parse-memory amplification must be nonzero",
            ));
        }
        Ok(Self {
            consumer,
            class,
            admission,
            amplification_bps,
            fixed_bytes,
        })
    }

    pub fn reservation_bytes(&self, encoded_bytes: u64) -> Result<u64> {
        let amplified = u128::from(encoded_bytes)
            .checked_mul(u128::from(self.amplification_bps))
            .and_then(|bytes| bytes.checked_add(9_999))
            .map(|bytes| bytes / 10_000)
            .ok_or_else(|| CdfError::data("external task parse reservation overflowed"))?;
        u64::try_from(
            amplified
                .checked_add(u128::from(self.fixed_bytes))
                .ok_or_else(|| CdfError::data("external task parse reservation overflowed"))?
                .max(1),
        )
        .map_err(|_| CdfError::data("external task parse reservation exceeds u64"))
    }

    fn reserve(
        &self,
        memory: Arc<dyn MemoryCoordinator>,
        encoded_bytes: u64,
    ) -> Result<MemoryLease> {
        let consumer = ConsumerKey::new(&self.consumer, self.class)?;
        let request = ReservationRequest::new(consumer, self.reservation_bytes(encoded_bytes)?)?;
        match self.admission {
            ExternalTaskParseAdmission::FailFast => {
                memory.try_reserve(&request)?.ok_or_else(|| {
                    CdfError::data(format!(
                        "external task parsing requires {} bytes for {}, but the memory ledger cannot admit it",
                        request.bytes, self.consumer
                    ))
                })
            }
            ExternalTaskParseAdmission::Blocking => reserve_blocking(memory, &request),
        }
    }
}

/// Shared authority retained once for every typed task decoded from one task-set reader.
pub struct RetainedExternalTaskAuthority<A> {
    model: A,
    _encoded: Arc<AccountedBytes>,
    _parse: MemoryLease,
}

impl<A> RetainedExternalTaskAuthority<A> {
    pub fn model(&self) -> &A {
        &self.model
    }
}

/// One decoded source task with its exact encoded and parse-memory leases.
pub struct RetainedExternalTask<A, T> {
    inner: Arc<RetainedExternalTaskInner<A, T>>,
}

struct RetainedExternalTaskInner<A, T> {
    task: T,
    authority: Arc<RetainedExternalTaskAuthority<A>>,
    canonical_ordinal: u64,
    content_sha256: String,
    retained_bytes: u64,
    _encoded: AccountedBytes,
    _parse: MemoryLease,
}

impl<A, T> RetainedExternalTask<A, T> {
    pub fn task(&self) -> &T {
        &self.inner.task
    }

    pub fn authority(&self) -> &A {
        self.inner.authority.model()
    }

    pub fn canonical_ordinal(&self) -> u64 {
        self.inner.canonical_ordinal
    }

    pub fn content_sha256(&self) -> &str {
        &self.inner.content_sha256
    }

    pub fn retained_bytes(&self) -> u64 {
        self.inner.retained_bytes
    }
}

impl<A, T> Clone for RetainedExternalTask<A, T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Closed budgets and parse-accounting policy for one typed task-set reader.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypedExternalTaskSetReaderConfig {
    task_type: String,
    maximum_task_bytes: u64,
    maximum_authority_bytes: u64,
    authority_parse: ExternalTaskParseMemory,
    task_parse: ExternalTaskParseMemory,
}

impl TypedExternalTaskSetReaderConfig {
    pub fn new(
        task_type: impl Into<String>,
        maximum_task_bytes: u64,
        maximum_authority_bytes: u64,
        authority_parse: ExternalTaskParseMemory,
        task_parse: ExternalTaskParseMemory,
    ) -> Result<Self> {
        let task_type = task_type.into();
        require_token("typed external task-set type", &task_type)?;
        if maximum_task_bytes == 0 || maximum_authority_bytes == 0 {
            return Err(CdfError::contract(
                "typed external task-set budgets must be nonzero",
            ));
        }
        Ok(Self {
            task_type,
            maximum_task_bytes,
            maximum_authority_bytes,
            authority_parse,
            task_parse,
        })
    }
}

/// Typed, cancellation-aware view over one canonical external task set.
pub struct TypedExternalTaskSetReader<C>
where
    C: ExternalTaskSetCodec,
{
    reader: ExternalTaskSetReader,
    codec: C,
    authority: Arc<RetainedExternalTaskAuthority<C::Authority>>,
    memory: Arc<dyn MemoryCoordinator>,
    task_parse: ExternalTaskParseMemory,
    cancellation: RunCancellation,
}

impl<C> TypedExternalTaskSetReader<C>
where
    C: ExternalTaskSetCodec,
{
    pub fn open(
        store: &ExternalTaskStore,
        reference: PlannedTaskSetReference,
        memory: Arc<dyn MemoryCoordinator>,
        cancellation: RunCancellation,
        config: TypedExternalTaskSetReaderConfig,
        codec: C,
    ) -> Result<Self> {
        cancellation.check()?;
        let reader = store.reader(
            reference,
            &config.task_type,
            config.maximum_task_bytes,
            config.maximum_authority_bytes,
            Arc::clone(&memory),
        )?;
        cancellation.check()?;
        let encoded = reader.retained_authority();
        let encoded_bytes = u64::try_from(encoded.payload().len())
            .map_err(|_| CdfError::data("external task authority exceeds u64"))?;
        let parse = config
            .authority_parse
            .reserve(Arc::clone(&memory), encoded_bytes)?;
        let authority = codec.decode_authority(encoded.payload())?;
        cancellation.check()?;
        if codec.authority_content_sha256(&authority)? != reader.authority_sha256() {
            return Err(CdfError::data(
                "decoded task-set authority does not match its task-store identity",
            ));
        }
        Ok(Self {
            reader,
            codec,
            authority: Arc::new(RetainedExternalTaskAuthority {
                model: authority,
                _encoded: encoded,
                _parse: parse,
            }),
            memory,
            task_parse: config.task_parse,
            cancellation,
        })
    }

    pub fn authority(&self) -> &C::Authority {
        self.authority.model()
    }

    pub fn next_task(
        &mut self,
        expected_ordinal: u64,
    ) -> Result<Option<RetainedExternalTask<C::Authority, C::Task>>> {
        self.cancellation.check()?;
        let Some(record) = self.reader.next_record()? else {
            return Ok(None);
        };
        if record.canonical_ordinal != expected_ordinal {
            return Err(CdfError::data(format!(
                "external task reader returned ordinal {} while execution requested {expected_ordinal}",
                record.canonical_ordinal
            )));
        }
        let encoded_bytes = u64::try_from(record.payload.payload().len())
            .map_err(|_| CdfError::data("external task payload exceeds u64"))?;
        let parse = self
            .task_parse
            .reserve(Arc::clone(&self.memory), encoded_bytes)?;
        let task = self
            .codec
            .decode_task(record.payload.payload(), self.authority.model())?;
        let task_ordinal = self.codec.task_canonical_ordinal(&task);
        let task_content_sha256 = encoded_task_content_sha256(&self.codec, &task)?;
        if task_ordinal != record.canonical_ordinal || task_content_sha256 != record.content_sha256
        {
            return Err(CdfError::data(
                "decoded external task ordinal or content does not match its task-store record",
            ));
        }
        self.cancellation.check()?;
        let retained_bytes = encoded_bytes
            .checked_add(parse.bytes())
            .ok_or_else(|| CdfError::data("retained external task bytes overflowed u64"))?;
        Ok(Some(RetainedExternalTask {
            inner: Arc::new(RetainedExternalTaskInner {
                task,
                authority: Arc::clone(&self.authority),
                canonical_ordinal: record.canonical_ordinal,
                content_sha256: record.content_sha256,
                retained_bytes,
                _encoded: record.payload,
                _parse: parse,
            }),
        }))
    }

    pub fn observed_task_count(&self) -> u64 {
        self.reader.observed_task_count()
    }
}

impl ExternalTaskSetReadCursor {
    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut bytes = [0_u8; N];
        self.file
            .read_exact(&mut bytes)
            .map_err(|error| artifact_io_error("read task-set artifact", &self.path, error))?;
        self.observe(&bytes)?;
        Ok(bytes)
    }

    fn read_vec(&mut self, length: usize) -> Result<Vec<u8>> {
        let mut bytes = vec![0_u8; length];
        self.file
            .read_exact(&mut bytes)
            .map_err(|error| artifact_io_error("read task-set artifact", &self.path, error))?;
        self.observe(&bytes)?;
        Ok(bytes)
    }

    fn observe(&mut self, bytes: &[u8]) -> Result<()> {
        self.hasher.update(bytes);
        self.observed_bytes = self
            .observed_bytes
            .checked_add(
                u64::try_from(bytes.len())
                    .map_err(|_| CdfError::data("task-set observed bytes exceeds u64"))?,
            )
            .ok_or_else(|| CdfError::data("task-set observed bytes overflowed u64"))?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ExternalTaskRecord {
    pub canonical_ordinal: u64,
    pub content_sha256: String,
    pub payload: AccountedBytes,
}

struct BoundedVec<'a> {
    bytes: &'a mut Vec<u8>,
    maximum: usize,
}

impl<'a> BoundedVec<'a> {
    fn new(bytes: &'a mut Vec<u8>, maximum: usize) -> Self {
        Self { bytes, maximum }
    }
}

impl Write for BoundedVec<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("task payload length overflow"))?;
        if next > self.maximum {
            return Err(io::Error::other(format!(
                "task payload exceeds configured {} byte budget",
                self.maximum
            )));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct DigestingWriter<'a> {
    output: &'a mut dyn Write,
    hasher: Sha256,
}

impl<'a> DigestingWriter<'a> {
    fn new(output: &'a mut dyn Write) -> Self {
        Self {
            output,
            hasher: Sha256::new(),
        }
    }

    fn finalize(self) -> [u8; 32] {
        self.hasher.finalize().into()
    }
}

impl Write for DigestingWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.output.write(bytes)?;
        self.hasher.update(&bytes[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
    }
}

struct HashingWriter {
    file: File,
    hasher: Sha256,
    bytes: u64,
}

impl HashingWriter {
    fn new(file: File) -> Self {
        Self {
            file,
            hasher: Sha256::new(),
            bytes: 0,
        }
    }
}

impl Write for HashingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.file.write(bytes)?;
        self.hasher.update(&bytes[..written]);
        self.bytes = self
            .bytes
            .checked_add(u64::try_from(written).map_err(|_| {
                io::Error::other(CdfError::internal("task-set byte count exceeds u64"))
            })?)
            .ok_or_else(|| io::Error::other(CdfError::internal("task-set byte count overflow")))?;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

fn install_content_addressed(
    temporary: NamedTempFile,
    final_path: &Path,
    expected_bytes: u64,
    expected_sha256: &str,
) -> Result<()> {
    if let Some(parent) = final_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| io_error("create task-set content directory", parent, error))?;
    }
    match temporary.persist_noclobber(final_path) {
        Ok(_) => {
            sync_parent(final_path)?;
            Ok(())
        }
        Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {
            verify_file(final_path, expected_bytes, expected_sha256)
        }
        Err(error) => Err(io_error(
            "install task-set content address",
            final_path,
            error.error,
        )),
    }
}

fn verify_file(path: &Path, expected_bytes: u64, expected_sha256: &str) -> Result<()> {
    let mut file = File::open(path)
        .map_err(|error| artifact_io_error("verify task-set artifact", path, error))?;
    let mut hasher = Sha256::new();
    let bytes = io::copy(&mut file, &mut hasher)
        .map_err(|error| artifact_io_error("hash task-set artifact", path, error))?;
    let digest = format!("sha256:{}", hex::encode(hasher.finalize()));
    if bytes != expected_bytes || digest != expected_sha256 {
        return Err(CdfError::contract(format!(
            "content-addressed task-set path {} contains different bytes",
            path.display()
        )));
    }
    Ok(())
}

fn sync_parent(path: &Path) -> Result<()> {
    #[cfg(unix)]
    if let Some(parent) = path.parent() {
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| io_error("sync task-set directory", parent, error))?;
    }
    Ok(())
}

fn configure_canonical_index(connection: &Connection, cache_bytes: u64) -> Result<()> {
    connection
        .pragma_update(
            None,
            "page_size",
            i64::try_from(SQLITE_PAGE_BYTES)
                .map_err(|_| CdfError::internal("SQLite page size exceeds i64"))?,
        )
        .and_then(|_| connection.pragma_update(None, "journal_mode", "OFF"))
        .and_then(|_| connection.pragma_update(None, "synchronous", "OFF"))
        .and_then(|_| connection.pragma_update(None, "locking_mode", "EXCLUSIVE"))
        .and_then(|_| connection.pragma_update(None, "temp_store", "FILE"))
        .and_then(|_| connection.pragma_update(None, "mmap_size", 0_i64))
        .and_then(|_| connection.pragma_update(None, "cache_spill", true))
        .map_err(|error| sqlite_error("configure canonical task index", error))?;
    let cache_kib = cache_bytes.div_ceil(1024).max(1);
    connection
        .pragma_update(
            None,
            "cache_size",
            -i64::try_from(cache_kib).unwrap_or(i64::MAX),
        )
        .map_err(|error| sqlite_error("configure canonical task index cache", error))
}

fn available_spill_bytes(spill: &dyn SpillBudgetCoordinator) -> u64 {
    let snapshot = spill.snapshot();
    snapshot.budget_bytes.saturating_sub(snapshot.current_bytes)
}

fn set_page_ceiling(connection: &Connection, reserved_bytes: u64) -> Result<()> {
    let pages = reserved_bytes / SQLITE_PAGE_BYTES;
    if pages < 2 {
        return Err(CdfError::data(
            "canonical task index spill reservation cannot hold two SQLite pages",
        ));
    }
    connection
        .pragma_update(
            None,
            "max_page_count",
            i64::try_from(pages).unwrap_or(i64::MAX),
        )
        .map_err(|error| sqlite_error("raise canonical task index page ceiling", error))
}

fn is_sqlite_constraint(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(failure, _)
            if failure.code == ErrorCode::ConstraintViolation
    )
}

fn sqlite_page_count(connection: &Connection) -> Result<u64> {
    let page_count: i64 = connection
        .query_row("PRAGMA page_count", [], |row| row.get(0))
        .map_err(|error| sqlite_error("inspect canonical task page count", error))?;
    u64::try_from(page_count)
        .map_err(|_| CdfError::internal("canonical task SQLite page count is negative"))
}

fn sqlite_single_leaf_fits(
    connection: &Connection,
    tree_name: &str,
    required_bytes: u64,
) -> Result<bool> {
    if required_bytes > SQLITE_INDEX_MAX_LOCAL_PAYLOAD_BYTES {
        return Ok(false);
    }
    let mut statement = connection
        .prepare("SELECT pagetype, unused FROM dbstat WHERE name = ?1 LIMIT 2")
        .map_err(|error| sqlite_error("prepare canonical task leaf inspection", error))?;
    let mut rows = statement
        .query(params![tree_name])
        .map_err(|error| sqlite_error("inspect canonical task leaf capacity", error))?;
    let Some(row) = rows
        .next()
        .map_err(|error| sqlite_error("read canonical task leaf capacity", error))?
    else {
        return Err(CdfError::internal(format!(
            "canonical task SQLite tree `{tree_name}` has no root page"
        )));
    };
    let page_type: String = row
        .get(0)
        .map_err(|error| sqlite_error("read canonical task page type", error))?;
    let unused_bytes: i64 = row
        .get(1)
        .map_err(|error| sqlite_error("read canonical task unused bytes", error))?;
    let unused_bytes = u64::try_from(unused_bytes)
        .map_err(|_| CdfError::internal("canonical task SQLite unused bytes are negative"))?;
    let has_second_page = rows
        .next()
        .map_err(|error| sqlite_error("read canonical task leaf count", error))?
        .is_some();
    Ok(page_type == "leaf" && !has_second_page && unused_bytes >= required_bytes)
}

fn sqlite_error(action: &str, error: rusqlite::Error) -> CdfError {
    if sqlite_host_error(&error) {
        CdfError::environment(format!(
            "{action} in CDF-managed task scratch: {error}; check temporary storage, permissions, free space, memory, and process file limits before retrying"
        ))
    } else {
        CdfError::internal(format!("{action} in CDF-managed task scratch: {error}"))
    }
}

fn is_sqlite_full(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(failure, _) if failure.code == ErrorCode::DiskFull
    )
}

fn sqlite_host_error(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(failure, _)
            if matches!(
                failure.code,
                ErrorCode::PermissionDenied
                    | ErrorCode::OutOfMemory
                    | ErrorCode::ReadOnly
                    | ErrorCode::SystemIoFailure
                    | ErrorCode::NotFound
                    | ErrorCode::DiskFull
                    | ErrorCode::CannotOpen
                    | ErrorCode::FileLockingProtocolFailed
                    | ErrorCode::NoLargeFileSupport
                    | ErrorCode::AuthorizationForStatementDenied
            )
    )
}

fn validate_relative_component(value: &str, label: &str) -> Result<()> {
    let path = Path::new(value);
    if path.components().count() != 1
        || !matches!(path.components().next(), Some(Component::Normal(_)))
    {
        return Err(CdfError::contract(format!(
            "{label} must be one safe path component"
        )));
    }
    Ok(())
}

fn require_token(label: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > usize::from(u16::MAX)
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(CdfError::contract(format!(
            "{label} must be a nonempty canonical ASCII token"
        )));
    }
    Ok(())
}

fn io_error(action: &str, path: &Path, error: io::Error) -> CdfError {
    if let Some(mut classified) = cdf_kernel::embedded_cdf_error(&error) {
        classified.message = format!("{action} {}: {}", path.display(), classified.message);
        return classified;
    }
    CdfError::environment(format!(
        "{action} {}: {error}; check the local path, permissions, temporary storage, and process file limits before retrying",
        path.display()
    ))
}

fn artifact_io_error(action: &str, path: &Path, error: io::Error) -> CdfError {
    if let Some(mut classified) = cdf_kernel::embedded_cdf_error(&error) {
        classified.message = format!("{action} {}: {}", path.display(), classified.message);
        return classified;
    }
    if matches!(
        error.kind(),
        io::ErrorKind::NotFound
            | io::ErrorKind::UnexpectedEof
            | io::ErrorKind::InvalidData
            | io::ErrorKind::NotADirectory
            | io::ErrorKind::IsADirectory
    ) || cdf_kernel::is_filesystem_loop(&error)
    {
        CdfError::data(format!("{action} {}: {error}", path.display()))
    } else {
        io_error(action, path, error)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use cdf_memory::{DeterministicMemoryCoordinator, MemoryCoordinator};
    use cdf_runtime::{FixedSpillBudget, SpillBudgetCoordinator};
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn sqlite_host_failure_is_environment_but_scratch_invariant_is_internal() {
        let host = sqlite_error(
            "open canonical task index",
            rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CANTOPEN),
                None,
            ),
        );
        assert_eq!(host.kind, cdf_kernel::ErrorKind::Environment);
        assert!(host.message.contains("temporary storage"));

        let invariant = sqlite_error("decode canonical task row", rusqlite::Error::InvalidQuery);
        assert_eq!(invariant.kind, cdf_kernel::ErrorKind::Internal);
    }

    #[test]
    fn task_artifact_io_separates_missing_data_from_host_failure() {
        let path = Path::new("tasks.cdf");
        let missing = artifact_io_error(
            "open task-set artifact",
            path,
            io::Error::new(io::ErrorKind::NotFound, "missing"),
        );
        assert_eq!(missing.kind, cdf_kernel::ErrorKind::Data);

        let directory = artifact_io_error(
            "read task-set artifact",
            path,
            io::Error::new(io::ErrorKind::IsADirectory, "is a directory"),
        );
        assert_eq!(directory.kind, cdf_kernel::ErrorKind::Data);

        let host = artifact_io_error(
            "open task-set artifact",
            path,
            io::Error::new(io::ErrorKind::PermissionDenied, "denied"),
        );
        assert_eq!(host.kind, cdf_kernel::ErrorKind::Environment);
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct SyntheticTask {
        partition: u64,
        path: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct SyntheticAuthority {
        version: u32,
    }

    #[derive(Default)]
    struct SyntheticCodec {
        authority_hash_override: Option<String>,
        task_hash_override: Option<String>,
    }

    impl ExternalTaskSetCodec for SyntheticCodec {
        type Authority = SyntheticAuthority;
        type Task = SyntheticTask;

        fn decode_authority(&self, payload: &[u8]) -> Result<Self::Authority> {
            let authority: SyntheticAuthority = serde_json::from_slice(payload)
                .map_err(|error| CdfError::data(format!("decode synthetic authority: {error}")))?;
            if authority.version != 1 {
                return Err(CdfError::data(
                    "synthetic authority has an unsupported version",
                ));
            }
            Ok(authority)
        }

        fn authority_content_sha256(&self, authority: &Self::Authority) -> Result<String> {
            if let Some(hash) = &self.authority_hash_override {
                return Ok(hash.clone());
            }
            canonical_json_hash(authority)
        }

        fn decode_task(&self, payload: &[u8], authority: &Self::Authority) -> Result<Self::Task> {
            if authority.version != 1 {
                return Err(CdfError::data(
                    "synthetic task authority changed during decode",
                ));
            }
            serde_json::from_slice(payload)
                .map_err(|error| CdfError::data(format!("decode synthetic task: {error}")))
        }

        fn task_canonical_ordinal(&self, task: &Self::Task) -> u64 {
            task.partition
        }

        fn encode_task(&self, task: &Self::Task, output: &mut dyn Write) -> Result<()> {
            serde_json::to_writer(&mut *output, task)
                .map_err(|error| CdfError::data(format!("encode synthetic task: {error}")))?;
            if let Some(suffix) = &self.task_hash_override {
                output.write_all(suffix.as_bytes()).map_err(|error| {
                    CdfError::data(format!("encode synthetic task identity suffix: {error}"))
                })?;
            }
            Ok(())
        }
    }

    impl ExternalTaskPlanningCodec for SyntheticCodec {
        fn set_task_canonical_ordinal(&self, task: &mut Self::Task, ordinal: u64) {
            task.partition = ordinal;
        }

        fn encode_authority(
            &self,
            authority: &Self::Authority,
            output: &mut dyn Write,
        ) -> Result<()> {
            serde_json::to_writer(output, authority)
                .map_err(|error| CdfError::data(format!("encode synthetic authority: {error}")))
        }
    }

    struct RejectDecodedTaskCodec;

    impl ExternalTaskSetCodec for RejectDecodedTaskCodec {
        type Authority = SyntheticAuthority;
        type Task = SyntheticTask;

        fn decode_authority(&self, payload: &[u8]) -> Result<Self::Authority> {
            serde_json::from_slice(payload)
                .map_err(|error| CdfError::data(format!("decode synthetic authority: {error}")))
        }

        fn authority_content_sha256(&self, authority: &Self::Authority) -> Result<String> {
            canonical_json_hash(authority)
        }

        fn decode_task(&self, _payload: &[u8], _authority: &Self::Authority) -> Result<Self::Task> {
            Err(CdfError::data("synthetic malformed planning record"))
        }

        fn task_canonical_ordinal(&self, task: &Self::Task) -> u64 {
            task.partition
        }

        fn encode_task(&self, task: &Self::Task, output: &mut dyn Write) -> Result<()> {
            serde_json::to_writer(output, task)
                .map_err(|error| CdfError::data(format!("encode synthetic task: {error}")))
        }
    }

    impl ExternalTaskPlanningCodec for RejectDecodedTaskCodec {
        fn set_task_canonical_ordinal(&self, task: &mut Self::Task, ordinal: u64) {
            task.partition = ordinal;
        }

        fn encode_authority(
            &self,
            authority: &Self::Authority,
            output: &mut dyn Write,
        ) -> Result<()> {
            serde_json::to_writer(output, authority)
                .map_err(|error| CdfError::data(format!("encode synthetic authority: {error}")))
        }
    }

    fn canonical_json_hash(value: &impl Serialize) -> Result<String> {
        let bytes = serde_json::to_vec(value)
            .map_err(|error| CdfError::data(format!("encode synthetic model: {error}")))?;
        Ok(format!("sha256:{}", hex::encode(Sha256::digest(bytes))))
    }

    fn typed_config(task_type: &str) -> TypedExternalTaskSetReaderConfig {
        TypedExternalTaskSetReaderConfig::new(
            task_type,
            4096,
            4096,
            ExternalTaskParseMemory::blocking(
                "synthetic-authority-parse",
                MemoryClass::Control,
                10_000,
                0,
            )
            .unwrap(),
            ExternalTaskParseMemory::blocking(
                "synthetic-task-parse",
                MemoryClass::Control,
                10_000,
                0,
            )
            .unwrap(),
        )
        .unwrap()
    }

    fn authorities(
        memory_bytes: u64,
        spill_bytes: u64,
    ) -> (Arc<dyn MemoryCoordinator>, FixedSpillBudget) {
        (
            Arc::new(DeterministicMemoryCoordinator::new(memory_bytes, BTreeMap::new()).unwrap()),
            FixedSpillBudget::new(spill_bytes).unwrap(),
        )
    }

    fn store(root: &TempDir) -> ExternalTaskStore {
        ExternalTaskStore::new(
            root.path(),
            ContentStoreNamespace::new("planner-artifacts").unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn unavailable_temporary_directory_is_environment_owned() {
        let root = TempDir::new().unwrap();
        fs::write(root.path().join("planner-artifacts"), b"not a directory").unwrap();
        let store = ExternalTaskStore::new(
            root.path(),
            ContentStoreNamespace::new("planner-artifacts").unwrap(),
        )
        .unwrap();

        let error = match store.temporary_workspace("audit") {
            Ok(_) => panic!("a file in place of the temporary root must fail"),
            Err(error) => error,
        };

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Environment);
        assert!(error.message.contains("temporary"));
        assert!(error.message.contains("process file limits"));
    }

    fn limits() -> TaskSetLimits {
        TaskSetLimits {
            maximum_task_bytes: 4096,
            maximum_authority_bytes: 4096,
            writer_buffer_bytes: 8192,
        }
    }

    fn canonical_limits() -> CanonicalTaskSetLimits {
        CanonicalTaskSetLimits {
            tasks: limits(),
            maximum_sort_key_bytes: 1024,
            index_cache_bytes: 16 * 1024,
            spill_growth_bytes: 16 * 1024,
            minimum_initial_spill_bytes: SQLITE_PAGE_BYTES * 2,
        }
    }

    fn encode_authority(output: &mut dyn Write) -> Result<()> {
        output
            .write_all(br#"{"version":1}"#)
            .map_err(|error| CdfError::data(format!("encode synthetic authority: {error}")))
    }

    fn push_task(
        writer: &mut ExternalTaskSetWriter,
        ordinal: u64,
        task: &SyntheticTask,
    ) -> Result<()> {
        writer.push_with(ordinal, |output| {
            serde_json::to_writer(output, task)
                .map_err(|error| CdfError::data(format!("encode synthetic task: {error}")))
        })
    }

    #[test]
    fn canonical_task_set_round_trips_with_bounded_memory_and_spill() {
        let root = TempDir::new().unwrap();
        let store = store(&root);
        let (memory, spill) = authorities(64 * 1024, 1024 * 1024);
        let mut writer = store
            .writer("synthetic-v1", limits(), Arc::clone(&memory), &spill)
            .unwrap();
        for ordinal in 0..100 {
            push_task(
                &mut writer,
                ordinal,
                &SyntheticTask {
                    partition: ordinal,
                    path: format!("s3://bucket/{ordinal:08}.parquet"),
                },
            )
            .unwrap();
        }
        let artifact = writer.finalize(encode_authority).unwrap();
        assert_eq!(artifact.task_count, 100);
        assert_eq!(artifact.reference.task_count, 100);
        assert_eq!(artifact.authority_sha256, writer_authority_hash());
        let portable = cdf_runtime::WorkerArtifactReference::from(&artifact.reference);
        portable.validate().unwrap();
        assert_eq!(
            portable.kind,
            cdf_runtime::WorkerArtifactKind::PlannedTaskSet
        );
        assert_eq!(spill.snapshot().current_bytes, 0);
        assert!(spill.snapshot().peak_bytes <= 1024 * 1024);
        assert!(memory.snapshot().peak_bytes <= 64 * 1024);

        let mut reader = store
            .reader(
                artifact.reference.clone(),
                "synthetic-v1",
                4096,
                4096,
                Arc::clone(&memory),
            )
            .unwrap();
        assert_eq!(reader.authority().payload(), br#"{"version":1}"#);
        assert_eq!(reader.authority_sha256(), writer_authority_hash());
        let mut count = 0;
        while let Some(record) = reader.next_record().unwrap() {
            let task: SyntheticTask = serde_json::from_slice(record.payload.payload()).unwrap();
            assert_eq!(record.canonical_ordinal, count);
            assert_eq!(task.partition, count);
            count += 1;
        }
        assert_eq!(count, 100);
        assert_eq!(reader.observed_task_count(), 100);
    }

    #[test]
    fn typed_reader_retains_one_accounted_authority_and_task_lifecycle() {
        let root = TempDir::new().unwrap();
        let store = store(&root);
        let (memory, spill) = authorities(64 * 1024, 1024 * 1024);
        let mut writer = store
            .writer("synthetic-v1", limits(), Arc::clone(&memory), &spill)
            .unwrap();
        let task = SyntheticTask {
            partition: 0,
            path: "file:///zero.parquet".to_owned(),
        };
        push_task(&mut writer, 0, &task).unwrap();
        let artifact = writer.finalize(encode_authority).unwrap();
        assert_eq!(memory.snapshot().current_bytes, 0);

        let authority_bytes = u64::try_from(br#"{"version":1}"#.len()).unwrap();
        let task_bytes = u64::try_from(serde_json::to_vec(&task).unwrap().len()).unwrap();
        let mut reader = TypedExternalTaskSetReader::open(
            &store,
            artifact.reference,
            Arc::clone(&memory),
            RunCancellation::default(),
            typed_config("synthetic-v1"),
            SyntheticCodec::default(),
        )
        .unwrap();
        assert_eq!(reader.authority(), &SyntheticAuthority { version: 1 });
        assert_eq!(
            memory.snapshot().current_bytes,
            authority_bytes * 2,
            "authority encoded and parse memory must each be leased once"
        );

        let retained = reader.next_task(0).unwrap().unwrap();
        assert_eq!(retained.task(), &task);
        assert_eq!(retained.canonical_ordinal(), 0);
        assert_eq!(
            retained.content_sha256(),
            canonical_json_hash(&task).unwrap()
        );
        assert_eq!(retained.retained_bytes(), task_bytes * 2);
        assert_eq!(
            memory.snapshot().current_bytes,
            authority_bytes * 2 + task_bytes * 2
        );
        let retained_task_address = std::ptr::from_ref(retained.task());
        let retained_clone = retained.clone();
        assert_eq!(
            std::ptr::from_ref(retained_clone.task()),
            retained_task_address,
            "scheduler lookahead clones must share one decoded task model"
        );
        assert!(reader.next_task(1).unwrap().is_none());
        drop(reader);
        assert_eq!(
            memory.snapshot().current_bytes,
            authority_bytes * 2 + task_bytes * 2,
            "retained task must keep its one shared authority alive"
        );
        drop(retained);
        assert_eq!(
            memory.snapshot().current_bytes,
            authority_bytes * 2 + task_bytes * 2,
            "one clone must retain the singular authority/task leases"
        );
        drop(retained_clone);
        assert_eq!(memory.snapshot().current_bytes, 0);
    }

    #[test]
    fn typed_reader_fails_closed_on_type_authority_ordinal_content_decode_and_cancellation() {
        fn artifact_with(
            store: &ExternalTaskStore,
            memory: Arc<dyn MemoryCoordinator>,
            spill: &dyn SpillBudgetCoordinator,
            encode: impl FnOnce(&mut ExternalTaskSetWriter) -> Result<()>,
        ) -> ExternalTaskSetArtifact {
            let mut writer = store
                .writer("synthetic-v1", limits(), memory, spill)
                .unwrap();
            encode(&mut writer).unwrap();
            writer.finalize(encode_authority).unwrap()
        }

        let task = SyntheticTask {
            partition: 0,
            path: "file:///zero.parquet".to_owned(),
        };

        let root = TempDir::new().unwrap();
        let store = store(&root);
        let (memory, spill) = authorities(64 * 1024, 1024 * 1024);
        let artifact = artifact_with(&store, Arc::clone(&memory), &spill, |writer| {
            push_task(writer, 0, &task)
        });
        let error = TypedExternalTaskSetReader::open(
            &store,
            artifact.reference.clone(),
            Arc::clone(&memory),
            RunCancellation::default(),
            typed_config("wrong-v1"),
            SyntheticCodec::default(),
        )
        .err()
        .unwrap();
        assert!(error.message.contains("type"));

        let error = TypedExternalTaskSetReader::open(
            &store,
            artifact.reference.clone(),
            Arc::clone(&memory),
            RunCancellation::default(),
            typed_config("synthetic-v1"),
            SyntheticCodec {
                authority_hash_override: Some(format!("sha256:{}", "00".repeat(32))),
                task_hash_override: None,
            },
        )
        .err()
        .unwrap();
        assert!(error.message.contains("authority"));

        let mut reader = TypedExternalTaskSetReader::open(
            &store,
            artifact.reference.clone(),
            Arc::clone(&memory),
            RunCancellation::default(),
            typed_config("synthetic-v1"),
            SyntheticCodec::default(),
        )
        .unwrap();
        assert!(
            reader
                .next_task(1)
                .err()
                .unwrap()
                .message
                .contains("execution requested")
        );
        drop(reader);

        let mut wrong_content = TypedExternalTaskSetReader::open(
            &store,
            artifact.reference.clone(),
            Arc::clone(&memory),
            RunCancellation::default(),
            typed_config("synthetic-v1"),
            SyntheticCodec {
                authority_hash_override: None,
                task_hash_override: Some(format!("sha256:{}", "11".repeat(32))),
            },
        )
        .unwrap();
        assert!(
            wrong_content
                .next_task(0)
                .err()
                .unwrap()
                .message
                .contains("ordinal or content")
        );
        drop(wrong_content);

        let wrong_ordinal = SyntheticTask {
            partition: 9,
            path: "file:///nine.parquet".to_owned(),
        };
        let ordinal_artifact = artifact_with(&store, Arc::clone(&memory), &spill, |writer| {
            push_task(writer, 0, &wrong_ordinal)
        });
        let mut ordinal_reader = TypedExternalTaskSetReader::open(
            &store,
            ordinal_artifact.reference,
            Arc::clone(&memory),
            RunCancellation::default(),
            typed_config("synthetic-v1"),
            SyntheticCodec::default(),
        )
        .unwrap();
        assert!(
            ordinal_reader
                .next_task(0)
                .err()
                .unwrap()
                .message
                .contains("ordinal or content")
        );
        drop(ordinal_reader);

        let decode_artifact = artifact_with(&store, Arc::clone(&memory), &spill, |writer| {
            writer.push_with(0, |output| {
                output
                    .write_all(b"not-json")
                    .map_err(|error| CdfError::data(format!("write invalid task: {error}")))
            })
        });
        let mut decode_reader = TypedExternalTaskSetReader::open(
            &store,
            decode_artifact.reference,
            Arc::clone(&memory),
            RunCancellation::default(),
            typed_config("synthetic-v1"),
            SyntheticCodec::default(),
        )
        .unwrap();
        assert!(
            decode_reader
                .next_task(0)
                .err()
                .unwrap()
                .message
                .contains("decode synthetic task")
        );
        drop(decode_reader);

        let cancellation = RunCancellation::default();
        let mut cancelled_reader = TypedExternalTaskSetReader::open(
            &store,
            artifact.reference,
            Arc::clone(&memory),
            cancellation.clone(),
            typed_config("synthetic-v1"),
            SyntheticCodec::default(),
        )
        .unwrap();
        cancellation.cancel();
        assert!(
            cancelled_reader
                .next_task(0)
                .err()
                .unwrap()
                .message
                .contains("cancelled")
        );
        drop(cancelled_reader);
        assert_eq!(
            memory.snapshot().current_bytes,
            0,
            "every failed/cancelled decode must release encoded and parse leases"
        );
    }

    #[test]
    fn typed_reader_parse_policy_rejects_overflow() {
        let policy = ExternalTaskParseMemory::fail_fast(
            "synthetic-overflow",
            MemoryClass::Discovery,
            u32::MAX,
            u64::MAX,
        )
        .unwrap();
        assert!(
            policy
                .reservation_bytes(u64::MAX)
                .unwrap_err()
                .message
                .contains("u64")
        );
    }

    #[test]
    fn typed_reader_fail_fast_pressure_and_cancellation_never_wait() {
        let root = TempDir::new().unwrap();
        let store = store(&root);
        let (writer_memory, spill) = authorities(64 * 1024, 1024 * 1024);
        let task = SyntheticTask {
            partition: 0,
            path: "file:///pressure.parquet".to_owned(),
        };
        let mut writer = store
            .writer("synthetic-v1", limits(), Arc::clone(&writer_memory), &spill)
            .unwrap();
        push_task(&mut writer, 0, &task).unwrap();
        let artifact = writer.finalize(encode_authority).unwrap();

        let authority_bytes = u64::try_from(br#"{"version":1}"#.len()).unwrap();
        let task_bytes = u64::try_from(serde_json::to_vec(&task).unwrap().len()).unwrap();
        let constrained_bytes = authority_bytes
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(task_bytes))
            .unwrap();
        let constrained: Arc<dyn MemoryCoordinator> = Arc::new(
            DeterministicMemoryCoordinator::new(constrained_bytes, BTreeMap::new()).unwrap(),
        );
        let fail_fast_config = || {
            TypedExternalTaskSetReaderConfig::new(
                "synthetic-v1",
                4096,
                4096,
                ExternalTaskParseMemory::fail_fast(
                    "synthetic-authority-parse",
                    MemoryClass::Discovery,
                    10_000,
                    0,
                )
                .unwrap(),
                ExternalTaskParseMemory::fail_fast(
                    "synthetic-task-parse",
                    MemoryClass::Discovery,
                    10_000,
                    0,
                )
                .unwrap(),
            )
            .unwrap()
        };

        let mut pressure_reader = TypedExternalTaskSetReader::open(
            &store,
            artifact.reference.clone(),
            Arc::clone(&constrained),
            RunCancellation::default(),
            fail_fast_config(),
            SyntheticCodec::default(),
        )
        .unwrap();
        let error = pressure_reader.next_task(0).err().unwrap();
        assert!(error.message.contains("cannot admit"));
        drop(pressure_reader);
        assert_eq!(constrained.snapshot().current_bytes, 0);

        let cancellation = RunCancellation::default();
        let mut cancelled_reader = TypedExternalTaskSetReader::open(
            &store,
            artifact.reference,
            Arc::clone(&constrained),
            cancellation.clone(),
            fail_fast_config(),
            SyntheticCodec::default(),
        )
        .unwrap();
        cancellation.cancel();
        let error = cancelled_reader.next_task(0).err().unwrap();
        assert!(error.message.contains("cancelled"));
        drop(cancelled_reader);
        assert_eq!(constrained.snapshot().current_bytes, 0);
    }

    #[test]
    fn typed_ordered_and_spill_sorted_builders_publish_one_canonical_identity() {
        let ordered_root = TempDir::new().unwrap();
        let canonical_root = TempDir::new().unwrap();
        let authority = SyntheticAuthority { version: 1 };

        let ordered_store = store(&ordered_root);
        let (ordered_memory, ordered_spill) = authorities(64 * 1024, 1024 * 1024);
        let mut ordered = TypedExternalTaskSetBuilder::new(
            &ordered_store,
            "synthetic-v1",
            limits(),
            Arc::clone(&ordered_memory),
            &ordered_spill,
            RunCancellation::default(),
            SyntheticCodec::default(),
        )
        .unwrap();
        for path in ["s3://bucket/a", "s3://bucket/b"] {
            ordered
                .push(&mut SyntheticTask {
                    partition: u64::MAX,
                    path: path.to_owned(),
                })
                .unwrap();
        }
        let ordered_artifact = ordered.finalize(&authority).unwrap();

        let canonical_store = store(&canonical_root);
        let (canonical_memory, canonical_spill) = authorities(256 * 1024, 1024 * 1024);
        let canonical_spill: Arc<dyn SpillBudgetCoordinator> = Arc::new(canonical_spill);
        let mut canonical = TypedCanonicalTaskSetBuilder::new(
            &canonical_store,
            "synthetic-v1",
            canonical_limits(),
            Arc::clone(&canonical_memory),
            Arc::clone(&canonical_spill),
            RunCancellation::default(),
            SyntheticCodec::default(),
        )
        .unwrap();
        for path in ["s3://bucket/b", "s3://bucket/a"] {
            let task = SyntheticTask {
                partition: u64::MAX,
                path: path.to_owned(),
            };
            assert!(
                canonical
                    .push_idempotent_by(task, |task| task.path.as_bytes())
                    .unwrap()
            );
        }
        let canonical_artifact = canonical.finalize(&authority).unwrap();

        assert_eq!(ordered_artifact.reference, canonical_artifact.reference);
        for (store, reference, memory) in [
            (
                &ordered_store,
                ordered_artifact.reference,
                Arc::clone(&ordered_memory),
            ),
            (
                &canonical_store,
                canonical_artifact.reference,
                Arc::clone(&canonical_memory),
            ),
        ] {
            let mut reader = TypedExternalTaskSetReader::open(
                store,
                reference,
                Arc::clone(&memory),
                RunCancellation::default(),
                typed_config("synthetic-v1"),
                SyntheticCodec::default(),
            )
            .unwrap();
            for (ordinal, path) in ["s3://bucket/a", "s3://bucket/b"].into_iter().enumerate() {
                let task = reader
                    .next_task(u64::try_from(ordinal).unwrap())
                    .unwrap()
                    .unwrap();
                assert_eq!(task.task().path, path);
            }
            assert!(reader.next_task(2).unwrap().is_none());
        }
        assert_eq!(ordered_memory.snapshot().current_bytes, 0);
        assert_eq!(ordered_spill.snapshot().current_bytes, 0);
        assert_eq!(canonical_memory.snapshot().current_bytes, 0);
        assert_eq!(canonical_spill.snapshot().current_bytes, 0);
    }

    #[test]
    fn planning_workspace_and_builder_failures_release_every_authority() {
        let root = TempDir::new().unwrap();
        let store = store(&root);
        let (memory, spill) = authorities(64 * 1024, 64 * 1024);
        let spill: Arc<dyn SpillBudgetCoordinator> = Arc::new(spill);
        let workspace = store
            .accounted_workspace(
                "synthetic-index",
                ExternalTaskWorkspaceLimits::new(
                    "synthetic-index",
                    MemoryClass::Control,
                    8192,
                    8192,
                    8192,
                )
                .unwrap(),
                Arc::clone(&memory),
                Arc::clone(&spill),
            )
            .unwrap();
        let workspace_path = workspace.path().to_path_buf();
        assert_eq!(memory.snapshot().current_bytes, 8192);
        assert_eq!(spill.snapshot().current_bytes, 8192);
        drop(workspace);
        assert_eq!(memory.snapshot().current_bytes, 0);
        assert_eq!(spill.snapshot().current_bytes, 0);
        assert!(!workspace_path.exists());

        let constrained_spill: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(FixedSpillBudget::new(4096).unwrap());
        assert!(
            store
                .accounted_workspace(
                    "constrained-index",
                    ExternalTaskWorkspaceLimits::new(
                        "constrained-index",
                        MemoryClass::Control,
                        8192,
                        8192,
                        8192,
                    )
                    .unwrap(),
                    Arc::clone(&memory),
                    Arc::clone(&constrained_spill),
                )
                .err()
                .unwrap()
                .message
                .contains("free spill")
        );
        assert_eq!(memory.snapshot().current_bytes, 0);
        assert_eq!(constrained_spill.snapshot().current_bytes, 0);

        let cancellation = RunCancellation::default();
        let mut cancelled = TypedExternalTaskSetBuilder::new(
            &store,
            "synthetic-v1",
            limits(),
            Arc::clone(&memory),
            spill.as_ref(),
            cancellation.clone(),
            SyntheticCodec::default(),
        )
        .unwrap();
        cancellation.cancel();
        assert!(
            cancelled
                .push(&mut SyntheticTask {
                    partition: 0,
                    path: "s3://bucket/cancelled".to_owned(),
                })
                .err()
                .unwrap()
                .message
                .contains("cancelled")
        );
        drop(cancelled);
        assert_eq!(memory.snapshot().current_bytes, 0);
        assert_eq!(spill.snapshot().current_bytes, 0);

        let mut mismatched = TypedExternalTaskSetBuilder::new(
            &store,
            "synthetic-v1",
            limits(),
            Arc::clone(&memory),
            spill.as_ref(),
            RunCancellation::default(),
            SyntheticCodec {
                authority_hash_override: Some(format!("sha256:{}", "00".repeat(32))),
                task_hash_override: None,
            },
        )
        .unwrap();
        mismatched
            .push(&mut SyntheticTask {
                partition: 0,
                path: "s3://bucket/mismatch".to_owned(),
            })
            .unwrap();
        assert!(
            mismatched
                .finalize(&SyntheticAuthority { version: 1 })
                .err()
                .unwrap()
                .message
                .contains("typed content identity")
        );
        assert_eq!(memory.snapshot().current_bytes, 0);
        assert_eq!(spill.snapshot().current_bytes, 0);
        let task_directory = root.path().join("planner-artifacts/task-sets");
        assert_eq!(fs::read_dir(task_directory).unwrap().count(), 0);
    }

    #[test]
    fn malformed_canonical_record_and_empty_inventory_are_fail_closed_and_deterministic() {
        let root = TempDir::new().unwrap();
        let store = store(&root);
        let (memory, spill) = authorities(256 * 1024, 1024 * 1024);
        let spill: Arc<dyn SpillBudgetCoordinator> = Arc::new(spill);
        let mut malformed = TypedCanonicalTaskSetBuilder::new(
            &store,
            "synthetic-v1",
            canonical_limits(),
            Arc::clone(&memory),
            Arc::clone(&spill),
            RunCancellation::default(),
            RejectDecodedTaskCodec,
        )
        .unwrap();
        malformed
            .push_idempotent_by(
                SyntheticTask {
                    partition: 0,
                    path: "s3://bucket/malformed".to_owned(),
                },
                |_| b"malformed",
            )
            .unwrap();
        assert!(
            malformed
                .finalize(&SyntheticAuthority { version: 1 })
                .err()
                .unwrap()
                .message
                .contains("malformed planning record")
        );
        assert_eq!(memory.snapshot().current_bytes, 0);
        assert_eq!(spill.snapshot().current_bytes, 0);

        let empty = TypedCanonicalTaskSetBuilder::new(
            &store,
            "synthetic-v1",
            canonical_limits(),
            Arc::clone(&memory),
            Arc::clone(&spill),
            RunCancellation::default(),
            SyntheticCodec::default(),
        )
        .unwrap()
        .finalize(&SyntheticAuthority { version: 1 })
        .unwrap();
        assert_eq!(empty.task_count, 0);
        assert_eq!(empty.reference.task_count, 0);
        assert_eq!(memory.snapshot().current_bytes, 0);
        assert_eq!(spill.snapshot().current_bytes, 0);
    }

    #[test]
    fn canonical_builder_admits_the_complete_finalize_overlap_once() {
        let root = TempDir::new().unwrap();
        let store = store(&root);
        let limits = canonical_limits();
        let scratch_bytes = limits
            .index_cache_bytes
            .checked_add(limits.tasks.maximum_task_bytes * 2)
            .and_then(|bytes| bytes.checked_add(limits.maximum_sort_key_bytes * 2))
            .unwrap();
        let (_, writer_bytes) =
            task_writer_memory_requirements("synthetic-v1", &limits.tasks).unwrap();
        let combined_bytes = scratch_bytes + writer_bytes;

        let constrained: Arc<dyn MemoryCoordinator> = Arc::new(
            DeterministicMemoryCoordinator::new(combined_bytes - 1, BTreeMap::new()).unwrap(),
        );
        let constrained_spill: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(FixedSpillBudget::new(1024 * 1024).unwrap());
        assert!(
            TypedCanonicalTaskSetBuilder::new(
                &store,
                "synthetic-v1",
                limits.clone(),
                Arc::clone(&constrained),
                Arc::clone(&constrained_spill),
                RunCancellation::default(),
                SyntheticCodec::default(),
            )
            .err()
            .unwrap()
            .message
            .contains("exceeds managed budget")
        );
        assert_eq!(constrained.snapshot().current_bytes, 0);
        assert_eq!(constrained_spill.snapshot().current_bytes, 0);

        let admitted: Arc<dyn MemoryCoordinator> =
            Arc::new(DeterministicMemoryCoordinator::new(combined_bytes, BTreeMap::new()).unwrap());
        let admitted_spill: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(FixedSpillBudget::new(1024 * 1024).unwrap());
        let mut builder = TypedCanonicalTaskSetBuilder::new(
            &store,
            "synthetic-v1",
            limits,
            Arc::clone(&admitted),
            Arc::clone(&admitted_spill),
            RunCancellation::default(),
            SyntheticCodec::default(),
        )
        .unwrap();
        builder
            .push_idempotent_by(
                SyntheticTask {
                    partition: 0,
                    path: "s3://bucket/admitted".to_owned(),
                },
                |task| task.path.as_bytes(),
            )
            .unwrap();
        builder
            .finalize(&SyntheticAuthority { version: 1 })
            .unwrap();
        assert_eq!(admitted.snapshot().peak_bytes, combined_bytes);
        assert_eq!(admitted.snapshot().current_bytes, 0);
        assert_eq!(admitted_spill.snapshot().current_bytes, 0);
    }

    #[test]
    fn canonical_builder_configured_spill_exhaustion_is_data_and_discards_scratch() {
        let root = TempDir::new().unwrap();
        let store = store(&root);
        let (memory, spill) = authorities(256 * 1024, 256 * 1024);
        let spill: Arc<dyn SpillBudgetCoordinator> = Arc::new(spill);
        let mut builder = store
            .canonical_builder(
                "synthetic-v1",
                canonical_limits(),
                Arc::clone(&memory),
                Arc::clone(&spill),
            )
            .unwrap();
        let first = SyntheticTask {
            partition: 0,
            path: format!("s3://bucket/{:08}/{}", 0, "x".repeat(1000)),
        };
        assert!(
            builder
                .push_idempotent_with(first.path.as_bytes(), |output| {
                    serde_json::to_writer(output, &first)
                        .map_err(|error| CdfError::data(format!("encode synthetic task: {error}")))
                })
                .unwrap()
        );
        let error = (1_u64..10_000)
            .find_map(|ordinal| {
                let task = SyntheticTask {
                    partition: ordinal,
                    path: format!("s3://bucket/{ordinal:08}/{}", "x".repeat(1000)),
                };
                builder
                    .push_idempotent_with(task.path.as_bytes(), |output| {
                        serde_json::to_writer(output, &task).map_err(|error| {
                            CdfError::data(format!("encode synthetic task: {error}"))
                        })
                    })
                    .err()
            })
            .expect("bounded spill must terminate canonical insertion");
        assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
        assert!(error.message.contains("spill"));
        assert!(
            !builder
                .push_idempotent_with(first.path.as_bytes(), |output| {
                    serde_json::to_writer(output, &first)
                        .map_err(|error| CdfError::data(format!("encode synthetic task: {error}")))
                })
                .unwrap(),
            "an exact duplicate must not need fresh spill admission"
        );
        let conflicting = SyntheticTask {
            partition: u64::MAX,
            path: first.path.clone(),
        };
        assert!(
            builder
                .push_idempotent_with(conflicting.path.as_bytes(), |output| {
                    serde_json::to_writer(output, &conflicting)
                        .map_err(|error| CdfError::data(format!("encode synthetic task: {error}")))
                })
                .unwrap_err()
                .message
                .contains("conflicting payloads"),
            "conflicting duplicate detection must not need fresh spill admission"
        );
        drop(builder);
        assert_eq!(memory.snapshot().current_bytes, 0);
        assert_eq!(spill.snapshot().current_bytes, 0);
        let scratch = root.path().join("planner-artifacts/scratch");
        assert_eq!(fs::read_dir(scratch).unwrap().count(), 0);
    }

    #[test]
    fn canonical_builder_admits_multi_page_insert_before_sqlite_mutation() {
        let root = TempDir::new().unwrap();
        let store = store(&root);
        let (memory, spill) = authorities(512 * 1024, 1024 * 1024);
        let spill: Arc<dyn SpillBudgetCoordinator> = Arc::new(spill);
        let limits = CanonicalTaskSetLimits {
            tasks: TaskSetLimits {
                maximum_task_bytes: 64 * 1024,
                maximum_authority_bytes: 4096,
                writer_buffer_bytes: 8192,
            },
            ..canonical_limits()
        };
        let mut builder = store
            .canonical_builder(
                "multi-page-v1",
                limits,
                Arc::clone(&memory),
                Arc::clone(&spill),
            )
            .unwrap();
        let initial_reservation = builder.spill_reservation.bytes();
        assert_eq!(initial_reservation, 16 * 1024);
        let payload = vec![7_u8; 50 * 1024];

        builder
            .push_with(b"multi-page", |output| {
                output
                    .write_all(&payload)
                    .map_err(|error| CdfError::data(format!("encode multi-page task: {error}")))
            })
            .unwrap();

        assert!(builder.spill_reservation.bytes() > initial_reservation);
        assert_eq!(builder.task_count(), 1);
    }

    #[test]
    fn canonical_builder_two_page_minimum_accepts_a_tiny_insert() {
        let root = TempDir::new().unwrap();
        let store = store(&root);
        let (memory, spill) = authorities(128 * 1024, SQLITE_PAGE_BYTES * 2);
        let spill: Arc<dyn SpillBudgetCoordinator> = Arc::new(spill);
        let limits = CanonicalTaskSetLimits {
            spill_growth_bytes: SQLITE_PAGE_BYTES * 2,
            minimum_initial_spill_bytes: SQLITE_PAGE_BYTES * 2,
            ..canonical_limits()
        };
        let mut builder = store
            .canonical_builder("tiny-v1", limits, Arc::clone(&memory), Arc::clone(&spill))
            .unwrap();

        builder
            .push_with(b"k", |output| {
                output
                    .write_all(b"x")
                    .map_err(|error| CdfError::data(format!("encode tiny task: {error}")))
            })
            .unwrap();

        assert_eq!(builder.spill_reservation.bytes(), SQLITE_PAGE_BYTES * 2);
        assert_eq!(builder.task_count(), 1);

        let oversized_local = vec![7_u8; 1200];
        let error = builder
            .push_with(b"overflow", |output| {
                output
                    .write_all(&oversized_local)
                    .map_err(|error| CdfError::data(format!("encode overflow task: {error}")))
            })
            .unwrap_err();
        assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
        assert!(error.message.contains("spill budget"));
        assert_eq!(builder.task_count(), 1);
    }

    #[test]
    fn cancellation_during_empty_finalization_prevents_atomic_install() {
        let root = TempDir::new().unwrap();
        let store = store(&root);
        let (memory, spill) = authorities(64 * 1024, 1024 * 1024);
        let cancellation = RunCancellation::default();
        let writer = store
            .writer("synthetic-v1", limits(), Arc::clone(&memory), &spill)
            .unwrap();
        let error = writer
            .finalize_with_authority_hash_and_cancellation(
                writer_authority_hash(),
                &cancellation,
                |output| {
                    encode_authority(output)?;
                    cancellation.cancel();
                    Ok(())
                },
            )
            .unwrap_err();
        assert!(error.message.contains("cancelled"));
        assert_eq!(memory.snapshot().current_bytes, 0);
        assert_eq!(spill.snapshot().current_bytes, 0);
        let task_directory = root.path().join("planner-artifacts/task-sets");
        assert_eq!(fs::read_dir(task_directory).unwrap().count(), 0);
    }

    #[test]
    fn provider_order_is_externalized_into_one_canonical_identity() {
        let first_root = TempDir::new().unwrap();
        let second_root = TempDir::new().unwrap();
        let mut references = Vec::new();
        for (root, order) in [
            (&first_root, vec![9_u64, 1, 7, 0, 8, 2, 6, 3, 5, 4]),
            (&second_root, (0_u64..10).collect()),
        ] {
            let store = store(root);
            let (memory, spill) = authorities(256 * 1024, 4 * 1024 * 1024);
            let mut builder = store
                .canonical_builder(
                    "synthetic-v1",
                    canonical_limits(),
                    Arc::clone(&memory),
                    Arc::new(spill),
                )
                .unwrap();
            for partition in order {
                let task = SyntheticTask {
                    partition,
                    path: format!("s3://bucket/{partition:08}.parquet"),
                };
                builder
                    .push_with(task.path.as_bytes(), |output| {
                        serde_json::to_writer(output, &task).map_err(|error| {
                            CdfError::data(format!("encode synthetic task: {error}"))
                        })
                    })
                    .unwrap();
            }
            let artifact = builder.finalize(encode_authority).unwrap();
            let mut reader = store
                .reader(
                    artifact.reference.clone(),
                    "synthetic-v1",
                    4096,
                    4096,
                    Arc::clone(&memory),
                )
                .unwrap();
            let mut expected = 0_u64;
            while let Some(record) = reader.next_record().unwrap() {
                let task: SyntheticTask = serde_json::from_slice(record.payload.payload()).unwrap();
                assert_eq!(task.partition, expected);
                expected += 1;
            }
            assert_eq!(expected, 10);
            references.push(artifact.reference);
        }
        assert_eq!(references[0], references[1]);
    }

    #[test]
    fn canonical_builder_rejects_duplicate_keys_and_releases_authorities() {
        let root = TempDir::new().unwrap();
        let store = store(&root);
        let (memory, spill) = authorities(256 * 1024, 4 * 1024 * 1024);
        let spill: Arc<dyn SpillBudgetCoordinator> = Arc::new(spill);
        let mut builder = store
            .canonical_builder(
                "synthetic-v1",
                canonical_limits(),
                Arc::clone(&memory),
                Arc::clone(&spill),
            )
            .unwrap();
        let task = SyntheticTask {
            partition: 0,
            path: "s3://bucket/same.parquet".to_owned(),
        };
        for expected_ok in [true, false] {
            let result = builder.push_with(task.path.as_bytes(), |output| {
                serde_json::to_writer(output, &task)
                    .map_err(|error| CdfError::data(format!("encode synthetic task: {error}")))
            });
            assert_eq!(result.is_ok(), expected_ok);
        }
        drop(builder);
        assert_eq!(memory.snapshot().current_bytes, 0);
        assert_eq!(spill.snapshot().current_bytes, 0);
    }

    #[test]
    fn idempotent_provider_input_collapses_only_identical_observations() {
        let root = TempDir::new().unwrap();
        let store = store(&root);
        let (memory, spill) = authorities(256 * 1024, 4 * 1024 * 1024);
        let mut builder = store
            .canonical_builder(
                "synthetic-v1",
                canonical_limits(),
                Arc::clone(&memory),
                Arc::new(spill),
            )
            .unwrap();
        let task = SyntheticTask {
            partition: 0,
            path: "s3://bucket/same.parquet".to_owned(),
        };
        assert!(
            builder
                .push_idempotent_with(task.path.as_bytes(), |output| {
                    serde_json::to_writer(output, &task)
                        .map_err(|error| CdfError::data(format!("encode synthetic task: {error}")))
                })
                .unwrap()
        );
        assert!(
            !builder
                .push_idempotent_with(task.path.as_bytes(), |output| {
                    serde_json::to_writer(output, &task)
                        .map_err(|error| CdfError::data(format!("encode synthetic task: {error}")))
                })
                .unwrap()
        );
        let conflicting = SyntheticTask {
            partition: 1,
            path: task.path.clone(),
        };
        let error = builder
            .push_idempotent_with(conflicting.path.as_bytes(), |output| {
                serde_json::to_writer(output, &conflicting)
                    .map_err(|error| CdfError::data(format!("encode synthetic task: {error}")))
            })
            .unwrap_err();
        assert!(error.message.contains("conflicting payloads"));
        assert_eq!(builder.task_count(), 1);
    }

    #[test]
    fn jobs_timing_and_store_location_do_not_change_identity() {
        let first_root = TempDir::new().unwrap();
        let second_root = TempDir::new().unwrap();
        let mut references = Vec::new();
        for root in [&first_root, &second_root] {
            let store = store(root);
            let (memory, spill) = authorities(64 * 1024, 1024 * 1024);
            let mut writer = store
                .writer("synthetic-v1", limits(), memory, &spill)
                .unwrap();
            for ordinal in 0..32 {
                push_task(
                    &mut writer,
                    ordinal,
                    &SyntheticTask {
                        partition: ordinal,
                        path: format!("s3://bucket/{ordinal:08}.parquet"),
                    },
                )
                .unwrap();
            }
            references.push(writer.finalize(encode_authority).unwrap().reference);
        }
        assert_eq!(references[0], references[1]);
    }

    #[test]
    fn tamper_and_noncanonical_order_fail_closed() {
        let root = TempDir::new().unwrap();
        let store = store(&root);
        let (memory, spill) = authorities(64 * 1024, 1024 * 1024);
        let mut writer = store
            .writer("synthetic-v1", limits(), Arc::clone(&memory), &spill)
            .unwrap();
        let task = SyntheticTask {
            partition: 0,
            path: "file:///zero.parquet".to_owned(),
        };
        assert!(
            push_task(&mut writer, 1, &task)
                .unwrap_err()
                .message
                .contains("out of order")
        );
        push_task(&mut writer, 0, &task).unwrap();
        let artifact = writer.finalize(encode_authority).unwrap();

        let mut bytes = fs::read(&artifact.path).unwrap();
        let payload_offset = bytes
            .windows(b"file:///zero.parquet".len())
            .position(|window| window == b"file:///zero.parquet")
            .unwrap();
        bytes[payload_offset] ^= 1;
        fs::write(&artifact.path, bytes).unwrap();
        let mut reader = store
            .reader(
                artifact.reference,
                "synthetic-v1",
                4096,
                4096,
                Arc::clone(&memory),
            )
            .unwrap();
        let error = loop {
            match reader.next_record() {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("tampered task set passed verification"),
                Err(error) => break error,
            }
        };
        assert!(
            error.message.contains("content identity")
                || error.message.contains("changed")
                || error.message.contains("footer")
        );
    }

    #[test]
    fn configured_task_and_spill_budgets_fail_cleanly() {
        let root = TempDir::new().unwrap();
        let store = store(&root);
        let (memory, spill) = authorities(64 * 1024, 96);
        let mut writer = store
            .writer("synthetic-v1", limits(), memory, &spill)
            .unwrap();
        let oversized = SyntheticTask {
            partition: 0,
            path: "x".repeat(5000),
        };
        assert!(
            push_task(&mut writer, 0, &oversized)
                .unwrap_err()
                .message
                .contains("configured")
        );

        let small = SyntheticTask {
            partition: 0,
            path: "file:///zero.parquet".to_owned(),
        };
        let error = push_task(&mut writer, 0, &small).unwrap_err();
        assert!(error.message.contains("disk budget"));
    }

    #[test]
    #[ignore = "slow million-task constant-memory conformance"]
    fn million_tasks_hold_the_configured_metadata_budget() {
        let root = TempDir::new().unwrap();
        let store = store(&root);
        let (memory, spill) = authorities(16 * 1024 * 1024, 512 * 1024 * 1024);
        let spill = Arc::new(spill);
        let production_limits = CanonicalTaskSetLimits {
            tasks: limits(),
            maximum_sort_key_bytes: 64 * 1024,
            index_cache_bytes: 8 * 1024 * 1024,
            spill_growth_bytes: 64 * 1024 * 1024,
            minimum_initial_spill_bytes: SQLITE_PAGE_BYTES * 2,
        };
        let mut builder = store
            .canonical_builder(
                "million-v1",
                production_limits.clone(),
                Arc::clone(&memory),
                spill.clone(),
            )
            .unwrap();
        for partition in (0..1_000_000).rev() {
            let task = SyntheticTask {
                partition,
                path: format!("s3://b/{partition:08}"),
            };
            builder
                .push_with(task.path.as_bytes(), |output| {
                    serde_json::to_writer(output, &task)
                        .map_err(|error| CdfError::data(format!("encode synthetic task: {error}")))
                })
                .unwrap();
        }
        let artifact = builder.finalize(encode_authority).unwrap();
        assert_eq!(artifact.task_count, 1_000_000);
        let mut reader = store
            .reader(
                artifact.reference,
                "million-v1",
                production_limits.tasks.maximum_task_bytes,
                production_limits.tasks.maximum_authority_bytes,
                Arc::clone(&memory),
            )
            .unwrap();
        let mut expected = 0_u64;
        while let Some(record) = reader.next_record().unwrap() {
            let task: SyntheticTask = serde_json::from_slice(record.payload.payload()).unwrap();
            assert_eq!(record.canonical_ordinal, expected);
            assert_eq!(task.partition, expected);
            expected += 1;
        }
        assert_eq!(expected, 1_000_000);
        assert!(memory.snapshot().peak_bytes <= 16 * 1024 * 1024);
        assert!(spill.snapshot().peak_bytes <= 512 * 1024 * 1024);
    }

    fn writer_authority_hash() -> &'static str {
        "sha256:2430f1a2ad2982d0067885488a4c89e21ad1d7c83b115ba8f1b20acc88dfaea8"
    }
}
