//! Content-store layout and invocation-local workspace ownership.

use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use cdf_kernel::{CdfError, ContentStoreNamespace, PlannedTaskSetReference, Result};
use cdf_memory::{
    ConsumerKey, MemoryCoordinator, MemoryLease, ReservationRequest, reserve_blocking,
};
use cdf_runtime::{SpillBudgetCoordinator, SpillReservation};

use crate::limits::{ExternalTaskWorkspaceLimits, require_token};
use crate::publication::io_error;

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

    pub(crate) fn namespace(&self) -> &ContentStoreNamespace {
        &self.namespace
    }

    pub(crate) fn task_set_directory(&self) -> PathBuf {
        self.root.join(self.namespace.as_str()).join("task-sets")
    }

    pub(crate) fn object_path(&self, object_key: &str) -> PathBuf {
        self.root.join(self.namespace.as_str()).join(object_key)
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
        let (consumer, memory_class, resident_bytes, spill_growth_bytes, minimum_spill_bytes) =
            limits.into_accounting_parts();
        let consumer_key = ConsumerKey::new(&consumer, memory_class)?;
        let memory_lease = reserve_blocking(
            memory,
            &ReservationRequest::new(consumer_key, resident_bytes)?,
        )?;
        let available = available_spill_bytes(spill.as_ref());
        let initial = spill_growth_bytes.min(available);
        if initial < minimum_spill_bytes {
            return Err(CdfError::data(format!(
                "{} requires at least {} free spill bytes but only {available} are available",
                consumer, minimum_spill_bytes
            )));
        }
        let spill_reservation = spill.try_reserve(initial)?.ok_or_else(|| {
            CdfError::data(format!(
                "{} could not acquire its initial shared spill reservation",
                consumer
            ))
        })?;
        let workspace = self.temporary_workspace(label)?;
        Ok(AccountedExternalTaskWorkspace {
            workspace,
            spill,
            spill_reservation,
            spill_growth_bytes,
            minimum_spill_bytes,
            consumer,
            _memory_lease: memory_lease,
        })
    }

    pub(crate) fn path_for_reference(
        &self,
        reference: &PlannedTaskSetReference,
    ) -> Result<PathBuf> {
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
        Ok(self.object_path(reference.object_key.as_str()))
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

pub(crate) fn available_spill_bytes(spill: &dyn SpillBudgetCoordinator) -> u64 {
    let snapshot = spill.snapshot();
    snapshot.budget_bytes.saturating_sub(snapshot.current_bytes)
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
