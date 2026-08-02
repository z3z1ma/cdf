//! Resource limits and shared task-store models.

use cdf_kernel::{CdfError, Result};
use cdf_memory::MemoryClass;

use crate::sqlite_capacity::SQLITE_PAGE_BYTES;

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

    pub(crate) fn into_accounting_parts(self) -> (String, MemoryClass, u64, u64, u64) {
        (
            self.consumer,
            self.memory_class,
            self.resident_bytes,
            self.spill_growth_bytes,
            self.minimum_spill_bytes,
        )
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

pub(crate) fn task_writer_memory_requirements(
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

pub(crate) fn require_token(label: &str, value: &str) -> Result<()> {
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
