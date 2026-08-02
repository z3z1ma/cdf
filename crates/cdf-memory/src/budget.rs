use cdf_kernel::{CdfError, Result};
use serde::{Deserialize, Serialize};

pub const DEFAULT_PROCESS_BUDGET_BYTES: u64 = 4 * 1024 * 1024 * 1024;
pub const DEFAULT_SPILL_BUDGET_BYTES: u64 = 8 * 1024 * 1024 * 1024;
pub const MINIMUM_NATIVE_HEADROOM_BYTES: u64 = 512 * 1024 * 1024;
pub const NATIVE_HEADROOM_PERCENT: u64 = 15;
pub const HEADROOM_POLICY_VERSION: &str = "native-headroom-v1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryBudgetResolution {
    pub requested_process_bytes: Option<u64>,
    pub effective_authority_bytes: u64,
    pub process_budget_bytes: u64,
    pub native_headroom_bytes: u64,
    pub managed_pool_bytes: u64,
    pub spill_budget_bytes: u64,
    pub headroom_policy_version: String,
}

impl MemoryBudgetResolution {
    pub fn validate(&self) -> Result<()> {
        if self.effective_authority_bytes == 0
            || self.process_budget_bytes == 0
            || self.native_headroom_bytes == 0
            || self.managed_pool_bytes == 0
            || self.spill_budget_bytes == 0
            || self.headroom_policy_version.is_empty()
        {
            return Err(CdfError::contract(
                "memory budget resolution requires nonzero authority, process, native headroom, managed pool, spill, and policy version",
            ));
        }
        if self.process_budget_bytes > self.effective_authority_bytes
            || self.requested_process_bytes.is_some_and(|requested| {
                requested != self.process_budget_bytes || requested > self.effective_authority_bytes
            })
            || self
                .managed_pool_bytes
                .checked_add(self.native_headroom_bytes)
                != Some(self.process_budget_bytes)
        {
            return Err(CdfError::contract(
                "memory budget resolution is internally inconsistent",
            ));
        }
        Ok(())
    }
}

pub fn resolve_memory_budget(
    requested_process_bytes: Option<u64>,
    effective_authority_bytes: u64,
    minimum_working_set_bytes: u64,
    spill_budget_bytes: u64,
) -> Result<MemoryBudgetResolution> {
    resolve_memory_budget_inner(
        requested_process_bytes,
        effective_authority_bytes,
        true,
        minimum_working_set_bytes,
        spill_budget_bytes,
    )
}

pub fn resolve_unenforced_memory_budget(
    requested_process_bytes: Option<u64>,
    effective_policy_bytes: u64,
    minimum_working_set_bytes: u64,
    spill_budget_bytes: u64,
) -> Result<MemoryBudgetResolution> {
    resolve_memory_budget_inner(
        requested_process_bytes,
        effective_policy_bytes,
        false,
        minimum_working_set_bytes,
        spill_budget_bytes,
    )
}

fn resolve_memory_budget_inner(
    requested_process_bytes: Option<u64>,
    effective_authority_bytes: u64,
    reserve_external_authority_margin: bool,
    minimum_working_set_bytes: u64,
    spill_budget_bytes: u64,
) -> Result<MemoryBudgetResolution> {
    if effective_authority_bytes == 0 || minimum_working_set_bytes == 0 || spill_budget_bytes == 0 {
        return Err(CdfError::contract(
            "memory authority, minimum working set, and spill budget must be nonzero",
        ));
    }
    let authority_ceiling = if reserve_external_authority_margin {
        effective_authority_bytes.saturating_mul(80) / 100
    } else {
        effective_authority_bytes
    };
    let process_budget_bytes = match requested_process_bytes {
        Some(requested) if requested > effective_authority_bytes => {
            return Err(CdfError::contract(format!(
                "requested process memory budget {requested} exceeds effective authority {effective_authority_bytes}"
            )));
        }
        Some(requested) => requested,
        None => DEFAULT_PROCESS_BUDGET_BYTES.min(authority_ceiling),
    };
    let native_headroom_bytes = MINIMUM_NATIVE_HEADROOM_BYTES
        .max(process_budget_bytes.saturating_mul(NATIVE_HEADROOM_PERCENT) / 100);
    let managed_pool_bytes = process_budget_bytes
        .checked_sub(native_headroom_bytes)
        .filter(|managed| *managed >= minimum_working_set_bytes)
        .ok_or_else(|| {
            CdfError::data(format!(
                "process memory budget {process_budget_bytes} leaves less than the {minimum_working_set_bytes}-byte minimum working set after {native_headroom_bytes} bytes of native headroom; raise the budget or reduce the working set"
            ))
        })?;
    let resolution = MemoryBudgetResolution {
        requested_process_bytes,
        effective_authority_bytes,
        process_budget_bytes,
        native_headroom_bytes,
        managed_pool_bytes,
        spill_budget_bytes,
        headroom_policy_version: HEADROOM_POLICY_VERSION.to_owned(),
    };
    resolution.validate()?;
    Ok(resolution)
}
