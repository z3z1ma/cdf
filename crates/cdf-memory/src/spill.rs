use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use cdf_kernel::{CdfError, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpillBudgetSnapshot {
    pub budget_bytes: u64,
    pub current_bytes: u64,
    pub peak_bytes: u64,
    pub reservation_failures: u64,
}

/// Process-wide disk-spill admission authority shared by every runtime-neutral buffer owner.
pub trait SpillBudgetCoordinator: Send + Sync {
    fn try_reserve(&self, bytes: u64) -> Result<Option<SpillReservation>>;
    fn snapshot(&self) -> SpillBudgetSnapshot;
}

pub struct SpillReservation {
    bytes: u64,
    account: Arc<SpillBudgetAccount>,
}

impl SpillReservation {
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub fn try_grow(&mut self, additional: u64) -> Result<bool> {
        if additional == 0 {
            return Ok(true);
        }
        if !self.account.try_acquire(additional)? {
            return Ok(false);
        }
        self.bytes = self
            .bytes
            .checked_add(additional)
            .ok_or_else(|| CdfError::data("spill reservation overflowed u64"))?;
        Ok(true)
    }

    pub fn shrink(&mut self, bytes: u64) {
        let released = bytes.min(self.bytes);
        if released > 0 {
            self.bytes -= released;
            self.account.current.fetch_sub(released, Ordering::AcqRel);
        }
    }
}

impl Drop for SpillReservation {
    fn drop(&mut self) {
        self.account.current.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

#[derive(Debug)]
pub struct FixedSpillBudget {
    account: Arc<SpillBudgetAccount>,
}

#[derive(Debug)]
struct SpillBudgetAccount {
    budget: u64,
    current: AtomicU64,
    peak: AtomicU64,
    failures: AtomicU64,
}

impl FixedSpillBudget {
    pub fn new(budget_bytes: u64) -> Result<Self> {
        if budget_bytes == 0 {
            return Err(CdfError::contract("spill budget must be nonzero"));
        }
        Ok(Self {
            account: Arc::new(SpillBudgetAccount {
                budget: budget_bytes,
                current: AtomicU64::new(0),
                peak: AtomicU64::new(0),
                failures: AtomicU64::new(0),
            }),
        })
    }
}

impl SpillBudgetCoordinator for FixedSpillBudget {
    fn try_reserve(&self, bytes: u64) -> Result<Option<SpillReservation>> {
        if bytes == 0 {
            return Err(CdfError::contract(
                "spill reservations must request nonzero bytes",
            ));
        }
        if !self.account.try_acquire(bytes)? {
            return Ok(None);
        }
        Ok(Some(SpillReservation {
            bytes,
            account: Arc::clone(&self.account),
        }))
    }

    fn snapshot(&self) -> SpillBudgetSnapshot {
        SpillBudgetSnapshot {
            budget_bytes: self.account.budget,
            current_bytes: self.account.current.load(Ordering::Acquire),
            peak_bytes: self.account.peak.load(Ordering::Acquire),
            reservation_failures: self.account.failures.load(Ordering::Acquire),
        }
    }
}

impl SpillBudgetAccount {
    fn try_acquire(&self, bytes: u64) -> Result<bool> {
        let mut current = self.current.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_add(bytes) else {
                return Err(CdfError::data("spill budget accounting overflowed u64"));
            };
            if next > self.budget {
                self.failures.fetch_add(1, Ordering::AcqRel);
                return Ok(false);
            }
            match self.current.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.peak.fetch_max(next, Ordering::AcqRel);
                    return Ok(true);
                }
                Err(observed) => current = observed,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservations_enforce_shared_budget_and_release_on_drop() {
        let budget = FixedSpillBudget::new(10).unwrap();
        let mut first = budget.try_reserve(6).unwrap().unwrap();
        assert!(budget.try_reserve(5).unwrap().is_none());
        assert!(first.try_grow(4).unwrap());
        assert!(!first.try_grow(1).unwrap());
        assert_eq!(budget.snapshot().peak_bytes, 10);
        drop(first);
        assert_eq!(budget.snapshot().current_bytes, 0);
        assert!(budget.try_reserve(10).unwrap().is_some());
    }
}
