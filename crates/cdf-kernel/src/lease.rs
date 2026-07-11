use serde::{Deserialize, Serialize};

use crate::{
    Checkpoint, CheckpointId, CheckpointStore, LeaseOwnerId, PromotionId,
    PromotionPublicationEvent, Receipt, Result, ScopeKey,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FencingToken(u64);

impl FencingToken {
    pub fn new(value: u64) -> Result<Self> {
        if value == 0 {
            return Err(crate::CdfError::contract("fencing token must be positive"));
        }
        Ok(Self(value))
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeLease {
    pub scope: ScopeKey,
    pub owner: LeaseOwnerId,
    pub fencing_token: FencingToken,
    pub acquired_at_ms: i64,
    pub expires_at_ms: i64,
}

impl ScopeLease {
    pub fn is_expired_at(&self, now_ms: i64) -> bool {
        self.expires_at_ms <= now_ms
    }
}

pub trait ScopeLeaseStore: Send + Sync {
    fn acquire(
        &self,
        scope: ScopeKey,
        owner: LeaseOwnerId,
        lease_duration_ms: u64,
    ) -> Result<ScopeLease>;

    fn renew(&self, lease: &ScopeLease, lease_duration_ms: u64) -> Result<ScopeLease>;

    fn release(&self, lease: &ScopeLease) -> Result<()>;

    fn assert_current(&self, lease: &ScopeLease) -> Result<()>;
}

pub trait ScopeLeaseClock: Send + Sync {
    fn now_ms(&self) -> Result<i64>;
}

/// Atomically advances promotion state under the same authoritative scope lease.
///
/// Implementations must check the lease and perform each protected mutation in one
/// consistency-domain transaction. A caller-side `assert_current` is not sufficient.
pub trait PromotionSettlementStore: CheckpointStore + ScopeLeaseStore {
    fn promotion_publication(
        &self,
        promotion_id: &PromotionId,
    ) -> Result<Option<PromotionPublicationEvent>>;

    fn commit_promotion_checkpoint(
        &self,
        lease: &ScopeLease,
        checkpoint_id: &CheckpointId,
        receipt: Receipt,
    ) -> Result<Checkpoint>;

    fn publish_promotion(
        &self,
        lease: &ScopeLease,
        event: PromotionPublicationEvent,
    ) -> Result<PromotionPublicationEvent>;
}
