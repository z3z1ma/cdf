use serde::{Deserialize, Serialize};

use crate::{
    Checkpoint, CheckpointId, CheckpointStore, LeaseAuthorityDomainId, LeaseOwnerId, PromotionId,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpiredScopeLeaseProof {
    pub expired_lease: ScopeLease,
    pub cleanup_lease: ScopeLease,
    pub proven_at_ms: i64,
}

impl ScopeLease {
    pub fn is_expired_at(&self, now_ms: i64) -> bool {
        self.expires_at_ms <= now_ms
    }
}

pub trait ScopeLeaseStore: Send + Sync {
    /// Stable identity of the consistency domain issuing fencing generations.
    ///
    /// Tokens from different domains are never comparable. Implementations backed by durable
    /// state must persist this identity so every process opening the same store observes the same
    /// domain.
    fn authority_domain_id(&self) -> LeaseAuthorityDomainId;

    fn acquire(
        &self,
        scope: ScopeKey,
        owner: LeaseOwnerId,
        lease_duration_ms: u64,
    ) -> Result<ScopeLease>;

    fn renew(&self, lease: &ScopeLease, lease_duration_ms: u64) -> Result<ScopeLease>;

    fn release(&self, lease: &ScopeLease) -> Result<()>;

    fn assert_current(&self, lease: &ScopeLease) -> Result<()>;

    /// Proves that a recorded lease generation can no longer be live and atomically claims cleanup.
    ///
    /// `None` means the generation itself, or a newer generation for the same scope, remains
    /// active. Implementations must retain monotonically increasing fencing state after release so
    /// a collector never substitutes object age or process-local state for this proof. A successful
    /// call must install `cleanup_lease` as the active next fencing generation in the same atomic
    /// operation; the collector renews it until deletion completes and then releases it.
    fn prove_expired(
        &self,
        lease: &ScopeLease,
        collector: LeaseOwnerId,
        cleanup_lease_duration_ms: u64,
    ) -> Result<Option<ExpiredScopeLeaseProof>>;
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
