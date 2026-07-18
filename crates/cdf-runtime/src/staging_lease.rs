use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use cdf_kernel::{
    CdfError, ContentClaimAttemptId, ContentPublicationClaim, ContentPublicationClaimId,
    ContentPublicationClaimState, DestinationId, ExpiredScopeLeaseProof, ImmutableContentIdentity,
    LeaseAuthorityDomainId, LeaseOwnerId, Result, ScopeKey, ScopeLease, ScopeLeaseStore,
    TargetName,
};
use serde::{Deserialize, Serialize};

use crate::LoadAttemptId;
use crate::{ExecutionHost, RunCancellation};

const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(120);
const DEFAULT_RENEW_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagingLeaseIdentity {
    pub destination_id: DestinationId,
    pub target: TargetName,
    pub attempt_id: LoadAttemptId,
}

impl StagingLeaseIdentity {
    pub fn new(
        destination_id: DestinationId,
        target: TargetName,
        attempt_id: LoadAttemptId,
    ) -> Self {
        Self {
            destination_id,
            target,
            attempt_id,
        }
    }

    fn scope(&self) -> ScopeKey {
        ScopeKey::Composite {
            parts: vec![
                ScopeKey::DestinationLoad {
                    destination: self.destination_id.clone(),
                    target: self.target.clone(),
                },
                ScopeKey::Stream {
                    name: format!("staging:{}", self.attempt_id),
                },
            ],
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagingLease {
    pub authority_domain_id: LeaseAuthorityDomainId,
    pub identity: StagingLeaseIdentity,
    pub scope_lease: ScopeLease,
}

impl StagingLease {
    pub fn fencing_token(&self) -> u64 {
        self.scope_lease.fencing_token.get()
    }

    pub fn authority_domain_id(&self) -> &LeaseAuthorityDomainId {
        &self.authority_domain_id
    }

    /// Whether two observations name the same fenced generation. Expiry is deliberately excluded:
    /// renewal extends it without creating a new owner/token generation.
    pub fn same_generation(&self, other: &Self) -> bool {
        self.authority_domain_id == other.authority_domain_id
            && self.identity == other.identity
            && self.scope_lease.scope == other.scope_lease.scope
            && self.scope_lease.owner == other.scope_lease.owner
            && self.scope_lease.fencing_token == other.scope_lease.fencing_token
            && self.scope_lease.acquired_at_ms == other.scope_lease.acquired_at_ms
    }

    pub fn content_publication_claim(
        &self,
        content: ImmutableContentIdentity,
        claim_id: ContentPublicationClaimId,
        claim_generation: u64,
        state: ContentPublicationClaimState,
    ) -> Result<ContentPublicationClaim> {
        self.validate()?;
        ContentPublicationClaim::new(
            self.identity.destination_id.clone(),
            self.identity.target.clone(),
            ContentClaimAttemptId::new(self.identity.attempt_id.as_str())?,
            self.authority_domain_id.clone(),
            self.scope_lease.fencing_token,
            content,
            claim_id,
            claim_generation,
            state,
        )
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.scope_lease.scope != self.identity.scope() {
            return Err(CdfError::contract(
                "staging lease identity does not match its fenced scope",
            ));
        }
        if self.scope_lease.expires_at_ms <= self.scope_lease.acquired_at_ms {
            return Err(CdfError::contract(
                "staging lease expiry must follow acquisition",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpiredStagingLeaseProof {
    expired_lease: StagingLease,
    cleanup_lease: StagingLease,
    proven_at_ms: i64,
}

impl ExpiredStagingLeaseProof {
    pub fn expired_lease(&self) -> &StagingLease {
        &self.expired_lease
    }

    pub fn cleanup_fencing_token(&self) -> u64 {
        self.cleanup_lease.fencing_token()
    }

    pub fn proven_at_ms(&self) -> i64 {
        self.proven_at_ms
    }

    pub fn proves(&self, lease: &StagingLease) -> bool {
        self.expired_lease.same_generation(lease)
    }

    pub fn assert_cleanup_guard(&self, guard: &StagingMutationGuard) -> Result<()> {
        let guarded = guard.assert_current()?;
        if !guarded.same_generation(&self.cleanup_lease) {
            return Err(CdfError::contract(
                "staging cleanup mutation guard does not bind the proof's cleanup generation",
            ));
        }
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        self.expired_lease.validate()?;
        self.cleanup_lease.validate()?;
        if self.expired_lease.authority_domain_id != self.cleanup_lease.authority_domain_id
            || self.expired_lease.identity != self.cleanup_lease.identity
            || self.cleanup_lease.fencing_token() <= self.expired_lease.fencing_token()
        {
            return Err(CdfError::contract(
                "staging cleanup proof does not fence the expired lease identity",
            ));
        }
        Ok(())
    }
}

pub trait StagingLeaseAuthority: Send + Sync {
    fn authority_domain_id(&self) -> LeaseAuthorityDomainId;

    fn acquire(
        &self,
        identity: StagingLeaseIdentity,
        owner: LeaseOwnerId,
        lease_duration_ms: u64,
    ) -> Result<StagingLease>;

    fn renew(&self, lease: &StagingLease, lease_duration_ms: u64) -> Result<StagingLease>;

    fn release(&self, lease: &StagingLease) -> Result<()>;

    fn assert_current(&self, lease: &StagingLease) -> Result<()>;

    /// Atomically claims cleanup authority for an inactive staging generation.
    ///
    /// A successful proof contains a newer active lease generation. Callers must renew that
    /// cleanup lease until deletion completes, then release it. This closes the prove-then-delete
    /// race with a new process acquiring the same staging identity.
    fn prove_expired(
        &self,
        lease: &StagingLease,
        collector: LeaseOwnerId,
        cleanup_lease_duration_ms: u64,
    ) -> Result<Option<ExpiredStagingLeaseProof>>;
}

pub struct ScopeStagingLeaseAuthority {
    scopes: Arc<dyn ScopeLeaseStore>,
}

impl ScopeStagingLeaseAuthority {
    pub fn new(scopes: Arc<dyn ScopeLeaseStore>) -> Self {
        Self { scopes }
    }
}

impl StagingLeaseAuthority for ScopeStagingLeaseAuthority {
    fn authority_domain_id(&self) -> LeaseAuthorityDomainId {
        self.scopes.authority_domain_id()
    }

    fn acquire(
        &self,
        identity: StagingLeaseIdentity,
        owner: LeaseOwnerId,
        lease_duration_ms: u64,
    ) -> Result<StagingLease> {
        let scope_lease = self
            .scopes
            .acquire(identity.scope(), owner, lease_duration_ms)?;
        let lease = StagingLease {
            authority_domain_id: self.authority_domain_id(),
            identity,
            scope_lease,
        };
        lease.validate()?;
        Ok(lease)
    }

    fn renew(&self, lease: &StagingLease, lease_duration_ms: u64) -> Result<StagingLease> {
        lease.validate()?;
        if lease.authority_domain_id != self.authority_domain_id() {
            return Err(CdfError::contract(
                "staging lease belongs to a different authority domain",
            ));
        }
        let renewed = StagingLease {
            authority_domain_id: lease.authority_domain_id.clone(),
            identity: lease.identity.clone(),
            scope_lease: self.scopes.renew(&lease.scope_lease, lease_duration_ms)?,
        };
        renewed.validate()?;
        Ok(renewed)
    }

    fn release(&self, lease: &StagingLease) -> Result<()> {
        lease.validate()?;
        if lease.authority_domain_id != self.authority_domain_id() {
            return Err(CdfError::contract(
                "staging lease belongs to a different authority domain",
            ));
        }
        self.scopes.release(&lease.scope_lease)
    }

    fn assert_current(&self, lease: &StagingLease) -> Result<()> {
        lease.validate()?;
        if lease.authority_domain_id != self.authority_domain_id() {
            return Err(CdfError::contract(
                "staging lease belongs to a different authority domain",
            ));
        }
        self.scopes.assert_current(&lease.scope_lease)
    }

    fn prove_expired(
        &self,
        lease: &StagingLease,
        collector: LeaseOwnerId,
        cleanup_lease_duration_ms: u64,
    ) -> Result<Option<ExpiredStagingLeaseProof>> {
        lease.validate()?;
        if lease.authority_domain_id != self.authority_domain_id() {
            return Err(CdfError::contract(
                "staging cleanup candidate belongs to a different authority domain",
            ));
        }
        self.scopes
            .prove_expired(&lease.scope_lease, collector, cleanup_lease_duration_ms)?
            .map(|proof: ExpiredScopeLeaseProof| {
                let proof = ExpiredStagingLeaseProof {
                    expired_lease: StagingLease {
                        authority_domain_id: lease.authority_domain_id.clone(),
                        identity: lease.identity.clone(),
                        scope_lease: proof.expired_lease,
                    },
                    cleanup_lease: StagingLease {
                        authority_domain_id: lease.authority_domain_id.clone(),
                        identity: lease.identity.clone(),
                        scope_lease: proof.cleanup_lease,
                    },
                    proven_at_ms: proof.proven_at_ms,
                };
                proof.validate()?;
                Ok(proof)
            })
            .transpose()
    }
}

#[derive(Clone, Copy)]
pub struct StagingLeaseTiming {
    pub lease_duration: Duration,
    pub renew_interval: Duration,
}

impl StagingLeaseTiming {
    fn validate(self) -> Result<Self> {
        if self.lease_duration.is_zero()
            || self.renew_interval.is_zero()
            || self.renew_interval >= self.lease_duration
        {
            return Err(CdfError::contract(
                "staging lease renewal interval must be positive and shorter than its duration",
            ));
        }
        Ok(self)
    }
}

impl Default for StagingLeaseTiming {
    fn default() -> Self {
        Self {
            lease_duration: DEFAULT_LEASE_DURATION,
            renew_interval: DEFAULT_RENEW_INTERVAL,
        }
    }
}

struct LeaseEntry {
    lease: StagingLease,
    failure: Option<CdfError>,
    cancellation: RunCancellation,
    guard_count: u64,
    release_pending: bool,
}

struct LeaseSupervisorState {
    next_registration: u64,
    leases: BTreeMap<u64, LeaseEntry>,
    terminal_failure: Option<CdfError>,
}

struct LeaseSupervisorShared {
    state: Mutex<LeaseSupervisorState>,
}

pub struct StagingLeaseSupervisor {
    authority: Arc<dyn StagingLeaseAuthority>,
    timing: StagingLeaseTiming,
    shared: Arc<LeaseSupervisorShared>,
    termination: cdf_kernel::InvocationTermination,
}

impl StagingLeaseSupervisor {
    pub fn new(
        authority: Arc<dyn StagingLeaseAuthority>,
        host: Arc<dyn ExecutionHost>,
    ) -> Result<Arc<Self>> {
        Self::with_timing(authority, host, StagingLeaseTiming::default())
    }

    pub fn with_timing(
        authority: Arc<dyn StagingLeaseAuthority>,
        host: Arc<dyn ExecutionHost>,
        timing: StagingLeaseTiming,
    ) -> Result<Arc<Self>> {
        let timing = timing.validate()?;
        let shared = Arc::new(LeaseSupervisorShared {
            state: Mutex::new(LeaseSupervisorState {
                next_registration: 1,
                leases: BTreeMap::new(),
                terminal_failure: None,
            }),
        });
        let mut scope = host.open_scope("cdf-staging-leases")?;
        let cancellation = scope.cancellation();
        let task_cancellation = cancellation.clone();
        let task_host = Arc::clone(&host);
        let task_authority = Arc::clone(&authority);
        let task_shared = Arc::clone(&shared);
        scope.spawn_io(Box::pin(async move {
            let result = lease_supervisor_loop(
                task_authority,
                task_host,
                timing,
                Arc::clone(&task_shared),
                task_cancellation,
            )
            .await;
            if let Err(error) = &result {
                fail_registered_leases(&task_shared, error.clone());
            }
            result
        }))?;
        let joined = scope.join();
        let termination = cdf_kernel::InvocationTermination::new(
            move || cancellation.cancel(),
            Box::pin(async move { joined.await.map(|_| ()) }),
        );
        Ok(Arc::new(Self {
            authority,
            timing,
            shared,
            termination,
        }))
    }

    pub fn acquire(
        self: &Arc<Self>,
        identity: StagingLeaseIdentity,
        owner: LeaseOwnerId,
    ) -> Result<ManagedStagingLease> {
        let duration_ms = duration_ms(self.timing.lease_duration)?;
        let lease = self
            .authority
            .acquire(identity.clone(), owner.clone(), duration_ms)?;
        if let Err(error) = validate_acquired_lease(
            &lease,
            &self.authority.authority_domain_id(),
            &identity,
            &owner,
        ) {
            return Err(with_release_failure(error, self.authority.release(&lease)));
        }
        match self.register(lease.clone()) {
            Ok(managed) => Ok(managed),
            Err(error) => Err(with_release_failure(error, self.authority.release(&lease))),
        }
    }

    fn register(self: &Arc<Self>, lease: StagingLease) -> Result<ManagedStagingLease> {
        lease.validate()?;
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| CdfError::internal("staging lease supervisor lock is poisoned"))?;
        if let Some(error) = &state.terminal_failure {
            return Err(error.clone());
        }
        let registration = state.next_registration;
        state.next_registration = state
            .next_registration
            .checked_add(1)
            .ok_or_else(|| CdfError::internal("staging lease registration overflow"))?;
        state.leases.insert(
            registration,
            LeaseEntry {
                lease,
                failure: None,
                cancellation: RunCancellation::default(),
                guard_count: 0,
                release_pending: false,
            },
        );
        Ok(ManagedStagingLease {
            supervisor: Arc::clone(self),
            registration: Some(registration),
        })
    }

    pub fn prove_expired(
        self: &Arc<Self>,
        lease: &StagingLease,
        collector: LeaseOwnerId,
    ) -> Result<Option<ManagedExpiredStagingLeaseProof>> {
        if lease.authority_domain_id != self.authority.authority_domain_id() {
            return Ok(None);
        }
        let duration_ms = duration_ms(self.timing.lease_duration)?;
        let Some(proof) = self
            .authority
            .prove_expired(lease, collector.clone(), duration_ms)?
        else {
            return Ok(None);
        };
        if let Err(error) = validate_cleanup_proof(&proof, lease, &collector) {
            return Err(with_release_failure(
                error,
                self.authority.release(&proof.cleanup_lease),
            ));
        }
        let cleanup_lease = proof.cleanup_lease.clone();
        match self.register(cleanup_lease.clone()) {
            Ok(managed) => Ok(Some(ManagedExpiredStagingLeaseProof {
                proof,
                cleanup_lease: Some(managed),
            })),
            Err(error) => Err(with_release_failure(
                error,
                self.authority.release(&cleanup_lease),
            )),
        }
    }

    fn snapshot(&self, registration: u64) -> Result<StagingLease> {
        let lease = {
            let state = self
                .shared
                .state
                .lock()
                .map_err(|_| CdfError::internal("staging lease supervisor lock is poisoned"))?;
            let entry = state
                .leases
                .get(&registration)
                .ok_or_else(|| CdfError::internal("staging lease registration is absent"))?;
            if let Some(error) = &entry.failure {
                return Err(error.clone());
            }
            entry.lease.clone()
        };
        self.authority.assert_current(&lease)?;
        Ok(lease)
    }

    fn mutation_guard(self: &Arc<Self>, registration: u64) -> Result<StagingMutationGuard> {
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| CdfError::internal("staging lease supervisor lock is poisoned"))?;
        let entry = state
            .leases
            .get_mut(&registration)
            .ok_or_else(|| CdfError::internal("staging lease registration is absent"))?;
        entry.guard_count = entry
            .guard_count
            .checked_add(1)
            .ok_or_else(|| CdfError::internal("staging mutation guard count overflow"))?;
        let cancellation = entry.cancellation.clone();
        Ok(StagingMutationGuard {
            supervisor: Arc::clone(self),
            registration,
            cancellation,
        })
    }

    fn release(&self, registration: u64, explicit: bool) -> Result<()> {
        let entry = {
            let mut state = self
                .shared
                .state
                .lock()
                .map_err(|_| CdfError::internal("staging lease supervisor lock is poisoned"))?;
            let guarded = state
                .leases
                .get_mut(&registration)
                .ok_or_else(|| CdfError::internal("staging lease registration is absent"))?;
            if guarded.guard_count != 0 {
                guarded.release_pending = true;
                if explicit {
                    return Err(CdfError::internal(
                        "staging lease cannot finish while mutation guards remain live",
                    ));
                }
                return Ok(());
            }
            state
                .leases
                .remove(&registration)
                .expect("staging lease registration was just observed")
        };
        let release = self.authority.release(&entry.lease);
        match (entry.failure, release) {
            (Some(mut error), Err(release)) => {
                error
                    .message
                    .push_str(&format!("; staging lease release also failed: {release}"));
                Err(error)
            }
            (Some(error), Ok(())) => Err(error),
            (None, result) => result,
        }
    }

    fn clone_guard(&self, registration: u64) -> Result<()> {
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| CdfError::internal("staging lease supervisor lock is poisoned"))?;
        let entry = state
            .leases
            .get_mut(&registration)
            .ok_or_else(|| CdfError::internal("staging lease registration is absent"))?;
        entry.guard_count = entry
            .guard_count
            .checked_add(1)
            .ok_or_else(|| CdfError::internal("staging mutation guard count overflow"))?;
        Ok(())
    }

    fn drop_guard(&self, registration: u64) {
        let entry = {
            let Ok(mut state) = self.shared.state.lock() else {
                return;
            };
            let Some(entry) = state.leases.get_mut(&registration) else {
                return;
            };
            entry.guard_count = entry.guard_count.saturating_sub(1);
            if entry.guard_count != 0 || !entry.release_pending {
                return;
            }
            state.leases.remove(&registration)
        };
        if let Some(entry) = entry {
            let _ = self.authority.release(&entry.lease);
        }
    }
}

impl Drop for StagingLeaseSupervisor {
    fn drop(&mut self) {
        self.termination.cancel();
    }
}

pub struct ManagedStagingLease {
    supervisor: Arc<StagingLeaseSupervisor>,
    registration: Option<u64>,
}

impl ManagedStagingLease {
    pub fn snapshot(&self) -> Result<StagingLease> {
        self.supervisor.snapshot(
            self.registration
                .ok_or_else(|| CdfError::internal("staging lease was already released"))?,
        )
    }

    pub fn mutation_guard(&self) -> Result<StagingMutationGuard> {
        self.supervisor.mutation_guard(
            self.registration
                .ok_or_else(|| CdfError::internal("staging lease was already released"))?,
        )
    }

    pub fn finish(mut self) -> Result<()> {
        let registration = self
            .registration
            .take()
            .ok_or_else(|| CdfError::internal("staging lease was already released"))?;
        self.supervisor.release(registration, true)
    }
}

/// Cloneable runtime-owned authority that must be checked immediately before each externally
/// durable staging mutation. Renewal failure cancels the guard before the next mutation can
/// publish or acknowledge work.
pub struct StagingMutationGuard {
    supervisor: Arc<StagingLeaseSupervisor>,
    registration: u64,
    cancellation: RunCancellation,
}

impl Clone for StagingMutationGuard {
    fn clone(&self) -> Self {
        self.supervisor
            .clone_guard(self.registration)
            .expect("live staging mutation guard retains its lease registration");
        Self {
            supervisor: Arc::clone(&self.supervisor),
            registration: self.registration,
            cancellation: self.cancellation.clone(),
        }
    }
}

impl StagingMutationGuard {
    pub fn assert_current(&self) -> Result<StagingLease> {
        let lease = self.supervisor.snapshot(self.registration)?;
        self.cancellation.check()?;
        Ok(lease)
    }

    pub fn cancellation(&self) -> RunCancellation {
        self.cancellation.clone()
    }
}

impl std::fmt::Debug for StagingMutationGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StagingMutationGuard")
            .field("registration", &self.registration)
            .finish_non_exhaustive()
    }
}

impl PartialEq for StagingMutationGuard {
    fn eq(&self, other: &Self) -> bool {
        self.registration == other.registration && Arc::ptr_eq(&self.supervisor, &other.supervisor)
    }
}

impl Eq for StagingMutationGuard {}

impl Drop for StagingMutationGuard {
    fn drop(&mut self) {
        self.supervisor.drop_guard(self.registration);
    }
}

impl Drop for ManagedStagingLease {
    fn drop(&mut self) {
        if let Some(registration) = self.registration.take() {
            let _ = self.supervisor.release(registration, false);
        }
    }
}

/// Exact expiry evidence coupled to the renewable fencing generation held during cleanup.
pub struct ManagedExpiredStagingLeaseProof {
    proof: ExpiredStagingLeaseProof,
    cleanup_lease: Option<ManagedStagingLease>,
}

impl ManagedExpiredStagingLeaseProof {
    pub fn proof(&self) -> &ExpiredStagingLeaseProof {
        &self.proof
    }

    pub fn mutation_guard(&self) -> Result<StagingMutationGuard> {
        self.cleanup_lease
            .as_ref()
            .ok_or_else(|| CdfError::internal("staging cleanup lease was already released"))?
            .mutation_guard()
    }

    pub fn finish(mut self) -> Result<()> {
        self.cleanup_lease
            .take()
            .ok_or_else(|| CdfError::internal("staging cleanup lease was already released"))?
            .finish()
    }

    /// Executes exact-generation cleanup and always releases the cleanup lease, preserving both
    /// errors when storage cleanup and authoritative release fail together.
    pub fn execute<T>(
        mut self,
        cleanup: impl FnOnce(&ExpiredStagingLeaseProof, &StagingMutationGuard) -> Result<T>,
    ) -> Result<T> {
        let guard = self
            .cleanup_lease
            .as_ref()
            .ok_or_else(|| CdfError::internal("staging cleanup lease was already released"))?
            .mutation_guard();
        let guard = match guard {
            Ok(guard) => guard,
            Err(error) => {
                let released = self
                    .cleanup_lease
                    .take()
                    .ok_or_else(|| {
                        CdfError::internal("staging cleanup lease was already released")
                    })?
                    .finish();
                return combine_cleanup_release(Err(error), released);
            }
        };
        let cleaned = cleanup(&self.proof, &guard);
        drop(guard);
        let released = self
            .cleanup_lease
            .take()
            .ok_or_else(|| CdfError::internal("staging cleanup lease was already released"))?
            .finish();
        combine_cleanup_release(cleaned, released)
    }
}

pub(crate) fn combine_cleanup_release<T>(cleaned: Result<T>, released: Result<()>) -> Result<T> {
    match (cleaned, released) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(release)) => Err(with_release_failure(error, Err(release))),
    }
}

pub(crate) fn with_release_failure(mut primary: CdfError, release: Result<()>) -> CdfError {
    if let Err(release) = release {
        primary
            .message
            .push_str(&format!("; staging lease release also failed: {release}"));
    }
    primary
}

async fn lease_supervisor_loop(
    authority: Arc<dyn StagingLeaseAuthority>,
    host: Arc<dyn ExecutionHost>,
    timing: StagingLeaseTiming,
    shared: Arc<LeaseSupervisorShared>,
    cancellation: RunCancellation,
) -> Result<()> {
    loop {
        if let Err(error) = host
            .delay(timing.renew_interval, cancellation.clone())
            .await
        {
            return if cancellation.is_cancelled() {
                Ok(())
            } else {
                Err(error)
            };
        }
        cancellation.check()?;
        let state = shared
            .state
            .lock()
            .map_err(|_| CdfError::internal("staging lease supervisor lock is poisoned"))?;
        let leases = state
            .leases
            .iter()
            .filter(|(_, entry)| entry.failure.is_none())
            .map(|(registration, entry)| (*registration, entry.lease.clone()))
            .collect::<Vec<_>>();
        drop(state);

        let duration_ms = match duration_ms(timing.lease_duration) {
            Ok(duration) => duration,
            Err(error) => return Err(error),
        };
        for (registration, lease) in leases {
            let renewed = authority
                .renew(&lease, duration_ms)
                .and_then(|renewed| validate_renewed_lease(renewed, &lease));
            let mut state = shared
                .state
                .lock()
                .map_err(|_| CdfError::internal("staging lease supervisor lock is poisoned"))?;
            let Some(entry) = state.leases.get_mut(&registration) else {
                continue;
            };
            if entry.lease != lease {
                continue;
            }
            match renewed {
                Ok(renewed) => entry.lease = renewed,
                Err(error) => {
                    entry.failure = Some(error);
                    entry.cancellation.cancel();
                }
            }
        }
    }
}

fn fail_registered_leases(shared: &LeaseSupervisorShared, error: CdfError) {
    let Ok(mut state) = shared.state.lock() else {
        return;
    };
    if state.terminal_failure.is_none() {
        state.terminal_failure = Some(error.clone());
    }
    for entry in state.leases.values_mut() {
        if entry.failure.is_none() {
            entry.failure = Some(error.clone());
        }
        entry.cancellation.cancel();
    }
}

fn duration_ms(duration: Duration) -> Result<u64> {
    u64::try_from(duration.as_millis())
        .map_err(|_| CdfError::contract("staging lease duration exceeds u64 milliseconds"))
}

fn validate_acquired_lease(
    lease: &StagingLease,
    authority_domain_id: &LeaseAuthorityDomainId,
    identity: &StagingLeaseIdentity,
    owner: &LeaseOwnerId,
) -> Result<()> {
    lease.validate()?;
    if &lease.authority_domain_id != authority_domain_id
        || &lease.identity != identity
        || &lease.scope_lease.owner != owner
    {
        return Err(CdfError::contract(
            "staging lease authority returned an acquisition outside the requested identity or owner",
        ));
    }
    Ok(())
}

fn validate_renewed_lease(renewed: StagingLease, previous: &StagingLease) -> Result<StagingLease> {
    renewed.validate()?;
    if renewed.authority_domain_id != previous.authority_domain_id
        || renewed.identity != previous.identity
        || renewed.scope_lease.owner != previous.scope_lease.owner
        || renewed.scope_lease.fencing_token != previous.scope_lease.fencing_token
        || renewed.scope_lease.acquired_at_ms != previous.scope_lease.acquired_at_ms
        || renewed.scope_lease.expires_at_ms <= previous.scope_lease.expires_at_ms
    {
        return Err(CdfError::contract(
            "staging lease authority returned a renewal that changed identity or did not extend expiry",
        ));
    }
    Ok(renewed)
}

fn validate_cleanup_proof(
    proof: &ExpiredStagingLeaseProof,
    expired: &StagingLease,
    collector: &LeaseOwnerId,
) -> Result<()> {
    proof.validate()?;
    if !proof.proves(expired)
        || &proof.cleanup_lease.scope_lease.owner != collector
        || proof.cleanup_lease.scope_lease.acquired_at_ms != proof.proven_at_ms
    {
        return Err(CdfError::contract(
            "staging lease authority returned a cleanup proof outside the requested generation or collector",
        ));
    }
    Ok(())
}
