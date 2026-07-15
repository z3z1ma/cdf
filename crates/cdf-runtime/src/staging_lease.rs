use std::{
    collections::BTreeMap,
    sync::{Arc, Condvar, Mutex},
    thread::JoinHandle,
    time::Duration,
};

use cdf_kernel::{
    CdfError, DestinationId, ExpiredScopeLeaseProof, LeaseOwnerId, Result, ScopeKey, ScopeLease,
    ScopeLeaseStore, TargetName,
};
use serde::{Deserialize, Serialize};

use crate::LoadAttemptId;

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
    pub identity: StagingLeaseIdentity,
    pub scope_lease: ScopeLease,
}

impl StagingLease {
    pub fn fencing_token(&self) -> u64 {
        self.scope_lease.fencing_token.get()
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
        &self.expired_lease == lease
    }

    fn validate(&self) -> Result<()> {
        self.expired_lease.validate()?;
        self.cleanup_lease.validate()?;
        if self.expired_lease.identity != self.cleanup_lease.identity
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
            identity,
            scope_lease,
        };
        lease.validate()?;
        Ok(lease)
    }

    fn renew(&self, lease: &StagingLease, lease_duration_ms: u64) -> Result<StagingLease> {
        lease.validate()?;
        let renewed = StagingLease {
            identity: lease.identity.clone(),
            scope_lease: self.scopes.renew(&lease.scope_lease, lease_duration_ms)?,
        };
        renewed.validate()?;
        Ok(renewed)
    }

    fn release(&self, lease: &StagingLease) -> Result<()> {
        lease.validate()?;
        self.scopes.release(&lease.scope_lease)
    }

    fn assert_current(&self, lease: &StagingLease) -> Result<()> {
        lease.validate()?;
        self.scopes.assert_current(&lease.scope_lease)
    }

    fn prove_expired(
        &self,
        lease: &StagingLease,
        collector: LeaseOwnerId,
        cleanup_lease_duration_ms: u64,
    ) -> Result<Option<ExpiredStagingLeaseProof>> {
        lease.validate()?;
        self.scopes
            .prove_expired(&lease.scope_lease, collector, cleanup_lease_duration_ms)?
            .map(|proof: ExpiredScopeLeaseProof| {
                let proof = ExpiredStagingLeaseProof {
                    expired_lease: StagingLease {
                        identity: lease.identity.clone(),
                        scope_lease: proof.expired_lease,
                    },
                    cleanup_lease: StagingLease {
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
}

struct LeaseSupervisorState {
    shutdown: bool,
    next_registration: u64,
    leases: BTreeMap<u64, LeaseEntry>,
}

struct LeaseSupervisorShared {
    state: Mutex<LeaseSupervisorState>,
    wake: Condvar,
}

pub struct StagingLeaseSupervisor {
    authority: Arc<dyn StagingLeaseAuthority>,
    timing: StagingLeaseTiming,
    shared: Arc<LeaseSupervisorShared>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl StagingLeaseSupervisor {
    pub fn new(authority: Arc<dyn StagingLeaseAuthority>) -> Result<Arc<Self>> {
        Self::with_timing(authority, StagingLeaseTiming::default())
    }

    pub fn with_timing(
        authority: Arc<dyn StagingLeaseAuthority>,
        timing: StagingLeaseTiming,
    ) -> Result<Arc<Self>> {
        let timing = timing.validate()?;
        let shared = Arc::new(LeaseSupervisorShared {
            state: Mutex::new(LeaseSupervisorState {
                shutdown: false,
                next_registration: 1,
                leases: BTreeMap::new(),
            }),
            wake: Condvar::new(),
        });
        let supervisor = Arc::new(Self {
            authority: Arc::clone(&authority),
            timing,
            shared: Arc::clone(&shared),
            worker: Mutex::new(None),
        });
        let worker = std::thread::Builder::new()
            .name("cdf-staging-leases".to_owned())
            .spawn(move || lease_supervisor_loop(authority, timing, shared))
            .map_err(|error| {
                CdfError::internal(format!("start staging lease supervisor: {error}"))
            })?;
        *supervisor
            .worker
            .lock()
            .map_err(|_| CdfError::internal("staging lease worker lock is poisoned"))? =
            Some(worker);
        Ok(supervisor)
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
        if let Err(error) = validate_acquired_lease(&lease, &identity, &owner) {
            let _ = self.authority.release(&lease);
            return Err(error);
        }
        match self.register(lease.clone()) {
            Ok(managed) => Ok(managed),
            Err(error) => {
                let _ = self.authority.release(&lease);
                Err(error)
            }
        }
    }

    fn register(self: &Arc<Self>, lease: StagingLease) -> Result<ManagedStagingLease> {
        lease.validate()?;
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| CdfError::internal("staging lease supervisor lock is poisoned"))?;
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
        let duration_ms = duration_ms(self.timing.lease_duration)?;
        let Some(proof) = self
            .authority
            .prove_expired(lease, collector.clone(), duration_ms)?
        else {
            return Ok(None);
        };
        if let Err(error) = validate_cleanup_proof(&proof, lease, &collector) {
            let _ = self.authority.release(&proof.cleanup_lease);
            return Err(error);
        }
        let cleanup_lease = proof.cleanup_lease.clone();
        match self.register(cleanup_lease.clone()) {
            Ok(managed) => Ok(Some(ManagedExpiredStagingLeaseProof {
                proof,
                cleanup_lease: Some(managed),
            })),
            Err(error) => {
                let _ = self.authority.release(&cleanup_lease);
                Err(error)
            }
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

    fn release(&self, registration: u64) -> Result<()> {
        let entry = self
            .shared
            .state
            .lock()
            .map_err(|_| CdfError::internal("staging lease supervisor lock is poisoned"))?
            .leases
            .remove(&registration)
            .ok_or_else(|| CdfError::internal("staging lease registration is absent"))?;
        let release = self.authority.release(&entry.lease);
        match (entry.failure, release) {
            (Some(error), _) => Err(error),
            (None, result) => result,
        }
    }
}

impl Drop for StagingLeaseSupervisor {
    fn drop(&mut self) {
        if let Ok(mut state) = self.shared.state.lock() {
            state.shutdown = true;
            self.shared.wake.notify_all();
        }
        if let Ok(worker) = self.worker.get_mut()
            && let Some(worker) = worker.take()
        {
            let _ = worker.join();
        }
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

    pub fn finish(mut self) -> Result<()> {
        let registration = self
            .registration
            .take()
            .ok_or_else(|| CdfError::internal("staging lease was already released"))?;
        self.supervisor.release(registration)
    }
}

impl Drop for ManagedStagingLease {
    fn drop(&mut self) {
        if let Some(registration) = self.registration.take() {
            let _ = self.supervisor.release(registration);
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

    pub fn finish(mut self) -> Result<()> {
        self.cleanup_lease
            .take()
            .ok_or_else(|| CdfError::internal("staging cleanup lease was already released"))?
            .finish()
    }
}

fn lease_supervisor_loop(
    authority: Arc<dyn StagingLeaseAuthority>,
    timing: StagingLeaseTiming,
    shared: Arc<LeaseSupervisorShared>,
) {
    loop {
        let state = match shared.state.lock() {
            Ok(state) => state,
            Err(_) => return,
        };
        if state.shutdown {
            return;
        }
        let wait = shared
            .wake
            .wait_timeout_while(state, timing.renew_interval, |state| !state.shutdown);
        let Ok((state, timeout)) = wait else {
            return;
        };
        if state.shutdown {
            return;
        }
        if !timeout.timed_out() {
            continue;
        }
        let leases = state
            .leases
            .iter()
            .filter(|(_, entry)| entry.failure.is_none())
            .map(|(registration, entry)| (*registration, entry.lease.clone()))
            .collect::<Vec<_>>();
        drop(state);

        let duration_ms = match duration_ms(timing.lease_duration) {
            Ok(duration) => duration,
            Err(_) => return,
        };
        for (registration, lease) in leases {
            let renewed = authority
                .renew(&lease, duration_ms)
                .and_then(|renewed| validate_renewed_lease(renewed, &lease));
            let Ok(mut state) = shared.state.lock() else {
                return;
            };
            let Some(entry) = state.leases.get_mut(&registration) else {
                continue;
            };
            if entry.lease != lease {
                continue;
            }
            match renewed {
                Ok(renewed) => entry.lease = renewed,
                Err(error) => entry.failure = Some(error),
            }
        }
    }
}

fn duration_ms(duration: Duration) -> Result<u64> {
    u64::try_from(duration.as_millis())
        .map_err(|_| CdfError::contract("staging lease duration exceeds u64 milliseconds"))
}

fn validate_acquired_lease(
    lease: &StagingLease,
    identity: &StagingLeaseIdentity,
    owner: &LeaseOwnerId,
) -> Result<()> {
    lease.validate()?;
    if &lease.identity != identity || &lease.scope_lease.owner != owner {
        return Err(CdfError::contract(
            "staging lease authority returned an acquisition outside the requested identity or owner",
        ));
    }
    Ok(())
}

fn validate_renewed_lease(renewed: StagingLease, previous: &StagingLease) -> Result<StagingLease> {
    renewed.validate()?;
    if renewed.identity != previous.identity
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
