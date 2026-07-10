use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};

use cdf_kernel::{ContractRef, LeaseOwnerId, Result, ScopeKey, ScopeLeaseClock, ScopeLeaseStore};

pub struct ManualScopeLeaseClock {
    now_ms: AtomicI64,
}

impl ManualScopeLeaseClock {
    pub fn new(now_ms: i64) -> Self {
        Self {
            now_ms: AtomicI64::new(now_ms),
        }
    }

    pub fn set(&self, now_ms: i64) {
        self.now_ms.store(now_ms, Ordering::SeqCst);
    }
}

impl ScopeLeaseClock for ManualScopeLeaseClock {
    fn now_ms(&self) -> Result<i64> {
        Ok(self.now_ms.load(Ordering::SeqCst))
    }
}

pub fn assert_scope_lease_store_send_sync<S: ScopeLeaseStore + Send + Sync>() {}

pub fn assert_scope_lease_store_conformance<S, F>(mut fresh_store: F)
where
    S: ScopeLeaseStore,
    F: FnMut(Arc<ManualScopeLeaseClock>) -> S,
{
    assert_scope_lease_store_send_sync::<S>();
    let clock = Arc::new(ManualScopeLeaseClock::new(1_000));
    assert_acquire_contention_expiry_and_fencing(&fresh_store(Arc::clone(&clock)), &clock);
    let clock = Arc::new(ManualScopeLeaseClock::new(2_000));
    assert_renew_and_release(&fresh_store(Arc::clone(&clock)), &clock);
    let clock = Arc::new(ManualScopeLeaseClock::new(3_000));
    assert_scope_isolation(&fresh_store(Arc::clone(&clock)));
}

fn scope(name: &str) -> ScopeKey {
    ScopeKey::SchemaContract {
        contract: ContractRef::new(name).unwrap(),
    }
}

fn owner(name: &str) -> LeaseOwnerId {
    LeaseOwnerId::new(name).unwrap()
}

fn assert_acquire_contention_expiry_and_fencing<S: ScopeLeaseStore>(
    store: &S,
    clock: &ManualScopeLeaseClock,
) {
    let scope = scope("orders");
    let first = store
        .acquire(scope.clone(), owner("executor-a"), 100)
        .unwrap();
    assert_eq!(first.fencing_token.get(), 1);
    assert_eq!(first.acquired_at_ms, 1_000);
    assert_eq!(first.expires_at_ms, 1_100);
    clock.set(1_099);
    store.assert_current(&first).unwrap();
    assert!(
        store
            .acquire(scope.clone(), owner("executor-b"), 100)
            .is_err()
    );

    clock.set(1_100);
    assert!(store.assert_current(&first).is_err());
    assert!(store.renew(&first, 100).is_err());
    assert!(store.release(&first).is_err());

    let second = store.acquire(scope, owner("executor-b"), 100).unwrap();
    assert_eq!(second.fencing_token.get(), 2);
    clock.set(1_101);
    assert!(store.assert_current(&first).is_err());
    assert!(store.renew(&first, 100).is_err());
    assert!(store.release(&first).is_err());
    store.assert_current(&second).unwrap();
}

fn assert_renew_and_release<S: ScopeLeaseStore>(store: &S, clock: &ManualScopeLeaseClock) {
    let scope = scope("events");
    let lease = store
        .acquire(scope.clone(), owner("executor-a"), 100)
        .unwrap();
    clock.set(2_050);
    let renewed = store.renew(&lease, 200).unwrap();
    assert_eq!(renewed.fencing_token, lease.fencing_token);
    assert_eq!(renewed.acquired_at_ms, lease.acquired_at_ms);
    assert_eq!(renewed.expires_at_ms, 2_250);
    clock.set(2_249);
    store.assert_current(&renewed).unwrap();

    store.release(&renewed).unwrap();
    assert!(store.assert_current(&renewed).is_err());
    assert!(store.release(&renewed).is_err());
    let next = store.acquire(scope, owner("executor-b"), 100).unwrap();
    assert_eq!(next.fencing_token.get(), lease.fencing_token.get() + 1);
}

fn assert_scope_isolation<S: ScopeLeaseStore>(store: &S) {
    let orders = store
        .acquire(scope("orders"), owner("executor-a"), 100)
        .unwrap();
    let events = store
        .acquire(scope("events"), owner("executor-b"), 100)
        .unwrap();
    assert_eq!(orders.fencing_token.get(), 1);
    assert_eq!(events.fencing_token.get(), 1);
    store.assert_current(&orders).unwrap();
    store.assert_current(&events).unwrap();
}
