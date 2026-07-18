use std::{
    collections::BTreeMap,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use futures_executor::block_on;

use super::*;

fn consumer(name: &str, class: MemoryClass) -> ConsumerKey {
    ConsumerKey::new(name, class).unwrap()
}

#[test]
fn shared_payload_clones_release_exactly_once() {
    let coordinator = DeterministicMemoryCoordinator::new(1024, BTreeMap::new()).unwrap();
    let request = ReservationRequest::new(consumer("decode", MemoryClass::Decode), 128).unwrap();
    let lease = coordinator.try_reserve(&request).unwrap().unwrap();
    let payload = AccountedBytes::new(bytes::Bytes::from(vec![7_u8; 64]), lease).unwrap();
    let clone = payload.clone();
    assert_eq!(coordinator.snapshot().current_bytes, 64);
    drop(payload);
    assert_eq!(coordinator.snapshot().current_bytes, 64);
    drop(clone);
    assert_eq!(coordinator.snapshot().current_bytes, 0);
    assert_eq!(coordinator.snapshot().peak_bytes, 128);
}

#[test]
fn logical_slices_retain_one_complete_physical_allocation() {
    let coordinator = DeterministicMemoryCoordinator::new(1024, BTreeMap::new()).unwrap();
    let request =
        ReservationRequest::new(consumer("coalesced-range", MemoryClass::Source), 16).unwrap();
    let lease = coordinator.try_reserve(&request).unwrap().unwrap();
    let physical =
        AccountedBytes::new(bytes::Bytes::from_static(b"0123456789abcdef"), lease).unwrap();
    let left = physical.slice(0..4).unwrap();
    let right = physical.slice(12..16).unwrap();

    assert_eq!(left.payload(), b"0123");
    assert_eq!(right.payload(), b"cdef");
    assert_eq!(left.lease().bytes(), 16);
    assert_eq!(coordinator.snapshot().current_bytes, 16);
    drop(physical);
    drop(left);
    assert_eq!(coordinator.snapshot().current_bytes, 16);
    drop(right);
    assert_eq!(coordinator.snapshot().current_bytes, 0);
}

#[test]
fn accounted_batch_reconciles_reservation_and_error_drops_release() {
    let coordinator = DeterministicMemoryCoordinator::new(4096, BTreeMap::new()).unwrap();
    let request = ReservationRequest::new(consumer("batch", MemoryClass::Transform), 2048).unwrap();
    let lease = coordinator.try_reserve(&request).unwrap().unwrap();
    let batch =
        RecordBatch::try_from_iter([("value", Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as _)])
            .unwrap();
    let observed = record_batch_retained_bytes(&batch).unwrap();
    let accounted = AccountedBatch::new(batch, lease).unwrap();
    assert_eq!(accounted.lease().bytes(), observed);
    drop(accounted);
    assert_eq!(coordinator.snapshot().current_bytes, 0);

    let small = coordinator
        .try_reserve(
            &ReservationRequest::new(consumer("small", MemoryClass::Transform), 1).unwrap(),
        )
        .unwrap()
        .unwrap();
    let batch =
        RecordBatch::try_from_iter([("value", Arc::new(Int64Array::from(vec![1, 2])) as _)])
            .unwrap();
    assert!(AccountedBatch::new(batch, small).is_err());
    assert_eq!(coordinator.snapshot().current_bytes, 0);
}

#[test]
fn retained_bytes_count_shared_arrow_allocations_once() {
    let values = Arc::new(Int64Array::from_iter_values(0..1024)) as ArrayRef;
    let batch = RecordBatch::try_from_iter([
        ("left", Arc::clone(&values)),
        ("right", Arc::clone(&values)),
    ])
    .unwrap();

    let naive = batch.get_array_memory_size() as u64;
    let retained = record_batch_retained_bytes(&batch).unwrap();
    assert!(retained < naive);
    assert!(retained >= values.get_buffer_memory_size() as u64);
}

#[test]
fn weighted_subcap_and_async_release_enforce_admission() {
    let tag = BudgetTag::new("discovery.metadata").unwrap();
    let coordinator = Arc::new(
        DeterministicMemoryCoordinator::new(256, BTreeMap::from([(tag.clone(), 128)])).unwrap(),
    );
    let first_request = ReservationRequest::new(consumer("probe-a", MemoryClass::Discovery), 96)
        .unwrap()
        .with_subcap(tag.clone())
        .as_minimum_working_set();
    let second_request = ReservationRequest::new(consumer("probe-b", MemoryClass::Discovery), 64)
        .unwrap()
        .with_subcap(tag)
        .as_minimum_working_set();
    let first = coordinator.try_reserve(&first_request).unwrap().unwrap();
    assert!(coordinator.try_reserve(&second_request).unwrap().is_none());

    let coordinator_trait: Arc<dyn MemoryCoordinator> = coordinator.clone();
    let mut pending = Box::pin(reserve(coordinator_trait, second_request));
    let first_poll = futures_poll(pending.as_mut());
    assert!(matches!(first_poll, Poll::Pending));
    drop(first);
    let second = block_on(pending).unwrap();
    assert_eq!(second.bytes(), 64);
    drop(second);
    assert_eq!(coordinator.snapshot().current_bytes, 0);
    assert!(
        coordinator
            .snapshot()
            .consumers
            .values()
            .any(|usage| usage.waits > 0)
    );
}

struct SnapshotOnWake {
    coordinator: Arc<dyn MemoryCoordinator>,
    observed: Arc<AtomicBool>,
}

impl Wake for SnapshotOnWake {
    fn wake(self: Arc<Self>) {
        let _ = self.coordinator.snapshot();
        self.observed.store(true, Ordering::SeqCst);
    }
}

#[test]
fn lease_release_invokes_reentrant_waker_outside_coordinator_lock() {
    let coordinator = Arc::new(DeterministicMemoryCoordinator::new(128, BTreeMap::new()).unwrap());
    let request = ReservationRequest::new(consumer("holder", MemoryClass::Decode), 128).unwrap();
    let lease = coordinator.try_reserve(&request).unwrap().unwrap();
    let observed = Arc::new(AtomicBool::new(false));
    let coordinator_trait: Arc<dyn MemoryCoordinator> = coordinator.clone();
    let waker = Waker::from(Arc::new(SnapshotOnWake {
        coordinator: coordinator_trait.clone(),
        observed: Arc::clone(&observed),
    }));
    coordinator_trait.register_waiter(&waker);

    drop(lease);

    assert!(observed.load(Ordering::SeqCst));
    assert_eq!(coordinator.snapshot().current_bytes, 0);
}

fn futures_poll<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

#[derive(Default)]
struct RegistrationRaceCoordinator {
    available: AtomicBool,
    registered: AtomicBool,
    unregistered: AtomicBool,
}

struct NoopLeaseAccount;

impl LeaseAccount for NoopLeaseAccount {
    fn resize(&self, _current_bytes: u64, _new_bytes: u64) -> Result<()> {
        Ok(())
    }

    fn release(&self, _bytes: u64) {}
}

impl MemoryCoordinator for RegistrationRaceCoordinator {
    fn try_reserve(&self, request: &ReservationRequest) -> Result<Option<MemoryLease>> {
        if self.available.swap(false, Ordering::SeqCst) {
            return Ok(Some(MemoryLease::from_account(
                request.bytes,
                Arc::new(NoopLeaseAccount),
            )?));
        }
        Ok(None)
    }

    fn register_waiter(&self, _waker: &Waker) {
        self.registered.store(true, Ordering::SeqCst);
        // Simulate capacity becoming available after the first reservation attempt but
        // before the waiter is visible. No release remains to issue another wake.
        self.available.store(true, Ordering::SeqCst);
    }

    fn unregister_waiter(&self, _waker: &Waker) {
        self.unregistered.store(true, Ordering::SeqCst);
    }

    fn snapshot(&self) -> MemorySnapshot {
        MemorySnapshot::default()
    }

    fn record_event(&self, _event: MemoryEvent) {}
}

#[test]
fn async_reservation_closes_release_before_registration_race() {
    let coordinator = Arc::new(RegistrationRaceCoordinator::default());
    let coordinator_trait: Arc<dyn MemoryCoordinator> = coordinator.clone();
    let mut future = Box::pin(reserve(
        coordinator_trait,
        ReservationRequest::new(consumer("race", MemoryClass::Queue), 64).unwrap(),
    ));

    let result = futures_poll(future.as_mut());
    assert!(matches!(result, Poll::Ready(Ok(_))));
    assert!(coordinator.registered.load(Ordering::SeqCst));
    assert!(coordinator.unregistered.load(Ordering::SeqCst));
}

#[derive(Default)]
struct CancellationCoordinator {
    registered: AtomicBool,
    unregistered: AtomicBool,
}

impl MemoryCoordinator for CancellationCoordinator {
    fn try_reserve(&self, _request: &ReservationRequest) -> Result<Option<MemoryLease>> {
        Ok(None)
    }

    fn register_waiter(&self, _waker: &Waker) {
        self.registered.store(true, Ordering::SeqCst);
    }

    fn unregister_waiter(&self, _waker: &Waker) {
        self.unregistered.store(true, Ordering::SeqCst);
    }

    fn snapshot(&self) -> MemorySnapshot {
        MemorySnapshot::default()
    }

    fn record_event(&self, _event: MemoryEvent) {}
}

#[test]
fn cancelling_pending_reservation_unregisters_its_waker() {
    let coordinator = Arc::new(CancellationCoordinator::default());
    let coordinator_trait: Arc<dyn MemoryCoordinator> = coordinator.clone();
    let mut future = Box::pin(reserve(
        coordinator_trait,
        ReservationRequest::new(consumer("cancel", MemoryClass::Queue), 64).unwrap(),
    ));
    assert!(matches!(futures_poll(future.as_mut()), Poll::Pending));
    assert!(coordinator.registered.load(Ordering::SeqCst));
    assert!(!coordinator.unregistered.load(Ordering::SeqCst));

    drop(future);
    assert!(coordinator.unregistered.load(Ordering::SeqCst));
}

#[derive(Default)]
struct WakeCounter(AtomicUsize);

impl std::task::Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn poll_with_waker<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(waker))
}

#[test]
fn shared_task_waker_remains_registered_until_every_reservation_leaves() {
    let coordinator = Arc::new(DeterministicMemoryCoordinator::new(64, BTreeMap::new()).unwrap());
    let held = coordinator
        .try_reserve(&ReservationRequest::new(consumer("held", MemoryClass::Queue), 64).unwrap())
        .unwrap()
        .unwrap();
    let counter = Arc::new(WakeCounter::default());
    let waker = Waker::from(Arc::clone(&counter));
    let coordinator_trait: Arc<dyn MemoryCoordinator> = coordinator.clone();
    let mut first = Box::pin(reserve(
        Arc::clone(&coordinator_trait),
        ReservationRequest::new(consumer("first", MemoryClass::Queue), 64).unwrap(),
    ));
    let mut second = Box::pin(reserve(
        coordinator_trait,
        ReservationRequest::new(consumer("second", MemoryClass::Queue), 64).unwrap(),
    ));
    assert!(matches!(
        poll_with_waker(first.as_mut(), &waker),
        Poll::Pending
    ));
    assert!(matches!(
        poll_with_waker(second.as_mut(), &waker),
        Poll::Pending
    ));

    drop(first);
    drop(held);
    assert_eq!(counter.0.load(Ordering::SeqCst), 1);
    assert!(matches!(
        poll_with_waker(second.as_mut(), &waker),
        Poll::Ready(Ok(_))
    ));
}

#[test]
fn reservation_reregisters_when_woken_capacity_is_taken_before_repoll() {
    let coordinator = Arc::new(DeterministicMemoryCoordinator::new(64, BTreeMap::new()).unwrap());
    let held = coordinator
        .try_reserve(&ReservationRequest::new(consumer("held", MemoryClass::Queue), 64).unwrap())
        .unwrap()
        .unwrap();
    let counter = Arc::new(WakeCounter::default());
    let waker = Waker::from(Arc::clone(&counter));
    let coordinator_trait: Arc<dyn MemoryCoordinator> = coordinator.clone();
    let request = ReservationRequest::new(consumer("waiter", MemoryClass::Queue), 64).unwrap();
    let mut future = Box::pin(reserve(coordinator_trait, request.clone()));
    assert!(matches!(
        poll_with_waker(future.as_mut(), &waker),
        Poll::Pending
    ));

    drop(held);
    assert_eq!(counter.0.load(Ordering::SeqCst), 1);
    let competitor = coordinator.try_reserve(&request).unwrap().unwrap();
    assert!(matches!(
        poll_with_waker(future.as_mut(), &waker),
        Poll::Pending
    ));
    drop(competitor);
    assert_eq!(counter.0.load(Ordering::SeqCst), 2);
    assert!(matches!(
        poll_with_waker(future.as_mut(), &waker),
        Poll::Ready(Ok(_))
    ));
}

#[test]
fn oversized_minimum_working_set_fails_without_hold_and_wait() {
    let coordinator = DeterministicMemoryCoordinator::new(128, BTreeMap::new()).unwrap();
    let request = ReservationRequest::new(consumer("wide", MemoryClass::Decode), 129)
        .unwrap()
        .as_minimum_working_set();
    let error = coordinator.try_reserve(&request).unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert_eq!(coordinator.snapshot().current_bytes, 0);
}

#[test]
fn budget_resolution_preserves_native_headroom_and_rejects_unsafe_shape() {
    let resolution = resolve_memory_budget(
        None,
        16 * 1024 * 1024 * 1024,
        64 * 1024 * 1024,
        8 * 1024 * 1024 * 1024,
    )
    .unwrap();
    assert_eq!(
        resolution.process_budget_bytes,
        DEFAULT_PROCESS_BUDGET_BYTES
    );
    assert_eq!(resolution.native_headroom_bytes, 644_245_094);
    assert_eq!(
        resolution.managed_pool_bytes,
        resolution.process_budget_bytes - resolution.native_headroom_bytes
    );
    assert_eq!(resolution.headroom_policy_version, HEADROOM_POLICY_VERSION);
    assert!(resolve_memory_budget(Some(256), 1024, 900, 1).is_err());
}

#[test]
fn unenforced_policy_resolution_does_not_shave_the_default_as_host_authority() {
    let resolution = resolve_unenforced_memory_budget(
        None,
        DEFAULT_PROCESS_BUDGET_BYTES,
        64 * 1024 * 1024,
        DEFAULT_SPILL_BUDGET_BYTES,
    )
    .unwrap();

    assert_eq!(
        resolution.process_budget_bytes,
        DEFAULT_PROCESS_BUDGET_BYTES
    );
    assert_eq!(
        resolution.managed_pool_bytes,
        DEFAULT_PROCESS_BUDGET_BYTES - resolution.native_headroom_bytes
    );
}

#[test]
fn cgroup_memory_report_reads_bounded_unbounded_and_events() {
    let root = unique_temp_dir("cdf-cgroup-memory");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("memory.max"), "1073741824\n").unwrap();
    std::fs::write(root.join("memory.current"), "268435456\n").unwrap();
    std::fs::write(root.join("memory.peak"), "536870912\n").unwrap();
    std::fs::write(
        root.join("memory.events"),
        "low 0\nhigh 1\nmax 2\noom 0\noom_kill 0\n",
    )
    .unwrap();

    let report = cgroup_v2_memory_report_from_root(&root);
    assert_eq!(report.max_bytes, Some(1_073_741_824));
    assert_eq!(report.current_bytes, Some(268_435_456));
    assert_eq!(report.peak_bytes, Some(536_870_912));
    assert_eq!(report.events["high"], 1);
    assert_eq!(report.events["oom_kill"], 0);
    assert!(report.read_errors.is_empty(), "{:?}", report.read_errors);

    std::fs::write(root.join("memory.max"), "max\n").unwrap();
    let unbounded = cgroup_v2_memory_report_from_root(&root);
    assert_eq!(unbounded.max_bytes, None);

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn cgroup_current_resolution_uses_process_scope_not_filesystem_root() {
    assert_eq!(
        parse_cgroup_v2_relative_path("0::/user.slice/user-1000.slice/session-7.scope\n").unwrap(),
        PathBuf::from("user.slice/user-1000.slice/session-7.scope")
    );
    assert_eq!(
        parse_cgroup_v2_relative_path("0::/\n").unwrap(),
        PathBuf::new()
    );
    assert!(parse_cgroup_v2_relative_path("0::/../escape\n").is_err());
    assert!(parse_cgroup_v2_relative_path("1:name=systemd:/not-v2\n").is_err());
}

#[test]
fn cgroup_current_report_reads_the_resolved_scope_files() {
    let root = unique_temp_dir("cdf-current-cgroup");
    let scope = root.join("user.slice/user-1000.slice/session-7.scope");
    std::fs::create_dir_all(&scope).unwrap();
    let proc = root.join("proc-self-cgroup");
    std::fs::write(&proc, "0::/user.slice/user-1000.slice/session-7.scope\n").unwrap();
    std::fs::write(scope.join("memory.max"), "2147483648\n").unwrap();
    std::fs::write(scope.join("memory.current"), "1234\n").unwrap();
    std::fs::write(scope.join("memory.peak"), "5678\n").unwrap();
    std::fs::write(scope.join("memory.events"), "oom 0\noom_kill 0\n").unwrap();

    let report = current_cgroup_v2_memory_report_from(&root, &proc).unwrap();
    assert_eq!(report.root, scope);
    assert_eq!(report.max_bytes, Some(2_147_483_648));
    assert_eq!(report.current_bytes, Some(1234));
    assert_eq!(report.peak_bytes, Some(5678));

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn neutral_crate_has_no_runtime_or_implementation_dependencies() {
    let manifest = include_str!("../Cargo.toml");
    for forbidden in [
        "datafusion",
        "tokio",
        "cdf-project",
        "cdf-engine",
        "cdf-dest-",
        "cdf-runtime",
    ] {
        assert!(
            !manifest.contains(forbidden),
            "forbidden dependency {forbidden}"
        );
    }
}

#[test]
fn telemetry_snapshot_is_json_reportable_with_typed_consumer_keys() {
    let coordinator = DeterministicMemoryCoordinator::new(128, BTreeMap::new()).unwrap();
    let lease = coordinator
        .try_reserve(
            &ReservationRequest::new(consumer("decoder:0", MemoryClass::Decode), 16).unwrap(),
        )
        .unwrap()
        .unwrap();
    let snapshot = coordinator.snapshot();
    let encoded = serde_json::to_string(&snapshot).unwrap();
    let decoded: MemorySnapshot = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded, snapshot);
    drop(lease);
}

#[test]
fn external_operator_profiles_admit_before_poll_and_falsify_understatement() {
    struct ExternalSource {
        profile: OperatorMemoryProfile,
    }
    struct ExternalDestination {
        profile: OperatorMemoryProfile,
    }
    let source = ExternalSource {
        profile: OperatorMemoryProfile::new(32, 64, PressureStrategy::Backpressure, true).unwrap(),
    };
    let destination = ExternalDestination {
        profile: OperatorMemoryProfile::new(64, 128, PressureStrategy::Spill, false).unwrap(),
    };
    let coordinator = Arc::new(DeterministicMemoryCoordinator::new(128, BTreeMap::new()).unwrap());
    let source_request = source
        .profile
        .poll_request(consumer("external-source", MemoryClass::Source))
        .unwrap();
    let source_lease = coordinator.try_reserve(&source_request).unwrap().unwrap();
    assert_eq!(source_lease.bytes(), 64);
    assert!(source.profile.verify_observed_operation(65).is_err());
    assert!(
        coordinator
            .try_reserve(
                &destination
                    .profile
                    .poll_request(consumer("external-destination", MemoryClass::Destination))
                    .unwrap()
            )
            .unwrap()
            .is_none()
    );
    drop(source_lease);
    assert!(
        coordinator
            .try_reserve(
                &destination
                    .profile
                    .poll_request(consumer("external-destination", MemoryClass::Destination))
                    .unwrap()
            )
            .unwrap()
            .is_some()
    );
    assert!(OperatorMemoryProfile::new(1, 2, PressureStrategy::Backpressure, false).is_err());
}

#[test]
fn panic_unwind_and_pending_reservation_cancellation_reconcile() {
    let coordinator = Arc::new(DeterministicMemoryCoordinator::new(64, BTreeMap::new()).unwrap());
    let request = ReservationRequest::new(consumer("panic", MemoryClass::Transform), 64).unwrap();
    let unwind_coordinator = Arc::clone(&coordinator);
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _lease = unwind_coordinator.try_reserve(&request).unwrap().unwrap();
        panic!("intentional unwind");
    }));
    assert!(outcome.is_err());
    assert_eq!(coordinator.snapshot().current_bytes, 0);

    let held = coordinator
        .try_reserve(&ReservationRequest::new(consumer("held", MemoryClass::Queue), 64).unwrap())
        .unwrap()
        .unwrap();
    let coordinator_trait: Arc<dyn MemoryCoordinator> = coordinator.clone();
    let mut pending = Box::pin(reserve(
        coordinator_trait,
        ReservationRequest::new(consumer("cancelled", MemoryClass::Queue), 64).unwrap(),
    ));
    assert!(matches!(futures_poll(pending.as_mut()), Poll::Pending));
    drop(pending);
    assert_eq!(coordinator.snapshot().current_bytes, 64);
    drop(held);
    assert_eq!(coordinator.snapshot().current_bytes, 0);
}

#[test]
fn blocking_reservation_parks_until_release_without_async_runtime() {
    let coordinator = Arc::new(DeterministicMemoryCoordinator::new(64, BTreeMap::new()).unwrap());
    let held = coordinator
        .try_reserve(&ReservationRequest::new(consumer("held", MemoryClass::Queue), 64).unwrap())
        .unwrap()
        .unwrap();
    let waiter: Arc<dyn MemoryCoordinator> = coordinator.clone();
    let worker = std::thread::spawn(move || {
        reserve_blocking(
            waiter,
            &ReservationRequest::new(consumer("worker", MemoryClass::Discovery), 64).unwrap(),
        )
        .unwrap()
    });
    std::thread::sleep(std::time::Duration::from_millis(5));
    assert!(!worker.is_finished());
    drop(held);
    let lease = worker.join().unwrap();
    assert_eq!(lease.bytes(), 64);
    drop(lease);
    assert_eq!(coordinator.snapshot().current_bytes, 0);
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "{prefix}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
