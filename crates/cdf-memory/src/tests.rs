use std::{
    collections::BTreeMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use arrow_array::{Int64Array, RecordBatch};
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
    let payload = AccountedBytes::new(Arc::from(vec![7_u8; 64]), lease).unwrap();
    let clone = payload.clone();
    assert_eq!(coordinator.snapshot().current_bytes, 64);
    drop(payload);
    assert_eq!(coordinator.snapshot().current_bytes, 64);
    drop(clone);
    assert_eq!(coordinator.snapshot().current_bytes, 0);
    assert_eq!(coordinator.snapshot().peak_bytes, 128);
}

#[test]
fn accounted_batch_reconciles_reservation_and_error_drops_release() {
    let coordinator = DeterministicMemoryCoordinator::new(4096, BTreeMap::new()).unwrap();
    let request = ReservationRequest::new(consumer("batch", MemoryClass::Transform), 2048).unwrap();
    let lease = coordinator.try_reserve(&request).unwrap().unwrap();
    let batch =
        RecordBatch::try_from_iter([("value", Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as _)])
            .unwrap();
    let observed = batch.get_array_memory_size() as u64;
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

fn futures_poll<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
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
