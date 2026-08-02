use super::{
    Arc, AtomicUsize, CdfError, ContractRef, Duration, InMemoryScopeLeaseStore, LOCK_FILE_NAME,
    LeaseOwnerId, LockFileCasFailpoint, Ordering, ScopeKey, ScopeLease, ScopeLeaseClock,
    ScopeLeaseStore, compare_and_swap_lock_file, compare_and_swap_lock_file_with_failpoint,
    compare_and_swap_lock_file_with_publication_hook, fs, lock_file_atomicity_capabilities, mpsc,
    read_lock_file_authority, thread, write_lock_file_guarded,
};

pub(super) fn schema_lease(store: &InMemoryScopeLeaseStore) -> ScopeLease {
    store
        .acquire(
            ScopeKey::SchemaContract {
                contract: ContractRef::new("orders").unwrap(),
            },
            LeaseOwnerId::new("promotion-executor").unwrap(),
            1_000,
        )
        .unwrap()
}

pub(super) struct StaleAtPublicationStore {
    pub(super) checks: AtomicUsize,
}

impl ScopeLeaseStore for StaleAtPublicationStore {
    fn authority_domain_id(&self) -> cdf_kernel::LeaseAuthorityDomainId {
        cdf_kernel::LeaseAuthorityDomainId::new("stale-publication-test").unwrap()
    }

    fn acquire(
        &self,
        _scope: ScopeKey,
        _owner: LeaseOwnerId,
        _lease_duration_ms: u64,
    ) -> cdf_kernel::Result<ScopeLease> {
        unreachable!()
    }

    fn renew(
        &self,
        _lease: &ScopeLease,
        _lease_duration_ms: u64,
    ) -> cdf_kernel::Result<ScopeLease> {
        unreachable!()
    }

    fn release(&self, _lease: &ScopeLease) -> cdf_kernel::Result<()> {
        unreachable!()
    }

    fn assert_current(&self, _lease: &ScopeLease) -> cdf_kernel::Result<()> {
        if self.checks.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(())
        } else {
            Err(CdfError::contract("lease superseded before publication"))
        }
    }

    fn prove_expired(
        &self,
        _lease: &ScopeLease,
        _collector: LeaseOwnerId,
        _cleanup_lease_duration_ms: u64,
    ) -> cdf_kernel::Result<Option<cdf_kernel::ExpiredScopeLeaseProof>> {
        Ok(None)
    }
}

#[test]
fn lock_file_cas_requires_exact_prior_bytes_hash_and_current_fence() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join(LOCK_FILE_NAME);
    fs::write(&path, b"version = 1\n").unwrap();
    let expected = read_lock_file_authority(&path).unwrap();
    let store = InMemoryScopeLeaseStore::new();
    let lease = schema_lease(&store);

    let report =
        compare_and_swap_lock_file(&path, &expected, b"version = 2\n", &store, &lease).unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"version = 2\n");
    assert_eq!(report.installed, read_lock_file_authority(&path).unwrap());
    #[cfg(unix)]
    assert!(report.parent_directory_synced);

    let error =
        compare_and_swap_lock_file(&path, &expected, b"version = 3\n", &store, &lease).unwrap_err();
    assert!(error.message.contains("prior authority changed"));
    assert_eq!(fs::read(&path).unwrap(), b"version = 2\n");

    let mut inconsistent = read_lock_file_authority(&path).unwrap();
    inconsistent.sha256.push_str("tampered");
    let error = compare_and_swap_lock_file(&path, &inconsistent, b"version = 3\n", &store, &lease)
        .unwrap_err();
    assert!(error.message.contains("supplied bytes hash"));
}

#[test]
fn lock_file_cas_failpoints_model_each_crash_boundary() {
    for (failpoint, expected_bytes) in [
        (LockFileCasFailpoint::BeforeTempSync, b"old\n".as_slice()),
        (LockFileCasFailpoint::BeforeRename, b"old\n".as_slice()),
        (LockFileCasFailpoint::AfterRename, b"new\n".as_slice()),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(LOCK_FILE_NAME);
        fs::write(&path, b"old\n").unwrap();
        let expected = read_lock_file_authority(&path).unwrap();
        let store = InMemoryScopeLeaseStore::new();
        let lease = schema_lease(&store);
        let error = compare_and_swap_lock_file_with_failpoint(
            &path,
            &expected,
            b"new\n",
            &store,
            &lease,
            Some(failpoint),
        )
        .unwrap_err();
        assert!(
            error
                .message
                .contains("injected cdf.lock publication crash")
        );
        assert_eq!(fs::read(&path).unwrap(), expected_bytes);
    }
}

#[test]
fn guarded_lock_writer_atomically_creates_replaces_and_rejects_stale_authority() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join(LOCK_FILE_NAME);
    write_lock_file_guarded(&path, None, b"created\n").unwrap();
    let created = read_lock_file_authority(&path).unwrap();
    assert_eq!(created.bytes, b"created\n");

    write_lock_file_guarded(&path, Some(&created), b"replaced\n").unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"replaced\n");
    let error = write_lock_file_guarded(&path, Some(&created), b"stale\n").unwrap_err();
    assert!(error.message.contains("prior authority changed"));
    assert_eq!(fs::read(&path).unwrap(), b"replaced\n");

    let temporary_files = fs::read_dir(temp.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".cas.tmp"))
        .count();
    assert_eq!(temporary_files, 0);
    assert!(
        temp.path()
            .join(".cdf/locks/cdf.lock.mutation.lock")
            .is_file()
    );
}

#[test]
fn guarded_cdf_writer_cannot_enter_cas_final_check_rename_window() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join(LOCK_FILE_NAME);
    fs::write(&path, b"old\n").unwrap();
    let expected = read_lock_file_authority(&path).unwrap();
    let writer_expected = expected.clone();
    let store = InMemoryScopeLeaseStore::new();
    let lease = schema_lease(&store);
    let (at_publication_tx, at_publication_rx) = mpsc::channel();
    let (continue_tx, continue_rx) = mpsc::channel();
    let cas_path = path.clone();
    let cas = thread::spawn(move || {
        compare_and_swap_lock_file_with_publication_hook(
            &cas_path,
            &expected,
            b"cas\n",
            &store,
            &lease,
            || {
                at_publication_tx.send(()).unwrap();
                continue_rx.recv().unwrap();
                Ok(())
            },
        )
    });
    at_publication_rx.recv().unwrap();

    let writer_path = path.clone();
    let (attempting_tx, attempting_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let writer = thread::spawn(move || {
        attempting_tx.send(()).unwrap();
        let result =
            write_lock_file_guarded(&writer_path, Some(&writer_expected), b"ordinary-writer\n");
        done_tx.send(result).unwrap();
    });
    attempting_rx.recv().unwrap();
    assert!(
        done_rx.recv_timeout(Duration::from_millis(100)).is_err(),
        "ordinary CDF writer must block while CAS holds the project mutation guard"
    );

    continue_tx.send(()).unwrap();
    assert_eq!(cas.join().unwrap().unwrap().installed.bytes, b"cas\n");
    let writer_error = done_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .unwrap_err();
    assert!(writer_error.message.contains("prior authority changed"));
    writer.join().unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"cas\n");
}

#[test]
fn stale_fencing_token_cannot_publish_after_temp_file_sync() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join(LOCK_FILE_NAME);
    fs::write(&path, b"old\n").unwrap();
    let expected = read_lock_file_authority(&path).unwrap();
    let real_store = InMemoryScopeLeaseStore::new();
    let lease = schema_lease(&real_store);
    let store = StaleAtPublicationStore {
        checks: AtomicUsize::new(0),
    };

    let error = compare_and_swap_lock_file(&path, &expected, b"new\n", &store, &lease).unwrap_err();
    assert!(error.message.contains("superseded before publication"));
    assert_eq!(fs::read(&path).unwrap(), b"old\n");
}

#[test]
fn lease_expiring_during_temp_write_cannot_publish() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join(LOCK_FILE_NAME);
    fs::write(&path, b"old\n").unwrap();
    let expected = read_lock_file_authority(&path).unwrap();
    struct ExpiringClock(AtomicUsize);

    impl ScopeLeaseClock for ExpiringClock {
        fn now_ms(&self) -> cdf_kernel::Result<i64> {
            Ok(match self.0.fetch_add(1, Ordering::SeqCst) {
                0 => 1_000,
                1 => 1_099,
                _ => 1_100,
            })
        }
    }

    let store = InMemoryScopeLeaseStore::with_clock(Arc::new(ExpiringClock(AtomicUsize::new(0))));
    let lease = store
        .acquire(
            ScopeKey::SchemaContract {
                contract: ContractRef::new("expiring").unwrap(),
            },
            LeaseOwnerId::new("promotion-executor").unwrap(),
            100,
        )
        .unwrap();

    let error = compare_and_swap_lock_file(&path, &expected, b"new\n", &store, &lease).unwrap_err();
    assert!(error.message.contains("expired, released, or superseded"));
    assert_eq!(fs::read(&path).unwrap(), b"old\n");
}

#[test]
fn lock_file_atomicity_capabilities_state_platform_limits() {
    let capabilities = lock_file_atomicity_capabilities();
    assert!(!capabilities.limitation.is_empty());
    assert!(capabilities.cooperating_cdf_writers_serialized);
    assert!(capabilities.limitation.contains("non-cooperating"));
    #[cfg(unix)]
    {
        assert!(capabilities.atomic_rename_over_existing);
        assert!(capabilities.parent_directory_fsync);
        assert!(capabilities.limitation.contains("same Unix filesystem"));
        assert!(capabilities.limitation.contains("POSIX rename"));
    }
    #[cfg(not(unix))]
    {
        assert!(!capabilities.atomic_rename_over_existing);
        assert!(!capabilities.parent_directory_fsync);
    }
}
