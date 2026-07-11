Status: done
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/done/2026-07-10-p2-residual-schema-promotion-program.md
Depends-On: .10x/specs/schema-promotion-corrections.md, .10x/specs/checkpoint-state-commit-gate.md

# P2 RP4 fenced schema-scope lease and atomic lock compare-and-swap

## Scope

Add an executor-neutral fenced lease primitive over `ScopeKey` with in-memory and SQLite implementations, then add crash-safe atomic `cdf.lock` compare-and-swap against exact prior bytes/hash. This supplies promotion concurrency safety and a reusable seam for later distributed stores without implementing a scheduler.

## Acceptance criteria

- Kernel lease values include scope, owner, fencing token, acquired/expiry times, and explicit renew/release semantics.
- Only the current unexpired fencing token may perform guarded promotion publication; stale owners fail closed.
- In-memory/SQLite stores pass shared acquire/contention/expiry/renew/release/fencing conformance.
- Lease persistence/migrations preserve current checkpoint store compatibility and commit-gate APIs.
- Atomic lock CAS writes a temporary file, syncs where supported, rename-over installs, and refuses changed prior bytes/hash.
- Failpoints cover crash before temp sync, before rename, after rename, and stale-token publication.
- Models contain no CLI, local executor, Spark/Flink, or destination-driver dependencies.

## Evidence expectations

Store conformance, concurrent contention tests, failpoint/crash tests, filesystem atomicity evidence/limits, migration fixtures, and coordination review with the distributed execution ticket.

## Explicit exclusions

No worker scheduler, remote lease store, destination correction, promotion planner, or package execution.

## Progress and notes

- 2026-07-10: Opened from the ratified schema-lease requirement; general distributed scheduling remains separately owned.
- 2026-07-10: Added executor-neutral kernel `ScopeLease`/`FencingToken`/`ScopeLeaseStore` contracts with caller-supplied epoch time, explicit acquire/renew/release/current-fence operations, and monotonically increasing per-`ScopeKey` tokens. Added shared conformance covering contention, expiry, renew, release, isolation, and stale fencing for in-memory and SQLite implementations; concurrent SQLite acquisition admits exactly one owner, and reopen tests preserve token history.
- 2026-07-10: Added the independently versioned `scope_lease_store` SQLite component without changing `CheckpointStore`, checkpoint schema version, or receipt-gated commit semantics. Migration tests initialize leases over a committed checkpoint fixture and verify the checkpoint head and receipt remain intact.
- 2026-07-10: Added fenced exact-prior-bytes/SHA-256 `cdf.lock` compare-and-swap in `cdf-project`: same-directory temporary write, file sync, second fence/authority check using a freshly sampled executor-supplied clock, Unix atomic rename-over, and parent-directory sync. Failpoint tests cover before temp sync, before rename, after rename, a fence becoming stale after temp sync, and lease expiry between initial validation and publication. The public capability report records that portable Rust does not guarantee rename-over/directory fsync off Unix and that Unix guarantees still depend on same-filesystem POSIX semantics.
- 2026-07-10: Re-read `.10x/knowledge/source-destination-extension-invariant.md`; kernel semantics contain no filesystem, SQLite, CLI, executor, source, destination, scheduler, or driver types. Persistence remains in `cdf-state-sqlite`; filesystem publication remains in `cdf-project`; both consume the kernel trait.
- 2026-07-10: Verification: `cargo test -p cdf-kernel --lib` passed 14/14; `cargo test -p cdf-state-sqlite --lib` passed 34/34; focused `cdf-project` lock CAS/failpoint/stale-expiry tests passed 5/5; strict `cargo clippy -p cdf-kernel -p cdf-state-sqlite --lib -- -D warnings` and `cargo clippy -p cdf-project --lib -- -D warnings` passed; targeted rustfmt checks passed. The full `cdf-project --lib` run passed 123/124, with only the stale single-file rejection test `local_parquet_discover_autopin_rejects_multi_file_glob_without_snapshot_write` failing because A10c now correctly accepts multi-file discovery; that test inversion is already owned by `.10x/tickets/done/2026-07-09-p2-ws-a10c-exhaustive-local-binary-discovery.md` and is outside RP4.
- 2026-07-10: Parent adversarial review rejected caller-supplied lease time and identified a cooperative-writer TOCTOU between the final lock-byte check and rename. Repaired the lease contract so public acquire/renew/release/assert-current operations accept no timestamp; each store owns its authoritative clock. Production constructors use a system clock, while deterministic conformance injects a store clock and advances it without giving lease callers time authority. This leaves future distributed stores free to use server-authoritative time behind the same kernel trait.
- 2026-07-10: Centralized all production CDF lock mutations under a project-level advisory file lock at `.cdf/locks/cdf.lock.mutation.lock`. Promotion CAS holds the guard from its first fence/authority check through rename and directory sync. Contract freeze, schema pin, discovery scan auto-pin, and `cdf add` now perform guarded writes with the exact prior bytes/hash captured when `ProjectContext` loaded; stale ordinary writers fail their precondition rather than overwriting a newer CAS publication. A deterministic race pauses CAS after its final byte check, proves the ordinary writer blocks, then proves it refuses the changed authority after CAS completes. The capability report now states the unavoidable boundary: non-cooperating editors/processes can ignore an advisory lock and remain outside CDF's serialization protocol.
- 2026-07-10: Repair verification: `cargo test -p cdf-state-sqlite --lib` passed 34/34; focused project CAS/failpoint/stale-expiry tests plus the final-check/rename contention regression passed 6/6; `cargo clippy -p cdf-kernel -p cdf-state-sqlite -p cdf-project -p cdf-cli --lib -- -D warnings` passed before subsequent unrelated RP3 shared-tree edits. Focused CLI lock-writer reruns are pending only until RP3's in-progress destination-protocol changes compile coherently; RP4 does not own or modify that surface.
- 2026-07-10: Final crash-safety review replaced the guarded writer's direct write with the same-directory temporary-write, file-sync, exact-authority recheck, atomic-install, and parent-directory-sync path. Existing lock updates use rename-over; first creation uses hard-link no-clobber semantics. `.cdf/locks` creation is race-safe via `create_dir_all`, and the persistent advisory guard lives at `.cdf/locks/cdf.lock.mutation.lock`, not beside the project-root lockfile. The create/replace/stale-authority regression verifies cleanup and guard placement.
- 2026-07-10: Final rerun after shared-tree stabilization: kernel 14/14, state 34/34, focused project lease/CAS/guard/crash/race tests 7/7, and strict affected-crate clippy all passed. Production CLI mutation-path tests passed for `cdf add`, contract freeze/test, and creation-time schema pin. The pre-existing schema pin/show/diff compatibility test reached the guarded writer successfully but failed its `summary.changed == false` assertion because A10c's fresh discovery observation currently differs; this is unrelated to lock mutation and remains owned by `.10x/tickets/done/2026-07-09-p2-ws-a10c-exhaustive-local-binary-discovery.md`.
- 2026-07-10: Updated the RP4-owned `state_migrate_initializes_sqlite_components_and_is_idempotent` CLI regression to assert all three independently versioned SQLite components (`checkpoint_store`, `run_ledger`, `scope_lease_store`), lease schema version 1, zero second-run applications, three current component rows, and the human three-component rendering. Focused CLI migration, additive migration idempotency, checkpoint-history/receipt commit-gate preservation, strict state/CLI clippy, formatting, and diff checks all passed.
- 2026-07-10: Parent integration verification and P0 extension-cost review passed. Evidence: `.10x/evidence/2026-07-10-p2-a10c-rp3-rp4-integration.md`. Review: `.10x/reviews/2026-07-10-p2-a10c-rp3-rp4-integration-review.md`. Retrospective guidance: `.10x/knowledge/fenced-lease-lock-publication.md`.

## Blockers

None.
