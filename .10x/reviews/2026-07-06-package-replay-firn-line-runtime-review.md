Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-06-package-replay-firn-line-runtime.md
Verdict: pass

# Prepared package firn-line runtime review

## Target

Review of the prepared-package DuckDB/SQLite runtime primitive in `crates/firn-project/src/runtime.rs`, its focused tests, and the scoped `firn-project` dependency additions.

## Assumptions tested

- Package validation must finish before checkpoint or destination mutation.
- Runtime inputs must be explicit; package filenames, package ids, and package artifacts must not silently define `StateDelta`, schema hash, target, disposition, merge keys, scope, or output position.
- New destination work must happen only after checkpoint proposal, and checkpoint commit must happen only after a durable verified receipt.
- Destination duplicate replay must reuse the durable receipt and still allow a new store checkpoint commit for a new checkpoint store.
- Recovery must be possible from the package/destination window without opening the source or applying another destination write.
- Receipt identity, schema hash, target, disposition, idempotency token, segment ids, row counts, and byte counts must all match before checkpoint commit.
- Pre-receipt destination failure must leave no committed checkpoint head.
- Checkpoint failure after receipt durability must not pretend state advanced, and the receipt must remain recoverable.
- The crate root must remain thin and non-monolithic.

## Findings

No unresolved findings.

The implementation follows the ticket scope and does not introduce a generic destination abstraction or CLI plumbing. Tests cover success, duplicate/idempotent replay, supplied-receipt recovery without a second destination write, receipt verification failure, receipt identity mismatch, missing ack, wrong ack counts, bad package hash, segment mismatch, non-replayable package rejection before mutation, destination failure before receipt with checkpoint abandon, and checkpoint failure after durable receipt with later recovery.

Review specifically checked the destination dependency: `DuckDbDestination::commit_package` records `_firn_loads` receipt mirrors transactionally with destination mutation before returning the receipt, and `verify_receipt` compares the supplied receipt to the stored JSON. The runtime verifies this receipt before calling `CheckpointStore::commit`.

The dependency additions are scoped to `firn-project` orchestration (`firn-package`, `firn-dest-duckdb`) and test support (`firn-state-sqlite`, Arrow, tempfile). `cargo machete` and `cargo +nightly udeps` both found no unused dependencies.

## Verdict

Pass. Acceptance criteria are covered by focused tests, workspace checks, mutation testing, security/supply-chain scans, CodeQL, Careful, Geiger/source unsafe inventory, and parent review. `crates/firn-project/src/lib.rs` remains a module/export root rather than a monolithic implementation file.

## Residual risk

Miri cannot execute the targeted runtime test because it reaches `rusqlite` native SQLite FFI after filesystem isolation is disabled. This is mitigated by no new owned unsafe code, direct unsafe inventory, Geiger zero-unsafe counts for `firn-project` and first-party dependencies in the scanned surface, and `cargo +nightly careful` passing all 24 `firn-project` tests.

Post-checkpoint package status write failure is not separately simulated in this primitive; the function would surface the package status write error after the checkpoint store has already committed. That crash/status boundary belongs with the broader lifecycle/chaos work in `.10x/tickets/2026-07-05-conformance-chaos-golden.md`, which remains open.
