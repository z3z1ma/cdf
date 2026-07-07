Status: done
Created: 2026-07-06
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-conformance-chaos-golden.md
Depends-On: .10x/tickets/done/2026-07-06-package-replay-commit-gate-runtime.md, .10x/tickets/done/2026-07-05-duckdb-destination.md, .10x/tickets/done/2026-07-05-checkpoint-store-sqlite.md, .10x/tickets/done/2026-07-05-package-builder-reader.md

# Implement prepared-package chaos conformance foundation

## Scope

Implement the first reusable chaos/recovery conformance slice for the normative package/checkpoint crash matrix: prepared Arrow IPC packages replayed into DuckDB with durable SQLite checkpoint state.

Own `crates/cdf-conformance/**` for the reusable harness and focused self-tests. The expected home is a focused module such as `crates/cdf-conformance/src/package_replay/` exported from the thin `cdf-conformance` crate root. The worker may add scoped conformance dependencies on `cdf-project`, `cdf-package`, `cdf-dest-duckdb`, `cdf-state-sqlite`, Arrow test crates, and `tempfile` as needed to exercise the public runtime primitive. The worker must not modify production runtime behavior unless the existing public API cannot express the invariant; any such need must be recorded as a blocker before editing outside `crates/cdf-conformance/**`.

This child intentionally uses the `PreparedDuckDbReplayRequest::after_receipt_verified` hook from `.10x/tickets/done/2026-07-06-package-replay-commit-gate-runtime.md` to simulate the committed-before-checkpointed crash window. A narrow test helper subprocess that exits or is killed at that hook is in scope so the harness exercises durable package, DuckDB, and SQLite state across a process boundary. A general process-kill chaos runner is out of scope.

## Acceptance criteria

- `cdf-conformance` exposes a reusable prepared-package DuckDB chaos/replay harness over public APIs: `PackageBuilder`/`PackageReader`, `DuckDbDestination`, `CheckpointStore`, and the `cdf-project` prepared replay/recovery runtime.
- The harness creates a deterministic replayable package fixture with at least one Arrow IPC segment and a caller-supplied `StateDelta`; it must not infer `StateDelta`, target, disposition, schema hash, or merge keys from package names or file paths.
- The harness proves the `packaged` with no receipts boundary: replay commits DuckDB destination state, records a durable receipt, commits the checkpoint head, marks the package `checkpointed`, and leaves no source contact requirement.
- The harness proves replay identity: re-driving the same package into the same DuckDB database returns duplicate/no-op receipt behavior and leaves only one destination load mirror entry.
- The harness proves the committed-before-checkpointed boundary across a helper-process boundary: a hook exit/kill after durable receipt verification leaves the package receipt, DuckDB `_cdf_loads`, and DuckDB `_cdf_state` evidence durable; leaves the SQLite checkpoint uncommitted/non-head; and recovery with the supplied durable receipt commits the checkpoint without another destination write.
- The harness proves bad recovery inputs fail closed: tampered/missing receipt acknowledgements or failed destination receipt verification do not commit a checkpoint head.
- Final recovery assertions compare checkpoint output position, package hash, schema hash, segment ids, row counts, and byte counts against the durable receipt and DuckDB mirror evidence.
- The harness includes negative self-tests or deliberately corrupted cases that would fail if the reusable harness skipped receipt durability, checkpoint-head, duplicate-replay, or no-second-destination-write assertions.
- `crates/cdf-conformance/src/lib.rs` remains a thin module/export root; do not grow a monolithic crate root.

## Evidence expectations

Record focused `cargo fmt --all -- --check`, `cargo test -p cdf-conformance --locked --no-fail-fast`, `cargo clippy -p cdf-conformance --all-targets --locked -- -D warnings`, `cargo test -p cdf-project --locked --no-fail-fast`, and `git diff --check`.

Because this is a reusable conformance harness for a commit-gate invariant, run bounded mutation testing over the new conformance module when feasible, with `cdf-conformance` as the test oracle and downstream runtime/project tests included if practical. If mutation tooling is structurally blocked or too slow, record the exact limit and harden with negative self-tests before closure.

Significant closure must follow `QUALITY.md`. Reuse the CodeQL database path from `.10x/knowledge/quality-gate-execution.md` and parallelize independent checks where practical.

## Explicit exclusions

No general process-kill chaos runner beyond the narrow helper-process test needed for this hook, no CLI `resume` command, no CLI `replay package` command, no `cdf run` orchestration, no live source extraction, no generic destination finalize trait, no Postgres/Parquet replay chaos, no persisted golden-package fixture suite, no package archive behavior, no package GC retention policy, no CI workflow changes, no MVP killer-demo harness, and no post-checkpoint package-status failure hook.

The broader `.10x/tickets/2026-07-05-conformance-chaos-golden.md` parent still owns full lifecycle killpoints, golden-package determinism, MVP killer-demo evidence, resource data completeness, live Postgres conformance, and cross-destination chaos.

## References

- `VISION.md` Chapter 11 lifecycle/crash matrix, Chapter 12 commit-gate invariant, Chapter 13 receipt verification/replay idempotency, Chapter 19 chaos/replay identity, and Chapter 22 MVP killer demo.
- `.10x/specs/conformance-governance-roadmap.md`
- `.10x/specs/package-lifecycle-determinism.md`
- `.10x/specs/checkpoint-state-commit-gate.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/knowledge/rust-crate-organization.md`
- `.10x/knowledge/quality-gate-execution.md`
- `.10x/tickets/2026-07-05-conformance-chaos-golden.md`
- `.10x/tickets/done/2026-07-06-package-replay-commit-gate-runtime.md`
- `.10x/evidence/2026-07-06-package-replay-commit-gate-runtime.md`
- `.10x/reviews/2026-07-06-package-replay-commit-gate-runtime-review.md`
- `.10x/evidence/2026-07-06-prepared-package-chaos-conformance.md`
- `.10x/reviews/2026-07-06-prepared-package-chaos-conformance-review.md`

## Progress and notes

- 2026-07-06: Split from the conformance/chaos/golden parent after the prepared-package runtime child closed. Current source has the exact public hook needed to simulate the committed-before-checkpointed window without source contact. Explorer Hubble recommended making the first chaos slice a narrow helper-process test so durable package, DuckDB, and SQLite state are proven across a real process boundary while still excluding a general process-kill chaos runner. This child consumes the runtime primitive from `cdf-conformance` rather than duplicating sequencing in product code.
- 2026-07-06: Do not implement in the ticket-creation turn. Assign to a worker in a later turn with the references above and a write boundary of `crates/cdf-conformance/**` plus dependency metadata needed only for that crate.
- 2026-07-06: Parent activated the ticket and is assigning implementation to a worker. Worker owns the scoped conformance harness, focused self-tests, scoped `cdf-conformance` dependency metadata, and focused verification. Parent owns final integration review, broader `QUALITY.md` evidence, ticket graph updates, and commit.
- 2026-07-06: Worker implemented `crates/cdf-conformance/src/package_replay/` as a focused prepared-package DuckDB/SQLite replay harness exported from the thin crate root. The harness builds a deterministic Arrow IPC package fixture, keeps `StateDelta` caller-supplied, wraps the public `cdf-project` replay/recovery runtime, and adds assertions for durable package receipts, checkpoint heads, duplicate/no-op replay identity, DuckDB mirror evidence, and recovery without a second destination write. Focused tests cover packaged/no-receipts success, duplicate replay, current-test-binary helper-process exit at `after_receipt_verified`, durable SQLite recovery, failed recovery inputs, and negative self-tests for harness assertion gaps. Initial `cargo test -p cdf-conformance --offline --no-fail-fast` passed after updating `Cargo.lock` for scoped conformance dependency edges; locked verification is still pending.
- 2026-07-06: Parent verification completed. Focused gates passed: `cargo fmt --all -- --check`, `git diff --check`, `cargo test -p cdf-conformance --locked --no-fail-fast`, `cargo clippy -p cdf-conformance --all-targets --locked -- -D warnings`, `cargo test -p cdf-project --locked --no-fail-fast`, and downstream package/DuckDB/SQLite tests. Broader `QUALITY.md` gates passed where structurally available, including workspace checks, clippy feature matrices, workspace tests, nextest, docs, coverage, audit/deny/vet/OSV/Semgrep/gitleaks, reusable CodeQL analysis at `target/quality/codeql-db-rust`, machete, udeps, semver-checks, rust-code-analysis, jscpd, tokei, scc, direct owned unsafe scan, and `cargo careful`. Kani, fuzz listing, and Geiger are recorded as tool/infrastructure limits rather than implementation failures in `.10x/evidence/2026-07-06-prepared-package-chaos-conformance.md`.
- 2026-07-06: Mutation testing over `crates/cdf-conformance/src/package_replay/mod.rs` passed with 36 mutants tested, 20 caught, 16 unviable, 0 missed, and 0 timed out. A parent review found no blocking issues in `.10x/reviews/2026-07-06-prepared-package-chaos-conformance-review.md`. Acceptance criteria are fully mapped to evidence, so this child is closed.

## Blockers

None for the prepared-package DuckDB/SQLite chaos conformance foundation.
