Status: done
Created: 2026-07-06
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-conformance-chaos-golden.md
Depends-On: .10x/tickets/done/2026-07-05-checkpoint-store-sqlite.md

# Implement checkpoint store conformance suite

## Scope

Implement the first reusable `cdf-conformance` suite: a public checkpoint-store conformance harness that future store implementations can run against the same commit-gate contract as the MVP in-memory and SQLite stores.

Owns `crates/cdf-conformance/**`, the smallest necessary `crates/cdf-state-sqlite/**` test integration, and its own evidence/review records. Keep `src/lib.rs` thin by splitting conformance support into focused modules.

## Acceptance criteria

- `cdf-conformance` exposes a reusable checkpoint-store conformance function or harness that accepts a fresh-store factory and exercises the public `CheckpointStore` trait without depending on private test hooks.
- The suite asserts that commits require receipts covering package hash, schema hash, every state segment, and segment row/byte counts.
- The suite asserts that proposed and abandoned checkpoints never become committed heads.
- The suite asserts committed head lookup, history ordering, resource isolation, and scope isolation.
- The suite asserts rewind rejects invalid targets, appends a rewind marker without deleting history, moves the head to the committed target, and reports packages ahead of state from the current branch.
- The suite includes a public compile-time or test helper proving candidate stores satisfy the `Send + Sync` `CheckpointStore` bound.
- MVP in-memory and SQLite stores run through the reusable conformance suite in tests.
- SQLite-specific tests remain responsible for SQLite-only guarantees such as WAL mode, unique-head index, cross-connection transactional uniqueness, and scalar-row corruption rejection; do not weaken or delete those tests.

## Evidence expectations

Record targeted `cargo test -p cdf-conformance --locked --no-fail-fast`, `cargo test -p cdf-state-sqlite --locked --no-fail-fast`, `cargo clippy -p cdf-conformance --all-targets --locked -- -D warnings`, `cargo clippy -p cdf-state-sqlite --all-targets --locked -- -D warnings`, and `cargo fmt --all -- --check` output. Record broader quality checks required before parent commit.

## Explicit exclusions

No resource conformance suite, destination conformance suite, chaos killpoints, golden package fixture generation, production store behavior changes, new checkpoint-store implementations, or SQLite schema changes.

## References

- `.10x/specs/checkpoint-state-commit-gate.md`
- `.10x/specs/conformance-governance-roadmap.md`
- `.10x/tickets/2026-07-05-conformance-chaos-golden.md`
- `.10x/tickets/done/2026-07-05-checkpoint-store-sqlite.md`
- `.10x/reviews/2026-07-06-checkpoint-store-sqlite-review.md`

## Progress and notes

- 2026-07-06: Split from the conformance parent after inspecting the checkpoint-store spec, existing in-memory/SQLite store tests, and the prior checkpoint-store review. This child lifts shared store-contract assertions into `cdf-conformance` without changing product semantics.
- 2026-07-06: Worker A began implementation after reading the ticket, referenced specs/tickets/review, `crates/cdf-conformance/**`, checkpoint store implementations/tests, and relevant kernel checkpoint/destination/id/position/scope types.
- 2026-07-06: Implemented public `cdf_conformance::checkpoint_store` harness over `CheckpointStore`, added the `Send + Sync` helper, and wired both `InMemoryCheckpointStore` and `SqliteCheckpointStore` through the suite from `cdf-state-sqlite` tests while preserving SQLite-only tests.
- 2026-07-06: Required verification passed: `cargo fmt --all -- --check`; `cargo test -p cdf-conformance --locked --no-fail-fast`; `cargo test -p cdf-state-sqlite --locked --no-fail-fast`; `cargo clippy -p cdf-conformance --all-targets --locked -- -D warnings`; `cargo clippy -p cdf-state-sqlite --all-targets --locked -- -D warnings`. Evidence recorded in `.10x/evidence/2026-07-06-checkpoint-store-conformance-suite.md`; review recorded in `.10x/reviews/2026-07-06-checkpoint-store-conformance-suite.md`.
- 2026-07-06: Parent review found that the reusable harness needed negative self-tests so mutation testing could prove the harness itself was not a no-op. Added faulty-store self-tests and hardened receipt row/byte count assertions in both overreported and underreported directions. Final bounded mutation run over the conformance module reported 28 mutants tested: 18 caught, 10 unviable, 0 missed.
- 2026-07-06: Full quality sweep recorded in `.10x/evidence/2026-07-06-checkpoint-store-conformance-suite.md`. Targeted checks, workspace checks, tests, nextest, docs, coverage, clippy variants, cargo-hack feature matrix, dependency hygiene, security scanners, CodeQL, and source-only secret scanning pass. Existing supply-chain policy blockers remain outside this child and owned by `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`.

## Blockers

None.
