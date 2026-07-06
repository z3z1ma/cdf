Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-05-checkpoint-store-sqlite.md
Verdict: pass

# Checkpoint store SQLite review

## Target

Implementation and records for `.10x/tickets/done/2026-07-05-checkpoint-store-sqlite.md`, including `firn-kernel` checkpoint contract additions, `firn-state-sqlite` in-memory and SQLite stores, tests, evidence, and spec updates.

## Findings

None blocking remain.

Resolved significant finding: the first worker implementation placed the runtime-neutral checkpoint contract in `firn-state-sqlite`. This violated `.10x/specs/architecture-layering-runtime.md` because higher runtime and CLI crates need the contract without depending on the SQLite implementation. The contract and checkpoint value types now live in `firn-kernel`; `firn-state-sqlite` depends downward on them.

Resolved significant finding: `SqliteCheckpointStore::connection(&self) -> &Connection` exposed a public bypass around the firn-line invariant. The public accessor was removed. SQLite tests inspect the private connection only from the module test submodule.

Resolved significant finding: book section 12.5 ratified `trait CheckpointStore: Send + Sync` with shared receivers. The trait now uses shared receivers, both stores hide mutation behind `Mutex` or SQLite transactions, and tests assert both store types satisfy `CheckpointStore + Send + Sync`.

Resolved significant finding: parent mutation testing initially reported 31 missed checkpoint-store mutants. The test suite was hardened for in-memory/SQLite parity, bad receipt coverage, abandon/head behavior, tuple isolation, rewind validation, branch-lineage packages ahead, SQLite row corruption, timestamp sanity, and unsupported state versions. The final parent rerun reported 111 mutants tested, 74 caught, 37 unviable, and 0 missed.

Minor residual: `cargo deny check` and `cargo vet` are not clean because repository supply-chain policy is not ratified. This is not specific to the checkpoint implementation and is owned by `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`.

Minor residual: `jscpd` reports similarity in existing prose and some Rust test/helper patterns. The Rust clone rate is low, and the repeated checkpoint-store tests intentionally exercise the same contract over both store implementations. No refactor is recommended now because abstraction would make the failure cases less legible.

## Evidence Reviewed

- `.10x/evidence/2026-07-06-checkpoint-store-sqlite.md`
- `.10x/evidence/2026-07-06-checkpoint-quality-gates.md`
- `.10x/specs/checkpoint-state-firn-line.md`
- `firn-the-book-of-the-system.md` section 12.5
- `crates/firn-kernel/src/lib.rs`
- `crates/firn-state-sqlite/src/lib.rs`

## Verdict

Pass. The ticket acceptance criteria are implemented and covered by recorded evidence. Residual repository-level supply-chain policy work has a separate owner and does not require keeping this child ticket open.

## Residual Risk

The checkpoint conformance suite is not yet a reusable cross-store harness; that broader work remains owned by `.10x/tickets/2026-07-05-conformance-chaos-golden.md`. Destination mirror recovery is outside this ticket and remains governed by `.10x/specs/checkpoint-state-firn-line.md` and future destination/CLI tickets.
