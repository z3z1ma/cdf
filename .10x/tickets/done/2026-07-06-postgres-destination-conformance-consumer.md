Status: done
Created: 2026-07-06
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-conformance-chaos-golden.md
Depends-On: .10x/tickets/done/2026-07-06-destination-conformance-suite-foundation.md, .10x/tickets/done/2026-07-06-postgres-live-execution.md

# Add Postgres destination conformance consumer

## Scope

Make Postgres a first-class consumer of the existing destination conformance suite now that `.10x/tickets/done/2026-07-06-postgres-live-execution.md` closed the driver-backed commit path.

Owns the smallest necessary changes to `crates/cdf-dest-postgres/**`, `crates/cdf-conformance/**` only if the reusable destination harness needs a strictly general public helper, and this ticket's evidence/review records. Keep crate roots thin.

## Acceptance criteria

- `cdf-dest-postgres` consumes `cdf-conformance::destination::assert_destination_conformance` through a dev-dependency, matching the DuckDB and Parquet destination pattern.
- The Postgres conformance case covers every disposition declared by the Postgres destination sheet: append, replace, and merge.
- The conformance case validates Postgres dry-run planning through the public `DestinationProtocol` surface without depending on private Postgres internals.
- The conformance case accounts for Postgres's expected system-table migration records instead of making an empty-migration assumption.
- The existing Postgres live tests remain in place and continue to prove append duplicate/no-op behavior, replace, merge, receipt verification, state/load mirrors, rollback, and decimal fidelity.
- If any existing Postgres live test behavior contradicts the generic destination conformance harness, the ticket must stop as blocked and record whether the sheet, conformance harness, or Postgres behavior is wrong.
- Parent conformance progress notes distinguish this planning-level Postgres conformance consumer from the already-closed live execution evidence and from still-open full lifecycle chaos/MVP demo work.

## Evidence expectations

Record targeted tests and quality gates sufficient for this conformance slice:

- `cargo test -p cdf-dest-postgres --locked --no-fail-fast`
- a focused live-test command whose output proves live Postgres tests actually ran rather than silently returning early because no `TEST_DATABASE_URL`, `initdb`, or `pg_ctl` was available
- `cargo test -p cdf-conformance --locked --no-fail-fast`
- `cargo clippy -p cdf-dest-postgres --all-targets --locked -- -D warnings`
- `cargo clippy -p cdf-conformance --all-targets --locked -- -D warnings`
- `cargo fmt --all -- --check`
- `git diff --check -- . ':(exclude).gitignore'`
- relevant supply-chain checks after any manifest/lockfile change, including `cargo deny check`, `cargo audit`, `osv-scanner`, `cargo vet`, and the direct `unsafe` scan
- reused CodeQL evidence through `tools/codeql-rust-quality.sh` if Rust source or manifests change; do not create a disposable CodeQL DB

Use mutation testing on the changed conformance-facing code if the implementation adds reusable harness logic. If the slice only adds a downstream consumer test, mutation testing may be limited to the destination/conformance targeted tests and recorded as not adding a new reusable oracle.

## Explicit exclusions

No new Postgres production commit behavior, no new destination semantics, no CDC/`cdc_apply`, no concurrent writer stress, no CLI changes, no project runtime changes, no full process-kill chaos layer, no MVP killer-demo harness, and no changes to the current DuckDB-backed Parquet policy.

## References

- `VISION.md` Chapter 13 and Chapter 19.
- `.10x/specs/conformance-governance-roadmap.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/tickets/2026-07-05-conformance-chaos-golden.md`
- `.10x/tickets/done/2026-07-06-destination-conformance-suite-foundation.md`
- `.10x/tickets/done/2026-07-06-postgres-live-execution.md`
- `.10x/evidence/2026-07-06-postgres-live-execution.md`
- `.10x/knowledge/rust-crate-organization.md`
- `.10x/knowledge/quality-gate-execution.md`

## Progress and notes

- 2026-07-06: Opened after re-reading the book, active conformance parent, destination conformance foundation, Postgres live execution evidence, and current `cdf-conformance`/`cdf-dest-postgres` source. This is the smallest unblocked conformance step because Postgres already has live execution but has not yet been wired into the reusable destination conformance harness.
- 2026-07-06: Activated for worker implementation. Write scope is expected to be limited to `crates/cdf-dest-postgres/Cargo.toml` and `crates/cdf-dest-postgres/src/tests.rs` unless the reusable conformance harness proves too narrow, in which case the ticket must record why before broadening.
- 2026-07-06: Implemented the Postgres consumer test through `cdf-conformance`, including expected system-table migrations from `plan_commit`; updated `Cargo.lock` offline for the local dev-dependency. Evidence is recorded in `.10x/evidence/2026-07-06-postgres-destination-conformance-consumer.md`; review is recorded in `.10x/reviews/2026-07-06-postgres-destination-conformance-consumer-review.md`.
- 2026-07-06: Closed after focused Postgres/conformance tests, live Postgres tests, nextest, check, clippy, formatting, diff check, cargo deny/audit/vet, OSV, Semgrep, CodeQL, gitleaks, unsafe scan, machete, and udeps passed.

## Blockers

None.
