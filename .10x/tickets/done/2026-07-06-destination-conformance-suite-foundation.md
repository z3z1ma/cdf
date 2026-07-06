Status: done
Created: 2026-07-06
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-conformance-chaos-golden.md
Depends-On: .10x/tickets/done/2026-07-05-duckdb-destination.md, .10x/tickets/done/2026-07-05-parquet-object-store-destination.md, .10x/tickets/done/2026-07-06-checkpoint-store-conformance-suite.md

# Implement destination conformance suite foundation

## Scope

Implement the first reusable `firn-conformance` destination suite over the public destination sheet and dry-run planning contract, and consume it from the MVP local destination crates that are already implemented: DuckDB and Parquet/object-store.

Owns `crates/firn-conformance/**` plus the smallest necessary DuckDB/Parquet destination test integration. Keep `crates/firn-conformance/src/lib.rs` thin by adding focused destination modules rather than expanding the crate root.

## Acceptance criteria

- `firn-conformance` exposes a reusable destination conformance harness that accepts a `DestinationProtocol` candidate and representative `DestinationCommitRequest` cases without depending on private destination internals.
- The suite asserts destination sheets tell the truth about supported dispositions by requiring each declared disposition to plan successfully and requiring unsupported MVP dispositions to return an error.
- The suite asserts `CommitPlan` values preserve request target, disposition, idempotency support, migrations, and mechanically derived delivery guarantees for append, replace, merge, and `cdc_apply` according to `.10x/specs/destination-receipts-guarantees.md`.
- The suite asserts destination sheets include identifier rules, concurrency limits, migration support, quarantine-table support, and at least one type mapping with explicit fidelity rather than empty folklore.
- The suite includes negative self-tests with deliberately faulty `DestinationProtocol` implementations so the harness is proven to catch false sheet claims, wrong idempotency, wrong target/disposition echoing, wrong delivery guarantees, and missing type-mapping evidence.
- DuckDB destination tests consume the reusable harness for append, replace, and merge planning and retain destination-specific receipt verification coverage through the public DuckDB commit/verify API.
- Parquet/object-store destination tests consume the reusable harness for append and replace planning, prove merge/`cdc_apply` unsupported behavior is honest, and retain destination-specific manifest receipt verification coverage through the public Parquet commit/verify API.
- Existing destination-specific tests for physical writes, idempotency mirrors/manifests, tampered receipts, type mappings, and identifier behavior remain in place; do not weaken them to make the harness fit.

## Evidence expectations

Record targeted `cargo test -p firn-conformance --locked --no-fail-fast`, `cargo test -p firn-dest-duckdb --locked --no-fail-fast`, `cargo test -p firn-dest-parquet --locked --no-fail-fast`, `cargo clippy -p firn-conformance --all-targets --locked -- -D warnings`, `cargo clippy -p firn-dest-duckdb --all-targets --locked -- -D warnings`, `cargo clippy -p firn-dest-parquet --all-targets --locked -- -D warnings`, `cargo fmt --all -- --check`, and the required `QUALITY.md` closure checks. Mutation testing should include `firn-conformance` plus at least one downstream destination consumer so the reusable harness is part of the mutation oracle.

## Explicit exclusions

No live Postgres conformance execution until `.10x/tickets/2026-07-05-postgres-destination.md` is unblocked. No chaos killpoints, golden-package fixtures, resource conformance suite, new destination production behavior, new generic finalize trait, or changes to destination receipt semantics.

## References

- `firn-the-book-of-the-system.md` Chapter 13 and Chapter 19.
- `.10x/specs/conformance-governance-roadmap.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/knowledge/rust-crate-organization.md`
- `.10x/knowledge/quality-gate-execution.md`
- `.10x/tickets/2026-07-05-conformance-chaos-golden.md`
- `.10x/tickets/done/2026-07-05-duckdb-destination.md`
- `.10x/tickets/done/2026-07-05-parquet-object-store-destination.md`
- `.10x/tickets/done/2026-07-06-checkpoint-store-conformance-suite.md`

## Progress and notes

- 2026-07-06: Split from the conformance parent after inspecting the book, conformance governance spec, destination receipts spec, current `DestinationProtocol`, existing `firn-conformance` checkpoint-store structure, and active blockers. The common kernel trait currently covers sheet and dry-run planning; destination-specific public APIs still own commit and receipt verification coverage.
- 2026-07-06: Parent marked the child active for worker implementation. The worker owns the scoped conformance harness and DuckDB/Parquet consumer test integration; parent owns final review, quality evidence, closure records, and commit.
- 2026-07-06: Implemented reusable `firn-conformance::destination` harness in `crates/firn-conformance/src/destination/mod.rs`; `lib.rs` remains a thin module export. DuckDB and Parquet tests consume the harness through dev-dependencies while retaining destination-specific physical commit and receipt verification tests.
- 2026-07-06: Review found and repaired a vacuous migration-support assertion and a clippy duplicate-branch lint in the faulty self-test implementation.
- 2026-07-06: Closure evidence recorded in `.10x/evidence/2026-07-06-destination-conformance-suite-foundation.md`; review recorded in `.10x/reviews/2026-07-06-destination-conformance-suite-foundation-review.md`.

## Blockers

None for the DuckDB/Parquet destination-conformance foundation. Supply-chain policy gaps from `cargo deny check` and `cargo vet` remain separately owned by `.10x/tickets/2026-07-06-ratify-supply-chain-policy.md`.
