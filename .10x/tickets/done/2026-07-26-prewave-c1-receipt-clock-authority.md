Status: done
Created: 2026-07-26
Updated: 2026-07-26
Parent: `.10x/tickets/2026-07-26-pre-wave-architecture-hardening-program.md`

# Centralize receipt assembly and commit time

## Scope

Add one validated ordinary/correction receipt assembly path to `cdf-package-contract` and migrate
DuckDB, Postgres, and Parquet. Bind destination commit/correction timestamps to the injected
execution host's Unix clock and remove destination-local process-wall-clock helpers.

## Non-goals

- No receipt schema/version, verify-clause, transaction metadata, or id derivation change.
- No clock in kernel and no recomputation of recorded time during replay.
- No change to destination physical commit logic.

## Acceptance criteria

- One common receipt draft/finalizer owns every required common field and rejects incomplete or
  inconsistent drafts.
- Ordinary and correction receipts for all destinations preserve golden serialized form and
  destination-specific metadata.
- Typed plan/request fields replace reconstruction from verify-parameter string maps.
- Production destination receipt/correction/staging paths contain no direct `SystemTime::now`.
- Deterministic host-clock tests, duplicate/replay verification, and crash/commit-gate conformance
  pass.

## References

- `.10x/specs/destination-common-services.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/specs/execution-host-structured-runtime.md`

## Assumptions

- Source-backed: `ExecutionHost::unix_now` already exists and every destination runtime can bind
  `ExecutionServices`.

## Journal

- 2026-07-26: Found duplicate receipt constructors in all three destinations and direct process
  wall-clock reads in DuckDB, Postgres, and Parquet commit/correction paths.
- 2026-07-26: Added a typed package-contract receipt draft/finalizer and migrated DuckDB,
  Postgres, and Parquet ordinary and correction construction. Postgres load plans now retain typed
  package, token, schema, and segment fields instead of recovering them from verification strings.
- 2026-07-26: Bound receipt and staging timestamps to each destination's injected
  `ExecutionServices` clock. A remaining direct wall-clock read in DuckDB profiling is telemetry
  outside this ticket's receipt/correction/staging scope.
- 2026-07-26: Focused replay testing exposed a project test fixture that stored services on a
  resolved destination without binding them to its runtime before calling that runtime directly.
  The shared fixture now uses `with_bound_execution_services`; the failed
  committed-before-package-record replay gate passes.
- 2026-07-26: A preservation audit retained Postgres's prior per-segment `_cdf_loaded_at_ms`
  sampling granularity while changing its authority to the host clock. Delegated review then
  found one remaining package-validation read from `verify.parameters["package_hash"]`; execution
  now reads `PostgresLoadPlan.package_hash` directly, and the live helper reads the typed token.
- 2026-07-26: Two further delegated-review stop lines were repaired. Postgres now derives its
  historical lexicographic receipt-ack order from typed segments instead of accidentally changing
  to package order. Common receipt construction now receives the typed ordinary/correction plan,
  derives migrations from it, and validates request-plan drift before any receipt publication.
  Artifact replay gained a neutral schema-aware verified-package planning seam so DuckDB carries
  the same migration authority as live planning; execution rejects physical migration drift.
- 2026-07-26: Final adversarial passes found four more authority and publication boundaries.
  DuckDB zero-data planning now performs the complete schema, identifier, target, and existing-table
  dry run while returning no unapplied migrations, and staged empty binding rejects a plan that
  claims any. The common finalizer now validates acknowledgement bytes as well as identity and row
  counts. Parquet correction publication validates the plan, manifest, objects, and receipt
  evidence before create-publishing the durable marker, then verifies the created or competing
  marker by readback. Focused negative tests pin every repair.

## Blockers

None.

## Evidence

- Common finalizer and serialized form: `cargo test -p cdf-package-contract --locked` passed 16
  unit tests and the package-contract build-graph integration test. The ordinary draft golden
  assertion covers every serialized receipt field; correction mapping, exact plan-migration
  mapping, request-plan drift, incomplete migration authority, acknowledgement byte drift, and
  contradictory verification-parameter rejection have focused tests. This proves those common
  mapping and validation assertions, not every future destination's physical evidence.
- Destination behavior: `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-dest-duckdb
  -p cdf-dest-postgres -p cdf-dest-parquet --locked` passed DuckDB 54/54, Parquet 42/42 with one
  release benchmark ignored, and Postgres 33/33 with two release benchmarks ignored. DuckDB's
  focused host wrapper fixes Unix time at `1788123456789` and proves the ordinary and correction
  receipts retain it; its zero-data tests prove full dry-run validation, no target mutation, and no
  claimed migrations. Postgres's `z-segment`/`a-segment` regression test proves the typed path
  preserves historical lexical acknowledgement serialization. Parquet's invalid-evidence
  regression proves correction publication does not leave a create-only receipt marker. The
  adapter suites cover serialized metadata, receipt verification, duplicate handling, rollback,
  corrections, staging, and live Postgres transactions within their assertions.
- Neutral planning seam: `cargo test -p cdf-runtime --locked` passed 149 unit tests with two
  performance tests ignored, seven build-graph tests, and one compile-fail doc test. This covers
  the runtime trait/default behavior within its assertions; the DuckDB and project gates below
  exercise the schema-aware override.
- Replay and commit gates: exact `cdf-project` library tests passed for
  `checkpoint_failure_after_receipt_keeps_receipt_recoverable_and_state_unadvanced`,
  `duplicate_destination_replay_returns_duplicate_receipt_and_commits_pinned_checkpoint`,
  `general_project_run_records_failure_after_durable_receipt_without_advancing_state`,
  `replay_commits_duckdb_receipt_then_checkpoint_and_marks_package_checkpointed`, and
  `verified_destination_receipt_before_package_record_replays_idempotently`. The last test first
  failed because its direct-runtime fixture had not bound services and passed after the binding
  repair; these focused gates do not claim the entire project suite.
- Static and compiler gates: `DUCKDB_DOWNLOAD_LIB=1 cargo clippy -p cdf-runtime
  -p cdf-package-contract -p cdf-dest-duckdb -p cdf-dest-postgres -p cdf-dest-parquet
  -p cdf-project --all-targets --locked -- -D warnings`, `cargo fmt --all -- --check`, and
  `git diff --check` passed. A source scan found no direct wall-clock read in destination receipt,
  correction, mirror, or staging paths; `cdf-dest-duckdb/src/profiling.rs` retains a
  profiling-only `SystemTime::now`. A Postgres scan found no execution-authority reads for package
  hash, idempotency token, or schema hash from verify parameters after review repair.
- Graph maintenance limit: `graphify update .` could not run because `graphify` is not installed
  in this environment (`command not found`). No graph refresh is claimed.

## Review

Delegated OCR review produced seven significant findings across successive passes: one remaining
Postgres verify-map authority read, changed Postgres segment-ack ordering, adapter-supplied
migrations without typed plan authority, incomplete DuckDB zero-data validation, acceptance of
unapplied empty-plan migrations, missing acknowledgement byte validation, and Parquet correction
marker publication before complete evidence validation. The executor repaired every finding and
reran the mapped contract, runtime, adapter, replay, formatting, and strict-lint gates.

Two fresh reviewers independently returned `pass` with no findings. Residual risk is limited to
environment-backed live Postgres behavior and external object-store races not reproducible in the
hermetic suites, plus the unavailable graph refresh; the ticket's live tests and create-only
conflict simulations cover the corresponding in-process contracts.

## Retrospective

- What broke: changing a time source exposed a fixture that carried services only on the resolved
  wrapper while bypassing that wrapper for a direct runtime call. The production path was sound,
  but the fixture's unusual lifecycle hid the missing binding until a crash-window test executed.
- What surprised: adding typed Postgres plan fields did not automatically eliminate string-map
  authority. A package-validation helper still reconstructed one of those values from the verify
  clause. Typed segments also preserved package order, while the old `BTreeMap` had made lexical
  order an observable receipt behavior. Finally, treating migrations as generic evidence left the
  common finalizer unable to distinguish planned from invented DDL. Only code-first adversarial
  review found all three.
- What worked: exact replay/crash gates found lifecycle drift; a fixed host proved recorded time;
  static scans found residual clock and string-map reads; preserving the previous sampling site
  avoided an unnecessary `_cdf_loaded_at_ms` semantic change. Deliberately contradictory plans,
  non-lexical segment IDs, byte counts, and manifest counts exposed authority crossings that
  ordinary happy-path fixtures could not.
- Five whys: the typed-authority and ordering escapes survived because the migration began at receipt
  construction; package validation lived in a different module; its helper still returned a
  validated newtype; ordinary fixtures used lexically sorted segment IDs and matching migration
  values; and no negative check separated the competing authorities. Future migrations must
  search all reads of the displaced representation and use deliberately disagreeing fixtures,
  not only test constructors of the new one. Durable create-only markers must be the final step
  after pure evidence validation, because an error after publication is an externally observable
  partial commit.
- Distillation: `.10x/knowledge/destination-receipt-authority.md` records the authoring and review
  invariant. No new skill is warranted: the work exposed a reusable design rule, not a repeatable
  operational procedure.
