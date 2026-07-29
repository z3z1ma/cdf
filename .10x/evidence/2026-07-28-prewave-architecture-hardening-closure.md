Status: recorded
Created: 2026-07-28
Updated: 2026-07-28

# Pre-wave architecture hardening closure evidence

## Observation

The program's final extension falsifier is green at commits `b19adbf0` and `5403347d`.
Ordinary state-store opens remain schema-only, while the explicit 10,000-member content-integrity
diagnostic is linear and completes in tens of milliseconds in an unoptimized test build. The
synthetic Nebula catalog source uses the shared spill-backed canonical task planner, external
artifact, typed reader, retained executable partition, and generic scheduler/package/destination/
receipt/checkpoint/replay lifecycle.

The Nebula changed-file topology is exactly:

- the renamed conformance source leaf;
- one data-driven source-catalog row and archetype/module declaration;
- one test-only direct `cdf-task-store` dependency and its generated lock edge;
- the owning Z1 journal.

No production runtime, project, CLI command, adapter, or identity-artifact code changed. Current
Quasar conformance remains catalog-installed and capability-driven, while the runtime's synthetic
staged/finalized law discovers ingress by declared capability rather than destination identity.

## Procedure and results

### Affected implementation and static graph

- `cargo test -p cdf-conformance nebula_source --locked -- --nocapture` with the repository's
  local DuckDB link environment passed both Nebula laws. The end-to-end law asserts two rows,
  plan honesty, package verification, destination receipt verification, checkpoint-after-receipt,
  artifact replay identity, and host rate admission.
- `cargo clippy -p cdf-conformance --all-features --all-targets --locked -- -D warnings` passed.
- `cargo metadata --locked --no-deps --format-version 1` reports the new direct
  `cdf-conformance → cdf-task-store` edge as `kind=dev`. The normal graph already reaches the task
  store transitively through enrolled production source adapters; no new production edge was
  added.
- `cargo test -p cdf-builtin-drivers --locked --quiet` passed all 3 catalog/graph tests.
- `cargo fmt --all -- --check`, `cargo metadata --locked --no-deps --format-version 1`, and
  `git diff --check` passed. Every backtick-delimited `.10x/` reference in the active parent and
  Z1 ticket resolved.

The first focused Nebula run modeled two catalog tasks. It failed an existing run-matrix assertion
because the fixture contract intentionally expects its two rows in one canonical segment. The
final fixture models one provider catalog selection as one typed task containing two rows, inserts
the identical task twice to falsify idempotent duplicate handling, and observes exactly one
published/executed task. This is a task-granularity correction, not a weakened generic assertion.

### Shared runtime, task, destination, and product paths

- `cargo test -p cdf-runtime --lib --locked --quiet` passed 151 tests with 2 intentional ignores.
  This includes the capability-discovered synthetic staged/finalized destination law and isolated
  source worker-admission law.
- `cargo test -p cdf-task-store --lib --locked --quiet` passed 22 tests with 1 intentional ignore.
- `cargo test -p cdf-conformance 'destination_catalog::' --locked -- --nocapture` passed 8 tests:
  catalog enrollment, complete capability artifacts, bulk preflight, generic destination-import
  and identity-branch fences, Quasar atomic publication/abort cleanup, and error ownership.
- `cargo test -p cdf-cli injected_quasar_destination --locked -- --nocapture` passed all 3
  Quasar product tests: lock/plan/run/duplicate replay/doctor/inspect, source-free finalized
  package replay, and durable-receipt recovery without duplicate commit.
- Five exact CLI product cells passed: HTTP monthly-glob incremental no-change no-op, local
  Parquet discovery/autopin/DuckDB commit, multi-file Parquet byte-stable autopin, filesystem
  Parquet destination, and source-free Parquet package replay.
- The current crash-recovery closure already passed all 300 CLI tests, while D3 recorded 55
  CLI-core tests and the then-current 299 CLI tests before the crash test was added. Z1 did not
  modify CLI code.

A full 99-test conformance invocation was stopped after its explicit 100-run golden loops exceeded
the bounded closure window with no reported failure. A second 95-test invocation excluded the four
deliberate 100/10-repeat golden cells; every test reported before the source×destination matrix
passed, including Nebula, Quasar, replay, property/fuzz, product demo, Postgres, and preview/run
parity. The matrix itself remained silent beyond the same bounded window and the invocation was
stopped rather than allowed to consume the session. No full-suite pass is claimed. The affected
Nebula end-to-end law, destination catalog laws, Quasar product laws, and fresh package-level
suites are the closure evidence; completed children retain the last full repeat-loop evidence.

### State diagnostic bound

- The state package passed all 68 tests and strict all-feature/all-target Clippy when `b19adbf0`
  was committed.
- A fresh exact diagnostic cell reported
  `member_count=10000 open_us=960 diagnostic_us=46352` and passed its 2-second ordinary-open and
  10-second explicit-diagnostic test-build guard. The earlier same-code observation was 709
  microseconds and 44,254 microseconds. These are local unoptimized regression guards, not product
  latency SLOs.
- Static inspection confirms checkpoint, run-event, promotion, content-claim, and root-member
  ordinary opens initialize/validate schema only. Full-history integrity work remains an explicit
  diagnostic/recovery action.

### Performance and external cells

Z1 introduced no production hot-path default. Current child measurements remain the comparable
performance authority:

- B1 Iceberg lifecycle proxy: `+1.1%` median, ordinary local variance.
- B2 5,000-task spill/canonical-planning proxy: a consistent `+7.2%` debug wall-time slowdown
  across the recorded seven-sample sets, with slightly lower sampled RSS. This is retained as a
  non-product local proxy limit, not relabelled as ordinary variance or used as the aggregate
  no-regression authority.
- B3 matched release TLC/FineWeb cells: TLC improved from 1.35 to 1.28 seconds; FineWeb moved from
  13.03 to 13.58 seconds, within the 10% gate, with lower median RSS in both candidates.
- D3 renderer/progress evidence: hosted progress delta `-0.4407%`, local million-event floor
  preserved, and 10,000-row build/render median 4.5571 milliseconds.

D3 also records the current public HTTPS→DuckDB smoke: five HTTPS partitions, 5,000 rows, five
segments, verified DuckDB receipt, committed checkpoint, and inspectable run. Z1's local product
samples complement rather than repeat that public-network cell.

`AWS_ACCESS_KEY_ID`, `AWS_PROFILE`, `AWS_SESSION_TOKEN`, and `AWS_SECRET_ACCESS_KEY` were all
unset. Therefore no credentialed Iceberg/Glue catalog cell was available. The shared typed-reader
and planning child suites cover both adapters locally and explicitly limit their claims to
non-credentialed semantics.

## Adversarial review

OCR deterministic preview/rule resolution selected the seven-file range
`ae52951b..5403347d`. Two independent read-only reviewers inspected architecture/correctness and
aggregate evidence in one frozen batch.

Both found that Nebula deep-cloned its decoded task before async execution, allowing the retained
encoded bytes and parse lease to drop. They also found the coupled use of a fresh cancellation
token. Commit `79cc13a7` repairs both by moving the retained executable task into the future,
borrowing its model there, and passing injected run cancellation. Both Nebula laws and strict
affected Clippy passed after repair.

The closure reviewer also identified the overstated B2 performance wording, corrected above. The
architecture reviewer challenged one-task Nebula as independent multi-task coverage. That
residual is accepted rather than hidden: B1/B2 already own multi-record/high-cardinality ordering,
spill, cancellation, identity, and cleanup. Z1 proves the governing spec's distinct third-source
reuse scenario and deliberately avoids changing generic segment expectations merely to duplicate
those lower-layer tests.

No critical/high finding remains open. The repaired ownership code was inspected and verified
without a serial re-review, following the user's explicit stop rule.

## What this supports

- Every Z1 acceptance criterion within the recorded environmental and repeat-loop limits.
- The parent's source/destination extension, identity preservation, quality, performance, and
  zero-generic-branch criteria.
- Reuse of child evidence where Z1 explicitly excludes unnecessary repetition of expensive
  hosted cells.

## Limits

No claim is made that the interrupted repeat-heavy conformance invocations passed. No
credentialed remote Iceberg/Glue catalog was contacted. Local diagnostic timings are not
release-mode performance measurements. `graphify update .` was attempted again but the
`graphify` executable remains unavailable, so source, Cargo metadata, tests, and OCR own the final
graph evidence.
