Status: done
Created: 2026-08-10
Updated: 2026-08-10

# Destination runtime capability truthfulness

## Scope

Make destination bulk evidence, batch control, blocking-lane concurrency, and native parallelism
describe executable reality. Represent measured, inconclusive, and unmeasured evidence explicitly;
distinguish destination-controlled batching from pass-through package batches; remove unsupported
ClickHouse parallelism and PostgreSQL lane concurrency claims; and keep inspection, doctor, run
evidence, conformance, and serialized capability artifacts consistent.

## Non-goals

- New destination protocols or physical writers.
- Compatibility readers or legacy serialized shapes.
- New benchmark measurements.

## Acceptance Criteria

- [x] SQLite's inconclusive roofline is never rendered or serialized as measured evidence.
- [x] Prepared row/byte settings are reported as effective only when the destination consumes them.
- [x] ClickHouse declares one native writer/parallel unit and PostgreSQL declares one synchronous lane.
- [x] Runtime, CLI, conformance, fixtures, and first-party destinations use one current typed model.
- [x] Focused tests, formatting, checks, and strict affected-package Clippy pass.

## References

- `.10x/decisions/schema-planned-destination-bulk-paths.md`
- `.10x/specs/database-connector-roofline.md`
- `.10x/tickets/2026-08-02-sqlite-destination-connector.md`

## Assumptions

- The user explicitly ratified addressing every 2026-08-10 destination-capability audit finding.
- Current-only artifact replacement applies; no compatibility surface is permitted.

## Journal

- 2026-08-10: Opened from the user-authorized audit repair. The audit proved SQLite evidence is
  inconclusive, finalized SQLite/PostgreSQL/ClickHouse paths do not consume prepared batch targets,
  ClickHouse has one ArrowStream writer, and PostgreSQL has one synchronous destination writer.
- 2026-08-10: Replaced the optional measured-version string with the closed
  `BulkPathEvidence::{Measured, Inconclusive, Unmeasured}` authority. Measured and inconclusive
  evidence carry one validated artifact version; unmeasured carries none. Selected evidence is now
  derived from the selected descriptor, so runtime capabilities no longer duplicate its version.
  SQLite names its retained roofline as inconclusive; the other recorded first-party cells remain
  measured.
- 2026-08-10: Added the closed destination-controlled/pass-through batch mode and made prepared row
  and byte targets present only for destination-controlled paths. DuckDB and Parquet consume the
  controlled values through one validated accessor. SQLite, PostgreSQL, and ClickHouse retain
  descriptive accepted ranges but no longer publish preferred values as effective run settings.
  Both ordinary project-run and package-replay event evidence now record status/version and omit
  ineffective batch settings.
- 2026-08-10: Renamed the misleading writer plateau to the literal safe `maximum_writers` ceiling.
  Parquet's no-adapter-cap representation remains `u16::MAX`, while host CPU, run jobs, and memory
  admission resolve its actual writer count. Corrected ClickHouse native parallelism and PostgreSQL
  synchronous lane concurrency to one.
- 2026-08-10: Updated Doctor and Inspect without destination-name branches. Doctor passes only a
  measured selected path; inconclusive and unmeasured paths warn with typed details. Inspect renders
  the exact status and evidence version. Updated conformance, mocks, current serialized catalog,
  and benchmark envelope joins. The derived current destination-matrix report changed only exact
  execution-descriptor hashes after the current typed descriptor shape and concurrent Parquet v7
  physical-plan version changed; raw samples, summaries, host, and evidence versions were untouched.
- 2026-08-10: Focused verification passed: `cdf-runtime` library tests 202/202 (two ignored);
  benchmark `lab_policy` 8/8 (one ignored); conformance destination-catalog slice 6/6; exact CLI
  Doctor, Inspect evidence rendering, and replay pass-through telemetry tests; exact project ledger
  pass-through telemetry test; and exact ClickHouse, SQLite, PostgreSQL, and DuckDB capability
  assertions. Affected multi-package check passed. Strict affected-package Clippy passed in two
  no-dependency invocations for runtime/SQLite/ClickHouse/DuckDB and CLI/project/conformance/
  benchmarks. `cargo fmt --all` and `git diff --check` passed. One earlier dependency-inclusive
  strict Clippy attempt reached the concurrently unfinished PostgreSQL CDC function's argument-count
  lint; it did not identify a finding in this ticket's packages and is not claimed as a pass.
- 2026-08-10: After the concurrent PostgreSQL owner completed its structural lint and test-lifetime
  repairs, the consolidated dependency-inclusive all-target strict Clippy certificate passed for
  runtime, engine, CLI, project, conformance, benchmarks, and all five affected destinations.
  Final `cargo fmt --all -- --check` and `git diff --check` passed in the same certificate.
- 2026-08-10: The required cognitive-complexity diagnostic completed and reported only the
  unchanged `cdf-engine::execution::orchestration::preview_resource` function (34/25). This ticket
  does not touch that function or its behavior, so no capability-truthfulness repair is warranted;
  the result is retained as bounded review input rather than presented as a strict gate failure.

## Blockers

None.

## Evidence

- Typed evidence and preparation authority: `crates/cdf-runtime/src/bulk.rs` and
  `crates/cdf-runtime/src/capabilities.rs`; the complete focused runtime suite passed 202 tests.
- Truthful first-party declarations: the five `crates/cdf-dest-*/src/runtime.rs` implementations;
  exact capability assertions passed for SQLite, ClickHouse, PostgreSQL, and DuckDB, while the
  concurrent Parquet ticket records its own full leaf certificate.
- Human and durable output: `crates/cdf-cli/src/doctor_command.rs`,
  `crates/cdf-cli/src/inspect_command/render.rs`, `crates/cdf-cli/src/package_run.rs`, and
  `crates/cdf-project/src/runtime/ledger.rs`; their four exact focused assertions passed.
- Catalog and performance artifact coherence: the destination-catalog conformance slice passed
  6/6 and benchmark `lab_policy` passed 8/8 with one maintenance helper ignored.
- Compilation and lint: the affected multi-package all-target check passed; strict affected-package
  no-dependency Clippy passed with `-D warnings`; formatting and whitespace validation passed.

## Review

The independent tranche-level red team re-read the integrated destination diff and returned
`pass` with no actionable findings. Its PostgreSQL-specific findings were repaired before the
final verdict; no residual risk was identified for this capability model.

## Retrospective

- What broke: one optional string encoded both existence and quality of performance evidence, while
  a second runtime string duplicated selection authority. Prepared batch defaults also implied
  control even for writers that merely accepted upstream package batches.
- What worked: closed enums made impossible claims structurally difficult. Deriving selected
  evidence from the selected descriptor removed drift instead of adding another equality check,
  and optional prepared batch targets let telemetry follow executable behavior without generic
  rebatching or adapter-name dispatch.
- Surprise: the benchmark execution hash correctly detected that changing descriptor vocabulary
  and the concurrent Parquet physical-plan version invalidated the derived registry join even though
  historical measurements were unchanged. Updating only the derived exact hashes preserved both
  fail-closed current matching and immutable raw measurements.
- Five-whys conclusion: the recurring false claims came from using scalar fields for concepts with
  distinct lifecycle states. Closed evidence quality and batch-control types move those distinctions
  to construction/validation, so renderers and ledgers cannot casually call an inconclusive result
  measured or a pass-through preference effective.
