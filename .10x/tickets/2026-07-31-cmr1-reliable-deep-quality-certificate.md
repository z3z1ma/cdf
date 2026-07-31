Status: active
Created: 2026-07-31
Updated: 2026-07-31
Parent: `.10x/tickets/2026-07-31-connector-mode-readiness-program.md`

# Make the deep quality certificate reliable and bounded

## Scope

Restructure scheduled deep quality so independent evidence remains visible after one gate fails,
and split the registered source-by-destination conformance matrix into bounded, attributable test
cells without weakening its shared assertions or catalog coverage.

## Non-goals

- No product/runtime semantic change.
- No deletion or weakening of repeat, chaos, replay, property, security, API, or generated-artifact
  gates.
- No live provider dependency in pull-request or scheduled credential-free jobs.
- No attempt to make every heavyweight tool a fast pull-request gate.

## Acceptance criteria

- Deep CI has independent jobs for compile/lint, tests/conformance, generated/API, and
  security/supply-chain evidence, with explicit dependencies only where technically required.
- The full conformance matrix is split into independently named source shards or cells; each
  reports its current cell before execution and preserves the aggregate catalog coverage law.
- Every conformance shard has a fixed CI timeout and produces machine-readable test output or an
  equivalent per-cell artifact even on failure.
- Workspace tests do not silently execute the full matrix and then execute it again in the same
  workflow.
- Focused conformance tests and workflow/static validation pass locally; the current workflow can
  be dispatched at HEAD for remote evidence.

## References

- `.10x/tickets/2026-07-31-connector-mode-readiness-program.md`
- `.10x/evidence/2026-07-28-prewave-architecture-hardening-closure.md`
- `.10x/knowledge/quality-gate-execution.md`
- `.10x/knowledge/product-integration-and-closure-gate.md`
- `QUALITY.md`

## Assumptions

- User-ratified: CI/test-harness changes are authorized by the approved connector-mode readiness
  recommendation.
- Source-backed: the matrix currently expands five source archetypes across four destinations and
  three dispositions inside one `registered_source_catalog_cells_persist_output` test.
- Source-backed: scheduled Slow Quality currently serializes all gates in one job and repeats
  conformance coverage after a workspace-wide test invocation.

## Journal

- 2026-07-31: Activated as the first readiness workstream. Existing matrix assertions and catalog
  discovery remain authority; this ticket changes execution granularity and evidence visibility,
  not the set of required cells.
- 2026-07-31: Added checked source and chaos shard manifests whose fast catalog-coverage tests
  fail if enrollment and scheduled execution drift. The expensive matrix and cross-destination
  chaos entry points now select one declared source/destination, print a start/pass marker for
  every cell/window, and emit compact JSON. The four deterministic repeat laws are explicitly
  scheduled/ignored rather than silently consumed by ordinary workspace testing.
- 2026-07-31: Split Slow Quality into independent compile/lint, workspace-test,
  general-conformance, source-matrix, destination-chaos, repeat, generated/API, metrics,
  supply-chain, and static-security jobs. Matrix/chaos/repeat processes have a command timeout
  shorter than the job timeout so their logs can upload after failure. Developer-quality jobs now
  use the documented DuckDB download linkage; the prior monolithic workflow omitted it and the
  last hosted run failed during compile before later gates executed.
- 2026-07-31: A plain parallel `cargo test` run reported package-replay/Nebula interference while
  a remaining cross-destination chaos test stayed active. Every reported failure passed when
  isolated; switching the ordinary conformance lane to nextest gave each test a process boundary.
  After moving chaos to its own shards, the exact general lane completed 95/95 in 13.450 seconds
  with six scheduled tests skipped. The Quasar chaos shard passed all four crash windows in 6.97
  seconds and emitted per-window markers plus compact JSON.
- 2026-07-31: The first file-source matrix probe passed all three DuckDB dispositions, then was
  deliberately stopped after identifying `file/parquet_filesystem/append` as the current long
  cell. This is evidence for observability, not a full file-shard pass; the hosted bounded shard
  remains the completion authority.

## Blockers

None.

## Evidence

- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo nextest run -p cdf-conformance --locked -j 12`:
  95 passed, 6 scheduled tests skipped, 13.450 seconds.
- Both checked shard-to-catalog coverage tests passed exactly.
- The Quasar destination-chaos shard passed four of four crash windows in 6.97 seconds with
  durable JSON output and per-window markers.
- Strict all-feature/all-target `cdf-conformance` Clippy passed. Formatting, `git diff --check`,
  JSON validation, and YAML parsing passed.
- Hosted Slow Quality dispatch and aggregate shard evidence remain pending this implementation
  checkpoint.

## Review

Pending program review.

## Retrospective

Pending execution.
