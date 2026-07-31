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
- 2026-07-31: Dispatched Slow Quality run `30669359331` at commit `fff4f6ed`. Independent jobs
  exposed the complete dormant failure set concurrently instead of serially hiding it behind the
  first compile failure. Four repeat shards and general conformance passed; the first corrective
  tranche is limited to the observed Linux test visibility defect, generated memory-owner input
  shape, `event-listener` soundness advisory, OSV/Semgrep/CodeQL invocation defects, Postgres chaos
  fixture isolation, and metrics-tool invocation defects. Existing cargo-machete findings are
  retained as an attributable nonblocking inventory because resolving unrelated manifest hygiene
  is outside this ticket and not runtime correctness work.
- 2026-07-31: Corrected the observed root causes without changing runtime behavior: Linux-only
  tests now import the crate-visible rlimit helper; dynamic typed consumer keys are constructed
  before the generated owner scanner's reservation boundary; Postgres chaos cases reset their
  conformance schema so independent crash windows cannot share `_cdf_state`; the transitive
  `event-listener` lock entry is patched to 5.4.2; OSV admits only the already-ratified paste
  advisory; Semgrep preserves the full informational inventory while warnings/errors remain
  blocking; CodeQL uses Rust's required buildless mode through the official analyze action; and
  metrics tools receive their required output directory/metadata mode.
- 2026-07-31: The first corrective hosted run restored cached Rust link artifacts without the
  downloaded native DuckDB library, causing every conformance link to fail with `-lduckdb` before
  tests ran. Added an explicit cached-directory declaration plus a pre-link fence that invalidates
  only `libduckdb-sys` when the native link input is absent. The earlier diagnostic run's Parquet
  cell markers remain an observation to re-evaluate after a clean link; they are not yet classified
  as a runtime defect.
- 2026-07-31: The cached-link corrective run advanced the supply-chain job far enough to expose two
  stale version contracts: the `event-listener` exemption still named 5.4.1 after the lockfile moved
  to audited 5.4.2, and pinned OSV Scanner 1.9.2 accepts `scan --output` rather than the v2
  `scan source --output-file` shape. Updated both definitions to match the exact pinned inputs;
  local OSV 1.9.2 validation reports only the ratified paste advisory.

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
- Hosted run `30669359331` proved all four deterministic repeat shards and the 95-test general
  conformance lane pass independently. It also produced complete, attributable failure evidence
  for the corrective tranche above; a clean rerun at the corrected commit remains pending.
- Corrective-tranche focused verification: 147/147 fast tests passed across `cdf-conformance`,
  `cdf-subprocess`, and `cdf-task-store` with eight scheduled/stress tests explicitly skipped;
  strict all-target/all-feature Clippy passed for the same crates. The exact Postgres chaos shard
  passed all four crash windows in 8.38 seconds, including stable duplicate retry and checkpoint
  ordering assertions.
- Generated owner freshness/check-closed, workflow YAML parsing, formatting, and diff checks
  passed. `cargo audit` found no unignored advisory after updating `event-listener` to 5.4.2; OSV
  reported only ratified `RUSTSEC-2024-0436`; the warning/error Semgrep tier scanned 754 tracked
  files with zero findings/errors; and rust-code-analysis successfully emitted 479 per-file
  reports to a pre-created output directory.
- `go run github.com/google/osv-scanner/cmd/osv-scanner@v1.9.2 scan --lockfile Cargo.lock
  --format json --output /tmp/cdf-osv-v1.json` produced valid JSON over 648 packages and reported
  only `RUSTSEC-2024-0436`, proving the workflow syntax against the pinned scanner rather than a
  locally installed v2 binary.

## Review

Pending program review.

## Retrospective

The earlier monolithic workflow made failures appear piecemeal because compile stopped the only
job before generated, security, supply-chain, and metrics tools ran. The parallel redesign exposed
the whole dormant frontier in one dispatch: most findings were stale tool contracts or generated
inputs, while only Linux test visibility, a new soundness advisory, and Postgres chaos isolation
needed correctness-oriented repair. Classifying those categories before editing prevented the
106 informational Semgrep results and pre-existing cargo-machete inventory from expanding into
unbounded product work.

The first Postgres isolation repair branched on the destination name. The existing generic-engine
law immediately rejected it, and uniformly resetting the unused conformance schema before every
case preserved connector agnosticism while isolating the SQL mirror. That negative self-test was
more valuable than another broad review pass. The recurring friction is now captured in
`.10x/knowledge/quality-gate-execution.md`: deep jobs remain independent, the first failing run is
triaged as one frontier, scanner severities and exact exceptions are explicit, hosted CodeQL uses
its supported buildless lifecycle, and local platform success is never treated as Linux proof.
