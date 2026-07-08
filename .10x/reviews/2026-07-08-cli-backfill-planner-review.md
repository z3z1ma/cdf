Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-07-cli-backfill-planner.md
Verdict: pass

# CLI backfill planner review

## Target

Review of `.10x/tickets/done/2026-07-07-cli-backfill-planner.md`, evidence `.10x/evidence/2026-07-08-cli-backfill-planner.md`, and the source changes in `crates/cdf-cli/src/*backfill*`, `crates/cdf-cli/src/project_run_resource.rs`, `crates/cdf-project/src/backfill.rs`, and `crates/cdf-project/src/runtime/resources.rs`.

## Findings

None blocking.

## Assumptions tested

Dry planning is no-write. The CLI dry-plan test asserts no `.cdf/packages`, `.cdf/state.db`, or DuckDB destination file exists after planning. The report also advertises package, destination, and checkpoint writes as false.

Execution goes through the general run spine. `backfill_command::execute_slice` calls `run_project(ProjectRunRequest { ... })` with the planned engine plan and destination. There is no source-specific or destination-specific commit shortcut in the backfill command.

Checkpoint ownership is window-scoped. The lower planner stamps each slice with `ScopeKey::Window { start, end }`; `WindowScopedResource` exposes the outer window scope to the plan/run spine while translating source-open calls back to the resource's inner scope. The live execute test proves the committed checkpoint head exists at the window scope and that the resource-scope head is absent.

Eligibility is fail-closed. The planner requires a declared non-unordered cursor and `IncrementalShape::Cursor`, validates exact pushdown for generated cursor-window predicates, rejects inverted numeric bounds, and the tests assert unsupported file-style resources are rejected without source opens or runtime artifacts.

The CLI architecture concern is improved. Backfill planning and slice semantics are in `cdf-project`; command code handles parsing, resolution, redaction, and report shaping. Shared construction for local-file, REST, and SQL run resources moved to `project_run_resource.rs`, reducing the command-handler duplication that `jscpd` had made visible.

## Verdict

Pass. The acceptance criteria are supported by focused CLI/project tests, a live SQL-source-to-DuckDB backfill execution test, full workspace tests, clippy/fmt, duplication and complexity scans, supply-chain/security checks, source-only Gitleaks scans, and CodeQL through the reusable database path.

## Residual risk

Backfill execution currently stops on the first failed slice and returns the error rather than a partial-progress summary. That is acceptable for this first bounded planner because no active record requires resumable multi-slice backfill reporting beyond ordinary run/package/checkpoint facts.

Calendar-aware slicing, page-token/log/CDC backfill, and timestamp residual filtering remain excluded by the ratified decision, not silently supported.
