Status: cancelled
Created: 2026-07-21
Updated: 2026-07-22
Parent: .10x/tickets/done/2026-07-21-p3-d18-duckdb-reference-adapter-closeout.md
Depends-On: .10x/tickets/done/2026-07-21-p3-d18a-duckdb-wide-roofline-profile.md, .10x/tickets/done/2026-07-21-p3-d18b-duckdb-sparse-wide-projection.md, .10x/tickets/cancelled/2026-07-21-p3-d18c-duckdb-native-write-envelope.md, .10x/tickets/cancelled/2026-07-21-p3-d18d-duckdb-physical-admission-telemetry.md, .10x/tickets/cancelled/2026-07-21-p3-d18e-duckdb-public-abi-scanner-envelope.md, .10x/tickets/cancelled/2026-07-21-p3-d18f-duckdb-lossless-type-closure.md

# P3 D18Z: DuckDB reference-adapter gate

## Scope

Delete losing D18 experiments, run the final correctness/performance/memory matrix, adversarially
review the sole surviving adapter, close the child graph, and publish what is proven versus bounded
residual risk.

## Non-goals

No new optimization during closure review and no baseline reset to make a regression green.

## Acceptance Criteria

- Source search and dependency audit prove one product ingress and no superseded path or unused edge.
- Append, replace, merge, duplicate, replay, correction, rollback/OOM redrive, provenance, nested/
  temporal/type-matrix, and jobs-invariance laws pass.
- Controlled EC2 median-of-N TLC and wide cells satisfy child gates with exact rows, receipt,
  checkpoint, RSS/cgroup, DuckDB buffer/temp, and spill evidence.
- Workspace formatting, strict clippy, focused/full governed tests, product smoke, graph update, and
  `git diff --check` pass on the final clean revision.
- Independent adversarial architecture, correctness, and performance review finds no critical or
  significant unresolved issue; every residual has an owner or measured no-action rationale.
- Parent/program/L7 references and ticket statuses accurately reflect closure.

## References

- `.10x/tickets/done/2026-07-21-p3-d18-duckdb-reference-adapter-closeout.md`
- `.10x/knowledge/quality-gate-execution.md`
- `.10x/knowledge/runtime-conformance-throughput-rule.md`
- `.10x/tickets/done/2026-07-18-p3-l7-ec2-benchmark-tranche-lifecycle.md`

## Assumptions

- User-ratified: review should be thorough in one pass, performance may not regress, and dead or
  superseded code must be deleted.

## Journal

- 2026-07-22: Cancelled by explicit user direction after D18A and D18B closure. The user chose to
  stop further DuckDB-specific work, accept the native pathological-wide floor, and retain the
  statistics-based improvement. This cancellation does not claim the original D18A-F matrix was
  executed; it records that D18C-F were deliberately cancelled instead.

## Blockers

None. Cancellation is deliberate, not blocked.

## Evidence

D18A and D18B contain their own complete verification and adversarial review. No separate D18Z
matrix was run.

## Review

Cancellation avoids misrepresenting a partial child graph as a fully executed gate.

## Retrospective

A closeout gate should be cancelled rather than weakened when the user intentionally terminates its
remaining experiments.
