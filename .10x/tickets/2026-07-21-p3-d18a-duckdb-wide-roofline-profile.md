Status: open
Created: 2026-07-21
Updated: 2026-07-21
Parent: .10x/tickets/2026-07-21-p3-d18-duckdb-reference-adapter-closeout.md

# P3 D18A: DuckDB wide roofline and profile

## Scope

Create a reproducible controlled-host workload for the exact finalized 3,513,266-row,
2,052-column package and measure CDF's current stock scanner against the closest semantics-labeled
raw DuckDB materialization. Capture operator timings, CPU, rows, logical/physical bytes, process and
cgroup memory, DuckDB peak buffer memory, peak temp-directory size, and spill.

## Non-goals

No product tuning, path change, source re-extraction, or conclusion from a laptop-only sample.

## Acceptance Criteria

- The retained package and schema/statistics identities are recorded without committing payload.
- The lab has a repeatable raw reference and full-CDF replay cell with explicit semantic bias.
- Median-of-N controlled EC2 evidence attributes scanner conversion, DuckDB sink/storage,
  checkpoint/receipt, peak buffer memory, peak temp storage, and process/cgroup memory.
- The profile names the dominant wide-schema cost and establishes comparison keys for D18B-E.
- Existing full-year TLC control is rerun on the same clean revision/host class.

## References

- `.10x/specs/performance-lab-and-envelope.md`
- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/tickets/2026-07-18-p3-l7-ec2-benchmark-tranche-lifecycle.md`
- `.10x/tickets/done/2026-07-21-p0-duckdb-wide-ingest-memory.md`

## Assumptions

- Record-backed: the finalized local package under `/Users/alexanderbut/code_projects/tmp/.cdf/packages/`
  is reusable benchmark input after manifest verification; no FQ12 source contact is required.
- User-ratified: performance claims require real end-to-end and EC2 evidence rather than intuition.

## Journal

None.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
