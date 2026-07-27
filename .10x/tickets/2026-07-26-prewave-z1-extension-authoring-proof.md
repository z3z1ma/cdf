Status: open
Created: 2026-07-26
Updated: 2026-07-27
Parent: `.10x/tickets/2026-07-26-pre-wave-architecture-hardening-program.md`
Depends-On: `.10x/tickets/done/2026-07-26-prewave-a1-builtin-driver-catalog.md`, `.10x/tickets/done/2026-07-26-prewave-a2-rust-safety-lint-walls.md`, `.10x/tickets/done/2026-07-26-prewave-a3-driver-concurrency-conformance.md`, `.10x/tickets/done/2026-07-26-prewave-b1-typed-task-set-reader.md`, `.10x/tickets/done/2026-07-26-prewave-b2-spill-task-planning-lifecycle.md`, `.10x/tickets/done/2026-07-26-prewave-b3-file-runtime-modularization.md`, `.10x/tickets/done/2026-07-26-prewave-c1-receipt-clock-authority.md`, `.10x/tickets/done/2026-07-26-prewave-c2-sql-mirror-commons.md`, `.10x/tickets/done/2026-07-27-prewave-c1b-promotion-receipt-clock-injection.md`, `.10x/tickets/done/2026-07-26-prewave-d1-environment-error-taxonomy.md`, `.10x/tickets/2026-07-26-prewave-d1b-adapter-error-audit.md`, `.10x/tickets/2026-07-26-prewave-d1c-product-error-audit.md`, `.10x/tickets/2026-07-26-prewave-d2-typed-cli-report-authority.md`, `.10x/tickets/2026-07-26-prewave-d3-holistic-cli-experience.md`

# Prove source and destination authoring closure

## Scope

Falsify the completed architecture by implementing synthetic Nebula catalog-task source and
Quasar destinations through test/conformance fixtures, auditing changed-file topology, and
running the focused product/performance/quality closure matrix.

## Non-goals

- No production Nebula/Quasar adapter or new connector capability.
- No repair hidden inside closure review; findings receive their owning child or a bounded
  follow-up before closure.
- No repetition of expensive evidence already recorded by children unless integration uniquely
  requires it.

## Acceptance criteria

- Synthetic catalog-task source reuses task planning/reader commons and requires no generic
  runtime/project/CLI command edit.
- Synthetic finalized and staged destinations use catalog, concurrency, receipt, and conformance
  laws without generic destination-name branches.
- Changed-file analysis answers exactly what adding one source/destination touches and finds no
  copied lifecycle or concrete-adapter leak.
- Static build graph, lint/unsafe, reference/status, formatting, focused/full quality, local
  Parquet→DuckDB, HTTPS→DuckDB, multi-file no-op rerun, Iceberg/Glue smoke where credentials are
  available, replay/verify, and Parquet destination paths pass.
- Performance cells selected from P3 history stay within ordinary variance or improve; any
  surprising movement is investigated before closure.
- Fresh adversarial architecture, correctness, performance, and CLI reviews pass with no critical
  or significant finding.
- Parent, children, roadmap, and coverage matrix return to a coherent zero-active-ticket state.

## References

- `.10x/tickets/2026-07-26-pre-wave-architecture-hardening-program.md`
- `.10x/knowledge/source-destination-extension-invariant.md`
- `QUALITY.md`

## Assumptions

- Record-backed: expensive hosted/EC2 checks are run only when their child acceptance cannot be
  proved locally or from current evidence; all cost-bearing hosts are terminated after use.

## Journal

- 2026-07-26: Shaped as a falsification/closure child, not an implementation catch-all.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
