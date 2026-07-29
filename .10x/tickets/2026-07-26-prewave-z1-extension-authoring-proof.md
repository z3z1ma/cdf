Status: active
Created: 2026-07-26
Updated: 2026-07-27
Parent: `.10x/tickets/2026-07-26-pre-wave-architecture-hardening-program.md`
Depends-On: `.10x/tickets/done/2026-07-26-prewave-a1-builtin-driver-catalog.md`, `.10x/tickets/done/2026-07-26-prewave-a2-rust-safety-lint-walls.md`, `.10x/tickets/done/2026-07-26-prewave-a3-driver-concurrency-conformance.md`, `.10x/tickets/done/2026-07-26-prewave-b1-typed-task-set-reader.md`, `.10x/tickets/done/2026-07-26-prewave-b2-spill-task-planning-lifecycle.md`, `.10x/tickets/done/2026-07-26-prewave-b3-file-runtime-modularization.md`, `.10x/tickets/done/2026-07-26-prewave-c1-receipt-clock-authority.md`, `.10x/tickets/done/2026-07-26-prewave-c2-sql-mirror-commons.md`, `.10x/tickets/done/2026-07-27-prewave-c1b-promotion-receipt-clock-injection.md`, `.10x/tickets/done/2026-07-26-prewave-d1-environment-error-taxonomy.md`, `.10x/tickets/done/2026-07-26-prewave-d1b-adapter-error-audit.md`, `.10x/tickets/done/2026-07-26-prewave-d1c-product-error-audit.md`, `.10x/tickets/done/2026-07-26-prewave-d2-typed-cli-report-authority.md`, `.10x/tickets/done/2026-07-26-prewave-d3-holistic-cli-experience.md`

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
- Normal-run state-store opens remain independent of total historical checkpoint, run-event,
  promotion, content-claim, and root-member populations. Explicit diagnostic/recovery integrity
  scans are measured with representative bounded histories and must meet the closure budget or
  receive a measured linear/bounded replacement before Z1 passes.
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
- 2026-07-27: D1c adversarial review initially found full-history scans on ordinary store opens.
  D1c repaired the default path: opens are schema-only, typed APIs validate consumed rows, and raw
  diagnostic/recovery consumers invoke explicit whole-store integrity checks. Z1 must confirm the
  normal-run bound and measure the remaining explicit diagnostic path, including large inline
  content-root membership.
- 2026-07-28: Activated after every implementation child and the project-publication crash
  follow-up closed. Static inspection confirmed ordinary checkpoint, run-event, promotion,
  content-claim, and root-member store opens initialize/validate schema only. The explicit content
  integrity path still compared each indexed member against the complete inline member vector,
  making one large root quadratic. Replaced that diagnostic-only comparison with one expected-key
  hash set that is consumed while rows stream, preserving missing/extra/absent-root diagnostics
  with bounded linear expected work. Added a 10,000-member measurement covering ordinary open and
  the explicit diagnostic.
- 2026-07-28: The 10,000-member cell measured a 709-microsecond ordinary open and a
  44,254-microsecond explicit full integrity diagnostic in an unoptimized test build. All 68
  state-store tests and strict all-feature/all-target state-store Clippy pass. The first invocation
  omitted the repository's local DuckDB link environment and failed only at link time; the exact
  rerun with the established environment passed and is the evidence-bearing observation.
- 2026-07-28: Recast the existing generic external-source fixture as the named synthetic Nebula
  catalog source. Its provider-owned typed catalog task crosses the shared spill-backed canonical
  planner, external task artifact, typed reader, retained executable-partition, scheduler,
  package, receipt, checkpoint, and replay paths. The changed-file topology is exactly the renamed
  conformance leaf, the data-driven source catalog row, the conformance archetype/module
  declaration, and the test-only `cdf-task-store` dependency plus lock edge; runtime, project, and
  CLI command code are untouched.
- 2026-07-28: The first Nebula end-to-end run used two catalog tasks and correctly surfaced the
  existing run-matrix law that this two-row fixture produces one canonical segment. The fixture now
  models the catalog selection as one typed task containing its two rows while still exercising
  canonical spill admission, identical-provider-task suppression, ordinal assignment, publication,
  typed reading, and retained execution. Both Nebula laws pass, including generic plan/run,
  destination receipt, checkpoint gate, duplicate replay, and artifact replay; strict
  all-feature/all-target conformance Clippy also passes.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
