Status: done
Created: 2026-07-26
Updated: 2026-07-28
Parent: `.10x/tickets/done/2026-07-26-pre-wave-architecture-hardening-program.md`
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

- `.10x/tickets/done/2026-07-26-pre-wave-architecture-hardening-program.md`
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
- 2026-07-28: The single frozen OCR batch found that Nebula deep-cloned its decoded task before
  entering the async open future, allowing the retained encoded bytes and parse lease to drop, and
  used a fresh cancellation token for rate admission. Repaired both together: the future now owns
  the retained executable task and borrows its typed model while using the injected run
  cancellation. Both Nebula laws and strict conformance Clippy pass after the repair.
- 2026-07-28: Closure corrected the B2 performance characterization. Its seven baseline and seven
  candidate debug samples show a consistent `+7.2%` proxy slowdown, not overlapping ordinary
  variance. It remains a disclosed non-product local limit; the comparable B3 release cells and D3
  hosted/product cells own the aggregate no-regression conclusion.

## Blockers

None.

## Evidence

- Synthetic source and exact touch surface: commit `5403347d` plus review repair `79cc13a7`;
  `.10x/evidence/2026-07-28-prewave-architecture-hardening-closure.md`. The Nebula fixture crosses
  the shared canonical planner, external artifact, typed reader, retained executable partition,
  and generic scheduler/package/DuckDB/receipt/checkpoint/replay path. The only direct dependency
  edge is test-only; no production runtime/project/CLI command file changed.
- Synthetic destinations: the fresh 151-test runtime suite covers capability-discovered staged
  and finalized synthetic ingress. Eight destination-catalog tests cover Quasar catalog
  enrollment, capability/bulk artifacts, atomic publication/abort cleanup, and generic
  identity/import fences. Three Quasar CLI tests cover lock/plan/run/replay/resume/doctor/inspect
  without generic destination-name branches.
- Product closure: exact HTTP no-change no-op, local Parquet→DuckDB, multi-file Parquet, Parquet
  destination, and source-free Parquet replay cells passed. D3's dated evidence supplies the
  current public HTTPS→DuckDB product smoke. Credentialed Iceberg/Glue cells were unavailable
  because all recognized AWS credential selectors were unset; B1/B2's local adapter suites own
  non-credentialed semantics.
- State bound: commit `b19adbf0`; all 68 state tests and strict state Clippy passed. The fresh
  10,000-member cell measured a 960-microsecond ordinary open and 46,352-microsecond explicit
  diagnostic, within its test-build guards. Ordinary opens remain independent of retained
  history.
- Static/quality/reference gates: 151 runtime tests, 22 task-store tests, 3 built-in catalog
  tests, both Nebula tests, strict affected Clippy, formatting, Cargo metadata, diff, and active
  record references passed. The repeat-heavy full conformance suite was bounded and stopped; the
  evidence record explicitly does not claim a full pass.
- Performance: B1's `+1.1%` local lifecycle proxy, B3's matched release TLC improvement and
  FineWeb movement within 10%, D3's hosted `-0.4407%` progress delta, and D3's renderer/product
  cells remain current. B2's consistent `+7.2%` debug proxy slowdown is disclosed as a limit and
  not used as no-regression authority.

## Review

Open-code-review delegation used OCR preview/rule resolution for the frozen range
`ae52951b..5403347d` and two independent read-only reviewers.

- Architecture reviewer: `concerns`, no critical/high findings. It identified the retained-task
  deep clone and fresh cancellation token as medium, and challenged the one-task fixture's ability
  to independently repeat B1/B2's multi-task coverage.
- Closure reviewer: `fail` due to the same retained-task issue at high severity; medium findings
  for cancellation and the overstated B2 variance characterization.
- Resolution: commit `79cc13a7` moves the retained task into the async future and uses the
  injected cancellation; focused laws and strict Clippy pass. The closure evidence now records
  B2 as a consistent debug-proxy slowdown.
- Accepted residual risk/no-action rationale: Nebula proves that a third source can reuse the
  shared lifecycle end to end, exactly as the governing spec requires. B1/B2 already cover
  multi-record ordinal advancement, high cardinality, opposite provider ordering, spill,
  cancellation, malformed records, and lease cleanup. Expanding the generic run-matrix segment
  contract to repeat those tests would add shared conformance scope and violate Z1's no-generic-
  edit touch-surface proof. The one-task Z1 fixture therefore remains deliberately bounded.

No critical/high finding remains open. Per the user's stop rule, the direct repairs were inspected
and verified once without commissioning a serial re-review.

## Retrospective

- What broke: the first third-source implementation copied a decoded task across the retained
  accounting boundary even though B1 had already documented that exact anti-pattern. Reuse of the
  shared reader is insufficient if the adapter drops its retained wrapper before execution.
- What surprised: the generic two-row matrix treats one canonical segment as part of its fixture
  contract. Two external tasks correctly produced two segments; preserving that assertion made
  the source task represent one provider catalog selection rather than two row-sized tasks.
- What worked: a named third-source leaf proved the shared planner/reader APIs were usable without
  touching production orchestration. Data-driven catalog enrollment and capability-discovered
  Quasar/staged laws kept generic code unchanged.
- Dead ends: two repeat-heavy full conformance attempts consumed their bounded windows without
  reporting a failure. Stopping them and relying on affected focused/product/package evidence was
  more honest than claiming a pass or spending the remainder of the session on repeat loops.
- Five whys: the accounting defect survived initial implementation because the typed model was
  cloneable; cloning looked mechanically convenient; the compiler could not express that the
  parse lease must dominate execution; the fixture asserted lifecycle output but not ownership
  topology; therefore the durable prevention remains the knowledge rule and typed retained
  wrapper pattern, with adversarial review focused on clone/drop boundaries.
- Distillation: closure updates the extension invariant, roadmap, coverage matrix, and cold-start
  handoff. No new final-closure skill is warranted. This program already produced and validated
  the canonical/mirrored `audit-error-ownership`, `audit-cli-report-authority`, and
  `audit-project-file-publication` skills; the final lessons are architecture judgment rather than
  another stable operator procedure.
