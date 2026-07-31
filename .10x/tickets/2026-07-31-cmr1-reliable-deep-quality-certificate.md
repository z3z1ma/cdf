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

## Blockers

None.

## Evidence

Pending execution.

## Review

Pending program review.

## Retrospective

Pending execution.
