Status: open
Created: 2026-08-02
Updated: 2026-08-02

# SQLite, ClickHouse, and MongoDB connector program

This is a parent planning ticket and is not executable implementation scope.

## Scope

Deliver six first-party connectors in strict tranche order:

1. `.10x/tickets/2026-08-02-sqlite-source-connector.md`
2. `.10x/tickets/2026-08-02-sqlite-destination-connector.md`
3. `.10x/tickets/2026-08-02-clickhouse-source-connector.md`
4. `.10x/tickets/done/2026-08-02-clickhouse-destination-connector.md`
5. `.10x/tickets/2026-08-02-mongodb-source-connector.md`
6. `.10x/tickets/2026-08-02-mongodb-destination-connector.md`

Each child owns implementation, focused connector validation, direct-library roofline evidence, an
independent red-team review, closure repair, and retrospective. The parent owns one fresh final
workspace/core-impact certificate and the final current-source roofline sweep after all six
implementations stabilize. The sequence is deliberate: each child touches workspace dependency
resolution and the single built-in catalog, and each database's destination may reuse or extract
only lower protocol mechanics proven by its source predecessor. A reviewed source implementation
may unblock its paired destination while parent-owned final validation remains pending.

## Non-goals

- MongoDB change streams or any resident CDC lifecycle.
- ClickHouse merge/`ReplacingMergeTree` semantics.
- Arbitrary SQL or aggregation-pipeline source execution.
- Remote cloud benchmark writes without separate authorization.
- Generic runtime branches for any concrete connector.

## Acceptance Criteria

- All six child implementations have focused acceptance evidence and pass review; child tickets
  close after the parent-owned final validation resolves their deferred gates.
- SQLite closes before ClickHouse begins; ClickHouse closes before MongoDB begins.
- Each connector is a leaf and is enrolled only through `cdf-builtin-drivers`; one fresh final
  workspace/core-impact certificate covers the stabilized six-connector change set.
- Six local macro cells meet the ratified 0.90 direct-library roofline.
- Workspace format, tests, all-feature Clippy, dependency policy/security gates, and documentation
  agree with the final catalog.
- No connector-specific lifecycle leaks into project, CLI, engine, scheduler, package, receipt, or
  checkpoint authorities.

## References

- `.10x/research/2026-08-02-sqlite-clickhouse-mongodb-connector-shaping.md`
- `.10x/specs/database-connector-roofline.md`
- `.10x/knowledge/source-destination-extension-invariant.md`
- `docs/connector-authoring.md`

## Assumptions

- The user ratified the recommended source lifecycle, truthful destination capabilities, MongoDB
  8.0+ floor, and 90% local roofline on 2026-08-02.
- Direct mainline execution was requested in the surrounding workstream; external cloud writes are
  not implied by that authorization.

## Journal

- 2026-08-02: Program created after source, destination, and performance semantics were ratified.
- 2026-08-02: The user explicitly reduced validation cadence after repeated per-repair full-suite
  runs proved slow and redundant. Child execution now uses focused connector, boundary, affected-
  package, error-audit, and benchmark evidence. One fresh full workspace/core-impact certificate
  and the final six-cell roofline sweep run only after all connector implementations stabilize.
- 2026-08-02: SQLite source implementation and both independent repair reviews are complete. Its
  current roofline remains a visible deferred parent gate; this does not block the paired SQLite
  destination from reusing the now-reviewed lower protocol boundary.

## Blockers

None.

## Evidence

Pending child-ticket closure.

## Review

Pending child reviews and final integration review.

## Retrospective

Pending.
