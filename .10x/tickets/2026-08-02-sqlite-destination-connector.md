Status: active
Created: 2026-08-02
Updated: 2026-08-02
Parent: .10x/tickets/2026-08-02-sqlite-clickhouse-mongodb-connector-program.md

# SQLite destination connector

## Scope

Implement and ship `cdf-dest-sqlite` with append, atomic replace, merge, package-token
idempotency, typed SQL mirrors, independently verifiable receipts, live crash coverage, operator
documentation, and a release-mode direct-`rusqlite` destination roofline cell. Reconcile any exact
lower SQLite protocol duplication with the source without creating a universal database wrapper.

## Non-goals

State-store reuse, parallel writers, implicit WAL changes, network filesystems, cross-attached-file
atomicity, CDC application, and arbitrary SQL.

## Acceptance Criteria

- The sheet, planner, runtime, mapping, transaction, provenance, mirror, and verification behavior
  implement `.10x/specs/sqlite-destination.md`.
- Append/replace/merge, duplicate package, zero rows, merge-key conflicts, schema incompatibility,
  journal/durability preservation, and crash-before/after-commit have unit and live tests.
- One transaction covers target and mirror mutations; a fresh connection independently verifies
  receipts before checkpoint commit.
- Built-in catalog integrity, generic destination/product/chaos/jobs laws, and
  `tools/certify-connector.py --kind destination --id sqlite --core-impact` pass.
- The destination macro benchmark records raw samples and reaches the 0.90 direct-`rusqlite`
  roofline under the same durability and journal mode.
- Independent review passes after any closure repair.

## References

- `.10x/specs/sqlite-destination.md`
- `.10x/specs/database-connector-roofline.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/specs/destination-common-services.md`
- `.10x/knowledge/source-destination-extension-invariant.md`
- `docs/connector-authoring.md`

## Assumptions

- Append/atomic replace/merge and the 90% roofline are user-ratified.
- SQLite's single-writer boundary is protocol authority, not a performance defect.

## Journal

- 2026-08-02: Ticket opened; execution waits for SQLite source closure.
- 2026-08-02: Source implementation dependency is satisfied: focused correctness, affected-package
  checks, exact error audit, and independent repair re-review pass. The source's fresh roofline and
  workspace certificate are parent-owned final integration gates under the user-ratified reduced
  validation cadence and do not block this paired destination implementation.
- 2026-08-02: Destination execution started. Read this complete ticket, its parent, every direct
  active authority, the transitive destination ingress/runtime/performance/concurrency authorities,
  the error-ownership taxonomy, and the mandatory rusqlite audit procedure. The executable boundary
  is fixed: one finalized-package, run-owned, non-parallel SQLite writer; one native transaction for
  payload, compact provenance, and typed SQL mirrors; generic receipt assembly and checkpoint
  ordering remain shared; verification opens a fresh connection; no generic database wrapper or
  destination-identity branch is permitted. Validation is limited to the user-ratified focused
  gates and one bounded roofline; the parent owns the final workspace/core-impact certificate.

## Blockers

None.

## Evidence

Pending.

## Review

Pending independent red-team review.

## Retrospective

Pending executor handback.
