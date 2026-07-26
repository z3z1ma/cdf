Status: open
Created: 2026-07-26
Updated: 2026-07-26
Parent: `.10x/tickets/2026-07-26-pre-wave-architecture-hardening-program.md`

# Centralize receipt assembly and commit time

## Scope

Add one validated ordinary/correction receipt assembly path to `cdf-package-contract` and migrate
DuckDB, Postgres, and Parquet. Bind destination commit/correction timestamps to the injected
execution host's Unix clock and remove destination-local process-wall-clock helpers.

## Non-goals

- No receipt schema/version, verify-clause, transaction metadata, or id derivation change.
- No clock in kernel and no recomputation of recorded time during replay.
- No change to destination physical commit logic.

## Acceptance criteria

- One common receipt draft/finalizer owns every required common field and rejects incomplete or
  inconsistent drafts.
- Ordinary and correction receipts for all destinations preserve golden serialized form and
  destination-specific metadata.
- Typed plan/request fields replace reconstruction from verify-parameter string maps.
- Production destination receipt/correction/staging paths contain no direct `SystemTime::now`.
- Deterministic host-clock tests, duplicate/replay verification, and crash/commit-gate conformance
  pass.

## References

- `.10x/specs/destination-common-services.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/specs/execution-host-structured-runtime.md`

## Assumptions

- Source-backed: `ExecutionHost::unix_now` already exists and every destination runtime can bind
  `ExecutionServices`.

## Journal

- 2026-07-26: Found duplicate receipt constructors in all three destinations and direct process
  wall-clock reads in DuckDB, Postgres, and Parquet commit/correction paths.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
