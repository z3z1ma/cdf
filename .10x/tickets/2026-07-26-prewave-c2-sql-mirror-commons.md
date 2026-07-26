Status: open
Created: 2026-07-26
Updated: 2026-07-26
Parent: `.10x/tickets/2026-07-26-pre-wave-architecture-hardening-program.md`
Depends-On: `.10x/tickets/2026-07-26-prewave-c1-receipt-clock-authority.md`

# Extract typed SQL destination mirror commons

## Scope

Create `cdf-dest-sql` with typed load/state/segment/quarantine mirror mutations, readbacks,
ordering/idempotency rules, and a transactional mirror manager. Migrate DuckDB and Postgres while
keeping physical SQL, parameters, JSON types, transactions, and row decoding in each adapter.
Unify identifier validation through sheet rules and retain dialect-owned quoting.

## Non-goals

- No arbitrary-string `SqlExecutor`, ORM, shared SQL generator, or cross-destination transaction.
- No change to payload bulk paths, type fidelity, destination guarantees, or warehouse support.
- No lowest-common-denominator SQL or JSON conversion.

## Acceptance criteria

- DuckDB and Postgres share one typed mirror lifecycle and common evidence models.
- Native backend wrappers execute all mirror work in the payload transaction required by their
  guarantee.
- Failure injection proves atomic rollback, duplicate idempotency, state monotonicity, quarantine
  uniqueness, and receipt readback.
- Identifier input is validated by destination sheet rules before dialect quoting; unsafe string
  interpolation is absent.
- Mirror and correction conformance plus focused bulk-path performance show no regression.

## References

- `.10x/specs/destination-common-services.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/specs/destination-extension-runtime-contract.md`

## Assumptions

- Source-backed: DuckDB and Postgres duplicate mirror lifecycle but require different physical SQL
  and transaction clients.

## Journal

- 2026-07-26: Scoped the shared API as typed mirror operations, explicitly rejecting a stringly
  executor that would leak SQL semantics upward.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
