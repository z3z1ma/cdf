Status: active
Created: 2026-07-26
Updated: 2026-07-26

# Transactional SQL mirror lifecycle

## Boundary

`cdf-dest-sql` owns typed lifecycle laws for load receipts, checkpoint state, segment ranges, and
quarantine evidence. A native adapter owns SQL text, parameters, JSON representation, row
decoding, dialect quoting, and the transaction. The common manager borrows a backend and has no
commit method, so mirror work cannot escape the payload transaction.

This boundary is intentionally smaller than a SQL abstraction. It shares semantic operations and
evidence, not a query language, executor, ORM, or lowest-common-denominator database model.

## Receipt evidence

A duplicate key match is insufficient. Replay MUST compare the complete expected logical receipt.
A newly inserted row MUST return the exact physical receipt, including backend-generated
transaction evidence.

Receipt JSON cannot verify itself. Adapters persist independently typed identity, counts, segment
count, migrations, and commit-time columns and reconcile decoded JSON against those columns before
the common layer accepts a duplicate. History-dependent values may be reused only after this
independent reconciliation and local arithmetic validation.

Legacy DuckDB rows that predate independent `segment_count` and `migrations_json` evidence fail
closed. Do not fabricate proof from the receipt JSON; an operational backfill requires its own
evidence-backed procedure.

## Ordering and concurrency

Checkpoint state advances by parent-checkpoint lineage, not wall-clock order. Commit time is
evidence, not a causal ordering primitive. Segment inserts validate exact identity and non-overlap;
correction commits are deliberately excluded from ordinary checkpoint/segment advancement.

In PostgreSQL `READ COMMITTED`, an advisory lock and duplicate query in one SQL statement share
the statement snapshot. Acquire the idempotency lock in a separate statement, then issue a fresh
duplicate query so a waiter can observe the winner's committed receipt.

Successful inserts/upserts return typed rows with native `RETURNING` (or the backend equivalent)
and are compared exactly. Conflict paths may query the existing row. Quarantine records are
streamed through that pattern; never collect an unbounded quarantine batch or add an unconditional
network read per record.

## Identifier proof

Dynamic identifiers cross the shared boundary as `ValidatedSqlIdentifier`, constructed from the
destination sheet's `IdentifierRules`. The adapter then applies dialect quoting. Static framework
identifiers use the same checked path; there is no public unchecked constructor that callers can
forge.

## Verification expectations

Changes to this lifecycle require:

- shared law tests for ordering, conflicts, drift, and exact readback;
- adapter integration tests for atomic rollback, duplicate replay, and correction behavior;
- live PostgreSQL concurrency coverage for advisory-lock serialization and checkpoint CAS;
- tamper tests that change receipt JSON while independent columns remain unchanged;
- focused payload and mirror-path performance evidence; and
- adversarial review of transaction ownership, identifier provenance, and self-referential
  evidence.
