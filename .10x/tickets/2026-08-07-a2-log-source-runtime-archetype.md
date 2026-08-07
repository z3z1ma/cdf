Status: open
Created: 2026-08-07
Updated: 2026-08-07
Parent: `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`
Depends-On: `.10x/tickets/done/2026-08-03-cdc-source-position-artifact-transition.md`, `.10x/tickets/2026-08-07-a1-5-package-native-keyed-effects.md`

# A2: log-source runtime archetype

## Scope

Implement the neutral finite-drain runtime contract between first-party CDC adapters and the
existing drain/package/receipt/checkpoint authorities. The runtime must admit one ordered source
stream, account a source settlement unit across arbitrarily rechunked Arrow batches, publish only
source-proven safe frontiers, lower complete-image and key-only changes into the existing typed CDC
batch contract, and wait for a safe boundary when package rotation or command termination requests
closure.

The first archetype covers committed PostgreSQL/MySQL transaction boundaries and MongoDB terminal
event-prefix resume tokens without branching on source kind in generic runtime code.

## Non-goals

- real PostgreSQL logical replication, MySQL binlog, or MongoDB change-stream adapters;
- a continuously resident run loop, daemon, leader election, or a `resume` command;
- a first-party destination `cdc_apply` implementation;
- protocol-specific connection, decoding, prerequisite, snapshot, or retention-gap behavior;
- parallel log partitions, cross-database transactions, or undocumented MongoDB transaction
  grouping;
- compatibility readers, migrations, aliases, or legacy artifact support.

## Acceptance criteria

- [ ] One neutral typed archetype represents ordered committed-log transactions and opaque ordered
      event prefixes without database-name branches or a universal wire-protocol trait.
- [ ] A row/byte/time/termination closure request received inside a settlement unit waits for the
      proven terminal boundary, records exact phase-local overshoot, and admits no later unit.
- [ ] No safe frontier, destination mutation, receipt, or checkpoint authority can be produced for
      a partially observed transaction or event prefix; restart retains the prior committed
      checkpoint.
- [ ] Multi-batch transactions remain bounded by the compiled `maximum_transaction_bytes`; the
      resolved host spill budget is the hard ceiling, a resource may only lower it, and exceeding
      the effective limit fails before checkpoint advance.
- [ ] Every admitted CDC batch has validated typed operation and exact source position metadata;
      insert/update require complete rows, delete requires the exact key-only shape, and successful
      finalization delegates to the A1.5 canonical keyed-effect reducer.
- [ ] Source-position scope, regression, unsupported event, inconsistent terminal position, and
      impossible aggregation failures retain narrow typed provenance and fail before publication.
- [ ] A deterministic synthetic source proves committed-frontier and Mongo event-prefix behavior
      under randomized Arrow rechunking, cadence boundaries, cancellation/failure injection,
      within/over-limit settlement units, and `jobs` invariance.
- [ ] A finite-drain conformance certificate proves package finalization, exact receipt settlement,
      checkpoint advancement, and crash recovery without introducing a second runtime lifecycle.
- [ ] Focused affected-package tests, formatting, check, and strict affected-package Clippy pass.

## References

- `.10x/specs/cdc-log-source-foundation.md`
- `.10x/specs/cdc-source-position-artifacts.md`
- `.10x/specs/package-keyed-delete-effects.md`
- `.10x/specs/stream-epochs-watermarks.md`
- `.10x/specs/checkpoint-state-commit-gate.md`
- `.10x/decisions/kernel-owned-stream-epoch-policy.md`
- `.10x/knowledge/developer-build-duckdb-linkage.md`

## Assumptions

- Record-backed: `ExecutionExtent::Drain`, `DrainEpochController`, package finalization,
  destination receipts, and checkpoint commit remain the sole runtime and durable advance
  authorities.
- User-ratified: the resolved host spill budget is the hard maximum for one PostgreSQL/MySQL
  transaction; a resource may lower but never raise it, and no kernel numeric default exists.
- Record-backed: MongoDB publishes an adapter-proven ordered event-prefix terminal token and is not
  grouped into undocumented source transactions.
- Record-backed: initial CDC execution has one ordered source partition; concurrency configuration
  cannot change event order or package identity.

## Journal

- 2026-08-07: Opened the executable A2 owner after A1 source-position authority and A1.5
  package-native keyed effects closed. The active CDC foundation spec contains the complete
  semantics; no behavior-changing assumption remains for this runtime boundary.

## Blockers

None. The ticket is executable after this record-publication turn.

## Evidence

Pending implementation.

## Review

Pending implementation and the program-level review barrier.

## Retrospective

Pending implementation.
