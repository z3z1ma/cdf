Status: open
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p2-rp9-promotion-execution-recovery-gc.md
Depends-On: .10x/tickets/2026-07-10-p2-rp9a-promotion-artifact-recovery-authority.md, .10x/tickets/2026-07-10-p2-rp9b-atomically-fenced-promotion-settlement.md, .10x/tickets/done/2026-07-10-parquet-promotion-identifier-policy.md

# RP9C promotion command, concurrency, and cross-destination conformance

## Scope

Prove the complete promotion command across multiple targets and implemented destination strategies, and make failure/recovery output satisfy the P1/JSON contract without weakening commit semantics.

## Acceptance criteria

- CLI human/JSON output reports the persisted current phase, committed targets, remaining action, and exact recovery command for every crash/failure boundary; intermediate states are real report values rather than unreachable enum variants.
- Multi-target execution proves deterministic package/checkpoint order, later-target failure/recovery, no publication before all exact targets commit, and exact publication target equality.
- DuckDB and Postgres in-place promotion execute through the command; Parquet correction-sidecar promotion executes after its identifier-policy dependency. Protocol/session dispatch remains destination-name-free.
- Promotion-vs-promotion, pin-vs-promotion, and run-vs-promotion races are tested. A run planned under old schema authority cannot advance an incompatible checkpoint after promotion publishes; the shared commit gate provides a typed authority check rather than a CLI lock hack.
- Secret-bearing destination configuration is redacted from staged artifacts, errors, human output, and JSON output.
- A version-3 run-ledger database migrates additively to publication support while prior run events remain readable and new publication operations work.

## Evidence expectations

Command-level DuckDB/Postgres/Parquet scenarios, multi-target and later-target failpoints, run/pin/promotion race harnesses, phase/render snapshots, secret fixture, v3-to-v4 migration fixture, generated CLI artifacts, full affected tests, and independent review.

## Explicit exclusions

No new destination strategy, arbitrary update SQL, distributed scheduler, or GC classification.

## Progress and notes

- 2026-07-10: Opened from the coverage/output/concurrency findings in `.10x/reviews/2026-07-10-p2-rp9-promotion-execution-independent-review.md`.
- 2026-07-10: Completed a read-only implementation/conformance preflight at `.10x/research/2026-07-10-rp9c-conformance-preflight.md`. Active specs imply canonical `(destination_id, target)` ordering, one deterministic schema-contract checkpoint chain, committed-prefix recovery, and exact publication equality. The record defines the DuckDB/Postgres/Parquet command fixtures, later-target/source-deletion and takeover matrices, run/pin/promotion races, legacy v3-to-v4 migration, secret-redaction and P1/JSON assertions, reusable harness seams, and code-smell traps. RP9C remains open and inactive pending RP9A/RP9B.

## Blockers

Depends on RP9A, RP9B, and the ratified Parquet namespace implementation.
