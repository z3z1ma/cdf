Status: done
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/done/2026-07-10-p2-rp9-promotion-execution-recovery-gc.md
Depends-On: .10x/tickets/done/2026-07-10-p2-rp9a-promotion-artifact-recovery-authority.md, .10x/tickets/done/2026-07-10-p2-rp9b-atomically-fenced-promotion-settlement.md, .10x/tickets/done/2026-07-10-parquet-promotion-identifier-policy.md

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
- 2026-07-10: Implemented the deterministic multi-target promotion checkpoint chain. Correction packages are built in canonical target order, each checkpoint consumes the prior target's projected committed authority, and fenced settlement rejects any checkpoint whose parent/input does not equal the exact current committed head. Command conformance now proves canonical order, the `H0 -> T0 -> T1` chain, and exact publication equality for two DuckDB targets. Focused test `schema_promote_multi_target_uses_canonical_checkpoint_chain_and_exact_publication` passes; remaining crash-boundary, cross-destination, race, redaction, and migration criteria stay open.
- 2026-07-10: Extended the multi-target command scenario through a later-target crash: execution is interrupted after target index 1 checkpoints, both originating source packages are deleted, and the authenticated correction packages recover to exact publication without re-extraction. This proves the committed-prefix/source-independent recovery path rather than only the clean two-target path.
- 2026-07-10: Added an explicit run-ledger v3-to-v4 migration fixture. It preserves and reads the pre-existing run event, reports the component migration, creates publication authority, and successfully writes/reads a new promotion publication after migration.
- 2026-07-10: Added a deterministic append-only recovery journal under each staged promotion. Every persisted crash boundary records a create-or-verify, secret-free phase event with committed/pending targets, remaining action, and exact recovery command; the journal is evidence only and does not replace checkpoint/publication authority. CLI JSON errors expose the structured status and human errors render the same fields. The full crash-boundary matrix asserts `staged`, `packaged`, `destination_settled`, `checkpointed`, `lock_published`, and `complete`; a secret-backed Postgres connection failure proves the resolved credential is absent from errors and all generated artifacts. Full affected suites pass: `cdf-project` 163, `cdf-state-sqlite` 37, and `cdf-cli` 257 tests.
- 2026-07-10: Added a live Postgres command scenario that reads exact residual evidence, adds the promoted column, updates addressed rows, clears residuals, commits the promotion checkpoint, and publishes through the same CLI path as DuckDB/Parquet. The scenario exposed and fixed a destination-boundary leak: promotion no longer calls raw protocol planning directly. `ProjectDestinationRuntime::prepare_correction_commit` now owns destination-specific preparation; Postgres performs live catalog inspection and binds its private correction request behind the generic trait. The promotion engine contains no destination-name branch. Postgres's full 40-test suite and the focused project/runtime tests pass.
- 2026-07-10: Closed run-vs-promotion authority at the shared SQLite checkpoint gate: after publication, a non-schema-contract checkpoint must carry the latest published schema hash for its resource in the same transaction that advances the head. Promotion-vs-promotion is covered by exclusive fenced scope leases; pin-vs-promotion is covered by the guarded lock CAS/stale-authority suite. Removed the recovery journal's provisional small sequence ranges in favor of checked `u64` target sequences with terminal values reserved for lock/publication. Final evidence is `.10x/evidence/2026-07-10-p2-rp9c-promotion-command-conformance.md`; the severity-focused adversarial review passes at `.10x/reviews/2026-07-10-p2-rp9c-promotion-command-review.md`.

## Blockers

None.
