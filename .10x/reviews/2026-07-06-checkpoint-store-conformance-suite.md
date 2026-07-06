Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-06-checkpoint-store-conformance-suite.md
Verdict: pass

# Checkpoint store conformance suite review

## Target

Implementation and records for `.10x/tickets/done/2026-07-06-checkpoint-store-conformance-suite.md`, including `crates/firn-conformance/**`, the minimal `firn-state-sqlite` test integration, `Cargo.lock`, and evidence `.10x/evidence/2026-07-06-checkpoint-store-conformance-suite.md`.

## Assumptions tested

- The harness must exercise only the public `CheckpointStore` trait and public kernel checkpoint values, with no private store hooks.
- The suite must test all ticket-listed checkpoint-store behavior, including schema-hash coverage and segment row/byte count coverage beyond the earlier one-segment local test shape.
- SQLite-specific protection tests must remain in `firn-state-sqlite`.
- The crate root for `firn-conformance` must stay thin.
- A reusable conformance harness needs evidence that the harness itself can fail on faulty implementations, not only that real stores pass it.

## Findings

None blocking.

Significant finding resolved: the first reusable harness shape had no local negative self-tests, so mutation testing showed that a broken or no-op harness could survive when only downstream store tests were selected. A combined harness/downstream mutation run then exposed two additional count-direction survivors. The final implementation adds intentionally faulty stores and negative self-tests for no-op execution, receipt row/byte counts in both directions, proposed heads, missing heads/history, invalid rewind acceptance, wrong rewind reports, and implausible timestamps. The final bounded `cargo mutants` run over `crates/firn-conformance/src/checkpoint_store/*.rs` reported 0 missed mutants.

Minor no-action rationale: `firn-state-sqlite` still retains older local contract-style tests alongside the new reusable conformance-suite calls. This duplicates some assertions, but it preserves the existing protective coverage exactly and avoids a larger test-file rewrite in this child. No follow-up ticket is recommended unless the parent wants a later cleanup-only pass.

Minor no-action rationale: `jscpd` reports expected duplication after lifting a reusable harness while intentionally preserving SQLite-specific local tests. The duplication does not justify abstracting this child further because the retained local tests protect SQLite-only behavior and the new conformance module owns the shared contract.

## Verdict

Pass. The implementation meets the child ticket acceptance criteria, required targeted commands pass, final mutation testing has 0 missed mutants for the conformance module, and SQLite-only tests remain responsible for SQLite-specific guarantees.

## Residual risk

This review does not cover resource conformance, destination conformance, chaos killpoints, golden-package fixtures, or full parent-plan closure; those remain outside this child ticket's explicit scope. Existing repository supply-chain policy blockers remain owned by `.10x/tickets/2026-07-06-ratify-supply-chain-policy.md`.
