Status: done
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/done/2026-07-10-p2-residual-schema-promotion-program.md
Depends-On: .10x/tickets/done/2026-07-09-p2-ws-c3-live-destination-normalization-duckdb-postgres.md, .10x/tickets/done/2026-07-10-p2-rp8-parquet-correction-sidecars.md

# Parquet promotion column identifier authority

## Scope

Ratify and implement the column-identifier policy used when schema promotion projects a residual path into a Parquet correction sidecar. The current Parquet destination sheet declares `object-key-component-v1`; that is an object-key rule, not a column normalizer, and the shared identifier adapter intentionally rejects it.

## Acceptance criteria

- An active decision or specification states whether Parquet columns use source `namecase-v1`, a distinct destination-sheet column rule, or another explicit policy.
- The Parquet sheet represents object-key and column identifier rules without overloading one field.
- Promotion planning normalizes and collision-checks Parquet promoted fields from the ratified column rule.
- A CLI conformance test executes `correction_sidecar` promotion through package, receipt, checkpoint, lock CAS, and publication event.
- Existing fail-closed coverage for unsupported non-column identifier rules remains intact.

## Explicit exclusions

No reinterpretation of `object-key-component-v1` as a column policy and no weakening of collision or normalization checks.

## Progress and notes

- 2026-07-10: RP9 cross-destination execution probing reached the existing `identifier_projection_unsupported` guard before mutation. RP8 correction-sidecar settlement is implemented; the missing authority is the separately deferred Parquet column-policy decision named by the completed C3 ticket.
- 2026-07-10: The user ratified `.10x/decisions/parquet-column-and-object-key-identifier-rules.md`: Parquet columns use `namecase-v1` with no length cap; `DestinationSheet.identifier_rules` is column-only; and `object-key-component-v1` moves into a distinct typed optional `ObjectKeyRules` capability. The ticket is activated for implementation and RP9 cross-destination proof.
- 2026-07-10: Implemented the typed namespace split, generic destination-sheet column-policy lookup, read-only Parquet sheet hook, shared ordinary/sidecar normalization coverage, and full CLI Parquet correction-sidecar execution. RP9A's strict live-receipt verification exposed a forged initial fixture; the final scenario establishes and verifies a real Parquet source receipt. Evidence: `.10x/evidence/2026-07-10-parquet-promotion-identifier-policy.md`. Review: `.10x/reviews/2026-07-10-parquet-promotion-identifier-policy-review.md`.
- 2026-07-10: Independent review `.10x/reviews/2026-07-10-parquet-promotion-identifier-policy-independent-review.md` correctly found that the first implementation broke public `DestinationSheet` construction and left object-key rules descriptive. Repaired both findings: the sheet is restored field-for-field; typed rules now travel through defaulted protocol capabilities/artifacts/locks; all Parquet key construction exhaustively dispatches through a validated capability-derived encoder; and missing declaration fails closed. Semver checks pass 196/196. Repair response: `.10x/reviews/2026-07-10-parquet-promotion-identifier-policy-repair-review.md`.
- 2026-07-10: Closed after independent re-review `.10x/reviews/2026-07-10-parquet-promotion-identifier-policy-independent-rereview.md` passed every criterion and the complete affected suite. Retrospective: column identifiers and object keys are distinct namespaces, and any new destination capability must enter through the defaulted protocol-capability artifact/lock seam and become executable authority at the adapter boundary. Those lessons are durable in `.10x/decisions/parquet-column-and-object-key-identifier-rules.md`, `.10x/decisions/destination-protocol-capabilities-extension-seam.md`, and the independent reviews; no unowned follow-up remains.

## Blockers

None. The column/object-key namespace split is ratified.

## Evidence expectations

Decision/spec reference, sheet artifact diff, adapter tests, promotion dry-plan and execute reports, sidecar receipt/checkpoint inspection, and regression proof that object-key rules still fail when presented as column rules.
