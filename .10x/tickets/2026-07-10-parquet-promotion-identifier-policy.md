Status: active
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p2-residual-schema-promotion-program.md
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

## Blockers

None. The column/object-key namespace split is ratified.

## Evidence expectations

Decision/spec reference, sheet artifact diff, adapter tests, promotion dry-plan and execute reports, sidecar receipt/checkpoint inspection, and regression proof that object-key rules still fail when presented as column rules.
