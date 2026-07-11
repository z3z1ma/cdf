Status: done
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/done/2026-07-08-p2-ws-b-schema-reconciliation-arrow-vocabulary.md
Depends-On: .10x/tickets/done/2026-07-09-p2-ws-b2-schema-reconciliation-core.md, .10x/specs/data-onramp-schema-intelligence.md, .10x/specs/data-onramp-conformance.md

# P2 WS-B4 widening property conformance

## Scope

Add conformance-owned property tests for the schema-reconciliation widening lattice: widening composition and no value loss for supported Arrow integer/float/date widening families.

## Acceptance criteria

- Property tests generate representative Arrow arrays for supported widening families and prove values survive the reconciler-selected materialization path.
- Composition is covered for at least one multi-step signed integer chain, one unsigned integer chain, and the float32-to-float64 chain where applicable.
- Reconciliation decisions remain verdict-bearing: generated widened fields produce `FieldCoercionDecision::Widened`, not `Preserved`, `LossyAllowed`, or `Unsupported`.
- The tests live in `cdf-conformance` so WS-I owns the property law rather than relying only on unit examples.

## Evidence expectations

Focused `cargo test -p cdf-conformance widening --locked`, relevant formatting/clippy checks for touched Rust, and a closure evidence/review record.

## Explicit exclusions

This ticket does not implement new casts, validation-program serialization, string parse coercion, decimal precision arithmetic beyond integer-to-decimal reconciliation classification, or destination-specific type mapping.

## Progress and notes

- 2026-07-09: Opened as an independent local slice while E2 and G1 run in parallel. This ticket deliberately touches only conformance/property code and WS-B/WS-I records.
- 2026-07-09: Added `cdf-conformance` property tests for signed integer widening chains, unsigned integer widening chains, float32-to-float64 preservation, and date32-to-timestamp day-instant preservation. The tests assert generated reconciliations produce `FieldCoercionDecision::Widened` and materialize generated Arrow arrays through the shared coercion path without value loss. Closure evidence: `.10x/evidence/2026-07-09-p2-e2-g1-b4-batch.md`. Review: `.10x/reviews/2026-07-09-p2-e2-g1-b4-batch-review.md`.

## Blockers

None.
