Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/evidence/2026-07-11-p0-fx1-shared-schema-materialization.md
Verdict: pass

# Review: shared physical schema materialization

## Assumptions tested

- A codec can emit physical Arrow without selecting the effective schema.
- The typed engine plan, rather than source metadata, remains the authority for coercion.
- Projection, widening, missing nullable columns, and provenance can be materialized in one format-neutral implementation.
- Existing metadata-injection defenses remain effective.

## Findings

No critical or significant finding remains in this slice. The contract layer validates output order and count against the effective constraint, validates every physical source name/type against the plan, rejects duplicate physical source identities, rejects non-materializable verdicts, and validates its resulting schema/evidence before returning it. Focused injection tests still pass.

The cast allocations are not yet charged to the unified ledger. That is not accepted as residual architecture: it remains an explicit unfinished FX1 integration requirement before production composition and deletion of the old path.

## Verdict

Pass for the schema-stage separation. FX1 remains open for accounted batch ownership, production registry routing, superseded-path deletion, and full conformance.

## Residual risk

Arrow cast support is broader than CDF policy. Safety depends on the typed plan admitting only ratified transitions; the materializer therefore treats the plan as authority and never invents a coercion from `can_cast_types` alone.
