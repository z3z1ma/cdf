Status: done
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-08-p2-ws-b-schema-reconciliation-arrow-vocabulary.md
Depends-On: .10x/tickets/done/2026-07-09-p2-ws-b2-schema-reconciliation-core.md, .10x/tickets/done/2026-07-09-p2-ws-b3-parquet-declared-schema-reconciliation.md, .10x/specs/data-onramp-schema-intelligence.md, .10x/decisions/data-onramp-schema-discovery-reconciliation.md

# P2 WS-B5 validation-program coercion evidence

## Scope

Serialize schema reconciliation/coercion decisions into the runtime evidence path so automatic width widenings and physical provenance are not only reader-local behavior. The validation program and package artifacts MUST contain enough structured evidence for an operator, replay verifier, or conformance test to see which fields were preserved, widened, policy-coerced, lossy-allowed, rejected, missing, or extra.

This ticket owns the smallest complete evidence slice:

- Extend the contract validation-program model with a backwards-compatible, optional schema coercion evidence field or equivalent nested artifact reference.
- Thread reconciliation plans produced by current Parquet declared-schema integration into that evidence model without changing package hashes except through the newly recorded evidence artifact.
- Ensure `plan/validation-program.json` and/or a dedicated `schema/coercion-plan.json` package artifact serializes the exact `SchemaCoercionPlan` decisions with `cdf:physical_type` provenance preserved.
- Add tests proving a local Parquet `int32 -> int64` declared-schema run records a `widened` decision and the observed physical type, and proving an unchanged/preserved field remains recorded without being misclassified.
- Keep the runtime deterministic: evidence ordering is stable and no wall-clock or host paths enter the coercion evidence.

## Acceptance Criteria

- `ValidationProgram` serde remains backward compatible for existing JSON lacking coercion evidence.
- A package created from a declared local Parquet schema with a lossless widening contains structured coercion evidence in the plan/schema artifacts.
- The evidence names both observed and constraint types for widened fields and classifies the decision as `widened`.
- Package output schema still carries `cdf:physical_type` metadata where the reconciler required it.
- Existing package replay, receipt, and golden behavior remains valid aside from intentional artifact identity changes caused by the new evidence file.

## Evidence Expectations

Focused tests in `cdf-contract`, `cdf-engine`, and/or `cdf-project` proving serde compatibility and package artifact contents; `cargo fmt --all -- --check`; focused package/execution tests; and inclusion in the next batch evidence record with quality gate limits.

## Explicit Exclusions

This ticket does not implement new coercion families, string parse coercions, `Hints`, NDJSON/REST/SQL reconciliation rewrites, or destination-specific type mapping.

## Progress and Notes

- 2026-07-09: Split from WS-B after B4. The existing reconciler and Parquet declared-schema integration record decisions locally, but package evidence still only writes the validation program without a reconciliation/coercion payload.
- 2026-07-09: Implemented optional `ValidationProgram.schema_coercion`, deterministic schema-coercion evidence extraction from reconciled output schemas carrying `cdf:physical_type`, package writes for `plan/validation-program.json` and `schema/coercion-plan.json`, and `schema/output.json` physical provenance metadata for reconciled fields. Evidence: `.10x/evidence/2026-07-09-p2-ws-b5-validation-program-coercion-evidence.md`. Review: `.10x/reviews/2026-07-09-p2-ws-b5-validation-program-coercion-evidence-review.md`.

## Blockers

None.
