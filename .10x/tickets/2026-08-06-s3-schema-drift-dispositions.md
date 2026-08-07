Status: open
Created: 2026-08-06
Updated: 2026-08-06
Parent: `.10x/tickets/2026-08-06-state-backed-schema-authority-program.md`
Depends-On: `.10x/tickets/2026-08-06-s1-state-schema-authority-foundation.md`

# S3 total schema drift dispositions

## Scope

Replace coupled evolution/quarantine policy with the ratified total admission model across contract,
declarative/project lowering, engine/runtime, packages, destinations, reports, and supported-source
conformance:

- delete `SchemaEvolutionMode::Evolve|Freeze`, automatic output widening, duplicate allow-new/
  unknown flags, and `quarantine.enabled`;
- introduce typed field/row/record/partition dispositions and shared evidence redaction policy;
- encode the exact Experimental/Governed/Financial/Serving presets;
- replace `control_critical` as decision authority with compiler-derived ordinary/required/
  identity/progress/CDC-operation/transaction roles and allowed action sets;
- preserve the exact `_cdf_variant` codec and make variant presence plan/schema-visible;
- keep accepted-with-residual distinct from quarantine in packages, receipts, checkpoints,
  telemetry, human output, and JSON;
- prove quarantine-only durable settlement before advancement;
- make plan show observed drift, disposition, wider-fetch cost, and no source-driven migration;
- retain source-specific isolation/advancement proofs behind adapter contracts.

## Non-goals

- changing active schema heads or implementing promotion;
- inventing a new variant encoding or storing raw residual values in state;
- destination-specific admission semantics in generic runtime;
- nested promotion, automatic DDL, or business-key correction;
- weakening source-position/CDC/transaction correctness to keep a run alive.

## Acceptance criteria

1. Current policy serialization contains no evolution mode, global quarantine switch, or impossible
   quarantine-verdict/disabled-mechanism combination.
2. Every relevant observation compiles to one typed total disposition at enforceable grain.
3. Four trust presets exactly match the ratified matrix, including Financial fail strictness and
   Serving's separately governed sampled fast path.
4. Field roles prevent variant/quarantine when identity/progress/operation/transaction correctness
   cannot be preserved.
5. Exact variant bytes, non-reconstructable redaction metadata, multi-path envelopes, and nulling
   behavior remain canonical.
6. Main accepted rows, variant rows, quarantined evidence, failed resources, receipt/checkpoint
   outcomes, and schema-head immutability are asserted for each supported source family.
7. Quarantine-only input cannot advance until canonical evidence and an empty/metadata settlement
   proof are durable.
8. Plan/run typed reports show exact dispositions/counts without double-counting or secret leakage.
9. Affected fmt/check/focused tests/strict Clippy and behavioral conformance pass.

## References

- `.10x/decisions/state-backed-schema-authority.md`
- `.10x/specs/schema-drift-dispositions.md`
- `.10x/specs/residual-variant-capture.md`
- `.10x/specs/schema-discovery-and-stream-admission.md`
- `.10x/specs/types-contracts-normalization.md` (only clauses not superseded by the drift spec)
- `.10x/specs/checkpoint-state-commit-gate.md`
- `.10x/knowledge/cli-report-authority.md`

## Assumptions

- User-ratified: exact four preset matrix in the governing spec.
- User-ratified: there is no unlocked/evolving established state and no global quarantine toggle.
- Record-backed: missing nullable remains typed null and compiled lossless coercion is not drift.
- Record-backed: control/source-advancement safety outranks row availability.

## Journal

- 2026-08-06: Opened dependency-gated behind S1 and independent of S2 after the shared types land.

## Blockers

S1 must provide the immutable-head oracle used by immutability assertions.

## Evidence

Pending execution.

## Review

Pending handback review.

## Retrospective

Pending execution.
