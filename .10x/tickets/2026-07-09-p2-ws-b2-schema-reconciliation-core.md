Status: open
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-08-p2-ws-b-schema-reconciliation-arrow-vocabulary.md
Depends-On: .10x/decisions/data-onramp-schema-discovery-reconciliation.md, .10x/specs/data-onramp-schema-intelligence.md, .10x/specs/types-contracts-normalization.md, .10x/tickets/done/2026-07-08-p2-ws-b1-declarative-arrow-type-vocabulary.md

# P2 WS-B2 schema reconciliation core

## Scope

Implement the format-independent schema reconciliation core: observed physical schema is fact; declared schema, pinned snapshots, or hints constrain/project it; the output is a reconciled Arrow schema plus a serializable verdict-bearing coercion plan. This child does not integrate every source format yet.

Owned write scope:

- `crates/cdf-contract/src/**` for reconciliation types, widening lattice, and focused tests;
- `crates/cdf-kernel/src/metadata.rs` and kernel metadata tests only if a shared `cdf:physical_type` metadata helper is needed;
- `crates/cdf-declarative/src/**` only for minimal tests or plumbing that proves a declared schema can call the reconciler without changing runtime file/REST/SQL behavior;
- this ticket's evidence and review records.

## Acceptance criteria

- A public reconciliation API accepts an observed Arrow `Schema`, a constraint Arrow `Schema`, and contract type policy, then returns:
  - a reconciled Arrow `Schema`;
  - a serializable coercion/verdict plan naming each preserved, widened, lossy-rejected, unsupported, missing, or extra field decision.
- Field matching is by source-original name: `cdf:source_name` metadata when present, otherwise the Arrow field name. Output field names preserve the constraint schema's names/metadata after normalization-facing metadata is applied.
- Reconciled fields that differ from the observed physical field carry `cdf:physical_type` metadata recording the observed Arrow type string.
- Automatic lossless widenings are implemented and tested:
  - signed integer widths widening within signed integers through `int64`;
  - unsigned integer widths widening within unsigned integers through `uint64`;
  - `float32 -> float64`;
  - integer -> decimal128/256 when declared precision and scale can exactly hold the source integer domain;
  - `date32 -> timestamp(<declared unit>, <declared timezone>)`.
- Unsupported or lossy mappings fail closed unless the existing type policy explicitly permits the relevant allowance. String parse-coercions remain opt-in through `coerce_types` and are not treated as automatic widenings.
- Plan-time errors name the field, observed type, declared/constraint type, and the two operator fixes where applicable: widen/change the declaration, or enable the relevant explicit coercion/lossy allowance.
- Tests cover representative success and failure cases for the lattice, missing/extra field classification, physical provenance metadata, and serialization of the coercion plan.

## Evidence expectations

Record focused evidence for:

- `cargo test -p cdf-contract <new reconciliation tests> --locked`;
- `cargo test -p cdf-contract --locked`;
- `cargo clippy -p cdf-contract --all-targets --locked -- -D warnings`;
- `cargo fmt --all -- --check`;
- `jscpd` scoped to touched Rust files;
- `rust-code-analysis-cli` scoped to new reconciliation code;
- `git diff --check`.

If the implementation touches kernel/declarative crates, include focused tests and clippy for those crates too.

## Explicit exclusions

This ticket does not integrate Parquet, NDJSON, REST, SQL, discovery snapshots, source readers, package writing, destination mapping, row execution of coercions, or conformance golden paths. Later WS-B children own per-format integration and validation-program execution.

## Progress and notes

- 2026-07-09: Opened after B1 closed declarative type expressibility. Source inspection found existing `ObservedSchema`, `ArrowType`, type-policy flags, and schema verdict vocabulary in `cdf-contract`, but no shared observed-vs-constraint reconciler or `cdf:physical_type` provenance helper.

## Blockers

None.
