Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Target: .10x/tickets/done/2026-07-09-p2-ws-b2-schema-reconciliation-core.md
Verdict: pass

# P2 WS-B2 schema reconciliation core review

## Target

Reviewed the B2 implementation in `crates/cdf-contract/src/reconciliation.rs`, its export from `crates/cdf-contract/src/lib.rs`, tests in `crates/cdf-contract/src/tests.rs`, and the shared `cdf:physical_type` metadata helper in `crates/cdf-kernel/src/metadata.rs`.

## Findings

- Pass: The API has a strict `reconcile_schema` path for fail-closed callers and an inspectable `plan_schema_reconciliation` report for CLI/planning callers that need all field decisions before rendering an error.
- Pass: Field matching uses source-original identity through `cdf:source_name` when present, and falls back to Arrow field names otherwise.
- Pass: Constraint field names and metadata are preserved in the reconciled schema, while missing `cdf:source_name` metadata is added so downstream evidence keeps source-original identity.
- Pass: `cdf:physical_type` is attached when reconciliation changes type or field identity, covering both widening/coercion and source-to-normalized rename cases.
- Pass: Automatic widenings are limited to the B2-ratified lattice. String parse coercions are separate and require `coerce_types`; lossy numeric/time mappings require `allow_lossy_mapping`.
- Pass: Unsupported and disallowed mappings produce fatal plan decisions and errors that name field, observed type, declared type, and operator fixes.
- Minor: The integer-to-decimal exactness rule is conservative and based on full integer-domain digit capacity. That is appropriate for a compiler-stage widening default, but later row-level execution may be able to admit narrower observed samples only as an explicit policy, not as this automatic lattice.
- Minor: Extra observed fields are classified in the plan but do not fail the core reconciler because B2 receives only `TypePolicy`, not `SchemaPolicy`. Later per-format/policy integration must decide whether extra fields are admitted, variant-captured, quarantined, or rejected.

## Verdict

Pass. The implementation satisfies the B2 ticket scope and does not integrate runtime readers or package paths prematurely.

## Residual Risk

The main residual risk is integration drift: later source-format children must call this shared reconciler instead of rebuilding similar checks in Parquet, NDJSON, REST, SQL, or CLI validation paths. B2 evidence is unit-level and crate-level, not conformance-level.
