Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Target: .10x/tickets/done/2026-07-09-p2-ws-b5-validation-program-coercion-evidence.md
Verdict: pass

# P2 WS-B5 validation-program coercion evidence review

## Target

Reviewed the B5 implementation in `crates/cdf-contract/src/compiler.rs`, `crates/cdf-contract/src/program.rs`, `crates/cdf-contract/src/reconciliation.rs`, `crates/cdf-contract/src/tests.rs`, `crates/cdf-engine/src/execution.rs`, and `crates/cdf-engine/src/tests.rs`.

## Findings

- Pass: `ValidationProgram.schema_coercion` is optional, skipped when absent, defaults to `None` on deserialize, and is initialized as `None` by the compiler.
- Pass: Package execution preserves the previous early `plan/validation-program.json` artifact for failure diagnostics, then rewrites it with enriched coercion evidence on successful completion.
- Pass: `schema/coercion-plan.json` is written only when coercion evidence exists, avoiding package identity churn for ordinary packages without physical provenance.
- Pass: Evidence extraction is deterministic and schema-local: it derives source name, output name, observed type, constraint type, decision, and outcome from reconciled Arrow schema metadata and data types, with no wall-clock or host-path inputs.
- Pass: `schema/output.json` emits CDF metadata only for fields with `cdf:physical_type`, carrying `cdf:source_name` alongside physical provenance while avoiding broad metadata additions to preserved fields.
- Pass: Focused tests prove the critical `Int32 -> Int64` widening evidence path, preserved-field classification, package artifact equality between validation-program evidence and schema artifact, and physical provenance serialization.
- Minor: Because the concurrent D4 tree owns `cdf-formats` and `cdf-declarative`, B5 did not alter or rerun the actual declared Parquet reader integration. The implementation relies on the B3-established reconciled schema metadata contract rather than changing `FormatRead` to expose reader-local plans.

## Verdict

Pass. The B5 implementation satisfies the package evidence slice without touching the concurrently edited reader crates.

## Residual Risk

Future policy-coercion families may need richer reader-to-engine plan plumbing if output schema metadata is not sufficient to reconstruct exact policy reasons. The current B5 slice covers the existing automatic widening and physical-provenance path required by the ticket.
