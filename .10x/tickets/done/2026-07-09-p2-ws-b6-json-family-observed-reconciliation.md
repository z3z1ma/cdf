Status: done
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/done/2026-07-08-p2-ws-b-schema-reconciliation-arrow-vocabulary.md
Depends-On: .10x/tickets/done/2026-07-09-p2-ws-b2-schema-reconciliation-core.md, .10x/tickets/done/2026-07-09-p2-ws-b5-validation-program-coercion-evidence.md, .10x/decisions/source-decode-type-drift-quarantine.md, .10x/specs/data-onramp-schema-intelligence.md

# P2 WS-B6 JSON-family observed-first reconciliation

## Scope

Replace the remaining JSON/NDJSON declared-schema-as-decoder-truth path with the shared observed-physical-schema reconciliation model. JSON-family readers MUST observe bounded physical facts, reconcile declared or pinned constraints through `cdf-contract`, materialize supported casts, preserve physical provenance, and retain the existing localized source-decode quarantine behavior.

This ticket owns local JSON and NDJSON file resources. It may extract shared helpers usable by REST, but REST runtime integration remains a separate child.

## Acceptance criteria

- JSON and NDJSON readers infer/observe the bounded input schema before applying declared or pinned constraints.
- The shared reconciliation stage records preserved, widened, parsed-coercion, lossy-allowed, missing, and extra-field decisions consistently with the existing Parquet path.
- Automatic lossless widenings materialize without value loss and carry `cdf:physical_type` metadata.
- Parse coercions require `coerce_types`; lossy casts require `allow_lossy_mapping`; disallowed cases name the field, observed type, constraint type, and both fixes.
- Localized row-field drift still produces deterministic `source_type_mismatch` quarantine facts while accepted rows preserve order and continue.
- Validation-program/package evidence contains the JSON-family coercion plan deterministically.
- Preview and package-producing execution use the same JSON-family reconciliation front end.

## Evidence expectations

Focused `cdf-formats`, `cdf-declarative`, `cdf-engine`, and package-evidence tests; adversarial malformed/localized-drift cases; widening/no-loss coverage reuse; preview/run parity coverage; and the `QUALITY.md` input-boundary/security/test profiles appropriate to the change set.

## Explicit exclusions

CSV, Arrow IPC, REST, Postgres, cloud transports, multi-file union discovery, and `SchemaSource::Hints` remain separate work. This ticket does not invent new contract flags or coercion families.

## Progress and notes

- 2026-07-09: Opened after the read-only P2 audit confirmed that JSON/NDJSON still build arrays directly from the declaration instead of reconciling observed physical facts through the shared model.
- 2026-07-09: Implemented the local JSON/NDJSON observed-first path. Accepted rows are decoded with an inferred physical Arrow schema, reconciled through `cdf-contract`, and projected/cast through the same materialization helper as Parquet. Automatic integer-to-decimal widening, `cdf:physical_type`, source-name projection, and extra-field projection are covered. JSON documents normalize into the same NDJSON front end.
- 2026-07-09: Preserved deterministic source-decode quarantine before Arrow materialization. Native scalar-kind mismatches remain `source_type_mismatch`, retain original row order/ordinal, honor `cdf:source_name`, and preserve PII redaction; malformed JSON still fails closed. An all-quarantined input retains the prior declared empty-batch behavior because there is no accepted physical batch to reconcile.
- 2026-07-09: Added explicit policy-aware `cdf-formats` APIs. Existing/live declarative calls remain fail-closed with `coerce_types = false` and `allow_lossy_mapping = false`; parse and lossy casts materialize only when a caller supplies an explicit `TypePolicy` allowance. No trust preset implicitly enables JSON parse coercion.
- 2026-07-09: Reconciliation now serializes its exact deterministic plan into reconciled schema metadata. Package execution captures that input plan before downstream execution can replace schema metadata, so widened, parsed-coercion, lossy-allowed, preserved, and extra decisions reach `plan/validation-program.json` and `schema/coercion-plan.json` exactly.
- 2026-07-09: Focused verification passed: `cargo test -p cdf-contract -p cdf-formats -p cdf-declarative -p cdf-engine --locked` (32 contract, 25 formats, 79 declarative, 31 engine unit tests plus doctests); `cargo fmt --all -- --check`; `cargo check -p cdf-contract --all-targets --all-features --locked`; `cargo check -p cdf-contract --all-targets --no-default-features --locked`; `cargo clippy -p cdf-contract -p cdf-formats -p cdf-declarative -p cdf-engine --all-targets --locked -- -D warnings`; and `git diff --check` all exited 0.
- 2026-07-09: Input-boundary/test-quality evidence includes adversarial malformed input, localized drift with accepted-row continuation, source-name override quarantine, lossless decimal materialization, denied/allowed parse and lossy policy gates, JSON/NDJSON front-end equivalence, preview/run same-partition equivalence, and exact package evidence. `jscpd` exited 0 over eight touched Rust files with 36 clone blocks/445 duplicated lines (4.88%), predominantly repetitive existing test scaffolding; no production duplication justified widening the diff. Reports: `target/codex-b6/quality/jscpd-p2-b6/jscpd-report.json`, `target/codex-b6/quality/rust-code-analysis-p2-b6/reconciliation.json`, and `target/codex-b6/quality/rust-code-analysis-p2-b6/readers.json`.
- 2026-07-09: Verification limit: the parent agent owns combined workspace/deep checks, adversarial review, durable evidence record, and closure. This child did not run network/live conformance, fuzzing, coverage, mutation testing, audit, or full-workspace tests.
- 2026-07-09: Closure repair removed Arrow schema metadata as an authority boundary. Exact reconciliation JSON now also rides the internal, serde-backward-compatible `BatchHeader.schema_coercion_plan` channel populated only by reconciled format reads. Package execution rejects source-carried `cdf:schema_coercion_plan` metadata when the internal header is absent, parses the internal evidence fallibly, requires header/schema metadata equality when both exist, validates unique/deterministic field coverage, output/source identities, observed/constraint types, decision/type relationships, outcomes, reasons, and extra-field ordering, and rejects inconsistent plans across input batches.
- 2026-07-09: Added trust-boundary negatives for malformed metadata, a syntactically valid false widening plan, syntactically valid Arrow IPC-carried metadata, Parquet's observed metadata-stripping behavior, source metadata presented to engine without an internal header, and malformed internal header JSON. Exact widened/lossy/extra package evidence remains green and deterministic; legacy batch-header JSON without the new optional field still deserializes to `None`.
- 2026-07-09: Closure repair made the JSON scalar prefilter policy-aware. Explicit `coerce_types` now admits string-to-decimal rows to shared reconciliation/casting; without the allowance those rows produce localized `source_type_mismatch` quarantine. Fractional JSON numbers in integer declarations likewise quarantine only the offending row unless `allow_lossy_mapping` is explicit, preventing mixed integer/float input from widening inference and failing the whole file.
- 2026-07-09: Post-repair focused verification passed: `cargo test -p cdf-kernel -p cdf-contract -p cdf-formats -p cdf-declarative -p cdf-engine --locked` (11 kernel, 34 contract, 29 formats, 79 declarative, 33 engine unit tests plus doctests); targeted trust-boundary/package/NDJSON/header tests; `cargo check -p cdf-contract --all-targets --all-features --locked`; `cargo check -p cdf-contract --all-targets --no-default-features --locked`; `cargo clippy -p cdf-kernel -p cdf-contract -p cdf-formats -p cdf-declarative -p cdf-engine --all-targets --locked -- -D warnings`; `cargo fmt --all -- --check`; and `git diff --check` all exited 0.
- 2026-07-09: Repair quality reports: `target/codex-b6/quality/jscpd-p2-b6-repair/jscpd-report.json` (7 files, 16 clone blocks, 197 duplicated lines/2.74%; repetitive test scaffolding, no production refactor warranted) and `target/codex-b6/quality/rust-code-analysis-p2-b6-repair/{reconciliation,readers,execution}.json`.
- 2026-07-09: Final provenance repair requires reserved schema metadata and the internal batch header to both exist and match; valid header-only fabricated `Extra` evidence now fails closed. Closed with `.10x/evidence/2026-07-09-p2-a8-b6-i3-integration.md` and `.10x/reviews/2026-07-09-p2-a8-b6-i3-integration-review.md`. Reusable trust-boundary learning is recorded in `.10x/knowledge/schema-coercion-evidence-provenance.md`.
- 2026-07-09: Residual closure repair tightened the evidence handshake: `BatchHeader.schema_coercion_plan = Some(...)` is accepted only when the RecordBatch schema also contains the reserved reconciler-written metadata key and its decoded plan is exactly equal. Valid header-only evidence is rejected before structural plan use. Added a syntactically valid header-only injection carrying a fabricated `Extra` decision plus a direct contract negative; internally reconciled JSON/Parquet exact-plan package tests remain green.
- 2026-07-09: Residual-repair verification passed: focused contract schema-coercion/header-handshake tests, engine package-injection and package-evidence tests, formats NDJSON and Arrow/Parquet metadata tests, `cargo fmt --all -- --check`, scoped `cargo clippy ... -- -D warnings`, and `git diff --check` all exited 0.

## Blockers

None. The active schema reconciliation decision and existing source-decode quarantine decision fully govern this slice.
