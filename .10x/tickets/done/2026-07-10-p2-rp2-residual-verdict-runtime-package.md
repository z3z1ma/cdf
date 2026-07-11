Status: done
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/done/2026-07-10-p2-residual-schema-promotion-program.md
Depends-On: .10x/tickets/done/2026-07-10-p2-rp1-residual-envelope-codec.md, .10x/tickets/done/2026-07-09-p2-ws-a10d-effective-schema-runtime-evidence.md, .10x/tickets/done/2026-07-09-p2-ws-b5-validation-program-coercion-evidence.md

# P2 RP2 residual verdict compiler, runtime, and package evidence

## Scope

Compile safe residual capture versus row/file quarantine at field/path grain; execute it for unknown fields, scalar mismatches, and isolated parse/coercion failures; materialize final nullable `_cdf_variant`; and serialize total evolution evidence into packages/replay.

## Acceptance criteria

- Discover/evolve defaults and freeze opt-in behavior match the active residual spec.
- Cursor, merge/primary key, required non-null, source-position, and operation-field violations quarantine rather than partial-admit.
- Safe nullable mismatches null only the typed field; unknown fields remain absent from typed output; conforming fields continue.
- `_cdf_variant` is last, nullable, semantically/version tagged, and null on clean rows.
- Encoding failure becomes `cdf.residual_encode_unsupported` quarantine with redaction, not an internal crash.
- Validation program, schema/output evidence, contract-evolution artifact, package identity, verification, and replay carry residual decisions and baseline/effective schema hashes.
- Different files/batches may emit different residual decisions under one verified effective schema without collapsing evidence.

## Evidence expectations

Compiler/runtime tests, mixed clean/residual/quarantine batches, package/tamper/replay checks, PII residual redaction, multi-file integration, and adversarial review.

## Explicit exclusions

No schema promotion command, row correction, destination capability changes, or retention behavior.

## Progress and notes

- 2026-07-10: Opened after exact safety and encoding ratification.
- 2026-07-10: Activated after A10d closure. RP2 must extend the existing validation-program/evaluator/package spine with total residual verdicts; it may not add a format-specific residual path, duplicate the contract lattice, or weaken quarantine for control-critical fields.
- 2026-07-10: Implemented the residual runtime and package spine against one canonical Arrow schema authority. Format and REST readers now localize neutral residual candidates in `BatchHeader`; the validation program classifies safe capture versus control-critical quarantine; engine execution redacts quarantine evidence, preserves exact reconciliation/coercion evidence, emits the final nullable `_cdf_variant`, and binds emitted batches to the compiled output schema before packaging.
- 2026-07-10: Removed the dormant duplicate residual model from `cdf-contract`; the kernel definitions are now the only residual envelope/decision IR. Framework variant-column recognition is exact and centralized (`_cdf_variant`, nullable UTF-8, `cdf:semantic=json`, `cdf:encoding=residual-json-v1`), so lookalike user columns remain rejected.
- 2026-07-10: Added fail-closed schema authority. Current planner output carries `schema_authority` plus the compiled output schema; legacy plans deserialize with the authority absent but execution/preflight rejects them before source or destination mutation. Runtime schema artifacts and tamper tests cover baseline/effective/output bindings, including zero-row execution.
- 2026-07-10: Added total package evolution handling. Package verification/replay reject unsupported top-level, capture, and decision encoding versions; unsupported residual values become the named, redacted `cdf.residual_encode_unsupported` quarantine verdict instead of an internal error. Physical source types remain in coercion evidence even after runtime output is rebound to the compiled schema.
- 2026-07-10: Added mixed-partition coverage proving that clean, residual-capture, and quarantine outcomes can coexist under one verified effective schema without collapsing per-input decisions or leaking a PII sentinel into the package tree. Drift conformance now asserts the typed residual envelope and the `residual:event_type:control-critical` / `cdf.residual_control_critical` rule.
- 2026-07-10: Replaced hand-authored current-plan fixtures in conformance with real planner output. Regenerated the affected live-run goldens through the repository's `CDF_PRINT_LIVE_RUN_GOLDEN=1` workflow after removing only documented volatile scan/explain observations; both DuckDB and Parquet committed packages remained deterministic across 100 rebuilds/runs.
- 2026-07-10: Verification evidence: `cargo nextest run -p cdf-kernel -p cdf-contract -p cdf-formats -p cdf-declarative -p cdf-engine -p cdf-package -p cdf-dest-duckdb -p cdf-dest-parquet -p cdf-dest-postgres -p cdf-project --all-features --no-fail-fast` passed 479/479; focused `cdf-formats` and `cdf-declarative` suites passed 34/34 and 88/88; focused mixed residual, package-version, schema-tamper, drift, and golden tests passed.
- 2026-07-10: Final workspace gate passed: `cargo fmt --all`; `cargo fmt --all -- --check`; `cargo clippy --workspace --all-targets --all-features -- -D warnings`; and `cargo nextest run --workspace --all-features --no-fail-fast` with 883/883 passed, 0 skipped (4 slow). `git diff --check` passed, and no dormant `#[cfg(any())]` residual definitions remain.
- 2026-07-10: Ticket intentionally remains active for parent-owned adversarial review, durable evidence/graph reconciliation, and closure. No commit or staging was performed; the worktree also contains the coordinated A10 lane edits and they were preserved.
- 2026-07-10: Parent closure completed. Evidence: `.10x/evidence/2026-07-10-p2-a10e-rp2-runtime-outcomes.md`. Adversarial review: `.10x/reviews/2026-07-10-p2-a10e-rp2-runtime-outcomes-review.md` (pass). The schema-authority/output-schema decision is recorded in `.10x/decisions/compiled-output-schema-and-runtime-provenance.md`. Parent-observed workspace nextest passed 883/883 with zero skipped; format and diff checks passed. No RP2 blocker remains.

## Blockers

None. RP1, A10d, and B5 are complete.
