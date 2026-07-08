Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-contract-depth-program.md
Depends-On: .10x/tickets/done/2026-07-08-p1-e2-quarantine-routing-redaction.md

# P1 E4: Variant capture and contract-evolution evidence

## Scope

Implement end-to-end `_cdf_variant` capture for unknown or violating nested substructure and record promotion as contract-evolution evidence.

Owns:

- `crates/cdf-contract/**` variant capture program details;
- `crates/cdf-engine/src/execution.rs` normalization/variant column materialization;
- package evidence for variant capture and promotion;
- conformance tests for nested/unknown structure handling.

## Governing records

- `VISION.md` Chapter 11.
- `VISION.md` Section 7.5.
- `.10x/specs/types-contracts-normalization.md`.
- `.10x/specs/package-lifecycle-determinism.md`.
- `.10x/specs/destination-receipts-guarantees.md`.
- `.10x/specs/run-orchestration-ledger.md`.
- `.10x/decisions/contract-live-verdict-execution-semantics.md`.
- `.10x/knowledge/runtime-conformance-throughput-rule.md`.

## Acceptance criteria

- `NestedAction::CaptureVariant` materializes `_cdf_variant` with semantic tag `json` for configured unknown or violating substructure.
- Variant capture preserves source evidence without silently dropping untyped data.
- Promotion from `_cdf_variant` to typed columns is recorded as a contract-evolution event and never happens implicitly.
- Variant artifacts and package schema evidence are deterministic and replayable from package contents.
- Quarantine redaction rules still apply to PII values inside variant evidence where rule failures expose observed values.

## Evidence expectations

Record nested/variant tests, package schema/evidence inspection, replay proof, redaction interaction checks, jscpd and `rust-code-analysis-cli` metrics, and adversarial review.

## Explicit exclusions

No arbitrary JSON schema inference engine. No destination-specific variant type mapping beyond existing sheet-supported Arrow/type behavior.

## Blockers

None; E2 is closed at `.10x/tickets/done/2026-07-08-p1-e2-quarantine-routing-redaction.md`.
