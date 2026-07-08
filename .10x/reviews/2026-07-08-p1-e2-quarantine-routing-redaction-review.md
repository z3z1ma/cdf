Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-p1-e2-quarantine-routing-redaction.md
Verdict: pass

# P1 E2 quarantine routing and redaction review

## Target

Review of the P1 E2 implementation that routes live row-level quarantine candidates into package Parquet artifacts and supported destination mirrors.

## Findings

- No blocking findings remain.
- Resolved significant issue: parent review found that package quarantine reads initially trusted manifest relative paths too directly. The implementation was hardened so readback normalizes artifact paths, requires `quarantine/`, requires `.parquet`, and rejects traversal such as `quarantine/../escape.parquet`; the package test now proves that rejection.
- Security/privacy check passed: the focused engine test validates that a `pii:email` value is represented as a deterministic SHA-256 redaction and the raw fixture value is absent from the quarantine Parquet bytes. Semgrep, gitleaks, CodeQL, and direct unsafe/FFI scans found no new actionable issues.
- Architecture check passed: quarantine remains a framework side channel and does not introduce a DataFusion multi-output plan. Destination mirroring is sheet-gated, with unsupported destinations recording an explicit non-mirror artifact.
- Scope check passed: the slice did not add dependencies, widen to dedup/variant/trust behavior, or add new destination categories.

## Residual risk

The live path evidence covers mixed accepted/quarantined rows. All-row-quarantined runs, richer quarantine row payloads, and downstream contract-depth behaviors remain outside E2 and are not accepted as done by this review. CodeQL extraction still reports macro-related extractor warnings consistent with prior Rust runs, but the SARIF query result count is 0.

## Verdict

Pass. The E2 ticket can close with the recorded evidence; remaining contract-depth work belongs to later P1 children.
