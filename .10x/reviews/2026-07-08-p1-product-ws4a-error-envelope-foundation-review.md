Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-p1-product-ws4a-error-envelope-foundation.md
Verdict: pass

# P1 WS4A error envelope foundation review

## Target

Implementation and evidence for `.10x/tickets/done/2026-07-08-p1-product-ws4a-error-envelope-foundation.md`, especially:

- `crates/cdf-cli/src/error_catalog.rs`
- `crates/cdf-cli/src/output.rs`
- `crates/cdf-cli/src/tests.rs`
- `.10x/evidence/2026-07-08-p1-product-ws4a-error-envelope-foundation.md`

## Findings

No blocking findings.

The implementation changes the centralized envelope path rather than sweeping construction sites, matching WS4A scope and preserving WS4B's work. Existing JSON fields are retained and the new `code` and `remediation` fields are additive. Success envelopes are not touched.

The generic lower-layer mapping preserves the existing exit-code taxonomy for lower-crate errors whose domain semantics are not owned by a CLI call site. The explicit match over every `ErrorKind` is acceptable despite cyclomatic 8 because it is the catalog table, not control-flow complexity in runtime behavior.

Plain human errors still start with the existing `error: <message>` text and append remediation only when present, so information does not regress before WS4D renderer integration.

Parent review reran full `cdf-cli` tests, clippy, fmt, Semgrep, Gitleaks, jscpd, scc, rust-code-analysis, CodeQL through the reusable database, cargo deny/audit/vet, unsafe scanning, forbidden phrase scanning, and scoped whitespace checks. Results are appended to `.10x/evidence/2026-07-08-p1-product-ws4a-error-envelope-foundation.md`.

## Residual risk

WS4B later migrated individual construction sites from broad generic codes to specific stable product codes. WS4C and WS4D still own suggestions, generated docs, and renderer-integrated error presentation. CodeQL's Rust extraction diagnostics remain noisy, so its zero findings are useful but not a complete security proof.

## Verdict

Pass. Acceptance criteria are supported by focused tests and quality evidence, with remaining work explicitly owned by later WS4 child tickets.
