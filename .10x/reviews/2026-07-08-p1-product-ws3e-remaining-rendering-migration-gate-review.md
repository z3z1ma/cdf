Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-p1-product-ws3e-remaining-rendering-migration-gate.md
Verdict: pass

# P1 WS3E remaining rendering migration gate review

## Target

WS3E changes in `crates/cdf-cli/src/**` plus focused tests and WS3E records. The review checked the renderer migration, JSON compatibility, redaction posture, static gate, scope boundaries, and quality evidence.

## Findings

No blocking findings.

Minor residual risks:

- `commands.rs` keeps `output`/`report_output` as a compatibility shim for parser-generated help/version. This is explicitly documented in code and excluded from the static gate so generated text can still be wrapped in a `RenderDocument::text`.
- jscpd reports 24 clones and 2.68% duplicated lines in the scoped Rust set, concentrated in long-form CLI scenario tests. This is acceptable for WS3E because abstracting those tests would obscure behavior assertions and broaden scope.
- CodeQL reports many extractor warnings and files "with errors" while producing 0 SARIF findings and 0 extraction errors. This is a known limitation of the current Rust extractor on macro-heavy Rust; the mandated reusable DB path and project helper were used.

## Checks

- Searched for raw human output bypasses after migration. Remaining hits are only the allowed core output/commands files, tests, renderer internals, and process `.output()` in the Python doctor probe.
- Reviewed JSON compatibility on migrated commands. A `package ls` array-shape regression was found during review, fixed, and covered by `package_ls_json_remains_array_while_human_uses_renderer`.
- Reviewed scope boundaries. Changes stayed within `crates/cdf-cli/src/**` plus WS3E records; unrelated WASM and release-engineering workspace changes were not reverted or incorporated.
- Reviewed redaction-sensitive paths. URI/userinfo display values are redacted in renderer documents where applicable, and existing redaction tests passed.
- Parent review reran focused renderer tests, full `cdf-cli` tests, clippy, fmt, Semgrep, Gitleaks, jscpd, scc, rust-code-analysis, CodeQL via the reusable database, `cargo deny`, `cargo audit`, `cargo vet`, unsafe-token scan, forbidden phrase scan, and scoped whitespace checks; evidence is recorded in `.10x/evidence/2026-07-08-p1-product-ws3e-remaining-rendering-migration-gate.md`.

## Verdict

Pass. WS3E acceptance criteria are supported by recorded evidence, with residual risks documented and non-blocking.
