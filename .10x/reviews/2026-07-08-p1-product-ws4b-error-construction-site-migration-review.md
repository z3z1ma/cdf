Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-p1-product-ws4b-error-construction-site-migration.md
Verdict: pass

# P1 WS4B error construction-site migration review

## Target

Implementation and evidence for `.10x/tickets/done/2026-07-08-p1-product-ws4b-error-construction-site-migration.md`, especially:

- `crates/cdf-cli/src/error_catalog.rs`
- `crates/cdf-cli/src/output.rs`
- migrated command modules under `crates/cdf-cli/src/**`
- `crates/cdf-cli/src/tests.rs`
- `.10x/evidence/2026-07-08-p1-product-ws4b-error-construction-site-migration.md`

## Findings

No blocking findings.

Parent integration review found and repaired only stale references after the ticket move to `done/`, plus one stale code comment that still described WS4B as future work. The repair did not change runtime behavior.

The implementation uses the WS4A envelope foundation instead of changing command behavior. `CliError::mapped`, `usage_with`, and `not_supported_with` preserve `ErrorKind`, exit code, and `not_supported` semantics while giving command-family call sites stable product codes and remediation.

Parser-only `args.rs` construction sites intentionally remain on `CDF-CLI-USAGE`; this is documented in `output.rs` and evidence, and is an acceptable generic mapping because those sites do not encode product/domain semantics beyond CLI grammar. Lower-layer `?` conversions remain on the WS4A documented generic lower-layer mapping, which is also acceptable where the lower crate owns the semantic error.

Not-supported production paths retain exit 78 and still name the lower layer, including preview runtime open support, destination driver/planner support, run loop supervisor, and multi-run resume drain.

Tests now cover code/remediation JSON fields across project init, scan, run, replay, package, state, SQL, not-supported, generic lower-layer conversion, and compatibility paths. Existing and extended redaction checks cover destination URI userinfo, secret references, Python probe stdout/stderr, SQL text non-echo, and project/state path context.

Quality evidence is broad: full `cdf-cli` tests pass, fmt/check/clippy pass across feature modes, Semgrep has 0 findings, cargo deny/audit/vet pass with known existing warnings, CodeQL SARIF has 0 results, and scoped Gitleaks over touched source has no leaks.

Parent reruns confirmed the same acceptance evidence after the reference cleanup: full `cdf-cli` tests passed, all checked feature-mode build and clippy variants passed, jscpd reported `newClones: 0`, rust-code-analysis and SCC reports were generated, scoped Gitleaks found no leaks, the reusable CodeQL Rust database refreshed under `target/quality/codeql-db-rust`, and the current SARIF result count is 0.

## Residual risk

Whole-history Gitleaks still reports 2 pre-existing leaks outside this scoped source scan. This does not appear introduced by WS4B, but it remains a repository-level hygiene issue for an owner outside this ticket.

jscpd reports existing duplication involving touched test/support files, but `newClones: 0` indicates this change did not introduce new clones according to the report.

CodeQL Rust extraction remains noisy with many extractor warnings and unresolved macros, so its 0 SARIF findings should not be treated as exhaustive security proof.

## Verdict

Pass. WS4B acceptance criteria are supported by code inspection, tests, and recorded quality evidence. The remaining risks are pre-existing repository/tooling limitations, not blockers for this ticket.
