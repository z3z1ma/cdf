Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-p1-product-ws4c-error-suggestions.md
Verdict: pass

# P1 WS4C error suggestions review

## Target

Implementation and evidence for `.10x/tickets/done/2026-07-08-p1-product-ws4c-error-suggestions.md`, especially:

- `crates/cdf-cli/src/output.rs`
- `crates/cdf-cli/src/suggestions.rs`
- `crates/cdf-cli/src/args.rs`
- `crates/cdf-cli/src/context.rs`
- `crates/cdf-cli/src/destination_uri.rs`
- destination error wrappers in scan, run, backfill, replay, and resume
- `crates/cdf-cli/src/tests.rs`
- `.10x/evidence/2026-07-08-p1-product-ws4c-error-suggestions.md`

## Assumptions tested

- Suggestions are additive JSON metadata and do not remove or rename existing error fields.
- Parser suggestions follow the ratified WS2C command grammar rather than inventing new command forms.
- Low-confidence suggestions are omitted rather than emitting noisy guesswork.
- Project resource suggestions use compiled resource inventory and disappear when no inventory exists.
- Destination suggestions do not print resolved secrets, userinfo credentials, or configured secret-backed DSNs.
- Error-size changes do not create a clippy regression across the broad `CliError` result surface.
- Destination suggestion wiring does not change destination support behavior or turn unsupported drivers into supported ones.

## Findings

No blocking findings.

Parent integration review re-read the worker patch and record changes after completion. No behavior repair was required; the implementation remained scoped to additive suggestions, project resource inventory, destination error wrappers, focused tests, and WS4 records.

The implementation keeps the behavior additive: `ErrorBody` gains `suggestions` with `skip_serializing_if = "Vec::is_empty"`, while `kind`, `message`, `exit_code`, `not_supported`, `code`, and `remediation` remain intact. Human output only gains a simple plain-text suggestions block when suggestions are present; renderer-specific presentation remains outside this ticket.

The suggestion algorithm is deterministic and bounded. It deduplicates candidates, ranks by edit distance, length delta, and candidate text, and caps output at three suggestions. Parser command suggestions then take only the single nearest command path, which avoids the initial noisy `staus -> status/state` failure.

Resource suggestions are attached at `ProjectContext::resource`, so existing command modules receive consistent behavior without changing command grammar. The no-inventory test proves the helper does not invent suggestions when compiled resources are absent.

Destination suggestions are intentionally conservative: they expose environment selectors and generic URI shapes rather than raw configured destination values. The replay redaction test covers a userinfo-bearing typo URI and proves `destination-secret` is absent from output while the redacted URI shape remains legible.

The clippy regression from increasing `CliError` size was resolved by storing suggestions as `Box<[String]>` in `CliError`, preserving the existing result-returning API without broad lint suppression.

Quality evidence is sufficient for closure: focused tests and full `cdf-cli` tests pass; fmt, feature-mode check, feature-mode clippy, Semgrep, jscpd `newClones: 0`, rust-code-analysis, scoped Gitleaks, cargo deny/audit/vet, direct unsafe scan, diff whitespace, and CodeQL SARIF result count all passed.

Parent reruns confirmed the same evidence after integration: full `cdf-cli` tests passed, feature-mode checks and clippy variants passed, Semgrep had 0 findings, jscpd reported `newClones: 0`, rust-code-analysis and SCC reports were generated, scoped Gitleaks found no leaks after the parent evidence append, supply-chain gates passed, the reusable CodeQL Rust database was reused from `target/quality/codeql-db-rust`, and the SARIF result count was 0.

## Residual risk

The current project configuration has one destination URI per environment and no independent named-destination registry, so WS4C cannot suggest named destinations beyond `--env <name>` selectors and URI shapes. That matches current source authority; a later named-destination model would need its own ticket/spec update.

CodeQL Rust extraction remains noisy and jscpd reports existing repository duplication, consistent with prior WS4 evidence. Neither appears introduced by this change.

WS4D still owns final renderer-integrated human presentation and generated docs.

## Verdict

Pass. WS4C acceptance criteria are supported by focused tests, full package tests, source inspection, and recorded quality/security evidence.
