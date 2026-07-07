Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-07-semantic-commit-gate-terminology-cleanup.md, .10x/specs/checkpoint-state-commit-gate.md, .10x/knowledge/cdf-glossary.md

# Semantic commit-gate terminology cleanup evidence

## What was observed

`VISION.md` is the ratifying source for the former line metaphor: it defines the state-advancement boundary as the "commit gate" in the preface, transition calculus, Chapter 13, MVP demo, review checklist, and glossary. `guarantee line` appears only for future `cdf plan` output.

The repository now uses `commit gate` / `commit-gate` for checkpoint, state advancement, receipt verification, crash recovery, invariant, and path-slug contexts.

## Procedure

Terminology and path checks:

- Subagent read-only semantic review confirmed the mapping: checkpoint/state advancement contexts use `commit gate`; `guarantee line` remains plan-output vocabulary.
- Renamed the checkpoint/state spec from its former line-metaphor slug to `.10x/specs/checkpoint-state-commit-gate.md`.
- Renamed the prepared-package replay runtime ticket, evidence, and review slugs from their former line-metaphor form to `package-replay-commit-gate-runtime`.
- Rewrote source, tests, specs, tickets, evidence, reviews, research, and knowledge references to the ratified vocabulary.
- A filename scan for the legacy hyphenated line-metaphor slug under `.10x/` returned no matches.
- A text scan for the legacy line-metaphor phrase family, legacy spec slug, legacy prepared-package runtime slug, and snake-case variant across `.10x`, source, Python, root Cargo metadata, `VISION.md`, and `QUALITY.md`, excluding generated/report directories, returned no matches.

Rust and quality checks:

- `cargo test -p cdf-cli run_human_output_mentions_receipt_verified_commit_gate --locked`: passed, 1 selected test.
- `cargo fmt --all -- --check`: passed.
- `cargo check -p cdf-cli --all-targets --locked`: passed.
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`: passed.
- `cargo test -p cdf-cli --locked --no-fail-fast`: passed, 68 tests.
- `cargo metadata --locked --format-version 1`: passed.
- `git diff --check`: passed.
- `cargo audit`: passed, 429 crate dependencies.
- `cargo deny check`: passed; duplicate-dependency warnings were emitted, and advisories, bans, licenses, and sources were OK.
- `cargo vet --locked --output-format json --output-file target/quality/reports/cargo-vet-semantic-commit-gate.json`: passed.
- `osv-scanner scan source -r . --format json --output target/quality/reports/osv-semantic-commit-gate.json`: passed, 0 vulnerabilities.
- `semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-rust-semantic-commit-gate.json crates/cdf-cli`: passed, 0 findings.
- `semgrep scan --config p/security-audit --error --json --output target/quality/reports/semgrep-security-semantic-commit-gate.json crates/cdf-cli`: passed, 0 findings.
- Source-only `gitleaks detect --no-git --redact --source <temporary source snapshot> --report-format json --report-path target/quality/reports/gitleaks-semantic-commit-gate.json --no-banner`: passed, no leaks found.

## What this supports

This supports closing `.10x/tickets/done/2026-07-07-semantic-commit-gate-terminology-cleanup.md`: the old line-metaphor terminology no longer appears in tracked source/record search scope, CLI output/tests use the ratified commit-gate language, renamed `.10x/` paths have coherent references, and the changed Rust package passes relevant fast gates plus security/supply-chain checks.

## Limits

This evidence does not claim a behavior change beyond terminology. CodeQL was skipped per the active goal. Mutation testing was skipped because this ticket changed terminology, record references, and one CLI assertion string rather than state-transition logic.

Two initial source-only gitleaks snapshot attempts failed before scanning: one used zsh's special `path` variable name and broke command lookup; one included deleted tracked paths from renamed files. The final guarded source snapshot passed.
