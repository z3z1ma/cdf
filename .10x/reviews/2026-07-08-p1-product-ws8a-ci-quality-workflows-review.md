Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-p1-product-ws8a-ci-quality-workflows.md
Verdict: pass

# WS8A CI quality workflow review

## Target

Review `.github/workflows/fast-quality.yml`, `.github/workflows/slow-quality.yml`, and the WS8A closure evidence.

## Findings

No blocking findings.

Parent review additionally installed and ran `actionlint` against both workflow files; it passed. Parent spot-checks also confirmed the OSV, semver-checks, and nextest flags used in the workflow are supported by the locally installed tools.

## Assumptions tested

- Fast trigger coverage: the fast workflow uses `pull_request` and `push`, satisfying the active release-policy CI phase contract.
- Fast gate contents: formatting, Clippy, compile, focused tests, cargo metadata/tree, source-only Gitleaks, jscpd, cargo-deny, and cargo-audit are present.
- Slow trigger coverage: the slow workflow uses weekly schedule plus manual dispatch, matching the requested slow phase.
- Slow gate contents: full tests/doctests, conformance/golden/property/chaos/live-run filters, smoke benchmark, coverage, Semgrep, CodeQL, jscpd, Rust complexity, cargo-deny, cargo-audit, cargo-vet, OSV, cargo-machete, and semver checks are present.
- Locked resolution: Cargo workspace commands use `--locked` where the tool supports it; `cargo audit` and OSV read the lockfile rather than resolving dependencies; `cargo install` commands use `--locked`.
- CodeQL reuse: the workflow restores `target/quality/codeql-db-rust` with an input-sensitive cache key before invoking `tools/codeql-rust-quality.sh`, whose own fingerprint still decides whether to reuse or refresh the database.
- Generated output hygiene: workflow reports and CodeQL database paths live under `target/quality`, and no generated report or database path was added to tracked source.
- Scope control: no docs, cdf-cli grammar/source files, WASM tickets, release artifact publishing, installer, changelog, parser semantics, or WASM work were changed by this ticket.

## Residual risk

Actual CI execution remains future evidence. The review cannot prove that hosted runner tool installation times, external scanner availability, GitHub CodeQL action behavior, or semver baseline shape will succeed until the workflows run on GitHub. This is acceptable for WS8A because local static validation and command-shape checks passed or were explicitly limited, and the workflow work itself is the artifact required by the ticket.

## Verdict

Pass. The workflows satisfy the WS8A acceptance criteria and preserve the existing CodeQL reuse and supply-chain policy boundaries.
