Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-p1-product-ws2d-completions-manpages-help.md
Verdict: pass

# P1 product WS2D completions, man pages, and help snapshots review

## Target

Review of WS2D implementation and evidence in:

- `crates/cdf-cli/src/cli_artifacts.rs`
- `crates/cdf-cli/src/bin/generate_cli_artifacts.rs`
- `crates/cdf-cli/generated/`
- `crates/cdf-cli/Cargo.toml`
- `Cargo.lock`
- `.github/workflows/fast-quality.yml`
- `.github/workflows/release-artifacts.yml`
- `.github/workflows/slow-quality.yml`
- `tools/package-release-artifact.sh`
- `tools/test-release-artifacts.sh`
- `supply-chain/config.toml`
- `.10x/evidence/2026-07-08-p1-product-ws2d-completions-manpages-help.md`

## Findings

No blocking findings.

Minor residual: generated help and man artifacts are committed and intentionally repetitive. The implementation keeps generated output out of full-repo jscpd scans with `**/generated/**`; freshness is instead enforced by a clap-derived byte comparison. This is the right quality boundary for generated command artifacts.

Minor residual: the release helper still tolerates absent generated directories for manual packaging paths, but release CI now checks committed freshness and generates `target/generated` before packaging. The non-CI fallback records absence in `generated/ARTIFACTS.txt` rather than silently pretending artifacts exist.

Minor residual: `cargo vet` required new explicit exemptions for `clap_complete`, `clap_mangen`, and `roff`. They are version-pinned in the same exemption mechanism the repository already uses, and `cargo deny`, `cargo audit`, `cargo vet`, and `cargo machete` pass on the final tree.

Resolved during parent review: generated help/man output initially contained trailing whitespace inherited from clap/man generation. The generator now normalizes generated text, and the freshness, clippy, release smoke, and whitespace gates pass after regeneration.

## Assumptions tested

- The generated artifacts come from `args::cli_command()` and `args::render_help()`, not from hand-maintained command prose.
- The freshness check generates into an isolated target/quality temp directory and compares file bytes against the committed generated tree.
- Release packaging consumes generated completions/man pages from `target/generated`, preserving the existing archive layout.
- The CLI grammar and command semantics are unchanged; `args.rs` only exposes existing clap/help builders within the crate for generation.
- JSON envelopes and exit-code behavior remain guarded by the full `cdf-cli` test suite.
- Parent review reran generator freshness, feature-gated full `cdf-cli` tests, clippy, release smoke tests, Semgrep, Gitleaks including generated artifacts, jscpd, scc, rust-code-analysis, CodeQL through the reusable database, supply-chain gates, unsafe scan, forbidden phrase scan, and scoped whitespace checks. Results are appended to `.10x/evidence/2026-07-08-p1-product-ws2d-completions-manpages-help.md`.

## Verdict

Pass. WS2D satisfies its acceptance criteria and has closure evidence for generated completions, man pages, help snapshots, freshness checks, release plumbing, focused secret scanning, duplication/complexity metrics, supply-chain gates, Semgrep, and CodeQL.

## Residual risk

Future command changes must update the generated snapshot files and review the generated diff. CodeQL's Rust extractor still reports the repository's known macro-warning profile, so the 0-result SARIF is security-query evidence with extractor limits rather than a claim of perfect extraction.
