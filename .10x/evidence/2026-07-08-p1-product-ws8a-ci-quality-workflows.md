Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p1-product-ws8a-ci-quality-workflows.md, .10x/specs/versioning-lts-release-policy.md, .10x/knowledge/quality-gate-execution.md

# WS8A CI quality workflow evidence

## What was observed

WS8A added `.github/workflows/fast-quality.yml` and `.github/workflows/slow-quality.yml`.

The fast workflow runs on pull requests and pushes. It checks Cargo metadata and tree shape with `--locked`, formatting, compile, Clippy, focused package tests, source-only Gitleaks, jscpd duplication, and fast supply-chain gates through `cargo deny --locked check advisories licenses sources` and `cargo audit --deny warnings --ignore RUSTSEC-2024-0436`.

The slow workflow runs on weekly schedule and manual dispatch. It checks dependency metadata, formatting, default/all-features/no-default-features compile, default/all-features Clippy, full nextest, doctests, focused conformance/golden/property/runtime-chaos/live-run test filters, smoke Criterion benchmark, coverage summary, jscpd duplication, Rust complexity metrics through `rust-code-analysis-cli`, cargo-machete dependency hygiene, Semgrep, source-only Gitleaks, cargo-deny, cargo-audit, cargo-vet, OSV, cargo-semver-checks against the default branch, and CodeQL.

The slow workflow restores `target/quality/codeql-db-rust` from `actions/cache` using OS, Rust toolchain fingerprint, and Rust source/manifest/lockfile hashes before running `tools/codeql-rust-quality.sh`. The helper retains its own CodeQL CLI version and input-content fingerprint check, so a restored database is analyzed when fresh and recreated only when stale. Local validation did not run the helper because doing so could refresh the existing reusable database; preserving `target/quality/codeql-db-rust` was part of the user boundary.

Generated reports are written under `target/quality/reports`, and the CodeQL database remains under `target/quality/codeql-db-rust`; both are covered by the repository's ignored `target/` tree. Reports are uploaded as CI artifacts, not committed release artifacts.

The cargo-audit RUSTSEC ignore is the existing narrow `RUSTSEC-2024-0436` native Parquet/paste exception recorded by `.10x/evidence/2026-07-07-rustsec-paste-parquet-exception.md` and represented in `deny.toml`.

## Procedure

- `ruby -e 'require "yaml"; ARGV.each { |f| YAML.load_file(f); puts "ok #{f}" }' .github/workflows/*.yml`
- `if command -v actionlint >/dev/null 2>&1; then actionlint .github/workflows/*.yml; else echo 'actionlint not installed'; fi`
- `git diff --check`
- `rg -n "[Kk]iller[ _-]?[Dd]emo" . --hidden`
- `rg -n "[Kk]iller[ _-]?[Dd]emo" . --hidden --glob '!target/**' --glob '!**/target/**'`
- `bash -n tools/codeql-rust-quality.sh`
- `cargo deny --locked check advisories licenses sources`
- `cargo audit --deny warnings --ignore RUSTSEC-2024-0436`
- `cargo vet --locked --no-minimize-exemptions`
- `npx --yes jscpd@5 .github/workflows --reporters console --output target/quality/reports/jscpd-ws8a-local --ignore "**/target/**,**/.git/**,**/reports/**" --min-lines 12 --min-tokens 80 --threshold 10 --no-colors`
- `gitleaks detect --no-git --source .github/workflows --no-banner --redact --report-format json --report-path target/quality/reports/gitleaks-ws8a-workflows.json`
- `command -v codeql && codeql --version | sed -n '1,8p'`
- `command -v gitleaks && gitleaks version`

## Results

- YAML parsing passed for both workflow files.
- `actionlint` was not installed locally, so actionlint did not run.
- `git diff --check` passed.
- The exact forbidden-phrase scan returned exit 2 because nested `crates/cdf-cli/target/cdf-cli-tests/...` Postgres files disappeared while ripgrep was walking the tree. No match was printed.
- The target-excluding forbidden-phrase substitute returned exit 1 with no matches.
- `bash -n tools/codeql-rust-quality.sh` passed.
- `cargo deny --locked check advisories licenses sources` passed with `advisories ok, licenses ok, sources ok`.
- `cargo audit --deny warnings --ignore RUSTSEC-2024-0436` passed.
- `cargo vet --locked --no-minimize-exemptions` passed with `Vetting Succeeded (452 exempted)`.
- Local jscpd smoke over `.github/workflows` completed with one expected setup-boilerplate clone, 5.94% duplicated lines, below the workflow threshold of 10%.
- Targeted Gitleaks over `.github/workflows` passed with no leaks.
- Local CodeQL CLI is available as 2.25.6.
- Local Gitleaks is available as 8.21.2.

## Parent verification

Parent review added these checks after the worker handoff:

- `go install github.com/rhysd/actionlint/cmd/actionlint@latest && "$HOME/go/bin/actionlint" .github/workflows/fast-quality.yml .github/workflows/slow-quality.yml`: passed.
- `ruby -e 'require "yaml"; ARGV.each { |f| YAML.load_file(f); puts "ok #{f}" }' .github/workflows/*.yml`: passed.
- `bash -n tools/codeql-rust-quality.sh`: passed.
- Scoped `git diff --check` over `.github` and WS8A records: passed.
- `rg -n "[Kk]iller[ _-]?[Dd]emo" . --hidden -g '!target/**' -g '!**/target/**' -g '!.git/**'`: no matches.
- `osv-scanner scan source --help` showed the workflow's `scan source --lockfile Cargo.lock --format json` shape is supported by the installed OSV scanner.
- `cargo semver-checks --help` showed `--workspace` and `--baseline-rev` are supported by the installed semver checker.
- `cargo nextest run --help | grep -F -- --locked`: confirmed `--locked` is supported.

The parent did not run local CodeQL analysis in order to preserve the reusable `target/quality/codeql-db-rust` database unless the helper decides it is stale.

## What this supports

This supports closing WS8A: the workflow files implement the required fast and slow quality phases, wire the required supply-chain gates into the appropriate phases, include jscpd and Rust complexity checks, preserve generated outputs under ignored build/report paths, and use the existing reusable CodeQL database policy rather than replacing it with a one-shot database workflow.

## Limits

The GitHub Actions jobs were not executed remotely in this local workstream. The slow workflow's full test, benchmark, Semgrep, OSV, cargo-semver-checks, coverage, and CodeQL steps are CI-only evidence until the workflow runs. Local CodeQL analysis was intentionally not run to avoid refreshing `target/quality/codeql-db-rust` during validation. The exact requested forbidden-phrase command was attempted but hit transient nested target files; the source/record-equivalent target-excluding rerun found no matches.
