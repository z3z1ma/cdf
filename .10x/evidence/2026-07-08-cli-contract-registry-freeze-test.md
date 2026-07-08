Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-07-cli-contract-registry-freeze-test.md, .10x/decisions/contract-freeze-lockfile-registry.md, .10x/specs/project-cli-observability-security.md, .10x/specs/types-contracts-normalization.md

# CLI contract registry freeze/test evidence

## What Was Observed

`cdf contract freeze` and `cdf contract test` are implemented over `cdf.lock` as the project-local contract snapshot registry. Contract snapshots now include descriptor contract reference, schema hash, trust-derived policy hash, and validation-program hash. `contract show` remains project-free.

The parent review found and fixed one integration issue before closure: ordinary `generate_lockfile` now computes full contract snapshots by default, so `cdf diff schema` does not report false drift immediately after `contract freeze`.

## Procedure

Implementation files inspected and changed:

- `crates/cdf-cli/src/contract_command.rs`
- `crates/cdf-cli/src/commands.rs`
- `crates/cdf-cli/src/tests.rs`
- `crates/cdf-project/src/lockfile.rs`
- `crates/cdf-project/src/internal.rs`
- `crates/cdf-project/src/tests.rs`
- `.10x/decisions/contract-freeze-lockfile-registry.md`

Verification commands run from `/Users/alexanderbut/code_projects/personal/firn`:

```text
cargo fmt --all
cargo check -p cdf-cli -p cdf-project --all-targets --locked
cargo clippy -p cdf-cli -p cdf-project --all-targets --locked -- -D warnings
cargo test -p cdf-cli contract_ --locked
cargo test -p cdf-project contract_ --locked
cargo test -p cdf-project lockfile_generation_round_trips_and_diffs_semantic_changes --locked
cargo fmt --all -- --check
git diff --check
cargo nextest run -p cdf-cli -p cdf-project --locked contract_
cargo nextest run -p cdf-cli -p cdf-project --locked
jscpd crates/cdf-cli/src crates/cdf-project/src --min-lines 8 --min-tokens 50 --reporters json,console --output target/quality/reports/jscpd-cli-contract --ignore "**/target/**,**/.git/**,**/reports/**"
jscpd crates/cdf-cli/src/contract_command.rs crates/cdf-project/src/lockfile.rs --min-lines 8 --min-tokens 50 --reporters json,console --output target/quality/reports/jscpd-contract-source --ignore "**/target/**,**/.git/**,**/reports/**"
rust-code-analysis-cli -m -O json -o target/quality/reports/rust-code-analysis-cli-contract -p crates/cdf-cli/src/contract_command.rs -p crates/cdf-project/src/lockfile.rs
cargo deny check
cargo audit
cargo vet
osv-scanner scan source -r .
gitleaks git --redact --report-format json --report-path target/quality/reports/gitleaks-git-contract.json .
semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-rust-contract.json .
gitleaks detect --no-git --source crates/cdf-cli/src --redact --report-format json --report-path target/quality/reports/gitleaks-source-cdf-cli-contract.json
gitleaks detect --no-git --source crates/cdf-project/src --redact --report-format json --report-path target/quality/reports/gitleaks-source-cdf-project-contract.json
rg -n "unsafe|extern \"|raw pointer|\*const|\*mut|Send|Sync" crates/cdf-cli/src/contract_command.rs crates/cdf-project/src/lockfile.rs crates/cdf-project/src/internal.rs crates/cdf-cli/src/tests.rs crates/cdf-project/src/tests.rs
tools/codeql-rust-quality.sh
cargo machete
cargo tree -p cdf-cli --locked -i paste
cargo tree -p cdf-project --locked -i paste
```

## Results

- `cargo check` passed for `cdf-cli` and `cdf-project`.
- `cargo clippy` passed for `cdf-cli` and `cdf-project` with `-D warnings`.
- Focused `cargo test` passed: 5 CLI `contract_` tests and 2 project `contract_` tests.
- The generic lockfile round-trip/diff test passed after the full-snapshot generation fix.
- Focused `cargo nextest run -p cdf-cli -p cdf-project --locked contract_` passed 7 tests.
- Full `cargo nextest run -p cdf-cli -p cdf-project --locked` passed 203 tests.
- `cargo fmt --all -- --check` and `git diff --check` passed.
- `jscpd` over production files `contract_command.rs` and `lockfile.rs` found 0 clones, 0 duplicated lines, and 0 duplicated tokens. The broader `cdf-cli/src` plus `cdf-project/src` scan reported 109 existing clones and 5.71% duplicated lines, dominated by pre-existing test/runtime duplication rather than the new production helper path.
- `rust-code-analysis-cli` over `contract_command.rs` and `lockfile.rs` reported max cyclomatic complexity 15 and max cognitive complexity 5. The max cyclomatic function is pre-existing `validate_project`; new slice functions stayed at or below the existing local complexity profile.
- `cargo deny check` passed. It still prints the known duplicate Arrow 58/59 warnings from the ratified DuckDB residual, but advisories, bans, licenses, and sources are `ok`.
- `cargo audit` passed with one allowed warning: `RUSTSEC-2024-0436` for `paste 1.0.15`.
- `cargo vet` passed with 452 exemptions and the existing prune warning.
- `osv-scanner scan source -r .` exited 1 only for `RUSTSEC-2024-0436` on `paste 1.0.15`; the advisory remains governed by `.10x/decisions/native-arrow-datafusion-parquet-policy.md` and `.10x/decisions/datafusion-git-pin-arrow59-tuple.md`.
- Full-history `gitleaks git` reported the two pre-existing historical Python findings already owned by `.10x/tickets/2026-07-08-historical-gitleaks-findings-triage.md`.
- Source-only Gitleaks scans over `crates/cdf-cli/src` and `crates/cdf-project/src` passed with no leaks.
- Semgrep Rust scan completed with 0 findings.
- Direct unsafe/FFI/raw-pointer scan over touched files found no matches.
- `tools/codeql-rust-quality.sh` refreshed the reusable DB at `target/quality/codeql-db-rust` because Rust inputs changed, then analyzed it with 0 SARIF results. The extractor reported 0 extraction errors and the known local macro-warning profile recorded in `.10x/knowledge/quality-gate-execution.md`.
- `cargo machete` found no unused dependencies.
- `cargo tree -p cdf-cli --locked -i paste` and `cargo tree -p cdf-project --locked -i paste` show `paste` only through `parquet v59.1.0` via existing CDF Parquet/package paths, not through this contract slice.

## What This Supports

This supports closing `.10x/tickets/done/2026-07-07-cli-contract-registry-freeze-test.md`:

- `cdf contract freeze` writes deterministic lockfile snapshots for selected resource or project scope.
- `cdf contract test` recomputes snapshots, fails closed for missing lock/snapshot state, returns exit code 1 on drift, and reports counts plus field-level drift details.
- `contract show` remains compatible and project-free.
- JSON output includes registry, resource ids, counts, snapshots/comparisons, and drift details without resolving or printing secrets.
- Lockfile generation and `diff schema` remain coherent with the new full contract snapshot shape.

## Limits

This evidence does not prove row-level quarantine routing, fixture execution, dedup, trust-ledger promotion/demotion, or external registry behavior. Those are excluded from this CLI slice and remain governed by the contract-depth records and runtime/conformance work.

The full-history Gitleaks findings are not resolved here because they are historical, outside the current source tree, and already owned by `.10x/tickets/2026-07-08-historical-gitleaks-findings-triage.md`.
