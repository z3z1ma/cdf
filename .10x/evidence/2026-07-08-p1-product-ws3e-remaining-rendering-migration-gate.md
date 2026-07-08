Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p1-product-ws3e-remaining-rendering-migration-gate.md, .10x/decisions/cli-design-language-and-renderer.md

# P1 WS3E remaining rendering migration gate evidence

## What was observed

WS3E migrated the remaining CLI human output paths to renderer-backed `RenderDocument` output and added a static migration gate. The legacy raw human-output variant was removed from `HumanOutput`; command families now construct `CommandOutput` through renderer helpers. Help/version keep a documented compatibility shim in `commands.rs` that wraps generated text in a renderer `TextBlock`.

Human rendering coverage was added or updated for status, SQL tables, preview, inspect project inventory, package list/archive, and the migration gate. JSON compatibility was preserved, including the `package ls` result remaining a JSON array rather than being wrapped in a new object.

## Procedure

Commands run from `/Users/alexanderbut/code_projects/personal/firn`:

```text
cargo fmt --all
cargo fmt --all -- --check
cargo check -p cdf-cli --all-targets --locked
cargo test -p cdf-cli renderer_migration_gate_rejects_raw_human_output_bypasses --locked
cargo test -p cdf-cli status_ --locked
cargo test -p cdf-cli sql_human_output_is_concise_for_scheduler_logs --locked
cargo test -p cdf-cli package_archive_supports_local_json_flag_and_human_output --locked
cargo test -p cdf-cli preview_reads_single_ndjson_file_without_creating_runtime_artifacts --locked
cargo test -p cdf-cli inspect_human_outputs_use_renderer_for_project_inventory --locked
cargo test -p cdf-cli package_ls_json_remains_array_while_human_uses_renderer --locked
cargo test -p cdf-cli --locked
cargo clippy -p cdf-cli --all-targets --locked -- -D warnings
semgrep scan --config p/rust --error --json --output target/quality/reports/ws3e-semgrep.json <14 touched Rust files>
gitleaks dir --no-banner --redact --report-format json --report-path target/quality/reports/ws3e-gitleaks.json target/quality/ws3e-gitleaks-scope
jscpd --format rust --min-lines 8 --min-tokens 80 --reporters console,json --output target/quality/reports/ws3e-jscpd --exit-code 0 <14 touched Rust files>
scc --by-file --format json --output target/quality/reports/ws3e-scc.json <14 touched Rust files>
rust-code-analysis-cli -m -O json --paths <file> -o target/quality/reports/ws3e-rust-code-analysis/<file-dir>  # repeated for 14 touched Rust files
tools/codeql-rust-quality.sh
git diff --check -- <WS3E touched files>
rg -n "\bunsafe\b|unsafe\s+impl|unsafe\s+trait|extern\s+\"C\"|from_raw|into_raw|transmute|MaybeUninit|\bUnpin\b|\bSend\b|\bSync\b" <14 touched Rust files>
rg -n -i <forbidden demo phrase> .10x docs crates VISION.md QUALITY.md README.md CHANGELOG.md tools
cargo deny check > target/quality/reports/ws3e-cargo-deny.txt 2>&1
cargo vet > target/quality/reports/ws3e-cargo-vet.txt 2>&1
```

## Results

- `cargo fmt --all -- --check`: passed.
- `cargo check -p cdf-cli --all-targets --locked`: passed.
- Focused tests listed above: passed.
- `cargo test -p cdf-cli --locked`: passed; 172 lib tests, 1 integration test, and 0 doc tests.
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`: passed.
- Static renderer migration gate: passed.
- Semgrep Rust scan: 0 findings, report at `target/quality/reports/ws3e-semgrep.json`.
- Gitleaks scoped scan: 0 leaks, report at `target/quality/reports/ws3e-gitleaks.json`.
- jscpd scoped scan: 24 clones, 337 duplicated lines, 2.68% duplicated lines, report at `target/quality/reports/ws3e-jscpd/jscpd-report.json`.
- scc report written to `target/quality/reports/ws3e-scc.json`.
- rust-code-analysis reports written for all 14 touched Rust files under `target/quality/reports/ws3e-rust-code-analysis/`.
- CodeQL Rust: report at `target/quality/reports/codeql-rust-current.sarif`; SARIF result count 0. The database was refreshed at the required reusable path `target/quality/codeql-db-rust` because the Rust source fingerprint changed. Metrics: extraction errors 0, extraction warnings 3747, files extracted total 243, files extracted with errors 191, files extracted without errors 52, macro calls total 5874, macro calls unresolved 5751.
- `git diff --check` over WS3E files: passed.
- Direct unsafe scan over WS3E Rust files: no matches.
- Forbidden phrase scan: no matches.
- `cargo deny check`: exit 0; advisories, bans, licenses, and sources ok. It emitted existing duplicate Arrow-family warnings that are covered by the active dependency-tuple records.
- `cargo vet`: exit 0; vetting succeeded with 452 exempted and existing exemption-pruning warnings.

## Parent review reruns

Parent review reran these checks after worker handoff:

- Focused tests for the renderer migration gate, `package ls` JSON compatibility, SQL human table output, and inspect inventory rendering: passed.
- `cargo fmt --all -- --check`: passed.
- `cargo test -p cdf-cli --locked`: passed, 172 library tests, 1 integration test, and 0 doc tests.
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`: passed.
- Direct unsafe/FFI/raw-pointer marker scan over the 14 touched Rust files: no matches.
- Forbidden legacy-demo-phrase scan across `.10x`, docs, crates, root docs, changelog, tools, `.github`, and `LICENSE`: no matches.
- Semgrep Rust scan over the 14 touched Rust files: 0 findings.
- Scoped Gitleaks over `target/quality/ws3e-parent-gitleaks-scope`: no leaks.
- `jscpd`: 14 Rust files, 24 clones, 337 duplicated lines, 2.68% duplicated lines.
- `scc`: 14 Rust files, 12,563 lines, 6,043 code lines, complexity 238.
- `rust-code-analysis-cli`: reports written under `target/quality/reports/ws3e-parent-rust-code-analysis/`.
- `tools/codeql-rust-quality.sh`: reused the fresh database at `target/quality/codeql-db-rust`; SARIF result count was `0`.
- `cargo deny --locked check advisories licenses sources`, `cargo audit --deny warnings --ignore RUSTSEC-2024-0436`, and `cargo vet --locked --no-minimize-exemptions`: passed.
- Scoped `git diff --check`: passed.

## What this supports or challenges

Supports closing WS3E acceptance criteria:

- Remaining command families now route human output through the renderer instead of raw human strings.
- SQL human output uses table rendering for result rows.
- Doctor/status/package/contract outputs use status lines, panels, tables, and next-command affordances.
- The static migration gate fails on future raw output bypass patterns outside the allowed core shim files.
- JSON output remains stable for checked compatibility surfaces, including `package ls` array shape.
- Redaction remains routed through renderer helpers for URI/userinfo-bearing display fields and existing redaction tests passed.

## Limits

jscpd was run as an evidence gate with `--exit-code 0`; it found existing duplicated test patterns in `crates/cdf-cli/src/tests.rs`. No new abstraction was introduced solely to silence this because the clones are in established scenario-style tests and refactoring them would widen WS3E beyond renderer migration.

CodeQL produced extractor warnings and many files marked "with errors" while still reporting 0 extraction errors and 0 SARIF findings. The same project helper was used, and it refreshed only the mandated reusable database path.
