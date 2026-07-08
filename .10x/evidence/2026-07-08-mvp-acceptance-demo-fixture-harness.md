Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-mvp-acceptance-demo-fixture-harness.md, .10x/decisions/mvp-acceptance-demo-fixture-boundary.md, .10x/tickets/2026-07-05-conformance-chaos-golden.md

# MVP acceptance demo fixture harness evidence

## What was observed

`cdf-conformance` now owns a deterministic MVP acceptance demo fixture harness in `crates/cdf-conformance/src/mvp_acceptance_demo.rs`.

The test composes existing CLI/runtime surfaces into one proof:

- `cdf plan` for a GitHub-Issues-shaped Tier-0 REST resource before package bytes exist.
- `cdf contract freeze` and `cdf contract test` against the fixture project.
- Deterministic REST execution through explicit `RestRuntimeDependencies`, with request recording and secret redaction.
- A simulated crash after destination receipt verification and before checkpoint commit through the existing lower `run_project` crash hook.
- `cdf resume` for the interrupted run, proving recovery without new source contact.
- Accepted issue rows loaded into DuckDB and read back from the target table.
- `cdf sql` over local package/state history.
- `cdf state history` proving committed checkpoint state.
- `cdf replay package` into a second DuckDB database.
- Duplicate replay/no-op behavior against the destination mirror.
- A drift-quarantine beat through the existing DuckDB drift fixture, proving accepted and quarantined row routing with receipt-verified checkpoint gating.

The fixture preserves the ratified boundary from `.10x/decisions/mvp-acceptance-demo-fixture-boundary.md`: it is deterministic and GitHub-shaped, but it does not claim live GitHub credentials, rate limits, or production egress policy are complete.

## Procedure

Focused and broad correctness checks:

```text
cargo test -p cdf-conformance mvp_acceptance_demo --offline
cargo fmt --all --check
cargo test -p cdf-conformance mvp_acceptance_demo --locked
cargo test -p cdf-conformance --locked
cargo test -p cdf-cli --locked
cargo clippy -p cdf-conformance --all-targets --locked -- -D warnings
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

Quality, duplication, complexity, security, and supply-chain checks:

```text
rg -n "\bunsafe\b|extern \"C\"|raw pointer|\*const|\*mut|Send for|Sync for" crates/cdf-conformance crates/cdf-cli
jscpd . --reporters json,console --output reports/ai-quality/jscpd --ignore "**/target/**,**/.git/**,**/reports/**"
rust-code-analysis-cli -m -p crates -O json -o reports/ai-quality/rust-code-analysis-acceptance-demo-current
semgrep scan --config p/rust --json --output reports/ai-quality/semgrep-rust.json .
gitleaks dir --redact --report-format json --report-path reports/ai-quality/gitleaks-crates.json --no-banner --log-level error crates
gitleaks dir --redact --report-format json --report-path reports/ai-quality/gitleaks-10x.json --no-banner --log-level error .10x
cargo deny check
cargo audit
cargo vet --locked
osv-scanner scan --lockfile Cargo.lock --format json --output reports/ai-quality/osv.json
./tools/codeql-rust-quality.sh
```

The reusable CodeQL wrapper reused `target/quality/codeql-db-rust` and refreshed it only because Rust source/manifests/lock content changed.

## Results

- `cargo test -p cdf-conformance mvp_acceptance_demo --locked`: pass; 1 test.
- `cargo test -p cdf-conformance --locked`: pass; 65 tests plus doc tests.
- `cargo test -p cdf-cli --locked`: pass; 134 unit tests, binary target, `tests/doctor_env.rs`, and doc tests.
- `cargo clippy -p cdf-conformance --all-targets --locked -- -D warnings`: pass.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: pass.
- `cargo test --workspace --locked`: pass across all workspace unit and doc tests.
- Direct unsafe/FFI/raw-pointer scan over `crates/cdf-conformance` and `crates/cdf-cli`: no hits.
- `jscpd`: pass; total duplicated lines 6309 / 107961 = 5.843776919%, Rust duplicated lines 5078 / 83400 = 6.088729017%, `newClones: 0`, `newDuplicatedLines: 0`.
- `rust-code-analysis-cli`: pass; 235 JSON metrics files generated for `crates`.
- `semgrep scan --config p/rust`: pass; 0 findings.
- Source-only `gitleaks` over `crates` and `.10x`: pass; no findings.
- `cargo deny check`: pass with the already-ratified duplicate Arrow 58/59 warning profile.
- `cargo audit`: pass with only the already-ratified `paste 1.0.15` / `RUSTSEC-2024-0436` warning.
- `cargo vet --locked`: pass; `Vetting Succeeded (452 exempted)`.
- `osv-scanner`: exited 1 only for the already-ratified `paste` / `RUSTSEC-2024-0436` advisory.
- CodeQL SARIF `target/quality/reports/codeql-rust-current.sarif`: 0 results.

## Acceptance mapping

- GitHub-Issues-shaped Tier-0 REST and plan-before-bytes: `mvp_acceptance_demo_fixture_proves_rest_duckdb_recovery_replay_and_drift` invokes `cdf plan`, checks the resource/package IDs and state-advancement text, and proves the package directory does not exist after planning.
- DuckDB load and queryability: the run writes two issue rows to DuckDB, reads them back in issue-number order, and records the SQL-surface boundary.
- Contract freeze/test and drift quarantine: the fixture invokes `cdf contract freeze/test` for the GitHub-shaped project and then runs the existing DuckDB drift-quarantine proof, asserting one accepted row, one quarantined row, receipt verification, and checkpoint gating.
- Crash/resume without source contact: the lower runtime hook panics after receipt verification but before checkpoint commit; `cdf resume` then commits from recorded package/receipt facts while the deterministic transport request count remains one.
- Replay and duplicate no-op: `cdf replay package` writes the package into a second DuckDB database, then lower artifact replay proves duplicate destination replay leaves the mirror snapshot unchanged.
- State history and commit gate: `cdf state history` reports one committed checkpoint; the store head receipt package hash matches the checkpoint delta package hash, and the DuckDB mirror load/state rows match that package hash.
- Redaction: rendered evidence excludes the demo secret value and temp path and contains `Bearer <redacted>`.

## What this supports

This supports closing `.10x/tickets/done/2026-07-08-mvp-acceptance-demo-fixture-harness.md`.

It also advances `.10x/tickets/2026-07-05-conformance-chaos-golden.md` and `VISION.md` Chapter 23 coverage by converting the acceptance demo from an open concept into a deterministic conformance-owned foundation harness.

## Limits

The harness uses a deterministic local transport for the GitHub-shaped REST source. It does not prove live GitHub credentials, rate limits, or production egress policy.

The crash hook and duplicate replay proof use lower project APIs where the public CLI intentionally has no test-only crash flag and where checkpoint-id reuse would obscure the destination idempotency assertion. This boundary is ratified by `.10x/decisions/mvp-acceptance-demo-fixture-boundary.md`.

The drift-quarantine beat uses the existing DuckDB drift fixture rather than drifting the GitHub-shaped issue payload in the same test. The same child acceptance criterion is satisfied, but the broader conformance parent remains open for any later integrated live-provider polish.
