Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Relates-To: .10x/tickets/done/2026-07-09-p2-ws-a6-rest-sample-discovery-autopin.md

# P2 WS-A6 REST sample discovery auto-pin evidence

## What was observed

The A6 slice makes declarative REST discover-mode resources usable through the generic discovery path:

- `cdf_declarative::discover_rest_sample_schema` samples one REST page through the existing request validation, allowlist, auth, retry, rate-limit, selector, and JSON decoding path.
- REST sample inference emits deterministic Arrow fields for scalar JSON values and marks null or missing sampled fields nullable, including fields first seen after earlier records.
- Runtime REST execution accepts `SchemaSource::Discovered { snapshot }` and uses the pinned snapshot hash for package evidence.
- `cdf-project` dispatches REST discovery through the generic resource discovery API when an explicit HTTP transport is supplied, and first-use preparation writes pinned snapshots.
- `cdf-cli` wires REST discover-mode resources through `cdf schema discover`, `cdf plan`, `cdf preview`, and `cdf run` with real HTTP transport construction and project secret resolution.

This evidence supports only the one-page REST sample discovery and auto-pin slice. It does not close multi-page REST sampling, cursor inference, `schema pin/show/diff`, `cdf add`, Python/WASM discovery, remote file discovery, or conformance S5 closure.

## Procedure and results

Focused behavior tests passed:

- `cargo test -p cdf-declarative rest_sample_discovery_infers_scalars_and_uses_runtime_request_path --locked`
- `cargo test -p cdf-declarative rest_runtime_executes_with_discovered_snapshot_hash --locked`
- `cargo test -p cdf-declarative rest_runtime_refreshes_auth_once_when_configured --locked`
- `cargo test -p cdf-project generic_schema_discovery_dispatch_samples_rest_without_snapshot_write --locked`
- `cargo test -p cdf-project generic_discover_prepare_autopins_rest_snapshot --locked`
- `cargo test -p cdf-project general_project_run_executes_rest_with_discovered_snapshot_hash --locked`
- `cargo test -p cdf-cli schema_discover_rest_reports_sample_schema_without_project_writes_or_secret_leak --locked`
- `cargo test -p cdf-cli rest_discover_mode_plan_preview_run_autopins_through_file_secret_without_leaks --locked`

Workspace gates passed after the final clippy cleanup:

- `cargo fmt --all -- --check`
- `git diff --check`
- `cargo clippy --workspace --all-targets --locked -- -D warnings`
- `cargo test --workspace --locked --no-fail-fast`

Quality and security checks:

- `jscpd --min-lines 8 --min-tokens 80 --threshold 0 ...` over implementation files found `0` clones, `0` duplicated lines, `0.00%` duplication.
- A broader report-only `jscpd` pass over touched implementation and tests found `75` existing test-heavy clones, `1114` duplicated lines, `4.97%` duplication; this was not used as a hard gate because the implementation-only gate is clean.
- `rust-code-analysis-cli -m -O json ...` completed and wrote JSON metrics under `target/quality/reports/rust-code-analysis-a6/`.
- `semgrep --config p/rust --config p/secrets ...` completed with `0` findings.
- `gitleaks detect --no-git --source crates ...` completed with no leaks.
- `cargo deny check` completed with `advisories ok, bans ok, licenses ok, sources ok`.
- `cargo audit --json --ignore RUSTSEC-2024-0436` reported `vulnerabilities.found = false`.
- `cargo vet --locked` reported `Vetting Succeeded (455 exempted)`.
- `cargo machete` found no unused dependencies.
- `osv-scanner scan --lockfile Cargo.lock --format json` reported only the already-ratified `RUSTSEC-2024-0436` `paste` advisory.
- `tools/codeql-rust-quality.sh` used `target/quality/codeql-db-rust` and completed analysis. SARIF results were limited to the three pre-existing `rust/hard-coded-cryptographic-value` findings in `crates/cdf-cli/src/tests.rs`, owned by `.10x/tickets/done/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md`. No A6 implementation finding was reported.

An unsafe keyword scan over touched Rust files found no unsafe Rust block. The only match was the test predicate string `PredicateId::new("unsafe")` in `crates/cdf-declarative/src/tests.rs`.

## What this supports

REST discovery no longer fails solely because a discover-mode declarative REST resource lacks hand-declared schema fields when the CLI or project layer can supply an HTTP transport. First-use auto-pin writes deterministic schema snapshots before plan/preview/run execute, and execution consumes the pinned `SchemaSource::Discovered` hash rather than falling back to declared-only behavior.

## Limits

The discovery sample is one page by design for this slice. The test coverage does not claim pagination-wide schema union, REST cursor inference, source-level conformance S5 closure, or final P2 exit criteria. CodeQL extraction still reports many Rust extraction warnings from macro-heavy code, matching the current tool limitations, but SARIF had no new A6 security finding.
