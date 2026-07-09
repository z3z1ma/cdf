Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Relates-To: .10x/tickets/done/2026-07-09-p2-ws-h2-cdf-add-single-file-parquet.md, .10x/specs/data-onramp-source-experience-cli.md

# P2 WS-H2 cdf add single-file Parquet evidence

## What was observed

`cdf add <id> <url-or-path>` now has the first S1-oriented product doorway for single-file Parquet resources. For a local or deterministic HTTPS Parquet URL, the command probes the Parquet schema, writes a pinned schema snapshot under `.cdf/schemas/`, writes `resources/<source>.toml`, updates `cdf.toml` with `[resources."<source>.<resource>"]`, regenerates `cdf.lock`, keeps append keyless, and renders the next command as `cdf run <id>`. `--dry-run` prints the proposed resource without writing project state. Signed/query URLs are rejected without echoing the secret query.

Generated clap artifacts were refreshed because adding `cdf add` changes completions, root help/man output, and introduces `cdf-add` help/man files.

## Procedure

- `CARGO_INCREMENTAL=0 cargo test -p cdf-cli add_ --locked`
  - Result: passed.
  - Coverage: `add_local_parquet_pins_schema_and_writes_resource_config`, `add_local_parquet_dry_run_writes_nothing`, `add_http_parquet_pins_schema_with_bounded_fixture_requests`, and `add_rejects_signed_url_without_leaking_secret_query` all passed.
- `cargo run -p cdf-cli --locked --features cli-artifacts --bin cdf-generate-cli-artifacts -- --out-dir crates/cdf-cli/generated`
  - Result: passed and regenerated committed CLI artifacts.
- `cargo test -p cdf-cli --locked --features cli-artifacts cli_generated_artifacts_match_committed_snapshots`
  - Result: passed.
- `cargo fmt --all -- --check`
  - Result: passed after integrating H2/D4/B5.
- `git diff --check`
  - Result: passed.

The H2/D4/B5 batch also completed broader integration gates before the final generated-artifact repair:

- `CARGO_INCREMENTAL=0 cargo check --workspace --all-targets --locked`: passed.
- `CARGO_INCREMENTAL=0 cargo check --workspace --all-targets --all-features --locked`: passed.
- `CARGO_INCREMENTAL=0 cargo check --workspace --all-targets --no-default-features --locked`: passed.
- `CARGO_INCREMENTAL=0 cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
- `CARGO_INCREMENTAL=0 cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`: passed.
- `CARGO_INCREMENTAL=0 cargo test --workspace --locked --no-fail-fast`: passed.
- `CARGO_INCREMENTAL=0 cargo nextest run --workspace --locked --no-fail-fast`: passed, 738 tests passed.
- `CARGO_INCREMENTAL=0 cargo test --workspace --doc --all-features --locked --no-fail-fast`: passed.
- `CARGO_INCREMENTAL=0 cargo doc --workspace --all-features --no-deps --locked`: passed.
- `CARGO_INCREMENTAL=0 cargo hack check --workspace --all-targets --each-feature --locked`: passed.
- `cargo deny --locked check`: passed with the already-ratified dual Arrow tuple warning.
- `cargo audit --deny warnings --ignore RUSTSEC-2024-0436`: passed.
- `cargo vet --locked --no-minimize-exemptions`: passed.
- `osv-scanner scan source --lockfile Cargo.lock --format json`: reported only the already-ratified `paste` / `RUSTSEC-2024-0436` advisory.
- `semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-rust-p2-h2-d4-b5.json --exclude target --exclude reports .`: passed with 0 findings.
- `cargo machete`: passed.
- `npx --yes jscpd@5 . --reporters json,console --output target/quality/reports/jscpd-p2-h2-d4-b5 --ignore "**/target/**,**/.git/**,**/reports/**,**/generated/**" --min-lines 12 --min-tokens 80 --threshold 10 --no-colors`: passed under the 10% threshold with 2.04% duplicated lines and 2.75% duplicated tokens.
- `rust-code-analysis-cli -m -p crates -O json -o target/quality/reports/rust-code-analysis-p2-h2-d4-b5`: passed after creating the output directory.
- Current tracked plus untracked non-ignored source Gitleaks mirror scan: passed with no leaks.
- `gitleaks git . --no-banner --redact`: reported exactly the two documented historical false positives from `.10x/knowledge/historical-gitleaks-findings.md`.
- Direct soundness inventory over touched files found no new `unsafe` blocks or FFI; hits were `Send`/`Sync` trait-object bounds in `cdf-declarative` file runtime and a test string.

## What this supports

This supports closing H2's scoped acceptance criteria: single-file local/HTTPS Parquet `cdf add`, deterministic pinned discovery artifacts, no-write dry run, no invented append key, redaction/signed URL failure, operator next-command output, and generated CLI artifact freshness.

It also advances P2 friction rows 2, 17, and 18 by making schema inference/scaffolding product-visible through `cdf add` for the first Parquet happy path. Full P2 S1/S2 closure still requires public TLC live evidence, HTTP glob/template enumeration, remote multi-file planning, and the recorded S1+S2 terminal session.

## Limits

This evidence does not close Postgres `cdf add`, REST `cdf add`, ad-hoc mode, interactive refinements, docs quickstart rewrite, public TLC S1/S2 live run, cloud object stores, HTTP template/glob enumeration, or final S1-S8 conformance.

`cargo llvm-cov`, the benchmark smoke gate, and the local CodeQL run were started after the broad gates above. `cargo llvm-cov` exposed only stale generated CLI artifacts; after the user directed not to rerun coverage for an artifact-only issue, coverage was stopped and the exact artifact freshness check was rerun instead. The benchmark and CodeQL runs were also stopped to commit the verified change set; they are not counted as passing evidence for H2.
