Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-07-cli-preview-resource-breadth.md, .10x/decisions/preview-one-batch-sampling-semantics.md, .10x/specs/project-cli-observability-security.md

# CLI preview resource breadth evidence

## What was observed

`cdf preview` now inspects one direct-stream batch for broader ratified resource shapes while preserving no-write behavior:

- local file preview covers CSV, JSON, Parquet, and Arrow IPC file resources;
- file glob preview samples the first deterministic path-sorted match;
- REST preview uses the declared REST runtime with the project secret provider and exact cursor pushdown when declared exact;
- table-backed Postgres SQL preview uses the declared SQL runtime with the project secret provider;
- SQL query resources fail closed;
- direct preview fails closed when residual predicates, projection, or limits would require package-producing engine work;
- JSON output includes `resource`, `partition`, `batch`, `row_count`, `byte_count`, and `write_effects`, while retaining the legacy `resource_id`, `partition_id`, `batch_id`, and `writes` fields.

The implementation adds `CompiledResource::open_preview` for file resources, keeps live file execution on the old single-file semantics, and adds a native Arrow IPC file reader in `cdf-formats`.

## Procedure

Correctness and runtime gates:

```text
cargo fmt --all -- --check
git diff --check
cargo check -p cdf-cli -p cdf-declarative -p cdf-formats --all-targets --locked
cargo clippy -p cdf-cli -p cdf-declarative -p cdf-formats --all-targets --locked -- -D warnings
cargo test -p cdf-cli -p cdf-declarative -p cdf-formats --locked --no-fail-fast
cargo nextest run -p cdf-cli -p cdf-declarative -p cdf-formats --locked
cargo doc -p cdf-cli -p cdf-declarative -p cdf-formats --no-deps --locked
cargo hack check -p cdf-cli -p cdf-declarative -p cdf-formats --all-targets --each-feature --locked
cargo +nightly careful test -p cdf-formats arrow_ipc --locked
```

Security, supply-chain, and source hygiene:

```text
cargo deny check
cargo audit
cargo vet --locked
osv-scanner --lockfile Cargo.lock
semgrep scan --config p/rust --error --json --metrics off --disable-version-check --output target/quality/preview-breadth/semgrep-rust.json crates/cdf-cli/src/scan_command.rs crates/cdf-declarative/src/file_runtime.rs crates/cdf-formats/src/readers.rs
tools/codeql-rust-quality.sh
gitleaks dir --no-banner --redact --log-level error --report-format json --report-path target/quality/preview-breadth/gitleaks-crates.json crates
gitleaks dir --no-banner --redact --log-level error --report-format json --report-path target/quality/preview-breadth/gitleaks-10x-decision.json .10x/decisions/preview-one-batch-sampling-semantics.md
gitleaks dir --no-banner --redact --log-level error --report-format json --report-path target/quality/preview-breadth/gitleaks-10x-spec.json .10x/specs/project-cli-observability-security.md
gitleaks dir --no-banner --redact --log-level error --report-format json --report-path target/quality/preview-breadth/gitleaks-10x-ticket.json .10x/tickets/done/2026-07-07-cli-preview-resource-breadth.md
gitleaks git --no-banner --redact --log-level error --report-format json --report-path target/quality/preview-breadth/gitleaks-git.json .
rg -n "unsafe\s*\{|unsafe\s+impl\s+(Send|Sync)|std::mem::transmute|\*const\s+|\*mut\s+" crates/cdf-cli/src crates/cdf-declarative/src crates/cdf-formats/src
cargo geiger --manifest-path /Users/alexanderbut/code_projects/personal/firn/crates/cdf-cli/Cargo.toml --all-targets --include-tests --locked --forbid-only --output-format Json
cargo geiger --manifest-path /Users/alexanderbut/code_projects/personal/firn/crates/cdf-declarative/Cargo.toml --all-targets --include-tests --locked --forbid-only --output-format Json
cargo geiger --manifest-path /Users/alexanderbut/code_projects/personal/firn/crates/cdf-formats/Cargo.toml --all-targets --include-tests --locked --forbid-only --output-format Json
```

Maintainability and dependency hygiene:

```text
rust-code-analysis-cli -m -p crates/cdf-cli/src/scan_command.rs -O json -o target/quality/preview-breadth/rust-code-analysis/scan_command
rust-code-analysis-cli -m -p crates/cdf-declarative/src/file_runtime.rs -O json -o target/quality/preview-breadth/rust-code-analysis/file_runtime
rust-code-analysis-cli -m -p crates/cdf-formats/src/readers.rs -O json -o target/quality/preview-breadth/rust-code-analysis/readers
jscpd crates/cdf-cli/src/scan_command.rs crates/cdf-declarative/src/file_runtime.rs crates/cdf-formats/src/readers.rs --min-lines 6 --min-tokens 50 --reporters console,json --output target/quality/preview-breadth/jscpd
cargo machete --with-metadata
scc crates/cdf-cli/src/scan_command.rs crates/cdf-declarative/src/file_runtime.rs crates/cdf-formats/src/readers.rs --format json
cargo metadata --format-version=1 --locked --no-deps
```

## Results

- Final cargo gate status: passed. Targeted crate tests reported `cdf-cli` 121 passed, `cdf-declarative` 50 passed, `cdf-formats` 10 passed, and doc tests with zero failures. Nextest reported 182 passed, 0 skipped. `cargo doc` and `cargo hack check --each-feature` passed.
- Focused careful gate: `cargo +nightly careful test -p cdf-formats arrow_ipc --locked` passed 2 Arrow IPC tests.
- `cargo deny check`: passed with advisories, bans, licenses, and sources OK.
- `cargo audit`: passed with one allowed warning for ratified `RUSTSEC-2024-0436` on `paste`.
- `cargo vet --locked`: passed, reporting `Vetting Succeeded (452 exempted)`.
- `osv-scanner --lockfile Cargo.lock`: non-zero only for ratified `RUSTSEC-2024-0436` on `paste 1.0.15`; no fixed version reported.
- Semgrep Rust scan: 0 findings, 0 errors.
- CodeQL via `tools/codeql-rust-quality.sh`: status 0, reusable DB path `target/quality/codeql-db-rust`, 0 SARIF results. The wrapper refreshed the DB because Rust source content changed.
- CodeQL extractor diagnostics: 0 extraction errors, 3521 extraction warnings, 229 Rust files extracted. This matches the documented local Rust extractor macro-warning limitation in `.10x/knowledge/quality-gate-execution.md`.
- Gitleaks source scans over `crates` and the touched `.10x` records: passed with no leaks.
- Gitleaks full-history scan: two pre-existing `generic-api-key` findings in removed historical paths `src/cdf/core/project.py` and `src/cdf/core/feature_flag/harness.py`. Existing owner: `.10x/tickets/2026-07-08-historical-gitleaks-findings-triage.md`. This slice added no source leak.
- Direct unsafe construct scan over touched crate source: no matches. A broader first pass matched only a test variable name `unsafe_predicate`, not an unsafe construct.
- Geiger forbid-only scans for `cdf-cli`, `cdf-declarative`, and `cdf-formats`: exit 0. The tool emitted dependency parser/match warnings for third-party crates, so the direct first-party unsafe scan is the stronger evidence for this slice.
- `cargo machete --with-metadata`: no unused dependencies found.
- `jscpd` over touched source files: 3 files, 1743 lines, 2 clones, 15 duplicated lines (0.86%), 104 duplicated tokens (0.94%).
- `rust-code-analysis-cli` completed for the touched source files. Highest cognitive-complexity hotspots were existing file-format/glob helpers; the new `validate_preview_direct_stream_plan` reports cognitive 5, cyclomatic 7, physical LOC 24. `scc` over touched source reports 3 Rust files, 1743 lines, 1616 code lines, complexity 114.
- `cargo metadata --locked --no-deps` confirms `cdf-cli`, `cdf-declarative`, and `cdf-formats` are `publish = false`; `cargo semver-checks` was not applicable to this unpublished internal slice.

## What this supports

The evidence supports all acceptance criteria in `.10x/tickets/done/2026-07-07-cli-preview-resource-breadth.md`: REST, table-backed SQL, Arrow IPC, and multi-file preview behavior exists; unsupported query and residual/direct-stream-incompatible requests fail closed; preview no-write behavior is covered by CLI tests; JSON output includes the required fields; and applicable quality gates ran with only recorded project-level residuals.

## Limits

This evidence does not claim package-free engine residual preview exists. The active decision requires fail-closed behavior for residual predicates, unpushed projection, and unpushed limit. Full-history Gitleaks residuals are outside this slice and remain owned by `.10x/tickets/2026-07-08-historical-gitleaks-findings-triage.md`. The accepted `paste` advisory remains governed by the active supply-chain decisions.
