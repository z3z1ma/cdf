Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p0-c5-property-fuzz-targets.md, .10x/specs/conformance-governance-roadmap.md

# P0 C5 property/fuzz targets

## What was observed

`cdf-conformance` now has a focused test-only `property_fuzz` module covering the C5 acceptance surfaces:

- contract row-disposition verdict-lattice totality;
- `SourcePosition` JSON serialization round-trips across `CHECKPOINT_STATE_VERSION`;
- adversarial `cdf_formats::read_ndjson_bytes` input;
- adversarial Singer and Airbyte protocol parser/read input.

No production parser behavior was changed. Native `cargo-fuzz` targets were intentionally not created because the active ticket made them optional, and the required C5 surfaces are covered by bounded property/adversarial tests without adding a fuzz workspace or corpus maintenance burden.

## Procedure

Implementation added `proptest v1.11.0` as a `cdf-conformance` dev-dependency, plus dev-dependency edges to `cdf-formats` and `cdf-subprocess` because the conformance property module imports those crates directly. `Cargo.lock` was kept to the minimal C5 graph: `cdf-conformance` dependency edges plus `proptest` and direct transitive packages.

Parent-observed commands:

```text
cargo fmt --all --check
git diff --check
jscpd crates/cdf-conformance/src/property_fuzz --reporters json,console --output target/quality/reports/jscpd-p0-c5-property-fuzz --ignore "**/target/**,**/.git/**,**/reports/**"
rust-code-analysis-cli -m -O json -p crates/cdf-conformance/src/property_fuzz > target/quality/reports/rust-code-analysis-p0-c5-property-fuzz.json
cargo fuzz list
gitleaks dir --redact --no-banner --report-format json --report-path target/quality/reports/gitleaks-p0-c5-property-fuzz-source.json crates/cdf-conformance/src/property_fuzz
cargo test --locked -p cdf-conformance property_fuzz -- --nocapture
cargo check -p cdf-conformance -p cdf-contract -p cdf-formats -p cdf-subprocess --all-targets --locked
cargo clippy -p cdf-conformance -p cdf-contract -p cdf-formats -p cdf-subprocess --all-targets --locked -- -D warnings
cargo nextest run -p cdf-conformance --locked
semgrep scan --config p/rust --error --json --no-git-ignore --output target/quality/reports/semgrep-p0-c5-property-fuzz-allfiles.json crates/cdf-conformance/src/property_fuzz crates/cdf-conformance/src/lib.rs crates/cdf-conformance/Cargo.toml
cargo deny check > target/quality/reports/cargo-deny-p0-c5-property-fuzz.txt
cargo audit --deny warnings --ignore RUSTSEC-2024-0436 > target/quality/reports/cargo-audit-p0-c5-property-fuzz.txt
cargo vet --locked > target/quality/reports/cargo-vet-p0-c5-property-fuzz.txt
gitleaks dir --redact --no-banner --report-format json --report-path target/quality/reports/gitleaks-p0-c5-cargo-lock.json Cargo.lock
gitleaks dir --redact --no-banner --report-format json --report-path target/quality/reports/gitleaks-p0-c5-cargo-toml.json crates/cdf-conformance/Cargo.toml
cargo machete --with-metadata crates/cdf-conformance > target/quality/reports/cargo-machete-p0-c5-cdf-conformance.txt
rg -n "\\bunsafe\\b|extern \"|\\*const|\\*mut|Send for|Sync for" crates/cdf-conformance/src/property_fuzz crates/cdf-conformance/Cargo.toml
osv-scanner --lockfile Cargo.lock --format json > target/quality/reports/osv-p0-c5-property-fuzz.json
```

## Results

- `cargo fmt --all --check`: pass.
- `git diff --check`: pass.
- `cargo test --locked -p cdf-conformance property_fuzz -- --nocapture`: pass; 14 tests passed, 0 failed, 46 filtered out, test execution 0.13s after build artifacts were warm.
- `cargo check -p cdf-conformance -p cdf-contract -p cdf-formats -p cdf-subprocess --all-targets --locked`: pass.
- `cargo clippy -p cdf-conformance -p cdf-contract -p cdf-formats -p cdf-subprocess --all-targets --locked -- -D warnings`: pass.
- `cargo nextest run -p cdf-conformance --locked`: pass; 60 tests passed, 0 failed, 0 skipped, one expected slow live-run test, total 62.470s.
- `jscpd`: pass; 4 Rust files analyzed, 563 lines, 3504 tokens, 0 clones, 0 duplicated lines/tokens.
- `rust-code-analysis-cli`: pass; 5 files in report, 33 functions, max cyclomatic complexity 4, max cognitive complexity 3.
- `semgrep scan --config p/rust --no-git-ignore`: pass; 6 files scanned, 11 Rust rules, 0 findings.
- `gitleaks dir` over `crates/cdf-conformance/src/property_fuzz`: pass; no leaks found.
- `gitleaks dir` over `Cargo.lock`: pass; no leaks found.
- `gitleaks dir` over `crates/cdf-conformance/Cargo.toml`: pass; no leaks found.
- Direct unsafe/FFI scan over the new harness and manifest: no matches.
- `cargo deny check`: pass; stdout ended with `advisories ok, bans ok, licenses ok, sources ok`. It emitted already-known duplicate Arrow 58/59 warnings covered by Workstream D records.
- `cargo audit --deny warnings --ignore RUSTSEC-2024-0436`: pass after scanning 447 locked crate dependencies.
- `cargo vet --locked`: initially failed on 9 new unvetted dev-test dependencies from the `proptest` graph: `bit-set`, `bit-vec`, `fnv`, `proptest`, `quick-error`, `rand_xorshift`, `rusty-fork`, `unarray`, and `wait-timeout`. Added exact current-version `safe-to-run` exemptions in `supply-chain/config.toml`, ran `cargo vet fmt`, then reran `cargo vet --locked`: pass; `Vetting Succeeded (402 exempted)`.
- `cargo machete --with-metadata crates/cdf-conformance`: pass for the touched crate. A full workspace machete run reported an unrelated existing `cdf-cli` / `cdf-dest-parquet` finding; C5 did not modify that crate.
- `cargo fuzz list`: expected failure because no `fuzz/Cargo.toml` exists. Native fuzz targets were not created for C5.
- `osv-scanner --lockfile Cargo.lock`: nonzero only for the already-ratified `paste 1.0.15` / `RUSTSEC-2024-0436` advisory. No new C5 dependency vulnerabilities were reported.

## What this supports

- Contract verdict-lattice generation succeeds exactly when generated `row_dispositions` cover every `RuleOutcome::ALL` value; a deterministic permutation test also proves every ordering of the complete active outcome set is accepted.
- Source-position coverage includes every active `SourcePosition` variant, every active `CursorValue` variant, `CHECKPOINT_STATE_VERSION == 1`, embedded version assertions, and JSON value/string round-trips.
- NDJSON adversarial coverage includes malformed records, mixed valid/invalid records, truncated records, invalid UTF-8, top-level non-object inputs, arbitrary byte vectors, and oversized/strange scalar values. Invalid inputs return errors instead of partial reads.
- Singer and Airbyte coverage includes malformed protocol messages, truncated streams, foreign state payload conversion, unknown fields retained in raw messages, and unknown message types accepted as `Other` according to current parser behavior.

## Limits

- The property tests are bounded, not coverage-guided fuzzing: NDJSON arbitrary-byte coverage uses 64 cases and protocol arbitrary-byte coverage uses 32 cases per run.
- No native `cargo-fuzz` targets or corpus were created; `cargo fuzz list` was therefore not applicable for this C5 implementation.
- CodeQL was not rerun for C5. This slice changed a test-only conformance harness plus dev-test dependencies; Semgrep, Gitleaks, direct unsafe/FFI search, cargo-audit, cargo-deny, cargo-vet, OSV, and focused/full conformance tests covered the risk without recreating the expensive CodeQL database.
