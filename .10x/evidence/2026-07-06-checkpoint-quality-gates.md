Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-05-checkpoint-store-sqlite.md, .10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md

# Checkpoint store quality gates

## What was observed

The checkpoint-store slice passed the current executable gates for formatting, workspace compile, feature combinations, lints, tests, doctests, coverage, mutation testing, semver comparison, dependency hygiene, vulnerability scans, secret scans, Semgrep, and CodeQL. The final source tree included the ratified `CheckpointStore: Send + Sync` shared-receiver trait in `firn-kernel`, the synchronized in-memory store, and the WAL-backed SQLite store.

Two supply-chain policy gates remain intentionally open under `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`: full `cargo deny check` needs a ratified license allowlist, and `cargo vet` needs an adoption decision plus a `supply-chain/` store.

## Procedure

All commands were run from `/Users/alexanderbut/code_projects/personal/firn` on 2026-07-06 unless a subdirectory is named.

Core gates passed:

```text
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo check --workspace --all-targets --all-features --locked
cargo check --workspace --all-targets --no-default-features --locked
cargo hack check --workspace --all-targets --each-feature --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings
cargo hack clippy --workspace --all-targets --each-feature --locked -- -D warnings
cargo test --workspace --all-targets --locked --no-fail-fast
cargo nextest run --workspace --locked
cargo test --workspace --doc --all-features --locked --no-fail-fast
git diff --check
```

The final workspace test run passed with 22 unit tests total: 8 in `firn-kernel`, 14 in `firn-state-sqlite`, and zero failures. The final nextest run passed 22 tests.

Coverage passed:

```text
cargo llvm-cov --workspace --all-features --locked --summary-only
```

Summary:

```text
firn-kernel/src/lib.rs        87.38% line coverage
firn-state-sqlite/src/lib.rs  97.03% line coverage
TOTAL                         94.61% line coverage
```

Mutation testing passed:

```text
cargo mutants -p firn-state-sqlite --test-tool nextest --timeout 60 --minimum-test-timeout 5 -j 4 -o /tmp/firn-mutants-checkpoint-parent-final --cargo-arg=--locked
```

Result: 111 mutants tested, 74 caught, 37 unviable, 0 missed.

Dependency and API hygiene passed:

```text
cargo machete
cargo audit
cargo deny check advisories
cargo semver-checks --workspace --baseline-rev HEAD
osv-scanner scan source -r . --format json --output /tmp/firn-osv-final.json
jq '(.results // .Results // []) | length' /tmp/firn-osv-final.json
```

`cargo machete` found no unused dependencies. `cargo audit` scanned 99 crate dependencies and exited successfully. `cargo deny check advisories` reported `advisories ok`. `cargo semver-checks` reported no semver update required across the workspace. OSV reported 0 vulnerability records.

Security scanners passed:

```text
semgrep scan --config p/rust --error --json --output /tmp/firn-semgrep-rust-final.json .
semgrep scan --config p/security-audit --error --json --output /tmp/firn-semgrep-security-final.json .
gitleaks git --no-banner --redact --report-format json --report-path /tmp/firn-gitleaks-git-final.json
gitleaks dir --no-banner --redact --report-format json --report-path /tmp/firn-gitleaks-dir-final.json .
codeql database create /tmp/firn-codeql-db-final --language=rust --source-root . --overwrite --command 'cargo check --workspace --all-targets --locked'
codeql database analyze /tmp/firn-codeql-db-final codeql/rust-queries --format=sarif-latest --output=/tmp/firn-codeql-rust-final.sarif
jq '.runs[0].results | length' /tmp/firn-codeql-rust-final.sarif
```

Semgrep Rust and security-audit configs reported 0 findings. `gitleaks git` and the final `gitleaks dir` scan reported 0 findings. CodeQL created and analyzed a Rust database and produced 0 SARIF results; the extractor reported Cargo metadata and macro-expansion warnings and metric data of 19 extracted Rust files with errors and 63 without error.

Unsafe and complexity probes:

```text
rustup toolchain list
rg -n "\bunsafe\b|unsafe\s+impl|unsafe\s+trait|extern\s+\"C\"|from_raw|into_raw|transmute|MaybeUninit|\bSend\b|\bSync\b" crates/firn-kernel crates/firn-state-sqlite
cargo geiger --all-features
rust-code-analysis-cli -m -p crates -O json -o /tmp/firn-rust-code-analysis-final
jscpd . --reporters json,console --output /tmp/firn-jscpd-final --ignore "**/target/**,**/.git/**,**/reports/**"
```

Only the stable toolchain is installed. Direct source search found no first-party `unsafe`, FFI, raw pointer conversion, transmute, or `MaybeUninit` usage; it found only the intentional `Send` and `Sync` type bounds. `cargo geiger`, run from `crates/firn-state-sqlite`, exited nonzero because of scanner warnings but reported first-party `firn-state-sqlite` and `firn-kernel` as `0/0` unsafe. `rust-code-analysis-cli` exited successfully. `jscpd` reported clones mostly in `QUALITY.md`, existing records, and test/helper repetition; no implementation change was made because the Rust clone rate was low and the repeated checkpoint-store test helpers intentionally exercise the same contract over both stores.

Policy-blocked gates:

```text
cargo deny check
cargo vet
```

`cargo deny check` exited 4: advisories, bans, and sources were ok, but licenses failed because the repository still has no `deny.toml` allowlist, so even Apache-2.0/MIT licenses are rejected. `cargo vet` exited 255 because `supply-chain/` has not been initialized. Both are owned by `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`.

Skipped gates:

Nightly-only `cargo +nightly udeps`, Miri, `cargo careful`, and sanitizer runs were skipped because only `stable-aarch64-apple-darwin` is installed and direct/Geiger checks found no first-party unsafe or FFI in this slice. `cargo fuzz`, Kani, benchmark, flamegraph, and bloat checks were skipped because this ticket added no configured fuzz targets, Kani proofs, benchmarks, or performance/binary-size acceptance criteria.

## What this supports or challenges

This supports closing the checkpoint-store child ticket: its acceptance criteria are covered by focused tests, workspace regressions stayed green, mutation testing found no surviving checkpoint-store mutants, and security scanners found no actionable finding in the current commit candidate.

This challenges supply-chain closure at the repository level only: the dependency policy cannot be called fully clean until `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md` is executed.

## Limits

Tool success does not prove absence of defects. CodeQL had extractor warnings, Geiger reported dependency unsafe code and scanner warnings, and jscpd is a similarity signal rather than a correctness proof. The supply-chain policy ticket remains open and must not be represented as completed by this checkpoint-store closure.
