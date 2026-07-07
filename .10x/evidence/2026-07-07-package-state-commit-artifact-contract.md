Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-06-package-state-commit-artifact-contract.md, .10x/decisions/package-state-commit-preimage-artifacts.md, .10x/specs/package-lifecycle-determinism.md, .10x/specs/destination-receipts-guarantees.md, .10x/specs/checkpoint-state-cdf-line.md

# Package state and commit artifact contract evidence

## What was observed

The package preimage artifact implementation writes and verifies identity-participating state/commit evidence and reconstructs runtime replay inputs from package files. Parent review hardened reconstruction-time validation after mutation testing exposed under-tested negative cases.

Focused implementation verification passed:

- `cargo fmt --all -- --check`
- `cargo clippy -p cdf-package -p cdf-project -p cdf-conformance -p cdf-engine --all-targets --locked -- -D warnings`
- `cargo test -p cdf-package -p cdf-project -p cdf-conformance --locked --no-fail-fast`
- `cargo test -p cdf-package --locked replay_inputs_rejects_invalid_state_preimage_semantics`

Focused package/conformance test coverage included:

- `cdf-conformance`: 37 unit tests passed, including prepared artifact replay, live artifact recovery without the source file, duplicate live package replay from artifacts, and 100-run live golden determinism.
- `cdf-package`: 26 unit tests passed, including typed reconstruction, tampered/missing preimage identity failures, and mutation-hardened invalid preimage semantics.
- `cdf-project`: 32 unit tests passed, including artifact replay reconstruction, corrupted/missing preimage failures before mutation, and manifest/package-hash mismatch failures before mutation.
- Doc tests for `cdf-conformance`, `cdf-package`, and `cdf-project`: 0 tests, passed.

Broader `QUALITY.md` verification passed:

- `cargo metadata --format-version=1 --locked --no-deps`
- `cargo check --workspace --all-targets --locked`
- `cargo clippy --workspace --all-targets --locked -- -D warnings`
- `cargo nextest run --workspace --locked --no-fail-fast`: 301 tests passed.
- `cargo hack check --workspace --all-targets --locked`
- `cargo test --doc --workspace --locked`
- `cargo doc --workspace --no-deps --locked`
- `cargo deny check`
- `cargo audit --json > target/quality/reports/cargo-audit-package-artifacts.json`
- `cargo vet --locked --output-format json --output-file target/quality/reports/cargo-vet-package-artifacts.json`
- `osv-scanner scan source -r . --format json --output target/quality/reports/osv-package-artifacts.json`: 0 result entries across 429 packages.
- `semgrep scan --config auto --error --json --output target/quality/reports/semgrep-package-artifacts-final.json crates/cdf-package crates/cdf-engine crates/cdf-project crates/cdf-conformance`: 0 findings.
- Source-only Gitleaks scan over an rsync mirror excluding `.git`, `target`, `reports`, `.venv`, and `.mypy_cache`: no leaks.
- `tools/codeql-rust-quality.sh`: refreshed the reusable database at `target/quality/codeql-db-rust` because Rust source changed, analyzed `codeql/rust-queries`, and produced 0 SARIF results. Extraction errors were 0; extraction warnings were the documented local Rust extractor macro-expansion limit.
- `cargo semver-checks check-release --workspace --baseline-rev HEAD`: no semver update required for workspace crates.
- `cargo machete`: no unused dependencies.
- `cargo +nightly udeps --workspace --all-targets --locked`: all deps used.
- `rust-code-analysis-cli -m -p crates -O json -o target/quality/reports/rust-code-analysis-package-artifacts`
- `jscpd . --reporters json,console --output target/quality/reports/jscpd-package-artifacts --ignore "**/target/**,**/.git/**,**/reports/**"`: exited 0; duplicate output treated as gradient noise.
- `cargo llvm-cov --workspace --locked --summary-only`: total line coverage 79.07%, functions 75.33%, regions 81.92%.
- Isolated `cargo geiger` package scan for `cdf-package` plus direct first-party unsafe/FFI/raw-pointer scan.
- `git diff --check -- . ':(exclude).gitignore'`

Mutation verification:

- Initial bounded `cargo mutants --package cdf-package --file crates/cdf-package/src/artifacts.rs --jobs 4 --test-tool cargo --cargo-arg --locked --output target/quality/mutants-package-artifacts -- -p cdf-package` runs found missed mutants in input checkpoint committed-head validation, tuple validation, parent/input-position validation, null-checkpoint validation, empty state-segment validation, and row/byte count validation.
- Parent hardening added focused negative assertions for each missed semantic branch.
- Final bounded mutation run passed: 27 mutants tested, 22 caught, 5 unviable, 0 missed.

Policy guardrails:

- Direct scan for `unsafe`, FFI, raw pointers, and manual `Send`/`Sync` implementations across `crates/cdf-package`, `crates/cdf-engine`, `crates/cdf-project`, and `crates/cdf-conformance` returned no matches.
- Direct scan for direct `parquet` or `paste` dependency entries in `Cargo.lock` and first-party crate manifests returned no matches, so this ticket did not change native Parquet policy.

## Procedure

Implemented typed preimage artifacts in `cdf-package`, an engine pre-finalization hook, live-run artifact writing in `cdf-project`, artifact-driven DuckDB replay/recovery, and conformance fixture/golden updates. Parent review then added reconstruction-time semantic validation tests in `cdf-package` until bounded mutation testing over `crates/cdf-package/src/artifacts.rs` had no surviving mutants.

## What this supports

This supports the ticket acceptance criteria for:

- writing `state/input_checkpoint.json`, `state/proposed_delta.json`, and `destination/commit_plan.json` before package finalization;
- omitting `package_hash` from state preimages and using `idempotency_token_source = "package_hash"` in commit-plan preimages;
- catching tampered or missing identity-participating state/commit artifacts through package verification;
- reconstructing final `StateDelta` and concrete DuckDB commit inputs from verified package artifacts plus the finalized manifest package hash;
- replaying and recovering live/prepared packages from package artifacts without source contact;
- updating live and prepared golden package evidence.
- preserving the existing no-direct-`parquet`/`paste` package state while native Arrow/DataFusion Parquet replacement is owned by separate tickets.

## Limits

Mutation testing was bounded to `crates/cdf-package/src/artifacts.rs`; runtime/conformance paths were covered by focused tests and full workspace nextest rather than separate runtime mutation. CodeQL extraction warnings remain the documented local Rust extractor macro-expansion limit with 0 extraction errors and 0 SARIF results. This evidence does not implement CLI `resume`, run-ledger defaults, native Parquet policy changes, or a generic destination replay abstraction.
