Status: recorded
Created: 2026-07-27
Updated: 2026-07-27
Relates-To: `.10x/tickets/2026-07-26-prewave-d1-environment-error-taxonomy.md`

# Foundational error ownership audit

## Observation

The foundational runtime, engine, package, task-store, object-access, kernel, and CLI boundaries
now distinguish host/process failures from external data shape and CDF-owned invariants.
`Environment` serializes as `environment`, renders as `CDF-ENV-HOST`, retains exit 70, and has
host-specific remediation. Typed errors survive JSON, Arrow, Parquet, scratch, archive, and
manifest wrapper chains. CLI error messages, details, suggestions, and remediation are redacted
before JSON and human rendering.

## Procedure

Cell: local `aarch64-apple-darwin`, Rust `1.96.1`, 2026-07-27.

- `cargo test -p cdf-kernel -p cdf-package -p cdf-task-store -p cdf-object-access
  -p cdf-cli-core --no-fail-fast` passed: kernel 75, package 93 then 95 after final review repairs
  with four performance tests ignored, task-store 22 with one slow stress test ignored,
  object-access 44 with one million-entry test ignored, and CLI-core 45.
- `cargo test -p cdf-engine -p cdf-runtime` passed: engine 208 with seven performance/stress tests
  ignored; runtime 151 with two performance tests ignored; seven build-graph tests and one
  compile-fail doctest passed.
- `cargo clippy -p cdf-kernel -p cdf-package -p cdf-task-store -p cdf-object-access
  -p cdf-runtime -p cdf-engine -p cdf-cli-core --all-targets --all-features -- -D warnings`
  passed. Package and CLI-core strict Clippy passed again after the final writer/redaction repair.
- `cargo run -p cdf-cli-core --locked --features cli-artifacts --bin
  cdf-generate-cli-artifacts -- --docs-dir docs --docs-only --check` reported the generated
  command and error reference fresh.
- With the existing dynamic test library exposed through `DUCKDB_LIB_DIR` and
  `DYLD_LIBRARY_PATH`, `cargo test -p cdf-cli` compiled, linked, and ran 272 product tests. The D1
  current-directory boundary and 269 other tests passed. Two schema-promotion receipt-clock tests
  failed because their direct destination path lacks injected `ExecutionServices`; that
  independently reproduced C1 follow-up is owned by
  `.10x/tickets/2026-07-27-prewave-c1b-promotion-receipt-clock-injection.md`.
- `git diff --check` passed.
- `graphify update .` failed with `command not found`; no graph refresh is claimed.

Focused assertions cover missing current directory, file-descriptor exhaustion, permission/local
I/O, temp/workspace failure, missing and malformed external artifacts, corrupt private scratch,
configured spill exhaustion, journal-free SQLite minimum and multi-page admission, symlink loops,
Arrow/Parquet read/write source chains, canonical JSON/manifest writer source chains, CLI
TTY/headless/JSON parity, multiple credential-bearing URIs, nested details/suggestions, and
private-key labels.

## What it supports or challenges

This supports D1's foundational ownership and presentation criteria within the named crates and
assertions. Two independent delegated OCR reviewers returned `pass` with no findings after
successive repairs to SQLite admission, CLI current-directory paths and redaction, artifact versus
scratch shape, archive/codec wrapper chains, and embedded writer failures.

## Limits

The tests inject representative I/O kinds and typed writer failures; they do not physically
exhaust disk, memory, or the process descriptor table on every supported OS. Filesystem-loop
classification uses stable platform error codes because the standard-library variant is unstable
on the active toolchain. Actual subprocess spawning belongs to D1b. The two independently owned
schema-promotion failures mean this evidence does not claim the complete CLI suite is green.
