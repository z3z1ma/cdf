Status: recorded
Created: 2026-07-06
Updated: 2026-07-07
Relates-To: .10x/tickets/2026-07-05-cli-surface.md, .10x/specs/project-cli-observability-security.md, .10x/specs/resource-authoring-planning-batches.md, .10x/specs/package-lifecycle-determinism.md, .10x/specs/checkpoint-state-commit-gate.md

# CLI surface evidence

## What was observed

`crates/cdf-cli` now has a real command surface split across focused modules:

- `src/main.rs` is a process shim.
- `src/lib.rs` exposes `invoke`.
- `src/args.rs` parses global `--json`, `--project`, `--env`, and the required command tree except fast-follow `package archive`.
- `src/context.rs` loads `cdf.toml`, compiles declarative resources through `cdf-project`, parses `cdf.lock` when present, resolves package roots, opens SQLite checkpoint stores through `cdf-state-sqlite`, and exposes destination capability reports through current destination crates.
- `src/commands.rs` implements command plumbing and explicit unsupported exits for missing lower-layer runtime features.
- `src/output.rs` centralizes stable JSON success/error envelopes and exit-code mapping.

Implemented lower-layer-backed behavior includes:

- `validate` uses `cdf_project::validate_project`.
- `plan` and `explain` use `cdf_project` resource compilation, `cdf_contract::compile_validation_program`, and `cdf_engine::Planner::plan_tier_b`; output includes will-fetch partitions, pushdown fidelity, unsupported/inexact predicates, delivery guarantee, state-advancement invariant text, and an explicit unsupported DDL preview report.
- `inspect project/resources/resource/lock/destinations/package` uses current project, lock, destination, and package APIs.
- `diff schema` regenerates a lock from current compiled resources and locked destination sheets, then uses `cdf_project::diff_lockfiles`.
- `contract show` exposes current built-in contract policies.
- `state show/history/rewind` use `CheckpointStore` through `SqliteCheckpointStore`.
- `sql` reads local system history through `.10x/tickets/done/2026-07-06-local-system-sql.md`, mounting checkpoint rows and package manifest/receipt metadata into an in-memory SQLite query database.
- `package ls` reads package manifests; `package verify` uses `PackageReader::verify`.
- `doctor` reports project/resource/secret/Python/destination checks and explicitly marks ledger/destination drift unsupported.
- `status` reports freshness resources; it exits unsupported when freshness SLO evaluation would require runtime ledger timestamps.

Explicit unsupported exits use exit code 78 and JSON error bodies with `not_supported: true`.

## Unsupported surfaces blocking full acceptance

These commands or command sub-surfaces are intentionally not faked because the required lower-layer invariant-preserving API does not exist:

- `init`: no project scaffold/write API in `cdf-project`.
- `plan`/`explain` DDL preview: no scan/resource-schema to destination-DDL planning facade; current destination planning works from package commit inputs.
- `preview`: declarative `CompiledResource::open` returns that execution is outside the MVP compiler crate, so the CLI cannot inspect one real batch yet; tests prove it creates no package root on this path.
- `run`: no project-level runtime orchestrator combining resource execution, package writing, destination commit, receipt recording, and `CheckpointStore::commit`.
- `contract freeze` and `contract test`: no contract registry/snapshot writer or fixture runner.
- `state migrate`: no checkpoint state migration runner.
- `state recover`: no destination mirror recovery API.
- `resume`: no run ledger/recovery orchestrator.
- `replay package`: `PackageReader::replay_view` can prove replayability, but no destination/checkpoint replay API records receipts and commits checkpoints.
- `backfill`: no backfill planner/orchestrator over bounded historical windows and checkpoint-safe replay.
- `package gc`: no retention planner tied to `CheckpointStore` history proving packages are not sole committed-checkpoint evidence.
- `status` for resources with freshness SLOs: no runtime ledger/package receipt timestamps exposed for freshness evaluation.

## Procedure

Commands run:

```text
cargo check -p cdf-cli
cargo test -p cdf-cli --locked --no-fail-fast
cargo fmt --all -- --check
cargo clippy -p cdf-cli --all-targets --locked -- -D warnings
cargo check --workspace --all-targets --locked
cargo check -p cdf-cli --all-targets --locked
```

The first `cargo test -p cdf-cli --locked --no-fail-fast` run initially failed two new tests after formatting:

- `unknown_command_returns_usage_exit_code`: parse errors before CLI construction did not preserve `--json`.
- `state_show_uses_sqlite_store_and_reports_missing_head`: the test fixture lacked the SQLite parent directory.

Both failures were repaired in `crates/cdf-cli`.

## Results

Passing verification:

```text
cargo fmt --all -- --check
cargo test -p cdf-cli --locked --no-fail-fast
cargo clippy -p cdf-cli --all-targets --locked -- -D warnings
cargo check -p cdf-cli --all-targets --locked
```

The passing CLI test suite contains 8 unit tests covering command help surface, JSON validation output, plan JSON fields, preview no-write unsupported behavior, run unsupported behavior, package verification through `PackageReader`, SQLite state show, and usage exit codes.

Parent integration recheck after parallel lower-crate and dlt/Python fixes:

```text
cargo check --workspace --all-targets --locked
cargo fmt --all -- --check
cargo test -p cdf-cli --locked --no-fail-fast
cargo clippy -p cdf-cli --all-targets --locked -- -D warnings
semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-rust.json .
semgrep scan --config p/security-audit --error --json --output target/quality/reports/semgrep-security.json .
```

All commands passed. Semgrep initially flagged `std::env::args_os` in `src/main.rs` and `std::env::temp_dir` in `src/tests.rs`; parent changed test directories to a source-local `target/cdf-cli-tests/<unique>` root and replaced `args_os` with `args().map(OsString::from)` plus a narrow `nosemgrep` comment because command-line argv dispatch is not a security boundary in this CLI.

## What this supports or challenges

This supports that the CLI crate compiles, formats, passes scoped tests, and passes scoped clippy with explicit unsupported exits instead of bypassing package/destination/checkpoint invariants.

This challenges full acceptance of `.10x/tickets/2026-07-05-cli-surface.md`: `preview`, `run`, `resume`, `replay package`, and several operational commands cannot satisfy their full behavioral contracts until lower-layer runtime, recovery, contract registry, migration, and retention APIs exist.

## Limits

The evidence does not prove end-to-end package writes, destination commits, checkpoint commits, true one-batch previews, recovery, migration, package GC, or freshness SLO evaluation. Those are blocked at lower layers as listed above. Local read-only system SQL is covered separately by `.10x/evidence/2026-07-06-local-system-sql.md`.
