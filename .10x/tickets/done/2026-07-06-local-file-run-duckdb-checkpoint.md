Status: done
Created: 2026-07-06
Updated: 2026-07-07
Parent: .10x/tickets/done/2026-07-05-cli-surface.md
Depends-On: .10x/tickets/done/2026-07-05-datafusion-engine-planner.md, .10x/tickets/done/2026-07-05-duckdb-destination.md, .10x/tickets/done/2026-07-05-checkpoint-store-sqlite.md, .10x/tickets/done/2026-07-06-package-replay-commit-gate-runtime.md, .10x/tickets/done/2026-07-06-declarative-file-preview-execution.md

# Implement local file run to DuckDB and checkpoint

## Scope

Implement the first live `cdf run` slice that preserves the commit-gate invariant end to end:

1. Open exactly one compiled declarative local file resource.
2. Execute it through `cdf-engine` into a package under the selected environment package root.
3. Commit that package into a local `duckdb://` destination.
4. Commit a SQLite checkpoint only after the DuckDB receipt verifies.

Owns the smallest necessary changes in:

- `crates/cdf-engine/**` only if the engine must expose segment/source-position metadata needed to build a valid `StateDelta` without reopening the source.
- `crates/cdf-project/**` for a reusable project-level live-run helper that composes `execute_to_package_with_run_id`, `replay_prepared_duckdb_package`, `DuckDbDestination`, and `CheckpointStore`.
- `crates/cdf-cli/**` for bounded `run` argument parsing, command wiring, and integration tests.
- Cargo manifests and `Cargo.lock` only for required internal crate edges.

Keep crate roots thin and follow `.10x/knowledge/rust-crate-organization.md`.

## Acceptance criteria

- `cdf run --resource <RESOURCE_ID> --pipeline <PIPELINE_ID> --target <TARGET> --package-id <PACKAGE_ID> --checkpoint-id <CHECKPOINT_ID>` succeeds for a single-match declarative local file resource in a project whose selected environment uses `duckdb://...` and `sqlite://...`.
- The command creates exactly one package directory at `<environment packages>/<PACKAGE_ID>` and refuses to overwrite an existing package directory or manifest.
- The package is produced by `cdf-engine` from the compiled resource and ends in `checkpointed` status only after the destination receipt has been verified and the SQLite checkpoint has committed.
- The DuckDB destination contains the loaded rows and `_cdf_loads` / `_cdf_state` mirror evidence for the committed package.
- The SQLite checkpoint head exists for the explicit `(pipeline_id, resource_id, resource state_scope)` tuple, references the committed package hash, schema hash, receipt id, state segments, and output position, and has no committed head before the receipt-gated commit path succeeds.
- The `StateDelta` is constructed from explicit command inputs, current resource descriptor, current committed head if present, engine/package segment evidence, and source positions observed during package execution. It MUST NOT infer pipeline id, target, checkpoint id, or package id from filenames, resource names, project names, or destination names in this slice.
- Schema hash handling is explicit. This first slice MUST support declared schemas with a concrete `SchemaSource::Declared` hash and fail with an actionable error for resources whose schema hash is absent or discovered-only.
- Destination handling is explicit. This first slice MUST support only local `duckdb://` destinations and MUST return the existing explicit unsupported style for Postgres, Parquet/object-store, external, or malformed destinations.
- `--loop` remains unsupported for this slice.
- JSON output includes command, resource id, pipeline id, target, package id, package directory, package hash, package status, checkpoint id, checkpoint committed/head status, receipt id, receipt source including duplicate/no-op details, row count, segment count, and write effects.
- Human output states the resource, package hash, destination target, checkpoint id, and that the commit gate was crossed after receipt verification.
- Existing `plan`, `explain`, `preview`, package archive, doctor, status, and state commands remain compatible.

## Evidence expectations

- CLI integration tests prove a successful local file run writes package, DuckDB rows, DuckDB mirrors, and SQLite checkpoint head.
- Negative tests prove omitted explicit run ids/target/package/checkpoint inputs fail before package or destination writes; non-DuckDB destinations fail before package or destination writes; existing package directories are refused; discovered-schema resources fail before destination/checkpoint writes; and `--loop` remains unsupported.
- A checkpoint-failure or injected post-receipt failure test proves the package/DuckDB receipt window remains recoverable and no checkpoint head is committed prematurely. If this requires a lower-layer hook, keep it narrow and test-only or reuse the existing prepared replay hook.
- Run, at minimum:
  - `cargo test -p cdf-engine -p cdf-project -p cdf-cli --locked --no-fail-fast`
  - `cargo clippy -p cdf-engine -p cdf-project -p cdf-cli --all-targets --locked -- -D warnings`
  - `cargo nextest run -p cdf-engine -p cdf-project -p cdf-cli --locked`
  - `cargo fmt --all -- --check`
  - `cargo metadata --locked --format-version 1`
  - `git diff --check`
- If manifests or `Cargo.lock` change, run applicable `QUALITY.md` supply-chain/security gates in parallel where possible, including `cargo deny`, `cargo audit`, OSV, `cargo vet`, `cargo machete`, `cargo udeps` when available, Semgrep, gitleaks, direct first-party unsafe scan, and `tools/codeql-rust-quality.sh` using the reusable database.
- Run bounded mutation testing over the new live-run orchestration path if feasible; otherwise record the exact limit and compensate with negative tests around commit-gate ordering and no-write failures.

## Explicit exclusions

No REST or SQL resource execution, no Postgres/Parquet/object-store run execution, no multi-resource run, no run ledger, no `inspect run`, no `resume`, no `replay package` CLI command, no automatic package id/checkpoint id/run id generation, no default pipeline or target inference, no package GC, no contract freeze/test implementation, no backfill planner, no loop/streaming supervisor, no native Arrow/DataFusion Parquet policy change, and no broad advisory ignore.

## Assumption provenance

- Record-backed: the commit-gate invariant, receipt-gated checkpoint commit, DuckDB receipt verification, and SQLite checkpoint behavior are governed by `.10x/specs/checkpoint-state-commit-gate.md`, `.10x/specs/destination-receipts-guarantees.md`, and `.10x/tickets/done/2026-07-06-package-replay-commit-gate-runtime.md`.
- Record-backed: declarative local file resource runtime support is closed in `.10x/tickets/done/2026-07-06-declarative-file-preview-execution.md`.
- Record-backed: `run` is currently blocked on an invariant-preserving orchestration API in `.10x/tickets/done/2026-07-05-cli-surface.md`; this ticket narrows that blocker to the first explicit local DuckDB slice.
- Source-observed but not generalized: local tests already use `pipeline-1` and target names such as `events`, but this ticket does not ratify any project-wide default for them. They must be explicit command inputs in this slice.

## References

- `VISION.md` Chapters 11, 12, 13, 17, 19, and 22.
- `.10x/specs/package-lifecycle-determinism.md`
- `.10x/specs/checkpoint-state-commit-gate.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/specs/project-cli-observability-security.md`
- `.10x/tickets/done/2026-07-05-cli-surface.md`
- `.10x/tickets/2026-07-05-conformance-chaos-golden.md`
- `.10x/tickets/done/2026-07-06-package-replay-commit-gate-runtime.md`
- `.10x/tickets/done/2026-07-06-declarative-file-preview-execution.md`
- `.10x/knowledge/rust-crate-organization.md`
- `.10x/knowledge/quality-gate-execution.md`

## Progress and notes

- 2026-07-06: Opened after the declarative file preview slice closed. This is the smallest live-run bridge from a real source to a real destination and checkpoint while avoiding unratified run-ledger defaults.
- 2026-07-06: Marked active for worker implementation. Worker owns the scoped engine/project/CLI implementation and focused tests; parent owns integration review, quality evidence, ticket closure, parent graph updates, and commit.
- 2026-07-06: Worker implementation complete for the explicit local file resource to local DuckDB/SQLite slice. Added semver-additive engine segment source-position reporting, project run orchestration, bounded CLI run wiring, and focused positive/negative tests. Focused fmt, test, clippy, nextest, metadata, and diff whitespace checks pass.
- 2026-07-06: Parent review hardened the slice: CLI JSON now reports the explicit package id, path-like package ids fail before writes, non-file resource and plan/package mismatches fail before writes, and divergent engine/package segment source positions fail closed.
- 2026-07-06: Bounded mutation testing over `crates/cdf-project/src/runtime.rs` initially found 3 missed mutants. After targeted tests, rerun result was 45 mutants tested, 36 caught, 9 unviable, 0 missed.
- 2026-07-06: Closed with evidence in `.10x/evidence/2026-07-06-local-file-run-duckdb-checkpoint.md` and pass review in `.10x/reviews/2026-07-06-local-file-run-duckdb-checkpoint-review.md`.

## Blockers

None for the explicit local file resource to DuckDB/SQLite slice.
