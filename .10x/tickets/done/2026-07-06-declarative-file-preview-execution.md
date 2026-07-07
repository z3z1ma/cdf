Status: done
Created: 2026-07-06
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-cli-surface.md
Depends-On: .10x/tickets/done/2026-07-05-project-format-lockfile-secrets.md, .10x/tickets/done/2026-07-05-declarative-resources.md, .10x/tickets/done/2026-07-05-datafusion-engine-planner.md, .10x/tickets/done/2026-07-06-resource-execution-conformance-file-sources.md

# Implement declarative file preview execution

## Scope

Implement the first supported `cdf preview` execution path for Tier 0 declarative file resources by connecting compiled `kind = "files"` resources to the existing `cdf-formats::FileResource` runtime.

Owns the smallest necessary changes in:

- `crates/cdf-declarative/**` for lower-layer `ResourceStream::open` support or a focused file-resource runtime adapter.
- `crates/cdf-project/**` only if project-root-relative file resolution must become explicit before declarative resources can safely open local files.
- `crates/cdf-cli/**` only for preview command tests and any command plumbing needed to consume the lower-layer runtime.
- Cargo manifests and `Cargo.lock` only for required internal crate wiring.

Keep crate roots thin. Do not grow monolithic `lib.rs` files; follow `.10x/knowledge/rust-crate-organization.md`.

## Acceptance criteria

- A declarative `kind = "files"` resource with exactly one matching local file can be opened through the lower-layer resource runtime used by `cdf preview`.
- `cdf preview <RESOURCE>` for a declarative local file resource succeeds for at least NDJSON, CSV, JSON, and Parquet file formats already supported by `cdf-formats::FileResource`.
- Preview drains at most one batch, reports the previewed resource id, batch id, partition id, row count, byte count, and write effects, and creates no package root, destination database, or checkpoint state.
- File paths are resolved under the project root or declared file-source root; relative-path behavior is explicit in code and tests.
- Zero matching files fail closed with an actionable error and no writes.
- Multiple matching files fail closed with an actionable error and no arbitrary file choice. Multi-file scan semantics remain out of scope until a later ticket ratifies them.
- Non-file declarative resources continue to return explicit unsupported preview/runtime errors until their lower-layer execution paths exist.
- Existing `plan` and `explain` behavior remains unchanged except where preview can now use the planned file partition.
- The implementation does not add native arrow-rs `parquet`/`paste`, does not change the current advisory policy, and does not alter the DuckDB-backed Parquet implementation.

## Evidence expectations

- Focused CLI preview tests prove success for a single matching file and no package/destination/checkpoint writes.
- Focused negative tests prove zero-match and multi-match file globs fail closed without writes.
- Existing file-source conformance and format tests remain passing.
- Run, at minimum:
  - `cargo test -p cdf-declarative -p cdf-project -p cdf-cli --locked --no-fail-fast`
  - `cargo clippy -p cdf-declarative -p cdf-project -p cdf-cli --all-targets --locked -- -D warnings`
  - `cargo nextest run -p cdf-declarative -p cdf-project -p cdf-cli --locked`
  - `cargo fmt --all -- --check`
  - `cargo metadata --locked --format-version 1`
  - `git diff --check`
- If manifests or `Cargo.lock` change, also run the applicable supply-chain gates from `QUALITY.md` in parallel where possible, including `cargo deny`, `cargo audit`, OSV, `cargo vet`, `cargo machete`, `cargo udeps` when available, Semgrep, gitleaks, direct first-party unsafe scan, and `tools/codeql-rust-quality.sh` using the reusable CodeQL database.
- Run bounded mutation testing over the new file-preview adapter or CLI preview path if feasible; otherwise record the exact limit and compensate with negative tests that exercise path resolution and glob cardinality.

## Explicit exclusions

No `cdf run`, no package creation, no destination commits, no checkpoint advancement, no run/resume/replay orchestration, no HTTP or SQL resource execution, no multi-file scan semantics, no project scaffold/init behavior, no native Arrow/DataFusion Parquet policy change, and no broad advisory ignore.

## References

- `VISION.md` Chapters 7, 8, and 17.
- `.10x/specs/resource-authoring-planning-batches.md`
- `.10x/specs/project-cli-observability-security.md`
- `.10x/tickets/2026-07-05-cli-surface.md`
- `.10x/tickets/done/2026-07-06-resource-execution-conformance-file-sources.md`
- `.10x/knowledge/rust-crate-organization.md`
- `.10x/knowledge/quality-gate-execution.md`

## Progress and notes

- 2026-07-06: Opened after file-source execution conformance made `cdf-formats::FileResource` available and after inspecting `cdf preview`, `CompiledResource::open`, project declarative resource loading, and current CLI preview tests. This child narrows the broad CLI preview blocker to single-match declarative local file resources and intentionally fails closed for zero or multiple glob matches.
- 2026-07-06: Marked active for worker implementation. Worker owns the scoped lower-layer file-resource preview runtime and focused CLI tests; parent owns graph coherence, final evidence, review, closure, and commit.
- 2026-07-06: Worker implemented the lower-layer file runtime adapter and project-root compile path. Parent review hardened partition validation, glob/path traversal, symlink-directory handling, zero/multi-match failures, and CLI preview no-write coverage.
- 2026-07-06: Closed with evidence in `.10x/evidence/2026-07-06-declarative-file-preview-execution.md` and review in `.10x/reviews/2026-07-06-declarative-file-preview-execution-review.md`. Final checks passed, including mutation testing over `crates/cdf-declarative/src/file_runtime.rs`.

## Blockers

None for the single-match declarative local file preview slice. At closure time, native Arrow/DataFusion Parquet policy remained separately blocked in `.10x/tickets/done/2026-07-06-native-arrow-parquet-policy.md` and was not required for this ticket. It was later ratified by `.10x/decisions/native-arrow-datafusion-parquet-policy.md`.
