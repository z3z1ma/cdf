Status: done
Created: 2026-07-26
Updated: 2026-07-26
Parent: `.10x/tickets/2026-07-26-pre-wave-architecture-hardening-program.md`

# Modularize the file-source runtime

## Scope

Split `cdf-source-files/src/runtime.rs` along existing ownership boundaries—discovery,
task authority, planning/inventory, input preparation/spooling/cache, decode/validation, and
glob/format/compression resolution—without changing public behavior or inserting compatibility
re-exports.

## Non-goals

- No new source abstraction, transport policy, format behavior, or performance knob.
- No arbitrary line-count split or cyclic internal module graph.
- No change to file inventory, plan, schema, task, package, or receipt identity.

## Acceptance criteria

- The 9,138-line monolith is replaced by cohesive internal modules with one-way dependencies.
- Public exports and `FileSourceDriver` construction remain minimal and documented.
- No module becomes a miscellaneous helper bucket or exceeds the monolith's concern count.
- Local/HTTP/object-store, discovery/pinned, multi-file, compression, preview/run, retry, and
  payload-cache conformance pass unchanged.
- Focused TLC/FineWeb performance evidence remains within ordinary variance of the current floor.

## References

- `.10x/specs/data-onramp-file-sources-transports.md`
- `.10x/decisions/native-format-driver-and-byte-source-boundary.md`
- `.10x/knowledge/source-destination-extension-invariant.md`

## Assumptions

- Source-backed: the monolith already has separable type/function clusters and existing lower
  transport/format/task-store boundaries.

## Journal

- 2026-07-26: Function/type inventory identified six existing concerns; this ticket is a
  behavior-preserving topology change.
- 2026-07-26: Activated after B2 closure. `graphify query "B3 modularize cdf-source-files runtime
  discovery task authority planning inventory spooling cache decode validation glob format
  compression"` could not run because the executable is unavailable. Direct inventory found 9,139
  lines: 252 lines of dependencies/public configuration, discovery at lines 253-862, task
  authority/readers at 902-1201, input/spool/cache/decode at 1948-4062, validation at 4063-4687,
  glob/format/compression resolution at 4688-5932, and tests thereafter. The move will follow
  those existing seams and preserve the root public surface.
- 2026-07-26: The first mechanical split compiled and passed all 48 library tests, but delegated
  review correctly rejected it: parent and child glob imports preserved real
  input/resolution/task/validation cycles and left input as a residual planning/decode bucket.
  Reworked the topology around a leaf file model, resolution-owned contexts/sink, dedicated
  planning and decode modules, and explicit production imports. `input.rs` now owns only
  preparation/spool/cache lifecycle. Removed the temporary crate-root task re-export and made the
  four pre-existing discovery exports explicit.
- 2026-07-26: Final source gates passed after the directed split:
  `cargo test -p cdf-source-files --lib --locked --quiet` passed 48/48 and
  `cargo clippy -p cdf-source-files --all-targets --locked -- -D warnings` passed. The required
  post-change `graphify update .` could not run because `graphify` remains unavailable.
- 2026-07-26: Preserved the exact pre-B3 release CLI from commit `e7b56b06`, then built the
  candidate from `e7b56b06+dirty` with Rust 1.96.1, fat LTO, native CPU, and the repository's
  downloaded-prebuilt DuckDB linkage. An initial build without `DUCKDB_DOWNLOAD_LIB=1` failed at
  final link with missing `-lduckdb`; the corrected documented build succeeded. Interleaved
  three-sample clean-state local cells loaded exact 3.0M-row TLC and 1.1M-row FineWeb outputs.
  TLC median moved from 1.35s to 1.28s (-5.2%) and FineWeb from 13.03s to 13.58s (+4.2%);
  median maximum RSS moved from 973,570,048 to 959,332,352 bytes and from 5,959,794,688 to
  5,929,664,512 bytes respectively. Both are within the existing 10% ordinary-variance gate.
- 2026-07-26: A broader 216-test `cdf-project` integration run reached the final replay case after
  reporting one HTTP jobs-invariance failure, then was stopped when that replay case remained
  silent beyond three minutes. The HTTP test failed identically and at the same assertion from an
  isolated worktree at exact pre-B3 commit `e7b56b06`; five candidate repetitions also reproduced
  it. This establishes a pre-existing scheduling-sensitive assertion rather than a modularization
  regression. The long replay test and full-suite completion remain outside B3's behavior-neutral
  source boundary and are not represented as passing evidence.

## Blockers

None.

## Evidence

- Cohesive topology: production file sizes are facade/resource `runtime.rs` 1,023 lines,
  `model.rs` 101, `task.rs` 307, `planning.rs` 362, `decode.rs` 444, `discovery.rs` 632,
  `validation.rs` 651, `input.rs` 1,298, and `resolution.rs` 1,320. The explicit production graph
  is acyclic: model is a leaf; resolution depends on model; validation depends on
  model/resolution; input depends on model; decode depends on input/model/validation; task depends
  on model/resolution/validation; planning composes decode/input/resolution/task/validation;
  discovery depends on input/resolution; the facade composes planning/resolution/task/validation
  and explicitly exports only discovery's four established APIs.
- Public construction: `FileSourceDriver` and `FileSourceDriver::new` now document the injected
  codec catalog/runtime factory boundary and contact-free construction. No compatibility wrapper
  or crate-root task re-export remains.
- Behavior/conformance: the final 48-test file-source library run covers registered local
  Parquet/CSV/JSON/NDJSON/fixed-width, HTTP templates, remote schemes, object-store recursive
  multi-file and gzip, discovery budget/retained handoff, pinned generation validation,
  projection/predicate decode, streaming/backpressure, cache disabled/hit/miss, and
  format/transform extensibility. Strict all-target Clippy additionally proves every production
  module and test target compiles through the explicit imports with the workspace safety walls.
  These checks prove their assertions, not every external provider or public-network retry.
- Performance: Apple M5 Pro, macOS 26.5.2, warm local APFS, release/fat-LTO/native CPU, jobs=2,
  three interleaved clean-state samples per binary. TLC used the 49,961,641-byte January 2024
  public fixture cached once before timing and DuckDB destination; FineWeb used the existing
  2,147,509,487-byte local fixture and Parquet destination. Baseline/candidate medians were
  1.35/1.28s TLC and 13.03/13.58s FineWeb; median maximum RSS was
  973,570,048/959,332,352 bytes and 5,959,794,688/5,929,664,512 bytes. Both binaries used the same
  SHA-256-identical downloaded `libduckdb.dylib`; the baseline binary was exact committed
  `e7b56b06`, while the candidate label is `e7b56b06+dirty` until this implementation commit.
  Logs are in ignored `target/b3-perf/{tlc,fineweb}-samples.txt`. This is same-host warm local
  evidence, not a replacement for controlled EC2 full-year TLC or public-network FineWeb floors.
- Integration limit: `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-project --lib --locked --quiet`
  executed 216 tests but did not complete because the final Parquet artifact replay case remained
  silent beyond three minutes. Its only reported failure,
  `tests::recorded_http_multifile_packages_are_jobs_invariant`, was reproduced unchanged at
  pre-B3 commit `e7b56b06` in an isolated worktree, failing the same
  `parallel_progress.peak_active_streams >= 2` timing assertion. This evidence excludes the
  failure from B3 causality; it does not claim the broader suite passes.

## Review

- Delegated OCR round 1: fail. High finding: sibling/parent glob imports preserved actual cyclic
  dependencies and compiler-enforced no seam. Medium findings: `input.rs` remained a planning and
  decode bucket; public discovery/task exposure and `FileSourceDriver` construction were not
  minimal/documented.
- Repairs: introduced leaf model, planning, and decode owners; moved the match sink and resolution
  contexts to resolution; removed all production `use super::*`; made the public discovery facade
  explicit; removed the crate-root task re-export; documented driver construction.
- Delegated OCR round 2: pass with no findings. The reviewer confirmed the production graph is
  explicit and acyclic, module concerns are cohesive, public exposure is narrow, and no behavior,
  identity, cleanup, accounting, visibility, or test-coverage defect was found. Residual
  performance risk was discharged by the matched TLC/FineWeb cells above.

## Retrospective

- What broke: line-oriented movement produced compilable files but did not create architectural
  boundaries; wildcard imports let every sibling retain monolith-wide reach.
- Why: the first pass treated lexical clustering as sufficient and used the compiler only for
  visibility repair, not dependency-direction enforcement.
- What worked: the reviewer falsified the claimed seam before commit. Extracting the shared model
  and the orchestration layer first made the remaining edges orient naturally; explicit imports
  then turned the intended DAG into compiler-visible structure.
- Durable lesson: a monolith split is complete only when production imports express an acyclic
  ownership graph. File count and line count are diagnostics, never evidence of modularity.
- Distillation: update `.10x/knowledge/source-destination-extension-invariant.md` with the
  compiler-visible module-DAG rule. No new skill is warranted: the repair is architectural
  judgment captured as an invariant, not a repeatable operational procedure with independent
  validation steps.
