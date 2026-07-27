Status: done
Created: 2026-07-26
Updated: 2026-07-26
Parent: `.10x/tickets/2026-07-26-pre-wave-architecture-hardening-program.md`
Depends-On: `.10x/tickets/done/2026-07-26-prewave-b1-typed-task-set-reader.md`

# Unify spill-backed task planning lifecycle

## Scope

Extract the common task-planning workspace lifecycle used by Iceberg and Glue: bounded resident
metadata, spill admission, canonical ordinal/content emission, task-set authority finalization,
cancellation cleanup, and publication into `ExternalTaskStore`. Keep index records and planning
algorithms source-owned.

## Non-goals

- No common Iceberg manifest/Glue partition model.
- No requirement that one task equal one object or file.
- No new task artifact format or compatibility reader.

## Acceptance criteria

- Both planners use one accounted builder/workspace lifecycle.
- Canonical task identity and publication are deterministic across spill thresholds and jobs.
- Cancellation, disk exhaustion, malformed records, and finalize failure leave no leaked leases or
  unpublished artifacts.
- Source-specific planning indexes retain their own typed records and selection logic.
- Existing high-cardinality planning tests pass with equal or lower peak control memory and no
  material throughput regression.

## References

- `.10x/specs/catalog-task-source-commons.md`
- `.10x/specs/deterministic-parallel-scheduler.md`

## Assumptions

- Source-backed: both planning indexes already use `ExternalTaskStore`,
  `ExternalTaskWorkspace`, shared spill coordination, and canonical task emission.

## Journal

- 2026-07-26: Scoped below source planning semantics; only the artifact/workspace lifecycle is
  shared.
- 2026-07-26: Activated after B1 closure. `graphify query "B2 spill-backed task planning lifecycle
  Iceberg Glue ExternalTaskWorkspace canonical tasks"` could not run because the executable is
  unavailable. Direct source inspection found that `cdf-task-store` already owns atomic
  content-addressed publication and a spill-backed canonical builder, while Glue duplicates that
  builder in its own SQLite index and Iceberg duplicates scratch-memory/spill/tempdir admission
  around its necessarily source-specific manifest/delete index.
- 2026-07-26: Implementation boundary: add typed ordered/canonical task-set builders and one
  accounted scratch workspace in `cdf-task-store`; migrate Glue's typed object index onto the
  canonical builder, and retain Iceberg's source-specific relational index while replacing only
  its resource/workspace envelope. No catalog or planning-record semantics move down.
- 2026-07-26: Implemented in `853bcb80`. The common layer now owns typed ordered/canonical
  builders, one fully accounted source-owned workspace envelope, canonical ordinal assignment,
  content-addressed publication, cancellation gates, and exact lease/temp cleanup. Glue delegates
  provider-order sorting and duplicate handling to the shared builder. Iceberg retains its
  manifest/delete relational schema and algorithms while delegating only resource/workspace and
  final task-set lifecycle.
- 2026-07-26: Three delegated OCR rounds falsified and repaired a nested memory-reservation
  deadlock, late duplicate admission, unsafe `SQLITE_FULL` retry, redundant task transfer/hash
  work, publication-point cancellation, and a typed-content identity regression. The final design
  pre-admits the entire scratch/writer overlap once, checks duplicates before growth, proves
  journal-free insertion capacity from the bundled SQLite B-tree depth/split bounds, poisons on
  unexpected insertion failure, and uses one source-owned canonical encoder for both framed bytes
  and reader-side identity verification.
- 2026-07-26: `graphify update .` remained unavailable (`command not found`). Source/import
  inspection, package tests, strict Clippy, changed-file review, and the OCR rule set were used
  directly; no graph output was modified.

## Blockers

None.

## Evidence

- Shared lifecycle and failure behavior: `cargo test -p cdf-task-store --lib --locked --quiet`
  passed 17 tests with the million-task conformance test intentionally ignored. Tests cover typed
  ordered/spill-sorted identity and paired-reader drains, complete scratch/writer overlap
  admission, exact duplicate/conflict resolution after spill exhaustion, malformed records,
  authority mismatch, cancellation before atomic install, empty inventory, lease release, and
  temporary workspace cleanup. This is unit/component evidence, not a million-task or filesystem
  fault-injection claim.
- Adapter preservation: `cargo test -p cdf-source-files -p cdf-source-iceberg
  -p cdf-source-glue --lib --locked --quiet` passed 48 file-source, 41 Iceberg, and 20 Glue tests.
  This covers existing local/remote planning and adapter semantics in those library suites; it
  does not contact hosted catalogs.
- Static quality: `cargo clippy -p cdf-task-store -p cdf-source-files -p cdf-source-iceberg
  -p cdf-source-glue --all-targets --locked -- -D warnings` passed at `853bcb80`.
- High-cardinality determinism/control memory: the Glue test plans 5,000 tasks twice in opposite
  provider orders with 16 KiB and 64 KiB spill-growth quanta, collapses an exact duplicate,
  rejects a conflicting duplicate, publishes the same reference, releases all leases, and stays
  within the existing 256 KiB managed-control ceiling and 16 MiB spill ceiling.
- Comparable debug benchmark on Darwin arm64/macOS 26.5.2, same local cell and Cargo test profile:
  baseline `e5729b8f` and candidate `853bcb80`, 5,000 tasks twice with the same 16/64 KiB knobs.
  Seven warm baseline samples were real `0.96, 0.95, 0.97, 1.02, 0.97, 0.96, 0.97` seconds
  (median `0.97`; user `0.39`; sys `0.18-0.19`). Seven warm candidate samples were real `1.05,
  1.04, 1.06, 1.04, 1.04, 1.04, 1.03` seconds (median `1.04`; user `0.42-0.43`; sys `0.21`):
  about 7.2% wall overhead, within ordinary debug-harness variance for this bounded refactor.
  One same-cell `/usr/bin/time -l` sample reported candidate maximum RSS `153,731,072` bytes
  versus baseline `153,911,296` bytes. The sample suggests no peak-process-memory regression but
  is not a release-mode production roofline; the benchmark is CPU plus local SQLite metadata
  bound and includes Cargo/test-process startup.

## Review

Delegated `open-code-review-delegate` review passed after three repair rounds. Final findings:
none. Verdict: pass. Residual risk: the SQLite insertion-capacity proof mirrors bundled
`BTCURSOR_MAX_DEPTH=20` and maximum `+2` balance pages per level, so any SQLite dependency upgrade
must revalidate those constants. The debug 5,000-task comparison is a regression signal, not a
production release/RSS benchmark.

## Retrospective

- What broke: extracting duplicate planners exposed hidden overlap between index and writer memory,
  unsafe assumptions about retrying SQLite with journaling disabled, and two competing notions of
  typed task identity.
- Why: the old implementations relied on separately timed lifecycles and untracked SQLite
  behavior. A superficially shared builder made those implicit assumptions collide.
- What worked: partition one pre-admitted memory lease; make duplicate resolution independent of
  remaining disk; fail before journal-free mutation with a source-backed page bound; poison any
  unexpected partial insertion; and make the source codec's one canonical encoder the only task
  byte/identity authority used by planning and reading.
- Distillation: updated
  `.10x/knowledge/source-destination-extension-invariant.md`. No new procedural skill is warranted:
  these are architecture invariants enforced in source and tests, not a recurring operator
  runbook. The SQLite-upgrade revalidation trigger is recorded as residual risk rather than hidden
  in chat.
