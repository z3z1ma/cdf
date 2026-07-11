Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-performance-investigation-backlog.md

# Triage package IO, hashing, and manifest overhead

## Scope

Investigate the performance cost of CDF's deterministic package boundary: Arrow IPC segment writes, canonical JSON artifacts, manifest collection, SHA-256 hashing, atomic status updates, directory syncs, trace writes, archive metadata, and verification.

This ticket is triage only. It does not authorize changing package identity, weakening hashes, removing fsync/atomicity behavior, changing canonical formats, or altering package layout.

## Current hypothesis

CDF intentionally treats packages as durable evidence, not disposable staging. That can make CDF slower than direct in-memory DataFusion, direct DuckDB, or Polars for jobs where durable package evidence is not needed. The question is not whether the overhead exists; it is how large it is, where it concentrates, and whether there are safe optimizations that preserve the active package lifecycle spec.

## Investigation questions

- What proportion of end-to-end runtime is spent writing Arrow IPC segments versus canonical JSON artifacts versus hashing versus directory sync/status updates?
- Does package overhead dominate small jobs, medium jobs, wide schemas, or many-small-segment workloads?
- Does manifest finalization re-read/hash more data than necessary?
- Are status updates and trace append syncs correctly safe but too frequent for high-throughput runs?
- Are there opportunities for incremental hashing, batched metadata writes, fewer syncs, or segment coalescing without changing identity semantics?
- Should performance docs explicitly distinguish "governed package mode" from future "ephemeral preview/query mode"?

## Candidate measurement scenarios

- Empty/tiny package with only metadata artifacts.
- One segment with small row count, to capture startup and metadata overhead.
- One large segment, to capture IPC write and hash throughput.
- Many small segments, to capture manifest and filesystem overhead.
- Package verification after write, to measure readback/hash cost.
- Package archive sidecar creation, if relevant to Parquet overhead.

## Acceptance criteria

- Produce a cost breakdown of package creation and finalization for at least representative small and medium local fixtures, or record why such measurement requires a separate harness first.
- Identify whether overhead is dominated by data bytes, number of files, number of segments, status/trace syncs, hashing, or serialization.
- Classify optimizations as `safe under current spec`, `requires spec/decision change`, `not worth it`, or `blocked by missing measurement`.
- If implementation is recommended, open separate tickets for specific changes such as incremental manifest hashing, sync batching, segment coalescing, or trace buffering.
- Preserve the active invariant that package identity remains deterministic and receipt/checkpoint evidence remains verifiable.

## Evidence expectations

- Inspection of `crates/cdf-package/src/builder.rs`, `crates/cdf-package/src/storage.rs`, package verification/read paths, and engine package writing.
- Measurement with `time`, tracing spans, or profiling tools only after selecting a deterministic fixture.
- Explicit note on filesystem dependence; APFS/local SSD behavior may not generalize to object storage or network filesystems.

## Explicit exclusions

No package format change, no hash weakening, no removal of atomic writes, no removal of directory syncs, no lifecycle-status semantic change, no package identity change, no archive behavior change, and no implementation of an ephemeral execution mode.

## References

- `.10x/tickets/2026-07-07-performance-investigation-backlog.md`
- `.10x/specs/package-lifecycle-determinism.md`
- `.10x/decisions/package-state-commit-preimage-artifacts.md`
- `crates/cdf-package/**`
- `crates/cdf-engine/src/execution.rs`

## Progress and notes

- 2026-07-07: Opened from performance discussion. The package boundary is likely CDF's most intentional overhead versus raw DataFusion/DuckDB/Polars, so it needs quantified tradeoff notes before optimization work.
- 2026-07-11: P3 audit confirmed immediate segment rereads plus final rereads and high-frequency durability work. E1–E4 own hash-while-write receipts, streaming manifest/durability, verification/replay I/O, and the ≤5%/roofline closeout. This triage owns no implementation and remains open until E4 records the measured disposition.
- 2026-07-11: WS-L measured package build at 0.235 MiB/s median and retained a real sample profile whose dominant captured subtree is segment persistence with file/directory synchronization. See `.10x/evidence/2026-07-11-p3-l5-preoptimization-baseline.md`; E1-E4 own the isolated before/after and ≤5% closeout.

## Blockers

None for investigation. Any change to package identity, lifecycle semantics, or crash safety is blocked on explicit spec/decision work.
