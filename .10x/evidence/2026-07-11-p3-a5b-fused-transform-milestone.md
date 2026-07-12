Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-a5b-fused-transform-kernel.md

# P3 A5b first fused transform milestone

## What was observed

The production default now selects a fused no-residual-candidate contract path. The unfused implementation remains callable only through `EngineExecutionOptions::with_unfused_transform_for_conformance`; this control is not serialized and cannot change plan/package authority.

The fused path calls the same vector `evaluate_record_batch` program and materializes accepted rows with the same Arrow bitmap only when violations occur. It removes the residual reference path's unconditional per-row acceptance/variant arrays and map construction when the batch has no residual candidates.

## Procedure and results

- `cargo test -p cdf-engine --lib` — 82 passed, three existing explicit stress/performance tests ignored.
- `cargo test -p cdf-engine fused_and_unfused_transform_modes_produce_identical_packages -- --nocapture` — passed. Engine output, package identity/hash/signature, segments, quarantine artifact bytes, lineage, positions, and evidence were equal; both package readers verified.
- `CDF_A5_FUSION_BENCH_ITERATIONS=200 cargo test --release -p cdf-engine fused_transform_hot_path_benchmark -- --ignored --nocapture` — 64k rows per iteration; unfused 1.426 GiB/s; fused 3.912 GiB/s; 2.743x speedup.
- `cargo clippy -p cdf-engine --all-targets -- -D warnings` — passed.

## What this supports

- The fused hot path materially reduces framework overhead while retaining vector contract semantics.
- Fusion choice is an implementation control, not package identity or business semantics.
- The optimized accepted-row path performs no scalar row reconstruction.

## Limits

This milestone specializes the overwhelmingly common no-residual-candidate case. Batches containing actual residual candidates still execute the unfused semantic reference. Output/scratch reservations before allocation and bounded detailed-evidence persistence remain required for A5b closure.
