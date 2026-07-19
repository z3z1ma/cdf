Status: done
Created: 2026-07-11
Updated: 2026-07-19
Parent: .10x/tickets/done/2026-07-11-p3-ws-v-vectorized-validation.md
Depends-On: .10x/tickets/done/2026-07-11-p3-v2-validation-graph-integration.md, .10x/tickets/done/2026-07-11-p3-c4-jobs-invariance-scaling-matrix.md

# P3 V3: validation envelope and conformance closeout

## Scope

Run the complete hot-rule/type/density/batch matrix, enforce ≥1 GB/s/core, publish roofline/profile/allocation evidence, make regression gates permanent in the appropriate benchmark tier, and close WS-V.

## Acceptance criteria

- Ratified 64k matrix reaches target without counting uninspected bytes.
- Vector/scalar property/fuzz and end-to-end semantics remain green.
- Kernel, evidence materialization, and end-to-end costs are separately visible.
- Regression gate is variance-aware and absent from fast checks.

## Evidence expectations

Raw host reports/profiles/counters, roofline ratios, correctness corpus, generated envelope, CI tier proof, and adversarial workload/performance review.

## Explicit exclusions

No target weakening without a superseding decision.

## Blockers

None. V2 and C4 are complete.

## Journal

- 2026-07-19: Activated after F1 closure because V3 is dependency-ready and converts an already-proven production hot path into a permanent performance law without changing runtime semantics. The existing ignored `cdf-contract` tests prove a 64k mixed kernel and scalar ratio but do not represent the ratified batch/type/density matrix, emit machine-readable host evidence, or enforce a variance-aware threshold. The implementation slice will live in `cdf-benchmarks`, keep timing out of fast checks, count only bytes each rule actually inspects, and retain boundary/evidence-materialization cells as visible non-throughput claims rather than inflating the ≥1 GB/s/core kernel gate.
- 2026-07-19: Self-review moved the matrix runner from the full `cdf-benchmarks` lab binary into the existing slim `cdf-bench-measure` executable. The envelope needs Arrow, `cdf-contract`, and host fingerprinting only; retaining it in `cdf-p3-lab` forced a fat-LTO relink of DuckDB, DataFusion, every source, and every destination for an unrelated kernel gate. The slim placement preserves one benchmark authority while keeping future V3 iteration out of the destination/source build graph.
- 2026-07-19: The dedicated EC2 run pinned execution to one core and measured seven samples over 33,554,432 target rows per sample. All 12 gated 64k hot-kernel cells passed; the slowest was 3.016 GB/s and the fastest 7.254 GB/s against a 16.689 GB/s memcpy roofline. The 45 remaining boundary, smaller-batch, and selected-evidence cells are trend-only by construction.
- 2026-07-19: The current full-year TLC product report separately records 11.869 GB through validation/normalization in 262.97 ms, approximately 45.1 GB/s for the concurrent product phase. This preserves end-to-end visibility without conflating that multi-stage result with the isolated single-core gate.
- 2026-07-19: `perf` is unavailable on the EC2 image. The failed capability probe is retained; closure relies on exact inspected bytes, mask/evidence counters, seven-sample median/MAD, host fingerprint, memcpy roofline, and the separate product report. No production runtime code changed.

## Evidence

- **64k matrix reaches target without uninspected bytes:** `.10x/evidence/2026-07-19-p3-v3-validation-envelope.md` maps the raw EC2 report's exact inspected-byte authority and all 12 passing cells. Unreferenced columns are excluded and trend-only cells cannot satisfy the gate.
- **Vector/scalar and end-to-end semantics remain green:** `cargo test -p cdf-contract --lib --locked -j 12` passed 90 tests, including scalar differential and vector boundary/property cases. V2's production architecture and macro evidence remain applicable because V3 changed only benchmark/workflow code.
- **Costs separately visible:** the raw report separates `kernel_masks` from `selected_evidence`; the current TLC report records the full product `validation_normalization` phase.
- **Variance-aware permanent gate outside fast checks:** the scheduled performance workflow uses median-of-N plus a two-MAD inconclusive band and requires exactly 12 passed cells. The command is absent from fast checks.

## Review

Verdict: pass.

Fresh adversarial review confirmed that fixture construction and plan binding are outside timing, a warm-up evaluation is discarded before timing, only value/offset/validity buffers actually inspected are counted, terminal string offsets are included, the gate cannot pass with missing cells, and selected evidence is never credited toward kernel throughput. The runner's slim dependency graph avoids a recurring multi-minute unrelated link. No critical, significant, or minor correctness/performance finding remains.

Residual risk: hardware performance counters and a general heap allocator trace are unavailable on this host. Structural output accounting and RSS remain recorded, and no product-path change depends on an inferred allocation result.

## Retrospective

The principal friction was build-graph placement, not kernel speed. A benchmark requiring only Arrow and contract kernels initially inherited DuckDB, DataFusion, every codec, and every destination through the monolithic lab executable. Moving measurement primitives into the existing slim runner cut clean iteration by roughly 9x while preserving one host/evidence schema. Future performance gates should begin from the smallest graph that contains the measured authority; broad roofline suites should compose their reports rather than force every cell through one binary.

## References

- `.10x/specs/vectorized-contract-validation.md`
