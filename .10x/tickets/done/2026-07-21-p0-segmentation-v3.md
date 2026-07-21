Status: done
Created: 2026-07-21
Updated: 2026-07-21
Parent: .10x/tickets/done/2026-07-21-p0-iceberg-execution-robustness.md
Depends-On: .10x/specs/canonical-segmentation-adaptive-batching.md, .10x/decisions/byte-first-canonical-segmentation-v3.md

# P0: canonical segmentation v3 performance default and knobs

## Scope

Replace the TLC-specific 32/64 MiB canonical segment default with the ratified v3 policy, expose
every identity-bearing segment/microbatch bound as an operator knob, and measure the exact wide
Iceberg workload that falsified v2.

## Non-goals

No runtime-adaptive canonical boundaries, artifact-format change, cross-partition segment, or
destination-specific segmentation branch.

## Acceptance Criteria

- The plan records and validates the v3 256 MiB target/maximum and v3 segment namespace.
- Plan, explain, preview, run, and backfill expose target/maximum rows and bytes plus microbatch
  minimum/maximum rows and bytes.
- Source rechunking and jobs invariance remain green.
- The exact release FQ12 run records wall time, peak RSS, segment count, and phase evidence against
  the retained v2/O(columns squared) baseline.
- Strict formatting, clippy, aggregate tests, and adversarial review pass before closure.

## References

- `.10x/specs/canonical-segmentation-adaptive-batching.md`
- `.10x/decisions/byte-first-canonical-segmentation-v3.md`
- `.10x/tickets/done/2026-07-11-p3-a12-byte-first-segments-shared-arrow-accounting.md`

## Assumptions

- User-ratified 2026-07-21: materially increase the default segment size immediately and expose all
  constants that deserve knobs.
- Record-backed: canonical boundaries remain identity-bearing plan data; only microbatch execution
  may adapt to live pressure.

## Journal

- 2026-07-21: Opened from the exact 2,052-column/1,188-segment FQ12 observation. Implemented v3 as
  a source/destination-neutral engine policy and threaded it through plan construction rather than
  adding an Iceberg special case.

## Blockers

None.

## Evidence

- `/tmp/cdf-iceberg-v3-smoke.log`: optimized release, default configuration, exact FQ12
  `flolake.transactions` workload (3,513,266 rows, 2,052 columns, 84 source tasks) committed to the
  Parquet destination in 36.94 seconds with 1,181,106,176-byte peak RSS. V3 produced 231 segments
  and 1,136,263,446 package data bytes versus v2's 1,188 segments, 2,726,542,792 bytes, and 50.89
  seconds. This is 27.4% lower wall time, 80.6% fewer segments, and 58.3% fewer package bytes.
- Phase evidence changed from v2 to v3 as follows: package execution 45.669s -> 33.540s,
  validation/normalization 28.758s -> 10.656s, segment encode 9.412s -> 6.464s, and persist/hash
  10.117s -> 2.170s. Decode varied 16.181s -> 21.703s across live S3 runs; the aggregate result is
  still decisively positive and no source-I/O claim is attributed to segmentation.
- CLI parser coverage resolves binary byte suffixes and exposes all eight bounds to `plan` and
  `run`; the same shared command constructor serves explain/preview and backfill. Engine tests cover
  policy validation, source rechunking invariance, positions, and canonical assembly.
- Integrated gates passed: `cargo check --workspace --all-targets`, strict workspace clippy, and the
  733-test aggregate core/runtime/Iceberg suite. Formatting and `git diff --check` are clean.

## Review

Pass. All eight segment/microbatch row/byte bounds are required plan data and shared CLI knobs;
there is no source/destination branch, runtime-adaptive identity boundary, or compatibility shim.
The exact release workload improved materially rather than trading performance for policy.

## Retrospective

The original 32 MiB policy was calibrated against a narrow schema and its fixed IPC framing cost
was invisible until a 2,052-column workload multiplied it 1,188 times. Logical-byte segmentation
remains the correct identity contract, but performance defaults require both narrow and very-wide
fixtures. Every identity-bearing boundary is now plan data and an operator knob rather than a source
or destination special case.
