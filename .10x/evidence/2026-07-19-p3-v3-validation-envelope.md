Status: recorded
Created: 2026-07-19
Updated: 2026-07-19
Relates-To: .10x/tickets/done/2026-07-11-p3-v3-validation-envelope-closeout.md, .10x/tickets/done/2026-07-11-p3-ws-v-vectorized-validation.md

# V3 validation envelope closeout

## Observation

The ratified single-core 64k hot-kernel matrix passes its exact 1 GB/s/core threshold in all 12 gated cells on the dedicated `c7i.4xlarge` benchmark host. The slowest gated result is numeric range validation with 100% violations at 3.016 GB/s; the fastest is string-domain validation with no violations at 7.254 GB/s. The host memcpy roofline is 16.689 GB/s, so the gated cells operate at 18.1% through 43.5% of that roofline.

The complete report contains 57 cells across 8k, 16k, and 64k batches; 0%, one-row, and 100% violation densities; numeric widths, strings, timestamps, TLC-width mixed data, decimal/nested boundaries, and selected-row evidence. Only the 12 ratified 64k data-inspecting hot-kernel cells can pass or fail the throughput gate. Boundary and evidence-materialization cells remain visible as trend-only measurements and cannot inflate the throughput claim.

The current full-year TLC product run separately records 11,868,646,460 validation/normalization bytes in 262,969,925 ns, approximately 45.1 GB/s for that concurrent product phase. Its complete run processed 41,169,720 rows in 10.477 seconds under a 6 GiB cgroup. This is end-to-end preservation evidence, not an isolated single-core comparison.

## Procedure

The clean revision `88de6ce1c` was synchronized to EC2 instance `i-05011a85b7f2a33fe` and built with the release profile and prebuilt DuckDB linkage. The authoritative command was:

```text
taskset -c 0 target/release/cdf-p3-measure validation-envelope 7 33554432
```

It completed in 88.99 seconds at 99% CPU with 134,084 KiB maximum RSS, zero swaps, and exit status zero. The raw report and GNU time output are:

- `.10x/evidence/.storage/2026-07-19-p3-v3-validation-envelope-ec2.json`
- `.10x/evidence/.storage/2026-07-19-p3-v3-validation-envelope-ec2.time`

The report's gate was independently checked to contain exactly 12 non-trend cells and require every one to be `passed`. `cargo test -p cdf-contract --lib --locked -j 12` passed 90 correctness tests with only the two explicitly ignored release-only performance tests omitted. `cargo test -p cdf-bench-measure validation_ --locked -j 12` passed both matrix/report contract tests, and strict Clippy passed for the slim measurement graph.

The permanent scheduled workflow runs the slim matrix command and a separate fused validation/normalization trend outside fast checks. Moving this runner out of the full lab graph reduced a clean local release link from 6m28s to 41.09s; the EC2 slim release build took 1m01s.

## What this supports or challenges

This closes the vector-validation performance envelope without counting uninspected columns or evidence bytes as kernel throughput. It supports the production vector path already proven by V2 and makes the target a variance-aware scheduled regression law. Selected evidence remains separately visible: the EC2 64k cells observed 0 rows at 1.044B rows/s, one row and 36 string bytes at 1.026B rows/s, and 65,536 rows plus 2,359,296 string bytes at 10.56M rows/s.

## Limits

The EC2 image does not expose `perf`; the attempted counter run is recorded as `.10x/evidence/.storage/2026-07-19-p3-v3-validation-envelope-perf.stat`. Consequently this evidence contains wall-clock, MAD, roofline, exact inspected-byte, retained-mask, and selected-evidence counters but no cycles, instructions, branches, or cache misses. The allocation evidence is structural retained-output accounting, not a general-purpose heap profile. These limits do not weaken the threshold or hide product-path costs, and no runtime code changed in V3.
