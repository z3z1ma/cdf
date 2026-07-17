Status: recorded
Created: 2026-07-16
Updated: 2026-07-17

# P3 G4 HTTP progress-liveness evidence

## Observation

A live full-year NYC TLC HTTPS run could retain an established response indefinitely after it stopped delivering body bytes. The run produced two partitions, then showed no durable progress for roughly four minutes with nearly idle CPU. A fresh three-month run against the same origin completed normally in 4.71 seconds. The HTTP file byte-source had no phase deadline around response establishment or body progress.

The bounded repair adds private file byte-source progress deadlines: 10 seconds to receive the file response and 10 seconds between delivered body frames. It deliberately adds no total request deadline and does not put `read_timeout` on the shared pooled client. Healthy transfers can run indefinitely while they continue delivering bytes; a file response that makes no progress becomes a sanitized typed `Transient` failure eligible for the existing generation-bound retry policy where the compiled source retry path owns retry.

## Procedure

Live observations on the same host and release build:

- Twelve public `yellow_tripdata_2024-*.parquet` objects through CDF: interrupted after `real 246.65s`, `user 4.63s`, `sys 10.43s`; two partitions had become durable and no later progress was visible.
- Fresh January-March resource through CDF: `real 4.71s`, 9.6 million rows, 153 MiB source physical bytes, 334 MiB package, DuckDB receipt committed.
- Post-repair fresh January-March resource through CDF: `real 5.40s`, 9.6 million rows, 153 MiB source physical bytes, 334 MiB package, DuckDB receipt committed. This preserves the fast path within public-endpoint noise but does not improve it.
- Post-repair fresh full-year resource through CDF: interrupted after `real 114.73s`, `user 3.02s`, `sys 9.42s`, max RSS about 1.0 GB. The package directory contained only first-partition `p00000000` segment files and the state ledger had only `run_started`, `plan_recorded`, and `package_started`. This remains a failing G4 live cell.
- Two-way parallel curl of all twelve objects: `real 4.34s`, 660 MiB downloaded.
- DuckDB native `read_parquet(..., union_by_name=true)` materialization: `real 2.53s`, 41,169,720 rows.

Focused deterministic verification after the boundary correction:

```text
CARGO_BUILD_JOBS=12 cargo test -p cdf-transport-http --lib --locked -j 12
15 passed; 0 failed

CARGO_BUILD_JOBS=12 cargo clippy -p cdf-transport-http --all-targets --locked -j 12 -- -D warnings
Finished successfully
```

The loopback tests cover: valid response headers followed by a stalled promised body; a slow transfer whose frames arrive every 30 ms under a 50 ms idle deadline; and a REST request that waits 120 ms despite the provider's private file deadlines being 20 ms. With the stalled body, the stream returns a typed `Transient` error within the external one-second bound, the error contains only sanitized operation context, and its memory lease returns to zero after cancellation.

## What this supports or challenges

- Supports adding bounded file payload progress liveness without imposing a total-transfer deadline.
- Supports keeping REST and file metadata behavior out of this file-transfer timeout policy.
- Supports reusing the existing transient retry taxonomy rather than adding HTTP-specific scheduler behavior.
- Challenges any claim that the interrupted twelve-month observation establishes a general throughput regression: the successful three-month run is fast, and the interrupted process consumed little CPU while stalled.
- Establishes a same-host G4 composite roofline of 6.87 seconds and a current 1.5x acceptance ceiling of 10.31 seconds for the full-year cell.
- Challenges closure of G4: the post-repair full-year run is still far outside the 10.31-second ceiling and did not finish the first partition-to-ledger publication path within 114.73 seconds.

## Limits

The interrupted public-endpoint observations are consistent with slow or stalled remote progress but cannot prove the remote or local socket-level cause after interruption. Public endpoint conditions are uncontrolled. The deterministic tests prove timeout classification, cleanup, bounded return, slow-progress allowance, and REST non-inheritance; they do not by themselves prove a scheduler retry succeeds. Existing scheduler retry laws remain the authority for that later transition. The full-year post-repair run failed the live G4 envelope and the provider matrix remains open G4 evidence.
