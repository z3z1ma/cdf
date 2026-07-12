Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-v2-validation-graph-integration.md

# V2 constant-memory quarantine closeout

## What was observed

A 100%-quarantine workload under a 512 KiB managed evidence budget exposed aggregate Parquet row-group metadata retained by one writer. Before repair, the exact write-only process peaked at 20,824,064 bytes RSS for 25,000 rows and 30,785,536 bytes for 250,000 rows.

The accumulator now finalizes each bounded evidence chunk as its own atomic, hash-while-write Parquet part. After repair, 25,000 rows peaked at 19,873,792 bytes and 250,000 at 20,201,472 bytes: only 327,680 bytes growth for 10x input. Both runs held the evidence ledger to 512 KiB and released it to zero.

A non-ignored 25,000-row law reads every part back in order and verifies the exact count and terminal ordinals. The too-small-budget law still fails before artifact creation.

## Procedure

```text
cargo test -p cdf-engine dense_quarantine_evidence_stays_bounded_without_losing_rows --locked
CDF_QUARANTINE_RSS_ROWS=25000 /usr/bin/time -l target/debug/deps/cdf_engine-3140021acfbdf0df --ignored --exact execution::transform_kernel_tests::dense_quarantine_evidence_rss_probe
CDF_QUARANTINE_RSS_ROWS=250000 /usr/bin/time -l target/debug/deps/cdf_engine-3140021acfbdf0df --ignored --exact execution::transform_kernel_tests::dense_quarantine_evidence_rss_probe
cargo test -p cdf-engine --locked
cargo clippy -p cdf-engine --all-targets --locked -- -D warnings
```

The complete engine suite passed 93 tests with five intentional slow/benchmark tests ignored.

## What this supports or challenges

This supports V2's high-failure boundedness/no-loss criterion and validates the existing 3x per-chunk lease calibration when aggregate Parquet metadata is also bounded. It challenges any design that treats a streamed writer as automatically constant-memory: footer/index metadata is aggregate state unless files rotate or metadata spills.

## Limits

This host RSS evidence is macOS/debug-test calibration, not the V3 release envelope. Rotating very small budget-driven parts increases fsync/file-count overhead; V3 owns throughput tuning and the production density/batch matrix without weakening the constant-memory law.
