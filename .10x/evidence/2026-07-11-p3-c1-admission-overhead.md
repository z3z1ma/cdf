Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-c1-scheduler-admission-contract.md

# Scheduler admission overhead and context switches

## What was observed

On the P3 baseline Apple M5 Pro host, the release-mode neutral admission controller completed 409,600 enqueue/admit/release transitions across 64 competing resource queues at 3,423,569 admissions/second, or 292.09 ns per admission. The measured controller interval was 119,641,209 ns. The complete process reported 0 voluntary and 203 involuntary context switches, 0 page faults, and 0 swaps.

## Procedure

1. `cargo build -p cdf-benchmarks --bin scheduler-admission --release`
2. `/usr/bin/time -l target/release/scheduler-admission`

The committed runner uses the production `FairAdmissionController` with a 64-job/CPU/I/O/connection envelope, 64 MiB accounted memory, 64 resource queues, and a shared-origin quota of 32. It emits machine-readable request count, wall time, ns/admission, and admissions/second. macOS `time -l` supplies OS context-switch and memory counters.

## What this supports

The admission calculus is orders of magnitude below Arrow batch decode/validation costs at the planned 8k–64k-row task grain and does not create worker threads or voluntary scheduling churn. It is suitable as a per-partition/unit control-plane operation without becoming the data-plane bottleneck.

## Limits

This measures the single-owner C1 admission state machine, not C2 task execution, channel/frontier contention, or end-to-end scaling. C3/C4 must measure runnable threads, context switches, CPU utilization, and roofline scaling after production fan-out exists. The OS counters include process startup outside the controller's internal timer.
