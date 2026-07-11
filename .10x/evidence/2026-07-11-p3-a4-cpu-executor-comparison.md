Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-a4-injected-execution-host.md, .10x/decisions/standalone-cpu-executor-v1.md

# P3 A4 CPU executor comparison

## What was observed

On the P3 baseline host (Apple M5 Pro, 18 logical CPUs), 1,152 independent CPU tasks each performing 2,048 SHA-256 rounds completed in 23,555,125 ns on CDF's fixed CPU-slot pool and 23,257,209 ns on Tokio `spawn_blocking`. The fixed pool was 1.0128x the Tokio time (1.28% slower), inside ordinary single-run variance while providing explicit CPU-slot admission and a pool isolated from blocking/FFI work.

## Procedure

`cargo run -p cdf-benchmarks --bin executor-compare --release`

The committed runner records host logical CPUs, task/round counts, both elapsed times, and ratio as JSON. It uses the production `StandaloneExecutionHost` for the fixed-pool arm and the already-resolved Tokio version for the comparison arm.

## What this supports

The measurement supports selecting the standard-library fixed pool without adding Rayon or another CPU executor dependency. It challenges any claim that generic `spawn_blocking` is materially faster for this CPU task class.

## Limits

This is one CPU kernel and one host, not the final envelope. WS-B/V and the lab must continue measuring Arrow decode/validation and context switches. A materially different result may supersede the executor decision.
