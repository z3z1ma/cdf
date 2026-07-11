Status: open
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/2026-07-10-p3-ws-a-streaming-runtime-pipeline.md, .10x/specs/performance-lab-and-envelope.md

# P3 WS-F: constant-memory guarantee

## Scope

Make the memory law executable: generated 100 GB input under 2 GiB, peak-RSS assertion, spill observation, successful completion, too-small-budget clean failure, `cdf doctor` budget reporting, and P1 run-panel peak ledger rendering.

## Acceptance criteria

- RSS remains within the ratified ceiling independent of input size.
- Keyless dedup, decompression, decoder windows, queues, package builders, and destination staging are ledger-accounted or spilled.
- A budget too small for one legal batch fails with a remedial `Data` error, never OOM.
- Stress and failure laws are permanent slow-tier CI.

## Blockers

Blocked on WS-A ledger/spill and WS-L measurement protocol.
