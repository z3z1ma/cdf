Status: open
Created: 2026-07-11
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-10-p3-ws-h-interop-boundaries.md
Depends-On: .10x/tickets/done/2026-07-11-p3-h1-interop-measurement-copy-proof.md, .10x/tickets/done/2026-07-11-p3-a4-injected-execution-host.md, .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md, .10x/tickets/done/2026-07-11-p3-b3-arrow-ipc-codecs.md, .10x/tickets/done/2026-07-11-p3-b5-json-codecs.md

# P3 H3: streaming supervised subprocess/protocol boundary

## Scope

Replace `wait_with_output` and materialized decoding with concurrent incremental stdout/stderr/control supervision, bounded IPC/row framing, process-tree budgets/groups, structured cancellation/reaping, typed state, shared reconciliation, and Singer/Airbyte compatibility streaming.

## Acceptance criteria

- Arbitrarily long stdout/stderr runs stay bounded and stdout backpressure reaches the child.
- Arrow IPC batches decode/publish incrementally; NDJSON/Singer/Airbyte use bounded row windows.
- Nonzero exit/protocol failure after data cannot gate the epoch; retry/recovery is deterministic.
- Timeout/cancel kills/reaps descendants, releases leases, preserves bounded redacted diagnostics, and leaves no checkpoint ahead of receipts.
- IPC/row throughput and copy/memory costs are reported separately.

## Evidence expectations

Adversarial child fixtures (stderr flood, stalls, truncation, signals, descendants, malformed/state ordering), process-tree/RSS traces, package/checkpoint inspection, before/after benchmarks, and supervision/security review.

## Explicit exclusions

No arbitrary shell parsing, ambient secret injection, or Wasmtime.

## Blockers

None. H1, runtime ledger, injected execution host, and streaming codecs are done; this ticket is executable.

## References

- `.10x/specs/foreign-stream-interop.md`
