Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-p1-product-ws1a-run-event-sink-foundation.md
Verdict: pass

# P1 product WS1A run event sink foundation review

## Target

Implementation and closure evidence for `.10x/tickets/done/2026-07-08-p1-product-ws1a-run-event-sink-foundation.md`.

## Assumptions tested

- The run ledger remains append-only evidence and is not replaced by the live sink.
- Live subscribers cannot become state-advancement authorities.
- Sink drops must not fail or stall runs.
- Live sink events must be the exact persisted envelope, including run id, sequence number, timestamp, pointers, kind, and details.
- Moving DTOs into `cdf-kernel` must preserve `cdf-state-sqlite` public API names.
- Secret validation must happen before live sink emission.
- Compile-fix call-site changes must not alter CLI rendering, grammar, conformance semantics, benchmarks, docs, release, WASM, or unrelated files.

## Findings

No blocking findings.

Parent review finding: the public `RunEventSink` trait originally relied on the ticket for its non-blocking contract. The implementation was updated with direct trait documentation so future CLI/tracing subscribers see the drop-not-stall requirement at the API boundary.

Minor residual risk: `RunRecord` and `RunLedgerSnapshot` remain owned by `cdf-state-sqlite`. This is intentional for this slice because the live sink and reusable event model require the event DTOs, not a full backend ledger snapshot abstraction. Future non-SQLite ledger work can rehome snapshot records under a separate spec/ticket if needed.

Minor residual risk: `jscpd` continues to report pre-existing repetition in the broad runtime test suite. The focused report shows `newClones = 0` and `newDuplicatedLines = 0`; refactoring that harness would exceed this ticket.

Minor residual risk: the sink is non-blocking by trait contract and by the tested bounded sink behavior, but Rust cannot prevent a bad implementation from sleeping inside `try_emit`. WS1/WS5 own the concrete CLI/tracing subscribers and must keep those implementations bounded and drop-capable.

Parent verification passed after the trait-doc update. CodeQL, Semgrep, Gitleaks, audit, deny, vet, jscpd, complexity metrics, direct unsafe scan, and targeted Rust tests were all observed by the parent; OSV only reported the already-ratified `paste` advisory.

## Verdict

Pass. The implementation satisfies WS1A: kernel owns the run event model and non-blocking sink contract, SQLite re-exports compatibility names, project runs accept an optional sink, recorder fanout occurs only after durable ledger append, drop results are ignored, focused tests cover order/drop/redaction behavior, and package/checkpoint receipt assertions remain intact.

## Residual risk

CLI live progress, tracing bridge, OTLP export, and broader replay/resume event-spine fanout remain future WS1/WS5 work and are explicitly excluded from this ticket.
