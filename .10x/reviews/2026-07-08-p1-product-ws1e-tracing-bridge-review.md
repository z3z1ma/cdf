Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-p1-product-ws1e-tracing-bridge.md
Verdict: pass

# P1 product WS1E tracing bridge review

## Target

Implementation and evidence for `.10x/tickets/done/2026-07-08-p1-product-ws1e-tracing-bridge.md`.

## Assumptions tested

- The bridge must consume the existing runtime event spine and must not define new event vocabulary.
- The bridge must be optional and non-authoritative; tracing loss must not fail or stall a run.
- Durable ledger append and redaction validation must remain before live/tracing publication.
- Structured tracing fields must preserve run, resource, scope, partition, package, destination, plan, checkpoint, receipt, event kind, and sequence data from the persisted event envelope.
- Details must not bypass existing redaction guardrails.
- No OTLP exporter, CLI progress renderer, CLI JSON behavior, parser grammar, package identity, receipt verification, checkpoint authority, or run success semantics may change.

## Findings

No blocking findings.

The bridge is implemented as `TracingRunEventSink`, so it reuses the WS1B non-authoritative sink boundary rather than adding a new orchestration path. `try_emit` validates event details again before emission and returns `Dropped` on invalid details or serialization failure; the existing fanout ignores sink drops, preserving run success and ledger completeness. The runtime fanout still sends only persisted event envelopes after durable append.

The tracing event uses fixed structured fields for authoritative identifiers and lifecycle metadata, and emits `details` as a JSON field. Dynamic detail flattening was deliberately not added: it would create unstable tracing field vocabulary outside WS1C's event-vocabulary ownership and is not required for this acceptance criterion, which only requires detail redaction guardrails.

During review, a lossy `scope` field representation was corrected before closure. The final implementation emits the serialized `ScopeKey`, while still preserving `partition_id` as its own field.

The focused tracing capture tests are strict: they assert the exact emitted field map against persisted ledger events, prove receipt/checkpoint/package fields are present on the relevant lifecycle events, and prove raw secret details produce no tracing event. The same test checks package/checkpoint/receipt identity invariants after tracing is enabled.

Quality evidence is sufficient for this slice. Parent integration reran the focused tracing/fanout tests, full `cdf-project` tests, `cdf-project` check/clippy, and `cargo fmt --all -- --check` after the concurrent CLI workstream formatting completed. CodeQL has 0 SARIF results but retains the known Rust extractor coverage limitations seen in prior WS1C evidence.

## Verdict

Pass. WS1E satisfies the tracing bridge acceptance criteria without implementing OTLP export or changing runtime event vocabulary, CLI behavior, package identity, ledger authority, receipt verification, checkpoint authority, or run success semantics.

## Residual risk

The public run request still accepts a single optional live sink. This is sufficient for WS1E because the bridge is itself a sink and the internal fanout remains non-authoritative. No follow-up ticket is opened for public sink composition because no current active spec or executable ticket requires simultaneous CLI live progress and tracing in one request.
