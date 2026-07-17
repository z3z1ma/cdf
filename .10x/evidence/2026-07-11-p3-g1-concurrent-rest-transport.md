Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-g1-streaming-transport-byte-sources.md, .10x/tickets/done/2026-07-11-p0-sx1-source-extension-boundary.md

# Shared REST transport milestone

## What was observed

`cdf-http::HttpTransport` now requires `Send + Sync` and shared `send(&self, ...)`. `RestRuntimeDependencies` stores `Arc<dyn HttpTransport>` directly; the former `Arc<Mutex<Box<dyn HttpTransport>>>` and transport-poison failure path are deleted. REST discovery, runtime retry/pagination/auth policy, declarative compatibility construction, project discovery, CLI commands, benchmarks, and conformance all use the same shared contract.

Stateful recording transports keep narrow internal mutexes around their response queues. Auth refresh keeps its independent mutation lock. Reqwest's client is shared without a CDF-wide transport lock.

## Procedure

- `cargo test -p cdf-source-rest --locked` — 5 passed.
- `cargo test -p cdf-http --locked` — 6 passed.
- `cargo test -p cdf-project general_project_run_executes_deterministic_rest_resource_stream --locked` — 1 passed.
- `cargo check --workspace --all-targets --locked` — passed with unrelated existing test-only warnings outside the changed surface.
- `cargo clippy -p cdf-http -p cdf-source-rest -p cdf-declarative -p cdf-project -p cdf-cli -p cdf-conformance -p cdf-benchmarks --all-targets --locked -- -D warnings` — passed.

The new permanent concurrency test issues two requests through one `RestRuntimeDependencies` transport and observes peak simultaneous entry of two. HTTP policy coverage preserves egress-before-send, auth redaction/refresh, paginator, retry, rate-limit, and format detection behavior.

## What this supports or challenges

This supports a source-neutral HTTP boundary without hidden adapter-wide serialization and proves current consumers accept the shared contract.

## Limits

REST currently executes one pagination chain sequentially by semantic necessity and still receives collected response bodies from blocking Reqwest. This milestone does not claim async streaming, multi-partition REST fan-out, SIMD decode, or throughput improvement; G1/B5/C2 own those steps.
