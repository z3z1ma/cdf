Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws1-runtime-event-spine.md
Depends-On: .10x/specs/run-orchestration-ledger.md, .10x/specs/project-cli-observability-security.md

# P1 product WS1A: Run event sink foundation

## Scope

Implement the first executable slice of the runtime event spine: a non-SQLite-owned run event model plus a non-blocking live event sink accepted by `ProjectRunRequest` and emitted from the current `ProjectRunRecorder` lifecycle seam.

Owns:

- Moving or rehoming the reusable run event DTOs/vocabulary from the SQLite backend boundary into `cdf-kernel` so live events are not SQLite-owned.
- Preserving `cdf-state-sqlite` public re-exports for existing callers while making the SQLite ledger consume the kernel event DTOs.
- Adding a small non-blocking `RunEventSink` contract suitable for CLI rendering, tracing bridge, and test subscribers.
- Adding `ProjectRunRequest::event_sink` or equivalent request input.
- Emitting the exact persisted lifecycle events to the live sink after durable ledger append, without making the sink a state-advancement authority.
- Focused tests proving ordering, sink drops, redaction guardrails, and no package/checkpoint artifact identity drift for the existing live run path.

## Acceptance criteria

- `cdf-kernel` exports the shared run event model: event kind, event details/value, append envelope, persisted event envelope, and secret-reference value type.
- `cdf-state-sqlite` still exposes the same run-ledger public API names for compatibility, but the DTO definitions are no longer private to the SQLite crate.
- `RunEventSink` is synchronous and non-blocking by contract: it uses a `try_emit`-style method whose dropped/full result does not fail or stall the run.
- `ProjectRunRequest` accepts an optional sink; callers that omit it keep current behavior.
- `ProjectRunRecorder` emits a live event for each event it appends to the ledger. The live event has the persisted run id, sequence number, timestamp, kind, pointers, and details.
- A focused run test asserts live sink event kinds match the ledger order for a successful run.
- A bounded/full sink test proves dropped live events do not fail the run and do not reduce ledger completeness.
- A redaction/secret test proves the live sink cannot receive raw secret-looking string values that the ledger guard would reject; typed `SecretRef` remains allowed.
- Existing package hash, receipt verification, checkpoint commit, and package status assertions for the touched run path remain unchanged.

## Evidence expectations

Run and record:

- `cargo fmt --all --check`
- `cargo test -p cdf-kernel --locked`
- `cargo test -p cdf-state-sqlite --locked`
- Focused `cargo test -p cdf-project` tests covering run event sink ordering/drop behavior.
- `cargo clippy -p cdf-kernel -p cdf-state-sqlite -p cdf-project --all-targets --locked -- -D warnings`
- Direct unsafe/FFI scan over touched Rust files.
- Source-only Gitleaks over touched Rust and `.10x` records.
- Focused `jscpd` over touched Rust modules and the new/updated records.

## Explicit exclusions

No CLI progress renderer, no spinners, no OTLP exporter, no `tracing` bridge implementation, no NDJSON event stream, no backfill/replay/resume full event plumbing beyond preserving existing compile behavior, no broad command grammar work, and no artifact schema changes.

## Design notes

This slice deliberately emits to the live sink after the SQLite ledger append so subscribers see the exact durable event envelope and the run remains recoverable even if a subscriber drops events. A later WS1 child may refactor the ledger writer into a first-class subscriber once the live event bus exists; this ticket must not weaken the ledger's current append-only authority.

## Progress and notes

- 2026-07-08: Split from `.10x/tickets/2026-07-08-p1-product-ws1-runtime-event-spine.md` after inspecting `crates/cdf-project/src/runtime/ledger.rs`, `crates/cdf-project/src/runtime/orchestration.rs`, and `crates/cdf-state-sqlite/src/run_ledger.rs`. The existing recorder is the smallest safe fanout seam for first live event evidence.

## Blockers

None.
