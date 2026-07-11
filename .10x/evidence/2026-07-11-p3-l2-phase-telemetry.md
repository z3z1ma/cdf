Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-10-p3-ws-l2-phase-telemetry.md

# P3 L2 phase telemetry evidence

## What was observed

CDF now records typed, terminal `phase_measured` events for decode, validation/normalization, segment encode, persist/hash, package finalize, destination write/receipt, checkpoint gate, and aggregate package execution. Each metric carries status, nanoseconds, input/output bytes, and operation count. Collection is explicitly disabled on the ordinary runtime path, opt-in through `RunTelemetryConfig`, capped at 32 terminal events by default, and backed by engine execution options rather than a benchmark-only channel.

The append-only SQLite run ledger migrated from schema v4 to v5 to admit the new event kind without weakening its `CHECK` constraint. Supported v1-v4 stores rebuild and copy their event table; unsupported future versions still fail closed.

## Procedure and results

From the repository root:

```text
CARGO_INCREMENTAL=0 cargo test -j1 -p cdf-kernel -p cdf-package -p cdf-engine -p cdf-state-sqlite -p cdf-project --locked
CARGO_INCREMENTAL=0 cargo test -j1 -p cdf-cli progress --locked
CARGO_INCREMENTAL=0 cargo clippy -j1 -p cdf-kernel -p cdf-package -p cdf-engine -p cdf-state-sqlite -p cdf-project -p cdf-cli --all-targets --locked -- -D warnings
cargo fmt --all -- --check
git diff --check
```

- Kernel: 23 tests passed, including legacy JSON byte compatibility and typed metric round-trip.
- Package: 34 tests passed.
- Engine: 52 tests passed, including telemetry-on/off manifest identity, package hash, and signature equality plus all required engine phase metrics.
- Project runtime: 171 tests passed, including a successful full run with all eight required terminal phases, bounded event count, nonzero durations/operations/bytes, failure terminalization, and live-sink secret rejection.
- SQLite state: 38 tests passed, including v1/v3 migration preservation, v5 authority, append-only behavior, and future-version rejection.
- CLI progress/rendering: 16 focused tests passed; existing ordinary progress ordering, headless output, quiet/verbose behavior, backpressure, and redaction remain green.
- All changed crates and targets passed Clippy with warnings denied; formatting and diff checks passed.

The first full verification found that two future-schema tests still used v5 as their deliberately unsupported value after v5 became current. They were corrected to v6 and the complete state suite then passed.

## What this supports

This supports every L2 acceptance criterion: kernel-owned additive schema, complete success/failure terminal evidence, additive JSON compatibility, bounded and genuinely disabled collection, secret-safe rendering/storage, and unchanged deterministic package identity.

## Limits

L2 observes the current sequential phases. It does not introduce Tokio, concurrent operators, a memory ledger, benchmark providers, rooflines, or performance claims. Those remain owned by later P3 workstreams.
