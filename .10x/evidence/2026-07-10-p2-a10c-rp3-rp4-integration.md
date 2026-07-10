Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-09-p2-ws-a10c-exhaustive-local-binary-discovery.md, .10x/tickets/done/2026-07-10-p2-rp3-correction-capabilities-row-provenance.md, .10x/tickets/done/2026-07-10-p2-rp4-schema-scope-lease-lock-cas.md

# P2 A10c/RP3/RP4 integration evidence

## What was observed

CDF now performs exhaustive resource-level discovery for local multi-file Parquet and Arrow IPC sets through one bounded binary-discovery orchestrator. It probes every metadata block, aggregates physical schemas with the shared widening lattice, normalizes after aggregation, emits one canonical discovery manifest, and carries manifest-linked v2 snapshots through discover, pin, diff, no-pin, auto-pin, add, and run. Runtime exact `FileManifest` SHA identity remains independent from bounded discovery identity.

The promotion foundation now has a kernel-owned original row address, versioned destination protocol capability aggregate, generic correction plans, truthful destination declarations, an executor-neutral fenced lease contract, in-memory/SQLite stores, and crash-safe guarded `cdf.lock` mutation. Postgres claims only the provenance persistence that its live target proves; it does not claim targetability, residual readback, or a correction strategy. DuckDB, Parquet, and new destinations inherit truthful unsupported defaults.

## Procedure

- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `cargo check --workspace --all-targets --locked`: passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
- `cargo nextest run --workspace --locked`: passed, 854/854, including four slow 100-run golden/live/run-matrix cases.
- `cargo test --workspace --doc --all-features --locked --no-fail-fast`: passed.
- `cargo doc --workspace --all-features --no-deps --locked`: passed.
- `cargo deny check`: passed with the repository's existing dual-Arrow warning.
- `cargo audit --deny warnings --ignore RUSTSEC-2024-0436`: passed with the existing documented exception.
- `cargo vet --locked`: passed, 455 exemptions.
- `cargo machete --with-metadata --skip-target-dir` over all affected crates: no unused dependencies.
- `cargo semver-checks check-release -p cdf-kernel --baseline-rev HEAD`: 196/196 passed.
- `cargo semver-checks check-release -p cdf-state-sqlite --baseline-rev HEAD`: 196/196 passed.
- `cargo semver-checks check-release -p cdf-project --baseline-rev HEAD`: 195/196 passed; the sole reported major change is the intentional `LockedDestination` `#[non_exhaustive]` constructor migration recorded in `.10x/decisions/destination-protocol-capabilities-extension-seam.md`. No other project API finding occurred.

Focused coverage proves bounded Parquet/IPC metadata reads; exhaustive widening, missing-field, nested, metadata-variance, normalizer-collision, malformed/incompatible, set-change, no-write, exact-baseline, and no-partial-evidence behavior; multi-file CLI lifecycle and exact runtime manifest preservation; correction vocabulary/round trips/impossible-claim rejection; live Postgres provenance persistence; generic false-claim conformance; lease contention/expiry/renew/release/fencing/persistence; checkpoint/receipt migration preservation; exact lock authority, stale fencing, failpoints, expiry-during-publication, guarded-writer contention, crash-safe creation/update, and additive migration idempotency.

## What this supports

This supports every scoped acceptance criterion for A10c, RP3, and RP4. It establishes the reusable source-set, destination capability, row-provenance, lease-store, and lock-publication seams needed by A10d/A10g and RP2/RP5-RP9.

## Limits

A10d still owns effective-schema runtime/package evidence and nullable missing-field materialization. A10g owns explicit sampled selection. RP2 owns live residual verdict routing; RP5-RP9 own promotion planning, destination corrections, recovery, and retention. Advisory lock coordination covers CDF writers; non-cooperating external filesystem actors remain outside that protocol and are reported explicitly rather than hidden.
