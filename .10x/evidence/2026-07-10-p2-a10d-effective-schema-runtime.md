Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-09-p2-ws-a10d-effective-schema-runtime-evidence.md

# P2 A10d effective schema runtime evidence

## What was observed

CDF now executes compatible multi-observation resources against an immutable discovered baseline and a separately identified plan-time effective schema. The kernel/engine authority is source-neutral, physical schemas are deduplicated by a recursive structural fingerprint, and each observation carries its exact existing `SchemaCoercionPlan`. Missing nullable fields materialize typed nulls; empty valid inputs emit zero-row evidence; repeated partitions may share one observation; bounded limits attest only attempted observations.

The end-to-end Parquet regression pins a one-file `int32` baseline, records exact lock and snapshot bytes, adds an `int64` widening, a new nullable field, a missing-field input, and an empty input, then plans and runs without changing the baseline reference, snapshot bytes, lock bytes, or snapshot inventory. Destination planning and package output use the effective schema. Package verification detects coercion-sidecar tampering, and replay succeeds after source, state, and destination deletion. Financial/freeze planning keeps conforming baseline authority and rejects compatible drift at the named A10e disposition boundary before package, state, or destination writes.

## Procedure

- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `cargo check --workspace --all-targets --locked`: passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
- `cargo nextest run --workspace --locked --no-fail-fast`: passed, 859/859, including the four slow 100-run golden/live/run-matrix cases.
- `cargo test --workspace --doc --all-features --locked --no-fail-fast`: passed.
- `cargo doc --workspace --all-features --no-deps --locked`: passed.
- `cargo deny check`: passed with the repository's existing dual-Arrow warnings.
- `cargo audit --deny warnings --ignore RUSTSEC-2024-0436`: passed with the existing documented exception.
- `cargo vet --locked`: passed, 455 exemptions.
- `cargo machete --with-metadata --skip-target-dir`: no unused dependencies.
- `cargo semver-checks check-release -p cdf-kernel --baseline-rev HEAD`: 196/196 passed.
- `cargo semver-checks check-release -p cdf-contract --baseline-rev HEAD`: 196/196 passed.
- `cargo semver-checks check-release -p cdf-engine --baseline-rev HEAD`: 195/196; the sole finding is the intentional `EnginePlan` non-exhaustive migration recorded in `.10x/decisions/effective-schema-runtime-authority.md`.
- `cargo semver-checks check-release -p cdf-project --baseline-rev HEAD`: 195/196; the sole finding is the intentional `ResourceSchemaDiscoveryArtifacts` non-exhaustive migration recorded in the same decision.

Focused tests cover delimiter-safe/map-order-independent fingerprints; nested child name/nullability/metadata sensitivity; non-observable pinned resources remaining source-free; repeated partitions sharing one observation; limit early termination; immutable-baseline evolve; freeze/A10e boundary; nullable missing fields; empty input attestation; package verification; tamper rejection; and source-free replay.

## What this supports

This supports every A10d acceptance criterion: baseline/effective/manifest identity separation, immutable-baseline evolution, freeze behavior, typed-null materialization, physical provenance, distinct exact per-observation coercion plans, deterministic package evidence, effective destination planning, source-free replay, and legacy omitted-field deserialization.

## Limits

A10e still owns terminal incompatible-observation quarantine, processed positions independent of output segments, all-quarantine packages, and gate-backed checkpoint advancement. RP2 owns residual row/path capture. A10g owns explicit sampled binary coverage only after both total runtime outcomes exist. Remote enumeration remains WS-E scope.
