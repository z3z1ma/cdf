Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Relates-To: .10x/tickets/done/2026-07-11-p3-b1-streaming-byte-transforms.md, .10x/tickets/done/2026-07-11-p0-fx1-native-format-extension-boundary.md

# Shared transform streaming primitives

## What was observed

The neutral runtime now owns `AccountedByteCursor` and `TransformExpansionGuard`. The cursor preserves each input chunk's memory lease while borrowed, drops an exhausted chunk before polling the next, performs checked position accounting, and exposes no codec or transport policy. The guard centralizes absolute expansion limits, compression-ratio limits, overflow handling, one-output-chunk streaming grace, and exact boundary enforcement.

`cdf-transform-gzip` deleted its private cursor and expansion logic and uses these shared contracts. This prevents each compression/character transform from independently reimplementing memory-lifetime and adversarial-expansion behavior.

## Procedure

- `cargo test -p cdf-transform-gzip --locked`
  - Result: 3 tests passed after the refactor, including the exact peak-memory assertion.
- `cargo clippy -p cdf-runtime -p cdf-transform-gzip --all-targets --locked -- -D warnings`
  - Result: passed.
- `git diff --check`
  - Result: passed before commit.

## What this supports or challenges

This supports B1's requirement that all transforms inherit identical ledger and expansion semantics and FX1's extension law: new driver crates need framing/parser logic, not copied orchestration mechanics.

## Limits

The primitives do not select transforms, compose them with byte sources, or provide checksum-window publication atomicity. Those remain B1 integration work.
