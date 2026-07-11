Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-10-p2-ws-f3-keyless-exact-row-dedup.md

# P2 F3 keyless exact-row dedup evidence

## Observation

An append resource may explicitly declare `deduplicate = "exact_row"` without primary or merge keys. The kernel descriptor records the semantic, the contract compiler binds a distinct all-column keep-first verdict into the validation program, and the engine applies that rule before segment persistence for append packages. Arrow row encoding supplies typed, null-safe, nested-capable identity. The package summary records input/output counts and dropped-to-kept package ordinals.

Ordinary append behavior and keyed contract dedup remain unchanged when the exact-row option is absent. Replace/merge plus this option fails during declarative compilation with both fixes.

## Procedure

- `cargo check --workspace --all-targets`: passed.
- `cargo test -p cdf-kernel -p cdf-contract -p cdf-engine -p cdf-declarative`: 22 + 69 + 51 + 100 tests passed, plus doc tests.
- `cargo test -p cdf-cli keyless_append_exact_row_dedup_is_explicit_and_evidence_preserving`: passed through real project plan/package/DuckDB commit.
- Focused ordinary append/replace keyed-dedup regression tests passed.
- `cargo clippy -p cdf-kernel -p cdf-contract -p cdf-engine -p cdf-declarative -p cdf-project -p cdf-cli --all-targets -- -D warnings`: passed.

## Limits

The evaluator retains package identity state in memory, as the pre-existing merge dedup evaluator did. The active performance backlog and P3 memory-ledger work own bounded/spilled execution without changing this semantic.
