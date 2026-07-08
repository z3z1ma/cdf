Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-07-p0-workstream-c-spine-conformance-harness.md

# P0 C5: Property and fuzz targets

## Scope

Wire property/fuzz targets into the quality cadence for the Workstream-C required surfaces: contract verdict-lattice totality, position serialization round-trips across `state_version`, and adversarial NDJSON/Singer/Airbyte parser input.

Owns:

- conformance property tests and/or fuzz target source;
- scoped dev-dependency additions only if the tests import them;
- quality-cadence documentation in evidence and, if useful, `.10x/knowledge/quality-gate-execution.md`;
- exact commands and runtime expectations for local/CI execution.

## Acceptance Criteria

- Contract verdict-lattice generation proves totality or records the exact active implementation gap if the live contract verdict type is not yet deep enough.
- Position serialization round-trips cover every active `SourcePosition` variant and `CHECKPOINT_STATE_VERSION`.
- NDJSON parser adversarial input tests exercise malformed records, mixed valid/invalid records, oversized/strange scalar values, and UTF-8 edge cases without panics or partial unsafe acceptance.
- Singer and Airbyte parser adversarial input tests exercise malformed protocol messages, foreign state payloads, unknown fields, and truncated streams.
- Property/fuzz commands are recorded with expected runtime and pass/fail interpretation.
- Tool or infrastructure limits are recorded as limits, not as silent skips.

## Evidence Expectations

Run the new property/fuzz tests, focused parser tests, `cargo fmt --all --check`, `cargo check -p cdf-conformance -p cdf-contract -p cdf-formats -p cdf-subprocess --all-targets --locked`, `cargo clippy` over touched crates with `-D warnings`, `cargo nextest run -p cdf-conformance --locked`, `cargo fuzz list` if fuzz targets are created, `git diff --check`, and relevant jscpd/rust-code-analysis metrics for new harness source.

## Explicit Exclusions

No production parser rewrite unless a target exposes a real bug that must be fixed to satisfy existing behavior. No speculative contract semantics beyond active specs/records. No broad CI workflow changes unless already required by an active quality record.

## Progress And Notes

- 2026-07-08: Split from P0 Workstream C. Current `cdf-conformance` has no `proptest`, `quickcheck`, or fuzz dependency; this child owns selecting the smallest appropriate property/fuzz mechanism.

## Blockers

None known. If a required surface is too shallow to express the target without inventing semantics, mark this child blocked with the exact missing active contract rather than adding guessed behavior.
