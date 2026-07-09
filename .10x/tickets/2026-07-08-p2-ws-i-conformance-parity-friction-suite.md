Status: open
Created: 2026-07-08
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-08-p2-data-onramp-program.md
Depends-On: .10x/specs/data-onramp-conformance.md, .10x/knowledge/runtime-conformance-throughput-rule.md

# P2 WS-I conformance, parity, and friction regression suite

## Scope

Make the harness catch up as P2 lands: S1-S8 golden paths, preview/run parity law, eighteen-friction regression mapping, deterministic fixtures plus live tier, and widening-lattice property tests.

Split executable child tickets before code for each conformance slice or property target. This workstream accrues throughout P2 and gates every other workstream closure.

## Acceptance criteria

- S1-S8 are represented in conformance, with deterministic fixture coverage for ordinary CI.
- Preview/run parity is a named law per source archetype.
- Each of the eighteen P2 frictions maps to a regression test in closure evidence.
- Widening-lattice property tests prove composition and no value loss for supported Arrow arrays.
- FileManifest state round-trips remain covered across checkpoint `state_version`.
- The final P2 evidence includes a recorded S1+S2 terminal session.

## Evidence expectations

Conformance matrix output, golden hashes, property-test output, fixture provenance, live-tier notes where applicable, and adversarial review of any recorded exclusions.

## Explicit exclusions

This ticket does not require cloud credentials in every push CI run; fixture-backed CI is acceptable when live-tier coverage is separately recorded.

## Progress and notes

- 2026-07-08: Opened as P2 workstream owner from the directive.
- 2026-07-08: Split first executable child `.10x/tickets/done/2026-07-08-p2-ws-i1-friction-regression-registry.md` to map the eighteen P2 frictions to existing tests or explicit open coverage gaps before implementation lanes start closing.
- 2026-07-09: Friction registry recorded in `.10x/evidence/2026-07-08-p2-friction-regression-registry.md`. Initial capture classified all eighteen directive frictions as open P2 coverage obligations, with partial primitive or negative coverage named where source/tests supported it.
- 2026-07-09: B1 closed the direct declarative-vocabulary guard for friction 3 through focused `cdf-declarative` tests. WS-I closure still requires replacing the remaining open-owner rows with actual regression tests/conformance scenarios or recorded exclusions.
- 2026-07-09: D1 added local modest-N primitive coverage for friction rows 8 and 9: deterministic per-file partitions and preview/open path parity inside `cdf-declarative`, plus live local-file golden reruns. WS-I still owes S2/S8 conformance coverage for manifest incrementality, remote/public globs, no-op reruns, and parity across source archetypes.
- 2026-07-09: B3 added focused local Parquet reader coverage for friction rows 4 and 5: declared-schema reads now reconcile physical Parquet types through the shared model, materialize `int32 -> int64` and `float32 -> float64`, fail closed for lossy narrowing, and preserve the undeclared physical path. WS-I still owes conformance-level S1/S2/S8 coverage and widening-lattice property tests.
- 2026-07-09: Split executable child `.10x/tickets/2026-07-09-p2-ws-i2-preview-run-parity-and-golden-path-matrix.md` for P2 scenario registry and preview/run parity matrix scaffolding.

## Blockers

Depends on implementation slices as they land; no blocker for harness shaping.
