Status: done
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-08-p2-ws-i-conformance-parity-friction-suite.md
Depends-On: .10x/specs/data-onramp-conformance.md, .10x/knowledge/runtime-conformance-throughput-rule.md

# P2 WS-I2 preview/run parity and golden-path matrix foundation

## Scope

Create the P2 conformance matrix scaffolding for golden paths S1-S8 and the preview/run parity law, initially marking unsupported cells as explicit exclusions and wiring the cells that current implementation can honestly prove.

## Acceptance criteria

- Conformance has a named P2 data-onramp module or matrix with S1-S8 scenario ids.
- Each scenario records status as covered, excluded with rationale, or pending on a named implementation ticket.
- Preview/run parity is asserted for current local file, REST fixture, and Postgres table resources where the runtime supports both paths.
- The eighteen-friction registry maps closed implementation slices to actual test names and keeps open rows tied to active tickets.
- The harness shape makes future E2/H2/D3 additions additive rather than a rewrite.

## Evidence expectations

Conformance test output, scenario registry snapshot, friction mapping evidence, adversarial review of exclusions, and normal quality gates for conformance changes.

## Explicit exclusions

This ticket does not require live public network, cloud credentials, final S1/S2 session recording, or all S1-S8 cells green.

## Progress and notes

- 2026-07-09: Opened because P2 closure cannot happen with implementation-only tests; conformance must own the laws as the primitives land.
- 2026-07-09: Added `crates/cdf-conformance/src/run_matrix/data_onramp.rs` as the P2 data-onramp matrix foundation. It names S1-S8, keeps S1-S7 pending or partially evidenced against active tickets instead of finalizing P2, records one explicit deterministic-fixture/live-network exclusion, maps all eighteen frictions to closed test names and/or active owners, and adds an S8 preview/run parity law for the currently supported local file, REST fixture, and Postgres table archetypes.
- 2026-07-09: Parent integration fixed the concurrent file-runtime compile blocker and ran `cargo test -p cdf-conformance p2_ --locked` plus `cargo test -p cdf-conformance run_matrix --locked`; both passed. Closure evidence is `.10x/evidence/2026-07-09-p2-ws-a7-d3-i2-batch.md`; closure review is `.10x/reviews/2026-07-09-p2-ws-a7-d3-i2-batch-review.md`.

## Blockers

None for the matrix foundation. Final P2 S1-S8 closure remains owned by later workstream tickets.
