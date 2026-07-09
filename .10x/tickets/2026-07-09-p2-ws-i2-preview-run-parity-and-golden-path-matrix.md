Status: open
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

## Blockers

None for the matrix foundation.
