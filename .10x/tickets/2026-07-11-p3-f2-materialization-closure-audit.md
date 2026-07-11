Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-f-constant-memory-guarantee.md
Depends-On: .10x/tickets/2026-07-11-p3-a5-streaming-operator-graph.md, .10x/tickets/2026-07-11-p3-a6-spillable-package-dedup.md, .10x/tickets/2026-07-11-p3-b13-native-format-matrix.md, .10x/tickets/2026-07-11-p3-d5-bulk-path-matrix.md, .10x/tickets/2026-07-11-p3-e4-package-io-envelope.md, .10x/tickets/2026-07-11-p3-g3-codec-download-decode-overlap.md

# P3 F2: production materialization and allocation-owner closure

## Scope

Generate/audit every production allocation/materialization across source/format/transport/engine/contract/package/destination/interop, remove residual whole-input/package/cardinality collections in scope, and map every native/child/metadata class to ledger/headroom/external staging evidence.

## Acceptance criteria

- No production input/package/listing/segment/provenance collection scales outside ledger/spill.
- Static architecture gates reject known eager APIs in production runtime paths.
- Every allocation class has one owner/classification and measured bound.
- Mock source/format/destination/child additions must declare memory behavior through conformance.

## Evidence expectations

Generated owner matrix, static scans/dependency graph, runtime owner telemetry, focused residual fixes, high-cardinality tests, and adversarial “hidden Vec/native allocation” review.

## Explicit exclusions

No unrelated product feature or performance tuning beyond closure blockers.

## Blockers

Depends on the runtime/codec/destination/package/remote materialization owners.

## References

- `.10x/specs/constant-memory-proof.md`
