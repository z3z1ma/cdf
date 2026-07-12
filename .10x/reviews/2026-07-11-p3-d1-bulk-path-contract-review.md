Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/tickets/done/2026-07-11-p3-d1-bulk-path-contract.md
Verdict: pass

# P3 D1 adversarial architecture review

## Target

The neutral destination bulk-path descriptor, preparation, bounded writer, fallback, and evidence contracts in `cdf-runtime`, plus first-party compatibility declarations.

## Findings

No critical or significant finding remains.

- Generic runtime does not branch on a destination name, protocol, path id, or Arrow field type. Drivers own path ordering and future eligibility decisions.
- The production bulk writer cannot receive a package-sized collection. `CommitBatch` retains the segment authority and memory ownership introduced by A5.
- Runtime fallback is fail-closed. A driver must return an attempt/path-matched proof of zero target visibility and external-staging cleanup, the descriptor must authorize rollback/full-redrive, and the replacement attempt id must differ.
- The chosen path and settings are excluded from package identity but convert to validated serializable run-event details. This preserves deterministic packages while making physical execution auditable.
- The first-party declarations are intentionally honest about current scalar/CSV/IPC compatibility paths rather than advertising the D2-D4 target state early.

## Verdict

Pass. D1 is a cohesive extension boundary suitable for new destinations. The concrete high-performance implementations and measured selection remain correctly isolated in D2-D5.

## Residual risk

Driver implementations could still publish descriptors that diverge from their live behavior. D5 owns cross-path conformance and measured falsification; this is downstream scope, not an unresolved D1 defect.
