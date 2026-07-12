Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Target: .10x/decisions/datafusion-analysis-scheduling-identity-boundary.md, .10x/specs/datafusion-currency-bridges.md, .10x/tickets/2026-07-12-p3-ws-j-datafusion-currency-bridges.md
Verdict: pass

# P3 DataFusion currency graph review

## Target

The DataFusion identity boundary, bridge specification, WS-J parent/children, and integration updates.

## Assumptions tested

- Whether the new guardrail conflicts with the active deep-DataFusion decision.
- Whether memory, format, transport, observability, validation, or portable-task work was duplicated.
- Whether DataFusion types could leak into kernel/extension contracts.
- Whether pruning, expression optimization, or physical-plan rewrites could silently change package identity or replay.
- Whether optional DataFusion datasource use could displace the native primary codec path without evidence.

## Findings

No critical or significant unresolved finding.

The boundary is compatible with deep DataFusion integration because native CDF operators may be DataFusion plan nodes; DataFusion schedules and measures them without owning their identity semantics. A2 is reused as completed memory authority. J2 adapts G1 rather than inventing transport credentials. J4 extends the existing observability owner. J5 composes with WX1 instead of adopting Ballista serialization as canonical authority. J6 is an audit-only gate and cannot replace primary codecs or kernels.

The specification requires plan-time optimizer output to be recorded, conservative pruning under uncertainty, and pruned/unpruned equivalence. These close the primary determinism and soundness risks.

## Verdict

Pass. The graph adds the missing DataFusion interoperability work without weakening native rooflines, package identity, extension boundaries, or portable execution authority.

## Residual risk

DataFusion API/version details, ADBC exposure shape, and Substrait fidelity remain implementation evidence questions owned by J1-J6. They are intentionally not guessed in this activation.

