Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: .10x/tickets/done/2026-07-09-p2-ws-g2-type-mismatch-diagnostics.md
Verdict: pass

# P2 G2 deep type diagnostics adversarial review

## Findings

- Significant, resolved: the first implementation classified every JSON-family discovery error as a warning. That could downgrade malformed JSON or an unreadable source, which is not a governed schema mismatch. Probe/decode failures now remain errors; only reconciliation failures after successful observation can become row-local warnings. A malformed-input regression test proves the boundary.
- Minor, resolved: the remote read helper exceeded the argument-count lint after policy propagation. Transport and spool policy now remain grouped behind the existing `FileRuntimeDependencies` abstraction instead of adding another loose parameter.
- Security, resolved: source-location rendering strips every query value and the fragment, independent of parameter naming. A focused test uses both a signed-looking and ordinary query value.

## Verdict

Pass. No unresolved critical, high, or significant findings remain. The implementation uses kernel resource policy and the shared reconciliation engine rather than format- or CLI-specific policy branches.

## Residual risk

Bounded JSON discovery can miss later drift by design; runtime contract evaluation remains authoritative and this is explicitly recorded in the governing decision/specification. Final source-archetype breadth remains in the WS-G/WS-I program graph.
