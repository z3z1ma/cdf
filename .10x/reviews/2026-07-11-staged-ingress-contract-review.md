Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/decisions/destination-staged-ingress-final-package-binding.md, .10x/specs/streaming-destination-ingress.md, .10x/tickets/2026-07-11-p3-a1-staged-ingress-final-binding.md
Verdict: pass

# Staged ingress contract review

## Assumptions tested

- A package token could be predicted before finalization: false because identity includes outcome-dependent evidence.
- Staging acknowledgements could reuse receipt segment acknowledgements: unsafe because they would appear checkpoint-eligible before package binding.
- All staging must survive process loss: unnecessary and dishonest for invisible transactional ingestion; explicit resumable versus rollback/redrive capability preserves truth.
- Attempt identity could enter the plan/package: rejected because it would break deterministic jobs-invariance.

## Findings

No critical or significant shaping issue remains. The state boundary preserves package-hash idempotency, receipt verification, target invisibility, checkpoint gating, deterministic identity, and generic capability selection while allowing real I/O overlap.

## Verdict

Pass for activation after L5/DX1 dependencies.

## Residual risk

Long-lived database transactions can create MVCC/WAL pressure even when memory is bounded. Destination implementations must benchmark resumable staging tables against transaction-ephemeral rollback/redrive and declare the selected recovery/cost model in their sheet rather than adopting one universal database strategy.
