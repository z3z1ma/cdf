Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: .10x/tickets/done/2026-07-10-p2-ws-e4-transport-doctor-probes.md
Verdict: pass

# P2 WS-E4 adversarial review

## Findings

- Pass: doctor calls the runtime partition planner rather than duplicating cloud/provider logic.
- Pass: no path, URL, secret value, credential reference, or provider option is serialized in check details.
- Pass: a denied remote resource produces one failed check and does not suppress Python, destination, or ledger checks.
- Resolved, high: the full suite exposed that exhaustive evolving resources no longer admitted compatible widening/unions after the WS-E3 shared-discovery refactor. Runtime classification now admits compatible aggregate evolution only for exhaustive `evolve`; sampled discovery remains pinned and sends unseen/incompatible values to quarantine/residual promotion. Both laws have regression tests.
- Clarified: manifest-bearing snapshot identity and effective Arrow schema identity are intentionally distinct for HTTP just as they are for local multi-file discovery. The ad-hoc conformance assertion now enforces the universal model and requires exhaustive coverage evidence.

## Verdict

Pass. No critical or high finding remains.

## Residual risk

Doctor currently resolves the complete configured match set. Large-N listing scalability is owned by P3's transport overlap and partition-coalescing performance work; this does not alter correctness or write safety.
