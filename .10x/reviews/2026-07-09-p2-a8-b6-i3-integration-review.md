Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Target: A8/B6/I3 working-tree implementation and .10x/tickets/done/2026-07-09-p2-ws-a8-autopin-lockfile-no-pin.md, .10x/tickets/done/2026-07-09-p2-ws-b6-json-family-observed-reconciliation.md, .10x/tickets/done/2026-07-09-p2-ws-i3-matrix-friction-reconciliation.md
Verdict: pass

# P2 A8, B6, and I3 adversarial integration review

## Findings

The first review failed closure on four findings:

1. Critical: ordinary commands re-probed and silently refreshed existing pins, violating infer-once governance.
2. Significant: source-controlled Arrow metadata could inject or corrupt identity-bearing coercion evidence.
3. Significant: JSON prefiltering blocked allowed decimal-string parsing and failed to localize fractional numeric drift.
4. Minor: I3's test hardcoded `open` although `active` and `blocked` are valid owner states.

Repairs made locked snapshot hydration authoritative, made snapshot reconstruction/path/version/hash/reference validation fail closed, moved exact coercion evidence through a dual-channel equality gate with structural validation, made the JSON scalar prefilter policy-aware, localized fractional drift, and made the owner test transition-safe.

A second review found one remaining significant gap: a syntactically valid header-only plan could still enter package evidence. The final repair now requires the reserved Arrow schema metadata and internal batch header to both be present and decode to exactly the same structurally valid plan. A fabricated header-only `Extra` plan is rejected.

The final targeted re-review passed with no findings. Parent inspection found no spec drift in the repaired boundaries, and the final 765-test workspace run plus strict lint/security/dependency/coverage profiles passed.

## Assumptions tested

- Existing pins remain the execution truth until explicit refresh.
- Missing, inconsistent, wrong-version, wrong-resource, or wrong-path snapshot artifacts fail closed.
- Source-carried Arrow/Parquet metadata cannot become package coercion evidence by itself.
- A public batch header cannot become evidence without matching reserved reconciliation metadata.
- Exact legitimate widened/lossy/extra decisions still survive into package artifacts.
- JSON coercion allowances do not silently broaden strict default behavior.
- Registry maintenance cannot promote pending golden paths or point at terminal owners.

## Verdict

Pass. All actionable findings were repaired and regression-tested. A8, B6, and I3 may close.

## Residual risk

The source connector implementation remains part of CDF's trusted code base; the new evidence checks prevent source-data metadata and one-channel producer claims from becoming evidence, but they are not a cryptographic attestation system. Public live-data and remaining P2 workstreams are outside this batch and remain owned by the open program graph.
