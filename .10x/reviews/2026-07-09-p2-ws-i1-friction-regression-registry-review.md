Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Target: .10x/tickets/done/2026-07-08-p2-ws-i1-friction-regression-registry.md
Verdict: pass

# P2 WS-I1 friction regression registry review

## Target

Review of `.10x/tickets/done/2026-07-08-p2-ws-i1-friction-regression-registry.md` and `.10x/evidence/2026-07-08-p2-friction-regression-registry.md`.

## Findings

- Pass: the registry lists all eighteen P2 frictions from the directive.
- Pass: every row names current coverage limits and an owning P2 ticket/workstream; no row is falsely marked complete.
- Pass: the registry distinguishes primitive/negative coverage from P2 acceptance coverage, especially for Parquet reading, `FileManifest`, normalizer primitives, REST HTTP transport, and append behavior.
- Pass: the registry is linked from the P2 parent and WS-I progress notes.
- Pass: no conformance code was changed, so omitting `cargo test -p cdf-conformance --locked` is acceptable for this record-only slice.
- Minor, accepted: row 3 references dirty B1 edits observed during the worker's final status. That is a time-bound observation, but the row still correctly refuses to treat those edits as coverage until B1 records verification.

## Verdict

Pass. WS-I1 is closed as a registry/coverage-map slice only. It does not close any P2 golden path or workstream.

## Residual risk

Future tickets must update the registry or cite it when they replace "open P2 owner" rows with actual regression tests. The registry can go stale if later implementation tickets close without repairing their row.
