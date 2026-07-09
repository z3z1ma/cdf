Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Target: .10x/tickets/done/2026-07-08-p2-ws-f1-append-default-merge-key-error.md
Verdict: pass

# P2 WS-F1 append default and merge-key error review

## Target

Review of the F1 implementation and records for append defaulting, keyless append, explicit merge-key validation, and local scaffold key removal.

## Findings

- No critical or significant issues found.
- Minor residual risk: broader CLI S7 wording and conformance coverage are outside this child ticket. The code path now fails at declarative compile time, but command-specific rendering remains a later WS-F/WS-G/WS-I concern.
- Minor residual risk: jscpd still reports existing duplication in large test files. The F1 tests were tightened to avoid adding a new clone pattern, and the scoped duplicate count went down during review.

## Assumptions Tested

- `primary_key` should not imply `merge_key`: tested by the missing-merge-key failure case that still declares `primary_key = ["id"]`.
- Append should require no key metadata: tested by omitted and explicit append forms with no key fields.
- Successful merge declarations should remain possible: tested by explicit `merge_key = ["id"]`.
- Generated append scaffolds should not nudge fake keys: checked by removing the local scaffold primary key and rerunning the scaffold validation test.

## Verdict

Pass. Acceptance criteria are covered by tests and evidence. Remaining work is correctly outside F1 and stays with the broader WS-F and conformance tickets.
