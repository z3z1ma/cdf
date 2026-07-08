Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-07-cli-contract-registry-freeze-test.md
Verdict: pass

# CLI contract registry freeze/test review

## Target

Review the implementation of `cdf contract freeze` and `cdf contract test` over the `cdf.lock` contract snapshot registry.

## Assumptions Tested

- `cdf.lock` is the correct project-local contract snapshot registry for this slice, per `.10x/decisions/contract-freeze-lockfile-registry.md`.
- The optional `contract` argument is a resource-id selector, not an independent contract-file name.
- The CLI should delegate snapshot semantics to `cdf-project`, not implement ad hoc hashing in `cdf-cli`.
- `contract show` must remain project-free.
- Drift detection is snapshot-level here; row-level quarantine and fixture execution are explicitly excluded.

## Findings

No blocking findings remain.

Resolved during review:

- `generate_lockfile` initially still produced partial descriptor-only contract snapshots when no explicit snapshot map was supplied. That would have made `cdf diff schema` report false drift after a successful `cdf contract freeze`. The closure patch made generated lockfiles compute full contract snapshots by default and added a CLI regression that freezes, tests, and then confirms `diff schema` reports an empty diff.

Checked risk areas:

- CLI architecture: `commands.rs` remains a one-line dispatcher change; `contract_command.rs` owns only project loading, file write, and output shaping; snapshot computation and comparison live in `cdf-project`.
- Registry determinism: snapshot maps use `BTreeMap` ordering, selected resources are sorted by resource id for project-scope operations, and tests assert stable hash-shaped fields.
- Existing lock preservation: focused project tests prove existing dependency tuple and destination sheets are preserved when freezing into an existing lock.
- Fail-closed behavior: tests cover missing `cdf.lock` and missing selected resource snapshot.
- Drift observability: tests cover schema and validation-program hash drift details.
- Secret handling: command implementation compiles resources and writes hashes; it does not resolve secrets or print resource secret values. Source-only Gitleaks scans passed.
- Supply chain: no manifest changes occurred; scanners still show only the already-ratified `paste` advisory and existing duplicate Arrow-major warning.

## Verdict

Pass. The implementation satisfies the ticket acceptance criteria, stays within the ratified `cdf.lock` registry decision, and preserves adjacent lockfile/diff behavior after the review fix.

## Residual Risk

This slice intentionally does not execute row-level contract fixtures or write quarantine artifacts. That is not a closure gap because it is excluded by the ticket and owned by the contract-depth program. Full-history Gitleaks still reports two historical Python-path findings; they are outside this change and triaged by `.10x/tickets/done/2026-07-08-historical-gitleaks-findings-triage.md`.
