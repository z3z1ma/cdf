Status: open
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-09-p2-ws-a10-multi-file-schema-discovery-pin.md
Depends-On: .10x/specs/sampled-schema-discovery-coverage.md, .10x/tickets/2026-07-09-p2-ws-a10a-discovery-manifest-artifact-budget.md, .10x/tickets/2026-07-09-p2-ws-a10b-aggregate-schema-join-core.md, .10x/tickets/2026-07-09-p2-ws-a10c-exhaustive-local-binary-discovery.md

# P2 WS-A10g explicit sampled binary discovery

## Scope

Add explicit `sample_files = N` discovery coverage to the format-neutral discovery-set orchestrator using `stratified-hash-v1`. Wire declarative validation, plan/snapshot/package evidence, CLI rendering, and local/transport-fixture Parquet/Arrow IPC behavior without changing exhaustive defaults.

## Acceptance criteria

- `sample_files` is positive, explicit, schema-validated, and absent by default.
- Candidate selection implements every edge and canonical score/stratum rule in `.10x/specs/sampled-schema-discovery-coverage.md`.
- `M <= N` records exhaustive coverage; `M > N` records sampled coverage and exact probed/unprobed entries.
- Selection precedes scheduling and is invariant to enumeration permutation, concurrency, and probe completion order.
- Budgets fail selected probes without substitution or membership changes.
- Selected incompatible schemas fail initial pin with the complete selected report; unprobed runtime drift flows to compiled residual/quarantine verdicts rather than mutating the pin.
- Discover/pin/diff/no-pin/auto-pin/preview/run render coverage and counts consistently in human and JSON output.
- Legacy exhaustive snapshot/manifest bytes remain stable where version/optional-field rules require.

## Evidence expectations

Selector unit/property tests, canonical manifest goldens, large-N fixture runs under varied budgets/concurrency, no-write failure tests, sampled pin/runtime package inspection, exhaustive compatibility regressions, and adversarial review.

## Explicit exclusions

No adaptive/statistical sampling, confidence estimates, row sampling inside text files, source-format-specific selector, promotion execution, or distributed scheduler.

## Progress and notes

- 2026-07-10: Opened after exact selector ratification. This child follows the exhaustive orchestrator so both coverage modes share one candidate/probe/aggregate model.

## Blockers

Depends on A10a/A10b/A10c.
