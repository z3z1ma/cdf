Status: active
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-08-p2-ws-g-source-diagnostics-deep-validate.md
Depends-On: .10x/specs/data-onramp-conformance.md, .10x/tickets/done/2026-07-09-p2-ws-a10e-incompatible-observation-quarantine.md

# P2 WS-G3 S6 drift verdict rendering

## Scope

Carry terminal schema-observation quarantine verdicts from the engine plan into the project run report and P1 CLI rendering so a successful governed run names the drifting file/observation, exact field path and types, rule, policy, and remediation options without rereading package JSON in the CLI.

## Acceptance criteria

- Project run reports expose typed terminal schema verdict summaries sourced from the plan/runtime authority.
- JSON and human run output name the observation/file, field path, physical and baseline/constraint types where recorded, rule id, and policy.
- Rendering explains the applicable fixes: widen/refresh the schema when compatible, explicitly enable a governed coercion when available, or keep the file quarantined under freeze/evolve policy.
- Successful quarantine remains a successful run and receipt/checkpoint semantics are unchanged.
- S6 becomes a standalone deterministic covered scenario.

## Explicit exclusions

Changing drift admission policy, reconstructing row-level residuals, or parsing quarantine artifacts back into authority in the CLI.

## Evidence expectations

Exact CLI JSON/human assertions over an incompatible multi-file fixture, unchanged package quarantine evidence, full affected tests, clippy, and severity-focused review.

## Blockers

None.

## Progress and notes

- 2026-07-10: Opened after S1/S2/S3/S8 promotion left S6 pending solely on operator rendering. The design keeps the plan as authority and avoids a CLI package-artifact side channel.
