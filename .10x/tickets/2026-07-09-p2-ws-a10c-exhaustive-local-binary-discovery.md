Status: open
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-09-p2-ws-a10-multi-file-schema-discovery-pin.md
Depends-On: .10x/tickets/done/2026-07-09-p2-ws-a10a-discovery-manifest-artifact-budget.md, .10x/tickets/done/2026-07-09-p2-ws-a10b-aggregate-schema-join-core.md, .10x/tickets/done/2026-07-09-p2-ws-a9-local-arrow-ipc-discover-run.md, .10x/tickets/done/2026-07-09-p2-ws-d5-binary-format-autodetection.md

# P2 WS-A10c exhaustive local binary discovery and pin lifecycle

## Scope

Replace the Parquet/Arrow single-candidate discovery gates with the generic discovery-set orchestrator for local files. Enumerate deterministically, probe every binary metadata block under the resolved per-executor budget, aggregate with A10b, persist A10a's sidecar, and expose the result through discover/pin/diff/no-pin/auto-pin.

## Acceptance criteria

- Multi-file local Parquet and Arrow IPC globs discover, pin, diff, and first-use auto-pin without narrowing to one file.
- Every matched candidate is probed; no result contains `unprobed` or sampled membership.
- Probe scheduling never exceeds the resolved 64 MiB per-file, 128 MiB in-flight, or 8-probe defaults; executor options can override and resolved values are recorded.
- Budget exhaustion fails before snapshot/lock writes and names the measured/allowed bytes and override/remediation path.
- First pin writes nothing if any initial file is malformed or schema-incompatible and reports every candidate verdict.
- Compatible widening, new/missing fields, nested schemas, metadata variance, deterministic order, no-change identity, add/remove/change manifest diff, and normalizer collisions are covered.
- The cardinality-one compatibility case produces the same artifact/evidence shape as a one-entry discovery set.
- Discovery does not compute runtime full-file SHA or read row/data pages; measured probe evidence proves the bound.
- Existing v1 pins hydrate unchanged. Ordinary commands verify/hydrate baseline authority before current-file observation, never rewrite it, and may reuse unchanged manifest probe evidence; non-file pinned resources retain existing no-probe behavior.

## Evidence expectations

Multi-file Parquet/IPC fixtures, measured-byte tests, budget/concurrency instrumentation, snapshot/manifest golden bytes, CLI no-write failures, legacy compatibility, full affected tests, and adversarial review for hidden full reads/single-file branches.

## Explicit exclusions

No mixed-schema package execution, nullable array materialization, effective-schema package stamping, file quarantine, remote Arrow, cloud transport, HTTP enumeration, text sampling, or preview traversal changes.

## Progress and notes

- 2026-07-09: Opened as the first I/O integration child after A10a/A10b.

## Blockers

None. A10a and A10b are complete.
