Status: active
Created: 2026-07-08
Updated: 2026-07-08

# Backfill Window Planner Command Contract

## Context

`VISION.md` Chapter 8.6 describes backfill as compiling date ranges into partition sets, and Chapter 13.4 makes scope part of checkpoint ownership. The active CLI spec requires `cdf backfill`, and `.10x/tickets/done/2026-07-07-cli-backfill-planner.md` owns the first implementation.

The current CLI surface validates an optional resource id and returns unsupported. `cdf run` already routes through the general run spine, but historical backfill needs an explicit bounded-slice contract before tests or code can encode defaults for dry-run, execution, destination selection, and checkpoint scope.

The implementation must also respect the project architecture constraint raised by the user on 2026-07-08: `cdf-cli` must not accumulate planner/orchestration semantics merely because command wiring is convenient. Backfill slice planning belongs in `cdf-project`; CLI owns parsing, rendering, and human-facing errors.

## Decision

CDF will implement `cdf backfill` as a bounded cursor-window planner with explicit execution opt-in.

The command contract is:

```text
cdf backfill RESOURCE --from CURSOR --to CURSOR --target TARGET [--execute] [--slice-size N]
```

`RESOURCE`, `--from`, `--to`, and `--target` are required. `--resource RESOURCE` remains accepted as an alias for the positional resource for parser consistency. If both are supplied, they must match.

The default mode is dry planning. Dry planning MUST NOT write package artifacts, destination data, checkpoint rows, or run-ledger events. It may load project configuration, compile plans, and perform the same destination dry-run planning already used by `cdf plan`.

`--execute` runs every planned slice through the existing general `run_project` spine. Each slice is one run attempt and therefore receives ordinary run-ledger events, package artifacts, destination receipt verification, and `CheckpointStore::commit` gating. Backfill MUST NOT directly mutate checkpoint state outside `CheckpointStore::commit`.

Each slice is a half-open interval over the resource cursor:

```text
cursor >= slice_start AND cursor < slice_end
```

The planner MUST emit explicit planned slices even when there is only one slice. Without `--slice-size`, the range produces one slice `[from, to)`. With `--slice-size N`, the first implementation supports only non-negative integer cursor literals and positive integer slice sizes; it splits the range into adjacent half-open numeric slices. Date/timestamp duration arithmetic is intentionally excluded until a calendar-aware parser is ratified or a dependency tuple already carries one.

Eligibility is fail-closed:

- the resource MUST declare a cursor;
- the cursor MUST NOT be `Unordered`;
- the resource MUST have cursor predicate support sufficient for the selected runtime to honor the generated predicates;
- file, page-token-only, log, CDC, Python, WASM, and subprocess resources are unsupported in this slice unless they already present as an eligible cursor-backed `QueryableResource`;
- residual filtering for timestamp/date cursor bounds is unsupported unless the resource pushes the predicates exactly or the engine grows typed timestamp/date predicate execution.

The planner MUST set the concrete checkpoint scope for each backfill run to `ScopeKey::Window { start: slice_start, end: slice_end }` before entering `run_project`, so historical slices cannot overwrite the live resource head. If the selected resource's capabilities do not allow window-scoped planning safely, the slice is rejected before source contact or checkpoint mutation.

Generated package and checkpoint ids MUST be deterministic, one path component, and derived from the resource id plus slice bounds. They are internal ids, not a semantic cursor encoding.

JSON output MUST include the mode, resource id, target, requested bounds, planned slices, generated filters, generated package/checkpoint ids, concrete scope, execution status, executed run ids, checkpoint ids, package pointers, and unsupported/skipped reasons.

## Alternatives considered

Execute by calling source-specific backfill helpers.

Rejected. It would recreate the specialized vertical slices the run-spine work just removed and would bypass shared run-ledger, receipt-verification, and checkpoint-gate behavior.

Infer the destination target from project or destination introspection.

Rejected for the first surface. The existing `cdf plan` and `cdf run` safety posture requires an explicit target, and backfill is mutating when executed.

Make execution the default and add `--dry-run`.

Rejected. Backfill is historical mutation with checkpoint consequences. Dry planning is the safer default and gives CI/tests a no-contact mode.

Implement date arithmetic by hand in the CLI.

Rejected. It would encode calendar semantics in the wrong layer and risk incorrect windows. Date/timestamp partitioning remains a follow-up once parser/dependency and typed predicate behavior are explicitly ratified.

Use the resource descriptor's existing `state_scope` unchanged.

Rejected. A resource-level scope would let a historical slice advance or collide with the live resource head. Backfill slices must commit under concrete window scopes.

## Consequences

The first backfill implementation is useful for bounded one-slice backfills and numeric cursor slicing, while failing closed for calendar partitioning rather than pretending to support it.

`cdf-project` gets a small reusable planner API for backfill slices. `cdf-cli` remains a thin command layer.

The command creates a clear future extension point for calendar-aware `--slice-size` values and source-specific eligibility expansion, but those extensions require records and tests before implementation.

Acceptance evidence must prove dry planning performs no package/destination/checkpoint/run-ledger writes and execution routes through `run_project` with concrete `ScopeKey::Window` scopes.
