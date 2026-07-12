Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-c2-parallel-frontier-execution.md, .10x/specs/deterministic-parallel-scheduler.md

# C2 canonical partition-open frontier milestone

## What was observed

The production run path now admits a bounded canonical frontier of partition opens. Runtime scheduler resolution remains owned by `cdf-runtime`: typed source and destination capabilities, injected execution services, and configured policy resolve before project execution, and the engine consumes only the resulting effective-jobs authority. The engine contains no source name, destination name, transport scheme, adapter constructor, or duplicate ceiling arithmetic.

`FuturesOrdered` allows partition open/download setup to overlap while yielding streams in fixed plan ordinal order. The frontier holds at most effective jobs entries and drops pending futures on failure/cancellation. Terminal schema-quarantine partitions bypass payload open. Any global limit forces a one-entry frontier and replenishes only after the current partition proves the limit remains unsatisfied, preserving exact attempted-input and attestation authority.

## Procedure

- `cargo test -p cdf-engine operator_graph_compiles_from_capabilities_without_driver_name_dispatch --locked`
- `cargo test -p cdf-engine effective_schema_reuses_observation_across_partitions_and_attests_only_attempted_inputs --locked`
- `cargo test -p cdf-engine --locked`
- `cargo test -p cdf-cli run_command --locked`
- `cargo check --workspace --all-targets --locked`
- `cargo clippy -p cdf-engine -p cdf-project -p cdf-cli --all-targets --locked -- -D warnings`

The focused jobs-invariance test passed at jobs 1 and 4 with identical manifest identity and lineage and zero residual managed-memory bytes. The exact-limit observation test passed with one payload open and no extra attestation. The full engine suite passed 93 tests with five explicitly slow benchmark/stress probes ignored. Workspace all-target compilation and scoped strict Clippy passed.

The broad `cdf-project` test command was also sampled and reached 147 passes before failing obsolete direct-`CompiledResource` execution fixtures with `compiled declarations are not executable; resolve their typed source driver`. That known migration is owned by `.10x/tickets/2026-07-11-p0-sx1-source-extension-boundary.md` and is independent of scheduler propagation; no compatibility shim was added.

## What this supports or challenges

This supports a real production latency-overlap milestone and the architectural invariant that scheduler policy has one owner. It also supports jobs-invariant canonical release and exact global-limit input authority for the covered paths.

## Limits

Partition streams are still consumed through validation and segment assembly one canonical partition at a time. This evidence does not claim parallel transform execution, retry/reattest, explicit admission-permit acquisition per open, byte-bounded out-of-order outcome retention, atomic multi-unit file completion, or measured scaling. C2 remains open.
