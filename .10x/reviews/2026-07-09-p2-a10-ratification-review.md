Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Target: .10x/decisions/multi-file-discovery-aggregation-and-budget.md, .10x/specs/data-onramp-schema-intelligence.md, .10x/specs/data-onramp-file-sources-transports.md, .10x/tickets/done/2026-07-09-p2-ws-a10-multi-file-schema-discovery-pin.md
Verdict: pass

# P2 A10 ratification and decomposition review

## Findings

The first ratified draft risked reading 128 MiB as a universal production ceiling. The decision now makes all three numbers configurable, plan-recorded per-executor defaults. Distributed topology scales through workers and explicit executor options without changing semantic membership or enabling sampling.

Review also found a possible ambiguity with A8's pinned-authority repair. The final wording requires baseline verification/hydration before current-file contact, forbids ordinary commands from refreshing the pin, and classifies file listing/probing as runtime front-end observation. An `evolve` effective output remains a verdict against the immutable baseline. Non-file resources preserve their existing no-probe ordinary-command behavior.

The A10 ticket was too broad for execution. It is now a parent plan with independent A10a artifact/budget and A10b pure-join foundations, followed by A10c exhaustive local integration, A10d compatible runtime evidence, A10e gate-backed file quarantine/positions, and A10f conformance. Each child has one primary outcome, dependencies, exclusions, and evidence expectations.

## Assumptions tested

- The user explicitly ratified all eight aggregation decisions and accepted the numeric defaults subject to production-scale suitability.
- The defaults are resource-accounting controls, not data semantics; resolved values are evidence.
- No binary sampling path remains authorized.
- The one-file case cannot use a different artifact or aggregation contract.
- Spark/Flink, container, Azure/object-store, Python, and WASM integration remain adapters around canonical facts rather than dependencies in lower models.
- Snapshot v1 compatibility, package identity, receipt gate, and runtime exact `FileManifest` authority remain explicit child criteria.

## Verdict

Pass. The semantic contract is active and A10a/A10b are executable in parallel on the next inner-loop turn.

## Residual risk

The exact Rust executor-options API is mechanical design owned by A10a and must remain below CLI-specific configuration. Actual Spark/Flink schedulers, Azure transport credentials, Python/WASM parsers, and memory-ledger-derived defaults are future owners; A10 must preserve their seam but must not implement speculative platform bindings.
