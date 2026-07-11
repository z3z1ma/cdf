Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: commits edc8468e, daff44b6, fa1b8092, 02420904 and .10x/tickets/done/2026-07-08-p1-product-ws7-python-front-door.md
Verdict: pass

# P1 Python front-door closure review

## Findings

- **Significant, resolved:** the initial product integration added a closed `CliProjectRunSource` enum and Python-specific branches to run, plan, preview, and inspect. That violated `.10x/knowledge/source-destination-extension-invariant.md`. Commit `fa1b8092` replaced it with boxed `QueryableResource`, moved adapter dependency preflight onto `ResourceStream`, and centralized concrete construction at the CLI adapter-resolution boundary. Generic orchestration and commands no longer change when a same-contract source is added.
- **Significant, resolved:** a DuckDB-only happy path did not prove the WS7B destination claim. Commit `02420904` registered Python in the permanent conformance matrix and proved all supported disposition/destination cells, duplicate redrive, artifact replay, package verification, receipt verification, and checkpoint gating.
- **Significant, resolved:** dlt source expansion tried to execute unselected and `skip` resources. Commit `daff44b6` filters both before materialization and proves it with imported decorators whose bodies raise if invoked.
- **Minor, resolved:** generated error reference drift from `CDF-PYTHON-RESOURCE` was regenerated.

## Assumptions tested

The review tested path containment, exact resource identity, missing interpreter remediation, plan/inspect row-callable non-execution, preview write isolation, general-spine ownership, source-free replay, destination capability exclusions, deterministic fixture identity, free-threaded doctor policy, and dlt destination non-delegation.

## Residual risk

The 3.14t job cannot be executed on the local host. The checked-in matrix is strict and compares artifacts in a dependent job, making the risk observable on GitHub. No critical or significant implementation finding remains open.

## Verdict

Pass. The work meets the active Python front-door specification and the source/destination extension invariant.
