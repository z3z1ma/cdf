Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-07-p0-b2-generic-package-replay-recovery.md
Verdict: pass

# P0 B2 Generic Package Replay and Recovery Review

## Target

Current B2 diff for `cdf-project` generic package replay/recovery, destination runtime adapters, registry proof, and focused runtime tests.

## Assumptions tested

- Package verification still happens before any segment is fed to a destination session.
- Recovery with a durable receipt does not mutate the destination.
- Receipt validation goes through the kernel `DestinationProtocol::verify` trait method rather than destination-specific free functions.
- DuckDB, Parquet, and Postgres receipt source semantics remain compatible with existing public wrapper reports.
- Generic failpoint/stage hooks preserve the currently ratified crash windows.
- The mock proof uses driver registration/resolution rather than constructing a runtime directly.

## Findings

- Significant, resolved: The first worker patch did not satisfy the B2 mock registered destination acceptance criterion because the test directly constructed `MockProjectDestinationRuntime`. The repair added `ProjectDestinationRegistry`, `MockProjectDestinationDriver`, and tests that resolve runtimes through the registry before replay, recovery, and generic stop-before-destination-write injection.
- Minor, residual: `ProjectDestinationRegistry` is a minimal registry/factory. It proves the B2 replay/recovery boundary but does not yet provide built-in DuckDB/Parquet/Postgres URI resolution or project-config construction. B3 owns that.
- At review time, bare `cargo vet` failed on the ratified DataFusion git pin's `policy.audit-as-crates-io` posture while `cargo vet --locked` passed. That residual is now closed under `.10x/tickets/done/2026-07-08-cargo-vet-datafusion-git-policy-bare-command.md`.

## Verdict

Pass. The B2 acceptance criteria are supported by parent-observed tests, quality gates, registry proof, trait-level receipt verification, and wrapper delegation through the generic replay/recovery skeleton.

## Residual risk

External callers still use public compatibility wrappers and closed run enums. That is deliberate B3/B4 scope and must not be treated as Workstream B closure.

The remaining largest runtime hotspots are module-level totals in `runtime/replay.rs`, `runtime/destinations.rs`, and pre-existing artifact/orchestration code. B3/B4 must keep reducing destination/run branching rather than re-concentrating it.
