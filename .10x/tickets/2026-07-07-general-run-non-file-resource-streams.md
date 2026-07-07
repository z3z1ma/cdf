Status: blocked
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-general-run-orchestrator.md
Depends-On: .10x/specs/run-orchestration-ledger.md, .10x/specs/package-lifecycle-determinism.md

# Add non-file resource streams to the general run orchestrator

## Scope

Extend `cdf-project` general run orchestration beyond declarative local file resources so REST resources and table-backed Postgres SQL resources can be run safely when deterministic runtime dependencies are supplied.

Owns:

- A project-run resource input shape that can accept supported `ResourceStream` implementations with their required runtime dependencies.
- State-delta construction for non-file `SourcePosition` values.
- Fail-closed validation for unsupported source/runtime combinations before package, destination, or checkpoint mutation.
- Tests for deterministic REST and table-backed Postgres SQL resource streams where existing lower-layer harnesses make them safe.

## Acceptance criteria

- The orchestrator can execute a supported REST `ResourceStream` without using `CompiledResource::open` for REST, which currently fails by design.
- The orchestrator can execute a supported table-backed Postgres SQL `ResourceStream` with explicit runtime dependencies.
- State-delta artifacts and checkpoint commits use ratified source-position semantics for each supported source kind.
- Recovery after package finalization or durable receipt does not contact the source.
- Unsupported or missing source runtime dependencies fail before mutation.

## Blockers

- `ProjectRunRequest` currently accepts `&CompiledResource`; `CompiledResource::open` only executes file resources and intentionally returns an error for REST and SQL resources.
- The state-delta builder currently requires `SourcePosition::FileManifest` evidence and rejects other source positions. REST cursor/page-token and SQL cursor semantics are not yet ratified for this orchestrator ticket.
- Runtime dependency ownership for REST transports/secrets and SQL secret providers is not represented in the project-run request.

## Explicit exclusions

No live external HTTP credentials, no arbitrary SQL query execution, no scheduler/resident streaming, no CLI parsing.

## Evidence expectations

Run focused `cdf-project` tests, existing REST/SQL resource conformance tests where deterministic, `cargo clippy -p cdf-project --all-targets --locked -- -D warnings`, and workspace check.

## Progress and notes

- 2026-07-07: Blocked during `.10x/tickets/2026-07-07-general-run-orchestrator.md` continuation. Inspection found safe lower-layer `RestResource` and `SqlResource` wrappers exist, but the general project-run request and checkpoint artifact semantics do not yet carry the needed runtime dependencies or non-file source-position contract.
