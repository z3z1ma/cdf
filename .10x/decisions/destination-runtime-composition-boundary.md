Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Destination runtime and composition boundary

## Context

The first open-orchestrator decision successfully removed closed run/replay enums and genericized package settlement. It also allowed `cdf-project` to own destination adapter modules and a built-in registry. Current source proves that this permission leaves a concrete-driver dependency fan-out in shared product/runtime code:

- `cdf-project` depends directly on all three destination crates and imports their planning/private types into `runtime.rs`;
- built-in registration and `ResolvedProjectDestination` convenience constructors name each driver;
- lockfile sheet generation matches URI schemes and constructs concrete destinations;
- CLI context, doctor drift, and replay contain destination-specific types and branches;
- conformance has multiple concrete destination enums/factories rather than one reusable adapter catalog.

This is not a generic-orchestration correctness failure, but it makes “add one destination” a cross-cutting edit and would force P3 streaming/bulk semantics to proliferate in the wrong layer.

## Decision

Create a lower `cdf-runtime` crate that depends on kernel, contract, engine, package, and HTTP abstractions but on no concrete source or destination crate. It owns the object-safe project/runtime destination boundary currently in `cdf-project`: resolution context, driver registry, destination inspection, resolved runtime, package-aware preparation, correction hooks, and streaming-commit capability vocabulary.

Each destination crate implements and exports its own runtime driver adapter. `cdf-project` consumes an injected `cdf_runtime::DestinationRegistry` and MUST NOT depend on any `cdf-dest-*` crate. Generic project run, plan, replay, resume, promotion, lock generation, and package settlement consume only runtime/kernel traits and sheet data.

The CLI is the explicit first-party composition root. One focused module constructs the builtin registry by registering exported drivers. Outside tests, concrete destination crates may appear only in that composition module and destination-specific optional command adapters that cannot be expressed by the shared contract; such exceptions require a named capability gap and owner. Non-CLI embedders construct the same registry without importing CLI code.

The driver boundary MUST provide enough plan-time data that generic consumers never re-match schemes:

- registered schemes and stable destination identity;
- no-mutation inspection returning description, `DestinationSheet`, artifact/hash data, and typed doctor probes;
- URI/target parsing and secret resolution;
- runtime resolution and package-aware commit preparation;
- explicit commit-ingress capability (`stream durable segments` or `requires finalized package knowledge`) and bounded staging declarations;
- replay/correction/readback hooks already required by active contracts.

Lockfile generation uses driver inspection/sheet artifacts. Doctor renders typed probes. Replay target parsing belongs to the driver. `ResolvedProjectDestination::{duckdb,postgres,parquet_filesystem}` production conveniences are removed; tests use registry fixtures or `ResolvedDestination::new` only at adapter-unit boundaries.

Conformance owns a data-driven destination adapter catalog. Adding a destination adds one catalog entry with fixture authority and automatically receives the shared disposition, receipt, replay, crash, correction, and jobs-invariance laws. The conformance engine itself MUST NOT gain a new match arm for the destination.

## Alternatives considered

- Keeping adapters in `cdf-project` and centralizing more helper functions was rejected because the dependency inversion remains wrong and every destination still edits shared runtime code.
- Moving package-aware traits into `cdf-kernel` was rejected because it would contaminate the kernel with package/engine/secret resolution types.
- Runtime auto-registration through linker inventory was rejected because hidden global registration harms embedding, determinism, auditability, and WASM/distributed composition.
- A `cdf-builtin-destinations` crate was deferred. The CLI is already the binary composition root; another crate is justified only if a second first-party product binary otherwise duplicates the same list.
- Static generics were rejected for heterogeneous configured destinations and dynamic URI resolution. Vtable dispatch occurs at session/segment boundaries, not per row, and MUST be measured under P3.

## Consequences

This decision supersedes `.10x/decisions/superseded/project-destination-driver-registry.md` only where that decision allowed concrete built-ins inside `cdf-project`; its generic settlement invariants remain preserved.

P3 WS-A and WS-D depend on this boundary before shared streaming/bulk changes land. WS-L may proceed independently. Adding a destination should require: its destination crate and driver implementation, one explicit first-party composition registration, and one conformance fixture/catalog entry. Generic project, CLI command, lockfile, doctor, replay, and conformance-engine files remain unchanged.
