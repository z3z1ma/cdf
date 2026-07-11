Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Destination extension runtime contract

## Purpose and scope

This specification governs the compile-time/runtime boundary for adding destinations, registry composition, inspection, package-aware planning, streaming ingress declarations, and automatic conformance ownership. Destination protocol semantics remain governed by `.10x/specs/destination-receipts-guarantees.md`; crate layering remains governed by `.10x/specs/architecture-layering-runtime.md`.

## Dependency and ownership rules

`cdf-runtime` MUST depend on no DataFusion implementation, concrete destination, engine, or product crate and MUST expose no concrete driver type. `cdf-project` MUST depend on `cdf-runtime` and MUST NOT depend on `cdf-dest-*`. Destination crates MAY depend on `cdf-runtime` and lightweight `cdf-memory` to implement driver/runtime and accounted-ingress traits without inheriting the DataFusion build graph.

Destination runtimes MUST receive run-scoped executor, cancellation, memory, telemetry, and blocking-lane services through the injected host contract in `.10x/specs/execution-host-structured-runtime.md`. They MUST NOT construct an async runtime, call a blocking executor around async work, or make generic orchestration identify a concrete destination to schedule it.

The CLI MUST construct the first-party registry in one composition module. Generic CLI commands MUST resolve, inspect, plan, run, replay, resume, and doctor destinations through that registry. A concrete destination reference outside composition or an adapter-specific diagnostic module is a stop-line finding.

## Driver contract

A destination driver MUST declare stable schemes and identity and MUST provide:

- no-mutation inspection yielding its sheet, sheet artifact/hash, display description, identifier rules, bulk/streaming declarations, and typed health probes;
- resolution from URI plus generic project/target/secret context into an object-safe destination runtime;
- driver-owned parsing of destination-specific locator/target details;
- package-aware commit preparation, correction, verification, and readback capabilities required by its sheet.

Registration MUST reject empty, malformed, or duplicate schemes deterministically. Inspection and resolution errors MUST retain redaction and the shared error taxonomy.

## Performance declarations

The sheet/runtime description MUST distinguish:

- durable segments can be streamed as soon as their identity is known;
- finalized package metadata is required before destination mutation;
- pre-finalization payloads are finalized-package-only or staged under `.10x/specs/streaming-destination-ingress.md`, never treated as committed package writes;
- single-writer versus concurrent segment ingestion;
- maximum useful in-flight segment/byte concurrency;
- bounded staging owned by the P3 memory ledger;
- measured bulk path identity and throughput evidence version.

These declarations are plan evidence. Generic runtime scheduling MUST join them with source/executor/memory capabilities without matching destination names.

Bulk path schema eligibility, bounded batch input, fallback/restart, tuning, and physical evidence MUST follow `.10x/specs/destination-bulk-path-runtime.md`. A sheet's bulk declaration is executable conformance authority, not an informational string.

## Conformance law

The conformance catalog entry supplies only fixture/environment construction and declared exclusions. Shared conformance discovers the runtime description and runs every applicable law. A new destination MUST NOT require edits to generic assertion, replay, crash, correction, or jobs-invariance logic.

Permanent architecture tests MUST prove:

- `cdf-project` has no dependency/import on `cdf-dest-*`;
- generic CLI command modules have no concrete destination imports;
- generic conformance engine modules have no destination-name match arms;
- a mock external driver can register, inspect, lock, plan, settle, replay, and doctor without editing shared code;
- registry order does not affect resolution, plan artifacts, or package identity.

## Acceptance scenario

Given a fourth destination crate implementing the runtime and kernel contracts, when it is registered at the composition root and added to the conformance fixture catalog, then project lock/plan/run/replay/resume/doctor and every applicable conformance law work without modifying `cdf-project` or generic CLI/conformance modules.

## Explicit exclusions

This spec does not implement a dynamic plugin ABI, linker inventory, registry service, destination marketplace, or distributed scheduler. Explicit compile-time composition is intentional.
