Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Source extension runtime contract

## Purpose and scope

This specification governs source registry/config compilation, runtime resolution, discovery/product hooks, extended capabilities, dependency isolation, and source conformance. Resource execution semantics remain governed by kernel/resource and P2/P3 focused specs.

## Registry and configuration

Registration MUST reject duplicate ids/schemes/kinds and noncanonical versions/schema hashes. Registry ordering cannot affect resolution, generated schemas, plans, or package identity.

The common config envelope MUST remain strongly typed. Driver options are raw only until the selected driver validates them against its versioned source/resource schema and compiles typed internal values. Unknown fields, type errors, secret values where references are required, common-field collisions, and unsupported source/resource combinations fail with exact location/remediation.

Generated JSON Schema MUST combine common authority with installed driver fragments and remain deterministic. A driver schema change that affects accepted config/defaults is versioned and lock/diff visible. Driver defaults are canonical compiled values, not implicit runtime behavior.

## Compiled plan and resolution

A compiled source/resource plan MUST include driver id/version/schema hash, canonical redacted option artifact/hash, secret references only, descriptor/capabilities, physical plan payload/hash, discovery snapshot/manifest authority, and normalization/schema policy references. Plans are serializable and validate before source contact.

Resolution MUST verify registry driver/version compatibility and option/plan hashes before resolving secrets/contacting the source. It receives injected services and returns `dyn QueryableResource`. Concrete source/parser/transport/database types cannot cross into project, CLI, or generic scheduler APIs.

Python, subprocess, and future WASM producers MUST adapt through `.10x/specs/foreign-stream-interop.md` and then this ordinary source/resource contract. Their concrete runtime types and row/protocol representations cannot cross into generic planning or execution.

## Capabilities and scheduling

Capabilities MUST truthfully declare minimum/maximum poll/decode working sets, max/useful concurrency, rate/quota policy, pause/spill behavior, retryable error classes, idempotent/reopen/resume semantics, attestation strength, retry granularity, ordering, estimates, pushdown fidelity, boundedness, and telemetry version.

Generic planning/scheduling MUST consume these declarations and reject unsafe combinations before run. A source driver cannot create an unaccounted pool/semaphore/retry loop that competes with the execution host. Rate limiting may maintain driver-owned protocol state but admission/timers/cancellation are injected and observable.

Partition/source positions and checkpoint scope remain typed kernel authority. Retry attempts and timing are nonidentity evidence; successful rows/positions/attestations must be identical. Snapshot/content identity changes fail/replan according to source policy rather than silently mixing versions.

## Discovery and product hooks

Drivers MUST provide no-mutation inspection and bounded discovery where their source can expose schema/data. Discovery returns shared observations; it cannot write its own lock/package format. `cdf add` may ask the driver for candidate resources/cursors/keys but labels evidence/suggestions under common UX rules.

Deep validation, preview, plan, and run MUST call the same compiled driver/options/physical semantics. Doctor probes are typed, redacted, bounded, and driver-owned; generic rendering does not match source ids.

## Dependency and extension law

Source implementation crates MAY depend on neutral runtime/kernel/memory and their own protocols/formats. They MUST NOT depend on project, CLI, destination implementations, or sibling sources. Shared transports/formats live behind their registries rather than copied source helpers.

`cdf-project` MUST NOT depend on `cdf-source-*`; generic CLI command modules MUST NOT import source implementations. Standard composition is one module. A new source requires no generic compiler/discovery/run/preview/doctor/conformance assertion edits.

## Conformance

Shared laws MUST cover config/schema generation, inspection, discover/pin/diff, add dry/write, deep validation, preview/run parity, projection/filter/limit fidelity, partitions/positions/attestation, retries/identity change, rate/backpressure, memory declaration, cancellation, redaction/egress, jobs invariance, package/replay/commit gate, and doctor.

A mock external source with custom options, discovery, retry/rate limits, and partitions MUST pass by registry/fixture addition only. First-party file, REST, and Postgres sources pass the same laws. Architecture tests enforce dependency/import/match boundaries.

## Explicit exclusions

This spec does not define dynamic plugins, a universal SQL dialect, destination semantics, distributed scheduling, or specific future source catalog entries.
