Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Source driver registry and resource-plan boundary

## Context

The kernel resource traits are generic, but declarative/project discovery and runtime construction enumerate three source kinds. Source-specific config and plans occupy shared compiler structs. P3 would otherwise add format/parallel/retry/performance behavior to those match trees and make every new source a cross-cutting edit.

## Decision

`cdf-runtime` defines a runtime-neutral object-safe `SourceDriver` and explicit deterministic `SourceRegistry`. A driver declares stable source id/version, URI schemes/kinds, source/resource option schemas, secret/egress needs, no-contact inspection, discovery/add/doctor hooks, compile/resolve operations, and a source capability sheet.

The existing kernel `ResourceStream` and `QueryableResource` remain the hot execution boundary. A resolved driver returns one or more object-safe queryable resources; generic engine/project execution never receives source-specific types.

Tier-0 parsing uses a common typed envelope plus driver-owned options:

- common source fields: name, `kind`, description/tags, and reserved framework metadata;
- common resource fields: id/source, schema mode/reference/hints, contract/trust, keys/cursor/disposition/dedup, partition/freshness, type allowances, and reserved framework metadata;
- every remaining source/resource field is a canonical raw TOML/JSON value validated and compiled by the selected driver schema.

This preserves ergonomic top-level `connection`, `table`, `path`, `glob`, `base_url`, `records`, and future fields without a closed Rust enum. Reserved/common collisions and unknown driver options fail. The published standard JSON Schema is generated from the first-party registry; external embedders can generate their registry schema. Runtime parsing is registry-driven, not limited to that standard catalog.

Driver compilation returns a canonical `CompiledSourcePlan`/resource artifact with driver id/version/schema hash, redacted canonical options, secret references, `ResourceDescriptor`, extended capabilities, physical plan payload/hash, and discovery authority. Opaque payload semantics belong to the driver, but generic identity/version fields are inspectable and lock/package/diff evidence. Semantic driver upgrades change plan authority visibly.

Runtime resolution receives only generic secret/egress/byte-source/execution/memory/state context through injected services. Drivers do not construct runtimes or checkpoint stores and cannot commit destination/checkpoint state. Source planning/open/attestation/retry remains upstream of the package/commit gate.

Source capability sheets extend current resource capabilities with plan/partition/decode working sets, max/useful concurrency, rate/quota/backpressure, retry/idempotent-read/resume semantics, snapshot/content attestation strength, partition/unit retry granularity, ordering, pushdown fidelity, boundedness/watermark needs, and driver telemetry version. Generic scheduling joins these declarations without source-name matches.

Discovery is a driver compiler stage returning the shared schema observation/manifest/snapshot model. `cdf add`, schema discover/pin/show/diff, validate/deep, preview, plan, run, inspect, and doctor dispatch through the registry. Preview/run parity and first-use pinning are generic laws.

First-party implementation crates are dependency-isolated: file source (transport plus format registries), REST/HTTP source, and Postgres/SQL source. Postgres source code MUST NOT live in or require `cdf-dest-postgres`. The CLI is the standard composition root; project depends only on neutral runtime/source plan artifacts. Adding a source edits its crate, composition/catalog/schema generation, and one conformance fixture—not generic project/CLI/compiler/discovery code.

## Alternatives considered

- Keep the enum because only three sources exist: rejected because enterprise breadth and P3 scheduling already expose the proliferation.
- Free-form unvalidated TOML maps: rejected because editor validation, deterministic compilation, and early diagnostics would regress.
- Put source driver traits in kernel: rejected because config/secret/HTTP/format/discovery product types do not belong in the calculus kernel.
- Use DataFusion `TableProvider` as the driver API: rejected because discovery, state positions, quarantine, attestation, rate/retry, and source lifecycle are broader.
- Linker auto-registration/dynamic ABI: rejected for deterministic embedding/audit reasons; explicit registry composition is sufficient.

## Consequences

Declarative closed enums/plans become compatibility migration inputs and then disappear from generic compilation. Source crates own their dependencies and schemas. P3 scheduler/format/remote lanes depend on SX1 rather than encoding current sources. A source-extension conformance catalog becomes a permanent architecture gate.
