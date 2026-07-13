Status: active
Created: 2026-07-12
Updated: 2026-07-12

# Lean CLI and package-contract build boundaries

## Context

A filtered `cdf-cli` test with zero seconds of test execution compiled for 5m24s under twelve Cargo jobs. The default `cdf-cli` graph contains 377 unique packages, 33 workspace crates, and the complete engine/database/codec/transport product. The neutral `cdf-runtime` graph contains 90 packages because it directly depends on the full filesystem/IPC/Parquet `cdf-package` implementation.

The active architecture already requires one explicit standard product composition root, DataFusion-free neutral runtime/extension contracts, driver-local source and destination implementations, and no compatibility obligation before first production. The CLI renderer decision explicitly permits a crate split when compile-time isolation is demonstrated. `.10x/research/2026-07-12-cargo-product-build-graph-audit.md` demonstrates it.

## Decision

Create two dependency leaves without creating parallel semantic authorities:

1. `cdf-cli-core` owns command grammar/argument models, help/completion/man generation, terminal policy, rendering documents/design language, and transport-neutral invocation output/error envelopes. It contains no command execution, project loading, engine/runtime construction, package I/O, state backend, network/database client, or concrete source/format/transform/destination code. The existing `cdf-cli` package remains the complete static first-party product composition root and owns the production `cdf` binary. It depends on `cdf-cli-core`; product command handlers return core rendering/output values. There is no compatibility re-export layer for moved internal modules.
2. `cdf-package-contract` owns the one canonical package artifact/replay model and the narrow capability contracts required to consume verified package facts and durable segment streams. It performs no filesystem I/O, IPC/Parquet encoding or decoding, hashing, archive writing, or artifact verification. `cdf-package` owns those implementations and depends on the contract leaf. `cdf-runtime` depends on the contract leaf and must not depend on or name concrete `cdf-package` readers/builders. Runtime remains authority for destination/staging lifecycle validation over verified facts supplied through the contract.

Tests follow ownership. Pure parser/help/artifact-generation/terminal/render/output assertions live with `cdf-cli-core`. Product dispatch, project, package, state, source/destination composition, and live integration assertions live with their product or conformance owner. Tests are moved, not duplicated. Fast CI and executor-focused commands target the smallest owner; complete product checks remain deliberate integration/slow gates.

The existing standard source registry from SX1 and destination registry from DX3/DX4 remain the only composition authorities in `cdf-cli`. This decision does not add another registry, plugin loader, global inventory, or provider factory. DataFusion adapter containment remains owned by WS-J/J6; this split only adds static graph assertions that neutral and core leaves cannot reach DataFusion.

## Alternatives considered

### Keep one CLI crate and optimize test filters/features

Rejected. Cargo dependencies are package-wide; a test-name filter does not prune the library graph. Optional features would make production completeness conditional, multiply feature combinations, and risk testing a product different from the shipped binary.

### Rename the full product to `cdf-product` and make `cdf-cli` the leaf

Rejected for now. It is conceptually pure but causes broad package/release/install churn without further graph isolation. Keeping `cdf-cli` as the complete product and extracting `cdf-cli-core` achieves the measured requirement with fewer moving parts.

### Extract only a renderer crate

Rejected. Parser/help/terminal/output tests are the repeated fast-check workload and share the same leaf concerns. A renderer-only split would leave grammar and artifact checks on the 377-package graph.

### Put package models in `cdf-kernel`

Rejected. Package layout/replay artifacts are evidence-container concerns, not the universal data calculus. Pulling them into kernel would expand the most foundational authority and make kernel changes artifact changes.

### Let `cdf-runtime` keep concrete package readers behind a feature

Rejected. Neutral runtime correctness must not depend on feature selection, and optional implementation edges would preserve the architectural leak in some builds.

## Consequences

Focused CLI UX tests compile a small leaf rather than the full product. Production remains one complete statically composed binary. A source/format/transform/destination addition still edits its implementation, the one standard composition catalog, and conformance enrollment—not core CLI or neutral runtime.

Package model moves require a coordinated import migration. Because there are no production consumers or old-artifact compatibility obligations, moved types are not re-exported from their previous owner merely to preserve paths. Golden package bytes and lifecycle semantics must remain identical.

The build graph becomes a tested architecture surface. Graph thresholds supplement, but do not replace, named forbidden-edge assertions and measured before/after timings.
