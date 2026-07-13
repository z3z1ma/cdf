Status: active
Created: 2026-07-12
Updated: 2026-07-12

# Product build-graph boundaries

## Purpose and scope

This specification governs compile-time isolation for the CLI experience, complete static product composition, package artifact contracts, test ownership, and DataFusion containment. It changes no command grammar, package bytes, execution semantics, source/destination behavior, or production feature set.

## CLI core and product composition

`cdf-cli-core` MUST own only grammar/arguments, help/completion/man generation, terminal policy, rendering, and transport-neutral output/error envelopes. It MUST NOT depend directly or transitively on `cdf-project`, `cdf-engine`, `cdf-runtime`, `cdf-package`, `cdf-state-*`, `cdf-source-*`, `cdf-format-*`, `cdf-transform-*`, `cdf-dest-*`, database/network implementation clients, DataFusion, Parquet, or DuckDB.

`cdf-cli` MUST remain the complete standard first-party composition root and production `cdf` binary. Its production registry catalogs MUST remain the single catalogs governed by SX1, FX1, and DX3/DX4. The shipped binary MUST include the same command and built-in adapter surface before and after extraction.

Core types MUST describe CLI requests and rendered results, not product services. The split MUST NOT introduce a service locator, global registration, callback map, command-handler trait per command, or a generic provider abstraction with one implementation.

## Package contract leaf

`cdf-package-contract` MUST contain the canonical package manifest/segment/file/lifecycle and replay-preimage models plus only the capability contracts needed by neutral consumers of already-verified package facts. It MUST depend downward on kernel/Arrow/Serde contracts only as required and MUST perform no filesystem, IPC, Parquet, hashing, archive, tempfile, or artifact-verification work.

`cdf-package` MUST implement package persistence, verification, codecs, archives, and durable streaming access against that contract. `cdf-runtime` MUST depend on `cdf-package-contract` and MUST NOT depend on `cdf-package`. Runtime final-binding logic MUST validate destination/staging lifecycle facts without opening package paths or downcasting to an implementation reader.

There MUST be one Rust type for each canonical package artifact. Old-owner compatibility re-exports, duplicate models, conversion mirrors, and feature-selected alternative package contracts are forbidden.

## Test ownership and gates

Pure parser, help, completion/man freshness, terminal, rendering, layout, and output-envelope tests MUST execute in `cdf-cli-core` without compiling the complete product graph. Product command dispatch and integration tests MUST remain in `cdf-cli` or conformance owners. A test MUST NOT be copied into both crates to improve a coverage count.

Static graph tests MUST inspect Cargo metadata/tree authority and fail when forbidden edges reappear. At closure:

- `cdf-cli-core` normal resolution MUST contain at most 8 workspace packages and at most 113 unique packages (a 70% reduction from the 377-package baseline), and MUST contain none of the forbidden product/engine/driver/database/codec nodes named above. Its normal+dev and all-features graphs MUST preserve every named forbidden-edge law; generated artifact support cannot smuggle the product graph back into the leaf.
- `cdf-runtime` normal resolution MUST contain neither `cdf-package`, `parquet`, `arrow-ipc`, nor `tempfile`, and MUST contain at most 67 unique packages (a 25% reduction from the 90-package baseline). Its normal+dev graph MUST also exclude `cdf-package`, `parquet`, and `arrow-ipc`; concrete package integration tests belong to `cdf-package`, product, or conformance rather than reintroducing the implementation as a runtime dev dependency.
- One recorded filtered parser/render test in `cdf-cli-core` MUST complete compilation at least 5x faster than the 5m24s baseline on the same host state, with test execution reported separately. The ratio is supporting evidence; forbidden-edge and package-count laws are the stable contract.
- The full production binary and product integration suite remain explicit non-fast gates; they MUST NOT be silently removed or replaced by leaf checks.

## DataFusion containment

DataFusion types and dependencies MUST remain in `cdf-engine` or focused engine-adapter crates under `.10x/specs/datafusion-currency-bridges.md`. `cdf-cli-core`, `cdf-package-contract`, `cdf-runtime`, kernel, and driver implementation contracts MUST not reach DataFusion. Optional exotic format hosting remains J6/FX1 authority; this specification creates no competing adapter.

## Acceptance scenarios

- Given a parser/help/render-only edit, when its focused owner tests run, then Cargo does not compile DataFusion, DuckDB, Parquet, object stores, databases, transports, package I/O, or concrete adapters.
- Given the production `cdf` binary, when its built-in catalog is inspected, then every supported first-party source, format, transform, transport, and destination remains registered through the existing single composition authority.
- Given a new destination or source, when it is enrolled, then `cdf-cli-core`, `cdf-package-contract`, and `cdf-runtime` require no concrete-adapter edit.
- Given a verified package from `cdf-package`, when a destination runtime prepares/finalizes staged ingress, then runtime validates the same manifest/replay/segment facts through the package contract without filesystem access or package-implementation types.
- Given package golden fixtures before and after the model extraction, then manifest bytes, package hashes, replay inputs, receipts, lifecycle transitions, and rejection behavior are identical.

## Explicit exclusions

This specification does not make the production binary modular at runtime, add dynamic plugins, preserve old Rust import paths, change old artifact readers, replace native codecs with DataFusion, or weaken complete product/integration verification.
