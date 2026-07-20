Status: done
Created: 2026-07-19
Updated: 2026-07-19
Parent: .10x/tickets/done/2026-07-19-iceberg-glue-source-program.md

# Iceberg F1: neutral object-access extraction

## Scope

Move reusable local/HTTP/S3/GCS/Azure metadata, listing, sequential/range `ByteSource`, client-pool, and transport composition authority out of `cdf-source-files` into one neutral crate consumed by the file source and future Iceberg/Glue sources. Preserve file-source semantics and measured hot paths exactly; delete the superseded source-local surface rather than keep a shim.

## Non-goals

No Iceberg dependency or protocol logic, source-position change, scan-task artifact, new transport behavior, performance tuning, generic project/runtime source-id branch, or AWS mutation.

## Acceptance Criteria

- `cdf-source-files` consumes neutral object access and retains only file-source planning/glob/discovery/compression/manifest behavior.
- The neutral crate exposes capability/request types sufficient for metadata, listing, sequential and exact-range `ByteSource` access with injected secret/egress/execution/memory authority.
- There is one client pool, retry/controller, cancellation, generation, telemetry, spool, and memory-accounting implementation; no compatibility re-export or duplicate helper remains.
- Existing local/HTTP/cloud file-source conformance and remote Parquet performance evidence remain unchanged within measurement noise.
- Architecture checks prevent source crates from depending on sibling sources and prevent generic runtime/project imports of concrete object-access implementations.

## References

- `.10x/decisions/iceberg-glue-source-boundaries.md`
- `.10x/specs/source-extension-runtime-contract.md`
- `.10x/specs/constant-memory-proof.md`
- `.10x/specs/data-onramp-file-sources-transports.md`
- `.10x/decisions/native-format-driver-and-byte-source-boundary.md`

## Assumptions

- User-ratified 2026-07-19: neutral extraction with no compatibility shim is approved.
- Record-backed: `cdf-runtime::ByteSource` remains the payload interface; file-source-specific policy stays in `cdf-source-files`.

## Journal

- 2026-07-19: Opened as the first executable, crate-bounded Iceberg foundation lane. Existing dirty `cdf-runtime/src/worker_protocol.rs` belongs to another worker and is out of scope.
- 2026-07-19: Execution started after the governing records landed in the prior turn. The active worker had committed its source-driver changes and retained one dirty `cdf-runtime/src/worker_protocol.rs`; F1 will not touch runtime worker protocol or source-driver files unless that worktree becomes clean and ownership is explicitly reconciled. The extraction is constrained to a physical move of the existing object-access dependency closure with no behavioral or tuning change.
- 2026-07-19: Mapped the complete physical dependency closure: transport composition, local and object-store `ByteSource` implementations, growing and evicting spools, and the payload cache must move together. Format inference, glob expansion, discovery, compression selection, `FileManifest`, and source scheduling remain in `cdf-source-files`.
- 2026-07-19: Created `cdf-object-access` and moved the six implementation modules without a compatibility re-export. `cdf-transport-http` now implements the neutral HTTP object-access trait directly instead of depending on `cdf-source-files`; CLI, conformance, benchmark, and test composition import the neutral facade explicitly.
- 2026-07-19: Removed the only file-source dependency from the moved transport: local directory listing now requires a caller-supplied validated `BlockingLaneSpec`. Standard file-source composition injects the existing `file_source_blocking_lane()` unchanged. No remote metadata, sequential, exact-range, retry, controller, spool, cache, or memory behavior changed.
- 2026-07-19: Static attribution audit compared every moved module to `HEAD`. `object_store_byte_source.rs` is byte-for-byte identical. Local byte-source, growing spool, evicting spool, and payload-cache diffs contain only cross-crate visibility changes. HTTP provider changes contain only import-path changes. Transport changes contain visibility, local-listing lane injection, and its focused law; the remote payload path is textually unchanged.
- 2026-07-19: A release `cdf-p3-lab` build completed with the product fat-LTO profile. The 9-sample local package smoke cell recorded a 144,481,209 ns median for Parquet package build and 156,467,250 ns for NDJSON package build, with zero spill. The stored same-host-class preoptimization baseline records 155,885,125 ns and 164,721,125 ns respectively. Because intervening revisions differ, these numbers are a no-regression smoke signal rather than causal speedup evidence; textual hot-path identity is the attribution evidence for F1.
- 2026-07-19: The full runtime build-graph test batch has one unrelated current-head failure: `cdf-runtime` now contains 85 packages versus its stale 67-package ceiling. Both new F1 graph laws pass independently. Broad strict clippy similarly reaches unrelated existing `cdf-benchmarks` warnings; strict clippy for all three F1 owning crates passes.

## Blockers

None.

## Evidence

- Acceptance 1 — `cdf-source-files` consumes neutral access and retains source semantics:
  - `CARGO_BUILD_JOBS=12 cargo check -p cdf-object-access -p cdf-source-files -p cdf-transport-http --all-targets` passed.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-files -p cdf-transport-http` passed: 46 file-source tests and 15 HTTP-provider tests, including multi-file globs, remote Parquet spool/range selection, compressed NDJSON, payload reuse, and object-store streaming.
  - Source inspection confirms `cdf-source-files/src/lib.rs` no longer declares or re-exports any moved module.
- Acceptance 2 — neutral metadata/list/sequential/range authority with injected services:
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-object-access` passed: 38 passed, 1 slow million-entry test ignored by its existing gate.
  - `local_listing_uses_caller_owned_blocking_lane` proves missing scheduling policy fails before work and the injected lane executes the bounded listing.
- Acceptance 3 — one authority, no shim or duplicate:
  - The source-local modules are deleted; repository search finds production definitions only under `cdf-object-access`.
  - The HTTP dependency edge is inverted from `cdf-transport-http -> cdf-source-files` to `cdf-transport-http -> cdf-object-access`.
  - Static `HEAD` comparison described in the Journal proves the moved hot implementations were not forked or rewritten.
- Acceptance 4 — conformance and performance preservation:
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-project object_store_multi_file_parquet_discovery_pins_one_reconciled_snapshot` passed.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli source_registry::tests::builtin_registry_is_process_scoped -- --exact` passed.
  - `CARGO_BUILD_JOBS=12 cargo build --release -p cdf-benchmarks --bin cdf-p3-lab` passed under fat LTO.
  - A 9-sample baseline smoke run observed 144.48 ms median Parquet package build and 156.47 ms NDJSON package build with zero spill; both are below the stored same-host-class preoptimization medians, subject to the comparison limit recorded above.
- Acceptance 5 — architectural enforcement:
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime --test build_graph object_access` passed both `neutral_object_access_graph_excludes_sources_and_upper_layers` and `generic_compiler_and_runtime_graphs_exclude_object_access_implementation`.
  - `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-object-access -p cdf-source-files -p cdf-transport-http --all-targets --no-deps -- -D warnings` passed.

## Review

### Findings

- Critical: none.
- Significant: none.
- Minor: none.

Fresh adversarial self-review traced dependency direction, duplicate definitions/re-exports, secret and egress injection, metadata memory accounting, cancellation and generation checks, local scheduling policy, payload-cache/spool ownership, HTTP provider composition, generic-layer reachability, and every production diff against `HEAD`. The neutral crate contains no source, format, glob, schema, manifest, project, CLI, engine, package, or destination authority. The file source contains no superseded object-access implementation or re-export. Remote sequential/range payload behavior is textually unchanged.

Verdict: pass.

Residual risk: this extraction did not repeat live S3/GCS/Azure/HTTP runs. That risk is bounded by the exact hot-path comparison, 99 focused transport/source tests, project-level object-store multi-file Parquet integration, strict owning-crate clippy, architecture laws, and the release smoke measurement. Live-provider behavior remains owned by the later program conformance children rather than being claimed here.

## Retrospective

The dependency closure was larger than the transport facade alone: moving only transport would have left spool and payload-cache policy trapped in the first consumer and forced Iceberg to recreate it. Moving the complete closure made the neutral boundary real.

The most valuable discovery was the reverse `cdf-transport-http -> cdf-source-files` dependency. Inverting it removed an existing extension smell while avoiding any generic runtime branch. The only source-specific reference inside the moved code was the local-listing blocking lane; injecting its validated spec was smaller and clearer than inventing a neutral concurrency default.

Static old/new source comparison was the most reliable performance attribution technique for a physical extraction. The broad runtime build-graph and benchmark clippy commands also exposed unrelated current-head failures; focused filters preserved useful F1 evidence without laundering those failures into this ticket. An initial `--exact` project-test filter matched zero tests and was rerun correctly without `--exact`.

No new knowledge or follow-up ticket is required: the governing boundary decision already captures the reusable rule, and F2/F3/F4/I1/G1 own the next program work.
