Status: done
Created: 2026-07-12
Updated: 2026-07-12

# Cargo product build-graph audit

## Question

Why do focused CLI and project tests spend minutes compiling before a zero-second assertion, and what crate boundaries remove that tax without weakening the complete production binary or creating a second source, destination, package, or DataFusion authority?

## Sources and methods

- Inspected workspace manifests and the normal dependency edges of `cdf-cli`, `cdf-runtime`, `cdf-package`, `cdf-engine`, and `cdf-project`.
- Inspected `cdf-cli`'s parser, terminal/rendering modules, command dispatch, source/destination registry composition, and binary entry point.
- Inspected `cdf-runtime`'s direct use of `cdf-package` from bulk/staged-ingress/final-binding contracts and `cdf-package`'s model, replay-preimage, verification, reader, IPC, Parquet, hashing, and filesystem responsibilities.
- Read `.10x/specs/architecture-layering-runtime.md`, `.10x/specs/source-extension-runtime-contract.md`, `.10x/specs/destination-extension-runtime-contract.md`, `.10x/specs/datafusion-currency-bridges.md`, `.10x/decisions/source-driver-registry-and-resource-plan-boundary.md`, `.10x/decisions/destination-runtime-composition-boundary.md`, `.10x/decisions/datafusion-analysis-scheduling-identity-boundary.md`, `.10x/decisions/cli-design-language-and-renderer.md`, SX1, DX3A, DX4, and J6.
- Counted unique resolved package nodes with `cargo tree -p <package> -e normal --prefix none`, workspace nodes by canonical workspace path, and direct edges with `--depth 1`. Counts describe the 2026-07-12 lockfile and default normal feature graph; they are not clean-build timings.
- Recorded root-orchestrator observations from the shared workspace. The commands used `CARGO_BUILD_JOBS=12`; therefore the elapsed times are not caused by a one-job override.

## Findings

### Observed compile latency

| Command | Cargo compile time | Test time | Limit |
|---|---:|---:|---|
| `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli --lib destination_doctor_rendering_redacts_driver_health_in_json_and_human_output --locked` | 5m24s | 0.00s | Shared dirty/incremental workspace; this is an observed integration compile, not a controlled cold benchmark. |
| A prior filtered `cdf-cli` report test over the same package graph | 4m00s | negligible | Exact filter was not retained, so this supports recurrence but not per-target comparison. |
| `CARGO_BUILD_JOBS=12 cargo test -p cdf-project generic_lock_plan_replay_and_recovery_drive_mock_runtime_without_destination_branch --locked` | 2m03s | negligible | Cargo rebuilt a different feature/test graph; not directly comparable to the CLI command. |

The first command's assertion required no runtime work. Nearly all wall time was graph construction/code generation/linking for unrelated product capabilities.

### Exact graph snapshot

| Root and edge set | Unique resolved packages | Workspace packages |
|---|---:|---:|
| `cdf-cli`, normal | 377 | 33 |
| `cdf-cli`, normal + dev | 377 | 33 |
| `cdf-engine`, normal | 209 | 7 |
| `cdf-runtime`, normal | 90 | 6 |
| `cdf-package`, normal | 62 | 3 |

`cdf-cli` has 41 direct normal dependencies, 30 of them workspace crates. The direct workspace edges are:

`cdf-contract`, `cdf-declarative`, all three `cdf-dest-*` crates, `cdf-engine`, all four current `cdf-format-*` crates, `cdf-http`, `cdf-kernel`, `cdf-memory`, `cdf-package`, `cdf-project`, `cdf-python`, `cdf-runtime`, all three `cdf-source-*` crates, `cdf-state-sqlite`, all eight current `cdf-transform-*` crates, and `cdf-transport-http`.

Its transitive normal graph contains 28 distinct `datafusion*` packages plus `duckdb`, `libduckdb-sys`, `parquet`, two `object_store` versions, `postgres`, `reqwest`, and `tokio`. Parser/help/terminal/render tests therefore compile the engine, databases, codecs, transports, and every statically composed first-party extension even though their assertions cannot exercise them.

### Composition and test topology are fused

The current `cdf-cli` library is simultaneously:

1. clap grammar and help generation;
2. terminal/output/rendering design system;
3. every product command handler;
4. the static source, format, transform, transport, and destination composition root;
5. the library target containing a monolithic product test module; and
6. the implementation called by the production `cdf` binary.

Cargo dependencies are package-wide. Filtering a test function does not omit unused normal dependencies or the rest of the library target. Faster test filters cannot solve this graph; only a crate/target boundary can.

The smallest boundary consistent with active authority is a lean `cdf-cli-core` library for grammar/help/terminal/render/output and the existing `cdf-cli` package as the complete static product composition and `cdf` binary. This preserves the established standard composition root and avoids an abstract command-service trait, callback registry, or a disruptive package/install rename. Compile-time isolation now satisfies the explicit trigger in `.10x/decisions/cli-design-language-and-renderer.md` for extracting the previously rejected renderer crate.

### The neutral runtime inherits package implementation weight

`cdf-runtime` directly depends on full `cdf-package`. Its runtime contracts import `SegmentEntry`, but staged final binding also imports concrete `PackageReader`, `VerifiedPackage`, and `PackageReplayInputs`. Consequently the neutral extension contract graph reaches Arrow IPC, Parquet, package filesystem verification, builder/archive code, hashing, and tempfile support. `cargo tree -p cdf-runtime -e normal` contains 90 unique packages and a `parquet` node.

The stable seam is not a second package implementation. A leaf `cdf-package-contract` must own canonical package artifact/replay models and capability-style verified package/segment access contracts. `cdf-package` implements filesystem/IPC/Parquet verification and streaming access behind that leaf. `cdf-runtime` consumes the leaf and must not open package paths or name concrete readers. The current runtime helper that constructs final binding from `PackageReader` belongs in the package implementation or an upper integration adapter; final-binding semantic validation remains runtime authority over supplied verified facts.

### Existing ownership prevents a duplicate DataFusion adapter ticket

DataFusion containment is already fully governed by `.10x/specs/datafusion-currency-bridges.md`, `.10x/decisions/datafusion-analysis-scheduling-identity-boundary.md`, parent WS-J, and J6. Those records require DataFusion types to remain in `cdf-engine` or focused engine-adapter crates, prohibit DataFusion in kernel/runtime/source/format/destination/package contracts, and assign optional exotic `FileFormat` hosting plus build-graph measurement to J6. Opening another adapter-containment ticket would create competing authority. The build-graph program should assert the same boundary and leave implementation/audit ownership with J6.

## Conclusions

1. Focused CLI latency is caused by a 377-package product graph attached to the parser/render library target. Test filtering cannot make it fast.
2. Extracting `cdf-cli-core` while retaining `cdf-cli` as the complete standard product/binary is the smallest coherent production-safe split.
3. Parser/help/render/terminal checks need a leaf test topology; product command semantics and live adapter laws remain in product/conformance owners and are not duplicated.
4. Extracting `cdf-package-contract` breaks a genuine lower-layer leak: `cdf-runtime` should depend on stable package facts/capabilities, not the full filesystem/IPC/Parquet implementation.
5. SX1 remains source composition authority, DX3/DX4 remain destination authority, and J6 remains DataFusion adapter authority. The graph work consumes and proves those boundaries rather than replacing them.

## Limits

- No controlled clean-build, incremental-touch, linker-time, or target-directory size experiment was run during this shaping audit. Executable tickets require before/after timing evidence under one recorded host state.
- Unique Cargo package count is a useful topology metric, not a direct proxy for compile cost; code generation, build scripts, native DuckDB compilation, feature unification, and linking have unequal costs.
- The proposed thresholds are graph guardrails derived from the current shape. Executors may tighten them after the first extracted graph is measured, but may not weaken named forbidden-edge assertions to make a target pass.
