Status: done
Created: 2026-07-12
Updated: 2026-07-17
Parent: .10x/tickets/done/2026-07-12-p0-cargo-product-build-graph.md
Depends-On: .10x/tickets/done/2026-07-11-p0-dx3-generic-lock-doctor-replay.md

# P0 CG1: lean CLI core and complete product composition

## Scope

Extract `cdf-cli-core` as the dependency-light owner of grammar/help/artifacts/terminal/render/output while retaining `cdf-cli` as the complete static first-party composition root and production `cdf` binary. Move code; do not duplicate it or add compatibility re-exports.

## Non-goals

- Dynamic plugins, optional production commands/adapters, handler factories, service locators, or a product-package rename.
- Changing command grammar, output semantics, first-party catalogs, redaction, exit codes, or execution behavior.
- Completing remaining source/destination/format driver migrations owned by SX1/DX3/FX1.

## Acceptance criteria

- `cdf-cli-core` owns the surfaces required by the governing spec and its normal graph satisfies the <=8 workspace, <=113 unique-package, and named forbidden-edge laws. Normal+dev and all-features graphs retain every forbidden-edge law, including help/man/completion generation.
- `cdf-cli` depends on the core and remains the sole complete production binary/composition root. Existing SX1/FX1/DX3 catalogs move only if necessary to preserve that one root; no second registry/catalog authority appears.
- The production binary exposes byte-for-byte-equivalent generated help/man/completion artifacts and structurally equivalent JSON/human/error envelopes for existing golden scenarios.
- No compatibility re-export, feature-selected lean/full implementation, callback router, or single-implementation product service trait is introduced.
- Before/after Cargo tree, timing, and binary catalog evidence are journaled with host/cache limits.

## References

- `.10x/specs/product-build-graph-boundaries.md`
- `.10x/decisions/lean-cli-and-package-contract-build-boundaries.md`
- `.10x/research/2026-07-12-cargo-product-build-graph-audit.md`
- `.10x/tickets/done/2026-07-11-p0-sx1-source-extension-boundary.md`
- `.10x/tickets/done/2026-07-11-p0-fx1-native-format-extension-boundary.md`
- `.10x/tickets/done/2026-07-12-p0-dx3a-cli-destination-registry-authority.md`

## Assumptions

- **Record-backed:** Keeping `cdf-cli` as the product package is the smallest split that preserves existing release/install composition while gaining compile isolation.
- **Record-backed:** Terminal DX3A supplied the explicit destination registry authority before module movement, preventing extraction from freezing hidden builtin reconstruction.

## Journal

- 2026-07-12 (shaping): Bounded extraction around dependency ownership. Product behavior is preservation-only; graph shape is the changed behavior.
- 2026-07-17 (execution): Extracted `cdf-cli-core` as a workspace package owning CLI grammar, terminal policy, output envelopes, renderer primitives, progress rendering, suggestions, error catalog mappings, generated help/man/completion checks, and the CLI artifact generator. `cdf-cli` remains the `cdf` production binary and complete static composition root for commands, project/source/runtime/destination wiring, and first-party catalogs.
- 2026-07-17 (execution): Removed the old `cdf-cli` artifact-generation feature/bin and updated `QUALITY.md` plus generated-artifact stale messages to run `cargo run -p cdf-cli-core --features cli-artifacts --bin cdf-generate-cli-artifacts`. Regenerated committed help/man/completion snapshots through the core generator.
- 2026-07-17 (execution): Moved pure parser, terminal, renderer, progress, redaction, and artifact snapshot tests to `cdf-cli-core`; retained product command/envelope tests in `cdf-cli`. Removed the public `cdf_cli::InvocationResult` compatibility re-export and updated conformance to name `cdf_cli_core::output::InvocationResult` directly.
- 2026-07-17 (execution): Preserved the single product catalog authority: no source, format, transform, or destination registry moved into core; no feature-selected lean/full product implementation, callback router, service locator, second registry, or adapter reconstruction path was introduced.
- 2026-07-17 (execution): Measured `cdf-cli-core` normal graph at 79 unique packages and all-features graph at 83 unique packages. Both graphs contain only two workspace packages (`cdf-cli-core`, allowed `cdf-kernel`) and no forbidden runtime/project/package/source/format/destination/DataFusion/object-store/network/database edges.

## Blockers

None. DX3 and DX3A are terminal, so CG1 can now move the completed single destination authority rather than preserving hidden reconstruction.

## Evidence

- Graph law:
  - `cargo tree -p cdf-cli-core -e normal --prefix none --locked | sort -u | wc -l` → 79 unique packages.
  - `cargo tree -p cdf-cli-core --all-features -e normal --prefix none --locked | sort -u | wc -l` → 83 unique packages.
  - `cargo tree -p cdf-cli-core -e normal --prefix none --locked | rg "^(cdf-(project|engine|runtime|package|state|source|format|transform|dest)|duckdb|parquet|datafusion|object_store|postgres|reqwest|tokio)"` → no matches.
  - `cargo tree -p cdf-cli-core --all-features -e normal --prefix none --locked | rg "^(cdf-(project|engine|runtime|package|state|source|format|transform|dest)|duckdb|parquet|datafusion|object_store|postgres|reqwest|tokio)"` → no matches.
- Core correctness:
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli-core --locked -j 12` → 34 passed.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli-core --features cli-artifacts --locked -j 12` → 36 passed, including generated help/man/completion snapshot checks.
  - `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-cli-core --all-targets --all-features --locked -j 12 -- -D warnings` → passed.
- Product preservation:
  - `CARGO_BUILD_JOBS=12 cargo check -p cdf-cli --tests --locked -j 12` → passed.
  - `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-cli --all-targets --locked -j 12 -- -D warnings` → passed.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli parser_accepts_canonical_color_policy_anywhere_without_changing_json_envelope --locked -j 12` → passed.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli parser_preserves_global_project_env_and_json_anywhere --locked -j 12` → passed.
  - `CARGO_BUILD_JOBS=12 cargo check -p cdf-conformance --tests --locked -j 12` → passed after conformance stopped using the removed public re-export.
- Hygiene:
  - `cargo fmt --all` → completed.
  - `git diff --check` → passed.
  - Repository search found no remaining `pub use cdf_cli_core::output::InvocationResult`, `cdf_cli::InvocationResult`, `crate::InvocationResult`, or stale `cargo run -p cdf-cli --features cli-artifacts` generator command.

## Review

### Fresh adversarial shaping review (2026-07-12)

#### Findings

No unresolved finding after the dependency repair. The prior DX3A-only edge allowed CG1 and active DX3 to become executable together even though CG1 must rewire imports across DX3-owned `cdf-cli` modules. Depending on DX3 preserves one writer for that product surface; DX3 transitively supplies DX3A's registry authority.

#### Confirmed boundaries

- Current source has one `cdf-cli/src/source_registry.rs` composition module for source, format, and transform drivers and one `cdf-cli/src/destination_registry.rs` module for destinations. The extraction need not introduce a handler trait, callback registry, provider factory, or second catalog.
- `args.rs` is product-service-free; rendering/terminal code is likewise leaf-shaped. `output.rs` and CLI artifact generation use kernel error types, which is compatible with the governing graph threshold and does not require engine/runtime/project or a concrete adapter.
- Production completeness is falsifiable through before/after command artifacts plus explicit catalog inspection; the product crate and `cdf` binary remain in place.

#### Verdict

**Pass for shaping after DX3 closes.** CG1 is bounded, preservation-only at the product surface, and executable once its repaired dependency is terminal.

#### Residual risk

The executor must keep product-specific reports and command execution in `cdf-cli`; moving them merely to maximize leaf test count would violate the core boundary. Timing evidence must state cache/host limits and cannot be treated as proof of semantic completeness.

### Fresh adversarial execution review (2026-07-17)

#### Findings

No blocking findings.

#### Confirmed boundaries

- `cdf-cli-core` is a dependency-light leaf over `cdf-kernel` and presentation dependencies. It does not link project loading, runtime execution, packages, adapters, transports, DataFusion, DuckDB, Postgres, Parquet, Tokio, or object-store code.
- `cdf-cli` still owns command execution and the complete production composition root. The source/format/transform/destination registries remain outside core; the extraction did not create a second catalog.
- The generated artifact owner changed packages intentionally. Committed snapshots still live under `crates/cdf-cli/generated/` because they are release artifacts for the product binary; the generator now lives in core to keep artifact checks out of the full product graph.
- The removed public `InvocationResult` re-export prevents the old `cdf_cli::InvocationResult` path from becoming a legacy compatibility surface. Product tests and conformance name the core type directly where they need it.

#### Verdict

**Pass.** Every acceptance criterion has direct evidence. The remaining private module aliases in `cdf-cli/src/lib.rs` are not re-exports or downstream API; they are crate-local import plumbing for the product modules while preserving the single composition root.

#### Residual risk

The graph evidence proves dependency shape, not wall-clock cold-build improvement on every host. CG2 owns the next topology/gate decision and must avoid reintroducing slow product links into fast checks without superseding the active fast-CI decision.

## Retrospective

The useful seam was presentation ownership, not command ownership: parser/render/progress/artifact code moved cleanly only because product execution stayed in `cdf-cli`. The one real legacy smell was the public `InvocationResult` re-export; removing it was cheaper and cleaner than preserving old internal paths. The artifact snapshots changed because earlier runtime-budget knobs were already part of the current CLI grammar but the committed snapshots had not been refreshed; moving the generator made that staleness visible, which is exactly what the core check should do. Future test-topology work should target this leaf crate directly and keep product smoke tests narrow rather than recompiling the entire adapter graph for grammar/render changes.
