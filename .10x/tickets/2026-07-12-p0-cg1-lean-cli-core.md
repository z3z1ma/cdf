Status: open
Created: 2026-07-12
Updated: 2026-07-12
Parent: .10x/tickets/2026-07-12-p0-cargo-product-build-graph.md
Depends-On: .10x/tickets/2026-07-11-p0-dx3-generic-lock-doctor-replay.md

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
- `.10x/tickets/2026-07-11-p0-sx1-source-extension-boundary.md`
- `.10x/tickets/done/2026-07-11-p0-fx1-native-format-extension-boundary.md`
- `.10x/tickets/2026-07-12-p0-dx3a-cli-destination-registry-authority.md`

## Assumptions

- **Record-backed:** Keeping `cdf-cli` as the product package is the smallest split that preserves existing release/install composition while gaining compile isolation.
- **Record-backed:** DX3A supplies the explicit destination registry authority before module movement, preventing extraction from freezing hidden builtin reconstruction.

## Journal

- 2026-07-12 (shaping): Bounded extraction around dependency ownership. Product behavior is preservation-only; graph shape is the changed behavior.

## Blockers

Depends on DX3, which already depends on DX3A. This prevents the extraction from racing the active report/doctor/replay owner and ensures it moves the completed single destination authority rather than preserving hidden reconstruction.

## Evidence

Pending execution.

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

## Retrospective

Pending execution.
