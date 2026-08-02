Status: done
Created: 2026-08-01
Updated: 2026-08-01
Parent: .10x/tickets/done/2026-08-01-rust-crate-architecture-cleanup.md

# Enumerate remaining core public facade exports

## Scope

Replace wildcard public re-exports with explicit lists in already-modular roots not owned by another cleanup child: `cdf-kernel`, `cdf-contract`, `cdf-package-contract`, `cdf-package`, `cdf-engine`, `cdf-object-access`, `cdf-declarative`, `cdf-source-files`, `cdf-source-postgres`, `cdf-source-rest`, `cdf-benchmarks`, and `cdf-dest-sql`.

## Non-goals

- No implementation movement beyond what is required to resolve ambiguous names.
- No public API addition, removal, rename, or behavior change.
- No conversion of private implementation modules into public modules.

## Acceptance Criteria

- Scoped crate roots contain no `pub use ...::*`.
- Explicit lists reproduce the prior public symbol set; public API comparison reports no unintended differences.
- New public items added to private modules can no longer leak automatically through a wildcard root.
- Targeted checks and strict lint pass.

## References

- `.10x/knowledge/source-destination-extension-invariant.md`
- `.10x/knowledge/rust-crate-organization.md`
- `.10x/specs/architecture-layering-runtime.md`

## Assumptions

- Record-backed: public facades must enumerate exports.
- User-ratified: the existing public API is the compatibility baseline for this child.

## Journal

- 2026-08-01: Ticket opened after the audit counted 130 non-test wildcard public re-exports across the workspace; roots owned by other children are excluded here.
- 2026-08-01: Execution started. Read the ticket and all referenced active records; constrained edits to public facade declarations in the twelve listed crates, with their existing exported symbol sets as the compatibility baseline.
- 2026-08-01: Generated pre-change rustdoc JSON for every scoped crate and expanded each wildcard source module into a sorted exported-name inventory. The `cdf-benchmarks` inventory also expanded the external `cdf_bench_core::*` facade from `cdf-bench-core`'s rustdoc root.
- 2026-08-01: Replaced all 60 scoped wildcard public re-export declarations with explicit item lists. No implementation module was made public and no implementation behavior moved.
- 2026-08-01: Compiler repair exposed crate-private names that the former wildcard imports also placed at the crate root for sibling production code or white-box tests. Preserved those internal edges with narrow `pub(crate) use` declarations for the existing names; none were added to the public inventory.
- 2026-08-01: Regenerated rustdoc JSON after the edits and compared unique public root names crate by crate. All twelve before/after sets match exactly with zero additions and zero removals.
- 2026-08-01: The required post-edit `graphify update .` could not run because the `graphify` executable is not installed on `PATH`; the source, compiler, rustdoc, and lint evidence below remains available for review.

## Blockers

None.

## Evidence

- Wildcard criterion: `rg -n '^\s*pub\s+use\s+[^;]*\*'` over the twelve scoped `src/lib.rs` files returned no matches after the edits.
- Compiler-derived public inventory parity (before/after, removed, added): `cdf-kernel` 343/343, 0, 0; `cdf-contract` 166/166, 0, 0; `cdf-package-contract` 67/67, 0, 0; `cdf-package` 50/50, 0, 0; `cdf-engine` 104/104, 0, 0; `cdf-object-access` 33/33, 0, 0; `cdf-declarative` 45/45, 0, 0; `cdf-source-files` 17/17, 0, 0; `cdf-source-postgres` 16/16, 0, 0; `cdf-source-rest` 12/12, 0, 0; `cdf-benchmarks` 146/146, 0, 0; `cdf-dest-sql` 20/20, 0, 0. The aggregate comparison covered 1,019 crate-qualified public names.
- Targeted compilation: `cargo check -p cdf-kernel -p cdf-contract -p cdf-package-contract -p cdf-package -p cdf-engine -p cdf-object-access -p cdf-declarative -p cdf-source-files -p cdf-source-postgres -p cdf-source-rest -p cdf-benchmarks -p cdf-dest-sql --all-targets --locked --message-format short` passed.
- Strict lint: the same twelve-package selection under `cargo clippy --all-targets --locked -- -D warnings` passed. Its resolved graph also compiled affected downstream workspace consumers including `cdf-runtime`, `cdf-project`, and `cdf-builtin-drivers`.
- Hygiene: package-scoped `cargo fmt -- --check` and scoped `git diff --check` passed.
- Tooling limit: `graphify update .` exited 127 with `command not found`, so graph output was not refreshed by this executor.

## Review

### Findings

None.

### Verdict

**Pass.** Independent source and rustdoc inspection found no public addition, removal, rename, visibility change, accidental module publication, or remaining scoped wildcard. The twelve current rustdoc JSON roots were inspected by resolving the crate-root module's item IDs through `index`, taking each public item's direct `name` or re-export `inner.use.name`, and comparing total and unique counts. They reproduce the recorded inventories exactly: 343, 166, 67, 50, 104, 33, 45, 17, 16, 12, 146, and 20. Every crate's total equals its unique count, so the name-set comparison is not concealing duplicate root names; all twelve contain zero root `is_glob` imports and zero public module items.

The source diff is confined to replacing the former wildcard facade declarations with named exports, apart from independently owned crate-documentation and engine-layout edits in the shared workspace. The explicit lists cover recursively re-exported items as well as direct declarations, including all 40 public `cdf-bench-core` root names formerly carried by `cdf_bench_core::*` into `cdf-benchmarks`. The narrow `pub(crate)` root aliases preserve every root-qualified production or white-box-test convenience required after wildcard removal: `NativeRowRule` (`crates/cdf-contract/src/lib.rs:61`), `CompiledSchemaAdmissionOutcome` (`crates/cdf-engine/src/lib.rs:56`), the package test/archive and production runtime-schema conveniences (`crates/cdf-package/src/lib.rs:30-34`, `:53-54`, and `:61`), file capabilities (`crates/cdf-source-files/src/lib.rs:173`), and the Postgres batch ceiling (`crates/cdf-source-postgres/src/lib.rs:13`). Their source declarations were already `pub(crate)`, so the aliases neither widen them publicly nor narrow a public item. No scoped root declares `pub mod`.

The downstream evidence is proportionate: the all-target selected-package check and strict Clippy compile `cdf-benchmarks`, whose dependency graph includes `cdf-runtime`, `cdf-project`, and `cdf-builtin-drivers`, in addition to the lower-level scoped crates. This exercises the principal in-workspace reverse consumers rather than only compiling each facade in isolation.

### Residual Risk

- The recorded pre-change rustdoc JSON and comparison script are not retained as durable ticket artifacts. This review could inspect the current JSON, the `HEAD` wildcard baselines, and the exact source diff, but a future cold reader cannot replay the claimed pre/post JSON comparison from the ticket alone.
- Root-name inventory proves path/name parity, not item signatures or associated-item details. Here that limit is mitigated because the reviewed facade edits do not change underlying declarations, and all-target downstream compilation passed; it does not substitute for a semver tool against unknown external consumers.
- The current `cdf-benchmarks` rustdoc JSON predates a later two-line crate-documentation addition, as shown by its root span ending at the former line 81 while the current root has 83 lines. That later change does not affect its verified 146-name facade inventory.

## Retrospective

- A root wildcard is broader than the documented public API: it also makes a module's `pub(crate)` items available at the root and recursively carries named public re-exports. A source-declaration grep alone would have omitted both classes. Rustdoc inventories plus all-target compiler repair made those hidden effects explicit without widening them.
- Comparing crate-qualified exported-name sets before and after gave a reviewable compatibility proof across a large facade. Because implementation declarations and signatures did not change, exact name parity plus downstream all-target compilation is proportionate evidence for this re-export-only change.
