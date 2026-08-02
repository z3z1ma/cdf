Status: done
Created: 2026-08-01
Updated: 2026-08-01
Parent: .10x/tickets/done/2026-08-01-rust-crate-architecture-cleanup.md

# Split the task-store crate root

## Scope

Turn `cdf-task-store/src/lib.rs` into a thin documented facade and split its production responsibilities into focused modules for limits/models, store/workspace, canonical builder/index, encoded reader/writer, typed codecs/retention, SQLite capacity authority, and filesystem publication helpers.

## Non-goals

- No artifact bytes, hashes, SQLite constants, capacity proof, spill policy, parse-memory policy, or publication semantics change.
- No public API rename or visibility widening.

## Acceptance Criteria

- `lib.rs` contains only crate policy/docs, focused module declarations, and explicit public exports.
- New production modules have explicit acyclic imports and responsibilities matching their names.
- Canonical task bytes and identities are unchanged under existing tests.
- High-cardinality, duplicate/conflict, capacity, cleanup, typed-reader retention, and strict lint checks pass.

## References

- `.10x/knowledge/rust-crate-organization.md`
- `.10x/knowledge/source-destination-extension-invariant.md`
- `crates/cdf-task-store/src/lib.rs`

## Assumptions

- Record-backed: the current root contains 2,315 production lines across multiple independently named responsibilities.
- User-ratified: organization changes must preserve public names and artifact semantics.

## Journal

- 2026-08-01: Ticket opened from the monolithic crate-root finding.
- 2026-08-01: Execution began. Read the ticket and governing organization/extension records; `graphify query` was attempted but the `graphify` executable is unavailable, so the crate source and tests are the authority for the split.
- 2026-08-01: Read all 3,789 lines of the former root, including the byte framing, writer/finalization path, reader verification barrier, canonical SQLite insertion/capacity proof, typed retention/parse admission, filesystem publication, and every existing test. Mapped the implementation into seven one-way production responsibilities before editing.
- 2026-08-01: Moved production code without changing constants, SQL, encoder/decoder bodies, error text, or publication order. `lib.rs` is now a 33-line facade over `limits`, `store`, `encoded`, `canonical`, `typed`, `sqlite_capacity`, and `publication`; unit tests moved intact to `tests.rs` with explicit private-helper imports.
- 2026-08-01: `cargo fmt -p cdf-task-store` caught a mechanical visibility substitution in `ExternalTaskWorkspaceLimits::new` before compilation; corrected the constructor parameters and restored the omitted `TaskSetLimits` derive. A subsequent package check reached unrelated, concurrently edited `cdf-runtime` and stopped on missing `cdf-kernel` exports before compiling this crate; focused verification will be rerun after the shared dependency graph stabilizes.
- 2026-08-01: Replaced direct cross-module access to `ExternalTaskStore` layout fields with crate-private store-owned path/namespace methods, keeping the representation private and publication/path construction under the store boundary. Replaced test access to the canonical builder's spill field with a test-only accessor; no production field visibility was widened.
- 2026-08-01: The dependency graph stabilized. Focused all-target check, ordinary unit suite, strict Clippy, documentation tests, formatting check, and the separately invoked ignored million-task conformance test all passed. `graphify update .` was attempted after the source changes and remains unavailable because the executable is not installed.
- 2026-08-01: Closure repair authorized after independent review. Scope is limited to restoring `ExternalTaskWorkspaceLimits` field privacy and replacing direct sibling access with the narrow consuming projection needed by `ExternalTaskStore::accounted_workspace`; constructor validation and workspace behavior must remain unchanged.
- 2026-08-01: Closure repair implemented. All five `ExternalTaskWorkspaceLimits` fields are private again. The validated value exposes one crate-private consuming projection, used only by `store::accounted_workspace`, which binds the owned values to named locals before performing the source-identical memory reservation, spill admission, error mapping, and workspace construction.
- 2026-08-01: Repair verification completed. Focused all-target compilation, the ordinary unit suite, strict all-target Clippy, and crate formatting pass. Source scans find neither crate-visible workspace-limit fields nor direct sibling field access. `graphify update .` remains unavailable (`command not found`).

## Blockers

None.

## Evidence

- Facade/layout: `wc -l crates/cdf-task-store/src/*.rs` reports a 33-line `lib.rs`; inspection shows it contains only crate policy, seven focused production module declarations, explicit public re-exports, and the test module declaration. Production files are `limits.rs`, `store.rs`, `encoded.rs`, `canonical.rs`, `typed.rs`, `sqlite_capacity.rs`, and `publication.rs`; `tests.rs` contains the relocated unit suite. This supports the first acceptance criterion. Limit: line count is structural evidence, not behavioral evidence.
- Import direction: inspection of every `use crate::...` edge yields `sqlite_capacity -> none`, `publication -> none`, `limits -> sqlite_capacity`, `store -> limits/publication`, `encoded -> limits/publication/store`, `canonical -> encoded/limits/sqlite_capacity/store`, and `typed -> canonical/encoded/limits/store`. `rg` found no production glob imports; the sole `use super::*` is in `tests.rs`, as permitted by the governing convention. This supports focused, explicit, acyclic production ownership. Limit: this is source-graph inspection, not a future-cycle guard.
- Public surface: comparing `git show HEAD:crates/cdf-task-store/src/lib.rs | rg '^pub (struct|enum|trait) '` with the new production modules found the same 21 public types/traits, and `lib.rs` explicitly re-exports all 21 under their original crate-root names. `CARGO_BUILD_JOBS=12 cargo check -p cdf-task-store --all-targets --locked` passed. This supports API preservation and all-target compilation. Limit: no semver-diff tool was used.
- Canonical identity and lifecycle: `CARGO_BUILD_JOBS=12 cargo test -p cdf-task-store --lib --locked -j 12` passed 22 tests with 1 intentionally ignored. The passing assertions cover byte/content identity across ordered and spill-sorted builders, provider order/location independence, bounded round-trip, tamper closure, duplicate/conflict handling, pre-mutation capacity admission, spill exhaustion cleanup, cancellation before atomic install, workspace cleanup, and typed authority/task retention. This supports artifact, SQLite, spill, parse-memory, and publication preservation within the existing test scenarios.
- High cardinality: `CARGO_BUILD_JOBS=12 cargo test -p cdf-task-store tests::million_tasks_hold_the_configured_metadata_budget --lib --locked -j 12 -- --ignored --exact --nocapture` passed in 30.53 seconds. This supports the million-record count/order identity and configured memory/spill ceilings asserted by that test. Limit: it is one deterministic local run.
- Strict hygiene: `cargo fmt -p cdf-task-store -- --check`, `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-task-store --all-targets --locked -- -D warnings`, and `CARGO_BUILD_JOBS=12 cargo test -p cdf-task-store --doc --locked -j 12` all passed; the crate has no documentation tests. This supports formatting, strict lint, and documentation-build health.
- Closure-repair evidence:
  - `CARGO_BUILD_JOBS=12 cargo check -p cdf-task-store --all-targets --locked`: passed, including the sibling-module consuming seam and every target.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-task-store --lib --locked -j 12`: passed, 22 passed, 0 failed, 1 intentionally ignored. The existing workspace tests continue to exercise constructor-admitted memory/spill budgets, cleanup, exhaustion classification, and publication behavior.
  - `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-task-store --all-targets --locked -- -D warnings` and `cargo fmt -p cdf-task-store -- --check`: passed.
  - `rg -n "pub\\(crate\\).*\\b(consumer|memory_class|resident_bytes|spill_growth_bytes|minimum_spill_bytes)\\b|limits\\.(consumer|memory_class|resident_bytes|spill_growth_bytes|minimum_spill_bytes)" crates/cdf-task-store/src/limits.rs crates/cdf-task-store/src/store.rs` produced no matches. `rg -n "into_accounting_parts" crates/cdf-task-store/src` reports exactly its definition in `limits.rs` and its sole consumer in `store.rs`.
  - `graphify update .` was re-attempted after the repair and failed with `command not found`; no graph artifact was fabricated or edited.

## Review

### Findings

- **Minor — the split widens the validated workspace-limit representation to the entire crate.** `ExternalTaskWorkspaceLimits` previously kept all five fields module-private, forcing construction through `ExternalTaskWorkspaceLimits::new`; the split marks `consumer`, `memory_class`, `resident_bytes`, `spill_growth_bytes`, and `minimum_spill_bytes` `pub(crate)` (`crates/cdf-task-store/src/limits.rs:18-22`) so `store` can read them. No current caller constructs an invalid value, but every present or future sibling can now bypass the constructor's token, nonzero, and minimum-versus-growth invariants or mutate a valid value after construction. This is the one production field-visibility widening found and weakens the compiler-visible boundary the split is intended to create. Keep the fields private and expose a validated read/consume seam instead.

### Verdict

**Concerns.** The behavior-preservation claims otherwise survived falsification. The root explicitly re-exports the same 21 public types/traits, and source comparison found the public inherent methods and trait methods unchanged. Framing magic/version/tags, authority and record hashing, footer layout, content-address construction, provider generation, SQL text, journal-free SQLite pragmas, mirrored B-tree constants, capacity calculation, pre-insert `ensure_insert_capacity` call, `SQLITE_FULL` poisoning, typed authority/task retention, cancellation checks, file sync, no-clobber install, duplicate verification, and post-install reservation release retain their former bodies and order. The production import graph is explicit and acyclic, with no production glob edge.

The relocated tests were inspected rather than accepted only by result: their assertions still compare ordered and spill-sorted artifact references, provider-order and store-location identity, duplicate/conflict behavior at exhausted spill, reservation growth before a multi-page insert, unchanged task count after rejected capacity, cancellation before atomic installation, tamper closure, singular `Arc`-shared decoded-task identity, authority/task lease retention through reader drop and clone drop, cleanup, and the million-record count/order and memory/spill ceilings. The minor visibility concern should be repaired or explicitly accepted before closure.

### Residual Risk

- Artifact-byte parity is supported by exact source-body/constant comparison and cross-builder identity assertions, but the suite does not pin a complete committed artifact byte vector or content digest against a pre-refactor golden.
- The SQLite capacity proof retains the prior constants and tests, but this review did not independently re-derive the mirrored limits from the bundled SQLite source; a future SQLite upgrade still requires the revalidation named by the governing knowledge record.
- Atomic publication ordering is source-identical and cancellation is tested before install; crash recovery across filesystem-specific `persist_noclobber`, file sync, and parent-directory sync behavior remains outside these unit-test limits.

## Retrospective

The source already had strong behavioral boundaries; the problem was that the crate root erased them. Moving exact implementation bodies behind names that match their authority made the dependency direction legible without adding product abstractions or changing algorithms. The one costly mechanical risk was slicing an item from a large file without its preceding attribute and broad text replacement touching constructor parameters; formatting and compilation exposed both immediately. Future large Rust moves should include leading attributes in the extraction inventory and prefer field-specific patches over global substitutions. Keeping framing constants with encoded I/O, SQLite constants/proofs together, and atomic installation/error classification together made preservation review substantially cheaper. The closure repair reinforced that value-object invariants include field privacy: when a sibling consumes a validated value, expose the consumption operation rather than the representation. The `graphify` executable remains unavailable, so no graph artifact could be refreshed in this execution environment.

## Repair Re-review — 2026-08-01

### Findings

- None. Fresh source inspection confirmed that all five `ExternalTaskWorkspaceLimits` fields are module-private and that the constructor's `Ok(Self { ... })` is the only struct-literal construction path. The type implements no `Default`, `From`, deserialization, setter, or other alternate construction/mutation seam; `Clone` can only reproduce an already validated value.
- `into_accounting_parts` is crate-private, consumes the validated value, and has exactly one call site: `ExternalTaskStore::accounted_workspace`. No sibling directly reads or mutates a workspace-limit field. Because sibling code cannot construct or alter the private representation, the projection does not provide a route around the constructor's canonical-token, nonzero-budget, or minimum-versus-growth validation.
- Comparing the repaired path with the pre-split root found the same public constructor and `accounted_workspace` signatures and the same root re-export. The new projection is not public API. Its tuple order and the store's named bindings preserve the former `ConsumerKey`, resident reservation, initial spill admission, error text/classification, retained growth/minimum values, and consumer identity. The repair does not enter task encoding, hashing, canonical ordering, or artifact publication, so it introduces no task-identity path.

### Verdict

**Pass.** The prior representation-visibility concern is resolved without a replacement resource-budget escape, leaky public abstraction, API change, or capacity/identity behavior change. The ticket remains active for the orchestrator's closure judgment.

### Residual Risk

- This repair review relied on direct source comparison plus the executor's journaled focused check, unit, Clippy, and formatting evidence; it did not repeat those commands. The broader pre-existing residual risks remain: no committed whole-artifact golden pins byte-for-byte output, and the mirrored SQLite capacity constants were not independently re-derived from bundled SQLite source in this review.
- `graphify` was unavailable, so the consumption graph was established with repository-wide symbol and field-access searches rather than a refreshed graph artifact. Those searches found exactly one projection definition and one consumer, and no alternate workspace-limit construction or direct sibling field access.
