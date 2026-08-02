Status: done
Created: 2026-08-01
Updated: 2026-08-01
Parent: .10x/tickets/done/2026-08-01-rust-crate-architecture-cleanup.md

# Close the Iceberg upstream-type public leak

## Scope

Remove execution-internal Iceberg authority types from `cdf-source-iceberg`'s public facade. Make `ValidatedIcebergTaskSetAuthority` and its upstream `iceberg::spec` accessors crate-private unless a real external consumer proves a public CDF-owned view is required. Audit other public functions in the crate for avoidable upstream `iceberg` types and narrow them when unused outside the crate.

## Non-goals

- No Iceberg task bytes, identity, planning, schema conversion, catalog behavior, or execution change.
- No removal of public source-driver, configuration, catalog-extension, or CDF-owned portable task contracts that external composition actually needs.
- No generic wrapper around all Iceberg upstream types.

## Acceptance Criteria

- `ValidatedIcebergTaskSetAuthority` is not publicly re-exported and does not expose `iceberg::spec` types outside the crate.
- Every remaining public upstream-Iceberg type has a demonstrated external consumer or a recorded rationale tied to the adapter extension surface.
- Internal task validation, planning, execution, catalog, and conformance behavior remains unchanged and passes focused checks.
- Public API comparison reports only the explicitly approved visibility narrowing.

## References

- `.10x/knowledge/source-destination-extension-invariant.md`
- `.10x/specs/source-extension-runtime-contract.md`
- `.10x/decisions/iceberg-glue-source-boundaries.md`
- `crates/cdf-source-iceberg/src/lib.rs`
- `crates/cdf-source-iceberg/src/scan_task.rs`

## Assumptions

- Record-backed: repository search found no consumer of `ValidatedIcebergTaskSetAuthority` outside `cdf-source-iceberg`.
- User-ratified: closing this concrete abstraction leak is authorized even though it narrows a pre-1.0 public surface.

## Journal

- 2026-08-01: Ticket opened from the audit finding at `scan_task.rs:236-289` and `lib.rs:41-46`.
- 2026-08-01: Execution assigned. Read the ticket, both governing records, and the referenced facade and task-authority source completely. The authorized invariant is to retain CDF-owned portable configuration/catalog/task contracts while making decoded upstream Iceberg authority state crate-private; no behavior or serialized-shape change is in scope.
- 2026-08-01: Audited every `iceberg::` use and every public declaration in the crate, then searched all workspace Rust consumers. The direct public upstream surfaces were the validated task-set authority and its typed accessors, `annotated_arrow_schema(&iceberg::spec::Schema)`, and `LoadedIcebergTable::metadata: Arc<iceberg::spec::TableMetadata>`. No Rust consumer outside `cdf-source-iceberg` referenced any of those surfaces. The only external crate imports are driver/runtime construction in `cdf-builtin-drivers` and `cdf-benchmarks`.
- 2026-08-01: Read the active Iceberg/Glue boundary decision after the audit surfaced the catalog field. It explicitly keeps Iceberg protocol types inside the Iceberg source boundary. Narrowed the validated authority, its conversion/validation path, its upstream accessors, the schema-annotation helper, and loaded-table metadata to crate visibility. The public driver, configuration, catalog-extension, portable task, Arrow schema, and serialized authority/task contracts remain in place.
- 2026-08-01: The first all-target check found `planner` and `task_reader` importing the validated authority through the crate facade. Redirected both to the private `scan_task` owner. Once the type was private, `model()` proved test-only; gated that accessor to tests so the production build remains warning-free.
- 2026-08-01: Focused checks, strict lint, the complete Iceberg library suite, downstream consumer compilation, formatting, and diff validation passed. Before/after rustdoc comparison reports only the authorized public narrowing recorded below.
- 2026-08-01: `graphify query` and the required post-edit `graphify update .` were attempted, but this environment has no `graphify` executable. This limits graph regeneration only; source, compiler, API, and behavior evidence are otherwise complete.

## Blockers

None.

## Evidence

- Public upstream-type audit: `rg -n --pcre2 '^\s*pub(?!\s*\()[^\n]*(?:iceberg::|TableMetadata|PartitionSpec|NameMapping)' crates/cdf-source-iceberg/src --glob '*.rs'` returned no matches. All remaining direct `iceberg::spec` types occur behind `pub(crate)` or private declarations. The public `decode_arrow_schema` bridge accepts bytes and returns neutral Arrow `Schema`; the visible catalog table fields are CDF-owned or neutral.
- Consumer audit: repository-wide Rust searches found no occurrence of `ValidatedIcebergTaskSetAuthority`, `annotated_arrow_schema`, or `LoadedIcebergTable` outside `cdf-source-iceberg`. `cdf_source_iceberg` imports outside the crate are limited to `AwsGlueCatalogClient`, `IcebergRuntimeDependencies`, `IcebergSourceDriver`, and `UnsupportedGlueCatalogClient`; none were changed.
- Public API comparison: generated locked, dependency-free rustdoc before and after the edit and compared normalized root item sets. The root changed from 57 to 55 items with exactly two removals: `ValidatedIcebergTaskSetAuthority` and `annotated_arrow_schema`; there were no additions. Targeted rustdoc comparison also shows only the authorized supporting narrowing: `LoadedIcebergTable::metadata`, `IcebergTaskSetAuthority::into_validated`, and `IcebergScanTask::validate_against` are no longer public. Every other root item, visible loaded-table field, and public portable task method is unchanged.
- `cargo check -p cdf-source-iceberg --all-targets --locked` — passed cleanly after direct owner imports replaced the two internal facade imports.
- `cargo clippy -p cdf-source-iceberg --all-targets --locked --no-deps -- -D warnings` — passed.
- `cargo test -p cdf-source-iceberg --lib --locked` — 43 passed, 0 failed, 0 ignored.
- `cargo check -p cdf-builtin-drivers -p cdf-benchmarks --all-targets --locked` — passed, covering both workspace crates that directly import the Iceberg adapter facade.
- `cargo fmt -p cdf-source-iceberg -- --check` and targeted `git diff --check` — passed.
- `graphify update .` — unavailable (`zsh: command not found: graphify`). Limit: this executor could not regenerate the existing graph artifacts; no product or source verification depends on that tool.

## Review

### Findings

- None. The independent OCR-delegated falsification found no critical, significant, minor, or nit defect introduced by this visibility-only change.

### Verdict

**Pass.** The workspace-mode OCR preview selected the five changed Iceberg production files, and the resolved source-extension rule was applied to each diff.

- External-consumer falsification: a repository-wide search outside `crates/cdf-source-iceberg` found no use of `ValidatedIcebergTaskSetAuthority`, `annotated_arrow_schema`, `LoadedIcebergTable`, `IcebergTaskSetAuthority`, `IcebergScanTask`, or `IcebergCatalogBinding`. The only manifest dependents are `cdf-builtin-drivers` and `cdf-benchmarks`; their imports remain limited to driver/runtime construction at `crates/cdf-builtin-drivers/src/lib.rs:18` and `crates/cdf-benchmarks/src/runners.rs:41`.
- Upstream-type completeness: the root facade retains the CDF-owned catalog/config/driver/portable-task contracts at `crates/cdf-source-iceberg/src/lib.rs:18`, `:23`, `:36`, and `:41`, but no longer exports the validated execution authority. The remaining typed Iceberg schemas, partition specs, and name mapping are held by the crate-private authority at `crates/cdf-source-iceberg/src/scan_task.rs:236`, `:262`, `:280`, and `:288`; table metadata is crate-private at `crates/cdf-source-iceberg/src/catalog.rs:66`; and the schema annotator is crate-private at `crates/cdf-source-iceberg/src/catalog.rs:987`. A full source scan found no public signature or public field containing an upstream `iceberg` type.
- Exact API delta: the diff changes visibility only. Root removals are exactly `ValidatedIcebergTaskSetAuthority` and `annotated_arrow_schema` (`crates/cdf-source-iceberg/src/lib.rs:18-46`). Supporting narrowing is limited to `LoadedIcebergTable::metadata` (`catalog.rs:66`), `IcebergTaskSetAuthority::into_validated` and the validated type/accessors (`scan_task.rs:189-289`), and `IcebergScanTask::validate_against` (`scan_task.rs:355`). These are the decoded upstream authority seam explicitly authorized by the ticket; no constructor, serialized CDF-owned field, driver/configuration symbol, catalog trait, or portable task method changed.
- Extension-contract falsification: `GlueCatalogClient`, `IcebergCatalogBinding`, their request/context/result types, registry registration, and neutral loaded-table observations remain public at `crates/cdf-source-iceberg/src/catalog.rs:53-113`, `:173-227`, and `:260-266`. Removing raw `TableMetadata` access therefore closes the upstream protocol leak without removing the catalog-extension entry points or the CDF-owned `table_identity`, selected snapshot, Arrow schema, byte, and object observations.
- Internal ownership and behavior: planning and reading now import the validated authority directly from its private owner at `crates/cdf-source-iceberg/src/planner.rs:31` and `crates/cdf-source-iceberg/src/task_reader.rs:20`. The task codec still validates authority and every decoded task at `task_reader.rs:97-115`; planning still constructs and validates the same authority at `planner.rs:726-824`; and preparation, execution, and attestation still revalidate the task/authority pair at `crates/cdf-source-iceberg/src/execution.rs:47`, `:566`, and `crates/cdf-source-iceberg/src/driver.rs:807`. No behavior body changed.
- Protective assertions: no test assertion or fixture changed. The retained tests cover authority projection/validation, empty-table rejection, canonical encoded identity, portable task-store round trip, secret exclusion, tamper rejection, and delete ordering at `crates/cdf-source-iceberg/src/scan_task.rs:891-1124`; planner authority/projection and manifest checks remain at `crates/cdf-source-iceberg/src/planner.rs:1182-1334`; and the end-to-end driver tests continue through discovery, canonical planning, isolated task reconstruction, and execution.

### Residual Risk

- Repository search cannot observe downstream crates outside this workspace. The pre-1.0 compatibility break is nevertheless the exact user-ratified narrowing, and no generic or catalog-extension contract needs the removed upstream types.
- The exact public API delta is recorded as execution evidence and is evident in the scoped source diff, but no checked-in public-API snapshot or external compile-fail fixture permanently guards against future re-exposure.
- `LoadedIcebergTable` already had the private `retained` field at `crates/cdf-source-iceberg/src/catalog.rs:71`, so an external catalog binding could not construct it directly even before this ticket. That pre-existing catalog-extension ergonomics issue was not introduced or worsened by the reviewed change, but remains outside the evidence claimed here.

## Retrospective

The public facade had become an internal convenience import path, so removing one re-export immediately identified the two production modules that depended on the wrong ownership edge. Redirecting those imports to `scan_task` both fixed compilation and made the intended private boundary compiler-visible.

The broader signature audit mattered: the named validated authority was not the only leak. The schema annotation helper and loaded-table metadata field independently exposed upstream Iceberg types despite having no external consumer. Capturing rustdoc before editing made it possible to prove the complete compatibility delta rather than infer it from source.

The recurring cause was allowing adapter internals to consume their crate root while the root also served as the public extension facade. Future adapter reviews should pair explicit facade enumeration with a direct-upstream-type scan and workspace consumer search. No follow-up code ticket is needed from this execution. The absent `graphify` binary is an environment/tooling limitation already visible to the aggregate program, not an Iceberg product defect.
