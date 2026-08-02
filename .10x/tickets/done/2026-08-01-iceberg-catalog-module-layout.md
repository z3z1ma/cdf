Status: done
Created: 2026-08-01
Updated: 2026-08-01

# Make Iceberg catalog bindings structurally explicit

## Scope

Reorganize the existing Iceberg REST and AWS Glue catalog bindings into parallel, focused catalog
submodules without adding a catalog kind or changing catalog behavior. Rename the Iceberg-specific
AWS Glue client from `AwsGlueCatalogClient` to `AwsIcebergGlueCatalogClient` so it cannot be
confused with the conventional external-table client exported by `cdf-source-glue`. Update all
in-repository consumers.

## Non-goals

- No new catalog provider, configuration field, authentication flow, endpoint behavior, or source
  kind.
- No change to filesystem catalog behavior or to `cdf-source-glue`.
- No compatibility alias retaining the ambiguous Iceberg `AwsGlueCatalogClient` name.
- No change to Glue/REST error classification, retry semantics, response accounting, metadata
  selection, snapshot identity, or object access.

## Acceptance Criteria

- `cdf-source-iceberg` expresses REST and Glue as focused sibling catalog modules under
  `src/catalog/`; the obsolete root `src/glue.rs` no longer exists.
- The public Iceberg client is named `AwsIcebergGlueCatalogClient`, and every repository consumer
  uses that exact name; the conventional Glue source keeps its independent
  `AwsGlueCatalogClient`.
- The standard Iceberg catalog registry still installs filesystem, REST, and Glue exactly once,
  with unchanged catalog-kind selection and downstream table semantics.
- Existing Glue pointer decoding and HTTP/provider error-kind tests remain present and passing;
  moving the adapter does not flatten or reclassify typed failures.
- Focused Iceberg, built-in-driver, and benchmark compilation/tests plus formatting, strict lint,
  workspace checking, and patch hygiene pass.

## References

- `.10x/decisions/iceberg-glue-source-boundaries.md`
- `.10x/specs/iceberg-source.md`
- `.10x/knowledge/error-ownership-taxonomy.md`
- `.10x/tickets/done/2026-08-01-rust-crate-architecture-cleanup.md`

## Assumptions

- Record-backed: Iceberg REST and Glue are already complete Iceberg catalog bindings; this ticket
  reorganizes them rather than adding behavior.
- User-ratified: use explicit REST and Glue catalog modules and the clearer Iceberg-specific Glue
  client name, then commit and push directly to `main`.
- Record-backed: catalog-specific protocol types remain inside `cdf-source-iceberg`; neutral AWS
  signing, credentials, HTTP, egress, and cancellation remain owned by `cdf-aws` and injected
  services.

## Journal

- 2026-08-01: Opened after confirming the user selected architecture-only option 1. Source and
  active records show three existing bindings—filesystem, REST, and Glue—so no provider semantics
  remain unresolved.
- 2026-08-01: Execution started. Read this ticket, all four referenced authority records, repository
  instructions, and the error-ownership audit procedure. Confirmed the implementation scope is
  limited to the Iceberg catalog module map, the explicit Iceberg Glue client rename, and repository
  consumers; the latest orchestration instruction supersedes the ticket's historical commit/push
  note, so this executor will not commit or push.
- 2026-08-01: Frozen the pre-move boundary inventory to `cdf-source-iceberg/src/catalog.rs` and
  `src/glue.rs`: 1,796 lines total, 85 typed error construction/mapping sites by the reproducible
  `rg -n -- 'CdfError::[a-z_]+|ErrorKind::|\.map_err\('` scan. The current Glue pointer/error tests
  live with the AWS client; two REST routing tests live in the shared catalog test module. These
  sites and assertions are preservation authority, not candidates for reclassification.
- 2026-08-01: Inventory correction recorded immediately: the exact pre-move scan returns 90 sites,
  not 85, across both scoped files. The earlier count is invalid; 90 is the frozen baseline for the
  same two-file manifest and exact search expression.
- 2026-08-01: Moved the shared catalog implementation to `src/catalog/mod.rs`, moved the REST
  protocol/binding/tests to `src/catalog/rest.rs`, and moved the Glue protocol/binding/AWS
  client/tests to `src/catalog/glue.rs`. The standard registry remains the single composition root
  and registers filesystem, REST, and Glue once each. The obsolete root `src/glue.rs` is absent.
- 2026-08-01: Renamed the Iceberg client to `AwsIcebergGlueCatalogClient`, re-exported it through
  the catalog module and crate root, and updated built-in-driver and benchmark consumers. A
  repository source scan confirms that the old `AwsGlueCatalogClient` name now identifies only the
  independent conventional `cdf-source-glue` client (aliased as `AwsGlueExternalCatalogClient`
  where both clients are in scope).
- 2026-08-01: Preserved the complete typed-error inventory: the same exact scan finds 90 sites in
  the three destination modules. Stored the final scope manifest and site-by-site disposition in
  `.10x/evidence/.storage/2026-08-01-iceberg-catalog-error-scope-files.txt` and
  `.10x/evidence/.storage/2026-08-01-iceberg-catalog-error-sites.tsv`.
- 2026-08-01: The initial locked all-target Iceberg check passed with two unused-import warnings
  left by the split; removed only those imports. Focused Glue and REST tests, the complete Iceberg
  library suite, built-in-driver tests, benchmark tests, strict targeted Clippy, and the locked
  workspace all-target check then passed. `cargo fmt --all -- --check` also passed.
- 2026-08-01: Did not invoke graph tooling, per the orchestration constraint, and did not commit or
  push. Tracked and untracked patch-whitespace checks passed. This ticket remains active for
  independent review.
- 2026-08-01: Independent closure review compared every moved implementation and test against the
  deleted files, audited both error ledgers, and passed with no finding. The orchestrator mapped
  every acceptance criterion to the recorded evidence and closed the ticket.

## Blockers

None.

## Evidence

- Layout and composition: `rg --files crates/cdf-source-iceberg/src/catalog` returns only
  `mod.rs`, `rest.rs`, and `glue.rs`; `test ! -e crates/cdf-source-iceberg/src/glue.rs` succeeds.
  Lines 186-188 of `catalog/mod.rs` contain the registry's one filesystem, one REST, and one Glue
  registration.
- Naming boundary: a Rust source scan for both client names finds
  `AwsIcebergGlueCatalogClient` in the Iceberg definition/re-exports and its two repository
  consumers. `AwsGlueCatalogClient` remains only in `cdf-source-glue` and its explicit
  built-in-driver alias.
- Error ownership: the exact pre-move and post-move
  `rg -n -- 'CdfError::[a-z_]+|ErrorKind::|\.map_err\('` inventories both contain 90 sites.
  The final three-file scope and all sites are durably listed in the two evidence storage files
  named in the journal. This supports preservation of constructor/mapping coverage; the focused
  assertions below support preservation of observable classifications and pointer mapping.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-iceberg
  catalog::glue::tests::error_classification_and_pointer_mapping_are_exact --lib --locked -j 12
  -- --exact`: 1 passed, 0 failed.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-iceberg catalog::rest::tests:: --lib --locked -j
  12`: 2 passed, 0 failed.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-iceberg --lib --locked -j 12`: 43 passed, 0
  failed. This includes the real driver Glue binding/pinned-table semantics and typed-error
  round-trip coverage.
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p cdf-builtin-drivers --lib --locked -j
  12`: 3 passed, 0 failed.
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p cdf-benchmarks --lib --locked -j 12`:
  27 passed, 0 failed, including compilation through the real Iceberg driver.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-source-iceberg -p cdf-builtin-drivers -p
  cdf-benchmarks --all-targets --locked --no-deps -j 12 -- -D warnings`: passed.
- `CARGO_BUILD_JOBS=12 cargo check --workspace --all-targets --locked -j 12`: passed.
- `cargo fmt --all -- --check`: passed.
- `git diff --check` passed for tracked changes; `git diff --no-index --check /dev/null <file>`
  produced no whitespace diagnostics for each new module, ticket, scope manifest, and site ledger
  (the expected exit code is 1 because each file differs from `/dev/null`).

## Review

### Independent Closure Review — 2026-08-01

#### Findings

- None. No critical, significant, minor, or nit finding survived review.

#### Verdict

**Pass.** Direct comparison with the deleted `catalog.rs` and `glue.rs` shows a behavior-only
move: shared catalog implementation is byte-for-byte preserved, and the REST and Glue bodies
differ only for required module visibility/path qualification and the authorized
`AwsGlueCatalogClient` to `AwsIcebergGlueCatalogClient` rename. All six former catalog tests and
the one former Glue-client test remain present with unchanged assertions (four shared, two REST,
one Glue). The standard registry still registers filesystem, REST, and Glue exactly once and keeps
the same kind dispatch. Repository Rust-source search finds the new Iceberg name only at its
definition/re-exports and the two updated consumers; the old name remains only with the independent
`cdf-source-glue` client and its explicit external-table alias. The split modules depend only on
Iceberg-local, neutral AWS/HTTP/kernel/memory/object-access/runtime authorities.

Typed error kinds, retry branches, cancellation, response bounds, retained-memory charges, and
byte/object accounting are unchanged with their moved bodies. The frozen baseline is reproducible
as 75 sites in old `catalog.rs` plus 15 in old `glue.rs`; current source contains 90 sites, and all
90 ledger path/line, family, site-kind, and disposition entries match the live files. The recorded
layout, naming, focused-test, compile, lint, formatting, workspace-check, and patch-hygiene claims
are internally consistent and appropriately bounded. Status remains active for orchestrator
closure.

#### Residual Risk

- This independent review did not rerun the executor's tests or quality commands. Its behavioral
  conclusion rests on direct old/new implementation and assertion parity plus the ticket's
  recorded passing executions.
- Removing the old public Iceberg client name is intentionally source-incompatible for downstream
  consumers outside this repository; that is the ticket's explicitly ratified no-alias contract,
  not accidental API drift.

Closure judgment: every acceptance criterion is supported by executor evidence and the independent
review verdict is Pass. No follow-up remains unowned.

## Retrospective

- Co-locating each catalog binding with its protocol types, provider adapter, and focused tests
  makes the REST/Glue boundary visible without introducing a new abstraction or changing the
  shared catalog execution path.
- The only compile fallout was two shared-module imports made obsolete by the split; removing them
  was sufficient. No error constructor, mapping, registry branch, or consumer behavior required
  repair.
- Freezing an exact error-site baseline before the move and pairing it with focused semantic tests
  made the no-reclassification requirement directly falsifiable. The initial manual count was
  wrong and was corrected immediately from the reproducible scan; future structural work should
  treat generated inventories, not transcribed counts, as authority.
- No blockers or follow-up work were discovered. The only residual risk is the explicitly
  ratified source incompatibility of removing the ambiguous old public client name.
