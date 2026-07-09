Status: done
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-08-p2-ws-a-discovery-compiler-stage.md
Depends-On: .10x/decisions/data-onramp-schema-discovery-reconciliation.md, .10x/specs/data-onramp-schema-intelligence.md, .10x/specs/types-contracts-normalization.md, .10x/tickets/done/2026-07-09-p2-ws-a4-generic-discovery-postgres-catalog.md

# P2 WS-A5 generic discover auto-pin for Postgres run and plan

## Scope

Replace the remaining local-Parquet-only first-use discovery preparer in CLI plan/run paths with a generic discovery auto-pin path, and make declarative Postgres table resources discovered through A4 executable by `cdf plan`, `cdf preview`, and `cdf run`.

This ticket turns the A4 catalog probe from a no-write inspection command into a package-producing compiler-stage input for the currently supported SQL table resource slice. It does not broaden discovery to REST, Python, WASM, Avro-like files, CSV/JSON/NDJSON, remote Parquet, or multi-file schema union.

Owned write scope:

- `crates/cdf-project/src/schema_discovery.rs` and focused tests for generic prepare/pin behavior.
- `crates/cdf-cli/src/run_command.rs`, `crates/cdf-cli/src/scan_command.rs`, and focused CLI tests for plan/preview/run behavior.
- `crates/cdf-dest-postgres/src/source.rs` and focused tests only as needed to accept pinned discovered schema sources and preserve source-name-aware execution.
- Small public exports or helper changes in `cdf-declarative`/`cdf-dest-postgres` only if needed by the generic prepare path.
- This ticket, its evidence record, its review record, and parent progress notes.

## Acceptance criteria

- The old `prepare_local_parquet_discover_resource` call sites in CLI plan/run are replaced by a generic `prepare_discover_resource` or equivalent that dispatches through `discover_resource_schema`.
- Existing local single-file Parquet auto-pin behavior remains unchanged: deterministic snapshot file under `.cdf/schemas`, pinned `SchemaSource::Discovered`, normalized field names, and existing negative behavior for multi-file/non-Parquet unsupported slices.
- Declarative Postgres table resources in `SchemaSource::Discover` mode auto-pin before package-producing execution. `cdf plan`, `cdf preview`, and `cdf run` MUST NOT fail with "requires a declared schema hash" solely because the resource started in discover mode.
- Postgres auto-pin writes the schema snapshot before plan/run, records the `SchemaSource::Discovered` reference in the in-memory compiled resource, and keeps the snapshot metadata from A4: probe `postgres-catalog`, source kind `sql`, dialect `postgres`, table, and `cdf:normalizer = namecase-v1`.
- Postgres source execution accepts pinned discovered schema sources as valid pinned schemas, while unpinned `SchemaSource::Discover`, `Hints`, and `Contract` remain rejected before execution.
- Source-name-aware Postgres execution is handled for normalized discovered schemas: a source column such as `"VendorID"` normalized to `vendor_id` MUST be selected from the physical source column using `cdf:source_name` metadata rather than querying `"vendor_id"` from the table.
- The CLI run path MUST use the pinned compiled resource for both engine planning and runtime resource opening, so the snapshot hash in reports and package evidence matches the discovered schema used to read rows.
- CLI JSON/human outputs for plan/run expose the existing auto-pin schema evidence without leaking the resolved Postgres DSN or secret value.
- Unsupported REST, arbitrary SQL query resources, non-Postgres dialects, Python, WASM, future Avro-like files, and multi-file/remote discovery remain fail-closed with source-kind-specific messages and no package/destination/checkpoint writes.

## Evidence expectations

Record focused evidence for:

- project tests proving generic prepare preserves local Parquet behavior and writes deterministic snapshots;
- project or CLI tests proving Postgres discover-mode resources auto-pin and produce a `SchemaSource::Discovered` schema source with A4 metadata;
- Postgres source tests proving `cdf:source_name` is used for SELECT/projection/cursor where needed;
- CLI tests proving `plan`, `preview`, and `run` work for a Postgres discover-mode table through a project secret-provider reference without secret leakage;
- negative CLI tests proving unsupported discover-mode slices fail before package/destination/checkpoint writes;
- focused affected-crate tests plus `cargo fmt`, `cargo clippy` for affected crates, `git diff --check`, `jscpd`, `rust-code-analysis-cli`, Semgrep, Gitleaks, and CodeQL if Rust source changed.

## Explicit exclusions

This ticket does not implement `cdf schema pin|show|diff`, REST sample-page discovery, Python generator discovery, WASM boundary discovery, Avro discovery, CSV/JSON/NDJSON sampling, remote Parquet ranged discovery, multi-file schema union/variance, lockfile persistence beyond existing run/plan behavior, `cdf add`, ad-hoc mode, or S4/S5 conformance closure.

This ticket also does not add unsupported Postgres catalog types such as `numeric`/decimal to the source execution slice. Those belong to the WS-A/WS-B integration that expands executable type semantics.

## Progress and notes

- 2026-07-09: Opened after user correctly objected that A4 did not make discovery remotely complete. Inspection found `crates/cdf-cli/src/run_command.rs` and `crates/cdf-cli/src/scan_command.rs` still call `prepare_local_parquet_discover_resource`, and `crates/cdf-dest-postgres/src/source.rs` still rejects `SchemaSource::Discovered` with the old "requires a declared schema hash" message.
- 2026-07-09: Implemented generic discover preparation for CLI plan/preview/run, Postgres pinned discovered-schema execution, and source-name-aware Postgres table reads. Focused and broad affected-crate verification passed; final closure evidence is recorded in `.10x/evidence/2026-07-09-p2-ws-a5-generic-discover-autopin-postgres-run.md`.
- 2026-07-09: Parent review and closure verification completed. Full workspace clippy/test, CodeQL, Semgrep, Gitleaks, jscpd, rust-code-analysis, and supply-chain gates are recorded in `.10x/evidence/2026-07-09-p2-ws-a5-generic-discover-autopin-postgres-run.md`; adversarial review is `.10x/reviews/2026-07-09-p2-ws-a5-generic-discover-autopin-postgres-run-review.md`.

## Blockers

None.
