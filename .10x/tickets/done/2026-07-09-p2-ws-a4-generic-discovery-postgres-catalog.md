Status: done
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-08-p2-ws-a-discovery-compiler-stage.md
Depends-On: .10x/decisions/data-onramp-schema-discovery-reconciliation.md, .10x/specs/data-onramp-schema-intelligence.md, .10x/specs/types-contracts-normalization.md, .10x/tickets/done/2026-07-09-p2-ws-a3-local-parquet-discover-autopin.md

# P2 WS-A4 generic discovery dispatcher and Postgres catalog probe

## Scope

Replace the Parquet-specific project/CLI discovery doorway with a source-archetype dispatcher, then add the first non-file discovery probe: `cdf schema discover <resource>` for declarative Postgres table resources in `SchemaSource::Discover` mode.

Discovery remains a bounded compiler-stage probe. This ticket broadens the shape from "local Parquet helper called directly by the CLI" to "project discovery dispatcher with per-source probes" without changing run determinism or package-producing execution semantics.

Owned write scope:

- `crates/cdf-project/src/schema_discovery.rs` and any small sibling modules needed to split source-archetype probe code out of a monolithic file.
- `crates/cdf-dest-postgres/src/**` and `crates/cdf-declarative/src/sql_runtime.rs` only for a catalog-discovery helper, public target resolution helper, and focused tests.
- `crates/cdf-cli/src/schema_command.rs` and focused CLI tests to route `cdf schema discover` through the generic dispatcher and `ProjectContext` secret provider.
- This ticket, its evidence record, its review record, and parent progress notes.

## Acceptance criteria

- `cdf schema discover <resource>` no longer imports or calls a Parquet-only project function directly. It calls a generic project discovery API that dispatches by compiled resource plan/source kind.
- The existing local single-file Parquet discover behavior remains unchanged: non-mutating CLI discovery, normalized schema output, candidate snapshot path/hash, source identity, and no project writes.
- Declarative Postgres table resources with no declared schema can be probed through `cdf schema discover <resource>` when the source connection is a `secret://` reference resolvable by the project secret provider.
- Postgres discovery reads catalog metadata only. It MUST NOT query user table rows, write `.cdf/schemas`, write `cdf.lock`, create packages, touch destinations, or commit checkpoints.
- The Postgres probe supports only catalog types that the current Postgres source execution path can materialize without extra semantics:
  - `boolean`/`bool` -> Arrow `Boolean`;
  - `smallint`, `integer`, and `bigint` families -> Arrow `Int64` with `cdf:physical_type` metadata naming the observed Postgres type;
  - `real` and `double precision` families -> Arrow `Float64` with `cdf:physical_type`;
  - text-like identifiers such as `text`, `character varying`, `character`, and `uuid` -> Arrow `Utf8`;
  - `date` -> Arrow `Date32`;
  - `timestamp without time zone` -> Arrow `Timestamp(Microsecond, None)`;
  - `timestamp with time zone` -> Arrow `Timestamp(Microsecond, Some("UTC"))`.
- Unsupported Postgres catalog types fail discovery with the resource id, column name, observed type, and a remediation that says this source type is not yet supported by the Postgres discovery/execution slice.
- Postgres nullable catalog state becomes Arrow field nullability. Physical column names are normalized through `namecase-v1`; original names are preserved in `cdf:source_name`, and observed catalog types are preserved in `cdf:physical_type`.
- Postgres schema snapshot metadata identifies the probe as a Postgres catalog probe and records source kind/dialect/table without recording the resolved DSN or secret value.
- CLI JSON and human reports expose the same write booleans as A3 and show no secret value, DSN, password, username-bearing URI, or unredacted connection string.
- REST, arbitrary SQL query resources, Python, WASM, future Avro-like file formats, and non-Postgres SQL dialects remain fail-closed through the dispatcher with source-kind-specific unsupported-probe messages. They MUST NOT fall through to the Parquet path.
- `cdf plan`/`cdf run` SQL auto-pin is not required in this ticket unless the implementation falls out as a trivial call to the generic API. If omitted, the exclusion must remain explicit in the evidence and WS-A parent notes.

## Evidence expectations

Record focused evidence for:

- project/discovery unit tests proving the generic dispatcher preserves local Parquet behavior;
- live or fixture-backed Postgres catalog discovery tests proving supported type mapping, nullability, `cdf:source_name`, `cdf:physical_type`, normalizer metadata, and secret redaction;
- CLI `cdf schema discover` tests for a Postgres table resource using a project secret-provider reference without leaking the resolved DSN;
- negative tests for unsupported Postgres catalog types and at least one unsupported source archetype through the generic dispatcher;
- `cargo test -p cdf-dest-postgres -p cdf-declarative -p cdf-project -p cdf-cli --locked` or narrower focused commands plus a justified broader follow-up;
- `cargo fmt --all -- --check`;
- `cargo clippy -p cdf-dest-postgres -p cdf-declarative -p cdf-project -p cdf-cli --all-targets --locked -- -D warnings`;
- `git diff --check`;
- scoped `jscpd` and `rust-code-analysis-cli` over touched Rust files, with clone/complexity findings classified;
- scoped Semgrep and Gitleaks over touched source files;
- CodeQL through `tools/codeql-rust-quality.sh` using reusable `target/quality/codeql-db-rust` if Rust source changed;
- supply-chain checks from `QUALITY.md` if Cargo metadata changes.

## Explicit exclusions

This ticket does not implement SQL `plan`/`run` auto-pin unless it is trivial after the dispatcher lands, REST sample-page discovery, Python generator discovery, WASM boundary discovery, Avro discovery, CSV/JSON/NDJSON sampling, remote Parquet ranged discovery, multi-file schema union/variance, `cdf schema pin|show|diff`, lockfile update semantics, `cdf add`, ad-hoc mode, or S4/S5 conformance closure.

This ticket also does not expand the Postgres source execution type set beyond what is required for the mapped catalog types above. Decimal/numeric source materialization remains a separate WS-A/WS-B integration slice because snapshots must not claim types the next run cannot currently execute.

## Progress and notes

- 2026-07-09: Opened after user clarified that discovery is critical across the product and must serve databases, declarative REST, future Avro, Python generator resources, and WASM boundaries rather than remaining a Parquet convenience. Source inspection found `crates/cdf-cli/src/schema_command.rs` calls `discover_local_parquet_resource_schema` directly, while plan/run auto-pin helpers remain local-Parquet-specific.
- 2026-07-09: Shaping inspection found SQL resources compile missing schema as `SchemaSource::Discover` plus an empty Arrow schema, and `ProjectContext::secret_provider()` already supplies the `secret://` resolver used by SQL runtime paths. The current Postgres source reader widens integers to `Int64` and floats to `Float64`, so this ticket intentionally discovers an executable widened Arrow schema instead of literal narrower catalog widths.
- 2026-07-09: Worker implementation added the generic project discovery dispatcher, routed `cdf schema discover` through the dispatcher and project secret provider, added catalog-only Postgres table discovery, and kept A3 local Parquet discovery behavior covered through the dispatcher. Focused and broad verification passed: Postgres catalog tests, project dispatcher tests, CLI schema-discover tests, `cargo test -p cdf-dest-postgres -p cdf-declarative -p cdf-project -p cdf-cli --locked`, `cargo fmt --all -- --check`, `cargo clippy -p cdf-dest-postgres -p cdf-declarative -p cdf-project -p cdf-cli --all-targets --locked -- -D warnings`, and `git diff --check`.
- 2026-07-09: SQL `plan`/`run` auto-pin remains explicitly excluded. The dispatcher shape is present, but safely auto-pinning declarative SQL at package-producing execution requires source-name-aware Postgres execution and pinned discovered-schema handling beyond this catalog-only CLI probe slice.
- 2026-07-09: Closed with evidence `.10x/evidence/2026-07-09-p2-ws-a4-generic-discovery-postgres-catalog.md` and review `.10x/reviews/2026-07-09-p2-ws-a4-generic-discovery-postgres-catalog-review.md`. Parent verification additionally ran full workspace `cargo clippy --workspace --all-targets --locked -- -D warnings`, full workspace `cargo test --workspace --locked --no-fail-fast`, CodeQL through the reusable wrapper/database path, Semgrep, Gitleaks, `jscpd`, `rust-code-analysis-cli`, and supply-chain gates.

## Blockers

None.
