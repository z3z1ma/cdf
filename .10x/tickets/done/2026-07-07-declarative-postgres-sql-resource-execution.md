Status: done
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-conformance-chaos-golden.md
Depends-On: .10x/tickets/done/2026-07-05-postgres-destination.md, .10x/tickets/done/2026-07-06-postgres-live-execution.md, .10x/tickets/done/2026-07-06-resource-conformance-suite-foundation.md, .10x/tickets/done/2026-07-07-declarative-rest-resource-execution.md

# Add declarative Postgres SQL resource execution

## Scope

Implement the first openable Tier-0 declarative SQL source runtime for Postgres table resources over the public `ResourceStream` and `QueryableResource` contracts.

Owns:

- `crates/cdf-dest-postgres/**` for a focused Postgres source runtime, safe table-scan SQL construction, driver-backed execution, row-to-Arrow conversion, and live local Postgres source tests.
- `crates/cdf-declarative/**` for a thin adapter from `CompiledResourcePlan::Sql` table plans to the Postgres runtime using explicit runtime dependencies.
- `crates/cdf-conformance/**` only for reusable execution conformance cases or assertion helpers needed by Postgres SQL resources.
- `.10x/` evidence, review, and ticket records for this child.

Keep crate roots thin. Add focused modules such as `source` or `sql_runtime` rather than expanding `lib.rs` or `compiled.rs` into monoliths.

## Acceptance criteria

- Declarative SQL resources with `dialect = "postgres"` and `table = "table"` or `table = "schema.table"` can be converted into an openable Postgres `ResourceStream` through explicit runtime dependencies. Connection strings MUST resolve through a `SecretProvider` or similarly explicit dependency; no ambient database URL, environment lookup, or implicit network access is allowed.
- The default `CompiledResource::open` behavior for SQL resources without runtime dependencies remains a clear error rather than silently opening a database connection.
- This first slice supports table-backed SQL resources only. Declarative SQL resources using `query` MUST fail closed in the runtime adapter with a clear unsupported error; safe wrapping and pushdown semantics for arbitrary queries remain out of scope.
- Non-Postgres dialects, missing connection secrets, empty connection strings, malformed table names, wrong partition metadata, empty declared schemas, unsupported Arrow/Postgres type mappings, and schema/runtime row mismatches MUST fail closed.
- Table-scan SQL construction MUST use validated `PostgresTarget` and `PostgresIdentifier` values for every table, schema, projection, order key, and cursor column. Requested predicates MUST NOT be concatenated as raw SQL.
- The runtime MUST preserve the current SQL capability claims by carrying negotiated table-scan projection, exact simple filters, ordering, and limit from `ScanRequest` into `PartitionPlan` metadata or an equivalent typed partition artifact consumed by `open`. Unsupported or unparseable predicates MUST remain unsupported and MUST NOT be smuggled into SQL.
- Exact predicate pushdown in this slice is limited to structured field/operator/literal predicates for declared fields and operators already advertised by `CompiledResource`: `=`, `>`, `>=`, `<`, and `<=`. Literal parsing MUST be type-aware for the supported declared schema subset and MUST use driver parameters for values.
- Cursor-bearing table resources emit `SourcePosition::Cursor` from the maximum observed cursor field in emitted rows when the cursor value is representable by current `CursorValue` shapes. Cursor fields missing from accepted rows MUST fail closed.
- Snapshot table opens emit `RecordBatch` payloads with correct resource id, partition id `sql`, unique batch ids, declared schema hash, row counts, byte counts, and Arrow schemas matching the declared resource schema.
- Live tests MUST use the existing local Postgres harness or an equivalent deterministic ephemeral/local Postgres fixture. Tests MAY skip when neither `TEST_DATABASE_URL` nor local Postgres binaries are available, matching the existing Postgres destination pattern; non-live planning and fail-closed tests MUST still run.
- Resource execution conformance covers the Postgres SQL table runtime for descriptor coherence, capability truth, batch metadata, payload schema, data completeness for the requested scan, and cursor-position emission.
- Existing declarative planning-level SQL behavior remains source-compatible except where a capability claim is proven unsafe; any intentional capability narrowing MUST be recorded in this ticket before implementation proceeds.

## Evidence expectations

Record focused checks:

- `cargo fmt --all -- --check`
- `git diff --check -- . ':(exclude).gitignore'`
- `cargo test -p cdf-dest-postgres --locked --no-fail-fast`
- `cargo test -p cdf-declarative --locked --no-fail-fast`
- `cargo test -p cdf-conformance --locked resource -- --nocapture` if conformance helpers change
- `cargo clippy -p cdf-dest-postgres -p cdf-declarative -p cdf-conformance --all-targets --locked -- -D warnings`
- `cargo nextest run -p cdf-dest-postgres -p cdf-declarative -p cdf-conformance --locked`

Before closure, run relevant `QUALITY.md` gates, parallelized where practical: workspace check/test/clippy, docs, cargo-hack feature checks, cargo deny/audit/vet/OSV, Semgrep over touched crates, source-only gitleaks, direct unsafe/FFI/raw-pointer scan, dependency hygiene, and bounded mutation testing over the new Postgres source runtime and conformance assertions where feasible. Skip CodeQL for this checkpoint per the active goal instruction; do not recreate the CodeQL database.

## Explicit exclusions

No CLI `preview` or `run` widening to SQL resources, no package/checkpoint lifecycle changes, no run ledger/default ids, no `resume`, no `replay package`, no arbitrary declarative SQL `query` execution, no SQL writes, no DDL, no log-based CDC, no `cdc_apply`, no streaming supervisor, no connection pool, no vault-class secret provider, no DataFusion `TableProvider` rewrite beyond existing `QueryableResource` contracts, no non-Postgres SQL dialects, no external hosted database dependency, no live GitHub/API work, no CI workflow changes, and no `.gitignore` edits.

The MVP killer-demo path remains parent scope until SQL and REST sources can be connected through `cdf run` with explicit runtime inputs in separate product-surface children.

## References

- `VISION.md` D-1, D-2, D-7, Chapter 8, Chapter 9.2, Chapter 15, Chapter 20, and Chapter 22.
- `.10x/specs/resource-authoring-planning-batches.md`
- `.10x/specs/conformance-governance-roadmap.md`
- `.10x/specs/architecture-layering-runtime.md`
- `.10x/tickets/done/2026-07-05-postgres-destination.md`
- `.10x/tickets/done/2026-07-06-postgres-live-execution.md`
- `.10x/tickets/done/2026-07-06-resource-conformance-suite-foundation.md`
- `.10x/tickets/done/2026-07-06-resource-execution-conformance-file-sources.md`
- `.10x/tickets/done/2026-07-07-declarative-rest-resource-execution.md`
- `.10x/knowledge/quality-gate-execution.md`
- `.10x/knowledge/rust-crate-organization.md`

## Evidence

- `.10x/evidence/2026-07-07-declarative-postgres-sql-resource-execution.md`

## Review

- `.10x/reviews/2026-07-07-declarative-postgres-sql-resource-execution-review.md`

## Progress and notes

- 2026-07-07: Split from the conformance parent after REST resource execution closed. Current declarative SQL resources compile and advertise exact pushdown, and the Postgres destination crate already has safe identifier helpers plus live local Postgres infrastructure, but no source-side Postgres `ResourceStream` exists. This child makes table-backed Postgres SQL resources openable through explicit runtime dependencies without adding CLI/package orchestration or arbitrary query-resource execution.
- 2026-07-07: Do not implement in the ticket-creation turn. Assign to a worker in a later turn with the bounded write boundary above; parent owns integration review, evidence, and final commit.
- 2026-07-07: Worker execution note before dependency edits: the declarative SQL runtime adapter needs a path dependency from `cdf-declarative` to the new `cdf-dest-postgres` source runtime. If Cargo updates `Cargo.lock`, that lockfile edit is required for the requested `--locked` verification rather than unrelated scope.
- 2026-07-07: Implemented `cdf-dest-postgres` table-source runtime and `cdf-declarative` SQL runtime adapter. Table scans now carry typed Postgres scan metadata for validated projection, structured exact predicates, ordering, and limit; arbitrary declarative SQL `query` resources and non-Postgres dialects fail closed in the adapter. Runtime opening resolves connection strings only through explicit `SecretProvider` dependencies and keeps default `CompiledResource::open` fail-closed. Added row-to-Arrow conversion for the supported declarative schema subset, cursor source-position emission, partition metadata validation, and fail-closed tests for malformed metadata, missing/empty secrets, empty/unsupported schemas, malformed table names, and unstructured predicates. The existing resource execution conformance helper covers the Postgres table source's execution headers/data in the live local Postgres harness, and the live source test directly asserts cursor source-position emission without changing the public conformance API.
- 2026-07-07: During package verification, concurrent local ephemeral Postgres startup produced a `pg_ctl start failed` race in an existing live test. Serialized only local cluster startup in the Postgres live test harness, then reran the full package successfully.
- 2026-07-07: Focused checks run: `cargo fmt --all -- --check` passed; `git diff --check -- . ':(exclude).gitignore'` passed; `cargo test -p cdf-dest-postgres --locked --no-fail-fast` passed with 25 tests including live local Postgres source coverage; `cargo test -p cdf-declarative --locked --no-fail-fast` passed with 48 tests; `cargo test -p cdf-conformance --locked resource -- --nocapture` passed; `cargo clippy -p cdf-dest-postgres -p cdf-declarative -p cdf-conformance --all-targets --locked -- -D warnings` passed.
- 2026-07-07: Parent review repaired four issues before closure: removed a public conformance API addition after `cargo semver-checks` flagged it; made text/date/timestamp exact predicate pushdown require quoted literals while numeric/bool pushdown requires bare literals; replaced saturating timestamp-millisecond cursor conversion with checked overflow handling; and added `PostgresTableResource` debug redaction for the resolved database URL.
- 2026-07-07: Closure evidence recorded in `.10x/evidence/2026-07-07-declarative-postgres-sql-resource-execution.md`; closure review passed in `.10x/reviews/2026-07-07-declarative-postgres-sql-resource-execution-review.md`. Full relevant `QUALITY.md` gate set ran with CodeQL intentionally skipped per active goal instruction and `cargo-geiger` recorded as an attempted-but-hanging tool limit. Bounded mutation over the new Postgres source runtime selected 12 mutants: 9 caught, 3 unviable, 0 missed.

## Blockers

None.
