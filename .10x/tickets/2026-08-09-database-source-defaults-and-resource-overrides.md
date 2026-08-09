Status: done
Created: 2026-08-09
Updated: 2026-08-09
Parent: `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`

# Database source defaults and resource overrides

## Scope

Make operational database connector controls ergonomic at both the configured-source and resource
levels. Resolve every supported control as built-in default, then source default, then explicit
resource override. Tighten SQLite so table/query resources do not author row/byte schema-sampling
bounds; catalog and prepared metadata remain authoritative, while the discovery command's own
bounded request is the only observation budget for ambiguous SQLite expressions.

Implement the current MongoDB, PostgreSQL, SQLite, and ClickHouse surfaces and make the open MySQL
contract require the same precedence from its first implementation.

## Non-goals

A universal connector option grammar; moving relation identity or query-specific semantics such as
table, query, filter, pipeline, projection, hint, collation, `let`, comments, stable keys, or
temporal field mappings into source configuration; compatibility aliases for the unshipped model.

## Acceptance Criteria

- SQLite table discovery remains metadata-only and authored SQLite resources reject
  `discovery_records`/`discovery_bytes`; source and resource operational controls resolve with
  resource precedence.
- MongoDB source defaults support schema/discovery budgets, cursor/output batching, operation
  timeout, read concern, and read preference; resources may override each independently.
- PostgreSQL source defaults support transaction isolation, transaction-local timeouts, output
  batching, and search path; resources may override each independently.
- Current ClickHouse transport controls support source defaults and resource overrides, and the
  MySQL contract requires the equivalent model for its adapter-owned controls.
- Resolved values remain validated, secret-safe, identity-bearing compiled-plan inputs and focused
  behavior tests prove default, source-only, and resource-override resolution.

## References

- `.10x/specs/sqlite-native-query-source.md`
- `.10x/specs/mongodb-native-extraction-surface.md`
- `.10x/specs/postgres-native-query-source.md`
- `.10x/specs/clickhouse-native-query-source.md`
- `.10x/specs/mysql-native-query-source.md`
- `.10x/decisions/connector-native-capability-before-commons.md`

## Assumptions

- User-ratified on 2026-08-09: connection/session/performance policies SHOULD be reusable source
  defaults and remain overridable per resource; `read_concern` and `read_preference` are explicit
  examples.
- User-ratified on 2026-08-09: ordinary SQL catalog/result metadata does not use bounded row
  discovery; SQLite dynamic-expression observation is an exceptional discovery aid, not authored
  SQL resource authority.
- Record-backed: performant adapter defaults and all existing safety ceilings remain unchanged.

## Journal

- 2026-08-09: Opened from the user's explicit correction of the resource-only placement. The
  existing adapter-native grammars remain independent; only precedence semantics are shared.
- 2026-08-09: Implemented source defaults plus resource overrides in MongoDB, PostgreSQL, SQLite,
  and current ClickHouse table resources. Resolved values are serialized into redacted and physical
  compiled-plan evidence. Driver versions and built-in catalog identities were advanced because
  the unshipped compiled surface changed incompatibly.
- 2026-08-09: Removed SQLite `discovery_records` and `discovery_bytes` from source/resource schemas,
  compiled artifacts, and `cdf add`. Table discovery remains catalog-only; ambiguous query
  expressions now consume only the caller-owned discovery request budget, with fixed internal
  bounds retained for preflight attestation.
- 2026-08-09: Updated the real `cdf_sandbox` project so SQLite/PostgreSQL operational policy and
  Atlas read/discovery policy live on configured sources. Resource files retain only relation and
  query-specific intent, with MongoDB `read_concern` left as a deliberate pipeline override.

## Blockers

None for closure. The supplied Atlas AWS session credentials now fail authentication, so fresh live Atlas
execution was unavailable; this does not block the independently exercised inheritance contract.

## Evidence

- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-source-sqlite -p cdf-source-mongodb
  -p cdf-source-postgres -p cdf-source-clickhouse --lib`: 142 passed, two explicitly ignored live
  environment tests, zero failures. This proves built-in defaults, source-only values, resource
  override precedence, option rejection, serialization, and the existing adapter laws.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-builtin-drivers
  catalog_matches_the_data_driven_first_party_fixture`: one passed, proving regenerated descriptor
  and option-schema identities match the shipped catalog fixture.
- Strict affected-package Clippy with all targets/features and `-D warnings`: passed for all four
  source crates and `cdf-builtin-drivers`.
- Fresh release build: `DUCKDB_DOWNLOAD_LIB=1 cargo build --release -p cdf-cli --bin cdf` passed in
  2m10s.
- Release sandbox `cdf validate`: 28/28 resources statically valid with zero errors and no external
  effects. Release sandbox compile/plan of `sqlite_native.events_snapshot` succeeded; its compiled
  artifact contains the four effective values inherited from `cdf.toml` while the resource file
  declares none.
- Release sandbox `cdf run sqlite_native.events_snapshot`: committed and verified 100,000 rows in
  782 ms through package, receipt, checkpoint, and destination gates.
- Release PostgreSQL compile connected and described the query, then correctly stopped at the
  existing explicit schema-promotion gate. Atlas compile reached the driver but failed MongoDB
  authentication with the expired supplied session credentials; no live Atlas success is claimed.

## Review

Pass from direct diff and behavior review. Relation/query semantics remain resource-only; only
uniform operational policy inherits. Source defaults are validated before resolution so an invalid
source value cannot be hidden by a valid resource override. Resolved values, not ambient config,
own compiled identity. Residual risk is limited to the unavailable live Atlas credential cell.

## Retrospective

The original resource-only design confused a value's effect with its most convenient placement.
Connection/session/performance policy is usually uniform across a configured source, while query
shape is not. Modeling precedence explicitly makes sparse resources the common path without
introducing a universal connector grammar. SQLite also demonstrated that an exceptional internal
observation bound should not automatically become an authored connector knob.
