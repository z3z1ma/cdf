Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Target: .10x/tickets/done/2026-07-07-declarative-postgres-sql-resource-execution.md
Verdict: pass

# Declarative Postgres SQL resource execution review

## Target

Review of the declarative Postgres SQL source execution implementation across:

- `crates/cdf-dest-postgres/src/source.rs`
- `crates/cdf-dest-postgres/src/live_tests.rs`
- `crates/cdf-declarative/src/sql_runtime.rs`
- `crates/cdf-declarative/src/compiled.rs`
- `crates/cdf-declarative/src/tests.rs`
- `crates/cdf-conformance/src/resource/execution.rs`

## Assumptions tested

- SQL resource opening must require explicit runtime dependencies and must not read ambient environment state.
- Predicate pushdown must not concatenate raw predicate strings into SQL.
- Capability claims must narrow when a SQL resource is not the supported Postgres table-backed subset.
- Partition metadata must be treated as an executable contract and revalidated before opening.
- Cursor positions must be emitted only from observed projected rows and must fail closed on missing or NULL cursor fields.
- The public conformance API must remain semver-compatible for this slice.

## Findings

- Significant: An initial public `SourcePositionRequirement::Cursor` conformance API addition would have been a semver break. `cargo semver-checks` caught this, and the implementation was changed to keep the public API stable while asserting cursor source-position emission in the live Postgres source test.
- Significant: Initial text/date/timestamp predicate parsing could have treated unquoted text-like literals as exact structured pushdown. Review tightened predicate parsing so text/date/timestamp exact pushdown requires quoted literals and numeric/bool exact pushdown requires bare literals.
- Significant: Initial timestamp-millisecond cursor conversion used a saturating multiply when converting to microseconds. Review changed this to checked multiplication with a data error on overflow.
- Significant: The source resource stores a connection string. Review added a custom `Debug` implementation that redacts `database_url` and a regression test proving the resolved secret is not formatted.
- Minor: Concurrent ephemeral local Postgres startup raced under full package testing. The live test harness now serializes local cluster startup only; tests reran successfully.

## Verdict

Pass. The implementation is within the ticket scope, keeps `lib.rs` roots thin, preserves public API compatibility, fails closed for unsupported SQL shapes, validates executable metadata before database access, uses validated identifiers and driver parameters for SQL construction, and has focused live/conformance/quality evidence.

## Residual risk

- The runtime uses synchronous `postgres::Client` inside async `ResourceStream::open`; acceptable for this first source-execution slice, but not the final Chapter 6 runtime shape.
- `cargo-geiger` did not complete in this environment. First-party source scan found no unsafe blocks/raw pointers in the touched surface, but this review does not claim a fresh full dependency unsafe census.
- The code remains table-snapshot only. Arbitrary SQL queries, SQL incremental scan semantics beyond cursor position emission, and CLI/package orchestration remain intentionally excluded.
