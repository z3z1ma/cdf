Status: active
Created: 2026-08-08
Updated: 2026-08-08

# Perfect connector-native extraction before extracting common source concepts

## Context

The finite PostgreSQL, SQLite, ClickHouse, and MongoDB sources were intentionally started with
table/collection-only surfaces. Their active specifications then turned that staging boundary into
an exclusion of native queries and aggregation pipelines. A draft `cdf-source-sql` plan proposed
extracting a shared relational grammar before MySQL existed. The user rejected both constraints:
first-party connectors must be able to express serious source-native extraction, and common engine
or runtime concepts must be learned from excellent concrete adapters rather than imposed first.

## Decision

Each first-party connector owns a world-class adapter-native resource surface. MongoDB owns BSON
find filters and aggregation pipelines. PostgreSQL, SQLite, ClickHouse, and MySQL each own their
native read-query validation, catalog/discovery strategy, consistency, settings, transport, and
batch controls. CDF does not define a universal source-query grammar or a dialect-switching source.

Native source extraction produces the relation consumed by the surrounding CDF SQL resource.
Discovery and execution MUST use the same native input and options. CDF projection, predicates,
ordering, cursor windows, and limits apply to that produced relation and may be pushed only when
the adapter proves exact equivalence. Every adapter MUST retain read-only execution, finite memory
and transport bounds, credential redaction, deterministic identity, source-generation validation,
and receipt-gated checkpoint semantics.

Shared engine/runtime concepts MAY be extracted only after at least two finished adapters exhibit
the same stable contract. Extraction follows concrete implementation and evidence; it does not
block MySQL or force the public resource options to converge.

The draft `.10x/specs/superseded/sql-source-commons.md` is superseded. The CDC foundation program's former
B0/B1-before-MySQL sequence is superseded: finite MySQL is built as one adapter first, and a later
duplication audit may identify proven commons without changing adapter-native authoring.

## Alternatives Considered

### One universal SQL/query grammar

This appears consistent but either exposes only the least common denominator or grows dialect
flags for catalog, consistency, settings, types, and transport. It makes concrete adapters harder
to use and freezes abstractions before their boundaries are known.

### Table/collection-only resources

This is safe but not a credible integration surface. Users cannot express joins, aggregations,
server-side filtering, source-native functions, index hints, or operational query controls, and
CDF needlessly transfers and processes data the source can handle better.

### Extract common relational machinery before MySQL

The existing adapters already share some intent, but their physical query and runtime contracts
remain materially different. Extracting first would make MySQL validate an abstraction rather
than reveal what a fourth production-quality implementation actually shares.

## Consequences

- Connector specifications and tickets must remove arbitrary-query/pipeline exclusions directly.
- Project artifacts bind canonical native query/filter/options; they never contain credentials.
- Authored native queries are trusted project code, but CDF still enforces read-only server
  execution and rejects known write/change-stream operations at its boundary.
- Similar option names or implementation patterns may remain duplicated until evidence justifies a
  neutral typed seam.
- Connector certification expands to native query/pipeline discovery, planning, execution,
  portability, redaction, cancellation, package, replay, checkpoint, and roofline cases.
