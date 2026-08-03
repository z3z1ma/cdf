Status: draft
Created: 2026-08-03
Updated: 2026-08-03

# SQL source commons

## Status and purpose

This draft defines the shared relational-source mechanics to extract before a MySQL source is
implemented. It is not authority for a universal SQL dialect or for changing the active finite
connector tickets. It becomes executable only after the current connector wave's overlapping
source work is stable and the exact extraction diff is bounded.

The objective is to let PostgreSQL, SQLite, ClickHouse, and MySQL share source-neutral relational
scan planning without copying lifecycle code or erasing server-specific correctness/performance.

## Governing principle

`cdf-source-sql` owns **relational intent and validation**. A concrete source crate owns **its SQL,
catalog, consistency, transport, decoding, and errors**.

No first-party source owns the broad driver kind `sql`. Driver identity remains concrete:
`postgres`, `sqlite`, `clickhouse`, `mysql`, and future backend ids.

## Scope

- typed table identity and catalog-observation normalization;
- common projection, filter, cursor, tie-break, and finite window scan intent;
- validation of stable field/key/cursor relationships;
- source-neutral pushdown result vocabulary and residual obligations;
- canonical plan/explain fragments and identity hashing for shared intent;
- reusable conformance fixtures for query-shaped table sources;
- removal of the constant-only Postgres `dialect=postgres` user option;
- an extraction seam proven by existing sources before MySQL consumes it.

## Explicit exclusions

- a `SqlSourceDriver` that switches on a dialect id;
- a universal `SqlDialect` responsible for complete server query generation;
- common catalog SQL, information-schema assumptions, or database type names;
- common identifier quoting or string literal escaping without a narrowly typed multi-implementation
  contract;
- a shared wire client, connection pool, transaction API, row decoder, Arrow builder, or retry loop;
- arbitrary authored SQL, joins, aggregation pipelines, stored procedures, or server-side DDL;
- a common snapshot protocol across PostgreSQL, SQLite, ClickHouse, and MySQL;
- moving adapter errors into untyped string wrappers;
- source-specific branches in `cdf-project`, `cdf-cli`, `cdf-engine`, or generic runtime code.

## Existing evidence

The extraction is justified by three live implementations, not anticipated reuse:

- PostgreSQL combines pg_catalog discovery, binary COPY, query/cursor planning, and protocol casts;
- SQLite combines PRAGMA catalog discovery, read-transaction semantics, identifier handling, and
  typed row decoding;
- ClickHouse combines system catalog discovery, ArrowStream types, HTTP query generation, and
  cursor/projection planning.

They independently express the same high-level scan question while differing in every physical
answer. MySQL should validate the shared high-level model rather than cause a fourth copy.

## Proposed crate boundary

`cdf-source-sql` SHOULD be a low, source-facing library crate depending only on neutral CDF kernel/
runtime types and serialization utilities required by the compiled intent. It MUST NOT depend on
any database client, `cdf-project`, `cdf-cli`, `cdf-engine`, or a concrete source crate.

Concrete source crates depend on `cdf-source-sql`; the commons never depends back on them.

If dependency-wall evidence shows the vocabulary belongs in `cdf-runtime` while implementation
helpers belong in `cdf-source-sql`, split them deliberately. Do not place a public type in runtime
merely to avoid a small leaf dependency.

## Shared typed vocabulary

### Relational table identity

A neutral table identity MUST preserve ordered namespace components and table name without
assuming `catalog.schema.table`, two-part SQLite names, or ClickHouse database semantics. The
adapter validates and renders its supported namespace arity.

The common type MUST NOT contain quoted SQL. It may include:

- normalized source-owned identity components;
- exact authored/display identity for explain/provenance;
- a source-provided stable catalog identity hash;
- optional immutable generation/snapshot evidence when already modeled by the adapter.

### Catalog observation

The common catalog observation should describe, in CDF vocabulary:

- ordered fields with source name, canonical Arrow type, nullability, semantic/physical provenance,
  and stable ordinal;
- primary/unique key observations with source evidence strength;
- supported cursor candidates and ordering/nullability facts;
- table/view/materialized-view kind as a typed capability, not a free-form server row;
- observation identity/version/hash and source-specific opaque attestation only when required.

Adapters own the queries and native rows used to construct this observation. The common layer owns
structural validation: unique ordinals/names, exact field references, canonical order, nonempty
identity, and hash consistency.

### Relational scan intent

The common compiled intent MUST be free of server SQL and contain:

- table identity binding;
- ordered output projection by stable source field identity;
- normalized predicates with per-predicate `Exact`, `Inexact`, or `Unsupported` pushdown status;
- residual predicate obligations retained for native CDF execution;
- optional finite cursor lower/upper bounds;
- deterministic tie-break key and null ordering where applicable;
- limit/sample intent only when semantics remain exact;
- expected source schema/catalog binding hash;
- canonical ordering claim and partition/window identity;
- a version and canonical hash.

The intent does not authorize a feature merely because it can represent it. Each adapter advertises
capabilities and must fail planning when it cannot render/execute the exact semantics.

### Cursor/window plan

The common layer owns rules shared by current database sources:

- a cursor field and every tie-break field must exist in the observed schema;
- key/tie-break fields must not be silently projected away;
- lower and upper bounds have explicit inclusive/exclusive semantics;
- null cursor policy is explicit and cannot depend on backend default ordering;
- cursor value kind must match the canonical Arrow/source mapping;
- ordering and position aggregation must be deterministic;
- authored cursor lag is applied exactly once by the current checkpoint/window authority;
- an adapter cannot claim exact cursor pushdown if server collation/coercion changes comparison.

Backend-specific value binding and literal syntax remain adapter-owned. Prefer parameters/native
binary binds where the client supports them; common code must not stringify values to gain reuse.

## Narrow adapter interfaces

Implementation SHOULD use focused functions/traits only where at least the existing three sources
need the exact same call shape. Candidate seams are:

- validate/normalize a source-built catalog observation;
- compile a neutral scan request into `RelationalScanIntent`;
- ask an adapter renderer to return a typed `RenderedRelationalQuery` containing statement text,
  ordered bound values, and declared pushdown fidelity;
- validate that the rendered query covers the intent without losing residual obligations;
- produce source-neutral explain metadata.

The renderer interface MUST NOT expose a bag of dialect flags. Separate adapter implementations
own their identifier policy, placeholders, casts, aliases, snapshot clauses, and server settings.
If two backends need incompatible phases, the common interface should be smaller rather than add
optional methods.

## Concrete adapter ownership

Each database source retains:

- connection/secret/egress configuration and health probes;
- catalog queries, permission interpretation, and object-kind eligibility;
- source type-to-Arrow mapping and semantic/physical metadata;
- SQL identifier validation/quoting and parameter/literal rendering;
- source-specific casts used to make binary/text decoding lossless;
- snapshot/read consistency, transaction/session setup, and topology constraints;
- physical partitions and concurrency decisions;
- transport/client, cancellation, retry, rate/quota, and memory accounting;
- binary/native decoding and pre-contract quarantine evidence;
- typed error ownership and remediation;
- direct-library roofline measurement.

## Postgres `dialect` cleanup

The Postgres source option `dialect` currently has one possible value—`postgres`—and duplicates the
driver id. The extraction ticket SHOULD remove it from:

- option JSON schema and generated schema artifacts;
- option decode/validation and redacted option output;
- add/discovery-generated configuration;
- partition metadata where it does not protect a distinct artifact version;
- tests, examples, docs, and fixtures that represent the first-party Postgres driver.

If existing serialized compiled source plans are an active compatibility surface at execution
time, the ticket MUST establish whether removing the redundant metadata requires a driver/plan
version change. It MUST NOT keep a public no-op option merely to avoid updating fixtures.

Generic synthetic test values named `sql` that genuinely test a category rather than Postgres
identity must not be mechanically rewritten.

## MySQL admission rule

MySQL source implementation MUST begin only after:

1. the shared intent/validation API is extracted from stable Postgres/SQLite/ClickHouse behavior;
2. those existing sources pass focused unchanged-behavior tests through the common layer;
3. dependency walls show no concrete client/dialect leaked into the commons;
4. the MySQL ticket identifies native binary protocol, snapshot consistency, catalog, type sheet,
   cursor semantics, and direct-library roofline independently.

MySQL is the first extension proof, not the excuse to invent unobserved extension points.

## Failure and error ownership

- Structural errors in a source-neutral intent are Contract when authored/compiled invalidly and
  Internal when a validated adapter output violates the common API.
- Server syntax, permissions, catalog drift, transport, decode, and consistency failures remain
  adapter-owned and preserve native provenance.
- The common layer MUST NOT convert a typed adapter error into a generic “SQL error.”
- Unsupported predicates/casts are planning outcomes with remediation, not execution surprises.
- Query rendering must validate identifiers and bound values before network I/O.

## Acceptance scenarios

1. Given the same canonical table schema and scan intent, the existing three adapters consume one
   shared validated intent while rendering distinct correct server queries.
2. Given a filter whose server comparison is inexact, the adapter records `Inexact` and native CDF
   residual execution remains in the compiled plan.
3. Given a projected scan with cursor/tie-break fields, the common validator prevents removal or
   reordering that would make checkpoints nondeterministic.
4. Given a backend with incompatible namespace/quoting rules, its adapter handles them without a
   common dialect flag or source-specific branch in the commons.
5. Given malformed catalog ordinals, duplicate names, or mismatched type metadata, the common
   observation validator rejects the adapter output before plan publication.
6. Given an existing Postgres resource, removing `dialect=postgres` changes no data, cursor,
   package, or error semantics beyond the intentional configuration/plan version effect.
7. Given a synthetic fourth relational source, it can implement the narrow renderer/catalog seams
   and pass common conformance without copying shared validation or editing generic runtime code.
8. Given MySQL implementation, all server-specific binary protocol, type, collation, snapshot, and
   error logic remains in `cdf-source-mysql`.

## Validation and evidence

- focused unit/property tests for canonical intent and catalog validation;
- unchanged-output/golden tests for existing compiled source identities, with explicitly approved
  version/hash changes separated from accidental drift;
- existing Postgres/SQLite/ClickHouse focused source and live tests;
- dependency/forbidden-import checks;
- one synthetic relational renderer conformance proof;
- MySQL connector certificate and direct-library roofline in its own later ticket.

No entire workspace test suite is required for each extraction repair. One final affected-boundary
certificate is sufficient after the extraction stabilizes, consistent with the active connector
program's economical validation policy.

## Open blockers

- exact module/crate split after a duplication map against the final stable connector sources;
- compatibility/version effect of removing `dialect` from serialized source plans;
- whether ClickHouse's query model shares enough exact renderer structure or should consume only
  the neutral intent/catalog validators;
- fresh MySQL protocol and snapshot research.

## References

- `.10x/research/2026-08-03-cdc-semantic-dsl-core-readiness-audit.md`
- `.10x/specs/source-extension-runtime-contract.md`
- `.10x/specs/catalog-task-source-commons.md`
- `.10x/decisions/database-source-kind-identity.md`
- `.10x/knowledge/source-destination-extension-invariant.md`
- `.10x/specs/database-connector-roofline.md`
