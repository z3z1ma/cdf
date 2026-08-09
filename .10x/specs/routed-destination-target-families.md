Status: active
Created: 2026-08-07
Updated: 2026-08-07

# Routed destination target families

## Purpose

Define how one logical CDF resource may route its accepted rows or keyed effects to a bounded
family of physical destination tables, including families whose outputs have distinct logical
schemas, while retaining one resource, package, run, receipt, and checkpoint authority.

## Proposed authoring contract

The recommended envelope extension is:

```sql
RESOURCE
TARGET warehouse.events
ROUTE BY source_table MAX TARGETS 256
DISPOSITION CDC_APPLY(id) DELETE HARD
AS
SELECT source_table, id, payload
FROM upstream(source => 'mysql', mode => 'cdc', bootstrap => 'snapshot');
```

`TARGET` remains the optional logical base target. When omitted, the path-derived resource target
remains the base. `ROUTE BY` names exactly one final typed output/control field. `MAX TARGETS` is a
mandatory positive compiled resource ceiling and may only lower the resolved host/destination
ceiling.

The recommended physical name is the base target with `__<route-token>` appended to its final
component. A route value that already matches the current project token grammar is used exactly.
Every other admitted scalar value uses a lowercase human-readable slug plus a fixed SHA-256 prefix
over its typed canonical bytes. Truncation preserves the hash suffix and obeys the selected
destination's identifier limit. Distinct typed values MUST NOT collide; an unresolved collision
fails before destination mutation.

## Identity and control laws

- The canonical resource id and logical target do not change with discovered route values.
- Schema authority is keyed by project, environment, resource, and a generic output binding. An
  unrouted resource uses `primary`; a routed output binding derives from the route field's typed
  canonical value and folding version, never from a destination-specific physical name.
- The route declaration, field type, folding version, maximum count, and ordered exact
  value-to-physical-target map are package/plan/receipt identity.
- Null, nested, binary, non-canonical, or otherwise unsupported route values fail before package
  publication. There is no default or overflow target.
- The routing field is control-critical. Projection, transform, redaction, quarantine, or contract
  behavior cannot remove or rewrite it before routing. A later explicit projection MAY omit it
  from physical destination rows after routing is resolved.
- A field marked sensitive by active semantic/contract authority cannot be used as a route key.
  Physical object names and route reports are operational metadata and are not secret stores.
- Route discovery never creates authored resource files or new resource/checkpoint identities.

## Heterogeneous schema families

Every routed output binding owns an independent logical schema generation/hash, drift disposition,
promotion history, destination migration plan, and installation record. A homogeneous routed
family may bind the same schema hash repeatedly; a heterogeneous family binds each distinct schema
explicitly. The full ordered output-binding/schema/target map is plan, package, receipt, replay,
and checkpoint authority.

One authored relational query MAY be compiled independently against every admitted output schema.
If so, every explicit expression must typecheck for every output; output-specific query override
syntax is outside this contract. Runtime cannot create a new output schema, migration, or target
from observed route values. An unknown output binding fails before package publication and
checkpoint advancement until explicit discovery/compilation establishes it.

## Package and destination contract

One package contains deterministic, schema-homogeneous route partitions over its accepted rows or
final keyed effects.
Canonical route order is typed-key order followed by canonical segment order within each route.
Changing jobs, batch boundaries, fan-out membership, or destination speed cannot change route
assignment or package bytes.

A destination advertises routed-target-family support separately from ordinary single-target
support. It MUST:

- plan every physical target and its bound schema/migration before mutation;
- bind the complete output/schema/target map and per-target content/effect identities;
- commit the package atomically across all routed targets, or return an unambiguous recoverable
  package outcome with equivalent semantics;
- apply one package idempotency token across the family;
- return one receipt containing the ordered target map and truthful per-target counts/evidence;
- verify that receipt without assuming a missing target succeeded.

The resource checkpoint advances only after the receipt covers every route. Partial success is a
recoverable destination attempt, never a partially advanced resource. A destination unable to
provide this guarantee rejects the plan.

## CDC interaction

Routing occurs after complete-image/keyed-effect validation and before destination commit. A
delete is routed from its protected route authority plus exact destination key; a key-only delete
package MUST therefore retain the route value even when that value is not a destination key.
Changing the route for one logical entity is two effects only when the source truthfully supplies
the old and new route; CDF MUST NOT infer the old route from destination state.

## Failure behavior

- exceeding `MAX TARGETS` fails the epoch before package publication and advances no checkpoint;
- invalid/colliding/overlength target derivation is Contract during plan when statically knowable,
  otherwise Data during package construction;
- a destination capability or atomicity mismatch fails during plan;
- a missing route field, null route, or route mutation fails before destination mutation;
- ambiguous multi-target commit remains recovery-required and blocks checkpoint/collection.

## Acceptance scenarios

1. Two route values produce two deterministic physical targets under one logical resource and one
   receipt/checkpoint.
2. Running the resource alone or in a shared-extraction group produces identical route assignment
   and package bytes.
3. A package replay applies no routed effect twice and verifies the same receipt.
4. One routed-target failure advances no checkpoint; recovery settles already committed work
   without re-extraction when the destination can prove it.
5. Null, sensitive, colliding, or over-ceiling route values fail closed.
6. PostgreSQL and DuckDB commit all routed tables in one transaction and report per-target effects.
7. Two routes with distinct schemas retain independent authority and destination migrations under
   one atomic package/receipt/checkpoint; an unknown route creates no implicit authority.

## Ratification

The user ratified the `ROUTE BY <field> MAX TARGETS <n>` grammar, mandatory resource ceiling, `__`
separator, exact-token/slug-plus-hash fold, null/sensitive-key rejection, and post-routing omission
rule on 2026-08-07. On 2026-08-09 the user ratified heterogeneous routed schema families, generic
resource/output-binding schema authority, independently compiled route schemas, and fail-closed
admission of newly observed routes.

## References

- `.10x/research/2026-08-07-routed-target-shared-extraction-readiness.md`
- `.10x/specs/sql-project-authoring.md`
- `.10x/specs/package-keyed-delete-effects.md`
- `.10x/specs/streaming-operator-graph.md`
- `.10x/specs/destination-receipts-guarantees.md`
