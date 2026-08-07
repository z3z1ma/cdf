Status: open
Created: 2026-08-06
Updated: 2026-08-06
Parent: `.10x/tickets/2026-08-06-state-backed-schema-authority-program.md`

# S1 state schema-authority foundation

## Scope

Introduce the backend-neutral logical schema authority and one current-only SQLite implementation:

- typed project id, authority key, immutable version, head/status, precondition, event, and store
  authority-domain identity;
- exact head/version lookup and ordered history;
- single and batch `establish_if_absent` with exact idempotency and all-or-none CAS;
- active/promoting head representation and fenced transition primitives needed by S4;
- corrupt/missing-version fail-closed validation;
- reusable store conformance exercised against SQLite and any existing in-memory test store only
  when it is a real current implementation;
- current SQLite schema version/tables/indexes/constraints/transactions with no migration reader.

Keep crate ownership literal: neutral types/traits in `cdf-kernel`, SQLite SQL/transactions in
`cdf-state-sqlite`, and no project/CLI/destination dependency in either authority contract.

## Non-goals

- project configuration/scaffold changes or CLI preparation wiring;
- portable-plan changes;
- drift policy/compiler/runtime changes;
- settlement permits and historical correction execution beyond the minimal fenced head seam;
- Postgres, a generic state implementation crate, schema export/import, or compatibility state.

## Acceptance criteria

1. Authority key validates stable project id, environment, resource id, and store domain without
   secret or filesystem identity.
2. Versions are canonical/content-identified/immutable; a head cannot reference missing or
   mismatched version bytes.
3. Absent head establishes exactly once; identical repeat is idempotent; a different concurrent
   proposal loses before any write.
4. Batch establishment creates every requested absent/matching head or none.
5. Dev cannot affect prod; A cannot affect B; store-domain tokens/preconditions are rejected by a
   different store.
6. Head generations and history are monotonic, append-only, canonically ordered, and bounded where
   public APIs require bounds.
7. SQLite transaction/failpoint tests prove crash atomicity; malformed/corrupt rows fail with narrow
   typed ownership rather than `Internal` flattening.
8. Focused fmt/check/tests/strict Clippy pass with required DuckDB developer linkage only where the
   actual dependency graph needs it.

## References

- `.10x/decisions/state-backed-schema-authority.md`
- `.10x/specs/resource-preparation-command-experience.md`
- `.10x/specs/schema-promotion-corrections.md`
- `.10x/knowledge/fenced-lease-lock-publication.md`
- `.10x/knowledge/error-ownership-taxonomy.md`

## Assumptions

- User-ratified: `[project].id` is stable project identity; state domain + project id + environment
  + resource is the authority key.
- User-ratified: SQLite only now; future Postgres consumes the trait but does not shape a second
  implementation abstraction.
- Record-backed: store owns clock/domain and fenced mutations validate inside one transaction.
- Record-backed: current-only product permits one SQLite schema without predecessor migration.

## Journal

- 2026-08-06: Opened executable but intentionally not started in the governing-record turn.

## Blockers

None.

## Evidence

Pending execution.

## Review

Pending handback review.

## Retrospective

Pending execution.
