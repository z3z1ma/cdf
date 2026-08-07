Status: done
Created: 2026-08-06
Updated: 2026-08-06
Parent: `.10x/tickets/done/2026-08-06-state-backed-schema-authority-program.md`

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
- 2026-08-06: Activated after the user explicitly continued from the ratified S0 handoff. Read the
  complete ticket and governing decision/specification/knowledge chain; implementation is bounded
  to kernel contracts plus the current SQLite store and reusable conformance.
- 2026-08-06: Added `ProjectId`, `EnvironmentName`, schema authority keys/preconditions, immutable
  canonical versions, active/promoting heads, fenced promotion events, and the backend-neutral
  `SchemaAuthorityStore` to `cdf-kernel`.
- 2026-08-06: Added the current-only SQLite component schema, shared persisted lease-domain
  identity, exact single/batch first-use CAS, bounded typed history, immutable version/history
  triggers, and live-lease validation inside fenced promotion transactions.
- 2026-08-06: Shared conformance found one ownership bug in the first concurrent implementation:
  the losing proposal looked up its proposed hash before comparing the established head and
  therefore reported missing private state. Reordering the fail-closed head validation and the
  caller-conflict comparison now returns `Contract` for the healthy race while retaining
  `Internal` for genuinely missing head versions.
- 2026-08-06: The error-ownership audit froze two changed construction files and classified all 55
  sites (30 Contract, 1 Data, 24 Internal). Existing `sqlite_error`/`private_state_decode` helpers
  remain the supporting provenance boundary; the complete SQLite suite includes their nested
  typed-error regression.

## Blockers

None.

## Evidence

1. Key/domain/project/environment/resource validation and isolation: reusable conformance
   `sqlite_schema_authority_passes_shared_conformance` passed, including dev/prod, A/B, foreign-key,
   and foreign-fence cases.
2. Canonical immutable versions and fail-closed head references: three focused `cdf-kernel`
   schema-authority tests passed; SQLite corruption tests passed for malformed version bytes and a
   missing version behind a head.
3. Exact first-use CAS: shared conformance proved absent establishment, identical idempotency, and
   different-proposal conflict; `sqlite_schema_authority_concurrent_first_use_has_one_winner`
   proved exactly one of two concurrent proposals commits and the loser is Contract-owned.
4. Batch atomicity: shared conformance proved a conflict leaves an otherwise absent peer absent;
   `sqlite_schema_authority_failure_rolls_back_complete_batch` injected failure after all version
   inserts and observed zero versions, heads, and events.
5. History/fencing: shared conformance proved Active→Promoting→Active, same-domain live-lease
   enforcement, generation `1→2`, event ordinals `1,2,3`, newest-two bounded ordering, and foreign
   domain refusal. SQLite schema constraints and no-update/no-delete triggers enforce immutable
   versions and append-only history below the Rust API.
6. Current-only schema and error ownership: unsupported and incomplete component schema tests
   passed. The reproducible frozen manifest is
   `.10x/evidence/.storage/2026-08-06-s1-error-ownership-files.txt`; the per-site ledger is
   `.10x/evidence/.storage/2026-08-06-s1-error-ownership-ledger.csv`. A post-write reproduction
   observed exactly 55 construction sites and no missing or stale ledger entries.
7. Commands from repository root:
   - `cargo fmt --all -- --check` and `git diff --check`: passed.
   - `cargo check -p cdf-kernel` and `cargo check -p cdf-state-sqlite`: passed.
   - `cargo test -p cdf-kernel schema_authority`: 3 passed.
   - `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-state-sqlite`: 76 passed plus doc tests.
   - `DUCKDB_DOWNLOAD_LIB=1 cargo clippy -p cdf-kernel -p cdf-state-sqlite -p cdf-conformance
     --all-targets -- -D warnings`: passed.
   - Cognitive-complexity diagnostic reported only pre-existing findings outside changed files;
     none in the new authority modules.
   - `cargo machete --with-metadata`: no unused dependencies.
   - `graphify update .`: unavailable because the `graphify` executable is not installed; no graph
     update claim is made.

## Review

The user explicitly deferred subagent/red-team review to the final S6 integration boundary. Local
review checked the trait/SQLite ownership seam, transaction order, foreign-domain fencing,
fail-closed typed reads, append-only SQL constraints, test assertions, and the complete diff. The
focused error-ownership audit found no provenance flattening: caller-invalid/stale inputs are
Contract, explicit missing read-only state is Data, private state corruption is Internal, and
unchanged SQLite wrappers retain host/transient ownership. Residual risk is limited to integration
with project preparation and promotion settlement, which S2 and S4 explicitly own.

## Retrospective

Reusing the persisted scope-lease authority domain kept fencing tokens and schema generations in
one real SQLite consistency domain without inventing a generic state crate. The most valuable test
was the concurrent different-proposal case: it distinguished a legitimate stale caller from
private corruption and forced validation order to express that distinction. Transactional
failpoint injection after version inserts gave a cheap crash-atomicity proof without durable
high-frequency machinery. The conformance crate's broad dependency graph made compilation much
larger than the runtime test itself, so subsequent store implementations should reuse this suite
without repeatedly rebuilding broad certificates during iteration.
