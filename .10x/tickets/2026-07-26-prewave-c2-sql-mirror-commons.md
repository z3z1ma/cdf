Status: active
Created: 2026-07-26
Updated: 2026-07-26
Parent: `.10x/tickets/2026-07-26-pre-wave-architecture-hardening-program.md`
Depends-On: `.10x/tickets/done/2026-07-26-prewave-c1-receipt-clock-authority.md`

# Extract typed SQL destination mirror commons

## Scope

Create `cdf-dest-sql` with typed load/state/segment/quarantine mirror mutations, readbacks,
ordering/idempotency rules, and a transactional mirror manager. Migrate DuckDB and Postgres while
keeping physical SQL, parameters, JSON types, transactions, and row decoding in each adapter.
Unify identifier validation through sheet rules and retain dialect-owned quoting.

## Non-goals

- No arbitrary-string `SqlExecutor`, ORM, shared SQL generator, or cross-destination transaction.
- No change to payload bulk paths, type fidelity, destination guarantees, or warehouse support.
- No lowest-common-denominator SQL or JSON conversion.

## Acceptance criteria

- DuckDB and Postgres share one typed mirror lifecycle and common evidence models.
- Native backend wrappers execute all mirror work in the payload transaction required by their
  guarantee.
- Failure injection proves atomic rollback, duplicate idempotency, state monotonicity, quarantine
  uniqueness, and receipt readback.
- Identifier input is validated by destination sheet rules before dialect quoting; unsafe string
  interpolation is absent.
- Mirror and correction conformance plus focused bulk-path performance show no regression.

## References

- `.10x/specs/destination-common-services.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/specs/destination-extension-runtime-contract.md`

## Assumptions

- Source-backed: DuckDB and Postgres duplicate mirror lifecycle but require different physical SQL
  and transaction clients.

## Journal

- 2026-07-26: Scoped the shared API as typed mirror operations, explicitly rejecting a stringly
  executor that would leak SQL semantics upward.
- 2026-07-26: Execution inspection confirmed the two adapters already share four logical
  operations—load receipt, state head/segment state, row-key segment range, and quarantine
  evidence—but encode sequencing in adapter functions and, for Postgres, statement-name lookups.
  The shared leaf will own typed operations, canonical ordering, identity validation,
  duplicate/readback comparison, and sequencing over a borrowed native transaction. DuckDB and
  Postgres retain every SQL string, parameter, JSON encoding, row decoder, and commit/rollback.
- 2026-07-26: Added the `cdf-dest-sql` leaf with typed load, state, segment, and quarantine
  evidence, validated SQL identifiers, lifecycle validation, checkpoint-lineage state
  successorship, streamed quarantine processing, and a backend contract that has no commit
  authority.
- 2026-07-26: Migrated DuckDB and PostgreSQL ordinary and correction paths. Each adapter retains
  its native transaction, SQL, parameters, dialect quoting, JSON representation, and row
  decoding. Successful mutations use typed returned rows; conflict paths read existing evidence.
- 2026-07-26: Initial adversarial reviews rejected key-only duplicate probing, clock-ordered state,
  forgeable framework identifiers, weak segment/quarantine comparison, correction drift,
  per-record quarantine readbacks, and a PostgreSQL idempotency race. The implementation was
  revised after each finding and re-reviewed.
- 2026-07-26: PostgreSQL testing exposed statement-snapshot behavior when an advisory lock and
  duplicate lookup shared one statement. The lock is now acquired in its own statement and the
  receipt is queried afterward, so a waiter observes the winner's committed row.
- 2026-07-26: A later review exposed that replay comparison was still self-referential for
  history-dependent counts and migrations. Both adapters now persist and decode independent typed
  load columns, reconcile receipt JSON against those columns, and only then compare the expected
  logical receipt. Legacy DuckDB rows without the new independent evidence fail closed.
- 2026-07-26: Empty append and replace plans established a canonical zero-count receipt shape.
  DuckDB integration and shared/PostgreSQL unit coverage prove those duplicates remain valid.
- 2026-07-26: `graphify query` and the required post-change `graphify update .` could not run
  because the `graphify` executable is not installed (`zsh: command not found: graphify`).

## Blockers

None.

## Evidence

- Shared lifecycle: `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-dest-sql
  -p cdf-dest-duckdb -p cdf-dest-postgres --locked` passed 13 shared-manager tests, 55 DuckDB
  tests, and 35 PostgreSQL tests; two opt-in release benchmarks were ignored. PostgreSQL tests
  launched local native servers and exercised duplicate replay, advisory-lock serialization,
  checkpoint-lineage CAS, quarantine, rollback, and correction paths.
- Transactional guarantees: DuckDB
  `mirror_failure_rolls_back_payload_load_state_and_row_key_mutations`,
  `correction_failure_after_planning_rolls_back_nullable_migration_and_all_updates`, and the
  PostgreSQL live rollback/correction tests passed. These observations prove payload and mirror
  changes share the native transaction; they do not claim crash durability beyond each backend's
  declared guarantee.
- Idempotency and readback: shared tests rejected conflicting logical duplicates and inexact
  physical insert results. Adapter tests passed replay after reopen, serialized same-package
  PostgreSQL commits, correction replay, independent receipt-column reconciliation, and empty
  append/replace duplicate behavior.
- State, segment, and quarantine laws: shared tests passed lineage successorship, stale-state
  rejection, segment/range drift rejection, streamed quarantine insertion, and conflicting
  quarantine readback rejection. PostgreSQL live state CAS and quarantine tests passed.
- Identifier safety: common identifier tests and adapter identifier tests passed. Dynamic target,
  field, stage, and schema names cross `ValidatedSqlIdentifier` before adapter-owned quoting;
  framework identifiers use the same checked constructor.
- Static quality: `DUCKDB_DOWNLOAD_LIB=1 cargo clippy -p cdf-dest-sql
  -p cdf-dest-duckdb -p cdf-dest-postgres --all-targets --locked -- -D warnings`,
  `cargo fmt --all --check`, and `git diff --check` passed.
- Dependency boundary: `cargo tree -p cdf-dest-sql --depth 1` showed only `cdf-kernel`,
  `cdf-package-contract`, and `serde`; no database client or SQL generator entered the shared
  leaf.
- Performance: `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-dest-postgres --release
  binary_copy::tests::binary_copy_encoder_is_at_least_twice_csv --locked -- --ignored --exact
  --nocapture` passed at 46,621,678 binary rows/s versus 15,389,946 CSV rows/s (3.03x). Mirror
  mutations use `RETURNING`, and quarantine is streamed with an existing-row query only on
  conflict. The benchmark covers the payload encoder, not end-to-end database throughput.
- Environment limit: an initial release invocation without `DUCKDB_DOWNLOAD_LIB=1` failed to link
  `-lduckdb`; the project-prescribed dynamic library environment made the same benchmark pass.
  `graphify update .` remained unavailable as recorded in the journal.

## Review

- Delegated review used `ocr delegate preview --repo .` and `ocr delegate rule --repo .` on every
  changed file before each frozen review. Reviewers did not edit files.
- Early verdicts were `fail` and drove repairs for lifecycle ordering, exact typed evidence,
  PostgreSQL concurrency, identifier provenance, correction inclusion, quarantine streaming, and
  duplicate receipt independence.
- Final Noether review: `pass`, no findings. Residual risk: legacy DuckDB rows without independent
  segment/migration evidence require an explicit operational remediation path; historical
  migration replay retains a bounded dependency on stored destination evidence and later package
  reconciliation.
- Final Aristotle review: `pass`, no findings. Residual risk: the two release-only PostgreSQL
  benchmarks remain opt-in; legacy DuckDB rows deliberately fail closed until the independent
  evidence is restored.

## Retrospective

- What worked: keeping the common API typed and transaction-borrowing preserved adapter power while
  making the lifecycle laws executable once. `RETURNING` reconciled correctness and hot-path
  efficiency better than unconditional read-after-write queries.
- What surprised us: an advisory lock inside the same PostgreSQL statement does not refresh that
  statement's snapshot after a concurrent winner commits. Lock acquisition and duplicate lookup
  must be separate statements.
- Dead end: comparing a stored receipt to an expected receipt reconstructed partly from that same
  stored JSON creates a tautology. Independent relational columns are necessary for fields that
  cannot be regenerated from the current package plan.
- Five whys: duplicate replay could accept tampered history because expected history reused stored
  values; it reused stored values because migrations and counts are partly execution outcomes;
  those outcomes lacked independent mirror columns; they lacked columns because the old mirror
  treated receipt JSON as both record and verifier; that model predated adversarial replay
  validation. The durable correction is receipt JSON plus independently decoded evidence, not
  another equality helper.
- Compatibility consequence: adding independent columns cannot retroactively prove old DuckDB
  rows. Failing closed is safer than manufacturing evidence; remediation belongs to a later
  operational owner if upgrade compatibility is required.
- Compounding: the reusable design and failure modes are captured in
  `.10x/knowledge/transactional-sql-mirror-lifecycle.md`. No new procedural skill was warranted:
  the lesson is design authority, not recurring deterministic toil.
