Status: done
Created: 2026-08-07
Updated: 2026-08-07
Parent: `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`

# A1.5: package-native keyed effects

## Scope

Replace the homogeneous package row content model with the closed rows/keyed-changes authority in
the active specification. Implement exact cross-kind reduction, typed upsert/delete segment
families, identity-bearing source deletion coverage and destination delete policy, state/staging/
commit/receipt/replay/archive/inspection propagation, and current-schema conformance through at
least one synthetic delete-capable source plus existing merge paths.

## Non-goals

- a first-party CDC wire adapter;
- PostgreSQL/DuckDB `cdc_apply` physical application;
- routed target families or shared-upstream fan-out;
- patch/sparse updates, predicate/range deletes, event-log preservation, or deletion timestamps;
- compatibility fields, readers, artifact migrations, or legacy rejection tests.

## Acceptance criteria

- [x] Packages have one closed `Rows` or `KeyedChanges` content identity; merge/CDC produce keyed
      changes and append/replace reject admitted delete effects.
- [x] Upserts and mechanically derived key-only deletes have distinct typed segment identities,
      schemas, ordinals, hashes, and state positions without leaking `_cdf_op` into destination rows.
- [x] One bounded spillable exact-key reducer selects at most one final effect across both families
      with ordinary-merge fail/explicit order and CDC protocol-order last semantics.
- [x] Source deletion coverage and explicit ignore/hard/Boolean-soft application are independent,
      validated, identity-bearing authorities with no default.
- [x] Package manifest/state, staged ingress, commit request, receipts, replay, verification,
      archive, inspection, and conformance carry effect kind and fail closed on mismatch.
- [x] Replaying a keyed package is idempotent and does not contact the source or rerun reduction;
      exact intent counts and truthful destination outcomes remain distinct.
- [x] Current artifacts/fixtures are replaced coherently with no legacy or compatibility machinery.
- [x] Focused affected-package behavioral tests, formatting, check, and strict Clippy pass.

## References

- `.10x/decisions/package-native-keyed-delete-effects.md`
- `.10x/specs/package-keyed-delete-effects.md`
- `.10x/specs/spillable-package-dedup.md`
- `.10x/specs/canonical-package-row-ordinal.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/specs/streaming-destination-ingress.md`
- `.10x/knowledge/developer-build-duckdb-linkage.md`

## Assumptions

- All product semantics are active, user-ratified record authority.
- The artifact graph must change coherently in one current-format implementation; intermediate
  commits may be compile-broken only locally and are not pushed.

## Journal

- 2026-08-07: Executable owner opened after the CDC readiness audit confirmed this is the first
  blocking implementation for every CDC adapter and destination. The existing spillable typed-key
  mechanism is the required substrate; the ticket must extend rather than duplicate its equality,
  collision, memory, cancellation, and provenance laws.
- 2026-08-07: Replaced the package artifact spine with manifest version 4 and a closed
  `Rows`/`KeyedChanges` content authority. Segment kind now survives manifest, state delta,
  staged ingress, commit preimage/request, receipt acknowledgement, replay, archive, Parquet
  metadata, inspection, and destination load-plan propagation.
- 2026-08-07: Ordinary merge execution now requires a non-null compiled effect key, uses the
  existing bounded exact-key spill reducer, emits typed upsert segments, and binds exact reduction
  counts into package identity. Append/replace retain ordinary rows; explicit exact-row dedup
  remains independent of contract-only row rules.
- 2026-08-07: Manifest/state/receipt validation was tightened from segment-family presence to
  exact typed effect row counts. Remaining work is transient delete-bearing CDC input, cross-kind
  reduction/canonical ordering, explicit delete application, keyed outcome receipts, and the
  synthetic source/replay certificate.
- 2026-08-07: Added typed homogeneous CDC batch metadata carrying insert/update/delete operation
  and an exact PostgreSQL/MySQL/Mongo source position. Execution rejects CDC metadata under
  append/replace, rejects `cdc_apply` batches without it, and binds one protocol/scope order into
  keyed reduction identity.
- 2026-08-07: Complete insert/update rows now lower to upserts while delete batches take a
  key-only path that rejects null or non-key payloads. The shared exact-key reducer selects one
  winner across both families, and the retained effects pass through a spill-budgeted,
  memory-reserved external sorter before canonical upsert-then-delete segment publication.
- 2026-08-07: Replaced flattened receipt counters with a closed tagged row/keyed count contract.
  Keyed receipts bind exact surviving upsert/delete intent separately from insert/update,
  hard/soft/missing/ignored delete outcomes. DuckDB, SQLite, PostgreSQL, ClickHouse, project
  ledger, CLI reports/JSON, and system SQL now preserve that distinction.
- 2026-08-07: Replay now compares the verified commit-plan content authority to the manifest
  before reconstructing inputs, and keyed execution without bounded services fails rather than
  emitting noncanonical effects. Destination fixtures were converted from row-shaped merge
  packages to typed upsert packages; destination-side silent deduplication is no longer accepted.
- 2026-08-07: The focused CLI artifact test exposed that CDC checkpoint version 3 was still being
  inserted into a new SQLite checkpoint table constrained to version 2. The current-only store
  schema and component version now derive from checkpoint version 3; superseded pre-production
  stores are rejected for recreation rather than migrated.

## Blockers

None. The ticket is executable after this record-publication turn.

## Evidence

- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-kernel -p cdf-package-contract -p cdf-package
  --locked` passed the artifact-contract suites (90 kernel and 94 package tests; the contract crate
  compiled in the same command and package performance probes remained intentionally ignored).
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-engine package_evidence:: --locked` passed 26 focused
  package-execution tests, including typed merge authority and exact-row/contract dedup separation.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-engine
  package_identity_is_invariant_to_source_batch_rechunking --locked` passed the current-format
  package identity snapshot and source-rechunking invariance check.
- `DUCKDB_DOWNLOAD_LIB=1 cargo check --workspace --all-targets --locked` passed after the coherent
  artifact and fixture transition. This proves compilation only, not the remaining delete-bearing
  CDC behavior named above.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-package --lib --locked` passed 94 tests with four
  explicitly ignored performance probes. This includes exact manifest identity, replay preimage,
  tamper, segment-kind, receipt, and archive behavior.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-engine package_evidence:: --locked` passed all 27
  package-execution tests. The dedicated CDC certificate reduces interleaved complete upserts and
  key-only deletes to two upserts and one delete with exact input/surviving counts.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-engine
  effect_sort_spill_orders_each_typed_family_across_merge_levels --locked` passed a forced
  multi-level spill/merge ordering certificate; the CDC certificate passed again after
  batch-boundary memory retention was removed.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-package-contract
  keyed_receipt_binds_exact_intent_and_delete_policy_outcomes --locked` passed the closed receipt
  kind, exact intent, and explicit delete-policy reconciliation certificate.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-dest-duckdb --lib --locked` passed all 59 tests after
  merge fixtures adopted typed upsert packages. `DUCKDB_DOWNLOAD_LIB=1 cargo test -p
  cdf-dest-sqlite --lib --locked` passed all 22 tests. The four initially exposed PostgreSQL
  row/keyed fixture and empty-count failures each passed on focused rerun, including two live local
  PostgreSQL merge/exact-value certificates.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-cli
  sql_mounts_checkpoint_package_and_receipt_tables_as_json_rows --locked` passed the current
  tagged receipt/system-SQL artifact contract. The SQLite checkpoint-store conformance and CDC
  lineage/token tests also passed under state version 3.
- Strict affected-package Clippy passed with `-D warnings` across kernel, package contract,
  package, engine, runtime, SQL destinations, project, CLI, conformance, state SQLite, benchmarks,
  and Iceberg. `DUCKDB_DOWNLOAD_LIB=1 cargo check --workspace --all-targets --locked`, `cargo fmt
  --all`, and `git diff --check` passed.

## Review

Verdict: pass for this ticket's bounded scope. Executor fresh-eye review found and resolved three
significant integration defects: row-shaped merge fixtures could contradict keyed receipts, the
SQLite checkpoint DDL lagged the current CDC state version, and merge output slices could retain
more Arrow input batches than the memory reservation claimed. No critical or significant finding
remains. The user-requested independent review remains deferred to the single CDC tranche closure
barrier; first-party wire adapters and physical `cdc_apply` delete execution remain explicitly
owned by subsequent tickets.

## Retrospective

The artifact contract was easier to make correct than the test ecosystem: many destination tests
bypassed engine publication and manufactured merge requests as ordinary rows. A closed receipt
enum immediately exposed those false fixtures and prevented destinations from laundering a
package mismatch through aggregate row counts. The reusable technique is to make test packages
carry the same typed content authority and segment family as production, including zero-effect
initial authority followed by exact final authority. The other recurring lesson is that bounded
external algorithms must account for retained Arrow buffers, not only logical row slices; flushing
before advancing an exhausted input batch made the merge reservation truthful without adding an
unbounded queue or durable telemetry.
