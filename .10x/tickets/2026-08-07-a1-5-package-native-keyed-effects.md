Status: active
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

- [ ] Packages have one closed `Rows` or `KeyedChanges` content identity; merge/CDC produce keyed
      changes and append/replace reject admitted delete effects.
- [ ] Upserts and mechanically derived key-only deletes have distinct typed segment identities,
      schemas, ordinals, hashes, and state positions without leaking `_cdf_op` into destination rows.
- [ ] One bounded spillable exact-key reducer selects at most one final effect across both families
      with ordinary-merge fail/explicit order and CDC protocol-order last semantics.
- [ ] Source deletion coverage and explicit ignore/hard/Boolean-soft application are independent,
      validated, identity-bearing authorities with no default.
- [ ] Package manifest/state, staged ingress, commit request, receipts, replay, verification,
      archive, inspection, and conformance carry effect kind and fail closed on mismatch.
- [ ] Replaying a keyed package is idempotent and does not contact the source or rerun reduction;
      exact intent counts and truthful destination outcomes remain distinct.
- [ ] Current artifacts/fixtures are replaced coherently with no legacy or compatibility machinery.
- [ ] Focused affected-package behavioral tests, formatting, check, and strict Clippy pass.

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

## Review

Pending the one tranche-level adversarial review requested after the CDC tranche stabilizes.

## Retrospective

Pending implementation.
