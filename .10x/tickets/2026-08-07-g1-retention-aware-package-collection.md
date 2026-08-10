Status: active
Created: 2026-08-07
Updated: 2026-08-07
Parent: `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`

# G1: retention-aware package collection

## Scope

Implement the active retention-aware collection contract end to end: neutral policy resolution and
candidate planning, crash-safe/idempotent tombstone execution, dry-run/`--execute` CLI parity,
post-checkpoint automatic collection, promotion-availability reporting, telemetry, and focused
behavioral tests.

## Non-goals

- deleting package directories, receipts, checkpoints, or minimal settlement proof;
- remote archive/object-store retention;
- ledger/checkpoint history compaction;
- CDC adapter or keyed-effect implementation;
- a second CLI-owned eligibility algorithm.

## Acceptance criteria

- [ ] Trust/default retention resolves exactly to runs, duration, or disabled using committed
      checkpoint order/settlement time rather than filesystem time.
- [ ] Only verified receipt-and-checkpoint-settled packages outside retention become candidates;
      incomplete, leased, recovery-required, corrupt, ambiguous, and missing states fail closed.
- [ ] `cdf package gc` is dry-run and `cdf package gc --execute` revalidates and executes the same
      canonical plan with truthful human/JSON classifications and reclaimed counts.
- [ ] Normal execution invokes the same bounded collector after successful checkpoint settlement;
      collection failure cannot falsify the committed checkpoint and remains retryable/visible.
- [ ] Tombstoning is idempotent and preserves manifest/hash/receipt/checkpoint proof while replay
      availability and schema-promotion consequences are reported truthfully.
- [ ] Focused package/project/CLI behavioral tests and affected-package formatting/check/Clippy pass.

## References

- `.10x/specs/retention-aware-package-collection.md`
- `.10x/research/2026-08-07-cdc-mysql-continuous-readiness.md`
- `.10x/specs/checkpoint-state-commit-gate.md`
- `.10x/specs/schema-promotion-corrections.md`
- `.10x/specs/package-lifecycle-determinism.md`
- `.10x/knowledge/developer-build-duckdb-linkage.md`

## Assumptions

- The policy and lifecycle semantics are user-ratified and active in the governing spec.
- Current pre-production artifacts are updated directly; no compatibility reader or migration is
  permitted.

## Journal

- 2026-08-07: Ticket opened after user ratification and current-source readiness inspection. The
  existing CLI planner permanently protects receipted/checkpointed packages, while the package
  crate already supplies crash-safe tombstoning. Implementation will reuse one neutral planner for
  automatic and manual paths.
- 2026-08-07: Implemented the neutral package collector in `cdf-project`, complete committed
  checkpoint history reads in `cdf-state-sqlite`, dry-run/`--execute` CLI parity, package-local
  receipt proof, fail-closed lifecycle/corruption handling, promotion availability reporting, and
  automatic post-checkpoint collection in normal `cdf run`.
- 2026-08-07: Moved tombstone publication before payload deletion so crashes never leave a live
  manifest claiming deleted canonical bytes. Archived packages with residual identity files are
  retryable candidates, while completed tombstones remain idempotent no-ops.
- 2026-08-09: Corrected automatic collection for genuinely continuous execution. The project
  orchestrator now invokes an invocation-local post-checkpoint hook after every committed drain
  epoch, and the CLI binds that hook to the same canonical collector as `cdf package gc
  --execute`. Collection no longer waits for the overall command to return, so a forever-running
  source cannot retain every settled package buffer indefinitely.
- 2026-08-09: Production Atlas execution exposed that collection still verified every retained
  package and ran the complete schema-promotion inventory after each epoch. In a sandbox with a
  multi-gigabyte retained package this consumed a CPU core and delayed the next CDC epoch by about
  a minute. Retained packages now fail closed from their manifest/checkpoint retention verdict
  without payload rehashing; only deletion candidates receive full package/receipt verification,
  and promotion inventory is limited to every local package for the candidate resources. This
  preserves last-promotable-copy authority while making the steady-state no-candidate path
  metadata-only.

## Blockers

None. The ticket is executable after this record-publication turn.

## Evidence

- Policy behavior: `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-project package_collection --lib
  --locked` passed 3 focused tests covering run-count retention, settlement-time duration
  retention, and disabled retention.
- Manual behavior: `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-cli package_gc --lib --locked` passed
  4 focused tests covering classification, promotion availability, explicit dry-run, execute,
  reclaimed counts, proof preservation, and idempotence.
- Automatic behavior: `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-cli
  run_automatically_collects_settled_packages_outside_retention --lib --locked` passed; two real
  source/destination epochs retained the newest package and tombstoned only the older settled
  package after the second checkpoint committed.
- Continuous automatic behavior: `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-project
  drain_project_settles_each_frontier_before_committing_the_next_epoch --lib --locked` passed and
  observed the post-checkpoint hook once for each of two independently committed drain epochs.
- Affected compilation: `DUCKDB_DOWNLOAD_LIB=1 cargo check -p cdf-project -p cdf-state-sqlite -p
  cdf-package -p cdf-cli-core -p cdf-cli --all-targets --locked` passed.

## Review

Pending the one tranche-level adversarial review requested after the CDC tranche stabilizes.

## Retrospective

- The pre-existing CLI GC classified hash presence rather than proving the full package-local
  receipt plus checkpoint chain; moving eligibility into `cdf-project` removed that split
  authority.
- Publishing an archived lifecycle before deletion is only crash-safe when the planner retries
  archived packages that still contain identity files. Treating every archived package as a
  completed no-op would strand bytes after an interrupted deletion.
- Automatic collection belongs after the receipt-gated checkpoint outcome. A failure can then be
  reported without rolling back or falsifying already committed destination/state authority.
