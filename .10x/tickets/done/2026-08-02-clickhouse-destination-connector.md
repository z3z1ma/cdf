Status: done
Created: 2026-08-02
Updated: 2026-08-03
Parent: .10x/tickets/2026-08-02-sqlite-clickhouse-mongodb-connector-program.md
Depends-On: .10x/tickets/2026-08-02-clickhouse-source-connector.md

# ClickHouse destination connector

## Scope

Implement and ship `cdf-dest-clickhouse` with verified append, capability-proven atomic replace,
default native ReplacingMergeTree merge, and opt-in atomic copy-on-write merge through the official
ArrowStream client path. Add deterministic insert-token settlement, recovery, type/engine/key
inspection, merge-mode policy, live crash coverage, built-in enrollment, documentation, and a
direct official-client destination roofline cell.

## Non-goals

Versioned ReplacingMergeTree guessing, silent engine replacement, cross-partition replacement
without proof, eventual compaction as a receipt, unacknowledged async inserts, generic/multi-table
transaction claims, mutations, or private wire protocols.

## Acceptance Criteria

- Sheet, mapping, bulk preparation, append tokens, synchronous acknowledgement, recoverable mirror
  settlement, replace capability proof, atomic exchange, zero-row marker, and receipt verification
  implement `.10x/specs/clickhouse-destination.md`.
- Merge defaults to direct ReplacingMergeTree ArrowStream insertion with exact sorting/partition
  proof, logical `FINAL` verification, deterministic recovery, and explicit eventual physical
  uniqueness; environment policy selects atomic copy-on-write when immediate uniqueness is needed.
- Live tests cover supported engine/topology matrices, materialized-view dedup capability, duplicate
  segments/packages, crashes between target and mirror settlement, complete atomic replace, and
  native/atomic merge, merge capability rejection, and unsupported targets.
- ArrowStream batches, compression, client reuse, writers, and in-flight bytes are injected,
  bounded, observable, and fully joined before settlement.
- Built-in catalog integrity and focused destination enrollment laws pass; broad generic
  product/chaos/jobs and core certification gates remain parent-owned final integration.
- The append and default native-merge macro cells reach the 0.90 direct ArrowStream roofline with
  identical acknowledgement/deduplication settings.
- One independent red-team review is completed; concrete correctness and throughput findings are
  repaired and judged once without a recursive review cycle.

## References

- `.10x/specs/clickhouse-destination.md`
- `.10x/specs/database-connector-roofline.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/specs/spillable-package-dedup.md`
- `.10x/decisions/clickhouse-merge-modes.md`
- `.10x/knowledge/source-destination-extension-invariant.md`
- `docs/connector-authoring.md`

## Assumptions

- Append, proven atomic replace, default ReplacingMergeTree merge, opt-in atomic copy-on-write
  merge, and the 90% roofline are user-ratified.
- The default merge mode explicitly guarantees logical `FINAL` uniqueness rather than immediate
  physical uniqueness; the atomic mode guarantees immediate publication of a unique target.
- A target that cannot prove deterministic insert deduplication or atomic replace fails the
  applicable prepared path rather than weakening its guarantee.

## Journal

- 2026-08-02: Ticket opened; execution waits for ClickHouse source closure.
- 2026-08-02: Execution started from reviewed and pushed ClickHouse source commit `afe7bab4`. The
  source child retains only parent-owned final integration gates; its official client, bounded
  ArrowStream, transport lease, type parser, identifier, error, and pinned live-server boundaries
  are stable enough for the declared destination dependency. Read the complete destination ticket
  and every direct spec, invariant, research, and authoring authority before implementation.
- 2026-08-02: User expanded the destination to support merge, superseding the active ClickHouse
  spec's explicit merge exclusion. Source authority establishes keyed, deterministic, effectively
  once merge semantics; ClickHouse leaves one consequential choice between immediate atomic
  copy-on-write publication and an eventually deduplicated ReplacingMergeTree representation.
  Paused implementation before encoding either semantic model.
- 2026-08-03: User ratified native ReplacingMergeTree as the throughput-first default and atomic
  copy-on-write as the opt-in immediate-uniqueness mode. Superseded the former ClickHouse spec,
  recorded `.10x/decisions/clickhouse-merge-modes.md`, and reshaped this ticket without resuming
  implementation in the same shaping turn.
- 2026-08-03: Execution resumed after explicit user authorization. Root owns implementation,
  focused validation, closure repairs, commits, and pushes; delegation is reserved for the final
  read-only red-team review.
- 2026-08-03: Implemented the destination leaf, built-in enrollment, project policy, conformance
  fixture, documentation, and direct-library roofline harness. The prepared path uses one reused
  memory-authorized official HTTP client, bounded ArrowStream batches, deterministic target and
  mirror tokens, synchronous acknowledgement, target/mirror capability proof, receipt-last
  settlement, and fresh trait-level verification.
- 2026-08-03: The live lifecycle law successively falsified and repaired target dependency
  inspection, settlement recovery, completed-stage publication recovery, and publication-marker
  metadata ownership. It now covers native duplicate/update, atomic duplicate/update, nonempty and
  zero-row replace, crash after target write, crash after a complete unpublished atomic stage,
  unsupported/versioned/dependent targets, final logical rows, preserved operator table comments,
  and complete stage cleanup.
- 2026-08-03: Host-driven adversarial review removed an unnecessary credential-serialization
  capability, made protocol-level dry planning honor the resolved merge mode, and removed implicit
  full-table counts from append/native-merge and metadata-only inspections. Replace alone pays its
  required exact prior-row count; atomic merge retains its explicit server-side count proofs.
- 2026-08-03: Focused validation passed: 11 destination unit tests with the one live law explicitly
  ignored, the pinned live lifecycle law itself, strict all-target/all-feature destination Clippy,
  built-in catalog integrity, and the two destination-catalog/runtime-chaos enrollment laws.
  Broad product/chaos/jobs and connector-certification gates remain parent-owned by explicit user
  direction to avoid repeatedly running the entire suite during child implementation.
- 2026-08-03: The final independent red-team challenged package-policy identity, package-wide merge
  uniqueness, canonical provenance verification, segment/state bounds, stage publication shape,
  historical atomic receipt verification, physical-batch streaming, mirror capabilities, stored
  state authority, and transforming MergeTree engines. These were concrete correctness or
  throughput findings, so the closure repair addressed all ten rather than starting another review
  round for style or hypothetical corruption cases.
- 2026-08-03: Closure repair moved merge mode into the destination commit-plan identity, made every
  merge package fail closed on duplicate keys unless an explicit deterministic dedup rule exists,
  verifies the typed package dedup summary, proves the exact dense canonical ordinal set, streams
  physical Arrow batches lazily, enforces 10,000-segment and 2 MiB state bounds, fingerprints atomic
  clones structurally, verifies superseded atomic receipts through the current CDF publication,
  authenticates stored state bytes, and rejects transforming engines and malformed mirrors.
- 2026-08-03: Post-repair focused validation passed: 17 `cdf-package-contract` unit tests; the
  engine's synthesized merge-dedup planning test; `cdf-dest-clickhouse` unit tests and its pinned
  ignored live lifecycle law; `cdf-project` library type-check; benchmark binary type-check; and
  strict all-target/all-feature destination Clippy. A focused `cdf-project` unit-test link attempt
  reached the linker but lacked the local DuckDB native library; the corresponding library check
  passed, and release linking succeeded with the repository's required `DUCKDB_DOWNLOAD_LIB=1`.
- 2026-08-03: The first final roofline attempt correctly rejected a benchmark fixture that used
  `ReplacingMergeTree` for append. The fixture now creates row-preserving `MergeTree` for append and
  unversioned `ReplacingMergeTree` for native merge. The direct cell was also brought to semantic
  parity with CDF's exact canonical ordinal proof instead of its former weaker row count.
- 2026-08-03: The final release cell used five samples and 750,000 rows against ClickHouse
  25.8.28.1. Append reached 0.902371 and native merge reached 1.011018 of the direct official-client
  roofline. Both passed the ratified 0.90 floor. System log merges were restored afterward and the
  pinned closure container was left running.

## Blockers

None. The merge-mode visibility tradeoff and configuration boundary are user-ratified and governed
by the active spec and decision record.

## Evidence

- Sheet, mapping, publication, recovery, native/atomic merge, capability rejection, and receipt
  behavior: the exact pinned live law
  `tests::live_native_and_atomic_merge_contract` passed against ClickHouse 25.8.28.1 after closure
  repair. It covers duplicate/update packages, nonempty and zero-row replace, interrupted target and
  stage settlement, unsupported/versioned/dependent targets, logical `FINAL` rows, comments,
  historical atomic receipts, and stage cleanup.
- Bounded official-client execution: destination unit tests, strict Clippy, and the generated closed
  memory-owner matrix support one reused 64 KiB HTTP lease, one 64 MiB writer lease, lazy one-batch
  physical streaming, a 32 MiB physical-batch pre-allocation ceiling, explicit response limits,
  10,000 package segments, and 2 MiB state evidence.
- Package identity and merge uniqueness: 17 package-contract tests and
  `merge_planning_synthesizes_fail_closed_package_key_authority` passed. The destination additionally
  checks the recorded merge-mode policy and typed dedup summary before any mutation.
- Enrollment: built-in catalog integrity and the focused destination catalog/runtime-chaos shard
  registration laws passed. Broad matrix/certification execution remains owned by the parent ticket.
- Error provenance and redaction: `.10x/evidence/2026-08-03-clickhouse-destination-error-ownership.md`
  freezes all 242 constructor/direct-kind sites and their ownership classifications.
- Throughput: `.10x/evidence/.storage/2026-08-03-clickhouse-destination-roofline.json` is a passing
  release report with append at 0.902371 and native merge at 1.011018 of the semantically equivalent
  direct ArrowStream cells. The report binds the executable and workspace-content hashes.
- Static closure: `cargo fmt --all -- --check`, generated-memory-ledger closure, and
  `git diff --check` passed on the final tree.

## Review

Verdict: pass after closure repair. The independent red-team's concrete findings are enumerated in
the journal and all were repaired. Per the user's explicit last-cycle boundary, closure judgment did
not commission a second red team or convert module length, benchmark ordering, full target-value
readback, or hypothetical server corruption into new scope.

The remaining verification boundary is deliberate: exact mapping and structural capability proof,
synchronous server acknowledgement, package-hash filtering, and the exact dense canonical ordinal
set prove the committed package without doubling network traffic through a full value readback.
Broad cross-product certification remains visibly parent-owned rather than falsely attributed to
this focused child.

## Retrospective

The most valuable closure pressure came from treating package policy and dedup authority as immutable
package inputs instead of adapter-local configuration. That made recovery semantics explicit and
prevented native merge from trusting a convention the package could not prove. The other durable
lesson is that throughput baselines must perform the same correctness work: the original direct cell
used a weaker count than CDF's exact ordinal proof and therefore measured an artificial advantage.

Focused laws found real faults faster than broad reruns: the single live lifecycle law exposed every
publication/recovery defect, strict crate Clippy closed code quality, and the direct roofline caught
the append-engine fixture mismatch. Recursive adversarial review would have added latency without a
new authority boundary, so this ticket closes after one independent review, one bounded repair, and
one final evidence pass.
