Status: done
Created: 2026-08-03
Updated: 2026-08-03
Parent: `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`

# CDC source-position artifact transition

## Scope

Implement A1 as one coherent current-schema replacement governed by
`.10x/specs/cdc-source-position-artifacts.md`: typed PostgreSQL/MySQL committed positions, typed
MongoDB opaque resume tokens, centralized position algebra, and every embedding artifact/version/
renderer/fixture updated atomically with no backward compatibility.

## Non-goals

- connecting to or decoding a live CDC source;
- implementing `_cdf_op`, row materialization, transaction spooling, Mongo event segmentation, or
  a destination `cdc_apply` path;
- deciding the PostgreSQL/MySQL maximum single-transaction byte policy;
- MySQL cross-primary failover;
- preserving any version-1 artifact or declaration;
- unrelated source/destination/configuration/compiler work.

## Acceptance Criteria

- `SOURCE_POSITION_VERSION` is 2; old generic `LogPosition { log, offset, sequence }` is removed and
  cannot deserialize through any current path.
- PostgreSQL/MySQL committed positions and MongoDB resume-token positions contain and validate every
  typed field required by the governing spec without signed truncation or stringly protocol bags.
- one kernel position algebra owns validation, scope, equivalence, reachability, join, and slice
  invariance; checkpoint aggregation and drain frontier comparison delegate to it.
- PostgreSQL/MySQL monotone joins advance correctly and reject regression/mixed scope; Mongo tokens
  round-trip exact BSON bytes, detect tampering, distinguish resume mode, and reject invented order.
- checkpoint state/store, package, portable worker, declarative/generated schema, system-SQL,
  inspect/status, examples/goldens, and conformance embeddings are replaced coherently and agree on
  canonical JSON/version identities.
- old artifacts fail closed with direct regenerate/recreate remediation. Production source contains
  no legacy reader, migration, compatibility shim, transitional field, or dead version branch.
- validation is economical: format once, run focused changed-crate tests in batched commands, run
  no workspace-wide suite unless a concrete cross-boundary failure demands it, and commission one
  consolidated adversarial review after the focused evidence is green.

## References

- `.10x/specs/cdc-source-position-artifacts.md`
- `.10x/research/2026-08-03-cdc-protocol-position-contract.md`
- `.10x/specs/checkpoint-state-commit-gate.md`
- `.10x/specs/stream-epochs-watermarks.md`
- `.10x/decisions/kernel-owned-stream-epoch-policy.md`
- `.10x/knowledge/net-new-no-compatibility-policy.md`
- `crates/cdf-kernel/src/position.rs`
- `crates/cdf-kernel/src/position_aggregation.rs`
- `crates/cdf-kernel/src/checkpoint.rs`
- `crates/cdf-state-sqlite/src/sqlite.rs`
- `crates/cdf-package-contract/src/artifacts.rs`
- `crates/cdf-runtime/src/worker_protocol.rs`
- `crates/cdf-declarative/src/declarations.rs`
- `crates/cdf-cli/src/system_sql.rs`

## Assumptions

- User-ratified on 2026-08-03: the typed position model and algebra recommended by A0.
- User-ratified on 2026-08-03: CDF is net-new/customer zero and carries no backward compatibility
  or transitional technical debt.
- User-ratified on 2026-08-03: MongoDB positions are opaque event-prefix resume authority advanced
  after destination receipt; Mongo transaction grouping is not part of this model.
- Record-backed: all current position/checkpoint artifacts are pre-production/current-schema-only.
- Record-backed: the current generic log position and equality-only fallback are insufficient for
  advancing CDC positions.

## Journal

- 2026-08-03: A0 official protocol research completed and the user ratified recommendations 1 and
  2. The user then clarified recommendation 3: no compatibility is required, and Mongo change
  streams accumulate into segments/packages whose terminal token advances after receipt without
  transaction grouping. The active focused spec and this A1 ticket were opened. Per 10x separation,
  implementation does not begin in the same turn that establishes this first executable owner.
- 2026-08-03: Activated after C1 and D1 closed on `main`. Reconfirmed the current-only replacement
  policy: no version-1 reader, migration, shim, transitional field, or compatibility fixture will
  remain. `graphify` is unavailable in this environment, so the A1 embedding inventory uses direct
  source/record search as the documented fallback.
- 2026-08-03: Replaced the generic log bag with protocol-specific PostgreSQL and MySQL committed
  positions plus a distinct MongoDB change-stream resume-token position. The kernel now owns
  validation, scope, equivalence, reachability, ordered join, and slice-invariance. Runtime drain,
  engine segmentation, and checkpoint aggregation delegate to that authority.
- 2026-08-03: Replaced every current embedding identity together: source position 2, checkpoint
  state/store 2, position algebra 2, declarative v5, worker protocol 2/package-v3, package manifest
  3, destination commit plan 3, and the position-bearing evidence identities. Removed the v1 worker
  fixtures instead of retaining compatibility readers; regenerated all four package goldens.
- 2026-08-03: Strict affected-crate clippy exposed inline enum-size amplification from the richer
  position payload. Boxed the declarative drain termination and schema-quarantine payloads at their
  ownership boundaries; serde remains unchanged and small variants no longer pay the CDC payload
  size.
- 2026-08-03: Applied the error-ownership audit to the changed kernel position boundary. The frozen
  two-file inventory and per-site ledger are in
  `.10x/evidence/.storage/2026-08-03-a1-error-ownership-files.txt` and
  `.10x/evidence/.storage/2026-08-03-a1-error-ownership-ledger.tsv`: two `Internal` constructions
  both name impossible immutable-map invariant failures; BSON/base64/GTID authored-shape failures
  remain `Contract`, while conflicting or regressing observations are `Data`. No foreign wrapper
  contains a typed `CdfError`, so no kind or retry metadata is flattened.
- 2026-08-03: The single consolidated red-team review found one critical checkpoint-lineage gap,
  three significant protocol/scope gaps, and two stale-reference minors. The authorized closure
  batch now binds every proposed/committed checkpoint to the exact live head and prior output,
  advances only one source-attested Mongo terminal prefix, preserves full typed CDC scope and
  Mongo resume mode through source capabilities/replay, and implements the complete MySQL 8.4
  tagged-GTID grammar used by this model. The current worker/package and VISION references were
  corrected in the same batch; no second review cycle was commissioned.
- 2026-08-03: Final targeted validation passed after correcting two test expectations: the
  declarative mismatch assertion now uses the actual `source_frontier` contract spelling, and
  branch-history conformance includes the explicit rewind marker introduced to construct a valid
  second branch under the stricter live-head gate. These were expectation drift, not weakened
  assertions or changes to product semantics.

## Blockers

None.

## Evidence

- AC1/AC2: `cargo nextest run -p cdf-kernel -E 'test(position::cdc::tests)'` passed 4/4,
  covering full-width PostgreSQL LSNs, MySQL canonical GTID sets and monotone/conflict laws, exact
  Mongo BSON token round-trip/tamper/mode/source behavior, and rejection of the old generic shape.
- AC3/AC4: the kernel CDC/aggregation batch passed 5/5, including full-width PostgreSQL LSNs,
  complete MySQL tagged GTID parsing/order/membership, exact Mongo BSON authority, and the
  one-attested-terminal Mongo prefix rule. The engine position-join regression also passed.
- AC3/AC4: targeted in-memory and SQLite checkpoint tests prove exact parent/input binding,
  PostgreSQL regression rejection, Mongo token advancement after commit, committed-watermark
  monotonicity, stale concurrent proposal rejection, and conformance for both stores.
- AC5: an 11-test batch across state, declarative, project replay, engine, and worker protocol
  passed across the initial run plus the three corrected expectation reruns. It covered exact
  source capability scope, full restart-scope replay binding, portable fixture round-trip,
  generated schema, typed task compilation, checkpoint conformance, and transactional head
  uniqueness. The package goldens were regenerated from actual outputs, not edited by guesswork.
- AC6: exact current-source searches returned no match for `LogPosition`, `declarative-v4`,
  `package-v1`, `package-v2`, the three v1 worker fixture names, `tx_boundary`, or a hard-coded
  source-position version 1. There is no current legacy reader, migration, shim, or dual-schema
  branch.
- AC7: `cargo fmt --all -- --check`, `git diff --check`, and strict `cargo clippy` over the 16
  affected crates with `--all-targets --locked -- -D warnings` passed. No workspace-wide test
  suite was run.
- Error-ownership inventory reproduction:
  `xargs rg -n -- 'CdfError::internal|\.internal\(|ErrorKind::Internal|\bInternal\b' < .10x/evidence/.storage/2026-08-03-a1-error-ownership-files.txt`.
- Limit: `graphify update .` could not run because the executable is not installed. The existing
  DuckDB link-input helper supplied the local native prerequisite for the final focused state
  tests; no linker behavior or unrelated build machinery was changed.

## Review

The consolidated independent red-team review initially returned `fail` with:

- critical: checkpoint publication did not bind parent/input authority to the live head or reject
  committed-log regression;
- significant: Mongo token aggregation included the prior input in an unordered equality join;
- significant: source-frontier capabilities and replay reduced typed CDC restart scope;
- significant: MySQL tagged-GTID parsing did not implement repeated tag groups for one UUID or the
  32-byte tag limit;
- minor: one current worker test still named `package-v1`, and VISION still described the retired
  generic log position.

Every finding was repaired in one authorized batch and mapped to focused passing evidence above.
Closure adjudication: `pass`. Residual risk is limited to live protocol integration behavior, which
is explicitly outside A1 and must be proven by the dedicated CDC source tickets; no known artifact,
algebra, checkpoint, capability, or replay correctness finding remains.

## Retrospective

- Typed positions made the cost of embedding a large restart authority visible in unrelated enum
  layouts. Strict clippy was valuable here: payload-bearing variants should own indirection at the
  declaration/admission boundary, not inflate every bounded or admitted value.
- Centralizing the algebra removed materially more code than the typed protocols added in runtime
  and segmentation, which confirms that position semantics belong in the kernel rather than in
  each consumer.
- Current-only artifact replacement was cheaper and clearer than migration machinery: a single
  coordinated version transition plus regenerated fixtures leaves no dual-schema behavior for CDC
  sources to inherit.
- A checkpoint store cannot treat a structurally valid delta as publishable authority. Parent ID,
  input position, and monotone protocol advancement must be one transition invariant evaluated
  against the live head at both proposal and commit.
- Opaque resume tokens need no invented total order, but the source can still attest one terminal
  prefix successor per closed aggregation. Keeping that operation distinct from equality-only join
  preserves Mongo correctness without transaction grouping.
