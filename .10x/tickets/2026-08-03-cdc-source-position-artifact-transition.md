Status: open
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

## Blockers

None.

## Evidence

Pending execution.

## Review

Pending one consolidated independent red-team review after focused validation is green.

## Retrospective

Pending execution.
