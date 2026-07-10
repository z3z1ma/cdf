Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-10-p2-rp9d-gc-promotion-availability.md, .10x/specs/schema-promotion-corrections.md, .10x/specs/package-lifecycle-determinism.md

# RP9D GC promotion availability evidence

## What was observed

- `cdf-project` now owns typed local promotion availability and collection assessment. CLI GC no longer scans Arrow columns or counts strings itself; it supplies the existing retention planner's actions to the shared service.
- Canonical residual scanning is shared with the RP5 local package inventory. It verifies package identity, attributes the resource through the state preimage, recognizes only the framework semantic residual field, decodes every non-null value through canonical `residual-json-v1`, and streams exact envelope byte counts.
- Structural receipt authority is also shared with planning. Receipts must match package hash, state/schema, commit target/disposition, package-hash idempotency token, and the exact ordered segment acknowledgements. Duplicate or inconsistent receipt authority is unavailable, never promotable.
- Reports independently expose `contains_local_residual_bytes`, `locally_promotable`, `local_residual_bytes`, `promotable_residual_bytes`, `last_locally_promotable_for_resource`, and `collection_removes_last_local_promotable_copy` plus the existing planned action, typed authority status, receipt targets, artifact location, and remediation.
- `collection_removes_last_local_promotable_copy` is true only for a locally promotable artifact planned as `would_collect` when no locally promotable retained artifact remains for the resource. Retention actions are inputs; RP9D does not alter them.
- Current dry-run retention protects receipted packages, so the real CLI fixture reports `planned_action = retain` and `collection_removes_last_local_promotable_copy = false`. Synthetic shared-service action matrices prove the future `would_collect` consequence without inventing a destructive mode.
- No destination readback is inferred. Human output says so explicitly and provides exact retain/restore remediation.

## Fixture coverage

Shared-service tests cover:

- verified canonical `residual-json-v1` plus exact receipt authority;
- canonical residual bytes without a receipt (`contains = true`, `locally_promotable = false`);
- malformed/ad-hoc JSON in the semantic residual column;
- a `_cdf_variant`-named field without the framework semantic metadata;
- retention tombstones;
- structurally inconsistent receipts;
- corrupt identity data and a missing manifest;
- one and multiple local packages with mixed retain/collect actions, all-collect actions, and last-copy calculation.

CLI coverage uses a real canonical residual fixture with state and destination commit preimages plus an exact receipt. JSON asserts the independent flags, exact bytes, action, authority, and final collection consequence. Human output asserts the availability table, action, remediation, and explicit no-readback statement.

## Procedure and results

```text
cargo test -p cdf-project --lib
```

Passed 162/162, including the shared availability/collection matrix and strengthened RP5 inventory receipt checks.

```text
cargo test -p cdf-cli package_gc -- --nocapture
```

Passed 3/3.

```text
cargo test -p cdf-cli --lib
```

Passed 253/253, including live local Postgres-backed CLI coverage.

```text
cargo clippy -p cdf-project -p cdf-cli --lib --tests -- -D warnings
cargo fmt --check
git diff --check
```

Passed on the affected working tree.

## What this supports

This supports RP9D's claim that GC reports local residual-promotion read availability from verified canonical authority rather than deletion reachability or raw string presence. It also supports deterministic future collection consequence reporting without changing retention, deleting packages, probing destinations, or inferring remote readback.

## Limits

- Actual retention tombstoning removes identity files, including state attribution. Such artifacts remain protected and typed `tombstone_only` in the local inspection result but cannot produce a resource-specific promotion assessment after attribution bytes are gone.
- Current retention rules make receipted promotion packages protected, so a true final-removal flag is exercised through the shared typed action matrix rather than a destructive CLI mode. This is required by RP9D's no-retention-change/no-destructive-GC exclusions.
