Status: done
Created: 2026-07-19
Updated: 2026-07-19
Parent: .10x/tickets/done/2026-07-19-iceberg-glue-source-program.md

# Iceberg F3: typed table-snapshot position

## Scope

Add the source-neutral typed table-snapshot position and its aggregation, package/checkpoint, replay/promotion, state rendering, serialization, tamper, and property conformance.

## Non-goals

No Iceberg catalog/scan code, file identity replacement, migration CLI, runtime worker-protocol edits while WX1 is dirty, or legacy state compatibility.

## Acceptance Criteria

- Position fields bind protocol, catalog/table/ref, snapshot/sequence/parent, and metadata generation with canonical validation.
- Identical partition snapshots aggregate only after complete authority; divergent snapshots fail.
- Batch slicing, package/checkpoint, replay/promotion, state/inspect, canonical JSON/hash, tamper, and property fixtures agree.
- Current state version becomes the only state shape; no migration shim is introduced for nonexistent customers.

## References

- `.10x/decisions/iceberg-glue-source-boundaries.md`
- `.10x/specs/checkpoint-state-commit-gate.md`
- `.10x/specs/iceberg-source.md`

## Assumptions

- User-ratified 2026-07-19: typed snapshot authority replaces semantic workarounds.

## Journal

- 2026-07-19: Activated after WX1 closed at `.10x/tickets/done/2026-07-11-p0-wx1-portable-partition-task-protocol.md`. The concurrent lane has only an unrelated formatting diff in `cdf-runtime/src/stream_policy.rs`; F3 will not touch it.
- 2026-07-19: Position shape is source-neutral and exact: protocol, catalog identity, table identity, typed selector, selected snapshot/sequence/parent, and metadata location/generation. Snapshot IDs remain signed 64-bit because table protocols such as Iceberg encode them as `long`; canonical validation requires positive IDs, nonnegative sequence/timestamp values, exact selector consistency, and control-free identities.
- 2026-07-19: Added exact table-snapshot aggregation, batch-slice invariance, package/replay/checkpoint validation, portable-worker validation, promotion fixtures, state rendering, canonical JSON/hash stability, SQLite semantic-tamper rejection, and property coverage. Checkpoint stores and package preimages now invoke the existing generic source-position validator rather than relying on callers to remember it.
- 2026-07-19: Adversarial layout review found that the initial inline snapshot record inflated every `SourcePosition` to roughly the large-variant threshold and triggered downstream enum-layout warnings. The new variant is boxed; serialized identity is unchanged, Clippy is clean, and a `<= 96`-byte layout regression assertion protects ordinary positions from paying table metadata's footprint.

## Blockers

None. WX1 is done; F3 may extend its exhaustive portable-position validation without changing worker-protocol authority.

## Evidence

- Canonical structure and aggregation: `CARGO_BUILD_JOBS=12 cargo test -p cdf-kernel --locked` passed 67/67 tests, including valid/invalid selector and identity cases, exact two-partition aggregation, divergence/mixed-kind rejection, prior-snapshot identity checks, batch-slice invariance, and the compact enum-layout law.
- Package/replay: `CARGO_BUILD_JOBS=12 cargo test -p cdf-package --locked` passed 61 tests with 4 explicit performance ignores. Table-snapshot canonical JSON is golden-pinned to SHA-256 `0e6d4a51d3cb81ce0ba7ba73b4684c9ac501886fe720ee1ee29087f19175e623`; processed-observation replay preserves the exact position and rejects generation tamper.
- Checkpoint persistence/tamper: `CARGO_BUILD_JOBS=12 cargo test -p cdf-state-sqlite --locked` passed 42/42 tests. Both stores reject invalid constructed snapshot authority, SQLite round-trips the current state shape, and a row whose duplicated position/delta JSON were coherently tampered still fails semantic validation on read.
- Product and portability: targeted CLI human/JSON state rendering, portable-worker local-path rejection, property round-trip, archived/normal promotion inventory, and package replay tests all passed. The table position displays protocol/catalog/table/ref/snapshot/sequence/parent/generation and remains a remote-safe inline worker position.
- Static quality: `cargo clippy -p cdf-kernel --lib --locked -- -D warnings` and `cargo clippy -p cdf-package-contract -p cdf-state-sqlite --lib --locked -- -D warnings` passed after boxing the large variant.
- Limit: a later combined runtime/project/conformance sweep was blocked by the concurrent, uncommitted `cdf-runtime/src/rolling_replay.rs` lane failing its own compile. Before that lane appeared, the F3-inclusive kernel/runtime/engine/project/CLI/conformance check completed successfully; all isolated F3 suites above remain green. No claim is made about the concurrent lane.

## Review

Verdict: pass.

Adversarial findings resolved in this execution:

- Significant: package and checkpoint boundaries could deserialize/store semantically invalid typed positions even though worker and aggregation paths validated them. Fixed by validating complete state-delta/preimage and processed-observation position authority at persistence/replay boundaries, with coherent-tamper tests.
- Significant/performance: the first inline variant bloated unrelated position/frontier values. Fixed with one boxed large variant and a permanent layout test; canonical serialization/hash did not change.
- Minor: old tests used placeholder file/foreign-state hashes that ceased to be valid once boundary validation became mandatory. Replaced with exact current-shape evidence rather than weakening validation.

Residual risk is owned by later children: F4 proves externalized complete task-set authority, and I1/I2 bind actual catalog-selected snapshots to these generic positions. F3 intentionally contains no catalog or scan behavior.

## Retrospective

The new semantic type exposed two general lessons already encoded in the implementation: validators are useful only when every persistence/replay boundary invokes them, and a rarely used metadata-rich enum variant must not tax the hot-path representation of every common variant. The source-neutral position boundary held: no Iceberg/AWS type or source-name branch entered generic orchestration, package, state, or worker code. No additional knowledge record is needed because the governing source-boundary decision already records the reusable rule and the compact-layout assertion prevents recurrence mechanically.
