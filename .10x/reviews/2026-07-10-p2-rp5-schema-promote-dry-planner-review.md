Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: .10x/tickets/done/2026-07-10-p2-rp5-schema-promote-dry-planner-cli.md
Verdict: fail

# P2 RP5 schema-promotion dry-planner adversarial review

## Target

The RP5 typed planner, local verified-package inventory adapter, proposed snapshot identity, destination mapping/strategy selection, and `cdf schema promote RESOURCE [--type JSON_POINTER=ARROW_TYPE ...]` dry CLI.

The review inspected the RP5 ticket and its governing promotion/residual specifications, decisions, knowledge, research, evidence, dependency tickets, the complete `crates/cdf-project/src/promotion.rs`, the schema-snapshot artifact contract, the shared destination-mapping resolver, the CLI boundary/rendering, and focused tests. No implementation files were modified by this review.

## Findings

### Significant — Target planning loses package-to-receipt evidence authority

`plan_schema_promotion` flattens every recorded receipt into a set of destination/target keys, then passes the union of all promotion paths plus one global `!residual_paths.is_empty()` retained-values flag to every target (`crates/cdf-project/src/promotion.rs:337`, `crates/cdf-project/src/promotion.rs:410`, `crates/cdf-project/src/promotion.rs:1156`). `plan_targets` consequently cross-products every selected path into every target migration and may select a correction strategy for a target whose own receipt-bearing packages are tombstone-only or do not contain that path (`crates/cdf-project/src/promotion.rs:1185`, `crates/cdf-project/src/promotion.rs:1263`).

This violates the specification's per-target correction-strategy and retained-evidence contract. In a mixed inventory, the overall plan is currently blocked by tombstone detail, which prevents an immediate write, but the target report is still false and is unsafe execution input for RP9. In a multi-target retained inventory, paths received only by target A are also reported as migrations for target B. The promotion identity compounds the loss: target identity records receipt ids but not the package-to-receipt-to-path association (`crates/cdf-project/src/promotion.rs:1468`).

Required correction: preserve verified package/receipt association through planning; derive affected paths, addresses, value availability, migrations, and strategy independently for each target; bind that exact association into promotion identity. Add mixed retained/tombstone and disjoint multi-target fixtures that prove no target inherits another target's bytes or paths.

### Significant — Promoted fields corrupt source-name provenance and future reconciliation identity

The proposed schema writes the RFC 6901 path itself into both `cdf:source_name` and `cdf:promoted_path` (`crates/cdf-project/src/promotion.rs:1084`). For top-level `/score`, the source identifier is `score`, while `cdf:source_name` becomes `/score`. VISION §7.4/D-14 require `cdf:source_name` to preserve the source identifier verbatim; residual paths have their own `cdf:promoted_path` metadata. Shared reconciliation keys fields by `cdf:source_name`, so the proposed constraint will not match a future observed field whose source name is `score` (`crates/cdf-contract/src/reconciliation.rs:479`).

Required correction: give promoted fields source identity that matches the ratified reconciliation model and retain the full JSON pointer only in promotion-path metadata. Nested-path behavior needs an explicit test because a flattened promoted output does not have the same identity shape as a top-level physical field. Materialize the proposed schema in a test and reconcile it against a fresh observed source schema.

### Significant — The advertised proposed snapshot is not valid under the schema-snapshot artifact contract

RP5 computes `new_schema_hash` from `SchemaPromotionSnapshotPreimage` and advertises the corresponding `.cdf/schemas/...` path (`crates/cdf-project/src/promotion.rs:1107`). The only current `SchemaSnapshotArtifact` constructors and validator compute and accept hashes from version-1 schema/metadata or version-2 schema/metadata/discovery-manifest preimages (`crates/cdf-project/src/schema_snapshot.rs:625`, `crates/cdf-project/src/schema_snapshot.rs:652`, `crates/cdf-project/src/schema_snapshot.rs:693`). Constructing the proposed schema through either supported artifact constructor therefore produces a different hash, while assigning RP5's hash to the artifact fails `validate_hash_input`.

RP9 may introduce a new, explicitly versioned promotion-snapshot artifact contract, but RP5 currently presents its hash/path as an exact future schema snapshot without defining that compatibility seam or proving materialization. The existing test only hashes the promotion preimage twice and checks that the path contains the hash; it never constructs, serializes, reads, or validates a schema snapshot artifact.

Required correction: either extend the schema-snapshot artifact contract with a versioned promotion preimage now, or make RP5 emit the exact currently valid snapshot artifact/hash plus a separately addressed promotion-plan artifact. Add a round-trip test through `SchemaSnapshotStore`/artifact validation that proves the reported `new_schema_hash` and path are exactly writable by RP9.

### Significant acceptance gap — No compiled project authority can currently grant the tested lossy allowance

The planner reconstructs `ContractPolicy::for_trust` and accepts it only when its semantic hash equals the lock's policy hash (`crates/cdf-project/src/promotion.rs:347`). That is the correct fail-closed behavior for the current project model: lock generation itself derives the same trust policy (`crates/cdf-project/src/lockfile.rs:477`), every trust preset leaves `allow_lossy_mapping` false, and `.10x/knowledge/type-policy-authority.md` explicitly forbids a runtime/test-only allowance until a ratified compiled surface exists.

The positive test named `explicit_lossy_promotion_requires_and_honors_locked_allowance` exercises only `build_path_reports` with a process-local mutated `TypePolicy`; it does not call the full planner with valid compiled/lock authority. Thus the implementation correctly rejects unauthorized lossy promotion, but RP5's progress/evidence overstated a positive governed branch that no valid project could reach at that revision. The later Tier-0 allowance surface is delivered in `.10x/tickets/done/2026-07-09-p2-ws-g2-type-mismatch-diagnostics.md`.

Required disposition: do not invent an RP5-only allowance. Qualify RP5 acceptance/evidence as fail-closed until the existing policy-authority blocker is ratified and implemented, or add that dependency and prove a full-planner positive case whose allowance changes compiled identity, lock policy hash, plan verdict, and package identity.

### Minor — Human rendering omits material plan authority available in JSON

The human report shows path counts, aggregate evidence rows, strategy labels, migration summaries, conflicts, and no-write flags, but omits affected package/address examples, coercion verdicts, destination sheet hashes, receipt-verification state, fresh content identity, and execution preconditions (`crates/cdf-cli/src/schema_command.rs:849`). The ticket requires P1 human and JSON output for affected packages/rows/targets, evidence availability, strategy, migrations, conflicts, and recovery. JSON carries most of this authority; human output does not yet make the plan independently reviewable.

## Passing observations

- The CLI has no `--execute` path and delegates semantic planning to `cdf-project`.
- The local adapter enumerates malformed entries explicitly, verifies non-archived packages, attributes resource ownership from the state-delta preimage, structurally checks receipts, streams segments, and bounds address examples.
- Fresh discovery is kept distinct from runtime residual type evidence; absent fresh authority or an explicit type produces a typed conflict.
- Destination mappings use the shared resolver with exact-over-family specificity and ambiguity rejection.
- Focused CLI evidence proves the inspected local-file happy path leaves the project tree byte-identical across JSON, repeated, human, invalid-type, unknown-path, and stale-lock invocations.

## Verification performed

- `cargo test -p cdf-project promotion -- --nocapture` — 12 passed, 0 failed. This covers helper-level coercion, canonical inventory ordering, fresh authority checks, local missing/tombstone classification, strategy selection, recovery quoting, and promotion-preimage determinism. It does not cover the package-to-target association, valid snapshot-artifact materialization, source-name reconciliation after promotion, or a full-planner lossy allowance.
- `cargo test -p cdf-cli schema_promote_plans_fresh_residual_correction_without_writes -- --nocapture` — 1 passed, 0 failed. It proves one retained package, one DuckDB target, one top-level path, determinism of two ids, and local project-tree no-write behavior. It does not exercise mixed evidence, disjoint targets, nested paths, external state assertions, or snapshot round-trip validation.
- `cargo test -p cdf-contract mapping -- --nocapture` — 6 passed, 0 failed. It covers the shared mapping resolver and policy-gated mapping helper, not RP5 target association or compiled policy authority.
- `git diff --check -- <RP5 implementation and generated CLI files>` — passed with no whitespace errors.

## Verdict

Fail. The dry command is conservatively no-write and several authority boundaries are sound, but the proposed schema cannot yet be trusted as RP9 execution input: it misstates source identity, loses target-specific evidence association, and advertises a snapshot hash that the current snapshot artifact contract cannot materialize. The lossy positive path is additionally unproved and currently unreachable by design. RP5 should remain open until the significant findings are repaired or explicitly reshaped through governing records, then receive a focused re-review.

## Residual limits

This review did not run the entire workspace suite, live Postgres/object-store fixtures, or nightly network conformance. RP5 is a dry planner and the focused failures are deterministic from typed code paths, so those broader environments would not resolve the findings above. Concurrent edits in unrelated P2 lanes were excluded from the verdict.
