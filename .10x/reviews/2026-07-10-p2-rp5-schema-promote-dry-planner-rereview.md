Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: .10x/tickets/done/2026-07-10-p2-rp5-schema-promote-dry-planner-cli.md
Verdict: pass

# P2 RP5 schema-promotion dry-planner final re-review

## Target

The repaired RP5 typed planner, version-3 promotion snapshot artifact, verified-package inventory seam, per-target correction plan and identity, `cdf schema promote` rendering, and strictly no-write command boundary. This re-review follows the fail findings in `.10x/reviews/2026-07-10-p2-rp5-schema-promote-dry-planner-review.md` and inspects every subsequent authority repair.

## Findings

- Pass: version 3 has one typed, versioned `SchemaSnapshotPromotionAuthority` owned by the schema-snapshot layer. `SchemaPromotionSnapshotPlan` carries only the materializable artifact; there is no parallel promotion preimage or type-erased JSON authority. The snapshot layer depends only on lower-level contract/kernel types, so no crate cycle was introduced.
- Pass: the version-3 constructor derives its complete generic metadata projection from typed authority. Only `cdf:normalizer` is projected for compatibility, store validation requires exact equality, legacy promotion metadata is rejected, and `SchemaSnapshotArtifact::normalizer_version` gives callers one precedence-free accessor. A content-addressed artifact can no longer carry contradictory normalizer, old-pin, or lineage-version claims.
- Pass: version-3 semantic validation binds resource id, proposed schema, canonical old-snapshot path, compiler lineage, sorted unique selected paths, nonempty typed evidence, field type/source/path metadata, selected coercion targets, packages, and nonempty sorted package/receipt/target associations. Unknown fields and tampered resource/schema/path/metadata/association facts fail construction or store hydration.
- Pass: version-1 declared and version-2 discovery artifacts remain additive, backward-compatible authorities. The existing exact v1 hash/serialization golden and v2 manifest-linked round trip pass unchanged; the new optional promotion field is omitted for both. Version 3 writes and reads through `SchemaSnapshotStore`, and its reported hash/path exactly match the hydrated artifact.
- Pass: transport-neutral inventory canonicalization now rejects empty or duplicate association receipt ids and cross-checks every package/destination/target association against attributed evidence on the exact receipt-id set and availability. Missing, extra, wrong-target, cross-package, duplicate, and availability-mismatched authority cannot enter target planning, promotion identity, or the v3 artifact.
- Pass: target discovery is the union of verified path associations and attributed receipt evidence. A real archived package with state-preimage and receipt authority remains visible as a tombstone target even though residual bytes and paths are absent; it receives no migrations or strategy and emits precise unavailable-evidence conflicts.
- Pass: retained multi-target planning is target-specific. Each target receives only its associated paths, packages, receipts, evidence, migrations, and capability-selected strategy. Path associations and target evidence are both bound into deterministic promotion identity, preventing the earlier path/target cross-product.
- Pass: promoted top-level fields preserve the verbatim decoded source identifier in `cdf:source_name` and keep the RFC 6901 pointer separately in `cdf:promoted_path`. Shared reconciliation proves the next observed top-level field matches. Nested paths remain reportable but fail closed with `nested_projection_requires_mapping` before snapshot or migration materialization.
- Pass: fresh discovery remains distinct from residual runtime evidence, stale pins and policy hashes fail closed, destination mappings use the shared specificity/ambiguity resolver, and coercion verdicts remain serialized total decisions.
- Pass: human output now exposes fresh content identity, source names, coercion verdicts, affected packages and address examples, per-target sheet/receipt/package/path/evidence facts, migrations, conflicts, all write flags, execution preconditions, and exact recovery command. JSON retains the complete typed report.
- Pass: the command has no execution flag or mutating planner branch. The CLI integration fixture proves byte-identical project state across repeated JSON/human dry plans and invalid, unknown-path, and stale-pin cases; the report declares snapshot, lockfile, package, destination, checkpoint, lease, and ledger writes false.
- Pass with recorded limit: the full planner does not claim a reachable lossy allowance. Current compiled trust policy remains fail-closed; the helper proves classification only. The future Tier-0 authority surface is explicitly owned by `.10x/tickets/2026-07-09-p2-ws-g2-type-mismatch-diagnostics.md` under `.10x/knowledge/type-policy-authority.md`.

## Verification performed

- `cargo test -p cdf-project promotion::tests -- --nocapture` — 14 passed, 0 failed. Covers typed v3 round trip/tamper rejection, normalizer projection, exact generic receipt association validation, real archived-package target inventory, per-target association helpers, source reconciliation, nested fail-closed behavior, deterministic identity, and lossy-helper limits.
- `cargo nextest run -p cdf-project --all-features` — 157 passed, 0 skipped. Includes v1/v2 compatibility, v3 store behavior, local/runtime package paths, and all project tests.
- `cargo test -p cdf-cli schema_promote_plans_fresh_residual_correction_without_writes -- --nocapture` — 1 passed, 0 failed. Covers the retained CLI happy path, deterministic repeated plans, rich human output markers, negative cases, and byte-identical no-write assertions.
- `cargo test -p cdf-contract mapping -- --nocapture` — 6 passed, 0 failed. Covers destination mapping specificity, ambiguity, unsupported authority, and policy-gated lossy mapping.
- `cargo test -p cdf-cli parser_provides_subcommand_help_at_nested_layers -- --nocapture` — 1 passed, 0 failed.
- `cargo fmt --all -- --check` and targeted `git diff --check` — passed.

## Verdict

Pass. The repaired implementation satisfies RP5's dry-planner contract without weakening source identity, schema-snapshot authority, receipt provenance, per-target correction planning, deterministic identity, or no-write guarantees. The original fail findings and both follow-up authority findings are resolved.

## Residual limits

This review proves dry planning only. It does not execute corrections, acquire a lease, reverify live destination state, settle checkpoints, publish the lock CAS, emit promotion ledger events, or change GC retention; those remain explicitly owned by RP9 and later conformance work. A positive full-planner lossy allowance remains blocked and durably owned by the G2 type-policy ticket named above. Live network/object-store conformance and the complete CLI suite were not rerun because they do not affect the repaired typed authority boundaries.
