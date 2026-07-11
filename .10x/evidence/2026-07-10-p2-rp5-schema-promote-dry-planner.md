Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-10-p2-rp5-schema-promote-dry-planner-cli.md, .10x/specs/schema-promotion-corrections.md, .10x/specs/residual-variant-capture.md

# RP5 schema promote dry planner evidence

## What was observed

The typed project-layer promotion planner and thin `cdf schema promote` CLI compile without warnings. Focused tests prove deterministic fresh-discovery auto-proposal, a store-readable version-3 promotion snapshot whose hash binds one typed artifact-owned promotion authority, inventory canonicalization, exact path/package/receipt/target association, transport-neutral/no-write local inventory, bounded address examples with evidence digests, mixed retained/tombstone classification, capability-only strategy selection and ambiguity, shared destination mapping specificity, helper-level lossy allowance classification, adversarial recovery quoting, CLI human/JSON rendering, stale/invalid/unknown negatives, and byte-identical project/state/destination trees across repeated dry plans. Promoted top-level fields preserve the verbatim terminal source identifier in `cdf:source_name`, retain the full pointer in `cdf:promoted_path`, and reconcile on a subsequent observed schema; nested paths fail closed before a proposed snapshot or migration. A real archived package carrying a durable state preimage and receipt is read through the local adapter: its target remains visible as tombstone evidence while paths/migrations stay empty and strategy selection fails closed.

Typed promotion authority is canonical: version-3 generic metadata contains only a constructor-derived `cdf:normalizer` compatibility projection, and the artifact accessor returns that typed value without precedence rules. Conflicting normalizer, legacy promotion metadata, empty/arbitrary authority, and duplicated outer-field mismatches fail store validation. Before target planning, the transport-neutral inventory boundary cross-checks every path association against attributed receipt evidence exactly; empty, duplicate, missing, extra, wrong-target, and availability-mismatched receipt associations are rejected.

Generated help, manpage, and completion artifacts include `schema promote` and match committed snapshots.

## Procedure

- `cargo fmt --all -- --check` — passed.
- `cargo nextest run -p cdf-project --all-features` — 157 passed, 0 skipped after the final authority and receipt cross-check repairs; this includes the exact v1/v2 snapshot-byte compatibility test.
- `cargo test -p cdf-contract destination_mapping_tests -- --nocapture` — 4 passed.
- `cargo test -p cdf-project promotion::tests -- --nocapture` — 14 passed after the final authority and receipt cross-check repairs.
- `cargo test -p cdf-cli schema_promote_plans_fresh_residual_correction_without_writes -- --nocapture` — passed, including repeated deterministic JSON planning and byte-for-byte no-write assertions.
- `cargo test -p cdf-cli parser_provides_subcommand_help_at_nested_layers -- --nocapture` — passed.
- `cargo clippy -p cdf-contract -p cdf-project -p cdf-cli --all-targets -- -D warnings` — passed.
- `cargo run -p cdf-cli --locked --features cli-artifacts --bin cdf-generate-cli-artifacts -- --out-dir crates/cdf-cli/generated` — generated the new command artifacts.
- `cargo test -p cdf-cli --features cli-artifacts cli_generated_artifacts_match_committed_snapshots -- --nocapture` — passed.

## What this supports

- RP5's default command performs no snapshot, lockfile, package, destination, checkpoint, lease, or ledger write.
- The planner consumes typed inventory/discovery/lock facts and is not coupled to filesystem or concrete destination names.
- Promotion and future snapshot identities bind exact authority and remain deterministic under inventory reordering and filesystem relocation.
- Human and JSON reports expose the complete dry-plan contract and pending live receipt/target re-verification precondition.
- Version-1 declared and version-2 discovery snapshot hash inputs remain separate authorities; promotion uses an additive, typed, versioned `SchemaSnapshotPromotionAuthority` and no discovery-manifest reference. Constructor and store validation reject empty selected authority, arbitrary unknown fields, resource/schema/old-path mismatches, noncanonical evidence, and field-provenance disagreement.

## Limits

This is dry-planner evidence only. It does not authorize or exercise correction writes, lease acquisition, destination receipt re-verification, checkpoint settlement, lock CAS publication, or promotion ledger publication; those remain owned by the execution tickets.

The positive lossy test exercises the typed path helper with an explicitly constructed `TypePolicy`; it is not a full-planner positive test. At this evidence revision Tier-0 compiled resource authority derived both allowances as false. The later authority surface is delivered by `.10x/tickets/done/2026-07-09-p2-ws-g2-type-mismatch-diagnostics.md` and satisfies `.10x/knowledge/type-policy-authority.md`; RP5 remains fail-closed and does not treat runtime injection as authority.
