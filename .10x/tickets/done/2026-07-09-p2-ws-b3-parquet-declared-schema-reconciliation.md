Status: done
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-08-p2-ws-b-schema-reconciliation-arrow-vocabulary.md
Depends-On: .10x/decisions/data-onramp-schema-discovery-reconciliation.md, .10x/specs/data-onramp-schema-intelligence.md, .10x/specs/types-contracts-normalization.md, .10x/tickets/done/2026-07-09-p2-ws-b2-schema-reconciliation-core.md

# P2 WS-B3 Parquet declared-schema reconciliation

## Scope

Integrate the shared observed-vs-constraint schema reconciler into the local Parquet file read path so Parquet physical schema facts constrain and feed declared schemas instead of bypassing them. This is the first per-format integration slice for WS-B and directly addresses P2 frictions 4 and 5 for local Parquet files.

Owned write scope:

- `crates/cdf-formats/src/**` for the Parquet reader handoff, batch projection/casting helper, and focused format tests.
- `crates/cdf-formats/Cargo.toml` and `Cargo.lock` only if Arrow cast kernels are needed to materialize the reconciled schema.
- `crates/cdf-declarative/src/file_runtime.rs` and focused declarative tests only if the existing caller gate must route declared schemas to Parquet.
- This ticket's evidence and review records.

## Acceptance criteria

- `read_file_source_with_declared_schema` applies reconciliation for `FileFormat::Parquet` instead of delegating to `read_file_source`.
- The observed Parquet Arrow schema remains the physical fact used for reconciliation; the output `FormatRead` descriptor, batches, `observed_schema`, and `schema_hash` use the reconciled schema when reconciliation succeeds.
- Lossless widenings supported by B2 are materialized for Parquet batches where Arrow has safe casts, including `int32 -> int64` and `float32 -> float64`; unsupported or policy-rejected mappings fail closed with the B2 reconciler's operator-fix message.
- Reconciled output fields preserve declared field names/metadata, carry automatic `cdf:source_name` and `cdf:physical_type` provenance where B2 requires it, and exclude extra observed fields outside the declared projection.
- Existing Parquet behavior remains unchanged when no declared schema is provided.
- Preview and run callers in `cdf-declarative` route non-empty declared Parquet schemas through the same reconciled read path.
- Focused tests prove:
  - a Parquet `INT32` physical field declared as `int64` loads with an `Int64` output column and preserved values;
  - a declared projection can rename a Parquet physical field by `cdf:source_name` and drops extra physical fields from the output;
  - a lossy declared narrowing fails before batches are emitted and names observed and declared types;
  - ordinary undeclared Parquet reads remain byte/schema compatible with the pre-existing path.

## Evidence expectations

Record focused evidence for:

- `cargo test -p cdf-formats parquet --locked`;
- `cargo test -p cdf-declarative parquet --locked` if declarative runtime is touched;
- `cargo test -p cdf-formats --locked`;
- `cargo clippy -p cdf-formats --all-targets --locked -- -D warnings`;
- `cargo clippy -p cdf-declarative --all-targets --locked -- -D warnings` if declarative runtime is touched;
- `cargo fmt --all -- --check`;
- `jscpd` scoped to touched Rust files;
- `rust-code-analysis-cli` scoped to touched Rust files;
- `git diff --check`.

If `Cargo.lock` changes, also record scoped dependency/supply-chain checks relevant to the added dependency.

## Explicit exclusions

This ticket does not implement discovery auto-pin, remote Parquet ranged reads, REST/SQL/NDJSON reconciliation unification, validation-program serialization of the coercion plan, row-level drift quarantine for incompatible Parquet files, conformance S1/S2 closure, or package/golden fixture regeneration beyond focused tests required by this slice.

## Progress and notes

- 2026-07-09: Opened after B2 established the shared reconciler and A2 established local Parquet footer discovery. Source inspection found `cdf-formats` still routes `FileFormat::Parquet` declared-schema reads through the undeclared physical path, while `cdf-declarative` only routes declared schemas for JSON/NDJSON.
- 2026-07-09: Parent marked this child active and assigned implementation to a worker subagent. The worker owns the scoped code patch; parent owns review, evidence, quality gates, record reconciliation, and commit.
- 2026-07-09: Implemented the Parquet declared-schema path in `cdf-formats`, routing declared Parquet reads through the shared reconciler, materializing supported Arrow casts with `arrow-cast@59.1.0`, preserving physical provenance metadata, and routing declared Parquet resources through `cdf-declarative`'s declared-schema reader gate.
- 2026-07-09: Parent verification completed. Evidence: `.10x/evidence/2026-07-09-p2-ws-b3-parquet-declared-schema-reconciliation.md`. Review: `.10x/reviews/2026-07-09-p2-ws-b3-parquet-declared-schema-reconciliation-review.md`.

## Blockers

None.
