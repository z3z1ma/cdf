Status: done
Created: 2026-07-08
Updated: 2026-07-09
Parent: .10x/tickets/done/2026-07-08-p2-ws-c-source-identity-normalization.md
Depends-On: .10x/decisions/data-onramp-source-identity-preview-disposition.md, .10x/specs/data-onramp-source-experience-cli.md

# P2 WS-C1 declarative schema source-name defaults and normalization

## Scope

Wire automatic `namecase-v1` normalization into declarative compiled schemas so source names such as `VendorID` no longer require manual `source_name` declarations to produce destination-safe field names.

Owned write scope:

- `crates/cdf-declarative/src/compiled.rs`
- `crates/cdf-declarative/src/tests.rs`
- `crates/cdf-declarative/Cargo.toml` only if an explicit dependency on `cdf-contract` is required for the existing normalizer

## Acceptance criteria

- When a declared schema field omits `source_name`, the compiler treats the declared field name as the source-original name and records it in `cdf:source_name`.
- The compiled schema field name is the `namecase-v1` normalized output name.
- Existing explicit `source_name` remains an override for source-original ambiguity and is preserved in metadata.
- Post-normalization collisions such as `userName` and `user_name` fail at compile/plan time with a rename hint.
- At least one regression test proves `VendorID` compiles to field name `vendor_id` with `cdf:source_name = "VendorID"`.
- Existing file/REST/SQL declarative tests are updated only where their assertions depend on the old unnormalized field names.

## Evidence expectations

Record focused evidence for:

- `cargo test -p cdf-declarative <new normalization tests> --locked`
- `cargo test -p cdf-declarative --locked`
- `cargo clippy -p cdf-declarative --all-targets --locked -- -D warnings`
- `cargo fmt --all -- --check`
- `jscpd` scoped to touched Rust files
- `git diff --check`

## Explicit exclusions

This ticket does not implement destination-specific sheet charset/length variants, schema snapshot recording, `cdf diff schema`, or package-level normalizer-version evidence. Later WS-C tickets own those.

## Progress and notes

- 2026-07-08: Opened after source inspection found `cdf-contract::normalize_arrow_schema` already exists, while `cdf-declarative::compile_schema` currently passes through field names and only records `source_name` when manually provided.
- 2026-07-09: Implemented automatic source-name metadata and `namecase-v1` normalization in `compile_schema` by reusing `cdf-contract::normalize_arrow_schema` and `IdentifierPolicy::default()`. Added focused tests for `VendorID` -> `vendor_id`, omitted `source_name`, explicit `source_name`, and post-normalization collision hints.
- 2026-07-09: Evidence recorded in `.10x/evidence/2026-07-09-p2-ws-c1-declarative-schema-normalization.md`; review recorded in `.10x/reviews/2026-07-09-p2-ws-c1-declarative-schema-normalization-review.md`.

## Blockers

None.
