Status: open
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-08-p2-ws-c-source-identity-normalization.md
Depends-On: .10x/decisions/data-onramp-source-identity-preview-disposition.md, .10x/specs/types-contracts-normalization.md, .10x/tickets/done/2026-07-08-p2-ws-c1-declarative-schema-normalization.md

# P2 WS-C2 destination identifier policy adapter

## Scope

Make destination-sheet identifier rules consumable by the contract normalizer so later plan/run integration does not hard-code `IdentifierPolicy::default()`. This is the adapter/foundation slice, not the full live destination join.

Owned write scope:

- `crates/cdf-contract/src/policy.rs` and `crates/cdf-contract/src/normalization.rs` for destination-rule adaptation and tests;
- `crates/cdf-kernel/src/destination.rs` only if a small helper or doc-level enum makes `IdentifierRules` less stringly typed without changing sheet serialization;
- destination sheet tests only if needed to prove current DuckDB/Postgres sheet rules adapt correctly;
- this ticket's evidence and review records.

## Acceptance criteria

- A public adapter converts kernel `IdentifierRules` into the contract `IdentifierPolicy` used by `normalize_schema`/`normalize_arrow_schema`.
- The adapter supports the current namecase-backed destination rules, including:
  - DuckDB `namecase-v1`;
  - Postgres `namecase-v1/postgres-quoted-v1`, preserving the sheet's 63-byte limit and reserved-pattern intent where expressible.
- Unsupported non-column identifier normalizers such as Parquet's object-key component rule fail with an explicit error that names the destination rule and says live column normalization for that rule is not implemented in this adapter.
- Adapter tests prove max-length behavior, allowed-pattern rejection where available, and stable collision behavior after applying the destination-derived policy.
- Existing C1 declarative default normalization remains unchanged until later integration wires destination-specific policy selection into planning.

## Evidence expectations

Record focused evidence for:

- `cargo test -p cdf-contract <new destination identifier adapter tests> --locked`;
- `cargo test -p cdf-contract --locked`;
- `cargo clippy -p cdf-contract --all-targets --locked -- -D warnings`;
- `cargo fmt --all -- --check`;
- scoped `jscpd` and `rust-code-analysis-cli` on touched Rust files;
- `git diff --check`;
- gitleaks and banned-phrase/rename scans on touched records.

## Explicit exclusions

This ticket does not integrate destination-derived policies into `cdf-project` plan/run, package manifests, schema snapshots, `cdf diff schema`, or conformance live runs. Later WS-C children own those after the adapter exists.

## Progress and notes

- 2026-07-09: Opened after C1 closed automatic declarative source-name metadata and default `namecase-v1` normalization. Source inspection found destination sheets already expose `IdentifierRules`, while `cdf-declarative` currently calls `IdentifierPolicy::default()` directly.

## Blockers

None.
