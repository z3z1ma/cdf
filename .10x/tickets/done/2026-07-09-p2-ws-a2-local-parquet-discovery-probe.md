Status: done
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/done/2026-07-08-p2-ws-a-discovery-compiler-stage.md
Depends-On: .10x/decisions/data-onramp-schema-discovery-reconciliation.md, .10x/specs/data-onramp-schema-intelligence.md, .10x/tickets/done/2026-07-08-p2-ws-a1-schema-source-model-snapshot-foundation.md

# P2 WS-A2 local Parquet discovery probe

## Scope

Implement the first concrete discovery probe: a bounded local Parquet footer/schema probe that produces a deterministic project schema snapshot artifact without materializing row batches.

Owned write scope:

- `crates/cdf-formats/src/**` for a local Parquet schema discovery API and focused tests
- `crates/cdf-project/src/**` for a small schema-snapshot handoff helper and focused tests
- `Cargo.toml` only if a direct internal dependency is needed for the project helper
- this ticket's evidence and review records

## Acceptance criteria

- A public `cdf-formats` API can inspect a local Parquet file and return its Arrow schema plus source content-identity evidence sufficient for a later probe cache.
- The Parquet probe reads schema/footer metadata and does not build record batch readers or materialize row data.
- A `cdf-project` helper can turn the discovered schema into a `SchemaSnapshotArtifact` and `SchemaSnapshotReference` using the existing `.cdf/schemas/<resource>@<hash>.json` model.
- Snapshot metadata records at least `probe = parquet-footer` and `format = parquet`; source identity evidence is returned by the probe or helper without leaking absolute local paths into the schema hash unless an existing active record requires it.
- Repeating the probe for unchanged input and resource id produces an identical schema snapshot hash and path.
- Invalid or non-Parquet input fails with an actionable data error naming Parquet metadata discovery.

## Evidence expectations

Record focused evidence for:

- `cargo test -p cdf-formats <new parquet discovery tests> --locked`
- `cargo test -p cdf-project <new snapshot handoff tests> --locked`
- `cargo test -p cdf-formats -p cdf-project --locked`
- `cargo clippy -p cdf-formats -p cdf-project --all-targets --locked -- -D warnings`
- `cargo fmt --all -- --check`
- `git diff --check`
- `jscpd` scoped to touched Rust files
- `rust-code-analysis-cli` scoped to new/changed discovery code

Run Semgrep/Gitleaks/supply-chain/CodeQL as needed for the final touched set; use the reusable CodeQL database path, not an ad-hoc database.

## Explicit exclusions

This ticket does not implement HTTP ranged Parquet discovery, object-store discovery, CSV/JSON/NDJSON sampling, SQL/REST discovery, CLI schema commands, first-use auto-pin, lockfile writes, run/plan integration, drift handling, or conformance S1/S2 closure.

## Progress and notes

- 2026-07-09: Opened after A1 established schema snapshot artifacts and E1 established a local/HTTP file transport facade. This child intentionally starts with local Parquet because the existing `cdf-formats` crate already owns Parquet Arrow dependencies and can prove the footer/schema probe shape before remote ranged transport is wired in.
- 2026-07-09: Implemented and verified the local Parquet footer/schema probe plus project schema snapshot handoff. Evidence is `.10x/evidence/2026-07-09-p2-ws-a2-local-parquet-discovery-probe.md`; review is `.10x/reviews/2026-07-09-p2-ws-a2-local-parquet-discovery-probe-review.md`.

## Blockers

None.
