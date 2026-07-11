Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Target: .10x/tickets/done/2026-07-08-p2-ws-a1-schema-source-model-snapshot-foundation.md
Verdict: pass

# P2 WS-A1 schema source model and snapshot foundation review

## Target

Review of the schema-source model split, project schema snapshot artifact/store, lockfile plumbing, validation behavior, and compatibility edits implemented for `.10x/tickets/done/2026-07-08-p2-ws-a1-schema-source-model-snapshot-foundation.md`.

Governing records:

- `.10x/decisions/data-onramp-schema-discovery-reconciliation.md`
- `.10x/specs/data-onramp-schema-intelligence.md`

Evidence:

- `.10x/evidence/2026-07-09-p2-ws-a1-schema-source-model-snapshot-foundation.md`

## Findings

No blocking findings.

## Assumptions Tested

The model split is real rather than a renamed optional hash. `SchemaSource::Discover` now represents unpinned intent, while `SchemaSource::Discovered { snapshot }` carries concrete pinned evidence. Hints mode can exist before pinning and can carry a pinned snapshot later. Focused kernel serde tests cover the new states.

The snapshot artifact does not depend on unstable map ordering or Arrow's optional serde feature. `SchemaSnapshotArtifact::new` builds a sorted deterministic hash input from a recursive project-owned Arrow schema JSON model plus sorted metadata, computes `sha256:<digest>`, and derives `.cdf/schemas/<resource>@<hash>.json`. The store validates hash input and path consistency on write and read.

Snapshot references cannot escape the project root through stored paths. Parent review found that `SchemaSnapshotStore::read` originally joined the reference path directly; the integration fix routes both read and path construction through `validate_snapshot_reference_path`, and the new traversal regression test rejects `../outside.json`.

Project validation no longer rejects every discovered source only because it is discovered. `pinned_schema_hash` accepts declared schemas, pinned discovered snapshots, and pinned hints snapshots, while still rejecting unpinned discover/hints and keeping contract-sourced schemas outside this slice.

Existing package/run behavior remains deterministic for the checked crates and the wider workspace. The final parent run passed `cargo fmt --all -- --check`, `git diff --check`, `cargo check --workspace --all-targets --locked`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `cargo test --workspace --all-targets --locked --no-fail-fast`, including live-run golden determinism after regenerating the expected hashes for the ratified schema-source model change.

Complexity was checked and acted on. `rust-code-analysis-cli` initially reported `SchemaSnapshotDataType::from_arrow` at cyclomatic 44; the function was split into scalar/temporal/binary/text/nested helpers, reducing final `from_arrow` to cyclomatic 6 without changing golden outcomes.

## Residual Risk

Compatibility edits landed outside the ticket's nominal write-scope list in `cdf-formats`, `cdf-dest-postgres`, `cdf-engine`, `cdf-python`, `cdf-benchmarks`, and `cdf-conformance`. They were necessary for the workspace to compile after the kernel enum split. They are mechanical descriptor/test updates that provide pinned snapshot references where observed schema hashes already existed; they do not add discovery probes, CLI behavior, auto-pin, or lockfile diff rendering.

`jscpd` reports residual duplication across the integrated touched file set with `newClones = 0`. This is a tracked quality signal, not a blocking finding for A1.

CodeQL reports three pre-existing hard-coded cryptographic value findings in `crates/cdf-cli/src/tests.rs`, owned by `.10x/tickets/done/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md`. OSV reports only the already-ratified `RUSTSEC-2024-0436` `paste` advisory exception.

## Verdict

Pass. Acceptance criteria are supported by the implementation and recorded evidence. No follow-up ticket is required from this review.
