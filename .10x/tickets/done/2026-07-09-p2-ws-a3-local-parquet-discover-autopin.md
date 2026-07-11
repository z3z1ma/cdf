Status: done
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/done/2026-07-08-p2-ws-a-discovery-compiler-stage.md
Depends-On: .10x/decisions/data-onramp-schema-discovery-reconciliation.md, .10x/specs/data-onramp-schema-intelligence.md, .10x/specs/types-contracts-normalization.md, .10x/tickets/done/2026-07-09-p2-ws-a2-local-parquet-discovery-probe.md, .10x/tickets/done/2026-07-08-p2-ws-c1-declarative-schema-normalization.md, .10x/tickets/done/2026-07-09-p2-ws-b3-parquet-declared-schema-reconciliation.md

# P2 WS-A3 local Parquet discover CLI and auto-pin

## Scope

Implement the first operator-visible and first-use discovery flow: a local file resource in `SchemaSource::Discover` mode, with `format = "parquet"` and exactly one resolved local file partition, can be probed with `cdf schema discover <resource>` and is footer-probed at plan/run time, normalized with `namecase-v1`, written as a deterministic schema snapshot under `.cdf/schemas/`, and executed as a pinned `SchemaSource::Discovered` resource.

Owned write scope:

- `crates/cdf-project/src/**` for the project-level auto-pin helper, schema snapshot metadata, and focused tests.
- `crates/cdf-declarative/src/**` only for a small owned-resource/descriptor/schema replacement helper if the project helper needs one.
- `crates/cdf-cli/src/args.rs`, `crates/cdf-cli/src/commands.rs`, `crates/cdf-cli/src/context.rs`, `crates/cdf-cli/src/scan_command.rs`, `crates/cdf-cli/src/run_command.rs`, a focused `schema_command` module if useful, and focused CLI tests for the `schema discover` command and plan/run routing.
- This ticket's evidence and review records.

## Acceptance criteria

- `cdf schema discover <resource>` exists in the CLI help/parser and supports local single-file Parquet discover-mode resources.
- `cdf schema discover <resource>` probes only Parquet metadata/footer, renders the discovered normalized schema, schema hash/path candidate, source identity evidence, and the next command; JSON mode exposes the same fields additively.
- `cdf schema discover <resource>` is non-mutating: it does not write `.cdf/schemas`, `cdf.lock`, packages, destination files, or checkpoint state.
- `cdf plan <resource>` and `cdf run <resource>` no longer fail with "requires a pinned schema hash" for a local discover-mode Parquet file resource that resolves to exactly one file.
- First use probes only Parquet metadata/footer, not row batches, and writes `.cdf/schemas/<resource>@<hash>.json` before any package-producing execution.
- The pinned snapshot hash becomes the resource descriptor's `SchemaSource::Discovered` hash, and plan/run state/commit artifacts use that pinned hash.
- The resource's Arrow schema is the normalized discovered schema: source physical names are preserved in `cdf:source_name`, output names use `namecase-v1`, and `VendorID`-style fields become destination-safe without handwritten `source_name`.
- The runtime open path uses the existing declared-schema Parquet reconciliation path from B3 so batches materialize the normalized schema from the physical Parquet columns.
- Repeating plan/run over unchanged input produces the same schema snapshot path and hash and does not rewrite semantically different snapshot content.
- Non-Parquet discover resources and multi-file discover globs remain fail-closed with an error that names the unsupported discovery slice and avoids silently choosing a file.
- Existing declared-schema resources are not re-probed or re-pinned.
- Focused tests prove:
  - `cdf schema discover <resource>` prints/serializes a local Parquet discovered schema without mutating the project;
  - local Parquet discover-mode plan writes a schema snapshot and reports/uses its hash;
  - local Parquet discover-mode run succeeds through DuckDB and commits a checkpoint using the pinned snapshot hash;
  - a physical `VendorID` Parquet column is normalized to `vendor_id` with `cdf:source_name = "VendorID"` in the pinned schema and output batch path;
  - a multi-match Parquet discover glob fails before package/destination/checkpoint writes.

## Evidence expectations

Record focused evidence for:

- `cargo test -p cdf-project <new auto-pin tests> --locked`;
- `cargo test -p cdf-cli <new discover Parquet plan/run tests> --locked`;
- `cargo test -p cdf-project -p cdf-cli --locked` if CLI/runtime integration is touched;
- `cargo clippy -p cdf-project -p cdf-cli --all-targets --locked -- -D warnings`;
- `cargo fmt --all -- --check`;
- `git diff --check`;
- scoped `jscpd` and `rust-code-analysis-cli` on touched Rust files;
- scoped Gitleaks/Semgrep on touched source files;
- CodeQL through `tools/codeql-rust-quality.sh` using the reusable DB path if the final touched set includes CLI/runtime code or the quality cadence requires it.

If Cargo metadata changes, also record dependency/supply-chain checks.

## Explicit exclusions

This ticket does not implement remote ranged Parquet discovery, multi-file schema union/variance, CSV/JSON/NDJSON sampling, SQL/REST discovery, future Avro/Python/WASM discovery, `cdf schema pin|show|diff`, lockfile update semantics, `cdf add`, ad-hoc mode, `--no-pin`, destination-specific identifier policy selection, or S1/S2/S8 conformance closure.

## Progress and notes

- 2026-07-09: Opened after B3 made declared Parquet reconciliation executable and A2 added the local footer probe/snapshot artifact helper. Source inspection found `run_project`, destination planning, and CLI plan/run still call `pinned_schema_hash(resource)` on the compiled resource, so unpinned `SchemaSource::Discover` resources fail before any local Parquet probe can run. CLI tests currently assert that discover-mode run failure.
- 2026-07-09: Amended before implementation after user clarification that operator-invoked discovery is product-critical. This child now includes `cdf schema discover <resource>` for local single-file Parquet as a no-mutation probe. Durable `schema pin/show/diff` remains a later CLI slice because `pin` needs lockfile/reference semantics to avoid writing orphaned snapshots.
- 2026-07-09: Marked active and assigned implementation to a worker subagent. The worker owns the scoped code patch; parent owns review, evidence, quality gates, record reconciliation, and commit.
- 2026-07-09: Implemented and parent-reviewed. Added `cdf schema discover <resource>` for local single-file Parquet, project-level local Parquet discover/auto-pin helper, normalized deterministic snapshot metadata, plan/run routing through a pinned clone, and fail-closed unsupported/multi-file behavior. Parent review removed an unnecessary append-resource test key, moved normalizer metadata to the actual normalization path, and eliminated A3-introduced duplicate fixture blocks before final quality.
- 2026-07-09: Closed with evidence `.10x/evidence/2026-07-09-p2-ws-a3-local-parquet-discover-autopin.md` and review `.10x/reviews/2026-07-09-p2-ws-a3-local-parquet-discover-autopin-review.md`. Focused discover/auto-pin tests, broad `cdf-project`/`cdf-cli` tests, fmt, clippy, diff check, implementation-only jscpd, broad touched-file jscpd with old-test residual classification, rust-code-analysis, Semgrep, Gitleaks, cargo deny/audit/vet/machete, OSV, and reusable-DB CodeQL were recorded.

## Blockers

None.
