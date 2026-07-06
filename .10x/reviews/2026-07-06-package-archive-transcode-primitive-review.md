Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-06-package-archive-transcode-primitive.md
Verdict: pass

# Package archive transcode primitive review

## Target

Review of the package archive transcode primitive and shared DuckDB-backed Parquet writer extraction in `crates/firn-package` and `crates/firn-dest-parquet`.

## Assumptions tested

- The primitive must verify package identity before producing any Parquet report for tampered canonical package files.
- Arrow IPC remains canonical; Parquet output is only an archive/interchange projection.
- The primitive must not write archive files, mutate package manifests, change lifecycle status, change identity/package hashes, or alter IPC replay preference.
- Reusing the Parquet destination writer must not change destination commit semantics.
- The implementation must avoid the direct arrow-rs `parquet`/`paste` supply-chain path.
- New crate code should follow the user-ratified non-monolithic crate-root convention.

## Findings

No unresolved findings.

Parent review initially found one significant test-quality gap: `cargo mutants` showed that `validate_field_names` could be replaced with `Ok(())` without failing tests. This was fixed by adding `archive_transcode_rejects_duplicate_column_names_before_duckdb_ddl`, and the final mutation run had 0 missed mutants.

The broad `gitleaks --no-git --source .` scan found generated-artifact hits under `target/` and prior quality JSON reports. This is not treated as a source leak because the tracked/untracked non-ignored source mirror scan passed with no leaks, and the hit files are outside the intended commit surface.

## Verdict

Pass. Acceptance criteria are covered by focused tests, workspace tests, mutation testing, coverage, supply-chain/security scans, CodeQL using the reusable database, and parent review. The implementation keeps `firn-package` split into focused modules, centralizes the DuckDB writer without adding `parquet`/`paste`, and leaves the parent archive CLI/placement/manifest work open.

## Residual risk

The primitive returns Parquet bytes in memory and does not yet define archive file layout, CLI UX, manifest metadata, retention, or lifecycle transitions. That is intentional ticket scope, and the remaining work is still owned by `.10x/tickets/done/2026-07-05-singer-airbyte-and-package-archive.md`.
