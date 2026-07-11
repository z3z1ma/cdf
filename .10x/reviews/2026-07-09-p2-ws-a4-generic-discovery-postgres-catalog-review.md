Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Target: .10x/tickets/done/2026-07-09-p2-ws-a4-generic-discovery-postgres-catalog.md
Verdict: pass

# P2 WS-A4 generic discovery dispatcher and Postgres catalog probe review

## Target

Review of the A4 implementation that changes `cdf schema discover` from a local-Parquet-only doorway into a generic project dispatcher and adds catalog-only discovery for declarative Postgres table resources.

## Findings

- Pass: the CLI schema command now calls the generic project discovery API and supplies the project secret provider, so Postgres discovery can resolve `secret://` references without rendering the resolved DSN.
- Pass: local single-file Parquet discovery remains covered through the dispatcher, preserving the A3 no-write discovery behavior and report shape.
- Pass: Postgres discovery is catalog-only. The implementation queries `information_schema.columns`, maps nullable state and supported catalog types into Arrow, and does not read user rows, create packages, touch destinations, or commit checkpoints.
- Pass: Postgres catalog type mapping is intentionally constrained to the executable source subset. Unsupported types fail with the resource id, column name, observed type, and remediation instead of producing a snapshot the current source path cannot honor.
- Pass: field names are normalized through `namecase-v1`; `cdf:source_name` and `cdf:physical_type` metadata are preserved in the report and snapshot artifact; snapshot metadata records probe/source/dialect/table without recording the resolved connection string.
- Pass: unsupported REST resources, arbitrary SQL query resources, and non-Postgres SQL dialects fail closed through the dispatcher and cannot fall through to the Parquet path.
- Pass: focused tests, full affected-crate tests, workspace clippy/tests, and mandatory QUALITY tooling were run. CodeQL and OSV surfaced only already-owned/ratified findings.

## Residual Risk

- SQL `plan`/`run` auto-pin remains excluded and correctly open under WS-A. Closing it safely requires pinned-discovered-schema handling in package-producing SQL execution and source-name-aware SQL materialization, not merely calling the catalog probe.
- REST, Python, WASM, Avro-like file discovery, CSV/JSON/NDJSON sampling, and remote ranged Parquet discovery remain future source-archetype children. The dispatcher shape reduces future risk but does not implement those probes.
- Broad touched-test `jscpd` still reports duplication in existing test harness scaffolds. The implementation-file clone scan is clean; broad harness cleanup is not part of A4 and should not block this slice.
- CodeQL still reports the three pre-existing CLI test-fixture hard-coded crypto values owned by `.10x/tickets/done/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md`.

## Verdict

Pass. The implementation satisfies A4 acceptance criteria, keeps discovery bounded and source-neutral in shape, and leaves the excluded product-wide discovery surfaces explicitly owned by the WS-A parent.
