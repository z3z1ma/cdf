Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-source-decode-type-drift-quarantine-seam.md
Verdict: pass

# Source decode type-drift quarantine seam review

## Target

Review of the source-decode type-drift quarantine seam and E6 conformance integration:

- `crates/cdf-kernel/src/batch.rs`
- `crates/cdf-formats/src/readers.rs`
- `crates/cdf-declarative/src/file_runtime.rs`
- `crates/cdf-engine/src/execution.rs`
- `crates/cdf-conformance/src/live_run/drift_quarantine/**`
- affected tests, CLI preview fixtures, and live-run golden expected evidence.

## Findings

- Pass: the seam follows the ratified decision. Localized scalar type mismatches under declared JSON/NDJSON schemas become pre-contract quarantine facts; malformed JSON, undeclared fields, and complex/unlocalizable values still fail closed before package finalization or destination mutation.
- Pass: accepted rows continue through the existing package, destination, receipt verification, and checkpoint-gate path. The conformance scenario proves both unsupported mirror exclusion and supported Postgres `_cdf_quarantine` mirroring for literal type drift.
- Pass: pre-contract facts are folded into ordinary package quarantine artifacts and verdict/quarantine summaries. The engine keeps `evaluate_record_batch` fail-closed and does not invent a multi-output DataFusion plan.
- Pass: source-decode observed values use the same `pii:*` redaction policy shape as contract quarantine candidates. A focused decoder test proves raw `alice@example.com` becomes the expected SHA-256 redaction.
- Resolved during review: CLI preview/glob tests declared `updated_at` as `int64` while several ad hoc fixtures used RFC3339 strings. The new seam correctly quarantined those rows, so the fixtures were repaired to numeric values rather than weakening declared-schema enforcement.
- Resolved during review: live-run golden package identities changed because declared-schema JSON/NDJSON reads now preserve declared Arrow schema metadata in the IPC segment and schema artifact. The golden fixtures were updated and verified with DuckDB/Parquet 100-run loops plus bounded Postgres coverage.

## Residual Risk

`filter_declared_ndjson_rows` is deliberately narrow: it supports scalar-ish declared JSON types and fails closed on unsupported complex declared types or undeclared fields. That is acceptable for this seam because the governing decision authorized only localized scalar source type-drift quarantine.

At review time, the existing `cargo machete` hint for `cdf-cli -> cdf-dest-parquet` was outside this seam; it is now closed under `.10x/tickets/done/2026-07-08-cdf-cli-unused-parquet-dependency.md`.

## Verdict

Pass. The seam and E6 scenario satisfy the acceptance criteria with evidence in `.10x/evidence/2026-07-08-source-decode-type-drift-quarantine-seam.md`.
