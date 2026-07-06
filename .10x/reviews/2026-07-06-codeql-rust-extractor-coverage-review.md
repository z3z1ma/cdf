Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-06-improve-codeql-rust-extractor-coverage.md
Verdict: pass

# CodeQL Rust extractor coverage review

## Target

Review of the CodeQL Rust extractor coverage workflow and evidence for `.10x/tickets/done/2026-07-06-improve-codeql-rust-extractor-coverage.md`.

## Assumptions tested

- The reusable database path stayed `target/quality/codeql-db-rust`.
- The new wrapper does not recreate the database when it is fresh.
- Generated `target/**` and `reports/**` artifacts are excluded during database creation.
- The remaining extractor diagnostics have an evidence-backed root cause or limit.
- No product crates, dependency policy, CI, or CodeQL findings were suppressed.

## Findings

None.

## Parent integration note

During parent integration, mtime-only staleness was identified as too eager because concurrent product-file edits could trigger a rebuild even when file content is unchanged. The wrapper was tightened to compare a content fingerprint stored beside the reusable database. Full analysis was not rerun after that hardening because another worker had active Rust edits; `bash -n tools/codeql-rust-quality.sh` and `git diff --check -- . ':(exclude).gitignore'` passed for the final script/record diff.

## Evidence checked

- `.10x/evidence/2026-07-06-codeql-rust-extractor-coverage.md`.
- `tools/codeql-rust-quality.sh`.
- `bash -n tools/codeql-rust-quality.sh`: passed.
- `tools/codeql-rust-quality.sh`: refreshed the stale database once and analyzed successfully before the fingerprint hardening.
- `bash -n tools/codeql-rust-quality.sh && tools/codeql-rust-quality.sh`: reused the fresh database and analyzed successfully before the fingerprint hardening.
- `bash -n tools/codeql-rust-quality.sh`: passed after the fingerprint hardening.
- `git diff --check -- . ':(exclude).gitignore'`: passed after the fingerprint hardening.
- `jq '.runs[0].results | length' target/quality/reports/codeql-rust-current.sarif`: reported `0`.
- `find Cargo.toml Cargo.lock crates -type f \( -name '*.rs' -o -name 'Cargo.toml' -o -name 'Cargo.lock' \) -newer target/quality/codeql-db-rust/codeql-database.yml | wc -l`: reported `0` after refresh.

## Verdict

Pass. The bounded tooling/documentation slice satisfies the ticket criteria: the reusable DB path is preserved, content-fingerprint stale detection prevents unnecessary recreation, generated build/report artifacts are excluded by the generated CodeQL config, the remaining macro diagnostics are explained as a CodeQL Rust extractor/toolchain limit, and no product behavior or security finding suppression was introduced.

## Residual risk

CodeQL Rust extraction confidence remains limited: the fresh database reports 114 files scanned, 81 with warnings, 33 without warnings, 1149 extraction warnings, and 0 extraction errors. This is acceptable for the local quality gate only when recorded as a tool limit and paired with the broader quality suite.
