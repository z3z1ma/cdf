Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-06-rustsec-paste-parquet-exception.md, .10x/decisions/native-arrow-datafusion-parquet-policy.md

# Scoped RUSTSEC paste exception evidence

## What was observed

`deny.toml` now ignores exactly `RUSTSEC-2024-0436`. This is the scoped native-Parquet exception ratified by `.10x/decisions/native-arrow-datafusion-parquet-policy.md`: `paste 1.0.15` is allowed only when introduced through the native arrow-rs/DataFusion Parquet path. No Cargo dependency versions were changed in this policy-only ticket.

The current dependency graph still does not contain `paste` or `parquet`; this ticket pre-authorizes the later native Parquet reader/writer work without exercising the exception yet.

## Procedure

- Read `.10x/decisions/native-arrow-datafusion-parquet-policy.md`, `.10x/research/2026-07-06-native-parquet-paste-risk.md`, `.10x/tickets/done/2026-07-06-rustsec-paste-parquet-exception.md`, and current `deny.toml`.
- Read-only subagent audit confirmed the minimal policy edit and found no need for cargo-vet metadata changes before native Parquet dependencies are added.
- Edited `deny.toml` only for advisory policy: one ignore entry plus a 10x comment describing the removal condition.
- `git diff --check`: passed.
- `cargo deny check > target/quality/reports/deny-rustsec-paste-exception.txt 2>&1`: passed. The report ends with `advisories ok, bans ok, licenses ok, sources ok` and emits `warning[advisory-not-detected]` because the ignored advisory is not in the current dependency graph yet.
- `cargo audit`: passed.
- `cargo audit --json > target/quality/reports/cargo-audit-rustsec-paste-exception.json`: reported `vulnerability_count: 0` and `warning_count: 0`.
- `osv-scanner scan source -r . --format json --output target/quality/reports/osv-rustsec-paste-exception.json`: passed with 0 results.
- `cargo vet --locked --output-format json --output-file target/quality/reports/cargo-vet-rustsec-paste-exception.json`: passed with `conclusion: success` and 0 failures.
- `cargo metadata --locked --format-version 1`: passed.
- `cargo tree --workspace --locked -i paste`: exited 101 because `paste` is absent from the graph.
- `cargo tree --workspace --locked -i parquet`: exited 101 because `parquet` is absent from the graph.
- `cargo fmt --all -- --check`: passed.
- Source-only `gitleaks detect --no-git --redact --source <temporary source snapshot> --report-format json --report-path target/quality/reports/gitleaks-rustsec-paste-exception.json --no-banner`: passed, 0 findings.

## What this supports

This supports closing `.10x/tickets/done/2026-07-06-rustsec-paste-parquet-exception.md`: local advisory policy now represents the ratified exception, no broad advisory ignore was introduced, scanner gates remain clean, cargo-vet metadata does not need a pre-dependency update, and native Parquet implementation tickets can proceed with the exception already explicit.

## Limits

This evidence does not add native Parquet dependencies, replace DuckDB-backed Parquet readers/writers, or prove the future dependency path. The future native Parquet tickets must record the actual `paste -> parquet -> arrow-rs/DataFusion` graph and scanner behavior once dependencies enter `Cargo.lock`.

The worktree still had an unrelated `.gitignore` modification before and after this ticket; it was not staged or included.
