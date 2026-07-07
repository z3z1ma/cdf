Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Target: .10x/tickets/done/2026-07-07-cli-replay-package-spine.md
Verdict: pass

# CLI Postgres Package Replay Review

## Target

Final Postgres slice and closure audit for `.10x/tickets/done/2026-07-07-cli-replay-package-spine.md`, covering `crates/cdf-cli/src/args.rs`, `crates/cdf-cli/src/commands.rs`, `crates/cdf-cli/src/tests.rs`, `crates/cdf-cli/Cargo.toml`, and `Cargo.lock`.

## Findings

No blocking findings.

The implementation follows `.10x/decisions/destination-introspection-package-and-cli-policy.md`: Postgres replay does not infer write semantics from destination introspection. It requires `--target schema.table` and `--merge-dedup fail`, rejects missing or unsupported values as usage/contract errors, and validates the explicit target against the package destination-commit target before state or destination mutation.

Secret-backed Postgres replay uses the existing project secret provider boundary and imports the `cdf_http::SecretProvider` trait explicitly. The redaction path is narrow: only secret-backed replay errors rewrite the resolved DSN, and tests cover target-mismatch errors that would otherwise carry the connection string.

Replay remains package-artifact based. The live CLI test deletes the source file and state store before replay, then verifies package receipt append, checkpoint commit, one `replay_recorded` ledger event, and the target row count in Postgres.

The large Postgres destination field is boxed in the internal CLI enum after Clippy flagged `large_enum_variant`; this keeps the enum size reasonable without changing behavior or adding abstraction.

## Residual risk

The optional local Postgres harness uses `/tmp` for the Unix socket directory to avoid macOS socket path length failures. The data directory remains under the existing CLI test target root. This is test infrastructure only.

The broader CLI spine remains open for `run`, `resume`, and `inspect run`; this review covers only the replay child.

## Verdict

Pass. The replay child acceptance criteria are supported by focused live tests, full workspace tests, supply-chain checks, Semgrep, CodeQL, secret scans, and recorded prior DuckDB/Parquet replay evidence.
