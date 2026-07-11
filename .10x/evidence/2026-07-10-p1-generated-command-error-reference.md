Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-08-p1-product-ws6b-generated-reference-freshness.md, .10x/tickets/done/2026-07-08-p1-product-ws4d-error-rendering-docs.md

# P1 generated command and error reference

## What was observed

The repository now generates one Markdown page for every visible clap command and a catalog table for every registered CLI error mapping. The error table includes code, area, shared error kind, exit code, meaning, remediation, and a representative command. Both trees are compared byte-for-byte with freshly generated output in fast CI.

The existing completion and man-page artifacts were regenerated at the same time because the previously landed REST `cdf add` flags had made those source-derived artifacts stale.

## Procedure

The following commands passed from the repository root:

```text
cargo run -p cdf-cli --locked --features cli-artifacts --bin cdf-generate-cli-artifacts -- --docs-dir docs --docs-only
cargo run -p cdf-cli --locked --features cli-artifacts --bin cdf-generate-cli-artifacts -- --docs-dir docs --docs-only --check
cargo run -p cdf-cli --locked --features cli-artifacts --bin cdf-generate-cli-artifacts -- --out-dir crates/cdf-cli/generated
cargo test -p cdf-cli --locked --features cli-artifacts cli_generated_artifacts_match_committed_snapshots
cargo clippy -p cdf-cli --all-targets --features cli-artifacts --locked -- -D warnings
```

A temporary copy of the docs was changed by appending a line to `commands/cdf.md`. The check failed with `stale commands/cdf.md` and printed the exact regeneration command. The complete `cdf-cli` feature-enabled suite executed 272 library tests: 271 passed and only the pre-regeneration CLI-artifact freshness test failed; after regenerating the artifacts, that test passed. No product-behavior test failed.

## What this supports

- Command syntax in `docs/commands/` is owned by clap definitions.
- Error reference fields in `docs/errors/README.md` are owned by the typed error catalog and its reference index.
- CI and `QUALITY.md` expose the same deterministic freshness command.
- Existing human/JSON error coverage continues to prove remediation, suggestions, not-supported exit 78, lower-layer kind mapping, and secret redaction.

## Limits

This evidence does not claim an external documentation-site build or release packaging. Those are outside WS6B/WS4D.
