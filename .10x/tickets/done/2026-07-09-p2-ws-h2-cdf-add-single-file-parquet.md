Status: done
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/done/2026-07-08-p2-ws-h-scaffolding-id-model-two-minute-path.md
Depends-On: .10x/tickets/done/2026-07-09-p2-ws-a3-local-parquet-discover-autopin.md, .10x/tickets/done/2026-07-09-p2-ws-e2-http-file-runtime-and-discovery.md, .10x/specs/data-onramp-source-experience-cli.md

# P2 WS-H2 cdf add single-file Parquet

## Scope

Implement `cdf add <id> <url-or-path>` for the S1 single-file Parquet path. The command probes, infers, pins, writes resource configuration, and prints the file it wrote; `--dry-run` prints the proposed TOML without mutation.

## Acceptance criteria

- `cdf add tlc.yellow <local-or-https-parquet>` creates or prints a project resource whose compiled id is `tlc.yellow`, source kind is files, disposition is append, schema source is discovered/pinned, and no key is invented.
- The command writes a deterministic schema snapshot under `.cdf/schemas/` and resource TOML/config in the current project when not dry-run.
- `--dry-run` does not write `.cdf/`, project config, package, destination, checkpoint, or state.
- The rendered output names the written config and next command `cdf run tlc.yellow`.
- Secret values and signed URLs are redacted in debug/errors/output.

## Evidence expectations

CLI tests for local Parquet immediately; HTTPS fixture tests once E2 lands; dry-run no-write assertions; generated TOML parse/validate; no fake key assertions; normal quality gates.

## Explicit exclusions

This ticket does not implement Postgres `cdf add`, REST `cdf add`, interactive refinements, ad-hoc mode, multi-file glob scaffolding, or docs quickstart rewrite.

## Progress and notes

- 2026-07-09: Opened as the first `cdf add` child. It intentionally depends on the E2 production HTTPS path for the full S1 URL case.
- 2026-07-09: E2 closed in `.10x/tickets/done/2026-07-09-p2-ws-e2-http-file-runtime-and-discovery.md`, so the HTTPS portion is unblocked. This ticket is executable for local and deterministic HTTPS single-file Parquet.
- 2026-07-09: Worker H2 added parser/dispatch wiring, `crates/cdf-cli/src/add_command.rs`, and focused CLI tests for local add, dry-run no-write, deterministic loopback HTTP ranged Parquet add, and signed URL query redaction. The implementation is intentionally single-file Parquet only; it writes `resources/<source>.toml`, appends `[resources."<source>.<resource>"]` to `cdf.toml`, writes the schema snapshot, and generates or updates `cdf.lock` with the pinned discovered snapshot when not `--dry-run`.
- 2026-07-09: `cargo fmt -p cdf-cli` completed. `git diff --check` completed with no whitespace errors. Early isolated verification was blocked by concurrent D4 edits and local disk pressure from bundled DuckDB rebuilds; those blockers cleared after the integrated H2/D4/B5 batch was assembled.
- 2026-07-09: Closed after integrated verification. `cdf add <id> <path-or-https-parquet>` now supports the scoped single-file Parquet happy path, dry-run no-write behavior, deterministic schema snapshot and lockfile writes, generated resource config, no invented keys, signed URL rejection/redaction, generated CLI help/completion/man artifact refresh, and next-command output. Evidence: `.10x/evidence/2026-07-09-p2-ws-h2-cdf-add-single-file-parquet.md`. Review: `.10x/reviews/2026-07-09-p2-ws-h2-cdf-add-single-file-parquet-review.md`.

## Blockers

None.
