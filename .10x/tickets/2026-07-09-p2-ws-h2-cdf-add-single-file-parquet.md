Status: open
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-08-p2-ws-h-scaffolding-id-model-two-minute-path.md
Depends-On: .10x/tickets/done/2026-07-09-p2-ws-a3-local-parquet-discover-autopin.md, .10x/tickets/2026-07-09-p2-ws-e2-http-file-runtime-and-discovery.md, .10x/specs/data-onramp-source-experience-cli.md

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

## Blockers

Blocked on `.10x/tickets/2026-07-09-p2-ws-e2-http-file-runtime-and-discovery.md` for the HTTPS portion. Local Parquet scaffolding can be prepared first if write scopes stay isolated.
