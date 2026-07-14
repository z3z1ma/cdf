Status: active
Created: 2026-07-05
Updated: 2026-07-13

# Project format, CLI, observability, and security

## Purpose and scope

This specification governs the user-facing project format, lockfile, CLI command set, error taxonomy, observability surfaces, secrets, and security boundaries. It derives from book Chapters 14, 15, 16, 17, 18, and 20 and decisions D-17, D-18, D-19, D-20, D-22, and D-23.

## Project format

`cdf.toml` MUST define project metadata, default environment, normalizer, environments, Python interpreter, defaults, and resource source mappings. Environments MUST overlay inherited settings. Secrets MUST appear only as `secret://provider/key` URIs.

Environment destination URIs MUST use destination-specific schemes. `duckdb://<path>` names a local DuckDB database path. `parquet://<root>` names a filesystem Parquet destination root/prefix, not a single file; commits MAY create multiple Parquet files, manifests, pointers, and receipt-supporting objects below the root.

Environment destination policy MAY declare destination-specific explicit semantic knobs. The first ratified Postgres destination policy shape is:

```toml
[environments.<name>.destination_policy.postgres]
merge_dedup = "fail"
```

`merge_dedup` applies only to `merge` writes when an incoming package/stage contains duplicate merge keys. `fail` MUST abort before target-table mutation when duplicates are detected.

`cdf.lock` MUST lock semantics, not just versions: dependency tuple, resource capability-sheet hashes, destination sheets including type mappings, contract snapshots, schema hashes, and normalizer version.

`cdf validate --env <env>` MUST check schema validity and secret resolvability without printing secret values.

## CLI

The CLI MUST be headless, scheduler-friendly, and support `--json` for commands where structured output is meaningful. Every architectural noun SHOULD have an inspect command.

The required command surface includes `init`, `validate`, `plan`, `explain`, `run`, `preview`, `sql`, `inspect`, `diff schema`, `contract freeze/show/test`, `state show/history/rewind/recover`, `resume`, `replay package`, `backfill`, `package ls/gc/verify`, `doctor`, and `status`. State stores initialize only the current schema automatically; no pre-production migration command ships. `package archive` is fast-follow.

`cdf plan` MUST show what will be fetched, pushdown fidelity, DDL preview, delivery guarantee, and state advancement before bytes move.

`cdf preview` MUST inspect bounded source data without writing package, destination, checkpoint, or run-ledger artifacts. For P2 source-onramp behavior, preview MUST share resource resolution, file listing, decode, discovery, schema reconciliation, and normalization with `cdf run` as governed by `.10x/specs/data-onramp-source-experience-cli.md` and `.10x/decisions/data-onramp-source-identity-preview-disposition.md`. The earlier preview-only first-file exception for multi-file globs is superseded.

`cdf run --loop` MAY exist only for local development and MUST NOT make the kernel a scheduler.

`cdf run` MUST route supported resource/destination/disposition combinations through the general run spine defined by `.10x/specs/run-orchestration-ledger.md`. It MUST mint a run id when one is not supplied and MUST fail closed on caller-supplied run-id collision.

`cdf resume` MUST drain interrupted work according to the run spine crash matrix. After package finalization, resume MUST NOT contact the source.

`cdf replay package <pkg> --to <dest>` MUST create a new run, use package replay inputs, and record duplicate receipts as observable facts.

`cdf replay package <pkg> --to postgres://...` MUST require explicit `--target` and `--merge-dedup` inputs. The supplied target MUST match the package destination-commit target. Replay MUST NOT infer target, disposition, merge keys, or merge-dedup policy from destination introspection.

`cdf inspect run <id>` MUST assemble plan, verdict summaries, receipts, transitions, package/checkpoint pointers, duplicate status, and recovery guidance. It MUST show missing artifacts explicitly and MUST redact secrets.

## Errors and retries

All tiers and crates MUST use one taxonomy: `Transient`, `RateLimited`, `Auth`, `Contract`, `Data`, `Destination`, and `Internal`.

Retries MUST occur at the smallest safe unit under a run-level retry budget. Contract verdicts are not retried. Blind retries of malformed data are forbidden.

## Observability

cdf's primary observability surface MUST be its own queryable artifacts: ledger, run ledger, packages, receipts, and mirrors. `cdf sql` MUST query system history where practical.

`tracing` MUST include run, resource, partition, and package IDs. OTLP export MAY be feature-gated.

`cdf doctor` MUST check environment health, secret resolvability, Python interpreter/free-threaded status, DuckDB ICU, and ledger/destination drift where applicable.

`cdf status` MUST evaluate freshness SLOs and exit nonzero on serving-resource breach.

## Security

Serialized artifacts MUST contain secret references only. Runtime resolution MUST use a `SecretProvider` trait. Resolved secrets MUST use zeroizing wrappers and registered redaction so traces, errors, plans, and panic formatting are scrubbed by construction.

Trust boundaries MUST match authoring tiers: Tier 0/1 trusted operator code; Tier 2 trusted but instrumented Python; Tier 3 untrusted WASM with capability-scoped WASI and host-mediated HTTP/secrets/logs; Tier 4 supervised OS subprocess.

The project file MAY declare egress allowlists per source. `cdf-http` and the WASM host MUST enforce them when present.

Supply-chain gates SHOULD include `cargo deny`, `cargo vet`, committed lockfiles, reproducible checked binaries, and dependency tuple pinning per cdf minor.

## Acceptance criteria

- `cdf.toml` and `cdf.lock` parse into typed models and reject secret values where only references are allowed.
- CLI commands provide stable JSON output where required and meaningful exit codes.
- Redaction tests prove a resolved secret cannot appear in traces, error messages, plan output, or package traces.
- `doctor` detects at least missing secrets, Python interpreter issues, DuckDB ICU status, and ledger/mirror drift when fixtures support them.

## Explicit exclusions

This spec does not define package file internals, destination commit internals, or resource capability truth-testing beyond CLI exposure.
