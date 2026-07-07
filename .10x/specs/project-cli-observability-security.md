Status: active
Created: 2026-07-05
Updated: 2026-07-05

# Project format, CLI, observability, and security

## Purpose and scope

This specification governs the user-facing project format, lockfile, CLI command set, error taxonomy, observability surfaces, secrets, and security boundaries. It derives from book Chapters 14, 15, 16, 17, 18, and 20 and decisions D-17, D-18, D-19, D-20, D-22, and D-23.

## Project format

`cdf.toml` MUST define project metadata, default environment, normalizer, environments, Python interpreter, defaults, and resource source mappings. Environments MUST overlay inherited settings. Secrets MUST appear only as `secret://provider/key` URIs.

`cdf.lock` MUST lock semantics, not just versions: dependency tuple, resource capability-sheet hashes, destination sheets including type mappings, contract snapshots, schema hashes, and normalizer version.

`cdf validate --env <env>` MUST check schema validity and secret resolvability without printing secret values.

## CLI

The CLI MUST be headless, scheduler-friendly, and support `--json` for commands where structured output is meaningful. Every architectural noun SHOULD have an inspect command.

The required command surface includes `init`, `validate`, `plan`, `explain`, `run`, `preview`, `sql`, `inspect`, `diff schema`, `contract freeze/show/test`, `state show/history/rewind/migrate/recover`, `resume`, `replay package`, `backfill`, `package ls/gc/verify`, `doctor`, and `status`. `package archive` is fast-follow.

`cdf plan` MUST show what will be fetched, pushdown fidelity, DDL preview, delivery guarantee, and state advancement before bytes move.

`cdf preview` MUST inspect one batch without writing.

`cdf run --loop` MAY exist only for local development and MUST NOT make the kernel a scheduler.

## Errors and retries

All tiers and crates MUST use one taxonomy: `Transient`, `RateLimited`, `Auth`, `Contract`, `Data`, `Destination`, and `Internal`.

Retries MUST occur at the smallest safe unit under a run-level retry budget. Contract verdicts are not retried. Blind retries of malformed data are forbidden.

## Observability

cdf's primary observability surface MUST be its own queryable artifacts: ledger, packages, receipts, and mirrors. `cdf sql` MUST query system history where practical.

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

