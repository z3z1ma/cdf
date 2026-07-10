Status: active
Created: 2026-07-08
Updated: 2026-07-08

# Data onramp source experience and CLI

## Purpose and scope

This specification governs P2 source identity, automatic normalization, key/disposition ergonomics, diagnostics, validation depth, `cdf add`, ad-hoc mode, and preview/run parity. It refines `.10x/specs/project-cli-observability-security.md` and `.10x/specs/types-contracts-normalization.md`.

## Behavior

The compiled resource id MUST be `<source>.<resource>` for new declarative project resources. `cdf validate` and `cdf inspect resources` MUST render compiled ids and report resource mapping patterns that match zero compiled ids.

Identifier normalization MUST run automatically at plan time. `namecase-v1`, destination sheet identifier rules, and `cdf:source_name` metadata populate the planned destination schema. Source-name overrides exist only to resolve ambiguity or intentional rename policy.

`append` MUST be the default write disposition and MUST require no key. `merge` MUST require an explicit merge identity. Plan-time merge-key errors MUST name the missing field and the two fixes: add `merge_key`, or stay append.

`cdf preview` MUST share resource resolution, transport/listing, decode, discovery, schema reconciliation, and normalization with `cdf run`, while remaining no-write and bounded. Preview MUST implement `preview-balanced-stratified-v1` from `.10x/decisions/preview-global-budget-and-payload-selection.md`: default global limits are 500 rendered rows, 64 MiB decoded input admitted to contract processing, and 64 admitted batches; deterministic `stratified-hash-v1` selects payload partitions; fair-share quotas prevent an early partition from consuming the batch budget; and other planned partitions are metadata-attested where exact authority exists and reported as payload-uninspected. Preview output MUST disclose limits, policy/selector versions, membership, partial inspection, and the distinction between decoded input and rendered output bytes.

`cdf validate --deep` MUST run the compiler front end without extraction or destination writes: resolve globs, count/list where safe, probe discovery, reconcile schema, normalize identifiers, check destination sheet compatibility, and emit source-specific diagnostics.

`cdf add <id> <url|path|dsn>` MUST probe, infer, pin, and write complete resource configuration. `--dry-run` prints the configuration. Interactive refinements MAY be added, but flag equivalents are required for automation. Key suggestions are advisory and only from evidence.

Ad-hoc mode MUST synthesize a real resource under `.cdf/adhoc/`, pin discovery, plan, package, commit, and gate through the normal pipeline. It MUST print the `cdf add` command that would make the resource permanent.

Diagnostics MUST name the command being run, the failing resource id, the file/source location where known, and a concrete remediation. Generic "fix the project" messages are not enough for P2 source-experience failures.

## Acceptance criteria

- S1 succeeds with `cdf add tlc.yellow <public parquet URL>` and `cdf run tlc.yellow` with zero hand-typed schema fields.
- S7 append emits no key warning, while merge without a key emits one precise plan-time error.
- `cdf validate --deep` catches schema/file/normalization/destination-front-end errors that would otherwise appear later in plan/run.
- Preview and run cannot diverge on file resolution, schema discovery/reconciliation, or identifier normalization for any P2 source archetype.

## Explicit exclusions

This spec does not define the final visual layout of P1 rendering, release packaging, or non-source CLI commands.
