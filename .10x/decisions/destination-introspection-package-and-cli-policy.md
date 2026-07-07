Status: active
Created: 2026-07-07
Updated: 2026-07-07

# Destination introspection, package scope, and CLI destination policy

## Context

CLI run and replay wiring needed ratified semantics for filesystem Parquet destination URI syntax, Postgres destination policy inputs, and the relationship between runs and packages.

The previous Postgres-specific decision `.10x/decisions/superseded/project-run-postgres-destination-inputs.md` required an explicit existing-table policy for the first project-run Postgres slice. Follow-up user review clarified that target introspection is not an optional future feature or Postgres-only behavior: destination introspection is standard CDF behavior wherever applicable. The product boundary is that introspection provides safety, planning, drift detection, verification, and clearer failure modes; it must not infer missing write semantics.

## Decision

Destination introspection is standard behavior across CDF wherever a destination can support it. Introspection SHOULD be used for safety checks, drift detection, migration/load planning, receipt verification, and actionable failure messages. Introspection MUST NOT infer missing write semantics such as target identity, write disposition, merge keys, dedup policy, resource identity, or checkpoint semantics.

`parquet://<root>` is the CLI/project URI spelling for a local filesystem Parquet destination root or prefix. The root is a destination object tree, not a single Parquet file. A package or commit may write multiple Parquet data files, manifests, replacement pointers, and receipt-supporting objects below that root. Relative roots resolve under the selected project root; absolute filesystem roots are allowed. Empty roots and nested non-filesystem URI values are invalid.

A package represents one resource transition. A run is an orchestration envelope that may contain one or many resource transitions and therefore one or many packages. For example, a GitHub run covering issues, pull requests, and commits records separate resource-scoped packages tied together by one run id and run ledger.

Postgres destination policy keeps the nested project-config shape even though the first ratified option is a single key:

```toml
[environments.<name>.destination_policy.postgres]
merge_dedup = "fail"
```

`merge_dedup` applies only to `merge` writes when the incoming package/stage contains multiple rows with the same merge key. `fail` means CDF MUST detect duplicate merge keys inside the commit unit and abort before mutating the target table. `first` and `last` remain valid lower-layer policies but are not the default project policy. The first CLI/project implementation SHOULD accept `fail` and MAY support `first`/`last` only when the implementation can explain the package-order basis in output and tests.

Postgres replay remains explicit because package artifacts do not own every destination-run semantic needed to construct a Postgres load plan. The CLI shape is:

```bash
cdf replay package <pkg> \
  --to postgres://secret://... \
  --target schema.table \
  --merge-dedup fail
```

The supplied target MUST match the package's recorded destination-commit target. Replay MUST NOT infer target, disposition, merge keys, or merge-dedup policy from destination introspection. Destination introspection MAY still be used during replay for safety checks, drift detection, planning, and verification.

## Alternatives considered

Require a `schema_mode = "introspect"` or `existing_table = "managed"` project policy.

Rejected. The user clarified that destination introspection is standard behavior wherever applicable, so requiring an option for the normal mode creates a misleading semantic knob.

Infer missing Postgres policy from the live destination.

Rejected. Destination state is mutable operational evidence, not a source of write semantics. It may prove a target is compatible or incompatible, but it must not decide deduplication, target, disposition, or key semantics.

Use `parquet://<file>` as a single-file URI.

Rejected. Parquet destination commits may produce multiple files and manifests. The URI must name a root/prefix.

Treat one multi-resource source extraction as one package.

Rejected for the current package model. Resource-scoped packages keep package identity, replay, receipts, and checkpoint transitions small and auditable, while the run ledger provides the multi-resource envelope.

## Consequences

`.10x/decisions/superseded/project-run-postgres-destination-inputs.md` is superseded. Any active implementation work should use this decision for CLI/project destination policy.

CLI `run` Parquet support is unblocked at the decision level by `parquet://<root>`.

CLI Postgres run support is unblocked at the decision level by `destination_policy.postgres.merge_dedup = "fail"` plus the already explicit run target. It still needs implementation to parse and carry the policy into `ProjectRunDestination::Postgres`.

CLI Postgres package replay is unblocked at the decision level by explicit `--target` and `--merge-dedup` flags. It still needs implementation to route through the Postgres artifact replay API.

Future destination policy keys can be added under the same nested destination-policy shape when they are independently ratified, such as lock/statement timeouts, DDL permission mode, drift strictness, or load tuning.
