Status: active
Created: 2026-08-03
Updated: 2026-08-03

# State uses one current schema and verified effect-package recovery

## Context

`.10x/decisions/superseded/state-current-schema-package-receipt-recovery.md` correctly removed
pre-production state migrations while retaining explicit verified package/receipt recovery. Its
command shape also carried a caller-selected Postgres merge-dedup policy. Package-native keyed
effects now make final winner selection and delete application immutable package/commit-plan
authority, so recovery must not accept a semantic override.

## Decision

Each SQLite state component MUST initialize its current schema automatically only when its
component tables and schema-version record are both absent. An existing component MUST open only
when its recorded version is exactly current and every required current table exists. Noncurrent,
unversioned, or incomplete layouts fail closed. CDF retains the version registry as a future
migration seam but ships no predecessor reader, upgrade function, historical fixture, or
`cdf state migrate` command while there is no supported installed predecessor.

`cdf state recover` remains package-receipt recovery, not destination-mirror scraping:

```text
cdf state recover --package <package-dir> --to <destination-uri> [--receipt <receipt-id>] [--target <target>]
```

Recovery MUST consume verified current package replay inputs and one uniquely selected durable
receipt, verify the receipt through the destination protocol, and advance state only through
`CheckpointStore::commit` or exact reuse of an already-committed head. It MUST NOT write
destination rows or reconstruct evidence it does not possess.

When `--target` is required by the destination command surface, it MUST equal the package's
recorded target. Recovery MUST consume the package's recorded disposition, keys, finalized keyed
effects, reduction evidence, and delete-application policy. It MUST NOT accept merge-dedup or
delete-policy overrides, contact the source, re-run effect reduction, or infer semantics from the
live destination.

## Alternatives considered

- Retain `--merge-dedup fail` as a required confirmation. Rejected because verified package
  identity already proves the final effect set; restating a policy adds no authority.
- Permit recovery to change delete application. Rejected because it would authorize a different
  destination mutation than the recorded receipt/checkpoint transition.
- Retain `cdf state migrate` until 1.0. Rejected because there is no supported installed base.
- Silently stamp an unversioned database. Rejected because old/corrupt layouts could be
  misinterpreted as current.
- Delete recovery together with migration. Rejected because current durable package/receipt
  recovery is part of the crash contract, not compatibility machinery.

## Consequences

This decision supersedes
`.10x/decisions/superseded/state-current-schema-package-receipt-recovery.md`. Its strict current
state-schema and receipt-gated recovery rules remain; only caller-selected package semantics are
removed. Current code, CLI docs, and fixtures will be replaced without ignored compatibility
flags when the keyed-effect artifact transition executes.
