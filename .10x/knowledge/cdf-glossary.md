Status: active
Created: 2026-07-05
Updated: 2026-07-05

# cdf glossary

`cdf` is the CLI binary and crate prefix; cdf in prose is the project.

Resource: the smallest stateful extraction unit. It declares schema, keys, cursor, state scope, disposition, contract, trust, and capabilities, and produces Arrow record batches.

Source: a configuration and discovery bundle over resources. It is not the unit of runtime state.

Batch: Arrow payload plus resource, partition, schema hash, rows, bytes, source position, watermarks, stats, and optional CDC operation information.

Scan plan: a negotiated read plan containing projection, classified filters, limits, partitioning, ordering, estimates, and pushdown fidelity.

Contract: a policy compiled into a validation program with a total verdict lattice.

Package: hash-addressed evidence of one attempted state transition. Package data is canonical Arrow IPC; stats, quarantine, and lineage are Parquet; manifests and receipts are canonical JSON.

Receipt: a destination's durable, independently verifiable acknowledgment that a package or segment set was committed.

Checkpoint: a typed, append-only state transition committed only after receipt verification.

CDF line: the commit boundary enforced by `CheckpointStore::commit`; a source cursor may advance only after all data represented by the cursor is durably committed and the destination receipt is recorded.

Scope: a sub-resource state key such as a partition, window, file, stream, schema-contract, or destination-load scope.

Sheet: a declared and lockfile-snapshotted capability table for a resource or destination.

Trust level: planner preset expanding operator intent into contract, validation, promotion/demotion, and retention policy. Values are `experimental`, `governed`, `financial`, and `serving`.

Disposition: destination write semantics. MVP dispositions are `append`, `replace`, and `merge`; `cdc_apply` arrives with log CDC.

Ice: committed state in the checkpoint ledger.

Snowfall: raw extraction batches before validation and packaging.

CDF: a load package: compacted evidence that can still melt if it never commits.

