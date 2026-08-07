Status: superseded
Created: 2026-08-03
Updated: 2026-08-06
Superseded-By: `.10x/decisions/state-backed-schema-authority.md`

# Project manifest path, compilation, and query policy

## Context

Project compilation facts are distributed across `cdf.toml`, declarative files, `cdf.lock`, typed
compiled plans, and runtime packages. `cdf sql` currently recompiles project resources merely to
mount package/checkpoint artifacts. CDF already owns a crash-safe multi-file publication journal
and stable-generation read protocol. The complete authority inventory is
`.10x/research/2026-08-03-project-compiler-authority-inventory.md`.

## Decision

The canonical generated project artifact is `.cdf/manifest.json`. It is local generated state and
the standard scaffold ignores `.cdf/`; `cdf.lock` remains committed expectation authority.

`cdf compile` is locked/offline: no source/destination network contact and no external mutation. It
publishes a manifest only when every required observation is already locked. `cdf compile
--refresh` is the explicit source-observation form and may update the manifest and `cdf.lock`, but
never destination data, state, packages, receipts, or checkpoints.

Publication reuses `.cdf/project-files.transaction.json`. Manifest-only compile installs the
manifest as its final target. A refresh changing manifest and lock installs the manifest first and
`cdf.lock` last. Read-only commands do not recover pending publication.

`cdf sql` locates the project/selected environment without compiling resource inputs, verifies the
manifest version/hash/environment/lock binding, and mounts exactly these initial SQLite tables:

- `manifest_project`;
- `manifest_inputs`;
- `manifest_resources`;
- `manifest_fields`;
- `manifest_semantics`;
- `manifest_lineage`;
- `manifest_diagnostics`.

It does not compile, refresh, publish, recover, or contact an adapter. The manifest retains only
the latest successful selected-environment compilation. Its typed JSON is the tooling artifact,
not runtime execution authority.

## Alternatives considered

### Root `cdf.manifest.json` committed with the project

Rejected. The artifact is selected-environment/compiler output, creates high-churn diffs, and sits
beside other local generated compiler/runtime state. Durable expectation belongs in `cdf.lock`.

### Generate the manifest implicitly during every project load

Rejected. Read-only commands would mutate, source contact could become surprising, and one failed
query could trigger publication recovery.

### Serve only virtual manifest tables without a serialized artifact

Rejected. Tools could not verify or diff compiler output independently, and queries would become a
second compilation path.

### Replace SQLite system SQL with DataFusion

Rejected. The current engine already supplies bounded read-only artifact queries. Replacing it adds
no D1 capability and would conflate project compilation with analytical runtime selection.

## Consequences

The scaffold, project models, CLI grammar, publication tests, and system-SQL catalog change. A
minimal project-location loader is required so manifest queries avoid declarative compilation.
Pending or stale manifests fail with explicit `cdf compile`/retry remediation. Future catalog
services consume the same verified artifact rather than reinterpreting project files.
