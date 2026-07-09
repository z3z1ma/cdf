Status: superseded
Created: 2026-07-08
Updated: 2026-07-08

# Preview one-batch sampling semantics

Superseded-By: `.10x/decisions/data-onramp-source-identity-preview-disposition.md`

## Context

`cdf preview` is required by `.10x/specs/project-cli-observability-security.md` to inspect one batch without writing package, destination, checkpoint, or run-ledger artifacts. `.10x/tickets/done/2026-07-07-cli-preview-resource-breadth.md` extends preview beyond the initial single local-file slice to REST, table-backed SQL, Arrow IPC, and multi-file file resources.

The active resource model makes rows a boundary detail and batches the runtime unit. For file resources, a glob can match multiple files, while `cdf preview` remains intentionally one-batch-only and must not create a package just to apply engine residual operators.

## Decision

`cdf preview` is a direct-stream one-batch sample. It opens the first planned partition, reads the first emitted batch, reports that batch, and performs no package/destination/checkpoint/run-ledger writes.

For file resources whose glob matches multiple files, preview MAY sample the first deterministic path-sorted file and return the first batch from that file. This is preview-only behavior and MUST NOT change live run/package semantics, which still fail closed until multi-file package scan semantics are explicitly implemented.

Preview MUST fail closed when the requested scan would require engine residual work that direct stream opening cannot perform without a package. That includes unsupported or inexact predicates, projection that is not pushed into the resource, and limits that are not pushed into the resource. Supported table-backed SQL resources may preview filtered/projected/limited batches because the Postgres partition metadata carries those operations to the source runtime. REST preview may use exact cursor pushdown when declared exact; inexact REST filters remain a fail-closed preview request.

## Alternatives considered

- Keep multi-file preview fail-closed.
  - Rejected because the command explicitly promises one-batch inspection, not a full resource scan, and deterministic first-batch sampling is useful for local authoring without committing to multi-file run semantics.
- Run the engine pipeline in preview to apply residual filters/projection/limit.
  - Rejected because the current CLI preview contract is no-write direct inspection. A package-free engine sampling mode can be ratified later, but this ticket should not invent it.
- Allow preview to return supersets for inexact predicates.
  - Rejected because preview output would look filtered while silently ignoring the residual predicate.

## Consequences

Preview output is honest and cheap: it shows one concrete batch and no write effects. Users who need filter/projection/limit preview must use resources that push those operations exactly, or omit those options. Future package-free engine preview can supersede this decision if it preserves no-write behavior and records stronger semantics.
