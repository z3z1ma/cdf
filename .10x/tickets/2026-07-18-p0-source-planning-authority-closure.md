Status: open
Created: 2026-07-18
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-18-p0-post-iceberg-integration-stabilization.md
Depends-On: .10x/tickets/done/2026-07-18-p0-external-partition-authority.md, .10x/tickets/done/2026-07-18-p0-typed-compiled-source-identities.md

# P0: close source planning authority seams

## Scope

Finish the external-task migration so source extensions consume one closed planning authority instead of representation-sensitive helpers. Remove public post-construction authority replacement and silent inline-only mutation, preserve file-manifest summary evidence across external drain epochs, and provide one source-SDK planning entrypoint whose bounded/high-cardinality choice is explicit rather than adapter folklore.

## Non-goals

- Replacing source-owned partition semantics with generic inference.
- Materializing external task sets for diagnostics or summaries.
- Adding speculative source drivers.

## Acceptance Criteria

- Identity-bearing partition authority cannot be replaced through a public mutable setter after `ScanPlan` construction.
- Any partition transformation handles inline and external authority explicitly; it never silently no-ops.
- External drain epochs preserve typed file-manifest summary evidence without task-set enumeration.
- A new source adapter has one documented/compiler-enforced path for bounded inline versus external task planning.
- Extension-boundary conformance rejects representation-dependent adapters.

## Assumptions

- Record-backed: invalid inline/external states were removed, but mutation and representation-sensitive helpers can recreate the same failure class.
- Record-backed: source semantics remain source-owned; generic layers validate authority rather than manufacture it.

## Journal

Pending activation.

## Blockers

None. Typed observation binding is closed.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
