Status: active
Created: 2026-07-07
Updated: 2026-07-07

# Project-Run Postgres Destination Inputs

## Context

`.10x/tickets/done/2026-07-07-general-run-postgres-destination.md` was blocked because the general project-run request could not safely construct `PostgresLoadPlanInput` without knowing target identity, column mappings, merge keys, dedup policy, and existing-table policy.

The user ratified the recommended project-run slice on 2026-07-07 and clarified that this is not a rejection of destination introspection as an overall product ambition. The decision here is scoped only to constructing the first general-run Postgres destination request.

## Decision

For the first general project-run Postgres destination slice, Postgres-specific destination inputs MUST be explicit or mechanically derived from already-ratified package/resource metadata:

- `PostgresTarget`, existing-table policy, and merge dedup policy MUST come from explicit destination/run configuration.
- Column mappings MUST derive from the package schema using the existing Postgres identifier and type-mapping rules.
- Merge keys MUST derive from the resource descriptor only when merge keys are explicitly present there.
- The project runtime MUST NOT infer table policy, target schema/table, dedup policy, or merge keys from destination introspection in this slice.

Destination introspection remains allowed as a future product capability for planning, drift detection, migration previews, and safety checks. This decision only forbids using introspection as an implicit source of missing semantics for the initial general-run Postgres commit path.

## Alternatives considered

Infer missing Postgres load inputs from the live destination.

Rejected for this slice. It would make project-run behavior depend on mutable destination state and could silently invent write policy.

Hard-code Postgres defaults in the runtime.

Rejected. Existing-table and merge policies are semantic choices and must be explicit or derived from ratified resource metadata.

Reject all Postgres project-run support until a larger introspection system exists.

Rejected. Explicit inputs are sufficient for the first deterministic run-spine slice.

## Consequences

`.10x/tickets/done/2026-07-07-general-run-postgres-destination.md` was unblocked by this decision and implemented by adding an explicit Postgres destination request shape and mapping package/resource metadata through existing destination rules.

Future destination introspection work needs its own focused spec/ticket and is not precluded by this decision.
