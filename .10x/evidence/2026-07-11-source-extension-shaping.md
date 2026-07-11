Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/decisions/source-driver-registry-and-resource-plan-boundary.md, .10x/specs/source-extension-runtime-contract.md, .10x/tickets/2026-07-11-p0-sx1-source-extension-boundary.md

# Source extension shaping evidence

## What was observed

Kernel/project execution can consume generic queryable resources, but declarative source/config/compiled plans enumerate REST/SQL/files; source-specific fields, discovery, capability and predicate logic, wrappers, and Postgres source ownership are spread across shared/source/destination crates.

## Procedure

Traced source declarations through compilation, discovery, runtime resolution, project wrappers, kernel capabilities, and crate imports; compared the edit set needed for a fourth source against the active extension invariant.

## What this supports

Preserving kernel resource traits while introducing an open source driver/config-schema/plan registry, extended scheduling capabilities, dependency-isolated source crates, and data-driven conformance.

## Limits

This is shaping evidence. SX1 must prove configuration migration, behavior/artifact compatibility, build isolation, and mock-source addition.
