Status: active
Created: 2026-08-03
Updated: 2026-08-03

# Net-new, customer-zero compatibility policy

CDF is net-new and the project itself is customer zero. There are no external production artifacts,
configurations, clients, or users whose historical behavior must be preserved.

## Default rule

When a contract, artifact, option, API, schema, or internal representation changes:

- replace it directly with the correct current design;
- bump every affected artifact/schema/protocol identity coherently;
- update all producers, consumers, fixtures, examples, and goldens in the same tranche;
- fail stale artifacts/configuration with direct regenerate/recreate remediation;
- delete superseded fields and paths.

Do not add backward-compatible readers, migrations, aliases, deprecated spellings, dual writes,
fallback branches, transitional fields, feature flags for the old shape, or speculative extension
points. These are technical debt without a customer requirement.

## What this does not relax

This policy does not weaken correctness, durability, crash recovery, type safety, error provenance,
security, or current-schema forward recovery. A current publication/checkpoint interrupted by a
crash still requires truthful recovery. The policy removes support for superseded historical
schemas; it does not permit corruption or partial transitions.

## Exception rule

An exception requires new evidence of an actual external compatibility obligation and explicit user
ratification of its exact lifetime and removal condition. Familiarity, hypothetical future users,
or avoiding fixture updates is not evidence.
