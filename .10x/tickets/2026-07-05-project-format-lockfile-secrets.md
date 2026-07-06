Status: open
Created: 2026-07-05
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: .10x/tickets/done/2026-07-05-kernel-core-types.md, .10x/tickets/2026-07-05-declarative-resources.md

# Implement project format, lockfile, and secrets

## Scope

Implement `firn-project`: `firn.toml` parsing, environment overlays, resource source resolution, retention policy model, Python interpreter configuration, secret URI model, secret providers for env/file/OS keychain where feasible, semantic lockfile generation/diffing, and project validation APIs. Owns `crates/firn-project/**`.

## Acceptance criteria

- Book `firn.toml` shape parses into typed configuration.
- Environment overlays inherit unspecified settings.
- Secret values are rejected in serialized artifacts where references are required.
- `firn.lock` captures dependency tuple, resource capability hashes, destination sheets, type mappings, contract snapshots, schema hashes, and normalizer version.
- Validation can check secret resolvability without printing values.

## Evidence expectations

Record config parser tests, overlay tests, lockfile snapshot/diff tests, secret redaction tests, and validation tests.

## Explicit exclusions

No CLI rendering; no destination-driver implementation.

## Progress and notes

- 2026-07-05: Opened from book and specs.

## Blockers

None.

