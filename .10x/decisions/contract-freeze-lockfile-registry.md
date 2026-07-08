Status: active
Created: 2026-07-08
Updated: 2026-07-08

# Contract freeze lockfile registry

## Context

`.10x/tickets/done/2026-07-07-cli-contract-registry-freeze-test.md` requires `cdf contract freeze` and `cdf contract test` over a project-local contract snapshot/fixture registry. `.10x/specs/project-cli-observability-security.md` already says `cdf.lock` MUST lock contract snapshots, schema hashes, and related semantic hashes. `crates/cdf-project/src/lockfile.rs` already has `ContractSnapshot` fields for `contract_ref`, `schema_hash`, `policy_hash`, and `validation_program_hash`.

The implementation needs a concrete local registry shape before source edits. Creating a second registry directory now would duplicate `cdf.lock` authority and force future reconciliation.

## Decision

For CDF 1.0 local project semantics, `cdf.lock` is the project-local contract snapshot registry.

`cdf contract freeze [RESOURCE]` MUST compile the current project resources, compute a deterministic `ContractSnapshot` for each selected resource, and write the result into `cdf.lock`. A positional or `--contract` value in the existing parser is interpreted as a resource id selector for this slice. Omitting it means project scope: freeze every compiled resource.

A snapshot MUST include:

- the resource descriptor's `contract_ref`, when declared;
- the compiled schema hash from the resource descriptor;
- a policy hash computed from the trust-derived `ContractPolicy`;
- a validation-program hash computed from `compile_validation_program(policy, ObservedSchema::from_arrow(resource.schema()))`.

`cdf contract test [RESOURCE]` MUST load `cdf.lock`, recompute the current snapshots for the selected resource or all resources, and fail closed if the lockfile or selected frozen snapshot is missing. It MUST report pass/drift counts and drift details for changed snapshot fields. It MUST NOT silently pass missing registry state.

`contract show` remains a policy/preset rendering command and does not require a project.

## Alternatives considered

- Create `.cdf/contracts/` JSON files.
  - Rejected for this slice because `cdf.lock` is already the spec-backed semantic registry and already participates in diff/show workflows.
- Store the full serialized validation program in `cdf.lock`.
  - Rejected for this slice because `ContractSnapshot` is already hash-based. Full review artifacts can be added later without changing the freeze/test command meaning.
- Treat the optional `contract` argument as an arbitrary contract name rather than a resource selector.
  - Rejected because the current project model does not expose named contract files, while the ticket requires selected-resource or project-scope freeze/test behavior.

## Consequences

This makes freeze/test deterministic, project-local, and compatible with current lockfile machinery. The command proves schema/policy/program drift at the contract boundary but does not execute row fixtures or write quarantine artifacts. Future work may add full review artifacts or fixture directories while keeping `cdf.lock` as the summary registry.
