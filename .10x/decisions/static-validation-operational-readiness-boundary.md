Status: active
Created: 2026-08-04
Updated: 2026-08-04

# Static validation and operational readiness boundary

## Context

`cdf validate` currently resolves secrets and its `--deep` form enumerates source data, probes
discovery, and checks destination compatibility. This makes a supposedly basic project check depend
on credentials, network access, local data presence, and unrelated resources. It also overlaps
plan, discover, and doctor, so failures routinely recommend a command that cannot repair them.

The user confirmed that validate is only a thin static project check. Environment-variable and
secret availability and other operational checks belong to a scope-able doctor command.

## Decision

`cdf validate` is strictly offline, deterministic, no-write validation of authored project
structure and locally present generated authority. It never resolves secret references, checks
environment-variable or secret-file existence, enumerates source data, invokes a driver discovery
or health operation, opens destination/state services, or performs network I/O. Missing lock or
compiled authority is reported as local status and is not itself a static validation failure.

`cdf validate --deep` is deleted. Its useful bounded-observation behavior moves to the command that
owns the user's intent:

- configured-source catalog inspection and resource schema observation: `cdf discover`;
- execution planning and destination compatibility: `cdf plan`;
- bounded row inspection: `cdf preview`;
- secret, environment, source, destination, runtime, and ledger readiness: scoped `cdf doctor`.

The existing 4,096-record/8-MiB JSON-family observation bounds, typed quarantinable warnings,
redaction requirements, and Tier-0 type allowances remain active when those observation paths use
them. Only their ownership by `validate --deep` is superseded.

## Alternatives Considered

- Keep shallow/deep validate: rejected because one command would retain two incompatible effect
  classes and scripts could not know whether validation was environmental or structural.
- Let validate resolve secrets but avoid network calls: rejected because project validity would
  still vary by shell and deployment host, recreating the one-resource credential problem.
- Make doctor a second all-project gate: rejected because it would preserve the same coupling
  under a different name. Operational checks must name their scope.

## Consequences

Validation becomes safe in editors, CI syntax gates, and credential-free environments. It cannot
claim that a resource is runnable; its report must say exactly which static facts were checked and
which operational facts were not. Doctor, discover, plan, and preview carry the additional cost and
failure authority only when explicitly invoked for a relevant scope.

This decision supersedes only the `cdf validate --deep` and validate-time secret-resolvability
clauses in `.10x/decisions/deep-validation-sampling-warnings-and-type-allowances.md`,
`.10x/specs/data-onramp-source-experience-cli.md`, and
`.10x/specs/project-cli-observability-security.md`; their remaining contracts stay active.
