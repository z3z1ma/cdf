Status: active
Created: 2026-07-10
Updated: 2026-07-10

# `cdf add` DSN secret persistence

## Context

S4 requires `cdf add <id> postgres://.../<table>` to probe and write a runnable resource in one command. Declarative SQL correctly permits only `secret://` connection references, so writing the supplied DSN into TOML would violate the secret boundary while merely inventing an unset environment reference would make the happy path nonfunctional.

## Decision

For a direct database DSN, `cdf add` stores the exact DSN in a project-private `.cdf/secrets/sources/<source>.dsn` file created with owner-only permissions, writes only its `secret://file/...` reference into resource TOML, and redacts credentials from every report/error. `--dry-run` probes using process-local authority but writes neither secret nor project artifacts and reports the proposed secret reference. Existing secret references remain references and are never copied.

The table is the final non-empty path segment; database remains the preceding path. Cursor candidates from catalog metadata are suggestions only and no cursor/key is silently selected.

## Alternatives considered

Writing inline DSNs was rejected as secret leakage. Inventing an unset environment variable was rejected because the generated resource would not run. Requiring a second credentials flag was rejected for the normative S4 one-command path, though future explicit provider selection may override private-file persistence.

## Consequences

The project-private file becomes operational secret state and MUST be gitignored, permission-checked, redacted, and included in doctor checks by reference only. Distributed deployments should use an explicit external secret reference rather than copying this local convenience file.
