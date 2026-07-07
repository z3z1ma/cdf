Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-cli-surface.md
Depends-On: .10x/specs/project-cli-observability-security.md, .10x/specs/types-contracts-normalization.md

# Implement contract registry freeze/test

## Scope

Implement `cdf contract freeze` and `cdf contract test` over a project-local contract snapshot/fixture registry.

Owns:

- `crates/cdf-cli/src/contract_command.rs` and focused tests.
- Lower `cdf-contract`/`cdf-project` registry helpers needed to write/read snapshots without ad hoc CLI-owned semantics.
- Drift fixture execution and fail-closed behavior for missing registry state.

## Acceptance criteria

- `cdf contract freeze` writes a deterministic contract snapshot for the selected resource or project scope.
- `cdf contract test` compares current observed schemas/fixtures against frozen snapshots and reports pass/drift/quarantine-relevant verdict summaries.
- Missing registry state fails closed for `test` with recovery guidance rather than silently passing.
- `contract show` remains compatible and continues to expose policy presets.
- JSON output is stable and contains snapshot identity, resource ids, verdict counts, and drift details without leaking secrets.

## Evidence expectations

Run focused CLI contract tests, deterministic snapshot rewrite tests, drift fixture tests, missing-registry failure tests, relevant `cdf-contract` tests, fmt/clippy/check/diff checks, and applicable security/duplication/complexity checks for touched modules.

## Explicit exclusions

No row-level quarantine routing unless split into the contract-governance parent, no trust-ring promotion/demotion ledger events, no external registry, and no package signing.

## Blockers

None. If the registry file layout is not already specified enough for implementation, self-ratify the smallest local layout before source edits.

## Progress and notes

- 2026-07-07: Split from `.10x/tickets/done/2026-07-07-cli-remaining-command-planners.md`. Current CLI supports `contract show`; freeze/test return not-supported because registry/snapshot helpers are not exposed yet.
