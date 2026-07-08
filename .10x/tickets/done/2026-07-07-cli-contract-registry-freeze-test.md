Status: done
Created: 2026-07-07
Updated: 2026-07-08
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
- 2026-07-08: Self-ratified `.10x/decisions/contract-freeze-lockfile-registry.md` before implementation. For this slice, `cdf.lock` is the project-local contract snapshot registry; the optional `contract` argument is interpreted as a resource id selector; omitted means all compiled resources. Freeze writes deterministic schema/policy/validation-program hashes into lock snapshots, and test compares current snapshots against frozen lock state.
- 2026-07-08: Implemented project-owned contract snapshot helpers plus CLI `contract freeze`/`contract test` wiring. Focused CLI tests cover project-free `contract show`, freeze/write/pass, missing lock, missing selected snapshot, and schema/program drift; focused project tests cover preserving existing dependency/destination lock data and field-level drift reporting.
- 2026-07-08: Parent review found and fixed a lockfile integration gap: generated lockfiles now compute full contract snapshots by default so `cdf diff schema` remains clean after `cdf contract freeze`. Closed with evidence `.10x/evidence/2026-07-08-cli-contract-registry-freeze-test.md` and review `.10x/reviews/2026-07-08-cli-contract-registry-freeze-test-review.md`.
