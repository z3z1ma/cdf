Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-cli-surface.md
Depends-On: .10x/specs/project-cli-observability-security.md, .10x/specs/package-lifecycle-determinism.md, .10x/specs/checkpoint-state-commit-gate.md

# Implement package GC retention planner

## Scope

Implement retention-safe `cdf package gc [DIR]` behavior tied to checkpoint history and package manifest reachability.

Owns:

- `crates/cdf-cli/src/package_command.rs` GC command and focused tests.
- Lower `cdf-package`/`cdf-project` retention planner APIs needed to classify packages without ad hoc CLI traversal semantics.
- Checkpoint-history reachability checks.

## Acceptance criteria

- `cdf package gc` identifies retained, collectible, missing, corrupt, and protected package artifacts from package manifests and checkpoint history.
- No package required by a committed checkpoint, replay proof, receipt, or retention tombstone is removed.
- Destructive behavior is explicit and ratified before implementation; default behavior MUST NOT silently delete proof artifacts.
- JSON output is stable and includes package path, package hash when readable, retention reason, and planned action.
- Corrupt or partially written packages fail closed unless the ratified command contract says they are collectible.

## Evidence expectations

Run focused CLI package GC tests, retention/reachability tests, corrupt-package tests, package verify/list regression tests, fmt/clippy/check/diff checks, and appropriate security/complexity scans.

## Explicit exclusions

No package archive behavior, no external object-store deletion, no retention policy UI, no checkpoint deletion, and no package-signing workflow.

## Blockers

None. If destructive flag semantics or tombstone layout are missing, self-ratify before source edits.

## Progress and notes

- 2026-07-07: Split from `.10x/tickets/done/2026-07-07-cli-remaining-command-planners.md`. Current `package gc` returns not-supported because retention-safe proof checks are not exposed.
