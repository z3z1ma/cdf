Status: done
Created: 2026-07-07
Updated: 2026-07-08
Parent: .10x/tickets/done/2026-07-05-cli-surface.md
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
- 2026-07-08: Implemented dry-run `cdf package gc [DIR]` planning. The planner reads package manifests, verifies package identity/archive metadata through `cdf-package`, protects committed-checkpoint package hashes via a read-only SQLite checkpoint query, protects package receipts and archived tombstones, reports missing committed-checkpoint artifacts, and fail-closed retains corrupt/partial packages. No deletion flag or destructive behavior was added.
- 2026-07-08: Added focused tests for GC dry-run planning, checkpoint-history protection, missing committed package classification, corrupt/partial fail-closed classification, tombstone protection, and package command regressions. Verification run in this workspace: `cargo fmt --all -- --check`; `cargo test -p cdf-cli package_ --locked`; `cargo test -p cdf-state-sqlite sqlite_committed_package_hashes_reports_only_committed_history --locked`; `cargo check -p cdf-cli -p cdf-state-sqlite --all-targets --locked`; `cargo clippy -p cdf-cli -p cdf-state-sqlite --all-targets --locked -- -D warnings`; `git diff --check`; `jscpd --reporters console --exit-code 0 --min-lines 6 --min-tokens 60 crates/cdf-cli/src/package_command.rs`; `rust-code-analysis-cli --metrics --output-format json --paths crates/cdf-cli/src/package_command.rs`.
- 2026-07-08: Parent verification and closure evidence recorded in `.10x/evidence/2026-07-08-cli-package-gc-retention.md`; adversarial review passed in `.10x/reviews/2026-07-08-cli-package-gc-retention-review.md`. The ticket is closed as dry-run retention planning; destructive collection remains intentionally unimplemented because no deletion flag semantics are ratified.
