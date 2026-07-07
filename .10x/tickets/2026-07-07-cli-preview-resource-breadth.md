Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-cli-surface.md
Depends-On: .10x/specs/project-cli-observability-security.md, .10x/specs/resource-authoring-planning-batches.md

# Broaden cdf preview resources

## Scope

Extend `cdf preview` beyond the closed single local-file slice to cover the remaining ratified preview surfaces.

Owns:

- `crates/cdf-cli/src/scan_command.rs` preview orchestration and tests.
- Lower runtime adapters needed for REST declarative preview, SQL declarative preview, Arrow IPC preview, and multi-file scan preview semantics.
- No-write proof helpers for package roots, destinations, checkpoint stores, and run ledgers.

## Acceptance criteria

- `cdf preview` can inspect one batch for supported REST, SQL, Arrow IPC, and multi-file resources.
- Preview applies the same projection/filter/limit planning semantics as `plan` where supported.
- Preview writes no package, destination data, receipt, checkpoint, or run-ledger event.
- Unsupported preview resource shapes fail closed with explicit lower-layer requirements.
- JSON output includes resource, partition, batch, row count, byte count, and write-effects fields.

## Evidence expectations

Run focused CLI preview tests for each resource family, no-write assertions, relevant resource runtime tests, `cargo fmt --all -- --check`, targeted clippy/check, `git diff --check`, Jscpd/rust-code-analysis over touched CLI modules, and applicable security scans.

## Explicit exclusions

No package creation, no destination commit, no checkpoint advancement, no multi-batch sampling, no arbitrary SQL query execution beyond ratified table-backed SQL resource semantics, and no network calls without explicit local/fake HTTP test harnesses.

## Blockers

None. If implementation needs to ratify Arrow IPC or multi-file preview details not covered by active specs, self-ratify before source edits.

## Progress and notes

- 2026-07-07: Split from `.10x/tickets/done/2026-07-07-cli-remaining-command-planners.md`. Existing preview covers single-match declarative local file resources; broader surfaces remain open.
