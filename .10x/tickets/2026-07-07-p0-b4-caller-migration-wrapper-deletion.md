Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-p0-workstream-b-open-orchestrator-world.md
Depends-On: .10x/tickets/done/2026-07-07-p0-b2-generic-package-replay-recovery.md, .10x/tickets/2026-07-07-p0-b3-generic-project-run-resolution.md

# P0 B4: Caller migration, wrapper deletion, and Workstream B closure

## Scope

Finish Workstream B by migrating all remaining callers to the generic runtime APIs, deleting temporary destination-specialized wrapper families, and recording closure evidence.

Owns:

- CLI `replay package` and `resume` migration to project-owned destination resolution and generic package replay/recovery APIs;
- conformance package replay, live-run, golden, and chaos helper migration;
- deletion of public DuckDB/Parquet/Postgres replay/recover wrapper families and DuckDB-only public failpoint wrappers;
- public API before/after inventory;
- Workstream B evidence, adversarial review, coverage-matrix update, and parent ticket status update.

## Acceptance criteria

- No external caller imports destination-specialized package replay/recovery request or wrapper types from `cdf-project`.
- `rg` proves the specialized wrapper family names are absent from public `cdf-project` API and from CLI/conformance callers.
- CLI run, replay package, resume, conformance, golden, and chaos callers route through the generic path.
- A mock destination registration test remains in place and proves adding a destination requires registration only, not generic orchestrator edits.
- `runtime.rs`/runtime modules satisfy the Workstream B module split and complexity expectations.
- Workstream B has evidence and adversarial review records before moving to done.

## Evidence expectations

Record `rg` wrapper-deletion proof, public API inventory before/after, `cargo fmt --check`, `cargo check --workspace`, focused tests for `cdf-project`, `cdf-cli`, and `cdf-conformance`, `jscpd`, `rust-code-analysis-cli`, `git diff --check`, and adversarial review.

## Explicit exclusions

No new destination implementation, no Workstream C scenario matrix expansion beyond preserving current conformance callers, no benchmark claims, and no public release.

## Progress and notes

- 2026-07-07: Opened from Workstream B. Huygens inventory names the caller sets that must migrate: CLI run/replay/resume, CLI tests, conformance package replay/live-run/chaos/golden helpers, and project runtime tests.

## Blockers

Depends on B2 and B3.
